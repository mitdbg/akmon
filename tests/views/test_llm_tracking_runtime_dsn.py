from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
import psycopg

from okg.substrate.llm.tracking import (
    TrackingConfigurationError,
    resolve_tracking_dsn,
)
from okg.views.execution.llm import LiteLLMClient, VLMClient

pytestmark = pytest.mark.postgres


def test_tracking_mode_precedence_keeps_runtime_dsn_opt_in() -> None:
    runtime = "postgresql://user:p%40ss%24token@db.example/one"

    assert resolve_tracking_dsn(
        tracking_mode="disabled",
        explicit_tracking_dsn=None,
        runtime_dsn=runtime,
        component="test",
    ) is None
    assert resolve_tracking_dsn(
        tracking_mode="optional",
        explicit_tracking_dsn=None,
        runtime_dsn=runtime,
        component="test",
    ) == runtime
    assert resolve_tracking_dsn(
        tracking_mode="optional",
        explicit_tracking_dsn="   ",
        runtime_dsn=runtime,
        component="test",
    ) == runtime


def test_required_tracking_fails_without_runtime_authority() -> None:
    with pytest.raises(TrackingConfigurationError, match="tracking_mode=required"):
        resolve_tracking_dsn(
            tracking_mode="required",
            explicit_tracking_dsn=None,
            runtime_dsn=None,
            component="test",
        )


def test_explicit_tracking_authority_wins_over_unused_runtime_placeholder() -> None:
    explicit = "postgresql://user:p%40ss@db.example/explicit"
    assert resolve_tracking_dsn(
        tracking_mode="optional",
        explicit_tracking_dsn=explicit,
        runtime_dsn="${WORKER_DSN}",
        component="test",
    ) == explicit


def test_malformed_tracking_authority_fails_before_client_construction() -> None:
    with pytest.raises(TrackingConfigurationError, match="malformed"):
        resolve_tracking_dsn(
            tracking_mode="required",
            explicit_tracking_dsn="not a dsn !!!",
            runtime_dsn=None,
            component="test",
        )


def test_unresolved_placeholder_fails_without_echoing_value() -> None:
    secret = "${COOPER_OKG_DSN}"
    with pytest.raises(TrackingConfigurationError) as caught:
        resolve_tracking_dsn(
            tracking_mode="optional",
            explicit_tracking_dsn=secret,
            runtime_dsn=None,
            component="test",
        )
    assert secret not in str(caught.value)
    assert caught.value.code == "llm_tracking_configuration"
    assert caught.value.data["reason"] == "unresolved_explicit_dsn"


def test_clients_inherit_only_when_tracking_mode_is_explicit() -> None:
    runtime = "postgresql://user:p%40ss@db.example/one"
    disabled = LiteLLMClient(model="test/model", runtime_dsn=runtime)
    enabled = LiteLLMClient(
        model="test/model", tracking_mode="optional", runtime_dsn=runtime
    )
    vlm = VLMClient(
        model="test/model", tracking_mode="optional", runtime_dsn=runtime
    )

    assert disabled._tracking_dsn is None
    assert enabled._tracking_dsn == runtime
    assert vlm._tracking_dsn == runtime


def test_visual_caption_enricher_uses_worker_authority() -> None:
    from okg.substrate.library.enrichers.visual_caption_enricher import (
        VisualCaptionEnricher,
    )

    runtime = "postgresql://user:p%40ss@db.example/one"
    enricher = VisualCaptionEnricher(
        model="test/model",
        tracking_mode="optional",
        runtime_dsn=runtime,
        caption_service=object(),
    )
    assert enricher._tracking_dsn == runtime
    assert not hasattr(enricher, "tracking_dsn")


def test_tracking_write_failure_is_counted_without_exposing_authority(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from okg.substrate.llm import tracker

    @contextmanager
    def failed_connection(_dsn: str):
        raise OSError("telemetry database unavailable")
        yield  # pragma: no cover

    before = tracker.telemetry_write_failures()
    monkeypatch.setattr(tracker, "_telemetry_connection", failed_connection)
    with caplog.at_level("WARNING"):
        tracker.record_llm_call(
            "postgresql://redacted/one",
            caller="fixture:write-failure",
            model="fixture/model",
            fragment_id=None,
            prompt_tokens=None,
            completion_tokens=None,
            total_tokens=None,
            cost_usd=None,
            latency_ms=1,
            success=False,
        )
    assert tracker.telemetry_write_failures() == before + 1
    assert "event=llm_tracking_write_failed" in caplog.text
    assert "postgresql://redacted/one" not in caplog.text


def test_runtime_inherited_tracking_persists_success_and_provider_failure(
    clean_runtime: str,
) -> None:
    runtime = clean_runtime
    deployment = f"fixture-tracking-{uuid4().hex}"
    client = LiteLLMClient(
        model="fixture/model",
        tracking_mode="optional",
        runtime_dsn=runtime,
        deployment=deployment,
    )
    response = SimpleNamespace(
        usage=SimpleNamespace(prompt_tokens=2, completion_tokens=3, total_tokens=5),
        _hidden_params={},
        model="fixture/model",
    )
    client._call_with_retry = lambda *_args, **_kwargs: ("ok", response)  # type: ignore[method-assign]
    client.complete("system", "success", caller="fixture:success", generation_id=41)

    disabled = LiteLLMClient(
        model="fixture/model", runtime_dsn=runtime, deployment=deployment
    )
    disabled._call_with_retry = lambda *_args, **_kwargs: ("ignored", response)  # type: ignore[method-assign]
    disabled.complete("system", "disabled", caller="fixture:disabled", generation_id=41)

    class ProviderRateLimitError(RuntimeError):
        status_code = 429

    def fail(*_args, **_kwargs):
        raise ProviderRateLimitError("rate limited")

    client._call_with_retry = fail  # type: ignore[method-assign]
    with pytest.raises(Exception):
        client.complete("system", "failure", caller="fixture:failure", generation_id=41)

    class ProviderUnauthorizedError(RuntimeError):
        status_code = 401

    def unauthorized(*_args, **_kwargs):
        raise ProviderUnauthorizedError("unauthorized")

    client._call_with_retry = unauthorized  # type: ignore[method-assign]
    with pytest.raises(Exception):
        client.complete("system", "unauthorized", caller="fixture:unauthorized", generation_id=41)

    def transport(*_args, **_kwargs):
        raise OSError("provider transport unavailable")

    client._call_with_retry = transport  # type: ignore[method-assign]
    with pytest.raises(Exception):
        client.complete(
            "system", "transport", caller="fixture:transport", generation_id=41
        )

    with psycopg.connect(runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT caller, success, error_kind, deployment_name, generation_id, extras "
            "FROM okg.llm_calls WHERE deployment_name = %s ORDER BY caller",
            (deployment,),
        )
        rows = cur.fetchall()
        cur.execute(
            "SELECT count(*) FROM okg.llm_calls WHERE caller = %s",
            ("fixture:disabled",),
        )
        disabled_count = cur.fetchone()[0]
    assert [row[0] for row in rows] == [
        "fixture:failure",
        "fixture:success",
        "fixture:transport",
        "fixture:unauthorized",
    ]
    failure, success, transport, unauthorized_row = rows
    assert failure[1] is False
    assert failure[2] == "ProviderRateLimitError"
    assert failure[4] == "41"
    assert failure[5]["provider_status_code"] == 429
    assert success[1] is True
    assert success[2] is None
    assert transport[1] is False
    assert transport[2] == "OSError"
    assert transport[5] is None or "provider_status_code" not in transport[5]
    assert unauthorized_row[1] is False
    assert unauthorized_row[2] == "ProviderUnauthorizedError"
    assert unauthorized_row[5]["provider_status_code"] == 401
    assert disabled_count == 0
