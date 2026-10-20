from __future__ import annotations

from types import SimpleNamespace

import pytest

from okg.substrate.embeddings import (
    EMBEDDING_DIMENSION,
    EmbeddingConfig,
    build_embedding_execution_plan,
)
from okg.substrate.errors import SubstrateError


class FakeEmbedder:
    def __init__(
        self,
        *,
        declared: int,
        returned: int,
        model_id: str = "fake:verified",
    ) -> None:
        self.dimension = declared
        self.returned = returned
        self.model_id = model_id
        self.calls: list[str] = []

    def embed(self, text: str) -> list[float]:
        self.calls.append(text)
        return [0.25] * self.returned


def _config() -> EmbeddingConfig:
    return EmbeddingConfig(enabled=True, backend="hash")


def test_declared_384_is_rejected() -> None:
    fake = FakeEmbedder(declared=384, returned=384)
    with pytest.raises(SubstrateError) as exc:
        build_embedding_execution_plan(_config(), builder=lambda _spec: fake)
    assert exc.value.code == "embedder_dim_mismatch"
    assert fake.calls == []


def test_declared_2560_returning_384_is_rejected_by_actual_probe() -> None:
    fake = FakeEmbedder(declared=EMBEDDING_DIMENSION, returned=384)
    with pytest.raises(SubstrateError) as exc:
        build_embedding_execution_plan(_config(), builder=lambda _spec: fake)
    assert exc.value.code == "embedder_dim_mismatch"
    assert len(fake.calls) == 1
    assert exc.value.data["observed_dimension"] == 384


def test_verified_2560_plan_is_deterministic_across_process_local_builds() -> None:
    first = build_embedding_execution_plan(
        _config(),
        builder=lambda _spec: FakeEmbedder(
            declared=EMBEDDING_DIMENSION,
            returned=EMBEDDING_DIMENSION,
        ),
    )
    second = build_embedding_execution_plan(
        _config(),
        builder=lambda _spec: FakeEmbedder(
            declared=EMBEDDING_DIMENSION,
            returned=EMBEDDING_DIMENSION,
        ),
    )
    assert first.fingerprint == second.fingerprint
    assert first.model_id == second.model_id == "fake:verified"
    assert first.observed_dimension == EMBEDDING_DIMENSION


def test_same_dimension_different_model_identity_has_different_fingerprint() -> None:
    plans = [
        build_embedding_execution_plan(
            _config(),
            builder=lambda _spec, model_id=model_id: FakeEmbedder(
                declared=EMBEDDING_DIMENSION,
                returned=EMBEDDING_DIMENSION,
                model_id=model_id,
            ),
        )
        for model_id in ("fake:a", "fake:b")
    ]
    assert plans[0].fingerprint != plans[1].fingerprint


def test_verified_plan_rechecks_each_backend_output() -> None:
    fake = FakeEmbedder(
        declared=EMBEDDING_DIMENSION,
        returned=EMBEDDING_DIMENSION,
    )
    plan = build_embedding_execution_plan(
        _config(),
        builder=lambda _spec: fake,
    )
    fake.returned = 384
    with pytest.raises(SubstrateError) as exc:
        plan.embed("post-preflight request")
    assert exc.value.code == "embedder_dim_mismatch"


def test_batch_refuses_full_manifest_config_drift_before_graph_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime.embed_workflow import _run_one_embed_batch

    plan = build_embedding_execution_plan(_config())
    changed = EmbeddingConfig(
        enabled=True,
        backend="hash",
        reason="changed after preflight",
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(embedding_config=changed),
    )
    with pytest.raises(RuntimeError, match="current admitted manifest"):
        _run_one_embed_batch(
            deployment="test",
            batch_size=1,
            embed_one=lambda *_args: pytest.fail("embed side effect ran"),
            embedding_plan=plan,
        )


def test_batch_claims_null_or_different_model_identity() -> None:
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[3]
        / "src/okg/substrate/runtime/embed_workflow.py"
    ).read_text(encoding="utf-8")
    assert "embedding_model IS DISTINCT FROM :model_id" in source


def test_worker_refuses_failed_preflight_before_wrapper_or_runtime_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.runtime.config import RuntimeConfig
    from okg.substrate.runtime.worker import run_worker

    calls: list[str] = []
    manifest = SimpleNamespace(embedding_config=_config())
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: manifest,
    )

    failure = SubstrateError("fake 384-dimensional backend")
    failure.code = "embedder_dim_mismatch"

    def reject(_config, **_kwargs):
        calls.append("preflight")
        raise failure

    monkeypatch.setattr(
        "okg.substrate.embeddings.build_embedding_execution_plan",
        reject,
    )
    monkeypatch.setattr(
        "signal.signal",
        lambda *_args: calls.append("signal"),
    )
    monkeypatch.setattr(
        "okg.substrate.db.capacity.release_process_reservations",
        lambda **_kwargs: calls.append("reservation_cleanup"),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority.verify_worker_release_authority",
        lambda **_kwargs: calls.append("release_authority"),
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.code_identity.record_worker_code_identity",
        lambda **_kwargs: calls.append("code_identity"),
    )

    with pytest.raises(SubstrateError) as exc:
        run_worker(
            deployment="test",
            dsn="postgres://must-not-be-read",
            runtime=RuntimeConfig(enabled=True),
        )
    assert exc.value.code == "embedder_dim_mismatch"
    assert calls == ["preflight"]
