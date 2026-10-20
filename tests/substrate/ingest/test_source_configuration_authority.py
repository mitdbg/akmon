"""Fail-closed source-root authority at the direct ingest boundary."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import okg.substrate.ingest.service as ingest_service
from okg.substrate.errors import SourceConfigurationAuthorityInvalid
from okg.substrate.ingest.service import IngestOptions, run_ingest


@pytest.fixture(autouse=True)
def _dummy_dsn_takes_no_source_sync_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The #3003 run guard needs a real session; these tests have none."""
    from contextlib import nullcontext

    real_guard = ingest_service.ingest_run_guard

    def guard(**kwargs: object) -> object:
        if kwargs["dsn"] == "postgres://unused":
            return nullcontext()
        return real_guard(**kwargs)

    monkeypatch.setattr(ingest_service, "ingest_run_guard", guard)


def test_direct_ingest_refuses_missing_root_before_registry_or_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment_dir = tmp_path / "deployment"
    deployment_dir.mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n",
        encoding="utf-8",
    )
    missing = tmp_path / "foreign-host-root"

    monkeypatch.setattr(
        ingest_service,
        "load_source_registry_admission",
        lambda *_args, **_kwargs: pytest.fail(
            "registry admitted before source-root authority",
        ),
    )
    monkeypatch.setattr(
        ingest_service,
        "_staged_source_redrive_state",
        lambda _options: pytest.fail("source state read before authority"),
    )
    monkeypatch.setattr(
        ingest_service,
        "_build_deployment_publisher_stack",
        lambda **_kwargs: pytest.fail("publisher built before authority"),
    )

    with pytest.raises(SourceConfigurationAuthorityInvalid) as exc:
        run_ingest(
            IngestOptions(
                dsn="postgres://unused",
                deployment="fixture",
                deployment_dir=deployment_dir,
                publish=True,
                source_registry_repo_root_override=missing,
            ),
        )

    assert exc.value.data["configured_path"] == str(missing)


def test_direct_ingest_reuses_canonical_root_for_forced_strict_reload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment_dir = tmp_path / "deployment"
    deployment_dir.mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "admission_contract: strict_v1\nsources: {}\n",
        encoding="utf-8",
    )
    canonical = tmp_path / "authoritative"
    canonical.mkdir()
    alias = tmp_path / "source-root-alias"
    alias.symlink_to(canonical, target_is_directory=True)

    real_load = ingest_service.load_source_registry_admission
    observed_roots: list[Path | str | None] = []

    def capture_load(*args, **kwargs):
        observed_roots.append(kwargs.get("repo_root_override"))
        return real_load(*args, **kwargs)

    planning_calls = 0

    def plan_bindings(*_args, **_kwargs):
        nonlocal planning_calls
        planning_calls += 1
        if planning_calls == 1:
            raise ValueError("force one strict whole-document reload")
        return SimpleNamespace(plans={})

    monkeypatch.setattr(
        ingest_service,
        "load_source_registry_admission",
        capture_load,
    )
    monkeypatch.setattr(
        ingest_service,
        "plan_source_registry_bindings",
        plan_bindings,
    )

    outcome = run_ingest(
        IngestOptions(
            dsn="postgres://unused",
            deployment="fixture",
            deployment_dir=deployment_dir,
            publish=False,
            source_registry_repo_root_override=alias,
        ),
    )

    assert outcome.any_failed is False
    assert planning_calls == 2
    assert observed_roots == [canonical.resolve(), canonical.resolve()]
