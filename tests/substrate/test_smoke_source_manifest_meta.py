"""Static meta-check: deployment smoke sources are active, non-frozen, owned.

``deployment-smoke-source-meta-check`` PACT (child of
``deployment-lifecycle-e2e-ci``). Guards the class of bug that shipped
okg-workspace ``smoke_sources: [openspec]`` — a frozen source whose
``smoke_publish`` can never change live rows, so every routine ``okg deployment
up`` silently no-ops its must-change-rows gate (the 2026-07-22/23 catch-up
deploy; PR #854). Pure static check — no database, no ``@pytest.mark.postgres``,
runs in the fast lane so it gates every PR cheaply.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.deployments import DeploymentManifest
from okg.substrate.deployments.smoke_source_policy import (
    FROZEN_SMOKE_SOURCE_ADAPTERS,
    SmokeSourceUnsuitable,
    check_smoke_source_suitable,
)
from okg.substrate.sources.registry import load_source_registry
from tests.mirror_scope import is_public_copy

# tests/substrate/test_smoke_source_manifest_meta.py -> repo root.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEPLOYMENTS_DIR = _REPO_ROOT / "deployments"


def _discover_manifest_paths() -> list[Path]:
    return sorted(
        child / "deployment.yaml"
        for child in _DEPLOYMENTS_DIR.iterdir()
        if child.is_dir() and (child / "deployment.yaml").exists()
    )


_MANIFEST_PATHS = _discover_manifest_paths()


def test_meta_check_discovers_in_repo_manifests() -> None:
    """Guard the guard: enumeration must not silently find zero manifests (a
    globbing regression would make the parametrized check vacuously pass)."""
    names = {path.parent.name for path in _MANIFEST_PATHS}
    # kubernetes-smoke ships and declares smoke sources; okg-ci and
    # okg-workspace are this repository's own and are absent from the public copy.
    shipped_names = {"kubernetes-smoke"}
    expected = shipped_names if is_public_copy() else shipped_names | {"okg-ci", "okg-workspace"}
    assert expected <= names, sorted(names)


@pytest.mark.parametrize(
    "manifest_path",
    _MANIFEST_PATHS,
    ids=[path.parent.name for path in _MANIFEST_PATHS],
)
def test_manifest_smoke_sources_are_active_owned_non_frozen(
    manifest_path: Path,
) -> None:
    """Every declared smoke source resolves in the deployment's registry, is
    deployment-owned, and is not a frozen/retired adapter."""
    manifest = DeploymentManifest.from_yaml(manifest_path)
    smoke_sources = tuple(getattr(manifest.runtime, "smoke_sources", ()) or ())
    if not smoke_sources:
        pytest.skip(f"{manifest.name}: no smoke_sources declared")
    registry = load_source_registry(manifest.source_registry)
    for name in smoke_sources:
        # Raises SmokeSourceUnsuitable (a test failure) with a naming message.
        check_smoke_source_suitable(manifest.name, name, registry.get(name))


def test_canonical_deployments_declare_suitable_smoke_sources() -> None:
    """Non-vacuity anchor: the two deployments that DO run a smoke canary
    (okg-ci, okg-workspace) must declare at least one smoke source and it must
    pass — so a regression that drops smoke_sources, or reverts okg-workspace to
    the frozen `openspec`, reds this test rather than skipping."""
    for name in ("okg-ci", "okg-workspace"):
        manifest = DeploymentManifest.from_yaml(
            _DEPLOYMENTS_DIR / name / "deployment.yaml"
        )
        smoke_sources = tuple(getattr(manifest.runtime, "smoke_sources", ()) or ())
        assert smoke_sources, f"{name} unexpectedly declares no smoke_sources"
        registry = load_source_registry(manifest.source_registry)
        for source_name in smoke_sources:
            check_smoke_source_suitable(
                manifest.name, source_name, registry.get(source_name)
            )


# --- the guard is not vacuous: it reds on the exact incident shape -----------


class _FakeEntry:
    """Minimal duck-typed SourceEntry (ownership_id + raw registry keys).

    Defaults describe an active, publishable source (versioned + published
    generation); override to model frozen / registry-only / legacy shapes.
    """

    def __init__(
        self,
        ownership_id: str,
        module: str,
        *,
        source_revision_kind: str = "content_hash",
        publication_mode: str = "published_generation",
    ) -> None:
        self.ownership_id = ownership_id
        self.raw = {
            "module": module,
            "source_revision_kind": source_revision_kind,
            "publication_mode": publication_mode,
        }


def test_frozen_openspec_smoke_source_is_refused() -> None:
    # Structurally publishable (versioned + published_generation) yet frozen in
    # content — the exact openspec shape; the frozen denylist must fire first.
    frozen = _FakeEntry("dep.openspec", "okg.substrate.library.sources.openspec")
    with pytest.raises(SmokeSourceUnsuitable, match="frozen"):
        check_smoke_source_suitable("dep", "openspec", frozen)


def test_missing_smoke_source_is_refused() -> None:
    with pytest.raises(SmokeSourceUnsuitable, match="not declared"):
        check_smoke_source_suitable("dep", "ghost", None)


def test_foreign_owned_smoke_source_is_refused() -> None:
    foreign = _FakeEntry("other-dep.pact", "okg.substrate.library.sources.pact")
    with pytest.raises(SmokeSourceUnsuitable, match="deployment-owned"):
        check_smoke_source_suitable("dep", "pact", foreign)


def test_non_publishable_smoke_source_is_refused() -> None:
    # A deployment-owned, non-frozen source that cannot produce a published
    # generation (registry-only / non-published) still no-ops smoke_publish.
    non_publishable = _FakeEntry(
        "dep.thing",
        "okg.substrate.library.sources.thing",
        publication_mode="registry_only",
    )
    with pytest.raises(SmokeSourceUnsuitable, match="published generation"):
        check_smoke_source_suitable("dep", "thing", non_publishable)


def test_active_owned_smoke_source_passes() -> None:
    active = _FakeEntry("dep.pact", "okg.substrate.library.sources.pact")
    check_smoke_source_suitable("dep", "pact", active)  # must not raise


def test_openspec_adapter_is_registered_frozen() -> None:
    assert (
        "okg.substrate.library.sources.openspec" in FROZEN_SMOKE_SOURCE_ADAPTERS
    )
