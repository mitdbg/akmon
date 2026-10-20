"""A sealed-release reseal must not invalidate ownership or the catalog.

`writer_fingerprint` bound `deployment_root_hash`, the sha256 of the
resolved deployment_dir. For a sealed release that path is

    .../artifacts/<CONTENT-ADDRESSED-HASH>/repo/deployments/<name>

so the hash changed on every reseal. The fingerprint is what BOTH the
ownership claim (`owner_fingerprint`) and the applied-catalog lookup
(`AND writer_fingerprint = %s`) match on, so a release upgrade
invalidated both at once — and the three resulting conditions all
surfaced under the single `catalog_ownership_mismatch` code, one of which
is a genuine cross-worktree safety refusal that must NOT be forced past.

Under a verified sealed release the runtime builder explicitly scopes the
writer by the selected environment, which is stable across reseals. An ambient
`OKG_ENVIRONMENT_ID` alone has no authority. Outside a sealed release, the
explicit path-based scope is unchanged so existing claims keep matching and the
cross-worktree guard still fires.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import ComposedCatalog
from okg.substrate.catalog.ownership import catalog_identity_for

ENVIRONMENT_ID = "okg-workspace-production"
DEPLOYMENT = "okg-workspace"

ARTIFACT_A = "25203ebf6dd50c797f9b8d168f496c0a7313a4c0630dbd15350c3dea52d9c8fb"
ARTIFACT_B = "9f1c0d5b6a2e4738c1d90fa4be7315286d0c4e1f8a37b25c6e0d9418ab73f5c2"


def _sealed_deployment_dir(root: Path, artifact: str) -> Path:
    """Mirror the sealed-release layout, including the content hash."""
    deployment_dir = root / "artifacts" / artifact / "repo" / "deployments" / DEPLOYMENT
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": DEPLOYMENT,
            "postgres": {"dsn": "${OKG_DSN}", "database": "okg_workspace"},
        }),
        encoding="utf-8",
    )
    return deployment_dir


@pytest.fixture()
def composed() -> ComposedCatalog:
    return ComposedCatalog(deployment_name=DEPLOYMENT, modules=["core"])


def _identity(
    deployment_dir: Path,
    composed: ComposedCatalog,
    *,
    environment_id: str | None = None,
):
    return catalog_identity_for(
        composed=composed,
        deployment_dir=deployment_dir,
        command="test",
        writer_scope=(
            "environment" if environment_id else "deployment_root"
        ),
        environment_id=environment_id,
    )


def test_reseal_preserves_the_writer_fingerprint(
    tmp_path: Path, composed: ComposedCatalog, monkeypatch,
) -> None:
    """The same environment on a new artifact keeps its writer identity."""
    monkeypatch.setenv("OKG_ENVIRONMENT_ID", "conflicting-ambient-value")

    before = _identity(
        _sealed_deployment_dir(tmp_path, ARTIFACT_A),
        composed,
        environment_id=ENVIRONMENT_ID,
    )
    after = _identity(
        _sealed_deployment_dir(tmp_path, ARTIFACT_B),
        composed,
        environment_id=ENVIRONMENT_ID,
    )

    assert before.deployment_root_hash != after.deployment_root_hash, (
        "the fixture must actually model a content-addressed path change"
    )
    assert before.writer_fingerprint == after.writer_fingerprint, (
        "a reseal changed the writer fingerprint, which invalidates BOTH "
        "the ownership claim and the applied catalog version"
    )
    assert before.provenance["writer_scope"] == "environment"


def test_a_different_environment_still_gets_a_different_writer(
    tmp_path: Path, composed: ComposedCatalog, monkeypatch,
) -> None:
    """Scoping by environment must not collapse two environments together."""
    deployment_dir = _sealed_deployment_dir(tmp_path, ARTIFACT_A)

    monkeypatch.setenv("OKG_ENVIRONMENT_ID", "ignored-ambient-value")
    production = _identity(
        deployment_dir,
        composed,
        environment_id=ENVIRONMENT_ID,
    )
    staging = _identity(
        deployment_dir,
        composed,
        environment_id="okg-workspace-staging",
    )

    assert production.writer_fingerprint != staging.writer_fingerprint


def test_outside_a_sealed_release_the_path_scope_is_unchanged(
    tmp_path: Path, composed: ComposedCatalog, monkeypatch,
) -> None:
    """Two checkouts must still get different writers — the P0 guard."""
    monkeypatch.delenv("OKG_ENVIRONMENT_ID", raising=False)

    one = _identity(_sealed_deployment_dir(tmp_path, ARTIFACT_A), composed)
    two = _identity(_sealed_deployment_dir(tmp_path, ARTIFACT_B), composed)

    assert one.writer_fingerprint != two.writer_fingerprint, (
        "without a sealed release the writer must stay path-scoped, or two "
        "checkouts could both claim one shared database"
    )
    assert one.provenance["writer_scope"] == "deployment_root"


def test_an_empty_ambient_environment_id_does_not_select_writer_scope(
    tmp_path: Path, composed: ComposedCatalog, monkeypatch,
) -> None:
    """A blank value must not be read as a valid environment."""
    monkeypatch.setenv("OKG_ENVIRONMENT_ID", "   ")

    identity = _identity(_sealed_deployment_dir(tmp_path, ARTIFACT_A), composed)

    assert identity.provenance["writer_scope"] == "deployment_root"


def test_a_catalog_change_moves_the_catalog_but_not_the_writer(
    tmp_path: Path, composed: ComposedCatalog, monkeypatch,
) -> None:
    """A reseal that changes the catalog re-applies as the SAME writer.

    #2229: the catalog fingerprint still moves, so the change must be
    loaded before publish; the writer fingerprint does not, so the
    deployment's own writer is never refused as foreign.
    """
    monkeypatch.setenv("OKG_ENVIRONMENT_ID", "ignored-ambient-value")
    deployment_dir = _sealed_deployment_dir(tmp_path, ARTIFACT_A)

    before = _identity(
        deployment_dir,
        composed,
        environment_id=ENVIRONMENT_ID,
    )
    changed = ComposedCatalog(
        deployment_name=DEPLOYMENT, modules=["core", "extra"],
    )
    after = _identity(
        deployment_dir,
        changed,
        environment_id=ENVIRONMENT_ID,
    )

    assert before.catalog_fingerprint != after.catalog_fingerprint
    assert before.schema_fingerprint != after.schema_fingerprint
    assert before.writer_fingerprint == after.writer_fingerprint
