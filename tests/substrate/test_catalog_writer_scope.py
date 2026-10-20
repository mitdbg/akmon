"""Focused writer-authority and error-classification coverage for #1303."""
from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.catalog import ownership as ownership_mod
from okg.substrate.catalog.modules_compose import ComposedCatalog
from okg.substrate.catalog.ownership import (
    WRITER_SCOPE_AMBIGUOUS,
    WRITER_SCOPE_CHANGE,
    WRITER_SCOPE_DEPLOYMENT_ROOT,
    WRITER_SCOPE_ENVIRONMENT,
    catalog_identity_for,
    claim_catalog_ownership,
    writer_scope_claim_problem,
)
from okg.substrate.errors import CatalogOwnershipMismatch, SubstrateError


DEPLOYMENT = "catalog-scope-fixture"
ENVIRONMENT_ID = "catalog-scope-production"


@pytest.fixture()
def composed() -> ComposedCatalog:
    return ComposedCatalog(deployment_name=DEPLOYMENT, modules=[])


@pytest.fixture()
def deployment_dir(tmp_path: Path) -> Path:
    root = tmp_path / "deployments" / DEPLOYMENT
    root.mkdir(parents=True)
    (root / "deployment.yaml").write_text(
        f"name: {DEPLOYMENT}\nmodules: []\n",
        encoding="utf-8",
    )
    return root


def _identity(
    composed: ComposedCatalog,
    deployment_dir: Path,
    *,
    writer_scope: str | None = WRITER_SCOPE_DEPLOYMENT_ROOT,
    environment_id: str | None = None,
):
    return catalog_identity_for(
        composed=composed,
        deployment_dir=deployment_dir,
        command="test catalog writer authority",
        writer_scope=writer_scope,
        environment_id=environment_id,
    )


def _environment(deployment_dir: Path, *, environment_id: str):
    manifest = SimpleNamespace(
        deployment_dir=deployment_dir,
        postgres_dsn="postgresql://fixture.example/okg",
    )
    return SimpleNamespace(
        id=environment_id,
        graph_deployment_name=DEPLOYMENT,
        manifest=manifest,
    )


def test_ambient_environment_id_is_not_writer_authority(
    composed: ComposedCatalog,
    deployment_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_ENVIRONMENT_ID", "ambient-production")

    identity = _identity(composed, deployment_dir)

    assert identity.provenance["writer_scope"] == WRITER_SCOPE_DEPLOYMENT_ROOT
    assert identity.provenance["environment_id"] is None
    assert identity.provenance["ambient_environment_id"] is None


def test_environment_scope_requires_an_explicit_environment_id(
    composed: ComposedCatalog,
    deployment_dir: Path,
) -> None:
    with pytest.raises(SubstrateError) as exc_info:
        _identity(
            composed,
            deployment_dir,
            writer_scope=WRITER_SCOPE_ENVIRONMENT,
        )

    assert exc_info.value.data["reason_code"] == (
        "catalog_environment_identity_required"
    )


def test_missing_writer_scope_is_a_distinct_refusal(
    composed: ComposedCatalog,
    deployment_dir: Path,
) -> None:
    with pytest.raises(SubstrateError) as exc_info:
        _identity(
            composed,
            deployment_dir,
            writer_scope=None,
        )

    assert exc_info.value.code == "catalog_writer_scope_required"
    assert exc_info.value.data["reason_code"] == (
        "catalog_writer_scope_required"
    )


def test_deployment_scope_rejects_an_environment_id(
    composed: ComposedCatalog,
    deployment_dir: Path,
) -> None:
    with pytest.raises(SubstrateError) as exc_info:
        _identity(
            composed,
            deployment_dir,
            writer_scope=WRITER_SCOPE_DEPLOYMENT_ROOT,
            environment_id=ENVIRONMENT_ID,
        )

    assert exc_info.value.data["reason_code"] == WRITER_SCOPE_AMBIGUOUS


def test_explicit_environment_identity_ignores_ambient_conflict(
    composed: ComposedCatalog,
    deployment_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_ENVIRONMENT_ID", "wrong-ambient-environment")
    with_ambient = _identity(
        composed,
        deployment_dir,
        writer_scope=WRITER_SCOPE_ENVIRONMENT,
        environment_id=ENVIRONMENT_ID,
    )
    monkeypatch.delenv("OKG_ENVIRONMENT_ID")
    without_ambient = _identity(
        composed,
        deployment_dir,
        writer_scope=WRITER_SCOPE_ENVIRONMENT,
        environment_id=ENVIRONMENT_ID,
    )

    assert with_ambient.writer_fingerprint == without_ambient.writer_fingerprint
    assert with_ambient.provenance["environment_id"] == ENVIRONMENT_ID


def test_force_does_not_bypass_a_stale_or_conflicting_scope(
    composed: ComposedCatalog,
    deployment_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming = _identity(composed, deployment_dir)
    report = {
        "owner": {
            "deployment_name": DEPLOYMENT,
            "owner_fingerprint": "sha256:recorded-environment-writer",
            "metadata": {
                "writer_scope": WRITER_SCOPE_ENVIRONMENT,
                "environment_id": ENVIRONMENT_ID,
            },
        },
        "recent_catalog_versions": [],
    }
    monkeypatch.setattr(
        ownership_mod,
        "diagnose_catalog_ownership",
        lambda conn, **kwargs: report,
    )

    with pytest.raises(CatalogOwnershipMismatch) as exc_info:
        claim_catalog_ownership(None, identity=incoming, force=True)

    assert exc_info.value.code == WRITER_SCOPE_CHANGE
    assert exc_info.value.data["reason_code"] == WRITER_SCOPE_CHANGE


def test_different_explicit_environments_are_a_scope_change(
    composed: ComposedCatalog,
    deployment_dir: Path,
) -> None:
    incoming = _identity(
        composed,
        deployment_dir,
        writer_scope=WRITER_SCOPE_ENVIRONMENT,
        environment_id="catalog-scope-staging",
    )
    problem = writer_scope_claim_problem(
        identity=incoming,
        report={
            "owner": {
                "owner_fingerprint": "sha256:production-writer",
                "metadata": {
                    "writer_scope": WRITER_SCOPE_ENVIRONMENT,
                    "environment_id": ENVIRONMENT_ID,
                },
            },
            "recent_catalog_versions": [],
        },
    )

    assert problem is not None
    assert problem["reason"] == WRITER_SCOPE_CHANGE


@pytest.mark.parametrize(
    "reason",
    [
        "catalog_ownership_unclaimed",
        "catalog_ownership_foreign_writer",
        "catalog_applied_version_absent",
        WRITER_SCOPE_CHANGE,
    ],
)
def test_ownership_refusals_keep_their_public_error_code(
    reason: str,
    composed: ComposedCatalog,
    deployment_dir: Path,
) -> None:
    identity = _identity(composed, deployment_dir)

    with pytest.raises(CatalogOwnershipMismatch) as exc_info:
        ownership_mod._raise_mismatch(
            "fixture refusal",
            identity=identity,
            observed=None,
            fix_hint="fixture repair",
            reason=reason,
        )

    assert exc_info.value.code == reason
    assert exc_info.value.data["reason_code"] == reason


def test_unspecified_legacy_refusal_keeps_the_registered_class_code(
    composed: ComposedCatalog,
    deployment_dir: Path,
) -> None:
    identity = _identity(composed, deployment_dir)

    with pytest.raises(CatalogOwnershipMismatch) as exc_info:
        ownership_mod._raise_mismatch(
            "legacy fixture refusal",
            identity=identity,
            observed=None,
            fix_hint="fixture repair",
        )

    assert exc_info.value.code == "catalog_ownership_mismatch"
    assert exc_info.value.data["reason_code"] == (
        "catalog_ownership_unspecified"
    )


def test_directory_target_is_explicit_deployment_scope(
    deployment_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli.catalog import _resolve_catalog_target

    monkeypatch.setenv("OKG_ENVIRONMENT_ID", "ambient-production")
    target = _resolve_catalog_target(
        argparse.Namespace(
            deployment=str(deployment_dir),
            dsn=None,
            writer_scope=None,
            environment_id=None,
        )
    )

    assert target.writer_scope == WRITER_SCOPE_DEPLOYMENT_ROOT
    assert target.environment_id is None


def test_named_target_does_not_ignore_environment_id_without_scope() -> None:
    from okg.substrate.cli.catalog import _resolve_catalog_target

    with pytest.raises(SubstrateError) as exc_info:
        _resolve_catalog_target(
            argparse.Namespace(
                deployment=DEPLOYMENT,
                dsn=None,
                writer_scope=None,
                environment_id=ENVIRONMENT_ID,
            )
        )

    assert exc_info.value.code == WRITER_SCOPE_AMBIGUOUS
    assert exc_info.value.data["reason_code"] == WRITER_SCOPE_AMBIGUOUS


def test_environment_target_resolves_explicit_authority(
    deployment_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import catalog as catalog_mod

    environment = _environment(
        deployment_dir, environment_id=ENVIRONMENT_ID,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.environment.load_environment",
        lambda *_args, **_kwargs: environment,
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.compose_catalog",
        lambda target: SimpleNamespace(deployment_name=DEPLOYMENT),
    )

    target = catalog_mod._resolve_catalog_target(
        argparse.Namespace(
            deployment=ENVIRONMENT_ID,
            dsn=None,
            writer_scope="environment",
            environment_id=None,
        )
    )

    assert target.writer_scope == WRITER_SCOPE_ENVIRONMENT
    assert target.environment_id == ENVIRONMENT_ID


def test_unknown_environment_keeps_not_found_classification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import catalog as catalog_mod

    monkeypatch.setattr(
        "okg.substrate.deployments.environment.load_environment",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(FileNotFoundError()),
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.environment.load_environment_fleet",
        lambda *_args, **_kwargs: (),
    )

    with pytest.raises(SubstrateError) as exc_info:
        catalog_mod._resolve_catalog_target(
            argparse.Namespace(
                deployment="missing-environment",
                dsn=None,
                writer_scope="environment",
                environment_id=None,
            )
        )

    assert exc_info.value.code == "deployment_not_found"
    assert exc_info.value.data["reason_code"] == "deployment_not_found"


def test_ambiguous_environment_keeps_ambiguous_classification(
    deployment_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import catalog as catalog_mod

    environments = (
        _environment(deployment_dir, environment_id="fixture-a"),
        _environment(deployment_dir, environment_id="fixture-b"),
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.environment.load_environment",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(FileNotFoundError()),
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.environment.load_environment_fleet",
        lambda *_args, **_kwargs: environments,
    )

    with pytest.raises(SubstrateError) as exc_info:
        catalog_mod._resolve_catalog_target(
            argparse.Namespace(
                deployment=DEPLOYMENT,
                dsn=None,
                writer_scope="environment",
                environment_id=None,
            )
        )

    assert exc_info.value.code == "catalog_environment_identity_ambiguous"
    assert exc_info.value.data["environment_ids"] == ["fixture-a", "fixture-b"]


def test_graph_name_keeps_identifier_kind_mismatch(
    deployment_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import catalog as catalog_mod
    from okg.substrate.cli.deployment_identifier import IDENTIFIER_KIND_MISMATCH

    environment = _environment(
        deployment_dir, environment_id=ENVIRONMENT_ID,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.environment.load_environment",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(FileNotFoundError()),
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.environment.load_environment_fleet",
        lambda *_args, **_kwargs: (environment,),
    )

    with pytest.raises(SubstrateError) as exc_info:
        catalog_mod._resolve_catalog_target(
            argparse.Namespace(
                deployment=DEPLOYMENT,
                dsn=None,
                writer_scope="environment",
                environment_id=None,
            )
        )

    assert exc_info.value.code == IDENTIFIER_KIND_MISMATCH
    assert exc_info.value.data["reason_code"] == IDENTIFIER_KIND_MISMATCH


def test_invalid_environment_is_not_relabelled_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import catalog as catalog_mod
    from okg.substrate.deployments import DeploymentConfigError

    monkeypatch.setattr(
        "okg.substrate.deployments.environment.load_environment",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            DeploymentConfigError("broken descriptor")
        ),
    )

    with pytest.raises(SubstrateError) as exc_info:
        catalog_mod._resolve_catalog_target(
            argparse.Namespace(
                deployment=ENVIRONMENT_ID,
                dsn=None,
                writer_scope="environment",
                environment_id=None,
            )
        )

    assert exc_info.value.code == "catalog_environment_invalid"
    assert exc_info.value.data["resolution_error_type"] == (
        "DeploymentConfigError"
    )


def test_unreadable_environment_is_not_relabelled_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import catalog as catalog_mod

    monkeypatch.setattr(
        "okg.substrate.deployments.environment.load_environment",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            PermissionError("descriptor denied")
        ),
    )

    with pytest.raises(SubstrateError) as exc_info:
        catalog_mod._resolve_catalog_target(
            argparse.Namespace(
                deployment=ENVIRONMENT_ID,
                dsn=None,
                writer_scope="environment",
                environment_id=None,
            )
        )

    assert exc_info.value.code == "catalog_environment_invalid"
    assert exc_info.value.data["resolution_error_type"] == "PermissionError"


def test_unresolvable_deployment_dsn_keeps_configuration_classification(
    deployment_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import catalog as catalog_mod

    manifest = SimpleNamespace(
        deployment_dir=deployment_dir,
        postgres_dsn=None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda *_args, **_kwargs: manifest,
    )

    with pytest.raises(SubstrateError) as exc_info:
        catalog_mod._resolve_catalog_target(
            argparse.Namespace(
                deployment=DEPLOYMENT,
                dsn=None,
                writer_scope=None,
                environment_id=None,
            )
        )

    assert exc_info.value.code == "deployment_config_invalid"
    assert exc_info.value.data["reason_code"] == "deployment_config_invalid"
    assert exc_info.value.data["resolution_error_type"] == (
        "DeploymentResolutionError"
    )


def test_unresolvable_environment_dsn_keeps_environment_classification(
    deployment_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import catalog as catalog_mod

    environment = _environment(
        deployment_dir,
        environment_id=ENVIRONMENT_ID,
    )
    environment.manifest.postgres_dsn = None
    monkeypatch.setattr(
        "okg.substrate.deployments.environment.load_environment",
        lambda *_args, **_kwargs: environment,
    )

    with pytest.raises(SubstrateError) as exc_info:
        catalog_mod._resolve_catalog_target(
            argparse.Namespace(
                deployment=ENVIRONMENT_ID,
                dsn=None,
                writer_scope="environment",
                environment_id=None,
            )
        )

    assert exc_info.value.code == "catalog_environment_invalid"
    assert exc_info.value.data["reason_code"] == "catalog_environment_invalid"
    assert exc_info.value.data["resolution_error_type"] == (
        "DeploymentResolutionError"
    )


def test_runtime_identity_refuses_missing_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.publisher.deployment_stack import (
        build_runtime_catalog_identity,
    )

    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority.verify_worker_release_authority",
        lambda **_kwargs: None,
    )
    with pytest.raises(SubstrateError) as exc_info:
        build_runtime_catalog_identity(
            dsn="postgresql://fixture",
            deployment_name=DEPLOYMENT,
            deployment_dir=None,
            command="test missing authority",
        )

    assert exc_info.value.code == "catalog_writer_scope_required"


def test_runtime_identity_preserves_partial_sealed_error(
    deployment_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.errors import SealedReleaseInvalid
    from okg.substrate.publisher.deployment_stack import (
        build_runtime_catalog_identity,
    )

    def _refuse(**_kwargs):
        raise SealedReleaseInvalid("sealed authority is incomplete")

    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority.verify_worker_release_authority",
        _refuse,
    )
    with pytest.raises(SealedReleaseInvalid, match="incomplete"):
        build_runtime_catalog_identity(
            dsn="postgresql://fixture",
            deployment_name=DEPLOYMENT,
            deployment_dir=deployment_dir,
            command="test partial sealed authority",
        )


def test_runtime_identity_uses_verified_environment_authority(
    deployment_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.publisher.deployment_stack import (
        build_runtime_catalog_identity,
    )

    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority.verify_worker_release_authority",
        lambda **_kwargs: {
            "definition_root": str(deployment_dir),
            "environment_id": ENVIRONMENT_ID,
        },
    )
    identity = build_runtime_catalog_identity(
        dsn="postgresql://fixture",
        deployment_name=DEPLOYMENT,
        deployment_dir=deployment_dir,
        command="test verified environment authority",
    )

    assert identity.provenance["writer_scope"] == WRITER_SCOPE_ENVIRONMENT
    assert identity.provenance["environment_id"] == ENVIRONMENT_ID


def test_catalog_parser_names_the_explicit_authority() -> None:
    from okg.substrate.cli.main import _build_parser

    parser = _build_parser()
    args = parser.parse_args(
        [
            "catalog",
            "ownership",
            "claim",
            "--deployment",
            ENVIRONMENT_ID,
            "--writer-scope",
            "environment",
            "--environment-id",
            ENVIRONMENT_ID,
        ]
    )

    assert args.writer_scope == "environment"
    assert args.environment_id == ENVIRONMENT_ID
    assert args.allow_scope_change is False
