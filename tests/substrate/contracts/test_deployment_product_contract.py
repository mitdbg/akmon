from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from okg.substrate.contracts.deployment_product import (
    APPLIED_INSTANCE_SCHEMA,
    AUTHORED_DEFINITION_SCHEMA,
    DISTRIBUTION_RELEASE_SCHEMA,
    PRODUCT_BUNDLE_SCHEMA,
    RESOLVED_LOCK_SCHEMA,
    SEALED_RELEASE_SCHEMA,
    AppliedInstance,
    AuthoredDeploymentDefinition,
    CompatibilityDecision,
    DistributionRelease,
    LocalAssetDescriptor,
    OwnerQualifiedReference,
    ProductBundle,
    ProvenanceRecord,
    ResolvedLock,
    SealedReleaseReference,
    SecretReference,
    TypedArtifactIdentity,
    TypedOverrideInput,
    validate_selection,
)
from okg.substrate.contracts.deployment_product import validation as validation_module


FIXTURES = Path(__file__).parents[2] / "fixtures" / "deployment_products" / "embedded_and_external"
_D0 = "sha256:" + "0" * 64
_D1 = "sha256:" + "1" * 64


def _load(name: str):
    raw = yaml.safe_load((FIXTURES / name).read_text())
    release = DistributionRelease.model_validate(raw["distribution_release"])
    bundles = tuple(ProductBundle.model_validate(item) for item in raw["product_bundles"])
    definition = AuthoredDeploymentDefinition.model_validate(raw["definition"])
    return release, bundles, definition


def test_embedded_and_external_products_share_one_validation_path() -> None:
    selections = []
    for name in ("embedded.yaml", "external.yaml"):
        release, bundles, definition = _load(name)
        selections.append(validate_selection(definition, release, bundles))

    assert selections[0].distribution_release.origin == "embedded"
    assert selections[1].distribution_release.origin == "external"
    assert tuple(bundle.bundle_id for bundle in selections[0].bundles) == ("core", "search")
    assert tuple(bundle.bundle_id for bundle in selections[1].bundles) == ("core", "search")
    assert type(selections[0].identity) is type(selections[1].identity)
    assert {item.source for item in selections[0].provenance} >= {"distribution", "bundle", "local", "override"}
    def provenance_shape(selection):
        return tuple(
            (
                item.path,
                item.source,
                bool(item.owner),
                bool(item.reference.owner),
                bool(item.reference.ref),
                bool(item.reference.digest),
                bool(item.value_digest),
            )
            for item in selection.provenance
        )
    assert provenance_shape(selections[0]) == provenance_shape(selections[1])
    resolver_source = inspect.getsource(validation_module.validate_selection).lower()
    assert "profile" not in resolver_source
    assert validation_module.validate_selection is validate_selection


def test_all_six_boundaries_are_distinct_and_versioned() -> None:
    release, bundles, definition = _load("embedded.yaml")
    selection = validate_selection(definition, release, bundles)
    assert type(release) is DistributionRelease
    assert type(bundles[0]) is ProductBundle
    assert type(definition) is AuthoredDeploymentDefinition
    assert release.schema_version == DISTRIBUTION_RELEASE_SCHEMA
    assert bundles[0].schema_version == PRODUCT_BUNDLE_SCHEMA
    assert definition.schema_version == AUTHORED_DEFINITION_SCHEMA

    lock = ResolvedLock(
        definition_id=definition.definition_id,
        definition_digest=_D1,
        composition_digest=_D0,
        product_identity=selection.identity,
        distribution_release_ref=release.reference,
        bundle_refs=tuple(bundle.reference for bundle in reversed(bundles)),
        package_refs=(TypedArtifactIdentity(owner="pypi", id="okg", kind="package", version="1", digest=_D1),),
        environment_refs=(TypedArtifactIdentity(owner="python", id="cp312", kind="environment", version="1", digest=_D1),),
        compatibility_decisions=(
            CompatibilityDecision(
                subject=OwnerQualifiedReference(owner="distribution", id="okg-standard", kind="release"),
                decision="compatible",
                reason="tested-bundle-set",
            ),
        ),
    )
    assert lock.schema_version == RESOLVED_LOCK_SCHEMA
    assert tuple(ref.bundle_id for ref in lock.bundle_refs) == ("core", "search")
    for missing_digest in ("definition_digest", "composition_digest"):
        missing = lock.model_dump(mode="python")
        missing.pop(missing_digest)
        with pytest.raises((ValidationError, ValueError)):
            ResolvedLock.model_validate(missing)

    lock_bytes = json.dumps(
        lock.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert b"supplied-secret" not in lock_bytes
    assert b"secret-version-2" not in lock_bytes
    assert b"host-state" not in lock_bytes
    assert b"generation-1" not in lock_bytes
    for forbidden_key, forbidden_value in {
        "secret_value": "supplied-secret",
        "secret_version": "secret-version-2",
        "host_state": "host-state",
        "generation_id": "generation-1",
    }.items():
        tampered = lock.model_dump(mode="python")
        tampered[forbidden_key] = forbidden_value
        with pytest.raises((ValidationError, ValueError)):
            ResolvedLock.model_validate(tampered)

    sealed = SealedReleaseReference(schema=SEALED_RELEASE_SCHEMA, artifact_id="sha256:" + "a" * 64)
    instance = AppliedInstance(
        instance_id="demo",
        definition_id=definition.definition_id,
        resolved_lock=lock,
        sealed_release=sealed,
        journal_id="journal-1",
    )
    assert instance.schema_version == APPLIED_INSTANCE_SCHEMA
    assert instance.sealed_release_ref.schema_version == SEALED_RELEASE_SCHEMA
    assert isinstance(instance, AppliedInstance)


def test_secret_references_are_allowed_but_literal_secrets_and_runtime_state_are_refused() -> None:
    release, bundles, definition = _load("embedded.yaml")
    assert definition.secret_references == (SecretReference(secret_id="db-password", provider="local-keychain"),)
    with pytest.raises((ValidationError, ValueError)):
        AuthoredDeploymentDefinition.model_validate(
            {
                **definition.model_dump(),
                "instance_values": {"database_password": "do-not-store"},
            }
        )
    with pytest.raises((ValidationError, ValueError)):
        AuthoredDeploymentDefinition.model_validate({**definition.model_dump(), "generation_id": "gen-1"})
    selection = validate_selection(definition, release, bundles)
    with pytest.raises((ValidationError, ValueError)):
        ResolvedLock(
            definition_id=definition.definition_id,
            product_identity=selection.identity,
            distribution_release_ref=release.reference,
            secret_version="v2",  # type: ignore[call-arg]
        )
    with pytest.raises((ValidationError, ValueError)):
        definition.model_copy(update={"instance_values": {"database_password": "do-not-store"}})
    with pytest.raises((ValidationError, ValueError)):
        definition.model_copy(update={"generation_id": "gen-1"})
    smuggled = definition.model_copy()
    object.__setattr__(smuggled, "instance_values", {"generation_id": "gen-1"})
    with pytest.raises((ValidationError, ValueError)):
        validate_selection(smuggled, release, bundles)
    for unsafe_key in (
        "dbPassword",
        "database-password",
        "secretVersion",
        "secret-version",
        "generationId",
        "hostState",
        "APIKey",
    ):
        with pytest.raises((ValidationError, ValueError)):
            AuthoredDeploymentDefinition.model_validate(
                {
                    **definition.model_dump(mode="python"),
                    "instance_values": {"nested": {unsafe_key: "supplied-secret"}},
                }
            )


def test_provenance_paths_are_posix_relative_logical_paths() -> None:
    valid = {
        "owner": "instance",
        "reference": OwnerQualifiedReference(owner="instance", id="value"),
        "source": "definition",
        "value_digest": _D1,
    }
    for path in ("", ".", "..", "./value", "value/./child", "value/../child", "/absolute", "../up", "C:/drive", "a\\b", "a//b", "a/\x00b"):
        with pytest.raises((ValidationError, ValueError)):
            ProvenanceRecord(path=path, **valid)
    assert ProvenanceRecord(path="instance/value", **valid).path == "instance/value"


def test_sealed_release_authority_cannot_be_replaced() -> None:
    with pytest.raises((ValidationError, ValueError)):
        SealedReleaseReference(schema="made.up.builder/v1", artifact_id="sha256:" + "a" * 64)
    with pytest.raises((ValidationError, ValueError)):
        SealedReleaseReference(schema=SEALED_RELEASE_SCHEMA, artifact_id="not-a-sha256-digest")


def test_unknown_fields_and_bad_digests_are_refused() -> None:
    with pytest.raises((ValidationError, ValueError)):
        DistributionRelease(distribution_id="x", version="1", made_up=True)  # type: ignore[call-arg]
    with pytest.raises((ValidationError, ValueError)):
        LocalAssetDescriptor(asset_id="local", kind="policy", digest="not-a-digest")
    with pytest.raises((ValidationError, ValueError)):
        TypedOverrideInput(slot_id="db_password", value_type="string", value="secret")
