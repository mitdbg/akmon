from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import shutil

import pytest
import yaml

from okg.substrate.contracts.deployment_product import (
    AuthoredDeploymentDefinition,
    CompatibilityDecision,
    CompatibilityMetadata,
    DeploymentLockError,
    DistributionRelease,
    OwnerQualifiedReference,
    PlanAction,
    ProductBundle,
    TypedArtifactIdentity,
    canonical_lock_bytes,
    canonical_json_bytes,
    make_action_plan,
    require_plan_before_apply,
    resolve_product_lock,
    sha256_digest,
    validate_selection,
    verify_plan_before_apply,
)


FIXTURES = Path(__file__).parents[2] / "fixtures" / "deployment_products" / "lock_determinism"


def _load(root: Path, *, reverse: bool = False):
    raw = yaml.safe_load((root / "product.yaml").read_text())
    if reverse:
        raw = deepcopy(raw)
        raw["product_bundles"] = list(reversed(raw["product_bundles"]))
        raw["definition"]["bundles"] = list(reversed(raw["definition"]["bundles"]))
        raw["definition"]["instance_values"] = dict(
            reversed(tuple(raw["definition"]["instance_values"].items()))
        )
    release = DistributionRelease.model_validate(raw["distribution_release"])
    bundles = tuple(ProductBundle.model_validate(item) for item in raw["product_bundles"])
    definition = AuthoredDeploymentDefinition.model_validate(raw["definition"])
    return release, bundles, definition


def _resolve(root: Path, *, reverse: bool = False):
    release, bundles, definition = _load(root, reverse=reverse)
    selection = validate_selection(definition, release, bundles)
    package = TypedArtifactIdentity(
        owner="pypi",
        id="okg-extension-search",
        kind="extension-package",
        version="2.4.0",
        digest="sha256:" + "9" * 64,
    )
    environments = (
        TypedArtifactIdentity(
            owner="python",
            id="cp312-linux-x86_64",
            kind="transitive-environment",
            version="2026.08",
            digest="sha256:" + "a" * 64,
        ),
        TypedArtifactIdentity(
            owner="postgres",
            id="pg17",
            kind="transitive-environment",
            version="17.2",
            digest="sha256:" + "b" * 64,
        ),
    )
    decision = CompatibilityDecision(
        subject=OwnerQualifiedReference(
            owner="distribution",
            id="okg-standard",
            kind="distribution-release",
            version="1.0",
            digest=release.release_digest,
        ),
        decision="compatible",
        reason="contract-version-compatible",
    )
    lock = resolve_product_lock(
        selection,
        package_identities=(package,),
        environment_identities=tuple(reversed(environments)) if reverse else environments,
        compatibility_decisions=(decision,),
    )
    return definition, lock


def _copied_roots(tmp_path: Path) -> tuple[Path, Path]:
    first = tmp_path / "checkout-a" / "product"
    second = tmp_path / "checkout-b" / "nested" / "product"
    shutil.copytree(FIXTURES, first)
    shutil.copytree(FIXTURES, second)
    return first, second


def test_equivalent_checkouts_and_unordered_inputs_emit_byte_identical_secret_free_locks(
    tmp_path: Path,
) -> None:
    first, second = _copied_roots(tmp_path)
    definition_a, lock_a = _resolve(first)
    definition_b, lock_b = _resolve(second, reverse=True)

    bytes_a = canonical_lock_bytes(lock_a)
    bytes_b = canonical_lock_bytes(lock_b)
    assert bytes_a == bytes_b
    assert lock_a.composition_digest == lock_b.composition_digest
    assert lock_a.definition_digest == lock_b.definition_digest
    assert definition_a.bundle_refs != definition_b.bundle_refs
    assert str(first).encode() not in bytes_a
    assert str(second).encode() not in bytes_a

    # Secret material and secret-provider version metadata are apply-time
    # inputs. Rotating them leaves the product identity and lock untouched.
    secret_material_v1 = {"database-password": b"supplied-secret-v1", "secret_version": "7"}
    secret_material_v2 = {"database-password": b"rotated-secret-v2", "secret_version": "8"}
    assert secret_material_v1 != secret_material_v2
    for forbidden in (
        b"supplied-secret-v1",
        b"rotated-secret-v2",
        b"secret_version",
        b"database-password",
    ):
        assert forbidden not in bytes_a

    assert lock_a.contract_version == "v1"
    assert lock_a.api_version == "okg.deployment_product.api/v1"
    assert lock_a.distribution_release_ref.digest
    assert all(item.digest for item in lock_a.bundle_refs)
    assert all(item.digest for item in lock_a.asset_refs)
    assert {item.kind for item in lock_a.environment_identities} == {"transitive-environment"}
    assert lock_a.package_identities[0].kind == "extension-package"
    assert lock_a.local_asset_digests == {
        "acme/instance-policy": "sha256:" + "7" * 64,
    }


def test_lock_provenance_enumerates_effective_owners_assets_digests_and_override_slots(
    tmp_path: Path,
) -> None:
    first, _ = _copied_roots(tmp_path)
    _, lock = _resolve(first)
    by_path = {item.path: item for item in lock.provenance}

    assert {
        "distribution_release",
        "distribution_release.assets.base-policy",
        "bundles.core",
        "bundles.core.assets.ontology",
        "bundles.search",
        "bundles.search.assets.search-schema",
        "connection_references.primary-database",
        "local_assets.instance-policy",
        "overrides.retention_days",
        "instance_values.region",
        "instance_values.replicas",
        "package_identities/pypi/extension-package/okg-extension-search",
        "environment_identities/python/transitive-environment/cp312-linux-x86_64",
        "environment_identities/postgres/transitive-environment/pg17",
        "compatibility_decisions/distribution/distribution-release/okg-standard",
    } <= set(by_path)
    for record in by_path.values():
        assert record.owner
        assert record.reference.owner == record.owner
        assert record.reference.ref
        assert record.reference.digest
        assert record.value_digest == record.reference.digest
    override = by_path["overrides.retention_days"]
    assert override.source == "override"
    assert override.reference.kind == "override_slot"
    assert lock.override_provenance == (override,)


def test_plan_verification_refuses_definition_artifact_expiry_and_plan_drift_before_mutation(
    tmp_path: Path,
) -> None:
    first, _ = _copied_roots(tmp_path)
    definition, lock = _resolve(first)
    artifact_bytes = {
        "cache/artifact-alpha.txt": (first / "artifact-alpha.txt").read_bytes(),
        "cache/artifact-beta.txt": (first / "artifact-beta.txt").read_bytes(),
    }
    artifact_digests = {
        logical_id: sha256_digest(content)
        for logical_id, content in artifact_bytes.items()
    }
    plan = make_action_plan(
        lock,
        expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        artifact_digests=artifact_digests,
        actions=(
            PlanAction(
                action_id="install-product",
                kind="apply-sealed-release",
                arguments={"artifact": "cache/artifact-alpha.txt"},
            ),
        ),
    )
    now = datetime(2029, 1, 1, tzinfo=timezone.utc)
    mutated: list[str] = []

    authorization = require_plan_before_apply(
        plan,
        definition=definition,
        artifacts=artifact_bytes,
        now=now,
        expected_plan_digest=plan.plan_digest,
    )
    mutated.append(authorization.composition_digest)
    assert mutated == [lock.composition_digest]

    drifted_definition = definition.model_copy(
        update={"instance_values": {"region": "us-east", "replicas": 3}}
    )
    cases = (
        ("definition_digest_mismatch", drifted_definition, artifact_bytes, now, plan, plan.plan_digest),
        (
            "artifact_digest_mismatch",
            definition,
            {**artifact_bytes, "cache/artifact-beta.txt": b"corrupt cache"},
            now,
            plan,
            plan.plan_digest,
        ),
        (
            "artifact_missing",
            definition,
            {"cache/artifact-alpha.txt": artifact_bytes["cache/artifact-alpha.txt"]},
            now,
            plan,
            plan.plan_digest,
        ),
        (
            "artifact_invalid",
            definition,
            {**artifact_bytes, "cache/artifact-beta.txt": "not bytes"},
            now,
            plan,
            plan.plan_digest,
        ),
        (
            "plan_expired",
            definition,
            artifact_bytes,
            datetime(2030, 1, 1, tzinfo=timezone.utc),
            plan,
            plan.plan_digest,
        ),
        (
            "plan_digest_mismatch",
            definition,
            artifact_bytes,
            now,
            replace(plan, actions=()),
            plan.plan_digest,
        ),
    )
    for expected_code, current_definition, artifacts, current_time, candidate, expected_digest in cases:
        before = list(mutated)
        verification = verify_plan_before_apply(
            candidate,
            definition=current_definition,
            artifacts=artifacts,
            now=current_time,
            expected_plan_digest=expected_digest,
        )
        assert not verification.ok
        assert not verification.mutation_allowed
        diagnostic = next(item for item in verification.diagnostics if item.code == expected_code)
        assert diagnostic.fix_hint
        assert diagnostic.expected is not None or expected_code == "artifact_missing"
        with pytest.raises(DeploymentLockError) as exc_info:
            require_plan_before_apply(
                candidate,
                definition=current_definition,
                artifacts=artifacts,
                now=current_time,
                expected_plan_digest=expected_digest,
            )
        assert expected_code in {item.code for item in exc_info.value.diagnostics}
        assert mutated == before

    foreign = replace(
        plan,
        schema_version="foreign.plan/v9",
        api_version="foreign.api/v9",
        plan_digest="sha256:" + "0" * 64,
    )
    foreign = replace(
        foreign,
        plan_digest=sha256_digest(
            canonical_json_bytes(foreign.payload(include_plan_digest=False))
        ),
    )
    verification = verify_plan_before_apply(
        foreign,
        definition=definition,
        artifacts=artifact_bytes,
        now=now,
        expected_plan_digest=foreign.plan_digest,
    )
    assert not verification.mutation_allowed
    assert {item.code for item in verification.diagnostics} >= {
        "plan_schema_unsupported",
        "plan_api_unsupported",
    }
    with pytest.raises(DeploymentLockError):
        require_plan_before_apply(
            foreign,
            definition=definition,
            artifacts=artifact_bytes,
            now=now,
            expected_plan_digest=foreign.plan_digest,
        )


def test_lock_refuses_digest_collisions_and_nonlogical_plan_artifact_ids(tmp_path: Path) -> None:
    first, _ = _copied_roots(tmp_path)
    release, bundles, definition = _load(first)
    selection = validate_selection(definition, release, bundles)
    collision = OwnerQualifiedReference(
        owner="distribution",
        id="base-policy",
        kind="policy",
        version="1",
        digest="sha256:" + "f" * 64,
    )
    with pytest.raises(DeploymentLockError) as exc_info:
        resolve_product_lock(selection, asset_refs=(collision,))
    assert exc_info.value.code == "identity_digest_collision"

    _, lock = _resolve(first)
    for bad in ("/absolute/cache.bin", "../cache.bin", "cache/../cache.bin", "C:/cache.bin"):
        with pytest.raises(ValueError):
            make_action_plan(
                lock,
                expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
                artifact_digests={bad: "sha256:" + "1" * 64},
            )
    with pytest.raises(ValueError, match="not a registered reference or digest"):
        PlanAction(
            action_id="unsafe-action",
            kind="apply-sealed-release",
            arguments={"payload": "SECRET-VALUE"},
        )
    with pytest.raises(ValueError, match="secret-material vocabulary"):
        PlanAction(
            action_id="unsafe-action",
            kind="apply-sealed-release",
            arguments={"artifact": "SECRET-VALUE"},
        )


def test_compatibility_metadata_is_resolved_or_refused_and_reason_text_is_closed(
    tmp_path: Path,
) -> None:
    first, _ = _copied_roots(tmp_path)
    release, bundles, definition = _load(first)
    compatible = release.model_copy(
        update={
            "compatibility": CompatibilityMetadata(
                contract_versions=("v1",),
                capabilities=("semantic-catalog",),
            )
        }
    )
    lock = resolve_product_lock(validate_selection(definition, compatible, bundles))
    assert {(item.subject.kind, item.decision, item.reason) for item in lock.compatibility_decisions} >= {
        ("contract-version", "compatible", "contract-version-compatible"),
        ("capability", "required", "capability-declared"),
    }
    assert any(
        item.path.endswith("okg-standard:capability:semantic-catalog")
        and item.source == "compatibility"
        for item in lock.provenance
    )

    incompatible = release.model_copy(
        update={
            "compatibility": CompatibilityMetadata(
                contract_versions=("v9",),
                capabilities=("semantic-catalog",),
            )
        }
    )
    with pytest.raises(DeploymentLockError) as exc_info:
        resolve_product_lock(validate_selection(definition, incompatible, bundles))
    assert exc_info.value.code == "compatibility_contract_unsupported"

    with pytest.raises(ValueError):
        CompatibilityDecision(
            subject=OwnerQualifiedReference(
                owner="distribution",
                id="okg-standard",
                kind="distribution-release",
                digest=release.release_digest,
            ),
            decision="compatible",
            reason="SECRET-VALUE",
        )
