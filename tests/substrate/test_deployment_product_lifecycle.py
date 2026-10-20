from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import stat
from types import SimpleNamespace
from typing import Callable, cast

import pytest

import okg.substrate.deployment_product_lifecycle as lifecycle
from okg.substrate.contracts.deployment_product.lock import (
    DeploymentProductPlan,
    PlanAction,
    authored_definition_digest,
    canonical_json_bytes,
    canonical_lock_bytes,
    composition_digest,
    sha256_digest,
)
from okg.substrate.contracts.deployment_product.models import (
    AuthoredDeploymentDefinition,
    CompatibilityMetadata,
    DistributionRelease,
    DistributionReleaseReference,
    OwnerQualifiedReference,
    ProductBundle,
    ProductBundleReference,
    ProductIdentity,
    ResolvedLock,
    SealedReleaseReference,
    TypedArtifactIdentity,
)
from okg.substrate.contracts.deployment_product.typed_assets import TypedAsset
from okg.substrate.deployment_product_lifecycle import (
    ContainedArtifactReader,
    ProductApplyRequest,
    ProductLifecycleError,
    admit_product_apply_request,
    approval_digest,
    project_product_operation_status,
    render_product_operation_status,
    save_product_promotion_approval,
    load_product_apply_request,
    load_product_plan,
    package_selection_from_mapping,
    package_selection_from_verified,
    save_product_apply_request,
    save_product_plan,
    verify_package_selection,
    verify_product_request_package_input,
)
from okg.substrate.deployment_promotion import (
    PromotionApproval,
    create_product_promotion_approval,
)
from okg.substrate.distributions import (
    AssetLocator,
    DistributionObject,
    DistributionPackageError,
    DistributionPackageManifest,
    DistributionPackageProvenance,
    DistributionPackageSource,
    PackagedAsset,
    build_distribution_package,
    canonical_bundle_digest,
    canonical_release_digest,
    verify_distribution_package,
)


ZERO = "sha256:" + "0" * 64


@dataclass(frozen=True)
class _RequestCase:
    request: ProductApplyRequest
    plan: DeploymentProductPlan
    payloads: dict[str, bytes]


def _seal_lock(lock: ResolvedLock) -> ResolvedLock:
    return cast(
        ResolvedLock,
        lock.model_copy(update={"composition_digest": composition_digest(lock)}),
    )


def _make_plan(
    lock: ResolvedLock,
    artifact_digests: tuple[tuple[str, str], ...],
) -> DeploymentProductPlan:
    lock_digest = sha256_digest(canonical_lock_bytes(lock))
    draft = DeploymentProductPlan(
        lock=lock,
        lock_digest=lock_digest,
        definition_digest=lock.definition_digest,
        artifact_digests=tuple(sorted(artifact_digests)),
        expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        actions=(),
        plan_digest=ZERO,
    )
    return replace(
        draft,
        plan_digest=sha256_digest(
            canonical_json_bytes(draft.payload(include_plan_digest=False))
        ),
    )


def _generic_package(
    tmp_path: Path,
    *,
    distribution_id: str,
    bundle_id: str,
) -> tuple[Path, dict[str, bytes]]:
    root = tmp_path / distribution_id / "source"
    output = tmp_path / distribution_id / "packages"
    root.mkdir(parents=True)
    output.mkdir()
    payloads = {
        "common": f"{distribution_id}:common\n".encode(),
        "selected": f"{bundle_id}:selected\n".encode(),
        "unselected": b"other-bundle-only\n",
    }

    def asset(asset_id: str, payload: bytes) -> TypedAsset:
        return TypedAsset(
            owner=distribution_id,
            asset_id=asset_id,
            kind="schema",
            plane="semantic-catalog",
            version="v1",
            digest=sha256_digest(payload),
            schema="example.schema/v1",
            merge_identity=asset_id,
            merge_rule="refuse",
            compatibility={"contract_versions": ["v1"]},
            provenance={
                "source": "package",
                "repository_url": "https://example.test/distribution",
                "revision": "a" * 40,
                "artifact_ref": f"{distribution_id}:{asset_id}",
            },
        )

    assets = {name: asset(name, payload) for name, payload in payloads.items()}

    def ref(name: str) -> OwnerQualifiedReference:
        item = assets[name]
        return OwnerQualifiedReference(
            owner=item.owner,
            id=item.asset_id,
            kind=item.kind,
            version=item.version,
            digest=item.digest,
        )

    def bundle(name: str, refs: tuple[OwnerQualifiedReference, ...]) -> ProductBundle:
        draft = ProductBundle(
            bundle_id=name,
            version="v1",
            distribution_id=distribution_id,
            distribution_version="v1",
            bundle_digest=ZERO,
            assets=refs,
            compatibility=CompatibilityMetadata(contract_versions=("v1",)),
        )
        return cast(
            ProductBundle,
            draft.model_copy(update={"bundle_digest": canonical_bundle_digest(draft)}),
        )

    selected_bundle = bundle(bundle_id, (ref("selected"),))
    other_bundle = bundle("other", (ref("unselected"),))
    release = DistributionRelease(
        distribution_id=distribution_id,
        version="v1",
        origin="external",
        release_digest=ZERO,
        assets=(ref("common"),),
        bundle_ids=(selected_bundle.bundle_id, other_bundle.bundle_id),
        compatibility=CompatibilityMetadata(contract_versions=("v1",)),
    )
    objects: list[DistributionObject] = []
    packaged_assets: list[PackagedAsset] = []
    sources: dict[str, DistributionPackageSource] = {}
    for name, payload in payloads.items():
        path = root / f"{name}.bin"
        path.write_bytes(payload)
        digest = sha256_digest(payload)
        objects.append(
            DistributionObject(
                digest=digest,
                size=len(payload),
                media_type="application/octet-stream",
            )
        )
        packaged_assets.append(
            PackagedAsset(
                asset=assets[name],
                locator=AssetLocator(object_digest=digest),
                uncompressed_size=len(payload),
            )
        )
        sources[digest] = DistributionPackageSource(root=root, path=path)
    package_identity = TypedArtifactIdentity(
        owner=distribution_id,
        id="package",
        kind="package",
        version="v1",
        digest=sha256_digest(b"package-identity"),
    )
    draft_manifest = DistributionPackageManifest(
        distribution_release=release,
        bundles=(selected_bundle, other_bundle),
        assets=tuple(packaged_assets),
        objects=tuple(objects),
        package_identities=(package_identity,),
        provenance=DistributionPackageProvenance(
            repository_url="https://example.test/distribution",
            revision="b" * 40,
            builder="generic-test-builder",
        ),
    )
    release = cast(
        DistributionRelease,
        release.model_copy(
            update={"release_digest": canonical_release_digest(draft_manifest)}
        ),
    )
    manifest = cast(
        DistributionPackageManifest,
        draft_manifest.model_copy(update={"distribution_release": release}),
    )
    return build_distribution_package(manifest, sources, output), payloads


def _request_case(
    tmp_path: Path,
    *,
    distribution_id: str = "generic-one",
    bundle_id: str = "blue",
) -> _RequestCase:
    package_path, payloads = _generic_package(
        tmp_path,
        distribution_id=distribution_id,
        bundle_id=bundle_id,
    )
    verified = verify_distribution_package(package_path)
    try:
        selection = package_selection_from_verified(verified, bundle_id=bundle_id)
    finally:
        verified.close_runtime_authority()
    bindings = tuple(
        {
            "logical_id": f"package/{item['id']}.bin",
            "asset_ref": f"{item['owner']}:{item['id']}",
        }
        for item in selection.assets
    )
    artifact_root = tmp_path / distribution_id / "artifact-root"
    artifact_root.mkdir()
    artifact_digests: dict[str, str] = {}
    for binding in bindings:
        asset_id = binding["asset_ref"].split(":", 1)[1]
        payload = payloads[asset_id]
        target = artifact_root / binding["logical_id"]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        artifact_digests[binding["logical_id"]] = sha256_digest(payload)
    local_id = "local/settings.yaml"
    local_payload = b"local-settings\n"
    local_target = artifact_root / local_id
    local_target.parent.mkdir(parents=True, exist_ok=True)
    local_target.write_bytes(local_payload)
    artifact_digests[local_id] = sha256_digest(local_payload)

    release = selection.distribution_release
    selected_bundle = selection.bundle
    lock = _seal_lock(
        ResolvedLock(
            definition_id="definition-one",
            definition_digest=ZERO,
            product_identity=ProductIdentity(
                distribution_id=release["distribution_id"],
                distribution_version=release["version"],
                bundle_ids=(selected_bundle["bundle_id"],),
                definition_id="definition-one",
                origin="external",
            ),
            distribution_release_ref=DistributionReleaseReference(
                owner=release["owner"],
                distribution_id=release["distribution_id"],
                version=release["version"],
                digest=release["release_digest"],
            ),
            bundle_refs=(
                ProductBundleReference(
                    owner=selected_bundle["owner"],
                    bundle_id=selected_bundle["bundle_id"],
                    version=selected_bundle["version"],
                    digest=selected_bundle["bundle_digest"],
                ),
            ),
            asset_refs=tuple(
                OwnerQualifiedReference(
                    owner=item["owner"],
                    id=item["id"],
                    kind=item["kind"],
                    version=item["version"],
                    digest=item["asset_digest"],
                )
                for item in selection.assets
            ),
            package_refs=tuple(
                TypedArtifactIdentity(
                    owner=item["owner"],
                    id=item["id"],
                    kind=item["kind"],
                    version=item["version"],
                    digest=item["digest"],
                )
                for item in selection.package_identities
            ),
            local_asset_digests={local_id: sha256_digest(local_payload)},
            composition_digest=ZERO,
        )
    )
    plan = _make_plan(lock, tuple(artifact_digests.items()))
    plan_path = tmp_path / distribution_id / "plan.json"
    save_product_plan(plan_path, plan)
    request = ProductApplyRequest(
        staging_environment_id="staging",
        target_environment_id="target",
        idempotency_key="promotion-1",
        authored_definition_path=tmp_path / distribution_id / "definition.json",
        product_plan_path=plan_path,
        expected_product_plan_digest=plan.plan_digest,
        distribution_package_path=package_path,
        package_selection=selection,
        package_selection_digest=selection.digest,
        package_artifact_bindings=bindings,
        artifact_root=artifact_root,
        sealed_release=SealedReleaseReference(
            schema="okg.sealed_release/v1",
            artifact_id=ZERO,
            input_id=ZERO,
            definition_digest=ZERO,
            artifact_root="artifacts/candidate",
        ),
        promotion_approval_path=tmp_path / distribution_id / "approval.json",
        promotion_approval_digest=ZERO,
        requested_transition="first_install",
        logical_artifact_digests=plan.artifact_digests,
    )
    return _RequestCase(request=request, plan=plan, payloads=payloads)


def _admission_case(tmp_path: Path) -> _RequestCase:
    case = _request_case(tmp_path)
    definition = AuthoredDeploymentDefinition(
        definition_id=case.plan.lock.definition_id,
        distribution_release_ref=case.plan.lock.distribution_release_ref,
        bundles=case.plan.lock.bundle_refs,
    )
    definition_digest = authored_definition_digest(definition)
    lock = _seal_lock(
        case.plan.lock.model_copy(update={"definition_digest": definition_digest})
    )
    lock_digest = sha256_digest(canonical_lock_bytes(lock))
    sealed = case.request.sealed_release
    action = PlanAction(
        action_id="apply-product",
        kind="apply_deployment_instance",
        arguments={
            "deployment_id": case.request.target_environment_id,
            "definition_id": lock.definition_id,
            "lock_digest": lock_digest,
            "sealed_release_digest": sealed.artifact_id,
        },
    )
    draft = DeploymentProductPlan(
        lock=lock,
        lock_digest=lock_digest,
        definition_digest=definition_digest,
        artifact_digests=case.plan.artifact_digests,
        expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        actions=(action,),
        plan_digest=ZERO,
    )
    plan = replace(
        draft,
        plan_digest=sha256_digest(
            canonical_json_bytes(draft.payload(include_plan_digest=False))
        ),
    )
    definition_path = Path(case.request.authored_definition_path)
    definition_path.write_text(
        json.dumps(definition.model_dump(mode="json", by_alias=True)),
        encoding="utf-8",
    )
    plan_path = Path(case.request.product_plan_path).with_name("admitted-plan.json")
    save_product_plan(plan_path, plan)
    request = replace(
        case.request,
        product_plan_path=plan_path,
        expected_product_plan_digest=plan.plan_digest,
        logical_artifact_digests=plan.artifact_digests,
    )
    base = PromotionApproval(
        promotion_id=request.idempotency_key,
        artifact_id=sealed.artifact_id,
        artifact_input_id=sealed.input_id,
        staging_environment_id=request.staging_environment_id,
        production_environment_id=request.target_environment_id,
        staging_descriptor_digest=ZERO,
        production_descriptor_digest=ZERO,
        definition_digest=sealed.definition_digest,
        production_definition_digest=ZERO,
        staging_evidence_digest=ZERO,
        supervision_evidence_digest=ZERO,
        staging_service_plan_digest=ZERO,
        production_service_plan_digest=ZERO,
        production_resources_digest=ZERO,
        approved_at="2026-08-30T07:00:00Z",
        expires_at="2026-08-30T08:00:00Z",
        approved_by="opérator",
    )
    approval = create_product_promotion_approval(
        base=base,
        transition_kind="first_install",
        expected_previous_artifact_id=None,
        request_digest=request.request_digest,
        product_plan_digest=plan.plan_digest,
        lock_digest=plan.lock_digest,
        composition_digest=plan.lock.composition_digest,
    )
    save_product_promotion_approval(request.promotion_approval_path, approval)
    request = replace(request, promotion_approval_digest=approval_digest(approval))
    return _RequestCase(request=request, plan=plan, payloads=case.payloads)


def _admitted_state(case: _RequestCase, admission: dict[str, object]) -> dict[str, object]:
    return {
        "schema": "okg.deployment_promotion/v1",
        "product_lifecycle_mode": "okg.deployment_product.admission_only/v1",
        "controller_status": "unavailable",
        "environment_id": "target",
        "promotion_id": case.request.idempotency_key,
        "approval_digest": admission["approval_digest"],
        "artifact_id": ZERO,
        "artifact_input_id": ZERO,
        "staging_descriptor_digest": ZERO,
        "production_descriptor_digest": ZERO,
        "supervision_evidence_digest": ZERO,
        "production_service_plan_digest": ZERO,
        "production_resources_digest": ZERO,
        "product_admission": dict(admission),
        **{key: value for key, value in admission.items() if key != "schema"},
        "release_observation": {
            "transition_kind": "first_install",
            "active_artifact_id": None,
            "previous_artifact_id": None,
            "active_absent": True,
            "previous_absent": True,
        },
        "status": "product_admitted",
        "completed_transitions": [],
        "transition_results": {},
        "operation_intents": {},
        "mutation_boundary": "none",
        "writers_fenced": False,
        "database_changed": False,
        "updated_at": "2026-08-30T07:30:00+00:00",
    }


class TestPackageInputCore:
    @pytest.fixture(autouse=True)
    def _record_pact_scope(
        self,
        record_property: Callable[[str, object], None],
    ) -> None:
        record_property(
            "requirements",
            "req.lifecycle.package-input-core acc.lifecycle.package-input-core",
        )

    @pytest.mark.parametrize(
        ("distribution_id", "bundle_id"),
        (("generic-one", "blue"), ("renamed-product", "green")),
    )
    def test_selected_closure_is_exact_and_name_neutral(
        self,
        tmp_path: Path,
        distribution_id: str,
        bundle_id: str,
    ) -> None:
        case = _request_case(
            tmp_path,
            distribution_id=distribution_id,
            bundle_id=bundle_id,
        )

        observed = verify_product_request_package_input(
            case.request,
            plan=case.plan,
        )

        assert observed.bundle["bundle_id"] == bundle_id
        assert {item["id"] for item in observed.assets} == {"common", "selected"}
        assert "unselected" not in {item["id"] for item in observed.assets}
        assert len(observed.objects) == 2

    def test_request_identity_excludes_locators_and_round_trips(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        moved = replace(
            case.request,
            authored_definition_path=tmp_path / "elsewhere" / "definition.json",
            promotion_approval_path=tmp_path / "elsewhere" / "approval.json",
        )
        request_path = tmp_path / "request.json"

        save_product_apply_request(request_path, case.request)

        assert (
            case.request.request_digest
            == "sha256:7cf500b1a5cef39c2bd08791076b57ef309774613da492afcde2e3c48f5aa84c"
        )
        assert case.request.request_digest == moved.request_digest
        assert case.request.to_mapping() != moved.to_mapping()
        loaded = load_product_apply_request(request_path)
        assert loaded.to_mapping() == case.request.to_mapping()
        assert loaded.request_digest == case.request.request_digest
        assert load_product_plan(case.request.product_plan_path) == case.plan

    def test_duplicate_json_key_refuses_without_replacement(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        target = tmp_path / "request.json"
        canonical = json.dumps(
            case.request.to_mapping(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        duplicate = canonical.replace(
            '"target_environment_id":"target"',
            '"target_environment_id":"other","target_environment_id":"target"',
        )
        assert duplicate != canonical
        target.write_text(duplicate + "\n", encoding="utf-8")

        with pytest.raises(ProductLifecycleError, match="duplicate object key"):
            load_product_apply_request(target)
        with pytest.raises(ProductLifecycleError, match="duplicate object key"):
            save_product_apply_request(target, case.request)

        assert target.read_text(encoding="utf-8") == duplicate + "\n"

    def test_request_digest_binds_the_exact_package_to_artifact_mapping(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        bindings = tuple(case.request.package_artifact_bindings)
        assert len(bindings) == 2
        swapped = (
            {
                "logical_id": bindings[1].logical_id,
                "asset_ref": bindings[0].asset_ref,
            },
            {
                "logical_id": bindings[0].logical_id,
                "asset_ref": bindings[1].asset_ref,
            },
        )

        changed = replace(case.request, package_artifact_bindings=swapped)

        assert changed.request_digest != case.request.request_digest

    def test_bindings_are_closed_nonempty_and_bijective(self, tmp_path: Path) -> None:
        case = _request_case(tmp_path)
        bindings = [
            item.to_mapping() for item in case.request.package_artifact_bindings
        ]

        with pytest.raises(ProductLifecycleError, match="non-empty"):
            replace(case.request, package_artifact_bindings=())
        with pytest.raises(ProductLifecycleError, match="logical IDs must be unique"):
            replace(
                case.request,
                package_artifact_bindings=(bindings[0], bindings[0]),
            )
        with pytest.raises(ProductLifecycleError, match="differ from selected assets"):
            replace(case.request, package_artifact_bindings=bindings[:-1])
        with pytest.raises(ProductLifecycleError, match="fields changed"):
            replace(
                case.request,
                package_artifact_bindings=({**bindings[0], "extra": "no"},),
            )

    def test_partition_refuses_overlap_and_unclassified_artifacts(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        package_id = case.request.package_artifact_bindings[0].logical_id
        package_digest = dict(case.plan.artifact_digests)[package_id]
        overlap_lock = _seal_lock(
            case.plan.lock.model_copy(
                update={
                    "local_asset_digests": {
                        **dict(case.plan.lock.local_asset_digests),
                        package_id: package_digest,
                    }
                }
            )
        )
        overlap_plan = _make_plan(overlap_lock, case.plan.artifact_digests)
        overlap_request = replace(
            case.request,
            expected_product_plan_digest=overlap_plan.plan_digest,
        )

        with pytest.raises(ProductLifecycleError, match="partitions overlap"):
            verify_product_request_package_input(
                overlap_request,
                plan=overlap_plan,
            )

        unclassified = (*case.plan.artifact_digests, ("extra/unowned.bin", ZERO))
        unclassified_plan = _make_plan(case.plan.lock, unclassified)
        unclassified_request = replace(
            case.request,
            expected_product_plan_digest=unclassified_plan.plan_digest,
            logical_artifact_digests=unclassified_plan.artifact_digests,
        )
        with pytest.raises(ProductLifecycleError, match="complete package/local"):
            verify_product_request_package_input(
                unclassified_request,
                plan=unclassified_plan,
            )

    def test_supplied_typed_plan_is_canonically_revalidated(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        tampered = replace(
            case.plan,
            expires_at=datetime(2042, 1, 1, tzinfo=timezone.utc),
        )
        assert tampered.plan_digest == case.plan.plan_digest

        with pytest.raises(ProductLifecycleError, match="plan digest"):
            verify_product_request_package_input(case.request, plan=tampered)

    def test_verification_cannot_override_the_claimed_artifact_root(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        claimed_root = Path(case.request.artifact_root)
        alternate_root = tmp_path / "alternate-valid-root"
        shutil.copytree(claimed_root, alternate_root)
        binding = case.request.package_artifact_bindings[0]
        (claimed_root / binding.logical_id).write_bytes(b"corrupt-claimed-root\n")

        with pytest.raises(ProductLifecycleError, match="contained artifact size"):
            verify_product_request_package_input(case.request, plan=case.plan)

        assert (alternate_root / binding.logical_id).read_bytes() != (
            claimed_root / binding.logical_id
        ).read_bytes()

    def test_nested_selection_tamper_refuses_against_the_lock(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        raw = case.request.package_selection.to_mapping()
        raw["bundle"]["bundle_digest"] = ZERO
        tampered = package_selection_from_mapping(raw)
        request = replace(
            case.request,
            package_selection=tampered,
            package_selection_digest=tampered.digest,
        )

        with pytest.raises(
            ProductLifecycleError, match="differs from the resolved lock"
        ):
            verify_product_request_package_input(request, plan=case.plan)

    def test_extra_lock_bundle_refuses_exact_selection(self, tmp_path: Path) -> None:
        case = _request_case(tmp_path)
        identity = case.plan.lock.product_identity.model_copy(
            update={
                "bundle_ids": (
                    *case.plan.lock.product_identity.bundle_ids,
                    "unexpected",
                )
            }
        )
        extra_bundle = ProductBundleReference(
            owner=case.plan.lock.bundle_refs[0].owner,
            bundle_id="unexpected",
            version="v1",
            digest=ZERO,
        )
        lock = _seal_lock(
            case.plan.lock.model_copy(
                update={
                    "product_identity": identity,
                    "bundle_refs": (*case.plan.lock.bundle_refs, extra_bundle),
                }
            )
        )
        plan = _make_plan(lock, case.plan.artifact_digests)
        request = replace(
            case.request,
            expected_product_plan_digest=plan.plan_digest,
        )

        with pytest.raises(
            ProductLifecycleError, match="differs from the resolved lock"
        ):
            verify_product_request_package_input(request, plan=plan)

    def test_contained_byte_drift_refuses_before_persistence(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        binding = case.request.package_artifact_bindings[0]
        contained = Path(case.request.artifact_root) / binding.logical_id
        contained.write_bytes(b"changed-contained-bytes\n")
        target = tmp_path / "request.json"

        with pytest.raises(ProductLifecycleError, match="contained artifact size"):
            save_product_apply_request(target, case.request)

        assert not target.exists()

    def test_persistence_reverifies_an_identical_existing_request(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        target = tmp_path / "request.json"
        save_product_apply_request(target, case.request)
        before = target.read_bytes()
        binding = case.request.package_artifact_bindings[0]
        (Path(case.request.artifact_root) / binding.logical_id).write_bytes(b"drift\n")

        with pytest.raises(ProductLifecycleError, match="contained artifact size"):
            save_product_apply_request(target, case.request)

        assert target.read_bytes() == before

    def test_persistence_never_overwrites_a_concurrent_contender(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        case = _request_case(tmp_path)
        target = tmp_path / "request.json"
        contender = b'{"contender":true}\n'
        original_link = lifecycle.os.link

        def race_link(
            source: str | bytes | Path,
            destination: str | bytes | Path,
            *,
            src_dir_fd: int | None = None,
            dst_dir_fd: int | None = None,
            follow_symlinks: bool = True,
        ) -> None:
            contender_fd = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=dst_dir_fd,
            )
            try:
                os.write(contender_fd, contender)
            finally:
                os.close(contender_fd)
            original_link(
                source,
                destination,
                src_dir_fd=src_dir_fd,
                dst_dir_fd=dst_dir_fd,
                follow_symlinks=follow_symlinks,
            )

        monkeypatch.setattr(lifecycle.os, "link", race_link)

        with pytest.raises(ProductLifecycleError, match="refusing to replace"):
            save_product_apply_request(target, case.request)

        assert target.read_bytes() == contender

    def test_atomic_publish_refuses_a_swapped_target(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        case = _request_case(tmp_path)
        target = tmp_path / "request.json"
        attacker = b'{"attacker":true}\n'
        original_link = lifecycle.os.link

        def swap_after_link(
            source: str | bytes | Path,
            destination: str | bytes | Path,
            *,
            src_dir_fd: int | None = None,
            dst_dir_fd: int | None = None,
            follow_symlinks: bool = True,
        ) -> None:
            original_link(
                source,
                destination,
                src_dir_fd=src_dir_fd,
                dst_dir_fd=dst_dir_fd,
                follow_symlinks=follow_symlinks,
            )
            os.unlink(destination, dir_fd=dst_dir_fd)
            attacker_fd = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=dst_dir_fd,
            )
            try:
                os.write(attacker_fd, attacker)
            finally:
                os.close(attacker_fd)

        monkeypatch.setattr(lifecycle.os, "link", swap_after_link)

        with pytest.raises(ProductLifecycleError, match="identity changed"):
            save_product_apply_request(target, case.request)

        assert target.read_bytes() == attacker

    def test_publish_window_drift_removes_the_unconfirmed_request(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        case = _request_case(tmp_path)
        target = tmp_path / "request.json"
        package_path = Path(case.request.distribution_package_path)
        original_write = lifecycle._write_json_mapping

        def race_write(*args, **kwargs):
            publication = original_write(*args, **kwargs)
            package_path.write_bytes(package_path.read_bytes() + b"changed")
            return publication

        monkeypatch.setattr(lifecycle, "_write_json_mapping", race_write)

        with pytest.raises(ProductLifecycleError):
            save_product_apply_request(target, case.request)

        assert not target.exists()

    def test_post_link_durability_failure_removes_the_unconfirmed_request(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        case = _request_case(tmp_path)
        target = tmp_path / "request.json"
        original_fsync = lifecycle.os.fsync
        calls = 0

        def fail_directory_fsync(descriptor: int) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("injected directory durability failure")
            original_fsync(descriptor)

        monkeypatch.setattr(lifecycle.os, "fsync", fail_directory_fsync)

        with pytest.raises(ProductLifecycleError, match="could not save"):
            save_product_apply_request(target, case.request)

        assert not target.exists()

    def test_raw_confirmation_failure_removes_the_unconfirmed_request(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        case = _request_case(tmp_path)
        target = tmp_path / "request.json"
        original_reader = lifecycle.ContainedArtifactReader
        calls = 0

        def fail_second_reader(root: str | Path) -> ContainedArtifactReader:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("injected artifact-root race")
            return original_reader(root)

        monkeypatch.setattr(lifecycle, "ContainedArtifactReader", fail_second_reader)

        with pytest.raises(ProductLifecycleError, match="confirmation failed"):
            save_product_apply_request(target, case.request)

        assert calls == 2
        assert not target.exists()

    def test_persistence_requires_a_private_operator_directory(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        shared = tmp_path / "shared"
        shared.mkdir(mode=0o777)
        shared.chmod(0o777)
        target = shared / "request.json"

        with pytest.raises(
            ProductLifecycleError,
            match="operator-owned and non-writable",
        ):
            save_product_apply_request(target, case.request)

        assert not target.exists()

    def test_persistence_refuses_replaceable_parent_ancestry(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        replaceable = tmp_path / "replaceable"
        replaceable.mkdir()
        replaceable.chmod(0o777)
        private = replaceable / "operator"
        private.mkdir(mode=0o700)
        target = private / "request.json"

        with pytest.raises(ProductLifecycleError, match="permits replacement"):
            save_product_apply_request(target, case.request)

        assert not target.exists()

    def test_untrusted_sticky_directory_owner_cannot_authorize_ancestry(
        self,
    ) -> None:
        operator = os.geteuid()
        untrusted = operator + 1 if operator != 0 else 1
        ancestor = SimpleNamespace(
            st_mode=stat.S_IFDIR | stat.S_ISVTX | 0o777,
            st_uid=untrusted,
        )
        child = SimpleNamespace(st_mode=stat.S_IFDIR | 0o700, st_uid=operator)

        assert not lifecycle._ancestor_protects_operator_child(ancestor, child)

    def test_repeated_lock_refusal_does_not_leak_descriptors(
        self,
        tmp_path: Path,
    ) -> None:
        descriptor_directory = Path("/proc/self/fd")
        if not descriptor_directory.is_dir():
            pytest.skip("descriptor census requires procfs")
        case = _request_case(tmp_path)
        target = tmp_path / "request.json"
        lock = tmp_path / ".request.json.lock"
        alias = tmp_path / ".request.json.lock-alias"
        lock.write_bytes(b"")
        os.link(lock, alias)
        before = len(tuple(descriptor_directory.iterdir()))

        for _ in range(10):
            with pytest.raises(ProductLifecycleError, match="private regular file"):
                save_product_apply_request(target, case.request)

        assert len(tuple(descriptor_directory.iterdir())) == before
        assert not target.exists()

    def test_success_does_not_depend_on_an_explicit_unlock(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        descriptor_directory = Path("/proc/self/fd")
        if not descriptor_directory.is_dir():
            pytest.skip("descriptor census requires procfs")
        case = _request_case(tmp_path)
        target = tmp_path / "request.json"
        original_flock = lifecycle.fcntl.flock
        before = len(tuple(descriptor_directory.iterdir()))

        def refuse_explicit_unlock(descriptor: int, operation: int) -> None:
            if operation & lifecycle.fcntl.LOCK_UN:
                raise OSError("explicit unlock must not define save success")
            original_flock(descriptor, operation)

        monkeypatch.setattr(lifecycle.fcntl, "flock", refuse_explicit_unlock)

        save_product_apply_request(target, case.request)

        assert target.exists()
        assert len(tuple(descriptor_directory.iterdir())) == before

    def test_existing_request_with_an_external_hardlink_refuses(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        target = tmp_path / "request.json"
        alias = tmp_path / "request-alias.json"
        save_product_apply_request(target, case.request)
        os.link(target, alias)

        with pytest.raises(ProductLifecycleError, match="not a regular file"):
            save_product_apply_request(target, case.request)

        assert alias.read_bytes() == target.read_bytes()

    def test_archive_drift_preserves_stable_package_refusal(
        self,
        tmp_path: Path,
    ) -> None:
        case = _request_case(tmp_path)
        package_path = Path(case.request.distribution_package_path)
        package_path.write_bytes(package_path.read_bytes() + b"changed")
        target = tmp_path / "request.json"

        with pytest.raises(ProductLifecycleError) as caught:
            save_product_apply_request(target, case.request)

        assert caught.value.data["package_error_code"]
        assert "logical_path" in caught.value.data
        assert not target.exists()

    def test_renamed_archive_without_rehash_refuses(self, tmp_path: Path) -> None:
        case = _request_case(tmp_path)
        original = Path(case.request.distribution_package_path)
        renamed = original.with_name(f"{'f' * 64}.okgdist.tar")
        original.rename(renamed)
        request = replace(case.request, distribution_package_path=renamed)

        with pytest.raises(ProductLifecycleError) as caught:
            verify_product_request_package_input(request, plan=case.plan)

        assert caught.value.data["package_error_code"] == "transport_digest_mismatch"

    def test_package_limit_details_survive_translation(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        case = _request_case(tmp_path)

        def refuse(_path: Path):
            raise DistributionPackageError(
                "package_limit_exceeded",
                "objects",
                limit="objects",
                observed=2,
                maximum=1,
            )

        monkeypatch.setattr(lifecycle, "verify_distribution_package", refuse)
        with pytest.raises(ProductLifecycleError) as caught:
            verify_package_selection(
                case.request.distribution_package_path,
                case.request.package_selection,
            )

        assert caught.value.data == {
            "package_error_code": "package_limit_exceeded",
            "logical_path": "objects",
            "limit": "objects",
            "observed": 2,
            "maximum": 1,
        }

    def test_contained_reader_refuses_escape(self, tmp_path: Path) -> None:
        root = tmp_path / "root"
        root.mkdir()
        outside = tmp_path / "outside.txt"
        outside.write_text("outside", encoding="utf-8")
        (root / "linked.txt").symlink_to(outside)
        reader = ContainedArtifactReader(root)

        with pytest.raises(ProductLifecycleError, match="unsafe path"):
            reader.read_bytes("../outside.txt")
        with pytest.raises(ProductLifecycleError, match="changed, or could not"):
            reader.read_bytes("linked.txt")

    def test_json_reader_refuses_fifo_without_blocking(self, tmp_path: Path) -> None:
        fifo = tmp_path / "request.json"
        os.mkfifo(fifo)

        with pytest.raises(ProductLifecycleError, match="not a regular file"):
            load_product_apply_request(fifo)

    def test_slice_has_no_deferred_runtime_entry_point(self) -> None:
        source = Path(cast(str, lifecycle.__file__)).read_text(encoding="utf-8")

        assert "from okg.substrate.cli" not in source
        assert "from okg.substrate.runtime" not in source
        assert "from okg.substrate.deployment_promotion_operations" not in source
        assert "def run_product_apply(" not in source
        assert "def project_product_apply_outcome(" not in source


class TestBoundedProductAdmissionAndSnapshot:
    @pytest.fixture(autouse=True)
    def _record_pact_scope(
        self,
        record_property: Callable[[str, object], None],
    ) -> None:
        record_property(
            "requirements",
            " ".join(
                (
                    "req.lifecycle.admission-journal-slice",
                    "req.lifecycle.snapshot-readback-slice",
                    "req.lifecycle.package-journal-slice",
                )
            ),
        )

    def test_admission_reopens_exact_package_plan_definition_and_v3(
        self, tmp_path: Path
    ) -> None:
        case = _admission_case(tmp_path)

        admission = admit_product_apply_request(
            case.request,
            now=datetime(2026, 8, 30, 7, 30, tzinfo=timezone.utc),
        )

        assert admission["request_digest"] == case.request.request_digest
        assert admission["product_plan_digest"] == case.plan.plan_digest
        assert admission["lock_digest"] == case.plan.lock_digest
        assert admission["package_selection"] == (
            case.request.package_selection.to_mapping()
        )
        assert admission["package_artifact_bindings"] == [
            item.to_mapping() for item in case.request.package_artifact_bindings
        ]
        approval = lifecycle.load_product_promotion_approval(
            case.request.promotion_approval_path
        )
        assert approval.digest == approval_digest(approval)
        assert approval.approved_by == "opérator"

    def test_admission_refuses_changed_contained_byte_before_journal_authority(
        self, tmp_path: Path
    ) -> None:
        case = _admission_case(tmp_path)
        binding = case.request.package_artifact_bindings[0]
        target = Path(case.request.artifact_root) / binding.logical_id
        target.write_bytes(b"changed")

        with pytest.raises(ProductLifecycleError, match="size differs|bytes differ"):
            admit_product_apply_request(
                case.request,
                now=datetime(2026, 8, 30, 7, 30, tzinfo=timezone.utc),
            )

    def test_snapshot_exposes_exact_identity_but_no_receipt_or_success(
        self, tmp_path: Path
    ) -> None:
        case = _admission_case(tmp_path)
        admission = admit_product_apply_request(
            case.request,
            now=datetime(2026, 8, 30, 7, 30, tzinfo=timezone.utc),
        )
        state = _admitted_state(case, admission)

        snapshot = project_product_operation_status(state)
        mapping = snapshot.to_mapping()

        assert mapping["status"]["state"] == "planned"
        assert mapping["status"]["safe_to_retry"] is False
        assert mapping["status"]["next_actions"] == []
        assert mapping["receipts"] == {
            "verification": "deferred",
            "verified": [],
        }
        assert mapping["digests"]["product_plan"] == case.plan.plan_digest
        assert mapping["digests"]["promotion_approval"] == admission[
            "approval_digest"
        ]
        rendered = render_product_operation_status(mapping)
        assert "state: planned" in rendered
        assert "receipt_verification: deferred" in rendered
        assert "safe_to_retry: false" in rendered

    def test_snapshot_authority_is_deeply_immutable(self, tmp_path: Path) -> None:
        case = _admission_case(tmp_path)
        admission = admit_product_apply_request(
            case.request,
            now=datetime(2026, 8, 30, 7, 30, tzinfo=timezone.utc),
        )
        snapshot = project_product_operation_status(
            _admitted_state(case, admission)
        )
        original = snapshot.to_mapping()

        with pytest.raises(TypeError):
            snapshot.digests["product_plan"] = "sha256:" + "f" * 64  # type: ignore[index]
        with pytest.raises(TypeError):
            snapshot.package["selection"]["package_id"] = "changed"  # type: ignore[index]
        with pytest.raises(TypeError):
            snapshot.package["selection"]["package_identities"][0]["id"] = (  # type: ignore[index]
                "changed"
            )
        with pytest.raises(TypeError):
            snapshot.journal["release_observation"]["active_absent"] = False  # type: ignore[index]
        with pytest.raises(TypeError):
            snapshot.receipts["verified"] = ("untrusted",)  # type: ignore[index]

        detached = snapshot.to_mapping()
        detached["digests"]["product_plan"] = "sha256:" + "f" * 64
        detached["package"]["selection"]["bundle"]["id"] = "changed"
        detached["journal"]["release_observation"]["active_absent"] = False
        assert snapshot.to_mapping() == original

    def test_v3_writer_refuses_untyped_or_invalid_approval(self, tmp_path: Path) -> None:
        target = tmp_path / "approval.json"

        with pytest.raises(ProductLifecycleError, match="ProductPromotionApprovalV3"):
            save_product_promotion_approval(
                target,
                {"schema": "okg.deployment_promotion_approval/v3"},
            )

        assert not target.exists()

    def test_snapshot_refuses_completed_or_effectful_admission_only_state(
        self, tmp_path: Path
    ) -> None:
        case = _admission_case(tmp_path)
        admission = admit_product_apply_request(
            case.request,
            now=datetime(2026, 8, 30, 7, 30, tzinfo=timezone.utc),
        )
        state = _admitted_state(case, admission)
        state["status"] = "completed"

        with pytest.raises(ProductLifecycleError, match="effect claims"):
            project_product_operation_status(state)
