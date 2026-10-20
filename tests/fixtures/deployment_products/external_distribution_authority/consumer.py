"""Outside-checkout consumer of the public distribution authority boundary."""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
from pathlib import Path

from okg.distributions import (
    CONFORMANCE_PROTOCOL,
    CONFORMANCE_REQUEST_SCHEMA,
    AssetLocator,
    CompatibilityMetadata,
    ConformanceDefinitionReference,
    DistributionConformanceAuthorityError,
    DistributionObject,
    DistributionPackageManifest,
    DistributionPackageProvenance,
    DistributionPackageSource,
    DistributionRelease,
    OwnerQualifiedReference,
    PackagedAsset,
    ProductBundle,
    TypedAsset,
    bind_distribution_conformance_request,
    build_distribution_package,
    canonical_bundle_digest,
    canonical_distribution_conformance_authority_bytes,
    canonical_release_digest,
    conformance_request_digest,
    derive_distribution_conformance_authority,
    distribution_conformance_authority_digest,
    load_conformance_request,
    verify_distribution_package,
)


ZERO_DIGEST = "sha256:" + "0" * 64
SOURCE_REVISION = "7" * 40


def digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def asset(
    owner: str,
    asset_id: str,
    payload: bytes,
    *,
    kind: str,
    plane: str,
) -> TypedAsset:
    return TypedAsset(
        owner=owner,
        asset_id=asset_id,
        kind=kind,
        plane=plane,
        version="v1",
        digest=digest(payload),
        schema="okg.deployment_product.typed_asset/v1",
        merge_identity=asset_id,
        merge_rule="refuse",
        compatibility={"contract_versions": ["v1"], "capabilities": ["declarative"]},
        provenance={
            "source": "distribution",
            "repository_url": "https://example.org/neutral-distribution",
            "revision": SOURCE_REVISION,
            "artifact_ref": f"{owner}:{asset_id}",
        },
    )


def reference(value: TypedAsset) -> OwnerQualifiedReference:
    return OwnerQualifiedReference(
        owner=value.owner,
        id=value.asset_id,
        kind=value.kind,
        version=value.version,
        digest=value.digest,
    )


def bundle(
    bundle_id: str,
    refs: tuple[OwnerQualifiedReference, ...],
) -> ProductBundle:
    provisional = ProductBundle(
        bundle_id=bundle_id,
        version="v1",
        distribution_id="neutral-lab",
        distribution_version="1.0.0",
        bundle_digest=ZERO_DIGEST,
        assets=refs,
        compatibility=CompatibilityMetadata(
            contract_versions=("v1",), capabilities=("declarative",)
        ),
    )
    return provisional.model_copy(
        update={"bundle_digest": canonical_bundle_digest(provisional)}
    )


def build_package(workspace: Path) -> Path:
    authoring = workspace / "authoring-distribution"
    output = workspace / "packages"
    authoring.mkdir()
    output.mkdir()
    payloads = {
        "neutral:shared-schema": b"schema: shared-domain\n",
        "operations:defaults": b"mode: operations\n",
        "analysis:defaults": b"mode: analysis\n",
    }
    files: dict[str, Path] = {}
    for qualified_id, payload in payloads.items():
        owner, asset_id = qualified_id.split(":", 1)
        path = authoring / owner / f"{asset_id}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        files[qualified_id] = path

    shared = asset(
        "neutral",
        "shared-schema",
        payloads["neutral:shared-schema"],
        kind="schema",
        plane="semantic-catalog",
    )
    operations = asset(
        "operations",
        "defaults",
        payloads["operations:defaults"],
        kind="playbook",
        plane="operations-governance",
    )
    analysis = asset(
        "analysis",
        "defaults",
        payloads["analysis:defaults"],
        kind="playbook",
        plane="operations-governance",
    )
    assets = (shared, operations, analysis)
    operations_bundle = bundle("operations", (reference(operations),))
    analysis_bundle = bundle("analysis", (reference(analysis),))
    release = DistributionRelease(
        distribution_id="neutral-lab",
        version="1.0.0",
        origin="external",
        release_digest=ZERO_DIGEST,
        assets=(reference(shared),),
        bundle_ids=("operations", "analysis"),
        compatibility=CompatibilityMetadata(
            contract_versions=("v1",), capabilities=("declarative",)
        ),
    )
    objects = tuple(
        DistributionObject(
            digest=digest(payloads[item.qualified_id]),
            size=len(payloads[item.qualified_id]),
            media_type="application/yaml",
        )
        for item in assets
    )
    packaged = tuple(
        PackagedAsset(
            asset=item,
            locator=AssetLocator(object_digest=digest(payloads[item.qualified_id])),
            uncompressed_size=len(payloads[item.qualified_id]),
        )
        for item in assets
    )
    manifest = DistributionPackageManifest(
        distribution_release=release,
        bundles=(operations_bundle, analysis_bundle),
        assets=packaged,
        objects=objects,
        provenance=DistributionPackageProvenance(
            repository_url="https://example.org/neutral-distribution",
            revision=SOURCE_REVISION,
            builder="neutral-fixture",
        ),
    )
    release = release.model_copy(
        update={"release_digest": canonical_release_digest(manifest)}
    )
    manifest = manifest.model_copy(update={"distribution_release": release})
    sources = {
        digest(payloads[qualified_id]): DistributionPackageSource(
            root=authoring,
            path=path,
        )
        for qualified_id, path in files.items()
    }
    package_path = build_distribution_package(manifest, sources, output)
    shutil.rmtree(authoring)
    assert not authoring.exists()
    return package_path


def request_for(authority, definition: ConformanceDefinitionReference):
    payload = {
        "schema_version": CONFORMANCE_REQUEST_SCHEMA,
        "request_id": "neutral-operations-contract",
        "subject": authority.subject("hermetic_fixture").model_dump(mode="json"),
        "selection": authority.selection(definition).model_dump(
            mode="json", by_alias=True
        ),
        "case": {
            "case_id": "neutral-package-binding",
            "claim_profile": "contract",
            "requested_arms": ["contract"],
        },
        "expectations": {
            "signals": [
                {
                    "signal_id": "package-authority",
                    "kind": "interface",
                    "target": "distribution-authority",
                }
            ]
        },
        "inputs": {},
        "admission": {
            "extension_boundary_digest": "sha256:" + "1" * 64,
            "package_admission_digest": "sha256:" + "2" * 64,
            "capability_digest": "sha256:" + "3" * 64,
            "resource_policy_digest": "sha256:" + "4" * 64,
        },
        "compatibility": {
            "product_contract_api": "okg.deployment_product.api/v1",
            "harness_protocol": CONFORMANCE_PROTOCOL,
        },
    }
    return load_conformance_request(payload)


def refusal(call) -> dict[str, str]:
    try:
        call()
    except DistributionConformanceAuthorityError as exc:
        return exc.payload
    raise AssertionError("expected distribution conformance refusal")


def replace_request(request, edit):
    payload = request.model_dump(mode="json", by_alias=True)
    edit(payload)
    return load_conformance_request(payload)


def replace_release_digest(payload, value: str) -> None:
    payload["subject"]["release_digest"] = value
    payload["selection"]["distribution_release"]["release_digest"] = value


def main(root: Path) -> dict[str, object]:
    package_path = build_package(root)
    verified = verify_distribution_package(package_path)
    operations = derive_distribution_conformance_authority(
        verified, bundle_ids=("operations",)
    )
    analysis = derive_distribution_conformance_authority(
        verified, bundle_ids=("analysis",)
    )
    assert {item.qualified_id for item in operations.assets} == {
        "neutral:shared-schema",
        "operations:defaults",
    }
    assert {item.qualified_id for item in analysis.assets} == {
        "neutral:shared-schema",
        "analysis:defaults",
    }

    definition = ConformanceDefinitionReference(
        definition_id="neutral-instance",
        definition_digest=digest(b"neutral instance definition"),
    )
    request = request_for(operations, definition)
    bound = bind_distribution_conformance_request(
        operations, request, definition=definition
    )
    changed_id = ConformanceDefinitionReference(
        definition_id="changed-instance",
        definition_digest=definition.definition_digest,
    )
    changed_digest = ConformanceDefinitionReference(
        definition_id=definition.definition_id,
        definition_digest=digest(b"changed instance definition"),
    )
    authority_bytes = canonical_distribution_conformance_authority_bytes(operations)
    authority_digest = operations.authority_digest
    operations.selection(changed_id)
    operations.selection(changed_digest)
    assert authority_bytes == canonical_distribution_conformance_authority_bytes(
        operations
    )
    assert authority_digest == operations.authority_digest
    assert authority_digest == distribution_conformance_authority_digest(operations)

    archive_request = replace_request(
        request,
        lambda payload: payload["subject"]["artifact"].update(
            {"digest": "sha256:" + "a" * 64}
        ),
    )
    release_request = replace_request(
        request,
        lambda payload: replace_release_digest(payload, "sha256:" + "d" * 64),
    )
    bundle_request = replace_request(
        request,
        lambda payload: payload["selection"]["bundles"][0].update(
            {"bundle_digest": "sha256:" + "b" * 64}
        ),
    )
    schema_request = replace_request(
        request,
        lambda payload: payload["selection"]["distribution_release"]["assets"][
            0
        ].update({"digest": "sha256:" + "c" * 64}),
    )

    encoded = authority_bytes.decode("utf-8")
    assert str(root) not in encoded
    assert "authoring-distribution" not in encoded
    return {
        "authority": json.loads(encoded),
        "authority_digest": operations.authority_digest,
        "analysis_authority_digest": analysis.authority_digest,
        "request_digest": conformance_request_digest(bound),
        "selected_assets": [item.qualified_id for item in operations.assets],
        "analysis_selected_assets": [item.qualified_id for item in analysis.assets],
        "definition_not_packaged": all(
            item.asset_id != definition.definition_id for item in operations.assets
        ),
        "refusals": {
            "definition_id": refusal(
                lambda: bind_distribution_conformance_request(
                    operations, request, definition=changed_id
                )
            ),
            "definition_digest": refusal(
                lambda: bind_distribution_conformance_request(
                    operations, request, definition=changed_digest
                )
            ),
            "archive": refusal(
                lambda: bind_distribution_conformance_request(
                    operations, archive_request, definition=definition
                )
            ),
            "release": refusal(
                lambda: bind_distribution_conformance_request(
                    operations, release_request, definition=definition
                )
            ),
            "bundle": refusal(
                lambda: bind_distribution_conformance_request(
                    operations, bundle_request, definition=definition
                )
            ),
            "schema": refusal(
                lambda: bind_distribution_conformance_request(
                    operations, schema_request, definition=definition
                )
            ),
        },
    }


if __name__ == "__main__":
    print(json.dumps(main(Path(sys.argv[1]).resolve()), sort_keys=True))
