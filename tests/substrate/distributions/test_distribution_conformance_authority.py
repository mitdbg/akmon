"""Focused proof for the verified-package conformance authority boundary."""

from __future__ import annotations

import ast
import hashlib
from dataclasses import replace
from functools import partial
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Any

import jsonschema
import pytest
from pydantic import ValidationError

from okg.distributions import (
    CONFORMANCE_RECEIPT_SCHEMA_V2,
    ExternalDistributionConformanceReceiptV2,
    conformance_request_digest,
    conformance_receipt_digest,
    load_conformance_receipt,
    validate_conformance_exchange,
    CONFORMANCE_PROTOCOL,
    CONFORMANCE_PROTOCOL_V2,
    CONFORMANCE_REQUEST_SCHEMA_V2,
    DISTRIBUTION_CONFORMANCE_AUTHORITY_SCHEMA_V2,
    DistributionConformanceAuthorityV2,
    OkgCompatibility,
    CONFORMANCE_REQUEST_SCHEMA,
    DISTRIBUTION_CONFORMANCE_AUTHORITY_SCHEMA,
    ConformanceDefinitionReference,
    DistributionConformanceAuthority,
    DistributionConformanceAuthorityError,
    DistributionPackageError,
    DistributionPackageManifest,
    ProductBundle,
    bind_distribution_conformance_request,
    canonical_bundle_digest,
    canonical_distribution_conformance_authority_bytes,
    canonical_release_digest,
    conformance_schema_catalog,
    derive_distribution_conformance_authority,
    distribution_conformance_authority_digest,
    distribution_conformance_authority_schema,
    load_conformance_request,
    verify_distribution_package,
)

from okg.substrate.distributions import package as package_impl

from .test_distribution_package import Fixture, _build, _with_assets, make_fixture
from .test_distribution_package_v2 import _promote_to_v2


ZERO_DIGEST = "sha256:" + "0" * 64
HISTORICAL_CONFORMANCE_SCHEMA_DIGEST = (
    "sha256:db38e9bbbe571dbb9efec29192f786d851095ca412f5a5fda17d1af81f62e662"
)
EXTERNAL_FIXTURE = (
    Path(__file__).parents[2]
    / "fixtures"
    / "deployment_products"
    / "external_distribution_authority"
    / "consumer.py"
)


def _sealed_bundle(bundle: ProductBundle, **updates: object) -> ProductBundle:
    payload = bundle.model_dump(mode="json", by_alias=True)
    payload.update(updates)
    payload["bundle_digest"] = ZERO_DIGEST
    provisional = ProductBundle.model_validate(payload)
    payload["bundle_digest"] = canonical_bundle_digest(provisional)
    return ProductBundle.model_validate(payload)


def _with_bundles(fixture: Fixture, bundles: tuple[ProductBundle, ...]) -> Fixture:
    manifest = fixture.manifest.model_dump(mode="json", by_alias=True)
    manifest["bundles"] = [
        bundle.model_dump(mode="json", by_alias=True) for bundle in bundles
    ]
    manifest["distribution_release"]["bundle_ids"] = sorted(
        bundle.bundle_id for bundle in bundles
    )
    manifest["distribution_release"]["release_digest"] = ZERO_DIGEST
    provisional = DistributionPackageManifest.model_validate(manifest)
    manifest["distribution_release"]["release_digest"] = canonical_release_digest(
        provisional
    )
    fixture.manifest = DistributionPackageManifest.model_validate(manifest)
    return fixture


def _with_changed_release_reference(
    fixture: Fixture, field: str, value: str
) -> Fixture:
    manifest = fixture.manifest.model_dump(mode="json", by_alias=True)
    manifest["distribution_release"]["assets"][0][field] = value
    manifest["distribution_release"]["release_digest"] = ZERO_DIGEST
    provisional = DistributionPackageManifest.model_validate(manifest)
    manifest["distribution_release"]["release_digest"] = canonical_release_digest(
        provisional
    )
    fixture.manifest = DistributionPackageManifest.model_validate(manifest)
    return fixture


def _request_for(authority, definition: ConformanceDefinitionReference):
    return load_conformance_request(
        {
            "schema_version": (
                CONFORMANCE_REQUEST_SCHEMA
                if authority.conformance_protocol == CONFORMANCE_PROTOCOL
                else CONFORMANCE_REQUEST_SCHEMA_V2
            ),
            "request_id": "package-authority-contract",
            "subject": authority.subject("hermetic_fixture").model_dump(mode="json"),
            "selection": authority.selection(definition).model_dump(
                mode="json", by_alias=True
            ),
            "case": {
                "case_id": "package-authority-binding",
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
                "harness_protocol": authority.conformance_protocol,
            },
        }
    )


def _changed_request(request, edit):
    payload = request.model_dump(mode="json", by_alias=True)
    edit(payload)
    return load_conformance_request(payload)


def _change_selected_asset_digest(payload: dict[str, Any], digest: str) -> None:
    selected = payload["selection"]
    target = selected["distribution_release"]["assets"][0]
    qualified_id = (target["owner"], target["id"])
    target["digest"] = digest
    for bundle in selected["bundles"]:
        for reference in bundle["assets"]:
            if (reference["owner"], reference["id"]) == qualified_id:
                reference["digest"] = digest


def _change_release_digest(payload: dict[str, Any], digest: str) -> None:
    payload["subject"]["release_digest"] = digest
    payload["selection"]["distribution_release"]["release_digest"] = digest


def _authority_refusal(call) -> DistributionConformanceAuthorityError:
    with pytest.raises(DistributionConformanceAuthorityError) as caught:
        call()
    return caught.value


def test_authority_is_versioned_path_free_and_relocation_stable(tmp_path: Path) -> None:
    fixture = make_fixture(tmp_path / "original")
    package_path = _build(fixture)
    verified = verify_distribution_package(package_path)
    authority = derive_distribution_conformance_authority(
        verified, bundle_ids=("cern-team",)
    )

    schema = distribution_conformance_authority_schema()
    assert schema["$id"] == DISTRIBUTION_CONFORMANCE_AUTHORITY_SCHEMA
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    jsonschema.Draft202012Validator.check_schema(schema)
    assert {
        "schema_version",
        "conformance_protocol",
        "authority_digest",
        "artifact",
        "manifest",
        "package_verification_receipt",
        "source_revision",
        "distribution_release",
        "bundles",
        "assets",
    } == set(schema["required"])
    assert {
        "digest_domain",
        "package_schema",
        "media_type",
        "digest",
        "size",
    } == set(schema["$defs"]["DistributionConformanceArtifact"]["required"])
    assert {"digest_domain", "digest", "size"} == set(
        schema["$defs"]["DistributionConformanceRawBlob"]["required"]
    )
    assert {"digest_domain", "schema_version", "digest"} == set(
        schema["$defs"]["DistributionConformanceCanonicalRecord"]["required"]
    )
    jsonschema.validate(
        authority.model_dump(mode="json", by_alias=True),
        schema,
        cls=jsonschema.Draft202012Validator,
    )
    assert authority.manifest.digest_domain == "raw-bytes/v1"
    assert authority.artifact.digest_domain == "raw-bytes/v1"
    assert authority.artifact.size == package_path.stat().st_size
    assert authority.package_verification_receipt.digest_domain == "canonical-json/v1"
    assert authority.authority_digest == distribution_conformance_authority_digest(
        authority
    )
    assert tuple(item.qualified_id for item in authority.assets) == tuple(
        sorted(item.qualified_id for item in authority.assets)
    )

    encoded = canonical_distribution_conformance_authority_bytes(authority)
    assert str(tmp_path).encode() not in encoded
    assert b"authoring-checkout" not in encoded
    assert b"definition" not in encoded
    assert b"secret" not in encoded

    missing_domain = authority.model_dump(mode="json", by_alias=True)
    missing_domain["artifact"].pop("digest_domain")
    with pytest.raises(ValidationError, match="digest_domain"):
        DistributionConformanceAuthority.model_validate(missing_domain)
    with pytest.raises(jsonschema.ValidationError, match="required property"):
        jsonschema.validate(missing_domain, schema, cls=jsonschema.Draft202012Validator)

    relocated_dir = tmp_path / "relocated"
    relocated_dir.mkdir()
    relocated_path = relocated_dir / package_path.name
    shutil.copyfile(package_path, relocated_path)
    relocated = verify_distribution_package(relocated_path)
    relocated_authority = derive_distribution_conformance_authority(
        relocated, bundle_ids=("cern-team",)
    )
    assert (
        canonical_distribution_conformance_authority_bytes(relocated_authority)
        == encoded
    )
    relocated.close_runtime_authority()
    verified.close_runtime_authority()


@pytest.mark.parametrize("protocol_version", ["v1", "v2"])
def test_authority_self_digest_covers_raw_object_and_member_identity(
    tmp_path: Path,
    protocol_version: str,
) -> None:
    fixture = make_fixture(tmp_path)
    if protocol_version == "v2":
        fixture = _promote_to_v2(fixture)
    verified = verify_distribution_package(_build(fixture))
    authority = derive_distribution_conformance_authority(
        verified, bundle_ids=("cern-team",), protocol_version=protocol_version
    )
    raw_object = authority.model_dump(mode="json", by_alias=True)
    raw_object["assets"][0]["raw_object"]["digest"] = "sha256:" + "a" * 64
    with pytest.raises(ValidationError, match="authority_digest"):
        type(authority).model_validate(raw_object)

    member = authority.model_dump(mode="json", by_alias=True)
    member_asset = next(item for item in member["assets"] if item["member"])
    member_asset["member"]["member_crc32"] = "00000000"
    with pytest.raises(ValidationError, match="authority_digest"):
        type(authority).model_validate(member)
    verified.close_runtime_authority()


def test_authority_preserves_required_nullable_asset_plane(tmp_path: Path) -> None:
    fixture = make_fixture(tmp_path)
    original = fixture.manifest.assets[0]
    asset_payload = original.asset.to_dict()
    asset_payload["plane"] = None
    asset = type(original.asset).from_value(asset_payload)
    packaged = type(original)(
        asset=asset,
        locator=original.locator,
        uncompressed_size=original.uncompressed_size,
    )
    assets = (packaged, *fixture.manifest.assets[1:])

    verified = verify_distribution_package(_build(_with_assets(fixture, assets)))
    authority = derive_distribution_conformance_authority(
        verified, bundle_ids=("cern-team",)
    )
    selected = next(
        item for item in authority.assets if item.asset_id == asset.asset_id
    )
    schema = distribution_conformance_authority_schema()

    assert selected.plane is None
    assert "plane" in set(
        schema["$defs"]["DistributionConformanceSelectedAsset"]["required"]
    )
    jsonschema.validate(
        authority.model_dump(mode="json", by_alias=True),
        schema,
        cls=jsonschema.Draft202012Validator,
    )
    verified.close_runtime_authority()


def test_inert_receipt_refuses_after_package_authority_is_closed(
    tmp_path: Path,
) -> None:
    verified = verify_distribution_package(_build(make_fixture(tmp_path)))
    receipt = verified.verification_receipt()
    assert receipt.digest.startswith("sha256:")
    assert not hasattr(receipt, "read")
    assert not hasattr(receipt, "path")

    verified.close_runtime_authority()
    with pytest.raises(DistributionPackageError) as caught:
        verified.verification_receipt()
    assert caught.value.code == "package_verification_receipt_unavailable"


@pytest.mark.parametrize("protocol_version", ["v1", "v2"])
def test_bundle_selection_is_canonical_and_enforces_dependency_closure(
    tmp_path: Path,
    protocol_version: str,
) -> None:
    derive = partial(
        derive_distribution_conformance_authority, protocol_version=protocol_version
    )
    fixture = make_fixture(tmp_path)
    first, second = fixture.manifest.bundles
    third = _sealed_bundle(second, bundle_id="third-bundle")
    first = _sealed_bundle(first, requires_bundle_ids=(second.bundle_id,))
    second = _sealed_bundle(second, requires_bundle_ids=(third.bundle_id,))
    fixture = _with_bundles(fixture, (first, second, third))
    if protocol_version == "v2":
        fixture = _promote_to_v2(fixture)
    package_path = _build(fixture)
    verified = verify_distribution_package(package_path)

    missing = _authority_refusal(
        lambda: derive(verified, bundle_ids=(first.bundle_id, second.bundle_id))
    )
    assert missing.code == "distribution_conformance_bundle_selection_invalid"
    assert missing.field == f"bundles.{second.bundle_id}.requires_bundle_ids"

    forward = derive(
        verified,
        bundle_ids=(first.bundle_id, second.bundle_id, third.bundle_id),
    )
    reverse = derive(
        verified,
        bundle_ids=(third.bundle_id, second.bundle_id, first.bundle_id),
    )
    assert canonical_distribution_conformance_authority_bytes(
        forward
    ) == canonical_distribution_conformance_authority_bytes(reverse)
    verified.close_runtime_authority()


@pytest.mark.parametrize(
    ("update", "field_suffix"),
    [
        ({"requires_bundle_ids": ("undeclared",)}, ".references"),
        ({"requires_bundle_ids": ("cern-team",)}, ".requires_bundle_ids"),
        (
            {"conflicts_with_bundle_ids": ("cern-team",)},
            ".conflicts_with_bundle_ids",
        ),
        (
            {"conflicts_with_bundle_ids": ("fixture-bundle",)},
            ".conflicts_with_bundle_ids",
        ),
    ],
)
def test_bundle_selection_refuses_invalid_graphs(
    tmp_path: Path, update: dict[str, object], field_suffix: str
) -> None:
    fixture = make_fixture(tmp_path)
    first, second = fixture.manifest.bundles
    changed = _sealed_bundle(first, **update)
    verified = verify_distribution_package(
        _build(_with_bundles(fixture, (changed, second)))
    )
    error = _authority_refusal(
        lambda: derive_distribution_conformance_authority(
            verified, bundle_ids=(first.bundle_id, second.bundle_id)
        )
    )
    assert error.code == "distribution_conformance_bundle_selection_invalid"
    assert error.field == f"bundles.{first.bundle_id}{field_suffix}"
    verified.close_runtime_authority()


@pytest.mark.parametrize(
    ("field", "value"), (("kind", "wrong-kind"), ("version", "v2"))
)
def test_asset_closure_refuses_disagreeing_release_and_bundle_references(
    tmp_path: Path, field: str, value: str
) -> None:
    verified = verify_distribution_package(
        _build(_with_changed_release_reference(make_fixture(tmp_path), field, value))
    )
    error = _authority_refusal(
        lambda: derive_distribution_conformance_authority(
            verified, bundle_ids=("cern-team",)
        )
    )
    assert error.code == "distribution_conformance_asset_closure_invalid"
    assert error.field == "assets.archi:wheel"
    verified.close_runtime_authority()


@pytest.mark.parametrize("protocol_version", ["v1", "v2"])
def test_request_binding_preserves_v1_and_keeps_definition_caller_owned(
    tmp_path: Path,
    protocol_version: str,
) -> None:
    fixture = make_fixture(tmp_path)
    if protocol_version == "v2":
        fixture = _promote_to_v2(fixture)
    verified = verify_distribution_package(_build(fixture))
    authority = derive_distribution_conformance_authority(
        verified, bundle_ids=("cern-team",), protocol_version=protocol_version
    )
    definition = ConformanceDefinitionReference(
        definition_id="site-instance",
        definition_digest="sha256:" + "5" * 64,
    )
    request = _request_for(authority, definition)
    assert (
        bind_distribution_conformance_request(authority, request, definition=definition)
        == request
    )

    authority_bytes = canonical_distribution_conformance_authority_bytes(authority)
    changed_definition = ConformanceDefinitionReference(
        definition_id="different-instance",
        definition_digest="sha256:" + "6" * 64,
    )
    authority.selection(changed_definition)
    assert (
        canonical_distribution_conformance_authority_bytes(authority) == authority_bytes
    )

    wrong_definition = _authority_refusal(
        lambda: bind_distribution_conformance_request(
            authority, request, definition=changed_definition
        )
    )
    assert (
        wrong_definition.code == "distribution_conformance_request_definition_mismatch"
    )
    assert wrong_definition.field == "selection.definition.definition_id"

    mutations: tuple[tuple[Any, str, str], ...] = (
        (
            lambda payload: _change_release_digest(payload, "sha256:" + "a" * 64),
            "distribution_conformance_request_subject_mismatch",
            "subject.release_digest",
        ),
        (
            lambda payload: payload["subject"]["artifact"].update(
                {"digest": "sha256:" + "7" * 64}
            ),
            "distribution_conformance_request_subject_mismatch",
            "subject.artifact.digest",
        ),
        (
            lambda payload: payload["selection"]["bundles"][0].update(
                {"bundle_digest": "sha256:" + "8" * 64}
            ),
            "distribution_conformance_request_bundle_mismatch",
            "selection.bundles.cern-team",
        ),
        (
            lambda payload: _change_selected_asset_digest(
                payload, "sha256:" + "9" * 64
            ),
            "distribution_conformance_request_asset_mismatch",
            "selection.distribution_release.assets",
        ),
    )
    for edit, code, field in mutations:
        changed = _changed_request(request, edit)
        refusal = _authority_refusal(
            lambda changed=changed: bind_distribution_conformance_request(
                authority, changed, definition=definition
            )
        )
        assert (refusal.code, refusal.field) == (code, field)

    assert (
        conformance_schema_catalog()["schema_digest"]
        == HISTORICAL_CONFORMANCE_SCHEMA_DIGEST
    )
    verified.close_runtime_authority()


def test_external_consumer_binds_verified_package_after_checkout_removal() -> None:
    source = EXTERNAL_FIXTURE.read_text(encoding="utf-8")
    imports = {
        node.module
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert "okg.substrate" not in source
    assert {item for item in imports if item.startswith("okg")} == {"okg.distributions"}

    with tempfile.TemporaryDirectory(prefix="okg-external-distribution-") as raw:
        root = Path(raw)
        consumer_dir = root / "consumer"
        workspace = root / "workspace"
        consumer_dir.mkdir()
        workspace.mkdir()
        script = consumer_dir / "consumer.py"
        shutil.copyfile(EXTERNAL_FIXTURE, script)
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("OKG_") and key != "PYTHONPATH"
        }
        environment["PYTHONNOUSERSITE"] = "1"
        completed = subprocess.run(
            [sys.executable, str(script), str(workspace)],
            cwd=consumer_dir,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )

    result = json.loads(completed.stdout)
    assert result["selected_assets"] == [
        "neutral:shared-schema",
        "operations:defaults",
    ]
    assert result["analysis_selected_assets"] == [
        "analysis:defaults",
        "neutral:shared-schema",
    ]
    assert result["definition_not_packaged"] is True
    assert result["refusals"] == {
        "archive": {
            "code": "distribution_conformance_request_subject_mismatch",
            "expected": result["authority"]["artifact"]["digest"],
            "field": "subject.artifact.digest",
            "observed": "sha256:" + "a" * 64,
        },
        "bundle": {
            "code": "distribution_conformance_request_bundle_mismatch",
            "expected": result["authority"]["bundles"][0]["bundle_digest"],
            "field": "selection.bundles.operations",
            "observed": "sha256:" + "b" * 64,
        },
        "definition_digest": {
            "code": "distribution_conformance_request_definition_mismatch",
            "expected": "sha256:"
            + hashlib.sha256(b"changed instance definition").hexdigest(),
            "field": "selection.definition.definition_digest",
            "observed": "sha256:"
            + hashlib.sha256(b"neutral instance definition").hexdigest(),
        },
        "definition_id": {
            "code": "distribution_conformance_request_definition_mismatch",
            "expected": "changed-instance",
            "field": "selection.definition.definition_id",
            "observed": "neutral-instance",
        },
        "schema": {
            "code": "distribution_conformance_request_asset_mismatch",
            "field": "selection.distribution_release.assets",
        },
        "release": {
            "code": "distribution_conformance_request_subject_mismatch",
            "expected": result["authority"]["distribution_release"]["release_digest"],
            "field": "subject.release_digest",
            "observed": "sha256:" + "d" * 64,
        },
    }
    encoded = json.dumps(result["authority"], sort_keys=True)
    assert raw not in encoded
    assert "definition" not in encoded
    assert "secret" not in encoded


def test_authority_refuses_a_package_schema_the_conformance_protocol_cannot_name(
    tmp_path: Path,
) -> None:
    """A package that is not v1 is refused, never relabelled as v1.

    ``artifact.package_schema`` is stamped from the module-level v1 constant
    and the #1178 protocol pins that field to v1, so the equality check in
    ``bind_distribution_conformance_request`` compares two values derived from
    the same constant and can never notice a v2 package.  The refusal has to
    happen here, before the payload is built and digest-sealed.
    """

    v1 = verify_distribution_package(_build(make_fixture(tmp_path / "v1")))
    authority = derive_distribution_conformance_authority(v1, bundle_ids=("cern-team",))
    assert authority.artifact.package_schema == package_impl.SCHEMA
    v1.close_runtime_authority()

    v2 = verify_distribution_package(
        _build(_promote_to_v2(make_fixture(tmp_path / "v2")))
    )
    assert v2.manifest.schema_version == package_impl.SCHEMA_V2
    refusal = _authority_refusal(
        lambda: derive_distribution_conformance_authority(v2, bundle_ids=("cern-team",))
    )
    assert refusal.code == "distribution_conformance_package_schema_unsupported"
    assert refusal.field == "artifact.package_schema"
    assert refusal.expected == package_impl.SCHEMA
    assert refusal.observed == package_impl.SCHEMA_V2

    # The guard reads the declared schema, so the bypass to rule out is a
    # manifest that claims v1 while carrying v2 fields.  The manifest model
    # refuses that at parse time, so no such package can reach this function.
    disguised = v2.manifest.to_mapping()
    disguised["schema_version"] = package_impl.SCHEMA
    with pytest.raises(ValidationError):
        DistributionPackageManifest.model_validate(disguised)
    v2.close_runtime_authority()


@pytest.mark.parametrize("package_version", ["v1", "v2"])
def test_opt_in_v2_authority_binds_verified_schema_and_survives_relocation(
    tmp_path, package_version
) -> None:
    fixture = make_fixture(tmp_path / "authoring")
    if package_version == "v2":
        fixture = _promote_to_v2(fixture)
    archive = _build(fixture)
    relocated = tmp_path / archive.name
    shutil.copyfile(archive, relocated)
    verified = verify_distribution_package(archive)
    try:
        authority = derive_distribution_conformance_authority(
            verified, bundle_ids=("cern-team",), protocol_version="v2"
        )
        assert isinstance(authority, DistributionConformanceAuthorityV2)
        assert authority.artifact.package_schema == verified.manifest.schema_version
        assert (
            authority.artifact.package_schema
            == verified.verification_receipt().package_schema
        )
        assert authority.conformance_protocol == CONFORMANCE_PROTOCOL_V2
        assert authority.schema_version == DISTRIBUTION_CONFORMANCE_AUTHORITY_SCHEMA_V2
        assert (
            authority.artifact.digest
            == "sha256:" + hashlib.sha256(archive.read_bytes()).hexdigest()
        )
        assert (
            authority.manifest.digest
            == verified.verification_receipt().manifest_raw_blob["digest"]
        )
        assert (
            authority.distribution_release.release_digest
            == canonical_release_digest(verified.manifest)
        )
        assert authority.source_revision == verified.manifest.provenance.revision
        assert (
            authority.package_verification_receipt.digest
            == verified.verification_receipt().digest
        )
        schema = distribution_conformance_authority_schema(protocol_version="v2")
        jsonschema.Draft202012Validator.check_schema(schema)
        jsonschema.validate(authority.model_dump(mode="json", by_alias=True), schema)
        assert authority.authority_digest == distribution_conformance_authority_digest(
            authority
        )
        encoded = canonical_distribution_conformance_authority_bytes(authority)
        assert str(tmp_path).encode() not in encoded
        for token in (b"definition", b"credential", b"dsn", b"admission"):
            assert token not in encoded
        definition = ConformanceDefinitionReference(
            definition_id="caller-owned", definition_digest="sha256:" + "8" * 64
        )
        request = _request_for(authority, definition)
        assert (
            bind_distribution_conformance_request(
                authority, request, definition=definition
            )
            == request
        )
        # Synthetic contract-only exchange over actual verified archive identity.
        # The fixture admission values do not claim policy or runtime authority.
        sample = (
            Path(__file__).parents[2]
            / "fixtures/deployment_products/external_conformance_interface/receipt_hermetic_fixture_contract_pass.json"
        )
        receipt_payload = json.loads(sample.read_bytes())
        receipt_payload.update(
            schema_version=CONFORMANCE_RECEIPT_SCHEMA_V2,
            subject=request.subject.model_dump(mode="json"),
            request_digest=conformance_request_digest(request),
            receipt_digest=ZERO_DIGEST,
            authority=request.admission.model_dump(mode="json"),
        )
        receipt_payload["harness"]["harness_protocol"] = CONFORMANCE_PROTOCOL_V2
        receipt = load_conformance_receipt(receipt_payload)
        assert isinstance(receipt, ExternalDistributionConformanceReceiptV2)
        receipt_payload["receipt_digest"] = conformance_receipt_digest(receipt)
        receipt = load_conformance_receipt(receipt_payload)
        validate_conformance_exchange(request, receipt)
        assert receipt.claim.scope == "contract"
        assert receipt.evidence.publish is None
        tampered = _changed_request(
            request,
            lambda value: value["subject"]["artifact"].update(
                package_schema=(
                    package_impl.SCHEMA
                    if package_version == "v2"
                    else package_impl.SCHEMA_V2
                )
            ),
        )
        refusal = _authority_refusal(
            lambda: bind_distribution_conformance_request(
                authority, tampered, definition=definition
            )
        )
        assert refusal.field == "subject.artifact.package_schema"
    finally:
        verified.close_runtime_authority()
    shutil.rmtree(fixture.root)
    reverified = verify_distribution_package(relocated)
    try:
        again = derive_distribution_conformance_authority(
            reverified, bundle_ids=("cern-team",), protocol_version="v2"
        )
        assert canonical_distribution_conformance_authority_bytes(again) == encoded
    finally:
        reverified.close_runtime_authority()


def test_v1_authority_golden_schema_and_bytes() -> None:
    # Captured from unmodified dev's real archive fixture before the v2 change.
    with tempfile.TemporaryDirectory() as root:
        verified = verify_distribution_package(
            _build(make_fixture(Path(root).resolve()))
        )
        try:
            authority = derive_distribution_conformance_authority(
                verified, bundle_ids=("cern-team",)
            )
            assert (
                authority.authority_digest
                == "sha256:d7625fbd355f1dafb09acb9e9561ee674032736710fea1e87b2bae9bd58acac1"
            )
            assert (
                hashlib.sha256(
                    canonical_distribution_conformance_authority_bytes(authority)
                ).hexdigest()
                == "d8cd215408710760baed437ea97d493dc157507098ab6dbb8e50c182cae74595"
            )
        finally:
            verified.close_runtime_authority()
    schema = json.dumps(
        distribution_conformance_authority_schema(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert (
        hashlib.sha256(schema).hexdigest()
        == "97e452c522cc93f6741c9c20e7f873afecd8174b15ceb9fec6d6710a1f6e1f4d"
    )


def test_v2_authority_refuses_receipt_schema_disagreement_and_closed_package(
    tmp_path, monkeypatch
) -> None:
    verified = verify_distribution_package(
        _build(_promote_to_v2(make_fixture(tmp_path)))
    )
    try:
        receipt = verified.verification_receipt()
        # Fault injection into the receipt provider; no such record is accepted as authority.
        with monkeypatch.context() as patch:
            patch.setattr(
                type(verified),
                "verification_receipt",
                lambda self: replace(receipt, package_schema=package_impl.SCHEMA),
            )
            refusal = _authority_refusal(
                lambda: derive_distribution_conformance_authority(
                    verified, bundle_ids=("cern-team",), protocol_version="v2"
                )
            )
        assert refusal.code == "distribution_conformance_package_schema_mismatch"
        assert refusal.field == "package_verification_receipt.package_schema"
        for version in ("v999", "", "okg.external_distribution_conformance/v2"):
            refusal = _authority_refusal(
                lambda: derive_distribution_conformance_authority(
                    verified, bundle_ids=("cern-team",), protocol_version=version
                )
            )
            assert refusal.code == "distribution_conformance_protocol_unsupported"
            with pytest.raises(DistributionConformanceAuthorityError):
                distribution_conformance_authority_schema(protocol_version=version)
    finally:
        verified.close_runtime_authority()
    refusal = _authority_refusal(
        lambda: derive_distribution_conformance_authority(
            verified, bundle_ids=("cern-team",), protocol_version="v2"
        )
    )
    assert refusal.code == "distribution_conformance_package_unavailable"


def test_v2_authority_rejects_v1_request_and_preserves_module_compatibility_bindings(
    tmp_path,
) -> None:
    v1 = verify_distribution_package(_build(make_fixture(tmp_path / "v1")))
    try:
        v1_authority = derive_distribution_conformance_authority(
            v1, bundle_ids=("cern-team",)
        )
        v2_authority = derive_distribution_conformance_authority(
            v1, bundle_ids=("cern-team",), protocol_version="v2"
        )
        definition = ConformanceDefinitionReference(
            definition_id="instance", definition_digest=ZERO_DIGEST
        )
        for authority, other in (
            (v1_authority, v2_authority),
            (v2_authority, v1_authority),
        ):
            request = _request_for(other, definition)
            refusal = _authority_refusal(
                lambda: bind_distribution_conformance_request(
                    authority, request, definition=definition
                )
            )
            assert refusal.code == "distribution_conformance_request_protocol_mismatch"
    finally:
        v1.close_runtime_authority()
    identities = []
    for index, kwargs in enumerate(
        (
            {},
            {"module_declarations": ()},
            {
                "okg_compatibility": OkgCompatibility(
                    min_version="0.2.0", max_version_exclusive="2.0.0"
                )
            },
        )
    ):
        fixture = _promote_to_v2(make_fixture(tmp_path / str(index)), **kwargs)
        verified = verify_distribution_package(_build(fixture))
        try:
            authority = derive_distribution_conformance_authority(
                verified, bundle_ids=("cern-team",), protocol_version="v2"
            )
            identities.append(
                (
                    authority.artifact.digest,
                    authority.manifest.digest,
                    authority.distribution_release.release_digest,
                    authority.authority_digest,
                )
            )
        finally:
            verified.close_runtime_authority()
    for column in zip(*identities):
        assert len(set(column)) == 3
