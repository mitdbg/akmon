"""The install/resolve verb: package + bundle -> plan and lock (#1179 s5).

Every test builds a real package-v2 archive, resolves it in a temporary
directory, and asserts on the bytes written. No database, no network, no
import of packaged code, and no ambient `$OKG_*`: the OKG version and the
environment that secret references resolve against are both injected, so
the arms are hermetic and the digests are stable.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.catalog.module_providers import (
    CrossOwnerSubtypeCollision,
    ModuleProviderError,
)
import okg.deployment as deployment
from okg.substrate.distributions.install_plan import (
    INSTALL_LOCK_SCHEMA,
    INSTALL_PLAN_SCHEMA,
    InstallBundleUnknown,
    InstallConnectorAssetMalformed,
    InstallConnectorRequiredModuleMissing,
    InstallConnectorSdkBoundInvalid,
    InstallConnectorSdkUnsupported,
    InstallConnectorVocabularyUnknown,
    InstallDeclaredAssetMissing,
    InstallLockDrift,
    InstallOkgIncompatible,
    InstallOutputPathExists,
    InstallPackageUnverified,
    NAMESPACE_OKG_SECRETREF,
    InstallPlanError,
    InstallSecretDeclarationUnselected,
    InstallSecretReferenceUnresolved,
    _resolve_secret_references,
    resolve_install_plan,
    secret_reference_token,
    write_install_plan,
)

from ..catalog.test_module_providers import _empty_cross_links, _write_module
from ..catalog.test_package_module_provider import (
    _MODULE_ID,
    _OWNER,
    _SCHEMA_YAML,
    _verified_package,
)


_OKG_VERSION = "1.0.0"
_SECRET_ENV = {"ARCHI_TOKEN": "never-recorded"}
# The default package carries a second asset of kind `source`, because a
# deferred credential may only be declared by a selected source or tool.
_DECLARATION_REF = f"{_OWNER}:lab-units-notes"
_INSTANCE: dict[str, Any] = {
    "name": "demo",
    "secrets": {
        "archi_token": {
            "reference": "env:ARCHI_TOKEN",
            "declaration": _DECLARATION_REF,
        },
    },
}


def _secrets(**entries: dict[str, str]) -> dict[str, Any]:
    return dict(_INSTANCE, secrets=dict(entries))


def _package_path(tmp_path: Path, **kwargs: Any) -> Path:
    """Build a package and hand back only its path, closing the reader."""
    kwargs.setdefault("extra_kind", "source")
    package = _verified_package(tmp_path, **kwargs)
    path = Path(package.path)
    package.close_runtime_authority()
    return path


def _builtin(tmp_path: Path, classes: dict[str, str] | None = None) -> Path:
    root = tmp_path / "builtin"
    _write_module(root, "reasoning", classes or {"Hypothesis": "entity"})
    return root


def _plan(
    tmp_path: Path,
    package: Path,
    *,
    bundle: str = "lab-alpha",
    instance: dict[str, Any] | None = None,
    output: str = "out",
    okg_version: str = _OKG_VERSION,
    sdk_version: str | None = None,
    environ: dict[str, str] | None = None,
    builtin: Path | None = None,
    replace_lock: bool = False,
) -> dict[str, Any]:
    return write_install_plan(
        package,
        bundle,
        _INSTANCE if instance is None else instance,
        tmp_path / output,
        replace_lock=replace_lock,
        okg_version=okg_version,
        sdk_version=sdk_version,
        environ=_SECRET_ENV if environ is None else environ,
        builtin_modules_root=_builtin(tmp_path) if builtin is None else builtin,
        cross_links_root=_empty_cross_links(tmp_path),
    )


# ---------------- (a) the plan, (b) the lock ----------------


def test_plan_and_lock_carry_the_resolved_identity(tmp_path: Path) -> None:
    """The plan names the package, bundle, modules, assets and compatibility."""
    package = _package_path(tmp_path)
    result = _plan(tmp_path, package)

    plan = json.loads(Path(result["plan_path"]).read_text())
    lock = json.loads(Path(result["lock_path"]).read_text())

    assert plan["schema"] == INSTALL_PLAN_SCHEMA
    assert lock["schema"] == INSTALL_LOCK_SCHEMA
    # (a) verified package identity: both digests.
    assert plan["package"]["transport_digest"].startswith("sha256:")
    assert plan["package"]["filename"] == package.name
    assert plan["package"]["release"]["release_digest"].startswith("sha256:")
    # the selected bundle
    assert plan["bundle"]["bundle_id"] == "lab-alpha"
    # resolved module providers: owner:name -> object digest
    assert [item["module_id"] for item in plan["modules"]] == [_MODULE_ID]
    assert plan["modules"][0]["provider_kind"] == "package"
    assert plan["modules"][0]["object_digests"] == lock["modules"][_MODULE_ID]
    # the okg_compatibility check result
    assert plan["okg"] == {
        "version": _OKG_VERSION,
        "compatibility": {"min_version": "0.1.0", "max_version_exclusive": "9.0.0"},
        "satisfied": True,
    }
    # every declared asset with its digest
    assert plan["assets"] and all(
        item["asset_digest"].startswith("sha256:") for item in plan["assets"]
    )
    assert plan["declared_inventory_entry_count"] == len(plan["assets"])
    # the composed catalog proves the packaged module actually resolved;
    # only the bundle's own module is selected, no built-in comes along
    assert plan["catalog"]["subtypes"] == ["lab_run", "lab_unit"]
    assert plan["catalog"]["module_refs"] == [_MODULE_ID]
    assert plan["catalog"]["subtype_origin"]["lab_unit"] == _MODULE_ID
    # (b) the lock records what resolved, so drift is detectable later
    assert lock["plan_digest"] == plan["plan_digest"]
    assert lock["okg_version"] == _OKG_VERSION
    assert lock["bundle_id"] == "lab-alpha"
    assert lock["package_selection"]["transport_digest"] == (
        plan["package"]["transport_digest"]
    )
    assert lock["lock_digest"] == result["lock_digest"]


def test_neither_a_secret_value_nor_its_variable_name_reaches_the_plan(
    tmp_path: Path,
) -> None:
    """The plan records an opaque token, a declaration and its digest."""
    package = _package_path(tmp_path)
    result = _plan(tmp_path, package)
    plan_bytes = Path(result["plan_path"]).read_bytes()
    lock_bytes = Path(result["lock_path"]).read_bytes()
    assert b"never-recorded" not in plan_bytes
    assert b"never-recorded" not in lock_bytes
    # The variable NAME is as sensitive as the value: it names the slot an
    # attacker would read. Neither document may carry it.
    assert b"ARCHI_TOKEN" not in plan_bytes
    assert b"ARCHI_TOKEN" not in lock_bytes
    plan = json.loads(plan_bytes)
    declaration = next(
        item
        for item in plan["assets"]
        if f"{item['owner']}:{item['id']}" == _DECLARATION_REF
    )
    assert plan["secret_references"] == [
        {
            "name": "archi_token",
            "declaration_ref": _DECLARATION_REF,
            "declaration_digest": declaration["asset_digest"],
            "reference": secret_reference_token(
                release_digest=plan["package"]["release"]["release_digest"],
                instance_name="demo",
                declaration_ref=_DECLARATION_REF,
                variable="ARCHI_TOKEN",
            ),
            "resolved": True,
        }
    ]


def test_the_recorded_declaration_digest_is_the_typed_asset_digest(
    tmp_path: Path,
) -> None:
    """Not the object digest.

    `ConformanceOptionalCredentialBinding.declaration_digest` is compared
    against the manifest asset reference, and `DistributionPackageManifest`
    binds every such reference to `asset.digest`, never to the locator
    object digest. The minimal fixture happens to give one payload both
    digests, so the resolver is also driven directly with a selected asset
    whose two digests differ.
    """
    result = _plan(tmp_path, _package_path(tmp_path))
    plan = json.loads(Path(result["plan_path"]).read_text())
    declaration = next(
        item
        for item in plan["assets"]
        if f"{item['owner']}:{item['id']}" == _DECLARATION_REF
    )
    assert plan["secret_references"][0]["declaration_digest"] == (
        declaration["asset_digest"]
    )

    asset_digest = "sha256:" + "ab" * 32
    resolved = _resolve_secret_references(
        {"t": {"reference": "env:ARCHI_TOKEN", "declaration": f"{_OWNER}:s"}},
        _SECRET_ENV,
        instance_name="demo",
        release_digest="sha256:" + "11" * 32,
        selected_assets=[
            {
                "owner": _OWNER,
                "id": "s",
                "kind": "source",
                "asset_digest": asset_digest,
                "object_digest": "sha256:" + "cd" * 32,
            }
        ],
    )
    assert resolved[0]["declaration_digest"] == asset_digest


def test_the_secretref_namespace_is_a_pinned_derivable_constant(tmp_path: Path) -> None:
    """A future edit may not silently repoint every token."""
    import uuid

    # The namespace was derived once with uuid5 from a fixed url; the value
    # itself is the contract. Pin it byte for byte and check it is still a
    # version-5 (name-based) UUID, so a regenerated or random value fails.
    assert NAMESPACE_OKG_SECRETREF == uuid.UUID("99c54f1e-9424-5e07-980a-6e206ac915b5")
    assert NAMESPACE_OKG_SECRETREF.version == 5
    assert NAMESPACE_OKG_SECRETREF.variant == uuid.RFC_4122


def test_the_token_derivation_is_pinned_to_a_golden_literal() -> None:
    """The exact token for fixed inputs, so no seed edit can pass silently.

    The relative assertions below (stable, differs per field) survive any
    permutation of the seed, but the token is a cross-system identifier: a
    consumer-side request producer copies it and a later acknowledgement
    binds a receipt to it, so re-ordering or re-spelling the seed is an
    interoperability break with no local symptom. This literal was
    computed from the derivation as landed; if it changes, the change is
    deliberate and every previously issued token is invalidated.
    """
    assert (
        secret_reference_token(
            release_digest="sha256:" + "11" * 32,
            instance_name="demo",
            declaration_ref="archi-test:lab-units-notes",
            variable="ARCHI_TOKEN",
        )
        == "secretref:319e8001-4f3b-57db-8d0a-ec5f5c57c6bf"
    )


def test_the_token_is_stable_and_differs_per_input(tmp_path: Path) -> None:
    """Deterministic, so the plan stays byte-idempotent; never shared."""
    base = dict(
        release_digest="sha256:" + "11" * 32,
        instance_name="demo",
        declaration_ref=_DECLARATION_REF,
        variable="ARCHI_TOKEN",
    )
    token = secret_reference_token(**base)
    assert secret_reference_token(**base) == token
    assert token.startswith("secretref:")
    assert token != secret_reference_token(**dict(base, instance_name="other"))
    assert token != secret_reference_token(
        **dict(base, declaration_ref=f"{_OWNER}:lab-units-schema")
    )
    assert token != secret_reference_token(
        **dict(base, release_digest="sha256:" + "22" * 32)
    )
    assert token != secret_reference_token(**dict(base, variable="OTHER_TOKEN"))


def test_the_plan_token_satisfies_the_conformance_secret_reference(
    tmp_path: Path,
) -> None:
    """A plan entry round-trips into the validator's binding model.

    Scope: this proves the DECLARATION half of the binding only. The model
    also carries `provider_connection_ref` and `provider_connection_digest`,
    and the request-level selection check requires that asset to be
    selected and of kind `provider-connection`. No package in this
    repository declares such an asset, so the values passed below are an
    `ontology_schema` asset that satisfies the field-level model and would
    NOT satisfy that selection check. Read this as: the plan's
    `declaration_ref`, `declaration_digest` and `secretref:` token are
    accepted verbatim; completing the binding is follow-up work.
    """
    from okg.substrate.contracts.deployment_product.conformance import (
        ConformanceOptionalCredentialBinding,
    )

    result = _plan(tmp_path, _package_path(tmp_path))
    plan = json.loads(Path(result["plan_path"]).read_text())
    entry = plan["secret_references"][0]
    binding = ConformanceOptionalCredentialBinding(
        binding_id="archi-token",
        binding_kind="source",
        declaration_ref=entry["declaration_ref"],
        declaration_digest=entry["declaration_digest"],
        provider_connection_ref=f"{_OWNER}:lab-units-schema",
        provider_connection_digest=plan["assets"][0]["asset_digest"],
        credential_reference=entry["reference"],
        readiness_policy="deferred",
        credential_mode="noninteractive",
    )
    assert binding.credential_reference == entry["reference"]
    assert binding.declaration_ref == _DECLARATION_REF


# ---------------- (d) no runtime effect ----------------


def test_the_verb_writes_only_the_plan_and_the_lock(tmp_path: Path) -> None:
    """No database, no apply, no staged tree left behind."""
    result = _plan(tmp_path, _package_path(tmp_path))
    output = Path(result["plan_path"]).parent
    assert sorted(item.name for item in output.iterdir()) == ["lock.json", "plan.json"]


# ---------------- (e) idempotence ----------------


def test_a_second_identical_run_is_byte_identical(tmp_path: Path) -> None:
    package = _package_path(tmp_path)
    first = _plan(tmp_path, package)
    plan_bytes = Path(first["plan_path"]).read_bytes()
    lock_bytes = Path(first["lock_path"]).read_bytes()

    second = _plan(tmp_path, package)

    assert Path(second["plan_path"]).read_bytes() == plan_bytes
    assert Path(second["lock_path"]).read_bytes() == lock_bytes
    assert second["plan_digest"] == first["plan_digest"]
    assert second["replaced_lock"] is False


def test_resolve_does_not_write_and_matches_the_written_plan(tmp_path: Path) -> None:
    package = _package_path(tmp_path)
    plan, lock = resolve_install_plan(
        package,
        "lab-alpha",
        _INSTANCE,
        okg_version=_OKG_VERSION,
        environ=_SECRET_ENV,
        builtin_modules_root=_builtin(tmp_path),
        cross_links_root=_empty_cross_links(tmp_path),
    )
    assert not (tmp_path / "out").exists()
    result = _plan(tmp_path, package)
    assert plan["plan_digest"] == result["plan_digest"]
    assert lock["lock_digest"] == result["lock_digest"]


# ---------------- (c) one test per refusal code ----------------


def test_an_incompatible_okg_version_refuses(tmp_path: Path) -> None:
    with pytest.raises(InstallOkgIncompatible) as caught:
        _plan(tmp_path, _package_path(tmp_path), okg_version="99.0.0")
    assert caught.value.code == "install_okg_incompatible"
    assert caught.value.data["declared"]["max_version_exclusive"] == "9.0.0"
    assert not (tmp_path / "out").exists()


def test_a_corrupted_package_refuses_before_planning(tmp_path: Path) -> None:
    package = _package_path(tmp_path)
    payload = bytearray(package.read_bytes())
    payload[len(payload) // 2] ^= 0xFF
    package.write_bytes(bytes(payload))
    with pytest.raises(InstallPackageUnverified) as caught:
        _plan(tmp_path, package)
    assert caught.value.code == "install_package_unverified"
    assert not (tmp_path / "out").exists()


def test_an_unknown_bundle_refuses_and_names_the_declared_ones(
    tmp_path: Path,
) -> None:
    with pytest.raises(InstallBundleUnknown) as caught:
        _plan(tmp_path, _package_path(tmp_path), bundle="lab-gamma")
    assert caught.value.code == "install_bundle_unknown"
    assert caught.value.data["declared_bundles"] == ["lab-alpha", "lab-beta"]


def test_an_undeclared_module_refuses(tmp_path: Path) -> None:
    """An instance naming an owner no provider supplies cannot resolve."""
    instance = dict(_INSTANCE, modules=["nobody:absent"])
    with pytest.raises(ModuleProviderError) as caught:
        _plan(tmp_path, _package_path(tmp_path), instance=instance)
    assert caught.value.code == "module_provider_unresolved"
    assert caught.value.data["owner"] == "nobody"


def test_a_cross_owner_subtype_collision_surfaces_from_composition(
    tmp_path: Path,
) -> None:
    """The built-in library and the package both claim `LabUnit`."""
    package = _package_path(tmp_path)
    builtin = _builtin(tmp_path, {"LabUnit": "entity"})
    instance = dict(_INSTANCE, modules=["reasoning"])
    with pytest.raises(CrossOwnerSubtypeCollision) as caught:
        _plan(tmp_path, package, instance=instance, builtin=builtin)
    assert caught.value.code == "duplicate_semantic_ownership"
    assert caught.value.data["phase"] == "pre_mutation"
    assert set(caught.value.data["owners"]) == {"okg", _OWNER}
    assert not (tmp_path / "out").exists()


def test_an_absent_secret_reference_refuses_rather_than_defaulting(
    tmp_path: Path,
) -> None:
    with pytest.raises(InstallSecretReferenceUnresolved) as caught:
        _plan(tmp_path, _package_path(tmp_path), environ={})
    assert caught.value.code == "install_secret_reference_unresolved"
    assert caught.value.data["reason"] == "environment_variable_absent"
    assert caught.value.data["reference"] == "env:ARCHI_TOKEN"


def test_a_literal_secret_value_is_refused_as_not_a_reference(
    tmp_path: Path,
) -> None:
    instance = _secrets(
        archi_token={"reference": "s3cret", "declaration": _DECLARATION_REF},
    )
    with pytest.raises(InstallSecretReferenceUnresolved) as caught:
        _plan(tmp_path, _package_path(tmp_path), instance=instance)
    assert caught.value.data["reason"] == "not_a_reference"
    assert "s3cret" not in json.dumps(caught.value.data)


def test_a_bare_reference_with_no_declaration_refuses(tmp_path: Path) -> None:
    """The older `name: env:NAME` form has no truthful declaration default."""
    instance = dict(_INSTANCE, secrets={"archi_token": "env:ARCHI_TOKEN"})
    with pytest.raises(InstallSecretReferenceUnresolved) as caught:
        _plan(tmp_path, _package_path(tmp_path), instance=instance)
    assert caught.value.data["reason"] == "declaration_missing"
    assert not (tmp_path / "out").exists()


def test_a_declaration_the_bundle_does_not_select_refuses(tmp_path: Path) -> None:
    """A shipped-but-unselected asset declares no credential."""
    instance = _secrets(
        archi_token={
            "reference": "env:ARCHI_TOKEN",
            "declaration": f"{_OWNER}:not-selected",
        },
    )
    with pytest.raises(InstallSecretDeclarationUnselected) as caught:
        _plan(tmp_path, _package_path(tmp_path), instance=instance)
    assert caught.value.code == "install_secret_declaration_unselected"
    assert caught.value.data["reason"] == "declaration_unselected"
    assert not (tmp_path / "out").exists()


def test_a_declaration_that_is_not_a_source_or_tool_refuses(tmp_path: Path) -> None:
    """The ontology schema is selected, but it declares no credential."""
    instance = _secrets(
        archi_token={
            "reference": "env:ARCHI_TOKEN",
            "declaration": f"{_OWNER}:lab-units-schema",
        },
    )
    with pytest.raises(InstallSecretDeclarationUnselected) as caught:
        _plan(tmp_path, _package_path(tmp_path), instance=instance)
    assert caught.value.data["reason"] == "declaration_kind_unsupported"
    assert caught.value.data["declaration_kind"] == "ontology_schema"
    assert not (tmp_path / "out").exists()


def test_a_tool_declaration_is_accepted_like_a_source(tmp_path: Path) -> None:
    package = _package_path(tmp_path, extra_kind="tool")
    result = _plan(tmp_path, package)
    plan = json.loads(Path(result["plan_path"]).read_text())
    assert plan["secret_references"][0]["declaration_ref"] == _DECLARATION_REF


def test_a_malformed_declaration_reference_refuses(tmp_path: Path) -> None:
    """A declaration that is not an owner-qualified id resolves to nothing."""
    instance = _secrets(
        archi_token={
            "reference": "env:ARCHI_TOKEN",
            "declaration": "NOT-An-Owner-Qualified-Id",
        },
    )
    with pytest.raises(InstallSecretDeclarationUnselected) as caught:
        _plan(tmp_path, _package_path(tmp_path), instance=instance)
    assert caught.value.code == "install_secret_declaration_unselected"
    assert caught.value.data["reason"] == "declaration_malformed"
    # The branch knows no resolvable declaration, so it names none.
    assert "declaration_ref" not in caught.value.data
    assert not (tmp_path / "out").exists()


def test_a_secret_entry_that_is_not_a_mapping_refuses(tmp_path: Path) -> None:
    """A list is neither the bare string form nor the mapping form."""
    instance = dict(_INSTANCE, secrets={"archi_token": ["env:ARCHI_TOKEN"]})
    with pytest.raises(InstallSecretReferenceUnresolved) as caught:
        _plan(tmp_path, _package_path(tmp_path), instance=instance)
    assert caught.value.data["reason"] == "declaration_missing"
    assert caught.value.data["secret_name"] == "archi_token"
    assert not (tmp_path / "out").exists()


def test_an_unrecognized_secret_key_refuses_rather_than_being_ignored(
    tmp_path: Path,
) -> None:
    """A misspelt or invented key is a config the operator must fix."""
    instance = _secrets(
        archi_token={
            "reference": "env:ARCHI_TOKEN",
            "declaration": _DECLARATION_REF,
            "value": "s3cret",
        },
    )
    with pytest.raises(InstallPlanError) as caught:
        _plan(tmp_path, _package_path(tmp_path), instance=instance)
    assert caught.value.code == "install_plan_refused"
    assert caught.value.data["unknown_keys"] == ["value"]
    assert caught.value.data["accepted_keys"] == [
        "aliases", "declaration", "reference", "required",
    ]
    assert not (tmp_path / "out").exists()


# ---------------- refusal precedence (pinned, not incidental) ----------------


def test_an_incompatible_package_is_reported_before_an_unset_variable(
    tmp_path: Path,
) -> None:
    """Secret RESOLUTION needs the selection, so it runs after the package.

    Both faults are present: the OKG build is outside the declared window
    AND the referenced variable is unset. A conformance receipt records
    the refusal code, so which one wins must be predictable.
    """
    with pytest.raises(InstallOkgIncompatible):
        _plan(tmp_path, _package_path(tmp_path), okg_version="99.0.0", environ={})
    assert not (tmp_path / "out").exists()


def test_an_unknown_bundle_is_reported_before_an_unset_variable(
    tmp_path: Path,
) -> None:
    """Same ordering: no selection means no declaration to bind against."""
    with pytest.raises(InstallBundleUnknown):
        _plan(tmp_path, _package_path(tmp_path), bundle="lab-gamma", environ={})
    assert not (tmp_path / "out").exists()


def test_a_malformed_instance_config_is_reported_before_the_package(
    tmp_path: Path,
) -> None:
    """The operator's own file is readable without trusting a package byte.

    The package is corrupted AND the config uses the old bare-string
    secret form. The config shape wins, because it is read first and is
    the fault the operator can act on.
    """
    package = _package_path(tmp_path)
    payload = bytearray(package.read_bytes())
    payload[len(payload) // 2] ^= 0xFF
    package.write_bytes(bytes(payload))
    instance = dict(_INSTANCE, secrets={"archi_token": "env:ARCHI_TOKEN"})
    with pytest.raises(InstallSecretReferenceUnresolved) as caught:
        _plan(tmp_path, package, instance=instance)
    assert caught.value.data["reason"] == "declaration_missing"
    assert not (tmp_path / "out").exists()


def test_a_pre_existing_output_path_refuses(tmp_path: Path) -> None:
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "notes.txt").write_text("operator data")
    with pytest.raises(InstallOutputPathExists) as caught:
        _plan(tmp_path, _package_path(tmp_path))
    assert caught.value.code == "install_output_path_exists"
    assert (tmp_path / "out" / "notes.txt").read_text() == "operator data"


def test_an_unrecognized_instance_key_refuses(tmp_path: Path) -> None:
    instance = dict(_INSTANCE, endpoints={"api": "https://example.invalid"})
    with pytest.raises(InstallPlanError) as caught:
        _plan(tmp_path, _package_path(tmp_path), instance=instance)
    assert caught.value.data["unknown_keys"] == ["endpoints"]


def test_a_declared_asset_the_package_does_not_carry_refuses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Package v2 refuses a dangling reference, so the adapter is proved here.

    A real archive cannot reach this arm: the manifest validators bind every
    bundle reference to a packaged asset. The translation from the landed
    lifecycle refusal to this verb's stable code still has to hold, so the
    seam is driven directly.
    """
    from okg.substrate import deployment_product_lifecycle as lifecycle

    def _refuse_selection(*_args: object, **_kwargs: object) -> None:
        raise lifecycle.ProductLifecycleError(
            "selected package assets do not resolve"
        )

    monkeypatch.setattr(
        lifecycle, "package_selection_from_verified", _refuse_selection
    )
    with pytest.raises(InstallDeclaredAssetMissing) as caught:
        _plan(tmp_path, _package_path(tmp_path))
    assert caught.value.code == "install_declared_asset_missing"
    assert caught.value.data["bundle_id"] == "lab-alpha"


# ---------------- (c) drift against the lock ----------------


def test_a_changed_package_is_drift_and_refuses(tmp_path: Path) -> None:
    """Re-planning against a mutated distribution must not silently relock."""
    first = _package_path(tmp_path / "v1")
    _plan(tmp_path, first)
    lock_before = (tmp_path / "out" / "lock.json").read_bytes()

    changed = _package_path(
        tmp_path / "v2",
        schema_payload=_SCHEMA_YAML + b"  LabSample:\n    annotations:\n"
        b"      archetype: entity\n",
    )
    with pytest.raises(InstallLockDrift) as caught:
        _plan(tmp_path, changed)
    assert caught.value.code == "install_lock_drift"
    assert caught.value.data["recorded_lock_digest"] != (
        caught.value.data["resolved_lock_digest"]
    )
    assert (tmp_path / "out" / "lock.json").read_bytes() == lock_before


def test_replace_lock_accepts_the_new_resolution(tmp_path: Path) -> None:
    first = _package_path(tmp_path / "v1")
    _plan(tmp_path, first)
    lock_before = (tmp_path / "out" / "lock.json").read_bytes()

    changed = _package_path(
        tmp_path / "v2",
        schema_payload=_SCHEMA_YAML + b"  LabSample:\n    annotations:\n"
        b"      archetype: entity\n",
    )
    result = _plan(tmp_path, changed, replace_lock=True)
    assert result["replaced_lock"] is True
    after = (tmp_path / "out" / "lock.json").read_bytes()
    assert after != lock_before
    plan = json.loads((tmp_path / "out" / "plan.json").read_text())
    assert "lab_sample" in plan["catalog"]["subtypes"]


# ---------------- (f) declared connectors, #1181 slice 8 ----------------
#
# A `connector` asset is a declaration read back at resolve time: the plan
# records the window and the modules the connector needs, and every check
# the SDK makes when the registration is *built* is made again here, against
# the `okg.deployment` build that would run it.


_CONNECTOR_REF = f"{_OWNER}:lab-units-connector"


def _registration(**overrides: Any) -> Any:
    kwargs: dict[str, Any] = dict(
        connector_type="lab_units_crawl",
        version="2.1.0",
        module="archi.connectors.lab_units",
        class_name="LabUnitsConnector",
        ownership_id="archi.lab_units",
        source_class="discovery_crawl",
        trust_label="archi_operated",
        sdk_range=deployment.SdkVersionRange("1.0.0", "2.0.0"),
        auth_modes=("env_credential",),
        capabilities=("checkpoint_resume",),
        required_modules=(_MODULE_ID,),
    )
    kwargs.update(overrides)
    return deployment.ConnectorRegistration(**kwargs)


def _connector_payload(*, mutate: dict[str, Any] | None = None, **overrides: Any) -> bytes:
    """The declared asset body, optionally edited past what the SDK builds.

    A package authored by an older SDK can carry a window or a vocabulary
    this build refuses, which is the whole reason resolve re-checks; those
    arms edit the JSON rather than the object.
    """
    payload = _registration(**overrides).as_asset_payload()
    if mutate is not None:
        payload["registration"].update(mutate)
    return json.dumps(payload).encode("utf-8")


def test_a_declared_connector_is_recorded_in_the_plan_and_the_lock(
    tmp_path: Path,
) -> None:
    package = _package_path(tmp_path, connector_payload=_connector_payload())
    result = _plan(tmp_path, package)
    plan = json.loads(Path(result["plan_path"]).read_text())
    lock = json.loads(Path(result["lock_path"]).read_text())

    assert plan["connectors"] == [
        {
            "asset_ref": _CONNECTOR_REF,
            "connector_type": "lab_units_crawl",
            "connector_version": "2.1.0",
            "object_digest": lock["connectors"][_CONNECTOR_REF],
            "sdk_range": {
                "min_version": "1.0.0",
                "max_version_exclusive": "2.0.0",
            },
            "sdk_satisfied": True,
            "required_modules": [_MODULE_ID],
        }
    ]
    assert lock["connectors"][_CONNECTOR_REF].startswith("sha256:")
    assert result["connectors"] == [_CONNECTOR_REF]


def test_the_running_sdk_version_is_what_the_window_is_checked_against(
    tmp_path: Path,
) -> None:
    """With no injected version the check reads `okg.deployment.__version__`."""
    below = deployment.SdkVersionRange(
        str(int(deployment.__version__.split(".")[0]) + 1) + ".0.0", "99.0.0"
    )
    package = _package_path(
        tmp_path, connector_payload=_connector_payload(sdk_range=below)
    )
    with pytest.raises(InstallConnectorSdkUnsupported) as caught:
        _plan(tmp_path, package)
    assert caught.value.data["sdk_version"] == deployment.__version__


def test_a_connector_outside_the_running_sdk_window_refuses(
    tmp_path: Path,
) -> None:
    package = _package_path(
        tmp_path,
        connector_payload=_connector_payload(
            sdk_range=deployment.SdkVersionRange("2.0.0", "3.0.0")
        ),
    )
    with pytest.raises(InstallConnectorSdkUnsupported) as caught:
        _plan(tmp_path, package, sdk_version="1.4.0")
    assert caught.value.code == "install_connector_sdk_unsupported"
    assert caught.value.data == {
        "asset_ref": _CONNECTOR_REF,
        "connector_type": "lab_units_crawl",
        "sdk_version": "1.4.0",
        "min_version": "2.0.0",
        "max_version_exclusive": "3.0.0",
    }
    assert not (tmp_path / "out").exists()


def test_a_prerelease_build_is_compared_by_its_base_release(tmp_path: Path) -> None:
    """`1.0.0rc2` carries the feature `min_version: 1.0.0` asks for."""
    package = _package_path(tmp_path, connector_payload=_connector_payload())
    result = _plan(tmp_path, package, sdk_version="1.0.0rc2")
    plan = json.loads(Path(result["plan_path"]).read_text())
    assert plan["connectors"][0]["sdk_satisfied"] is True


def test_a_non_release_declared_bound_refuses(tmp_path: Path) -> None:
    """Refused, not normalized: a `1.0.0rc1` bound cannot change a verdict
    that compares base releases, exactly as package v2 refuses one on
    `okg_compatibility`."""
    package = _package_path(
        tmp_path,
        connector_payload=_connector_payload(
            mutate={
                "sdk_range": {
                    "min_version": "1.0.0rc1",
                    "max_version_exclusive": "2.0.0",
                }
            }
        ),
    )
    with pytest.raises(InstallConnectorSdkBoundInvalid) as caught:
        _plan(tmp_path, package)
    assert caught.value.code == "install_connector_sdk_bound_invalid"
    assert caught.value.data["field"] == "min_version"
    assert caught.value.data["value"] == "1.0.0rc1"
    assert (
        caught.value.data["registration_error_code"]
        == "connector_sdk_bound_invalid"
    )


def test_an_unknown_capability_refuses_at_resolve_time(tmp_path: Path) -> None:
    package = _package_path(
        tmp_path,
        connector_payload=_connector_payload(
            mutate={"capabilities": ["teleport"]}
        ),
    )
    with pytest.raises(InstallConnectorVocabularyUnknown) as caught:
        _plan(tmp_path, package)
    assert caught.value.code == "install_connector_vocabulary_unknown"
    assert caught.value.data["capability"] == "teleport"
    assert (
        caught.value.data["registration_error_code"]
        == "connector_capability_unknown"
    )


def test_an_unknown_auth_mode_refuses_at_resolve_time(tmp_path: Path) -> None:
    package = _package_path(
        tmp_path,
        connector_payload=_connector_payload(
            mutate={"auth_modes": ["mtls_client_cert"]}
        ),
    )
    with pytest.raises(InstallConnectorVocabularyUnknown) as caught:
        _plan(tmp_path, package)
    assert caught.value.data["auth_mode"] == "mtls_client_cert"


def test_a_required_module_the_install_does_not_resolve_refuses(
    tmp_path: Path,
) -> None:
    package = _package_path(
        tmp_path,
        connector_payload=_connector_payload(
            required_modules=(_MODULE_ID, "archi-test:absent")
        ),
    )
    with pytest.raises(InstallConnectorRequiredModuleMissing) as caught:
        _plan(tmp_path, package)
    assert caught.value.code == "install_connector_required_module_missing"
    assert caught.value.data["missing_modules"] == ["archi-test:absent"]
    assert caught.value.data["resolved_modules"] == [_MODULE_ID]


def test_a_required_builtin_module_matches_its_bare_instance_entry(
    tmp_path: Path,
) -> None:
    """`modules: [reasoning]` and `required_modules: [okg:reasoning]` name
    the same module; a connector always writes the qualified spelling."""
    package = _package_path(
        tmp_path,
        connector_payload=_connector_payload(
            required_modules=(_MODULE_ID, "okg:reasoning")
        ),
    )
    result = _plan(
        tmp_path, package, instance=dict(_INSTANCE, modules=["reasoning"])
    )
    plan = json.loads(Path(result["plan_path"]).read_text())
    assert plan["connectors"][0]["required_modules"] == [
        _MODULE_ID,
        "okg:reasoning",
    ]


def test_a_connector_asset_that_is_not_json_refuses(tmp_path: Path) -> None:
    package = _package_path(tmp_path, connector_payload=b"not json at all")
    with pytest.raises(InstallConnectorAssetMalformed) as caught:
        _plan(tmp_path, package)
    assert caught.value.code == "install_connector_asset_malformed"
    assert caught.value.data["asset_ref"] == _CONNECTOR_REF


def test_a_connector_body_with_an_unknown_field_refuses_with_a_code(
    tmp_path: Path,
) -> None:
    """The dataclass would raise a bare `TypeError`; the SDK codes it."""
    package = _package_path(
        tmp_path,
        connector_payload=_connector_payload(mutate={"retry_budget": 3}),
    )
    with pytest.raises(InstallConnectorAssetMalformed) as caught:
        _plan(tmp_path, package)
    assert (
        caught.value.data["registration_error_code"]
        == "connector_asset_payload_invalid"
    )


def test_a_repackaged_connector_is_drift_against_the_recorded_lock(
    tmp_path: Path,
) -> None:
    """The connector's object digest rides in the lock, so re-declaring it
    with different bytes is drift rather than a silent swap."""
    first = _plan(
        tmp_path,
        _package_path(
            tmp_path / "a", connector_payload=_connector_payload()
        ),
    )
    recorded_digest = json.loads(
        Path(first["lock_path"]).read_text()
    )["connectors"][_CONNECTOR_REF]
    changed = _package_path(
        tmp_path / "b",
        connector_payload=_connector_payload(version="2.2.0"),
    )
    with pytest.raises(InstallLockDrift) as caught:
        _plan(tmp_path, changed)
    assert caught.value.code == "install_lock_drift"
    assert caught.value.data["recorded_lock_digest"] == first["lock_digest"]

    accepted = _plan(tmp_path, changed, replace_lock=True)
    lock = json.loads(Path(accepted["lock_path"]).read_text())
    assert lock["connectors"][_CONNECTOR_REF] != recorded_digest


def test_a_second_run_with_a_connector_is_byte_identical(tmp_path: Path) -> None:
    package = _package_path(tmp_path, connector_payload=_connector_payload())
    first = _plan(tmp_path, package)
    plan_bytes = Path(first["plan_path"]).read_bytes()
    lock_bytes = Path(first["lock_path"]).read_bytes()
    second = _plan(tmp_path, package)
    assert Path(second["plan_path"]).read_bytes() == plan_bytes
    assert Path(second["lock_path"]).read_bytes() == lock_bytes


# ---------------- issue 1794: optional credentials are deferred ----------------


def _optional(**extra: Any) -> dict[str, Any]:
    return _secrets(
        archi_token={
            "reference": "env:ARCHI_TOKEN",
            "declaration": _DECLARATION_REF,
            "required": False,
            **extra,
        },
    )


def test_an_unset_optional_credential_is_deferred_not_refused(tmp_path: Path) -> None:
    result = _plan(tmp_path, _package_path(tmp_path), instance=_optional(), environ={})
    plan_bytes = Path(result["plan_path"]).read_bytes()
    assert b"ARCHI_TOKEN" not in plan_bytes
    assert b"ARCHI_TOKEN" not in Path(result["lock_path"]).read_bytes()
    plan = json.loads(plan_bytes)
    declaration = next(
        item
        for item in plan["assets"]
        if f"{item['owner']}:{item['id']}" == _DECLARATION_REF
    )
    (entry,) = plan["secret_references"]
    supply_later = entry.pop("supply_later")
    assert supply_later.startswith("export the variable")
    assert entry == {
        "name": "archi_token",
        "declaration_ref": _DECLARATION_REF,
        "declaration_digest": declaration["asset_digest"],
        "reference": secret_reference_token(
            release_digest=plan["package"]["release"]["release_digest"],
            instance_name="demo",
            declaration_ref=_DECLARATION_REF,
            variable="ARCHI_TOKEN",
        ),
        "required": False,
        "readiness_policy": "deferred",
    }
    # Reported, not recorded: the result says it is deferred right now.
    (deferred,) = result["deferred_credentials"]
    assert deferred["reference"] == entry["reference"]


def test_an_unset_required_credential_still_refuses(tmp_path: Path) -> None:
    instance = _optional(required=True, aliases=["env:ARCHI_TOKEN_OLD"])
    with pytest.raises(InstallSecretReferenceUnresolved) as caught:
        _plan(tmp_path, _package_path(tmp_path), instance=instance, environ={})
    assert caught.value.data["reason"] == "environment_variable_absent"
    # The refusal names every variable that would satisfy it, never a value.
    assert caught.value.data["aliases"] == ["env:ARCHI_TOKEN_OLD"]
    assert "ARCHI_TOKEN_OLD" in (caught.value.fix_hint or "")
    assert not (tmp_path / "out").exists()


def test_the_optional_credential_state_never_enters_a_digest(tmp_path: Path) -> None:
    """Set or unset, an optional credential yields the same plan and lock.

    The lock digest is the packaged release identity's ``install_lock``, so
    a difference here is what made supplying a deferred credential later
    refuse as ``release_or_configuration_drift``.
    """
    from okg.substrate.distributions.packaged_release import (
        PackagedReleaseIdentity,
        verify_plan_binding,
    )

    package = _package_path(tmp_path)
    unset = _plan(tmp_path, package, instance=_optional(), environ={}, output="unset")
    supplied = _plan(tmp_path, package, instance=_optional(), output="supplied")
    via_alias = _plan(
        tmp_path,
        package,
        instance=_optional(aliases=["env:ARCHI_TOKEN_OLD"]),
        environ={"ARCHI_TOKEN_OLD": "never-recorded"},
        output="alias",
    )
    assert unset["lock_digest"] == supplied["lock_digest"]
    assert unset["plan_digest"] == supplied["plan_digest"]
    assert (
        Path(unset["plan_path"]).read_bytes()
        == Path(supplied["plan_path"]).read_bytes()
    )
    # An alias changes the instance config, so it may change the plan, but it
    # must be set-or-unset invariant too.
    alias_unset = _plan(
        tmp_path,
        package,
        instance=_optional(aliases=["env:ARCHI_TOKEN_OLD"]),
        environ={},
        output="alias-unset",
    )
    assert alias_unset["lock_digest"] == via_alias["lock_digest"]
    assert len(unset["deferred_credentials"]) == 1
    assert supplied["deferred_credentials"] == []

    def identity(result: dict[str, Any]) -> PackagedReleaseIdentity:
        plan = json.loads(Path(result["plan_path"]).read_bytes())
        lock = json.loads(Path(result["lock_path"]).read_bytes())
        recorded = PackagedReleaseIdentity(
            "sha256:" + "1" * 64, "sha256:" + "2" * 64, "sha256:" + "3" * 64,
            lock["lock_digest"], "installed-profile/v1",
        )
        verify_plan_binding(recorded, plan, lock)
        return recorded

    identity(supplied).require_same(identity(unset))


def test_a_deferred_plan_replans_with_the_variable_set_into_the_same_output(
    tmp_path: Path,
) -> None:
    """Following ``supply_later`` must not read as lock drift."""
    package = _package_path(tmp_path)
    first = _plan(tmp_path, package, instance=_optional(), environ={})
    lock_bytes = Path(first["lock_path"]).read_bytes()
    second = _plan(tmp_path, package, instance=_optional())
    assert second["lock_digest"] == first["lock_digest"]
    assert Path(second["lock_path"]).read_bytes() == lock_bytes
    assert second["replaced_lock"] is False
    # And back: unsetting it again is not drift either.
    third = _plan(tmp_path, package, instance=_optional(), environ={})
    assert third["lock_digest"] == first["lock_digest"]


def test_a_supplied_optional_credential_carries_the_required_token(
    tmp_path: Path,
) -> None:
    package = _package_path(tmp_path)
    optional = _plan(tmp_path, package, instance=_optional())
    required = _plan(tmp_path, package, output="required")
    (deferrable,) = json.loads(Path(optional["plan_path"]).read_bytes())[
        "secret_references"
    ]
    (resolved,) = json.loads(Path(required["plan_path"]).read_bytes())[
        "secret_references"
    ]
    assert deferrable["reference"] == resolved["reference"]
    assert resolved["resolved"] is True
    assert optional["deferred_credentials"] == []


# ---- one helper splits plan secrets into supplied and deferred ----


#: Issue #2560: the deferred-credential check was `okg doctor sources`, which
#: the parser rejects. It is now built by `okg_command` from the parser.
_CHECK_WITH = "okg doctor --deployment demo --check source-auth"


def test_the_deferred_credential_check_is_a_command_okg_parses() -> None:
    import argparse
    import shlex

    from okg.substrate.cli.main import _build_parser

    tokens = shlex.split(_CHECK_WITH)
    assert tokens[0] == "okg"
    args = _build_parser().parse_args(tokens[1:])
    assert (args.command, args.deployment, args.check) == (
        "doctor", "demo", "source-auth",
    )
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["doctor", "sources"])
    # `--check` is free text to argparse; the doctor registry decides which
    # names run. `source-auth` reports each source's credential_refs.
    from okg.substrate.cli.doctor import _build_doctor_registry

    names = _build_doctor_registry(argparse.Namespace(deployment=None)).names()
    assert "source-auth" in names


def _two_secret_instance() -> dict[str, Any]:
    return _secrets(
        archi_token={
            "reference": "env:ARCHI_TOKEN",
            "declaration": _DECLARATION_REF,
        },
        jira_token={
            "reference": "env:JIRA_TOKEN",
            "declaration": _DECLARATION_REF,
            "required": False,
            "aliases": ["env:JIRA_TOKEN_OLD"],
        },
    )


@pytest.mark.parametrize(
    ("environ", "deferred_names"),
    [
        ({"ARCHI_TOKEN": "x"}, ["jira_token"]),
        ({"ARCHI_TOKEN": "x", "JIRA_TOKEN": "y"}, []),
        ({"ARCHI_TOKEN": "x", "JIRA_TOKEN_OLD": "y"}, []),
    ],
)
def test_split_plan_secrets_never_lists_a_deferred_token_as_supplied(
    tmp_path: Path, environ: dict[str, str], deferred_names: list[str]
) -> None:
    from okg.substrate.distributions.install_plan import split_plan_secrets

    instance = _two_secret_instance()
    result = _plan(tmp_path, _package_path(tmp_path), instance=instance, environ=environ)
    plan = json.loads(Path(result["plan_path"]).read_bytes())
    supplied, deferred = split_plan_secrets(plan, instance, environ)
    assert [item["name"] for item in deferred] == deferred_names
    supplied_tokens = {item["reference"] for item in supplied}
    assert supplied_tokens.isdisjoint(item["reference"] for item in deferred)
    assert len(supplied) + len(deferred) == len(plan["secret_references"])
    assert result["deferred_credentials"] == deferred
    for item in deferred:
        assert item["check_with"] == _CHECK_WITH
        assert "JIRA_TOKEN" not in json.dumps(item)


def test_split_plan_secrets_refuses_an_instance_naming_another_variable(
    tmp_path: Path,
) -> None:
    """Same secret name, different variable: the token check refuses it.

    A name-only match would read OPT_B (set) and report the credential the
    plan was made for (OPT_A, unset) as supplied.
    """
    from okg.substrate.distributions.install_plan import split_plan_secrets

    def instance(variable: str) -> dict[str, Any]:
        return _secrets(
            opt={
                "reference": f"env:{variable}",
                "declaration": _DECLARATION_REF,
                "required": False,
            },
        )

    result = _plan(tmp_path, _package_path(tmp_path), instance=instance("OPT_A"), environ={})
    plan = json.loads(Path(result["plan_path"]).read_bytes())
    assert split_plan_secrets(plan, instance("OPT_A"), {})[1]
    with pytest.raises(InstallPlanError) as caught:
        split_plan_secrets(plan, instance("OPT_B"), {"OPT_B": "set"})
    assert caught.value.data["reason"] == "plan_instance_secret_mismatch"
    assert caught.value.data["secret_name"] == "opt"
    other_name = dict(instance("OPT_A"), name="other")
    with pytest.raises(InstallPlanError):
        split_plan_secrets(plan, other_name, {})


def test_split_plan_secrets_refuses_an_instance_that_does_not_match_the_plan(
    tmp_path: Path,
) -> None:
    from okg.substrate.distributions.install_plan import split_plan_secrets

    result = _plan(tmp_path, _package_path(tmp_path), instance=_two_secret_instance())
    plan = json.loads(Path(result["plan_path"]).read_bytes())
    with pytest.raises(InstallPlanError) as caught:
        split_plan_secrets(plan, _INSTANCE, _SECRET_ENV)
    assert caught.value.data["reason"] == "plan_instance_secret_mismatch"


def test_an_alias_supplies_a_required_credential_under_the_declared_token(
    tmp_path: Path,
) -> None:
    instance = _optional(required=True, aliases=["env:ARCHI_TOKEN_OLD"])
    result = _plan(
        tmp_path,
        _package_path(tmp_path),
        instance=instance,
        environ={"ARCHI_TOKEN_OLD": "never-recorded"},
    )
    plan_bytes = Path(result["plan_path"]).read_bytes()
    assert b"ARCHI_TOKEN" not in plan_bytes
    plan = json.loads(plan_bytes)
    (entry,) = plan["secret_references"]
    assert entry["resolved"] is True
    assert entry["reference"] == secret_reference_token(
        release_digest=plan["package"]["release"]["release_digest"],
        instance_name="demo",
        declaration_ref=_DECLARATION_REF,
        variable="ARCHI_TOKEN",
    )


@pytest.mark.parametrize(
    ("extra", "reason"),
    [
        ({"required": "no"}, "required_not_boolean"),
        ({"aliases": "env:OTHER"}, "not_a_reference"),
        ({"aliases": ["OTHER"]}, "not_a_reference"),
    ],
)
def test_a_malformed_required_or_alias_refuses(
    tmp_path: Path, extra: dict[str, Any], reason: str
) -> None:
    with pytest.raises(InstallPlanError) as caught:
        _plan(tmp_path, _package_path(tmp_path), instance=_optional(**extra))
    assert caught.value.data["reason"] == reason
    assert not (tmp_path / "out").exists()


def test_a_deferred_entry_is_a_v2_binding_without_a_provider_connection(
    tmp_path: Path,
) -> None:
    """The deferred entry feeds the v2 binding with no provider-connection asset."""
    from okg.substrate.contracts.deployment_product.conformance import (
        ConformanceOptionalCredentialBindingV2,
    )

    result = _plan(tmp_path, _package_path(tmp_path), instance=_optional(), environ={})
    (entry,) = json.loads(Path(result["plan_path"]).read_bytes())["secret_references"]
    binding = ConformanceOptionalCredentialBindingV2(
        binding_id="archi-token",
        binding_kind="source",
        declaration_ref=entry["declaration_ref"],
        declaration_digest=entry["declaration_digest"],
        credential_reference=entry["reference"],
        readiness_policy=entry["readiness_policy"],
        credential_mode="noninteractive",
    )
    assert binding.provider_connection_ref is None


# ---- issue 1794: `okg install` passes the profile's credential declarations ----


def _profile_with_sources(root: Path, files: dict[str, str]) -> Any:
    from types import SimpleNamespace

    directory = root / "consumer" / "bundles" / "demo" / "source-defaults"
    directory.mkdir(parents=True)
    for name, text in files.items():
        (directory / name).write_text(text)
    return SimpleNamespace(source_defaults_dir=directory)


_CONSUMER_DIGEST = "sha256:" + "ab" * 32
_OWNER_ID = _DECLARATION_REF.split(":", 1)


def _selected(member: str, digest: str = _CONSUMER_DIGEST) -> dict[str, Any]:
    return {
        "owner": _OWNER_ID[0],
        "id": _OWNER_ID[1],
        "kind": "source",
        "member_path": member,
        "object_digest": digest,
    }


_JIRA = """\
jira:
  module: consumer.sources.jira
  class: Jira
  credential_refs: [ARCHI_TOKEN]
  credential_aliases:
    ARCHI_TOKEN: [ARCHI_TOKEN_OLD]
  required_for_baseline: {required}
"""
_NO_CREDENTIAL = """\
releases:
  module: consumer.sources.releases
  class: Releases
"""


def _profile_secrets(tmp_path: Path, *, required: str = "false", assets=None):
    from okg.substrate.distributions.install_execution import (
        profile_credential_secrets,
    )

    profile = _profile_with_sources(
        tmp_path,
        {
            "jira.yaml": _JIRA.format(required=required),
            "releases.yaml": _NO_CREDENTIAL,
            "twiki.yaml.example": _JIRA.format(required="true"),
        },
    )
    member = "consumer/bundles/demo/source-defaults/jira.yaml"
    return profile_credential_secrets(
        profile,
        assets=[_selected(member)] if assets is None else assets,
        installed_root=tmp_path,
        consumer_digest=_CONSUMER_DIGEST,
    )


@pytest.mark.parametrize(("flag", "required"), [("false", False), ("true", True)])
def test_the_profile_credential_declarations_become_plan_secrets(
    tmp_path: Path, flag: str, required: bool
) -> None:
    assert _profile_secrets(tmp_path, required=flag) == {
        "jira.credential.0": {
            "reference": "env:ARCHI_TOKEN",
            "declaration": _DECLARATION_REF,
            "required": required,
            "aliases": ["env:ARCHI_TOKEN_OLD"],
        },
    }


def test_install_defers_an_unset_optional_profile_credential(tmp_path: Path) -> None:
    secrets = _profile_secrets(tmp_path, required="false")
    result = _plan(
        tmp_path,
        _package_path(tmp_path),
        instance={"name": "demo", "secrets": secrets},
        environ={},
    )
    (entry,) = json.loads(Path(result["plan_path"]).read_bytes())["secret_references"]
    assert entry["name"] == "jira.credential.0"
    assert entry["readiness_policy"] == "deferred"
    assert entry["reference"].startswith("secretref:")
    (deferred,) = result["deferred_credentials"]
    assert deferred["source_id"] == "jira"


def test_install_refuses_an_unset_required_profile_credential(tmp_path: Path) -> None:
    secrets = _profile_secrets(tmp_path, required="true")
    with pytest.raises(InstallSecretReferenceUnresolved):
        _plan(
            tmp_path,
            _package_path(tmp_path),
            instance={"name": "demo", "secrets": secrets},
            environ={},
        )
    assert not (tmp_path / "out").exists()


def test_a_credential_file_the_bundle_does_not_select_refuses(tmp_path: Path) -> None:
    from okg.substrate.errors import SubstrateError

    other = "consumer/bundles/demo/source-defaults/releases.yaml"
    with pytest.raises(SubstrateError) as caught:
        _profile_secrets(tmp_path, assets=[_selected(other)])
    assert caught.value.data["reason"] == "profile_dependency_not_selected"
    with pytest.raises(SubstrateError) as caught:
        _profile_secrets(
            tmp_path / "wrong-digest",
            assets=[
                _selected(
                    "consumer/bundles/demo/source-defaults/jira.yaml",
                    digest="sha256:" + "cd" * 32,
                )
            ],
        )
    assert caught.value.data["reason"] == "profile_dependency_not_selected"


@pytest.mark.parametrize(
    "body",
    [
        "jira:\n  credential_refs: ARCHI_TOKEN\n",
        "jira:\n  credential_refs: [NOT-AN-IDENTIFIER]\n",
        "jira:\n  credential_refs: [ARCHI_TOKEN]\n  required_for_baseline: yes please\n",
        "jira:\n  credential_refs: [ARCHI_TOKEN]\n  credential_aliases: [X]\n",
        "jira:\n  credential_refs: [ARCHI_TOKEN]\n  credential_aliases: {ARCHI_TOKEN: OLD}\n",
    ],
)
def test_a_malformed_profile_credential_declaration_refuses(
    tmp_path: Path, body: str
) -> None:
    from okg.substrate.distributions.install_execution import (
        profile_credential_secrets,
    )
    from okg.substrate.errors import SubstrateError

    profile = _profile_with_sources(tmp_path, {"jira.yaml": body})
    with pytest.raises(SubstrateError) as caught:
        profile_credential_secrets(
            profile,
            assets=[_selected("consumer/bundles/demo/source-defaults/jira.yaml")],
            installed_root=tmp_path,
            consumer_digest=_CONSUMER_DIGEST,
        )
    assert caught.value.data["reason"] == "source_credential_declaration_malformed"


def test_two_files_declaring_one_source_id_refuse_before_any_secret(
    tmp_path: Path,
) -> None:
    """The credential reader runs the scaffold's duplicate-id refusal."""
    from okg.substrate.deployment_bootstrap.profile_init import (
        DuplicateSourceIdError,
    )
    from okg.substrate.distributions.install_execution import (
        profile_credential_secrets,
    )

    profile = _profile_with_sources(
        tmp_path,
        {"jira.yaml": _JIRA.format(required="false"),
         "jira-cloud.yaml": _JIRA.format(required="true")},
    )
    profile.name = "demo"
    profile.path = tmp_path / "profile.yaml"
    with pytest.raises(DuplicateSourceIdError) as caught:
        profile_credential_secrets(
            profile,
            assets=[_selected("consumer/bundles/demo/source-defaults/jira.yaml")],
            installed_root=tmp_path,
            consumer_digest=_CONSUMER_DIGEST,
        )
    assert caught.value.data["source_id"] == "jira"


def test_a_profile_without_source_defaults_passes_no_secrets(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from okg.substrate.distributions.install_execution import (
        profile_credential_secrets,
    )

    assert profile_credential_secrets(
        SimpleNamespace(source_defaults_dir=None),
        assets=[],
        installed_root=tmp_path,
        consumer_digest=_CONSUMER_DIGEST,
    ) == {}


def test_run_install_passes_the_profile_credentials_into_the_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The packaged `okg install` hands the profile's secrets to the planner.

    Everything around the plan call is replaced, so this pins only the
    wiring: before issue 1794 the planner received a name and modules only.
    """
    import argparse
    from types import SimpleNamespace

    import okg.substrate.catalog.profile as profile_module
    import okg.substrate.deployment_product_lifecycle as lifecycle
    from okg.substrate.distributions import install_execution
    from okg.substrate.distributions import install_plan as install_plan_module
    from okg.substrate.distributions import package as package_module
    from okg.substrate.errors import SubstrateError

    profile = _profile_with_sources(tmp_path, {"jira.yaml": _JIRA.format(required="false")})
    bundle_dir = profile.source_defaults_dir.parent
    profile.modules_file = bundle_dir / "modules.yaml"
    members = ("profile.yaml", "modules.yaml", "source-defaults/jira.yaml")
    assets = [_selected(f"consumer/bundles/demo/{name}") for name in members]
    consumer = SimpleNamespace(
        package="consumer", digest=_CONSUMER_DIGEST, installed_root=tmp_path
    )
    monkeypatch.setattr(
        install_execution, "active_bootstrap", lambda: SimpleNamespace(consumer=consumer)
    )
    monkeypatch.setattr(
        package_module,
        "verify_distribution_package",
        lambda path: SimpleNamespace(close_runtime_authority=lambda: None),
    )
    monkeypatch.setattr(
        lifecycle,
        "package_selection_from_verified",
        lambda package, bundle_id: SimpleNamespace(assets=assets),
    )
    monkeypatch.setattr(profile_module, "load_profile", lambda path: profile)
    monkeypatch.setattr(profile_module, "load_profile_modules", lambda loaded: [])
    monkeypatch.setattr(
        install_execution, "_packaged_module_files", lambda package, modules: {}
    )
    captured: list[Any] = []

    def _capture(package_path: Any, bundle_id: str, instance: Any, **_: Any) -> Any:
        captured.append(instance)
        raise SubstrateError("stop after planning")

    monkeypatch.setattr(install_plan_module, "resolve_install_plan", _capture)
    monkeypatch.setenv("OKG_TEST_INSTALL_DSN_1794", "postgresql://unused")
    args = argparse.Namespace(
        search_query=None, search_generation=None, update_source=None,
        dry_run=False, scaffold_only=False, no_publish=False,
        deployment_name="demo", target=str(tmp_path / "target"),
        install_dsn_ref="env:OKG_TEST_INSTALL_DSN_1794", verify_query="phrase",
        personal_private=False, source_policy=None,
        distribution_package="package.okgdist.tar", bundle="demo",
    )
    assert install_execution.run_install(args, as_json=True) != 0
    assert captured == [
        {
            "name": "demo",
            "modules": [],
            "secrets": {
                "jira.credential.0": {
                    "reference": "env:ARCHI_TOKEN",
                    "declaration": _DECLARATION_REF,
                    "required": False,
                    "aliases": ["env:ARCHI_TOKEN_OLD"],
                },
            },
        }
    ]


# ---- issue 1794: end to end through a real packaged source-defaults file ----
#
# The package below carries a consumer "wheel" object (a zip) whose bundle
# members -- profile.yaml, modules.yaml and two source-defaults files -- are
# each their own typed asset, addressed by member path, exactly as the
# example connector's package builder lays them out. The real verifier,
# the real selection, the real profile loader, the real credential reader
# and the real planner run; only the verified-wheel bootstrap, the renderer
# and the database-side execution are replaced.

_E2E_BUNDLE = "lab-alpha"
_E2E_ROOT = f"consumer/bundles/{_E2E_BUNDLE}"
_E2E_JIRA = """\
jira:
  module: consumer.sources.jira
  class: Jira
  credential_refs: [E2E_JIRA_TOKEN]
  credential_aliases:
    E2E_JIRA_TOKEN: [E2E_JIRA_TOKEN_OLD]
  required_for_baseline: {required}
"""


def _e2e_package(tmp_path: Path, *, kind: str, required: str = "false") -> tuple[Path, Path, str]:
    """Build a package whose credentialed source-defaults file has ``kind``.

    Returns the package path, the installed root the consumer's files are
    unpacked into, and the consumer object digest.
    """
    import zipfile

    from okg.substrate.distributions import (
        AssetLocator,
        DistributionObject,
        DistributionPackageManifest,
        DistributionPackageProvenance,
        DistributionPackageSource,
        ModuleDeclaration,
        OkgCompatibility,
        PackagedAsset,
        build_distribution_package,
        canonical_release_digest,
    )
    from okg.substrate.contracts.deployment_product import (
        CompatibilityMetadata,
        DistributionRelease,
    )

    from ..catalog.test_package_module_provider import (
        _REVISION,
        _ZERO_DIGEST,
        _asset,
        _bundle,
        _digest,
        _ref,
        package_impl,
    )

    members = {
        f"{_E2E_ROOT}/profile.yaml": (
            f"name: {_E2E_BUNDLE}\nversion: 1.0.0\nmodules: modules.yaml\n"
            "source_defaults: source-defaults/\n"
        ),
        f"{_E2E_ROOT}/modules.yaml": f"modules:\n  - {_MODULE_ID}\n",
        f"{_E2E_ROOT}/source-defaults/jira.yaml": _E2E_JIRA.format(required=required),
        f"{_E2E_ROOT}/source-defaults/releases.yaml": _NO_CREDENTIAL,
    }
    import base64
    import hashlib

    authoring = tmp_path / "authoring"
    authoring.mkdir(parents=True)
    # A RECORD-complete wheel, which is what the package verifier accepts as
    # a consumer object: WHEEL, METADATA and a RECORD over every member.
    dist_info = "consumer-1.0.0.dist-info"
    payloads = {name: text.encode() for name, text in members.items()}
    payloads[f"{dist_info}/WHEEL"] = (
        b"Wheel-Version: 1.0\nGenerator: hermetic-test\n"
        b"Root-Is-Purelib: true\nTag: py3-none-any\n"
    )
    payloads[f"{dist_info}/METADATA"] = (
        b"Metadata-Version: 2.1\nName: consumer\nVersion: 1.0.0\n"
    )
    record = "".join(
        "{},sha256={},{}\n".format(
            name,
            base64.urlsafe_b64encode(hashlib.sha256(body).digest()).rstrip(b"=").decode(),
            len(body),
        )
        for name, body in payloads.items()
    )
    payloads[f"{dist_info}/RECORD"] = (record + f"{dist_info}/RECORD,,\n").encode()
    wheel = authoring / "consumer-1.0.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, body in payloads.items():
            info = zipfile.ZipInfo(name)
            info.create_system = 3
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, body)
    installed_root = tmp_path / "installed"
    with zipfile.ZipFile(wheel) as archive:
        archive.extractall(installed_root)
    wheel_bytes = wheel.read_bytes()
    wheel_digest = _digest(wheel_bytes)
    schema_file = authoring / "schema.yaml"
    schema_file.write_bytes(_SCHEMA_YAML)

    packaged = []
    schema_asset = _asset("lab-units-schema", _SCHEMA_YAML, kind="ontology_schema")
    packaged.append(
        PackagedAsset(
            asset=schema_asset,
            locator=AssetLocator(object_digest=_digest(_SCHEMA_YAML)),
            uncompressed_size=len(_SCHEMA_YAML),
        )
    )
    with zipfile.ZipFile(wheel) as archive:
        for info in sorted(archive.infolist(), key=lambda item: item.filename):
            if not info.filename.startswith(_E2E_ROOT + "/"):
                continue
            payload = archive.read(info.filename)
            member_kind = kind if info.filename.endswith("/jira.yaml") else "source-template"
            asset_id = "bundle-" + info.filename.rsplit("/", 1)[1].replace(".", "-")
            packaged.append(
                PackagedAsset(
                    asset=_asset(asset_id, payload, kind=member_kind,
                                 plane="ingestion-projection"),
                    locator=AssetLocator(
                        object_digest=wheel_digest,
                        member_path=info.filename,
                        member_crc32=f"{info.CRC:08x}",
                        member_compressed_size=info.compress_size,
                        compression_method="deflated",
                    ),
                    uncompressed_size=len(payload),
                )
            )
    refs = tuple(_ref(item.asset) for item in packaged)
    compatibility = CompatibilityMetadata(
        contract_versions=("v1",), capabilities=("public",)
    )
    release = DistributionRelease(
        distribution_id=_OWNER,
        version="1.0.0",
        origin="external",
        release_digest=_ZERO_DIGEST,
        assets=refs,
        bundle_ids=(_E2E_BUNDLE,),
        compatibility=compatibility,
    )
    provisional = DistributionPackageManifest(
        schema_version=package_impl.SCHEMA_V2,
        distribution_release=release,
        bundles=(_bundle(_E2E_BUNDLE, refs),),
        assets=tuple(packaged),
        objects=tuple(
            sorted(
                (
                    DistributionObject(
                        digest=wheel_digest,
                        size=len(wheel_bytes),
                        media_type="application/zip",
                    ),
                    DistributionObject(
                        digest=_digest(_SCHEMA_YAML),
                        size=len(_SCHEMA_YAML),
                        media_type="application/octet-stream",
                    ),
                ),
                key=lambda item: item.digest,
            )
        ),
        provenance=DistributionPackageProvenance(
            repository_url="https://github.com/example/lab-units",
            revision=_REVISION,
            builder="okg-hermetic-test",
        ),
        okg_compatibility=OkgCompatibility(
            min_version="0.1.0", max_version_exclusive="9.0.0"
        ),
        module_declarations=(
            ModuleDeclaration(module_id=_MODULE_ID, asset_refs=(refs[0],)),
        ),
    )
    manifest = provisional.model_copy(
        update={
            "distribution_release": release.model_copy(
                update={"release_digest": canonical_release_digest(provisional)}
            )
        }
    )
    output = tmp_path / "packages"
    output.mkdir()
    archive_path = build_distribution_package(
        manifest,
        {
            wheel_digest: DistributionPackageSource(root=authoring, path=wheel),
            _digest(_SCHEMA_YAML): DistributionPackageSource(
                root=authoring, path=schema_file
            ),
        },
        output,
    )
    return Path(archive_path), installed_root, wheel_digest


def _e2e_run_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    kind: str = "source",
    required: str = "false",
    environ: dict[str, str],
    as_json: bool = True,
) -> tuple[int, str, list[dict[str, Any]]]:
    import argparse
    from types import SimpleNamespace

    from okg.substrate.distributions import install_execution

    package, installed_root, consumer_digest = _e2e_package(
        tmp_path, kind=kind, required=required
    )
    consumer = SimpleNamespace(
        package="consumer", digest=consumer_digest, installed_root=installed_root
    )
    monkeypatch.setattr(
        install_execution, "active_bootstrap", lambda: SimpleNamespace(consumer=consumer)
    )
    monkeypatch.setattr(install_execution, "render_verified_profile", lambda **_: {})
    executed: list[dict[str, Any]] = []

    def _execute(**kwargs: Any) -> dict[str, Any]:
        executed.append({"plan": kwargs["plan"], "lock": kwargs["lock"]})
        return {"state": "published", "target": "hidden"}

    monkeypatch.setattr(install_execution, "execute_prepared_install", _execute)
    for name in ("E2E_JIRA_TOKEN", "E2E_JIRA_TOKEN_OLD"):
        monkeypatch.delenv(name, raising=False)
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("OKG_TEST_INSTALL_DSN_1794", "postgresql://unused")
    args = argparse.Namespace(
        search_query=None, search_generation=None, update_source=None,
        dry_run=False, scaffold_only=False, no_publish=False,
        deployment_name="demo", target=str(tmp_path / "target"),
        install_dsn_ref="env:OKG_TEST_INSTALL_DSN_1794", verify_query="phrase",
        personal_private=False, source_policy=None,
        distribution_package=str(package), bundle=_E2E_BUNDLE,
        authorize_cluster_bootstrap=False,
    )
    status = install_execution.run_install(args, as_json=as_json)
    captured = capsys.readouterr()
    return status, captured.out + captured.err, executed


@pytest.mark.parametrize("kind", ["source", "source-binding", "source-template"])
def test_packaged_install_defers_an_optional_credential_end_to_end(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    kind: str,
) -> None:
    status, output, executed = _e2e_run_install(
        tmp_path / "unset", monkeypatch, capsys, kind=kind, environ={}
    )
    assert status == 0, output
    payload = json.loads(output)
    payload = payload.get("result", payload)
    (deferred,) = payload["deferred_credentials"]
    assert deferred["source_id"] == "jira"
    assert deferred["name"] == "jira.credential.0"
    assert deferred["check_with"] == _CHECK_WITH
    assert deferred["supply_later"].startswith("export the variable")
    assert "E2E_JIRA_TOKEN" not in output
    (unset_run,) = executed
    (entry,) = unset_run["plan"]["secret_references"]
    assert entry["declaration_ref"] == f"{_OWNER}:bundle-jira-yaml"
    assert "E2E_JIRA_TOKEN" not in json.dumps(unset_run["plan"])

    # Supplying it later (here: through its alias) re-plans to the same
    # lock, so the recorded release identity still matches.
    status, output, executed = _e2e_run_install(
        tmp_path / "set", monkeypatch, capsys, kind=kind,
        environ={"E2E_JIRA_TOKEN_OLD": "never-recorded"},
    )
    assert status == 0, output
    payload = json.loads(output)
    payload = payload.get("result", payload)
    assert payload["deferred_credentials"] == []
    assert "never-recorded" not in output
    (set_run,) = executed
    assert set_run["lock"]["lock_digest"] == unset_run["lock"]["lock_digest"]
    assert set_run["plan"] == unset_run["plan"]


def test_packaged_install_text_output_lists_the_deferred_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    status, output, _ = _e2e_run_install(
        tmp_path, monkeypatch, capsys, environ={}, as_json=False
    )
    assert status == 0, output
    assert "deferred_credentials" in output
    (line,) = [item for item in output.splitlines() if "jira.credential.0" in item]
    assert line.startswith("  - jira.credential.0 (source jira, declaration ")
    assert line.endswith(f"check with {_CHECK_WITH}")
    assert "{" not in line
    assert "E2E_JIRA_TOKEN" not in output


def test_packaged_install_refuses_an_unset_required_credential_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    status, output, executed = _e2e_run_install(
        tmp_path, monkeypatch, capsys, required="true", environ={}
    )
    assert status != 0
    assert "environment_variable_absent" in output
    assert executed == []
    assert not (tmp_path / "target").exists()


def test_packaged_install_refuses_a_credential_file_of_a_non_source_kind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A bundle file of a kind that declares no source carries no credential."""
    status, output, executed = _e2e_run_install(
        tmp_path, monkeypatch, capsys, kind="profile", environ={}
    )
    assert status != 0
    assert "source_credential_declaration_kind_unsupported" in output
    assert "jira.yaml" in output
    assert executed == []
