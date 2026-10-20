"""Provider provenance through sealed release, activation, upgrade (#1179 s6).

Every arm is hermetic: a real package-v2 archive built and verified in a
temporary directory, composed in memory. No database, no network, no
import of packaged code, and nothing here skips on a filesystem probe.

The golden constants below were recorded from a detached `origin/dev`
worktree at 81959cc950, before `module_provenance` existed. They are the
whole claim that a release built from built-ins only is byte-identical to
what it was.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.catalog.module_providers import (
    BUILTIN_OWNER,
    ModuleProvider,
)
from okg.substrate.catalog.module_provenance import (
    ReleaseModuleProvenanceMismatch,
    UpgradeModuleProviderDropped,
    module_provenance_entries,
    module_provenance_from_composed,
    module_provider_changes,
    refuse_dropped_module_owner,
    refuse_release_provenance_mismatch,
)
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.sealed_release import SealedReleaseInputs

from .test_module_providers import (
    _deployment,
    _empty_cross_links,
    _write_module,
)
from .test_package_module_provider import (
    _MODULE_ID,
    _OWNER,
    _SCHEMA_YAML,
    _providers,
    _verified_package,
)


# Recorded from a detached origin/dev worktree at 81959cc950 and then
# removed. An input set with a postgres capacity digest and one without,
# so both branches of `to_dict` are pinned.
_GOLDEN_INPUTS = dict(
    code_revision="a" * 40,
    dependency_lock_digest="sha256:" + "1" * 64,
    schema_checksum="sha256:" + "2" * 64,
    catalog_fingerprint="sha256:" + "3" * 64,
    definition_digest="sha256:" + "4" * 64,
    skill_bundle_digest=None,
    postgres_capacity_digest="sha256:" + "5" * 64,
    deployment_revision="b" * 40,
    deployment_tree_sha="c" * 40,
    deployment_layout="monorepo",
    deployment_repo_path=".",
)
_GOLDEN_INPUT_ID = (
    "sha256:ff7811cc9e6cc6c33f809e0b813485585ac5a3a72f8b709fd6f41443b31f67ed"
)
_GOLDEN_MINIMAL_INPUT_ID = (
    "sha256:f7c32d499fe513e4ce21037ac4caaf56a31768025cf693cba33c0ded8b119ac7"
)


def _composed_with_package(tmp_path: Path, *, schema: bytes = _SCHEMA_YAML):
    """Compose one built-in and one packaged module, returning the catalog."""
    package = _verified_package(tmp_path, schema_payload=schema)
    providers = {
        BUILTIN_OWNER: ModuleProvider(BUILTIN_OWNER, tmp_path / "builtin"),
        **_providers(tmp_path, package),
    }
    _write_module(tmp_path / "builtin", "reasoning", {"Hypothesis": "entity"})
    return compose_catalog(
        _deployment(tmp_path, ["reasoning", _MODULE_ID]),
        providers=providers,
        cross_links_root=_empty_cross_links(tmp_path),
    )


# ---------- (a) the sealed release records provider provenance ----------


def test_sealed_release_records_provenance_for_the_packaged_module_only(
    tmp_path: Path,
) -> None:
    composed = _composed_with_package(tmp_path)
    provenance = module_provenance_from_composed(composed)

    assert [entry["module_id"] for entry in provenance] == [_MODULE_ID]
    entry = provenance[0]
    assert entry["owner"] == _OWNER
    assert entry["provider_kind"] == "package"
    assert entry["package_release_digest"].startswith("sha256:")
    assert entry["object_digests"] and all(
        digest.startswith("sha256:") for digest in entry["object_digests"]
    )
    # The built-in module is pinned by the release's own code_revision, so
    # recording it would add a byte without adding a fact.
    assert "reasoning" not in json.dumps(provenance)


def test_release_digest_covers_the_provenance_record(tmp_path: Path) -> None:
    composed = _composed_with_package(tmp_path)
    sealed = SealedReleaseInputs(
        **_GOLDEN_INPUTS,
        module_provenance=module_provenance_from_composed(composed),
    )

    assert sealed.input_id != _GOLDEN_INPUT_ID
    assert sealed.to_dict()["module_provenance"][0]["module_id"] == _MODULE_ID


def test_a_changed_object_digest_changes_the_release_identity(
    tmp_path: Path,
) -> None:
    """The same module id, different packaged bytes, a different release."""
    first = SealedReleaseInputs(
        **_GOLDEN_INPUTS,
        module_provenance=module_provenance_from_composed(
            _composed_with_package(tmp_path / "one"),
        ),
    )
    second = SealedReleaseInputs(
        **_GOLDEN_INPUTS,
        module_provenance=module_provenance_from_composed(
            _composed_with_package(
                tmp_path / "two",
                schema=_SCHEMA_YAML + b"\n# a later revision of the module\n",
            ),
        ),
    )

    first_entry = first.module_provenance[0]
    second_entry = second.module_provenance[0]
    assert first_entry["module_id"] == second_entry["module_id"]
    assert first_entry["object_digests"] != second_entry["object_digests"]
    assert first.input_id != second.input_id


# ---------- the golden: built-ins only is byte-identical ----------


def test_a_builtins_only_release_is_byte_identical_to_the_pre_change_golden(
    tmp_path: Path,
) -> None:
    _write_module(tmp_path / "builtin", "reasoning", {"Hypothesis": "entity"})
    composed = compose_catalog(
        _deployment(tmp_path, ["reasoning"]),
        providers={
            BUILTIN_OWNER: ModuleProvider(BUILTIN_OWNER, tmp_path / "builtin"),
        },
        cross_links_root=_empty_cross_links(tmp_path),
    )

    assert module_provenance_from_composed(composed) == ()
    sealed = SealedReleaseInputs(**_GOLDEN_INPUTS)
    assert "module_provenance" not in sealed.to_dict()
    assert sealed.input_id == _GOLDEN_INPUT_ID

    minimal = SealedReleaseInputs(
        code_revision="a" * 40,
        dependency_lock_digest="sha256:" + "1" * 64,
        schema_checksum="sha256:" + "2" * 64,
        catalog_fingerprint="sha256:" + "3" * 64,
        definition_digest="sha256:" + "4" * 64,
        skill_bundle_digest="sha256:" + "6" * 64,
    )
    assert "module_provenance" not in minimal.to_dict()
    assert minimal.input_id == _GOLDEN_MINIMAL_INPUT_ID


# ---------- (b) activation refuses a provenance that no longer resolves ----


def test_activation_refuses_a_changed_package_release_digest(
    tmp_path: Path,
) -> None:
    recorded = module_provenance_from_composed(
        _composed_with_package(tmp_path / "sealed"),
    )
    resolved = module_provenance_from_composed(
        _composed_with_package(
            tmp_path / "installed",
            schema=_SCHEMA_YAML + b"\n# a later revision\n",
        ),
    )

    with pytest.raises(ReleaseModuleProvenanceMismatch) as excinfo:
        refuse_release_provenance_mismatch(
            recorded, resolved, command="okg environment activate-release",
        )

    data = excinfo.value.data
    assert excinfo.value.code == "release_module_provenance_mismatch"
    assert data["module_id"] == _MODULE_ID
    assert data["reason"] == "provider_digest_changed"
    assert data["recorded_release_digest"] == recorded[0][
        "package_release_digest"
    ]
    assert data["resolved_release_digest"] == resolved[0][
        "package_release_digest"
    ]
    assert data["recorded_object_digests"] != data["resolved_object_digests"]
    assert data["phase"] == "pre_activation"


def test_activation_refuses_a_module_the_providers_no_longer_supply(
    tmp_path: Path,
) -> None:
    recorded = module_provenance_from_composed(
        _composed_with_package(tmp_path / "sealed"),
    )

    with pytest.raises(ReleaseModuleProvenanceMismatch) as excinfo:
        refuse_release_provenance_mismatch(
            recorded, (), command="okg environment activate-release",
        )

    assert excinfo.value.data["reason"] == "module_no_longer_supplied"
    assert excinfo.value.data["resolved_release_digest"] is None


def test_activation_accepts_provenance_that_still_resolves(
    tmp_path: Path,
) -> None:
    recorded = module_provenance_from_composed(
        _composed_with_package(tmp_path / "sealed"),
    )
    # A second, independent build of the same declarations and bytes.
    resolved = module_provenance_from_composed(
        _composed_with_package(tmp_path / "installed"),
    )

    refuse_release_provenance_mismatch(
        recorded, resolved, command="okg environment activate-release",
    )
    # And a release read back from JSON compares equal to what it recorded.
    refuse_release_provenance_mismatch(
        module_provenance_entries(json.loads(json.dumps(list(recorded)))),
        resolved,
        command="okg environment activate-release",
    )


def test_a_builtins_only_release_never_reaches_the_provenance_check() -> None:
    refuse_release_provenance_mismatch(
        (), (), command="okg environment activate-release",
    )


def test_a_release_predating_provenance_is_accepted_only_against_builtins(
    tmp_path: Path,
) -> None:
    """The rule for a release sealed before this field existed.

    Such a release records nothing, which `module_provenance_entries`
    reads back as an empty tuple. Against built-ins it activates
    unchanged; against providers that now supply a packaged module it
    refuses `module_not_in_release` rather than adopting what it never
    recorded.
    """
    legacy = module_provenance_entries(
        json.loads(json.dumps({"catalog_fingerprint": "sha256:0"})).get(
            "module_provenance"
        ),
    )
    assert legacy == ()

    refuse_release_provenance_mismatch(
        legacy, (), command="okg environment activate-release",
    )

    resolved = module_provenance_from_composed(
        _composed_with_package(tmp_path / "installed"),
    )
    with pytest.raises(ReleaseModuleProvenanceMismatch) as excinfo:
        refuse_release_provenance_mismatch(
            legacy, resolved, command="okg environment activate-release",
        )

    data = excinfo.value.data
    assert data["reason"] == "module_not_in_release"
    assert data["module_id"] == _MODULE_ID
    assert data["recorded_release_digest"] is None
    assert data["recorded_object_digests"] is None
    assert data["resolved_release_digest"] == resolved[0][
        "package_release_digest"
    ]
    assert data["phase"] == "pre_activation"


# ---------- (c) the upgrade report and its refusal ----------


def _entry(module_id: str, digest: str, objects: tuple[str, ...] = ()) -> dict:
    owner = module_id.split(":", 1)[0]
    return {
        "module_id": module_id,
        "owner": owner,
        "provider_kind": "package",
        "package_release_digest": digest,
        "object_digests": list(objects),
    }


def test_upgrade_report_lists_added_removed_and_digest_changed() -> None:
    previous = (
        _entry("archi:core", "sha256:aa", ("sha256:11",)),
        _entry("archi:extra", "sha256:aa"),
        _entry("cms:tracks", "sha256:cc"),
    )
    target = (
        _entry("archi:core", "sha256:bb", ("sha256:22",)),
        _entry("archi:new", "sha256:bb"),
        _entry("cms:tracks", "sha256:cc"),
    )

    changes = module_provider_changes(previous, target)

    assert changes["added"] == ["archi:new"]
    assert changes["removed"] == ["archi:extra"]
    assert [item["module_id"] for item in changes["digest_changed"]] == [
        "archi:core"
    ]
    assert changes["digest_changed"][0]["previous_release_digest"] == "sha256:aa"
    assert changes["digest_changed"][0]["target_release_digest"] == "sha256:bb"
    assert changes["digest_changed"][0]["target_object_digests"] == ["sha256:22"]


def test_upgrade_refuses_to_drop_a_packaged_owner_silently() -> None:
    previous = (
        _entry("archi:core", "sha256:aa"),
        _entry("cms:tracks", "sha256:cc"),
    )
    target = (_entry("cms:tracks", "sha256:cc"),)

    with pytest.raises(UpgradeModuleProviderDropped) as excinfo:
        refuse_dropped_module_owner(previous, target)

    data = excinfo.value.data
    assert excinfo.value.code == "upgrade_module_provider_owner_dropped"
    assert data["dropped_owners"] == ["archi"]
    assert data["dropped_modules"] == ["archi:core"]
    assert data["changes"]["removed"] == ["archi:core"]
    assert data["phase"] == "pre_upgrade"


def test_upgrade_allows_a_declared_module_swap_inside_one_owner() -> None:
    """Dropping a module is an operator's choice; dropping its owner is not."""
    previous = (_entry("archi:core", "sha256:aa"), _entry("archi:old", "sha256:aa"))
    target = (_entry("archi:core", "sha256:aa"), _entry("archi:new", "sha256:bb"))

    changes = refuse_dropped_module_owner(previous, target)

    assert changes["added"] == ["archi:new"]
    assert changes["removed"] == ["archi:old"]
    assert changes["digest_changed"] == []


def test_upgrade_from_and_to_builtins_only_reports_nothing() -> None:
    assert refuse_dropped_module_owner((), ()) == {
        "schema": "okg.release.module_provenance/v1",
        "added": [],
        "removed": [],
        "digest_changed": [],
    }


def test_malformed_recorded_provenance_refuses_rather_than_defaulting() -> None:
    with pytest.raises(ReleaseModuleProvenanceMismatch):
        module_provenance_entries("archi:core")
    with pytest.raises(ReleaseModuleProvenanceMismatch):
        module_provenance_entries([{"owner": "archi"}])
    assert module_provenance_entries(None) == ()


# ---------- (d) the two follow-ups #1774's review left for slice 7 ----------


class _Composed:
    """A composed catalog whose module origins are supplied verbatim."""

    def __init__(self, origins: dict) -> None:
        self.module_origins = origins


def test_selection_is_by_owner_so_a_kindless_origin_refuses() -> None:
    """Filtering on `provider_kind != builtin` alone sealed an empty kind.

    An origin carrying no kind read as non-built-in and was recorded with
    `provider_kind: ""`, which activation would then compare against a real
    kind and refuse. The owner decides now, and a kindless origin is a
    malformed composition that refuses before it can be sealed.
    """
    with pytest.raises(ReleaseModuleProvenanceMismatch) as excinfo:
        module_provenance_from_composed(
            _Composed({"archi:core": {"owner": "archi"}})
        )

    assert "archi:core" in excinfo.value.data["observed"]


def test_an_owner_and_kind_that_disagree_refuse_in_both_directions() -> None:
    with pytest.raises(ReleaseModuleProvenanceMismatch):
        module_provenance_from_composed(
            _Composed({"reasoning": {"owner": BUILTIN_OWNER, "provider_kind": "package"}})
        )
    with pytest.raises(ReleaseModuleProvenanceMismatch):
        module_provenance_from_composed(
            _Composed({"archi:core": {"owner": "archi", "provider_kind": "builtin"}})
        )


def test_a_builtin_owned_module_is_still_excluded_by_owner() -> None:
    assert module_provenance_from_composed(
        _Composed(
            {
                "reasoning": {"owner": BUILTIN_OWNER, "provider_kind": "builtin"},
                "archi:core": {
                    "owner": "archi",
                    "provider_kind": "package",
                    "package_release_digest": "sha256:aa",
                },
            }
        )
    ) == (_entry("archi:core", "sha256:aa"),)


def test_dropping_the_modules_from_the_manifest_does_not_clear_the_refusal() -> None:
    """The old fix hint named a route that cannot work.

    It advised removing the owner's modules from the deployment manifest
    "so the drop is declared". But the refusal compares the target against
    what the previous release recorded, and that record still names the
    owner, so the next upgrade refuses again. The hint now says the one
    thing that does clear it.
    """
    previous = (_entry("archi:core", "sha256:aa"),)

    with pytest.raises(UpgradeModuleProviderDropped) as excinfo:
        refuse_dropped_module_owner(previous, ())

    hint = excinfo.value.fix_hint
    assert "does not" in hint and "clear this refusal" in hint
    assert "install the owner's distribution" in hint
