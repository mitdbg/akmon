"""Upgrade, rollback and drift receipts over planned installs (#1179 s7).

Every arm builds two real package-v2 archives -- same owner, different
object bytes, so their release digests differ -- plans both, and asserts
on the documents produced. No database, no network, no import of packaged
code, and no ambient `$OKG_*`: the OKG version and the environment that
secret references resolve against are both injected, so the digests are
stable and the receipts are byte-reproducible.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.catalog.module_provenance import UpgradeModuleProviderDropped
from okg.substrate.contracts.deployment_product.conformance import (
    ConformanceLegacyAuthorityEvidence,
)
from okg.substrate.contracts.deployment_product.lock import (
    canonical_json_bytes,
    sha256_digest,
)
from okg.substrate.distributions.install_plan import (
    InstallOutputPathExists,
    resolve_install_plan,
)
from okg.substrate.distributions.lifecycle_receipts import (
    LEGACY_INPUT_KINDS,
    canonical_legacy_inventory_entries,
    legacy_input_inventory_declaration,
    legacy_input_inventory_digest,
    DRIFT_RECEIPT_SCHEMA,
    ROLLBACK_RECEIPT_SCHEMA,
    UPGRADE_RECEIPT_SCHEMA,
    LegacyInventoryNotSupplied,
    LifecycleReceiptError,
    ReleaseSide,
    RollbackBaselinePackageMissing,
    RollbackCandidateNotDistinct,
    UpgradeOkgRangeBroken,
    legacy_authority_evidence,
    recovery_evidence_from_receipts,
    resolve_drift_receipt,
    resolve_rollback_receipt,
    resolve_upgrade_receipt,
    write_lifecycle_receipt,
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
# No secrets: these arms are the upgrade/rollback plan pair, and a
# deferred credential now needs a selected source or tool declaration,
# which this minimal package does not carry.
_INSTANCE: dict[str, Any] = {"name": "demo"}
# Same classes, different bytes: the object digest and so the release
# digest move while the composed catalog stays identical, which is what a
# real point upgrade of one packaged module looks like.
_SCHEMA_V2 = _SCHEMA_YAML + b"\n# revision two\n"
_GENERATION = "gen-0001"
_BASELINE_RELEASE = "sha256:" + "a" * 64
_CANDIDATE_RELEASE = "sha256:" + "b" * 64


def _builtin(tmp_path: Path) -> Path:
    root = tmp_path / "builtin"
    _write_module(root, "reasoning", {"Hypothesis": "entity"})
    return root


def _side(
    tmp_path: Path,
    name: str,
    *,
    schema: bytes = _SCHEMA_YAML,
    release: str = _BASELINE_RELEASE,
    declarations: Any = None,
) -> ReleaseSide:
    """Build a package, plan it, and wrap the result as one release side."""
    kwargs: dict[str, Any] = {"schema_payload": schema}
    if declarations is not None:
        kwargs["declarations"] = declarations
    package = _verified_package(tmp_path / name, **kwargs)
    package_path = Path(package.path)
    package.close_runtime_authority()
    plan, lock = resolve_install_plan(
        package_path,
        "lab-alpha",
        _INSTANCE,
        okg_version=_OKG_VERSION,
        environ=_SECRET_ENV,
        builtin_modules_root=_builtin(tmp_path),
        cross_links_root=_empty_cross_links(tmp_path),
    )
    return ReleaseSide(plan, lock, release)


@pytest.fixture()
def baseline(tmp_path: Path) -> ReleaseSide:
    return _side(tmp_path, "v1")


@pytest.fixture()
def candidate(tmp_path: Path) -> ReleaseSide:
    return _side(tmp_path, "v2", schema=_SCHEMA_V2, release=_CANDIDATE_RELEASE)


def _package_file(tmp_path: Path, name: str) -> Path:
    return next((tmp_path / name / "packages").glob("*.okgdist.tar"))


# ---------------- (a) the upgrade receipt ----------------


def test_the_upgrade_receipt_lists_the_module_provider_changes(
    tmp_path: Path,
    baseline: ReleaseSide,
    candidate: ReleaseSide,
) -> None:
    """Both sides' inputs, and the per-module digest change between them."""
    receipt = resolve_upgrade_receipt(baseline, candidate)

    assert receipt["schema"] == UPGRADE_RECEIPT_SCHEMA
    # The two packages really are different releases of one owner.
    assert (
        receipt["baseline"]["package_release_digest"]
        != receipt["candidate"]["package_release_digest"]
    )
    assert receipt["baseline"]["sealed_release_artifact_id"] == _BASELINE_RELEASE
    assert receipt["candidate"]["sealed_release_artifact_id"] == _CANDIDATE_RELEASE
    assert receipt["baseline"]["lock_digest"] == baseline.lock["lock_digest"]
    assert receipt["candidate"]["plan_digest"] == candidate.plan["plan_digest"]
    changes = receipt["module_provider_changes"]
    assert changes["added"] == [] and changes["removed"] == []
    changed = changes["digest_changed"]
    assert [item["module_id"] for item in changed] == [_MODULE_ID]
    assert changed[0]["previous_object_digests"] != changed[0]["target_object_digests"]
    assert receipt["okg"] == {
        "version": _OKG_VERSION,
        "compatibility": {"min_version": "0.1.0", "max_version_exclusive": "9.0.0"},
        "satisfied": True,
    }
    assert receipt["receipt_digest"].startswith("sha256:")


def test_the_upgrade_refuses_a_candidate_that_drops_an_owner(
    tmp_path: Path,
    baseline: ReleaseSide,
    candidate: ReleaseSide,
) -> None:
    """Nothing in the candidate says who takes the owner's subtypes over."""
    dropped = ReleaseSide(
        {**candidate.plan, "modules": []},
        candidate.lock,
        _CANDIDATE_RELEASE,
    )
    with pytest.raises(UpgradeModuleProviderDropped) as excinfo:
        resolve_upgrade_receipt(baseline, dropped)

    assert excinfo.value.data["dropped_owners"] == [_OWNER]
    assert excinfo.value.data["phase"] == "pre_upgrade"


def test_the_upgrade_refuses_a_candidate_outside_this_okg_build(
    tmp_path: Path,
    baseline: ReleaseSide,
    candidate: ReleaseSide,
) -> None:
    """The window is checked against the version the deployment runs."""
    with pytest.raises(UpgradeOkgRangeBroken) as excinfo:
        resolve_upgrade_receipt(baseline, candidate, okg_version="9.5.0")

    assert excinfo.value.data["reason"] == "outside_declared_window"
    assert excinfo.value.data["okg_version"] == "9.5.0"


# ---------------- (b) the rollback receipt ----------------


def test_the_rollback_receipt_records_both_digests_and_the_baseline_package(
    tmp_path: Path,
    baseline: ReleaseSide,
    candidate: ReleaseSide,
) -> None:
    receipt = resolve_rollback_receipt(
        baseline,
        candidate,
        baseline_package_path=_package_file(tmp_path, "v1"),
    )

    assert receipt["schema"] == ROLLBACK_RECEIPT_SCHEMA
    assert receipt["activation_reverted_to"] == "baseline"
    assert receipt["baseline_package_present"] is True
    assert receipt["baseline"]["sealed_release_artifact_id"] == _BASELINE_RELEASE
    assert receipt["candidate"]["sealed_release_artifact_id"] == _CANDIDATE_RELEASE
    assert (
        receipt["baseline_package_filename"] == (baseline.plan["package"]["filename"])
    )
    # A receipt names the package, never where it happened to sit.
    assert str(tmp_path) not in str(receipt)


def test_the_rollback_refuses_when_the_baseline_package_is_gone(
    tmp_path: Path,
    baseline: ReleaseSide,
    candidate: ReleaseSide,
) -> None:
    """A revert to bytes nobody can re-verify is refused, not recorded."""
    package = _package_file(tmp_path, "v1")
    package.unlink()

    with pytest.raises(RollbackBaselinePackageMissing) as excinfo:
        resolve_rollback_receipt(
            baseline,
            candidate,
            baseline_package_path=package,
        )

    assert excinfo.value.data["reason"] == "package_absent"
    assert excinfo.value.data["present"] is False
    assert excinfo.value.data["expected_filename"] == package.name


@pytest.mark.parametrize("mutation", ["corrupt", "empty", "truncated", "substitute"])
def test_rollback_refuses_unverified_bytes_at_the_expected_filename(
    tmp_path: Path,
    baseline: ReleaseSide,
    candidate: ReleaseSide,
    mutation: str,
) -> None:
    package = _package_file(tmp_path, "v1")
    original = package.read_bytes()
    replacement = {
        "corrupt": b"corrupt transport bytes",
        "empty": b"",
        "truncated": original[: len(original) // 2],
        "substitute": _package_file(tmp_path, "v2").read_bytes(),
    }[mutation]
    package.write_bytes(replacement)
    output = tmp_path / "rollback-output"
    with pytest.raises(RollbackBaselinePackageMissing) as caught:
        receipt = resolve_rollback_receipt(
            baseline, candidate, baseline_package_path=package
        )
        write_lifecycle_receipt(receipt, output, "rollback")
    assert caught.value.data == {
        "reason": "package_verification_failed",
        "package_error_code": "transport_digest_mismatch",
    }
    assert not output.exists()


@pytest.mark.parametrize("mismatch", [False, True], ids=["success", "plan-mismatch"])
def test_rollback_closes_verified_authority_on_success_and_plan_mismatch(
    tmp_path: Path,
    baseline: ReleaseSide,
    candidate: ReleaseSide,
    monkeypatch: pytest.MonkeyPatch,
    mismatch: bool,
) -> None:
    from okg.substrate.distributions import lifecycle_receipts as lifecycle
    from okg.substrate.distributions.package import DistributionPackageError

    actual_verifier = lifecycle.verify_distribution_package
    verified_packages = []

    def observe_verification(path):
        verified = actual_verifier(path)
        verified_packages.append(verified)
        return verified

    monkeypatch.setattr(lifecycle, "verify_distribution_package", observe_verification)
    if mismatch:
        plan = deepcopy(baseline.plan)
        plan["package"]["transport_digest"] = "sha256:" + "0" * 64
        baseline = ReleaseSide(plan, baseline.lock, baseline.sealed_release_artifact_id)
        with pytest.raises(RollbackBaselinePackageMissing) as caught:
            resolve_rollback_receipt(
                baseline,
                candidate,
                baseline_package_path=_package_file(tmp_path, "v1"),
            )
        assert caught.value.data == {"reason": "package_transport_digest_mismatch"}
    else:
        receipt = resolve_rollback_receipt(
            baseline,
            candidate,
            baseline_package_path=_package_file(tmp_path, "v1"),
        )
        assert receipt["baseline_package_present"] is True
    assert len(verified_packages) == 1
    with pytest.raises(DistributionPackageError) as closed:
        verified_packages[0].verification_receipt()
    assert closed.value.code == "package_verification_receipt_unavailable"


@pytest.mark.parametrize("same_release", [False, True])
def test_rollback_keeps_name_and_release_refusal_priority(
    tmp_path: Path,
    baseline: ReleaseSide,
    candidate: ReleaseSide,
    monkeypatch: pytest.MonkeyPatch,
    same_release: bool,
) -> None:
    from okg.substrate.distributions import lifecycle_receipts as lifecycle

    def unexpected_verification(_path):
        pytest.fail("archive verification must follow the existing identity refusals")

    monkeypatch.setattr(
        lifecycle, "verify_distribution_package", unexpected_verification
    )
    renamed = _package_file(tmp_path, "v1").rename(tmp_path / "wrong-name.okgdist.tar")
    if same_release:
        with pytest.raises(RollbackCandidateNotDistinct) as caught:
            resolve_rollback_receipt(baseline, baseline, baseline_package_path=renamed)
        assert caught.value.data["reason"] == "candidate_equals_baseline"
    else:
        with pytest.raises(RollbackBaselinePackageMissing) as caught:
            resolve_rollback_receipt(baseline, candidate, baseline_package_path=renamed)
        assert caught.value.data["reason"] == "filename_mismatch"


def test_the_rollback_refuses_a_candidate_that_is_the_baseline(
    tmp_path: Path,
    baseline: ReleaseSide,
) -> None:
    with pytest.raises(RollbackCandidateNotDistinct) as excinfo:
        resolve_rollback_receipt(
            baseline,
            ReleaseSide(baseline.plan, baseline.lock, _BASELINE_RELEASE),
            baseline_package_path=_package_file(tmp_path, "v1"),
        )

    assert excinfo.value.data["reason"] == "candidate_equals_baseline"


# ---------------- (c) the drift receipt ----------------


def test_a_clean_re_plan_reports_match(baseline: ReleaseSide) -> None:
    receipt = resolve_drift_receipt(baseline.lock, baseline.lock)

    assert receipt["schema"] == DRIFT_RECEIPT_SCHEMA
    assert receipt["status"] == "match"
    assert receipt["drift_code"] is None
    assert [item["status"] for item in receipt["modules"]] == ["match"]


def test_a_re_plan_against_newer_bytes_reports_drift_per_module(
    baseline: ReleaseSide,
    candidate: ReleaseSide,
) -> None:
    receipt = resolve_drift_receipt(baseline.lock, candidate.lock)

    assert receipt["status"] == "drift"
    assert receipt["drift_code"] == "install_lock_drift"
    assert receipt["recorded_lock_digest"] == baseline.lock["lock_digest"]
    assert receipt["resolved_lock_digest"] == candidate.lock["lock_digest"]
    (module,) = receipt["modules"]
    assert module["module_id"] == _MODULE_ID
    assert module["status"] == "digest_changed"
    assert module["recorded_object_digests"] != module["resolved_object_digests"]


# ---------------- writing: one file, never an overwrite ----------------


def test_each_receipt_is_written_once_and_is_byte_identical_on_rerun(
    tmp_path: Path,
    baseline: ReleaseSide,
    candidate: ReleaseSide,
) -> None:
    """A receipt is a function of its inputs, so a rerun re-derives it."""
    receipt = resolve_upgrade_receipt(baseline, candidate)
    first = write_lifecycle_receipt(receipt, tmp_path / "out", "upgrade")
    written = Path(first["receipt_path"]).read_bytes()

    again = resolve_upgrade_receipt(baseline, candidate)
    second = write_lifecycle_receipt(again, tmp_path / "out-2", "upgrade")

    assert Path(second["receipt_path"]).read_bytes() == written
    assert again == receipt
    assert sorted(
        item.name for item in Path(first["receipt_path"]).parent.iterdir()
    ) == ["upgrade-receipt.json"]


def test_writing_refuses_a_path_that_already_exists(
    tmp_path: Path,
    baseline: ReleaseSide,
    candidate: ReleaseSide,
) -> None:
    output = tmp_path / "out"
    output.mkdir()
    receipt = resolve_upgrade_receipt(baseline, candidate)

    with pytest.raises(InstallOutputPathExists):
        write_lifecycle_receipt(receipt, output, "upgrade")


# ---------------- (d) the #1185 evidence blocks ----------------


@pytest.mark.parametrize(
    "input_kind", ["matching", "contradictory", "caller_claims", "absent"]
)
def test_recovery_evidence_refuses_unmeasured_planning_inputs(
    tmp_path: Path,
    baseline: ReleaseSide,
    candidate: ReleaseSide,
    input_kind: str,
) -> None:
    """Even consistent plans or caller claims cannot establish executed recovery."""
    upgrade = resolve_upgrade_receipt(baseline, candidate)
    rollback = resolve_rollback_receipt(
        baseline,
        candidate,
        baseline_package_path=_package_file(tmp_path, "v1"),
    )
    if input_kind == "contradictory":
        rollback["candidate"] = dict(
            rollback["candidate"], lock_digest="sha256:" + "0" * 64
        )
    elif input_kind == "caller_claims":
        rollback.update(
            {
                "interruption_recovered": True,
                "rollback_verified": True,
                "digest_fences_verified": True,
                "restored_generation_id": _GENERATION,
                "restored_sealed_release_artifact_id": _BASELINE_RELEASE,
            }
        )
    elif input_kind == "absent":
        upgrade, rollback = {}, {}
    original = deepcopy((upgrade, rollback))
    input_marker = "caller-only-generation-do-not-echo"

    with pytest.raises(LifecycleReceiptError) as excinfo:
        recovery_evidence_from_receipts(
            upgrade,
            rollback,
            last_known_good_generation_id=input_marker,
        )

    error = excinfo.value
    assert error.code == "lifecycle_receipt_refused"
    assert error.data == {"reason": "recovery_evidence_from_plans_unsupported"}
    assert input_marker not in str(error.to_json())
    assert _BASELINE_RELEASE not in str(error.to_json())
    assert "planning" in str(error).lower()
    assert (upgrade, rollback) == original


@pytest.mark.parametrize("inventory", [None, []])
def test_legacy_authority_refuses_to_invent_an_inventory(inventory: Any) -> None:
    """A hermetic run has no legacy deployment, so it cannot claim the arm."""
    with pytest.raises(LegacyInventoryNotSupplied) as excinfo:
        legacy_authority_evidence(inventory)

    assert excinfo.value.data["reason"] == "inventory_absent"


def _entry(input_id: str, kind: str, disposition: str) -> dict[str, str]:
    return {
        "input_id": input_id,
        "kind": kind,
        "disposition": disposition,
        "evidence_ref": f"audit/{input_id.split(':', 1)[1]}.json",
    }


# Already in canonical order (sorted by input_id).
_INVENTORY = [
    _entry("legacy:install-invocation", "legacy_install_invocation", "migrated"),
    _entry("legacy:profiles-dir", "authoring_checkout_path", "migrated"),
    _entry("legacy:pruned-bridge-schema", "copied_schema_authority", "refused"),
]


def test_a_real_inventory_fills_legacy_authority_evidence() -> None:
    audit = legacy_authority_evidence(list(reversed(_INVENTORY)))
    validated = ConformanceLegacyAuthorityEvidence.model_validate(audit)

    assert validated.declared_inputs == 3
    assert validated.migrated_inputs == 2
    assert validated.refused_inputs == 1
    assert validated.unresolved_inputs == 0
    assert validated.fallback_authority_absent is True


def test_an_unresolved_legacy_input_is_refused_rather_than_counted() -> None:
    with pytest.raises(LifecycleReceiptError) as excinfo:
        legacy_authority_evidence(
            [_entry("legacy:profiles-dir", "authoring_checkout_path", "unresolved")]
        )

    assert excinfo.value.data["reason"] == "inventory_entry_invalid"


# ---------- issue 1794: the inventory is one readable, digest-bound document ----------


def test_the_denominator_is_every_unique_entry_and_the_kinds_are_closed() -> None:
    assert LEGACY_INPUT_KINDS == {
        "authoring_checkout_path",
        "legacy_install_invocation",
        "copied_schema_authority",
    }
    with pytest.raises(LifecycleReceiptError) as unknown_kind:
        legacy_authority_evidence(
            [_entry("legacy:x", "legacy_profile_data", "migrated")]
        )
    assert unknown_kind.value.data["reason"] == "inventory_kind_unknown"
    for broken in (
        {**_INVENTORY[0], "evidence_ref": ""},
        {**_INVENTORY[0], "evidence_ref": "two words"},
        {**_INVENTORY[0], "input_id": "Legacy:Profiles"},
        {key: value for key, value in _INVENTORY[0].items() if key != "kind"},
        {**_INVENTORY[0], "extra": "field"},
    ):
        with pytest.raises(LifecycleReceiptError) as invalid:
            legacy_authority_evidence([broken])
        assert invalid.value.data["reason"] == "inventory_entry_invalid"


def test_request_declaration_and_evidence_share_one_reproducible_digest() -> None:
    entries = list(reversed(_INVENTORY))
    declaration = legacy_input_inventory_declaration("legacy-inventory-a", entries)
    audit = legacy_authority_evidence(entries)
    canonical = canonical_legacy_inventory_entries(entries)

    assert canonical == _INVENTORY
    recomputed = "sha256:" + hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert declaration["inventory_digest"] == recomputed
    assert audit["declared_inventory_digest"] == recomputed
    assert legacy_input_inventory_digest(entries) == recomputed
    assert declaration == {
        "inventory_id": "legacy-inventory-a",
        "inventory_digest": recomputed,
        "media_type": "application/json",
        "entry_count": 3,
        "empty": False,
        "complete": True,
    }
    # The four fields are all in the preimage: changing any one moves the digest.
    for field, value in (
        ("kind", "copied_schema_authority"),
        ("disposition", "refused"),
        ("evidence_ref", "audit/other.json"),
    ):
        changed = [dict(_INVENTORY[0], **{field: value}), *_INVENTORY[1:]]
        assert legacy_input_inventory_digest(changed) != recomputed

    empty = legacy_input_inventory_declaration("legacy-inventory-empty", [])
    assert (empty["entry_count"], empty["empty"]) == (0, True)
    with pytest.raises(LifecycleReceiptError) as bad_id:
        legacy_input_inventory_declaration("Not An Id", entries)
    assert bad_id.value.data["reason"] == "inventory_id_missing"


def test_the_plan_asset_digest_and_the_legacy_digest_are_different_values(
    baseline: ReleaseSide,
) -> None:
    """Two fields named ``declared_inventory_digest`` count different things.

    ``install_plan.py`` digests the *selected package assets* of a planned
    install; ``lifecycle_receipts.py`` digests the *legacy inputs* a consumer
    is migrating away from.  Nothing joins them, and reading one as the other
    would let a consumer claim a migration audit from an install plan.  This
    pins that they are computed by different functions over different inputs,
    produce different values for a real planned deployment, and that neither
    function accepts the other's input.
    """

    assets = baseline.plan["assets"]
    assert assets, "the fixture deployment must select at least one asset"
    plan_digest = baseline.plan["declared_inventory_digest"]
    # install_plan.py: sha256 over the selected assets.
    assert plan_digest == sha256_digest(canonical_json_bytes(assets))
    assert baseline.plan["declared_inventory_entry_count"] == len(assets)

    # lifecycle_receipts.py: sha256 over the canonical legacy entries.
    legacy = legacy_authority_evidence(_INVENTORY)
    legacy_digest = legacy["declared_inventory_digest"]
    assert legacy_digest == legacy_input_inventory_digest(_INVENTORY)
    assert legacy["declared_inventory_entry_count"] == len(_INVENTORY)

    assert plan_digest != legacy_digest

    # Neither input is accepted by the other's function: the legacy digest
    # refuses package assets rather than digesting them into a false audit.
    with pytest.raises(LifecycleReceiptError) as excinfo:
        legacy_input_inventory_digest(assets)
    assert excinfo.value.data["reason"] == "inventory_entry_invalid"
    assert sha256_digest(canonical_json_bytes(_INVENTORY)) != plan_digest


def test_the_readable_example_inventory_reproduces_its_declared_digest() -> None:
    readable = json.loads(
        (
            Path(__file__).parents[2]
            / "fixtures"
            / "deployment_products"
            / "external_conformance_interface"
            / "legacy_input_inventory_archi_cern_team_example.json"
        ).read_bytes()
    )
    request = json.loads(
        (
            Path(__file__).parents[2]
            / "fixtures"
            / "deployment_products"
            / "external_conformance_interface"
            / "request_archi_cern_team_lifecycle_supplied.json"
        ).read_bytes()
    )
    declaration = legacy_input_inventory_declaration(
        readable["inventory_id"], readable["entries"]
    )
    assert request["inputs"]["legacy_input_inventory"] == declaration
    assert {entry["kind"] for entry in readable["entries"]} == LEGACY_INPUT_KINDS


# ---------- review pass: absence must not read as agreement ----------


def test_a_drift_receipt_refuses_two_locks_that_carry_no_digest() -> None:
    """Both sides missing a digest would compare equal and read as clean."""
    with pytest.raises(LifecycleReceiptError) as excinfo:
        resolve_drift_receipt({"modules": {}}, {"modules": {}})

    assert excinfo.value.data["reason"] == "lock_digest_absent"


def test_a_duplicated_legacy_input_is_refused_rather_than_double_counted() -> None:
    """Counting one input twice balances the model while covering less."""
    with pytest.raises(LifecycleReceiptError) as excinfo:
        legacy_authority_evidence(
            [
                _entry("legacy:source-a", "authoring_checkout_path", "migrated"),
                _entry("legacy:source-a", "authoring_checkout_path", "refused"),
            ]
        )

    assert excinfo.value.data["reason"] == "inventory_duplicate_input"
