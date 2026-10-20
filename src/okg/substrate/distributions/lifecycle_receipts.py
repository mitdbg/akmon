"""Upgrade, rollback and drift receipts for an installed distribution.

Slice 7 of #1179. Slice 5 produced the install plan and lock; slice 6
sealed the module-provider provenance into a release and made activation
re-resolve it. What was still missing is the *record of a lifecycle
event*: an operator could upgrade, roll back, or re-plan, and nothing
inert was written down that a reviewer could read afterwards.

This module writes those three records and nothing else.

**Nothing here runs.** No database is opened, no release is sealed, no
activation is changed, and no packaged code is imported. Every input is a
document slice 5 already wrote plus a sealed-release artifact id the
caller already holds; the only bytes written are one JSON file under the
operator-named output directory. Upgrading and rolling back stay the
verbs that already own them -- this only records what they did.

**A receipt is a function of its inputs.** The documents carry no
timestamp, no absolute path, and no ambient state: a package is named by
its transport-bound filename rather than where it sits, so a second run
over identical inputs writes byte-identical bytes. That is what lets a
reviewer re-derive a receipt instead of trusting it.

**Two refusals, not two reports.** An upgrade that would leave a packaged
owner supplying nothing refuses through slice 6's
:func:`refuse_dropped_module_owner`, and one whose candidate excludes the
OKG build the deployment actually runs refuses here. A rollback whose
baseline package is no longer on disk refuses too: a receipt claiming a
revert to a release nobody can rebuild is worse than no receipt.

**Conformance evidence.** Planning receipts do not establish executed recovery.
:func:`recovery_evidence_from_receipts` refuses that conversion; restoration,
interruption recovery and digest fences remain unproven by these documents.
:func:`legacy_authority_evidence` fills
:class:`ConformanceLegacyAuthorityEvidence` only from the real legacy inventory
the caller supplies; it does not invent an inventory.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Optional

from okg.substrate.catalog.module_provenance import (
    module_provenance_entries,
    refuse_dropped_module_owner,
)
from okg.substrate.contracts.deployment_product.lock import (
    canonical_json_bytes,
    sha256_digest,
)
from okg.substrate.distributions.install_plan import (
    InstallLockDrift,
    InstallOutputPathExists,
)
from okg.substrate.distributions.package import (
    DistributionPackageError,
    OkgCompatibility,
    verify_distribution_package,
)
from okg.substrate.errors import SubstrateError


__all__ = [
    "DRIFT_RECEIPT_SCHEMA",
    "LegacyInventoryNotSupplied",
    "LifecycleReceiptError",
    "RECEIPT_FILENAMES",
    "ROLLBACK_RECEIPT_SCHEMA",
    "ReleaseSide",
    "RollbackBaselinePackageMissing",
    "RollbackCandidateNotDistinct",
    "UPGRADE_RECEIPT_SCHEMA",
    "UpgradeOkgRangeBroken",
    "LEGACY_INPUT_INVENTORY_SCHEMA",
    "LEGACY_INPUT_KINDS",
    "canonical_legacy_inventory_entries",
    "legacy_authority_evidence",
    "legacy_input_inventory_declaration",
    "legacy_input_inventory_digest",
    "recovery_evidence_from_receipts",
    "resolve_drift_receipt",
    "resolve_rollback_receipt",
    "resolve_upgrade_receipt",
    "write_lifecycle_receipt",
]


UPGRADE_RECEIPT_SCHEMA = "okg.deployment_product.upgrade_receipt/v1"
ROLLBACK_RECEIPT_SCHEMA = "okg.deployment_product.rollback_receipt/v1"
DRIFT_RECEIPT_SCHEMA = "okg.deployment_product.drift_receipt/v1"

RECEIPT_FILENAMES = {
    "upgrade": "upgrade-receipt.json",
    "rollback": "rollback-receipt.json",
    "drift": "drift-receipt.json",
}

_MIGRATED = "migrated"
_REFUSED = "refused"
_DISPOSITIONS = frozenset({_MIGRATED, _REFUSED})


class LifecycleReceiptError(SubstrateError):
    """A receipt could not be produced from the inputs as given.

    Every payload names a `reason`, and nothing partial has been written.
    A refusal can also mean that planning inputs cannot support the requested
    evidence, even when the inputs agree.
    """

    code = "lifecycle_receipt_refused"


class UpgradeOkgRangeBroken(LifecycleReceiptError):
    """The candidate's OKG window excludes the build this deployment runs.

    The candidate plan was resolved against whatever OKG built it; this
    checks it against the version the deployment is actually on, which is
    the one the upgrade would leave running.
    """

    code = "upgrade_okg_range_broken"


class RollbackBaselinePackageMissing(LifecycleReceiptError):
    """The baseline package is absent or its bytes cannot be verified."""

    code = "rollback_baseline_package_missing"


class RollbackCandidateNotDistinct(LifecycleReceiptError):
    """The candidate release is the baseline; there is nothing to revert."""

    code = "rollback_candidate_not_distinct"


class LegacyInventoryNotSupplied(LifecycleReceiptError):
    """No real legacy inventory was supplied, and none is invented here."""

    code = "legacy_inventory_not_supplied"


@dataclass(frozen=True)
class ReleaseSide:
    """One side of a lifecycle event: its plan, its lock, its release id."""

    plan: Mapping[str, Any]
    lock: Mapping[str, Any]
    sealed_release_artifact_id: str


def _sealed(document: dict[str, Any]) -> dict[str, Any]:
    """Stamp the receipt's own digest over everything else it says."""
    body = {name: value for name, value in document.items() if name != "receipt_digest"}
    document["receipt_digest"] = sha256_digest(canonical_json_bytes(body))
    return document


def _side(side: ReleaseSide) -> dict[str, Any]:
    plan, lock = side.plan, side.lock
    return {
        "plan_digest": plan["plan_digest"],
        "lock_digest": lock["lock_digest"],
        "okg_version": lock["okg_version"],
        "bundle_id": lock["bundle_id"],
        # The filename is transport-bound, so naming the file rather than
        # its path keeps a receipt identical across machines.
        "package_filename": plan["package"]["filename"],
        "package_transport_digest": plan["package"]["transport_digest"],
        "package_release_digest": plan["package"]["release"]["release_digest"],
        "sealed_release_artifact_id": side.sealed_release_artifact_id,
    }


def _provenance(plan: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    """Read slice 6's provenance shape off a slice 5 plan.

    The plan records each module's kind and object digests but not its
    owner, which is the first segment of the `owner:name` id, nor the
    release digest, which is the package the whole plan resolved from.
    """
    release_digest = plan["package"]["release"]["release_digest"]
    return module_provenance_entries(
        [
            {
                "module_id": str(item["module_id"]),
                "owner": str(item["module_id"]).split(":", 1)[0],
                "provider_kind": item.get("provider_kind"),
                "package_release_digest": release_digest,
                "object_digests": item.get("object_digests") or (),
            }
            for item in plan.get("modules") or ()
        ]
    )


def resolve_upgrade_receipt(
    baseline: ReleaseSide,
    candidate: ReleaseSide,
    *,
    okg_version: Optional[str] = None,
) -> dict[str, Any]:
    """Record what an upgrade to `candidate` would change, or refuse it."""

    version = str(okg_version or baseline.lock["okg_version"])
    declared = candidate.plan["okg"]["compatibility"]
    if declared is not None:
        window = OkgCompatibility(
            min_version=str(declared["min_version"]),
            max_version_exclusive=str(declared["max_version_exclusive"]),
        )
        try:
            supported = window.supports(version)
        except ValueError as exc:
            raise UpgradeOkgRangeBroken(
                f"OKG {version} is not a version the candidate window can be "
                f"compared against",
                data={
                    "okg_version": version,
                    "declared": dict(declared),
                    "reason": "okg_version_unknown",
                },
                fix_hint="report the deployment's OKG build as a PEP 440 version",
            ) from exc
        if not supported:
            raise UpgradeOkgRangeBroken(
                f"the candidate package refuses OKG {version}, which this "
                f"deployment runs",
                data={
                    "okg_version": version,
                    "declared": dict(declared),
                    "reason": "outside_declared_window",
                },
                fix_hint=(
                    "upgrade OKG into the candidate's window first, or select a "
                    "candidate package whose window covers this build"
                ),
            )
    return _sealed(
        {
            "schema": UPGRADE_RECEIPT_SCHEMA,
            "baseline": _side(baseline),
            "candidate": _side(candidate),
            "okg": {
                "version": version,
                "compatibility": None if declared is None else dict(declared),
                "satisfied": True,
            },
            "module_provider_changes": refuse_dropped_module_owner(
                _provenance(baseline.plan),
                _provenance(candidate.plan),
            ),
        }
    )


def resolve_rollback_receipt(
    baseline: ReleaseSide,
    candidate: ReleaseSide,
    *,
    baseline_package_path: Path | str,
) -> dict[str, Any]:
    """Record that activation was reverted from `candidate` to `baseline`.

    The baseline package must still be on disk under the transport-bound
    filename the baseline plan names, and its verified transport digest must
    match that plan. This establishes available package bytes, not execution
    of a rollback. Absent or unverifiable bytes refuse rather than record.
    """

    if candidate.sealed_release_artifact_id == baseline.sealed_release_artifact_id:
        raise RollbackCandidateNotDistinct(
            "the candidate sealed release is the baseline; nothing was reverted",
            data={
                "sealed_release_artifact_id": baseline.sealed_release_artifact_id,
                "reason": "candidate_equals_baseline",
            },
            fix_hint="name the release that was activated, not the one it replaced",
        )
    expected = str(baseline.plan["package"]["filename"])
    path = Path(baseline_package_path)
    if not path.is_file() or path.name != expected:
        raise RollbackBaselinePackageMissing(
            f"the baseline package {expected} is not present at {path}",
            data={
                "expected_filename": expected,
                "observed_name": path.name,
                "present": path.is_file(),
                "reason": ("filename_mismatch" if path.is_file() else "package_absent"),
            },
            fix_hint=(
                "restore the baseline package bytes before recording the "
                "rollback; the receipt must name a release that can still be "
                "verified"
            ),
        )
    verified = None
    try:
        verified = verify_distribution_package(path)
        transport = verified.verification_receipt().transport_archive_raw_blob["digest"]
        if transport != baseline.plan["package"]["transport_digest"]:
            raise RollbackBaselinePackageMissing(
                "the verified baseline archive does not match the plan's transport digest",
                data={"reason": "package_transport_digest_mismatch"},
                fix_hint="restore the exact baseline package recorded in the plan",
            )
    except DistributionPackageError as exc:
        raise RollbackBaselinePackageMissing(
            "the baseline package bytes could not be verified",
            data={
                "reason": "package_verification_failed",
                "package_error_code": exc.code,
            },
            fix_hint="restore the exact baseline package recorded in the plan",
        ) from exc
    finally:
        if verified is not None:
            verified.close_runtime_authority()
    return _sealed(
        {
            "schema": ROLLBACK_RECEIPT_SCHEMA,
            "baseline": _side(baseline),
            "candidate": _side(candidate),
            "activation_reverted_to": "baseline",
            "baseline_package_present": True,
            "baseline_package_filename": expected,
        }
    )


def resolve_drift_receipt(
    recorded_lock: Mapping[str, Any],
    resolved_lock: Mapping[str, Any],
) -> dict[str, Any]:
    """Record match or drift per module between a recorded and a fresh lock.

    The caller re-plans with :func:`resolve_install_plan` and hands the
    resolved lock in. This reports rather than refuses -- a drift receipt
    that raised would be unable to describe the drift -- but it names the
    same `install_lock_drift` code the planning verb raises, so the
    refusal and the report read as one vocabulary.
    """

    recorded_digest = recorded_lock.get("lock_digest")
    resolved_digest = resolved_lock.get("lock_digest")
    if not recorded_digest or not resolved_digest:
        # Two locks that both name no digest would compare equal and read
        # as a clean install. Absence is not agreement.
        raise LifecycleReceiptError(
            "a drift receipt needs a lock digest on both sides",
            data={"reason": "lock_digest_absent"},
            fix_hint="compare locks written by install-plan; one of these has no digest",
        )
    recorded = dict(recorded_lock.get("modules") or {})
    resolved = dict(resolved_lock.get("modules") or {})
    modules = []
    for module_id in sorted(set(recorded) | set(resolved)):
        was, now = recorded.get(module_id), resolved.get(module_id)
        if was is None:
            status = "added"
        elif now is None:
            status = "removed"
        else:
            status = "match" if was == now else "digest_changed"
        modules.append(
            {
                "module_id": module_id,
                "status": status,
                "recorded_object_digests": was,
                "resolved_object_digests": now,
            }
        )
    drifted = recorded_digest != resolved_digest
    return _sealed(
        {
            "schema": DRIFT_RECEIPT_SCHEMA,
            "recorded_lock_digest": recorded_digest,
            "resolved_lock_digest": resolved_digest,
            "status": "drift" if drifted else "match",
            "drift_code": InstallLockDrift.code if drifted else None,
            "modules": modules,
        }
    )


def write_lifecycle_receipt(
    receipt: Mapping[str, Any],
    output_dir: Path | str,
    kind: str,
) -> dict[str, Any]:
    """Write one resolved receipt into an output directory that must be new."""

    output_dir = Path(output_dir)
    path = output_dir / RECEIPT_FILENAMES[kind]
    if output_dir.exists():
        raise InstallOutputPathExists(
            f"output path {output_dir} already exists",
            data={"output_dir": str(output_dir)},
            fix_hint="name an unused output directory; a receipt is never overwritten",
        )
    output_dir.mkdir(parents=True)
    path.write_bytes(canonical_json_bytes(dict(receipt)) + b"\n")
    return {
        "receipt_path": str(path),
        "kind": kind,
        "receipt_digest": receipt["receipt_digest"],
    }


def recovery_evidence_from_receipts(
    upgrade: Mapping[str, Any],
    rollback: Mapping[str, Any],
    *,
    last_known_good_generation_id: str,
) -> dict[str, Any]:
    """Refuse recovery claims unsupported by inert planning receipts.

    This callable is retained for compatibility. Neither matching receipt
    identities nor a supplied generation establishes that recovery executed.
    There is no measured execution producer connected to this helper, so it
    never returns a conformance recovery block.
    """
    raise LifecycleReceiptError(
        "planning receipts cannot establish executed recovery",
        data={"reason": "recovery_evidence_from_plans_unsupported"},
        fix_hint=(
            "Record and verify executed recovery separately; this planning-only "
            "helper cannot produce conformance recovery evidence."
        ),
    )


LEGACY_INPUT_INVENTORY_SCHEMA = "okg.deployment_product.legacy_input_inventory/v1"
# The closed set of legacy inputs a consumer declares.  Nothing outside the set
# is a legacy input, and every entry in the set is counted: the denominator of
# the legacy_authority arm is the number of unique entries, with no exclusions.
LEGACY_INPUT_KINDS = frozenset(
    {
        # A path into the authoring checkout the legacy install read at run
        # time (an ``OKG_PROFILES_DIR`` pointing at a repository).
        "authoring_checkout_path",
        # An install command that consumed the checkout rather than the
        # sealed package (``okg install --profile <bundle>`` against it).
        "legacy_install_invocation",
        # A hand-copied or hand-pruned schema slice the install still required.
        "copied_schema_authority",
    }
)
_LEGACY_ENTRY_FIELDS = ("input_id", "kind", "disposition", "evidence_ref")
# The wire ``IdentifierValue`` shape; the inventory id and every input id
# travel in conformance documents, so they are refused here, never rewritten.
_LEGACY_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,127}$")


def canonical_legacy_inventory_entries(
    inventory: Sequence[Mapping[str, Any]],
) -> list[dict[str, str]]:
    """Validate and sort the readable inventory entries.

    Every entry carries exactly the four fields ``input_id``, ``kind``,
    ``disposition`` and ``evidence_ref`` as non-empty strings; ``kind`` is one
    of :data:`LEGACY_INPUT_KINDS` and ``disposition`` is ``migrated`` or
    ``refused``.  An unresolved entry, an unknown kind, a missing evidence
    reference or a duplicated input id refuses: the audit counts inputs it can
    account for, never inputs it is unsure about.
    """

    entries: list[dict[str, str]] = []
    for item in inventory:
        if not isinstance(item, Mapping) or set(item) != set(_LEGACY_ENTRY_FIELDS):
            raise LifecycleReceiptError(
                "every legacy inventory entry carries exactly input_id, kind, "
                "disposition and evidence_ref",
                data={"reason": "inventory_entry_invalid"},
                fix_hint="declare the four fields and nothing else",
            )
        entry = {field: item[field] for field in _LEGACY_ENTRY_FIELDS}
        if not all(isinstance(value, str) and value for value in entry.values()):
            raise LifecycleReceiptError(
                "every legacy inventory field must be a non-empty string",
                data={"reason": "inventory_entry_invalid"},
                fix_hint="resolve the input before the audit",
            )
        if not _LEGACY_IDENTIFIER_RE.fullmatch(entry["input_id"]):
            raise LifecycleReceiptError(
                "legacy inventory input_id does not fit the wire identifier shape",
                data={"reason": "inventory_entry_invalid"},
                fix_hint=(
                    "use at most 128 lowercase letters, digits, '_', '.', ':' "
                    "or '-'"
                ),
            )
        if any(char.isspace() for char in entry["evidence_ref"]):
            raise LifecycleReceiptError(
                "legacy inventory evidence_ref must be one opaque reference",
                data={"reason": "inventory_entry_invalid"},
                fix_hint="name one file, URL or record id without whitespace",
            )
        if entry["kind"] not in LEGACY_INPUT_KINDS:
            raise LifecycleReceiptError(
                "legacy inventory entry kind is not in the closed kind set",
                data={"reason": "inventory_kind_unknown", "kind": entry["kind"]},
                fix_hint=f"use one of {', '.join(sorted(LEGACY_INPUT_KINDS))}",
            )
        if entry["disposition"] not in _DISPOSITIONS:
            raise LifecycleReceiptError(
                "every legacy inventory entry needs a migrated or refused disposition",
                data={"reason": "inventory_entry_invalid"},
                fix_hint=(
                    "resolve the input before the audit; an unresolved input is "
                    "refused rather than counted"
                ),
            )
        entries.append(entry)
    entries.sort(key=lambda entry: tuple(entry[field] for field in _LEGACY_ENTRY_FIELDS))
    if len({entry["input_id"] for entry in entries}) != len(entries):
        # One input counted twice would inflate declared_inputs and still
        # balance against migrated plus refused, so the model would accept
        # an audit that covers fewer inputs than it claims.
        raise LifecycleReceiptError(
            "the legacy inventory names an input more than once",
            data={"reason": "inventory_duplicate_input"},
            fix_hint="give each declared legacy input exactly one disposition",
        )
    return entries


def legacy_input_inventory_digest(inventory: Sequence[Mapping[str, Any]]) -> str:
    """The one digest that binds the readable inventory to request and receipt.

    It is the sha256 of the canonical JSON of the sorted entries, so a
    consumer can recompute it from the readable file with no OKG code.
    """

    return sha256_digest(canonical_json_bytes(canonical_legacy_inventory_entries(inventory)))


def legacy_input_inventory_declaration(
    inventory_id: str,
    inventory: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Fill the request's ``inputs.legacy_input_inventory`` from the readable file.

    An explicitly empty inventory is a valid declaration (``entry_count`` 0,
    ``empty`` true), but :func:`legacy_authority_evidence` never turns it into
    passing evidence.
    """

    if not isinstance(inventory_id, str) or not _LEGACY_IDENTIFIER_RE.fullmatch(
        inventory_id
    ):
        raise LifecycleReceiptError(
            "a legacy inventory declaration needs a wire-shaped inventory id",
            data={"reason": "inventory_id_missing"},
            fix_hint="name the readable inventory file's identity",
        )
    entries = canonical_legacy_inventory_entries(inventory)
    return {
        "inventory_id": inventory_id,
        "inventory_digest": sha256_digest(canonical_json_bytes(entries)),
        "media_type": "application/json",
        "entry_count": len(entries),
        "empty": not entries,
        "complete": True,
    }


def legacy_authority_evidence(
    inventory: Optional[Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    """Fill `ConformanceLegacyAuthorityEvidence` from a real legacy inventory.

    There is no argument that makes this synthesize an inventory. A run
    with no legacy deployment behind it -- the hermetic fixture arm, for
    one -- has nothing to audit, and an audit of nothing that reported
    `fallback_authority_absent` would pass the arm while proving nothing.
    Such a caller must leave the arm unclaimed.

    The inventory is the readable document behind the request declaration
    (see :func:`legacy_input_inventory_declaration`); both sides digest the
    same canonical entries, so the evidence binds the declared inventory.
    """

    if not inventory:
        raise LegacyInventoryNotSupplied(
            "no legacy inventory was supplied, and none is synthesized here",
            data={"reason": "inventory_absent"},
            fix_hint=(
                "supply the real legacy deployment's declared inputs, each with "
                "a migrated or refused disposition, or leave the "
                "legacy_authority arm unclaimed"
            ),
        )
    entries = canonical_legacy_inventory_entries(inventory)
    audit = {
        "declared_inventory_digest": sha256_digest(canonical_json_bytes(entries)),
        "declared_inventory_entry_count": len(entries),
        "declared_inputs": len(entries),
        "migrated_inputs": sum(
            1 for entry in entries if entry["disposition"] == _MIGRATED
        ),
        "refused_inputs": sum(
            1 for entry in entries if entry["disposition"] == _REFUSED
        ),
        "unresolved_inputs": 0,
        "fallback_authority_absent": True,
    }
    audit["audit_digest"] = sha256_digest(canonical_json_bytes(audit))
    return audit
