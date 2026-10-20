#!/usr/bin/env python3
"""Prove the worktree-branch safety model and its load-bearing guards."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCSetupError,
    find_tla2tools_jar,
    run_tlc_formal_report,
    select_properties,
    tlc_jvm_args,
    tlc_workers,
)
from pact.digest import intent_digest as v5_intent_digest
from pact.store import load as v5_load


REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = REPO_ROOT / "formal" / "worktree_branches"
MODEL = MODEL_DIR / "WorktreeBranchSafety.tla"
CONFIG = MODEL_DIR / "WorktreeBranchSafety.cfg"
BINDINGS = MODEL_DIR / "pact_bindings.yaml"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "worktree-branch-safety"


@dataclass(frozen=True)
class PropertyOwner:
    change_id: str
    requirement_id: str
    formal_id: str
    refinement_tests: tuple[str, ...]


OWNERS = {
    "DefaultIsolation": PropertyOwner(
        "worktree-branch-execution-context",
        "req.worktree-branch-context.capsule",
        "formal.worktree-branch-context.default-isolation",
        (
            "tests/pact/test_worktree_branch_safety_model_contract.py::"
            "test_default_isolation_refines_complete_capsule_and_default_guard",
        ),
    ),
    "ContributionOverlayIsolation": PropertyOwner(
        "worktree-branch-execution-context",
        "req.worktree-branch-context.state-schema",
        "formal.worktree-branch-context.contribution-overlay",
        (
            "tests/pact/test_worktree_branch_safety_model_contract.py::"
            "test_contribution_overlay_refines_branch_qualified_state",
        ),
    ),
    "TransactionRecovery": PropertyOwner(
        "worktree-branch-transaction-safety",
        "req.worktree-branch-transaction.operation-protocol",
        "formal.worktree-branch-transaction.recovery",
        (
            "tests/pact/test_worktree_branch_safety_model_contract.py::"
            "test_transaction_recovery_refines_atomic_publication_and_effects",
        ),
    ),
    "ProjectionClosure": PropertyOwner(
        "worktree-deterministic-trunk-projection",
        "req.worktree-trunk-projection.closed-input",
        "formal.worktree-trunk-projection.closure",
        (
            "tests/pact/test_worktree_branch_safety_model_contract.py::"
            "test_projection_closure_refines_clean_trunk_without_preview_copy",
        ),
    ),
    "LifecycleFencing": PropertyOwner(
        "worktree-branch-lifecycle",
        "req.worktree-branch-lifecycle.refresh",
        "formal.worktree-branch-lifecycle.fencing",
        (
            "tests/pact/test_worktree_branch_safety_model_contract.py::"
            "test_lifecycle_fencing_refines_epoch_leases_and_receipt_restart",
        ),
    ),
    "CapabilityFencing": PropertyOwner(
        "activate-worktree-chronos",
        "req.worktree-activation.dark-release",
        "formal.worktree-activation.capability-fencing",
        (
            "tests/pact/test_worktree_branch_safety_model_contract.py::"
            "test_capability_fencing_refines_db_time_and_receipt_authority",
        ),
    ),
}

PROPERTIES = [
    TLCProperty(
        property_id,
        {
            "DefaultIsolation": (
                "TLC proved missing or stale worktree context cannot mutate default."
            ),
            "ContributionOverlayIsolation": (
                "TLC proved branch contributions remain isolated over an immutable base."
            ),
            "TransactionRecovery": (
                "TLC proved staged work is non-authoritative and durable recovery "
                "publishes and releases effects at most once."
            ),
            "ProjectionClosure": (
                "TLC proved both Git merge orders produce the same complete clean-trunk "
                "projection without preview leakage."
            ),
            "LifecycleFencing": (
                "TLC proved shared/exclusive lifecycle fencing, refresh/GC lease epochs, "
                "and rollback-versus-restart receipt semantics."
            ),
            "CapabilityFencing": (
                "TLC proved a complete digest-bound canary is revalidated with "
                "database-authoritative time immediately before publication."
            ),
        }[property_id],
        refinement_tests=owner.refinement_tests,
        expected_invariants=(
            (
                property_id,
                "PostCommitRecoveryEnabled",
                "RollbackPostCommitRecoveryEnabled",
            )
            if property_id == "TransactionRecovery"
            else (property_id,)
        ),
        expected_temporal_properties=(
            ("CommittedEventuallyFinalized",)
            if property_id == "TransactionRecovery"
            else ()
        ),
    )
    for property_id, owner in OWNERS.items()
]

WITNESS_CONFIGS = {
    property_id: MODEL_DIR / f"WorktreeBranchSafety{property_id}Witness.cfg"
    for property_id in OWNERS
}
POST_COMMIT_WITNESS = MODEL_DIR / "WorktreeBranchSafetyPostCommitRecoveryWitness.cfg"

# TLC stops at the first violated invariant.  A witness configuration that
# lists the positive config's whole invariant set therefore proves only
# whichever invariant happens to fire first, and can silently stop proving the
# invariant named by the surrounding checker.  Keep this map explicit so the
# checker validates the config before launching TLC.
WITNESS_EXPECTATIONS = {
    **{property_id: property_id for property_id in OWNERS},
    "PostCommitRecovery": "RollbackPostCommitRecoveryEnabled",
}

_TLC_CONFIG_DIRECTIVES = {
    "CHECK_DEADLOCK",
    "CONSTANT",
    "CONSTRAINT",
    "INVARIANT",
    "INVARIANTS",
    "PROPERTY",
    "PROPERTIES",
    "SPECIFICATION",
    "VIEW",
}

def _report_path(path: Path) -> str:
    """Return a repo-relative artifact path when possible, absolute otherwise."""
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT.resolve()))
    except ValueError:
        return str(resolved)


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return payload


def _config_invariants(path: Path) -> tuple[str, ...]:
    """Return invariant names in declaration order from a TLC config.

    This deliberately preserves duplicates.  A set-based parser would let a
    duplicated invariant (or a config that copied the positive set) pass a
    cardinality check while still leaving TLC's first-violation ordering in
    charge of which property is actually witnessed.
    """
    names: list[str] = []
    in_block = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("\\*", 1)[0].strip()
        if not line:
            continue
        parts = line.split()
        directive = parts[0]
        if directive in {"INVARIANT", "INVARIANTS"}:
            in_block = True
            names.extend(part.rstrip(",") for part in parts[1:])
            continue
        if directive in _TLC_CONFIG_DIRECTIVES:
            in_block = False
            continue
        if in_block:
            names.append(directive.rstrip(","))
    return tuple(names)


def _validate_witness_configs() -> None:
    """Reject missing, bundled, or misnamed worktree witness configs."""
    configs = {
        **WITNESS_CONFIGS,
        "PostCommitRecovery": POST_COMMIT_WITNESS,
    }
    for witness_id, expected_invariant in WITNESS_EXPECTATIONS.items():
        config = configs[witness_id]
        if not config.is_file():
            raise ValueError(f"missing worktree witness config: {config}")
        invariants = _config_invariants(config)
        if invariants != (expected_invariant,):
            raise ValueError(
                f"{config.relative_to(REPO_ROOT)} must declare exactly one "
                f"invariant [{expected_invariant}], got {list(invariants)!r}"
            )
def _validate_bindings() -> None:
    payload = _load_yaml(BINDINGS)
    rows = payload.get("bindings")
    if payload.get("schema_version") != 1 or not isinstance(rows, list):
        raise ValueError("pact_bindings.yaml must use schema_version 1 and bindings[]")
    by_property = {row.get("property_id"): row for row in rows if isinstance(row, dict)}
    if set(by_property) != set(OWNERS):
        raise ValueError(
            "pact binding properties differ from checker ownership: "
            f"expected {sorted(OWNERS)}, got {sorted(by_property)}"
        )

    for property_id, owner in OWNERS.items():
        row = by_property[property_id]
        expected_owner = {
            "owner_change_id": owner.change_id,
            "owner_requirement_id": owner.requirement_id,
            "formal_id": owner.formal_id,
        }
        for field, expected in expected_owner.items():
            if row.get(field) != expected:
                raise ValueError(
                    f"{property_id} {field} must be {expected!r}, got {row.get(field)!r}"
                )

        manifest_path = REPO_ROOT / "pact" / "changes" / owner.change_id / "pact.yaml"
        if not manifest_path.is_file():
            raise ValueError(
                f"owner manifest is missing: {manifest_path}; a checkout without "
                "this repository's PACT change records (such as the public copy) "
                "can run the model alone with --without-pact-records "
                "(scripts/run_formal_suite.py --without-pact-records forwards it)"
            )
        current_digest = v5_intent_digest(v5_load(manifest_path))
        if row.get("intent_digest") != current_digest:
            raise ValueError(
                f"{property_id} owner intent digest is stale: "
                f"bound {row.get('intent_digest')!r}, current {current_digest!r}"
            )

        # v5 has no `formal[]`. `pact migrate` carried each v4 binding into a
        # `formal_check` evidence row whose note names the property, module
        # and checker command; the row's requirement is the owning one. The
        # v4 `formal_id`, `config` and `refinement_tests` were not carried, so
        # the config and every refinement test are checked as files here and
        # `formal_id` stays a record in this file rather than a manifest
        # assertion.
        carried = [
            candidate
            for candidate in v5_load(manifest_path).evidence
            if candidate.kind == "formal_check"
            and candidate.note
            and " ".join(candidate.note.split()).startswith(
                f"formal binding property {property_id} in "
            )
        ]
        if len(carried) != 1:
            raise ValueError(
                f"{owner.change_id} must carry exactly one {property_id} "
                f"formal binding, found {len(carried)}"
            )
        binding_row = carried[0]
        note = " ".join(binding_row.note.split())
        if binding_row.requirement != owner.requirement_id:
            raise ValueError(
                f"{property_id} requirement drifted: expected "
                f"{owner.requirement_id!r}, got {binding_row.requirement!r}"
            )
        if "formal/worktree_branches/WorktreeBranchSafety.tla" not in note:
            raise ValueError(f"{property_id} module drifted: {note!r}")
        for reference in (
            "formal/worktree_branches/WorktreeBranchSafety.cfg",
            *owner.refinement_tests,
        ):
            target = REPO_ROOT / str(reference).partition("::")[0]
            if not target.is_file():
                raise ValueError(
                    f"{property_id} names a missing refinement reference: {reference}"
                )
        expected_checker = (
            "uv run python scripts/check_formal_worktree_branch_safety.py "
            f"--property-id {property_id}"
        )
        checker_command = " ".join(str(binding_row.command or "").split())
        if checker_command != expected_checker:
            raise ValueError(
                f"{property_id} checker_command drifted: "
                f"expected {expected_checker!r}, got {checker_command!r}"
            )


def _run_witnesses(
    *,
    property_ids: tuple[str, ...],
    out_dir: Path,
    timeout_seconds: int,
    witness_ids: tuple[str, ...] = (),
) -> int:
    jar = find_tla2tools_jar(REPO_ROOT)
    java = shutil.which("java")
    if jar is None or java is None:
        print(
            "formal-worktree-branch-safety: Java and tla2tools.jar are required",
            file=sys.stderr,
        )
        return 2

    workers = tlc_workers()
    witness_dir = out_dir / "guard-removal-witnesses"
    witness_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    if witness_ids:
        config_by_id = {
            **WITNESS_CONFIGS,
            "PostCommitRecovery": POST_COMMIT_WITNESS,
        }
        guard_by_id = {
            **{property_id: f"Guard{property_id}" for property_id in OWNERS},
            "PostCommitRecovery": "GuardCommittedOutboxRecovery",
        }
        witness_cases = [
            (
                witness_id,
                WITNESS_EXPECTATIONS[witness_id],
                guard_by_id[witness_id],
                config_by_id[witness_id],
            )
            for witness_id in witness_ids
        ]
    else:
        witness_cases = [
            (
                property_id,
                property_id,
                f"Guard{property_id}",
                WITNESS_CONFIGS[property_id],
            )
            for property_id in property_ids
        ]
        if "TransactionRecovery" in property_ids:
            witness_cases.append(
                (
                    "PostCommitRecovery",
                    "RollbackPostCommitRecoveryEnabled",
                    "GuardCommittedOutboxRecovery",
                    POST_COMMIT_WITNESS,
                )
            )
    for witness_id, expected_invariant, guard_removed, config in witness_cases:
        metadir = witness_dir / f"tlc-meta-{witness_id}-{uuid.uuid4().hex}"
        command = [
            java,
            *tlc_jvm_args(workers),
            "-cp",
            str(jar),
            "tlc2.TLC",
            "-config",
            str(config),
            "-deadlock",
            "-workers",
            workers,
            "-metadir",
            str(metadir),
            str(MODEL),
        ]
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
            output = completed.stdout + completed.stderr
            matched = (
                completed.returncode == 12
                and f"Invariant {expected_invariant} is violated" in output
            )
            exit_code = completed.returncode
        except subprocess.TimeoutExpired as exc:
            stdout = (
                exc.stdout.decode(errors="replace")
                if isinstance(exc.stdout, bytes)
                else (exc.stdout or "")
            )
            stderr = (
                exc.stderr.decode(errors="replace")
                if isinstance(exc.stderr, bytes)
                else (exc.stderr or "")
            )
            output = stdout + stderr
            matched = False
            exit_code = 124
        artifact = witness_dir / f"{witness_id}.log"
        artifact.write_text(output, encoding="utf-8")
        rows.append(
            {
                "witness_id": witness_id,
                "guard_removed": guard_removed,
                "config": str(config.relative_to(REPO_ROOT)),
                "expected_invariant": expected_invariant,
                "matched": matched,
                "exit_code": exit_code,
                "artifact": _report_path(artifact),
            }
        )

    ok = all(row["matched"] is True for row in rows)
    report = witness_dir / "witnesses.json"
    report.write_text(
        json.dumps({"ok": ok, "witnesses": rows}, indent=2) + "\n",
        encoding="utf-8",
    )
    if not ok:
        print(
            f"formal-worktree-branch-safety: guard witness failure; see {report}",
            file=sys.stderr,
        )
        return 1
    print(
        "formal-worktree-branch-safety: all selected guard-removal witnesses "
        f"passed; report at {report}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=Path, default=MODEL)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--property-id", action="append", default=[], dest="property_ids"
    )
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument(
        "--skip-witnesses",
        action="store_true",
        help="run only the positive proof (not valid for final formal evidence)",
    )
    parser.add_argument(
        "--witness-only",
        action="store_true",
        help="run selected guard-removal witnesses without the positive proof",
    )
    parser.add_argument(
        "--witness-id",
        action="append",
        default=[],
        dest="witness_ids",
        help="run exactly this named witness config (repeatable)",
    )
    parser.add_argument(
        "--without-pact-records",
        action="store_true",
        help=(
            "skip checking the model's PACT owner bindings against this "
            "repository's change records, for a checkout that does not carry "
            "them; the TLC run and the witnesses are unchanged"
        ),
    )
    args = parser.parse_args(argv)

    try:
        if args.without_pact_records:
            print(
                "formal-worktree-branch-safety: PACT owner bindings NOT checked "
                "(--without-pact-records)",
                file=sys.stderr,
            )
        else:
            _validate_bindings()
        _validate_witness_configs()
        unknown_witnesses = sorted(set(args.witness_ids) - set(WITNESS_EXPECTATIONS))
        if unknown_witnesses:
            raise ValueError(
                "unknown witness id(s): " + ", ".join(unknown_witnesses)
            )
        selected = select_properties(PROPERTIES, args.property_ids)
    except (KeyError, ValueError) as exc:
        print(f"formal-worktree-branch-safety: {exc}", file=sys.stderr)
        return 2

    selected_ids = tuple(prop.property_id for prop in selected)
    if args.witness_ids and args.property_ids:
        print(
            "formal-worktree-branch-safety: --witness-id and --property-id "
            "cannot be combined",
            file=sys.stderr,
        )
        return 2
    if args.witness_ids and not args.witness_only:
        print(
            "formal-worktree-branch-safety: --witness-id requires "
            "--witness-only",
            file=sys.stderr,
        )
        return 2
    if args.witness_only:
        if args.skip_witnesses:
            print(
                "formal-worktree-branch-safety: --witness-only and "
                "--skip-witnesses are mutually exclusive",
                file=sys.stderr,
            )
            return 2
        witness_rc = _run_witnesses(
            property_ids=selected_ids if not args.witness_ids else (),
            out_dir=args.out.resolve(),
            timeout_seconds=args.timeout_seconds,
            witness_ids=tuple(args.witness_ids),
        )
        return witness_rc
    checker_command = "uv run python scripts/check_formal_worktree_branch_safety.py"
    if args.property_ids:
        checker_command += " " + " ".join(
            f"--property-id {property_id}" for property_id in args.property_ids
        )
    if args.without_pact_records:
        # The report must show that the owner bindings were not checked.
        checker_command += " --without-pact-records"

    try:
        result = run_tlc_formal_report(
            repo_root=REPO_ROOT,
            model_dir=MODEL_DIR,
            model=args.model,
            config=args.config,
            out_dir=args.out,
            properties=selected,
            expected_properties=PROPERTIES,
            checker_command=checker_command,
            timeout_seconds=args.timeout_seconds,
            min_distinct_states=100,
        )
    except TLCSetupError as exc:
        print(f"formal-worktree-branch-safety: {exc}", file=sys.stderr)
        return exc.exit_code
    if result.run_failed:
        print(
            "formal-worktree-branch-safety: TLC failed; see "
            f"{result.stdout_path} and {result.stderr_path}",
            file=sys.stderr,
        )
        return result.exit_code or 1

    if not args.skip_witnesses:
        witness_rc = _run_witnesses(
            property_ids=selected_ids,
            out_dir=args.out.resolve(),
            timeout_seconds=args.timeout_seconds,
        )
        if witness_rc != 0:
            return witness_rc

    print(f"formal-worktree-branch-safety: TLC passed; report at {result.report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
