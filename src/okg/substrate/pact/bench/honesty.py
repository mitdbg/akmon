"""Metric 1 — envelope honesty.

Probe states with known ground truth are driven through the verbs; the metric
is the fraction whose envelope verdict matches ground truth.

The six audited ok-lie classes are pinned as NAMED, IMMUTABLE rows. Their
``ground_truth`` is frozen here and asserted literally in
``tests/pact/bench/test_friction_metrics.py``: the ground truth must not be
editable by the same change that is being measured against it. The derived
rows below may grow with the verdict-contract table; the six may not be
redefined.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okg.substrate.pact.bench import calls, fixtures
from okg.substrate.pact.bench.calls import CallResult, Recorder
from okg.substrate.pact.bench.fixtures import (
    FixtureRepo,
    base_manifest,
    blocked_manifest,
    proven_manifest,
)
from okg.substrate.pact.bench.isolation import IsolationRefusal

RECORDED = "recorded"
DEFERRED = "deferred"
REFUSED = "refused"
# Today one error path emits no `ok` key at all (operations.py:3076 raises;
# cli.py prints `{"error": {...}}`). That is not a verdict, so it gets its own
# observed value rather than being folded into `refused`.
ABSENT = "absent"

# The six audited lie classes. IMMUTABLE: names and ground truth are frozen.
IMMUTABLE_CASE_IDS: tuple[str, ...] = (
    "bootstrap_on_blocked_change",
    "evidence_status_all_unsatisfied",
    "gate_post_edit_ledger_dead",
    "gate_task_done_proof_complete_ledger_dead",
    "gate_task_done_unknown_task",
    "attach_with_unknown_kind",
)

IMMUTABLE_GROUND_TRUTH: dict[str, str] = {
    "bootstrap_on_blocked_change": REFUSED,
    "evidence_status_all_unsatisfied": REFUSED,
    "gate_post_edit_ledger_dead": DEFERRED,
    "gate_task_done_proof_complete_ledger_dead": DEFERRED,
    "gate_task_done_unknown_task": REFUSED,
    "attach_with_unknown_kind": REFUSED,
}


def observed_verdict(result: CallResult) -> str:
    """Map today's envelope onto the tri-state vocabulary.

    A ``verdict`` key wins when present (that is what child 1 adds); until it
    exists, ``ok`` is the only signal, and a missing ``ok`` is ``absent``.
    """
    verdict = result.payload.get("verdict")
    if isinstance(verdict, str):
        return verdict
    if not result.ok_present:
        return ABSENT
    return RECORDED if result.ok else REFUSED


def verdict_is_coherent(result: CallResult) -> bool:
    """The envelope's ``ok`` must agree with its ``verdict``.

    Matching the ground truth is not enough once a ``verdict`` key exists: a
    payload could stamp ``verdict: refused`` while leaving ``ok: true`` for
    the exit status, and every existing consumer branches on ``ok``. That
    envelope is still lying, so it must not score as honest.
    """
    if not result.ok_present:
        return False
    return result.ok is (observed_verdict(result) == RECORDED)


def is_honest(result: CallResult, ground_truth: str) -> bool:
    return observed_verdict(result) == ground_truth and verdict_is_coherent(result)


@dataclass
class HonestyCase:
    case: str
    verb: str
    ground_truth: str
    immutable: bool
    run: Callable[[Recorder, Path, FixtureRepo], CallResult]
    precondition: Callable[[dict[str, Any]], bool] | None = None
    detail: Callable[[dict[str, Any]], dict[str, Any]] = field(
        default=lambda _payload: {}
    )


def _requirements_all_unsatisfied(payload: dict[str, Any]) -> bool:
    rows = payload.get("requirements") or []
    return bool(rows) and all(row.get("status") != "satisfied" for row in rows)


def _requirements_all_satisfied(payload: dict[str, Any]) -> bool:
    rows = payload.get("requirements") or []
    return bool(rows) and all(row.get("status") == "satisfied" for row in rows)


def _manifest_blockers(payload: dict[str, Any]) -> list[str]:
    """The blockers the BOOTSTRAP packet traces to the manifest itself.

    ``actionable`` is derived from the cutover gate (cli.py:4259), and in this
    fixture the gate's binding row is always environmental (no graph, no
    ledger) — it reads the same on a healthy change as on a fully blocked one.
    So the blocked-change probe is anchored on the packet's own task blockers,
    which come from the manifest's blocked task, not from the environment.
    """
    task = payload.get("task") or {}
    return [str(token) for token in (task.get("blockers") or [])]


def _blocker_category(payload: dict[str, Any]) -> str | None:
    blocker = payload.get("blocker") or {}
    category = blocker.get("category")
    return str(category) if category else None


def _ledger_is_dead(payload: dict[str, Any]) -> bool:
    authority = payload.get("workflow_authority") or {}
    ledger = authority.get("ledger_authority") or {}
    if ledger:
        return not ledger.get("fresh", False)
    registration = payload.get("intent_registration") or {}
    return registration.get("status") != "registered"


def _cases() -> tuple[HonestyCase, ...]:
    return (
        HonestyCase(
            case="bootstrap_on_blocked_change",
            verb="view --format=bootstrap",
            ground_truth=IMMUTABLE_GROUND_TRUTH["bootstrap_on_blocked_change"],
            immutable=True,
            run=lambda rec, root, _repo: calls.view_bootstrap(
                rec, root, "bench-honesty-blocked"
            ),
            # The subject must really be a BLOCKED CHANGE: the packet's task
            # blockers have to come from the manifest's own blocked task. An
            # earlier precondition tested `actionable is False`, which every
            # change in this fixture satisfies for environmental reasons — it
            # passed without the change being blocked at all.
            precondition=lambda payload: bool(_manifest_blockers(payload)),
            detail=lambda payload: {
                "actionable": payload.get("actionable"),
                "manifest_blockers": len(_manifest_blockers(payload)),
                "blocker_category": _blocker_category(payload),
            },
        ),
        HonestyCase(
            case="evidence_status_all_unsatisfied",
            verb="evidence status",
            ground_truth=IMMUTABLE_GROUND_TRUTH["evidence_status_all_unsatisfied"],
            immutable=True,
            run=lambda rec, root, _repo: calls.evidence_status(
                rec, root, "bench-honesty-unproven"
            ),
            precondition=_requirements_all_unsatisfied,
            detail=lambda payload: {
                "missing_requirements": len(payload.get("missing_requirements") or []),
            },
        ),
        HonestyCase(
            case="gate_post_edit_ledger_dead",
            verb="gate post-edit --full",
            ground_truth=IMMUTABLE_GROUND_TRUTH["gate_post_edit_ledger_dead"],
            immutable=True,
            run=lambda rec, root, _repo: calls.gate_post_edit(
                rec, root, "bench-honesty-unproven"
            ),
            precondition=_ledger_is_dead,
            detail=lambda payload: {
                "intent_registration": (payload.get("intent_registration") or {}).get(
                    "status"
                ),
            },
        ),
        HonestyCase(
            case="gate_task_done_proof_complete_ledger_dead",
            verb="gate task-done",
            ground_truth=IMMUTABLE_GROUND_TRUTH[
                "gate_task_done_proof_complete_ledger_dead"
            ],
            immutable=True,
            run=lambda rec, root, _repo: calls.gate_task_done(
                rec, root, "bench-honesty-proven", "task.one"
            ),
            precondition=lambda payload: (
                not payload.get("missing_proof") and _ledger_is_dead(payload)
            ),
            detail=lambda payload: {
                "marked": payload.get("marked"),
                "missing_proof": len(payload.get("missing_proof") or []),
                "queued_closure_intent": "closure_intent" in payload,
            },
        ),
        HonestyCase(
            case="gate_task_done_unknown_task",
            verb="gate task-done",
            ground_truth=IMMUTABLE_GROUND_TRUTH["gate_task_done_unknown_task"],
            immutable=True,
            run=lambda rec, root, _repo: calls.gate_task_done(
                rec, root, "bench-honesty-proven", "task.does-not-exist"
            ),
            precondition=lambda payload: "error" in payload,
            detail=lambda payload: {
                "error_code": (payload.get("error") or {}).get("code"),
                "ok_key_present": "ok" in payload,
            },
        ),
        HonestyCase(
            case="attach_with_unknown_kind",
            verb="evidence attach",
            ground_truth=IMMUTABLE_GROUND_TRUTH["attach_with_unknown_kind"],
            immutable=True,
            run=lambda rec, root, _repo: calls.evidence_attach(
                rec,
                root,
                "bench-honesty-attach",
                "req.one",
                # A kind the registry genuinely does not know. The audit found
                # this class through `intent_review`, which is now canonical
                # because `evidence approve` writes it — probing with a
                # registered kind would measure nothing.
                kind=fixtures.UNREGISTERED_KIND,
                ref="tests/bench/test_probe.py",
                summary="Frozen benchmark fixture evidence.",
            ),
            detail=lambda payload: {"evidence": payload.get("evidence")},
        ),
        # --- derived rows: generated from the verdict-contract table --------
        # CONTROL for `bootstrap_on_blocked_change`: the same verb, the same
        # isolation, a change with nothing blocked. It reports the SAME
        # envelope (ok true, actionable false, blocker category `graph`) — so
        # the packet's verdict carries no information about whether the change
        # is workable, which is the point of the immutable row above.
        HonestyCase(
            case="bootstrap_on_healthy_change",
            verb="view --format=bootstrap",
            ground_truth=RECORDED,
            immutable=False,
            run=lambda rec, root, _repo: calls.view_bootstrap(
                rec, root, "bench-honesty-proven"
            ),
            precondition=lambda payload: not _manifest_blockers(payload),
            detail=lambda payload: {
                "actionable": payload.get("actionable"),
                "manifest_blockers": len(_manifest_blockers(payload)),
                "blocker_category": _blocker_category(payload),
            },
        ),
        HonestyCase(
            case="new_fresh_change",
            verb="new",
            ground_truth=RECORDED,
            immutable=False,
            run=lambda rec, root, _repo: calls.new_change(
                rec,
                root,
                "bench-honesty-new",
                title="Bench honesty new",
                intent="The fixture must record a fresh change.",
                acceptance="The manifest exists and validates.",
            ),
        ),
        HonestyCase(
            case="new_duplicate_change_id",
            verb="new",
            ground_truth=REFUSED,
            immutable=False,
            run=lambda rec, root, _repo: calls.new_change(
                rec,
                root,
                "bench-honesty-new",
                title="Bench honesty new",
                intent="The fixture must record a fresh change.",
                acceptance="The manifest exists and validates.",
            ),
            precondition=lambda payload: "error" in payload,
            detail=lambda payload: {"ok_key_present": "ok" in payload},
        ),
        HonestyCase(
            case="attach_known_kind_ledger_dead",
            verb="evidence attach",
            ground_truth=DEFERRED,
            immutable=False,
            run=lambda rec, root, _repo: calls.evidence_attach(
                rec,
                root,
                "bench-honesty-attach-known",
                "req.one",
                kind="executed_test",
                ref="tests/bench/test_probe.py",
                command="uv run pytest tests/bench/test_probe.py -q",
                summary="Frozen benchmark fixture evidence.",
            ),
            precondition=lambda payload: payload.get("coordination") == "deferred",
            detail=lambda payload: {
                "coordination": payload.get("coordination"),
                "pending_closure_intents": payload.get("pending_closure_intents"),
            },
        ),
        # The design's operator-action-required counterexample, probed
        # DIRECTLY. It used to enter the corpus only as a side effect of the
        # dead-ledger episode looping on `evidence sync`; once that loop was
        # fixed nothing called the verb, and the one refusal class with no
        # in-band repair silently left the population. A counterexample that
        # depends on a defect to be measured is not a counterexample.
        # Ordered after `attach_known_kind_ledger_dead`, which is what queues
        # the intent this refuses to replay.
        HonestyCase(
            case="evidence_sync_ledger_dead",
            verb="evidence sync",
            ground_truth=REFUSED,
            immutable=False,
            run=lambda rec, root, _repo: calls.evidence_sync(
                rec, root, "bench-honesty-attach-known"
            ),
            precondition=lambda payload: bool(payload.get("pending")),
            detail=lambda payload: {
                "pending": payload.get("pending"),
                "owner": payload.get("owner"),
                "remedy": bool(payload.get("remedy")),
            },
        ),
        HonestyCase(
            case="gate_post_edit_invalid_manifest",
            verb="gate post-edit --full",
            ground_truth=REFUSED,
            immutable=False,
            run=lambda rec, root, _repo: calls.gate_post_edit(
                rec, root, "bench-honesty-invalid"
            ),
            precondition=lambda payload: bool(
                (payload.get("validation") or {}).get("errors")
            ),
            detail=lambda payload: {
                "errors": len((payload.get("validation") or {}).get("errors") or []),
            },
        ),
        HonestyCase(
            case="gate_task_done_unproven_subject",
            verb="gate task-done",
            ground_truth=REFUSED,
            immutable=False,
            run=lambda rec, root, _repo: calls.gate_task_done(
                rec, root, "bench-honesty-unproven", "task.one"
            ),
            precondition=lambda payload: bool(payload.get("missing_proof")),
            detail=lambda payload: {
                "missing_proof": len(payload.get("missing_proof") or []),
            },
        ),
        HonestyCase(
            case="evidence_status_satisfied",
            verb="evidence status",
            ground_truth=RECORDED,
            immutable=False,
            run=lambda rec, root, _repo: calls.evidence_status(
                rec, root, "bench-honesty-proven"
            ),
            precondition=_requirements_all_satisfied,
            detail=lambda payload: {
                "missing_requirements": len(payload.get("missing_requirements") or []),
            },
        ),
        HonestyCase(
            case="view_status_healthy_change",
            verb="view --format=status",
            ground_truth=RECORDED,
            immutable=False,
            run=lambda rec, root, _repo: calls.view_status(
                rec, root, "bench-honesty-proven"
            ),
        ),
    )


def seed(repo: FixtureRepo) -> None:
    """Write the honesty probe fixtures."""
    repo.write_change(blocked_manifest("bench-honesty-blocked"))
    repo.write_change(base_manifest("bench-honesty-unproven"))
    repo.write_change(proven_manifest("bench-honesty-proven"))
    repo.write_change(base_manifest("bench-honesty-attach"))
    repo.write_change(base_manifest("bench-honesty-attach-known"))
    invalid = base_manifest("bench-honesty-invalid")
    invalid["requirements"][0]["evidence_policy"] = {"required": []}
    repo.write_change(invalid)


def run(recorder: Recorder, root: Path, repo: FixtureRepo) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for case in _cases():
        result = case.run(recorder, root, repo)
        verdict = observed_verdict(result)
        if case.precondition is not None and not case.precondition(result.payload):
            raise IsolationRefusal(
                "pact bench probe state drifted: "
                f"{case.case} did not reach its intended fixture state",
                data={"case": case.case, "payload_keys": sorted(result.payload)},
                fix_hint="fix the fixture or the case; never score a drifted probe",
            )
        rows.append(
            {
                "case": case.case,
                "verb": case.verb,
                "immutable": case.immutable,
                "ground_truth": case.ground_truth,
                "observed_verdict": verdict,
                "observed_ok": result.ok,
                "ok_key_present": result.ok_present,
                "verdict_coherent": verdict_is_coherent(result),
                "honest": is_honest(result, case.ground_truth),
                "detail": case.detail(result.payload),
            }
        )
    immutable = [row for row in rows if row["immutable"]]
    derived = [row for row in rows if not row["immutable"]]
    return {
        "rows": sorted(rows, key=lambda row: row["case"]),
        "immutable_rows": len(immutable),
        "immutable_honest": sum(1 for row in immutable if row["honest"]),
        "immutable_honest_fraction": _fraction(immutable),
        "derived_rows": len(derived),
        "derived_honest": sum(1 for row in derived if row["honest"]),
        "derived_honest_fraction": _fraction(derived),
        "honest_fraction": _fraction(rows),
    }


def _fraction(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    return round(sum(1 for row in rows if row["honest"]) / len(rows), 4)
