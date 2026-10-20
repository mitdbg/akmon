"""PACT v4 review loop: derived state, review queue, and the decide verb.

Per `standardize-pact-review-interface`: the queue is defined, not designed —
queue(principal) = changes whose pending decision that principal may make,
plus repair rows for changes that fail to load. The scan never aborts on a
broken change. Decisions are ledger events (see decisions.py); enacting one
applies the FSM transition, updates the change.status projection, and moves
terminal changes to the archive path.
"""

from __future__ import annotations

import shlex
import time
from pathlib import Path
from typing import Any

import yaml
from yaml.nodes import MappingNode, ScalarNode, SequenceNode

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import decisions as ledger
from okg.substrate.pact import envelope
from okg.substrate.pact import fsm
from okg.substrate.pact.evaluation_context import PactEvaluationContext
from okg.substrate.pact.fsm import DecisionEvent, DerivedState
from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.store import (
    change_dir,
    intent_digest,
    load_manifest,
    load_manifest_bytes,
    load_manifest_file,
    manifest_file_write_lock,
    manifest_path,
    manifest_write_lock,
    save_manifest,
    state_digest,
)
from okg.substrate.pact.validation import validate_manifest


def archive_dir(repo_root: Path, change_id: str) -> Path:
    return repo_root / "pact" / "archive" / change_id


def infer_tier(
    manifest: PactManifest,
    *,
    repo_root: Path | None = None,
    context: PactEvaluationContext | None = None,
) -> int:
    """Return the lifecycle tier from the structured field or label.

    ``change.tier`` is authoritative for v4 manifests. ``tier-N`` labels are
    retained as a compatibility projection; when both are present they must
    agree. Old artifact versions retain tier-1 semantics. A v4 manifest may
    omit its tier only when the repository explicitly declares the migrated
    tier-less compatibility cohort.
    """
    declared = manifest.change.tier
    label_tiers: list[int] = []
    for label in manifest.change.labels:
        if label.startswith("tier-"):
            suffix = label.removeprefix("tier-")
            if not suffix.isdigit():
                raise SubstrateError(
                    "PACT tier label is not canonical",
                    data={"label": label, "expected": "tier-0 through tier-3"},
                )
            try:
                tier = int(suffix)
            except ValueError:
                raise SubstrateError(
                    "PACT tier label is not canonical",
                    data={"label": label, "expected": "tier-0 through tier-3"},
                ) from None
            if tier not in {0, 1, 2, 3}:
                raise SubstrateError(
                    "PACT tier label is outside the supported range",
                    data={"label": label, "supported_tiers": [0, 1, 2, 3]},
                )
            if suffix != str(tier):
                raise SubstrateError(
                    "PACT tier label is not canonical",
                    data={"label": label, "expected": f"tier-{tier}"},
                )
            label_tiers.append(tier)
    if declared is not None:
        if label_tiers and any(tier != declared for tier in label_tiers):
            raise SubstrateError(
                "PACT tier field conflicts with tier label",
                data={"tier": declared, "label_tiers": label_tiers},
            )
        return declared
    if label_tiers:
        if len(set(label_tiers)) > 1:
            raise SubstrateError(
                "PACT declares multiple conflicting tier labels",
                data={"label_tiers": label_tiers},
            )
        return label_tiers[0]
    target_version = manifest.target_pact_version or manifest.pact_version
    if target_version < 4:
        return 1
    if repo_root is not None:
        from okg.substrate.pact.project_settings import legacy_missing_tier_allowed

        if legacy_missing_tier_allowed(
            repo_root,
            manifest.change.id,
            context=context,
        ):
            return 1
    raise SubstrateError(
        "v4 PACT is missing an explicit lifecycle tier",
        data={
            "change": manifest.change.id,
            "required_field": "change.tier",
            "compatibility_setting": (
                "pact_defaults.legacy_missing_tier with an explicit id/snapshot cohort"
            ),
        },
        fix_hint="set change.tier to 1, 2, or 3 in pact.yaml",
    )


def manifest_condition(
    manifest: PactManifest,
    *,
    repo_root: Path | None = None,
    context: PactEvaluationContext | None = None,
    validation_report: Any | None = None,
) -> dict[str, Any]:
    """The FSM's manifest-side inputs, from validation + alignment."""
    report = validation_report
    if report is None:
        report = validate_manifest(
            manifest,
            repo_root=repo_root,
            context=context,
        )
    alignment = report.alignment
    req_align = alignment.get("requirements", {}) or {}
    task_align = alignment.get("tasks", {}) or {}
    evidence_satisfied = all(
        (entry or {}).get("satisfaction_status") == "satisfied"
        for entry in req_align.values()
    )
    # Per `right-size-pact-authoring` req.rightsize.consistency: satisfying
    # evidence that predates a later change to the declared scope no longer
    # counts — this is the input that feeds the FSM's awaiting_review ->
    # in_progress demotion guard.
    if evidence_satisfied and repo_root is not None:
        from okg.substrate.pact.scope_drift import staleness_report

        for requirement in manifest.requirements:
            staleness = staleness_report(
                Path(repo_root),
                manifest,
                requirement.id,
                context=context,
            )
            if staleness.get("revoked"):
                evidence_satisfied = False
                break
    tasks = manifest.tasks
    tasks_terminal = bool(tasks) and all(
        t.status == "done"
        or (task_align.get(t.id) or {}).get("satisfaction_status") == "satisfied"
        for t in tasks
    )
    return {
        "valid": report.ok,
        "tasks_terminal": tasks_terminal,
        "evidence_satisfied": evidence_satisfied,
        "tier": infer_tier(manifest, repo_root=repo_root, context=context),
        "tier_explicit": (
            manifest.change.tier is not None
            or any(label.startswith("tier-") for label in manifest.change.labels)
        ),
        "tier_source": (
            "structured"
            if manifest.change.tier is not None
            else (
                "label"
                if any(label.startswith("tier-") for label in manifest.change.labels)
                else "legacy_compatibility"
            )
        ),
    }


def derive_change_state(
    manifest: PactManifest,
    decision_events: list[DecisionEvent],
    *,
    repo_root: Path | None = None,
    context: PactEvaluationContext | None = None,
    validation_report: Any | None = None,
) -> DerivedState:
    cond = manifest_condition(
        manifest,
        repo_root=repo_root,
        context=context,
        validation_report=validation_report,
    )
    target_version = manifest.target_pact_version or manifest.pact_version
    current_digest = intent_digest(manifest)
    applicable_events = decision_events
    if target_version < 4:
        # Legacy rows were not digest-bound. Keep those rows compatible, but
        # do not let a newer, digest-bound decision advance a different
        # manifest snapshot merely because the manifest itself is legacy.
        applicable_events = [
            decision
            for decision in decision_events
            if decision.intent_digest is None
            or decision.intent_digest == current_digest
        ]
    return fsm.derive_state(
        valid=cond["valid"],
        tier=cond["tier"],
        tasks_terminal=cond["tasks_terminal"],
        evidence_satisfied=cond["evidence_satisfied"],
        decisions=applicable_events,
        # Digest binding is the v4 authority boundary. Legacy decision rows
        # were intentionally undigested and retain their historical behavior.
        intent_digest=(current_digest if target_version >= 4 else None),
    )


_DECISION_RANK = {"repair": 0, "archive": 1, "review": 2, "go-ahead": 3}
_DECISION_QUEUE_STATE = {
    "repair": "needs_repair",
    "review": "needs_review",
    "go-ahead": "needs_go_ahead",
    "archive": "needs_archive",
}
_DECISION_ACTION = {
    "repair": "rework",
    "review": "decision",
    "go-ahead": "decision",
    "archive": "archive",
}
_DECISION_PRIORITY_REASON = {
    "repair": (
        "Needs repair before review; agents cannot rely on this PACT until it "
        "loads cleanly."
    ),
    "archive": (
        "Already-terminal work still appears active; archive cleanup keeps the "
        "queue honest."
    ),
    "review": (
        "Completed work is waiting for an accept/revise decision; older waiting "
        "items sort before newer ones, then title only breaks exact ties."
    ),
    "go-ahead": (
        "This idea needs a start/no-start decision; older proposals sort before "
        "newer ones, then title only breaks exact ties."
    ),
}


def _decision_operator_status(
    *, state: str, decision: str, flags: list[str],
) -> dict[str, Any]:
    return {
        "surface": "review_queue",
        "authoritative": "decision_queue",
        "queue_state": _DECISION_QUEUE_STATE.get(decision, "needs_decision"),
        "decision": decision,
        "raw_state": state,
        "task_state": state,
        "flags": list(flags),
    }


def _intent_operator_status(
    *,
    state: str | None,
    freshness: dict[str, Any],
    divergence: str,
) -> dict[str, Any]:
    claim_expired = bool(freshness["expired"])
    return {
        "surface": "review_queue",
        "authoritative": "intent_claim",
        "queue_state": "stale_claim" if claim_expired else "in_flight",
        "raw_state": state,
        "task_state": state,
        "claim_freshness": freshness["status"],
        "claim_expired": claim_expired,
        "divergence": divergence,
    }


def _queue_priority_fields(row: dict[str, Any]) -> dict[str, Any]:
    decision = str(row.get("decision") or "review")
    priority = _DECISION_RANK.get(decision, 4)
    return {
        "priority": priority,
        "priority_reason": _DECISION_PRIORITY_REASON.get(
            decision,
            "This item needs human attention; title only breaks exact ties.",
        ),
        "tie_breaker": "title then change id",
    }


def _queue_sort_key(row: dict[str, Any]) -> tuple[int, float, str, str]:
    return (
        int(row.get("priority", 4)),
        -float(row.get("age_seconds") or 0.0),
        str(row.get("title") or "").casefold(),
        str(row.get("change") or "").casefold(),
    )


def build_queue(
    repo_root: Path,
    *,
    context: PactEvaluationContext | None = None,
) -> dict[str, Any]:
    """Repo-wide review queue. Never aborts on a broken change; ledger
    unavailability is a first-class warning, not a silent omission."""
    warnings: list[str] = []
    decisions_by_change: dict[str, list[DecisionEvent]] = {}
    intents: list[dict[str, Any]] = []
    if context is not None and context.ledger_authoritative:
        if context.ledger_available:
            decisions_by_change = {
                change_id: list(events)
                for change_id, events in (
                    context.decision_events_by_change.items()
                )
            }
            intents = [dict(intent) for intent in context.ledger_intents]
        else:
            warnings.append(
                "decision ledger unavailable "
                f"({context.ledger_reason or 'captured authority unavailable'}); "
                "derived states ignore recorded decisions and in-flight "
                "intents are invisible"
            )
    else:
        try:
            conn = ledger.require_ledger_connection(repo_root=repo_root)
            try:
                decisions_by_change = ledger.load_all_decisions(conn)
                intents = ledger.load_intents(conn)
            finally:
                conn.close()
        except SubstrateError as err:
            warnings.append(
                f"decision ledger unavailable ({err.data.get('reason')}); "
                "derived states ignore recorded decisions and in-flight "
                "intents are invisible"
            )

    attention = _attention_metrics(
        decisions_by_change,
        now_epoch=(
            (
                context.evaluation_time
                if context.evaluation_time is not None
                else 0.0
            )
            if context is not None and context.ledger_authoritative
            else None
        ),
    )
    rows: list[dict[str, Any]] = []
    captured_manifests: dict[str, tuple[Path, bytes, float | None]] = {}
    if context is not None and context.files is not None:
        for relpath, payload, mtime in context.iter_repo_files(
            repo_root,
            prefix="pact/changes",
            suffix="/pact.yaml",
        ):
            parts = Path(relpath).parts
            if (
                len(parts) == 4
                and parts[:2] == ("pact", "changes")
                and parts[3] == "pact.yaml"
            ):
                captured_manifests[parts[2]] = (
                    context.resolve_repo_path(repo_root, relpath),
                    payload,
                    mtime,
                )
        captured_mtimes = [
            mtime
            for _path, _payload, mtime in captured_manifests.values()
            if mtime is not None
        ]
        now = (
            context.evaluation_time
            if context.evaluation_time is not None
            else max(captured_mtimes, default=0.0)
        )
        manifest_entries = [
            (change_id, *captured_manifests[change_id])
            for change_id in sorted(captured_manifests)
        ]
    else:
        changes_root = repo_root / "pact" / "changes"
        now = time.time()
        manifest_entries = [
            (
                path.parent.name,
                path,
                None,
                path.stat().st_mtime,
            )
            for path in sorted(changes_root.glob("*/pact.yaml"))
        ]

    for change_id, path, captured_bytes, captured_mtime in manifest_entries:
        effective_mtime = (
            captured_mtime if captured_mtime is not None else now
        )
        age_seconds = max(0.0, now - effective_mtime)
        age_days = round(age_seconds / 86400, 1)
        try:
            manifest = (
                load_manifest_bytes(captured_bytes, source_path=path)
                if captured_bytes is not None
                else load_manifest(repo_root, change_id)
            )
        except SubstrateError as err:
            flags = ["invalid"]
            decision = "repair"
            rows.append({
                "change": change_id,
                "state": "draft",
                "decision": decision,
                "action": _DECISION_ACTION[decision],
                "age_seconds": age_seconds,
                "age_days": age_days,
                "flags": flags,
                "title": err.message[:80],
                "operator_status": _decision_operator_status(
                    state="draft",
                    decision=decision,
                    flags=flags,
                ),
            })
            continue
        try:
            derived = derive_change_state(
                manifest,
                decisions_by_change.get(change_id, []),
                repo_root=repo_root,
                context=context,
            )
        except SubstrateError as err:
            flags = ["invalid"]
            decision = "repair"
            rows.append({
                "change": change_id,
                "state": "draft",
                "decision": decision,
                "action": _DECISION_ACTION[decision],
                "age_seconds": age_seconds,
                "age_days": age_days,
                "flags": flags,
                "title": err.message[:80],
                "operator_status": _decision_operator_status(
                    state="draft",
                    decision=decision,
                    flags=flags,
                ),
            })
            continue
        if derived.pending_decision is None and derived.state not in fsm.terminal_states():
            continue
        flags: list[str] = []
        if derived.state in fsm.terminal_states():
            # terminal per ledger but still in pact/changes/: needs archive
            flags.append("unarchived-terminal")
        decision = derived.pending_decision or "archive"
        rows.append({
            "change": change_id,
            "state": derived.state,
            "decision": decision,
            "action": _DECISION_ACTION[decision],
            "age_seconds": age_seconds,
            "age_days": age_days,
            "flags": flags,
            "title": manifest.change.title[:80],
            "operator_status": _decision_operator_status(
                state=derived.state,
                decision=decision,
                flags=flags,
            ),
        })
    for row in rows:
        row.update(_queue_priority_fields(row))
    rows.sort(key=_queue_sort_key)

    action_counts = {
        action: sum(row.get("action") == action for row in rows)
        for action in ("decision", "rework", "archive")
    }

    # Tier E: in-flight intents from OTHER checkouts whose content
    # diverges from (or does not exist in) this tree. Same-digest
    # intents are merged reality and add nothing.
    here = (
        dict(context.worktree_identity)
        if context is not None and context.ledger_authoritative
        else ledger.current_worktree_identity() or {}
    )
    in_flight: list[dict[str, Any]] = []
    for intent in intents:
        if intent.get("worktree_suffix") == here.get("suffix"):
            continue
        local_digest = None
        try:
            if context is not None and context.files is not None:
                captured = captured_manifests.get(str(intent["change_id"]))
                local_manifest = (
                    load_manifest_bytes(captured[1], source_path=captured[0])
                    if captured is not None
                    else None
                )
            else:
                local_manifest = load_manifest(
                    repo_root,
                    intent["change_id"],
                )
            if local_manifest is not None:
                local_digest = intent_digest(local_manifest)
        except SubstrateError:
            local_digest = None
        if local_digest == intent.get("intent_digest"):
            continue
        if context is not None and context.ledger_authoritative:
            freshness = {
                "status": str(
                    intent.get("claim_freshness") or "expired"
                ),
                "expired": bool(
                    intent.get("claim_expired", True)
                ),
                "expiry_seconds": intent.get("claim_expiry_seconds"),
            }
        else:
            freshness = ledger.claim_freshness(
                intent.get("last_event_age_seconds")
            )
        divergence = "absent-here" if local_digest is None else "differs"
        in_flight.append({
            "change": intent["change_id"],
            "state": intent.get("derived_state"),
            "branch": intent.get("branch"),
            "worktree": intent.get("worktree_suffix"),
            "head_sha": intent.get("head_sha"),
            "session_id": intent.get("session_id"),
            "registered_at": intent.get("registered_at"),
            "last_event_at": intent.get("last_event_at"),
            "last_event_age_seconds": intent.get("last_event_age_seconds"),
            "claim_freshness": freshness["status"],
            "claim_expired": freshness["expired"],
            "claim_expiry_seconds": freshness["expiry_seconds"],
            "transcript_watermark_status": intent.get(
                "transcript_watermark_status"
            ),
            "transcript_watermark_age_seconds": intent.get(
                "transcript_watermark_age_seconds"
            ),
            "transcript_watermark_last_reconcile_at": intent.get(
                "transcript_watermark_last_reconcile_at"
            ),
            "transcript_watermarks": intent.get("transcript_watermarks", []),
            "divergence": divergence,
            "operator_status": _intent_operator_status(
                state=intent.get("derived_state"),
                freshness=freshness,
                divergence=divergence,
            ),
        })
    return envelope.recorded(
        text=(
            f"review queue: {action_counts['decision']} change(s) awaiting a "
            f"decision; {action_counts['rework']} need rework; "
            f"{action_counts['archive']} need archive"
        ),
        queue=rows,
        # Keep `count` as the all-attention-row count for existing consumers.
        # The decision headline has its own explicit count so terminal cleanup
        # remains visible without being described as verdict-ready work.
        count=len(rows),
        action_count=len(rows),
        decision_count=action_counts["decision"],
        rework_count=action_counts["rework"],
        archive_count=action_counts["archive"],
        action_counts=action_counts,
        in_flight=in_flight,
        attention=attention,
        warnings=warnings,
    )


def _attention_metrics(
    decisions_by_change: dict[str, list[DecisionEvent]],
    *,
    now_epoch: float | None = None,
) -> dict[str, Any]:
    """Ledger-derived attention metrics (req.budget.attention-metrics).

    Measured, not asserted: decisions in the last 7 days, batch share
    (verdicts landing within 60s of another by the same principal), and
    reopen rate per principal - including reopens of approve-per-
    recommendation decisions, the measure of whether recommendations
    deserve the trust they get. Brief-to-verdict latency is reported as a
    NAMED GAP: brief render events are not recorded yet, and a fake number
    is worse than an honest absence."""
    from datetime import datetime, timedelta, timezone

    events = [e for evs in decisions_by_change.values() for e in evs]
    parsed: list[tuple[datetime, DecisionEvent]] = []
    for e in events:
        if e.decided_at:
            try:
                parsed.append((datetime.fromisoformat(e.decided_at), e))
            except ValueError:
                continue
    parsed.sort(key=lambda p: p[0])
    now = (
        datetime.fromtimestamp(now_epoch, tz=timezone.utc)
        if now_epoch is not None
        else datetime.now(timezone.utc)
    )
    week = [p for p in parsed if now - p[0] <= timedelta(days=7)]
    batch = 0
    by_principal: dict[str, list[datetime]] = {}
    for ts, e in parsed:
        by_principal.setdefault(e.principal, []).append(ts)
    for times in by_principal.values():
        for i, ts in enumerate(times):
            if (i > 0 and (ts - times[i - 1]).total_seconds() <= 60) or (
                i + 1 < len(times)
                and (times[i + 1] - ts).total_seconds() <= 60
            ):
                batch += 1
    reopens_by_principal: dict[str, int] = {}
    decided_by_principal: dict[str, int] = {}
    for _, e in parsed:
        decided_by_principal[e.principal] = (
            decided_by_principal.get(e.principal, 0) + 1
        )
        if e.verdict == "reopen":
            reopens_by_principal[e.principal] = (
                reopens_by_principal.get(e.principal, 0) + 1
            )
    return {
        "decisions_total": len(parsed),
        "decisions_7d": len(week),
        "batch_share": round(batch / len(parsed), 2) if parsed else None,
        "reopen_rate": {
            principal: round(
                reopens_by_principal.get(principal, 0) / count, 3
            )
            for principal, count in sorted(decided_by_principal.items())
        },
        "brief_to_verdict_latency": (
            "unavailable: brief render events are not recorded "
            "(named gap, not a zero)"
        ),
    }


def open_task_ids(manifest: Any) -> list[str]:
    """Tasks that still hold the change out of `awaiting_review`."""
    return [task.id for task in manifest.tasks if task.status != "done"]


def _guard_refusal_with_enabling_path(
    repo_root: Path,
    manifest: Any,
    state: str,
    verdict: str,
    exc: SubstrateError,
    *,
    is_operator: bool,
    target: str | None,
) -> SubstrateError:
    """Re-raise an FSM guard violation carrying the repair for THAT guard.

    `legal_transition` raises for three different reasons, and only one of them
    is a state problem. Wrapping all three in an enabling path answered
    "supersede requires --target" with "reach awaiting_review first" — advice
    for a wall the caller has not hit. Each guard now gets its own repair:

    ``legal_transition``
        the audited case. The refusal named `legal_verdicts_here` and nothing
        else, so nine sessions answered "approve is not legal in in_progress"
        by trying another verdict instead of closing the open tasks. The
        commands are generated from the transition table, so a refusal can
        never recommend a verdict the next call rejects;
    ``target_exists``
        a MISSING ARGUMENT, not a missing state: the repair is the same
        invocation with ``--target <change-id>``;
    ``operator_only``
        no command an agent can run clears it — the repair is a named OWNER
        and an out-of-band remedy, and `next_commands` stays empty.
    """
    from okg.substrate.pact import repair as repair_mod

    guard = str((exc.data or {}).get("violated_guard") or "")
    prefix = repair_mod.repo_command(repo_root)
    change_id = manifest.change.id

    if guard == "target_exists":
        # The corrected invocation, with the one argument the guard named.
        corrected = (
            f"{prefix} pact decide {shlex.quote(change_id)} --verdict {verdict} "
            "--target <change-id> --json"
        )
        data = dict(exc.data or {})
        data["missing_argument"] = "--target"
        data["corrected_invocation"] = corrected
        # Not `next_commands`: `<change-id>` is a slot only the caller can
        # fill, so this is guidance until they do. Publishing it as a runnable
        # command would put an unrunnable string at the head of the list.
        return SubstrateError(
            (
                f"{exc.message} — supersede records which change replaces this "
                "one; rerun the same verb with --target <change-id>"
            ),
            data=data,
            fix_hint=corrected,
        )

    if guard == "operator_only":
        remedy = {
            "owner": repair_mod.OPERATOR_OWNER_DEFAULT,
            "remedy": (
                f"ask the repo operator to run `{prefix} pact decide "
                f"{shlex.quote(change_id)} --verdict {verdict}` as an operator "
                "principal (OKG_PRINCIPAL_KIND=operator); agent authority does "
                "not grant this verdict"
            ),
            "cause": "operator_only_verdict",
            "in_band_repair": False,
        }
        data = dict(exc.data or {})
        data["operator_action"] = remedy
        data["next_commands"] = []
        return SubstrateError(
            (
                f"{exc.message} — no transition sequence grants operator "
                f"authority; {remedy['owner']} must record it"
            ),
            data=data,
            fix_hint=remedy["remedy"],
        )

    path = fsm.enabling_path(state, verdict, is_operator=is_operator)
    commands = repair_mod.enabling_commands(
        path,
        change_id,
        prefix=prefix,
        open_tasks=open_task_ids(manifest),
        target=target,
    )
    data = dict(exc.data or {})
    data["enabling_path"] = path.to_dict()
    data["next_commands"] = commands
    if path.reachable and commands:
        message = (
            f"{exc.message} — reachable: {' then '.join(path.events)} "
            f"(-> {path.target_state}); run the commands in next_commands"
        )
        hint = commands[0]
    elif path.reachable:
        message = (
            f"{exc.message} — reachable via {' then '.join(path.events)}, which "
            "is not a caller action (the machine moves itself)"
        )
        hint = None
    else:
        message = (
            f"{exc.message} — no transition sequence reaches a state where "
            f"{verdict} is legal ({path.unreachable_reason})"
        )
        hint = None
    return SubstrateError(
        message,
        data=data,
        fix_hint=hint or exc.fix_hint,
    )


def decide_change(
    repo_root: Path,
    change_id: str,
    *,
    verdict: str,
    note: str | None = None,
    target: str | None = None,
    escalate_to: str | None = None,
    principal: str | None = None,
    explicit_dsn: str | None = None,
) -> dict[str, Any]:
    """Record and project one decision against one locked manifest snapshot."""
    active = manifest_path(repo_root, change_id)
    archived = archive_dir(repo_root, change_id) / "pact.yaml"
    if archived.is_file() and (verdict == "approve" or not active.is_file()):
        lock = manifest_file_write_lock(archived)
    else:
        # This preserves the canonical active-change not-found diagnostic.
        lock = manifest_write_lock(repo_root, change_id)
    with lock:
        return _decide_change_locked(
            repo_root,
            change_id,
            verdict=verdict,
            note=note,
            target=target,
            escalate_to=escalate_to,
            principal=principal,
            explicit_dsn=explicit_dsn,
        )


def _decide_change_locked(
    repo_root: Path,
    change_id: str,
    *,
    verdict: str,
    note: str | None = None,
    target: str | None = None,
    escalate_to: str | None = None,
    principal: str | None = None,
    explicit_dsn: str | None = None,
) -> dict[str, Any]:
    principal = principal or ledger.acting_principal()
    operator = ledger.is_operator(principal)
    if verdict == "escalate" and not escalate_to:
        raise SubstrateError(
            "escalate requires --to <principal>",
            data={"violated_guard": "escalation_target"},
        )

    # reopen targets an archived change: restore the dir before deriving.
    restored_from_archive = False
    if verdict == "reopen" and not manifest_path(repo_root, change_id).is_file():
        src = archive_dir(repo_root, change_id)
        if not (src / "pact.yaml").is_file():
            raise SubstrateError(
                f"reopen: change not found in archive: {change_id}",
                data={"archive_path": str(src)},
            )
        dest = change_dir(repo_root, change_id)
        dest.parent.mkdir(parents=True, exist_ok=True)
        src.rename(dest)
        if not _rewrite_pact_location_paths(
            repo_root,
            change_id,
            tree="changes",
            from_tree="archive",
            to_tree="changes",
        ):
            _rollback_active_move_to_archive(repo_root, change_id)
            raise _location_rewrite_error(change_id, "archive", "changes")
        restored_from_archive = True

    active_manifest = manifest_path(repo_root, change_id)
    archived_manifest = archive_dir(repo_root, change_id) / "pact.yaml"
    loaded_from_archive = False
    if verdict == "approve" and archived_manifest.is_file():
        manifest = load_manifest_file(archived_manifest)
        loaded_from_archive = True
    elif active_manifest.is_file():
        manifest = load_manifest(repo_root, change_id)
    elif archived_manifest.is_file():
        manifest = load_manifest_file(archived_manifest)
        loaded_from_archive = True
    else:
        manifest = load_manifest(repo_root, change_id)
    digest = intent_digest(manifest)

    conn = ledger.require_ledger_connection(
        explicit_dsn=explicit_dsn,
        repo_root=repo_root,
    )
    try:
        # Filesystem locks are checkout-local, while the decision ledger is
        # shared by every worktree. Hold one session-level Postgres advisory
        # lock for this change through history read and decision/event writes.
        # Connection close releases it even though the helpers commit.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
                (f"pact-decision:{change_id}",),
            )
        history = ledger.load_decisions(conn, change_id)

        # Direct pact.yaml edits intentionally do not acquire the advisory
        # file lock. Re-read after acquiring the shared ledger lock and refuse
        # before writing a decision for a snapshot that has already changed.
        current_manifest = (
            load_manifest(repo_root, change_id)
            if active_manifest.is_file()
            else load_manifest_file(archived_manifest)
        )
        current_digest = intent_digest(current_manifest)
        if current_digest != digest:
            raise SubstrateError(
                "PACT intent changed while the decision was waiting to record",
                data={
                    "change": change_id,
                    "expected_intent_digest": digest,
                    "current_intent_digest": current_digest,
                },
                fix_hint=f"rerun `okg pact decide {change_id} --verdict {verdict} --json`",
            )
        target_version = manifest.target_pact_version or manifest.pact_version
        approved_digests = {
            decision.intent_digest
            for decision in history
            if decision.verdict == "approve"
            and decision.to_state in (None, "retired")
        }
        refresh_archived_approval = (
            verdict == "approve"
            and loaded_from_archive
            and digest not in approved_digests
            and target_version < 4
        )
        restored_changed_intent = False
        if (
            verdict == "approve"
            and loaded_from_archive
            and digest not in approved_digests
            and target_version >= 4
        ):
            # An archived v4 file whose intent digest changed is new work, not
            # a polish-only refresh of the old terminal decision. Restore it
            # to the active tree and revoke task proof exactly like EditIntent
            # in PactAgentLifecycle.tla. This approval can then serve only the
            # proposal boundary for tier 2+, never silently re-retire it.
            src = archive_dir(repo_root, change_id)
            dest = change_dir(repo_root, change_id)
            dest.parent.mkdir(parents=True, exist_ok=True)
            src.rename(dest)
            loaded_from_archive = False
            restored_from_archive = True
            restored_changed_intent = True
            if not _rewrite_pact_location_paths(
                repo_root,
                change_id,
                tree="changes",
                from_tree="archive",
                to_tree="changes",
            ):
                _rollback_active_move_to_archive(repo_root, change_id)
                raise _location_rewrite_error(change_id, "archive", "changes")
            active_path = manifest_path(repo_root, change_id)
            if not _reset_done_task_proof_in_place(active_path, digest):
                _rollback_active_move_to_archive(repo_root, change_id)
                raise SubstrateError(
                    "PACT intent changed while restoring archived task proof",
                    data={"change": change_id, "intent_digest": digest},
                    fix_hint=f"rerun `okg pact decide {change_id} --verdict approve --json`",
                )
            manifest = load_manifest(repo_root, change_id)
            if intent_digest(manifest) != digest:
                _rollback_active_move_to_archive(repo_root, change_id)
                raise SubstrateError(
                    "PACT intent changed while restoring archived task proof",
                    data={"change": change_id, "intent_digest": digest},
                    fix_hint=f"rerun `okg pact decide {change_id} --verdict approve --json`",
                )
            active_manifest = manifest_path(repo_root, change_id)
            archived_manifest = archive_dir(repo_root, change_id) / "pact.yaml"
        derived = derive_change_state(manifest, history, repo_root=repo_root)
        recovery_decision = next(
            (
                decision
                for decision in reversed(history)
                if decision.verdict == verdict
                and decision.intent_digest == digest
                and decision.to_state in fsm.terminal_states()
            ),
            None,
        )
        recover_terminal_projection = (
            (active_manifest.is_file() or archived_manifest.is_file())
            and derived.state in fsm.terminal_states()
            and recovery_decision is not None
            and recovery_decision.to_state == derived.state
        )
        # A post-archive manifest polish needs a fresh digest approval, not a
        # lifecycle transition or archive move.
        if refresh_archived_approval:
            next_state = None
        elif recover_terminal_projection:
            next_state = derived.state
        else:
            try:
                next_state = fsm.legal_transition(
                    derived.state, verdict, is_operator=operator, target=target,
                )
            except SubstrateError as exc:
                raise _guard_refusal_with_enabling_path(
                    repo_root, manifest, derived.state, verdict, exc,
                    is_operator=operator, target=target,
                ) from exc
        decision_from_state = (
            str(recovery_decision.from_state or derived.state)
            if recover_terminal_projection and recovery_decision is not None
            else (
            "retired"
            if verdict == "approve" and archived_manifest.is_file()
            else derived.state
            )
        )
        if recover_terminal_projection:
            decision_rows = [
                row
                for row in ledger.load_decision_rows(conn)
                if row.get("change_id") == change_id
                and row.get("verdict") == verdict
                and row.get("intent_digest") == digest
                and row.get("to_state") == derived.state
            ]
            if not decision_rows:
                raise SubstrateError(
                    "terminal PACT decision row could not be recovered",
                    data={"change": change_id, "intent_digest": digest},
                )
            recorded = {
                "decision_id": decision_rows[-1]["decision_id"],
                "recovered_existing_decision": True,
            }
            coordination_event = None
        else:
            recorded = ledger.record_decision(
                conn,
                change_id=change_id,
                verdict=verdict,
                principal=principal,
                authority="operator" if operator else "agent",
                note=note,
                target=target or escalate_to,
                intent_digest=digest,
                from_state=decision_from_state,
                to_state=next_state,
            )
            # A final v4 verdict is recorded after the archive projection, so
            # its state digest binds the actual terminal manifest rather than
            # the pre-move snapshot. Non-terminal and legacy events retain
            # the existing single-transaction behavior.
            if target_version >= 4 and next_state in fsm.terminal_states():
                coordination_event = None
            else:
                coordination_event = ledger.record_coordination_event(
                    conn,
                    event_type="verdict",
                    change_id=change_id,
                    intent_digest=digest,
                    principal=principal,
                    payload={
                        "command": "decide",
                        "verdict": verdict,
                        "from_state": decision_from_state,
                        "to_state": next_state,
                        "decision_id": recorded["decision_id"],
                        "note": note,
                        "target": target or escalate_to,
                    },
                )
    finally:
        conn.close()

    projection_skipped = False
    effective_state = next_state or derived.state
    post_decision_manifest = (
        load_manifest(repo_root, change_id)
        if active_manifest.is_file()
        else load_manifest_file(archived_manifest)
    )
    post_decision_digest = intent_digest(post_decision_manifest)
    if post_decision_digest != digest:
        # The decision is durably bound to the snapshot it reviewed, so it is
        # safe but no longer advances the newly authored intent. Never
        # overwrite or archive that newer file as if the old digest won.
        projection_skipped = True
        # ``history`` predates the decision recorded above. Keep it that way:
        # the new row is bound to ``digest`` and must not advance the newer
        # manifest even for legacy projections that accept undigested history.
        effective_state = derive_change_state(
            post_decision_manifest,
            history,
            repo_root=repo_root,
        ).state

    archived = False
    if recover_terminal_projection and loaded_from_archive:
        if not _rewrite_pact_location_paths(
            repo_root,
            change_id,
            tree="archive",
            from_tree="changes",
            to_tree="archive",
        ):
            raise _location_rewrite_error(change_id, "changes", "archive")
        recovered_digest = intent_digest(load_manifest_file(archived_manifest))
        if recovered_digest != digest:
            raise SubstrateError(
                "PACT intent changed while recovering terminal archive metadata",
                data={
                    "change": change_id,
                    "expected_intent_digest": digest,
                    "current_intent_digest": recovered_digest,
                },
                fix_hint=f"rerun `okg pact decide {change_id} --verdict {verdict} --json`",
            )
        archived = True
    elif next_state is not None and not projection_skipped:
        # Re-read at each file projection. Direct pact.yaml editing is
        # canonical and does not honor our advisory lock, so projecting a
        # stale in-memory snapshot would overwrite a newer intent.
        projection_manifest = load_manifest(repo_root, change_id)
        projection_digest = intent_digest(projection_manifest)
        if projection_digest != digest:
            projection_skipped = True
            post_decision_manifest = projection_manifest
            post_decision_digest = projection_digest
            effective_state = derive_change_state(
                projection_manifest,
                history,
                repo_root=repo_root,
            ).state
        elif target_version < 4:
            projection_manifest.change.status = fsm.STATUS_PROJECTION.get(
                next_state, projection_manifest.change.status
            )
            save_manifest(repo_root, projection_manifest)
        elif verdict == "revise" and next_state == "in_progress":
            # The formal Revise action invalidates task proof. Project that
            # semantic effect, not lifecycle status, into pact.yaml so the
            # normative derivation cannot immediately bounce back to review.
            projected = _reset_done_task_proof_in_place(active_manifest, digest)
            post_projection_manifest = load_manifest(repo_root, change_id)
            post_projection_digest = intent_digest(post_projection_manifest)
            if post_projection_digest != digest:
                projection_skipped = True
                post_decision_manifest = post_projection_manifest
                post_decision_digest = post_projection_digest
                effective_state = derive_change_state(
                    post_projection_manifest,
                    history,
                    repo_root=repo_root,
                ).state
            elif not projected:
                raise SubstrateError(
                    "task proof could not be reset safely after revise",
                    data={"change": change_id, "intent_digest": digest},
                    fix_hint=f"rerun `okg pact decide {change_id} --verdict revise --json`",
                )

    if (
        next_state is not None
        and next_state in fsm.terminal_states()
        and not projection_skipped
        and not archived
    ):
        dest = archive_dir(repo_root, change_id)
        dest.parent.mkdir(parents=True, exist_ok=True)
        src = change_dir(repo_root, change_id)
        src.rename(dest)
        paths_rewritten = _rewrite_pact_location_paths(
            repo_root,
            change_id,
            tree="archive",
            from_tree="changes",
            to_tree="archive",
        )
        archived_manifest_after_move = load_manifest_file(dest / "pact.yaml")
        archived_digest = intent_digest(archived_manifest_after_move)
        if paths_rewritten and archived_digest == digest:
            archived = True
        else:
            # A direct editor wrote after the pre-projection digest check.
            # Move that newer intent back to the active tree; the recorded
            # decision remains safely bound to the older digest and therefore
            # cannot advance this file.
            reversed_paths = _rewrite_pact_location_paths(
                repo_root,
                change_id,
                tree="archive",
                from_tree="archive",
                to_tree="changes",
            )
            dest.rename(src)
            if not reversed_paths:
                raise _location_rewrite_error(change_id, "archive", "changes")
            projection_skipped = True
            post_decision_manifest = load_manifest(repo_root, change_id)
            post_decision_digest = intent_digest(post_decision_manifest)
            effective_state = derive_change_state(
                post_decision_manifest,
                history,
                repo_root=repo_root,
            ).state

    if (
        target_version >= 4
        and next_state in fsm.terminal_states()
        and archived
        and not projection_skipped
    ):
        terminal_manifest = load_manifest_file(archived_manifest)
        coordination_event = _record_final_verdict_event(
            repo_root,
            change_id=change_id,
            verdict=verdict,
            from_state=decision_from_state,
            to_state=next_state,
            decision_id=int(recorded["decision_id"]),
            note=note,
            principal=principal,
            expected_intent_digest=digest,
            manifest=terminal_manifest,
            explicit_dsn=explicit_dsn,
        )

    transition = (
        f"({decision_from_state} -> {next_state})"
        if next_state is not None
        else f"({decision_from_state}, recorded only)"
    )
    ledger_target = ledger.resolve_ledger_dsn(
        explicit_dsn=explicit_dsn,
        repo_root=repo_root,
    ).get(
        "target"
    ) or {}
    target_suffix = (
        f" [db={ledger_target['database']}@"
        f"{ledger_target.get('host')}:{ledger_target.get('port')}]"
        if ledger_target.get("database")
        else " [db=unknown]"
    )
    echo = (
        f"recorded: {change_id} {verdict} {transition} by {principal}"
        f"{target_suffix}"
    )
    if projection_skipped:
        echo += " [projection skipped: intent changed during decision]"
    successor_commands = _decision_successor_commands(
        repo_root,
        change_id,
        from_state=decision_from_state,
        to_state=(effective_state if projection_skipped else next_state),
        verdict=verdict,
    )
    if projection_skipped:
        from okg.substrate.pact import repair

        successor_commands = [
            f"{repair.repo_command(repo_root)} pact gate post-edit "
            f"{shlex.quote(change_id)} --json"
        ]
    intent_edit_required = (
        verdict == "revise"
        and next_state == "draft"
        and not projection_skipped
    )
    return envelope.recorded(
        text=echo,
        blocked_by=["intent_edit_required"] if intent_edit_required else [],
        state=effective_state,
        from_state=decision_from_state,
        to_state=effective_state if projection_skipped else next_state,
        next_commands=successor_commands,
        next_action={
            "command": successor_commands[0] if successor_commands else None,
            "owner": "operator" if next_state in {"proposed", "awaiting_review"} else "agent",
            "why": (
                "edit pact.yaml to address the revision note; rerun only after "
                "the intent digest changes"
                if intent_edit_required
                else "continue the derived lifecycle"
            ),
            "wait_condition": (
                "intent_digest_changes_after_direct_pact_edit"
                if intent_edit_required
                else None
            ),
        },
        ledger_target=ledger_target,
        recorded={
            "change": change_id,
            "verdict": verdict,
            "from_state": decision_from_state,
            "to_state": next_state,
            "principal": principal,
            "note": note,
            "target": target or escalate_to,
            "intent_digest": digest,
            **recorded,
        },
        coordination_event=coordination_event,
        archived=archived,
        restored_from_archive=restored_from_archive,
        restored_changed_intent=restored_changed_intent,
        recovered_projection=recover_terminal_projection,
        projection_skipped=projection_skipped,
        reviewed_intent_digest=digest,
        current_intent_digest=post_decision_digest,
        echo=echo,
    )


def _record_final_verdict_event(
    repo_root: Path,
    *,
    change_id: str,
    verdict: str,
    from_state: str,
    to_state: str,
    decision_id: int,
    note: str | None,
    principal: str,
    expected_intent_digest: str,
    manifest: PactManifest,
    explicit_dsn: str | None,
) -> dict[str, Any]:
    """Record or recover the one digest-bound terminal verdict event."""
    from okg.substrate.pact.proof_identity import code_proof_receipt_digests

    current_intent_digest = intent_digest(manifest)
    if current_intent_digest != expected_intent_digest:
        raise SubstrateError(
            "terminal PACT intent changed before its verdict event was recorded",
            data={
                "change": change_id,
                "expected_intent_digest": expected_intent_digest,
                "current_intent_digest": current_intent_digest,
            },
        )
    payload = {
        "command": "decide",
        "verdict": verdict,
        "from_state": from_state,
        "to_state": to_state,
        "decision_id": decision_id,
        "note": note,
        "target": change_id,
        "state_digest": state_digest(manifest),
        "intent_digest": current_intent_digest,
        "code_proof_receipt_digests": code_proof_receipt_digests(manifest),
    }
    conn = ledger.require_ledger_connection(
        explicit_dsn=explicit_dsn,
        repo_root=repo_root,
    )
    try:
        existing = [
            event
            for event in ledger.load_coordination_events(conn, change_id)
            if event.get("event_type") == "verdict"
            and (event.get("payload") or {}).get("decision_id") == decision_id
        ]
        if existing:
            event = existing[-1]
            if (
                event.get("change_id") != change_id
                or event.get("principal") != principal
                or event.get("intent_digest") != current_intent_digest
                or event.get("payload") != payload
            ):
                raise SubstrateError(
                    "terminal PACT verdict event does not match its decision state",
                    data={
                        "change": change_id,
                        "decision_id": decision_id,
                        "expected_payload": payload,
                        "actual_event": event,
                    },
                )
            return {
                "status": "recorded",
                "recovered_existing_event": True,
                **event,
            }
        return ledger.record_coordination_event(
            conn,
            event_type="verdict",
            change_id=change_id,
            intent_digest=current_intent_digest,
            principal=principal,
            payload=payload,
        )
    finally:
        conn.close()


def _decision_successor_commands(
    repo_root: Path,
    change_id: str,
    *,
    from_state: str,
    to_state: str | None,
    verdict: str,
) -> list[str]:
    """Return one legal command after a recorded lifecycle decision."""
    from okg.substrate.pact import repair

    prefix = repair.repo_command(repo_root)
    quoted = shlex.quote(change_id)
    if to_state == "draft":
        return [f"{prefix} pact gate post-edit {quoted} --json"]
    if to_state in {"in_progress", "awaiting_review"}:
        return [f"{prefix} pact view {quoted} --format=bootstrap --json"]
    if to_state == "retired":
        return [f"{prefix} pact gate pre-merge {quoted} --json"]
    if to_state in {"rejected", "superseded"}:
        return [f"{prefix} pact view {quoted} --format=status --json"]
    if verdict in {"defer", "escalate"}:
        return [f"{prefix} pact view --format=review-queue --json"]
    if from_state == "retired":
        return [f"{prefix} pact gate pre-merge {quoted} --json"]
    if from_state in {"rejected", "superseded"}:
        return [f"{prefix} pact view {quoted} --format=status --json"]
    return []


def _rewrite_pact_location_paths(
    repo_root: Path,
    change_id: str,
    *,
    tree: str,
    from_tree: str,
    to_tree: str,
) -> bool:
    """Keep proof metadata portable without reserializing canonical intent.

    ``changes`` and ``archive`` deliberately have equal width. Rewriting only
    the matched location tokens in place means a direct pact.yaml edit cannot
    be overwritten by a stale whole-manifest snapshot during an archive move.
    A concurrent atomic editor wins by replacing the inode; the caller then
    re-reads the intent digest and restores the PACT to the active tree.
    """
    directory = repo_root / "pact" / tree / change_id
    path = directory / "pact.yaml"
    source_prefix = f"pact/{from_tree}/{change_id}/"
    target_prefix = f"pact/{to_tree}/{change_id}/"
    if len(source_prefix.encode("utf-8")) != len(target_prefix.encode("utf-8")):
        raise AssertionError("PACT location trees must have equal-width names")
    if not _rewrite_location_file_in_place(
        path,
        source_prefix,
        target_prefix,
        yaml_top_level_key="evidence",
    ):
        return False

    # Formal reports carry their own command/artifact locations. Rewrite only
    # structured metadata, never TLC stdout (whose digest is evidence-bound).
    reports = directory / "reports"
    if not reports.is_dir():
        return True
    for report_path in reports.rglob("*.yaml"):
        if not _rewrite_location_file_in_place(
            report_path,
            source_prefix,
            target_prefix,
        ):
            return False
    return True


def _rewrite_location_file_in_place(
    path: Path,
    source_prefix: str,
    target_prefix: str,
    *,
    yaml_top_level_key: str | None = None,
) -> bool:
    """Replace equal-width path tokens without a stale whole-file save.

    For pact.yaml, marks from PyYAML's composed node tree bound writes to the
    top-level evidence value. Intent-bearing requirements, tasks, test plans,
    and formal bindings are never rewritten merely because they cite a path.
    """
    source = source_prefix.encode("utf-8")
    target = target_prefix.encode("utf-8")
    if len(source) != len(target):
        raise AssertionError("location token rewrite must preserve byte width")
    try:
        with path.open("r+b") as stream:
            payload = stream.read()
            start = 0
            end = len(payload)
            if yaml_top_level_key is not None:
                try:
                    text = payload.decode("utf-8")
                    document = yaml.compose(text)
                except (UnicodeDecodeError, yaml.YAMLError):
                    return False
                if not isinstance(document, MappingNode):
                    return False
                value_node = next(
                    (
                        value
                        for key, value in document.value
                        if isinstance(key, ScalarNode)
                        and key.value == yaml_top_level_key
                    ),
                    None,
                )
                if value_node is None:
                    return True
                start = len(text[: value_node.start_mark.index].encode("utf-8"))
                end = len(text[: value_node.end_mark.index].encode("utf-8"))
            offsets: list[int] = []
            offset = payload.find(source, start, end)
            while offset >= 0:
                offsets.append(offset)
                offset = payload.find(source, offset + len(source), end)
            for offset in offsets:
                stream.seek(offset)
                if stream.read(len(source)) != source:
                    return False
                stream.seek(offset)
                stream.write(target)
            stream.flush()
        return True
    except OSError:
        return False


def _location_rewrite_error(
    change_id: str,
    from_tree: str,
    to_tree: str,
) -> SubstrateError:
    return SubstrateError(
        "PACT location metadata could not be rewritten safely",
        data={
            "change": change_id,
            "from_tree": from_tree,
            "to_tree": to_tree,
        },
        fix_hint=f"rerun the decision for {change_id}; location rewrites are idempotent",
    )


def _reset_done_task_proof_in_place(
    path: Path,
    expected_intent_digest: str,
) -> bool:
    """Reset only task status/evidence bytes, preserving concurrent intent.

    The task status token changes ``done``→``todo`` at equal width. Evidence
    sequence bytes become an empty flow sequence while all existing newline
    positions are retained. No requirement, title, verification, test-plan,
    or formal-binding byte is written from a stale model snapshot.
    """
    try:
        with path.open("r+b") as stream:
            payload = stream.read()
            manifest = load_manifest_bytes(payload, source_path=path)
            if intent_digest(manifest) != expected_intent_digest:
                return False
            text = payload.decode("utf-8")
            document = yaml.compose(text)
            if not isinstance(document, MappingNode):
                return False
            tasks_node = next(
                (
                    value
                    for key, value in document.value
                    if isinstance(key, ScalarNode) and key.value == "tasks"
                ),
                None,
            )
            if not isinstance(tasks_node, SequenceNode):
                return False
            operations: list[tuple[int, bytes, bytes]] = []
            for task_node in tasks_node.value:
                if not isinstance(task_node, MappingNode):
                    return False
                fields = {
                    key.value: (key, value)
                    for key, value in task_node.value
                    if isinstance(key, ScalarNode)
                }
                status_field = fields.get("status")
                status_node = status_field[1] if status_field is not None else None
                if not isinstance(status_node, ScalarNode) or status_node.value != "done":
                    continue
                status_start = len(
                    text[: status_node.start_mark.index].encode("utf-8")
                )
                status_end = len(text[: status_node.end_mark.index].encode("utf-8"))
                status_original = payload[status_start:status_end]
                status_replacements = {
                    b"done": b"todo",
                    b"'done'": b"'todo'",
                    b'"done"': b'"todo"',
                }
                status_replacement = status_replacements.get(status_original)
                if status_replacement is None:
                    return False
                operations.append(
                    (status_start, status_original, status_replacement)
                )

                evidence_field = fields.get("evidence")
                if evidence_field is None:
                    continue
                evidence_key, evidence_node = evidence_field
                if not isinstance(evidence_node, SequenceNode) or not evidence_node.value:
                    continue
                evidence_start = len(text[: evidence_node.start_mark.index].encode("utf-8"))
                evidence_end = len(
                    text[: evidence_node.end_mark.index].encode("utf-8")
                )
                block_sequence = evidence_node.start_mark.line > evidence_key.end_mark.line
                if block_sequence:
                    key_end = len(
                        text[: evidence_key.end_mark.index].encode("utf-8")
                    )
                    colon = payload.find(b":", key_end, evidence_start)
                    if colon < 0:
                        return False
                    evidence_start = colon + 1
                evidence_original = payload[evidence_start:evidence_end]
                minimum = 3 if block_sequence else 2
                if len(evidence_original) < minimum:
                    return False
                evidence_empty = bytearray(
                    byte if byte in {10, 13} else 32 for byte in evidence_original
                )
                if block_sequence:
                    # Replace the newline after ``evidence:`` with a space and
                    # use the vacated indentation for an inline empty list.
                    evidence_empty[0:3] = b" []"
                else:
                    evidence_empty[0:2] = b"[]"
                operations.append(
                    (evidence_start, evidence_original, bytes(evidence_empty))
                )

            for offset, original, _replacement in operations:
                stream.seek(offset)
                if stream.read(len(original)) != original:
                    return False
            for offset, _original, replacement in operations:
                stream.seek(offset)
                stream.write(replacement)
            stream.flush()
        return True
    except (OSError, UnicodeDecodeError, yaml.YAMLError, SubstrateError):
        return False


def _rollback_active_move_to_archive(repo_root: Path, change_id: str) -> None:
    """Restore placement after a pre-ledger archive→active rewrite failure.

    The reverse rewrite is best-effort and idempotent. Even if an individual
    token remains stale, putting the directory back in archive guarantees the
    documented retry re-enters the same restore branch and completes it.
    """
    active = change_dir(repo_root, change_id)
    archived = archive_dir(repo_root, change_id)
    _rewrite_pact_location_paths(
        repo_root,
        change_id,
        tree="changes",
        from_tree="changes",
        to_tree="archive",
    )
    active.rename(archived)
