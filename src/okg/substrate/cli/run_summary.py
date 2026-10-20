"""The short default report for ``okg run --once`` (issue #2282).

``okg run --once`` used to print the publisher's whole cycle summary through
the generic ``key: value`` renderer: about 150 lines for a cycle that changed
nothing and about 200 for one that published two records, most of them stage
timings and configuration receipts. Nothing in that dump told the operator the
three things they ran the command for -- did it publish, what changed, and is
anything wrong -- without reading all of it.

This module turns the cycle summary into:

* ``build_run_summary`` -- a small, JSON-safe block (status, graph version,
  record and relationship deltas, graph totals, per-source status when the
  cycle reports any, LLM-invariant counts when they ran, and every warning
  found in the summary). ``okg run --once --json`` carries it as the
  ``run_summary`` key next to the unchanged cycle summary.
* ``render_run_summary`` -- the few text lines printed by default. The full
  cycle summary is printed only with ``--verbose``.
* ``publish_outcome`` and ``EXIT_CODES`` -- the one verdict of a cycle
  summary (published, no change, blocked, failed, or unknown) and the exit
  code each verdict gives ``okg run --once`` (issue #2346). The DBOS path
  in ``cli/main.py`` reads the same status sets and the same blocked exit
  code, so both paths exit alike for the same cycle summary.

Warnings are never dropped for brevity. Every known problem field of the
cycle summary is checked explicitly. A generic walk then reports every other
nested mapping whose ``status``/``result``/``outcome`` is a problem value or
a value ``STATUS_CLASSES`` does not know (fail closed), that carries an
error, a failure reason, or a non-zero error, failure, violation or degraded
count, so a new publisher stage that reports trouble still reaches the
default output.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from okg.substrate.db.readiness import AD_HOC_DEPLOYMENT_NAME, migrate_command
from okg.substrate import publish_outcome as _publish_outcome

SUMMARY_SCHEMA = "okg.run_summary.v1"

OK = "ok"
PROBLEM = "problem"

# Every status, result or outcome value the publisher and the stages it runs
# can put in a cycle summary, classified as ok or problem. The values were
# collected from the source, not written from memory:
# ``tests/substrate/cli/test_run_quiet_summary.py`` reads the modules listed
# in ``STATUS_SOURCE_MODULES`` and fails when one of them can emit a value
# that is missing here, so a new status forces a decision in this table.
#
# A value that is not in this table is reported as a problem (fail closed):
# a status this summary does not know is never silently treated as fine.
# A value of the form ``kind:detail`` (``failed:incomplete_inputs``,
# ``withheld:unknown_declared_source:x``) is classified by ``kind``.
STATUS_CLASSES: dict[str, str] = {
    # admission.py, publisher.py: admission and derived-verdict decisions.
    "accepted": OK,
    "pending": OK,  # waiting for a later admission decision
    "published": OK,
    "rejected": PROBLEM,
    "quarantine": PROBLEM,
    "quarantined": PROBLEM,
    # cli/main.py cycle statuses and publisher.py outcomes.
    "blocked": PROBLEM,
    "gate_blocked": PROBLEM,
    "fail": PROBLEM,
    "failed": PROBLEM,
    "error": PROBLEM,
    "errored": PROBLEM,
    # consistency/gates.py ConsistencyGateResult; sql_gate.py post-publish.
    "evaluated": OK,
    "blocked_error": PROBLEM,
    "blocked_incomplete": PROBLEM,
    "blocked_timeout": PROBLEM,
    # consistency/transform_gate.py derived-node cleanup check.
    "clean": OK,
    # consistency/observations.py coverage and finding states.
    "absent": OK,
    "candidate": OK,
    "excluded": OK,
    "open": PROBLEM,  # an open consistency finding
    "not_evaluated": PROBLEM,
    "running": PROBLEM,  # not finished when the cycle reported
    # consistency/batch_schedule.py, batch_measurements.py.
    "failure": PROBLEM,
    "unsupported_outcome": PROBLEM,
    # derived_node_liveness.py derived-node support checks.
    "not_managed": OK,
    "legacy_mismatch": PROBLEM,
    "legacy_policy_missing": PROBLEM,
    "malformed": PROBLEM,
    "missing": PROBLEM,
    "producer_mismatch": PROBLEM,
    "unknown_policy": PROBLEM,
    "unknown_schema": PROBLEM,
    "unresolved_member": PROBLEM,
    # publisher.py view refresh and attribute effective view.
    "materialized": OK,
    "materialized_with_degradation": PROBLEM,
    "materialized_with_failures": PROBLEM,
    "not_materialized": OK,  # the view was already materialized
    "not_run": OK,
    "deferred": OK,  # intermediate publish; the final one refreshes
    "coalesced": OK,  # refresh interval or delay policy
    "disabled": OK,  # no view materializers configured
    "skipped": OK,
    "skipped_reused_previous": OK,
    "reused": OK,
    # publisher.py pre-lock enrich rechecks (#2941 fix 1): "changed" means a
    # pre-lock result was recomputed under the lock, which is correct
    # behaviour; the fallback and its reason are reported beside it.
    "changed": OK,
    "unchanged": OK,
    # publisher/native_view_stage.py, views/base.py per-view outcomes.
    "success": OK,
    "degraded": PROBLEM,
    # publisher.py enricher runs and enrichers/*.py progress.
    "never": OK,  # the enricher has not run before
    "partial": PROBLEM,  # did not process all inputs; retries next cycle
    "withheld": PROBLEM,  # input source unregistered or held for admission
    "unknown": PROBLEM,  # outcome never determined
    "completed": OK,
    "resolved": OK,
    "superseded": OK,
    # publisher.py maintained derivations; derived_artifacts/*.py.
    "maintained": OK,
    "unavailable": PROBLEM,  # served the previous generation's rows
    "available": OK,
    "lineage_available": OK,
    "lineage_unavailable": PROBLEM,
    "carried_forward": OK,
    "audit_failed": PROBLEM,
    "passed": OK,
    "selected": OK,
    "unresolved": PROBLEM,
    "valid": OK,
    # publisher.py producer-profile lookups.
    "compat": OK,
    "missing_profile_treated_untrusted": PROBLEM,
    "synthetic_untrusted_lineage": PROBLEM,
    # publisher.py first-publish budget receipt.
    "not_applicable": OK,
    "not_configured": OK,
    "not_triggered": OK,
    "warning": PROBLEM,
    # publisher.py miscellany.
    "active": OK,  # branch registration
    "exact": OK,  # repository provenance
    # repository provenance when okg runs from a verified installed wheel:
    # the deployment is exact Git ids, okg is the verified wheel identity.
    "distribution": OK,
    "ok": OK,
    # publish_lock_headroom.py lock-hold headroom verdict, reported under
    # timing_details.lock.headroom.
    "pass": OK,
    "unbounded": PROBLEM,
    "unmeasured": PROBLEM,
    # Not emitted by the modules above today; kept so a stage that uses
    # these common failure words is reported by name.
    "refused": PROBLEM,
    "timeout": PROBLEM,
    "timed_out": PROBLEM,
    "warn": PROBLEM,
}

# Where STATUS_CLASSES comes from, relative to ``okg/substrate``. The guard
# test extracts every status literal from these files. The claim-precedence
# algebra under ``consistency/claim_model/`` is left out: its states are
# internal to claim resolution and never reach a cycle summary by
# themselves (a stage that did pass one through is still reported, because
# an unknown value fails closed).
STATUS_SOURCE_MODULES = (
    "admission.py",
    "attribute_integrity.py",
    "catalog/invariants_llm.py",
    "cli/main.py",
    "consistency/*.py",
    "derived_artifacts/*.py",
    "derived_node_liveness.py",
    "enrichers/*.py",
    "publish_lock_headroom.py",
    "publisher/*.py",
    "views/*.py",
)

# The publish verdict of one cycle summary (issue #2346). A blocked or failed
# publish used to exit 0 on the in-process path while its summary read
# "NOT PUBLISHED"; scripts and agents that check the exit code saw success.
#
# ``blocked``    the cycle built a graph version and a gate refused to
#                publish it (any of: status blocked, a block reason, or
#                ``phase_b_blocked``); readers still see the previous version.
#                Blocked wins over a failure status reported alongside it:
#                the refusal is the fact that decides what readers see.
# ``failed``     the cycle reported its own failure.
# ``skipped``    the runtime workflow decided not to run the cycle at all
#                (for example while a source sync is active). Nothing was
#                checked, which is a different fact from ``no_change``.
# ``unknown``    the status is missing or is a value this module does not
#                know; nothing proves a publish, so it fails closed.
# ``published``  the cycle published a new graph version.
# ``no_change``  the cycle ran and found nothing new; the current version
#                stays.
#
# Both ``okg run --once`` paths classify a cycle summary with
# ``publish_outcome``: the in-process path and the runtime-queue (DBOS)
# path, so the same summary gives the same outcome and exit code on both.
#
# Warnings on a published cycle (excluded sources, rejected derived output,
# degraded views, LLM-invariant fires) do not change the verdict: the graph
# version was published and readers see it, so the exit code stays 0 and the
# warnings print with their fix lines.
# The status sets and the classification live in
# ``okg.substrate.publish_outcome``, the one typed publish result.
BLOCKED_STATUSES = _publish_outcome.BLOCKED_STATUSES
FAILED_STATUSES = _publish_outcome.FAILED_STATUSES
SKIPPED_STATUSES = _publish_outcome.SKIPPED_STATUSES

OUTCOME_PUBLISHED = "published"
OUTCOME_NO_CHANGE = "no_change"
OUTCOME_BLOCKED = "blocked"
OUTCOME_FAILED = "failed"
OUTCOME_SKIPPED = "skipped"
OUTCOME_UNKNOWN = "unknown"

# 0 and 1 keep their meaning for every okg command (success; refusal or
# failure). 2 is taken by usage and extension errors. 3 means only this:
# the cycle ran, a gate refused the graph version it built, and readers
# still see the previous version.
EXIT_OK = 0
EXIT_FAILED = 1
EXIT_BLOCKED = 3
# 8 (decision 23a): the command succeeded and its publish committed, but the
# publish held the publisher lock longer than the capacity bar. A caller that
# treats only failures as fatal proceeds on it. `okg provision` returns it;
# `okg run --once` keeps reporting the same condition as a warning on exit 0.
# 4-7 are taken by the runtime worker's own exit statuses.
EXIT_CAPACITY_WARNING = 8
EXIT_CODES: dict[str, int] = {
    OUTCOME_PUBLISHED: EXIT_OK,
    OUTCOME_NO_CHANGE: EXIT_OK,
    OUTCOME_SKIPPED: EXIT_OK,
    OUTCOME_BLOCKED: EXIT_BLOCKED,
    OUTCOME_FAILED: EXIT_FAILED,
    OUTCOME_UNKNOWN: EXIT_FAILED,
}


def publish_outcome(out: Mapping[str, Any]) -> str:
    """The publish verdict of one cycle summary; see ``EXIT_CODES``.

    Delegates to ``publish_outcome.classify_publish``: blocked is checked
    before published, and a cycle that minted a new version is
    ``published`` even when its row deltas are zero (#2472).
    """
    return _publish_outcome.classify_publish(out).kind


FAILURE_VALUES = frozenset(
    value for value, cls in STATUS_CLASSES.items() if cls == PROBLEM
)
_STATUS_KEYS = ("status", "result", "outcome")

# Keys whose presence with a value is itself a failure report, with or
# without a status key (for example the attribute integrity receipt's
# ``error_type`` or a maintained derivation's ``error``).
_ERROR_KEYS = (
    "error", "error_type", "exception", "exception_class", "failure_reason",
    "feedback_error",
)
# Reason keys whose words name a failure (``maintain_failed``,
# ``post_checkpoint_finalization_failed``, ``post_publish_check_exception``).
_REASON_KEYS = ("reason", "fallback_reason")
_FAILURE_WORDS = frozenset({
    "crashed", "error", "errored", "errors", "exception", "fail", "failed",
    "failure", "failures", "timeout",
})
# A ``*_count`` key naming one of these words reports a problem when it is
# above zero (``degraded_view_count``, ``call_failures_count``,
# ``parse_errors_count``, ``fires_count``, ``span_invalid_count``).
_PROBLEM_COUNT_WORDS = _FAILURE_WORDS | {
    "degraded", "fires", "invalid", "violation", "violations",
}
_WORD_SPLIT = re.compile(r"[^a-z0-9]+")

# Top-level keys the generic walk skips: the timing receipts mostly repeat
# stage payloads (a second report of the same finding), and the one verdict
# only they carry, the publish lock-hold headroom, has its own explicit
# check; the top-level status and blocked_reason have their own explicit
# warning.
_WALK_SKIP = frozenset({
    "blocked_reason",
    "status",
    "timing_details",
    "timings_ms",
})


def classify_status(value: Any) -> str:
    """``ok``, ``problem``, or ``unknown`` for one status value."""
    text = str(value).strip().lower()
    if text in STATUS_CLASSES:
        return STATUS_CLASSES[text]
    head = text.split(":", 1)[0]
    if head != text and head in STATUS_CLASSES:
        return STATUS_CLASSES[head]
    return "unknown"


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _words(text: str) -> set[str]:
    return set(_WORD_SPLIT.split(text.lower()))


def _short(value: Any, limit: int = 160) -> str:
    text = str(value)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _warning(kind: str, message: str, fix_hint: str) -> dict[str, str]:
    return {"kind": kind, "message": message, "fix_hint": fix_hint}


def _target(deployment: str) -> str:
    """The deployment selector for a fix command.

    Mirrors ``migrate_command``: the ad-hoc ``--dsn`` run has no deployment
    name, so the hint asks for the DSN without printing it.
    """
    if deployment and deployment != AD_HOC_DEPLOYMENT_NAME:
        return f"--deployment {deployment}"
    return "--dsn <the deployment DSN>"


def _blocked_detail(blocked_reason: Any) -> str:
    if not isinstance(blocked_reason, Mapping):
        return str(blocked_reason) if blocked_reason else "no reason recorded"
    parts = [str(blocked_reason.get("reason") or "no reason recorded")]
    names: list[str] = []
    for key in ("failed_consistency_gates", "failed_invariants",
                "failed_sources"):
        for item in blocked_reason.get(key) or []:
            if isinstance(item, Mapping):
                name = (item.get("check_id") or item.get("gate_name")
                        or item.get("name") or item.get("invariant")
                        or item.get("source"))
                if name:
                    names.append(str(name))
            elif item:
                names.append(str(item))
    if names:
        parts.append("failed: " + ", ".join(names))
    # #3624: name each missing narrowing or subtype and the module that
    # declares it, not only the gate that failed.
    from okg.substrate.consistency.missing_patterns import (
        blocked_reason_fix_lines,
    )

    # Fix lines can hold "; " themselves, so " | " keeps them apart.
    pattern_fixes = blocked_reason_fix_lines(blocked_reason)
    if pattern_fixes:
        parts.append(" | ".join(pattern_fixes))
    fix = blocked_reason.get("fix_hint")
    if fix:
        parts.append(f"hint: {fix}")
    return "; ".join(parts)


def _gate_warnings(
    gates: Any, *, label: str, inspect: str, covered: set[int]
) -> list[dict[str, str]]:
    warnings: list[dict[str, str]] = []
    if not isinstance(gates, list):
        return warnings
    for gate in gates:
        if not isinstance(gate, Mapping):
            continue
        name = str(gate.get("check_id") or gate.get("gate_name") or label)
        opened = _int(gate.get("newly_opened_count"))
        open_count = _int(gate.get("open_observation_count"))
        raw = gate.get("result") or gate.get("status")
        result_class = classify_status(raw) if raw else OK
        blocks = bool(gate.get("blocks_publish"))
        if not (blocks or opened or open_count or result_class != OK):
            continue
        covered.add(id(gate))
        detail = [f"{label} {name}"]
        if blocks:
            detail.append("blocks the publish")
        if result_class != OK:
            detail.append(f"result {raw}")
        if open_count or opened:
            detail.append(
                f"{open_count} open violation(s), {opened} new this cycle"
            )
        if gate.get("reason"):
            detail.append(f"reason {gate.get('reason')}")
        warnings.append(_warning(
            "consistency_violation",
            ": ".join([detail[0], ", ".join(detail[1:])]),
            inspect,
        ))
    return warnings


def _problems_in(
    value: Mapping[str, Any], *, skip_status: bool = False
) -> list[str]:
    """The problem flags of one mapping, without looking at its children.

    ``skip_status`` leaves out the status keys of a mapping whose status an
    explicit warning already reported; its error, reason and count flags are
    still checked.
    """
    parts: list[str] = []
    for key in () if skip_status else _STATUS_KEYS:
        raw = value.get(key)
        if not isinstance(raw, str) or not raw.strip():
            continue
        cls = classify_status(raw)
        if cls == PROBLEM:
            parts.append(f"{key} is {raw}")
        elif cls == "unknown":
            parts.append(
                f"{key} is {raw}, a value this summary does not know, so "
                f"it is reported"
            )
    for key in _ERROR_KEYS:
        raw = value.get(key)
        if raw and not isinstance(raw, bool):
            parts.append(f"{key} {_short(raw)}")
    for key in _REASON_KEYS:
        raw = value.get(key)
        if isinstance(raw, str) and _words(raw) & _FAILURE_WORDS:
            parts.append(f"{key} {_short(raw)}")
    for key, raw in value.items():
        if (
            isinstance(key, str) and key.endswith("_count")
            and not isinstance(raw, bool) and _int(raw) > 0
            and _words(key[: -len("_count")]) & _PROBLEM_COUNT_WORDS
        ):
            parts.append(f"{key} {_int(raw)}")
    return parts


def _walk_problems(
    value: Any, path: str, found: list[str], covered: set[int],
    status_reported: set[int],
) -> None:
    if isinstance(value, Mapping):
        if id(value) not in covered:
            parts = _problems_in(
                value, skip_status=id(value) in status_reported,
            )
            if parts:
                found.append(f"{path}: " + "; ".join(parts))
        for key, child in value.items():
            _walk_problems(
                child, f"{path}.{key}", found, covered, status_reported,
            )
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _walk_problems(
                child, f"{path}[{index}]", found, covered, status_reported,
            )


def _view_names(items: Any) -> list[str]:
    names: list[str] = []
    for item in items or []:
        if isinstance(item, Mapping):
            names.append(str(
                item.get("view_id") or item.get("view") or item.get("name")
                or _short(dict(item), 80)
            ))
        else:
            names.append(str(item))
    return names


def _llm_rows(out: Mapping[str, Any]) -> list[dict[str, Any]]:
    judge = out.get("llm_judge")
    if not isinstance(judge, Mapping):
        return []
    rows: list[dict[str, Any]] = []
    for result in judge.get("results") or []:
        if not isinstance(result, Mapping):
            continue
        rows.append({
            "name": str(result.get("name")),
            "fires": _int(result.get("fires_count")),
            "call_failures": _int(result.get("call_failures_count")),
            "parse_errors": _int(result.get("parse_errors_count")),
            "span_invalid": _int(result.get("span_invalid_count")),
            "skipped_reason": result.get("skipped_reason"),
        })
    return rows


def collect_run_warnings(
    out: Mapping[str, Any], *, deployment: str
) -> list[dict[str, str]]:
    """Every warning or failure the cycle summary reports, in plain words.

    Fix lines inspect; they never run another publish. With a graph version
    in hand the fix is ``okg generation describe``; without one it is
    ``okg doctor``.
    """
    generation_id = out.get("generation_id")
    target = _target(deployment)
    doctor = f"okg doctor {target}"
    inspect = (
        f"okg generation describe {generation_id} {target}"
        if generation_id else doctor
    )
    warnings: list[dict[str, str]] = []
    # Mappings an explicit check below already reported; the generic walk
    # still descends into them but does not report them a second time.
    covered: set[int] = set()
    # Mappings whose status an explicit warning already reported; the walk
    # still checks their other flags.
    status_reported: set[int] = set()

    status = str(out.get("status") or "").strip().lower()
    outcome = publish_outcome(out)
    if outcome == OUTCOME_SKIPPED:
        warnings.append(_warning(
            "publish_skipped",
            f"the runtime skipped this cycle and published nothing "
            f"(reason: {_skip_reason(out) or 'not reported'}); readers "
            f"still see the previous graph version",
            doctor,
        ))
    elif status != "published":
        kind = {
            OUTCOME_BLOCKED: "publish_blocked",
            OUTCOME_FAILED: "publish_failed",
        }.get(outcome, "publish_not_published")
        warnings.append(_warning(
            kind,
            f"the cycle ended with status {status or 'missing'}; the graph "
            f"version it built is not the one readers see: "
            f"{_blocked_detail(out.get('blocked_reason'))}",
            inspect,
        ))
        feedback = out.get("admission_feedback")
        if isinstance(feedback, Mapping) and (
            str(feedback.get("status") or "").strip().lower() == status
        ):
            # The feedback block repeats the generation status.
            status_reported.add(id(feedback))
    elif outcome == OUTCOME_BLOCKED:
        warnings.append(_warning(
            "publish_blocked",
            f"the cycle reported status published and also a block, so it "
            f"is treated as not published: "
            f"{_blocked_detail(out.get('blocked_reason'))}",
            inspect,
        ))

    excluded = out.get("excluded_sources") or []
    if excluded:
        warnings.append(_warning(
            "sources_excluded",
            "a gate kept these sources out of this graph version: "
            + ", ".join(str(s) for s in excluded),
            doctor,
        ))

    warnings.extend(_gate_warnings(
        out.get("consistency_gates"), label="consistency gate",
        inspect=inspect, covered=covered,
    ))
    warnings.extend(_gate_warnings(
        out.get("post_publish_consistency_checks"),
        label="post-publish consistency check",
        inspect=inspect, covered=covered,
    ))

    refresh = out.get("native_view_refresh")
    if isinstance(refresh, Mapping):
        reported: set[str] = set()
        for count_key, list_key, kind, verb in (
            ("failed_view_count", "failed_views", "view_refresh_failed",
             "failed to refresh"),
            ("degraded_view_count", "degraded_views", "view_refresh_degraded",
             "refreshed with degraded output"),
        ):
            count = _int(refresh.get(count_key))
            if not count:
                continue
            names = _view_names(refresh.get(list_key))
            reported.update(names)
            covered.add(id(refresh))
            for item in refresh.get(list_key) or []:
                covered.add(id(item))
            warnings.append(_warning(
                kind, f"{count} view(s) {verb}: " + ", ".join(names), inspect,
            ))
        for view in out.get("views") or []:
            if isinstance(view, Mapping) and str(
                view.get("view_id") or view.get("view") or view.get("name")
            ) in reported:
                covered.add(id(view))

    # Producers whose verdict already has a warning. The derived-verdict
    # events under ``enrichers`` repeat the verdicts of ``enricher_runs`` and
    # ``derived_rejections``, so a producer warns once.
    reported_producers: set[str] = set()
    rejection_warning: dict[str, str] | None = None
    rejections = out.get("derived_rejections")
    if isinstance(rejections, Mapping) and (
        _int(rejections.get("rejected_producer_count"))
        or _int(rejections.get("discarded_record_count"))
    ):
        covered.add(id(rejections))
        verdicts: list[str] = []
        for producer in rejections.get("producers") or []:
            if not isinstance(producer, Mapping):
                continue
            covered.add(id(producer))
            producer_id = str(producer.get("producer_id") or "")
            if producer_id:
                reported_producers.add(producer_id)
            verdicts.append(
                f"{producer_id or 'unnamed'} {producer.get('status')}"
                + (f" {producer.get('code')}" if producer.get("code") else "")
            )
        rejection_warning = _warning(
            "derived_output_rejected",
            f"{_int(rejections.get('rejected_producer_count'))} producer(s) "
            f"had derived output rejected; "
            f"{_int(rejections.get('discarded_record_count'))} record(s) "
            f"discarded"
            + (f" ({', '.join(verdicts)})" if verdicts else ""),
            inspect,
        )
        warnings.append(rejection_warning)
    for index, run in enumerate(out.get("enricher_runs") or []):
        if not isinstance(run, Mapping) or not run.get("producer_id"):
            continue
        producer_id = str(run.get("producer_id"))
        parts = _problems_in(run)
        if rejection_warning is not None and (
            producer_id in reported_producers and parts
        ):
            # The rejection warning above already names this producer; add
            # the run's own problem flags to it instead of a second warning.
            covered.add(id(run))
            rejection_warning["message"] += (
                f"; enricher_runs[{index}]: " + "; ".join(parts)
            )
        elif parts:
            # The walk reports this run; its verdict event is a repeat.
            reported_producers.add(producer_id)
    for event in out.get("enrichers") or []:
        if not isinstance(event, Mapping):
            continue
        policy = event.get("producer_policy")
        if str(event.get("producer_id") or "") in reported_producers:
            covered.add(id(event))
            if isinstance(policy, Mapping):
                covered.add(id(policy))
        elif isinstance(policy, Mapping) and event.get("status"):
            # The policy block repeats the event's own status.
            status_reported.add(id(policy))

    lock = out.get("timing_details")
    lock = lock.get("lock") if isinstance(lock, Mapping) else None
    headroom = lock.get("headroom") if isinstance(lock, Mapping) else None
    if isinstance(headroom, Mapping) and (
        headroom.get("ok") is False
        or classify_status(headroom.get("status") or "ok") != OK
    ):
        warnings.append(_warning(
            "lock_headroom",
            f"publish lock-hold headroom is {headroom.get('status')}: "
            f"{headroom.get('reason') or 'no reason recorded'}",
            str(headroom.get("fix_hint") or inspect),
        ))

    violations = out.get("envelope_violations") or []
    if violations:
        warnings.append(_warning(
            "envelope_violation",
            f"{len(violations)} derived-output envelope violation(s)",
            inspect,
        ))

    budget = out.get("first_publish_budget_warning")
    if isinstance(budget, Mapping) and budget.get("triggered"):
        covered.add(id(budget))
        warnings.append(_warning(
            "first_publish_budget",
            "the first publish exceeded its declared size budget: "
            + ", ".join(str(t) for t in budget.get("triggered_by") or []),
            inspect,
        ))

    readiness = out.get("database_readiness")
    if readiness:
        command = (
            readiness.get("migrate_command")
            if isinstance(readiness, Mapping) else None
        )
        migrations = schema_objects = 0
        if isinstance(readiness, Mapping):
            migrations = int(readiness.get("pending_migration_count") or 0)
            schema_objects = int(readiness.get("pending_schema_object_count") or 0)
        if schema_objects and not migrations:
            text = "the database is missing search indexes `okg migrate --apply` builds"
        elif schema_objects:
            text = ("the database has pending migrations and is missing search "
                    "indexes `okg migrate --apply` builds")
        else:
            text = "the database has pending migrations"
        warnings.append(_warning(
            "migrations_pending",
            text,
            str(command or migrate_command(deployment)),
        ))

    judge = out.get("llm_judge")
    if isinstance(judge, Mapping):
        for result in judge.get("results") or []:
            if isinstance(result, Mapping):
                covered.add(id(result))
        for row in _llm_rows(out):
            counts = [
                f"{row[key]} {label}"
                for key, label in (
                    ("fires", "fire(s)"),
                    ("call_failures", "call failure(s)"),
                    ("parse_errors", "parse error(s)"),
                    ("span_invalid", "invalid span(s)"),
                )
                if row[key]
            ]
            if counts:
                warnings.append(_warning(
                    "llm_invariant",
                    f"LLM invariant {row['name']}: " + ", ".join(counts),
                    inspect,
                ))

    found: list[str] = []
    for key, value in out.items():
        if key in _WALK_SKIP:
            continue
        _walk_problems(value, key, found, covered, status_reported)
    for text in found:
        warnings.append(_warning("stage_failure", text, inspect))
    return warnings


def enricher_failures(out: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    """The enrichers that raised during this cycle, in run order.

    An enricher that raises is logged and swallowed by the publisher: the
    publish still commits, the enricher's watermark stays put, and the next
    cycle retries it (``Publisher._run_enrichers``). The cycle summary keeps
    the proof in ``enricher_runs`` (``status`` ``failed``, the exception
    class, and the consecutive-failure count read back from
    ``okg.enricher_progress``). This lifts those rows out so the one-line
    outcome and the JSON can name them instead of reading as a bare publish.

    Only raised runs count. A ``partial`` run (it returned but did not cover
    every input) is not a crash; it is still reported as a warning.
    """
    if not isinstance(out, Mapping):
        return []
    failures: list[dict[str, Any]] = []
    for run in out.get("enricher_runs") or []:
        if not isinstance(run, Mapping):
            continue
        status = str(run.get("status") or "").strip().lower()
        if status.split(":", 1)[0] != "failed":
            continue
        error_class = run.get("exception_class")
        if not error_class:
            reason = str(run.get("reason") or "")
            error_class = reason.partition(":")[2] or "unknown"
        failures.append({
            "name": str(run.get("name") or run.get("producer_id") or "unnamed"),
            "error_class": str(error_class),
            "consecutive_failures": _int(run.get("consecutive_failure_count")),
        })
    return failures


def enricher_failure_text(failures: list[Mapping[str, Any]]) -> str:
    """``N enricher(s) failed: name (ErrorClass, K cycles in a row), ...``."""
    parts = []
    for row in failures:
        count = _int(row.get("consecutive_failures"))
        streak = (
            f", {count} cycles in a row" if count > 1
            else ", first failure" if count == 1
            else ""
        )
        parts.append(f"{row['name']} ({row['error_class']}{streak})")
    noun = "enricher" if len(failures) == 1 else "enrichers"
    return f"{len(failures)} {noun} failed: " + ", ".join(parts)


def enricher_failure_hint(
    deployment: str, outcome: str, *, command_exit_code: int = 0,
) -> str:
    """What an enricher failure means for this cycle, and how to inspect it.

    The wording follows the publish ``outcome``: only a cycle that published
    a new graph version committed "without that enricher's output". A
    no-change cycle published nothing new, and a blocked or failed cycle
    published nothing at all and does not exit 0. ``command_exit_code`` is
    what the whole command returns (``okg ingest`` exits 1 when any source
    failed); the hint never claims exit 0 when the command does not.
    """
    inspect = (
        f"Inspect: okg doctor --check enricher-liveness {_target(deployment)}"
    )
    exit_note = "The exit code stays 0"
    if command_exit_code:
        exit_note = (
            "The enricher does not set the exit code; this command exits "
            f"{command_exit_code} because another source or publish failed"
        )
    if outcome == OUTCOME_PUBLISHED:
        meaning = (
            "the new graph version was published without that enricher's "
            f"output; the next cycle retries it. {exit_note}"
        )
    elif outcome == OUTCOME_NO_CHANGE:
        meaning = (
            "no new graph version was published; the enricher still fails "
            f"and retries next cycle. {exit_note}"
        )
    else:
        meaning = (
            f"this cycle published nothing (outcome {outcome}); the enricher "
            "also failed and retries next cycle. The exit code follows the "
            "publish outcome, not the enricher"
        )
    return f"{meaning}. {inspect}"


def _skip_reason(out: Mapping[str, Any]) -> str | None:
    reason = out.get("skipped") or out.get("skip_reason")
    return str(reason) if reason else None


def _sources(out: Mapping[str, Any]) -> list[dict[str, Any]]:
    per_source = out.get("per_source_status") or {}
    rows: list[dict[str, Any]] = []
    if isinstance(per_source, Mapping):
        for name in sorted(per_source):
            value = per_source[name]
            if isinstance(value, Mapping):
                state = value.get("status")
            else:
                state = getattr(value, "status", None) or value
            rows.append({"source": str(name), "status": str(state)})
    return rows


def build_run_summary(
    out: Mapping[str, Any], *, deployment: str
) -> dict[str, Any]:
    """The JSON-safe summary block of one inline publish cycle."""
    outcome = publish_outcome(out)
    return {
        "schema": SUMMARY_SCHEMA,
        "deployment": deployment,
        "status": out.get("status"),
        "outcome": outcome,
        "exit_code": EXIT_CODES[outcome],
        "published": outcome in (OUTCOME_PUBLISHED, OUTCOME_NO_CHANGE),
        "skip_reason": _skip_reason(out),
        "noop": bool(out.get("noop")),
        "generation_id": out.get("generation_id"),
        "predecessor_generation_id": out.get("predecessor_generation_id"),
        "records": {
            "added": _int(out.get("nodes_added")),
            "updated": _int(out.get("nodes_updated")),
            "retracted": _int(out.get("nodes_retracted")),
            "total": _int(out.get("node_count")),
        },
        "relationships": {
            "added": _int(out.get("edges_added")),
            "updated": _int(out.get("edges_updated")),
            "retracted": _int(out.get("edges_retracted")),
            "total": _int(out.get("edge_count")),
        },
        "sources": _sources(out),
        "llm_invariants": _llm_rows(out),
        "excluded_sources": [str(s) for s in out.get("excluded_sources") or []],
        "duration_ms": out.get("total_duration_ms"),
        "enricher_failures": enricher_failures(out),
        "warnings": collect_run_warnings(out, deployment=deployment),
    }


def render_run_summary(summary: Mapping[str, Any]) -> list[str]:
    """The default text lines for ``okg run --once``."""
    gen = summary.get("generation_id")
    rec = summary["records"]
    rel = summary["relationships"]
    lines: list[str] = []
    failures = summary.get("enricher_failures") or []
    if summary.get("outcome") == OUTCOME_PUBLISHED and failures:
        # The publish committed, so the headline still says so, and in the
        # same line names every enricher that raised (the standing rule: a
        # swallowed failure is never printed as a bare "published").
        lines.append(
            f"Published graph version {gen}; "
            f"{enricher_failure_text(failures)}"
        )
    elif summary.get("outcome") == OUTCOME_NO_CHANGE:
        lines.append(
            f"No change: this cycle found nothing new to publish; graph "
            f"version {gen} is still current."
        )
    elif summary.get("outcome") == OUTCOME_PUBLISHED:
        lines.append(f"Published graph version {gen}")
    elif summary.get("outcome") == OUTCOME_SKIPPED:
        lines.append(
            f"Skipped: the runtime did not run this cycle (reason: "
            f"{summary.get('skip_reason') or 'not reported'}); readers "
            f"still see the previous graph version."
        )
    else:
        lines.append(
            f"NOT PUBLISHED (status {summary.get('status')}): graph version "
            f"{gen} was built but readers still see the previous one."
        )
    if failures:
        if summary.get("outcome") != OUTCOME_PUBLISHED:
            lines.append(enricher_failure_text(failures) + ".")
        lines.append(
            "  " + enricher_failure_hint(
                str(summary.get("deployment") or ""),
                str(summary.get("outcome") or OUTCOME_UNKNOWN),
            )
        )
    lines.append(
        f"Records: {rec['added']} added, {rec['updated']} updated, "
        f"{rec['retracted']} retracted. Relationships: {rel['added']} added, "
        f"{rel['updated']} updated, {rel['retracted']} retracted."
    )
    lines.append(
        f"Graph now holds {rec['total']} records and {rel['total']} "
        f"relationships."
    )
    # The inline publish runs no source (it publishes what `okg ingest` or
    # source sync already staged), so its cycle summary carries no
    # per-source status and the line is left out.
    sources = summary.get("sources") or []
    if sources:
        lines.append("Sources: " + ", ".join(
            f"{row['source']} ({row['status']})" for row in sources
        ))
    llm_rows = summary.get("llm_invariants") or []
    if llm_rows:
        lines.append(
            f"LLM invariants: {len(llm_rows)} run; "
            f"{sum(r['fires'] for r in llm_rows)} fire(s), "
            f"{sum(r['call_failures'] for r in llm_rows)} call failure(s), "
            f"{sum(r['parse_errors'] for r in llm_rows)} parse error(s)."
        )
    warnings = summary.get("warnings") or []
    for warning in warnings:
        lines.append(f"warning [{warning['kind']}]: {warning['message']}")
        lines.append(f"  fix: {warning['fix_hint']}")
    duration = summary.get("duration_ms")
    took = f"Took {int(duration) / 1000:.1f}s. " if duration is not None else ""
    lines.append(
        f"{took}{len(warnings)} warning(s). Full report: add --verbose, "
        f"or --json."
    )
    return lines


__all__ = [
    "BLOCKED_STATUSES",
    "EXIT_BLOCKED",
    "EXIT_CAPACITY_WARNING",
    "EXIT_CODES",
    "EXIT_FAILED",
    "EXIT_OK",
    "FAILED_STATUSES",
    "SKIPPED_STATUSES",
    "FAILURE_VALUES",
    "STATUS_CLASSES",
    "STATUS_SOURCE_MODULES",
    "SUMMARY_SCHEMA",
    "build_run_summary",
    "classify_status",
    "collect_run_warnings",
    "enricher_failure_hint",
    "enricher_failure_text",
    "enricher_failures",
    "publish_outcome",
    "render_run_summary",
]
