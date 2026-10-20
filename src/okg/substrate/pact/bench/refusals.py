"""Metric 3 — refusal self-sufficiency, over THREE populations.

Every refusal the corpus produces lands in exactly one population, and each
population is scored on its own terms and published as its own rate. No
population may absorb another's shortfall — that is the whole point of the
split, and the reason a single blended number is not reported:

* **command-repairable** — an `okg pact` (or `git`) command repairs it. Scored
  by EXECUTING the repair against a copy of the fixture and re-running the
  refused call: the residual cause set, net of operator-action tokens, must be
  a strict subset of the original. Bad-argument refusals (unknown id,
  duplicate id) are scored through CORRECTED RE-INVOCATION instead — see
  :func:`_correction`;
* **author-action** — the repair is a `pact.yaml` edit. `pact.yaml` is
  canonical and hand-authored by design, so no verb writes it; the refusal
  instead carries a complete `manifest_edits` descriptor, and this metric
  applies it with the BLIND applier (`bench/applier.py`, which imports nothing
  from `okg`). Success is re-validation clearing that code with no new error
  code, no evidence rows added, and manifest quality no lower — an edit that
  clears a code by deleting the thing it quantifies over is not a repair;
* **operator-action** — nothing in this process clears it (the unreachable
  decision ledger is the canonical case). Success is naming an OWNER and a
  remedy DERIVED FROM THE ACTUAL CAUSE. Re-emitting `evidence sync` there is
  the measured infinite loop, not a repair.

``harness_gap`` remains a fourth bucket, excluded from every rate: a verb the
instrument has not wired says nothing about PACT.

WHY THIS FILE CHANGED (an instrument edited alongside the system it measures
owes an explicit justification, or the numbers move for reasons the reader
cannot audit). Two changes, both of them measurement defects rather than
rescorings:

1. THE POPULATION SPLIT. The single tri-valued classification blended three
   different repairs into one rate: a command that fails to execute, a
   descriptor that cannot be applied, and a missing owner are different
   failures with different fixes, and one number over all of them let a gain in
   one pay for a regression in another. Three populations, three rates, each
   scored on its own terms and ratcheted independently. The classification is
   TOTAL — `_population` is exhaustive, and `unclassified` stays a published
   field so a future bucket cannot be added without the number moving.
2. THE REPLAY ARTIFACTS. A repair is judged by REPLAYING the refused call, and
   two things about the replay were wrong rather than the repairs it judged:
   the cause sets were compared including the ledger tokens the harness itself
   guarantees are unclearable (so a repair that cleared everything it could
   read as "did not shrink"), and the throwaway fixture copy did not carry the
   graph-projection stub (so the replay was answered from the ORIGINAL
   manifest, and under whatever arm happened to be ambient). Both are fixed
   here — see :func:`_net` and :func:`fixture_copy` — and neither changes what
   counts as a repair, only what the instrument can see.
"""
from __future__ import annotations

import copy
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import yaml

from okg.substrate.pact.bench import applier, calls, commands, quality
from okg.substrate.pact.bench.calls import CallResult, Recorder
from okg.substrate.pact.bench.honesty import ABSENT, REFUSED, observed_verdict
from okg.substrate.pact.bench.isolation import (
    fixture_root_redirected,
    graph_projection_as,
)

COMMAND_REPAIRABLE = "command_repairable"
AUTHOR_ACTION = "author_action"
OPERATOR_ACTION = "operator_action"
# Not a verdict about PACT: the harness could not run the recommended command
# because IT has no in-process wrapper for that verb. Reported as instrument
# coverage and excluded from every rate, so a hole in the instrument can never
# be read as a defect in the system under test.
HARNESS_GAP = "harness_gap"

POPULATIONS = (COMMAND_REPAIRABLE, AUTHOR_ACTION, OPERATOR_ACTION)

# Blockers no `okg pact` command can clear. The design's counterexample to a
# flat 100% bar — and the tokens SUBTRACTED FROM BOTH SIDES before a repair is
# judged: the ledger is dead for the whole run, so leaving its token in the
# comparison made every otherwise-successful repair read as "did not shrink".
#
# `no_ledger_approval` and `approval_not_archived` are deliberately NOT here.
# Both are merge-boundary causes where the ledger ANSWERED (issue #1443), so
# neither is an out-of-band ledger fault: the first is cleared by obtaining a
# decision, the second by archiving the approved change. Neither is currently
# EXERCISED by this corpus — no bench case reaches `gate pre-merge` against a
# live ledger, and the measured populations did not move when they were
# introduced. They are named here so that a future case which does reach that
# gate cannot be scored as an unrepairable ledger fault by omission.
OPERATOR_ACTION_TOKENS = frozenset(
    {"ledger_dsn_not_configured", "ledger_not_fresh"}
)

# A task the AUTHOR marked `blocked` with a prose reason is waiting on
# something outside this process by construction. The token is parameterized by
# task id, so it is matched by prefix rather than listed.
OPERATOR_ACTION_TOKEN_PREFIXES = ("task_blocked:",)


def is_operator_action_token(token: Any) -> bool:
    text = str(token)
    return text in OPERATOR_ACTION_TOKENS or text.startswith(
        OPERATOR_ACTION_TOKEN_PREFIXES
    )

# The argument each unknown-id refusal names, and the origin slot that carries
# it. This is the FIXED RULE the corrected re-invocation is built by — never a
# search over the argv for something that looks close.
_OFFENDING_ARGUMENT: dict[str, tuple[str, str | None]] = {
    # data key -> (origin slot, kwargs key)
    "task_id": ("task", None),
    "change": ("change", None),
    "requirement_id": ("task", "target"),
}

# The same rule for a rejected WRITE: the validation code the write gate
# refused names which argument of the invocation was wrong. `--kind` is the
# audited one (`evidence attach --kind <not in the registry>`).
_OFFENDING_ARGUMENT_BY_CODE: dict[str, str] = {
    "unknown_evidence_kind": "kind",
}


@contextmanager
def fixture_copy(root: Path, *, graph_projection: str | None = None) -> Iterator[Path]:
    """A throwaway copy of the fixture repo, so repairs do not leak.

    Two things travel with the copy, both of them replay artifacts the reviewer
    measured:

    * the stubbed graph projection reads the manifest off
      ``RuntimeControls.fixture_root``, so without the redirect it answered a
      replay against the COPY with the ORIGINAL manifest's state digest — an
      edit the replay had just made was invisible to the graph arm;
    * the projection ARM itself, so a call recorded under a fresh projection is
      replayed under one.
    """
    with TemporaryDirectory(prefix="pact-bench-replay-") as tmp:
        destination = Path(tmp) / "repo"
        shutil.copytree(root, destination, symlinks=True)
        with fixture_root_redirected(destination), graph_projection_as(
            graph_projection
        ):
            yield destination


def _message(payload: dict[str, Any]) -> str | None:
    for key in ("message", "text", "why"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    error = payload.get("error") or {}
    value = error.get("message")
    return value if isinstance(value, str) and value.strip() else None


def _names_offending_value(payload: dict[str, Any]) -> bool:
    error = payload.get("error") or {}
    if error.get("data"):
        return True
    if payload.get("missing_proof") or payload.get("blockers"):
        return True
    validation = payload.get("validation") or {}
    rows = list(validation.get("errors") or []) + list(
        payload.get("validation_errors") or []
    )
    return any(row.get("path") for row in rows)


def _first_command(payload: dict[str, Any]) -> str | None:
    # `next_commands` is the envelope key; `recommended_commands` is the
    # pre-envelope detail several verbs still carry. Reading only the latter
    # made the classifier blind to every command the envelope now publishes —
    # refusals that DO name a repair scored `no_recommended_command`.
    for key in ("next_commands", "recommended_commands"):
        for candidate in payload.get(key) or []:
            if isinstance(candidate, str) and candidate.strip():
                return candidate
    next_command = payload.get("next_command")
    if isinstance(next_command, str) and next_command.strip():
        return next_command
    error = payload.get("error") or {}
    fix_hint = error.get("fix_hint") or payload.get("fix_hint")
    return fix_hint if isinstance(fix_hint, str) and fix_hint.strip() else None


def _operator_action(payload: dict[str, Any]) -> dict[str, Any]:
    """The refusal's operator-action block, wherever the verb carries it."""
    for candidate in (
        (payload.get("repair") or {}).get("operator_action"),
        payload.get("operator_action"),
        (payload.get("error") or {}).get("data", {}).get("operator_action")
        if isinstance((payload.get("error") or {}).get("data"), dict)
        else None,
    ):
        if isinstance(candidate, dict) and candidate:
            return candidate
    return {}


def _names_operator_remedy(payload: dict[str, Any]) -> bool:
    if _operator_action(payload).get("remedy"):
        return True
    authority = payload.get("workflow_authority") or {}
    ledger = authority.get("ledger_authority") or {}
    return bool(ledger.get("fix_hint") or ledger.get("reason"))


def _names_owner(payload: dict[str, Any]) -> bool:
    if _operator_action(payload).get("owner"):
        return True
    authority = payload.get("workflow_authority") or {}
    ledger = authority.get("ledger_authority") or {}
    return bool(ledger.get("deployment") or payload.get("owner"))


def _remedy_is_cause_derived(payload: dict[str, Any]) -> bool:
    """The remedy names the CAUSE the refusal actually reported.

    A defaulted remedy ("export OKG_DSN=...") is not cause-derived: it is the
    fix for one cause emitted for all of them, which is how two verbs in the
    same shell named different remedies for the same dead ledger.
    """
    action = _operator_action(payload)
    if not action.get("remedy"):
        return False
    source = action.get("remedy_source")
    if source is None:
        # A verb-local operator block (the FSM's operator-only verdict) states
        # its own cause instead of carrying the ledger's.
        return bool(action.get("cause"))
    return source == "cause_fix_hint" or bool(action.get("cause"))


def _manifest_edits(payload: dict[str, Any]) -> list[dict[str, Any]]:
    repair = payload.get("repair") or {}
    edits = repair.get("manifest_edits") or payload.get("manifest_edits") or []
    return [row for row in edits if isinstance(row, dict)]


def _error_data(payload: dict[str, Any]) -> dict[str, Any]:
    data = (payload.get("error") or {}).get("data")
    return data if isinstance(data, dict) else {}


def _causes(payload: dict[str, Any]) -> list[str]:
    return commands.blocked_by(payload)


def _net(causes: Any) -> set[str]:
    """The cause set an in-process repair could possibly clear."""
    return {
        str(token)
        for token in causes or ()
        if not is_operator_action_token(token)
    }


# --- corrected re-invocation ----------------------------------------------


def _correction(payload: dict[str, Any], origin: dict[str, Any]) -> dict[str, Any] | None:
    """The corrected invocation for a BAD-ARGUMENT refusal, by a fixed rule.

    Fences, all five of them:

    B1  the payload must carry near-miss CANDIDATES, and `candidate_source`
        must say they are difflib hits. The listing fallback (a truncated legal
        set for a totally unrelated id) is a menu, not a correction, and
        building a re-invocation on it would be the `legal_values[0]` guess in
        another costume;
    B2  the offending argument is located by the data key the refusal used
        (`task_id`, `change`, ...), never by scanning the argv;
    B3  the correction is `candidates[0]` at that position — same verb, one
        token;
    B4  exactly ONE argv token may differ from the original invocation;
    B5  a duplicate change id is the one carve-out: its repair is to USE the
        change that already exists (`okg pact view <id>`), never to scaffold a
        second one. The verb changes by construction, so B4 does not apply and
        the row records `use_existing` instead.
    """
    data = _error_data(payload)
    if not data or not origin:
        return None

    if data.get("cause") == "duplicate_change_id" and data.get("existing_change"):
        return {
            "kind": "use_existing",
            "origin": {"verb": "view_status", "change": str(data["existing_change"])},
            "changed": "verb",
        }

    candidates = [str(value) for value in data.get("candidates") or [] if value]
    if not candidates or data.get("candidate_source") != "near_miss":
        return None

    for row in data.get("errors") or []:
        kwargs_key = _OFFENDING_ARGUMENT_BY_CODE.get(str((row or {}).get("code")))
        if kwargs_key is None or not isinstance(origin.get("kwargs"), dict):
            continue
        corrected = copy.deepcopy(dict(origin))
        corrected["kwargs"][kwargs_key] = candidates[0]
        original_command = commands.render_origin(origin)
        corrected_command = commands.render_origin(corrected)
        if not original_command or not corrected_command:
            return None
        if commands.differing_argv_tokens(original_command, corrected_command) != 1:
            return None
        return {
            "kind": "corrected_argument",
            "origin": corrected,
            "changed": kwargs_key,
            "command": corrected_command,
        }

    for key, (slot, kwargs_key) in _OFFENDING_ARGUMENT.items():
        if key not in data:
            continue
        corrected = copy.deepcopy(dict(origin))
        corrected[slot] = candidates[0]
        if kwargs_key and isinstance(corrected.get("kwargs"), dict):
            corrected["kwargs"][kwargs_key] = candidates[0]
        original_command = commands.render_origin(origin)
        corrected_command = commands.render_origin(corrected)
        if not original_command or not corrected_command:
            return None
        differing = commands.differing_argv_tokens(original_command, corrected_command)
        if differing != 1:
            return None
        return {
            "kind": "corrected_argument",
            "origin": corrected,
            "changed": slot,
            "command": corrected_command,
        }
    return None


def _attempt_correction(
    root: Path,
    correction: dict[str, Any],
    baseline: list[str],
) -> dict[str, Any]:
    """Run the corrected invocation and judge it against the ORIGINAL causes."""
    with fixture_copy(
        root, graph_projection=correction["origin"].get("graph_projection")
    ) as copy_root:
        recorder = Recorder(root=copy_root)
        after = calls.replay(recorder, copy_root, correction["origin"])
        if after is None:
            return {"status": "unavailable", "reason": "correction is not replayable"}
        remaining = commands.blocked_by(after.payload)
    return {
        "status": "executed",
        "kind": correction["kind"],
        "shrank_blocked_by": _net(remaining) < _net(baseline),
        "blocked_by_before": len(_net(baseline)),
        "blocked_by_after": len(_net(remaining)),
    }


# --- command repairs -------------------------------------------------------


def _attempt_repair(
    root: Path,
    result: CallResult,
    parsed: commands.ParsedCommand,
    baseline: list[str],
) -> dict[str, Any]:
    """Execute the first command on a copy, then re-run the refused call."""
    if not result.origin:
        return {"status": "unavailable", "reason": "call is not replayable"}
    with fixture_copy(
        root, graph_projection=result.origin.get("graph_projection")
    ) as copy_root:
        replay_recorder = Recorder(root=copy_root)
        executed = commands.execute(replay_recorder, copy_root, parsed)
        if executed is None:
            return {"status": "unavailable", "reason": "command not executable"}
        after = calls.replay(replay_recorder, copy_root, result.origin)
        if after is None:
            return {"status": "unavailable", "reason": "call is not replayable"}
        remaining = commands.blocked_by(after.payload)
    # Net of the operator-action tokens on BOTH sides: the harness offers no
    # DSN by construction, so a ledger token present before and after says
    # nothing about the repair, and leaving it in made a genuine repair read as
    # a failure whenever it was the only survivor.
    return {
        "status": "executed",
        "shrank_blocked_by": _net(remaining) < _net(baseline),
        "blocked_by_before": len(_net(baseline)),
        "blocked_by_after": len(_net(remaining)),
    }


# --- author-action repairs -------------------------------------------------


def _structural_codes(path: Path) -> set[str]:
    from okg.substrate.pact.store import load_manifest_file
    from okg.substrate.pact.validation import structural_issues

    return {
        issue.code
        for issue in structural_issues(load_manifest_file(path))
        if issue.severity == "error"
    }


def _attempt_edits(
    root: Path,
    change_id: str,
    edits: list[dict[str, Any]],
) -> dict[str, Any]:
    """Apply the descriptors BLIND, then judge the edited manifest.

    The bar is deliberately not "the applier did something": an edit that
    cleared a code by deleting its subject, or that quietly added an evidence
    row, would pass that. Four conditions, all measured on the file:
    the code is gone, no NEW error code appeared, the evidence row count is
    unchanged, and the manifest-quality score (metric 5) is no lower.
    """
    if not change_id:
        return {"status": "unavailable", "reason": "refusal names no change"}
    with fixture_copy(root) as copy_root:
        path = copy_root / "pact" / "changes" / change_id / "pact.yaml"
        if not path.is_file():
            return {"status": "unavailable", "reason": "manifest not in the fixture"}
        before_document = yaml.safe_load(path.read_text(encoding="utf-8"))
        before_codes = _structural_codes(path)
        before_quality = quality.score_manifest_file(path)["score"]
        before_rows = len((before_document or {}).get("evidence") or [])

        applied, _document = applier.apply_to_file(path, edits)
        if not applied.ok:
            return {
                "status": "applied" if applied.applied else "refused",
                "cleared": False,
                "reason": (
                    applied.failed[0].reason if applied.failed else "no descriptors"
                ),
                "detail": applied.failed[0].detail if applied.failed else None,
                "result": applied.to_dict(),
            }

        after_document = yaml.safe_load(path.read_text(encoding="utf-8"))
        after_codes = _structural_codes(path)
        after_quality = quality.score_manifest_file(path)["score"]
        after_rows = len((after_document or {}).get("evidence") or [])

    targeted = {str(edit.get("code")) for edit in edits} | {
        str(code) for edit in edits for code in edit.get("also_clears") or ()
    }
    new_codes = sorted(after_codes - before_codes)
    return {
        "status": "applied",
        "cleared": bool(
            not (targeted & after_codes)
            and not new_codes
            and after_rows == before_rows
            and after_quality >= before_quality
        ),
        "targeted_codes": sorted(targeted),
        "codes_before": sorted(before_codes),
        "codes_after": sorted(after_codes),
        "new_codes": new_codes,
        "evidence_rows_before": before_rows,
        "evidence_rows_after": after_rows,
        "quality_before": before_quality,
        "quality_after": after_quality,
        "result": applied.to_dict(),
    }


# --- classification --------------------------------------------------------


def _population(
    payload: dict[str, Any],
    *,
    causes: list[str],
    first: str | None,
    correction: dict[str, Any] | None,
    edits: list[dict[str, Any]],
) -> str:
    """Which population owns this refusal. Total: nothing falls through.

    Order matters and is stated here rather than spread across the branches:
    an operator-only cause outranks everything (no in-band repair exists); a
    bad-argument refusal is judged by its correction, not by whatever generic
    hint it also carries; a command beats an edit, because the command is what
    an agent runs first.
    """
    from okg.substrate.pact.repair import EDIT_REPAIRABLE_CODES

    if causes and not _net(causes):
        return OPERATOR_ACTION
    if correction is not None:
        return COMMAND_REPAIRABLE
    if first is not None:
        return COMMAND_REPAIRABLE
    if edits:
        return AUTHOR_ACTION
    # No repair was published at all. The population is decided by the CAUSE,
    # so the row fails the population it belongs to instead of landing in a
    # residual bucket that nobody has to fix.
    if _net(causes) & set(EDIT_REPAIRABLE_CODES):
        return AUTHOR_ACTION
    return COMMAND_REPAIRABLE


def classify(root: Path, result: CallResult) -> dict[str, Any]:
    payload = result.payload
    baseline = _causes(payload)
    message = _message(payload)
    first = _first_command(payload)
    edits = _manifest_edits(payload)
    correction = _correction(payload, result.origin)
    change_id = str(
        payload.get("change") or _error_data(payload).get("change") or ""
    )

    parsed = commands.parse(first) if first else None
    guards_ok = commands.fits_guards(first) if first else False
    coverage, coverage_detail = (
        commands.coverage(parsed) if parsed is not None else (None, None)
    )
    executable = coverage == commands.EXECUTABLE

    population = _population(
        payload,
        causes=baseline,
        first=first,
        correction=correction,
        edits=edits,
    )

    repair: dict[str, Any] = {"status": "not_attempted"}
    repaired = False
    reason: str | None = None

    if population == OPERATOR_ACTION:
        repaired = bool(
            _names_owner(payload)
            and _names_operator_remedy(payload)
            and _remedy_is_cause_derived(payload)
        )
        if not repaired:
            reason = (
                "operator_action_without_owner"
                if not _names_owner(payload)
                else "operator_remedy_not_cause_derived"
            )
    elif correction is None and _error_data(payload).get("candidates"):
        # A bad-argument refusal that published its legal set but no single
        # correction: the caller has to CHOOSE. Named as its own reason so it
        # cannot be read as "the refusal said nothing".
        reason = (
            "correction_not_determined:candidate_source="
            f"{_error_data(payload).get('candidate_source')}"
        )
    elif population == AUTHOR_ACTION:
        if not edits:
            reason = "no_manifest_edit_descriptor"
        else:
            repair = _attempt_edits(root, change_id, edits)
            repaired = bool(repair.get("cleared"))
            if not repaired:
                reason = f"manifest_edit_did_not_clear:{repair.get('reason') or 'guard'}"
    elif correction is not None:
        repair = _attempt_correction(root, correction, baseline)
        if repair.get("status") != "executed":
            return _row(
                result,
                population=HARNESS_GAP,
                repaired=False,
                reason=f"harness_could_not_replay:{repair.get('reason')}",
                payload=payload,
                baseline=baseline,
                coverage=coverage,
                message=message,
                first=first,
                guards_ok=guards_ok,
                parsed=parsed,
                repair=repair,
                edits=edits,
                correction=correction,
            )
        repaired = bool(repair.get("shrank_blocked_by"))
        if not repaired:
            reason = "corrected_re_invocation_did_not_shrink_blocked_by"
    elif first is None:
        reason = "no_recommended_command"
    elif not guards_ok:
        reason = "first_command_trips_repo_guards"
    elif coverage == commands.NOT_A_PACT_COMMAND:
        reason = f"first_command_is_not_a_pact_command:{coverage_detail}"
    elif coverage == commands.UNKNOWN_PACT_VERB:
        reason = f"first_command_is_not_a_known_pact_verb:{coverage_detail}"
    elif coverage == commands.INCOMPLETE_COMMAND:
        reason = f"first_command_omits_required_arguments:{coverage_detail}"
    elif coverage == commands.HARNESS_GAP:
        population = HARNESS_GAP
        reason = f"harness_has_no_wrapper_for:{coverage_detail}"
    elif executable:
        repair = _attempt_repair(root, result, parsed, baseline)
        if repair.get("status") == "unavailable":
            population = HARNESS_GAP
            reason = f"harness_could_not_replay:{repair.get('reason')}"
        elif not message:
            reason = "no_message"
        elif not _names_offending_value(payload):
            reason = "offending_value_absent"
        elif repair.get("shrank_blocked_by"):
            repaired = True
        else:
            reason = "first_command_did_not_shrink_blocked_by"

    return _row(
        result,
        population=population,
        repaired=repaired,
        reason=reason,
        payload=payload,
        baseline=baseline,
        coverage=coverage,
        message=message,
        first=first,
        guards_ok=guards_ok,
        parsed=parsed,
        repair=repair,
        edits=edits,
        correction=correction,
    )


def _row(
    result: CallResult,
    *,
    population: str,
    repaired: bool,
    reason: str | None,
    payload: dict[str, Any],
    baseline: list[str],
    coverage: str | None,
    message: str | None,
    first: str | None,
    guards_ok: bool,
    parsed: Any,
    repair: dict[str, Any],
    edits: list[dict[str, Any]],
    correction: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "verb": result.verb,
        "population": population,
        "repaired": bool(repaired),
        "reason": reason,
        "coverage": coverage,
        "has_message": bool(message),
        "names_offending_value": _names_offending_value(payload),
        "first_command_present": first is not None,
        "first_command_fits_guards": guards_ok,
        "first_command_executable": bool(parsed and parsed.executable),
        "blocked_by": baseline,
        "repair": repair,
        "manifest_edits": len(edits),
        "correction": None if correction is None else correction.get("kind"),
        "names_owner": _names_owner(payload),
        "names_out_of_band_remedy": _names_operator_remedy(payload),
        "remedy_is_cause_derived": _remedy_is_cause_derived(payload),
    }


def _rate(rows: list[dict[str, Any]], population: str) -> dict[str, Any]:
    members = [row for row in rows if row["population"] == population]
    repaired = sum(1 for row in members if row["repaired"])
    return {
        "population": population,
        "rows": len(members),
        "repaired": repaired,
        "rate": round(repaired / len(members), 4) if members else 0.0,
        "unrepaired_reasons": sorted(
            {str(row["reason"]) for row in members if not row["repaired"]}
        ),
    }


def run(recorder: Recorder, root: Path) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for result in recorder.results:
        if observed_verdict(result) not in {REFUSED, ABSENT}:
            continue
        rows.append(classify(root, result))
    rows.sort(key=lambda row: (row["verb"], row["population"], str(row["reason"])))
    populations = {name: _rate(rows, name) for name in POPULATIONS}
    gaps = [row for row in rows if row["population"] == HARNESS_GAP]
    counted = sum(row["rows"] for row in populations.values()) + len(gaps)
    return {
        "rows": rows,
        "refusals": len(rows),
        "populations": populations,
        # The three published rates, hoisted so the ratchet and the report can
        # name them individually. No blended number is published: one rate over
        # three populations is exactly the "no population absorbs another's
        # shortfall" failure the split exists to prevent.
        "command_repairable_rate": populations[COMMAND_REPAIRABLE]["rate"],
        "author_action_rate": populations[AUTHOR_ACTION]["rate"],
        "operator_action_rate": populations[OPERATOR_ACTION]["rate"],
        "command_repairable": populations[COMMAND_REPAIRABLE]["rows"],
        "author_action": populations[AUTHOR_ACTION]["rows"],
        "operator_action": populations[OPERATOR_ACTION]["rows"],
        # Instrument coverage, excluded from every rate: a verb the harness has
        # not wired says nothing about PACT.
        "harness_gap": len(gaps),
        "harness_gap_reasons": sorted({str(row["reason"]) for row in gaps}),
        # Every refusal lands in exactly one bucket by construction; the field
        # exists so a future bucket cannot be added without the number moving.
        "unclassified": len(rows) - counted,
        # The number that must not be gamed downward: a change that makes a
        # verb emit FEWER refusals raises every rate for free, so the count is
        # ratcheted alongside the rates.
        "unrepaired": sum(
            1
            for row in rows
            if row["population"] in POPULATIONS and not row["repaired"]
        ),
        "operator_rows_naming_owner_and_remedy": sum(
            1
            for row in rows
            if row["population"] == OPERATOR_ACTION
            and row["names_owner"]
            and row["names_out_of_band_remedy"]
        ),
    }
