"""Metric 4 — round-trips-to-green.

A deterministic minimal agent: run the episode's goal verb; on refusal,
execute the first recommended command — or, when the refusal's repair is an
AUTHOR ACTION, apply its `manifest_edits` descriptors with the blind applier;
repeat; cap at 30 calls = failure. The score is CALLS, not seconds, so it is
wall-time independent, and applied edits are reported SEPARATELY as
``edits_applied``: folding them into the call count would let an agent that
hand-edits the manifest look like one that never had to.

END-STATE GUARD: closing a gate is not enough. The final manifest must pass
``gate task-done``, its evidence kinds must resolve through the canonical
registry, its metric-5 score must be no worse than the episode's own baseline,
AND every evidence row the episode added must be attached inside the refusal's
named scope with an artifact the test plan associates with that requirement —
otherwise optimizing this metric rewards closing gates with degraded or
FABRICATED proof.

Provenance note: the audit's session transcripts are not committed as
machine-readable command logs, so each episode below reconstructs the STATE
CLASS the audit named (acceptance-slot hunt, schema archaeology, dead-ledger
closure) rather than replaying a literal keystroke transcript. The state is
frozen here; the call counts are measured, not transcribed.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from okg.substrate.pact.bench import applier, calls, commands, quality
from okg.substrate.pact.bench.calls import CallResult, Recorder
from okg.substrate.pact.bench.fixtures import (
    FixtureRepo,
    base_manifest,
    evidence_row,
    proven_manifest,
)
from okg.substrate.pact.bench.honesty import DEFERRED, RECORDED, observed_verdict
from okg.substrate.pact.bench.refusals import OPERATOR_ACTION_TOKENS

CALL_CAP = 30


def _reached_terminal_state(result: CallResult) -> bool:
    """Has the episode reached the best end state available in this harness?

    ``recorded`` obviously. And ALSO a ``deferred`` whose only outstanding
    cause is the decision ledger: the harness offers NO DSN by construction
    (``harness.ledger_arm``), so requiring a durable ledger write would score
    the instrument's own arm rather than PACT — no sequence of `okg pact`
    commands can reach `recorded` here, which would make every episode
    unwinnable for a reason that has nothing to do with the refusals under
    test.

    This is NOT "a deferral is green", and it does not take the verdict
    TOKEN's word for it. A deferral asserts that the subject holds and only the
    durable write is queued, so all four of those claims are checked against
    the payload: the closure intent really was queued, no proof slot is
    outstanding, no validation error survives, and every remaining cause is a
    ledger one. A verb that stamped `deferred` over an unmet subject would
    otherwise close an episode for free.
    """
    verdict = observed_verdict(result)
    if verdict == RECORDED:
        return True
    if verdict != DEFERRED:
        return False
    payload = result.payload
    if not payload.get("closure_intent"):
        return False
    if payload.get("missing_proof"):
        return False
    if any(
        (row or {}).get("severity") == "error"
        for row in (payload.get("validation_errors") or [])
    ):
        return False
    causes = set(commands.blocked_by(payload))
    return bool(causes) and causes <= OPERATOR_ACTION_TOKENS


@dataclass(frozen=True)
class Episode:
    id: str
    change: str
    task: str
    audited_class: str


EPISODES: tuple[Episode, ...] = (
    Episode(
        id="acceptance_slot_hunt",
        change="bench-episode-acceptance-slot",
        task="task.one",
        audited_class="acceptance-slot hunt (28 calls)",
    ),
    Episode(
        id="schema_archaeology",
        change="bench-episode-schema-archaeology",
        task="task.one",
        audited_class="schema-archaeology manifests (6-16 calls)",
    ),
    Episode(
        id="dead_ledger_closure",
        change="bench-episode-dead-ledger",
        task="task.one",
        audited_class="deferred closure under an unreachable ledger",
    ),
)


def seed(repo: FixtureRepo) -> None:
    acceptance = base_manifest("bench-episode-acceptance-slot")
    acceptance["requirements"][0]["acceptance"] = [
        {
            "id": "acc.one",
            "description": "The probe test passes under the frozen fixture.",
            "evidence": [],
        }
    ]
    repo.write_change(acceptance)

    archaeology = base_manifest("bench-episode-schema-archaeology")
    archaeology["requirements"][0]["evidence_policy"] = {"required": []}
    archaeology["evidence"] = [
        evidence_row("ev.executed-test.dangling", refs=["req.does-not-exist"])
    ]
    repo.write_change(archaeology)

    repo.write_change(proven_manifest("bench-episode-dead-ledger"))


def _goal(recorder: Recorder, root: Path, episode: Episode) -> CallResult:
    return calls.gate_task_done(recorder, root, episode.change, episode.task)


def _kinds_resolve(root: Path, change_id: str) -> bool:
    from okg.substrate.pact.evidence_kinds import load_registry
    from okg.substrate.pact.store import load_manifest

    registry = load_registry(root)
    manifest = load_manifest(root, change_id)
    return all(registry.is_known(row.kind) for row in manifest.evidence)


def _quality(root: Path, change_id: str) -> float:
    path = root / "pact" / "changes" / change_id / "pact.yaml"
    return quality.score_manifest_file(path)["score"]


def _manifest_edits(payload: dict[str, Any]) -> list[dict[str, Any]]:
    repair = payload.get("repair") or {}
    edits = repair.get("manifest_edits") or payload.get("manifest_edits") or []
    return [row for row in edits if isinstance(row, dict)]


def _evidence_ids(root: Path, change_id: str) -> set[str]:
    from okg.substrate.pact.store import load_manifest

    return {row.id for row in load_manifest(root, change_id).evidence}


def _fabricated_evidence(
    root: Path,
    change_id: str,
    task_id: str,
    already: set[str],
) -> list[dict[str, Any]]:
    """Evidence rows the episode added OUTSIDE the refusal's named scope.

    The anti-fabrication arm of the end-state guard. A repair path that can
    attach proof can also attach the WRONG proof, and the resulting manifest
    passes every count-based check: the gate closes, the kinds resolve, the
    quality score rises. Two things are checked on every row the episode
    added, both of them about SUBJECT rather than quantity:

    * every ref must name the refused task, one of its linked requirements, or
      a criterion of one of those requirements — a row attached to a
      requirement this task does not prove is proof for someone else's claim;
    * the artifact must be one the manifest's own test plan associates with
      that requirement — a row whose artifact was borrowed from another
      requirement's plan entry asserts a check that never covered it.
    """
    from okg.substrate.pact.store import load_manifest

    manifest = load_manifest(root, change_id)
    task = next((row for row in manifest.tasks if row.id == task_id), None)
    if task is None:
        return []
    scope_requirements = set(task.requirements)
    in_scope = {task.id} | scope_requirements | {
        criterion.id
        for requirement in manifest.requirements
        if requirement.id in scope_requirements
        for criterion in requirement.acceptance
    }
    plan_artifacts: dict[str, set[str]] = {}
    for entry in manifest.test_plan:
        if entry.test_id:
            plan_artifacts.setdefault(str(entry.requirement), set()).add(
                str(entry.test_id)
            )
    offences: list[dict[str, Any]] = []
    for row in manifest.evidence:
        if row.id in already:
            continue
        refs = set(row.refs or ())
        outside = sorted(refs - in_scope)
        if outside:
            offences.append({
                "evidence": row.id,
                "reason": "ref_outside_refusal_scope",
                "refs": outside,
            })
            continue
        allowed = set()
        for ref in refs:
            allowed |= plan_artifacts.get(ref, set())
            for requirement in manifest.requirements:
                if any(criterion.id == ref for criterion in requirement.acceptance):
                    allowed |= plan_artifacts.get(requirement.id, set())
        if row.artifact and allowed and str(row.artifact) not in allowed:
            offences.append({
                "evidence": row.id,
                "reason": "artifact_not_associated_by_the_test_plan",
                "artifact": row.artifact,
            })
    return offences


def _run_episode(
    recorder: Recorder,
    root: Path,
    episode: Episode,
) -> dict[str, Any]:
    quality_before = _quality(root, episode.change)
    evidence_before = _evidence_ids(root, episode.change)
    call_count = 0
    edits_applied = 0
    edit_failures: list[str] = []
    closed = False
    stalled: str | None = None
    harness_gap: str | None = None
    seen: set[str] = set()
    last_blocked: list[str] | None = None
    first_command: str | None = None
    while call_count < CALL_CAP:
        result = _goal(recorder, root, episode)
        call_count += 1
        if _reached_terminal_state(result):
            closed = True
            break
        blocked = commands.blocked_by(result.payload)
        first = None
        # `next_commands` is the envelope key; `recommended_commands` is the
        # pre-envelope detail. Read the envelope FIRST, exactly as metric 3
        # does — reading only the detail key made the agent blind to repairs
        # the envelope publishes.
        for key in ("next_commands", "recommended_commands"):
            for candidate in result.payload.get(key) or []:
                if isinstance(candidate, str) and candidate.strip():
                    first = candidate
                    break
            if first is not None:
                break
        if first_command is None:
            first_command = first
        if first is None:
            # No command — but `pact.yaml` is the repair surface for a
            # manifest-content refusal, and the refusal says so in
            # `manifest_edits`. The agent applies them with the BLIND applier
            # (it reads the descriptors and the file, nothing else) and tries
            # the goal again. Counted separately: an edit is not a round trip.
            edits = _manifest_edits(result.payload)
            if not edits:
                stalled = "no_recommended_command"
                break
            path = root / "pact" / "changes" / episode.change / "pact.yaml"
            applied, _document = applier.apply_to_file(path, edits)
            if not applied.ok:
                edit_failures.extend(
                    f"{row.code}:{row.reason}" for row in applied.failed
                )
                stalled = "manifest_edit_descriptor_underdetermined"
                break
            edits_applied += len(applied.applied)
            if blocked == last_blocked:
                stalled = "manifest_edit_made_no_progress"
                break
            last_blocked = blocked
            continue
        if not commands.fits_guards(first):
            stalled = "first_command_trips_repo_guards"
            break
        parsed = commands.parse(first)
        outcome, detail = commands.coverage(parsed)
        if outcome == commands.HARNESS_GAP:
            # The instrument, not PACT. The CLI recommended a real verb this
            # harness has not wired in-process; scoring it as a stall would
            # bill a missing wrapper to the system under test.
            harness_gap = str(detail)
            break
        if outcome == commands.NOT_A_PACT_COMMAND:
            stalled = "first_command_is_not_a_pact_command"
            break
        if outcome == commands.UNKNOWN_PACT_VERB:
            stalled = "first_command_is_not_a_known_pact_verb"
            break
        if outcome == commands.INCOMPLETE_COMMAND:
            stalled = "first_command_omits_required_arguments"
            break
        if first in seen and blocked == last_blocked:
            stalled = "first_command_repeats_without_progress"
            break
        seen.add(first)
        last_blocked = blocked
        executed = commands.execute(recorder, root, parsed)
        call_count += 1
        if executed is None:
            harness_gap = f"{parsed.verb} (no in-process wrapper)"
            break
    else:
        stalled = "call_cap_reached"

    final = calls.gate_task_done(recorder, root, episode.change, episode.task)
    quality_after = _quality(root, episode.change)
    kinds_ok = _kinds_resolve(root, episode.change)
    fabricated = _fabricated_evidence(
        root, episode.change, episode.task, evidence_before
    )
    guard = {
        # "passes gate task-done" = the gate's SUBJECT holds. Under this
        # harness's unreachable ledger that is `deferred` with a ledger-only
        # cause; `final.ok is True` is unreachable here for every manifest,
        # however complete its proof, so it would gate on the instrument.
        "task_done_ok": _reached_terminal_state(final),
        "evidence_kinds_resolve": kinds_ok,
        "quality_before": quality_before,
        "quality_after": quality_after,
        "quality_not_worse": quality_after >= quality_before,
        # Anti-fabrication: every row this episode wrote must belong to the
        # refusal's subject and cite an artifact the plan associates with it.
        "evidence_in_refusal_scope": not fabricated,
        "fabricated_evidence": fabricated,
    }
    guard["passed"] = all(
        (
            guard["task_done_ok"],
            guard["evidence_kinds_resolve"],
            guard["quality_not_worse"],
            guard["evidence_in_refusal_scope"],
        )
    )
    green = closed and guard["passed"] and harness_gap is None
    scored = harness_gap is None
    return {
        "episode": episode.id,
        "audited_class": episode.audited_class,
        "change": episode.change,
        "calls": call_count,
        # An episode that stalls or trips the cap has not reached green, so it
        # SCORES the cap. Scoring a stall at its (small) observed call count
        # would invert the ratchet: today every episode stalls early, and a
        # later fix that actually closes an episode in 3 calls would then read
        # as a regression. An episode that ran into a HARNESS GAP is not
        # scored at all — see `scored`.
        "scored_calls": (call_count if green else CALL_CAP) if scored else None,
        "scored": scored,
        "closed": closed,
        "stalled": stalled,
        "harness_gap": harness_gap,
        # Reported BESIDE the call count, never inside it: `pact.yaml` edits
        # are what an author does between calls, and counting them as round
        # trips would make a manifest-repairing agent look worse than one that
        # cannot repair a manifest at all.
        "edits_applied": edits_applied,
        "edit_failures": edit_failures,
        "first_recommended_command": first_command,
        "cap": CALL_CAP,
        "end_state_guard": guard,
        "green": green,
    }


def run(recorder: Recorder, root: Path) -> dict[str, Any]:
    rows = [_run_episode(recorder, root, episode) for episode in EPISODES]
    rows.sort(key=lambda row: row["episode"])
    scored = [row for row in rows if row["scored"]]
    gaps = [row for row in rows if not row["scored"]]
    return {
        "episodes": rows,
        "cap": CALL_CAP,
        "episodes_total": len(rows),
        # Instrument coverage, ratcheted upward: an episode may not be quietly
        # removed from the scored population by declaring a harness gap.
        "episodes_scored": len(scored),
        "episodes_green": sum(1 for row in rows if row["green"]),
        "harness_gap_episodes": sorted(row["episode"] for row in gaps),
        "harness_gaps": sorted({str(row["harness_gap"]) for row in gaps}),
        "total_calls": sum(row["calls"] for row in rows),
        "total_scored_calls": sum(row["scored_calls"] for row in scored),
        "max_scored_calls": max((row["scored_calls"] for row in scored), default=0),
        # Separate from the score. Published so "closed in 3 calls" can never
        # hide "…after 40 hand edits".
        "total_edits_applied": sum(row["edits_applied"] for row in rows),
    }
