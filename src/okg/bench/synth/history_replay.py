"""Replay a sealed history through the DIRECT publish path, step by step.

RUNNER side of the purity boundary: this module may import substrate, because
publishing IS the system under test. :mod:`okg.bench.synth.history` computes the
reference answer; this module executes the history against a real deployment and
reports where the two agree.

Three things are proven here, in the order the PACT states them.

1. REPLAY DETERMINISM. Extraction runs ONCE per update; its fact set is
   RECORDED, and every executor that replays that update consumes the recorded
   facts verbatim through ``publish_fact_list``. Without this rule, a history
   replayed twice through a nondeterministic extractor yields two different claim
   sets, and a downstream parity check would read provider drift as a maintenance
   defect — the single most expensive confusion this program could ship. The rule
   is witnessed load-bearing (``tests/bench/synth/test_history_replay.py``): the
   forbidden path, re-extracting per executor, is shown able to differ.

2. THE EXPECTED DIFF. Each step's realized accepted state — the
   ``bucket_resolution`` rows at that step's pinned generation — is compared
   against the gold expected state and the gold expected diff. The direct path
   runs a FULL republish per step (no reuse), so a mismatch is a disagreement
   between gold's restatement of the precedence order and the engine's, never a
   reuse artifact.

3. THE TRAJECTORY. Per update the funnel is computed over the corruptions LIVE
   at that update, and across updates each corruption carries its state sequence.
   The falsifiable property: a corruption repaired at update k whose bucket no
   later update touches must still be repaired at every later update. A
   spontaneous un-repair means an unrelated document changed a settled answer,
   and the report names the update, the bucket, and whether the reference agrees.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Sequence

from okg.bench.synth.history import (
    History,
    history_to_json,
    verify_chain,
)
from okg.bench.synth.resolution import (
    STATE_REPAIRED,
    assert_arm_signal_scope,
    funnel,
)
from okg.bench.synth.runner import (
    RunnerError,
    canonicalize_value,
    constraint_policy_facts,
    detection_state,
    extract_facts,
    functional_policies,
    publish_fact_list,
    resolutions_at,
    synth_value_equivalent,
)

REPLAY_SCHEMA = "okg.synth.history-replay.v1"
RECORDED_FACTS_DIRNAME = "recorded-facts"
REPLAY_FILENAME = "synth-history-replay.json"
HISTORY_FILENAME = "synth-history.json"

# The verdicts the no-regression check emits. Naming WHICH failure it is, is the
# requirement: the check is cheap precisely because it needs no reference run, so
# it must still say whether the reference answer moved with the engine.
VERDICT_UNSOUND = "settled_answer_moved_reference_agrees_it_should_not_have"
VERDICT_GOLD_DISAGREES = "settled_answer_moved_and_reference_moved_too"


class HistoryReplayError(RunnerError):
    """A replay could not be executed or a per-step contract was breached."""


# --------------------------------------------------------------------------- #
# Recorded fact sets — the replay-determinism mechanism                        #
# --------------------------------------------------------------------------- #
def serialize_facts(facts: Sequence[Any]) -> list[dict[str, Any]]:
    return [
        {
            "node_id": fact.node_id,
            "subtype": fact.subtype,
            "attrs": dict(fact.attrs),
            "source_record_id": dict(fact.source_record_id),
            "source_revision": dict(fact.source_revision),
            "op": fact.op,
            "ownership_mode": fact.ownership_mode,
        }
        for fact in facts
    ]


def deserialize_facts(rows: Sequence[Mapping[str, Any]]) -> list[Any]:
    from okg.substrate.library.sources.base import NodeFact

    return [
        NodeFact(
            row["node_id"],
            row["subtype"],
            dict(row["attrs"]),
            dict(row["source_record_id"]),
            dict(row["source_revision"]),
            op=row.get("op", "I"),
            ownership_mode=row.get("ownership_mode", "authoritative"),
        )
        for row in rows
    ]


def facts_digest(rows: Sequence[Mapping[str, Any]]) -> str:
    import hashlib

    return hashlib.sha256(
        json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def apply_history_stamps(
    facts: Sequence[Any], stamps: Mapping[str, Mapping[str, str]], admits: Sequence[str]
) -> list[Any]:
    """Stamp the sealed per-step decisive signals onto the EXTRACTED claims.

    The same ingest seam ``apply_arm_stamps`` uses, and for the same reason: a
    signal stamped at ingest never enters the document, so the corpus is
    byte-identical between a history that carries the signal and one that does
    not, and a claim lost at extraction has nothing to stamp. The loud per-arm
    scope guard runs here too — a signal the history did not declare fails the
    publish rather than silently settling a bucket.
    """
    out: list[Any] = []
    for fact in facts:
        stamp = stamps.get(fact.node_id)
        if stamp and fact.subtype == "consistency_claim":
            out.append(replace(fact, attrs={**fact.attrs, **stamp}))
        else:
            out.append(fact)
    assert_arm_signal_scope(
        (f for f in out if f.subtype == "consistency_claim"), tuple(admits)
    )
    return out


# --------------------------------------------------------------------------- #
# The per-step leakage scan                                                    #
# --------------------------------------------------------------------------- #
def step_leakage_violations(
    manifest: Mapping[str, Any], step: Any, recorded: Sequence[Mapping[str, Any]]
) -> list[str]:
    """No gold surface enters the engine at this step.

    Two surfaces, scanned by two rules, because they are sanctioned differently.

    * The INGEST STAMPS are the only thing a history authors that the engine
      reads directly, so they are scanned against the FULL forbidden token set
      (record identifiers, digit-bearing name tokens, canaries). A fact id or a
      patient identifier reaching a stamp would put gold inside the engine.
    * The published fact set is a CORPUS-DERIVED surface: it carries patient
      names and record identifiers because the corpus does (a ``record_reference``
      line renders one), and scanning it against those tokens would fail by
      construction and teach nothing. It is scanned against the CANARIES, which
      are planted in the manifest and never rendered — so a canary appearing in
      published output is a leak by construction and nothing else.

    The DELTA PROGRAM is deliberately not scanned: it names corruption ids and
    the sites they target, so it IS sealed gold. It is never handed to the
    engine, and scanning gold against itself would fail by construction.
    """
    from okg.bench.synth.goldseal import scan_unsanctioned_surface
    from okg.bench.synth.render import pack_bytes

    surfaces = {
        f"history_step_{step.index:02d}_stamps": json.dumps(
            {k: dict(v) for k, v in step.stamps.items()}, sort_keys=True
        ),
        "template_pack": pack_bytes().decode("utf-8"),
    }
    violations = scan_unsanctioned_surface(manifest, surfaces)
    published = json.dumps(list(recorded), sort_keys=True)
    for canary in manifest.get("canary_tokens", ()):
        if canary in published:
            violations.append(
                f"published_facts_step_{step.index:02d}: contains canary {canary!r} - "
                "a manifest-only token reached the engine"
            )
    return violations


# --------------------------------------------------------------------------- #
# Corpus materialization                                                       #
# --------------------------------------------------------------------------- #
def materialize_base(history: History, corpus_dir: Path) -> None:
    corpus_dir.mkdir(parents=True, exist_ok=True)
    for doc, text in sorted(history.base_docs.items()):
        (corpus_dir / doc).write_text(text, encoding="utf-8")


def materialize_step(history: History, index: int, corpus_dir: Path) -> list[str]:
    """Write ONLY the documents this update touches. Returns the docs written."""
    step = history.steps[index]
    for doc in step.touched_docs:
        (corpus_dir / doc).write_text(step.doc_texts[doc], encoding="utf-8")
    return list(step.touched_docs)


def corpus_digests_on_disk(corpus_dir: Path) -> dict[str, str]:
    import hashlib

    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(Path(corpus_dir).glob("*.md"))
    }


# --------------------------------------------------------------------------- #
# The realized accepted state                                                  #
# --------------------------------------------------------------------------- #
def accepted_state(resolutions: Sequence[Any]) -> dict[str, dict[str, Any]]:
    """bucket -> {outcome, winner_value, basis} from the engine's own rows.

    The comparison unit is the resolved VALUE, not the winning claim id: ``≻``
    ranks CLAIMS but the question a bucket asks is which VALUE stands, and two
    claims restating one value are interchangeable answers to it.
    """
    out: dict[str, dict[str, Any]] = {}
    for res in resolutions:
        winner = res.winner_value
        out[res.bucket] = {
            "outcome": res.outcome,
            "winner_value": canonicalize_value(winner) if winner is not None else None,
            "basis": res.basis,
        }
    return out


def state_diff(
    before: Mapping[str, Mapping[str, Any]], after: Mapping[str, Mapping[str, Any]]
) -> dict[str, dict[str, Any]]:
    """The realized accepted-state diff between two steps (the changed buckets)."""
    out: dict[str, dict[str, Any]] = {}
    for bucket in sorted(set(before) | set(after)):
        a, b = before.get(bucket), after.get(bucket)
        if a != b:
            out[bucket] = {
                "before": dict(a) if a is not None else None,
                "after": dict(b) if b is not None else None,
            }
    return out


def compare_step(
    *,
    index: int,
    gold_state: Mapping[str, Mapping[str, Any]],
    gold_diff: Mapping[str, Any],
    realized_state: Mapping[str, Mapping[str, Any]],
    realized_diff: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    """Every disagreement between gold and the realized accepted state, named."""
    problems: list[str] = []
    for bucket in sorted(set(gold_state) | set(realized_state)):
        gold_cell = gold_state.get(bucket)
        live_cell = realized_state.get(bucket)
        if gold_cell != live_cell:
            problems.append(
                f"update {index} bucket {bucket}: gold expects {gold_cell}, "
                f"the engine produced {live_cell}"
            )
    gold_changed = {b: dict(cell) for b, cell in gold_diff["changed"].items()}
    for bucket in sorted(set(gold_changed) | set(realized_diff)):
        expected = gold_changed.get(bucket)
        realized = realized_diff.get(bucket)
        if expected is None:
            problems.append(
                f"update {index} bucket {bucket}: gold declared it UNCHANGED, the "
                f"engine changed it {realized['before']} -> {realized['after']}"
            )
        elif realized is None:
            problems.append(
                f"update {index} bucket {bucket}: gold declared it CHANGED "
                f"{expected['before']} -> {expected['after']}, the engine left it alone"
            )
        elif (expected["before"], expected["after"]) != (
            realized["before"],
            realized["after"],
        ):
            problems.append(
                f"update {index} bucket {bucket}: gold expected "
                f"{expected['before']} -> {expected['after']}, the engine produced "
                f"{realized['before']} -> {realized['after']}"
            )
    return problems


# --------------------------------------------------------------------------- #
# Trajectories + the no-spontaneous-regression property                        #
# --------------------------------------------------------------------------- #
def build_trajectories(
    step_states: Sequence[Mapping[str, str]],
) -> dict[str, dict[str, Any]]:
    """corruption_id -> {sequence, terminal_state, transitions}.

    ``step_states`` is the per-update funnel's ``per_corruption`` map; a
    corruption absent from an update's map was not live then, which is itself
    part of the trajectory (planted at 3, repaired at 5, corrected away at 11).
    """
    ids = sorted({cid for states in step_states for cid in states})
    out: dict[str, dict[str, Any]] = {}
    for cid in ids:
        sequence = [
            {"update": index, "state": states[cid]}
            for index, states in enumerate(step_states)
            if cid in states
        ]
        transitions = []
        previous = None
        for entry in sequence:
            if entry["state"] != previous:
                transitions.append(
                    {
                        "update": entry["update"],
                        "from": previous or "absent",
                        "to": entry["state"],
                    }
                )
                previous = entry["state"]
        last_live = sequence[-1]["update"]
        removed = last_live < len(step_states) - 1
        out[cid] = {
            "sequence": sequence,
            "planted_at": sequence[0]["update"],
            "last_live_at": last_live,
            "corrected_away": removed,
            "terminal_state": "corrected_away" if removed else sequence[-1]["state"],
            "transitions": transitions,
        }
    return out


def no_spontaneous_regression(
    *,
    trajectories: Mapping[str, Mapping[str, Any]],
    corruption_buckets: Mapping[str, str],
    touched_buckets: Sequence[frozenset[str]],
    expected_states: Sequence[Mapping[str, Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    """THE falsifiable maintenance property, checked on every replay.

    A corruption repaired at update k, whose bucket NO later update touches, must
    still be repaired at every later update. A spontaneous un-repair means an
    unrelated document changed a settled answer — either an unsound reuse or a
    scoring defect — and this report says WHICH by naming the update, the bucket,
    and whether the reference (gold's expected accepted state, computed without
    running the engine) agrees the answer should not have moved.

    Independent of per-update parity and cheaper: it needs no reference
    EXECUTION, so it holds where a reference run is too expensive.
    """
    violations: list[dict[str, Any]] = []
    for cid, trajectory in sorted(trajectories.items()):
        bucket = corruption_buckets.get(cid, "")
        # ``anchor`` is the most recent update at which this corruption read
        # REPAIRED with nothing having touched its bucket since. A touch clears
        # the anchor (a changed answer there is work, not a regression) and a
        # later repair re-arms it, so a corruption that is repaired, legitimately
        # re-broken, and repaired again is still protected afterwards.
        anchor: int | None = None
        for entry in trajectory["sequence"]:
            index = entry["update"]
            state = entry["state"]
            touched_here = bucket in touched_buckets[index]
            regressed = (
                anchor is not None and not touched_here and state != STATE_REPAIRED
            )
            repaired_at, anchor = anchor, (index if state == STATE_REPAIRED else None)
            if not regressed:
                continue
            reference_agrees = expected_states[index].get(bucket) == expected_states[
                repaired_at
            ].get(bucket)
            if reference_agrees:
                attribution = (
                    "the reference AGREES the answer should not have moved, so the "
                    "engine moved a settled answer nothing touched - an unsound "
                    "reuse or a scoring defect"
                )
            else:
                attribution = (
                    "the reference answer ALSO moved for this untouched bucket, so "
                    "the defect is in scoring or in the reference model itself, not "
                    "in reuse"
                )
            violations.append(
                {
                    "corruption_id": cid,
                    "bucket": bucket,
                    "repaired_at": repaired_at,
                    "update": index,
                    "state": entry["state"],
                    "reference_agrees": reference_agrees,
                    "verdict": VERDICT_UNSOUND if reference_agrees else VERDICT_GOLD_DISAGREES,
                    "detail": (
                        f"corruption {cid} was repaired at update {repaired_at}; no "
                        f"update since touched bucket {bucket}, yet at update {index} "
                        f"it reads {entry['state']}. Attribution: {attribution}."
                    ),
                }
            )
    return violations


# --------------------------------------------------------------------------- #
# The replay                                                                   #
# --------------------------------------------------------------------------- #
def replay_history(
    *,
    dsn: str,
    deployment_dir: Path,
    history: History,
    manifest: Mapping[str, Any],
    out_dir: Path,
    lane: Any = None,
) -> dict[str, Any]:
    """Replay every update through the DIRECT publish path (full republish, no
    reuse) and return the per-step report.

    The direct path is deliberate: the expected diff is a claim about what a FULL
    redo produces, so the witness that gold matches reality must not go through a
    mechanism that could skip work. The scheduler-governed comparison is the
    consumer's job (concord-exact-parity), and it consumes the SAME recorded fact
    sets this replay writes.
    """
    broken = verify_chain(history)
    if broken:
        raise HistoryReplayError(
            f"refusing to replay a history whose gold chain is broken: {broken}"
        )
    out_dir = Path(out_dir)
    corpus_dir = out_dir / "corpus"
    facts_dir = out_dir / RECORDED_FACTS_DIRNAME
    facts_dir.mkdir(parents=True, exist_ok=True)
    materialize_base(history, corpus_dir)
    # The artifact set is SELF-CONTAINED: the sealed history, the recorded fact
    # sets, and the report. A consumer (concord-exact-parity) replays the same
    # updates from these files alone — it never re-generates gold and never
    # re-extracts, which is what makes its parity verdicts attributable to reuse.
    (out_dir / HISTORY_FILENAME).write_text(
        json.dumps(history_to_json(history), sort_keys=True, indent=1) + "\n",
        encoding="utf-8",
    )

    extra_policies, excluded = constraint_policy_facts(manifest)
    policies = functional_policies(manifest, exclude=excluded)

    def _equivalent(a: str, b: str) -> bool:
        return synth_value_equivalent(canonicalize_value(a), canonicalize_value(b))

    previous_ids: frozenset[str] = frozenset()
    prior_state: dict[str, dict[str, Any]] = {}
    steps: list[dict[str, Any]] = []
    step_states: list[dict[str, str]] = []
    problems: list[str] = []
    leakage_scanned = 0

    for step in history.steps:
        written = materialize_step(history, step.index, corpus_dir)
        on_disk = corpus_digests_on_disk(corpus_dir)
        if on_disk != dict(step.corpus_digests):
            raise HistoryReplayError(
                f"update {step.index}: the materialized corpus does not match the "
                f"sealed digests; gold and the corpus are out of step"
            )

        # --- extraction runs ONCE, and is recorded ---------------------------
        facts = extract_facts(
            corpus_dir, policies, lane=lane, phase=f"history-{step.index:02d}"
        )
        facts = apply_history_stamps(facts, step.stamps, history.admits)
        facts = list(facts) + list(extra_policies)
        recorded = serialize_facts(facts)
        (facts_dir / f"step_{step.index:02d}.json").write_text(
            json.dumps(recorded, sort_keys=True, indent=1) + "\n", encoding="utf-8"
        )
        leaks = step_leakage_violations(manifest, step, recorded)
        if leaks:
            raise HistoryReplayError(
                f"update {step.index}: gold leaked onto an engine-visible "
                f"surface: {leaks}"
            )
        leakage_scanned += 1

        # --- every executor consumes the RECORDED facts, never a re-extract ---
        out, previous_ids = publish_fact_list(
            dsn, deployment_dir, deserialize_facts(recorded), previous_ids
        )
        state = detection_state(dsn, out["generation_id"])
        resolutions = resolutions_at(dsn, out["generation_id"], state.claims)
        realized_state = accepted_state(resolutions)
        realized_diff = state_diff(prior_state, realized_state)

        problems.extend(
            compare_step(
                index=step.index,
                gold_state=step.expected_state,
                gold_diff=step.expected_diff,
                realized_state=realized_state,
                realized_diff=realized_diff,
            )
        )

        live_manifest = {
            **manifest,
            "gold": {
                **manifest["gold"],
                "corruptions": [
                    site
                    for site in manifest["gold"]["corruptions"]
                    if site["corruption_id"] in set(step.live_corruptions)
                ],
            },
        }
        step_funnel = funnel(
            manifest=live_manifest,
            extracted_claims=state.claims,
            resolutions=resolutions,
            findings=state.findings,
            equivalent=_equivalent,
        )
        step_states.append(dict(step_funnel["per_corruption"]))
        steps.append(
            {
                "update": step.index,
                "delta": step.delta.as_json(),
                "generation_id": str(out["generation_id"]),
                "touched_docs": written,
                "facts_digest": facts_digest(recorded),
                "published_claims": len(state.claims),
                "live_corruptions": list(step.live_corruptions),
                "funnel_states": step_funnel["states"],
                "funnel_total": step_funnel["total"],
                "realized_state": realized_state,
                "realized_diff": realized_diff,
                "expected_diff": {
                    "changed": sorted(step.expected_diff["changed"]),
                    "skippable": list(step.expected_diff["skippable"]),
                    "touched_buckets": list(step.expected_diff["touched_buckets"]),
                },
            }
        )
        prior_state = realized_state

    trajectories = build_trajectories(step_states)
    corruption_buckets = _corruption_buckets(manifest)
    regressions = no_spontaneous_regression(
        trajectories=trajectories,
        corruption_buckets=corruption_buckets,
        touched_buckets=[
            frozenset(s.expected_diff["touched_buckets"]) for s in history.steps
        ],
        expected_states=[s.expected_state for s in history.steps],
    )
    report = {
        "schema": REPLAY_SCHEMA,
        "history_digest": history.history_digest,
        "config_digest": history.config_digest,
        "manifest_digest": history.manifest_digest,
        "execution": "direct",
        "updates": len(steps),
        "steps": steps,
        "trajectories": trajectories,
        "expected_diff_problems": problems,
        "leakage_scanned_steps": leakage_scanned,
        "no_spontaneous_regression": {
            "checked": len(trajectories),
            "violations": regressions,
        },
        "skippable_steps": [
            s.index for s in history.steps if s.expected_diff["skippable"]
        ],
    }
    (out_dir / REPLAY_FILENAME).write_text(
        json.dumps(report, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )
    return report


def _corruption_buckets(manifest: Mapping[str, Any]) -> dict[str, str]:
    from okg.bench.synth.resolution import corruption_bucket_map

    return corruption_bucket_map(manifest)


def replay_recorded_step(
    *,
    dsn: str,
    deployment_dir: Path,
    recorded_path: Path,
    previous_ids: frozenset[str] = frozenset(),
) -> tuple[dict[str, Any], dict[str, str]]:
    """Publish ONE recorded fact set verbatim and return (publish, claim map).

    The surface a second executor uses: it never sees the corpus, only the facts
    the first extraction recorded, so two executors cannot disagree because their
    extractors did.
    """
    rows = json.loads(Path(recorded_path).read_text(encoding="utf-8"))
    out, ids = publish_fact_list(
        dsn, deployment_dir, deserialize_facts(rows), previous_ids
    )
    state = detection_state(dsn, out["generation_id"])
    return out, dict(state.claims)


__all__ = [
    "HISTORY_FILENAME",
    "REPLAY_FILENAME",
    "REPLAY_SCHEMA",
    "RECORDED_FACTS_DIRNAME",
    "VERDICT_GOLD_DISAGREES",
    "VERDICT_UNSOUND",
    "HistoryReplayError",
    "accepted_state",
    "apply_history_stamps",
    "build_trajectories",
    "compare_step",
    "corpus_digests_on_disk",
    "deserialize_facts",
    "facts_digest",
    "materialize_base",
    "materialize_step",
    "no_spontaneous_regression",
    "replay_history",
    "replay_recorded_step",
    "serialize_facts",
    "state_diff",
]
