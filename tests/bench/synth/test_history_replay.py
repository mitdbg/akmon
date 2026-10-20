"""task.hist.expected-diff + task.hist.trajectory witnesses: the REAL replay.

One generated history is replayed through the DIRECT publish path — a full
republish per update, no reuse — against a dedicated database. Per step the
witness asserts the realized accepted state equals gold's expected state and the
realized diff equals gold's expected diff; the funnel is exhaustive over the
corruptions LIVE at that update; and the no-spontaneous-regression property holds
over every repaired-and-since-untouched bucket.

The non-vacuity witnesses are pure and cheap: a deliberately wrong expected
winner must fail the comparison NAMING the bucket, and a constructed regression
(a recorded state sequence mutated to un-repair an untouched bucket) must fire
the property naming update, bucket, and whether the reference agrees.
"""

from __future__ import annotations

import copy
import dataclasses
import json
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.corruption import Planner  # noqa: E402
from okg.bench.synth.goldseal import build_manifest  # noqa: E402
from okg.bench.synth.history import (  # noqa: E402
    DELTA_DOMINATED_CLAIM,
    DELTA_REAFFIRMATION,
    GOLD_PRECEDENCE_ORDER,
    SIG_SOURCE_AUTHORITY,
    SIG_SUPPORT,
    HistoryConfig,
    build_history,
    history_from_json,
    history_to_json,
    verify_chain,
)
from okg.bench.synth.history_replay import (  # noqa: E402
    HISTORY_FILENAME,
    RECORDED_FACTS_DIRNAME,
    REPLAY_FILENAME,
    VERDICT_GOLD_DISAGREES,
    VERDICT_UNSOUND,
    accepted_state,
    apply_history_stamps,
    build_trajectories,
    compare_step,
    deserialize_facts,
    no_spontaneous_regression,
    replay_history,
    serialize_facts,
    step_leakage_violations,
)
from okg.bench.synth.prf import SynthPrf  # noqa: E402
from okg.bench.synth.render import render_corpus  # noqa: E402
from okg.bench.synth.resolution import (  # noqa: E402
    FUNNEL_STATES,
    STATE_DETECTED_ONLY,
    STATE_REPAIRED,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"
# >= 20 updates: the parity child consumes this history, and its acceptance is
# stated over "a history of at least 20 updates".
_UPDATES = 22


def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    d = tmp_path / "catalog-scratch"
    d.mkdir(exist_ok=True)
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    (d / "schemas").mkdir(exist_ok=True)
    composed = compose_catalog(d)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)
    from tests.pytest.catalog_fixture_builders import apply_explicit_test_catalog
    apply_explicit_test_catalog(dsn, _DEP_DIR)


def _build(world, smoke_config, updates: int = _UPDATES):
    prf = SynthPrf(smoke_config.seed)
    planner = Planner(world, smoke_config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, smoke_config, planner, plan, prf)
    manifest = build_manifest(
        config=smoke_config, world=world, plan=plan, rendered=rendered
    )
    history = build_history(
        config=smoke_config, world=world, manifest=manifest,
        base_docs=rendered.clean_docs, history_config=HistoryConfig(updates=updates),
    )
    return history, manifest


# =========================================================================== #
# THE replay witness                                                          #
# =========================================================================== #
@pytest.mark.postgres
def test_history_replay_matches_gold_at_every_step(
    clean_runtime: str, tmp_path: Path, world, smoke_config
) -> None:
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    history, manifest = _build(world, smoke_config)
    report = replay_history(
        dsn=dsn, deployment_dir=_DEP_DIR, history=history, manifest=manifest,
        out_dir=tmp_path / "replay",
    )

    # ---- the expected diff holds at EVERY step ----------------------------
    assert report["updates"] == _UPDATES
    assert report["expected_diff_problems"] == [], (
        "the realized accepted-state diff disagrees with sealed gold; the first "
        f"five: {report['expected_diff_problems'][:5]}"
    )

    # ---- non-vacuity: the run really published, and really conflicted ------
    # An update whose facts change nothing in the graph publishes no new
    # generation: the store drops writes whose payload is unchanged and the
    # publisher reuses the previous generation. That is legitimate only when
    # neither the realized state nor the sealed gold expected a change, and
    # most updates must still publish, or the run grades nothing.
    steps = report["steps"]
    reused = [
        index for index in range(1, len(steps))
        if steps[index]["generation_id"] == steps[index - 1]["generation_id"]
    ]
    for index in reused:
        assert not steps[index]["realized_diff"], (index, steps[index]["realized_diff"])
        assert not steps[index]["expected_diff"]["changed"], (
            index, steps[index]["expected_diff"]
        )
    assert len(reused) <= _UPDATES // 4, reused
    assert len({step["generation_id"] for step in steps}) == _UPDATES - len(reused)
    assert all(step["published_claims"] > 0 for step in report["steps"])
    changed_total = sum(len(step["realized_diff"]) for step in report["steps"])
    assert changed_total >= 10, (
        f"only {changed_total} accepted-state changes over {_UPDATES} updates - a "
        "history that changes almost nothing cannot grade maintenance"
    )
    assert report["skippable_steps"], "gold declared no step skippable"
    for index in report["skippable_steps"]:
        assert report["steps"][index]["realized_diff"] == {} or set(
            report["steps"][index]["realized_diff"]
        ).isdisjoint(report["steps"][index]["expected_diff"]["skippable"])

    # ---- the four live-proof shapes really ran ----------------------------
    kinds = {step["delta"]["kind"] for step in report["steps"]}
    assert {"fresh_conflict", "bucket_increment", DELTA_REAFFIRMATION,
            DELTA_DOMINATED_CLAIM} <= kinds

    # ---- the funnel is exhaustive over the LIVE set at every update --------
    for step in report["steps"]:
        live = set(step["live_corruptions"])
        assert step["funnel_total"] == len(live), step["update"]
        assert sum(step["funnel_states"].values()) == len(live), step["update"]
        assert set(step["funnel_states"]) == set(FUNNEL_STATES)
    live_sizes = [len(s["live_corruptions"]) for s in report["steps"]]
    assert live_sizes[0] == 0 and max(live_sizes) >= 3
    assert any(a > b for a, b in zip(live_sizes, live_sizes[1:])), (
        "no corruption was ever corrected away - the live set never shrank"
    )

    # ---- trajectories: terminal state + transitions ------------------------
    trajectories = report["trajectories"]
    assert len(trajectories) >= 5
    repaired = [
        cid for cid, t in trajectories.items()
        if any(e["state"] == STATE_REPAIRED for e in t["sequence"])
    ]
    assert repaired, "no corruption ever reached repaired - the property is vacuous"
    corrected = [cid for cid, t in trajectories.items() if t["corrected_away"]]
    assert corrected, "no corruption was corrected away - no trajectory has an end"
    for cid, t in trajectories.items():
        assert t["transitions"][0]["from"] == "absent"
        assert t["terminal_state"] in set(FUNNEL_STATES) | {"corrected_away"}

    # ---- the falsifiable maintenance property ------------------------------
    assert report["no_spontaneous_regression"]["violations"] == [], (
        "a settled answer moved in a bucket no update touched: "
        f"{report['no_spontaneous_regression']['violations'][:2]}"
    )
    assert report["no_spontaneous_regression"]["checked"] >= 5

    # ---- the recorded fact sets are on disk for the parity child -----------
    facts_dir = tmp_path / "replay" / RECORDED_FACTS_DIRNAME
    recorded = sorted(facts_dir.glob("step_*.json"))
    assert len(recorded) == _UPDATES
    assert (tmp_path / "replay" / REPLAY_FILENAME).exists()
    # the artifact set is self-contained: the sealed history rehydrates and
    # verifies from the run directory alone, which is what the parity child needs
    sealed = json.loads((tmp_path / "replay" / HISTORY_FILENAME).read_text())
    rehydrated = history_from_json(sealed)
    assert verify_chain(rehydrated) == []
    assert rehydrated.history_digest == report["history_digest"]

    # ---- per-step leakage: nothing gold enters the engine ------------------
    # The scan is a RUN GATE (replay_history raises on a leak), so a completed
    # replay already proves it passed; assert it ran on every step and show it
    # can FIRE on a forged one.
    assert report["leakage_scanned_steps"] == _UPDATES
    canary = manifest["canary_tokens"][0]
    forged = dataclasses.replace(
        history.steps[0], stamps={"claim:x": {"source_authority": canary}}
    )
    assert step_leakage_violations(manifest, forged, []), "the leakage scan is inert"
    rows = json.loads(recorded[0].read_text())
    rows.append({"node_id": f"claim:{canary}", "subtype": "consistency_claim",
                 "attrs": {}, "source_record_id": {}, "source_revision": {}})
    assert any(
        "canary" in violation
        for violation in step_leakage_violations(manifest, history.steps[0], rows)
    )


@pytest.mark.postgres
def test_a_wrong_gold_precedence_model_is_caught_by_the_direct_replay(
    clean_runtime: str, tmp_path: Path, world, smoke_config
) -> None:
    """THE reason the direct-path replay exists, shown to FIRE.

    The gold-side precedence order is a deliberate independent restatement of the
    engine's, so the two can drift. Here gold is given a WRONG order —
    corroboration transposed above declared authority — and the replay must
    report the disagreement per step, naming the bucket. A witness that only ever
    ran against a correct model would be evidence of nothing.
    """
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    prf = SynthPrf(smoke_config.seed)
    planner = Planner(world, smoke_config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, smoke_config, planner, plan, prf)
    manifest = build_manifest(
        config=smoke_config, world=world, plan=plan, rendered=rendered
    )
    transposed = list(GOLD_PRECEDENCE_ORDER)
    i = transposed.index(SIG_SOURCE_AUTHORITY)
    j = transposed.index(SIG_SUPPORT)
    transposed[i], transposed[j] = transposed[j], transposed[i]
    wrong = build_history(
        config=smoke_config, world=world, manifest=manifest,
        base_docs=rendered.clean_docs,
        history_config=HistoryConfig(updates=8, precedence_order=tuple(transposed)),
    )
    report = replay_history(
        dsn=dsn, deployment_dir=_DEP_DIR, history=wrong, manifest=manifest,
        out_dir=tmp_path / "wrong",
    )
    problems = report["expected_diff_problems"]
    assert problems, "a knowingly wrong gold precedence model passed the replay"
    assert any("bucket" in p for p in problems)
    # the disagreement is about a WINNER, not about a missing row: the engine
    # settled on the authority-backed value where gold's transposed order picked
    # the corroborated one
    assert any("gold expects" in p and "the engine produced" in p for p in problems)


# =========================================================================== #
# Replay determinism, witnessed load-bearing                                  #
# =========================================================================== #
class _DriftingLane:
    """An extractor that answers the SAME corpus differently on each call.

    Not a strawman: this is what a provider-backed lane does when a retry lands
    on a different sampling path and one document's line is missed. The point of
    the recorded-fact rule is that such drift can never reach a parity verdict,
    so the rule has to be shown load-bearing against exactly this behaviour.
    """

    def __init__(self) -> None:
        self.calls = 0

    def extract(self, corpus_dir: Path, policies, *, phase: str = ""):
        from okg.bench.synth.runner import extract_facts

        self.calls += 1
        facts = extract_facts(Path(corpus_dir), tuple(policies))
        claims = [f for f in facts if f.subtype == "consistency_claim"]
        if self.calls % 2 == 0 and claims:
            dropped = max(claims, key=lambda f: f.node_id).node_id
            return [f for f in facts if f.node_id != dropped]
        return facts


@pytest.mark.postgres
def test_replay_determinism_is_load_bearing(
    clean_runtime: str, tmp_path: Path, world, smoke_config
) -> None:
    """The recorded-fact rule holds, and the path it forbids is shown able to
    differ — so the rule is evidence, not a slogan."""
    from okg.bench.synth.history_replay import (
        materialize_base,
        materialize_step,
    )
    from okg.bench.synth.runner import (
        constraint_policy_facts,
        detection_state,
        extract_facts,
        functional_policies,
        publish_fact_list,
    )

    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    history, manifest = _build(world, smoke_config, updates=4)
    corpus = tmp_path / "corpus"
    materialize_base(history, corpus)
    materialize_step(history, 0, corpus)
    extra, excluded = constraint_policy_facts(manifest)
    policies = functional_policies(manifest, exclude=excluded)

    facts = extract_facts(corpus, policies)
    facts = apply_history_stamps(facts, history.steps[0].stamps, history.admits)
    recorded = serialize_facts(list(facts) + list(extra))

    # (1) the recorded set publishes one claim map ...
    out, ids = publish_fact_list(dsn, _DEP_DIR, deserialize_facts(recorded), frozenset())
    claims_first = dict(detection_state(dsn, out["generation_id"]).claims)
    assert claims_first

    # (2) ... the FORBIDDEN path — re-extracting per executor — can differ ...
    lane = _DriftingLane()
    drift_a = extract_facts(corpus, policies, lane=lane, phase="drift-a")
    drift_b = extract_facts(corpus, policies, lane=lane, phase="drift-b")
    ids_a = {f.node_id for f in drift_a if f.subtype == "consistency_claim"}
    ids_b = {f.node_id for f in drift_b if f.subtype == "consistency_claim"}
    assert ids_a != ids_b, "the drift witness did not drift - it proves nothing"

    drift_facts = apply_history_stamps(drift_b, history.steps[0].stamps, history.admits)
    out, ids = publish_fact_list(
        dsn, _DEP_DIR, list(drift_facts) + list(extra), ids
    )
    claims_drift = dict(detection_state(dsn, out["generation_id"]).claims)
    assert claims_drift != claims_first, (
        "a re-extraction that dropped a claim published the SAME claim set; the "
        "witness cannot show the rule is load-bearing"
    )

    # (3) ... while the recorded set replays to the SAME claim map, even after
    #     an intervening publish moved the graph.
    out, ids = publish_fact_list(dsn, _DEP_DIR, deserialize_facts(recorded), ids)
    claims_again = dict(detection_state(dsn, out["generation_id"]).claims)
    assert claims_again == claims_first

    # and the recorded rows themselves round-trip byte-identically, so two
    # executors reading the same file cannot disagree about what they consumed
    assert serialize_facts(deserialize_facts(recorded)) == recorded


# =========================================================================== #
# Non-vacuity of the expected-diff comparison                                 #
# =========================================================================== #
def test_a_wrong_expected_winner_fails_the_witness_naming_the_bucket(
    world, smoke_config, tmp_path: Path
) -> None:
    """Non-vacuity: a deliberately wrong expected winner in a COPIED gold file
    fails the comparison, and the failure names the bucket."""
    history, _manifest = _build(world, smoke_config, updates=6)
    step = next(s for s in history.steps if s.expected_diff["changed"])
    bucket = sorted(step.expected_diff["changed"])[0]

    realized_state = {b: dict(cell) for b, cell in step.expected_state.items()}
    realized_diff = {
        b: {"before": cell["before"], "after": cell["after"]}
        for b, cell in step.expected_diff["changed"].items()
    }
    # the honest gold agrees with itself
    assert compare_step(
        index=step.index, gold_state=step.expected_state,
        gold_diff=step.expected_diff, realized_state=realized_state,
        realized_diff=realized_diff,
    ) == []

    # a COPY of gold on disk with one winner falsified
    copied = tmp_path / "gold-copy.json"
    copied.write_text(json.dumps(history_to_json(history), sort_keys=True))
    payload = json.loads(copied.read_text())
    bad_step = payload["steps"][step.index]
    bad_state = copy.deepcopy(bad_step["expected_state"])
    bad_diff = copy.deepcopy(bad_step["expected_diff"])
    if bucket in bad_state:
        bad_state[bucket]["winner_value"] = "WRONG-WINNER"
    bad_diff["changed"][bucket]["after"] = {
        "outcome": "canonical", "winner_value": "WRONG-WINNER", "basis": "tie",
    }
    problems = compare_step(
        index=step.index, gold_state=bad_state, gold_diff=bad_diff,
        realized_state=realized_state, realized_diff=realized_diff,
    )
    assert problems, "a falsified expected winner passed the comparison"
    assert all(f"update {step.index}" in p for p in problems)
    assert any(bucket in p and "WRONG-WINNER" in p for p in problems), problems

    # and the other direction: gold says CHANGED where the engine did nothing
    problems = compare_step(
        index=step.index, gold_state=step.expected_state,
        gold_diff=step.expected_diff, realized_state={}, realized_diff={},
    )
    assert any("the engine left it alone" in p for p in problems), problems


# =========================================================================== #
# Non-vacuity of the no-spontaneous-regression property                       #
# =========================================================================== #
def test_constructed_regression_fires_naming_update_bucket_and_reference() -> None:
    """ACCEPTANCE: a recorded state sequence mutated to un-repair an untouched
    bucket must FIRE the property, naming update, bucket, and whether the
    reference agrees."""
    bucket = "body_weight:patient_x"
    honest = [
        {},                                    # 0: not planted yet
        {"corr:c1": STATE_REPAIRED},           # 1: planted and repaired
        {"corr:c1": STATE_REPAIRED},           # 2: untouched, still repaired
        {"corr:c1": STATE_REPAIRED},           # 3: untouched, still repaired
    ]
    touched = [frozenset({bucket}), frozenset({bucket}), frozenset(), frozenset()]
    settled = {bucket: {"outcome": "canonical", "winner_value": "77.2 kg",
                        "basis": "dependency_adjusted_support"}}
    expected = [dict(settled) for _ in honest]

    assert no_spontaneous_regression(
        trajectories=build_trajectories(honest),
        corruption_buckets={"corr:c1": bucket},
        touched_buckets=touched,
        expected_states=expected,
    ) == []

    # MUTATE: update 3 un-repairs a bucket nothing has touched since update 1.
    regressed = copy.deepcopy(honest)
    regressed[3]["corr:c1"] = STATE_DETECTED_ONLY
    violations = no_spontaneous_regression(
        trajectories=build_trajectories(regressed),
        corruption_buckets={"corr:c1": bucket},
        touched_buckets=touched,
        expected_states=expected,
    )
    assert len(violations) == 1
    (violation,) = violations
    assert violation["update"] == 3
    assert violation["bucket"] == bucket
    # the anchor is the MOST RECENT untouched repair, not the first one: that is
    # the update after which nothing legitimately moved this bucket
    assert violation["repaired_at"] == 2
    assert violation["state"] == STATE_DETECTED_ONLY
    assert violation["reference_agrees"] is True
    assert violation["verdict"] == VERDICT_UNSOUND
    assert "unsound reuse or a scoring defect" in violation["detail"]

    # The OTHER attribution: when the reference answer moved too, the defect is
    # not in reuse, and the report must say so rather than blaming the scheduler.
    moved = list(expected)
    moved[3] = {bucket: {"outcome": "contested", "winner_value": None, "basis": "tie"}}
    (violation,) = no_spontaneous_regression(
        trajectories=build_trajectories(regressed),
        corruption_buckets={"corr:c1": bucket},
        touched_buckets=touched,
        expected_states=moved,
    )
    assert violation["reference_agrees"] is False
    assert violation["verdict"] == VERDICT_GOLD_DISAGREES

    # And it must NOT fire when a later update legitimately touched the bucket:
    # a changed answer there is work, not a regression.
    touched_later = [frozenset({bucket}), frozenset({bucket}), frozenset(),
                     frozenset({bucket})]
    assert no_spontaneous_regression(
        trajectories=build_trajectories(regressed),
        corruption_buckets={"corr:c1": bucket},
        touched_buckets=touched_later,
        expected_states=expected,
    ) == []

    # Re-arming: repaired, LEGITIMATELY re-broken by a touch, repaired again, then
    # un-repaired with nothing touching it. The property must still fire, anchored
    # on the SECOND repair — a corruption that was re-broken once must not lose
    # its protection for the rest of the history.
    rearmed = [
        {},
        {"corr:c1": STATE_REPAIRED},
        {"corr:c1": STATE_DETECTED_ONLY},   # a touch at 2 legitimately moved it
        {"corr:c1": STATE_REPAIRED},        # a touch at 3 settled it again
        {"corr:c1": STATE_DETECTED_ONLY},   # 4 touched nothing: a REGRESSION
    ]
    rearm_touched = [
        frozenset({bucket}), frozenset({bucket}), frozenset({bucket}),
        frozenset({bucket}), frozenset(),
    ]
    (violation,) = no_spontaneous_regression(
        trajectories=build_trajectories(rearmed),
        corruption_buckets={"corr:c1": bucket},
        touched_buckets=rearm_touched,
        expected_states=[dict(settled) for _ in rearmed],
    )
    assert (violation["repaired_at"], violation["update"]) == (3, 4)


def test_trajectories_record_planting_repair_and_removal() -> None:
    sequence = [
        {},
        {"corr:c1": "not_detected"},
        {"corr:c1": STATE_REPAIRED},
        {},
    ]
    trajectories = build_trajectories(sequence)
    trajectory = trajectories["corr:c1"]
    assert trajectory["planted_at"] == 1
    assert trajectory["last_live_at"] == 2
    assert trajectory["corrected_away"] is True
    assert trajectory["terminal_state"] == "corrected_away"
    assert [t["to"] for t in trajectory["transitions"]] == [
        "not_detected", STATE_REPAIRED,
    ]


def test_accepted_state_reads_values_not_claim_ids() -> None:
    from okg.bench.synth.resolution import BucketResolution

    rows = (
        BucketResolution("body_weight", "patient_x", "canonical",
                         "dependency_adjusted_support", "claim:a", ("claim:a",),
                         "82000 g"),
        BucketResolution("body_height", "patient_x", "contested", "tie", "", (), None),
    )
    state = accepted_state(rows)
    # the winner is canonicalized exactly as the ingest seam canonicalizes it, so
    # a benign unit-equivalent surface never reads as a different answer
    assert state["body_weight:patient_x"]["winner_value"] == "82.0 kg"
    assert state["body_height:patient_x"]["winner_value"] is None
