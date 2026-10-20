"""req.cmp.paired / req.cmp.uncertainty / req.cmp.validity.

Every gate here is proven ABLE TO FAIL: each refusal test has a control that
runs the SAME builder with only the tripping field mutated and asserts the
comparison succeeds. A test that only asserts a refusal fires cannot tell a
working gate from one that refuses everything, and a test that only asserts a
string appears in the source cannot tell a gate from a comment.

No test may pass vacuously: where a test asserts something about a population
(pairs, uncomparable instances, rendered lines), it first asserts that
population is non-empty.
"""

from __future__ import annotations

import builtins
import json
import random
from pathlib import Path
from typing import Any

import pytest

from okg.bench.ab.cli import main
from okg.bench.ab.compare import (
    VERDICT_INSUFFICIENT,
    bootstrap_ci,
    compare,
    render_paired_delta_chart,
)
from okg.bench.ab.errors import AbBenchError, ComparisonRefused

BENCHMARK = "swebench-verified"
# 12 instances: above the default floor of 10, so a two-instance loss still
# leaves a comparison that runs (the floor must be tripped deliberately, not
# incidentally by every other test).
IDS = [f"i{k:02d}" for k in range(12)]


# -- synthetic run directories -----------------------------------------


def _make_run(
    runs_root: Path,
    run_id: str,
    arm: str,
    *,
    scores: dict[str, float | None],
    ordering: list[str] | None = None,
    recorded: list[str] | None = None,
    benchmark: str = BENCHMARK,
    model: str = "sonnet",
    schema_version: str = "okg-bench-ab.v1",
    dataset_ref: dict[str, Any] | None = None,
    judge: dict[str, Any] | None = None,
    score_kind: str = "resolve",
    corpus: dict[str, Any] | None = None,
    ordering_mode: str = "pilot",
    drop_manifest_keys: tuple[str, ...] = (),
) -> Path:
    """One run directory: manifest + task records + scores.json.

    ``scores`` maps instance_id -> value; ``None`` writes a score record that
    declines to score (with a reason). An id ABSENT from ``scores`` gets no
    score record at all — the two are different and the comparison must say so.
    """
    ordering = list(IDS if ordering is None else ordering)
    recorded = list(ordering if recorded is None else recorded)
    run_dir = runs_root / run_id
    (run_dir / "tasks").mkdir(parents=True)
    manifest = {
        "schema_version": schema_version,
        "run_id": run_id,
        "created_at": "2026-08-04T00:00:00Z",
        "benchmark": benchmark,
        "arm": arm,
        "model": model,
        "claude_cli_version": "2.1.195 (Claude Code)",
        "framework_git_sha": None,
        "dataset_ref": (
            {"dataset": "d", "revision": "r"} if dataset_ref is None
            else dataset_ref
        ),
        "subset": None,
        "pilot": True,
        "limit": None,
        "prompt_digest": "p",
        "ordering": {"mode": ordering_mode, "instance_ids": ordering},
        "budgets": {"task_timeout_s": 1800.0},
        "native_memory_mode": None,
        "judge": judge,
        "corpus": corpus,
        "argv": [],
    }
    for key in drop_manifest_keys:
        manifest.pop(key)
    (run_dir / "manifest.json").write_text(json.dumps(manifest))

    for index, iid in enumerate(recorded):
        record = {
            "task_index": index,
            "instance": {"instance_id": iid},
            "result": {
                "instance_id": iid, "arm": arm, "status": "completed",
                "answer": "", "patch": None, "session_id": None, "error": None,
                "detail": {},
                "metrics": {
                    "input_tokens": 1000, "output_tokens": 100,
                    "cache_read_tokens": 0, "cache_creation_tokens": 0,
                    "tool_calls": 7, "wall_s": 30.0, "num_turns": 5,
                    "cost_usd": None,
                },
            },
            "between": {},
            "recorded_at": "2026-08-04T00:00:00Z",
        }
        (run_dir / "tasks" / f"{iid}.json").write_text(json.dumps(record))

    (run_dir / "scores.json").write_text(
        json.dumps(
            [
                {
                    "instance_id": iid,
                    "value": value,
                    "kind": score_kind,
                    "detail": {},
                    "unscored_reason": None if value is not None else "judge timeout",
                }
                for iid, value in scores.items()
            ]
        )
    )
    return run_dir


def _corpus(
    tmp_path: Path,
    name: str = "bench-repo-history",
    *,
    read_generation: str = "gen-new",
    root_name: str | None = None,
) -> dict[str, Any]:
    """The corpus record a run's manifest carries — a copy of the corpus.json
    the okg arm read, exactly as `runner._load_corpus_config` stores it.

    The corpus.json is ALSO written into the corpus root, because that is what
    the builders leave on disk and the vacuity test needs it there: it always
    declares the same generation the run recorded, which is why reading it back
    can never detect staleness.
    """
    root = tmp_path / "corpora" / (root_name or name)
    root.mkdir(parents=True, exist_ok=True)
    record = {
        "deployment_name": name,
        "generation_id": read_generation,
        "corpus_root": str(root),
        "dsn": "postgresql:///bench",
    }
    (root / "corpus.json").write_text(json.dumps(record))
    return record


def _declare(path: Path, name: str, generation: str) -> Path:
    """A corpus.json declaring the generation currently in force — the
    EXTERNAL statement `--declared-corpus` supplies. It is a separate file from
    any run's corpus_root on purpose: a declaration a run wrote about itself
    proves nothing about that run."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"deployment_name": name, "generation_id": generation})
    )
    return path


def _corpus_and_declaration(
    tmp_path: Path,
    name: str = "bench-repo-history",
    *,
    generation: str = "gen-new",
) -> tuple[dict[str, Any], Path]:
    """A corpus record and the matching CURRENT declaration, for tests that
    build their runs directly instead of through `_pair_of_runs`."""
    corpus = _corpus(tmp_path, name, read_generation=generation)
    return corpus, _declare(tmp_path / f"declared-{name}.json", name, generation)


def _pair_of_runs(
    tmp_path: Path,
    *,
    baseline_scores: dict[str, float | None] | None = None,
    against_scores: dict[str, float | None] | None = None,
    declared_generation: str | None = None,
    **against_kwargs: Any,
) -> tuple[Path, Path, Path | None]:
    """The control configuration every mutation test starts from: a fresh arm
    and an okg arm, same benchmark/model/ordering, a real effect in the okg
    arm's favour, and the declared corpus the okg arm must be checked against.

    Returns ``(baseline, against, declared)``; ``declared`` is None only when
    the caller suppressed the okg arm's corpus record. Passing
    ``declared_generation`` states a generation OTHER than the one the run
    read — the superseded case.
    """
    runs = tmp_path / "runs"
    baseline = _make_run(
        runs, "r-fresh", "fresh",
        scores=baseline_scores or dict.fromkeys(IDS, 0.0),
    )
    # NOT setdefault: its argument is evaluated eagerly, and `_corpus` WRITES
    # files — an eager call would clobber a corpus a caller staged deliberately.
    if "corpus" not in against_kwargs:
        against_kwargs["corpus"] = _corpus(tmp_path)
    corpus = against_kwargs["corpus"]
    against = _make_run(
        runs, "r-okg", "okg",
        scores=against_scores or dict.fromkeys(IDS, 1.0),
        **against_kwargs,
    )
    declared = None
    if corpus and corpus.get("deployment_name"):
        declared = _declare(
            tmp_path / "declared" / "current.json",
            corpus["deployment_name"],
            declared_generation or corpus.get("generation_id") or "gen-new",
        )
    return baseline, against, declared


def _cmp(baseline: Path, against: Path, declared: Path | None = None, **kwargs: Any):
    """`compare` with the test defaults: a small resample count, and the
    declared corpus `_pair_of_runs` staged (an okg arm has no comparison
    without one)."""
    kwargs.setdefault("resamples", 2000)
    if declared is not None and "declared_corpora" not in kwargs:
        kwargs["declared_corpora"] = [declared]
    return compare(baseline, against, **kwargs)


# -- req.cmp.paired -----------------------------------------------------


def test_pairs_by_instance_and_counts_wins(tmp_path):
    """The control: a comparison that runs, over a non-empty population."""
    # okg wins 7, loses 3, ties 2.
    baseline_vals = dict.fromkeys(IDS, 0.5)
    against_vals = dict(baseline_vals)
    for iid in IDS[:7]:
        against_vals[iid] = 1.0
    for iid in IDS[7:10]:
        against_vals[iid] = 0.0
    result = _cmp(*_pair_of_runs(
        tmp_path, baseline_scores=baseline_vals, against_scores=against_vals
    ))
    assert result.n_pairs == 12
    assert result.uncomparable == ()
    assert (result.against_wins, result.baseline_wins, result.ties) == (7, 3, 2)
    assert result.mean_delta == pytest.approx((7 * 0.5 - 3 * 0.5) / 12)
    # The delta is per-instance, not a difference of means computed elsewhere.
    assert {p.instance_id for p in result.pairs} == set(IDS)
    assert all(
        p.delta == pytest.approx(against_vals[p.instance_id]
                                 - baseline_vals[p.instance_id])
        for p in result.pairs
    )


def test_unpairable_instances_are_reported_not_dropped(tmp_path):
    """An instance scored in one arm and not the other is NAMED, with its
    reason — never folded into a mean over a quietly smaller set."""
    baseline_vals: dict[str, float | None] = dict.fromkeys(IDS, 0.0)
    against_vals: dict[str, float | None] = dict.fromkeys(IDS, 1.0)
    against_vals["i03"] = None            # scored record, declined
    del against_vals["i04"]               # no score record at all
    baseline, against, declared = _pair_of_runs(
        tmp_path,
        baseline_scores=baseline_vals,
        against_scores=against_vals,
        recorded=[i for i in IDS if i != "i05"],  # task never ran
    )
    # BOTH floors are lowered so THIS test is about pairing, not about either
    # floor (each has its own test); 9 of 12 would otherwise trip min_pairs=10
    # and then the 80% coverage floor.
    result = _cmp(baseline, against, declared, min_pairs=6, min_coverage=0.5)

    assert result.n_pairs == 9
    assert len(result.uncomparable) == 3
    by_id = {p.instance_id: p for p in result.uncomparable}
    assert set(by_id) == {"i03", "i04", "i05"}
    assert "judge timeout" in by_id["i03"].reason
    assert "no score record" in by_id["i04"].reason
    assert "no task record" in by_id["i05"].reason
    for pair in result.uncomparable:
        assert pair.delta is None
        assert not pair.comparable

    # The mean is over the 9 REPORTED pairs, and the 3 are visible in the
    # rendered output rather than absorbed.
    assert result.mean_delta == pytest.approx(1.0)
    rendered = result.render()
    assert rendered
    for iid in ("i03", "i04", "i05"):
        assert f"{iid}: UNCOMPARABLE" in rendered
    assert "uncomparable: 3" in rendered


def test_instance_recorded_outside_the_shared_ordering_is_surfaced(tmp_path):
    """A task record nobody's ordering accounts for is reported, not skipped."""
    runs = tmp_path / "runs"
    corpus, declared = _corpus_and_declaration(tmp_path)
    baseline = _make_run(runs, "r-fresh", "fresh", scores=dict.fromkeys(IDS, 0.0))
    stray = "i99"
    against = _make_run(
        runs, "r-okg", "okg",
        scores={**dict.fromkeys(IDS, 1.0), stray: 1.0},
        recorded=[*IDS, stray],
        corpus=corpus,
    )
    result = _cmp(baseline, against, declared)
    reasons = {p.instance_id: p.reason for p in result.uncomparable}
    assert reasons, "expected at least one uncomparable instance"
    assert stray in reasons
    assert "absent from the manifest ordering" in reasons[stray]
    assert result.n_pairs == 12


def test_too_few_pairs_refuses(tmp_path):
    """Mutation of the control: drop pairs below the floor and it refuses;
    the same builder above the floor returns a comparison."""
    against_vals: dict[str, float | None] = dict.fromkeys(IDS, 1.0)
    for iid in IDS[:3]:                     # 12 - 3 = 9 pairs, floor is 10
        against_vals[iid] = None
    baseline, against, declared = _pair_of_runs(tmp_path, against_scores=against_vals)
    with pytest.raises(ComparisonRefused) as excinfo:
        _cmp(baseline, against, declared)
    assert "9 instance pair(s)" in str(excinfo.value)
    assert "floor is 10" in str(excinfo.value)
    # The refusal itself names what could not be paired.
    assert "judge timeout" in str(excinfo.value)
    # ... and the gate is not simply always-on. min_coverage is lowered too:
    # 9 of 12 is 75%, so leaving it at its default would make this line pass
    # for the WRONG reason -- a coverage refusal reading as a min_pairs one.
    assert _cmp(
        baseline, against, declared, min_pairs=9, min_coverage=0.5
    ).n_pairs == 9


def test_min_pairs_below_two_is_not_a_floor(tmp_path):
    baseline, against, declared = _pair_of_runs(tmp_path)
    with pytest.raises(ComparisonRefused, match="not a floor"):
        _cmp(baseline, against, declared, min_pairs=1)


# -- req.cmp.uncertainty ------------------------------------------------


def test_straddling_interval_is_insufficient_evidence(tmp_path):
    """Deltas centred on zero yield INSUFFICIENT EVIDENCE — never 'slightly
    ahead'. The control mutation (a real, consistent effect) yields AHEAD, so
    the verdict is discriminating rather than constant."""
    # Six +0.5, six -0.5: mean 0, interval must contain 0.
    baseline_vals = dict.fromkeys(IDS, 0.5)
    against_vals = {
        iid: (1.0 if k % 2 == 0 else 0.0) for k, iid in enumerate(IDS)
    }
    straddle = _cmp(*_pair_of_runs(
        tmp_path / "a", baseline_scores=baseline_vals, against_scores=against_vals
    ))
    assert straddle.mean_delta == pytest.approx(0.0)
    assert straddle.ci_low <= 0.0 <= straddle.ci_high
    assert straddle.insufficient_evidence
    assert straddle.verdict.startswith(VERDICT_INSUFFICIENT)
    assert "ahead" not in straddle.verdict.lower()
    assert VERDICT_INSUFFICIENT in straddle.render()

    # Mutation: make the effect real and consistent.
    ahead = _cmp(*_pair_of_runs(
        tmp_path / "b",
        baseline_scores=dict.fromkeys(IDS, 0.0),
        against_scores=dict.fromkeys(IDS, 1.0),
    ))
    assert not ahead.insufficient_evidence
    assert ahead.ci_low > 0.0
    assert "AHEAD OF" in ahead.verdict
    assert VERDICT_INSUFFICIENT not in ahead.verdict


def test_a_small_mean_gap_is_not_reported_as_a_result(tmp_path):
    """The 20-instance-pilot failure mode: 0.42 vs 0.33 on 12 noisy instances
    is inside the interval, and must be stated as such."""
    baseline_vals = {iid: v for iid, v in zip(
        IDS, [1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0])}
    against_vals = {iid: v for iid, v in zip(
        IDS, [1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0])}
    result = _cmp(*_pair_of_runs(
        tmp_path, baseline_scores=baseline_vals, against_scores=against_vals
    ))
    assert result.mean_delta > 0.0            # okg "ahead" on the raw means
    assert result.insufficient_evidence       # but not distinguishable from 0
    assert VERDICT_INSUFFICIENT in result.verdict


def test_bootstrap_is_seeded_and_reproducible(tmp_path):
    # Deltas must VARY, or the resampled means land on a lattice so coarse
    # that every seed yields the same percentiles and the test would "prove"
    # reproducibility on data no seed could disturb.
    deltas = [0.02, 0.13, -0.07, 0.44, 0.21, -0.15,
              0.31, 0.08, 0.55, -0.03, 0.17, 0.26]
    baseline_vals = dict.fromkeys(IDS, 0.2)
    against_vals = {iid: round(0.2 + d, 4) for iid, d in zip(IDS, deltas)}
    assert len(set(deltas)) == len(IDS)
    baseline, against, declared = _pair_of_runs(
        tmp_path, baseline_scores=baseline_vals, against_scores=against_vals
    )
    first = _cmp(baseline, against, declared, seed=7)
    second = _cmp(baseline, against, declared, seed=7)
    assert (first.ci_low, first.ci_high) == (second.ci_low, second.ci_high)
    assert first.verdict == second.verdict
    assert first.seed == 7
    assert "seed=7" in first.render()
    assert first.as_dict()["bootstrap"] == {"seed": 7, "resamples": 2000}

    # The seed is load-bearing: a different one resamples differently. (If it
    # did not, the recorded seed would be decoration.)
    other = _cmp(baseline, against, declared, seed=99)
    assert (other.ci_low, other.ci_high) != (first.ci_low, first.ci_high)


def test_interval_is_over_paired_differences_not_over_each_arm(tmp_path):
    """A constant per-instance gain under wide between-instance spread is a
    finding when paired and noise when not. The implementation must be on the
    paired side of that line."""
    baseline_vals = {iid: round(k * 0.08, 4) for k, iid in enumerate(IDS)}
    against_vals = {iid: round(v + 0.08, 4) for iid, v in baseline_vals.items()}
    result = _cmp(*_pair_of_runs(
        tmp_path, baseline_scores=baseline_vals, against_scores=against_vals
    ))
    assert result.n_pairs == 12
    assert result.mean_delta == pytest.approx(0.08)
    assert result.ci_low == pytest.approx(0.08)
    assert result.ci_high == pytest.approx(0.08)
    assert not result.insufficient_evidence

    # The same data, resampled as two INDEPENDENT arms, straddles zero. This
    # is the interval the requirement forbids; asserting it differs is what
    # proves the shipped one is paired.
    rng = random.Random(7)
    b_vals = list(baseline_vals.values())
    a_vals = list(against_vals.values())
    unpaired = sorted(
        sum(rng.choices(a_vals, k=len(a_vals))) / len(a_vals)
        - sum(rng.choices(b_vals, k=len(b_vals))) / len(b_vals)
        for _ in range(2000)
    )
    assert unpaired[49] < 0.0 < unpaired[-50], (
        "the unpaired interval must straddle zero for this test to prove "
        "anything"
    )


def test_bootstrap_ci_brackets_the_mean_and_is_deterministic():
    deltas = [0.0, 0.25, -0.5, 1.0, 0.5, -0.25, 0.75, 0.0, 0.25, -0.75]
    low, high = bootstrap_ci(deltas, seed=3, resamples=2000, confidence=0.95)
    assert low < sum(deltas) / len(deltas) < high
    assert (low, high) == bootstrap_ci(
        deltas, seed=3, resamples=2000, confidence=0.95
    )
    tight = bootstrap_ci(deltas, seed=3, resamples=2000, confidence=0.50)
    assert tight[0] > low and tight[1] < high


# -- req.cmp.validity ---------------------------------------------------


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ({"benchmark": "swe-qa"}, "benchmark differs"),
        ({"model": "opus"}, "model differs"),
        ({"schema_version": "okg-bench-ab.v2"}, "schema_version differs"),
        (
            {"dataset_ref": {"dataset": "d", "revision": "r2"}},
            "dataset_ref differs",
        ),
        ({"dataset_ref": {}}, "dataset_ref is not recorded"),
        ({"ordering": [*IDS[:-1], "iZZ"]}, "ordering differs"),
        ({"ordering": list(reversed(IDS))}, "different order"),
        ({"ordering_mode": "full"}, "ordering.mode differs"),
    ],
)
def test_incomparable_runs_are_refused_naming_the_field(
    tmp_path, mutation, expected
):
    """Each mutation trips exactly one gate; the unmutated control passes."""
    control = _cmp(*_pair_of_runs(tmp_path / "control"))
    assert control.n_pairs == 12

    baseline, against, declared = _pair_of_runs(tmp_path / "mutated", **mutation)
    with pytest.raises(ComparisonRefused) as excinfo:
        _cmp(baseline, against, declared)
    assert expected in str(excinfo.value)


def test_missing_identity_field_is_refused_not_treated_as_matching(tmp_path):
    """Absence is not agreement: a manifest that never recorded its model
    cannot be shown to share one."""
    baseline, against, declared = _pair_of_runs(tmp_path, drop_manifest_keys=("model",))
    with pytest.raises(ComparisonRefused) as excinfo:
        _cmp(baseline, against, declared)
    assert "model" in str(excinfo.value)
    # Control: the same builder WITH the field compares fine.
    assert _cmp(*_pair_of_runs(tmp_path / "ok")).n_pairs == 12


def test_empty_model_string_is_refused(tmp_path):
    baseline, against, declared = _pair_of_runs(tmp_path, model="")
    with pytest.raises(ComparisonRefused, match="model is not recorded"):
        _cmp(baseline, against, declared)


def test_missing_ordering_ids_are_refused(tmp_path):
    runs = tmp_path / "runs"
    corpus, declared = _corpus_and_declaration(tmp_path)
    baseline = _make_run(runs, "r-fresh", "fresh", scores=dict.fromkeys(IDS, 0.0))
    against = _make_run(
        runs, "r-okg", "okg", scores=dict.fromkeys(IDS, 1.0), corpus=corpus,
    )
    manifest = json.loads((against / "manifest.json").read_text())
    manifest["ordering"] = {"mode": "pilot"}
    (against / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ComparisonRefused, match="instance_ids is not recorded"):
        _cmp(baseline, against, declared)


def test_run_compared_with_itself_is_refused(tmp_path):
    baseline, _, _ = _pair_of_runs(tmp_path)
    with pytest.raises(ComparisonRefused, match="with itself"):
        _cmp(baseline, baseline)


def test_missing_manifest_is_refused(tmp_path):
    baseline, against, declared = _pair_of_runs(tmp_path)
    (against / "manifest.json").unlink()
    with pytest.raises(ComparisonRefused, match="no manifest.json"):
        _cmp(baseline, against, declared)


def test_differing_judge_identity_is_refused(tmp_path):
    judge = {"judge_model": "gpt-4o", "rubric_version": "v1"}
    runs = tmp_path / "runs"
    corpus, declared = _corpus_and_declaration(tmp_path)
    baseline = _make_run(
        runs, "r-fresh", "fresh", scores=dict.fromkeys(IDS, 0.0),
        judge=judge, score_kind="llm_judge",
    )
    against = _make_run(
        runs, "r-okg", "okg", scores=dict.fromkeys(IDS, 1.0),
        judge={**judge, "rubric_version": "v2"}, score_kind="llm_judge",
        corpus=corpus,
    )
    with pytest.raises(ComparisonRefused, match="judge identity differs"):
        _cmp(baseline, against, declared)


def test_judge_scored_runs_with_no_recorded_judge_model_are_refused(tmp_path):
    """Two null judge identities are EQUAL and are not an identity.

    `judge_config()` writes `judge_model: os.environ.get(...) or None`, so a
    pair of runs judged with the env var unset compares equal on a field
    neither one filled in. Equality passing here is the exact shape this
    program keeps finding: a predicate true of the whole population.
    """
    unidentified = {"judge_model": None, "rubric_version": "v1"}
    runs = tmp_path / "runs"
    corpus = _corpus(tmp_path)
    declared = _declare(
        tmp_path / "declared.json", corpus["deployment_name"],
        corpus["generation_id"],
    )
    baseline = _make_run(
        runs, "r-fresh", "fresh", scores=dict.fromkeys(IDS, 0.0),
        judge=unidentified, score_kind="llm_judge",
    )
    against = _make_run(
        runs, "r-okg", "okg", scores=dict.fromkeys(IDS, 1.0),
        judge=unidentified, score_kind="llm_judge", corpus=corpus,
    )
    with pytest.raises(ComparisonRefused, match="records no judge_model"):
        _cmp(baseline, against, declared)

    # Mutation proving the gate is not always-on: name the judge and it runs.
    identified = {"judge_model": "gpt-4o", "rubric_version": "v1"}
    ok_runs = tmp_path / "ok"
    ok_corpus = _corpus(tmp_path, "bench-ok")
    ok_declared = _declare(
        tmp_path / "declared-ok.json", "bench-ok", ok_corpus["generation_id"]
    )
    ok_baseline = _make_run(
        ok_runs, "r-fresh", "fresh", scores=dict.fromkeys(IDS, 0.0),
        judge=identified, score_kind="llm_judge",
    )
    ok_against = _make_run(
        ok_runs, "r-okg", "okg", scores=dict.fromkeys(IDS, 1.0),
        judge=identified, score_kind="llm_judge", corpus=ok_corpus,
    )
    assert _cmp(ok_baseline, ok_against, ok_declared).n_pairs == 12


def test_unjudged_benchmarks_do_not_need_a_judge_identity(tmp_path):
    """The judge gate applies WHERE SCORING USES ONE — a resolve-scored
    benchmark with judge=None is comparable, not refused."""
    result = _cmp(*_pair_of_runs(tmp_path))
    assert result.n_pairs == 12


def test_stale_corpus_generation_is_refused(tmp_path):
    """The real case: the okg arm ran on a generation a later rebuild
    superseded (10,434 nodes with an island forge subgraph -> 23,249)."""
    baseline, against, declared = _pair_of_runs(
        tmp_path,
        corpus=_corpus(tmp_path, read_generation="gen-2317"),
        declared_generation="gen-rebuilt",
    )
    with pytest.raises(ComparisonRefused) as excinfo:
        _cmp(baseline, against, declared)
    message = str(excinfo.value)
    assert "STALE" in message
    assert "gen-2317" in message and "gen-rebuilt" in message

    # Mutation: declare the generation the run actually read, and it compares.
    fresh_baseline, fresh_against, fresh_declared = _pair_of_runs(
        tmp_path / "current",
        corpus=_corpus(tmp_path / "current", read_generation="gen-rebuilt"),
    )
    assert _cmp(fresh_baseline, fresh_against, fresh_declared).n_pairs == 12


def test_missing_declared_corpus_is_refused_not_self_compared(tmp_path):
    """The regression guard for a VACUOUS staleness check.

    The run recorded its generation FROM `<corpus_root>/corpus.json`. Reading
    that file back to check the run compares a value to itself: it passes for
    every run ever made and can only fail if someone hand-edited the file in
    between. This test stages exactly that premise — the self-declaration is
    on disk and agrees — and asserts the comparison REFUSES for want of an
    external declaration instead of quietly passing on the agreement.
    """
    corpus = _corpus(tmp_path, read_generation="gen-2317")
    baseline, against, _ = _pair_of_runs(tmp_path, corpus=corpus)
    self_declaration = Path(corpus["corpus_root"], "corpus.json")
    assert self_declaration.exists(), "the vacuity premise needs the file"
    assert (
        json.loads(self_declaration.read_text())["generation_id"]
        == corpus["generation_id"]
    ), "a run's own corpus.json always agrees with it — that is the point"

    with pytest.raises(ComparisonRefused) as excinfo:
        _cmp(baseline, against)  # no --declared-corpus
    message = str(excinfo.value)
    assert "no --declared-corpus" in message
    assert "compares a value to itself" in message

    # It is the missing DECLARATION that refuses, not anything about the runs:
    # supply one and the identical inputs compare.
    declared = _declare(
        tmp_path / "declared.json", corpus["deployment_name"], "gen-2317"
    )
    assert _cmp(baseline, against, declared).n_pairs == 12


def test_superseded_rebuild_in_a_new_corpus_root_is_refused(tmp_path):
    """The true shape of the case the requirement was written for.

    `corpus_domain.build` REFUSES to overwrite an existing corpus.json, so a
    rebuild lands in a DIFFERENT corpus_root and the original file survives —
    still on disk, still declaring the superseded generation, still agreeing
    with the run that read it. The staleness signal exists only in the second
    root, which is why the declaration has to be supplied from outside.
    """
    read = _corpus(tmp_path, read_generation="gen-2317", root_name="build-1")
    rebuilt = _declare(
        tmp_path / "corpora" / "build-2" / "corpus.json",
        read["deployment_name"],
        "gen-rebuilt",
    )
    baseline, against, _ = _pair_of_runs(tmp_path, corpus=read)

    stale_file = Path(read["corpus_root"], "corpus.json")
    assert rebuilt.parent != stale_file.parent, "the rebuild must be a new root"
    assert json.loads(stale_file.read_text())["generation_id"] == "gen-2317", (
        "the superseded root still declares the old generation — this is what "
        "a self-referential check would have read and passed on"
    )

    with pytest.raises(ComparisonRefused) as excinfo:
        _cmp(baseline, against, rebuilt)
    message = str(excinfo.value)
    assert "STALE" in message
    assert "gen-2317" in message and "gen-rebuilt" in message

    # Handing it the SUPERSEDED root's declaration passes — which is precisely
    # why the declaration may not be resolved from the run's own corpus_root.
    superseded = _declare(
        tmp_path / "superseded.json", read["deployment_name"], "gen-2317"
    )
    assert _cmp(baseline, against, superseded).n_pairs == 12


def test_each_run_is_checked_against_its_own_declared_corpus(tmp_path):
    """Two okg arms on different deployments: each is checked against the
    declaration for ITS deployment, and the refusal names the stale run."""
    runs = tmp_path / "runs"
    corpus_a = _corpus(tmp_path, "bench-a", read_generation="a1")
    corpus_b = _corpus(tmp_path, "bench-b", read_generation="b1")
    baseline = _make_run(
        runs, "r-a", "okg", scores=dict.fromkeys(IDS, 0.0), corpus=corpus_a
    )
    against = _make_run(
        runs, "r-b", "okg", scores=dict.fromkeys(IDS, 1.0), corpus=corpus_b
    )
    current_a = _declare(tmp_path / "a.json", "bench-a", "a1")
    current_b = _declare(tmp_path / "b.json", "bench-b", "b1")
    assert _cmp(
        baseline, against, declared_corpora=[current_a, current_b]
    ).n_pairs == 12

    stale_b = _declare(tmp_path / "b2.json", "bench-b", "b2")
    with pytest.raises(ComparisonRefused) as excinfo:
        _cmp(baseline, against, declared_corpora=[current_a, stale_b])
    assert "against run r-b" in str(excinfo.value)
    assert "'b1'" in str(excinfo.value) and "'b2'" in str(excinfo.value)

    stale_a = _declare(tmp_path / "a2.json", "bench-a", "a2")
    with pytest.raises(ComparisonRefused) as excinfo:
        _cmp(baseline, against, declared_corpora=[stale_a, current_b])
    assert "baseline run r-a" in str(excinfo.value)


def test_declared_corpus_for_another_deployment_is_refused(tmp_path):
    corpus = _corpus(tmp_path, "bench-repo-history", read_generation="g1")
    baseline, against, _ = _pair_of_runs(tmp_path, corpus=corpus)
    misaimed = _declare(tmp_path / "other.json", "bench-cti", "g1")
    # A --declared-corpus naming a deployment neither run read is an operator
    # error: ignoring it would leave the staleness check unperformed while the
    # comparison reported a verdict.
    with pytest.raises(ComparisonRefused, match="which neither run read"):
        _cmp(baseline, against, declared_corpora=[misaimed])


def test_okg_run_without_a_corpus_record_is_refused(tmp_path):
    baseline, against, declared = _pair_of_runs(tmp_path, corpus=None)
    assert declared is None
    with pytest.raises(ComparisonRefused, match="records no corpus"):
        _cmp(baseline, against, declared)


def test_declared_corpus_path_that_does_not_exist_is_refused(tmp_path):
    """An unreadable declaration is an UNPERFORMED staleness check."""
    baseline, against, _ = _pair_of_runs(tmp_path)
    with pytest.raises(ComparisonRefused, match="does not exist"):
        _cmp(baseline, against, declared_corpora=[tmp_path / "gone.json"])


def test_corpus_record_without_deployment_name_is_refused(tmp_path):
    corpus = _corpus(tmp_path)
    corpus.pop("deployment_name")
    baseline, against, declared = _pair_of_runs(tmp_path, corpus=corpus)
    with pytest.raises(ComparisonRefused, match="no deployment_name"):
        _cmp(baseline, against, declared)


def test_corpus_record_without_generation_id_is_refused(tmp_path):
    corpus = _corpus(tmp_path)
    corpus.pop("generation_id")
    baseline, against, declared = _pair_of_runs(tmp_path, corpus=corpus)
    with pytest.raises(ComparisonRefused, match="no generation_id"):
        _cmp(baseline, against, declared)


def test_partial_run_is_labeled_on_every_line(tmp_path):
    """A partial run may be compared over what it recorded — and every line
    says so, because a footnote is read once."""
    ordering = [*IDS, "i12", "i13"]
    runs = tmp_path / "runs"
    corpus = _corpus(tmp_path)
    declared = _declare(
        tmp_path / "declared.json", corpus["deployment_name"],
        corpus["generation_id"],
    )
    baseline = _make_run(
        runs, "r-fresh", "fresh", ordering=ordering,
        scores=dict.fromkeys(ordering, 0.0),
    )
    against = _make_run(
        runs, "r-okg", "okg", ordering=ordering, recorded=IDS,
        scores=dict.fromkeys(IDS, 1.0), corpus=corpus,
    )
    result = _cmp(baseline, against, declared)
    assert result.partial
    assert "12/14" in (result.partial_note or "")
    lines = result.render().splitlines()
    assert len(lines) > 5
    assert all(line.startswith("[PARTIAL: ") for line in lines)

    # A complete pair carries no such tag — the label means something.
    complete = _cmp(*_pair_of_runs(tmp_path / "complete"))
    assert not complete.partial
    assert not any(
        line.startswith("[PARTIAL") for line in complete.render().splitlines()
    )


# -- task.cmp.cli -------------------------------------------------------


def _roots(tmp_path: Path) -> list[str]:
    return [
        "--data-root", str(tmp_path / "data"),
        "--runs-root", str(tmp_path / "runs"),
    ]


def test_cli_compare_prints_the_table_interval_and_verdict(tmp_path, capsys):
    *_, declared = _pair_of_runs(
        tmp_path,
        baseline_scores=dict.fromkeys(IDS, 0.0),
        against_scores={iid: (1.0 if k < 9 else 0.0) for k, iid in enumerate(IDS)},
    )
    rc = main(
        _roots(tmp_path)
        + ["compare", "--baseline", "r-fresh", "--against", "r-okg",
           "--declared-corpus", str(declared), "--resamples", "2000"]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "pairs: 12" in out
    for iid in IDS:
        assert iid in out
    assert "mean paired delta:" in out
    assert "bootstrap CI" in out
    assert "seed=" in out
    assert "verdict:" in out


def test_cli_compare_exits_non_zero_when_refused(tmp_path, capsys):
    *_, declared = _pair_of_runs(tmp_path, benchmark="swe-qa")
    rc = main(
        _roots(tmp_path)
        + ["compare", "--baseline", "r-fresh", "--against", "r-okg",
           "--declared-corpus", str(declared)]
    )
    assert rc != 0, "a refusal must not be readable as a result"
    err = capsys.readouterr().err
    assert "benchmark differs" in err

    # A missing --declared-corpus is likewise a non-zero refusal, not a
    # silently unchecked verdict.
    rc_undeclared = main(
        _roots(tmp_path)
        + ["compare", "--baseline", "r-fresh", "--against", "r-okg"]
    )
    assert rc_undeclared != 0

    # Control: the same invocation on comparable runs exits 0.
    *_, ok_declared = _pair_of_runs(tmp_path / "ok")
    rc_ok = main(
        ["--data-root", str(tmp_path / "data"),
         "--runs-root", str(tmp_path / "ok" / "runs"),
         "compare", "--baseline", "r-fresh", "--against", "r-okg",
         "--declared-corpus", str(ok_declared), "--resamples", "2000"]
    )
    assert rc_ok == 0


def test_cli_compare_json_output(tmp_path, capsys):
    *_, declared = _pair_of_runs(tmp_path)
    rc = main(
        _roots(tmp_path)
        + ["compare", "--baseline", "r-fresh", "--against", "r-okg",
           "--declared-corpus", str(declared), "--resamples", "2000", "--json"]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["n_pairs"] == 12
    assert len(payload["pairs"]) == 12
    assert payload["bootstrap"]["seed"]
    assert payload["ci"][0] <= payload["mean_delta"] <= payload["ci"][1]


def test_cli_compare_renders_the_sorted_paired_delta_chart(tmp_path, capsys):
    pytest.importorskip("matplotlib")
    *_, declared = _pair_of_runs(
        tmp_path,
        baseline_scores={iid: k / 20.0 for k, iid in enumerate(IDS)},
        against_scores={iid: k / 20.0 + (0.2 if k % 3 else -0.1)
                        for k, iid in enumerate(IDS)},
    )
    chart = tmp_path / "charts" / "paired_delta.png"
    rc = main(
        _roots(tmp_path)
        + ["compare", "--baseline", "r-fresh", "--against", "r-okg",
           "--declared-corpus", str(declared), "--resamples", "2000",
           "--chart", str(chart)]
    )
    assert rc == 0
    assert chart.exists() and chart.stat().st_size > 0
    assert f"chart: {chart}" in capsys.readouterr().out


def test_chart_plots_ties_and_excludes_uncomparable_instances(tmp_path):
    """A tie is a zero-height bar, i.e. nothing drawn; it must still be
    visible, and the uncomparable instances must not be silently absent from
    the axis label."""
    pytest.importorskip("matplotlib")
    against_vals: dict[str, float | None] = dict.fromkeys(IDS, 1.0)
    against_vals["i00"] = 0.0        # a tie against a 0.0 baseline
    against_vals["i01"] = None       # uncomparable
    result = _cmp(
        *_pair_of_runs(tmp_path, against_scores=against_vals), min_pairs=6
    )
    assert result.ties == 1 and len(result.uncomparable) == 1
    chart = render_paired_delta_chart(result, tmp_path / "c" / "delta.png")
    assert chart.exists() and chart.stat().st_size > 0


def test_comparison_arithmetic_does_not_need_matplotlib(tmp_path, monkeypatch):
    """matplotlib stays behind the bench extra: the verdict must compute
    without it, and the chart must fail loudly with the install hint."""
    baseline, against, declared = _pair_of_runs(tmp_path)
    real_import = builtins.__import__

    def _no_matplotlib(name, *args, **kwargs):
        if name.split(".")[0] == "matplotlib":
            raise ImportError("no matplotlib")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_matplotlib)
    result = _cmp(baseline, against, declared)
    assert result.n_pairs == 12
    assert result.verdict
    with pytest.raises(AbBenchError, match="bench extra"):
        render_paired_delta_chart(result, tmp_path / "x.png")


def test_a_full_record_run_that_mostly_failed_to_score_is_not_reported_clean(
    tmp_path,
):
    """The verdict must not be labelled complete when it covers a fraction of
    the instances the run ordered.

    THE DEFECT. `partial` was computed from MISSING TASK RECORDS only, so a
    run that recorded every task and then lost most of them at SCORING -- a
    judge that times out, refuses, or returns something unparseable -- emitted
    a full-confidence directional verdict with `"partial": false` in the JSON.
    `min_pairs` could not catch it either: it is an ABSOLUTE floor, so 10 of 10
    and 10 of 500 both clear it and only the first is the comparison that was
    ordered.

    That matters because the instances that drop out are not a random sample.
    A judge fails disproportionately on the HARD instances, which is exactly
    where the two arms differ, so the survivors are a selected subsample and
    their mean estimates a different quantity than the ordering defines.
    """
    # Every task RECORDED on both arms -- so the old `partial` is False by
    # construction -- but the against arm declines all but four scores.
    kept = set(IDS[:4])
    against_vals: dict[str, float | None] = {
        iid: (1.0 if iid in kept else None) for iid in IDS
    }
    baseline, against, declared = _pair_of_runs(
        tmp_path, baseline_scores=dict.fromkeys(IDS, 0.0),
        against_scores=against_vals,
    )

    # 4 of 12 is 33%: below the coverage floor, so it REFUSES outright...
    with pytest.raises(ComparisonRefused) as excinfo:
        _cmp(baseline, against, declared, min_pairs=2)
    assert "4 of 12" in str(excinfo.value)
    assert "33.3% coverage" in str(excinfo.value)
    assert "not a random sample" in str(excinfo.value)

    # ...and lowering the floor is an EXPLICIT choice that still cannot buy a
    # clean label: the comparison reports, tagged partial on every line.
    result = _cmp(baseline, against, declared, min_pairs=2, min_coverage=0.3)
    assert result.n_pairs == 4
    assert result.n_instances == 12
    assert result.coverage == pytest.approx(4 / 12)
    assert result.partial, (
        "a verdict over a third of the ordered instances is not complete, "
        "whatever the task records say"
    )
    assert result.as_dict()["partial"] is True, "the JSON consumer sees it too"
    assert result.as_dict()["coverage"] == pytest.approx(4 / 12)

    # The caveat is on EVERY line, not a footnote, and it says what is wrong.
    for line in result.render().splitlines():
        assert line.startswith("[PARTIAL:"), line
    assert "[PARTIAL: None]" not in result.render(), (
        "coverage alone sets `partial`, so the tag must fall back to the "
        "coverage statement rather than printing a bare None"
    )
    assert "4/12 ordered instances paired" in result.render()
