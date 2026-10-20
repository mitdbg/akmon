"""Paired arm comparison: refuse first, pair by instance, bound the delta.

`report.py` answers "what did ONE run do". This module answers the question
the whole framework exists for — "does the okg arm do better work" — and it
answers it in the only three ways that are honest about the answer:

**Paired.** The arms run the SAME instances, so the unit is the per-instance
difference, not the difference of two means. An instance scored in one arm and
unscored in the other is reported UNCOMPARABLE with its reason; it is never
dropped into a smaller mean, because dropping it silently changes which
instances the mean is over. Below a stated floor of pairs the comparison
refuses rather than reporting a delta over a handful.

**Bounded.** A mean delta is never reported alone. A seeded bootstrap over the
PAIRED differences (the arms are not independent samples, so resampling each
arm separately would be the wrong interval) gives a percentile interval. An
interval containing zero is INSUFFICIENT EVIDENCE, worded that way. The seed is
recorded, so the verdict reproduces from the run directories alone: an unseeded
resample is a gate that passes at random.

**Refusable.** Before any arithmetic the comparison gates on what makes two
runs comparable and refuses NAMING the field. A missing field is a refusal, not
a match — "absence dressed as agreement" is the defect class this program keeps
finding, and a comparability check that treats two absent judge identities as
one identity is that defect in the output layer. An okg arm additionally
requires an EXTERNALLY supplied `--declared-corpus`: the generation it read has
to be checked against a declaration the run did not write, or the check is a
value compared to itself (see `_declared_corpus_for`).

Arithmetic here is stdlib-only. matplotlib is needed only for the chart and is
imported lazily, exactly as in `report.py`.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import ComparisonRefused
from .manifest import IDENTITY_FIELDS, RunManifest
from .model import load_json
from .report import load_run

#: Below this many pairs the bootstrap resamples from a set too small for the
#: interval to describe anything, and a mean delta is an anecdote. Stated in
#: every rendered comparison; overridable with `--min-pairs` so the floor is a
#: decision the operator makes on the record, not a constant they never see.
DEFAULT_MIN_PAIRS = 10

DEFAULT_SEED = 20260804
DEFAULT_RESAMPLES = 10_000
DEFAULT_CONFIDENCE = 0.95

#: Fraction of the shared ordering that must survive pairing before a verdict
#: is reported at all.
#:
#: `min_pairs` is an ABSOLUTE floor and cannot see this: 10 pairs out of 10
#: instances and 10 pairs out of 500 both clear it, and only the first is the
#: comparison the run intended. The instances that fail to pair are not a
#: random sample -- a judge times out, refuses, or returns an unparseable
#: score disproportionately on the HARD instances, which is exactly where the
#: arms differ. What survives is then a convenience sample, and a mean over it
#: estimates a different quantity than the one the ordering defines.
#:
#: 0.8 is a floor, not a blessing: between the floor and 1.0 the comparison is
#: reported but labelled partial on every line. Lower it explicitly with
#: `--min-coverage` when a known-unscoreable subset is expected, so that the
#: choice is recorded in the invocation rather than absorbed silently.
DEFAULT_MIN_COVERAGE = 0.8

#: Score kinds produced by an LLM judge. When a run's scores carry one of
#: these, the judge identity is load-bearing and must be RECORDED in both
#: manifests, not merely equal-because-both-absent.
JUDGE_SCORE_KINDS = ("llm_judge",)

VERDICT_INSUFFICIENT = "INSUFFICIENT EVIDENCE"


# -- results ------------------------------------------------------------


@dataclass(frozen=True)
class InstancePair:
    """One instance across both arms. ``reason is None`` iff comparable."""

    instance_id: str
    baseline: float | None
    against: float | None
    reason: str | None

    @property
    def comparable(self) -> bool:
        return self.reason is None

    @property
    def delta(self) -> float | None:
        if not self.comparable or self.baseline is None or self.against is None:
            return None
        return self.against - self.baseline

    def as_dict(self) -> dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "baseline": self.baseline,
            "against": self.against,
            "delta": self.delta,
            "comparable": self.comparable,
            "uncomparable_reason": self.reason,
        }


@dataclass(frozen=True)
class Comparison:
    benchmark: str
    baseline_run_id: str
    against_run_id: str
    baseline_arm: str
    against_arm: str
    pairs: tuple[InstancePair, ...]
    uncomparable: tuple[InstancePair, ...]
    mean_delta: float
    ci_low: float
    ci_high: float
    confidence: float
    seed: int
    resamples: int
    min_pairs: int
    against_wins: int
    baseline_wins: int
    ties: int
    partial_note: str | None
    #: Size of the ordering both runs agreed on -- the denominator `partial`
    #: is measured against, and the number the verdict is ABOUT.
    n_instances: int
    min_coverage: float

    @property
    def n_pairs(self) -> int:
        return len(self.pairs)

    @property
    def coverage(self) -> float:
        """Fraction of the shared ordering this comparison actually covers."""
        return self.n_pairs / self.n_instances if self.n_instances else 0.0

    @property
    def partial(self) -> bool:
        """True when the verdict is over fewer instances than were ordered.

        Measured from COVERAGE, not from whether task records are missing.
        Those are different failures and only the first is always visible: a
        run can record every task and still lose most of them at scoring (a
        judge that times out or refuses), which left `partial` False on a
        verdict drawn from 10 of 50 instances -- with `"partial": false` in
        the JSON, where the human-readable pairs/uncomparable line that would
        have given it away does not exist.
        """
        return self.n_pairs < self.n_instances or self.partial_note is not None

    @property
    def verdict(self) -> str:
        """A straddling interval is stated as evidence of nothing.

        Never "slightly ahead": the whole point of the interval is that a mean
        inside it is not distinguishable from zero by this data.
        """
        if self.ci_low <= 0.0 <= self.ci_high:
            return (
                f"{VERDICT_INSUFFICIENT} — the "
                f"{self.confidence:.0%} bootstrap interval includes zero"
            )
        direction = "AHEAD OF" if self.ci_low > 0.0 else "BEHIND"
        return (
            f"{self.against_arm} {direction} {self.baseline_arm} "
            f"({self.confidence:.0%} interval excludes zero)"
        )

    @property
    def insufficient_evidence(self) -> bool:
        return self.ci_low <= 0.0 <= self.ci_high

    def _coverage_note(self) -> str:
        return (
            f"{self.n_pairs}/{self.n_instances} ordered instances paired "
            f"({self.coverage:.1%}); the other "
            f"{self.n_instances - self.n_pairs} did not score on both arms, "
            "so this is not the instance set the run ordered"
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "benchmark": self.benchmark,
            "baseline": {"run_id": self.baseline_run_id, "arm": self.baseline_arm},
            "against": {"run_id": self.against_run_id, "arm": self.against_arm},
            "n_pairs": self.n_pairs,
            "min_pairs": self.min_pairs,
            "n_instances": self.n_instances,
            "coverage": self.coverage,
            "min_coverage": self.min_coverage,
            "mean_delta": self.mean_delta,
            "ci": [self.ci_low, self.ci_high],
            "confidence": self.confidence,
            "bootstrap": {"seed": self.seed, "resamples": self.resamples},
            "against_wins": self.against_wins,
            "baseline_wins": self.baseline_wins,
            "ties": self.ties,
            "uncomparable": [p.as_dict() for p in self.uncomparable],
            "pairs": [p.as_dict() for p in self.pairs],
            "partial": self.partial,
            "partial_note": self.partial_note,
            "verdict": self.verdict,
            "insufficient_evidence": self.insufficient_evidence,
        }

    def render(self) -> str:
        """The printed comparison.

        A partial run's caveat is repeated on EVERY line. A footnote is read
        once and forgotten; the numbers are read one line at a time, and each
        one of them is over fewer instances than the run intended.
        """
        # `partial` can be true with no record-shortfall note -- coverage
        # alone sets it -- so the tag falls back to the coverage statement
        # rather than printing "[PARTIAL: None]".
        tag = f"[PARTIAL: {self.partial_note or self._coverage_note()}] " if self.partial else ""
        out: list[str] = []

        def line(text: str) -> None:
            out.append(f"{tag}{text}")

        line(
            f"{self.benchmark}: {self.against_arm} ({self.against_run_id}) "
            f"vs {self.baseline_arm} ({self.baseline_run_id})"
        )
        line(
            f"pairs: {self.n_pairs}/{self.n_instances} "
            f"({self.coverage:.1%} coverage, floor {self.min_coverage:.0%}; "
            f"pair floor {self.min_pairs}); "
            f"uncomparable: {len(self.uncomparable)}"
        )
        line("per-instance paired deltas (sorted, not run order):")
        for pair in sorted(self.pairs, key=lambda p: (_delta(p), p.instance_id)):
            line(
                f"  {pair.instance_id}: {self.baseline_arm}={pair.baseline} "
                f"{self.against_arm}={pair.against} delta={_delta(pair):+.4f}"
            )
        for pair in self.uncomparable:
            line(f"  {pair.instance_id}: UNCOMPARABLE — {pair.reason}")
        line(
            f"wins: {self.against_arm}={self.against_wins} "
            f"{self.baseline_arm}={self.baseline_wins} ties={self.ties}"
        )
        line(
            f"mean paired delta: {self.mean_delta:+.4f} "
            f"({self.confidence:.0%} bootstrap CI "
            f"[{self.ci_low:+.4f}, {self.ci_high:+.4f}], "
            f"seed={self.seed}, resamples={self.resamples})"
        )
        line(f"verdict: {self.verdict}")
        return "\n".join(out)


def _delta(pair: InstancePair) -> float:
    """A comparable pair's delta, or a loud failure.

    Never ``pair.delta or 0.0``: that maps an absent delta onto a perfect tie,
    which is a silent fallback wearing the costume of a data point.
    """
    if pair.delta is None:
        raise ComparisonRefused(
            f"instance {pair.instance_id} has no paired delta but was counted "
            f"as comparable (reason={pair.reason!r}) — refusing to plot or "
            "average a value that does not exist"
        )
    return pair.delta


# -- the comparison -----------------------------------------------------


def compare(
    baseline_dir: Path,
    against_dir: Path,
    *,
    declared_corpora: list[Path] | None = None,
    min_pairs: int = DEFAULT_MIN_PAIRS,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
    seed: int = DEFAULT_SEED,
    resamples: int = DEFAULT_RESAMPLES,
    confidence: float = DEFAULT_CONFIDENCE,
) -> Comparison:
    """Compare two runs, or refuse naming why. Never returns a partial answer."""
    if min_pairs < 2:
        raise ComparisonRefused(
            f"min_pairs={min_pairs} is not a floor: a paired bootstrap needs at "
            "least 2 differences to resample"
        )
    if not 0.0 < min_coverage <= 1.0:
        raise ComparisonRefused(
            f"min_coverage={min_coverage} must be in (0, 1]: a coverage floor "
            "of zero is not a floor"
        )
    if not 0.0 < confidence < 1.0:
        raise ComparisonRefused(f"confidence={confidence} must be in (0, 1)")
    if resamples < 1000:
        raise ComparisonRefused(
            f"resamples={resamples} is too few for a percentile interval "
            "(minimum 1000)"
        )

    baseline = _load(baseline_dir, "baseline")
    against = _load(against_dir, "against")
    _gate_comparability(baseline, against)
    _gate_declared_corpora_apply(baseline, against, declared_corpora)
    _gate_corpus_generation(baseline, declared_corpora)
    _gate_corpus_generation(against, declared_corpora)

    pairs, uncomparable = _pair(baseline, against)
    n_instances = len(baseline["manifest"].ordering["instance_ids"])
    detail = "\n".join(f"  {p.instance_id}: {p.reason}" for p in uncomparable)
    if len(pairs) < min_pairs:
        raise ComparisonRefused(
            f"refusing to report a delta over {len(pairs)} instance pair(s): "
            f"the floor is {min_pairs}. "
            f"{len(uncomparable)} instance(s) could not be paired"
            + (f":\n{detail}" if detail else ".")
        )
    # The COVERAGE floor, which the absolute pair floor cannot express: 10 of
    # 10 and 10 of 500 both clear `min_pairs`, and only the first is the
    # comparison that was ordered. Instances fail to pair for reasons
    # correlated with difficulty, so the survivors are a selected subsample,
    # not a smaller random one.
    coverage = len(pairs) / n_instances if n_instances else 0.0
    if coverage < min_coverage:
        raise ComparisonRefused(
            f"refusing to report a delta over {len(pairs)} of {n_instances} "
            f"ordered instances ({coverage:.1%} coverage): the floor is "
            f"{min_coverage:.0%}. The instances that dropped out are not a "
            "random sample of the ordering, so a mean over the survivors "
            "estimates a different quantity than the one the run ordered. "
            "Lower --min-coverage explicitly to report anyway"
            + (f":\n{detail}" if detail else ".")
        )

    deltas_f = [_delta(p) for p in pairs]
    mean_delta = sum(deltas_f) / len(deltas_f)
    ci_low, ci_high = bootstrap_ci(
        deltas_f, seed=seed, resamples=resamples, confidence=confidence
    )
    return Comparison(
        benchmark=baseline["manifest"].benchmark,
        baseline_run_id=baseline["run_id"],
        against_run_id=against["run_id"],
        baseline_arm=baseline["manifest"].arm,
        against_arm=against["manifest"].arm,
        pairs=tuple(pairs),
        uncomparable=tuple(uncomparable),
        mean_delta=mean_delta,
        ci_low=ci_low,
        ci_high=ci_high,
        confidence=confidence,
        seed=seed,
        resamples=resamples,
        min_pairs=min_pairs,
        against_wins=sum(1 for d in deltas_f if d > 0),
        baseline_wins=sum(1 for d in deltas_f if d < 0),
        ties=sum(1 for d in deltas_f if d == 0),
        partial_note=_partial_note(baseline, against),
        n_instances=n_instances,
        min_coverage=min_coverage,
    )


def _partial_note(baseline: dict[str, Any], against: dict[str, Any]) -> str | None:
    notes = [
        f"{run['side']} {run['manifest'].arm} recorded "
        f"{len(run['rows'])}/{run['expected_tasks']} tasks"
        for run in (baseline, against)
        if run["partial"]
    ]
    return "; ".join(notes) if notes else None


# -- validity gates -----------------------------------------------------


def _load(run_dir: Path, side: str) -> dict[str, Any]:
    """Load a run, turning a malformed/missing manifest into a named refusal.

    ``RunManifest(**json)`` raises TypeError on a manifest missing an identity
    field. That is exactly the case the validity gate must refuse rather than
    crash through, so name the missing keys here.
    """
    if not (run_dir / "manifest.json").exists():
        raise ComparisonRefused(
            f"{side} run {run_dir} has no manifest.json — a run with no "
            "reproducibility record cannot be shown comparable to anything"
        )
    try:
        run = load_run(run_dir)
    except TypeError as exc:
        recorded = set(load_json(run_dir / "manifest.json"))
        missing = sorted(set(IDENTITY_FIELDS) - recorded)
        raise ComparisonRefused(
            f"{side} run {run_dir.name} has a manifest this framework cannot "
            f"read ({exc}); missing identity field(s): {missing or 'unknown'}"
        ) from exc
    run["side"] = side
    run["dir"] = run_dir
    return run


def _gate_comparability(baseline: dict[str, Any], against: dict[str, Any]) -> None:
    """Refuse an incomparable pair of runs, NAMING the differing field."""
    bm: RunManifest = baseline["manifest"]
    am: RunManifest = against["manifest"]

    if bm.run_id == am.run_id:
        raise ComparisonRefused(
            f"refusing to compare run {bm.run_id} with itself: a run's delta "
            "against itself is zero by construction, not a result"
        )

    for name in ("schema_version", "benchmark", "model"):
        b_value = getattr(bm, name)
        a_value = getattr(am, name)
        # A field absent from BOTH manifests is not agreement; it is two runs
        # that never recorded what they were.
        if not b_value or not a_value:
            raise ComparisonRefused(
                f"refusing to compare {bm.run_id} vs {am.run_id}: {name} is "
                f"not recorded (baseline={b_value!r}, against={a_value!r})"
            )
        if b_value != a_value:
            raise ComparisonRefused(
                f"refusing to compare {bm.run_id} vs {am.run_id}: {name} "
                f"differs — baseline={b_value!r} against={a_value!r}"
            )

    # The pinned dataset is what `gold` came from. Two arms legitimately
    # differ in prompt_digest (the okg arm's corpus binding rewrites prompts),
    # but never in the dataset they were scored against.
    if not bm.dataset_ref or not am.dataset_ref:
        raise ComparisonRefused(
            f"refusing to compare {bm.run_id} vs {am.run_id}: dataset_ref is "
            f"not recorded (baseline={bm.dataset_ref!r}, "
            f"against={am.dataset_ref!r})"
        )
    if bm.dataset_ref != am.dataset_ref:
        raise ComparisonRefused(
            f"refusing to compare {bm.run_id} vs {am.run_id}: dataset_ref "
            f"differs — baseline={bm.dataset_ref!r} against={am.dataset_ref!r}"
        )

    _gate_ordering(baseline, against)
    _gate_judge(baseline, against)


def _gate_ordering(baseline: dict[str, Any], against: dict[str, Any]) -> None:
    bm: RunManifest = baseline["manifest"]
    am: RunManifest = against["manifest"]
    b_ids = (bm.ordering or {}).get("instance_ids")
    a_ids = (am.ordering or {}).get("instance_ids")
    if not b_ids or not a_ids:
        raise ComparisonRefused(
            f"refusing to compare {bm.run_id} vs {am.run_id}: ordering."
            "instance_ids is not recorded (baseline="
            f"{'present' if b_ids else 'MISSING'}, against="
            f"{'present' if a_ids else 'MISSING'}) — without it there is no "
            "instance set to pair over"
        )
    if list(b_ids) != list(a_ids):
        b_only = sorted(set(b_ids) - set(a_ids))
        a_only = sorted(set(a_ids) - set(b_ids))
        same_set = not b_only and not a_only
        detail = (
            "same instances in a different order"
            if same_set
            else f"baseline-only={b_only[:5]} against-only={a_only[:5]}"
        )
        raise ComparisonRefused(
            f"refusing to compare {bm.run_id} vs {am.run_id}: ordering "
            f"differs — {detail}"
        )
    b_mode = (bm.ordering or {}).get("mode")
    a_mode = (am.ordering or {}).get("mode")
    if b_mode != a_mode:
        raise ComparisonRefused(
            f"refusing to compare {bm.run_id} vs {am.run_id}: ordering.mode "
            f"differs — baseline={b_mode!r} against={a_mode!r}"
        )


def _judge_scored(run: dict[str, Any]) -> bool:
    return any(row.get("score_kind") in JUDGE_SCORE_KINDS for row in run["rows"])


def _gate_judge(baseline: dict[str, Any], against: dict[str, Any]) -> None:
    """Judge identity, checked WHERE SCORING USES ONE.

    `judge_config()` records `judge_model: os.environ.get(...) or None`, so a
    run judged with the env var unset records a judge dict whose model is null.
    Two such runs compare equal while neither knows what judged it. Equality is
    therefore necessary but not sufficient: when the scores say a judge scored
    them, the judge must be IDENTIFIED.
    """
    bm: RunManifest = baseline["manifest"]
    am: RunManifest = against["manifest"]
    if bm.judge != am.judge:
        raise ComparisonRefused(
            f"refusing to compare {bm.run_id} vs {am.run_id}: judge identity "
            f"differs — baseline={bm.judge!r} against={am.judge!r}"
        )
    judged = [run for run in (baseline, against) if _judge_scored(run)]
    if not judged:
        return
    for run in judged:
        manifest: RunManifest = run["manifest"]
        model = (manifest.judge or {}).get("judge_model")
        if not model:
            raise ComparisonRefused(
                f"refusing to compare {bm.run_id} vs {am.run_id}: the "
                f"{run['side']} run's scores were produced by an LLM judge but "
                "its manifest records no judge_model "
                f"(judge={manifest.judge!r}). Two unrecorded judge identities "
                "are not the same judge identity"
            )


def _gate_declared_corpora_apply(
    baseline: dict[str, Any],
    against: dict[str, Any],
    declared_corpora: list[Path] | None,
) -> None:
    """A `--declared-corpus` that matches no run's deployment is refused.

    Ignoring it would let a typo'd or misaimed path do nothing while the
    comparison reported a verdict — the staleness check the operator asked for
    would simply not have run, and its absence would read as its result.
    """
    if not declared_corpora:
        return
    deployments = {
        (run["manifest"].corpus or {}).get("deployment_name")
        for run in (baseline, against)
    } - {None}
    for path in declared_corpora:
        if not path.exists():
            raise ComparisonRefused(f"--declared-corpus {path} does not exist")
        named = load_json(path).get("deployment_name")
        if named not in deployments:
            raise ComparisonRefused(
                f"--declared-corpus {path} declares deployment {named!r}, "
                f"which neither run read (they read {sorted(deployments)!r}); "
                "refusing rather than ignoring it"
            )


def _gate_corpus_generation(
    run: dict[str, Any], declared_corpora: list[Path] | None
) -> None:
    """Refuse a run measured against a superseded corpus generation.

    This is the real case: the program's only three-arm run measured its okg
    arm on a repo-history generation a later rebuild superseded (10,434 nodes
    with an island forge subgraph -> 23,249), so the arm ran against a graph
    that could not perform the traversals it was supposed to win on. Nothing
    in the output would have said so.
    """
    manifest: RunManifest = run["manifest"]
    corpus = manifest.corpus
    if corpus is None:
        if manifest.arm == "okg":
            raise ComparisonRefused(
                f"refusing to compare {run['side']} run {manifest.run_id}: it "
                "is an okg-arm run whose manifest records no corpus, so the "
                "generation it read cannot be checked against the declared one"
            )
        # fresh/native/echo arms read no graph: no corpus to be stale.
        return
    recorded_gen = corpus.get("generation_id")
    if not recorded_gen:
        raise ComparisonRefused(
            f"refusing to compare {run['side']} run {manifest.run_id}: its "
            "corpus record has no generation_id, so staleness is unknowable "
            "(and unknowable is not fresh)"
        )
    deployment = corpus.get("deployment_name")
    if not deployment:
        raise ComparisonRefused(
            f"refusing to compare {run['side']} run {manifest.run_id}: its "
            "corpus record has no deployment_name, so the generation it read "
            "cannot be matched against the one declared for any deployment"
        )
    declared_path, declared = _declared_corpus_for(
        run, deployment, declared_corpora
    )
    declared_gen = declared.get("generation_id")
    if not declared_gen:
        raise ComparisonRefused(
            f"refusing to compare {run['side']} run {manifest.run_id}: the "
            f"declared corpus {declared_path} records no generation_id"
        )
    if declared_gen != recorded_gen:
        raise ComparisonRefused(
            f"refusing to compare {run['side']} run {manifest.run_id}: STALE "
            f"corpus generation — the run read generation {recorded_gen!r} of "
            f"deployment {deployment!r}, but the generation currently declared "
            f"for this benchmark ({declared_path}) is {declared_gen!r}. The "
            "arm was measured against a graph that is no longer the corpus"
        )


def _declared_corpus_for(
    run: dict[str, Any],
    deployment: str,
    declared_corpora: list[Path] | None,
) -> tuple[Path, dict[str, Any]]:
    """Where the currently-declared generation is read from, and its content.

    The declaration is RETURNED rather than re-read by the caller, and it is
    selected BY `deployment_name`, so it is for this deployment by
    construction. A caller re-checking that would be writing a predicate that
    cannot fail — the shape this module exists to keep out.

    ONLY an operator-supplied `--declared-corpus` for this deployment. There
    is deliberately NO fallback to the corpus.json in the run's own recorded
    `corpus_root`, and that is the whole point of this function.

    The run recorded its generation FROM that file. Reading it back to check
    the run for staleness compares a value to itself, so the check passes for
    every run that ever ran and can only fail if someone hand-edited the file
    in between. It is a gate that cannot fire on the case it exists for: the
    program's only three-arm run read a repo-history generation that a later
    rebuild superseded, and because `corpus_domain.build` refuses to overwrite
    an existing corpus.json, that rebuild landed in a DIFFERENT corpus_root —
    leaving the stale file in place, still declaring the superseded
    generation, still matching itself.

    So an okg-arm comparison without a `--declared-corpus` for its deployment
    is REFUSED. Refusing costs one flag; the fallback costs a wrong result
    that looks right. An unperformed staleness check is not a passed one — the
    same rule already applied here when the file is unreadable, and a
    self-referential read is just as unperformed.
    """
    manifest: RunManifest = run["manifest"]
    for path in declared_corpora or []:
        if not path.exists():
            raise ComparisonRefused(f"--declared-corpus {path} does not exist")
        declared = load_json(path)
        if declared.get("deployment_name") == deployment:
            return path, declared
    raise ComparisonRefused(
        f"refusing to compare {run['side']} run {manifest.run_id}: no "
        f"--declared-corpus names deployment {deployment!r}, so its recorded "
        "generation cannot be checked for staleness. There is no fallback to "
        "the run's own corpus.json ON PURPOSE — the run recorded its "
        "generation from that file, so reading it back compares a value to "
        "itself and passes for every run ever made. Pass --declared-corpus "
        "<corpus.json> naming the generation currently declared for this "
        "deployment."
    )


# -- pairing ------------------------------------------------------------


def _pair(
    baseline: dict[str, Any], against: dict[str, Any]
) -> tuple[list[InstancePair], list[InstancePair]]:
    """Join the two runs on instance_id over the (gated-identical) ordering.

    Every instance lands in exactly one of the two returned lists. Nothing is
    dropped: an instance that cannot be paired is REPORTED with the reason it
    cannot, because a mean over a silently smaller set is a different estimate
    of a different quantity.
    """
    ordering: list[str] = list(baseline["manifest"].ordering["instance_ids"])
    b_rows = {row["instance_id"]: row for row in baseline["rows"]}
    a_rows = {row["instance_id"]: row for row in against["rows"]}

    pairs: list[InstancePair] = []
    uncomparable: list[InstancePair] = []
    for iid in ordering:
        b_row = b_rows.get(iid)
        a_row = a_rows.get(iid)
        reason = _uncomparable_reason(baseline, against, b_row, a_row)
        b_score = None if b_row is None else b_row["score"]
        a_score = None if a_row is None else a_row["score"]
        pair = InstancePair(
            instance_id=iid, baseline=b_score, against=a_score, reason=reason
        )
        (uncomparable if reason else pairs).append(pair)

    # An instance recorded by a run but absent from the ordering both runs
    # agreed on is a corrupted run directory. It is surfaced, not skipped:
    # skipping it is how a task record nobody can account for stays invisible.
    for side, rows in (("baseline", b_rows), ("against", a_rows)):
        for iid in sorted(set(rows) - set(ordering)):
            uncomparable.append(
                InstancePair(
                    instance_id=iid,
                    baseline=b_rows.get(iid, {}).get("score"),
                    against=a_rows.get(iid, {}).get("score"),
                    reason=(
                        f"recorded by the {side} run but absent from the "
                        "manifest ordering both runs share"
                    ),
                )
            )
    return pairs, uncomparable


def _uncomparable_reason(
    baseline: dict[str, Any],
    against: dict[str, Any],
    b_row: dict[str, Any] | None,
    a_row: dict[str, Any] | None,
) -> str | None:
    missing = [
        side
        for side, row in (("baseline", b_row), ("against", a_row))
        if row is None
    ]
    if missing:
        arms = {
            "baseline": baseline["manifest"].arm,
            "against": against["manifest"].arm,
        }
        return (
            "no task record in "
            + ", ".join(f"the {s} ({arms[s]}) run" for s in missing)
            + " — that run is partial"
        )
    assert b_row is not None and a_row is not None
    for side, row in (("baseline", b_row), ("against", a_row)):
        arm = (baseline if side == "baseline" else against)["manifest"].arm
        if not row["score_recorded"]:
            return (
                f"the {side} ({arm}) run recorded the task but the scorer "
                "produced no score record for it"
            )
        if row["score"] is None:
            return (
                f"unscored in the {side} ({arm}) run: "
                f"{row['unscored_reason'] or 'no reason recorded'}"
            )
    return None


# -- bootstrap ----------------------------------------------------------


def bootstrap_ci(
    deltas: list[float],
    *,
    seed: int,
    resamples: int,
    confidence: float,
) -> tuple[float, float]:
    """Percentile interval over the PAIRED differences, from a seeded RNG.

    Resampling each arm separately would treat two runs of the same instances
    as independent samples, which they are not; the pairing is the whole point.
    The seed is an argument and is reported with the interval — an unseeded
    resample makes the same data yield different verdicts on re-run.
    """
    if not deltas:
        raise ComparisonRefused("bootstrap over an empty set of paired deltas")
    rng = random.Random(seed)
    n = len(deltas)
    means = sorted(sum(rng.choices(deltas, k=n)) / n for _ in range(resamples))
    alpha = (1.0 - confidence) / 2.0
    return _percentile(means, alpha), _percentile(means, 1.0 - alpha)


def _percentile(sorted_values: list[float], q: float) -> float:
    idx = q * (len(sorted_values) - 1)
    low = math.floor(idx)
    high = math.ceil(idx)
    if low == high:
        return sorted_values[int(idx)]
    frac = idx - low
    return sorted_values[low] * (1.0 - frac) + sorted_values[high] * frac


# -- chart --------------------------------------------------------------


def render_paired_delta_chart(comparison: Comparison, out_path: Path) -> Path:
    """Sorted per-instance paired deltas — NOT metric vs task index.

    Task index is run ORDER, which carries no information about a score, so a
    line over it is a sawtooth that hides the shape of the effect. Sorted
    paired deltas show it directly: how many instances moved, by how much, in
    which direction.
    """
    from .report import _matplotlib

    plt = _matplotlib()
    ordered = sorted(comparison.pairs, key=lambda p: (_delta(p), p.instance_id))
    deltas = [_delta(p) for p in ordered]
    colors = ["#2e7d32" if d > 0 else "#c62828" if d < 0 else "#9e9e9e" for d in deltas]
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    ax.bar(range(len(deltas)), deltas, color=colors)
    ax.axhline(0.0, color="black", linewidth=0.8)
    # A tie draws a zero-height bar, i.e. nothing. Mark them, or the chart
    # shows fewer instances than the comparison paired and the reader counts
    # the difference as absent rather than as zero.
    ties = [k for k, d in enumerate(deltas) if d == 0.0]
    if ties:
        ax.scatter(
            ties, [0.0] * len(ties), s=28, facecolors="none",
            edgecolors="#616161", zorder=3, label=f"tie ({len(ties)})",
        )
    ax.axhline(
        comparison.mean_delta, color="#1565c0", linestyle="--", linewidth=1.0,
        label=f"mean {comparison.mean_delta:+.3f}",
    )
    ax.fill_between(
        [-0.5, len(deltas) - 0.5],
        comparison.ci_low,
        comparison.ci_high,
        color="#1565c0",
        alpha=0.12,
        label=(
            f"{comparison.confidence:.0%} CI "
            f"[{comparison.ci_low:+.3f}, {comparison.ci_high:+.3f}] "
            f"(seed {comparison.seed})"
        ),
    )
    ax.set_xlim(-0.5, len(deltas) - 0.5)
    ax.set_xlabel(
        f"instance, sorted by paired delta (n={comparison.n_pairs}; "
        f"{len(comparison.uncomparable)} uncomparable, not plotted)"
    )
    ax.set_ylabel(
        f"score delta ({comparison.against_arm} - {comparison.baseline_arm})"
    )
    # Same fallback as render(): coverage alone can set `partial`, and the
    # chart title must not read "[PARTIAL: None]".
    partial = (
        f" [PARTIAL: {comparison.partial_note or comparison._coverage_note()}]"
        if comparison.partial
        else ""
    )
    ax.set_title(f"{comparison.benchmark}: {comparison.verdict}{partial}", fontsize=9)
    ax.legend(fontsize=8)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path
