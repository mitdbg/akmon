"""Compare two replay runs: setup diff, paired bootstrap, noise report.

Two runs compare only when their setups match. Without ``--change`` the two
setup digests must be equal (the noise-floor check). With ``--change FIELD``
exactly that setup field (and the fields derived from it, listed in
``DERIVED``) must differ and nothing else: one change per comparison.

The difference d = run B - run A of each answer-based measure gets a paired
bootstrap range: each replicate resamples questions (shared by both runs),
then, per run, tries within each question, then judge passes within each
try. 10,000 replicates under each of four fixed seeds (stdlib ``random.Random``,
so the report does not depend on numpy); the reported 95% range
is the widest of the four (lowest low, highest high). The runs are *within
the noise floor* when both are complete, the setups pass the rule above, and
the range of d contains 0 for required-fact recall and for the contradicted
claim rate. The floor of a measure is the half-width of its range.

Also reported: the judge-only floor (pass 2 - pass 1 on the same answers, per
run), the 3-try floor (tries 1-3 only, for comparison with earlier 3-try
runs), and cost ratios (B / A with a range; reported, never gated).

The report (``okg.akb.replay_report.v1``) holds numbers, digests, ids and
field names only. The measurement record (v1) is unchanged.
"""
from __future__ import annotations

import json
import math
import random
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import refuse
from .measure import UNIT_MEASURES, _UNIT_FIELD, _nested_mean, completeness, unit_values
from .setup import check_setup, diff_setups

REPORT_SCHEMA = "okg.akb.replay_report.v1"
REPLICATES = 10_000
SEEDS = (11, 23, 37, 41)
LEVEL = 0.95
GATED = ("required_fact_recall", "contradicted_claim_rate")
COST_FIELDS = {"usd_per_question": "ledger_answer_usd", "model_calls_per_try": "model_calls",
               "tokens_per_try": "tokens", "tool_errors_per_try": "tool_errors",
               "latency_seconds_per_try": "wall_s"}
#: A change to the key field also changes these (prefixes); they may differ with it.
DERIVED: Mapping[str, tuple[str, ...]] = {
    "answerer.system_prompt": ("model_input.system_text",),
    "served": ("model_input.",),
    "deployment.generation": ("served.", "model_input."),
    "deployment.tree": ("served.", "model_input."),
    "okg.commit": ("okg.runner_tree", "served.", "model_input."),
    # Issue #4280: a trial branch serves its own surface_text layer (and, once
    # it publishes, its own generation); a text change moves what is served.
    "deployment.branch": ("deployment.generation", "served.", "model_input."),
    "served.surface_text": ("served.", "model_input."),
}
DETERMINISM_NOTE = (
    "Run 2's answers are not deterministic: the answering model runs live and writes new answers each run. "
    "Only the judge is cached, so judging the same answer again is free and gives the same verdict, but run 2's "
    "answers are new and are judged afresh. The noise floor measures that answer-plus-judge variation."
)
_BATCH = 500


@dataclass(frozen=True)
class RunData:
    setup: Mapping[str, Any]
    summary: Mapping[str, Any]
    rows: list[dict[str, Any]]

    @property
    def run_id(self) -> str:
        return str(self.summary["run_id"])


def load_run(run_dir: Path) -> RunData:
    run_dir = Path(run_dir)
    try:
        setup = json.loads((run_dir / "setup.json").read_text(encoding="utf-8"))
        summary = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
        rows = [json.loads(line) for line in (run_dir / "tries.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()]
    except (OSError, ValueError) as exc:
        raise refuse("run_unreadable", f"run folder {run_dir.name} lacks a readable setup.json, run.json "
                     f"or tries.jsonl ({type(exc).__name__})") from exc
    check_setup(setup)
    return RunData(setup, summary, rows)


def check_one_change(differing: Sequence[str], change: str | None) -> None:
    if change is None:
        if differing:
            raise refuse("setup_mismatch",
                         f"the setups differ in {len(differing)} field(s): {', '.join(differing[:12])}; "
                         "pass --change FIELD to compare one change")
        return
    if not differing:
        raise refuse("change_absent", f"--change {change}: the setups do not differ at all")
    allowed = (change, *DERIVED.get(change, ()))
    others = [f for f in differing
              if not any(f == a or f.startswith(a if a.endswith(".") else a + ".") for a in allowed)]
    if others:
        raise refuse("more_than_one_change",
                     f"--change {change}: other fields also differ: {', '.join(others[:12])}")
    if not any(f == change or f.startswith(change + ".") for f in differing):
        raise refuse("change_absent", f"--change {change}: that field does not differ")


def _rng(seed: int) -> random.Random:
    return random.Random(seed)


def _pick(rng: random.Random, n: int) -> int:
    return int(rng.random() * n)  # random() < 1, so the index is below n


def _resample_units(arr: Sequence[Sequence[Sequence[float]]], qi: Sequence[int], rng: random.Random) -> float:
    """Mean of arr[Q][T][P] at resampled questions ``qi``, then tries, then passes (one replicate)."""
    total, count = 0.0, 0
    for q in qi:
        tries = arr[q]
        t_n = len(tries)
        for _ in range(t_n):
            passes = tries[_pick(rng, t_n)]
            p_n = len(passes)
            for _ in range(p_n):
                total += passes[_pick(rng, p_n)]
            count += p_n
    return total / count


def _resample_tries(arr: Sequence[Sequence[float]], qi: Sequence[int], rng: random.Random) -> float:
    """Mean of arr[Q][T] at resampled questions ``qi``, then tries (one replicate)."""
    total, count = 0.0, 0
    for q in qi:
        tries = arr[q]
        t_n = len(tries)
        for _ in range(t_n):
            total += tries[_pick(rng, t_n)]
        count += t_n
    return total / count


def _quantile(sorted_values: Sequence[float], q: float) -> float:
    """Linear-interpolation quantile of sorted values (numpy's default method)."""
    h = (len(sorted_values) - 1) * q
    lo = math.floor(h)
    hi = min(lo + 1, len(sorted_values) - 1)
    return sorted_values[lo] + (h - lo) * (sorted_values[hi] - sorted_values[lo])


def _range(draw: Callable[[random.Random], float | None], seeds: Sequence[int], replicates: int,
           level: float) -> tuple[float, float] | None:
    """Widest central ``level`` range over the seeds of a replicate statistic; None when a replicate has none."""
    lows, highs = [], []
    for seed in seeds:
        rng = _rng(seed)
        stat = []
        for _ in range(replicates):
            value = draw(rng)
            if value is None:
                return None
            stat.append(value)
        stat.sort()
        lows.append(_quantile(stat, (1 - level) / 2))
        highs.append(_quantile(stat, 1 - (1 - level) / 2))
    return min(lows), max(highs)


def _always(out: tuple[float, float] | None) -> tuple[float, float]:
    if out is None:  # pragma: no cover - only a ratio draw can have no value
        raise RuntimeError("a difference draw returned no value")
    return out


def _questions(rng: random.Random, n: int) -> list[int]:
    return [_pick(rng, n) for _ in range(n)]


def _paired_unit_range(a, b, seeds, replicates, level) -> tuple[float, float]:
    def draw(rng: random.Random) -> float:
        qi = _questions(rng, len(a))
        return _resample_units(b, qi, rng) - _resample_units(a, qi, rng)
    return _always(_range(draw, seeds, replicates, level))


def _judge_only_range(arr, seeds, replicates, level) -> tuple[float, float]:
    """Pass 2 - pass 1 on the same answers: resample questions, then tries (passes kept paired)."""
    diff = [[tr[1] - tr[0] for tr in q] for q in arr]

    def draw(rng: random.Random) -> float:
        return _resample_tries(diff, _questions(rng, len(diff)), rng)
    return _always(_range(draw, seeds, replicates, level))


def _flat_mean(arr: Sequence[Sequence[float]]) -> float:
    return sum(v for q in arr for v in q) / sum(len(q) for q in arr)


def _ratio_range(a, b, seeds, replicates, level) -> tuple[float, float] | None:
    """Range of mean(B) / mean(A); None when some replicate has mean(A) = 0 (no ratio exists)."""
    def draw(rng: random.Random) -> float | None:
        qi = _questions(rng, len(a))
        ma, mb = _resample_tries(a, qi, rng), _resample_tries(b, qi, rng)
        return mb / ma if ma > 0 else None
    if _flat_mean(a) == 0.0:
        return None
    return _range(draw, seeds, replicates, level)


def _try_array(rows: Sequence[Mapping[str, Any]], ids: Sequence[str], tries: int, field: str) -> list[list[float]]:
    seen = {(r["question_id"], r["try"]): r for r in rows}

    def value(r: Mapping[str, Any]) -> float:
        if field == "tokens":
            return float(r["input_tokens"] + r["output_tokens"])
        return float(r[field])
    return [[value(seen[(q, t)]) for t in range(1, tries + 1)] for q in ids]


def compare_runs(a: RunData, b: RunData, *, change: str | None = None, replicates: int = REPLICATES,
                 seeds: Sequence[int] = SEEDS, level: float = LEVEL) -> dict[str, Any]:
    """The okg.akb.replay_report.v1 of run B against run A; refuses a setup the rule does not allow."""
    differing = diff_setups(a.setup["fields"], b.setup["fields"])
    check_one_change(differing, change)
    ids_a, ids_b = list(a.summary["question_ids"]), list(b.summary["question_ids"])
    if sorted(ids_a) != sorted(ids_b):
        raise refuse("question_mismatch", "the runs answered different questions")
    ids = sorted(ids_a)
    tries_a, tries_b = int(a.summary["tries"]), int(b.summary["tries"])
    passes_a, passes_b = int(a.summary["passes"]), int(b.summary["passes"])
    for run, tries, passes in ((a, tries_a, passes_a), (b, tries_b, passes_b)):
        state = completeness(run.rows, ids, tries, passes)
        if not state["complete"]:
            raise refuse("run_incomplete", f"run {run.run_id} is incomplete: {state}")
    measures: dict[str, Any] = {}
    judge_only: dict[str, Any] = {}
    three: dict[str, Any] | None = {} if min(tries_a, tries_b) >= 3 else None
    for name in UNIT_MEASURES:
        field = _UNIT_FIELD[name]
        ua = unit_values(a.rows, ids, tries_a, passes_a, field)
        ub = unit_values(b.rows, ids, tries_b, passes_b, field)
        lo, hi = _paired_unit_range(ua, ub, seeds, replicates, level)
        va, vb = _nested_mean(ua), _nested_mean(ub)
        measures[name] = {"run_a": round(va, 6), "run_b": round(vb, 6), "difference": round(vb - va, 6),
                          "range": [round(lo, 6), round(hi, 6)], "contains_zero": lo <= 0.0 <= hi,
                          "floor_half_width": round((hi - lo) / 2, 6), "gated": name in GATED}
        judge_only[name] = {}
        for label, arr, run_passes in (("run_a", ua, passes_a), ("run_b", ub, passes_b)):
            if run_passes >= 2:
                jlo, jhi = _judge_only_range(arr, seeds, replicates, level)
                judge_only[name][label] = {"range": [round(jlo, 6), round(jhi, 6)],
                                           "floor_half_width": round((jhi - jlo) / 2, 6)}
            else:
                judge_only[name][label] = None
        if three is not None:
            tlo, thi = _paired_unit_range([q[:3] for q in ua], [q[:3] for q in ub], seeds, replicates, level)
            three[name] = {"range": [round(tlo, 6), round(thi, 6)], "floor_half_width": round((thi - tlo) / 2, 6)}
    costs: dict[str, Any] = {}
    for name, field in COST_FIELDS.items():
        ca, cb = _try_array(a.rows, ids, tries_a, field), _try_array(b.rows, ids, tries_b, field)
        ma, mb = _flat_mean(ca), _flat_mean(cb)
        rng_ = _ratio_range(ca, cb, seeds, replicates, level)
        costs[name] = {"run_a": round(ma, 8), "run_b": round(mb, 8),
                       "ratio": round(mb / ma, 6) if ma else None,
                       "range": [round(rng_[0], 6), round(rng_[1], 6)] if rng_ else None}
    within = all(measures[name]["contains_zero"] for name in GATED)
    return {
        "schema": REPORT_SCHEMA,
        "runs": [_run_summary(a, ids, tries_a, passes_a), _run_summary(b, ids, tries_b, passes_b)],
        "setup": {"same": not differing, "differing_fields": differing, "change": change},
        "bootstrap": {"replicates": replicates, "seeds": list(seeds), "level": level,
                      "resampled": "questions (paired), then tries, then judge passes",
                      "range_rule": "widest over seeds"},
        "measures": measures,
        "judge_only": judge_only,
        "three_tries": three,
        "cost_ratios": costs,
        "within_floor": within,
        "determinism": DETERMINISM_NOTE,
    }


def _run_summary(run: RunData, ids: Sequence[str], tries: int, passes: int) -> dict[str, Any]:
    return {"run_id": run.run_id, "setup_digest": run.setup["digest"],
            "generation_id": run.summary["generation_id"], "questions": len(ids), "tries": tries,
            "passes": passes}
