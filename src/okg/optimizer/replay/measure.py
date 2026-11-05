"""Per-try rows to an ``okg.akb.measurement.v1`` record and the acceptance-gate inputs.

A *unit* is one judge pass over one try of one question. Answer-based
measures are means over questions of the mean over that question's units
(tries x passes), so every question weighs the same:

* ``required_fact_recall``: entailed required facts / required facts;
* ``answer_correct_rate``: every required fact entailed;
* ``contradicted_claim_rate``: at least one fact contradicted.

A try the answer itself failed (output_limit, empty_answer, answer_timeout,
step_limit, context_overflow) scores recall 0, not correct, not contradicted,
in every pass. A try failed by the runner or the provider (``api_error``,
``other``: ``RUN_FAILURE_LABELS``) says nothing about the answer, so it is
not scored: it leaves the run incomplete.

Cost measures are means over tries: ``usd_per_question`` (the
answer's model spend from the ledger, retries and reserve-charged calls
included; judge spend excluded), ``model_calls_per_try``, ``tokens_per_try``
(input plus output), ``tool_errors_per_try`` and ``latency_seconds_per_try``.

A run is *complete* only when every question has every try and every try
every pass, with no ``judge_failed`` unit and no try failed by the runner or
provider. No record is built for an
incomplete run: a missing unit read as zero would look like a result.
``out_of_scope_reported_rate`` and the hard constraints are not measured by a
replay and stay out of the record, so the scorer reports them unmeasured.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

from okg.optimizer.acceptance import RewriteAcceptanceError, RewriteReplayMeasurement
from okg.optimizer.objectives import (
    MEASUREMENT_SCHEMA,
    MeasurementRecord,
    Objectives,
    ObjectivesError,
    acceptance_thresholds,
    measured_values,
    parse_measurement,
    replay_decision,
    score_objectives,
)
from okg.optimizer.question_sets import LoopView

from .errors import ReplayError, ReplayProblem, refuse

RUNNER_ID = "okg.optimizer.replay"
FRESH_FOR = timedelta(days=7)
UNIT_MEASURES = ("required_fact_recall", "answer_correct_rate", "contradicted_claim_rate")
#: Try failures that say nothing about the answer: the run is incomplete, never scored 0.
RUN_FAILURE_LABELS = ("api_error", "other")
_UNIT_FIELD = {"required_fact_recall": "recall", "answer_correct_rate": "correct",
               "contradicted_claim_rate": "contradicted"}


def completeness(rows: Iterable[Mapping[str, Any]], question_ids: Sequence[str], tries: int,
                 passes: int) -> dict[str, Any]:
    """What is missing from a run: tries not answered or not judged, judge failures, runner failures."""
    seen: dict[tuple[str, int], Mapping[str, Any]] = {(r["question_id"], r["try"]): r for r in rows}
    missing_tries = [(q, t) for q in question_ids for t in range(1, tries + 1) if (q, t) not in seen]
    unjudged = judge_failed = 0
    for row in seen.values():
        by_pass = {p["pass"]: p for p in row.get("passes") or ()}
        for p in range(1, passes + 1):
            unit = by_pass.get(p)
            if unit is None:
                unjudged += 1
            elif unit["status"] == "judge_failed":
                judge_failed += 1
    extra = sorted({q for q, _ in seen} - set(question_ids))
    run_failed = sum(1 for r in seen.values() if r.get("failure") in RUN_FAILURE_LABELS)
    return {"complete": not (missing_tries or unjudged or judge_failed or extra or run_failed),
            "missing_tries": len(missing_tries), "unjudged_units": unjudged,
            "judge_failed_units": judge_failed, "run_failed_tries": run_failed,
            "unknown_questions": len(extra)}


def unit_values(rows: Iterable[Mapping[str, Any]], question_ids: Sequence[str], tries: int, passes: int,
                field: str) -> list[list[list[float]]]:
    """values[question][try][pass] of one unit field, as floats (bools as 0/1)."""
    seen = {(r["question_id"], r["try"]): r for r in rows}
    out: list[list[list[float]]] = []
    for q in question_ids:
        per_q: list[list[float]] = []
        for t in range(1, tries + 1):
            by_pass = {p["pass"]: p for p in seen[(q, t)]["passes"]}
            per_q.append([float(by_pass[p][field]) for p in range(1, passes + 1)])
        out.append(per_q)
    return out


def _nested_mean(values: list[list[list[float]]]) -> float:
    per_q = [sum(v for tr in q for v in tr) / sum(len(tr) for tr in q) for q in values]
    return sum(per_q) / len(per_q)


def run_measures(rows: Sequence[Mapping[str, Any]], question_ids: Sequence[str], tries: int,
                 passes: int) -> dict[str, float]:
    state = completeness(rows, question_ids, tries, passes)
    if not state["complete"]:
        raise refuse("run_incomplete",
                      f"the run is incomplete ({state['missing_tries']} tries missing, {state['unjudged_units']} "
                      f"units unjudged, {state['judge_failed_units']} judge failures, "
                      f"{state['run_failed_tries']} tries failed by the runner or provider); no measurement is built")
    out = {name: _nested_mean(unit_values(rows, question_ids, tries, passes, _UNIT_FIELD[name]))
           for name in UNIT_MEASURES}
    n = len(rows)
    out["usd_per_question"] = sum(float(r["ledger_answer_usd"]) for r in rows) / n
    out["model_calls_per_try"] = sum(r["model_calls"] for r in rows) / n
    out["tokens_per_try"] = sum(r["input_tokens"] + r["output_tokens"] for r in rows) / n
    out["tool_errors_per_try"] = sum(r["tool_errors"] for r in rows) / n
    out["latency_seconds_per_try"] = sum(r["wall_s"] for r in rows) / n
    return {k: round(v, 10) for k, v in out.items()}


def measurement_record(rows: Sequence[Mapping[str, Any]], view: LoopView, *, deployment: str,
                       generation_id: str, run_id: str, setup_digest: str, tries: int,
                       passes: int) -> dict[str, Any]:
    """The okg.akb.measurement.v1 record of one complete run, validated."""
    ids = [q.id for q in view.questions]
    measures = run_measures(rows, ids, tries, passes)
    per_tag: dict[str, dict[str, float]] = {}
    tags = sorted({tag for q in view.questions for tag in q.scope_tags})
    for tag in tags:
        tagged = [q.id for q in view.questions if tag in q.scope_tags]
        tagged_rows = [r for r in rows if r["question_id"] in set(tagged)]
        per_tag[tag] = {name: round(_nested_mean(unit_values(tagged_rows, tagged, tries, passes,
                                                              _UNIT_FIELD[name])), 10)
                        for name in UNIT_MEASURES}
    record = {
        "schema": MEASUREMENT_SCHEMA, "deployment": deployment, "generation_id": generation_id,
        "run_id": run_id, "setup_digest": setup_digest,
        "question_set": {"id": view.set_id, "digest": view.digest},
        "tries": tries, "measures": measures, "per_scope_tag": per_tag,
    }
    try:
        parse_measurement(record)
    except ObjectivesError as exc:
        raise ReplayError([ReplayProblem("measurement_invalid", f"{p.code}: {p.message}", p.pointer)
                           for p in exc.problems]) from exc
    return record


def replay_measurement(objectives: Objectives, record: MeasurementRecord, *, runner_digest: str,
                       command: str, artifact_refs: Sequence[str], measured_at: datetime) -> RewriteReplayMeasurement:
    """The acceptance gate's RewriteReplayMeasurement for a same-setup run.

    Baseline and trial are the same generation (a replay measures one
    generation); trial_id is the run id; fresh for seven days. The decision is
    the scorer's: unmeasured hard constraints make it FAIL, never PASS."""
    try:
        score = score_objectives(objectives, record, deployment=record.deployment)
        policy_id, thresholds = acceptance_thresholds(objectives)
        return _build(record, score, policy_id, thresholds, runner_digest, command, artifact_refs, measured_at)
    except ObjectivesError as exc:
        raise ReplayError([ReplayProblem(p.code, p.message, p.pointer) for p in exc.problems]) from exc
    except RewriteAcceptanceError as exc:
        raise refuse("acceptance_input_invalid", str(exc)) from exc


def _build(record: MeasurementRecord, score: Any, policy_id: str, thresholds: Mapping[str, float],
           runner_digest: str, command: str, artifact_refs: Sequence[str],
           measured_at: datetime) -> RewriteReplayMeasurement:
    return RewriteReplayMeasurement(
        measurement_id=f"{record.run_id}:{record.setup_digest}",
        trial_id=record.run_id,
        workload_id=record.question_set_id or "",
        workload_owner=record.deployment,
        baseline_generation_id=record.generation_id,
        trial_generation_id=record.generation_id,
        runner_id=RUNNER_ID,
        runner_digest=runner_digest,
        measurement_command=command,
        artifact_refs=tuple(artifact_refs),
        metric_values=measured_values(score),
        policy_id=policy_id,
        threshold_values=thresholds,
        decision=replay_decision(score),
        measured_at=measured_at,
        fresh_until=measured_at + FRESH_FOR,
    )
