"""Write replay evidence that is safe to commit: numbers, digests, ids, names.

Evidence is built from fixed shapes (setup, run, report), and the writer
checks it again before writing:

* every mapping key is on ``ALLOWED_KEYS`` (fixed names and the measure names);
* every string is one short token (``SAFE_TOKEN``: no spaces, at most 160
  characters), except the fixed notes in ``FIXED_TEXT``;
* no 21-character stretch of any private text (question, required fact,
  reference answer, answer, judge rationale) appears anywhere in it.

Scope tags are left out (a run's evidence counts them); per-tag values stay
in the private run folder. A refusal names the offending key or pointer,
never the text.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from okg.optimizer.objectives import MEASURES

from .agent import FAILURE_LABELS
from .compare import DETERMINISM_NOTE
from .errors import refuse
from .setup import flatten

EVIDENCE_SCHEMA = "okg.replay.evidence.v1"
SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_.:@/+=#-]{0,160}$")
FIXED_TEXT = frozenset({DETERMINISM_NOTE, "questions (paired), then tries, then judge passes", "widest over seeds"})
PRIVATE_WINDOW = 21
ALLOWED_KEYS = frozenset({
    "schema", "kind", "digest", "fields", "run_id", "setup_digest", "generation_id", "deployment",
    "question_set", "id", "tries", "passes", "questions", "held_out", "complete", "missing_tries",
    "unjudged_units", "judge_failed_units", "unknown_questions", "measures", "scope_tags", "spend",
    "run_usd", "total_usd", "answer_usd", "judge_usd", "judge_calls_cached", "judge_calls_made",
    "failed_tries", "forced_final_tries", "runs", "setup", "same", "differing_fields", "change",
    "bootstrap", "replicates", "seeds", "level", "resampled", "range_rule", "run_a", "run_b",
    "difference", "range", "contains_zero", "floor_half_width", "gated", "judge_only", "three_tries",
    "cost_ratios", "ratio", "within_floor", "determinism", "decision", "policy_id", "objectives",
    "status", "value", "threshold", "reason", "entries", "measure", "unit", "blocking", "passes_objectives",
    "failures_by_label", "run_failed_tries", "budget", "expected_usd_per_try", "check_after_tries", "max_ratio",
    "checked", "finished_tries", "usd_per_try", "stop_codes", "stop_phase", "reserve_charged_calls",
    "reserve_charged_usd", "workers",
}) | frozenset(MEASURES) | frozenset(FAILURE_LABELS)


def setup_evidence(setup: Mapping[str, Any]) -> dict[str, Any]:
    rows = [[name, _safe_field(name, value)] for name, value in flatten(setup["fields"]).items()]
    return {"schema": EVIDENCE_SCHEMA, "kind": "setup", "digest": setup["digest"], "fields": rows,
            "workers": setup.get("workers")}


def _safe_field(name: str, value: Any) -> Any:
    """An env_files prefix that is not a short token ("Bearer ") is shown by its sha256.

    The setup record keeps the prefix itself; only this commit-safe copy hashes it.
    """
    if name != "serve_env.env_files" or not isinstance(value, list):
        return value
    out = []
    for item in value:
        if isinstance(item, list) and len(item) == 2 and isinstance(item[1], str) and not SAFE_TOKEN.match(item[1]):
            item = [item[0], "sha256:" + hashlib.sha256(item[1].encode("utf-8")).hexdigest()]
        out.append(item)
    return out


def run_evidence(summary: Mapping[str, Any], record: Mapping[str, Any] | None,
                 score: Mapping[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "schema": EVIDENCE_SCHEMA, "kind": "run", "run_id": summary["run_id"],
        "setup_digest": summary["setup_digest"], "generation_id": summary["generation_id"],
        "deployment": summary["deployment"],
        "question_set": {"id": summary["question_set_id"], "digest": summary["question_set_digest"]},
        "questions": len(summary["question_ids"]), "held_out": summary["held_out"],
        "tries": summary["tries"], "passes": summary["passes"],
        "workers": summary.get("workers"),
        "complete": summary["completeness"]["complete"],
        "missing_tries": summary["completeness"]["missing_tries"],
        "unjudged_units": summary["completeness"]["unjudged_units"],
        "judge_failed_units": summary["completeness"]["judge_failed_units"],
        "run_failed_tries": summary["completeness"]["run_failed_tries"],
        "status": summary["status"], "stop_phase": summary["stop_phase"],
        "stop_codes": [p["code"] for p in summary["stop"] or ()],
        "failed_tries": summary["failed_tries"], "failures_by_label": dict(summary["failures_by_label"]),
        "budget": None if summary.get("budget") is None else dict(summary["budget"]),
        "forced_final_tries": summary["forced_final_tries"],
        "judge_calls_made": summary["judge_calls_made"], "judge_calls_cached": summary["judge_calls_cached"],
        "spend": dict(summary["spend"]),
    }
    if record is not None:
        out["measures"] = dict(record["measures"])
        out["scope_tags"] = len(record.get("per_scope_tag") or {})
    if score is not None:
        out["objectives"] = {
            "policy_id": score["policy_id"], "passes_objectives": score["passes"],
            "entries": [{k: v for k, v in e.items() if k in ("kind", "id", "status", "measure", "unit", "value",
                                                              "threshold", "reason", "blocking")}
                        for e in score["entries"]],
        }
    return out


def report_evidence(report: Mapping[str, Any]) -> dict[str, Any]:
    return {"schema": EVIDENCE_SCHEMA, "kind": "replay_report", **{k: v for k, v in report.items() if k != "schema"}}


def check_evidence(obj: Any, private_texts: Iterable[str], pointer: str = "") -> None:
    """Refuse a key not allowed, a string that is not a safe token, or a private-text stretch."""
    strings: list[str] = []
    _walk(obj, pointer, strings)
    windows = set()
    for s in strings:
        for i in range(max(0, len(s) - PRIVATE_WINDOW + 1)):
            windows.add(s[i:i + PRIVATE_WINDOW])
    if not windows:
        return
    for text in private_texts:
        for i in range(max(0, len(text) - PRIVATE_WINDOW + 1)):
            if text[i:i + PRIVATE_WINDOW] in windows:
                raise refuse("evidence_private_text",
                             "the evidence holds a stretch of private text (question, fact, answer or rationale)")


def _walk(obj: Any, pointer: str, strings: list[str]) -> None:
    if isinstance(obj, Mapping):
        for key, value in obj.items():
            if key not in ALLOWED_KEYS:
                raise refuse("evidence_key_not_allowed", f"key at {pointer}/<{len(str(key))} chars> is not allowed",
                             pointer)
            strings.append(str(key))
            _walk(value, f"{pointer}/{key}", strings)
    elif isinstance(obj, list | tuple):
        for index, value in enumerate(obj):
            _walk(value, f"{pointer}/{index}", strings)
    elif isinstance(obj, str):
        if obj not in FIXED_TEXT and not SAFE_TOKEN.match(obj):
            raise refuse("evidence_text_not_allowed",
                         f"the string at {pointer} is not a short token ({len(obj)} chars)", pointer)
        if obj not in FIXED_TEXT:
            strings.append(obj)
    elif obj is None or isinstance(obj, bool | int | float):
        return
    else:
        raise refuse("evidence_value_not_allowed", f"value at {pointer} is a {type(obj).__name__}", pointer)


def write_evidence(obj: Mapping[str, Any], path: Path, private_texts: Iterable[str]) -> Path:
    check_evidence(obj, list(private_texts))
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=1, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return path
