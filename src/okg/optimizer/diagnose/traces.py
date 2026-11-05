"""One trace format for diagnosis, and adapters into it (issue #4281).

A *try* is one answer to one question: the calls the agent made (tool,
arguments, reply text, error and timeout flags, reply size) and the final
answer. A *miss* is a required fact of a question that no judge pass found
entailed in that try's answer (the hand census's definition).

Two sources are read:

* a replay run folder written by ``okg.optimizer.replay`` (``answers.jsonl``
  with each call's ``args`` and ``reply``, ``judgments.jsonl``); facts and
  question text come from the run's question set;
* the record format of the earlier CompOps harness (lane re's
  ``records.jsonl`` with ``answer.calls[].{tool,args,response}``) plus a
  misses file with one row per missed fact. This is what the hand census
  labelled, so it is the reference for agreement.

Everything here is private data (questions, answers, replies); nothing in
this module writes it anywhere.
"""
from __future__ import annotations

import ast
import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CallTrace:
    index: int
    tool: str
    args: Mapping[str, Any]
    reply: str
    error: bool = False
    timed_out: bool = False
    bytes: int = 0
    cut: bool = False


@dataclass(frozen=True)
class TryTrace:
    question_id: str
    try_number: int
    calls: tuple[CallTrace, ...]
    answer: str = ""
    question: str = ""
    forced_final: str | None = None
    #: Tokens per model call, when the source records them (replay runs do).
    turn_tokens: tuple[int, ...] = ()
    #: Required facts this try's answer was judged to state: {fact_id: text}.
    entailed_facts: Mapping[str, str] = field(default_factory=dict)
    #: How many required facts the question has (0 when the source does not say).
    required_facts: int = 0

    @property
    def key(self) -> tuple[str, int]:
        return (self.question_id, self.try_number)


@dataclass(frozen=True)
class Miss:
    question_id: str
    try_number: int
    fact_id: str
    fact_text: str

    @property
    def key(self) -> tuple[str, int, str]:
        return (self.question_id, self.try_number, self.fact_id)


def _read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _as_args(raw: Any) -> Mapping[str, Any]:
    """Arguments as a mapping; the older harness stored some as Python reprs."""
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        for parse in (json.loads, ast.literal_eval):
            try:
                value = parse(raw)
            except (ValueError, SyntaxError, TypeError, RecursionError, MemoryError):
                continue
            if isinstance(value, Mapping):
                return dict(value)
        return {"_raw": raw}
    return {}


def _flag(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def _absent_none(value: Any) -> str | None:
    """None for a missing value and for the harness's "None" / "False" / "" strings."""
    if value is None or (isinstance(value, str) and value.strip() in ("", "None", "False", "null")):
        return None
    return str(value)


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _call(index: int, raw: Mapping[str, Any]) -> CallTrace:
    reply = raw.get("reply")
    if reply is None:
        reply = raw.get("response") or raw.get("result_head") or ""
    cut = raw.get("cut_from_bytes")
    return CallTrace(
        index=index,
        tool=str(raw.get("tool") or ""),
        args=_as_args(raw.get("args")),
        reply=str(reply),
        error=_flag(raw.get("error")),
        timed_out=_flag(raw.get("timed_out")),
        bytes=_int(raw.get("bytes")) or len(str(reply).encode("utf-8")),
        cut=cut not in (None, "", "None"),
    )


# ── replay run folders ───────────────────────────────────────────────


def load_replay_run(run_dir: Path, facts: Mapping[str, Mapping[str, str]],
                    questions: Mapping[str, str] | None = None, *, passes: int = 1,
                    ) -> tuple[list[TryTrace], list[Miss]]:
    """Tries and misses of one replay run.

    ``facts`` maps question id -> {required fact id: text} (from the run's
    question set). A fact is missed when no judged pass of the try found it
    entailed; a try whose answer failed misses every required fact. An
    answered try is loaded only once all ``passes`` judge passes succeeded:
    a later pass may still find a fact, and the census counts a miss only
    when no pass did. When a resumed run wrote a row twice, the last wins.
    """
    run_dir = Path(run_dir)
    entailed: dict[tuple[str, int], set[str]] = {}
    judged_passes: dict[tuple[str, int], set[int]] = {}
    for row in _read_jsonl(run_dir / "judgments.jsonl"):
        if row.get("status") != "judged":
            continue
        key = (row["question_id"], int(row["try"]))
        judged_passes.setdefault(key, set()).add(int(row.get("pass", 1)))
        bucket = entailed.setdefault(key, set())
        for j in row.get("judgments") or ():
            if j.get("outcome") == "entailed":
                bucket.add(j["atom_id"])
    rows = {(r["question_id"], int(r["try"])): r for r in _read_jsonl(run_dir / "answers.jsonl")}
    tries: list[TryTrace] = []
    misses: list[Miss] = []
    for (qid, t), row in sorted(rows.items()):
        q_facts = facts.get(qid, {})
        answered = row.get("status") == "answered"
        if answered and len(judged_passes.get((qid, t), ())) < passes:
            continue  # not fully judged yet: no misses can be named
        got = entailed.get((qid, t), set()) if answered else set()
        tries.append(TryTrace(
            question_id=qid, try_number=t,
            calls=tuple(_call(i, c) for i, c in enumerate(row.get("calls") or ())),
            answer=str(row.get("answer") or ""),
            question=(questions or {}).get(qid, ""),
            forced_final=_absent_none(row.get("forced_final")),
            turn_tokens=tuple(_int(x.get("input_tokens")) + _int(x.get("output_tokens"))
                              for x in row.get("turns") or ()),
            entailed_facts={f: q_facts[f] for f in got if f in q_facts},
            required_facts=len(q_facts),
        ))
        misses.extend(Miss(qid, t, f, text) for f, text in sorted(q_facts.items()) if f not in got)
    return tries, misses


# ── the earlier CompOps harness (the hand census's input) ─────────────


def load_harness_records(records: Path, misses: Path, *, run_id: str | None = None,
                         all_tries: bool = False, questions: Mapping[str, str] | None = None,
                         ) -> tuple[list[TryTrace], list[Miss]]:
    """Tries from a harness ``records.jsonl`` and misses from a misses file.

    A misses row names ``qid``, ``attempt``, ``atom`` and ``text`` (and
    ``rid``, kept to ``run_id`` when given; a file naming more than one run
    is refused without ``run_id``, since the records belong to one run).
    Only tries with a miss are loaded, unless ``all_tries``. Their entailed
    facts are not known (the misses file lists misses only), so
    ``entailed_facts`` stays empty.
    """
    wanted: list[Miss] = []
    rows = list(_read_jsonl(misses))
    runs = {r.get("rid") for r in rows if r.get("rid") is not None}
    if run_id is None and len(runs) > 1:
        raise ValueError(f"the misses file names {len(runs)} runs; pass run_id to pick the one these records are")
    for row in rows:
        if run_id is not None and row.get("rid") != run_id:
            continue
        wanted.append(Miss(str(row["qid"]), int(row["attempt"]), str(row["atom"]), str(row.get("text") or "")))
    keys = {(m.question_id, m.try_number) for m in wanted}
    tries: list[TryTrace] = []
    for row in _read_jsonl(records):
        key = (str(row["qid"]), int(row["attempt"]))
        if not all_tries and key not in keys:
            continue
        answer = row.get("answer") or {}
        tries.append(TryTrace(
            question_id=key[0], try_number=key[1],
            calls=tuple(_call(i, c) for i, c in enumerate(answer.get("calls") or ())),
            answer=str(answer.get("answer") or ""),
            question=(questions or {}).get(key[0], ""),
            forced_final=_absent_none(answer.get("forced_final_reason")) or (
                "forced" if _flag(answer.get("forced_final")) else None),
        ))
    return tries, wanted


def index(tries: Iterable[TryTrace]) -> dict[tuple[str, int], TryTrace]:
    return {t.key: t for t in tries}


__all__ = ["CallTrace", "Miss", "TryTrace", "index", "load_harness_records", "load_replay_run"]
