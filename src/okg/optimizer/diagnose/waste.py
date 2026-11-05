"""Wasted work per try, by rule, with no model call (issue #4281).

Efficiency is an objective in its own right (Jason, 2026-10-31): a change
that cuts calls or dollars without losing accuracy is worth keeping. Each
rule below names calls that cost something and bought nothing, with the call
index and a short quote as evidence:

* ``repeated_search``: a search whose arguments equal an earlier one's, or
  whose query shares at least ``jaccard`` of its words with an earlier query
  and whose reply shows no id the earlier replies had not shown;
* ``tool_error``: an error reply (not a timeout);
* ``abandoned_refusal``: an error reply after which the agent never called
  that tool again;
* ``timeout``: a call that timed out;
* ``unused_large_reply``: a reply of at least ``large_bytes`` from which no
  id reached a later call's arguments or the answer;
* ``after_evidence``: in a try whose answer was judged to state EVERY
  required fact, the calls made after each fact's words (at least
  ``evidence_share``, default 0.75) had appeared in some reply; a word-overlap
  test, so marked weak. A try with a missed fact is skipped: later calls may
  have been chasing it;
* ``turn_limit``: the try was forced to a final answer;
* ``heavy_turn``: a model call whose tokens exceed ``heavy_tokens``.

The rules are heuristics and say so: each record carries ``weak`` where the
match is by word overlap. They are cheap enough to run on every replay.
"""
from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .traces import TryTrace

SCHEMA = "okg.akb.waste.v1"
#: The stage a fix for each kind most often touches: a default by rule, named
#: `stage_hint` in each item; the miss labeller decides stages case by case.
STAGE_HINTS = {
    "repeated_search": "organization",      # ranking did not surface what the query wanted
    "tool_error": "served_text",            # the call was formed wrong: descriptions or hints
    "abandoned_refusal": "served_text",     # the refusal text did not show a working call
    "timeout": "organization",              # no index or plan for that read
    "unused_large_reply": "reply_format",   # the reply was bigger than its use
    "after_evidence": "model",              # the agent kept going with the evidence in hand
    "turn_limit": "model",
    "heavy_turn": "reply_format",           # re-sent tool replies dominate prompt tokens
}
#: Typed ids in replies: ``kind:rest`` (node ids, handles, ticket keys) and paths.
_ID = re.compile(r"\b[a-z][a-z0-9_]*:[A-Za-z0-9][\w./@:-]{2,}|\b[\w.-]+/[\w./-]+\.[A-Za-z0-9]{1,5}\b")
_WORD = re.compile(r"[a-z0-9]+")
_STOP = frozenset({"the", "a", "an", "of", "in", "on", "for", "to", "and", "or", "is", "are", "by", "with"})


@dataclass(frozen=True)
class WasteSettings:
    search_tools: tuple[str, ...] = ("search",)
    jaccard: float = 0.8
    large_bytes: int = 32_768
    heavy_tokens: int = 200_000
    #: Share of a fact's words that must appear in replies seen so far.
    evidence_share: float = 0.75


def ids_in(text: str) -> set[str]:
    return set(_ID.findall(text or ""))


def words(text: str) -> set[str]:
    return {w for w in _WORD.findall((text or "").lower()) if w not in _STOP and len(w) > 1}


def _query(args: Mapping[str, Any]) -> str:
    for key in ("query", "pattern", "q", "command"):
        value = args.get(key)
        if isinstance(value, str):
            return value
    return json.dumps(args, sort_keys=True)


def _quote(text: str, limit: int = 160) -> str:
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _jaccard(a: set[str], b: set[str]) -> float:
    return len(a & b) / len(a | b) if a or b else 1.0


def waste_of_try(trace: TryTrace, settings: WasteSettings = WasteSettings()) -> dict[str, Any]:
    """One try's waste record: each wasted call with its rule and evidence."""
    items: list[dict[str, Any]] = []
    calls = list(trace.calls)
    seen_ids: set[str] = set()
    earlier: list[tuple[str, set[str], str]] = []  # (args json, query words, tool)
    for call in calls:
        reply_ids = ids_in(call.reply)
        if call.tool in settings.search_tools and not (call.error or call.timed_out):
            # A failed search is a tool_error or timeout, not an earlier search to repeat.
            args_json = json.dumps(call.args, sort_keys=True)
            qwords = words(_query(call.args))
            for prev_args, prev_words, prev_tool in earlier:
                if prev_tool != call.tool:
                    continue
                same = prev_args == args_json
                near = _jaccard(qwords, prev_words) >= settings.jaccard and not (reply_ids - seen_ids)
                if same or near:
                    items.append({"kind": "repeated_search", "call": call.index, "weak": not same,
                                  "evidence": _quote(_query(call.args))})
                    break
            earlier.append((args_json, qwords, call.tool))
        if call.timed_out:
            items.append({"kind": "timeout", "call": call.index, "weak": False, "evidence": _quote(call.reply)})
        elif call.error:
            items.append({"kind": "tool_error", "call": call.index, "weak": False, "evidence": _quote(call.reply)})
            if not any(c.tool == call.tool for c in calls[call.index + 1:]):
                items.append({"kind": "abandoned_refusal", "call": call.index, "weak": False,
                              "evidence": _quote(call.reply)})
        seen_ids |= reply_ids
    # Large replies nothing later used.
    for call in calls:
        if call.bytes < settings.large_bytes or call.error:
            continue
        later_text = " ".join(json.dumps(c.args, sort_keys=True) for c in calls[call.index + 1:]) + " " + trace.answer
        if not (ids_in(call.reply) & ids_in(later_text)):
            items.append({"kind": "unused_large_reply", "call": call.index, "weak": True,
                          "evidence": f"{call.bytes} bytes; no id from it used later"})
    # Calls after the answer's evidence was in hand.
    if calls and trace.required_facts and len(trace.entailed_facts) >= trace.required_facts:
        needs = [words(text) for text in trace.entailed_facts.values() if words(text)]
        have: set[str] = set()
        for call in calls:
            have |= words(call.reply)
            if needs and all(len(n & have) >= settings.evidence_share * len(n) for n in needs):
                for later in calls[call.index + 1:]:
                    items.append({"kind": "after_evidence", "call": later.index, "weak": True,
                                  "evidence": f"every stated fact's words seen by call {call.index}"})
                break
    if trace.forced_final:
        items.append({"kind": "turn_limit", "call": None, "weak": False, "evidence": str(trace.forced_final)})
    for i, tokens in enumerate(trace.turn_tokens):
        if tokens > settings.heavy_tokens:
            items.append({"kind": "heavy_turn", "call": None, "weak": False,
                          "evidence": f"model call {i}: {tokens} tokens"})
    for item in items:
        item["stage_hint"] = STAGE_HINTS[item["kind"]]
    wasted_calls = {i["call"] for i in items if i["call"] is not None}
    firm_calls = {i["call"] for i in items if i["call"] is not None and not i["weak"]}
    return {
        "schema": SCHEMA, "question_id": trace.question_id, "try": trace.try_number,
        "calls": len(calls), "wasted_calls": len(wasted_calls),
        # Calls named by a firm rule; the rest rest on word overlap only.
        "wasted_calls_firm": len(firm_calls), "wasted_calls_weak_only": len(wasted_calls - firm_calls),
        "by_kind": _count(i["kind"] for i in items), "items": items,
    }


def _count(kinds: Iterable[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for k in kinds:
        out[k] = out.get(k, 0) + 1
    return dict(sorted(out.items()))


def summarize(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Totals over tries: calls, wasted calls, and items by kind (no text)."""
    rows = list(records)
    calls = sum(r["calls"] for r in rows)
    wasted = sum(r["wasted_calls"] for r in rows)
    firm = sum(r["wasted_calls_firm"] for r in rows)
    kinds: dict[str, int] = {}
    tries_with: dict[str, int] = {}
    for r in rows:
        for k, n in r["by_kind"].items():
            kinds[k] = kinds.get(k, 0) + n
            tries_with[k] = tries_with.get(k, 0) + 1
    return {
        "schema": SCHEMA + ".summary", "tries": len(rows), "calls": calls, "wasted_calls": wasted,
        "wasted_share": round(wasted / calls, 4) if calls else 0.0,
        "wasted_calls_firm": firm, "wasted_calls_weak_only": wasted - firm,
        "wasted_share_firm": round(firm / calls, 4) if calls else 0.0,
        "items_by_kind": dict(sorted(kinds.items())), "tries_with_kind": dict(sorted(tries_with.items())),
    }


__all__ = ["SCHEMA", "STAGE_HINTS", "WasteSettings", "ids_in", "summarize", "waste_of_try", "words"]
