"""Diagnosis of misses and wasted calls (issue #4281, PACT akb-diagnose).

Synthetic traces only: no deployment, no question set of any real program,
no model call (the labeler runs against a fake client).
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from okg.optimizer.diagnose import agreement, labeler, traces, waste
from okg.optimizer.diagnose.__main__ import main as cli
from okg.optimizer.diagnose.traces import CallTrace, Miss, TryTrace


def _call(i: int, tool: str, args: dict[str, Any], reply: str, **kw: Any) -> CallTrace:
    return CallTrace(index=i, tool=tool, args=args, reply=reply, bytes=kw.pop("bytes", len(reply)), **kw)


# ── agreement ──────────────────────────────────────────────────────────


def test_kappa_matches_the_hand_census_formula() -> None:
    a = {1: "a", 2: "a", 3: "c", 4: "c", 5: "g", 6: "g", 7: "f", 8: "c"}
    b = {1: "a", 2: "c", 3: "c", 4: "c", 5: "g", 6: "c", 7: "f", 8: "c", 9: "a"}
    r = agreement.cohen_kappa(a, b)
    # p_a = 6/8; marginals a: a2 c3 g2 f1, b: a1 c5 g1 f1 -> p_e = (2+15+2+1)/64
    pa, pe = 6 / 8, 20 / 64
    assert r["compared"] == 8 and r["agree"] == 6 and r["only_in_b"] == 1
    assert r["kappa"] == round((pa - pe) / (1 - pe), 4)
    assert r["disagreements"] == {"a->c": 1, "g->c": 1}


def test_kappa_names_what_it_could_not_compare() -> None:
    r = agreement.cohen_kappa({1: "a", 2: "x"}, {1: "a", 2: "a", 3: "b"})
    assert r["compared"] == 1 and r["unlabelled_or_other"] == 1 and r["only_in_b"] == 1
    assert agreement.cohen_kappa({}, {})["kappa"] is None


def test_labels_from_rows_keys_by_the_named_fields() -> None:
    rows = [{"rid": "r", "qid": "q", "attempt": 1, "atom": "x", "cls": "c"},
            {"rid": "r", "qid": "q", "attempt": 1, "atom": "y"}]
    assert agreement.labels_from_rows(rows, ["rid", "qid", "attempt", "atom"], "cls") == {("r", "q", "1", "x"): "c"}


# ── waste rules ────────────────────────────────────────────────────────


def test_each_waste_rule_names_its_call() -> None:
    big = "rows: " + " ".join(f"node:item-{i}" for i in range(4000))
    trace = TryTrace(
        question_id="q1", try_number=1, answer="It is blue (node:widget-1).",
        forced_final="model_turn_limit", turn_tokens=(1_000, 250_000),
        entailed_facts={"colour": "widget colour blue"}, required_facts=1,
        calls=(
            _call(0, "search", {"query": "widget colour"}, "search ok · node:widget-1 colour blue"),
            _call(1, "search", {"query": "widget colour"}, "search ok · node:widget-1 colour blue"),
            _call(2, "search", {"query": "colour widget the"}, "search ok · node:widget-1"),
            _call(3, "expand", {"input": "node:widget-1"}, "refused unsupported_operator_form", error=True),
            _call(4, "inspect", {"node": "x"}, "tool call timed out after 120 s", error=True, timed_out=True),
            _call(5, "filter", {"input": "node:widget-1"}, big),
        ),
    )
    rec = waste.waste_of_try(trace)
    kinds = {(i["kind"], i["call"]) for i in rec["items"]}
    assert ("repeated_search", 1) in kinds          # same arguments
    assert ("repeated_search", 2) in kinds          # same words, no new id
    assert ("tool_error", 3) in kinds and ("abandoned_refusal", 3) in kinds
    assert ("timeout", 4) in kinds and ("tool_error", 4) not in kinds
    assert ("unused_large_reply", 5) in kinds
    assert ("after_evidence", 1) in kinds and ("after_evidence", 0) not in kinds
    assert ("turn_limit", None) in kinds and ("heavy_turn", None) in kinds
    assert rec["calls"] == 6 and rec["wasted_calls"] == 5
    # Calls 1, 3, 4 have a firm rule; 2 and 5 rest on word overlap only.
    assert rec["wasted_calls_firm"] == 3 and rec["wasted_calls_weak_only"] == 2
    assert all(i["stage_hint"] == waste.STAGE_HINTS[i["kind"]] for i in rec["items"])
    # A try with a missed fact gets no after_evidence: later calls may be chasing it.
    partial = waste.waste_of_try(TryTrace(**{**trace.__dict__, "required_facts": 2}))
    assert not [i for i in partial["items"] if i["kind"] == "after_evidence"]
    summary = waste.summarize([rec, waste.waste_of_try(TryTrace("q2", 1, ()))])
    assert summary["tries"] == 2 and summary["wasted_calls"] == 5 and summary["tries_with_kind"]["timeout"] == 1


def test_a_reworded_query_that_finds_a_new_row_is_not_waste() -> None:
    trace = TryTrace("q", 1, (
        _call(0, "search", {"query": "rule history"}, "node:rule-1"),
        _call(1, "search", {"query": "history of rule"}, "node:rule-2 new row"),  # same words, new id
        _call(2, "search", {"query": "deleted replication rules"}, "node:rule-3"),
        _call(3, "search", {"query": "rule history"}, "node:rule-1"),  # identical arguments
    ))
    assert [(i["kind"], i["call"]) for i in waste.waste_of_try(trace)["items"]] == [("repeated_search", 3)]


# ── adapters ───────────────────────────────────────────────────────────


def _jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    return path


def test_replay_run_misses_are_facts_no_pass_entailed(tmp_path: Path) -> None:
    _jsonl(tmp_path / "answers.jsonl", [
        {"question_id": "q1", "try": 1, "status": "answered", "answer": "blue", "forced_final": None,
         "turns": [{"input_tokens": 10, "output_tokens": 2}],
         "calls": [{"tool": "search", "args": {"query": "colour"}, "reply": "node:w blue", "bytes": 11,
                    "cut_from_bytes": None, "error": False, "timed_out": False}]},
        {"question_id": "q2", "try": 1, "status": "failed", "answer": "", "calls": []},
        {"question_id": "q3", "try": 1, "status": "answered", "answer": "x", "calls": []},  # not judged yet
    ])
    _jsonl(tmp_path / "judgments.jsonl", [
        {"question_id": "q1", "try": 1, "pass": 1, "status": "judged",
         "judgments": [{"atom_id": "colour", "outcome": "entailed"}, {"atom_id": "size", "outcome": "not_mentioned"}]},
        {"question_id": "q1", "try": 1, "pass": 2, "status": "judged",
         "judgments": [{"atom_id": "colour", "outcome": "not_mentioned"}, {"atom_id": "size", "outcome": "not_mentioned"}]},
    ])
    facts = {"q1": {"colour": "the colour is blue", "size": "the size is small"}, "q2": {"x": "fact x"},
             "q3": {"y": "fact y"}}
    tries, misses = traces.load_replay_run(tmp_path, facts, {"q1": "Which colour?"})
    assert [m.key for m in misses] == [("q1", 1, "size"), ("q2", 1, "x")]
    t1 = traces.index(tries)[("q1", 1)]
    assert t1.calls[0].args == {"query": "colour"} and t1.question == "Which colour?"
    assert t1.entailed_facts == {"colour": "the colour is blue"} and t1.turn_tokens == (12,)
    assert ("q3", 1) not in traces.index(tries)


def test_harness_records_are_read_with_python_repr_arguments(tmp_path: Path) -> None:
    records = _jsonl(tmp_path / "records.jsonl", [{"qid": "q1", "attempt": 2, "answer": {
        "answer": "blue", "forced_final_reason": None,
        "calls": [{"tool": "search", "args": "{'query': 'colour', 'limit': 8}", "response": "node:w",
                   "bytes": "6", "cut_from_bytes": "None", "error": "False", "timed_out": "False"}]}}])
    misses = _jsonl(tmp_path / "misses.jsonl", [
        {"rid": "r1", "qid": "q1", "attempt": 2, "atom": "size", "text": "small"},
        {"rid": "r2", "qid": "q9", "attempt": 1, "atom": "z", "text": "z"},
    ])
    tries, wanted = traces.load_harness_records(records, misses, run_id="r1")
    assert [m.key for m in wanted] == [("q1", 2, "size")]
    with_q, _ = traces.load_harness_records(records, misses, run_id="r1", questions={"q1": "Which size?"})
    assert with_q[0].question == "Which size?"
    every, _ = traces.load_harness_records(records, misses, run_id="r2", all_tries=True)
    assert [t.key for t in every] == [("q1", 2)]
    assert traces.load_harness_records(records, misses, run_id="r2")[0] == []
    call = tries[0].calls[0]
    assert call.args == {"query": "colour", "limit": 8} and not call.error and not call.cut and call.bytes == 6


# ── labeler (fake client) ──────────────────────────────────────────────


class _FakeClient:
    model, reasoning_effort = "gpt-6-luna", "high"

    def __init__(self, replies: list[str]) -> None:
        self.replies = list(replies)
        self.sent: list[dict[str, Any]] = []

    async def respond(self, **kw: Any) -> Any:
        self.sent.append(kw)
        return SimpleNamespace(text=self.replies.pop(0))


GOOD = json.dumps({"cls": "c", "stage": "model", "call": 0, "quote": "node:w blue", "note": "seen", "confidence": "high"})


def test_a_label_is_checked_cached_and_retried(tmp_path: Path) -> None:
    trace = TryTrace("q1", 1, (_call(0, "search", {"query": "colour"}, "node:w blue"),), answer="unknown",
                     question="Which colour?")
    miss = Miss("q1", 1, "colour", "the colour is blue")
    cache = labeler.LabelCache(tmp_path)
    bad = json.dumps({"cls": "c", "stage": "model", "call": 7, "quote": "", "note": "", "confidence": "high"})
    client = _FakeClient([bad, GOOD])
    first = asyncio.run(labeler.label_miss(client, cache, trace, miss))
    assert first["status"] == "labelled" and first["cls"] == "c" and not first["cached"] and len(client.sent) == 2
    assert client.sent[0]["text_format"]["strict"] is True and client.sent[0]["purpose"] == "diagnose"
    again = asyncio.run(labeler.label_miss(_FakeClient([]), cache, trace, miss))
    assert again["cached"] and again["stage"] == "model"
    failed = asyncio.run(labeler.label_miss(_FakeClient(["no", "{}"]), labeler.LabelCache(tmp_path / "x"), trace, miss))
    assert failed["status"] == "label_failed"
    out = asyncio.run(labeler.label_misses(_FakeClient([]), cache, {}, [miss]))
    assert out[0]["status"] == "trace_missing"


@pytest.mark.parametrize("raw, why", [
    ("[]", "not an object"),
    (json.dumps({"cls": "z", "stage": "model", "call": None, "confidence": "high"}), "unknown class"),
    (json.dumps({"cls": "a", "stage": "magic", "call": None, "confidence": "high"}), "unknown stage"),
    (json.dumps({"cls": "a", "stage": "model", "call": True, "confidence": "high"}), "not a call index"),
])
def test_check_label_refuses_by_name(raw: str, why: str) -> None:
    assert why in labeler.check_label(raw, 3)


def test_the_transcript_is_bounded_and_says_what_it_cut() -> None:
    calls = tuple(_call(i, "search", {"query": f"q{i}"}, "x" * 5_000) for i in range(30))
    text = labeler.transcript(TryTrace("q", 1, calls), labeler.LabelSettings(reply_chars=100, transcript_chars=1_000))
    assert "left out here for you; the agent saw them" in text and "calls in the middle not shown" in text
    assert len(text) < 1_500


def test_the_prompt_names_every_class_and_stage() -> None:
    for cls in agreement.CENSUS_CLASSES:
        assert f"\n{cls}. " in labeler.SYSTEM_PROMPT
    for stage in labeler.STAGES:
        assert f"- {stage}:" in labeler.SYSTEM_PROMPT


# ── CLI ────────────────────────────────────────────────────────────────


def test_cli_waste_and_agree(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    records = _jsonl(tmp_path / "records.jsonl", [{"qid": "q1", "attempt": 1, "answer": {"answer": "", "calls": [
        {"tool": "search", "args": {"query": "a"}, "response": "node:x"},
        {"tool": "search", "args": {"query": "a"}, "response": "node:x"}]}}])
    misses = _jsonl(tmp_path / "misses.jsonl", [{"rid": "r", "qid": "q1", "attempt": 1, "atom": "f", "text": "t"}])
    assert cli(["waste", "--records", str(records), "--misses", str(misses), "--out", str(tmp_path / "w.jsonl")]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["wasted_calls"] == 1 and summary["items_by_kind"] == {"repeated_search": 1}
    a = _jsonl(tmp_path / "a.jsonl", [{"rid": "r", "qid": "q1", "attempt": 1, "atom": "f", "cls": "c"}])
    b = _jsonl(tmp_path / "b.jsonl", [{"rid": "r", "qid": "q1", "attempt": 1, "atom": "f", "cls": "c"}])
    assert cli(["agree", "--a", str(a), "--b", str(b), "--key", "rid,qid,attempt,atom"]) == 0
    assert json.loads(capsys.readouterr().out)["agree"] == 1


# ── review fixes ───────────────────────────────────────────────────────


def test_a_retry_of_a_failed_search_is_not_a_repeat() -> None:
    trace = TryTrace("q", 1, (
        _call(0, "search", {"query": "x"}, "tool call timed out", error=True, timed_out=True),
        _call(1, "search", {"query": "x"}, "node:a"),
    ))
    kinds = [(i["kind"], i["call"]) for i in waste.waste_of_try(trace)["items"]]
    assert kinds == [("timeout", 0)]


def test_a_partly_judged_try_names_no_misses_and_the_last_row_wins(tmp_path: Path) -> None:
    _jsonl(tmp_path / "answers.jsonl", [
        {"question_id": "q1", "try": 1, "status": "failed", "answer": "", "calls": []},
        {"question_id": "q1", "try": 1, "status": "answered", "answer": "blue", "calls": [],
         "forced_final": "None"},
        {"question_id": "q2", "try": 1, "status": "answered", "answer": "x", "calls": []},
    ])
    _jsonl(tmp_path / "judgments.jsonl", [
        {"question_id": "q1", "try": 1, "pass": 1, "status": "judged", "judgments": [{"atom_id": "f", "outcome": "entailed"}]},
        {"question_id": "q1", "try": 1, "pass": 2, "status": "judged", "judgments": []},
        {"question_id": "q2", "try": 1, "pass": 1, "status": "judged", "judgments": []},
        {"question_id": "q2", "try": 1, "pass": 2, "status": "judge_failed"},
    ])
    tries, misses = traces.load_replay_run(tmp_path, {"q1": {"f": "fact"}, "q2": {"g": "fact"}}, passes=2)
    assert [t.key for t in tries] == [("q1", 1)] and misses == []
    assert tries[0].forced_final is None and tries[0].required_facts == 1


def test_a_misses_file_of_several_runs_needs_a_run_id(tmp_path: Path) -> None:
    records = _jsonl(tmp_path / "records.jsonl", [])
    misses = _jsonl(tmp_path / "misses.jsonl", [
        {"rid": "r1", "qid": "q", "attempt": 1, "atom": "a"}, {"rid": "r2", "qid": "q", "attempt": 1, "atom": "a"},
    ])
    with pytest.raises(ValueError, match="2 runs"):
        traces.load_harness_records(records, misses)


def test_quotes_are_verified_against_the_cited_reply() -> None:
    trace = TryTrace("q", 1, (_call(0, "search", {}, "Rule  state:\nSUSPENDED"),), answer="It is active.")
    assert labeler.quote_verified({"call": 0, "quote": "rule state: suspended"}, trace)
    assert labeler.quote_verified({"call": None, "quote": "it is active"}, trace)
    assert not labeler.quote_verified({"call": 0, "quote": "made up text"}, trace)
    assert not labeler.quote_verified({"call": 0, "quote": ""}, trace)


def test_a_failed_or_cut_model_call_fails_one_miss_only(tmp_path: Path) -> None:
    from okg.optimizer.replay.openai_client import ModelCallError

    class Broken(_FakeClient):
        async def respond(self, **kw: Any) -> Any:
            raise ModelCallError("HTTP 500 after retries")

    trace = TryTrace("q1", 1, (_call(0, "search", {}, "node:w blue"),))
    miss = Miss("q1", 1, "colour", "blue")
    failed = asyncio.run(labeler.label_miss(Broken([]), labeler.LabelCache(tmp_path / "a"), trace, miss))
    assert failed["status"] == "label_failed" and "model call failed" in failed["error"]
    cut = SimpleNamespace(text="", status="incomplete", incomplete_reason="max_output_tokens")

    class Cut(_FakeClient):
        async def respond(self, **kw: Any) -> Any:
            return cut

    out = asyncio.run(labeler.label_miss(Cut([]), labeler.LabelCache(tmp_path / "b"), trace, miss))
    assert out["status"] == "label_failed" and "max_output_tokens" in out["error"]
    rows: list[dict[str, Any]] = []
    asyncio.run(labeler.label_misses(_FakeClient([GOOD]), labeler.LabelCache(tmp_path / "c"),
                                     {("q1", 1): trace}, [miss], on_row=rows.append))
    assert rows[0]["status"] == "labelled" and rows[0]["quote_verified"] is True


def test_a_long_transcript_keeps_the_first_and_last_calls() -> None:
    calls = tuple(_call(i, "search", {"query": f"q{i}"}, f"reply {i} " + "x" * 400) for i in range(40))
    text = labeler.transcript(TryTrace("q", 1, calls), labeler.LabelSettings(reply_chars=200, transcript_chars=2_000))
    assert "[0] search" in text and "[39] search" in text and "calls in the middle not shown" in text
    assert len(text) < 2_200
    assert labeler.check_label(GOOD, 0) == "call 0 is not a call index of this try"


def test_cli_agree_keys_each_side_by_its_own_fields(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    hand = _jsonl(tmp_path / "hand.jsonl", [{"rid": "r", "qid": "q", "attempt": 1, "atom": "f", "cls": "c"}])
    machine = _jsonl(tmp_path / "machine.jsonl", [
        {"rid": "r", "question_id": "q", "try": 1, "fact_id": "f", "cls": "c", "status": "labelled"}])
    assert cli(["agree", "--a", str(hand), "--b", str(machine), "--key", "rid,qid,attempt,atom",
                "--b-key", "rid,question_id,try,fact_id"]) == 0
    assert json.loads(capsys.readouterr().out)["compared"] == 1
    with pytest.raises(SystemExit):
        cli(["agree", "--a", str(hand), "--b", str(machine), "--key", "rid,qid,attempt,atom"])


def test_each_call_shows_how_many_fact_words_its_full_reply_holds() -> None:
    reply = "head " + "x" * 5_000 + " the rule weight was 1000 tail"
    trace = TryTrace("q", 1, (_call(0, "search", {}, reply), _call(1, "search", {}, "nothing here")))
    text = labeler.transcript(trace, labeler.LabelSettings(reply_chars=200), "rule weight 1000")
    assert "fact words in full reply: 3/3" in text and "fact words in full reply: 0/3" in text
    assert "1000 tail" in text  # the tail of a long reply is kept
