"""Replay runner units (PACT akb-replay-runner, #4279): run file, setup digest,
ledger and cap, OpenAI client pricing, judge and cache, measures, compare,
evidence. No network: OpenAI is httpx.MockTransport; questions are synthetic.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

import httpx
import pytest

from okg.optimizer.objectives import load_objectives, parse_measurement, validate_measurement
from okg.optimizer.question_sets import parse_question_set
from okg.optimizer.replay import judge as judge_mod
from okg.optimizer.replay.compare import DETERMINISM_NOTE, RunData, check_one_change, compare_runs
from okg.optimizer.replay.config import load_config
from okg.optimizer.replay.errors import CapReached, ProviderStop, ReplayError, SpendUnknown
from okg.optimizer.replay.evidence import check_evidence, report_evidence, setup_evidence
from okg.optimizer.replay.ledger import Ledger
from okg.optimizer.replay.measure import completeness, measurement_record, replay_measurement
from okg.optimizer.replay.openai_client import OpenAIResponses, cost_of, key_from_env, mask
from okg.optimizer.replay.setup import build_setup, check_setup, diff_setups, sha256_text
from tests.optimizer.replay_fakes import KEY, env_for, message, question_doc, response, write_inputs


def _run(coro):
    return asyncio.run(coro)


# --- run file ---------------------------------------------------------------------------------------------------


def test_run_file_loads_with_env_only_in_host_fields(tmp_path: Path) -> None:
    path = write_inputs(tmp_path)
    config = load_config(path, environ=env_for(tmp_path))
    assert config.output_dir == tmp_path / "out"
    assert config.spend.ledger == tmp_path / "ledger.jsonl"
    assert config.serve.tools == ("inspect", "search")
    assert config.answerer.provider == "openai" and config.judge.passes == 2


@pytest.mark.parametrize(("extra", "code"), [
    ({"answerer.provider": "anthropic"}, "provider_unsupported"),
    ({"judge.provider": "openrouter"}, "provider_unsupported"),
    ({"answerer.model": "${REPLAY_TEST_OUT}"}, "env_not_allowed"),
    ({"output_dir": "${REPLAY_UNSET_VAR}"}, "env_unset"),
    ({"serve.guidance": "guidance.md"}, "serve_guidance_unsupported"),
    ({"spend.run_cap_usd": 20.0}, "caps_inverted"),
    ({"answerer.reasoning_effort": "max"}, "effort_invalid"),
    ({"budget.max_ratio": 0.5}, "budget_ratio_invalid"),
    ({"serve.pass_env": ["OPENAI_API_KEY"]}, "env_name_reserved"),
    ({"serve.pass_env": "OPENROUTER_API_KEY"}, "pass_env_invalid"),
    ({"output_dir": "deployments/synthetic/out"}, "path_inside_deployment"),
    ({"spend.ledger": "deployments/synthetic/ledger.jsonl"}, "path_inside_deployment"),
    ({"budget": {"expected_usd_per_try": 0.001, "check_after_tries": 4}}, "key_missing"),
    ({"answerer.max_model_turns": 1}, "int_invalid"),
    ({"judge.prompt": "builtin:other"}, "judge_prompt_unknown"),
    ({"surprise": 1}, "unknown_key"),
    ({"serve.env_files": {"OKG_DSN": {"file": "dsn", "prefix": ""}}}, "env_name_reserved"),
    ({"extra_endpoints": [{"name": "okg", "url": "http://x/mcp", "token_file": "dsn", "tools": "all",
                           "instructions_shown": False}]}, "endpoint_name_duplicate"),
])
def test_run_file_refusals_are_named(tmp_path: Path, extra: dict, code: str) -> None:
    path = write_inputs(tmp_path, extra=extra)
    with pytest.raises(ReplayError) as info:
        load_config(path, environ=env_for(tmp_path))
    assert code in info.value.codes


def test_run_file_missing_key_has_no_default(tmp_path: Path) -> None:
    path = write_inputs(tmp_path)
    text = path.read_text().replace("workers: 2\n", "")
    path.write_text(text)
    with pytest.raises(ReplayError) as info:
        load_config(path, environ=env_for(tmp_path))
    assert "key_missing" in info.value.codes


def test_run_file_repeated_key_is_refused(tmp_path: Path) -> None:
    path = write_inputs(tmp_path)
    path.write_text(path.read_text() + "tries: 3\n")
    with pytest.raises(ReplayError) as info:
        load_config(path, environ=env_for(tmp_path))
    assert info.value.codes == ("config_yaml_parse",)


# --- setup digest -----------------------------------------------------------------------------------------------


def _setup(**over):
    base = dict(okg={"commit": "a" * 40, "runner_tree": "sha256:" + "1" * 64},
                deployment={"name": "d", "tree": "sha256:" + "2" * 64, "branch": "default", "generation": "g1"},
                served={"surface": "s"}, model_input={"system_text": "t", "tool_list": "l"},
                question_set={"id": "q", "digest": "x", "questions": 3, "held_out": 1},
                answerer={"model": "m", "system_prompt": sha256_text("p")}, judge={"model": "m"},
                extra_endpoints={}, serve_env={}, tries=5, prices="p", runner={"setup_retries": 2})
    base.update(over)
    return build_setup(**base)


def test_setup_digest_is_stable_and_names_the_changed_field() -> None:
    a, b = _setup(), _setup()
    assert a["digest"] == b["digest"] and a["digest"].startswith("sha256:")
    c = _setup(answerer={"model": "m", "system_prompt": sha256_text("p!")})
    assert c["digest"] != a["digest"]
    assert diff_setups(a["fields"], c["fields"]) == ["answerer.system_prompt"]
    assert check_setup(a) == a["digest"]
    edited = json.loads(json.dumps(a))
    edited["fields"]["tries"] = 3
    with pytest.raises(ReplayError) as info:
        check_setup(edited)
    assert info.value.codes == ("setup_record_edited",)


def test_one_change_rule() -> None:
    check_one_change([], None)
    with pytest.raises(ReplayError) as info:
        check_one_change(["tries"], None)
    assert info.value.codes == ("setup_mismatch",)
    check_one_change(["answerer.system_prompt", "model_input.system_text"], "answerer.system_prompt")
    with pytest.raises(ReplayError) as info:
        check_one_change(["answerer.model", "tries"], "answerer.model")
    assert info.value.codes == ("more_than_one_change",)
    with pytest.raises(ReplayError) as info:
        check_one_change([], "answerer.model")
    assert info.value.codes == ("change_absent",)
    # Issue #4280: a served-text change moves the tool list and model input with it,
    # and a trial branch also moves what it serves; neither is a second change.
    text_change = ["model_input.tool_list", "served.surface", "served.surface_text", "served.tools"]
    check_one_change(text_change, "served.surface_text")
    check_one_change(["deployment.branch", "deployment.generation", *text_change], "deployment.branch")
    with pytest.raises(ReplayError) as info:
        check_one_change(["deployment.branch", "tries"], "deployment.branch")
    assert info.value.codes == ("more_than_one_change",)


# --- ledger -----------------------------------------------------------------------------------------------------


def _ledger(tmp_path: Path, run: str = "r1", run_cap: float = 0.015, total: float = 1.0,
            reserve: float = 0.01) -> Ledger:
    return Ledger(tmp_path / "ledger.jsonl", run_id=run, run_cap_usd=run_cap, total_cap_usd=total,
                  reserve_usd=reserve)


def test_ledger_reserve_counts_until_settled_and_cap_stops_second_call(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.check_start()
    rid = ledger.reserve(purpose="answer", question_id="q1", try_number=1)
    assert ledger.totals() == {"total_usd": 0.01, "run_usd": 0.01}
    ledger.settle(rid, usd=0.006, source="usage", model="gpt-6-luna", usage=None, purpose="answer",
                  question_id="q1", try_number=1)
    assert ledger.totals()["run_usd"] == 0.006
    with pytest.raises(CapReached) as info:
        ledger.reserve(purpose="answer", question_id="q1", try_number=1)
    assert info.value.codes == ("run_cap_reached",)


def test_ledger_total_cap_and_run_id_reuse(tmp_path: Path) -> None:
    first = _ledger(tmp_path, run="r1", run_cap=0.6, total=1.0)
    rid = first.reserve(purpose="answer", question_id="q", try_number=1)
    first.settle(rid, usd=0.5, source="usage", model="m", usage=None, purpose="answer", question_id="q",
                 try_number=1)
    with pytest.raises(ReplayError) as info:
        _ledger(tmp_path, run="r2", run_cap=0.6, total=1.0).check_start()
    assert info.value.codes == ("total_cap_insufficient",)
    with pytest.raises(ReplayError) as info:
        _ledger(tmp_path, run="r1", run_cap=0.1, total=1.0).check_start()
    assert info.value.codes == ("run_id_in_ledger",)
    # An open reserve from a process that died keeps counting.
    other = _ledger(tmp_path, run="r3", run_cap=0.5, total=1.0, reserve=0.45)
    other.reserve(purpose="answer", question_id="q", try_number=1)
    with pytest.raises(CapReached) as info:
        _ledger(tmp_path, run="r4", run_cap=0.5, total=1.0, reserve=0.1).reserve(
            purpose="answer", question_id="q", try_number=1)
    assert info.value.codes == ("total_cap_reached",)


def test_ledger_corrupt_row_stops(tmp_path: Path) -> None:
    (tmp_path / "ledger.jsonl").write_text('{"kind": "reserve"\n')
    with pytest.raises(ReplayError) as info:
        _ledger(tmp_path).totals()
    assert info.value.codes == ("ledger_corrupt",)


# --- OpenAI client ----------------------------------------------------------------------------------------------


def _client(tmp_path: Path, handler, **kw) -> tuple[OpenAIResponses, Ledger]:
    ledger = _ledger(tmp_path, run_cap=kw.pop("run_cap", 1.0), reserve=kw.pop("reserve", 0.01))

    async def no_sleep(_s: float) -> None:
        return None
    client = OpenAIResponses(ledger, model="gpt-6-luna", reasoning_effort="medium", key=KEY,
                             transport=httpx.MockTransport(handler), sleep=no_sleep)
    return client, ledger


def _call(client: OpenAIResponses):
    return client.respond(instructions="sys", input_items=[{"role": "user", "content": "hi"}],
                          max_output_tokens=100, purpose="answer", question_id="q1", try_number=1)


def test_cost_is_usage_times_pinned_prices(tmp_path: Path) -> None:
    client, ledger = _client(tmp_path, lambda r: httpx.Response(200, json=response(
        [message("hello")], input_tokens=219_000, cached=188_000, output_tokens=4_000)))
    reply = _run(_call(client))
    expected = ((219_000 - 188_000) * 0.10 + 188_000 * 0.01 + 4_000 * 0.50) / 1e6
    assert reply.text == "hello" and reply.usd == round(expected, 8)
    assert cost_of("gpt-6-luna", 219_000, 188_000, 4_000)[0] == round(expected, 8)
    settles = [r for r in ledger.run_rows()]
    assert len(settles) == 1 and settles[0]["usd"] == round(expected, 8) and settles[0]["source"] == "usage"


def test_cap_stops_before_the_second_call(tmp_path: Path) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(200, json=response([message("x")], input_tokens=10, cached=0, output_tokens=2_000))
    client, _ = _client(tmp_path, handler, run_cap=0.001, reserve=0.0009)
    _run(_call(client))
    with pytest.raises(CapReached):
        _run(_call(client))
    assert len(calls) == 1


def test_reply_without_usage_stops_and_charges_the_reserve(tmp_path: Path) -> None:
    client, ledger = _client(tmp_path, lambda r: httpx.Response(200, json={"status": "completed", "output": []}))
    with pytest.raises(SpendUnknown) as info:
        _run(_call(client))
    assert info.value.codes == ("usage_missing",)
    assert [(r["usd"], r["source"]) for r in ledger.run_rows()] == [(0.01, "reserve_unknown_usage")]


def test_auth_error_stops_without_charge_and_masks_the_key(tmp_path: Path) -> None:
    client, ledger = _client(tmp_path, lambda r: httpx.Response(401, text=f"bad key {KEY}"))
    with pytest.raises(ProviderStop) as info:
        _run(_call(client))
    assert KEY not in str(info.value) and "sk-<masked>" in str(info.value)
    assert KEY not in repr(client)
    assert [r["source"] for r in ledger.run_rows()] == ["http_error_status"]
    assert mask(f"x {KEY} y") == "x sk-<masked> y"


@pytest.mark.parametrize("status", [503, 520, 524])
def test_server_error_is_retried_then_succeeds(tmp_path: Path, status: int) -> None:
    # Any 5xx, including a CDN's 520-524 in front of the API, is retried rather than stopping the run.
    seq = [httpx.Response(status, text="busy"), httpx.Response(200, json=response([message("ok")]))]
    client, ledger = _client(tmp_path, lambda r: seq.pop(0))
    assert _run(_call(client)).text == "ok"
    assert [r["source"] for r in ledger.run_rows()] == ["http_error_status", "usage"]


def test_unpriced_tier_stops(tmp_path: Path) -> None:
    client, _ = _client(tmp_path, lambda r: httpx.Response(200, json=response([message("x")], tier="flex")))
    with pytest.raises(SpendUnknown) as info:
        _run(_call(client))
    assert info.value.codes == ("service_tier_unpriced",)


def test_key_only_from_environment() -> None:
    with pytest.raises(ReplayError) as info:
        key_from_env({})
    assert info.value.codes == ("openai_key_missing",)
    assert key_from_env({"OPENAI_API_KEY": KEY}) == KEY


# --- judge ------------------------------------------------------------------------------------------------------


def _question():
    return parse_question_set(question_doc()).question("q1")


def test_judge_prompt_and_schema_are_pinned() -> None:
    assert sha256_text(judge_mod.COMPARATOR_SYSTEM_PROMPT) == judge_mod.PROMPT_SHA256
    assert judge_mod.COMPARATOR_SYSTEM_PROMPT.startswith("You compare one complete answer with fixed gold atoms.")
    assert judge_mod.COMPARATOR_SYSTEM_PROMPT.endswith("outside the\nJSON object.\n")


def test_judge_cache_makes_the_second_judgment_free_and_identical(tmp_path: Path) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(body)
        assert body["text"]["format"]["name"] == "judgments" and body["instructions"].startswith("You compare")
        payload = json.loads(body["input"][0]["content"][0]["text"])
        js = [{"atom_id": a["id"], "outcome": "entailed", "rationale": "r"} for a in payload["gold_atoms"]]
        return httpx.Response(200, json=response([message(json.dumps({"judgments": js}))]))
    client, _ = _client(tmp_path, handler)
    cache = judge_mod.VerdictCache(tmp_path / "cache")
    settings = judge_mod.JudgeSettings("gpt-6-luna", "xhigh", 32000)
    q = _question()
    first = _run(judge_mod.judge_answer(client, cache, settings, q, "an answer", pass_number=1, try_number=1))
    second = _run(judge_mod.judge_answer(client, cache, settings, q, "an answer", pass_number=1, try_number=1))
    other_pass = _run(judge_mod.judge_answer(client, cache, settings, q, "an answer", pass_number=2,
                                             try_number=1))
    other_try = _run(judge_mod.judge_answer(client, cache, settings, q, "an answer", pass_number=1,
                                            try_number=2))
    assert first["cached"] is False and second["cached"] is True and second["model_calls"] == 0
    assert first["judgments"] == second["judgments"]
    assert other_pass["cached"] is False
    assert other_try["cached"] is False  # an identical answer on another try is its own judgment
    assert len(calls) == 3
    assert judge_mod.score(q, first["judgments"])["recall"] == 1.0


def test_judge_refuses_bad_output_three_times(tmp_path: Path) -> None:
    client, _ = _client(tmp_path, lambda r: httpx.Response(200, json=response(
        [message(json.dumps({"judgments": [{"atom_id": "zz", "outcome": "entailed", "rationale": "r"}]}))])))
    out = _run(judge_mod.judge_answer(client, judge_mod.VerdictCache(tmp_path / "c"),
                                      judge_mod.JudgeSettings("gpt-6-luna", "xhigh", 32000), _question(), "a",
                                      pass_number=1, try_number=1))
    assert out["status"] == "judge_failed" and out["model_calls"] == judge_mod.JUDGE_TRIES


def test_failed_answer_scores_zero() -> None:
    s = judge_mod.score(_question(), None)
    assert s == {"required_facts": 2, "required_entailed": 0, "recall": 0.0, "contradicted_facts": 0,
                 "contradicted": False, "correct": False}


# --- measures and compare ---------------------------------------------------------------------------------------


def _rows(ids, tries, passes, recall, *, contradicted=False, usd=0.007):
    rows = []
    for q in ids:
        for t in range(1, tries + 1):
            rows.append({"question_id": q, "try": t, "status": "answered", "failure": None, "forced_final": None,
                         "model_calls": 7, "input_tokens": 200_000, "cached_tokens": 170_000,
                         "output_tokens": 4_000, "tool_errors": 1, "wall_s": 30.0, "ledger_answer_usd": usd,
                         "passes": [{"pass": p, "status": "judged", "recall": recall(q, t, p),
                                     "correct": recall(q, t, p) == 1.0, "contradicted": contradicted}
                                    for p in range(1, passes + 1)]})
    return rows


def test_measurement_record_validates_and_gate_input_builds(tmp_path: Path) -> None:
    from datetime import UTC, datetime

    write_inputs(tmp_path)
    view = parse_question_set(question_doc()).loop_view()
    ids = [q.id for q in view.questions]
    rows = _rows(ids, 2, 2, lambda q, t, p: 0.5 if t == 1 else 1.0)
    record = measurement_record(rows, view, deployment="synthetic", generation_id="g", run_id="r1",
                                setup_digest="sha256:" + "0" * 64, tries=2, passes=2)
    assert validate_measurement(record) == []
    assert record["measures"]["required_fact_recall"] == 0.75
    assert set(record["per_scope_tag"]) == {"alpha", "beta"}
    objectives = load_objectives(tmp_path / "objectives.yaml")
    gate = replay_measurement(objectives, parse_measurement(record), runner_digest="sha256:x",
                              command="python -m okg.optimizer.replay run", artifact_refs=("measurement.json",),
                              measured_at=datetime.now(UTC))
    assert gate.decision.value == "fail"  # hard constraints are not measured by a replay
    assert gate.baseline_generation_id == gate.trial_generation_id == "g"
    with pytest.raises(ReplayError) as info:
        measurement_record(rows[:-1], view, deployment="synthetic", generation_id="g", run_id="r1",
                           setup_digest="sha256:" + "0" * 64, tries=2, passes=2)
    assert info.value.codes == ("run_incomplete",)
    assert completeness(rows[:-1], ids, 2, 2)["missing_tries"] == 1


def _run_data(rows, digest="sha256:" + "a" * 64, fields=None, run_id="r1", tries=3, passes=2):
    setup = build_setup(**{**dict(okg={}, deployment={}, served={}, model_input={}, question_set={},
                                  answerer={}, judge={}, extra_endpoints={}, serve_env={}, tries=tries, prices="p",
                                  runner={}), **(fields or {})})
    ids = sorted({r["question_id"] for r in rows})
    return RunData(setup, {"run_id": run_id, "question_ids": ids, "tries": tries, "passes": passes,
                           "generation_id": "g"}, rows)


def test_compare_same_runs_are_within_floor_and_deterministic() -> None:
    ids = [f"q{i}" for i in range(12)]
    def noisy(q: str, t: int, p: int) -> float:
        return ((int(q[1:]) * 7 + t * 3 + p) % 3) / 2  # 0, 0.5 or 1
    a = _run_data(_rows(ids, 3, 2, noisy))
    b = _run_data(_rows(ids, 3, 2, noisy), run_id="r2")
    report = compare_runs(a, b, replicates=500)
    again = compare_runs(a, b, replicates=500)
    assert report == again
    assert report["within_floor"] is True and report["setup"]["same"] is True
    assert report["measures"]["required_fact_recall"]["difference"] == 0.0
    assert report["three_tries"] is not None and report["judge_only"]["required_fact_recall"]["run_a"] is not None
    assert report["determinism"] == DETERMINISM_NOTE and "not deterministic" in DETERMINISM_NOTE


def test_compare_flags_a_real_shift_and_refuses_another_setup() -> None:
    ids = [f"q{i}" for i in range(12)]
    a = _run_data(_rows(ids, 3, 2, lambda q, t, p: 0.2))
    b = _run_data(_rows(ids, 3, 2, lambda q, t, p: 0.9), run_id="r2")
    report = compare_runs(a, b, replicates=500)
    assert report["within_floor"] is False
    assert report["measures"]["required_fact_recall"]["contains_zero"] is False
    c = _run_data(_rows(ids, 3, 2, lambda q, t, p: 0.2), fields={"tries": 3, "prices": "other"}, run_id="r3")
    with pytest.raises(ReplayError) as info:
        compare_runs(a, c, replicates=100)
    assert info.value.codes == ("setup_mismatch",)
    assert compare_runs(a, c, change="prices", replicates=100)["setup"]["differing_fields"] == ["prices"]


# --- evidence ---------------------------------------------------------------------------------------------------


def test_evidence_refuses_private_text_long_strings_and_unknown_keys() -> None:
    private = ["Synthetic question q1: which widget colour is configured for the lab?"]
    setup = _setup()
    check_evidence(setup_evidence(setup), private)
    with pytest.raises(ReplayError) as info:
        check_evidence({"schema": "x", "run_id": "which-widget-colour-is-configured-for"},
                       ["xx which-widget-colour-is-configured-for yy"])
    assert info.value.codes == ("evidence_private_text",)
    with pytest.raises(ReplayError) as info:
        check_evidence({"schema": "an answer with spaces"}, [])
    assert info.value.codes == ("evidence_text_not_allowed",)
    with pytest.raises(ReplayError) as info:
        check_evidence({"answer": "x"}, [])
    assert info.value.codes == ("evidence_key_not_allowed",)


def test_setup_evidence_hashes_an_env_files_prefix_with_a_space() -> None:
    setup = _setup(serve_env={"env_files": [["OKG_LIVE_AUTH", "Bearer "], ["PGPASSWORD", ""]]})
    evidence = setup_evidence(setup)
    check_evidence(evidence, [])
    rows = dict((name, value) for name, value in evidence["fields"])
    assert rows["serve_env.env_files"] == [["OKG_LIVE_AUTH", "sha256:" + hashlib.sha256(b"Bearer ").hexdigest()],
                                           ["PGPASSWORD", ""]]
    assert evidence["digest"] == setup["digest"]
    assert "Bearer " not in json.dumps(evidence)


def test_report_evidence_passes_the_writer() -> None:
    ids = [f"q{i}" for i in range(6)]
    a = _run_data(_rows(ids, 3, 2, lambda q, t, p: 1.0))
    b = _run_data(_rows(ids, 3, 2, lambda q, t, p: 1.0), run_id="r2")
    check_evidence(report_evidence(compare_runs(a, b, replicates=100)), ["some private answer text here"])


# --- review fixes (B1, S3, S5, S6, nits) ------------------------------------------------------------------------


def _judge_once(tmp_path: Path, handler):
    client, _ = _client(tmp_path, handler)
    return _run(judge_mod.judge_answer(client, judge_mod.VerdictCache(tmp_path / "c"),
                                       judge_mod.JudgeSettings("gpt-6-luna", "xhigh", 4000), _question(), "a",
                                       pass_number=1, try_number=1))


def test_judge_call_that_fails_after_retries_is_a_judge_failed_unit(tmp_path: Path) -> None:
    out = _judge_once(tmp_path, lambda r: httpx.Response(503, text="busy"))
    assert out["status"] == "judge_failed" and out["model_calls"] == 1 and "model call failed" in out["error"]
    context = _judge_once(tmp_path / "ctx", lambda r: httpx.Response(400, text="context_length_exceeded"))
    assert context["status"] == "judge_failed"


def test_unjudgeable_fails_the_unit_instead_of_scoring_not_entailed(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(json.loads(request.content)["input"][0]["content"][0]["text"])
        js = [{"atom_id": a["id"], "outcome": "unjudgeable", "rationale": "r"} for a in payload["gold_atoms"]]
        return httpx.Response(200, json=response([message(json.dumps({"judgments": js}))]))
    out = _judge_once(tmp_path, handler)
    assert out["status"] == "judge_failed" and out["model_calls"] == judge_mod.JUDGE_TRIES
    assert not list((tmp_path / "c").rglob("*.json"))  # nothing cached


def test_reply_model_must_be_the_priced_model_or_its_snapshot(tmp_path: Path) -> None:
    client, _ = _client(tmp_path, lambda r: httpx.Response(200, json=response([message("ok")],
                                                                               model="gpt-6-luna-2026-10-01")))
    assert _run(_call(client)).text == "ok"
    for i, served in enumerate(("gpt-7", "gpt-6-luna-mini", None)):
        client, _ = _client(tmp_path / str(i), lambda r, s=served: httpx.Response(200, json=response(
            [message("ok")], model=s)))
        with pytest.raises(ProviderStop) as info:
            _run(_call(client))
        assert info.value.codes == ("reply_model_mismatch",)


def test_runner_or_provider_failure_makes_the_run_incomplete() -> None:
    ids = ["q1", "q2"]
    rows = _rows(ids, 2, 2, lambda q, t, p: 1.0)
    assert completeness(rows, ids, 2, 2)["complete"] is True
    for label, complete in (("api_error", False), ("other", False), ("output_limit", True)):
        changed = json.loads(json.dumps(rows))
        changed[0].update(status="failed", failure=label)
        state = completeness(changed, ids, 2, 2)
        assert state["complete"] is complete and state["run_failed_tries"] == (0 if complete else 1)


def test_dirty_pyproject_or_lock_is_refused(tmp_path: Path) -> None:
    import subprocess

    from okg.optimizer.replay.setup import okg_identity

    git = ["git", "-C", str(tmp_path), "-c", "user.email=t@example.invalid", "-c", "user.name=t"]
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "a.py").write_text("x = 1\n")
    (tmp_path / "pyproject.toml").write_text("[project]\n")
    (tmp_path / "uv.lock").write_text("version = 1\n")
    subprocess.run([*git, "add", "."], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "init"], check=True)
    assert okg_identity(tmp_path)["commit"]
    for name in ("pyproject.toml", "uv.lock"):
        (tmp_path / name).write_text((tmp_path / name).read_text() + "# edit\n")
        with pytest.raises(ReplayError) as info:
            okg_identity(tmp_path)
        assert info.value.codes == ("okg_tree_dirty",)
        subprocess.run([*git, "checkout", "-q", "--", name], check=True)


def test_server_gets_an_allowlisted_environment(tmp_path: Path) -> None:
    from okg.optimizer.replay.serve import server_env

    path = write_inputs(tmp_path, extra={"serve.env_files": {"OKG_EXTRA": {"file": "prompt.md", "prefix": "p:"}}})
    config = load_config(path, environ=env_for(tmp_path))
    env = server_env(config, "tok", base={"PATH": "/bin", "LC_ALL": "C", "OKG_FLAG": "1", "OPENAI_API_KEY": KEY,
                                          "OKG_DSN": "postgresql://elsewhere/x"})
    assert env["PATH"] == "/bin" and env["LC_ALL"] == "C"
    assert "OKG_FLAG" not in env and "OPENAI_API_KEY" not in env
    assert env["OKG_DSN"] == "postgresql://synthetic@127.0.0.1:1/none" and env["OKG_EXTRA"].startswith("p:")


def test_compare_evidence_needs_the_run_file(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from okg.optimizer.replay.__main__ import main

    code = main(["compare", str(tmp_path / "a"), str(tmp_path / "b"), "--out", str(tmp_path / "r.json"),
                 "--evidence-dir", str(tmp_path / "ev")])
    assert code == 2 and "compare_evidence_needs_config" in capsys.readouterr().err


# --- re-review fixes (H1, M1) -------------------------------------------------------------------------------------


def test_worst_call_input_is_capped_at_the_context_window(monkeypatch: pytest.MonkeyPatch) -> None:
    from okg.optimizer.replay import openai_client

    worst, priced = openai_client.worst_call_usd("gpt-6-luna", 10**9, 64_000)
    assert priced == openai_client.CONTEXT_WINDOWS["gpt-6-luna"] == 1_050_000
    assert worst == cost_of("gpt-6-luna", 1_050_000, 0, 64_000)[0] == pytest.approx(0.258)
    assert openai_client.worst_call_usd("gpt-6-luna", 1_000, 100) == (cost_of("gpt-6-luna", 1_000, 0, 100)[0], 1_000)
    monkeypatch.setitem(openai_client.PRICES, "gpt-other", (1.0, 0.1, 2.0))
    with pytest.raises(ReplayError) as info:
        openai_client.check_priced("gpt-other", "/answerer/model")
    assert info.value.codes == ("model_context_unknown",)


def test_pass_env_reaches_the_server_and_an_unset_one_is_refused(tmp_path: Path) -> None:
    from okg.optimizer.replay.serve import server_env

    path = write_inputs(tmp_path, extra={"serve.pass_env": ["OPENROUTER_API_KEY"]})
    config = load_config(path, environ=env_for(tmp_path))
    assert config.serve.pass_env == ("OPENROUTER_API_KEY",)
    env = server_env(config, "tok", base={"PATH": "/bin", "OPENROUTER_API_KEY": "or-test-key"})
    assert env["OPENROUTER_API_KEY"] == "or-test-key"
    with pytest.raises(ReplayError) as info:
        server_env(config, "tok", base={"PATH": "/bin"})
    assert info.value.codes == ("pass_env_unset",)


def test_deployment_needing_an_unpassed_key_is_refused(tmp_path: Path) -> None:
    from okg.optimizer.replay.serve import check_server_env, required_server_env

    path = write_inputs(tmp_path)
    manifest = tmp_path / "deployments" / "synthetic" / "deployment.yaml"
    manifest.write_text("name: synthetic\nsync_root: ${repo_root}/x\n"
                        "postgres:\n  dsn: postgresql://u:${SYNTH_EXTRA}@127.0.0.1:1/db\n"
                        "sources:\n  git:\n    token: ${SYNTH_SOURCE_TOKEN}\n"  # ingestion only: not required
                        "embeddings:\n  enabled: true\n  backend: openrouter:google/gemini-embedding-2@768\n")
    config = load_config(path, environ=env_for(tmp_path))
    assert set(required_server_env(config.deployment_dir)) == {"OPENROUTER_API_KEY", "SYNTH_EXTRA"}
    with pytest.raises(ReplayError) as info:
        check_server_env(config)
    assert info.value.codes == ("server_env_missing",)
    assert "OPENROUTER_API_KEY" in str(info.value) and "SYNTH_EXTRA" in str(info.value)
    assert "SYNTH_SOURCE_TOKEN" not in str(info.value)
    passed = load_config(write_inputs(tmp_path / "p", extra={"serve.pass_env": ["OPENROUTER_API_KEY", "SYNTH_EXTRA"]}),
                         environ=env_for(tmp_path / "p"))
    (tmp_path / "p" / "deployments" / "synthetic" / "deployment.yaml").write_text(manifest.read_text())
    check_server_env(passed)


def test_sigterm_during_judge_exits_130_and_restores_the_handler(monkeypatch: pytest.MonkeyPatch,
                                                                 capsys: pytest.CaptureFixture[str]) -> None:
    import signal
    import time

    from okg.optimizer.replay import __main__ as main_mod

    def judged(_args):
        signal.raise_signal(signal.SIGTERM)  # handled as Ctrl-C: KeyboardInterrupt before the sleep ends
        time.sleep(5)
        return 0
    monkeypatch.setattr(main_mod, "_judge", judged)
    old_int = signal.signal(signal.SIGINT, signal.default_int_handler)  # a worker may ignore SIGINT
    before = signal.getsignal(signal.SIGTERM)
    try:
        assert main_mod.main(["judge", "--config", "run.yaml", "--run-id", "r1"]) == 130
    finally:
        signal.signal(signal.SIGINT, old_int)
    assert signal.getsignal(signal.SIGTERM) == before
    assert "interrupted" in capsys.readouterr().err
