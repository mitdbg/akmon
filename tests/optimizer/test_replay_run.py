"""Replay runner end to end with fakes (PACT akb-replay-runner, #4279): the tool
loop and its limits, pinning the served setup, a full run with judging and a
measurement record, the cap stop, and a surface change mid-run.

The graph server is an in-process fake MCP session; OpenAI is
httpx.MockTransport. No network, no database, no paid call.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest

from okg.optimizer.objectives import validate_measurement
from okg.optimizer.question_sets import parse_question_set
from okg.optimizer.replay import agent as agent_mod
from okg.optimizer.replay.config import load_config
from okg.optimizer.replay.errors import CapReached, ReplayError
from okg.optimizer.replay.ledger import Ledger
from okg.optimizer.replay.openai_client import OpenAIResponses
from okg.optimizer.replay.runner import judge_existing, prepare, run_replay
from okg.optimizer.replay.serve import Surface
from okg.optimizer.replay.setup import diff_setups
from tests.optimizer.replay_fakes import (
    GEN,
    KEY,
    FakeOpenAI,
    FakeServer,
    env_for,
    fake_connector,
    fake_server_factory,
    function_call,
    message,
    question_doc,
    response,
    write_inputs,
)

OKG_ID = {"commit": "c" * 40, "runner_tree": "sha256:" + "d" * 64}
URL = "http://fake/mcp"


def _config(tmp_path: Path, **kw):
    return load_config(write_inputs(tmp_path, **kw), environ=env_for(tmp_path))


def _prepare(config, server: FakeServer):
    return asyncio.run(prepare(config, fake_connector({URL: server}), URL, {"Authorization": "Bearer t"},
                               okg=OKG_ID))


# --- the tool loop ----------------------------------------------------------------------------------------------


def _try(tmp_path: Path, handler, server: FakeServer | None = None, *, turns: int = 4, cap: int = 20,
         reply_cap: int = 4096, tool_timeout: float = 5.0, answer_timeout: float = 30.0, run_cap: float = 1.0):
    server = server or FakeServer()
    ledger = Ledger(tmp_path / "l.jsonl", run_id="r", run_cap_usd=run_cap, total_cap_usd=5.0, reserve_usd=0.01)
    client = OpenAIResponses(ledger, model="gpt-6-luna", reasoning_effort="medium", key=KEY,
                             transport=httpx.MockTransport(handler))
    endpoint = agent_mod.Endpoint("okg", URL, {"Authorization": "Bearer t"}, ("inspect", "search"), True, True)
    surface = Surface(server.instructions, tuple(server.tool_defs()))
    model_input = agent_mod.build_model_input("Base prompt.", [endpoint], {"okg": surface})
    settings = agent_mod.AnswerSettings(1000, turns, cap, reply_cap, tool_timeout, answer_timeout)
    row = asyncio.run(agent_mod.answer_try(
        client, fake_connector({URL: server}), [endpoint], settings, system_prompt="Base prompt.",
        question_id="q1", question="Which colour?", try_number=2, pinned=model_input.digests))
    return row, server, model_input


def test_try_answers_after_a_tool_call_and_labels_every_request(tmp_path: Path) -> None:
    fake = FakeOpenAI()
    row, server, model_input = _try(tmp_path, fake.handler)
    assert row["status"] == "answered" and row["answer"] == fake.answer_text
    assert row["model_calls"] == 2 and row["tool_calls"] == 1 and row["forced_final"] is None
    assert row["input_tokens"] == 2000 and row["answer_usd"] > 0
    for seen in server.seen:
        assert seen["headers"]["X-OKG-Question"] == "q1" and seen["headers"]["X-OKG-Try"] == "2"
    first = fake.requests[0]
    assert first["instructions"] == model_input.system_text
    assert first["instructions"].endswith("Synthetic graph instructions.")
    assert [t["name"] for t in first["tools"]] == ["inspect", "search"]
    assert first["reasoning"] == {"effort": "medium"} and first["store"] is False
    second = fake.requests[1]
    assert [i["type"] for i in second["input"][1:]] == ["function_call", "function_call_output"]
    # Issue #4281: the private record keeps each call's arguments and reply for diagnosis.
    call = row["calls"][0]
    sent = [s for s in server.seen if s["op"] == "call_tool"][0]
    assert call["args"] == sent["args"]
    assert call["reply"].startswith(f"{sent['name']} ok")


def test_model_turn_limit_forces_a_final_answer(tmp_path: Path) -> None:
    fake = FakeOpenAI(always_call_tools=True)
    row, _, _ = _try(tmp_path, fake.handler, turns=3)
    assert row["status"] == "answered" and row["forced_final"] == "model_turn_limit"
    assert row["model_calls"] == 3 and row["tool_calls"] == 2
    assert fake.requests[-1]["tool_choice"] == "none"


def test_safety_cap_does_not_run_the_batch(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if body.get("tool_choice") == "none":
            return httpx.Response(200, json=response([message("final")]))
        return httpx.Response(200, json=response([function_call("search", {"query": str(i)}, f"c{i}")
                                                  for i in range(3)]))
    row, server, _ = _try(tmp_path, handler, cap=2)
    assert row["forced_final"] == "tool_call_safety_cap" and row["tool_calls"] == 0
    assert not [s for s in server.seen if s["op"] == "call_tool"]


def test_long_reply_is_cut_and_unknown_tool_is_an_error(tmp_path: Path) -> None:
    server = FakeServer(reply=lambda name, args: ("x" * 10_000, False))

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        outs = [i for i in body["input"] if i.get("type") == "function_call_output"]
        if outs:
            assert "[tool reply cut" in outs[0]["output"] and "unknown tool" in outs[1]["output"]
            return httpx.Response(200, json=response([message("done")]))
        return httpx.Response(200, json=response([function_call("search", {"query": "a"}, "c1"),
                                                  function_call("delete_everything", {}, "c2")]))
    row, _, _ = _try(tmp_path, handler, server, reply_cap=1000)
    assert row["status"] == "answered" and row["replies_cut"] == 1 and row["tool_errors"] == 1


def test_tool_timeout_is_recorded_and_the_try_goes_on(tmp_path: Path) -> None:
    row, _, _ = _try(tmp_path, FakeOpenAI().handler, FakeServer(delay_s=0.5), tool_timeout=0.05)
    assert row["status"] == "answered" and row["tool_timeouts"] == 1


def test_output_limit_and_answer_timeout_fail_the_try(tmp_path: Path) -> None:
    row, _, _ = _try(tmp_path, lambda r: httpx.Response(200, json=response(
        [message("partial")], status="incomplete", incomplete="max_output_tokens")))
    assert row["status"] == "failed" and row["failure"] == "output_limit" and row["answer"] == ""
    row, _, _ = _try(tmp_path / "t", FakeOpenAI().handler, FakeServer(delay_s=1.0), tool_timeout=5.0,
                     answer_timeout=0.2)
    assert row["failure"] == "answer_timeout"


def test_cap_inside_a_try_stops_the_run(tmp_path: Path) -> None:
    with pytest.raises(CapReached):
        _try(tmp_path, FakeOpenAI().handler, run_cap=0.005)


def test_changed_surface_inside_a_try_stops_the_run(tmp_path: Path) -> None:
    fake = FakeOpenAI()
    ledger = Ledger(tmp_path / "l.jsonl", run_id="r", run_cap_usd=1.0, total_cap_usd=5.0, reserve_usd=0.01)
    client = OpenAIResponses(ledger, model="gpt-6-luna", reasoning_effort="medium", key=KEY,
                             transport=httpx.MockTransport(fake.handler))
    endpoint = agent_mod.Endpoint("okg", URL, {}, ("inspect", "search"), True, True)
    settings = agent_mod.AnswerSettings(1000, 4, 20, 4096, 5.0, 30.0)
    with pytest.raises(ReplayError) as info:
        asyncio.run(agent_mod.answer_try(
            client, fake_connector({URL: FakeServer(instructions="Edited.")}), [endpoint], settings,
            system_prompt="Base prompt.", question_id="q1", question="Q?", try_number=1,
            pinned={"system_text": "sha256:" + "0" * 64, "tool_list": "sha256:" + "0" * 64}))
    assert info.value.codes == ("model_input_changed",)
    assert fake.requests == []


# --- pinning the setup ------------------------------------------------------------------------------------------


def test_prepare_pins_generation_tools_and_surface(tmp_path: Path) -> None:
    config = _config(tmp_path)
    prepared = _prepare(config, FakeServer())
    fields = prepared.setup["fields"]
    assert fields["deployment"]["generation"] == GEN and fields["served"]["tool_names"] == ["inspect", "search"]
    assert fields["question_set"] == {"id": "synthetic-3", "digest": prepared.view.digest, "questions": 3,
                                      "held_out": 1}
    assert fields["served"]["surface_text"] == "not_published"
    for server, code in ((FakeServer(generation="okg:other"), "generation_mismatch"),
                         (FakeServer(tools=("inspect", "search", "expand")), "served_tools_mismatch")):
        with pytest.raises(ReplayError) as info:
            _prepare(config, server)
        assert info.value.codes == (code,)
    pinned = _config(tmp_path / "p", extra={"serve.expect_surface_digest": "sha256:" + "f" * 64})
    with pytest.raises(ReplayError) as info:
        _prepare(pinned, FakeServer())
    assert info.value.codes == ("surface_mismatch",)


def test_same_inputs_same_digest_host_values_excluded_prompt_edit_named(tmp_path: Path) -> None:
    one = _prepare(_config(tmp_path / "a"), FakeServer()).setup
    two = _prepare(_config(tmp_path / "b"), FakeServer()).setup  # other output dir, ledger and paths
    assert one["digest"] == two["digest"]
    three = _prepare(_config(tmp_path / "c"), FakeServer()).setup
    (tmp_path / "c" / "prompt.md").write_text((tmp_path / "c" / "prompt.md").read_text() + "!")
    four = _prepare(load_config(tmp_path / "c" / "run.yaml", environ=env_for(tmp_path / "c")), FakeServer()).setup
    assert three["digest"] == one["digest"]
    assert diff_setups(three["fields"], four["fields"]) == ["answerer.system_prompt", "model_input.system_text"]


def test_served_text_digest_is_recorded_and_is_the_only_difference(tmp_path: Path) -> None:
    """Issue #4280: a server publishing a different surface_text digest (a branch
    overriding one fix_hint) gives a setup that differs in that field only."""
    base = {"schema": "okg.mcp.surface_text.v1", "digest": "sha256:" + "a" * 64}
    branch = {**base, "digest": "sha256:" + "b" * 64, "branch_layer": "trial"}
    one = _prepare(_config(tmp_path / "a"), FakeServer(surface_text=base)).setup
    two = _prepare(_config(tmp_path / "b"), FakeServer(surface_text=branch)).setup
    assert one["fields"]["served"]["surface_text"] == base["digest"]
    assert two["fields"]["served"]["surface_text"] == branch["digest"]
    assert one["digest"] != two["digest"]
    assert diff_setups(one["fields"], two["fields"]) == ["served.surface_text"]
    with pytest.raises(ReplayError) as info:
        _prepare(_config(tmp_path / "c"), FakeServer(surface_text={"digest": "md5:x"}))
    assert info.value.codes == ("surface_text_unreadable",)


def test_held_out_questions_are_never_loaded_and_ids_must_be_header_safe(tmp_path: Path) -> None:
    prepared = _prepare(_config(tmp_path), FakeServer())
    assert [q.id for q in prepared.view.questions] == ["q1", "q2", "q3"]
    assert not agent_mod.header_safe_id("q 1") and not agent_mod.header_safe_id("q#1")
    assert agent_mod.header_safe_id("set-a-17")


# --- a whole run ------------------------------------------------------------------------------------------------


async def _no_sleep(_s: float) -> None:
    return None


def _full_run(tmp_path: Path, fake: FakeOpenAI, server: FakeServer | None = None, run_id: str = "run-1", **kw):
    config = _config(tmp_path, **kw)
    outcome = asyncio.run(run_replay(config, run_id=run_id, connector=fake_connector({URL: server or FakeServer()}),
                                     server=fake_server_factory(URL), key=KEY, command="test",
                                     transport=fake.transport(), okg=OKG_ID, sleep=_no_sleep))
    return config, outcome


def test_full_run_measures_and_rejudging_is_free(tmp_path: Path) -> None:
    fake = FakeOpenAI()
    config, outcome = _full_run(tmp_path, fake)
    assert outcome.stop is None and outcome.summary["completeness"]["complete"] is True
    assert fake.answer_requests == 3 * 2 * 2  # 3 questions x 2 tries x (tool call + answer)
    assert fake.judge_requests == 3 * 2 * 2  # 3 questions x 2 tries x 2 passes
    assert validate_measurement(outcome.record) == []
    assert outcome.record["measures"]["required_fact_recall"] == 1.0
    assert outcome.record["tries"] == 2 and outcome.record["setup_digest"] == outcome.summary["setup_digest"]
    run_dir = outcome.run_dir
    for name in ("setup.json", "run.json", "tries.jsonl", "measurement.json", "score.json", "acceptance.json"):
        assert (run_dir / name).is_file(), name
    # The worker count is recorded beside the setup, outside its digest.
    stored_setup = json.loads((run_dir / "setup.json").read_text())
    assert stored_setup["workers"] == 2 and "workers" not in stored_setup["fields"]
    assert outcome.summary["workers"] == 2
    tries_text = (run_dir / "tries.jsonl").read_text()
    assert fake.answer_text not in tries_text and "Synthetic question" not in tries_text
    assert json.loads((run_dir / "acceptance.json").read_text())["decision"] == "fail"
    spend = outcome.summary["spend"]
    assert spend["run_usd"] == pytest.approx(spend["answer_usd"] + spend["judge_usd"])
    before = fake.judge_requests
    assert outcome.summary["status"] == "complete" and outcome.summary["budget"]["checked"] is True
    again = judge_existing(config, run_dir, key=KEY, command="test", transport=fake.transport(), okg=OKG_ID)
    assert fake.judge_requests == before  # every verdict came from the cache
    assert again.record["measures"] == outcome.record["measures"]


def test_run_stops_at_the_cap_with_a_named_code(tmp_path: Path) -> None:
    fake = FakeOpenAI()
    _, outcome = _full_run(tmp_path, fake, run_cap=0.0105, reserve=0.01, workers=1)
    assert outcome.stop is not None and outcome.stop.codes == ("run_cap_reached",)
    assert outcome.record is None and outcome.summary["stop_phase"] == "answer"
    ledger = [json.loads(line) for line in (tmp_path / "ledger.jsonl").read_text().splitlines()]
    open_reserves = {r["id"] for r in ledger if r["kind"] == "reserve"} - {r["id"] for r in ledger
                                                                           if r["kind"] == "settle"}
    assert open_reserves == set()


def test_run_refuses_when_the_total_cap_cannot_cover_it(tmp_path: Path) -> None:
    earlier = Ledger(tmp_path / "ledger.jsonl", run_id="earlier", run_cap_usd=0.5, total_cap_usd=0.6,
                     reserve_usd=0.01)
    rid = earlier.reserve(purpose="answer", question_id="q1", try_number=1)
    earlier.settle(rid, usd=0.2, source="usage", model="gpt-6-luna", usage=None, purpose="answer",
                   question_id="q1", try_number=1)
    fake = FakeOpenAI()
    with pytest.raises(ReplayError) as info:
        _full_run(tmp_path, fake, run_cap=0.5, total_cap=0.6)
    assert info.value.codes == ("total_cap_insufficient",)
    assert fake.requests == [] and not (tmp_path / "out" / "run-1").exists()


def test_generation_change_after_the_last_try_stops_the_run(tmp_path: Path) -> None:
    server = FakeServer()
    calls = {"n": 0}
    original = server.tool_defs

    def flipping():  # the surface is read 1 (prepare) + 3x2 (tries) times, then once after the last try
        calls["n"] += 1
        if calls["n"] > 1 + 3 * 2:
            server.generation = "okg:synthetic:later"
        return original()
    server.tool_defs = flipping  # type: ignore[method-assign]
    _, outcome = _full_run(tmp_path, FakeOpenAI(), server)
    assert outcome.stop is not None and outcome.stop.codes == ("generation_changed",)
    assert outcome.record is None


def test_question_doc_is_synthetic() -> None:
    qs = parse_question_set(question_doc())
    assert qs.id == "synthetic-3" and len(qs.questions) == 4


# --- review fixes: failures are recorded, never hidden ------------------------------------------------------------


def test_judge_model_error_leaves_the_run_incomplete_and_judge_resumes(tmp_path: Path) -> None:
    fake = FakeOpenAI(judge_status=503)
    config, outcome = _full_run(tmp_path, fake)
    assert outcome.stop is None and outcome.record is None
    summary = json.loads((outcome.run_dir / "run.json").read_text())
    assert summary["status"] == "incomplete" and summary["completeness"]["judge_failed_units"] == 3 * 2 * 2
    assert (outcome.run_dir / "tries.jsonl").is_file() and not (outcome.run_dir / "measurement.json").exists()
    fake.judge_status = 200
    again = judge_existing(config, outcome.run_dir, key=KEY, command="test", transport=fake.transport(), okg=OKG_ID)
    assert again.stop is None and again.summary["status"] == "complete" and again.record is not None


def test_failed_post_run_probe_still_writes_run_json(tmp_path: Path) -> None:
    server = FakeServer()
    calls = {"n": 0}
    original = server.tool_defs

    def breaking():  # 1 (prepare) + 3x2 (tries) reads, then the post-run probe fails
        calls["n"] += 1
        if calls["n"] > 1 + 3 * 2:
            raise RuntimeError("probe broke")
        return original()
    server.tool_defs = breaking  # type: ignore[method-assign]
    _, outcome = _full_run(tmp_path, FakeOpenAI(), server)
    assert outcome.stop is not None and outcome.stop.codes == ("run_crashed",)
    summary = json.loads((outcome.run_dir / "run.json").read_text())
    assert summary["status"] == "stopped" and summary["stop_phase"] == "probe" and outcome.record is None


def test_api_error_tries_make_the_run_incomplete_and_are_counted_by_label(tmp_path: Path) -> None:
    _, outcome = _full_run(tmp_path, FakeOpenAI(answer_status=503))
    assert outcome.stop is None and outcome.record is None
    s = outcome.summary
    assert s["status"] == "incomplete" and s["failures_by_label"] == {"api_error": 6}
    assert s["completeness"]["run_failed_tries"] == 6 and s["completeness"]["complete"] is False
    from okg.optimizer.replay.evidence import check_evidence, run_evidence

    check_evidence(run_evidence(s, None, None), [])  # status, labels and budget pass the evidence writer


def test_rejudge_refuses_an_edited_run_file_or_another_okg(tmp_path: Path) -> None:
    fake = FakeOpenAI()
    config, outcome = _full_run(tmp_path, fake)
    with pytest.raises(ReplayError) as info:
        judge_existing(config, outcome.run_dir, key=KEY, command="test", transport=fake.transport(),
                       okg={"commit": "e" * 40, "runner_tree": OKG_ID["runner_tree"]})
    assert info.value.codes == ("okg_changed",)
    path = tmp_path / "run.yaml"
    path.write_text(path.read_text().replace("\ntries: 2\n", "\ntries: 3\n"))
    edited = load_config(path, environ=env_for(tmp_path))
    assert edited.tries == 3
    with pytest.raises(ReplayError) as info:
        judge_existing(edited, outcome.run_dir, key=KEY, command="test", transport=fake.transport(), okg=OKG_ID)
    assert info.value.codes == ("run_file_changed",)
    assert fake.judge_requests == 3 * 2 * 2  # refused before any judge call


def test_budget_stop_after_the_first_tries(tmp_path: Path) -> None:
    fake = FakeOpenAI()
    _, outcome = _full_run(tmp_path, fake, workers=1, extra={"budget.expected_usd_per_try": 0.0001,
                                                             "budget.check_after_tries": 2})
    assert outcome.stop is not None and outcome.stop.codes == ("budget_ratio_exceeded",)
    budget = outcome.summary["budget"]
    assert budget["checked"] is True and budget["ratio"] > 1.5 and budget["finished_tries"] == 2
    assert fake.answer_requests == 2 * 2 and outcome.record is None


@pytest.mark.parametrize("extra", [{"spend.reserve_usd": 0.002},  # below a judge call's worst case
                                   {"answerer.tool_reply_cap_bytes": 200_000}])  # below an answer call's
def test_reserve_below_one_calls_worst_case_is_refused(tmp_path: Path, extra: dict) -> None:
    with pytest.raises(ReplayError) as info:
        _prepare(_config(tmp_path, extra=extra), FakeServer())
    assert info.value.codes == ("reserve_below_worst_call",)


def test_server_env_names_are_in_the_setup(tmp_path: Path) -> None:
    one = _prepare(_config(tmp_path / "a"), FakeServer()).setup
    two = _prepare(_config(tmp_path / "b", extra={"serve.env_files": {"OKG_EXTRA": {"file": "prompt.md",
                                                                                    "prefix": ""}}}),
                   FakeServer()).setup
    assert diff_setups(one["fields"], two["fields"]) == ["serve_env.env_files"]


# --- re-review fixes (H1, M2, Low) --------------------------------------------------------------------------------

#: The plan's answerer and judge (PLAN-4279.md): Luna medium 64k output, 30 turns, safety cap 200, 512 KiB
#: replies; Luna xhigh judge 32k output; $3 run cap, $10 total, 6 workers.
PLANNED = {"answerer.max_output_tokens": 64_000, "answerer.max_model_turns": 30,
           "answerer.tool_call_safety_cap": 200, "answerer.tool_reply_cap_bytes": 524_288,
           "judge.max_output_tokens": 32_000}


def test_planned_config_fits_a_reserve_under_the_run_cap(tmp_path: Path) -> None:
    config = _config(tmp_path / "ok", run_cap=3.0, total_cap=10.0, workers=6, extra={**PLANNED,
                                                                                    "spend.reserve_usd": 0.26})
    _prepare(config, FakeServer())  # the answer bound is capped at the 1.05M-token window: worst call $0.258
    assert config.workers * config.spend.reserve_usd < config.spend.run_cap_usd
    low = _config(tmp_path / "low", run_cap=3.0, total_cap=10.0, workers=6, extra={**PLANNED,
                                                                                  "spend.reserve_usd": 0.25})
    with pytest.raises(ReplayError) as info:
        _prepare(low, FakeServer())
    assert info.value.codes == ("reserve_below_worst_call",) and "capped at the context window" in str(info.value)


def test_budget_that_could_never_be_checked_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ReplayError) as info:
        _prepare(_config(tmp_path, extra={"budget.check_after_tries": 3 * 2 + 1}), FakeServer())
    assert info.value.codes == ("budget_never_checked",)


def test_cancelled_run_writes_run_json_then_raises(tmp_path: Path) -> None:
    started = asyncio.Event()

    server = FakeServer()

    def reply(name, args):  # the first search lets the test cancel; every later tool call hangs
        started.set()
        server.delay_s = 30.0
        return "search ok · 1 row", False
    server.reply = reply
    config = _config(tmp_path)

    async def main():
        task = asyncio.create_task(run_replay(
            config, run_id="run-c", connector=fake_connector({URL: server}), server=fake_server_factory(URL),
            key=KEY, command="test", transport=FakeOpenAI().transport(), okg=OKG_ID, sleep=_no_sleep))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(main())
    summary = json.loads((tmp_path / "out" / "run-c" / "run.json").read_text())
    assert summary["status"] == "stopped" and summary["stop_phase"] == "answer"
    assert [p["code"] for p in summary["stop"]] == ["interrupted"]


def test_objectives_error_while_measuring_is_a_named_stop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from okg.optimizer.objectives import ObjectivesError, ObjectivesProblem
    from okg.optimizer.replay import runner as runner_mod

    fake = FakeOpenAI()
    config, outcome = _full_run(tmp_path, fake)
    assert (outcome.run_dir / "measurement.json").is_file()

    def broken(*_a, **_k):
        raise ObjectivesError([ObjectivesProblem("objective_unknown_measure", "synthetic refusal")])
    monkeypatch.setattr(runner_mod, "score_objectives", broken)
    again = judge_existing(config, outcome.run_dir, key=KEY, command="test", transport=fake.transport(), okg=OKG_ID)
    assert again.stop is not None and again.stop.codes == ("objective_unknown_measure",)
    summary = json.loads((outcome.run_dir / "run.json").read_text())
    assert summary["status"] == "stopped" and summary["stop_phase"] == "measure"
    assert not (outcome.run_dir / "measurement.json").exists()  # no record outlives a stopped re-measure


def test_reserve_charged_blip_is_left_out_of_the_budget_ratio(tmp_path: Path) -> None:
    # Known cost per try is 2 x $0.000107 = $0.000214; one blip settles at the $0.01 reserve.
    budget = {"budget.expected_usd_per_try": 0.0002, "budget.check_after_tries": 2}
    _, outcome = _full_run(tmp_path / "blip", FakeOpenAI(transport_errors=1), workers=1, extra=budget)
    b = outcome.summary["budget"]
    assert outcome.stop is None and outcome.summary["status"] == "complete"
    assert b["checked"] is True and b["ratio"] < 1.5
    assert b["reserve_charged_calls"] == 1 and b["reserve_charged_usd"] == pytest.approx(0.01)
    assert outcome.summary["spend"]["answer_usd"] >= 0.01  # the cap still charges the blip
    overrun = {"budget.expected_usd_per_try": 0.0001, "budget.check_after_tries": 2}
    _, stopped = _full_run(tmp_path / "over", FakeOpenAI(transport_errors=1), workers=1, extra=overrun)
    assert stopped.stop is not None and stopped.stop.codes == ("budget_ratio_exceeded",)
    assert stopped.summary["budget"]["reserve_charged_calls"] == 1


# --- round 3: a stop already set is never replaced ----------------------------------------------------------------


def _probe_stopped_run(tmp_path: Path):
    server = FakeServer()
    calls = {"n": 0}
    original = server.tool_defs

    def breaking():  # 1 (prepare) + 3x2 (tries) reads, then the post-run probe fails
        calls["n"] += 1
        if calls["n"] > 1 + 3 * 2:
            raise RuntimeError("probe broke")
        return original()
    server.tool_defs = breaking  # type: ignore[method-assign]
    return _full_run(tmp_path, FakeOpenAI(), server)


def test_interrupted_or_crashed_rejudge_keeps_the_probe_stop(tmp_path: Path) -> None:
    config, outcome = _probe_stopped_run(tmp_path)
    run_json = outcome.run_dir / "run.json"
    assert json.loads(run_json.read_text())["stop_phase"] == "probe"
    with pytest.raises(KeyboardInterrupt):
        judge_existing(config, outcome.run_dir, key=KEY, command="test", okg=OKG_ID,
                       transport=FakeOpenAI(judge_raises=KeyboardInterrupt()).transport())
    summary = json.loads(run_json.read_text())
    assert summary["status"] == "stopped" and summary["stop_phase"] == "probe"
    assert [p["code"] for p in summary["stop"]] == ["run_crashed"]
    assert summary["last_interrupt"]["code"] == "interrupted"
    crashed = judge_existing(config, outcome.run_dir, key=KEY, command="test", okg=OKG_ID,
                             transport=FakeOpenAI(judge_raises=RuntimeError("judge broke")).transport())
    assert crashed.summary["stop_phase"] == "probe" and crashed.summary["last_interrupt"]["code"] == "run_crashed"
    fine = judge_existing(config, outcome.run_dir, key=KEY, command="test", okg=OKG_ID,
                          transport=FakeOpenAI().transport())
    assert fine.summary["status"] == "stopped" and fine.summary["stop_phase"] == "probe" and fine.record is None
    assert not (outcome.run_dir / "measurement.json").exists()


def test_teardown_error_does_not_replace_the_stop(tmp_path: Path) -> None:
    import contextlib

    @contextlib.asynccontextmanager
    async def failing_teardown():
        yield URL, {"Authorization": "Bearer t"}
        raise RuntimeError("server stop failed")
    config = _config(tmp_path, workers=1, extra={"budget.expected_usd_per_try": 0.0001,
                                                 "budget.check_after_tries": 2})
    outcome = asyncio.run(run_replay(config, run_id="run-t", connector=fake_connector({URL: FakeServer()}),
                                     server=failing_teardown, key=KEY, command="test",
                                     transport=FakeOpenAI().transport(), okg=OKG_ID, sleep=_no_sleep))
    assert outcome.stop is not None and outcome.stop.codes == ("budget_ratio_exceeded",)
    summary = json.loads((outcome.run_dir / "run.json").read_text())
    assert summary["stop_phase"] == "answer" and summary["error_during"]["code"] == "run_crashed"
    assert summary["error_during"]["step"] == "teardown"
    from okg.optimizer.replay.runner import describe

    assert describe(outcome)["error_during"]["step"] == "teardown" and "last_interrupt" not in describe(outcome)


def test_teardown_error_after_a_passed_check_is_resumable_and_judge_completes_it(tmp_path: Path) -> None:
    import contextlib

    @contextlib.asynccontextmanager
    async def failing_teardown():
        yield URL, {"Authorization": "Bearer t"}
        raise RuntimeError("server stop failed")
    fake = FakeOpenAI()
    config = _config(tmp_path)
    outcome = asyncio.run(run_replay(config, run_id="run-r", connector=fake_connector({URL: FakeServer()}),
                                     server=failing_teardown, key=KEY, command="test", transport=fake.transport(),
                                     okg=OKG_ID, sleep=_no_sleep))
    assert outcome.stop is not None and outcome.stop.codes == ("run_crashed",)
    assert outcome.summary["stop_phase"] == "teardown" and outcome.record is None
    assert outcome.summary["error_during"]["step"] == "teardown" and fake.judge_requests == 0
    again = judge_existing(config, outcome.run_dir, key=KEY, command="test", transport=fake.transport(), okg=OKG_ID)
    assert again.stop is None and again.summary["status"] == "complete"
    assert (outcome.run_dir / "measurement.json").is_file()
