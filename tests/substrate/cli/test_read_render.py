"""The parse step and the render step of the read commands, without a database
(PACT read-fast: the seam a thin client reuses).

* `parse_read_call` and `render_reply` / `render_refusal` / `render_failure`
  run without a database and without the server module;
* golden: what the commands printed BEFORE the split
  (`fixtures/read_render_golden.json`, recorded by
  `_read_render_golden_build.py` on dev 4c28b5a92c0) is what they print
  now, through `main(argv)`, and through the two steps alone;
* the one JSON error shape is defined by the render step.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from okg.substrate.cli import _read_call, _read_operators as ops, _read_render
from okg.substrate.cli import _read_shell
from okg.substrate.cli.main import _build_parser
from okg.substrate.errors import SubstrateError
from tests.substrate.cli import _read_render_golden_build as golden

GOLDEN = json.loads(
    (Path(__file__).parent / "fixtures" / "read_render_golden.json").read_text(encoding="utf-8")
)
CASES = {case["name"]: case for case in golden.CASES}


@pytest.fixture(autouse=True)
def _environment(monkeypatch):
    monkeypatch.setenv("OKG_DEPLOYMENT", golden.DEPLOYMENT)
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    monkeypatch.delenv("OKG_AGENT", raising=False)


def _parsed(argv: list[str]) -> tuple[argparse.Namespace, bool]:
    args = _build_parser().parse_args(argv)
    return args, bool(getattr(args, "json", False))


def test_every_recorded_case_has_a_golden_and_the_other_way_round():
    assert set(GOLDEN) == set(CASES)
    kinds = {case["kind"] for case in CASES.values()}
    assert kinds == {"reply", "parse_refusal", "failure", "driver"}


# ── no database, no server module ────────────────────────────────────────


def test_parse_and_render_import_neither_the_server_nor_a_database():
    script = """
import argparse, json, sys
from okg.substrate.cli import _read_call, _read_render
steps_loaded_psycopg = "psycopg" in sys.modules
from okg.substrate.cli.main import _build_parser

args = _build_parser().parse_args(["query", "select 1"])
call = _read_call.parse_read_call("query", args, as_json=False)
assert call.operator == "query" and call.arguments == {"sql": "select 1"}, call
reply = _read_render.Reply("query ok · rows=1\\nrow 1: 1")
done = _read_render.render_reply(
    "query", reply, as_json=False, arguments=call.arguments, args=args,
)
assert done.exit_code == 0 and done.stdout.startswith("query ok"), done
refused = _read_render.Reply("query refused bad\\nmessage: m", is_error=True)
done = _read_render.render_reply(
    "query", refused, as_json=False, arguments=call.arguments, args=args,
    refusal_hint="hint: x",
)
assert done.exit_code == 2 and done.stderr.endswith("hint: x\\n"), done
from okg.substrate.errors import SubstrateError
err = SubstrateError("no server", fix_hint="start one")
err.code = "no_server"
assert _read_render.render_refusal(err, "query", args, mode="json").exit_code == 2
assert _read_render.render_failure(err, as_json=True).exit_code == 1
bad = sorted(
    name for name in sys.modules
    if name == "okg.substrate.mcp.server" or name.startswith("okg.substrate.mcp.server.")
)
print(json.dumps({"server_modules": bad, "psycopg": steps_loaded_psycopg,
                  "mcp_modules": sorted(n for n in sys.modules if n.startswith("okg.substrate.mcp"))}))
"""
    env = {**os.environ, "OKG_DEPLOYMENT": golden.DEPLOYMENT}
    env.pop("OKG_READ_DEBUG", None)
    done = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, env=env,
        cwd=str(Path(__file__).resolve().parents[3]), timeout=120,
    )
    assert done.returncode == 0, done.stderr[-2000:]
    seen = json.loads(done.stdout.strip().splitlines()[-1])
    assert seen["server_modules"] == [], seen["mcp_modules"]
    # The two steps alone load no database driver. (The command parser in
    # `cli.main`, imported after them, does load psycopg, so this is read
    # before that import.)
    assert seen["psycopg"] is False


# ── golden, through main(argv): the whole command is unchanged ───────────


@pytest.mark.parametrize("name", sorted(CASES))
def test_the_command_prints_what_it_printed_before_the_split(name):
    got = golden.run_case(CASES[name])
    assert got == GOLDEN[name]


# ── golden, through the two steps alone ──────────────────────────────────


def _replied(case) -> tuple:
    argv = case["argv"]
    args, as_json = _parsed(argv)
    operator = str(args.command)
    return args, as_json, operator


@pytest.mark.parametrize("name", sorted(n for n, c in CASES.items() if c["kind"] == "reply"))
def test_parse_then_render_equals_what_the_command_printed(name):
    case = CASES[name]
    args, as_json, operator = _replied(case)
    call = _read_call.parse_read_call(operator, args, as_json=as_json)
    assert not call.needs_driver
    want = GOLDEN[name]
    # What parse says to send is what the command sent.
    assert [[call.arguments, call.result_format]] == want["sent"]
    spec = golden.reply_spec(case, call.arguments, call.result_format)
    reply = _read_render.reply_of(golden.Result(spec))
    done = _read_render.render_reply(
        operator, reply, as_json=call.choices.as_json, arguments=call.arguments,
        args=args, refusal_hint=call.choices.refusal_hint,
    )
    assert (done.stdout, done.stderr, done.exit_code) == (
        want["stdout"], want["stderr"], want["exit"],
    )


@pytest.mark.parametrize("name", sorted(n for n, c in CASES.items() if c["kind"] == "parse_refusal"))
def test_parse_refuses_what_the_command_refused(name):
    case = CASES[name]
    args, as_json, operator = _replied(case)
    with pytest.raises(_read_call.ReadCallRefused) as refused:
        _read_call.parse_read_call(operator, args, as_json=as_json)
    done = _read_render.render_refusal(
        refused.value.error, refused.value.operator, args, mode=refused.value.mode,
    )
    want = GOLDEN[name]
    assert want["sent"] == []
    assert (done.stdout, done.stderr, done.exit_code) == (
        want["stdout"], want["stderr"], want["exit"],
    )
    assert done.exit_code == 2


def test_a_refusal_in_json_is_one_document_with_status_refused():
    case = CASES["parse_refusal_args_twice_json"]
    document = json.loads(GOLDEN["parse_refusal_args_twice_json"]["stderr"])
    assert document["status"] == "refused"
    assert set(document["error"]) >= {"code", "message", "data"}
    assert document["error"]["code"] == "read_argument_given_twice"
    assert case["kind"] == "parse_refusal"


# ── the parse step's output choices ──────────────────────────────────────


@pytest.mark.parametrize(
    ("argv", "mode", "as_json", "all_pages", "needs_driver"),
    [
        (["filter", "--input", "{}"], "text", False, False, False),
        (["--json", "filter", "--input", "{}"], "json", True, False, False),
        (["filter", "--input", "{}", "--jsonl"], "jsonl", False, False, True),
        (["filter", "--input", "{}", "--ids"], "ids", False, False, True),
        (["filter", "--input", "{}", "--all"], "jsonl", False, True, True),
        (["--json", "filter", "--input", "{}", "--all"], "json", True, True, True),
    ],
)
def test_the_output_choices_say_how_the_reply_is_printed(argv, mode, as_json, all_pages, needs_driver):
    args, want_json = _parsed(argv)
    call = _read_call.parse_read_call("filter", args, as_json=want_json)
    assert (call.choices.mode, call.choices.as_json, call.choices.all_pages, call.needs_driver) == (
        mode, as_json, all_pages, needs_driver,
    )
    assert call.result_format == ("json" if as_json else "text")


def test_parse_of_search_gives_the_embedder_and_the_tool_arguments():
    args, as_json = _parsed(["search", "words", "--view", "list", "--embedder", "m:1"])
    call = _read_call.parse_read_call("search", args, as_json=as_json)
    assert call.operator == "search"
    assert call.arguments == {"query": "words", "view": "list"}
    assert call.embedder_spec == "m:1"


def test_parse_of_query_that_is_not_sql_carries_the_search_hint():
    args, as_json = _parsed(["query", "recent pacts"])
    call = _read_call.parse_read_call("query", args, as_json=as_json)
    assert call.choices.refusal_hint == ops.TEXT_SEARCH_HINT


def test_parse_names_the_deployment_it_resolved(monkeypatch):
    args, as_json = _parsed(["query", "select 1"])
    _read_call.parse_read_call("query", args, as_json=as_json)
    assert args.deployment == golden.DEPLOYMENT


def test_an_error_other_than_a_refusal_is_not_turned_into_one(monkeypatch):
    """Only the refusals of the command line are `ReadCallRefused`; any other
    failure while planning is raised as it is, as before."""
    def boom(*a, **k):
        raise SubstrateError("not a shell refusal")

    monkeypatch.setattr(_read_shell, "build_plan", boom)
    args, as_json = _parsed(["query", "select 1"])
    with pytest.raises(SubstrateError, match="not a shell refusal"):
        _read_call.parse_read_call("query", args, as_json=as_json)


# ── the one JSON error shape ─────────────────────────────────────────────


def _failure(code="golden_failure"):
    err = SubstrateError("it broke", data={"where": "here"}, fix_hint="mend it")
    err.code = code
    return err


def test_a_failure_document_is_the_errors_own_json():
    err = _failure()
    document = _read_render.error_document(
        err.code, err.message, fix_hint=err.fix_hint, data=err.data,
    )
    assert document == err.to_json()
    bare = SubstrateError("plain")
    bare.code = "plain_code"
    assert _read_render.error_document(
        bare.code, bare.message, fix_hint=bare.fix_hint, data=bare.data,
    ) == bare.to_json()


def test_a_refusal_document_adds_status_and_the_retry_call():
    call = {"tool": "filter", "mode": "corrected", "arguments": {"limit": 5}}
    document = _read_render.error_document(
        "c", "m", fix_hint="f", data={"k": 1}, retry_call=call, status="refused",
    )
    assert document == {
        "status": "refused",
        "error": {"code": "c", "message": "m", "fix_hint": "f", "data": {"k": 1}, "retry_call": call},
    }
    assert list(document["error"]) == ["code", "message", "fix_hint", "data", "retry_call"]


@pytest.mark.parametrize("as_json", [False, True])
def test_render_failure_equals_the_legacy_emitter(capsys, as_json):
    from okg.substrate.cli import _read_side

    err = _failure()
    assert _read_side._emit_error(err, as_json=as_json) == 1
    legacy = capsys.readouterr()
    done = _read_render.render_failure(err, as_json=as_json)
    assert (done.stdout, done.stderr, done.exit_code) == (legacy.out, legacy.err, 1)


def test_render_refusal_exits_2_and_writes_nothing_to_stdout():
    args = argparse.Namespace()
    err = _failure("no_server")
    for mode in ("json", "text"):
        done = _read_render.render_refusal(err, "filter", args, mode=mode)
        assert done.exit_code == 2 and done.stdout == ""
    document = json.loads(_read_render.render_refusal(err, "filter", args, mode="json").stderr)
    assert document == {
        "status": "refused",
        "error": {"code": "no_server", "message": "it broke", "fix_hint": "mend it", "data": {"where": "here"}},
    }
    text = _read_render.render_refusal(err, "filter", args, mode="text").stderr
    assert text.splitlines()[0] == "filter refused no_server: it broke"
    assert text.splitlines()[1] == "  fix_hint: mend it"


def test_render_masks_a_path_in_a_refusal():
    err = SubstrateError("cannot read /home/someone/okg/x.py", fix_hint="fix")
    err.code = "c"
    done = _read_render.render_refusal(err, "filter", argparse.Namespace(), mode="text")
    assert "/home/someone" not in done.stderr and "<path>" in done.stderr


def test_the_write_of_a_rendering_goes_to_the_two_streams(capsys):
    done = _read_render.Rendered(stdout="out\n", stderr="err\n", exit_code=2)
    assert done.write() == 2
    captured = capsys.readouterr()
    assert (captured.out, captured.err) == ("out\n", "err\n")


def test_a_reply_is_read_from_a_call_tool_result():
    class Block:
        def __init__(self, type_, text=""):
            self.type, self.text = type_, text

    class Result:
        content = [Block("text", "first"), Block("text", "second"), Block("image")]
        isError = True
        structuredContent = {"a": 1}

    reply = _read_render.reply_of(Result())
    assert reply == _read_render.Reply("first", True, 2, {"a": 1})
    assert _read_render.reply_of(object()) == _read_render.Reply("", False, 0, None)


def test_the_shell_exit_codes_agree_with_the_render_step():
    assert _read_render.REFUSED_EXIT == _read_shell.REFUSED_EXIT == 2
