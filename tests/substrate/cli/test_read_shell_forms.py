"""Shell forms and pipe output of the okg read commands, without a database
(#3666 step S2, PACT read-shell-forms-3666).

`--where k=v`, the bare forms agents typed, `--jsonl`, `--ids`, `--all`,
ids from standard input and the `retry:` line. Each parse is checked
against the argument object it must build, each renderer against a reply of
the tool's shape. What the tools then answer is covered against a real
publish in `test_read_operator_commands_db.py`.
"""
from __future__ import annotations

import io
import json
import shlex
from typing import Any

import pytest

from okg.substrate.cli import _read_operators as ops
from okg.substrate.cli import _read_shell as shell
from okg.substrate.cli.main import _build_parser, main


def _plan(argv: list[str], *, as_json: bool = False) -> shell.Plan:
    args = _build_parser().parse_args(argv)
    ops.require_deployment(args, args.command, as_json=as_json)
    if args.command == "search":
        arguments = ops.search_operator_arguments(args)
    else:
        arguments = ops.operator_arguments(args.command, args)
    return shell.build_plan(args.command, args, arguments, as_json=as_json)


def _refused(argv: list[str], **kwargs: Any) -> shell.ShellRefusal:
    with pytest.raises(shell.ShellRefusal) as refused:
        _plan(argv, **kwargs)
    return refused.value


# ── --where k=v ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("term", "condition"),
    [
        ("attrs.status=Open", {"attrs.status": "Open"}),
        ("attrs.count=3", {"attrs.count": 3}),
        ("attrs.ratio=-1.5e2", {"attrs.ratio": -150.0}),
        ("attrs.key=007", {"attrs.key": "007"}),
        ('attrs.key="12"', {"attrs.key": "12"}),
        ("attrs.done=true", {"attrs.done": True}),
        ("attrs.done=false", {"attrs.done": False}),
        ("attrs.owner=null", {"op": "not", "clause": {"attrs.owner": {"op": "exists"}}}),
        ("attrs.owner!=null", {"attrs.owner": {"op": "exists"}}),
        ("attrs.status!=Open", {"op": "not", "clause": {"attrs.status": "Open"}}),
        ("attrs.status=Open,Closed", {"attrs.status": ["Open", "Closed"]}),
        ('attrs.title="a,b"', {"attrs.title": "a,b"}),
        ("attrs.status!=Open,Closed",
         {"op": "not", "clause": {"attrs.status": ["Open", "Closed"]}}),
        ("attrs.count>3", {"attrs.count": {"op": "gt", "value": 3}}),
        ("date.created<2026-01-01", {"date.created": {"op": "lt", "value": "2026-01-01"}}),
        ("date.created>=2026-01-01", {"op": "or", "clauses": [
            {"date.created": {"op": "gt", "value": "2026-01-01"}},
            {"date.created": "2026-01-01"},
        ]}),
        ("attrs.count<=3", {"op": "or", "clauses": [
            {"attrs.count": {"op": "lt", "value": 3}}, {"attrs.count": 3},
        ]}),
        ("attrs.path~lib/rucio/", {"attrs.path": {"op": "prefix", "value": "lib/rucio/"}}),
        ("type=jira_issue", {"type": "jira_issue"}),
        (" attrs.summary = Fix the reaper ", {"attrs.summary": "Fix the reaper"}),
        ("attrs.url=https://x/y?a=b", {"attrs.url": "https://x/y?a=b"}),
    ],
)
def test_one_term_is_the_condition_the_tools_take(term, condition):
    assert shell.where_condition([term]) == condition


def test_terms_are_joined_by_and():
    assert shell.where_condition(["type=ticket", "attrs.status=Open"]) == {
        "type": "ticket", "attrs.status": "Open",
    }
    # One field twice, or a long form among them: an explicit and.
    assert shell.where_condition(["attrs.n>1", "attrs.n<5"]) == {"op": "and", "clauses": [
        {"attrs.n": {"op": "gt", "value": 1}}, {"attrs.n": {"op": "lt", "value": 5}},
    ]}
    assert shell.where_condition(['{"attrs.a": 1}', "attrs.b=2"]) == {
        "attrs.a": 1, "attrs.b": 2,
    }


def test_one_json_where_is_sent_exactly_as_typed():
    for raw in ('{"op":"eq","field":"type","value":"doc"}', "[1, 2]", "{}"):
        assert shell.where_condition([raw]) == json.loads(raw)


@pytest.mark.parametrize(
    ("term", "corrected"),
    [
        ("attrs.status==Open", {"attrs.status": "Open"}),
        ("attrs.status='Open'", {"attrs.status": "Open"}),
        ("attrs.status != 'Open'", {"op": "not", "clause": {"attrs.status": "Open"}}),
        # Quote marks the shell passed on are never part of a value.
        ('attrs.status="Open","Closed"', {"attrs.status": ["Open", "Closed"]}),
        ("attrs.status='Open','Closed'", {"attrs.status": ["Open", "Closed"]}),
        ('attrs.title=a,"b,c"', {"attrs.title": ["a", "b,c"]}),
        ('attrs.status!="Open","Closed"',
         {"op": "not", "clause": {"attrs.status": ["Open", "Closed"]}}),
    ],
)
def test_a_term_with_one_reading_is_refused_with_that_reading(term, corrected):
    with pytest.raises(shell.WhereRefusal) as refused:
        shell.where_condition([term])
    assert refused.value.corrected == corrected
    # Beside other terms, the whole condition is corrected.
    with pytest.raises(shell.WhereRefusal) as refused:
        shell.where_condition([term, "type=ticket"])
    assert refused.value.corrected == shell._joined([corrected, {"type": "ticket"}])


@pytest.mark.parametrize(
    "term",
    [
        "status Open",
        "attrs.status=",
        "status = 'Open' AND priority > 3",
        "attrs.n>=<3",
        "attrs.done>true",
        "attrs.owner=a,null",
        "attrs.status=a,,b",
        '{"attrs.status": ',
        'attrs.title=say "hi"',
        "attrs.title='it''s'",
        'attrs.status="Open" "Closed"',
        'attrs.n>"1","2"',
    ],
)
def test_a_term_that_is_not_a_clean_k_op_v_is_refused_without_a_guess(term):
    with pytest.raises(shell.WhereRefusal) as refused:
        shell.where_condition([term])
    assert refused.value.corrected is None
    assert refused.value.fix_hint


def test_a_refused_where_prints_the_json_form_as_a_command_that_runs(capsys):
    rc = main(["filter", "--deployment", "d", "--input", '{"kind":"nodes"}',
               "--where", "status = 'Open' AND priority > 3"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "filter refused read_where_invalid" in err
    # Two conditions in one term: no command is offered for a guess.
    assert not [line for line in err.splitlines() if line.startswith("retry: ")]

    rc = main(["filter", "--deployment", "d", "--input", '{"kind":"nodes"}',
               "--where", "attrs.status==Open", "--limit", "5"])
    assert rc == 2
    err = capsys.readouterr().err
    (line,) = [line for line in err.splitlines() if line.startswith("retry: ")]
    argv = shlex.split(line[len("retry: "):])
    assert argv[0] == "okg"
    plan = _plan(argv[1:])
    assert plan.arguments == {
        "input": {"kind": "nodes"}, "limit": 5, "where": {"attrs.status": "Open"},
    }


@pytest.mark.parametrize(
    ("term", "where"),
    [
        ('attrs.status="Open","Closed"', {"attrs.status": ["Open", "Closed"]}),
        ("attrs.status='Open','Closed'", {"attrs.status": ["Open", "Closed"]}),
        ('attrs.title=a,"b,c"', {"attrs.title": ["a", "b,c"]}),
    ],
)
def test_a_where_retry_is_the_whole_command_as_typed(term, where, capsys):
    """Every flag stays: --type, --stdin, the output flags, the tool's own;
    only the --where terms become one JSON condition."""
    rc = main(["filter", "--deployment", "d", "--type", "doc", "--stdin", "--limit", "7",
               "--where", "attrs.n>1", "--where", term, "--ids", "--all"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "filter refused read_where_invalid" in err
    (line,) = [line for line in err.splitlines() if line.startswith("retry: ")]
    argv = shlex.split(line[len("retry: "):])
    assert argv[:2] == ["okg", "filter"]
    for flag in ("--type", "--stdin", "--limit", "--ids", "--all", "--deployment"):
        assert flag in argv, flag
    assert argv.count("--where") == 1
    args = _build_parser().parse_args(argv[1:])
    assert (args.shell_type, args.shell_stdin, args.shell_ids, args.shell_all) == (
        "doc", True, True, True,
    )
    assert shell.where_condition(args.shell_where) == shell._joined([
        {"attrs.n": {"op": "gt", "value": 1}}, where,
    ])
    assert ops.operator_arguments("filter", args) == {"limit": 7}


def test_a_search_where_retry_keeps_every_flag(capsys):
    rc = main(["search", "--deployment", "d", "words", "--limit", "5", "--select",
               "node_id", "--where", "type==doc", "--ids"])
    assert rc == 2
    (line,) = [line for line in capsys.readouterr().err.splitlines() if line.startswith("retry: ")]
    argv = shlex.split(line[len("retry: "):])[1:]
    args = _build_parser().parse_args(argv)
    assert ops.search_operator_arguments(args) == {
        "query": "words", "limit": 5, "select": ["node_id"], "where": {"type": "doc"},
    }
    assert args.shell_ids is True


def test_search_where_takes_the_same_terms():
    plan = _plan(["search", "--deployment", "d", "words", "--where", "type=doc",
                  "--where", "attrs.n>2"])
    assert plan.arguments["where"] == {"type": "doc", "attrs.n": {"op": "gt", "value": 2}}
    with pytest.raises(shell.ShellRefusal) as refused:
        _plan(["search", "--deployment", "d", "words", "--where", "type==doc"])
    assert refused.value.retry == ("search", {"query": "words", "where": {"type": "doc"}})


def test_where_on_each_command_that_takes_a_condition():
    for operator in shell.WHERE_COMMANDS:
        rest = ["doc:1"] if operator == "expand" else ["--input", '{"kind":"nodes"}']
        plan = _plan([operator, "--deployment", "d", *rest, "--where", "attrs.a=1"])
        assert plan.arguments["where"] == {"attrs.a": 1}, operator
    refused = _refused(["filter", "--deployment", "d", "--args", '{"where": {}}',
                        "--where", "a=1"])
    assert refused.code == "read_argument_given_twice"


# ── bare forms ───────────────────────────────────────────────────────────


def test_a_bare_target_is_the_node_and_says_so():
    for argv, node_id in (
        (["inspect", "--deployment", "d", "--target", "jira:CMS-1"], "jira:CMS-1"),
        (["inspect", "--deployment", "d", "--target", '"doc:1"'], "doc:1"),
        (["inspect", "--deployment", "d", "--target", "123"], "123"),
        (["inspect", "--deployment", "d", "doc:2"], "doc:2"),
    ):
        plan = _plan(argv)
        assert plan.arguments["target"] == {"kind": "node", "node_id": node_id}
        assert plan.shell and len(plan.adjusted) == 1
        assert plan.adjusted[0]["served"].startswith("target {")
    # A JSON target is the call as typed, with no note.
    plan = _plan(["inspect", "--deployment", "d", "--target", '{"kind":"subtype","subtype":"doc"}'])
    assert plan.arguments["target"] == {"kind": "subtype", "subtype": "doc"}
    assert not plan.shell and plan.adjusted == []
    assert _refused(["inspect", "--deployment", "d", "doc:1", "--target", "doc:2"]).code == (
        "read_argument_given_twice"
    )


def test_a_bare_input_word_is_a_subtype_the_catalog_must_declare():
    plan = _plan(["filter", "--deployment", "d", "--input", "git_commit"])
    assert plan.arguments["input"] == {"kind": "nodes", "subtype": "git_commit"}
    assert plan.subtype_words == [("--input", "git_commit")]
    # A node id or text is not read as a subtype: the tool answers it.
    for value in ("doc:000", "status = 'Open'"):
        plan = _plan(["filter", "--deployment", "d", "--input", value])
        assert plan.arguments["input"] == value and not plan.subtype_words


def test_a_subtype_object_without_kind_is_a_node_set():
    for operator in ("filter", "map"):
        plan = _plan([operator, "--deployment", "d", "--input", '{"subtype": "ticket"}'])
        assert plan.arguments["input"] == {"kind": "nodes", "subtype": "ticket"}
        assert plan.adjusted and not plan.subtype_words
    # aggregate already takes it as written.
    plan = _plan(["aggregate", "--deployment", "d", "--input", '{"subtype": "ticket"}'])
    assert plan.arguments["input"] == {"subtype": "ticket"} and not plan.adjusted


def test_type_is_the_subtype_of_the_input():
    plan = _plan(["map", "--deployment", "d", "--type", "ticket", "--projection", "node_id"])
    assert plan.arguments["input"] == {"kind": "nodes", "subtype": "ticket"}
    plan = _plan(["filter", "--deployment", "d", "--input", '{"kind": "nodes"}', "--type", "ticket"])
    assert plan.arguments["input"] == {"kind": "nodes", "subtype": "ticket"}
    assert plan.subtype_words == [("--type", "ticket")]
    refused = _refused(["filter", "--deployment", "d", "--input",
                        '{"kind": "nodes", "subtype": "doc"}', "--type", "ticket"])
    assert refused.code == "read_input_unresolved"


class _Cursor:
    def __init__(self, names: list[str]) -> None:
        self.names = names

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        assert "okg.subtypes" in sql

    def fetchall(self) -> list[tuple[str]]:
        return [(name,) for name in self.names]


class _Pool:
    def __init__(self, names: list[str]) -> None:
        self.names = names

    def connection(self):
        pool = self

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *exc) -> None:
                return None

            def cursor(self):
                return _Cursor(pool.names)

        return _Conn()


class _Server:
    def __init__(self, names: list[str]) -> None:
        self._okg_pool = _Pool(names)


def test_a_word_the_catalog_does_not_declare_is_refused_with_the_nearest():
    plan = _plan(["filter", "--deployment", "d", "--input", "git_comit"])
    with pytest.raises(shell.ShellRefusal) as refused:
        shell.resolve_subtypes(plan, _Server(["git_commit", "git_tag", "ticket"]))
    assert refused.value.code == "read_input_unresolved"
    assert "git_commit" in refused.value.fix_hint
    shell.resolve_subtypes(_plan(["filter", "--deployment", "d", "--input", "ticket"]),
                           _Server(["ticket"]))


# ── output flags, select and stdin ───────────────────────────────────────


def test_output_modes():
    base = ["filter", "--deployment", "d", "--input", '{"kind":"nodes"}']
    assert (_plan(base).mode, _plan(base).all_pages) == ("text", False)
    assert _plan([*base, "--all"]).mode == "jsonl" and _plan([*base, "--all"]).all_pages
    assert _plan([*base, "--ids", "--all"]).mode == "ids"
    assert _plan(base, as_json=True).mode == "json"
    assert _refused([*base, "--jsonl", "--ids"]).code == "read_output_flags_conflict"
    assert _refused([*base, "--jsonl"], as_json=True).code == "read_output_flags_conflict"


def test_one_row_per_line_carries_its_id_and_subtype():
    plan = _plan(["filter", "--deployment", "d", "--type", "ticket", "--select",
                  "attrs.status", "--jsonl"])
    assert plan.arguments["select"] == ["node_id", "subtype", "attrs.status"]
    plan = _plan(["map", "--deployment", "d", "--type", "ticket", "--projection",
                  "attrs.status", "--ids"])
    assert plan.arguments["projection"] == ["node_id", "attrs.status"]
    # Text output sends the select as typed.
    plan = _plan(["filter", "--deployment", "d", "--type", "ticket", "--select", "attrs.status"])
    assert plan.arguments["select"] == ["attrs.status"]


def test_record_text_is_refused_with_the_inspect_form():
    refused = _refused(["filter", "--deployment", "d", "--type", "ticket", "--select",
                        "node_id,text", "--all", "--jsonl"])
    assert refused.code == "read_select_text_unavailable"
    assert '"kind": "texts"' in refused.fix_hint


def _stdin(monkeypatch, text: str) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO(text))


def test_ids_from_standard_input(monkeypatch):
    ids = [f"doc:{i:03d}" for i in range(120)]
    lines = "\n".join(ids[:60]) + "\n\n" + "\n".join(
        json.dumps({"node_id": i, "subtype": "doc"}) for i in ids[60:]
    ) + "\n" + ids[0] + "\n"
    _stdin(monkeypatch, lines)
    plan = _plan(["inspect", "--deployment", "d", "-", "--select", "node_id"])
    assert plan.stdin_ids == ids
    calls = plan.calls()
    assert [len(c["target"]["node_ids"]) for c in calls] == [50, 50, 20]
    assert all(c["select"] == ["node_id"] for c in calls)

    _stdin(monkeypatch, "\n".join(ids))
    plan = _plan(["filter", "--deployment", "d", "--stdin", "--where", "attrs.a=1"])
    (call,) = plan.calls()
    assert call["input"] == {"kind": "nodes"}
    assert call["where"] == {"attrs.a": 1, "node_id": ids}

    _stdin(monkeypatch, "doc:1\ndoc:2\n")
    plan = _plan(["expand", "--deployment", "d", "-", "--edge-type", "contains"])
    assert [c["input"] for c in plan.calls()] == ["doc:1", "doc:2"]
    assert all(c["edge_type"] == "contains" for c in plan.calls())


@pytest.mark.parametrize(
    ("piped", "line"),
    [
        ('doc:1\n{"subtype": "doc"}\n', 2),
        ("filter ok · rows 2 · generation 7\ndoc:1\n", 1),
        ("doc:1\n! adjusted: --type doc read as input\n", 2),
        ('{\n  "node_id": "doc:1"\n}\n', 1),
        ('doc:1\n  "node_id": "doc:2",\n', 2),
        ("[1] doc:1 (doc)\n", 1),
        ("doc 1\n", 1),
    ],
)
def test_a_line_that_is_not_an_id_is_refused_by_number(monkeypatch, piped, line):
    _stdin(monkeypatch, piped)
    refused = _refused(["inspect", "--deployment", "d", "--stdin"])
    assert refused.code == "read_stdin_invalid" and refused.data["line"] == line


def test_bad_standard_input_is_refused_by_line(monkeypatch):
    _stdin(monkeypatch, "\n".join(f"d:{i}" for i in range(shell.MAX_EXPAND_STDIN_IDS + 1)))
    assert _refused(["expand", "--deployment", "d", "-"]).code == "read_stdin_invalid"
    _stdin(monkeypatch, "doc:1\n")
    assert _refused(["inspect", "--deployment", "d", "-", "--target", "doc:2"]).code == (
        "read_argument_given_twice"
    )


def test_the_inspect_batch_is_the_batch_reads_own_limit():
    from okg.substrate.mcp.operator_models import MAX_NODE_BATCH

    assert shell.INSPECT_STDIN_BATCH == MAX_NODE_BATCH


# ── renderers ────────────────────────────────────────────────────────────


def test_rows_and_ids_of_each_reply_shape():
    assert shell.reply_rows("filter", {"items": [{"node_id": "a"}]}) == [{"node_id": "a"}]
    assert shell.reply_rows("inspect", {"result_kind": "node_batch", "records": [1]}) == [1]
    node = {"node_id": "a", "subtype": "doc"}
    assert shell.reply_rows("inspect", node) == [node]
    assert shell.reply_rows("aggregate", {"groups": [{"key": 1}]}) == [{"key": 1}]
    edge = {"edge_id": "e", "src": "a", "dst": "b"}
    assert shell.row_id("expand", {**edge, "direction": "out"}, {}) == "b"
    assert shell.row_id("expand", {**edge, "direction": "in"}, {}) == "a"
    assert shell.row_id("expand", edge, {"direction": "in"}) == "a"
    assert shell.row_id("filter", {"edge_id": "e"}, {}) == "e"
    assert shell.row_id("aggregate", {"key": 1}, {}) is None


def test_the_next_page_and_a_set_that_cannot_be_covered():
    sent = {"input": {"kind": "nodes"}, "limit": 2}
    assert shell.next_call("filter", sent, {"next_offset": 2, "generation_id": 7}, page=1) == {
        **sent, "offset": 2, "generation_id": 7,
    }
    assert shell.next_call("inspect", {"target": {}, "authority": {"a": 1}},
                           {"continuation_token": "t"}, page=1) == {"next": "t", "authority": {"a": 1}}
    assert shell.next_call("search", {"query": "q", "offset": 5},
                           {"continuation_token": "t"}, page=1) == {"query": "q", "continuation_token": "t"}
    assert shell.next_call("filter", sent, {"items": [], "truncated": False}, page=1) is None
    for payload in (
        {"truncated": True},
        {"next_offset_reason": "page_reference", "next_offset_remedy": "map the reference"},
        {"continuation_token": "t"},
    ):
        with pytest.raises(shell.ShellRefusal) as refused:
            shell.next_call("filter", sent, payload, page=3)
        assert refused.value.code == "read_all_incomplete", payload
    with pytest.raises(shell.ShellRefusal) as refused:
        shell.next_call("filter", sent, {"next_offset_reason": "x", "next_offset_remedy": "do y"}, page=3)
    assert refused.value.fix_hint == "do y"


def test_a_retry_command_parses_back_to_the_call_it_names():
    args = _build_parser().parse_args(["filter", "--deployment", "my graph", "--jsonl"])
    ops.require_deployment(args, "filter", as_json=False)
    call = {"tool": "expand", "mode": "corrected", "arguments": {
        "input": {"node_id": "doc:000"}, "edge_type": "contains", "select": ["dst"],
        "where": {"attrs.title": "it's \"quoted\""}, "limit": 7,
    }}
    line = shell.retry_line(call, {}, args)
    assert line.startswith("retry: okg expand --deployment 'my graph' --jsonl ")
    argv = shlex.split(line[len("retry: "):])[1:]
    parsed = _build_parser().parse_args(argv)
    built = ops.operator_arguments("expand", parsed)
    plan = shell.build_plan("expand", parsed, built, as_json=False)
    assert plan.arguments == call["arguments"]

    # Arguments the call names to repeat come from the call that was sent;
    # a guess says so; a continuation or a tool with no command is no line.
    guess = {"tool": "search", "mode": "guess", "arguments": {"query": "q"}, "repeat": ["authority"]}
    line = shell.retry_line(guess, {"authority": {"principal": "p"}}, args)
    assert line.endswith("  # a guess")
    assert json.loads(shlex.split(line.split("  #")[0])[-1]) == {
        "query": "q", "authority": {"principal": "p"},
    }
    assert shell.retry_line({**call, "mode": "continue"}, {}, args) is None
    assert shell.retry_line({**call, "tool": "record"}, {}, args) is None


def test_the_retry_call_is_read_from_a_text_refusal():
    call = {"tool": "filter", "mode": "corrected", "arguments": {"input": {"kind": "nodes"}}}
    one_line = f"filter refused x · retry: filter(...)\nmessage: m\nretry_call: {json.dumps(call)}"
    block = f"filter refused x\n--- retry_call\n{json.dumps(call)}"
    assert shell.retry_call_in_text(one_line) == call
    assert shell.retry_call_in_text(block) == call
    assert shell.retry_call_in_text("filter refused x\nretry: drop nonsense") is None


# ── the command, over a stand-in server ──────────────────────────────────


class _Block:
    type = "text"

    def __init__(self, text: str) -> None:
        self.text = text


class _Result:
    def __init__(self, payload: Any, *, is_error: bool = False) -> None:
        text = payload if isinstance(payload, str) else json.dumps(payload)
        self.content = [_Block(text)]
        self.isError = is_error


def _serve(monkeypatch, reply) -> list[tuple[dict, str]]:
    """Replace the server: `reply(arguments, result_format)` answers each call."""
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    calls: list[tuple[dict, str]] = []

    def call(server, operator, arguments, *, result_format):
        calls.append((arguments, result_format))
        return reply(arguments, result_format)

    monkeypatch.setattr(ops, "build_read_server", lambda args, *, surface, embedder_spec=None, dataset_card=True: object())
    monkeypatch.setattr(ops, "call_operator", call)
    monkeypatch.setattr(ops, "close_server", lambda server: None)
    return calls


def _page(offset: int, size: int, total: int) -> dict:
    rows = [{"node_id": f"doc:{i:03d}", "subtype": "doc"} for i in range(offset, min(offset + size, total))]
    page = {"status": "ok", "items": rows, "generation_id": 7, "offset": offset}
    if offset + size < total:
        page["next_offset"] = offset + size
    return page


def test_all_follows_next_to_the_end_and_prints_one_row_per_line(monkeypatch, capsys):
    calls = _serve(monkeypatch, lambda a, f: _Result(_page(a.get("offset", 0), 2, 5)))
    rc = main(["filter", "--deployment", "d", "--input", '{"kind":"nodes"}', "--limit", "2", "--all"])
    assert rc == 0
    out, err = capsys.readouterr()
    rows = [json.loads(line) for line in out.splitlines()]
    assert [row["node_id"] for row in rows] == [f"doc:{i:03d}" for i in range(5)]
    assert [a.get("offset") for a, _f in calls] == [None, 2, 4]
    # Later pages read the generation the first one read.
    assert all(a["generation_id"] == 7 for a, _f in calls[1:])
    assert all(f == "json" for _a, f in calls)
    assert err.splitlines()[0].startswith("filter ok")
    assert err.splitlines()[-1] == "okg filter --all: 5 rows in 3 pages"


#: Text a stored record may hold: every C0 control, DEL, the C1 next-line
#: control and the Unicode line and paragraph separators.
_AWKWARD_TEXT = (
    "a\nb\tc\rd" + "".join(chr(i) for i in range(32)) + "\x7f\x85e\u2028f\u2029g"
)


def test_jsonl_rows_stay_one_line_whatever_their_text_holds(monkeypatch, capsys):
    """Issue #3666: each `--jsonl` row is one line for any line reader, and
    reads back as the very row. Python's `json` leaves U+0085, U+2028 and
    U+2029 raw, and `str.splitlines` breaks a line at each."""
    rows = [{"node_id": f"doc:{i:03d}", "subtype": "doc", "attrs": {"text": _AWKWARD_TEXT}}
            for i in range(2)]
    _serve(monkeypatch, lambda a, f: _Result({"status": "ok", "items": rows}))
    rc = main(["filter", "--deployment", "d", "--input", '{"kind":"nodes"}', "--jsonl"])
    assert rc == 0
    out = capsys.readouterr().out
    assert out.endswith("\n")
    for lines in (out.splitlines(), out[:-1].split("\n"), io.StringIO(out).readlines()):
        assert [json.loads(line) for line in lines] == rows
    assert out.count("\n") == len(rows)
    assert not any(
        ord(char) < 0x20 or char in "\x85\u2028\u2029" for char in out.replace("\n", "")
    )


def test_all_with_json_prints_one_document_in_the_one_page_shape(monkeypatch, capsys):
    """Issue #3666: `--all --json` printed one JSON document per page, so the
    output as a whole did not parse. It prints one document: the last page's
    fields, every page's rows in order, and how many pages were joined."""
    def page(arguments, result_format):
        offset = arguments.get("offset", 0)
        reply = {**_page(offset, 2, 5), "limit": 2, "total_count": 5}
        reply["returned_count"] = len(reply["items"])
        if offset == 4:
            reply["limit"] = 1  # a last page that says less than was asked
        return _Result(reply)

    calls = _serve(monkeypatch, page)
    rc = main(["--json", "filter", "--deployment", "d", "--input", '{"kind":"nodes"}',
               "--limit", "2", "--all"])
    assert rc == 0
    out, err = capsys.readouterr()
    document = json.loads(out)
    assert [row["node_id"] for row in document["items"]] == [f"doc:{i:03d}" for i in range(5)]
    # Every count and paging field is the set's, not the last page's.
    assert document["returned_count"] == 5
    assert document["offset"] == 0 and document["limit"] == 2
    assert document["total_count"] == 5 and "next_offset" not in document
    assert document["all_pages"] == {"pages": 3, "rows": 5, "complete": True}
    assert len(calls) == 3
    assert "5 rows from 3 replies, printed as one JSON document" in err


def test_all_with_json_at_the_page_cap_prints_what_was_read_with_the_next_call(
    monkeypatch, capsys,
):
    monkeypatch.setattr(shell, "MAX_ALL_PAGES", 2)
    _serve(monkeypatch, lambda a, f: _Result(_page(a.get("offset", 0), 2, 10)))
    rc = main(["--json", "filter", "--deployment", "d", "--input", '{"kind":"nodes"}',
               "--limit", "2", "--all"])
    assert rc == 0
    out, err = capsys.readouterr()
    # The whole of stdout is one JSON value: the document, and no refusal.
    document = json.loads(out)
    assert [row["node_id"] for row in document["items"]] == [f"doc:{i:03d}" for i in range(4)]
    assert document["all_pages"] == {"pages": 2, "rows": 4, "complete": False}
    assert document["next_offset"] == 4
    assert "4 rows were printed and the set goes on" in err
    assert "0 rows" not in err and "refused" not in err
    (resume,) = [line for line in err.splitlines() if line.startswith("resume: ")]
    assert resume.startswith("resume: okg --json filter ")


def test_all_with_json_at_the_page_cap_names_the_stdin_ids_not_read(monkeypatch, capsys):
    """Ids from standard input over three calls; the cap stops the first,
    so the other two calls are not made, and stderr says how many ids."""
    monkeypatch.setattr(shell, "MAX_ALL_PAGES", 1)

    def batch(arguments, result_format):
        ids = arguments["where"]["node_id"]
        return _Result({"status": "ok", "items": [{"node_id": i, "subtype": "doc"} for i in ids[:2]],
                        "next_offset": 2})

    calls = _serve(monkeypatch, batch)
    _stdin(monkeypatch, "\n".join(f"doc:{i:04d}" for i in range(1200)))
    rc = main(["--json", "filter", "--deployment", "d", "--stdin", "--all"])
    assert rc == 0
    assert len(calls) == 1
    out, err = capsys.readouterr()
    document = json.loads(out)
    assert document["all_pages"] == {"pages": 1, "rows": 2, "complete": False}
    assert "700 of 1200 ids from standard input were not read (2 calls were not made)" in err


def test_one_page_with_json_prints_the_reply_as_it_came(monkeypatch, capsys):
    page = _page(0, 3, 3)
    _serve(monkeypatch, lambda a, f: _Result(page))
    rc = main(["--json", "filter", "--deployment", "d", "--input", '{"kind":"nodes"}', "--all"])
    assert rc == 0
    assert json.loads(capsys.readouterr().out) == page


def test_all_with_json_prints_nothing_when_a_later_page_is_refused(monkeypatch, capsys):
    """A set `--all` could not read to its end is never printed as one
    document: stdout stays empty and the refusal goes to stderr."""
    refusal = {"status": "refused", "error": {"code": "budget", "message": "busy"}}
    _serve(monkeypatch, lambda a, f: _Result(refusal if a.get("offset") else _page(0, 2, 5)))
    rc = main(["--json", "filter", "--deployment", "d", "--input", '{"kind":"nodes"}',
               "--limit", "2", "--all"])
    assert rc == 2
    out, err = capsys.readouterr()
    assert out == ""
    assert '"refused"' in err
    assert "okg filter: 2 rows from 1 page were read and not printed, because page 2 was refused" in err


def test_json_over_ids_expanded_one_call_each_prints_each_reply_unchanged(monkeypatch, capsys):
    """`printf 'A\\nB\\n' | okg expand --json -`: A's reply is cut, B's is
    whole. Each prints as its call answered, in input order; no field of
    one is read as the other's."""
    replies = {
        "node:A": {"status": "ok", "input": "node:A", "returned_count": 2, "truncated": True,
                   "next_offset": 2, "neighbors": [{"node_id": "n:1"}, {"node_id": "n:2"}]},
        "node:B": {"status": "ok", "input": "node:B", "returned_count": 1, "truncated": False,
                   "next_offset": None, "neighbors": [{"node_id": "n:3"}]},
    }
    calls = _serve(monkeypatch, lambda a, f: _Result(replies[a["input"]]))
    _stdin(monkeypatch, "node:A\nnode:B\n")
    assert main(["--json", "expand", "--deployment", "d", "-"]) == 0
    assert [a["input"] for a, _f in calls] == ["node:A", "node:B"]
    out, err = capsys.readouterr()
    assert json.loads(out) == [replies["node:A"], replies["node:B"]]
    assert "printed as a JSON array, one reply per call" in err


def test_json_over_stdin_says_which_reply_was_refused(monkeypatch, capsys):
    refusal = {"status": "refused", "error": {"code": "budget", "message": "busy"}}

    def batch(arguments, result_format):
        ids = arguments["target"]["node_ids"]
        if ids[0] == "doc:100":
            return _Result(refusal)
        return _Result({"status": "ok", "result_kind": "node_batch",
                        "records": [{"node_id": i, "status": "ok"} for i in ids]})

    _serve(monkeypatch, batch)
    _stdin(monkeypatch, "\n".join(f"doc:{i:03d}" for i in range(120)))
    assert main(["--json", "inspect", "--deployment", "d", "-"]) == 2
    out, err = capsys.readouterr()
    assert out == ""
    assert ("okg inspect: 100 rows from 2 replies were read and not printed, "
            "because reply 3 was refused") in err


def test_json_over_stdin_with_all_counts_pages_and_calls_apart(monkeypatch, capsys):
    """Under --all each call reads pages: the note names the page and the
    call that were refused."""
    refusal = {"status": "refused", "error": {"code": "budget", "message": "busy"}}

    def batch(arguments, result_format):
        ids = arguments["where"]["node_id"]
        offset = arguments.get("offset", 0)
        if ids[0] == "doc:0500" and offset:
            return _Result(refusal)
        page = {"status": "ok", "items": [{"node_id": i, "subtype": "doc"}
                                          for i in ids[offset:offset + 250]]}
        if offset == 0:
            page["next_offset"] = 250
        return _Result(page)

    _serve(monkeypatch, batch)
    _stdin(monkeypatch, "\n".join(f"doc:{i:04d}" for i in range(1200)))
    assert main(["--json", "filter", "--deployment", "d", "--stdin", "--all"]) == 2
    out, err = capsys.readouterr()
    assert out == ""
    assert ("okg filter: 750 rows from 3 pages of 2 calls were read and not printed, "
            "because page 2 of call 2 was refused") in err


def test_an_unscoped_search_on_the_tool_route_says_so_under_the_status_line(
    monkeypatch, capsys,
):
    """Issue 797 on the MCP-tool route: the text reply names no profile
    only among its other fields; the command says it under the status line,
    in the text form and on stderr under --all."""
    text = "search ok · attrs · 0 of 0 · complete\n--- other fields\nsearch_profile: null"
    _serve(monkeypatch, lambda a, f: _Result(text if f == "text" else {
        "status": "ok", "method": "attrs", "results": [], "total_count": 0,
        "search_profile": None,
    }))
    assert main(["search", "--deployment", "d", "--method", "attrs", "--where", "type=doc"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("search ok") and lines[1] == "! search profile: none (unscoped)"
    assert main(["search", "--deployment", "d", "--method", "attrs", "--where", "type=doc",
                 "--all"]) == 0
    err = capsys.readouterr().err.splitlines()
    assert err[0].startswith("search ok") and err[1] == "! search profile: none (unscoped)"


def test_json_over_batches_of_stdin_ids_keeps_each_batch_count(monkeypatch, capsys):
    """`filter --stdin --json` over 1,200 ids makes three calls; each reply's
    total_count is that batch's, printed as the batch's reply."""
    def batch(arguments, result_format):
        ids = arguments["where"]["node_id"]
        return _Result({"status": "ok", "total_count": len(ids),
                        "items": [{"node_id": i, "subtype": "doc"} for i in ids]})

    calls = _serve(monkeypatch, batch)
    _stdin(monkeypatch, "\n".join(f"doc:{i:04d}" for i in range(1200)))
    assert main(["--json", "filter", "--deployment", "d", "--stdin"]) == 0
    assert len(calls) == 3
    printed = json.loads(capsys.readouterr().out)
    assert isinstance(printed, list) and len(printed) == 3
    assert [reply["total_count"] for reply in printed] == [500, 500, 200]
    assert [row["node_id"] for reply in printed for row in reply["items"]] == [
        f"doc:{i:04d}" for i in range(1200)
    ]
    assert all("all_pages" not in reply for reply in printed)


def test_ids_print_one_id_per_line(monkeypatch, capsys):
    _serve(monkeypatch, lambda a, f: _Result(_page(0, 3, 3)))
    assert main(["filter", "--deployment", "d", "--input", '{"kind":"nodes"}', "--ids"]) == 0
    out, err = capsys.readouterr()
    assert out == "doc:000\ndoc:001\ndoc:002\n"
    assert err.startswith("filter ok")


def test_all_with_jsonl_at_the_page_cap_keeps_the_rows_and_says_how_to_go_on(
    monkeypatch, capsys,
):
    """`--all` alone prints JSON Lines (a text page has its status line on
    top), so a text reply never meets the cap; under --jsonl the rows read
    stay printed, exit 0, and stderr says how many and how to go on."""
    monkeypatch.setattr(shell, "MAX_ALL_PAGES", 2)
    _serve(monkeypatch, lambda a, f: _Result(_page(a.get("offset", 0), 2, 10)))
    for flags in (["--all"], ["--all", "--jsonl"]):
        rc = main(["filter", "--deployment", "d", "--input", '{"kind":"nodes"}',
                   "--limit", "2", *flags])
        assert rc == 0
        out, err = capsys.readouterr()
        assert [json.loads(line)["node_id"] for line in out.splitlines()] == [
            f"doc:{i:03d}" for i in range(4)
        ]
        assert "4 rows were printed and the set goes on" in err
        assert any(line.startswith("resume: okg filter ") for line in err.splitlines())
    assert shell.output_mode(_build_parser().parse_args(
        ["filter", "--deployment", "d", "--all"]), as_json=False) == ("jsonl", True)


@pytest.mark.parametrize("json_flag", [[], ["--json"]])
def test_the_resume_line_of_a_capped_search_runs_as_written(monkeypatch, capsys, json_flag):
    """An offered call runs as written: the `resume:` line of `okg search`
    on the tool route, parsed and run, reads the next page."""
    monkeypatch.setattr(shell, "MAX_ALL_PAGES", 1)

    def page(arguments, result_format):
        token = arguments.get("continuation_token")
        at = 0 if token is None else int(token.split(":")[1])
        return _Result({
            "status": "ok", "method": "grep", "generation_id": 7,
            "results": [{"node_id": f"doc:{at + i:03d}", "subtype": "doc"} for i in range(2)],
            "continuation_token": f"page:{at + 2}",
        })

    calls = _serve(monkeypatch, page)
    assert main([*json_flag, "search", "--deployment", "d", "--method", "grep", "word",
                 "--all"]) == 0
    (resume,) = [line for line in capsys.readouterr().err.splitlines()
                 if line.startswith("resume: ")]
    argv = shlex.split(resume[len("resume: "):])
    assert argv[0] == "okg"
    assert main(argv[1:]) == 0
    out = capsys.readouterr().out
    assert calls[-1][0]["continuation_token"] == "page:2"
    assert calls[-1][0]["query"] == "word" and calls[-1][0]["method"] == "grep"
    if json_flag:
        assert [r["node_id"] for r in json.loads(out)["results"]] == ["doc:002", "doc:003"]
    else:
        assert [json.loads(line)["node_id"] for line in out.splitlines()] == ["doc:002", "doc:003"]


def test_all_stops_at_the_page_cap_and_says_so(monkeypatch, capsys):
    monkeypatch.setattr(shell, "MAX_ALL_PAGES", 2)
    _serve(monkeypatch, lambda a, f: _Result(_page(a.get("offset", 0), 1, 10)))
    rc = main(["filter", "--deployment", "d", "--input", '{"kind":"nodes"}', "--all", "--ids"])
    # A labelled cut is an answer, not a refusal: exit 0.
    assert rc == 0
    out, err = capsys.readouterr()
    assert out == "doc:000\ndoc:001\n"
    assert "refused" not in err
    assert "stopped at the cap of 2 pages; 2 rows were printed and the set goes on" in err
    (resume,) = [line for line in err.splitlines() if line.startswith("resume: ")]
    argv = shlex.split(resume[len("resume: "):])
    assert argv[:2] == ["okg", "filter"] and "--ids" in argv and "--all" in argv
    assert json.loads(argv[argv.index("--offset") + 1]) == 2


def test_the_page_cap_counts_only_the_pages_all_follows(monkeypatch, capsys):
    """Batches of ids from standard input are calls of their own: three
    batches under a cap of two pages are all read."""
    monkeypatch.setattr(shell, "MAX_ALL_PAGES", 2)

    def batch(arguments, result_format):
        ids = arguments["target"]["node_ids"]
        return _Result({"status": "ok", "result_kind": "node_batch",
                        "records": [{"node_id": i, "status": "ok"} for i in ids]})

    calls = _serve(monkeypatch, batch)
    _stdin(monkeypatch, "\n".join(f"doc:{i:03d}" for i in range(120)))
    assert main(["inspect", "--deployment", "d", "-", "--ids", "--all"]) == 0
    assert len(calls) == 3
    assert len(capsys.readouterr().out.splitlines()) == 120


@pytest.mark.parametrize(
    ("rest", "hint"),
    [
        ([], "pass --limit up to 1000"),
        (["--limit", "10"], "raise --limit (now 10"),
        (["--limit", "1000"], "the pages cannot be larger"),
    ],
)
def test_the_page_cap_says_raise_limit_only_when_it_helps(monkeypatch, capsys, rest, hint):
    monkeypatch.setattr(shell, "MAX_ALL_PAGES", 1)
    _serve(monkeypatch, lambda a, f: _Result(_page(a.get("offset", 0), 1, 10)))
    rc = main(["filter", "--deployment", "d", "--input", '{"kind":"nodes"}', *rest, "--all", "--ids"])
    assert rc == 0
    (note,) = [line for line in capsys.readouterr().err.splitlines() if "stopped at the cap" in line]
    assert hint in note
    if hint.startswith("the pages"):
        assert "--limit" not in note


def test_a_refusal_in_a_pipe_mode_goes_to_stderr_with_the_retry_line(monkeypatch, capsys):
    retry = {"tool": "filter", "mode": "corrected",
             "arguments": {"input": {"kind": "nodes"}, "select": ["node_id"]}}
    refusal = {"status": "refused", "error": {
        "code": "select_unknown_field", "message": "no such field", "retry_call": retry,
    }}
    _serve(monkeypatch, lambda a, f: _Result(refusal, is_error=True))
    rc = main(["filter", "--deployment", "d", "--input", '{"kind":"nodes"}',
               "--select", "nodeid", "--jsonl"])
    assert rc == 2
    out, err = capsys.readouterr()
    assert out == ""
    lines = err.splitlines()
    # The row's id was added to the select for --jsonl, and said.
    assert lines[0].startswith('! adjusted: select ["nodeid"] read as ')
    assert lines[1].startswith("filter refused select_unknown_field")
    assert lines[-1] == (
        "retry: okg filter --deployment d --jsonl --input '{\"kind\": \"nodes\"}' "
        "--select '[\"node_id\"]'"
    )


def test_a_text_refusal_ends_with_the_retry_line(monkeypatch, capsys):
    retry = {"tool": "filter", "mode": "corrected", "arguments": {"input": {"kind": "nodes"}}}
    text = (
        "filter refused invalid_operator_arguments · retry: filter(input={...})\n"
        f"message: m\nretry_call: {json.dumps(retry)}"
    )
    _serve(monkeypatch, lambda a, f: _Result(text, is_error=True))
    rc = main(["filter", "--input", "{}", "--deployment", "d"])
    assert rc == 2
    out, err = capsys.readouterr()
    assert out == ""
    assert err == text + "\nretry: okg filter --deployment d --input '{\"kind\": \"nodes\"}'\n"


@pytest.mark.parametrize(
    ("reply", "code"),
    [
        (_Result({"status": "ok", "items": []}), 0),
        (_Result({"status": "ok", "items": [], "adjusted": [
            {"rule": "clause_without_op", "sent": "a", "served": "eq"}]}), 0),
        (_Result({"status": "refused", "error": {"code": "invalid_operator_arguments"}},
                 is_error=True), 2),
        (_Result({"status": "refused", "error": {"code": "operator_execution_failed"}},
                 is_error=True), 1),
    ],
)
def test_exit_codes_in_a_pipe_mode(monkeypatch, capsys, reply, code):
    _serve(monkeypatch, lambda a, f: reply)
    assert main(["filter", "--deployment", "d", "--input", '{"kind":"nodes"}', "--jsonl"]) == code


def test_a_closed_pipe_ends_the_command_quietly(monkeypatch, capsys):
    class _Closed(io.StringIO):
        def write(self, text: str) -> int:
            raise BrokenPipeError

    _serve(monkeypatch, lambda a, f: _Result(_page(0, 3, 3)))
    monkeypatch.setattr("sys.stdout", _Closed())
    rc = main(["filter", "--deployment", "d", "--input", '{"kind":"nodes"}', "--ids"])
    assert rc == shell.BROKEN_PIPE_EXIT


def test_a_bare_form_note_is_printed_and_the_call_is_served(monkeypatch, capsys):
    calls = _serve(monkeypatch, lambda a, f: _Result("inspect ok\n[1] doc:1 (doc)"))
    assert main(["inspect", "--deployment", "d", "--target", "doc:1"]) == 0
    out, err = capsys.readouterr()
    assert out == "inspect ok\n[1] doc:1 (doc)\n"
    assert err == '! adjusted: --target doc:1 read as target {"kind": "node", "node_id": "doc:1"}\n'
    assert calls == [({"target": {"kind": "node", "node_id": "doc:1"}}, "text")]


def test_a_shell_refusal_has_the_tools_json_shape(capsys):
    rc = main(["--json", "filter", "--deployment", "d", "--input", "{}", "--where", "a b"])
    assert rc == 2
    (line,) = capsys.readouterr().err.strip().splitlines()
    refusal = json.loads(line)
    assert refusal["status"] == "refused"
    assert refusal["error"]["code"] == "read_where_invalid"
    assert set(refusal["error"]) >= {"code", "message", "fix_hint", "data"}


# ── okg inspect texts - (#3666 step S5, PACT read-shell-texts-3666) ──────


def test_the_texts_batch_is_the_texts_targets_own_limit():
    from okg.substrate.mcp.operator_models import TTexts

    schema = TTexts.model_json_schema()
    assert shell.TEXTS_STDIN_BATCH == schema["properties"]["items"]["maxItems"] == 8


def test_inspect_texts_reads_the_ids_on_stdin(monkeypatch):
    for form in (["texts", "-"], ["texts", "--stdin"]):
        _stdin(monkeypatch, 'doc:1\n{"node_id": "doc:2"}\ndoc:1\n')
        plan = _plan(["inspect", "--deployment", "d", *form])
        assert plan.texts and plan.mode == "jsonl" and plan.shell
        assert plan.stdin_ids == ["doc:1", "doc:2"]
        assert plan.text_lines is None and plan.adjusted == []

    _stdin(monkeypatch, "doc:1\n")
    plan = _plan(["inspect", "--deployment", "d", "texts", "-", "--lines", "1-40,120-160,300-,7"])
    assert plan.text_lines == [[1, 40], [120, 160], [300, None], [7, 7]]

    # --json can only mean the records as JSON: served as the lines, said.
    _stdin(monkeypatch, "doc:1\n")
    plan = _plan(["inspect", "--deployment", "d", "texts", "-"], as_json=True)
    assert plan.mode == "jsonl"
    assert plan.adjusted == [{"sent": "--json", "served": "JSON Lines, one record per text"}]

    # Without - or --stdin, texts is a node id, as every word was before.
    plan = _plan(["inspect", "--deployment", "d", "texts"])
    assert not plan.texts
    assert plan.arguments["target"] == {"kind": "node", "node_id": "texts"}


@pytest.mark.parametrize(
    ("rest", "code"),
    [
        (["texts", "-", "--ids"], "read_output_flags_conflict"),
        (["texts", "-", "--select", "node_id"], "read_output_flags_conflict"),
        (["texts", "-", "--next", "tok"], "read_output_flags_conflict"),
        (["texts", "-", "--target", "doc:1"], "read_argument_given_twice"),
        (["texts", "-", "--lines", "5-x"], "read_lines_invalid"),
        (["texts", "-", "--lines", "0-3"], "read_lines_invalid"),
        (["texts", "-", "--lines", "1-3,"], "read_lines_invalid"),
        (["doc:1", "doc:2"], "read_argument_given_twice"),
        (["-", "--lines", "1-3"], "read_lines_invalid"),
    ],
)
def test_inspect_texts_refuses_what_it_cannot_serve(monkeypatch, rest, code):
    _stdin(monkeypatch, "doc:1\n")
    refused = _refused(["inspect", "--deployment", "d", *rest])
    assert refused.code == code, refused.message
    assert refused.retry is None


def test_lines_on_one_record_is_refused_with_its_text_read():
    refused = _refused(["inspect", "--deployment", "d", "doc:1", "--lines", "3-9,20-"])
    assert refused.code == "read_lines_invalid"
    assert refused.retry == ("inspect", {
        "target": {"kind": "text", "node_id": "doc:1", "lines": [[3, 9], [20, None]]},
    })


_FENCE_OPEN = "⟦UNTRUSTED_DATA source=connector:x⟧"
_FENCE_CLOSE = "⟦/UNTRUSTED_DATA⟧"


def _fenced(body: str) -> str:
    from okg.substrate.mcp.provenance import escape_embedded_fence_markers

    return f"{_FENCE_OPEN}\n{escape_embedded_fence_markers(body)}\n{_FENCE_CLOSE}"


class _TextsTool:
    """A stand-in `texts` batch target: each page serves the next `size`
    characters of each text still open, as one piece of its one chunk, and
    a `next` token while any text goes on. An id it does not hold is
    refused on the first page, as the tool refuses a missing record."""

    def __init__(self, texts: dict[str, str], *, size: int, untrusted: tuple[str, ...] = (),
                 endless: bool = False, refuse_call: int | None = None) -> None:
        self.texts, self.size, self.untrusted = texts, size, untrusted
        self.endless, self.refuse_call = endless, refuse_call
        self.tokens: dict[str, tuple[list[dict], dict[str, int]]] = {}
        self.calls = 0
        #: requested id -> the id the read serves (a text twin).
        self.aliases: dict[str, str] = {}

    def __call__(self, arguments: dict, result_format: str) -> _Result:
        self.calls += 1
        if self.calls == self.refuse_call:
            return _Result({"status": "refused", "error": {
                "code": "text_batch_budget_too_small", "message": "too small",
            }}, is_error=True)
        if "next" in arguments:
            items, positions = self.tokens[arguments["next"]]
        else:
            items = arguments["target"]["items"]
            positions = {item["key"]: 0 for item in items}
        entries, rest = [], {}
        for item in items:
            key, node_id = item["key"], self.aliases.get(item["node_id"], item["node_id"])
            if key not in positions:
                continue
            if node_id not in self.texts:
                entries.append({"key": key, "node_id": node_id, "state": "refused", "segments": [],
                                "error": {"code": "node_not_found",
                                          "message": "requested source text is unavailable"}})
                continue
            text, start = self.texts[node_id], positions[key]
            end = min(len(text), start + self.size)
            body = text[start:end]
            piece: dict[str, Any] = {"chunk_id": f"{node_id}#0"}
            if start or end < len(text):
                piece["text_range"] = [start, end]
            trust = "untrusted" if node_id in self.untrusted else "trusted"
            piece["text"] = _fenced(body) if trust == "untrusted" else body
            piece["provenance"] = {"trust": trust}
            done = end == len(text) and not self.endless
            if not done:
                rest[key] = end if end < len(text) else start
            entries.append({"key": key, "node_id": node_id, "subtype": "doc",
                            "source_coverage": "complete", "order_basis": "chunk_ordinal",
                            "state": "delivered" if done else "partial", "segments": [piece]})
        page: dict[str, Any] = {"status": "ok", "result_kind": "text_batch", "items": entries,
                                "generation_id": 7, "complete": not rest, "truncated": bool(rest)}
        if rest:
            token = f"t{len(self.tokens)}"
            self.tokens[token] = (items, rest)
            page["continuation_token"] = token
        return _Result(page)


def test_join_fenced_is_one_fence_with_a_split_marker_escaped():
    whole = f"a {_FENCE_CLOSE} b ⟦UNTRUSTED_DATA source=y⟧ c"
    for cut in range(1, len(whole)):
        joined = shell.join_fenced(_fenced(whole[:cut]), _fenced(whole[cut:]))
        assert joined == _fenced(whole), cut
    assert shell.join_fenced("plain ", "text") == "plain text"
    # Two fences of different sources stay two.
    other = "⟦UNTRUSTED_DATA source=connector:y⟧\nb\n" + _FENCE_CLOSE
    assert shell.join_fenced(_fenced("a"), other) == _fenced("a") + other


def test_inspect_texts_prints_each_whole_text_once_in_order(monkeypatch, capsys):
    texts = {f"doc:{i}": f"text {i}: " + "zirconium " * i for i in range(10)}
    texts["doc:fenced"] = f"before {_FENCE_CLOSE} after " * 3
    ids = [*texts, "doc:missing"]
    tool = _TextsTool(texts, size=7, untrusted=("doc:fenced",))
    calls = _serve(monkeypatch, tool)
    _stdin(monkeypatch, "\n".join(ids) + "\n")
    rc = main(["inspect", "--deployment", "d", "texts", "-", "--max-bytes", "4096",
               "--lines", "1-"])
    out, err = capsys.readouterr()
    # Every text printed, and exit 2: one id was refused, so the set is
    # not whole.
    assert rc == 2, err
    records = [json.loads(line) for line in out.splitlines()]
    assert [record["node_id"] for record in records] == ids
    for record in records[:-1]:
        assert record["state"] == "delivered" and "cut" not in record, record
        assert record["chunks"] == 1 and record["coverage"] == "complete"
        expected = texts[record["node_id"]]
        if record["node_id"] == "doc:fenced":
            assert record["provenance"] == [{"trust": "untrusted"}]
            expected = _fenced(expected)
        assert record["text"] == expected
    missing = records[-1]
    assert missing["state"] == "refused" and missing["text"] is None
    assert missing["error"]["code"] == "node_not_found"

    # 8 texts per call, one call at a time; `next` alone goes on; the
    # second batch reads the generation the first read.
    firsts = [arguments for arguments, _f in calls if "target" in arguments]
    assert [len(a["target"]["items"]) for a in firsts] == [8, 4]
    assert all(item["lines"] == [[1, None]] for a in firsts for item in a["target"]["items"])
    assert all(a["max_bytes"] == 4096 for a in firsts)
    assert "generation_id" not in firsts[0] and firsts[1]["generation_id"] == 7
    assert all(set(a) == {"next"} for a, _f in calls if "target" not in a)
    assert all(f == "json" for _a, f in calls)
    assert len(calls) > len(firsts)
    lines = err.splitlines()
    refused_note = (
        "! okg inspect texts: doc:missing refused node_not_found: "
        "requested source text is unavailable"
    )
    assert [line for line in lines if "doc:missing" in line] == [refused_note]
    assert lines[-1] == f"okg inspect texts: 12 texts in {len(calls)} calls; 1 refused"


def test_a_text_the_calls_cannot_finish_is_printed_as_cut(monkeypatch, capsys):
    monkeypatch.setattr(shell, "MAX_TEXT_PAGES", 2)
    _serve(monkeypatch, _TextsTool({"doc:1": "abcdefghij"}, size=3, endless=True))
    _stdin(monkeypatch, "doc:1\n")
    rc = main(["inspect", "--deployment", "d", "texts", "-"])
    out, err = capsys.readouterr()
    # A labelled cut is an answer: exit 0, the cut said in the record and on stderr.
    assert rc == 0
    (record,) = [json.loads(line) for line in out.splitlines()]
    assert record["state"] == "cut" and record["cut"] is True
    assert record["text"] == "abcdef"
    assert "at most 2 calls" in record["cut_reason"]
    assert "! okg inspect texts: doc:1 is cut, 6 characters printed" in err
    assert err.splitlines()[-1] == "okg inspect texts: 1 texts in 2 calls; 1 cut"


def test_a_refused_call_stops_the_texts_and_keeps_what_was_printed(monkeypatch, capsys):
    texts = {f"doc:{i}": f"text {i}" for i in range(10)}
    # Every text fits one page: call 1 is the first batch, call 2 the second.
    _serve(monkeypatch, _TextsTool(texts, size=100, refuse_call=2))
    _stdin(monkeypatch, "\n".join(texts) + "\n")
    rc = main(["inspect", "--deployment", "d", "texts", "-"])
    out, err = capsys.readouterr()
    assert rc == 2
    assert [json.loads(line)["node_id"] for line in out.splitlines()] == list(texts)[:8]
    assert "inspect refused text_batch_budget_too_small" in err
    assert err.splitlines()[-1] == (
        "okg inspect texts: stopped at text 9 of 10; 8 texts were printed before"
    )


def test_every_text_whole_exits_0_and_one_record_prints_once(monkeypatch, capsys):
    tool = _TextsTool({"doc:1": "one text", "doc:2": "two"}, size=100)
    # doc:1-twin is read as doc:1, as a record is read as its text twin.
    tool.aliases = {"doc:1-twin": "doc:1"}
    _serve(monkeypatch, tool)
    _stdin(monkeypatch, "doc:1\ndoc:1-twin\ndoc:2\n")
    rc = main(["inspect", "--deployment", "d", "texts", "-"])
    out, err = capsys.readouterr()
    assert rc == 0, err
    assert [json.loads(line)["node_id"] for line in out.splitlines()] == ["doc:1", "doc:2"]
    assert "! okg inspect texts: doc:1-twin reads doc:1, whose text is printed already" in err
    assert err.splitlines()[-1] == "okg inspect texts: 2 texts in 1 calls"


def test_too_many_ids_are_refused_before_any_call(monkeypatch, capsys):
    monkeypatch.setattr(shell, "MAX_TEXTS_STDIN_IDS", 3)
    calls = _serve(monkeypatch, _TextsTool({}, size=1))
    _stdin(monkeypatch, "a\nb\nc\nd\n")
    assert main(["inspect", "--deployment", "d", "texts", "-"]) == 2
    assert calls == []
    assert "refused read_stdin_invalid: okg inspect texts reads at most 3 ids" in (
        capsys.readouterr().err
    )


def test_texts_lines_without_the_dash_offers_the_texts_form(capsys):
    rc = main(["inspect", "--deployment", "d", "texts", "--lines", "1-2"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "refused read_lines_invalid" in err
    assert err.splitlines()[-1] == "retry: okg inspect --deployment d texts - --lines 1-2"
    assert "node_id" not in err


def test_no_ids_reads_nothing(monkeypatch, capsys):
    calls = _serve(monkeypatch, _TextsTool({}, size=1))
    _stdin(monkeypatch, "\n")
    assert main(["inspect", "--deployment", "d", "texts", "-"]) == 0
    out, err = capsys.readouterr()
    assert out == "" and calls == []
    assert err == "okg inspect texts: no ids on standard input; nothing was read\n"


def _help(command: str) -> str:
    import argparse

    parser = _build_parser()
    sub = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    return " ".join(sub.choices[command].format_help().split())


def test_the_help_shows_the_texts_pipe():
    for command in ("inspect", "filter", "guide"):
        assert ops.TEXTS_PIPE_TIP in _help(command), command
    text = _help("inspect")
    assert "okg filter --type doc --where 'attrs.path~docs/' --ids --all" in text
    assert "| okg inspect texts - \\ | python3 -c 'import json, sys;" in text
    # The script names the texts it could not count, not a partial count.
    assert 'r.get("cut") or r["state"] == "refused"' in text
    assert "exit 2 when a text is cut or refused" in text
    assert "--lines RANGES" in text


def test_okg_guide_leads_with_the_pipeline_and_then_prints_the_served_text(
        monkeypatch, capsys):
    """`okg guide` prints the pipeline lead, a rule, then the served text
    byte for byte; --json gives the served text alone (its hash is the
    session_start instructions_hash)."""

    class _Server:
        instructions = "The graph holds tickets."

    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    monkeypatch.setattr(ops, "build_read_server", lambda args, **kwargs: _Server())
    monkeypatch.setattr(ops, "close_server", lambda server: None)
    assert main(["guide", "--deployment", "d"]) == 0
    out = capsys.readouterr().out
    assert out.startswith(ops.PIPELINE_LEAD + "\n")
    lead, served = out.split(ops.GUIDE_INSTRUCTIONS_RULE + "\n")
    assert lead == ops.pipeline_text() + "\n\n"
    assert served == "The graph holds tickets.\n"
    assert main(["--json", "guide", "--deployment", "d"]) == 0
    assert json.loads(capsys.readouterr().out)["text"] == "The graph holds tickets."


# ── the pipeline lead and the pipe hint (#3666) ──────────────────────────


@pytest.mark.parametrize("command", ["search", *ops.OPERATOR_COMMANDS, "guide"])
def test_every_read_commands_help_opens_with_the_pipeline_lead(command):
    import argparse

    parser = _build_parser()
    sub = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    text = sub.choices[command].format_help()
    # The usage, then the lead and its examples before anything else.
    after_usage = text.split("\n\n", 1)[1]
    assert after_usage.startswith(ops.pipeline_text()), (command, after_usage[:300])


def test_each_pipeline_example_prints_as_one_runnable_command():
    """Each example is printed with one pipe stage per line; joined back at
    the line breaks it is the command the database test runs."""
    printed = ops.pipeline_text()
    assert len(ops.PIPELINE_EXAMPLES) == 3
    for question, command in ops.PIPELINE_EXAMPLES:
        block = printed.split(f"  # {question}\n", 1)[1].split("\n  # ", 1)[0]
        assert block.replace(" \\\n    | ", " | ").strip() == command
        # Each stage parses as a shell command line.
        for stage in command.split(" | "):
            assert shlex.split(stage)
        assert "--jsonl" in command or "--ids" in command


def _text_page(total: int, size: int) -> str:
    from okg.substrate.mcp.result_text import render

    page = _page(0, size, total)
    page["total_count"] = total
    return render(page, tool="filter", kind=None, select=None)


@pytest.mark.parametrize(
    ("total", "size", "hinted"),
    [(30, 25, True), (21, 21, True), (20, 20, False), (5, 5, False), (300, 10, True)],
)
def test_a_text_reply_past_twenty_rows_ends_with_a_pipe_hint(
        monkeypatch, capsys, total, size, hinted):
    text = _text_page(total, size)
    _serve(monkeypatch, lambda a, f: _Result(text))
    argv = ["filter", "--deployment", "d", "--input", '{"kind":"nodes"}', "--offset", "0"]
    assert main(argv) == 0
    out, err = capsys.readouterr()
    # The reply itself is unchanged.
    assert out == text + "\n"
    if not hinted:
        assert err == ""
        return
    shown = f"{size} of {total} rows shown" if total > size else f"{size} rows"
    command = "okg filter --deployment d --input '{\"kind\":\"nodes\"}' --all --jsonl"
    assert err == (
        f"hint: {shown}; to count, join or list them, pipe them instead of reading "
        f"them: {command} | {shell.PIPE_HINT_PROGRAM}\n"
    )
    # The hinted command parses back to the call, every page, as JSON Lines.
    hinted_argv = shlex.split(err.split("them: ", 1)[1].split(" | ")[0])[1:]
    plan = _plan(hinted_argv)
    assert (plan.mode, plan.all_pages) == ("jsonl", True)
    assert plan.arguments == {"input": {"kind": "nodes"}}


def test_the_hint_keeps_where_terms_and_type_as_typed(monkeypatch, capsys):
    text = _text_page(40, 40)
    _serve(monkeypatch, lambda a, f: _Result(text))
    monkeypatch.setattr(shell, "resolve_subtypes", lambda plan, server: None)
    assert main([
        "filter", "--deployment", "d", "--type", "doc", "--where", "attrs.status!=Closed",
    ]) == 0
    err = capsys.readouterr().err
    assert err.splitlines()[-1].endswith(
        "them: okg filter --deployment d --where 'attrs.status!=Closed' --type doc "
        f"--all --jsonl | {shell.PIPE_HINT_PROGRAM}"
    ), err


@pytest.mark.parametrize(("argv", "count"), [
    (["filter", "--deployment", "d", "--stdin"], 600),
    (["expand", "--deployment", "d", "-"], 30),
])
def test_no_hint_when_the_ids_came_from_standard_input(monkeypatch, capsys, argv, count):
    """Run as printed, such a hint would have nothing piped in; and the ids
    make several calls (filter 500 a call, expand one per id), which must
    not print a line each."""
    _stdin(monkeypatch, "\n".join(f"doc:{i:03d}" for i in range(count)) + "\n")
    text = _text_page(40, 40)
    calls = _serve(monkeypatch, lambda a, f: _Result(text))
    monkeypatch.setattr(shell, "resolve_subtypes", lambda plan, server: None)
    assert main(argv) == 0
    err = capsys.readouterr().err
    assert len(calls) >= 2, calls
    assert "hint:" not in err, err


def test_the_hint_drops_a_typed_next_token_and_offset(monkeypatch, capsys):
    text = _text_page(40, 40)
    _serve(monkeypatch, lambda a, f: _Result(text))
    assert main(["expand", "--deployment", "d", "doc:001", "--next", "T", "--offset", "50"]) == 0
    hint = capsys.readouterr().err.splitlines()[-1]
    command = hint.split("them: ", 1)[1].split(" | ")[0]
    assert command == "okg expand --deployment d doc:001 --all --jsonl", hint


def test_the_hint_keeps_a_lower_bound_count_and_quotes_user_values(monkeypatch, capsys):
    args = _build_parser().parse_args(["filter", "--deployment", "d"])
    hint = shell.pipe_hint("filter", "filter ok · 1-50 of 312+", args)
    assert hint is not None and hint.startswith("hint: 50 of 312+ rows shown;"), hint
    # A bounded page that does not count its set (#3716's status form).
    hint = shell.pipe_hint("filter", "filter ok · 1-25, more follow", args)
    assert hint is not None and hint.startswith("hint: 25 rows shown, more follow;"), hint
    assert shell.pipe_hint("filter", "filter ok · 1-20, more follow", args) is None
    text = _text_page(40, 40)
    _serve(monkeypatch, lambda a, f: _Result(text))
    monkeypatch.setattr(shell, "resolve_subtypes", lambda plan, server: None)
    # A quote mark inside a --where value is refused; $() and & are not.
    term = "attrs.title=$(touch x) & y"
    assert main(["filter", "--deployment", "d", "--where", term]) == 0
    hint = capsys.readouterr().err.splitlines()[-1]
    assert "--where 'attrs.title=$(touch x) & y' --all --jsonl" in hint, hint


def test_no_hint_on_a_search_page_json_a_pipe_mode_or_a_refusal(monkeypatch, capsys):
    # Search's page of 10 of a large set is read for its rank.
    assert shell.pipe_hint("search", "search ok · 1-10 of 500", None) is None
    text = _text_page(30, 30)
    _serve(monkeypatch, lambda a, f: _Result(_page(0, 30, 30)))
    argv = ["filter", "--deployment", "d", "--input", '{"kind":"nodes"}']
    for run in (["--json", *argv], [*argv, "--jsonl"], [*argv, "--ids"]):
        assert main(run) == 0
        assert "hint:" not in capsys.readouterr().err, run
    refused = text.replace("filter ok", "filter refused invalid_operator_arguments", 1)
    _serve(monkeypatch, lambda a, f: _Result(refused, is_error=True))
    assert main(argv) == 2
    assert "hint:" not in capsys.readouterr().err
