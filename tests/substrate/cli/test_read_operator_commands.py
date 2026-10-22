"""The read-operator commands, without a database (PACT read-commands-3666).

`okg inspect`, `okg expand`, `okg filter`, `okg aggregate`, `okg map`,
`okg query` and the operator path of `okg search` turn flags into the
argument object of the MCP tool of the same name. These tests cover that
turn, the table the flags are built from, and which path `okg search` takes.
What the tool then answers is covered against a real publish in
`test_read_operator_commands_db.py`.
"""
from __future__ import annotations

import io
import json
import typing

import pytest

from okg.substrate.cli import _read_operators as ops
from okg.substrate.cli import _read_side
from okg.substrate.cli.main import _build_parser, main
from okg.substrate.errors import SubstrateError

OPERATORS = ("search", "inspect", "expand", "filter", "aggregate", "map", "query")


def _parse(argv: list[str]):
    return _build_parser().parse_args(argv)


def _arguments(argv: list[str]) -> dict:
    args = _parse(argv)
    if args.command == "search":
        return ops.search_operator_arguments(args)
    return ops.operator_arguments(args.command, args)


def _subparser(name: str):
    import argparse

    parser = _build_parser()
    sub = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    return sub.choices[name]


# ── the table ────────────────────────────────────────────────────────────


def test_the_argument_table_equals_the_registered_mcp_tools(monkeypatch):
    """A tool that gains, loses or renames an argument fails here."""
    from okg.substrate.mcp.operators import COMPOSITION_TOOLS, OPERATOR_TOOLS
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    server = build_server(dsn=PROBE_DSN, deployment_name="default")
    try:
        registered = {
            name: {
                info.alias or field
                for field, info in tool.fn_metadata.arg_model.model_fields.items()
            }
            for name, tool in server._tool_manager._tools.items()
            if name in OPERATOR_TOOLS and name not in COMPOSITION_TOOLS
        }
    finally:
        server._okg_pool.close()
        if server._okg_write_pool is not None:
            server._okg_write_pool.close()

    # `run` (read-sets-run-3666) has no command yet; the shell composes
    # with pipes.
    assert set(ops.OPERATOR_ARGUMENTS) == set(OPERATORS) == (
        set(OPERATOR_TOOLS) - set(COMPOSITION_TOOLS)
    )
    assert set(registered) == set(OPERATORS)
    for operator in OPERATORS:
        table = ops.OPERATOR_ARGUMENTS[operator]
        assert len(table) == len(set(table)), operator
        # An argument whose flag would be the command's own `--args`
        # reaches the tool inside `--args` (expand's live-edge `args`).
        only_in_args = set(ops.ARGS_OBJECT_ONLY.get(operator, ()))
        assert not only_in_args & set(table), operator
        assert set(table) | only_in_args == registered[operator], operator


def test_the_search_methods_equal_the_mcp_tool_methods():
    from okg.substrate.mcp.operator_models import SearchMethod

    assert set(ops.SEARCH_METHODS) == set(typing.get_args(SearchMethod))
    assert ops.LEGACY_SEARCH_METHODS < set(ops.SEARCH_METHODS)


@pytest.mark.parametrize("operator", ops.OPERATOR_COMMANDS)
def test_every_tool_argument_is_a_flag_of_the_same_name(operator):
    flags = {
        flag for action in _subparser(operator)._actions for flag in action.option_strings
    }
    for argument in ops.OPERATOR_ARGUMENTS[operator]:
        assert "--" + argument.replace("_", "-") in flags, argument
    assert {"--args", "--json", "--deployment", "--dsn"} <= flags


def test_every_search_argument_has_exactly_one_way_in():
    """Each MCP `search` argument is a flag the command had, a new flag, or
    `--args` only; none is unreachable and none has two flags."""
    shared = set(ops.SEARCH_SHARED_FLAGS)
    new = set(ops.SEARCH_OPERATOR_FLAGS)
    args_only = set(ops.SEARCH_ARGS_ONLY)
    by_hand = {"query", "allow_unverified_embedding_provenance"}
    groups = [shared, new, args_only, by_hand]
    assert sum(len(g) for g in groups) == len(set().union(*groups))
    assert set().union(*groups) == set(ops.OPERATOR_ARGUMENTS["search"])
    flags = {
        flag for action in _subparser("search")._actions for flag in action.option_strings
    }
    for argument in new:
        assert "--" + argument.replace("_", "-") in flags, argument
    assert "--args" in flags


# ── flag values ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("argument", "raw", "expected"),
    [
        ("predicate", '{"op":"eq","field":"type","value":"doc"}',
         {"op": "eq", "field": "type", "value": "doc"}),
        ("limit", "10", 10),
        ("retain", "true", True),
        ("order_by", "date.created desc", "date.created desc"),
        ("edge_type", "contains", "contains"),
        ("select", "node_id, attrs.summary", ["node_id", "attrs.summary"]),
        ("select", '["node_id"]', ["node_id"]),
        ("by", "subtype", ["subtype"]),
        ("metrics", "count", ["count"]),
        ("projection", '{"id": "node_id"}', {"id": "node_id"}),
        ("input", '"123"', "123"),
        ("edge_type", "NaN", "NaN"),
        ("input", "doc:000", "doc:000"),
    ],
)
def test_a_flag_value_is_json_when_it_parses_and_a_string_otherwise(argument, raw, expected):
    assert ops.parse_flag_value(argument, raw) == expected


def test_filter_flags_become_the_argument_object():
    assert _arguments([
        "filter", "--deployment", "d",
        "--input", '{"subtype": "doc"}',
        "--predicate", '{"op":"exists","field":"attrs.path"}',
        "--select", "node_id,attrs.path", "--limit", "5", "--max-bytes", "8192",
        "--generation-id", "42", "--order-by", "node_id",
    ]) == {
        "input": {"subtype": "doc"},
        "predicate": {"op": "exists", "field": "attrs.path"},
        "select": ["node_id", "attrs.path"], "limit": 5, "max_bytes": 8192,
        "generation_id": 42, "order_by": "node_id",
    }


def test_an_omitted_flag_is_not_sent():
    assert _arguments(["inspect", "--deployment", "d", "--target", '{"kind":"session"}']) == {
        "target": {"kind": "session"},
    }


def test_positional_shorthands_are_the_text_typed():
    assert _arguments(["expand", "--deployment", "d", "123", "--edge-type", "contains"]) == {
        "input": "123", "edge_type": "contains",
    }
    assert _arguments(["query", "--deployment", "d", "select 1"]) == {"sql": "select 1"}


def test_a_positional_and_its_flag_together_are_refused():
    with pytest.raises(SubstrateError) as refused:
        _arguments(["expand", "--deployment", "d", "doc:1", "--input", "doc:2"])
    assert refused.value.code == "read_argument_given_twice"
    assert refused.value.data["arguments"] == ["input"]


# ── --args ───────────────────────────────────────────────────────────────


def test_args_takes_json_a_file_and_standard_input(tmp_path, monkeypatch):
    whole = {"input": {"subtype": "doc"}, "by": ["subtype"], "metrics": ["count"]}
    inline = _arguments(["aggregate", "--deployment", "d", "--args", json.dumps(whole)])
    path = tmp_path / "args.json"
    path.write_text(json.dumps(whole), encoding="utf-8")
    from_file = _arguments(["aggregate", "--deployment", "d", "--args", f"@{path}"])
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(whole)))
    from_stdin = _arguments(["aggregate", "--deployment", "d", "--args", "-"])
    assert inline == from_file == from_stdin == whole


def test_a_live_edge_s_args_reach_expand_inside_args_and_new_flags_parse():
    """expand's `args` has no flag (it would be the command's `--args`);
    filter takes `--predicate` and `--order-by`, aggregate `--predicate` (#3666)."""
    assert _arguments([
        "expand", "--deployment", "d", "site:Z2_XX_A", "--edge-type", "distance",
        "--args", '{"args": {"to": "site:Z2_XX_B"}}',
    ]) == {"input": "site:Z2_XX_A", "edge_type": "distance", "args": {"to": "site:Z2_XX_B"}}
    predicate = {"op": "eq", "field": "attrs.tier", "value": "1"}
    assert _arguments([
        "filter", "--deployment", "d", "--input", '{"ref": "l1.x"}',
        "--predicate", json.dumps(predicate), "--order-by=-attrs.size",
    ]) == {"input": {"ref": "l1.x"}, "predicate": predicate, "order_by": "-attrs.size"}
    assert _arguments([
        "aggregate", "--deployment", "d", "--predicate", json.dumps(predicate),
    ]) == {"predicate": predicate}


def test_args_and_flags_combine_when_they_name_different_arguments():
    assert _arguments([
        "map", "--deployment", "d", "--args", '{"input": {"subtype": "doc"}}',
        "--projection", "node_id",
    ]) == {"input": {"subtype": "doc"}, "projection": ["node_id"]}


def test_an_argument_in_args_and_as_a_flag_is_refused_by_name():
    with pytest.raises(SubstrateError) as refused:
        _arguments([
            "filter", "--deployment", "d", "--args", '{"limit": 5, "offset": 0}',
            "--limit", "10",
        ])
    assert refused.value.code == "read_argument_given_twice"
    assert refused.value.data["arguments"] == ["limit"]
    assert "limit" in refused.value.message


@pytest.mark.parametrize("spec", ["[1, 2]", "not json", '"text"'])
def test_args_that_is_not_one_json_object_is_refused(spec):
    with pytest.raises(SubstrateError) as refused:
        _arguments(["inspect", "--deployment", "d", "--args", spec])
    assert refused.value.code == "read_args_invalid"


def test_an_unreadable_args_file_is_refused(tmp_path):
    with pytest.raises(SubstrateError) as refused:
        _arguments(["inspect", "--deployment", "d", "--args", f"@{tmp_path / 'absent.json'}"])
    assert refused.value.code == "read_args_unreadable"


def test_a_bad_args_value_is_refused_with_exit_two(capsys):
    """Before any database work: a refusal in a tool refusal's shape, exit 2
    (PACT read-shell-forms-3666: 2 is refused, 1 is an error)."""
    rc = main(["--json", "filter", "--deployment", "d", "--args", "[]"])
    assert rc == 2
    refusal = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert refusal["status"] == "refused"
    assert refusal["error"]["code"] == "read_args_invalid"
    rc = main(["filter", "--deployment", "d", "--input", "{}", "--args", '{"input": {}}'])
    assert rc == 2
    assert capsys.readouterr().err.startswith("filter refused read_argument_given_twice: ")


# ── okg search: which path ───────────────────────────────────────────────

LEGACY = [
    ["search", "--deployment", "d", "words"],
    ["search", "--deployment", "d", "--query", "words", "--method", "alias"],
    [
        "search", "--deployment", "d", "words", "--method", "lexical", "--limit", "5",
        "--offset", "5", "--generation-id", "7", "--select", "node_id,excerpt",
        "--subtype", "doc", "--scope", "analyst", "--source-family", "docs",
        "--max-per-parent", "2", "--no-concept-expansion", "--completeness", "exact",
        "--max-chars", "128", "--rank-by", "self", "--attr-filter", "status=open",
        "--allow-unverified-embedding-provenance",
        "--nomos-access-context-json", "{}",
    ],
    ["search", "--deployment", "d", "words", "--method", "semantic", "--embedder", "hash"],
]

OPERATOR = [
    ["search", "--deployment", "d", "words", "--where", '{"type": "doc"}'],
    ["search", "--deployment", "d", "--args", '{"query": "words"}'],
    ["search", "--deployment", "d", "words", "--view", "list"],
    ["search", "--deployment", "d", "words", "--max-bytes", "8192"],
    ["search", "--deployment", "d", "CMS-[0-9]+", "--method", "grep"],
    ["search", "--deployment", "d", "--method", "attrs", "--attr-filters", '{"status": "open"}'],
    ["search", "--deployment", "d", "words", "--subtype-filter", "doc"],
    ["search", "--deployment", "d", "words", "--retain", "true"],
]


@pytest.mark.parametrize("argv", LEGACY)
def test_todays_search_flags_stay_on_todays_path(argv):
    assert ops.search_takes_operator_path(_parse(argv)) is False


@pytest.mark.parametrize("argv", OPERATOR)
def test_an_operator_only_flag_takes_the_operator_path(argv):
    assert ops.search_takes_operator_path(_parse(argv)) is True


def test_todays_search_defaults_are_unchanged():
    args = _parse(["search", "--deployment", "d", "words"])
    assert (args.method, args.limit, args.offset) == ("lexical", 50, 0)


def _reached(monkeypatch) -> list[str]:
    """Record which path `cmd_search` took, stopping each before a database."""
    reached: list[str] = []

    def operator_path(args, *, as_json):
        reached.append("operator")
        return 0

    def legacy_dsn(args, *, surface):
        reached.append("legacy")
        raise SubstrateError("stopped before the database")

    monkeypatch.setattr(ops, "cmd_search_operator", operator_path)
    monkeypatch.setattr(_read_side, "resolve_read_dsn", legacy_dsn)
    return reached


@pytest.mark.parametrize("argv", LEGACY[:2])
def test_cmd_search_runs_todays_search_for_todays_flags(monkeypatch, capsys, argv):
    reached = _reached(monkeypatch)
    assert main(argv) == 1
    assert reached == ["legacy"]
    assert "stopped before the database" in capsys.readouterr().err


@pytest.mark.parametrize("argv", OPERATOR[:2])
def test_cmd_search_hands_operator_flags_to_the_mcp_tool(monkeypatch, argv):
    reached = _reached(monkeypatch)
    assert main(argv) == 0
    assert reached == ["operator"]


# ── okg search: the operator path's arguments ────────────────────────────


def test_search_sends_only_what_was_typed():
    """`--limit`, `--offset` and `--method` have defaults on today's path;
    the MCP tool must not receive a default nobody typed."""
    assert _arguments(["search", "--deployment", "d", "words", "--where", '{"type": "doc"}']) == {
        "query": "words", "where": {"type": "doc"},
    }


def test_search_maps_todays_flags_onto_the_tool_arguments():
    assert _arguments([
        "search", "--deployment", "d", "words", "--view", "list",
        "--method", "lexical", "--limit", "50", "--offset", "0",
        "--generation-id", "7", "--select", "node_id", "--select", "excerpt,score",
        "--subtype", "doc", "--scope", "analyst", "--source-family", "docs",
        "--max-per-parent", "2", "--no-concept-expansion", "--completeness", "exact",
        "--max-chars", "128", "--rank-by", "self", "--attr-filter", "status=open",
        "--allow-unverified-embedding-provenance",
    ]) == {
        "query": "words", "view": "list", "method": "lexical", "limit": 50, "offset": 0,
        "generation_id": 7, "select": ["node_id", "excerpt", "score"],
        "subtype_filter": "doc", "profile": "analyst", "source_family": ["docs"],
        "max_per_parent": 2, "expand_concepts": False, "completeness": "exact",
        "max_chars": 128, "rank_by": "self", "attr_filters": {"status": "open"},
        "allow_unverified_embedding_provenance": True,
    }


def test_search_takes_the_mcp_scope_argument_through_args():
    assert _arguments([
        "search", "--deployment", "d", "words", "--args", '{"scope": {"subtype": "doc"}}',
    ]) == {"query": "words", "scope": {"subtype": "doc"}}


@pytest.mark.parametrize(
    ("extra", "flags"),
    [
        (["--subtype", "doc", "--subtype-filter", "doc"], ["--subtype", "--subtype-filter"]),
        (["--attr-filter", "a=1", "--attr-filters", '{"a": 1}'],
         ["--attr-filter", "--attr-filters"]),
        (["--no-concept-expansion", "--expand-concepts", "true"],
         ["--no-concept-expansion", "--expand-concepts"]),
    ],
)
def test_search_refuses_two_flags_for_one_argument(extra, flags):
    with pytest.raises(SubstrateError) as refused:
        _arguments(["search", "--deployment", "d", "words", *extra])
    assert refused.value.code == "search_flags_conflict"
    assert refused.value.data["flags"] == flags
    assert flags[1] in refused.value.fix_hint


def test_search_refuses_the_access_context_flag_on_the_operator_path():
    with pytest.raises(SubstrateError) as refused:
        _arguments([
            "search", "--deployment", "d", "words", "--where", '{"type": "doc"}',
            "--nomos-access-context-json", "{}",
        ])
    assert refused.value.code == "search_flags_conflict"
    assert refused.value.data["flags"] == ["--nomos-access-context-json", "--where"]
    assert "--authority" in refused.value.fix_hint


def test_search_refuses_the_access_context_variable_on_the_operator_path(monkeypatch):
    """The command's own search reads it; the MCP tool never would."""
    monkeypatch.setenv("OKG_NOMOS_ACCESS_CONTEXT", "{}")
    with pytest.raises(SubstrateError) as refused:
        _arguments(["search", "--deployment", "d", "words", "--view", "list"])
    assert refused.value.code == "search_flags_conflict"
    assert refused.value.data["flags"] == ["OKG_NOMOS_ACCESS_CONTEXT", "--view"]


def test_search_refuses_two_different_queries_on_the_operator_path():
    with pytest.raises(SubstrateError) as refused:
        _arguments(["search", "--deployment", "d", "one", "--query", "two", "--view", "list"])
    assert refused.value.code == "search_query_conflict"


def test_search_refuses_a_query_in_args_and_on_the_command_line():
    with pytest.raises(SubstrateError) as refused:
        _arguments(["search", "--deployment", "d", "words", "--args", '{"query": "other"}'])
    assert refused.value.code == "read_argument_given_twice"
    assert refused.value.data["arguments"] == ["query"]


# ── the reply ────────────────────────────────────────────────────────────


class _Block:
    type = "text"

    def __init__(self, text: str) -> None:
        self.text = text


class _Result:
    def __init__(self, text: str, *, is_error: bool = False) -> None:
        self.content = [_Block(text)]
        self.isError = is_error


def test_a_served_reply_prints_uncut_on_stdout_and_exits_zero(capsys):
    text = "served 1 of 1\n[1] doc:000 (doc)" + " x" * 50_000
    assert ops.print_reply(_Result(text), as_json=False) == 0
    out = capsys.readouterr()
    assert out.out == text + "\n" and out.err == ""


def test_a_refused_reply_prints_on_stderr_and_exits_two(capsys):
    text = json.dumps({"status": "refused", "error": {"code": "invalid_operator_arguments"}})
    assert ops.print_reply(_Result(text, is_error=True), as_json=True) == 2
    out = capsys.readouterr()
    assert out.err == text + "\n" and out.out == ""


@pytest.mark.parametrize("as_json", [True, False])
def test_a_server_fault_exits_one_not_two(capsys, as_json):
    payload = {"status": "refused", "error": {"code": "operator_execution_failed"}}
    text = (
        json.dumps(payload) if as_json
        else "filter refused operator_execution_failed\nretry: none given"
    )
    assert ops.print_reply(_Result(text, is_error=True), as_json=as_json) == 1
    out = capsys.readouterr()
    assert out.err == text + "\n" and out.out == ""


@pytest.mark.parametrize("as_json", [True, False])
def test_the_fault_code_elsewhere_in_a_refusal_is_not_a_fault(capsys, as_json):
    """Only the reply's own error code decides: a refusal that quotes the
    fault code in its message, or in a row, is still a refusal (PACT
    read-commands-shell-3666)."""
    payload = {
        "status": "refused",
        "error": {
            "code": "invalid_operator_arguments",
            "message": "unknown value operator_execution_failed",
        },
    }
    text = (
        json.dumps(payload) if as_json
        else "filter refused invalid_operator_arguments\n"
        "retry: drop operator_execution_failed"
    )
    assert ops.reply_error_code(text, as_json=as_json) == "invalid_operator_arguments"
    assert ops.print_reply(_Result(text, is_error=True), as_json=as_json) == 2
    assert capsys.readouterr().err == text + "\n"


def test_a_json_reply_that_says_refused_exits_two_without_the_error_flag(capsys):
    text = json.dumps({"status": "refused", "error": {"code": "x"}})
    assert ops.print_reply(_Result(text), as_json=True) == 2
    assert capsys.readouterr().err == text + "\n"


# ── one command in a shell (PACT read-commands-shell-3666) ───────────────

#: A file of a deployment, as a start-up warning names one.
HOST_PATH = "/srv/deployments/example/sources.yaml"


@pytest.fixture
def bare_checkout(tmp_path, monkeypatch):
    """A working directory with no deployment named anywhere."""
    for name in ("OKG_DEPLOYMENT", "OKG_PACT_GRAPH_DEPLOYMENT", "OKG_AGENT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_the_deployment_is_the_flag_then_the_environment_then_the_configured_default(
        bare_checkout, monkeypatch):
    def named(argv):
        args = _parse(argv)
        assert ops.require_deployment(args, argv[0], as_json=False) is None
        return args.deployment

    config = bare_checkout / ".okg" / "coordination.yaml"
    config.parent.mkdir()
    config.write_text("graph_projection:\n  deployment: configured\n", encoding="utf-8")
    assert named(["filter", "--input", "{}"]) == "configured"
    monkeypatch.setenv("OKG_DEPLOYMENT", "from-env")
    assert named(["filter", "--input", "{}"]) == "from-env"
    assert named(["search", "words"]) == "from-env"
    assert named(["filter", "--deployment", "typed", "--input", "{}"]) == "typed"


@pytest.mark.parametrize(
    "argv",
    [
        ["filter", "--input", "{}"],
        ["inspect", "--target", '{"kind": "session"}'],
        ["search", "words"],
        ["search", "words", "--view", "list"],
    ],
)
def test_no_deployment_anywhere_is_the_named_error(bare_checkout, capsys, argv):
    assert main(argv) == 2
    out = capsys.readouterr()
    assert out.out == ""
    assert out.err.startswith(
        f"{argv[0]} refused deployment_not_specified: okg {argv[0]} needs a deployment"
    )
    for way in ("--deployment", "OKG_DEPLOYMENT", "graph_projection.deployment"):
        assert way in out.err, way

    assert main(["--json", *argv]) == 2
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["code"] == "deployment_not_specified"
    assert error["data"] == {"command": f"okg {argv[0]}"}


def test_a_command_runs_against_the_deployment_the_environment_names(monkeypatch):
    import inspect

    seen: list[tuple] = []

    def run(
        operator, arguments, args, *, as_json, embedder_spec=None, refusal_hint=None,
        driver=None,
    ):
        assert driver is None  # no shell form here: the plain path
        seen.append((operator, arguments, args.deployment, refusal_hint))
        return 0

    # The stand-in takes what the function it stands in for takes.
    assert list(inspect.signature(run).parameters) == list(
        inspect.signature(ops.run_operator).parameters
    )
    monkeypatch.setattr(ops, "run_operator", run)
    monkeypatch.setenv("OKG_DEPLOYMENT", "from-env")
    assert main(["query", "select 1"]) == 0
    assert main(["query", "--deployment", "typed", "select 1"]) == 0
    # Not SQL: the call still reaches the tool, which is what refuses it;
    # the command has the line about `okg search` ready for that refusal.
    assert main(["query", "recent pacts"]) == 0
    assert main(["search", "words", "--view", "list"]) == 0
    assert seen == [
        ("query", {"sql": "select 1"}, "from-env", None),
        ("query", {"sql": "select 1"}, "typed", None),
        ("query", {"sql": "recent pacts"}, "from-env", ops.TEXT_SEARCH_HINT),
        ("search", {"query": "words", "view": "list"}, "from-env", None),
    ]


def test_the_help_says_where_the_deployment_comes_from():
    for operator in OPERATORS:
        text = _subparser(operator).format_help()
        assert "OKG_DEPLOYMENT" in text, operator


@pytest.mark.parametrize("operator", OPERATORS)
def test_an_unknown_flag_prints_the_commands_own_usage(operator, capsys):
    with pytest.raises(SystemExit) as stopped:
        main([operator, "--deployment", "d", "--no-such-flag"])
    assert stopped.value.code == 2
    out = capsys.readouterr()
    assert out.out == ""
    assert out.err.startswith(f"usage: okg {operator} ")
    assert out.err.rstrip().endswith(
        f"okg {operator}: error: unrecognized arguments: --no-such-flag"
    )
    # Not the top-level parser's list of every okg command.
    for other in ("migrate", "mcp-serve", "ingest"):
        assert other not in out.err, other


def test_an_unknown_flag_names_the_flag_it_is_near(capsys):
    with pytest.raises(SystemExit) as stopped:
        main(["filter", "--deployment", "d", "--inputs", "x"])
    assert stopped.value.code == 2
    assert capsys.readouterr().err.rstrip().endswith(
        "okg filter: error: unrecognized arguments: --inputs x (did you mean --input?)"
    )


@pytest.mark.parametrize(
    ("argv", "error"),
    [
        (["search", "--deployment", "d", "--limit", "abc"],
         "okg search: error: argument --limit: invalid int value: 'abc'"),
        (["search", "--deployment", "d", "words", "--method", "fuzzy"],
         "okg search: error: argument --method: invalid choice: 'fuzzy'"),
        (["filter", "--deployment", "d", "--input"],
         "okg filter: error: argument --input: expected one argument"),
    ],
)
def test_a_bad_value_prints_the_commands_usage_and_one_error_line(argv, error, capsys):
    """Every parse error of a read command, not only an unknown flag: the
    usage, then one line that names the flag and the value."""
    with pytest.raises(SystemExit) as stopped:
        main(argv)
    assert stopped.value.code == 2
    out = capsys.readouterr()
    assert out.out == ""
    assert out.err.startswith(f"usage: okg {argv[0]} ")
    lines = [line for line in out.err.splitlines() if "error" in line]
    assert len(lines) == 1 and lines[0].startswith(error), out.err


def _search_flags() -> tuple[set[str], set[str]]:
    """`okg search` long flags: those it had before the MCP tool's, and the
    tool's."""
    every = {
        flag for action in _subparser("search")._actions
        for flag in action.option_strings if flag.startswith("--")
    }
    # The shell layer's output flags (#3666 step S2) are newer too.
    new = {
        "--args", "--jsonl", "--ids", "--all",
        *(ops.flag_of(a) for a in ops.SEARCH_OPERATOR_FLAGS),
    }
    assert new < every
    return every - new, new


def test_a_search_flag_prefix_means_what_it_meant_before_the_tool_flags():
    """Every prefix of every flag: it matches the earlier flags that start
    with it and no newer one, so no prefix changed meaning or became
    ambiguous when the tool's flags were added."""
    parser = _subparser("search")
    old, new = _search_flags()
    checked = 0
    for flag in sorted(old | new):
        for end in range(3, len(flag)):
            prefix = flag[:end]
            if prefix in old or prefix in new:
                continue
            matched = {match[1] for match in parser._get_option_tuples(prefix)}
            assert matched == {f for f in old if f.startswith(prefix)}, prefix
            checked += 1
    assert checked > 100


@pytest.mark.parametrize(
    ("rest", "attribute", "value"),
    [
        (["--dep", "d", "words"], "deployment", "d"),
        (["--deployment", "d", "--ds", "postgresql://x/y", "words"], "dsn", "postgresql://x/y"),
        (["--deployment", "d", "--em", "hash", "words"], "embedder", "hash"),
        (["--deployment", "d", "--su", "doc", "words"], "subtype", "doc"),
        (["--deployment", "d", "--attr", "a=1", "words"], "attr_filter", ["a=1"]),
        (["--deployment", "d", "--max-c", "128", "words"], "max_chars", 128),
    ],
)
def test_search_still_takes_the_prefixes_it_took(rest, attribute, value):
    args = _parse(["search", *rest])
    assert getattr(args, attribute) == value
    assert ops.search_takes_operator_path(args) is False


@pytest.mark.parametrize(
    "rest",
    [
        ["--ar", "{}"], ["--wh", "{}"], ["--vi", "list"], ["--subtype-f", "doc"],
        ["--ret", "true"], ["--max-b", "8192"], ["--auth={}"],
    ],
)
def test_a_prefix_never_means_a_flag_of_the_mcp_tool(rest, capsys):
    with pytest.raises(SystemExit) as stopped:
        _parse(["search", "--deployment", "d", "words", *rest])
    assert stopped.value.code == 2
    err = capsys.readouterr().err
    assert err.startswith("usage: okg search ")
    assert "unrecognized arguments" in err


def test_a_tool_flag_typed_in_full_is_taken():
    args = _parse([
        "search", "--deployment", "d", "words", "--attr-filters={}", "--where", "{}",
    ])
    # `--where` is repeatable (#3666 step S2): its terms are joined by and.
    assert args.op_attr_filters == "{}" and args.op_where == ["{}"]
    assert ops.search_takes_operator_path(args) is True


@pytest.mark.parametrize("operator", ops.OPERATOR_COMMANDS)
def test_the_other_commands_take_no_abbreviation(operator, capsys):
    with pytest.raises(SystemExit) as stopped:
        _parse([operator, "--dep", "d"])
    assert stopped.value.code == 2
    assert f"okg {operator}: error:" in capsys.readouterr().err


# ── stderr ───────────────────────────────────────────────────────────────


class _InfoOnly:
    def filter(self, record) -> bool:
        import logging

        return record.levelno <= logging.INFO


def _server_logger():
    """A logger as the server's is in a fresh process, once the MCP library
    has configured logging: INFO reaches stderr. Its warnings go to the root
    logger, where the command decides."""
    import logging
    import sys

    log = logging.getLogger("okg.test.read_commands_shell")
    handler = logging.StreamHandler(sys.stderr)
    handler.addFilter(_InfoOnly())
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    return log, handler


#: The cause the registry gives on a host without the deployment's code
#: checkouts (`sources/registry.py`).
CODE_REPOS_CAUSE = (
    f"{HOST_PATH}: code_repos.base must resolve to an absolute path (got "
    "'${OKG_CODE_REPOS_ROOT}' -> ${OKG_CODE_REPOS_ROOT}); set an absolute path "
    "or an env var like ${OKG_CODE_REPOS_ROOT}"
)


def _log_the_upload_tool_warning(deployment: str = "d") -> None:
    """The record `mcp/upload_admin.py` emits while the server is built: its
    logger, its level, its format and its arguments."""
    import logging

    logging.getLogger("okg.substrate.mcp.upload_admin").warning(
        "upload tool not provisioned for deployment %r: unexpected "
        "registration read failure (%s: %s)",
        deployment, "ValueError", ValueError(CODE_REPOS_CAUSE),
    )


def _log_the_claim_notice(log, deployment: str = "d") -> None:
    """The record `db/capacity.py` emits when the claim is granted: INFO,
    which reaches stderr only through the MCP library's handler."""
    log.info(
        "reserved %d connections for %s (pid %d) through the MCP "
        "reservation functions; other live claims %d, server usable %d",
        2, f"okg-mcp-{deployment}", 4242, 20, 97,
    )


def test_quiet_stderr_hides_the_claim_notice_and_the_upload_tool_warning(capsys):
    """The two blocks a shell run of the read commands printed before most
    replies, as records shaped like the real ones; and logging is put back."""
    import logging

    log, handler = _server_logger()
    root = logging.getLogger()
    before = (root.manager.disable, list(root.handlers))
    try:
        with ops.quiet_stderr():
            _log_the_claim_notice(log)
            log.debug("tool cache miss")
            _log_the_upload_tool_warning()
        assert capsys.readouterr().err == ""
        assert (root.manager.disable, list(root.handlers)) == before
        _log_the_claim_notice(log)
        assert "server usable 97" in capsys.readouterr().err
    finally:
        log.removeHandler(handler)


def test_only_that_emitters_warning_is_hidden(capsys):
    """Not a loose phrase: the same cause from another logger, and another
    warning from the same logger, are both printed (masked)."""
    import logging

    with ops.quiet_stderr():
        logging.getLogger("okg.substrate.sources.registry").warning(
            "registry unreadable: %s", CODE_REPOS_CAUSE,
        )
        logging.getLogger("okg.substrate.mcp.upload_admin").warning(
            "upload registration changed for deployment %r", "d",
        )
        logging.getLogger("okg.substrate.mcp.server").warning(
            "note: upload tool not provisioned for deployment %r", "d",
        )
    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == 3, lines
    assert lines[0].startswith(
        "WARNING okg.substrate.sources.registry: registry unreadable: <path>: "
        "code_repos.base must resolve to an absolute path"
    )
    assert "/srv" not in lines[0]
    assert lines[1] == (
        "WARNING okg.substrate.mcp.upload_admin: upload registration changed "
        "for deployment 'd'"
    )


def test_quiet_stderr_prints_every_other_warning_as_one_line_without_a_host_path(capsys):
    import logging
    import warnings

    with ops.quiet_stderr():
        # A lost audit row is never silent.
        logging.getLogger("okg.substrate.mcp.tracing").warning(
            "MCP trace write failed for %s: %s", "filter", f"could not open\n{HOST_PATH}",
        )
        warnings.warn(f"{HOST_PATH}: sources carry a retired key", UserWarning)
    lines = capsys.readouterr().err.splitlines()
    assert lines == [
        "WARNING okg.substrate.mcp.tracing: MCP trace write failed for filter: "
        "could not open <path>",
        "warning: UserWarning: <path>: sources carry a retired key",
    ]


def test_the_embedder_deprecation_warning_prints(monkeypatch, capsys):
    """`okg search --method semantic --embedder X` on a deployment whose
    manifest already names X: the real warning, as one line."""
    from types import SimpleNamespace

    from okg.substrate import deployments, embeddings
    from okg.substrate.cli import main as cli_main

    manifest = SimpleNamespace(
        embedding_config=SimpleNamespace(enabled=True, backend="hash"),
    )
    monkeypatch.setattr(deployments, "resolve_deployment", lambda name: manifest)
    calls: list[dict] = []

    def build(config, **kwargs):
        calls.append(kwargs)
        return "the plan"

    monkeypatch.setattr(embeddings, "build_embedding_execution_plan", build)
    with ops.quiet_stderr():
        assert cli_main._mcp_serve_embedding_plan("d", asserted_spec="hash") == "the plan"
    # A served query plan makes no paid probe call.
    assert calls == [{"paid_backend": "no_probe"}]
    err = capsys.readouterr().err
    assert err.count("\n") == 1
    assert err.startswith("warning: UserWarning: mcp-serve --embedder is deprecated")


def test_quiet_stderr_prints_an_error_as_one_line_without_the_traceback(capsys):
    import logging

    log = logging.getLogger("okg.test.read_commands_shell_fault")
    with ops.quiet_stderr():
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            log.error("operator filter raised error_id=ab12", exc_info=True)
    err = capsys.readouterr().err
    assert err.count("\n") == 1
    assert "operator filter raised error_id=ab12" in err
    assert "RuntimeError" in err and "OKG_READ_DEBUG=1" in err
    assert "Traceback" not in err and __file__ not in err


def _stand_in_server(monkeypatch, result):
    """Replace the server with one that only makes the server's noise."""
    monkeypatch.delenv("OKG_AGENT", raising=False)
    log, handler = _server_logger()

    def build(args, *, surface, embedder_spec=None, dataset_card=True):
        _log_the_claim_notice(log, args.deployment)
        _log_the_upload_tool_warning(args.deployment)
        return object()

    def call(server, operator, arguments, *, result_format):
        log.info("Processing request of type CallToolRequest")
        return result

    monkeypatch.setattr(ops, "build_read_server", build)
    monkeypatch.setattr(ops, "call_operator", call)
    monkeypatch.setattr(ops, "close_server", lambda server: None)
    return log, handler


def test_a_served_call_prints_nothing_on_stderr(monkeypatch, capsys):
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    log, handler = _stand_in_server(monkeypatch, _Result("filter ok\n[1] doc:000"))
    try:
        assert main(["filter", "--deployment", "d", "--input", "{}"]) == 0
    finally:
        log.removeHandler(handler)
    out = capsys.readouterr()
    assert out.out == "filter ok\n[1] doc:000\n"
    assert out.err == ""


def test_a_refused_call_prints_only_the_refusal_on_stderr(monkeypatch, capsys):
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    refusal = "filter refused invalid_operator_arguments\nretry: drop nonsense"
    log, handler = _stand_in_server(monkeypatch, _Result(refusal, is_error=True))
    try:
        assert main(["filter", "--deployment", "d", "--input", "{}"]) == 2
    finally:
        log.removeHandler(handler)
    out = capsys.readouterr()
    assert out.out == ""
    assert out.err == refusal + "\n"


def test_a_failed_start_prints_only_the_error(monkeypatch, capsys):
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    log, handler = _stand_in_server(monkeypatch, _Result("unused"))

    def build(args, *, surface, embedder_spec=None, dataset_card=True):
        log.info("about to fail: %s", HOST_PATH)
        raise SubstrateError("the database is not ready")

    monkeypatch.setattr(ops, "build_read_server", build)
    try:
        assert main(["filter", "--deployment", "d", "--input", "{}"]) == 1
    finally:
        log.removeHandler(handler)
    out = capsys.readouterr()
    assert out.out == ""
    assert out.err.startswith("ERROR [")
    assert "the database is not ready" in out.err and HOST_PATH not in out.err


@pytest.mark.parametrize("as_json", [False, True])
def test_a_full_connection_ledger_refuses_the_command_by_name(monkeypatch, capsys, as_json):
    from okg.substrate.db.concurrency_budget import BudgetError

    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    log, handler = _stand_in_server(monkeypatch, _Result("unused"))

    def build(args, *, surface, embedder_spec=None, dataset_card=True):
        raise BudgetError(
            "connection reservation refused: okg-mcp-d needs 2,\n"
            "other live claims hold 99 and the server can serve 100"
        )

    monkeypatch.setattr(ops, "build_read_server", build)
    argv = ["filter", "--deployment", "d", "--input", "{}"]
    try:
        assert main(["--json", *argv] if as_json else argv) == 2
    finally:
        log.removeHandler(handler)
    out = capsys.readouterr()
    line = (
        "okg filter: error: BudgetError: connection reservation refused: "
        "okg-mcp-d needs 2, other live claims hold 99 and the server can serve 100"
    )
    assert out.out == ""
    if as_json:
        assert json.loads(out.err)["error"]["message"] == line
    else:
        assert out.err == line + "\n"


def test_the_debug_variable_shows_the_logs_and_the_phase_times(monkeypatch, capsys):
    import re

    monkeypatch.setenv("OKG_READ_DEBUG", "1")
    log, handler = _stand_in_server(monkeypatch, _Result("filter ok"))
    try:
        assert main(["filter", "--deployment", "d", "--input", "{}"]) == 0
    finally:
        log.removeHandler(handler)
    out = capsys.readouterr()
    assert out.out == "filter ok\n"
    assert "reserved 2 connections for okg-mcp-d (pid 4242) through the MCP" in out.err
    assert re.search(
        r"^okg filter timing: parse=\d+ms load=\d+ms build=\d+ms call=\d+ms close=\d+ms "
        r"print=\d+ms$",
        out.err, flags=re.MULTILINE,
    ), out.err


# ── okg query given words ────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("text", "sql"),
    [
        ("select 1", True), ("  SELECT 1", True), ("with a as (select 1) select * from a", True),
        ("values (1)", True), ("(select 1)", True),
        ("-- newest\nselect 1", True), ("/* newest */ select 1", True),
        ("recent pacts", False), ("selection of pacts", False), ("-- select\nrecent pacts", False),
        ("", False), ("delete from okg.nodes", True), ("explain select 1", True),
        ("EXPLAIN (analyze) select 1", True), ("show all", True), ("explaining pacts", False),
    ],
)
def test_text_that_does_not_open_as_a_statement_is_words(text, sql):
    assert ops.looks_like_sql(text) is sql


def test_okg_query_adds_the_search_hint_after_a_refusal_of_words(monkeypatch, capsys):
    refusal = "query refused sql_plan_error\nretry: check column names"
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.setattr(ops, "build_read_server", lambda args, **kw: object())
    monkeypatch.setattr(ops, "close_server", lambda server: None)
    monkeypatch.setattr(
        ops, "call_operator", lambda *a, **k: _Result(refusal, is_error=True),
    )
    assert main(["query", "--deployment", "d", "recent pacts"]) == 2
    assert capsys.readouterr().err == refusal + "\n" + ops.TEXT_SEARCH_HINT + "\n"
    # SQL the tool refuses gets the tool's refusal alone.
    assert main(["query", "--deployment", "d", "select nope"]) == 2
    assert capsys.readouterr().err == refusal + "\n"
    # SQL the tool does not serve is still SQL.
    assert main(["query", "--deployment", "d", "explain select 1"]) == 2
    assert capsys.readouterr().err == refusal + "\n"
    # The JSON stream stays one document.
    assert main(["--json", "query", "--deployment", "d", "recent pacts"]) == 2
    assert capsys.readouterr().err == refusal + "\n"

    # A refusal that is not the tool's (the connection ledger is full) is
    # not about the words, and gets no hint.
    from okg.substrate.db.concurrency_budget import BudgetError

    def full(args, **kwargs):
        raise BudgetError("connection reservation refused: no room")

    monkeypatch.setattr(ops, "build_read_server", full)
    assert main(["query", "--deployment", "d", "recent pacts"]) == 2
    assert capsys.readouterr().err == (
        "okg query: error: BudgetError: connection reservation refused: no room\n"
    )


# ── every stderr line is masked; nothing escapes as a traceback ──────────

HOME_DIR = "/home/user"


def test_a_path_is_masked_whole_and_shown_under_the_debug_variable(monkeypatch):
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    text = (
        f"read {HOST_PATH}, {HOME_DIR} and '{HOME_DIR}/.config/okg/' "
        "at postgresql://reader@db.example:5432/graph, route /mcp, 3/4 done"
    )
    assert ops.mask(text) == (
        "read <path>, <path> and '<path>' "
        "at postgresql://reader@db.example:5432/graph, route /mcp, 3/4 done"
    )
    assert ops.mask_value({"at": [HOST_PATH], "n": 2, "ok": None}) == {
        "at": ["<path>"], "n": 2, "ok": None,
    }
    monkeypatch.setenv("OKG_READ_DEBUG", "1")
    assert ops.mask(text) == text


@pytest.mark.parametrize(
    ("text", "masked"),
    [
        # A DSN's password, as a URL and as key=value, and a --dsn flag.
        (
            "connect to postgresql://reader:s3cret@db.example:5432/graph failed",
            "connect to postgresql://reader:***@db.example:5432/graph failed",
        ),
        (
            "host=db.example user=reader password=s3cret dbname=graph",
            "host=db.example user=reader password=*** dbname=graph",
        ),
        (
            "ran okg --dsn postgresql://reader:s3cret@db/graph",
            "ran okg --dsn <redacted-dsn>",
        ),
        # A socket directory and a password in one DSN.
        ("host=/var/run/postgresql password=s3cret", "host=<path> password=***"),
        # A file URL, whole, and not the bracket after it.
        ("see (file:///srv/deployments/example/sources.yaml)", "see (<path>)"),
        # A bracket pair inside a file URL is part of it.
        ("open file:///srv/a(b)/c now", "open <path> now"),
        ("see (file:///srv/a(b)/c)", "see (<path>)"),
        # A part with one space, when the path goes on past it.
        ("at /srv/My Documents/notes.txt now", "at <path> now"),
        ("at /srv/My Documents/okg/sources.yaml now", "at <path> now"),
        # Ordinary words are left alone.
        ("read /etc/hosts and/or the rest", "read <path> and/or the rest"),
        ("read /etc/hosts and the dir/sub", "read <path> and the dir/sub"),
        ("a profile=file, not a url", "a profile=file, not a url"),
    ],
)
def test_a_password_a_file_url_and_a_path_with_a_space_are_masked(
        monkeypatch, text, masked):
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    assert ops.mask(text) == masked
    assert ops.one_line(text) == masked
    assert ops.mask_value({"cause": [text]}) == {"cause": [masked]}
    monkeypatch.setenv("OKG_READ_DEBUG", "1")
    assert ops.mask(text) == text


def test_a_password_in_an_error_is_masked_on_stderr(monkeypatch, capsys):
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    monkeypatch.delenv("OKG_AGENT", raising=False)

    def broken(operator, args):
        raise RuntimeError("no route to postgresql://reader:s3cret@db.example/graph")

    monkeypatch.setattr(ops, "operator_arguments", broken)
    assert main(["filter", "--deployment", "d", "--input", "{}"]) == 1
    err = capsys.readouterr().err
    assert "s3cret" not in err and "reader:***@db.example" in err, err


def _typed_failure():
    return SubstrateError(
        f"the manifest at {HOST_PATH} is unreadable",
        data={"path": HOST_PATH, "home": HOME_DIR},
        fix_hint=f"check {HOME_DIR}/.okg/local.env",
    )


@pytest.mark.parametrize("as_json", [False, True])
def test_a_typed_error_is_masked_like_every_other_line(monkeypatch, capsys, as_json):
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    log, handler = _stand_in_server(monkeypatch, _Result("unused"))

    def build(args, *, surface, embedder_spec=None, dataset_card=True):
        raise _typed_failure()

    monkeypatch.setattr(ops, "build_read_server", build)
    argv = ["filter", "--deployment", "d", "--input", "{}"]
    try:
        assert main(["--json", *argv] if as_json else argv) == 1
        err = capsys.readouterr().err
        assert "/srv" not in err and "/home" not in err and "reader" not in err, err
        if as_json:
            error = json.loads(err)["error"]
            assert error["message"] == "the manifest at <path> is unreadable"
            assert error["data"] == {"path": "<path>", "home": "<path>"}
            assert error["fix_hint"] == "check <path>"
        else:
            assert err.splitlines()[0] == (
                "ERROR [substrate_error]: the manifest at <path> is unreadable"
            )
        # The debug variable shows what was masked, and the traceback.
        monkeypatch.setenv("OKG_READ_DEBUG", "1")
        assert main(["--json", *argv] if as_json else argv) == 1
        err = capsys.readouterr().err
        assert HOST_PATH in err and HOME_DIR in err and "Traceback" in err
    finally:
        log.removeHandler(handler)


def test_an_error_nothing_reports_is_one_line_not_a_traceback(monkeypatch, capsys):
    """The last resort: a handler that raises what it never expected."""
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    monkeypatch.delenv("OKG_AGENT", raising=False)

    def broken(operator, args):
        raise RuntimeError(f"no such file {HOST_PATH}\nsecond line")

    monkeypatch.setattr(ops, "operator_arguments", broken)
    assert main(["filter", "--deployment", "d", "--input", "{}"]) == 1
    err = capsys.readouterr().err
    assert err.splitlines()[0] == (
        "ERROR [read_command_failed]: okg filter could not finish: no such file <path>"
    )
    assert "Traceback" not in err and "/srv" not in err and "second line" not in err

    monkeypatch.setenv("OKG_READ_DEBUG", "1")
    with pytest.raises(RuntimeError, match="no such file /srv"):
        main(["filter", "--deployment", "d", "--input", "{}"])


def test_a_database_connect_failure_is_reported_masked(monkeypatch, capsys):
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    log, handler = _stand_in_server(monkeypatch, _Result("unused"))

    def build(args, *, surface, embedder_spec=None, dataset_card=True):
        refused = OSError(f"connection refused; see {HOST_PATH}")
        refused.okg_connect_target = {"host": "db.example", "port": 5432}
        raise refused

    monkeypatch.setattr(ops, "build_read_server", build)
    try:
        assert main(["filter", "--deployment", "d", "--input", "{}"]) == 1
    finally:
        log.removeHandler(handler)
    err = capsys.readouterr().err
    assert err.startswith("ERROR [") and "Traceback" not in err and "/srv" not in err


# ── interrupts, and several errors at once ───────────────────────────────


def test_an_interrupt_inside_a_task_group_ends_the_command_as_an_interrupt(monkeypatch):
    """Not as a group traceback with exit 1: the interrupt itself, after the
    pool is closed (which releases the claim). Python exits 130 on it."""
    import argparse

    closed: list[object] = []

    def call(*args, **kwargs):
        raise BaseExceptionGroup("unhandled errors in a TaskGroup", [
            BaseExceptionGroup("unhandled errors in a TaskGroup", [KeyboardInterrupt()]),
        ])

    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    monkeypatch.setattr(ops, "build_read_server", lambda args, **kwargs: "the server")
    monkeypatch.setattr(ops, "close_server", closed.append)
    monkeypatch.setattr(ops, "call_operator", call)
    with pytest.raises(KeyboardInterrupt):
        ops.run_operator("filter", {}, argparse.Namespace(), as_json=False)
    assert closed == ["the server"]

    def stopped(args, **kwargs):
        raise SystemExit(143)

    monkeypatch.setattr(ops, "build_read_server", stopped)
    with pytest.raises(SystemExit) as exited:
        ops.run_operator("filter", {}, argparse.Namespace(), as_json=False)
    assert exited.value.code == 143


@pytest.mark.parametrize("as_json", [False, True])
def test_ctrl_c_ends_a_command_with_130_and_one_line(monkeypatch, capsys, as_json):
    """Python prints a traceback for an interrupt that reaches it, with the
    host's paths in it. A read command says it was interrupted, once, after
    the pool is closed (which releases the claim)."""
    closed: list[object] = []

    def call(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.setattr(ops, "build_read_server", lambda args, **kwargs: "the server")
    monkeypatch.setattr(ops, "close_server", closed.append)
    monkeypatch.setattr(ops, "call_operator", call)
    argv = ["filter", "--deployment", "d", "--input", "{}"]
    assert main(["--json", *argv] if as_json else argv) == ops.INTERRUPTED_EXIT == 130
    out, err = capsys.readouterr()
    assert out == "" and closed == ["the server"]
    assert "Traceback" not in err and "/" not in err, err
    if as_json:
        error = json.loads(err)["error"]
        assert error["code"] == "read_command_failed"
        assert error["message"] == "okg filter was interrupted"
    else:
        assert err == "ERROR [read_command_failed]: okg filter was interrupted\n"

    # The debug variable leaves the interrupt to Python, traceback and all.
    monkeypatch.setenv("OKG_READ_DEBUG", "1")
    with pytest.raises(KeyboardInterrupt):
        main(argv)


_INTERRUPTED_IN_EXEC = """
import argparse
import sys

from okg.substrate.cli import _read_operators as ops


def handler(args, *, as_json):
    # Ctrl-C inside code run from a string, as `dataclasses` runs it for
    # each class a module defines while the read server is built.
    exec("raise KeyboardInterrupt")


sys.exit(ops.guarded(handler)(argparse.Namespace(command="filter"), as_json=False))
"""


def test_ctrl_c_inside_exec_still_exits_130_under_python_m(tmp_path):
    """CPython marks an interrupt that leaves `exec` of a string as
    unhandled, even once it is caught, and `python -m` then ends the process
    by SIGINT (exit -2) instead of with the code it was given. The command
    clears that mark, so it exits 130 as a shell expects."""
    import os
    import subprocess
    import sys

    from tests.pytest.child_pythonpath import child_pythonpath

    (tmp_path / "interrupted_in_exec.py").write_text(_INTERRUPTED_IN_EXEC)
    env = {
        key: value for key, value in os.environ.items()
        if key not in {"OKG_READ_DEBUG", "OKG_AGENT"}
    }
    env["PYTHONPATH"] = child_pythonpath(tmp_path)
    done = subprocess.run(
        [sys.executable, "-P", "-m", "interrupted_in_exec"],
        env=env, capture_output=True, text=True, timeout=120, check=False,
    )
    assert done.returncode == ops.INTERRUPTED_EXIT, (done.returncode, done.stderr)
    assert "Traceback" not in done.stderr, done.stderr
    assert done.stderr.splitlines()[-1:] == [
        "ERROR [read_command_failed]: okg filter was interrupted"
    ], done.stderr


def test_the_hard_end_of_a_stuck_command_names_it_and_prints_no_stacks(
        monkeypatch, capfd):
    """When the unwind after SIGTERM hangs, the stop guard writes its marker
    straight to fd 2 and ends the process. For a read command the marker
    names the command, not okg mcp-serve, and the thread stacks, which name
    files of the host, are left out unless OKG_READ_DEBUG=1."""
    import contextlib

    from okg.substrate.mcp import server as server_mod

    passed: list[dict[str, object]] = []

    @contextlib.contextmanager
    def unwind(release, **options):
        passed.append(options)
        yield

    monkeypatch.setattr(server_mod, "_unwind_default_sigterm", unwind)
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    with ops.released_on_stop("d", "okg query"):
        pass
    monkeypatch.setenv("OKG_READ_DEBUG", "1")
    with ops.released_on_stop("d", "okg query"):
        pass
    assert passed == [
        {"exit_message": "okg query did not finish", "thread_dump": False},
        {"exit_message": "okg query did not finish", "thread_dump": True},
    ]

    capfd.readouterr()
    server_mod._flush_before_hard_exit(143, True, "okg query did not finish", False)
    err = capfd.readouterr().err
    assert err == (
        "okg query did not finish within 5s of SIGTERM; connection claim "
        "released; exit 143\n"
    )
    # okg mcp-serve keeps its marker and its stacks.
    server_mod._flush_before_hard_exit(143, True)
    err = capfd.readouterr().err
    assert err.startswith(server_mod.MCP_STOP_GUARD_EXIT_MESSAGE + " within 5s"), err
    assert "most recent call first" in err, err


def test_of_several_errors_the_one_that_decides_is_reported(monkeypatch, capsys):
    import argparse

    from okg.substrate.db.concurrency_budget import BudgetError

    typed = SubstrateError("typed")
    full = BudgetError("no room")
    broken = ConnectionError("stream closed")
    group = ExceptionGroup
    assert ops.single_cause(group("g", [broken, typed])) == (typed, 2)
    assert ops.single_cause(group("g", [ValueError("v"), group("g", [full]), typed])) == (full, 3)
    assert ops.single_cause(group("g", [broken, ValueError("v")])) == (broken, 2)
    stop = KeyboardInterrupt()
    assert ops.single_cause(BaseExceptionGroup("g", [full, stop]))[0] is stop

    raised: list[BaseException] = [group("g", [broken, typed])]

    def call(*args, **kwargs):
        raise raised[0]

    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.setattr(ops, "build_read_server", lambda args, **kwargs: object())
    monkeypatch.setattr(ops, "close_server", lambda server: None)
    monkeypatch.setattr(ops, "call_operator", call)
    assert ops.run_operator("filter", {}, argparse.Namespace(), as_json=False) == 1
    assert capsys.readouterr().err == "ERROR [substrate_error]: typed\n"
    raised[0] = group("g", [ValueError("v"), full])
    assert ops.run_operator("filter", {}, argparse.Namespace(), as_json=False) == 2
    assert capsys.readouterr().err == "okg filter: error: BudgetError: no room\n"


def test_a_command_puts_the_signal_handlers_back(monkeypatch):
    """Tests, and a program that embeds okg, call a command in-process."""
    import argparse
    import signal

    seen: list[object] = []

    def call(*args, **kwargs):
        seen.append(signal.getsignal(signal.SIGTERM))
        seen.append(signal.getsignal(signal.SIGHUP))
        return _Result("filter ok")

    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    monkeypatch.setattr(ops, "build_read_server", lambda args, **kwargs: object())
    monkeypatch.setattr(ops, "close_server", lambda server: None)
    monkeypatch.setattr(ops, "call_operator", call)
    # As a shell starts a command: both signals at their defaults. The test
    # runner's own handlers are put back afterwards.
    runner = (signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGHUP))
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGHUP, signal.SIG_DFL)
    try:
        assert ops.run_operator("filter", {}, argparse.Namespace(), as_json=False) == 0
        assert all(callable(handler) for handler in seen) and len(seen) == 2
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL
        assert signal.getsignal(signal.SIGHUP) is signal.SIG_DFL
    finally:
        signal.signal(signal.SIGTERM, runner[0])
        signal.signal(signal.SIGHUP, runner[1])


# ── the call, the pool ───────────────────────────────────────────────────


def _echo_server():
    """A small MCP server, and the tools/list requests its clients sent."""
    from mcp import types
    from mcp.server.fastmcp import FastMCP

    server = FastMCP("read-commands-shell")

    @server.tool()
    def echo(word: str) -> str:
        return f"echo ok {word}"

    handlers = server._mcp_server.request_handlers
    listed: list[object] = []
    list_tools = handlers[types.ListToolsRequest]

    async def counting(request):
        # The library's own server refreshes its tool cache by calling this
        # handler with None (the okg server does not); a client's request is
        # a request object.
        if request is not None:
            listed.append(request)
        return await list_tools(request)

    handlers[types.ListToolsRequest] = counting
    return server, listed


def test_one_call_sends_no_tools_list_request():
    """`ClientSession.call_tool` lists every tool after a served call, to
    learn the tool's output schema; the command hands it the schema."""
    server, listed = _echo_server()

    result = ops.call_operator(server, "echo", {"word": "once"}, result_format="text")
    assert result.isError is False
    assert ops.reply_text(result) == ("echo ok once", 0)
    assert listed == []

    refused = ops.call_operator(server, "echo", {}, result_format="json")
    assert refused.isError is True and listed == []

    # The control: without the schema in hand the library does list.
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    async def through_call_tool():
        async with create_connected_server_and_client_session(server) as client:
            return await client.call_tool("echo", {"word": "once"})

    assert ops.reply_text(anyio.run(through_call_tool)) == ("echo ok once", 0)
    assert len(listed) == 1


def test_a_reply_that_breaks_the_tools_output_schema_fails_the_command(
        monkeypatch, capsys):
    """Skipping the listing does not skip the client's check. What the
    caller of the command sees: exit 1 and one line that names the tool and
    says its reply did not match the shape it declares."""
    import argparse

    server, listed = _echo_server()
    assert ops.output_schema_of(server, "echo")["properties"]["result"]["type"] == "string"
    assert ops.output_schema_of(server, "absent") is ops._NOT_REGISTERED

    wrong = {
        "type": "object",
        "properties": {"result": {"type": "integer"}},
        "required": ["result"],
    }
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    monkeypatch.setattr(ops, "output_schema_of", lambda server, operator: wrong)
    monkeypatch.setattr(ops, "build_read_server", lambda args, **kwargs: server)
    monkeypatch.setattr(ops, "close_server", lambda server: None)

    code = ops.run_operator("echo", {"word": "once"}, argparse.Namespace(), as_json=False)
    out = capsys.readouterr()
    assert code == 1 and out.out == ""
    assert out.err == (
        "ERROR [read_command_failed]: okg echo could not complete the call: "
        "the reply of the echo tool did not match the output shape the tool "
        "declares: 'echo ok once' is not of type 'integer'\n"
    )
    assert "TaskGroup" not in out.err and "Traceback" not in out.err
    assert listed == []

    code = ops.run_operator("echo", {"word": "once"}, argparse.Namespace(), as_json=True)
    error = json.loads(capsys.readouterr().err)["error"]
    assert code == 1 and error["code"] == "read_command_failed"
    assert "TaskGroup" not in error["message"] and "echo tool" in error["message"]


def test_the_cause_inside_a_task_group_is_what_the_command_reports(monkeypatch, capsys):
    import argparse

    nested = ExceptionGroup("unhandled errors in a TaskGroup", [
        ExceptionGroup("unhandled errors in a TaskGroup", [
            ValueError(f"cannot read {HOST_PATH}\nsecond line"), KeyError("later"),
        ]),
        OSError("last"),
    ])
    assert ops.single_cause(nested)[1] == 3
    assert ops.single_cause(ValueError("alone"))[1] == 1

    def call(*args, **kwargs):
        raise nested

    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.setattr(ops, "build_read_server", lambda args, **kwargs: object())
    monkeypatch.setattr(ops, "close_server", lambda server: None)
    monkeypatch.setattr(ops, "call_operator", call)
    assert ops.run_operator("filter", {}, argparse.Namespace(), as_json=False) == 1
    err = capsys.readouterr().err
    assert err.splitlines()[0] == (
        "ERROR [read_command_failed]: okg filter could not complete the call: "
        "cannot read <path> (and 2 more errors)"
    )
    assert "TaskGroup" not in err and "second line" not in err

    # A typed error inside the group is reported as itself.
    nested = ExceptionGroup("unhandled errors in a TaskGroup", [SubstrateError("typed")])
    assert ops.run_operator("filter", {}, argparse.Namespace(), as_json=False) == 1
    assert capsys.readouterr().err.startswith("ERROR [substrate_error]: typed")


class _Stopped(Exception):
    """Raised by a stand-in `build_server` once it has recorded its call."""


def test_mcp_serve_builds_its_server_with_the_pool_it_always_had(monkeypatch):
    """`okg mcp-serve` passes no pool width and no reservation switch, and
    the defaults it therefore gets are the ones it had."""
    import inspect

    from okg.substrate.db.concurrency_budget import declared_terms
    from okg.substrate.mcp import server as server_module

    for build in (server_module.build_server, server_module._build_server):
        defaults = inspect.signature(build).parameters
        assert defaults["min_conns"].default == 2
        assert defaults["max_conns"].default is None
        assert defaults["one_call"].default is False
    (read_pool,) = [t for t in declared_terms() if t.name == "mcp.read_pool"]
    assert read_pool.default_width == 16
    assert server_module.MCP_CONNECTIONS_PER_WORKER == 2

    passed: dict = {}

    def build_server(**kwargs):
        passed.update(kwargs)
        raise _Stopped

    monkeypatch.setattr(server_module, "build_server", build_server)
    with pytest.raises(_Stopped):
        server_module.run_server(dsn="postgresql://reader@localhost/graph")
    assert passed["allow_writes"] is False
    assert not {"min_conns", "max_conns", "one_call"} & set(passed)


def test_a_failed_build_releases_only_a_claim_it_asked_for(monkeypatch):
    """A build that fails before it asks for its claim made none, and must
    not release the claim another server of the deployment holds here."""
    from okg.substrate.db import capacity
    from okg.substrate.mcp import server as server_module

    released: list[str] = []
    monkeypatch.setattr(
        capacity, "release_process_reservations",
        lambda *, application_name: released.append(application_name),
    )

    def before_the_claim(**kwargs):
        raise _Stopped

    def after_the_claim(**kwargs):
        kwargs["claim_attempted"].append(True)
        raise _Stopped

    monkeypatch.setattr(server_module, "_build_server", before_the_claim)
    with pytest.raises(_Stopped):
        server_module.build_server(dsn="postgresql://reader@localhost/graph", deployment_name="d")
    assert released == []

    monkeypatch.setattr(server_module, "_build_server", after_the_claim)
    with pytest.raises(_Stopped):
        server_module.build_server(dsn="postgresql://reader@localhost/graph", deployment_name="d")
    assert released == ["okg-mcp-d"]


@pytest.mark.parametrize("demand", [0, -1, True, 2.0, "2"])
def test_an_exact_claim_must_be_a_positive_whole_number(demand):
    from okg.substrate.db import capacity

    with pytest.raises(ValueError, match="demand must be a positive integer"):
        capacity.reserve_process_demand(
            "postgresql://reader@localhost/graph", application_name="okg-mcp-d",
            mcp_read_role=True, demand=demand, heartbeat_interval_seconds=None,
        )


def test_a_command_builds_its_server_for_one_call(monkeypatch, capsys):
    """One connection at the start, a hard limit of the two one read holds
    at once, a claim for those two, and the database-behind note printed."""
    import argparse
    import sys
    from types import SimpleNamespace

    from okg.substrate.cli import dsn_authority
    from okg.substrate.cli import main as cli_main
    from okg.substrate.db import readiness
    from okg.substrate import deployment_release, local_database_roles
    from okg.substrate.mcp import server as server_module

    passed: dict = {}

    def build_server(**kwargs):
        passed.update(kwargs)
        return "the server"

    resolution = SimpleNamespace(dsn="postgresql://reader@localhost/graph", authority={}, target=None)
    monkeypatch.setattr(dsn_authority, "resolve_read_dsn", lambda args, *, surface: resolution)
    monkeypatch.setattr(readiness, "require_database_ready", lambda *a, **k: None)
    monkeypatch.setattr(readiness, "require_mcp_read_dsn_usable", lambda **k: None)
    monkeypatch.setattr(local_database_roles, "require_role_dsn_usable", lambda *a, **k: None)
    monkeypatch.setattr(
        deployment_release, "enforce_release_contract_if_declared", lambda **k: None,
    )
    monkeypatch.setattr(
        cli_main, "_report_database_behind",
        lambda *a, surface, **k: print(
            f"warning: {surface}: 2 tool indexes are missing", file=sys.stderr,
        ),
    )
    monkeypatch.setattr(
        cli_main, "_mcp_serve_embedding_plan",
        lambda deployment, *, asserted_spec: SimpleNamespace(enabled=False),
    )
    monkeypatch.setattr(server_module, "build_server", build_server)

    with ops.quiet_stderr():
        built = ops.build_read_server(
            argparse.Namespace(deployment="d"), surface="okg filter",
        )
    assert built == "the server"
    # A note about the database is the operator's to read: it is printed.
    assert capsys.readouterr().err == "warning: okg filter: 2 tool indexes are missing\n"
    assert passed["min_conns"] == 1
    assert passed["max_conns"] == server_module.MCP_CONNECTIONS_PER_WORKER == 2
    assert server_module.tool_call_width(passed["max_conns"]) == 1
    assert passed["one_call"] is True
    assert passed["allow_writes"] is False and passed["deployment_name"] == "d"
    # The card's facts are read unless the caller says its tool states none.
    assert passed["dataset_card"] is True
    with ops.quiet_stderr():
        ops.build_read_server(
            argparse.Namespace(deployment="d"), surface="okg filter", dataset_card=False,
        )
    assert passed["dataset_card"] is False


# ── a faster start, the same reply (PACT read-commands-trim-3666) ────────


@pytest.mark.parametrize("operator", OPERATORS)
def test_only_inspect_builds_its_server_with_the_dataset_card(monkeypatch, capsys, operator):
    """`inspect` (kinds `session_start` and `surface`) is the one tool whose
    reply states the card; every other command skips reading its facts."""
    monkeypatch.delenv("OKG_AGENT", raising=False)
    built: list[bool] = []

    def build(args, *, surface, embedder_spec=None, dataset_card=True):
        built.append(dataset_card)
        return object()

    monkeypatch.setattr(ops, "build_read_server", build)
    monkeypatch.setattr(
        ops, "call_operator", lambda *a, **k: _Result(f"{operator} ok"),
    )
    monkeypatch.setattr(ops, "close_server", lambda server: None)
    argv = (
        ["search", "anything", "--method", "grep"] if operator == "search"
        else [operator, "--args", "{}"]
    )
    assert main([*argv, "--deployment", "d"]) == 0, capsys.readouterr().err
    assert built == [operator == "inspect"]
    assert ops.CARD_OPERATORS == frozenset({"inspect"})


_DBOS_IDENTITY = """
import json, sys
from okg.substrate.telemetry import store
before = store._dbos_identity_attributes()
loaded = "dbos" in sys.modules
import dbos._context
after = store._dbos_identity_attributes()
print(json.dumps({"before": before, "after": after, "loaded": loaded}))
"""


@pytest.mark.parametrize("environment", [
    {},
    {"DBOS__APPVERSION": "v7"},
    {"DBOS__VMID": "vm-1"},
    {"DBOS__VMID": "local", "DBOS__APPVERSION": " "},
    {"DBOS__VMID": " ", "DBOS_APP_ID": "app-3", "DBOS_WORKER_ID": "w-2"},
])
def test_a_span_states_the_same_dbos_identity_without_loading_dbos(environment):
    """A trace span of a process that never loaded DBOS (every read command)
    no longer imports it, about half a second of the call; the identity
    attributes it stores are the ones DBOS itself would give."""
    import os
    import subprocess
    import sys

    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith("DBOS")
    }
    env.update(environment)
    child = subprocess.run(
        [sys.executable, "-c", _DBOS_IDENTITY],
        env=env, capture_output=True, text=True, timeout=120, check=True,
        start_new_session=True,
    )
    result = json.loads(child.stdout.strip().splitlines()[-1])
    assert result["loaded"] is False
    assert result["before"] == result["after"], result
    if environment.get("DBOS__APPVERSION") == "v7":
        assert result["before"]["dbos.application.version"] == "v7"
