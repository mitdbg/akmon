"""`okg <read tool> --help` and `okg guide`, without a database (PACT
read-commands-help-3666).

The help of each read command is generated from the MCP server built with no
database (`scripts/gen_read_help_text.py`) and checked in as
`okg.substrate.cli._read_help_text`, so the parser never imports the server.
These tests hold the checked-in text to a fresh generation, each example to
the tool's own argument model and to the real parser, and every line a
command prints to the rule that a shell is never sent to tools/list.
`okg guide` against a published graph is in
`test_read_operator_commands_db.py`.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import json
import re
import shlex
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.cli import _read_help_text as help_text
from okg.substrate.cli import _read_operators as ops
from okg.substrate.cli.main import _build_parser, main
from okg.substrate.errors import SubstrateError

ROOT = Path(__file__).resolve().parents[3]
GENERATOR = ROOT / "scripts" / "gen_read_help_text.py"
OPERATORS = tuple(ops.OPERATOR_ARGUMENTS)


def _generator() -> Any:
    spec = importlib.util.spec_from_file_location("_gen_read_help_text", GENERATOR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def generator() -> Any:
    return _generator()


@pytest.fixture
def probe_server(generator):
    """The server the help is generated from: no database anywhere."""
    with generator.hermetic_environment():
        server = generator.build_probe_server()
    try:
        yield server
    finally:
        generator.close_probe_server(server)


def _subparser(name: str) -> argparse.ArgumentParser:
    parser = _build_parser()
    sub = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    return sub.choices[name]


def _squeezed(text: str) -> str:
    """`text` without white space: argparse wraps a help line wherever it
    likes, long words included."""
    return "".join(text.split())


# ── the generated module ─────────────────────────────────────────────────


def test_the_checked_in_help_text_is_a_fresh_generation(generator):
    """A tool whose description or schema changed fails here until
    `python scripts/gen_read_help_text.py` is run again."""
    written = Path(help_text.__file__).read_text(encoding="utf-8")
    assert written == generator.render(), (
        "src/okg/substrate/cli/_read_help_text.py differs from the server; "
        "run python scripts/gen_read_help_text.py"
    )


def test_the_help_text_covers_every_command_and_every_flag():
    assert tuple(help_text.READ_HELP) == OPERATORS
    for operator, entry in help_text.READ_HELP.items():
        assert any(line.strip() for line in entry["description"]), operator
        assert tuple(entry["arguments"]) == ops.OPERATOR_ARGUMENTS[operator], operator
        assert all(line.strip() for line in entry["arguments"].values()), operator


def test_the_help_text_module_imports_nothing_of_okg():
    """The parser reads this module on every okg command; it must not pull
    the server in."""
    tree = ast.parse(Path(help_text.__file__).read_text(encoding="utf-8"))
    imported = {
        node.module if isinstance(node, ast.ImportFrom) else alias.name
        for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert imported <= {"__future__", "typing"}, imported


def test_the_generator_runs_without_the_help_module(tmp_path):
    """`scripts/gen_read_help_text.py` imports `_read_operators`, so that
    module must import without the generated table: a missing or broken
    `_read_help_text.py` is then written again instead of stopping the
    script. Checked in a fresh interpreter with the module made
    unimportable. Without it every okg command that builds the full parser
    fails loudly, not only a read command's --help: `register_subparsers`
    calls `tool_description` for each read command."""
    import subprocess
    import sys

    probe = (
        "import sys\n"
        "sys.modules['okg.substrate.cli._read_help_text'] = None\n"
        "from okg.substrate.cli import _read_operators as ops\n"
        "assert ops.OPERATOR_ARGUMENTS\n"
        "try:\n"
        "    ops.tool_description('filter')\n"
        "except ImportError:\n"
        "    print('refused')\n"
    )
    done = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, timeout=120,
        cwd=tmp_path,
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "refused", (done.stdout, done.stderr)


def test_equal_bounds_read_as_an_exact_count(generator):
    schema = {
        "properties": {
            "metrics": {
                "type": "array", "items": {"const": "count"},
                "minItems": 1, "maxItems": 1,
            },
            "pair": {"type": "array", "items": {"type": "string"}, "minItems": 2, "maxItems": 2},
        },
    }
    lines = generator.argument_lines(("metrics", "pair"), schema)
    assert lines["metrics"] == "list of count, exactly 1 item"
    assert lines["pair"] == "list of string, exactly 2 items"


def test_an_argument_line_states_the_type_and_the_bounds(generator):
    """The summary of a published schema, on a schema written here: the
    bound, the allowed values, the keys of an object, a union by kind."""
    schema = {
        "properties": {
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
            "view": {"enum": ["cards", "list"], "default": "cards"},
            "input": {"anyOf": [{"type": "string"}, {"$ref": "#/$defs/Scope"}]},
            "target": {"oneOf": [{"$ref": "#/$defs/One"}, {"$ref": "#/$defs/Many"}]},
        },
        "required": ["input"],
        "$defs": {
            "Scope": {
                "type": "object",
                "properties": {"kind": {"enum": ["nodes", "edges"]}, "subtype": {"type": "string"}},
                "required": ["kind"],
            },
            "One": {
                "type": "object",
                "properties": {"kind": {"const": "node"}, "node_id": {"type": "string"}},
                "required": ["kind", "node_id"],
            },
            "Many": {
                "type": "object",
                "properties": {
                    "kind": {"const": "nodes"},
                    "node_ids": {"type": "array", "items": {"type": "string"}, "maxItems": 50},
                },
                "required": ["kind"],
            },
        },
    }
    lines = generator.argument_lines(
        ("limit", "view", "input", "target", "subtype_filter"), schema,
    )
    assert lines["limit"] == "integer from 1 to 1000"
    assert lines["view"] == "one of cards, list; default cards"
    assert lines["input"] == (
        "required. object {kind (required): one of nodes, edges; subtype: string} or string"
    )
    assert lines["target"] == (
        "object, one of: kind node needs node_id; kind nodes may take node_ids"
    )
    assert lines["subtype_filter"] == generator.UNPUBLISHED
    with pytest.raises(LookupError):
        generator.summary({"$ref": "#/$defs/Missing"}, {})


def test_only_the_lifted_search_arguments_are_called_where_conditions(generator):
    """Only `search`'s `subtype_filter`, `attr_filters` and `filter` are read
    as a `where` condition. Every other unpublished argument says what the
    `surface` listing says of it, or points there; none claims the `where`
    sentence (an earlier generation gave it to `offset`, `generation_id`,
    `view`, `input` and every other hidden argument)."""
    from okg.substrate.mcp.lean_surface import HIDDEN_ARGUMENTS
    from okg.substrate.mcp.operator_models import UNPUBLISHED_SEARCH_ARGUMENTS

    condition = "reads it as a `where` condition"
    labelled = {
        (tool, name)
        for tool, entry in help_text.READ_HELP.items()
        for name, line in entry["arguments"].items()
        if condition in line
    }
    assert labelled == {("search", name) for name in UNPUBLISHED_SEARCH_ARGUMENTS}
    for tool, hidden in HIDDEN_ARGUMENTS.items():
        lines = help_text.READ_HELP[tool]["arguments"]
        for name in hidden:
            if name not in lines:
                continue
            assert lines[name].startswith(generator.HIDDEN), (tool, name)
            assert condition not in lines[name], (tool, name)
    # A note from the surface listing, and the pointer where it has none.
    search = help_text.READ_HELP["search"]["arguments"]
    # Published again (PACT publish-paging-args-3666): a cut page names
    # `offset`, and `max_chars` is the excerpt length.
    for name in ("offset", "max_chars"):
        assert not search[name].startswith(generator.HIDDEN), (name, search[name])
    assert search["generation_id"] == (
        generator.HIDDEN + ": reads this one call at another published generation"
    )
    assert help_text.READ_HELP["filter"]["arguments"]["generation_id"] == (
        generator.HIDDEN + generator.SURFACE_POINTER
    )
    # The failure case: the old one-line-for-all label is refused.
    assert generator.hidden_line("filter", "offset") != generator.UNPUBLISHED
    assert generator.hidden_line("expand", "filter") != generator.UNPUBLISHED


@pytest.mark.parametrize("key", ["anyOf", "oneOf"])
def test_a_union_reads_the_same_in_either_member_order(generator, key):
    """Python's typing cache treats Union[int, str] and Union[str, int] as
    one key, so the server publishes `generation_id` as [integer, string]
    or [string, integer] depending on which spelling a process imported
    first (issue 3678). The line must not follow that order."""
    integer, string, null = {"type": "integer"}, {"type": "string"}, {"type": "null"}
    first = generator.summary({key: [integer, string, null]}, {})
    second = generator.summary({key: [null, string, integer]}, {})
    assert first == second == "integer or string"
    assert generator.summary({"type": ["string", "integer"]}, {}) == "integer or string"


# ── the examples ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("operator", OPERATORS)
def test_each_example_is_a_call_the_tool_accepts(operator, probe_server):
    """The tool's own argument model, and its undeclared-argument check,
    take the example's argument object as written."""
    example = help_text.READ_HELP[operator]["example_arguments"]
    tool = probe_server._tool_manager.get_tool(operator)
    tool.fn_metadata.arg_model.model_validate(example)
    probe_server._refuse_unknown_operator_arguments(operator, dict(example))


@pytest.mark.parametrize("operator", OPERATORS)
def test_each_example_command_line_sends_its_argument_object(operator):
    entry = help_text.READ_HELP[operator]
    args = _build_parser().parse_args([operator, "--deployment", "d", *entry["example_argv"]])
    if operator == "search":
        assert ops.search_takes_operator_path(args)
        built = ops.search_operator_arguments(args)
    else:
        built = ops.operator_arguments(operator, args)
    assert built == entry["example_arguments"]


def test_a_wrong_example_is_caught(probe_server):
    """The check above fails on the two mistakes agents made most: a scope
    `kind` the tool lacks, and a bare subtype word for the scope."""
    import pydantic

    model = probe_server._tool_manager.get_tool("filter").fn_metadata.arg_model
    for wrong in (["--input", '{"kind": "all"}'], ["--input", "ticket"]):
        args = _build_parser().parse_args(["filter", "--deployment", "d", *wrong])
        with pytest.raises(pydantic.ValidationError):
            model.model_validate(ops.operator_arguments("filter", args))


# ── what --help prints ───────────────────────────────────────────────────


@pytest.mark.parametrize("operator", OPERATORS)
def test_the_help_prints_the_description_each_flags_type_and_the_example(operator):
    entry = help_text.READ_HELP[operator]
    text = _subparser(operator).format_help()
    # argparse closes up three or more line breaks to two; nothing else in
    # the description changes.
    description = re.sub(r"\n\n\n+", "\n\n", ops.tool_description(operator))
    assert description in text
    assert shlex.join(["okg", operator, *entry["example_argv"]]) in text
    assert json.dumps(entry["example_arguments"]) in text
    shown = (
        ops.SEARCH_OPERATOR_FLAGS if operator == "search" else ops.OPERATOR_ARGUMENTS[operator]
    )
    for argument in shown:
        assert _squeezed(entry["arguments"][argument]) in _squeezed(text), argument
    # The template every flag had before says nothing about the shape.
    assert "(JSON, or a string)" not in text


@pytest.mark.parametrize("command", [*OPERATORS, "guide"])
def test_no_help_sends_a_shell_to_tools_list(command):
    assert "tools/list" not in _subparser(command).format_help()


def test_the_top_level_help_names_the_read_commands_and_guide():
    parser = _build_parser()
    text = parser.format_help()
    assert ops.READ_COMMANDS_EPILOG in text
    squeezed = " ".join(ops.READ_COMMANDS_EPILOG.split())
    for command in ("okg guide", *OPERATORS):
        assert command in squeezed, command


# ── what a refusal prints ────────────────────────────────────────────────


def _refusals(server: Any, operator: str) -> list[SubstrateError]:
    """The two refusals whose fix hint sends an MCP client to the tool's
    input schema: an undeclared argument, and an invalid one."""
    found: list[SubstrateError] = []
    for check, arguments in (
        (server._refuse_unknown_operator_arguments, {"nonsense": 1}),
        (server._validate_operator_arguments, {"generation_id": [1]}),
    ):
        with pytest.raises(SubstrateError) as raised:
            check(operator, arguments)
        found.append(raised.value)
    return found


@pytest.mark.parametrize("operator", OPERATORS)
def test_a_refusal_points_at_the_commands_help_not_at_tools_list(
        operator, probe_server, capsys):
    from okg.substrate.mcp.response_bounds import served_call_tool_result

    for refusal in _refusals(probe_server, operator):
        payload = {"status": "refused", "operator_name": operator, **refusal.to_json()}
        for result_format in ("json", "text"):
            result = served_call_tool_result(
                payload, is_error=True, tool=operator, result_format=result_format,
            )
            served = result.content[0].text
            # What an MCP client reads: the pointer a shell cannot follow.
            assert "tools/list" in served, served
            capsys.readouterr()
            assert ops.print_reply(result, as_json=result_format == "json") == 2
            out = capsys.readouterr()
            assert out.out == ""
            assert "tools/list" not in out.err, out.err
            assert f"run `okg {operator} --help`" in out.err, out.err
            # Only the pointer changed.
            assert out.err == ops.shell_refusal_text(served) + "\n"
            if result_format == "json":
                assert json.loads(out.err)["error"]["code"] == refusal.code


def test_a_served_reply_is_never_rewritten(capsys):
    class Block:
        type = "text"
        text = "search ok · re-read search's inputSchema in tools/list"

    class Result:
        content = [Block()]
        isError = False

    assert ops.print_reply(Result(), as_json=False) == 0
    assert capsys.readouterr().out == Block.text + "\n"


# ── okg guide ────────────────────────────────────────────────────────────


INSTRUCTIONS = "The graph holds tickets.\n\n## Deployment guidance\nAsk about é."


class _Server:
    instructions = INSTRUCTIONS


@pytest.fixture
def guide_server(monkeypatch):
    built: list[Any] = []
    closed: list[Any] = []

    def build(args, **kwargs):
        built.append((args.deployment, kwargs))
        return _Server()

    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    monkeypatch.setattr(ops, "build_read_server", build)
    monkeypatch.setattr(ops, "close_server", closed.append)
    return built, closed


def test_guide_prints_the_instructions_as_served(guide_server, capsys):
    built, closed = guide_server
    assert main(["guide", "--deployment", "d"]) == 0
    out = capsys.readouterr()
    # The pipeline lead and a rule, then the served text as served.
    assert out.out == (
        ops.pipeline_text() + "\n\n" + ops.GUIDE_INSTRUCTIONS_RULE + "\n" + INSTRUCTIONS + "\n"
    )
    assert out.err == ""
    assert built == [("d", {"surface": "okg guide"})]
    assert len(closed) == 1


def test_guide_json_gives_the_text_its_bytes_and_its_sha256(guide_server, capsys):
    assert main(["guide", "--deployment", "d", "--json"]) == 0
    out = capsys.readouterr()
    assert out.err == ""
    reply = json.loads(out.out)
    data = INSTRUCTIONS.encode("utf-8")
    assert reply == {
        "text": INSTRUCTIONS,
        "bytes": len(data),
        "sha256": "sha256:" + hashlib.sha256(data).hexdigest(),
    }
    # The same function the server's session_start readback hashes with.
    from okg.substrate.mcp.surface_freshness import served_instructions_fields

    assert reply["sha256"] == served_instructions_fields(INSTRUCTIONS)["instructions_hash"]


def test_guide_reports_a_server_that_cannot_start_and_prints_no_text(
        monkeypatch, capsys):
    def build(args, **kwargs):
        error = SubstrateError("the read role cannot log in", fix_hint="grant okg_mcp")
        error.code = "read_role_unusable"
        raise error

    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    monkeypatch.setattr(ops, "build_read_server", build)
    monkeypatch.setattr(ops, "close_server", lambda server: pytest.fail("nothing to close"))
    assert main(["guide", "--deployment", "d"]) == 1
    out = capsys.readouterr()
    assert out.out == ""
    assert out.err.startswith("ERROR [read_role_unusable]: the read role cannot log in")


def test_guide_takes_no_tool_flag(capsys):
    with pytest.raises(SystemExit) as stopped:
        main(["guide", "--deployment", "d", "--target", "{}"])
    assert stopped.value.code == 2
    err = capsys.readouterr().err
    assert err.startswith("usage: okg guide ")
