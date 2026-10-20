"""Two faces, one reply (PACT read-commands-3666).

Over a published fixture graph, each read operator is called once as an okg
command and once from an MCP client, and the replies are compared: the JSON
reply, the text reply, one refusal per operator, `--args` from a file and
from standard input, and a call under access rules.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import shlex
import time
from pathlib import Path
from typing import Any

import anyio
import pytest

from okg.substrate.cli import _read_operators as ops
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.cli.main import _build_parser, main
from tests.substrate.mcp.test_search_list import (
    EXTERNAL,
    MAIN,
    READER,
    TERM,
    Served,
    _activate,
    _corpus,
    _trust,
)

pytestmark = pytest.mark.postgres

#: Reply fields that differ between two calls that are otherwise the same:
#: the time of the call, and the name of the client session, of one error
#: report or of one access decision record. Nothing else is ignored, in the
#: JSON reply or in the text reply.
CALL_TIME_FIELDS = frozenset({"publish_age_seconds", "publish_lag_seconds", "elapsed_ms"})
CALL_IDENTITY_FIELDS = frozenset({
    "logical_session_id", "error_id", "decision_id", "nomos_decision_id",
})

#: A size a reply states about itself that counts a call-time field. Only
#: `query` has one: it measures its payload with `elapsed_ms` (whole
#: milliseconds) inside it, so a call that took 9 ms and one that took 10 ms
#: differ by one byte in `payload_bytes`.
CALL_TIME_DERIVED_FIELDS: dict[str, frozenset[str]] = {
    "query": frozenset({"payload_bytes"}),
}

#: Where the text form prints a reply's own fields that no place in its
#: grammar takes (`result_text._key_lines`): one unindented `name: value`
#: line each, after this marker. A record's attributes are printed in the
#: body, before it, so one named like a field below is still compared.
_OTHER_FIELDS = "--- other fields"
#: The one notice that states such a field (`publish_lag_seconds`).
_LAG_NOTICE = re.compile(r"^(! publish is stale: )\S+(s behind the newest source poll)$")

#: A served call that waits for a connection it cannot have waits the pool's
#: acquire timeout (5 s) before it gives up, so a call this fast did not.
CALL_SECONDS_BOUND = 4.0

WHERE_DOC = {"op": "eq", "field": "type", "value": "doc"}
TICKETS = {"kind": "nodes", "subtype": "ticket"}
SQL = "SELECT node_id FROM okg.v_nodes ORDER BY node_id LIMIT 3"

#: operator -> (the command line after the deployment flags, the same call as
#: an MCP argument object).
CASES: dict[str, tuple[list[str], dict[str, Any]]] = {
    "search": (
        [TERM, "--where", json.dumps(WHERE_DOC), "--limit", "5"],
        {"query": TERM, "where": WHERE_DOC, "limit": 5},
    ),
    "inspect": (
        ["--target", '{"kind": "node", "node_id": "doc:000"}'],
        {"target": {"kind": "node", "node_id": "doc:000"}},
    ),
    "expand": (
        ["doc:000", "--edge-type", "contains"],
        {"input": "doc:000", "edge_type": "contains"},
    ),
    "filter": (
        ["--input", json.dumps(TICKETS), "--predicate", "{}"],
        {"input": TICKETS, "predicate": {}},
    ),
    "aggregate": (
        ["--input", '{"kind": "nodes"}', "--by", "subtype", "--metrics", "count"],
        {"input": {"kind": "nodes"}, "by": ["subtype"], "metrics": ["count"]},
    ),
    "map": (
        ["--input", json.dumps(TICKETS), "--projection", "node_id"],
        {"input": TICKETS, "projection": ["node_id"]},
    ),
    "query": (
        [SQL, "--row-cap", "10"],
        {"sql": SQL, "row_cap": 10},
    ),
}


def _stable(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _stable(item) for key, item in value.items()
            if key not in CALL_TIME_FIELDS and key not in CALL_IDENTITY_FIELDS
        }
    if isinstance(value, list):
        return [_stable(item) for item in value]
    return value


def _stable_reply(operator: str, reply: Any) -> Any:
    """`_stable`, and without the operator's own call-time sizes: top-level
    fields of the reply, never a field of a row."""
    stable = _stable(reply)
    if isinstance(stable, dict):
        for name in CALL_TIME_DERIVED_FIELDS.get(operator, ()):
            stable.pop(name, None)
    return stable


def _stable_text(operator: str, text: str) -> str:
    """A text reply without the values that differ between two calls, and
    nothing else removed. Only the reply's own lines are touched: an
    unindented `name: value` line after `--- other fields` whose name is
    exactly a call-time or call-identity field (or, for `query`, its payload
    size), and the lag notice. Each keeps its words and loses the value;
    every other line, and the number and order of lines, is kept."""
    names = (
        CALL_TIME_FIELDS | CALL_IDENTITY_FIELDS
        | CALL_TIME_DERIVED_FIELDS.get(operator, frozenset())
    )
    lines = text.split("\n")
    own = max(
        (index for index, line in enumerate(lines) if line == _OTHER_FIELDS),
        default=len(lines),
    )
    out: list[str] = []
    for index, line in enumerate(lines):
        name, found, _value = line.partition(": ")
        if index > own and found and name in names:
            line = f"{name}: *"
        else:
            line = _LAG_NOTICE.sub(r"\1*\2", line)
        out.append(line)
    return "\n".join(out)


def test_the_text_comparison_ignores_only_the_fields_that_vary_per_call():
    """No database. What two calls of one read may differ in, and no more."""
    one = (
        "query ok · rows 3 · generation 7\n"
        "! publish is stale: 4000s behind the newest source poll\n"
        "[1] doc:000\n"
        "  elapsed_ms: 9\n"
        "  attrs.payload_bytes: 381\n"
        "payload_bytes: 381\n"
        "--- other fields\n"
        "payload_bytes: 381\n"
        "payload_byte_cap: 1000000\n"
        "elapsed_ms: 9\n"
        "note: payload_bytes: 381 is quoted in a row\n"
    )
    two = (
        one.replace("4000s", "4001s")
        .replace("\nelapsed_ms: 9\n", "\nelapsed_ms: 10\n")
        .replace("fields\npayload_bytes: 381\n", "fields\npayload_bytes: 382\n")
    )
    assert one != two and two.count("382") == 1
    assert _stable_text("query", one) == _stable_text("query", two)
    assert _stable_text("query", one).count("\n") == one.count("\n")
    # Only `query` measures a payload that holds its own elapsed time.
    assert _stable_text("filter", one) != _stable_text("filter", two)
    # A row, a count, a cap, a field of another name, and a record's
    # attribute named like a varying field still have to match.
    for old, new in (
        ("doc:000", "doc:001"), ("rows 3", "rows 4"), ("generation 7", "generation 8"),
        ("payload_byte_cap: 1000000", "payload_byte_cap: 1000001"),
        ("is quoted in a row", "is quoted in a cell"),
        ("note: payload_bytes: 381 is", "note: payload_bytes: 382 is"),
        ("  elapsed_ms: 9", "  elapsed_ms: 10"),
        ("  attrs.payload_bytes: 381", "  attrs.payload_bytes: 382"),
        ("[1] doc:000\n  elapsed_ms: 9\n  attrs.payload_bytes: 381\npayload_bytes: 381",
         "[1] doc:000\n  elapsed_ms: 9\n  attrs.payload_bytes: 381\npayload_bytes: 382"),
    ):
        assert old in one
        assert _stable_text("query", one) != _stable_text("query", one.replace(old, new, 1)), old

    row = {"node_id": "doc:000", "payload_bytes": 381}
    reply = {"rows": [row], "payload_bytes": 381, "elapsed_ms": 9}
    assert _stable_reply("query", reply) == {"rows": [row]}
    assert _stable_reply("filter", reply) == {"rows": [row], "payload_bytes": 381}


def _mcp_calls(dsn: str, deployment: str, *calls: tuple[str, dict, str]) -> list[Any]:
    """Each ``(operator, arguments, format)`` from one MCP client session.

    The server is closed before this returns, so it never runs beside the
    one a command builds for itself.
    """
    from mcp.shared.memory import create_connected_server_and_client_session

    server = Served(dsn, deployment)
    out: list[Any] = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(server.mcp) as client:
            for operator, arguments, result_format in calls:
                out.append(await client.call_tool(
                    operator, arguments, meta={"okg/result_format": result_format},
                ))

    try:
        anyio.run(drive)
    finally:
        server.close()
    return out


def _initialize_reply(dsn: str, deployment: str) -> Any:
    """The `initialize` reply a read-only MCP client of the deployment gets,
    over the SDK's in-memory streams. The server is closed before this
    returns."""
    from mcp.client.session import ClientSession
    from mcp.shared.memory import create_client_server_memory_streams

    server = Served(dsn, deployment)
    lowlevel = server.mcp._mcp_server
    replies: list[Any] = []

    async def drive() -> None:
        async with create_client_server_memory_streams() as (client_streams, server_streams):
            async with anyio.create_task_group() as group:
                group.start_soon(lambda: lowlevel.run(
                    server_streams[0], server_streams[1],
                    lowlevel.create_initialization_options(),
                    raise_exceptions=True,
                ))
                async with ClientSession(*client_streams) as client:
                    replies.append(await client.initialize())
                group.cancel_scope.cancel()

    try:
        anyio.run(drive)
    finally:
        server.close()
    (reply,) = replies
    return reply


def _argv(operator: str, dsn: str, deployment: str, rest: list[str]) -> list[str]:
    return [operator, "--deployment", deployment, "--dsn", dsn, *rest]


def _run(capsys, argv: list[str], *, as_json: bool) -> tuple[int, str, str]:
    capsys.readouterr()
    rc = main(["--json", *argv] if as_json else argv)
    captured = capsys.readouterr()
    return rc, captured.out, captured.err


def _last_json_line(text: str) -> dict:
    """The reply on a stream that may also carry warning lines."""
    for line in reversed(text.strip().splitlines()):
        try:
            parsed = json.loads(line)
        except ValueError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise AssertionError(f"no JSON object on the stream: {text[-400:]!r}")


def _node_ids(value: Any) -> set[str]:
    """Every `node_id` of every row in a reply, wherever the rows sit."""
    found: set[str] = set()
    if isinstance(value, dict):
        if isinstance(value.get("node_id"), str):
            found.add(value["node_id"])
        for item in value.values():
            found |= _node_ids(item)
    elif isinstance(value, list):
        for item in value:
            found |= _node_ids(item)
    return found


@pytest.fixture
def graph(clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """The published search-list corpus: ``(dsn, deployment)``."""
    monkeypatch.delenv("OKG_AGENT", raising=False)
    return clean_runtime, _corpus(tmp_path, clean_runtime)


def test_the_command_lines_are_the_calls_they_are_compared_with():
    """No database: each case's flags build exactly its MCP argument object."""
    for operator, (rest, arguments) in CASES.items():
        args = _build_parser().parse_args(_argv(operator, "postgresql://x/y", "d", rest))
        built = (
            ops.search_operator_arguments(args) if operator == "search"
            else ops.operator_arguments(operator, args)
        )
        assert built == arguments, operator
    assert set(CASES) == set(ops.OPERATOR_ARGUMENTS)


def test_the_help_examples_are_the_calls_run_here():
    """No database. The example `okg <tool> --help` prints is the call of
    this file's CASES, which the tests here run against a published graph
    (PACT read-commands-help-3666)."""
    from okg.substrate.cli._read_help_text import READ_HELP

    for operator, (rest, arguments) in CASES.items():
        assert list(READ_HELP[operator]["example_argv"]) == rest, operator
        assert READ_HELP[operator]["example_arguments"] == arguments, operator
    assert set(READ_HELP) == set(CASES)


def test_okg_guide_prints_the_instructions_session_start_names(graph, capsys):
    """`okg guide --json` gives the text a read-only MCP client is sent in
    its `initialize` reply, and that text's hash; the same deployment's
    `inspect` session_start reports that hash."""
    from okg.substrate.mcp.surface_freshness import served_instructions_fields

    dsn, deployment = graph
    rc, out, err = _run(capsys, ["guide", "--deployment", deployment, "--dsn", dsn], as_json=True)
    assert rc == 0 and err == "", (rc, err)
    guide = json.loads(out)
    assert set(guide) == {"text", "bytes", "sha256"}
    assert guide["text"].strip(), guide
    assert guide["bytes"] == len(guide["text"].encode("utf-8"))

    # What a real MCP client is sent when it connects.
    sent = _initialize_reply(dsn, deployment).instructions
    assert isinstance(sent, str) and sent.strip(), sent
    assert guide["text"] == sent
    assert guide["sha256"] == "sha256:" + hashlib.sha256(sent.encode("utf-8")).hexdigest()
    assert guide["sha256"] == served_instructions_fields(sent)["instructions_hash"]

    rc, out, err = _run(capsys, _argv("inspect", dsn, deployment, [
        "--target", '{"kind": "session_start"}',
    ]), as_json=True)
    assert rc == 0, (out, err)
    reply = json.loads(out)
    hashes = _values_of(reply, "instructions_hash")
    assert hashes == [guide["sha256"]], (guide["sha256"], reply)
    assert _values_of(reply, "instructions_bytes") == [guide["bytes"]], reply

    # The text form leads with the pipeline, then gives the same text, as served.
    rc, out, err = _run(capsys, ["guide", "--deployment", deployment, "--dsn", dsn], as_json=False)
    assert rc == 0 and err == "", (rc, err)
    assert out == (
        ops.pipeline_text() + "\n\n" + ops.GUIDE_INSTRUCTIONS_RULE + "\n"
        + guide["text"] + ("" if guide["text"].endswith("\n") else "\n")
    )


def _values_of(value: Any, key: str) -> list[Any]:
    """Every value stored under `key`, wherever it sits in a reply."""
    found: list[Any] = []
    if isinstance(value, dict):
        if key in value:
            found.append(value[key])
        for item in value.values():
            found += _values_of(item, key)
    elif isinstance(value, list):
        for item in value:
            found += _values_of(item, key)
    return found


def _record_card_builds(monkeypatch) -> list[bool]:
    """Record, for each server a command builds, whether it read the
    dataset card's facts. The MCP face (`Served`) always does."""
    from okg.substrate.mcp import server as server_module

    built: list[bool] = []
    build_server = server_module.build_server

    def recording(**kwargs):
        built.append(kwargs.get("dataset_card", True))
        return build_server(**kwargs)

    monkeypatch.setattr(server_module, "build_server", recording)
    return built


def test_each_command_prints_the_json_reply_an_mcp_client_gets(graph, capsys, monkeypatch):
    """The two faces, and the trimmed start (PACT read-commands-trim-3666):
    every command but `inspect` builds its server without the dataset card,
    and its reply is still the MCP client's."""
    dsn, deployment = graph
    expected = _mcp_calls(dsn, deployment, *(
        (operator, arguments, "json") for operator, (_rest, arguments) in CASES.items()
    ))
    card_builds = _record_card_builds(monkeypatch)
    for (operator, (rest, _arguments)), result in zip(CASES.items(), expected):
        reply = json.loads(result.content[0].text)
        rc, out, err = _run(capsys, _argv(operator, dsn, deployment, rest), as_json=True)
        if result.isError:
            # Only `query` may be withheld from a surface; the command then
            # reports it exactly as the MCP client is told.
            assert operator == "query", (operator, reply)
            assert rc == 2, (operator, out, err)
            assert _stable(_last_json_line(err)) == _stable(reply), operator
            assert out == "", operator
            continue
        assert rc == 0, (operator, out, err)
        assert _stable_reply(operator, json.loads(out)) == _stable_reply(operator, reply), operator
        # Nothing cut: the printed reply is the client's text block.
        assert len(out) >= len(result.content[0].text), operator
    assert card_builds == [operator == "inspect" for operator in CASES]


def test_each_command_refuses_as_the_mcp_tool_refuses(graph, capsys, monkeypatch):
    """An argument the operator does not declare: exit 2, the MCP refusal,
    also from a server built without the dataset card."""
    dsn, deployment = graph
    stray = {"nonsense": 1}
    expected = _mcp_calls(dsn, deployment, *(
        (operator, stray, result_format)
        for operator in CASES for result_format in ("json", "text")
    ))
    card_builds = _record_card_builds(monkeypatch)
    for index, operator in enumerate(CASES):
        as_json, as_text = expected[2 * index], expected[2 * index + 1]
        assert as_json.isError is True and as_text.isError is True, operator
        refusal = json.loads(as_json.content[0].text)
        argv = _argv(operator, dsn, deployment, ["--args", json.dumps(stray)])

        # The one change the command makes to a refusal (PACT
        # read-commands-help-3666): the pointer to the tool's input schema
        # in tools/list names `okg <operator> --help`.
        shell_json = ops.shell_refusal_text(as_json.content[0].text)
        shell_text = ops.shell_refusal_text(as_text.content[0].text)

        rc, out, err = _run(capsys, argv, as_json=True)
        assert rc == 2, (operator, out, err)
        assert out == "", operator
        assert _stable(_last_json_line(err)) == _stable(json.loads(shell_json)), operator
        assert "tools/list" not in err, (operator, err)
        assert refusal["status"] == "refused", refusal
        if "error_id" not in json.dumps(refusal):
            # A served operator names the stray argument; only a withheld
            # `query` answers with an error report instead.
            assert refusal["error"]["data"]["unknown_arguments"] == ["nonsense"], refusal
            assert "tools/list" in refusal["error"]["fix_hint"], refusal
            assert f"run `okg {operator} --help`" in err, (operator, err)

        rc, out, err = _run(capsys, argv, as_json=False)
        assert rc == 2, (operator, out, err)
        assert out == "", operator
        assert "tools/list" not in err, (operator, err)
        if "error_id" not in json.dumps(refusal):
            assert err.endswith(shell_text + "\n"), operator
    # Two commands per operator, JSON then text.
    assert card_builds == [operator == "inspect" for operator in CASES for _ in (1, 2)]


def test_the_default_output_is_the_text_an_mcp_client_reads(graph, capsys, monkeypatch):
    dsn, deployment = graph
    names = tuple(CASES)
    expected = _mcp_calls(dsn, deployment, *(
        (operator, CASES[operator][1], "text") for operator in names
    ))
    card_builds = _record_card_builds(monkeypatch)
    for operator, result in zip(names, expected):
        rc, out, err = _run(
            capsys, _argv(operator, dsn, deployment, CASES[operator][0]), as_json=False,
        )
        if result.isError:
            # Only `query` may be withheld from a surface.
            assert operator == "query", (operator, result.content[0].text)
            assert rc == 2 and out == "", (operator, out, err)
            continue
        assert rc == 0, operator
        # The two calls are compared whole, apart from the values of the
        # fields that state the time or the identity of one call.
        from okg.substrate.mcp.result_text import result_generation

        served = result.content[0].text + "\n"
        if result_generation(served) is None:
            # #3666 (read-passage-rows): an MCP session names the generation
            # on its first reply only; the shell has no session and names it
            # on every reply. The shell's is then compared without it.
            out = re.sub(r" · generation [^ ·\n]+\n", "\n", out, count=1)
        assert _stable_text(operator, out) == _stable_text(operator, served), operator
        with pytest.raises(ValueError):
            json.loads(out)
    assert card_builds == [operator == "inspect" for operator in names]


def test_args_from_a_file_and_from_standard_input(graph, capsys, tmp_path, monkeypatch):
    dsn, deployment = graph
    arguments = CASES["filter"][1]
    (reply,) = _mcp_calls(dsn, deployment, ("filter", arguments, "json"))
    assert reply.isError is False
    expected = _stable(json.loads(reply.content[0].text))
    path = tmp_path / "filter.json"
    path.write_text(json.dumps(arguments), encoding="utf-8")

    rc, out, err = _run(
        capsys, _argv("filter", dsn, deployment, ["--args", f"@{path}"]), as_json=True,
    )
    assert rc == 0, err
    assert _stable(json.loads(out)) == expected

    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(arguments)))
    rc, out, err = _run(
        capsys, _argv("filter", dsn, deployment, ["--args", "-"]), as_json=True,
    )
    assert rc == 0, err
    assert _stable(json.loads(out)) == expected

    # The same object split between --args and a flag is one call too.
    rc, out, err = _run(capsys, _argv("filter", dsn, deployment, [
        "--args", json.dumps({"input": arguments["input"]}), "--predicate", "{}",
    ]), as_json=True)
    assert rc == 0, err
    assert _stable(json.loads(out)) == expected


def test_search_with_only_todays_flags_still_runs_todays_search(graph, capsys):
    """The path `okg search` had before: its own envelope, not the MCP one."""
    dsn, deployment = graph
    rc, out, err = _run(capsys, _argv("search", dsn, deployment, [
        TERM, "--limit", "3", "--completeness", "exact",
    ]), as_json=True)
    assert rc == 0, err
    page = json.loads(out)
    assert "nomos_enforcement_mode" in page and "authority" in page
    assert page["limit"] == 3 and len(page["results"]) == 3


def test_a_denied_record_is_absent_from_okg_filter_as_from_mcp_filter(
        graph, capsys, monkeypatch):
    """Access rules decide the same on both faces: the reader the server's
    environment names, the same policy, the same rows."""
    dsn, deployment = graph
    docs = {"kind": "nodes", "subtype": "doc"}
    by_path = {"op": "eq", "field": "attrs.path", "value": "x/y.md"}
    arguments = {"input": docs, "predicate": by_path}
    argv = _argv("filter", dsn, deployment, [
        "--input", json.dumps(docs), "--predicate", json.dumps(by_path),
    ])

    # Without access rules the record is there, on both faces.
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    monkeypatch.delenv("OKG_NOMOS_SUBJECT", raising=False)
    (open_reply,) = _mcp_calls(dsn, deployment, ("filter", arguments, "json"))
    rc, out, err = _run(capsys, argv, as_json=True)
    assert rc == 0, err
    assert _node_ids(json.loads(open_reply.content[0].text)) == {"doc:ext"}
    assert _node_ids(json.loads(out)) == {"doc:ext"}

    # The operator activates the bundle; the server then enforces it.
    _activate(dsn, deployment, [
        {"rule_id": "deny_external", "effect": "deny", "actions": ["graph.read.mcp"],
         "subjects": [READER], "match": {"source_id": EXTERNAL}, "priority": 200},
        {"rule_id": "allow_read", "effect": "allow",
         "actions": ["graph.read.mcp", "mcp.tool.call"],
         "subjects": [READER], "priority": 100},
    ])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    (denied_reply,) = _mcp_calls(dsn, deployment, ("filter", arguments, "json"))
    assert denied_reply.isError is False, denied_reply.content[0].text
    denied = json.loads(denied_reply.content[0].text)
    assert _node_ids(denied) == set()

    rc, out, err = _run(capsys, argv, as_json=True)
    assert rc == 0, err
    assert _node_ids(json.loads(out)) == set()
    assert _stable(json.loads(out)) == _stable(denied)

    # A reader the policy does not name is refused at the boundary on both.
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:not-in-the-policy")
    (stranger,) = _mcp_calls(dsn, deployment, ("filter", arguments, "json"))
    rc, out, err = _run(capsys, argv, as_json=True)
    assert stranger.isError is True
    assert rc == 2 and out == ""
    assert _stable(_last_json_line(err)) == _stable(json.loads(stranger.content[0].text))


# ── one command in a shell (PACT read-commands-shell-3666) ───────────────


def _record_pools_and_claims(monkeypatch) -> tuple[list, list]:
    """Record each pool the server builds and each connection claim it asks
    for; both still happen."""
    from okg.substrate.db import capacity
    from okg.substrate.mcp import server as server_module

    pools: list[tuple[str, int, int, Any]] = []
    claims: list[dict[str, Any]] = []
    build_pool = server_module._build_pool
    reserve = capacity.reserve_process_demand

    def recording_pool(**kwargs):
        pool = build_pool(**kwargs)
        pools.append((kwargs["name"], kwargs["min_size"], kwargs["max_size"], pool))
        return pool

    def recording_reserve(*args, **kwargs):
        granted = reserve(*args, **kwargs)
        claims.append({**kwargs, "granted": granted["_total"]})
        return granted

    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    monkeypatch.setattr(server_module, "_build_pool", recording_pool)
    monkeypatch.setattr(capacity, "reserve_process_demand", recording_reserve)
    return pools, claims


def _ledger(dsn: str, application_name: str) -> list[tuple[int, int]]:
    """The live rows of the host-wide ledger for one application name."""
    import psycopg

    with psycopg.connect(dsn) as conn:
        return [
            (int(pid), int(demand)) for pid, demand in conn.execute(
                "SELECT pid, declared_demand FROM okg.connection_reservations "
                "WHERE application_name = %s", (application_name,),
            )
        ]


def test_a_command_claims_the_two_connections_it_can_open_and_releases_them(
        graph, capsys, monkeypatch):
    """One connection from the start, a second while the freshness stamp is
    read inside the call, and a hard limit of two. The ledger holds a claim
    for exactly two while the command runs and nothing after it."""
    import os

    from okg.substrate.db import capacity
    from okg.substrate.mcp import server as server_module

    dsn, deployment = graph
    name = f"okg-mcp-{deployment}"
    pools, claims = _record_pools_and_claims(monkeypatch)
    during: list[list[tuple[int, int]]] = []
    call = ops.call_operator

    def call_and_read_the_ledger(*args, **kwargs):
        during.append(_ledger(dsn, name))
        return call(*args, **kwargs)

    monkeypatch.setattr(ops, "call_operator", call_and_read_the_ledger)

    for operator in ("filter", "search", "inspect"):
        rc, _out, err = _run(
            capsys, _argv(operator, dsn, deployment, CASES[operator][0]), as_json=True,
        )
        assert rc == 0, (operator, err)
        assert _ledger(dsn, name) == [], operator

    assert [
        (claim["application_name"], claim["demand"], claim["granted"],
         claim["heartbeat_interval_seconds"])
        for claim in claims
    ] == [(name, 2, 2, None)] * 3
    assert during == [[(os.getpid(), 2)]] * 3
    assert name not in capacity._process_reservation_names()
    assert not [key for key in capacity._RESERVATION_OWNERS if key[1] == name]
    assert [(pool_name, low, high) for pool_name, low, high, _pool in pools] == [
        ("okg-mcp", 1, server_module.MCP_CONNECTIONS_PER_WORKER)
    ] * 3
    for _name, _low, _high, pool in pools:
        # Connections the pool established over its life, read after it
        # closed; with a limit of two it cannot pass two unless one broke.
        stats = pool.get_stats()
        assert 1 <= stats.get("connections_num", 0) <= 2, stats
        assert stats.get("connections_errors", 0) == 0, stats


def test_a_full_ledger_refuses_the_command_by_name(graph, capsys):
    """Another process holds every connection the server can serve: the
    command asks for its two, is refused, and opens no pool."""
    import psycopg

    dsn, deployment = graph
    holder = ("okg-test-holder", 1)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.connection_reservations "
            "(application_name, pid, deployment_name, declared_demand) "
            "VALUES (%s, %s, %s, 1000000)", (*holder, deployment),
        )
    try:
        argv = _argv("filter", dsn, deployment, CASES["filter"][0])
        rc, out, err = _run(capsys, argv, as_json=False)
        assert rc == 2 and out == "", (rc, out, err)
        assert err.count("\n") == 1, err
        assert err.startswith("okg filter: error: BudgetError: "), err
        assert _ledger(dsn, f"okg-mcp-{deployment}") == []

        rc, out, err = _run(capsys, argv, as_json=True)
        assert rc == 2 and out == "", (rc, out, err)
        assert "BudgetError" in json.loads(err)["error"]["message"]
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                "DELETE FROM okg.connection_reservations "
                "WHERE application_name = %s AND pid = %s", holder,
            )
    rc, _out, err = _run(capsys, argv, as_json=False)
    assert rc == 0, err


def test_a_server_that_stays_up_keeps_its_pool_and_its_claim(graph, monkeypatch):
    """Built as `okg mcp-serve` builds it: the width and the claim it had."""
    from okg.substrate.db.concurrency_budget import connection_demand, width
    from okg.substrate.mcp import server as server_module

    dsn, deployment = graph
    name = f"okg-mcp-{deployment}"
    pools, claims = _record_pools_and_claims(monkeypatch)

    server = server_module.build_server(
        dsn=dsn, deployment_name=deployment, branch_name="default", allow_writes=False,
    )
    try:
        (claim,) = claims
        assert claim["application_name"] == name
        assert "demand" not in claim and "heartbeat_interval_seconds" not in claim
        assert claim["granted"] == connection_demand(include_mcp=True)["_total"]
        assert [demand for _pid, demand in _ledger(dsn, name)] == [claim["granted"]]
        assert [(pool_name, low, high) for pool_name, low, high, _pool in pools] == [
            ("okg-mcp", 2, width("mcp.read_pool"))
        ]
    finally:
        ops.close_server(server)
    assert _ledger(dsn, name) == []


def _unavailable_freshness(value: Any) -> list[Any]:
    """Every freshness block of a reply that is the `unavailable` marker."""
    found: list[Any] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if (
                key in ("source_freshness", "publish_freshness")
                and isinstance(item, dict)
                and "unavailable" in (item.get("status"), item.get("freshness"))
            ):
                found.append({key: item})
            found += _unavailable_freshness(item)
    elif isinstance(value, list):
        for item in value:
            found += _unavailable_freshness(item)
    return found


def test_every_command_stamps_real_freshness_and_never_waits_for_a_connection(
        graph, capsys, monkeypatch):
    """A read that could not get its second connection is not a failure: the
    server waits the pool's timeout, stamps the reply `freshness
    unavailable` and serves it. So a starved pool is caught here, by the
    stamp and by the time the call took, for every operator."""
    dsn, deployment = graph
    seconds: list[float] = []
    call = ops.call_operator

    def timed(*args, **kwargs):
        started = time.perf_counter()
        try:
            return call(*args, **kwargs)
        finally:
            seconds.append(time.perf_counter() - started)

    monkeypatch.setattr(ops, "call_operator", timed)
    for operator, (rest, _arguments) in CASES.items():
        argv = _argv(operator, dsn, deployment, rest)
        rc, out, err = _run(capsys, argv, as_json=True)
        if rc != 0:
            # Only `query` may be withheld from a surface.
            assert operator == "query" and rc == 2, (operator, err)
        else:
            reply = json.loads(out)
            assert _unavailable_freshness(reply) == [], operator
            # The stamp is there, and it is a real one: the age of the
            # publish this reply was read from.
            stamp = reply.get("publish_freshness")
            assert isinstance(stamp, dict), (operator, sorted(reply))
            assert isinstance(stamp.get("publish_age_seconds"), (int, float)), (operator, stamp)
            assert stamp.get("latest_published_at"), (operator, stamp)
        rc, out, err = _run(capsys, argv, as_json=False)
        assert "freshness unavailable" not in out + err, (operator, out[:400], err)
    assert len(seconds) == 2 * len(CASES)
    assert max(seconds) < CALL_SECONDS_BOUND, dict(zip(
        [f"{operator}/{form}" for operator in CASES for form in ("json", "text")],
        [round(value, 3) for value in seconds],
    ))


def test_okg_query_given_words_adds_the_search_hint_after_the_tools_refusal(
        graph, capsys, monkeypatch):
    """A real dispatch: the deployment from the environment, the words sent
    to the tool, the tool's own refusal, then one line."""
    dsn, deployment = graph
    monkeypatch.setenv("OKG_DEPLOYMENT", deployment)

    rc, out, err = _run(capsys, ["query", "--dsn", dsn, "recent pacts"], as_json=False)
    assert rc == 2 and out == "", (rc, out, err)
    lines = err.rstrip("\n").split("\n")
    assert lines[-1] == ops.TEXT_SEARCH_HINT, err
    assert len(lines) > 1 and ops.TEXT_SEARCH_HINT not in lines[:-1], err

    # SQL is never given the hint, served or refused.
    rc, out, err = _run(capsys, ["query", "--dsn", dsn, SQL], as_json=False)
    assert ops.TEXT_SEARCH_HINT not in out + err
    rc, out, err = _run(
        capsys, ["query", "--dsn", dsn, "select no_such_column from okg.v_nodes"],
        as_json=False,
    )
    assert rc == 2 and ops.TEXT_SEARCH_HINT not in err, err


def test_stderr_is_empty_when_served_and_only_the_refusal_when_refused(
        graph, capsys, monkeypatch):
    from okg.substrate.mcp.result_text import parse_status_line

    monkeypatch.delenv("OKG_READ_DEBUG", raising=False)
    dsn, deployment = graph
    for operator, (rest, _arguments) in CASES.items():
        for as_json in (True, False):
            rc, out, err = _run(capsys, _argv(operator, dsn, deployment, rest), as_json=as_json)
            if rc == 0:
                assert err == "" and out, (operator, as_json, err)
            else:
                # Only `query` may be withheld from a surface.
                assert operator == "query" and rc == 2 and out == "", (operator, err)

        argv = _argv(operator, dsn, deployment, ["--args", '{"nonsense": 1}'])
        rc, out, err = _run(capsys, argv, as_json=True)
        assert rc == 2 and out == "", (operator, out, err)
        # The whole stream is the one JSON reply: nothing before or after it.
        assert json.loads(err)["status"] == "refused", (operator, err)

        rc, out, err = _run(capsys, argv, as_json=False)
        assert rc == 2 and out == "", (operator, out, err)
        status = parse_status_line(err)
        if operator != "query" or status is not None:
            # The first line of stderr is the refusal's own status line.
            assert status is not None and status.status == "refused", (operator, err)
            assert status.tool == operator, (operator, err)


# ── a command as its own process: stderr, and every way it can end ───────
#
# Under pytest the MCP library finds logging already configured and installs
# no stderr handler of its own, and a signal reaches the test runner, not a
# command. A fresh interpreter is what a shell starts.


def _shell_environment(**changes: str | None) -> dict[str, str]:
    import os

    env = {
        key: value for key, value in os.environ.items()
        if key not in {"OKG_AGENT", "OKG_READ_DEBUG", "OKG_CODE_REPOS_ROOT"}
    }
    for key, value in changes.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    return env


def _default_signals() -> None:
    """In the child, before okg starts: the dispositions a shell gives a
    foreground command. A test runner started under `nohup`, or in the
    background, hands its children SIGHUP or SIGINT already ignored."""
    import signal

    for number in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        signal.signal(number, signal.SIG_DFL)


def _spawn(dsn: str, deployment: str, operator: str, rest: list[str], **env: str | None):
    import subprocess

    return subprocess.Popen(
        # The okg this test runs under, as every okg child is started.
        [*running_okg_argv(), *_argv(operator, dsn, deployment, rest)],
        env=_shell_environment(**env), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, preexec_fn=_default_signals,
        # Its own process group, so no signal it gets or sends reaches the
        # xdist worker (#703); the tests signal it by its pid.
        start_new_session=True,
    )


def _finish(child, *, timeout: float = 120) -> tuple[int, str, str]:
    try:
        out, err = child.communicate(timeout=timeout)
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate()
    return child.returncode, out, err


def _wait_for_the_claim(dsn: str, name: str, child) -> list[tuple[int, int]]:
    """The command's row in the ledger, as soon as it is there."""
    import psycopg

    claimed: list[tuple[int, int]] = []
    deadline = time.monotonic() + 120
    with psycopg.connect(dsn, autocommit=True) as conn:
        while not claimed and child.poll() is None and time.monotonic() < deadline:
            claimed = [
                (int(pid), int(demand)) for pid, demand in conn.execute(
                    "SELECT pid, declared_demand FROM okg.connection_reservations "
                    "WHERE application_name = %s", (name,),
                )
            ]
            if not claimed:
                time.sleep(0.01)
    return claimed


#: A call that lasts where `query` is served; where it is withheld from the
#: surface the signal lands in the rest of the start-up instead.
SLOW_QUERY = ["select pg_sleep(15)"]


def test_a_served_command_as_its_own_process_prints_nothing_and_leaves_no_claim(graph):
    dsn, deployment = graph
    code, out, err = _finish(_spawn(dsn, deployment, "filter", CASES["filter"][0]))
    assert code == 0, (out[-400:], err[-2000:])
    assert err == ""
    assert out.split("\n", 1)[0].startswith("filter ok"), out[:200]
    assert _ledger(dsn, f"okg-mcp-{deployment}") == []


def test_a_refused_command_as_its_own_process_prints_the_refusal_and_leaves_no_claim(graph):
    from okg.substrate.mcp.result_text import parse_status_line

    dsn, deployment = graph
    code, out, err = _finish(
        _spawn(dsn, deployment, "filter", ["--args", '{"nonsense": 1}']),
    )
    assert code == 2 and out == "", (code, out[-400:], err[-2000:])
    status = parse_status_line(err)
    assert status is not None and status.status == "refused", err
    assert _ledger(dsn, f"okg-mcp-{deployment}") == []


def test_a_command_that_fails_after_its_claim_leaves_no_claim(graph, capsys, monkeypatch):
    """Exit 1 after the claim was made, in the two places it can happen: the
    rest of the server build, and the call. In this process: nothing a
    caller can type makes a real command fail after its claim."""
    from okg.substrate.mcp import server as server_module

    dsn, deployment = graph
    name = f"okg-mcp-{deployment}"
    argv = _argv("filter", dsn, deployment, CASES["filter"][0])
    seen: list[list[tuple[int, int]]] = []
    build_pool = server_module._build_pool
    call = ops.call_operator

    def no_pool(**kwargs):
        seen.append(_ledger(dsn, name))
        raise RuntimeError("the pool could not be built")

    def no_call(*args, **kwargs):
        seen.append(_ledger(dsn, name))
        raise RuntimeError("the call could not be made")

    monkeypatch.setattr(server_module, "_build_pool", no_pool)
    rc, out, err = _run(capsys, argv, as_json=False)
    assert rc == 1 and out == "" and "the pool could not be built" in err, (rc, err)
    assert _ledger(dsn, name) == []

    monkeypatch.setattr(server_module, "_build_pool", build_pool)
    monkeypatch.setattr(ops, "call_operator", no_call)
    rc, out, err = _run(capsys, argv, as_json=False)
    assert rc == 1 and out == "" and "the call could not be made" in err, (rc, err)
    assert _ledger(dsn, name) == []
    # Both failures came after the claim was in the ledger.
    assert [[demand for _pid, demand in rows] for rows in seen] == [[2], [2]]

    monkeypatch.setattr(ops, "call_operator", call)
    rc, _out, err = _run(capsys, argv, as_json=False)
    assert rc == 0, err


@pytest.mark.parametrize(
    ("signal_name", "exit_codes"),
    [
        # `timeout 2 okg query ...`, and a harness that stops a process group.
        ("SIGTERM", {143}),
        # The terminal or the ssh session went away; passed on as SIGTERM.
        ("SIGHUP", {143}),
        # Ctrl-C. The command says it was interrupted and exits 130, as a
        # shell reports a program Ctrl-C ended.
        ("SIGINT", {130}),
    ],
)
def test_a_command_told_to_stop_releases_its_claim(graph, signal_name, exit_codes):
    """Without a handler SIGTERM and SIGHUP end the process at once, with no
    cleanup, and its claim counts against the host for 300 s. Whatever
    stops it, stderr holds no traceback and no path of the host."""
    import signal

    dsn, deployment = graph
    name = f"okg-mcp-{deployment}"
    child = _spawn(dsn, deployment, "query", SLOW_QUERY)
    sent: float | None = None
    try:
        claimed = _wait_for_the_claim(dsn, name, child)
        assert claimed == [(child.pid, 2)], (claimed, child.poll())
        child.send_signal(getattr(signal, signal_name))
        sent = time.monotonic()
    finally:
        code, out, err = _finish(child, timeout=90)
    ended = time.monotonic()
    assert code in exit_codes, (code, out[-400:], err[-2000:])
    assert _ledger(dsn, name) == []
    assert "Traceback" not in err, err[-3000:]
    assert re.search(r"(?<![\w:/.@<-])/[\w.@+~-]+/", err) is None, err[-3000:]
    if signal_name == "SIGINT":
        # Last, and once; a warning the pool logs on closing is one line too.
        interrupted = "ERROR [read_command_failed]: okg query was interrupted"
        assert err.splitlines()[-1:] == [interrupted], err[-3000:]
        assert err.count(interrupted) == 1, err[-3000:]
    else:
        # The command unwound by itself: the stop guard ends a command that
        # is still alive 5 s after SIGTERM, and writes its marker when it does.
        assert "did not finish" not in err, err[-3000:]
        assert sent is not None and ended - sent < 3.0, (ended - sent, err[-2000:])


def test_a_killed_command_leaves_its_claim_until_the_ledgers_stale_horizon(
        graph, capsys):
    """SIGKILL cannot be handled. The row of two connections stays; nothing
    looks at whether its pid is alive; the next claim sweeps it once it is
    300 s old. The horizon is fixed inside the claim function, so the test
    ages the row instead of waiting."""
    import signal

    import psycopg

    dsn, deployment = graph
    name = f"okg-mcp-{deployment}"
    argv = _argv("filter", dsn, deployment, CASES["filter"][0])
    child = _spawn(dsn, deployment, "query", SLOW_QUERY)
    try:
        claimed = _wait_for_the_claim(dsn, name, child)
        assert claimed == [(child.pid, 2)], (claimed, child.poll())
        child.send_signal(signal.SIGKILL)
    finally:
        code, _out, _err = _finish(child, timeout=60)
    assert code == -signal.SIGKILL
    assert _ledger(dsn, name) == [(child.pid, 2)]

    # Another command runs beside the dead one's row and does not remove it.
    rc, _out, err = _run(capsys, argv, as_json=False)
    assert rc == 0, err
    assert _ledger(dsn, name) == [(child.pid, 2)]

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.connection_reservations "
            "SET heartbeat_at = now() - interval '301 seconds' "
            "WHERE application_name = %s AND pid = %s", (name, child.pid),
        )
    rc, _out, err = _run(capsys, argv, as_json=False)
    assert rc == 0, err
    assert _ledger(dsn, name) == []


def test_a_command_whose_reader_went_away_leaves_no_claim(graph):
    """`okg filter ... | head -1`, or a parent that closed the pipe: the
    reply cannot be written, and the claim was already given back."""
    dsn, deployment = graph
    child = _spawn(dsn, deployment, "filter", CASES["filter"][0])
    child.stdout.close()
    child.stdout = None
    try:
        child.wait(timeout=120)
    finally:
        if child.poll() is None:
            child.kill()
        err = child.stderr.read()
        child.stderr.close()
    assert "Traceback" not in err, err[-2000:]
    assert _ledger(dsn, f"okg-mcp-{deployment}") == []


def test_the_two_notices_of_a_real_start_are_not_printed(graph):
    """What a shell run printed before most replies: the connection claim
    notice, and the warning that the upload tool is not provisioned, whose
    cause names the deployment's folder when the registry's code checkouts
    cannot be located. The debug run shows both are really emitted here."""
    dsn, deployment = graph
    registry = Path(deployment) / "source_registry.yaml"
    assert registry.is_file(), registry
    before = registry.read_text(encoding="utf-8")
    registry.write_text(
        "sources: {}\n"
        "code_repos:\n"
        "  base: ${OKG_CODE_REPOS_ROOT}\n"
        "  repos:\n"
        "    - slug: example\n"
        "      dir: example\n",
        encoding="utf-8",
    )
    try:
        code, out, err = _finish(_spawn(
            dsn, deployment, "filter", CASES["filter"][0], OKG_READ_DEBUG="1",
        ))
        assert code == 0, (out[-400:], err[-3000:])
        shown = " ".join(err.split())
        assert "upload tool not provisioned for deployment" in shown, err[-3000:]
        assert "through the MCP reservation functions" in shown, err[-3000:]
        # The shell run's "OKG_CODE_REPOS_ROOT is unset" line (563 of 1,682
        # outputs) is this warning's cause: the registry's code_repos.base.
        assert "OKG_CODE_REPOS_ROOT" in shown, err[-3000:]

        code, out, err = _finish(_spawn(dsn, deployment, "filter", CASES["filter"][0]))
        assert code == 0, (out[-400:], err[-2000:])
        assert err == ""

        # A refused command prints only its refusal: no such line either.
        code, out, err = _finish(
            _spawn(dsn, deployment, "filter", ["--args", '{"nonsense": 1}']),
        )
        assert code == 2 and out == "", (code, out[-400:], err[-2000:])
        assert "OKG_CODE_REPOS_ROOT" not in err and "upload tool" not in err, err
    finally:
        registry.write_text(before, encoding="utf-8")


def test_a_command_line_select_is_rewritten_as_an_mcp_select_is(graph, capsys):
    """The command calls the registered tool through an MCP client, so a
    `--select` meets the same rewrite (#3666 step 3a): a grep list is cut to
    the selected row keys and the reply names the field left out."""
    dsn, deployment = graph
    select = ["node_id", "matches", "excerpt"]
    rest = [TERM, "--method", "grep", "--view", "list", "--limit", "3",
            "--select", json.dumps(select)]
    arguments = {"query": TERM, "method": "grep", "view": "list", "limit": 3, "select": select}
    args = _build_parser().parse_args(_argv("search", dsn, deployment, rest))
    assert ops.search_operator_arguments(args) == arguments
    (result,) = _mcp_calls(dsn, deployment, ("search", arguments, "json"))
    reply = json.loads(result.content[0].text)
    assert not result.isError, reply
    assert [record["sent"] for record in reply["adjusted"]] == ["select `excerpt`"]
    assert reply["results"] and all(
        "matches" in row and "first_match" not in row for row in reply["results"])
    rc, out, err = _run(capsys, _argv("search", dsn, deployment, rest), as_json=True)
    assert rc == 0, (out, err)
    assert _stable(json.loads(out)) == _stable(reply)


# ── where a command's time goes (PACT read-commands-trim-3666) ───────────

PHASE_PROBE = Path(__file__).with_name("_read_phase_probe.py")


def _phases(
    dsn: str, deployment: str, operator: str, rest: list[str], out: Path, **env: str,
) -> dict:
    """One command in a fresh process, with each start-up step timed
    (`_read_phase_probe.py`) and `OKG_READ_DEBUG=1`'s own phase line."""
    import subprocess
    import sys

    started = time.perf_counter()
    child = subprocess.run(
        [sys.executable, "-P", str(PHASE_PROBE), *_argv(operator, dsn, deployment, rest)],
        env=_shell_environment(OKG_READ_DEBUG="1", OKG_READ_PHASE_OUT=str(out), **env),
        capture_output=True, text=True, timeout=300, start_new_session=True,
    )
    wall = time.perf_counter() - started
    assert out.is_file(), child.stderr[-3000:]
    measured = json.loads(out.read_text(encoding="utf-8"))
    assert measured["rc"] == child.returncode, child.stderr[-3000:]
    line = [row for row in child.stderr.splitlines() if " timing: " in row]
    marks = Path(f"{out}.exit").read_text(encoding="utf-8").split()
    measured.update(
        wall=wall, debug_line=line[-1] if line else "",
        exit=dict(zip(marks[::2], map(float, marks[1::2]))),
    )
    return measured


#: Rows of the exit, from the probe's marks: (label, from, to). `total` is
#: the end of the command, `wall` the end of the process.
_EXIT_ROWS = (
    ("exit: thread exit hooks (executor joins)", "total", "thread_hooks_done"),
    ("exit: joins of other threads", "thread_hooks_done", "threads_joined"),
    ("exit: atexit handlers", "threads_joined", "atexit_done"),
    ("interpreter start-up and finalization", "atexit_done", "wall"),
)


def _phase_table(runs: dict[str, dict]) -> str:
    labels: list[str] = []
    for run in runs.values():
        labels += [label for label in run["phases"] if label not in labels]
    names = list(runs)
    rows = ["| step | " + " | ".join(names) + " |", "|---" * (len(names) + 1) + "|"]
    exits = {label: (start, end) for label, start, end in _EXIT_ROWS}
    for label in [*labels, *exits, "wall (process start to exit)"]:
        cells = []
        for name in names:
            run = runs[name]
            if label.startswith("wall"):
                cells.append(f"{run['wall'] * 1000:.0f}")
                continue
            if label in exits:
                at = {**run["exit"], "total": run["total"], "wall": run["wall"]}
                start, end = exits[label]
                cells.append(
                    f"{(at[end] - at[start]) * 1000:.0f}"
                    if start in at and end in at else "?"
                )
                continue
            seconds, count = run["phases"].get(label, (None, 0))
            cells.append(
                "-" if seconds is None
                else f"{seconds * 1000:.0f}" + (f" ({count}x)" if count > 1 else "")
            )
        rows.append(f"| {label} | " + " | ".join(cells) + " |")
    rows += [f"{name}: {runs[name]['debug_line']}" for name in names]
    return "\n".join(rows)


def test_where_a_command_spends_its_time(graph, tmp_path):
    """Each operator once, served, and one refused call, each in a fresh
    process: milliseconds per start-up step, printed (run with -s). What
    the trim is measured by; the assertions hold only what each command
    must and must not do, never a time."""
    import subprocess
    import sys

    dsn, deployment = graph
    runs: dict[str, dict] = {}
    for operator, (rest, _arguments) in CASES.items():
        runs[operator] = _phases(dsn, deployment, operator, rest, tmp_path / f"{operator}.json")
        assert runs[operator]["rc"] in ((0, 2) if operator == "query" else (0,)), operator
    runs["refused"] = _phases(
        dsn, deployment, "filter", ["--args", '{"nonsense": 1}'], tmp_path / "refused.json",
    )
    assert runs["refused"]["rc"] == 2
    # The same filter, ended by os._exit after the last atexit handler: what
    # the interpreter's own finalization costs.
    runs["filter, no finalization"] = _phases(
        dsn, deployment, "filter", CASES["filter"][0], tmp_path / "hard.json",
        OKG_READ_PHASE_HARD_EXIT="0",
    )
    assert runs["filter, no finalization"]["rc"] == 0

    help_started = time.perf_counter()
    subprocess.run(
        [*running_okg_argv(), "--help"], env=_shell_environment(),
        capture_output=True, check=True, timeout=120, start_new_session=True,
    )
    help_wall = time.perf_counter() - help_started
    import_started = time.perf_counter()
    subprocess.run(
        [sys.executable, "-P", "-c", "import okg.substrate.cli.main"],
        env=_shell_environment(), check=True, timeout=120, start_new_session=True,
    )
    import_wall = time.perf_counter() - import_started
    bare_started = time.perf_counter()
    subprocess.run(
        [sys.executable, "-P", "-c", "pass"], check=True, timeout=120,
        start_new_session=True,
    )
    bare_wall = time.perf_counter() - bare_started
    print(
        "\nPHASES (milliseconds)\n" + _phase_table(runs)
        + f"\nokg --help: {help_wall * 1000:.0f} ms"
        + f"; import of the command line alone: {import_wall * 1000:.0f} ms"
        + f"; an interpreter that does nothing: {bare_wall * 1000:.0f} ms\n"
    )
    for name, run in runs.items():
        assert run["phases"]["command"][1] == 1, name
        # The trim (PACT read-commands-trim-3666): no command loads DBOS
        # (the trace span read its identity through it), and only `inspect`
        # reads the dataset card's facts.
        assert run["dbos_loaded"] is False, name
        assert ("server: dataset card facts" in run["phases"]) is (name == "inspect"), name


# ── shell forms and pipe output (#3666 step S2, PACT read-shell-forms-3666) ──

DOCS = {"kind": "nodes", "subtype": "doc"}


def _doc_ids(dsn: str, deployment: str) -> list[str]:
    """Every doc id, read page by page from an MCP client."""
    ids: list[str] = []
    offset = 0
    while True:
        (reply,) = _mcp_calls(dsn, deployment, (
            "filter", {"input": DOCS, "predicate": {}, "limit": 1000, "offset": offset}, "json",
        ))
        assert not reply.isError, reply.content[0].text[:600]
        page = json.loads(reply.content[0].text)
        ids += [row["node_id"] for row in page["items"]]
        if page.get("next_offset") is None:
            return sorted(ids)
        offset = page["next_offset"]


def _lines(text: str) -> list[str]:
    return [line for line in text.splitlines() if line]


def test_all_reads_every_page_of_the_set_once(graph, capsys):
    dsn, deployment = graph
    expected = _doc_ids(dsn, deployment)
    assert len(expected) > 100
    rest = ["--input", json.dumps(DOCS), "--predicate", "{}", "--limit", "25"]
    rc, out, err = _run(capsys, _argv("filter", dsn, deployment, [*rest, "--all", "--ids"]),
                        as_json=False)
    assert rc == 0, err
    ids = _lines(out)
    assert len(ids) == len(set(ids)) and sorted(ids) == expected
    pages = -(-len(expected) // 25)
    assert _lines(err)[0].startswith("filter ok"), err
    assert _lines(err)[-1] == f"okg filter --all: {len(expected)} rows in {pages} pages"

    # --all alone prints JSON Lines: the same rows.
    rc, out, err = _run(capsys, _argv("filter", dsn, deployment, [*rest, "--all"]), as_json=False)
    assert rc == 0, err
    assert sorted(json.loads(line)["node_id"] for line in _lines(out)) == expected


def test_jsonl_rows_are_the_rows_of_the_json_reply(graph, capsys):
    dsn, deployment = graph
    rest = ["--input", json.dumps(DOCS), "--predicate", "{}", "--limit", "7",
            "--select", "node_id,subtype,attrs.path"]
    rc, out, err = _run(capsys, _argv("filter", dsn, deployment, rest), as_json=True)
    assert rc == 0, err
    reply_rows = json.loads(out)["items"]
    assert len(reply_rows) == 7
    rc, out, err = _run(capsys, _argv("filter", dsn, deployment, [*rest, "--jsonl"]),
                        as_json=False)
    assert rc == 0, err
    rows = [json.loads(line) for line in _lines(out)]
    assert rows == reply_rows
    # The selected fields, and the provenance the tool puts on every row.
    keys = {key for row in rows for key in row}
    assert {"node_id", "subtype", "attrs_path"} <= keys
    assert keys - {"node_id", "subtype", "attrs_path"} <= {"provenance"}, keys
    # The status line is on stderr, not among the rows.
    assert _lines(err)[0].startswith("filter ok"), err


def test_where_terms_are_the_json_condition(graph, capsys):
    dsn, deployment = graph
    as_terms = ["--input", json.dumps(DOCS), "--where", "attrs.path=docs/001.md"]
    as_object = ["--input", json.dumps(DOCS), "--where", json.dumps({"attrs.path": "docs/001.md"})]
    rc, out_terms, err = _run(capsys, _argv("filter", dsn, deployment, as_terms), as_json=True)
    assert rc == 0, err
    rc, out_json, err = _run(capsys, _argv("filter", dsn, deployment, as_object), as_json=True)
    assert rc == 0, err
    assert _stable(json.loads(out_terms)) == _stable(json.loads(out_json))
    assert _node_ids(json.loads(out_terms)) == {"doc:001"}

    rc, out, err = _run(capsys, _argv("filter", dsn, deployment, [
        "--type", "doc", "--where", "attrs.path~docs/00", "--ids", "--all",
    ]), as_json=False)
    assert rc == 0, err
    assert sorted(_lines(out)) == [f"doc:{i:03d}" for i in range(10)]
    assert "! adjusted: --type doc read as input" in err

    rc, out, err = _run(capsys, _argv("search", dsn, deployment, [
        TERM, "--where", "type=doc", "--limit", "5", "--ids",
    ]), as_json=False)
    assert rc == 0, err
    ids = _lines(out)
    assert len(ids) == 5 and all(node_id.startswith("doc:") for node_id in ids), ids


def test_ids_from_standard_input(graph, capsys, monkeypatch):
    dsn, deployment = graph
    ids = _doc_ids(dsn, deployment)[:60]
    piped = "\n".join(ids[:30]) + "\n" + "\n".join(
        json.dumps({"node_id": node_id, "subtype": "doc"}) for node_id in ids[30:]
    ) + "\n"

    # inspect reads them through the batch read: 50, then 10.
    monkeypatch.setattr("sys.stdin", io.StringIO(piped))
    rc, out, err = _run(capsys, _argv("inspect", dsn, deployment, [
        "-", "--select", "node_id,subtype", "--all", "--jsonl",
    ]), as_json=False)
    assert rc == 0, err
    records = [json.loads(line) for line in _lines(out)]
    assert [record["node_id"] for record in records] == ids
    assert all(record.get("status") == "ok" for record in records), records[:2]

    # filter as node_id any of.
    monkeypatch.setattr("sys.stdin", io.StringIO(piped))
    rc, out, err = _run(capsys, _argv("filter", dsn, deployment, [
        "--stdin", "--type", "doc", "--ids", "--all",
    ]), as_json=False)
    assert rc == 0, err
    assert sorted(_lines(out)) == ids

    # expand, one call per id.
    monkeypatch.setattr("sys.stdin", io.StringIO("doc:000\ndoc:001\n"))
    rc, out, err = _run(capsys, _argv("expand", dsn, deployment, [
        "-", "--edge-type", "contains", "--ids",
    ]), as_json=False)
    assert rc == 0, err
    assert sorted(_lines(out)) == sorted(
        f"doc_chunk:{i:03d}-{j}" for i in range(2) for j in range(3)
    )


def test_a_refusals_retry_line_run_as_given_is_served(graph, capsys, monkeypatch):
    """The `retry:` line on stderr is a command that runs as printed: the
    deployment as typed, the DSN from the environment, never printed."""
    dsn, deployment = graph
    monkeypatch.setenv("OKG_DSN", dsn)
    # The tool's own refusal: an operator written as a key. The grammar has
    # one reading for it and offers it as the corrected call.
    argv = ["filter", "--deployment", deployment, "--input", json.dumps(DOCS),
            "--where", '{"attrs.path": {"eq": "docs/001.md"}}', "--ids"]
    for printed in ("text", "ids"):
        run = argv if printed == "ids" else argv[:-1]
        capsys.readouterr()
        rc = main(run)
        out, err = capsys.readouterr()
        assert rc == 2 and out == "", (printed, out, err)
        (line,) = [line for line in err.splitlines() if line.startswith("retry: okg ")]
        assert dsn not in line
        retry = shlex.split(line[len("retry: "):])
        assert retry[:2] == ["okg", "filter"]
        assert ("--ids" in retry) is (printed == "ids")
        rc = main(retry[1:])
        out, err = capsys.readouterr()
        assert rc == 0, err
        assert "doc:001" in out and "doc:002" not in out, out


def test_a_where_refusals_retry_keeps_type_and_stdin_and_is_served(
        graph, capsys, monkeypatch):
    """A refused --where term's retry line is the whole command as typed,
    --type, --stdin and the output flags included; run as printed, with the
    same ids piped in, it is served."""
    dsn, deployment = graph
    monkeypatch.setenv("OKG_DSN", dsn)
    piped = "doc:001\ndoc:002\ndoc:003\n"
    argv = ["filter", "--deployment", deployment, "--stdin", "--type", "doc",
            "--where", "attrs.path=='docs/001.md'", "--ids", "--all"]
    monkeypatch.setattr("sys.stdin", io.StringIO(piped))
    capsys.readouterr()
    rc = main(argv)
    out, err = capsys.readouterr()
    assert rc == 2 and out == "", (out, err)
    (line,) = [line for line in err.splitlines() if line.startswith("retry: okg ")]
    retry = shlex.split(line[len("retry: "):])
    for flag in ("--stdin", "--type", "--ids", "--all"):
        assert flag in retry, (flag, line)
    monkeypatch.setattr("sys.stdin", io.StringIO(piped))
    rc = main(retry[1:])
    out, err = capsys.readouterr()
    assert rc == 0, err
    assert _lines(out) == ["doc:001"]


def test_exit_codes_served_refused_and_a_word_that_is_no_subtype(graph, capsys):
    dsn, deployment = graph
    rc, out, err = _run(capsys, _argv("filter", dsn, deployment, [
        "--input", "doc", "--predicate", "{}", "--ids", "--all",
    ]), as_json=False)
    assert rc == 0, err
    assert sorted(_lines(out)) == _doc_ids(dsn, deployment)
    assert "! adjusted: --input doc read as input" in err

    rc, out, err = _run(capsys, _argv("filter", dsn, deployment, ["--input", "docs"]), as_json=False)
    assert rc == 2 and out == "", (out, err)
    assert "filter refused read_input_unresolved" in err and "doc" in err

    rc, out, err = _run(capsys, _argv("filter", dsn, deployment, ["--input", "docs"]), as_json=True)
    assert rc == 2 and out == ""
    assert _last_json_line(err)["error"]["code"] == "read_input_unresolved"

    # A tool's refusal in a pipe mode: exit 2, the refusal on stderr.
    rc, out, err = _run(capsys, _argv("filter", dsn, deployment, [
        "--input", json.dumps(DOCS), "--predicate", "{}", "--args", '{"nonsense": 1}', "--jsonl",
    ]), as_json=False)
    assert rc == 2 and out == "", (out, err)
    assert "filter refused" in err


# ── okg inspect texts - (#3666 step S5, PACT read-shell-texts-3666) ──────

#: The word the pipe below counts in every text.
WORD = "zirconium"


@pytest.fixture
def texts_graph(clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """Twelve reports and one memo with chunked texts:
    ``(dsn, deployment, times WORD occurs in the reports' texts)``.

    One report has 30 chunks of about 3,000 characters, larger than a
    call's default budget (64 KiB), so its text arrives over several pages;
    the twelve reports are two batches of the `texts` target."""
    from okg.substrate.library.sources.base import EdgeFact
    from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes
    from tests.substrate.mcp.test_page_next_db import CHUNK, DOC

    monkeypatch.delenv("OKG_AGENT", raising=False)
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes: list[Any] = []
    edges: list[Any] = []

    def add(doc_id: str, title: str, texts: list[str]) -> int:
        nodes.append(node(doc_id, "document", f"{title} record", title=title))
        for i, text in enumerate(texts):
            chunk_id = f"{doc_id}:chunk:{i}"
            nodes.append(node(chunk_id, "document_chunk", text, chunk_index=i,
                              chunk_ordinal=i, chunk_total=len(texts)))
            edges.append(EdgeFact(
                src=doc_id, dst=chunk_id, edge_type="contains",
                source_record_id={"id": f"{doc_id}|{i}"}, source_revision={"v": 1},
            ))
        return sum(text.count(WORD) for text in texts)

    count = add("doc:big", "Report big", [
        f"Part {i}: " + f"filler{i} " * 170 + f"{WORD} " + f"filler{i} " * 170 for i in range(30)
    ])
    for k in range(11):
        count += add(f"doc:report-{k:02d}", f"Report {k:02d}", [
            f"Report {k} part {j}: " + f"{WORD} alloy. " * (k + 1) for j in range(1 + k % 3)
        ])
    add("doc:memo", "Memo", [f"{WORD} " * 50])
    publish_nodes(
        clean_runtime, deployment, subtypes=[DOC, CHUNK],
        narrowings=[("document", "contains", "document_chunk")], nodes=nodes, edges=edges,
    )
    return clean_runtime, deployment, count


def _one_text(dsn: str, deployment: str, node_id: str, lines: Any = None) -> str:
    """One record's text from an MCP client: single text reads at the
    largest budget, following `next`, the pieces joined."""
    from mcp.shared.memory import create_connected_server_and_client_session

    server = Served(dsn, deployment)
    parts: list[str] = []

    async def drive() -> None:
        target: dict[str, Any] = {"kind": "text", "node_id": node_id}
        if lines is not None:
            target["lines"] = lines
        arguments: dict[str, Any] = {"target": target, "max_bytes": 262144}
        async with create_connected_server_and_client_session(server.mcp) as client:
            for _ in range(50):
                result = await client.call_tool(
                    "inspect", arguments, meta={"okg/result_format": "json"},
                )
                assert not result.isError, result.content[0].text[:600]
                page = json.loads(result.content[0].text)
                parts.extend(row["text"] for row in page["items"])
                if not page.get("continuation_token"):
                    return
                arguments = {"next": page["continuation_token"]}
            raise AssertionError("the text read did not end")

    try:
        anyio.run(drive)
    finally:
        server.close()
    return "".join(parts)


def test_the_texts_of_a_set_through_one_pipe(texts_graph, tmp_path):
    """The pipe an agent wants, run by a shell exactly as typed: the ids of
    a set, every text of them, a script that counts a word."""
    import os
    import shutil
    import subprocess
    import sys

    dsn, deployment, count = texts_graph
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    # `okg` and `python3` on PATH are the okg and the interpreter under test.
    for name, argv in (("okg", running_okg_argv()), ("python3", [sys.executable])):
        shim = bin_dir / name
        shim.write_text(f'#!/bin/sh\nexec {shlex.join(argv)} "$@"\n', encoding="utf-8")
        shim.chmod(0o755)
    program = (
        "import json, re, sys; records = [json.loads(line) for line in sys.stdin]; "
        f"print(len(records), sum(len(re.findall(r'{WORD}', r['text'])) for r in records), "
        "sum(r['state'] != 'delivered' for r in records))"
    )
    pipeline = (
        "okg filter --type document --where 'attrs.title~Report' --ids --all"
        " | okg inspect texts -"
        f" | python3 -c {shlex.quote(program)}"
    )
    bash = shutil.which("bash")
    assert bash is not None
    env = _shell_environment(
        OKG_DSN=dsn, OKG_DEPLOYMENT=deployment,
        PATH=f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
    )
    child = subprocess.Popen(
        [bash, "-c", "set -o pipefail; " + pipeline],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )
    code, out, err = _finish(child, timeout=300)
    assert code == 0, (out, err[-3000:])
    # Twelve reports, every one delivered whole, the word counted in all.
    assert out.split() == ["12", str(count), "0"], (out, err[-3000:])
    lines = _lines(err)
    assert "okg filter --all: 12 rows in 1 pages" in lines, err[-3000:]
    (summary,) = [line for line in lines if line.startswith("okg inspect texts:")]
    calls = int(re.fullmatch(r"okg inspect texts: 12 texts in (\d+) calls", summary).group(1))
    # Two batches, and the large text took pages of its own.
    assert calls > 2, summary
    assert dsn not in err


def test_inspect_texts_gives_each_whole_text_as_one_read_does(texts_graph, capsys, monkeypatch):
    dsn, deployment, _count = texts_graph
    ids = ["doc:big", "doc:report-04", "doc:missing"]
    monkeypatch.setattr("sys.stdin", io.StringIO("\n".join(ids) + "\n"))
    rc, out, err = _run(capsys, _argv("inspect", dsn, deployment, [
        "texts", "-", "--max-bytes", "4096",
    ]), as_json=False)
    # Every text printed; exit 2, as one id was refused.
    assert rc == 2, err
    records = [json.loads(line) for line in _lines(out)]
    assert [record["node_id"] for record in records] == ids
    for record in records[:2]:
        assert record["state"] == "delivered" and "cut" not in record, record
        assert record["subtype"] == "document" and "coverage" in record
        # Cut by many 4 KiB pages, put back together: the text one read of
        # the whole record gives, byte for byte.
        assert record["text"] == _one_text(dsn, deployment, record["node_id"])
    assert records[0]["chunks"] == 30
    assert records[0]["text"].count(WORD) == 30
    assert records[2]["state"] == "refused" and records[2]["text"] is None
    assert records[2]["error"]["code"] == "node_not_found"
    assert "! okg inspect texts: doc:missing refused node_not_found" in err
    calls = int(re.search(r"in (\d+) calls", _lines(err)[-1]).group(1))
    assert calls > 20, err[-2000:]

    # --lines reads those lines of each text, as one text read does.
    monkeypatch.setattr("sys.stdin", io.StringIO("doc:report-04\ndoc:big\n"))
    rc, out, err = _run(capsys, _argv("inspect", dsn, deployment, [
        "texts", "-", "--lines", "1-2",
    ]), as_json=False)
    assert rc == 0, err
    records = [json.loads(line) for line in _lines(out)]
    for record in records:
        assert record["state"] == "delivered" and record["lines"]["total"] >= 1, record
        assert record["text"] == _one_text(dsn, deployment, record["node_id"], [[1, 2]])


# ── the pipeline lead's examples and the pipe hint (#3666) ───────────────

#: The fixture of the pipeline examples: ticket:T-1 .. ticket:T-30.
TICKET_STATUSES = ("Waiting", "Open", "In Progress", "Closed", "Open")


def _ticket_status(i: int) -> str:
    return TICKET_STATUSES[i % 5]


def _ticket_text(i: int) -> str:
    return f"Ticket {i}: the transfer " + ("hit a timeout." if i % 3 == 0 else "finished.")


@pytest.fixture
def tickets_graph(clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """Thirty tickets with a status and a one-chunk text; T-2 .. T-6
    mention T-1 and T-1 mentions T-7: ``(dsn, deployment)``."""
    from okg.substrate.library.sources.base import EdgeFact
    from tests.pytest.catalog_fixture_builders import SubtypeSpec
    from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes
    from tests.substrate.mcp.test_page_next_db import CHUNK

    monkeypatch.delenv("OKG_AGENT", raising=False)
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes = []
    edges = []
    for i in range(1, 31):
        ticket = f"ticket:T-{i}"
        nodes.append(node(ticket, "ticket", f"Ticket {i}", status=_ticket_status(i),
                          summary=f"Ticket {i}"))
        # A record's text is its chunks, as the text read serves it.
        nodes.append(node(f"{ticket}:chunk:0", "document_chunk", _ticket_text(i),
                          chunk_index=0, chunk_ordinal=0, chunk_total=1))
        edges.append(EdgeFact(
            src=ticket, dst=f"{ticket}:chunk:0", edge_type="contains",
            source_record_id={"id": f"{ticket}|0"}, source_revision={"v": 1},
        ))
    links = [(f"ticket:T-{i}", "ticket:T-1") for i in range(2, 7)] + [("ticket:T-1", "ticket:T-7")]
    edges += [
        EdgeFact(src=src, dst=dst, edge_type="mentions",
                 source_record_id={"id": f"{src}|{dst}"}, source_revision={"v": 1})
        for src, dst in links
    ]
    ticket_spec = SubtypeSpec("ticket", metadata={
        "searchable_text_fields": ["summary", "text"],
        "passage_rollup": {"edge_type": "contains", "child_subtype": "document_chunk"},
    })
    publish_nodes(
        clean_runtime, deployment, subtypes=[ticket_spec, CHUNK],
        narrowings=[("ticket", "mentions", "ticket"), ("ticket", "contains", "document_chunk")],
        nodes=nodes, edges=edges,
    )
    # A trusted source: its values reach the program bare, as a deployment's
    # declared sources do.
    _trust(clean_runtime, deployment, MAIN)
    return clean_runtime, deployment


def _bash(dsn: str, deployment: str, tmp_path: Path, script: str) -> tuple[int, str, str]:
    """`script` run by bash, with pipefail, as an agent's shell runs it:
    `okg` and `python3` on PATH are the okg and the interpreter under test,
    and the deployment and DSN come from the environment."""
    import os
    import shutil
    import subprocess
    import sys

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for name, argv in (("okg", running_okg_argv()), ("python3", [sys.executable])):
        shim = bin_dir / name
        shim.write_text(f'#!/bin/sh\nexec {shlex.join(argv)} "$@"\n', encoding="utf-8")
        shim.chmod(0o755)
    bash = shutil.which("bash")
    assert bash is not None
    env = _shell_environment(
        OKG_DSN=dsn, OKG_DEPLOYMENT=deployment,
        PATH=f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
    )
    child = subprocess.Popen(
        [bash, "-c", "set -o pipefail; " + script],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )
    return _finish(child, timeout=300)


def _printed_examples() -> dict[str, str]:
    """question -> the example's lines exactly as the lead prints them."""
    examples: dict[str, str] = {}
    for block in ops.pipeline_text().split("\n  # ")[1:]:
        question, command = block.split("\n", 1)
        examples[question] = command
    assert list(examples) == [question for question, _command in ops.PIPELINE_EXAMPLES]
    return examples


def test_the_pipeline_examples_run_as_printed(tickets_graph, tmp_path):
    """Each example of `okg guide` and every read command's --help, run by
    bash exactly as printed (line breaks and all), answers its question over
    a published graph."""
    import ast

    dsn, deployment = tickets_graph
    examples = _printed_examples()
    count_q, join_q, texts_q = examples

    code, out, err = _bash(dsn, deployment, tmp_path, examples[count_q])
    assert code == 0, (out, err[-3000:])
    total, counted = out.strip().split(" ", 1)
    expected: dict[str, int] = {}
    for i in range(1, 31):
        if _ticket_status(i) != "Closed":
            expected[_ticket_status(i)] = expected.get(_ticket_status(i), 0) + 1
    assert int(total) == sum(expected.values()) == 24
    assert dict(ast.literal_eval(counted)) == expected
    assert "okg filter --all: 24 rows in 1 pages" in _lines(err), err[-2000:]

    code, out, err = _bash(dsn, deployment, tmp_path, examples[join_q])
    assert code == 0, (out, err[-3000:])
    assert sorted(_lines(out)) == sorted(
        f"ticket:T-{i} {_ticket_status(i)}" for i in range(2, 7)
    ), (out, err[-2000:])

    code, out, err = _bash(dsn, deployment, tmp_path, "echo ticket:T-6 | okg inspect texts -")
    assert code == 0, (out, err[-3000:])
    (record,) = [json.loads(line) for line in _lines(out)]
    assert record["state"] == "delivered" and record["text"] == _ticket_text(6), record

    code, out, err = _bash(dsn, deployment, tmp_path, examples[texts_q])
    assert code == 0, (out, err[-3000:])
    named, unread = out.strip().split(" unread: ")
    open_ids = [i for i in range(1, 31) if _ticket_status(i) == "Open"]
    assert sorted(ast.literal_eval(named)) == sorted(
        f"ticket:T-{i}" for i in open_ids if i % 3 == 0
    ), (out, err[-2000:])
    assert ast.literal_eval(unread) == []
    assert dsn not in err


def test_a_long_text_reply_ends_with_a_hint_whose_command_runs(tickets_graph, tmp_path):
    """As a client sees it, from its own process: a text reply of 24 rows is
    the reply on stdout and one hint line on stderr; the hint's command,
    run as printed, counts the whole set. A reply of 6 rows has no hint."""
    from okg.substrate.cli._read_shell import PIPE_HINT_PROGRAM

    dsn, deployment = tickets_graph
    code, out, err = _bash(
        dsn, deployment, tmp_path, "okg filter --type ticket --where 'attrs.status!=Closed'",
    )
    assert code == 0, err[-3000:]
    assert out.startswith("filter ok"), out[:400]
    hint = _lines(err)[-1]
    assert hint.startswith("hint: 24 rows; to count, join or list them, pipe them"), err
    command = hint.split("reading them: ", 1)[1]
    assert command == (
        "okg filter --where 'attrs.status!=Closed' --type ticket --all --jsonl"
        f" | {PIPE_HINT_PROGRAM}"
    ), hint
    code, out, err = _bash(dsn, deployment, tmp_path, command)
    assert code == 0, err[-3000:]
    assert out == "24\n"

    # The plain path (no shell form) gives it too, and keeps what was typed.
    typed = "okg filter --input '{\"kind\": \"nodes\", \"subtype\": \"ticket\"}' --predicate '{}' --limit 25"
    code, out, err = _bash(dsn, deployment, tmp_path, typed)
    assert code == 0, err[-3000:]
    # The page's status reads `1-25 of count unknown`, or `1-25, more
    # follow` once a bounded page says so (#3716); the hint follows it.
    assert _lines(err) in ([
        f"hint: {said}; to count, join or list them, pipe them instead of "
        f"reading them: {typed} --all --jsonl | {PIPE_HINT_PROGRAM}"
    ] for said in ("25 rows", "25 rows shown, more follow")), err
    code, out, err = _bash(dsn, deployment, tmp_path, _lines(err)[0].split("reading them: ", 1)[1])
    assert code == 0 and out == "30\n", err[-3000:]

    code, out, err = _bash(
        dsn, deployment, tmp_path, "okg filter --type ticket --where attrs.status=Closed",
    )
    assert code == 0, err[-3000:]
    assert out.startswith("filter ok") and "1-6 of 6" in out.splitlines()[0], out[:400]
    assert not any(line.startswith("hint:") for line in _lines(err)), err
    assert dsn not in err
