"""PostgreSQL proof for issue #2899: the graph path runs the text it checked.

The graph path used to re-render the caller's query several times before
PostgreSQL saw it. It now splices view bodies, Chronos subqueries, and the row
limit into the caller's original text and leaves every other byte alone. String
forms whose meaning depends on escape processing are refused on this path, and
the connection must use standard strings.
"""
from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.mcp import open_session
from okg.substrate.mcp.run_sql import run_sql
from okg.substrate.mcp.sql_safety import RunSqlInvalid
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
from tests.substrate._db_helpers import ensure_published_generation, seed_catalog_version

pytestmark = pytest.mark.postgres


def _publish_nodes(dsn: str) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact("n:a", "page", {"text": "alpha"}, {"id": "n:a"}, {"v": 1}),
                NodeFact("n:b", "page", {"text": "beta 50%"}, {"id": "n:b"}, {"v": 1}),
            ]
        )
    Publisher(
        dsn,
        "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()


@pytest.mark.parametrize(
    ("sql", "code"),
    [
        ("SELECT E'a' AS a FROM okg.v_nodes", "run_sql_graph_literal_refused"),
        ("SELECT U&'a' AS a FROM okg.v_nodes", "run_sql_graph_literal_refused"),
    ],
    ids=["escape-string", "unicode-string"],
)
def test_graph_path_refuses_escape_dependent_string_literals(
    clean_runtime: str, sql: str, code: str
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        with pytest.raises(RunSqlInvalid) as excinfo:
            run_sql(conn, session, sql, timeout_ms=5_000)
    assert excinfo.value.code == code


def test_plain_graph_query_with_quotes_percent_and_comments_runs_verbatim(
    clean_runtime: str,
) -> None:
    _publish_nodes(clean_runtime)
    sql = (
        "/* caller comment with 'quote' */ "
        "SELECT node_id, 'it''s ' || $$x$$ || $t$ :_chronos_branch_point $t$ AS lit, "
        "'%(_chronos_branch_point)s' AS pct "
        "FROM okg.v_nodes AS n -- trailing comment\n"
        "WHERE n.node_id LIKE 'n:%' ORDER BY node_id"
    )
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        out = run_sql(conn, session, sql, timeout_ms=5_000)
    assert out["query_plan_summary"] == "Chronos checkout"
    assert out["rows"] == [
        ["n:a", "it's x :_chronos_branch_point ", "%(_chronos_branch_point)s"],
        ["n:b", "it's x :_chronos_branch_point ", "%(_chronos_branch_point)s"],
    ]


def test_unaliased_graph_view_keeps_qualified_column_references(
    clean_runtime: str,
) -> None:
    _publish_nodes(clean_runtime)
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        out = run_sql(
            conn,
            session,
            "SELECT v_nodes.node_id, e.edge_id FROM okg.v_nodes "
            "LEFT JOIN okg.v_edges e ON e.src = v_nodes.node_id "
            "ORDER BY v_nodes.node_id",
            timeout_ms=5_000,
        )
    assert [row[0] for row in out["rows"]] == ["n:a", "n:b"]


def test_graph_path_refuses_when_standard_conforming_strings_is_off(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        conn.execute("SET standard_conforming_strings = off")
        conn.commit()
        with pytest.raises(RunSqlInvalid) as excinfo:
            run_sql(conn, session, "SELECT node_id FROM okg.v_nodes", timeout_ms=5_000)
    assert excinfo.value.code == "run_sql_graph_string_mode_refused"


@pytest.mark.parametrize(
    ("sql", "code"),
    [
        ("SELECT node_id FROM ONLY okg.v_nodes", "run_sql_graph_form_unsupported"),
        ("SELECT node_id FROM okg.v_nodes *", "run_sql_graph_form_unsupported"),
        # The sqlglot checker (PACT sql-safety-without-pglast) cannot read a
        # TABLE statement and refuses it as unparseable before the graph path.
        ("TABLE okg.v_nodes", "run_sql_invalid"),
    ],
    ids=["only", "star", "table"],
)
def test_graph_path_refuses_only_star_and_table_forms_by_name(
    clean_runtime: str, sql: str, code: str
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        with pytest.raises(RunSqlInvalid) as excinfo:
            run_sql(conn, session, sql, timeout_ms=5_000)
    assert excinfo.value.code == code
    assert "_chronos" not in str(excinfo.value)


def test_issue_2899_repro_never_runs_pg_sleep_on_the_graph_path(
    clean_runtime: str,
) -> None:
    """The #2899 repro: E'\\\\' is one backslash and pg_sleep(1) sits inside a
    string. A renderer that read the E-string differently turned that text
    into code. The graph path refuses the E'...' string before it splices or
    executes anything, so no sleep runs (two nodes would sleep two seconds)."""
    import time

    _publish_nodes(clean_runtime)
    sql = "SELECT E'\\\\' AS a, ' || pg_sleep(1) || ' AS b FROM okg.v_nodes"
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        t0 = time.monotonic()
        with pytest.raises(RunSqlInvalid) as excinfo:
            run_sql(conn, session, sql, timeout_ms=5_000)
        elapsed = time.monotonic() - t0
        # Only this test's database: another xdist worker's tests (the date
        # budget tests) run pg_sleep on their own databases at the same time.
        pg_sleep_calls = conn.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE query ILIKE '%pg_sleep%' "
            "AND pid <> pg_backend_pid() AND datname = current_database()"
        ).fetchone()[0]
    assert excinfo.value.code == "run_sql_graph_literal_refused"
    assert elapsed < 1.0
    assert pg_sleep_calls == 0


def test_executed_text_is_the_checked_text_with_only_the_name_spliced(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Capture the text PostgreSQL receives: it is the checked text, byte for
    byte, with the relation name replaced by the checkout subquery and the
    row-limit wrapper around it."""
    from okg.substrate.mcp import graph_sql_splice
    from okg.substrate.mcp.sql_safety import checked_run_sql

    _publish_nodes(clean_runtime)
    raw = (
        "/* lead */ SELECT node_id, x.v::float AS f, 'a''b' AS q /* keep */ "
        "FROM okg.v_nodes n, (VALUES (1)) x(v) -- tail\n ORDER BY node_id ; "
    )
    checked = checked_run_sql(raw)
    head, _, tail = checked.partition("okg.v_nodes")
    executed: list[str] = []
    real_execute = graph_sql_splice.execute_spliced_sql

    def capture(conn, sql):
        executed.append(sql)
        return real_execute(conn, sql)

    monkeypatch.setattr(graph_sql_splice, "execute_spliced_sql", capture)
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        out = run_sql(conn, session, raw, row_cap=10, timeout_ms=5_000)
    assert [row[0] for row in out["rows"]] == ["n:a", "n:b"]
    assert len(executed) == 1
    sent = executed[0]
    prefix = "SELECT * FROM (\n" + head + "("
    suffix = ")" + tail + "\n) AS _okg_bounded LIMIT 11"
    assert sent.startswith(prefix), sent
    assert sent.endswith(suffix), sent
    spliced_in = sent[len(prefix) : len(sent) - len(suffix)]
    assert "_chronos" in spliced_in
    assert "okg.v_nodes" not in sent


def test_every_deployed_okg_view_plans_under_the_view_body_check(
    clean_runtime: str,
) -> None:
    """The view-body check (final review round of #2579) refuses a graph name
    the plan did not read as a relation. Every okg.v_* view a provisioned
    database defines must still plan, and a view over the graph must route
    to the checkout."""
    from okg.substrate.mcp import graph_sql_splice

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")
        names = [
            row[0]
            for row in conn.execute(
                "SELECT c.relname FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'okg' AND c.relkind IN ('v', 'm') "
                "AND c.relname LIKE 'v\\_%' ORDER BY c.relname"
            ).fetchall()
        ]
        routed = []
        for name in names:
            plan = graph_sql_splice.plan_graph_sql(
                f'SELECT * FROM okg."{name}"', conn
            )
            if plan.routes_to_graph:
                routed.append(name)
    print(f"planned {len(names)} okg.v_* views; {len(routed)} route to the graph")
    assert len(names) >= 40
    assert {"v_nodes", "v_edges"} <= set(routed)


# ---- issue #3666: the `query` guide, backslash regexes, fitting a reply ----


def _publish_texts(dsn: str, rows: list[tuple[str, dict]]) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(node_id, "page", attrs, {"id": node_id}, {"v": 1})
                for node_id, attrs in rows
            ]
        )
    published = Publisher(
        dsn,
        "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert published["status"] == "published"


_GUIDE_ROWS = [
    ("n:fail", {"text": "build 4521 failed on the worker", "created": "2026-03-01T10:00:00Z"}),
    ("n:ok", {"text": "build passed", "created": "2026-03-02T11:00:00Z"}),
    ("n:failure", {"text": "a failure in step 7"}),
]


def _call_query(dsn: str, arguments: dict, *, json_format: bool = True) -> dict:
    """Call `query` through a real MCP client session.

    Returns the served description and the call result as the client
    received it (JSON result format, or the text a model reads)."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp._result_payload import JSON_META

    server = build_server(dsn=dsn)
    out: dict = {}

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            listed = {t.name: t for t in (await session.list_tools()).tools}
            out["description"] = listed["query"].description
            out["result"] = await session.call_tool(
                "query", arguments, meta=JSON_META if json_format else {},
            )

    try:
        anyio.run(drive)
    finally:
        server._okg_pool.close()
        server._okg_release_reservation()
    return out


def test_the_served_query_description_examples_run_as_written(
    clean_runtime: str,
) -> None:
    """The served `query` description names the views and columns, the text
    and field reads, $$ regexes, the size bound and the pin; every example
    it prints (a count, a join through v_edges, a text regex) runs exactly
    as printed through the MCP client. The fields line from the dataset
    card's sample is tested on a published deployment in
    tests/substrate/mcp/test_dataset_card.py."""
    from tests.substrate.mcp._result_payload import payload

    _publish_texts(clean_runtime, _GUIDE_ROWS)
    description = _call_query(clean_runtime, {"sql": "SELECT 1 AS one"})["description"]
    for needle in (
        "okg.v_nodes(node_id, subtype, attrs jsonb)",
        "okg.v_edges(edge_id, src, dst, edge_type, attrs)",
        "attrs->>'text'", "$$...$$", "max_bytes", "LIMIT", "left()",
        "Read-only", "pinned generation",
    ):
        assert needle in description, needle
    examples = [
        line.strip() for line in description.splitlines()
        if line.strip().startswith("SELECT ")
    ]
    assert len(examples) == 3, description
    assert "okg.v_edges" in examples[1] and "~*" in examples[2], examples
    served = []
    for sql in examples:
        body = payload(_call_query(clean_runtime, {"sql": sql})["result"])
        assert body.get("status") != "refused", (sql, body)
        served.append(body["rows"])
    assert served[0], examples[0]
    # The printed regex finds the failed and failure records only.
    assert sorted(row[0] for row in served[2]) == ["n:fail", "n:failure"]


def test_a_regex_with_backslashes_in_a_plain_string_is_served(
    clean_runtime: str,
) -> None:
    """`'\\d{4}'` under standard strings is the regex \\d{4}: served on the
    graph path through MCP (refused as ambiguous before #3666), with the
    same rows as the pattern in $$...$$."""
    from tests.substrate.mcp._result_payload import payload

    _publish_texts(clean_runtime, _GUIDE_ROWS)
    plain = payload(_call_query(clean_runtime, {
        "sql": "SELECT node_id FROM okg.v_nodes WHERE attrs->>'text' ~ '\\d{4}' ORDER BY 1",
    })["result"])
    dollar = payload(_call_query(clean_runtime, {
        "sql": "SELECT node_id FROM okg.v_nodes WHERE attrs->>'text' ~ $$\\d{4}$$ ORDER BY 1",
    })["result"])
    assert plain.get("status") != "refused", plain
    assert plain["rows"] == [["n:fail"]]
    assert dollar["rows"] == plain["rows"]
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        out = run_sql(
            conn, session,
            "SELECT '\\' AS one, 'a\\b' AS two FROM okg.v_nodes LIMIT 1",
            timeout_ms=5_000,
        )
    assert out["rows"] == [["\\", "a\\b"]]


def test_the_served_query_description_names_the_postgres_word_boundary(
    clean_runtime: str,
) -> None:
    """Issue #3666 (pq17): an agent wrote `\\b` for a word boundary, which
    Postgres reads as a backspace, and got 0 rows without an error. The
    served description says `\\y`; on this graph `\\y` finds the record and
    `\\b` silently finds none, which is why the line is there."""
    from tests.substrate.mcp._result_payload import payload

    _publish_texts(clean_runtime, _GUIDE_ROWS)
    word_y = _call_query(clean_runtime, {
        "sql": "SELECT node_id FROM okg.v_nodes WHERE attrs->>'text' ~* $$\\yfailure\\y$$",
    })
    assert "word boundary \\y, not \\b" in word_y["description"], word_y["description"]
    assert payload(word_y["result"])["rows"] == [["n:failure"]]
    word_b = payload(_call_query(clean_runtime, {
        "sql": "SELECT node_id FROM okg.v_nodes WHERE attrs->>'text' ~* $$\\bfailure\\b$$",
    })["result"])
    assert word_b.get("status") != "refused", word_b
    assert word_b["rows"] == []


def test_an_escape_string_refusal_offers_the_exact_dollar_quoted_retry(
    clean_runtime: str,
) -> None:
    """E'...' stays refused on the graph path. When its only escapes are a
    doubled backslash, the refusal carries the same query with $$...$$, and
    that retry runs as given. An unknown escape gets no retry."""
    from tests.substrate.mcp._result_payload import payload

    _publish_texts(clean_runtime, _GUIDE_ROWS)
    refused = payload(_call_query(clean_runtime, {
        "sql": "SELECT node_id FROM okg.v_nodes WHERE attrs->>'text' ~ E'\\\\d{4}'",
    })["result"])
    assert refused["status"] == "refused"
    assert refused["error"]["code"] == "run_sql_graph_literal_refused"
    retry = refused["error"]["retry_call"]
    assert retry["tool"] == "query" and retry["mode"] == "corrected"
    assert retry["arguments"]["sql"] == (
        "SELECT node_id FROM okg.v_nodes WHERE attrs->>'text' ~ $$\\d{4}$$"
    )
    again = payload(_call_query(clean_runtime, retry["arguments"])["result"])
    assert again["rows"] == [["n:fail"]]
    # E'\d' is the letter d to PostgreSQL: a $$ rewrite would change what the
    # query means, so none is offered.
    unclear = payload(_call_query(clean_runtime, {
        "sql": "SELECT node_id FROM okg.v_nodes WHERE attrs->>'text' ~ E'\\d{4}'",
    })["result"])
    assert unclear["error"]["code"] == "run_sql_graph_literal_refused"
    assert "retry_call" not in unclear["error"]


def test_a_backslash_string_cannot_hide_code_when_the_session_reads_escapes(
    clean_runtime: str,
) -> None:
    """The guard behind accepting backslashes. With the connection's own
    standard_conforming_strings OFF, PostgreSQL would read 'x\\' as an open
    escape and run the middle of this text as code. run_sql sets the setting
    on for the transaction that runs the caller's text, so the database
    reads two plain strings, as the checks did. Operational path: the graph
    path refuses such a connection outright."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ensure_published_generation(conn, deployment_name="default")
    sql = (
        "SELECT 'x\\' AS a, ' , current_user , ' AS b "
        "FROM okg.v_runtime_queue_health LIMIT 1"
    )
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        conn.execute("SET standard_conforming_strings = off")
        conn.commit()
        out = run_sql(conn, session, sql, timeout_ms=5_000)
        after = conn.execute("SHOW standard_conforming_strings").fetchone()[0]
    assert out["rows"] == [["x\\", " , current_user , "]]
    # SET LOCAL: the connection's own setting is back after the query.
    assert after == "off"


def test_a_too_large_query_reply_is_served_as_pages(
    clean_runtime: str,
) -> None:
    """A reply over max_bytes is served as the rows that fit, cut at a row,
    with `next_query`: the statement wrapped with an OFFSET at the same pin,
    and a note that pages follow the statement's ORDER BY. Following the
    calls reads every row exactly once (the statement orders by node_id).
    Text form: the status line says `next: next_query below`."""
    from tests.substrate.mcp._result_payload import payload, result_text

    rows = [(f"n:{i:03d}", {"text": f"row {i} " + "x" * 1_500}) for i in range(12)]
    _publish_texts(clean_runtime, rows)
    call = {
        "sql": "SELECT node_id, attrs->>'text' AS t FROM okg.v_nodes ORDER BY node_id",
        "max_bytes": 6_000,
    }
    seen: list[str] = []
    pages = 0
    while call is not None:
        page = payload(_call_query(clean_runtime, call)["result"])
        assert page.get("status") != "refused", page
        pages += 1
        assert pages <= 12, "the pages do not end"
        seen.extend(row[0] for row in page["rows"])
        nxt = page.get("next_query")
        if nxt is None:
            assert not page.get("truncation_reason"), page
            call = None
            continue
        assert page["truncated"] is True and page["response_byte_budget"] == 6_000
        assert "ORDER BY" in nxt["note"]
        assert nxt["tool"] == "query"
        assert nxt["arguments"]["max_bytes"] == 6_000
        assert nxt["arguments"]["generation_id"] == page["pinned_generation_id"]
        # One wrapper only, however many pages.
        assert nxt["arguments"]["sql"].count("_okg_page") == 1
        call = nxt["arguments"]
    assert pages >= 3
    assert seen == [f"n:{i:03d}" for i in range(12)]
    text = result_text(_call_query(
        clean_runtime,
        {"sql": "SELECT node_id, attrs->>'text' AS t FROM okg.v_nodes ORDER BY node_id",
         "max_bytes": 6_000},
        json_format=False,
    )["result"])
    head = text.splitlines()[0]
    assert "bound reached (max_bytes 6000)" in head and "next: next_query below" in head, head
    assert "_okg_page OFFSET" in text, text


def test_a_query_row_too_large_for_any_page_says_how_to_fit(
    clean_runtime: str,
) -> None:
    """When not even one row fits max_bytes the reply is refused with how to
    fit (LIMIT, fewer columns, left()) and the exact retry at a larger
    allowed budget, which is then served."""
    from tests.substrate.mcp._result_payload import payload

    _publish_texts(clean_runtime, [("n:big", {"text": "y" * 6_000})])
    small = {"sql": "SELECT node_id, attrs->>'text' AS t FROM okg.v_nodes"}
    refused = payload(_call_query(clean_runtime, {**small, "max_bytes": 4_096})["result"])
    assert refused["status"] == "refused"
    error = refused["error"]
    assert error["code"] == "response_byte_budget_exceeded"
    for words in ("LIMIT", "fewer columns", "left(attrs->>'text', 200)"):
        assert words in error["remedy"], error["remedy"]
    retry = error["retry_call"]
    assert retry["tool"] == "query" and retry["mode"] == "corrected"
    assert retry["arguments"]["sql"] == small["sql"]
    assert 4_096 < retry["arguments"]["max_bytes"] <= 262_144
    # M12's own retry, at the page's pin: 3b's general retry carries none.
    assert retry["arguments"].get("generation_id") is not None, retry
    served = payload(_call_query(clean_runtime, retry["arguments"])["result"])
    assert served.get("status") != "refused", served
    assert served["pinned_generation_id"] == retry["arguments"]["generation_id"]
    assert [row[0] for row in served["rows"]] == ["n:big"]


# ---- serve `query` by default without access rules (issue #3666) ----
# Jason, via orchestrator chat 2026-10-13, query-by-default: serve query by
# default without access rules.


def _listed_and_called(dsn: str, sql: str, *, json_format: bool = True) -> tuple[list[str], Any, str]:
    """The tools a client lists, a `query` call's result as the client gets
    it, and the server's instructions."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp._result_payload import JSON_META

    server = build_server(dsn=dsn)
    out: dict = {}

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            out["tools"] = [t.name for t in (await session.list_tools()).tools]
            out["result"] = await session.call_tool(
                "query", {"sql": sql}, meta=JSON_META if json_format else {},
            )

    try:
        anyio.run(drive)
        instructions = server.instructions or ""
    finally:
        server._okg_pool.close()
        server._okg_release_reservation()
    return out["tools"], out["result"], instructions


def test_query_is_listed_and_served_by_default_without_access_rules(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.substrate.mcp._result_payload import payload

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    _publish_texts(clean_runtime, _GUIDE_ROWS)
    tools, result, instructions = _listed_and_called(
        clean_runtime, "SELECT node_id FROM okg.v_nodes ORDER BY 1",
    )
    assert "query" in tools, tools
    assert "query" in instructions.split("serves exactly these tools:", 1)[1].split(".", 1)[0]
    body = payload(result)
    assert body.get("status") != "refused", body
    assert [row[0] for row in body["rows"]] == ["n:fail", "n:failure", "n:ok"]


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_query_is_off_and_refused_plainly_under_access_rules(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, mode: str,
) -> None:
    """Under audit or enforce `query` is never listed, never named in the
    instructions, and a call is refused by name, saying the access rules
    took it off: in JSON, in the text a model reads, and through the
    in-process call the `okg query` command makes."""
    from okg.substrate.cli._read_operators import call_operator, close_server
    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp._result_payload import payload, result_text

    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    _publish_texts(clean_runtime, _GUIDE_ROWS)
    # After the publish: the rules govern the read surface under test.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    sql = "SELECT node_id FROM okg.v_nodes"
    tools, result, instructions = _listed_and_called(clean_runtime, sql)
    assert "query" not in tools, tools
    assert "`query`" not in instructions and ", query" not in instructions
    body = payload(result)
    assert body["status"] == "refused", body
    error = body["error"]
    assert error["code"] == "query_off_under_access_rules", error
    assert f"access rules are {mode}" in error["message"], error
    assert "search, filter, map" in error["fix_hint"], error
    _tools, text_result, _ = _listed_and_called(clean_runtime, sql, json_format=False)
    text = result_text(text_result)
    assert "query_off_under_access_rules" in text.splitlines()[0], text
    assert f"access rules are {mode}" in text, text
    # The shell face: `okg query` calls the tool the same way, in process.
    server = build_server(dsn=clean_runtime, one_call=True)
    try:
        shell = call_operator(server, "query", {"sql": sql}, result_format="text")
    finally:
        close_server(server)
        server._okg_release_reservation()
    shell_text = result_text(shell)
    assert "query_off_under_access_rules" in shell_text, shell_text
    assert "n:fail" not in shell_text


def _query_started_with_rules_off(dsn: str, monkeypatch: pytest.MonkeyPatch, switch) -> tuple[list[str], Any, list]:
    """Build a server with access rules off, then call `switch` (which turns
    the rules on, or makes the mode unreadable) and call `query` as a
    client. Returns the tools listed before the switch, the call's result,
    and every run_sql call the server made (none may run)."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp import run_sql as run_sql_mod
    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp._result_payload import JSON_META

    ran: list = []
    real = run_sql_mod.run_sql

    def _recording(*args, **kwargs):
        ran.append(kwargs.get("sql", args[0] if args else None))
        return real(*args, **kwargs)

    monkeypatch.setattr(run_sql_mod, "run_sql", _recording)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    server = build_server(dsn=dsn)
    out: dict = {}

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            out["tools"] = [t.name for t in (await session.list_tools()).tools]
            switch()
            out["result"] = await session.call_tool(
                "query", {"sql": "SELECT node_id FROM okg.v_nodes"}, meta=JSON_META,
            )

    try:
        anyio.run(drive)
    finally:
        server._okg_pool.close()
        server._okg_release_reservation()
    return out["tools"], out["result"], ran


@pytest.mark.parametrize("mode", ["off", "audit", "enforce"])
def test_access_rules_switched_on_after_start_refuse_query_by_name(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, mode: str,
) -> None:
    """The mode is read again on every `query` call: a server started with
    the rules off refuses `query` once they are on. Under audit the SQL
    would otherwise only be recorded and run; here none runs and no row
    comes back."""
    from tests.substrate.mcp._result_payload import payload

    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    _publish_texts(clean_runtime, _GUIDE_ROWS)
    tools, result, ran = _query_started_with_rules_off(
        clean_runtime, monkeypatch,
        lambda: monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode),
    )
    assert "query" in tools, tools
    body = payload(result)
    if mode == "off":
        # The control: with the rules still off the same call runs once.
        assert body.get("status") != "refused", body
        assert len(ran) == 1, ran
        return
    assert body["status"] == "refused", body
    assert body["error"]["code"] == "query_off_under_access_rules", body
    assert body["error"]["data"]["access_rules"] == mode, body
    assert "rows" not in body and "n:fail" not in str(body), body
    assert ran == [], ran


def test_an_unreadable_access_rule_mode_refuses_query_by_name(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail closed: when the deployment's mode cannot be read (a present
    deployment.yaml that does not parse raises this error), `query` is
    refused by name and no SQL runs."""
    from okg.substrate.nomos import runtime_enforcement
    from tests.substrate.mcp._result_payload import payload

    def _unreadable(_name: str) -> str:
        raise runtime_enforcement.NomosEnforcementResolutionError(
            "deployment.yaml is present but could not be read",
        )

    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    _publish_texts(clean_runtime, _GUIDE_ROWS)
    tools, result, ran = _query_started_with_rules_off(
        clean_runtime, monkeypatch,
        lambda: monkeypatch.setattr(
            runtime_enforcement, "deployment_nomos_enforcement_mode", _unreadable,
        ),
    )
    assert "query" in tools, tools
    body = payload(result)
    assert body["error"]["code"] == "query_off_under_access_rules", body
    assert body["error"]["data"]["access_rules"] == "unreadable", body
    assert ran == [], ran


@pytest.mark.parametrize(
    ("mode", "listed"),
    [("off", ["inspect", "query"]), ("enforce", ["inspect"])],
)
def test_an_explicit_narrowing_still_decides_the_surface(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, mode: str, listed: list[str],
) -> None:
    """An allowlist naming `query` serves exactly what it names when access
    rules are off; under enforce `query` stays off even though it is named."""
    from tests.substrate.mcp._result_payload import payload

    monkeypatch.setenv("OKG_MCP_TOOL_ALLOWLIST", "inspect,query")
    _publish_texts(clean_runtime, _GUIDE_ROWS)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    tools, result, _ = _listed_and_called(clean_runtime, "SELECT 1 AS one")
    assert tools == listed, tools
    body = payload(result)
    if mode == "off":
        assert body.get("status") != "refused", body
    else:
        assert body["error"]["code"] == "query_off_under_access_rules", body


def test_a_profile_without_query_leaves_it_off_without_access_rules(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The discovery profile does not carry `query`; with access rules off
    it stays unlisted, as before (the default is the full read surface)."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp.server import build_server

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    _publish_texts(clean_runtime, _GUIDE_ROWS)
    server = build_server(dsn=clean_runtime, tool_profile="discovery")
    names: list[str] = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            names.extend(t.name for t in (await session.list_tools()).tools)

    try:
        anyio.run(drive)
    finally:
        server._okg_pool.close()
        server._okg_release_reservation()
    assert "query" not in names and "inspect" in names, names
