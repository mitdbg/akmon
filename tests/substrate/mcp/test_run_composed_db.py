"""`run` composed: each step reads its whole set or is refused with the
exact count (issue #3666, PACT run-composed-3666).

PostgreSQL tests on a published graph of 520 items (250 buckets), 40 docs
whose text holds "zebra" (4 topics) and 3 notes. Every call goes through an
MCP client session and every assertion reads the reply a client receives.
"""
from __future__ import annotations

import json
import time

import anyio
import pytest

from okg.substrate.library.sources.base import EdgeFact
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes

pytestmark = pytest.mark.postgres

ITEMS = 520
BUCKETS = 250
DOCS = 40
SUBTYPES = [SubtypeSpec("item"), SubtypeSpec("doc"), SubtypeSpec("note")]
BLURB = "a long description that makes each doc row large enough to cut " * 2


def _docs(start: int, stop: int) -> list:
    return [
        node(f"doc:{i:03d}", "doc", f"zebra page {i}", topic=f"t{i % 4}", blurb=BLURB)
        for i in range(start, stop)
    ]


def _corpus(tmp_path, dsn: str) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    nodes = [
        node(f"item:{i:04d}", "item", f"item {i}", bucket=i % BUCKETS,
             parity="even" if i % 2 == 0 else "odd")
        for i in range(ITEMS)
    ]
    nodes += _docs(0, DOCS)
    nodes += [node(f"note:{i}", "note", f"note {i}", kind="plain") for i in range(3)]
    publish_nodes(dsn, deployment, subtypes=SUBTYPES, nodes=nodes)
    return deployment


class Server:
    def __init__(self, dsn: str, deployment: str) -> None:
        from okg.substrate.mcp import server as srv

        self.mcp = srv.build_server(dsn=dsn, deployment_name=deployment)

    def close(self) -> None:
        self.mcp._okg_pool.close()
        if self.mcp._okg_write_pool:
            self.mcp._okg_write_pool.close()

    def call(self, tool: str, arguments: dict, result_format: str = "text"):
        from mcp.shared.memory import create_connected_server_and_client_session

        out: list = []

        async def drive() -> None:
            async with create_connected_server_and_client_session(self.mcp) as client:
                out.append(await client.call_tool(
                    tool, arguments, meta={"okg/result_format": result_format},
                ))

        anyio.run(drive)
        return out[0]

    def text(self, steps: list, **extra) -> str:
        result = self.call("run", {"steps": steps, **extra})
        assert not result.isError, result.content[0].text
        return result.content[0].text

    def json(self, steps: list, **extra) -> dict:
        return self.json_call({"steps": steps, **extra})

    def json_call(self, arguments: dict) -> dict:
        result = self.call("run", arguments, "json")
        assert not result.isError, result.content[0].text
        return json.loads(result.content[0].text)


@pytest.fixture
def corpus(clean_runtime, okg_mcp_can_log_in, tmp_path):
    deployment = _corpus(tmp_path, clean_runtime)
    server = Server(clean_runtime, deployment)
    try:
        yield server, clean_runtime, deployment
    finally:
        server.close()


@pytest.fixture
def served(corpus):
    return corpus[0]


def _row(reply: dict, number: int) -> dict:
    return reply["steps"][number - 1]


def _result(reply: dict, number: int) -> dict:
    row = _row(reply, number)
    assert row["status"] == "ok", row
    return row["result"]


def _ids(rows: list) -> list[str]:
    return [str(row["node_id"]) for row in rows]


def _step_text(text: str, number: int) -> list[str]:
    out, inside = [], False
    for line in text.splitlines():
        if line.startswith("[step "):
            inside = line.startswith(f"[step {number}]")
        if inside:
            out.append(line)
    return out


def _error(row: dict) -> dict:
    assert row["status"] == "refused", row
    return row["result"]["error"]


# --------------------------------------------------------------------------- #
# whole sets and exact counts                                                 #
# --------------------------------------------------------------------------- #


def test_every_step_states_its_exact_count(served):
    steps = [
        {"filter": {"where": {"type": "doc"}}},
        {"map": {"where": "$1", "select": ["node_id"]}},
        {"aggregate": {"where": "$2", "by": ["attrs.topic"], "metrics": ["count"]}},
    ]
    lines = served.text(steps).splitlines()
    assert lines[0] == "run ok · 3 steps"
    assert lines[1] == f"[step 1] filter: {DOCS} records"
    assert lines[2] == f"[step 2] map: {DOCS} records"
    assert lines[3] == f"[step 3] aggregate: {DOCS} records in 4 groups"
    reply = served.json(steps)
    assert [_row(reply, n).get("count") for n in (1, 2, 3)] == [DOCS] * 3
    assert _row(reply, 3)["groups"] == 4
    assert sum(g["count"] for g in _result(reply, 3)["groups"]) == DOCS


def _watch_sets(served, monkeypatch) -> list[str]:
    """Every step set's SQL, in step order, as the backend wrote it."""
    backend = served.mcp._okg_run_backend
    real = backend.step_set
    seen: list[str] = []

    def watched(*args, **kwargs):
        sql = real(*args, **kwargs)
        seen.append(sql)
        return sql

    monkeypatch.setattr(backend, "step_set", watched)
    return seen


#: A step set read as a membership subquery (``run_steps.lower_marker``).
SUBQUERY = "target_id = ANY(ARRAY(SELECT "


def test_a_set_with_a_plain_condition_is_read_as_that_condition(served, monkeypatch):
    """FAILS BEFORE (#3666): "$1" over a filter's set compiled as a membership
    subquery, read again by every later statement over every record (the
    served example timed out). A set whose condition compares stored fields
    is now read as that condition: same counts and groups, under `not` too."""
    seen = _watch_sets(served, monkeypatch)
    even = {"type": "item", "attrs.parity": "even"}
    reply = served.json([
        {"filter": {"where": even}, "show": False},
        {"aggregate": {"where": "$1", "by": ["attrs.parity"], "metrics": ["count"]},
         "show": True},
        {"aggregate": {"where": {"op": "not", "clause": "$1"}, "by": ["subtype"],
                       "metrics": ["count"]}},
    ])
    assert [_row(reply, n)["count"] for n in (1, 2, 3)] == [
        ITEMS // 2, ITEMS // 2, ITEMS // 2 + DOCS + 3,
    ]
    assert [(g["attrs.parity"], g["count"]) for g in _result(reply, 2)["groups"]] == [
        ("even", ITEMS // 2),
    ]
    inline = served.json([
        {"aggregate": {"where": {"op": "not", "clause": even}, "by": ["subtype"],
                       "metrics": ["count"]}},
    ])
    assert _result(reply, 3)["groups"] == _result(inline, 1)["groups"]
    assert len(seen) >= 3 and not any(SUBQUERY in sql for sql in seen)


def test_a_set_with_no_plain_condition_is_still_read_as_a_subquery(served, monkeypatch):
    """Failure case: a search step's set is its match ids, not a condition;
    a later step reads it as a subquery and answers as before."""
    seen = _watch_sets(served, monkeypatch)
    reply = served.json([
        {"search": {"query": "zebra", "method": "grep", "limit": 2}},
        {"aggregate": {"where": "$1", "by": ["attrs.topic"], "metrics": ["count"]}},
    ])
    assert _row(reply, 2)["count"] == DOCS
    assert sorted(g["count"] for g in _result(reply, 2)["groups"]) == [DOCS // 4] * 4
    assert len(seen) == 1 and SUBQUERY in seen[0]


def _coalesce_lowering(node, *, kind, columns, relation, attrs_sql, readable):
    """The lowering before #3666's index fix: ``COALESCE(x IN (..), FALSE)``,
    kept here as the reference the new form must answer exactly as."""
    from okg.substrate.mcp import run_steps

    found = run_steps._marker(node)
    if found is None:
        return run_steps.NOT_A_MARKER
    if readable or kind != "nodes":
        return None
    entry, value_field = found
    field = node.get("field")
    rel = f"{relation}." if relation else ""
    if value_field is None:
        if field != "node_id":
            return None
        return f"COALESCE({rel}target_id IN ({run_steps.embed(entry.raw_sql)}), FALSE)"
    source = run_steps.embed(entry.values[value_field].raw_sql)
    if field.startswith("attrs."):
        target = f"({attrs_sql} -> {run_steps._sql_text(field.split('.', 1)[1])})"
        return (
            f"COALESCE({target} IN (SELECT run_values.v FROM ({source}) "
            "AS run_values(v) WHERE run_values.v IS NOT NULL "
            "AND jsonb_typeof(run_values.v) <> 'null'), FALSE)"
        )
    column = columns.get(field)
    if column is None:
        return None
    return (
        f"COALESCE({rel}{column} IN (SELECT run_values.v #>> '{{}}' FROM "
        f"({source}) AS run_values(v) WHERE jsonb_typeof(run_values.v) = 'string'), FALSE)"
    )


def _count_and_not(where) -> list[dict]:
    by = {"by": ["subtype"], "metrics": ["count"]}
    return [
        {"aggregate": {"where": where, **by}},
        {"aggregate": {"where": {"op": "not", "clause": where}, **by}},
    ]


#: Runs whose later steps read an earlier step's set as a subquery: a set,
#: an empty set, a value list of an attribute some records lack, a value
#: list of ids, and a set whose own query reads another set (nested).
_ZEBRA = {"search": {"query": "zebra page 3", "method": "grep", "limit": 2}}
INDEXED_RUNS = {
    "set": [_ZEBRA, *_count_and_not("$1")],
    "empty": [{"search": {"query": "qqqnomatchqqq", "method": "grep", "limit": 2}},
              *_count_and_not("$1")],
    "attr values": [_ZEBRA, *_count_and_not({"attrs.topic": "$1.attrs.topic"})],
    "id values": [_ZEBRA, *_count_and_not({"node_id": "$1.node_id"})],
    "nested": [
        _ZEBRA,
        {"filter": {"where": {"op": "and", "clauses": [
            "$1", {"op": "not", "clause": {"attrs.topic": "t3"}}]}}, "show": False},
        *_count_and_not("$2"),
    ],
}


def _answers(served, steps: list) -> list:
    reply = served.json(steps)
    return [
        (row["status"], row.get("count"),
         sorted((g["subtype"], g["count"]) for g in row["result"].get("groups", []))
         if row["status"] == "ok" and isinstance(row.get("result"), dict) else None)
        for row in reply["steps"]
    ]


def test_a_set_subquery_answers_as_the_coalesce_form_did(served, monkeypatch):
    """FAILS BEFORE (#3666): a "$n" set lowered to ``COALESCE(x IN (..),
    FALSE)``, which hides `x` from its index, so each statement over the set
    read every record. It now lowers to ``x = ANY(ARRAY(..))``. Same records
    as the old form for a set, an empty set, value lists (an attribute some
    records lack, ids) and a nested set, with and without ``not``."""
    from okg.substrate.mcp import run_steps

    real = run_steps.lower_marker
    lowered: list[str] = []

    def watched(node, **kwargs):
        sql = real(node, **kwargs)
        if isinstance(sql, str):
            lowered.append(sql)
        return sql

    monkeypatch.setattr(run_steps, "lower_marker", watched)
    new = {name: _answers(served, steps) for name, steps in INDEXED_RUNS.items()}
    # Each test is `(x IS NOT NULL AND ..)`, never the COALESCE form.
    assert lowered and all(
        sql.startswith("(") and " IS NOT NULL AND " in sql.split(" FROM ", 1)[0]
        for sql in lowered
    )
    # The positive whole-set tests probe the index; those under `not` hash.
    assert any("target_id = ANY(ARRAY(SELECT " in sql for sql in lowered)
    assert any("target_id IN (SELECT " in sql for sql in lowered)
    monkeypatch.setattr(run_steps, "lower_marker", _coalesce_lowering)
    old = {name: _answers(served, steps) for name, steps in INDEXED_RUNS.items()}
    assert new == old
    # The answers are the expected ones, not merely equal.
    tail = {name: [answer[1] for answer in answers[-2:]] for name, answers in new.items()}
    everyone = ITEMS + DOCS + 3
    matched = new["set"][0][1]
    assert 0 < matched < DOCS
    assert tail["set"] == [matched, everyone - matched]
    assert tail["empty"] == [0, everyone]
    # Items and notes have no topic: false under the test, true under `not`.
    assert tail["attr values"] == [DOCS, ITEMS + 3]
    assert tail["id values"] == [matched, everyone - matched]
    nested = new["nested"][1][1]
    assert 0 < nested < matched
    assert tail["nested"] == [nested, everyone - nested]


def test_a_null_far_end_reads_as_the_coalesce_form_did_under_not(corpus):
    """FAILS BEFORE (review of e081216e01d): an expand step reads its far
    end through a LEFT JOIN, so an edge whose far end is not in the view has
    a null ``target_id``. ``x = ANY(..)`` is null there, and under ``not``
    the edge was dropped where ``COALESCE(x IN (..), FALSE)`` kept it. A
    published graph cannot hold such an edge (the publish gate refuses a
    dangling one), so every form is evaluated on PostgreSQL over a null, a
    member and a non-member, against a set that holds a null."""
    import psycopg

    from okg.substrate.mcp import run_steps

    _server, dsn, _deployment = corpus
    sets = run_steps.RunSets()
    sets.sets["1"] = run_steps.StepSet(
        raw_sql="SELECT v FROM (VALUES ('a'), (NULL)) AS s(v)")
    sets.sets["1"].values["node_id"] = run_steps.ValueList(
        raw_sql="SELECT to_jsonb(v) FROM (VALUES ('a'), (NULL)) AS s(v)")
    sets.sets["1"].values["attrs.k"] = run_steps.ValueList(
        raw_sql="SELECT v FROM (VALUES ('\"a\"'::jsonb), ('null'::jsonb), (NULL)) AS s(v)")
    leaves = {
        "set": {"op": "in", "field": "node_id", "values": [sets.set_marker("1")]},
        "ids": {"op": "in", "field": "node_id", "values": [sets.values_marker("1", "node_id")]},
        "attr": {"op": "in", "field": "attrs.k", "values": [sets.values_marker("1", "attrs.k")]},
    }
    kwargs = {"kind": "nodes", "columns": {"node_id": "target_id"}, "relation": "r",
              "attrs_sql": "COALESCE(r.attrs, '{}'::jsonb)", "readable": False}
    rows = ("(VALUES (NULL::text, NULL::jsonb), ('a', '{\"k\": \"a\"}'), "
            "('b', '{\"k\": \"b\"}')) AS r(target_id, attrs)")
    answers: dict[str, list] = {}
    with run_steps.registry_scope(sets), psycopg.connect(dsn) as conn:
        for name, leaf in leaves.items():
            negated = run_steps.mark_negated({"op": "not", "clause": leaf})["clause"]
            for label, positive, under_not in (
                ("old", _coalesce_lowering(leaf, **kwargs), _coalesce_lowering(leaf, **kwargs)),
                ("new", run_steps.lower_marker(leaf, **kwargs),
                 run_steps.lower_marker(negated, **kwargs)),
            ):
                sql = (f"SELECT r.target_id, {positive}, NOT {under_not} FROM {rows} "
                       "ORDER BY r.target_id NULLS FIRST")
                answers[f"{label} {name}"] = conn.execute(sql).fetchall()
    for name in leaves:
        assert answers[f"new {name}"] == answers[f"old {name}"], name
        # A null far end is out of the set and in its `not`; never null.
        assert answers[f"new {name}"] == [
            (None, False, True), ("a", True, False), ("b", False, True)], name


def test_an_expand_step_under_not_over_a_set_answers_as_before(history, monkeypatch):
    """An expand step's far-end condition `not "$1"` (its far end read
    through the LEFT JOIN) and `"$1"`: the same edges under the new lowering
    as under ``COALESCE(x IN (..), FALSE)``."""
    from okg.substrate.mcp import run_steps

    steps = [
        {"search": {"query": "Tune daemons", "method": "grep", "limit": 2}},
        {"expand": {"from": FILE_ID, "edge_type": "affects", "direction": "in",
                    "where": {"op": "not", "clause": "$1"}, "select": ["node_id"]},
         "show": True},
        {"expand": {"from": FILE_ID, "edge_type": "affects", "direction": "in",
                    "where": "$1", "select": ["node_id"]}},
    ]

    def far_ends(reply):
        return [sorted(_ids(_result(reply, n)["neighbors"])) for n in (2, 3)]

    new = far_ends(history.json(steps))
    monkeypatch.setattr(run_steps, "lower_marker", _coalesce_lowering)
    assert far_ends(history.json(steps)) == new
    assert new == [["commit:c1", "commit:c2", "commit:c5", "commit:c6"],
                   ["commit:c3", "commit:c4"]]


def test_a_printed_step_over_200_rows_is_refused_with_the_count(served):
    """Failure case: a printed map over 520 records is refused with its
    count and two calls, and each offered call is served."""
    steps = [{"map": {"where": {"type": "item"}, "select": ["node_id"]}}]
    reply = served.json(steps)
    error = _error(_row(reply, 1))
    assert error["code"] == "run_step_too_many_rows"
    assert f"{ITEMS} records" in error["message"]
    hidden, counted = error["data"]["retry_calls"]
    assert hidden["tool"] == counted["tool"] == "run"
    shown = served.json_call(hidden["arguments"])
    assert _row(shown, 1)["count"] == ITEMS and _row(shown, 1)["rows_shown"] == 0
    total = served.json_call(counted["arguments"])
    assert [(g["subtype"], g["count"]) for g in _result(total, 1)["groups"]] == [("item", ITEMS)]
    text = served.text(steps)
    assert "run_step_too_many_rows" in text and "520 records" in text


def test_a_filter_step_over_500_records_is_refused_with_the_count(served):
    """FAILS BEFORE: a filter over 520 records printed 50 rows with no cap."""
    reply = served.json([{"filter": {"where": {"type": "item"}}}])
    error = _error(_row(reply, 1))
    assert error["code"] == "run_step_too_many_rows"
    assert f"{ITEMS} records" in error["message"]
    assert "item:0001" not in json.dumps(reply)


def test_an_aggregate_over_200_groups_is_refused_with_the_group_count(served):
    reply = served.json([
        {"aggregate": {"where": {"type": "item"}, "by": ["attrs.bucket"], "metrics": ["count"]}},
    ])
    error = _error(_row(reply, 1))
    assert error["code"] == "run_step_too_many_rows"
    assert f"{ITEMS} records in {BUCKETS} groups" in error["message"]
    # Not printed, the same step passes its whole set on.
    passed = served.json([
        {"aggregate": {"where": {"type": "item"}, "by": ["attrs.bucket"], "metrics": ["count"]}},
        {"aggregate": {"where": "$1", "by": ["attrs.parity"], "metrics": ["count"]}},
    ])
    assert _row(passed, 1)["groups"] == BUCKETS
    assert sorted(g["count"] for g in _result(passed, 2)["groups"]) == [ITEMS // 2] * 2


def test_a_limit_above_200_is_refused_with_the_count(served):
    reply = served.json([{"filter": {"where": {"type": "item"}, "limit": 300}}])
    assert _error(_row(reply, 1))["code"] == "run_step_too_many_rows"
    # Below the cap a large limit prints the whole set.
    small = served.json([{"filter": {"where": {"type": "doc"}, "limit": 300}}])
    assert len(_result(small, 1)["items"]) == DOCS
    assert "next_call" not in _row(small, 1)


def test_a_step_limit_cuts_printing_not_the_set(served):
    """FAILS BEFORE: a search step with limit 5 over 40 grep matches fed a
    later step 5 records. The search and filter steps print their limit;
    the next step reads all 40."""
    steps = [
        {"search": {"query": "zebra", "method": "grep", "limit": 5}, "show": True},
        {"filter": {"where": "$1", "limit": 3}, "show": True},
        {"aggregate": {"where": "$2", "by": ["subtype"], "metrics": ["count"]}},
    ]
    reply = served.json(steps)
    assert _row(reply, 1)["count"] == DOCS and _row(reply, 1)["rows_shown"] == 5
    assert _row(reply, 2)["count"] == DOCS and _row(reply, 2)["rows_shown"] == 3
    assert _row(reply, 2)["rows_left"] == DOCS - 3
    assert [(g["subtype"], g["count"]) for g in _result(reply, 3)["groups"]] == [("doc", DOCS)]
    lines = _step_text(served.text(steps), 2)
    assert lines[0] == f"[step 2] filter: {DOCS} records"
    assert lines[-1].startswith(f"! cut at limit 3: 3 rows shown, {DOCS - 3} left; next: run(")


def test_a_grep_step_set_is_its_whole_match_set(served):
    reply = served.json([
        {"search": {"query": "zebra", "method": "grep", "limit": 2}},
        {"aggregate": {"where": "$1", "by": ["attrs.topic"], "metrics": ["count"]}},
    ])
    assert _row(reply, 1)["count"] == DOCS
    assert sum(g["count"] for g in _result(reply, 2)["groups"]) == DOCS


def test_a_step_whose_set_is_not_exact_prints_no_count_line(served):
    text = served.text([{"search": {"query": "zebra", "limit": 3}}])
    lines = _step_text(text, 1)
    assert lines[0].startswith("[step 1] search ok"), lines
    assert any(line.startswith("! not a total: a search page") for line in lines)
    reply = served.json([{"search": {"query": "zebra", "limit": 3}}])
    assert "count" not in _row(reply, 1)


def test_a_ranked_search_step_named_later_is_refused(served):
    """Failure case: a ranked page named by a later step is refused by name,
    and the step that names it is skipped, not answered from the page."""
    steps = [
        {"search": {"query": "zebra", "method": "semantic", "where": {"type": "doc"}, "limit": 5}},
        {"aggregate": {"where": "$1", "by": ["subtype"], "metrics": ["count"]}},
    ]
    reply = served.json(steps)
    error = _error(_row(reply, 1))
    assert "a ranked page of the best matches, not a whole set" in error["message"]
    assert _row(reply, 2)["status"] == "skipped"
    # The offered call (M7's text condition) is served and reads the whole set.
    (offered,) = error["data"]["retry_calls"]
    assert offered["arguments"]["steps"][0] == {"filter": {"where": {
        "type": "doc", "text": {"op": "matches", "value": "zebra"}}}}
    answered = served.json_call(offered["arguments"])
    assert _row(answered, 1)["count"] == DOCS
    assert [(g["subtype"], g["count"]) for g in _result(answered, 2)["groups"]] == [("doc", DOCS)]
    # With no query words nothing is offered, and the reply says why.
    bare = served.json([
        {"search": {"method": "semantic", "where": {"type": "doc"}}},
        {"aggregate": {"where": "$1", "by": ["subtype"], "metrics": ["count"]}},
    ])
    assert "no filter call is offered: the step has no query words" in _error(_row(bare, 1))["fix_hint"]


def test_a_count_that_does_not_finish_is_refused(served, monkeypatch):
    """Failure case: the count runs past the step's deadline; the step is
    refused run_count_not_finished, never estimated, and its reader skipped."""
    from okg.substrate.mcp import run_steps

    monkeypatch.setattr(run_steps, "STEP_BUDGET_MS", 1_500)
    pool = served.mcp._okg_pool
    backend = served.mcp._okg_run_backend

    def slow_count(raw_sql, generation, authority):
        # The real count, behind a sleep, under the statement_timeout a
        # step's budget sets (what is left): the timeout cuts it at the
        # deadline.
        assert run_steps.counting()
        left = run_steps.run_time_left_ms()
        assert left
        with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            # Local to this transaction: the pooled connection keeps its own.
            cur.execute("SELECT set_config('statement_timeout', %s, true)", (f"{left}ms",))
            cur.execute(f"SELECT count(*) FROM ({raw_sql}) AS c, pg_sleep(5)")
            return cur.fetchone()[0]

    monkeypatch.setattr(backend, "count", slow_count)
    reply = served.json([
        {"filter": {"where": {"type": "doc"}}},
        {"aggregate": {"where": "$1", "by": ["subtype"], "metrics": ["count"]}},
    ])
    error = _error(_row(reply, 1))
    assert error["code"] == "run_count_not_finished", error
    assert "never estimated" in error["message"]
    assert _row(reply, 2)["status"] == "skipped"


def test_a_set_read_cut_by_its_timeout_is_the_budget_refusal(served, monkeypatch):
    """FAILS BEFORE (#3666): a statement timeout in the read of a step's
    set (made after the step's call) was served as operator_execution_failed
    with an error id. It is the step's budget refusal, with what to do."""
    from okg.substrate.mcp import run_steps

    monkeypatch.setattr(run_steps, "STEP_BUDGET_MS", 1_500)
    pool = served.mcp._okg_pool
    backend = served.mcp._okg_run_backend

    def slow_set(*args, **kwargs):
        # The timeout a step's budget sets: what is left, so it cuts the
        # read at the step's deadline.
        left = run_steps.run_time_left_ms()
        assert left
        with pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute("SELECT set_config('statement_timeout', %s, true)", (f"{left}ms",))
            cur.execute("SELECT pg_sleep(5)")
        raise AssertionError("the statement timeout did not cut the read")

    monkeypatch.setattr(backend, "step_set", slow_set)
    reply = served.json([
        {"filter": {"where": {"type": "doc"}}},
        {"aggregate": {"where": "$1", "by": ["subtype"], "metrics": ["count"]}},
    ])
    error = _error(_row(reply, 1))
    assert error["code"] == "run_over_budget", error
    assert "time budget" in error["message"]
    assert "narrow the step's condition" in json.dumps(error)
    assert _row(reply, 2)["status"] == "skipped"
    text = served.text([{"filter": {"where": {"type": "doc"}}}, {"filter": {"where": "$1"}}])
    assert "run_over_budget" in text and "operator_execution_failed" not in text


def test_a_read_past_the_step_deadline_is_cut_there(served, monkeypatch):
    """FAILS BEFORE (#3666): each statement's timeout is what was left when
    its read opened, so a read could run past the step's deadline. The read
    running at the deadline is cancelled there and refused by name."""
    from okg.substrate.mcp import run_steps

    monkeypatch.setattr(run_steps, "STEP_BUDGET_MS", 1_500)
    # No statement timeout cuts anything: only the deadline can.
    monkeypatch.setattr(run_steps, "statement_budget_ms", lambda: 600_000)
    real = run_steps.lower_marker

    def slow_marker(node, **kwargs):
        sql = real(node, **kwargs)
        if not isinstance(sql, str):
            return sql
        # Evaluated once per statement (an uncorrelated subquery).
        return f"({sql} AND (SELECT 1 FROM pg_sleep(6)) = 1)"

    monkeypatch.setattr(run_steps, "lower_marker", slow_marker)
    watches: list = []
    real_watch = run_steps.watch_deadline

    def kept_watch(conn):
        watch = real_watch(conn)
        if watch is not None:
            watches.append((conn, watch))
        return watch

    monkeypatch.setattr(run_steps, "watch_deadline", kept_watch)
    started = time.monotonic()
    reply = served.json([
        {"search": {"query": "zebra", "method": "grep", "limit": 2}},
        {"aggregate": {"where": "$1", "by": ["attrs.topic"], "metrics": ["count"]}},
    ])
    elapsed = time.monotonic() - started
    error = _error(_row(reply, 2))
    assert error["code"] == "run_over_budget", error
    assert elapsed < 5, elapsed
    # M1: a connection the watch sent a cancel on is closed, so the pool
    # discards it and no later borrower meets a late cancel; the others
    # go back to the pool open.
    fired = [conn for conn, watch in watches if watch.fired]
    assert fired, watches
    assert all(conn.closed for conn in fired)
    # A pooled connection can serve several reads; one never fired on stays open.
    assert all(not conn.closed for conn, _w in watches if all(conn is not f for f in fired))
    pool = served.mcp._okg_pool
    with pool.connection() as conn:
        assert all(conn is not gone for gone in fired)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1


# --------------------------------------------------------------------------- #
# one reply, one cut line                                                     #
# --------------------------------------------------------------------------- #


def test_six_printed_steps_share_the_default_reply_bytes(served):
    """FAILS BEFORE: at max_bytes 16384 six printed steps were refused with
    invalid_response_byte_budget. Now they share it, cut with one line each."""
    step = {"filter": {"where": {"type": "doc"}}, "show": True}
    result = served.call("run", {"steps": [step] * 6, "max_bytes": 16384})
    text = result.content[0].text
    assert not result.isError, text
    assert len(text.encode("utf-8")) <= 16384
    for number in range(1, 7):
        lines = _step_text(text, number)
        assert lines[0] == f"[step {number}] filter: {DOCS} records"
        assert lines[-1].startswith("! cut to fit ") and " left; next: run(" in lines[-1]


def test_cut_step_next_call_joins_at_the_pinned_generation(corpus):
    """FAILS BEFORE: a cut step's page continued only by a token refused
    outside its run. Generation A is cut; B is published; next_call reads A:
    the two pages are the step's whole set at A, no row twice, none missing."""
    server, dsn, deployment = corpus
    steps = [{"filter": {"where": {"type": "doc"}, "select": ["node_id", "attrs.blurb"]}}]
    first = server.json(steps, max_bytes=4096)
    row = _row(first, 1)
    generation = first["generation_id"]
    assert row["count"] == DOCS and 0 < row["rows_shown"] < DOCS, row
    assert row["rows_left"] == DOCS - row["rows_shown"]
    call = row["next_call"]
    assert call["tool"] == "run"
    assert call["arguments"]["generation_id"] == generation
    publish_nodes(dsn, deployment, subtypes=SUBTYPES, nodes=_docs(DOCS, DOCS + 5))
    started = time.monotonic()
    second = server.json_call({**call["arguments"], "max_bytes": 65536})
    rerun_ms = (time.monotonic() - started) * 1000
    print(f"next_call rerun: {rerun_ms:.0f} ms")
    assert second["generation_id"] == generation
    rows = _ids(_result(first, 1)["items"]) + _ids(_result(second, 1)["items"])
    assert len(rows) == len(set(rows)) == DOCS
    assert sorted(rows) == [f"doc:{i:03d}" for i in range(DOCS)]
    assert "next_call" not in _row(second, 1)
    # The text form names the same call on the cut line.
    text = server.text(steps, max_bytes=4096, generation_id=generation)
    last = text.splitlines()[-1]
    assert last.startswith("! cut to fit ") and " rows shown, " in last
    assert f'generation_id={json.dumps(generation)}' in last


# --------------------------------------------------------------------------- #
# issue #3771 acceptance shapes and the link join, on a small history graph   #
# --------------------------------------------------------------------------- #

FILE_ID = "file:repo:config/daemons.yaml"
HISTORY_COMMITS = [
    # id, sha, committed_at, is_merge, subject, author, ticket key
    ("commit:c1", "aaa1", "2023-11-02T10:00:00Z", False, "Seed daemons", "ana", "T-1"),
    ("commit:c2", "aaa2", "2024-03-01T10:00:00Z", True, "Merge branch main", "bo", None),
    ("commit:c3", "aaa3", "2024-06-10T10:00:00Z", False, "Tune daemons", "cy", "T-2"),
    ("commit:c4", "aaa4", "2025-02-01T10:00:00Z", False, 'Revert "Tune daemons"', "ana", "T-404"),
    ("commit:c5", "aaa5", "2026-01-15T10:00:00Z", False, "Add daemon", "bo", None),
    ("commit:c6", "aaa6", "2026-03-02T10:00:00Z", False, "Bump version", "cy", None),
]
TAGS = [
    # id, name, target sha
    ("tag:release-30.0.0", "release-30.0.0", "aaa3"),
    ("tag:release-31.0.0", "release-31.0.0", "aaa4"),
    ("tag:release-32.0.0", "release-32.0.0", "aaa5"),
    ("tag:release-32.1.0", "release-32.1.0", "aaa6"),
    ("tag:nightly-1", "nightly-1", "aaa6"),
]
HISTORY_TYPES = [
    SubtypeSpec("git_commit"), SubtypeSpec("git_tag"), SubtypeSpec("file"),
    SubtypeSpec("ticket"),
]


def _link(src: str, edge_type: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src, dst=dst, edge_type=edge_type,
        source_record_id={"id": f"{src}|{edge_type}|{dst}"}, source_revision={"v": 1},
    )


@pytest.fixture
def history(clean_runtime, okg_mcp_can_log_in, tmp_path):
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes = [node(FILE_ID, "file", "daemons config", path="config/daemons.yaml")]
    edges = []
    for commit_id, sha, when, merge, subject, author, key in HISTORY_COMMITS:
        attrs = {"sha": sha, "committed_at": when, "is_merge": merge,
                 "subject": subject, "author": author, "repo": "example-repo"}
        if key is not None:
            attrs["ticket_key"] = key
        nodes.append(node(commit_id, "git_commit", subject, **attrs))
        edges.append(_link(commit_id, "affects", FILE_ID))
        # The references link of a key with no ticket record is not stored,
        # as an ingest that skips a missing target leaves it (pq17).
        if key in ("T-1", "T-2"):
            edges.append(_link(commit_id, "references", f"ticket:{key}"))
    nodes += [node(f"ticket:{k}", "ticket", f"ticket {k}", key=k) for k in ("T-1", "T-2")]
    for tag_id, name, sha in TAGS:
        nodes.append(node(tag_id, "git_tag", name, name=name,
                          target_commit_sha=sha, repo="example-repo"))
    publish_nodes(
        clean_runtime, deployment, subtypes=HISTORY_TYPES,
        narrowings=[("git_commit", "affects", "file"), ("git_commit", "references", "ticket")],
        edges=edges, nodes=nodes,
    )
    server = Server(clean_runtime, deployment)
    try:
        yield server
    finally:
        server.close()


def test_pq19_shape_release_tags_of_this_year_in_one_call(history):
    """#3771 pq19: tags of the commits made in 2026, in one run call: the
    second step reads the first step's shas as a value list, no id list
    is copied between calls."""
    steps = [
        {"filter": {"where": {"type": "git_commit", "attrs.repo": "example-repo",
                              "attrs.committed_at": {"op": "gt", "value": "2026-01-01"}}}},
        {"filter": {"where": {"type": "git_tag", "attrs.repo": "example-repo",
                              "attrs.name": {"op": "prefix", "value": "release-"},
                              "attrs.target_commit_sha": "$1.attrs.sha"},
                    "select": ["node_id", "attrs.name"]}},
    ]
    reply = history.json(steps)
    assert _row(reply, 1)["count"] == 2 and _row(reply, 1)["rows_shown"] == 0
    assert sorted(_ids(_result(reply, 2)["items"])) == [
        "tag:release-32.0.0", "tag:release-32.1.0",
    ]
    lines = history.text(steps).splitlines()
    assert lines[:3] == ["run ok · 2 steps", "[step 1] filter: 2 records", "[step 2] filter: 2 records"]


def test_pq12_shape_changes_to_one_file_since_2024_with_their_reverts(history):
    """#3771 pq12: the non-merge commits that changed one file since 2024,
    with author, date and subject, in one run call; the revert's target is
    among them."""
    steps = [
        {"expand": {"from": FILE_ID, "edge_type": "affects", "direction": "in",
                    "where": {"attrs.is_merge": False,
                              "attrs.committed_at": {"op": "gt", "value": "2024-01-01"}}}},
        {"map": {"where": "$1", "select": [
            "node_id", "attrs.author", "attrs.committed_at", "attrs.subject"]}},
    ]
    reply = history.json(steps)
    assert _row(reply, 1)["count"] == 4
    rows = {row["node_id"]: row for row in _result(reply, 2)["items"]}
    assert sorted(rows) == ["commit:c3", "commit:c4", "commit:c5", "commit:c6"]
    # The fields arrive with each row (however map names them): the revert
    # and its target are both there to be paired by subject.
    for commit_id, subject, author in (
        ("commit:c3", "Tune daemons", "cy"), ("commit:c4", 'Revert "Tune daemons"', "ana"),
    ):
        row = json.dumps(rows[commit_id])
        assert json.dumps(subject)[1:-1] in row and author in row, row


def test_join_counts_starts_without_a_link(history):
    """run-link-join: an expand step from "$1" reads the far records of the
    whole set, counts the starting records with no such link and names
    them, and says links to records not in the graph are not counted."""
    steps = [
        {"filter": {"where": {"type": "git_commit", "attrs.ticket_key": {"op": "exists"}}}},
        {"expand": {"from": "$1", "edge_type": "references", "direction": "out"}},
    ]
    reply = history.json(steps)
    join = _row(reply, 2)["join"]
    assert join == {
        "from_step": 1, "edge_type": "references", "direction": "out", "starts": 3,
        "starts_without_link": 1, "without_link_ids": ["commit:c4"], "reached": 2,
        "complete": False, "not_counted": "cannot count links to records not in the graph",
    }
    assert sorted(_ids(_result(reply, 2)["items"])) == ["ticket:T-1", "ticket:T-2"]
    lines = _step_text(history.text(steps), 2)
    assert lines[0] == "[step 2] expand: 2 records"
    assert lines[1] == (
        "! join from step 1: 1 of 3 starting records have no references out "
        "link (commit:c4); 2 records reached"
    )
    assert lines[2] == "! join: cannot count links to records not in the graph"
    # A later step reads the join's whole set.
    counted = history.json([*steps[:1], {**steps[1], "show": False},
                            {"aggregate": {"where": "$2", "by": ["subtype"], "metrics": ["count"]}}])
    assert [(g["subtype"], g["count"]) for g in _result(counted, 3)["groups"]] == [("ticket", 2)]


def test_join_always_says_it_cannot_count_unloaded_targets(history):
    """Every join says it, also over a link type with no missing target."""
    reply = history.json([
        {"filter": {"where": {"type": "git_commit"}}},
        {"expand": {"from": "$1", "edge_type": "affects", "direction": "out"}},
    ])
    join = _row(reply, 2)["join"]
    assert join["starts"] == 6 and join["starts_without_link"] == 0 and join["reached"] == 1
    assert join["complete"] is False
    assert join["not_counted"] == "cannot count links to records not in the graph"
    # Failure case: a join names one link type and direction.
    refused = history.json([
        {"filter": {"where": {"type": "git_commit"}}},
        {"expand": {"from": "$1", "direction": "out"}},
    ])
    assert "names one link type (edge_type) and direction" in _error(_row(refused, 2))["message"]
