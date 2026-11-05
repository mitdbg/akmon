"""Stored totals through the served tools (PACT read-stored-metrics).

`aggregate` with sum, min and max over a stored scope and over a result
reference, on a published graph whose `attrs.n` holds every case the one
number rule and the one grouping rule name: 1, 1.0, -0.0, 0, 100, 1e2 (planted
as jsonb text), "1", null, absent, number text the rule refuses, other text,
a bool, a high-precision decimal (planted), an integer above 2^53, and two
overlay values (one on a record that carries no `n` in the graph).

Every call goes through one MCP client session, as a client reads it. Each
test fails on the base (`3d646f6e53`), where `aggregate` refuses every metric
but count on a stored set ("aggregate counts graph records only") and `map`
calls a typo'd type "a stored set"; the docstrings say where a test pins
something else.
"""
from __future__ import annotations

import json
import os
import re
import time
from typing import Any

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.semantic_readiness import resolve_node_segment
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes
from tests.substrate.mcp.test_loose_match_db import (
    _GRAPH_READ,
    Tools,
    _record_statements,
)
from tests.substrate.mcp.test_search_list import MAIN, TERM, _node, _trust

pytestmark = pytest.mark.postgres

ITEM = SubtypeSpec("item", metadata={
    "searchable_text_fields": ["text"], "created_at_from": "opened_at",
    "filterable_attrs": {"label": "text"},
})
NOTE = SubtypeSpec("note", metadata={"searchable_text_fields": ["text"]})
ABSENT = object()
#: Planted with SQL after the publish: jsonb text Python's json cannot write.
PLANTED = [
    ("item:06", "n", "1e2"), ("item:18", "n", "0.1000000000000000055"),
    # Re-review R1: the same characters as a number (item:18) and as text
    # (item:19, published as text) in one more key.
    ("item:18", "n2", "0.1000000000000000055"),
    # Re-review R4: the each() count reads list elements exactly too.
    ("item:18", "tags", "[0.1000000000000000055]"),
    # Re-review N1, N2: the number inside a list and an object key.
    ("item:18", "lst", "[0.1000000000000000055]"),
    ("item:18", "obj", '{"v": 0.1000000000000000055}'),
]
EXACT = "0.1000000000000000055"
VALUES: dict[str, Any] = {
    "item:01": 1, "item:02": 1.0, "item:03": -0.0, "item:04": 0, "item:05": 100,
    "item:06": 0, "item:07": "1", "item:08": None, "item:09": ABSENT,
    "item:10": " 10 ", "item:11": "+5", "item:12": "010", "item:13": ".5",
    "item:14": "1_000", "item:15": "10", "item:16": "abc", "item:17": True,
    "item:18": 0, "item:19": 0.1, "item:20": 9007199254740993,
    "item:21": 9007199254740992.0, "item:22": 2.5, "item:23": ABSENT,
}
#: Overlay values at the published generation (the merged branch).
OVERLAY = {"item:22": 7, "item:23": 4}
WHOLE = 1 + 1 + 0 + 0 + 100 + 100 + 1 + 10 + 9007199254740993 + 9007199254740992 + 7 + 4
SUM = {"sum(attrs.n)": WHOLE, "sum(attrs.n) floats": 0.2}
SKIPPED = {
    "null": {"count": 1, "first_ids": ["item:08"]},
    "absent": {"count": 1, "first_ids": ["item:09"]},
    "not_a_number": {"count": 2, "first_ids": ["item:16", "item:17"]},
    "number_text_rejected": {
        "count": 5, "first_ids": ["item:10", "item:11", "item:12", "item:13", "item:14"],
    },
    "read_from_text": 2,
}
SCOPE = {"where": {"type": "item"}}


def _owner(node_id: str) -> str:
    return "a" if int(node_id.split(":")[1]) % 2 else "b"


def _items() -> list:
    out = []
    for node_id, value in VALUES.items():
        attrs: dict[str, Any] = {
            "owner": _owner(node_id), "label": "x",
            "opened_at": f"2025-0{1 + int(node_id[-1]) % 9}-01T00:00:00Z",
        }
        if value is not ABSENT:
            attrs["n"] = value
        if node_id == "item:18":
            attrs["n2"] = 0
            attrs["tags"] = [0]
            attrs["lst"], attrs["obj"] = [0], {"v": 0}
        if node_id == "item:19":
            # The same month as item:18, so a period count would merge the two
            # if it took the number for the text.
            attrs["n2"] = EXACT
            attrs["opened_at"] = "2025-09-01T00:00:00Z"
            attrs["tags"] = [0.1]
            attrs["lst"], attrs["obj"] = [EXACT], {"v": EXACT}
        out.append(_node(node_id, "item", f"{TERM} {node_id}.", **attrs))
    out.append(_node("note:1", "note", f"{TERM} note.", n=5))
    return out


def _deployment(tmp_path, dsn: str) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    _catalog, generation = publish_nodes(dsn, deployment, subtypes=[ITEM, NOTE], nodes=_items())
    _trust(dsn, deployment, MAIN)
    with psycopg.connect(dsn, autocommit=True) as conn:
        table = resolve_node_segment(conn)
        for node_id, key, text in PLANTED:
            conn.execute(
                f"UPDATE {table} SET attrs = jsonb_set(attrs, '{{{key}}}', %s::jsonb) "
                "WHERE node_id = %s",
                (text, node_id),
            )
        for node_id, value in OVERLAY.items():
            conn.execute(
                "INSERT INTO okg.attribute_effective_attrs (target_kind, target_id, "
                "generation_id, namespace, attr_key, observation_id, value, value_hash, "
                "producer) VALUES ('node', %s, %s, 'default', 'n', %s, %s::jsonb, %s, "
                "'test:stored-metrics')",
                (node_id, generation, f"obs:{node_id}", json.dumps(value), f"h{node_id}"),
            )
    return deployment


@pytest.fixture
def tools(clean_runtime, okg_mcp_can_log_in, tmp_path):
    server = Tools(clean_runtime, _deployment(tmp_path, clean_runtime))
    try:
        yield server
    finally:
        server.close()


def _ok(reply: dict) -> dict:
    assert reply.get("status") != "refused", json.dumps(reply)[:2000]
    return reply


def _refused(reply: dict, reason: str) -> dict:
    assert reply.get("status") == "refused", json.dumps(reply)[:2000]
    error = reply["error"]
    assert (error.get("data") or {}).get("reason") == reason, json.dumps(error)[:2000]
    return error


def _sum(tools, **extra) -> dict:
    return tools.call("aggregate", {**SCOPE, "by": [], "metrics": [
        "sum(attrs.n)", "min(attrs.n)", "max(attrs.n)",
    ], **extra})


def _reference(tools) -> str:
    reply = _ok(tools.call("search", {
        "query": TERM, "method": "lexical", "retain": True, "limit": 50,
        "where": {"type": "item"},
    }))
    return str(reply["result_reference"]["id"])


# ------------------------------------------------------------------ the totals


def test_stored_sum_min_max_on_a_scope_and_a_reference_agree(tools) -> None:
    scope = _ok(_sum(tools))
    group = scope["groups"][0]
    assert {k: group[k] for k in SUM} == SUM
    assert group["min(attrs.n)"] == 0 and group["max(attrs.n)"] == 9007199254740993
    assert scope["records_read"] == len(VALUES)
    assert scope["sum_not_exact"] == ["sum(attrs.n)"]
    ref = _reference(tools)
    referenced = _ok(tools.call("aggregate", {
        "input": {"ref": ref}, "by": [], "metrics": ["sum(attrs.n)", "min(attrs.n)", "max(attrs.n)"],
    }))
    assert referenced["groups"] == scope["groups"]
    assert referenced["values_skipped"] == scope["values_skipped"]
    # Grouped by a stored field: each owner's total.
    owners = _ok(tools.call("aggregate", {**SCOPE, "by": ["attrs.owner"],
                                          "metrics": ["count", "sum(attrs.n)"]}))
    by_owner = {g["attrs.owner"]: g for g in owners["groups"]}
    assert by_owner["a"]["count"] + by_owner["b"]["count"] == len(VALUES)
    total = sum(g["sum(attrs.n)"] for g in by_owner.values())
    assert total == WHOLE


def test_the_column_read_reads_only_the_named_keys(tools, monkeypatch) -> None:
    seen = _record_statements(monkeypatch)
    _ok(tools.call("aggregate", {**SCOPE, "by": ["attrs.owner"], "metrics": ["sum(attrs.n)"]}))
    graph = [s for s in seen if _GRAPH_READ.search(s)]
    counts = [s for s in graph if s.startswith("SELECT count(*) FROM (SELECT 1 ")]
    reads = [s for s in graph if "jsonb_typeof(" in s and " ? " in s]
    assert len(counts) == 1 and len(reads) == 1, [s[:160] for s in graph]
    assert "LIMIT 50001" in counts[0]
    head = reads[0].split(" FROM ", 1)[0]
    # The select list: id, subtype, and per key its text, type and presence.
    assert head.count("->>") == 2 and head.count("jsonb_typeof(") == 2
    assert re.search(r"effective_aggregate\.attrs\s*(,|$)", head) is None, head
    assert "'n'" in head and "'owner'" in head and "'label'" not in head


def test_order_by_ranks_a_stored_sum(tools) -> None:
    reply = _ok(tools.call("aggregate", {
        **SCOPE, "by": ["attrs.owner"], "metrics": ["count", "max(attrs.n)"],
        "order_by": "max(attrs.n) asc",
    }))
    assert [g["rank"] for g in reply["groups"]] == [1, 2]
    values = [g["max(attrs.n)"] for g in reply["groups"]]
    assert values == sorted(values)
    assert reply["order_by"] == {"name": "max(attrs.n)", "direction": "asc", "kind": "metric"}
    missing = tools.call("aggregate", {
        **SCOPE, "by": ["attrs.owner"], "metrics": ["count"], "order_by": "sum(attrs.n) desc",
    })
    error = missing["error"]
    assert error["data"]["reason"] == "order_by_not_in_metrics", error
    assert "computes count only" not in json.dumps(missing)


@pytest.mark.parametrize("by", [["month(date.created)"], ["match"], ["each(attrs.tags)"]])
def test_a_period_or_match_key_beside_sum_is_refused(tools, by) -> None:
    error = _refused(tools.call("aggregate", {**SCOPE, "by": by, "metrics": ["sum(attrs.n)"]}),
                     "stored_metric_by")
    assert by[0] in error["message"]
    assert 'metrics=["count"]' in error["fix_hint"]


def test_a_run_aggregate_step_with_sum_equals_the_call_alone_and_dollar_n_is_its_records(
    tools,
) -> None:
    alone = _ok(tools.call("aggregate", {**SCOPE, "by": ["attrs.owner"], "metrics": ["sum(attrs.n)"]}))
    ran = tools.call("run", {"steps": [
        {"aggregate": {**SCOPE, "by": ["attrs.owner"], "metrics": ["sum(attrs.n)"]},
         "show": True},
        {"aggregate": {"where": "$1", "by": ["subtype"], "metrics": ["count"]}},
    ]})
    first, second = ran["steps"][0], ran["steps"][1]
    assert first["status"] == "ok", first
    assert sorted(first["result"]["groups"], key=str) == sorted(alone["groups"], key=str)
    assert second["status"] == "ok", second
    assert [(g["subtype"], g["count"]) for g in second["result"]["groups"]] == [
        ("item", len(VALUES)),
    ]


# ------------------------------------------------------------- skips, groups, cover


def test_skipped_values_are_named_by_class_with_ids(tools) -> None:
    reply = _ok(_sum(tools))
    assert reply["values_skipped"] == {"attrs.n": SKIPPED}
    text = tools.call("aggregate", {**SCOPE, "by": [], "metrics": ["sum(attrs.n)"]}, "text")
    assert (
        "skipped: 1 null (first ids: item:08), 1 absent (first ids: item:09), 2 not a "
        "number (first ids: item:16, item:17), 5 number text rejected (first ids: "
        "item:10, item:11, item:12, item:13, item:14); 2 read from text"
    ) in text, text
    # The number() route is a live set's; a stored set is not offered it in A.
    assert "number(attrs.n)" not in text


def _count_groups(tools) -> tuple:
    reply = _ok(tools.call("aggregate", {**SCOPE, "by": ["attrs.n"], "metrics": ["count"],
                                         "limit": 100}))
    return sorted((json.dumps(g["attrs.n"]), g["count"]) for g in reply["groups"]), reply


def test_memory_and_sql_groups_are_equal(tools) -> None:
    counted, count_reply = _count_groups(tools)
    summed = _ok(tools.call("aggregate", {**SCOPE, "by": ["attrs.n"],
                                          "metrics": ["count", "max(attrs.owner)"],
                                          "limit": 100}))
    in_memory = sorted((json.dumps(g["attrs.n"]), g["count"]) for g in summed["groups"])
    assert counted == in_memory
    assert ("1", 2) in counted and ("0", 2) in counted and ("100", 2) in counted
    assert ('"1"', 1) in counted and ("null", 1) in counted and ('"(absent)"', 1) in counted
    # 0.1000000000000000055 and 0.1 are two groups (jsonb keeps them apart).
    # The first is served as a number literal with all its digits, named in
    # exact_numbers; a client reading JSON numbers as floats sees 0.1 twice
    # (test_an_exact_number_stays_a_number_on_every_path reads it exactly).
    assert sorted(c for k, c in counted if k == "0.1") == [1, 1]
    assert count_reply["exact_numbers"] == ["attrs.n"] == summed["exact_numbers"]
    assert ("9007199254740993", 1) in counted and ("9007199254740992", 1) in counted
    assert count_reply["absent_keys"] == {"attrs.n": 1} == summed["absent_keys"]


def test_stored_totals_say_what_they_cover_exactly(tools) -> None:
    reply = _ok(tools.call("aggregate", {**SCOPE, "by": [], "metrics": ["sum(attrs.n)"]}))
    generation = reply["generation_id"]
    assert reply["totals_cover"] == (
        f"sum(attrs.n) over 14 of 23 records at generation {generation}; skipped: 1 null "
        "(first ids: item:08), 1 absent (first ids: item:09), 2 not a number (first ids: "
        "item:16, item:17), 5 number text rejected (first ids: item:10, item:11, item:12, "
        "item:13, item:14); 2 read from text"
    )
    assert "at least" not in reply["totals_cover"]
    assert reply["total_count_basis"] == "exact"
    assert reply["sum_not_exact"] == ["sum(attrs.n)"]


# ------------------------------------------------------------- bound and budget


def test_a_sum_over_the_bound_is_refused_before_the_column_read(tools, monkeypatch) -> None:
    from okg.substrate.mcp import stored_metrics

    monkeypatch.setattr(stored_metrics, "MAX_ROWS", 5)
    seen = _record_statements(monkeypatch)
    error = _refused(_sum(tools), "stored_set_too_large")
    assert error["data"]["records"] == len(VALUES)
    assert f"this set holds {len(VALUES)}" in error["message"]
    assert error["retry_call"]["arguments"]["metrics"] == ["count"]
    assert not [s for s in seen if "jsonb_typeof(" in s and " ? " in s]
    assert [s for s in seen if "LIMIT 6" in s]


def test_a_spent_budget_refuses_naming_the_phase(tools, monkeypatch) -> None:
    from okg.substrate.mcp import stored_metrics

    monkeypatch.setattr(stored_metrics, "BUDGET_MS", 0)
    error = _refused(_sum(tools), "stored_read_over_budget")
    assert error["data"]["phase"] == "count"
    assert "no value is served" in error["message"]
    # The count fits; the column read finds the budget spent.
    left = iter([5_000, 0, 0])
    monkeypatch.setattr(stored_metrics, "BUDGET_MS", 5_000)
    monkeypatch.setattr(stored_metrics.Budget, "remaining_ms", lambda self: next(left, 0))
    error = _refused(_sum(tools), "stored_read_over_budget")
    assert error["data"]["phase"] == "column read"
    assert "groups" not in json.dumps(error)


def test_a_date_conditioned_sum_shares_the_date_budget(tools, monkeypatch) -> None:
    from okg.substrate.mcp import stored_metrics

    budgets: list = []
    original = stored_metrics.Budget.__init__

    def record(self, started_at, date_budget=None):
        budgets.append(date_budget)
        original(self, started_at, date_budget)

    monkeypatch.setattr(stored_metrics.Budget, "__init__", record)
    reply = _ok(tools.call("aggregate", {
        "where": {"type": "item", "date.created": {"op": "between",
                                                    "values": ["2025-01-01", "2025-12-31"]}},
        "by": [], "metrics": ["sum(attrs.n)"],
    }))
    assert budgets and budgets[-1] is not None
    assert budgets[-1].total_ms == stored_metrics.BUDGET_MS
    assert reply["groups"][0]["sum(attrs.n)"] == WHOLE


# ------------------------------------------------------------- refusals


@pytest.mark.parametrize("name", ["nope", "iten"])
def test_an_unknown_type_names_the_closest_types(tools, name) -> None:
    for tool, arguments in (
        ("aggregate", {"where": {"type": name}, "by": [], "metrics": ["sum(attrs.n)"]}),
        ("map", {"where": {"type": name}, "compute": {"x": "len(node_id)"}}),
    ):
        error = _refused(tools.call(tool, arguments), "unknown_type")
        assert error["message"].startswith(f'{tool}: no record type is named "{name}"')
        assert "stored set" not in error["message"]
        assert error["data"]["closest"], error
    assert _refused(tools.call("map", {"where": {"type": "iten"}, "compute": {"x": "len(node_id)"}}),
                    "unknown_type")["data"]["closest"][0] == "item"


def test_a_metric_on_a_declared_text_attribute_is_refused_before_any_read(tools, monkeypatch) -> None:
    seen = _record_statements(monkeypatch)
    error = _refused(tools.call("aggregate", {**SCOPE, "by": [], "metrics": ["sum(attrs.label)"]}),
                     "stored_metric_needs_number")
    assert "declared text" in error["message"] and "number()" in error["fix_hint"]
    assert [s for s in seen if _GRAPH_READ.search(s)] == []


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_refusal_order_under_access_rules_reveals_no_names(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, mode,
) -> None:
    deployment = _deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    served = Tools(clean_runtime, deployment)
    try:
        for arguments in (
            {"where": {"type": "nope"}, "by": [], "metrics": ["sum(attrs.n)"]},
            {**SCOPE, "by": [], "metrics": ["sum(attrs.label)"]},
        ):
            error = _refused(served.call("aggregate", arguments),
                             "stored_metric_under_access_rules")
            assert "closest" not in json.dumps(error) and "declared" not in json.dumps(error)
    finally:
        served.close()


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_stored_metrics_are_refused_under_audit_and_enforce_with_zero_reads(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, mode,
) -> None:
    from okg.substrate.mcp import server as srv

    deployment = _deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    mcp = srv.build_server(dsn=clean_runtime, deployment_name=deployment)
    try:
        seen = _record_statements(monkeypatch)
        with pytest.raises(SubstrateError) as caught:
            mcp._tool_manager._tools["aggregate"].fn(
                input={"kind": "nodes", "subtype": "item"}, by=[], metrics=["sum(attrs.n)"],
            )
        error = caught.value
        assert error.data["reason"] == "stored_metric_under_access_rules"
        assert error.data["access_rules"] == "on"
        assert str(error) == (
            "aggregate: stored sums are not served while access rules are on "
            "(audit/enforce); this is a later change"
        )
        assert error.fix_hint == (
            'Still served: aggregate(metrics=["count"]), filter and search'
        )
        assert [s for s in seen if _GRAPH_READ.search(s)] == []
        described = mcp._tool_manager._tools["aggregate"].description
        assert "not while access rules are on" in described
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_count_and_references_under_access_rules_work_as_on_dev(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch,
) -> None:
    """Not a must-fail: a count under access rules is refused as on dev,
    with dev's words, and a sum is refused by its own reason."""
    deployment = _deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    served = Tools(clean_runtime, deployment)
    try:
        count = served.call("aggregate", {**SCOPE, "by": ["subtype"], "metrics": ["count"]})
        assert count["status"] == "refused"
        assert "counting is not available" in count["error"]["message"]
        # Re-review R6: a by=[] count refusal echoes the by=[] the caller sent.
        total = served.call("aggregate", {**SCOPE, "by": [], "metrics": ["count"]})
        assert "counting is not available" in total["error"]["message"]
        assert total["error"]["data"].get("by", []) == [], total
        assert 'by=["subtype"]' not in json.dumps(total)  # N5: no echo anywhere
        summed = served.call("aggregate", {**SCOPE, "by": [], "metrics": ["sum(attrs.n)"]})
        assert summed["error"]["data"]["reason"] == "stored_metric_under_access_rules"
    finally:
        served.close()


# ------------------------------------------------------------- teaching and cost


def test_served_examples_execute(tools) -> None:
    """The description's and the docs' stored-total examples run here."""
    described = tools.mcp._tool_manager._tools["aggregate"].description
    assert '"sum(attrs.size)"' in described and "50,000 records" in described
    for arguments in (
        {"where": {"type": "item"}, "by": [], "metrics": ["sum(attrs.n)"]},
        {"where": {"type": "item"}, "by": ["attrs.owner"], "metrics": ["count", "max(attrs.n)"],
         "order_by": "max(attrs.n) desc"},
    ):
        _ok(tools.call("aggregate", arguments))


#: The cost run's size: CI runs a small set for the shape of the reads; a lab
#: host sets OKG_STORED_METRICS_COST_N=50000 (and _KB for the attribute size)
#: and attaches the printed numbers as evidence.
COST_N = int(os.environ.get("OKG_STORED_METRICS_COST_N", "600"))
COST_KB = int(os.environ.get("OKG_STORED_METRICS_COST_KB", "7"))


def test_costs_at_50000_records(clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch) -> None:
    from okg.substrate.mcp import stored_metrics

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes = [
        _node(f"item:{i:06d}", "item", f"{TERM} {i}.", n=i % 97, owner=f"o{i % 7}",
              opened_at="2025-03-01T00:00:00Z")
        for i in range(COST_N)
    ]
    publish_nodes(clean_runtime, deployment, subtypes=[ITEM, NOTE], nodes=nodes)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        table = resolve_node_segment(conn)
        # Large, TOASTed attributes (random text compresses poorly).
        words = max(1, COST_KB * 1024 // 33)
        conn.execute(
            f"UPDATE {table} n SET attrs = n.attrs || jsonb_build_object('description', "
            f"(SELECT string_agg(md5(random()::text || n.node_id || g), ' ') "
            f"FROM generate_series(1, {words}) g))"
        )
        conn.execute(f"VACUUM ANALYZE {table}")
        size = conn.execute(f"SELECT avg(pg_column_size(attrs)) FROM {table}").fetchone()[0]
    served = Tools(clean_runtime, deployment)
    out = []
    try:
        cases = [
            ("sum over a scope", {**SCOPE, "by": [], "metrics": ["sum(attrs.n)"]}),
            ("sum over a scope with a date condition", {
                "where": {"type": "item", "date.created": {
                    "op": "between", "values": ["2025-01-01", "2025-12-31"]}},
                "by": [], "metrics": ["sum(attrs.n)"]}),
        ]
        for label, arguments in cases:
            seen = _record_statements(monkeypatch)
            started = time.perf_counter()
            reply = served.call("aggregate", arguments)
            wall = time.perf_counter() - started
            graph = [s for s in seen if _GRAPH_READ.search(s)]
            out.append((label, wall, len(graph), reply.get("status"),
                        (reply.get("error") or {}).get("data")))
            # No full row is read: the select lists hold no bare attrs column.
            for statement in graph:
                head = statement.split(" FROM ", 1)[0]
                assert not re.search(r"effective_(aggregate|page)\.attrs\s*(,|$)", head), head
            if reply.get("status") != "refused":
                assert reply["groups"][0]["sum(attrs.n)"] == sum(i % 97 for i in range(COST_N))
            else:
                assert reply["error"]["data"]["reason"] == "stored_read_over_budget", reply
        monkeypatch.setattr(stored_metrics, "MAX_ROWS", COST_N - 1)
        seen = _record_statements(monkeypatch)
        started = time.perf_counter()
        reply = served.call("aggregate", {**SCOPE, "by": [], "metrics": ["sum(attrs.n)"]})
        out.append(("over-bound refusal", time.perf_counter() - started,
                    len([s for s in seen if _GRAPH_READ.search(s)]), reply.get("status"),
                    (reply.get("error") or {}).get("data")))
        assert reply["error"]["data"]["reason"] == "stored_set_too_large"
    finally:
        served.close()
    load = os.getloadavg()
    for label, wall, statements, status, data in out:
        print(f"COST n={COST_N} attrs_bytes={float(size):.0f} {label}: {wall:.3f}s "
              f"graph_statements={statements} status={status} data={data} load={load}")


# ------------------------------------------------------------- one total (by=[])
#
# Calls refused on the base in the qs10 read eval (lane re, records.jsonl of
# re-qs10-G0 and re-qs10-GC2): 47 `aggregate` calls with by=[] over a graph
# set, 18 of them as run steps, every one refused "by must name 1..4 fields;
# only a kept live answer ... may omit it for one total". The arguments are
# copied from the traces with the fixture's type and fields in place of
# jira_issue and its fields.

#: qs-01 t10 (G0): a count per year, one call per year.
QS01 = {"where": {"type": "item", "attrs.owner": "a",
                  "attrs.opened_at": {"op": "between", "values": ["2025-01-01", "2025-12-31"]}},
        "by": [], "metrics": ["count"]}
#: qs-02 t4 (G0): a text condition beside a node id prefix.
QS02 = {"where": {"type": "item", "node_id": {"op": "prefix", "value": "item:1"},
                  "text": {"op": "matches", "value": "re:/item:1[0-4]/"}},
        "by": [], "metrics": ["count"]}
OWNER_A = sum(1 for node_id in VALUES if _owner(node_id) == "a")


def test_one_total_count_over_a_stored_set(tools) -> None:
    for arguments, expected in ((QS01, OWNER_A), (QS02, 5)):
        reply = _ok(tools.call("aggregate", arguments))
        assert reply["groups"] == [{"count": expected}], reply["groups"]
        assert reply["by"] == [] and reply["total_count"] == 1
        assert reply["total_count_basis"] == "exact"
    ref = _reference(tools)
    referenced = _ok(tools.call("aggregate", {"input": {"ref": ref}, "by": [],
                                              "metrics": ["count"]}))
    assert referenced["groups"] == [{"count": len(VALUES)}]
    ranked = _ok(tools.call("aggregate", {**QS01, "order_by": "count desc"}))
    assert ranked["groups"] == [{"rank": 1, "count": OWNER_A}]
    text = tools.call("aggregate", QS01, "text")
    assert "may omit it for one total" not in text


def test_one_total_count_as_a_run_step(tools) -> None:
    # qs-02 t10 (GC2) and qs-01 t9 (GC2: where "$1" after a graph step).
    ran = tools.call("run", {"steps": [
        {"aggregate": {"where": {"type": "item", "attrs.owner": "a"}, "by": [],
                       "metrics": ["count"]}, "show": True},
        {"filter": {"where": {"type": "item", "attrs.owner": "b"}}},
        {"aggregate": {"where": "$2", "by": [], "metrics": ["count"]}},
    ]})
    first, third = ran["steps"][0], ran["steps"][2]
    assert first["status"] == "ok", first
    assert first["result"]["groups"] == [{"count": OWNER_A}]
    assert third["status"] == "ok", third
    assert third["result"]["groups"] == [{"count": len(VALUES) - OWNER_A}]


def test_a_count_by_a_field_and_a_date_period_prints_numbers_as_numbers(tools) -> None:
    """Review H2: the period fold prints grouped numbers canonically, never
    as text (base printed 1.0 and 0.1 as numbers; the first build printed
    "1.0" and "0.1")."""
    reply = _ok(tools.call("aggregate", {
        **SCOPE, "by": ["attrs.n", "month(date.created)"], "metrics": ["count"], "limit": 100,
    }))
    values = [g["attrs.n"] for g in reply["groups"]]
    numbers = [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]
    assert 1 in numbers and 0.1 in numbers and 9007199254740992 in numbers, values
    assert all(isinstance(v, int) for v in numbers if float(v).is_integer()), values
    texts = {v for v in values if isinstance(v, str)}
    assert texts == {"1", " 10 ", "+5", "010", ".5", "1_000", "10", "abc"}, texts
    # A period count does not tell a missing key from a stored null (as on
    # the base): both are the null group.
    assert None in values
    assert reply["exact_numbers"] == ["attrs.n"]


def test_one_total_over_an_unknown_type_is_refused_not_counted_as_zero(tools) -> None:
    """Review M1: by=[] over a type that does not exist names the closest
    types; it is never an exact 0."""
    error = _refused(tools.call("aggregate", {"where": {"type": "iten"}, "by": [],
                                              "metrics": ["count"]}), "unknown_type")
    assert error["data"]["closest"][0] == "item"


def _exact(tools, tool: str, arguments: dict) -> dict:
    """The JSON reply read with every number exact (Decimal), as a client
    that keeps the digits reads it."""
    import anyio
    from decimal import Decimal

    from mcp.shared.memory import create_connected_server_and_client_session

    out: list = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(tools.mcp) as client:
            result = await client.call_tool(
                tool, arguments, meta={"okg/result_format": "json"},
            )
            out.append(json.loads(result.content[0].text, parse_float=Decimal))

    anyio.run(drive)
    return _ok(out[0])


def test_an_exact_number_stays_a_number_on_every_path(tools) -> None:
    """Re-review R1: a stored number no float holds is a number literal with
    all its digits on the count, sum and date-period paths, apart from the
    text with the same characters, and ranks among the numbers."""
    from decimal import Decimal

    number, text = Decimal(EXACT), EXACT
    calls = {
        "count": {**SCOPE, "by": ["attrs.n2"], "metrics": ["count"]},
        "sum": {**SCOPE, "by": ["attrs.n2"], "metrics": ["count", "max(attrs.n)"]},
        "period": {**SCOPE, "by": ["attrs.n2", "month(date.created)"], "metrics": ["count"]},
    }
    for name, arguments in calls.items():
        reply = _exact(tools, "aggregate", arguments)
        keys = [g["attrs.n2"] for g in reply["groups"]]
        numbers = [k for k in keys if isinstance(k, Decimal)]
        texts = [k for k in keys if isinstance(k, str) and k == text]
        assert numbers == [number] and texts == [text], (name, keys)
        assert reply["exact_numbers"] == ["attrs.n2"], (name, reply)
    ranked = _exact(tools, "aggregate", {**calls["count"], "order_by": "attrs.n2"})
    order = [g["attrs.n2"] for g in ranked["groups"]]
    assert order[0] == number and isinstance(order[0], Decimal), order
    assert order[1] == text, order
    raw = tools.call("aggregate", calls["count"], "text")
    assert EXACT in raw


def test_each_counts_read_list_numbers_exactly(tools) -> None:
    """Re-review R4: an each() count keeps 0.1000000000000000055 and 0.1
    apart in print as in SQL."""
    from decimal import Decimal

    reply = _exact(tools, "aggregate", {**SCOPE, "by": ["each(attrs.tags)"],
                                        "metrics": ["count"]})
    keys = [g["each(attrs.tags)"] for g in reply["groups"]]
    numbers = sorted(k for k in keys if isinstance(k, Decimal))
    assert numbers == [Decimal("0.1"), Decimal(EXACT)], keys
    assert reply["exact_numbers"] == ["each(attrs.tags)"]


def test_a_typo_in_a_type_list_or_a_link_type_is_refused(tools) -> None:
    """Re-review R3: a type list with one unknown member, and an unknown
    link type, are refused unknown_type; never a count of the rest or 0."""
    error = _refused(tools.call("aggregate", {"where": {"type": ["item", "iten"]}, "by": [],
                                              "metrics": ["count"]}), "unknown_type")
    assert '"iten"' in error["message"] and error["data"]["closest"][0] == "item"
    error = _refused(tools.call("aggregate", {"input": {"kind": "edges", "edge_type": "nope_edge"},
                                              "by": [], "metrics": ["count"]}), "unknown_type")
    assert error["message"].startswith('aggregate: no link type is named "nope_edge"')


@pytest.mark.parametrize("field", ["attrs.lst", "attrs.obj"])
def test_an_exact_number_inside_a_list_or_object_key_stays_apart(tools, field) -> None:
    """Re-review N1, N2: a key holding a list or object with a number no
    float holds is served on the count, sum and date-period paths (the sum
    and period paths failed the call), apart from the same text, and
    exact_numbers names the field."""
    from decimal import Decimal

    def inner(key):
        return key[0] if isinstance(key, list) else (key or {}).get("v")

    calls = {
        "count": {**SCOPE, "by": [field], "metrics": ["count"]},
        "sum": {**SCOPE, "by": [field], "metrics": ["count", "max(attrs.n)"]},
        "period": {**SCOPE, "by": [field, "month(date.created)"], "metrics": ["count"]},
    }
    for name, arguments in calls.items():
        reply = _exact(tools, "aggregate", arguments)
        held = [inner(g[field]) for g in reply["groups"]
                if isinstance(g[field], (list, dict))]
        assert Decimal(EXACT) in held and EXACT in held, (name, held)
        assert [type(v) for v in held if v == Decimal(EXACT)] == [Decimal], (name, held)
        assert reply["exact_numbers"] == [field], (name, reply)


def test_min_and_max_keep_an_exact_number(tools) -> None:
    """Re-review N3: min and max of a number no float holds print it with
    all its digits, as the group key does."""
    from decimal import Decimal

    reply = _exact(tools, "aggregate", {**SCOPE, "by": ["attrs.n2"],
                                        "metrics": ["max(attrs.n2)", "min(attrs.n2)"]})
    numbers = [g for g in reply["groups"] if isinstance(g["attrs.n2"], Decimal)]
    assert numbers and numbers[0]["max(attrs.n2)"] == Decimal(EXACT), numbers
    assert numbers[0]["min(attrs.n2)"] == Decimal(EXACT)
