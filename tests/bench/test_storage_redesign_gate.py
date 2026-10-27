"""The pure parts of scripts/storage_redesign_gate.py (no database).

PACT lane-s-storage-redesign-s2, gate requirements: percentiles, the three
timing thresholds, the read-statement and planner-setting detectors, and the
independent evaluator the recompute uses.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parents[2] / "scripts" / "storage_redesign_gate.py"
_spec = importlib.util.spec_from_file_location("storage_redesign_gate_under_test", _PATH)
gate = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(gate)


def test_percentiles() -> None:
    assert gate.percentile([], 50) is None
    assert gate.percentile([3, 1, 2], 50) == 2
    assert gate.percentile([4, 1, 3, 2], 50) == 2.5
    runs = list(range(1, 11))
    assert gate.percentile(runs, 95) == 10  # nearest rank: ceil(9.5) = 10th
    assert gate.percentile(runs, 90) == 9
    assert gate.percentile([7.0], 95) == 7.0


@pytest.mark.parametrize("today,limit", [(10.0, 15.0), (25.0, 30.0), (100.0, 120.0)])
def test_sql_p50_limit_is_the_larger_of_20_percent_and_5_ms(today, limit) -> None:
    assert gate.sql_p50_limit(today) == pytest.approx(limit)


@pytest.mark.parametrize("today,limit", [(10.0, 20.0), (20.0, 30.0), (100.0, 150.0)])
def test_sql_p95_limit_is_the_larger_of_50_percent_and_10_ms(today, limit) -> None:
    assert gate.sql_p95_limit(today) == pytest.approx(limit)


def test_tool_p50_limit_is_ten_percent() -> None:
    assert gate.tool_p50_limit(200.0) == pytest.approx(220.0)


def test_gate_pair_fails_exactly_past_each_limit() -> None:
    today = {"sql_ms": [10.0] * 10, "tool_ms": [100.0] * 10}
    at_limit = {"sql_ms": [15.0] * 9 + [20.0], "tool_ms": [110.0] * 10}
    result = gate.gate_pair(today, at_limit)
    assert result["sql_p50"]["pass"] and result["sql_p95"]["pass"] and result["tool_p50"]["pass"]
    over = {"sql_ms": [15.01] * 9 + [20.01], "tool_ms": [110.01] * 10}
    result = gate.gate_pair(today, over)
    assert not result["sql_p50"]["pass"]
    assert not result["sql_p95"]["pass"]
    assert not result["tool_p50"]["pass"]


def test_read_statements_and_planner_settings() -> None:
    assert gate.is_read_statement("SELECT 1 FROM okg.attribute_effective_attrs o")
    assert gate.is_read_statement("WITH cut AS (SELECT id FROM public._chronos_b_interval_okg_graph_nodes_x)")
    assert not gate.is_read_statement("EXPLAIN SELECT 1 FROM okg.attribute_effective_view")
    assert not gate.is_read_statement("SELECT current_user")
    assert gate.sets_planner_option("SET LOCAL enable_seqscan = off")
    assert gate.sets_planner_option("select set_config('plan_cache_mode', 'force_custom_plan', true)")
    assert gate.sets_planner_option("SET max_parallel_workers_per_gather = 4")
    assert not gate.sets_planner_option("select set_config('statement_timeout', '5000', true)")
    assert not gate.sets_planner_option("SELECT 'enable_seqscan' AS name")


def test_a_map_page_is_read_by_its_projected_keys() -> None:
    page = {"items": [{"attrs_t": "2021", "attrs_v": "0.1", "provenance": {}},
                      {"attrs_t": None, "provenance": {}}]}
    assert gate.answer_projected(page, ["attrs.t", "attrs.v"]) == [["2021", "0.1"], [None, None]]
    assert gate.answer_projected(page, None) is None


def test_answers_ignore_volatile_fields_and_compare_groups_as_sets() -> None:
    a = {"items": [{"node_id": "a"}, {"node_id": "b"}], "total_count": 2,
         "publish_freshness": {"age": 1}, "notes": []}
    b = {**a, "publish_freshness": {"age": 9}}
    assert gate.digest(gate.answer_of(a)) == gate.digest(gate.answer_of(b))
    assert gate.answer_ids(a) == ["a", "b"]
    swapped = {**a, "items": list(reversed(a["items"]))}
    assert gate.digest(gate.answer_of(a)) != gate.digest(gate.answer_of(swapped))
    g1 = {"groups": [{"key": ["x"], "count": 2}, {"key": ["y"], "count": 1}]}
    g2 = {"groups": list(reversed(g1["groups"]))}
    assert gate.digest(gate.answer_of(g1)) == gate.digest(gate.answer_of(g2))
    nulls = {"groups": [{"key": [None], "count": 2}, {"key": [None], "count": 3}]}
    assert gate.answer_groups(nulls) == {"[null]": 5}


def test_the_evaluator() -> None:
    row = {"id": "n", "subtype": "doc", "attrs": {"a": 1, "b": "x", "n": None, "flag": True,
                                                  "tags": ["p", "q"]}}
    m = gate.matches
    assert m(row, {"op": "eq", "field": "attrs.a", "value": 1.0})
    assert not m(row, {"op": "eq", "field": "attrs.flag", "value": 1})
    assert m(row, {"op": "eq", "field": "attrs.n", "value": None})
    assert m(row, {"op": "eq", "field": "attrs.missing", "value": None})
    assert not m(row, {"op": "exists", "field": "attrs.missing"})
    assert m(row, {"op": "exists", "field": "attrs.n"})
    assert m(row, {"op": "between", "field": "attrs.a", "values": [0, 1]})
    assert not m(row, {"op": "between", "field": "attrs.b", "values": [0, 1]})
    assert m(row, {"op": "and", "clauses": [
        {"op": "eq", "field": "subtype", "value": "doc"},
        {"op": "or", "clauses": [{"op": "eq", "field": "attrs.b", "value": "y"},
                                 {"op": "gt", "field": "attrs.a", "value": 0}]}]})
    assert m(row, {"op": "not", "clause": {"op": "eq", "field": "attrs.b", "value": "y"}})
    with pytest.raises(ValueError):
        m(row, {"op": "prefix", "field": "attrs.b", "value": "x"})


def test_containment_is_object_containment_below_the_top() -> None:
    c = gate.json_contains
    assert c({"tags": ["p", "q"]}, {"tags": ["p"]})
    assert not c({"tags": ["p", "q"]}, {"tags": "p"})
    assert c({"meta": {"k": 1, "j": 2}}, {"meta": {"k": 1}})
    assert not c({"a": 1}, {"a": True})
    attrs = {"text": "Water sample", "site": "s1"}
    assert gate.supplement_matches(attrs, {"site": "s1"}, ["WATER", "sam"])
    assert not gate.supplement_matches(attrs, {"site": "s2"}, ["water"])
    assert not gate.supplement_matches({"site": "s1"}, {"site": "s1"}, ["water"])


def _block(digest: str, ids: list[str], **extra) -> dict:
    return {"answer_digest": digest, "rows_stable": True, "ids": ids, "groups": None,
            "supplement": [], **extra}


def test_row_checks_list_todays_refusal_and_stale_supplement_by_name() -> None:
    expected = {"ids": ["a", "b"]}
    same = gate.compare_rows("x", [_block("d1", ["a", "b"])], [_block("d1", ["a", "b"])], expected)
    assert same["paths_equal"] and same["recompute_equal"] and same["listed"] is None
    differ = gate.compare_rows("x", [_block("d0", ["a"])], [_block("d1", ["a", "b"])], expected)
    assert differ["paths_equal"] is False and differ["listed"] is None
    refused = gate.compare_rows(
        "x", [_block("dx", [], exception="SubstrateError",
                     exception_data_keys=["retained_from_ordinal"])],
        [_block("d1", ["a", "b"])], expected)
    assert refused["listed"] == gate.BELOW_WINDOW and refused["paths_equal"] is None
    wrong = gate.compare_rows("x", [_block("d1", ["a"])], [_block("d1", ["a"])], expected)
    assert wrong["recompute_equal"] is False
    # A page the response byte budget cut serves the recompute's first rows,
    # and its total must be the recompute's count.
    cut = {"ids": ["a", "b", "c"], "total_count": 9}
    short = gate.compare_rows("x", [_block("d1", ["a", "b"], budget_cut=True, total_count=9)],
                              [_block("d1", ["a", "b"], budget_cut=True, total_count=9)], cut)
    assert short["recompute_equal"]
    for ids, total, budget in ((["a", "c"], 9, True), (["a", "b"], 8, True),
                               (["a", "b"], 9, False), ([], 9, True)):
        bad = gate.compare_rows("x", [_block("d1", ids)],
                                [_block("d1", ids, budget_cut=budget, total_count=total)], cut)
        assert bad["recompute_equal"] is False, (ids, total, budget)
    # A capped count is a lower bound: the recompute's total must reach it.
    capped = {"ids": ["a", "b"], "total_count": 320_000}
    for total, ok in ((100_000, True), (400_000, False)):
        got = gate.compare_rows("x", [_block("d1", ["a", "b"])], [_block(
            "d1", ["a", "b"], total_count=total, total_count_basis="lower_bound")], capped)
        assert got["recompute_equal"] is ok, total
    assert not gate.compare_rows("x", [_block("d1", ["a", "b"])], [_block(
        "d1", ["a", "b"], total_count=100_000, total_count_basis="exact")], capped)["recompute_equal"]
    # A map page (no ids) compares its projected values.
    proj = {"ids": ["a", "b"], "projected": [["x", 1], ["y", 2]], "total_count": 2}
    assert gate.compare_rows("m", [_block("d1", [])], [_block(
        "d1", [], projected=[["x", 1], ["y", 2]], total_count=2)], proj)["recompute_equal"]
    assert not gate.compare_rows("m", [_block("d1", [])], [_block(
        "d1", [], projected=[["x", 1], ["y", 3]], total_count=2)], proj)["recompute_equal"]
    sup = {"supplement": [["a", "b"]]}
    stale = gate.compare_rows(
        "s", [_block("d0", [], supplement=[{"ids": ["a"]}])],
        [_block("d1", [], supplement=[{"ids": ["a", "b"]}])], sup)
    assert stale["listed"] == gate.STALE_SUPPLEMENT and stale["recompute_equal"]


def test_markdown_has_one_row_per_shape_and_the_verdicts() -> None:
    pair = gate.gate_pair({"sql_ms": [10.0], "tool_ms": [100.0]},
                          {"sql_ms": [30.0], "tool_ms": [100.0]})
    row = {"state": "head10", "shape": "a", "pairs": [pair], "paths_equal": True,
           "recompute_equal": True, "listed": None,
           "exec_p50": {"today": 9.0, "graph": 29.0}}
    verdict = {k: True for k in ("gate-sql-p50", "gate-sql-p95", "gate-tool-call",
                                 "gate-rows-equal", "gate-production-settings")}
    verdict["gate-sql-p50"] = False
    text = gate.render_markdown([row], verdict)
    assert "| head10 | a | 10.0 / 30.0 |" in text
    assert "| F | F | P | P / P |" in text
    assert "| gate-sql-p50 | FAIL |" in text and "| gate-tool-call | PASS |" in text


def test_rows_leave_out_the_facts_a_budget_bounded_count_decides() -> None:
    exact = {"items": [{"node_id": "a"}], "total_count": 636, "total_count_basis": "exact",
             "has_more": True, "truncated": True}
    unknown = {"items": [{"node_id": "a"}], "total_count": None,
               "total_count_basis": "unknown", "has_more": True, "truncated": True}

    def rows(result):
        return gate.digest(gate.rows_of(gate.answer_of(result)))

    assert gate.digest(gate.answer_of(exact)) != gate.digest(gate.answer_of(unknown))
    assert rows(exact) == rows(unknown)
    assert rows(dict(unknown, items=[{"node_id": "b"}])) != rows(unknown)
    # has_more is a row fact, and an answer that drops its count keys differs.
    assert rows(dict(unknown, has_more=False)) != rows(unknown)
    no_count = {k: v for k, v in exact.items() if k not in ("total_count", "total_count_basis")}
    assert rows(no_count) != rows(exact)
    assert gate.count_of(exact) == {"n": 636, "basis": "exact"}
    assert gate.count_of(unknown) == {"n": None, "basis": "unknown"}
    assert gate.count_of({"exception": "X"}) is None
    assert gate.count_of({"groups": []}) is None
    assert gate.count_of(no_count) is None


def _c(n, basis="exact"):
    return {"n": n, "basis": "unknown" if n is None else basis}


def _round(path, role="okg_mcp", t_end=None):
    """The round fields a bench block carries: the role and an end time
    (today before graph, so the pair alternates)."""
    return {"role": [role, "auto", "2"], "label": path,
            "t_end": t_end or ("2026-10-19T00:00:01Z" if path != "graph" else "2026-10-19T00:00:02Z")}


def test_rule_a_counts() -> None:
    # Today ran out of budget, the graph path counted: not a difference.
    got = gate.compare_counts([_c(None), _c(None)], [_c(636), _c(636)])
    assert got["agree"] and not got["lost"]
    # The graph path lost an exact count today had, in any call: its own failure.
    got = gate.compare_counts([_c(6366), _c(6366)], [_c(6366), _c(None)])
    assert got["agree"] and got["lost"]
    assert gate.compare_counts([_c(10)], [_c(5, "lower_bound")])["lost"]
    # Two known counts that differ, across or within a path.
    assert not gate.compare_counts([_c(636)], [_c(635)])["agree"]
    assert not gate.compare_counts([], [_c(636), _c(637)])["agree"]
    # A lower bound must not pass an exact count.
    assert gate.compare_counts([_c(900)], [_c(500, "lower_bound")])["agree"]
    assert not gate.compare_counts([_c(400)], [_c(500, "lower_bound")])["agree"]
    assert gate.compare_counts([None], [None]) == {"agree": True, "lost": False,
                                                   "today": [], "graph": []}


def test_row_checks_apply_rule_a_to_counts() -> None:
    expected = {"ids": ["a", "b"], "total_count": 636}

    def blk(*counts, ids=("a", "b"), digest="r1"):
        return _block("whole-" + "-".join(str(c["n"]) for c in counts), list(ids),
                      row_digest=digest, ids_stable=True, counts=list(counts),
                      rows_stable=len({c["n"] for c in counts}) == 1)

    # Today unknown, graph exact and equal to the recompute: rows equal, nothing lost.
    got = gate.compare_rows("b", [blk(_c(None))], [blk(_c(636))], expected)
    assert got["paths_equal"] and got["graph_stable"] and got["recompute_equal"]
    assert got["counts_agree"] and not got["count_lost"]
    # The graph count flips between exact and unknown: rows still stable.
    got = gate.compare_rows("h", [blk(_c(None))], [blk(_c(636), _c(None))], expected)
    assert got["graph_stable"] and got["paths_equal"] and not got["count_lost"]
    # Today exact, graph unknown in one call: counted as lost.
    got = gate.compare_rows("h", [blk(_c(636))], [blk(_c(636), _c(None))], expected)
    assert got["paths_equal"] and got["count_lost"]
    # A known graph count that is not the recompute's fails the recompute check.
    got = gate.compare_rows("h", [blk(_c(None))], [blk(_c(635))], expected)
    assert got["recompute_equal"] is False
    # Different rows are still a difference.
    got = gate.compare_rows("h", [blk(_c(None), digest="r0")], [blk(_c(636))], expected)
    assert got["paths_equal"] is False


def test_verdict_reports_a_lost_count_as_its_own_failure(tmp_path) -> None:
    import json

    def block(path, counts):
        shape = {"answer_digest": "w", "row_digest": "r", "rows_stable": True,
                 "ids_stable": True, "ids": ["a"], "groups": None, "supplement": [],
                 "counts": counts, "tool_ms": [100.0], "sql_ms": [10.0], "exec_ms": [9.0],
                 "sql_ms_returned": [4.0], "failed_statements": [1], "statements": []}
        return {"path": path, "state": "head10", "shapes": {"h": shape}, **_round(path)}

    files = []
    for name, data in (("t.json", block("materialized", [_c(1)])),
                       ("g.json", block("graph", [_c(None)]))):
        p = tmp_path / name
        p.write_text(json.dumps(data))
        files.append(str(p))
    out = tmp_path / "v.json"
    md = tmp_path / "v.md"
    rc = gate.main(["verdict", "--out", str(out), "--blocks", *files, "--markdown", str(md)])
    verdict = json.loads(out.read_text())["verdict"]
    assert rc == 1
    assert verdict["gate-rows-equal"] and not verdict["gate-count-kept"]
    assert verdict["count_became_unknown"] == [["head10", "h"]]
    assert "| became unknown |" in md.read_text()
    # The old measure (statements that returned only) sits beside the gate measure.
    assert "| 10.0 / 10.0 |" in md.read_text() and "| 4.0 / 4.0 | P | P |" in md.read_text()
    table = json.loads(out.read_text())["table"][0]
    assert table["pairs_returned_only"][0]["sql_p50"]["today"] == 4.0
    assert table["sql_p50_returned"] == {"today": 4.0, "graph": 4.0}
    assert table["failed_statements"] == {"today": 1, "graph": 1}


def test_verdict_fails_rows_when_known_counts_differ(tmp_path) -> None:
    import json

    files = []
    for name, path, n in (("t.json", "materialized", 636), ("g.json", "graph", 635)):
        shape = {"answer_digest": "w", "row_digest": "r", "rows_stable": True,
                 "ids_stable": True, "ids": ["a"], "groups": None, "supplement": [],
                 "counts": [_c(n)], "tool_ms": [100.0], "sql_ms": [10.0], "exec_ms": [9.0],
                 "statements": []}
        p = tmp_path / name
        p.write_text(json.dumps({"path": path, "state": "head10", "shapes": {"b": shape}, **_round(path)}))
        files.append(str(p))
    out = tmp_path / "v.json"
    assert gate.main(["verdict", "--out", str(out), "--blocks", *files]) == 1
    verdict = json.loads(out.read_text())["verdict"]
    assert not verdict["gate-rows-equal"] and verdict["gate-count-kept"]


def test_verdict_names_a_shape_that_passes_only_because_failed_statements_count(tmp_path) -> None:
    import json

    files = []
    # Today: a 0.5 s page plus a 5 s count the budget cancelled; the graph path:
    # a 2.6 s page and a 3 s count that finished. Gate measure 5.5 s vs 5.6 s
    # passes; the old measure 0.5 s vs 5.6 s fails, so the shape is named.
    for name, path, sql, returned, n in (("t.json", "materialized", 5500.0, 500.0, None),
                                         ("g.json", "graph", 5600.0, 5600.0, 636)):
        shape = {"answer_digest": "w" + name, "row_digest": "r", "rows_stable": True,
                 "ids_stable": True, "ids": ["a"], "groups": None, "supplement": [],
                 "counts": [_c(n)], "tool_ms": [6000.0], "sql_ms": [sql], "exec_ms": [9.0],
                 "sql_ms_returned": [returned], "failed_statements": [1 if n is None else 0],
                 "statements": []}
        p = tmp_path / name
        p.write_text(json.dumps({"path": path, "state": "head10", "shapes": {"h": shape}, **_round(path)}))
        files.append(str(p))
    out, md = tmp_path / "v.json", tmp_path / "v.md"
    assert gate.main(["verdict", "--out", str(out), "--blocks", *files,
                      "--markdown", str(md)]) == 0
    verdict = json.loads(out.read_text())["verdict"]
    assert verdict["gate-sql-p50"] and verdict["sql_passes_only_with_failed_time"] == [["head10", "h"]]
    assert "(the old measure fails them): head10 h" in md.read_text()


def test_verdict_rejects_owner_role_non_alternating_and_missing_states(tmp_path) -> None:
    import json

    def write(name, path, **round_kw):
        shape = {"answer_digest": "w", "row_digest": "r", "rows_stable": True,
                 "ids_stable": True, "ids": ["a"], "groups": None, "supplement": [],
                 "counts": [_c(1)], "tool_ms": [100.0], "sql_ms": [10.0], "exec_ms": [9.0],
                 "statements": []}
        p = tmp_path / name
        p.write_text(json.dumps({"path": path, "state": "head10", "shapes": {"h": shape},
                                 **_round(path, **round_kw)}))
        return str(p)

    def verdict(files, *extra):
        out = tmp_path / "v.json"
        rc = gate.main(["verdict", "--out", str(out), "--blocks", *files, *extra])
        return rc, json.loads(out.read_text())["verdict"]

    good = [write("t.json", "materialized"), write("g.json", "graph")]
    rc, v = verdict(good, "--require-states", "head10")
    assert rc == 0 and v["gate-rounds-valid"] and v["rejected_rounds"] == []
    rc, v = verdict(good, "--require-states", "head10,head1")
    assert rc == 1 and v["missing_states"] == ["head1"]
    owner = [write("t2.json", "materialized", role="krama_pilot"), write("g2.json", "graph")]
    rc, v = verdict(owner)
    assert rc == 1 and "not okg_mcp" in v["rejected_rounds"][0]
    graph_first = [write("t3.json", "materialized", t_end="2026-10-19T00:00:09Z"),
                   write("g3.json", "graph", t_end="2026-10-19T00:00:01Z")]
    rc, v = verdict(graph_first)
    assert rc == 1 and "do not alternate" in v["rejected_rounds"][0]


def test_aggregate_groups_check_served_groups_and_the_group_total() -> None:
    a, b, null = '{"attrs.k": "a"}', '{"attrs.k": "b"}', '{"attrs.k": null}'
    expected = {"groups": {a: 2, b: 1}}

    def check(groups, total=None, **extra):
        fields = {"groups": groups, **extra}
        if total is not None:
            fields.update(total_count=total, total_count_basis="exact")
        return gate.compare_rows("f", [_block("d1", [])], [_block("d1", [], **fields)],
                                 expected)["recompute_equal"]

    # A page of `limit` groups (the tool sets truncated, no truncation reason):
    # served groups right and the group total right passes.
    assert check({a: 2}, total=2) is True
    # A group missing from the whole set shows in the group total.
    assert check({a: 2}, total=1) is False
    # A served group with a wrong count, or an extra group.
    assert check({a: 3}, total=2) is False
    assert check({a: 2, b: 1, '{"attrs.k": "c"}': 1}, total=3) is False
    # Every group served: they must all match.
    assert check({a: 2, b: 1}, total=2) is True
    # A byte-budget cut keeps the per-group check; an old block without a
    # total keeps only the per-group check.
    assert check({a: 2}, total=2, budget_cut=True) is True
    assert check({a: 2}) is True
    # The tool may count a missing key and JSON null as two groups; both
    # sides fold them into one.
    folded = {"groups": {null: 3, a: 1}}
    assert gate.compare_rows("f", [_block("d1", [])], [_block(
        "d1", [], groups={null: 3}, total_count=3, total_count_basis="exact")],
        folded)["recompute_equal"] is True


def test_round_problems_name_mixed_pins_settings_and_code_trees() -> None:
    def blk(path, t, **extra):
        return {"path": path, "label": f"{path}{t}", "t_end": f"2026-10-19T00:00:0{t}Z",
                "role": ["okg_mcp", "auto", "2"], "pin": "p1",
                "server_file": f"/{'code' if path == 'graph' else 'devcode'}/server.py", **extra}

    good = [blk("materialized", 1), blk("graph", 2), blk("materialized", 3), blk("graph", 4)]
    assert gate.round_problems(good) == []
    assert "pins" in " ".join(gate.round_problems(good[:3] + [blk("graph", 4, pin="p0")]))
    assert "settings" in " ".join(gate.round_problems(
        good[:3] + [blk("graph", 4, role=["okg_mcp", "auto", "8"])]))
    assert "graph code trees" in " ".join(gate.round_problems(
        good[:3] + [blk("graph", 4, server_file="/old/server.py")]))
    assert "do not alternate" in " ".join(gate.round_problems(
        [blk("graph", 1), blk("materialized", 2)]))


# --- parity: the whole-graph overlay comparison (no database) ----------------


def _node(i: str, **attrs):
    return {"id": i, "subtype": "doc", "attrs": dict(attrs)}


def _graph_row(i: str, **attrs):
    # The page's row names its id node_id and carries more than is compared.
    return {"node_id": i, "subtype": "doc", "attrs": dict(attrs), "provenance": {"x": 1}}


def _edge(i: str, **attrs):
    return {"id": i, "edge_type": "references", "src": "a", "dst": "b", "attrs": dict(attrs)}


def test_parity_agreeing_streams_compare_every_row_with_no_mismatch() -> None:
    ref = [_node("a", owner="x"), _node("b", rank=1.0), _node("c")]
    graph = [_graph_row("a", owner="x"), _graph_row("b", rank=1), _graph_row("c")]
    mat = [_graph_row("a", owner="x"), _graph_row("b", rank=1), _graph_row("c")]
    out = gate.compare_streams("nodes", {"recompute": ref, "graph": graph, "materialized": mat},
                               "recompute")
    assert out["compared"] == 3 and out["mismatches"] == [] and out["stopped"] is None
    assert out["counts"] == {"recompute": 3, "graph": 3, "materialized": 3}
    assert out["complete"] is True  # every stream was read to its end


def test_parity_a_planted_differing_overlay_value_is_reported_with_its_id() -> None:
    ref = [_node("a", owner="x"), _node("b", owner="y"), _node("c", owner="z")]
    graph = [_graph_row("a", owner="x"), _graph_row("b", owner="PLANTED"),
             _graph_row("c", owner="z")]
    out = gate.compare_streams("nodes", {"recompute": ref, "graph": graph}, "recompute")
    assert out["compared"] == 3
    [bad] = out["mismatches"]
    assert (bad["id"], bad["side"], bad["problem"]) == ("b", "graph", "values_differ")
    assert bad["attrs"]["different"] == {"owner": ["y", "PLANTED"]}
    report = _parity_report([out])
    assert report["ok"] is False and report["mismatch_count"] == 1


def test_parity_a_value_on_one_side_only_and_a_wrong_type_field_are_mismatches() -> None:
    ref = [_node("a", k=1), _node("b")]
    graph = [_graph_row("a", k=1, extra=True), _graph_row("b")]
    graph[1]["subtype"] = "other"
    out = gate.compare_streams("nodes", {"recompute": ref, "graph": graph}, "recompute")
    by_id = {m["id"]: m for m in out["mismatches"]}
    assert by_id["a"]["attrs"]["only_side"] == ["extra"]
    assert by_id["b"]["fields"] == ["subtype"]
    # True is not 1: a JSON boolean never equals a number.
    out = gate.compare_streams("nodes", {"recompute": [_node("a", k=True)],
                                         "graph": [_graph_row("a", k=1)]}, "recompute")
    assert len(out["mismatches"]) == 1


def test_parity_a_missing_or_extra_row_ends_the_walk_and_fails() -> None:
    ref = [_node("a"), _node("b"), _node("c")]
    out = gate.compare_streams("nodes", {"recompute": ref, "graph": [_graph_row("a"),
                                                                    _graph_row("c")]},
                               "recompute")
    [bad] = out["mismatches"]
    assert bad["problem"] == "id_differs" and bad["reference_id"] == "b" and bad["side_id"] == "c"
    assert out["stopped"] == "ids_differ" and out["compared"] == 1
    # A stream that ends early, and one that runs past the reference.
    short = gate.compare_streams("nodes", {"recompute": ref, "graph": [_graph_row("a")]},
                                 "recompute")
    assert short["mismatches"][0]["side_id"] is None
    long = gate.compare_streams("nodes", {"recompute": ref[:1],
                                          "graph": [_graph_row("a"), _graph_row("b")]},
                                "recompute")
    assert long["mismatches"][0]["reference_id"] is None
    assert _parity_report([out])["ok"] is False


def test_parity_a_materialized_side_that_differs_fails_even_when_graph_agrees() -> None:
    ref = [_edge("1", w=1), _edge("2", w=2)]
    graph = [dict(r, edge_id=r["id"]) for r in ref]
    mat = [dict(ref[0], edge_id="1"), dict(ref[1], edge_id="2", attrs={"w": 3})]
    out = gate.compare_streams("edges", {"recompute": ref, "graph": graph, "materialized": mat},
                               "recompute")
    [bad] = out["mismatches"]
    assert (bad["side"], bad["id"]) == ("materialized", "2")
    assert bad["attrs"]["different"] == {"w": [2, 3]}


def test_parity_limit_and_max_mismatches_stop_the_walk() -> None:
    ref = [_node(str(i)) for i in range(10)]
    out = gate.compare_streams("nodes", {"recompute": iter(ref), "graph": iter(
        [_graph_row(str(i)) for i in range(10)])}, "recompute", limit=4)
    assert out["compared"] == 4 and out["counts"]["graph"] == 4 and out["stopped"] is None
    assert out["complete"] is False  # cut by the limit: the rest was never read
    bad = [_graph_row(str(i), x=1) for i in range(10)]
    out = gate.compare_streams("nodes", {"recompute": ref, "graph": bad}, "recompute",
                               max_mismatches=3)
    assert len(out["mismatches"]) == 3 and out["stopped"] == "max_mismatches"


def _parity_report(kinds, **over):
    args = dict(pin="g1", generation_id="uuid", branch_point=7,
                overlay_targets={"node": 2, "edge": 1}, kinds=kinds,
                materialized={"status": "compared"},
                fallback_notes={"graph": [], "materialized": []},
                role=["okg_mcp", "auto", "2"], code={"server_file": "/s/server.py",
                                                     "git_head": "abc", "git_dirty": False},
                settings={"plan_cache_mode": "auto"}, limit=None, batch=1000,
                generations={"recompute": "gen-1", "graph": "gen-1", "materialized": "gen-1"})
    args.update(over)
    return gate.build_parity_report(**args)


def test_parity_report_names_pin_code_settings_counts_and_passes_when_clean() -> None:
    ok = gate.compare_streams("nodes", {"recompute": [_node("a")], "graph": [_graph_row("a")]},
                              "recompute")
    ok_edges = gate.compare_streams("edges", {"recompute": [_edge("1")],
                                              "graph": [dict(_edge("1"), edge_id="1")]},
                                    "recompute")
    report = _parity_report([ok, ok_edges])
    assert report["ok"] is True and report["mismatch_count"] == 0
    assert report["compared"] == {"nodes": 1, "edges": 1}
    assert report["pin"] == "g1" and report["code"]["git_head"] == "abc"
    assert report["role"][0] == "okg_mcp" and report["overlay_targets"] == {"node": 2, "edge": 1}
    assert report["settings"] == {"plan_cache_mode": "auto"}
    json.dumps(report)  # the report is plain JSON


def test_parity_report_fails_on_a_fallback_note_an_early_stop_or_nothing_compared() -> None:
    ok = gate.compare_streams("nodes", {"recompute": [_node("a")], "graph": [_graph_row("a")]},
                              "recompute")
    note = {"read_path": "materialized", "reason": "shape_not_served"}
    assert _parity_report([ok], fallback_notes={"graph": [note], "materialized": []})["ok"] is False
    empty = gate.compare_streams("nodes", {"recompute": [], "graph": []}, "recompute")
    assert empty["compared"] == 0 and _parity_report([empty])["ok"] is False
    assert _parity_report([dict(ok, stopped="max_mismatches")])["ok"] is False


def test_parity_attrs_diff_shortens_a_long_value() -> None:
    diff = gate.attrs_diff({"t": "x" * 500}, {"t": "y"})
    assert len(json.dumps(diff["different"]["t"][0])) < 260


def test_parity_a_partial_walk_is_incomplete_and_not_ok() -> None:
    """--limit stops the walk early. The report says ``complete: false`` and
    ``ok`` is false, even with zero mismatches. Mutation: dropping ``complete``
    from the ``ok`` rule (or setting it true when the limit is hit) turns the
    first two asserts red; making a full walk report incomplete turns the
    last one red."""
    ref = [_node(str(i)) for i in range(10)]
    graph = [_graph_row(str(i)) for i in range(10)]
    cut = gate.compare_streams("nodes", {"recompute": iter(ref), "graph": iter(graph)},
                               "recompute", limit=4)
    assert cut["mismatches"] == [] and cut["compared"] == 4 and cut["complete"] is False
    report = _parity_report([cut], limit=4)
    assert report["complete"] is False and report["ok"] is False
    assert "incomplete_walk" in report["failure_reasons"]
    # A limit larger than the stream reads it to the end: complete and ok.
    whole = gate.compare_streams("nodes", {"recompute": iter(ref), "graph": iter(graph)},
                                 "recompute", limit=50)
    assert whole["complete"] is True
    assert _parity_report([whole], limit=50)["ok"] is True
    # A limit that equals the stream length is still reported incomplete: the
    # walk cannot tell that nothing follows.
    exact = gate.compare_streams("nodes", {"recompute": iter(ref), "graph": iter(graph)},
                                 "recompute", limit=10)
    assert exact["complete"] is False


def test_parity_connections_that_resolved_different_generations_fail() -> None:
    """Each connection resolves the pin itself. Mutation: dropping the
    generations check from the ``ok`` rule turns the second assert red; the
    clean case turns red if equal ids were flagged."""
    ok = gate.compare_streams("nodes", {"recompute": [_node("a")], "graph": [_graph_row("a")]},
                              "recompute")
    clean = _parity_report([ok])
    assert clean["ok"] is True and clean["generations"]["graph"] == "gen-1"
    split = _parity_report([ok], generations={"recompute": "gen-1", "graph": "gen-2",
                                              "materialized": "gen-1"})
    assert split["ok"] is False and split["failure_reasons"] == ["generations_differ"]
    assert split["generations"]["graph"] == "gen-2"  # each side's id is recorded


class _FakeServer:
    """A page reader over a sorted id list that honours ``after_id`` and
    ``limit`` like the real page, and records each call."""

    def __init__(self, ids: list[str], *, lie_more: bool = False) -> None:
        self.ids = ids
        self.calls: list[tuple[str | None, int]] = []
        self.lie_more = lie_more

    def _okg_effective_view_sql_page(self, conn, session, *, kind, predicate, limit, offset,
                                     after_id=None, page_facts=None):
        assert offset == 0 and predicate is None
        self.calls.append((after_id, limit))
        rest = [i for i in self.ids if after_id is None or i > after_id]
        page = rest[:limit]
        return [{"node_id": i} for i in page], (len(rest) > limit) or self.lie_more


def _walk(server, monkeypatch, *, batch, limit=None, kind="nodes") -> list[str]:
    notes: list = []
    return [r["node_id"] for r in gate._graph_rows(
        server, object(), object(), kind, "graph", batch, limit, notes)]


def test_graph_rows_keyset_loop_crosses_batch_boundaries_without_gaps_or_repeats(
    monkeypatch,
) -> None:
    """7 ids in batches of 3: three pages (3, 3, then the partial 1). Each page
    starts after the last id of the one before. Mutation: continuing from the
    first id of a page (``rows[0]``) repeats rows; from a fixed ``None``
    repeats the first page; skipping ``after`` by one drops an id; returning
    after a full page drops the tail; each turns the id list or the calls red."""
    ids = [f"n{i}" for i in range(7)]
    server = _FakeServer(ids)
    assert _walk(server, monkeypatch, batch=3) == ids
    assert server.calls == [(None, 3), ("n2", 3), ("n5", 3)]
    # The last page is exactly full and nothing follows: no extra empty page.
    even = _FakeServer(ids[:6])
    assert _walk(even, monkeypatch, batch=3) == ids[:6]
    assert even.calls == [(None, 3), ("n2", 3)]
    # A single partial batch, and an empty graph.
    assert _walk(_FakeServer(ids[:2]), monkeypatch, batch=3) == ids[:2]
    assert _walk(_FakeServer([]), monkeypatch, batch=3) == []


def test_graph_rows_stops_on_an_empty_page_and_honours_the_limit(monkeypatch) -> None:
    """``more`` claiming more after an empty page must not loop forever; under
    a limit the last request asks only for what is left. Mutation: dropping
    the ``not rows`` guard raises IndexError on the empty page; using
    ``batch`` instead of ``limit - seen`` returns more than the limit."""
    ids = [f"n{i}" for i in range(6)]
    liar = _FakeServer(ids, lie_more=True)
    assert _walk(liar, monkeypatch, batch=3) == ids
    assert len(liar.calls) == 3  # two full pages, then the empty one ends it
    capped = _FakeServer(ids)
    assert _walk(capped, monkeypatch, batch=3, limit=4) == ids[:4]
    assert capped.calls == [(None, 3), ("n2", 1)]


def test_graph_rows_refuses_any_read_path_but_the_graph() -> None:
    """PACT lane-s-storage-cutover: the graph path is the only read path, so
    asking the walk for the materialized one is refused before any page is
    read, not silently answered by the graph path."""
    server = _FakeServer(["n0"])
    with pytest.raises(RuntimeError, match="only the graph read path"):
        list(gate._graph_rows(server, object(), object(), "nodes", "materialized", 3, None, []))
    assert server.calls == []


def test_gate_has_no_read_path_switch_or_materialized_flag() -> None:
    """The gate harness keeps no switch helper and no ``--skip-materialized``
    flag: the parity subcommand compares the graph path with the recompute."""
    for name in ("_SWITCH_ENV", "_read_path_switch", "_has_graph_read", "_materialized_refusal"):
        assert not hasattr(gate, name), name
    source = Path(gate.__file__).read_text(encoding="utf-8")
    assert "OKG_EFFECTIVE_READ_PATH" not in source
    assert "--skip-materialized" not in source
