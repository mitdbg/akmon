"""`aggregate` ranks its groups with `order_by` (PACT read-aggregate-order-by).

Hermetic: the argument check through a server with no database (every
refusal is decided before anything is opened), the ranking rule, `run`
steps over held rows, and the reply text. The PostgreSQL tests (a real
publish, and live typed sets through `call_tool` with the recording fake
system) are in ``test_aggregate_order_by_db.py``.

Tests that must fail on the base say so: there `aggregate` declares no
`order_by` (an unknown argument), a `run` step over rows does not take it,
and the live resolver reads `order_by="count desc"` as an upstream key.
The rule's own module is imported inside each test, so the base fails each
test for its own reason, not at collection.
"""
from __future__ import annotations

import json
import math
import time
from typing import Any

import anyio
import pytest

from okg.substrate.mcp import live_result_sets, run_live, run_steps

# Assembled from pieces so the public-copy export does not read it as a
# credential; nothing listens on port 1.
DEAD_DSN = "postgresql://postgres" + ":okg" + "@localhost:1/okg_probe"
SET = {"input": {"kind": "nodes", "subtype": "t"}, "by": ["attrs.kind"], "metrics": ["count"]}
KEPT = {"input": {"ref": "l1.notakeptset"}, "by": ["attrs.site"]}


@pytest.fixture
def mcp_server(monkeypatch: pytest.MonkeyPatch):
    from okg.substrate.mcp.server import build_server

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_RO_DSN", DEAD_DSN)
    server = build_server(dsn=DEAD_DSN)
    try:
        yield server
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()


def _reply(server: Any, arguments: dict[str, Any]) -> tuple[bool, dict[str, Any], str]:
    from tests.substrate.mcp._result_payload import call_json, payload

    async def run() -> tuple[Any, Any]:
        return (
            await call_json(server, "aggregate", arguments),
            await server.call_tool("aggregate", arguments),
        )

    as_json, as_text = anyio.run(run)
    return bool(as_json.isError), payload(as_json), as_text.content[0].text


def _refused(server: Any, arguments: dict[str, Any]) -> tuple[dict[str, Any], str]:
    failed, reply, text = _reply(server, arguments)
    assert failed and reply["status"] == "refused", reply
    return reply["error"], text


def _order_refusal(server: Any, arguments: dict[str, Any], reason: str) -> tuple[dict, str]:
    error, text = _refused(server, arguments)
    # On the base: "aggregate does not accept 'order_by'" (an unknown argument).
    assert "unknown_arguments" not in (error.get("data") or {}), error
    assert error["code"] == "invalid_operator_arguments", error
    assert error["data"]["reason"] == reason, error
    return error, text


def _held(rows: list[dict[str, Any]], source: str = "external_live:fx") -> run_live.HeldRows:
    return run_live.HeldRows(rows=rows, sources=[source] * len(rows), head="", cover="fx")


def _rows_step(arguments: dict[str, Any]) -> run_steps.Step:
    return run_steps.parse_steps([{"values": {"x": [1]}}, {"aggregate": {"input": "$1", **arguments}}])[1]


def _unfence(value: Any) -> Any:
    if isinstance(value, str) and "\n" in value:
        return value.split("\n")[1]
    return value


# ============================================================ the argument


def test_order_by_spellings_name_a_metric_or_a_by_field(mcp_server) -> None:
    """FAILS ON THE BASE: order_by is an unknown argument of aggregate."""
    failed, reply, _text = _reply(mcp_server, {**SET, "order_by": "count desc"})
    data = (reply.get("error") or {}).get("data") or {}
    assert "order_by" not in data.get("unknown_arguments", []), reply
    assert data.get("reason") not in ("order_by_unknown", "order_by_spelling"), reply

    from okg.substrate.mcp.aggregate_order import check_order_by

    def check(text: str, **kw: Any) -> tuple[str, str, str]:
        order = check_order_by(text, **{"by": ["attrs.kind"], "metrics": ["count"], **kw})
        assert order is not None
        return order.name, order.direction, order.kind

    assert check("count") == ("count", "asc", "metric")
    for desc in ("count desc", "count DESC", "count Desc", "-count", "- count"):
        assert check(desc) == ("count", "desc", "metric"), desc
    assert check("count asc") == check("count ASC") == ("count", "asc", "metric")
    assert check("attrs.kind desc") == ("attrs.kind", "desc", "group")
    # A by field holding " desc" matches whole first, then with a direction.
    assert check("attrs.my desc", by=["attrs.my desc"]) == ("attrs.my desc", "asc", "group")
    assert check("attrs.my desc desc", by=["attrs.my desc"]) == (
        "attrs.my desc", "desc", "group",
    )
    assert check("occurrences desc", by=["match"], match=True) == (
        "occurrences", "desc", "metric",
    )
    assert check("match", by=["match"], match=True) == ("match", "asc", "group")
    assert check("month(date.created) desc", by=["month(date.created)"]) == (
        "month(date.created)", "desc", "group",
    )
    labels = ["count", "max(attrs.static_total)"]
    assert check("max(attrs.static_total) desc", by=["node_id"], metrics=labels, live=True) == (
        "max(attrs.static_total)", "desc", "metric",
    )
    # No name is built: a later printed label (part 3's calc.<name>) works as is.
    assert check("calc.share desc", by=["calc.share"]) == ("calc.share", "desc", "group")
    assert check_order_by(None, by=["attrs.kind"], metrics=["count"]) is None


def test_an_unknown_order_by_is_refused_naming_what_it_takes(mcp_server) -> None:
    """FAILS ON THE BASE: order_by is an unknown argument of aggregate."""
    error, text = _order_refusal(
        mcp_server, {**SET, "order_by": "count down"}, "order_by_unknown",
    )
    assert error["message"] == (
        'aggregate: order_by "count down" is not a metric or a by field of this '
        "call; it takes one of: count, attrs.kind (closest: count)"
    )
    assert error["data"]["takes"] == ["count", "attrs.kind"]
    assert error["data"]["closest"] == "count"
    assert "order_by_unknown" in text or "is not a metric or a by field" in text
    error, _ = _order_refusal(mcp_server, {**SET, "order_by": "attrs.knd desc"}, "order_by_unknown")
    assert error["data"]["closest"] == "attrs.kind"
    error, _ = _order_refusal(mcp_server, {**SET, "order_by": "sum(attrs.x) desc"}, "order_by_unknown")
    assert error["message"].endswith("; a stored set computes count only")


def test_order_by_count_missing_from_metrics_is_refused_with_a_corrected_call(mcp_server) -> None:
    """FAILS ON THE BASE: order_by is an unknown argument of aggregate. The
    refusal comes before the kept set is opened (there is none here)."""
    arguments = {**KEPT, "metrics": ["max(attrs.size)"], "order_by": "count desc"}
    error, _text = _order_refusal(mcp_server, arguments, "order_by_not_in_metrics")
    assert error["message"] == 'aggregate: order_by "count" is not in metrics'
    assert error["retry_call"]["mode"] == "corrected"
    assert error["retry_call"]["arguments"]["metrics"] == ["max(attrs.size)", "count"]
    assert error["retry_call"]["arguments"]["order_by"] == "count desc"
    error, _text = _order_refusal(
        mcp_server, {**KEPT, "metrics": ["count"], "order_by": "-sum(attrs.size)"},
        "order_by_not_in_metrics",
    )
    assert error["retry_call"]["arguments"]["metrics"] == ["count", "sum(attrs.size)"]
    # With four metrics already, no corrected call drops one (review m4).
    four = ["count", "sum(attrs.a)", "min(attrs.a)", "max(attrs.a)"]
    error, _text = _order_refusal(
        mcp_server, {**KEPT, "metrics": four, "order_by": "-sum(attrs.size)"},
        "order_by_not_in_metrics",
    )
    assert "metrics takes at most 4; replace one with sum(attrs.size)" in error["message"]
    assert "retry_call" not in error


def test_a_doubled_direction_is_refused(mcp_server) -> None:
    """FAILS ON THE BASE: order_by is an unknown argument of aggregate."""
    error, _text = _order_refusal(mcp_server, {**SET, "order_by": "-count desc"}, "order_by_spelling")
    assert error["message"] == (
        'aggregate: order_by is "<name>", "<name> asc", "<name> desc" or "-<name>"'
    )
    _order_refusal(mcp_server, {**SET, "order_by": "  "}, "order_by_spelling")


def test_a_kept_set_order_by_typo_is_refused_before_the_set_is_opened(
    mcp_server, monkeypatch,
) -> None:
    """N2: the kept-ref branch runs the shared check before `_live_open`."""
    from okg.substrate.mcp import live_result_sets as lrs

    opened: list[Any] = []
    real = lrs.live_store

    def spy(*args: Any, **kwargs: Any) -> Any:
        opened.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(lrs, "live_store", spy)
    _order_refusal(
        mcp_server, {**KEPT, "metrics": ["count"], "order_by": "cnt desc"}, "order_by_unknown",
    )
    assert opened == []


def test_order_by_on_the_record_anchor_and_edge_summary_forms_is_refused(mcp_server) -> None:
    error, _ = _order_refusal(mcp_server, {
        "input": "n:anchor", "by": ["edge_type", "dst_subtype"], "metrics": ["count"],
        "order_by": "count desc",
    }, "order_by_needs_set")
    assert error["message"] == "aggregate: order_by ranks the groups of a set"
    _order_refusal(mcp_server, {
        "input": {"kind": "edges"}, "by": ["src_subtype", "edge_type", "dst_subtype"],
        "metrics": ["count"], "order_by": "count desc",
    }, "order_by_needs_set")


def test_sort_by_sort_and_order_are_refused_naming_order_by_and_order_by_in_input_is_lifted(
    mcp_server,
) -> None:
    """FAILS ON THE BASE: the refusal names no order_by, and order_by inside
    input stays inside it."""
    for spelling in ("sort_by", "sort", "order"):
        error, text = _refused(mcp_server, {**SET, spelling: "count desc"})
        assert error["data"]["unknown_arguments"] == [spelling]
        assert error["data"]["instead"][spelling] == 'use order_by, e.g. order_by="count desc"'
        assert "use order_by" in text
    from okg.substrate.mcp.read_grammar import rewrite_call

    rewritten = rewrite_call("aggregate", {
        "input": {"kind": "nodes", "subtype": "t", "order_by": "count desc"},
        "by": ["attrs.kind"], "metrics": ["count"],
    })
    assert rewritten.arguments["order_by"] == "count desc"
    assert "order_by" not in rewritten.arguments["input"]


def test_named_keys_drops_aggregate_order_by() -> None:
    """FAILS ON THE BASE (m10): `named_keys` read `count desc` as an
    upstream key, so a whole listing refused it as a typo."""
    from okg.substrate.mcp.live_resolver import named_keys

    assert named_keys("aggregate", {"order_by": "count desc"}) == []
    assert named_keys("aggregate", {
        "by": ["attrs.site"], "metrics": ["max(attrs.size)"], "order_by": "-attrs.site",
    }) == [("by", "site"), ("metrics", "size")]
    # filter's own row order still reads its order_by (unchanged here).
    assert [a for a, _k in named_keys("filter", {"order_by": "attrs.size"})] == ["order_by"]


# ============================================================ the rule


def _order(name: str, desc: bool, kind: str = "metric") -> Any:
    from okg.substrate.mcp.aggregate_order import OrderBy

    return OrderBy(name, desc, kind)


def _live_rank(groups: list[tuple[tuple[Any, ...], dict[str, Any]]], name: str, desc: bool,
               fields: list[str], kind: str = "metric") -> Any:
    from okg.substrate.mcp import aggregate_order as ao

    order = _order(name, desc, kind)
    return ao.rank(groups, order, value=lambda g: ao.live_value(g, order, fields),
                   key=lambda g: g[0])


def test_ties_break_by_group_key_with_null_and_absent_last_in_both_directions() -> None:
    from okg.substrate.mcp.aggregate_order import ABSENT_VALUE

    groups = [
        ((ABSENT_VALUE,), {"count": 2}), (("b",), {"count": 2}), ((None,), {"count": 2}),
        (("a",), {"count": 2}), ((7,), {"count": 2}), (("c",), {"count": 1}),
    ]
    for desc in (False, True):
        ranked = _live_rank(groups, "count", desc, ["attrs.k"])
        keys = [g[0][0] for g in ranked.groups]
        tied = [7, "a", "b", None, ABSENT_VALUE]
        assert keys == (tied + ["c"] if desc else ["c"] + tied), (desc, keys)


def test_no_value_null_and_absent_rank_last_in_both_directions() -> None:
    from okg.substrate.mcp import aggregate_order as ao

    groups = [
        (("x",), {"max(attrs.s)": None}), (("y",), {"max(attrs.s)": 3}),
        (("z",), {"max(attrs.s)": 9}), (("w",), {"max(attrs.s)": None}),
    ]
    for desc, first in ((True, ["z", "y"]), (False, ["y", "z"])):
        ranked = _live_rank(groups, "max(attrs.s)", desc, ["attrs.k"])
        assert [g[0][0] for g in ranked.groups] == [*first, "w", "x"]
        assert ranked.no_value == 2
        facts = ao.page_facts(ranked, _order("max(attrs.s)", desc), offset=0, page=10)
        assert facts["ranked_last_no_value"] == 2
    # Ranked by a key: a JSON null before a missing key, both last.
    keyed = [((ao.ABSENT_VALUE,), {"count": 1}), ((None,), {"count": 1}), ((1,), {"count": 1})]
    for desc in (False, True):
        ranked = _live_rank(keyed, "attrs.k", desc, ["attrs.k"], kind="group")
        assert [g[0][0] for g in ranked.groups] == [1, None, ao.ABSENT_VALUE]


def test_mixed_kinds_large_integers_nan_and_one_against_one_point_zero_order_totally() -> None:
    big = 10 ** 30
    values = [3, "b", True, big, float("nan"), {"a": 1}, 1.0, 1, "a", False,
              float("inf"), big + 1, 2.5]
    groups = [((i,), {"max(attrs.v)": v}) for i, v in enumerate(values)]
    ranked = _live_rank(groups, "max(attrs.v)", False, ["attrs.i"])
    got = [g[1]["max(attrs.v)"] for g in ranked.groups]
    # 1 and 1.0 are equal numbers: their keys (positions 7 and 6) decide.
    assert got[:2] == [1.0, 1] and [type(v) for v in got[:2]] == [float, int]
    assert got[2:6] == [2.5, 3, big, big + 1]
    assert got[6:8] == ["a", "b"] and got[8:10] == [False, True]
    assert [json.dumps(v) for v in got[10:]] == ["Infinity", "NaN", '{"a": 1}']
    desc = _live_rank(groups, "max(attrs.v)", True, ["attrs.i"])
    got_desc = [g[1]["max(attrs.v)"] for g in desc.groups]
    # Largest first keeps the kinds in order (numbers first, review m1); only
    # the values within a kind reverse. Ties still go by the key, ascending.
    assert got_desc[:6] == [big + 1, big, 3, 2.5, 1.0, 1]
    assert got_desc[6:10] == ["b", "a", True, False]
    assert [json.dumps(v) for v in got_desc[10:]] == ['{"a": 1}', "NaN", "Infinity"]
    # The order is total: the same groups in any input order rank the same.
    again = _live_rank(list(reversed(groups)), "max(attrs.v)", False, ["attrs.i"])
    assert [g[0] for g in again.groups] == [g[0] for g in ranked.groups]
    assert math.isnan(got[11])


def test_a_sum_with_a_float_part_ranks_by_its_served_whole() -> None:
    label = "sum(attrs.s)"
    floats = live_result_sets.float_part_label(label)
    groups = [
        (("a",), {label: 10}), (("b",), {label: 10, floats: 0.5}),
        (("c",), {label: 10, floats: 0.25}), (("d",), {label: 9, floats: 1.75}),
        (("e",), {label: 2 ** 70}),
    ]
    ranked = _live_rank(groups, label, True, ["attrs.k"])
    assert [g[0][0] for g in ranked.groups] == ["e", "d", "b", "c", "a"]


def test_rank_is_a_unique_position_across_pages_and_a_tie_at_the_page_end_is_named() -> None:
    """FAILS ON THE BASE: a run step over rows does not take order_by."""
    rows = [{"site": s, "size": n} for s, n in
            (("A", 5), ("B", 9), ("C", 9), ("D", 9), ("E", 1), ("F", 2))]
    step = _rows_step({"by": ["site"], "metrics": ["max(size)"],
                       "order_by": "max(size) desc", "limit": 2})
    out = run_live.rows_step(step, 1, _held(rows), lambda row, predicate: True)
    ranks = [(g["rank"], _unfence(g["attrs.site"])) for g in out.groups]
    assert ranks == [(1, "B"), (2, "C"), (3, "D"), (4, "A"), (5, "F"), (6, "E")]
    assert out.facts["tie_at_page_end"] == {
        "position": 1, "value": 9, "groups_tied": 3, "shown": 2,
    }
    assert "! positions 1-3 hold the same max(attrs.size); 2 shown; ties are ordered by attrs.site" in out.notes

    from okg.substrate.mcp import aggregate_order as ao

    groups = [((s,), {"count": c}) for s, c in
              (("a", 3), ("b", 2), ("c", 2), ("d", 2), ("e", 1))]
    order = _order("count", True)
    ranked = ao.rank(groups, order, value=lambda g: ao.live_value(g, order, ["attrs.k"]),
                     key=lambda g: g[0])
    pages = [ranked.groups[o: o + 2] for o in (0, 2, 4)]
    assert [g[0][0] for page in pages for g in page] == ["a", "b", "c", "d", "e"]
    facts = ao.page_facts(ranked, order, offset=0, page=2,
                          value=lambda g: ao.live_value(g, order, ["attrs.k"]))
    assert facts["tie_at_page_end"] == {"position": 2, "value": 2, "groups_tied": 3, "shown": 1}
    facts = ao.page_facts(ranked, order, offset=2, page=2,
                          value=lambda g: ao.live_value(g, order, ["attrs.k"]))
    assert "tie_at_page_end" not in facts
    facts = ao.page_facts(ranked, order, offset=2, page=1,
                          value=lambda g: ao.live_value(g, order, ["attrs.k"]))
    assert facts["tie_at_page_end"] == {"position": 2, "value": 2, "groups_tied": 3, "shown": 1}


def test_stored_and_live_paths_rank_the_same_groups_identically() -> None:
    """The stored form (key, count, missing flags) and the live form (key
    with ABSENT_VALUE, computed) of one set of groups rank identically."""
    from okg.substrate.mcp import aggregate_order as ao

    stored = [
        (("x", 1), 4, (False, False)), ((None, 1), 4, (True, False)),
        ((None, 1), 4, (False, False)), (("y", None), 2, (False, False)),
        ((1.0, 2), 4, (False, False)), ((1, 2), 4, (False, False)),
    ]
    live = [(ao.stored_key(item, 2), {"count": item[1]}) for item in stored]
    for name, kind in (("count", "metric"), ("attrs.a", "group"), ("attrs.b", "group")):
        for desc in (False, True):
            order = _order(name, desc, kind)
            by = ["attrs.a", "attrs.b"]

            def stored_value(item: Any) -> Any:
                if kind == "metric":
                    return item[1]
                return ao.stored_key(item, 2)[by.index(name)]

            a = ao.rank(stored, order, value=stored_value, key=lambda i: ao.stored_key(i, 2))
            b = ao.rank(live, order, value=lambda g: ao.live_value(g, order, by),
                        key=lambda g: g[0])
            assert [ao.stored_key(i, 2) for i in a.groups] == [g[0] for g in b.groups]
            assert a.values == b.values and a.no_value == b.no_value


def test_match_ranking_compares_raw_text_not_fenced_text() -> None:
    from okg.substrate.mcp import aggregate_order as ao
    from okg.substrate.mcp.text_match import MatchGroup

    def group(raw: str, fenced: bool, records: int = 1) -> MatchGroup:
        text = f"⟦UNTRUSTED_DATA source=s⟧\n{raw}\n⟦/UNTRUSTED_DATA⟧" if fenced else raw
        return MatchGroup(text=text, records=records, occurrences=records, variants="",
                          raw=raw, fenced=fenced)

    groups = [group("b", False), group("a", True), group("c", True), group("Z", False)]
    order = _order("match", False, "group")
    ranked = ao.rank(groups, order, value=lambda g: ao.match_value(g, order), key=ao.match_key)
    assert [g.raw for g in ranked.groups] == ["Z", "a", "b", "c"]
    # By count, equal counts break on the raw text too.
    order = _order("count", True)
    ranked = ao.rank(groups, order, value=lambda g: ao.match_value(g, order), key=ao.match_key)
    assert [g.raw for g in ranked.groups] == ["Z", "a", "b", "c"]


def test_a_tie_value_that_is_a_live_key_is_served_fenced() -> None:
    from okg.substrate.mcp import aggregate_order as ao

    groups = [(("T1", "x"), {"count": 1}), (("T1", "y"), {"count": 1}),
              (("T0", "z"), {"count": 1})]
    order = _order("attrs.tier", True, "group")
    fields = ["attrs.tier", "attrs.site"]
    ranked = ao.rank(groups, order, value=lambda g: ao.live_value(g, order, fields),
                     key=lambda g: g[0])
    facts = ao.page_facts(
        ranked, order, offset=0, page=1,
        value=lambda g: ao.live_value(g, order, fields),
        tie_value=lambda v: live_result_sets.fence_rows(v, "fx_list"),
    )
    tie = facts["tie_at_page_end"]
    assert tie["position"] == 1 and tie["groups_tied"] == 2 and tie["shown"] == 1
    assert tie["value"] != "T1" and "UNTRUSTED" in tie["value"] and "T1" in tie["value"]
    # A metric's tie value is a plain number.
    order = _order("count", True)
    facts = ao.page_facts(ao.rank(groups, order, value=lambda g: ao.live_value(g, order, fields),
                                  key=lambda g: g[0]),
                          order, offset=0, page=1, value=lambda g: ao.live_value(g, order, fields),
                          tie_value=lambda v: live_result_sets.fence_rows(v, "fx_list"))
    assert facts["tie_at_page_end"]["value"] == 1


def test_ranking_fifty_thousand_groups_sorts_once_within_a_loose_bound() -> None:
    from okg.substrate.mcp import aggregate_order as ao

    groups = [((f"s{i:05d}",), {"max(attrs.v)": (i * 7919) % 50_000}) for i in range(50_000)]
    order = _order("max(attrs.v)", True)
    calls = 0

    def value(g: Any) -> Any:
        nonlocal calls
        calls += 1
        return ao.live_value(g, order, ["attrs.s"])

    started = time.perf_counter()
    ranked = ao.rank(groups, order, value=value, key=lambda g: g[0])
    elapsed = time.perf_counter() - started
    print(f"rank 50000 groups: {elapsed:.3f} s")
    assert calls == 50_000  # each group's value read once (keys computed once)
    assert len(ranked) == 50_000 and ranked.groups[0][1]["max(attrs.v)"] == 49_999
    assert elapsed < 5.0


# ============================================================ run steps over rows


def test_run_rows_step_ranks_then_limits_and_holds_rank_as_a_column() -> None:
    """FAILS ON THE BASE: a run step over rows does not take order_by."""
    rows = [{"node_id": f"site:S{i}", "static_total": t}
            for i, t in enumerate([40, 10, 90, 70, 20])]
    step = _rows_step({"by": ["node_id"], "metrics": ["max(static_total)"],
                       "order_by": "max(static_total) desc", "limit": 2})
    out = run_live.rows_step(step, 1, _held(rows), lambda row, predicate: True)
    # Every group is ranked and held: the limit cuts only what is printed.
    assert len(out.groups) == 5
    assert [(g["rank"], g["max(attrs.static_total)"]) for g in out.groups] == [
        (1, 90), (2, 70), (3, 40), (4, 20), (5, 10),
    ]
    assert list(out.groups[0])[0] == "rank"
    assert out.facts["order_by"] == {"name": "max(attrs.static_total)", "direction": "desc",
                                     "kind": "metric"}
    assert out.facts["ranking_complete"] is True
    assert "! ordered by max(attrs.static_total), largest first · all 5 groups ranked" in out.notes
    # A later step filters the held groups on rank (N3: op lt).
    later = run_steps.parse_steps([
        {"values": {"x": [1]}},
        {"aggregate": {"input": "$1", "by": ["node_id"], "metrics": ["max(static_total)"],
                       "order_by": "max(static_total) desc"}},
        {"filter": {"input": "$2", "where": {"rank": {"op": "lt", "value": 3}}}},
    ])[2]
    seen: list[Any] = []

    def lt(row: dict[str, Any], predicate: Any) -> bool:
        seen.append(predicate)
        return row["rank"] < 3

    kept = run_live.rows_step(later, 2, out, lt)
    assert [_unfence(r["attrs.node_id"]) for r in kept.rows] == ["site:S2", "site:S3"]
    assert "attrs.rank" in json.dumps(seen[0]) and '"lt"' in json.dumps(seen[0])


def test_run_rows_step_normalises_order_by_and_refuses_a_column_named_count() -> None:
    """FAILS ON THE BASE: a run step over rows does not take order_by."""
    rows = [{"k": "a", "size": 2}, {"k": "b", "size": 7}, {"k": "b", "size": 1}]
    for order_by, name in (("max(size) desc", "max(attrs.size)"), ("-max(attrs.size)", "max(attrs.size)"),
                           ("count", "count"), ("k desc", "attrs.k")):
        step = _rows_step({"by": ["k"], "metrics": ["count", "max(size)"], "order_by": order_by})
        out = run_live.rows_step(step, 1, _held(rows), lambda row, predicate: True)
        assert out.facts["order_by"]["name"] == name, order_by
    step = _rows_step({"by": ["count"], "metrics": ["count"], "order_by": "count desc"})
    with pytest.raises(run_steps.RunRefusal) as caught:
        run_live.rows_step(step, 1, _held([{"count": 1}]), lambda row, predicate: True)
    assert "write it as attrs.count" in caught.value.message
    step = _rows_step({"by": ["k"], "metrics": ["count"], "order_by": "max(size) desc"})
    with pytest.raises(run_steps.RunRefusal) as caught:
        run_live.rows_step(step, 1, _held(rows), lambda row, predicate: True)
    assert 'order_by "max(attrs.size)" is not in metrics' in caught.value.message


def test_a_run_rows_step_order_by_typo_is_refused_before_any_step_runs() -> None:
    """Review m3: classify_steps checks a rows step's order_by from its own
    by and metrics, so nothing upstream is called for a run that cannot run."""
    live = {"expand": {"input": {"node_id": "n:hub"}, "live": {
        "relation": "fx_list", "args": {"pattern": "*"}}}}
    steps = run_steps.parse_steps([
        live,
        {"aggregate": {"input": "$1", "by": ["site"], "metrics": ["max(size)"],
                       "order_by": "max(sise) desc"}},
    ])
    run_steps.check_references(steps)
    with pytest.raises(run_steps.RunRefusal) as caught:
        run_steps.classify_steps(steps)
    assert 'order_by "max(attrs.sise)" is not a metric or a by field' in caught.value.message
    steps = run_steps.parse_steps([
        live,
        {"aggregate": {"input": "$1", "by": ["site"], "metrics": ["max(size)"],
                       "order_by": "max(size) desc"}},
    ])
    run_steps.check_references(steps)
    run_steps.classify_steps(steps)
    assert steps[1].kind == "rows"


def test_run_rows_step_over_a_partial_step_says_the_ranking_covers_the_rows_read() -> None:
    held = _held([{"k": "a"}, {"k": "b"}, {"k": "b"}])
    held.partial = True
    step = _rows_step({"by": ["k"], "metrics": ["count"], "order_by": "count desc"})
    out = run_live.rows_step(step, 1, held, lambda row, predicate: True)
    assert out.notes[0].startswith("! PARTIAL: step 1 did not read every value")
    assert out.facts["ranking_complete"] is False
    assert any(n.startswith("! the ranking covers only the values read: step 1") for n in out.notes)
    assert "! ordered by count, largest first · 2 groups ranked, over the rows read" in out.notes


def test_the_trap_note_fires_only_when_limit_cuts_a_non_count_metric() -> None:
    rows = [{"k": k, "size": s} for k, s in (("a", 1), ("b", 9), ("c", 5))]

    def run(arguments: dict[str, Any]) -> run_live.HeldRows:
        return run_live.rows_step(_rows_step(arguments), 1, _held(rows), lambda r, p: True)

    out = run({"by": ["k"], "metrics": ["count", "max(size)"], "limit": 2})
    assert out.facts["order_note"] == {
        "ordered_by": "count, then key", "rank_with": 'order_by="max(attrs.size) desc"',
    }
    assert (
        '! groups are ordered by count, then key, not by max(attrs.size); pass '
        'order_by="max(attrs.size) desc" to rank by it'
    ) in out.notes
    for quiet in (
        {"by": ["k"], "metrics": ["count", "max(size)"], "limit": 3},  # not cut
        {"by": ["k"], "metrics": ["count"], "limit": 1},  # count only
        {"by": ["k"], "metrics": ["max(size)"], "limit": 1, "order_by": "max(size) desc"},
    ):
        out = run(quiet)
        assert "order_note" not in out.facts, quiet
        assert not any("groups are ordered by count" in n for n in out.notes)

    from okg.substrate.mcp.result_text import render

    text = render({
        "status": "ok", "groups": [{"attrs.k": "a", "count": 1, "max(attrs.size)": 1}],
        "by": ["attrs.k"], "metrics": ["count", "max(attrs.size)"], "total_count": 3,
        "total_count_basis": "exact", "truncated": True, "limit": 1, "offset": 0,
        "next_offset": 1, "order_note": {"ordered_by": "count, then key",
                                         "rank_with": 'order_by="max(attrs.size) desc"'},
    }, tool="aggregate", size_facts=False)
    assert (
        '! groups are ordered by count, then key, not by max(attrs.size); pass '
        'order_by="max(attrs.size) desc" to rank by it'
    ) in text.splitlines()
    assert "order_note" not in text


# ============================================================ the reply text


RANKED = {
    "status": "ok",
    "groups": [{"rank": 1, "node_id": "site:T0", "max(attrs.static_total)": 90},
               {"rank": 2, "node_id": "site:T1", "max(attrs.static_total)": 70}],
    "by": ["node_id"], "metrics": ["max(attrs.static_total)"], "total_count": 11,
    "total_count_basis": "exact", "truncated": True, "limit": 2, "offset": 0,
    "next_offset": 2, "generation_id": "g7", "pinned_generation_id": "g7",
    "order_by": {"name": "max(attrs.static_total)", "direction": "desc", "kind": "metric"},
    "ranking_complete": True,
    "tie_at_page_end": {"position": 2, "value": 70, "groups_tied": 3, "shown": 1},
}


def test_a_ranked_reply_prints_its_head_rank_column_tie_and_next() -> None:
    from okg.substrate.mcp.result_text import render

    lines = render(RANKED, tool="aggregate", size_facts=False).splitlines()
    assert lines[0].endswith("next: offset=2 generation_id=g7"), lines[0]
    assert lines[1] == "ordered by max(attrs.static_total), largest first · all 11 groups ranked"
    assert lines[2].split()[:2] == ["rank", "node_id"]
    tie = lines.index(
        "! positions 2-4 hold the same max(attrs.static_total); 1 shown; "
        "ties are ordered by node_id"
    )
    # After the table rows, before any other field.
    assert lines[tie - 1].startswith("2") and tie > 3
    text = "\n".join(lines)
    for key in ("order_by:", "ranking_complete", "tie_at_page_end", "ranked_last_no_value"):
        assert key not in text, key
    live = {**RANKED, "served_from": "retained_live_answer", "input": {"ref": "l1.x"},
            "ranking_complete": False, "not_read": {"count": 2},
            "total_count": 261}
    lines = render(live, tool="aggregate", size_facts=False).splitlines()
    assert lines[0].endswith(
        'next: aggregate(input={"ref": "l1.x"}, by=["node_id"], '
        'metrics=["max(attrs.static_total)"], order_by="max(attrs.static_total) desc", '
        "limit=2, offset=2)"
    ), lines[0]
    assert "ordered by max(attrs.static_total), largest first · 261 groups ranked, over the rows read" in lines
    assert "! ranked over 261 of 263 nodes; 2 not read could rank anywhere (see not_read)" in lines
    other = {**live, "by": ["attrs.tier"], "not_comparable": {"values": 1}}
    lines = render(other, tool="aggregate", size_facts=False).splitlines()
    assert (
        "! 2 nodes not read and 1 not comparable are left out of every group; "
        "the counts and sums may be low"
    ) in lines
    cut = {**RANKED, "ranking_complete": False, "ranking_cut": ["listing cut at 1,000"]}
    lines = render(cut, tool="aggregate", size_facts=False).splitlines()
    assert "! the listing was cut (listing cut at 1,000); nodes past the cut are not ranked" in lines


# Unranked replies as the base renders them (captured from the base renderer,
# e9d53159afd): the ranking adds nothing to a reply without order_by.
UNRANKED = {
    "stored": {
        "status": "ok", "groups": [{"attrs.kind": "a", "count": 3}, {"attrs.kind": None, "count": 3}],
        "by": ["attrs.kind"], "metrics": ["count"], "total_count": 5,
        "total_count_basis": "exact", "retrieval_exhaustive": True, "truncated": True,
        "limit": 2, "offset": 0, "next_offset": 2, "generation_id": "g1",
        "pinned_generation_id": "g1", "predicate_fingerprint": "fp",
    },
    "match": {
        "status": "ok", "groups": [{"match": "Platform", "count": 2, "occurrences": 3}],
        "by": ["match"], "metrics": ["count"], "total_count": 1, "total_count_basis": "exact",
        "retrieval_exhaustive": True, "truncated": False, "limit": 50, "offset": 0,
        "next_offset": None, "generation_id": "g1", "pinned_generation_id": "g1",
    },
    "period": {
        "status": "ok", "groups": [{"month(date.created)": "2026-01-01", "count": 4},
                                   {"month(date.created)": None, "count": 1}],
        "by": ["month(date.created)"], "metrics": ["count"], "total_count": 2,
        "total_count_basis": "exact", "retrieval_exhaustive": True, "truncated": False,
        "limit": 50, "offset": 0, "next_offset": None, "generation_id": "g1",
        "pinned_generation_id": "g1",
    },
    "live": {
        "live_line": "live · Example · read 10:00:00-10:00:01Z · 3 of 3 sites read · 4 calls (1 list + 3 attributes) · kept",
        "status": "ok", "groups": [{"attrs.tier": "1", "count": 2, "max(attrs.size)": 5}],
        "by": ["attrs.tier"], "metrics": ["count", "max(attrs.size)"], "rows_counted": 3,
        "totals_cover": "3 rows of example_list_all_sites(limit=1000); nothing outside these calls is counted",
        "total_count": 2, "groups_total": 2, "total_count_basis": "exact",
        "retrieval_exhaustive": True, "selection_basis": "retained_live_answer",
        "truncated": True, "limit": 1, "offset": 0, "next_offset": 1,
        "pinned_generation_id": "g1", "input": {"ref": "l1.x"},
        "served_from": "retained_live_answer", "trust": "untrusted",
    },
}
UNRANKED_TEXT: dict[str, str] = json.loads(r'''{
 "stored": "aggregate ok · 1-2 of 5 · bound reached (limit 2) · next: offset=2\n\"\\\"attrs.kind\\\"\"\tcount\na\t3\nnull\t3\n--- other fields\nretrieval_exhaustive: true\nlimit: 2\npredicate_fingerprint: fp",
 "match": "aggregate ok · 1-1 of 1 · complete\nmatch\tcount\toccurrences\nPlatform\t2\t3\n--- other fields\nretrieval_exhaustive: true\nlimit: 50",
 "period": "aggregate ok · 1-2 of 2 · complete\n\"\\\"month(date.created)\\\"\"\tcount\n2026-01-01\t4\nnull\t1\n--- other fields\nretrieval_exhaustive: true\nlimit: 50",
 "live": "aggregate ok · 1-1 of 2 · bound reached (limit 1) · next: offset=1\n! totals cover: 3 rows of example_list_all_sites(limit=1000); nothing outside these calls is counted\n\"\\\"attrs.tier\\\"\"\tcount\t\"\\\"max(attrs.size)\\\"\"\n\"1\"\t2\t5\n--- other fields\nlive_line: \"live · Example · read 10:00:00-10:00:01Z · 3 of 3 sites read · 4 calls (1 list + 3 attributes) · kept\"\nrows_counted: 3\ngroups_total: 2\nretrieval_exhaustive: true\nselection_basis: retained_live_answer\nlimit: 1\ninput: {\"ref\":\"l1.x\"}\nserved_from: retained_live_answer\ntrust: untrusted"
}''')


@pytest.mark.parametrize("case", sorted(UNRANKED))
def test_without_order_by_the_renderer_prints_the_base_text(case) -> None:
    """Renderer only (review m5): these payloads print exactly as the base
    printed them. The server-side proof that replies without order_by are
    unchanged is the stored no-change replay in the PACT evidence and the
    unranked assertions of the PostgreSQL tests (the live payload here has no
    `order_note`; the build server adds one to such a reply)."""
    from okg.substrate.mcp.result_text import render

    text = render(UNRANKED[case], tool="aggregate", size_facts=False)
    assert "ordered by" not in text and "rank" not in text.split("\n", 2)[1]
    assert text == UNRANKED_TEXT[case]


def test_without_order_by_a_run_rows_step_is_unchanged() -> None:
    rows = [{"k": "a", "size": 2}, {"k": "b", "size": 7}, {"k": "b", "size": 1}, {"size": 3}]
    step = _rows_step({"by": ["k"], "metrics": ["count", "sum(size)"]})
    out = run_live.rows_step(step, 1, _held(rows), lambda row, predicate: True)
    assert [(_unfence(g["attrs.k"]), g["count"]) for g in out.groups] == [
        ("b", 2), ("(absent)", 1), ("a", 1),
    ]
    assert all("rank" not in g for g in out.groups)
    assert set(out.facts) == {
        "kind", "operator", "input", "rows", "groups", "by", "metrics", "totals_cover",
        "partial", "absent_keys",
    }
    assert out.notes == [
        "! totals cover: 4 rows of step 1 (fx); nothing outside these calls is counted",
        "! 1 row has no k: it is the group (absent)",
    ]


def test_a_later_page_points_to_ranking_left_out_not_to_blocks_that_page_lacks() -> None:
    from okg.substrate.mcp import aggregate_order

    base = {
        "order_by": {"name": "node_id", "direction": "asc"},
        "ranking_complete": False,
        "by": ["node_id"],
        "total_count": 7,
    }
    # Page 2 of a kept set: the counts live only in ranking_left_out.
    page2 = {**base, "ranking_left_out": {"not_read": 2, "not_comparable": 1}}
    (line,) = aggregate_order.partial_lines(page2)
    assert line == (
        "! ranked over 7 of 10 nodes; 2 not read and 1 not comparable could rank anywhere "
        "(see ranking_left_out)"
    )
    # The read that made the set carries its own blocks, and the line names them.
    first = {**base, "not_read": {"count": 2}, "not_comparable": {"values": 1}}
    (line,) = aggregate_order.partial_lines(first)
    assert line.endswith("(see not_read and not_comparable)")
    # A mix: one count on this page's block, one only in ranking_left_out.
    mixed = {**base, "not_read": {"count": 2}, "ranking_left_out": {"not_comparable": 1}}
    (line,) = aggregate_order.partial_lines(mixed)
    assert line.endswith("(see not_read and ranking_left_out)")
