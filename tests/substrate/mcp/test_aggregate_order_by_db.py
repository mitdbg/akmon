"""`aggregate`'s `order_by` on a real publish (PACT read-aggregate-order-by).

Stored sets through a real server and client session: every group ranked
before the page is cut, pages followed by the printed `next:` text, the
missing-key and JSON-null groups, date periods, `match` groups, a result
reference, the statement count, and a `run` graph step.

Live typed sets through the server's own `call_tool` with the recording fake
system of ``test_live_graph_resolver.py``: the q017 shape (the two largest of
all nodes by a per-node field), the adapter calls with and without
`order_by`, refusals with zero calls, partial reads labelled, a cut listing,
page 2 from the printed hint with zero calls, `(absent)` last, access rules
and another reader.
"""
from __future__ import annotations

import json
import pathlib
import re
from collections import Counter
from typing import Any, Iterator

import psycopg
import pytest

from okg.substrate.mcp import live_result_sets
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp._result_payload import JSON_META, payload, result_text
from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes
from tests.substrate.mcp.test_live_graph_resolver import (
    KEEP_AGG,
    REGISTRY,
    Lab,
    _lab,
    _seed_keep_sites,
    _sites,
    _unfence,
)
from tests.substrate.mcp.test_page_next_db import _drive

pytestmark = pytest.mark.postgres

ITEM = SubtypeSpec("item", metadata={"searchable_text_fields": ["text"]})
PROBE = SubtypeSpec("probe")
#: 120 groups of attrs.g; group i holds (i * 37) % 9 + 1 records, so many
#: counts tie, a tie crosses most page edges.
GROUPS = 120


def _size(i: int) -> int:
    return (i * 37) % 9 + 1


def _facts() -> list[Any]:
    facts: list[Any] = []
    for i in range(GROUPS):
        for j in range(_size(i)):
            facts.append(node(
                f"item:{i:03d}-{j}", "item",
                f"widget code-{chr(65 + i % 3)}{i % 2} part {i}",
                g=f"g{i:03d}", when=f"2026-{1 + i % 4:02d}-1{j % 9}",
            ))
    # B2: a record missing the key, one with a JSON null, one with a value.
    facts.append(node("probe:missing", "probe", "missing", tags=["t"]))
    facts.append(node("probe:null", "probe", "null", k=None, tags=["t"]))
    facts.append(node("probe:value", "probe", "value", k="v", tags=["t"]))
    return facts


@pytest.fixture
def deployment(clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path) -> str:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(clean_runtime, deployment, subtypes=[ITEM, PROBE], nodes=_facts())
    return deployment


async def _json(client: Any, tool: str, arguments: dict) -> dict:
    result = await client.call_tool(tool, arguments, meta=JSON_META)
    assert not result.isError, result_text(result)
    return payload(result)


async def _text(client: Any, tool: str, arguments: dict) -> str:
    result = await client.call_tool(tool, arguments)
    assert not result.isError, result_text(result)
    return result_text(result)


async def _refused(client: Any, arguments: dict) -> dict:
    result = await client.call_tool("aggregate", arguments, meta=JSON_META)
    assert result.isError, result_text(result)
    return payload(result)["error"]


BY_G = {"input": {"kind": "nodes", "subtype": "item"}, "by": ["attrs.g"], "metrics": ["count"]}


def _checker_sort(groups: list[dict], key: str, desc: bool) -> list[tuple[Any, int]]:
    """The checker's own ranking of an unranked full list: by count (desc or
    asc), ties by the key ascending."""
    pairs = [(g[key], g["count"]) for g in groups]
    pairs.sort(key=lambda p: p[0])
    pairs.sort(key=lambda p: p[1], reverse=desc)
    return pairs


# ============================================================ stored


def test_stored_ranking_is_exact_over_all_groups_not_the_first_page(
    clean_runtime: str, deployment: str,
) -> None:
    """FAILS ON THE BASE: order_by is an unknown argument of aggregate."""
    async def body(client: Any) -> None:
        full = await _json(client, "aggregate", {**BY_G, "limit": 500})
        assert full["truncated"] is False and full["total_count"] == GROUPS
        assert "rank" not in full["groups"][0] and "order_by" not in full
        smallest = await _json(client, "aggregate", {**BY_G, "order_by": "count", "limit": 2})
        expected = _checker_sort(full["groups"], "attrs.g", desc=False)
        assert [(g["attrs.g"], g["count"]) for g in smallest["groups"]] == expected[:2]
        assert [g["rank"] for g in smallest["groups"]] == [1, 2]
        assert smallest["total_count"] == GROUPS and smallest["total_count_basis"] == "exact"
        assert smallest["order_by"] == {"name": "count", "direction": "asc", "kind": "metric"}
        assert smallest["ranking_complete"] is True
        # Every first page of the unranked reply holds larger groups only.
        first_page = await _json(client, "aggregate", {**BY_G, "limit": 2})
        assert {g["count"] for g in first_page["groups"]} == {9}
        ranked = await _json(client, "aggregate", {**BY_G, "order_by": "count desc", "limit": 500})
        assert [(g["attrs.g"], g["count"]) for g in ranked["groups"]] == _checker_sort(
            full["groups"], "attrs.g", desc=True,
        )
        by_key = await _json(client, "aggregate", {**BY_G, "order_by": "-attrs.g", "limit": 3})
        assert [g["attrs.g"] for g in by_key["groups"]] == ["g119", "g118", "g117"]

    _drive(clean_runtime, deployment, body)


_NEXT = re.compile(r"next: offset=(\d+) generation_id=(\S+)")


def test_stored_pages_followed_by_the_printed_next_hint_join_to_the_full_ranked_list(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        full = await _json(client, "aggregate", {**BY_G, "order_by": "count desc", "limit": 500})
        want = [(g["rank"], g["attrs.g"]) for g in full["groups"]]
        call = {**BY_G, "order_by": "count desc", "limit": 7}
        got: list[tuple[int, str]] = []
        ties_seen = 0
        for page in range(40):
            text = await _text(client, "aggregate", call)
            as_json = await _json(client, "aggregate", call)
            got += [(g["rank"], g["attrs.g"]) for g in as_json["groups"]]
            ties_seen += "tie_at_page_end" in as_json
            assert any(line.startswith("ordered by count, largest first")
                       for line in text.splitlines()), text[:800]
            found = _NEXT.search(text.splitlines()[0])
            if found is None:
                assert as_json["truncated"] is False
                break
            if page == 1:
                # A publish between pages: the printed hint is pinned.
                _republish(clean_runtime, deployment)
            call = {**BY_G, "order_by": "count desc", "limit": 7,
                    "offset": int(found.group(1)), "generation_id": found.group(2)}
        assert got == want
        assert len({g for _r, g in got}) == GROUPS
        assert ties_seen > 0

    _drive(clean_runtime, deployment, body)


def _republish(dsn: str, deployment: str) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [node("item:late", "item", "late widget", g="g000")],
            source="system:analyst_search_fixture", strict_endpoints=True,
        )
    result = Publisher(dsn, deployment, catalog_identity=bind_test_catalog_writer(dsn, deployment)).cycle()
    assert result["status"] == "published", result


PROBE_K = {"input": {"kind": "nodes", "subtype": "probe"}, "by": ["attrs.k"], "metrics": ["count"]}


def test_stored_missing_key_and_json_null_groups(clean_runtime: str, deployment: str) -> None:
    """B2 and N1: pins what publish keeps, that the unranked reply keeps its
    two None groups (unranked order as the base: count desc, then str(key)),
    and that the ranked reply prints null before (absent), both last, in the
    same order on 20 calls."""
    async def body(client: Any) -> None:
        unranked = await _json(client, "aggregate", PROBE_K)
        keys = [g["attrs.k"] for g in unranked["groups"]]
        # Publish keeps both: the missing key (SQL NULL) and the JSON null
        # are two groups, each printed null without order_by.
        assert sorted(map(str, keys)) == ["None", "None", "v"], unranked["groups"]
        assert all(set(g) == {"attrs.k", "count"} for g in unranked["groups"])
        assert unranked["total_count"] == 3
        seen = set()
        for desc in ("", " desc") * 10:
            ranked = await _json(client, "aggregate", {**PROBE_K, "order_by": "attrs.k" + desc})
            order = [g["attrs.k"] for g in ranked["groups"]]
            assert order == ["v", None, "(absent)"], order
            assert ranked["ranked_last_no_value"] == 2
            seen.add(json.dumps(ranked["groups"]))
        assert len(seen) == 1
        by_count = await _json(client, "aggregate", {**PROBE_K, "order_by": "count desc"})
        # Equal counts: ties by the key, null before (absent), both last.
        assert [g["attrs.k"] for g in by_count["groups"]] == ["v", None, "(absent)"]
        text = await _text(client, "aggregate", {**PROBE_K, "order_by": "attrs.k"})
        assert "2 with no attrs.k, ranked last" in text

    _drive(clean_runtime, deployment, body)


def test_an_each_count_with_another_field_ranks_null_and_absent_in_a_total_order(
    clean_runtime: str, deployment: str,
) -> None:
    """Review m2: on a list-element count the other attrs. fields carry the
    hidden IS NULL flag, so a missing key and a JSON null stay two groups in
    one fixed order; the element itself is never flagged."""
    call = {"input": {"kind": "nodes", "subtype": "probe"},
            "by": ["each(attrs.tags)", "attrs.k"], "metrics": ["count"]}

    async def body(client: Any) -> None:
        unranked = await _json(client, "aggregate", call)
        assert sorted(str(g["attrs.k"]) for g in unranked["groups"]) == ["None", "None", "v"]
        seen = set()
        for desc in ("", " desc") * 5:
            ranked = await _json(client, "aggregate", {**call, "order_by": "attrs.k" + desc})
            assert [g["attrs.k"] for g in ranked["groups"]] == ["v", None, "(absent)"]
            assert {g["each(attrs.tags)"] for g in ranked["groups"]} == {"t"}
            seen.add(json.dumps(ranked["groups"]))
        assert len(seen) == 1

    _drive(clean_runtime, deployment, body)


def test_period_groups_rank_by_count_and_newest_first(clean_runtime: str, deployment: str) -> None:
    month = {"input": {"kind": "nodes", "subtype": "item"}, "by": ["month(date.attrs.when)"],
             "metrics": ["count"]}

    async def body(client: Any) -> None:
        unranked = await _json(client, "aggregate", month)
        assert [g["month(date.attrs.when)"] for g in unranked["groups"]] == sorted(
            g["month(date.attrs.when)"] for g in unranked["groups"]
        )
        newest = await _json(client, "aggregate", {**month, "order_by": "month(date.attrs.when) desc"})
        assert [g["month(date.attrs.when)"] for g in newest["groups"]] == list(reversed(
            [g["month(date.attrs.when)"] for g in unranked["groups"]]
        ))
        by_count = await _json(client, "aggregate", {**month, "order_by": "count desc", "limit": 1})
        top = max(unranked["groups"], key=lambda g: (g["count"], ""))
        assert by_count["groups"][0]["count"] == top["count"]
        assert by_count["groups"][0]["rank"] == 1

    _drive(clean_runtime, deployment, body)


def test_match_groups_rank_by_occurrences(clean_runtime: str, deployment: str) -> None:
    call = {"where": {"type": "item", "text": {"op": "matches", "value": "re:/code-([A-Z][0-9])/"}},
            "by": ["match"], "metrics": ["count"]}

    async def body(client: Any) -> None:
        unranked = await _json(client, "aggregate", call)
        assert "rank" not in unranked["groups"][0]
        ranked = await _json(client, "aggregate", {**call, "order_by": "occurrences"})
        occurrences = [g["occurrences"] for g in ranked["groups"]]
        assert occurrences == sorted(occurrences)
        assert [g["rank"] for g in ranked["groups"]] == list(range(1, len(occurrences) + 1))
        by_text = await _json(client, "aggregate", {**call, "order_by": "match desc"})
        texts = [g["match"] for g in by_text["groups"]]
        assert len(texts) == len(unranked["groups"])
        assert [t.split("\n")[1] if "\n" in t else t for t in texts] == sorted(
            (t.split("\n")[1] if "\n" in t else t for t in texts), reverse=True,
        )

    _drive(clean_runtime, deployment, body)


def test_a_result_reference_aggregate_takes_order_by(clean_runtime: str, deployment: str) -> None:
    async def body(client: Any) -> None:
        search = await _json(client, "search", {"query": "widget", "retain": True, "limit": 50})
        ref = search["result_reference"]["id"]
        call = {"input": {"ref": ref}, "by": ["attrs.g"], "metrics": ["count"], "limit": 500}
        unranked = await _json(client, "aggregate", call)
        assert "rank" not in unranked["groups"][0] and "order_by" not in unranked
        ranked = await _json(client, "aggregate", {**call, "order_by": "count desc"})
        assert [(g["attrs.g"], g["count"]) for g in ranked["groups"]] == _checker_sort(
            unranked["groups"], "attrs.g", desc=True,
        )
        assert ranked["total_count"] == unranked["total_count"]

    _drive(clean_runtime, deployment, body)


def _counting(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []
    real = psycopg.Cursor.execute

    def execute(self: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
        seen.append(query if isinstance(query, str) else repr(query))
        return real(self, query, *args, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", execute)
    return seen


def test_stored_ranking_runs_no_extra_sql_statement(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = _counting(monkeypatch)

    async def body(client: Any) -> None:
        ranked_call = {**BY_G, "order_by": "count desc"}
        for warm in (BY_G, ranked_call):
            await _json(client, "aggregate", warm)
        # Some statements run on some calls and not others, for reasons that
        # have nothing to do with ranking, so the statements of two calls
        # differ (nightly 40288405182: unranked 50, 52, 51; #4196 CI: 5 pool
        # resets in one call, 4 in the other):
        # - the session's database fingerprint, read once per pooled
        #   connection (`session._db_fingerprint`);
        # - the planner's row estimate, which reads `pg_stats` only once
        #   autovacuum has analyzed the table (`graph_read._column_row_estimate`);
        # - the pool's reset of a returned connection (`db.pool.
        #   reset_returned_connection`: advisory-lock release and the
        #   commit-witness clear), which psycopg_pool runs on its maintenance
        #   thread and so can land inside the next call's capture.
        # So the check is one-sided: a ranked call may run no statement that
        # the unranked call of the same round did not, other than these; the
        # fingerprint and the estimate run at most once per call, and the
        # grouping statement itself (whose columns ranking extends) exactly
        # once.
        once_at_most = ("FROM pg_control_system()", "FROM pg_stats WHERE")
        timing_only = (
            *once_at_most,
            "pg_advisory_unlock_all()",
            "SELECT set_config(%s, '', false)",
        )

        def statements() -> Counter[str]:
            return Counter(s for s in seen if "GROUP BY" not in s)

        for _round in range(3):
            rounds: dict[str, Counter[str]] = {}
            for name, call in (("unranked", BY_G), ("ranked", ranked_call)):
                seen.clear()
                await _json(client, "aggregate", call)
                assert sum("GROUP BY" in s for s in seen) == 1, (name, seen)
                for v in once_at_most:
                    assert sum(v in s for s in seen) <= 1, (name, v, seen)
                rounds[name] = statements()
            extra = rounds["ranked"] - rounds["unranked"]
            added = {
                s: n for s, n in extra.items()
                if not any(t in s for t in timing_only)
            }
            assert not added, (added, rounds)

    _drive(clean_runtime, deployment, body)


def test_a_stored_order_by_typo_is_refused_before_any_statement(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FAILS ON THE BASE for the reason: order_by is an unknown argument."""
    seen = _counting(monkeypatch)

    async def body(client: Any) -> None:
        await _json(client, "aggregate", BY_G)  # warm
        seen.clear()
        error = await _refused(client, {**BY_G, "order_by": "cnt desc"})
        assert error["data"]["reason"] == "order_by_unknown", error
        assert not any("GROUP BY" in s or "effective_aggregate" in s for s in seen), seen

    _drive(clean_runtime, deployment, body)


def test_order_by_in_a_run_graph_step_reaches_aggregate(clean_runtime: str, deployment: str) -> None:
    async def body(client: Any) -> None:
        result = await client.call_tool("run", {"steps": [
            {"aggregate": {**BY_G, "order_by": "count desc", "limit": 3}},
        ]}, meta=JSON_META)
        assert not result.isError, result_text(result)
        text = json.dumps(payload(result))
        assert '"order_by": {"name": "count", "direction": "desc", "kind": "metric"}' in text
        assert '"rank": 1' in text

    _drive(clean_runtime, deployment, body)


# ============================================================ live


@pytest.fixture
def lab(clean_runtime: str, tmp_path, monkeypatch) -> Iterator[Lab]:
    previous = live_result_sets.install_live_store(live_result_sets.new_store())
    try:
        with _lab(clean_runtime, tmp_path, monkeypatch) as built:
            yield built
    finally:
        live_result_sets.install_live_store(previous)


Q017 = {"input": {"subtype": "site"}, "by": ["node_id"],
        "metrics": ["max(attrs.static_total)"],
        "order_by": "max(attrs.static_total) desc", "limit": 2}


def _seed_totals(lab: Lab, count: int = 30) -> list[str]:
    lab.example.sites = _sites(count)
    names = [row["site"] for row in lab.example.sites]
    lab.example.totals = {name: (i * 7919) % 1000 for i, name in enumerate(names)}
    return names


def _top(lab: Lab, n: int) -> list[str]:
    ordered = sorted(lab.example.totals.items(), key=lambda kv: (-kv[1], kv[0]))
    return [f"site:{name}" for name, _total in ordered[:n]]


def test_live_ranking_by_max_returns_the_top_two_of_all_nodes(lab) -> None:
    """FAILS ON THE BASE: order_by is an unknown argument of aggregate."""
    _seed_totals(lab)
    body = lab.ok("aggregate", Q017)
    got = [_unfence(g["node_id"]) for g in body["groups"]]
    assert got == _top(lab, 2), body["groups"]
    assert [g["rank"] for g in body["groups"]] == [1, 2]
    assert body["total_count"] == 30 and body["ranking_complete"] is True
    assert body["order_by"]["name"] == "max(attrs.static_total)"
    # Unranked: the trap (every node a group of count 1, name order) and its note.
    lab.example.calls.clear()
    plain = lab.ok("aggregate", {k: v for k, v in Q017.items() if k != "order_by"})
    assert [_unfence(g["node_id"]) for g in plain["groups"]] == [
        "site:Z2_XX_S0000", "site:Z2_XX_S0001",
    ]
    assert plain["order_note"] == {
        "ordered_by": "count, then key",
        "rank_with": 'order_by="max(attrs.static_total) desc"',
    }


def test_live_ranking_makes_the_same_adapter_calls_as_the_unranked_call(lab) -> None:
    _seed_totals(lab)
    lab.ok("aggregate", Q017)
    ranked = sorted(json.dumps(c, sort_keys=True) for c in lab.example.calls)
    lab.example.calls.clear()
    lab.ok("aggregate", {k: v for k, v in Q017.items() if k not in ("order_by", "limit")})
    unranked = sorted(json.dumps(c, sort_keys=True) for c in lab.example.calls)
    assert ranked == unranked and len(ranked) == 31


def test_a_live_order_by_typo_is_refused_with_zero_adapter_calls(lab) -> None:
    """FAILS ON THE BASE for the reason: order_by is an unknown argument."""
    _seed_totals(lab)
    body = lab.refused("aggregate", {**Q017, "order_by": "max(attrs.static_totl) desc"})
    assert body["error"]["data"]["reason"] == "order_by_unknown", body
    assert body["error"]["data"]["closest"] == "max(attrs.static_total)"
    assert lab.example.total() == 0
    body = lab.refused("aggregate", {**Q017, "order_by": "count desc"})
    assert body["error"]["data"]["reason"] == "order_by_not_in_metrics"
    assert lab.example.total() == 0


def test_live_order_by_count_desc_is_not_read_as_an_upstream_field(lab) -> None:
    _seed_keep_sites(lab, sites=40)
    lab.example.fail = set()
    body = lab.ok("aggregate", {**KEEP_AGG, "order_by": "count desc"})
    assert [g["count"] for g in body["groups"]] == [27, 10, 3]
    assert lab.example.total() == 41


def _followed(lab: Lab, text: str) -> dict[str, Any]:
    """The arguments of the `next: aggregate(...)` hint on a reply's status
    line, exactly as printed: nothing is added by hand."""
    status = text.splitlines()[0]
    start = status.index("next: aggregate(") + len("next: aggregate(")
    decoder = json.JSONDecoder()
    arguments: dict[str, Any] = {}
    rest = status[start:]
    while not rest.startswith(")"):
        name, _, rest = rest.partition("=")
        value, end = decoder.raw_decode(rest)
        arguments[name.strip()] = value
        rest = rest[end:].removeprefix(", ")
    return arguments


def test_live_ranking_with_failed_node_calls_or_not_comparable_rows_is_labelled_incomplete(lab) -> None:
    lab.example.sites = _sites(12)
    names = [row["site"] for row in lab.example.sites]
    for i, name in enumerate(names):
        lab.example.attrs[name] = {"weight": i, "tier": str(i % 2)}
    lab.example.fail = {names[3], names[4]}
    call = {"input": {"subtype": "site"}, "by": ["node_id"], "metrics": ["max(attrs.weight)"],
            "order_by": "max(attrs.weight) desc", "limit": 3}
    body = lab.ok("aggregate", call)
    assert body["ranking_complete"] is False and body["not_read"]["count"] == 2
    text = lab.call("aggregate", call, fmt="text").content[0].text
    assert "! ranked over 10 of 12 nodes; 2 not read could rank anywhere (see not_read)" in text
    assert "10 groups ranked, over the rows read" in text
    # Page 2, read from the kept set by the printed hint (review M2): still
    # incomplete, with the first read's counts.
    page2 = lab.ok("aggregate", _followed(lab, text))
    assert page2["ranking_complete"] is False, json.dumps(page2)[:2000]
    # The kept set holds the nodes not read (their fields say so): this page
    # counts them itself.
    assert page2["not_read"]["count"] == 2
    text2 = lab.call("aggregate", _followed(lab, text), fmt="text").content[0].text
    assert "! ranked over 10 of 12 nodes; 2 not read could rank anywhere (see not_read)" in text2
    # Review N-1: the two nodes whose call failed are still rows of the kept
    # set; a ranking that needs none of their fields covers all 12.
    ref = _followed(lab, text)["input"]
    whole = {"input": ref, "by": ["node_id"], "metrics": ["count"], "order_by": "node_id"}
    body = lab.ok("aggregate", whole)
    assert len(body["groups"]) == 12 and body["total_count"] == 12
    assert body["ranking_complete"] is True, json.dumps(body)[:2000]
    assert "ranking_left_out" not in body and "not_read" not in body
    text3 = lab.call("aggregate", whole, fmt="text").content[0].text
    assert "all 12 groups ranked" in text3 and "not read could rank anywhere" not in text3
    # Rows a condition cannot decide, by another field: left out of every group.
    lab.example.fail = set()
    lab.example.attrs[names[5]]["weight"] = "heavy"
    other = {"where": {"type": "site", "attrs.weight": {"op": "gt", "value": 2}},
             "by": ["attrs.tier"], "metrics": ["count"], "order_by": "count desc", "limit": 1}
    body = lab.ok("aggregate", other)
    assert body["ranking_complete"] is False, json.dumps(body)[:2000]
    text = lab.call("aggregate", other, fmt="text").content[0].text
    assert "1 not comparable is left out of every group; the counts and sums may be low" in text
    page2 = lab.ok("aggregate", _followed(lab, text))
    assert page2["ranking_complete"] is False, json.dumps(page2)[:2000]
    assert page2["ranking_left_out"]["not_comparable"] == 1
    text2 = lab.call("aggregate", _followed(lab, text), fmt="text").content[0].text
    assert "1 not comparable is left out of every group; the counts and sums may be low" in text2
    assert "all 2 groups ranked" not in text2


def test_live_ranking_over_a_cut_listing_says_cut(lab) -> None:
    lab.example.sites = _sites(1000)
    body = lab.ok("aggregate", {"input": {"subtype": "site"}, "by": ["node_id"],
                                "metrics": ["count"], "order_by": "node_id desc", "limit": 2})
    assert lab.example.total() == 1
    assert body["ranking_complete"] is False and body["ranking_cut"]
    text = lab.call("aggregate", {"input": {"subtype": "site"}, "by": ["node_id"],
                                  "metrics": ["count"], "order_by": "node_id desc", "limit": 2},
                    fmt="text").content[0].text
    assert "! the listing was cut (" in text and "nodes past the cut are not ranked" in text


def test_a_total_over_a_cut_typed_listing_says_it_is_short(lab) -> None:
    """PACT live-kept-answer-cut (review M5): a typed listing cut by the
    external system or at its limit (`live_calls.listing`) reads as short
    on the cover line and in the JSON fields. A whole set stays exact
    (test_a_whole_kept_call_aggregated_stays_exact)."""
    lab.example.sites = _sites(1000)
    body = lab.ok("aggregate", {"input": {"subtype": "site"}, "by": [], "metrics": ["count"]})
    assert body["groups"][0]["count"] == 1000
    cover = body["totals_cover"]
    assert "; the live listing was cut (" in cover, cover
    assert cover.endswith("so totals count only the rows listed; count is at least the value shown")
    assert body["listing_cut"] and body["coverage"]["retrieval"]["status"] == "bounded"
    assert body["total_count_basis"] == "lower_bound"


def test_live_ranked_page_two_from_the_printed_next_hint_makes_zero_calls(lab) -> None:
    """Review M1: the printed hint is followed exactly as printed, with no
    argument added by hand; it carries the page size and, on a kept read
    with a condition, the condition."""
    _seed_totals(lab)
    text = lab.call("aggregate", Q017, fmt="text").content[0].text
    follow = _followed(lab, text)
    calls = lab.example.total()
    assert follow["input"]["ref"].startswith("l1.") and follow["limit"] == 2
    assert follow["order_by"] == "max(attrs.static_total) desc" and follow["offset"] == 2
    page2 = lab.ok("aggregate", follow)
    assert lab.example.total() == calls
    assert [_unfence(g["node_id"]) for g in page2["groups"]] == _top(lab, 4)[2:]
    assert [g["rank"] for g in page2["groups"]] == [3, 4]
    # A kept read with a condition and a page size of its own: page 2 from
    # its hint ranks the same filtered groups.
    ref = follow["input"]["ref"]
    kept = {"input": {"ref": ref}, "by": ["node_id"], "metrics": ["max(attrs.static_total)"],
            "predicate": {"op": "lt", "field": "attrs.static_total", "value": 500},
            "order_by": "max(attrs.static_total) desc", "limit": 3}
    first = lab.ok("aggregate", kept)
    text = lab.call("aggregate", kept, fmt="text").content[0].text
    follow = _followed(lab, text)
    assert follow["limit"] == 3 and follow["offset"] == 3 and "predicate" in follow, follow
    page2 = lab.ok("aggregate", follow)
    assert lab.example.total() == calls
    below = sorted(
        (t for t in lab.example.totals.values() if t < 500), reverse=True,
    )
    shown = [g["max(attrs.static_total)"] for g in first["groups"] + page2["groups"]]
    assert shown == below[:6], (shown, below[:6])
    assert [g["rank"] for g in page2["groups"]] == [4, 5, 6]


def test_a_run_rows_step_order_by_typo_makes_no_upstream_call(lab) -> None:
    """Review m3: refused when the steps are classified, before step 1's
    listing call."""
    from tests.substrate.mcp.test_live_graph_resolver import STORED_SYSTEM

    lab.example.sites = _sites(5)
    body = lab.refused("run", {"steps": [
        {"expand": {"input": {"node_id": STORED_SYSTEM},
                    "live": {"relation": "example_list_all_sites", "args": {"limit": 1000}}}},
        {"aggregate": {"input": "$1", "by": ["site"], "metrics": ["count"],
                       "order_by": "cnt desc"}},
    ]})
    assert "is not a metric or a by field" in json.dumps(body), json.dumps(body)[:2000]
    assert lab.example.total() == 0


def test_live_absent_group_ranks_last(lab) -> None:
    _seed_keep_sites(lab, sites=40)
    lab.example.fail = set()
    for desc in ("", " desc"):
        body = lab.ok("aggregate", {**KEEP_AGG, "order_by": "attrs.keep_flag" + desc})
        keys = [_unfence(g["attrs.keep_flag"]) for g in body["groups"]]
        assert keys == ([True, False] if desc else [False, True]) + ["(absent)"], keys
        assert body["ranked_last_no_value"] == 1


def test_a_ranked_live_aggregate_is_refused_under_access_rules(lab) -> None:
    _seed_totals(lab, 5)
    lab.mcp._okg_access_rules_state = lambda: True
    try:
        body = lab.refused("aggregate", Q017)
    finally:
        del lab.mcp._okg_access_rules_state
    assert body["error"]["code"] == "live_refused_under_access_rules"
    assert lab.example.total() == 0


def test_a_ranked_kept_set_read_with_another_reader_key_is_refused(lab, monkeypatch) -> None:
    _seed_totals(lab)
    first = lab.ok("aggregate", Q017)
    ref = first["input"]["ref"]
    calls = lab.example.total()
    own = lab.ok("aggregate", {**Q017, "input": {"ref": ref}, "offset": 2})
    assert [g["rank"] for g in own["groups"]] == [3, 4]
    from okg.substrate.mcp import continuation

    monkeypatch.setattr(continuation, "reader_key", lambda *a, **k: "another-reader")
    body = lab.refused("aggregate", {**Q017, "input": {"ref": ref}, "offset": 2})
    assert body["error"]["code"] in ("result_reference_invalid", "continuation_token_invalid"), body
    assert lab.example.total() == calls


def test_a_kept_ref_order_by_typo_is_refused_with_zero_reads(lab, monkeypatch) -> None:
    """N2: on input={"ref": "l1..."} the shared check runs before the kept
    set is opened (`_live_open`: no anchor or binding re-check, no rows
    read) and before any growth call. (The resolver still looks the set up
    in memory to decide whether a field must be fetched; when one must, its
    own argument check refuses before the fan-out.)"""
    _seed_totals(lab)
    ref = lab.ok("aggregate", Q017)["input"]["ref"]
    calls = lab.example.total()
    from okg.substrate.mcp import live_reader

    opened: list[Any] = []
    real = live_reader.names_of

    def names_of(*args: Any, **kwargs: Any) -> Any:
        opened.append(args)
        return real(*args, **kwargs)

    monkeypatch.setattr(live_reader, "names_of", names_of)
    body = lab.refused("aggregate", {**Q017, "input": {"ref": ref}, "order_by": "max(attrs.x) desc"})
    assert body["error"]["data"]["reason"] == "order_by_not_in_metrics", body
    body = lab.refused("aggregate", {**Q017, "input": {"ref": ref}, "order_by": "maxx desc"})
    assert body["error"]["data"]["reason"] == "order_by_unknown", body
    # A field the kept set lacks would grow it: refused before the fan-out.
    body = lab.refused("aggregate", {**Q017, "input": {"ref": ref},
                                     "metrics": ["max(attrs.static_used)"],
                                     "order_by": "maxx desc"})
    assert body["error"]["data"]["reason"] == "order_by_unknown", body
    assert opened == [] and lab.example.total() == calls
    assert REGISTRY is lab.mcp._okg_live_types
