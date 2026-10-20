"""scale-operator-attr-reads / task.pushdown.

The attr-operator scan (aggregate / filter / map over attrs.<key>) pushes the
input's subtype (or edge_type) and any attr-FREE predicate clause into the
chronos scan WHERE, so it processes O(matched rows) instead of a fixed
graph-wide 10K window. Two proofs:

  (a) structural / no-DB: the pushdown predicate an attr aggregate produces
      lowers to SQL whose WHERE carries the subtype (and a stable eq/in
      predicate), while a map with no subtype lowers to no WHERE; and
  (b) DB-backed: a SELECTIVE input (a tiny subtype) over a fixture graph with
      >10,000 nodes total does NOT raise the bounded-overlay error, returns the
      correct effective-attr group counts, and matches the prior Python-overlay
      path bit-for-bit (parity) -- including when an accepted overlay overrides a
      raw value and when the predicate itself touches attrs (stays in Python).

The prior (pre-pushdown) behavior is reproduced by forcing the unfiltered scan
(`scan_predicate=None`) on the same fixture, which trips the cap -- proving the
pushdown is what makes the selective input scale, not a larger cap.
"""

from __future__ import annotations

import dataclasses
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.attributes import (  # noqa: E402
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.mcp import open_session  # noqa: E402
from okg.substrate.mcp.server import build_server  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.transform import (  # noqa: E402
    FunctionRegistry,
    Registries,
    chronos_surface_registry,
    lower_query,
)
from okg.substrate.transform.operators import (  # noqa: E402
    filter_program,
    map_program,
)
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


# --------------------------------------------------------------------------- #
# (a) structural: the pushdown predicate lowers to a subtype/predicate WHERE.
# This needs no DB -- it lowers the same TransformProgram the attr-operator scan
# feeds when a subtype (and a stable eq/in predicate) are pushed down.
# --------------------------------------------------------------------------- #


def test_attr_pushdown_lowers_subtype_and_predicate_into_sql_where() -> None:
    registries = Registries(
        surfaces=chronos_surface_registry(), functions=FunctionRegistry()
    )

    # The shape `aggregate(input={"subtype":"ci_workflow"}, by=["attrs.conclusion"])`
    # produces for the pushdown: subtype pushed, attr-free `in` on a stable field
    # pushed, attrs.<key> NOT pushed (stays Python). Here we lower just the
    # SQL-pushable portion (subtype + a stable `in`).
    scan_predicate = {
        "op": "and",
        "clauses": [
            {"op": "eq", "field": "subtype", "value": "ci_workflow"},
            {"op": "in", "field": "node_id", "values": ["a", "b"]},
        ],
    }
    program, _columns = filter_program(
        kind="nodes",
        predicate=scan_predicate,
        projection=None,
        limit=10_001,
        offset=0,
    )
    sql, params = lower_query(program, registries)

    # The WHERE pushes subtype + the stable predicate into SQL (not a graph-wide
    # scan), naming only the Chronos graph surface.
    assert "WHERE" in sql
    assert "subtype" in sql
    assert "okg.graph_nodes" in sql
    assert "okg.nodes_live" not in sql and "okg.v_nodes" not in sql
    assert "ci_workflow" in params.values()
    assert "a" in params.values() and "b" in params.values()

    # By contrast, a map with no subtype/predicate lowers to NO WHERE -- the
    # graph-wide scan the pushdown is designed to avoid.
    map_prog, _ = map_program(kind="nodes", projection=None, limit=10_001, offset=0)
    map_sql, _ = lower_query(map_prog, registries)
    assert "WHERE" not in map_sql


def test_attr_pushdown_edge_scope_pushes_edge_type() -> None:
    registries = Registries(
        surfaces=chronos_surface_registry(), functions=FunctionRegistry()
    )
    program, _columns = filter_program(
        kind="edges",
        predicate={"op": "eq", "field": "edge_type", "value": "contains"},
        projection=None,
        limit=10_001,
        offset=0,
    )
    sql, params = lower_query(program, registries)
    assert "WHERE" in sql
    assert "edge_type" in sql
    assert "okg.graph_edges" in sql
    assert "contains" in params.values()


# --------------------------------------------------------------------------- #
# (b) DB-backed: a selective subtype over a >10K-node graph does NOT trip the
# bounded-overlay cap; results match the prior Python-overlay path (parity).
# --------------------------------------------------------------------------- #

_BULK_NODE_COUNT = 10_050  # > the 10_000 bounded-Python-overlay cap


def _seed_large_graph(clean_runtime: str) -> dict[str, str]:
    """Seed a graph with >10K `bulk` nodes plus a tiny selective `ci_workflow`
    subtype, then accept an effective-attr overlay that OVERRIDES a raw value
    on one selective node (so parity must honor the overlay), and publish +
    materialize the effective-attrs snapshot for the published generation."""
    suffix = uuid.uuid4().hex[:8]
    # 3 selective ci_workflow nodes: raw conclusion success / failure / success.
    wf = [
        (f"wf:1:{suffix}", "success"),
        (f"wf:2:{suffix}", "failure"),
        (f"wf:3:{suffix}", "success"),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["ci_workflow", "bulk"])
        store = ChronosGraphStore.from_connection(conn)
        bulk_facts = [
            NodeFact(
                f"bulk:{i}:{suffix}",
                "bulk",
                {"conclusion": "noise"},
                {"id": f"bulk:{i}:{suffix}"},
                {"v": 1},
            )
            for i in range(_BULK_NODE_COUNT)
        ]
        wf_facts = [
            NodeFact(
                node_id,
                "ci_workflow",
                {"conclusion": conclusion},
                {"id": node_id},
                {"v": 1},
            )
            for node_id, conclusion in wf
        ]
        store.apply_facts(
            bulk_facts + wf_facts, source="default", strict_endpoints=True
        )
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])

    # Accept an overlay that OVERRIDES wf:3's raw conclusion success -> failure.
    # A parity bug that pushed the attr predicate into raw SQL would mis-group it.
    overridden = wf[2][0]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id=overridden,
            generation_id=gen,
            attr_key="conclusion",
            value="failure",
            producer="test:pushdown-overlay",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return {"generation_id": gen, "suffix": suffix, "overridden": overridden}


def _attr_aggregate(tools, *, gen: str, subtype: str = "ci_workflow"):
    return tools["aggregate"].fn(
        input={"kind": "nodes", "subtype": subtype},
        by=["attrs.conclusion"],
        metrics=["count"],
        generation_id=gen,
    )


def test_attr_aggregate_over_10k_subtype_counts_complete_in_sql(
    clean_runtime: str,
) -> None:
    """MUST-FIX #1 (the headline requirement): aggregate over a >10K-row subtype
    returns the COMPLETE correct count, counted entirely in SQL via GROUP BY, NOT
    a 10_001-row window. The OLD code returned {"noise": 10001} with
    truncated=False (a silent undercount); the fixed path counts every one of the
    10_050 `bulk` rows in SQL."""
    fixture = _seed_large_graph(clean_runtime)
    gen = fixture["generation_id"]
    mcp = build_server(dsn=clean_runtime)
    tools = mcp._tool_manager._tools
    try:
        result = tools["aggregate"].fn(
            input={"kind": "nodes", "subtype": "bulk"},
            by=["attrs.conclusion"],
            metrics=["count"],
            generation_id=gen,
        )
        groups = {g["attrs.conclusion"]: g["count"] for g in result["groups"]}
        # The COMPLETE count -- all 10_050 bulk rows, not a 10_001 window.
        assert groups == {"noise": _BULK_NODE_COUNT}, groups
        assert groups != {"noise": 10_001}
        # One group, far under the page limit -> not truncated.
        assert result["truncated"] is False, result
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_attr_aggregate_group_page_truncation_is_true(
    clean_runtime: str,
) -> None:
    """MUST-FIX #1 truncation honesty: when there are MORE distinct groups than
    the page `limit`, the aggregate reports truncated=true (a real truncation of
    the group listing), never a silent partial. Each bulk node here gets a unique
    `gid`, so grouping by attrs.gid yields 10_050 groups; limit=5 must truncate."""
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["bulk"])
        store = ChronosGraphStore.from_connection(conn)
        facts = [
            NodeFact(
                f"g:{i}:{suffix}",
                "bulk",
                {"gid": f"k{i}"},
                {"id": f"g:{i}:{suffix}"},
                {"v": 1},
            )
            for i in range(_BULK_NODE_COUNT)
        ]
        store.apply_facts(facts, source="default", strict_endpoints=True)
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    gen = str(published["generation_id"])
    mcp = build_server(dsn=clean_runtime)
    tools = mcp._tool_manager._tools
    try:
        result = tools["aggregate"].fn(
            input={"kind": "nodes", "subtype": "bulk"},
            by=["attrs.gid"],
            metrics=["count"],
            generation_id=gen,
            limit=5,
        )
        assert result["truncated"] is True, result
        assert len(result["groups"]) == 5, result
        # Every returned group has count 1 (unique gid), and the result is honest
        # that there are more.
        assert all(g["count"] == 1 for g in result["groups"]), result["groups"]
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_attr_aggregate_explain_uses_index_scan(
    clean_runtime: str,
) -> None:
    """Verification point #8: the SQL GROUP BY path uses an INDEX SCAN on the
    snapshot's node-subtype index for a SELECTIVE subtype, not a Seq Scan. We
    EXPLAIN the exact GROUP BY query the fast aggregate path issues for the tiny
    ci_workflow subtype over the >10K-node snapshot."""
    fixture = _seed_large_graph(clean_runtime)
    gen = fixture["generation_id"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            # schema-interval-narrow-split: attrs resolve through the
            # version-store join, mirroring the production from-clause.
            cur.execute(
                "EXPLAIN SELECT (attrs -> 'conclusion') AS g0, count(*) AS n "
                "FROM okg.attribute_effective_view "
                "JOIN okg.attribute_versions "
                "USING (target_kind, target_id, attrs_hash) "
                "WHERE target_kind = 'node' AND generation_id = %s "
                "AND namespace = 'default' "
                "AND subtype = 'ci_workflow' "
                "GROUP BY 1",
                (gen,),
            )
            plan = "\n".join(r[0] for r in cur.fetchall())
    assert "Index Scan" in plan or "Index Only Scan" in plan, plan
    assert "Seq Scan on attribute_effective_view" not in plan, plan


def test_selective_attr_aggregate_over_large_graph_does_not_trip_cap(
    clean_runtime: str,
) -> None:
    fixture = _seed_large_graph(clean_runtime)
    gen = fixture["generation_id"]
    mcp = build_server(dsn=clean_runtime)
    tools = mcp._tool_manager._tools
    try:
        # WITH pushdown: subtype=ci_workflow is pushed to SQL, so only 3 rows are
        # scanned through the Python overlay -- well under the 10K cap -- even
        # though the graph has >10K nodes.
        result = _attr_aggregate(tools, gen=gen)
        groups = {g["attrs.conclusion"]: g["count"] for g in result["groups"]}
        # Effective attrs: wf:1 success, wf:2 failure, wf:3 OVERRIDDEN to failure.
        assert groups == {"success": 1, "failure": 2}, result["groups"]
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_unfiltered_scan_still_trips_cap_on_layer1_fallback(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Layer-1 contract (the chronos-scan + Python-overlay FALLBACK path): an
    unfiltered scan over the >10K graph trips the bounded-overlay cap, while a
    pushed-down subtype scopes the cap to the matched set and succeeds. We drop
    the Layer-2 indexed snapshot so the fallback path runs; with the snapshot
    present (the default), the cap no longer fires at all -- see
    `test_unfiltered_scan_completes_in_sql_with_layer2_view` below."""
    fixture = _seed_large_graph(clean_runtime)
    gen = fixture["generation_id"]
    # The production interval source refuses an incomplete stab loudly. This
    # test is specifically the legacy Layer-1 seam, so disable that source
    # after publishing and then remove the per-generation snapshot.
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    mcp = build_server(dsn=clean_runtime)
    try:
        attr_rows = mcp._okg_operator_attr_rows  # type: ignore[attr-defined]
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            # Force the Layer-1 fallback by removing both Layer-2 effective-view
            # representations for this generation, so _effective_view_ready() is
            # false regardless of the interval-view feature flag.
            conn.execute(
                "DELETE FROM okg.attribute_effective_view WHERE generation_id = %s",
                (gen,),
            )
            conn.execute(
                "DELETE FROM okg.attribute_effective_view_intervals "
                "WHERE branch_id = 'default'",
            )
            session = dataclasses.replace(
                open_session(conn, deployment_name="default"), generation_id=gen
            )
            with pytest.raises(SubstrateError, match="bounded"):
                attr_rows(conn, session, kind="nodes", limit=10_000, scan_predicate=None)
            # With the subtype pushed down, the same call succeeds (3 rows).
            # _operator_attr_rows returns (rows, scan_truncated).
            pushed, pushed_truncated = attr_rows(
                conn,
                session,
                kind="nodes",
                limit=10_000,
                scan_predicate={"op": "eq", "field": "subtype", "value": "ci_workflow"},
            )
            assert len(pushed) == 3
            assert pushed_truncated is False
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_unfiltered_scan_completes_in_sql_with_layer2_view(
    clean_runtime: str,
) -> None:
    """Layer-2 contract (scale-operator-attr-reads / req.effective-attrs-view):
    with the indexed effective-attrs snapshot present, an UNFILTERED scan over the
    >10K graph no longer trips the bounded-overlay cap -- it completes in SQL,
    reading effective attrs straight from the index. The OLD code capped this at
    10_001 rows (a SILENT undercount, the MUST-FIX #1 bug); the fixed fast path
    returns ALL matched rows (10_050 bulk + 3 ci_workflow = 10_053) and reports
    `scan_truncated=False` because the count is well under the 200K result-size
    ceiling. This is what lets a 470K subtype complete COMPLETELY where Layer 1
    capped out."""
    fixture = _seed_large_graph(clean_runtime)
    gen = fixture["generation_id"]
    mcp = build_server(dsn=clean_runtime)
    try:
        attr_rows = mcp._okg_operator_attr_rows  # type: ignore[attr-defined]
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = dataclasses.replace(
                open_session(conn, deployment_name="default"), generation_id=gen
            )
            # No cap error -- the SQL fast path serves the unfiltered scan and
            # returns EVERY row (NOT a 10_001 window). The result is NOT truncated
            # because it is far below the result-size ceiling.
            rows, truncated = attr_rows(
                conn, session, kind="nodes", limit=10_000, scan_predicate=None
            )
            assert len(rows) == _BULK_NODE_COUNT + 3, len(rows)
            assert truncated is False
            # Subtype pushdown still narrows to exactly the 3 ci_workflow rows.
            pushed, pushed_truncated = attr_rows(
                conn,
                session,
                kind="nodes",
                limit=10_000,
                scan_predicate={"op": "eq", "field": "subtype", "value": "ci_workflow"},
            )
            assert len(pushed) == 3
            assert pushed_truncated is False
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_fast_path_rows_match_layer1_fallback_rows(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """req.parity (Layer 2): for the SAME selective operator scan, the SQL fast
    path (snapshot present) and the Layer-1 chronos-scan + Python-overlay
    fallback (snapshot deleted) return BYTE-IDENTICAL effective attrs per node --
    same id set, same merged attrs (overlay override included). This pins that
    routing to the indexed surface introduces no semantic drift vs the prior
    path."""
    fixture = _seed_large_graph(clean_runtime)
    gen = fixture["generation_id"]
    mcp = build_server(dsn=clean_runtime)
    pred = {"op": "eq", "field": "subtype", "value": "ci_workflow"}
    try:
        attr_rows = mcp._okg_operator_attr_rows  # type: ignore[attr-defined]
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = dataclasses.replace(
                open_session(conn, deployment_name="default"), generation_id=gen
            )
            # Fast path (Layer-2 snapshot present).
            fast, _ = attr_rows(
                conn, session, kind="nodes", limit=10_000, scan_predicate=pred
            )
            # Force the Layer-1 fallback by deleting both effective-view
            # representations for this generation.
            conn.execute(
                "DELETE FROM okg.attribute_effective_view WHERE generation_id = %s",
                (gen,),
            )
            conn.execute(
                "DELETE FROM okg.attribute_effective_view_intervals "
                "WHERE branch_id = 'default'",
            )
            monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
            slow, _ = attr_rows(
                conn, session, kind="nodes", limit=10_000, scan_predicate=pred
            )

        def _norm(rows):
            return {
                r["node_id"]: dict((r.get("attrs") or {}))
                for r in rows
            }

        fast_n, slow_n = _norm(fast), _norm(slow)
        assert set(fast_n) == set(slow_n), (set(fast_n), set(slow_n))
        for nid in fast_n:
            assert fast_n[nid] == slow_n[nid], (nid, fast_n[nid], slow_n[nid])
        # wf:3's overlay override must appear identically on both paths.
        assert fast_n[fixture["overridden"]]["conclusion"] == "failure"
        assert slow_n[fixture["overridden"]]["conclusion"] == "failure"
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_attr_aggregate_parity_pushdown_matches_python_overlay(
    clean_runtime: str,
) -> None:
    """The pushdown path returns the SAME groups/counts as the prior
    Python-overlay path. We reproduce the prior path by reading the full
    (unfiltered, capped) scan and grouping in Python over a graph small enough
    to fit under the cap -- here the selective subtype IS the whole comparison
    set, so both paths must agree exactly, overlay included."""
    fixture = _seed_large_graph(clean_runtime)
    gen = fixture["generation_id"]
    mcp = build_server(dsn=clean_runtime)
    tools = mcp._tool_manager._tools
    try:
        pushdown = _attr_aggregate(tools, gen=gen)

        # Prior path reference: scan the ci_workflow subtype rows with the SAME
        # effective-attrs overlay (scan_predicate pushed only to make it fit the
        # cap; grouping is identical Python logic), then group by attrs.conclusion.
        attr_rows = mcp._okg_operator_attr_rows  # type: ignore[attr-defined]
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = dataclasses.replace(
                open_session(conn, deployment_name="default"), generation_id=gen
            )
            rows, _ = attr_rows(
                conn,
                session,
                kind="nodes",
                limit=10_000,
                scan_predicate={"op": "eq", "field": "subtype", "value": "ci_workflow"},
            )
        ref: dict[str, int] = {}
        for row in rows:
            key = (row.get("attrs") or {}).get("conclusion")
            ref[key] = ref.get(key, 0) + 1

        push_groups = {g["attrs.conclusion"]: g["count"] for g in pushdown["groups"]}
        assert push_groups == ref, (push_groups, ref)
        assert push_groups == {"success": 1, "failure": 2}
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_attr_aggregate_with_attr_filter_counts_effective_in_sql(
    clean_runtime: str,
) -> None:
    """MUST-FIX #1: an aggregate whose `filter` references attrs is counted in
    SQL over the EFFECTIVE snapshot attrs (overlay applied), so the residual
    predicate sees the overlaid value -- byte-identical to the per-row Python
    path. wf:3's raw conclusion is 'success' but the overlay makes it 'failure',
    so filtering attrs.conclusion=='failure' and grouping by attrs.conclusion
    must count wf:2 + wf:3 = 2."""
    fixture = _seed_large_graph(clean_runtime)
    gen = fixture["generation_id"]
    mcp = build_server(dsn=clean_runtime)
    tools = mcp._tool_manager._tools
    try:
        res = tools["aggregate"].fn(
            input={
                "kind": "nodes",
                "subtype": "ci_workflow",
                "filter": {
                    "op": "eq", "field": "attrs.conclusion", "value": "failure",
                },
            },
            by=["attrs.conclusion"],
            metrics=["count"],
            generation_id=gen,
        )
        groups = {g["attrs.conclusion"]: g["count"] for g in res["groups"]}
        # Only the 'failure' group survives the filter; it counts wf:2 (raw) and
        # wf:3 (overlaid success->failure) = 2.
        assert groups == {"failure": 2}, groups
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_attr_predicate_stays_in_python_when_overlay_changes_value(
    clean_runtime: str,
) -> None:
    """A predicate that itself touches attrs is NOT pushed to SQL (which sees
    only raw attrs); it stays in the Python post-filter over EFFECTIVE attrs.
    wf:3's raw conclusion is 'success' but the accepted overlay makes it
    'failure', so filtering attrs.conclusion == 'failure' MUST include wf:3
    (overlay) and exclude it from a 'success' filter."""
    fixture = _seed_large_graph(clean_runtime)
    gen = fixture["generation_id"]
    overridden = fixture["overridden"]
    mcp = build_server(dsn=clean_runtime)
    tools = mcp._tool_manager._tools
    try:
        failing = tools["filter"].fn(
            input={"kind": "nodes", "subtype": "ci_workflow"},
            predicate={"op": "eq", "field": "attrs.conclusion", "value": "failure"},
            select=["node_id"],
            generation_id=gen,
        )
        failing_ids = {item["node_id"] for item in failing["items"]}
        # wf:2 (raw failure) + wf:3 (overlay failure, raw success) -> 2 rows.
        assert overridden in failing_ids, failing_ids
        assert len(failing_ids) == 2, failing_ids

        succeeding = tools["filter"].fn(
            input={"kind": "nodes", "subtype": "ci_workflow"},
            predicate={"op": "eq", "field": "attrs.conclusion", "value": "success"},
            select=["node_id"],
            generation_id=gen,
        )
        succeeding_ids = {item["node_id"] for item in succeeding["items"]}
        # Only wf:1 -- wf:3 was overridden away from success.
        assert overridden not in succeeding_ids, succeeding_ids
        assert len(succeeding_ids) == 1, succeeding_ids
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
