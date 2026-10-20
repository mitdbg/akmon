"""expand's pagination contract (PACT fix-expand-pagination-contract).

Two defects, both silent, both filed against the `cooper` deployment:

  #903  Row schema changed between page 1 and page 2, because `offset=0` is
        falsy and routed the two pages to different implementations. An agent
        that keyed on `src` found it absent from page 2 onward.
  #904  `truncated=true` came back with no cursor and a `total_count` equal to
        the page size, so a caller could neither continue the result set nor
        learn how much of it it was missing.

The real-Postgres tests here are the ones that matter: they publish a node
whose degree exceeds the page size and walk `offset` to exhaustion, asserting
the returned edge ids are the ground-truth set exactly once each. A test that
monkeypatches the SQL away stays green while the emitted SQL is wrong.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import expand_neighbors, open_session
from okg.substrate.mcp.server import (
    EXPAND_MAX_OFFSET,
    add_pagination_hint,
    validate_expand_offset,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

# NOT a file-level postgres mark: the offset-validation tests below are pure
# and belong in the fast lane. Marking the module would hide them from
# per-PR CI entirely.
_DEGREE = 12  # comfortably more than the page sizes used below


def _publish_hub(conn: psycopg.Connection, sfx: str) -> tuple[str, list[str]]:
    """Publish one hub with `_DEGREE` in-edges. Returns (hub_id, peer_ids)."""
    seed_catalog(
        conn,
        subtypes=[SubtypeSpec("page")],
        narrowings=[("page", "cites", "page")],
    )
    hub = f"n:hub-{sfx}"
    peers = [f"n:peer-{sfx}-{i:03d}" for i in range(_DEGREE)]
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(hub, "page", {"text": "hub"}, {"id": hub}, {"v": 1}),
        ]
        + [NodeFact(p, "page", {"text": p}, {"id": p}, {"v": 1}) for p in peers]
        + [
            EdgeFact(
                src=p,
                dst=hub,
                edge_type="cites",
                attrs={},
                source_record_id={"src": p, "dst": hub},
                source_revision={"v": 1},
            )
            for p in peers
        ]
    )
    return hub, peers


def _walk(conn, session, hub: str, *, limit: int, **kwargs) -> list[dict]:
    """Follow `next_offset` to exhaustion, collecting every page's rows."""
    pages, offset, guard = [], 0, 0
    while True:
        guard += 1
        assert guard < 50, "cursor did not terminate"
        page = expand_neighbors(
            conn,
            session,
            hub,
            direction="in",
            limit=limit,
            offset=offset,
            **kwargs,
        )
        pages.append(page)
        if not page["truncated"]:
            assert page["next_offset"] is None
            return pages
        assert page["next_offset"] > offset, "cursor must advance"
        offset = page["next_offset"]


def _expand_tool(dsn: str):
    """The real `expand` tool. Since #3201 the filtered/ordered one-hop path
    is a closure over the effective-attribute relation, reachable only
    through the tool."""
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=dsn)
    return mcp, mcp._tool_manager._tools["expand"].fn


#: Issue #3666: a 1-hop row is the linked record unless `select` names only
#: the link's own fields. This list asks for the whole link row, as the
#: default was (`link.attrs` is the link row's `attrs`).
LINK_ROWS = [
    "edge_id", "src", "dst", "edge_type", "direction", "link.attrs", "read_provenance",
]


def _walk_filtered(expand, hub: str, *, limit: int, **kwargs):
    """The same walk, through the filtered/ordered implementation and its
    real SQL: the LEFT JOIN, the edge-id tiebreak, OFFSET and the bounded
    count. `order_by` alone routes there without dropping any edge."""
    kwargs.setdefault("select", list(LINK_ROWS))
    pages, offset, guard = [], 0, 0
    while True:
        guard += 1
        assert guard < 50, "cursor did not terminate"
        page = expand(
            input={"node_id": hub},
            direction="in",
            order_by="node_id",
            limit=limit,
            offset=offset,
            **kwargs,
        )
        pages.append(page)
        if not page["truncated"]:
            assert page["next_offset"] is None
            return pages
        assert page["next_offset"] > offset, "cursor must advance"
        offset = page["next_offset"]


# --------------------------------------------------------------------------- #
# req.expand.stable-row-schema — issue #903
# --------------------------------------------------------------------------- #


@pytest.mark.postgres
def test_row_keys_identical_across_pages(clean_runtime: str) -> None:
    """Page 2 has the same keys as page 1. This is the #903 regression."""
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, _ = _publish_hub(conn, sfx)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        pages = _walk(conn, session, hub, limit=5)

    assert len(pages) > 1, "fixture must truncate for this test to mean anything"
    key_sets = {frozenset(row) for page in pages for row in page["neighbors"]}
    assert len(key_sets) == 1, f"row schema changed between pages: {key_sets}"
    keys = next(iter(key_sets))
    # The fields an agent walking a neighborhood keys on. Their absence from
    # page 2 was the reported defect.
    assert {"src", "dst", "edge_id", "edge_type", "direction"} <= keys


@pytest.mark.postgres
def test_select_honored_on_every_page(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, _ = _publish_hub(conn, sfx)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        pages = _walk(conn, session, hub, limit=5, select=["src", "edge_type"])

    assert len(pages) > 1
    for page in pages:
        for row in page["neighbors"]:
            # select plus the always-on keys, and notably no `attrs`.
            assert set(row) == {
                "src",
                "dst",
                "edge_id",
                "edge_type",
                "direction",
                # The far end's subtype and title, on every 1-hop row (#2675).
                "neighbor",
            }


@pytest.mark.postgres
def test_offset_does_not_route_to_the_filtered_implementation(
    clean_runtime: str,
) -> None:
    """`direction="both"` is the crisp behavioral proof that offset no longer
    picks the implementation: the filtered path refuses anything but out/in,
    so under the old router `offset>0` turned a working call into a refusal.
    Now every page of one neighborhood comes from `expand_neighbors`."""
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, peers = _publish_hub(conn, sfx)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        first = expand_neighbors(
            conn, session, hub, direction="both", limit=5, offset=0
        )
        second = expand_neighbors(
            conn, session, hub, direction="both", limit=5, offset=5
        )

    assert second["neighbors"], "offset=5 returned nothing on a hub of degree 12"
    assert {frozenset(r) for r in first["neighbors"]} == {
        frozenset(r) for r in second["neighbors"]
    }
    assert first["total_count"] == second["total_count"] == len(peers)
    ids = {r["edge_id"] for r in first["neighbors"]} & {
        r["edge_id"] for r in second["neighbors"]
    }
    assert not ids, f"pages 1 and 2 overlap: {ids}"


def test_total_count_basis_under_enforce(monkeypatch) -> None:
    """Under Nomos `enforce` the reported count must never be the unfiltered
    cardinality: within budget it counts only rows that passed the per-row
    filter, and above budget it is omitted rather than leaked."""
    from okg.substrate.mcp import tools

    class _FakeGraph:
        def __init__(self, total: int) -> None:
            self._total = total

        def query(self, sql: str, params: dict):
            if "count(*)" in sql:
                return [{"c": self._total}]
            return [{"edge_id": f"e{i}"} for i in range(self._total)]

    session = SimpleNamespace(
        deployment_name="d", generation_id=1, branch_name="default"
    )
    monkeypatch.setattr(
        tools, "deployment_nomos_enforcement_mode", lambda _name: "enforce"
    )
    # Half the rows are hidden from this reader.
    monkeypatch.setattr(
        tools,
        "filter_edge_rows_for_session",
        lambda conn, sess, rows: list(rows)[: len(list(rows)) // 2],
    )

    monkeypatch.setattr(
        tools, "_chronos_checkout", lambda conn, sess: _FakeGraph(10)
    )
    count, basis = tools._neighbors_match_count(
        None, session, "n:hub", edge_type=None, direction="in"
    )
    assert basis == "exact_visible"
    assert count == 5, "enforce must report the visible count, not the raw 10"

    over = tools._NEIGHBORS_EXACT_COUNT_BUDGET + 1
    monkeypatch.setattr(
        tools, "_chronos_checkout", lambda conn, sess: _FakeGraph(over)
    )
    count, basis = tools._neighbors_match_count(
        None, session, "n:hub", edge_type=None, direction="in"
    )
    assert count is None
    assert basis == "omitted_over_budget_under_enforce"


@pytest.mark.postgres
def test_filtered_path_matches_the_unfiltered_shape_and_row_set(
    clean_runtime: str,
) -> None:
    """The two implementations must agree — same keys, same edges, same
    values, walked through the filtered path's real SQL (#3201: the
    effective-attribute relation, not run_sql)."""
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, peers = _publish_hub(conn, sfx)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        plain = expand_neighbors(
            conn, session, hub, direction="in", limit=len(peers) * 2
        )
    mcp, expand = _expand_tool(clean_runtime)
    try:
        filtered_pages = _walk_filtered(expand, hub, limit=5)
        dest_filtered = expand(
            input={"node_id": hub},
            direction="in",
            filter={"op": "eq", "field": "subtype", "value": "page"},
            order_by="node_id",
            limit=len(peers) * 2,
            select=list(LINK_ROWS),
        )
    finally:
        mcp._okg_pool.close()

    filtered_rows = [r for page in filtered_pages for r in page["neighbors"]]
    # Same keys as the unfiltered path — the #903 guarantee, across paths.
    assert {frozenset(r) for r in plain["neighbors"]} == {
        frozenset(r) for r in filtered_rows
    }
    # Same edges, each exactly once, walked through the real OFFSET + tiebreak.
    plain_ids = sorted(r["edge_id"] for r in plain["neighbors"])
    walked_ids = sorted(r["edge_id"] for r in filtered_rows)
    assert walked_ids == plain_ids
    assert len(set(walked_ids)) == len(walked_ids)
    assert filtered_pages[-1]["total_count"] == len(peers)
    # And the same values, not merely the same field names.
    by_id = {r["edge_id"]: r for r in filtered_rows}
    for row in plain["neighbors"]:
        assert by_id[row["edge_id"]]["attrs"] == row["attrs"]
        assert by_id[row["edge_id"]]["src"] == row["src"]
        assert by_id[row["edge_id"]]["direction"] == row["direction"]
    assert sorted(r["edge_id"] for r in dest_filtered["neighbors"]) == plain_ids


@pytest.mark.postgres
def test_expand_tool_routes_every_page_to_one_implementation(
    clean_runtime: str,
) -> None:
    """Routing through the real `expand` tool, not the helpers underneath it.
    Page 1 and page 2 must come from the same implementation — the #903
    defect was that `offset=0` being falsy sent them to different ones."""
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, _ = _publish_hub(conn, sfx)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    called: list[str] = []
    import okg.substrate.mcp.server as _server

    # server.py binds expand_neighbors at import as `_expand_neighbors`; the
    # filtered path (#3201) is a closure that never calls it.
    real_plain = _server._expand_neighbors

    def spy_plain(*a, **kw):
        called.append("plain")
        return real_plain(*a, **kw)

    _server._expand_neighbors = spy_plain
    try:
        from okg.substrate.mcp.server import build_server

        mcp = build_server(dsn=clean_runtime, auto_advance="always")
        try:
            expand = mcp._tool_manager._tools["expand"].fn
            page1 = expand(
                input={"node_id": hub}, direction="in", limit=5, offset=0,
                select=list(LINK_ROWS),
            )
            page2 = expand(
                input={"node_id": hub}, direction="in", limit=5, offset=5,
                select=list(LINK_ROWS),
            )
        finally:
            mcp._okg_pool.close()
    finally:
        _server._expand_neighbors = real_plain

    assert called == ["plain", "plain"], (
        f"offset changed the implementation: {called}"
    )
    assert {frozenset(r) for r in page1["neighbors"]} == {
        frozenset(r) for r in page2["neighbors"]
    }
    assert page1["pagination_hint"]
    assert page1["next_offset"] == 5


def test_complete_page_under_enforce_never_reports_raw_cardinality(
    monkeypatch,
) -> None:
    """`offset` counts RAW positions and a page counts VISIBLE rows, so
    `offset + len(page)` on a final page under enforce reports the pre-filter
    cardinality — the exact number the enforce branch exists to withhold, and
    a number that disagrees with every earlier page of the same walk."""
    from okg.substrate.mcp import tools

    raw = [
        {
            "edge_id": f"e{i}", "src": f"n:p{i}", "dst": "n:hub",
            "edge_type": "cites", "attrs": {}, "source": "fixture",
        }
        for i in range(6)
    ]
    hidden = {"e0", "e3"}

    monkeypatch.setattr(
        tools, "deployment_nomos_enforcement_mode", lambda _n: "enforce"
    )
    monkeypatch.setattr(
        tools,
        "filter_edge_rows_for_session",
        lambda conn, sess, rows: [
            r for r in rows if r["edge_id"] not in hidden
        ],
    )
    class _Graph:
        """The probe path: a raw COUNT to size the budget, then the matching
        rows, which the per-row filter above reduces to the visible set."""

        def query(self, sql, params):
            if "count(*)" in sql:
                return [{"c": len(raw)}]
            return list(raw)

    monkeypatch.setattr(tools, "_chronos_checkout", lambda conn, sess: _Graph())
    monkeypatch.setattr(
        tools,
        "execute_list_neighbors_chronos",
        lambda graph, *, node_id, edge_type, direction, limit, offset: raw[
            offset: offset + limit
        ],
    )
    monkeypatch.setattr(
        tools, "apply_effective_attrs_to_rows", lambda conn, rows, **kw: rows
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.provenance.serialize_edge_read_rows",
        lambda conn, rows, **kw: rows,
    )

    session = SimpleNamespace(
        deployment_name="d", generation_id=1, branch_name="default"
    )
    # The last page of the walk: offset past most of the raw rows, nothing
    # truncated. There are 4 VISIBLE edges out of 6 raw.
    page = expand_neighbors(
        None, session, "n:hub", direction="in", limit=2, offset=6
    )
    assert page["truncated"] is False
    assert page["total_count"] == 4, (
        "a complete page under enforce must report the visible count, not "
        f"the raw cardinality; got {page['total_count']}"
    )
    assert page["total_count_basis"] == "exact_visible"


@pytest.mark.parametrize("bad_limit", [0, -1])
def test_non_positive_limit_refuses(bad_limit: int) -> None:
    """limit<1 made `truncated` true with a cursor that could not advance —
    the follow-next_offset loop would never terminate."""
    with pytest.raises(SubstrateError, match="limit must be >= 1"):
        expand_neighbors(
            None, SimpleNamespace(deployment_name="d", generation_id=1),
            "n:hub", limit=bad_limit,
        )


@pytest.mark.parametrize("offset", [-1, -50])
def test_negative_offset_refuses(offset: int) -> None:
    with pytest.raises(ValueError, match="offset must be >= 0"):
        validate_expand_offset(offset)


def test_deep_offset_refuses_rather_than_clamping() -> None:
    """A clamped cursor would serve page 1 while the caller believed it had
    paged — a silent fallback. Refuse loudly instead."""
    with pytest.raises(ValueError, match="offset must be <="):
        validate_expand_offset(EXPAND_MAX_OFFSET + 1)
    assert validate_expand_offset(EXPAND_MAX_OFFSET) == EXPAND_MAX_OFFSET
    assert validate_expand_offset(None) == 0


# --------------------------------------------------------------------------- #
# req.expand.honest-total-and-cursor — issue #904
# --------------------------------------------------------------------------- #


@pytest.mark.postgres
def test_total_count_is_true_match_count(clean_runtime: str) -> None:
    """`total_count` is the match count at every page size — not the page
    size, which is only ever the true total when you did not need it."""
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, peers = _publish_hub(conn, sfx)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        for limit in (1, 2, 5, len(peers), len(peers) * 2):
            page = expand_neighbors(
                conn,
                session,
                hub,
                direction="in",
                limit=limit,
            )
            assert page["total_count"] == len(peers), (
                f"limit={limit} reported total_count={page['total_count']}"
            )
            assert page["returned_count"] == min(limit, len(peers))
            assert page["total_count_basis"] == "exact"


@pytest.mark.postgres
def test_truncated_payload_advertises_cursor(clean_runtime: str) -> None:
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, _ = _publish_hub(conn, sfx)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        page = expand_neighbors(
            conn,
            session,
            hub,
            direction="in",
            limit=5,
        )
    assert page["truncated"] is True
    assert page["offset"] == 0 and page["limit"] == 5
    assert page["next_offset"] == 5
    # add_pagination_hint could never fire on expand before, because the
    # envelope echoed neither offset nor limit.
    stamped = add_pagination_hint(dict(page), "expand")
    assert stamped["pagination"]["next_offset"] == 5
    assert "offset=5" in stamped["pagination_hint"]


@pytest.mark.postgres
def test_cursor_walk_yields_every_edge_exactly_once(clean_runtime: str) -> None:
    """The promise a cursor makes. Exercised across page sizes because the
    off-by-one this guards against only appears at particular boundaries."""
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, peers = _publish_hub(conn, sfx)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        for limit in (1, 3, 5, 7, len(peers) - 1, len(peers)):
            pages = _walk(conn, session, hub, limit=limit)
            seen = [row["edge_id"] for page in pages for row in page["neighbors"]]
            assert len(seen) == len(set(seen)), f"limit={limit} served an edge twice"
            assert len(seen) == len(peers), (
                f"limit={limit} walked {len(seen)} of {len(peers)} edges"
            )
            assert sorted(
                row["src"] for page in pages for row in page["neighbors"]
            ) == sorted(peers)


@pytest.mark.postgres
def test_offset_past_the_end_is_an_empty_complete_page(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, peers = _publish_hub(conn, sfx)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        page = expand_neighbors(
            conn,
            session,
            hub,
            direction="in",
            limit=5,
            offset=len(peers) + 10,
        )
    assert page["neighbors"] == []
    assert page["truncated"] is False
    assert page["next_offset"] is None


# --------------------------------------------------------------------------- #
# expand destination filters on attrs — issue #3387
# --------------------------------------------------------------------------- #


def _publish_fan(conn: psycopg.Connection, sfx: str) -> tuple[str, dict[str, str]]:
    """Publish one hub with out-edges to three neighbours whose attrs differ.

    Both joined relations (the edge and its far node) carry `attrs`, which is
    what made an unqualified `attrs` reference ambiguous (#3387). The edges
    carry attrs too, so a filter that read the edge's attrs by mistake would
    pick a different neighbour.
    """
    seed_catalog(
        conn,
        subtypes=[SubtypeSpec("page")],
        narrowings=[("page", "cites", "page")],
    )
    hub = f"n:fan-{sfx}"
    peers = {
        "open": f"n:fan-{sfx}-open",
        "closed": f"n:fan-{sfx}-closed",
        "bare": f"n:fan-{sfx}-bare",
    }
    peer_attrs = {
        "open": {"text": "open", "status": "open", "owner": "alice"},
        "closed": {"text": "closed", "status": "closed"},
        "bare": {"text": "bare"},
    }
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [NodeFact(hub, "page", {"text": "hub"}, {"id": hub}, {"v": 1})]
        + [
            NodeFact(peers[k], "page", peer_attrs[k], {"id": peers[k]}, {"v": 1})
            for k in peers
        ]
        + [
            EdgeFact(
                src=hub,
                dst=peers[k],
                edge_type="cites",
                # The edge's own attrs name the OTHER status, so reading the
                # wrong relation's attrs selects the wrong neighbour.
                attrs={"status": "closed" if k == "open" else "open", "owner": "edge"},
                source_record_id={"src": hub, "dst": peers[k]},
                source_revision={"v": 1},
            )
            for k in peers
        ]
    )
    return hub, peers


_ATTRS_FILTER_CASES = [
    ("eq", {"op": "eq", "field": "attrs.status", "value": "open"}, {"open"}),
    ("in", {"op": "in", "field": "attrs.status", "values": ["closed"]}, {"closed"}),
    ("exists", {"op": "exists", "field": "attrs.owner"}, {"open"}),
    (
        "contains",
        {"op": "contains", "field": "attrs", "value": {"status": "closed"}},
        {"closed"},
    ),
    (
        "not_exists",
        {"op": "not", "clause": {"op": "exists", "field": "attrs.status"}},
        {"bare"},
    ),
    ("subtype_control", {"op": "eq", "field": "subtype", "value": "page"},
     {"open", "closed", "bare"}),
]


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("predicate", "keys"),
    [(predicate, keys) for _name, predicate, keys in _ATTRS_FILTER_CASES],
    ids=[name for name, _predicate, _keys in _ATTRS_FILTER_CASES],
)
def test_expand_attrs_filter_returns_the_matching_neighbours(
    clean_runtime: str, predicate: dict, keys: set[str]
) -> None:
    """#3387: a one-hop `expand` filter on `attrs.<key>` (eq, in, exists) or
    on `attrs` (contains) failed with 'column reference "attrs" is
    ambiguous'. Each must return exactly the edges whose far node matches,
    with at least one neighbour excluded; a `subtype` filter is the control.
    """
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, peers = _publish_fan(conn, sfx)
    Publisher(
        clean_runtime,
        "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    mcp, expand = _expand_tool(clean_runtime)
    try:
        page = expand(
            input={"node_id": hub}, direction="out", filter=predicate, limit=10,
            select=list(LINK_ROWS),
        )
    finally:
        mcp._okg_pool.close()

    assert all(r["src"] == hub for r in page["neighbors"])
    assert {r["dst"] for r in page["neighbors"]} == {peers[k] for k in keys}
    assert page["truncated"] is False
