"""Paged MCP reads must reach every visible row, and their counts must mean
what they are named.

Issue #1438 — semantic search fetched a raw prefix of `offset + limit + 1`
rows, filtered it, then applied `offset` a SECOND time to the survivors.
Hidden rows before the first visible one shifted the cursor twice, so page 1
under-filled while `next_offset` still advanced by `limit`; the rows in
between were served by no page at all. Alias search capped candidates to the
page size before hydration and computed `truncated` only from survivors, so
an under-filled page reported `next_offset=null` while later visible aliases
existed.

Issue #1440 — aggregate ordered only by count and filter only by the caller's
field, so ties had no identity tiebreak and OFFSET pages could repeat or skip;
`total_count` came from `run_sql`, which computes it AFTER the caller's OFFSET
and saturates it at `row_cap`, making a page-local number read as a set total.

What each test binds, stated plainly:

- The page-walk tests are the behavioral proof. They exhaust every page at a
  small limit and assert each visible row appears exactly once, in the same
  order a single wide read returns. Asserting page 1 looks right proves
  nothing about skips. At the audited base commit the semantic walk served
  NOTHING at limit=1, and the aggregate walk served one tied group twice and
  another not at all.
- `total_count` assertions are behavioral and deterministic: the pre-fix value
  was page-local and saturated, so it disagrees with the true set size.
- The generated ORDER BY assertions for #1440 are shape tests, kept
  alongside the walks (since #3201, over the effective-attribute SQL the
  ordered filter and set aggregate tools now read). A tie's observed order
  depends on the plan and the data, so a walk that happens to pass on some
  fixture is not evidence that a tiebreak exists; reading the emitted clause is. `test_alias_pages_are_
  stable_across_repeated_calls` is in the same category — it holds at the base
  commit too, and is a forward guarantee rather than a regression proof.
"""

from __future__ import annotations

import math
import uuid
from typing import Any, Iterator

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.embeddings import HashEmbedder  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.mcp import open_session  # noqa: E402
from okg.substrate.mcp.search import search  # noqa: E402
from okg.substrate.mcp import server as server_mod  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

QUERY = "query keyword pagination"


def _cosine_distance(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 1.0
    return 1.0 - dot / (na * nb)


def _publish(dsn: str) -> str:
    identity = bind_test_catalog_writer(dsn, "default")
    out = Publisher(dsn, "default", catalog_identity=identity).cycle()
    assert out["status"] == "published", out
    return str(out["generation_id"])


# ------------------------------------------------------------ #1438 semantic


@pytest.fixture
def hidden_before_visible(clean_runtime: str) -> dict:
    """Two filtered nodes rank AHEAD of three visible ones.

    The ordering is the bug: a fixture whose filtered rows trail the visible
    ones passes on broken code, because the double-shift only bites when the
    raw prefix is consumed by rows the page cannot serve.
    """
    embedder = HashEmbedder()
    qvec = embedder.embed(QUERY)
    suffix = uuid.uuid4().hex[:8]
    # Rank by measured distance, then assign tiers by rank, so "hidden first"
    # is a fact about this fixture rather than a hope about the embedder.
    texts = [f"{QUERY} variant {i} {suffix}" for i in range(12)]
    vectors = {text: embedder.embed(text) for text in texts}
    ranked = sorted(texts, key=lambda t: _cosine_distance(qvec, vectors[t]))
    hidden = ranked[:2]
    visible = ranked[2:7]
    node_of = {text: f"n:{suffix}:{i}" for i, text in enumerate(ranked)}

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_of[text],
                    "page",
                    {
                        "text": text,
                        "tier": "hidden" if text in hidden else "visible",
                    },
                    {"id": node_of[text]},
                    {"v": 1},
                )
                for text in hidden + visible
            ],
            source="default",
        )
    generation = _publish(clean_runtime)
    # Imported inside the fixture, matching test_mcp_read_surface_bounds: a
    # module-level import of another test module couples the two at COLLECTION
    # time, and collection differs between the local run and the hosted lanes.
    from tests.substrate.test_search import _write_embedding

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for text in hidden + visible:
            _write_embedding(conn, node_of[text], vectors[text], generation)
        # A fact the next cycle can publish, so the embeddings above are
        # captured into a checkpoint.
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    f"n:{suffix}:delta", "page",
                    {"text": "publish delta", "tier": "visible"},
                    {"id": f"n:{suffix}:delta"}, {"v": 1},
                )
            ],
            source="default",
        )
    _publish(clean_runtime)
    return {
        "dsn": clean_runtime,
        "embedder": embedder,
        "hidden_ids": [node_of[t] for t in hidden],
        "visible_ids": [node_of[t] for t in visible],
    }


def _walk_semantic(conn, session, fixture, *, limit: int) -> list[str]:
    served: list[str] = []
    offset = 0
    for _ in range(50):
        page = search(
            conn, session, QUERY, method="semantic",
            embedder=fixture["embedder"],
            filter={"attr_filters": {"tier": "visible"}},
            limit=limit, offset=offset,
        )
        served.extend(row["node_id"] for row in page["results"])
        if page["next_offset"] is None:
            assert page["truncated"] is False
            return served
        assert page["next_offset"] > offset, (
            "a truncated page must advance the cursor; "
            f"offset={offset} next_offset={page['next_offset']}"
        )
        offset = page["next_offset"]
    raise AssertionError("page walk did not terminate")


@pytest.mark.parametrize("limit", [1, 2, 3])
def test_semantic_page_walk_serves_every_visible_row_once(
    hidden_before_visible: dict, limit: int,
) -> None:
    """Exhaust the pages. Every visible row, exactly once, in rank order.

    Pre-fix at limit=1 this served NOTHING: two hidden rows ahead of the first
    visible one consumed the raw window on every page, and the cursor still
    advanced by `limit` until `truncated` went false.
    """
    fixture = hidden_before_visible
    with psycopg.connect(fixture["dsn"], autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        served = _walk_semantic(conn, session, fixture, limit=limit)
        whole = search(
            conn, session, QUERY, method="semantic",
            embedder=fixture["embedder"],
            filter={"attr_filters": {"tier": "visible"}},
            limit=100, offset=0,
        )

    assert len(served) == len(set(served)), f"a row was served twice: {served}"
    assert not set(served) & set(fixture["hidden_ids"])
    expected = [row["node_id"] for row in whole["results"]]
    # The paged walk and one wide read must agree on membership AND order.
    assert served == expected
    assert set(fixture["visible_ids"]) <= set(served)


def test_semantic_total_count_states_its_basis(
    hidden_before_visible: dict,
) -> None:
    """`total_count` is a count of verified visible matches, and says so."""
    fixture = hidden_before_visible
    with psycopg.connect(fixture["dsn"], autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        whole = search(
            conn, session, QUERY, method="semantic",
            embedder=fixture["embedder"],
            filter={"attr_filters": {"tier": "visible"}},
            limit=100, offset=0,
        )
        first = search(
            conn, session, QUERY, method="semantic",
            embedder=fixture["embedder"],
            filter={"attr_filters": {"tier": "visible"}},
            limit=1, offset=0,
        )

    assert whole["total_count_basis"] == "exact"
    assert whole["total_count"] == len(whole["results"])
    assert whole["truncated"] is False
    assert whole["next_offset"] is None
    assert whole["rank_capped"] is False

    # A truncated page's total_count is never the page size.
    assert first["truncated"] is True
    assert first["total_count"] == whole["total_count"]
    assert first["next_offset"] == 1


# --------------------------------------------------------------- #1438 alias


@pytest.fixture
def fuzzy_aliases(clean_runtime: str) -> dict:
    """Trigram-similar aliases whose TOP-ranked canonicals are filtered out.

    `okg.aliases` is keyed by (alias, catalog_version_id), so the exact tier
    can only ever return one row; the multi-candidate page that #1438 is about
    is the fuzzy cascade's trigram tier.
    """
    suffix = uuid.uuid4().hex[:8]
    needle = f"pagination-alias-{suffix}"
    # Same subtype vocabulary; the caller's `subtype` filter is what hides the
    # first candidates, exactly as a policy deny or an effective-attr residual
    # would.
    nodes = [(f"node:{suffix}:{i}", "note" if i < 3 else "page") for i in range(8)]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        catalog_version_id = seed_catalog(conn, subtypes=["page", "note"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_id, subtype, {"text": node_id},
                    {"id": node_id}, {"v": 1},
                )
                for node_id, subtype in nodes
            ],
            source="default",
        )
    _publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT catalog_version_id FROM okg.catalog_versions "
                "WHERE status = 'applied' ORDER BY catalog_version_id DESC "
                "LIMIT 1"
            )
            catalog_version_id = int(cur.fetchone()[0])
            for index, (node_id, _subtype) in enumerate(nodes):
                cur.execute(
                    "INSERT INTO okg.aliases "
                    "  (alias, canonical, scope, catalog_version_id) "
                    "VALUES (%s, %s, NULL, %s)",
                    (f"{needle}-{index}", node_id, catalog_version_id),
                )
    return {
        "dsn": clean_runtime,
        "needle": needle,
        "filtered_ids": [n for n, s in nodes if s == "note"],
        "visible_ids": [n for n, s in nodes if s == "page"],
    }


def test_alias_page_walk_never_falsely_reports_completion(
    fuzzy_aliases: dict,
) -> None:
    """Exhaust the pages; `next_offset=null` must be a true claim.

    Pre-fix the candidate fetch was `offset + limit + 1`, so at limit=1 the
    first page read two candidates, both of them filtered, computed
    `truncated` from the zero survivors, and returned `next_offset=null` —
    reporting completion over five visible aliases it never fetched.
    """
    fixture = fuzzy_aliases
    served: list[str] = []
    offset = 0
    with psycopg.connect(fixture["dsn"], autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        for _ in range(50):
            page = search(
                conn, session, fixture["needle"], method="alias",
                fuzzy=True, filter={"subtype": "page"},
                limit=1, offset=offset,
            )
            served.extend(row["canonical"] for row in page["results"])
            if page["next_offset"] is None:
                assert page["truncated"] is False
                break
            assert page["next_offset"] > offset
            offset = page["next_offset"]
        else:
            raise AssertionError("alias page walk did not terminate")
        whole = search(
            conn, session, fixture["needle"], method="alias",
            fuzzy=True, filter={"subtype": "page"},
            limit=50, offset=0,
        )

    assert len(served) == len(set(served)), f"an alias was served twice: {served}"
    assert not set(served) & set(fixture["filtered_ids"])
    assert set(served) == set(fixture["visible_ids"])
    assert served == [row["canonical"] for row in whole["results"]]
    assert whole["total_count"] == len(fixture["visible_ids"])
    assert whole["total_count_basis"] == "exact"
    assert whole["candidates_capped"] is False


def test_alias_pages_are_stable_across_repeated_calls(
    fuzzy_aliases: dict,
) -> None:
    """The same offset names the same row every time (total candidate order)."""
    fixture = fuzzy_aliases
    with psycopg.connect(fixture["dsn"], autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        pages = [
            [
                row["canonical"]
                for row in search(
                    conn, session, fixture["needle"], method="alias",
                    fuzzy=True, filter={"subtype": "page"},
                    limit=2, offset=offset,
                )["results"]
            ]
            for _ in range(3)
            for offset in (0, 2, 4)
        ]
    assert pages[0:3] == pages[3:6] == pages[6:9]


# ----------------------------------------------------------------- #1440


@pytest.fixture
def tied_groups(clean_runtime: str) -> dict:
    """Six statuses, two nodes each: every aggregate group ties on count."""
    suffix = uuid.uuid4().hex[:8]
    statuses = [f"s{i}-{suffix}" for i in range(6)]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    f"n:{suffix}:{status}:{copy}", "page",
                    {"text": "tied", "status": status},
                    {"id": f"n:{suffix}:{status}:{copy}"}, {"v": 1},
                )
                for status in statuses
                for copy in range(2)
            ],
            source="default",
        )
    _publish(clean_runtime)
    return {"dsn": clean_runtime, "statuses": statuses, "suffix": suffix}


@pytest.fixture
def tied_tools(tied_groups: dict) -> Iterator[dict[str, Any]]:
    """The real operator tools over `tied_groups` (#3201: ordered filter and
    set aggregate are tool paths on the effective-attribute relation; the
    run_sql helpers these tests used to call are gone)."""
    mcp = server_mod.build_server(dsn=tied_groups["dsn"])
    try:
        yield {
            "tools": {name: tool.fn for name, tool in mcp._tool_manager._tools.items()},
            "mcp": mcp,
            **tied_groups,
        }
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def _aggregate(tools: dict, *, limit: int, offset: int) -> dict:
    return tools["aggregate"](
        input={"kind": "nodes", "subtype": "page"},
        by=["attrs.status"], metrics=["count"],
        limit=limit, offset=offset,
    )


def test_aggregate_page_walk_serves_every_tied_group_once(
    tied_tools: dict,
) -> None:
    fixture = tied_tools
    served: list[Any] = []
    offset = 0
    for _ in range(50):
        page = _aggregate(fixture["tools"], limit=1, offset=offset)
        served.extend(group["attrs.status"] for group in page["groups"])
        if page["next_offset"] is None:
            assert page["truncated"] is False
            break
        assert page["next_offset"] > offset
        offset = page["next_offset"]
    else:
        raise AssertionError("aggregate page walk did not terminate")

    tied = [status for status in served if status in fixture["statuses"]]
    assert len(tied) == len(set(tied)), f"a group was served twice: {served}"
    assert set(fixture["statuses"]) <= set(served)


def test_aggregate_total_count_is_the_group_count_not_the_page(
    tied_tools: dict,
) -> None:
    """The binding deterministic failure for #1440's count half.

    Pre-fix `total_count` came from `run_sql`, which counts rows AFTER the
    caller's OFFSET and saturates at `row_cap` — so a limit=1 page reported
    `total_count == 1` for a six-group set.
    """
    page = _aggregate(tied_tools["tools"], limit=1, offset=0)
    whole = _aggregate(tied_tools["tools"], limit=100, offset=0)

    assert whole["truncated"] is False
    assert whole["total_count"] == len(whole["groups"])
    assert whole["total_count_basis"] == "exact"

    assert page["truncated"] is True
    assert len(page["groups"]) == 1
    assert page["total_count"] == whole["total_count"]
    assert page["total_count"] > 1
    assert page["total_count_basis"] == "exact"


def _filter(tools: dict, *, limit: int, offset: int, order_by: str = "subtype") -> dict:
    return tools["filter"](
        input={"kind": "nodes", "subtype": "page"},
        predicate=None, order_by=order_by,
        limit=limit, offset=offset, select=["node_id"],
    )


def test_filter_page_walk_and_total_count(tied_tools: dict) -> None:
    """Every row ties on `subtype`, so only the identity tiebreak orders the
    walk; a missing tiebreak shows up as a repeated or skipped row."""
    tools = tied_tools["tools"]
    served: list[str] = []
    offset = 0
    for _ in range(80):
        page = _filter(tools, limit=2, offset=offset)
        served.extend(item["node_id"] for item in page["items"])
        if page["next_offset"] is None:
            assert page["truncated"] is False
            break
        assert page["next_offset"] > offset
        offset = page["next_offset"]
    else:
        raise AssertionError("filter page walk did not terminate")

    whole = _filter(tools, limit=200, offset=0)
    first = _filter(tools, limit=1, offset=0)

    assert len(served) == len(set(served)), f"a row was served twice: {served}"
    assert served == [item["node_id"] for item in whole["items"]]
    assert whole["total_count"] == len(whole["items"])
    assert whole["total_count_basis"] == "exact"

    assert first["truncated"] is True
    assert len(first["items"]) == 1
    # #3201: a cut first page is counted by a bounded COUNT over its own SQL.
    assert first["total_count"] == whole["total_count"]
    assert first["total_count_basis"] == "exact"
    assert first["total_count"] > 1


def test_run_sql_reports_returned_count_and_no_total(tied_groups: dict) -> None:
    """`run_sql` cannot know the caller's set size, so it no longer claims to.

    The value it used to publish as `total_count` was computed after the
    caller's own OFFSET and clamped to `row_cap` — a page-local number under a
    set-sized name.
    """
    from okg.substrate.mcp.run_sql import run_sql

    fixture = tied_groups
    with psycopg.connect(fixture["dsn"], autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        out = run_sql(
            conn, session,
            "SELECT node_id FROM okg.v_nodes ORDER BY node_id OFFSET 2",
            row_cap=3,
        )
    assert "total_count" not in out
    assert out["returned_count"] == len(out["rows"]) == 3
    assert out["truncated"] is True


def test_operator_order_clauses_are_total(tied_tools: dict) -> None:
    """The deterministic proof that the tiebreak exists.

    A tie's observed order is stable under a fixed plan and fixed data, so a
    page walk cannot be relied on to expose a MISSING tiebreak. This reads the
    generated ORDER BY instead: the caller's field refines the order, the
    identity always ends it, and expand ends on its edge id. (A set
    aggregate's groups are ordered by count, then by every `by` key, in
    `_effective_view_aggregate_groups`; the walk above serves each once.)
    """
    order_sql = tied_tools["mcp"]._okg_effective_view_order_sql
    declared: dict[str, str] = {}
    assert order_sql(
        kind="nodes", order_by="subtype desc", params={}, declared=declared,
        relation="effective_page",
    ) == (
        " ORDER BY effective_page.subtype DESC NULLS LAST, "
        "effective_page.target_id ASC"
    )
    # An edge's id is its bigint graph edge_id: it sorts as a number.
    assert order_sql(
        kind="edges", order_by="edge_type", params={}, declared=declared,
        relation="effective_page",
    ).endswith(", (effective_page.target_id)::bigint ASC")
    assert order_sql(
        kind="nodes", order_by=None, params={}, declared=declared,
        relation="effective_page",
    ) == " ORDER BY effective_page.target_id ASC"
    assert order_sql(
        kind="nodes", order_by="subtype", params={}, declared=declared,
        relation="expand_node", tiebreak="expand_edge.target_id ASC",
    ) == (
        " ORDER BY expand_node.subtype ASC NULLS LAST, expand_edge.target_id ASC"
    )
    with pytest.raises(ValueError, match="order_by"):
        order_sql(
            kind="nodes", order_by="subtype descending", params={},
            declared=declared,
        )
