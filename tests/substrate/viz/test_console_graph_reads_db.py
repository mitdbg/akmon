"""The console Graph reads against a real published graph, through the real routes.

PACT console-graph-views. Each test publishes a fixture graph with the real
publisher (so both maintained rollups exist) and reads it through
``register_operator_record_routes``, which registers the four Graph routes
beside the record routes:

* ``console/graph/types`` equals the count rollup and the ontology rollup;
* ``console/graph/record`` counts a hub over 5,000 neighbours exactly, reaches
  every relationship and every neighbour type however their ids interleave,
  counts distinct neighbour records rather than links, refuses an absent
  record, and under an enforced policy hides and does not count what the
  reader may not see, deciding every counted row on its own;
* ``console/graph/source-links`` counts relationships by their ends' sources
  and answers a second read from its cache;
* ``console/graph/ontology`` lists declared types, attributes and rules beside
  the version's counts and a labelled fill sample.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator

import psycopg
import pytest

pytest.importorskip("fastapi")
from fastapi import APIRouter, FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from okg.substrate.console_views import graph_record, graph_views  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.mcp.session import open_session  # noqa: E402
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.viz.record_endpoints import register_operator_record_routes  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate._db_helpers import seed_catalog_version  # noqa: E402

pytestmark = pytest.mark.postgres

DEPLOYMENT = "default"
SUBJECT = "subject:operator"
RESTRICTED = {"classification": "restricted"}
HUB_FANOUT = 5001


def _node(node_id: str, subtype: str, **attrs: Any) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs=attrs,
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


def _apply(dsn: str, facts: list[Any], *, source: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(facts, source=source)


def _publish_cycle(dsn: str) -> None:
    result = Publisher(
        dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    ).cycle()
    assert result["status"] == "published"


@pytest.fixture
def small_graph(clean_runtime: str) -> str:
    """A hub doc citing three releases (one restricted), mentioned by two pages
    from a second source, plus one restricted doc."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    _apply(clean_runtime, [
        _node("doc:hub", "doc", title="Hub guide"),
        _node("doc:hidden", "doc", title="Hidden", **RESTRICTED),
        _node("release:a", "release", name="A"),
        _node("release:b", "release", name="B", **RESTRICTED),
        _node("release:c", "release", name="C"),
        EdgeFact(src="doc:hub", dst="release:a", edge_type="cites"),
        EdgeFact(src="doc:hub", dst="release:b", edge_type="cites"),
        EdgeFact(src="doc:hub", dst="release:c", edge_type="cites"),
    ], source="fixture")
    _apply(clean_runtime, [
        _node("page:one", "page", title="One"),
        _node("page:two", "page", title="Two"),
        EdgeFact(src="page:one", dst="doc:hub", edge_type="mentions"),
        EdgeFact(src="page:two", dst="doc:hub", edge_type="mentions"),
    ], source="other")
    _publish_cycle(clean_runtime)
    return clean_runtime


@pytest.fixture
def big_hub(clean_runtime: str) -> str:
    """One record with 5,001 `member_of` neighbours of one type and one `owns`."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    members = [_node(f"member:{i:05d}", "member", name=f"M{i}") for i in range(HUB_FANOUT)]
    _apply(clean_runtime, [
        _node("branch:head", "branch", name="HEAD"),
        _node("person:owner", "person", name="Owner"),
        *members,
        *[EdgeFact(src=m.node_id, dst="branch:head", edge_type="member_of") for m in members],
        EdgeFact(src="branch:head", dst="person:owner", edge_type="owns"),
    ], source="fixture")
    _publish_cycle(clean_runtime)
    return clean_runtime


#: Neighbour types of one `contains` relationship, in id order: a few imports
#: first, more references than one policy scan decides, then symbols whose ids
#: sort after every reference. A scan capped per relationship by neighbour id
#: never reaches the symbols.
TYPED_CONTAINS = (("import", "i", 10), ("reference", "r", 600), ("symbol", "s", 30))


@pytest.fixture
def typed_hub(clean_runtime: str) -> str:
    """One file that `contains` 640 records of three types whose ids do not interleave."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    children = [
        _node(f"{prefix}:{i:04d}", subtype, name=f"{prefix}{i}")
        for subtype, prefix, n in TYPED_CONTAINS
        for i in range(n)
    ]
    _apply(clean_runtime, [
        _node("file:hub", "file", name="hub.py"),
        *children,
        *[EdgeFact(src="file:hub", dst=c.node_id, edge_type="contains") for c in children],
    ], source="fixture")
    _publish_cycle(clean_runtime)
    return clean_runtime


@pytest.fixture
def linked_both_ways(clean_runtime: str) -> str:
    """A document that `contains` three chunks, each `member_of` it, and `cites` one.

    Seven links, three neighbour records."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    chunks = [
        _node(f"chunk:{i}", "chunk", title=f"Chunk {i}", **(RESTRICTED if i == 2 else {}))
        for i in (1, 2, 3)
    ]
    _apply(clean_runtime, [
        _node("doc:d", "doc", title="D"),
        *chunks,
        *[EdgeFact(src="doc:d", dst=c.node_id, edge_type="contains") for c in chunks],
        *[EdgeFact(src=c.node_id, dst="doc:d", edge_type="member_of") for c in chunks],
        EdgeFact(src="doc:d", dst="chunk:1", edge_type="cites"),
    ], source="fixture")
    _publish_cycle(clean_runtime)
    return clean_runtime


MIXED_FANOUT = 60


def _restricted_member(i: int) -> bool:
    return i % 7 == 3


@pytest.fixture
def mixed_hub(clean_runtime: str) -> str:
    """One record with 60 `member_of` neighbours, every seventh one restricted."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    members = [
        _node(
            f"member:{i:05d}", "member", name=f"M{i}",
            **(RESTRICTED if _restricted_member(i) else {}),
        )
        for i in range(MIXED_FANOUT)
    ]
    _apply(clean_runtime, [
        _node("branch:head", "branch", name="HEAD"),
        *members,
        *[EdgeFact(src=m.node_id, dst="branch:head", edge_type="member_of") for m in members],
    ], source="fixture")
    _publish_cycle(clean_runtime)
    return clean_runtime


class _Pool:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def connection(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn)


class _Ctx:
    """The slice of VizContext the Graph routes read."""

    def __init__(self, dsn: str, *, deployment_dir: Path | None = None) -> None:
        self.dsn = dsn
        self.deployment_name = DEPLOYMENT
        with psycopg.connect(dsn, autocommit=True) as conn:
            pinned = open_session(conn, deployment_name=DEPLOYMENT)
        self.session = SimpleNamespace(
            branch_name="default", generation_id=pinned.generation_id,
        )
        self.catalog_version_id = pinned.catalog_version_id
        self.pool = _Pool(dsn)
        self.deployment_dir = deployment_dir

    @contextmanager
    def conn(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            yield conn


def _client(ctx: _Ctx) -> TestClient:
    app = FastAPI()
    router = APIRouter(prefix="/viz/api")
    register_operator_record_routes(router, ctx_provider=lambda: ctx)
    app.include_router(router)
    return TestClient(app)


def _rule(rule_id: str, *, match: dict[str, Any], effect: str = "allow", priority: int = 100) -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "effect": effect,
        "actions": ["graph.read.local_operator"],
        "subjects": [SUBJECT],
        "match": match,
        "priority": priority,
        "reason_code": f"nomos.test.{rule_id}",
    }


def _enforce(dsn: str, monkeypatch: pytest.MonkeyPatch, rules: list[dict[str, Any]]) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:console-graph-reads",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules,
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=DEPLOYMENT,
            bundle=bundle,
            scope=f"deployment:{DEPLOYMENT}",
            activated_by="test:operator",
        )
        conn.commit()
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)


ALLOW_ALL_BUT_RESTRICTED = [
    _rule("allow_ui_boundary", match={"resource_kind": "local_operator_boundary"}),
    _rule("allow_ui_rows", match={}),
    _rule("deny_restricted", match={"classification": "restricted"}, effect="deny", priority=200),
]


# --------------------------------------------------------------------------- #
# types                                                                        #
# --------------------------------------------------------------------------- #
def test_types_equal_the_maintained_rollups(small_graph: str) -> None:
    from okg.substrate.console_views.console_count_rollup_read import read_console_count_rollup

    ctx = _Ctx(small_graph)
    response = _client(ctx).get("/viz/api/console/graph/types")

    assert response.status_code == 200, response.text
    body = response.json()
    payload = body["payload"]
    assert body["generation_id"] == payload["generation_id"] == ctx.session.generation_id
    assert payload["deployment"] == DEPLOYMENT
    assert payload["count_basis"] == "published_total_before_access_rules"
    with psycopg.connect(small_graph, autocommit=True) as conn:
        counts = read_console_count_rollup(conn, generation_id=ctx.session.generation_id)
    rollup_types = {v: n for (k, v), n in counts.items() if k == "subtype"}
    assert {row["type"]: row["count"] for row in payload["types"]} == rollup_types
    assert payload["totals"] == {"records": 7, "relationships": 5}
    assert [row["type"] for row in payload["types"]][:2] == ["release", "doc"]
    sources = {row["source"]: row for row in payload["sources"]}
    assert sources["fixture"] == {"source": "fixture", "kind": "source", "records": 5, "relationships": 3}
    assert sources["other"]["relationships"] == 2
    assert {(r["source"], r["type"]): r["count"] for r in payload["records_by_source"]}[("other", "page")] == 2
    patterns = {(p["from"], p["relationship"], p["to"]): p for p in payload["patterns"]}
    assert patterns[("doc", "cites", "release")]["count"] == 3
    assert patterns[("page", "mentions", "doc")]["producers"] == [{"producer": "other", "count": 2}]


def test_types_refuse_an_unmaintained_version_by_name(small_graph: str) -> None:
    """A version without a maintained rollup is a named 503, never empty lists."""
    ctx = _Ctx(small_graph)
    missing = ctx.session.generation_id.rsplit(":", 1)[0] + ":000000000000"
    response = _client(ctx).get("/viz/api/console/graph/types", params={"version": missing})

    # The version itself is not published, so the pin refuses it first; the
    # composer's own 503 is proven in the unit tests with the reader failing.
    assert response.status_code == 404
    assert response.json()["detail"]["reason"] == "version_not_published"


# --------------------------------------------------------------------------- #
# record                                                                       #
# --------------------------------------------------------------------------- #
def test_record_groups_neighbours_with_producers(small_graph: str) -> None:
    response = _client(_Ctx(small_graph)).get(
        "/viz/api/console/graph/record", params={"id": "doc:hub"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert payload["record"] == {"id": "doc:hub", "type": "doc", "label": "Hub guide", "source": "fixture"}
    assert payload["count_basis"] == "published_total_before_access_rules"
    groups = {(g["direction"], g["relationship"], g["type"]): g for g in payload["groups"]}
    assert set(groups) == {("out", "cites", "release"), ("in", "mentions", "page")}
    cites = groups[("out", "cites", "release")]
    assert cites["count"] == 3 and cites["count_is_floor"] is False
    assert [item["id"] for item in cites["items"]] == ["release:a", "release:b", "release:c"]
    assert {item["producer"] for item in cites["items"]} == {"fixture"}
    mentions = groups[("in", "mentions", "page")]
    assert [(i["id"], i["label"], i["producer"]) for i in mentions["items"]] == [
        ("page:one", "One", "other"), ("page:two", "Two", "other"),
    ]
    assert payload["capped"] == {"in": False, "out": False}


def test_record_refuses_an_absent_record(small_graph: str) -> None:
    response = _client(_Ctx(small_graph)).get(
        "/viz/api/console/graph/record", params={"id": "doc:nowhere"},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["reason"] == "record_not_found"


def test_a_hub_over_five_thousand_is_counted_exactly_and_lists_the_first_25(big_hub: str) -> None:
    response = _client(_Ctx(big_hub)).get(
        "/viz/api/console/graph/record", params={"id": "branch:head"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert payload["scan_cap"] == payload["count_cap"] == 100_000
    assert payload["per_group"] == 25
    assert payload["capped"] == {"in": False, "out": False}
    groups = {(g["direction"], g["relationship"], g["type"]): g for g in payload["groups"]}
    members = groups[("in", "member_of", "member")]
    # Counted in the database, not over a capped scan.
    assert members["count"] == HUB_FANOUT and members["count_is_floor"] is False
    # The first 25 by neighbour id, not the most connected.
    assert [item["id"] for item in members["items"]] == [f"member:{i:05d}" for i in range(25)]
    totals = {(r["direction"], r["relationship"]): r for r in payload["relationships"]}
    assert totals[("in", "member_of")] == {
        "direction": "in", "relationship": "member_of", "count": HUB_FANOUT,
        "count_is_floor": False, "types_complete": True,
    }
    # The small relationship is still reached beside the huge one.
    assert groups[("out", "owns", "person")]["count"] == 1
    assert payload["neighbours"] == {
        "in": {"count": HUB_FANOUT, "count_is_floor": False},
        "out": {"count": 1, "count_is_floor": False},
        "all": {"count": HUB_FANOUT + 1, "count_is_floor": False},
    }


def test_a_policy_hides_and_does_not_count_what_the_reader_may_not_see(
    small_graph: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _Ctx(small_graph)
    _enforce(small_graph, monkeypatch, ALLOW_ALL_BUT_RESTRICTED)
    client = _client(ctx)

    response = client.get("/viz/api/console/graph/record", params={"id": "doc:hub"})

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert payload["count_basis"] == "visible_to_reader"
    cites = next(g for g in payload["groups"] if g["relationship"] == "cites")
    assert [item["id"] for item in cites["items"]] == ["release:a", "release:c"]
    assert cites["count"] == 2
    assert {(r["direction"], r["relationship"], r["count"]) for r in payload["relationships"]} == {
        ("out", "cites", 2), ("in", "mentions", 2),
    }
    assert "release:b" not in response.text

    hidden = client.get("/viz/api/console/graph/record", params={"id": "doc:hidden"})
    absent = client.get("/viz/api/console/graph/record", params={"id": "doc:nowhere"})
    assert hidden.status_code == absent.status_code == 404
    assert hidden.json()["detail"]["reason"] == absent.json()["detail"]["reason"] == "record_not_found"


def test_under_a_policy_a_hub_counts_what_it_read_and_says_it_is_a_floor(
    big_hub: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _Ctx(big_hub)
    _enforce(big_hub, monkeypatch, ALLOW_ALL_BUT_RESTRICTED)
    # This test is about the per-group cap; the read's budget is tested below.
    monkeypatch.setattr(graph_record, "GRAPH_CHECK_BUDGET", 10_000)

    started = time.perf_counter()
    response = _client(ctx).get("/viz/api/console/graph/record", params={"id": "branch:head"})
    elapsed_ms = round((time.perf_counter() - started) * 1000)

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert payload["count_basis"] == "visible_to_reader"
    # Under a policy each group is decided up to 500 links.
    assert payload["scan_cap"] == 500
    members = next(g for g in payload["groups"] if g["relationship"] == "member_of")
    assert members["count"] == 500 and members["count_is_floor"] is True
    assert [item["id"] for item in members["items"]] == [f"member:{i:05d}" for i in range(25)]
    totals = {(r["direction"], r["relationship"]): r for r in payload["relationships"]}
    # Under a policy the 5,001 total is not served: it would count rows unread.
    assert totals[("in", "member_of")] == {
        "direction": "in", "relationship": "member_of", "count": 500,
        "count_is_floor": True, "types_complete": True,
    }
    assert totals[("out", "owns")]["count"] == 1
    assert payload["capped"] == {"in": True, "out": False}
    assert payload["neighbours"]["in"] == {"count": 500, "count_is_floor": True}
    print(f"policy-filtered hub read: {elapsed_ms} ms")


def _typed_groups(payload: dict[str, Any]) -> dict[str, tuple[int, bool, list[str]]]:
    return {
        g["type"]: (g["count"], g["count_is_floor"], [item["id"] for item in g["items"]])
        for g in payload["groups"]
        if g["relationship"] == "contains"
    }


def test_every_neighbour_type_is_reached_however_the_ids_sort(typed_hub: str) -> None:
    response = _client(_Ctx(typed_hub)).get(
        "/viz/api/console/graph/record", params={"id": "file:hub"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    groups = _typed_groups(payload)
    assert groups == {
        "import": (10, False, [f"i:{i:04d}" for i in range(10)]),
        "reference": (600, False, [f"r:{i:04d}" for i in range(25)]),
        "symbol": (30, False, [f"s:{i:04d}" for i in range(25)]),
    }
    assert payload["relationships"] == [{
        "direction": "out", "relationship": "contains", "count": 640,
        "count_is_floor": False, "types_complete": True,
    }]


def test_under_a_policy_every_type_is_reached_and_only_a_cut_group_is_a_floor(
    typed_hub: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 500-link policy scan applies per type: the symbols after 600
    references are still decided, and the imports' 10 is exact, not "10+"."""
    ctx = _Ctx(typed_hub)
    _enforce(typed_hub, monkeypatch, ALLOW_ALL_BUT_RESTRICTED)
    monkeypatch.setattr(graph_record, "GRAPH_CHECK_BUDGET", 10_000)

    response = _client(ctx).get("/viz/api/console/graph/record", params={"id": "file:hub"})

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert payload["count_basis"] == "visible_to_reader" and payload["scan_cap"] == 500
    groups = _typed_groups(payload)
    assert groups == {
        "import": (10, False, [f"i:{i:04d}" for i in range(10)]),
        "reference": (500, True, [f"r:{i:04d}" for i in range(25)]),
        "symbol": (30, False, [f"s:{i:04d}" for i in range(25)]),
    }
    assert payload["relationships"] == [{
        "direction": "out", "relationship": "contains", "count": 540,
        "count_is_floor": True, "types_complete": True,
    }]
    assert payload["capped"] == {"in": False, "out": True}


def test_a_relationship_past_the_count_cap_names_that_its_types_may_be_incomplete(
    typed_hub: str,
) -> None:
    """Past the count cap the symbols are never seen; the read says so instead
    of letting the group vanish."""
    from okg.substrate.console_views.graph_record import compose_graph_record

    with psycopg.connect(typed_hub) as conn:
        session = open_session(conn, deployment_name=DEPLOYMENT)
        payload = compose_graph_record(
            conn, session=session, deployment_name=DEPLOYMENT, node_id="file:hub",
            count_cap=100,
        )

    groups = _typed_groups(payload)
    assert {t: (n, floor) for t, (n, floor, _) in groups.items()} == {
        "import": (10, True), "reference": (90, True),
    }
    assert payload["relationships"] == [{
        "direction": "out", "relationship": "contains", "count": 100,
        "count_is_floor": True, "types_complete": False,
    }]
    assert payload["scan_cap"] == 100
    assert payload["capped"] == {"in": False, "out": True}
    assert payload["neighbours"]["out"] == {"count": 100, "count_is_floor": True}


#: A group whose first 500 links (by neighbour id) are all hidden and whose
#: last 20 are visible: one policy check per group never reaches a visible one.
HIDDEN_PREFIX, VISIBLE_TAIL = 500, 20


@pytest.fixture
def hidden_prefix_hub(clean_runtime: str) -> str:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    members = [
        _node(
            f"member:{i:05d}", "member", name=f"M{i}",
            **(RESTRICTED if i < HIDDEN_PREFIX else {}),
        )
        for i in range(HIDDEN_PREFIX + VISIBLE_TAIL)
    ]
    _apply(clean_runtime, [
        _node("branch:head", "branch", name="HEAD"),
        *members,
        *[EdgeFact(src=m.node_id, dst="branch:head", edge_type="member_of") for m in members],
    ], source="fixture")
    _publish_cycle(clean_runtime)
    return clean_runtime


def test_a_group_whose_checked_links_are_all_hidden_still_marks_its_side_a_floor(
    hidden_prefix_hub: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first 500 links are hidden, so no group is shown; the side must not
    read as an exact zero ("No relationships"), because 20 more links exist."""
    ctx = _Ctx(hidden_prefix_hub)
    _enforce(hidden_prefix_hub, monkeypatch, ALLOW_ALL_BUT_RESTRICTED)
    monkeypatch.setattr(graph_record, "GRAPH_CHECK_BUDGET", 10_000)

    response = _client(ctx).get("/viz/api/console/graph/record", params={"id": "branch:head"})

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert payload["groups"] == [] and payload["relationships"] == []
    assert payload["unchecked_links"] == {"in": False, "out": False}
    assert payload["capped"] == {"in": True, "out": False}
    assert payload["capped_by"] == {"in": ["scan_cap"], "out": []}
    assert payload["neighbours"] == {
        "in": {"count": 0, "count_is_floor": True},
        "out": {"count": 0, "count_is_floor": False},
        "all": {"count": 0, "count_is_floor": True},
    }
    assert "member:00000" not in response.text


def test_the_read_stops_at_its_check_budget_and_says_so(
    typed_hub: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """540 links would be checked; the 400-row budget is shared evenly, so the
    two small groups stay exact and only the big one is a named floor."""
    ctx = _Ctx(typed_hub)
    _enforce(typed_hub, monkeypatch, ALLOW_ALL_BUT_RESTRICTED)
    assert graph_record.GRAPH_CHECK_BUDGET == 400

    response = _client(ctx).get("/viz/api/console/graph/record", params={"id": "file:hub"})

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert payload["check_budget"] == 400
    assert payload["unchecked_links"] == {"in": False, "out": False}
    # 600 references: more than one group's 500-link check as well.
    assert payload["capped_by"] == {"in": [], "out": ["check_budget", "scan_cap"]}
    groups = {g["type"]: (g["count"], g["count_is_floor"], g["floor_reason"]) for g in payload["groups"]}
    assert groups == {
        "import": (10, False, None),
        "symbol": (30, False, None),
        "reference": (360, True, "check_budget"),
    }
    assert payload["capped"] == {"in": False, "out": True}


def test_a_group_the_budget_never_reaches_is_not_named_and_its_side_says_so(
    small_graph: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _Ctx(small_graph)
    _enforce(small_graph, monkeypatch, ALLOW_ALL_BUT_RESTRICTED)
    monkeypatch.setattr(graph_record, "GRAPH_CHECK_BUDGET", 1)

    response = _client(ctx).get("/viz/api/console/graph/record", params={"id": "doc:hub"})

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert payload["check_budget"] == 1

    # Two groups (2 mentions, 3 cites) and one check: the smaller group gets
    # none. Its relationship was named only by totals read before the access
    # check, so it is not named: the side says links went unchecked.
    assert payload["unchecked_links"] == {"in": True, "out": False}
    assert payload["capped_by"] == {"in": ["check_budget"], "out": ["check_budget"]}
    assert "mentions" not in response.text
    assert [g["relationship"] for g in payload["groups"]] == ["cites"]
    cites = payload["groups"][0]
    assert (cites["count"], cites["count_is_floor"], cites["floor_reason"]) == (1, True, "check_budget")
    assert payload["capped"] == {"in": True, "out": True}
    assert payload["neighbours"]["all"]["count_is_floor"] is True


@pytest.fixture
def hidden_sibling_hub(clean_runtime: str) -> str:
    """One file that `contains` 600 alphas (the first 500 hidden), 2 betas and 1 gamma."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    children = [
        *[_node(f"a:{i:04d}", "alpha", name=f"a{i}", **(RESTRICTED if i < 500 else {})) for i in range(600)],
        *[_node(f"b:{i:04d}", "beta", name=f"b{i}") for i in range(2)],
        _node("c:0000", "gamma", name="c0"),
    ]
    _apply(clean_runtime, [
        _node("file:hub", "file", name="hub.py"),
        *children,
        *[EdgeFact(src="file:hub", dst=c.node_id, edge_type="contains") for c in children],
    ], source="fixture")
    _publish_cycle(clean_runtime)
    return clean_runtime


def test_a_relationship_with_a_capped_hidden_group_is_a_floor_beside_its_visible_siblings(
    hidden_sibling_hub: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The alphas' first 500 checked links are all hidden, so no alpha group is
    shown; `contains` must still read "3+", not an exact 3."""
    ctx = _Ctx(hidden_sibling_hub)
    _enforce(hidden_sibling_hub, monkeypatch, ALLOW_ALL_BUT_RESTRICTED)
    monkeypatch.setattr(graph_record, "GRAPH_CHECK_BUDGET", 10_000)

    response = _client(ctx).get("/viz/api/console/graph/record", params={"id": "file:hub"})

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert {g["type"]: (g["count"], g["count_is_floor"]) for g in payload["groups"]} == {
        "beta": (2, False), "gamma": (1, False),
    }
    assert payload["relationships"] == [{
        "direction": "out", "relationship": "contains", "count": 3,
        "count_is_floor": True, "types_complete": True,
    }]
    assert payload["capped"] == {"in": False, "out": True}
    assert payload["capped_by"] == {"in": [], "out": ["scan_cap"]}
    assert payload["neighbours"]["out"] == {"count": 3, "count_is_floor": True}
    assert "alpha" not in response.text


def test_counts_are_distinct_records_not_links(linked_both_ways: str) -> None:
    response = _client(_Ctx(linked_both_ways)).get(
        "/viz/api/console/graph/record", params={"id": "doc:d"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert {(r["direction"], r["relationship"], r["count"]) for r in payload["relationships"]} == {
        ("out", "contains", 3), ("in", "member_of", 3), ("out", "cites", 1),
    }
    # Seven links; three records.
    assert payload["neighbours"] == {
        "in": {"count": 3, "count_is_floor": False},
        "out": {"count": 3, "count_is_floor": False},
        "all": {"count": 3, "count_is_floor": False},
    }


def test_under_a_policy_distinct_records_count_only_what_the_reader_may_see(
    linked_both_ways: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _Ctx(linked_both_ways)
    _enforce(linked_both_ways, monkeypatch, ALLOW_ALL_BUT_RESTRICTED)

    response = _client(ctx).get("/viz/api/console/graph/record", params={"id": "doc:d"})

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert {(r["direction"], r["relationship"], r["count"]) for r in payload["relationships"]} == {
        ("out", "contains", 2), ("in", "member_of", 2), ("out", "cites", 1),
    }
    assert payload["neighbours"] == {
        "in": {"count": 2, "count_is_floor": False},
        "out": {"count": 2, "count_is_floor": False},
        "all": {"count": 2, "count_is_floor": False},
    }
    assert "chunk:2" not in response.text


def _decided_members(dsn: str) -> set[str]:
    """The member records any recorded row decision names."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT DISTINCT target->>'node_id' FROM okg.nomos_decisions"
            " WHERE target->>'node_id' LIKE 'member:%%'"
        ).fetchall()
    return {row[0] for row in rows}


def _visible_members(start: int = 0) -> list[str]:
    return [
        f"member:{i:05d}" for i in range(start, MIXED_FANOUT) if not _restricted_member(i)
    ]


def test_under_a_policy_every_counted_row_gets_its_own_decision(
    mixed_hub: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A group larger than it lists is still decided row by row: every member
    counted has its own recorded policy decision, and only visible ones list."""
    ctx = _Ctx(mixed_hub)
    _enforce(mixed_hub, monkeypatch, ALLOW_ALL_BUT_RESTRICTED)
    assert _decided_members(mixed_hub) == set()

    response = _client(ctx).get("/viz/api/console/graph/record", params={"id": "branch:head"})

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert payload["count_basis"] == "visible_to_reader"
    members = next(g for g in payload["groups"] if g["relationship"] == "member_of")
    visible = _visible_members()
    assert len(visible) == 51
    assert members["count"] == 51 and members["count_is_floor"] is False
    assert [item["id"] for item in members["items"]] == visible[:25]
    assert payload["relationships"] == [
        {
            "direction": "in", "relationship": "member_of", "count": 51,
            "count_is_floor": False, "types_complete": True,
        },
    ]
    for i in range(MIXED_FANOUT):
        if _restricted_member(i):
            assert f"member:{i:05d}" not in response.text
    # Every one of the 60 members was decided on its own, hidden ones included.
    assert _decided_members(mixed_hub) == {f"member:{i:05d}" for i in range(MIXED_FANOUT)}


def test_a_rule_on_one_record_id_is_honoured_by_the_count(
    mixed_hub: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _Ctx(mixed_hub)
    _enforce(mixed_hub, monkeypatch, [
        *ALLOW_ALL_BUT_RESTRICTED,
        _rule("deny_one", match={"node_id": "member:00001"}, effect="deny", priority=300),
    ])

    response = _client(ctx).get("/viz/api/console/graph/record", params={"id": "branch:head"})

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    members = next(g for g in payload["groups"] if g["relationship"] == "member_of")
    visible = [m for m in _visible_members() if m != "member:00001"]
    assert members["count"] == len(visible) == 50
    assert [item["id"] for item in members["items"]] == visible[:25]
    assert "member:00001" not in response.text


def test_a_policy_that_denies_the_boundary_refuses_every_graph_read(
    small_graph: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _Ctx(small_graph)
    _enforce(small_graph, monkeypatch, [_rule("rows_only", match={"resource_kind": "graph_object"})])
    client = _client(ctx)
    for path, params in (
        ("types", {}),
        ("source-links", {}),
        ("record", {"id": "doc:hub"}),
        ("ontology", {}),
    ):
        response = client.get(f"/viz/api/console/graph/{path}", params=params)
        assert response.status_code == 403, path
        assert response.json()["detail"]["reason"] == "policy_denied"


# --------------------------------------------------------------------------- #
# source links                                                                 #
# --------------------------------------------------------------------------- #
def test_source_links_count_by_both_ends_and_answer_again_from_cache(small_graph: str) -> None:
    graph_views.SOURCE_LINKS_CACHE.clear()
    client = _client(_Ctx(small_graph))

    first = client.get("/viz/api/console/graph/source-links")
    second = client.get("/viz/api/console/graph/source-links")

    assert first.status_code == second.status_code == 200, first.text
    one, two = first.json()["payload"], second.json()["payload"]
    assert one["basis"] == "computed_on_request"
    assert (one["cached"], two["cached"]) == (False, True)
    assert one["rows"] == two["rows"]
    assert {(r["from_source"], r["to_source"], r["producer"]): r["count"] for r in one["rows"]} == {
        ("fixture", "fixture", "fixture"): 3,
        ("other", "fixture", "other"): 2,
    }
    graph_views.SOURCE_LINKS_CACHE.clear()


def test_source_links_timeout_is_named_with_its_elapsed_time(
    small_graph: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph_views.SOURCE_LINKS_CACHE.clear()
    monkeypatch.setattr(
        graph_views, "_SOURCE_LINKS_SQL",
        "SELECT NULL AS from_source, NULL AS to_source, NULL AS producer, 0 AS n "
        "FROM okg.graph_edges, pg_sleep(1) LIMIT 1",
    )
    assert graph_views.SOURCE_LINKS_TIMEOUT_MS == 18_000
    monkeypatch.setattr(graph_views, "SOURCE_LINKS_TIMEOUT_MS", 100)

    response = _client(_Ctx(small_graph)).get("/viz/api/console/graph/source-links")

    assert response.status_code == 503, response.text
    detail = response.json()["detail"]
    assert detail["reason"] == "source_links_unavailable"
    assert detail["cause"] == "timeout"
    assert detail["elapsed_ms"] >= 100
    assert graph_views.SOURCE_LINKS_CACHE.entries == {}

    # The pooled connection goes back with the timeout it came with.
    with psycopg.connect(small_graph, autocommit=True) as conn:
        conn.execute("SET statement_timeout = '7s'")
        session = open_session(conn, deployment_name=DEPLOYMENT)
        with pytest.raises(graph_views.GraphReadRefused):
            graph_views.compose_source_links(conn, session=session, deployment_name=DEPLOYMENT)
        assert conn.execute("SHOW statement_timeout").fetchone()[0] == "7s"


# --------------------------------------------------------------------------- #
# ontology                                                                     #
# --------------------------------------------------------------------------- #
_SCHEMA = """
id: https://example.org/fixture
name: fixture
default_range: string
slots:
  owner:
    range: Person
    required: true
classes:
  Named:
    attributes:
      name:
        required: true
  Doc:
    is_a: Named
    annotations:
      archetype: artifact
    slots:
      - owner
    attributes:
      title:
        required: true
      summary: {}
  Release:
    is_a: Named
    annotations:
      archetype: artifact
    attributes:
      classification: {}
"""


def _deployment_dir(tmp_path: Path) -> Path:
    root = tmp_path / "deployment"
    (root / "schemas").mkdir(parents=True)
    (root / "deployment.yaml").write_text("name: default\nmodules: []\n")
    (root / "schemas" / "fixture.yaml").write_text(_SCHEMA)
    return root


def test_ontology_lists_declared_beside_populated(small_graph: str, tmp_path: Path) -> None:
    ctx = _Ctx(small_graph, deployment_dir=_deployment_dir(tmp_path))
    with psycopg.connect(small_graph, autocommit=True) as conn:
        for name in ("doc", "release", "page", "unused"):
            conn.execute(
                "INSERT INTO okg.subtypes (name, metadata, parent_subtype, catalog_version_id)"
                " VALUES (%s, %s::jsonb, NULL, %s) ON CONFLICT DO NOTHING",
                (name, '{"archetype": "artifact"}', ctx.catalog_version_id),
            )
        for parent, edge, child in (("doc", "cites", "release"), ("doc", "cites", "unused")):
            conn.execute(
                "INSERT INTO okg.narrowings (parent_subtype, edge_type, child_subtype, catalog_version_id)"
                " VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (parent, edge, child, ctx.catalog_version_id),
            )

    response = _client(ctx).get("/viz/api/console/graph/ontology")

    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert payload["catalog_version_id"] == ctx.catalog_version_id
    assert payload["attributes_basis"] == "deployment_schema_files_at_read_time"
    types = {t["type"]: t for t in payload["types"]}
    assert types["doc"]["count"] == 2 and types["unused"]["count"] == 0
    assert types["doc"]["module"] == "deployment:default"
    assert types["doc"]["attributes"] == [
        {"name": "name", "required": True, "range": "string"},
        {"name": "owner", "required": True, "range": "Person"},
        {"name": "summary", "required": False, "range": "string"},
        {"name": "title", "required": True, "range": "string"},
    ]
    assert types["page"]["declared_in_schema"] is False
    rules = {(r["from"], r["relationship"], r["to"]): r["count"] for r in payload["rules"]}
    assert rules == {("doc", "cites", "release"): 3, ("doc", "cites", "unused"): 0}
    doc_fill = payload["fill"]["doc"]
    assert (doc_fill["population"], doc_fill["sampled"], doc_fill["method"]) == (2, 2, "first_1000_by_index")
    fill = {a["name"]: a for a in doc_fill["attributes"]}
    assert fill["title"]["fill_rate"] == 1.0 and fill["title"]["declared"] is True
    # Sampled before access rules, so only declared attributes are reported,
    # and never one the read policy decides on: `classification` sits on
    # doc:hidden but is neither declared nor reportable.
    assert set(fill) == {"title"}
    assert payload["fill_basis"] == "declared_attributes_before_access_rules"
    assert payload["fill_excluded_attributes"] == [
        "classification", "nomos_redacted", "nomos_source_lineage",
        "untrusted_content_origin", "untrusted_content_tool_names",
    ]
    assert all(a["declared"] for f in payload["fill"].values() for a in f["attributes"])
    # Declared by a schema is not enough when the policy decides on it.
    release_fill = payload["fill"]["release"]
    assert {a["name"] for a in release_fill["attributes"]} == {"name"}
    assert "classification" not in release_fill["declared_never_seen"]
    assert doc_fill["declared_never_seen"] == ["name", "owner", "summary"]
    assert "unused" not in payload["fill"]


def test_ontology_refuses_by_name_when_the_schema_files_cannot_be_read(
    small_graph: str, tmp_path: Path,
) -> None:
    ctx = _Ctx(small_graph, deployment_dir=tmp_path / "no-such-deployment")

    response = _client(ctx).get("/viz/api/console/graph/ontology")

    assert response.status_code == 503
    assert response.json()["detail"]["reason"] == "declared_schema_unavailable"
