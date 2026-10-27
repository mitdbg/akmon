"""Nomos audit and enforce see the S2 row dicts (PostgreSQL).

PACT lane-s-storage-redesign-s2 and lane-s-storage-cutover, requirement
``nomos-unchanged``: with Nomos ``audit`` or ``enforce`` the graph path (the
only read path since the cutover) hands ``filter_node_rows_for_session`` and
``filter_edge_rows_for_session`` the row dicts S2 defined, built here from
the Chronos rows and the overlay rows, and the page is masked as before. A
planted extra key in the row dicts fails the comparison.

The fixture has a policy that redacts a restricted document for the MCP
reader, an overlay that moves a document INTO restricted (and supplies a raw
identifier), an overlay that moves another OUT of restricted, a key present
only in an overlay, a key present only in the graph row, and an edge with an
overlay, published once.
"""
from __future__ import annotations

import dataclasses
import json
import uuid
from typing import Any

import psycopg
import pytest

from okg.substrate.attributes import (
    ATTRIBUTE_EFFECTIVE_TABLE,
    RESERVED_EFFECTIVE_ATTR_KEYS,
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import graph_read, open_session
from okg.substrate.mcp import server as server_mod
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.nomos.read_masking import MASK_TOKEN
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)
from tests.substrate.mcp.test_operator_attr_nomos_parity import (
    _redact_on_restricted_policy,
)

pytestmark = pytest.mark.postgres

_SUBJECT = "subject:mcp-reader"
_PLANTED = "planted_extra_key"


class _NomosWorld:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        s = uuid.uuid4().hex[:8]
        self.ids = {k: f"doc:{k}:{s}" for k in ("in", "out", "plain")}
        with psycopg.connect(dsn, autocommit=True) as conn:
            seed_catalog(conn, subtypes=[SubtypeSpec("document", metadata={
                "pii_classes": {
                    "classification": "non_pii",
                    "owner_email": "direct_identifier",
                    "text": "free_text",
                },
            })])
            activate_policy_bundle(
                conn, deployment_name="default",
                bundle=compile_policy_graph(_redact_on_restricted_policy()),
                scope="deployment:default", activated_by="test:s2", bootstrap=True,
            )
            store = ChronosGraphStore.from_connection(conn)
            store.apply_facts([
                NodeFact(self.ids["in"], "document", {
                    "classification": "internal", "owner_email": "raw@example.com",
                    "text": "secret-in"}, {"id": self.ids["in"]}, {"v": 1}),
                NodeFact(self.ids["out"], "document", {
                    "classification": "restricted", "owner_email": "out@example.com",
                    "text": "secret-out", "graph_only": "g"},
                    {"id": self.ids["out"]}, {"v": 1}),
                NodeFact(self.ids["plain"], "document", {
                    "classification": "internal", "text": "plain"},
                    {"id": self.ids["plain"]}, {"v": 1}),
            ], source="default", strict_endpoints=True)
            store.apply_facts([
                EdgeFact(self.ids["in"], self.ids["out"], "references",
                         {"classification": "internal", "weight": 1},
                         source_record_id={"id": f"e1{s}"}, source_revision={"v": 1}),
                EdgeFact(self.ids["out"], self.ids["plain"], "references",
                         {"classification": "internal", "weight": 2},
                         source_record_id={"id": f"e2{s}"}, source_revision={"v": 1}),
            ], source="default")
        out = Publisher(dsn, "default",
                        catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
        assert out["status"] == "published", out
        self.gen = str(out["generation_id"])
        with psycopg.connect(dsn, autocommit=True) as conn:
            checkout = ChronosGraphStore.from_connection(
                conn, ensure_schema=False).checkout_generation(self.gen)
            edges = sorted(
                (str(next(iter(r.values())) if isinstance(r, dict) else r[0])
                 for r in checkout.query("SELECT edge_id FROM okg.graph_edges", {})),
                key=int,
            )
            for target, key, value, kind in (
                (self.ids["in"], "classification", "restricted", "node"),
                (self.ids["in"], "owner_email", "alice@example.com", "node"),
                (self.ids["in"], "overlay_only", "o", "node"),
                (self.ids["out"], "classification", "internal", "node"),
                (edges[0], "weight", 9, "edge"),
            ):
                record_attribute_observation(
                    conn, target_kind=kind, target_id=target, generation_id=self.gen,
                    attr_key=key, value=value, producer="test:s2-nomos",
                )
            materialize_effective_attributes_for_generation(conn, generation_id=self.gen)


@pytest.fixture
def world(clean_runtime: str, monkeypatch: pytest.MonkeyPatch) -> _NomosWorld:
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", _SUBJECT)
    return _NomosWorld(clean_runtime)


def _canon(rows: Any) -> Any:
    return json.loads(json.dumps(rows, default=str, sort_keys=True))


def _read(world: _NomosWorld, monkeypatch, mode: str, *,
          plant: bool = False) -> dict[str, Any]:
    """The rows handed to the read filter, the filter's output, and the
    page, for nodes and edges. With ``plant`` the row dicts get an extra
    key before the filter sees them."""
    assert mode == "graph", mode
    real = {"nodes": server_mod.filter_node_rows_for_session,
            "edges": server_mod.filter_edge_rows_for_session}
    handed: dict[str, list] = {"nodes": [], "edges": []}
    served: dict[str, list] = {"nodes": [], "edges": []}

    def spy(kind: str):
        def wrapped(conn, session, rows, **kw):
            rows = list(rows)
            if plant and mode == "graph":
                for row in rows:
                    row[_PLANTED] = True
            handed[kind].append(_canon(rows))
            out = real[kind](conn, session, rows, **kw)
            served[kind].append(_canon(out))
            return out
        return wrapped

    graph_reads: list[str] = []
    real_pin = graph_read.require_graph_pin

    def pin(conn, session):
        graph_reads.append(session.generation_id)
        return real_pin(conn, session)

    monkeypatch.setattr(server_mod, "filter_node_rows_for_session", spy("nodes"))
    monkeypatch.setattr(server_mod, "filter_edge_rows_for_session", spy("edges"))
    monkeypatch.setattr(graph_read, "require_graph_pin", pin)
    pages: dict[str, list] = {}
    mcp = server_mod.build_server(dsn=world.dsn, deployment_name="default")
    try:
        with psycopg.connect(world.dsn, autocommit=True) as conn:
            session = dataclasses.replace(
                open_session(conn, deployment_name="default",
                             requested_generation=world.gen),
                nomos_subject=_SUBJECT,
            )
            for kind, scope in (
                ("nodes", {"op": "eq", "field": "subtype", "value": "document"}),
                ("edges", {"op": "eq", "field": "edge_type", "value": "references"}),
            ):
                rows, _more = mcp._okg_effective_view_sql_page(
                    conn, session, kind=kind, predicate=scope, limit=50, offset=0,
                )
                pages[kind] = _canon(rows)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
        monkeypatch.setattr(server_mod, "filter_node_rows_for_session", real["nodes"])
        monkeypatch.setattr(server_mod, "filter_edge_rows_for_session", real["edges"])
        monkeypatch.setattr(graph_read, "require_graph_pin", real_pin)
    return {"handed": handed, "served": served, "pages": pages,
            "graph_reads": graph_reads}


def _s2_rows(world: _NomosWorld, kind: str) -> dict[str, dict]:
    """The row dicts S2 hands the read filter (design 1.3 and 1.4), by id,
    built from the Chronos rows at the pin and that generation's overlay
    rows: ``attrs`` is the graph attrs when the target has an overlay row,
    else the effective attrs; ``_effective_attrs`` the graph attrs merged
    with the overlays (reserved keys excluded). The node's ``text_hash`` is
    left out."""
    with psycopg.connect(world.dsn, autocommit=True) as conn:
        checkout = ChronosGraphStore.from_connection(
            conn, ensure_schema=False).checkout_generation(world.gen)
        sql = ("SELECT node_id AS id, subtype, attrs FROM okg.graph_nodes "
               "WHERE subtype = 'document'" if kind == "nodes" else
               "SELECT edge_id::text AS id, src, dst, edge_type, attrs FROM okg.graph_edges "
               "WHERE edge_type = 'references'")
        columns = (("id", "subtype", "attrs") if kind == "nodes"
                   else ("id", "src", "dst", "edge_type", "attrs"))
        graph = [dict(r) if isinstance(r, dict) else dict(zip(columns, r))
                 for r in checkout.query(sql, {})]
        overlays: dict[str, dict[str, Any]] = {}
        for target, key, value in conn.execute(
            f"SELECT target_id, attr_key, value FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
            "WHERE generation_id = %s AND target_kind = %s AND namespace = 'default'",
            (world.gen, "node" if kind == "nodes" else "edge"),
        ).fetchall():
            if key not in RESERVED_EFFECTIVE_ATTR_KEYS:
                overlays.setdefault(str(target), {})[key] = value
    out: dict[str, dict] = {}
    for row in graph:
        target = str(row.pop("id"))
        stored = dict(row.pop("attrs"))
        effective = {**stored, **overlays.get(target, {})}
        id_field = "node_id" if kind == "nodes" else "edge_id"
        out[target] = _canon({id_field: target, **row,
                              "attrs": stored if target in overlays else effective,
                              "_effective_attrs": effective})
    return out


def _assert_s2_rows(world: _NomosWorld, read: dict[str, Any]) -> None:
    """Every row handed to the read filter, every batch, is the S2 row dict
    of its id (``text_hash`` aside)."""
    for kind, id_field in (("nodes", "node_id"), ("edges", "edge_id")):
        expected = _s2_rows(world, kind)
        for batch in read["handed"][kind]:
            for row in batch:
                if kind == "nodes":
                    assert "text_hash" in row, row
                    row = {k: v for k, v in row.items() if k != "text_hash"}
                assert row == expected[str(row[id_field])], (kind, row)


def _node(page: list[dict], node_id: str) -> dict | None:
    return next((r for r in page if r.get("node_id") == node_id), None)


@pytest.mark.parametrize("enforcement", ["audit", "enforce"])
def test_nomos_gets_the_s2_rows_and_masks_the_page(
    world: _NomosWorld, monkeypatch, enforcement: str,
) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", enforcement)
    read = _read(world, monkeypatch, "graph")
    assert read["graph_reads"], "the graph path answered"
    assert read["handed"]["nodes"] and read["handed"]["edges"]
    _assert_s2_rows(world, read)
    handed_ids = {row["node_id"] for batch in read["handed"]["nodes"] for row in batch}
    assert {world.ids["in"], world.ids["out"]} <= handed_ids, handed_ids
    page = read["pages"]["nodes"]
    moved_in = _node(page, world.ids["in"])
    moved_out = _node(page, world.ids["out"])
    assert moved_out is not None and moved_out["attrs"]["classification"] == "internal"
    # The overlay moved "in" to restricted: the row is kept (audit and
    # enforce both show it), its identifier is masked, and the raw overlay
    # value never reaches the page.
    assert moved_in is not None and moved_in["attrs"]["owner_email"] == MASK_TOKEN
    serialized = json.dumps(page)
    assert "alice@example.com" not in serialized
    assert "secret-in" not in serialized
    assert moved_in["attrs"].get("overlay_only") in ("o", MASK_TOKEN)


@pytest.mark.parametrize("enforcement", ["audit", "enforce"])
def test_a_planted_extra_key_in_the_graph_rows_fails_the_comparison(
    world: _NomosWorld, monkeypatch, enforcement: str,
) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", enforcement)
    read = _read(world, monkeypatch, "graph", plant=True)
    with pytest.raises(AssertionError):
        _assert_s2_rows(world, read)
    assert all(_PLANTED in row for batch in read["handed"]["nodes"] for row in batch)
