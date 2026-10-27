"""Shared fixture for the cross-source endpoint wedge tests.

Source ``a`` owns ``widget`` records. Source ``b`` writes ``note`` records and
the edge ``note references widget`` onto a widget it does not own. Source
``c`` writes unrelated ``document`` records. Each source reads a JSON fixture
file that a test rewrites between runs, so a test can run the sources in any
order, retract a widget (by dropping it from ``a``'s fixture), and bring it
back.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import psycopg
import yaml

from okg.substrate.ingest import IngestOptions, run_ingest
from tests.substrate.test_ingest_service import (
    _claim_layered_fixture_catalog,
    _commit_layered_fixture_deployment,
    _fixture_source_privacy_policy,
    _source,
    _write_layered_fixture_deployment,
)

SCHEMA = """\
id: https://example.invalid/okg/endpoint-wedge
name: endpoint_wedge
classes:
  Widget:
    attributes:
      text: {}
    annotations:
      archetype: artifact
      transient: 'false'
  Note:
    attributes:
      text: {}
    annotations:
      archetype: artifact
      transient: 'false'
  Document:
    attributes:
      text: {}
    annotations:
      archetype: artifact
      transient: 'false'
  Gadget:
    attributes:
      text: {}
    annotations:
      archetype: artifact
      transient: 'false'
"""

BRIDGE = """\
id: https://example.invalid/okg/endpoint-wedge-bridges
name: endpoint_wedge_bridges
classes:
  EdgeNarrowings:
    annotations:
      okg_edge_narrowings: 'true'
    attributes:
      note_references_widget:
        annotations:
          edge_archetype: references
          src_subtypes: Note
          dst_subtypes: Widget
"""

_SYNC = {
    "triggers": ["fs_event", "manual", "reconcile"],
    "default_event_mode": "scope_complete",
    "reconcile_mode": "scope_complete",
}


def node(node_id: str, subtype: str) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "subtype": subtype,
        "attrs": {"text": node_id},
        "source_record_id": {"id": node_id},
    }


def edge(src: str, dst: str, edge_type: str = "references") -> dict[str, Any]:
    return {"src": src, "dst": dst, "edge_type": edge_type}


def _entry(
    name: str,
    fixture: Path,
    *,
    nodes: list[tuple[str, str]],
    edges: list[tuple[str, str, str, str]] | None = None,
    extra_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entry = _source(f"svc.{name}")
    clauses: list[dict[str, Any]] = []
    signature_nodes = []
    for subtype, clause_id in nodes:
        signature_nodes.append({"subtype": subtype, "clause_id": clause_id})
        clauses.append({
            "id": clause_id, "effect": "allow",
            "record_kind": "node", "subtype": subtype,
        })
    signature_edges = []
    for src, edge_type, dst, clause_id in edges or []:
        signature_edges.append({
            "src_subtype": src, "edge_type": edge_type,
            "dst_subtype": dst, "clause_id": clause_id,
        })
        clauses.append({
            "id": clause_id, "effect": "allow",
            "record_kind": "edge", "triple": [src, edge_type, dst],
        })
    signature: dict[str, Any] = {"nodes": signature_nodes}
    if signature_edges:
        signature["edges"] = signature_edges
    policy = entry["admission_policy"]
    policy["allowed_record_kinds"] = (
        ["node", "edge"] if signature_edges else ["node"]
    )
    policy["output_signature"] = signature
    policy["policy_clauses"] = clauses
    policy["output_scope_summary"] = {
        "summary": f"{name} fixture facts",
        "nodes": [subtype for subtype, _ in nodes],
        "edges": [f"{s} {t} {d}" for s, t, d, _ in edges or []],
    }
    policy.update(extra_policy or {})
    return {
        **entry,
        "module": "okg.substrate.library.sources.bootstrap_fixture",
        "class": "BootstrapFixtureSource",
        "params": {"fixture_path": str(fixture)},
        "source_policy": _fixture_source_privacy_policy(),
        "sync": dict(_SYNC),
    }


def build_deployment(
    tmp_path: Path,
    dsn: str,
    *,
    b_policy: dict[str, Any] | None = None,
    consistency: dict[str, Any] | None = None,
    b_dst_subtype: str = "widget",
    at_most_one_target: bool = False,
    b_endpoint_absent_refuse: bool = False,
) -> Path:
    dep = _write_layered_fixture_deployment(tmp_path)
    manifest_path = dep / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text())
    # Tiny runs: the default 20 percent cap would refuse a one-edge run whose
    # only edge waits, so the fixture lifts it unless a test sets its own.
    manifest["consistency"] = (
        {"max_deferred_edge_fraction": 1.0} if consistency is None
        else consistency
    )
    manifest_path.write_text(yaml.safe_dump(manifest))
    (dep / "schemas" / "fixture.yaml").write_text(SCHEMA)
    bridges = dep / "schemas" / "bridges"
    bridges.mkdir()
    (bridges / "bridges.yaml").write_text(
        BRIDGE + ("          cardinality: at_most_one_target\n" if at_most_one_target else "")
    )
    fixtures = dep / "fixtures"
    fixtures.mkdir()
    for name in ("a", "b", "c", "d"):
        write_fixture(dep, name, [], [])
    registry_path = dep / "source_registry.yaml"
    registry = {
        "sources": {
            "a": _entry(
                "a", fixtures / "a.json",
                nodes=[("widget", "a_widget"), ("gadget", "a_gadget")],
            ),
            "b": _entry(
                "b", fixtures / "b.json",
                nodes=[("note", "b_note")],
                edges=[(
                    "note", "references", b_dst_subtype, "b_note_dst",
                )],
                extra_policy=b_policy,
            ),
            "c": _entry(
                "c", fixtures / "c.json",
                nodes=[("document", "c_document")],
            ),
            # A second producer of the same cross-source edge identity.
            "d": _entry(
                "d", fixtures / "d.json",
                nodes=[("note", "d_note")],
                edges=[("note", "references", "widget", "d_note_widget")],
            ),
        },
    }
    if b_endpoint_absent_refuse:
        for rule in registry["sources"]["b"]["admission_policy"][
            "output_signature"
        ]["edges"]:
            rule["endpoint_absent"] = "refuse"
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(dsn, dep)
    return dep


def write_fixture(
    dep: Path,
    name: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> None:
    (dep / "fixtures" / f"{name}.json").write_text(
        json.dumps({"nodes": nodes, "edges": edges}),
    )
    _commit_layered_fixture_deployment(dep, message=f"fixture {name}")


def ingest(
    dsn: str,
    dep: Path,
    *sources: str,
    publish: bool = True,
    reset_cursor: bool = False,
):
    return run_ingest(
        IngestOptions(
            dsn=dsn,
            deployment="svc",
            deployment_dir=dep,
            include=set(sources),
            include_predecessors=False,
            include_dependents=False,
            publish=publish,
            reset_cursor=reset_cursor,
        ),
    )


def result_for(outcome, source: str) -> dict[str, Any]:
    for row in outcome.report["results"]:
        if row.get("source") == f"svc.{source}" or row.get("name") == source:
            return row
    raise AssertionError(
        f"no result for {source}: {outcome.report['results']}",
    )


def graph_counts(dsn: str) -> dict[str, int]:
    """Live nodes and edges on the default branch head."""
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        checkout = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout()
        nodes = checkout.query("SELECT count(*) AS n FROM okg.graph_nodes")
        edges = checkout.query("SELECT count(*) AS n FROM okg.graph_edges")
    return {"nodes": int(nodes[0]["n"]), "edges": int(edges[0]["n"])}


def node_ids(dsn: str, source: str | None = None) -> set[str]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        checkout = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout()
        rows = checkout.query("SELECT node_id, source FROM okg.graph_nodes")
    return {
        str(row["node_id"])
        for row in rows
        if source is None or row["source"] == f"svc.{source}"
    }


def edge_triples(dsn: str) -> set[tuple[str, str, str]]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        checkout = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout()
        rows = checkout.query(
            "SELECT src, edge_type, dst FROM okg.graph_edges",
        )
    return {
        (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
        for row in rows
    }


def waiting_rows(dsn: str) -> list[dict[str, Any]]:
    """The waiting table on the default branch head (operator view)."""
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        checkout = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout()
        return [
            dict(row) for row in checkout.query(
                "SELECT * FROM okg.graph_edge_deferrals ORDER BY edge_id",
            )
        ]


def edge_rows(dsn: str) -> list[dict[str, Any]]:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.edge_deferrals import (
        EDGE_ROW_COLUMNS,
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        checkout = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout()
        return [
            {column: row.get(column) for column in EDGE_ROW_COLUMNS}
            for row in checkout.query(
                "SELECT * FROM okg.graph_edges ORDER BY edge_id",
            )
        ]


def waiting_rows_at(dsn: str, generation_id: str) -> list[dict[str, Any]]:
    """The waiting table as a published generation's checkpoint holds it."""
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        checkout = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(generation_id)
        return [
            dict(row) for row in checkout.query(
                "SELECT * FROM okg.graph_edge_deferrals ORDER BY edge_id",
            )
        ]
