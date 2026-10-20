"""PostgreSQL proof for item 92a: who owns an edge several producers write.

The code-structure source and the code_reference enricher both write the
``calls`` edge caller -> callee (and the ``references`` edge reference ->
callee). Before this change the graph kept whichever wrote last, so a
source rewrite in an incremental publish replaced the enricher's row while a
rebuild of the same inputs kept the enricher's. With ``edge_precedence``
ranking the enricher first, both store the enricher's version.

Modelled on tests/substrate/library/linkers/test_code_reference_call_site_refresh_db.py.
The database-free rule cases live in tests/substrate/test_edge_declarers.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres

DEPLOYMENT = "test-edge-precedence"
STRUCTURE = "code-structure"
ENRICHER = "_enricher:code_reference"
CALLER = "symbol:demo:tests/test_mw.py:test_json_write:88"
CALLEE = "symbol:demo:tinydb/database.py:TinyDB:20"
REFERENCE = "reference:demo:tests/test_mw.py:TinyDB:106"
RANKING = {
    "calls": [ENRICHER, "code-lsp-resolution", STRUCTURE],
    "references": [ENRICHER, "code-lsp-resolution", STRUCTURE],
}


def _rank(dsn: str, ranking: dict[str, list[str]]) -> None:
    """Copy a ranking into okg.edge_precedence and drop the declarations it
    no longer covers, as `okg migrate` and worker start do."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.edge_declarers import (
        sync_edge_precedence,
    )

    with psycopg.connect(dsn) as conn:
        sync_edge_precedence(conn, ranking)
    store = ChronosGraphStore.connect(dsn)
    try:
        store.purge_declarations_outside_ranking()
    finally:
        store.close()


def _apply_catalog(dsn: str, tmp_path: Path) -> Any:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    dep = tmp_path / "dep"
    if not dep.exists():
        dep.mkdir()
        (dep / "deployment.yaml").write_text(yaml.safe_dump({
            "name": DEPLOYMENT,
            "description": "edges several producers write",
            "modules": [
                "git_graph",
                "openspec",
                "agent_sessions",
                "compute_env",
                "ci",
                "code_graph",
            ],
        }))
        (dep / "schemas").mkdir()
    with psycopg.connect(dsn) as conn:
        apply_composed_catalog(conn, compose_catalog(dep))
    return bind_test_catalog_writer(dsn, DEPLOYMENT)


def _symbol(node_id: str, path: str, name: str, line: int):
    from okg.substrate.library.sources.base import NodeFact

    return NodeFact(
        node_id=node_id,
        subtype="code_symbol",
        attrs={
            "repo": "demo",
            "path": path,
            "kind": "function",
            "name": name,
            "qualified_name": name,
            "start_line": line,
            "end_line": line + 20,
        },
        source_record_id={"node_id": node_id},
        source_revision={"rev": 1},
    )


def _reference(name: str = "TinyDB"):
    from okg.substrate.library.sources.base import NodeFact

    return NodeFact(
        node_id=REFERENCE,
        subtype="code_reference",
        attrs={
            "repo": "demo",
            "path": "tests/test_mw.py",
            "language": "python",
            "name": name,
            "qualified_name": name,
            "reference_kind": "call",
            "enclosing_symbol_id": CALLER,
            "start_line": 106,
            "end_line": 106,
        },
        source_record_id={"node_id": REFERENCE},
        source_revision={"rev": 1},
    )


def _structure_edges(version: str, *, op: str = "I") -> list[Any]:
    """The source's own calls and references edges, version-stamped."""
    from okg.substrate.library.sources.base import EdgeFact

    return [
        EdgeFact(
            src=CALLER,
            dst=CALLEE,
            edge_type="calls",
            attrs={} if op == "R" else {"line": 106, "via": version},
            source_record_id={"edge": "calls"},
            source_revision={"rev": version},
            op=op,
        ),
        EdgeFact(
            src=REFERENCE,
            dst=CALLEE,
            edge_type="references",
            attrs={} if op == "R" else {"via": version},
            source_record_id={"edge": "references"},
            source_revision={"rev": version},
            op=op,
        ),
    ]


class _CodeStructure:
    name = "fixture"
    profile = "discovery_crawl"

    def __init__(self, facts: list[Any]) -> None:
        self.facts = facts

    def run(self, run_id: str, *, mode: str, cursor: dict[str, Any] | None = None):
        from okg.substrate.library.sources.base import SourceRun

        del run_id, mode, cursor
        return SourceRun(
            facts=iter(self.facts),
            completed_scope=True,
            run_mode="scope_complete",
        )


def _facts(*, edges: list[Any], reference_name: str = "TinyDB") -> list[Any]:
    return [
        _symbol(CALLER, "tests/test_mw.py", "test_json_write", 88),
        _symbol(CALLEE, "tinydb/database.py", "TinyDB", 20),
        _reference(reference_name),
        *edges,
    ]


def _publish(dsn: str, catalog_identity: Any, facts: list[Any]) -> dict[str, Any]:
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.linkers.code_reference import CodeReferenceResolver
    from okg.substrate.publisher import Publisher

    SourceRunner(dsn, source_name=STRUCTURE).run(
        _CodeStructure(facts),
        mode="scope_complete",
    )
    out = Publisher(
        dsn,
        DEPLOYMENT,
        enrichers=[CodeReferenceResolver()],
        catalog_identity=catalog_identity,
    ).cycle()
    assert out["status"] == "published", out
    return out


def _rows(dsn: str, generation_id: int, sql: str) -> list[dict[str, Any]]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        return [
            dict(row)
            for row in store.checkout_generation(generation_id).query(sql)
        ]
    finally:
        store.close()


def _shared_edges(dsn: str, generation_id: int) -> dict[tuple[str, str, str], Any]:
    """The stored ranked rows as a rebuild comparison sees them: everything
    but the lineage stamp and the write-time columns."""
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

    return {
        (row["src"], row["edge_type"], row["dst"]): {
            "source": row["source"],
            "provenance": row["provenance"],
            "confidence": float(row["confidence"]),
            "attrs": {
                key: value
                for key, value in dict(row["attrs"] or {}).items()
                if key != LINEAGE_ATTR
            },
        }
        for row in _rows(
            dsn,
            generation_id,
            "SELECT src, edge_type, dst, source, provenance, confidence, attrs "
            "FROM okg.graph_edges WHERE edge_type IN ('calls', 'references')",
        )
    }


def _declarers(
    dsn: str,
    generation_id: Any | None = None,
) -> dict[tuple[str, str], str]:
    """Declarations at the branch head, or as of ``generation_id``.

    A write that changes only a declaration (a lower-ranked rewrite or
    retraction) changes no stored row, so the publisher reuses the last
    generation and the declaration reaches a checkpoint with the next publish
    that changes the graph; the writers read the branch head.
    """
    sql = "SELECT edge_type, source, attrs FROM okg.graph_edge_declarers"
    rows = _live(dsn, sql) if generation_id is None else _rows(
        dsn, generation_id, sql,
    )
    return {
        (row["edge_type"], row["source"]): str(
            dict(row["attrs"] or {}).get("via") or "derived"
        )
        for row in rows
    }


def test_source_rewrite_keeps_enricher_row_and_matches_rebuild(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Steps 1-3 of the design note: the enricher's row survives a source
    rewrite whose enricher inputs stay clean, and the incremental graph's
    shared rows equal a rebuild of the same final inputs."""
    from tests.substrate.conftest import reset_okg_test_runtime

    catalog_identity = _apply_catalog(clean_runtime, tmp_path)
    _rank(clean_runtime, RANKING)

    # 1. The source writes the edges and the enricher derives the same ones.
    first = _publish(
        clean_runtime, catalog_identity, _facts(edges=_structure_edges("v1")),
    )
    rows = _shared_edges(clean_runtime, first["generation_id"])
    assert rows[(CALLER, "calls", CALLEE)]["source"] == ENRICHER
    assert rows[(REFERENCE, "references", CALLEE)]["source"] == ENRICHER

    # 2. The source rewrites both edges; nothing the enricher reads changed,
    #    so the enricher does not run. The source's rewrite must not take the
    #    rows over: it only changes the source's own declaration.
    second = _publish(
        clean_runtime, catalog_identity, _facts(edges=_structure_edges("v2")),
    )
    incremental = _shared_edges(clean_runtime, second["generation_id"])
    assert incremental[(CALLER, "calls", CALLEE)]["source"] == ENRICHER
    assert incremental[(REFERENCE, "references", CALLEE)]["source"] == ENRICHER
    assert _declarers(clean_runtime) == {
        ("calls", ENRICHER): "derived",
        ("calls", STRUCTURE): "v2",
        ("references", ENRICHER): "derived",
        ("references", STRUCTURE): "v2",
    }
    # As of the first generation the source still declared v1.
    assert _declarers(clean_runtime, first["generation_id"])[
        ("calls", STRUCTURE)
    ] == "v1"

    # 3. Rebuild the same final inputs into a fresh database.
    reset_okg_test_runtime(clean_runtime)
    catalog_identity = _apply_catalog(clean_runtime, tmp_path)
    _rank(clean_runtime, RANKING)
    rebuild = _publish(
        clean_runtime, catalog_identity, _facts(edges=_structure_edges("v2")),
    )
    assert _shared_edges(clean_runtime, rebuild["generation_id"]) == incremental


def test_ownership_moves_and_lower_rank_is_inert(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Step 4: a lower-ranked retraction leaves the winner's row; the
    winner's withdrawal hands the row to the next declarer; the last
    declarer's retraction deletes it."""
    catalog_identity = _apply_catalog(clean_runtime, tmp_path)
    _rank(clean_runtime, RANKING)
    _publish(clean_runtime, catalog_identity, _facts(edges=_structure_edges("v1")))

    # The source retracts its edges: the enricher still declares them.
    retracted = _publish(
        clean_runtime,
        catalog_identity,
        _facts(edges=_structure_edges("v1", op="R")),
    )
    rows = _shared_edges(clean_runtime, retracted["generation_id"])
    assert rows[(CALLER, "calls", CALLEE)]["source"] == ENRICHER
    assert set(_declarers(clean_runtime)) == {
        ("calls", ENRICHER),
        ("references", ENRICHER),
    }

    # The source asserts them again, then the reference stops resolving: the
    # enricher retracts its own declarations and the rows pass to the source.
    _publish(clean_runtime, catalog_identity, _facts(edges=_structure_edges("v3")))
    handed = _publish(
        clean_runtime,
        catalog_identity,
        _facts(edges=_structure_edges("v3"), reference_name="NoSuchSymbol"),
    )
    rows = _shared_edges(clean_runtime, handed["generation_id"])
    assert rows[(CALLER, "calls", CALLEE)] == {
        "source": STRUCTURE,
        "provenance": "explicit",
        "confidence": 1.0,
        "attrs": {"line": 106, "via": "v3"},
    }
    assert rows[(REFERENCE, "references", CALLEE)]["source"] == STRUCTURE
    assert set(_declarers(clean_runtime, handed["generation_id"])) == {
        ("calls", STRUCTURE),
        ("references", STRUCTURE),
    }

    # The last declarer retracts: the rows go.
    gone = _publish(
        clean_runtime,
        catalog_identity,
        _facts(edges=_structure_edges("v3", op="R"), reference_name="NoSuchSymbol"),
    )
    assert _shared_edges(clean_runtime, gone["generation_id"]) == {}
    assert _declarers(clean_runtime, gone["generation_id"]) == {}


def test_enricher_declares_over_an_identical_lower_ranked_row(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Design step 5: the enricher compares with its own declaration, not
    the live row, so it declares the edge even when a lower-ranked producer
    already stored exactly the attributes it derives."""
    from okg.substrate.library.sources.base import EdgeFact

    catalog_identity = _apply_catalog(clean_runtime, tmp_path)
    _rank(clean_runtime, RANKING)
    derived = {
        "line": 106,
        "linker": "code_reference",
        "reference_id": REFERENCE,
        "resolution": "unique_code_symbol",
        "resolution_method": "heuristic",
    }
    same_as_enricher = EdgeFact(
        src=CALLER,
        dst=CALLEE,
        edge_type="calls",
        attrs=dict(derived),
        source_record_id={"edge": "calls"},
        source_revision={"rev": "copy"},
    )
    out = _publish(
        clean_runtime, catalog_identity, _facts(edges=[same_as_enricher]),
    )
    row = _shared_edges(clean_runtime, out["generation_id"])[
        (CALLER, "calls", CALLEE)
    ]
    assert row["attrs"] == derived
    assert row["source"] == ENRICHER
    assert ("calls", ENRICHER) in _declarers(clean_runtime)


def _apply(dsn: str, facts: list[Any], *, source: str):
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn)
    try:
        return store.apply_facts(facts, source=source, strict_endpoints=False)
    finally:
        store.close()


def _live(dsn: str, sql: str) -> list[dict[str, Any]]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        return [dict(row) for row in store.query(sql)]
    finally:
        store.close()


def test_write_time_refusals_purge_and_unranked_types(
    clean_runtime: str,
) -> None:
    """An unranked collision is refused naming both producers; a deleted
    node takes its ranked edges' declarations with it; an unranked edge type
    keeps last-writer-wins and records no declarer."""
    from okg.substrate.graph_store_internal.edge_declarers import (
        EdgePrecedenceUnranked,
    )
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    _rank(clean_runtime, {"calls": [ENRICHER, STRUCTURE]})
    symbols = [
        _symbol(CALLER, "tests/test_mw.py", "test_json_write", 88),
        _symbol(CALLEE, "tinydb/database.py", "TinyDB", 20),
    ]
    calls = EdgeFact(src=CALLER, dst=CALLEE, edge_type="calls", attrs={"via": "s"})
    _apply(clean_runtime, [*symbols, calls], source=STRUCTURE)

    # A producer the ranking does not name declares the same edge.
    with pytest.raises(EdgePrecedenceUnranked) as refused:
        _apply(
            clean_runtime,
            [EdgeFact(src=CALLER, dst=CALLEE, edge_type="calls", attrs={"via": "x"})],
            source="other-producer",
        )
    assert refused.value.data["unranked_sources"] == ["other-producer"]
    assert refused.value.data["declaring_sources"] == [
        STRUCTURE,
        "other-producer",
    ]
    stored = _live(
        clean_runtime,
        "SELECT source, attrs FROM okg.graph_edges WHERE edge_type = 'calls'",
    )
    assert [(row["source"], row["attrs"].get("via")) for row in stored] == [
        (STRUCTURE, "s"),
    ]

    # An unranked edge type: last writer wins, no declarer rows.
    member = {"src": CALLER, "dst": CALLEE, "edge_type": "member_of"}
    _apply(clean_runtime, [EdgeFact(**member, attrs={"via": "a"})], source="a")
    _apply(clean_runtime, [EdgeFact(**member, attrs={"via": "b"})], source="b")
    assert [
        (row["source"], row["attrs"].get("via"))
        for row in _live(
            clean_runtime,
            "SELECT source, attrs FROM okg.graph_edges "
            "WHERE edge_type = 'member_of'",
        )
    ] == [("b", "b")]
    assert _live(
        clean_runtime,
        "SELECT source FROM okg.graph_edge_declarers "
        "WHERE edge_type = 'member_of'",
    ) == []

    # The enricher declares the calls edge too; then the source deletes the
    # callee node it owns. Every declaration of the incident ranked edge
    # goes with the node, whoever made it.
    _apply(
        clean_runtime,
        [EdgeFact(
            src=CALLER, dst=CALLEE, edge_type="calls", attrs={"via": "e"},
            provenance="derived_deterministic",
        )],
        source=ENRICHER,
    )
    assert {
        row["source"]
        for row in _live(
            clean_runtime, "SELECT source FROM okg.graph_edge_declarers",
        )
    } == {ENRICHER, STRUCTURE}
    _apply(
        clean_runtime,
        [NodeFact(
            node_id=CALLEE,
            subtype="code_symbol",
            attrs={},
            source_record_id={"node_id": CALLEE},
            source_revision={"rev": 1},
            op="R",
        )],
        source=STRUCTURE,
    )
    assert _live(
        clean_runtime,
        "SELECT edge_id FROM okg.graph_edges WHERE edge_type = 'calls'",
    ) == []
    assert _live(
        clean_runtime, "SELECT source FROM okg.graph_edge_declarers",
    ) == []


def _edge(src: str, dst: str, via: str, *, op: str = "I"):
    from okg.substrate.library.sources.base import EdgeFact

    return EdgeFact(
        src=src,
        dst=dst,
        edge_type="calls",
        attrs={} if op == "R" else {"via": via},
        source_record_id={"edge": f"{src}->{dst}"},
        source_revision={"rev": via},
        op=op,
    )


def _node(node_id: str, *, op: str = "I"):
    from okg.substrate.library.sources.base import NodeFact

    return NodeFact(
        node_id=node_id,
        subtype="code_symbol",
        attrs={} if op == "R" else {"name": node_id},
        source_record_id={"node_id": node_id},
        source_revision={"rev": 1},
        op=op,
    )


def _ranked_state(store: Any, branch: str) -> dict[str, list[dict[str, Any]]]:
    return {
        table: [
            dict(row)
            for row in store.query(
                f"SELECT * FROM okg.{table} ORDER BY {order}", branch=branch,
            )
        ]
        for table, order in (
            ("graph_nodes", "node_id"),
            ("graph_edges", "edge_id"),
            ("graph_edge_declarers", "edge_id, source"),
        )
    }


def test_failed_run_undo_restores_ranked_edges_and_their_declarations(
    clean_runtime: str,
) -> None:
    """A failed run of a ranked source is undone to its run base: its
    rewritten declaration, a winning row it withdrew (handed to the next
    rank), and the declarations its node delete purged all come back."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest.compensation import retract_chronos_run
    from okg.substrate.ingest.run_base import (
        hold_run_liveness,
        run_base_checkpoint_id,
        take_run_base,
    )

    other = "other-producer"
    _rank(clean_runtime, {"calls": [ENRICHER, STRUCTURE, other]})
    store = ChronosGraphStore.connect(clean_runtime)
    liveness = None
    try:
        a, b, d, e = "sym:a", "sym:b", "sym:d", "sym:e"
        # Before the run: the enricher wins a->b and a->e, the source wins
        # a->d over the other producer.
        store.apply_facts(
            [_node(a), _node(b), _node(d), _node(e),
             _edge(a, b, "s1"), _edge(a, d, "s1"), _edge(a, e, "s1")],
            source=STRUCTURE, run_id="good-run", strict_endpoints=False,
        )
        store.apply_facts(
            [_edge(a, b, "e1"), _edge(a, e, "e1")],
            source=ENRICHER, run_id="enricher-run", strict_endpoints=False,
        )
        store.apply_facts(
            [_edge(a, d, "o1")],
            source=other, run_id="other-run", strict_endpoints=False,
        )
        store.checkpoint_generation(1, deployment_name=DEPLOYMENT)
        store.create_branch("expected", from_branch="default")
        before = _ranked_state(store, "default")

        checkpoint_id = run_base_checkpoint_id("default", "bad-run")
        liveness = hold_run_liveness(clean_runtime, checkpoint_id)
        assert take_run_base(
            store, deployment=DEPLOYMENT, branch="default",
            source=STRUCTURE, run_id="bad-run",
        ) is not None
        store.apply_facts(
            [_edge(a, b, "s2"), _edge(a, d, "s1", op="R"), _node(e, op="R")],
            source=STRUCTURE, run_id="bad-run", strict_endpoints=False,
        )
        damaged = _ranked_state(store, "default")
        owners = {row["dst"]: row["source"] for row in damaged["graph_edges"]}
        assert owners == {b: ENRICHER, d: other}  # a->d handed off, a->e gone
        assert damaged != before

        stats = retract_chronos_run(
            clean_runtime, admission_id=None, source=STRUCTURE, run_id="bad-run",
        )
        assert stats["mode"] == "undo_to_run_base", stats
        assert _ranked_state(store, "default") == _ranked_state(store, "expected")
        assert _ranked_state(store, "default") == before
    finally:
        if liveness is not None:
            liveness.close()
        store.close()


def test_declarations_merge_back_with_the_graph_rows(clean_runtime: str) -> None:
    """The declarer table is a registered Chronos table: a branch's
    declarations merge back with its edge rows."""
    from okg.substrate.graph_store import ChronosGraphStore

    _rank(clean_runtime, RANKING)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.apply_facts(
            [_node("sym:a"), _node("sym:b"), _edge("sym:a", "sym:b", "s1")],
            source=STRUCTURE, strict_endpoints=False,
        )
        store.create_branch("staging:m1", from_branch="default")
        store.create_branch("experiment:f", from_branch="staging:m1")
        store.apply_facts(
            [_edge("sym:a", "sym:b", "e1")],
            source=ENRICHER, strict_endpoints=False, branch="experiment:f",
        )
        assert {
            row["source"]
            for row in store.query(
                "SELECT source FROM okg.graph_edge_declarers",
                branch="staging:m1",
            )
        } == {STRUCTURE}
        store.merge("experiment:f", "staging:m1")
        assert {
            row["source"]
            for row in store.query(
                "SELECT source FROM okg.graph_edge_declarers",
                branch="staging:m1",
            )
        } == {STRUCTURE, ENRICHER}
        assert [
            row["source"]
            for row in store.query(
                "SELECT source FROM okg.graph_edges WHERE edge_type = 'calls'",
                branch="staging:m1",
            )
        ] == [ENRICHER]
    finally:
        store.close()


def test_a_ranked_source_name_also_ranks_its_ownership_id(
    clean_runtime: str,
) -> None:
    """The ingest service writes a source's rows under its ownership id;
    the copied ranking names both identities at the source's rank."""
    from okg.substrate.graph_store_internal.edge_declarers import (
        load_edge_precedence,
        sync_edge_precedence,
    )

    owned = "demo.code-structure"
    with psycopg.connect(clean_runtime) as conn:
        sync_edge_precedence(
            conn,
            {"calls": [ENRICHER, STRUCTURE]},
            source_identities={STRUCTURE: owned},
        )
        ranks = load_edge_precedence(conn)
    assert ranks == {"calls": {ENRICHER: 0, STRUCTURE: 1, owned: 1}}
    _apply(
        clean_runtime,
        [_node("sym:a"), _node("sym:b"), _edge("sym:a", "sym:b", "s1")],
        source=owned,
    )
    _apply(clean_runtime, [_edge("sym:a", "sym:b", "e1")], source=ENRICHER)
    assert [
        row["source"]
        for row in _live(
            clean_runtime,
            "SELECT source FROM okg.graph_edges WHERE edge_type = 'calls'",
        )
    ] == [ENRICHER]
