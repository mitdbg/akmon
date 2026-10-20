"""M5.4 — ontology reconcile flow (narrowings only).

Covers:
  - classify_narrowings: pure diff of two triple sets
  - count_violators: SQL count against edges_live + nodes_live
  - emit_plan_yaml / load_plan: YAML round-trip + action validation
  - apply_plan: retract emits retraction facts bounded by
    violator count; catalog_versions bumps; aborted actions
    leave data untouched

Deferred (per M5.4 commit notes):
  - rename (structural) action
  - LinkML/alias/invariant diffs
"""
from __future__ import annotations

import uuid
from pathlib import Path

import psycopg
import pytest

from okg.substrate.catalog.reconcile import (
    CatalogDiff,
    NarrowingChange,
    apply_plan,
    classify_narrowings,
    count_violators,
    emit_plan_yaml,
    load_plan,
)
from okg.substrate.errors import SubstrateError
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


# ---------- diff classifier (pure) ----------

def test_classify_all_additive() -> None:
    diff = classify_narrowings(
        current=[("a", "r", "b")],
        proposed=[("a", "r", "b"), ("c", "r", "d")],
    )
    assert len(diff.changes) == 1
    assert diff.changes[0].kind == "additive"
    assert diff.changes[0].parent == "c"
    assert diff.is_additive_only is True


def test_classify_mixed() -> None:
    diff = classify_narrowings(
        current=[("a", "r", "b"), ("x", "r", "y")],
        proposed=[("a", "r", "b"), ("c", "r", "d")],
    )
    kinds = sorted((c.kind, c.parent) for c in diff.changes)
    assert kinds == [("additive", "c"), ("restrictive", "x")]


def test_classify_noop_diff_has_zero_changes() -> None:
    diff = classify_narrowings(
        current=[("a", "r", "b")],
        proposed=[("a", "r", "b")],
    )
    assert diff.changes == []


def test_classify_existing_triple_cardinality_as_restrictive_annotation() -> None:
    triple = ("bib_reference", "resolves_to", "paper")
    diff = classify_narrowings(
        current=[triple],
        proposed=[triple],
        current_attr_rules={triple: {}},
        proposed_attr_rules={
            triple: {"cardinality": "at_most_one_target"},
        },
    )
    assert len(diff.changes) == 1
    assert diff.changes[0].kind == "annotation_restrictive"


def test_classify_cardinality_removal_as_explicit_widening() -> None:
    triple = ("bib_reference", "resolves_to", "paper")
    diff = classify_narrowings(
        current=[triple],
        proposed=[triple],
        current_attr_rules={triple: {"cardinality": "at_most_one_target"}},
        proposed_attr_rules={triple: {}},
    )
    assert [change.kind for change in diff.changes] == ["annotation_widening"]


def test_classify_rejects_child_triples_with_conflicting_cardinality() -> None:
    a = ("page", "links", "concept")
    b = ("page", "links", "ticket")
    with pytest.raises(SubstrateError, match="must agree on cardinality"):
        classify_narrowings(
            current=[a, b],
            proposed=[a, b],
            proposed_attr_rules={
                a: {"cardinality": "at_most_one_target"},
                b: {},
            },
        )


# ---------- violator counting (DB) ----------

def _seed_chronos_graph(
    conn: psycopg.Connection,
    nodes: list[tuple[str, str]],
    edges: list[tuple[str, str, str]],
) -> None:
    """Write the same nodes/edges into the Chronos graph.

    Chronos cutover: `count_violators` reads okg.graph_nodes /
    okg.graph_edges via a Chronos checkout, not okg.nodes_live /
    okg.edges_live. Mirror the live-table seed into the Chronos
    graph (and bootstrap the empty branch even when there is
    nothing to write) so the violator count sees the rows.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    store = ChronosGraphStore.from_connection(conn)
    try:
        facts: list[NodeFact | EdgeFact] = [
            NodeFact(nid, subtype, {}, {"id": nid}, {"v": 1})
            for nid, subtype in nodes
        ]
        facts += [
            EdgeFact(
                src, dst, etype,
                source_record_id={"id": f"{src}->{dst}"},
                source_revision={"v": 1},
            )
            for src, etype, dst in edges
        ]
        if facts:
            store.apply_facts(facts, source="default", strict_endpoints=True)
    finally:
        store.close()


def test_count_violators_empty(clean_runtime: str) -> None:
    diff = CatalogDiff(changes=[
        NarrowingChange("restrictive", "page", "refs", "ticket"),
    ])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_chronos_graph(conn, nodes=[], edges=[])
        resolved = count_violators(conn, diff)
    assert resolved.changes[0].violator_count == 0


def test_count_violators_live_rows(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        nodes = [("n:a", "page"), ("n:b", "concept"), ("n:c", "ticket")]
        edges = [("n:a", "refs", "n:b"), ("n:a", "refs", "n:c")]
        _seed_chronos_graph(conn, nodes=nodes, edges=edges)
        diff = CatalogDiff(changes=[
            NarrowingChange("restrictive", "page", "refs", "concept"),
            NarrowingChange("restrictive", "page", "refs", "ticket"),
            NarrowingChange("restrictive", "page", "refs", "absent"),
        ])
        resolved = count_violators(conn, diff)
    counts = {c.child: c.violator_count for c in resolved.changes}
    assert counts["concept"] == 1
    assert counts["ticket"] == 1
    assert counts["absent"] == 0


def test_cardinality_violator_count_includes_descendant_sources(
    clean_runtime: str,
) -> None:
    triple = ("page", "links", "concept")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            cv = int(cur.fetchone()[0])
            cur.executemany(
                "INSERT INTO okg.subtype_ancestors "
                "(catalog_version_id, subtype_name, ancestor_name, depth) "
                "VALUES (%s, %s, %s, %s)",
                [
                    (cv, "page", "page", 0),
                    (cv, "special_page", "special_page", 0),
                    (cv, "special_page", "page", 1),
                ],
            )
        _seed_chronos_graph(
            conn,
            nodes=[
                ("page:special", "special_page"),
                ("concept:a", "concept"),
                ("concept:b", "concept"),
            ],
            edges=[
                ("page:special", "links", "concept:a"),
                ("page:special", "links", "concept:b"),
            ],
        )
        diff = count_violators(conn, classify_narrowings(
            current=[triple],
            proposed=[triple],
            current_attr_rules={triple: {}},
            proposed_attr_rules={
                triple: {"cardinality": "at_most_one_target"},
            },
        ))
        assert diff.changes[0].violator_count == 1


# ---------- plan emit / load ----------

def test_emit_and_load_plan_roundtrip(tmp_path: Path) -> None:
    diff = CatalogDiff(changes=[
        NarrowingChange("additive", "a", "r", "b"),
        NarrowingChange("restrictive", "x", "r", "y", violator_count=17),
    ])
    p = tmp_path / "plan.yaml"
    emit_plan_yaml(diff, p)
    entries = load_plan(p)
    assert len(entries) == 2
    # Additive → action: apply (auto)
    assert entries[0]["change"] == "add_narrowing"
    assert entries[0]["action"] == "apply"
    # Restrictive → action: abort (safe default)
    assert entries[1]["change"] == "remove_narrowing"
    assert entries[1]["action"] == "abort"
    assert entries[1]["violators"] == 17


def test_load_plan_rejects_bad_action(tmp_path: Path) -> None:
    p = tmp_path / "plan.yaml"
    p.write_text(
        "reconcile:\n"
        "  - change: add_narrowing\n"
        "    parent: a\n    edge: r\n    child: b\n"
        "    action: do_whatever\n"
    )
    with pytest.raises(SubstrateError, match="invalid action"):
        load_plan(p)


# ---------- apply: additive only ----------

def test_apply_additive_only_bumps_catalog(clean_runtime: str) -> None:
    """An additive-only plan bumps catalog_version, copies the
    narrowings forward, adds the new triple. No retractions."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Seed current cv with one narrowing.
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            cv_old = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO okg.narrowings "
                "  (parent_subtype, edge_type, child_subtype, "
                "   catalog_version_id) "
                "VALUES ('a', 'r', 'b', %s)",
                (cv_old,),
            )
        diff = CatalogDiff(changes=[
            NarrowingChange("additive", "c", "r", "d"),
        ])
        plan = [{
            "change": "add_narrowing",
            "parent": "c", "edge": "r", "child": "d",
            "action": "apply",
        }]
        result = apply_plan(
            conn, diff=diff, plan=plan,
            current_catalog_version_id=cv_old,
        )
    assert result.new_catalog_version_id > cv_old
    assert result.retractions_emitted == 0
    assert len(result.applied) == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT parent_subtype, edge_type, child_subtype "
            "FROM okg.narrowings "
            "WHERE catalog_version_id = %s ORDER BY parent_subtype",
            (result.new_catalog_version_id,),
        )
        rows = cur.fetchall()
    assert rows == [("a", "r", "b"), ("c", "r", "d")]


def test_annotation_reconcile_refuses_violators_then_stamps_after_disposition(
    clean_runtime: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact

    triple = ("bib_reference", "resolves_to", "paper")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_chronos_graph(
            conn,
            nodes=[
                ("bib:one", "bib_reference"),
                ("paper:a", "paper"),
                ("paper:b", "paper"),
            ],
            edges=[
                ("bib:one", "resolves_to", "paper:a"),
                ("bib:one", "resolves_to", "paper:b"),
            ],
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            cv_old = int(cur.fetchone()[0])
            cur.execute(
                "INSERT INTO okg.narrowings "
                "(parent_subtype, edge_type, child_subtype, attr_rules, "
                " catalog_version_id) VALUES (%s, %s, %s, '{}'::jsonb, %s)",
                (*triple, cv_old),
            )

        proposed = {"cardinality": "at_most_one_target"}
        diff = count_violators(conn, classify_narrowings(
            current=[triple],
            proposed=[triple],
            current_attr_rules={triple: {}},
            proposed_attr_rules={triple: proposed},
        ))
        assert diff.changes[0].violator_count == 1
        plan = [{
            "change": "annotate_narrowing",
            "parent": triple[0],
            "edge": triple[1],
            "child": triple[2],
            "violators": 1,
            "attr_rules": proposed,
            "action": "apply",
            "disposition": "operator_removed_duplicate_target",
        }]
        with pytest.raises(SubstrateError, match="multi-target groups remain"):
            apply_plan(
                conn,
                diff=diff,
                plan=plan,
                current_catalog_version_id=cv_old,
                attr_rules_to_add={triple: proposed},
            )

        # Explicit operator disposition: remove the selected duplicate. The
        # reconcile path itself never chooses a survivor.
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [EdgeFact(
                "bib:one", "paper:b", "resolves_to",
                source_record_id={"disposition": "operator"},
                source_revision={"v": 2},
                op="R",
            )],
            source=None,
            strict_endpoints=False,
        )
        result = apply_plan(
            conn,
            diff=diff,
            plan=plan,
            current_catalog_version_id=cv_old,
            attr_rules_to_add={triple: proposed},
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT attr_rules->>'cardinality' FROM okg.narrowings "
                "WHERE catalog_version_id = %s AND parent_subtype = %s "
                "AND edge_type = %s AND child_subtype = %s",
                (result.new_catalog_version_id, *triple),
            )
            assert cur.fetchone()[0] == "at_most_one_target"


def test_annotation_reconcile_rejects_partial_pair_disposition(
    clean_runtime: str,
) -> None:
    changes = [
        NarrowingChange(
            "annotation_restrictive", "page", "links", child,
            proposed_attr_rules={"cardinality": "at_most_one_target"},
        )
        for child in ("concept", "ticket")
    ]
    plan = [
        {
            "change": "annotate_narrowing",
            "parent": "page", "edge": "links", "child": "concept",
            "action": "apply",
        },
        {
            "change": "annotate_narrowing",
            "parent": "page", "edge": "links", "child": "ticket",
            "action": "abort",
        },
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(SubstrateError, match="whole parent/edge pair"):
            apply_plan(
                conn,
                diff=CatalogDiff(changes),
                plan=plan,
                current_catalog_version_id=1,
            )


def test_annotation_widening_updates_catalog_pair(clean_runtime: str) -> None:
    triple = ("page", "links", "concept")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            cv = int(cur.fetchone()[0])
            cur.execute(
                "INSERT INTO okg.narrowings "
                "(parent_subtype, edge_type, child_subtype, attr_rules, "
                "catalog_version_id) VALUES (%s,%s,%s,%s::jsonb,%s)",
                (*triple, '{"cardinality":"at_most_one_target"}', cv),
            )
        diff = classify_narrowings(
            current=[triple], proposed=[triple],
            current_attr_rules={
                triple: {"cardinality": "at_most_one_target"},
            },
            proposed_attr_rules={triple: {}},
        )
        result = apply_plan(
            conn,
            diff=diff,
            plan=[{
                "change": "widen_narrowing_annotation",
                "parent": triple[0], "edge": triple[1], "child": triple[2],
                "action": "keep_with_widened_narrowing",
            }],
            current_catalog_version_id=cv,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT attr_rules FROM okg.narrowings "
                "WHERE catalog_version_id = %s",
                (result.new_catalog_version_id,),
            )
            assert cur.fetchone()[0] == {}


# ---------- apply: retract emits bounded retractions ----------

def test_apply_retract_emits_proportional_facts(
    clean_runtime: str,
) -> None:
    """Core M5.4 verification (Chronos cutover): retract deletes
    exactly N violating graph edges for N violators, leaving the
    legitimate edges intact. Work is proportional to violators,
    not graph size."""
    from okg.substrate.graph_store import ChronosGraphStore

    # Seed: 100 nodes total, 17 of which form violating edges.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # A graph with 50 pages, 30 concepts, 20 tickets; 17
        # violating (page, refs, ticket) edges; otherwise
        # unrelated edges.
        nodes = (
            [(f"n:p{i}", "page") for i in range(50)]
            + [(f"n:c{i}", "concept") for i in range(30)]
            + [(f"n:t{i}", "ticket") for i in range(20)]
        )
        edges = (
            # 17 violating edges
            [(f"n:p{i}", "refs", f"n:t{i}") for i in range(17)]
            # 25 unrelated page→concept edges (legitimate)
            + [(f"n:p{i}", "refs", f"n:c{i}") for i in range(25)]
        )
        _seed_chronos_graph(conn, nodes=nodes, edges=edges)

        # Seed catalog: (page, refs, ticket) is currently
        # allowed; we want to remove it.
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions "
                "  (status, applied_at) VALUES ('applied', now()) "
                "RETURNING catalog_version_id"
            )
            cv_old = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO okg.narrowings "
                "  (parent_subtype, edge_type, child_subtype, "
                "   catalog_version_id) "
                "VALUES ('page', 'refs', 'ticket', %s), "
                "       ('page', 'refs', 'concept', %s)",
                (cv_old, cv_old),
            )

        diff = CatalogDiff(changes=[
            NarrowingChange(
                "restrictive", "page", "refs", "ticket",
                violator_count=17,
            ),
        ])
        plan = [{
            "change": "remove_narrowing",
            "parent": "page", "edge": "refs", "child": "ticket",
            "violators": 17,
            "action": "retract",
        }]
        result = apply_plan(
            conn, diff=diff, plan=plan,
            current_catalog_version_id=cv_old,
        )
    assert result.retractions_emitted == 17

    # Confirm at the graph level: the 17 violating page->ticket edges
    # are gone; the 25 legitimate page->concept edges remain.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout()
        ticket_rows = graph.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_edges el
              JOIN okg.graph_nodes d ON d.node_id = el.dst
             WHERE el.edge_type = 'refs' AND d.subtype = 'ticket'
            """
        )
        assert int(ticket_rows[0]["n"]) == 0
        concept_rows = graph.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_edges el
              JOIN okg.graph_nodes d ON d.node_id = el.dst
             WHERE el.edge_type = 'refs' AND d.subtype = 'concept'
            """
        )
        assert int(concept_rows[0]["n"]) == 25

        # New catalog has only (page, refs, concept); the
        # restrictive triple is gone.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT parent_subtype, edge_type, child_subtype "
                "FROM okg.narrowings "
                "WHERE catalog_version_id = %s ORDER BY child_subtype",
                (result.new_catalog_version_id,),
            )
            rows = cur.fetchall()
    assert rows == [("page", "refs", "concept")]


def test_apply_retract_deletes_cross_source_but_stays_scoped(
    clean_runtime: str,
) -> None:
    """Regression (P0 silent-failure): catalog-driven narrowing
    removal must retract violating edges regardless of which source
    wrote them — the violators come from ordinary upstream sources
    (e.g. 'default'), not from '_reconcile'. A source-scoped delete
    matched the '_reconcile' source and silently retracted ZERO
    edges, so the narrowing-removal claimed success while the
    violating edges survived in the graph.

    This test seeds violating (page, refs, ticket) edges from a
    non-_reconcile source AND an unrelated (page, mentions, concept)
    edge from a *different* source, then asserts the violating edges
    ARE retracted while the unrelated edge is PRESERVED — proving the
    cross-source delete stays scoped to the retracted triple.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Seed nodes + the 17 violating edges from source 'default'.
        nodes = (
            [(f"n:p{i}", "page") for i in range(20)]
            + [(f"n:t{i}", "ticket") for i in range(20)]
            + [("n:cc", "concept")]
        )
        violating = [(f"n:p{i}", "refs", f"n:t{i}") for i in range(17)]
        _seed_chronos_graph(conn, nodes=nodes, edges=violating)

        # Seed ONE unrelated edge from a *different* source on a
        # different edge_type (page -mentions-> concept). It shares the
        # 'page' parent subtype but is neither the retracted edge_type
        # nor child_subtype, and was written by another source. It MUST
        # survive a correctly-scoped cross-source retraction.
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        try:
            store.apply_facts(
                [
                    NodeFact("n:p0", "page", {}, {"id": "n:p0"}, {"v": 1}),
                    NodeFact("n:cc", "concept", {}, {"id": "n:cc"}, {"v": 1}),
                    EdgeFact(
                        "n:p0", "n:cc", "mentions",
                        source_record_id={"id": "n:p0->n:cc"},
                        source_revision={"v": 1},
                    ),
                ],
                source="other_source",
                strict_endpoints=False,
            )
        finally:
            store.close()

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions "
                "  (status, applied_at) VALUES ('applied', now()) "
                "RETURNING catalog_version_id"
            )
            cv_old = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO okg.narrowings "
                "  (parent_subtype, edge_type, child_subtype, "
                "   catalog_version_id) "
                "VALUES ('page', 'refs', 'ticket', %s)",
                (cv_old,),
            )

        diff = CatalogDiff(changes=[
            NarrowingChange(
                "restrictive", "page", "refs", "ticket",
                violator_count=17,
            ),
        ])
        plan = [{
            "change": "remove_narrowing",
            "parent": "page", "edge": "refs", "child": "ticket",
            "violators": 17,
            "action": "retract",
        }]
        result = apply_plan(
            conn, diff=diff, plan=plan,
            current_catalog_version_id=cv_old,
        )

    # The 17 'default'-sourced violating edges ARE retracted even
    # though '_reconcile' never wrote them.
    assert result.retractions_emitted == 17

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout()
        ticket_rows = graph.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_edges el
              JOIN okg.graph_nodes d ON d.node_id = el.dst
             WHERE el.edge_type = 'refs' AND d.subtype = 'ticket'
            """
        )
        assert int(ticket_rows[0]["n"]) == 0
        # overDeleteCheck: the unrelated 'other_source' edge of a
        # different edge_type survives — the cross-source delete did
        # NOT over-broaden to unrelated edge types or sources.
        mentions_rows = graph.query(
            "SELECT count(*) AS n FROM okg.graph_edges "
            "WHERE edge_type = 'mentions'"
        )
        assert int(mentions_rows[0]["n"]) == 1


def test_apply_abort_leaves_data_untouched(clean_runtime: str) -> None:
    """An 'abort' entry skips the change: no retractions, no
    narrowing drop. Catalog version still bumps (records the
    abort in its reconcile_plan)."""
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_chronos_graph(
            conn,
            nodes=[("n:a", "page"), ("n:b", "ticket")],
            edges=[("n:a", "refs", "n:b")],
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            cv_old = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO okg.narrowings "
                "  (parent_subtype, edge_type, child_subtype, "
                "   catalog_version_id) "
                "VALUES ('page', 'refs', 'ticket', %s)",
                (cv_old,),
            )

        diff = CatalogDiff(changes=[
            NarrowingChange(
                "restrictive", "page", "refs", "ticket",
                violator_count=1,
            ),
        ])
        plan = [{
            "change": "remove_narrowing",
            "parent": "page", "edge": "refs", "child": "ticket",
            "action": "abort",
        }]
        result = apply_plan(
            conn, diff=diff, plan=plan,
            current_catalog_version_id=cv_old,
        )
    assert result.retractions_emitted == 0
    assert len(result.aborted) == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # The violating edge is untouched (abort emitted no retraction).
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout()
        edge_rows = graph.query(
            "SELECT count(*) AS n FROM okg.graph_edges WHERE edge_type='refs'"
        )
        assert int(edge_rows[0]["n"]) == 1
        # The restrictive narrowing STAYS at the new cv (abort
        # means "don't drop it").
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.narrowings "
                "WHERE catalog_version_id = %s "
                "  AND parent_subtype='page' AND edge_type='refs' "
                "  AND child_subtype='ticket'",
                (result.new_catalog_version_id,),
            )
            assert cur.fetchone()[0] == 1


def test_subsequent_publish_uses_new_catalog_version(
    clean_runtime: str,
) -> None:
    """After apply_plan bumps the catalog, the next
    Publisher.cycle() picks up the new catalog_version_id on
    its reserved generation (since `reserve` reads
    MAX(catalog_version_id) WHERE status='applied')."""
    from okg.substrate.publisher import Publisher

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            cv_old = cur.fetchone()[0]

        # Reconcile an additive-only plan (cheapest).
        diff = CatalogDiff(changes=[
            NarrowingChange("additive", "page", "refs", "concept"),
        ])
        plan = [{
            "change": "add_narrowing",
            "parent": "page", "edge": "refs", "child": "concept",
            "action": "apply",
        }]
        result = apply_plan(
            conn, diff=diff, plan=plan,
            current_catalog_version_id=cv_old,
        )
    assert result.new_catalog_version_id > cv_old

    # Seed a node_fact so the publisher has work.
    run_id = str(uuid.uuid4())
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES ('n:a', 'page', '{"text":"t"}'::jsonb, 'test',
                    '{"id":"n:a"}'::jsonb, '{"v":1}'::jsonb,
                    now(), %s, %s, 'I')
            """,
            (f"{run_id}:n:a", run_id),
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    # A publish needs explicit catalog writer authority. The test writer
    # binding clones the latest unbound applied catalog -- the one apply_plan
    # just produced -- into a deployment-bound version, so the publish runs on
    # that clone. Prove the clone's template is the reconciled version.
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT diff_summary->>'pytest_template_catalog_version_id' "
            "FROM okg.catalog_versions WHERE catalog_version_id = %s",
            (out["catalog_version_id"],),
        )
        row = cur.fetchone()
    assert row is not None, out
    assert row[0] == str(result.new_catalog_version_id), (out, row)


def test_retract_policy_guard_is_consulted(
    clean_runtime: str, monkeypatch,
) -> None:
    """M5.4 verification (tasks.md 5.4 + M3.5 cross-ref): the
    retraction path first calls `assert_retraction_permitted`. We mock
    the guard to refuse and confirm `apply_plan` raises without
    retracting any graph edge.
    """
    from okg.substrate.catalog import reconcile as recon
    from okg.substrate.errors import RetractionNotPermitted
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_chronos_graph(
            conn,
            nodes=[("n:a", "page"), ("n:b", "ticket")],
            edges=[("n:a", "refs", "n:b")],
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            cv_old = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO okg.narrowings "
                "  (parent_subtype, edge_type, child_subtype, "
                "   catalog_version_id) "
                "VALUES ('page', 'refs', 'ticket', %s)",
                (cv_old,),
            )

        # Force the guard to refuse.
        def _always_refuse(**kw):
            err = RetractionNotPermitted(
                "test: guard refuses reconcile retractions",
                data={"source_class": kw.get("source_class")},
            )
            raise err
        monkeypatch.setattr(
            recon, "assert_retraction_permitted", _always_refuse,
        )

        diff = CatalogDiff(changes=[
            NarrowingChange(
                "restrictive", "page", "refs", "ticket",
                violator_count=1,
            ),
        ])
        plan = [{
            "change": "remove_narrowing",
            "parent": "page", "edge": "refs", "child": "ticket",
            "action": "retract",
        }]
        with pytest.raises(RetractionNotPermitted):
            apply_plan(
                conn, diff=diff, plan=plan,
                current_catalog_version_id=cv_old,
            )

    # No retractions applied: the violating edge survives in the graph.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout()
        rows = graph.query(
            "SELECT count(*) AS n FROM okg.graph_edges WHERE edge_type = 'refs'"
        )
        assert int(rows[0]["n"]) == 1


def test_apply_wrong_action_for_kind_raises() -> None:
    """'apply' on a restrictive change or 'retract' on an
    additive change is a plan authoring error; refuse."""
    diff = CatalogDiff(changes=[
        NarrowingChange("restrictive", "a", "r", "b"),
    ])
    plan = [{
        "change": "remove_narrowing",
        "parent": "a", "edge": "r", "child": "b",
        "action": "apply",  # wrong — apply is for additive
    }]
    # Don't even need a real conn: the mismatch check runs before
    # any DB I/O.
    with pytest.raises(SubstrateError, match="only valid for additive"):
        apply_plan(
            None, diff=diff, plan=plan,  # type: ignore[arg-type]
            current_catalog_version_id=1,
        )
