"""Cross-source endpoint wedge: an edge to another source's missing record.

Source ``a`` owns ``widget`` records, source ``b`` writes ``note references
widget`` onto them, source ``c`` is unrelated (see
``tests/pytest/endpoint_wedge_fixture.py``). Three paths turn "b points at a
widget that is not on the branch" into lost work or a stopped deployment
(pact/changes/cross-source-endpoint-wedge/design.md section 1):

* b runs before a: the whole b run is refused (admission);
* a retired widget is named again by b (admission);
* a retracts a widget b points at: the dangling edge blocks the publish of
  every source (the gate, default ``block`` policy).

Each test states the behavior the PACT requires. On the base commit the
first, third and fourth tests fail (the wedge reproduces; the reproduce
commit marked them as expected failures, this build removes the marks).
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.ingest.incomplete_recovery import source_sync_hold
from tests.pytest import endpoint_wedge_fixture as fx
from tests.pytest.source_jobs_inline import use_inline_source_jobs

pytestmark = pytest.mark.postgres


@pytest.fixture(params=["incremental", "full"])
def gate_path(request, monkeypatch: pytest.MonkeyPatch) -> str:
    """Run the test on the incremental gate path and on the full one.

    The full path is forced by making the gate widen every incremental
    evaluation to a full sweep, as a catalog-version change does.
    """
    if request.param == "full":
        from okg.substrate.consistency.transform_gate import (
            TransformConsistencyGate,
        )

        monkeypatch.setattr(
            TransformConsistencyGate,
            "_incremental_fallback_reason",
            lambda self, **_kwargs: "forced_full_for_test",
        )
    return request.param


@pytest.fixture(autouse=True)
def _inline(monkeypatch: pytest.MonkeyPatch) -> None:
    use_inline_source_jobs(monkeypatch)


def _admission_status(dsn: str, source: str) -> list[tuple[str, str | None]]:
    from tests.pytest.admission_envelope_reads import fetch_admission_envelope

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        row = fetch_admission_envelope(
            cur,
            columns=("status", "verdict->>'code'"),
            intent="source_sync",
            deployment_name="svc",
            source=f"svc.{source}",
        )
    return [] if row is None else [row]


def _publish_statuses(outcome) -> list[str]:
    return [str(row.get("status")) for row in outcome.report["publishes"]]


def test_b_before_a_refuses_whole_run(clean_runtime: str, tmp_path) -> None:
    """B's note and its edge onto a never-written widget: B must not be refused."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(
        dep, "b",
        [fx.node("note:1", "note")],
        [fx.edge("note:1", "widget:1")],
    )

    outcome = fx.ingest(clean_runtime, dep, "b")

    assert outcome.any_failed is False, outcome.report
    assert _admission_status(clean_runtime, "b")[-1][0] in {
        "accepted", "published",
    }, _admission_status(clean_runtime, "b")
    assert fx.node_ids(clean_runtime, "b") == {"note:1"}


def test_b_before_a_sibling_publishes_without_freeze(
    clean_runtime: str, tmp_path,
) -> None:
    """B with an unrelated sibling c: both publish, nothing freezes.

    On the base commit b was refused ``producer_edge_endpoint_missing``,
    c published, and no recovery checkpoint was left requiring repair
    (design.md 1b, confirmed by the reproduce run). Now b publishes too,
    its edge waiting.
    """
    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(
        dep, "b",
        [fx.node("note:1", "note")],
        [fx.edge("note:1", "widget:1")],
    )
    fx.write_fixture(dep, "c", [fx.node("doc:1", "document")], [])

    outcome = fx.ingest(clean_runtime, dep, "b", "c")

    b_status = _admission_status(clean_runtime, "b")[-1]
    assert b_status == ("published", None)
    assert _admission_status(clean_runtime, "c")[-1][0] == "published"
    assert set(_publish_statuses(outcome)) == {"published"}, outcome.report
    assert fx.node_ids(clean_runtime, "c") == {"doc:1"}
    assert fx.node_ids(clean_runtime, "b") == {"note:1"}
    assert [row["state"] for row in fx.waiting_rows(clean_runtime)] == [
        "waiting",
    ]
    hold = source_sync_hold(
        dsn=clean_runtime,
        deployment="svc",
        sources={"b": "svc.b", "c": "svc.c"},
    )
    assert hold is None, (hold, outcome.report)


def test_retracted_record_named_again_dangles(
    clean_runtime: str, tmp_path,
) -> None:
    """A widget a retired, named again by b, must not refuse or block b.

    On the base commit this is a refusal, not a dangling edge: the run wrote
    straight to the graph, so admission finds no live endpoint and never
    reaches the node-fact fallback that design.md 1a describes (that
    fallback serves only runs recorded as facts). The refusal is
    ``producer_edge_endpoint_missing``, as for a record never written.
    """
    dep = fx.build_deployment(tmp_path, clean_runtime)
    keep = fx.node("widget:2", "widget")
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget"), keep], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    fx.write_fixture(dep, "a", [keep], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert fx.node_ids(clean_runtime, "a") == {"widget:2"}
    fx.write_fixture(
        dep, "b",
        [fx.node("note:1", "note")],
        [fx.edge("note:1", "widget:1")],
    )

    outcome = fx.ingest(clean_runtime, dep, "b")

    assert outcome.any_failed is False, outcome.report
    assert set(_publish_statuses(outcome)) == {"published"}, outcome.report
    assert fx.node_ids(clean_runtime, "b") == {"note:1"}


def test_a_retraction_blocks_every_publish(
    clean_runtime: str, tmp_path,
) -> None:
    """A retracts a widget b points at: c must keep publishing."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    keep = fx.node("widget:2", "widget")
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget"), keep], [])
    fx.write_fixture(
        dep, "b",
        [fx.node("note:1", "note")],
        [fx.edge("note:1", "widget:1")],
    )
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    first = fx.ingest(clean_runtime, dep, "b")
    assert first.any_failed is False, first.report
    assert set(_publish_statuses(first)) == {"published"}, first.report
    assert fx.edge_triples(clean_runtime) == {
        ("note:1", "references", "widget:1"),
    }

    fx.write_fixture(dep, "a", [keep], [])
    retracted = fx.ingest(clean_runtime, dep, "a")
    fx.write_fixture(dep, "c", [fx.node("doc:1", "document")], [])
    unrelated = fx.ingest(clean_runtime, dep, "c")

    assert set(_publish_statuses(retracted)) == {"published"}, retracted.report
    assert set(_publish_statuses(unrelated)) == {"published"}, unrelated.report
    assert fx.node_ids(clean_runtime, "c") == {"doc:1"}


# --- The build: the edge waits, then the original row is promoted ----------


def _b_with_edge(dep, note="note:1", widget="widget:1") -> None:
    fx.write_fixture(
        dep, "b",
        [fx.node(note, "note")],
        [fx.edge(note, widget)],
    )


def test_b_before_a_publishes_with_edge_waiting(
    clean_runtime: str, tmp_path,
) -> None:
    """B's notes publish, the edge waits, C is not refused, no hold."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b_with_edge(dep)

    outcome = fx.ingest(clean_runtime, dep, "b")

    result = fx.result_for(outcome, "b")
    assert result["status"] == "OK", outcome.report
    assert "deferred_edges" in result, result["admission_verdict"]
    assert result["deferred_edges"]["count"] == 1
    assert result["deferred_edges"]["by_state"] == {"waiting": 1}
    assert result["deferred_edges"]["sample"][0]["dst"] == "widget:1"
    assert set(_publish_statuses(outcome)) == {"published"}, outcome.report
    assert fx.node_ids(clean_runtime, "b") == {"note:1"}
    assert fx.edge_triples(clean_runtime) == set()
    waiting = fx.waiting_rows(clean_runtime)
    assert [(row["src"], row["dst"], row["state"]) for row in waiting] == [
        ("note:1", "widget:1", "waiting"),
    ]
    assert source_sync_hold(
        dsn=clean_runtime, deployment="svc",
        sources={"b": "svc.b", "c": "svc.c"},
    ) is None
    fx.write_fixture(dep, "c", [fx.node("doc:1", "document")], [])
    sibling = fx.ingest(clean_runtime, dep, "c")
    assert sibling.any_failed is False, sibling.report
    assert fx.node_ids(clean_runtime, "c") == {"doc:1"}


def test_a_publish_promotes_waiting_edge(
    clean_runtime: str, tmp_path, gate_path: str,
) -> None:
    """A writes the widget: the same row moves into the graph, byte for byte."""
    from okg.substrate.graph_store_internal.edge_deferrals import (
        EDGE_ROW_COLUMNS,
    )

    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b_with_edge(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    (held,) = fx.waiting_rows(clean_runtime)
    written = {column: held[column] for column in EDGE_ROW_COLUMNS}

    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    outcome = fx.ingest(clean_runtime, dep, "a")

    assert set(_publish_statuses(outcome)) == {"published"}, outcome.report
    assert fx.waiting_rows(clean_runtime) == []
    assert fx.edge_rows(clean_runtime) == [written]


def test_a_retraction_moves_edge_to_waiting(
    clean_runtime: str, tmp_path, gate_path: str,
) -> None:
    """A retracts the widget: the edge waits, C keeps publishing."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    keep = fx.node("widget:2", "widget")
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget"), keep], [])
    _b_with_edge(dep)
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    (edge_before,) = fx.edge_rows(clean_runtime)
    assert fx.waiting_rows(clean_runtime) == []

    fx.write_fixture(dep, "a", [keep], [])
    retracted = fx.ingest(clean_runtime, dep, "a")
    fx.write_fixture(dep, "c", [fx.node("doc:1", "document")], [])
    unrelated = fx.ingest(clean_runtime, dep, "c")

    assert set(_publish_statuses(retracted)) == {"published"}, retracted.report
    assert set(_publish_statuses(unrelated)) == {"published"}, unrelated.report
    assert fx.edge_triples(clean_runtime) == set()
    (held,) = fx.waiting_rows(clean_runtime)
    assert held["state"] == "waiting" and held["dst"] == "widget:1"

    # The restore: the same row is promoted again.
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget"), keep], [])
    restored = fx.ingest(clean_runtime, dep, "a")
    assert set(_publish_statuses(restored)) == {"published"}, restored.report
    assert fx.waiting_rows(clean_runtime) == []
    assert fx.edge_rows(clean_runtime) == [edge_before]


def test_b_retracts_waiting_edge(
    clean_runtime: str, tmp_path, gate_path: str,
) -> None:
    """B drops an edge that is waiting: the waiting row goes with it."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b_with_edge(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    assert len(fx.waiting_rows(clean_runtime)) == 1

    fx.write_fixture(dep, "b", [fx.node("note:1", "note")], [])
    outcome = fx.ingest(clean_runtime, dep, "b")

    assert outcome.any_failed is False, outcome.report
    assert set(_publish_statuses(outcome)) == {"published"}, outcome.report
    assert fx.waiting_rows(clean_runtime) == []
    assert fx.node_ids(clean_runtime, "b") == {"note:1"}


def test_b_retracting_its_note_takes_its_waiting_edge(
    clean_runtime: str, tmp_path,
) -> None:
    dep = fx.build_deployment(tmp_path, clean_runtime)
    keep = fx.node("note:2", "note")
    fx.write_fixture(
        dep, "b",
        [fx.node("note:1", "note"), keep],
        [fx.edge("note:1", "widget:1")],
    )
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    assert len(fx.waiting_rows(clean_runtime)) == 1

    fx.write_fixture(dep, "b", [keep], [])
    outcome = fx.ingest(clean_runtime, dep, "b")

    assert outcome.any_failed is False, outcome.report
    assert fx.waiting_rows(clean_runtime) == []
    assert fx.node_ids(clean_runtime, "b") == {"note:2"}


def test_waiting_only_change_cuts_generation(
    clean_runtime: str, tmp_path,
) -> None:
    """A run whose only change retracts a waiting edge cuts a new generation."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b_with_edge(dep)
    first = fx.ingest(clean_runtime, dep, "b")
    first_generation = first.report["publishes"][-1]["generation_id"]

    # b keeps its note and drops the waiting edge: no node or graph edge
    # changes, so only the waiting table differs.
    fx.write_fixture(dep, "b", [fx.node("note:1", "note")], [])
    second = fx.ingest(clean_runtime, dep, "b")

    publish = second.report["publishes"][-1]
    assert publish["status"] == "published", second.report
    assert publish["generation_id"] != first_generation
    assert fx.waiting_rows(clean_runtime) == []
    # The change is in a checkpoint: the first generation still holds the
    # waiting row, the second does not.
    assert fx.waiting_rows_at(clean_runtime, first_generation) != []
    assert fx.waiting_rows_at(clean_runtime, publish["generation_id"]) == []


# --- Held states: the edge stays out of the graph and never blocks --------


def test_subtype_mismatch_held_not_blocking(
    clean_runtime: str, tmp_path, gate_path: str,
) -> None:
    """A holds the id as a gadget; b's note-references-widget is held."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(dep, "a", [fx.node("widget:1", "gadget")], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    _b_with_edge(dep)

    outcome = fx.ingest(clean_runtime, dep, "b")

    assert outcome.any_failed is False, outcome.report
    assert set(_publish_statuses(outcome)) == {"published"}, outcome.report
    assert fx.edge_triples(clean_runtime) == set()
    (held,) = fx.waiting_rows(clean_runtime)
    assert held["state"] == "subtype_mismatch"
    assert held["expected_subtype"] == "widget"
    fx_result = fx.result_for(outcome, "b")
    assert fx_result["deferred_edges"]["by_state"] == {"subtype_mismatch": 1}


def test_mismatch_rechecked_when_endpoint_rewritten(
    clean_runtime: str, tmp_path, gate_path: str,
) -> None:
    """A rewrites the id as a widget: the held edge is promoted."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(dep, "a", [fx.node("widget:1", "gadget")], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    _b_with_edge(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    assert fx.waiting_rows(clean_runtime)[0]["state"] == "subtype_mismatch"

    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    outcome = fx.ingest(clean_runtime, dep, "a")

    assert set(_publish_statuses(outcome)) == {"published"}, outcome.report
    assert fx.waiting_rows(clean_runtime) == []
    assert fx.edge_triples(clean_runtime) == {
        ("note:1", "references", "widget:1"),
    }


def test_cardinality_violation_held_not_blocking(
    clean_runtime: str, tmp_path, gate_path: str,
) -> None:
    """Two waiting edges from one note, at most one target allowed.

    Both widgets arrive in one publish, the producer clause passes, and the
    catalog cardinality check fails: both rows are held as cardinality
    violations and the publish is not blocked. (A narrowing violation, the
    third held state, cannot be built from a loadable producer: a producer
    profile whose clause the catalog lacks is refused when it loads.)
    """
    dep = fx.build_deployment(
        tmp_path, clean_runtime, at_most_one_target=True,
    )
    fx.write_fixture(
        dep, "b",
        [fx.node("note:1", "note")],
        [fx.edge("note:1", "widget:1"), fx.edge("note:1", "widget:2")],
    )
    first = fx.ingest(clean_runtime, dep, "b")
    assert first.any_failed is False, "\n".join(
        str(g["details"].get("error"))
        for p in first.report["publishes"] for g in p["consistency_gates"]
    )
    assert len(fx.waiting_rows(clean_runtime)) == 2

    fx.write_fixture(
        dep, "a",
        [fx.node("widget:1", "widget"), fx.node("widget:2", "widget")], [],
    )
    outcome = fx.ingest(clean_runtime, dep, "a")

    assert set(_publish_statuses(outcome)) == {"published"}, outcome.report
    assert fx.edge_triples(clean_runtime) == set()
    assert {row["state"] for row in fx.waiting_rows(clean_runtime)} == {
        "cardinality_violation",
    }


def test_dedupe_step_drops_waiting_row_present_in_graph(
    clean_runtime: str, tmp_path,
) -> None:
    """An edge in both tables (a merge or rollback left it) keeps one row."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.edge_deferrals import (
        EDGE_DEFERRALS_TABLE,
        edge_row,
    )

    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b_with_edge(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    (held,) = fx.waiting_rows(clean_runtime)
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    # Put the same edge into the graph by hand, leaving the waiting row.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.checkout().upsert_rows("okg.graph_edges", [edge_row(held)])
        assert EDGE_DEFERRALS_TABLE and len(fx.waiting_rows(clean_runtime)) == 1

    outcome = fx.ingest(clean_runtime, dep, "a")

    assert set(_publish_statuses(outcome)) == {"published"}, outcome.report
    assert fx.waiting_rows(clean_runtime) == []
    assert [row["edge_id"] for row in fx.edge_rows(clean_runtime)] == [
        held["edge_id"],
    ]


# --- Read boundary -----------------------------------------------------------


def test_mcp_role_has_no_grant_on_waiting_table(
    clean_runtime: str, tmp_path,
) -> None:
    """The pattern grant skips the waiting table and revokes an earlier one."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b_with_edge(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        physical = [
            row[0] for row in conn.execute(
                "SELECT physical_table FROM _chronos_branch_tables"
                " WHERE table_name = 'okg.graph_edge_deferrals'",
            ).fetchall()
        ]
        assert physical, "the waiting table is not registered"
        graph_physical = conn.execute(
            "SELECT physical_table FROM _chronos_branch_tables"
            " WHERE table_name = 'okg.graph_edges'",
        ).fetchone()[0]

        def has_select(table: str) -> bool:
            return bool(conn.execute(
                "SELECT has_table_privilege('okg_mcp', %s, 'SELECT')",
                (f"public.{table}",),
            ).fetchone()[0])

        # The graph table is granted by the same pattern: the control.
        assert has_select(graph_physical)
        assert not any(has_select(table) for table in physical)

        # A database where the pattern grant already ran is revoked.
        for table in physical:
            conn.execute(f'GRANT SELECT ON TABLE public."{table}" TO okg_mcp')
        assert all(has_select(table) for table in physical)
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.graph_store_internal import schema

        schema._MCP_GRANTED_AT_EPOCH.clear()
        ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        )._grant_chronos_public_tables_to_mcp_if_present()
        assert not any(has_select(table) for table in physical)


def test_remediator_keeps_writer_grants_on_waiting_table(
    clean_runtime: str, tmp_path,
) -> None:
    """The remediator is a writer, not a read role: its retraction withdraws
    the producer's waiting rows, so it holds the same grants on the waiting
    table as on every other _chronos table. A database where an earlier build
    revoked them, or queued that revoke for retry, is repaired by the next
    grant catch-up, and okg_mcp stays withheld (#4057 regression)."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b_with_edge(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    writer = ("SELECT", "INSERT", "UPDATE", "DELETE")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Every schema version too, when Chronos keeps that registry (the
        # grant block reads it under the same to_regclass check).
        registries = ["_chronos_branch_tables"]
        if conn.execute(
            "SELECT to_regclass('public._chronos_branch_table_schema_versions')"
        ).fetchone()[0] is not None:
            registries.append("_chronos_branch_table_schema_versions")
        physical = sorted({
            row[0]
            for registry in registries
            for row in conn.execute(
                f"SELECT physical_table FROM {registry}"
                " WHERE table_name = 'okg.graph_edge_deferrals'",
            ).fetchall()
        })
        assert physical, "the waiting table is not registered"

        def has(role: str, table: str, privilege: str) -> bool:
            return bool(conn.execute(
                "SELECT has_table_privilege(%s, %s, %s)",
                (role, f"public.{table}", privilege),
            ).fetchone()[0])

        def remediator_writes(table: str) -> bool:
            return all(
                has("projection_remediator", table, privilege)
                for privilege in writer
            )

        assert all(remediator_writes(table) for table in physical)
        assert not any(has("okg_mcp", table, "SELECT") for table in physical)

        stale = [
            f'REVOKE ALL ON TABLE public."{table}" FROM projection_remediator'
            for table in physical
        ]
        for statement in stale:
            conn.execute(statement)
            conn.execute(
                "INSERT INTO okg.pending_grants (statement, last_error)"
                " VALUES (%s, 'queued by an earlier build')",
                (statement,),
            )
        assert not any(remediator_writes(table) for table in physical)
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.graph_store_internal import schema

        schema._MCP_GRANTED_AT_EPOCH.clear()
        ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        )._grant_chronos_public_tables_to_mcp_if_present()
        assert all(remediator_writes(table) for table in physical)
        assert not any(has("okg_mcp", table, "SELECT") for table in physical)
        assert conn.execute(
            "SELECT count(*) FROM okg.pending_grants WHERE statement = ANY(%s)",
            (stale,),
        ).fetchone()[0] == 0


def test_unchanged_reemit_cuts_no_generation(
    clean_runtime: str, tmp_path,
) -> None:
    """B re-emits an identical waiting edge: no write, no new generation."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b_with_edge(dep)
    first = fx.ingest(clean_runtime, dep, "b")
    assert first.any_failed is False, first.report
    (before,) = fx.waiting_rows(clean_runtime)

    again = fx.ingest(clean_runtime, dep, "b")

    assert again.any_failed is False, again.report
    publish = again.report["publishes"][-1] if again.report["publishes"] else {}
    assert publish.get("generation_id", first.report["publishes"][-1][
        "generation_id"]) == first.report["publishes"][-1]["generation_id"], (
        publish
    )
    assert fx.waiting_rows(clean_runtime) == [before]


def test_order_independent_equals_dependency_order(
    clean_runtime: str, tmp_path,
) -> None:
    """B-then-A and A-then-B end in the same graph edges and no waiting rows.

    Two pairs in one deployment: ``note:1 -> widget:1`` is written b first,
    ``note:2 -> widget:2`` a first (the dependency order a rebuild uses). The
    two edge rows agree on every column but the ids, and nothing waits.
    """
    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(
        dep, "b",
        [fx.node("note:1", "note")], [fx.edge("note:1", "widget:1")],
    )
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False

    fx.write_fixture(
        dep, "a",
        [fx.node("widget:1", "widget"), fx.node("widget:2", "widget")], [],
    )
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    fx.write_fixture(
        dep, "b",
        [fx.node("note:1", "note"), fx.node("note:2", "note")],
        [fx.edge("note:1", "widget:1"), fx.edge("note:2", "widget:2")],
    )
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False

    assert fx.waiting_rows(clean_runtime) == []
    rows = fx.edge_rows(clean_runtime)
    assert len(rows) == 2

    def shape(row):
        return {
            key: value for key, value in row.items()
            if key not in {"edge_id", "src", "dst", "source_record_id", "run_id",
                           "observed_at", "attrs", "source_revision"}
        } | {"attr_keys": sorted(row["attrs"])}

    assert shape(rows[0]) == shape(rows[1])
    assert {(row["src"], row["dst"]) for row in rows} == {
        ("note:1", "widget:1"), ("note:2", "widget:2"),
    }


# --- #3666 link-endpoint placeholders never count as an existing endpoint --


def _write_placeholder(dsn: str, node_id: str, subtype: str = "widget") -> None:
    from okg.substrate.catalog.linker_config import (
        LINK_ENDPOINT_SOURCE,
        LOAD_STATE_ATTR,
        NOT_LOADED,
    )
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        session = store.checkout()
        with session.transaction():
            session.upsert_rows("okg.graph_nodes", [{
                "node_id": node_id,
                "subtype": subtype,
                "attrs": {LOAD_STATE_ATTR: NOT_LOADED, "text": node_id},
                "source": LINK_ENDPOINT_SOURCE,
                "source_record_id": {"id": node_id},
                "source_revision": {"v": 1},
                "run_id": "placeholder-fixture",
            }])


def test_source_edge_onto_placeholder_not_served(
    clean_runtime: str, tmp_path,
) -> None:
    """b's edge onto a placeholder waits; the placeholder does not promote it."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(dep, "c", [fx.node("doc:1", "document")], [])
    assert fx.ingest(clean_runtime, dep, "c").any_failed is False
    _write_placeholder(clean_runtime, "widget:1")
    _b_with_edge(dep)

    outcome = fx.ingest(clean_runtime, dep, "b")

    assert outcome.any_failed is False, outcome.report
    assert set(_publish_statuses(outcome)) == {"published"}, outcome.report
    assert fx.edge_triples(clean_runtime) == set()
    (held,) = fx.waiting_rows(clean_runtime)
    assert (held["dst"], held["state"]) == ("widget:1", "waiting")
    # A later publish with the placeholder still the only row at the id does
    # not promote the edge either.
    fx.write_fixture(dep, "c", [fx.node("doc:2", "document")], [])
    assert fx.ingest(clean_runtime, dep, "c").any_failed is False
    assert fx.edge_triples(clean_runtime) == set()
    assert len(fx.waiting_rows(clean_runtime)) == 1


def test_publish_retry_after_crash_same_state(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crash between the publish step and the checkpoint is completed by a retry.

    a retracts a widget b points at. The step moves b's edge to the waiting
    table and commits; the checkpoint write then fails once. The retried
    publish reaches the same graph and the same waiting row.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    dep = fx.build_deployment(tmp_path, clean_runtime)
    keep = fx.node("widget:2", "widget")
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget"), keep], [])
    _b_with_edge(dep)
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    (edge_before,) = fx.edge_rows(clean_runtime)

    real = ChronosGraphStore.checkpoint_generation
    crashed = {"n": 0}

    def crash_once(self, *args, **kwargs):
        if not crashed["n"]:
            crashed["n"] += 1
            raise RuntimeError("injected crash after the publish step")
        return real(self, *args, **kwargs)

    monkeypatch.setattr(ChronosGraphStore, "checkpoint_generation", crash_once)
    fx.write_fixture(dep, "a", [keep], [])
    try:
        first = fx.ingest(clean_runtime, dep, "a")
    except RuntimeError as exc:
        assert "injected crash" in str(exc)
    else:
        assert first.any_failed or any(
            row.get("status") != "published"
            for row in first.report["publishes"]
        ), first.report
    assert crashed["n"] == 1

    retried = fx.ingest(clean_runtime, dep, "a")

    assert set(_publish_statuses(retried)) == {"published"}, retried.report
    assert fx.edge_triples(clean_runtime) == set()
    (held,) = fx.waiting_rows(clean_runtime)
    assert (held["edge_id"], held["state"]) == (
        edge_before["edge_id"], "waiting",
    )
    assert len(fx.waiting_rows(clean_runtime)) == 1


# --- Operator views ---------------------------------------------------------


def test_deferred_edges_report_counts_age_and_warn_threshold(
    clean_runtime: str, tmp_path,
) -> None:
    """Counts by producer, edge type, subtype and state; the warn threshold."""
    from datetime import datetime, timedelta, timezone

    from okg.substrate.graph_store_internal.edge_deferrals import (
        deferred_edges_report,
    )

    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b_with_edge(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        fresh = deferred_edges_report(conn, warn_after_seconds=24 * 3600)
        later = deferred_edges_report(
            conn,
            warn_after_seconds=24 * 3600,
            now=datetime.now(timezone.utc) + timedelta(hours=25),
        )

    assert fresh["table_present"] is True and fresh["total"] == 1
    assert fresh["groups"][0]["producer"] == "svc.b"
    assert fresh["groups"][0]["edge_type"] == "references"
    assert fresh["groups"][0]["endpoint_subtype"] == "widget"
    assert fresh["groups"][0]["state"] == "waiting"
    assert fresh["sample"][0]["dst"] == "widget:1"
    assert fresh["warn"] is False
    assert later["warn"] is True


def test_runtime_deferred_edges_verb_parses_and_doctor_check_exists() -> None:
    from okg.substrate.cli.main import _build_parser

    args = _build_parser().parse_args(
        ["runtime", "deferred-edges", "--deployment", "svc"],
    )
    assert args.rt_subcmd == "deferred-edges"

    from okg.substrate.cli.doctor import _build_doctor_registry

    registry = _build_doctor_registry(args)
    assert "deferred-edges" in registry.names()


def test_owner_published_cap_mode_end_to_end(
    clean_runtime: str, tmp_path,
) -> None:
    """With a cap of 0: waiting on an owner that never published is allowed;
    waiting on an owner that published and still lacks the record is refused."""
    dep = fx.build_deployment(
        tmp_path, clean_runtime,
        consistency={
            "deferred_cap_mode": "owner_published",
            "max_deferred_edges_per_run": 0,
        },
    )
    _b_with_edge(dep)
    quiet = fx.ingest(clean_runtime, dep, "b")
    assert quiet.any_failed is False, quiet.report
    assert len(fx.waiting_rows(clean_runtime)) == 1

    fx.write_fixture(dep, "a", [fx.node("widget:2", "widget")], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    fx.write_fixture(
        dep, "b",
        [fx.node("note:1", "note"), fx.node("note:3", "note")],
        [fx.edge("note:1", "widget:1"), fx.edge("note:3", "widget:9")],
    )
    refused = fx.ingest(clean_runtime, dep, "b")
    assert refused.any_failed is True
    assert refused.report["results"][0]["admission_reject_reason"] == (
        "producer_deferred_edges_over_limit"
    )


# --- Rollback, merge-back, compaction, clone, undo, retirement, two writers --


def _published_generation(outcome) -> str:
    return str(outcome.report["publishes"][-1]["generation_id"])


def test_rollback_across_a_promotion(clean_runtime: str, tmp_path) -> None:
    """Restoring the generation before a promotion puts the edge back to waiting."""
    from okg.substrate.graph_store import ChronosGraphStore

    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b_with_edge(dep)
    waiting_generation = _published_generation(
        fx.ingest(clean_runtime, dep, "b"),
    )
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    promoted = fx.ingest(clean_runtime, dep, "a")
    assert fx.waiting_rows(clean_runtime) == []
    assert len(fx.edge_rows(clean_runtime)) == 1
    assert _published_generation(promoted) != waiting_generation

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.restore_branch_to_generation(
            waiting_generation, branch="default", deployment_name="svc",
        )
    finally:
        store.close()

    assert fx.edge_rows(clean_runtime) == []
    assert "widget:1" not in fx.node_ids(clean_runtime)
    assert [row["state"] for row in fx.waiting_rows(clean_runtime)] == [
        "waiting",
    ]


def test_merge_back_reapplies_waiting_rows_after_a_rollback(
    clean_runtime: str, tmp_path,
) -> None:
    """A waiting-only write after a generation is read back and reapplied.

    This is what merge-back does around a blocked landing: read the waiting
    rows written since the pre-land generation, roll back, put them back.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b_with_edge(dep)
    generation = _published_generation(fx.ingest(clean_runtime, dep, "b"))
    # b drops the edge and is not published: only the waiting table changes.
    fx.write_fixture(dep, "b", [fx.node("note:1", "note")], [])
    assert fx.ingest(clean_runtime, dep, "b", publish=False).any_failed is False
    assert fx.waiting_rows(clean_runtime) == []

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        pending = store.edge_deferrals_since_generation(
            generation, branch="default",
        )
        assert len(pending) == 1 and pending[0][1] is None
        store.restore_branch_to_generation(
            generation, branch="default", deployment_name="svc",
        )
        assert len(fx.waiting_rows(clean_runtime)) == 1
        counts = store.reapply_edge_deferrals(pending, branch="default")
    finally:
        store.close()

    assert counts == {"upserted": 0, "deleted": 1}
    assert fx.waiting_rows(clean_runtime) == []


def test_history_compaction_leaves_waiting_history_intact(
    clean_runtime: str, tmp_path,
) -> None:
    """Compaction retires node and edge history, not the waiting table's."""
    from psycopg import sql

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.history_compaction import (
        apply_history_compaction_plan,
        create_history_compaction_plan,
    )
    from okg.substrate.library.sources.base import NodeFact
    from tests.pytest.catalog_fixture_builders import seed_catalog

    deployment = "wait-compaction"
    with psycopg.connect(clean_runtime) as conn:
        catalog = seed_catalog(conn, subtypes=["claim"])
    store = ChronosGraphStore.connect(clean_runtime)
    checkpoints: list[str] = []
    try:
        session = store.checkout()
        for index in range(6):
            store.apply_facts([NodeFact("n:hot", "claim", {"r": index}, {}, {})])
            session.upsert_rows("okg.graph_edge_deferrals", [{
                "edge_id": 1, "src": "n:hot", "dst": "missing:1",
                "edge_type": "references", "attrs": {"r": index},
                "provenance": "explicit", "confidence": 1.0, "source": "s",
                "source_record_id": {}, "source_revision": {}, "run_id": "r",
                "state": "waiting", "waiting_on": "missing:1",
                "waiting_role": "dst", "expected_subtype": "widget",
                "deferred_at": "2026-01-01T00:00:00+00:00",
                "deferred_generation": "g", "catalog_version_id": catalog,
                "profile_fingerprint": "fp",
            }])
            checkpoints.append(store.checkpoint_generation(
                "100" if index == 0 else f"history-{index}",
                deployment_name=deployment, catalog_version_id=catalog,
                metadata={"target_generation_id": 100 + index},
            ))
    finally:
        store.close()

    policy = {
        "default": "latest-only", "audit_tail_generations": 2,
        "nodes": {"claim": "latest-only"}, "edges": {},
    }
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        physical = conn.execute(
            "SELECT physical_table FROM _chronos_branch_tables WHERE backend = "
            "'interval' AND table_name = 'okg.graph_edge_deferrals'",
        ).fetchone()[0]
        ident = sql.Identifier(*str(physical).split("."))
        before = conn.execute(
            sql.SQL("SELECT count(*) FROM {}").format(ident),
        ).fetchone()[0]
        plan = create_history_compaction_plan(
            conn, store=store, deployment_name=deployment, policy_raw=policy,
        )
        assert plan["retired_checkpoints"] == checkpoints[:-2]
        apply_history_compaction_plan(
            conn, store=store, plan_id=plan["plan_id"],
            confirm=plan["plan_id"], apply=True, policy_raw=policy,
        )
        after = conn.execute(
            sql.SQL("SELECT count(*) FROM {}").format(ident),
        ).fetchone()[0]
        kept = store.checkout_generation(checkpoints[-2]).query(
            "SELECT attrs FROM okg.graph_edge_deferrals",
        )
        head = store.checkout_generation(checkpoints[-1]).query(
            "SELECT attrs FROM okg.graph_edge_deferrals",
        )
    assert after == before
    assert kept == [{"attrs": {"r": 4}}] and head == [{"attrs": {"r": 5}}]


def test_environment_clone_purges_the_waiting_table() -> None:
    from okg.substrate.environment_clone import clone_table_disposition

    assert clone_table_disposition("okg", "graph_edge_deferrals") == "purge"
    assert clone_table_disposition(
        "public", "_chronos_b_interval_okg_graph_edge_deferrals",
    ) == "purge"


def test_a_run_undo_keeps_b_edge(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """a's run inserts a widget b points at, then is undone: b's edge stays.

    The edge was written after the run base onto a node the undo deletes, and
    used to be deleted with it. It can wait, so it is kept and the next
    publish moves it to the waiting table.
    """
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest.compensation import retract_chronos_run
    from okg.substrate.ingest.run_base import (
        hold_run_liveness,
        run_base_checkpoint_id,
        take_run_base,
    )
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(dep, "a", [fx.node("widget:2", "widget")], [])
    fx.write_fixture(
        dep, "b", [fx.node("note:1", "note")],
        [fx.edge("note:1", "widget:2")],
    )
    assert fx.ingest(clean_runtime, dep, "a", "b").any_failed is False
    run_id = "undo-run-1"
    checkpoint_id = run_base_checkpoint_id("default", run_id)
    live = hold_run_liveness(clean_runtime, checkpoint_id)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        take_run_base(
            store, deployment="svc", branch="default", source="svc.a",
            run_id=run_id,
        )
        store.apply_facts(
            [NodeFact(
                "widget:1", "widget", {"text": "w"}, {"id": "widget:1"},
                {"rev": 1},
            )],
            source="svc.a", run_id=run_id, strict_endpoints=False,
        )
        # b's edge onto the new widget, written as a later b run writes it
        # (a new ingest would refuse to start while a's run is unpublished).
        store.apply_facts(
            [
                NodeFact(
                    "note:3", "note", {"text": "n"}, {"id": "note:3"},
                    {"rev": 1},
                ),
                EdgeFact(
                    "note:3", "widget:1", "references",
                    source_record_id={"id": "note:3->widget:1"},
                    source_revision={"rev": 1},
                ),
            ],
            source="svc.b", run_id="b-later-run", strict_endpoints=False,
        )
        stats = retract_chronos_run(
            clean_runtime, admission_id=None, source="svc.a", run_id=run_id,
        )
        assert stats["mode"] == "undo_to_run_base", stats
    finally:
        live.close()
        store.close()

    assert "widget:1" not in fx.node_ids(clean_runtime)
    assert ("note:3", "references", "widget:1") in fx.edge_triples(
        clean_runtime,
    )
    fx.write_fixture(dep, "c", [fx.node("doc:1", "document")], [])
    outcome = fx.ingest(clean_runtime, dep, "c")
    assert set(_publish_statuses(outcome)) == {"published"}, outcome.report
    assert [(row["src"], row["dst"]) for row in fx.waiting_rows(
        clean_runtime)] == [("note:3", "widget:1")]


def test_a_source_retirement_moves_edges(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retiring the source that owns the widget leaves b's edge, which waits;
    retiring b takes its waiting edges."""
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    from okg.substrate.source_retirement import (
        plan_source_retirement,
        remediate_source_retirement,
    )

    def identity_for(name: str):
        def identity(*_args, **_kwargs):
            return (
                name, {"ownership_id": f"svc.{name}"},
                (name, f"svc.{name}"), "registry-digest",
            )
        return identity

    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(
        dep, "a", [fx.node("widget:1", "widget"), fx.node("widget:2", "widget")],
        [],
    )
    _b_with_edge(dep)
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    assert len(fx.edge_rows(clean_runtime)) == 1

    monkeypatch.setattr(
        "okg.substrate.source_retirement._registry_identity",
        identity_for("a"),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        plan = plan_source_retirement(
            conn, source="a", deployment_name="svc",
        )
        assert plan.edges == (), "b's edge can wait, so retirement leaves it"
        remediate_source_retirement(
            conn, source_name="a", deployment_name="svc",
            expected_plan_digest=plan.plan_digest, confirm_source="a",
            apply=True,
        )
    assert fx.node_ids(clean_runtime, "a") == set()
    fx.write_fixture(dep, "c", [fx.node("doc:1", "document")], [])
    outcome = fx.ingest(clean_runtime, dep, "c")
    assert set(_publish_statuses(outcome)) == {"published"}, outcome.report
    assert fx.edge_triples(clean_runtime) == set()
    assert len(fx.waiting_rows(clean_runtime)) == 1

    monkeypatch.setattr(
        "okg.substrate.source_retirement._registry_identity",
        identity_for("b"),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        plan = plan_source_retirement(
            conn, source="b", deployment_name="svc",
        )
        remediate_source_retirement(
            conn, source_name="b", deployment_name="svc",
            expected_plan_digest=plan.plan_digest, confirm_source="b",
            apply=True,
        )
    assert fx.waiting_rows(clean_runtime) == []


def test_two_sources_same_edge_identity(clean_runtime: str, tmp_path) -> None:
    """Between two waiting writers the last wins; the promoted row is theirs."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(
        dep, "b", [fx.node("note:1", "note")], [fx.edge("note:1", "widget:1")],
    )
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    (first,) = fx.waiting_rows(clean_runtime)
    assert first["source"] == "svc.b"
    fx.write_fixture(
        dep, "d", [fx.node("note:9", "note")], [fx.edge("note:1", "widget:1")],
    )

    assert fx.ingest(clean_runtime, dep, "d").any_failed is False

    (second,) = fx.waiting_rows(clean_runtime)
    assert (second["edge_id"], second["source"]) == (
        first["edge_id"], "svc.d",
    )
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert fx.waiting_rows(clean_runtime) == []
    (promoted,) = fx.edge_rows(clean_runtime)
    assert promoted["source"] == "svc.d"


def test_narrowing_violation_state_with_a_synthetic_narrowing_rule(
    clean_runtime: str, tmp_path,
) -> None:
    """No loadable producer reaches ``narrowing_violation`` (a clause the
    catalog lacks is refused when its profile loads), so the promotion step
    is driven directly with a synthetic narrowing verdict: the row passes the
    producer clause, fails the narrowing check, and stays held."""
    from okg.substrate.graph_store_internal.edge_deferrals import (
        DeferralPolicy,
        run_edge_deferral_step,
    )

    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b_with_edge(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    assert fx.ingest(clean_runtime, dep, "a", publish=False).any_failed is False
    assert fx.edge_triples(clean_runtime) == set()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        result = run_edge_deferral_step(
            conn,
            policy=DeferralPolicy(enabled=True),
            deployment_name="svc",
            branch_name="default",
            catalog_version_id=1,
            target_generation_id="0",
            candidate_edge_ids=None,
            dirty_node_ids=None,
            check_scoped_edges=lambda ids: ([], list(ids)),
            check_cardinality=lambda _edges, _nodes: [],
        )

    assert result.promoted == 0
    assert result.held == {"narrowing_violation": 1}
    assert fx.edge_triples(clean_runtime) == set()
    assert [row["state"] for row in fx.waiting_rows(clean_runtime)] == [
        "narrowing_violation",
    ]
