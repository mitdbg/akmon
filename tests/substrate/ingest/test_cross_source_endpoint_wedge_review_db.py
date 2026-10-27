"""Cross-source endpoint wedge: cases the independent review asked for.

Cardinality trials that must not touch published edges, a crash inside a
trial, the grant registry and the waiting table, undo and retirement with
deferral on and off, stray retraction of a waiting edge, non-dangling
mismatches left to the gate, a non-deferrable dangling edge that still
blocks, generation metadata, the read boundary, and the operator views.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

import psycopg
import pytest

from okg.substrate.ingest.incomplete_recovery import source_sync_hold
from tests.pytest import endpoint_wedge_fixture as fx
from tests.pytest.source_jobs_inline import use_inline_source_jobs

pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _inline(monkeypatch: pytest.MonkeyPatch) -> None:
    use_inline_source_jobs(monkeypatch)


def _statuses(outcome) -> set[str]:
    return {str(row.get("status")) for row in outcome.report["publishes"]}


def _b(dep, note="note:1", widget="widget:1") -> None:
    fx.write_fixture(
        dep, "b", [fx.node(note, "note")], [fx.edge(note, widget)],
    )


# --- Blocker 1: a trial never removes a published edge ----------------------


def test_cardinality_trial_holds_only_the_trial_edge(
    clean_runtime: str, tmp_path,
) -> None:
    """note:1 already points at widget:2 (published). A second edge, onto a
    widget that arrives later, would break at-most-one-target: the new edge is
    held and the published edge stays."""
    dep = fx.build_deployment(tmp_path, clean_runtime, at_most_one_target=True)
    fx.write_fixture(dep, "a", [fx.node("widget:2", "widget")], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    fx.write_fixture(
        dep, "b", [fx.node("note:1", "note")],
        [fx.edge("note:1", "widget:2"), fx.edge("note:1", "widget:1")],
    )
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    assert fx.edge_triples(clean_runtime) == {
        ("note:1", "references", "widget:2"),
    }
    assert [r["dst"] for r in fx.waiting_rows(clean_runtime)] == ["widget:1"]

    fx.write_fixture(
        dep, "a", [fx.node("widget:1", "widget"), fx.node("widget:2", "widget")],
        [],
    )
    outcome = fx.ingest(clean_runtime, dep, "a")

    assert _statuses(outcome) == {"published"}, outcome.report
    assert fx.edge_triples(clean_runtime) == {
        ("note:1", "references", "widget:2"),
    }
    (held,) = fx.waiting_rows(clean_runtime)
    assert (held["dst"], held["state"]) == ("widget:1", "cardinality_violation")


def test_trial_failure_is_limited_to_trial_edges(
    clean_runtime: str, tmp_path,
) -> None:
    """The gate's cardinality query returns every edge of a violating group,
    and an unrelated earlier violation too; only trial edges are held and the
    published ones are never removed."""
    from okg.substrate.graph_store_internal.edge_deferrals import (
        DeferralPolicy,
        run_edge_deferral_step,
    )

    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(dep, "a", [fx.node("widget:2", "widget")], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    fx.write_fixture(
        dep, "b", [fx.node("note:1", "note")],
        [fx.edge("note:1", "widget:2"), fx.edge("note:1", "widget:1")],
    )
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    fx.write_fixture(
        dep, "a", [fx.node("widget:1", "widget"), fx.node("widget:2", "widget")],
        [],
    )
    assert fx.ingest(clean_runtime, dep, "a", publish=False).any_failed is False
    (published,) = fx.edge_rows(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        result = run_edge_deferral_step(
            conn,
            policy=DeferralPolicy(enabled=True),
            deployment_name="svc", branch_name="default",
            catalog_version_id=1, target_generation_id="0",
            candidate_edge_ids=None, dirty_node_ids=None,
            check_scoped_edges=lambda ids: ([], []),
            # Every edge of the group and an unrelated id, as the gate's
            # query returns them.
            check_cardinality=lambda ids, nodes: [
                *ids, int(published["edge_id"]), 987654321,
            ],
        )

    assert result.promoted == 0
    assert result.held == {"cardinality_violation": 1}
    assert [r["edge_id"] for r in fx.edge_rows(clean_runtime)] == [
        published["edge_id"],
    ]


def test_a_trial_is_one_transaction(clean_runtime: str, tmp_path) -> None:
    """A crash in the middle of a trial leaves the graph and the waiting row
    as they were, so a retry reaches the same result."""
    from okg.substrate.graph_store_internal.edge_deferrals import (
        DeferralPolicy,
        run_edge_deferral_step,
    )

    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    assert fx.ingest(clean_runtime, dep, "a", publish=False).any_failed is False
    before = fx.waiting_rows(clean_runtime)

    def crash(_ids, _nodes):
        raise RuntimeError("injected crash inside the trial")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(RuntimeError, match="injected crash"):
            run_edge_deferral_step(
                conn, policy=DeferralPolicy(enabled=True),
                deployment_name="svc", branch_name="default",
                catalog_version_id=1, target_generation_id="0",
                candidate_edge_ids=None, dirty_node_ids=None,
                check_scoped_edges=lambda ids: ([], []),
                check_cardinality=crash,
            )

    assert fx.edge_triples(clean_runtime) == set()
    assert fx.waiting_rows(clean_runtime) == before
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        retried = run_edge_deferral_step(
            conn, policy=DeferralPolicy(enabled=True),
            deployment_name="svc", branch_name="default",
            catalog_version_id=1, target_generation_id="0",
            candidate_edge_ids=None, dirty_node_ids=None,
            check_scoped_edges=lambda ids: ([], []),
            check_cardinality=lambda ids, nodes: [],
        )
    assert retried.promoted == 1
    assert len(fx.edge_rows(clean_runtime)) == 1
    assert fx.waiting_rows(clean_runtime) == []


# --- High 2: the grant registry and the waiting table -----------------------


def test_apply_grant_registry_keeps_the_waiting_table_unreadable(
    clean_runtime: str, tmp_path,
) -> None:
    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        physical = [
            f"public.{row[0]}" for row in conn.execute(
                "SELECT physical_table FROM _chronos_branch_tables"
                " WHERE table_name = 'okg.graph_edge_deferrals'",
            ).fetchall()
        ]
        assert physical
        for table in physical:
            conn.execute(f"GRANT SELECT ON TABLE {table} TO okg_mcp")
        conn.execute("SELECT okg.apply_grant_registry()")
        graph = conn.execute(
            "SELECT 'public.' || physical_table FROM _chronos_branch_tables"
            " WHERE table_name = 'okg.graph_edges'",
        ).fetchone()[0]

        def can_read(table: str) -> bool:
            return bool(conn.execute(
                "SELECT has_table_privilege('okg_mcp', %s, 'SELECT')",
                (table,),
            ).fetchone()[0])

        assert can_read(graph), "the pattern grant still serves the graph"
        assert not any(can_read(table) for table in physical)
        drift = [
            row for row in conn.execute(
                "SELECT finding_kind, relation FROM okg.grant_registry_drift()",
            ).fetchall()
            if row[1] in physical
        ]
        assert drift == []
        # A direct grant is a finding, not silently covered by the pattern.
        conn.execute(f"GRANT SELECT ON TABLE {physical[0]} TO okg_mcp")
        exposed = [
            row for row in conn.execute(
                "SELECT finding_kind, relation FROM okg.grant_registry_drift()",
            ).fetchall()
            if row[1] == physical[0]
        ]
        assert exposed and exposed[0][0] == "unregistered_grant"
        conn.execute("SELECT okg.apply_grant_registry()")
        assert not can_read(physical[0])


# --- High 3: undo and retirement follow the effective policy ----------------


def _undo_with(clean_runtime: str, dep) -> list[tuple[str, str, str]]:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest.compensation import retract_chronos_run
    from okg.substrate.ingest.run_base import (
        hold_run_liveness,
        run_base_checkpoint_id,
        take_run_base,
    )
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    fx.write_fixture(dep, "a", [fx.node("widget:2", "widget")], [])
    fx.write_fixture(
        dep, "b", [fx.node("note:1", "note")], [fx.edge("note:1", "widget:2")],
    )
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    run_id = "undo-policy-run"
    checkpoint_id = run_base_checkpoint_id("default", run_id)
    live = hold_run_liveness(clean_runtime, checkpoint_id)
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        take_run_base(
            store, deployment="svc", branch="default", source="svc.a",
            run_id=run_id,
        )
        store.apply_facts(
            [NodeFact("widget:1", "widget", {"text": "w"},
                      {"id": "widget:1"}, {"rev": 1})],
            source="svc.a", run_id=run_id, strict_endpoints=False,
        )
        store.apply_facts(
            [
                NodeFact("note:3", "note", {"text": "n"}, {"id": "note:3"},
                         {"rev": 1}),
                EdgeFact("note:3", "widget:1", "references",
                         source_record_id={"id": "e"},
                         source_revision={"rev": 1}),
            ],
            source="svc.b", run_id="b-later", strict_endpoints=False,
        )
        retract_chronos_run(
            clean_runtime, admission_id=None, source="svc.a", run_id=run_id,
        )
    finally:
        live.close()
        store.close()
    return sorted(fx.edge_triples(clean_runtime))


def test_undo_keeps_an_edge_only_when_deferral_is_on(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    dep = fx.build_deployment(tmp_path, clean_runtime)
    on = _undo_with(clean_runtime, dep)
    assert ("note:3", "references", "widget:1") in on


def test_undo_deletes_the_edge_when_deferral_is_off(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    dep = fx.build_deployment(
        tmp_path, clean_runtime,
        consistency={"defer_dangling_edges": False,
                     "max_deferred_edge_fraction": 1.0},
    )
    off = _undo_with(clean_runtime, dep)
    assert off == [("note:1", "references", "widget:2")]


def test_retirement_leaves_the_edge_only_when_deferral_is_on(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.source_retirement import plan_source_retirement

    def identity(*_a, **_k):
        return ("a", {"ownership_id": "svc.a"}, ("a", "svc.a"), "digest")

    monkeypatch.setattr(
        "okg.substrate.source_retirement._registry_identity", identity,
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(
        dep, "a", [fx.node("widget:1", "widget"), fx.node("widget:2", "widget")],
        [],
    )
    _b(dep)
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert plan_source_retirement(
            conn, source="a", deployment_name="svc",
        ).edges == ()

    # The same graph with the policy off: the edge goes with the node.
    manifest = (dep / "deployment.yaml")
    import yaml

    document = yaml.safe_load(manifest.read_text())
    document["consistency"] = {"defer_dangling_edges": False}
    manifest.write_text(yaml.safe_dump(document))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        plan = plan_source_retirement(conn, source="a", deployment_name="svc")
    assert [e["src"] for e in plan.edges] == ["note:1"]


# --- High 4: a waiting edge a source no longer emits is retracted -----------


def test_reset_cursor_retracts_a_waiting_edge_the_source_dropped(
    clean_runtime: str, tmp_path,
) -> None:
    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    assert len(fx.waiting_rows(clean_runtime)) == 1
    fx.write_fixture(dep, "b", [fx.node("note:1", "note")], [])

    outcome = fx.ingest(clean_runtime, dep, "b", reset_cursor=True)

    assert outcome.any_failed is False, outcome.report
    assert fx.waiting_rows(clean_runtime) == []
    assert fx.node_ids(clean_runtime, "b") == {"note:1"}


# --- Medium 7: step 1 leaves published non-dangling edges alone -------------


def test_published_edge_with_a_changed_endpoint_subtype_is_not_moved(
    clean_runtime: str, tmp_path,
) -> None:
    """a re-types widget:1 as a gadget under a published edge. The edge is not
    dangling, so the step does not move it; the gate judges it as before."""
    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    _b(dep)
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    assert len(fx.edge_rows(clean_runtime)) == 1

    fx.write_fixture(dep, "a", [fx.node("widget:1", "gadget")], [])
    outcome = fx.ingest(clean_runtime, dep, "a")

    assert fx.waiting_rows(clean_runtime) == []
    assert _statuses(outcome) == {"blocked"}, outcome.report


def test_non_deferrable_dangling_edge_still_blocks(
    clean_runtime: str, tmp_path,
) -> None:
    dep = fx.build_deployment(
        tmp_path, clean_runtime, b_endpoint_absent_refuse=True,
    )
    keep = fx.node("widget:2", "widget")
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget"), keep], [])
    _b(dep)
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False

    fx.write_fixture(dep, "a", [keep], [])
    outcome = fx.ingest(clean_runtime, dep, "a")

    assert _statuses(outcome) == {"blocked"}, outcome.report
    assert fx.waiting_rows(clean_runtime) == []


# --- Rechecks ----------------------------------------------------------------


def test_held_rows_rechecked_after_a_profile_change(
    clean_runtime: str, tmp_path,
) -> None:
    """A held row recorded against an older profile fingerprint is checked
    again on the incremental path even when none of its endpoints changed."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.edge_deferrals import (
        DeferralPolicy,
        run_edge_deferral_step,
    )

    dep = fx.build_deployment(tmp_path, clean_runtime)
    fx.write_fixture(dep, "a", [fx.node("widget:1", "gadget")], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    _b(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    (held,) = fx.waiting_rows(clean_runtime)
    assert held["state"] == "subtype_mismatch"
    current = held["profile_fingerprint"]
    assert current

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        session = store.checkout()
        with session.transaction():
            session.upsert_rows("okg.graph_edge_deferrals", [
                {**held, "profile_fingerprint": "sha256:older-profile"},
            ])
        assert fx.waiting_rows(clean_runtime)[0]["profile_fingerprint"] == (
            "sha256:older-profile"
        )
        result = run_edge_deferral_step(
            conn, policy=DeferralPolicy(enabled=True),
            deployment_name="svc", branch_name="default",
            catalog_version_id=1, target_generation_id="0",
            candidate_edge_ids=[], dirty_node_ids=[],
            check_scoped_edges=lambda ids: ([], []),
            check_cardinality=lambda ids, nodes: [],
        )

    assert result.held == {"subtype_mismatch": 1}
    assert fx.waiting_rows(clean_runtime)[0]["profile_fingerprint"] == current


# --- Generation metadata, Nomos lineage, image prune ------------------------


def test_generation_metadata_counts_deferred_promoted_and_waiting(
    clean_runtime: str, tmp_path,
) -> None:
    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b(dep)
    first = fx.ingest(clean_runtime, dep, "b")

    def step(outcome) -> dict[str, Any]:
        """The gate passes of a run, added up; the last one's waiting set."""
        passes = [
            gate["details"]["edge_deferrals"]
            for publish in outcome.report["publishes"]
            for gate in publish["consistency_gates"]
            if "edge_deferrals" in (gate.get("details") or {})
        ]
        assert passes, outcome.report["publishes"]
        total = dict(passes[-1])
        for key in ("deferred", "promoted"):
            total[key] = sum(p[key] for p in passes)
        return total

    deferred = step(first)
    assert deferred["deferred"] == 1 and deferred["promoted"] == 0
    assert deferred["waiting_by_state"] == {"waiting": 1}
    assert deferred["waiting_total"] == 1
    assert deferred["oldest_deferred_at"]

    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    run = fx.ingest(clean_runtime, dep, "a")
    promoted = step(run)
    # The gate may judge a publish more than once; the metadata of the last
    # pass names the waiting set the generation leaves behind.
    assert promoted["waiting_by_state"] == {}
    assert promoted["waiting_total"] == 0
    assert promoted["oldest_deferred_at"] is None


def test_promoted_edge_keeps_nomos_lineage(clean_runtime: str, tmp_path) -> None:
    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    (held,) = fx.waiting_rows(clean_runtime)
    lineage = held["attrs"]["nomos_source_lineage"]
    assert lineage["status"] == "ok" and lineage["source_ids"] == ["svc.b"]

    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False

    (promoted,) = fx.edge_rows(clean_runtime)
    assert promoted["attrs"]["nomos_source_lineage"] == lineage
    assert promoted["nomos_read_class"] == held["nomos_read_class"]


def test_image_prune_reads_no_waiting_row(clean_runtime: str, tmp_path) -> None:
    """Image references live in node attrs; the waiting table holds edges, so
    image prune neither reads nor needs to keep it."""
    from okg.substrate.images.prune import image_reference_sources

    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            sources, _registered = image_reference_sources(cur)
            waiting_physical = {
                row[0] for row in conn.execute(
                    "SELECT physical_table FROM _chronos_branch_tables WHERE "
                    "table_name = 'okg.graph_edge_deferrals'",
                ).fetchall()
            }
    assert not any(
        source.relation.split(".")[-1] in waiting_physical for source in sources
    )
    assert len(fx.waiting_rows(clean_runtime)) == 1


# --- The read boundary -------------------------------------------------------


def test_no_read_operator_serves_a_waiting_edge(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.mcp.server import build_server

    dep = fx.build_deployment(tmp_path, clean_runtime)
    # One published edge (the sibling the calls must still return) and one
    # waiting edge of the same type from the same note.
    fx.write_fixture(dep, "a", [fx.node("widget:2", "widget")], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    fx.write_fixture(
        dep, "b", [fx.node("note:1", "note")],
        [fx.edge("note:1", "widget:2"), fx.edge("note:1", "widget:1")],
    )
    outcome = fx.ingest(clean_runtime, dep, "b")
    generation = str(outcome.report["publishes"][-1]["generation_id"])
    assert [r["dst"] for r in fx.waiting_rows(clean_runtime)] == ["widget:1"]
    assert fx.edge_triples(clean_runtime) == {
        ("note:1", "references", "widget:2"),
    }

    mcp = build_server(dsn=clean_runtime, deployment_name="svc")
    edges = {"kind": "edges", "edge_type": "references"}
    calls = [
        ("inspect", {"target": {"kind": "node", "node_id": "note:1"}}),
        ("inspect", {"target": {"kind": "graph"}}),
        ("expand", {"input": {"node_id": "note:1"}, "path": ["references:out"]}),
        ("search", {"query": "note"}),
        # Each would return the waiting edge if it were served: same edge
        # type, same endpoints.
        ("filter", {"input": edges, "predicate": {"src": "note:1"}}),
        ("filter", {"input": edges, "predicate": {}}),
        ("map", {"input": edges}),
        ("aggregate", {"input": edges, "by": ["edge_type"],
                       "metrics": ["count"]}),
    ]

    async def drive() -> list[str]:
        seen = []
        for name, arguments in calls:
            result = await mcp.call_tool(name, arguments)
            seen.append(json.dumps(
                getattr(result, "structuredContent", None)
                or [getattr(c, "text", "") for c in getattr(
                    result, "content", result[0] if isinstance(result, tuple)
                    else result)],
                default=str,
            ))
        return seen

    served = asyncio.run(drive())
    names = [name for name, _ in calls]
    # The published sibling is returned by the operators that list edges, so
    # the arguments do reach edges; the waiting edge never appears.
    for index in (names.index("expand"), 4, 5, 6):
        assert "widget:2" in served[index], (calls[index][0], served[index])
    assert not any("widget:1" in text for text in served), served
    aggregate = served[names.index("aggregate")]
    # The grouped count of the edge type is the published edge only.
    grouped = json.loads(aggregate)[0]
    assert "edge_type\tcount\nreferences\t1\n" in grouped, grouped

    # History: the deferring generation's graph has no such edge.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        history = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(generation).query(
            "SELECT edge_id FROM okg.graph_edges",
        )
        assert [row["edge_id"] for row in history] == [
            row["edge_id"] for row in fx.edge_rows(clean_runtime)
        ], "only the published sibling is in the generation"
        # The physical tables, read as the MCP role (the query tool's path).
        physical = [
            row[0] for row in conn.execute(
                "SELECT physical_table FROM _chronos_branch_tables WHERE "
                "table_name = 'okg.graph_edge_deferrals'",
            ).fetchall()
        ]
        for table in physical:
            with conn.transaction():
                conn.execute("SET LOCAL ROLE okg_mcp")
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    conn.execute(f'SELECT * FROM public."{table}"').fetchall()


# --- Operator views ----------------------------------------------------------


def test_doctor_check_reports_counts_withholds_ids_and_warns_without_failing(
    clean_runtime: str, tmp_path, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import datetime, timedelta, timezone

    from okg.substrate.cli import doctor
    from okg.substrate.graph_store_internal import edge_deferrals

    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    capsys.readouterr()

    code = doctor._doctor_deferred_edges(
        argparse.Namespace(dsn=clean_runtime, deployment=None), as_json=True,
    )
    fresh = json.loads(capsys.readouterr().out)
    assert code == 0 and fresh["ok"] is True and fresh["total"] == 1
    assert fresh["groups"][0]["state"] == "waiting"
    assert "sample" not in fresh and "widget:1" not in json.dumps(fresh)
    assert fresh["warnings"] == []

    real = edge_deferrals.deferred_edges_report

    def later(conn, **kwargs):
        return real(
            conn, now=datetime.now(timezone.utc) + timedelta(hours=30), **kwargs,
        )

    monkeypatch.setattr(edge_deferrals, "deferred_edges_report", later)
    code = doctor._doctor_deferred_edges(
        argparse.Namespace(dsn=clean_runtime, deployment=None), as_json=True,
    )
    aged = json.loads(capsys.readouterr().out)
    assert code == 0, "an old waiting edge is a warning, not a failure"
    assert aged["warnings"] == ["oldest_waiting_edge_past_warning_age"]
    assert source_sync_hold(
        dsn=clean_runtime, deployment="svc", sources={"b": "svc.b"},
    ) is None


def test_unpublished_owner_ceiling_bounds_what_the_cap_does_not_count() -> None:
    from okg.substrate.graph_store_internal.edge_deferrals import DeferralPolicy
    from okg.substrate.producer_authority import (
        DEFERRED_EDGES_OVER_LIMIT_CODE,
        ProducerPolicyViolation,
    )
    from tests.substrate.test_edge_deferral_admission import (
        _edge,
        _evaluate,
        _profile,
    )

    policy = DeferralPolicy(
        enabled=True, max_per_run=1, max_fraction=1.0,
        cap_mode="owner_published", unpublished_owner_ceiling=3,
    )
    edges = [_edge(f"widget:{i}") for i in range(4)]
    ok = _evaluate(
        _profile(), edges[:3], policy=policy, owner_published=lambda _s: False,
    )
    assert ok["producer_policy"]["deferred_edges"]["count"] == 3
    with pytest.raises(ProducerPolicyViolation) as refused:
        _evaluate(
            _profile(), edges, policy=policy,
            owner_published=lambda _s: False,
        )
    assert refused.value.code == DEFERRED_EDGES_OVER_LIMIT_CODE


# --- Delta review: rollback writes nothing; real checks are atomic -----------


def _held_cardinality_state(clean_runtime: str, tmp_path):
    """note:1 -> widget:2 published; note:1 -> widget:1 held (cardinality)."""
    dep = fx.build_deployment(tmp_path, clean_runtime, at_most_one_target=True)
    fx.write_fixture(dep, "a", [fx.node("widget:2", "widget")], [])
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    fx.write_fixture(
        dep, "b", [fx.node("note:1", "note")],
        [fx.edge("note:1", "widget:2"), fx.edge("note:1", "widget:1")],
    )
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    fx.write_fixture(
        dep, "a", [fx.node("widget:1", "widget"), fx.node("widget:2", "widget")],
        [],
    )
    assert fx.ingest(clean_runtime, dep, "a").any_failed is False
    assert [r["state"] for r in fx.waiting_rows(clean_runtime)] == [
        "cardinality_violation",
    ]
    return dep


def test_a_held_row_recheck_writes_nothing(
    clean_runtime: str, tmp_path,
) -> None:
    """Rechecking a held row (trial, failure, rollback) leaves the working
    segment empty, so the publish that follows is a no-op."""
    from okg.substrate.graph_store_internal.edge_deferrals import (
        DeferralPolicy,
        run_edge_deferral_step,
    )
    from okg.substrate.publisher import Publisher

    _held_cardinality_state(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        catalog = conn.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            "WHERE status = 'applied'",
        ).fetchone()[0]
        assert Publisher._chronos_current_segment_has_graph_writes(
            conn, branch_name="default",
        ) is False
        result = run_edge_deferral_step(
            conn, policy=DeferralPolicy(enabled=True),
            deployment_name="svc", branch_name="default",
            catalog_version_id=catalog, target_generation_id="0",
            candidate_edge_ids=None, dirty_node_ids=None,
            check_scoped_edges=lambda ids: ([], []),
            check_cardinality=lambda ids, nodes: list(ids),
        )
        assert result.held == {"cardinality_violation": 1}
        assert Publisher._chronos_current_segment_has_graph_writes(
            conn, branch_name="default",
        ) is False, "the recheck left a write behind"


def test_unchanged_republish_after_a_recheck_is_a_noop(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.consistency.transform_gate import (
        TransformConsistencyGate,
    )

    dep = _held_cardinality_state(clean_runtime, tmp_path)
    before = fx.waiting_rows(clean_runtime)
    # Judge the held row again on the full path.
    monkeypatch.setattr(
        TransformConsistencyGate, "_incremental_fallback_reason",
        lambda self, **_kwargs: "forced_full_for_test",
    )
    again = fx.ingest(clean_runtime, dep, "a")
    assert again.any_failed is False, again.report
    assert fx.waiting_rows(clean_runtime) == before
    assert [r["state"] for r in fx.waiting_rows(clean_runtime)] == [
        "cardinality_violation",
    ]
    assert fx.edge_triples(clean_runtime) == {
        ("note:1", "references", "widget:2"),
    }


def test_real_gate_checks_leave_nothing_behind_when_the_trial_crashes(
    clean_runtime: str, tmp_path,
) -> None:
    """The gate's own narrowing and cardinality queries run inside the trial.
    A failure injected after they ran must roll the trial write back, so the
    checks do not commit the connection's transaction on the way."""
    from okg.substrate.consistency.transform_gate import (
        TransformConsistencyGate,
        _family_activations,
    )
    from okg.substrate.graph_store_internal.edge_deferrals import (
        DeferralPolicy,
    )

    dep = fx.build_deployment(tmp_path, clean_runtime)
    _b(dep)
    assert fx.ingest(clean_runtime, dep, "b").any_failed is False
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget")], [])
    assert fx.ingest(clean_runtime, dep, "a", publish=False).any_failed is False
    before = fx.waiting_rows(clean_runtime)

    gate = TransformConsistencyGate(edge_deferral=DeferralPolicy(enabled=True))
    real_scoped = gate._eval_chronos_scoped_edges
    ran: list[str] = []

    def crash_after_real_checks(*args, **kwargs):
        ran.append("real narrowing check")
        real_scoped(*args, **kwargs)
        raise RuntimeError("injected failure after the real checks ran")

    gate._eval_chronos_scoped_edges = crash_after_real_checks  # type: ignore[method-assign]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        catalog = conn.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            "WHERE status = 'applied'",
        ).fetchone()[0]
        with pytest.raises(RuntimeError, match="injected failure"):
            gate._run_edge_deferral_step(
                conn, target_generation_id="0", catalog_version_id=catalog,
                deployment_name="svc", branch_name="default",
                activations=_family_activations(conn, catalog),
                candidate_edge_ids=None, dirty_node_ids=None,
            )

    assert ran, "the real check never ran"
    assert fx.edge_triples(clean_runtime) == set(), (
        "the trial row was committed by the checks"
    )
    assert fx.waiting_rows(clean_runtime) == before
