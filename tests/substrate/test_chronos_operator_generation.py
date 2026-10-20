"""Chronos-native operator surfaces for published generations."""
from __future__ import annotations

import argparse
import json
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.cli.generation import _cmd_generation
from okg.substrate.cli.main import _collect_status
from okg.substrate.consistency.sql_gate import (
    run_post_publish_consistency_checks,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.qa.metrics import snapshot


pytestmark = pytest.mark.postgres


def _seed_chronos_generation(dsn: str) -> tuple[str, str, int]:
    deployment_name = f"test-chronos-ops-{uuid4().hex[:8]}"
    target_generation_id = 990_001
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:chronos:one",
                    "document",
                    {"text": "one"},
                    {"id": "one"},
                    {"rev": 1},
                ),
                NodeFact(
                    "doc:chronos:two",
                    "document",
                    {"text": "two"},
                    {"id": "two"},
                    {"rev": 1},
                ),
                EdgeFact(
                    "doc:chronos:one",
                    "doc:chronos:two",
                    "references",
                    source_record_id={"id": "one-two"},
                    source_revision={"rev": 1},
                ),
            ],
            source="test.chronos.operator",
            run_id="run:test-chronos-operator",
        )
        checkpoint_id = store.checkpoint_generation(
            None,
            deployment_name=deployment_name,
            catalog_version_id=17,
            status="published",
            metadata={
                "target_generation_id": target_generation_id,
                "publisher": "test_chronos_operator",
                "node_count": 2,
                "edge_count": 1,
                "invariant_summary": {
                    "substrate_integrity": {"checks": {}},
                    "total_duration_ms": 0,
                    "total_budget_exceeded": False,
                },
                "excluded_sources": [],
                "publish_wallclock_seconds": 1.5,
            },
        )
    return deployment_name, checkpoint_id, target_generation_id


def test_chronos_operator_surfaces_resolve_target_generation(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    deployment_name, checkpoint_id, target_generation_id = (
        _seed_chronos_generation(clean_runtime)
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        assert (
            store.resolve_generation(
                target_generation_id,
                deployment_name=deployment_name,
            ).checkpoint_id
            == checkpoint_id
        )

    status = _collect_status(clean_runtime, deployment_name)
    assert status["publishing"]["latest_published_generation"] == checkpoint_id
    assert (
        status["publishing"]["latest_published_target_generation"]
        == target_generation_id
    )
    assert status["publishing"]["latest_published_node_count"] == 2
    assert status["publishing"]["latest_published_edge_count"] == 1
    assert (
        status["publishing"]["latest_published_catalog_version"]
        == 17
    )

    qa = snapshot(
        dsn=clean_runtime,
        deployment=deployment_name,
        generation_id=target_generation_id,
        skip_doctor=True,
    )
    assert qa.generation_id == checkpoint_id
    assert qa.nodes_live_delta == 2
    assert qa.edges_live_delta == 1
    assert qa.publish_wallclock_seconds == 1.5

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rerun = run_post_publish_consistency_checks(
            conn,
            [],
            deployment_name=deployment_name,
            generation_id=target_generation_id,
            scope_subject_ids=["doc:chronos:one"],
        )
    assert rerun["generation_id"] == target_generation_id
    assert rerun["native_generation_id"] == checkpoint_id
    assert rerun["catalog_version_id"] == 17
    assert rerun["node_count"] == 2
    assert rerun["edge_count"] == 1
    assert rerun["scope_subject_ids"] == ["doc:chronos:one"]

    rc = _cmd_generation(
        argparse.Namespace(
            subcmd="describe",
            generation_id=str(target_generation_id),
            deployment=None,
            dsn=clean_runtime,
        ),
        as_json=True,
    )
    assert rc == 0
    described = json.loads(capsys.readouterr().out)
    assert described["generation_id"] == checkpoint_id
    assert described["target_generation_id"] == target_generation_id
    assert described["deployment_name"] == deployment_name
    assert described["catalog_version_id"] == 17
    assert described["nodes_live_at_gen"] == 2
    assert described["edges_live_at_gen"] == 1



def _seed_published_source(dsn: str, *, maintain_rollup: bool) -> str:
    """Two nodes and an edge from `test.status.counts`, published.

    A publish maintains the console count rollup before it cuts the
    checkpoint; ``maintain_rollup=False`` leaves a generation without one.
    """
    from okg.substrate.publisher.console_count_rollup import (
        maintain_console_count_rollup,
    )

    deployment_name = f"test-status-counts-{uuid4().hex[:8]}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact("doc:counts:one", "document", {}, {"id": "1"}, {"rev": 1}),
                NodeFact("doc:counts:two", "document", {}, {"id": "2"}, {"rev": 1}),
                EdgeFact(
                    "doc:counts:one",
                    "doc:counts:two",
                    "references",
                    source_record_id={"id": "1-2"},
                    source_revision={"rev": 1},
                ),
            ],
            source="test.status.counts",
            run_id="run:test-status-counts",
        )
        if maintain_rollup:
            maintain_console_count_rollup(
                conn,
                generation_id=990_101,
                changed_node_ids=[],
                changed_edge_ids=[],
                prior_generation_checkpoint=None,
                force_full=True,
            )
        store.checkpoint_generation(
            None,
            deployment_name=deployment_name,
            catalog_version_id=17,
            status="published",
            metadata={"target_generation_id": 990_101},
        )
        conn.execute(
            "INSERT INTO okg.source_watermarks (source, last_reconcile_at) "
            "VALUES ('test.status.counts', now())"
        )
    return deployment_name


def test_status_counts_each_sources_published_rows(clean_runtime: str) -> None:
    """`okg status` reports each source's rows in the published generation.

    Sources write the Chronos graph, not the legacy fact log, so counting
    the fact log showed 0 nodes for every source right after a publish.
    """
    deployment_name = _seed_published_source(clean_runtime, maintain_rollup=True)

    status = _collect_status(clean_runtime, deployment_name)

    by_source = {row["source"]: row for row in status["sources"]}
    row = by_source["test.status.counts"]
    assert row["n_node_facts"] == 2
    assert row["n_edge_facts"] == 1
    assert status["source_counts"]["available"] is True


def test_status_reports_missing_count_rollup_instead_of_zero(
    clean_runtime: str,
) -> None:
    deployment_name = _seed_published_source(clean_runtime, maintain_rollup=False)

    status = _collect_status(clean_runtime, deployment_name)

    by_source = {row["source"]: row for row in status["sources"]}
    row = by_source["test.status.counts"]
    assert row["n_node_facts"] is None
    assert row["n_edge_facts"] is None
    assert status["source_counts"]["available"] is False
    assert "no_sentinel" in status["source_counts"]["reason"]
    # Unknown counts are not a publish failure: `errors` drives the exit code.
    assert status["errors"] == []


def test_status_does_not_read_a_carried_forward_rollup_when_disabled(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the rollup flag off the publisher carries the old rollup
    forward, so status reports the counts as unavailable, not stale."""
    deployment_name = _seed_published_source(clean_runtime, maintain_rollup=True)
    monkeypatch.setenv("OKG_IVM_CONSOLE_COUNT_ROLLUP", "0")

    status = _collect_status(clean_runtime, deployment_name)

    row = {r["source"]: r for r in status["sources"]}["test.status.counts"]
    assert row["n_node_facts"] is None
    assert status["source_counts"]["available"] is False
    assert "OKG_IVM_CONSOLE_COUNT_ROLLUP=0" in status["source_counts"]["reason"]
    assert status["errors"] == []


def test_status_reads_on_after_a_failed_rollup_query(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rollup query that fails in SQL aborts the transaction; status rolls
    it back so its later reads (live subtypes) still run."""
    from okg.substrate.console_views.console_count_rollup_read import (
        ConsoleCountRollupUnavailable,
    )

    deployment_name = _seed_published_source(clean_runtime, maintain_rollup=True)

    def failing_read(conn, *, generation_id, kinds):  # noqa: ANN001, ARG001
        try:
            conn.execute("SELECT 1 FROM okg.no_such_rollup_table")
        except psycopg.errors.UndefinedTable:
            pass
        raise ConsoleCountRollupUnavailable(
            generation_id, "table_unregistered", "fixture"
        )

    monkeypatch.setattr(
        "okg.substrate.source_live_counts.read_console_count_rollup",
        failing_read,
    )

    status = _collect_status(clean_runtime, deployment_name)

    assert status["source_counts"]["available"] is False
    assert status["errors"] == []
    assert {"subtype": "document", "count": 2} in status["live_subtypes"]
