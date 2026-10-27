"""Issue #3024: a published source reads visible in the source-frontier view.

`okg.source_frontier_state()` used to call every source whose
`okg.source_watermarks.last_published_rev` was empty `unpublished_watermark`.
No publish writes that column, so every healthy published source read as not
visible and `source_frontier_report` answered `attention_required`.

The view now asks the latest published checkpoint instead: a source reads
`unpublished_writes` only while it has graph row versions -- additions,
updates or tombstones -- visible at the branch head and not at that
checkpoint. Such a source is waiting for the next publish, not broken, so it
does not make the report `attention_required`; neither does an embedding
write, which re-writes a published node under its source after the publish.

A source whose newest run FAILED reads `last_run_failed` and `blocked`:
`sync.last_run` is written only on success, and the failed run leaves only
`sync.record_set_coverage` with its status. On the live comp-ops data the
checkpoint rule alone would have called two failed sources visible.

A source whose unpublished work sits behind a blocked publish reads
`publish_blocked`: the next publish will not take it until a person acts.

The ingest tests drive a real `run_ingest` and a real publish of the layered
`svc` fixture on a disposable database; none plants a generation. Where a
state has no real producer the test edits one stored value and says so: a
coverage timestamp and status (the ordering guard), a blob reference (a
missing blob), a checkpoint segment (missing published state). The helper
tests for edges and forks drive the graph store and publisher directly.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.ingest import IngestOptions, run_ingest
from okg.substrate.mcp.source_coverage import _run_rows
from okg.substrate.source_sync_diagnostics import source_frontier_report
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.source_jobs_inline import use_inline_source_jobs
from tests.substrate.test_ingest_service import (
    _claim_layered_fixture_catalog,
    _commit_layered_fixture_deployment,
    _write_layered_fixture_deployment,
)


pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _inline_source_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    # These tests prove what the source-frontier view reads from
    # database state, not the source child process;
    # `tests/substrate/ingest/test_source_jobs.py` proves that boundary.
    use_inline_source_jobs(monkeypatch)

_SOURCES = ("svc.first", "svc.second")


def _deployment(dsn: str, tmp_path: Path) -> Path:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(dsn, dep)
    return dep


def _ingest(
    dsn: str,
    dep: Path,
    *,
    publish: bool,
    include: set[str] | None = None,
    expect_failed: bool = False,
    allow_full_retraction: tuple[str, ...] = (),
) -> None:
    outcome = run_ingest(
        IngestOptions(
            dsn=dsn,
            deployment="svc",
            deployment_dir=dep,
            include=include,
            include_predecessors=include is None,
            include_dependents=False,
            publish=publish,
            allow_full_retraction=allow_full_retraction,
        ),
    )
    assert outcome.any_failed is expect_failed, json.dumps(
        outcome.report, default=str,
    )


def _rewrite_first(dep: Path, nodes: list[dict[str, Any]]) -> None:
    (dep / "first.json").write_text(json.dumps({"nodes": nodes, "edges": []}))
    # A publish refuses a deployment outside a clean Git tree.
    _commit_layered_fixture_deployment(dep, message="first source changed")


def _states(dsn: str) -> dict[str, str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            """
            SELECT source_name, read_visibility_state
              FROM okg.v_source_frontier_state
             WHERE deployment = 'svc'
            """
        ).fetchall()
    return {str(name): str(state) for name, state in rows}


def _frontier(dsn: str) -> dict[str, str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            """
            SELECT source_name, frontier_state
              FROM okg.v_source_frontier_state
             WHERE deployment = 'svc'
            """
        ).fetchall()
    return {str(name): str(state) for name, state in rows}


def _row(dsn: str, source: str) -> dict[str, Any]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        cur = conn.execute(
            "SELECT * FROM okg.v_source_frontier_state WHERE source_name = %s",
            (source,),
        )
        names = [d.name for d in cur.description]
        row = cur.fetchone()
    assert row is not None, source
    return dict(zip(names, row))


def _progress(dsn: str, source: str) -> dict[str, dict[str, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT key, value FROM okg.source_progress"
            " WHERE source_name = %s AND key IN"
            " ('sync.last_run', 'sync.record_set_coverage')",
            (source,),
        ).fetchall()
    return {str(key): json.loads(value) for key, value in rows}


def _break_first(dep: Path) -> None:
    """Make the first source's reader fail: its fixture is not JSON."""
    (dep / "first.json").write_text("{ this is not json")
    _commit_layered_fixture_deployment(dep, message="first source broken")


def _latest_generation(dsn: str) -> str | None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            """
            SELECT id FROM okg.v_generations
             WHERE deployment_name = 'svc'
             ORDER BY published_at DESC NULLS LAST, id DESC
             LIMIT 1
            """
        ).fetchone()
    return None if row is None else str(row[0])


def _published_revs(dsn: str) -> dict[str, Any]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT source, last_published_rev FROM okg.source_watermarks"
            " WHERE source = ANY(%s)",
            (list(_SOURCES),),
        ).fetchall()
    return {str(source): rev for source, rev in rows}


def test_published_sources_read_visible_and_report_ready(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """The #3024 reproduction: on dev both sources read unpublished_watermark."""
    dep = _deployment(clean_runtime, tmp_path)

    _ingest(clean_runtime, dep, publish=True)

    assert _latest_generation(clean_runtime) is not None
    # The premise of the bug: a real publish leaves last_published_rev empty.
    assert _published_revs(clean_runtime) == {
        source: {} for source in _SOURCES
    }
    assert _states(clean_runtime) == {
        source: "read_visible" for source in _SOURCES
    }
    report = source_frontier_report(clean_runtime, deployment="svc")
    assert report["status"] == "ready", report
    assert report["blocked_count"] == 0
    assert report["read_visibility_state_counts"] == {"read_visible": 2}
    # The MCP source_coverage run block reads the same view.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        runs, unavailable = _run_rows(conn, "svc")
    assert unavailable is None
    assert {
        name: runs[name]["read_visibility_state"] for name in _SOURCES
    } == {source: "read_visible" for source in _SOURCES}


def test_applied_but_unpublished_work_reads_not_visible_until_published(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """Changed and removed records stay not visible until the next publish."""
    dep = _deployment(clean_runtime, tmp_path)
    _ingest(clean_runtime, dep, publish=True)
    published = _latest_generation(clean_runtime)

    # An update the publish has not taken.
    _rewrite_first(dep, [{
        "node_id": "svc:first",
        "subtype": "document",
        "attrs": {"text": "first layer, edited"},
        "source_record_id": {"path": "first.json"},
    }])
    _ingest(clean_runtime, dep, publish=False, include={"first"})
    assert _latest_generation(clean_runtime) == published
    assert _states(clean_runtime) == {
        "svc.first": "unpublished_writes",
        "svc.second": "read_visible",
    }
    # Waiting for a publish is not something a person has to fix.
    report = source_frontier_report(clean_runtime, deployment="svc")
    assert report["status"] == "ready", report
    assert report["blocked_count"] == 0
    assert _frontier(clean_runtime)["svc.first"] == "publish_pending"

    _ingest(clean_runtime, dep, publish=True, include={"first"})
    assert _latest_generation(clean_runtime) != published
    assert _states(clean_runtime) == {
        source: "read_visible" for source in _SOURCES
    }

    # A removal the publish has not taken: the only row version the run
    # writes is a tombstone. Removing the source's only record empties it,
    # which the issue #2818 guard refuses unless the operator passes
    # `--allow-full-retraction first`.
    published = _latest_generation(clean_runtime)
    _rewrite_first(dep, [])
    _ingest(
        clean_runtime, dep, publish=False, include={"first"},
        allow_full_retraction=("first",),
    )
    assert _latest_generation(clean_runtime) == published
    assert _states(clean_runtime)["svc.first"] == "unpublished_writes"

    _ingest(clean_runtime, dep, publish=True, include={"first"})
    assert _states(clean_runtime) == {
        source: "read_visible" for source in _SOURCES
    }
    assert source_frontier_report(
        clean_runtime, deployment="svc",
    )["status"] == "ready"


def test_other_visibility_states_are_unchanged(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """no_published_generation, replay_required and record_set_blob_missing."""
    dep = _deployment(clean_runtime, tmp_path)

    _ingest(clean_runtime, dep, publish=False)
    assert _latest_generation(clean_runtime) is None
    assert _states(clean_runtime) == {
        source: "no_published_generation" for source in _SOURCES
    }

    _ingest(clean_runtime, dep, publish=True)
    assert _states(clean_runtime) == {
        source: "read_visible" for source in _SOURCES
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET replay_required = %s::jsonb, replay_required_at = now()
             WHERE source = 'svc.first'
            """,
            (json.dumps({"obligation": "authoritative_full_replay"}),),
        )
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET last_applied_rev = last_applied_rev || %s::jsonb
             WHERE source = 'svc.second'
            """,
            (json.dumps({"record_set_ref": {"sha256": "0" * 64}}),),
        )
    assert _states(clean_runtime) == {
        "svc.first": "replay_required",
        "svc.second": "record_set_blob_missing",
    }


def test_missing_published_segment_is_reported_not_assumed_visible(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """Without the checkpoint's segment the view cannot judge, and says so."""
    dep = _deployment(clean_runtime, tmp_path)
    _ingest(clean_runtime, dep, publish=True)
    generation = _latest_generation(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE public._chronos_branch_interval_checkpoints"
            " SET segment_id = -1 WHERE checkpoint_id = %s",
            (generation,),
        )
    assert _states(clean_runtime) == {
        source: "published_state_unavailable" for source in _SOURCES
    }
    assert _frontier(clean_runtime) == {
        source: "blocked" for source in _SOURCES
    }
    report = source_frontier_report(clean_runtime, deployment="svc")
    assert report["status"] == "attention_required", report


def test_failed_last_run_reads_blocked_until_a_successful_run(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """A published source whose newest run fails is flagged, then clears."""
    dep = _deployment(clean_runtime, tmp_path)
    _ingest(clean_runtime, dep, publish=True)
    published = _latest_generation(clean_runtime)
    succeeded = _row(clean_runtime, "svc.first")
    assert succeeded["read_visibility_state"] == "read_visible"
    assert succeeded["last_attempt_status"] == "OK"

    _break_first(dep)
    _ingest(
        clean_runtime, dep, publish=False, include={"first"},
        expect_failed=True,
    )
    assert _latest_generation(clean_runtime) == published

    # The premise: the failed run wrote no sync.last_run; only the coverage
    # row records it, under the same source name the runner uses.
    progress = _progress(clean_runtime, "svc.first")
    assert progress["sync.record_set_coverage"]["status"] == "FAIL"
    assert progress["sync.record_set_coverage"]["source_name"] == "svc.first"
    assert progress["sync.last_run"]["source_name"] == "svc.first"
    assert progress["sync.last_run"]["run_id"] == succeeded["last_run_id"]

    failed = _row(clean_runtime, "svc.first")
    assert failed["read_visibility_state"] == "last_run_failed"
    assert failed["frontier_state"] == "blocked"
    assert failed["last_attempt_status"] == "FAIL"
    # The successful run's id stays in last_run_id; the failed attempt is
    # named beside it, never hidden behind it.
    assert failed["last_run_id"] == succeeded["last_run_id"]
    assert failed["last_attempt_run_id"] != succeeded["last_run_id"]
    assert _states(clean_runtime)["svc.second"] == "read_visible"
    report = source_frontier_report(clean_runtime, deployment="svc")
    assert report["status"] == "attention_required", report
    assert report["blocked_count"] == 1
    assert report["read_visibility_state_counts"] == {
        "last_run_failed": 1,
        "read_visible": 1,
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        runs, unavailable = _run_rows(conn, "svc")
    assert unavailable is None
    assert runs["svc.first"]["read_visibility_state"] == "last_run_failed"
    assert runs["svc.first"]["frontier_state"] == "blocked"

    # Repaired: the next successful run clears the flag by itself.
    _rewrite_first(dep, [{
        "node_id": "svc:first",
        "subtype": "document",
        "attrs": {"text": "first layer, repaired"},
        "source_record_id": {"path": "first.json"},
    }])
    _ingest(clean_runtime, dep, publish=True, include={"first"})
    assert _states(clean_runtime) == {
        source: "read_visible" for source in _SOURCES
    }
    assert source_frontier_report(
        clean_runtime, deployment="svc",
    )["status"] == "ready"


def test_failed_first_run_reads_last_run_failed_with_or_without_watermark(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """A source that never succeeded reports its failure, not no_watermark.

    A real failed first run leaves an empty watermark row (as on the live
    comp-ops deployment). The second half removes that row, which no ingest
    path does, to show last_run_failed outranks no_watermark too.
    """
    dep = _deployment(clean_runtime, tmp_path)
    _break_first(dep)
    _ingest(clean_runtime, dep, publish=True, include={"second"})
    assert _latest_generation(clean_runtime) is not None
    _ingest(
        clean_runtime, dep, publish=False, include={"first"},
        expect_failed=True,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        watermark = conn.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks"
            " WHERE source = 'svc.first'",
        ).fetchone()
    # The premise: the source never succeeded; its watermark is empty.
    assert watermark is not None and watermark[0] == {}
    assert "sync.last_run" not in _progress(clean_runtime, "svc.first")

    def assert_failed() -> None:
        row = _row(clean_runtime, "svc.first")
        assert row["read_visibility_state"] == "last_run_failed"
        assert row["frontier_state"] == "blocked"
        assert row["last_run_id"] is None
        assert row["last_attempt_status"] == "FAIL"
        report = source_frontier_report(clean_runtime, deployment="svc")
        assert report["status"] == "attention_required", report
        assert report["blocked_count"] == 1

    assert_failed()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "DELETE FROM okg.source_watermarks WHERE source = 'svc.first'"
        )
    assert_failed()


def test_newer_success_outranks_an_older_failure_record(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """The ordering guard: a stale FAIL never outlives a newer success.

    Every ingest path refreshes the coverage row, so no real run leaves a
    FAIL older than a success. To reach the guard the test moves the real
    failed run's coverage timestamp: first to the same instant as the real
    successful run's sync.last_run (a tie still counts as the newest run),
    then behind it. It also renames the failure status, since any status
    other than OK is a failure, not only FAIL.
    """
    dep = _deployment(clean_runtime, tmp_path)
    _ingest(clean_runtime, dep, publish=True)
    _break_first(dep)
    _ingest(
        clean_runtime, dep, publish=False, include={"first"},
        expect_failed=True,
    )
    assert _states(clean_runtime)["svc.first"] == "last_run_failed"

    def move_coverage(offset: str) -> None:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(
                """
                UPDATE okg.source_progress
                   SET updated_at = (
                         SELECT updated_at - %s::interval
                           FROM okg.source_progress
                          WHERE source_name = 'svc.first'
                            AND key = 'sync.last_run'
                       )
                 WHERE source_name = 'svc.first'
                   AND key = 'sync.record_set_coverage'
                """,
                (offset,),
            )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            UPDATE okg.source_progress
               SET value = jsonb_set(value::jsonb, '{status}', '"ERROR"')::text
             WHERE source_name = 'svc.first'
               AND key = 'sync.record_set_coverage'
            """
        )
    assert _row(clean_runtime, "svc.first")["last_attempt_status"] == "ERROR"
    assert _states(clean_runtime)["svc.first"] == "last_run_failed"
    move_coverage("0 seconds")
    assert _states(clean_runtime)["svc.first"] == "last_run_failed"
    move_coverage("1 second")
    assert _states(clean_runtime)["svc.first"] == "read_visible"


def _embed_first(dsn: str) -> None:
    """Write an embedding for the published svc:first node (real write path)."""
    from okg.substrate.embeddings import (
        EmbeddingConfig,
        build_embedding_execution_plan,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.runtime.embed_workflow import _embed_and_write_one

    with psycopg.connect(dsn) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout()
        rows = graph.query(
            "SELECT text_hash, attrs FROM okg.graph_nodes"
            " WHERE node_id = 'svc:first'"
        )
    assert len(rows) == 1 and rows[0]["text_hash"], rows

    outcome = _embed_and_write_one(
        dsn=dsn,
        node_id="svc:first",
        text=str(rows[0]["attrs"]["text"]),
        text_hash=str(rows[0]["text_hash"]),
        embedding_plan=build_embedding_execution_plan(
            EmbeddingConfig(enabled=True, backend="hash")
        ),
    )
    assert outcome.get("skipped") is not True, outcome
    assert outcome["cache_hit"] is False


def test_embedding_write_after_publish_is_pending_not_blocked(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """An embedding write re-writes a published node under its source.

    The source then reads unpublished_writes until the next publish, and the
    report stays ready: nobody has to act.
    """
    dep = _deployment(clean_runtime, tmp_path)
    _ingest(clean_runtime, dep, publish=True)
    published = _latest_generation(clean_runtime)
    _embed_first(clean_runtime)
    assert _latest_generation(clean_runtime) == published

    assert _states(clean_runtime) == {
        "svc.first": "unpublished_writes",
        "svc.second": "read_visible",
    }
    assert _frontier(clean_runtime)["svc.first"] == "publish_pending"
    report = source_frontier_report(clean_runtime, deployment="svc")
    assert report["status"] == "ready", report
    assert report["blocked_count"] == 0


def test_missing_blob_outranks_a_pending_embedding_write(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """A missing record-set blob needs a person even while work is pending.

    The embedding write is real; the cursor's blob reference is pointed at a
    blob that does not exist, as in test_other_visibility_states_are_unchanged.
    """
    dep = _deployment(clean_runtime, tmp_path)
    _ingest(clean_runtime, dep, publish=True)
    _embed_first(clean_runtime)
    assert _states(clean_runtime)["svc.first"] == "unpublished_writes"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            UPDATE okg.source_watermarks
               SET last_applied_rev = last_applied_rev || %s::jsonb
             WHERE source = 'svc.first'
            """,
            (json.dumps({"record_set_ref": {"sha256": "0" * 64}}),),
        )
    assert _states(clean_runtime) == {
        "svc.first": "record_set_blob_missing",
        "svc.second": "read_visible",
    }
    report = source_frontier_report(clean_runtime, deployment="svc")
    assert report["status"] == "attention_required", report
    assert report["blocked_count"] == 1


def test_blocked_publish_flags_its_unpublished_work_until_a_clean_publish(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """A publish a consistency gate blocks leaves its work unpublished.

    The blocked publish cuts an okg_generation checkpoint with status blocked
    and the published generation stays put, so the next publish will not take
    the work until a person removes the cause. The source reads
    publish_blocked and the report attention_required; once the gate is gone
    a clean publish returns it to read_visible.
    """
    import yaml

    from tests.substrate.test_ingest_service import _blocking_consistency_gate

    dep = _deployment(clean_runtime, tmp_path)
    _ingest(clean_runtime, dep, publish=True)
    published = _latest_generation(clean_runtime)
    assert source_frontier_report(
        clean_runtime, deployment="svc",
    )["status"] == "ready"

    deployment_path = dep / "deployment.yaml"
    clean = deployment_path.read_text()
    gated = yaml.safe_load(clean)
    gated["consistency_gates"] = [_blocking_consistency_gate()]
    deployment_path.write_text(yaml.safe_dump(gated))
    (dep / "first.json").write_text(json.dumps({"nodes": [{
        "node_id": "svc:first",
        "subtype": "document",
        "attrs": {"text": "first layer, behind a blocked publish"},
        "source_record_id": {"path": "first.json"},
    }], "edges": []}))
    _claim_layered_fixture_catalog(clean_runtime, dep)
    _ingest(
        clean_runtime, dep, publish=True, include={"first"},
        expect_failed=True,
    )

    # The premise: the newest generation checkpoint is blocked and the
    # published one did not move.
    assert _latest_generation(clean_runtime) == published
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        newest = conn.execute(
            """
            SELECT checkpoint_id, (metadata::jsonb)->>'status'
              FROM public._chronos_branch_interval_checkpoints
             WHERE (metadata::jsonb)->>'kind' = 'okg_generation'
               AND (metadata::jsonb)->>'deployment_name' = 'svc'
             ORDER BY created_at::timestamptz DESC, checkpoint_id DESC
             LIMIT 1
            """
        ).fetchone()
        helper = conn.execute(
            "SELECT blocked_generation_id, blocked_generation_status"
            "  FROM okg.source_frontier_unpublished_writes()"
            " WHERE deployment = 'svc'"
        ).fetchone()
    assert newest is not None and newest[1] == "blocked", newest
    assert helper == (newest[0], "blocked")

    assert _states(clean_runtime) == {
        "svc.first": "publish_blocked",
        "svc.second": "read_visible",
    }
    assert _frontier(clean_runtime)["svc.first"] == "blocked"
    report = source_frontier_report(clean_runtime, deployment="svc")
    assert report["status"] == "attention_required", report
    assert report["blocked_count"] == 1

    # Cause removed: the next publish takes the work.
    deployment_path.write_text(clean)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    _ingest(clean_runtime, dep, publish=True, include={"first"})
    assert _latest_generation(clean_runtime) != published
    assert _states(clean_runtime) == {
        source: "read_visible" for source in _SOURCES
    }
    report = source_frontier_report(clean_runtime, deployment="svc")
    assert report["status"] == "ready", report


def _unpublished(dsn: str, deployment: str) -> tuple[Any, ...]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT generation_id, evaluated, unpublished_sources"
            "  FROM okg.source_frontier_unpublished_writes()"
            " WHERE deployment = %s",
            (deployment,),
        ).fetchone()
    assert row is not None, deployment
    return tuple(row)


def _publish_default(dsn: str, *, branch: str = "default") -> str:
    from okg.substrate.consistency import TransformConsistencyGate
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    out = Publisher(
        dsn, "default", branch_name=branch,
        consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert out["status"] == "published", out
    return str(out["generation_id"])


def _seed_ticket_catalog(conn: psycopg.Connection) -> None:
    seed_catalog(
        conn,
        subtypes=["ticket", "person"],
        narrowings=[("ticket", "mentions", "person")],
    )


def test_edge_only_change_reads_unpublished(clean_runtime: str) -> None:
    """A write that adds only an edge still counts as unpublished work."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_ticket_catalog(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact("t:1", "ticket", {}, {"id": "t:1"}, {"v": 1}),
                NodeFact("p:1", "person", {}, {"id": "p:1"}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
    generation = _publish_default(clean_runtime)
    assert _unpublished(clean_runtime, "default") == (generation, True, [])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [EdgeFact("t:1", "p:1", "mentions",
                      source_record_id={"id": "e"},
                      source_revision={"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        tables = dict(conn.execute(
            "SELECT table_name, physical_table FROM public._chronos_branch_tables"
            " WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')"
        ).fetchall())
        # The premise: the edge write added no node row version.
        node_versions = conn.execute(
            f'SELECT count(*) FROM public."{tables["okg.graph_nodes"]}"'
        ).fetchone()[0]
    assert node_versions == 2
    assert _unpublished(clean_runtime, "default") == (
        generation, True, ["default"],
    )


def test_fork_publish_is_judged_against_its_own_branch_head(
    clean_runtime: str,
) -> None:
    """A fork's newest published checkpoint is compared with the fork's head.

    Forks publish okg_generation checkpoints under the same deployment name on
    their own branch; the helper must judge such a checkpoint against that
    branch's head, not the default branch's.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    fork = "experiment:frontier3024"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_ticket_catalog(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("p:base", "person", {}, {"id": "p:base"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
    base = _publish_default(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(
            fork,
            from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": base},
        )
        store.apply_facts(
            [NodeFact("t:fork", "ticket", {}, {"id": "t:fork"}, {"v": 1})],
            branch=fork,
            source="default",
            strict_endpoints=True,
        )
    fork_generation = _publish_default(clean_runtime, branch=fork)
    assert _unpublished(clean_runtime, "default") == (
        fork_generation, True, [],
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact("t:fork2", "ticket", {}, {"id": "t:fork2"}, {"v": 1})],
            branch=fork,
            source="default",
            strict_endpoints=True,
        )
    assert _unpublished(clean_runtime, "default") == (
        fork_generation, True, ["default"],
    )


def test_unpublished_writes_helper_is_not_granted_to_readers(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """Readers reach the helper only through source_frontier_state().

    okg_mcp and app_ro hold no EXECUTE on the helper, yet okg_mcp still reads
    the view, because source_frontier_state() is SECURITY DEFINER.
    """
    dep = _deployment(clean_runtime, tmp_path)
    _ingest(clean_runtime, dep, publish=True)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        grants = conn.execute(
            """
            SELECT r.rolname,
                   has_function_privilege(
                     r.oid, 'okg.source_frontier_unpublished_writes()',
                     'EXECUTE'),
                   has_function_privilege(
                     r.oid, 'okg.source_frontier_state()', 'EXECUTE')
              FROM pg_roles r
             WHERE r.rolname IN ('okg_mcp', 'app_ro')
             ORDER BY r.rolname
            """
        ).fetchall()
        assert grants == [("app_ro", False, True), ("okg_mcp", False, True)]
        conn.execute("SET ROLE okg_mcp")
        try:
            rows = conn.execute(
                "SELECT source_name, read_visibility_state"
                "  FROM okg.v_source_frontier_state WHERE deployment = 'svc'"
                " ORDER BY source_name"
            ).fetchall()
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(
                    "SELECT * FROM okg.source_frontier_unpublished_writes()"
                )
        finally:
            conn.execute("RESET ROLE")
    assert rows == [
        ("svc.first", "read_visible"),
        ("svc.second", "read_visible"),
    ]
