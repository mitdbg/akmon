from __future__ import annotations

import json
import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql
import pytest

from okg.substrate.consistency.transform_gate import TransformConsistencyGate
from okg.substrate.db.management import database_health_report
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.graph_store_internal.common import NODE_TABLE
from okg.substrate.ingest import SourceRunContext, SourceRunner
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.git_graph_files import GitFilesSource
from okg.substrate.library.sources.git_graph_history import GitHistorySource
from okg.substrate.mcp import open_session
from okg.substrate.mcp.run_sql import run_sql
from okg.substrate.publisher import Publisher
from okg.substrate.runtime_queue_health import summarize_runtime_queue_health
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _seed_catalog_for_facts(
    dsn: str,
    facts: list[NodeFact | EdgeFact],
) -> None:
    subtypes = {
        fact.node_id: fact.subtype
        for fact in facts
        if isinstance(fact, NodeFact)
    }
    narrowings = {
        (subtypes[fact.src], fact.edge_type, subtypes[fact.dst])
        for fact in facts
        if isinstance(fact, EdgeFact)
    }
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=sorted(set(subtypes.values())),
            narrowings=sorted(narrowings),
        )


def _physical_source_interval_counts(dsn: str, source: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            """
            SELECT table_name, physical_table
              FROM _chronos_branch_tables
             WHERE backend = 'interval'
               AND table_name IN ('okg.graph_nodes', 'okg.graph_edges')
            """
        ).fetchall()
        for logical, physical in rows:
            count = conn.execute(
                sql.SQL("SELECT count(*) FROM {} WHERE source = %s").format(
                    sql.Identifier(str(physical))
                ),
                (source,),
            ).fetchone()[0]
            counts[str(logical)] = int(count)
    return counts


def _pruned_count(publish: dict) -> int:
    return sum(
        int(summary.get("details", {}).get("pruned_dangling_edge_count", 0) or 0)
        for summary in publish.get("consistency_gates", [])
    )


def _pruned_ids(publish: dict) -> set[int]:
    return {
        int(edge_id)
        for summary in publish.get("consistency_gates", [])
        for edge_id in summary.get("details", {}).get(
            "pruned_dangling_edge_ids",
            [],
        )
    }


@pytest.mark.dbos_integration
def test_repeated_git_history_reconcile_has_zero_interval_delta(
    clean_runtime: str,
    dbos_test_runtime,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reproduce the old prune/re-emit loop, exercise substitute ownership and
    restoration, then force a second completed-scope execution context and
    prove its semantic replay writes no graph or physical interval versions."""
    # dbos_test_runtime resets/recreates the shared test database as part of
    # its documented setup. Restore the OKG schema after that reset so this
    # proof exercises both the graph and the authoritative DBOS queue tables.
    from okg.substrate.db import migrate as migrate_module

    # The reset dropped the Chronos tables behind this process's back. Forget
    # what it cached about them, as tests/substrate/test_admission_quarantine.py
    # does after its out-of-band DROPs, so migrate bootstraps from scratch.
    from chronos_core.branching._common import clear_all_registry_caches
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache
    from okg.substrate.graph_store_internal.schema import _clear_mcp_grant_guard

    clear_all_registry_caches()
    clear_schema_ensured_cache()
    _clear_mcp_grant_guard()

    required_extensions = dict(migrate_module.REQUIRED_EXTENSIONS)
    migrate_module.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        migrate_module.ensure_required_extensions(clean_runtime)
        migrate_module.migrate(clean_runtime)
    finally:
        migrate_module.REQUIRED_EXTENSIONS.clear()
        migrate_module.REQUIRED_EXTENSIONS.update(required_extensions)

    monkeypatch.setenv("OKG_DISABLE_CHANGE_PROBE_GATE", "1")
    monkeypatch.setattr(
        Publisher,
        "_nomos_generation_publish_blocker",
        lambda *_args, **_kwargs: None,
    )

    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "dev")
    _git(repo, "config", "user.email", "fixture@example.invalid")
    _git(repo, "config", "user.name", "Fixture")
    (repo / "src").mkdir()
    (repo / "vendor").mkdir()
    (repo / "src/rich.py").write_text("RICH = True\n", encoding="utf-8")
    (repo / "vendor/narrow.py").write_text("NARROW = True\n", encoding="utf-8")
    _git(repo, "add", "src/rich.py", "vendor/narrow.py")
    _git(repo, "commit", "-m", "fixture")
    (repo / "vendor/narrow.py").write_text(
        "NARROW = True\nSECOND = True\n",
        encoding="utf-8",
    )
    _git(repo, "add", "vendor/narrow.py")
    _git(repo, "commit", "-m", "fixture parent edge")

    file_source = GitFilesSource(
        repo="demo",
        repo_root=repo,
        include_globs=["src/**"],
    )
    history_source = GitHistorySource(repo="demo", repo_root=repo)
    file_preview = list(file_source.run("preview-files", mode="scope_complete").facts)
    history_preview_run = history_source.run("preview-history", mode="scope_complete")
    history_preview = list(history_preview_run.facts)
    assert {
        "file:demo:src/rich.py",
        "file:demo:vendor/narrow.py",
    } <= {
        fact.node_id
        for fact in history_preview
        if isinstance(fact, NodeFact) and fact.subtype == "source_file"
    }
    _seed_catalog_for_facts(clean_runtime, [*file_preview, *history_preview])

    file_runner = SourceRunner(
        clean_runtime,
        source_name="demo.git-files",
        deployment_name="demo",
    )
    history_runner = SourceRunner(
        clean_runtime,
        source_name="demo.git-history",
        deployment_name="demo",
    )
    rich_id = "file:demo:src/rich.py"
    vendor_id = "file:demo:vendor/narrow.py"

    # Full ownership lifecycle: history substitute first, richer git-files
    # takeover, an unchanged history replay that cannot clobber it, source-
    # scoped authoritative retraction, and history restoration before publish.
    history_runner.run(history_source, mode="scope_complete")
    file_runner.run(file_source, mode="scope_complete")
    history_runner.run(history_source, mode="reconcile")
    owner_after_takeover: str
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        owner_after_takeover = store.query(
            "SELECT source FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": rich_id},
        )[0]["source"]
    _git(repo, "rm", "src/rich.py")
    _git(repo, "commit", "-m", "remove rich source")
    file_runner.run(file_source, mode="scope_complete")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        left_after_delete = store.query(
            "SELECT source FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": rich_id},
        )
    # git-files retracts its claim; git-history still declares the file as a
    # substitute, so the node passes to it instead of being deleted (#1901).
    assert left_after_delete == [{"source": "demo.git-history"}]
    assert owner_after_takeover == "demo.git-files"

    history_runner.run(history_source, mode="scope_complete")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        restored_owner = ChronosGraphStore.from_connection(conn).query(
            "SELECT source FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": rich_id},
        )[0]["source"]
    assert restored_owner == "demo.git-history"
    _git(repo, "checkout", "HEAD~1", "--", "src/rich.py")
    _git(repo, "commit", "-m", "restore rich source")
    file_runner.run(file_source, mode="reconcile")

    # Recreate the pre-fix publish state without altering the stable source:
    # remove only the historical vendor substitute while retaining its affects
    # edge. The narrow git-files scope cannot fill this endpoint, so prune
    # removes the dangling edge once.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).checkout().delete_keys(
            NODE_TABLE,
            [{"node_id": vendor_id}],
        )
    pruned_publish = Publisher(
        clean_runtime,
        "demo",
        consistency_gates=[TransformConsistencyGate(dangling_policy="prune")],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "demo"),
    ).cycle()
    assert pruned_publish["status"] == "published", json.dumps(
        pruned_publish,
        default=str,
        indent=2,
    )
    assert _pruned_count(pruned_publish) > 0
    pruned_ids = _pruned_ids(pruned_publish)
    assert pruned_ids

    recovery_context = SourceRunContext(
        run_id=str(uuid.uuid4()),
        observed_at=datetime.now(timezone.utc),
        workflow_id="source-sync:fs_event",
    )
    recovered = history_runner.run(
        history_source,
        mode="scope_complete",
        run_context=recovery_context,
    )
    recovered_publish = Publisher(
        clean_runtime,
        "demo",
        consistency_gates=[TransformConsistencyGate(dangling_policy="prune")],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "demo"),
    ).cycle()
    assert recovered_publish["status"] == "published", recovered_publish
    assert _pruned_count(recovered_publish) == 0
    assert recovered.nodes_appended > 0
    assert recovered.edges_appended > 0

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        checkout = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout_generation(recovered_publish["generation_id"])
        file_rows = checkout.query(
            """
            SELECT node_id, attrs, source
              FROM okg.graph_nodes
             WHERE node_id = ANY(:ids)
             ORDER BY node_id
            """,
            {"ids": [rich_id, vendor_id]},
        )
        missing_affects = checkout.query(
            """
            SELECT edge_id
              FROM okg.graph_edges e
             WHERE edge_type = 'affects'
               AND NOT EXISTS (
                     SELECT 1 FROM okg.graph_nodes n WHERE n.node_id = e.dst
                   )
            """
        )
        recovered_pruned = checkout.query(
            "SELECT edge_id FROM okg.graph_edges WHERE edge_id = ANY(:ids)",
            {"ids": sorted(pruned_ids)},
        )
    by_id = {row["node_id"]: row for row in file_rows}
    assert by_id[rich_id]["source"] == "demo.git-files"
    assert by_id[rich_id]["attrs"]["git_blob_sha"]
    assert by_id[vendor_id]["source"] == "demo.git-history"
    assert missing_affects == []
    assert {int(row["edge_id"]) for row in recovered_pruned} == pruned_ids

    before = _physical_source_interval_counts(clean_runtime, "demo.git-history")
    repeat_context = SourceRunContext(
        run_id=str(uuid.uuid4()),
        observed_at=datetime.now(timezone.utc),
        workflow_id="source-sync:poll",
    )
    repeated = history_runner.run(
        history_source,
        mode="reconcile",
        run_context=repeat_context,
    )
    repeated_publish = Publisher(
        clean_runtime,
        "demo",
        consistency_gates=[TransformConsistencyGate(dangling_policy="prune")],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "demo"),
    ).cycle()
    after = _physical_source_interval_counts(clean_runtime, "demo.git-history")

    assert repeated.graph_write_set_as_dict() is None
    assert after == before
    assert repeated_publish["status"] == "published", repeated_publish
    assert repeated_publish["noop"] is True
    assert _pruned_count(repeated_publish) == 0

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        mcp_session = open_session(conn, deployment_name="demo")
        mcp_payload = run_sql(
            conn,
            mcp_session,
            "SELECT count(*) AS n FROM okg.v_edges WHERE edge_type = 'affects'",
        )
        mcp_read_count = int(mcp_payload["rows"][0][0])
        queue = summarize_runtime_queue_health(cur)
    doctor = database_health_report(clean_runtime, deployment_name="demo")
    assert mcp_read_count > 0
    assert queue["ok"] is True, queue
    assert queue["active_queue_depth"] == 0
    assert queue["dlq_count"] == 0
    assert doctor["ok"] is True

    report = {
        "schema": "okg.graph_report.git_history_reemission.v1",
        "kind": "graph_report",
        "refs": ["req.git-history-idempotence.end-to-end"],
        "status": "pass",
        "deployment": "demo",
        "generation_id": repeated_publish["compat_generation_id"],
        "generation_ref": repeated_publish["generation_id"],
        "execution_contexts": [
            {
                "workflow_id": recovery_context.workflow_id,
                "mode": recovered.mode,
            },
            {
                "workflow_id": repeat_context.workflow_id,
                "mode": repeated.mode,
            },
        ],
        "run_ids": [recovered.run_id, repeated.run_id],
        "ref_digest": history_source.change_probe.build_token()["components"][
            "content"
        ]["value"],
        "pruned_before_recovery": _pruned_count(pruned_publish),
        "pruned_after_recovery": _pruned_count(recovered_publish),
        "recovery_nodes_appended": recovered.nodes_appended,
        "recovery_edges_appended": recovered.edges_appended,
        "second_candidate_nodes": repeated.nodes_appended,
        "second_candidate_edges": repeated.edges_appended,
        "second_graph_write_set": repeated.graph_write_set_as_dict(),
        "interval_counts_before": before,
        "interval_counts_after": after,
        "preserved_owner": by_id[rich_id]["source"],
        "publish_status": repeated_publish["status"],
        "publish_noop": repeated_publish["noop"],
        "queue": queue,
        "doctor_ok": doctor["ok"],
        "mcp_graph_read_edge_count": mcp_read_count,
    }
    evidence_path = os.environ.get("OKG_EVIDENCE_PATH")
    if evidence_path:
        path = Path(evidence_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(report, default=str, indent=2, sort_keys=True) + "\n",
        )


def _published_affects(dsn: str, generation_id: str) -> list[tuple]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout_generation(generation_id).query(
            """
            SELECT src, dst, attrs->>'change_type' AS change_type,
                   attrs->>'renamed_from_path' AS renamed_from_path
              FROM okg.graph_edges
             WHERE edge_type = 'affects'
             ORDER BY src, dst
            """
        )
    return [
        (row["src"], row["dst"], row["change_type"], row["renamed_from_path"])
        for row in rows
    ]


def test_rename_setting_change_rewrites_published_affects_edges(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """History published with rename detection off stores `git mv a.py
    b/a.py` as added + deleted. Turning the setting on (refs unchanged) makes
    the next cursor run rewrite those same two edges in place: the published
    generation then holds `b/a.py` as renamed from `a.py`, no duplicate and no
    extra edge. Turning it off again restores `added`."""
    monkeypatch.setattr(
        Publisher,
        "_nomos_generation_publish_blocker",
        lambda *_args, **_kwargs: None,
    )
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "dev")
    _git(repo, "config", "user.email", "fixture@example.invalid")
    _git(repo, "config", "user.name", "Fixture")
    (repo / "a.py").write_text(
        "".join(f"value_{i} = {i}\n" for i in range(30)),
        encoding="utf-8",
    )
    _git(repo, "add", "a.py")
    _git(repo, "commit", "-m", "add")
    (repo / "b").mkdir()
    _git(repo, "mv", "a.py", "b/a.py")
    _git(repo, "commit", "-m", "move")
    commit = f"commit:demo:{_git(repo, 'rev-parse', 'HEAD')}"

    off = GitHistorySource(repo="demo", repo_root=repo)
    on = GitHistorySource(repo="demo", repo_root=repo, detect_renames=True)
    _seed_catalog_for_facts(
        clean_runtime,
        [
            *off.run("preview-off", mode="scope_complete").facts,
            *on.run("preview-on", mode="scope_complete").facts,
        ],
    )
    runner = SourceRunner(
        clean_runtime,
        source_name="demo.git-history",
        deployment_name="demo",
    )

    def publish() -> dict:
        result = Publisher(
            clean_runtime,
            "demo",
            catalog_identity=bind_test_catalog_writer(clean_runtime, "demo"),
        ).cycle()
        assert result["status"] == "published", result
        return result

    def rename_edges(rows: list[tuple]) -> list[tuple]:
        return [row[1:] for row in rows if row[0] == commit]

    runner.run(off, mode="cursor")
    before = _published_affects(clean_runtime, publish()["generation_id"])
    assert rename_edges(before) == [
        ("file:demo:a.py", "deleted", None),
        ("file:demo:b/a.py", "added", None),
    ]

    rewalk = runner.run(on, mode="cursor")
    assert rewalk.edges_appended > 0
    renamed_generation = publish()
    after = _published_affects(clean_runtime, renamed_generation["generation_id"])
    assert rename_edges(after) == [
        ("file:demo:a.py", "deleted", None),
        ("file:demo:b/a.py", "renamed", "a.py"),
    ]
    assert len(after) == len(before)
    assert [row[:2] for row in after] == [row[:2] for row in before]

    # Same setting again: the runner writes nothing and publish is a no-op.
    repeat = runner.run(on, mode="cursor")
    assert repeat.edges_appended == 0
    assert publish()["noop"] is True

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        payload = run_sql(
            conn,
            open_session(conn, deployment_name="demo"),
            "SELECT count(*) AS n FROM okg.v_edges WHERE edge_type = 'affects'"
            " AND attrs->>'change_type' = 'renamed'",
        )
    assert int(payload["rows"][0][0]) == 1

    runner.run(off, mode="cursor")
    restored = _published_affects(clean_runtime, publish()["generation_id"])
    assert rename_edges(restored) == rename_edges(before)
    assert len(restored) == len(before)
