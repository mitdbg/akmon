"""CLI coverage for source-sync policy diagnostics."""
from __future__ import annotations

import json
import hashlib
import hmac
import threading
import urllib.request
from pathlib import Path

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate import source_sync_producers
from okg.substrate.source_sync import SourceSyncJob
from okg.substrate.source_sync_producers import make_webhook_http_server
from okg.substrate.sources.registry import load_source_registry
from tests.mirror_scope import private_path


def _capture_source_sync_dbos_enqueues(monkeypatch) -> list[dict]:
    calls: list[dict] = []

    def fake_enqueue_compacted_source_sync_via_dbos(**kwargs):
        calls.append(dict(kwargs))
        return f"wf-source-sync-{len(calls)}"

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "enqueue_compacted_source_sync_via_dbos",
        fake_enqueue_compacted_source_sync_via_dbos,
    )
    return calls


def test_sync_explain_emits_static_policy(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_sync_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))

    rc = main([
        "--json",
        "sync",
        "explain",
        "--deployment",
        "sync-demo",
    ])

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    docs = out["sources"]["docs"]
    assert docs["triggers"] == ["fs_event", "manual", "poll", "reconcile", "webhook"]
    assert docs["scope_support"] == ["path"]
    assert docs["dependent_fanout"]["on_event"] == "direct"
    assert docs["compaction"]["window_seconds"] == 30
    assert docs["webhook"]["path"] == "/hooks/docs"
    assert docs["poll"]["probe"] == "change_probe"


def test_sync_explain_path_event_shows_matching_policy(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    dep = _write_sync_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    changed = dep / "docs" / "guide.md"
    changed.parent.mkdir()
    changed.write_text("hello")

    rc = main([
        "--json",
        "sync",
        "explain",
        "--deployment",
        "sync-demo",
        "--path",
        str(changed),
    ])

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["match_count"] == 1
    match = out["matches"][0]
    assert match["source"] == "docs"
    assert match["matched_scope_key"] == "docs/guide.md"
    assert match["run_mode"] == "scope_incomplete"
    assert match["dependent_fanout"] == "direct"
    assert match["scoped_execution"] == "path"


@pytest.mark.parametrize(
    ("source", "path", "trigger_kind", "event_fanout"),
    [
        (
            "agent-sessions",
            Path.home()
            / ".claude/projects/-Users-example-projects-mit-okg/session.jsonl",
            "poll",
            "none",
        ),
        (
            "security-analysis",
            Path(__file__).resolve().parents[3]
            / "reports/security/semgrep.json",
            "fs_event",
            "direct",
        ),
        (
            "agent-memory",
            Path(__file__).resolve().parents[3]
            / ".artifacts/agent_memory/demo/memory.jsonl",
            "fs_event",
            "direct",
        ),
        # Frozen, not deleted: compute-env-remote is quarantined out of
        # okg-workspace's source registry (2026-08-24). Drop the mark when
        # the entry is uncommented in
        # deployments/okg-workspace/source_registry.yaml.
        pytest.param(
            "compute-env-remote",
            Path.home() / ".claude/projects/demo/session.jsonl",
            "poll",
            "none",
            marks=pytest.mark.skip(
                reason="frozen: compute-env-remote is quarantined (admission "
                "rejects every run: unclassified_empty)"
            ),
        ),
    ],
)
def test_okg_workspace_sync_explain_shows_scoped_and_delete_fallback(
    source: str,
    path: Path,
    trigger_kind: str,
    event_fanout: str,
    capsys,
) -> None:
    # okg-workspace is this repository's own deployment; the public copy
    # skips this test and runs the synthetic one below (tests/mirror_scope.py).
    private_path("deployments/okg-workspace/source_registry.yaml")
    rc = main([
        "--json",
        "sync",
        "explain",
        "--deployment",
        "okg-workspace",
        "--source",
        source,
        "--path",
        str(path),
        "--trigger",
        trigger_kind,
    ])

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["match_count"] == 1
    match = out["matches"][0]
    assert match["source"] == source
    assert match["trigger_kind"] == trigger_kind
    assert match["run_mode"] == "scope_incomplete"
    assert match["dependent_fanout"] == event_fanout
    assert match["scoped_execution"] == "path"
    assert match["fallback_run_mode"] == "scope_complete"
    assert match["deletes_require_reconcile"] is True
    assert match["delete_fallback_run_mode"] == "scope_complete"
    assert match["delete_fallback_dependent_fanout"] == "closure"

    rc = main([
        "--json",
        "sync",
        "explain",
        "--deployment",
        "okg-workspace",
        "--source",
        source,
        "--path",
        str(path),
        "--trigger",
        "reconcile",
    ])

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["match_count"] == 1
    reconcile = out["matches"][0]
    assert reconcile["run_mode"] == "scope_complete"
    assert reconcile["dependent_fanout"] == "closure"
    assert reconcile["scoped_execution"] == "path"


def test_sync_explain_shows_scoped_run_and_delete_fallback(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """The scoped-event and delete-fallback policy on the synthetic sync-demo.

    The same explain output the okg-workspace test above checks, on a
    deployment the public copy ships with the tests.
    """
    dep = _write_sync_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    changed = dep / "docs" / "guide.md"
    changed.parent.mkdir()
    changed.write_text("hello")

    def explain(trigger: str) -> dict:
        rc = main([
            "--json", "sync", "explain", "--deployment", "sync-demo",
            "--source", "docs", "--path", str(changed), "--trigger", trigger,
        ])
        assert rc == 0
        out = json.loads(capsys.readouterr().out)
        assert out["match_count"] == 1
        return out["matches"][0]

    match = explain("fs_event")
    assert match["source"] == "docs"
    assert match["trigger_kind"] == "fs_event"
    assert match["run_mode"] == "scope_incomplete"
    assert match["dependent_fanout"] == "direct"
    assert match["scoped_execution"] == "path"
    assert match["fallback_run_mode"] == "scope_complete"
    assert match["deletes_require_reconcile"] is True
    assert match["delete_fallback_run_mode"] == "scope_complete"
    assert match["delete_fallback_dependent_fanout"] == "closure"

    reconcile = explain("reconcile")
    assert reconcile["run_mode"] == "scope_complete"
    assert reconcile["dependent_fanout"] == "closure"
    assert reconcile["scoped_execution"] == "path"


@pytest.mark.postgres
def test_sync_webhook_enqueue_validates_and_writes_job(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch,
    capsys,
) -> None:
    calls = _capture_source_sync_dbos_enqueues(monkeypatch)
    _write_sync_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setenv("DOCS_WEBHOOK_SECRET", "secret")
    payload = '{"scope_keys":["docs/a.md","docs/b.md"],"event_id":"evt-1"}'
    signature = "sha256=" + hmac.new(
        b"secret",
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    rc = main([
        "--json",
        "sync",
        "webhook",
        "--deployment",
        "sync-demo",
        "--dsn",
        clean_runtime,
        "--source",
        "docs",
        "--payload-json",
        payload,
        "--signature",
        signature,
        "--no-publish",
    ])

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["producer"] == "webhook"
    assert out["result"]["event_count"] == 2
    assert out["result"]["workflow_id"] == "wf-source-sync-1"
    assert len(calls) == 1
    job = calls[0]["job"]
    assert isinstance(job, SourceSyncJob)
    queued = job.to_dict()
    assert queued["sources"] == ["docs"]
    assert queued["dependent_fanout"] == "direct"
    assert [event["scope_key"] for event in queued["events"]] == [
        "docs/a.md",
        "docs/b.md",
    ]


@pytest.mark.postgres
def test_sync_webhook_removed_path_uses_reconcile_policy(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch,
    capsys,
) -> None:
    calls = _capture_source_sync_dbos_enqueues(monkeypatch)
    _write_sync_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setenv("DOCS_WEBHOOK_SECRET", "secret")
    payload = '{"commits":[{"removed":["docs/deleted.md"]}]}'
    signature = "sha256=" + hmac.new(
        b"secret",
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    rc = main([
        "--json",
        "sync",
        "webhook",
        "--deployment",
        "sync-demo",
        "--dsn",
        clean_runtime,
        "--source",
        "docs",
        "--payload-json",
        payload,
        "--signature",
        signature,
        "--no-publish",
    ])

    assert rc == 0
    _ = capsys.readouterr()
    assert len(calls) == 1
    queued = calls[0]["job"].to_dict()
    assert queued["run_mode"] == "scope_complete"
    assert queued["dependent_fanout"] == "closure"
    assert queued["events"][0]["trigger_kind"] == "reconcile"
    assert queued["events"][0]["reason"] == "webhook:removed"


@pytest.mark.postgres
def test_sync_webhook_http_server_receives_signed_payload(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch,
    capsys,
) -> None:
    calls = _capture_source_sync_dbos_enqueues(monkeypatch)
    dep = _write_sync_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setenv("DOCS_WEBHOOK_SECRET", "secret")
    registry = load_source_registry(dep / "source_registry.yaml")
    server, stats = make_webhook_http_server(
        clean_runtime,
        deployment="sync-demo",
        registry=registry,
        host="127.0.0.1",
        port=0,
        publish=True,
    )
    payload = b'{"scope_key":"docs/http.md","event_id":"evt-http"}'
    signature = "sha256=" + hmac.new(
        b"secret",
        payload,
        hashlib.sha256,
    ).hexdigest()
    thread = threading.Thread(target=server.handle_request)
    thread.start()
    try:
        port = server.server_address[1]
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/hooks/docs",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "X-OKG-Signature": signature,
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            assert response.status == 202
    finally:
        thread.join(timeout=5)
        server.server_close()

    assert stats.request_count == 1
    assert stats.enqueue_count == 1
    assert len(calls) == 1
    queued = calls[0]["job"].to_dict()
    assert queued["events"][0]["scope_key"] == "docs/http.md"


@pytest.mark.postgres
def test_sync_poll_enqueues_once_and_health_reports_queue(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch,
    capsys,
) -> None:
    calls = _capture_source_sync_dbos_enqueues(monkeypatch)
    dep = _write_sync_deployment(tmp_path)
    docs = dep / "docs"
    docs.mkdir()
    (docs / "guide.md").write_text("hello")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  queue_name text NOT NULL,
                  name text NOT NULL,
                  deduplication_id text,
                  status text NOT NULL,
                  inputs text,
                  serialization text,
                  priority integer,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )

    rc = main([
        "--json",
        "sync",
        "poll",
        "--deployment",
        "sync-demo",
        "--dsn",
        clean_runtime,
        "--source",
        "docs",
    ])

    assert rc == 0
    _ = capsys.readouterr()
    assert len(calls) == 1
    assert calls[0]["deduplication_id"].startswith(
        "sync:sync-demo:poll:"
    )

    rc = main([
        "--json",
        "sync",
        "health",
        "--deployment",
        "sync-demo",
        "--dsn",
        clean_runtime,
    ])

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["dlq_count"] == 0
    assert out["producers"][0]["producer"] == "poll"
    assert out["queue_depth"] == []
    assert "legacy_jobs" not in out["source_sync_status"]["wakeups"]


@pytest.mark.postgres
def test_sync_health_blocks_on_runtime_config_warning(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch,
    capsys,
) -> None:
    dep = _write_sync_deployment(tmp_path)
    (dep / "deployment.yaml").write_text(
        "name: sync-demo\n"
        "description: source sync diagnostic fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "runtime: invalid\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))

    rc = main([
        "--json",
        "sync",
        "health",
        "--deployment",
        "sync-demo",
        "--dsn",
        clean_runtime,
    ])

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert {
        warning["code"] for warning in out["config_warnings"]
    } >= {"runtime_source_sync_config_unresolved"}
    assert {
        blocker["code"]
        for blocker in out["source_sync_status"]["blockers"]
    } >= {"source_sync_config_warning"}
    assert out["source_sync_status"]["ok"] is False


@pytest.mark.postgres
def test_sync_poll_does_not_ack_fingerprint_when_enqueue_fails(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch,
) -> None:
    dep = _write_sync_deployment(tmp_path)
    docs = dep / "docs"
    docs.mkdir()
    (docs / "guide.md").write_text("hello")
    registry = load_source_registry(dep / "source_registry.yaml")

    def fail_enqueue(*_args, **_kwargs):
        raise RuntimeError("enqueue unavailable")

    monkeypatch.setattr(
        source_sync_producers,
        "enqueue_events_for_source",
        fail_enqueue,
    )

    out = source_sync_producers.run_poll_once(
        clean_runtime,
        deployment="sync-demo",
        registry=registry,
        source="docs",
    )

    assert out["errors"][0]["source_name"] == "docs"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.source_progress "
            "WHERE key LIKE 'sync.poll.%.fingerprint'"
        )
        assert cur.fetchone()[0] == 0


def test_sync_poll_cli_fails_loudly_on_enqueue_errors(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_sync_deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))

    def fake_run_poll_once(*_args, **_kwargs):
        return {
            "deployment": "sync-demo",
            "producer": "poll",
            "results": [],
            "errors": [{
                "source_name": "docs",
                "error": "RuntimeError: enqueue unavailable",
            }],
        }

    monkeypatch.setattr(
        source_sync_producers,
        "run_poll_once",
        fake_run_poll_once,
    )

    rc = main([
        "--json",
        "sync",
        "poll",
        "--deployment",
        "sync-demo",
        "--dsn",
        "postgres://unused",
        "--source",
        "docs",
    ])

    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    error = json.loads(captured.err)["error"]
    assert error["code"] == "source_sync_poll_enqueue_failed"
    assert "durable DBOS enqueue" in error["message"]
    assert "runtime worker is healthy" in error["fix_hint"]
    assert error["data"]["errors"] == [{
        "source_name": "docs",
        "error": "RuntimeError: enqueue unavailable",
    }]


def test_sync_poll_uses_runtime_backstop_multiplier(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    dep = _write_sync_deployment(tmp_path)
    (dep / "deployment.yaml").write_text(
        "name: sync-demo\n"
        "description: source sync diagnostic fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "runtime:\n"
        "  enabled: false\n"
        "  source_sync:\n"
        "    poll:\n"
        "      backstop_multiplier: 17\n"
        "      backstop_dependent_fanout: none\n"
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    captured: dict[str, object] = {}

    def fake_run_poll_once(*args, **kwargs):
        captured["args"] = args
        captured.update(kwargs)
        return {"deployment": "sync-demo", "producer": "poll", "results": []}

    monkeypatch.setattr(
        source_sync_producers,
        "run_poll_once",
        fake_run_poll_once,
    )

    rc = main([
        "--json",
        "sync",
        "poll",
        "--deployment",
        "sync-demo",
        "--dsn",
        "postgres://unused",
        "--source",
        "docs",
    ])

    assert rc == 0
    _ = json.loads(capsys.readouterr().out)
    assert captured["source"] == "docs"
    assert "route_via_dbos" not in captured
    assert captured["backstop_multiplier"] == 17
    assert captured["backstop_dependent_fanout"] == "none"


def _write_sync_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "deployments" / "sync-demo"
    dep.mkdir(parents=True)
    (dep / "schemas").mkdir()
    fixture = dep / "docs-fixture.json"
    fixture.write_text(json.dumps({
        "nodes": [
            {
                "node_id": f"doc:sync-demo:{path}",
                "subtype": "document",
                "attrs": {"path": path, "text": path},
                "source_record_id": {"path": path},
                "source_revision": {"v": 1},
            }
            for path in (
                "docs/a.md",
                "docs/b.md",
                "docs/guide.md",
                "docs/http.md",
            )
        ],
        "edges": [],
    }))
    (dep / "deployment.yaml").write_text(
        "name: sync-demo\n"
        "description: source sync diagnostic fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
    )
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  docs:\n"
        "    module: okg.substrate.library.sources.bootstrap_fixture\n"
        "    class: BootstrapFixtureSource\n"
        "    ownership_id: sync-demo.docs\n"
        "    params:\n"
        f"      fixture_path: {fixture}\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n"
        "    sync:\n"
        "      watch:\n"
        f"        roots: [{dep}]\n"
        "        include_globs: ['docs/**/*.md']\n"
        "        exclude_globs: ['docs/private/**']\n"
        "      triggers: [fs_event, manual, poll, reconcile, webhook]\n"
        "      default_event_mode: scope_incomplete\n"
        "      reconcile_mode: scope_complete\n"
        "      deletes_require_reconcile: true\n"
        "      scope:\n"
        "        accepts: [path]\n"
        "        unsupported_fallback: scope_complete\n"
        "      dependents:\n"
        "        on_event: direct\n"
        "        on_reconcile: closure\n"
        "      compaction:\n"
        "        window_seconds: 30\n"
        "      webhook:\n"
        "        path: /hooks/docs\n"
        "        secret_ref: DOCS_WEBHOOK_SECRET\n"
        "      poll:\n"
        "        probe: change_probe\n"
        "        interval_seconds: 60\n"
    )
    return dep
