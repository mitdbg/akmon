from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.cli.deployment import (
    CI_READY_SOURCES,
    _deployment_ready_live_checks,
    _ready_progress_freshness_sources,
    _ready_queue_status,
    _ready_source_groups_for_profile,
    _ready_source_freshness_status,
    _ready_source_sync_liveness_status,
)
from okg.substrate.cli.main import main


def _write_deployment(tmp_path: Path, *, name: str, manifest: dict) -> Path:
    deployment_dir = tmp_path / name
    deployment_dir.mkdir()
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump(manifest),
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n",
        encoding="utf-8",
    )
    return deployment_dir


def _nomos_ci_policy() -> dict:
    return {
        "runtime_enforcement": "enforce",
        "deployment_posture": {
            "deployment_class": "ci_authority",
            "read_principals": ["ci_runner", "okg_service"],
            "write_principals": ["ci_runner", "okg_service"],
            "mcp_write_default": "deny",
            "export_policy": "internal_only",
            "secret_handling": "secret_ref_required",
            "live_call_default": "review",
            "allowed_source_sensitivities": ["internal"],
            "allowed_data_classifications": ["internal"],
            "allowed_exportability": ["internal"],
            "privacy_obligations": ["audit", "secret_ref_required"],
            "readiness_profiles": ["ci"],
        },
        "source_policy_defaults": {
            "by_source_class": {
                "discovery_crawl": {
                    "sensitivity": "internal",
                    "data_classification": "internal",
                    "credential_ref_policy": "none",
                    "live_call_allowed": False,
                    "exportability": "internal",
                    "retention": "normal",
                    "provenance": "local_filesystem",
                    "privacy_obligations": ["audit"],
                },
            }
        },
    }


@pytest.fixture
def patched_deployments(monkeypatch):
    deployments: dict[str, Path] = {}

    def _find(name: str) -> Path | None:
        return deployments.get(name)

    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        _find,
    )
    return deployments


def test_ci_ready_profile_uses_ci_defaults_and_process_env_authority(
    tmp_path: Path,
    patched_deployments,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(
        "OKG_CI_DSN",
        "postgres://process:readiness-secret@localhost/okg_ci",
    )
    profile = tmp_path / "local.env"
    profile.write_text("OKG_CI_DSN=postgres://profile/wrong\n")
    monkeypatch.setenv("OKG_LOCAL_ENV", str(profile))
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci",
        manifest={
            "name": "okg-ci",
            "profile": {"name": "okg-ci"},
            "schema_dir": "schemas",
            "source_registry": "source_registry.yaml",
            "postgres": {"dsn": "${OKG_CI_DSN}"},
            "nomos": _nomos_ci_policy(),
        },
    )
    patched_deployments["okg-ci"] = deployment_dir
    calls: dict[str, object] = {}

    def _fake_live_checks(**kwargs):
        calls.update(kwargs)
        return {
            "checks": [],
            "blockers": [],
            "warnings": [],
            "generation": {
                "generation_id": "okg:okg-ci:branch:default:gen:test",
                "status": "published",
                "count_integrity": {"ok": True, "status": "clean"},
            },
            "source_freshness": {"ok": True, "missing": [], "stale": []},
            "source_sync_status": {"ok": True, "status": "healthy"},
            "mcp": {"ok": True},
            "database": {"dsn": kwargs["dsn"]},
        }

    monkeypatch.setattr(
        "okg.substrate.cli.deployment._deployment_ready_live_checks",
        _fake_live_checks,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.deployment._ready_nomos_active_state_status",
        lambda **kwargs: {
            "schema": "okg.nomos.runtime_active_state.v1",
            "ok": True,
            "status": "pass",
            "deployment": kwargs["deployment"],
            "effective_mode": "enforce",
            "blocks_actions": True,
            "blocking_required": True,
            "readiness_profile": kwargs["readiness_profile"],
            "rollout_exception": None,
            "findings": [],
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_apply.deployment_apply_health",
        lambda **kwargs: {
            "ok": True,
            "status": "ready",
            "managed": True,
            "deployment": kwargs["deployment"],
        },
    )

    rc = main([
        "deployment",
        "ready",
        "okg-ci",
        "--checks",
        "ci",
        "--json",
    ])

    assert rc == 0
    raw_output = capsys.readouterr().out
    payload = json.loads(raw_output)
    assert calls["dsn"] == (
        "postgres://process:readiness-secret@localhost/okg_ci"
    )
    assert "readiness-secret" not in raw_output
    assert payload["database"]["dsn"] == (
        "postgres://process:***@localhost/okg_ci"
    )
    assert calls["readiness_profile"] == "ci"
    assert calls["max_source_age_seconds"] == 6 * 60 * 60
    assert calls["max_producer_age_seconds"] == 15 * 60
    assert payload["profile"] == {
        "name": "ci",
        "max_source_age_seconds": 6 * 60 * 60,
        "max_producer_age_seconds": 15 * 60,
    }
    assert payload["environment"]["environment"][0]["source"] == "process_env"
    assert payload["environment"]["authority"]["mode"] == (
        "process_env_precedence"
    )
    assert (
        payload["environment"]["authority"]["local_profile_authoritative"]
        is False
    )
    assert payload["rollout"]["concept"] == "OKG-CI"
    assert payload["rollout"]["project_scope"]["kind"] == "ci_authority_graph"
    assert payload["rollout"]["project_scope"]["mcp_target"][
        "readiness_command"
    ] == "uv run okg deployment ready okg-ci --checks ci --json"


def test_ci_source_groups_honor_fail_closed_manifest_override(
    tmp_path: Path,
) -> None:
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci-dev",
        manifest={
            "name": "okg-ci-dev",
            "release": {"channel": "dev"},
            "nomos": {
                "source_policy_defaults": {
                    "by_source_class": {
                        "mutable_api": {"live_call_allowed": False},
                    },
                },
            },
            "readiness": {
                "override_authority": "staging_fail_closed",
                "source_groups": {
                    "ci": {
                        "repo": ["git-history", "pact"],
                        "test_artifacts": ["test-reports"],
                        "dependencies": ["package-manifests"],
                    },
                },
            },
        },
    )
    (deployment_dir / "source_registry.yaml").write_text(
        "sources:\n  github-issues:\n    source_class: mutable_api\n",
        encoding="utf-8",
    )

    assert _ready_source_groups_for_profile(
        "ci",
        deployment="okg-ci-dev",
        deployment_dir=deployment_dir,
    ) == {
        "repo": ("git-history", "pact"),
        "test_artifacts": ("test-reports",),
        "dependencies": ("package-manifests",),
    }


@pytest.mark.postgres
def test_ci_freshness_resolves_thin_overlay_ownership_id(
    clean_runtime: str,
) -> None:
    now = datetime.now(timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_watermarks
                  (source, last_applied_rev, last_reconcile_at)
                VALUES (%s, %s::jsonb, %s)
                """,
                ("okg-ci.pact", json.dumps({"run_id": "staging"}), now),
            )

        freshness = _ready_source_freshness_status(
            conn,
            deployment="okg-ci-dev",
            max_source_age_seconds=6 * 60 * 60,
            source_groups={"repo": ("pact",)},
            source_aliases={"pact": ("okg-ci.pact",)},
        )

    assert freshness["ok"] is True
    assert freshness["groups"]["repo"]["sources"][0]["source"] == "okg-ci.pact"


def test_ci_ready_sources_exclude_local_agent_context() -> None:
    assert "agent-sessions" not in CI_READY_SOURCES
    assert "codex-sessions" not in CI_READY_SOURCES
    assert "coordination-native" not in CI_READY_SOURCES
    assert "compute-env-local" not in CI_READY_SOURCES

    groups = _ready_source_groups_for_profile("ci")
    assert groups == {
        "repo": ("git-history", "github-issues", "pact"),
        "test_artifacts": ("test-reports",),
        "dependencies": ("package-manifests",),
    }


def test_ready_queue_status_uses_runtime_queue_health_not_retired_view(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "okg.substrate.runtime_dlq.current_runtime_dlq",
        lambda _cur, **_kwargs: {
            "current_count": 0,
            "historical_count": 2,
            "current_application_version": "v2",
            "current_failures": [],
        },
    )
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci",
        manifest={
            "name": "okg-ci",
            "embeddings": {"enabled": True, "backend": "hash"},
        },
    )

    class _Cursor:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, query: str) -> None:
            self.queries.append(str(query))

        def fetchone(self):
            return (True,)

        def fetchall(self):
            return [
                (
                    "okg-okg-ci-source-sync",
                    "failed",
                    "available",
                    3,
                    1,
                    2,
                    0,
                    1,
                    2,
                    0,
                    2,
                    4,
                    0,
                    120.0,
                    5.0,
                    600,
                    False,
                    None,
                    None,
                    "okg_source_sync",
                ),
                (
                    "_dbos_internal_queue",
                    "running",
                    "available",
                    1,
                    1,
                    0,
                    0,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    30.0,
                    30.0,
                    600,
                    False,
                    None,
                    None,
                    "non_source_sync_workflow",
                ),
                (
                    "_dbos_internal_queue",
                    "running",
                    "available",
                    1,
                    1,
                    0,
                    0,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    30.0,
                    30.0,
                    600,
                    False,
                    None,
                    None,
                    "okg_source_poll_interval_300s",
                ),
            ]

    class _Conn:
        def __init__(self) -> None:
            self.cursor_obj = _Cursor()

        def cursor(self):
            return self.cursor_obj

    conn = _Conn()
    monkeypatch.setattr(
        "okg.substrate.runtime_queue_health.current_runtime_dlq",
        lambda _cur: {
            "current_count": 0,
            "historical_count": 2,
            "current_failures": [],
        },
    )

    status = _ready_queue_status(
        conn,
        deployment_dir=deployment_dir,
        max_active_queue_depth=0,
    )

    assert status["active_queue_depth"] == 1
    assert status["queue_depth"]["ingest"] == 1
    assert status["dlq_count"] == 0
    assert "doctor_workflow" in status["ignored_runtime_workflow_names"]
    assert "okg_source_poll_interval_300s" in status[
        "ignored_runtime_workflow_names"
    ]
    assert "okg_source_sync" in status["ignored_runtime_workflow_names"]
    assert status["runtime_queue_health"][0]["queue_name"] == (
        "okg-okg-ci-source-sync"
    )
    assert status["source"] == "okg.v_runtime_queue_health"
    joined = "\n".join(conn.cursor_obj.queries)
    assert "okg.v_runtime_queue_health" in joined
    assert "okg.v_job_queue" not in joined
    assert "okg.jobs" not in joined


@pytest.mark.postgres
def test_ready_queue_status_accepts_absent_legacy_jobs_table(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci",
        manifest={
            "name": "okg-ci",
            "embeddings": {"enabled": True, "backend": "hash"},
        },
    )

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.jobs')")
            assert cur.fetchone()[0] is None
            cur.execute("SELECT to_regclass('dbos.workflow_status')")
            if cur.fetchone()[0] is not None:
                cur.execute("DELETE FROM dbos.workflow_status")

        status = _ready_queue_status(
            conn,
            deployment_dir=deployment_dir,
            max_active_queue_depth=0,
        )

    assert status["active_queue_depth"] == 0
    assert status["dlq_count"] == 0
    assert all(count == 0 for count in status["queue_depth"].values())


def test_ci_ready_runs_doctor_and_binds_source_convergence_to_mcp(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from okg.substrate.cli import deployment as deployment_mod
    from okg.substrate import source_sync_status

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *_args, **_kwargs: _FakeConnection(),
    )
    monkeypatch.setattr(
        deployment_mod,
        "_ready_database_status",
        lambda _conn: {"missing_relations": []},
    )
    monkeypatch.setattr(
        deployment_mod,
        "_ready_generation_status",
        lambda _conn, _deployment: {
            "status": "published",
            "generation_id": "g1",
            "count_integrity": {"ok": True, "status": "clean"},
        },
    )
    monkeypatch.setattr(
        deployment_mod,
        "_ready_nomos_source_lineage_status",
        lambda *_args, **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        deployment_mod,
        "_ready_nomos_read_probe_status",
        lambda *_args, **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        deployment_mod,
        "_ready_queue_status",
        lambda _conn, **_kwargs: {
            "queue_depth": {},
            "ignored_job_kinds": [],
            "active_queue_depth": 0,
            "max_active_queue_depth": 0,
            "dlq_count": 0,
        },
    )
    monkeypatch.setattr(
        deployment_mod,
        "_ready_progress_freshness_sources",
        lambda *_args, **_kwargs: set(),
    )
    monkeypatch.setattr(
        deployment_mod,
        "_ready_source_freshness_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "max_source_age_seconds": 21600,
            "groups": {},
            "missing": [],
            "stale": [],
        },
    )
    monkeypatch.setattr(
        deployment_mod,
        "_ready_source_sync_liveness_status",
        lambda **_kwargs: {"deployment": "okg-ci"},
    )
    monkeypatch.setattr(
        deployment_mod,
        "_ready_mcp_status",
        lambda _conn, _deployment: {
            "ok": True,
            "pinned_generation_id": "g1",
        },
    )
    doctor_calls: list[dict[str, object]] = []

    def _doctor(deployment: str, *, dsn: str, skip: bool) -> dict[str, object]:
        doctor_calls.append({"deployment": deployment, "dsn": dsn, "skip": skip})
        return {"ok": True, "status": "healthy"}

    monkeypatch.setattr(deployment_mod, "_ready_doctor_status", _doctor)

    calls: list[dict] = []

    def _fake_status_contract(
        health,
        *,
        source_freshness=None,
        generation=None,
        mcp=None,
    ):
        calls.append({
            "health": health,
            "source_freshness": source_freshness,
            "generation": generation,
            "mcp": mcp,
        })
        return {
            "ok": True,
            "status": "healthy",
            "convergence": {
                "ok": bool(mcp and mcp.get("pinned_generation_id")),
                "state": "ready",
            },
        }

    monkeypatch.setattr(
        source_sync_status,
        "build_source_sync_status_contract",
        _fake_status_contract,
    )

    payload = _deployment_ready_live_checks(
        deployment="okg-ci",
        deployment_dir=tmp_path,
        dsn="postgres://example/okg_ci",
        max_active_queue_depth=0,
        max_source_age_seconds=21600,
        readiness_profile="ci",
        skip_doctor=False,
    )

    assert doctor_calls == [{
        "deployment": "okg-ci",
        "dsn": "postgres://example/okg_ci",
        "skip": False,
    }]
    assert payload["doctor"] == {"ok": True, "status": "healthy"}
    assert next(
        check for check in payload["checks"]
        if check["id"] == "doctor.okg_workspace"
    )["status"] == "pass"
    assert calls[-1]["mcp"] == {"ok": True, "pinned_generation_id": "g1"}
    assert payload["source_sync_status"]["convergence"]["ok"] is True
    sync_check = next(
        check for check in payload["checks"]
        if check["id"] == "sources.sync_liveness"
    )
    assert sync_check["details"]["convergence"]["state"] == "ready"


def test_ready_source_sync_liveness_blocks_on_runtime_config_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "source_registry.yaml").write_text("sources: {}\n")
    # Runtime source-sync config now resolves from the deployment_dir
    # manifest itself; a malformed runtime block is the failure mode.
    (tmp_path / "deployment.yaml").write_text(
        "name: broken\nruntime: not-a-mapping\n"
    )

    def _fake_producer_health(*_args, **kwargs):
        return {
            "deployment": kwargs["deployment"],
            "ok": True,
            "producer_count": 0,
            "expected_producer_count": 0,
            "disabled_producer_count": 0,
            "producers": [],
            "expected_producers": [],
            "disabled_producers": [],
            "missing_producers": [],
            "stale_producers": [],
            "config_warnings": [],
            "queue_depth": [],
            "dbos_queue_depth": {},
            "dlq_count": 0,
        }

    monkeypatch.setattr(
        "okg.substrate.source_sync_producers.source_sync_producer_health",
        _fake_producer_health,
    )

    out = _ready_source_sync_liveness_status(
        dsn="postgres://unused",
        deployment="broken",
        deployment_dir=tmp_path,
        max_producer_age_seconds=900,
    )

    assert out["ok"] is False
    assert {
        warning["code"] for warning in out["config_warnings"]
    } == {"runtime_source_sync_config_unresolved"}
    assert {
        blocker["code"] for blocker in out["source_sync_status"]["blockers"]
    } == {"source_sync_config_warning"}


@pytest.mark.postgres
def test_ci_append_only_sources_use_progress_freshness(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    deployment_dir = _write_deployment(
        tmp_path,
        name="okg-ci",
        manifest={
            "name": "okg-ci",
            "profile": {"name": "okg-ci"},
            "postgres": {"dsn": "${OKG_CI_DSN}"},
            "source_registry": "source_registry.yaml",
        },
    )
    (deployment_dir / "source_registry.yaml").write_text(
        """
sources:
  test-reports:
    module: okg.substrate.library.sources.test_reports
    class: TestReportSource
    ownership_id: okg-ci.test-reports
    params:
      append_only: true
    source_class: discovery_crawl
    record_identity_kind: scoped_locator
    record_identity_fields: [repo, path]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    sync:
      triggers: [poll, manual, reconcile]
      default_event_mode: scope_incomplete
      reconcile_mode: scope_incomplete
""".lstrip(),
        encoding="utf-8",
    )
    now = datetime.now(timezone.utc)
    stale = now - timedelta(hours=24)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_watermarks
                  (source, last_applied_rev, last_reconcile_at)
                VALUES (%s, %s::jsonb, %s)
                """,
                (
                    "okg-ci.test-reports",
                    json.dumps({"run_id": "old"}),
                    stale,
                ),
            )
            cur.execute(
                """
                INSERT INTO okg.source_progress
                  (deployment, source_name, key, value, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    "okg-ci",
                    "okg-ci.test-reports",
                    "sync.last_run",
                    json.dumps({
                        "schema": "okg.source_runner.last_run.v1",
                        "finished_at": now.isoformat(),
                    }),
                    now,
                ),
            )

        freshness = _ready_source_freshness_status(
            conn,
            deployment="okg-ci",
            max_source_age_seconds=6 * 60 * 60,
            source_groups={"test_artifacts": ("test-reports",)},
            progress_freshness_sources=_ready_progress_freshness_sources(
                deployment_dir,
                "ci",
            ),
        )

    assert freshness["ok"] is True
    row = freshness["groups"]["test_artifacts"]["sources"][0]
    assert row["freshness_kind"] == "source_progress"
    assert row["progress_key"] == "sync.last_run"
    assert row["watermark_age_seconds"] > 6 * 60 * 60


@pytest.mark.postgres
def test_source_freshness_accepts_only_explicit_definition_namespace(
    clean_runtime: str,
) -> None:
    now = datetime.now(timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO okg.source_watermarks
                  (source, last_applied_rev, last_reconcile_at)
                VALUES (%s, %s::jsonb, %s)
                """,
                [
                    ("shared-definition.pact", json.dumps({"rev": "good"}), now),
                    (
                        "runtime-staging.pact",
                        json.dumps({"rev": "stale-runtime"}),
                        now - timedelta(days=1),
                    ),
                    ("pact", json.dumps({"rev": "bare"}), now),
                    ("foreign.pact", json.dumps({"rev": "foreign"}), now),
                    ("openspec", json.dumps({"rev": "bare"}), now),
                    ("foreign.openspec", json.dumps({"rev": "foreign"}), now),
                ],
            )

        freshness = _ready_source_freshness_status(
            conn,
            deployment="runtime-staging",
            source_namespace="shared-definition",
            max_source_age_seconds=6 * 60 * 60,
            source_groups={"history": ("pact", "openspec")},
        )

    pact_row, openspec_row = freshness["groups"]["history"]["sources"]
    assert pact_row == {
        "source": "runtime-staging.pact",
        "observed_source": "shared-definition.pact",
        "status": "fresh",
        "last_reconcile_at": now,
        "age_seconds": pytest.approx(0, abs=5),
    }
    assert openspec_row["source"] == "runtime-staging.openspec"
    assert openspec_row["status"] == "missing"
    assert freshness["missing"] == ["runtime-staging.openspec"]
    assert freshness["source_namespace"] == "shared-definition"
