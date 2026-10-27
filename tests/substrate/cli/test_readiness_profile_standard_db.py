"""The `standard` readiness profile end to end, against a real database (#3958).

PACT `deployment-readiness-profile-declared-3958` (T4, T5, the registry case of
T6, the live half of T7, the capacity command of T8). Each case runs the real
live checks against PostgreSQL, builds the real `okg dogfood status` payload
from them, and reads the chat banner from that payload; case (e) also runs
the console's own readiness read.

Only the probes this deployment cannot have are stubbed: the published
generation and the MCP read (no graph was published into the test database)
and the code-drift probe (no checkout). Source watermarks are real rows.

Every test here is postgres-marked and new with this change, so the file has
its own owner row in `tests/qa_lanes.yaml`.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml

from okg.substrate.cli import deployment as deployment_cli

NAME = "standard-demo"
GENERATION = f"okg:{NAME}:branch:default:gen:test"
DAY = 24 * 3600
RUNTIME_OFF = {"enabled": False}


def _write_deployment(
    tmp_path: Path,
    *,
    sources: dict[str, bool],
    runtime: Any = RUNTIME_OFF,
    disabled: tuple[str, ...] = (),
) -> Path:
    """A plain install that declares `standard`, with the given sources.

    ``sources`` maps a registry key to whether it is `required_for_baseline`.
    Keys are spelled with underscores and ownership ids with dashes, so the
    freshness lookup has to go through the ownership id (T5).
    """
    directory = tmp_path / NAME
    directory.mkdir()
    manifest: dict[str, Any] = {
        "name": NAME,
        "postgres": {"dsn": "${OKG_DSN}"},
        "source_registry": "source_registry.yaml",
        "readiness": {"profile": "standard"},
    }
    if runtime is not None:
        manifest["runtime"] = runtime
    (directory / "deployment.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")
    (directory / "source_registry.yaml").write_text(
        yaml.safe_dump({
            "sources": {
                key: {
                    "module": "static_demo.source",
                    "class": "StaticDemoSource",
                    "ownership_id": f"{NAME}.{key.replace('_', '-')}",
                    "source_class": "discovery_crawl",
                    "record_identity_kind": "scoped_locator",
                    "record_identity_fields": ["path"],
                    "source_revision_kind": "content_hash",
                    "deletion_semantics": "missing_from_completed_scope",
                    "publication_mode": "published_generation",
                    "required_for_baseline": baseline,
                    **(
                        {"enabled_by_default": False} if key in disabled else {}
                    ),
                    "sync": {
                        "triggers": ["manual", "reconcile"],
                        "default_event_mode": "scope_complete",
                        "reconcile_mode": "scope_complete",
                    },
                }
                for key, baseline in sources.items()
            },
        }),
        encoding="utf-8",
    )
    return directory


def _watermarks(dsn: str, ages: dict[str, float]) -> None:
    """One real watermark row per source, under its dashed ownership id."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dbos")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dbos.workflow_status (
              workflow_uuid text PRIMARY KEY,
              name text,
              queue_name text,
              status text NOT NULL,
              created_at timestamptz,
              updated_at timestamptz,
              started_at_epoch_ms bigint,
              priority int
            )
            """
        )
        for key, age_seconds in ages.items():
            source = f"{NAME}.{key.replace('_', '-')}"
            cur.execute(
                """
                INSERT INTO okg.source_watermarks
                  (source, last_applied_rev, last_reconcile_at)
                VALUES (%s, %s::jsonb, %s)
                ON CONFLICT (source)
                DO UPDATE SET last_applied_rev = EXCLUDED.last_applied_rev,
                              last_reconcile_at = EXCLUDED.last_reconcile_at
                """,
                (
                    source,
                    json.dumps({"source": source}),
                    datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
                ),
            )


def _stub_graph(
    monkeypatch: pytest.MonkeyPatch,
    *,
    published: bool = True,
    drift_status: str = "misconfigured",
) -> None:
    import okg.substrate.runtime.deployment_drift as drift_module

    monkeypatch.setattr(
        deployment_cli,
        "_ready_generation_status",
        lambda conn, deployment: {
            "generation_id": GENERATION if published else None,
            "deployment_name": deployment,
            "status": "published" if published else "missing",
            "node_count": 1,
            "edge_count": 0,
            "count_integrity": {"ok": True, "status": "clean"},
        },
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_mcp_status",
        lambda conn, deployment: {
            "ok": published,
            "pinned_generation_id": GENERATION if published else None,
            "node_count": 1,
        },
    )
    # A plain install declares no sync_root: drift is `misconfigured`, which
    # dogfood makes red and standard reports as a warning.
    monkeypatch.setattr(
        drift_module,
        "check_deployment_code_drift",
        lambda **kwargs: drift_module.DriftVerdict(
            deployment=NAME,
            status=drift_status,
            base_ref=None,
            mode=drift_module.MODE_PINNED,
            problems=(
                [] if drift_status == "ok" else [f"deployment {NAME!r} declares no sync_root"]
            ),
        ),
    )


NO_WORKER = {"ok": False, "status": "not_observed", "roots": [], "split_executor": False}
ONE_WORKER = {
    "ok": True,
    "status": "single_executor",
    "root_count": 1,
    "roots": [{"pid": 4242, "ppid": 1, "command": "okg runtime worker"}],
    "split_executor": False,
}


def _status(
    dsn: str,
    directory: Path,
    *,
    process_audit: dict[str, Any] | None = None,
    skip_doctor: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], Any]:
    """Live checks -> `okg dogfood status` payload -> chat banner reading."""
    from okg.substrate.chat.health_banner import read_health
    from okg.substrate.cli import dogfood

    profile = deployment_cli._ready_resolved_readiness_profile(directory)
    assert (profile["name"], profile["source"]) == ("standard", "declared")
    live = deployment_cli._deployment_ready_live_checks(
        deployment=NAME,
        deployment_dir=directory,
        dsn=dsn,
        max_active_queue_depth=0,
        max_source_age_seconds=14 * DAY,
        readiness_profile="standard",
        max_producer_age_seconds=15 * 60,
        skip_doctor=skip_doctor,
    )
    live = dict(live)
    live["runtime_processes"] = dict(process_audit or NO_WORKER)
    live["runtime_config"] = dogfood._declared_runtime_config(directory)
    payload = dogfood._status_payload(
        deployment=NAME,
        environment={"ok": True},
        live=live,
        resolved_dsn=True,
        include_details=False,
        readiness_profile="standard",
        profile_resolution=profile,
    )
    return live, payload, read_health(payload)


def _ids(rows: list[dict[str, Any]]) -> set[str]:
    return {str(row["id"]) for row in rows}


def _check(live: dict[str, Any], check_id: str) -> dict[str, Any]:
    rows = [row for row in live["checks"] if row["id"] == check_id]
    assert len(rows) == 1, (check_id, [row["id"] for row in live["checks"]])
    return rows[0]


# --- T4 (a), T5: fresh baseline sources, runtime off -----------------------------


@pytest.mark.postgres
def test_a_fresh_standard_install_is_ready_and_the_banner_healthy(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_graph(monkeypatch)
    directory = _write_deployment(
        tmp_path, sources={"release_notes": True, "doc_corpus": True}
    )
    _watermarks(clean_runtime, {"release_notes": 2 * DAY, "doc_corpus": 2 * DAY})

    live, payload, reading = _status(clean_runtime, directory)

    # T5: found under the dashed ownership id, not the underscore key.
    rows = live["source_freshness"]["groups"]["declared_baseline"]["sources"]
    assert {row["source"] for row in rows} == {
        f"{NAME}.release-notes",
        f"{NAME}.doc-corpus",
    }
    assert live["source_freshness"]["max_source_age_seconds"] == 14 * DAY
    assert _check(live, "sources.freshness")["status"] == "pass"
    sync = _check(live, "sources.sync_liveness")
    assert sync["status"] == "skipped"
    assert sync["details"]["reason"] == "runtime_disabled_by_config"
    assert _check(live, "deployment.code_drift")["status"] == "warning"
    assert "sources.optimization_budget" not in _ids(live["checks"])
    assert "publisher.lock_headroom" not in _ids(live["checks"])
    assert payload["blockers"] == []
    assert payload["trust_level"] == "ready"
    assert payload["runtime_worker"]["status"] == "disabled_by_config"
    assert reading.state == "healthy"


# --- T4 (b): one baseline source 15 days old --------------------------------------


@pytest.mark.postgres
def test_a_baseline_source_older_than_fourteen_days_is_stale_context(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_graph(monkeypatch)
    directory = _write_deployment(
        tmp_path, sources={"release_notes": True, "doc_corpus": True}
    )
    _watermarks(clean_runtime, {"release_notes": 15 * DAY, "doc_corpus": 2 * DAY})

    live, payload, reading = _status(clean_runtime, directory)

    assert live["source_freshness"]["stale"] == [f"{NAME}.release_notes"]
    assert _ids(payload["blockers"]) == {"sources.freshness"}
    assert payload["trust_level"] == "stale_context"
    assert reading.state == "degraded"
    assert "source freshness" in reading.conditions
    assert "dead-letter queue (not reported)" not in reading.conditions


@pytest.mark.postgres
def test_a_missing_baseline_source_is_stale_context(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_graph(monkeypatch)
    directory = _write_deployment(
        tmp_path, sources={"release_notes": True, "doc_corpus": True}
    )
    _watermarks(clean_runtime, {"doc_corpus": 2 * DAY})

    live, payload, _reading = _status(clean_runtime, directory)

    assert live["source_freshness"]["missing"] == [f"{NAME}.release_notes"]
    assert payload["trust_level"] == "stale_context"


# --- T4 (c): no published generation ------------------------------------------------


@pytest.mark.postgres
def test_no_published_generation_is_unavailable(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_graph(monkeypatch, published=False)
    directory = _write_deployment(tmp_path, sources={"release_notes": True})
    _watermarks(clean_runtime, {"release_notes": 2 * DAY})

    _live, payload, reading = _status(clean_runtime, directory)

    assert "generation.latest_published" in _ids(payload["blockers"])
    assert payload["trust_level"] == "unavailable"
    assert reading.state == "unavailable"
    assert "published generation" in reading.conditions


# --- T4 (d), (d2): no runtime block ---------------------------------------------------


@pytest.mark.postgres
def test_an_undeclared_runtime_is_ready_with_a_warning(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_graph(monkeypatch)
    directory = _write_deployment(
        tmp_path, sources={"release_notes": True}, runtime=None
    )
    _watermarks(clean_runtime, {"release_notes": 2 * DAY})

    live, payload, reading = _status(clean_runtime, directory)

    assert _check(live, "sources.sync_liveness")["details"]["reason"] == (
        "runtime_not_declared"
    )
    assert payload["runtime_config"]["status"] == "not_declared"
    assert payload["runtime_worker"]["status"] == "not_declared_default_disabled"
    assert "runtime.enabled_not_declared" in _ids(payload["warnings"])
    assert payload["blockers"] == []
    assert payload["trust_level"] == "ready"
    assert reading.state == "healthy"


@pytest.mark.postgres
def test_a_worker_seen_with_no_runtime_block_is_stale_context(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_graph(monkeypatch)
    directory = _write_deployment(
        tmp_path, sources={"release_notes": True}, runtime=None
    )
    _watermarks(clean_runtime, {"release_notes": 2 * DAY})

    _live, payload, reading = _status(
        clean_runtime, directory, process_audit=ONE_WORKER
    )

    blockers = {row["id"]: row for row in payload["blockers"]}
    assert set(blockers) == {"runtime.undeclared_but_running"}
    assert "runtime block is not declared" in blockers[
        "runtime.undeclared_but_running"
    ]["message"]
    assert payload["trust_level"] == "stale_context"
    assert reading.state == "degraded"
    assert "dead-letter queue (not reported)" not in reading.conditions


# --- T4 (e): no baseline sources, one source 90 days old ---------------------------


@pytest.mark.postgres
def test_no_baseline_sources_is_not_checked_with_an_advisory_and_a_healthy_banner(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.console_views import console_readiness

    _stub_graph(monkeypatch)
    directory = _write_deployment(
        tmp_path, sources={"release_notes": False, "doc_corpus": False}
    )
    _watermarks(clean_runtime, {"release_notes": 90 * DAY, "doc_corpus": 2 * DAY})

    live, payload, reading = _status(clean_runtime, directory)

    not_checked = {"status": "not_checked", "reason": "no_baseline_sources_declared"}
    assert payload["source_freshness"] == not_checked
    assert _check(live, "sources.freshness")["status"] == "skipped"
    assert _check(live, "sources.sync_liveness")["status"] == "skipped"
    advisory = {row["id"]: row for row in payload["warnings"]}[
        "sources.freshness_advisory"
    ]
    assert f"{NAME}.release_notes" in advisory["message"]
    assert f"{NAME}.doc_corpus" not in advisory["message"]
    assert payload["blockers"] == []
    assert payload["trust_level"] == "ready"
    # The documented gap: chat shows no staleness for this deployment.
    assert reading.state == "healthy"

    # The console reports the same not_checked, not `not_requested`.
    monkeypatch.setattr(
        console_readiness,
        "_generation_status",
        lambda conn, deployment, session=None: {
            "generation_id": GENERATION,
            "status": "published",
        },
    )
    monkeypatch.setattr(
        console_readiness,
        "_mcp_status",
        lambda conn, deployment, session=None: {
            "ok": True,
            "pinned_generation_id": GENERATION,
        },
    )
    console = console_readiness.compose_console_readiness(
        deployment_name=NAME,
        deployment_dir=directory,
        dsn=clean_runtime,
    )
    authority = console["read_authority"]
    assert authority["readiness_profile"]["name"] == "standard"
    assert authority["source_freshness"] == not_checked
    assert authority["source_sync_liveness"] == not_checked
    assert authority["source_sync_status"] == not_checked


@pytest.mark.postgres
def test_the_console_uses_the_standard_age_limit_for_baseline_sources(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.console_views import console_readiness

    directory = _write_deployment(tmp_path, sources={"release_notes": True})
    _watermarks(clean_runtime, {"release_notes": 2 * DAY})
    seen: dict[str, Any] = {}
    original = console_readiness._source_freshness_status

    def _freshness(conn: Any, **kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return original(conn, **kwargs)

    monkeypatch.setattr(console_readiness, "_source_freshness_status", _freshness)
    monkeypatch.setattr(
        console_readiness,
        "_generation_status",
        lambda conn, deployment, session=None: {"status": "published"},
    )
    monkeypatch.setattr(
        console_readiness,
        "_mcp_status",
        lambda conn, deployment, session=None: {"ok": True},
    )

    console = console_readiness.compose_console_readiness(
        deployment_name=NAME,
        deployment_dir=directory,
        dsn=clean_runtime,
    )

    assert seen["max_source_age_seconds"] == 14 * DAY
    assert console["read_authority"]["source_freshness"]["status"] == "available"
    assert console["details"]["readiness"]["source_freshness"]["ok"] is True


@pytest.mark.postgres
def test_the_advisory_leaves_out_a_disabled_source(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The design's "enabled declared source": `enabled_by_default: false`
    is the registry's own dormant rule, so a disabled source is not advice."""
    _stub_graph(monkeypatch)
    directory = _write_deployment(
        tmp_path,
        sources={"release_notes": False, "doc_corpus": False},
        disabled=("doc_corpus",),
    )
    _watermarks(clean_runtime, {"release_notes": 90 * DAY})

    live, payload, reading = _status(clean_runtime, directory)

    advisory = live["source_freshness_advisory"]
    assert advisory["checked"] == [f"{NAME}.release_notes"]
    assert advisory["disabled"] == [f"{NAME}.doc_corpus"]
    assert advisory["stale"] == [f"{NAME}.release_notes"]
    assert advisory["missing"] == []
    warning = {row["id"]: row for row in payload["warnings"]}[
        "sources.freshness_advisory"
    ]
    assert f"{NAME}.doc_corpus" not in warning["message"]
    assert payload["trust_level"] == "ready"
    assert reading.state == "healthy"


# --- T4 (f): the advisory probe fails ------------------------------------------------


@pytest.mark.postgres
@pytest.mark.parametrize("failure", ["raises", "times_out"])
def test_a_failing_advisory_probe_is_a_warning_and_leaves_trust_alone(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    _stub_graph(monkeypatch)
    directory = _write_deployment(tmp_path, sources={"release_notes": False})
    _watermarks(clean_runtime, {"release_notes": 90 * DAY})

    def _advisory(conn: Any, **_kwargs: Any) -> dict[str, Any]:
        if failure == "raises":
            raise RuntimeError("advisory probe broke")
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 1")
            try:
                cur.execute("SELECT pg_sleep(1)")
            finally:
                cur.execute("SET statement_timeout = 30000")
        raise AssertionError("the statement should have been cancelled")

    monkeypatch.setattr(
        deployment_cli, "_ready_source_freshness_advisory_status", _advisory
    )

    live, payload, reading = _status(clean_runtime, directory)

    warnings = {row["id"]: row for row in payload["warnings"]}
    assert "sources.freshness_advisory_unmeasured" in warnings
    assert "sources.freshness_advisory" not in warnings
    unmeasured = _check(live, "sources.freshness_advisory_unmeasured")
    assert unmeasured["status"] == "warning"
    assert unmeasured["details"]["reason"] == (
        "probe_failed" if failure == "raises" else "readiness_probe_timeout"
    )
    assert not any(row["id"].endswith(".unavailable") for row in live["checks"])
    assert payload["unknowns"] == []
    assert payload["trust_level"] == "ready"
    assert reading.state == "healthy"


# --- T6: a source registry that does not load -----------------------------------------


@pytest.mark.postgres
def test_an_unloadable_registry_is_named_and_never_a_database_failure(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_graph(monkeypatch)
    directory = _write_deployment(tmp_path, sources={"release_notes": True})
    (directory / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": {"release_notes": {"module": "x"}}}),
        encoding="utf-8",
    )

    live, payload, reading = _status(clean_runtime, directory)

    blockers = {row["id"]: row for row in live["blockers"]}
    assert "database.connectivity" not in blockers
    registry = blockers["sources.registry.unavailable"]
    assert registry["details"]["code"] == "source_registry_unloadable"
    assert "sources.freshness_advisory" not in _ids(live["checks"])
    assert payload["trust_level"] == "unavailable"
    assert reading.state == "unavailable"


# --- T7 (live half): standard runs the doctor when asked ---------------------------


@pytest.mark.postgres
@pytest.mark.parametrize("skip_doctor", [False, True])
def test_standard_runs_the_doctor_only_when_asked(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    skip_doctor: bool,
) -> None:
    _stub_graph(monkeypatch)
    directory = _write_deployment(tmp_path, sources={"release_notes": True})
    _watermarks(clean_runtime, {"release_notes": 2 * DAY})
    calls: list[bool] = []

    def _doctor(deployment: str, *, dsn: str, skip: bool) -> dict[str, Any]:
        calls.append(skip)
        if skip:
            return {"ok": None, "status": "skipped", "reason": "skip requested"}
        return {"ok": True, "status": "pass", "checks": []}

    monkeypatch.setattr(deployment_cli, "_ready_doctor_status", _doctor)

    live, _payload, _reading = _status(
        clean_runtime, directory, skip_doctor=skip_doctor
    )

    assert calls == [skip_doctor]
    assert live["doctor"]["status"] == ("skipped" if skip_doctor else "pass")


# --- T8: the capacity repair command names the profile -------------------------------


@pytest.mark.postgres
@pytest.mark.parametrize("profile", ["standard", "ci", "dogfood"])
def test_the_capacity_repair_command_names_the_profile_and_parses(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    profile: str,
) -> None:
    import shlex

    from okg.substrate.cli.main import _build_parser

    _stub_graph(monkeypatch)
    directory = _write_deployment(tmp_path, sources={"release_notes": True})

    def _capacity(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("capacity authority unreadable")

    monkeypatch.setattr(deployment_cli, "_ready_postgres_capacity_status", _capacity)

    live = deployment_cli._deployment_ready_live_checks(
        deployment=NAME,
        deployment_dir=directory,
        dsn=clean_runtime,
        max_active_queue_depth=0,
        max_source_age_seconds=14 * DAY,
        readiness_profile=profile,
        max_producer_age_seconds=15 * 60,
        skip_doctor=True,
    )

    command = live["database"]["postgres_capacity"]["repair_command"]
    words = shlex.split(command)
    if words[:2] == ["uv", "run"]:
        words = words[2:]
    assert words[0] == "okg"
    args = _build_parser().parse_args(words[1:])
    assert args.profile == profile
