"""Plain-install readiness and the chat health banner, against a real database.

Split from test_static_deployment_readiness_contract.py (PACT
lane-bt-health-banner-own-sources-3958): every test here is postgres-marked
and new with this change, so the pull-request lane-scope check needs its own
pull-request owner row for this file rather than the nightly-only owner of the
contract file.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.cli import deployment as deployment_cli
from tests.substrate.cli.test_static_deployment_readiness_contract import (
    _write_workspace_graph_deployment,
)


def _stub_plain_install_runtime(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    watermark_age_seconds: float,
    drift_status: str = "ok",
    mcp_ok: bool = True,
) -> None:
    """Everything `_deployment_ready_live_checks` reads, for a deployment named
    plain-install whose only measured source is code-structure."""
    from datetime import timedelta

    import okg.substrate.runtime.deployment_drift as drift_module

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
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
                    "plain-install.code-structure",
                    json.dumps({"source": "plain-install.code-structure"}),
                    datetime.now(timezone.utc)
                    - timedelta(seconds=watermark_age_seconds),
                ),
            )
    generation = "okg:plain-install:branch:default:gen:test"
    monkeypatch.setattr(
        deployment_cli,
        "_ready_generation_status",
        lambda conn, deployment: {
            "generation_id": generation,
            "deployment_name": deployment,
            "status": "published",
            "node_count": 1,
            "edge_count": 0,
            "count_integrity": {"ok": True, "status": "clean"},
        },
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_mcp_status",
        lambda conn, deployment: {
            "ok": mcp_ok,
            "pinned_generation_id": generation,
            "node_count": 1,
        },
    )
    monkeypatch.setattr(
        deployment_cli,
        "_ready_source_optimization_status",
        lambda **kwargs: {
            "ok": True,
            "status": "pass",
            "readiness_required_count": 0,
            "violation_count": 0,
            "warnings": [],
            "violations": [],
        },
    )
    monkeypatch.setattr(
        drift_module,
        "check_deployment_code_drift",
        lambda **kwargs: drift_module.DriftVerdict(
            deployment="plain-install",
            status=drift_status,
            base_ref=None,
            mode=drift_module.MODE_PINNED,
            problems=(
                []
                if drift_status == "ok"
                else ["deployment 'plain-install' declares no sync_root"]
            ),
        ),
    )


def _plain_install_status_and_banner(
    clean_runtime: str,
    tmp_path: Path,
    *,
    key_style: str = "dash",
    declared_sources: tuple[str, ...] = ("code-structure",),
    registry_text: str | None = None,
) -> tuple[dict[str, object], object]:
    """Live checks -> `okg dogfood status` payload -> chat banner reading."""
    from okg.substrate.chat.health_banner import read_health
    from okg.substrate.cli import dogfood

    deployment_dir = _write_workspace_graph_deployment(
        tmp_path,
        name="plain-install",
        declared_sources=declared_sources,
        runtime_disabled=True,
        key_style=key_style,
        # These cases run the dogfood checks; the deployment says so (#3958),
        # which is what makes the undeclared-source warning apply.
        readiness_profile="dogfood",
    )
    if registry_text is not None:
        (deployment_dir / "source_registry.yaml").write_text(
            registry_text, encoding="utf-8"
        )
    live = deployment_cli._deployment_ready_live_checks(
        deployment="plain-install",
        deployment_dir=deployment_dir,
        dsn=clean_runtime,
        max_active_queue_depth=0,
        max_source_age_seconds=60 * 60,
        readiness_profile="dogfood",
        max_producer_age_seconds=15 * 60,
        skip_doctor=True,
    )
    live = dict(live)
    live["runtime_config"] = {"status": "disabled"}
    payload = dogfood._status_payload(
        deployment="plain-install",
        environment={"ok": True},
        live=live,
        resolved_dsn=True,
        include_details=False,
    )
    return payload, read_health(payload)


@pytest.mark.postgres
def test_plain_install_with_a_fresh_declared_source_has_no_false_freshness_condition(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_plain_install_runtime(
        clean_runtime,
        monkeypatch,
        watermark_age_seconds=60,
        drift_status="ok",
    )

    payload, reading = _plain_install_status_and_banner(clean_runtime, tmp_path)

    freshness = payload["source_freshness"]
    assert freshness["missing"] == []
    assert freshness["stale"] == []
    assert freshness["undeclared"] == [
        "agent-sessions",
        "codex-sessions",
        "coordination-config",
        "coordination-native",
        "git-files",
        "openspec",
        "pact",
    ]
    blocker_ids = {row["id"] for row in payload["blockers"]}
    assert "sources.freshness" not in blocker_ids
    assert "sources.readiness_undeclared" in {
        row["id"] for row in payload["warnings"]
    }
    # What is left is not a source problem: the stubbed install has no
    # measured lock hold and no source lineage, so trust stays stale_context.
    # The banner names no condition, and it is not unavailable.
    assert payload["trust_level"] == "stale_context"
    assert reading.state == "degraded"
    assert reading.conditions == ()


@pytest.mark.postgres
def test_plain_install_with_a_stale_declared_source_still_warns(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_plain_install_runtime(
        clean_runtime,
        monkeypatch,
        watermark_age_seconds=3 * 24 * 3600,
        drift_status="ok",
    )

    payload, reading = _plain_install_status_and_banner(clean_runtime, tmp_path)

    assert payload["source_freshness"]["stale"] == [
        "plain-install.code-structure"
    ]
    assert payload["trust_level"] == "stale_context"
    assert reading.state == "degraded"
    assert reading.severity == "warning"
    assert "source freshness" in reading.conditions


@pytest.mark.postgres
def test_an_unreadable_graph_turns_the_banner_unavailable_with_no_source_cause(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sources alone never produce `unavailable` (they lower trust to
    `stale_context`). An MCP graph read that fails does, and the banner then
    names no source condition: the red state needs a separate cause."""
    _stub_plain_install_runtime(
        clean_runtime,
        monkeypatch,
        watermark_age_seconds=60,
        mcp_ok=False,
    )

    payload, reading = _plain_install_status_and_banner(clean_runtime, tmp_path)

    assert payload["source_freshness"]["missing"] == []
    assert "mcp.graph_read" in {row["id"] for row in payload["blockers"]}
    assert payload["trust_level"] == "unavailable"
    assert reading.state == "unavailable"
    assert "source freshness" not in reading.conditions


@pytest.mark.postgres
def test_underscore_registry_key_is_readiness_required_like_a_dashed_key(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cms-kb-public spells the key `code_structure` and declares no
    sync_root. Sync liveness must count that source as the readiness source
    `code-structure`, the way freshness does."""
    _stub_plain_install_runtime(
        clean_runtime,
        monkeypatch,
        watermark_age_seconds=60,
        drift_status="misconfigured",
    )

    payload, reading = _plain_install_status_and_banner(
        clean_runtime,
        tmp_path,
        key_style="underscore",
    )

    assert payload["source_freshness"]["missing"] == []
    assert payload["trust_level"] == "stale_context"
    assert reading.state == "degraded"


@pytest.mark.postgres
def test_a_registry_that_does_not_load_is_not_reported_as_a_database_failure(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_plain_install_runtime(
        clean_runtime,
        monkeypatch,
        watermark_age_seconds=60,
    )

    payload, reading = _plain_install_status_and_banner(
        clean_runtime,
        tmp_path,
        registry_text=yaml.safe_dump({
            "sources": {"code-structure": {"module": "x"}},
        }),
    )

    blocker_ids = {row["id"] for row in payload["blockers"]}
    assert "database.connectivity" not in blocker_ids
    assert "sources.sync_liveness.unavailable" in blocker_ids
    assert reading.state == "unavailable"


@pytest.mark.postgres
def test_a_workspace_graph_deployment_declaring_none_of_the_eight_is_warned(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing is checked for freshness, and the status says so in a warning."""
    _stub_plain_install_runtime(
        clean_runtime,
        monkeypatch,
        watermark_age_seconds=60,
    )

    payload, reading = _plain_install_status_and_banner(
        clean_runtime,
        tmp_path,
        declared_sources=("doc-corpus",),
    )

    warnings = {row["id"]: row for row in payload["warnings"]}
    assert warnings["sources.readiness_undeclared"]["details"]["undeclared"] == (
        sorted(
            source
            for sources in (
                deployment_cli._READY_AGENT_CONTEXT_SOURCE_GROUPS.values()
            )
            for source in sources
        )
    )
    assert "sources.freshness" not in {row["id"] for row in payload["blockers"]}
