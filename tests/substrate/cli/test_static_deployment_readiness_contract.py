from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.cli import deployment as deployment_cli


_EXACT_SOURCES = tuple(f"exact.source-{index}" for index in range(9))


def _write_exact_policy_deployment(tmp_path: Path) -> Path:
    deployment_dir = tmp_path / "exact-policy"
    policy_dir = deployment_dir / "nomos"
    policy_dir.mkdir(parents=True)
    deployment = "exact-policy"
    scope = f"deployment:{deployment}"
    source_policy = {
        "sensitivity": "public",
        "data_classification": "public",
        "credential_ref_policy": "none",
        "live_call_allowed": False,
        "exportability": "internal",
        "retention": "source_defined",
        "provenance": "local_filesystem",
        "privacy_obligations": ["audit", "no_export"],
    }
    manifest = {
        "name": deployment,
        "modules": [],
        "source_registry": "source_registry.yaml",
        "nomos": {
            "runtime_enforcement": "enforce",
            "source_binding_policy": "nomos/source-bindings.policy.json",
            "deployment_posture": {
                "deployment_class": "org_operational_private",
                "read_principals": [
                    "human_operator", "agent", "okg_service", "mcp_client",
                ],
                "write_principals": [
                    "human_operator", "okg_service", "source_adapter",
                ],
                "mcp_write_default": "review",
                "export_policy": "internal_only",
                "secret_handling": "secret_ref_required",
                "live_call_default": "deny",
                "allowed_source_sensitivities": ["public", "private"],
                "allowed_data_classifications": ["public", "private"],
                "allowed_exportability": ["internal", "no_export"],
                "privacy_obligations": [
                    "audit", "review", "no_export", "secret_ref_required",
                ],
                "readiness_profiles": ["org-private"],
            },
        },
    }
    registry = {
        "sources": {
            f"source-{index}": {
                "ownership_id": source,
                "source_class": "reference_catalog",
                "source_policy": source_policy,
            }
            for index, source in enumerate(_EXACT_SOURCES)
        },
    }
    rules = []
    for source in _EXACT_SOURCES:
        for action, resource_kind in (
            ("source.ingest", "source"),
            ("source.emit", "node"),
        ):
            rules.append({
                "rule_id": (
                    f"exact_{source.replace('.', '_').replace('-', '_')}_"
                    f"{action.split('.')[1]}"
                ),
                "effect": "allow",
                "priority": 150,
                "subjects": [f"source:{source}"],
                "actions": [action],
                "match": {
                    "deployment": deployment,
                    "scope": scope,
                    "source": source,
                    "graph_branch": "default",
                    "resource_kind": resource_kind,
                    "classification": "internal",
                    "principal_kind": "source_adapter",
                },
                "reason_code": "nomos.deployment.source_binding_allow",
                "obligations": [{"type": "audit"}],
            })
    artifact = {
        "policy_bundle_id": f"nomos:deployment:{deployment}:source-bindings",
        "policy_version": "v1",
        "default_effect": "deny",
        "vocabulary_strict": True,
        "scope": {"deployment": deployment, "scope": scope},
        "rules": rules,
    }
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump(manifest),
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        yaml.safe_dump(registry),
        encoding="utf-8",
    )
    (policy_dir / "source-bindings.policy.json").write_text(
        json.dumps(artifact),
        encoding="utf-8",
    )
    return deployment_dir


def test_nomos_readiness_uses_composed_exact_policy_fingerprint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.nomos as nomos
    import psycopg
    from okg.substrate.nomos.baseline import deployment_baseline_spec

    deployment_dir = _write_exact_policy_deployment(tmp_path)
    manifest = yaml.safe_load(
        (deployment_dir / "deployment.yaml").read_text(encoding="utf-8")
    )
    spec = deployment_baseline_spec(
        deployment="exact-policy",
        deployment_dir=deployment_dir,
    )
    rules = spec["policy_graph"]["rules"]
    assert len(rules) == 30
    assert sum(rule["rule_id"].startswith("exact_") for rule in rules) == 18
    rule_ids = {rule["rule_id"] for rule in rules}
    assert "rollout_allow_registered_source_runs" not in rule_ids
    assert "rollout_allow_manifest_local_operator_boundary" in rule_ids

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            del args

    monkeypatch.setattr(psycopg, "connect", lambda *args, **kwargs: Connection())
    monkeypatch.setattr(
        nomos,
        "active_nomos_summary",
        lambda *args, **kwargs: {
            "active_policy": {
                "policy_fingerprint": spec["policy_fingerprint"],
            },
            "active_config": {
                "config_fingerprint": spec["config_fingerprint"],
            },
            "recent_decision_count": 0,
        },
    )

    status = deployment_cli._ready_nomos_active_state_status(
        deployment="exact-policy",
        dsn="postgres://unused",
        manifest=manifest,
        deployment_dir=deployment_dir,
        readiness_profile="org-private",
    )

    assert status["ok"] is True
    assert status["expected_policy"]["policy_fingerprint"] == (
        spec["policy_fingerprint"]
    )
    assert status["findings"] == []


def _write_static_deployment(tmp_path: Path) -> Path:
    deployment_dir = tmp_path / "static-demo"
    deployment_dir.mkdir()
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "static-demo",
            "postgres": {"dsn": "${STATIC_DEMO_DSN}"},
            "runtime": {
                "enabled": False,
                "source_sync": {
                    "watch": {"enabled": False},
                    "poll": {"enabled": False},
                    "webhook": {"enabled": False},
                },
            },
            "source_registry": "source_registry.yaml",
        }),
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({
            "sources": {
                "docs": {
                    "module": "static_demo.source",
                    "class": "StaticDemoSource",
                    "ownership_id": "static-demo.docs",
                    "source_class": "discovery_crawl",
                    "record_identity_kind": "scoped_locator",
                    "record_identity_fields": ["path"],
                    "source_revision_kind": "content_hash",
                    "deletion_semantics": "missing_from_completed_scope",
                    "publication_mode": "published_generation",
                    "enabled_by_default": True,
                    "required_for_baseline": True,
                    "sync": {
                        "triggers": ["poll", "manual", "reconcile"],
                        "default_event_mode": "scope_complete",
                        "reconcile_mode": "scope_complete",
                        "poll": {
                            "probe": "change_probe",
                            "interval_seconds": 300,
                        },
                    },
                },
            },
        }),
        encoding="utf-8",
    )
    return deployment_dir


@pytest.mark.postgres
def test_static_dogfood_readiness_uses_declared_baseline_sources(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment_dir = _write_static_deployment(tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Dev's source-sync status contract fails closed when the DBOS
            # DLQ surface is unobservable; give the probe an empty (healthy)
            # dbos.workflow_status table so DLQ status is known-zero.
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
                    "static-demo.docs",
                    json.dumps({"source": "static-demo.docs"}),
                    datetime.now(timezone.utc),
                ),
            )

    monkeypatch.setattr(
        deployment_cli,
        "_ready_generation_status",
        lambda conn, deployment: {
            "generation_id": "okg:static-demo:branch:default:gen:test",
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
            "ok": True,
            "pinned_generation_id": "okg:static-demo:branch:default:gen:test",
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

    payload = deployment_cli._deployment_ready_live_checks(
        deployment="static-demo",
        deployment_dir=deployment_dir,
        dsn=clean_runtime,
        max_active_queue_depth=0,
        max_source_age_seconds=60 * 60,
        readiness_profile="dogfood",
        max_producer_age_seconds=15 * 60,
        skip_doctor=True,
    )

    assert payload["source_freshness"]["groups"].keys() == {
        "declared_baseline",
    }
    assert payload["source_freshness"]["groups"]["declared_baseline"]["ok"]
    assert payload["source_freshness"]["missing"] == []
    assert payload["source_freshness"]["stale"] == []
    assert {
        row["source"]
        for row in payload["source_freshness"]["groups"][
            "declared_baseline"
        ]["sources"]
    } == {"static-demo.docs"}

    assert payload["source_sync_status"]["ok"] is True
    assert payload["source_sync_liveness"]["expected_producer_count"] == 0
    assert payload["source_sync_liveness"]["disabled_producer_count"] == 1
    assert payload["source_sync_liveness"]["disabled_producers"][0][
        "source_name"
    ] == "docs"
    assert {
        blocker["id"] for blocker in payload["blockers"]
    }.isdisjoint({"sources.freshness", "sources.sync_liveness"})


def test_okg_workspace_dogfood_still_uses_agent_context_sources(
    tmp_path: Path,
) -> None:
    deployment_dir = tmp_path / "okg-workspace"
    deployment_dir.mkdir()
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "okg-workspace",
            "postgres": {"dsn": "${OKG_DSN}"},
            # Detection is manifest-based (deployment-qa capability forbids
            # name branches): the real okg-workspace manifest declares the
            # workspace-graph module set, which is what selects the
            # agent-context source groups.
            "modules": sorted(deployment_cli._READY_WORKSPACE_GRAPH_MODULES),
        }),
        encoding="utf-8",
    )

    groups = deployment_cli._ready_source_groups_for_profile(
        "dogfood",
        deployment="okg-workspace",
        deployment_dir=deployment_dir,
    )

    assert groups == deployment_cli._READY_AGENT_CONTEXT_SOURCE_GROUPS


def _write_workspace_graph_deployment(
    tmp_path: Path,
    *,
    name: str,
    declared_sources: tuple[str, ...],
    runtime_disabled: bool = False,
    key_style: str = "dash",
    readiness_profile: str | None = None,
) -> Path:
    """A deployment built from the workspace-graph profile that declares only
    ``declared_sources``, the shape of a plain install of that profile.

    ``readiness_profile`` adds a `readiness.profile` declaration (#3958).
    """
    deployment_dir = tmp_path / name
    deployment_dir.mkdir()
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": name,
            "profile": {"name": "workspace-graph"},
            "postgres": {"dsn": "${OKG_DSN}"},
            "source_registry": "source_registry.yaml",
            **({"runtime": {"enabled": False}} if runtime_disabled else {}),
            **(
                {"readiness": {"profile": readiness_profile}}
                if readiness_profile
                else {}
            ),
        }),
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({
            "sources": {
                (
                    source.replace("-", "_")
                    if key_style == "underscore"
                    else source
                ): {
                    "module": "static_demo.source",
                    "class": "StaticDemoSource",
                    "ownership_id": f"{name}.{source}",
                    "source_class": "discovery_crawl",
                    "record_identity_kind": "scoped_locator",
                    "record_identity_fields": ["path"],
                    "source_revision_kind": "content_hash",
                    "deletion_semantics": "missing_from_completed_scope",
                    "publication_mode": "published_generation",
                    "enabled_by_default": True,
                    "sync": {
                        "triggers": ["manual", "reconcile"],
                        "default_event_mode": "scope_complete",
                        "reconcile_mode": "scope_complete",
                    },
                }
                for source in declared_sources
            },
        }),
        encoding="utf-8",
    )
    return deployment_dir


class _WatermarkConnection:
    """Answers the watermark lookup with one source age, in seconds."""

    def __init__(self, age_seconds: float) -> None:
        self._age_seconds = age_seconds
        self._row: tuple[object, ...] | None = None

    def cursor(self) -> "_WatermarkConnection":
        return self

    def __enter__(self) -> "_WatermarkConnection":
        return self

    def __exit__(self, *args: object) -> None:
        del args

    def execute(self, sql: str, params: tuple[list[str], list[str]]) -> None:
        del sql
        self._row = (
            params[0][0],
            datetime.now(timezone.utc),
            self._age_seconds,
        )

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row


def test_workspace_graph_dogfood_judges_only_declared_sources(
    tmp_path: Path,
) -> None:
    """Issue 3958: a deployment that declares only some of the team's own
    sources must not be judged against the ones it never declared."""
    deployment_dir = _write_workspace_graph_deployment(
        tmp_path,
        name="plain-install",
        declared_sources=("code-structure",),
    )

    groups = deployment_cli._ready_source_groups_for_profile(
        "dogfood",
        deployment="plain-install",
        deployment_dir=deployment_dir,
    )

    assert groups == {"code": ("code-structure",)}


def test_workspace_graph_dogfood_keeps_every_declared_agent_context_source(
    tmp_path: Path,
) -> None:
    every_source = tuple(
        source
        for sources in deployment_cli._READY_AGENT_CONTEXT_SOURCE_GROUPS.values()
        for source in sources
    )
    deployment_dir = _write_workspace_graph_deployment(
        tmp_path,
        name="team-workspace",
        declared_sources=every_source,
    )

    groups = deployment_cli._ready_source_groups_for_profile(
        "dogfood",
        deployment="team-workspace",
        deployment_dir=deployment_dir,
    )

    assert groups == deployment_cli._READY_AGENT_CONTEXT_SOURCE_GROUPS


def test_workspace_graph_dogfood_reports_a_stale_declared_source(
    tmp_path: Path,
) -> None:
    deployment_dir = _write_workspace_graph_deployment(
        tmp_path,
        name="plain-install",
        declared_sources=("code-structure",),
    )
    groups = deployment_cli._ready_source_groups_for_profile(
        "dogfood",
        deployment="plain-install",
        deployment_dir=deployment_dir,
    )

    fresh = deployment_cli._ready_source_freshness_status(
        _WatermarkConnection(age_seconds=60),
        deployment="plain-install",
        max_source_age_seconds=3600,
        source_groups=groups,
    )
    stale = deployment_cli._ready_source_freshness_status(
        _WatermarkConnection(age_seconds=7200),
        deployment="plain-install",
        max_source_age_seconds=3600,
        source_groups=groups,
    )

    assert fresh["ok"] is True
    assert fresh["missing"] == []
    assert fresh["groups"].keys() == {"code"}
    assert stale["ok"] is False
    assert stale["stale"] == ["plain-install.code-structure"]
    assert stale["missing"] == []


def test_workspace_graph_dogfood_matches_underscore_registry_keys(
    tmp_path: Path,
) -> None:
    """cern-team spells the registry key `code_structure`; the readiness list
    says `code-structure`. The source is declared either way."""
    deployment_dir = _write_workspace_graph_deployment(
        tmp_path,
        name="plain-install",
        declared_sources=("code-structure",),
        key_style="underscore",
    )

    groups = deployment_cli._ready_source_groups_for_profile(
        "dogfood",
        deployment="plain-install",
        deployment_dir=deployment_dir,
    )

    assert groups == {"code": ("code-structure",)}


def test_workspace_graph_dogfood_keeps_full_list_for_an_unloadable_registry(
    tmp_path: Path,
) -> None:
    """A registry that exists but does not load must reach the sync-liveness
    probe (which reports the load error), not abort the status as a database
    failure."""
    deployment_dir = _write_workspace_graph_deployment(
        tmp_path,
        name="plain-install",
        declared_sources=("code-structure",),
    )
    (deployment_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": {"code-structure": {"module": "x"}}}),
        encoding="utf-8",
    )

    groups = deployment_cli._ready_source_groups_for_profile(
        "dogfood",
        deployment="plain-install",
        deployment_dir=deployment_dir,
    )

    assert groups == deployment_cli._READY_AGENT_CONTEXT_SOURCE_GROUPS


def test_real_okg_workspace_keeps_the_full_agent_context_list() -> None:
    """The team's own workspace declares every agent-context source, so the
    narrowing must leave its list untouched."""
    deployment_dir = Path(__file__).resolve().parents[3] / "deployments" / (
        "okg-workspace"
    )

    groups = deployment_cli._ready_source_groups_for_profile(
        "dogfood",
        deployment="okg-workspace",
        deployment_dir=deployment_dir,
    )

    assert groups == deployment_cli._READY_AGENT_CONTEXT_SOURCE_GROUPS


def test_a_source_dropped_from_the_workspace_registry_is_reported(
    tmp_path: Path,
) -> None:
    every_source = tuple(
        source
        for sources in deployment_cli._READY_AGENT_CONTEXT_SOURCE_GROUPS.values()
        for source in sources
    )
    # The warning is for a deployment that declares dogfood (#3958).
    deployment_dir = _write_workspace_graph_deployment(
        tmp_path,
        name="team-workspace",
        declared_sources=tuple(s for s in every_source if s != "openspec"),
        readiness_profile="dogfood",
    )
    from okg.substrate.cli.deployment_readiness import (
        _ready_undeclared_agent_context_sources,
    )

    groups = deployment_cli._ready_source_groups_for_profile(
        "dogfood",
        deployment="team-workspace",
        deployment_dir=deployment_dir,
    )
    undeclared = _ready_undeclared_agent_context_sources(
        "dogfood",
        deployment="team-workspace",
        deployment_dir=deployment_dir,
        source_groups=groups,
    )

    assert undeclared == ["openspec"]


def test_sync_liveness_counts_an_underscore_registry_key_as_the_readiness_source(
    tmp_path: Path,
) -> None:
    from okg.substrate.source_sync_convergence import (
        _readiness_summary,
        source_inventory_for_registry,
    )
    from okg.substrate.sources.registry import load_source_registry

    deployment_dir = _write_workspace_graph_deployment(
        tmp_path,
        name="plain-install",
        declared_sources=("code-structure",),
        key_style="underscore",
    )
    groups = {"code": ("code-structure",)}

    inventory = source_inventory_for_registry(
        deployment="plain-install",
        registry=load_source_registry(deployment_dir / "source_registry.yaml"),
        readiness_source_groups=groups,
    )
    summary = _readiness_summary(
        "plain-install",
        inventory,
        readiness_source_groups=groups,
    )

    assert [row["source_name"] for row in inventory] == ["code_structure"]
    assert inventory[0]["freshness_policy"]["readiness_required"] is True
    assert summary["missing_required_sources"] == []
    assert summary["present_required_sources"] == ["plain-install.code-structure"]
