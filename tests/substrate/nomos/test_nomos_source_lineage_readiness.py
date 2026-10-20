from __future__ import annotations

from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.nomos.doctor import (
    _registry_source_ids,
    source_lineage_readiness_report,
)
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR, normalize_source_lineage
from tests.pytest.catalog_fixture_builders import seed_catalog


pytestmark = pytest.mark.postgres


def test_registry_source_ids_resolve_manifest_relative_overlay(
    tmp_path: Path,
    monkeypatch,
) -> None:
    deployments = tmp_path / "deployments"
    shared = deployments / "okg-ci"
    overlay = deployments / "okg-ci-dev"
    shared.mkdir(parents=True)
    overlay.mkdir()
    (shared / "source_registry.yaml").write_text(
        "sources:\n  pact: {}\n  git-history: {}\n",
        encoding="utf-8",
    )
    (overlay / "deployment.yaml").write_text(
        "name: okg-ci-dev\nsource_registry: ../okg-ci/source_registry.yaml\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda name: overlay if name == "okg-ci-dev" else None,
    )

    report = _registry_source_ids("okg-ci-dev")

    assert report["status"] == "ok"
    assert report["source_ids"] == ["git-history", "pact"]
    assert report["path"].endswith("okg-ci-dev/../okg-ci/source_registry.yaml")


def test_source_lineage_readiness_reports_active_state(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "nomos-source-lineage-ready-active"
    bundle = _activate_source_policy(clean_runtime, deployment, ["public_docs"])
    _publish_nodes(
        clean_runtime,
        deployment,
        [("node:public", "public_docs", bundle.policy_fingerprint)],
    )
    _patch_registry(monkeypatch, ["public_docs"])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with psycopg.connect(clean_runtime) as conn:
        report = source_lineage_readiness_report(
            conn,
            deployment_name=deployment,
            readiness_profile="dogfood",
        )

    assert report["ok"] is True
    assert report["mode"] == "active"
    assert report["sources_missing_policy"] == []
    assert report["graph_source_ids"] == ["public_docs"]
    assert report["read_surfaces"]["mcp"] == "source_lineage_filtered"


def test_source_lineage_readiness_reports_missing_policy_source(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "nomos-source-lineage-ready-missing-policy"
    bundle = _activate_source_policy(
        clean_runtime,
        deployment,
        ["public_docs"],
        include_generic_boundary=True,
    )
    _publish_nodes(
        clean_runtime,
        deployment,
        [
            ("node:public", "public_docs", bundle.policy_fingerprint),
            ("node:google", "google_drive", None),
        ],
    )
    _patch_registry(monkeypatch, ["public_docs", "google_drive"])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with psycopg.connect(clean_runtime) as conn:
        report = source_lineage_readiness_report(
            conn,
            deployment_name=deployment,
            readiness_profile="dogfood",
        )

    assert report["ok"] is False
    assert report["mode"] == "missing_policy"
    assert report["sources_missing_policy"] == ["google_drive"]
    assert report["source_policy"]["generic_read_policy_count"] == 1
    assert report["source_policy"]["covered_source_ids"] == ["public_docs"]


def test_source_lineage_readiness_does_not_trust_source_supplied_attrs_lineage(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "nomos-source-lineage-ready-lineage-spoof"
    bundle = _activate_source_policy(clean_runtime, deployment, ["public_docs"])
    _publish_node_with_attr_lineage_and_row_source(
        clean_runtime,
        deployment,
        node_id="node:spoofed",
        attr_source_id="public_docs",
        row_source="google_drive",
        policy_fingerprint=bundle.policy_fingerprint,
    )
    _patch_registry(monkeypatch, ["public_docs", "google_drive"])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with psycopg.connect(clean_runtime) as conn:
        report = source_lineage_readiness_report(
            conn,
            deployment_name=deployment,
            readiness_profile="dogfood",
        )

    assert report["ok"] is False
    assert report["mode"] == "missing_policy"
    assert report["graph_source_ids"] == ["google_drive"]
    assert report["sources_missing_policy"] == ["google_drive"]


def test_source_lineage_readiness_uses_deployment_scope_policy_only(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "nomos-source-lineage-ready-scope"
    _activate_source_policy(
        clean_runtime,
        deployment,
        [],
        include_generic_boundary=True,
    )
    _activate_source_policy(
        clean_runtime,
        deployment,
        ["public_docs"],
        scope="deployment:other-scope",
    )
    _publish_nodes(
        clean_runtime,
        deployment,
        [("node:public", "public_docs", None)],
    )
    _patch_registry(monkeypatch, ["public_docs"])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with psycopg.connect(clean_runtime) as conn:
        report = source_lineage_readiness_report(
            conn,
            deployment_name=deployment,
            readiness_profile="dogfood",
        )

    assert report["ok"] is False
    assert report["mode"] == "missing_policy"
    assert report["active_policy_count"] == 1
    assert report["source_policy"]["covered_source_ids"] == []
    assert report["sources_missing_policy"] == ["public_docs"]


def test_source_lineage_readiness_reports_stale_policy_fingerprint_as_audit(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "nomos-source-lineage-ready-stale"
    _activate_source_policy(clean_runtime, deployment, ["public_docs"])
    _publish_nodes(
        clean_runtime,
        deployment,
        [("node:stale", "public_docs", "sha256:old-policy")],
    )
    _patch_registry(monkeypatch, ["public_docs"])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with psycopg.connect(clean_runtime) as conn:
        report = source_lineage_readiness_report(
            conn,
            deployment_name=deployment,
            readiness_profile="dogfood",
        )

    assert report["ok"] is True
    assert report["mode"] == "active"
    assert report["stale_policy_sources"] == ["public_docs"]


def test_source_lineage_readiness_quarantines_missing_lineage(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "nomos-source-lineage-ready-quarantine"
    _activate_source_policy(clean_runtime, deployment, ["public_docs"])
    _publish_nodes(
        clean_runtime,
        deployment,
        [("node:missing-lineage", None, None)],
    )
    _patch_registry(monkeypatch, ["public_docs"])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with psycopg.connect(clean_runtime) as conn:
        report = source_lineage_readiness_report(
            conn,
            deployment_name=deployment,
            readiness_profile="dogfood",
        )

    assert report["ok"] is False
    assert report["mode"] == "quarantine"
    assert report["missing_lineage"]["node_count"] == 1


def test_source_lineage_readiness_quarantines_malformed_lineage_without_row_source_fallback(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "nomos-source-lineage-ready-malformed-no-fallback"
    bundle = _activate_source_policy(clean_runtime, deployment, ["public_docs"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Same as `_publish_nodes`: the checkpoint must name an applied
        # catalog version, or the readiness probe is refused (#2248).
        catalog_version_id = seed_catalog(conn, subtypes=["page"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "node:malformed",
                    "page",
                    {
                        "classification": "internal",
                        "text": "malformed",
                        LINEAGE_ATTR: {
                            "schema": "okg.nomos.source_lineage.v1",
                            "status": "malformed",
                            "source_ids": ["public_docs"],
                            "sources": [
                                {
                                    "source_id": "public_docs",
                                    "policy_fingerprints": [
                                        bundle.policy_fingerprint
                                    ],
                                }
                            ],
                        },
                    },
                    {"id": "node:malformed"},
                    {"v": 1},
                ),
            ],
            source="_publisher_derived",
            policy_fingerprint=None,
        )
        store.checkpoint_generation(
            0,
            deployment_name=deployment,
            catalog_version_id=catalog_version_id,
        )
    _patch_registry(monkeypatch, ["public_docs"])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with psycopg.connect(clean_runtime) as conn:
        report = source_lineage_readiness_report(
            conn,
            deployment_name=deployment,
            readiness_profile="dogfood",
        )

    assert report["ok"] is False
    assert report["mode"] == "quarantine"
    assert report["sources_missing_policy"] == []
    assert "_publisher_derived" not in report["graph_source_ids"]
    assert report["missing_lineage"]["node_count"] == 1


def test_source_lineage_readiness_blocks_protected_bypass(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "nomos-source-lineage-ready-bypass"
    _patch_registry(monkeypatch, [])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "0")

    with psycopg.connect(clean_runtime) as conn:
        report = source_lineage_readiness_report(
            conn,
            deployment_name=deployment,
            readiness_profile="dogfood",
        )

    assert report["ok"] is False
    assert report["mode"] == "bypassed"
    assert report["errors"][0]["code"] == "nomos_source_lineage_runtime_bypassed"
    assert report["read_surfaces"]["mcp"] == "compatibility_bypass"


def test_deployment_ready_and_dogfood_payload_include_source_lineage(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "nomos-source-lineage-ready-surface"
    bundle = _activate_source_policy(clean_runtime, deployment, ["public_docs"])
    _publish_nodes(
        clean_runtime,
        deployment,
        [("node:public", "public_docs", bundle.policy_fingerprint)],
    )
    _patch_registry(monkeypatch, ["public_docs"])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    from okg.substrate.cli.deployment import _deployment_ready_live_checks

    live = _deployment_ready_live_checks(
        deployment=deployment,
        deployment_dir=Path("."),
        dsn=clean_runtime,
        max_active_queue_depth=0,
        max_source_age_seconds=3600,
        readiness_profile="general",
    )

    assert live["nomos_source_lineage"]["mode"] == "active"
    assert any(
        check["id"] == "nomos.source_lineage" and check["status"] == "pass"
        for check in live["checks"]
    )

    from okg.substrate.cli import dogfood

    monkeypatch.setattr(
        dogfood,
        "_runtime_worker_process_audit",
        lambda _deployment: {
            "ok": True,
            "status": "single_executor",
            "split_executor": False,
            "process_count": 1,
            "root_count": 1,
            "processes": [],
            "roots": [],
        },
    )
    payload = dogfood._status_payload(
        deployment=deployment,
        environment={"ok": True},
        live={
            "checks": [],
            "blockers": [],
            "warnings": [],
            "generation": {
                "status": "published",
                "generation_id": "generation:test",
            },
            "mcp": {
                "ok": True,
                "pinned_generation_id": "generation:test",
            },
            "nomos_source_lineage": live["nomos_source_lineage"],
        },
        resolved_dsn=True,
        include_details=False,
    )

    assert payload["trust_level"] == "ready"
    assert payload["nomos_source_lineage"]["mode"] == "active"


def _activate_source_policy(
    dsn: str,
    deployment: str,
    source_ids: list[str],
    *,
    include_generic_boundary: bool = False,
    scope: str | None = None,
) -> Any:
    scope = scope or f"deployment:{deployment}"
    rules: list[dict[str, Any]] = []
    if include_generic_boundary:
        rules.append({
            "rule_id": "allow_generic_mcp_boundary",
            "effect": "allow",
            "priority": 200,
            "subjects": ["subject:mcp_client"],
            "actions": ["graph.read.mcp"],
            "match": {
                "deployment": deployment,
                "scope": scope,
            },
        })
    for source_id in source_ids:
        rules.append({
            "rule_id": f"allow_{source_id}_mcp_read",
            "effect": "allow",
            "priority": 100,
            "subjects": ["subject:mcp_client"],
            "actions": ["graph.read.mcp"],
            "match": {
                "deployment": deployment,
                "scope": scope,
                "source_id": source_id,
            },
        })
    bundle = compile_policy_graph({
        "policy_bundle_id": f"nomos:test:{deployment}",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules,
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:source-lineage-readiness",
            bootstrap=True,
        )
        conn.commit()
    return bundle


def _publish_nodes(
    dsn: str,
    deployment: str,
    nodes: list[tuple[str, str | None, str | None]],
) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        # The checkpoint names an applied catalog version that declares
        # `page`. Without one, the readiness generation probe is refused with
        # GenerationPinInvalid before the source-lineage check runs (#2248).
        catalog_version_id = seed_catalog(conn, subtypes=["page"])
        store = ChronosGraphStore.from_connection(conn)
        for node_id, source_id, policy_fingerprint in nodes:
            store.apply_facts(
                [
                    NodeFact(
                        node_id,
                        "page",
                        {
                            "classification": "internal",
                            "text": node_id,
                        },
                        {"id": node_id},
                        {"v": 1},
                    ),
                ],
                source=source_id,
                policy_fingerprint=policy_fingerprint,
            )
        return store.checkpoint_generation(
            0,
            deployment_name=deployment,
            catalog_version_id=catalog_version_id,
        )


def _publish_node_with_attr_lineage_and_row_source(
    dsn: str,
    deployment: str,
    *,
    node_id: str,
    attr_source_id: str,
    row_source: str,
    policy_fingerprint: str,
) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        # Same as `_publish_nodes`: the checkpoint must name an applied
        # catalog version, or the readiness probe is refused (#2248).
        catalog_version_id = seed_catalog(conn, subtypes=["page"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_id,
                    "page",
                    {
                        "classification": "internal",
                        "text": node_id,
                        LINEAGE_ATTR: normalize_source_lineage(
                            attr_source_id,
                            policy_fingerprint=policy_fingerprint,
                        ),
                    },
                    {"id": node_id},
                    {"v": 1},
                ),
            ],
            source=row_source,
            policy_fingerprint=None,
        )
        return store.checkpoint_generation(
            0,
            deployment_name=deployment,
            catalog_version_id=catalog_version_id,
        )


def _patch_registry(
    monkeypatch: pytest.MonkeyPatch,
    source_ids: list[str],
) -> None:
    from okg.substrate.nomos import doctor

    monkeypatch.setattr(
        doctor,
        "_registry_source_ids",
        lambda _deployment_name: {
            "status": "ok",
            "source_ids": sorted(source_ids),
            "path": "test/source_registry.yaml",
        },
    )
