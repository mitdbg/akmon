"""Disposable-database proof for the source-sync status ownership move.

This is deliberately one real substrate cycle.  The fixture is authored in
the test, the catalog is composed and applied in the test database, and the
two registry layers flow through ``run_ingest`` before the graph and runtime
health surfaces are inspected.  It runs on the disposable database the
substrate fixture resolves from ``OKG_TEST_DSN`` or ``OKG_TEST_DSN_TEMPLATE``
(CI sets only the template), and records that database's name as the fixture
identity.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.ingest import IngestOptions, run_ingest
from okg.substrate.source_sync_producers import (
    record_source_sync_producer_health,
    source_sync_producer_health,
)
from okg.substrate.source_sync_status import (
    SOURCE_SYNC_STATUS_SCHEMA,
    build_source_sync_status_contract,
    source_sync_status_for_source,
)
from okg.substrate.sources.registry import load_source_registry
from tests.pytest.catalog_fixture_builders import commit_fixture_deployment
from tests.substrate.conftest import require_okg_test_dsn


pytestmark = pytest.mark.postgres

_DEPLOYMENT = "slice2-status"


def _source(
    ownership_id: str,
    *,
    predecessor_source: str | None = None,
) -> dict[str, object]:
    entry: dict[str, object] = {
        "ownership_id": ownership_id,
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["id"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
        "admission_policy": {
            "producer_id": ownership_id,
            "producer_kind": "source",
            "trust_label": "fixture-trusted",
            "admission_mode": "fast_track",
            "output_scope_summary": {"summary": "fixture source facts"},
        },
        "sync": {
            "triggers": ["poll", "manual", "reconcile"],
            "default_event_mode": "scope_complete",
            "reconcile_mode": "scope_complete",
            "poll": {"probe": "always_changed", "interval_seconds": 300},
            "scope": {
                "accepts": ["path"],
                "unsupported_fallback": "scope_complete",
            },
        },
        "source_policy": {
            "sensitivity": "local_private",
            "data_classification": "private",
            "credential_ref_policy": "none",
            "live_call_allowed": False,
            "exportability": "no_export",
            "retention": "normal",
            "provenance": "local_filesystem",
            "privacy_obligations": ["audit"],
            "store_raw": True,
        },
    }
    if predecessor_source is not None:
        entry["predecessor_source"] = predecessor_source
    return entry


def _write_layered_fixture_deployment(tmp_path: Path) -> Path:
    """Author the two source layers and their minimal deployment registry."""
    dep = tmp_path / "deployments" / _DEPLOYMENT
    (dep / "schemas").mkdir(parents=True)
    (dep / "schemas" / "fixture.yaml").write_text(
        "id: https://example.invalid/okg/slice2-status\n"
        "name: slice2_status_fixture\n"
        "classes:\n"
        "  Document:\n"
        "    attributes:\n"
        "      text: {}\n"
        "    annotations:\n"
        "      archetype: artifact\n"
        "      transient: 'false'\n",
        encoding="utf-8",
    )

    first_fixture = dep / "first.json"
    second_fixture = dep / "second.json"
    first_fixture.write_text(
        json.dumps(
            {
                "nodes": [
                    {
                        "node_id": "slice2:first",
                        "subtype": "document",
                        "attrs": {"text": "first layer"},
                        "source_record_id": {"path": "first.json"},
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )
    second_fixture.write_text(
        json.dumps(
            {
                "nodes": [
                    {
                        "node_id": "slice2:second",
                        "subtype": "document",
                        "attrs": {"text": "second layer"},
                        "source_record_id": {"path": "second.json"},
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )

    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": _DEPLOYMENT,
                "description": "source-sync status publish parity fixture",
                "schema_dir": "schemas",
                "source_registry": "source_registry.yaml",
                "modules": [],
                "nomos": {
                    "deployment_posture": {
                        "deployment_class": "dogfood_workspace",
                        "read_principals": ["human_operator", "agent"],
                        "write_principals": ["human_operator", "source_adapter"],
                        "mcp_write_default": "review",
                        "export_policy": "internal_only",
                        "secret_handling": "secret_ref_required",
                        "live_call_default": "review",
                        "allowed_source_sensitivities": ["local_private"],
                        "allowed_data_classifications": ["private"],
                        "allowed_exportability": ["no_export"],
                        "privacy_obligations": ["audit"],
                        "readiness_profiles": ["dogfood"],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (dep / "source_registry.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": {
                    "first": {
                        **_source(f"{_DEPLOYMENT}.first"),
                        "module": (
                            "okg.substrate.library.sources.bootstrap_fixture"
                        ),
                        "class": "BootstrapFixtureSource",
                        "params": {"fixture_path": str(first_fixture)},
                    },
                    "second": {
                        **_source(
                            f"{_DEPLOYMENT}.second",
                            predecessor_source="first",
                        ),
                        "module": (
                            "okg.substrate.library.sources.bootstrap_fixture"
                        ),
                        "class": "BootstrapFixtureSource",
                        "params": {"fixture_path": str(second_fixture)},
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    # A publish refuses a deployment outside a clean Git tree (#2554).
    return commit_fixture_deployment(dep)


def _install_minimal_catalog(dsn: str, dep: Path) -> None:
    """Compose and apply only this fixture's catalog and ownership claim."""
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        claim_catalog_ownership,
    )

    composed = compose_catalog(dep)
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=dep,
        command="source-sync status publish parity",
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        claim_catalog_ownership(conn, identity=identity, force=True)
        apply_composed_catalog(conn, composed, identity=identity)


def _assert_slice2_database(dsn: str) -> str:
    """Prove ``dsn`` is the configured disposable test database; return its name.

    The DSN comes from the substrate fixture's own resolution, which accepts
    ``OKG_TEST_DSN`` or ``OKG_TEST_DSN_TEMPLATE``. Issue #2554: this used to
    read ``OKG_TEST_DSN`` alone and pin one hand-prepared database name and
    port, so it failed in every CI lane.
    """
    configured = require_okg_test_dsn()
    assert os.environ.get("OKG_TEST_DSN_TRUST_AS_EPHEMERAL") == "1"
    assert configured == dsn, "clean_runtime must use the configured test DSN"
    info = psycopg.conninfo.conninfo_to_dict(configured)
    assert info.get("host") in {"127.0.0.1", "localhost"}, info
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_user")
        database, user = cur.fetchone()
    assert database == info.get("dbname"), (database, info)
    assert str(database).startswith("okg_test"), database
    assert user == "postgres"
    return str(database)


def _non_bootstrap_generations(dsn: str) -> list[tuple[int, str, str]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT generation_id, deployment_name, status
              FROM okg.graph_generations
             WHERE generation_id > 0
             ORDER BY generation_id
            """
        )
        return cur.fetchall()


@pytest.mark.postgres
def test_source_sync_status_preserves_real_publish_parity(
    tmp_path: Path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    record_property,
) -> None:
    """One disposable two-layer ingest proves status ownership and publish."""
    database = _assert_slice2_database(clean_runtime)
    assert _non_bootstrap_generations(clean_runtime) == []

    dep = _write_layered_fixture_deployment(tmp_path)
    _install_minimal_catalog(clean_runtime, dep)
    registry = load_source_registry(dep / "source_registry.yaml")

    # Record both producer heartbeats through the effectful compatibility
    # facade, then acquire health through its public read facade. The status
    # builder and source slicer below are direct imports from the new pure
    # owner, so neither path is mocked.
    for source_name in ("first", "second"):
        record_source_sync_producer_health(
            clean_runtime,
            deployment=_DEPLOYMENT,
            source_name=source_name,
            producer="poll",
            enqueue_count=0,
            error_count=0,
            event_count=0,
        )
    health = source_sync_producer_health(
        clean_runtime,
        deployment=_DEPLOYMENT,
        registry=registry,
        runtime_source_sync={"poll": {"enabled": True}},
        runtime_enabled=True,
    )
    status = build_source_sync_status_contract(
        health,
        generation={"status": "published"},
    )
    source_slice = source_sync_status_for_source(status, "first")
    assert status["schema"] == SOURCE_SYNC_STATUS_SCHEMA
    assert status["schema"] == "okg.source_sync.status.v1"
    assert source_slice["schema"] == SOURCE_SYNC_STATUS_SCHEMA
    assert source_slice["deployment"] == _DEPLOYMENT
    assert source_slice["runtime_liveness"]["status"] == "healthy"

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment=_DEPLOYMENT,
            deployment_dir=dep,
            owner_id="slice2-status-proof",
            owner_meta={"fixture": database},
        )
    )
    assert outcome.any_failed is False, json.dumps(
        outcome.report, default=str
    )
    assert len(outcome.report["publishes"]) == 1, outcome.report
    publish = outcome.report["publishes"][0]
    assert publish["status"] == "published", publish
    assert publish["nodes_added"] == 2, publish
    assert publish["edges_added"] == 0, publish

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generations = store.list_generations(
            deployment_name=_DEPLOYMENT,
            status=None,
        )
        assert len(generations) == 1, generations
        generation = generations[0]
        assert generation.checkpoint_id == publish["generation_id"]
        assert generation.metadata["status"] == "published"
        checkout = store.checkout_generation(publish["generation_id"])
        live_nodes = int(
            checkout.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
        )
        live_edges = int(
            checkout.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
        )
    finally:
        store.close()
    assert (live_nodes, live_edges) == (2, 0)

    from okg.substrate.publish_proof import dlq_count

    current_dlq, queue_depth = dlq_count(clean_runtime)
    assert current_dlq == 0
    assert all(int(value or 0) == 0 for value in queue_depth.values()), queue_depth

    rc = main(["--json", "doctor", "--dsn", clean_runtime])
    doctor = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert rc == 0, doctor
    assert doctor["ok"] is True, doctor

    proof = {
        "fixture_identity": database,
        "status_schema": status["schema"],
        "source_slice": {
            "source": "first",
            "schema": source_slice["schema"],
            "status": source_slice["runtime_liveness"]["status"],
        },
        "generation_id": publish["generation_id"],
        "generation_status": publish["status"],
        "live_counts": {"nodes": live_nodes, "edges": live_edges},
        "queue_depth": queue_depth,
        "dlq_count": current_dlq,
        "doctor": "clean",
        "result": {"passed": 1, "failed": 0, "skipped": 0},
    }
    record_property("requirements", "req.source-sync-status.publish-parity")
    record_property("fixture_identity", database)
    record_property("status_schema", status["schema"])
    record_property("source_slice", json.dumps(proof["source_slice"], sort_keys=True))
    record_property("generation_id", publish["generation_id"])
    record_property("generation_status", publish["status"])
    record_property("live_counts", json.dumps(proof["live_counts"], sort_keys=True))
    record_property("queue_depth", json.dumps(queue_depth, sort_keys=True))
    record_property("dlq_count", current_dlq)
    record_property("doctor", "clean")
    record_property("evidence_result", json.dumps(proof["result"], sort_keys=True))
    record_property("publish_proof", json.dumps(proof, sort_keys=True, default=str))
