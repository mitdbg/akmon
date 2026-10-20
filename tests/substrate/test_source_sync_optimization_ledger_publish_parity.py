"""Disposable publish proof for the optimization-ledger ownership move.

The fixture runs only on the disposable database the substrate fixture
resolves from ``OKG_TEST_DSN`` or ``OKG_TEST_DSN_TEMPLATE`` (CI sets only the
template), and records that database's name as the fixture identity.  Fixed-time
ledger relation rows become the payload of two fixture nodes, which then flow
through the public ingest service into one real published generation.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

import psycopg
import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.ingest import IngestOptions, run_ingest
from okg.substrate.source_sync_optimization_ledger import (
    MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA,
    MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA,
    build_source_optimization_ledger,
    source_optimization_ledger_relation_rows,
)
from okg.substrate.sources.registry import load_source_registry
from tests.pytest.catalog_fixture_builders import commit_fixture_deployment
from tests.substrate.conftest import require_okg_test_dsn


pytestmark = pytest.mark.postgres

_DEPLOYMENT = "slice3-optimization-ledger"
_GENERATED_AT = "2026-08-16T12:00:00+00:00"


def _representative_inventory() -> dict[str, Any]:
    return {
        "schema": "okg.source_sync.microbench.inventory.v1",
        "deployment": _DEPLOYMENT,
        "generated_at": "2026-08-16T11:59:00+00:00",
        "source_count": 1,
        "enricher_count": 1,
        "live_state": {"status": "loaded"},
        "coverage_counts": {
            "automatic_hot_path": 1,
            "source_backed_enricher": 1,
        },
        "sources": [
            {
                "source_name": "pact",
                "owner": "slice3-optimization-ledger.pact",
                "coverage_class": "automatic_hot_path",
                "readiness_required": True,
                "latest_source_runner_ms": 125.0,
                "latest_stage_total_ms": 180.0,
                "latest_stage_source_runner_ms": 125.0,
                "latest_nodes_appended": 1,
                "latest_edges_appended": 0,
                "latest_dedupe_drops_node": 0,
                "latest_dedupe_drops_edge": 0,
                "scope_support_status": "declared",
                "cursor_support_status": "completed_scope_requires_audit",
                "producer_status": "configured",
                "benchmark_scenarios": ["source-only", "full-pipeline"],
                "blocker": None,
                "source_class": "discovery_crawl",
                "module": "fixture.pact",
                "class_name": "FixturePactSource",
            }
        ],
        "enrichers": [
            {
                "name": "FixtureLedgerEnricher",
                "owner": "_enricher:FixtureLedgerEnricher",
                "coverage_class": "source_backed_enricher",
                "scope_support_status": "needs_dirty_scope_audit",
                "benchmark_scenarios": ["full-pipeline"],
                "blocker": None,
                "module": "fixture.enricher",
                "class_name": "FixtureLedgerEnricher",
            }
        ],
    }


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


def _published_node_payload(
    *,
    ledger: Mapping[str, Any],
    relation_row: Mapping[str, Any],
) -> str:
    return json.dumps(
        {
            "ledger_schema": ledger["schema"],
            "relation_schema": MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA,
            "generated_at": ledger["generated_at"],
            "relation_row": relation_row,
        },
        sort_keys=True,
        default=str,
    )


def _write_layered_fixture_deployment(
    tmp_path: Path,
    *,
    ledger: Mapping[str, Any],
    relation_rows: Sequence[Mapping[str, Any]],
) -> Path:
    """Author two source layers whose node payloads are owner-built rows."""
    assert len(relation_rows) == 2
    dep = tmp_path / "deployments" / _DEPLOYMENT
    (dep / "schemas").mkdir(parents=True)
    (dep / "schemas" / "fixture.yaml").write_text(
        "id: https://example.invalid/okg/slice3-optimization-ledger\n"
        "name: slice3_optimization_ledger_fixture\n"
        "classes:\n"
        "  Document:\n"
        "    attributes:\n"
        "      text: {}\n"
        "    annotations:\n"
        "      archetype: artifact\n"
        "      transient: 'false'\n",
        encoding="utf-8",
    )

    fixture_paths = [dep / "first.json", dep / "second.json"]
    for index, (fixture_path, relation_row) in enumerate(
        zip(fixture_paths, relation_rows, strict=True),
        start=1,
    ):
        fixture_path.write_text(
            json.dumps(
                {
                    "nodes": [
                        {
                            "node_id": f"slice3:ledger:{index}",
                            "subtype": "document",
                            "attrs": {
                                "text": _published_node_payload(
                                    ledger=ledger,
                                    relation_row=relation_row,
                                )
                            },
                            "source_record_id": {"path": fixture_path.name},
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
                "description": "optimization-ledger publish parity fixture",
                "schema_dir": "schemas",
                "source_registry": "source_registry.yaml",
                "modules": [],
                "nomos": {
                    "deployment_posture": {
                        "deployment_class": "dogfood_workspace",
                        "read_principals": ["human_operator", "agent"],
                        "write_principals": [
                            "human_operator",
                            "source_adapter",
                        ],
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
                        "module": ("okg.substrate.library.sources.bootstrap_fixture"),
                        "class": "BootstrapFixtureSource",
                        "params": {"fixture_path": str(fixture_paths[0])},
                    },
                    "second": {
                        **_source(
                            f"{_DEPLOYMENT}.second",
                            predecessor_source="first",
                        ),
                        "module": ("okg.substrate.library.sources.bootstrap_fixture"),
                        "class": "BootstrapFixtureSource",
                        "params": {"fixture_path": str(fixture_paths[1])},
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    # A publish refuses a deployment outside a clean Git tree (#2554).
    return commit_fixture_deployment(dep)


def _install_minimal_catalog(dsn: str, dep: Path) -> None:
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
        command="source-sync optimization-ledger publish parity",
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        claim_catalog_ownership(conn, identity=identity, force=True)
        apply_composed_catalog(conn, composed, identity=identity)


def _assert_slice3_database(dsn: str) -> str:
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


def test_optimization_ledger_preserves_real_publish_parity(
    tmp_path: Path,
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
    record_property,
) -> None:
    """One isolated two-layer ingest publishes owner-built ledger payloads."""
    database = _assert_slice3_database(clean_runtime)
    assert _non_bootstrap_generations(clean_runtime) == []

    ledger = build_source_optimization_ledger(
        _representative_inventory(),
        generated_at=_GENERATED_AT,
    )
    relation_rows = source_optimization_ledger_relation_rows(ledger)
    assert ledger["schema"] == MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA
    assert ledger["schema"] == ("okg.source_sync.microbench.optimization_ledger.v1")
    assert ledger["generated_at"] == _GENERATED_AT
    assert ledger["row_count"] == 2
    assert len(relation_rows) == 2
    rank_order = [
        {
            "row_key": row["row_key"],
            "name": row["name"],
            "priority_rank": row["priority_rank"],
        }
        for row in relation_rows
    ]
    assert [row["priority_rank"] for row in relation_rows] == [1, 2]

    dep = _write_layered_fixture_deployment(
        tmp_path,
        ledger=ledger,
        relation_rows=relation_rows,
    )
    _install_minimal_catalog(clean_runtime, dep)
    load_source_registry(dep / "source_registry.yaml")

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment=_DEPLOYMENT,
            deployment_dir=dep,
            owner_id="slice3-optimization-ledger-proof",
            owner_meta={
                "fixture": database,
                "ledger_schema": ledger["schema"],
                "relation_schema": MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA,
            },
        )
    )
    assert outcome.any_failed is False, json.dumps(outcome.report, default=str)
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
        published_payloads = [
            json.loads(row["text"])
            for row in checkout.query(
                "SELECT attrs->>'text' AS text FROM okg.graph_nodes ORDER BY node_id"
            )
        ]
    finally:
        store.close()
    assert (live_nodes, live_edges) == (2, 0)
    assert [payload["ledger_schema"] for payload in published_payloads] == [
        MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA,
        MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA,
    ]
    assert [payload["relation_schema"] for payload in published_payloads] == [
        MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA,
        MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA,
    ]
    assert [payload["relation_row"] for payload in published_payloads] == (
        json.loads(json.dumps(relation_rows, sort_keys=True, default=str))
    )

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
        "ledger_schema": ledger["schema"],
        "relation_schema": MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA,
        "ledger_row_count": ledger["row_count"],
        "rank_order": rank_order,
        "generation_id": publish["generation_id"],
        "generation_status": publish["status"],
        "live_counts": {"nodes": live_nodes, "edges": live_edges},
        "queue_depth": queue_depth,
        "dlq_count": current_dlq,
        "doctor": "clean",
        "result": {"passed": 1, "failed": 0, "skipped": 0},
    }
    record_property(
        "requirements",
        "req.source-sync-optimization-ledger.publish-parity",
    )
    record_property("fixture_identity", database)
    record_property("ledger_schema", ledger["schema"])
    record_property(
        "relation_schema",
        MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA,
    )
    record_property("ledger_row_count", ledger["row_count"])
    record_property("rank_order", json.dumps(rank_order, sort_keys=True))
    record_property("generation_id", publish["generation_id"])
    record_property("generation_status", publish["status"])
    record_property(
        "live_counts",
        json.dumps(proof["live_counts"], sort_keys=True),
    )
    record_property("queue_depth", json.dumps(queue_depth, sort_keys=True))
    record_property("dlq_count", current_dlq)
    record_property("doctor", "clean")
    record_property(
        "evidence_result",
        json.dumps(proof["result"], sort_keys=True),
    )
    record_property(
        "publish_proof",
        json.dumps(proof, sort_keys=True, default=str),
    )
