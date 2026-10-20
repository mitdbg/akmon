"""Chronos generation resolution consistency across operator surfaces."""

from __future__ import annotations

import argparse
import json
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.cli.deployment import _ready_generation_status
from okg.substrate.cli.generation import _cmd_generation
from okg.substrate.cli.main import _collect_status
from okg.substrate.generation_resolver import (
    chronos_generation_inventory,
    resolve_okg_generation,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp.session import open_session
from okg.substrate.qa.metrics import snapshot


pytestmark = pytest.mark.postgres


def _seed_chronos_generation(
    dsn: str,
    *,
    deployment_name: str | None = None,
    count_stamp_recovery: dict[str, object] | None = None,
) -> tuple[str, str, int]:
    deployment_name = deployment_name or f"test-chronos-resolver-{uuid4().hex[:8]}"
    target_generation_id = 991_000 + int(uuid4().hex[:5], 16)
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:chronos:resolver:one",
                    "document",
                    {"text": "one"},
                    {"id": "one"},
                    {"rev": 1},
                ),
                NodeFact(
                    "doc:chronos:resolver:two",
                    "document",
                    {"text": "two"},
                    {"id": "two"},
                    {"rev": 1},
                ),
                EdgeFact(
                    "doc:chronos:resolver:one",
                    "doc:chronos:resolver:two",
                    "references",
                    source_record_id={"id": "one-two"},
                    source_revision={"rev": 1},
                ),
            ],
            source="test.chronos.resolver",
            run_id=f"run:test-chronos-resolver:{deployment_name}",
        )
        checkpoint_id = store.checkpoint_generation(
            None,
            deployment_name=deployment_name,
            catalog_version_id=17,
            status="published",
            metadata={
                "target_generation_id": target_generation_id,
                "publisher": "test_chronos_generation_resolution",
                "predecessor_generation_id": "okg:resolver:prior",
                "node_count": 2,
                "edge_count": 1,
                "invariant_summary": {
                    "substrate_integrity": {"checks": {}},
                    "total_duration_ms": 0,
                    "total_budget_exceeded": False,
                },
                "excluded_sources": [],
                "publish_wallclock_seconds": 1.5,
                "count_stamp_recovery": count_stamp_recovery,
            },
        )
    return deployment_name, checkpoint_id, target_generation_id


def test_generation_describe_accepts_chronos_id(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    deployment_name, checkpoint_id, target_generation_id = _seed_chronos_generation(
        clean_runtime
    )

    rc = _cmd_generation(
        argparse.Namespace(
            subcmd="describe",
            generation_id=checkpoint_id,
            deployment=None,
            dsn=clean_runtime,
        ),
        as_json=True,
    )

    assert rc == 0
    described = json.loads(capsys.readouterr().out)
    assert described["generation_id"] == checkpoint_id
    assert described["target_generation_id"] == target_generation_id
    assert described["deployment_name"] == deployment_name
    assert described["catalog_version_id"] == 17
    assert described["predecessor_generation_id"] == "okg:resolver:prior"
    assert described["nodes_live_at_gen"] == 2
    assert described["edges_live_at_gen"] == 1


def test_generation_describe_surfaces_count_stamp_recovery_receipt(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    receipt: dict[str, object] = {
        "schema": "okg.count_stamp_recovery.v1",
        "authorization_digest": "sha256:test-authorization",
        "classification_binding_digest": "sha256:test-classification",
    }
    deployment_name, checkpoint_id, _target_generation_id = (
        _seed_chronos_generation(
            clean_runtime,
            count_stamp_recovery=receipt,
        )
    )

    rc = _cmd_generation(
        argparse.Namespace(
            subcmd="describe",
            generation_id=checkpoint_id,
            deployment=deployment_name,
            dsn=clean_runtime,
        ),
        as_json=True,
    )

    assert rc == 0
    described = json.loads(capsys.readouterr().out)
    assert described["count_stamp_recovery"] == receipt


def test_current_generation_resolver_is_shared(clean_runtime: str) -> None:
    deployment_name, checkpoint_id, target_generation_id = _seed_chronos_generation(
        clean_runtime
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        resolved = resolve_okg_generation(
            conn,
            deployment_name=deployment_name,
            include_counts=True,
        )
        ready = _ready_generation_status(conn, deployment_name)
        session = open_session(conn, deployment_name=deployment_name)

    assert resolved is not None
    assert resolved.generation_id == checkpoint_id
    assert resolved.target_generation_id == target_generation_id
    assert resolved.catalog_version_id == 17
    assert resolved.node_count == 2
    assert resolved.edge_count == 1

    status = _collect_status(clean_runtime, deployment_name)
    assert status["publishing"]["latest_published_generation"] == checkpoint_id
    assert (
        status["publishing"]["latest_published_target_generation"]
        == target_generation_id
    )

    assert ready["generation_id"] == checkpoint_id
    assert ready["target_generation_id"] == target_generation_id
    assert ready["catalog_version_id"] == 17
    assert ready["node_count"] == 2
    assert ready["edge_count"] == 1

    assert session.generation_id == checkpoint_id
    assert session.compat_generation_id == target_generation_id
    assert session.catalog_version_id == 17

    qa = snapshot(
        dsn=clean_runtime,
        deployment=deployment_name,
        skip_doctor=True,
    )
    assert qa.generation_id == checkpoint_id
    assert qa.nodes_live_delta == 2
    assert qa.edges_live_delta == 1


def test_chronos_generation_inventory_uses_checkpoint_authority(
    clean_runtime: str,
) -> None:
    first_deployment, first_checkpoint_id, _target_generation_id = (
        _seed_chronos_generation(clean_runtime)
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert chronos_generation_inventory(
            conn,
            deployment_name=first_deployment,
            checkpoint_id=first_checkpoint_id,
        ) == {
            "total": 1,
            "matching": 1,
            "foreign": 0,
            "root_lineage": True,
        }

    same_deployment, second_checkpoint_id, _target_generation_id = (
        _seed_chronos_generation(
            clean_runtime,
            deployment_name=first_deployment,
        )
    )
    assert same_deployment == first_deployment
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert chronos_generation_inventory(
            conn,
            deployment_name=first_deployment,
            checkpoint_id=first_checkpoint_id,
        ) == {
            "total": 2,
            "matching": 2,
            "foreign": 0,
            "root_lineage": True,
        }
        assert chronos_generation_inventory(
            conn,
            deployment_name=first_deployment,
            checkpoint_id=second_checkpoint_id,
        ) == {
            "total": 2,
            "matching": 2,
            "foreign": 0,
            "root_lineage": False,
        }

    foreign_deployment, _checkpoint_id, _target_generation_id = (
        _seed_chronos_generation(clean_runtime)
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert chronos_generation_inventory(
            conn,
            deployment_name=foreign_deployment,
            checkpoint_id=_checkpoint_id,
        ) == {
            "total": 3,
            "matching": 1,
            "foreign": 2,
            "root_lineage": False,
        }
