from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.cli.deployment import _ready_generation_status
from okg.substrate.cli.main import main
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.substrate.cli.test_deployment_ready_evidence import _write_ready_fixture
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_ready_generation_status_counts_the_pinned_checkout(
    clean_runtime: str,
) -> None:
    node_id = "case:readiness-count-integrity"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["case"], narrowings=[])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(node_id, "case", {}, {"id": node_id}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        generation = _ready_generation_status(conn, "default")

    assert generation["generation_id"] == published["generation_id"]
    assert generation["count_integrity"] == {
        "schema": "okg.generation_count_integrity.v1",
        "ok": True,
        "status": "clean",
        "generation_id": published["generation_id"],
        "checkpoint": {"node_count": 1, "edge_count": 0},
        "pinned": {"node_count": 1, "edge_count": 0},
        "mismatches": [],
    }


def test_ready_query_failure_is_structured_count_integrity_blocker(
    clean_runtime: str,
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node_id = "case:readiness-count-query-failure"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["case"], narrowings=[])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(node_id, "case", {}, {"id": node_id}, {"v": 1})],
            source="ready-demo",
            strict_endpoints=True,
        )
    published = Publisher(clean_runtime, "ready-demo", catalog_identity=bind_test_catalog_writer(clean_runtime, "ready-demo")).cycle()
    assert published["status"] == "published", published

    real_from_connection = ChronosGraphStore.from_connection

    class CheckoutFailureStore:
        def __init__(self, store):
            self._store = store

        def __getattr__(self, name):
            return getattr(self._store, name)

        def checkout_generation(self, *_args, **_kwargs):
            raise RuntimeError("pinned checkout unavailable")

    def store_with_failed_checkout(conn, *args, **kwargs):
        return CheckoutFailureStore(real_from_connection(conn, *args, **kwargs))

    monkeypatch.setattr(
        ChronosGraphStore,
        "from_connection",
        store_with_failed_checkout,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        generation = _ready_generation_status(conn, "ready-demo")
    integrity = generation["count_integrity"]
    assert integrity["status"] == "unavailable"
    assert integrity["generation_id"] == published["generation_id"]
    assert integrity["checkpoint"] == {"node_count": 1, "edge_count": 0}
    assert integrity["pinned"] is None
    assert integrity["mismatches"] == ["pinned_count_query_failed"]

    deployment_dir = _write_ready_fixture(tmp_path)
    monkeypatch.setenv("READY_DEMO_DSN", clean_runtime)
    rc = main([
        "deployment",
        "ready",
        str(deployment_dir),
        "--skip-doctor",
        "--json",
    ])
    payload = json.loads(capsys.readouterr().out)
    blockers = {item["id"]: item for item in payload["blockers"]}
    assert rc == 1
    assert "generation.count_integrity" in blockers
    assert blockers["generation.count_integrity"]["details"] == integrity


def test_mcp_verdict_identity_state_digest_changes_when_a_binding_is_disabled(
    clean_runtime: str,
) -> None:
    """Issue #2547: the console keeps a passing MCP probe verdict keyed on a
    digest of the identity rows. The digest is SQL; this runs it on a migrated
    database and shows a status flip on one binding changes it."""
    from okg.substrate.cli.deployment_readiness import McpSampleVerdicts

    deployment = "default"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        empty = McpSampleVerdicts.identity_state(conn, deployment)
        assert [part.split(":")[0] for part in empty] == ["0", "0", "0"]
        conn.execute(
            """
            INSERT INTO okg.nomos_subject_bindings (
              deployment_name, binding_id, provider_id, issuer, claim_path,
              claim_value, subject, scope
            )
            VALUES (%s, 'binding:2547', 'provider:2547', 'https://issuer',
                    'sub', 'alice', 'subject:alice', %s)
            """,
            (deployment, f"deployment:{deployment}"),
        )
        active = McpSampleVerdicts.identity_state(conn, deployment)
        conn.execute(
            "UPDATE okg.nomos_subject_bindings SET status = 'disabled' "
            "WHERE deployment_name = %s AND binding_id = 'binding:2547'",
            (deployment,),
        )
        disabled = McpSampleVerdicts.identity_state(conn, deployment)
        # A binding in another scope is not this probe's input.
        conn.execute(
            """
            INSERT INTO okg.nomos_subject_bindings (
              deployment_name, binding_id, provider_id, issuer, claim_path,
              claim_value, subject, scope
            )
            VALUES (%s, 'binding:other-scope', 'provider:2547',
                    'https://issuer', 'sub', 'bob', 'subject:bob',
                    'deployment:elsewhere')
            """,
            (deployment,),
        )
        unrelated = McpSampleVerdicts.identity_state(conn, deployment)

    assert active[1].startswith("1:") and active != empty
    assert disabled[1].startswith("1:") and disabled != active
    assert active[0] == disabled[0] and active[2] == disabled[2]
    assert unrelated == disabled


# Issue #2259: the console Sources reads (source-diagnostics, producers) build
# the console readiness on every uncached read, and its MCP graph-read probe
# filters a 100-row sample through Nomos. On okg-workspace that sample cost
# 300 policy evaluations (100 rows x 3) and about 1.5 s. Since #2881 the
# console pays them once per generation, policy and identity state, not once
# per read. This cap fails a regression back to paying them on every read,
# and a per-row cost that grows past three evaluations.
_SAMPLE_ROWS = 100  # the probe's `LIMIT 100` in `_ready_mcp_graph_status`
_EVALUATIONS_PER_SAMPLE_ROW = 3
MAX_POLICY_EVALUATIONS_PER_READINESS_BUILD = (
    _SAMPLE_ROWS * _EVALUATIONS_PER_SAMPLE_ROW
)
_CONSOLE_READS = 5


def _publish_sample_sized_graph(dsn: str) -> None:
    """More nodes than the probe samples, all from one allowed source."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    f"node:page-{i:03d}",
                    "page",
                    {"classification": "internal", "text": f"page {i}"},
                    {"id": f"node:page-{i:03d}"},
                    {"v": 1},
                )
                for i in range(_SAMPLE_ROWS + 20)
            ],
            source="public_docs",
        )
    result = Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")
    ).cycle()
    assert result["status"] == "published"


def test_console_readiness_reads_cap_policy_evaluations(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Five console readiness reads of one generation pay at most one sample's
    policy evaluations between them, counted at the Nomos evaluator itself."""
    from okg.substrate.cli import deployment_readiness
    from okg.substrate.cli.deployment_readiness import McpSampleVerdicts
    from okg.substrate.console_views import console_readiness
    from okg.substrate.mcp.session import open_session
    from okg.substrate.nomos import runtime_enforcement
    from tests.substrate.mcp.test_nomos_source_lineage_mcp import (
        _activate_source_policy,
    )

    _publish_sample_sized_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    # The test cluster's `okg_mcp` role is NOLOGIN, which makes the probe stop
    # at its login check before any sample. That check evaluates no policy;
    # skipping it here lets the probe reach the Nomos-filtered sample without
    # changing a cluster-wide role other tests share.
    monkeypatch.setattr(
        deployment_readiness, "_ready_mcp_principal_login_status", lambda conn: None
    )
    # A fresh verdict store, as a newly started console process holds.
    monkeypatch.setattr(
        console_readiness, "_MCP_SAMPLE_VERDICTS", McpSampleVerdicts()
    )

    evaluations = 0
    real_evaluate = runtime_enforcement.evaluate

    def _counted(*args: Any, **kwargs: Any) -> Any:
        nonlocal evaluations
        evaluations += 1
        return real_evaluate(*args, **kwargs)

    monkeypatch.setattr(runtime_enforcement, "evaluate", _counted)

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")

    per_read: list[int] = []
    reuse: list[Any] = []
    for _ in range(_CONSOLE_READS):
        before = evaluations
        payload = console_readiness.compose_console_readiness(
            deployment_name="default",
            deployment_dir=tmp_path,
            dsn=clean_runtime,
            session=session,
        )
        per_read.append(evaluations - before)
        mcp = payload["read_authority"]["mcp"]
        assert mcp["status"] == "available", mcp
        reuse.append(mcp.get("verdict_reuse", {}).get("status"))

    # The first read measured the full sample: the evaluator really ran.
    assert per_read[0] >= _SAMPLE_ROWS, per_read
    assert sum(per_read) <= MAX_POLICY_EVALUATIONS_PER_READINESS_BUILD, (
        f"{_CONSOLE_READS} console readiness reads of one generation ran "
        f"{sum(per_read)} Nomos policy evaluations {per_read}; the cap is "
        f"{MAX_POLICY_EVALUATIONS_PER_READINESS_BUILD} (one {_SAMPLE_ROWS}-row "
        "sample), so the reads re-ran the sample or its per-row cost grew"
    )
    assert reuse == ["computed"] + ["reused"] * (_CONSOLE_READS - 1), reuse
