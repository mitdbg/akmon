"""okg up guards that outlived the corrupt-predecessor recovery.

The okg-ci incident test that lived here drove the recovery classification
and the repair verb, both retired with the attribute copies (PACT
lane-s-storage-drop-copies). The catalog-claim guard and the CI final-proof
checks below do not depend on them.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from okg.substrate.deployments import resolve_deployment
from okg.substrate.deployment_upgrade import (
    CommandResult,
    UpgradeOptions,
    UpgradeContext,
    _collect_ci_deployment_final_proof,
)
from tests.substrate.deployment_catalog_claim import (
    point_deployment_at_test_database,
)

pytestmark = pytest.mark.postgres


def _ok(payload) -> CommandResult:
    return CommandResult(0, json.dumps(payload, default=str), "")


@pytest.mark.parametrize(
    ("target_db", "refusal"),
    [
        ("okg_ci", "not a disposable test database"),
        ("okg_test_someone_else", "the configured test database is"),
    ],
)
def test_catalog_claim_refuses_a_dsn_outside_the_test_database(
    monkeypatch: pytest.MonkeyPatch,
    target_db: str,
    refusal: str,
) -> None:
    """The okg-ci catalog claim above runs real CLI commands that resolve the
    DSN from okg-ci's own manifest. The guard refuses, before any command
    runs or any variable changes, a target that is not the configured test
    database, and points okg-ci at the test database when it is."""
    configured = "postgres://127.0.0.1:1/okg_test_guard"
    monkeypatch.setenv("OKG_TEST_DSN", configured)
    monkeypatch.setenv("OKG_CI_DSN", "postgres://127.0.0.1:1/okg_ci")
    deployment_dir = Path(resolve_deployment("okg-ci").deployment_dir)

    with pytest.raises(pytest.fail.Exception, match=refusal):
        point_deployment_at_test_database(
            deployment_dir, f"postgres://127.0.0.1:1/{target_db}", monkeypatch,
        )
    assert os.environ["OKG_CI_DSN"].endswith("/okg_ci")

    assert point_deployment_at_test_database(
        deployment_dir, configured, monkeypatch,
    ) == "OKG_CI_DSN"
    assert os.environ["OKG_CI_DSN"] == configured


@pytest.mark.parametrize(
    "mutation",
    ["missing_doctor", "default_dlq", "cross_mcp_generation", "missing_release"],
)
def test_ci_final_proof_rejects_missing_defaulted_or_cross_bound_evidence(
    tmp_path: Path,
    mutation: str,
) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({
        "phase_results": [
            {"phase": "catalog_apply", "ok": True},
            {"phase": "nomos_baseline", "ok": True},
        ]
    }))
    ctx = UpgradeContext(
        options=UpgradeOptions(deployment="okg-ci", readiness_profile="ci"),
        dsn="postgres://unused",
        resolution={"target": {}},
        env={},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=lambda argv, env: _ok(
            {} if mutation == "default_dlq"
            else {"current_count": 0, "historical_count": 1}
        ),
        data={"smoke_publish": {"publishes": [{"generation_id": "gen-2"}]}},
    )
    release = {"ok": True, "release_revision": "rev-1"}
    readiness = {
        "database": {"ok": True},
        "generation": {
            "status": "published",
            "generation_id": "gen-2",
            "count_integrity": {
                "ok": True,
                "checkpoint": {"node_count": 2, "edge_count": 1},
                "pinned": {"node_count": 2, "edge_count": 1},
            },
        },
        "queues": {"active_queue_depth": 0, "current_dlq": 0},
        "source_freshness": {"ok": True, "missing": [], "stale": []},
        "source_sync_liveness": {
            "ok": True, "missing_producers": [], "stale_producers": [],
        },
        "source_sync_status": {"convergence": {"ok": True, "state": "ready"}},
        "nomos_audit": {"ok": True},
        "nomos_runtime": {"ok": True},
        "doctor": {"ok": True},
        "mcp": {"ok": True, "generation_id": "gen-2"},
    }
    if mutation == "missing_doctor":
        readiness.pop("doctor")
    elif mutation == "default_dlq":
        pass
    elif mutation == "cross_mcp_generation":
        readiness["mcp"]["generation_id"] = "gen-1"
    else:
        release.pop("release_revision")
    proof = _collect_ci_deployment_final_proof(ctx, release=release, readiness=readiness)
    assert proof["ok"] is False
    assert not all(proof["checks"].values())
