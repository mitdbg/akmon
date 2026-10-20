from __future__ import annotations

import json
import math
from pathlib import Path
from types import SimpleNamespace
import uuid

import psycopg
import pytest
import yaml

from okg.substrate import deployment_upgrade as du
from okg.substrate.db.cold_history import (
    INTERVAL_SOURCE_READ_INDEX_SPECS,
    cold_history_report,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.runtime import host_headroom


pytestmark = pytest.mark.postgres


def _database_names(dsn: str) -> list[str]:
    with psycopg.connect(dsn) as conn:
        return [
            str(row[0])
            for row in conn.execute(
                "SELECT datname FROM pg_database ORDER BY datname"
            ).fetchall()
        ]


def test_real_postgres_restore_headroom_refusal_reports_cold_history_bytes(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    record_property,
) -> None:
    deployment = f"cold-history-upgrade-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_id=f"document:{deployment}",
                    subtype="document",
                    attrs={"title": "real headroom proof"},
                    source_record_id={"id": deployment},
                    source_revision={"revision": 1},
                )
            ],
            source="issue-1129-runtime-proof",
            strict_endpoints=True,
        )
        for ordinal in range(102):
            store.checkpoint_generation(
                f"{deployment}-{ordinal}",
                deployment_name=deployment,
                status="published",
            )
        database_size = int(
            conn.execute(
                "SELECT pg_database_size(current_database())"
            ).fetchone()[0]
        )

    before = cold_history_report(clean_runtime)
    before_candidates = [
        row
        for row in before["indexes"]
        if row["rotatable"] and row["full_index"] is not None
    ]
    # Every rotatable spec is built on a fresh runtime; count from the specs so
    # removing or adding one does not break this proof.
    expected_rotatable = sum(
        1
        for specs in INTERVAL_SOURCE_READ_INDEX_SPECS.values()
        for spec in specs
        if spec.rotate
    )
    assert expected_rotatable > 0
    assert len(before_candidates) == expected_rotatable
    gross_bytes = sum(
        int(row["full_index"]["bytes"]) for row in before_candidates
    )
    assert gross_bytes > 0

    restore_root = tmp_path / "restore-volume"
    restore_root.mkdir()
    total_bytes, available_bytes = host_headroom.probe_filesystem(restore_root)
    blocker_pct = 6.0
    blocker_floor_bytes = math.ceil(total_bytes * blocker_pct / 100.0)
    minimum_needed = max(
        math.ceil(database_size * 1.25),
        available_bytes - blocker_floor_bytes + 1,
    )
    restore_peak_factor = (minimum_needed + database_size) / database_size

    deployments_root = tmp_path / "deployments"
    deployment_dir = deployments_root / deployment
    schema_dir = deployment_dir / "schemas"
    schema_dir.mkdir(parents=True)
    (deployment_dir / "source_registry.yaml").write_text("sources: []\n")
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": deployment,
                "schema_dir": "schemas",
                "source_registry": "source_registry.yaml",
                "postgres": {"dsn": clean_runtime},
                "host_storage": {
                    "warn_available_pct": 15,
                    "blocker_available_pct": blocker_pct,
                },
                "deployment_up": {
                    "backup": {
                        "verification_policy": "current_full_restore",
                        "restore_volume": {
                            "kind": "host_path",
                            "path": str(restore_root),
                        },
                        "restore_peak_factor": restore_peak_factor,
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    restore_database_token = uuid.uuid4().hex[:16]
    expected_restore_database = f"okg_upgrade_verify_{restore_database_token}"
    monkeypatch.setattr(
        du,
        "uuid4",
        lambda: SimpleNamespace(hex=restore_database_token),
    )
    restore_commands: list[list[str]] = []

    def reject_restore_command(argv: list[str], env: dict[str, str]):
        del env
        restore_commands.append(argv)
        pytest.fail(f"restore command unexpectedly started: {argv}")

    ctx = du.UpgradeContext(
        options=du.UpgradeOptions(deployment=deployment),
        dsn=clean_runtime,
        resolution={"target": {"database": "test", "identity": "isolated"}},
        env={},
        state_file=tmp_path / "upgrade-state.json",
        backup_dir=tmp_path / "backups",
        command_runner=reject_restore_command,
    )
    policy = du._backup_policy_config(ctx)
    assert policy["restore_peak_factor"] == restore_peak_factor
    databases_before = _database_names(clean_runtime)
    assert expected_restore_database not in databases_before

    result = du._run_current_full_restore_verification(
        ctx,
        tmp_path / "backup-that-must-not-be-read.dump",
        snapshot={},
        policy=policy,
        source_database_size=database_size,
        prior_restored_size=None,
    )

    databases_after = _database_names(clean_runtime)
    after = cold_history_report(clean_runtime)
    after_candidates = [
        row
        for row in after["indexes"]
        if row["rotatable"] and row["full_index"] is not None
    ]
    headroom = result["restore_headroom"]
    advisory = headroom["cold_history_reclaim"]
    assert result["ok"] is False
    assert result["reason"] == (
        "insufficient or unprobeable restore-volume headroom"
    )
    assert headroom["code"] == "host_headroom_blocker"
    assert headroom["source_database_size_bytes"] == database_size
    assert headroom["total_bytes"] == total_bytes
    assert headroom["available_bytes"] > 0
    assert headroom["projected_available_bytes"] == (
        headroom["available_bytes"] - headroom["estimated_peak_bytes"]
    )
    assert headroom["shortfall_bytes"] == (
        headroom["blocker_floor_bytes"]
        - headroom["projected_available_bytes"]
    )
    assert headroom["shortfall_bytes"] > 0
    assert advisory["status"] == "candidates"
    assert advisory["read_only"] is True
    assert advisory["floor"] == before["floor"]["floor"]
    assert advisory["headroom_generations"] == 100
    assert advisory["rotatable_index_count"] == expected_rotatable
    assert advisory["rotatable_full_index_bytes"] == gross_bytes
    assert len(advisory["rotatable_indexes"]) == expected_rotatable
    assert advisory["inspect_command"] == (
        f"uv run okg db cold-history --deployment {deployment} --json"
    )
    assert advisory["activate_command"] == (
        "uv run okg db cold-history --deployment "
        f"{deployment} --activate --confirm --apply --json"
    )
    assert advisory["rotation_performed"] is False
    assert f"{gross_bytes} gross bytes across {expected_rotatable} rotatable" in headroom["message"]
    assert advisory["inspect_command"] in headroom["message"]
    assert advisory["activate_command"] in headroom["message"]
    assert "drops the listed full-history B-tree predecessors" in headroom["message"]
    assert expected_restore_database not in databases_after
    assert restore_commands == []
    assert [row["full_index"]["index_name"] for row in after_candidates] == [
        row["full_index"]["index_name"] for row in before_candidates
    ]
    assert after["manifest"] == before["manifest"]
    assert [row["index_name"] for row in after["support_indexes"]] == [
        row["index_name"] for row in before["support_indexes"]
    ]

    evidence = {
        "deployment": deployment,
        "database_size_bytes": database_size,
        "restore_volume_total_bytes": headroom["total_bytes"],
        "restore_volume_available_bytes": headroom["available_bytes"],
        "restore_volume_blocker_floor_bytes": headroom["blocker_floor_bytes"],
        "restore_peak_factor": restore_peak_factor,
        "restore_needed_bytes": headroom["needed_bytes"],
        "estimated_peak_bytes": headroom["estimated_peak_bytes"],
        "projected_available_bytes": headroom["projected_available_bytes"],
        "shortfall_bytes": headroom["shortfall_bytes"],
        "refusal_message": headroom["message"],
        "restore_refused": headroom["refuse"],
        "advisory_status": advisory["status"],
        "read_only": advisory["read_only"],
        "cold_history_floor": advisory["floor"],
        "headroom_generations": advisory["headroom_generations"],
        "rotatable_index_count": advisory["rotatable_index_count"],
        "rotatable_full_index_bytes": advisory[
            "rotatable_full_index_bytes"
        ],
        "rotatable_indexes": advisory["rotatable_indexes"],
        "inspect_command": advisory["inspect_command"],
        "activate_command": advisory["activate_command"],
        "estimate_kind": advisory["estimate_kind"],
        "warning": advisory["warning"],
        "rotation_performed": advisory["rotation_performed"],
        "full_predecessor_names_unchanged": (
            [row["full_index"]["index_name"] for row in after_candidates]
            == [row["full_index"]["index_name"] for row in before_candidates]
        ),
        "cold_history_manifest_unchanged": (
            after["manifest"] == before["manifest"]
        ),
        "support_index_names_unchanged": (
            [row["index_name"] for row in after["support_indexes"]]
            == [row["index_name"] for row in before["support_indexes"]]
        ),
        "restore_database_absent_before": (
            expected_restore_database not in databases_before
        ),
        "restore_database_absent_after": (
            expected_restore_database not in databases_after
        ),
        "restore_command_count": len(restore_commands),
    }
    record_property("requirements", "req.upgrade-headroom.cold-history-remediation")
    record_property("runtime_evidence", json.dumps(evidence, sort_keys=True))
    print(json.dumps(evidence, sort_keys=True))
