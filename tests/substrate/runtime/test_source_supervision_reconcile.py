from __future__ import annotations

import subprocess
from pathlib import Path

import psycopg
import pytest

from okg.substrate.deployment_apply import (
    desired_deployment_state,
    plan_deployment_apply,
    record_worker_acknowledgment,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.runtime.source_poll_schedule import source_poll_schedule_id


pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _observe_test_worker_from_desired_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import deployment_apply as deployment_apply_module

    def observe(*, deployment, deployment_dir, dsn):
        desired = deployment_apply_module.desired_deployment_state(
            deployment=deployment,
            deployment_dir=deployment_dir,
            dsn=dsn,
        )
        expected = desired["service_authority"]
        return {
            "authority_source": "running_worker_process",
            "deployment": deployment,
            **{
                field: expected[field]
                for field in (
                    "working_directory",
                    "process_executable",
                    "interpreter",
                    "import_root",
                    "code_revision",
                    "database",
                )
            },
        }

    monkeypatch.setattr(
        deployment_apply_module,
        "_observed_runtime_authority",
        observe,
    )


def _deployment(tmp_path: Path) -> Path:
    root = tmp_path / "source-supervision"
    root.mkdir()
    (root / "deployment.yaml").write_text(
        "name: source-supervision\nmodules: []\n"
        "source_registry: source_registry.yaml\nruntime:\n  enabled: true\n",
        encoding="utf-8",
    )
    (root / "source_registry.yaml").write_text(
        "sources:\n"
        "  docs:\n"
        "    ownership_id: source-supervision.docs\n"
        "    sync:\n"
        "      triggers: [poll]\n"
        "      poll: {interval_seconds: 300}\n"
        "  git:\n"
        "    ownership_id: source-supervision.git\n"
        "    sync:\n"
        "      triggers: [poll]\n"
        "      poll: {interval_seconds: 300}\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(
        ["git", "config", "user.email", "tests@okg.example"],
        cwd=root,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "OKG Tests"],
        cwd=root,
        check=True,
    )
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(
        ["git", "commit", "-qm", "test deployment"],
        cwd=root,
        check=True,
    )
    return root


def test_worker_ack_requires_exact_source_set_and_upserts_once(
    tmp_path: Path, clean_runtime: str,
) -> None:
    test_db_dsn = clean_runtime
    root = _deployment(tmp_path)
    with pytest.raises(SubstrateError, match="source set"):
        record_worker_acknowledgment(
            dsn=test_db_dsn,
            deployment="source-supervision",
            deployment_dir=root,
            active_sources=["docs"],
            active_schedules=[],
        )

    schedules = [source_poll_schedule_id("interval_300s")]
    with pytest.raises(SubstrateError, match="schedule set"):
        record_worker_acknowledgment(
            dsn=test_db_dsn,
            deployment="source-supervision",
            deployment_dir=root,
            active_sources=["docs", "git"],
            active_schedules=[],
        )
    first = record_worker_acknowledgment(
        dsn=test_db_dsn,
        deployment="source-supervision",
        deployment_dir=root,
        active_sources=["git", "docs"],
        active_schedules=schedules,
    )
    second = record_worker_acknowledgment(
        dsn=test_db_dsn,
        deployment="source-supervision",
        deployment_dir=root,
        active_sources=["docs", "git"],
        active_schedules=schedules,
    )
    assert first["desired_state_digest"] == second["desired_state_digest"]
    with psycopg.connect(test_db_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), active_sources, active_schedules "
            "FROM okg.deployment_worker_ack WHERE deployment_name=%s "
            "GROUP BY active_sources, active_schedules",
            ("source-supervision",),
        )
        count, sources, active_schedules = cur.fetchone()
    assert count == 1
    assert sources == ["docs", "git"]
    assert active_schedules == schedules


def test_source_diff_enqueues_only_added_source(
    tmp_path: Path, clean_runtime: str,
) -> None:
    root = _deployment(tmp_path)
    schedule = source_poll_schedule_id("interval_300s")
    record_worker_acknowledgment(
        dsn=clean_runtime,
        deployment="source-supervision",
        deployment_dir=root,
        active_sources=["docs", "git"],
        active_schedules=[schedule],
    )
    registry = root / "source_registry.yaml"
    registry.write_text(
        registry.read_text()
        + "  new:\n"
        + "    ownership_id: source-supervision.new\n"
        + "    sync:\n"
        + "      triggers: [poll]\n"
        + "      poll: {interval_seconds: 300}\n",
        encoding="utf-8",
    )

    plan = plan_deployment_apply(
        deployment="source-supervision",
        deployment_dir=root,
        dsn=clean_runtime,
    )
    source_action = next(
        row for row in plan["actions"] if row["stage"] == "source_supervision"
    )
    assert source_action["needed"] is True
    assert source_action["sources"] == ["new"]
    assert source_action["removed_sources"] == []


def test_source_diff_enqueues_same_name_when_normalized_entry_changes(
    tmp_path: Path, clean_runtime: str,
) -> None:
    root = _deployment(tmp_path)
    schedule = source_poll_schedule_id("interval_300s")
    record_worker_acknowledgment(
        dsn=clean_runtime,
        deployment="source-supervision",
        deployment_dir=root,
        active_sources=["docs", "git"],
        active_schedules=[schedule],
    )
    registry = root / "source_registry.yaml"
    registry.write_text(
        registry.read_text().replace(
            "ownership_id: source-supervision.docs\n",
            "ownership_id: source-supervision.docs-v2\n",
        ),
        encoding="utf-8",
    )

    plan = plan_deployment_apply(
        deployment="source-supervision",
        deployment_dir=root,
        dsn=clean_runtime,
    )
    source_action = next(
        row for row in plan["actions"] if row["stage"] == "source_supervision"
    )
    assert source_action["needed"] is True
    assert source_action["sources"] == ["docs"]
    assert source_action["removed_sources"] == []


def test_desired_source_fingerprint_uses_manifest_sync_root(
    tmp_path: Path, clean_runtime: str,
) -> None:
    root = _deployment(tmp_path)
    sync_root_a = tmp_path / "sync-root-a"
    sync_root_b = tmp_path / "sync-root-b"
    sync_root_a.mkdir()
    sync_root_b.mkdir()
    manifest = root / "deployment.yaml"
    manifest.write_text(
        manifest.read_text() + f"sync_root: {sync_root_a}\n",
        encoding="utf-8",
    )
    registry = root / "source_registry.yaml"
    registry.write_text(
        registry.read_text().replace(
            "    ownership_id: source-supervision.docs\n",
            "    ownership_id: source-supervision.docs\n"
            "    params:\n"
            "      repo_root: ${repo_root}\n",
        ),
        encoding="utf-8",
    )

    first = desired_deployment_state(
        deployment="source-supervision",
        deployment_dir=root,
        dsn=clean_runtime,
    )
    manifest.write_text(
        manifest.read_text().replace(str(sync_root_a), str(sync_root_b)),
        encoding="utf-8",
    )
    second = desired_deployment_state(
        deployment="source-supervision",
        deployment_dir=root,
        dsn=clean_runtime,
    )

    assert first["source_fingerprints"]["docs"] != second["source_fingerprints"]["docs"]
