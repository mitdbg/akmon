"""End-to-end deployment-lifecycle gate against an ephemeral Postgres.

`deployment-lifecycle-e2e-lane` PACT (child of deployment-lifecycle-e2e-ci).
The ~100 hermetic tests in tests/substrate/test_deployment_upgrade.py pass a
mock command_runner and stub the backup I/O boundary; NO CI lane drives the
real orchestrated lifecycle against a real database. This gate stages a small
deployment-owned fixture and, against a real ephemeral Postgres:

  * provisions it and asserts a real published generation with positive live
    node/edge deltas visible to the read side (the deployment pipeline end to
    end: migrate -> catalog -> nomos -> ingest -> publish), and
  * drives the real ``okg deployment up`` CLI in mutation-free plan mode and
    asserts it executes the full canonical ``PHASES`` sequence in order — the
    real-runner counterpart to the mock-runner hermetic phase-order pin.

Boundary (recorded honestly): the real backup-phase EXECUTION is not driven end
to end here. Its admission is an anti-tamper immutable planner that only trusts
a pristine, committed okg checkout at HEAD driving a Git-pinned deployment
(deployment_upgrade._planner_integrity_inventory + _git_target_revision), and a
real dump targets the whole shared test database. That path is covered by
tests/substrate/test_deployment_upgrade.py::test_risk_admission_uses_real_target_and_postgres_without_dump
(real dump + disposable restore on the okg-workspace-isolated runtime) and the
hermetic classifier suite; ``--plan`` exercises the orchestration wiring without
the pristine-checkout coupling or a shared-database dump.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.deployment_upgrade import PHASES

from .._deployment_repo import commit_deployment_tree
from .test_publish_gate_multi_source import (
    _claim_and_provision,
    _json_out,
    _live_rows,
    _write_sample_sources,
)

pytestmark = pytest.mark.postgres


def _deployment_up_block() -> dict:
    """A minimal, valid ``deployment_up.backup`` block so ``okg deployment up``
    resolves its backup policy. ``current_full_restore`` needs no pre-minted DR
    receipt; ``up --plan`` validates and previews the phase without dumping."""
    return {
        "backup": {
            "verification_policy": "current_full_restore",
            "restore_peak_factor": 1.25,
            "restore_volume": {"kind": "docker_data_root"},
            "budgets_seconds": {
                "routine": {"backup": 60, "attribute_read_model": 30, "total": 600},
                "forced": {"dump": 1012, "backup": 4200, "total": 7200},
            },
        }
    }


def _stage_lifecycle_fixture(tmp_path: Path, capsys, *, name: str) -> Path:
    """Author a small bootstrap fixture deployment (records + docs) and add an
    ``up``-drivable lifecycle config (dev channel, host storage thresholds,
    backup policy, and the records source as the smoke canary)."""
    import shutil

    dep, brief = _write_sample_sources(tmp_path)
    for step in (
        ["plan", "--source", "records=fixtures/records.json",
         "--source", "docs=fixtures/corpus", "--brief", str(brief),
         "--name", name],
        ["inspect-source"],
        ["draft"],
    ):
        rc = main([
            "--json", "deployment", "bootstrap", step[0],
            "--deployment", str(dep), *step[1:], "--apply",
        ])
        capsys.readouterr()
        assert rc == 0, step

    staged = dep / ".okg_bootstrap/staged"
    deploy_root = tmp_path / "deployments"
    target = deploy_root / name
    shutil.copytree(staged, target)

    reg = yaml.safe_load((target / "source_registry.bootstrap.yaml").read_text())
    for sid, entry in reg["sources"].items():
        entry["params"] = {"fixture_path": str(target / "fixtures" / f"{sid}.json")}
    (target / "source_registry.yaml").write_text(yaml.safe_dump(reg))

    manifest = yaml.safe_load((target / "deployment.yaml").read_text())
    manifest["release"] = {"channel": "dev"}
    manifest["host_storage"] = {"warn_available_pct": 15, "blocker_available_pct": 6}
    manifest["deployment_up"] = _deployment_up_block()
    # Publish the first bootstrap record source as the smoke canary. (No nomos
    # deployment_posture: the bootstrap fixture sources carry no vetted privacy
    # policy, so a dogfood_workspace posture would fail the source Nomos audit at
    # provision; the routine no-dump backup arm, which needs that posture and a
    # trusted smoke adapter, is out of scope for this self-contained fixture.)
    runtime = manifest.setdefault("runtime", {})
    runtime["smoke_sources"] = [next(iter(reg["sources"]))]
    (target / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    # The publisher refuses a deployment outside a committed git tree
    # (repository provenance, #1880), so commit before claim/provision.
    commit_deployment_tree(target, "lifecycle e2e fixture deployment")
    return target


def _fixture_name(tmp_path: Path) -> str:
    return f"e2elc{tmp_path.name.lower().replace('-', '')[-8:]}"


@pytest.mark.postgres
def test_lifecycle_provisions_publishes_and_up_drives_phase_sequence(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """The deployment lifecycle end to end against a real ephemeral Postgres:
    provisioning a small fixture publishes a real generation (positive live-row
    deltas, read-side-visible, clean doctor), and the real ``okg deployment up``
    CLI then drives its full canonical ``PHASES`` sequence."""
    name = _fixture_name(tmp_path)
    target = _stage_lifecycle_fixture(tmp_path, capsys, name=name)

    # (1) Pipeline end to end: provisioning publishes a real generation.
    out = _claim_and_provision(
        target, clean_runtime, capsys, name=name, monkeypatch=monkeypatch,
    )
    assert out["ok"] is True, out
    proof = out["publish_proof"]
    assert proof["status"] == "published", proof
    assert proof["nodes_added"] > 0, proof
    assert proof["edges_added"] > 0, proof
    assert proof["dlq_count"] == 0, proof
    live = _live_rows(clean_runtime, "SELECT count(*) AS n FROM okg.graph_nodes")
    assert live[0]["n"] > 0, "no live nodes on the published generation"
    rc = main(["--json", "doctor", "--dsn", clean_runtime])
    doctor = _json_out(capsys)
    assert rc == 0, doctor

    # (2) Orchestration: the real `okg deployment up` CLI drives the full phase
    # machine. Provisioning above established the ownership + migrated + catalog
    # + published baseline the orchestration drives. `--plan` is mutation-free:
    # _phase_backup short-circuits before its effect-inventory read, so this
    # exercises the orchestration wiring without the real backup-phase execution
    # (an EXECUTION-mode concern with a pristine-checkout admission + shared-DB
    # dump — design.md §2/§5b). `--profile prod` (not dev): the fixture's
    # declared DB is the ephemeral test DB, driven directly; `--profile dev`'s
    # worktree-isolation guard (isolation-suffixed clone) would fail preflight.
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc = main([
        "--json", "deployment", "up", name,
        "--target", "prod", "--dsn", clean_runtime,
        "--no-apply-services", "--no-sync-code", "--no-sync-deps", "--plan",
        "--backup-dir", str(tmp_path / "backups"),
        "--state-file", str(tmp_path / "up" / "state.json"),
        "--reset-state",
    ])
    payload = _json_out(capsys)
    phase_order = [p.get("phase") for p in payload.get("phases", [])]
    detail = {
        "ok": payload.get("ok"),
        "failed_phase": payload.get("failed_phase"),
        "reason": payload.get("reason"),
        "phase_order": phase_order,
    }
    assert rc == 0 and payload.get("ok") is True, json.dumps(detail, indent=2, default=str)[:3000]
    # The real CLI + real Postgres orchestrator walks the full canonical
    # sequence, in order (a raised phase aborts the loop with rc != 0).
    assert phase_order == list(PHASES), json.dumps(detail, indent=2, default=str)
    not_ok = [
        {"phase": p.get("phase"), "reason": p.get("reason")}
        for p in payload["phases"] if p.get("ok") is not True
    ]
    assert not_ok == [], not_ok
