"""Witnesses for the host-headroom sentinel (PACT
``bound-host-storage-pressure``, req.host.headroom-sentinel).

Covers: WARN / BLOCKER classification at simulated thresholds, undeclared
thresholds as a loud misconfiguration (never a silent default — P0), the
okg-up backup-phase refusal naming the shortfall, the complete-reference
harness refusal likewise, and dogfood-status surfacing (WARN in warnings;
BLOCKER degrades trust and points next_action at the reclaim runbook).

NO test touches real disk state: every filesystem read goes through an
injected fake probe (fixed byte numbers), and the deployment configs live
in pytest tmp dirs.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import okg.substrate.deployment_upgrade as du
from okg.substrate.deployment_upgrade import CommandResult

# Import-graph priming: importing okg.substrate.qa.complete_reference as the
# FIRST okg.substrate.consistency-adjacent module trips a pre-existing
# circular import (possible_effects <-> claim_effective_derivation) on this
# branch; loading the consistency package first resolves the cycle the same
# way the in-repo consistency witnesses do. Owned by the consistency work,
# not this sentinel.
import okg.substrate.consistency as _consistency_warmup

from okg.substrate.qa.complete_reference import (
    HostHeadroomRefusal,
    OutcomeReplaySource,
    _preflight_scratch_host_headroom,
    run_complete_reference,
)
from okg.substrate.runtime import host_headroom
from okg.substrate.runtime.host_headroom import (
    RECLAIM_RUNBOOK,
    big_writer_preflight,
    check_host_disk_headroom,
    load_host_storage_thresholds,
    parse_host_storage_thresholds,
)

assert _consistency_warmup is not None  # cycle priming is intentional

GIB = 1024**3


@pytest.fixture(autouse=True)
def _stub_backup_tool_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hermetic: never hash the host's real binaries or reach a real DB."""
    monkeypatch.setattr(
        du,
        "_backup_tool_executable_identity",
        lambda tool: {"sha256": "dump" if tool == "pg_dump" else "restore"},
    )
    monkeypatch.setattr(
        du,
        "_backup_compatibility_snapshot",
        lambda ctx, toolchain: {
            "target": {
                "deployment": ctx.options.deployment,
                "host": "host",
                "port": 5432,
                "database": "db",
            },
            "database_owner": {
                "deployment": ctx.options.deployment,
                "uuid": "owner",
                "schema_fingerprint": "schema",
            },
            "cluster_system_identifier": "cluster",
            "server_version": "170010",
            "extension_versions": [],
            "schema_migrations_digest": "sha256:schema",
            "candidate_plan_digest": "sha256:plan",
            "archive_format": "postgresql-custom",
            "dump_tool_major": 17,
            "restore_tool_major": 17,
        },
    )


def _fake_probe(total: int, available: int):
    def probe(path: Path) -> tuple[int, int]:
        del path
        return (total, available)

    return probe


def _write_deployment(
    tmp_path: Path,
    *,
    name: str = "headroom-dep",
    host_storage: dict | None = None,
) -> Path:
    dep_dir = tmp_path / name
    dep_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict = {"name": name}
    if host_storage is not None:
        manifest["host_storage"] = host_storage
    lines = [f"name: {manifest['name']}"]
    if host_storage is not None:
        lines.append("host_storage:")
        for key, value in host_storage.items():
            lines.append(f"  {key}: {value}")
    (dep_dir / "deployment.yaml").write_text(
        "\n".join(lines) + "\n", encoding="utf-8",
    )
    return dep_dir


DECLARED = {"warn_available_pct": 15, "blocker_available_pct": 8}


# --------------------------------------------------------------------------- #
# WARN / BLOCKER / OK classification at simulated thresholds.                 #
# --------------------------------------------------------------------------- #
def test_warn_verdict_at_simulated_threshold(tmp_path: Path) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=DECLARED)
    verdict = check_host_disk_headroom(
        deployment="headroom-dep",
        deployment_dir=dep_dir,
        probe=_fake_probe(1000 * GIB, 120 * GIB),  # 12% — between 8 and 15
    )
    assert verdict.status == "warn"
    assert verdict.ok is True  # warn does not flip ok; it surfaces
    assert verdict.code == "host_headroom_warn"
    assert "15" in verdict.message and RECLAIM_RUNBOOK in verdict.message
    tightest = min(verdict.filesystems, key=lambda fs: fs.available_pct)
    assert tightest.available_bytes == 120 * GIB
    assert tightest.available_pct == 12.0


def test_blocker_verdict_at_simulated_threshold(tmp_path: Path) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=DECLARED)
    verdict = check_host_disk_headroom(
        deployment="headroom-dep",
        deployment_dir=dep_dir,
        probe=_fake_probe(1000 * GIB, 50 * GIB),  # 5% — below 8
    )
    assert verdict.status == "blocker"
    assert verdict.ok is False
    assert verdict.code == "host_headroom_blocker"
    assert RECLAIM_RUNBOOK in verdict.message
    assert str(50 * GIB) in verdict.message  # real bytes named


def test_ok_verdict_above_warn_floor(tmp_path: Path) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=DECLARED)
    verdict = check_host_disk_headroom(
        deployment="headroom-dep",
        deployment_dir=dep_dir,
        probe=_fake_probe(1000 * GIB, 500 * GIB),  # 50%
    )
    assert verdict.status == "ok"
    assert verdict.ok is True
    assert verdict.code == "host_headroom_ok"


def test_worst_filesystem_drives_classification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=DECLARED)
    docker_root = tmp_path / "fake-docker-data"
    monkeypatch.setattr(
        host_headroom, "resolve_docker_data_root", lambda: docker_root,
    )

    def probe(path: Path) -> tuple[int, int]:
        if Path(path) == docker_root:
            return (1000 * GIB, 30 * GIB)  # 3% — blocker
        return (1000 * GIB, 500 * GIB)  # 50% — fine

    verdict = check_host_disk_headroom(
        deployment="headroom-dep", deployment_dir=dep_dir, probe=probe,
    )
    assert verdict.status == "blocker"
    kinds = {fs.kind for fs in verdict.filesystems}
    assert kinds == {"deployment_dir", "docker_data_root"}
    docker_fs = next(
        fs for fs in verdict.filesystems if fs.kind == "docker_data_root"
    )
    assert docker_fs.status == "blocker"


# --------------------------------------------------------------------------- #
# Undeclared / invalid thresholds are a loud misconfiguration — never a       #
# silent default (P0).                                                        #
# --------------------------------------------------------------------------- #
def test_undeclared_thresholds_are_loud_misconfiguration(
    tmp_path: Path,
) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=None)
    verdict = check_host_disk_headroom(
        deployment="headroom-dep",
        deployment_dir=dep_dir,
        probe=_fake_probe(1000 * GIB, 500 * GIB),
    )
    assert verdict.status == "misconfigured"
    assert verdict.ok is False
    assert verdict.code == "host_headroom_thresholds_undeclared"
    # The message tells the operator exactly what to declare.
    assert "declare" in verdict.message
    assert "warn_available_pct" in verdict.message
    assert "blocker_available_pct" in verdict.message
    # NO substituted default pair — the P0 contract.
    assert verdict.thresholds is None
    # The probed numbers are still reported honestly, unclassified.
    assert verdict.filesystems
    assert all(fs.status == "unclassified" for fs in verdict.filesystems)


@pytest.mark.parametrize(
    "bad_block",
    [
        {"warn_available_pct": 15},                              # missing blocker
        {"blocker_available_pct": 8},                            # missing warn
        {"warn_available_pct": 8, "blocker_available_pct": 15},  # inverted
        {"warn_available_pct": 8, "blocker_available_pct": 8},   # equal
        {"warn_available_pct": "lots", "blocker_available_pct": 8},
        {"warn_available_pct": 150, "blocker_available_pct": 8},  # out of range
        {"warn_available_pct": 15, "blocker_available_pct": 0},   # out of range
    ],
)
def test_invalid_threshold_pair_is_misconfiguration(
    tmp_path: Path, bad_block: dict,
) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=bad_block)
    thresholds, problems = load_host_storage_thresholds(dep_dir)
    assert thresholds is None
    assert problems and all("declare" in p for p in problems)
    verdict = check_host_disk_headroom(
        deployment="headroom-dep",
        deployment_dir=dep_dir,
        probe=_fake_probe(1000 * GIB, 500 * GIB),
    )
    assert verdict.status == "misconfigured"
    assert verdict.ok is False


def test_missing_deployment_dir_is_misconfiguration() -> None:
    thresholds, problems = load_host_storage_thresholds(None)
    assert thresholds is None
    assert problems


def test_candidate_manifest_thresholds_parse_without_checkout() -> None:
    thresholds, problems = parse_host_storage_thresholds(
        {"host_storage": DECLARED},
        source_label="candidate deployment.yaml at abc123",
    )

    assert problems == []
    assert thresholds is not None
    assert thresholds.to_dict() == {
        "warn_available_pct": 15.0,
        "blocker_available_pct": 8.0,
    }


def test_probe_failure_is_loud_error_not_default_numbers(
    tmp_path: Path,
) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=DECLARED)

    def broken_probe(path: Path) -> tuple[int, int]:
        raise OSError("statvfs exploded")

    verdict = check_host_disk_headroom(
        deployment="headroom-dep", deployment_dir=dep_dir, probe=broken_probe,
    )
    assert verdict.status == "error"
    assert verdict.ok is False
    assert verdict.code == "host_headroom_probe_failed"
    assert "statvfs exploded" in verdict.message


# --------------------------------------------------------------------------- #
# Big-writer preflight semantics.                                             #
# --------------------------------------------------------------------------- #
def test_big_writer_preflight_refuses_below_blocker_naming_shortfall(
    tmp_path: Path,
) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=DECLARED)
    total, available = 1000 * GIB, 50 * GIB  # 5% < 8% floor
    result = big_writer_preflight(
        operation="witness-op",
        deployment="headroom-dep",
        deployment_dir=dep_dir,
        target_path=dep_dir,
        needed_bytes=None,
        probe=_fake_probe(total, available),
    )
    assert result["refuse"] is True
    assert result["code"] == "host_headroom_blocker"
    floor = result["blocker_floor_bytes"]
    assert floor == 80 * GIB
    assert result["shortfall_bytes"] == floor - available
    assert str(available) in result["message"]
    assert "shortfall" in result["message"]
    assert RECLAIM_RUNBOOK in result["message"]


def test_big_writer_preflight_refuses_when_write_would_cross_floor(
    tmp_path: Path,
) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=DECLARED)
    total = 1000 * GIB
    available = 100 * GIB     # 10% — above the 8% floor right now
    needed = 30 * GIB         # ...but the write would land at 7%
    result = big_writer_preflight(
        operation="witness-op",
        deployment="headroom-dep",
        deployment_dir=dep_dir,
        target_path=dep_dir,
        needed_bytes=needed,
        probe=_fake_probe(total, available),
    )
    assert result["refuse"] is True
    # Shortfall names needed vs available.
    assert str(needed) in result["message"]
    assert str(available) in result["message"]
    assert result["shortfall_bytes"] == 80 * GIB - (available - needed)


def test_big_writer_preflight_passes_with_headroom(tmp_path: Path) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=DECLARED)
    result = big_writer_preflight(
        operation="witness-op",
        deployment="headroom-dep",
        deployment_dir=dep_dir,
        target_path=dep_dir,
        needed_bytes=10 * GIB,
        probe=_fake_probe(1000 * GIB, 500 * GIB),
    )
    assert result["refuse"] is False
    assert result["ok"] is True
    assert result["status"] == "ok"


def test_big_writer_preflight_unarmed_when_thresholds_undeclared(
    tmp_path: Path,
) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=None)
    probe_calls: list[Path] = []

    def counting_probe(path: Path) -> tuple[int, int]:
        probe_calls.append(Path(path))
        return (1000 * GIB, 500 * GIB)

    result = big_writer_preflight(
        operation="witness-op",
        deployment="headroom-dep",
        deployment_dir=dep_dir,
        target_path=dep_dir,
        probe=counting_probe,
    )
    assert result["refuse"] is False
    assert result["status"] == "misconfigured"
    assert result["ok"] is False  # loud in the payload, not silent
    assert "NOT armed" in result["message"]
    assert "declare" in result["message"]
    assert probe_calls == []  # enforcement un-armed => no probe either


def test_big_writer_preflight_probe_failure_fails_closed(
    tmp_path: Path,
) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=DECLARED)

    def broken_probe(path: Path) -> tuple[int, int]:
        raise OSError("statvfs exploded")

    result = big_writer_preflight(
        operation="witness-op",
        deployment="headroom-dep",
        deployment_dir=dep_dir,
        target_path=dep_dir,
        probe=broken_probe,
    )
    assert result["refuse"] is True
    assert result["code"] == "host_headroom_probe_failed"


# --------------------------------------------------------------------------- #
# okg up backup-phase preflight.                                              #
# --------------------------------------------------------------------------- #
def _toolchain_runner(calls: list[list[str]]):
    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        if argv[:2] in (
            ["pg_dump", "--version"], ["pg_restore", "--version"],
        ):
            return CommandResult(0, f"{argv[0]} (PostgreSQL) 17.10\n", "")
        if argv[:2] == ["pg_restore", "--list"]:
            return CommandResult(0, "; archive listing\n", "")
        if argv and argv[0] == "pg_dump":
            path = Path(argv[argv.index("--file") + 1])
            path.write_bytes(b"valid custom archive")
            return CommandResult(0, "", "")
        return CommandResult(0, json.dumps({"ok": True}), "")

    return runner


def _upgrade_ctx(tmp_path: Path, deployment: str) -> SimpleNamespace:
    calls: list[list[str]] = []
    ctx = SimpleNamespace(
        # The REAL frozen dataclass, not a hand-rolled namespace. A
        # SimpleNamespace silently lacks any field added later, which is how
        # these tests broke: `apply_services` was introduced on UpgradeOptions
        # and the fake options raised AttributeError deep inside the phase.
        # Constructing the dataclass means a new field with a default just
        # works, and a new REQUIRED field fails loudly here instead of
        # mid-phase.
        options=du.UpgradeOptions(
            deployment=deployment,
            skip_backup=False,
            skip_migrate=False,
            plan_only=False,
            stop_after=None,
            readiness_profile="general",
            confirm_destructive_migrations=False,
            # These headroom tests exercise the direct forced-dump flow;
            # the independent-verify path preserves it without the routine
            # effect-classification machinery.
            independent_backup_verify=True,
        ),
        dsn="postgres://user@127.0.0.1:9/db",
        env={},
        backup_dir=tmp_path / "backups",
        command_runner=_toolchain_runner(calls),
        data={"migrations_before": {"pending": [], "destructive_pending": []}},
    )
    ctx.calls = calls
    return ctx


def _backup_manifest(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        deployment_dir=tmp_path,
        deployment_up={
            "backup": {
                # The independent-verify path writes a disaster-recovery
                # receipt; only the risk_admitted policy declares the
                # recovery-point objective that the receipt requires.
                "verification_policy": "risk_admitted",
                "disaster_recovery_archive_max_age_seconds": 86400,
                "disaster_recovery_restore_max_age_seconds": 604800,
                "recovery_point_objective_seconds": 86400,
                "restore_volume": {"kind": "docker_data_root"},
                "restore_peak_factor": 1.25,
            }
        },
        release={"channel": "prod"},
        nomos={"deployment_posture": {"deployment_class": "production"}},
    )


def test_okg_up_backup_refuses_under_pressure_naming_shortfall(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_deployment(
        tmp_path / "deployments", name="presdep", host_storage=DECLARED,
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setattr(
        host_headroom, "probe_filesystem",
        _fake_probe(1000 * GIB, 50 * GIB),  # 5% < 8% floor
    )
    needed = 4 * GIB
    monkeypatch.setattr(du, "_estimate_source_db_bytes", lambda ctx: needed)
    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: _backup_manifest(tmp_path),
    )

    ctx = _upgrade_ctx(tmp_path, "presdep")
    result = du._phase_backup(ctx)

    assert result["ok"] is False
    assert result["reason"] == "insufficient host disk headroom for backup"
    headroom = result["headroom"]
    assert headroom["refuse"] is True
    # Shortfall named: needed vs available, in the message and the payload.
    assert headroom["needed_bytes"] == needed
    assert headroom["available_bytes"] == 50 * GIB
    assert str(needed) in headroom["message"]
    assert str(50 * GIB) in headroom["message"]
    assert "shortfall" in headroom["message"]
    assert RECLAIM_RUNBOOK in result["fix_hint"]
    # pg_dump itself never ran — only the toolchain version probes did.
    assert all(call[:2] != ["pg_dump", "--file"] for call in ctx.calls)
    assert not any(
        call[0] == "pg_dump" and "--file" in call for call in ctx.calls
    )
    assert not ctx.backup_dir.exists()


def test_okg_up_backup_proceeds_with_headroom(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_deployment(
        tmp_path / "deployments", name="presdep", host_storage=DECLARED,
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "deployments"))
    monkeypatch.setattr(
        host_headroom, "probe_filesystem",
        _fake_probe(1000 * GIB, 500 * GIB),  # 50%
    )
    monkeypatch.setattr(du, "_estimate_source_db_bytes", lambda ctx: 4 * GIB)
    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: _backup_manifest(tmp_path),
    )
    monkeypatch.setattr(
        du,
        "_backup_compatibility_snapshot",
        lambda ctx, toolchain: {
            "target": {
                "deployment": "presdep",
                "host": "host",
                "port": 5432,
                "database": "db",
            },
            "database_owner": {
                "deployment": "presdep",
                "uuid": "owner",
                "schema_fingerprint": "schema",
            },
            "cluster_system_identifier": "cluster",
            "server_version": "170010",
            "extension_versions": [],
            "schema_migrations_digest": "sha256:schema",
            "candidate_plan_digest": "sha256:plan",
            "archive_format": "postgresql-custom",
            "dump_tool_major": 17,
            "restore_tool_major": 17,
        },
    )
    monkeypatch.setattr(
        du,
        "_restore_headroom_preflight",
        lambda ctx, policy, **kwargs: {
            "ok": True,
            "refuse": False,
            "status": "ok",
        },
    )
    monkeypatch.setattr(
        du, "_verify_backup_restore",
        lambda ctx, path, **_: {
            "ok": True,
            "restore_verified": True,
            "observed_restored_size_bytes": 1024,
        },
    )

    ctx = _upgrade_ctx(tmp_path, "presdep")
    result = du._phase_backup(ctx)

    assert result["ok"] is True
    assert result["headroom"]["refuse"] is False
    assert any(call[0] == "pg_dump" and "--file" in call for call in ctx.calls)


def test_okg_up_backup_unarmed_annotation_when_undeclared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Deployment with NO deployment dir at all (the hermetic-test shape):
    # enforcement is un-armed, so the large write refuses before the DB-size
    # estimate is observed, but pg_dump is never attempted.
    empty_root = tmp_path / "empty-deployments"
    empty_root.mkdir()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(empty_root))
    estimate_calls: list[object] = []

    def counting_estimate(ctx) -> int:
        estimate_calls.append(ctx)
        return 1

    monkeypatch.setattr(du, "_estimate_source_db_bytes", counting_estimate)
    monkeypatch.setattr(
        du, "_verify_backup_restore",
        lambda ctx, path, **_: {"ok": True, "restore_verified": True},
    )
    monkeypatch.setattr(
        du,
        "_resolve_upgrade_deployment",
        lambda ctx: _backup_manifest(tmp_path),
    )

    ctx = _upgrade_ctx(tmp_path, "no-such-dep")
    result = du._phase_backup(ctx)

    assert result["ok"] is False
    assert result["reason"] == "insufficient host disk headroom for backup"
    assert result["headroom"]["status"] == "misconfigured"
    assert "not enforceable" in result["headroom"]["message"]
    assert estimate_calls == [ctx]
    assert not any(call[0] == "pg_dump" and "--file" in call for call in ctx.calls)


# --------------------------------------------------------------------------- #
# Complete-reference harness preflight.                                       #
# --------------------------------------------------------------------------- #
def test_harness_preflight_refuses_under_pressure_naming_shortfall(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=DECLARED)
    monkeypatch.setattr(
        host_headroom, "probe_filesystem",
        _fake_probe(1000 * GIB, 50 * GIB),
    )
    with pytest.raises(HostHeadroomRefusal) as exc:
        _preflight_scratch_host_headroom(dep_dir)
    message = str(exc.value)
    assert "shortfall" in message
    assert str(50 * GIB) in message
    assert RECLAIM_RUNBOOK in message


def test_harness_run_refuses_before_any_db_touch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The scratch DSN points at a dead endpoint: if the pipeline touched the
    # DB before the headroom preflight, we would see a connection error, not
    # the typed refusal.
    dep_dir = _write_deployment(tmp_path, host_storage=DECLARED)
    monkeypatch.setattr(
        host_headroom, "probe_filesystem",
        _fake_probe(1000 * GIB, 50 * GIB),
    )
    with pytest.raises(HostHeadroomRefusal):
        run_complete_reference(
            [object()],
            deployment_dir=dep_dir,
            scratch_dsn="postgresql://127.0.0.1:9/okg_test_headroom_witness",
            outcome_store_source=OutcomeReplaySource(
                deployment="headroom-dep", rows=(),
            ),
        )


def test_harness_preflight_unarmed_when_undeclared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog,
) -> None:
    dep_dir = _write_deployment(tmp_path, host_storage=None)
    probe_calls: list[Path] = []

    def counting_probe(path: Path) -> tuple[int, int]:
        probe_calls.append(Path(path))
        return (1000 * GIB, 1 * GIB)

    monkeypatch.setattr(host_headroom, "probe_filesystem", counting_probe)
    with caplog.at_level("WARNING"):
        _preflight_scratch_host_headroom(dep_dir)  # no raise
    assert probe_calls == []
    assert any(
        "NOT armed" in record.getMessage() for record in caplog.records
    )


# --------------------------------------------------------------------------- #
# Dogfood status surfacing.                                                   #
# --------------------------------------------------------------------------- #
def _healthy_environment() -> dict:
    return {
        "ok": True,
        "authority": {
            "target": {
                "identity": "ok",
                "database": "okg_workspace",
                "expected_database": "okg_workspace",
            }
        },
    }


def _headroom_check_row(status: str, message: str) -> dict:
    return {
        "id": "host.storage_headroom",
        "status": status,
        "ok": False if status == "blocker" else None,
        "message": message,
        "details": {"code": f"host_headroom_{status}"},
    }


def test_dogfood_blocker_degrades_trust_and_names_reclaim_runbook() -> None:
    from okg.substrate.cli.dogfood import _status_payload

    payload = _status_payload(
        deployment="okg-workspace",
        environment=_healthy_environment(),
        live={
            "blockers": [
                _headroom_check_row(
                    "blocker", "host disk headroom is below the blocker floor",
                ),
            ],
            "warnings": [],
            "generation": {"status": "published", "generation_id": "g1"},
            "mcp": {"ok": True},
            "host_storage": {"status": "blocker", "ok": False},
        },
        resolved_dsn=True,
        include_details=False,
    )
    # Trust degrades (a healthy graph under host pressure is NOT "ready").
    assert payload["trust_level"] == "stale_context"
    assert payload["ok"] is False
    assert any(
        row["id"] == "host.storage_headroom" for row in payload["blockers"]
    )
    # next_action IS the reclaim runbook.
    next_action = payload["next_action"]
    assert next_action["code"] == "reclaim_host_storage"
    assert next_action["runbook"] == "docs/runbooks/troubleshooting/storage.md"
    assert "docs/runbooks/troubleshooting/storage.md" in next_action["command"]
    # The headroom block is surfaced with its numbers.
    assert payload["host_storage"] == {"status": "blocker", "ok": False}


def test_dogfood_warn_appears_in_warnings_without_degrading_trust() -> None:
    from okg.substrate.cli.dogfood import _status_payload

    payload = _status_payload(
        deployment="okg-workspace",
        environment=_healthy_environment(),
        live={
            "blockers": [],
            "warnings": [
                _headroom_check_row(
                    "warning", "host disk headroom is below the warn floor",
                ),
            ],
            "generation": {"status": "published", "generation_id": "g1"},
            "mcp": {"ok": True},
            "host_storage": {"status": "warn", "ok": True},
        },
        resolved_dsn=True,
        include_details=False,
    )
    assert payload["trust_level"] == "ready"
    assert any(
        row["id"] == "host.storage_headroom" for row in payload["warnings"]
    )
    assert payload["next_action"]["code"] == "none"


# --------------------------------------------------------------------------- #
# Readiness wiring: the sentinel reports even when the database is dead.      #
# --------------------------------------------------------------------------- #
def test_readiness_reports_headroom_even_when_database_is_down(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli.deployment import _deployment_ready_live_checks

    dep_dir = _write_deployment(tmp_path, host_storage=DECLARED)
    monkeypatch.setattr(
        host_headroom, "probe_filesystem",
        _fake_probe(1000 * GIB, 50 * GIB),  # blocker pressure
    )
    payload = _deployment_ready_live_checks(
        deployment="headroom-dep",
        deployment_dir=dep_dir,
        dsn="postgresql://127.0.0.1:9/okg_test_headroom_witness",  # dead
        max_active_queue_depth=0,
        max_source_age_seconds=60,
    )
    check_ids = [row["id"] for row in payload["checks"]]
    # The headroom check ran FIRST — before the connectivity failure.
    assert check_ids[0] == "host.storage_headroom"
    blocker_ids = {row["id"] for row in payload["blockers"]}
    assert "host.storage_headroom" in blocker_ids
    assert "database.connectivity" in blocker_ids
    assert payload["host_storage"]["status"] == "blocker"


@pytest.mark.parametrize(
    "profile", ["minimal", "workspace-graph", "academic-researcher"],
)
def test_shipped_profiles_declare_host_storage_thresholds(profile: str) -> None:
    """Issue #3578: a fresh `okg install` reported this check misconfigured
    because no shipped profile declared the thresholds."""
    import yaml

    import okg.substrate.library as library

    defaults = (
        Path(library.__file__).parent / "profiles" / profile
        / "deployment-defaults.yaml"
    )
    manifest = yaml.safe_load(defaults.read_text(encoding="utf-8"))
    thresholds, problems = host_headroom.parse_host_storage_thresholds(manifest)
    assert problems == []
    assert thresholds is not None
    assert thresholds.blocker_available_pct < thresholds.warn_available_pct


def test_deployment_new_scaffold_declares_host_storage_thresholds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.deployment_scaffold import create_deployment_scaffold

    monkeypatch.delenv("OKG_DSN", raising=False)
    result = create_deployment_scaffold(
        "headroom-scaffold", deployments_root=tmp_path / "deployments",
    )
    thresholds, problems = host_headroom.load_host_storage_thresholds(
        result.deployment_dir,
    )
    assert problems == []
    assert thresholds is not None


def test_deployment_bootstrap_writes_host_storage_thresholds(tmp_path: Path) -> None:
    """Review finding (#3578): `okg deployment bootstrap` writes its own
    deployment.yaml, and newcomers are pointed at it."""
    import yaml

    from okg.substrate.deployment_bootstrap.workflow import (
        _build_artifacts,
        _ensure_default_authoring_plan,
        create_plan,
        draft,
        load_plan,
    )

    dep = tmp_path / "boot"
    brief = tmp_path / "brief.md"
    brief.write_text("A probe deployment.\n", encoding="utf-8")
    create_plan(deployment=dep, brief_file=brief, apply=True)
    draft(deployment=dep)
    plan = load_plan(dep)
    _ensure_default_authoring_plan(plan)
    written = yaml.safe_load(_build_artifacts(dep, plan)["deployment.yaml"])
    thresholds, problems = host_headroom.parse_host_storage_thresholds(written)
    assert problems == []
    assert thresholds is not None
