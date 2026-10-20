"""Witnesses for `okg up` backup retention (bound-host-storage-pressure,
req.host.workspace-hygiene, artifact class 1).

Retention contract under test:

- keep-last-N verified archives per deployment (N = UpgradeOptions.backup_keep,
  default 1), applied ONLY at the tail of a fully successful upgrade;
- the archive the run just wrote and verified is never deleted;
- a failed run deletes nothing (its archive may be the only good backup);
- a run that wrote no archive (--skip-backup, --plan) prunes nothing;
- only regular files/directories matching this deployment's
  `<name>-<UTC>.(dump|dumpdir)` pattern are eligible — foreign files and
  symlinks in the backup dir are never touched;
- the report lists every deleted archive with its size.
"""
from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest

from okg.substrate.deployment_upgrade import (
    CommandResult,
    UpgradeOptions,
    run_deployment_upgrade,
)
from okg.substrate.errors import SubstrateError


def _ok(payload: dict | None = None) -> CommandResult:
    return CommandResult(0, json.dumps(payload or {"ok": True}), "")


def _published(generation_id: str) -> dict:
    """Smoke-publish payload that passes the uniform real-publish gate.

    Issue #1291: the gate also requires the receipt to show that the publish
    held the advisory lock well inside the configured share of the hard
    statement timeout, so a healthy hermetic payload carries that measurement.
    """
    return {
        "status": "published",
        "generation_id": generation_id,
        "nodes_added": 3,
        "edges_added": 2,
        "timing_details": {
            "schema": "okg.publisher.timing_details.v1",
            "lock": {
                "coordination_mode": "dbos",
                "acquire_ms": 4.0,
                "wait_ms": 4.0,
                "hold_ms": 12_000.0,
                "hard_timeout_ms": 900_000,
            },
        },
    }


@pytest.fixture(autouse=True)
def _hermetic_upgrade(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Same hermetic harness as tests/substrate/test_deployment_upgrade.py."""
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_target",
        lambda options: {
            "dsn": "postgres://user@host/db",
            "target": {"database": "db", "identity": "ok"},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_sync_code",
        lambda ctx: {"ok": True, "code_revision": "deadbeefcafe"},
    )
    # The deployment above is faked at a temp path, so it is inside no
    # repository. `dependency_sync` resolves the checkout whose lock it applies
    # through the same repository authority `sync_code` uses, so it has no
    # target here -- which is why the line above already stubs `_phase_sync_code`
    # for that phase. This is the other half of that stub, and it names THIS
    # checkout, which is what an in-repo deployment resolves to. The retention
    # assertions below are unchanged: this only keeps the run reaching the
    # phase each test is actually about.
    from okg.substrate import deployment_upgrade as _du

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._dependency_sync_repo_root",
        lambda *_args, **_kwargs: Path(_du.__file__).resolve().parents[3],
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._resolve_upgrade_deployment",
        lambda ctx: __import__("types").SimpleNamespace(
            deployment_dir=tmp_path / "scratch-dep",
            release={"channel": "prod"},
            nomos={"deployment_posture": {"deployment_class": "production"}},
            deployment_up={
                "backup": {
                    "verification_policy": "current_full_restore",
                    "restore_volume": {"kind": "docker_data_root"},
                    "restore_peak_factor": 1.25,
                }
            },
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._ensure_upgrade_extensions",
        lambda ctx: {"created": [], "already_present": []},
    )
    # Like every other direct-database helper stubbed here, the catalog
    # ownership reclaim opens its own connection to the (unreachable)
    # harness DSN. Retention flows assert phase order, not ownership; the
    # guarded reclaim is proven in tests/substrate/test_up_catalog_reclaim.py.
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._reclaim_changed_catalog",
        lambda ctx, deployment_dir: {"reclaimed": False},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_nomos_baseline",
        lambda ctx: {"ok": True, "skipped": "unit"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_generation_recovery",
        lambda ctx: {"ok": True, "classification": "not_required"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._phase_source_convergence",
        lambda ctx: {"ok": True, "state": "ready"},
    )
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn, *, ignored_workflow_names=None: (0, {}),
    )
    # Hermetic: never hash the host's real pg_dump/pg_restore binaries.
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_tool_executable_identity",
        lambda tool: {"sha256": "dump" if tool == "pg_dump" else "restore"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._verify_backup_restore",
        lambda ctx, path, **_: {
            "ok": True,
            "restore_verified": True,
            "schema_ok": True,
            "ownership_ok": True,
            "observed_restored_size_bytes": 1024,
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._estimate_source_db_bytes",
        lambda ctx: 1024,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_headroom_preflight",
        lambda ctx, **kwargs: {"ok": True, "refuse": False, "status": "ok"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_post_dump_headroom_preflight",
        lambda ctx: {"ok": True, "refuse": False, "status": "ok"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._restore_headroom_preflight",
        lambda ctx, policy, **kwargs: {
            "ok": True,
            "refuse": False,
            "status": "ok",
        },
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._backup_compatibility_snapshot",
        lambda ctx, toolchain: {
            "target": {
                "deployment": "dep",
                "host": "host",
                "port": 5432,
                "database": "db",
            },
            "database_owner": {
                "deployment": "dep",
                "uuid": "owner",
                "schema_fingerprint": "schema",
            },
            "cluster_system_identifier": "cluster",
            "server_version": "170010",
            "extension_versions": [],
            "schema_migrations_digest": "sha256:schema",
            "candidate_plan_digest": "sha256:test-plan",
            "archive_format": "postgresql-custom",
            "dump_tool_major": 17,
            "restore_tool_major": 17,
        },
    )

    # The effect classifier reads the real repo/manifest and refuses before
    # the forced path when unreadable; these retention flows only need a
    # closed inventory so the production posture selects the forced dump.
    def closed_inventory(ctx, expected_digest=None):
        from okg.substrate.deployment_backup_policy import (
            EffectInventoryChecks,
        )

        checks = EffectInventoryChecks(**{
            item.name: True for item in dataclasses.fields(
                EffectInventoryChecks
            )
        })
        return checks, (), {
            "schema": "okg.deployment_effect_inventory.v1",
            "target_revision": "test-revision",
            "effect_digest": "sha256:test-effect",
            "execution_checkout_exact": True,
            "smoke": {"selected_source_digest": "sha256:test-smoke"},
            "checks": checks.to_dict(),
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._collect_effect_inventory",
        closed_inventory,
    )


def _runner(fail_phase_command: str | None = None):
    """Command runner driving a full successful upgrade (or one failure)."""

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        if fail_phase_command and fail_phase_command in argv:
            return CommandResult(1, "", "injected failure")
        if argv[:2] in (["pg_dump", "--version"], ["pg_restore", "--version"]):
            return CommandResult(0, f"{argv[0]} (PostgreSQL) 17.10\n", "")
        if argv[:2] == ["pg_restore", "--list"]:
            return CommandResult(0, "; archive listing\n", "")
        if argv and argv[0] == "pg_dump":
            path = Path(argv[argv.index("--file") + 1])
            path.write_bytes(b"fresh verified archive bytes")
            return CommandResult(0, "", "")
        if "migrations" in argv:
            return _ok({"pending": [], "destructive_pending": []})
        if "inspect-effective-attribute-recovery" in argv:
            return _ok({
                "schema": "okg.effective_attribute_recovery_classification.v1",
                "classification": "no_published_generation",
                "deployment": "dep",
                "branch": "default",
                "binding_digest": "sha256:" + "a" * 64,
            })
        if "ingest" in argv:
            return _ok({"publishes": [_published("gen-smoke")]})
        if "ready" in argv:
            return _ok({"generation": {"generation_id": "gen-smoke"}})
        if "release" in argv:
            return _ok({"ok": True, "channel": "dev"})
        return _ok()

    return runner


def _seed_archives(backup_dir: Path, deployment: str = "dep") -> dict[str, Path]:
    """Two superseded archives + two foreign files that must never be touched."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    oldest = backup_dir / f"{deployment}-20260101T000000Z.dump"
    older = backup_dir / f"{deployment}-20260102T000000Z.dump"
    oldest.write_bytes(b"x" * 11)
    older.write_bytes(b"y" * 22)
    foreign_name = backup_dir / f"{deployment}-manual-copy.dump"
    foreign_deployment = backup_dir / "otherdep-20260101T000000Z.dump"
    foreign_name.write_bytes(b"keep me")
    foreign_deployment.write_bytes(b"keep me too")
    return {
        "oldest": oldest,
        "older": older,
        "foreign_name": foreign_name,
        "foreign_deployment": foreign_deployment,
    }


def _options(tmp_path: Path, **overrides) -> UpgradeOptions:
    defaults = dict(
        deployment="dep",
        state_file=tmp_path / "up-state.json",
        backup_dir=tmp_path / "backups",
    )
    defaults.update(overrides)
    return UpgradeOptions(**defaults)


def test_success_deletes_superseded_archives_keeping_last_one(
    tmp_path: Path,
) -> None:
    seeded = _seed_archives(tmp_path / "backups")

    result = run_deployment_upgrade(
        _options(tmp_path),
        command_runner=_runner(),
    )

    assert result["ok"] is True
    assert result["status"] == "applied"
    retention = result["backup_retention"]
    assert retention["phase"] == "backup_retention"
    assert retention["ok"] is True
    assert retention["keep"] == 1

    # Both superseded archives deleted, each reported with its size.
    deleted = {row["path"]: row["size_bytes"] for row in retention["deleted"]}
    assert deleted == {
        str(seeded["oldest"]): 11,
        str(seeded["older"]): 22,
    }
    assert retention["deleted_bytes"] == 33
    assert not seeded["oldest"].exists()
    assert not seeded["older"].exists()

    # The just-written verified archive is kept.
    backup_row = next(
        row for row in result["phases"] if row["phase"] == "backup"
    )
    just_written = Path(backup_row["backup_path"])
    assert just_written.exists()
    assert [row["path"] for row in retention["kept"]] == [str(just_written)]

    # Foreign files (wrong pattern / other deployment) are never touched.
    assert seeded["foreign_name"].exists()
    assert seeded["foreign_deployment"].exists()

    # The retention report is persisted in the upgrade state.
    state = json.loads((tmp_path / "up-state.json").read_text())
    assert state["backup_retention"]["deleted_bytes"] == 33


def test_backup_keep_two_retains_newest_superseded_archive(
    tmp_path: Path,
) -> None:
    seeded = _seed_archives(tmp_path / "backups")

    result = run_deployment_upgrade(
        _options(tmp_path, backup_keep=2),
        command_runner=_runner(),
    )

    retention = result["backup_retention"]
    assert retention["ok"] is True
    assert [row["path"] for row in retention["deleted"]] == [
        str(seeded["oldest"]),
    ]
    assert not seeded["oldest"].exists()
    assert seeded["older"].exists()
    kept_paths = {row["path"] for row in retention["kept"]}
    assert str(seeded["older"]) in kept_paths
    assert len(kept_paths) == 2


def test_success_prunes_contained_directory_archive_without_following_symlink(
    tmp_path: Path,
) -> None:
    backup_dir = tmp_path / "backups"
    seeded = _seed_archives(backup_dir)
    old_directory = backup_dir / "dep-20251231T000000Z.dumpdir"
    old_directory.mkdir(mode=0o700)
    (old_directory / "1.dat.gz").write_bytes(b"directory member")
    outside = tmp_path / "outside.dat"
    outside.write_bytes(b"must survive")
    foreign_symlink = backup_dir / "dep-20251230T000000Z.dumpdir"
    foreign_symlink.symlink_to(outside)

    result = run_deployment_upgrade(
        _options(tmp_path),
        command_runner=_runner(),
    )

    assert result["ok"] is True
    deleted = {row["path"] for row in result["backup_retention"]["deleted"]}
    assert str(old_directory) in deleted
    assert not old_directory.exists()
    assert outside.read_bytes() == b"must survive"
    assert foreign_symlink.is_symlink()
    assert seeded["foreign_name"].exists()


def test_failed_run_never_deletes_any_archive(tmp_path: Path) -> None:
    seeded = _seed_archives(tmp_path / "backups")

    result = run_deployment_upgrade(
        _options(tmp_path),
        command_runner=_runner(fail_phase_command="ready"),
    )

    assert result["ok"] is False
    assert result["failed_phase"] == "readiness"
    assert "backup_retention" not in result
    # Every archive survives a failed run — including this run's own dump,
    # which may be the only good backup.
    assert seeded["oldest"].exists()
    assert seeded["older"].exists()
    survivors = list((tmp_path / "backups").glob("dep-*.dump"))
    assert len(survivors) == 4  # oldest, older, foreign_name, fresh dump


def test_skip_backup_mutating_run_refuses_and_prunes_nothing(tmp_path: Path) -> None:
    seeded = _seed_archives(tmp_path / "backups")

    result = run_deployment_upgrade(
        _options(tmp_path, skip_backup=True),
        command_runner=_runner(),
    )

    assert result["ok"] is False
    assert result["failed_phase"] == "backup"
    assert "backup_retention" not in result
    assert seeded["oldest"].exists()
    assert seeded["older"].exists()


def test_plan_only_run_prunes_nothing(tmp_path: Path) -> None:
    seeded = _seed_archives(tmp_path / "backups")

    result = run_deployment_upgrade(
        _options(
            tmp_path,
            plan_only=True,
            apply_services=False,
            sync_code=False,
            sync_deps=False,
        ),
        command_runner=_runner(),
    )

    assert result["status"] == "plan"
    assert "backup_retention" not in result
    assert seeded["oldest"].exists()
    assert seeded["older"].exists()


def test_backup_keep_below_one_is_a_loud_misconfiguration(
    tmp_path: Path,
) -> None:
    with pytest.raises(SubstrateError) as excinfo:
        run_deployment_upgrade(
            _options(tmp_path, backup_keep=0),
            command_runner=_runner(),
        )
    assert "backup retention" in excinfo.value.message
