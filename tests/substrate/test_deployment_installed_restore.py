"""`okg deployment backup restore` for an installed deployment (#3415).

No database: the restore path, the fingerprints, the access copy and the
rename swap are stubs that record what ran, so each test proves which steps
ran and, for the refusals and failures, that the live database was never
swapped. tests/substrate/test_installed_upgrade_backup_pg.py drives the
same code against a real PostgreSQL server.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate import deployment_installed_restore as dir_
from okg.substrate import deployment_installed_upgrade as diu
from okg.substrate.deployment_upgrade import CommandResult

NAME = "installed-dep"
DATABASE = {"database": "okgdb", "port": 5432, "system_identifier": "7"}
FINGERPRINT = {
    "schema_migrations": 40, "catalog_versions": 1, "generation_id": "gen-1",
    "node_count": 15, "edge_count": 16, "published_generations": 1,
    "published_generation_ids": ["gen-1"],
}


ACCESS = {"digest": "sha256:" + "a" * 64, "rows": 2, "entries": [
    ["schema", "okg", "okg", "okg", "okg", "USAGE", False],
    ["schema", "okg", "okg", "okg", "okg_mcp", "USAGE", False],
]}


def _identity(version: str) -> dict:
    contract = {"schema": "okg.release_contract.v1", "okg_version": version,
                "sections": {"cli": {"up": {}}}}
    return {
        "okg_version": version, "payload_digest": None, "installed_from_wheel": False,
        "schema_checksum": "sha256:" + "5" * 64,
        "contract_digest": diu._contract_digest(contract), "contract": contract,
    }


def _setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, previous_receipt: bool = True,
    bless: bool = True,
) -> tuple[Path, Path]:
    root = tmp_path / "deployments"
    deployment_dir = root / NAME
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({"name": NAME, "release": {"channel": "dev", "kind": "standalone"}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "user-home"))
    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    archive = diu.backups_dir(NAME) / "installed-dep-0.2.0-to-0.3.0-x.dump"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"PGDMP fake archive")
    diu.write_installed_release_receipt(
        NAME, deployment_dir, written_by="install", identity=_identity("0.2.0"),
    )
    previous = json.loads(diu.receipt_path(NAME).read_text())
    diu._write_private_json(diu.sidecar_path(archive), {
        "schema": diu.BACKUP_SCHEMA,
        "deployment": NAME,
        "deployment_dir": str(deployment_dir),
        "archive": archive.name,
        "archive_sha256": diu._file_sha256(archive),
        "archive_bytes": archive.stat().st_size,
        "from_version": "0.2.0",
        "to_version": "0.3.0",
        "resumed_upgrade": False,
        "applied_migrations": ["0001", "0040"],
        "database": DATABASE,
        "fingerprint": FINGERPRINT,
        "access_fingerprint": ACCESS,
        "previous_receipt": previous if previous_receipt else None,
    })
    diu.write_installed_release_receipt(
        NAME, deployment_dir, written_by="upgrade", identity=_identity("0.3.0"),
        upgrade={"pre_upgrade_archive": str(archive) if bless else None},
    )
    diu._write_marker(NAME, {"worker_stopped_by_upgrade": True})
    return deployment_dir, archive


class _Stubs:
    def __init__(self, monkeypatch: pytest.MonkeyPatch):
        self.events: list[str] = []
        self.worker_running = True
        self.restored_fingerprint = dict(FINGERPRINT)
        self.live_fingerprint = dict(FINGERPRINT)
        self.database = dict(DATABASE)
        monkeypatch.setattr(diu, "_resolve_dsn", lambda options: "postgresql://u@h/okgdb")
        monkeypatch.setattr(diu, "_database_identity", lambda dsn: self.database)
        monkeypatch.setattr(
            diu, "_client_tools_check",
            lambda dsn, tools=(), **kwargs: {"ok": True, "server_major": 17},
        )
        monkeypatch.setattr(dir_, "_database_access", lambda dsn, live, names: {
            "actor": "postgres", "superuser": True, "owner": "postgres",
            "sessions": [], "problems": [],
        })

        def audit(deployment):
            return {
                "status": "single_executor" if self.worker_running else "not_observed",
                "root_count": 1 if self.worker_running else 0,
                "roots": [{"pid": 1}] if self.worker_running else [],
            }

        monkeypatch.setattr(diu, "_worker_audit", audit)
        self.service_available = True
        monkeypatch.setattr(
            diu, "worker_service",
            lambda deployment, **kwargs: {"available": self.service_available},
        )

        def restore(source, target, archive, **kwargs):
            self.events.append(f"restore:{target.rsplit('/', 1)[-1][:13]}")
            return {"ok": True, "operation": "database-clone"}

        monkeypatch.setattr("okg.substrate.postgres_clone.restore_database_dump", restore)

        def fingerprint(dsn, deployment):
            if "_restore_" in dsn:
                self.events.append("fingerprint:restored")
                return self.restored_fingerprint
            self.events.append("fingerprint:live")
            return self.live_fingerprint

        monkeypatch.setattr(diu, "_backup_fingerprint", fingerprint)
        monkeypatch.setattr(
            dir_, "_copy_database_access",
            lambda dsn, live, restored: self.events.append("copy_access")
            or {"step": "copy_access", "ok": True},
        )
        monkeypatch.setattr(
            dir_, "_fence_drain_swap",
            lambda dsn, live, restored, kept, **kw: self.events.append(f"swap:{kept[:17]}")
            or {"step": "swap", "ok": True, "swapped": True},
        )
        self.restored_access = dict(ACCESS)
        self.live_access = dict(ACCESS)
        monkeypatch.setattr(
            diu, "replay_archive_access",
            lambda dsn, archive: self.events.append("replay_access")
            or {"owner_statements": 3, "acl_entries": 4},
        )
        monkeypatch.setattr(
            diu, "access_fingerprint",
            lambda dsn: self.restored_access if "_restore_" in dsn else self.live_access,
        )
        monkeypatch.setattr(
            dir_, "_start_timescaledb_workers",
            lambda dsn, live: self.events.append("timescaledb_workers")
            or {"step": "timescaledb_workers", "ok": True},
        )

    def runner(self, argv: list[str], env: dict[str, str]) -> CommandResult:
        tail = argv[argv.index("-m") + 2:]
        self.events.append(" ".join(tail[:2]))
        if tail[:2] == ["dogfood", "down"]:
            self.worker_running = False
            return CommandResult(0, "{}")
        raise AssertionError(argv)


def test_plan_changes_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    report = dir_.run_installed_restore(NAME, archive, apply=False, command_runner=stubs.runner)
    assert report["status"] == "planned"
    assert report["restored_database"].startswith("okgdb_restore_")
    assert report["kept_database"].startswith("okgdb_pre_restore_")
    assert report["worker"]["running"] is True
    assert stubs.events == []
    assert json.loads(diu.receipt_path(NAME).read_text())["okg_version"] == "0.3.0"


@pytest.mark.parametrize("apply", [False, True])
def test_a_missing_pg_restore_is_refused_before_the_plan_or_the_worker_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, apply: bool,
) -> None:
    """Issue #3587: the restore needs pg_restore; say so before anything."""
    from okg.substrate import postgres_clone as pc

    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    monkeypatch.setattr(diu, "_client_tools_check", _REAL_CLIENT_TOOLS_CHECK)
    monkeypatch.setattr(pc, "server_major", lambda dsn: 17)
    monkeypatch.setattr(pc.shutil, "which", lambda name: None)

    with pytest.raises(dir_.InstalledRestoreRefused) as excinfo:
        dir_.run_installed_restore(NAME, archive, apply=apply, command_runner=stubs.runner)

    assert excinfo.value.message == (
        "pg_restore is not on PATH; install the PostgreSQL client tools "
        "(pg_dump and pg_restore)"
    )
    assert "postgresql-client-17" in excinfo.value.fix_hint
    assert stubs.events == []
    assert stubs.worker_running is True


_REAL_CLIENT_TOOLS_CHECK = getattr(diu, "_client_tools_check", None)


def test_apply_swaps_puts_back_the_receipt_and_leaves_the_worker_stopped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["ok"] is True, report
    assert report["status"] == "restored"
    assert stubs.events == [
        "dogfood down", "restore:okgdb_restore", "replay_access",
        "fingerprint:restored", "copy_access", "swap:okgdb_pre_restore",
        "timescaledb_workers", "fingerprint:live",
    ]
    assert "dogfood up" not in stubs.events
    receipt = json.loads(diu.receipt_path(NAME).read_text())
    assert receipt["okg_version"] == "0.2.0"
    aside = list(diu.state_dir(NAME).glob("installed-release.pre-restore-*.json"))
    assert len(aside) == 1 and json.loads(aside[0].read_text())["okg_version"] == "0.3.0"
    assert not diu.marker_path(NAME).exists()
    assert len(list(diu.state_dir(NAME).glob("upgrade-in-progress.pre-restore-*.json"))) == 1
    assert Path(report["record"]).is_file()
    assert "okg-0.2.0-py3-none-any.whl" in report["next_action"]
    assert f"okg deployment ready {NAME}" in report["next_action"]
    assert f"okg dogfood up --deployment {NAME} --apply" in report["next_action"]


def test_apply_without_previous_receipt_moves_the_receipt_aside(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch, previous_receipt=False)
    stubs = _Stubs(monkeypatch)
    stubs.worker_running = False
    diu.marker_path(NAME).unlink()
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["ok"] is True, report
    assert not diu.receipt_path(NAME).exists()
    receipt = next(step for step in report["steps"] if step["step"] == "receipt")
    assert "--from-contract" in receipt["note"]
    assert "dogfood up" not in report["next_action"]


def test_unmatched_restored_copy_is_never_swapped_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    stubs.restored_fingerprint = {**FINGERPRINT, "node_count": 14}
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["ok"] is False
    assert report["failed_step"] == "verify_restored"
    assert report["swapped"] is False
    assert not any(event.startswith("swap") for event in stubs.events)
    assert "okgdb was not changed" in report["next_action"]
    assert "kept for inspection" in report["next_action"]
    assert json.loads(diu.receipt_path(NAME).read_text())["okg_version"] == "0.3.0"


def test_restore_exception_is_reported_not_raised(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)

    def failing(*args, **kwargs):
        raise RuntimeError("pg_restore exited 1")

    monkeypatch.setattr("okg.substrate.postgres_clone.restore_database_dump", failing)
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["failed_step"] == "restore"
    assert report["steps"][-1]["error"]["type"] == "RuntimeError"
    assert report["swapped"] is False


def test_missing_sidecar_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    diu.sidecar_path(archive).unlink()
    _Stubs(monkeypatch)
    with pytest.raises(dir_.InstalledRestoreRefused) as excinfo:
        dir_.run_installed_restore(NAME, archive, apply=True)
    assert "no sidecar" in excinfo.value.message
    assert "okg db clone --restore-dump" in excinfo.value.fix_hint


def test_changed_archive_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    archive.write_bytes(b"PGDMP other bytes")
    _Stubs(monkeypatch)
    with pytest.raises(dir_.InstalledRestoreRefused) as excinfo:
        dir_.run_installed_restore(NAME, archive, apply=True)
    assert "sha256" in excinfo.value.data["reason"] or "size" in excinfo.value.data["reason"]


def test_archive_from_another_database_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    stubs.database = {**DATABASE, "system_identifier": "8"}
    with pytest.raises(dir_.InstalledRestoreRefused) as excinfo:
        dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert "different database" in excinfo.value.message
    assert stubs.events == []


def test_insufficient_rights_are_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    monkeypatch.setattr(dir_, "_database_access", lambda dsn, live, names: {
        "actor": "reader", "superuser": False, "owner": "okg",
        "sessions": [], "problems": ["reader lacks CREATEDB"],
    })
    with pytest.raises(dir_.InstalledRestoreRefused) as excinfo:
        dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert excinfo.value.data["problems"] == ["reader lacks CREATEDB"]
    assert stubs.events == []


def test_git_checkout_deployment_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(diu, "installed_upgrade_route", lambda deployment: None)
    with pytest.raises(dir_.InstalledRestoreRefused):
        dir_.run_installed_restore(NAME, archive, apply=False)


def test_derived_names_fit_postgres_identifiers() -> None:
    name = dir_._derived_name("d" * 63, "_pre_restore_20261006120000")
    assert len(name.encode("utf-8")) == 63
    assert name.endswith("_pre_restore_20261006120000")


def test_cli_routes_backup_restore(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    from okg.substrate.cli import main as cli_main

    seen: list[tuple[str, str, bool]] = []
    monkeypatch.setattr(
        dir_, "run_installed_restore",
        lambda deployment, archive, apply: seen.append((deployment, str(archive), apply))
        or {"ok": True, "status": "planned"},
    )
    archive = tmp_path / "a.dump"
    assert cli_main.main([
        "deployment", "backup", "restore", NAME, "--archive", str(archive), "--json",
    ]) == 0
    assert seen == [(NAME, str(archive), False)]
    assert json.loads(capsys.readouterr().out)["status"] == "planned"
    assert cli_main.main([
        "deployment", "backup", "restore", NAME, "--archive", str(archive), "--apply",
    ]) == 0
    assert seen[-1][2] is True


# ------------------------------------------- second review round of #3434

_REAL_FENCE_DRAIN_SWAP = dir_._fence_drain_swap


def test_archive_not_recorded_as_pre_upgrade_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MED-4: a backup taken by a resumed upgrade may hold part of it."""
    _, archive = _setup(tmp_path, monkeypatch, bless=False)
    stubs = _Stubs(monkeypatch)
    with pytest.raises(dir_.InstalledRestoreRefused) as excinfo:
        dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert "not recorded as the backup taken before the upgrade" in excinfo.value.message
    assert excinfo.value.data["applied_migrations"] == ["0001", "0040"]
    assert stubs.events == []


def test_marker_archive_is_accepted_and_its_worker_duty_carried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MED-3: the upgrade stopped the worker; the restore must say so."""
    _, archive = _setup(tmp_path, monkeypatch, bless=False)
    diu._write_marker(NAME, {
        "worker_stopped_by_upgrade": True, "pre_upgrade_archive": str(archive),
    })
    stubs = _Stubs(monkeypatch)
    stubs.worker_running = False
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["ok"] is True, report
    assert "dogfood down" not in stubs.events
    assert report["upgrade_stopped_worker"] is True
    assert report["worker_stopped"] is True
    assert f"okg dogfood up --deployment {NAME} --apply" in report["next_action"]
    record = json.loads(Path(report["record"]).read_text())
    assert record["worker_stopped"] is True


def test_access_mismatch_is_never_swapped_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HIGH-1: owners or grants that differ from the backup's record stop it."""
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    stubs.restored_access = {
        "digest": "sha256:" + "b" * 64, "rows": 1, "entries": [ACCESS["entries"][0]],
    }
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["failed_step"] == "verify_restored"
    failed = report["steps"][-1]
    assert "owners or privileges" in failed["reason"]
    assert failed["access_differences"]["missing"] == [ACCESS["entries"][1]]
    assert not any(event.startswith("swap") for event in stubs.events)


def test_sidecar_readable_by_others_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    diu.sidecar_path(archive).chmod(0o644)
    _Stubs(monkeypatch)
    with pytest.raises(dir_.InstalledRestoreRefused) as excinfo:
        dir_.run_installed_restore(NAME, archive, apply=False)
    assert "readable or writable by others" in excinfo.value.data["reason"]


def test_sidecar_without_access_fingerprint_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    payload = json.loads(diu.sidecar_path(archive).read_text())
    payload.pop("access_fingerprint")
    diu._write_private_json(diu.sidecar_path(archive), payload)
    _Stubs(monkeypatch)
    with pytest.raises(dir_.InstalledRestoreRefused) as excinfo:
        dir_.run_installed_restore(NAME, archive, apply=False)
    assert "no access fingerprint" in excinfo.value.data["reason"]


def test_damaged_previous_receipt_is_refused_before_anything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    payload = json.loads(diu.sidecar_path(archive).read_text())
    payload["previous_receipt"]["contract"]["sections"]["cli"]["extra"] = {}
    diu._write_private_json(diu.sidecar_path(archive), payload)
    stubs = _Stubs(monkeypatch)
    with pytest.raises(dir_.InstalledRestoreRefused) as excinfo:
        dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert "previous receipt cannot be put back" in excinfo.value.data["reason"]
    assert stubs.events == []


def test_failed_record_write_does_not_fail_a_finished_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    real_write = diu._write_private_json

    def write(path, payload):
        if "restores" in str(path):
            raise OSError("disk full")
        return real_write(path, payload)

    monkeypatch.setattr(diu, "_write_private_json", write)
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["ok"] is True
    assert report["status"] == "restored"
    assert report["record"] is None
    assert "disk full" in report["record_error"]["message"]


class _NullConnection:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _swap_harness(monkeypatch: pytest.MonkeyPatch, *, undo_fails: bool) -> dict:
    """The real _fence_drain_swap; the second rename fails, the undo maybe too."""
    seen: dict = {"renames": [], "allow": [], "reopen": None}
    monkeypatch.setattr(dir_, "_fence_drain_swap", _REAL_FENCE_DRAIN_SWAP)
    monkeypatch.setattr(
        "okg.substrate.db.connection.connect", lambda *a, **k: _NullConnection(),
    )
    monkeypatch.setattr(dir_, "_allow_connections", lambda conn, db: True)
    monkeypatch.setattr(
        dir_, "_set_allow", lambda conn, db, value: seen["allow"].append((db, value)),
    )
    monkeypatch.setattr(dir_, "_sessions", lambda conn, dbs: [])

    def rename(conn, old, new):
        seen["renames"].append((old, new))
        if len(seen["renames"]) == 2 or (undo_fails and len(seen["renames"]) == 3):
            raise RuntimeError(f"rename {old} failed")

    monkeypatch.setattr(dir_, "_rename", rename)

    def reopen(dsn, wanted):
        seen["reopen"] = dict(wanted)
        live, kept, restored = list(wanted)
        if undo_fails:
            return {
                live: {"exists": False},
                kept: {"exists": True, "allow_connections": True},
                restored: {"exists": True, "allow_connections": True},
            }
        return {
            live: {"exists": True, "allow_connections": True},
            kept: {"exists": False},
            restored: {"exists": True, "allow_connections": True},
        }

    monkeypatch.setattr(dir_, "_reopen_observed", reopen)
    return seen


def test_swap_failure_after_the_fence_reopens_and_reports_what_is_there(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MED-2: second rename and its undo both fail; nothing claims 'not changed'."""
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    seen = _swap_harness(monkeypatch, undo_fails=True)
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["failed_step"] == "swap"
    swap = report["steps"][-1]
    assert swap["error"]["type"] == "RuntimeError"
    assert seen["reopen"] is not None
    assert report["observed_databases"]["okgdb"] == {"exists": False}
    assert "no database is named okgdb now" in report["next_action"]
    assert "was not changed" not in report["next_action"]
    assert report["swapped"] is False


def test_swap_rename_failure_is_undone_and_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    seen = _swap_harness(monkeypatch, undo_fails=False)
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["failed_step"] == "swap"
    assert len(seen["renames"]) == 3
    assert seen["renames"][2][1] == "okgdb"
    assert "okgdb was not changed" in report["next_action"]
    assert ("okgdb", False) in seen["allow"]
    assert seen["reopen"]["okgdb"] is True


@pytest.mark.parametrize(
    ("session", "kind"),
    [
        ({"backend_type": "client backend", "application": "TimescaleDB x"}, "client"),
        ({"backend_type": "TimescaleDB Background Worker Scheduler"}, "timescaledb_worker"),
        ({"backend_type": "", "application": "TimescaleDB Background Worker Scheduler"},
         "background_worker"),
        ({"backend_type": "", "application": "psql", "superuser": True}, "background_worker"),
        ({"backend_type": "", "application": "", "superuser": False}, "background_worker"),
        ({"backend_type": "pg_cron launcher", "application": "pg_cron scheduler"},
         "background_worker"),
    ],
)
def test_session_kinds(session: dict, kind: str) -> None:
    assert dir_._session_kind(session) == kind


# -------------------------------------------- third review round of #3434


def test_failure_after_the_swap_leaves_the_receipt_describing_the_live_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """M3: a check that fails after the swap must not leave the upgraded receipt."""
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    stubs.live_fingerprint = {**FINGERPRINT, "node_count": 1}
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["failed_step"] == "verify_live"
    assert report["swapped"] is True
    assert [s["step"] for s in report["steps"]][-4:] == [
        "swap", "receipt", "timescaledb_workers", "verify_live",
    ]
    assert json.loads(diu.receipt_path(NAME).read_text())["okg_version"] == "0.2.0"
    assert not diu.marker_path(NAME).exists()
    assert "receipt from before the upgrade is back in place" in report["next_action"]
    assert "okgdb now holds the restored copy" in report["next_action"]


def test_unknown_original_connection_setting_is_never_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LOW: if the first connection fails, nothing is reopened blindly."""
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    seen = _swap_harness(monkeypatch, undo_fails=False)

    def no_connection(*args, **kwargs):
        raise OSError("connection refused")

    monkeypatch.setattr("okg.substrate.db.connection.connect", no_connection)
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["failed_step"] == "swap"
    assert set(seen["reopen"].values()) == {None}
    assert seen["renames"] == []


def test_a_timescaledb_worker_is_terminated_once(monkeypatch: pytest.MonkeyPatch) -> None:
    sessions = [
        [{"pid": 7, "kind": "timescaledb_worker"}],
        [{"pid": 7, "kind": "timescaledb_worker"}],
        [],
    ]
    terminated: list[int] = []
    monkeypatch.setattr(dir_, "_sessions", lambda conn, dbs: sessions.pop(0))
    monkeypatch.setattr(dir_, "_terminate", lambda conn, pid: terminated.append(pid))
    monkeypatch.setattr(dir_, "_rename", lambda conn, old, new: None)
    monkeypatch.setattr(dir_.time, "sleep", lambda seconds: None)
    result = dir_._drain_and_rename(
        None, {"step": "swap"}, "a", "b", "c", drain_timeout_seconds=30,
    )
    assert result["ok"] is True
    assert terminated == [7]
    assert len(result["terminated_timescaledb_workers"]) == 1


# -------------------------------------------- fourth review round of #3434


def test_receipt_failure_after_the_swap_still_restarts_timescaledb(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MEDIUM (round 4): the scheduler the swap ended must start again."""
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)

    def failing_receipt(deployment, sidecar, stamp):
        raise OSError("read-only state directory")

    monkeypatch.setattr(dir_, "_put_back_receipt", failing_receipt)
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["failed_step"] == "receipt"
    assert report["swapped"] is True
    assert [s["step"] for s in report["steps"]][-4:] == [
        "swap", "receipt", "timescaledb_workers", "verify_live",
    ]
    assert "timescaledb_workers" in stubs.events
    assert "TimescaleDB's scheduler is running again" in report["next_action"]
    assert "were NOT put back" in report["next_action"]


def test_every_failed_step_after_the_swap_is_named(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    stubs.live_fingerprint = {**FINGERPRINT, "node_count": 1}

    def failing_receipt(deployment, sidecar, stamp):
        raise OSError("read-only state directory")

    monkeypatch.setattr(dir_, "_put_back_receipt", failing_receipt)
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["failed_step"] == "receipt"
    assert report["failed_steps"] == ["receipt", "verify_live"]
    assert "these restore steps failed: receipt, verify_live" in report["next_action"]


def test_failure_before_the_swap_runs_nothing_after_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    stubs.restored_fingerprint = {**FINGERPRINT, "node_count": 1}
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["failed_step"] == "verify_restored"
    assert [s["step"] for s in report["steps"]][-1] == "verify_restored"
    assert "timescaledb_workers" not in stubs.events



def test_without_a_worker_service_the_next_action_says_to_start_it_by_hand(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, archive = _setup(tmp_path, monkeypatch)
    stubs = _Stubs(monkeypatch)
    stubs.service_available = False
    report = dir_.run_installed_restore(NAME, archive, apply=True, command_runner=stubs.runner)
    assert report["ok"] is True, report
    assert "dogfood down" in stubs.events
    assert f"okg runtime worker --deployment {NAME}" in report["next_action"]
    assert "okg dogfood up" not in report["next_action"]
