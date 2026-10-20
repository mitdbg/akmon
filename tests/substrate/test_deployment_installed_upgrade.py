"""`okg up` for a deployment run from an installed okg package (#3415).

No database: every database, subprocess and compatibility effect is a stub
that records what was called, so each test proves which steps ran and,
for the refusals, that nothing after the refusal ran.
"""

from __future__ import annotations

import json
import stat
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from okg.substrate import deployment_installed_upgrade as diu
from okg.substrate import deployment_upgrade as du
from okg.substrate.deployment_upgrade import CommandResult, UpgradeOptions


def _deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    release: dict[str, Any] | None,
    name: str = "installed-dep",
) -> tuple[str, Path]:
    root = tmp_path / "deployments"
    deployment_dir = root / name
    deployment_dir.mkdir(parents=True)
    manifest: dict[str, Any] = {"name": name}
    if release is not None:
        manifest["release"] = release
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump(manifest), encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "user-home"))
    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    return name, deployment_dir


def _forbid_git(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    calls: list[list[str]] = []
    real_init = subprocess.Popen.__init__

    def guarded(self, args, *rest, **kwargs):
        argv = [str(a) for a in args] if isinstance(args, (list, tuple)) else [str(args)]
        if argv and Path(argv[0]).name == "git":
            calls.append(argv)
            raise AssertionError(f"git subprocess started: {argv}")
        return real_init(self, args, *rest, **kwargs)

    monkeypatch.setattr(subprocess.Popen, "__init__", guarded)
    return calls


def _contract(version: str, verbs: tuple[str, ...] = ("up",)) -> dict[str, Any]:
    return {
        "schema": "okg.release_contract.v1",
        "okg_version": version,
        "sections": {"cli": {verb: {} for verb in verbs}},
    }


def _identity(version: str, payload: str | None = "sha256:" + "b" * 64) -> dict[str, Any]:
    contract = _contract(version)
    return {
        "okg_version": version,
        "payload_digest": payload,
        "installed_from_wheel": payload is not None,
        "schema_checksum": "sha256:" + "5" * 64,
        "contract_digest": diu._contract_digest(contract),
        "contract": contract,
    }


class _Harness:
    """Stubs for every effect; records the order in which they ran."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, new_version: str = "0.3.0"):
        self.events: list[str] = []
        self.runner_calls: list[list[str]] = []
        self.runner_envs: list[dict[str, str]] = []
        self.dsn = "postgresql://stub/db"
        self.database = {"database": "db", "port": 5432, "system_identifier": "7"}
        self.database_after_migrate: dict[str, Any] | None = None
        self.pending: list[str] = []
        self.pending_after_migrate: list[str] = []
        self.migrated = False
        self.raise_in: str | None = None
        self.compat: dict[str, Any] = {
            "verdict": "compatible", "exit_code": 0, "findings": [],
            "breaking_since": {
                "from_version": "0.2.0", "to_version": new_version,
                "breaking_count": 0, "affecting_this_deployment": [],
                "breaking": [],
            },
        }
        self.compat_calls: list[dict[str, Any]] = []
        self.worker_running = False
        self.backup_ok = True
        self.migrate_rc = 0
        self.ready_rc = 0
        self.apply_health: dict[str, Any] = {"ok": True, "status": "ready"}
        monkeypatch.setattr(
            diu, "current_installed_identity", lambda: _identity(new_version),
        )

        def compat(deployment, *, against=None, **kwargs):
            self.events.append("compat")
            seen = None
            if against is not None:
                seen = json.loads(Path(against).read_text(encoding="utf-8"))
            self.compat_calls.append({"deployment": deployment, "against": seen})
            return self.compat

        monkeypatch.setattr(
            "okg.substrate.deployment_compat.check_deployment_compat", compat,
        )
        monkeypatch.setattr(diu, "_resolve_dsn", lambda options: self.dsn)

        def identity(dsn):
            if self.migrated and self.database_after_migrate is not None:
                return self.database_after_migrate
            return self.database

        monkeypatch.setattr(diu, "_database_identity", identity)
        self.client_tools: dict[str, Any] = {
            "ok": True, "server_major": 17, "problems": [],
        }
        monkeypatch.setattr(
            diu, "_client_tools_check", lambda dsn, *a, **k: self.client_tools,
        )
        self.rights_problems: list[str] = []
        monkeypatch.setattr(diu, "restore_rights", lambda dsn, database: {
            "actor": "postgres", "superuser": not self.rights_problems,
            "owner": "postgres", "timescaledb": True,
            "problems": list(self.rights_problems),
        })
        monkeypatch.setattr(
            diu, "_pending_migrations",
            lambda dsn: self.pending_after_migrate if self.migrated else self.pending,
        )
        self.graph: dict[str, Any] = {
            "generation_id": "gen-7", "node_count": 14, "edge_count": 15,
            "published_generations": 1, "published_generation_ids": ["gen-7"],
        }
        self.graph_after: dict[str, Any] | None = None
        self.snapshots = 0

        def snapshot(dsn, deployment):
            self.snapshots += 1
            if self.snapshots > 1 and self.graph_after is not None:
                return self.graph_after
            return self.graph

        monkeypatch.setattr(diu, "_graph_snapshot", snapshot)
        self.ready_blockers_before: list[dict[str, Any]] = []
        self.ready_blockers_after: list[dict[str, Any]] = []
        self.ready_calls = 0

        def audit(deployment):
            return {
                "status": "single_executor" if self.worker_running else "not_observed",
                "root_count": 1 if self.worker_running else 0,
                "roots": [{"pid": 4242}] if self.worker_running else [],
            }

        monkeypatch.setattr(diu, "_worker_audit", audit)
        self.service_available = True
        self.template_writable = True
        self.adapter = "systemd"
        monkeypatch.setattr(diu, "worker_service", lambda deployment, **kwargs: {
            "adapter": self.adapter, "unit": f"okg-runtime-worker@{deployment}.service",
            "available": self.service_available,
            "self_installable": self.adapter == "launchd",
            "template": "/etc/systemd/system/okg-runtime-worker@.service",
            "template_writable": self.template_writable,
            "reason": None if self.service_available else "systemd unit is not enabled",
        })

        self.archive = "/backups/a.dump"

        def backup(options, deployment, deployment_dir, dsn, previous, identity, **kwargs):
            self.backup_kwargs = kwargs
            self.events.append("backup")
            if self.raise_in == "backup":
                raise RuntimeError("backup exploded")
            if not self.backup_ok:
                return {"step": "backup", "ok": False, "reason": "pg_dump failed"}
            return {"step": "backup", "ok": True, "archive": self.archive}

        monkeypatch.setattr(diu, "_step_backup", backup)

        def health(**kwargs):
            self.events.append("apply_health")
            if self.raise_in == "deployment_apply":
                raise RuntimeError("apply exploded")
            return self.apply_health

        monkeypatch.setattr(
            "okg.substrate.deployment_apply.deployment_apply_health", health,
        )

    def runner(self, argv: list[str], env: dict[str, str]) -> CommandResult:
        self.runner_calls.append(list(argv))
        self.runner_envs.append(dict(env))
        tail = argv[argv.index("-m") + 2:] if "-m" in argv else argv
        verb = " ".join(tail[:2])
        self.events.append(verb)
        if self.raise_in == "restart_worker" and tail[:2] == ["dogfood", "up"]:
            raise RuntimeError("dogfood up exploded")
        if self.raise_in == "migrate" and tail[:1] == ["migrate"]:
            raise RuntimeError("migrate exploded")
        if (
            self.raise_in == "readiness"
            and tail[:2] == ["deployment", "ready"]
            and self.ready_calls
        ):
            raise RuntimeError("ready exploded")
        if tail[:2] == ["dogfood", "down"]:
            self.worker_running = False
            return CommandResult(0, "{}")
        if tail[:2] == ["dogfood", "up"]:
            self.worker_running = True
            return CommandResult(0, "{}")
        if tail[:1] == ["migrate"]:
            self.migrated = self.migrate_rc == 0
            return CommandResult(self.migrate_rc, '{"ok": true}', "boom")
        if tail[:2] == ["deployment", "apply"]:
            if "--apply" in tail:
                return CommandResult(0, '{"ok": true, "applied": true}')
            return CommandResult(0, '{"plan_id": "plan-1"}')
        if tail[:2] == ["deployment", "ready"]:
            self.ready_calls += 1
            blockers = (
                self.ready_blockers_before if self.ready_calls == 1
                else self.ready_blockers_after
            )
            rc = 1 if blockers else (0 if self.ready_calls == 1 else self.ready_rc)
            return CommandResult(rc, json.dumps({"ok": not blockers, "blockers": blockers}))
        raise AssertionError(argv)


def _write_receipt(deployment_dir: Path, version: str = "0.2.0") -> None:
    diu.write_installed_release_receipt(
        deployment_dir.name,
        deployment_dir,
        written_by="install",
        identity=_identity(version, payload="sha256:" + "a" * 64),
    )


# ------------------------------------------------------------------ routing


@pytest.mark.parametrize("kind", ["standalone", "distribution"])
def test_route_declared_installed_kinds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": kind},
    )
    route = diu.installed_upgrade_route(name)
    assert route == {
        "release_kind": kind,
        "decided_by": "declared",
        "deployment_dir": str(deployment_dir),
    }


def test_route_detects_wheel_when_no_framework_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(tmp_path, monkeypatch, {"channel": "dev"})

    def no_checkout(path):
        raise du.RevisionAuthorityError(
            authority_role="framework", repository=None,
            requested_revision=None, resolved_revision=None,
            operation="discover_framework_repository", reason="wheel",
        )

    monkeypatch.setattr(du, "_resolve_repo_authorities", no_checkout)
    monkeypatch.setattr(
        "okg.substrate.distributions.installed_framework."
        "installed_framework_distribution",
        lambda: SimpleNamespace(payload_digest="sha256:" + "c" * 64),
    )
    route = diu.installed_upgrade_route(name)
    assert route == {
        "release_kind": "distribution",
        "decided_by": "detected_installed_wheel",
        "deployment_dir": str(deployment_dir),
    }


def test_route_keeps_git_path_without_wheel_or_with_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, _ = _deployment(tmp_path, monkeypatch, {"channel": "dev"})

    def no_checkout(path):
        raise du.RevisionAuthorityError(
            authority_role="framework", repository=None,
            requested_revision=None, resolved_revision=None,
            operation="discover_framework_repository", reason="none",
        )

    monkeypatch.setattr(du, "_resolve_repo_authorities", no_checkout)
    monkeypatch.setattr(
        "okg.substrate.distributions.installed_framework."
        "installed_framework_distribution",
        lambda: None,
    )
    assert diu.installed_upgrade_route(name) is None
    monkeypatch.setattr(du, "_resolve_repo_authorities", lambda path: (path, path))
    assert diu.installed_upgrade_route(name) is None


@pytest.mark.parametrize(
    "release",
    [
        {"channel": "prod", "kind": "packaged"},
        {"channel": "dev", "pin": {"code_repository": "../okg"}},
    ],
)
def test_route_never_takes_packaged_or_explicit_repository(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release: dict[str, Any],
) -> None:
    name, _ = _deployment(tmp_path, monkeypatch, release)
    monkeypatch.setattr(
        du, "_resolve_repo_authorities",
        lambda path: (_ for _ in ()).throw(AssertionError("not consulted")),
    )
    assert diu.installed_upgrade_route(name) is None


def test_okg_up_routes_distribution_before_git_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Before #3415 a distribution deployment failed in `_git_target_revision`."""
    name, _ = _deployment(tmp_path, monkeypatch, {"channel": "dev", "kind": "distribution"})
    git_calls = _forbid_git(monkeypatch)
    for attr in ("_build_context", "_git_target_revision", "_resolve_repo_authorities"):
        monkeypatch.setattr(du, attr, lambda *a, **k: (_ for _ in ()).throw(AssertionError(attr)))
    seen: list[dict[str, Any]] = []
    monkeypatch.setattr(
        diu, "run_installed_upgrade",
        lambda options, route, command_runner=None: seen.append(dict(route)) or {"ok": True},
    )
    assert du.run_deployment_upgrade(UpgradeOptions(deployment=name)) == {"ok": True}
    assert seen[0]["release_kind"] == "distribution"
    assert git_calls == []


def test_git_deployment_refuses_installed_only_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, _ = _deployment(tmp_path, monkeypatch, {"channel": "dev"})
    monkeypatch.setattr(diu, "installed_upgrade_route", lambda deployment: None)
    monkeypatch.setattr(
        du, "_build_context",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("context built")),
    )
    with pytest.raises(du.SubstrateError) as excinfo:
        du.run_deployment_upgrade(
            UpgradeOptions(deployment=name, accept_unknown_compat=True),
        )
    assert "apply only to an installed deployment" in excinfo.value.message


# ------------------------------------------------------------------ upgrade


def _run(name: str, harness: _Harness, **overrides: Any) -> dict[str, Any]:
    route = diu.installed_upgrade_route(name)
    assert route is not None
    return diu.run_installed_upgrade(
        UpgradeOptions(deployment=name, **overrides), route,
        command_runner=harness.runner,
    )


def test_upgrade_runs_every_step_in_order_and_writes_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir, "0.2.0")
    harness = _Harness(monkeypatch)
    harness.worker_running = True
    harness.apply_health = {"ok": False, "problems": ["stage_needed:catalog_apply"]}

    report = _run(name, harness)

    assert report["ok"] is True, report
    assert report["status"] == "upgraded"
    assert harness.events == [
        "compat", "deployment ready", "dogfood down", "backup",
        "migrate --deployment", "apply_health", "deployment apply",
        "deployment apply", "dogfood up", "deployment ready",
    ]
    assert [s["step"] for s in report["steps"]] == list(diu.STEPS)
    assert harness.compat_calls[0]["against"] == _contract("0.2.0")
    receipt = diu.read_installed_release_receipt(name, deployment_dir)
    assert receipt["okg_version"] == "0.3.0"
    assert receipt["written_by"] == "upgrade"
    assert receipt["previous"]["okg_version"] == "0.2.0"
    assert receipt["upgrade"]["backup_archive"] == "/backups/a.dump"
    assert receipt["upgrade"]["compat_override"] is None
    path = diu.receipt_path(name)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    apply_call = harness.runner_calls[-3]
    assert apply_call[-4:] == ["--apply", "--confirm", "plan-1", "--json"]
    # Everything lives in the private state directory, not the deployment's.
    assert path.parent == tmp_path / "user-home" / "state" / "deployments" / name
    assert not (deployment_dir / ".okg").exists()
    assert receipt["deployment_dir"] == str(deployment_dir)
    assert receipt["upgrade"]["pre_upgrade_archive"] == "/backups/a.dump"
    assert not diu.marker_path(name).exists()
    assert report["resumed"] is False
    assert report["database"] == harness.database


def test_rerun_after_success_is_up_to_date_and_touches_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    harness = _Harness(monkeypatch)
    diu.write_installed_release_receipt(
        name, deployment_dir, written_by="install",
        identity=diu.current_installed_identity(),
    )
    report = _run(name, harness)
    assert report["status"] == "up_to_date"
    assert harness.events == []
    assert harness.runner_calls == []


@pytest.mark.parametrize("cause", ["blocker", "affecting"])
def test_compat_blocker_refuses_before_stop_backup_or_migrate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cause: str,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir, "0.2.0")
    harness = _Harness(monkeypatch)
    harness.worker_running = True
    if cause == "blocker":
        harness.compat.update({
            "verdict": "incompatible", "exit_code": 1,
            "findings": [{
                "code": "compat.sources.class_missing", "severity": "blocker",
                "message": "source class Foo no longer exists",
            }],
        })
    else:
        # A breaking change touching this deployment blocks even when the
        # compat verdict itself says compatible (issue #3415 decision 2).
        harness.compat["breaking_since"]["affecting_this_deployment"] = [
            {"section": "cli", "item": "ingest --old-flag", "change": "removed"},
        ]

    with pytest.raises(diu.InstalledUpgradeIncompatible) as excinfo:
        _run(name, harness)

    error = excinfo.value.to_json()["error"]
    assert error["code"] == "installed_upgrade_incompatible"
    assert "nothing was changed" in error["message"]
    if cause == "blocker":
        assert "source class Foo no longer exists" in error["message"]
    else:
        assert "ingest --old-flag (removed)" in error["message"]
    assert "okg-0.2.0-py3-none-any.whl" in error["fix_hint"]
    assert error["data"]["release_notes"] == (
        "https://github.com/mitdbg/akmon/releases/tag/v0.3.0"
    )
    assert harness.events == ["compat"]
    assert harness.runner_calls == []
    assert harness.worker_running is True
    assert diu.read_installed_release_receipt(name, deployment_dir)["okg_version"] == "0.2.0"


def test_unknown_compat_without_receipt_refuses_before_anything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    harness = _Harness(monkeypatch)
    harness.compat.pop("breaking_since")
    with pytest.raises(diu.InstalledUpgradeCompatUnknown) as excinfo:
        _run(name, harness)
    error = excinfo.value.to_json()["error"]
    assert error["code"] == "installed_upgrade_compat_unknown"
    assert "no installed-release receipt" in error["message"]
    assert "--from-contract" in error["fix_hint"]
    assert "--accept-unknown-compat" in error["fix_hint"]
    assert harness.events == ["compat"]
    assert harness.runner_calls == []
    assert not diu.receipt_path(name).exists()


def test_unknown_compat_check_refuses_even_with_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.compat.update({
        "verdict": "unknown", "exit_code": 2,
        "findings": [{
            "code": "compat.sources.could_not_run", "severity": "unknown",
            "message": "the sources check could not run",
        }],
    })
    with pytest.raises(diu.InstalledUpgradeCompatUnknown):
        _run(name, harness)
    assert harness.events == ["compat"]


def test_accept_unknown_compat_proceeds_loudly_and_is_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    harness = _Harness(monkeypatch)
    harness.compat.pop("breaking_since")
    report = _run(name, harness, accept_unknown_compat=True)
    assert report["ok"] is True, report
    assert "WARNING: --accept-unknown-compat" in capsys.readouterr().err
    receipt = diu.read_installed_release_receipt(name, deployment_dir)
    assert receipt["upgrade"]["compat_override"]["flag"] == "--accept-unknown-compat"
    assert "previous" not in receipt


def test_accept_unknown_compat_never_overrides_a_blocker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, _ = _deployment(tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"})
    harness = _Harness(monkeypatch)
    harness.compat.update({
        "verdict": "incompatible", "exit_code": 1,
        "findings": [{"code": "compat.lint.x", "severity": "blocker", "message": "bad"}],
    })
    with pytest.raises(diu.InstalledUpgradeIncompatible):
        _run(name, harness, accept_unknown_compat=True)
    assert harness.events == ["compat"]


def test_from_contract_supplies_the_previous_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, _ = _deployment(tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"})
    harness = _Harness(monkeypatch)
    contract_file = tmp_path / "okg-contract-v0.2.0.json"
    contract_file.write_text(json.dumps(_contract("0.2.0", ("up", "ingest"))), encoding="utf-8")
    report = _run(name, harness, from_contract=contract_file)
    assert report["ok"] is True, report
    assert harness.compat_calls[0]["against"] == _contract("0.2.0", ("up", "ingest"))
    assert report["steps"][0]["contract_source"] == f"--from-contract {contract_file}"
    receipt = diu.read_installed_release_receipt(name, tmp_path / "deployments" / name)
    assert receipt["previous"]["okg_version"] == "0.2.0"
    assert receipt["previous"]["source"] == f"--from-contract {contract_file}"


def test_backup_failure_stops_before_migrate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.worker_running = True
    harness.backup_ok = False
    report = _run(name, harness)
    assert report["ok"] is False
    assert report["failed_step"] == "backup"
    assert harness.events == ["compat", "deployment ready", "dogfood down", "backup"]
    assert not any("migrate" in " ".join(c) for c in harness.runner_calls)
    assert "the database was not changed" in report["next_action"]
    assert "okg dogfood up --deployment" in report["next_action"]
    assert diu.read_installed_release_receipt(name, deployment_dir)["okg_version"] == "0.2.0"


def test_real_backup_step_stops_before_migrate_when_pg_dump_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real `_step_backup`, with a failing dump, still stops the run."""
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    monkeypatch.setattr(diu, "_step_backup", _REAL_STEP_BACKUP)

    def failing_dump(dsn, path, **kwargs):
        raise du.SubstrateError("required PostgreSQL clone tool is missing")

    monkeypatch.setattr(diu, "_create_backup_dump", failing_dump)
    report = _run(name, harness, backup_dir=tmp_path / "backups")
    assert report["failed_step"] == "backup"
    assert report["steps"][-1]["reason"] == "pg_dump failed"
    assert harness.events == ["compat", "deployment ready"]


_REAL_STEP_BACKUP = diu._step_backup
_REAL_CLIENT_TOOLS_CHECK = getattr(diu, "_client_tools_check", None)


def _no_client_tools(monkeypatch: pytest.MonkeyPatch, harness: "_Harness") -> None:
    """The real tool check on a host with neither pg_dump nor pg_restore."""
    from okg.substrate import postgres_clone as pc

    monkeypatch.setattr(diu, "_client_tools_check", _REAL_CLIENT_TOOLS_CHECK)
    monkeypatch.setattr(pc, "server_major", lambda dsn: 17)
    monkeypatch.setattr(pc.shutil, "which", lambda name: None)


def test_plan_reports_missing_client_tools_before_the_real_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #3587: `okg up --plan` passed, then the run stopped at backup."""
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    _no_client_tools(monkeypatch, harness)

    report = _run(name, harness, plan_only=True, apply_services=False,
                  sync_code=False, sync_deps=False)

    assert report["ok"] is False
    assert report["status"] == "blocked"
    assert report["text"] == (
        f"plan: okg up {name} would stop at its backup step: pg_dump and "
        "pg_restore are not on PATH; install the PostgreSQL client tools "
        "(pg_dump and pg_restore); no change made"
    )
    assert "postgresql-client-17" in report["next_action"]
    assert [p["tool"] for p in report["client_tools"]["problems"]] == [
        "pg_dump", "pg_restore",
    ]
    assert harness.events == ["compat"]
    assert harness.runner_calls == []


def test_missing_client_tools_stop_the_run_before_the_worker_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.worker_running = True
    _no_client_tools(monkeypatch, harness)

    report = _run(name, harness)

    assert report["failed_step"] == "preflight"
    assert report["steps"][-1]["reason"].startswith(
        "pg_dump and pg_restore are not on PATH"
    )
    assert harness.events == ["compat"]
    assert harness.worker_running is True
    assert report["next_action"].startswith("install the PostgreSQL client tools")
    assert "the database was not changed" in report["next_action"]


def test_plan_without_a_resolved_database_says_the_version_was_not_checked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate import postgres_clone as pc

    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    _no_client_tools(monkeypatch, harness)

    def unresolved(options):
        raise diu.InstalledUpgradeRefused("deployment database is unresolved")

    monkeypatch.setattr(diu, "_resolve_dsn", unresolved)
    monkeypatch.setattr(pc, "server_major", lambda dsn: pytest.fail("no DSN to ask"))
    report = _run(name, harness, plan_only=True, apply_services=False,
                  sync_code=False, sync_deps=False)

    assert report["status"] == "blocked"
    assert report["client_tools"]["version_checked"] is False
    assert report["client_tools"]["version_skipped"] == (
        "the database is unresolved: deployment database is unresolved"
    )


def test_a_backup_that_still_misses_a_tool_names_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The backup step's report keeps the tool and the install hint."""
    from okg.substrate import postgres_clone as pc

    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    monkeypatch.setattr(diu, "_step_backup", _REAL_STEP_BACKUP)
    monkeypatch.setattr(pc.shutil, "which", lambda name: None)

    def dump(dsn, path, **kwargs):
        pc._tool("pg_dump")

    monkeypatch.setattr(diu, "_create_backup_dump", dump)
    report = _run(name, harness, backup_dir=tmp_path / "backups")

    error = report["steps"][-1]["error"]
    assert report["failed_step"] == "backup"
    assert error["tool"] == "pg_dump"
    assert error["message"].startswith("pg_dump is not on PATH")
    assert report["next_action"].startswith("install the PostgreSQL client tools")


def test_migrate_failure_names_the_backup_and_how_to_go_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.migrate_rc = 1
    report = _run(name, harness)
    assert report["failed_step"] == "migrate"
    assert report["backup_archive"] == "/backups/a.dump"
    assert "okg deployment backup restore" in report["next_action"]
    assert "okg-0.2.0-py3-none-any.whl" in report["next_action"]
    assert "deployment apply" not in harness.events


def test_new_readiness_blocker_fails_the_upgrade_and_keeps_old_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.ready_blockers_before = [{"id": "mcp.graph_read"}]
    harness.ready_blockers_after = [{"id": "mcp.graph_read"}, {"id": "queues.dlq"}]
    report = _run(name, harness)
    assert report["failed_step"] == "readiness"
    failed = report["steps"][-1]
    assert failed["new_blockers"] == [diu._finding_key({"id": "queues.dlq"})]
    assert failed["pre_existing_blockers"] == [diu._finding_key({"id": "mcp.graph_read"})]
    assert diu.read_installed_release_receipt(name, deployment_dir)["okg_version"] == "0.2.0"


def test_pre_existing_readiness_blocker_is_reported_not_hidden(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.ready_blockers_before = [{"id": "mcp.graph_read"}]
    harness.ready_blockers_after = [{"id": "mcp.graph_read"}]
    report = _run(name, harness)
    assert report["ok"] is True, report
    readiness = report["steps"][-2]
    assert readiness["step"] == "readiness"
    assert readiness["pre_existing_blockers"] == [diu._finding_key({"id": "mcp.graph_read"})]
    assert "new_blockers" not in readiness


@pytest.mark.parametrize(
    ("after", "reason"),
    [
        ({"generation_id": "gen-7", "node_count": 13, "edge_count": 15,
          "published_generations": 1, "published_generation_ids": ["gen-7"]},
         "generation gen-7 now reads 13 nodes / 15 edges, was 14 / 15"),
        ({"generation_id": None, "node_count": None, "edge_count": None,
          "published_generations": 0, "published_generation_ids": []},
         "published generation gen-7 is gone"),
    ],
)
def test_graph_must_survive_the_upgrade(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    after: dict[str, Any], reason: str,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.graph_after = after
    report = _run(name, harness)
    assert report["failed_step"] == "readiness"
    assert report["steps"][-1]["reason"] == reason


def test_plan_mode_checks_compat_and_changes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    report = _run(name, harness, plan_only=True, apply_services=False,
                  sync_code=False, sync_deps=False)
    assert report["status"] == "planned"
    assert report["planned_steps"] == list(diu.STEPS[1:])
    assert harness.events == ["compat"]
    assert harness.runner_calls == []


@pytest.mark.parametrize(
    ("overrides", "flag"),
    [
        ({"skip_backup": True}, "--skip-backup"),
        ({"profile": "dev"}, "--target dev"),
        ({"include": ("code",)}, "--include"),
        ({"apply_services": False}, "--no-apply-services"),
    ],
)
def test_git_only_options_are_refused_not_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, Any], flag: str,
) -> None:
    name, _ = _deployment(tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"})
    harness = _Harness(monkeypatch)
    with pytest.raises(diu.InstalledUpgradeRefused) as excinfo:
        _run(name, harness, **overrides)
    assert excinfo.value.data["options"] == [flag]
    assert harness.events == []


def test_damaged_receipt_refuses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    path = diu.receipt_path(name)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["contract"]["sections"]["cli"]["extra"] = {}
    path.write_text(json.dumps(payload), encoding="utf-8")
    harness = _Harness(monkeypatch)
    with pytest.raises(diu.InstalledReceiptUnreadable) as excinfo:
        _run(name, harness)
    assert excinfo.value.data["reason"] == "contract does not match contract_digest"
    assert harness.events == []


def test_record_install_receipt_writes_for_installed_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    monkeypatch.setattr(diu, "current_installed_identity", lambda: _identity("0.2.0"))
    outcome = diu.record_install_receipt(name)
    assert outcome == {"written": True, "path": str(diu.receipt_path(name))}
    assert diu.read_installed_release_receipt(name, deployment_dir)["written_by"] == "install"
    other, other_dir = _deployment(tmp_path / "x", monkeypatch, {"channel": "dev"}, "git-dep")
    monkeypatch.setattr(diu, "installed_upgrade_route", lambda deployment: None)
    assert diu.record_install_receipt(other)["written"] is False
    assert not diu.receipt_path(other).exists()


def test_cli_maps_installed_upgrade_flags(tmp_path: Path) -> None:
    from okg.substrate.cli.deployment import _upgrade_options_from_args
    from okg.substrate.cli.main import _build_parser as build_parser

    contract = tmp_path / "c.json"
    args = build_parser().parse_args(
        ["up", "dep", "--from-contract", str(contract), "--accept-unknown-compat"],
    )
    options = _upgrade_options_from_args(args)
    assert options.from_contract == contract
    assert options.accept_unknown_compat is True
    args = build_parser().parse_args(["deployment", "up", "dep"])
    options = _upgrade_options_from_args(args)
    assert options.from_contract is None
    assert options.accept_unknown_compat is False


# ------------------------------------------------- fix round (review of #3434)


@pytest.mark.parametrize(
    ("overrides", "flag"),
    [
        ({"dsn": "postgresql://elsewhere/db"}, "--dsn"),
        ({"backup_keep": 2}, "--backup-keep"),
        ({"state_file": Path("s.json")}, "--state-file"),
        ({"resume": False}, "--no-resume"),
        ({"reset_state": True}, "--reset-state"),
    ],
)
def test_options_that_could_split_or_mislead_are_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, Any], flag: str,
) -> None:
    name, _ = _deployment(tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"})
    harness = _Harness(monkeypatch)
    with pytest.raises(diu.InstalledUpgradeRefused) as excinfo:
        _run(name, harness, **overrides)
    assert excinfo.value.data["options"] == [flag]
    assert harness.events == []


@pytest.mark.parametrize("manifest_dsn", [None, "${INSTALLED_DEP_DSN}"])
def test_every_child_runs_against_the_resolved_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, manifest_dsn: str | None,
) -> None:
    """HIGH1: a literal or ${VAR} manifest, and a parent env pointing elsewhere."""
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    if manifest_dsn is not None:
        manifest = yaml.safe_load((deployment_dir / "deployment.yaml").read_text())
        manifest["postgres"] = {"dsn": manifest_dsn}
        (deployment_dir / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    monkeypatch.setenv("OKG_DSN", "postgresql://elsewhere/other")
    monkeypatch.setenv("INSTALLED_DEP_DSN", "postgresql://elsewhere/other")
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.worker_running = True
    harness.apply_health = {"ok": False, "problems": ["x"]}
    report = _run(name, harness)
    assert report["ok"] is True, report
    assert len(harness.runner_envs) == 7
    for env in harness.runner_envs:
        assert env["OKG_DSN"] == harness.dsn
        if manifest_dsn is not None:
            assert env["INSTALLED_DEP_DSN"] == harness.dsn


def test_migrate_that_left_this_database_pending_fails_and_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.pending_after_migrate = ["0041_blob_store"]
    report = _run(name, harness)
    assert report["failed_step"] == "migrate"
    assert report["steps"][-1]["pending_after"] == ["0041_blob_store"]
    assert "different database" in report["steps"][-1]["reason"]
    assert "apply_health" not in harness.events
    assert harness.events[-1] == "migrate --deployment"


def test_database_identity_change_stops_before_apply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.database_after_migrate = {**harness.database, "system_identifier": "8"}
    report = _run(name, harness)
    assert report["failed_step"] == "deployment_apply"
    assert "database changed during the upgrade" in report["steps"][-1]["reason"]
    assert "apply_health" not in harness.events


@pytest.mark.parametrize(
    "step", ["backup", "migrate", "deployment_apply", "restart_worker", "readiness"],
)
def test_an_exception_in_a_step_still_produces_a_full_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, step: str,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.worker_running = True
    harness.raise_in = step
    report = _run(name, harness)
    assert report["ok"] is False
    assert report["failed_step"] == step
    failed = report["steps"][-1]
    assert failed["error"]["type"] == "RuntimeError"
    assert "exploded" in failed["error"]["message"]
    for key in ("backup_archive", "pre_upgrade_archive", "worker_stopped", "next_action"):
        assert key in report
    assert report["worker_stopped"] is (step != "readiness")
    if step != "backup":
        assert report["pre_upgrade_archive"] == "/backups/a.dump"
        assert "okg deployment backup restore" in report["next_action"]
    assert diu.read_installed_release_receipt(name, deployment_dir)["okg_version"] == "0.2.0"


def test_rerun_after_failure_inherits_restart_and_pre_upgrade_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MED c: run 1 stops the worker and fails; run 2 must still restart it."""
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.worker_running = True
    harness.migrate_rc = 1
    first = _run(name, harness)
    assert first["failed_step"] == "migrate"
    assert first["worker_stopped"] is True
    marker = diu.read_upgrade_marker(name)
    assert marker["worker_stopped_by_upgrade"] is True
    assert marker["pre_upgrade_archive"] == "/backups/a.dump"
    assert stat.S_IMODE(diu.marker_path(name).stat().st_mode) == 0o600

    second_harness = _Harness(monkeypatch)
    second_harness.archive = "/backups/b.dump"
    assert second_harness.worker_running is False
    second = _run(name, second_harness)
    assert second["ok"] is True, second
    assert second["resumed"] is True
    assert "dogfood up" in second_harness.events
    assert "dogfood down" not in second_harness.events
    # The "before" of the run that started the upgrade is kept.
    assert second_harness.ready_calls == 1
    receipt = diu.read_installed_release_receipt(name, deployment_dir)
    assert receipt["upgrade"]["pre_upgrade_archive"] == "/backups/a.dump"
    assert receipt["upgrade"]["backup_archive"] == "/backups/b.dump"
    assert not diu.marker_path(name).exists()


def test_matching_receipt_with_pending_migrations_runs_the_full_upgrade(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    harness = _Harness(monkeypatch)
    diu.write_installed_release_receipt(
        name, deployment_dir, written_by="install",
        identity=diu.current_installed_identity(),
    )
    harness.pending = ["0041_blob_store"]
    report = _run(name, harness)
    assert report["status"] == "upgraded", report
    assert report["receipt_matches_but_pending"] == ["0041_blob_store"]
    assert "migrate --deployment" in harness.events


def test_different_schema_checksum_is_not_up_to_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    harness = _Harness(monkeypatch)
    same = diu.current_installed_identity()
    diu.write_installed_release_receipt(
        name, deployment_dir, written_by="install",
        identity={**same, "schema_checksum": "sha256:" + "6" * 64},
    )
    report = _run(name, harness)
    assert report["status"] == "upgraded"


def test_downgrade_is_refused_and_points_at_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    diu.write_installed_release_receipt(
        name, deployment_dir, written_by="upgrade",
        identity=_identity("0.4.0"),
        upgrade={"pre_upgrade_archive": "/state/backups/x.dump"},
    )
    harness = _Harness(monkeypatch, new_version="0.3.0")
    with pytest.raises(diu.InstalledUpgradeRefused) as excinfo:
        _run(name, harness)
    error = excinfo.value.to_json()["error"]
    assert "older than okg 0.4.0" in error["message"]
    assert (
        f"okg deployment backup restore {name} --archive /state/backups/x.dump"
        in error["fix_hint"]
    )
    assert harness.events == []


def test_receipt_for_another_deployment_directory_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, _ = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    diu.write_installed_release_receipt(
        name, tmp_path / "somewhere-else", written_by="install",
        identity=_identity("0.2.0"),
    )
    harness = _Harness(monkeypatch)
    with pytest.raises(diu.InstalledReceiptUnreadable) as excinfo:
        _run(name, harness)
    assert "somewhere-else" in excinfo.value.data["reason"]
    assert harness.events == []


def test_install_keeps_a_receipt_for_a_different_okg(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir, "0.2.0")
    monkeypatch.setattr(diu, "current_installed_identity", lambda: _identity("0.3.0"))
    outcome = diu.record_install_receipt(name)
    assert outcome["written"] is False
    assert "kept the existing installed-release receipt" in outcome["warning"]
    assert diu.read_installed_release_receipt(name, deployment_dir)["okg_version"] == "0.2.0"


def test_backup_dir_tracked_by_git_is_refused_and_ignored_is_allowed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    work = tmp_path / "work"
    work.mkdir()
    subprocess.run(["git", "init", "-q", str(work)], check=True)
    (work / ".gitignore").write_text("ignored/\n", encoding="utf-8")
    harness = _Harness(monkeypatch)
    with pytest.raises(diu.InstalledUpgradeRefused) as excinfo:
        _run(name, harness, backup_dir=work / "dumps")
    assert excinfo.value.data["work_tree"] == str(work.resolve())
    assert harness.events == []
    report = _run(name, harness, backup_dir=work / "ignored" / "dumps")
    assert report["ok"] is True, report


def test_readiness_compares_causes_not_only_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MED e: the same check id with a changed cause fails the step."""
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.ready_blockers_before = [{
        "id": "mcp.role", "status": "blocker", "message": "role lacks LOGIN",
        "details": {"role": "okg_mcp", "reason": "nologin"},
    }]
    harness.ready_blockers_after = [{
        "id": "mcp.role", "status": "blocker", "message": "role lacks LOGIN",
        "details": {"role": "okg_mcp", "reason": "password_missing"},
    }]
    report = _run(name, harness)
    assert report["failed_step"] == "readiness"
    readiness = report["steps"][-1]
    assert len(readiness["new_blockers"]) == 1
    assert "password_missing" in readiness["new_blockers"][0]

    key = diu._finding_key({"id": "probe", "status": "unknown", "message": "took 31s"})
    assert key == diu._finding_key({"id": "probe", "status": "unknown", "message": "took 4s"})


def test_new_unknown_fails_and_pre_existing_unknown_is_listed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    old = {"id": "queues.age", "status": "unknown", "message": "not measured"}
    new = {"id": "mcp.read", "status": "unknown", "message": "probe timed out"}
    calls = {"n": 0}

    def runner(argv, env):
        if "ready" in argv:
            calls["n"] += 1
            unknowns = [old] if calls["n"] == 1 else [old, new]
            return CommandResult(1, json.dumps({"blockers": [], "unknowns": unknowns}))
        return harness.runner(argv, env)

    route = diu.installed_upgrade_route(name)
    report = diu.run_installed_upgrade(
        UpgradeOptions(deployment=name), route, command_runner=runner,
    )
    assert report["failed_step"] == "readiness"
    readiness = report["steps"][-1]
    assert readiness["new_unknowns"] == [diu._finding_key(new)]
    assert readiness["pre_existing_unknowns"] == [diu._finding_key(old)]


def test_unreadable_readiness_before_makes_every_finding_new(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    calls = {"n": 0}

    def runner(argv, env):
        if "ready" in argv:
            calls["n"] += 1
            if calls["n"] == 1:
                return CommandResult(1, "not json")
            return CommandResult(1, json.dumps({"blockers": [{"id": "mcp.role"}]}))
        return harness.runner(argv, env)

    route = diu.installed_upgrade_route(name)
    report = diu.run_installed_upgrade(
        UpgradeOptions(deployment=name), route, command_runner=runner,
    )
    assert report["failed_step"] == "readiness"
    assert report["steps"][-1]["readiness_before_unmeasured"] is True
    assert report["steps"][-1]["new_blockers"] == [diu._finding_key({"id": "mcp.role"})]


def test_from_contract_over_a_receipt_is_announced_and_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir, "0.2.0")
    harness = _Harness(monkeypatch)
    contract_file = tmp_path / "c.json"
    contract_file.write_text(json.dumps(_contract("0.1.0", ("up", "x"))), encoding="utf-8")
    report = _run(name, harness, from_contract=contract_file)
    assert report["ok"] is True, report
    assert "overrides the receipt's contract" in capsys.readouterr().err
    receipt = diu.read_installed_release_receipt(name, deployment_dir)
    assert receipt["upgrade"]["contract_override"]["overrode_receipt_version"] == "0.2.0"


def test_from_contract_equal_to_the_installed_contract_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, _ = _deployment(tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"})
    harness = _Harness(monkeypatch)
    same = tmp_path / "same.json"
    same.write_text(json.dumps(_contract("0.3.0")), encoding="utf-8")
    with pytest.raises(diu.InstalledUpgradeRefused) as excinfo:
        _run(name, harness, from_contract=same)
    assert "nothing to compare" in excinfo.value.message
    assert harness.events == []


def test_state_directory_is_private_and_names_are_checked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    assert stat.S_IMODE(diu.state_dir(name).stat().st_mode) == 0o700
    with pytest.raises(diu.InstalledUpgradeRefused):
        diu.state_dir("../escape")


def test_okg_install_argv_writes_the_receipt_only_when_it_should(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MED f: the real `okg install` argv path, with the installer stubbed."""
    from okg.substrate.cli import main as cli_main

    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    monkeypatch.setattr(diu, "current_installed_identity", lambda: _identity("0.2.0"))
    installs: list[str] = []
    monkeypatch.setattr(
        cli_main, "_cmd_install_routed",
        lambda args, as_json=False: installs.append(args.deployment_name) or 0,
    )
    argv = ["install", "--profile", "workspace-graph", "--deployment-name", name]
    assert cli_main.main([*argv, "--dry-run"]) == 0
    assert not diu.receipt_path(name).exists()
    assert cli_main.main(argv) == 0
    assert diu.read_installed_release_receipt(name, deployment_dir)["okg_version"] == "0.2.0"
    monkeypatch.setattr(diu, "current_installed_identity", lambda: _identity("0.3.0"))
    assert cli_main.main(argv) == 0
    assert diu.read_installed_release_receipt(name, deployment_dir)["okg_version"] == "0.2.0"
    assert installs == [name, name, name]


def test_role_without_restore_rights_is_refused_before_the_worker_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HIGH-2: the backup check needs the TimescaleDB restore rights."""
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.worker_running = True
    harness.rights_problems = ["okg may not set timescaledb.restoring"]
    report = _run(name, harness)
    assert report["failed_step"] == "preflight"
    assert "timescaledb.restoring" in report["steps"][-1]["reason"]
    assert harness.events == ["compat"]
    assert harness.worker_running is True
    assert "the database was not changed" in report["next_action"]


def test_resumed_backup_is_marked_in_its_sidecar_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MED-4: the backup of a resumed upgrade is told so."""
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.migrate_rc = 1
    _run(name, harness)
    assert harness.backup_kwargs == {"resumed": False}
    harness2 = _Harness(monkeypatch)
    _run(name, harness2)
    assert harness2.backup_kwargs == {"resumed": True}


# -------------------------------------------- third review round of #3434

_TOC = """;
; Archive created at 2026-10-06
;
35; 2615 210369 SCHEMA - okg okg_owner
1301; 1255 211561 FUNCTION okg _fill_default_branch_id() okg_owner
1376; 1255 210644 FUNCTION okg stamp(text, timestamp with time zone, bigint) okg_owner
8298; 0 0 ACL - SCHEMA okg okg_owner
500; 1259 3 TABLE okg node_facts okg_owner
501; 1259 4 SEQUENCE okg node_facts_id_seq okg_owner
502; 0 0 SEQUENCE OWNED BY okg node_facts_id_seq okg_owner
503; 0 4 TABLE DATA okg node_facts okg_owner
504; 2604 5 DEFAULT okg node_facts id okg_owner
505; 2618 6 VIEW okg v okg_owner
506; 0 0 COMMENT okg TABLE node_facts okg_owner
"""


def test_owner_entries_come_from_the_table_of_contents_only() -> None:
    """H1: owners are read from the TOC, never from the archive's SQL text."""
    entries = diu.owner_entries(_TOC)
    assert entries[0] == {
        "kind": "TABLE", "schema": "okg", "name": "node_facts", "owner": "okg_owner",
    }
    assert {(e["kind"], e["name"]) for e in entries} == {
        ("SCHEMA", "okg"), ("FUNCTION", "_fill_default_branch_id()"),
        ("FUNCTION", "stamp(text, timestamp with time zone, bigint)"),
        ("TABLE", "node_facts"), ("SEQUENCE", "node_facts_id_seq"), ("VIEW", "v"),
    }


@pytest.mark.parametrize(
    "line",
    [
        "507; 1259 7 TABLE okg two words okg_owner",
        "508; 1255 8 FUNCTION okg no_parens okg_owner",
        "509; 3600 9 TEXT SEARCH DICTIONARY okg dict okg_owner",
        "510; 0 0 SOMETHING NEW okg x okg_owner",
    ],
)
def test_unparseable_or_unhandled_owner_entries_refuse(line: str) -> None:
    with pytest.raises(diu.InstalledUpgradeRefused):
        diu.owner_entries(_TOC + line + "\n")


# -------------------------------------------- fourth review round of #3434


def test_chunk_predicate_is_one_sql_definition() -> None:
    """M2 (round 5): the rule compares the catalog prefix as text, not as a pattern."""
    without = diu._chunk_predicate("s", "n", timescaledb=False)
    assert without == "(s = '_timescaledb_internal')"
    rule = diu._chunk_predicate("n.nspname", "c.relname", timescaledb=True)
    assert "_timescaledb_catalog.hypertable" in rule
    assert "h.associated_schema_name" in rule and "h.associated_table_prefix" in rule
    assert "left(c.relname, length(h.associated_table_prefix) + 1)" in rule
    assert "'^[0-9]+_chunk(_compressed)?$'" in rule


def test_chunk_acl_entries_are_left_to_timescaledb() -> None:
    """HIGH (round 4): a chunk retention dropped in the copy has no ACL to replay."""
    listing = "\n".join([
        "8298; 0 0 ACL - SCHEMA okg postgres",
        "8301; 0 0 ACL okg TABLE node_facts postgres",
        "8302; 0 0 ACL _timescaledb_internal TABLE _hyper_7_12_chunk postgres",
        "8303; 0 0 ACL my_chunks TABLE _hyper_8_1_chunk postgres",
        "8305; 0 0 ACL my_chunks TABLE _hyper_13_2_chunk_compressed postgres",
        "8306; 0 0 ACL _timescaledb_internal COLUMN _hyper_1_1_chunk.v postgres",
        "8308; 0 0 ACL my_chunks COLUMN _hyper_8_3_chunk.odd name postgres",
        "8307; 0 0 ACL okg COLUMN node_facts.v postgres",
        "8304; 0 0 DEFAULT ACL okg DEFAULT PRIVILEGES FOR TABLES postgres",
        "500; 1259 3 TABLE okg node_facts postgres",
    ])
    chunks = {
        ("_timescaledb_internal", "_hyper_7_12_chunk"),
        ("my_chunks", "_hyper_8_1_chunk"),
        ("my_chunks", "_hyper_13_2_chunk_compressed"),
        ("_timescaledb_internal", "_hyper_1_1_chunk"),
        ("my_chunks", "_hyper_8_3_chunk"),
    }
    entries, skipped = diu.acl_toc_entries(listing, chunks)
    assert skipped == 5
    assert [line.split(";")[0] for line in entries] == ["8298", "8301", "8307", "8304"]
    assert diu._acl_table_part("8308; 0 0 ACL my_chunks COLUMN _hyper_8_3_chunk.odd name postgres") == (
        "my_chunks", "_hyper_8_3_chunk",
    )


# -------------------------------------------- unsupervised worker (workerrestart A)


def test_running_worker_without_a_service_is_refused_before_anything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.worker_running = True
    harness.service_available = False
    with pytest.raises(diu.InstalledUpgradeWorkerUnsupervised) as excinfo:
        _run(name, harness)
    error = excinfo.value.to_json()["error"]
    assert error["code"] == "installed_upgrade_worker_unsupervised"
    assert "Nothing was changed" in error["message"]
    assert error["data"]["pids"] == [4242]
    assert error["data"]["reason"] == "worker_running_without_service"
    # M2 (final review): stopping the worker leaves its ack, so the advice is
    # to install the service, not to stop and restart the worker by hand.
    assert "stop_command" not in error["data"] and "kill" not in error["fix_hint"]
    assert "Stopping it would not help" in error["message"]
    assert f"okg dogfood service install --deployment {name} --adapter systemd --apply` as root" in error["fix_hint"]
    assert "first item after 2026-10-17" in error["fix_hint"]
    assert harness.events == ["compat"]
    assert harness.runner_calls == []
    assert harness.worker_running is True
    assert not diu.marker_path(name).exists()
    assert not diu.backups_dir(name).exists()


def test_plan_also_reports_the_unsupervised_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.worker_running = True
    harness.service_available = False
    with pytest.raises(diu.InstalledUpgradeWorkerUnsupervised):
        _run(name, harness, plan_only=True, apply_services=False,
             sync_code=False, sync_deps=False)


def test_no_worker_running_needs_no_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.service_available = False
    report = _run(name, harness)
    assert report["ok"] is True, report
    assert report["worker_service"]["worker_running"] is False
    assert report["worker_service"]["runtime_enabled"] is False
    assert report["worker_service"]["worker_reload"]["needed"] is False


def test_running_worker_with_a_service_takes_the_existing_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.worker_running = True
    report = _run(name, harness)
    assert report["ok"] is True, report
    assert report["worker_service"]["available"] is True
    assert "dogfood down" in harness.events and "dogfood up" in harness.events


def test_worker_service_needs_this_deployments_enabled_instance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """H1 (final review): LoadState=loaded says nothing about this deployment."""
    import platform
    import shutil

    seen: list[list[str]] = []
    state = {"unit_file": "disabled"}

    def run(argv, **kwargs):
        seen.append(argv)
        return subprocess.CompletedProcess(argv, 0, state["unit_file"] + "\n", "")

    template_checks: list[str] = []
    matches = {"value": (False, "the installed template was written for another deployment")}

    def template(deployment, deployment_dir, dsn):
        template_checks.append(deployment)
        return matches["value"]

    monkeypatch.setattr(platform, "system", lambda: "Linux")
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/systemctl")
    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(diu, "_systemd_template_matches", template)
    dsn = lambda: "postgresql://stub/db"  # noqa: E731
    # A supervised deployment; a plain-install folder has no service at all.
    (tmp_path / "deployment.yaml").write_text(
        "name: dep\nruntime:\n  enabled: true\n  supervisor: systemd\n",
        encoding="utf-8",
    )

    service = diu.worker_service("dep", deployment_dir=tmp_path, resolve_dsn=dsn)
    assert service["available"] is False
    assert "not enabled" in service["reason"]
    assert seen == [[
        "systemctl", "show", "-p", "UnitFileState", "--value", "okg-runtime-worker@dep.service",
    ]]
    assert template_checks == []

    state["unit_file"] = "enabled"
    service = diu.worker_service("dep", deployment_dir=tmp_path, resolve_dsn=dsn)
    assert service["available"] is False
    assert "another deployment" in service["reason"]
    assert template_checks == ["dep"]

    matches["value"] = (True, "matches")
    assert diu.worker_service("dep", deployment_dir=tmp_path, resolve_dsn=dsn)["available"] is True


def test_template_check_compares_the_rendered_template_byte_for_byte(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed = tmp_path / "okg-runtime-worker@.service"
    rendered = {"content": "[Service]\nExecStart=okg runtime worker --deployment %i\n"}
    seen: list[list[str]] = []

    def run(argv, **kwargs):
        seen.append(argv)
        return subprocess.CompletedProcess(
            argv, 0, json.dumps({**rendered, "destination": str(installed)}), "",
        )

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(
        "okg.substrate.deployment_apply.desired_deployment_state",
        lambda **kwargs: {"service_authority": {
            "adapter": "systemd", "working_directory": "/srv/dep",
        }},
    )
    installed.write_text("[Service]\nExecStart=okg runtime worker --deployment other\n")
    ok, reason = diu._systemd_template_matches("dep", tmp_path, "postgresql://stub/db")
    assert ok is False and "another deployment" in reason
    assert seen[0][-12:] == [
        "dogfood", "service", "render", "--deployment", "dep", "--adapter", "systemd",
        "--repo-root", "/srv/dep", "--dsn", "${OKG_DEPLOYMENT_APPLY_DSN}", "--json",
    ]
    installed.write_text(rendered["content"])
    assert diu._systemd_template_matches("dep", tmp_path, "postgresql://stub/db") == (True, "matches")


def _stale_ack_health() -> dict[str, Any]:
    return {
        "ok": False,
        "status": "blocked",
        "problems": ["stage_needed:worker_reload"],
        "actions": [{
            "stage": "worker_reload", "needed": True,
            "reason": "worker_ack_fingerprint_drift",
        }],
        "worker_ack": {"desired_state_digest": "sha256:old"},
    }


def test_system_service_needs_root_for_a_worker_reload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """H2 (final review): the reload rewrites the system unit; non-root refuses."""
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.template_writable = False
    harness.apply_health = _stale_ack_health()
    with pytest.raises(diu.InstalledUpgradeWorkerUnsupervised) as excinfo:
        _run(name, harness)
    error = excinfo.value.to_json()["error"]
    assert error["data"]["reason"] == "worker_service_needs_root"
    assert f"run `okg up {name}` as root" in error["fix_hint"]
    assert "first item after 2026-10-17" in error["fix_hint"]
    assert harness.events == ["compat", "apply_health"]
    assert harness.runner_calls == []


def test_system_service_with_a_writable_template_proceeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.template_writable = True
    report = _run(name, harness)
    assert report["ok"] is True, report
    assert report["worker_service"]["template_writable"] is True


@pytest.mark.parametrize("service_available", [False, True])
def test_unavailable_apply_health_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, service_available: bool,
) -> None:
    """M1 (final review): an unreadable reload check refuses, never proceeds."""
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.service_available = service_available
    harness.template_writable = False
    harness.apply_health = {"ok": False, "status": "unavailable", "reason": "relation okg.deployment_apply_receipt does not exist"}
    with pytest.raises(diu.InstalledUpgradeWorkerUnsupervised) as excinfo:
        _run(name, harness)
    error = excinfo.value.to_json()["error"]
    assert error["data"]["reason"] == "worker_reload_unknown"
    assert "okg.deployment_apply_receipt does not exist" in error["message"]
    assert harness.runner_calls == []


def test_launchd_needs_no_root_and_installs_its_own_plist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """M3 (final review): on macOS the converge step writes the plist itself.

    Stubbed: not run on a Mac.
    """
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _enable_runtime(deployment_dir)
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.adapter = "launchd"
    harness.service_available = False
    harness.apply_health = _stale_ack_health()
    report = _run(name, harness)
    assert report["ok"] is True, report
    _write_receipt(deployment_dir)
    harness2 = _Harness(monkeypatch)
    harness2.adapter = "launchd"
    harness2.service_available = False
    harness2.worker_running = True
    with pytest.raises(diu.InstalledUpgradeWorkerUnsupervised) as excinfo:
        _run(name, harness2)
    error = excinfo.value.to_json()["error"]
    assert error["data"]["reason"] == "worker_running_without_service"
    assert "--adapter launchd --apply`" in error["fix_hint"]
    assert "as root" not in error["fix_hint"]


def test_worker_service_on_darwin_is_self_installable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import platform

    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    (tmp_path / "deployment.yaml").write_text(
        "name: dep\nruntime:\n  enabled: true\n  supervisor: launchd\n",
        encoding="utf-8",
    )
    service = diu.worker_service("dep", deployment_dir=tmp_path, resolve_dsn=lambda: "x")
    assert service["adapter"] == "launchd"
    assert service["available"] is False and service["self_installable"] is True


def test_resumed_run_does_not_claim_nothing_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """L4 (final review): an earlier unfinished run already changed things."""
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    diu._write_marker(name, {"started_at": "2026-10-06T10:00:00+00:00", "worker_stopped_by_upgrade": True})
    harness = _Harness(monkeypatch)
    harness.service_available = False
    harness.apply_health = _stale_ack_health()
    with pytest.raises(diu.InstalledUpgradeWorkerUnsupervised) as excinfo:
        _run(name, harness)
    error = excinfo.value.to_json()["error"]
    assert "Nothing was changed" not in error["message"]
    assert "an earlier `okg up` started on 2026-10-06T10:00:00+00:00 did not finish" in error["message"]
    assert error["data"]["resumed_upgrade"] is True


def test_start_worker_command_says_when_it_could_not_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """L5 (final review): a failed service probe is said, not hidden."""
    def broken(deployment, **kwargs):
        raise RuntimeError("systemctl hung")

    monkeypatch.setattr(diu, "worker_service", broken)
    advice = diu.start_worker_command("dep", deployment_dir=tmp_path, resolve_dsn=lambda: "x")
    assert "okg could not check which: RuntimeError: systemctl hung" in advice
    assert "okg runtime worker --deployment dep" in advice


# -------------------------------------------- runtime without a service (workerruntime A)


def _enable_runtime(deployment_dir: Path) -> None:
    manifest = yaml.safe_load((deployment_dir / "deployment.yaml").read_text())
    manifest["runtime"] = {"enabled": True}
    (deployment_dir / "deployment.yaml").write_text(yaml.safe_dump(manifest))


@pytest.mark.parametrize("worker_running", [False, True])
def test_runtime_enabled_without_a_service_is_refused_before_anything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, worker_running: bool,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _enable_runtime(deployment_dir)
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.worker_running = worker_running
    harness.service_available = False
    for plan in (True, False):
        overrides = (
            {"plan_only": True, "apply_services": False, "sync_code": False, "sync_deps": False}
            if plan else {}
        )
        with pytest.raises(diu.InstalledUpgradeWorkerUnsupervised) as excinfo:
            _run(name, harness, **overrides)
        error = excinfo.value.to_json()["error"]
        assert error["data"]["reason"] == "runtime_enabled_without_service"
        assert "Nothing was changed" in error["message"]
        assert (
            f"okg dogfood service install --deployment {name} --adapter systemd --apply"
            in error["fix_hint"]
        )
        assert "per-user systemd worker services, planned as the first item" in error["fix_hint"]
        assert "kill" not in error["fix_hint"]
    assert harness.events == ["compat", "compat"]
    assert harness.runner_calls == []
    assert harness.worker_running is worker_running
    assert not diu.marker_path(name).exists()
    assert not diu.backups_dir(name).exists()


def test_runtime_enabled_with_a_service_is_not_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _enable_runtime(deployment_dir)
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    report = _run(name, harness)
    assert report["ok"] is True, report
    assert report["worker_service"]["runtime_enabled"] is True
    assert report["worker_service"]["available"] is True


def test_runtime_disabled_and_no_worker_needs_no_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.service_available = False
    report = _run(name, harness)
    assert report["ok"] is True, report
    assert report["worker_service"]["worker_running"] is False
    assert report["worker_service"]["runtime_enabled"] is False
    assert report["worker_service"]["worker_reload"]["needed"] is False



def test_stale_worker_ack_without_a_service_is_refused_before_anything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """workerack A: a worker ran before; the converge step would reload it."""
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.service_available = False
    harness.apply_health = {
        "ok": False,
        "status": "blocked",
        "problems": ["stage_needed:worker_reload"],
        "actions": [{
            "stage": "worker_reload", "needed": True,
            "reason": "worker_ack_fingerprint_drift",
        }],
        "worker_ack": {"desired_state_digest": "sha256:old"},
    }
    for plan in (True, False):
        overrides = (
            {"plan_only": True, "apply_services": False, "sync_code": False, "sync_deps": False}
            if plan else {}
        )
        with pytest.raises(diu.InstalledUpgradeWorkerUnsupervised) as excinfo:
            _run(name, harness, **overrides)
        error = excinfo.value.to_json()["error"]
        assert error["data"]["reason"] == "stale_worker_ack"
        assert error["data"]["runtime_enabled"] is False
        assert error["data"]["pids"] == []
        assert error["data"]["worker_reload"] == {
            "health_status": "blocked", "unknown": False, "error": None, "needed": True,
            "reason": "worker_ack_fingerprint_drift", "worker_ack": True,
        }
        assert "A deployment that has run a worker needs that service installed, as root, before `okg up`" in error["message"]
        assert "Nothing was changed" in error["message"]
        assert "first item after 2026-10-17" in error["fix_hint"]
    # Only the compatibility check and the read-only apply health check ran:
    # no readiness, stop, backup, migrate or marker.
    assert harness.events == ["compat", "apply_health", "compat", "apply_health"]
    assert harness.runner_calls == []
    assert not diu.marker_path(name).exists()
    assert not diu.backups_dir(name).exists()


def test_absent_worker_ack_with_runtime_disabled_is_not_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No worker ever ran: the apply health check only warns, so okg up proceeds."""
    name, deployment_dir = _deployment(
        tmp_path, monkeypatch, {"channel": "dev", "kind": "standalone"},
    )
    _write_receipt(deployment_dir)
    harness = _Harness(monkeypatch)
    harness.service_available = False
    harness.apply_health = {
        "ok": True,
        "status": "pending_worker_ack",
        "problems": [],
        "warnings": ["stage_needed:worker_reload"],
        "actions": [{"stage": "worker_reload", "needed": True, "reason": "worker_ack_absent"}],
        "worker_ack": None,
    }
    report = _run(name, harness)
    assert report["ok"] is True, report
    assert report["worker_service"]["worker_reload"]["needed"] is False
