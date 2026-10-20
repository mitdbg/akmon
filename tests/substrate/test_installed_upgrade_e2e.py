"""End to end: upgrade a wheel-installed deployment with `okg up` (#3415).

Installs an OLDER okg wheel into a fresh virtual environment and builds two
Workspace Graph deployments of a small Git repository with `okg install`,
each with its own database, then installs the NEWER wheel into the same
environment, the way an operator upgrades the package. No worker service
(systemd unit or launchd plist) is installed, which is the common case for a
wheel install without root.

``worker-e2e`` runs a real background worker on the older okg, so it proves
the refusals:

* with ``runtime.enabled: true``, a worker started by hand completes a
  source-sync job (its DBOS workflow reaches SUCCESS);
* `okg up` and `okg up --plan` then refuse with
  ``installed_upgrade_worker_unsupervised``: while the worker runs and after
  it is stopped (``runtime_enabled_without_service``), and with
  ``runtime.enabled: false`` because the worker left an ack the upgrade would
  have to reload through the missing service (``stale_worker_ack``). Nothing
  changes: no backup, no unfinished-upgrade marker, no migration.

``upgrade-e2e`` never runs a worker before the upgrade, so it proves the
upgrade and the way back:

* without a receipt and without `--from-contract`, `okg up` refuses and
  changes nothing; `--plan` changes nothing;
* `okg up --from-contract <old contract>` backs up, migrates (the newer wheel
  must carry a migration the older one lacks), keeps the published generation
  and its node and edge counts, passes readiness, and writes the receipt;
* a second `okg up` reports `up_to_date`; `okg doctor` passes and a search
  reads the upgraded graph;
* with the runtime enabled, a worker started by hand on the newer okg
  completes a job; `okg deployment backup restore --apply` stops it, swaps
  the pre-upgrade backup in (keeping the upgraded database under another
  name), and its next action says to start the worker by hand;
* after reinstalling the older wheel, `okg deployment ready` reads the same
  generation and counts as before the upgrade, the newer migration is gone,
  grants are as before, and a worker started by hand completes a job against
  the restored database, after which readiness no longer reports the job
  queues unavailable.

Only the worker processes this test starts are ever stopped or killed.

The two wheels are inputs, not built here: building and installing two
wheels with all their dependencies takes minutes and needs a package index,
which the per-pull-request database lane does not have. Set
``OKG_INSTALLED_UPGRADE_WHEELS=<old.whl>:<new.whl>`` (the older wheel must be
a lower version) and ``OKG_TEST_DSN`` (a server where the test may create and
drop databases); without them the test is skipped with that reason.
``OKG_INSTALLED_UPGRADE_EVIDENCE=<file>`` writes a summary with no
credentials in it.
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.postgres

WHEELS_ENV = "OKG_INSTALLED_UPGRADE_WHEELS"
EVIDENCE_ENV = "OKG_INSTALLED_UPGRADE_EVIDENCE"
DEPLOYMENT = "upgrade-e2e"
WORKER_DEPLOYMENT = "worker-e2e"
NEW_MIGRATION = "0041_blob_store"
WORKER_SOURCE = "git_history"
JOB_TIMEOUT_SECONDS = 600
TERMINAL_WORKFLOW_STATES = frozenset({
    "SUCCESS", "ERROR", "CANCELLED", "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
})


class _Workers:
    """Hand-started `okg runtime worker` processes; only these are ever stopped."""

    def __init__(self, env: dict[str, str], cwd: Path, logs: Path) -> None:
        self.env, self.cwd, self.logs = env, cwd, logs
        self.procs: dict[str, subprocess.Popen[bytes]] = {}

    def start(self, label: str, okg: list[str], deployment: str) -> int:
        log = (self.logs / f"worker-{label}.log").open("wb")
        proc = subprocess.Popen(
            [*okg, "runtime", "worker", "--deployment", deployment],
            env=self.env, cwd=self.cwd, stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.procs[label] = proc
        return proc.pid

    def alive(self, label: str) -> bool:
        return self.procs[label].poll() is None

    def wait_exit(self, label: str, timeout: float) -> int | None:
        try:
            return self.procs[label].wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None

    def stop(self, label: str) -> int | None:
        proc = self.procs[label]
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
        code = self.wait_exit(label, 120)
        if code is None:
            os.killpg(proc.pid, signal.SIGKILL)
            code = self.wait_exit(label, 30)
        return code

    def log(self, label: str) -> str:
        path = self.logs / f"worker-{label}.log"
        return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""

    def tail(self, label: str) -> str:
        return self.log(label)[-3000:]

    def stop_all(self) -> None:
        for label, proc in self.procs.items():
            if proc.poll() is None:
                self.stop(label)


def _workflow_state(dsn: str, workflow_id: str) -> str | None:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = %s",
            (workflow_id,),
        ).fetchone()
    return str(row[0]) if row else None


def _run_job(okg: list[str], env: dict[str, str], cwd: Path, dsn: str,
             workers: _Workers, label: str, deployment: str) -> dict[str, Any]:
    """Enqueue one source-sync job (no publish) and wait for the worker to finish it.

    The job is enqueued only once the worker has launched: an enqueue pins
    the application version the worker registers at launch, and a worker
    cancels, at launch, a queued job pinned to another version.
    """
    deadline = time.monotonic() + 180
    while "Listening to" not in workers.log(label) and time.monotonic() < deadline:
        assert workers.alive(label), workers.tail(label)
        time.sleep(1)
    assert "Listening to" in workers.log(label), workers.tail(label)
    enqueued = _run([
        *okg, "jobs", "enqueue-source", "--deployment", deployment,
        "--source", WORKER_SOURCE, "--no-publish", "--apply", "--json",
    ], env, cwd)
    payload = _payload(enqueued)
    assert enqueued.returncode == 0, json.dumps(payload)[-3000:] + enqueued.stderr[-2000:]
    workflow_id = str(payload["dbos_workflow_id"])
    deadline = time.monotonic() + JOB_TIMEOUT_SECONDS
    state = _workflow_state(dsn, workflow_id)
    while state not in TERMINAL_WORKFLOW_STATES and time.monotonic() < deadline:
        assert workers.alive(label), workers.tail(label)
        time.sleep(2)
        state = _workflow_state(dsn, workflow_id)
    assert state == "SUCCESS", f"workflow {workflow_id} is {state}\n{workers.tail(label)}"
    status = _payload(_run([*okg, "jobs", "status", "--deployment", deployment, "--json"], env, cwd))
    return {
        "workflow_id": workflow_id,
        "state": state,
        "source_sync_counts": status.get("dbos_source_sync_counts"),
        "jobs_status": status.get("status"),
    }


def _ready_blockers(okg: list[str], env: dict[str, str], cwd: Path, deployment: str) -> list[str]:
    ready = _payload(_run(
        [*okg, "deployment", "ready", deployment, "--checks", "general", "--json"], env, cwd,
    ))
    return sorted(str(b.get("id")) for b in ready.get("blockers") or ())


def _inputs() -> tuple[Path, Path, str]:
    spec = os.environ.get(WHEELS_ENV)
    if not spec:
        pytest.skip(f"{WHEELS_ENV}=<old.whl>:<new.whl> is not set")
    admin = os.environ.get("OKG_TEST_DSN")
    if not admin:
        pytest.skip("OKG_TEST_DSN is not set")
    if shutil.which("uv") is None:
        pytest.skip("uv is not on PATH")
    old, _, new = spec.partition(":")
    return Path(old), Path(new), admin


def _run(argv: list[str], env: dict[str, str], cwd: Path, timeout: float = 1800) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv, env=env, cwd=cwd, capture_output=True, text=True,
        timeout=timeout, check=False,
    )


def _payload(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    for stream in (result.stdout, result.stderr):
        text = stream.strip()
        start = text.find("{")
        if start < 0:
            continue
        try:
            return json.loads(text[start:])
        except ValueError:
            continue
    raise AssertionError(
        f"no JSON payload (rc={result.returncode}):\n"
        f"{result.stdout[-3000:]}\n{result.stderr[-3000:]}"
    )


def _git_repo(root: Path) -> Path:
    repo = root / "repo"
    repo.mkdir()
    env = {**os.environ, "GIT_AUTHOR_NAME": "e2e", "GIT_AUTHOR_EMAIL": "e2e@example.invalid",
           "GIT_COMMITTER_NAME": "e2e", "GIT_COMMITTER_EMAIL": "e2e@example.invalid"}
    (repo / "calc.py").write_text("def add(a, b):\n    return a + b\n", encoding="utf-8")
    (repo / "README.md").write_text("# demo\n", encoding="utf-8")
    for argv in (["git", "init", "-q"], ["git", "add", "."], ["git", "commit", "-qm", "init"]):
        subprocess.run(argv, cwd=repo, env=env, check=True)
    with (repo / "calc.py").open("a", encoding="utf-8") as handle:
        handle.write("\n\ndef sub(a, b):\n    return a - b\n")
    subprocess.run(["git", "commit", "-qam", "second"], cwd=repo, env=env, check=True)
    return repo


def _migration_applied(dsn: str, migration: str) -> bool:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT count(*) FROM okg.schema_migrations "
            "WHERE migration_id = %s AND status = 'applied'",
            (migration,),
        ).fetchone()
    return bool(row and row[0])


def _access_checks(dsn: str) -> dict[str, Any]:
    """Grants a restore must keep: role access, the grant registry, revokes."""
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        roles = [
            row[0] for row in conn.execute(
                "SELECT rolname FROM pg_roles WHERE rolname IN ('okg_mcp', 'okg_app_ro') "
                "ORDER BY 1"
            ).fetchall()
        ]
        return {
            "schema_usage": {
                role: conn.execute(
                    "SELECT has_schema_privilege(%s, 'okg', 'USAGE')", (role,),
                ).fetchone()[0]
                for role in roles
            },
            "grant_registry_drift_rows": conn.execute(
                "SELECT count(*) FROM okg.grant_registry_drift()"
            ).fetchone()[0],
            "public_can_execute_hmac": conn.execute(
                "SELECT has_function_privilege('public', "
                "'okg.external_action_hmac_sha256(bytea,bytea)', 'EXECUTE')"
            ).fetchone()[0],
            "okg_schema_owner": conn.execute(
                "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'okg'"
            ).fetchone()[0],
        }


def _status_counts(okg: list[str], env: dict[str, str], cwd: Path) -> dict[str, Any]:
    result = _run([*okg, "deployment", "ready", DEPLOYMENT, "--checks", "general", "--json"], env, cwd)
    generation = _payload(result).get("generation") or {}
    return {
        "generation_id": generation.get("generation_id"),
        "node_count": generation.get("node_count"),
        "edge_count": generation.get("edge_count"),
    }


class _Manifest:
    """The runtime switch in a deployment.yaml, as an operator edits it."""

    def __init__(self, path: Path, url: str, database: str) -> None:
        self.path = path
        text = path.read_text(encoding="utf-8")
        assert "\nruntime:" not in text
        # A runtime-enabled deployment must also name its database
        # (`okg deployment compat` blocks it otherwise).
        dsn_line = f"  dsn: {url}\n"
        assert dsn_line in text
        text = text.replace(dsn_line, f"{dsn_line}  database: {database}\n")
        path.write_text(text.rstrip("\n") + "\nruntime:\n  enabled: false\n", encoding="utf-8")

    def set_runtime(self, enabled: bool) -> None:
        text = self.path.read_text(encoding="utf-8")
        old, new = (
            ("runtime:\n  enabled: false\n", "runtime:\n  enabled: true\n") if enabled
            else ("runtime:\n  enabled: true\n", "runtime:\n  enabled: false\n")
        )
        assert old in text
        self.path.write_text(text.replace(old, new), encoding="utf-8")


def test_okg_up_upgrades_a_wheel_installed_deployment(tmp_path: Path) -> None:
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    old_wheel, new_wheel, admin_dsn = _inputs()
    info = conninfo_to_dict(admin_dsn)
    password = str(info.pop("password", "") or "")
    stem = f"okg_upgrade_e2e_{secrets.token_hex(4)}"
    database, worker_database = stem, f"{stem}_w"

    def dsn_for(name: str) -> str:
        # In-process only (the test's own SQL checks); never on a command line.
        return make_conninfo(**{**info, "dbname": name, "password": password})

    def url_for(name: str) -> str:
        return (
            f"postgresql://{info.get('user', 'postgres')}@{info.get('host', '127.0.0.1')}:"
            f"{info.get('port', '5432')}/{name}"
        )

    full_dsn, worker_dsn = dsn_for(database), dsn_for(worker_database)
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        for name in (database, worker_database):
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    evidence: dict[str, Any] = {
        "old_wheel": old_wheel.name,
        "new_wheel": new_wheel.name,
        "database": database,
        "deployments": {
            WORKER_DEPLOYMENT: (
                f"database {worker_database}: runs a real worker on the older okg, "
                "then proves the three refusals; never upgraded, because once a "
                "worker has run, okg up needs a worker service, which this host "
                "(no root) does not have"
            ),
            DEPLOYMENT: (
                f"database {database}: never runs a worker before the upgrade, so "
                "okg up succeeds; then proves the restore stopping a worker on the "
                "newer okg and a worker job against the restored database"
            ),
        },
        "worker_service": "none installed on this host (no systemd unit, no root)",
    }
    workers: _Workers | None = None
    try:
        home = tmp_path / "home"
        for sub in ("okg", "fake", "codex/s", "codex/a", "xdg/data", "xdg/cache", "xdg/config", "xdg/state"):
            (home / sub).mkdir(parents=True)
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(home / "fake"),
            "OKG_USER_HOME": str(home / "okg"),
            "XDG_DATA_HOME": str(home / "xdg/data"),
            "XDG_CACHE_HOME": str(home / "xdg/cache"),
            "XDG_CONFIG_HOME": str(home / "xdg/config"),
            "XDG_STATE_HOME": str(home / "xdg/state"),
            "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
            # The password reaches libpq through the environment only; no
            # command line and no written file carries it.
            "PGPASSWORD": password,
        }
        uv_env = {**os.environ}
        venv = tmp_path / "venv"
        python = venv / "bin" / "python"
        okg = [str(venv / "bin" / "okg")]
        repo = _git_repo(tmp_path)
        workers = _Workers(env, tmp_path, tmp_path)

        assert _run(["uv", "venv", "-q", "--python", "3.12", str(venv)], uv_env, tmp_path).returncode == 0
        installed = _run(["uv", "pip", "install", "-q", "--python", str(python), str(old_wheel)], uv_env, tmp_path)
        assert installed.returncode == 0, installed.stderr[-3000:]
        old_version = _run([*okg, "--version"], env, tmp_path).stdout.split()[-1]

        for name, db in ((DEPLOYMENT, database), (WORKER_DEPLOYMENT, worker_database)):
            install = _run([
                *okg, "install", "--profile", "workspace-graph",
                "--deployment-name", name, "--repo-root", str(repo),
                "--postgres-dsn", url_for(db),
                "--codex-sessions-root", str(home / "codex/s"),
                "--codex-archived-sessions-root", str(home / "codex/a"),
                "--json",
            ], env, tmp_path)
            assert install.returncode == 0, install.stderr[-3000:]
        deployment_dir = home / "okg" / "deployments" / DEPLOYMENT
        worker_dir = home / "okg" / "deployments" / WORKER_DEPLOYMENT
        assert deployment_dir.is_dir() and worker_dir.is_dir()
        state_dir = home / "okg" / "state" / "deployments" / DEPLOYMENT
        worker_state = home / "okg" / "state" / "deployments" / WORKER_DEPLOYMENT
        contract = tmp_path / "okg-contract-previous.json"
        written = _run([
            str(python), "-c",
            (
                "from okg.substrate.release_contract import build_contract, render_contract;"
                "print(render_contract(build_contract()), end='')"
            ),
        ], env, tmp_path)
        assert written.returncode == 0, written.stderr[-2000:]
        contract.write_text(written.stdout, encoding="utf-8")
        assert not _migration_applied(full_dsn, NEW_MIGRATION)
        before = _status_counts(okg, env, tmp_path)
        access_before = _access_checks(full_dsn)
        assert before["generation_id"] and before["node_count"] and before["edge_count"]
        evidence["old_version"] = old_version
        evidence["before"] = before

        # worker-e2e: a real background worker on the older okg, started by
        # hand. The worker refuses a deployment whose runtime is not enabled.
        worker_manifest = _Manifest(worker_dir / "deployment.yaml", url_for(worker_database), worker_database)
        worker_manifest.set_runtime(True)
        blockers_without_worker = _ready_blockers(okg, env, tmp_path, WORKER_DEPLOYMENT)
        bootstrap = _run([*okg, "runtime", "bootstrap", "--deployment", WORKER_DEPLOYMENT, "--json"], env, tmp_path)
        assert bootstrap.returncode == 0, bootstrap.stdout[-3000:] + bootstrap.stderr[-3000:]
        old_worker_pid = workers.start("old-worker-e2e", okg, WORKER_DEPLOYMENT)
        job_before = _run_job(okg, env, tmp_path, worker_dsn, workers, "old-worker-e2e", WORKER_DEPLOYMENT)
        evidence["worker_before_upgrade"] = {
            "deployment": WORKER_DEPLOYMENT,
            "okg_version": old_version,
            "pid": old_worker_pid,
            "job": job_before,
            "ready_blockers_before_bootstrap": blockers_without_worker,
            "ready_blockers_with_worker": _ready_blockers(okg, env, tmp_path, WORKER_DEPLOYMENT),
        }

        # The operator upgrades the package in place.
        upgraded = _run(["uv", "pip", "install", "-q", "--python", str(python), str(new_wheel)], uv_env, tmp_path)
        assert upgraded.returncode == 0, upgraded.stderr[-3000:]
        new_version = _run([*okg, "--version"], env, tmp_path).stdout.split()[-1]
        assert new_version != old_version
        evidence["new_version"] = new_version

        # worker-e2e: with no worker service, every okg up refuses before
        # changing anything, in --plan and in apply.
        def refuse(reason: str) -> dict[str, Any]:
            seen = {}
            for mode, extra in (("plan", ["--plan"]), ("apply", [])):
                attempt = _run(
                    [*okg, "up", WORKER_DEPLOYMENT, *extra, "--from-contract", str(contract), "--json"],
                    env, tmp_path,
                )
                attempt_error = _payload(attempt).get("error") or {}
                assert attempt.returncode != 0, attempt.stdout[-3000:]
                assert attempt_error.get("code") == "installed_upgrade_worker_unsupervised", (
                    attempt.stdout[-3000:] + attempt.stderr[-3000:]
                )
                assert attempt_error["data"]["reason"] == reason
                seen[mode] = {
                    "returncode": attempt.returncode,
                    "code": attempt_error.get("code"),
                    "reason": attempt_error["data"]["reason"],
                    "pids": attempt_error["data"]["pids"],
                    "worker_reload": attempt_error["data"].get("worker_reload"),
                    "message": attempt_error.get("message"),
                }
            assert not (worker_state / "backups").exists(), reason
            assert not (worker_state / "upgrade-in-progress.json").exists(), reason
            assert not _migration_applied(worker_dsn, NEW_MIGRATION), reason
            return seen

        refused_running = refuse("runtime_enabled_without_service")
        assert old_worker_pid in refused_running["apply"]["pids"]
        assert workers.alive("old-worker-e2e")
        stopped_code = workers.stop("old-worker-e2e")
        refused_stopped = refuse("runtime_enabled_without_service")
        assert refused_stopped["apply"]["pids"] == []
        worker_manifest.set_runtime(False)
        refused_stale = refuse("stale_worker_ack")
        assert refused_stale["apply"]["worker_reload"]["needed"] is True
        assert refused_stale["apply"]["worker_reload"]["worker_ack"] is True
        evidence["worker_refusal"] = {
            "deployment": WORKER_DEPLOYMENT,
            "runtime_enabled_worker_running": refused_running,
            "worker_exit_code_after_sigterm": stopped_code,
            "runtime_enabled_worker_stopped": refused_stopped,
            "runtime_disabled_stale_worker_ack": refused_stale,
            "nothing_changed": {
                "backups_dir": False, "marker": False, "migration_0041": False,
            },
        }

        # upgrade-e2e: never ran a worker, so okg up may proceed.
        refused = _run([*okg, "up", DEPLOYMENT, "--json"], env, tmp_path)
        error = _payload(refused).get("error") or {}
        assert refused.returncode != 0
        assert error.get("code") == "installed_upgrade_compat_unknown", refused.stderr[-3000:]
        assert not (state_dir / "backups").exists()
        assert not _migration_applied(full_dsn, NEW_MIGRATION)
        evidence["without_receipt"] = {"returncode": refused.returncode, "code": error.get("code")}

        plan = _run([*okg, "up", DEPLOYMENT, "--plan", "--from-contract", str(contract), "--json"], env, tmp_path)
        assert plan.returncode == 0, plan.stderr[-3000:]
        assert _payload(plan)["status"] == "planned"
        assert not _migration_applied(full_dsn, NEW_MIGRATION)

        up = _run([*okg, "up", DEPLOYMENT, "--from-contract", str(contract), "--json"], env, tmp_path)
        report = _payload(up)
        assert up.returncode == 0, json.dumps(report, indent=1)[-6000:] + up.stderr[-3000:]
        assert report["status"] == "upgraded"
        assert [step["step"] for step in report["steps"]] == [
            "compat", "preflight", "stop_worker", "backup", "migrate", "deployment_apply",
            "restart_worker", "readiness", "receipt",
        ]
        assert _migration_applied(full_dsn, NEW_MIGRATION)
        archive = Path(report["backup_archive"])
        assert archive.is_file() and archive.stat().st_size > 0
        backup = next(step for step in report["steps"] if step["step"] == "backup")
        assert backup["source"] == backup["restored"]
        assert backup["source"]["generation_id"] == before["generation_id"]
        assert backup["verify_database_dropped"] is True
        after = _status_counts(okg, env, tmp_path)
        assert after == before
        assert archive.parent == state_dir / "backups"
        assert not (deployment_dir / ".okg" / "installed-release.json").exists()
        receipt = json.loads((state_dir / "installed-release.json").read_text(encoding="utf-8"))
        assert receipt["okg_version"] == new_version
        assert receipt["previous"]["okg_version"] == old_version
        assert receipt["upgrade"]["backup_archive"] == str(archive)
        readiness = next(step for step in report["steps"] if step["step"] == "readiness")
        stop_step = next(step for step in report["steps"] if step["step"] == "stop_worker")
        assert stop_step["was_running"] is False
        evidence["upgrade"] = {
            "deployment": DEPLOYMENT,
            "returncode": up.returncode,
            "status": report["status"],
            "steps": [{"step": s["step"], "ok": s.get("ok"), "skipped": s.get("skipped")} for s in report["steps"]],
            "worker_service": report.get("worker_service"),
            "backup_source": backup["source"],
            "backup_restored": backup["restored"],
            "readiness_pre_existing_blockers": readiness.get("pre_existing_blockers"),
            "graph_before": report.get("graph_before"),
            "graph_after": readiness.get("graph_after"),
            "migration_0041_before": False,
            "migration_0041_after": True,
            "receipt_version": receipt["okg_version"],
            "receipt_previous_version": receipt["previous"]["okg_version"],
        }
        evidence["after"] = after

        again = _run([*okg, "up", DEPLOYMENT, "--json"], env, tmp_path)
        assert again.returncode == 0, again.stderr[-3000:]
        assert _payload(again)["status"] == "up_to_date"
        evidence["second_up"] = _payload(again)["status"]

        doctor = _run([*okg, "doctor", "--deployment", DEPLOYMENT, "--json"], env, tmp_path)
        assert doctor.returncode == 0, doctor.stdout[-3000:]
        search = _run([*okg, "search", "--deployment", DEPLOYMENT, "--query", "calc", "--json"], env, tmp_path)
        assert search.returncode == 0, search.stderr[-3000:]
        hits = _payload(search)
        evidence["doctor_returncode"] = doctor.returncode
        evidence["search_result_count"] = len(hits.get("results") or hits.get("hits") or [])
        assert evidence["search_result_count"] > 0

        # upgrade-e2e, after the upgrade: the operator enables the runtime and
        # starts a worker by hand on the newer okg, so the restore has a real
        # one to stop.
        upgrade_manifest = _Manifest(deployment_dir / "deployment.yaml", url_for(database), database)
        upgrade_manifest.set_runtime(True)
        new_bootstrap = _run([*okg, "runtime", "bootstrap", "--deployment", DEPLOYMENT, "--json"], env, tmp_path)
        assert new_bootstrap.returncode == 0, new_bootstrap.stdout[-3000:] + new_bootstrap.stderr[-3000:]
        new_worker_pid = workers.start("new-before-restore", okg, DEPLOYMENT)
        job_on_new = _run_job(okg, env, tmp_path, full_dsn, workers, "new-before-restore", DEPLOYMENT)

        # The way back, run with the newer okg that took the backup.
        restore_plan = _run([
            *okg, "deployment", "backup", "restore", DEPLOYMENT,
            "--archive", str(archive), "--json",
        ], env, tmp_path)
        plan_payload = _payload(restore_plan)
        assert restore_plan.returncode == 0, restore_plan.stderr[-3000:]
        assert plan_payload["status"] == "planned"
        assert _migration_applied(full_dsn, NEW_MIGRATION)
        restored = _run([
            *okg, "deployment", "backup", "restore", DEPLOYMENT,
            "--archive", str(archive), "--apply", "--json",
        ], env, tmp_path)
        restore_report = _payload(restored)
        assert restored.returncode == 0, json.dumps(restore_report, indent=1)[-6000:] + restored.stderr[-3000:]
        assert restore_report["status"] == "restored"
        restore_stop = next(s for s in restore_report["steps"] if s["step"] == "stop_worker")
        assert restore_stop["was_running"] is True and restore_stop["stopped"] is True
        assert new_worker_pid in restore_stop["pids"]
        new_worker_exit = workers.wait_exit("new-before-restore", 60)
        assert new_worker_exit is not None, "the restore did not stop the worker"
        assert restore_report["worker_stopped"] is True
        assert f"okg runtime worker --deployment {DEPLOYMENT}" in restore_report["next_action"]
        kept_database = restore_report["kept_database"]
        assert not _migration_applied(full_dsn, NEW_MIGRATION)
        kept_dsn = make_conninfo(**{**info, "dbname": kept_database, "password": password})
        assert _migration_applied(kept_dsn, NEW_MIGRATION)
        assert not (state_dir / "installed-release.json").exists()

        downgraded = _run(["uv", "pip", "install", "-q", "--python", str(python), str(old_wheel)], uv_env, tmp_path)
        assert downgraded.returncode == 0, downgraded.stderr[-3000:]
        assert _run([*okg, "--version"], env, tmp_path).stdout.split()[-1] == old_version
        after_restore = _status_counts(okg, env, tmp_path)
        assert after_restore == before
        access_after_restore = _access_checks(full_dsn)
        assert access_after_restore == access_before
        replay = next(s for s in restore_report["steps"] if s["step"] == "replay_access")
        verify_live = next(s for s in restore_report["steps"] if s["step"] == "verify_live")
        ready_old = _payload(_run(
            [*okg, "deployment", "ready", DEPLOYMENT, "--checks", "general", "--json"], env, tmp_path,
        ))
        # The restored database is the pre-upgrade one, taken before the
        # runtime was bootstrapped: the operator bootstraps it with the older
        # okg and starts the worker again by hand. It does work there.
        old_bootstrap = _run([*okg, "runtime", "bootstrap", "--deployment", DEPLOYMENT, "--json"], env, tmp_path)
        assert old_bootstrap.returncode == 0, old_bootstrap.stdout[-3000:] + old_bootstrap.stderr[-3000:]
        restored_worker_pid = workers.start("old-after-restore", okg, DEPLOYMENT)
        job_after_restore = _run_job(okg, env, tmp_path, full_dsn, workers, "old-after-restore", DEPLOYMENT)
        blockers_after_restore = _ready_blockers(okg, env, tmp_path, DEPLOYMENT)
        assert "queues.unavailable" not in blockers_after_restore
        workers.stop("old-after-restore")
        evidence["worker_after_restore"] = {
            "deployment": DEPLOYMENT,
            "new_worker_on_upgraded_okg": {
                "okg_version": new_version, "pid": new_worker_pid, "job": job_on_new,
            },
            "restore_stop_worker": {
                key: restore_stop.get(key) for key in ("was_running", "stopped", "pids")
            },
            "new_worker_exit_code": new_worker_exit,
            "restore_worker_stopped": restore_report["worker_stopped"],
            "restored_worker": {
                "okg_version": old_version, "pid": restored_worker_pid, "job": job_after_restore,
            },
            "ready_blockers_after_restore_before_worker": sorted(
                str(b.get("id")) for b in ready_old.get("blockers") or ()
            ),
            "ready_blockers_with_worker_after_restore": blockers_after_restore,
        }
        evidence["restore"] = {
            "deployment": DEPLOYMENT,
            "plan_status": plan_payload["status"],
            "returncode": restored.returncode,
            "status": restore_report["status"],
            "steps": [{"step": s["step"], "ok": s.get("ok")} for s in restore_report["steps"]],
            "kept_database": kept_database,
            "restored_database_name_used": restore_report["restored_database"],
            "migration_0041_live_after_restore": False,
            "migration_0041_kept_database": True,
            "old_wheel_version_after_restore": old_version,
            "old_wheel_ready_counts": after_restore,
            "counts_equal_before_upgrade": after_restore == before,
            "next_action": restore_report.get("next_action"),
            "access_before_upgrade": access_before,
            "access_after_restore": access_after_restore,
            "replay": {
                key: replay.get(key)
                for key in (
                    "owner_statements", "chunk_owners_left_to_timescaledb",
                    "acl_entries",
                )
            },
            "expected_access": restore_report.get("expected_access"),
            "live_access_digest": verify_live.get("access_digest"),
            "live_access_rows": verify_live.get("access_rows"),
        }
    finally:
        if workers is not None:
            workers.stop_all()
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            # Both databases, and every copy the restore made of them.
            names = [
                row[0] for row in conn.execute(
                    "SELECT datname FROM pg_database WHERE datname LIKE %s",
                    (stem + "%",),
                ).fetchall()
            ]
            for name in names:
                conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        target = os.environ.get(EVIDENCE_ENV)
        if target:
            text = json.dumps(evidence, indent=1, sort_keys=True) + "\n"
            assert password not in text
            Path(target).write_text(text, encoding="utf-8")
