"""A fresh okg-workspace database completes first-time setup, with or without pg_cron.

``ops/pg/initdb/00-extensions.sql`` creates the ``pg_cron`` extension only when
the server preloads it. The okg-workspace dogfood server does not (substrate
pg_cron schedules are retired, commit 627f805dd0); before the guard its first
start on an empty data directory failed with ``unrecognized configuration
parameter "cron.database_name"``. This proof renders the okg-workspace
descriptor and starts its Postgres command on an empty data directory with the
``ops/pg/initdb`` scripts mounted, three ways:

* as rendered (no pg_cron): setup completes and pg_cron is absent;
* with pg_cron preloaded and ``cron.database_name`` naming the database: setup
  completes and pg_cron exists;
* with pg_cron preloaded but ``cron.database_name`` left at its default: setup
  still fails, so the guard skips only a server without pg_cron and does not
  hide a misconfigured one.

Needs a container engine (``docker`` or ``podman``) and the ``ops/pg`` image
(``OKG_PG_LAUNCH_IMAGE``, default the descriptor's ``okg-pg17:local``). The
data directory is a tmpfs, so the proof leaves no volume behind. Run:

    OKG_PG_LAUNCH_LIVE=1 pytest tests/live/postgres_launch -p no:xdist
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main

REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_MOUNT = "/home/postgres/pgdata/data"
INIT_DONE = "PostgreSQL init process complete"
CRON_ERROR = 'unrecognized configuration parameter "cron.database_name"'
# pg_cron preloaded, cron.database_name left at its default ("postgres").
WRONG_DATABASE_ERROR = "can only create extension in database postgres"
SKIP_NOTICE = "pg_cron is not in shared_preload_libraries; not creating it"


def _engine() -> str:
    requested = os.environ.get("OKG_PG_LAUNCH_ENGINE")
    for candidate in ([requested] if requested else ["docker", "podman"]):
        if candidate and shutil.which(candidate):
            return candidate
    pytest.fail("OKG_PG_LAUNCH_LIVE=1 but no docker or podman on PATH")


def _rendered_postgres(capsys) -> tuple[dict, Path]:
    rc = main([
        "dogfood",
        "service",
        "render",
        "--deployment",
        "okg-workspace",
        "--adapter",
        "docker-compose",
        "--repo-root",
        str(REPO_ROOT),
        "--okg-bin",
        "uv run okg",
        "--json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    service = yaml.safe_load(payload["content"])["services"]["postgres"]
    return service, Path(payload["destination"]).parent


def _initdb_dir(service: dict, compose_dir: Path) -> Path:
    for volume in service["volumes"]:
        source, _, rest = str(volume).partition(":")
        if rest.startswith("/docker-entrypoint-initdb.d"):
            path = (compose_dir / source).resolve()
            assert path.is_dir(), path
            return path
    pytest.fail("rendered descriptor mounts no initdb directory")


def _start_fresh(engine: str, service: dict, initdb: Path, command: list[str]) -> str:
    name = os.environ.get("OKG_PG_LAUNCH_CONTAINER") or f"okg-live-pg-launch-{uuid.uuid4().hex[:8]}"
    environment = service["environment"]
    argv = [
        engine, "run", "-d", "--name", name,
        # Empty data directory on a tmpfs: a fresh volume that leaves nothing behind.
        "--tmpfs", f"{DATA_MOUNT}:rw,mode=1777,size=2g",
        "-e", f"PGDATA={DATA_MOUNT}/pgdata",
        "-e", f"POSTGRES_PASSWORD={environment['POSTGRES_PASSWORD']}",
        "-e", f"POSTGRES_DB={environment['POSTGRES_DB']}",
        "-e", f"POSTGRES_INITDB_ARGS={environment['POSTGRES_INITDB_ARGS']}",
        "-v", f"{initdb}:/docker-entrypoint-initdb.d:ro",
        os.environ.get("OKG_PG_LAUNCH_IMAGE", service["image"]),
        *command,
    ]
    subprocess.run(argv, check=True, capture_output=True, text=True)  # noqa: S603
    return name


def _logs(engine: str, name: str) -> str:
    done = subprocess.run(  # noqa: S603
        [engine, "logs", name], capture_output=True, text=True, check=False
    )
    return done.stdout + done.stderr


def _running(engine: str, name: str) -> bool:
    done = subprocess.run(  # noqa: S603
        [engine, "inspect", "-f", "{{.State.Running}}", name],
        capture_output=True, text=True, check=False,
    )
    return done.stdout.strip() == "true"


def _wait_for_setup(engine: str, name: str, timeout: float = 180.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        logs = _logs(engine, name)
        failed = CRON_ERROR in logs or WRONG_DATABASE_ERROR in logs
        if INIT_DONE in logs or failed or not _running(engine, name):
            return logs
        time.sleep(2)
    pytest.fail(f"first-time setup neither finished nor failed in {timeout}s")


def _remove(engine: str, name: str) -> None:
    # No -v: the data directory is a tmpfs, and no volume is ever removed here.
    subprocess.run([engine, "rm", "-f", name], capture_output=True, check=False)  # noqa: S603


def _extensions(engine: str, name: str, database: str) -> set[str]:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        done = subprocess.run(  # noqa: S603
            [engine, "exec", name, "psql", "-U", "postgres", "-d", database,
             "-XAtc", "select extname from pg_extension order by 1"],
            capture_output=True, text=True, check=False,
        )
        if done.returncode == 0:
            return set(done.stdout.split())
        time.sleep(2)
    pytest.fail(f"could not list extensions in {database} within 60s")


def _with_pg_cron(command: list[str], cron_database: str | None) -> list[str]:
    """The rendered command with pg_cron preloaded (and optionally its database)."""
    out = [
        "shared_preload_libraries=timescaledb,pg_cron,pg_textsearch"
        if part.startswith("shared_preload_libraries=") else part
        for part in command
    ]
    assert out != command, "rendered command has no shared_preload_libraries"
    if cron_database is not None:
        out += ["-c", f"cron.database_name={cron_database}"]
    return out


def test_rendered_descriptor_completes_first_time_setup(capsys) -> None:
    engine = _engine()
    service, compose_dir = _rendered_postgres(capsys)
    command = [str(part) for part in service["command"]]
    assert not any("pg_cron" in part for part in command), command
    database = service["environment"]["POSTGRES_DB"]
    name = _start_fresh(engine, service, _initdb_dir(service, compose_dir), command)
    try:
        logs = _wait_for_setup(engine, name)
        assert CRON_ERROR not in logs
        assert INIT_DONE in logs, logs[-2000:]
        assert SKIP_NOTICE in logs
        extensions = _extensions(engine, name, database)
        assert {"pg_textsearch", "timescaledb", "vector"} <= extensions
        assert "pg_cron" not in extensions
    finally:
        _remove(engine, name)


def test_a_server_that_preloads_pg_cron_gets_the_extension(capsys) -> None:
    engine = _engine()
    service, compose_dir = _rendered_postgres(capsys)
    database = service["environment"]["POSTGRES_DB"]
    command = _with_pg_cron([str(part) for part in service["command"]], database)
    name = _start_fresh(engine, service, _initdb_dir(service, compose_dir), command)
    try:
        logs = _wait_for_setup(engine, name)
        assert INIT_DONE in logs, logs[-2000:]
        assert SKIP_NOTICE not in logs
        extensions = _extensions(engine, name, database)
        assert {"pg_cron", "pg_textsearch", "timescaledb"} <= extensions
    finally:
        _remove(engine, name)


def test_pg_cron_preloaded_for_another_database_still_fails_setup(capsys) -> None:
    """The guard skips a server without pg_cron; it does not hide a misconfigured one."""
    engine = _engine()
    service, compose_dir = _rendered_postgres(capsys)
    command = _with_pg_cron([str(part) for part in service["command"]], None)
    name = _start_fresh(engine, service, _initdb_dir(service, compose_dir), command)
    try:
        logs = _wait_for_setup(engine, name)
        assert WRONG_DATABASE_ERROR in logs, logs[-2000:]
        assert INIT_DONE not in logs
    finally:
        _remove(engine, name)
