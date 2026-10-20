#!/usr/bin/env python3
"""Prove the sanctioned database clone path against the pinned local image.

The proof starts one disposable PostgreSQL container with TimescaleDB and
pg_cron workers enabled, clones the source database repeatedly, and validates
ordinary and hypertable data after every replacement.  It does not attempt to
induce a PostgreSQL fault.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import psycopg


REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from okg.substrate.postgres_clone import clone_database  # noqa: E402


def _run(argv: list[str], *, check: bool = True) -> str:
    result = subprocess.run(
        argv,
        cwd=REPO,
        text=True,
        capture_output=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(argv)}\n"
            f"stdout:\n{result.stdout[-4000:]}\n"
            f"stderr:\n{result.stderr[-4000:]}"
        )
    return result.stdout.strip()


def _image_pin() -> str:
    dockerfile = (REPO / "ops" / "pg" / "Dockerfile").read_text()
    match = re.search(r"^FROM ((?:docker\.io/)?timescale/timescaledb-ha:[^\s]+)$", dockerfile, re.M)
    if match is None or "@sha256:" not in match.group(1):
        raise RuntimeError("ops/pg/Dockerfile does not contain an immutable base pin")
    return match.group(1)


def _wait_ready(container: str, *, timeout_seconds: float = 90.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        result = subprocess.run(
            [
                "docker",
                "exec",
                container,
                "pg_isready",
                "-h",
                "127.0.0.1",
                "-U",
                "postgres",
                "-d",
                "okg",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(1)
    logs = _run(["docker", "logs", container], check=False)
    raise RuntimeError(f"disposable PostgreSQL did not become ready\n{logs[-4000:]}")


def _initialize_source(container: str) -> None:
    sql = """
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_cron;
CREATE TABLE ordinary (id integer PRIMARY KEY, value text NOT NULL);
INSERT INTO ordinary VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma');
CREATE TABLE measurements (observed_at timestamptz NOT NULL, value integer NOT NULL);
SELECT create_hypertable('measurements', 'observed_at');
INSERT INTO measurements VALUES
    ('2026-01-01T00:00:00Z', 10),
    ('2026-01-02T00:00:00Z', 20);
SELECT cron.schedule('safe-clone-proof', '0 0 1 1 *', $$SELECT 1$$);
"""
    _run(
        [
            "docker",
            "exec",
            container,
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            "-d",
            "okg",
            "-c",
            sql,
        ]
    )


def _source_observation(dsn: str) -> dict[str, Any]:
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
        postgres_version = str(conn.execute("SHOW server_version").fetchone()[0])
        preload_libraries = str(
            conn.execute("SHOW shared_preload_libraries").fetchone()[0]
        )
        extensions = dict(
            conn.execute(
                "SELECT extname, extversion FROM pg_extension "
                "WHERE extname IN ('timescaledb', 'pg_cron') ORDER BY extname"
            ).fetchall()
        )
        workers = [
            {
                "backend_type": str(row[0] or ""),
                "database": str(row[1] or ""),
                "application": str(row[2] or ""),
            }
            for row in conn.execute(
                "SELECT backend_type, datname, application_name "
                "FROM pg_stat_activity "
                "WHERE backend_type ILIKE '%timescale%' "
                "   OR backend_type ILIKE '%cron%' "
                "   OR application_name ILIKE '%timescale%' "
                "   OR application_name ILIKE '%cron%' "
                "ORDER BY backend_type, datname"
            ).fetchall()
        ]
        cron_jobs = int(conn.execute("SELECT count(*) FROM cron.job").fetchone()[0])
    return {
        "postgres_version": postgres_version,
        "shared_preload_libraries": preload_libraries,
        "extensions": extensions,
        "background_workers": workers,
        "cron_jobs": cron_jobs,
    }


def _set_nondefault_target_policy(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
        conn.execute(
            "DO $$ BEGIN CREATE ROLE safe_clone_owner; "
            "EXCEPTION WHEN duplicate_object THEN NULL; END $$"
        )
        conn.execute(
            "DO $$ BEGIN CREATE ROLE safe_clone_reader; "
            "EXCEPTION WHEN duplicate_object THEN NULL; END $$"
        )
        conn.execute(
            "DO $$ BEGIN CREATE ROLE safe_clone_writer; "
            "EXCEPTION WHEN duplicate_object THEN NULL; END $$"
        )
        conn.execute("ALTER DATABASE okg_clone_verify OWNER TO safe_clone_owner")
        conn.execute("REVOKE CONNECT ON DATABASE okg_clone_verify FROM PUBLIC")
        conn.execute(
            "GRANT CONNECT ON DATABASE okg_clone_verify "
            "TO safe_clone_reader WITH GRANT OPTION"
        )
        conn.execute(
            "GRANT TEMPORARY ON DATABASE okg_clone_verify TO safe_clone_writer"
        )


def _verify_target(
    dsn: str,
    *,
    expected_timescaledb: str,
    expected_nondefault_policy: bool,
) -> dict[str, Any]:
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
        ordinary_rows = int(conn.execute("SELECT count(*) FROM ordinary").fetchone()[0])
        hypertable_rows = int(
            conn.execute("SELECT count(*) FROM measurements").fetchone()[0]
        )
        hypertables = int(
            conn.execute(
                "SELECT count(*) FROM timescaledb_information.hypertables "
                "WHERE hypertable_schema = 'public' "
                "AND hypertable_name = 'measurements'"
            ).fetchone()[0]
        )
        extensions = dict(
            conn.execute(
                "SELECT extname, extversion FROM pg_extension "
                "WHERE extname IN ('timescaledb', 'pg_cron')"
            ).fetchall()
        )
        cron_schema = conn.execute("SELECT to_regnamespace('cron')::text").fetchone()[0]
        owner = str(
            conn.execute(
                "SELECT pg_get_userbyid(datdba) FROM pg_database "
                "WHERE datname = current_database()"
            ).fetchone()[0]
        )
        privileges = {
            (str(row[0]), str(row[1]), bool(row[2]))
            for row in conn.execute(
                "SELECT CASE WHEN acl.grantee = 0 THEN 'PUBLIC' "
                "ELSE role.rolname END, acl.privilege_type, acl.is_grantable "
                "FROM pg_database AS database "
                "CROSS JOIN LATERAL aclexplode(COALESCE("
                "database.datacl, acldefault('d', database.datdba))) AS acl "
                "LEFT JOIN pg_roles AS role ON role.oid = acl.grantee "
                "WHERE database.datname = current_database()"
            ).fetchall()
        }
    observed = {
        "ordinary_rows": ordinary_rows,
        "hypertable_rows": hypertable_rows,
        "hypertables": hypertables,
        "extensions": extensions,
        "cron_schema": cron_schema,
        "owner": owner,
        "privileges": sorted(privileges),
    }
    if ordinary_rows != 3 or hypertable_rows != 2 or hypertables != 1:
        raise RuntimeError(f"restored target data mismatch: {observed}")
    if extensions.get("timescaledb") != expected_timescaledb:
        raise RuntimeError(f"restored TimescaleDB version mismatch: {observed}")
    if "pg_cron" in extensions or cron_schema is not None:
        raise RuntimeError(f"server-bound pg_cron state leaked into target: {observed}")
    if expected_nondefault_policy:
        required = {
            ("safe_clone_reader", "CONNECT", True),
            ("safe_clone_writer", "TEMPORARY", False),
        }
        if owner != "safe_clone_owner" or not required.issubset(privileges):
            raise RuntimeError(f"target ACL/owner was not preserved: {observed}")
        if any(
            grantee in {"PUBLIC", "postgres"} and privilege == "CONNECT"
            for grantee, privilege, _grantable in privileges
        ):
            raise RuntimeError(f"temporary/public CONNECT grant leaked: {observed}")
    return observed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--image", default="okg-pg:safe-clone-verification")
    args = parser.parse_args()
    if args.repetitions < 2:
        parser.error("--repetitions must be at least 2")

    base_reference = _image_pin()
    pull_output = _run(["docker", "pull", base_reference])
    _run(
        [
            "docker",
            "build",
            "--pull",
            "--tag",
            args.image,
            str(REPO / "ops" / "pg"),
        ]
    )
    base_image_id = _run(["docker", "image", "inspect", base_reference, "--format", "{{.Id}}"])
    built_image_id = _run(["docker", "image", "inspect", args.image, "--format", "{{.Id}}"])
    labels = json.loads(
        _run(["docker", "image", "inspect", args.image, "--format", "{{json .Config.Labels}}"])
    )
    postgres_label = labels.get("io.okg.postgresql.version")
    timescaledb_label = labels.get("io.okg.timescaledb.version")
    if not postgres_label or not timescaledb_label:
        raise RuntimeError("built image is missing PostgreSQL/TimescaleDB version labels")
    if f"pg{postgres_label}-ts{timescaledb_label}-all" not in base_reference:
        raise RuntimeError(
            "built image labels do not match the exact upstream release tag: "
            f"base={base_reference} pg={postgres_label} ts={timescaledb_label}"
        )

    container = f"okg-safe-clone-{uuid.uuid4().hex[:12]}"
    report: dict[str, Any] = {
        "ok": False,
        "repetitions": args.repetitions,
        "base_reference": base_reference,
        "base_pull_result": pull_output.splitlines()[-1] if pull_output else "",
        "base_image_id": base_image_id,
        "built_image": args.image,
        "built_image_id": built_image_id,
        "image_labels": {
            "postgresql": postgres_label,
            "timescaledb": timescaledb_label,
        },
        "iterations": [],
    }
    try:
        _run(
            [
                "docker",
                "run",
                "--detach",
                "--init",
                "--name",
                container,
                "--publish",
                "127.0.0.1::5432",
                "--env",
                "POSTGRES_PASSWORD=okg",
                "--env",
                "POSTGRES_DB=okg",
                args.image,
                "-c",
                "shared_preload_libraries=timescaledb,pg_cron",
                "-c",
                "cron.database_name=okg",
                "-c",
                "timescaledb.telemetry_level=off",
            ]
        )
        _wait_ready(container)
        port = _run(
            [
                "docker",
                "inspect",
                container,
                "--format",
                '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}',
            ]
        )
        running_image_id = _run(
            ["docker", "inspect", container, "--format", "{{.Image}}"]
        )
        if running_image_id != built_image_id:
            raise RuntimeError(
                f"container image mismatch: built={built_image_id} running={running_image_id}"
            )
        report["running_image_id"] = running_image_id
        service_directory = tempfile.TemporaryDirectory(prefix="okg-safe-clone-")
        service_file = Path(service_directory.name) / "pg_service.conf"
        service_file.write_text(
            "[safe-clone-proof]\n"
            "host=127.0.0.1\n"
            f"port={port}\n"
            "user=postgres\n"
            "password=okg\n"
            "sslmode=disable\n",
            encoding="utf-8",
        )
        service_file.chmod(0o600)
        prior_service_file = os.environ.get("PGSERVICEFILE")
        os.environ["PGSERVICEFILE"] = str(service_file)
        source_dsn = "service=safe-clone-proof dbname=okg"
        target_dsn = "service=safe-clone-proof dbname=okg_clone_verify"
        admin_dsn = "service=safe-clone-proof dbname=postgres"
        _initialize_source(container)
        report["source"] = _source_observation(source_dsn)
        source_timescaledb = report["source"]["extensions"].get("timescaledb")
        if source_timescaledb != timescaledb_label:
            raise RuntimeError(f"running image version mismatch: {report['source']}")
        if not report["source"]["postgres_version"].startswith(postgres_label):
            raise RuntimeError(f"running PostgreSQL version mismatch: {report['source']}")
        preload = report["source"]["shared_preload_libraries"]
        if not {"timescaledb", "pg_cron"}.issubset(
            {item.strip() for item in preload.split(",")}
        ):
            raise RuntimeError(f"required background workers are not preloaded: {preload}")
        worker_text = json.dumps(report["source"]["background_workers"])
        if "TimescaleDB Background Worker Scheduler" not in worker_text:
            raise RuntimeError(f"TimescaleDB worker is not active: {report['source']}")
        if "pg_cron" not in worker_text or report["source"]["cron_jobs"] != 1:
            raise RuntimeError(f"pg_cron proof fixture is vacuous: {report['source']}")

        for iteration in range(1, args.repetitions + 1):
            clone_report = clone_database(
                source_dsn,
                target_dsn,
                admin_dsn=admin_dsn,
            )
            target_observation = _verify_target(
                target_dsn,
                expected_timescaledb=timescaledb_label,
                expected_nondefault_policy=iteration > 1,
            )
            _run(
                [
                    "docker",
                    "exec",
                    container,
                    "pg_isready",
                    "-h",
                    "127.0.0.1",
                    "-U",
                    "postgres",
                    "-d",
                    "okg_clone_verify",
                ]
            )
            report["iterations"].append(
                {
                    "iteration": iteration,
                    "clone": clone_report,
                    "target": target_observation,
                    "pg_isready": True,
                }
            )
            if iteration == 1:
                _set_nondefault_target_policy(target_dsn)
        report["ok"] = True
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
        return 0
    finally:
        if "prior_service_file" in locals():
            if prior_service_file is None:
                os.environ.pop("PGSERVICEFILE", None)
            else:
                os.environ["PGSERVICEFILE"] = prior_service_file
        if "service_directory" in locals():
            service_directory.cleanup()
        _run(["docker", "rm", "--force", "--volumes", container], check=False)


if __name__ == "__main__":
    raise SystemExit(main())
