#!/usr/bin/env python3
"""(Re)pin the migration-convergence dump — the documented refresh procedure.

The convergence CI lane (`schema-migration-hardening` /
req.convergence-gate) restores the dump this script produces, runs
``migrate()``, and requires an empty schema diff against a fresh
install. The dump is a full plain-format ``pg_dump`` of a
fresh-install-plus-runtime-ensures database captured at the commit this
script runs from, so the restored side carries exactly what a real
deployment of that commit carried: the migration ledger rows, catalog
seeds, Chronos metadata, and the runtime-ensured relations
(deployment-apply pair, attribute tables) that are not yet in
schema.sql.

Refresh procedure (run from a clean checkout of the commit to pin):

    uv run python scripts/refresh_convergence_dump.py \
        --base-dsn postgres://postgres:okg@127.0.0.1:5433/postgres

then commit the regenerated dump + ``.meta.json`` together, and say in
the commit message WHY the pin moved.

Re-pin policy (v1.0 re-baseline, rolling-baseline rule): the dump
re-pins when the v1.0 snapshot ships — the convergence lane's pinned
dump becomes the v1.0 dump — and thereafter ONLY at declared major
versions via this procedure, recorded in the meta file. Ordinary schema
evolution goes through ordered migrations, which the lane proves
converge FROM the existing pin; casually re-pinning after every schema
edit would reduce the lane to comparing a database with itself.
"""
from __future__ import annotations

import argparse
import datetime as _datetime
import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from check_migration_convergence import (  # noqa: E402
    PINNED_DUMP_META_PATH,
    PINNED_DUMP_PATH,
    build_fresh_install,
    drop_database,
)


def _git_head() -> str:
    completed = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout.strip()


def _server_versions(dsn: str) -> dict[str, str]:
    import psycopg

    with psycopg.connect(dsn, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute("SHOW server_version")
        server = str(cur.fetchone()[0])
        cur.execute(
            "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
        )
        row = cur.fetchone()
        timescale = str(row[0]) if row else "absent"
    return {"postgres": server, "timescaledb": timescale}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-dsn",
        default=os.environ.get(
            "OKG_TEST_DSN", "postgres://postgres:okg@127.0.0.1:5433/postgres"
        ),
    )
    parser.add_argument("--out", type=Path, default=PINNED_DUMP_PATH)
    args = parser.parse_args(argv)

    pg_dump = shutil.which("pg_dump")
    if pg_dump is None:
        print("pg_dump not found on PATH", file=sys.stderr)
        return 2

    dbname = f"okg_test_conv_pin_{uuid.uuid4().hex[:12]}"
    try:
        dsn = build_fresh_install(args.base_dsn, dbname)
        versions = _server_versions(dsn)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        # --no-owner only: privileges are part of the pinned input shape. The
        # migration reapplies the shared grant registry, while the dump still
        # preserves grants outside that registry so the convergence comparison
        # can expose any unintended authorization change.
        completed = subprocess.run(
            [
                pg_dump,
                "--no-owner",
                "--format", "plain",
                "--file", str(args.out),
                "--dbname", dsn,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            print(
                f"pg_dump failed (rc={completed.returncode}):\n"
                f"{completed.stderr[-4000:]}",
                file=sys.stderr,
            )
            return 1
        # pg_dump >= 18 wraps plain dumps in `\restrict <token>` /
        # `\unrestrict <token>` psql meta-commands. Older psql (the CI
        # runner ships client 17) aborts on the unknown command under
        # ON_ERROR_STOP, and the token is freshly random per run — a
        # nondeterministic diff line on every re-pin. Strip them: they
        # are a psql sandboxing wrapper, not schema content.
        lines = args.out.read_text(encoding="utf-8").splitlines(keepends=True)
        args.out.write_text(
            "".join(
                line
                for line in lines
                if not line.startswith(("\\restrict", "\\unrestrict"))
            ),
            encoding="utf-8",
        )
    finally:
        drop_database(args.base_dsn, dbname)

    meta = {
        "captured_commit": _git_head(),
        "captured_at": _datetime.datetime.now(
            _datetime.timezone.utc
        ).isoformat(timespec="seconds"),
        "generator": "scripts/refresh_convergence_dump.py",
        "server_versions": versions,
        "repin_policy": (
            "re-pins at the v1.0 re-baseline, thereafter only at declared "
            "major versions (see the module docstring)"
        ),
    }
    meta_path = PINNED_DUMP_META_PATH if args.out == PINNED_DUMP_PATH else (
        args.out.with_suffix(".meta.json")
    )
    meta_path.write_text(
        json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    size_kib = args.out.stat().st_size / 1024
    print(f"pinned dump written: {args.out} ({size_kib:.1f} KiB)")
    print(f"meta written: {meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
