#!/usr/bin/env python3
"""Advisory pg-schema-diff of a live (current) schema against a target DDL file.

ADVISORY ONLY. Per the pg-schema-diff trial (PR #407 / issue #360) the tool is a
developer diagnostic, not a CI gate or an auto-applied migration source. This
wrapper:

  1. provisions a disposable target database on the same cluster as --from-dsn,
  2. applies the target DDL file (the "golden" schema) to it,
  3. runs `pg-schema-diff plan --from-dsn <current> --to-dsn <target>`,
  4. drops the disposable target database,

and emits a JSON envelope. It degrades to ``status: unavailable`` (never an
error exit) when the pg-schema-diff binary is absent so the scorecard can treat
it as a soft, advisory section.

Safety: the disposable database name is always prefixed ``okg_schemadiff_`` and
this script only ever DROPs a database with that prefix.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid
from typing import Any

try:
    import psycopg
except Exception:  # pragma: no cover - psycopg is a hard dep of the repo
    psycopg = None  # type: ignore[assignment]


DISPOSABLE_PREFIX = "okg_schemadiff_"


def _split_dsn(dsn: str) -> tuple[str, str]:
    """Return (maintenance_dsn_on_'postgres', dbname) for a libpq URL DSN."""
    if psycopg is None:
        raise RuntimeError("psycopg is required")
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    dbname = info.get("dbname") or "postgres"
    maint = dict(info)
    maint["dbname"] = "postgres"
    return psycopg.conninfo.make_conninfo(**maint), str(dbname)


def _target_dsn(dsn: str, target_db: str) -> str:
    info = psycopg.conninfo.conninfo_to_dict(dsn)  # type: ignore[union-attr]
    info["dbname"] = target_db
    return psycopg.conninfo.make_conninfo(**info)  # type: ignore[union-attr]


def _binary() -> str | None:
    return os.environ.get("OKG_PG_SCHEMA_DIFF") or shutil.which("pg-schema-diff")


def run(*, from_dsn: str, target_sql: Path) -> dict[str, Any]:
    binary = _binary()
    if binary is None:
        return {
            "status": "unavailable",
            "detail": "pg-schema-diff binary not found (set OKG_PG_SCHEMA_DIFF or install it)",
        }
    if not target_sql.is_file():
        return {"status": "unavailable", "detail": f"target DDL not found: {target_sql}"}

    maint_dsn, _src_db = _split_dsn(from_dsn)
    target_db = f"{DISPOSABLE_PREFIX}{uuid.uuid4().hex[:12]}"
    target_dsn = _target_dsn(from_dsn, target_db)

    created = False
    try:
        with psycopg.connect(maint_dsn, autocommit=True) as conn:  # type: ignore[union-attr]
            with conn.cursor() as cur:
                cur.execute(f'CREATE DATABASE "{target_db}"')
                created = True
        with psycopg.connect(target_dsn, autocommit=True) as conn:  # type: ignore[union-attr]
            with conn.cursor() as cur:
                cur.execute(target_sql.read_text(encoding="utf-8"))

        completed = subprocess.run(
            [
                binary,
                "plan",
                "--from-dsn",
                from_dsn,
                "--to-dsn",
                target_dsn,
                "--disable-plan-validation",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        plan = (completed.stdout or "").strip()
        statements = [
            line for line in plan.splitlines() if line.strip() and not line.startswith("#")
        ]
        return {
            "status": "advisory",
            "returncode": completed.returncode,
            "statement_count": len(statements),
            "plan": plan[:8000],
            "stderr": (completed.stderr or "").strip()[:2000],
        }
    except Exception as exc:  # noqa: BLE001 - advisory must never hard-fail the scorecard.
        return {"status": "unavailable", "detail": f"{type(exc).__name__}: {exc}"}
    finally:
        if created and target_db.startswith(DISPOSABLE_PREFIX):
            try:
                with psycopg.connect(maint_dsn, autocommit=True) as conn:  # type: ignore[union-attr]
                    with conn.cursor() as cur:
                        cur.execute(
                            f'DROP DATABASE IF EXISTS "{target_db}" WITH (FORCE)'
                        )
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-dsn", required=True, help="Current (live) migrated DSN.")
    parser.add_argument("--target", required=True, help="Path to the target/golden DDL file.")
    parser.add_argument("--json", action="store_true", dest="json_output")
    args = parser.parse_args(argv)

    result = run(from_dsn=args.from_dsn, target_sql=Path(args.target))
    if args.json_output:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"schema diff advisory: {result['status']}")
        if result.get("statement_count") is not None:
            print(f"  drift statements: {result['statement_count']}")
        if result.get("detail"):
            print(f"  {result['detail']}")
    # Advisory: always exit 0 unless argparse failed.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
