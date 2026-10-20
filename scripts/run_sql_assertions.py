#!/usr/bin/env python3
"""Run database-native SQL assertion files against a migrated test database."""
from __future__ import annotations

import argparse
import glob
import json
import os
from pathlib import Path
import sys
from typing import Any

import psycopg


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SQL_GLOBS = (
    "tests/sql/roles/*.sql",
    "tests/sql/schema/*.sql",
)


def _repo_relative(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _has_glob_magic(path: str) -> bool:
    return any(char in path for char in "*?[")


def strip_psql_meta_commands(sql: str) -> str:
    """Remove psql-only backslash commands before psycopg executes SQL."""
    kept: list[str] = []
    for line in sql.splitlines(keepends=True):
        if line.lstrip().startswith("\\"):
            continue
        kept.append(line)
    return "".join(kept)


def resolve_sql_files(paths: list[str] | None = None) -> list[Path]:
    specs = list(paths or DEFAULT_SQL_GLOBS)
    resolved: list[Path] = []
    seen: set[Path] = set()

    for spec in specs:
        candidate = Path(spec)
        if _has_glob_magic(spec):
            pattern = spec if candidate.is_absolute() else str(ROOT / spec)
            matches = [Path(path) for path in glob.glob(pattern)]
        else:
            matches = [candidate if candidate.is_absolute() else ROOT / candidate]

        for match in sorted(matches):
            path = match.resolve()
            if path in seen:
                continue
            seen.add(path)
            if path.is_file():
                resolved.append(path)

    return sorted(resolved)


def resolve_dsn(
    *,
    explicit_dsn: str | None,
    allow_okg_dsn: bool,
) -> str:
    if explicit_dsn:
        return explicit_dsn
    if test_dsn := os.environ.get("OKG_TEST_DSN"):
        return test_dsn
    if template := os.environ.get("OKG_TEST_DSN_TEMPLATE"):
        worker_id = os.environ.get("PYTEST_XDIST_WORKER", "sqlassertions")
        return template.format(workerid=worker_id)
    if okg_dsn := os.environ.get("OKG_DSN"):
        if allow_okg_dsn:
            return okg_dsn
        raise RuntimeError(
            "Refusing to use OKG_DSN for SQL assertions. Set OKG_TEST_DSN, "
            "OKG_TEST_DSN_TEMPLATE, pass --dsn, or use --allow-okg-dsn "
            "explicitly for a known disposable target."
        )
    raise RuntimeError(
        "SQL assertions require --dsn, OKG_TEST_DSN, or OKG_TEST_DSN_TEMPLATE."
    )


def run_assertion_file(*, dsn: str, path: Path) -> dict[str, Any]:
    sql = strip_psql_meta_commands(path.read_text(encoding="utf-8"))
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    except Exception as exc:  # noqa: BLE001 - assertion output should be concise.
        return {
            "file": _repo_relative(path),
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
    return {"file": _repo_relative(path), "ok": True}


def run_assertions(*, dsn: str, sql_files: list[Path]) -> dict[str, Any]:
    results = [run_assertion_file(dsn=dsn, path=path) for path in sql_files]
    failures = [result for result in results if not result["ok"]]
    return {
        "ok": not failures,
        "assertion_count": len(results),
        "results": results,
        "failures": failures,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run OKG SQL assertion scripts against a migrated test database. "
            "Defaults to tests/sql/roles/*.sql and tests/sql/schema/*.sql."
        )
    )
    parser.add_argument(
        "--dsn",
        help="Database DSN. Defaults to OKG_TEST_DSN or OKG_TEST_DSN_TEMPLATE.",
    )
    parser.add_argument(
        "--path",
        action="append",
        default=[],
        help="SQL assertion file or glob. Repeatable.",
    )
    parser.add_argument(
        "--allow-okg-dsn",
        action="store_true",
        help="Allow OKG_DSN fallback for an explicitly disposable target.",
    )
    parser.add_argument(
        "--list-files",
        action="store_true",
        help="Print resolved SQL assertion files and exit.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Emit machine-readable results.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    sql_files = resolve_sql_files(args.path)

    if args.list_files:
        for path in sql_files:
            print(_repo_relative(path))
        return 0

    if not sql_files:
        print("sql assertions: no SQL files matched", file=sys.stderr)
        return 2

    try:
        dsn = resolve_dsn(
            explicit_dsn=args.dsn,
            allow_okg_dsn=args.allow_okg_dsn,
        )
        result = run_assertions(dsn=dsn, sql_files=sql_files)
    except Exception as exc:  # noqa: BLE001 - CLI should report actionable errors.
        result = {
            "ok": False,
            "assertion_count": len(sql_files),
            "results": [],
            "failures": [{"error": f"{type(exc).__name__}: {exc}"}],
        }

    if args.json_output:
        print(json.dumps(result, indent=2, sort_keys=True))
    elif result["ok"]:
        print(f"sql assertions: ok {result['assertion_count']} file(s)")
    else:
        print("sql assertions: FAIL", file=sys.stderr)
        for failure in result["failures"]:
            location = failure.get("file", "<setup>")
            print(f"  - {location}: {failure['error']}", file=sys.stderr)

    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
