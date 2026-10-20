"""Command line: build the synthetic okg-workspace scale graph.

    python -m okg.bench.workspace_scale --dsn postgres://.../okg_workspace_scale \\
        --scale 1.0 --profile deep --report report.json

Exit 0 with the JSON report on stdout (and in ``--report``), or exit 1 with
the refusal on stderr (and the partial report in ``--report`` when the
refusal came from the final verification). The database must be empty and
named ``okg_workspace_*``; ``--dsn`` defaults to ``OKG_WORKSPACE_SCALE_DSN``.

Migration installs ``pg_cron``, which PostgreSQL accepts only in the database
the server setting ``cron.database_name`` names. Point that setting at the
build database before the run, or migrate it elsewhere and pass
``--no-migrate``.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from okg.bench.workspace_scale.build import BuildOptions, BuildRefused, build
from okg.substrate.errors import SubstrateError


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m okg.bench.workspace_scale")
    parser.add_argument("--dsn", default=os.environ.get("OKG_WORKSPACE_SCALE_DSN"))
    parser.add_argument("--scale", type=float, required=True,
                        help="1.0 reproduces the recorded okg-workspace size")
    parser.add_argument("--profile", default="deep",
                        help="history profile from targets.yaml (default deep, "
                             "the shape the budgets were set on; current is one "
                             "publish)")
    parser.add_argument("--no-migrate", action="store_true",
                        help="the database is already migrated. Without it the "
                             "server's cron.database_name must name this "
                             "database, because migration installs pg_cron")
    parser.add_argument("--chunk-size", type=int, default=20000)
    parser.add_argument("--report", type=Path, default=None)
    args = parser.parse_args(argv)
    if not args.dsn:
        parser.error("--dsn (or OKG_WORKSPACE_SCALE_DSN) is required")
    try:
        report = build(BuildOptions(
            dsn=args.dsn,
            scale=args.scale,
            profile=args.profile,
            migrate=not args.no_migrate,
            chunk_size=args.chunk_size,
        ))
    except BuildRefused as exc:
        print(f"workspace_scale: refused: {exc}", file=sys.stderr)
        if exc.report is not None and args.report is not None:
            args.report.write_text(
                json.dumps(exc.report, indent=2, sort_keys=True, default=str) + "\n",
                encoding="utf-8",
            )
        return 1
    except SubstrateError as exc:
        print(f"workspace_scale: refused: {exc}", file=sys.stderr)
        data = getattr(exc, "data", None)
        if data:
            print(json.dumps(data, indent=2, sort_keys=True, default=str), file=sys.stderr)
        return 1
    text = json.dumps(report, indent=2, sort_keys=True, default=str)
    if args.report is not None:
        args.report.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
