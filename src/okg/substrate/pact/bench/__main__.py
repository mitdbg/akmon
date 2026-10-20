"""Print the benchmark report, or (re)write the committed baseline."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from okg.substrate.pact.bench.runner import repo_root_from, run_benchmark


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="okg.substrate.pact.bench")
    parser.add_argument("--json", action="store_true", help="print metrics.json")
    parser.add_argument(
        "--write-baseline",
        type=Path,
        default=None,
        help="write metrics.json to this path (ratchet update)",
    )
    parser.add_argument(
        "--allow-dirty-env",
        action="store_true",
        help="skip the isolation refusal (numbers become machine-dependent)",
    )
    args = parser.parse_args(argv)

    if args.write_baseline is not None and args.allow_dirty_env:
        # The baseline is the ratchet's reference point. Numbers measured with
        # the isolation refusal disabled are machine-dependent (a live DSN, a
        # stray PACT_ACTOR, a real coordination-gates file all move them), so
        # committing them would pin the ratchet to one developer's environment
        # and silently green or red every later run.
        parser.error(
            "--write-baseline cannot be combined with --allow-dirty-env: "
            "a baseline measured in a dirty environment is not reproducible"
        )

    result = run_benchmark(
        repo_root=repo_root_from(),
        enforce_env=not args.allow_dirty_env,
    )
    if args.write_baseline is not None:
        args.write_baseline.parent.mkdir(parents=True, exist_ok=True)
        args.write_baseline.write_text(result.to_json(), encoding="utf-8")
    if args.json:
        sys.stdout.write(result.to_json())
    else:
        sys.stdout.write(result.to_table())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
