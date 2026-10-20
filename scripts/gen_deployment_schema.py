#!/usr/bin/env python3
"""Write schemas/deployment.schema.json from the deployment key table.

The schema is generated from ``okg.substrate.deployments.keys`` so it cannot
drift from what the loader accepts. ``--check`` exits 1 when the checked-in
file differs from the generated one.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from okg.substrate.deployments.keys import render_deployment_schema

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "schemas" / "deployment.schema.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 if the checked-in schema differs from the generated one",
    )
    args = parser.parse_args()
    generated = render_deployment_schema()
    if args.check:
        current = TARGET.read_text(encoding="utf-8") if TARGET.is_file() else ""
        if current != generated:
            print(
                f"{TARGET.relative_to(ROOT)} is stale; run "
                "`python scripts/gen_deployment_schema.py`",
                file=sys.stderr,
            )
            return 1
        return 0
    TARGET.write_text(generated, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
