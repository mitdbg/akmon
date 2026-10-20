#!/usr/bin/env python3
"""Rewrite v4 manifests into the PACT v5 shape.

Thin wrapper over ``pact migrate`` so the repository has one documented entry
point. Dry run by default (the verb itself is); pass ``--apply`` to write.

    python scripts/pact_migrate_v5.py --all --decisions decisions.json
    python scripts/pact_migrate_v5.py <id> [<id> ...] --apply
"""
from __future__ import annotations

import sys

from pact.cli import main


def run(argv: list[str]) -> int:
    return main(["migrate", *argv, "--json"], prog="pact")


if __name__ == "__main__":
    sys.exit(run(sys.argv[1:]))
