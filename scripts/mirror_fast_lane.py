#!/usr/bin/env python3
"""Run AKMON's fast test lane.

Reads the ``fast`` lane from ``tests/qa_lanes.yaml``, keeps the test paths
that exist in this checkout, and runs pytest with the lane's marker
expression. Node ids listed in ``tests/mirror_deselect.txt`` are deselected.
Extra arguments are passed to pytest.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def lane_command(extra: list[str]) -> list[str]:
    lanes = yaml.safe_load((ROOT / "tests" / "qa_lanes.yaml").read_text(encoding="utf-8"))
    step = next(s for s in lanes["lanes"]["fast"]["steps"] if s.get("kind") == "pytest")
    paths = [p for p in step["paths"] if (ROOT / p).exists()]
    if not paths:
        raise SystemExit("fast lane: no test paths exist in this checkout")
    deselect: list[str] = []
    listing = ROOT / "tests" / "mirror_deselect.txt"
    if listing.is_file():
        for line in listing.read_text(encoding="utf-8").splitlines():
            node = line.split("#", 1)[0].strip()
            if node:
                deselect += ["--deselect", node]
    return [
        sys.executable, "-m", "pytest", *paths,
        "-m", " ".join(step["markers"].split()),
        "-q", "--no-cov", "--tb=short", "-n", "auto", "--dist=loadscope",
        *deselect,
        *extra,
    ]


def main() -> int:
    command = lane_command(sys.argv[1:])
    return subprocess.run(command, cwd=ROOT, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
