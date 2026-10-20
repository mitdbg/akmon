#!/usr/bin/env python3
"""MCP server entry point for the okg-workspace deployment."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC = _REPO_ROOT / "src"
if _SRC.is_dir() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from okg.substrate.mcp.deployment_entry import run_for_deployment  # noqa: E402


def main() -> None:
    run_for_deployment("{{ deployment_name }}")


if __name__ == "__main__":
    main()
