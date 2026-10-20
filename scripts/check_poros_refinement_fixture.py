#!/usr/bin/env python3
"""Check the Poros runtime-to-TLA+ refinement fixture."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from okg.substrate.exchange.refinement import (
    PorosRefinementError,
    build_refinement_state_from_trace,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TRACE = REPO_ROOT / "formal" / "poros" / "fixtures" / "two-okg-trace.json"
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "poros-refinement"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace", type=Path, default=DEFAULT_TRACE)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(argv)

    trace_path = args.trace.expanduser().resolve()
    out_dir = args.out.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    state_path = out_dir / "refinement-state.json"
    error_path = out_dir / "refinement-error.txt"
    if error_path.exists():
        error_path.unlink()

    try:
        trace = json.loads(trace_path.read_text(encoding="utf-8"))
        if not isinstance(trace, dict):
            raise PorosRefinementError("Poros refinement trace must be an object")
        state = build_refinement_state_from_trace(trace)
    except (OSError, json.JSONDecodeError, PorosRefinementError) as exc:
        error_path.write_text(str(exc) + "\n", encoding="utf-8")
        print(
            f"formal-poros-refinement: failed; see {error_path}",
            file=sys.stderr,
        )
        return 1

    state_path.write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"formal-poros-refinement: passed; state in {state_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
