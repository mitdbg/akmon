#!/usr/bin/env python3
"""Build and check a Poros refinement trace from two live OKG databases."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from okg.substrate.exchange.refinement import build_refinement_state_from_trace
from okg.substrate.exchange.refinement_db import build_trace_from_databases


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = REPO_ROOT / ".scratch" / "formal" / "poros-db-refinement"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sender-dsn", required=True)
    parser.add_argument("--receiver-dsn", required=True)
    parser.add_argument("--message-id", required=True)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(argv)

    out_dir = args.out.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    trace_path = out_dir / "runtime-trace.json"
    state_path = out_dir / "refinement-state.json"
    error_path = out_dir / "refinement-error.txt"
    if error_path.exists():
        error_path.unlink()

    try:
        trace = build_trace_from_databases(
            sender_dsn=args.sender_dsn,
            receiver_dsn=args.receiver_dsn,
            message_id=args.message_id,
        )
        state = build_refinement_state_from_trace(trace)
    except Exception as exc:  # noqa: BLE001 - CLI artifact is the error surface.
        error_path.write_text(str(exc) + "\n", encoding="utf-8")
        print(
            f"formal-poros-db-refinement: failed; see {error_path}",
            file=sys.stderr,
        )
        return 1

    trace_path.write_text(
        json.dumps(trace, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    state_path.write_text(
        json.dumps(state, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    print(f"formal-poros-db-refinement: passed; state in {state_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
