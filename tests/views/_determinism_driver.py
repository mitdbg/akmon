"""Subprocess driver for cross-interpreter planner-determinism.

Run by ``tests/views/test_cross_cutting.py`` under different
``PYTHONHASHSEED`` values; produces a JSON list of spec ids on
stdout. The parent compares the two outputs for equality.

This script intentionally lives next to its caller so it can
be inspected without spelunking through subprocess invocations.
The leading underscore stops pytest from collecting it as a
test module.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

# Add deployments/af_pubs to sys.path so `from views.X import Y`
# resolves the same way it does inside pytest. Pytest's
# conftest.py does this for us when running tests; this driver
# does it itself when run as a standalone subprocess.
_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO / "deployments" / "af_pubs"))

from views.supersedure_health import (  # noqa: E402
    supersedure_health_planner,
    supersedure_health_view_def,
)
from okg.views.types import Anchor  # noqa: E402


def _make_corpus() -> dict[str, Anchor]:
    """Synthetic corpus that triggers all four supersedure_health
    rules in non-trivial combinations.

    Built deterministically from a fixed list of (pub, edge)
    tuples — but inserted into the dict in scrambled order
    relative to the planner's natural sort, so dict-iteration
    ordering changes under different ``PYTHONHASHSEED`` values
    (which is what the cross-interpreter test is verifying the
    planner is robust to).
    """
    pubs = [
        # pub_id, pub_number, status, certified_date, rescinded_date
        ("pub:c", "C", "rescinded", "2019-01-01", "2018-12-01"),  # R4 fires
        ("pub:a", "A", "rescinded", "2018-01-01", "2020-06-01"),
        ("pub:b", "B", "current", "2020-01-01", ""),
        ("pub:d", "D", "current", "2017-01-01", ""),
    ]
    edges = [
        (3, "pub:b", "pub:a"),
        (1, "pub:a", "pub:b"),  # R1 + R2 fire
        (2, "pub:d", "pub:b"),  # R1 + R3 fire (both current)
    ]
    corpus: dict[str, Anchor] = {}
    for pub_id, num, status, cert, resc in pubs:
        content = (
            f"pub_number: {num}\n"
            f"title: \n"
            f"status: {status}\n"
            f"certified_date: {cert}\n"
            f"rescinded_date: {resc}\n"
            f"opr: AF/A3\n"
            f"pdf_url: "
        )
        corpus[f"{pub_id}:catalog"] = Anchor.from_content(
            id=f"{pub_id}:catalog", content=content,
        )
    for edge_id, src, dst in edges:
        content = json.dumps({
            "edge_id": edge_id,
            "src": src,
            "dst": dst,
            "attrs": {},
        }, sort_keys=True)
        corpus[f"edge:supersedes:{edge_id}"] = Anchor.from_content(
            id=f"edge:supersedes:{edge_id}", content=content,
        )
    return corpus


def main() -> int:
    corpus = _make_corpus()
    specs = supersedure_health_planner(corpus, supersedure_health_view_def())
    print(json.dumps([s.id for s in specs]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
