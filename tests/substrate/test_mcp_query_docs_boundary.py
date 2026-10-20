"""The MCP reference keeps the SQL escape hatch read-only in its prose.

Moved out of tests/substrate/test_fence_untrusted_node_text.py (issue #2306)
so that file, which runs in the per-pull-request database step, reads no
docs page. This check needs no database.
"""
from __future__ import annotations

from pathlib import Path


def test_query_operator_docs_preserve_the_read_only_boundary() -> None:
    doc = Path("docs/reference/mcp.md").read_text(
        encoding="utf-8",
    )
    section = doc.split("## The SQL escape hatch", 1)[1].split(
        "## Operational state", 1
    )[0]
    assert "read-only" in section
    assert "approved `okg.v_*` views" in section
    assert "rejects writes" in section
