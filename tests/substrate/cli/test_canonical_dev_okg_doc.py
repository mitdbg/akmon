"""canonical-dev-okg: the dev-setup runbook documents the dev workflow.

Pins that the canonical-dev-OKG runbook exists and covers bootstrap, preflight,
persistence (pinning a durable DB), refresh, and the query surface — so the
documented dev path can't silently drift away.
"""
from __future__ import annotations

from pathlib import Path

DOC = (
    Path(__file__).resolve().parents[3]
    / "deployments/okg-workspace/canonical-dev-okg.md"
)


def test_dev_setup_doc_covers_the_workflow() -> None:
    assert DOC.is_file(), "canonical-dev-okg runbook must exist"
    text = DOC.read_text()
    # bootstrap + preflight (one command + the readiness report)
    assert "okg provision" in text
    assert "okg doctor --check preflight" in text
    # persistence: pinning a durable canonical DB
    assert "OKG_DSN" in text
    # refresh: idempotent re-run
    assert "idempotent" in text.lower()
    # query surface: CLI search/trace, raw SQL via psql, the AKMON MCP operators
    assert "okg search" in text
    assert "okg trace node" in text
    assert "psql" in text
    assert "inspect" in text
    # guard the drift this runbook was born with: `okg query "..."` is NOT a
    # real CLI verb (raw SQL is psql; the MCP `query` tool is the agent path).
    assert 'okg query "' not in text
