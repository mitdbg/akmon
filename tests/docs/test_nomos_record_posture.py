"""Semantic guard for deployment-managed MCP Record authority docs."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "docs" / "runbooks" / "security" / "nomos.md"


def test_nomos_runbook_explains_posture_derived_record_authority() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    for required in (
        "Deployment-Managed MCP Record Authority",
        "write_principals",
        "mcp_write_default: review",
        "mcp.record",
        "OKG_MCP_ALLOW_WRITES=1",
        "OKG_NOMOS_SUBJECT",
        "authority.subject",
        "authority.actor",
        "authority.producer_id",
        "audit",
        "pending",
        "institutional-IAM",
        "custom policy bundle",
        "review` to `deny",
    ):
        assert required in text, f"Nomos runbook is missing {required!r}"
