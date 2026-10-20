"""Live relations and multi-user access, proved on a running deployment (#3413).

PACT ``live-proof-3413``. Collected only with ``OKG_MCP_DEPLOYMENT_LIVE=1``
(``tests/live/conftest.py``). Once asked for, it FAILS rather than skips when
an input is missing: being asked to prove a deployment and having nothing to
prove it against is a failure, not a pass.

The inputs are the harness's own environment defaults (``harness.py``):

    OKG_MCP_DEPLOYMENT_LIVE=1
    OKG_LIVE_MCP_URL=http://127.0.0.1:<port>/mcp
    OKG_LIVE_MCP_TOKEN_ENV=<name of the variable holding the bearer token>
    OKG_LIVE_MCP_PRINCIPAL_HEADER=X-OKG-Principal
    OKG_LIVE_MCP_PRINCIPAL_A=<principal A> OKG_LIVE_MCP_PRINCIPAL_B=<principal B>
    OKG_LIVE_MCP_EXPECTATIONS=<expectations.json>
    OKG_LIVE_MCP_PHASE=baseline | after-restart | after-rotation
    OKG_LIVE_MCP_OLD_TOKEN_ENV=<after-rotation only>
    OKG_LIVE_MCP_BASELINE_EVIDENCE=<optional, a baseline evidence file>
    OKG_LIVE_MCP_EVIDENCE_OUT=<where to write this run's evidence>

The evidence file is written whether the run passes or fails, so a failed
proof is still recorded.
"""

from __future__ import annotations

import json
from pathlib import Path

from tests.live.mcp_deployment import harness


def test_the_deployment_serves_live_relations_and_separates_two_principals() -> None:
    exit_code = harness.main([])
    assert exit_code != 2, "the harness inputs are unusable; see stderr"
    out = Path(harness.build_parser().parse_args([]).out)
    evidence = json.loads(out.read_text(encoding="utf-8"))
    assert exit_code == 0, json.dumps(evidence["summary"], indent=2)
    assert evidence["summary"]["ok"] is True
