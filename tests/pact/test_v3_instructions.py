"""The agent loop in `pact/AGENTS.md` covers the v3 enforcement gates.

Per `simplify-pact-surface` task 1, the literal "## PACT V3 Quick Loop"
header + the per-command `--json` invocations are no longer first-class
prose in the doc. The surviving invariants:

  - the loop covers gate post-edit, evidence ingest/status, gate task-done,
    and view --format=packet (reviewer surfaces);
  - the doc is concise;
  - OKG is read-only context.
"""
from __future__ import annotations

from pathlib import Path

from tests.private_policy import private_policy_path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_agent_loop_covers_v3_gates() -> None:
    agents = private_policy_path("pact/AGENTS.md").read_text(encoding="utf-8")
    text = " ".join(agents.split())

    # The v5 verbs (pact-v5-git-native).
    for verb in ("new", "view", "evidence", "check", "digest", "migrate", "usage"):
        assert f"okg pact {verb}" in text or f"`okg pact {verb}" in agents

    # The one gate and its two modes are named.
    assert "okg pact check" in agents
    assert "--strict" in agents
    assert "evidence attach" in agents

    # The reviewer surface is the approval line in the pull-request review.
    assert "Pact-Approved" in text
    assert "intent digest" in text

    # OKG read-only.
    assert "read-only" in text.lower()


# 80 was `simplify-pact-surface` task 1's budget. `pact-honest-envelope` spends
# 8 of the remaining lines on the verdict contract — the one thing an agent has
# to know to read any verb's answer, and the section req.envelope.migration
# requires to live here rather than only in the skill. The cap still binds; it
# was raised by the size of that section and by nothing else.
#
# +3 for the hygiene-disposition section (#978): a heading and two lines
# stating the class rule and the forward freshness rule. The full contract
# lives in `okg.substrate.pact.hygiene_policy`; only the deciding rule is here,
# because an agent that cannot see the rule cannot follow it.
# pact-v5-git-native: the v5 contract (about 90 lines) is followed by the
# unchanged v4 contract as a transition appendix while both manifest shapes
# live under pact/changes; the cap drops back to the v5 section alone at the
# corpus cutover.
AGENT_DOC_MAX_LINES = 215


def test_agent_doc_is_concise() -> None:
    # The point of the simplification is that AGENTS.md fits on one screen.
    agents = private_policy_path("pact/AGENTS.md").read_text(encoding="utf-8")
    lines = [line for line in agents.splitlines() if line.strip()]
    assert len(lines) <= AGENT_DOC_MAX_LINES, (
        f"pact/AGENTS.md has {len(lines)} non-blank lines; target is "
        f"<={AGENT_DOC_MAX_LINES} (simplify-pact-surface task 1, raised by "
        "pact-honest-envelope for the verdict-envelope section)"
    )
