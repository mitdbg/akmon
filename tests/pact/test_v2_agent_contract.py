"""The agent-facing PACT contract is `pact/AGENTS.md`.

Per `simplify-pact-surface` task 1, this file is now the single source for
the contract and is intentionally lean. These tests check the **invariants**
the contract must keep, not the literal phrasing — the old test asserted
specific sentences ("Create or reuse a PACT", "Update the current PACT",
"Amend / Split / Supersede", "target_pact_version: 2") that were part of
the verbose v2 framing the simplification removed.
"""
from __future__ import annotations

from pathlib import Path

from tests.private_policy import private_policy_path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_agent_contract_names_supported_verbs_and_yaml_canonical() -> None:
    agents = private_policy_path("pact/AGENTS.md").read_text(encoding="utf-8")
    text = " ".join(agents.split())

    # The v5 verbs are named (pact-v5-git-native: gate/hooks/migrate-version
    # are gone and their removal is documented).
    for verb in ("new", "view", "evidence", "check", "digest", "migrate", "usage"):
        assert f"`okg pact {verb}" in agents, (
            f"agent contract must name supported verb `okg pact {verb}`"
        )
    assert "verb_removed" in agents

    # YAML is canonical and the only authority.
    assert "`pact.yaml` is canonical" in agents

    # The loop is documented: evidence attach, check --strict, the approval line.
    assert "evidence attach" in agents
    assert "check <id>" in agents and "--strict" in agents
    assert "Pact-Approved" in agents

    # OKG is read-only context, not a writer surface.
    assert "read-only" in text.lower()
    assert "OKG" in text
