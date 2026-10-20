"""Operating-model invariants for PACT.

Per `simplify-pact-surface` task 1, the old tests asserted multi-sentence
prose (Create/Update/Amend/Split/Supersede policy) that lived in the
264-line `pact-development-contract.md`. That document is now a 14-line
stub pointing at `pact/AGENTS.md`. The invariants survive but the prose
does not. These tests check what must still hold.
"""
from __future__ import annotations

from pathlib import Path

from tests.private_policy import private_policy_path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _agents_text() -> str:
    return private_policy_path("pact/AGENTS.md").read_text(encoding="utf-8")


def test_when_to_create_a_pact_is_documented() -> None:
    text = " ".join(_agents_text().split())

    # The contract must say "create a PACT before nontrivial change."
    assert "Create or reuse a PACT" in text
    # The list of scope triggers must include behavior / contracts / tests /
    # architecture / PACT-itself.
    for trigger in ("behavior", "contracts", "tests", "workflow", "architecture", "PACT itself"):
        assert trigger in text, f"trigger {trigger!r} missing from when-to-create policy"
    # The "tiny mechanical edits" escape hatch is documented and bounded.
    assert "Skip" in text
    assert "design choice" in text or "design ambiguity" in text


def test_canonical_writer_is_pact_yaml() -> None:
    text = " ".join(_agents_text().split())

    # The whole point of the simplification is that pact.yaml is canonical.
    assert "`pact.yaml` is canonical" in text
    # And that direct editing is endorsed, not a fallback.
    assert "edit it directly" in text.lower() or "edit `pact.yaml` directly" in text.lower()
