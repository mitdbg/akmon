"""OKG-assisted PACT usage: graph is read-only context.

Per `simplify-pact-surface` task 1, the verbose "Use OKG as pinned
context, not live truth" / "pin a generation" / specific refresh commands
were trimmed. The surviving invariant is just: OKG carries a read-only
projection; edits go to pact.yaml.
"""
from __future__ import annotations

from pathlib import Path

from tests.private_policy import private_policy_path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_graph_projection_is_read_only_context() -> None:
    agents = private_policy_path("pact/AGENTS.md").read_text(encoding="utf-8")
    text = " ".join(agents.split())

    # OKG is read-only context, not a writer surface.
    assert "read-only" in text.lower()
    # Editing goes to pact.yaml, not graph rows.
    assert "Edit `pact.yaml`, not the graph" in agents or "edit pact.yaml" in text.lower()
    # The doc still names a refresh path for stale projection.
    assert "stale" in text.lower()
