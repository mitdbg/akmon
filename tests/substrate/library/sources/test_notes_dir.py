"""Smoke tests for the notes_dir source."""
from __future__ import annotations

from pathlib import Path

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.notes_dir import NotesDirSource


_NOTES_MD = """---
okg-meeting:
  kind: one_on_one
  participants:
    - Alice Chen
  actions:
    - owner: Alice Chen
      due: 2026-05-30
      description: send draft of paper
    - owner: Alice Chen
      due: 2026-06-01
      description: prep slides for group meeting
    - owner: Riley
      description: review paper draft
---

# 1:1 with Alice — 2026-05-15

Discussed bitemporal data systems progress.
"""


def test_notes_dir_emits_meeting_and_actions(tmp_path: Path) -> None:
    notes_dir = tmp_path / "notes"
    notes_dir.mkdir()
    (notes_dir / "2026-05-15-alice-1on1.md").write_text(
        _NOTES_MD, encoding="utf-8"
    )
    src = NotesDirSource(notes_dir=str(notes_dir))
    run = src.run(run_id="test-run-1")
    facts = list(run.facts)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    events = [n for n in nodes if n.subtype == "calendar_event"]
    actions = [n for n in nodes if n.subtype == "action_item"]
    produces_edges = [e for e in edges if e.edge_type == "produces"]
    assert len(events) == 1
    assert len(actions) == 3
    assert len(produces_edges) == 3
    assert events[0].attrs.get("title") == "1:1 with Alice — 2026-05-15"
    assert events[0].attrs.get("kind") == "one_on_one"


def test_notes_dir_missing_dir_returns_empty() -> None:
    src = NotesDirSource(notes_dir="/nonexistent/path")
    run = src.run(run_id="test-run-2")
    assert list(run.facts) == []
