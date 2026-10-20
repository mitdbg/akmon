from __future__ import annotations

from okg.substrate.pact.markdown_view import render_markdown_view
from tests.pact.test_markdown_view import _demo_manifest


def test_review_surface_is_decision_memo_first() -> None:
    manifest = _demo_manifest()
    manifest.target_pact_version = 1
    manifest.change.affected_areas = ["pact"]
    manifest.change.affected_code = ["src/okg/substrate/pact/"]
    manifest.prose["open_questions"] = "Who approves the intent review?"

    text = render_markdown_view(manifest)

    assert text.index("## Decision") < text.index("## Requirements")
    assert text.index("## Proposed Changes") < text.index("## Requirements")
    assert text.index("## Lifecycle Gates") < text.index("## Requirements")
    assert text.index("## Reviewer Actions") < text.index("## Requirements")
    assert "Do not treat planned tests as verification evidence." in text
    assert "| Requirement | Required Evidence | Traceability | Satisfaction |" in text
