from __future__ import annotations

from okg.substrate.pact.markdown_view import render_markdown_view
from okg.substrate.pact.models import Change, PactManifest


def test_review_surface_leads_with_operator_choices() -> None:
    manifest = PactManifest(
        pact_version=0,
        target_pact_version=2,
        change=Change(
            id="demo",
            title="Demo",
            status="proposal",
            rationale="Review the operating choice.",
            root_cause="Humans need the steering decision before the tables.",
        ),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Review view must lead with operator choices.",
                "evidence_policy": {"required": ["planned_test"]},
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Implement review surface",
                "verification": "pytest tests/pact/test_v2_human_surface.py -q",
                "requirements": ["req.demo"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v2_human_surface.py::test_review_surface_leads_with_operator_choices",
            }
        ],
    )

    text = render_markdown_view(manifest)

    assert text.index("## Operator Brief") < text.index("## Decision")
    for label in (
        "Decision needed:",
        "Current state:",
        "Next action:",
        "Human choices:",
        "Proof state:",
    ):
        assert label in text
    # The brief names the verdicts LEGAL in the derived state, not the whole
    # vocabulary. Demanding all five here pinned the loop issue #1476 reports:
    # a change in `in_progress` was told to run `revise`, and `decide` then
    # answered "verdict revise is not legal in state in_progress".
    from okg.substrate.pact.fsm import HUMAN_VERDICTS, legal_verdicts
    from okg.substrate.pact.markdown_view import operator_brief
    from okg.substrate.pact.validation import derive_alignment

    brief = operator_brief(manifest, derive_alignment(manifest))
    legal = set(legal_verdicts(brief["human_choices_state"]))
    assert brief["human_choices"]
    assert set(brief["human_choices"]) <= legal
    head = text.split("Proof state:")[0]
    for choice in HUMAN_VERDICTS:
        assert (choice in head) is (choice in legal), choice
