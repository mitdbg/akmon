"""Module-scoped smoke tests for academic_researcher schema.

Verifies that the schema YAML loads cleanly, the 11 subtypes are
declared with their expected `is_a:` parents (where applicable),
and the EdgeNarrowings carries every expected narrowing slot
(intra + cross-module). Full composition behavior (parent closure,
cross-module rules firing under sibling modules) is exercised in
`tests/substrate/library/ontologies/test_academic_researcher.py`.
"""
from __future__ import annotations

from pathlib import Path

from okg.substrate import fastyaml


SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent / "schema.yaml"
)


def _load() -> dict:
    with SCHEMA_PATH.open() as f:
        return fastyaml.safe_load(f) or {}


def test_schema_yaml_parses() -> None:
    schema = _load()
    assert schema.get("name") == "academic_researcher"
    assert "classes" in schema


def test_ten_subtypes_present() -> None:
    # Meeting REMOVED per `refine-academic-pi-with-comms-and-calendar`.
    # Replaced by calendar.CalendarEvent.
    classes = _load().get("classes") or {}
    expected = {
        "Advisee", "Lab", "Grant", "Deadline", "Committee",
        "Review", "Course", "TalkEngagement",
        "RecommendationLetter", "ActionItem",
    }
    missing = expected - set(classes.keys())
    assert not missing, f"missing subtypes: {missing}"
    # Meeting MUST NOT be present.
    assert "Meeting" not in classes, (
        "Meeting was migrated to calendar.CalendarEvent and "
        "MUST be absent from academic_researcher"
    )


def test_refinements_declare_is_a() -> None:
    classes = _load().get("classes") or {}
    assert classes["Advisee"].get("is_a") == "Person", (
        "Advisee SHALL refine person.Person via LinkML is_a:"
    )
    assert classes["Lab"].get("is_a") == "Team", (
        "Lab SHALL refine person.Team via LinkML is_a:"
    )


def test_top_level_subtypes_have_no_parent() -> None:
    classes = _load().get("classes") or {}
    top_level = {
        "Grant", "Deadline", "Committee", "Review", "Course",
        "TalkEngagement", "RecommendationLetter",
        "ActionItem",
    }
    for name in top_level:
        assert classes[name].get("is_a") is None, (
            f"{name!r} SHALL be top-level (no is_a:)"
        )


def test_intra_module_narrowings_present() -> None:
    classes = _load().get("classes") or {}
    en = classes.get("EdgeNarrowings") or {}
    attrs = en.get("attributes") or {}
    # `meeting_produces_action_item` REMOVED with Meeting; its
    # replacement (`calendar_event_produces_action_item`) lives
    # in the calendar module's cross_links.
    expected_intra = {
        "grant_has_deadline",
        "committee_includes_review",
    }
    missing = expected_intra - set(attrs.keys())
    assert not missing, f"missing intra narrowings: {missing}"
    # Affirm the removed narrowing is gone.
    assert "meeting_produces_action_item" not in attrs, (
        "meeting_produces_action_item was migrated to "
        "calendar_event_produces_action_item"
    )


def test_cross_module_narrowings_present_in_rule_files() -> None:
    """Cross-module narrowings live in cross_links/*.yaml files
    (per the substrate's silent-skip-when-sibling-absent
    convention). Verify each expected rule file exists and lists
    the expected rule IDs.
    """
    cl_dir = Path(__file__).resolve().parent.parent / "cross_links"
    expected_rules_by_file = {
        "academic_researcher_x_person.yaml": {
            "person_advises_advisee",
            "person_owns_action_item",
        },
        "academic_researcher_x_scholarly.yaml": {
            "advisee_author_of_paper",
            "grant_acknowledged_in_paper",
            "review_reviews_paper",
            "talk_engagement_presents_paper",
            "talk_engagement_hosted_by_organization",
            "lab_affiliated_with_organization",
            "person_affiliated_with_organization",
            "course_offered_by_organization",
        },
        "academic_researcher_x_bookkeeping.yaml": {
            "grant_funds_project",
            "grant_has_milestone",
        },
        "academic_researcher_x_reasoning.yaml": {
            "action_item_addresses_question",
        },
        "academic_researcher_x_extraction.yaml": {
            "recommendation_letter_has_document",
            # `meeting_has_document` removed; replaced by
            # `calendar_event_has_document` in the calendar
            # module's cross_links.
        },
    }
    for file_name, expected_ids in expected_rules_by_file.items():
        path = cl_dir / file_name
        assert path.is_file(), f"missing cross_link file: {file_name}"
        with path.open() as f:
            doc = fastyaml.safe_load(f) or {}
        rule_ids = {r.get("id") for r in (doc.get("rules") or [])}
        missing = expected_ids - rule_ids
        assert not missing, (
            f"{file_name} missing rules: {missing}"
        )


def test_relation_kinds_declared() -> None:
    classes = _load().get("classes") or {}
    en = classes.get("EdgeNarrowings") or {}
    rel_kinds = (en.get("annotations") or {}).get(
        "okg_relation_kind"
    ) or {}
    # Containment archetypes must be classified contains so the
    # forward walker treats them as hierarchy edges.
    for archetype in ("produces", "has_deadline", "has_milestone",
                      "has_document"):
        assert rel_kinds.get(archetype) == "contains", (
            f"{archetype!r} SHALL be classified as contains"
        )
    # Reference archetypes must be explicit references so they
    # don't accidentally extend the hierarchy.
    for archetype in ("advises", "author_of", "acknowledged_in",
                      "funds", "reviews", "presents", "hosted_by",
                      "addresses", "affiliated_with"):
        assert rel_kinds.get(archetype) == "references", (
            f"{archetype!r} SHALL be classified as references"
        )
