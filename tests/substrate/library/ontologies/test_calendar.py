"""calendar ontology compose tests.

Module-scoped tests under
`src/okg/substrate/library/ontologies/calendar/tests/` would
clash with Python stdlib `calendar`, so both schema-shape and
compose-level tests live here.

Per `refine-academic-pi-with-comms-and-calendar`: CalendarEvent
(broad-kind events) + CalendarFeed (per-source feed tracking).
Replaces academic_researcher.Meeting.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.module_manifest import load_manifest


CALENDAR_DIR = (
    Path(__file__).resolve().parents[4]
    / "src/okg/substrate/library/ontologies/calendar"
)
SCHEMA_PATH = CALENDAR_DIR / "schema.yaml"
MANIFEST_PATH = CALENDAR_DIR / "module.yaml"


def _classes() -> dict:
    with SCHEMA_PATH.open() as f:
        return (yaml.safe_load(f) or {}).get("classes") or {}


# ---------- Schema-level ----------


def test_module_yaml_validates() -> None:
    manifest = load_manifest(MANIFEST_PATH)
    assert manifest.name == "calendar"
    assert manifest.version == "1.0.0"
    for sibling in ("person", "scholarly", "extraction", "academic_researcher"):
        assert sibling in manifest.extends


def test_two_subtypes_present() -> None:
    classes = _classes()
    assert "CalendarEvent" in classes
    assert "CalendarFeed" in classes


def test_calendar_event_kind_enum_breadth() -> None:
    """CalendarEvent.kind enum SHALL cover at least 13 distinct
    values per the broader-kind decision.
    """
    cls = _classes().get("CalendarEvent") or {}
    kind_attr = (cls.get("attributes") or {}).get("kind") or {}
    desc = (kind_attr.get("description") or "").lower()
    for v in ("lab_meeting", "one_on_one", "committee",
              "sponsor_call", "colloquium", "seminar",
              "conference_day", "travel", "external",
              "office_hours", "personal_block"):
        assert v in desc, (
            f"CalendarEvent.kind enum SHALL document {v!r}"
        )


# ---------- Compose-level ----------


@pytest.fixture
def calendar_deployment(tmp_path: Path) -> Path:
    """Calendar + transitive deps for compose tests.

    The module list mirrors what the academic-researcher profile loads;
    concepts pulls methodology (for MethodCard narrowings),
    reasoning pulls openspec + agent_memory + git_graph for its
    own narrowings to resolve. Keeping the list aligned with the
    profile ensures the compose path here matches reality.
    """
    dep = tmp_path / "test-calendar"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-calendar",
        "description": "calendar compose smoke",
        "modules": [
            "calendar",
            "person",
            "extraction",
            "scholarly",
            "academic_researcher",
            # Transitive deps required by other modules' narrowings:
            "dataset",
            "concepts",
            "methodology",   # concepts → method_card narrowing
            "reasoning",
            "bookkeeping",
            "openspec",      # reasoning + bookkeeping narrowings
            "agent_memory",  # reasoning narrowings
            "git_graph",     # openspec narrowings
            "code_graph",    # concepts → code_symbol narrowing
            "experimental",  # concepts → experiment narrowing
            "agent_sessions",  # transitive
            "ci",            # code_graph → test_case narrowing
            "quality_evidence",  # coordination narrowings
            "coordination",  # transitive
            "compute_env",   # agent_sessions → remote_host
            "external_repos",  # code_graph
            "personas",      # extraction → critique
            "views",
        ],
    }))
    (dep / "schemas").mkdir()
    return dep


def test_compose_emits_calendar_event_and_feed(
    calendar_deployment: Path,
) -> None:
    composed = compose_catalog(calendar_deployment)
    assert "calendar_event" in composed.subtypes
    assert "calendar_feed" in composed.subtypes


def test_intra_narrowing_calendar_event_at_feed(
    calendar_deployment: Path,
) -> None:
    composed = compose_catalog(calendar_deployment)
    triples = composed.narrowing_triples()
    assert ("calendar_event", "at_feed", "calendar_feed") in triples


def test_cross_module_narrowings(
    calendar_deployment: Path,
) -> None:
    composed = compose_catalog(calendar_deployment)
    triples = composed.narrowing_triples()
    # Person attended CalendarEvent
    assert ("person", "attended", "calendar_event") in triples
    # CalendarEvent has_document Document
    assert ("calendar_event", "has_document", "document") in triples
    # CalendarEvent produces ActionItem
    assert ("calendar_event", "produces", "action_item") in triples
