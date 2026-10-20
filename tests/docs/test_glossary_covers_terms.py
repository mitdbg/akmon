"""The glossary defines current product and code terms without process residue."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
GLOSSARY = ROOT / "docs/reference/glossary.md"

REQUIRED_TERMS = (
    "Admission",
    "Federation contract",
    "Formal assurance",
    "Incorporation",
    "Mailbox",
    "Message board",
    "Nomos",
    "Peer registry",
    "Policy bundle",
    "Poros",
    "Redaction",
    # Words a data owner meets when adding a source or a record type.
    "Archetype",
    "Bridge",
    "Change probe",
    "Connector",
    "Cross-links",
    "Edge archetype",
    "Invariant",
    "Module",
    "Ontology",
    "Profile",
    "Reader",
    "Registration",
)


@pytest.fixture(scope="module")
def glossary_text() -> str:
    return GLOSSARY.read_text(encoding="utf-8")


@pytest.mark.parametrize("term", REQUIRED_TERMS)
def test_required_term_has_a_definition(glossary_text: str, term: str) -> None:
    # Entries are level-3 headings under the two audience sections.
    heading = f"\n### {term}\n"
    assert heading in glossary_text
    start = glossary_text.index(heading)
    body = start + len(heading)
    following = re.search(r"\n#{2,3} ", glossary_text[body:])
    entry = glossary_text[start : body + following.start() if following else None]
    assert len(entry.split()) >= 12


def test_glossary_splits_user_words_from_internals(glossary_text: str) -> None:
    sections = re.findall(r"^## (.+)$", glossary_text, re.MULTILINE)
    assert sections == ["Words you will meet", "Internals"]


def test_glossary_is_a_compact_reader_reference(glossary_text: str) -> None:
    assert len(glossary_text.split()) < 1800
    for residue in (
        "canonical home:",
        "deepest link",
        "compatibility history",
        "per the design discussion",
    ):
        assert residue not in glossary_text.lower()


def test_links_into_the_glossary_resolve(glossary_text: str) -> None:
    anchors = {
        re.sub(r"[^\w\- ]", "", heading.lower()).strip().replace(" ", "-")
        for heading in re.findall(r"^#+ (.+)$", glossary_text, re.MULTILINE)
    }
    broken = [
        f"{path.relative_to(ROOT)}#{anchor}"
        for path in sorted([*ROOT.glob("docs/**/*.md"), *ROOT.glob("src/**/*.md")])
        for anchor in re.findall(
            r"glossary(?:\.md|/)#([\w-]+)", path.read_text(encoding="utf-8")
        )
        if anchor not in anchors
    ]
    assert broken == []
