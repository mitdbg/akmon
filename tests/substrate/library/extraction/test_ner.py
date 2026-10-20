"""NER tests — composite dedupe semantics + regex backend.

spaCy / GLiNER backends require model downloads we don't run in
CI; their lazy-load behavior + ModelNotInstalled error are
covered by import-time tests, not extraction-time tests.
"""
from __future__ import annotations

import pytest

from okg.extraction import ExtractedEntity


# ---- composite dedupe — the central correctness contract ---------


def test_composite_canonicalizes_type_case() -> None:
    """spaCy's `'PERSON'` and a regex `'person'` collide on the
    same canonical type."""
    from okg.substrate.library.extraction.ner import CompositeNER

    class _Up:
        def extract(self, text):
            return [ExtractedEntity(type="PERSON", value="Alice",
                                     start=0, end=5, confidence=0.85)]

    class _Lo:
        def extract(self, text):
            return [ExtractedEntity(type="person", value="Alice",
                                     start=0, end=5, confidence=0.95)]

    out = CompositeNER(_Up(), _Lo()).extract("Alice")
    assert len(out) == 1
    assert out[0].confidence == pytest.approx(0.95)


def test_composite_span_overlap_subsumes_shorter() -> None:
    """spaCy `('PERSON', 'Alice Smith', 0, 11)` subsumes regex
    `('person', 'Alice', 0, 5)` even though spans aren't equal."""
    from okg.substrate.library.extraction.ner import CompositeNER

    class _Long:
        def extract(self, text):
            return [ExtractedEntity(type="PERSON", value="Alice Smith",
                                     start=0, end=11, confidence=0.92)]

    class _Short:
        def extract(self, text):
            return [ExtractedEntity(type="person", value="Alice",
                                     start=0, end=5, confidence=1.00)]

    # Regex wins on confidence (1.00 > 0.92).
    out = CompositeNER(_Long(), _Short()).extract("Alice Smith met Bob")
    assert len(out) == 1
    assert out[0].value == "Alice"


def test_composite_higher_confidence_wins_in_overlap() -> None:
    from okg.substrate.library.extraction.ner import CompositeNER

    class _Lo:
        def extract(self, text):
            return [ExtractedEntity(type="ent", value="X",
                                     start=0, end=3, confidence=0.5)]

    class _Hi:
        def extract(self, text):
            return [ExtractedEntity(type="ent", value="X",
                                     start=0, end=3, confidence=0.9)]

    out = CompositeNER(_Lo(), _Hi()).extract("XYZ")
    assert len(out) == 1
    assert out[0].confidence == pytest.approx(0.9)


def test_composite_tied_confidence_longer_span_wins() -> None:
    from okg.substrate.library.extraction.ner import CompositeNER

    class _A:
        def extract(self, text):
            return [ExtractedEntity(type="x", value="abc",
                                     start=0, end=3, confidence=0.8)]

    class _B:
        def extract(self, text):
            return [ExtractedEntity(type="x", value="abcde",
                                     start=0, end=5, confidence=0.8)]

    out = CompositeNER(_A(), _B()).extract("abcdefgh")
    assert len(out) == 1
    assert out[0].value == "abcde"  # longer wins


def test_composite_non_overlapping_kept_separately() -> None:
    from okg.substrate.library.extraction.ner import CompositeNER

    class _A:
        def extract(self, text):
            return [ExtractedEntity(type="x", value="abc",
                                     start=0, end=3, confidence=0.9)]

    class _B:
        def extract(self, text):
            return [ExtractedEntity(type="x", value="def",
                                     start=4, end=7, confidence=0.9)]

    out = CompositeNER(_A(), _B()).extract("abc def")
    assert len(out) == 2
    assert {e.value for e in out} == {"abc", "def"}


def test_composite_different_canonical_types_kept_separately() -> None:
    from okg.substrate.library.extraction.ner import CompositeNER

    class _Person:
        def extract(self, text):
            return [ExtractedEntity(type="person", value="Alice",
                                     start=0, end=5, confidence=0.9)]

    class _Org:
        def extract(self, text):
            return [ExtractedEntity(type="org", value="Alice",
                                     start=0, end=5, confidence=0.9)]

    # Same span, but different canonical types → both kept.
    out = CompositeNER(_Person(), _Org()).extract("Alice")
    assert len(out) == 2


def test_composite_empty_text() -> None:
    from okg.substrate.library.extraction.ner import (
        CompositeNER, RegexEntityExtractor,
    )
    composite = CompositeNER(RegexEntityExtractor([("x", "foo")]))
    assert composite.extract("") == []


def test_composite_requires_backend() -> None:
    from okg.substrate.library.extraction.ner import CompositeNER
    with pytest.raises(ValueError):
        CompositeNER()


# ---- regex backend (re-export) -----------------------------------


def test_regex_backend_re_exports() -> None:
    """The regex_backend re-export points at the canonical
    RegexEntityExtractor."""
    from okg.substrate.library.extraction.ner.regex_backend import (
        RegexEntityExtractor as _A,
    )
    from okg.substrate.projection.extractor import (
        RegexEntityExtractor as _B,
    )
    assert _A is _B


def test_regex_backend_basic_extract() -> None:
    from okg.substrate.library.extraction.ner.regex_backend import (
        RegexEntityExtractor,
    )
    ex = RegexEntityExtractor([("ticket", "CMS-[0-9]+")])
    out = ex.extract("filed CMS-1234 yesterday")
    assert len(out) == 1
    assert out[0].type == "ticket"
    assert out[0].value == "CMS-1234"


def test_regex_backend_capture_group_span_tracks_value() -> None:
    from okg.substrate.library.extraction.ner.regex_backend import (
        RegexEntityExtractor,
    )
    text = "See issue #42 for details"
    ex = RegexEntityExtractor([
        ("github_issue_ref", r"(?:issue\s+)(#[0-9]+)"),
    ])

    out = ex.extract(text)

    assert len(out) == 1
    assert out[0].value == "#42"
    assert text[out[0].start:out[0].end] == "#42"


# ---- spaCy ModelNotInstalled error path ---------------------------


def test_spacy_missing_model_raises_with_fix_hint() -> None:
    from okg.substrate.library.extraction.ner import (
        ModelNotInstalled, SpacyNER,
    )
    ner = SpacyNER(model="not_a_real_spacy_model_xyz")
    with pytest.raises(ModelNotInstalled) as exc:
        ner.extract("Alice met Bob.")
    # fix_hint names the fix.
    assert "spacy download" in (exc.value.fix_hint or "")
