"""What a record read shows a reader, and what it refuses to repeat.

These are pure presentation rules over already-read rows, so they need no
database. They exist because the first live run of these reads against the
Archiv3 deployment was unreadable: every result was the record's name, then its
type and source, then an "excerpt" that was the name again followed by an
attribute KEY, and the record page restated the title under two more fields.
"""
from __future__ import annotations

from okg.substrate.console_views.records import (
    _informative_excerpt,
    _record_fields_and_sections,
)


class TestExcerpt:
    """An excerpt earns its line only by saying something new."""

    def test_the_live_cmssw_excerpt_is_dropped(self) -> None:
        # Exactly what the Archiv3 deployment indexes for a CMSSW release: the
        # record's own name, then the VALUE of its release_type field. Both are
        # already on the row, so the excerpt is a third line saying them again.
        assert _informative_excerpt(
            "CMSSW_0_6_X\n\nrelease_family",
            "CMSSW_0_6_X",
            {
                "name": "CMSSW_0_6_X",
                "release": "CMSSW_0_6_X",
                "release_type": "release_family",
                "major": "0",
            },
        ) is None

    def test_an_excerpt_that_is_only_the_label_and_attribute_keys_is_dropped(self) -> None:
        assert _informative_excerpt(
            "CMSSW_0_6_X release_family",
            "CMSSW_0_6_X",
            {"release_family": "CMSSW_0_6_X", "name": "CMSSW_0_6_X"},
        ) is None

    def test_an_excerpt_repeating_only_the_label_is_dropped(self) -> None:
        assert _informative_excerpt("CMSSW_15_0_15", "CMSSW_15_0_15", {}) is None

    def test_an_excerpt_carrying_real_text_is_kept(self) -> None:
        # Prose survives because its words are parts of one long attribute
        # value, not values in their own right.
        excerpt = "Overview of the release process for the documentation site"
        assert _informative_excerpt(
            excerpt,
            "Introduction · FTS3 Documentation",
            {"title": "Introduction · FTS3 Documentation", "text": excerpt},
        ) == excerpt

    def test_one_new_word_beside_the_label_is_enough_to_keep_it(self) -> None:
        assert _informative_excerpt(
            "CMSSW_15_0_15 deprecated", "CMSSW_15_0_15", {"name": "x"},
        ) == "CMSSW_15_0_15 deprecated"

    def test_a_path_indexed_again_as_its_own_words_is_dropped(self) -> None:
        # A source file indexes as its path, then the path split into words.
        assert _informative_excerpt(
            ".pre-commit-config.yaml\n\npre commit config yaml",
            ".pre-commit-config.yaml",
            {"path": ".pre-commit-config.yaml"},
        ) is None

    def test_prose_is_kept_even_though_it_is_drawn_from_an_attribute(self) -> None:
        # The excerpt is a window into a long value. Treating that value's own
        # words as "already shown" would suppress every real excerpt there is.
        body = (
            "FTS 3.10 FTS 3.11 FTS 3.12 FTS 3.13 FTS3 Docker deployment notes "
            "for the transfer service, with configuration guidance and limits."
        )
        assert _informative_excerpt(
            body, "Document Chunk · bf314e1f", {"text": body, "kind": "chunk"},
        ) == body

    def test_a_missing_or_blank_excerpt_is_not_invented(self) -> None:
        assert _informative_excerpt(None, "anything", {}) is None
        assert _informative_excerpt("   ", "anything", {}) is None


class TestTitleEcho:
    """A field whose value IS the title is reported, not silently dropped."""

    def test_fields_repeating_the_title_are_flagged_and_kept(self) -> None:
        fields, _ = _record_fields_and_sections(
            "cmssw_release",
            {
                "name": "CMSSW_15_0_15",
                "release": "CMSSW_15_0_15",
                "release_type": "Production",
                "state": "Announced",
            },
            label="CMSSW_15_0_15",
        )
        by_key = {field["key"]: field for field in fields}

        # Still present: an operator comparing against the source needs to know
        # which keys the connector wrote.
        assert set(by_key) == {"name", "release", "release_type", "state"}
        assert by_key["name"]["echoes_title"] is True
        assert by_key["release"]["echoes_title"] is True
        assert by_key["release_type"]["echoes_title"] is False
        assert by_key["state"]["echoes_title"] is False

    def test_a_value_that_merely_contains_the_title_is_not_flagged(self) -> None:
        fields, _ = _record_fields_and_sections(
            "cmssw_release",
            {"note": "CMSSW_15_0_15 was announced on Tuesday"},
            label="CMSSW_15_0_15",
        )

        assert fields[0]["echoes_title"] is False

    def test_a_content_section_that_only_restates_the_record_is_not_shown(self) -> None:
        # The live CMSSW record's indexed text is its name, its release type and
        # its platform list -- every one of them a field already on the page.
        _, sections = _record_fields_and_sections(
            "cmssw_release",
            {
                "name": "CMSSW_15_0_15",
                "release_type": "Production",
                "architecture": "el8_aarch64_gcc12,el8_amd64_gcc12",
                "text": "CMSSW_15_0_15\n\nProduction\n\nel8_aarch64_gcc12,el8_amd64_gcc12",
            },
            label="CMSSW_15_0_15",
        )

        assert sections == []

    def test_a_content_section_with_real_prose_is_still_shown(self) -> None:
        body = "Overview of the transfer service, its Docker image and its limits."
        _, sections = _record_fields_and_sections(
            "documentation_page",
            {"title": "Introduction · FTS3 Documentation", "text": body},
            label="Introduction · FTS3 Documentation",
        )

        assert [section["value"] for section in sections] == [body]

    def test_no_label_flags_nothing(self) -> None:
        fields, _ = _record_fields_and_sections(
            "cmssw_release", {"name": "CMSSW_15_0_15"}, label=None,
        )

        assert fields[0]["echoes_title"] is False
