"""Document rendering, header safety and the hashes (PACT
openrouter-embeddings-hnsw, design v3.2). No database."""
from __future__ import annotations

import hashlib

import pytest

from okg.substrate.embeddings.config import parse_embedding_config
from okg.substrate.embeddings.documents import (
    NONBLANK_BODY_REGEX,
    check_templates,
    doc_hash,
    field_label,
    header_field_shown,
    record_still_safe,
    render_document,
    template_hash,
    template_record,
)
from okg.substrate.errors import SubstrateError


def _set(**overrides):
    main = {"backend": "hash"}
    main.update(overrides)
    return parse_embedding_config(
        plural={"enabled": True, "sets": {"main": main}}
    ).search_vector_set


JIRA = {
    "document": {
        "default": {"header": ["subtype", "title"], "body": "text"},
        "types": {"jira_issue": {"header": ["subtype", "title", "status", "labels"]}},
    }
}


def test_the_fixed_format() -> None:
    vector_set = _set(**JIRA)
    rendered = render_document(
        vector_set,
        subtype="jira_issue",
        attrs={
            "title": "Fix  the\nbuild",
            "status": "open",
            "labels": ["ci", "build"],
            "text": "The build fails on main.",
            "owner_id": "u-1",
        },
        classification=None,
    )
    assert rendered.text == (
        "Type: jira_issue\n"
        "Title: Fix the build\n"
        "Status: open\n"
        "Labels: ci, build\n"
        "\n"
        "The build fails on main."
    )
    assert rendered.doc_hash == hashlib.sha256(rendered.text.encode()).hexdigest()
    assert rendered.doc_hash == doc_hash(rendered.text)
    assert rendered.omitted_fields == ()


def test_missing_fields_are_skipped_and_other_types_use_the_default() -> None:
    vector_set = _set(**JIRA)
    rendered = render_document(
        vector_set, subtype="chunk", attrs={"text": "body only"}, classification=None,
    )
    assert rendered.text == "Type: chunk\n\nbody only"
    bare = _set(document={"default": {"header": []}})
    assert render_document(
        bare, subtype="chunk", attrs={"text": "body only"}, classification=None,
    ).text == "body only"


def test_the_cut_applies_to_the_whole_document() -> None:
    vector_set = _set(max_chars=20)
    rendered = render_document(
        vector_set, subtype="chunk", attrs={"title": "T", "text": "x" * 100},
        classification=None,
    )
    assert len(rendered.text) == 20
    assert rendered.text.startswith("Type: chunk\nTitle: T")


def test_no_document_without_body_or_outside_the_subtypes() -> None:
    vector_set = _set(subtypes=["chunk"])
    assert render_document(
        vector_set, subtype="chunk", attrs={"title": "no text"}, classification=None,
    ) is None
    assert render_document(
        vector_set, subtype="chunk", attrs={"text": "   "}, classification=None,
    ) is None
    assert render_document(
        vector_set, subtype="file", attrs={"text": "x"}, classification=None,
    ) is None


def test_labels() -> None:
    assert field_label("subtype") == "Type"
    assert field_label("component_name") == "Component name"


def test_headersafe_rule_a_follows_todays_read_classification() -> None:
    # Unannotated subtype: the interim legacy classifier shows everything but
    # its free-text keys.
    assert header_field_shown(None, "title")
    assert not header_field_shown(None, "evidence")
    assert not header_field_shown(None, "provenance")
    # Annotated: only non_pii shows; an unlisted attribute fails safe.
    declared = {"title": "non_pii", "owner": "direct_identifier"}
    assert header_field_shown(declared, "title")
    assert not header_field_shown(declared, "owner")
    assert not header_field_shown(declared, "status")
    assert not header_field_shown({}, "title")
    # The subtype line is always shown; denied names never are.
    assert header_field_shown({}, "subtype")
    assert not header_field_shown(None, "owner_id")


def test_a_masked_header_value_never_reaches_the_document() -> None:
    vector_set = _set(**JIRA)
    attrs = {"title": "Secret plan", "status": "open", "text": "body"}
    shown = render_document(vector_set, subtype="jira_issue", attrs=attrs,
                            classification={"title": "non_pii", "status": "non_pii"})
    hidden = render_document(vector_set, subtype="jira_issue", attrs=attrs,
                             classification={"title": "direct_identifier",
                                             "status": "non_pii"})
    assert "Secret plan" in shown.text
    assert "Secret plan" not in hidden.text
    assert hidden.omitted_fields == ("title", "labels")
    assert shown.doc_hash != hidden.doc_hash


def test_must_fail_a_header_field_that_joins_the_deny_list_stops_its_documents(
    monkeypatch,
) -> None:
    # Review P5 F1: the fields a stored document holds are decided from the
    # stored record alone; a field that a later release denies (here
    # ``status``) must fail the check, not drop out of it.
    from okg.substrate.embeddings import documents

    vector_set = _set(**JIRA)
    declared = {"title": "non_pii", "status": "non_pii", "labels": "non_pii"}
    record = template_record(vector_set, "jira_issue", declared)
    assert record_still_safe(record, [declared])
    real_denied = documents.header_field_denied
    monkeypatch.setattr(
        documents, "header_field_denied",
        lambda name: "a test denial" if name == "status" else real_denied(name),
    )
    assert not record_still_safe(record, [declared])
    # A field the record says was hidden at build is not in the document.
    hidden = template_record(
        vector_set, "jira_issue", {"title": "non_pii", "labels": "non_pii"},
    )
    assert record_still_safe(hidden, [{"title": "non_pii", "labels": "non_pii"}])


def test_template_hash_tracks_template_and_header_classification() -> None:
    vector_set = _set(**JIRA)
    base = template_hash(vector_set, "jira_issue", {"title": "non_pii"})
    # A class change of a header field makes a new template hash ...
    assert template_hash(vector_set, "jira_issue", {"title": "quasi_identifier"}) != base
    # ... and so does moving from the legacy classifier to a declaration.
    assert template_hash(vector_set, "jira_issue", None) != base
    # A class for an attribute the header does not name does not.
    assert template_hash(
        vector_set, "jira_issue", {"title": "non_pii", "owner": "direct_identifier"},
    ) == base
    # Per subtype, and per template.
    assert template_hash(vector_set, "chunk", None) != template_hash(
        vector_set, "jira_issue", None,
    )
    other = _set(document={"types": {"jira_issue": {"header": ["subtype"]}}})
    assert template_hash(other, "jira_issue", None) != template_hash(
        vector_set, "jira_issue", None,
    )


def test_check_templates_refuses_a_masked_type_field_and_reports_defaults() -> None:
    vector_set = _set(**JIRA)
    with pytest.raises(SubstrateError) as caught:
        check_templates(
            vector_set,
            {"jira_issue": {"title": "non_pii", "status": "direct_identifier"}},
            ["jira_issue"],
        )
    assert caught.value.code == "embedding_header_field_masked"
    assert caught.value.data["fields_by_subtype"] == {"jira_issue": ["status", "labels"]}
    omitted = check_templates(
        vector_set,
        {
            "jira_issue": {"title": "non_pii", "status": "non_pii", "labels": "non_pii"},
            "person": {"title": "direct_identifier"},
        },
        ["jira_issue", "person", "chunk"],
    )
    assert omitted == {"person": ["title"]}


def test_must_fail_the_blank_test_pattern_is_ascii_and_names_every_space() -> None:
    """Lane EH review P12 S2: the pattern is a bound query parameter, so a
    non-ASCII character in it raises UnicodeEncodeError on a SQL_ASCII or
    LATIN1 client encoding and breaks every semantic search and coverage
    query. Must-fail: the pattern held raw U+0085, U+00A0, U+3000 and more.
    It must still name exactly the characters str.strip() removes."""
    pattern = NONBLANK_BODY_REGEX
    assert pattern.isascii(), sorted({c for c in pattern if not c.isascii()})
    pattern.encode("ascii")
    assert pattern.startswith("[^") and pattern.endswith("]")
    escapes = pattern[2:-1].split("\\u")
    assert escapes[0] == ""
    named = [chr(int(code, 16)) for code in escapes[1:]]
    assert all(len(code) == 4 for code in escapes[1:])
    assert named == [chr(c) for c in range(0x10000) if chr(c).isspace()]
