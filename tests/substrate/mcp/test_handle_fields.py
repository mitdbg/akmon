"""`handle_fields`: a record's title, path and date from its catalog (no DB).

PACT `read-model-s1-catalog-fields`, design `read-model` D11.
"""

from __future__ import annotations

import pytest

from okg.substrate.mcp.handle_fields import (
    FIELD_ABSENT,
    FIELD_DECLARED,
    FIELD_FALLBACK_LIST,
    FIELD_MASKED,
    FIELD_NOT_DECLARED,
    FIELD_NOT_TEXT,
    FIELD_UNPARSEABLE,
    HandleField,
    handle_fields,
    normalize_handle_date,
)
from okg.substrate.mcp.passage_rollup import (
    PARENT_TITLE_ATTRS,
    PARENT_TITLE_MAX_CHARS,
)
from okg.substrate.nomos.read_masking import (
    MASK_TOKEN,
    READ_DROP_CLASSES,
    REDACTION_MARKER,
    mask_attrs,
)


_COMMIT_META = {
    "title_from": "message_subject",
    "created_at_from": "authored_at",
    "last_modified_at_from": "committed_at",
}


# ---------- title ----------


def test_declared_title_is_read_from_its_attribute():
    fields = handle_fields(
        _COMMIT_META,
        {"message_subject": "read-model: catalog fields", "title": "ignored"},
    )
    assert fields.title == HandleField(
        "read-model: catalog fields", "message_subject", FIELD_DECLARED,
    )


def test_declared_title_is_its_first_line_cut_like_parent_titles():
    long_line = "x" * (PARENT_TITLE_MAX_CHARS + 30)
    fields = handle_fields(
        {"title_from": "message"}, {"message": f"  {long_line}\nbody text"},
    )
    assert fields.title.value is not None
    assert len(fields.title.value) == PARENT_TITLE_MAX_CHARS
    assert fields.title.value.endswith("...")
    assert "\n" not in fields.title.value


@pytest.mark.parametrize(
    ("attrs", "status"),
    [
        ({}, FIELD_ABSENT),
        ({"message_subject": None}, FIELD_ABSENT),
        ({"message_subject": "   "}, FIELD_ABSENT),
        ({"message_subject": MASK_TOKEN}, FIELD_MASKED),
        ({"message_subject": 42}, FIELD_NOT_TEXT),
    ],
)
def test_declared_title_that_yields_nothing_never_falls_back(attrs, status):
    """The catalog said where the title lives: no guess past it."""
    attrs = {**attrs, "title": "a fixed-list title", "name": "a name"}
    fields = handle_fields(_COMMIT_META, attrs)
    assert fields.title == HandleField(None, "message_subject", status)


def test_undeclared_title_uses_todays_fixed_list_in_order():
    fields = handle_fields({}, {"name": "second", "summary": "first"})
    assert fields.title == HandleField("first", "summary", FIELD_FALLBACK_LIST)


def test_undeclared_title_fixed_list_is_the_passage_rollup_list():
    assert PARENT_TITLE_ATTRS == (
        "title", "summary", "name", "display_name", "label", "key", "change_id",
    )


def test_undeclared_title_skips_a_masked_fixed_list_value():
    fields = handle_fields({}, {"title": MASK_TOKEN, "name": "visible"})
    assert fields.title == HandleField("visible", "name", FIELD_FALLBACK_LIST)


def test_undeclared_title_with_no_fixed_list_attribute_is_no_title():
    """Attributes outside the list are never guessed at, however title-like."""
    fields = handle_fields(
        {}, {"message_subject": "looks like a title", "heading": "also"},
    )
    assert fields.title == HandleField(None, None, FIELD_NOT_DECLARED)


def test_declared_fields_dropped_by_the_read_filter_are_masked_not_absent():
    """A free-text class is dropped, not mask-tokened, on the read path.

    The handle must say "hidden", not "this record has no title/path/date".
    """
    meta = {**_COMMIT_META, "path_from": "path"}
    raw = {
        "message_subject": "secret subject",
        "path": "secret/path.py",
        "authored_at": "2026-10-02T14:05:00Z",
        "author": "visible",
    }
    attrs = mask_attrs(
        raw,
        classifier=lambda key: "non_pii" if key == "author" else "free_text",
        drop_classes=READ_DROP_CLASSES,
    )
    assert "message_subject" not in attrs and attrs[REDACTION_MARKER] is True
    fields = handle_fields(meta, attrs)
    assert fields.title == HandleField(None, "message_subject", FIELD_MASKED)
    assert fields.path == HandleField(None, "path", FIELD_MASKED)
    assert fields.date == HandleField(None, "authored_at", FIELD_MASKED)


def test_missing_declared_field_on_unredacted_attrs_is_absent():
    fields = handle_fields(
        {**_COMMIT_META, "path_from": "path"}, {REDACTION_MARKER: False},
    )
    assert fields.title.status == FIELD_ABSENT
    assert fields.path.status == FIELD_ABSENT
    assert fields.date.status == FIELD_ABSENT


# ---------- path ----------


def test_declared_path_is_read_from_its_attribute():
    fields = handle_fields(
        {"path_from": "doc_path"}, {"doc_path": " docs/use/index.md "},
    )
    assert fields.path == HandleField(
        "docs/use/index.md", "doc_path", FIELD_DECLARED,
    )


def test_undeclared_path_is_none_even_when_a_path_attribute_exists():
    fields = handle_fields(
        {}, {"path": "src/a.py", "member_path": "a/b", "doc_path": "c.md"},
    )
    assert fields.path == HandleField(None, None, FIELD_NOT_DECLARED)


@pytest.mark.parametrize(
    ("attrs", "status"),
    [
        ({}, FIELD_ABSENT),
        ({"path": ""}, FIELD_ABSENT),
        ({"path": MASK_TOKEN}, FIELD_MASKED),
        ({"path": ["a", "b"]}, FIELD_NOT_TEXT),
    ],
)
def test_declared_path_that_yields_nothing_is_none(attrs, status):
    fields = handle_fields({"path_from": "path"}, {**attrs, "doc_path": "x"})
    assert fields.path == HandleField(None, "path", status)


# ---------- date ----------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-10-02T14:05:00Z", "2026-10-02T14:05:00+00:00"),
        ("2026-10-02T14:05:00+02:00", "2026-10-02T14:05:00+02:00"),
        ("2026-10-02 14:05:00", "2026-10-02T14:05:00"),
        ("2026-10-02", "2026-10-02T00:00:00"),
        ("Fri, 02 Oct 2026 14:05:00 -0400", "2026-10-02T14:05:00-04:00"),
    ],
)
def test_declared_date_is_normalized_to_iso_8601(raw, expected):
    fields = handle_fields(_COMMIT_META, {"authored_at": raw})
    assert fields.date == HandleField(expected, "authored_at", FIELD_DECLARED)


def test_a_date_without_a_timezone_is_not_assumed_utc():
    assert normalize_handle_date("2026-10-02T14:05:00") == "2026-10-02T14:05:00"


def test_created_at_from_wins_over_last_modified_at_from():
    fields = handle_fields(
        _COMMIT_META,
        {"authored_at": "2026-01-01", "committed_at": "2026-02-02"},
    )
    assert fields.date.attr == "authored_at"
    assert fields.date.value == "2026-01-01T00:00:00"


def test_last_modified_at_from_is_used_when_it_is_the_only_date_declared():
    fields = handle_fields(
        {"last_modified_at_from": "last_modified_at"},
        {"last_modified_at": "2026-03-04T05:06:07Z"},
    )
    assert fields.date == HandleField(
        "2026-03-04T05:06:07+00:00", "last_modified_at", FIELD_DECLARED,
    )


def test_absent_declared_created_date_does_not_fall_back_to_last_modified():
    fields = handle_fields(_COMMIT_META, {"committed_at": "2026-02-02"})
    assert fields.date == HandleField(None, "authored_at", FIELD_ABSENT)


@pytest.mark.parametrize(
    ("raw", "status"),
    [
        (None, FIELD_ABSENT),
        ("", FIELD_ABSENT),
        (MASK_TOKEN, FIELD_MASKED),
        ("last Tuesday", FIELD_UNPARSEABLE),
        ("2026-13-45", FIELD_UNPARSEABLE),
        (1759413900, FIELD_UNPARSEABLE),
        (1759413900.5, FIELD_UNPARSEABLE),
        (True, FIELD_UNPARSEABLE),
    ],
)
def test_declared_date_that_is_not_a_date_says_why(raw, status):
    fields = handle_fields(_COMMIT_META, {"authored_at": raw})
    assert fields.date == HandleField(None, "authored_at", status)


def test_undeclared_date_is_none_even_with_a_created_at_attribute():
    fields = handle_fields({}, {"created_at": "2026-10-02", "date": "2026"})
    assert fields.date == HandleField(None, None, FIELD_NOT_DECLARED)


# ---------- whole record ----------


@pytest.mark.parametrize("meaning", ["created", "modified", "published", "resolved", "effective"])
def test_named_dates_are_independent_and_do_not_fall_back(meaning):
    from okg.substrate.mcp.handle_fields import DATE_MEANINGS, resolve_named_date

    meta = {key: f"{name}_value" for name, key in DATE_MEANINGS.items()}
    attrs = {f"{name}_value": "2026-10-03" for name in DATE_MEANINGS if name != meaning}
    result = resolve_named_date(meta, attrs, meaning)
    assert (result.meaning, result.attr, result.value, result.status, result.timezone) == (
        meaning, f"{meaning}_value", None, FIELD_ABSENT, None,
    )
    undeclared = resolve_named_date({}, attrs, meaning)
    assert undeclared.status == FIELD_NOT_DECLARED
    assert undeclared.value is None


@pytest.mark.parametrize("raw,status", [(MASK_TOKEN, FIELD_MASKED), ("bad date", FIELD_UNPARSEABLE)])
def test_named_date_masked_or_invalid_never_substitutes_creation(raw, status):
    from okg.substrate.mcp.handle_fields import resolve_named_date

    result = resolve_named_date({"created_at_from": "created", "effective_at_from": "effective"},
                                {"created": "2026-10-03", "effective": raw}, "effective")
    assert result.value is None
    assert result.attr == "effective"
    assert result.status == status
    dropped = resolve_named_date({"effective_at_from": "effective"},
                                 {REDACTION_MARKER: True}, "effective")
    assert dropped.status == FIELD_MASKED


@pytest.mark.parametrize("raw,expected,timezone,precision", [
    ("2026-10-03", "2026-10-03T00:00:00", "naive", "date"),
    ("20261003", "2026-10-03T00:00:00", "naive", "date"),
    ("2026-W40-6", "2026-10-03T00:00:00", "naive", "date"),
    ("2026W406", "2026-10-03T00:00:00", "naive", "date"),
    ("2026-10-03T00:00:00", "2026-10-03T00:00:00", "naive", "datetime"),
    ("2026-10-03T10:00:00", "2026-10-03T10:00:00", "naive", "datetime"),
    ("Sat, 03 Oct 2026 10:00:00 -0000", "2026-10-03T10:00:00", "naive", "datetime"),
    ("Sat, 03 Oct 2026 10:00:00", "2026-10-03T10:00:00", "naive", "datetime"),
    ("Sat, 03 Oct 2026 10:00:00 GMT", "2026-10-03T10:00:00+00:00", "aware", "datetime"),
    ("Mon, 14 May 2001 16:39:00 -0700 (PDT)", "2001-05-14T16:39:00-07:00", "aware", "datetime"),
    ("2026-10-03T10:00:00+02:00", "2026-10-03T10:00:00+02:00", "aware", "datetime"),
    ("2026-10-03T08:00:00Z", "2026-10-03T08:00:00+00:00", "aware", "datetime"),
    ("2026-10-03T08:00:00+00:00", "2026-10-03T08:00:00+00:00", "aware", "datetime"),
    ("2026-10-03T08:00:00-00:30", "2026-10-03T08:00:00-00:30", "aware", "datetime"),
])
def test_named_date_exposes_timezone_and_precision_without_inventing_them(
    raw, expected, timezone, precision,
):
    from okg.substrate.mcp.handle_fields import resolve_named_date

    result = resolve_named_date({"effective_at_from": "at"}, {"at": raw}, "effective")
    assert (result.value, result.status, result.timezone, result.precision) == (
        expected, FIELD_DECLARED, timezone, precision,
    )


def test_date_only_value_is_distinguishable_from_a_real_midnight():
    from okg.substrate.mcp.handle_fields import (
        PRECISION_DATE, PRECISION_DATETIME, resolve_named_date,
    )

    meta = {"effective_at_from": "at"}
    day = resolve_named_date(meta, {"at": "2026-10-03"}, "effective")
    midnight = resolve_named_date(meta, {"at": "2026-10-03T00:00:00"}, "effective")
    assert day.value == midnight.value
    assert (day.precision, midnight.precision) == (PRECISION_DATE, PRECISION_DATETIME)


@pytest.mark.parametrize("raw", [
    "2026-10-03T10:00:00-00:00",
    "2026-10-03T10:00:00-0000",
    "2026-10-03 10:00:00-0000",
    "2026-10-03T10:00:00-00:00:00",
])
def test_rfc3339_unknown_offset_is_naive_not_utc(raw):
    from okg.substrate.mcp.handle_fields import resolve_named_date

    result = resolve_named_date({"effective_at_from": "at"}, {"at": raw}, "effective")
    assert (result.value, result.timezone, result.precision) == (
        "2026-10-03T10:00:00", "naive", "datetime",
    )


@pytest.mark.parametrize("raw", [
    # An ISO week without a day is seven days, not one date.
    "2026-W40",
    "2026W40",
    # A zone name RFC 2822 does not define: its offset is unknowable.
    "Sat, 03 Oct 2026 10:00:00 CEST",
    "Sat, 03 Oct 2026 10:00:00 (CEST)",
    # Bad offset components before a trailing comment, or at the end.
    "Sat, 03 Oct 2026 10:00:00 +0260 (X)",
    "Sat, 03 Oct 2026 10:00:00 +0260",
    "Sat, 03 Oct 2026 10:00:00 +2400 (X)",
    "Sat, 30 Feb 2026 10:00:00 +0000",
    "Sat, 03 Oct 2026 10:00:00 +02:60",
    # A name that is not a weekday.
    "Xyz, 03 Oct 2026 10:00:00 +0000",
    # A ten-digit number (a phone number in a private deployment's archive).
    "4460012345",
])
def test_ambiguous_or_invalid_dates_are_refused(raw):
    from okg.substrate.mcp.handle_fields import resolve_named_date

    assert normalize_handle_date(raw) is None
    result = resolve_named_date({"effective_at_from": "at"}, {"at": raw}, "effective")
    assert (result.value, result.status, result.timezone, result.precision) == (
        None, FIELD_UNPARSEABLE, None, None,
    )


@pytest.mark.parametrize("raw", ["2025-02-29", "2026-02-30", "2026-10-03T08:00:00+25:00",
                                  "2026-10-03T08:00:00+00:60", "2026-10-03T08:00:00+00:00:60"])
def test_invalid_calendar_and_offset_dates_are_refused(raw):
    assert normalize_handle_date(raw) is None


def test_unknown_named_date_has_no_implicit_generic_meaning():
    from okg.substrate.mcp.handle_fields import resolve_named_date

    with pytest.raises(ValueError, match="valid meanings: created, modified, published, resolved, effective"):
        resolve_named_date(_COMMIT_META, {}, "date")


def test_display_date_meaning_is_additive_and_preserves_values():
    fields = handle_fields(_COMMIT_META, {"authored_at": "2026-10-03"})
    assert fields.date_meaning == "created"
    assert set(fields.values()) == {"title", "path", "date"}
    assert handle_fields({"last_modified_at_from": "at"}, {}).date_meaning == "modified"
    assert handle_fields({"effective_at_from": "at"}, {"at": "2026-10-03"}).date_meaning is None


def test_no_metadata_and_no_attrs_is_three_nones():
    fields = handle_fields(None, None)
    assert fields.values() == {"title": None, "path": None, "date": None}
    assert {f.status for f in (fields.title, fields.path, fields.date)} == {
        FIELD_NOT_DECLARED,
    }


def test_values_returns_the_three_fields():
    fields = handle_fields(
        {"title_from": "title", "path_from": "path",
         "last_modified_at_from": "last_modified_at"},
        {"title": "Install", "path": "docs/install.md",
         "last_modified_at": "2026-09-30T12:00:00Z"},
    )
    assert fields.values() == {
        "title": "Install",
        "path": "docs/install.md",
        "date": "2026-09-30T12:00:00+00:00",
    }


def test_resolver_reads_composed_catalog_metadata(tmp_path):
    """The resolver takes the metadata compose writes, key for key."""
    import json

    from okg.substrate.catalog.modules_compose import compose_catalog

    library = tmp_path / "library" / "git_like"
    library.mkdir(parents=True)
    (library / "schema.yaml").write_text(
        "classes:\n  Change:\n    annotations:\n      archetype: process\n"
        "      okg_title_from: subject\n      okg_path_from: file\n"
        "      okg_created_at_from: at\n"
        "    attributes:\n      subject: {}\n      file: {}\n      at: {}\n"
    )
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        json.dumps({"name": "fixture", "modules": ["git_like"]}),
    )
    cross_links = tmp_path / "cross_links"
    cross_links.mkdir()
    composed = compose_catalog(
        deployment,
        modules_root=tmp_path / "library",
        cross_links_root=cross_links,
    )
    fields = handle_fields(
        composed.subtype_metadata["change"],
        {"subject": "Fix it", "file": "a/b.py", "at": "2026-10-01T00:00:00Z"},
    )
    assert fields.values() == {
        "title": "Fix it",
        "path": "a/b.py",
        "date": "2026-10-01T00:00:00+00:00",
    }


@pytest.mark.parametrize("raw,expected", [
    # 3 October 2026 is a Saturday; the weekday is redundant and ignored.
    ("Fri, 03 Oct 2026 10:00:00 +0000", "2026-10-03T10:00:00+00:00"),
    ("Fri, 03 Oct 2026 10:00:00 -0000", "2026-10-03T10:00:00"),
    # A real Gmail value from a private deployment: 14 February 2028 is a Monday.
    ("Tue, 14 Feb 2028 10:02:00 -0800", "2028-02-14T10:02:00-08:00"),
])
def test_email_weekday_that_contradicts_the_date_is_ignored(raw, expected):
    assert normalize_handle_date(raw) == expected


@pytest.mark.parametrize("raw,expected,timezone", [
    # A real Gmail value from a private deployment; the offset must be kept, never dropped.
    ("Mon, 21 Jun 2027 10:05:00 -07:00", "2027-06-21T10:05:00-07:00", "aware"),
    ("Sat, 03 Oct 2026 10:00:00 +05:30 (IST)", "2026-10-03T10:00:00+05:30", "aware"),
    ("Sat, 03 Oct 2026 10:00:00 -00:00", "2026-10-03T10:00:00", "naive"),
])
def test_email_offset_written_with_a_colon_is_kept(raw, expected, timezone):
    from okg.substrate.mcp.handle_fields import resolve_named_date

    result = resolve_named_date({"effective_at_from": "at"}, {"at": raw}, "effective")
    assert (result.value, result.status, result.timezone, result.precision) == (
        expected, FIELD_DECLARED, timezone, "datetime",
    )


@pytest.mark.parametrize("raw,expected,timezone", [
    # XML Schema date form: a day in a zone, never a clock time.
    ("2026-10-03-07:00", "2026-10-03T00:00:00-07:00", "aware"),
    ("2026-10-03+05:30", "2026-10-03T00:00:00+05:30", "aware"),
    ("2026-10-03+14:00", "2026-10-03T00:00:00+14:00", "aware"),
    ("2026-10-03-14:00", "2026-10-03T00:00:00-14:00", "aware"),
    ("2026-W40-6-07:00", "2026-10-03T00:00:00-07:00", "aware"),
    ("2026-10-03Z", "2026-10-03T00:00:00+00:00", "aware"),
    ("2026-10-03-00:00", "2026-10-03T00:00:00", "naive"),
])
def test_date_followed_by_an_offset_is_a_day_in_that_zone(raw, expected, timezone):
    from okg.substrate.mcp.handle_fields import resolve_named_date

    result = resolve_named_date({"effective_at_from": "at"}, {"at": raw}, "effective")
    assert (result.value, result.status, result.timezone, result.precision) == (
        expected, FIELD_DECLARED, timezone, "date",
    )


@pytest.mark.parametrize("raw", [
    "2026100312304",  # 13 digits: not a date
    "2026-10-03.12",  # separator other than T or one space
    "2026-10-03é12:00",
    "2026-10-03t10:00",
    "2026-10-03  10:00",
    "٢٠٢٦-10-03",  # non-ASCII digits in the ISO path
    "Sat, ٠٣ Oct 2026 10:00:00 +0000",  # and in the email path
    "Sat, 03 Oct 2026 10:00:00 +٠٢٠٠",
    "Sat, 03 Oct ٢٠٢٦ 10:00:00 +0000",
])
def test_iso_separator_digit_strings_and_non_ascii_digits_are_refused(raw):
    assert normalize_handle_date(raw) is None


def test_hours_only_unknown_offset_is_naive():
    from okg.substrate.mcp.handle_fields import resolve_named_date

    result = resolve_named_date(
        {"effective_at_from": "at"}, {"at": "2026-10-03T12:00-00"}, "effective",
    )
    assert (result.value, result.timezone, result.precision) == (
        "2026-10-03T12:00:00", "naive", "datetime",
    )
    assert normalize_handle_date("2026-10-03T12:00-07") == "2026-10-03T12:00:00-07:00"


@pytest.mark.parametrize("raw,expected", [
    ("Sat, 03 Oct 26 10:00:00 +0000", "2026-10-03T10:00:00+00:00"),
    ("Sun, 03 Oct 49 10:00:00 +0000", "2049-10-03T10:00:00+00:00"),
    ("Tue, 03 Oct 50 10:00:00 +0000", "1950-10-03T10:00:00+00:00"),
    ("Thu, 03 Oct 68 10:00:00 +0000", "1968-10-03T10:00:00+00:00"),
    ("Sun, 03 Oct 99 10:00:00 +0000", "1999-10-03T10:00:00+00:00"),
    ("Sat, 03 Oct 126 10:00:00 +0000", "2026-10-03T10:00:00+00:00"),
])
def test_email_years_follow_rfc_2822(raw, expected):
    assert normalize_handle_date(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    # One space before the offset after a time, as Git --date=iso, Ruby and Go write.
    ("2026-10-03 12:00:00 +0000", "2026-10-03T12:00:00+00:00"),
    ("2026-10-03 12:00:00 -0700", "2026-10-03T12:00:00-07:00"),
    ("2026-10-03T12:00:00 Z", "2026-10-03T12:00:00+00:00"),
    ("2026-10-03T12:00:00 +05:30", "2026-10-03T12:00:00+05:30"),
    ("2026-10-03 12:00:00 -0000", "2026-10-03T12:00:00"),
])
def test_one_space_before_an_offset_after_a_time_is_accepted(raw, expected):
    assert normalize_handle_date(raw) == expected


@pytest.mark.parametrize("raw", [
    "20261003-1230",  # a build stamp, not a day in a zone
    "20261003+05",
    "20261003Z",
    "2026W406-07:00",
    "2026-10-03-07",  # hour-only day offset
    "2026-10-03-23:59",  # beyond the XML Schema 14:00 limit
    "2026-10-03+14:01",
    "2026-10-03 -07:00",  # a space before a day offset
    "2026-10-03T12:00:00  Z",  # two spaces
    "2026-10-03T12:00+05:3000",  # mixed extended and basic offset
    "2026-10-03T12:00+0530:00",
])
def test_compact_or_out_of_range_day_offsets_and_mixed_offsets_are_refused(raw):
    assert normalize_handle_date(raw) is None
