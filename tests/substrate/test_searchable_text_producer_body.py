"""A producer's own `text` body stays searchable (issue #2991).

Before #2991 the searchable-text stamper rebuilt `attrs.text` from a
subtype's other declared fields and silently threw away the body a producer
had written into `text`: a record became its title, a cms-kb `Section` its
file stem, a markdown heading its slug. The body is now appended as the last
part of the rollup and kept under `PRODUCER_TEXT_ATTR`, so a later restamp
starts from that copy and not from the old rollup.

These tests need no database. The SQL mirror is pinned against the Python
writer in `test_searchable_text_path_fields.py` (postgres-marked). The
stamping tests go through the writer's own `_stamp_searchable_text`, which
exists before and after the change, so they run, and fail, against the
pre-#2991 code too.
"""
from __future__ import annotations

import pytest

from okg.substrate.graph_store_internal.writes import ChronosGraphWriteMixin
from okg.substrate.searchable_text import (
    ASCII_WHITESPACE,
    split_camel_value,
    split_path_value,
)

RESERVED = "okg_producer_text"
BODY = "Quarterly renewal blocked on the SSO bug; see JIRA-4411 for the fix."


class _Writer(ChronosGraphWriteMixin):
    """The real writer mixin with the catalog read replaced by a dict."""

    def __init__(
        self,
        fields: dict[str, list[str]],
        path_fields: dict[str, list[str]] | None = None,
    ) -> None:
        self._fields = fields
        self._path_fields = path_fields or {}

    def _searchable_text_fields(self, subtype, *, key="searchable_text_fields"):
        table = self._fields if key == "searchable_text_fields" else self._path_fields
        return list(table.get(subtype, []))


def _stamp(fields, attrs, *, path_fields=None, subtype="s"):
    writer = _Writer({subtype: list(fields)}, {subtype: list(path_fields or [])})
    return writer._stamp_searchable_text(subtype, dict(attrs), cache={})


def _dev_stamp(fields, attrs, path_fields=()):
    """The stamping rule as it stood on dev before #2991 (f5f7c81ca3), copied
    verbatim in behaviour: the reference for "no producer body -> byte-equal
    output"."""
    attrs = dict(attrs)
    if not fields:
        return attrs
    parts: list[str] = []
    text_value = None
    for name in fields:
        value = attrs.get(name)
        if name == "text":
            if isinstance(value, str) and value.strip(ASCII_WHITESPACE):
                text_value = value
            continue
        if isinstance(value, str):
            stripped = value.strip(ASCII_WHITESPACE)
            if stripped and stripped not in parts:
                parts.append(stripped)
    for name in path_fields:
        if name == "text":
            continue
        value = attrs.get(name)
        if not isinstance(value, str) or not value.strip(ASCII_WHITESPACE):
            continue
        split = split_path_value(value)
        if split and split not in parts:
            parts.append(split)
        camel = split_camel_value(value)
        if camel and camel not in parts:
            parts.append(camel)
    if parts:
        attrs["text"] = "\n\n".join(parts)
    elif text_value is not None:
        attrs["text"] = text_value
    return attrs


# --- producer body + another field: both searchable, body kept ------------


def test_title_and_body_are_both_searchable_and_the_body_is_kept() -> None:
    out = _stamp(["title", "text"], {"title": "Renewal blocker", "text": BODY})
    assert out["text"] == f"Renewal blocker\n\n{BODY}"
    assert out[RESERVED] == BODY
    assert out["title"] == "Renewal blocker"


def test_title_alone_declared_still_keeps_the_producer_body() -> None:
    """Listing `text` changes nothing: the body is the producer's either way."""
    listed = _stamp(["title", "text"], {"title": "T", "text": BODY})
    unlisted = _stamp(["title"], {"title": "T", "text": BODY})
    assert listed == unlisted
    assert unlisted["text"] == f"T\n\n{BODY}"


@pytest.mark.parametrize(
    ("fields", "attrs", "expected"),
    [
        # cms-kb Section (`stem,text`): the body, not the file stem alone.
        (["stem", "text"], {"stem": "03-Selection", "text": "Events pass HLT."},
         "03-Selection\n\nEvents pass HLT."),
        # library markdown_heading (`text,slug`): the heading, not the slug.
        (["text", "slug"], {"text": "Getting Started", "slug": "getting-started"},
         "getting-started\n\nGetting Started"),
        # OpenSpec reasoning bodies (`statement,status,domain,text`).
        (["statement", "status", "domain", "text"],
         {"statement": "S", "status": "open", "text": "## Rationale\nlong body"},
         "S\n\nopen\n\n## Rationale\nlong body"),
    ],
)
def test_issue_subtypes_keep_their_body(fields, attrs, expected) -> None:
    assert _stamp(fields, attrs)["text"] == expected


def test_body_is_the_last_part_after_path_splits() -> None:
    out = _stamp(
        ["path", "text"], {"path": "notes/SWGuideCrab.md", "text": BODY},
        path_fields=["path"],
    )
    assert out["text"] == (
        "notes/SWGuideCrab.md\n\nnotes SWGuideCrab md\n\n"
        f"notes SW Guide Crab md\n\n{BODY}"
    )


def test_body_is_kept_byte_for_byte() -> None:
    """#2972 stopped stripping `text`; the appended body is not stripped
    either, and the reserved copy is the exact producer value."""
    body = "  indented body\n\twith a tab \n"
    out = _stamp(["title", "text"], {"title": "T", "text": body})
    assert out["text"] == f"T\n\n{body}"
    assert out[RESERVED] == body


def test_body_repeating_a_part_is_not_appended_twice() -> None:
    """The body holds the title, so the title is left out (issue #3469) and
    the text is the body alone, byte for byte. The copy is still kept: a
    later declaration without `title` must get the body back (review round
    2 of #2991)."""
    out = _stamp(["title", "text"], {"title": "Same", "text": " Same\n"})
    assert out["text"] == " Same\n"
    assert out[RESERVED] == " Same\n"
    assert _stamp(["title", "text"], out) == out


def test_body_equal_to_a_path_split_is_not_appended_twice() -> None:
    """A split is never left out, so a body equal to one is not repeated."""
    out = _stamp(["p", "text"], {"p": "a/b", "text": "a b"}, path_fields=["p"])
    assert out["text"] == "a/b\n\na b"
    assert out[RESERVED] == "a b"


def test_text_only_declaration_is_unchanged_and_stores_no_copy() -> None:
    """A document chunk's `text` must equal its source slice (#2972)."""
    chunk = "  chunk slice with padding \n"
    out = _stamp(["text"], {"text": chunk, "char_offset": 0})
    assert out == {"text": chunk, "char_offset": 0}


def test_blank_other_fields_keep_the_body_as_the_whole_text() -> None:
    out = _stamp(["title", "text"], {"title": "   ", "text": BODY})
    assert out["text"] == BODY
    assert RESERVED not in out


# --- restamp is idempotent ------------------------------------------------


IDEMPOTENT_CASES = [
    (["title", "text"], {"title": "T", "text": BODY}, []),
    (["title"], {"title": "T", "text": BODY}, []),
    (["title", "x"], {"title": "T", "x": "X"}, []),
    (["title", "x", "text"], {"title": "T", "x": "X"}, []),
    (["title", "text"], {"title": "T", "text": "T"}, []),
    (["text"], {"text": BODY}, []),
    (["title", "text"], {"title": "  ", "text": BODY}, []),
    (["path", "text"], {"path": "a/BHello.md", "text": BODY}, ["path"]),
    (["title", "text"], {"title": "T", "text": "T\n\nX"}, []),
    (["a", "b", "text"], {"a": "A", "b": "B", "text": "A\n\nB\n\nC"}, []),
    (["title", "text"], {"title": "T", "text": "  padded body \n"}, []),
    (["title", "text"], {"title": "T", "text": 17}, []),
    # Issue #3469: parts the body holds are left out.
    (["key", "summary", "text"],
     {"key": "JIRA-4411", "summary": "SSO bug", "text": BODY}, []),
    (["key", "summary", "text"],
     {"key": "JIRA-4411", "summary": "Renewal", "text": BODY}, []),
    (["title", "text"], {"title": "C++ (v1.2)", "text": "use C++ (v1.2) now"}, []),
    (["path", "text"],
     {"path": "notes/SWGuideCrab.md", "text": "see notes/SWGuideCrab.md"},
     ["path"]),
]


@pytest.mark.parametrize(("fields", "attrs", "path_fields"), IDEMPOTENT_CASES)
def test_stamp_twice_equals_stamp_once(fields, attrs, path_fields) -> None:
    once = _stamp(fields, attrs, path_fields=path_fields)
    twice = _stamp(fields, once, path_fields=path_fields)
    assert twice == once


def test_restamp_of_stored_attrs_uses_the_reserved_copy() -> None:
    """Stored attrs whose title changed since the stamp go back through
    `unstamp_searchable_text` (the hand-off and catalog paths do), and the
    body survives rather than the old rollup being read as a body."""
    from okg.substrate.searchable_text import unstamp_searchable_text

    once = _stamp(["title", "text"], {"title": "T", "text": BODY})
    moved = {**once, "title": "New title"}
    producer = unstamp_searchable_text(dict(moved), ["title", "text"], [])
    assert producer == {"title": "New title", "text": BODY}
    out = _stamp(["title", "text"], producer)
    assert out["text"] == f"New title\n\n{BODY}"
    assert out[RESERVED] == BODY


def test_handoff_of_a_declaration_stamped_under_an_older_declaration() -> None:
    """A shared node handed to its surviving declarer is re-stamped from the
    declarer's STORED row, which may predate a declaration change. It goes
    back through `unstamp_searchable_text`, so the body is kept and the old
    rollup is not read as a new body."""
    stored = _stamp(["title", "summary", "text"],
                    {"title": "T", "summary": "S", "text": BODY})
    rows, refused = _Writer({"memo": ["title", "text"]})._prepare_handoff_rows(
        [{"node_id": "n1", "subtype": "memo", "attrs": stored, "source": "s"}],
        current_nodes={"n1": {"subtype": "memo"}},
        searchable_fields_cache={},
    )
    assert refused == []
    assert rows[0]["attrs"]["text"] == f"T\n\n{BODY}"
    assert rows[0]["attrs"][RESERVED] == BODY


def test_supplied_text_wins_over_a_stale_copy() -> None:
    """Review round 2, finding 3: a new `text` that merely ends with the old
    copy (stored attrs mixed with a new text) is the producer's body; it
    must not be cut back to the copy."""
    out = _stamp(["title", "text"],
                 {"title": "T", "text": "A\n\nC", RESERVED: "C"})
    assert out["text"] == "T\n\nA\n\nC"
    assert out[RESERVED] == "A\n\nC"


def test_a_rewritten_text_replaces_a_stale_reserved_copy() -> None:
    """A writer that re-emits stored attrs with a NEW `text` wins over the
    old copy: the copy is only trusted while `text` is still its stamp."""
    once = _stamp(["title", "text"], {"title": "T", "text": BODY})
    out = _stamp(["title", "text"], {**once, "text": "a new body"})
    assert out["text"] == "T\n\na new body"
    assert out[RESERVED] == "a new body"


def test_copy_is_trusted_only_after_the_part_separator() -> None:
    """A new body that merely ends with the old copy's words is still new."""
    once = _stamp(["title", "text"], {"title": "T", "text": "the fix"})
    out = _stamp(["title", "text"], {**once, "text": "apply the fix"})
    assert out["text"] == "T\n\napply the fix"
    assert out[RESERVED] == "apply the fix"


def test_stale_copy_is_dropped_when_no_body_is_appended() -> None:
    once = _stamp(["title", "text"], {"title": "T", "text": BODY})
    out = _stamp(["title", "text"], {**once, "text": "T"})
    assert out == {"title": "T", "text": "T"}


# --- issue #3469: a declared value the body holds is not repeated --------

JIRA = ["key", "summary", "text"]


def test_declared_values_the_body_holds_are_left_out() -> None:
    """A Jira issue's key and summary are already in its body: the text is
    the body alone, and the copy is kept so a restamp still finds it."""
    out = _stamp(JIRA, {"key": "JIRA-4411", "summary": "SSO bug", "text": BODY})
    assert out["text"] == BODY
    assert out[RESERVED] == BODY
    assert out["key"] == "JIRA-4411" and out["summary"] == "SSO bug"


def test_only_the_held_parts_are_left_out_in_declared_order() -> None:
    out = _stamp(
        ["title", "key", "summary", "text"],
        {"title": "Renewal blocker", "key": "JIRA-4411",
         "summary": "Quarterly", "text": BODY},
    )
    assert out["text"] == f"Renewal blocker\n\n{BODY}"
    assert out[RESERVED] == BODY


@pytest.mark.parametrize(
    ("part", "body"),
    [
        # Inside a longer word or number: not a whole token.
        ("HLT", "the HLTConfig module"),
        ("HLT", "see L1HLT"),
        ("JIRA-4411", "see JIRA-44110"),
        ("JIRA-4411", "see aJIRA-4411"),
        ("C++ (v1.2)", "C++ (v1.2)x"),
        # Case and punctuation count.
        ("SSO bug", "the sso bug"),
        ("SSO bug", "the SSO-bug"),
        # Regex metacharacters are literal, never a pattern.
        ("a.b*c", "aXbbbc"),
        ("x|y", "only x here"),
        ("[z]", "z"),
        ("(?i)t", "T"),
        ("\\d", "7"),
    ],
)
def test_a_part_the_body_does_not_hold_as_a_whole_token_is_kept(part, body) -> None:
    out = _stamp(["title", "text"], {"title": part, "text": body})
    assert out["text"] == f"{part}\n\n{body}"
    assert out[RESERVED] == body


@pytest.mark.parametrize(
    ("part", "body"),
    [
        ("HLT", "Events pass HLT."),
        ("HLT", "HLT\n"),
        ("HLT", "(HLT)"),
        ("HLT", "HLT_v2 and HLTConfig"),  # `_` is not a letter or digit
        ("HLT", "ÉHLT"),  # a non-ASCII neighbour does not count
        ("JIRA-4411", "JIRA-44110 and JIRA-4411"),  # a later occurrence
        ("C++ (v1.2)", "use C++ (v1.2) now"),
        ("a.b*c", "x a.b*c!"),
        ("x|y", "(x|y)"),
        ("[z]", "a [z]"),
        ("100%", "at 100%."),
        ("it's", "it's 'quoted'"),
        ("line one\nline two", "x line one\nline two y"),
    ],
)
def test_a_part_the_body_holds_as_a_whole_token_is_left_out(part, body) -> None:
    out = _stamp(["title", "text"], {"title": part, "text": body})
    assert out["text"] == body
    assert out[RESERVED] == body


def test_path_and_camel_splits_are_always_kept() -> None:
    """The raw path is left out (the body holds it); its separator split and
    CamelCase split stay, since they make the component words searchable."""
    out = _stamp(
        ["path", "text"],
        {"path": "notes/SWGuideCrab.md",
         "text": "see notes/SWGuideCrab.md and notes SWGuideCrab md"},
        path_fields=["path"],
    )
    assert out["text"] == (
        "notes SWGuideCrab md\n\nnotes SW Guide Crab md\n\n"
        "see notes/SWGuideCrab.md and notes SWGuideCrab md"
    )


def test_no_body_still_stamps_every_part() -> None:
    out = _stamp(JIRA, {"key": "JIRA-4411", "summary": "SSO bug"})
    assert out == {"key": "JIRA-4411", "summary": "SSO bug",
                   "text": "JIRA-4411\n\nSSO bug"}


def _legacy(fields, attrs, path_fields=()):
    """What the #2991 rule stored: every part, then the body, plus the copy."""
    from okg.substrate.searchable_text import searchable_parts

    parts = searchable_parts(attrs, list(fields), list(path_fields))
    body = attrs["text"]
    text = "\n\n".join(parts)
    if body.strip(ASCII_WHITESPACE) not in parts:
        text = f"{text}\n\n{body}"
    return {**attrs, "text": text, RESERVED: body}


LEGACY_CASES = [
    (JIRA, {"key": "JIRA-4411", "summary": "SSO bug", "text": BODY}, []),
    (JIRA, {"key": "JIRA-4411", "summary": "Renewal", "text": BODY}, []),
    (["title", "text"], {"title": "Same", "text": " Same\n"}, []),
    (["path", "text"],
     {"path": "notes/SWGuideCrab.md", "text": "see notes/SWGuideCrab.md"},
     ["path"]),
]


@pytest.mark.parametrize(("fields", "attrs", "path_fields"), LEGACY_CASES)
def test_node_stored_under_the_old_rule_restamps_to_the_new_form(
    fields, attrs, path_fields,
) -> None:
    """A node stored as every part then the body must restamp to the shorter
    form; reading its old text as a new body would double it again."""
    fresh = _stamp(fields, attrs, path_fields=path_fields)
    stored = _legacy(fields, attrs, path_fields)
    restamped = _stamp(fields, stored, path_fields=path_fields)
    assert restamped == fresh
    assert _stamp(fields, restamped, path_fields=path_fields) == restamped


@pytest.mark.parametrize(("fields", "attrs", "path_fields"), LEGACY_CASES)
def test_unstamp_gives_back_the_body_for_old_and_new_forms(
    fields, attrs, path_fields,
) -> None:
    from okg.substrate.searchable_text import (
        unstamp_searchable_text,
        unstamp_without_declaration,
    )

    producer = {k: v for k, v in attrs.items()}
    for stored in (
        _stamp(fields, attrs, path_fields=path_fields),
        _legacy(fields, attrs, path_fields),
    ):
        assert unstamp_searchable_text(
            dict(stored), list(fields), list(path_fields),
        ) == producer
        assert unstamp_without_declaration(dict(stored)) == producer


def test_old_form_survives_a_catalog_change_and_a_handoff() -> None:
    from okg.substrate.searchable_text import unstamp_searchable_text

    attrs = {"key": "JIRA-4411", "summary": "SSO bug", "title": "T", "text": BODY}
    stored = _legacy(JIRA, attrs)
    new = ["title", "key", "summary", "text"]
    producer = unstamp_searchable_text(dict(stored), JIRA, [])
    assert _stamp(new, producer) == _stamp(new, attrs)
    assert _stamp(new, attrs)["text"] == f"T\n\n{BODY}"
    rows, refused = _Writer({"memo": new})._prepare_handoff_rows(
        [{"node_id": "n1", "subtype": "memo", "attrs": stored, "source": "s"}],
        current_nodes={"n1": {"subtype": "memo"}},
        searchable_fields_cache={},
    )
    assert refused == []
    assert rows[0]["attrs"] == _stamp(new, attrs)


# --- catalog-change restamp keeps the body --------------------------------


def _catalog_restamp(old, new, attrs, *, old_path=(), new_path=()):
    """Stamp under `old`, then run the catalog restamp step: unstamp under
    the prior declaration and stamp under the new one."""
    from okg.substrate.searchable_text import unstamp_searchable_text

    stored = _stamp(old, attrs, path_fields=list(old_path))
    # The catalog restamp keeps an old plain rollup when the new declaration
    # is empty (modules_apply._restamp_changed_subtypes).
    keep = {"keep_plain_rollup": True} if not new else {}
    producer = unstamp_searchable_text(dict(stored), list(old), list(old_path), **keep)
    return _stamp(new, producer, path_fields=list(new_path))


@pytest.mark.parametrize(
    ("old", "new", "attrs", "old_path", "new_path"),
    [
        # A field is added: body kept, not doubled.
        (["title", "text"], ["title", "summary", "text"],
         {"title": "T", "summary": "S", "text": BODY}, [], []),
        # A field is removed.
        (["title", "summary", "text"], ["title", "text"],
         {"title": "T", "summary": "S", "text": BODY}, [], []),
        # A path declaration is added.
        (["path", "text"], ["path", "text"],
         {"path": "a/b.md", "text": BODY}, [], ["path"]),
        # Text-only becomes title + text: the stored body was never a rollup.
        (["text"], ["title", "text"], {"title": "T", "text": BODY}, [], []),
        # No producer body at all: the old rollup is not read as one.
        (["title", "x"], ["title", "x", "y"],
         {"title": "T", "x": "X", "y": "Y"}, [], []),
        (["title", "x"], ["x"], {"title": "T", "x": "X"}, [], []),
        # The subtype had no declaration before.
        ([], ["title", "text"], {"title": "T", "text": BODY}, [], []),
        # Issue #3469: a new field the body already holds is left out.
        (["title", "text"], ["title", "key", "text"],
         {"title": "T", "key": "JIRA-4411", "text": BODY}, [], []),
        (["title", "key", "text"], ["title", "text"],
         {"title": "T", "key": "JIRA-4411", "text": BODY}, [], []),
        # Review round 2, finding 2: a body that repeats a part.
        (["title", "summary"], ["title"],
         {"title": "T", "summary": "S", "text": "S"}, [], []),
        (["title", "summary", "text"], ["title", "text"],
         {"title": "T", "summary": "S", "text": " S\n"}, [], []),
        # ... and a declaration that goes away: dev kept the text, and so
        # must the restamp (it used to drop it, taking the node out of search).
        (["title"], [], {"title": "T", "text": "T"}, [], []),
        (["title", "text"], [], {"title": "T", "text": BODY}, [], []),
    ],
)
def test_catalog_change_restamp_equals_a_fresh_stamp(
    old, new, attrs, old_path, new_path,
) -> None:
    restamped = _catalog_restamp(
        old, new, attrs, old_path=old_path, new_path=new_path,
    )
    fresh = _stamp(new, attrs, path_fields=list(new_path))
    assert restamped == fresh
    if isinstance(attrs.get("text"), str):
        assert attrs["text"] in restamped["text"]


def test_removed_declaration_keeps_a_plain_rollup_like_dev() -> None:
    """A node with no body keeps its old rollup when its subtype stops
    declaring searchable fields: the stamper then leaves attrs alone, and dev
    kept that text. Removing it would drop the node from search and from
    embedding."""
    assert _catalog_restamp(["title"], [], {"title": "T"}) == {
        "title": "T", "text": "T",
    }
    assert _catalog_restamp(["title", "x"], [], {"title": "T", "x": "X"}) == {
        "title": "T", "x": "X", "text": "T\n\nX",
    }


def test_body_equal_to_the_whole_rollup_is_indistinguishable_from_no_body() -> None:
    """Review round 2, finding 2, first repro, and why it cannot match a fresh
    stamp. A producer whose `text` is exactly the multi-part rollup stores the
    SAME attrs as a node with no body, byte for byte, because a no-body node
    must stamp exactly as dev did and stamping twice must equal stamping
    once. A restamp therefore cannot tell them apart and gives the no-body
    answer: the new declaration's rollup."""
    decl = ["title", "summary", "text"]
    with_body = _stamp(decl, {"title": "T", "summary": "S", "text": "T\n\nS"})
    no_body = _stamp(decl, {"title": "T", "summary": "S"})
    assert with_body == no_body
    restamped = _catalog_restamp(
        decl, ["title", "text"], {"title": "T", "summary": "S", "text": "T\n\nS"},
    )
    assert restamped == {"title": "T", "summary": "S", "text": "T"}


class _PriorCatalogCursor:
    """Answers the two reads `_subtypes_with_changed_search_fields` makes."""

    def __init__(self, prior_rows):
        self._prior_rows = prior_rows
        self._last = None

    def execute(self, sql, params=None):
        self._last = sql

    def fetchone(self):
        return (1,)

    def fetchall(self):
        return list(self._prior_rows)


def test_catalog_apply_hands_the_prior_declaration_to_the_restamp() -> None:
    """The restamp can only unstamp under the declaration a node was stamped
    with if the catalog diff reports it; an unchanged subtype reports none."""
    from okg.substrate.catalog.modules_apply import (
        _subtypes_with_changed_search_fields,
    )
    from okg.substrate.catalog.modules_compose import ComposedCatalog

    composed = ComposedCatalog(deployment_name="t")
    composed.subtypes = {"memo", "same"}
    composed.subtype_metadata = {
        "memo": {"searchable_text_fields": ["title", "summary", "text"]},
        "same": {"searchable_text_fields": ["title"]},
    }
    cursor = _PriorCatalogCursor([
        ("memo", ["title", "text"], ["title"]),
        ("same", ["title"], None),
    ])
    prior: dict = {}
    changed = _subtypes_with_changed_search_fields(
        cursor, 2, composed, prior_out=prior,
    )
    assert changed == ["memo"]
    assert prior == {"memo": (["title", "text"], ["title"])}


# --- no producer body: byte-equal to dev ----------------------------------


NO_BODY_CASES = [
    (["title", "x"], {"title": "T", "x": "X"}, []),
    (["title", "text"], {"title": "T"}, []),
    (["title", "text"], {"title": "T", "text": "   "}, []),
    (["title", "text"], {"title": "T", "text": ""}, []),
    (["title", "text"], {"title": "T", "text": 17}, []),
    (["title", "text"], {"title": "T", "text": None}, []),
    (["title"], {"title": " padded \t"}, []),
    (["title", "text"], {"text": "   "}, []),
    (["title", "text"], {}, []),
    (["q", "p"], {"q": "x", "p": "src/app.py"}, ["p"]),
    (["q", "p"], {"q": "SWGuideCrab", "p": "SWGuideCrab"}, ["p"]),
    (["q", "p"], {"q": " nbsp ", "p": "a/b"}, ["p"]),
    ([], {"title": "T", "text": BODY}, []),
    (["text"], {"text": BODY}, []),
    (["text"], {"text": "  keep padding \n"}, []),
    (["title", "text"], {"title": "T", "text": "T"}, []),
    (["a", "b", "text"], {"a": "A", "b": "B", "text": "A\n\nB"}, []),
]


@pytest.mark.parametrize(("fields", "attrs", "path_fields"), NO_BODY_CASES)
def test_no_producer_body_stamps_what_dev_stamped(fields, attrs, path_fields) -> None:
    assert _stamp(fields, attrs, path_fields=path_fields) == _dev_stamp(
        fields, attrs, path_fields,
    )


# --- catalog composition refuses the reserved key -------------------------


def test_reserved_key_cannot_be_declared_searchable(tmp_path) -> None:
    import yaml

    from okg.substrate.catalog.modules_compose import (
        ComposedCatalog,
        _extract_subtypes,
    )

    schema = tmp_path / "schema.yaml"
    schema.write_text(yaml.safe_dump({"classes": {"Memo": {"annotations": {
        "archetype": "artifact",
        "okg_searchable_text_fields": f"title,{RESERVED}",
    }}}}))
    with pytest.raises(ValueError, match="reserved"):
        _extract_subtypes(schema, "test", ComposedCatalog(deployment_name="test"))


# --- quickstart bootstrap defaults ---------------------------------------


def test_bootstrap_default_subtypes_search_title_and_body() -> None:
    from okg.substrate.deployment_bootstrap.workflow import _planned_subtype_defs

    record, finding = _planned_subtype_defs({"deployment": {"name": "qs"}})
    assert record["searchable_text_fields"] == ["title", "body"]
    assert "body" in record["attrs"] and "text" not in record["attrs"]
    assert finding["searchable_text_fields"] == ["label", "body"]
    assert "body" in finding["attrs"] and "text" not in finding["attrs"]


def test_bootstrap_record_is_searchable_by_a_body_only_word() -> None:
    from okg.substrate.deployment_bootstrap.workflow import (
        _fact_fixture_for_records,
        _planned_subtype_defs,
    )

    record_def, finding_def = _planned_subtype_defs({"deployment": {"name": "qs"}})
    records = [{
        "id": "REC-2",
        "summary": "Dataset: SALINITY-2026",
        "description": "Coastal measurements collected by the buoy array.",
    }]
    fixture = _fact_fixture_for_records(
        source_id="records", records=records,
        record_subtype="qs_record", finding_subtype="qs_finding",
    )
    writer = _Writer({
        "qs_record": record_def["searchable_text_fields"],
        "qs_finding": finding_def["searchable_text_fields"],
    })
    by_subtype = {node["subtype"]: node for node in fixture["nodes"]}
    record = writer._stamp_searchable_text(
        "qs_record", dict(by_subtype["qs_record"]["attrs"]), cache={},
    )
    assert record["title"] == "Dataset: SALINITY-2026"
    assert "buoy" not in record["title"]
    assert "buoy" in record["body"]
    # The body is written once, into `body`: no producer `text`, no copy.
    assert RESERVED not in record
    assert record["text"] == (
        "Dataset: SALINITY-2026\n\n"
        "Coastal measurements collected by the buoy array."
    )
    finding = writer._stamp_searchable_text(
        "qs_finding", dict(by_subtype["qs_finding"]["attrs"]), cache={},
    )
    assert "buoy" in finding["text"]


# --- the reserved copy is system state: merge and reads -------------------


def test_entity_merge_never_offers_the_copy_and_chooses_between_bodies() -> None:
    """Review round 2, finding 3: entity merge compares producer bodies. The
    copy is never a conflict of its own, the chosen body is what the survivor
    write stamps again, and two nodes with the same body need no write."""
    from okg.substrate.entity_merge import _merge_attributes

    decl = ["title", "text"]
    survivor = _stamp(decl, {"title": "T", "text": "survivor body"})
    retired = _stamp(decl, {"title": "T", "text": "retired body"})
    conflicts, merged = _merge_attributes(
        survivor, retired,
        overrides={"text": "keep_retiree"}, declared={},
        accept_survivor_attributes=False,
    )
    assert [c["attribute"] for c in conflicts] == ["text"]
    assert conflicts[0]["survivor"] == "survivor body"
    assert conflicts[0]["retired"] == "retired body"
    assert merged == {"title": "T", "text": "retired body"}
    assert _stamp(decl, merged) == retired

    same = _stamp(decl, {"title": "T", "text": "survivor body"})
    conflicts, merged = _merge_attributes(
        survivor, same, overrides={}, declared={},
        accept_survivor_attributes=False,
    )
    assert conflicts == () and merged is None


def test_compact_reads_leave_out_the_copy() -> None:
    """Review round 2, finding 4: MCP reads return the body once, in `text`."""
    from okg.substrate.mcp.response_shape import (
        compact_for_wire,
        move_lineage_to_rows,
    )

    stamped = _stamp(["stem", "text"], {"stem": "03-Selection", "text": BODY})
    node = {"operator_name": "inspect", "node_id": "a", "attrs": stamped}
    out = compact_for_wire(node, {"target": {"kind": "node"}})
    assert out["attrs"] == {
        "stem": "03-Selection", "text": f"03-Selection\n\n{BODY}",
    }
    page = {"operator_name": "expand", "results": [dict(node)]}
    assert RESERVED not in compact_for_wire(page, {})["results"][0]["attrs"]
    moved = move_lineage_to_rows(dict(node), drop_attrs=False, single_row=True)
    assert RESERVED not in moved["attrs"]


def test_console_record_fields_leave_out_the_copy() -> None:
    from okg.substrate.console_views.records import _record_fields_and_sections

    stamped = _stamp(["stem", "text"], {"stem": "03-Selection", "text": BODY})
    fields, sections = _record_fields_and_sections("section", stamped)
    keys = {f.get("key") for f in fields} | {s.get("key") for s in sections}
    assert RESERVED not in keys
    assert "stem" in keys


# --- review round 3 --------------------------------------------------------

SECRET = "SECRET salary figures for Alice"


def _secret_node() -> dict:
    return _stamp(["title", "text"], {"title": "Quarterly review", "text": SECRET})


def _values(obj) -> str:
    return repr(obj)


def test_redacted_read_without_pii_classes_never_returns_the_body() -> None:
    """Review round 3, finding 1 (security): the read filter's redact effect
    on a subtype with no `pii_classes` used to return the copy unmasked."""
    from okg.substrate.nomos.read_masking import (
        READ_DROP_CLASSES,
        mask_attrs,
        subtype_attr_classifier,
    )

    stamped = _secret_node()
    assert stamped[RESERVED] == SECRET
    out = mask_attrs(
        stamped, classifier=subtype_attr_classifier(None),
        drop_classes=READ_DROP_CLASSES,
    )
    assert SECRET not in _values(out)
    assert out == {"title": "Quarterly review", "nomos_redacted": True}


def test_redacted_export_never_returns_the_body() -> None:
    from okg.substrate.nomos.store import _redacted_attrs

    out = _redacted_attrs(_secret_node())
    assert SECRET not in _values(out)
    assert out == {"title": "Quarterly review", "nomos_redacted": True}


@pytest.mark.parametrize(
    ("pii_classes", "expected_copy"),
    [
        # `text` unlisted: both masked, never revealed.
        ({"title": "non_pii"}, "[nomos:masked]"),
        # `text` dropped: the copy is dropped with it.
        ({"title": "non_pii", "text": "free_text"}, None),
        # `text` revealed: the copy says nothing `text` does not.
        ({"title": "non_pii", "text": "non_pii"}, SECRET),
    ],
)
def test_redacted_read_with_pii_classes_treats_the_copy_like_text(
    pii_classes, expected_copy,
) -> None:
    from okg.substrate.nomos.read_masking import (
        READ_DROP_CLASSES,
        mask_attrs,
        subtype_attr_classifier,
    )

    out = mask_attrs(
        _secret_node(), classifier=subtype_attr_classifier(pii_classes),
        drop_classes=READ_DROP_CLASSES,
    )
    assert out.get(RESERVED) == expected_copy
    if expected_copy != SECRET:
        assert SECRET not in _values(out)


def test_runtime_payload_redaction_drops_the_copy() -> None:
    from okg.substrate.nomos.runtime_enforcement import _redact_payload

    out = _redact_payload({"node": {"attrs": _secret_node()}})
    assert SECRET not in _values(out)


def test_every_mcp_response_leaves_out_the_copy() -> None:
    """Review round 3, finding 2: query/filter/map/aggregate rows and inspect
    of any kind go through the same response envelope, which strips the copy
    from every `attrs` mapping, and keeps a redaction proof's type."""
    from okg.substrate.mcp.response_shape import compact_for_wire
    from okg.substrate.nomos.read_masking import ReadRedactedAttrs

    stamped = _stamp(["stem", "text"], {"stem": "03-Selection", "text": BODY})
    query = {"operator_name": "query", "rows": [
        {"node_id": "a", "attrs": dict(stamped)},
        {"node_id": "b", "attrs": ReadRedactedAttrs(stamped)},
    ]}
    out = compact_for_wire(query, {})
    assert all(RESERVED not in row["attrs"] for row in out["rows"])
    assert isinstance(out["rows"][1]["attrs"], ReadRedactedAttrs)
    assert RESERVED in query["rows"][0]["attrs"], "input must not be mutated"
    edge = {"operator_name": "inspect", "edge": {"attrs": dict(stamped)}}
    assert RESERVED not in compact_for_wire(
        edge, {"target": {"kind": "edge"}},
    )["edge"]["attrs"]
    plain = {"operator_name": "query", "rows": [{"attrs": {"title": "T"}}]}
    assert compact_for_wire(plain, {}) is plain


def test_console_neighbour_rows_leave_out_the_copy() -> None:
    from okg.substrate.console_views.graph_record import _neighbour_row

    stamped = _stamp(["stem", "text"], {"stem": "03-Selection", "text": BODY})
    row = _neighbour_row({
        "nbr": "n", "nbr_subtype": "section", "nbr_attrs": stamped,
        "nbr_source": "s", "nbr_source_record_id": {}, "nbr_source_revision": {},
    })
    assert RESERVED not in row["attrs"]


@pytest.mark.parametrize(
    ("stored_under", "attrs", "handed_under"),
    [
        # A node with no body, handed off after a field was removed.
        (["title", "summary"], {"title": "T", "summary": "S"}, ["title"]),
        # A body that repeats a part, handed off after a reorder.
        (["title", "summary", "text"],
         {"title": "T", "summary": "S", "text": "T"}, ["summary", "title", "text"]),
        # A real body, under an older declaration.
        (["title", "summary", "text"],
         {"title": "T", "summary": "S", "text": BODY}, ["title", "text"]),
    ],
)
def test_handoff_equals_a_fresh_stamp_after_a_declaration_change(
    stored_under, attrs, handed_under,
) -> None:
    """Review round 3, finding 4: a declaration row does not say which
    declaration stamped it, so the hand-off must not use the current one to
    recognise its rollup."""
    stored = _stamp(stored_under, attrs)
    rows, refused = _Writer({"memo": handed_under})._prepare_handoff_rows(
        [{"node_id": "n1", "subtype": "memo", "attrs": stored, "source": "s"}],
        current_nodes={"n1": {"subtype": "memo"}},
        searchable_fields_cache={},
    )
    assert refused == []
    assert rows[0]["attrs"] == _stamp(handed_under, attrs)


def test_console_redacted_record_does_not_name_the_copy() -> None:
    """Review of round 3, item 2: a redacted console record listed the
    copy as a hidden field, leaking its name (not its value)."""
    from okg.substrate.console_views.records import (
        _read_time_hidden_keys,
        _record_fields_and_sections,
    )
    from okg.substrate.nomos.read_masking import ReadRedactedAttrs

    stored = {"title": "T", "text": BODY, RESERVED: BODY}
    visible = ReadRedactedAttrs({"title": "T"})
    removed = _read_time_hidden_keys(stored, visible)
    assert removed == {"text"}
    fields, _sections = _record_fields_and_sections(
        "memo", dict(visible), removed_keys=removed, label="T",
    )
    assert RESERVED not in {field["key"] for field in fields}


def test_graph_viewer_node_rows_and_light_reads_leave_the_copy_out() -> None:
    """Review of round 3, item 1: the graph viewer's node, expand, khop and
    sample reads returned the copy, and light mode kept it."""
    import inspect

    from okg.viz import reads

    row = reads.NodeRow.from_row(("n1", "memo", {"title": "T", "text": BODY, RESERVED: BODY}))
    assert RESERVED not in row.attrs
    assert row.attrs["text"] == BODY
    source = inspect.getsource(reads)
    light_arrays = source.count("'pdf_url','local_pdf_path','pdf_bytes','raw','content','html',")
    assert light_arrays == 3
    assert source.count("'okg_producer_text'])::jsonb") == light_arrays
