"""Tokenization helper for path-valued searchable text fields.

Per `index-document-source-file-search` (issue #900). The BM25 node-text
index is built `WITH (text_config='english')`
(`graph_store_internal/schema.py`), so Postgres' default parser decides
what a lexeme is. That parser recognizes a slash-separated path as a
SINGLE `file` token:

    SELECT to_tsvector('english', 'refs/notes/07-Background_Estimation.tex');
    -- 'refs/notes/07-background_estimation.tex':1

One lexeme. No word-level query can ever match it, so a subtype whose
only searchable field holds a path (`document.title`, filled with the
repo-relative path by `library/sources/doc_corpus.py`) is present in the
index and still unfindable by name.

A subtype declares `okg_searchable_text_path_fields` to say "these field
values are structured identifiers; also index their component words".
The stamper appends `split_path_value(value)` alongside the raw value,
so both an exact-path query and an interior-word query match.

The stamper also appends `split_camel_value(value)`, a second form that
breaks CamelCase and letter-to-digit boundaries (archi-physics/cms-kb#6).
A TWiki page title such as `SWGuideCrab` is one lexeme under the same
parser, so "crab guide" never matched it; the extra form is
`SW Guide Crab`. It is added beside the separator split, not in place of
it: a path such as `formal/admission/AdmissionSmoke.cfg` is one `file`
token, so its component `AdmissionSmoke` is findable only through the
separator split, and replacing that split would lose it.

`okg._stamp_searchable_text` in `db/schema.sql` mirrors this function in
SQL. The two MUST agree; `tests/substrate/test_searchable_text_path_fields.py`
pins them against the same inputs.
"""
from __future__ import annotations

import re


#: Characters that separate components inside a structured identifier:
#: path separators, and the word separators that appear inside a path
#: component (`07-Background_Estimation.tex`). Kept in sync with the
#: `regexp_replace` class in `okg._stamp_searchable_text`.
_PATH_SEPARATORS = re.compile(r"[/\\_.:\-]+")

#: ASCII whitespace ONLY — deliberately not `\s` and not `str.strip()`.
#: Python's `\s` and bare `.strip()` also match U+00A0 and the other
#: Unicode spaces; Postgres' regex `\s` and `btrim` do not. Using the
#: wider Python set here would make this function and its SQL mirror
#: disagree on any value containing a non-breaking space, which
#: `req.path-fields-tokenized` forbids: the two write paths would stamp
#: different text, hence different `text_hash`, hence different embedding
#: cache keys for the same node.
ASCII_WHITESPACE = " \t\n\r\f\v"
_ASCII_WHITESPACE_RUN = re.compile(r"[ \t\n\r\f\v]+")

#: Word boundaries inside one identifier, applied in this order, each as a
#: capture-group substitution. `okg._stamp_searchable_text` runs the same
#: four `regexp_replace` calls in the same order, so the two mirrors agree
#: by construction, not by two regex engines agreeing on zero-width
#: matches. ASCII classes only, like the whitespace set above.
#:
#: 1. lower -> Upper:             `SWGuideCrab`   -> `SWGuide Crab`
#: 2. end of an acronym:          `SWGuide`       -> `SW Guide`,
#:                                `HLTConfig`     -> `HLT Config`
#: 3. digit -> capitalized word:  `CMST0Transfer` -> `CMST0 Transfer`
#: 4. word (2+ lowercase) -> digit: `Meeting28092011` -> `Meeting 28092011`
#:
#: An all-caps run with no following lowercase letter stays whole
#: (`EcalPFGFAQ` -> `Ecal PFGFAQ`): nothing marks where `PFG` ends.
_CAMEL_BOUNDARIES = (
    (re.compile(r"([a-z])([A-Z])"), r"\1 \2"),
    (re.compile(r"([A-Z])([A-Z][a-z])"), r"\1 \2"),
    (re.compile(r"([0-9])([A-Z][a-z])"), r"\1 \2"),
    (re.compile(r"([a-z]{2})([0-9])"), r"\1 \2"),
)


#: A hash-like component: hex digits only, 7 or more long (`fdd423463766`,
#: a short commit SHA). The CamelCase rules would chop it into meaningless
#: pieces (`fdd 423463766`), so `split_camel_value` leaves it whole.
_HEX_LIKE = re.compile(r"[0-9A-Fa-f]{7,}")


def split_path_value(value: str) -> str:
    """Return `value` with separator runs replaced by single spaces.

    Returns the empty string when the value has no word content. The
    result is a plain space-joined token sequence, which the `english`
    text-search config lexes into one lexeme per component. A component
    keeps its own spelling here (`AdmissionSmoke`, `oauth2`), so a query
    for a whole file-name component still matches it.
    """
    replaced = _PATH_SEPARATORS.sub(" ", value)
    collapsed = _ASCII_WHITESPACE_RUN.sub(" ", replaced)
    return collapsed.strip(ASCII_WHITESPACE)


def split_camel_value(value: str) -> str:
    """Return the `split_path_value` form with each component also split
    at CamelCase and letter-digit boundaries (see `_CAMEL_BOUNDARIES`).

    The stampers append this as an EXTRA part after the separator split,
    never instead of it, and drop it when it equals a part already present
    (a value with no CamelCase). Hash-like components (`_HEX_LIKE`) are
    left whole. Returns the empty string when the value has no word
    content.
    """
    words = split_path_value(value)
    if not words:
        return ""
    out: list[str] = []
    for word in words.split(" "):
        if not _HEX_LIKE.fullmatch(word):
            for pattern, replacement in _CAMEL_BOUNDARIES:
                word = pattern.sub(replacement, word)
        out.append(word)
    return " ".join(out)


#: The reserved attribute that keeps a producer's own `text` body once the
#: stamper has folded it into the searchable rollup (issue #2991).
#:
#: `attrs.text` is both an input and the output. A producer may write a real
#: body there (a note, a section, a markdown heading), and the stamper
#: rebuilds `text` from the subtype's declared searchable fields. Before
#: #2991 the rebuild dropped the body whenever another declared field had a
#: value. Now the body is appended as the LAST part of the rollup (or not
#: repeated, when it equals a part) and a copy is kept here, so a later
#: restamp starts from the copy and never from the old rollup (restamping the
#: rollup would give `fresh\n\nold_rollup`). The copy is written whenever a
#: body was supplied and at least one declared part exists; a node with no
#: producer body stores exactly what it stored before.
#:
#: Issue #3469: a declared value the body already holds as a whole token is
#: left out of the rollup, so a Jira issue's summary is not written twice
#: (on the comp-ops graph every declared part of five subtypes was already in
#: the body, and `text` was about twice the body). The copy is still written
#: when every part was left out and `text` is the body alone. A node stored
#: under the #2991 rule (every part, then the body) is recognised by its
#: copy and restamps to the shorter form.
#:
#: It is a SYSTEM attribute, like the lineage envelope: the stamper writes it,
#: entity merge never offers it for a decision (`producer_view`), and reads
#: leave it out (`without_producer_text`), because its value already sits at
#: the end of `text`. `okg._stamp_searchable_text` in `db/schema.sql` uses the
#: same name.
PRODUCER_TEXT_ATTR = "okg_producer_text"

#: Keys that never contribute a declared part: `text` is the output slot and
#: the reserved copy is its input.
_NOT_A_PART = frozenset({"text", PRODUCER_TEXT_ATTR})


def _blank(value: str) -> bool:
    return not value.strip(ASCII_WHITESPACE)


def _declared_parts(
    attrs: dict,
    fields: list[str],
    path_fields: list[str],
) -> list[tuple[str, bool]]:
    """`searchable_parts`, each tagged with whether a body may drop it.

    A trimmed declared value (from `fields`) is droppable: when the producer
    body already holds it as a whole token (`_body_holds`), repeating it only
    doubles the text (issue #3469). A path field's separator split and
    CamelCase split are never droppable: they exist to make component words
    searchable and they rarely occur verbatim in a body.
    """
    parts: list[tuple[str, bool]] = []
    seen: set[str] = set()
    for field_name in fields:
        value = attrs.get(field_name)
        if field_name in _NOT_A_PART or not isinstance(value, str):
            continue
        # Order-preserving dedupe. Two declared fields can hold the same
        # value (Document's `title` is seeded from `path`); repeating it only
        # inflates BM25 term frequency.
        stripped = value.strip(ASCII_WHITESPACE)
        if stripped and stripped not in seen:
            seen.add(stripped)
            parts.append((stripped, True))
    # `index-document-source-file-search` (issue #900): a path value is one
    # BM25 lexeme under the index's `english` config, so append its component
    # words too, and (archi-physics/cms-kb#6) the CamelCase form as an EXTRA
    # part after the separator split. Each is dropped when already present.
    for field_name in path_fields:
        value = attrs.get(field_name)
        if field_name in _NOT_A_PART or not isinstance(value, str) or _blank(value):
            continue
        for split in (split_path_value(value), split_camel_value(value)):
            if split and split not in seen:
                seen.add(split)
                parts.append((split, False))
    return parts


def searchable_parts(
    attrs: dict,
    fields: list[str],
    path_fields: list[str],
) -> list[str]:
    """The rollup parts from the declared fields, in declared order.

    Only string values count, each trimmed of ASCII whitespace
    (`ASCII_WHITESPACE`, the set the SQL mirror's `btrim` uses) and added
    once. Each declared path field then adds its separator split and its
    CamelCase split when they are new. `text` and `PRODUCER_TEXT_ATTR` are
    skipped: they carry the producer body, which the stamper appends last.
    These are the parts a stamp with no producer body writes; with a body,
    `_text_with_body` leaves out the values the body already holds.
    """
    return [part for part, _droppable in _declared_parts(attrs, fields, path_fields)]


def _rollup(parts: list[str]) -> str | None:
    return "\n\n".join(parts) if parts else None


#: ASCII letters and digits: what may not touch a declared value for the
#: body to count as holding it. ASCII only, like `ASCII_WHITESPACE`, so the
#: SQL mirror's `[A-Za-z0-9]` class agrees on every input.
_TOKEN_CHARS = "A-Za-z0-9"
_TOKEN_CHAR_SET = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
)


def _body_holds(body: str, part: str) -> bool:
    """Whether `body` holds `part` as a whole token (issue #3469).

    True when `part`'s exact text (case and punctuation included) occurs in
    `body` with no ASCII letter or digit immediately before or after that
    occurrence. `JIRA-4411` is held by `see JIRA-4411.`; `HLT` is not held by
    `HLTConfig`. `okg._searchable_text_with_body` in `db/schema.sql` runs
    the same test.
    """
    # A find-and-check-neighbours walk, not a regex: the stamper runs per
    # row on bulk writes, and a pattern built per part defeats the regex
    # cache (about 170x slower per row on Jira-sized nodes).
    end_limit = len(body) - len(part)
    pos = body.find(part)
    while pos != -1:
        end = pos + len(part)
        if (pos == 0 or body[pos - 1] not in _TOKEN_CHAR_SET) and (
            pos == end_limit or body[end] not in _TOKEN_CHAR_SET
        ):
            return True
        pos = body.find(part, pos + 1)
    return False


def _text_with_body(tagged: list[tuple[str, bool]], body: str) -> str:
    """The `text` a stamp writes for `body` over the tagged parts.

    A droppable part the body already holds is left out (issue #3469); the
    rest are joined by a blank line and the body is appended, byte for byte,
    unless its trimmed form equals a part still present. With no part left
    the text is the body alone.
    """
    parts = [
        part for part, droppable in tagged
        if not (droppable and _body_holds(body, part))
    ]
    rollup = _rollup(parts)
    if rollup is None:
        return body
    if body.strip(ASCII_WHITESPACE) in parts:
        return rollup
    return f"{rollup}\n\n{body}"


def _legacy_text_with_body(parts: list[str], body: str) -> str:
    """The `text` the #2991 rule wrote, before #3469: every part, then the
    body. Only used to recognise a node stored under that rule, so a restamp
    reads its body from the reserved copy and never from the old text."""
    rollup = _rollup(parts)
    if rollup is None:
        return body
    if body.strip(ASCII_WHITESPACE) in parts:
        return rollup
    return f"{rollup}\n\n{body}"


def _is_stamp_of(
    text: object, tagged: list[tuple[str, bool]], kept: str,
) -> bool:
    """Whether `text` is what this stamper, or the #2991 rule before it,
    wrote for the body `kept` over these parts."""
    if not isinstance(text, str):
        return False
    return text == _text_with_body(tagged, kept) or text == _legacy_text_with_body(
        [part for part, _droppable in tagged], kept,
    )


def _usable_copy(kept: object) -> bool:
    return isinstance(kept, str) and not _blank(kept)


def stamp_searchable_text(
    attrs: dict,
    fields: list[str],
    path_fields: list[str],
) -> dict:
    """Write the searchable rollup into `attrs["text"]` and return `attrs`.

    The rollup is the declared parts joined by a blank line. The producer
    body is the reserved copy when `text` is EXACTLY the stamp that copy gives
    over the current parts, under this rule or the #2991 rule before it (a
    restamp of a stored node); otherwise it is a non-blank `text` that is not
    exactly the rollup (what a stamp with no body wrote). With a body, a
    declared value the body already holds as a whole token is left out
    (issue #3469; path and CamelCase splits always stay), the body is
    appended as the last part, byte for byte, unless its trimmed form equals
    a part still present, and the body is kept under `PRODUCER_TEXT_ATTR`
    whenever a declared part exists, even when every part was left out. With
    no declared part the body is the whole text, stored unchanged (issue
    #2972) and with no copy. With no body the result is the plain rollup,
    exactly as before #2991. A supplied `text` that is not the copy's stamp
    wins over a stale copy. Stamping the result again returns the same attrs.
    """
    if not fields:
        return attrs
    tagged = _declared_parts(attrs, fields, path_fields)
    rollup = _rollup([part for part, _droppable in tagged])
    text = attrs.get("text")
    kept = attrs.pop(PRODUCER_TEXT_ATTR, None)
    body: str | None = None
    if _usable_copy(kept) and _is_stamp_of(text, tagged, kept):
        body = kept
    elif isinstance(text, str) and not _blank(text) and text != rollup:
        body = text
    if body is not None:
        attrs["text"] = _text_with_body(tagged, body)
        if rollup is not None:
            attrs[PRODUCER_TEXT_ATTR] = body
    elif rollup is not None:
        attrs["text"] = rollup
    return attrs


def unstamp_searchable_text(
    attrs: dict,
    fields: list[str],
    path_fields: list[str],
    *,
    keep_plain_rollup: bool = False,
) -> dict:
    """Turn stored attrs back into the attrs their producer wrote.

    `fields` and `path_fields` are the declaration the attrs were stamped
    under. A catalog change restamps stored nodes under a NEW declaration,
    where the stored `text` is no longer the stamp, so `stamp_searchable_text`
    alone would read an old rollup as a producer body. This puts the reserved
    copy back into `text` (the stored text is this stamper's output, so a
    `text` that is the copy, or ends with it after the part separator, is
    recognised even when the declaration has moved on since), or drops `text`
    when it is exactly the old rollup (the node had no body), and leaves a
    body stamped with no declared part alone.

    `keep_plain_rollup` keeps that old rollup instead of dropping it. The
    catalog restamp passes it when the NEW declaration is empty: the stamper
    then leaves attrs alone, and dropping the rollup would take the node out
    of search, where the pre-#2991 rule kept it. Returns `attrs`.
    """
    if not fields:
        return attrs
    text = attrs.get("text")
    kept = attrs.pop(PRODUCER_TEXT_ATTR, None)
    tagged = _declared_parts(attrs, fields, path_fields)
    parts = [part for part, _droppable in tagged]
    if _usable_copy(kept) and isinstance(text, str) and (
        _is_stamp_of(text, tagged, kept)
        or text == kept
        or text.endswith("\n\n" + kept)
    ):
        attrs["text"] = kept
        return attrs
    if parts and text == _rollup(parts) and not keep_plain_rollup:
        attrs.pop("text", None)
    return attrs


def unstamp_without_declaration(attrs: dict) -> dict:
    """Turn stored attrs back into producer attrs when the declaration they
    were stamped under is not known (a shared-node declaration row, which may
    predate a declaration change).

    A usable copy IS the producer's body: the stamper writes one only with a
    body and drops a stale one, so `text` becomes the copy. Without a copy,
    `text` is dropped when every blank-line-separated piece of it is a value
    this node already carries (a trimmed string attribute, or its path or
    CamelCase split): such a `text` is a rollup of some declaration, and
    dropping it loses nothing the attributes do not still hold. Anything else
    is a body and is kept. Returns `attrs`.
    """
    text = attrs.get("text")
    kept = attrs.pop(PRODUCER_TEXT_ATTR, None)
    if _usable_copy(kept):
        attrs["text"] = kept
        return attrs
    if not isinstance(text, str) or _blank(text):
        return attrs
    carried: set[str] = set()
    for key, value in attrs.items():
        if key in _NOT_A_PART or not isinstance(value, str) or _blank(value):
            continue
        stripped = value.strip(ASCII_WHITESPACE)
        carried.add(stripped)
        carried.update(piece for piece in stripped.split("\n\n"))
        carried.add(split_path_value(value))
        carried.add(split_camel_value(value))
    if all(piece in carried for piece in text.split("\n\n")):
        attrs.pop("text", None)
    return attrs


def producer_view(attrs: dict) -> dict:
    """A copy of stored attrs with `text` set back to the producer's body.

    For callers that let a person or a rule choose between stored attribute
    values (entity merge): the choice is between producer bodies, and the
    reserved copy is never a separate choice. The next write stamps `text`
    and the copy again from the chosen body. Stored attrs without a copy are
    returned as they are.
    """
    out = dict(attrs)
    kept = out.pop(PRODUCER_TEXT_ATTR, None)
    if _usable_copy(kept):
        out["text"] = kept
    return out


def without_producer_text(attrs: object) -> object:
    """`attrs` without the reserved copy, for read responses.

    The copy repeats the end of `text`; returning both would double every
    body (up to 200 kB for a cms-kb Section). Anything that is not a mapping
    is returned unchanged.
    """
    if isinstance(attrs, dict) and PRODUCER_TEXT_ATTR in attrs:
        kept = {k: v for k, v in attrs.items() if k != PRODUCER_TEXT_ATTR}
        # A dict subclass (the read filter's redaction proof) keeps its type.
        return kept if type(attrs) is dict else type(attrs)(kept)
    return attrs
