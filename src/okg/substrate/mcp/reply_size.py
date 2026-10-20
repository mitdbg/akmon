"""The reply's size and the knobs that change it, on its first line (#3666).

PACT read-reply-size-3666. From :data:`SIZE_LINE_FROM_BYTES` up, a search
reply's status line says, before its closing run of counts and ending
(which stay the last facts), its size and the exact arguments that make it
smaller (``fewer:``), and, when its text was cut, the one that shows more
(``more text:``):

    search ok · "q" · 21 KB · fewer: limit=5, max_chars=300, select=["node_id","handle"] · rows 1-10 of 40 · next: offset=10

An ``inspect`` reply of a node or a text says its size, in the same place,
and, from :data:`KNOB_BYTES` up, the arguments for a smaller one
(``smaller:``). Every knob is a published argument of that tool (gate 1):
search's ``limit``, ``max_chars`` and ``select``; inspect's ``select`` and
``max_bytes``. The JSON form carries the same facts in :data:`REPLY_SIZE_KEY`.

Why: in the Sol graph runs (ERAG-500, lane X2, 2026-10-18) tool replies
were 90% of the prompt tokens, and agents could not see what a reply cost
or which argument would shrink it. Agents asked for 100 rows and a reply
filled the whole 64 KB budget.

The size is the served text's own UTF-8 size, measured after the facts are
added (:func:`with_size_facts`), rounded as printed. Every byte measurer
renders this same text, so the fit loop counts the size facts and a reply
never exceeds its budget because of them.

This module imports nothing heavy: ``result_text`` imports it.
"""
from __future__ import annotations

import json
from typing import Any, Callable, Optional

__all__ = [
    "HEAD_ONLY_SELECT",
    "HEAD_SELECT_NAMES",
    "KNOB_BYTES",
    "REPLY_SIZE_KEY",
    "ROW_TEXT_KEYS",
    "inspect_size_facts",
    "narrow_row",
    "search_size_facts",
    "select_is_head_only",
    "size_fact_lines",
    "size_text",
    "with_size_facts",
    "with_size_field",
]

#: The JSON form's key for the same facts the text prints.
REPLY_SIZE_KEY = "reply_size"
#: The `select` a `fewer:` knob names: head lines only (`select_narrowing`).
HEAD_ONLY_SELECT: tuple[str, ...] = ("node_id", "handle")
#: A reply from this size up names the knobs for a smaller one.
KNOB_BYTES = 2 * 1024
#: A reply under this size (without the facts) says nothing of its size:
#: about 500 tokens or fewer, nothing worth steering, and the facts would be
#: a tenth of it. Set to 0 to have every reply say its size.
SIZE_LINE_FROM_BYTES = KNOB_BYTES
#: The least `max_bytes` inspect takes (`response_bounds.MIN_RESPONSE_BYTES`;
#: restated so this module stays light, a test pins the two).
MIN_MAX_BYTES = 4 * 1024
#: The largest `max_chars` search takes (`operator_models.MAX_PREVIEW_CHARS`;
#: restated, pinned by a test).
MAX_CHARS_CEILING = 4096
#: Attribute keys an inspect(node) `smaller:` select names at most.
SMALLER_SELECT_KEYS = 3
#: An attribute value at most this long is a field, not a text.
SHORT_ATTR_CHARS = 120
_SEP = " · "


def size_text(size: int) -> str:
    """``850 B``, ``3.2 KB`` or ``28 KB`` (1 KB = 1024 bytes), rounded."""
    if size < 1024:
        return f"{size} B"
    if size < 10 * 1024:
        return f"{size / 1024:.1f} KB"
    return f"{round(size / 1024)} KB"


def _text_len(value: Any) -> int:
    return len(value) if isinstance(value, str) else 0


#: The `max_chars` values a knob names: tidy numbers, at least 300 (X2 arm C
#: transcripts: `max_chars=103` and `301` read as noise).
TIDY_CHARS: tuple[int, ...] = (300, 400, 600, 800, 1200, 1600, 2000, 3000, 4000)
#: The `limit` values a knob names.
TIDY_LIMITS: tuple[int, ...] = (5, 10, 20, 50, 100)
#: A `more text:` max_chars is above the longest served text by this much at
#: least, so it is never the caller's own value: a passage cut at
#: `max_chars` is a few characters short of it (the cut is at a word).
MORE_CHARS_MARGIN = 1.25


def _tidy_below(values: tuple[int, ...], target: float, current: int) -> Optional[int]:
    """The largest tidy value at most `target`, else the smallest tidy
    value; None unless it is below `current` (it must shrink the page)."""
    below = [value for value in values if value <= target]
    chosen = below[-1] if below else values[0]
    return chosen if chosen < current else None


def _shrinkable_text(row: dict[str, Any]) -> tuple[int, bool, bool]:
    """``(longest, cut, any_text)`` of a search row: the longest text that
    `max_chars` sizes (a passage, a grep comment or prose line, which
    carries `chars_left`, or a bare excerpt), whether such a text was cut
    short, and whether the row prints any matched text at all. A grep
    file, code or field line is cut to 200 characters whatever `max_chars`
    says (`grep_search.MATCH_LINE_CHARS`): it counts as text, not as
    shrinkable."""
    longest, cut, any_text = 0, False, False
    passage = row.get("passage")
    if isinstance(passage, dict) and isinstance(passage.get("text"), str):
        longest = max(longest, _text_len(passage["text"]))
        cut = cut or passage.get("whole") is False
        any_text = True
    for line in row.get("match_lines") or ():
        any_text = True
        if isinstance(line, dict) and "chars_left" in line:
            longest = max(longest, _text_len(line.get("text")))
            left = line.get("chars_left")
            cut = cut or (isinstance(left, int) and not isinstance(left, bool) and left > 0)
    if not isinstance(passage, dict) and not row.get("match_lines"):
        excerpt = _text_len(row.get("excerpt"))
        longest = max(longest, excerpt)
        any_text = any_text or excerpt > 0
    return longest, cut, any_text


def search_size_facts(payload: dict[str, Any], rows: list[Any], size: int) -> dict[str, Any]:
    """The size facts of a search page: rows, bytes, `fewer`, `more`.

    From :data:`KNOB_BYTES` up, `fewer` names the knobs that shrink this
    page, each a tidy value (:data:`TIDY_LIMITS`, :data:`TIDY_CHARS`) and
    each only when it is below what the page holds: `limit` (about half the
    rows); `max_chars` (about half the longest text `max_chars` sizes, at
    least 300); the head-only `select` when a row prints matched text.
    `more` is set when text was cut: with passages shortened to fit the
    reply, a smaller `limit` (then `fewer` names no `limit`: one knob, one
    meaning); with a passage cut at its length, a tidy `max_chars` above
    the longest text served and at most 4000. Rows past the page are
    already named by `next:`."""
    dict_rows = [row for row in rows if isinstance(row, dict)]
    longest, cut, any_text = 0, False, False
    for row in dict_rows:
        row_longest, row_cut, row_text = _shrinkable_text(row)
        longest, cut = max(longest, row_longest), cut or row_cut
        any_text = any_text or row_text
    more: dict[str, Any] = {}
    shortened = payload.get("passages_shortened_to")
    if isinstance(shortened, int) and not isinstance(shortened, bool) and len(dict_rows) >= 2:
        limit = _tidy_below(TIDY_LIMITS, len(dict_rows) / 2, len(dict_rows))
        if limit is not None:
            more["limit"] = limit
    elif cut:
        wanted = [value for value in TIDY_CHARS if value >= longest * MORE_CHARS_MARGIN]
        if wanted and wanted[0] <= MAX_CHARS_CEILING:
            more["max_chars"] = wanted[0]
    fewer: dict[str, Any] = {}
    if size >= KNOB_BYTES and "limit" not in more:
        limit = _tidy_below(TIDY_LIMITS, len(dict_rows) / 2, len(dict_rows))
        if limit is not None:
            fewer["limit"] = limit
    if size >= KNOB_BYTES and longest > 0 and "max_chars" not in more:
        chars = _tidy_below(TIDY_CHARS, longest / 2, longest)
        if chars is not None:
            fewer["max_chars"] = chars
    if size >= KNOB_BYTES and any_text:
        fewer["select"] = list(HEAD_ONLY_SELECT)
    out: dict[str, Any] = {"rows": len(rows), "bytes": size}
    if fewer:
        out["fewer"] = fewer
    if more:
        out["more"] = more
    return out


def _short_attr_keys(attrs: Any) -> list[str]:
    """Up to :data:`SMALLER_SELECT_KEYS` attribute keys of a node whose
    values are short (a number, a date, a short name), in stored order."""
    if not isinstance(attrs, dict):
        return []
    keys: list[str] = []
    for key, value in attrs.items():
        if not isinstance(key, str) or not key or key == "text" or key.startswith("_"):
            continue
        if not all(c.isalnum() or c in "_-." for c in key):
            continue
        if isinstance(value, bool) or isinstance(value, (int, float)) or (
            isinstance(value, str) and 0 < len(value) <= SHORT_ATTR_CHARS
        ):
            keys.append(key)
        if len(keys) >= SMALLER_SELECT_KEYS:
            break
    return keys


def inspect_size_facts(payload: dict[str, Any], kind: Optional[str], size: int) -> dict[str, Any]:
    """The size facts of an inspect reply: bytes and, from
    :data:`KNOB_BYTES` up, `smaller`: `max_bytes` (half this reply,
    when that is at least the least inspect takes) and, on a trusted node, a
    `select` of up to three of its short attributes."""
    out: dict[str, Any] = {"bytes": size}
    if size < KNOB_BYTES:
        return out
    smaller: dict[str, Any] = {}
    provenance = payload.get("provenance")
    trusted = isinstance(provenance, dict) and provenance.get("trust") == "trusted"
    if kind == "node" and trusted:
        # Attribute keys are the producer's words: named only for a trusted
        # record, as a key of an untrusted one is printed only fenced.
        keys = _short_attr_keys(payload.get("attrs"))
        if keys:
            smaller["select"] = [f"attrs.{key}" for key in keys]
    half = (size // 2) // 1024 * 1024
    if half >= MIN_MAX_BYTES:
        smaller["max_bytes"] = half
    if smaller:
        out["smaller"] = smaller
    return out


def _knobs(knobs: dict[str, Any]) -> str:
    return ", ".join(
        f"{name}={json.dumps(value, ensure_ascii=False, separators=(',', ':'))}"
        for name, value in knobs.items()
    )


def size_fact_lines(facts: dict[str, Any]) -> list[str]:
    """The status-line facts for `facts`: the size, then `fewer:` or
    `smaller:`, then `more text:`. The row count is already on the line."""
    out = [size_text(int(facts["bytes"]))]
    for label, key in (("fewer", "fewer"), ("smaller", "smaller"), ("more text", "more")):
        knobs = facts.get(key)
        if isinstance(knobs, dict) and knobs:
            out.append(f"{label}: {_knobs(knobs)}")
    return out


def with_size_facts(
    text: str,
    facts_for: Callable[[int], dict[str, Any]],
    split: Callable[[str], tuple[str, str]] = lambda line: (line, ""),
) -> str:
    """`text` with the size facts on its first line, between the two parts
    `split` cuts it into (by default, at its end). The status line's
    counts and ending stay its last facts (`result_text` splits before them).

    The size is the result's own UTF-8 size, facts included: measured again
    until the printed size stops changing (twice at most in practice; four
    rounds bound it). Only the rounded value prints, so a last change of a
    few bytes does not change what it says.

    A text under :data:`SIZE_LINE_FROM_BYTES` is returned as it is. That
    bound is under the least reply budget (`MIN_MAX_BYTES`), so a served
    text that crosses it while its measured form did not still fits."""
    size = len(text.encode("utf-8"))
    if size < SIZE_LINE_FROM_BYTES:
        return text
    first, newline, rest = text.partition("\n")
    head, tail = split(first)
    out = text
    for _ in range(4):
        line = _SEP.join([head, *size_fact_lines(facts_for(size)), *([tail] if tail else [])])
        out = line + newline + rest
        measured = len(out.encode("utf-8"))
        if measured == size:
            break
        size = measured
    return out


def with_size_field(payload: dict[str, Any], encode: Callable[[dict[str, Any]], str],
                    facts_for: Callable[[int], dict[str, Any]]) -> str:
    """The JSON form: `payload` with :data:`REPLY_SIZE_KEY` holding the
    same facts, its `bytes` the encoded result's own size; none under
    :data:`SIZE_LINE_FROM_BYTES`, as the text says none."""
    out = encode(payload)
    size = len(out.encode("utf-8"))
    if size < SIZE_LINE_FROM_BYTES:
        return out
    for _ in range(4):
        out = encode({**payload, REPLY_SIZE_KEY: facts_for(size)})
        measured = len(out.encode("utf-8"))
        if measured == size:
            break
        size = measured
    return out


# ── select narrowing ───────────────────────────────────────────────────────
#
# A `select` naming only id and head fields asks for head lines: the rows
# print no passage and no match lines (`select_is_head_only`). Any other
# name keeps them, as before: `excerpt`, `match_lines` or `text`, whole
# `attrs` (the 10-15 guard: `select=["node_id","attrs"]` printed grep rows
# with no matched text at all when grep dropped its lines), any
# `attrs.<key>` (the server cannot tell from the name whether a stored key
# holds text, and read-passage-rows D6 serves a select of attributes beside
# the matched passage), and every other row field.

#: The id and head fields search serves as `select` names. The `where`
#: names a caller may also send (`title`, `path`, `source`, `type`,
#: `date.created`) reach search already read as one of these by the read
#: grammar (`select_vocabulary.WHERE_NAMES_IN_ROWS`, `SERVED_SECOND_NAMES`).
HEAD_SELECT_NAMES: frozenset[str] = frozenset({
    "node_id", "canonical_node_id", "subtype", "provenance", "handle", "evidence_kind",
})
#: Row keys that carry a search row's matched text; a head-only `select`
#: leaves them out of every row (:func:`narrow_row`).
ROW_TEXT_KEYS: tuple[str, ...] = ("passage", "match_lines", "excerpt", "match_windows")


def select_is_head_only(select: Any) -> bool:
    """Whether `select` names only id and head fields (head lines only).

    False for no `select`, an empty one, or one with any other name."""
    if not isinstance(select, (list, tuple)) or not select:
        return False
    for name in select:
        if not isinstance(name, str):
            return False
        if name not in HEAD_SELECT_NAMES:
            return False
    return True


def narrow_row(row: Any) -> Any:
    """`row` without its matched text (:data:`ROW_TEXT_KEYS`), as a copy."""
    if not isinstance(row, dict):
        return row
    return {key: value for key, value in row.items() if key not in ROW_TEXT_KEYS}
