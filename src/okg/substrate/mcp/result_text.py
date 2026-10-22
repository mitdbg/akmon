"""The plain-text form of an MCP tool result ("cards"; issues #3059, #3076).

Every okg MCP tool result is served ONCE, as one text block in ``content``.
By default that block is this module's rendering of the tool's dictionary;
a program asks for compact JSON instead with the per-call request ``_meta``
key :data:`RESULT_FORMAT_META_KEY`. The grammar is frozen with the format and
documented in ``docs/reference/mcp-results.md``; the design and its review
history are in ``pact/changes/mcp-result-diet-3076/DESIGN.md`` section 1.

A result is four parts, in order:

1. one status line, ``<tool> ok|refused <code>|error <code>|status=<v>``
   followed by `` · ``-joined facts (subject, method, counts, coverage,
   ``phrase=no_exact_match`` on a quoted lexical search none of whose checked
   candidates holds every quoted span, paging, generation). It is the only
   part programs parse (:func:`parse_status_line`);
2. zero or more notice lines, each starting ``! ``;
3. the body, shaped per operator;
4. ``--- other fields``: every top-level field with no place in the grammar.

Source text can never forge that structure: a body line that starts like
structure, after the normalization in :func:`structural`, gets one leading
backslash (:func:`escape_body`). Inline values keep their JSON type
(:func:`scalar`) and producer-controlled keys print bare only when they are
plain identifiers (:func:`key_text`).

This module imports nothing heavy, so chat, bench and probe code can import
:func:`parse_status_line` without pulling in the server.
"""

from __future__ import annotations

import functools
import hashlib
import json
import re
import unicodedata
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Callable, Iterator, NamedTuple, Optional

#: The request ``_meta`` key a program sets to choose the result format.
RESULT_FORMAT_META_KEY = "okg/result_format"
#: The formats a caller may ask for. ``text`` is the default for every caller.
RESULT_FORMATS = ("text", "json")
DEFAULT_RESULT_FORMAT = "text"

#: The one server-instructions line that explains the text form to a model.
RESULT_TEXT_INSTRUCTION = (
    "Result text: a line of source text that starts like result structure "
    "begins with one backslash; a quoted value is a JSON string, and a bare "
    "null, true, false or number is that JSON value."
)

FENCE = "\u27e6UNTRUSTED_DATA"
#: The closing fence marker (``provenance.FENCE_CLOSE``; not imported, so this
#: module stays light).
FENCE_CLOSE = "\u27e6/UNTRUSTED_DATA\u27e7"
#: The attrs key the read mask stamps when it dropped or masked a key
#: (`nomos.read_masking.REDACTION_MARKER`; not imported, as above).
_REDACTION_MARKER = "nomos_redacted"
#: The one-line untrusted mark for a handle value (read-model slice 2): the
#: value as a JSON string between an opening and a closing marker, so the
#: handle stays one line and the text still sits between the two markers the
#: trust contract names.
INLINE_FENCE_OPEN = FENCE + "\u27e7"
#: The search row field that carries a record's title, path and date (PACT
#: read-model-s2-handle). One name for the producer (`search._with_handles`),
#: the fence (`provenance`) and this renderer; it may be renamed.
HANDLE_KEY = "handle"
#: The handle's text fields, in print order. A field the read filter hid is
#: named in the handle's `masked` list instead of carrying a value.
HANDLE_FIELDS = ("title", "path", "date")
HANDLE_MASKED_KEY = "masked"
#: The row and read field holding the id a reader cites,
#: ``{"<attr>": "<value>"}`` (`citation_id.CITATION_KEY`, restated: this
#: renderer imports no operator module; PACT read-names-list-3666).
CITATION_KEY = "citation_id"
#: The search row field that points inside a record (PACT
#: read-model-s5-location, design `read-model` 3.6, D10):
#: ``{"chunk": <chunk id>, "offset": <character offset in that chunk's
#: text>, "hash": "sha256:<hex of that text>"}``. The handle line prints
#: ``@ <chunk>:<offset>``; the hash stays in the JSON.
LOCATION_KEY = "location"
#: The handle key marking a visible date as a calendar day (``"date"``,
#: written as that day's midnight), as ``handle_fields.resolve_named_date``
#: reports its precision. A date without it names a time of day.
HANDLE_DATE_PRECISION_KEY = "date_precision"
#: A value longer than this never goes on a header or key line.
LONG = 200
SEP = " \u00b7 "

#: Every character ``str.splitlines()`` breaks on. Any of them forces block
#: form for a value and quoting for an inline string.
LINE_BREAKS = "\n\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029"

#: Structure, matched on a cleaned matching copy of a body line (see
#: :func:`_matching_copy`). A marker must be followed by whitespace or the
#: end of the line. A line whose raw first character is a backslash is
#: structural too, so the escape is reversible.
_STRUCTURE = re.compile(
    "|".join((
        r"^(\\",
        r"\[\d+(\.\d+)*\](\s|$)",
        r"---\s",
        r"!\s",
        r"[a-z0-9_]+\s+(ok|refused|error)(\s|$)",
        r"[a-z0-9_]+\s+status=)",
    ))
)
#: A cleaned prefix that could still become structure if the line went on:
#: an unterminated tool token or card number, or a lone marker character.
#: Used only by the fast path, which then falls back to the whole line.
_STRUCTURE_OPEN = re.compile(
    r"\\?(\[[\d.]*\]?|[a-z0-9_]+(\s+[a-z=]{0,8})?|-{1,3}|!)"
)
_TOOL_TOKEN = re.compile(r"[A-Za-z0-9_]+")
_BARE_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_\-]*")
_LITERAL_WORDS = frozenset({
    "true", "false", "none", "null", "nil", "yes", "no", "on", "off",
    "nan", "inf", "-inf", "+inf", "infinity", "-infinity", "undefined",
})
_NUMBER_LIKE = re.compile(
    r"[+-]?(0x[0-9a-f_]+|0o[0-7_]+|0b[01_]+|\d[\d_]*(\.\d*)?(e[+-]?\d+)?"
    r"|\.\d+(e[+-]?\d+)?)",
    re.IGNORECASE,
)
#: Characters that render blank but are not Unicode whitespace.
_BLANK_LOOKALIKES = "\u115f\u1160\u3164\uffa0\u2800"
#: Lookalikes of the four marker characters that NFKC does not fold. The
#: fence brackets (U+27E6, U+27E7) map to brackets too, which is harmless: a
#: fence line never has a digit after its bracket.
_CONFUSABLES = str.maketrans({
    **{c: "-" for c in (
        "\u2010\u2011\u2012\u2013\u2014\u2015\u2043\u2212\u2e3a\u2e3b"
        "\u02d7\u2796\u23af\u2500\u2501\u30fc\u4e00"
    )},
    **{c: "[" for c in (
        "\u2045\u27e6\u27e8\u2772\u2768\u276a\u3010\u3014\u3016\u301a"
        "\u298b\u298d\u298f"
    )},
    **{c: "]" for c in (
        "\u2046\u27e7\u27e9\u2773\u2769\u276b\u3011\u3015\u3017\u301b"
        "\u298c\u298e\u2990"
    )},
    **{c: "!" for c in "\u01c3\u2757\u2755\u2762\ua71d\u2d51"},
})
#: Visible characters the fast path cleans before deciding; the leading
#: invisible run and invisible characters between them are always included.
_FAST_PATH_VISIBLE = 64


class StatusLine(NamedTuple):
    """The parsed first line of a text result.

    ``status`` is ``ok``, ``refused``, ``error`` or ``status``; for the last,
    ``value`` holds the copied payload status. ``code`` is the error code of
    a refusal or error.
    """

    tool: str
    status: str
    code: Optional[str] = None
    value: Any = None


@dataclass(frozen=True)
class ResultContext:
    """The call a result is being built for, set once per MCP call."""

    format: str = DEFAULT_RESULT_FORMAT
    tool: Optional[str] = None
    kind: Optional[str] = None
    #: The field names the call's ``select`` argument named, or None when it
    #: named none. The text view prints a select-only row field
    #: (:data:`SELECT_ONLY_ROW_FIELDS`) only when it is named here.
    select: Optional[tuple[str, ...]] = None


_RESULT_CONTEXT: ContextVar[Optional[ResultContext]] = ContextVar(
    "okg_mcp_result_context", default=None,
)
#: The format a call uses when its request names none. Only tests and
#: in-process callers set it; a served call always starts from ``text``.
_DEFAULT_FORMAT: ContextVar[str] = ContextVar(
    "okg_mcp_default_result_format", default=DEFAULT_RESULT_FORMAT,
)


def current_result_context() -> ResultContext:
    """The current call's format, tool and inspect kind (defaults outside one)."""

    context = _RESULT_CONTEXT.get()
    if context is None:
        return ResultContext(format=_DEFAULT_FORMAT.get())
    return context


def current_result_format() -> str:
    return current_result_context().format


def default_result_format() -> str:
    return _DEFAULT_FORMAT.get()


@contextmanager
def result_context(context: ResultContext) -> Iterator[ResultContext]:
    """Set the call's result context for the duration of one tool call."""

    token = _RESULT_CONTEXT.set(context)
    try:
        yield context
    finally:
        _RESULT_CONTEXT.reset(token)


@contextmanager
def result_format_scope(result_format: str) -> Iterator[None]:
    """Make calls in this scope that name no format use ``result_format``.

    For in-process callers and tests that drive ``FastMCP.call_tool``
    directly, where there is no request ``_meta`` to carry the choice.
    """

    if result_format not in RESULT_FORMATS:
        raise ValueError(f"unknown result format {result_format!r}")
    token = _DEFAULT_FORMAT.set(result_format)
    try:
        yield
    finally:
        _DEFAULT_FORMAT.reset(token)


# ── values ────────────────────────────────────────────────────────────────


def _unsafe(c: str) -> bool:
    return c in LINE_BREAKS or unicodedata.category(c) in ("Cc", "Cf")


def _finite(value: Any) -> Any:
    """``value`` with every NaN or infinite float replaced by ``None``.

    JSON has no NaN or Infinity. Before #3059 the SDK serialized a
    structured result through pydantic, which wrote them as ``null``; both
    result formats keep that, so a bare ``NaN`` never reads as a number.
    """

    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        return None
    if isinstance(value, dict):
        return {key: _finite(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite(item) for item in value]
    return value


def _dumps(value: Any) -> str:
    try:
        return json.dumps(
            value, separators=(",", ":"), ensure_ascii=False, default=str,
            allow_nan=False,
        )
    except ValueError:
        return json.dumps(
            _finite(value), separators=(",", ":"), ensure_ascii=False, default=str,
            allow_nan=False,
        )


def safe_json(value: Any) -> str:
    """Compact JSON with every line break, control and format character escaped.

    Python's ``json`` leaves U+2028, U+2029 and U+0085 raw; here every one of
    them, and every Cc and Cf character, becomes ``\\uXXXX``, so a quoted
    value is always exactly one line. NaN and infinities print as ``null``.
    """

    text = _dumps(value)
    if not any(_unsafe(c) for c in text):
        return text
    return "".join(f"\\u{ord(c):04x}" if _unsafe(c) else c for c in text)


def compact_json(value: Any) -> str:
    """The JSON result format: compact, UTF-8, the same dictionary.

    NaN and infinities are written as ``null``, as the structured result
    was before #3059; the output is always valid JSON.
    """

    return _dumps(value)


#: Characters that look like the fact separator's middle dot (review B3 on
#: 2102d20798) and whose Unicode name does not say so (see
#: :func:`_dot_like`). A string holding any of them, or any whitespace other
#: than the ASCII space, could forge a `` · `` fact, so it is quoted.
_MIDDOT_LIKE = frozenset(
    "\u00b7\u0387\u2027\u30fb\uff65\u16eb\u2e31\u1427\ua78f\u10fb\u0701\u0702\u05c5\u2981"
)
#: Words in a Unicode character name that mark a dot or bullet lookalike.
_DOT_NAME_WORDS = ("DOT", "BULLET", "POINT", "TELEIA", "PUNCTUS", "INTERPUNCT")


@functools.lru_cache(maxsize=8192)
def _dot_like(c: str) -> bool:
    """Whether a non-ASCII character could pass for the separator's dot.

    Derived from the Unicode database rather than a fixed list (re-review of
    06855a5bc9): any punctuation, symbol or modifier character whose name
    says DOT, BULLET or POINT (U+02D9 DOT ABOVE, U+2E33 RAISED DOT, U+10101
    AEGEAN WORD SEPARATOR DOT, U+22C5 DOT OPERATOR, ...), plus the few whose
    names do not. Letters with a dot (U+010A) are not separators.
    """
    if c in _MIDDOT_LIKE:
        return True
    if unicodedata.category(c)[0] not in "PSL" or unicodedata.category(c) in ("Lu", "Ll", "Lt", "Lo"):
        return False
    name = unicodedata.name(c, "")
    return any(word in name for word in _DOT_NAME_WORDS)


def needs_quote(s: str) -> bool:
    """Whether a string must print as a JSON string to keep its type."""

    if s == "" or s != s.strip() or any(_unsafe(c) for c in s):
        return True
    if SEP in s or '"' in s or "\\" in s:
        # A quote or backslash anywhere: a reader would take a mid-string
        # `"` for the start of a JSON string and fall out of step
        # (re-review of 06855a5bc9).
        return True
    if any(
        c in _BLANK_LOOKALIKES
        or (c != " " and c.isspace())
        or (c > "\x7f" and _dot_like(c))
        for c in s
    ):
        return True
    if s.casefold() in _LITERAL_WORDS or _NUMBER_LIKE.fullmatch(s):
        return True
    try:
        json.loads(s)
    except ValueError:
        return False
    return True


def scalar(value: Any) -> str:
    """A typed inline value: strings bare unless ambiguous, the rest JSON."""

    if isinstance(value, str):
        return safe_json(value) if needs_quote(value) else value
    return safe_json(value)


def token_text(value: Any) -> str:
    """A status value, error code or status-line subject: :func:`scalar`,
    and quoted when it holds a space, so it reads as exactly one token and
    can never pass for another fact (such as ``generation <id>``)."""

    text = scalar(value)
    if isinstance(value, str) and text == value and " " in value:
        return safe_json(value)
    return text


def ident_text(value: Any) -> str:
    """A row header's id or ``(<subtype>)``: :func:`scalar`, and quoted when
    it holds `` (`` or a parenthesis would make the header ambiguous."""

    text = scalar(value)
    if isinstance(value, str) and text == value and ("(" in value or ")" in value):
        return safe_json(value)
    return text


def part_text(value: Any) -> str:
    """A value inside a ``, ``-joined fact (a parent): :func:`scalar`, and
    quoted when it holds ``, ``."""

    text = scalar(value)
    if isinstance(value, str) and text == value and ", " in value:
        return safe_json(value)
    return text


def _inner_text(value: Any) -> str:
    """A value inside a parenthesized, ``, ``-joined fact (a rank cap):
    :func:`token_text`, and quoted when it holds a parenthesis or a comma."""

    text = token_text(value)
    if isinstance(value, str) and text == value and any(c in value for c in "(),"):
        return safe_json(value)
    return text


def key_text(key: Any) -> str:
    """A producer-controlled key: bare only when it is a plain identifier."""

    key = str(key)
    return key if _BARE_KEY.fullmatch(key) else safe_json(key)


def is_fenced(value: Any) -> bool:
    return isinstance(value, str) and FENCE in value


def has_fence(value: Any) -> bool:
    if isinstance(value, dict):
        return any(has_fence(v) for v in value.values())
    if isinstance(value, list):
        return any(has_fence(v) for v in value)
    return is_fenced(value)


def is_block(value: Any) -> bool:
    """A value that never goes on a header or key line."""

    if isinstance(value, str):
        return (
            any(c in LINE_BREAKS for c in value)
            or FENCE in value
            or len(value) > LONG
        )
    if isinstance(value, (dict, list)):
        return has_fence(value) or len(safe_json(value)) > LONG
    return False


def _inline(value: Any) -> str:
    return safe_json(value) if isinstance(value, (dict, list)) else scalar(value)


# ── escaping ──────────────────────────────────────────────────────────────


def _clean(text: str, cf_as: str) -> str:
    out = []
    for c in text:
        category = unicodedata.category(c)
        if category == "Cf":
            out.append(cf_as)
        elif category in ("Mn", "Me"):
            continue
        elif c in _BLANK_LOOKALIKES:
            out.append(" ")
        else:
            out.append(c)
    return "".join(out)


def _matching_copy(text: str, cf_as: str) -> str:
    """One matching copy of a line: cleaned, NFKC, mapped, casefolded.

    Format characters (Cf) become ``cf_as`` (``""`` deletes them, ``" "``
    spaces them); combining marks (Mn, Me) are deleted; blank lookalikes
    become spaces. Then NFKC, the same deletions again (NFKC can expose
    marks), the marker lookalikes, casefold, and leading whitespace stripped.
    """

    cleaned = _clean(unicodedata.normalize("NFKC", _clean(text, cf_as)), cf_as)
    return cleaned.translate(_CONFUSABLES).casefold().lstrip()


@functools.lru_cache(maxsize=4096)
def _is_invisible(c: str) -> bool:
    """Whether ``c`` cleans to nothing visible in a matching copy.

    Judged on the CLEANED form, not the raw character: a spacing accent
    such as U+00A8, U+00B4 or U+00AF looks visible but NFKC turns it into a
    space plus a combining mark, which the cleaning deletes (review B1 on
    58c3678f40). Counting it as visible let 64 of them fill the fast-path
    prefix and hide a marker behind them.
    """
    if c.isspace() or c in _BLANK_LOOKALIKES:
        return True
    if unicodedata.category(c) in ("Cf", "Mn", "Me"):
        return True
    return not _matching_copy(c, " ").strip()


def _fast_prefix(line: str) -> str:
    """The leading invisible run plus the first visible characters.

    Invisible characters between the visible ones are kept, so a marker
    split by format or combining characters is still whole in the prefix.
    """

    visible = 0
    for index, c in enumerate(line):
        if not _is_invisible(c):
            visible += 1
            if visible > _FAST_PATH_VISIBLE:
                return line[:index]
    return line


def _structural_copy(copy: str, *, whole: bool) -> Optional[bool]:
    """Decide on one matching copy; None when a prefix cannot decide."""

    match = _STRUCTURE.match(copy)
    if whole:
        return match is not None
    if not copy.strip():
        # A prefix that cleans to nothing decides nothing: the marker, if
        # any, is past it.
        return None
    if match is not None and match.end() < len(copy):
        return True
    if match is None and _STRUCTURE_OPEN.fullmatch(copy.rstrip()) is None:
        return False
    return None


def structural(line: str) -> bool:
    """Whether a body line starts like result structure (after cleaning).

    Two matching copies are tried, with format characters deleted and with
    them read as spaces: ``sea\\u2060rch ok`` matches the first,
    ``[2]\\u200bFAKE`` the second. The whole line is cleaned when the fast
    path cannot decide, so no invisible run of any length hides a marker.
    Only the matching copy is changed; the line prints as it came, plus the
    escape.
    """

    if line.startswith("\\"):
        return True
    prefix = _fast_prefix(line)
    whole = len(prefix) == len(line)
    undecided = False
    for cf_as in ("", " "):
        verdict = _structural_copy(_matching_copy(prefix, cf_as), whole=whole)
        if verdict:
            return True
        if verdict is None:
            undecided = True
    if not undecided:
        return False
    return any(
        _STRUCTURE.match(_matching_copy(line, cf_as)) is not None
        for cf_as in ("", " ")
    )


def structural_slow(line: str) -> bool:
    """:func:`structural` without the fast path: the reference rule."""

    if line.startswith("\\"):
        return True
    return any(
        _STRUCTURE.match(_matching_copy(line, cf_as)) is not None
        for cf_as in ("", " ")
    )


def escape_body(text: str) -> str:
    """Give every structural line one leading backslash.

    Splits on the full ``str.splitlines()`` break set; each piece keeps its
    own terminator, so the text is otherwise unchanged. To read the source
    text back, strip one leading backslash from each line that has one.
    """

    out = []
    for piece in text.splitlines(keepends=True) or [""]:
        body = piece.rstrip(LINE_BREAKS)
        out.append(("\\" + piece) if structural(body) else piece)
    return "".join(out)


def unescape_body(text: str) -> str:
    """The inverse of :func:`escape_body`."""

    return "".join(
        piece[1:] if piece.startswith("\\") else piece
        for piece in text.splitlines(keepends=True)
    )


# ── building blocks ───────────────────────────────────────────────────────


def _flatten(value: Any, path: str) -> list[tuple[str, Any]]:
    if isinstance(value, dict) and value:
        out: list[tuple[str, Any]] = []
        for key, item in value.items():
            out += _flatten(item, f"{path}.{key_text(key)}")
        return out
    if isinstance(value, list) and value and has_fence(value):
        out = []
        for index, item in enumerate(value):
            out += _flatten(item, f"{path}[{index}]")
        return out
    return [(path, value)]


def _block_lines(name: str, value: Any, indent: str = "") -> list[str]:
    body = value if isinstance(value, str) else safe_json(value)
    return [f"{indent}--- {name}", escape_body(body)]


class _Parts:
    """Header facts and trailing blocks of one card or line group."""

    def __init__(self, *, comma: bool = False) -> None:
        self.facts: list[str] = []
        self.blocks: list[str] = []
        #: Facts that will be joined with ``, `` (a parent): a value that
        #: holds ``, `` is quoted so the join stays unambiguous.
        self.comma = comma

    def inline(self, value: Any) -> str:
        text = _inline(value)
        if self.comma and isinstance(value, str) and text == value and ", " in value:
            return safe_json(value)
        return text

    def fact(self, name: str, value: Any, *, label: Optional[str] = None) -> None:
        """``label value`` (or ``name=value``), or a block when it must be.

        ``name`` is already a rendered key or path (see :func:`key_text`).
        """

        if isinstance(value, (dict, list)) and has_fence(value):
            for path, leaf in _flatten(value, name):
                self.fact(path, leaf)
            return
        if is_block(value):
            self.block(name, value)
            return
        text = self.inline(value)
        self.facts.append(f"{label} {text}" if label else f"{name}={text}")

    def block(self, name: str, value: Any) -> None:
        self.blocks += _block_lines(name, value)


def _key_lines(
    mapping: Any, indent: str = "",
) -> tuple[list[str], list[str]]:
    """``key: value`` lines, then blocks (key lines precede blocks)."""

    lines: list[str] = []
    blocks: list[str] = []
    for raw_key, value in (mapping or {}).items():
        key = key_text(raw_key)
        if isinstance(value, (dict, list)) and has_fence(value):
            for path, leaf in _flatten(value, key):
                if is_block(leaf):
                    blocks += _block_lines(path, leaf, indent)
                else:
                    lines.append(f"{indent}{path}: {_inline(leaf)}")
        elif is_block(value):
            blocks += _block_lines(key, value, indent)
        else:
            lines.append(f"{indent}{key}: {_inline(value)}")
    return lines, blocks


# ── status line and notices ───────────────────────────────────────────────

_STATUS_ANSWER = frozenset({"ok"})


def _status_word(payload: dict[str, Any], *, raised: bool) -> str:
    status = payload.get("status")
    if status == "refused":
        return "refused"
    if status is None or status in _STATUS_ANSWER:
        return "error" if raised and payload.get("error") else "ok"
    return f"status={token_text(status)}"


def _generation_of(payload: dict[str, Any]) -> Any:
    generation = payload.get("pinned_generation_id") or payload.get("generation_id")
    error = payload.get("error")
    if generation is None and isinstance(error, dict):
        data = error.get("data")
        if isinstance(data, dict):
            generation = data.get("pinned_generation_id") or data.get("generation_id")
    return generation or payload.get("session_default_generation_id")


def _int(value: Any) -> Optional[int]:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _lower_bound(payload: dict[str, Any]) -> bool:
    # `N+` when the count is a lower bound. Until search-result-shape sends
    # `total_count_basis` on lexical pages, a page that says only
    # `rank_capped` is read as the same lower bound (a transitional rule,
    # deleted once every search path sends the basis).
    basis = payload.get("total_count_basis")
    return basis == "lower_bound" or bool(basis is None and payload.get("rank_capped"))


#: The count fact of a page whose total was not counted inside its share of
#: the call's time budget (`total_count_reason: count_not_computed_in_time`).
_COUNT_NOT_IN_TIME = "count not computed in time"


#: What a grep page's scan bound counts, as the status line names it.
_GREP_BOUND_UNITS = {
    "candidate_rows": "candidate records",
    "index_rows": "rows the word index returned",
    "id_rows": "records of the scope",
}


def _grep_scanned(payload: dict[str, Any]) -> Optional[str]:
    """``no match in the first 2,000 candidate records, more may follow``
    for a grep page that stopped at its bound with no record of its own
    (`grep_searched.bound_read`), or None when the page does not say."""
    searched = payload.get("grep_searched")
    bound_read = searched.get("bound_read") if isinstance(searched, dict) else None
    if not isinstance(bound_read, dict):
        return None
    read = _int(bound_read.get("rows"))
    unit = _GREP_BOUND_UNITS.get(str(bound_read.get("bound")))
    if read is None or unit is None:
        return None
    where = "next" if bound_read.get("continued") is True else "first"
    more = "more may follow" if payload.get("continuation_token") else "not read further"
    return f"no match in the {where} {read:,} {unit}, {more}"


def _counts(payload: dict[str, Any], n_rows: int) -> Optional[str]:
    """``<first>-<last> of <N | N+ | count unknown>`` (design `read-model` 3.5).

    A page with no ``total_count`` reads ``count unknown``, unless it is the
    whole set: not cut (:func:`_cut`) and starting at the first row, so its
    row count is the set's size. A floor that is only the rows delivered so
    far reads ``<first>-<last>, more follow`` (issue #3666): ``71+`` there
    says nothing but that, and read as a count to chase.
    """
    total = payload.get("total_count")
    offset = _int(payload.get("offset")) or 0
    bounded = _not_a_total(payload)
    if bounded is not None:
        # A page that stopped at a bound never shows a total (issue #3666):
        # `0 of 0+` read as "no match" where only N records were read.
        span = f"{offset + 1}-{offset + n_rows}" if n_rows else "0"
        read = _int(bounded.get("records_read"))
        return f"{span} on this page" + (
            f" of {read:,} read" if read is not None else ""
        ) + ", not a total"
    if payload.get("page_basis") == "scan_bound_reached" and n_rows == 0:
        # A grep page that stopped at a row bound before any record of its
        # own: no count line ("0 of 0+" reads as no matches). Lane ri,
        # tokens: agents read "no records on this page" as no match (12 such
        # pages, none continued), so the line says how much was read.
        scanned = _grep_scanned(payload)
        if scanned is not None:
            return scanned
        return "bound reached, no records on this page"
    unknown = payload.get("total_count_basis") == "unknown"
    if total is None and n_rows == 0 and not unknown:
        return None
    span = f"{offset + 1}-{offset + n_rows}" if n_rows else "0"
    if unknown and payload.get("total_count_reason") == "count_not_computed_in_time":
        # The total had a share of the call's time budget and did not
        # finish in it: said here, where the count would be.
        return f"{span} of {_COUNT_NOT_IN_TIME}"
    if unknown:
        return f"{span} of count unknown"
    if total is None:
        if offset == 0 and not _cut(payload):
            return f"{span} of {n_rows}"
        return f"{span} of count unknown"
    lower = _lower_bound(payload)
    cap = payload.get("ranked_cap")
    if (
        lower and n_rows and _int(total) == offset + n_rows
        and not (isinstance(cap, dict) and cap)
        # A windows page's `+` counts undecided records, not rows to come.
        and _undecided(payload) is None
        # Only where the page names what continues it: the same line ends
        # `next: offset=<n>`. A floor nothing continues keeps its `+`.
        and _next(payload) is not None
    ):
        return f"{span}, more follow"
    text = f"{span} of {scalar(total)}{'+' if lower else ''}"
    if lower and isinstance(cap, dict) and cap:
        # The rank cap that makes the count a floor, with its unit
        # (PACT search-result-shape, D2).
        text += " (cap " + ", ".join(
            f"{_inner_text(value)} {key_text(unit)}" for unit, value in cap.items()
        ) + ")"
    return text


def _not_a_total(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    """A page's well-formed ``not_a_total`` block, or None (issue #3666).

    ``{"reason": <the bound>, "records_read": <int | null>, "for_a_total":
    <a call | null>}``: the page stopped at a scan, count or byte bound, so
    the rows it holds say nothing about how many records match.
    """
    block = payload.get("not_a_total")
    if isinstance(block, dict) and isinstance(block.get("reason"), str):
        return block
    return None


def _value_hint(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    """A lookup's well-formed ``value_hint`` (issue #3666), or None:
    ``{"field", "sent", "stored"}``, a stored value equal to the sent one
    ignoring case, when the equality matched nothing."""
    hint = payload.get("value_hint")
    if isinstance(hint, dict) and all(
        isinstance(hint.get(key), str) for key in ("field", "sent", "stored")
    ):
        return hint
    return None


def _not_a_total_notice(payload: dict[str, Any]) -> Optional[str]:
    """``! not a total: ...`` and the call that counts, when one is proven."""
    block = _not_a_total(payload)
    if block is None:
        return None
    line = f"! not a total: the read stopped at {scalar(block['reason'])}"
    call = block.get("for_a_total")
    if not isinstance(call, dict):
        return line + "; narrow the call so it reads every match"
    rendered = (
        _retry_call_text(call)
        if isinstance(call.get("tool"), str) and isinstance(call.get("arguments"), dict)
        else None
    )
    return line + "; for a total: " + (rendered or "see not_a_total.for_a_total")


def cut_line(reason: str, shown: int, left: Optional[int], next_call: str) -> str:
    """The one line a cut page prints (issue #3666, reply-cap's cut line):
    ``! <reason>: K rows shown, L left; next: <call>``. ``reason`` is what
    cut it (``cut to fit B bytes``, ``cut at limit L``); ``left`` is None
    when the rows left are not counted."""
    rest = "count unknown" if left is None else f"{left:,} left"
    return f"! {reason}: {shown:,} rows shown, {rest}; next: {next_call}"


def page_rows_key(payload: dict[str, Any], tool: str) -> Optional[str]:
    """The key of the rows list the text view prints for `tool`'s page."""
    return _rows_key(payload, tool)


def _exact_requested(payload: dict[str, Any]) -> bool:
    # `exact_over_pages` (issue #3666): an exact listing that takes more
    # than one page; its next page is asked for the same way.
    return payload.get("completeness") in ("exact", "exact_over_pages")


def _cut_exact_listing(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    """The ``listing`` of an exact lexical page whose read stopped at a
    bound (issue #3666), or None. Only such a page carries a listing under
    ``completeness: "exact"`` (grep and attrs listings say
    ``exact_over_pages``)."""
    listing = payload.get("listing")
    if (
        payload.get("completeness") == "exact" and isinstance(listing, dict)
        and listing.get("complete") is False and not has_fence(listing)
    ):
        return listing
    return None


def _cut_listing_cap(listing: dict[str, Any]) -> Optional[str]:
    """``200 rows`` from a cut listing's ``ranked_cap``, or None."""
    cap = listing.get("ranked_cap")
    if isinstance(cap, dict) and cap:
        return ", ".join(f"{_inner_text(value)} {key_text(unit)}" for unit, value in cap.items())
    return None


def _cut_listing_fact(listing: dict[str, Any]) -> str:
    """``exact within the first <cap> ranked; listing stops there (a literal
    text match: method=grep completeness=exact)`` (issue #3666). Without a
    cap: the work-cap bound it names, or, under enforced access rules,
    only that the listing is not shown whole."""
    cap = _cut_listing_cap(listing)
    bounds = listing.get("bounds")
    if cap is not None:
        head = f"exact within the first {cap} ranked; listing stops there"
    elif isinstance(bounds, list) and bounds:
        head = "exact within the rows read; listing stops at " + ", ".join(map(str, bounds))
    else:
        head = "exact for these rows; listing not shown whole"
    literal = listing.get("literal_text_match")
    if isinstance(literal, dict) and literal:
        head += " (a literal text match: " + " ".join(
            f"{key_text(key)}={_inner_text(value)}" for key, value in literal.items()
        ) + ")"
    return head


def _listing_fact(payload: dict[str, Any]) -> Optional[str]:
    """What an exact listing that spans pages says about itself, or None.

    ``listing`` is ``{"complete": bool, "reason": <word>}`` on a page served
    under ``completeness="exact"`` whose matches do not fit one page. One
    page is never called every match: the pages together are. An exact
    lexical page cut at a bound says it is exact only within the rows it
    read (:func:`_cut_listing_fact`).
    """
    listing = payload.get("listing")
    cut = _cut_exact_listing(payload)
    if cut is not None:
        return _cut_listing_fact(cut)
    if payload.get("completeness") != "exact_over_pages" or not isinstance(listing, dict):
        return None
    if listing.get("complete") is True:
        return "listing complete (the pages up to this one are every match)"
    if listing.get("complete") is False:
        return "listing continues (the pages together are every match)"
    return None


def _retrieval(payload: dict[str, Any]) -> dict[str, Any]:
    """`coverage.retrieval` (PACT operator-page-counts-3190), or empty."""
    coverage = payload.get("coverage")
    retrieval = coverage.get("retrieval") if isinstance(coverage, dict) else None
    return retrieval if isinstance(retrieval, dict) else {}


def _metadata(payload: dict[str, Any]) -> bool:
    """A ``view="metadata"`` search page (#3222 slice 3): no rows."""
    return payload.get("view") == "metadata"


def _summary(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    """A metadata page's well-formed count ``summary``, or None."""
    block = payload.get("summary")
    if (
        isinstance(block, dict)
        and isinstance(block.get("by"), list)
        and isinstance(block.get("groups"), list)
        and all(isinstance(group, dict) for group in block["groups"])
        and _int(block.get("groups_total")) is not None
    ):
        return block
    return None


def _summary_next(payload: dict[str, Any]) -> Optional[str]:
    """``aggregate(input={"ref": <id>}, by=[...], offset=<n>)``: the call that
    reads a summary's remaining groups, when the summary names one."""
    block = _summary(payload)
    call = block.get("next") if block is not None else None
    arguments = call.get("arguments") if isinstance(call, dict) else None
    if not isinstance(arguments, dict):
        return None
    reference = arguments.get("input")
    if not isinstance(reference, dict) or not isinstance(reference.get("ref"), str):
        return None
    text = (
        f'aggregate(input={{"ref": {safe_json(reference["ref"])}}}, '
        f'by={safe_json(arguments.get("by"))}, '
        f'metrics={safe_json(arguments.get("metrics") or ["count"])}'
    )
    offset = _int(arguments.get("offset"))
    return text + (f", offset={offset})" if offset else ")")


def _next(payload: dict[str, Any]) -> Optional[str]:
    """What continues the read: ``offset=<n>`` or the token itself.

    A token is passed back as ``next`` (PACT read-model-s6-page-next); an
    operator that pages only by offset says ``offset=<n>``.
    """
    if _metadata(payload):
        # A metadata page delivers no row, so only a summary cut short
        # continues it, by `aggregate` over the reference (design D3, L7).
        return _summary_next(payload)
    if isinstance(payload.get("next_query"), dict):
        # A `query` page cut at max_bytes (issue #3666): the call that reads
        # the next rows is printed in full below, with its order note.
        return "next_query below"
    reference = payload.get("result_reference")
    delivered = _int(reference.get("next_offset")) if isinstance(reference, dict) else None
    if payload.get("next_offset_reason") == "page_reference" and delivered is not None:
        # A retained search pages its reference, not search (#3222, H1): from
        # the members this page delivered (a passage row is not a member).
        # Without the reference's own offset nothing is guessed from rows.
        return f'map(input={{"ref": {safe_json(reference.get("id"))}}}, offset={delivered})'
    if payload.get("next_offset") is not None:
        # The next page of an exact lexical read is exact only if asked for
        # again (PACT lexical-index-scan-cap-3162).
        mode = ' completeness="exact"' if _exact_requested(payload) else ""
        return f"offset={scalar(payload['next_offset'])}{mode}"
    if payload.get("continuation_token"):
        if payload.get("method") == "grep":
            # `search` takes no `next` (no route reads a `grep_page` token),
            # so a grep page names the argument its token goes back as.
            return f"continuation_token={scalar(payload['continuation_token'])}"
        return scalar(payload["continuation_token"])
    search_next = _search_next(payload)
    if search_next.get("offset") is not None:
        # Every member was delivered, but search itself has more hits: they
        # are read by search at that offset, without retain (#3222, M5).
        return f"search offset={scalar(search_next['offset'])} without retain"
    return None


def _search_next(payload: dict[str, Any]) -> dict[str, Any]:
    """A retained search's own continuation (``search_next``), or empty."""
    value = payload.get("search_next")
    return value if isinstance(value, dict) else {}


def _reference_line(payload: dict[str, Any]) -> Optional[str]:
    """``reference <id> · order rank · expires <time>[ · parent <id>]``.

    A result reference (#3222) on its own top line, so a model reads the id
    without parsing JSON. None when the payload carries none.
    """
    reference = payload.get("result_reference")
    if not isinstance(reference, dict) or not isinstance(reference.get("id"), str):
        return None
    parts = [
        f"reference {token_text(reference['id'])}",
        f"order {token_text(reference.get('order'))}",
        f"expires {scalar(reference.get('expires_at'))}",
    ]
    if reference.get("parent") is not None:
        parts.append(f"parent {token_text(reference['parent'])}")
    if _int(reference.get("passages_excluded")):
        # Page rows the records fold left as passages: delivered, not members.
        parts.append(f"passages excluded {scalar(reference['passages_excluded'])}")
    search_next = _search_next(payload)
    if search_next.get("offset") is not None:
        # Search's own next page, past this reference's members (#3222, M5).
        parts.append(f"search next offset {scalar(search_next['offset'])}")
    if search_next.get("reason") is not None:
        parts.append(f"search next reason {scalar(search_next['reason'])}")
    if search_next.get("remedy") is not None:
        parts.append(f"search next remedy {safe_json(search_next['remedy'])}")
    return SEP.join(parts)


def _undecided(payload: dict[str, Any]) -> Optional[int]:
    """How many records a `view="windows"` page could not decide, or None.

    PACT read-record-windows: an undecided record makes `total_count` a
    lower bound without any rank cap, so the page says ``N undecided``
    instead of ``bound reached (rank cap)``; its rows are still delivered.
    """
    value = payload.get("undecided_records")
    if payload.get("view") == "windows" and isinstance(value, list) and value:
        return len(value)
    return None


def _cut(payload: dict[str, Any]) -> bool:
    """The page is not the whole answer the call asked for."""
    return bool(
        _next(payload) is not None
        or payload.get("truncated")
        or payload.get("complete") is False
        or payload.get("truncation_reason")
        or "response_byte_budget" in payload
        or payload.get("next_offset_reason") is not None
        or _search_next(payload).get("reason") is not None
        or (_lower_bound(payload) and _undecided(payload) is None)
    )


def _grep_timed_out(payload: dict[str, Any]) -> bool:
    """A grep page whose scan stopped at its statement timeout (no row bound)."""
    return payload.get("method") == "grep" and payload.get("page_basis") == "timed_out"


def _bound(payload: dict[str, Any]) -> str:
    """The bound a cut page reached, as ``bound reached (<what>)`` says it."""
    if payload.get("truncation_reason") or "response_byte_budget" in payload:
        budget = payload.get("response_byte_budget")
        return "max_bytes" + (f" {scalar(budget)}" if budget is not None else "")
    if payload.get("method") == "grep" and payload.get("page_basis") == "scan_bound_reached":
        name = payload.get("page_scan_bound_hit")
        bounds = payload.get("page_scan_bound")
        if name is not None:
            value = bounds.get(name) if isinstance(bounds, dict) else None
            return scalar(name) + (f" {scalar(value)}" if value is not None else "")
    if payload.get("payload_truncated"):
        cap = payload.get("payload_byte_cap")
        return "payload bytes" + (f" {scalar(cap)}" if cap is not None else "")
    if payload.get("order_basis") is not None and payload.get("complete") is False:
        # inspect text: the page filled the call's byte budget.
        return "max_bytes"
    if payload.get("result_kind") in ("text_batch", "node_batch"):
        # inspect texts: the page filled the batch's signed byte budget.
        budget = payload.get("max_bytes")
        return "max_bytes" + (f" {scalar(budget)}" if budget is not None else "")
    if isinstance(payload.get("truncated_sections"), dict):
        size = payload.get("section_page_size")
        return "page_size" + (f" {scalar(size)}" if size is not None else "")
    if _grep_timed_out(payload):
        ms = payload.get("statement_timeout_ms")
        return "timeout" + (f" {scalar(ms)} ms" if ms is not None else "")
    if payload.get("next_offset_reason") is not None and _next(payload) is None:
        return "exact order"
    cap = payload.get("ranked_cap")
    if _lower_bound(payload) and _next(payload) is None:
        if isinstance(cap, dict) and cap:
            return "rank cap " + ", ".join(
                f"{_inner_text(value)} {key_text(unit)}" for unit, value in cap.items()
            )
        return "rank cap"
    if _int(payload.get("limit")) is not None:
        return f"limit {payload['limit']}"
    if "returned_count" in payload:
        # query: the SQL's own LIMIT/OFFSET page it; the server caps rows.
        return "row cap"
    return "page"


def _delivery_complete(payload: dict[str, Any]) -> tuple[bool, Any]:
    """``(True, value)`` when the page carries ``coverage.delivery.complete``.

    Delivery is independent of retrieval (design `read-model` section 14):
    a retrieval cap with every selected row delivered is delivery-complete.
    """
    coverage = payload.get("coverage")
    if isinstance(coverage, dict) and isinstance(coverage.get("delivery"), dict):
        delivery = coverage["delivery"]
        if "complete" in delivery:
            return True, delivery["complete"]
    return False, None


def _ending(payload: dict[str, Any]) -> list[str]:
    """The last facts of an answer's status line (design `read-model` 3.5).

    Exactly one of ``complete``; ``completion unknown``; or ``bound reached
    (<what>)`` then ``next: <offset=n | token>``, with ``next: none`` when
    nothing continues the read (the exact-order bound, a query's row cap).

    A page with ``coverage.delivery`` (search, filter, map, aggregate) ends
    as its delivery says, so text and JSON agree: ``complete`` for true
    (a retrieval cap shows only in the coverage facts), ``bound reached``
    for false, ``completion unknown`` for null. Other pages end by their
    own cut facts (:func:`_cut`).
    """
    if _metadata(payload):
        # Review L7: a metadata page always ends at a bound, never
        # `complete`: its reference holds at most `limit` members.
        return [f"bound reached ({_metadata_bound(payload)})", f"next: {_next(payload) or 'none'}"]
    cut = _cut_exact_listing(payload)
    if cut is not None and _next(payload) is None:
        # Issue #3666: the last page an exact lexical read cut at a bound can
        # reach is not the end of the matches: never `complete`. The bound
        # is the listing's own; none is named when the listing names none.
        cap = _cut_listing_cap(cut)
        bounds = cut.get("bounds")
        what = (
            f"rank cap {cap}" if cap is not None
            else ", ".join(map(str, bounds)) if isinstance(bounds, list) and bounds
            else "not shown whole"
        )
        return [f"bound reached ({what})", "next: none"]
    has_delivery, complete = _delivery_complete(payload)
    # A cursor always ends `bound reached ... · next: <cursor>`: it is never
    # hidden, even under a delivery value that disagrees with it (the
    # envelope never builds one; `delivery.complete` is false with a cursor).
    if has_delivery and _next(payload) is None:
        if complete is True:
            return ["complete"]
        if complete is None:
            return ["completion unknown"]
    elif not _cut(payload):
        return ["complete"]
    return [f"bound reached ({_bound(payload)})", *_text_left_facts(payload),
            f"next: {_next(payload) or 'none'}"]


def _text_left_facts(payload: dict[str, Any]) -> list[str]:
    """A cut text page's remainder (#3634): how much is left and the
    max_bytes that reads it in one page, so a reader does not take a cut
    page for the whole record."""
    left = payload.get("text_left")
    if not isinstance(left, dict) or not isinstance(left.get("chars"), int):
        return []
    segments = left.get("segments")
    facts = [f"{left['chars']:,} chars left"
             + (f" in {segments} segments" if isinstance(segments, int) and segments > 1 else "")]
    if isinstance(left.get("max_bytes_for_rest"), int):
        facts.append(
            f"max_bytes {left['max_bytes_for_rest']} "
            + ("reads the most per page" if left.get("more_than_one_page") else "reads the rest")
        )
    return facts


def _metadata_bound(payload: dict[str, Any]) -> str:
    """The bound a metadata page names: a byte cut of its summary, the
    summary's group cap, else the reference's member ``limit``."""
    block = _summary(payload)
    bounds = block.get("bounds") if block is not None else None
    bounds = bounds if isinstance(bounds, list) else []
    if "output_bytes" in bounds:
        return "max_bytes"
    if "summary_groups" in bounds and block is not None:
        return f"summary_groups {len(block['groups'])}"
    limit = _int(payload.get("limit"))
    if limit is None:
        coverage = payload.get("coverage")
        selection = coverage.get("selection") if isinstance(coverage, dict) else None
        limit = _int(selection.get("limit")) if isinstance(selection, dict) else None
    return "limit" + (f" {limit}" if limit is not None else "")


def _metadata_counts(payload: dict[str, Any]) -> Optional[str]:
    """``<n> selected[ of <total>[+]]``: a metadata page's exact selected
    size (``coverage.selection.size``), and search's own count when known."""
    coverage = payload.get("coverage")
    selection = coverage.get("selection") if isinstance(coverage, dict) else None
    size = _int(selection.get("size")) if isinstance(selection, dict) else None
    if size is None:
        return None
    text = f"{size} selected"
    total = payload.get("total_count")
    if _int(total) is not None:
        text += f" of {total}{'+' if _lower_bound(payload) else ''}"
    return text


def _summary_lines(block: dict[str, Any]) -> list[str]:
    """A metadata page's count summary: a header, then its groups as the
    ``aggregate`` table prints them."""
    head = [
        "summary by " + ", ".join(part_text(field) for field in block["by"]),
        f"{len(block['groups'])} of {block['groups_total']} groups",
    ]
    bounds = block.get("bounds")
    if isinstance(bounds, list) and bounds:
        head.append("bounds " + ", ".join(key_text(bound) for bound in bounds))
    return [SEP.join(head), *_table_rows("aggregate", block["groups"], {}, 1, _Shown())]


#: Token refusals where the token itself did not open as sent, so copying it
#: again from the reply that printed it can help (continuation.OPAQUE_REASONS).
_COPYABLE_TOKEN_REASONS = frozenset({
    "malformed", "signature_invalid", "payload_invalid", "version_invalid",
})


def _retry(error: dict[str, Any]) -> tuple[str, Optional[str]]:
    """A refusal's ``retry: <change>`` text, and the error key it came from.

    A refusal that carries a call the caller can send as written
    (``retry_call`` with ``tool``, ``arguments`` and mode ``corrected`` or
    ``guess``) names that call here, on the line a reader meets first. It is
    always built from ``retry_call`` by one renderer (:func:`_retry_call_text`):
    printed plain, never JSON-quoted, one line. When it is too long for the
    line this says ``see retry_call below``; it never says ``see fix_hint``
    while such a call is offered, and the hint is not used for it. A
    continuation's ``retry_call`` (mode ``continue``) holds a token
    placeholder and a size range, not a call to send, and keeps its hint.
    Otherwise: the error's own ``fix_hint`` or ``remedy`` when it is one
    line; else the fallback it names; else ``none given``.
    """
    call = error.get("retry_call")
    if (
        isinstance(call, dict) and isinstance(call.get("tool"), str)
        and isinstance(call.get("arguments"), dict)
        and call.get("mode") in ("corrected", "guess")
    ):
        rendered = _retry_call_text(call)
        return (rendered, "retry_call") if rendered is not None else (
            "see retry_call below", None,
        )
    if (
        isinstance(call, dict) and isinstance(call.get("tool"), str)
        and isinstance(call.get("arguments"), dict) and call.get("mode") == "restart"
        and error.get("code") == "continuation_token_invalid"
    ):
        # Lane ri, tokens: a continuation token that did not open. The
        # restart is the caller's own call without the token, printed as
        # sent; a token that was miscopied can also be copied again.
        rendered = _retry_call_text(call)
        if rendered is not None:
            data = error.get("data") if isinstance(error.get("data"), dict) else {}
            copy_again = data.get("reason") in _COPYABLE_TOKEN_REASONS
            return (
                ("copy the token exactly as printed, or restart: " if copy_again
                 else "restart: ") + rendered,
                "retry_call",
            )
    for key in ("fix_hint", "remedy"):
        value = error.get(key)
        if isinstance(value, str) and value and not is_block(value):
            return scalar(value), key
    if error.get("fallback_method") is not None:
        return f"method={scalar(error['fallback_method'])}", None
    if error.get("fallback_operator") is not None:
        return f"use {scalar(error['fallback_operator'])}", None
    for key in ("fix_hint", "remedy", "schema_hint"):
        if error.get(key) is not None:
            return f"see {key}", None
    return "none given", None


def _retry_call_text(call: dict[str, Any]) -> Optional[str]:
    """``tool(arg=<json>, ...)`` for a corrected call, or None when it does
    not fit one safe line (then the line points at the ``retry_call`` field)."""
    tool, arguments = call["tool"], call["arguments"]
    if not all(isinstance(name, str) and name.isidentifier() for name in (tool, *arguments)):
        return None
    if has_fence(arguments):
        return None
    text = f"{tool}(" + ", ".join(
        f"{name}={safe_json(value)}" for name, value in arguments.items()
    ) + ")"
    repeat = call.get("repeat")
    if isinstance(repeat, list) and repeat and all(
        isinstance(name, str) and name.isidentifier() for name in repeat
    ):
        text += ", with the same " + " and ".join(repeat)
    if call.get("mode") == "guess":
        text += " (a guess)"
    if len(text) > LONG or SEP in text or any(_unsafe(c) for c in text):
        return None
    return text


def _where_checked_fact(payload: dict[str, Any]) -> Optional[str]:
    """The status line's ``where checked on the top <cap> ranked only`` fact
    (issue #3666), or None.

    Lexical search checks a row filter it cannot run in SQL on its ranked
    candidates only; ``where_checked_on`` is set when that read filled its
    cap, so a matching record past it was never checked. Under enforced
    access rules it says only ``ranked_rows_only``: no cap, not shown whole.
    Said next to the count, with the fact of what checks every record."""
    checked = payload.get("where_checked_on")
    if not isinstance(checked, dict) or has_fence(checked):
        return None
    cap = checked.get("ranked_cap")
    if isinstance(cap, dict) and cap:
        head = "where checked on the top " + ", ".join(
            f"{_inner_text(value)} {key_text(unit)}" for unit, value in cap.items()
        ) + " ranked only"
    elif checked.get("ranked_rows_only") is True:
        head = "where checked on the ranked rows only (not shown whole)"
    else:
        return None
    every = checked.get("every_record")
    safe = isinstance(every, str) and not any(_unsafe(c) for c in every) and SEP not in every
    return head + (f"; {every}" if safe else "")


def _where_fact(
    payload: dict[str, Any], *, profile_said: bool = False,
) -> tuple[Optional[str], set[str]]:
    """The status line's ``where <applied predicate>`` fact, and the keys it says.

    PACT read-model-s2-handle (design `read-model` section 5). The page's own
    ``where_applied`` when it carries one (a string prints as itself, any
    other value as compact JSON). Otherwise the predicate is composed from
    the search scope the page discloses: ``type in [...]``, ``source in
    [...]`` (only when the id list is whole) and ``path not glob [...]``,
    joined by `` and ``. None when nothing narrowed the page.

    ``profile_said``: this session was already shown the profile's type
    list, so the clause names the profile and how many types it holds
    (``type in profile <name> (<n> types)``) instead of listing them again
    (#3666, M10). The JSON form always carries the list.
    """
    if payload.get("where_applied") is not None:
        value = payload["where_applied"]
        if isinstance(value, str) and not is_block(value):
            return f"where {scalar(value)}", {"where_applied"}
        if not is_block(value):
            return f"where {safe_json(value)}", {"where_applied"}
        return None, set()
    clauses: list[str] = []
    said: set[str] = set()
    for key, word in (
        ("scoped_subtypes", "type in"),
        ("scoped_source_ids", "source in"),
        ("excluded_path_globs", "path not glob"),
    ):
        value = payload.get(key)
        if not isinstance(value, list) or not value or has_fence(value):
            continue
        if key == "scoped_source_ids" and payload.get("scoped_source_ids_truncated"):
            continue
        if key == "scoped_subtypes" and profile_said:
            clauses.append(
                f"type in profile {scalar(payload['search_profile'])} ({len(value)} types)"
            )
            said.add(key)
            continue
        clauses.append(f"{word} {safe_json(value)}")
        said.add(key)
    if not clauses:
        return None, set()
    text = " and ".join(clauses)
    if len(text) > LONG:
        return None, set()
    return f"where {text}", said


def _profile_scope_said(
    payload: dict[str, Any], session_state: Optional[dict[str, Any]],
) -> bool:
    """Whether this session was already shown this page's profile type list.

    A search scoped by a named profile (usually the deployment default)
    lists the profile's types on a session's first such page, and again
    whenever the profile or its types change; later pages name the profile
    instead (#3666, M10: seven type names on every search's first line).
    With ``session_state`` None (a byte measurement, a direct render) the
    types are always listed, as the stale-source notice is always printed.
    """
    profile = payload.get("search_profile")
    subtypes = payload.get("scoped_subtypes")
    if (
        session_state is None
        or payload.get("where_applied") is not None
        or not isinstance(profile, str) or profile in ("", "all")
        or not isinstance(subtypes, list) or not subtypes
        or has_fence(subtypes) or is_block(profile)
    ):
        return False
    key = [profile, [str(s) for s in subtypes]]
    if session_state.get("profile_scope") == key:
        return True
    session_state["profile_scope"] = key
    return False


def _stale_entries(freshness: dict[str, Any]) -> list[Any]:
    stale = freshness.get("stale")
    if isinstance(stale, list):
        return stale
    names = freshness.get("stale_sources")
    return names if isinstance(names, list) else []


def _source_name(item: Any) -> str:
    return str(item.get("source") if isinstance(item, dict) else item)


def stale_key(freshness: Any) -> Any:
    """The announced identity of an abnormal ``source_freshness`` block.

    ``None`` when every source is fresh; ``"unavailable"`` when the probe is
    unavailable; else the stale count with the sorted stale source names.
    """

    if not isinstance(freshness, dict):
        return None
    if freshness.get("status") == "unavailable":
        return "unavailable"
    stale = freshness.get("stale")
    count = freshness.get("stale_count", _int(stale))
    if not count:
        return None
    return (count, tuple(sorted(_source_name(item) for item in _stale_entries(freshness))))


def _hours(value: Any) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f"{value:g}h"
    return f"{scalar(value)}h"


def _stale_notice(freshness: dict[str, Any], count: Any) -> str:
    """One line for a stale ``source_freshness``: the count, the threshold,
    the oldest age, how many were never polled, and the stale names."""

    entries = _stale_entries(freshness)
    ages = [item.get("age_hours") for item in entries if isinstance(item, dict)]
    known = [age for age in ages if isinstance(age, (int, float)) and not isinstance(age, bool)]
    never = sum(
        1 for item in entries
        if isinstance(item, dict) and item.get("age_hours") is None
    )
    oldest = freshness.get("oldest_hours") or max(known, default=None)
    checked = freshness.get("sources_checked", freshness.get("of"))
    detail = f"oldest {_hours(oldest)}" if oldest is not None else "no poll age known"
    if never:
        detail += f"; {never} never polled"
    if freshness.get("schedules") == "unknown":
        # #3666: the registry was unreadable, so every source was judged.
        detail += "; poll schedules unknown"
    line = (
        f"! {scalar(count)} of {scalar(checked)} sources not polled for over "
        f"{_hours(freshness.get('threshold_hours'))} ({detail})"
    )
    names = sorted(_source_name(item) for item in entries)
    if names:
        line += ": " + ", ".join(scalar(name) for name in names)
    return line


def _grep_phrase_call(spans: list[str]) -> Optional[str]:
    """The `search(method="grep")` call that finds every span word for word,
    or None when grep would refuse it unscoped (the index rule: no span with
    3 or more letters or digits in a row) or cannot parse it."""
    from okg.substrate.mcp.grep_query import (
        assert_indexable_or_scoped,
        parse_grep_query,
    )
    from okg.substrate.errors import SubstrateError

    query = " ".join(f'"{span}"' for span in spans)
    try:
        assert_indexable_or_scoped(parse_grep_query(query), scoped=False)
    except SubstrateError:
        return None
    return f'search(method="grep", query={safe_json(query)})'


def _phrase_notice(payload: dict[str, Any]) -> Optional[str]:
    """`! no exact match ...` for a quoted lexical search none of whose
    checked candidates holds every quoted span (PACT
    lexical-quoted-phrases-3064): the spans no candidate holds (all of them
    when each is held somewhere but never together), the retrieval status,
    and the grep call that finds the exact text, when grep accepts it."""
    phrase = payload.get("phrase")
    if not isinstance(phrase, dict) or phrase.get("status") != "no_exact_match":
        return None
    spans = [s for s in phrase.get("spans") or [] if isinstance(s, str)]
    named = [s for s in phrase.get("unmatched_spans") or [] if isinstance(s, str)] or spans
    checked = phrase.get("checked")
    line = (
        "! no exact match for " + ", ".join(safe_json(s) for s in named)
        # A page that withholds its count withholds this one too.
        + (f" among {scalar(checked)} candidates" if checked is not None else "")
        + f" (retrieval {scalar(_retrieval(payload).get('status'))}); "
        + "results rank by shared words"
    )
    call = _grep_phrase_call(spans) if spans else None
    if call is not None:
        line += f"; exact text: {call}"
    return line


def _exact_match_lines(payload: dict[str, Any]) -> list[str]:
    """The exact pass beside a lexical page (PACT
    lexical-exact-identifier-hits): one line per span naming the records
    whose text holds it exactly, or what grep found and why it stopped, or
    why the span was not checked; each record not already on the page
    followed by its match window, escaped as a window body is. Shown before
    the ranked rows.

    A filename span (PACT filename-exact-hits) first gets a `(path ...)`
    line naming one record per path that ends with it; that line is left
    out when the path pass read every path and found none."""
    block = payload.get("exact_matches")
    if not isinstance(block, dict):
        return []
    out: list[str] = []
    for entry in block.get("spans") or []:
        if not isinstance(entry, dict):
            continue
        if "path_status" not in entry:
            out.extend(_exact_entry_lines(entry, label="grep"))
            continue
        records = [r for r in entry.get("records") or [] if isinstance(r, dict)]
        path_records = [r for r in records if r.get("matched_on") == "path"]
        path_entry = {
            "span": entry.get("span"), "status": entry.get("path_status"),
            "count": entry.get("path_count"), "count_basis": entry.get("path_count_basis"),
            "count_unit": entry.get("path_count_unit"), "records": path_records,
            "candidates_read": entry.get("path_candidates_read"),
        }
        if path_records or entry.get("path_status") not in (
            "no_exact_match", "no_visible_match",
        ):
            out.extend(_exact_entry_lines(path_entry, label="path"))
        text_entry = {
            **entry, "records": [r for r in records if r.get("matched_on") != "path"],
        }
        out.extend(_exact_entry_lines(
            text_entry, label="grep", listed_above=len(path_records),
        ))
    dropped = [s for s in block.get("dropped_spans") or [] if isinstance(s, str)]
    if dropped:
        out.append("= exact pass skipped " + ", ".join(safe_json(s) for s in dropped)
                   + f" (at most {len(block.get('spans') or [])} spans per call)")
    return out


def _exact_total(entry: dict[str, Any]) -> str:
    """An exact-pass count as read: `11 records`, `2000+ records`, or
    `count unknown`."""
    count = entry.get("count")
    basis = entry.get("count_basis")
    unit = entry.get("count_unit")
    noun = f"{scalar(unit)}s" if isinstance(unit, str) and unit else "matches"
    if count is None:
        return "count unknown"
    if basis == "exact":
        return f"{scalar(count)} {noun}"
    return f"{scalar(count)}+ {noun}"


def _exact_entry_lines(
    entry: dict[str, Any], *, label: str, listed_above: int = 0,
) -> list[str]:
    """One span's line (and its record lines) of the exact pass. `label`
    names the pass: `grep` for the text, `path` for the own-path pass.
    `listed_above` counts the path matches listed on the line before, which
    the text records leave out."""
    out: list[str] = []
    head = f"= exact {safe_json(entry.get('span'))} ({label}"
    status = entry.get("status")
    status_text = status if isinstance(status, str) else ""
    records = [r for r in entry.get("records") or [] if isinstance(r, dict)]
    holds = "has this path" if label == "path" else "holds this text"
    if status == "no_exact_match":
        out.append(head + f"): no record {holds}")
        return out
    if status == "no_visible_match":
        out.append(head + f"): no record you can read {holds} "
                   "(the count is a lower bound)")
        return out
    if status_text.startswith("omitted:"):
        out.append(head + "): dropped to fit max_bytes; rerun with a larger "
                   "max_bytes or method=\"grep\" to see it")
        return out
    if not records:
        read = entry.get("candidates_read")
        if read is not None and (status == "timeout" or status_text.startswith("refused:")):
            out.append(head + f", {_exact_total(entry)}): stopped ({scalar(status)}) after "
                       f"reading {scalar(read)} matches, before one per path was chosen")
        elif status == "matched" and listed_above:
            out.append(head + f", {_exact_total(entry)}): none listed beyond the path matches above")
        elif status == "timeout" or status_text.startswith("incomplete:"):
            out.append(head + f"): checked, stopped early ({scalar(status)}); "
                       "no match read before it stopped")
        else:
            out.append(head + f"): not checked ({scalar(status)})")
        return out
    read = entry.get("candidates_read")
    among = f"; one per path among the first {scalar(read)} read" if read is not None else ""
    out.append(head + f", {len(records)} of {_exact_total(entry)}{among}):")
    for record in records:
        line = f"  {scalar(record.get('node_id'))} ({scalar(record.get('subtype'))})"
        if record.get("result_position") is not None:
            line += f" = result [{scalar(record['result_position'])}]"
        out.append(line)
        window = record.get("window")
        if isinstance(window, str) and window.strip():
            out.append("    " + _escape_window_body(" ".join(window.split())))
    return out


#: The opening of the ISO-only notice per `date_forms_reason`. The reasons
#: are `date_conditions.ISO_ONLY_REASONS`, which this module cannot import
#: (that module imports this one); a test pins the two equal.
_DATE_FORMS_NOTICES = {
    "access_rules": "under access rules",
    "scope_too_large": (
        "this scope is too large to examine every stored date form in time{remedy}, so"
    ),
}
#: What the caller can still do, per `date_forms_remedy`
#: (`date_conditions.ISO_ONLY_REMEDIES`; a test pins the two equal). Only
#: what is true of the call is said: a call that names a type is not told to.
_DATE_FORMS_REMEDIES = {
    "name_a_type": " (name a type)",
    "add_a_narrower_condition": " (add a narrower condition that is not on a date)",
}
_DATE_FORMS_ALSO = {
    "access_rules": ", or hidden from you,",
    "scope_too_large": "",
}


def _adjusted_notice(payload: dict[str, Any]) -> Optional[str]:
    """The one line saying what a served call was read as (issue #3666).

    `adjusted` is a list of `{"rule", "sent", "served"}` the server wrote;
    any other shape is not this notice's to print.
    """
    adjusted = payload.get("adjusted")
    if not isinstance(adjusted, list) or not adjusted:
        return None
    parts: list[str] = []
    for record in adjusted:
        if not isinstance(record, dict):
            return None
        sent, served = record.get("sent"), record.get("served")
        if not isinstance(sent, str) or not isinstance(served, str):
            return None
        if any(_unsafe(c) for c in sent + served) or SEP in sent + served:
            return None
        if record.get("rule") == "more_not_listed":
            # The count of adjustments past the cap: "3 more not listed".
            parts.append(f"{sent} {served}")
        else:
            parts.append(f"{sent} read as {served}")
    return "! adjusted: " + "; ".join(parts)


def _matched_loosely_notices(payload: dict[str, Any]) -> Optional[list[str]]:
    """What each substring test compared (loose-match-3666): one line for the
    like tests, which ignore accents and case, and one for like_exact.

    `matched_loosely` is a list of `{"field", "op", "operand", "folded"}`
    the server wrote; any other shape is not this notice's to print.
    """
    records = payload.get("matched_loosely")
    if not isinstance(records, list) or not records:
        return None
    loose: list[str] = []
    exact: list[str] = []
    for record in records:
        if not isinstance(record, dict):
            return None
        field, op, folded = record.get("field"), record.get("op"), record.get("folded")
        if not (isinstance(field, str) and isinstance(folded, str)) or op not in (
            "like", "like_exact",
        ):
            return None
        (loose if op == "like" else exact).append(
            f"{token_text(field)} contains {safe_json(folded)}"
        )
    lines = []
    if loose:
        lines.append("! matched loosely: " + "; ".join(loose) + " ignoring accents and case")
    if exact:
        lines.append("! matched exactly: " + "; ".join(exact))
    return lines


#: The row key a cut row names its cut values under (response_bounds
#: `ROW_CUT_KEY`; #3666 step 3b).
ROW_CUT_KEY = "values_cut"


#: The keys a cut inspect(node) names what it cut under (response_bounds
#: `_partial_node`; #3647, #3666 lane RD).
NODE_CUT_KEYS = frozenset({"attrs_cut", "parts_cut", "read_whole"})


def _target_call_text(call: Any) -> Optional[str]:
    """A cut part's `{"tool", "target"}` read as a call to send, or None."""
    if not isinstance(call, dict) or not isinstance(call.get("tool"), str):
        return None
    return _call_text({
        "tool": call["tool"],
        "arguments": {key: value for key, value in call.items() if key != "tool"},
    })


def _record_text_cut_line(entry: dict[str, Any]) -> str:
    """The `! record text ...` line of a cut ticket thread (#3666 lane RD):
    the characters shown of the WHOLE thread's, the description's share,
    and the comments shown by position with how many are left out before
    and after them. On a long thread the node read already kept only the
    newest comments, so the comments shown need not start the thread."""
    whole = _int(entry.get("chars"))
    whole_text = (
        f"the whole thread's {_count(whole)} chars" if whole is not None
        else "a thread of unrecorded length"
    )
    served = _int(entry.get("served_chars")) or 0
    comments = entry.get("comments") if isinstance(entry.get("comments"), dict) else {}
    if not served:
        total = _int(entry.get("comments_total"))
        return (
            f"! record text left out to fit max_bytes ({whole_text}"
            + (f", {_count(total)} comments" if total is not None else "")
            + ")"
        )
    shown_bits: list[str] = []
    description = entry.get("description")
    if isinstance(description, dict):
        kept = _int(description.get("served_chars")) or 0
        chars = _int(description.get("chars")) or 0
        if kept >= chars:
            shown_bits.append("the description")
        elif kept:
            shown_bits.append(f"the description's first {_count(kept)} of {_count(chars)} chars")
    total = _int(comments.get("total")) or 0
    shown = _int(comments.get("shown")) or 0
    first = _int(comments.get("first"))
    if shown and first is not None:
        last = first + shown - 1
        span = f"comment {first:,}" if shown == 1 else f"comments {first:,}-{last:,}"
        shown_bits.append(
            f"{span} of {_count(total)}"
            + (", the last one cut short" if comments.get("last_cut") else "")
        )
    elif total:
        shown_bits.append(f"none of its {_count(total)} comments")
    left_bits = [
        f"{_count(count)} {side}"
        for side, count in (
            ("earlier", _int(comments.get("earlier_left")) or 0),
            ("later", _int(comments.get("later_left")) or 0),
        )
        if count
    ]
    said = (
        f"! record text cut to fit max_bytes: {_count(served)} of {whole_text} are shown"
    )
    if shown_bits:
        said += ": " + " and ".join(shown_bits)
    if left_bits and shown:
        said += "; " + " and ".join(left_bits) + " comments are left out"
    return said


def _node_cut_notices(payload: dict[str, Any]) -> list[str]:
    """What a cut inspect(node) left out, in plain words: the attribute
    values cut or left out, the record text served from its start, and the
    listings shortened (live relations, derived documents, the live
    overlay), each with its full size and the call that reads it whole."""
    out: list[str] = []
    cut = payload.get("attrs_cut")
    whole = _target_call_text(payload.get("read_whole"))
    bits = []
    text_cut = False
    for field, entry in (cut.items() if isinstance(cut, dict) else ()):
        if not isinstance(entry, dict):
            continue
        if entry.get("chars") is not None:
            # A text value: its start is shown; the record's text has it all.
            text_cut = True
            bits.append(
                f"{key_text(field)} {_count(entry.get('served_chars'))} of "
                f"{_count(entry.get('chars'))} chars"
            )
            continue
        # A list or object, left out whole: the record's text does not hold
        # it; the select=["attrs"] read does.
        read = _target_call_text(entry.get("read"))
        bits.append(
            f"{key_text(field)} left out ({_count(entry.get('bytes'))} B; "
            + (f"read it with {read})" if read is not None else "too large for one read)")
        )
    if bits:
        line = "! attributes cut to fit max_bytes: " + "; ".join(bits)
        if whole is not None and text_cut:
            line += f"; read the record's whole text with {whole}"
        out.append(line)
    elif whole is not None:
        out.append(f"! cut to fit max_bytes; read the record's whole text with {whole}")
    parts = payload.get("parts_cut")
    if not isinstance(parts, dict):
        return out
    text = parts.get("record_text")
    if isinstance(text, dict):
        read = _target_call_text(text.get("read_whole"))
        out.append(
            _record_text_cut_line(text)
            + (f"; read it page by page with {read}" if read is not None else "")
        )
    live = parts.get("live_relations")
    if isinstance(live, dict):
        read = _target_call_text(live.get("read_whole"))
        names = live.get("names")
        count = _count(live.get("relations"))
        size = _count(live.get("bytes"))
        if isinstance(names, list) and names:
            said = (
                f"! live relations listed by name only to fit max_bytes ({count}; "
                f"their argument schemas, {size} B, are left out): "
                + ", ".join(scalar(name) for name in names)
            )
        else:
            said = f"! live relations left out to fit max_bytes ({count} relations, {size} B)"
        out.append(said + (f"; read their arguments with {read}" if read is not None else ""))
    for key, label, reading in (
        ("documents", "derived documents", "read them with"),
        ("external_live", "live fields", "read them with"),
    ):
        entry = parts.get(key)
        if not isinstance(entry, dict):
            continue
        read = _target_call_text(entry.get("read_whole"))
        names = entry.get("names")
        count = _count(entry.get("count"))
        size = _count(entry.get("bytes"))
        if isinstance(names, list) and names:
            said = (
                f"! {label} listed by name only to fit max_bytes ({count}; {size} B): "
                + ", ".join(scalar(name) for name in names)
            )
        else:
            said = f"! {label} left out to fit max_bytes ({count}; {size} B)"
        out.append(said + (f"; {reading} {read}" if read is not None else ""))
    return out


def _node_cut_reads(payload: dict[str, Any], limit: int = 3) -> list[str]:
    """The calls that read what a cut inspect(node) left out, each once, in
    the order its notices name them: the record's whole text, then each cut
    part's own read (`parts_cut`), then a left-out attribute's read."""
    calls: list[Any] = [payload.get("read_whole")]
    parts = payload.get("parts_cut")
    if isinstance(parts, dict):
        calls += [entry.get("read_whole") for entry in parts.values() if isinstance(entry, dict)]
    cut = payload.get("attrs_cut")
    if isinstance(cut, dict):
        calls += [entry.get("read") for entry in cut.values() if isinstance(entry, dict)]
    out: list[str] = []
    for call in calls:
        text = _target_call_text(call)
        if text is not None and text not in out:
            out.append(text)
    return out[:limit]


def _count(value: Any) -> str:
    """A count or size with thousands separators; other values as scalars."""
    number = _int(value)
    return f"{number:,}" if number is not None else scalar(value)


def _cut_value_notices(payload: dict[str, Any]) -> list[str]:
    """What a reply cut to fit max_bytes left out (#3666 step 3b), one line
    each: a row's cut values, the ontology's left-out metadata, and the
    subtype samples left out, so the text never reads as whole."""
    out: list[str] = []
    for key in _ROW_KEYS:
        rows = payload.get(key)
        if not isinstance(rows, list):
            continue
        start = (_int(payload.get("offset")) or 0) + 1
        for index, row in enumerate(rows, start):
            cut = row.get(ROW_CUT_KEY) if isinstance(row, dict) else None
            if not isinstance(cut, dict):
                continue
            for field, entry in cut.items():
                if not isinstance(entry, dict):
                    continue
                if entry.get("chars") is not None:
                    out.append(
                        f"! [{index}] {key_text(field)} cut: {scalar(entry.get('served_chars'))} "
                        f"of {scalar(entry.get('chars'))} chars"
                    )
                else:
                    out.append(
                        f"! [{index}] {key_text(field)} left out "
                        f"({scalar(entry.get('bytes'))} B)"
                    )
        break
    out += _node_cut_notices(payload)
    metadata = payload.get("metadata_cut")
    if isinstance(metadata, dict):
        out.append(
            f"! metadata of {scalar(metadata.get('subtypes'))} subtypes left out to fit "
            'max_bytes; read one with inspect(target={"kind":"subtype","subtype":<name>})'
        )
    omitted = payload.get("sample_nodes_omitted")
    if isinstance(omitted, list) and omitted:
        out.append(
            "! sample nodes left out to fit max_bytes: "
            + ", ".join(safe_json(node_id) for node_id in omitted)
        )
    return out


def _text_match_notice(payload: dict[str, Any]) -> Optional[str]:
    """How much text a text condition read (issue #3666): never a total
    that stopped at a bound, so the count beside it stands."""
    facts = payload.get("text_match")
    if not isinstance(facts, dict):
        return None
    read = _int(facts.get("records_scanned"))
    if read is None:
        return None
    line = f"! text condition: {read:,} records' text read"
    if facts.get("index_timed_out") is True:
        # PACT read-search-caps-3666: the word index read passed the
        # timeout, so the records the rest of the condition admits were
        # read whole instead.
        line += " (the rest of the condition's records; the word index read timed out)"
    counted = _int(facts.get("records_counted"))
    if counted is not None:
        captured = facts.get("capture_group") is True
        merged = facts.get("merged")
        # PACT read-aggregate-variants-3666: what a group is.
        what = "the capture group's text" if captured else "every match"
        line += f"; {what} in {counted:,} records counted; "
        if isinstance(merged, str):
            line += f"groups merge {merged} (variants lists each text as matched)"
        else:
            line += "groups keep letter case though matching ignores it"
        line += (
            "; only the capture group is counted, other terms choose records"
            if captured else "; a spot two terms match counts once per term"
        )
        repeated = _int(facts.get("repeated_passages"))
        if repeated:
            line += (
                f"; {repeated:,} passages repeating their record's own text "
                "counted once, in the own text"
            )
        missing = _int(facts.get("records_without_match"))
        if missing:
            line += (
                f"; {missing:,} of the records hold no text the capture group matched"
                if captured else
                f"; {missing:,} of the records hold no matched text (a phrase "
                "running across two passages)"
            )
    cap = _int(facts.get("matches_cap"))
    rows = payload.get("items")
    if cap is not None and isinstance(rows, list) and any(
        isinstance(row, dict) and "matches" in row for row in rows
    ):
        line += f"; matches lists the first {cap} per record, match_count all"
    return line


def _once(session_state: Optional[dict[str, Any]], key: str, value: Any) -> bool:
    """Whether a repeated notice (or fact) prints on this reply (#3666).

    Once per session under ``key``, and again when ``value`` changes.
    ``value`` None clears the key (the state is normal again) and prints
    nothing. With no session state (a byte measurement, a direct render,
    the shell) it always prints, so a fitted page never grows when served.
    """
    if session_state is None:
        return value is not None
    once = session_state.setdefault("once", {})
    if value is None:
        once.pop(key, None)
        return False
    if key in once and once[key] == value:
        return False
    once[key] = value
    return True


def _source_hint_notices(payload: dict[str, Any]) -> list[str]:
    """Issue #3666: an empty search page under a `source` condition, one
    line per condition (`source_hint.source_hint`)."""
    hint = payload.get("source_hint")
    if not isinstance(hint, list):
        return []
    out: list[str] = []
    for entry in hint:
        if not isinstance(entry, dict):
            continue
        sent = safe_json(entry.get("sent"))
        ids = entry.get("source_ids")
        if ids is None:
            out.append(f"! source {sent}: {_one_line(entry.get('unavailable'))}")
            continue
        if not isinstance(ids, list) or not all(isinstance(i, str) for i in ids):
            continue
        if not ids:
            out.append(f"! source {sent}: {_one_line(entry.get('note'))}")
            continue
        more = entry.get("more")
        named = ", ".join(safe_json(i) for i in ids) + (
            f", +{more} more" if isinstance(more, int) and more else ""
        )
        note = entry.get("note")
        lead = f"{_one_line(note)}; " if isinstance(note, str) else ""
        out.append(f"! source {sent}: {lead}source ids holding it: {named}")
    return out


def _names_only_line(payload: dict[str, Any]) -> Optional[str]:
    """Lane ri (CompOps q020): a read that picked files by name names the
    grep over file texts, with their count (`name_only_hint`)."""
    from okg.substrate.mcp.name_only_hint import NAMES_ONLY_KEY, render

    return render(payload.get(NAMES_ONLY_KEY))


def _notices(
    payload: dict[str, Any], session_state: Optional[dict[str, Any]],
) -> list[str]:
    out: list[str] = []
    bounded = _not_a_total_notice(payload)
    if bounded is not None:
        out.append(bounded)
    hint = _value_hint(payload)
    if hint is not None:
        field = hint["field"]
        field = (
            "attrs." + key_text(field[len("attrs."):]) if field.startswith("attrs.")
            else key_text(field)
        )
        out.append(
            f"! no record has {field} = {safe_json(hint['sent'])}; "
            f"did you mean {safe_json(hint['stored'])}? (stored with that case)"
        )
    freshness = payload.get("source_freshness")
    key = stale_key(freshness)
    announce = key is not None
    if session_state is not None:
        if key is None:
            session_state.pop("stale", None)
        elif session_state.get("stale") == key:
            announce = False
        else:
            session_state["stale"] = key
    if announce and key == "unavailable":
        out.append(f"! source freshness unavailable ({scalar(freshness.get('cause'))})")
    elif announce:
        out.append(_stale_notice(freshness, key[0]))
    # PACT read-passage-rows-3666 (D8): the publish state (stale, or
    # unavailable) is metadata a reader asks for, never a notice on a read
    # reply. The JSON keeps `publish_freshness`; inspect(graph) prints the
    # whole block.
    members = payload.get("reference_members")
    if isinstance(members, int) and not isinstance(members, bool):
        # A read over a result reference covers the rows that reference
        # holds, never the whole graph: an agent took "complete" on 30 saved
        # rows for all 233 matches (#3657, #3658).
        out.append(
            f"! covers only the {members} record(s) held by the result reference; "
            "the search that made it may match more"
        )
    lowered = payload.get("max_chars_lowered")
    if isinstance(lowered, dict):
        # Issue #3252: an over-limit max_chars was lowered, not refused.
        out.append(
            f"! max_chars {scalar(lowered.get('requested'))} lowered to "
            f"{scalar(lowered.get('applied'))} (the maximum)"
        )
    if payload.get("date_forms_compared") == "iso_8601_only":
        # Issue #3666: a date condition compared only stored values written
        # as ISO-8601, and why. Fixed text per reason: it says nothing of
        # what any record holds.
        why = _DATE_FORMS_NOTICES.get(payload.get("date_forms_reason"))
        if why is not None:
            why = why.replace(
                "{remedy}", _DATE_FORMS_REMEDIES.get(payload.get("date_forms_remedy"), ""))
            out.append(
                f"! {why} a date condition compares only stored dates written "
                "as ISO-8601 (2025-01-01 or 2025-01-01T00:00:00Z); a date "
                f"stored in another form{_DATE_FORMS_ALSO[payload['date_forms_reason']]} "
                "matches neither the condition nor its negation"
            )
    if payload.get("total_count_reason") == "count_not_computed_in_time":
        # Issue #3666: a date call's total has a share of the call's time
        # budget; past it there is no total. The rows shown are whole.
        out.append(
            "! total not counted in time (count_not_computed_in_time): counting "
            "every match did not finish inside this call's time budget; the "
            "rows shown are exact, only the total is missing"
        )
    raised = payload.get("max_bytes_raised")
    if isinstance(raised, dict):
        # Issue #3153: a max_bytes under the floor was raised, not refused.
        out.append(
            f"! max_bytes {scalar(raised.get('requested'))} raised to "
            f"{scalar(raised.get('applied'))} (the minimum)"
        )
    as_utc = payload.get("dates_read_as_utc")
    if isinstance(as_utc, dict):
        # Issue #3666: a date with no time zone was compared as UTC, said
        # once per reply whatever the number of values.
        parts = []
        for key, what in (("bounds", "bound"), ("stored", "stored value")):
            names = as_utc.get(key)
            if isinstance(names, list) and names:
                parts.append(f"a {what} of " + ", ".join(scalar(n) for n in names))
        if parts:
            out.append(
                "! read as UTC, having no time zone: " + "; ".join(parts)
            )
    sorted_as_text = payload.get("sorted_as_text")
    if isinstance(sorted_as_text, str):
        # Issue #3666 (2c): order_by named an attribute with no declared
        # type, so the page is in text order.
        out.append(
            f"! sorted as text: {scalar(sorted_as_text)} has no declared type; "
            "numbers sort as text too"
        )
    out += _source_hint_notices(payload)
    names_only = _names_only_line(payload)
    if names_only is not None:
        out.append(names_only)
    texts = payload.get("contains_text")
    if isinstance(texts, list) and texts and all(isinstance(n, str) for n in texts):
        # Issue #3666: what a text `contains` cannot match.
        out.append("! " + "; ".join(texts))
    compared = payload.get("compared_as_stored")
    if isinstance(compared, list) and compared and all(isinstance(n, str) for n in compared):
        # Issue #3666 (2c): an eq or in on a declared attribute with an
        # operand of another JSON type, served as written.
        out.append("! compared as stored: " + "; ".join(compared))
    adjusted = _adjusted_notice(payload)
    if adjusted is not None:
        out.append(adjusted)
    out.extend(_matched_loosely_notices(payload) or ())
    resolved = payload.get("resolved_path")
    if isinstance(resolved, dict):
        # #3666 step 3b: a record named by its path; the id is exact from here.
        source = resolved.get("source")
        out.append(
            f"! path {safe_json(resolved.get('path'))}"
            + (f" in {safe_json(source)}" if source is not None else "")
            + f" resolved to {safe_json(resolved.get('node_id'))}"
        )
    out += _cut_value_notices(payload)
    text_match = _text_match_notice(payload)
    if text_match is not None:
        out.append(text_match)
    facts = payload.get("text_match")
    pairs = facts.get("may_be_one") if isinstance(facts, dict) else None
    if isinstance(pairs, list) and pairs and all(
        isinstance(p, list) and len(p) == 2 and all(isinstance(s, str) for s in p)
        for p in pairs
    ):
        # PACT read-aggregate-variants-3666: groups whose words another
        # group's words hold; no normalization can tell they are one.
        out.append(
            "! may be one value: " + "; ".join(
                f"{safe_json(a)} holds {safe_json(b)}" for a, b in pairs
            ) + "; to merge, leave the extra words out of the capture group: "
            "re:/<label>: (?:<extra> )?([A-Za-z ]+)/"
        )
    if isinstance(facts, dict) and facts.get("one_group_plain_words") is True:
        # Format v10 item C (#3666): by=["match"] over plain words.
        out.append(
            "! plain words match only themselves, so one group; a regex "
            '(re:/<regex>/) groups each distinct matched text, e.g. re:/[A-Z]+-[0-9]+/'
        )
    if payload.get("truncation_reason") or "response_byte_budget" in payload:
        bits = [f"! page cut to fit max_bytes={scalar(payload.get('response_byte_budget'))}"]
        reason = payload.get("truncation_reason")
        if reason not in (None, "response_byte_budget"):
            bits.append(f"(reason {scalar(reason)})")
        if "requested_limit" in payload or "effective_limit" in payload:
            bits.append(
                f"(limit {scalar(payload.get('requested_limit'))} requested, "
                f"{scalar(payload.get('effective_limit'))} served)"
            )
        remedy = payload.get("truncation_remedy")
        cut_reads = _node_cut_reads(payload)
        if isinstance(remedy, str) and remedy:
            # Issue #3467: the caller's own select is why few rows fit.
            out.append(" ".join(bits) + f"; {remedy}")
        elif cut_reads:
            # Lane ri, tokens: a cut node read ends `next: none`, and agents
            # moved on from 90 of 98 such pages; this line names the calls
            # that read what was cut.
            out.append(" ".join(bits) + "; read what was cut with " + " or ".join(cut_reads))
        else:
            out.append(" ".join(bits))
    # PACT read-passage-rows-3666 (D8): the rerun advice for a bounded
    # retrieval (`coverage.retrieval.recovery`, policy option A of PACT
    # lexical-index-scan-cap-3162) is off by default in text; the JSON keeps
    # it.
    phrase_notice = _phrase_notice(payload)
    if phrase_notice is not None:
        out.append(phrase_notice)
    out.extend(_exact_match_lines(payload))
    passages = payload.get("passages_not_shown")
    if isinstance(passages, dict):
        # Issue #3467: a record read leaves its declared passages out.
        # Uncounted (a Nomos runtime), it claims no passage exists.
        count = passages.get("count")
        child = scalar(passages.get("child_subtype"))
        said = (f"{scalar(count)} {child} passages are not in this read"
                if count is not None else f"this read leaves out {child} passages, if any")
        read = passages.get("read")
        out.append(
            f"! {said}"
            + (f"; read them with {read}" if isinstance(read, str) and read else "")
        )
    elsewhere = payload.get("text_elsewhere")
    if isinstance(elsewhere, list):
        # Issue #3467: a scoped code record's content sits in a document,
        # which a windows call scoped to that document reads.
        for item in elsewhere:
            if isinstance(item, dict):
                out.append(
                    f"! the content of {safe_json(item.get('record_id'))} is in "
                    f"{safe_json(item.get('document_id'))}; search it with "
                    'scope={"records":['
                    f"{safe_json(item.get('document_id'))}]}}"
                )
    incoming = payload.get("incoming_not_shown")
    if isinstance(incoming, dict):
        # Issue #3467: an outgoing expand names the incoming links it left out.
        count = incoming.get("count")
        said = (f"{scalar(count)} incoming links not shown" if count is not None
                else "incoming links, if any, are not shown")
        out.append(f'! {said}; read them with direction="in"')
    # Issue #3666: cited records known only by key.
    from okg.substrate.mcp.link_targets import link_targets_line

    link_line = link_targets_line(payload.get("link_targets"))
    if link_line is not None:
        out.append(link_line)
    load_note = payload.get("load_note")
    if isinstance(load_note, str) and load_note:
        out.append(f"! {load_note}")
    # What a total over a kept live answer covers.
    cover = payload.get("totals_cover")
    if isinstance(cover, str) and cover:
        out.append(f"! totals cover: {cover}")
    # #3666, lane ri: a group key some rows lack, and a sum that added a
    # non-integer value, are said, never left for the reader to find.
    absent_keys = payload.get("absent_keys")
    if isinstance(absent_keys, dict) and absent_keys:
        from okg.substrate.mcp.live_result_sets import absent_sentence

        out.append(f"! {absent_sentence(absent_keys)}")
    not_exact = payload.get("sum_not_exact")
    if isinstance(not_exact, list) and not_exact:
        from okg.substrate.mcp.live_result_sets import inexact_sentence

        out.append(f"! {inexact_sentence([str(label) for label in not_exact])}")
    if "zero_hit_reason" in payload:
        out.append(
            f"! no hits: {scalar(payload.get('zero_hit_reason'))} "
            f"(scanned {scalar(payload.get('zero_hit_scanned'))}, "
            f"subtype indexed {scalar(payload.get('subtype_indexed'))})"
        )
    if payload.get("diversity_applied") and payload.get("suppressed_sibling_count"):
        # A bounded lexical page has no pre-suppression count (it is null,
        # PACT lexical-index-scan-cap-3162); say none rather than "null before".
        before = payload.get("total_count_before_diversity")
        out.append(
            f"! {scalar(payload['suppressed_sibling_count'])} sibling rows suppressed "
            f"(max {scalar(payload.get('max_per_parent'))} per parent"
            + (f"; {scalar(before)} before)" if before is not None else ")")
        )
    collapsed = payload.get("duplicates_collapsed")
    if isinstance(collapsed, int) and not isinstance(collapsed, bool) and collapsed > 0:
        # Issue #3465: exact copies left the list; each kept row names its own.
        out.append(
            f"! {scalar(collapsed)} exact duplicate {'row' if collapsed == 1 else 'rows'} collapsed "
            "(each kept row lists its copies)"
        )
    hit = payload.get("page_scan_bound_hit")
    if hit is not None:
        # grep (PACT mcp-grep-page): the scan stopped at a row bound.
        bound = payload.get("page_scan_bound")
        size = bound.get(hit) if isinstance(bound, dict) else None
        line = f"! bound reached ({key_text(hit)} {scalar(size)})"
        if payload.get("continuation_token"):
            # Format v10 item G2: the token continues only a grep call with
            # the same query and where (offset does not read on).
            line += (
                ': for the next records send continuation_token with method="grep" '
                "and the same query and where, or narrow the query"
            )
        elif payload.get("continuation_withheld") is not None:
            line += (
                f"; no continuation ({scalar(payload['continuation_withheld'])}): "
                "narrow the query"
            )
        elif payload.get("next_offset") is None:
            line += ": narrow the query"
        out.append(line)
    elif payload.get("continuation_withheld") is not None and not payload.get("continuation_token"):
        # A continuation page past the listed-record cap: no bound was hit.
        out.append(
            f"! no continuation ({scalar(payload['continuation_withheld'])}): "
            "narrow the query to see later records"
        )
    if _grep_timed_out(payload):
        # grep (PACT mcp-grep-page): the scan hit its statement timeout, not a
        # row bound and not a ranking cap, so no exact-order claim is made.
        ms = payload.get("statement_timeout_ms")
        out.append(
            "! scan timed out" + (f" ({scalar(ms)} ms)" if ms is not None else "")
            + ": narrow the query"
        )
    elif payload.get("next_offset_reason") not in (None, "page_reference") and (
        hit is None or payload.get("ordered_exactly_through") is not None
    ):
        through = payload.get("ordered_exactly_through")
        remedy = payload.get("next_offset_remedy")
        # A filled read of the expanded query ended the exact order: the
        # later records come back by rerunning without expansion, once per
        # expanded word in `rerun_each` (PACT lexical-index-scan-cap-3162).
        if isinstance(remedy, dict) and remedy:
            words = remedy.get("rerun_each")
            advice = (
                "rerun with " + ", ".join(
                    f"{key_text(key)}={safe_json(value)}"
                    for key, value in remedy.items() if key != "rerun_each"
                ) + (
                    f" once per word of {safe_json(words)} (separate rankings)"
                    if isinstance(words, list) and words
                    else " once per expanded word"
                ) + ", or narrow the query, to see later records"
            )
        else:
            advice = "narrow the query to see later records"
        out.append(
            f"! order exact through record {scalar(through)}; {advice}"
            if through is not None
            # Bounded lexical: the exact prefix holds only within retrieved
            # candidates, so no record number is claimed.
            else f"! order exact only within retrieved candidates; {advice}"
        )
    if payload.get("excerpt_window_basis") not in (None, "lexized") and _once(
        session_state, "excerpt_window_basis", payload.get("excerpt_window_basis"),
    ):
        out.append(
            "! excerpt windows by whole-word match (word bound reached)"
        )
    if payload.get("diversity_reason") is not None and _once(
        session_state, "diversity_reason", payload.get("diversity_reason"),
    ):
        out.append(
            "! profile max_per_parent not applied: one row per record"
        )
    expansion = payload.get("concept_expansion")
    if isinstance(expansion, dict) and expansion.get("applied"):
        out.append(f"! query expanded: {safe_json(expansion.get('expanded_query'))}")
    relation = payload.get("parent_relation")
    if isinstance(relation, dict):
        passage_count = relation.get("passage_count")
        passage_count = passage_count if isinstance(passage_count, dict) else {}
        if relation.get("edge_scan_cap_reached"):
            out.append(
                "! parent lookup stopped at its scan cap "
                f"({scalar(relation.get('edge_scan_cap'))} edges)"
            )
        if passage_count.get("withheld") or relation.get("passage_count_withheld"):
            out.append("! passage counts withheld")
    recency = payload.get("recency")
    if isinstance(recency, dict) and recency.get("annotated"):
        bits = [f"{_RECENCY_NOTICE} {scalar(recency.get('attribute'))}"]
        detail = [
            f"{key} {scalar(recency[key])}"
            for key in ("derivation", "basis") if recency.get(key) is not None
        ]
        if detail:
            bits.append("(" + ", ".join(detail) + ")")
        out.append(" ".join(bits))
    warning = payload.get("warning")
    if isinstance(warning, dict):
        out.append(f"! warning {scalar(warning.get('code'))}: {scalar(warning.get('message'))}")
    elif warning is not None:
        out.append(f"! warning: {scalar(warning)}")
    if "schema_behind_by" in payload:
        out.append(f"! schema behind by {scalar(payload['schema_behind_by'])}")
    if payload.get("pin_scope") not in (None, "transport_session") and _once(
        session_state, "pin_scope", payload.get("pin_scope"),
    ):
        out.append(f"! pin scope: {scalar(payload['pin_scope'])}")
    out += _location_notices(payload)
    out += _line_notices(payload)
    return out


#: Every value a text read's `lines.basis` takes (#3666): the numbers are the
#: source text's own, or lines of the text the read shows. Defined here, the
#: module every reader of a result imports, and nowhere else.
LINES_OF_SOURCE = "source"
LINES_OF_SHOWN_TEXT = "shown_text"
LINE_BASES = (LINES_OF_SOURCE, LINES_OF_SHOWN_TEXT)
#: Every value a search match's `line_basis` takes when its line is not a
#: line of the record's text: lines inside the passage, or lines of the
#: record's own `text` attribute.
MATCH_LINE_IN_PASSAGE = "passage"
MATCH_LINE_IN_OWN_TEXT = "own_text"
MATCH_LINE_BASES = (MATCH_LINE_IN_PASSAGE, MATCH_LINE_IN_OWN_TEXT)


def _whole(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _line_block(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    """A text read's `lines` block (#3666) when it has the shape the status
    line and notices say whole; any other value prints as an other field."""
    block = payload.get("lines")
    if not isinstance(block, dict) or not _whole(block.get("total")):
        return None
    if block.get("basis") not in LINE_BASES:
        return None
    for key in ("requested", "served"):
        if key not in block:
            continue
        # An asked range may be open: `[first, null]`.
        open_end = key == "requested"
        if not isinstance(block[key], list) or not all(
            isinstance(pair, list) and len(pair) == 2 and _whole(pair[0])
            and (_whole(pair[1]) or (open_end and pair[1] is None))
            for pair in block[key]
        ):
            return None
    if any(key in block and not _whole(block[key]) for key in ("first", "last")):
        return None
    if block.get("past_end", True) is not True:
        return None
    cut = block.get("cut")
    if cut is not None and not (
        isinstance(cut, dict) and set(cut) == {"last_line", "next_line"}
        and all(_whole(n) for n in cut.values())
    ):
        return None
    known = {"basis", "total", "requested", "served", "first", "last", "past_end", "cut"}
    return block if set(block) <= known else None


def _line_fact(block: dict[str, Any]) -> Optional[str]:
    """`source lines 1278-1302, 1555-1578 of 2961`: whose lines they are,
    the lines this page holds, and the last line of the text. The numbers
    print as `lines` takes them, with no digit grouping."""
    whose = "source" if block["basis"] == LINES_OF_SOURCE else "shown-text"
    if "served" in block:
        held = ", ".join(f"{first}-{last}" for first, last in block["served"]) or "none"
    elif "first" in block and "last" in block:
        held = f"{block['first']}-{block['last']}"
    else:
        return None
    return f"{whose} lines {held} of {block['total']}"


def _not_read_calls(payload: dict[str, Any], first: int, total: int) -> list[str]:
    """The calls that read a cut text page's lines `first`-`total`: the
    page's own `next`, and the lines read of the record, each printed as it
    is sent (lane ri, tokens). Empty when neither can be printed safely."""
    ways: list[str] = []
    token = payload.get("continuation_token")
    if isinstance(token, str) and token:
        call = _call_text({"tool": "inspect", "arguments": {"next": token}})
        if call is not None:
            ways.append(f"read on with {call}")
    node_id = payload.get("node_id")
    if isinstance(node_id, str) and node_id:
        call = _call_text({"tool": "inspect", "arguments": {"target": {
            "kind": "text", "node_id": node_id, "lines": [[first, total]],
        }}})
        if call is not None:
            ways.append(f"read those lines with {call}")
    return ways


def _line_notices(payload: dict[str, Any]) -> list[str]:
    """What a reader must know before trusting a line number (#3666): that
    the numbers are not the source's, that a range ran past the text's end,
    and where a size cut stopped. A text page and a batch item say them the
    same way."""
    block = _line_block(payload)
    if block is None:
        return []
    out: list[str] = []
    if block["basis"] == LINES_OF_SHOWN_TEXT:
        # Why is the page's own coverage (a batch item's `source_coverage`).
        gaps = "partial" in (payload.get("coverage"), payload.get("source_coverage"))
        out.append(
            "! lines: of the shown text, "
            + ("source has gaps" if gaps else "not the source's own")
        )
    if block.get("past_end"):
        out.append(f"! lines: the text ends at line {block['total']}")
    last, total = block.get("last"), block.get("total")
    if (
        "served" not in block and isinstance(last, int) and isinstance(total, int)
        and last < total and payload.get("truncated") is True
    ):
        # A page read cut short (in one eval ~80% of cut reads were never
        # continued): say what was not read and the calls that read it.
        # Lane ri, tokens: with "`next` reads on" alone, 10 of 50 such notes
        # in one eval run were followed; the calls are now printed whole.
        ways = _not_read_calls(payload, last + 1, total)
        out.append(
            f"! not read: lines {last + 1}-{total}; " + (
                ", or ".join(ways) if ways
                else "`next` reads on, or target lines/matching read chosen parts"
            )
        )
    cut = block.get("cut")
    if cut is not None:
        out.append(
            f"! lines: served through line {cut['last_line']}; "
            f"next continues at line {cut['next_line']}"
        )
    return out


def _location_notices(payload: dict[str, Any]) -> list[str]:
    """The notices of a text read at a location (PACT read-model-s5-location).

    A single text page and each batch item say them the same way.
    """
    out: list[str] = []
    read_from = payload.get("read_from")
    if _read_from_normal(read_from):
        out.append(
            f"! read starts at {ident_text(read_from['chunk'])} @{read_from['offset']}; "
            "earlier text was not sent"
        )
    if payload.get("location_check") == "offset_only":
        out.append(
            "! location checked by offset only (no hash): changed chunk text is not detected"
        )
    return out


def _read_from_normal(value: Any) -> bool:
    """A `read_from` the notice says whole (so it needs no other-fields line)."""
    return (
        isinstance(value, dict)
        and set(value) == {"chunk", "offset", "text_before_sent"}
        and isinstance(value["chunk"], str)
        and not is_block(value["chunk"])
        and isinstance(value["offset"], int)
        and not isinstance(value["offset"], bool)
        and value["text_before_sent"] is False
    )


def _trust_fact(
    parts: _Parts, row: dict[str, Any], page: dict[str, Any], *, has_text: bool,
) -> None:
    provenance = row.get("provenance", row.get("read_provenance"))
    if isinstance(provenance, dict):
        trusted = provenance.get("trust") == "trusted"
        if not trusted:
            parts.facts.append(f"trust={scalar(provenance.get('trust') or 'unlabeled')}")
        # The trust label of a trusted row is its normal state and prints
        # nothing; its source is data a reader cites, so it always prints
        # (review on 2102d20798: a trusted row otherwise lost its source).
        if not trusted or provenance.get("source") is not None:
            parts.fact("source", provenance.get("source"), label="source")
        for key, value in provenance.items():
            if key not in ("trust", "source"):
                parts.fact(key_text(key), value)
    elif has_text and page.get("row_trust") != "trusted":
        parts.facts.append("trust=unlabeled")


# ── placed fields: printed in place, summarized, or under other fields ────

#: Top-level fields the grammar places. Each one prints in its designed
#: place, is summarized there (#3076 DESIGN 1.2, "What is placed but not
#: printed"), is in its normal state, or prints under `--- other fields`
#: (:func:`_placed_leftovers`). No placed field is ever dropped while it is
#: not in its normal state (review B1 on 2102d20798).
_PLACED_TOP = frozenset({
    "status", "error", "operator_name", "response_shape", "pinned_generation_id",
    "generation_id", "session_default_generation_id", "total_count", "next_offset",
    "truncated", "offset", "continuation_token", "source_freshness", "publish_freshness",
    "warning", "schema_behind_by", "pin_scope", "pagination", "pagination_hint",
    "rank_capped", "literal_read_bounded_ms", "method", "diversity_applied", "max_per_parent",
    "suppressed_sibling_count", "total_count_before_diversity", "duplicates_collapsed", "recency",
    "parent_relation", "concept_expansion", "parent_summaries", "row_defaults",
    "total_count_basis", "coverage", "complete", "truncation_reason", "response_byte_budget",
    "requested_limit", "effective_limit", "zero_hit_reason", "zero_hit_scanned",
    "subtype_indexed", "answer", "row_trust", "ranked_cap", "results", "neighbors",
    "result_unit", "ordered_exactly_through", "next_offset_reason", "next_offset_remedy",
    "excerpt_window_basis", "diversity_applicable", "diversity_reason", "rank_by",
    "max_per_parent_origin", "page_scan_bound_hit", "continuation_withheld", "completeness",
    "where_applied", "read_from", "location_check", "view", "max_chars_lowered",
    "max_bytes_raised", "dates_read_as_utc", "date_forms_compared", "date_forms_reason",
    "total_count_reason", "date_forms_remedy",
    "adjusted", "matched_loosely", "phrase", "text_match", "sorted_as_text",
    "compared_as_stored",
    "result_reference", "search_next", "exact_matches", "incoming_not_shown",
    "passages_not_shown", "truncation_remedy", "text_elsewhere", "resolved_path",
    "metadata_cut", "sample_nodes_omitted", "link_targets", "load_state", "load_note",
    "totals_cover", "names_only", "absent_keys", "sum_not_exact",
    "items", "groups", "rows", "columns", "mentions", "not_a_total", "value_hint",
    "where_checked_on",
})

_ROW_KEYS = ("results", "neighbors", "items", "groups", "rows")

#: Keys of `recency` that its notice says, or that each row repeats.
_RECENCY_SHOWN = frozenset({
    "annotated", "attribute", "derivation", "basis", "rows_on_page", "rows_annotated",
})
#: Keys of `parent_relation` that its notices say or that each search card
#: applies (the parent's subtype, edge type and relation source).
_RELATION_SHOWN = frozenset({
    "source", "child_subtypes", "edge_scan_cap", "edge_scan_cap_reached",
    "passage_count", "passage_count_withheld", "rows_on_page",
    "rows_with_declared_relation", "rows_resolved", "catalog_version_id",
})


class _Shown:
    """What the status line, notices and body said, for the leftover pass."""

    def __init__(self) -> None:
        self.keys: set[str] = set()
        self.paging = False
        #: The counts fact printed the total (so `N+` could say a lower bound).
        self.total = False
        #: The counts fact printed a row span, which says the offset.
        self.offset = False
        self.rows_key: Optional[str] = None
        self.summaries_used: set[Any] = set()
        self.columns = False


def _placed_leftovers(
    payload: dict[str, Any], *, tool: str, kind: Optional[str], shown: _Shown,
    generation: Any, selected: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Placed top-level fields that are not normal and were not printed.

    The rule per field: its normal state prints nothing; a field the
    status line, a notice or the body printed (or summarized, per #3076
    DESIGN 1.2) is done; anything else goes under `--- other fields`.
    """

    out: dict[str, Any] = {}
    graph = tool == "inspect" and kind == "graph"
    for key, value in payload.items():
        if key not in _PLACED_TOP or key in shown.keys:
            continue
        keep: Any = value
        cut = bool(payload.get("truncation_reason") or "response_byte_budget" in payload)
        if key in (
            # Always said elsewhere or always normal: the status word, the
            # format version, the row-trust default the tags apply, the
            # ranking cap (search-result-shape prints it), the schema and
            # pin-scope notices, and diversity's switch and settings, which
            # the suppression notice says when it suppressed anything.
            "status", "response_shape", "row_trust", "ranked_cap", "schema_behind_by",
            "pin_scope", "diversity_applied", "max_per_parent",
            "total_count_before_diversity",
            # Said by its notice when it removed a row; 0 and null are
            # normal (null: the page states no count).
            "duplicates_collapsed",
        ):
            continue
        if key == "operator_name":
            if value in (None, tool):
                continue
        elif key in ("pinned_generation_id", "generation_id", "session_default_generation_id"):
            if value in (None, generation):
                continue
        elif key in ("error", "answer", "method"):
            if value is None:
                continue
        elif key in ("zero_hit_reason", "zero_hit_scanned", "subtype_indexed"):
            if value is None or "zero_hit_reason" in payload:
                continue
        elif key == "phrase":
            # `matched` is normal; `no_exact_match` is said by its notice.
            if value is None or (
                isinstance(value, dict)
                and value.get("status") in ("matched", "no_exact_match")
            ):
                continue
        elif key in (
            "incoming_not_shown", "passages_not_shown", "text_elsewhere", "resolved_path",
            "metadata_cut", "sample_nodes_omitted", "link_targets",
        ):
            # Said by its notice line (issue #3467; a resolved path, #3666).
            # `link_targets` with nothing not loaded is its normal state.
            if value is None or isinstance(value, (dict, list)):
                continue
        elif key in ("load_note", "totals_cover"):
            # Said by its notice line (issue #3666).
            if value is None or isinstance(value, str):
                continue
        elif key in ("absent_keys", "sum_not_exact"):
            # Said by its notice line (#3666, lane ri).
            if value is None or isinstance(value, (dict, list)):
                continue
        elif key == "names_only":
            # Said by its notice line whenever it renders (lane ri, q020).
            if value is None or _names_only_line(payload) is not None:
                continue
        elif key == "load_state":
            # "not_loaded" is said by the load_note notice (issue #3666);
            # any other state prints under other fields.
            if value is None or value == "not_loaded":
                continue
        elif key == "truncation_remedy":
            # Said on the page-cut notice, which prints whenever it is set.
            if value is None or isinstance(value, str):
                continue
        elif key == "exact_matches":
            # Said span by span by its notice lines (PACT
            # lexical-exact-identifier-hits); the JSON carries the block.
            if value is None or isinstance(value, dict):
                continue
        elif key in ("truncation_reason", "response_byte_budget", "requested_limit",
                     "effective_limit"):
            if value is None or cut:
                continue
        elif key == "total_count":
            if value is None or shown.total:
                continue
        elif key == "offset":
            if not _int(value) or shown.offset:
                continue
        elif key == "next_offset":
            if value is None or shown.paging:
                continue
        elif key == "continuation_token":
            if not value or (shown.paging and payload.get("next_offset") is None):
                continue
        elif key == "truncated":
            if not value:
                continue
        elif key == "rank_capped":
            if not value or (shown.total and _lower_bound(payload)):
                continue
        elif key == "total_count_basis":
            if value in (None, "exact") or (value == "lower_bound" and shown.total):
                continue
        elif key == "complete":
            if value is not False:
                continue
        elif key in ("pagination", "pagination_hint"):
            if value is None or shown.paging:
                continue
        elif key == "source_freshness":
            # A stale or unavailable block is the notice; a fresh one is
            # normal. inspect(graph) always carries the whole block (N-f).
            if not graph and isinstance(value, dict):
                continue
        elif key == "publish_freshness":
            if not graph and isinstance(value, dict):
                continue
        elif key == "warning":
            if not isinstance(value, dict):
                continue
            keep = {k: v for k, v in value.items() if k not in ("code", "message")}
            if not keep:
                continue
        elif key == "suppressed_sibling_count":
            if not value or payload.get("diversity_applied"):
                continue
        # search-result-shape's envelope fields: each prints as a status
        # fact or a notice when it is not normal (see `render` and
        # `_notices`); a value the notice does not name stays here.
        elif key == "result_unit":
            if value in (None, "record"):
                continue
        elif key == "rank_by":
            default_rank = "records" if payload.get("method") == "lexical" else "self"
            if value in (None, default_rank):
                continue
        elif key == "next_offset_reason":
            if value in (None, "beyond_exact_order"):
                continue
            if value == "scan_bound_reached" and payload.get("page_scan_bound_hit"):
                # The bound notice says it.
                continue
        elif key == "not_a_total":
            # The count fact and the `! not a total` notice say it.
            if _not_a_total(payload) is not None:
                continue
        elif key == "value_hint":
            # The `did you mean` notice says it.
            if _value_hint(payload) is not None:
                continue
        elif key == "page_scan_bound_hit":
            # The bound notice says it.
            if value is not None:
                continue
        elif key == "continuation_withheld":
            # The bound notice says it when no token was given.
            if value is None or not payload.get("continuation_token"):
                continue
        elif key == "next_offset_remedy":
            # The exact-order notice says it whenever the reason is set.
            if value is None or payload.get("next_offset_reason") is not None:
                continue
        elif key == "ordered_exactly_through":
            # Normal on every folded page; the notice says it when the next
            # page would start past the exact prefix.
            continue
        elif key == "excerpt_window_basis":
            if value in (None, "lexized", "whole_word_fallback"):
                continue
        elif key == "diversity_reason":
            if value in (None, "records_returns_one_row_per_parent"):
                continue
        elif key == "diversity_applicable":
            if value is not False or payload.get("diversity_reason") is not None:
                continue
        elif key == "where_applied":
            # No predicate applied: nothing to say.
            if value is None:
                continue
        elif key == "max_per_parent_origin":
            # Where the bound came from; the rank mode and the diversity
            # notices already follow from it.
            if value in (None, "argument", "profile"):
                continue
        elif key == "read_from":
            # The notice says it whole; any other shape stays here.
            if _read_from_normal(value):
                continue
        elif key == "location_check":
            if value == "offset_only":
                continue
        elif key == "sorted_as_text":
            # Its notice says it whole.
            if isinstance(value, str):
                continue
        elif key == "compared_as_stored":
            # Its notice says it whole.
            if isinstance(value, list) and value and all(isinstance(n, str) for n in value):
                continue
        elif key in ("max_chars_lowered", "max_bytes_raised", "dates_read_as_utc"):
            # The lowered-max_chars, raised-max_bytes and read-as-UTC notices
            # say it whole.
            if value is None or isinstance(value, dict):
                continue
        elif key == "date_forms_compared":
            # Its notice says the one value it has, when it knows why; any
            # other prints here.
            if value is None or (
                value == "iso_8601_only"
                and payload.get("date_forms_reason") in _DATE_FORMS_NOTICES
            ):
                continue
        elif key == "date_forms_remedy":
            if value is None or (
                value in _DATE_FORMS_REMEDIES
                and payload.get("date_forms_reason") in _DATE_FORMS_NOTICES
            ):
                continue
        elif key == "total_count_reason":
            # Its notice says the one value it has; any other prints here.
            if value in (None, "count_not_computed_in_time"):
                continue
        elif key == "date_forms_reason":
            if value is None or (
                value in _DATE_FORMS_NOTICES
                and payload.get("date_forms_compared") == "iso_8601_only"
            ):
                continue
        elif key == "adjusted":
            # The adjusted notice says it whole.
            if value is None or _adjusted_notice(payload) is not None:
                continue
        elif key == "matched_loosely":
            # The matched-loosely notice says it whole (loose-match-3666).
            if value is None or _matched_loosely_notices(payload) is not None:
                continue
        elif key == "text_match":
            # The text-condition notice says what was read; the value and
            # the cap are the caller's own and the JSON form keeps them.
            if value is None or _text_match_notice(payload) is not None:
                continue
        elif key == "concept_expansion":
            if isinstance(value, dict):
                if not value.get("applied"):
                    continue
                keep = {k: v for k, v in value.items() if k not in ("applied", "expanded_query")}
                if not keep:
                    continue
            elif value is None:
                continue
        elif key == "recency":
            if isinstance(value, dict):
                if not value.get("annotated"):
                    continue
                keep = {k: v for k, v in value.items() if k not in _RECENCY_SHOWN}
                if not keep:
                    continue
            elif value is None:
                continue
        elif key == "parent_relation":
            if value is None:
                continue
            if tool == "search" and isinstance(value, dict):
                keep = {k: v for k, v in value.items() if k not in _RELATION_SHOWN}
                if not keep:
                    continue
        elif key == "parent_summaries":
            if isinstance(value, dict):
                keep = {k: v for k, v in value.items() if k not in shown.summaries_used}
            if not keep:
                continue
        elif key == "row_defaults":
            if isinstance(value, dict) and tool == "search":
                # A hoisted select-only field leaves the default view as it
                # does on each row (PACT read-model-s2-handle).
                keep = {
                    k: v for k, v in value.items()
                    if k not in SELECT_ONLY_ROW_FIELDS or k in selected
                }
            if not keep:
                continue
        elif key in _ROW_KEYS:
            if key == shown.rows_key or value in (None, []):
                continue
        elif key == "columns":
            if shown.columns or value in (None, []):
                continue
        elif key == "mentions":
            if value in (None, payload.get("results")):
                continue
        out[key] = keep
    return out


# ── shapes ────────────────────────────────────────────────────────────────

def _copies_fact(row: dict[str, Any], page: dict[str, Any]) -> Optional[str]:
    """A `records` row's exact copies (issue #3465): ``N identical copies:
    <id>, ...``, or ``N identical copies (first 10 listed): ...`` when the
    row lists only the first ids and `copies_total` counts them all. Found
    among the rows the ranked reads retrieved; only an exhaustive retrieval
    makes them every copy, as for matching passages."""
    copies = row.get("copies")
    if not isinstance(copies, list) or not copies:
        return None
    total = row.get("copies_total")
    capped = isinstance(total, int) and not isinstance(total, bool) and total > len(copies)
    count = total if capped else len(copies)
    whole = _retrieval(page).get("status") == "exhaustive"
    noun = "identical copy" if count == 1 else "identical copies"
    return (
        f"{count} {noun}"
        + ("" if whole else " among retrieved candidates")
        + (f" (first {len(copies)} listed)" if capped else "")
        + ": " + ", ".join(part_text(copy) for copy in copies)
    )

def _window_record_card(index: int, row: dict[str, Any]) -> list[str]:
    """One record of `search(view="windows")` (PACT read-record-windows).

    ``[n] <id> (<type>)`` and the record's facts, then each window as a
    ``--- window`` line followed by its ``--- match``, ``--- location`` and
    ``--- text`` / ``--- separator`` lines. Every key the JSON carries
    prints as a fact; a fragment's literal text follows its line as an
    escaped body, so source text cannot forge a window, match or card line.
    """
    head = f"[{index}] {ident_text(row.get('node_id'))} ({ident_text(row.get('subtype'))})"
    parts = _Parts()
    for key, value in row.items():
        if key not in ("node_id", "subtype", "windows", "record_id"):
            parts.fact(key_text(key), value)
    if row.get("record_id") not in (None, row.get("node_id")):
        parts.fact("record_id", row["record_id"])
    lines = [SEP.join([head, *parts.facts]), *parts.blocks]
    windows = row.get("windows")
    if not isinstance(windows, list):
        if windows is not None:
            lines += _block_lines("windows", windows)
        return lines
    for window in windows:
        if not isinstance(window, dict):
            lines += _block_lines("window", window)
            continue
        wparts = _Parts()
        for key, value in window.items():
            if key not in ("window_id", "matches", "locations", "fragments"):
                wparts.fact(key_text(key), value)
        lines.append(SEP.join([f"--- window {scalar(window.get('window_id'))}", *wparts.facts]))
        lines += wparts.blocks
        for kind, label in (("matches", "match"), ("locations", "location")):
            for item in window.get(kind) or []:
                iparts = _Parts()
                if isinstance(item, dict):
                    for key, value in item.items():
                        iparts.fact(key_text(key), value)
                else:
                    iparts.fact("value", item)
                lines.append(SEP.join([f"--- {label}", *iparts.facts]))
                lines += iparts.blocks
        for fragment in window.get("fragments") or []:
            if not isinstance(fragment, dict):
                lines += _block_lines("fragment", fragment)
                continue
            fparts = _Parts()
            for key, value in fragment.items():
                if key not in ("text", "kind"):
                    fparts.fact(key_text(key), value)
            text = fragment.get("text")
            if not isinstance(text, str):
                fparts.fact("text", text)
            label = "separator" if fragment.get("kind") == "separator" else "text"
            if fragment.get("kind") not in (None, "chunk", "separator"):
                fparts.fact("kind", fragment["kind"])
            if isinstance(text, str):
                fparts.fact("chars", len(text))
            lines.append(SEP.join([f"--- {label}", *fparts.facts]))
            lines += fparts.blocks
            if isinstance(text, str) and text:
                lines.append(_escape_window_body(text))
    return lines


#: A body line that reads like a match-window line (`line 5, chars 1-2...`),
#: matched on the same cleaned, casefolded copies `structural` uses.
_WINDOW_LINE = re.compile(r"line\s*\S+\s*,\s*chars(\s|$)")


def _window_like(line: str) -> bool:
    return any(
        _WINDOW_LINE.match(_matching_copy(line, cf_as)) is not None
        for cf_as in ("", " ")
    )


def _escape_window_body(text: str) -> str:
    """`escape_body`, plus one leading backslash on a source line that reads
    like a window line -- after leading whitespace, invisible and format
    characters, lookalikes and case are cleaned as `structural` cleans them --
    so a window's text cannot forge another window."""
    out = []
    for piece in escape_body(text).splitlines(keepends=True) or [""]:
        if not piece.startswith("\\") and _window_like(piece.rstrip(LINE_BREAKS)):
            piece = "\\" + piece
        out.append(piece)
    return "".join(out)


#: Search row fields the DEFAULT text view leaves out (PACT
#: read-model-s2-handle; design `read-model` section 5): the content hash,
#: the source revision, the evidence-kind basis, the raw score or distance,
#: and the producer record block (only when the row's handle names a path:
#: otherwise the record is often the only file name, so it stays). Each
#: prints, as before, when the call's `select` names it. The JSON result
#: keeps every one (decision D9).
SELECT_ONLY_ROW_FIELDS = frozenset({
    "text_hash", "source_revision", "evidence_kind_provenance", "score",
    "distance", "source_record_id",
})


def _selected_names(select: Any) -> frozenset[str]:
    if isinstance(select, (list, tuple, set, frozenset)):
        return frozenset(str(item) for item in select)
    return frozenset()


def _fenced_inner(value: str) -> Optional[str]:
    """The text inside one well-formed fence, or None.

    A fence is an opener line ``⟦UNTRUSTED_DATA source=...⟧``, the escaped
    text, and a closer line (``provenance._fence_value``). Anything else,
    including a marker inside the text, is not unwrapped.
    """
    lines = value.split("\n")
    if (
        len(lines) < 3
        or not lines[0].startswith(FENCE + " source=")
        or not lines[0].endswith("⟧")
        or lines[-1] != FENCE_CLOSE
    ):
        return None
    inner = "\n".join(lines[1:-1])
    if FENCE in inner or "⟦/UNTRUSTED_DATA" in inner:
        return None
    return inner


def _neutralise_markers(text: str) -> str:
    """``text`` with every fence marker sequence escaped, as
    ``provenance.escape_embedded_fence_markers`` escapes producer text."""
    return text.replace("⟦/UNTRUSTED_DATA", "⟦ESCAPED_/UNTRUSTED_DATA").replace(
        FENCE, "⟦ESCAPED_UNTRUSTED_DATA",
    )


def _handle_value(parts: _Parts, name: str, value: Any, *, quote: bool, inline: bool = False) -> Optional[str]:
    """One handle field as an inline fact, or None after printing a block.

    A string is a JSON string (``quote``), so a title can never read as
    another fact such as ``source <name>``; a date prints bare. A fenced
    value prints its text as a JSON string between the opening and closing
    markers on the one line (:data:`INLINE_FENCE_OPEN`); a well-formed fence
    too long for a header line prints as a block. Any other string, from any
    source, has its marker sequences neutralised first
    (:func:`_neutralise_markers`), so a title can never open or close a fence.
    """
    if isinstance(value, str) and FENCE in value:
        inner = _fenced_inner(value)
        if inner is not None and len(inner) > LONG and not inline:
            parts.block(name, value)
            return None
        if inner is not None:
            return f"{INLINE_FENCE_OPEN}{safe_json(inner)}{FENCE_CLOSE}"
    if isinstance(value, str):
        value = _neutralise_markers(value)
        if len(value) > LONG and not inline:
            parts.block(name, value)
            return None
        return safe_json(value) if quote else scalar(value)
    return safe_json(value)


#: A day-precision date as the server writes it: that day's midnight, with
#: the source's offset when it gave one (the XML Schema date form).
_MIDNIGHT_DAY = re.compile(r"^(\d{4}-\d{2}-\d{2})T00:00:00((?:[+-]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?)$")


def _handle_day(value: Any, precision: Any) -> Any:
    """A day-precision handle date as the day it names, else `value` unchanged.

    The server writes a calendar day as that day's midnight so it sorts with
    datetimes; it is not an instant at midnight. The text view prints the day
    (``2026-10-03``, or ``2026-10-03-07:00`` for a day in a zone) so a reader
    cannot take it for a clock time. A value of any other shape, a datetime
    and a legacy handle without a precision print as before.
    """
    if precision != "date" or not isinstance(value, str):
        return value
    match = _MIDNIGHT_DAY.match(value)
    return value if match is None else match[1] + match[2]


def _handle_facts(parts: _Parts, handle: Any, *, inline: bool = False) -> list[str]:
    """``<title> · <path> · <date>`` from a row's handle (design 3.6).

    Each field is optional and prints only when the row has it; a record
    with none of the three prints nothing here. A field the read filter hid
    prints as ``<field> masked`` in its place, so hiding never reads as
    absence. A date marked ``date_precision: "date"`` prints as the day it
    names. Any other key of the handle prints as a ``handle.<key>=`` fact.
    """
    if not isinstance(handle, dict):
        parts.fact(HANDLE_KEY, handle)
        return []
    masked = handle.get(HANDLE_MASKED_KEY)
    masked_names = (
        {item for item in masked if isinstance(item, str)}
        if isinstance(masked, list) else set()
    )
    facts: list[str] = []
    date_meaning = handle.get("date_meaning")
    date_attr = handle.get("date_attr")
    labeled_date = (
        date_meaning in ("created", "modified")
        and isinstance(date_attr, str)
        and (handle.get("date") is not None or "date" in masked_names)
    )
    for key in HANDLE_FIELDS:
        value = handle.get(key)
        date_source = None
        if key == "date" and labeled_date:
            date_source = _handle_value(
                parts, "handle.date_attr", _neutralise_markers(date_attr), quote=True, inline=inline,
            )
        if value is None:
            if key in masked_names:
                label = f"{date_meaning} date masked" if key == "date" and labeled_date else f"{key} masked"
                facts.append(label + (f" (attr={date_source})" if date_source is not None else ""))
            continue
        if key == "date":
            value = _handle_day(value, handle.get(HANDLE_DATE_PRECISION_KEY))
        text = _handle_value(parts, key, value, quote=key != "date", inline=inline)
        if text is not None:
            if key == "date" and labeled_date:
                text = f"{date_meaning} {text}" + (f" (attr={date_source})" if date_source is not None else "")
            facts.append(text)
    for key, value in handle.items():
        if key in HANDLE_FIELDS:
            continue
        if labeled_date and key in {"date_meaning", "date_attr"}:
            continue
        if key == HANDLE_DATE_PRECISION_KEY and value == "date":
            continue
        if key == HANDLE_MASKED_KEY and isinstance(value, list) and all(
            item in HANDLE_FIELDS for item in value
        ):
            continue
        parts.fact(f"{HANDLE_KEY}.{key_text(key)}", value)
    return facts

def _location_fact(parts: _Parts, location: Any) -> Optional[str]:
    """``@ <chunk>:<offset>`` from a row's location (design 3.6, D10).

    Returns the chunk id it printed, or None. The hash is not printed: it
    is for the next call (`inspect` `at`), which reads it from the JSON. A
    location of any other shape prints as a plain ``location=`` fact."""
    if (
        isinstance(location, dict)
        and isinstance(location.get("chunk"), str)
        and isinstance(location.get("offset"), int)
        and not isinstance(location.get("offset"), bool)
        and not is_block(location["chunk"])
    ):
        parts.facts.append(f"@ {ident_text(location['chunk'])}:{location['offset']}")
        return location["chunk"]
    parts.fact(LOCATION_KEY, location)
    return None


def _parent_value(value: Any) -> str:
    """A value in a parent fact: never holds a bare ``, `` or parenthesis."""
    text = part_text(value)
    if isinstance(value, str) and text == value and ("(" in value or ")" in value):
        return safe_json(value)
    return text


class _LineParts(_Parts):
    """:class:`_Parts` that never leaves its line (PACT read-model-s4-list).

    A value that would print as a trailing block prints inline instead, as a
    JSON string (fenced data keeps its inline fence), so a list row is one
    physical line whatever its source text holds and nothing is dropped.
    """

    def block(self, name: str, value: Any) -> None:
        if isinstance(value, str) and FENCE in value:
            inner = _fenced_inner(value)
            if inner is not None:
                self.facts.append(f"{name} {INLINE_FENCE_OPEN}{safe_json(inner)}{FENCE_CLOSE}")
                return
        if isinstance(value, str):
            value = _neutralise_markers(value)
        self.facts.append(f"{name}={safe_json(value)}")


#: Row keys the list line places itself; any other key prints as a fact.
_LIST_ROW_PLACED = frozenset({
    "node_id", "subtype", HANDLE_KEY, "provenance", "read_provenance", "evidence_kind",
    "unit", "copies", "copies_total", "parent", "matches", "first_match", CITATION_KEY,
})
#: ``search_views.LIST_ROW_UNIT_KEY``, restated: result_text imports no
#: operator module.
LIST_ROW_UNIT_KEY = "unit"


#: What a list page's notice says when every row's line counts the same text
#: (PACT read-names-list-3666): said once, not on each row.
_SHARED_LINE_BASIS_NOTICE = {
    MATCH_LINE_IN_PASSAGE: "! lines: each counted in its passage",
    MATCH_LINE_IN_OWN_TEXT: "! lines: each counted in the record's own text",
}


def _shared_line_basis(rows: list[Any]) -> Optional[str]:
    """The one `line_basis` every list row with a first match carries, when
    there are at least two such rows and the basis has a notice; else None."""
    bases = [
        row["first_match"].get("line_basis") for row in rows
        if isinstance(row, dict) and isinstance(row.get("first_match"), dict)
    ]
    if len(bases) < 2 or len(set(bases)) != 1 or bases[0] not in _SHARED_LINE_BASIS_NOTICE:
        return None
    return bases[0]


def _plain_text(value: Any) -> Optional[str]:
    """A string's text without its inline fence, or None."""
    if not isinstance(value, str):
        return None
    inner = _fenced_inner(value) if FENCE in value else value
    return inner if isinstance(inner, str) else None


def _restated_in_handle(text: Any, handle: Any) -> Optional[str]:
    """``"title"`` or ``"path"`` when a list row's matched line says nothing
    its handle does not (the line, ellipses cut, is inside that field,
    ignoring case), else None. Fenced and unfenced values compare by text;
    a fenced line is only said to restate a field of the same row."""
    line = _plain_text(text)
    if not line or not isinstance(handle, dict):
        return None
    core = line.strip().removeprefix("...").removesuffix("...").strip().casefold()
    if not core:
        return None
    for name in ("title", "path"):
        field = _plain_text(handle.get(name))
        if field and core in field.casefold():
            return name
    return None


def _search_list_line(
    index: int, row: dict[str, Any], page: dict[str, Any],
    *, shared_basis: Optional[str] = None,
) -> str:
    """One record of ``view="list"`` as exactly one physical line.

    ``[n] <id> (<type>) · <title> · <path> · <date>`` from the handle, as on a
    card, then the same evidence-kind and trust/source facts a card prints.
    Every string is quoted or fenced inline, so a title holding a newline or
    a row marker cannot start a new row or add a fact.
    """
    parts = _LineParts()
    head = f"[{index}] {ident_text(row.get('node_id'))} ({ident_text(row.get('subtype'))})"
    if row.get(LIST_ROW_UNIT_KEY) == "passage":
        # A passage the records fold kept as its own row, never a record.
        parts.facts.append("passage")
    # PACT read-names-list-3666: the id a reader cites, then title and path.
    parts.facts += _citation_facts(parts, row.get(CITATION_KEY))
    if HANDLE_KEY in row:
        parts.facts += _handle_facts(parts, row[HANDLE_KEY], inline=True)
    if "matches" in row:
        # A grep list row (#3659): how many of the record's text units
        # match (`N+` when the page stopped before reading them all), then
        # where the first match is and its line, quoted inline.
        count = scalar(row["matches"])
        more = "+" if page.get("matches_basis") == "scanned" else ""
        parts.facts.append(f"{count}{more} match" + ("" if row["matches"] == 1 and not more else "es"))
    # Outside the count: a `select` may keep `first_match` without `matches`.
    first = row.get("first_match")
    restated = (
        _restated_in_handle(first.get("text"), row.get(HANDLE_KEY))
        if isinstance(first, dict) else None
    )
    if restated is not None:
        # PACT read-names-list-3666: the matched line is the title (or the
        # path) the row already printed: said once, without its line, which
        # the JSON form keeps.
        parts.facts.append(f"match in {restated}")
    elif isinstance(first, dict):
        where = f"line {scalar(first.get('line'))}"
        if first.get("chunk") != row.get("node_id"):
            where = f"{token_text(first.get('chunk'))} {where}"
        own_text = (
            first.get("line_basis") == MATCH_LINE_IN_OWN_TEXT
            and shared_basis != MATCH_LINE_IN_OWN_TEXT
        )
        # PACT read-names-list-3666: the record's own `text` attribute,
        # which `inspect` text does not read, named in two words.
        parts.facts.append(f"first own-text {where}" if own_text else f"first {where}")
        if first.get("text") is not None:
            parts.block("text", first["text"])
        if own_text or (shared_basis is not None and first.get("line_basis") == shared_basis):
            # Said beside the line, or once in the page's notice
            # (`_SHARED_LINE_BASIS_NOTICE`).
            pass
        elif first.get("line_basis") == MATCH_LINE_IN_PASSAGE:
            # #3666: the line could not be placed in the record's text.
            parts.facts.append("line counted in the passage")
        elif first.get("line_basis") is not None:
            parts.fact("line_basis", first["line_basis"])
    parent = row.get("parent")
    if isinstance(parent, dict) and parent.get("node_id"):
        # Issue #3467: which record a listed passage belongs to.
        parts.facts.append(f"in {part_text(parent['node_id'])}")
    if row.get("evidence_kind") not in (None, "unknown"):
        parts.fact("evidence_kind", row["evidence_kind"])
    copies = _copies_fact(row, page)
    if copies is not None:
        parts.facts.append(copies)
    # Handle titles and paths are source text, so an unlabeled row says so.
    _trust_fact(parts, row, page, has_text=True)
    for key, value in row.items():
        if key in SELECT_ONLY_ROW_FIELDS or key in _LIST_ROW_PLACED:
            continue
        parts.fact(key_text(key), value)
    return SEP.join([head, *parts.facts])


#: The row list each shape prints as rows; another tool takes the first.
_TOOL_ROWS_KEY = {
    "search": "results", "expand": "neighbors", "filter": "items", "map": "items",
    "aggregate": "groups", "query": "rows", "inspect:text": "items",
}


def _rows_key(payload: dict[str, Any], tool: str = "") -> Optional[str]:
    preferred = _TOOL_ROWS_KEY.get(tool)
    if preferred is not None and isinstance(payload.get(preferred), list):
        return preferred
    for key in _ROW_KEYS:
        if isinstance(payload.get(key), list):
            return key
    return None


def _cell(value: Any) -> str:
    """A table cell or bare row value: typed, and never readable as structure.

    Group keys and query cells are raw source values (review B2 on
    58c3678f40), and a bare string such as ``[2] ticket:FAKE (ticket)`` or
    ``--- other fields`` would start a line as structure. Such a string is
    printed as a JSON string instead, in every column. A non-string cell
    that reads as structure (a list ``[1]``) is handled per line by
    :func:`_table`.
    """
    text = _inline(value)
    if isinstance(value, str) and text == value and structural(text):
        return safe_json(value)
    return text


def _table_line(cells: list[str]) -> str:
    """One table line, escaped by the body rule when it reads as structure
    (review on 2102d20798: a list cell ``[1]`` followed by a tab)."""

    line = "\t".join(cells)
    return "\\" + line if structural(line) else line


#: A missing key in a dictionary row prints as an empty cell; ``null`` and
#: ``""`` print as themselves, so the three stay apart.
_MISSING = object()


def _table(columns: list[Any], rows: list[list[Any]]) -> Optional[list[str]]:
    if any(has_fence(cell) for row in rows for cell in row):
        return None
    out = [_table_line([_cell(key_text(column)) for column in columns])]
    for row in rows:
        out.append(_table_line(["" if cell is _MISSING else _cell(cell) for cell in row]))
    return out


def _table_rows(
    tool: str, rows: list[Any], payload: dict[str, Any], start: int, shown: _Shown,
) -> list[str]:
    """``aggregate`` and ``query`` rows: a typed table, or rows when it can't be.

    The columns are the payload's declared ``columns`` first, then any other
    row key in first-seen order, so no row field is dropped and an empty
    result still names its columns. A missing key prints as an empty cell.
    A table with any fenced cell, or rows that are neither dictionaries nor
    lists, fall back to ``[n]`` rows.
    """

    declared = payload.get("columns") if isinstance(payload.get("columns"), list) else None
    if not rows:
        if declared:
            shown.columns = True
            return [_table_line([_cell(key_text(column)) for column in declared])]
        return []
    if all(isinstance(row, (list, tuple)) for row in rows):
        width = max(len(row) for row in rows)
        if declared is not None and len(declared) == width:
            columns = list(declared)
            shown.columns = True
        else:
            columns = [f"col{index + 1}" for index in range(width)]
        cells = [list(row) + [_MISSING] * (width - len(row)) for row in rows]
    elif all(isinstance(row, dict) for row in rows):
        columns = list(declared) if declared is not None else []
        if declared is not None:
            shown.columns = True
        for row in rows:
            for key in row:
                if key not in columns:
                    columns.append(key)
        cells = [[row.get(column, _MISSING) for column in columns] for row in rows]
    else:
        return _generic_rows(rows, payload, start, id_keys=())
    table = _table(columns, cells)
    if table is None:
        return _generic_rows(
            [
                {str(c): v for c, v in zip(columns, row) if v is not _MISSING}
                for row in cells
            ],
            payload, start, id_keys=(),
        )
    return table


def _generic_rows(
    rows: list[Any],
    page: dict[str, Any],
    start: int,
    id_keys: tuple[str, ...] = ("node_id", "edge_id"),
    session_state: Optional[dict[str, Any]] = None,
    call: Optional[str] = None,
) -> list[str]:
    """Rows as a head line and key lines. With a session (`filter` rows,
    PACT read-reply-size-3666), a row whose exact lines this session
    already printed, at the same generation, prints ``shown earlier
    (...)`` in their place; a row whose lines are shorter than that note
    prints them. A node read printed whole never stands for a filter row
    (review M2): the row may carry fields the read did not print."""
    lines: list[str] = []
    for index, row in enumerate(rows, start):
        if not isinstance(row, dict):
            lines.append(f"[{index}] {_cell(row)}")
            continue
        parts = _Parts()
        identity = [ident_text(row[key]) for key in id_keys if key in row]
        kind = next(
            (ident_text(row[key]) for key in ("subtype", "edge_type") if key in row), None,
        )
        head = f"[{index}] " + " ".join(identity) + (f" ({kind})" if kind else "")
        _trust_fact(parts, row, page, has_text="attrs" in row)
        skip = {*id_keys, "subtype", "edge_type", "provenance", "read_provenance"}
        rest = {k: v for k, v in row.items() if k not in skip and k != "attrs"}
        if isinstance(row.get("attrs"), dict):
            rest.update(row["attrs"])
        elif "attrs" in row:
            rest["attrs"] = row["attrs"]
        key_lines, key_blocks = _key_lines(rest, indent="  ")
        lines.append(SEP.join([head.rstrip(), *parts.facts]))
        body = parts.blocks + key_lines + key_blocks
        ident = row.get("node_id")
        if session_state is not None and body and isinstance(ident, str):
            key = _passage_key("rows", body)
            entry = _served_record(session_state, ident)
            row_call = entry["keys"].get(key) if entry is not None else None
            before = (row_call, False) if isinstance(row_call, str) else None
            if before is not None:
                said = "  " + _shown_line(before).lstrip()
                if len("\n".join(body)) > len(said):
                    body = [said]
            else:
                _mark_served(session_state, ident, call, key)
        lines += body
    return lines


def _linked_record_lines(index: int, row: dict[str, Any], page: dict[str, Any]) -> list[str]:
    """A 1-hop `expand` row that is a linked record (issue #3666).

    The head is the record, as a `filter` row prints one, then its title.
    The facts say which link reached it (`via <type> →` going out, `←`
    coming in), that link's id, and how many links reach it when more than
    one. Anything else the link carries (`src`, `dst`, its own `attrs`)
    prints as one `link` fact; the record's attributes are key lines.
    """
    link = row["link"]
    parts = _Parts()
    head = f"[{index}] {ident_text(row.get('node_id'))} ({ident_text(row.get('subtype'))})"
    title = row.get("title")
    block_title = None
    if title is not None and is_block(title):
        block_title = title
    elif title is not None:
        head += " " + safe_json(title)
    arrow = "←" if link.get("direction") == "in" else "→"
    parts.facts.append(f"via {scalar(link.get('edge_type'))} {arrow}")
    parts.facts.append(f"edge {scalar(link.get('edge_id'))}")
    count = row.get("link_count")
    if isinstance(count, int) and not isinstance(count, bool) and count > 1:
        parts.facts.append(f"{count} links")
    if row.get("load_state") == "not_loaded":
        # Issue #3666: the graph holds no record at this id, only its key.
        parts.facts.append("not loaded (known by key)")
    if block_title is not None:
        parts.block("title", block_title)
    _trust_fact(parts, row, page, has_text=bool(row.get("attrs")))
    more = {
        key: value for key, value in link.items()
        if key not in ("edge_type", "direction", "edge_id")
    }
    if more:
        parts.fact("link", more)
    for key, value in row.items():
        if key not in (
            "node_id", "subtype", "title", "link", "link_count", "attrs",
            "provenance", "read_provenance",
        ) and not (key == "load_state" and value == "not_loaded"):
            parts.fact(key_text(key), value)
    attrs = row.get("attrs")
    if attrs is not None and not isinstance(attrs, dict):
        parts.fact("attrs", attrs)
    key_lines, key_blocks = _key_lines(
        attrs if isinstance(attrs, dict) else {}, indent="  ",
    )
    return [SEP.join([head, *parts.facts]), *parts.blocks, *key_lines, *key_blocks]


def _error_lines(
    payload: dict[str, Any], tool: str, *, raised: bool,
    session_state: Optional[dict[str, Any]] = None,
) -> str:
    raw_error = payload.get("error")
    error = raw_error if isinstance(raw_error, dict) else {}
    head_word = f"{tool} {_status_word(payload, raised=raised)}"
    code = error.get("code")
    head = [head_word + (f" {token_text(code)}" if code is not None else "")]
    # The refusal ending (PACT read-model-s6-page-next): the change that
    # makes a retry work. The error block below keeps every field, the
    # `fix_hint` it repeats included. No generation (D8, below).
    retry, _retry_key = _retry(error)
    head.append(f"retry: {retry}")
    generation = _generation_of(payload)
    lines = [SEP.join(head)]
    adjusted = _adjusted_notice(payload)
    if adjusted is not None:
        lines.append(adjusted)
    body: dict[str, Any] = {}
    if "answer" in payload:
        body["answer"] = payload["answer"]
    if raw_error is not None and not isinstance(raw_error, dict):
        # A string (or other non-object) error: its text is the message.
        body["error"] = raw_error
    order = (
        "message", "fix_hint", "remedy", "fallback_method", "fallback_operator",
        "allowed_operators", "example", "schema_hint",
    )
    for key in order:
        if key in error:
            body[key] = error[key]
    for key, value in error.items():
        if key == "max_bytes" and tool == "search":
            # PACT read-passage-rows-3666 (gate 1): search does not publish
            # max_bytes, so its refusal text never names it; the JSON keeps
            # the reply budget.
            continue
        if key not in order and key not in ("code", "data"):
            body[key] = value
    if "data" in error:
        body["data"] = error["data"]
    for key in ("warning", "schema_behind_by"):
        if key in payload:
            body[key] = payload[key]
    key_lines, key_blocks = _key_lines(body)
    lines += key_lines + key_blocks
    rest = {
        k: v for k, v in payload.items()
        if k not in (
            "status", "error", "answer", "warning", "schema_behind_by",
        )
        and not (k == "operator_name" and v == tool)
        and not (k == "pinned_generation_id" and v == generation)
        # #3076 slice A: session bookkeeping in its normal state.
        and not (k == "pin_scope" and v == "transport_session")
        and not (k == "session_default_generation_id" and v == generation)
        and not (k == "adjusted" and adjusted is not None)
    }
    if rest:
        rest_lines, rest_blocks = _key_lines(rest)
        lines += ["--- other fields", *rest_lines, *rest_blocks]
    return "\n".join(lines)


def _text_segment_lines(rows: list[Any], payload: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict):
            lines += ["", f"[{index}] {_cell(row)}"]
            continue
        parts = _Parts()
        head = f"[{index}] {scalar(row.get('chunk_id'))} @{scalar(row.get('char_offset'))}"
        if row.get("heading_path") is not None:
            if is_block(row["heading_path"]):
                parts.block("heading", row["heading_path"])
            else:
                parts.facts.append(f"§ {scalar(row['heading_path'])}")
        _trust_fact(parts, row, payload, has_text=True)
        text = row.get("text")
        held = row.get("lines")
        lines_said = (
            isinstance(held, list) and len(held) == 2 and all(_whole(n) for n in held)
        )
        if lines_said:
            # #3666: the lines of the text this piece holds.
            parts.facts.append(f"lines {held[0]}-{held[1]}")
        for key, value in row.items():
            if key == "lines" and lines_said:
                continue
            if key not in ("chunk_id", "char_offset", "heading_path", "provenance", "text"):
                parts.fact(key_text(key), value)
        if not isinstance(text, str):
            # A segment whose text is not a string (null, an object)
            # prints that value, typed, instead of an empty body.
            parts.fact("text", text)
        lines += ["", SEP.join([head, *parts.facts]), *parts.blocks]
        if isinstance(text, str):
            if parts.blocks:
                lines.append("--- text")
            lines.append(escape_body(text))
    return lines


def _render_text_batch(payload: dict[str, Any], session_state: Any) -> str:
    facts = ["batch text", f"{len(payload.get('items', []))} request items", *_ending(payload)]
    lines = [SEP.join(["inspect ok", *facts]), *_notices(payload, session_state)]
    for item in payload.get("items", []):
        lines += ["", SEP.join([safe_json(item["key"]), token_text(item.get("node_id")),
                                 token_text(item.get("state"))])]
        lines += _location_notices(item)
        lines += _line_notices(item)
        for name in ("requested_extent", "source_coverage", "order_basis", "error", "outcome",
                     "read_from", "location_check", "lines"):
            if name == "read_from" and _read_from_normal(item.get(name)):
                continue
            if name == "location_check" and item.get(name) == "offset_only":
                continue
            if name == "lines" and _line_block(item) is not None:
                # #3666: whose lines a line item's numbers are, and the last
                # line; each piece below names the lines it holds.
                whose = (
                    "source" if item["lines"]["basis"] == LINES_OF_SOURCE else "shown-text"
                )
                lines.append(f"lines: {whose} lines, {item['lines']['total']} in all")
                continue
            if name in item:
                values, blocks = _key_lines({name: item[name]})
                lines += values + blocks
        lines += _text_segment_lines(item.get("segments", []), item)
    for name in ("coverage", "work"):
        if name in payload:
            values, blocks = _key_lines({name: payload[name]})
            lines += values + blocks
    return "\n".join(lines)


def _render_node_batch(payload: dict[str, Any], session_state: Any, select: Any) -> str:
    """inspect nodes (#3657): one status line for the page, then each record
    as inspect(node) prints it, headed by its id, subtype and status."""
    records = payload.get("records", [])
    facts = ["batch nodes", f"{len(records)} of {scalar(payload.get('request_count'))} records"]
    if payload.get("cut_count"):
        # Said before the ending, so a page of shortened records never
        # reads as plainly complete.
        facts.append(f"{scalar(payload['cut_count'])} cut")
    facts += _ending(payload)
    lines = [SEP.join(["inspect ok", *facts]), *_notices(payload, session_state)]
    for record in records:
        status = record.get("status")
        head = ident_text(record.get("node_id"))
        if record.get("subtype") is not None:
            head += f" ({ident_text(record.get('subtype'))})"
        lines += ["", SEP.join([head, token_text(status)])]
        if status == "not_visible":
            continue
        body = {k: v for k, v in record.items() if k != "status"}
        rendered = _render(body, tool="inspect", kind="node", select=select).split("\n")
        lines += rendered[1:]
        if (
            isinstance(body.get("attrs"), dict) and not _selected_names(
                select if select is not None else current_result_context().select,
            )
            and not any(body.get(key) is not None for key in NODE_CUT_KEYS)
        ):
            # PACT read-reply-size-3666: printed whole, as inspect(node).
            _mark_served(
                session_state, body.get("node_id"),
                _session_call(session_state, "inspect", payload), "", whole=_WHOLE_NODE,
            )
    for name in ("omitted", "remaining_count"):
        if name in payload:
            values, blocks = _key_lines({name: payload[name]})
            lines += values + blocks
    return "\n".join(lines)


def infer_inspect_kind(payload: dict[str, Any]) -> Optional[str]:
    """The inspect shape of a payload when the call's target kind is unknown."""

    if isinstance(payload.get("items"), list) and "order_basis" in payload:
        return "text"
    if "attrs" in payload and "node_id" in payload:
        return "node"
    return None


#: Top-level fields the text form never prints (#3076 slice A): echoes of
#: the call's own arguments, constants, and session bookkeeping that
#: `inspect(target={"kind":"session"})` reports. The JSON form keeps them.
_TEXT_UNSAID_ALL = frozenset({"logical_session_id"})
_TEXT_UNSAID_BY_TOOL: dict[str, frozenset[str]] = {
    # `search_profile` stays: it names the profile applied, the default included.
    "search": frozenset({"query", "max_chars"}),
    "filter": frozenset({"input", "predicate", "projection", "query_plan_summary", "elapsed_ms"}),
    "map": frozenset({"input", "predicate", "projection", "query_plan_summary", "elapsed_ms"}),
    "query": frozenset({"elapsed_ms"}),
}
#: inspect(node): the stored text's hash; `select` returns it in JSON.
_TEXT_UNSAID_NODE = frozenset({"text_hash"})


def _without_unsaid(payload: dict[str, Any], tool: str, kind: Optional[str]) -> dict[str, Any]:
    if tool == "inspect" and kind == "session":
        # inspect(session) is where the session's own ids are read.
        return payload
    unsaid = _TEXT_UNSAID_ALL | _TEXT_UNSAID_BY_TOOL.get(tool, frozenset())
    if tool == "inspect" and kind == "node":
        unsaid |= _TEXT_UNSAID_NODE
    if not unsaid & payload.keys():
        return payload
    return {k: v for k, v in payload.items() if k not in unsaid}


# ── passage rows (#3666, PACT read-passage-rows-3666) ─────────────────────
#
# A search row in text is a head line, the matched passage and the exact call
# that reads more (design: read-output-design DESIGN.md). The JSON result keeps
# every field; the text prints only these. A row without the `head` and
# `passage` keys (another deployment, an older payload) prints a compact head
# from its handle and its excerpt, never an attribute dump.

#: Head field names whose value prints bare: the value says what it is.
_BARE_HEAD_FIELDS = frozenset({
    "status", "key", "id", "sha", "repo", "path", "location", "author",
    "qualified_name", "name", "lines", "subject", "document", "section", "date",
})
#: Indent of every line a row prints under its head.
_ROW_INDENT = "    "
#: A passage line that reads like a line the row itself prints is escaped
#: with one leading backslash, as :func:`escape_body` escapes structure.
#: Only the lines a row prints at its own indent, and only at exactly that
#: indent (`_escape_row_line`): an ordinary source line such as
#: `description: ...` or a deeper-indented `comments: ...` forges nothing
#: and prints as written (#3666; a stray backslash printed before them).
_ROW_LINE = re.compile(
    r"(more:|source:|shown (whole )?(in call|earlier)\b|match in\b|match:|L\d+(-\d+)?:|"
    r"attrs\.)",
    re.IGNORECASE,
)
#: The most served-record entries a session keeps for "shown earlier".
_SERVED_CAP = 4096


#: Characters of the earlier call's query a repeat row names.
_SHOWN_QUERY_CHARS = 40


def _session_call(
    session_state: Optional[dict[str, Any]], tool: str, payload: Any,
) -> Optional[str]:
    """How a later reply names this one when it repeats a record this reply
    printed: its tool and, for a search, its query cut to
    `_SHOWN_QUERY_CHARS` and quoted (``search "keep_miniaod T2"``). Not a
    number: the agent's call list also holds other servers' tools, so an okg
    call count named the wrong call on 10% of repeats (X2, #3666). None
    without a session (a measurement, the shell)."""
    if session_state is None:
        return None
    query = payload.get("query") if isinstance(payload, dict) else None
    if tool == "search" and isinstance(query, str) and query.strip():
        cut = query.strip()
        if len(cut) > _SHOWN_QUERY_CHARS:
            cut = cut[:_SHOWN_QUERY_CHARS - 1].rstrip() + "…"
        return f"search {safe_json(cut)}"
    return tool


def _served(session_state: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if session_state is None:
        return None
    served = session_state.get("served")
    if not isinstance(served, dict):
        served = {}
        session_state["served"] = served
    return served


#: The session-state key counting the entries `served` holds over all its
#: records (`_SERVED_CAP` bounds it).
_SERVED_COUNT = "served_count"
#: How a record was shown whole: by a read of its text (every passage of it
#: is shown), or of its node (its attributes: only a row read as a node).
_WHOLE_TEXT = "text"
_WHOLE_NODE = "node"
#: The session-state key holding the generation of the reply being rendered
#: (`_render` sets it). A record's memory is kept with the generation it was
#: printed at, and a reply read at another generation ignores it (review M2
#: of PACT read-reply-size-3666): a value that changed in a later publish is
#: printed again, never hidden behind `shown earlier`.
_READING_GENERATION = "served_generation"


def _served_record(
    session_state: Optional[dict[str, Any]], node_id: Any,
) -> Optional[dict[str, Any]]:
    """This session's memory of one record, or None: ``{"gen": generation,
    "whole": {how: call}, "keys": {passage or line key: call}}`` (PACT
    read-reply-size-3666: every passage, not only the last one). None when
    it was printed at another generation than the reply being rendered."""
    served = _served(session_state)
    if served is None or not isinstance(node_id, str):
        return None
    assert session_state is not None
    entry = served.get(node_id)
    if (
        isinstance(entry, dict) and isinstance(entry.get("whole"), dict)
        and isinstance(entry.get("keys"), dict)
        and entry.get("gen") == session_state.get(_READING_GENERATION)
    ):
        return entry
    return None


def _whole_before(
    session_state: Optional[dict[str, Any]], node_id: Any, read_kind: Optional[str],
) -> Optional[tuple[str, bool]]:
    """``(call, True)`` when this session printed the record whole: its
    text, or its node when the row reads as a node (`read_kind`)."""
    entry = _served_record(session_state, node_id)
    if entry is None:
        return None
    whole = entry["whole"]
    if isinstance(whole.get(_WHOLE_TEXT), str):
        return whole[_WHOLE_TEXT], True
    if read_kind == _WHOLE_NODE and isinstance(whole.get(_WHOLE_NODE), str):
        return whole[_WHOLE_NODE], True
    return None


def _shown_before(
    session_state: Optional[dict[str, Any]], node_id: Any, key: str,
    read_kind: Optional[str] = None,
) -> Optional[tuple[str, bool]]:
    """``(call, whole)`` when this session already printed this record's
    text: whole (an inspect of the record), or this same passage, among
    every passage it printed of the record. ``call`` names the earlier call
    (:func:`_session_call`). None otherwise, and always None without a
    session."""
    whole = _whole_before(session_state, node_id, read_kind)
    if whole is not None:
        return whole
    entry = _served_record(session_state, node_id)
    call = entry["keys"].get(key) if entry is not None else None
    return (call, False) if isinstance(call, str) else None


def _shown_line(before: tuple[str, bool]) -> str:
    """``shown earlier (search "q")`` or ``shown whole earlier (inspect)``."""
    call, whole = before
    return f"{_ROW_INDENT}shown {'whole ' if whole else ''}earlier ({call})"


def _mark_served(
    session_state: Optional[dict[str, Any]], node_id: Any, call: Optional[str],
    key: str, *, whole: Optional[str] = None,
) -> None:
    """Remember that `call` printed this record's passage `key`, or the
    record whole (`whole`: `_WHOLE_TEXT` or `_WHOLE_NODE`). The memory keeps
    at most `_SERVED_CAP` entries over all records; the record printed
    longest ago is forgotten first (gate 3: bounded as before, when it held
    one entry per record)."""
    served = _served(session_state)
    if served is None or call is None or not isinstance(node_id, str):
        return
    assert session_state is not None
    entry = _served_record(session_state, node_id)
    count = session_state.get(_SERVED_COUNT)
    count = count if isinstance(count, int) and not isinstance(count, bool) else 0
    if entry is None:
        old = served.pop(node_id, None)
        if isinstance(old, dict):
            # Printed at another generation: forgotten, and its entries
            # leave the count.
            count -= sum(
                len(part) for part in (old.get("whole"), old.get("keys"))
                if isinstance(part, dict)
            )
        elif old is not None:
            count -= 1
        entry = {"gen": session_state.get(_READING_GENERATION), "whole": {}, "keys": {}}
    else:
        served.pop(node_id)
    held = entry["whole"] if whole is not None else entry["keys"]
    name = whole if whole is not None else key
    if name not in held:
        count += 1
    else:
        held.pop(name)
    held[name] = call
    served[node_id] = entry
    while count > _SERVED_CAP and served:
        oldest = next(iter(served))
        gone = served.pop(oldest)
        if isinstance(gone, dict):
            count -= sum(
                len(part) for part in (gone.get("whole"), gone.get("keys"))
                if isinstance(part, dict)
            )
        else:
            count -= 1
    session_state[_SERVED_COUNT] = max(count, 0)


def _passage_key(*parts: Any) -> str:
    return hashlib.sha256(safe_json(list(parts)).encode("utf-8")).hexdigest()[:16]


def _escape_row_line(line: str) -> str:
    """One source line printed under a row: escaped when it reads like
    result structure or like a line the row prints at its indent."""
    if structural(line):
        return "\\" + line
    if (
        line.startswith(_ROW_INDENT)
        and line[len(_ROW_INDENT):len(_ROW_INDENT) + 1] not in ("", " ", "\t")
        and _ROW_LINE.match(line, len(_ROW_INDENT))
    ):
        return "\\" + line
    return line


def _indented_text(text: str, indent: str = _ROW_INDENT) -> list[str]:
    """Source text as indented lines: blank runs folded to one blank line,
    leading and trailing blank lines dropped, each line escaped."""
    out: list[str] = []
    for raw in text.splitlines():
        line = raw.rstrip()
        if not line.strip():
            if out and out[-1]:
                out.append("")
            continue
        if line == FENCE_CLOSE or (
            line.startswith(FENCE + " source=") and line.endswith("⟧")
        ):
            # An untrusted-data fence line stays at the start of its line,
            # where a reader finds it; producer text cannot hold one (its
            # marker sequences are escaped where it is fenced).
            out.append(line)
            continue
        # The escape goes before the indent, as `escape_body` escapes an
        # indented line: a reader strips one leading backslash per line.
        out.append(_escape_row_line(indent + line))
    while out and not out[-1]:
        out.pop()
    return out


def _who(value: Any) -> str:
    """An author or date on a row line: bare when plain, a fenced author
    (an untrusted row's) as its inline fence, never a raw line break."""
    text = _handle_value(_LineParts(), "author", value, quote=False, inline=True)
    return text if text is not None else safe_json(value)


def _one_line(text: Any) -> str:
    """A match line's text on one physical line, whitespace folded."""
    value = text if isinstance(text, str) else safe_json(text)
    return " ".join(value.split())


def _head_text(parts: _Parts, name: str, value: Any, *, quote: bool) -> Optional[str]:
    if value is None or value == "" or value == []:
        return None
    if isinstance(value, (dict, list)):
        return safe_json(value)
    return _handle_value(parts, name, value, quote=quote, inline=True)


def _citation_facts(parts: _Parts, value: Any) -> list[str]:
    """``<attr> <value>`` for a row's or read's `citation_id` (PACT
    read-names-list-3666): the id a reader cites, beside the record's id."""
    if value is None:
        return []
    if not isinstance(value, dict) or len(value) != 1:
        parts.fact(CITATION_KEY, value)
        return []
    ((name, cited),) = value.items()
    text = _head_text(parts, str(name), cited, quote=False)
    return [] if text is None else [f"{key_text(str(name))} {text}"]


def _read_citation_facts(payload: dict[str, Any], shown: Any) -> list[str]:
    """An inspect node or text read's citation id as status-line facts; a
    value that cannot print on that line is left for the other-fields block."""
    cited = _LineParts()
    citation = _citation_facts(cited, payload.get(CITATION_KEY))
    if not citation or cited.facts:
        return []
    shown.keys.add(CITATION_KEY)
    return citation


def _head_fields(parts: _Parts, fields: Any) -> list[str]:
    """``[[name, value], ...]`` (or a mapping) as head facts, in order."""
    pairs: list[tuple[Any, Any]] = []
    if isinstance(fields, dict):
        pairs = list(fields.items())
    elif isinstance(fields, list):
        for item in fields:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                pairs.append((item[0], item[1]))
    facts: list[str] = []
    for name, value in pairs:
        text = _head_text(parts, str(name), value, quote=False)
        if text is None:
            continue
        name = str(name)
        if name in ("heading", "heading_path"):
            facts.append(f"§ {text}")
        elif name in _BARE_HEAD_FIELDS:
            facts.append(text)
        else:
            facts.append(f"{key_text(name)} {text}")
    return facts


def _call_text(call: Any) -> Optional[str]:
    """``tool(name=<json>, ...)``: a call to send as written, or None."""
    if not isinstance(call, dict):
        return None
    tool, arguments = call.get("tool"), call.get("arguments")
    if not isinstance(tool, str) or not isinstance(arguments, dict):
        return None
    if not all(isinstance(name, str) and name.isidentifier() for name in (tool, *arguments)):
        return None
    if has_fence(arguments):
        return None
    text = f"{tool}(" + ", ".join(
        f"{name}={safe_json(value)}" for name, value in arguments.items()
    ) + ")"
    if SEP in text or any(_unsafe(c) for c in text) or len(text) > 1000:
        return None
    return text


def call_text(call: Any) -> Optional[str]:
    """A `{"tool", "arguments"}` call as a reader sends it, or None when it
    cannot be printed safely (the public name of :func:`_call_text`)."""
    return _call_text(call)


def _shared_row_source(page: dict[str, Any]) -> Optional[str]:
    """The one source name every row of a search page shares, when there
    are two rows or more and the name is plain text; else None. The reply
    then names it once on its status line, not on every row (#3666)."""
    rows_key = _rows_key(page, "search")
    rows = page.get(rows_key) if rows_key else None
    if not isinstance(rows, list) or len(rows) < 2:
        return None
    shared: Optional[str] = None
    for row in rows:
        provenance = row.get("provenance", row.get("read_provenance")) if isinstance(row, dict) else None
        source = provenance.get("source") if isinstance(provenance, dict) else None
        if not isinstance(source, str) or not source or FENCE in source:
            return None
        if shared is None:
            shared = source
        elif source != shared:
            return None
    return shared


def _row_trust(parts: _Parts, row: dict[str, Any], page: dict[str, Any]) -> None:
    """``trust=<label>`` for a row whose text is not trusted, then the
    row's ``source <name>``, unless every row on the page shares it: the
    status line then names it once (#3666). The JSON form keeps each row's
    `provenance.source`."""
    provenance = row.get("provenance", row.get("read_provenance"))
    if isinstance(provenance, dict):
        if provenance.get("trust") != "trusted":
            parts.facts.append(f"trust={scalar(provenance.get('trust') or 'unlabeled')}")
        if provenance.get("source") is not None and _shared_row_source(page) is None:
            parts.fact("source", provenance.get("source"), label="source")
    elif page.get("row_trust") != "trusted":
        parts.facts.append("trust=unlabeled")


def _passage_head(index: int, row: dict[str, Any], page: dict[str, Any]) -> tuple[list[str], Any]:
    """The head line (and any block it needs), and the record id."""
    parts = _LineParts()
    head = row.get("head")
    if isinstance(head, dict):
        ident = head.get("id", row.get("node_id"))
        kind = head.get("kind", row.get("subtype"))
        line = f"[{index}] {ident_text(ident)}"
        if kind is not None:
            line += f" ({ident_text(kind)})"
        # A list row that is a passage, not a record (`unit`), says so first
        # (PACT read-model-s4-list), as a list line without a head did.
        facts = ["passage"] if row.get("unit") == "passage" else []
        facts += _citation_facts(parts, row.get(CITATION_KEY))
        facts += _head_fields(parts, head.get("fields"))
        title = _head_text(parts, "title", head.get("title"), quote=True)
        values = head.get("fields") if isinstance(head.get("fields"), list) else []
        repeated = any(
            isinstance(item, (list, tuple)) and len(item) == 2 and item[1] == head.get("title")
            for item in values
        )
        if title is not None and not repeated:
            # A code row's title is its qualified name, already a field.
            facts.append(title)
        handle = row.get(HANDLE_KEY)
        masked = handle.get(HANDLE_MASKED_KEY) if isinstance(handle, dict) else None
        if isinstance(masked, list) and masked:
            # A field the read filter hid is said as `<field> masked`, as
            # the handle says it, so hiding never reads as absence (#3666).
            facts += _handle_facts(parts, {
                key: handle[key] for key in (HANDLE_MASKED_KEY, "date_meaning", "date_attr")
                if key in handle
            }, inline=True)
    else:
        ident = row.get("node_id")
        line = f"[{index}] {ident_text(ident)}"
        if row.get("subtype") is not None:
            line += f" ({ident_text(row.get('subtype'))})"
        facts = _citation_facts(parts, row.get(CITATION_KEY))
        if isinstance(row.get(HANDLE_KEY), dict):
            facts += _handle_facts(parts, row[HANDLE_KEY], inline=True)
        parent = row.get("parent")
        if isinstance(parent, dict) and parent.get("node_id"):
            facts.append(f"in {part_text(parent['node_id'])}")
    if row.get("phrase_match") is False:
        # A quoted lexical search: this record does not hold the phrase.
        facts.append("no exact phrase")
    count = row.get("match_count")
    if isinstance(count, int) and not isinstance(count, bool):
        # A grep within="record" row: every match over the record's units
        # (PACT read-search-caps-3666), on the head line, so a sweep of head
        # lines still says how much each record holds.
        facts.append(f"{count:,} {'match' if count == 1 else 'matches'}")
    copies = _copies_fact(row, page)
    if copies is not None:
        facts.append(copies)
    _row_trust(parts, row, page)
    return [SEP.join([line, *facts, *parts.facts]), *parts.blocks], ident


def _passage_body(passage: Any) -> tuple[list[str], Optional[str]]:
    """A passage's lines and its identity key, or ``([], None)``."""
    if not isinstance(passage, dict):
        return [], None
    text = passage.get("text")
    if not isinstance(text, str) or not text.strip():
        return [], None
    lines: list[str] = []
    where = passage.get("where")
    if where == "comment":
        who = " ".join(
            _who(value) for value in (passage.get("author"), passage.get("date"))
            if isinstance(value, str) and value
        )
        lines.append(_ROW_INDENT + "match in comment" + (f", {who}" if who else "") + ":")
    start, end = passage.get("start_line"), passage.get("end_line")
    if where in ("lines", "signature") and _whole(start) and _whole(end):
        lines.append(_ROW_INDENT + (f"L{start}-{end}:" if start != end else f"L{start}:"))
    lines += _indented_text(text)
    return lines, _passage_key(passage.get("chunk_id"), where, text)


def _match_line_body(matches: Any) -> tuple[list[str], Optional[str]]:
    """A grep row's matches, one rg -n style line each (#3666)."""
    if not isinstance(matches, list) or not matches:
        return [], None
    lines: list[str] = []
    for match in matches:
        if not isinstance(match, dict):
            lines.append(_ROW_INDENT + "match: " + _one_line(match))
            continue
        line, field = match.get("line"), match.get("field")
        if match.get("author") or field == "comment":
            who = " ".join(
                _who(value) for value in (match.get("author"), match.get("date"))
                if isinstance(value, str) and value
            )
            label = "comment" + (f", {who}" if who else "")
        elif _whole(line):
            label = f"L{line}"
        elif isinstance(field, str) and field:
            label = key_text(field)
        else:
            label = "match"
        unit = match.get("unit")
        if isinstance(unit, str) and unit:
            # A grep within="record" line names the unit it is in
            # (`text`, or a passage's type and its stated place).
            place = match.get("unit_place")
            name = key_text(unit) + (f" {place}" if _whole(place) else "")
            label = f"{name} {label}" if _whole(line) else name
        lines.append(
            f"{_ROW_INDENT}{label}: {_one_line(match.get('text'))}{_cut_note(match)}"
        )
    return lines, _passage_key(matches)


def _chars_left(match: Any) -> int:
    """The characters of a grep comment or prose line not shown."""
    left = match.get("chars_left") if isinstance(match, dict) else None
    return left if isinstance(left, int) and not isinstance(left, bool) and left > 0 else 0


def _cut_note(match: Any) -> str:
    """`` [cut, N chars left]`` after a grep comment or prose line that
    does not show its whole comment or paragraph (#3666, lane RD)."""
    left = _chars_left(match)
    return f" [cut, {left:,} chars left]" if left else ""


def _read_whole_line(row: dict[str, Any], page: dict[str, Any]) -> Optional[str]:
    """``read whole: inspect(...)``, the call that reads a grep row's
    record whole, for a row with a cut comment or prose line (#3666, lane
    RD): the row's ``read_more``, or the page ``read_more`` listing its id
    when the row fields were left out to fit (D4). None when neither names
    it."""
    lines = row.get("match_lines")
    if not isinstance(lines, list) or not any(_chars_left(line) for line in lines):
        return None
    target = None
    call = row.get("read_more")
    arguments = call.get("arguments") if isinstance(call, dict) else None
    if isinstance(call, dict) and call.get("tool") == "inspect" and isinstance(arguments, dict):
        target = arguments.get("target")
    if not isinstance(target, dict):
        page_call = page.get("read_more")
        kinds = page_call.get("kinds") if isinstance(page_call, dict) else None
        if isinstance(kinds, dict) and page_call.get("tool") == "inspect":
            for kind, ids in kinds.items():
                if isinstance(ids, list) and row.get("node_id") in ids:
                    target = {"kind": kind, "node_id": row.get("node_id")}
                    break
    if not (
        isinstance(target, dict) and target.get("kind") in ("text", "node")
        and isinstance(target.get("node_id"), str)
    ):
        return None
    shown = json.dumps(
        {"kind": target["kind"], "node_id": target["node_id"]},
        ensure_ascii=False, separators=(",", ":"),
    )
    return f"{_ROW_INDENT}read whole: inspect(target={shown})"


def _window_body(row: dict[str, Any]) -> tuple[list[str], Optional[str]]:
    """An older row's grep match windows (one line each), else its excerpt
    (a row with neither ``head`` nor ``passage``)."""
    windows = row.get("match_windows")
    if isinstance(windows, list) and windows:
        matches = [
            {"line": w.get("line"), "text": w.get("excerpt")}
            for w in windows if isinstance(w, dict) and w.get("excerpt") is not None
        ]
        if matches:
            return _match_line_body(matches)
    excerpt = row.get("excerpt")
    if isinstance(excerpt, str) and excerpt.strip():
        return _indented_text(excerpt), _passage_key(None, None, excerpt)
    return [], None


def _row_read_kind(row: dict[str, Any], page: dict[str, Any]) -> Optional[str]:
    """The inspect kind that reads this row's record (``text`` or
    ``node``): its ``read_more``, else the page's when the rows were cut to
    fit. None when neither names it."""
    call = row.get("read_more")
    arguments = call.get("arguments") if isinstance(call, dict) else None
    target = arguments.get("target") if isinstance(arguments, dict) else None
    if isinstance(target, dict) and target.get("kind") in (_WHOLE_TEXT, _WHOLE_NODE):
        return str(target["kind"])
    page_call = page.get("read_more")
    kinds = page_call.get("kinds") if isinstance(page_call, dict) else None
    if isinstance(kinds, dict):
        for kind, ids in kinds.items():
            if kind in (_WHOLE_TEXT, _WHOLE_NODE) and isinstance(ids, list) and (
                row.get("node_id") in ids
            ):
                return str(kind)
    return None


def _match_lines_served(
    matches: list[Any],
    ident: Any,
    read_kind: Optional[str],
    session_state: Optional[dict[str, Any]],
    call: Optional[str],
) -> tuple[list[str], bool]:
    """A grep row's match lines, less the ones this session already printed
    (PACT read-reply-size-3666): each line is remembered on its own. All
    shown before: the one ``shown earlier (...)`` line. Some: only the new
    lines, then ``N lines shown earlier``. Returns the lines and whether the
    row is a repeat (no new text)."""
    lines, _key = _match_line_body(matches)
    whole = _whole_before(session_state, ident, read_kind)
    if whole is not None:
        return [_shown_line(whole)], True
    keys = [_match_line_key(match) for match in matches]
    entry = _served_record(session_state, ident)
    calls = [entry["keys"].get(key) if entry is not None else None for key in keys]
    shown = [isinstance(earlier, str) for earlier in calls]
    if session_state is not None and all(shown):
        return [_shown_line((str(calls[-1]), False))], True
    for key, said in zip(keys, shown):
        if not said:
            _mark_served(session_state, ident, call, key)
    earlier = sum(shown)
    if not earlier:
        return lines, False
    note = f"{_ROW_INDENT}{earlier} {'line' if earlier == 1 else 'lines'} shown earlier"
    dropped = [line for line, said in zip(lines, shown) if said]
    if len(note) >= sum(len(line) + 1 for line in dropped):
        # The note would be longer than the lines it stands for: a served
        # row never grows past the row its byte measure counted.
        return lines, False
    return [line for line, said in zip(lines, shown) if not said] + [note], False


def _match_line_key(match: Any) -> str:
    """One grep match line's identity: where it is and its text."""
    if not isinstance(match, dict):
        return _passage_key("line", match)
    return _passage_key(
        "line", match.get("line"), match.get("field"), match.get("author"),
        match.get("date"), match.get("text"),
    )


def _passage_row(
    index: int,
    row: dict[str, Any],
    page: dict[str, Any],
    session_state: Optional[dict[str, Any]],
    call: Optional[str],
    repeats: Optional[list[bool]] = None,
) -> list[str]:
    """One search row: head, selected values, matched passage (or grep
    lines). The read call is the reply's one pointer line (D5). `repeats`
    gets whether the row printed no new text (`shown earlier`)."""
    lines, ident = _passage_head(index, row, page)
    lines += _selected_lines(row.get("selected"))
    read_kind = _row_read_kind(row, page)
    repeat = False
    if isinstance(row.get("match_lines"), list) and row["match_lines"]:
        body, repeat = _match_lines_served(
            row["match_lines"], ident, read_kind, session_state, call,
        )
    else:
        if "passage" in row or isinstance(row.get("head"), dict):
            body, key = _passage_body(row.get("passage"))
        else:
            body, key = _window_body(row)
        if body and key is not None:
            before = _shown_before(session_state, ident, key, read_kind)
            if before is not None:
                body = [_shown_line(before)]
                repeat = True
            else:
                _mark_served(session_state, ident, call, key)
    lines += body
    if repeats is not None:
        repeats.append(repeat)
    read_whole = None if repeat else _read_whole_line(row, page)
    if read_whole is not None:
        lines.append(read_whole)
    head = row.get("head")
    url = head.get("citation_url") if isinstance(head, dict) else None
    # A repeat row's link was printed with it in the earlier call (#3666).
    if not repeat and isinstance(url, str) and url and not is_block(url):
        lines.append(f"{_ROW_INDENT}source: {scalar(url)}")
    return lines


#: A selected value longer than this prints shortened, on one line (D6).
_SELECTED_CHARS = 200
#: Whole-attrs text keys that often hold the same words; each prints once.
_DUPLICATE_TEXT_KEYS = ("summary", "title", "description", "text")


def _selected_value(value: Any) -> str:
    """A selected value on one line: whitespace folded, long text cut."""
    if isinstance(value, str) and FENCE in value:
        inner = _fenced_inner(value)
        text = " ".join((inner if inner is not None else _neutralise_markers(value)).split())
        if len(text) > _SELECTED_CHARS:
            text = text[:_SELECTED_CHARS].rstrip() + "\u2026"
        return f"{INLINE_FENCE_OPEN}{safe_json(text)}{FENCE_CLOSE}"
    if isinstance(value, str):
        text = " ".join(_neutralise_markers(value).split())
        if len(text) > _SELECTED_CHARS:
            text = text[:_SELECTED_CHARS].rstrip() + "\u2026"
        return scalar(text) if text else '""'
    text = safe_json(value)
    if len(text) > _SELECTED_CHARS:
        text = text[:_SELECTED_CHARS] + "\u2026"
    return text


def _folded(value: Any) -> str:
    inner = _fenced_inner(value) if isinstance(value, str) and FENCE in value else None
    text = inner if inner is not None else value
    return " ".join(text.split()).casefold() if isinstance(text, str) else ""


def _attrs_without_repeats(attrs: dict[str, Any]) -> dict[str, Any]:
    """Whole selected attrs with the text keys that repeat another left out
    (D6): a summary, title, description or text whose words another of them
    already holds prints once, as the shortest one that says it."""
    kept: dict[str, Any] = {}
    said: list[str] = []
    for key, value in attrs.items():
        if key in _DUPLICATE_TEXT_KEYS and isinstance(value, str):
            folded = _folded(value)
            if folded and any(folded == other for other in said):
                continue
        kept[key] = value
        if key in _DUPLICATE_TEXT_KEYS and isinstance(value, str):
            said.append(_folded(value))
    # A text that holds every other printed text key adds no words at the
    # one-line size: the longer copy goes.
    texts = [k for k in _DUPLICATE_TEXT_KEYS if isinstance(kept.get(k), str) and _folded(kept[k])]
    for key in sorted(texts, key=lambda k: -len(_folded(kept[k]))):
        others = [_folded(kept[k]) for k in texts if k != key and k in kept]
        if others and all(other in _folded(kept[key]) for other in others):
            kept.pop(key)
            break
    return kept


def _selected_lines(selected: Any) -> list[str]:
    """A row's selected values (D6, #3666): ``key: value`` under the head,
    one line each. Whole attrs print key by key, repeats left out."""
    if not isinstance(selected, dict) or not selected:
        return []
    lines: list[str] = []
    for key, value in selected.items():
        if key == "attrs" and isinstance(value, dict):
            for name, item in _attrs_without_repeats(value).items():
                lines.append(f"{_ROW_INDENT}attrs.{key_text(name)}: {_selected_value(item)}")
            continue
        lines.append(f"{_ROW_INDENT}{_dotted_text(str(key))}: {_selected_value(value)}")
    return lines


def _dotted_text(name: str) -> str:
    """A select name such as ``attrs.summary``: bare when plain, else JSON."""
    return name if re.fullmatch(r"[A-Za-z0-9_.\-]+", name) else safe_json(name)


def _not_stored_notice(payload: dict[str, Any]) -> Optional[str]:
    """inspect(node): the selected `attrs.<key>` names the record does not
    store, and the keys it has (issue #3666, lane RD).

    When the access rules dropped keys from the read (the redaction marker
    on its `attrs`), a key the read lacks may be stored and withheld: the
    line says so, never that the record does not store it (review P2-3)."""
    names = payload.get("attrs_not_stored")
    if not isinstance(names, list) or not names:
        return None
    attrs = payload.get("attrs")
    withheld = isinstance(attrs, dict) and attrs.get(_REDACTION_MARKER) is True
    head = (
        "! not stored on this record, or withheld by access rules: " if withheld
        else "! not stored on this record: "
    )
    line = head + ", ".join(
        _dotted_text(str(name)) for name in names
    )
    have = payload.get("attr_keys")
    if isinstance(have, list) and have:
        line += "; it has attrs " + ", ".join(key_text(str(key)) for key in have)
    return line


def _unknown_selected_notice(rows: list[Any], select: Any) -> Optional[str]:
    """One notice for selected attribute keys no row on the page holds
    (D6): ``! no row has attrs.x; these rows have: a, b, ...``."""
    names = [n for n in (select or ()) if isinstance(n, str) and n.startswith("attrs.")]
    if not names:
        return None
    dict_rows = [row for row in rows if isinstance(row, dict)]
    if not dict_rows:
        return None
    missing: list[str] = []
    for name in names:
        key = name[len("attrs."):]
        found = False
        for row in dict_rows:
            chosen = row.get("selected")
            attrs = row.get("attrs")
            if isinstance(chosen, dict) and (name in chosen or key in chosen or (
                isinstance(chosen.get("attrs"), dict) and key in chosen["attrs"]
            )):
                found = True
            elif isinstance(attrs, dict) and key in attrs:
                found = True
            if found:
                break
        if not found:
            missing.append(name)
    if not missing:
        return None
    have: list[str] = []
    for row in dict_rows:
        for source in (row.get("attrs"), (row.get("selected") or {}).get("attrs")
                       if isinstance(row.get("selected"), dict) else None):
            if isinstance(source, dict):
                for key in source:
                    if isinstance(key, str) and key not in have:
                        have.append(key)
        keys = row.get("attr_keys")
        if isinstance(keys, list):
            for key in keys:
                if isinstance(key, str) and key not in have:
                    have.append(key)
    line = "! no row on this page has " + ", ".join(_dotted_text(n) for n in missing)
    if have:
        shown = have[:16]
        line += "; these rows have attrs " + ", ".join(key_text(k) for k in shown) + (
            ", ..." if len(have) > len(shown) else ""
        )
    return line


def _grep_searched_notice(payload: dict[str, Any], n_rows: int) -> Optional[str]:
    """What an exact-text search read and which spellings it matched (#3666)."""
    block = payload.get("grep_searched")
    if not isinstance(block, dict):
        return None
    fields = [str(f) for f in block.get("fields") or [] if isinstance(f, (str, int))]
    comments = block.get("comments")
    what = ", ".join(key_text(f) for f in fields) or "record text"
    if block.get("name_only") is True:
        # Lane ri (CompOps q020): `source_file` records hold path words only.
        what = (
            "path words (a source_file holds no file contents; contents are "
            "document records)"
        )
    elif comments is True:
        what += ", comments"
    elif comments is False:
        what += "; comments not searched"
    matched = [s for s in block.get("spellings_matched") or [] if isinstance(s, str)]
    tried = [s for s in block.get("spellings_tried") or [] if isinstance(s, str)]
    bound_read = block.get("bound_read")
    if (
        n_rows == 0 and not payload.get("offset")
        and payload.get("page_basis") == "scan_bound_reached"
        and isinstance(bound_read, dict)
        and _int(bound_read.get("rows")) is not None
    ):
        # Format v10 item G2: the scan stopped at a bound before any record
        # of its own, so nothing past the rows it read was judged: never
        # "no match".
        read = _int(bound_read.get("rows")) or 0
        unit = {
            "candidate_rows": "candidate records",
            "index_rows": "rows the word index returned",
        }.get(str(bound_read.get("bound")), "records of the scope")
        if bound_read.get("continued") is True:
            # A continuation page read on from where its page ended.
            line = (
                f"! no match in the next {read:,} {unit} past the previous page; "
                f"the rest are not read yet; searched: {what}"
            )
        else:
            line = (
                f"! no match in the first {read:,} {unit} (of {read + 1:,} or more); "
                f"the rest are not read yet; searched: {what}"
            )
        if payload.get("continuation_token"):
            line += (
                '; next: the same search with method="grep", the same query and '
                "where, and the continuation_token above"
            )
        else:
            line += "; next: narrow the query"
        return line
    if n_rows == 0 and not payload.get("offset"):
        line = f"! no match; searched: {what}"
        if tried:
            line += "; spellings tried: " + ", ".join(safe_json(s) for s in tried)
        return line
    line = f"! searched: {what}"
    if matched:
        line += "; spellings matched: " + ", ".join(safe_json(s) for s in matched)
    return line


def _search_status(
    payload: dict[str, Any],
    rows: list[Any],
    query: Any,
    session_state: Optional[dict[str, Any]],
    *,
    raised: bool,
) -> str:
    """``search ok · "<query>" · rows 1-10 of 47 · next: offset=10`` (#3666).

    One short line: the query, the rows, the continuation. No retrieval or
    count internals, no profile type list, and no generation (D8: it is
    asked for with inspect(target={"kind": "graph"})).
    """
    facts: list[str] = []
    method = payload.get("method")
    if isinstance(query, str) and query:
        said = safe_json(query)
        if method == "grep":
            said = f"(exact text) {said}"
        elif isinstance(method, str) and method not in ("lexical",) and not is_block(method):
            said = f"({token_text(method)}) {said}"
        facts.append(said)
    elif method is not None and not is_block(method):
        facts.append(token_text(method))
    if method == "grep" and payload.get("and_scope") in ("text_unit", "record"):
        # PACT read-search-caps-3666: where the terms had to meet, said on
        # every grep page, so an empty unit page reads as a reason to try
        # within="record" rather than as "no record holds these words".
        facts.append("within record" if payload["and_scope"] == "record" else "within unit")
    if payload.get("view") == "list" and payload.get("view_implied") is True:
        # PACT read-names-list-3666: the call named no view; grep's default
        # is one name line per record, and the next call that shows the
        # matched lines is named.
        facts.append('list (grep default; view="cards" shows matched lines)')
    elif payload.get("view") in ("list", "metadata", "windows"):
        facts.append(str(payload["view"]))
    passages = payload.get("result_unit") == "passage"
    if passages:
        # Each row is a passage, not a record: the counts are passages.
        facts.append("passages")
    default_rank = "records" if method == "lexical" else "self"
    if payload.get("rank_by") not in (None, default_rank) and not is_block(payload["rank_by"]):
        facts.append(f"rank {token_text(payload['rank_by'])}")
    scoped = payload
    if isinstance(payload.get("search_profile"), str) and payload.get("where_applied") is None:
        # The profile's type list is the default scope: not said (#3666).
        scoped = {k: v for k, v in payload.items() if k != "scoped_subtypes"}
    where, _keys = _where_fact(scoped)
    if where is not None:
        facts.append(where)
    checked = _where_checked_fact(payload)
    if checked is not None:
        # Issue #3666 (dev, 4c432e92c5/c55331e109): a row filter checked on
        # the ranked candidates only says so, beside the where it qualifies.
        facts.append(checked)
    members = payload.get("reference_members")
    if isinstance(members, int) and not isinstance(members, bool):
        facts.append(f"over the reference's {members} row(s) only")
    undecided = _undecided(payload)
    if undecided is not None:
        facts.append(f"{undecided} undecided")
    phrase = payload.get("phrase")
    if isinstance(phrase, dict) and phrase.get("status") not in (None, "matched"):
        facts.append(f"phrase={scalar(phrase.get('status'))}")
    basis = payload.get("total_count_basis")
    if basis not in (None, "exact", "lower_bound", "unknown"):
        # A count of another kind (an estimate) says so: a count claim.
        facts.append(f"count {scalar(basis)}")
    n_rows = len(rows)
    offset = _int(payload.get("offset")) or 0
    shortened = payload.get("passages_shortened_to")
    if _int(shortened) is not None:
        # D4: every row is kept; the passages were cut to fit the reply. A
        # matched comment or description kept whole past that cap is said
        # (`passages_kept_whole`), so a page holding passages never reads
        # as head lines only.
        whole = _int(payload.get("passages_kept_whole")) or 0
        kept = (
            f"; {whole} matched {'passage' if whole == 1 else 'passages'} kept whole"
            if whole > 0 else ""
        )
        if shortened > 0:
            facts.append(f"passages shortened to {shortened} chars to fit{kept}")
        elif whole > 0:
            facts.append(f"other passages left out to fit{kept}")
        else:
            facts.append("head lines only to fit")
    heads_cut = payload.get("head_lines_shortened")
    if isinstance(heads_cut, dict) and heads_cut:
        # D4: even the head lines were shortened to keep every row.
        left_out = [
            words for key, words in (
                ("source_links", "no source links"), ("source_names", "no source names"),
            ) if heads_cut.get(key)
        ]
        title_chars = _int(heads_cut.get("title_chars"))
        if title_chars is not None:
            left_out.append(f"titles cut to {title_chars} chars")
        # `row_fields` (row fields only the JSON carries) prints nothing:
        # a text search row never prints them.
        if left_out:
            facts.append("head lines shortened to fit: " + ", ".join(left_out))
    row_sources = payload.get("row_sources")
    if isinstance(row_sources, list) and row_sources:
        # The source names left off the rows, said once for the page.
        facts.append(
            ("source " if len(row_sources) == 1 else "sources ")
            + ", ".join(scalar(name) for name in row_sources)
        )
    else:
        shared = _shared_row_source(payload)
        if shared is not None:
            # Every row shares one source: named once here, not per row.
            facts.append(f"source {scalar(shared)}")
    metadata = _metadata(payload)
    count = _metadata_counts(payload) if metadata else _counts(payload, n_rows)
    ending = _ending(payload)
    listing = _listing_fact(payload)
    retrieval = _retrieval(payload).get("status")
    if ending == ["complete"] and retrieval not in (None, "exhaustive"):
        # Every retrieved row is here, but retrieval itself was bounded or
        # approximate: "all shown" would claim every match, so the page says
        # which retrieval it is (a completeness caveat, not an internal).
        if count:
            facts.append(f"rows {count}")
        facts += [f"retrieval {scalar(retrieval)}", "complete"]
    elif (
        ending == ["complete"] and not metadata and offset == 0
        and _not_a_total(payload) is None
    ):
        noun = "passage" if passages else "record"
        facts.append(
            f"{n_rows} {noun}{'' if n_rows == 1 else 's'}, all shown" if n_rows
            else f"no {noun}s match"
        )
    else:
        if count:
            facts.append(
                f"rows {count}" if not metadata and count[:1].isdigit() else count
            )
        if retrieval == "unknown" and payload.get("completeness") == "exact":
            # Issue #3666 (dev): under enforced access rules an exact read's
            # proof is withheld; the page says its retrieval is unknown, the
            # one bit it discloses, whether or not it is the last page. A
            # page that asked for no exact read says nothing of retrieval.
            facts.append("retrieval unknown")
        if listing is not None:
            facts.append(listing)
        if (
            not metadata and ending[0].startswith("bound reached")
            and not ending[-1].endswith(": none")
        ):
            # The page-cut notice says a byte cut; a limit is the call's own.
            facts += ending[1:]
        else:
            facts += [
                re.sub(r"\(max_bytes[^)]*\)", "(reply size)", fact) for fact in ending
            ]
    return SEP.join([f"search {_status_word(payload, raised=raised)}", *facts])


def _render_search(
    payload: dict[str, Any],
    session_state: Optional[dict[str, Any]],
    query: Any,
    call: Optional[str],
    *,
    raised: bool,
    selected: frozenset[str] = frozenset(),
) -> str:
    """A search page in text (#3666): the short status line, the actionable
    notices, then one passage row per record. No other-fields block: the
    JSON form carries every field."""
    rows_key = _rows_key(payload, "search")
    rows = payload[rows_key] if rows_key else []
    lines = [_search_status(payload, rows, query, session_state, raised=raised)]
    reference_line = _reference_line(payload)
    if reference_line is not None:
        lines.append(reference_line)
    error = payload.get("error")
    if error is not None:
        # A page that carries an error beside its rows (a failed status)
        # says it; the error block is in the JSON form.
        message = error.get("message", error.get("code")) if isinstance(error, dict) else error
        lines.append(f"! error: {_one_line(message)}")
    lines += [
        notice for notice in map(_search_notice, _notices(payload, session_state))
        if notice is not None
    ]
    searched = _grep_searched_notice(payload, len(rows))
    if searched is not None:
        lines.append(searched)
    count_call = _count_each_match_line(payload)
    if count_call is not None:
        lines.append(count_call)
    unknown = _unknown_selected_notice(rows, selected)
    if unknown is not None:
        lines.append(unknown)
    shared_basis = (
        _shared_line_basis(rows) if payload.get("view") == "list" else None
    )
    if shared_basis is not None:
        lines.append(_SHARED_LINE_BASIS_NOTICE[shared_basis])
    start = (_int(payload.get("offset")) or 0) + 1
    windows_view = payload.get("view") == "windows"
    repeats: list[bool] = []
    for index, row in enumerate(rows, start):
        if payload.get("view") == "list" and isinstance(row, dict):
            # D1: the short row shape is the head line alone.
            if isinstance(row.get("head"), dict):
                lines.append(_passage_head(index, row, payload)[0][0])
                # D6: a list row with a select is the head plus the chosen keys.
                lines += _selected_lines(row.get("selected"))
            else:
                lines.append(
                    _search_list_line(index, row, payload, shared_basis=shared_basis)
                )
            continue
        lines.append("")
        if not isinstance(row, dict):
            lines.append(f"[{index}] {_cell(row)}")
        elif windows_view:
            lines += _window_record_card(index, row)
        else:
            lines += _passage_row(index, row, payload, session_state, call, repeats)
    help_lines = _repeat_help(payload, query, len(rows), sum(repeats))
    if help_lines is not None:
        fact, notice = help_lines
        head, tail = _before_ending(lines[0])
        lines[0] = SEP.join([head, fact, *([tail] if tail else [])])
        lines.insert(1, notice)
    block = _summary(payload) if _metadata(payload) else None
    if block is not None:
        lines.append("")
        lines += _summary_lines(block)
    pointer = _read_pointer(rows, payload.get("read_more"))
    if pointer is not None:
        lines += ["", pointer]
    return "\n".join(lines)


#: A search page fires the repeat help when every row was shown earlier, or
#: at least this many rows and this share of them (PACT read-search-caps-3666).
REPEAT_HELP_ROWS = 3
REPEAT_HELP_SHARE = 0.8
#: Arguments the repeat help leaves out of the calls it builds: they page,
#: size or shape this call's reply, not what it searches.
_REPEAT_HELP_DROPPED = frozenset({
    "offset", "continuation_token", "max_bytes", "max_chars", "view", "rank_by",
    "completeness", "retain", "summary", "scope", "matching", "context_chars",
    "max_per_parent", "select", "limit", "within",
})


def _repeat_help(
    payload: dict[str, Any], query: Any, n_rows: int, n_repeat: int,
) -> Optional[tuple[str, str]]:
    """``(status fact, notice)`` when a search page holds mostly rows this
    session already printed (PACT read-search-caps-3666), else None.

    Agents on the hard-50 fishing questions repeated near-identical searches
    whose rows were all `shown earlier` and never changed method. The page
    says so on its first line and names the broader moves, each a call built
    from this one: other words as a regex alternation, every term anywhere
    in a record (grep `within="record"`), every matching record as ids only
    (the sweep), and a type or path filter."""
    if n_rows <= 0 or not (
        n_repeat == n_rows
        or (n_repeat >= REPEAT_HELP_ROWS and n_repeat >= REPEAT_HELP_SHARE * n_rows)
    ):
        return None
    fact = (
        f"all {n_rows} rows shown earlier" if n_repeat == n_rows
        else f"{n_repeat} of {n_rows} rows shown earlier"
    )
    from okg.substrate.mcp.response_bounds import call_arguments

    sent = call_arguments()
    base: dict[str, Any] = {}
    if isinstance(sent, dict) and isinstance(sent.get("query"), str):
        base = {k: v for k, v in sent.items() if k not in _REPEAT_HELP_DROPPED}
    elif isinstance(query, str) and query:
        # The query as the reply read it (the page's own `query` key is
        # not always kept on the rendered payload).
        base = {"query": query}
        if isinstance(payload.get("path_prefix"), str):
            base["path_prefix"] = payload["path_prefix"]
    moves = ["other words: re:/x|y/ matches either spelling"]
    if base.get("query"):
        record = {**base, "method": "grep", "within": "record"}
        if payload.get("and_scope") != "record":
            shown = call_text({"tool": "search", "arguments": record})
            if shown is not None:
                moves.append(f"each term anywhere in a record: {shown}")
        sweep = call_text({"tool": "search", "arguments": {
            **record, "select": ["node_id", "handle"], "limit": 100,
        }})
        if sweep is not None:
            moves.append(f"every match, ids only: {sweep}")
    moves.append('narrower: where={"type":"<type>"} or path_prefix="<path>"')
    return fact, "! rows repeat; to find other records: " + "; ".join(moves)


def _count_each_match_line(payload: dict[str, Any]) -> Optional[str]:
    """Format v10 item C (#3666): a cut grep page whose query holds a regex
    names the aggregate call counting each distinct match. Only on grep and
    only on a cut page; the server attaches the call under the same rule."""
    if payload.get("method") != "grep" or not _cut(payload):
        return None
    from okg.substrate.mcp.match_count_hint import (
        COUNT_EACH_MATCH_KEY,
        render_count_each_match,
    )

    return render_count_each_match(payload.get(COUNT_EACH_MATCH_KEY))


#: A search notice naming an argument search does not publish (D3, #3666).
_HIDDEN_SEARCH_ARGUMENT = re.compile(r"\bmax_bytes\b")


#: The start of the recency-annotation notice (`_notices`).
_RECENCY_NOTICE = "! rows annotated with recency by"


def _search_notice(notice: str) -> Optional[str]:
    """A notice as a search reply prints it: no clause names
    ``max_bytes``, which search accepts but does not publish (D3, #3666).
    The page-cut notice says the reply size instead; a notice whose only
    point is that argument is not printed."""
    if notice.startswith("! max_bytes "):
        return None
    if notice.startswith(_RECENCY_NOTICE):
        # PACT read-passage-rows-3666 (f): how a search's rows were
        # annotated with recency is an internal of the read; the JSON keeps
        # the `recency` block.
        return None
    notice = re.sub(r"max_bytes=[^\s;]+", "the reply size", notice, count=1)
    clauses = notice.split("; ")
    kept = [clauses[0]] + [c for c in clauses[1:] if not _HIDDEN_SEARCH_ARGUMENT.search(c)]
    if _HIDDEN_SEARCH_ARGUMENT.search(kept[0]):
        return None
    return "; ".join(kept)


def _read_pointer(rows: list[Any], page_call: Any = None) -> Optional[str]:
    """The one line that says how to read a record (D5, #3666): the rows'
    ``read_more`` calls share a shape, so it prints once per reply, with
    ``<id>`` for the record's id. Rows keep their exact call in JSON, unless
    a page at its row floor carries one page ``read_more`` with the ids of
    each target kind (``{"tool": "inspect", "kinds": {kind: [ids]}}``)."""
    kinds: list[str] = []
    page_kinds = page_call.get("kinds") if isinstance(page_call, dict) else None
    if isinstance(page_kinds, dict) and page_call.get("tool") == "inspect":
        kinds = [kind for kind in page_kinds if kind in ("text", "node")]
    for row in rows:
        call = row.get("read_more") if isinstance(row, dict) else None
        arguments = call.get("arguments") if isinstance(call, dict) else None
        target = arguments.get("target") if isinstance(arguments, dict) else None
        if (
            isinstance(call, dict) and call.get("tool") == "inspect"
            and isinstance(target, dict)
            and target.get("kind") in ("text", "node") and target["kind"] not in kinds
        ):
            kinds.append(target["kind"])
    if not kinds:
        return None
    line = 'read a record: inspect(target={"kind":"%s","node_id":"<id>"})' % kinds[0]
    if len(kinds) > 1:
        line += '; rows with no text (code): "kind":"node"' if kinds[0] == "text" else (
            '; rows with text: "kind":"text"'
        )
    return line


# ── live rows (#3666: expand(live=...) follows the row rules) ─────────────
#
# A live relation's answer prints one line per row, `[n] key value · key
# value`, identifying fields first, then states and numbers, then the rest.
# No row is a JSON dump: a nested value prints as a count. When the rows do
# not fit the reply budget, the live step (`live_dispatch._bounded_rows`)
# shortens every row's values evenly, then keeps fewer non-key fields per
# row, and only then serves a prefix; it says which in
# `external_live[LIVE_TEXT_SHORTENED_KEY]`, which only this text form reads.

#: The envelope key the live step sets when the text form must cut values to
#: fit: ``{"value_chars": int}``, plus ``"fields": int`` (non-key fields kept
#: per row) when cutting values was not enough.
LIVE_TEXT_SHORTENED_KEY = "text_shortened"
#: Characters a non-key value prints before it is cut, when nothing is cut
#: to fit.
LIVE_VALUE_CHARS = 120
#: The shortest a non-key value is cut to before fields are dropped.
LIVE_VALUE_FLOOR = 12
#: Characters an identifying value prints at most, whatever the fit.
LIVE_KEY_CHARS = 300
#: Identifying fields, printed first, in this order (generic names, matched
#: without case). A row holding both `scope` and `name` prints them as one
#: `scope:name` field.
_LIVE_KEY_FIELDS = (
    "name", "dataset", "block", "block_name", "logical_file_name", "lfn", "file",
    "path", "rse", "rse_expression", "rule_id", "id", "account", "key", "title",
)
#: States and numbers, printed next, in this order; any other field whose
#: name ends in state or status counts as a state, and any other number or
#: date-named field follows them.
_LIVE_STATE_FIELDS = (
    "state", "status", "availability", "type", "did_type",
    "size", "bytes", "length", "file_size", "files", "file_count", "num_files",
    "events", "event_count", "lumis", "lumi_count", "blocks", "block_count", "copies",
    "created", "created_at", "creation_date", "updated", "updated_at",
    "last_modification_date", "accessed_at", "expires_at",
)
_LIVE_DATE_SUFFIXES = ("_at", "_date", "_time")


def _live_plain(value: Any) -> Any:
    """A live value without its untrusted-data fence, on one line.

    The whole row block sits inside one fence (:func:`_live_lines`), so each
    value prints its inner text; a value whose fence is not well formed
    prints with its marker sequences neutralised, so it cannot close the
    block's fence."""
    if not isinstance(value, str):
        return value
    inner = _fenced_inner(value) if FENCE in value else None
    text = _neutralise_markers(inner if inner is not None else value)
    return " ".join(text.split())


#: Characters a nested (object or list) value prints before its remaining
#: entries are counted, on a reply that is not cut to fit; a `next` page of a
#: kept answer prints nested values whole (Live build 3).
LIVE_NESTED_CHARS = 600
#: Levels a nested value is opened; deeper ones print as a count.
_LIVE_NESTED_DEPTH = 3


def _live_inner(value: Any, depth: int) -> str:
    """One nested entry's value, whole: objects as `key: value`, lists
    joined, both in parentheses below the top level."""
    if isinstance(value, dict):
        if depth >= _LIVE_NESTED_DEPTH:
            return f"{len(value)} field" + ("" if len(value) == 1 else "s")
        return "(" + ", ".join(
            f"{key_text(k)}: {_live_inner(v, depth + 1)}" for k, v in value.items()
        ) + ")"
    if isinstance(value, (list, tuple)):
        if depth >= _LIVE_NESTED_DEPTH:
            return f"{len(value)} item" + ("" if len(value) == 1 else "s")
        return "(" + ", ".join(_live_inner(v, depth + 1) for v in value) + ")"
    return scalar(_live_plain(value))


def _live_nested(value: Any, limit: Optional[int]) -> tuple[str, bool]:
    """A nested live value as `key: value, ...` (an object) or `a, b, ...` (a
    list), with as many entries as `limit` characters hold and the rest
    counted (`+N more`); `(text, cut)`. `limit` None prints it whole."""
    if isinstance(value, dict):
        entries = [f"{key_text(k)}: {_live_inner(v, 1)}" for k, v in value.items()]
    else:
        entries = [_live_inner(v, 1) for v in value]
    if not entries:
        return "empty", False
    if limit is None:
        return ", ".join(entries), False
    shown: list[str] = []
    used = 0
    for entry in entries:
        extra = len(entry) + (2 if shown else 0)
        if used + extra > limit:
            break
        shown.append(entry)
        used += extra
    if not shown:
        # The first entry alone is too long: its start, cut.
        shown, cut_first = [entries[0][:limit].rstrip() + "…"], True
    else:
        cut_first = False
    rest = len(entries) - len(shown)
    text = ", ".join(shown) + (f" +{rest} more" if rest else "")
    return text, bool(rest) or cut_first


def live_nested_cut(rows: Any, limit: int = LIVE_NESTED_CHARS) -> bool:
    """Whether the text form of these live rows, printed at full width, cuts
    any nested value (so the reply must say how to read it whole)."""
    items = rows if isinstance(rows, list) else [rows]
    for row in items:
        if not isinstance(row, dict):
            continue
        for value in row.values():
            if isinstance(value, (dict, list, tuple)) and _live_nested(value, limit)[1]:
                return True
    return False


def _live_value(value: Any, limit: Optional[int], nested: Optional[int] = LIVE_NESTED_CHARS) -> str:
    if isinstance(value, (dict, list, tuple)):
        # Object and list values print their entries (#3666, Live build 3):
        # `states SITE_A: AVAILABLE`, never a bare count of fields.
        # Cut to fit: the same even cut as every other value. Otherwise the
        # nested bound (None on a `next` page: whole).
        shortened = limit is not None and limit < LIVE_VALUE_CHARS
        return _live_nested(value, limit if shortened else nested)[0]
    value = _live_plain(value)
    if isinstance(value, str) and limit is not None and len(value) > limit:
        value = value[:limit].rstrip() + "…"
    return scalar(value)


def _live_rank(key: str, value: Any) -> tuple[int, int]:
    folded = key.casefold()
    if folded in _LIVE_KEY_FIELDS:
        return 0, _LIVE_KEY_FIELDS.index(folded)
    if folded in _LIVE_STATE_FIELDS:
        return 1, _LIVE_STATE_FIELDS.index(folded)
    if folded.endswith(("state", "status")):
        return 1, len(_LIVE_STATE_FIELDS)
    if (
        isinstance(value, (int, float)) and not isinstance(value, bool)
    ) or folded.endswith(_LIVE_DATE_SUFFIXES):
        return 1, len(_LIVE_STATE_FIELDS) + 1
    return 2, 0


def _live_fields(row: dict[str, Any]) -> list[tuple[str, Any, bool]]:
    """``(label, value, is_key)`` of one live row, in print order.

    Labels are printed as given (already a key text); the row's own key
    order breaks ties, so the order is stable for one relation."""
    folded = {str(key).casefold(): key for key in row}
    combined: list[tuple[str, Any, bool]] = []
    skip: set[Any] = set()
    scope_key, name_key = folded.get("scope"), folded.get("name")
    if scope_key is not None and name_key is not None:
        scope, name = _live_plain(row[scope_key]), _live_plain(row[name_key])
        if isinstance(scope, str) and isinstance(name, str):
            combined.append(("scope:name", f"{scope}:{name}", True))
            skip = {scope_key, name_key}
    ranked = sorted(
        (
            (_live_rank(str(key), value), position, key, value)
            for position, (key, value) in enumerate(row.items())
            if key not in skip
        ),
        key=lambda item: (item[0], item[1]),
    )
    fields = combined + [
        (key_text(key), value, rank[0] == 0) for rank, _pos, key, value in ranked
    ]
    if fields and not any(is_key for _l, _v, is_key in fields):
        # A row with no identifying field: its first field identifies it.
        label, value, _ = fields[0]
        fields[0] = (label, value, True)
    return fields


def _live_row_parts(
    row: dict[str, Any], *, value_chars: int, keep_fields: Optional[int],
    nested: Optional[int] = LIVE_NESTED_CHARS,
) -> list[tuple[str, str]]:
    """``(label, value text)`` pairs one row prints, cut as the fit says."""
    out: list[tuple[str, str]] = []
    kept = 0
    for label, value, is_key in _live_fields(row):
        if is_key:
            out.append((label, _live_value(value, LIVE_KEY_CHARS, nested)))
            continue
        if keep_fields is not None and kept >= keep_fields:
            continue
        kept += 1
        out.append((label, _live_value(value, value_chars, nested)))
    return out


def _live_shortened(live: dict[str, Any]) -> tuple[int, Optional[int]]:
    spec = live.get(LIVE_TEXT_SHORTENED_KEY)
    value_chars, keep_fields = LIVE_VALUE_CHARS, None
    if isinstance(spec, dict):
        chars = _int(spec.get("value_chars"))
        if chars is not None and chars > 0:
            value_chars = chars
        fields = _int(spec.get("fields"))
        if fields is not None and fields >= 0:
            keep_fields = fields
    return value_chars, keep_fields


def _live_shown(live: dict[str, Any]) -> tuple[int, int, Optional[int]]:
    """``(first row index, rows shown, total)`` of a live envelope; the
    first index is 0-based (a `next` page starts past row 1)."""
    rows = live.get("rows")
    shown = (
        len(rows) if isinstance(rows, list)
        else (1 if isinstance(rows, dict) and rows else 0)
    )
    summary = live.get("summary") if isinstance(live.get("summary"), dict) else {}
    offset = _int(live.get("offset")) or 0
    return max(offset, 0), shown, _int(summary.get("row_count"))


def _live_next_line(live: dict[str, Any]) -> Optional[str]:
    """The one line that says what a cut live answer left out and the ONE
    call that goes on (#3666, Live build 3), or None when nothing was cut.

    `expand(next=...)` serves the kept answer from the row the token names,
    every field whole, without asking the live system again. Fields are cut
    before rows, so after a field cut the token starts again at row 1."""
    spec = live.get(LIVE_TEXT_SHORTENED_KEY)
    spec = spec if isinstance(spec, dict) else None
    truncated = live.get("truncated") is True
    nested_cut = live.get("nested_cut") is True
    if not truncated and spec is None and not nested_cut:
        return None
    offset, shown, total = _live_shown(live)
    if shown == 0:
        what = f"no rows shown of {total:,}" if total is not None else "no rows shown"
    elif truncated:
        what = f"rows {offset + 1}-{offset + shown} of " + (
            f"{total:,}" if total is not None else "count unknown"
        ) + " shown"
    else:
        what = f"{shown:,} row" + ("" if shown == 1 else "s") + " shown"
    _value_chars, keep_fields = _live_shortened(live)
    if spec is not None:
        what += ", fields cut to fit" if keep_fields is not None else ", values cut to fit"
    elif nested_cut:
        # Object or list values with more entries than one line holds.
        what += ", nested values cut"
    token = live.get("next")
    unavailable = live.get("next_unavailable")
    if isinstance(token, str):
        start = _int(live.get("next_from_row")) or 1
        whole = (
            "with values not cut" if spec is not None and keep_fields is None
            else "with nested values whole" if spec is None and nested_cut
            else "with all fields"
        )
        return (
            f"! {what}; next: expand(next={safe_json(token)}) returns rows "
            f"{start:,} onward {whole}"
        )
    if unavailable is not None:
        return (
            f"! {what}; the answer could not be kept to page "
            f"({token_text(unavailable)}): narrow the args"
        )
    return f"! {what}; narrow the args to see the rest" if truncated else None


def _live_status_facts(live: dict[str, Any]) -> list[str]:
    """The status line's live facts: the relation, then its rows."""
    relation = token_text(live.get("relation"))
    state = live.get("state")
    if state != "ok":
        return [f"live {relation} {token_text(state)}"]
    facts = [f"live {relation}"]
    offset, shown, total = _live_shown(live)
    if (live.get("truncated") is True or offset) and total is not None:
        facts.append(
            f"rows {offset + 1}-{offset + shown} of {total:,}" if shown
            else f"0 of {total:,} rows shown"
        )
    elif live.get("truncated") is True:
        facts.append(
            f"rows {offset + 1}-{offset + shown} of count unknown" if shown
            else "no rows shown"
        )
    else:
        facts.append(f"{shown:,} row" + ("" if shown == 1 else "s") + (", all shown" if shown else ""))
    if live.get("served_from") == "retained_answer":
        # A `next` page: read from the kept answer, the live system not asked.
        facts.append("from the kept answer")
    if isinstance(live.get(LIVE_TEXT_SHORTENED_KEY), dict):
        value_chars, keep_fields = _live_shortened(live)
        if keep_fields == 0:
            facts.append("identifying fields only, to fit")
        elif keep_fields is not None:
            facts.append(
                f"values cut to {value_chars} chars and {keep_fields} other "
                "field(s) per row, to fit"
            )
        else:
            facts.append(f"values cut to {value_chars} chars to fit")
    return facts


def _live_lines(node: Any, live: dict[str, Any]) -> list[str]:
    """The live section of an expand reply: its rows, then what continues it."""
    relation = live.get("relation")
    lines = [f"--- live {token_text(relation)}"]
    if live.get("state") != "ok":
        error = live.get("error")
        key_lines, key_blocks = _key_lines(error if isinstance(error, dict) else {"error": error})
        lines += key_lines + key_blocks
        query = live.get("query")
        if isinstance(query, dict) and query:
            # Said on every form that has no rows, so the error form the
            # reply budget reserves is never smaller than the digest form.
            lines.append(f"args: {safe_json(query)}")
        return lines
    value_chars, keep_fields = _live_shortened(live)
    # A `next` page of a kept answer prints nested values whole; any other
    # reply counts what does not fit LIVE_NESTED_CHARS (and says so below).
    nested = None if live.get("served_from") == "retained_answer" else LIVE_NESTED_CHARS
    rows = live.get("rows")
    source = _marker_source_text(f"external_live:{relation}")
    if isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], dict) and (
        live.get("truncated") is not True and not live.get("offset")
    ):
        rows = rows[0]
    if isinstance(rows, dict):
        # One object (a get or ping relation): one `key: value` line each.
        lines.append(f"{FENCE} source={source}⟧")
        for label, text in _live_row_parts(
            rows, value_chars=value_chars, keep_fields=keep_fields, nested=nested,
        ):
            lines.append(f"{label}: {text}")
        lines.append(FENCE_CLOSE)
    elif isinstance(rows, list) and rows:
        lines.append(f"{FENCE} source={source}⟧")
        # A `next` page numbers its rows from where it starts.
        for index, row in enumerate(rows, _live_shown(live)[0] + 1):
            if isinstance(row, dict):
                parts = _live_row_parts(
                    row, value_chars=value_chars, keep_fields=keep_fields, nested=nested,
                )
                body = SEP.join(f"{label} {text}" for label, text in parts)
            else:
                body = _live_value(row, value_chars, nested)
            lines.append(f"[{index}] {body}".rstrip())
        lines.append(FENCE_CLOSE)
    shown = len(rows) if isinstance(rows, list) else (1 if isinstance(rows, dict) and rows else 0)
    reference = live.get("result_reference")
    reference = reference if isinstance(reference, dict) else {}
    # Live build 3 (#3666): a cut answer names ONE call that goes on, its
    # `next` token; no rerun with retain, no second read.
    cut_line = _live_next_line(live)
    if cut_line is not None:
        lines.append(cut_line)
    query = live.get("query")
    if shown == 0 and live.get("truncated") is True and isinstance(query, dict) and query:
        lines.append(f"args: {safe_json(query)}")
    if reference.get("retained") is True and reference.get("id") is not None and live.get("each"):
        # A fanned-out answer is kept for its total: name the call that adds.
        lines.append(
            f"kept; total a column: aggregate(input={{\"ref\": {safe_json(reference['id'])}}}, "
            'by=[], metrics=["sum(attrs.<field>)"]); also min, max, filter, map'
        )
    elif reference.get("retained") is True and reference.get("id") is not None:
        # The caller asked to keep the answer: its id, for filter/map/aggregate.
        lines.append(
            f"kept: filter/map/aggregate(input={{\"ref\": {safe_json(reference['id'])}}})"
        )
    elif reference.get("retained") is False and cut_line is None:
        lines.append(f"not kept: {token_text(reference.get('reason'))}")
    error = live.get("error")
    if isinstance(error, dict):
        # Only the reply budget's reserve carries both: it is sized to hold
        # the digest form and the error form, in text as in JSON.
        key_lines, key_blocks = _key_lines(error)
        lines += key_lines + key_blocks
    altered = live.get("neutralised_keys")
    if isinstance(altered, list) and altered:
        lines.append(
            f"! {len(altered)} upstream field name(s) were rewritten to be safe to print"
        )
    hint = live.get("run_hint")
    if isinstance(hint, dict) and isinstance(hint.get("call"), dict):
        # H3 (#3666, lane ri): built from this session's own calls.
        from okg.substrate.mcp.run_tool import call_text

        lines.append(
            f"! {scalar(hint.get('values'))} calls differ only in "
            f"{scalar(hint.get('argument'))}; one run makes them all (add the "
            f"other values to the list): {call_text(hint['call'])}"
        )
    return lines


def held_rows_lines(
    rows: list[Any], sources: list[str], first: int = 1,
) -> list[str]:
    """Rows a `run` step holds (#3666, lane ri), printed as live rows are:
    one line each, inside an untrusted-data fence per run of rows from one
    source (a live relation, a values step). `rows` are already wrapped
    (``_fence_payload``); each value prints its inner text."""
    lines: list[str] = []
    current: Optional[str] = None
    for index, (row, source) in enumerate(zip(rows, sources), first):
        if source != current:
            if current is not None:
                lines.append(FENCE_CLOSE)
            lines.append(f"{FENCE} source={_marker_source_text(source)}⟧")
            current = source
        if isinstance(row, dict):
            parts = _live_row_parts(row, value_chars=LIVE_VALUE_CHARS, keep_fields=None)
            body = SEP.join(f"{label} {text}" for label, text in parts)
        else:
            body = _live_value(row, LIVE_VALUE_CHARS)
        lines.append(f"[{index}] {body}".rstrip())
    if current is not None:
        lines.append(FENCE_CLOSE)
    return lines


def held_groups_lines(
    groups: list[dict[str, Any]], by: list[str], metrics: list[str], source: str,
) -> list[str]:
    """An aggregate's groups over a `run` step's rows (#3666, lane ri): one
    line each, the group's key values first, then its metrics, in the order
    asked. Key values are wrapped (``_fence_payload``) and print their inner
    text inside one fence; ``(absent)`` is the server's word."""
    lines = [f"{FENCE} source={_marker_source_text(source)}⟧"]
    for index, group in enumerate(groups, 1):
        parts = [
            f"{_neutralise_markers(name)} {_live_value(group.get(name), LIVE_KEY_CHARS)}"
            for name in by
        ]
        parts += [f"{name} {scalar(group.get(name))}" for name in metrics]
        lines.append(f"[{index}] " + SEP.join(parts))
    lines.append(FENCE_CLOSE)
    return lines


def _marker_source_text(source: str) -> str:
    """A fence opener's source label, as ``provenance._marker_source`` writes it."""
    return (
        source.replace("\r", " ").replace("\n", " ")
        .replace("⟦", "[").replace("⟧", "]")
    )


def _record_text_lines(
    payload: dict[str, Any],
    record: dict[str, Any],
    session_state: Optional[dict[str, Any]],
    call: Optional[str],
) -> list[str]:
    """inspect of a ticket (#3666): its fields once, the description once,
    then its dated comments in order; a cut thread says how many are left
    and the call that reads them."""
    node_id = payload.get("node_id")
    parts = _LineParts()
    fields = record.get("fields")
    title_value = record.get("title")
    if isinstance(fields, dict):
        # The summary is the title; the description prints once, below.
        fields = {k: v for k, v in fields.items() if k != "description"}
        if title_value is None and isinstance(fields.get("summary"), str):
            title_value = fields.pop("summary")
    facts = _head_fields(parts, fields)
    title = _head_text(parts, "title", title_value, quote=True)
    if title is not None:
        facts.append(title)
    lines: list[str] = []
    if facts or parts.facts:
        lines.append(SEP.join([ident_text(node_id), *facts, *parts.facts]))
    description = record.get("description")
    if isinstance(description, str) and description.strip():
        lines.append("description:")
        lines += _indented_text(description, "  ")
    comments = [c for c in record.get("comments") or [] if isinstance(c, dict)]
    total = _int(record.get("comments_total"))
    total = total if total is not None else len(comments)
    if comments or total:
        lines.append(f"comments ({total}):")
    for comment in comments:
        who = " ".join(
            _who(value) for value in (comment.get("date"), comment.get("author"))
            if isinstance(value, str) and value
        )
        text = comment.get("text")
        raw = (text if isinstance(text, str) else safe_json(text)).strip().splitlines()
        first = raw[0].strip() if raw else ""
        # The first line follows the date and author; the rest is indented.
        lines.append(_escape_row_line(f"  {who}: {first}" if who else f"  {first}"))
        lines += _indented_text("\n".join(raw[1:]), "    ")
    shown = _int(record.get("comments_shown"))
    shown = shown if shown is not None else len(comments)
    left = _int(record.get("comments_left"))
    if left is not None and left > 0 and shown >= total:
        total = shown + left
    if shown < total:
        read = _call_text(record.get("read_rest")) or _call_text({
            "tool": "inspect", "arguments": {"target": {"kind": "text", "node_id": node_id}},
        })
        lines.append(
            f"! {shown} of {total} comments shown"
            + (f"; read the rest with {read}" if read is not None else "")
        )
    elif record.get("whole") is not False:
        _mark_served(session_state, node_id, call, "", whole=_WHOLE_TEXT)
    return lines


def _text_read_whole(payload: dict[str, Any]) -> bool:
    """Whether an inspect(text) page printed its record's whole text: one
    complete page holding line 1 to the last line, read with no line
    ranges."""
    block = _line_block(payload)
    return (
        block is not None and "requested" not in block and "served" not in block
        and block.get("cut") is None and block.get("first") == 1
        and block.get("last") == block["total"] and _ending(payload) == ["complete"]
    )


#: inspect kinds whose reply says its size on its status line.
_SIZED_INSPECT_KINDS = frozenset({"node", "text", "nodes", "texts"})


def size_facts_for(
    payload: Any, *, tool: str, kind: Optional[str], raised: bool = False,
) -> Optional[Callable[[int], dict[str, Any]]]:
    """The size facts a reply says (PACT read-reply-size-3666), as a
    function of its served size, or None for a reply that says none: a
    refusal or error, and every tool other than search and inspect of a
    node, a text or a batch of either. The text and JSON forms both use it."""
    from okg.substrate.mcp.reply_size import inspect_size_facts, search_size_facts

    if not isinstance(payload, dict):
        return None
    if payload.get("status") == "refused" or (raised and payload.get("error")):
        return None
    if tool == "search":
        rows_key = _rows_key(payload, "search")
        rows = payload.get(rows_key) if rows_key else None
        rows = rows if isinstance(rows, list) else []
        return lambda size: search_size_facts(payload, rows, size)
    if tool == "inspect":
        if payload.get("result_kind") == "text_batch":
            kind = "texts"
        elif payload.get("result_kind") == "node_batch":
            kind = "nodes"
        elif kind is None:
            kind = infer_inspect_kind(payload)
        if kind in _SIZED_INSPECT_KINDS:
            return lambda size: inspect_size_facts(payload, kind, size)
    return None


def render(
    payload: Any,
    *,
    tool: str,
    kind: Optional[str] = None,
    session_state: Optional[dict[str, Any]] = None,
    raised: bool = False,
    select: Any = None,
    size_facts: bool = True,
) -> str:
    """Render one tool result dictionary as text (:func:`_render`), the
    reply's size and the knobs that change it on its status line (PACT
    read-reply-size-3666, :func:`size_facts_for`). `size_facts=False` is
    for a caller that does not serve this text as an MCP reply (the `okg`
    shell): its size and the MCP call knobs would describe a reply it never
    prints."""
    from okg.substrate.mcp.reply_size import with_size_facts

    text = _render(
        payload, tool=tool, kind=kind, session_state=session_state,
        raised=raised, select=select,
    )
    if not size_facts:
        return text
    facts_for = size_facts_for(
        payload, tool=tool if _TOOL_TOKEN.fullmatch(tool or "") else "result",
        kind=kind, raised=raised,
    )
    return text if facts_for is None else with_size_facts(text, facts_for, _before_ending)


#: A count fact: `1-10 of 47`, `rows 1-5, more follow` (`_counts`).
_COUNT_FACT = re.compile(r"(rows )?\d[\d,]*-\d[\d,]*( of |, more follow)")


def _ending_fact(fact: str) -> bool:
    """Whether a status-line fact is part of the page's closing run: the
    counts and the ending (design `read-model` 3.5 and the search forms of
    #3666), which programs read together from the end of the line: a count,
    `retrieval ...`, `listing ...`, `complete`, `completion unknown`,
    `bound reached (...)`, `next: ...`, `N records, all shown` and `no
    records match`."""
    return (
        fact in ("complete", "completion unknown")
        or fact.startswith(("bound reached", "next: ", "retrieval ", "listing "))
        or fact.endswith(", all shown")
        or (fact.startswith("no ") and fact.endswith(" match"))
        or _COUNT_FACT.match(fact) is not None
    )


def _before_ending(line: str) -> tuple[str, str]:
    """A status line cut before its closing run of counts and ending, so a
    fact added between the two parts leaves that run last (PACT
    read-reply-size-3666)."""
    parts = _split_facts(line)
    # Everything from `bound reached` on is the ending, the facts a cut page
    # adds between it and `next:` (`chars left`, `max_bytes N reads the
    # rest`) included.
    cut = next(
        (i for i, part in enumerate(parts) if i and part.startswith("bound reached")),
        len(parts),
    )
    while cut > 1 and _ending_fact(parts[cut - 1]):
        cut -= 1
    return SEP.join(parts[:cut]), SEP.join(parts[cut:])


def _render(
    payload: Any,
    *,
    tool: str,
    kind: Optional[str] = None,
    session_state: Optional[dict[str, Any]] = None,
    raised: bool = False,
    select: Any = None,
) -> str:
    """Render one tool result dictionary as text.

    ``tool`` is the registered tool name; ``kind`` the ``inspect`` target
    kind. ``session_state`` carries the stale-source notice state of one
    logical session (the notice prints once per session, and again when the
    stale set changes); ``None`` prints it on every abnormal result, which is
    also how every byte measurement renders. ``raised`` marks a raised error
    with no payload status, which prints as ``error``. ``select`` is the
    call's ``select`` names (default: the current call's, from
    :class:`ResultContext`); a search row's :data:`SELECT_ONLY_ROW_FIELDS`
    print only when named.
    """

    selected = _selected_names(
        select if select is not None else current_result_context().select
    )
    tool = tool if _TOOL_TOKEN.fullmatch(tool or "") else "result"
    # How a later reply names this one when it repeats a record (#3666).
    call = _session_call(session_state, tool, payload)
    if isinstance(session_state, dict) and isinstance(payload, dict):
        # The generation this reply was read at: `shown earlier` holds only
        # within one generation (PACT read-reply-size-3666, review M2).
        session_state[_READING_GENERATION] = _generation_of(payload)
    if not isinstance(payload, dict):
        body = payload if isinstance(payload, str) else safe_json(payload)
        return f"{tool} ok{SEP}complete\n{escape_body(body)}"
    if payload.get("status") == "refused" or (raised and payload.get("error")):
        return _error_lines(
            _without_unsaid(payload, tool, kind), tool, raised=raised,
            session_state=session_state,
        )
    if tool == "search" and payload.get("view") != "windows":
        # #3666 (read-passage-rows): the short status line and passage rows.
        # `view="windows"` (every match, window by window, with its counts)
        # keeps its full form below: it is the lossless walk, and its text
        # carries what its JSON carries.
        return _render_search(
            _without_unsaid(payload, tool, kind), session_state, payload.get("query"),
            call, raised=raised, selected=selected,
        )
    if tool == "expand" and payload.get("result_kind") == "live_page" and isinstance(
        payload.get("external_live"), dict,
    ):
        # #3666 (Live build 3): a `next` page of a cut live answer.
        live = payload["external_live"]
        return "\n".join([
            SEP.join(["expand ok", *_live_status_facts(live)]),
            *_live_lines(payload.get("node_id"), live),
        ])
    if tool == "inspect" and (kind == "texts" or payload.get("result_kind") == "text_batch"):
        return _render_text_batch(_without_unsaid(payload, tool, kind), session_state)
    if tool == "inspect" and (kind == "nodes" or payload.get("result_kind") == "node_batch"):
        return _render_node_batch(_without_unsaid(payload, tool, kind), session_state, select)
    if tool == "inspect" and kind not in ("node", "text"):
        kind = infer_inspect_kind(payload) if kind is None else kind
    payload = _without_unsaid(payload, tool, kind)
    shown = _Shown()
    shown.rows_key = _rows_key(
        payload, "inspect:text" if (tool == "inspect" and kind == "text") else tool,
    )
    node_shape = tool == "inspect" and kind == "node"
    text_shape = tool == "inspect" and kind == "text"
    if node_shape:
        # A node has no rows; any list it carries is printed as a field.
        shown.rows_key = None
    rows = payload[shown.rows_key] if shown.rows_key else []
    generation = _generation_of(payload)
    facts: list[str] = []
    if node_shape:
        facts.append(
            f"{token_text(payload.get('node_id'))} ({token_text(payload.get('subtype'))})"
        )
        # PACT read-names-list-3666: the id a reader cites, beside the id.
        facts += _read_citation_facts(payload, shown)
    elif text_shape:
        facts.append(
            f"text of {token_text(payload.get('node_id'))} ({token_text(payload.get('subtype'))})"
        )
        # PACT read-names-list-3666: the id a reader cites, beside the id.
        facts += _read_citation_facts(payload, shown)
        facts.append(f"{len(rows)} segment" + ("" if len(rows) == 1 else "s"))
        line_block = _line_block(payload)
        line_fact = _line_fact(line_block) if line_block is not None else None
        if line_fact is not None:
            # #3666: whose lines, the lines on this page, the last line.
            facts.append(line_fact)
    elif tool == "expand":
        facts.append(
            f"{token_text(payload.get('node_id'))} {token_text(payload.get('direction'))}"
        )
    if payload.get("method") is not None and not is_block(payload["method"]):
        facts.append(token_text(payload["method"]))
        shown.keys.add("method")
    if tool == "search":
        if payload.get("view") in ("list", "metadata"):
            facts.append(str(payload["view"]))
            shown.keys.add("view")
        if payload.get("result_unit") == "passage":
            facts.append("passages")
            shown.keys.add("result_unit")
        default_rank = "records" if payload.get("method") == "lexical" else "self"
        if payload.get("rank_by") not in (None, default_rank) and not is_block(payload["rank_by"]):
            facts.append(f"rank {token_text(payload['rank_by'])}")
            shown.keys.add("rank_by")
        # The applied predicate, said once (read-model-s3-where-scope's
        # `where_applied`, else the disclosed scope): a filter that did not
        # apply shows by its absence here.
        where, where_keys = _where_fact(
            payload, profile_said=_profile_scope_said(payload, session_state),
        )
        if where is not None:
            facts.append(where)
            shown.keys.update(where_keys)
    else:
        where_keys = set()
    if not (node_shape or text_shape) and (
        tool in ("search", "expand", "filter", "map", "aggregate", "query")
        or (shown.rows_key and payload.get("total_count") is not None)
    ):
        count = (
            _metadata_counts(payload) if tool == "search" and _metadata(payload)
            else _counts(payload, len(rows))
        )
        if count:
            facts.append(count)
            # A bounded page's count fact says no total (issue #3666).
            shown.total = (
                payload.get("total_count") is not None and _not_a_total(payload) is None
            )
            shown.offset = bool(rows)
    checked = _where_checked_fact(payload) if tool == "search" else None
    if checked is not None:
        facts.append(checked)
        shown.keys.add("where_checked_on")
    basis = payload.get("total_count_basis")
    if basis == "unknown" and any(
        fact.endswith((" of count unknown", f" of {_COUNT_NOT_IN_TIME}", ", not a total"))
            for fact in facts
    ):
        shown.keys.add("total_count_basis")
    elif basis not in (None, "exact", "lower_bound"):
        facts.append(f"count {scalar(basis)}")
        shown.keys.add("total_count_basis")
    coverage = payload.get("coverage")
    # The `+` on the count is these records; `undecided_records` lists them
    # (PACT read-record-windows). It is said on every page, the last one too,
    # so the ending's `complete` (delivery: every selected record was sent)
    # cannot read as every record decided.
    undecided = _undecided(payload)
    if undecided is not None:
        facts.append(f"{undecided} undecided")
    if isinstance(coverage, dict) and "retrieval" in coverage:
        # #3666 (read-passage-rows): the retrieval status, the count set and
        # the bounds are internals the reader does not act on; the count
        # fact, the ending and the notices say what the page holds and what
        # continues it. The JSON form carries `coverage` whole. Two facts
        # stay, because each limits what the rows claim.
        retrieval = coverage["retrieval"]
        members = payload.get("reference_members")
        if isinstance(members, int) and not isinstance(members, bool):
            # Exhaustive over a result reference means "all of its rows",
            # which reads as "all of the graph": say the scope instead.
            facts.append(f"over the reference's {members} row(s) only")
        elif (
            isinstance(retrieval, dict)
            and retrieval.get("status") == "unknown"
            and payload.get("total_count_reason") == "count_not_computed_in_time"
        ):
            # What is true of this page: its rows are the matches in order
            # up to here; how many more there are was not counted.
            facts.append("retrieval exact for these rows, more not counted")
        elif isinstance(retrieval, dict) and retrieval.get("status") not in (None, "exhaustive"):
            # A bounded or approximate retrieval qualifies `complete`.
            facts.append(f"retrieval {scalar(retrieval['status'])}")
        shown.keys.add("coverage")
    phrase = payload.get("phrase") if tool == "search" else None
    if isinstance(phrase, dict):
        # A quoted lexical search none of whose checked candidates holds every
        # span (PACT lexical-quoted-phrases-3064). `matched` is the normal
        # state and adds no fact: each card says `exact phrase` or not, and
        # the JSON carries the block.
        if phrase.get("status") == "no_exact_match":
            facts.append("phrase=no_exact_match")
        elif phrase.get("status") != "matched":
            facts.append(f"phrase={scalar(phrase.get('status'))}")
        shown.keys.add("phrase")
    # The page ending (PACT read-model-s6-page-next): `complete`, or
    # `bound reached (<what>) · next: <offset=n | token | none>`. Only
    # inspect(graph) adds `generation` after it, last: the frozen grammar
    # puts it there and `result_generation` reads it by position.
    listing_fact = _listing_fact(payload) if tool == "search" else None
    if listing_fact is not None:
        facts.append(listing_fact)
    ending = _ending(payload)
    facts += ending
    if generation and tool == "inspect" and kind == "graph":
        # PACT read-passage-rows-3666 (D8): no read reply names its
        # generation; inspect(graph) is how a reader asks for it (and for
        # the publish freshness block, printed whole below).
        facts.append(f"generation {scalar(generation)}")
    shown.paging = len(ending) > 1 and not ending[-1].endswith(": none")
    if tool == "expand" and isinstance(payload.get("external_live"), dict):
        # #3666: the live relation and its rows, after the graph page's own
        # facts (`rows 1-k of N`, or `N rows, all shown`).
        facts += _live_status_facts(payload["external_live"])
    shown.keys.update({"truncated", "complete", "text_left"})
    lines = [SEP.join([f"{tool} {_status_word(payload, raised=raised)}", *facts])]
    reference_line = _reference_line(payload)
    if reference_line is not None:
        lines.append(reference_line)
        shown.keys.add("result_reference")
        if _search_next(payload):
            shown.keys.add("search_next")
    lines += _notices(payload, session_state)
    if payload.get("completeness") in ("bounded", "exact", "exact_over_pages"):
        # The requested mode: said by the retrieval fact, the recovery
        # notice and the paging fact.
        shown.keys.add("completeness")
    start = (_int(payload.get("offset")) or 0) + 1
    consumed: set[str] = set(where_keys)
    if listing_fact is not None:
        consumed.add("listing")
    if tool == "search":
        # Only `view="windows"` reaches here (the others: `_render_search`).
        for index, row in enumerate(rows, start):
            lines.append("")
            lines += (
                _window_record_card(index, row)
                if isinstance(row, dict) else [f"[{index}] {_cell(row)}"]
            )
    elif (node_shape or text_shape) and isinstance(payload.get("record_text"), dict):
        # #3666 (read-passage-rows): a ticket-shaped record reads as its
        # fields once, its description once and its dated comments; the
        # attributes and the chunk segments stay in the JSON form.
        parts = _Parts()
        _row_trust(parts, payload, payload)
        if parts.facts:
            lines.append(SEP.join(parts.facts))
        lines += _record_text_lines(payload, payload["record_text"], session_state, call)
        consumed |= {
            "node_id", "subtype", "provenance", "attrs", "record_text", "order_basis",
            "coverage", "lines", "text_left",
        }
        if shown.rows_key:
            consumed.add(shown.rows_key)
    elif node_shape:
        parts = _Parts()
        _trust_fact(parts, payload, payload, has_text=True)
        if parts.facts:
            lines.append(SEP.join(parts.facts))
        lines += parts.blocks
        not_stored = _not_stored_notice(payload)
        if not_stored is not None:
            lines.append(not_stored)
            consumed |= {"attrs_not_stored", "attr_keys"}
        attrs = payload.get("attrs")
        key_lines, key_blocks = _key_lines(attrs if isinstance(attrs, dict) else {})
        lines += key_lines + key_blocks
        consumed |= {"node_id", "subtype", "provenance"}
        if isinstance(attrs, dict) or attrs is None:
            consumed.add("attrs")
        if isinstance(attrs, dict) and not selected and not any(
            payload.get(key) is not None for key in NODE_CUT_KEYS
        ):
            # PACT read-reply-size-3666: every attribute printed, none cut:
            # a later search row read as this node says so, not its passage.
            _mark_served(session_state, payload.get("node_id"), call, "", whole=_WHOLE_NODE)
    elif text_shape:
        lines += _text_segment_lines(rows, payload)
        if _text_read_whole(payload):
            # PACT read-reply-size-3666: the record's whole text, line 1 to
            # its last, on one complete page.
            _mark_served(session_state, payload.get("node_id"), call, "", whole=_WHOLE_TEXT)
        consumed |= {"node_id", "subtype", "order_basis"}
        if payload.get("coverage") in (None, "unknown"):
            consumed.add("coverage")
        if _line_block(payload) is not None and (
            _line_fact(payload["lines"]) is not None or not rows
        ):
            # Said by the status line and the `! lines:` notices; the asked
            # ranges are the call's own argument.
            consumed.add("lines")
    elif tool == "expand":
        node = payload.get("node_id")
        # A selected `attrs.<key>` no row on the page stores (issue #3666,
        # lane RD): said, so rows without it never read as the answer.
        unknown = _unknown_selected_notice(rows, sorted(selected))
        if unknown is not None:
            lines.append(unknown)
        for index, row in enumerate(rows, start):
            if not isinstance(row, dict):
                lines.append(f"[{index}] {_cell(row)}")
                continue
            if isinstance(row.get("link"), dict) and "node_id" in row:
                # Issue #3666: a 1-hop row is the linked record.
                lines += _linked_record_lines(index, row, payload)
                continue
            parts = _Parts()
            if row.get("src") == node:
                head = f"[{index}] {scalar(row.get('edge_type'))} → {scalar(row.get('dst'))}"
            elif row.get("dst") == node:
                head = f"[{index}] {scalar(row.get('edge_type'))} ← {scalar(row.get('src'))}"
            else:
                head = (
                    f"[{index}] {scalar(row.get('edge_type'))} "
                    f"{scalar(row.get('src'))} → {scalar(row.get('dst'))}"
                )
            neighbor = row.get("neighbor")
            neighbor_title = None
            if isinstance(neighbor, dict):
                # The far end's type and title where an agent scans, so a
                # link reads as something to follow (#2675, #3072). A fenced
                # or long title never goes on the line: it is a block, as a
                # parent card's title is (`_parent_bits`).
                head += f" [{scalar(neighbor.get('subtype'))}]"
                title = neighbor.get("title")
                if title is not None and is_block(title):
                    neighbor_title = title
                elif title is not None:
                    head += " " + safe_json(title)
            parts.facts.append(f"edge {scalar(row.get('edge_id'))}")
            if neighbor_title is not None:
                parts.block("neighbor title", neighbor_title)
            _trust_fact(parts, row, payload, has_text=bool(row.get("attrs")))
            extra = {
                k: v for k, v in row.items()
                if k not in (
                    "edge_id", "src", "dst", "edge_type", "attrs",
                    "read_provenance", "provenance", "direction",
                    *(("neighbor",) if neighbor is None or isinstance(neighbor, dict) else ()),
                )
            }
            if (
                row.get("direction") not in (None, payload.get("direction"))
                and payload.get("direction") != "both"
            ):
                extra["direction"] = row["direction"]
            for key, value in extra.items():
                parts.fact(key_text(key), value)
            attrs = row.get("attrs")
            if attrs is not None and not isinstance(attrs, dict):
                parts.fact("attrs", attrs)
            key_lines, key_blocks = _key_lines(
                attrs if isinstance(attrs, dict) else {}, indent="  ",
            )
            lines += [SEP.join([head, *parts.facts]), *parts.blocks, *key_lines, *key_blocks]
        consumed |= {"node_id", "direction", "external_live"}
        if isinstance(payload.get("external_live"), dict):
            # #3666: one line per live row, never a JSON dump.
            lines += _live_lines(node, payload["external_live"])
        elif isinstance(payload.get("external_live"), list):
            lines += _block_lines("external_live", payload["external_live"])
        elif payload.get("external_live") is not None:
            consumed.discard("external_live")
    elif tool in ("aggregate", "query"):
        lines += _table_rows(tool, rows, payload, start, shown)
        if tool == "aggregate":
            consumed |= {"by", "metrics"}
    elif tool in ("filter", "map"):
        # PACT read-reply-size-3666: filter rows say `shown earlier` as
        # search rows do; a map row is a projection the caller chose.
        lines += _generic_rows(
            rows, payload, start,
            session_state=session_state if tool == "filter" else None, call=call,
        )
    else:
        # The generic shape: the answer first, every unplaced field as a key
        # line, then the rows.
        body = {k: v for k, v in payload.items() if k not in _PLACED_TOP}
        if tool == "inspect" and kind == "graph" and "schema" in body:
            body["schema"] = _graph_schema_without_repeats(payload, body["schema"])
        if payload.get("answer") is not None:
            body = {"answer": payload["answer"], **body}
            shown.keys.add("answer")
        key_lines, key_blocks = _key_lines(body)
        lines += key_lines + key_blocks
        lines += _generic_rows(rows, payload, start) if shown.rows_key else []
        consumed |= set(body)
    # A cut node's cut notes print as `!` lines (`_node_cut_notices`).
    consumed |= {key for key in NODE_CUT_KEYS if isinstance(payload.get(key), dict)}
    rest = {
        k: v for k, v in payload.items()
        if k not in _PLACED_TOP and k not in consumed
    }
    rest.update(_placed_leftovers(
        payload, tool=tool, kind=kind, shown=shown, generation=generation,
        selected=selected,
    ))
    if tool == "query" and "query_plan_summary" in rest:
        lines.append(f"query_plan_summary: {_inline(rest.pop('query_plan_summary'))}")
    if rest:
        rest_lines, rest_blocks = _key_lines(rest)
        lines += ["--- other fields", *rest_lines, *rest_blocks]
    return "\n".join(lines)


def _graph_schema_without_repeats(payload: dict[str, Any], schema: Any) -> Any:
    """inspect(graph)'s `schema` block minus what the page already printed.

    The minimal digest rebuilds the page's own count maps (`subtypes` from
    `subtype_counts`; `edge_types` and `edge_type_counts` from
    `edge_type_counts`) and repeats its deployment and pin, so the text
    printed subtype counts twice and edge counts three times (#2674). A key
    is dropped only when it equals what the page carries; the JSON form is
    unchanged.
    """
    if not isinstance(schema, dict) or schema.get("minimal") is not True:
        return schema
    subtype_counts = payload.get("subtype_counts")
    edge_counts = payload.get("edge_type_counts")
    # inspect(graph) names them `deployment_name` and `generation_id`.
    repeats = {
        "deployment": payload.get("deployment_name", payload.get("deployment")),
        "pinned_generation_id": payload.get(
            "pinned_generation_id", payload.get("generation_id"),
        ),
    }
    if isinstance(subtype_counts, dict):
        repeats["subtype_count"] = len(subtype_counts)
        repeats["subtypes"] = {
            name: {"node_count": count} for name, count in subtype_counts.items()
        }
    if isinstance(edge_counts, dict):
        repeats["edge_type_count"] = len(edge_counts)
        repeats["edge_types"] = sorted(edge_counts)
        repeats["edge_type_counts"] = edge_counts
    return {
        key: value for key, value in schema.items()
        if not (key in repeats and repeats[key] is not None and value == repeats[key])
    }


def _split_facts(line: str) -> list[str]:
    """A status line split on `` · ``, outside JSON strings.

    A value that holds the separator is always printed as a JSON string
    (:func:`needs_quote`), so a separator inside quotes is part of a value.
    """

    parts: list[str] = []
    current: list[str] = []
    index, quoted = 0, False
    while index < len(line):
        c = line[index]
        if quoted:
            current.append(c)
            if c == "\\" and index + 1 < len(line):
                current.append(line[index + 1])
                index += 2
                continue
            if c == '"':
                quoted = False
        elif c == '"':
            quoted = True
            current.append(c)
        elif line.startswith(SEP, index):
            parts.append("".join(current))
            current = []
            index += len(SEP)
            continue
        else:
            current.append(c)
        index += 1
    parts.append("".join(current))
    return parts


def _value_at(text: str) -> tuple[Any, str]:
    """Read one typed value from the start of ``text``: a JSON string, or a
    bare token up to the next space. Returns the value and the remainder."""

    if text.startswith('"'):
        value, end = json.JSONDecoder().raw_decode(text)
        return value, text[end:]
    token, _, rest = text.partition(" ")
    return token, (" " + rest if rest else "")


def _typed(token: Any) -> Any:
    if not isinstance(token, str):
        return token
    try:
        return json.loads(token)
    except ValueError:
        return token


def parse_status_line(text: Any) -> Optional[StatusLine]:
    """The status line of a text result, or ``None`` when there is none.

    ``None`` means the text is not a rendered okg result: a write or admin
    tool's raised exception (the SDK's ``str(exc)``), a vendor error, or
    anything else. Callers count ``None`` as a failure. A status value or
    code that holds a space or the separator is a JSON string, and is read
    as one.
    """

    if not isinstance(text, str) or not text:
        return None
    first = text.split("\n", 1)[0]
    head = _split_facts(first)[0]
    tool, _, rest = head.partition(" ")
    if not rest or not _TOOL_TOKEN.fullmatch(tool):
        return None
    if rest == "ok":
        return StatusLine(tool, "ok")
    word, _, tail = rest.partition(" ")
    if word in ("refused", "error"):
        code: Any = None
        if tail:
            try:
                value, remainder = _value_at(tail)
            except ValueError:
                return None
            code = tail if remainder.strip() else value
        return StatusLine(tool, word, None if code == "null" else code)
    if rest.startswith("status="):
        # `<tool> status=<value> [<code>]`: a copied payload status, and the
        # error code when a raised error carried one (review N1).
        try:
            value, remainder = _value_at(rest[len("status="):])
        except ValueError:
            return None
        if remainder and not remainder.startswith(" "):
            return None
        code = remainder.strip() or None
        if code is not None and code.startswith('"'):
            try:
                code = json.loads(code)
            except ValueError:
                pass
        return StatusLine(tool, "status", code, value)
    return None


def status_facts(text: Any) -> list[str]:
    """The facts of a text result's status line, after the status word.

    Split on `` · `` outside JSON strings. Empty when there is no status
    line (:func:`parse_status_line` returns ``None``).
    """

    if parse_status_line(text) is None:
        return []
    return _split_facts(text.split("\n", 1)[0])[1:]


def fact_value(facts: list[str], label: str, *, last: bool = False) -> Any:
    """The typed value of the first (or, with ``last``, the last)
    ``<label> <value>`` or ``<label>=<value>`` fact, or ``None`` when no
    fact has that label."""

    for fact in (reversed(facts) if last else facts):
        for sep in (" ", "="):
            if fact.startswith(label + sep):
                text = fact[len(label) + 1:]
                if not text.startswith('"'):
                    # A bare value is the rest of the fact (it may hold
                    # ASCII spaces); one that parses as JSON is that value.
                    return _typed(text)
                try:
                    value, _remainder = _value_at(text)
                except ValueError:
                    return None
                return value
    return None


def result_generation(text: Any) -> Optional[str]:
    """The ``generation <id>`` fact of a text result's status line.

    The server writes the generation as the LAST fact, so the last match is
    read: a subject is quoted when it holds a space, but reading by
    position keeps a value shaped like ``generation x`` from ever winning.
    """

    value = fact_value(status_facts(text), "generation", last=True)
    return value if isinstance(value, str) else None


class Card(NamedTuple):
    """One numbered row header of a text result: ``[n] <id> ... · facts``."""

    index: str
    ident: Any
    #: The `(<subtype>)` after the id, typed, or None when the head has none.
    kind: Any
    head: str
    facts: tuple[str, ...]


_CARD_HEAD = re.compile(r"\[(\d+(?:\.\d+)*)\] ")


def result_cards(text: Any) -> list[Card]:
    """Every row header line of a text result, in order.

    A row header is a line the renderer built: it starts ``[n] `` with no
    escape. Source text that looks like one is escaped with a backslash
    (:func:`escape_body`), so it never matches. ``ident`` is the row's
    first value (a node id, an edge type, a chunk id), typed.
    """

    if parse_status_line(text) is None:
        return []
    cards: list[Card] = []
    for line in text.split("\n")[1:]:
        match = _CARD_HEAD.match(line)
        if match is None:
            continue
        parts = _split_facts(line)
        head = parts[0][match.end():]
        kind: Any = None
        if head.startswith('"'):
            try:
                ident, remainder = _value_at(head)
            except ValueError:
                ident, remainder = None, ""
        else:
            # A bare id may hold ASCII spaces; the `(<subtype>)` is last.
            cut = head.rfind(" (") if head.endswith(")") else -1
            ident = _typed(head[:cut] if cut >= 0 else head)
            remainder = head[cut:] if cut >= 0 else ""
        if remainder.startswith(" (") and remainder.endswith(")"):
            inner = remainder[2:-1]
            try:
                kind = _value_at(inner)[0] if inner.startswith('"') else _typed(inner)
            except ValueError:
                kind = inner
        cards.append(Card(match.group(1), ident, kind, parts[0], tuple(parts[1:])))
    return cards


def classify_status_line(line: Optional[StatusLine]) -> str:
    """``answer``, ``refused`` or ``failed``, by okg's one classifier rule.

    ``ok`` is an answer; ``refused`` and ``error`` are refusals (the graph
    was reached and said no); a copied status is failed when it is ``error``
    or ``failed`` and an answer otherwise; no status line is failed.
    """

    if line is None:
        return "failed"
    if line.status == "ok":
        return "answer"
    if line.status in ("refused", "error"):
        return "refused"
    value = str(line.value or "").strip().lower()
    return "failed" if value in ("error", "failed") else "answer"


def first_text_block(content: Any) -> Optional[str]:
    """The text of the first ``text`` item of a result's content list."""

    for item in content or ():
        if isinstance(item, dict):
            if item.get("type") == "text" and isinstance(item.get("text"), str):
                return item["text"]
            continue
        if getattr(item, "type", None) == "text" and isinstance(getattr(item, "text", None), str):
            return item.text
    return None
