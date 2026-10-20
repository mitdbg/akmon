"""The `search(method="grep")` query language and its index rule (#3067).

PACT mcp-grep-3067; design: `pact/changes/mcp-substring-search-3067/DESIGN.md`
sections 4(c) and 11 (decisions 168a-170b). Pure Python, no database: every
refusal here happens before any SQL runs.

A query is a list of terms separated by whitespace, AND-ed within one text
unit (one node's `attrs.text`), all case-insensitive:

- ``word``: a substring (``uuid`` matches ``uuid4``); a run of ``_``,
  ``-``, ``.`` or space between two letters or digits matches any such run
  (``keep_value`` finds ``Keep-Value`` and ``keep value``; #3666 R5); a
  word with ``|`` between other characters is refused, since ``a|b`` is
  meant as either word: write it as a regex, or quote it for the literal
  text (#3183);
- ``"a quoted phrase"``: its words in order, any run of whitespace between them;
- ``a NEAR/80 b``: both literals within 80 characters, either order (the SQL
  also ANDs each operand alone, so the trigram index narrows it; PACT
  grep-near-operands-3251);
- ``-term``: excludes rows the term matches;
- a regular expression written as :data:`REGEX_TERM_SYNTAX`;
- ``path:<substring>`` (or ``path:"a b"``): a case-insensitive substring of
  the record's ``attrs.path`` -- the text unit's own, or its declared
  record's -- not of its text (PACT mcp-grep-rank). It ANDs with the other
  terms and takes ``-`` to exclude.

Every term compiles to one PostgreSQL advanced regular expression matched with
``~*``. The same static walk then decides, for all of them, whether the
trigram index can narrow the term: it must contain a run of at least
:data:`MIN_INDEXABLE_RUN` literal letters or digits that every match has to
contain (DESIGN.md section 11, `evidence/rxcheck.py`).
"""
from __future__ import annotations

import re
import warnings
from dataclasses import dataclass, replace
from re import _constants as _C  # type: ignore[attr-defined]
from re import _parser as _P  # type: ignore[attr-defined]
from typing import Optional

from okg.substrate.errors import SubstrateError

__all__ = [
    "GrepPatternTooShort",
    "alternation_words",
    "GrepQuery",
    "GrepQueryInvalid",
    "GrepRegexUnindexable",
    "GrepTerm",
    "MAX_NEAR_DISTANCE",
    "MAX_QUERY_CHARS",
    "MAX_REGEX_CHARS",
    "MAX_TERMS",
    "MIN_INDEXABLE_RUN",
    "PATH_TERM_PREFIX",
    "REGEX_TERM_SYNTAX",
    "assert_indexable_or_scoped",
    "FOLD_CLASS",
    "FOLD_SEPARATORS",
    "filename_shaped",
    "literal_run",
    "own_path_tail_query",
    "parse_grep_query",
    "phrase_pattern",
    "quoted_spans",
    "read_regex_term",
    "spelling_tried",
    "strip_quotes",
]

#: How a regex term is written: the ONE place the syntax lives (confirmed by
#: Jason, 2026-10-01). The pattern runs to the first unescaped closing marker
#: followed by whitespace or the end of the query. Changing this string
#: changes the parser and the refusal messages; a test holds the published
#: `search` description to it.
REGEX_TERM_SYNTAX = "re:/<pattern>/"
_REGEX_OPEN, _REGEX_CLOSE = REGEX_TERM_SYNTAX.split("<pattern>")
#: How a path term starts (PACT mcp-grep-rank): `path:<substring>`.
PATH_TERM_PREFIX = "path:"

#: A regular expression longer than this is refused before it is parsed.
MAX_REGEX_CHARS = 256
#: The whole query, and the number of terms in it.
MAX_QUERY_CHARS = 1024
MAX_TERMS = 16
#: `NEAR/n` compiles to `.{0,n}`; PostgreSQL refuses a count above 255.
MAX_NEAR_DISTANCE = 255
#: Literal letters or digits in a row that make a term indexable.
MIN_INDEXABLE_RUN = 3

_NEAR = re.compile(r"NEAR/(\d+)")
#: Characters a literal escapes to be read literally by a PostgreSQL ARE.
_PG_META = frozenset("\\.^$|?*+()[]{}")
#: Escaped letters both parsers read the same way. Anything else -- `\b`,
#: `\B`, `\x`, back-references, PostgreSQL-only `\m \M \y \Y` -- is refused.
_SHARED_LETTER_ESCAPES = frozenset("dDwWsSAZntrfvauU")
# Python warns about regex syntax it may read differently later (a nested
# set `[[`, a set difference `--`, ...); PostgreSQL's own compile decides
# those patterns, so the warning says nothing to a grep caller. It is
# silenced by one narrow process filter for exactly these messages, never
# per parse with `warnings.catch_warnings()`: that saves and restores the
# whole process filter list, and two parses in concurrent request workers
# (#3493) can restore each other's copies and leave a blanket FutureWarning
# ignore installed for good.
_NESTED_SET_WARNING = r"Possible (nested set|set \w+( \w+)?) at position \d+"


def _silence_nested_set_warnings() -> None:
    """Install the narrow filter unless it is already in place.

    Checked per parse, not only at import: anything that later restores an
    older filter list (a `catch_warnings` block entered before this module
    was imported, a test harness) drops it, and a dropped filter only
    re-surfaces the warning, so re-adding it is all the recovery needed.
    """

    for action, message, category, _module, _lineno in list(warnings.filters):
        if (
            action == "ignore"
            and category is FutureWarning
            and getattr(message, "pattern", None) == _NESTED_SET_WARNING
        ):
            return
    warnings.filterwarnings(
        "ignore", message=_NESTED_SET_WARNING, category=FutureWarning,
    )


_silence_nested_set_warnings()

_DEFAULT_FLAGS = _P.parse("").state.flags


class GrepQueryInvalid(SubstrateError):
    """A grep query the grammar or the regex subset does not accept."""

    code = "grep_query_invalid"


class GrepRegexUnindexable(SubstrateError):
    """A regex term with no literal run the trigram index can use, unscoped."""

    code = "grep_regex_unindexable"


class GrepPatternTooShort(SubstrateError):
    """Every positive literal is shorter than the index can narrow, unscoped."""

    code = "grep_pattern_too_short"


@dataclass(frozen=True)
class GrepTerm:
    """One term: how it was written, its PostgreSQL regex, and its index run."""

    kind: str  # "word" | "phrase" | "near" | "regex" | "path"
    text: str
    pg_regex: str
    negated: bool
    run: int
    #: The literal words a positive term ranks on (BM25, PACT
    #: mcp-grep-rank); empty for a path term and for a regex, except an
    #: alternation of plain literals (PACT mcp-grep-page, `alternation_words`).
    words: tuple[str, ...] = ()
    #: A NEAR term's two operand regexes, exactly as they appear inside
    #: `pg_regex` (PACT grep-near-operands-3251). Every NEAR match contains a
    #: match of each, so the SQL ANDs `~* a AND ~* b` before the NEAR regex:
    #: pg_trgm cannot narrow the two-order alternation, but it narrows each
    #: operand. Empty for every other kind.
    operands: tuple[str, ...] = ()
    #: Whether a separator run inside the literal was folded
    #: (:data:`FOLD_CLASS`); a NEAR term is folded when an operand is.
    folded: bool = False

    @property
    def indexable(self) -> bool:
        return self.run >= MIN_INDEXABLE_RUN

    def disclosure(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "term": self.text,
            "negated": self.negated,
            "indexable": self.indexable,
        }


@dataclass(frozen=True)
class GrepQuery:
    #: Text terms: each matched against a text unit's `attrs.text`.
    terms: tuple[GrepTerm, ...]
    #: Path terms: each matched against the record's `attrs.path`.
    path_terms: tuple[GrepTerm, ...] = ()

    @property
    def positive(self) -> tuple[GrepTerm, ...]:
        return tuple(t for t in self.terms if not t.negated)

    @property
    def negative(self) -> tuple[GrepTerm, ...]:
        return tuple(t for t in self.terms if t.negated)

    @property
    def indexable(self) -> bool:
        """Whether at least one AND-ed positive text term narrows by the index."""
        return any(t.indexable for t in self.positive)

    @property
    def path_scoped(self) -> bool:
        """Whether a positive path term is long enough to count as a scope:
        3 or more literal letters or digits in a row, narrowed by the path
        trigram index."""
        return any(t.indexable for t in self.path_terms if not t.negated)

    @property
    def rank_words(self) -> tuple[str, ...]:
        """The literal words of the positive text terms, in query order and
        without repeats: what BM25 ranks on. Empty for a regex-only query,
        unless a regex is an alternation of plain literals."""
        seen: dict[str, None] = {}
        for term in self.positive:
            for word in term.words:
                seen.setdefault(word, None)
        return tuple(seen)

    def disclosure(self) -> list[dict[str, object]]:
        return [term.disclosure() for term in (*self.terms, *self.path_terms)]


def _literal_alternation(word: str) -> str:
    """`a|b` as a regex alternation of its literal parts: empty parts dropped
    and regex metacharacters escaped, so `c++|java` suggests `c\\+\\+|java`."""
    parts = [part for part in word.split("|") if part]
    return "|".join(re.sub(r"([.^$*+?()\[\]{}\\])", r"\\\1", part) for part in parts)


def _invalid(reason: str, query: str) -> GrepQueryInvalid:
    return GrepQueryInvalid(
        f"grep query refused: {reason}",
        data={"reason": reason, "query": query[:MAX_QUERY_CHARS]},
        fix_hint=(
            "write words, \"quoted phrases\", a NEAR/<n> b, -term to exclude, "
            f"or {REGEX_TERM_SYNTAX}"
        ),
    )


def read_regex_term(query: str, start: int) -> Optional[tuple[str, int]]:
    """`(pattern, end)` when `query[start:]` begins a regex term, else None.

    The only function that knows how a regex term is delimited. The pattern
    ends at the first unescaped closing marker followed by whitespace or the
    end of the query, so a pattern may contain the marker (`re:/a/b/`) and
    escaped characters; `end` is the index just past the closing marker.
    """
    if not query.startswith(_REGEX_OPEN, start):
        return None
    i = start + len(_REGEX_OPEN)
    while i < len(query):
        if query[i] == "\\":
            i += 2
            continue
        if query.startswith(_REGEX_CLOSE, i):
            after = i + len(_REGEX_CLOSE)
            if after == len(query) or query[after].isspace():
                return query[start + len(_REGEX_OPEN): i], after
        i += 1
    raise _invalid(f"unterminated regular expression; write {REGEX_TERM_SYNTAX}", query)


def _escape_literal(text: str) -> str:
    return "".join(f"\\{c}" if c in _PG_META else c for c in text)


def _check_escapes(pattern: str, query: str) -> None:
    i = 0
    while i < len(pattern):
        if pattern[i] != "\\":
            i += 1
            continue
        if i + 1 >= len(pattern):
            raise _invalid("a regular expression ends in a lone backslash", query)
        c = pattern[i + 1]
        if c in "bB":
            raise _invalid(
                "\\b and \\B are not supported: Python reads a word boundary, "
                "PostgreSQL a backspace", query,
            )
        if c.isdigit():
            raise _invalid(
                "back-references and octal escapes (\\1, \\0) are not supported", query,
            )
        if c == "x":
            raise _invalid(
                "\\x escapes are read differently by PostgreSQL; write the "
                "character or a class", query,
            )
        if c.isalpha() and c not in _SHARED_LETTER_ESCAPES:
            raise _invalid(
                f"the escape \\{c} is not supported (PostgreSQL-only escapes "
                "such as \\m, \\M and \\y are refused)", query,
            )
        i += 2


def _walk(items: object, query: str) -> int:
    """The longest literal run every match must contain (rxcheck.py)."""
    best = cur = 0
    for op, av in items:  # type: ignore[attr-defined]
        if op is _C.LITERAL:
            cur = cur + 1 if chr(av).isalnum() else 0
        elif op in (_C.MAX_REPEAT, _C.MIN_REPEAT):
            lo, hi, sub = av
            if lo > MAX_NEAR_DISTANCE or (hi is not _C.MAXREPEAT and hi > MAX_NEAR_DISTANCE):
                raise _invalid("repetition counts above 255 are not supported", query)
            cur = 0
            inner = _walk(sub, query)
            if lo >= 1:
                best = max(best, inner)
        elif op is _C.SUBPATTERN:
            _gid, add, dele, sub = av
            if add or dele:
                raise _invalid("inline flags are not supported", query)
            cur = 0
            best = max(best, _walk(sub, query))
        elif op is _C.BRANCH:
            cur = 0
            best = max(best, min(_walk(branch, query) for branch in av[1]))
        elif op in (_C.GROUPREF, _C.GROUPREF_EXISTS):
            raise _invalid("back-references are not supported", query)
        elif op in (_C.ASSERT, _C.ASSERT_NOT):
            raise _invalid("lookahead and lookbehind are not supported", query)
        elif op in (_C.POSSESSIVE_REPEAT, _C.ATOMIC_GROUP):
            raise _invalid(
                "atomic groups and possessive quantifiers are not supported", query,
            )
        elif op is _C.AT and av in (_C.AT_BOUNDARY, _C.AT_NON_BOUNDARY):
            raise _invalid("\\b and \\B are not supported", query)
        else:
            cur = 0
        best = max(best, cur)
    return best


def alternation_words(pattern: str) -> tuple[str, ...]:
    """The words of a regex that is an alternation of plain literals.

    `re:/rollback|roll back|revert/` gives `("rollback", "roll", "back",
    "revert")`: every match contains one branch's text, so BM25 can order
    the matches on those words (PACT mcp-grep-page). Any other regex, a
    single literal included, gives `()` and keeps node-id order. The words
    only order; the regex still decides every match, and a match no BM25
    word reaches (a branch inside a longer token) scores 0 and follows.
    """
    # Python's parser folds a shared prefix out of the branches, so the
    # pattern text is read directly: plain characters and `|` only.
    if "|" not in pattern or _ALTERNATION.fullmatch(pattern) is None:
        return ()
    words: dict[str, None] = {}
    for branch in pattern.split("|"):
        if not branch.strip():
            return ()
        for word in branch.split():
            words.setdefault(word, None)
    return tuple(words)


#: An alternation of plain literal branches: letters, digits, spaces, `_`
#: and `-` (none is special to either regex engine outside a class).
_ALTERNATION = re.compile(r"[A-Za-z0-9 _\-|]+")


def literal_run(pattern: str, *, query: Optional[str] = None) -> int:
    """Check a regex against the accepted subset; return its literal run.

    Refuses (`grep_query_invalid`, reason named) a pattern over
    `MAX_REGEX_CHARS`, one Python cannot parse, and back-references,
    lookaround, inline flags, named groups, `\\b`/`\\B`, atomic groups,
    possessive quantifiers, repetition counts over 255 and escapes the two
    parsers read differently.
    """
    shown = pattern if query is None else query
    if len(pattern) > MAX_REGEX_CHARS:
        raise _invalid(
            f"a regular expression is longer than {MAX_REGEX_CHARS} characters", shown,
        )
    _check_escapes(pattern, shown)
    try:
        _silence_nested_set_warnings()
        tree = _P.parse(pattern)
    except re.error as exc:
        raise _invalid(f"invalid regular expression: {exc}", shown) from exc
    if tree.state.groupdict:
        raise _invalid("named groups are not supported", shown)
    if tree.state.flags != _DEFAULT_FLAGS:
        raise _invalid("inline flags are not supported", shown)
    return _walk(tree.data, shown)


#: The separators a word folds (#3666 R5): inside a word, a run of these
#: between two letters or digits matches any run of them, so `keep_value`,
#: `keep-value`, `keep.value` and `keep value` find each other. Case is
#: already folded (`~*`). A separator at a word's start or end stays literal.
FOLD_SEPARATORS = "_-. "
#: The class a folded separator run compiles to. Both regex engines read it
#: the same way: `-` first is literal, `.` inside a class is literal.
FOLD_CLASS = "[-_. ]+"


def _folded_literal(text: str) -> tuple[str, bool]:
    """`(pg_regex, folded)`: `text` escaped, each separator run between two
    letters or digits written as :data:`FOLD_CLASS`. `folded` says whether
    any run was."""
    out: list[str] = []
    folded = False
    i, n = 0, len(text)
    while i < n:
        if text[i] in FOLD_SEPARATORS:
            j = i
            while j < n and text[j] in FOLD_SEPARATORS:
                j += 1
            if 0 < i and j < n and text[i - 1].isalnum() and text[j].isalnum():
                out.append(FOLD_CLASS)
                folded = True
            else:
                out.append(_escape_literal(text[i:j]))
            i = j
            continue
        out.append(_escape_literal(text[i]))
        i += 1
    return "".join(out), folded


def _literal_term(text: str, *, kind: str, negated: bool, query: str) -> GrepTerm:
    folded = False
    if kind == "phrase":
        words = tuple(text.split())
        if not words:
            raise _invalid("an empty quoted phrase", query)
        parts = [_folded_literal(w) for w in words]
        pg = r"\s+".join(part for part, _f in parts)
        folded = any(f for _p, f in parts)
        shown = " ".join(words)
    elif kind == "path":
        if not text:
            raise _invalid(
                f"{PATH_TERM_PREFIX} needs a substring: {PATH_TERM_PREFIX}<text>", query,
            )
        # A path term is one literal; it ranks on nothing.
        pg, shown, words = _escape_literal(text), text, ()
    else:
        (pg, folded), shown, words = _folded_literal(text), text, (text,)
    # A literal is not held to the regex length bound: it was escaped here,
    # not written as a pattern. Its walk only measures the literal run.
    try:
        run = _walk(_P.parse(pg).data, query)
    except re.error as exc:  # pragma: no cover - an escaped literal always parses
        raise _invalid(f"invalid literal: {exc}", query) from exc
    return GrepTerm(kind, shown, pg, negated, run, words, folded=folded)


def spelling_tried(term: GrepTerm) -> str:
    """What a text term matched, in words (#3666 R5: a reply says which
    spellings it tried, so "no match" is never read as more than that)."""
    if term.kind == "regex":
        return f"{REGEX_TERM_SYNTAX.replace('<pattern>', term.text)} as written, any case"
    if term.folded:
        return (
            f"{term.text}: any case, and any run of _ - . or space where it "
            "has one between letters or digits"
        )
    return f"{term.text}: any case"


#: Typographic double quotes read as straight ones by :func:`quoted_spans`:
#: U+201C and U+201E open, U+201D closes.
_TYPOGRAPHIC_QUOTES = str.maketrans({"\u201c": '"', "\u201d": '"', "\u201e": '"'})
#: The whitespace between a phrase's words: ASCII only, as PostgreSQL's
#: ``\s`` reads it in grep's SQL pattern, so a no-break space (U+00A0) or an
#: ideographic space (U+3000) separates words in neither.
_PHRASE_GAP = r"[ \t\n\r\f\v]+"


def quoted_spans(query: str) -> tuple[str, ...]:
    """The double-quoted spans a lexical query asks for, in query order.

    PACT lexical-quoted-phrases-3064. The phrase rule is grep's
    (:func:`_literal_term`): a span's words are its whitespace-separated
    pieces, shown joined by one space. Typographic quotes (U+201C, U+201E,
    U+201D) read as straight ones. A ``"`` opens a span and the next ``"`` closes
    it; an unbalanced quote leaves the rest of the query literal and an
    empty pair is skipped, never refused (lexical search accepts any text).
    A span written ``-"..."`` is not a requirement (lexical search has no
    exclusion) when the ``-`` starts a term as grep reads it: at the start,
    after whitespace, or right after a closing quote (``"a"-"b"``). Repeats
    are kept once.
    """
    if not isinstance(query, str):
        return ()
    text = query.translate(_TYPOGRAPHIC_QUOTES)
    spans: dict[str, None] = {}
    i = 0
    last_close = -2
    while True:
        start = text.find('"', i)
        if start < 0:
            break
        close = text.find('"', start + 1)
        if close < 0:
            break
        dash = start - 1
        negated = dash >= 0 and text[dash] == "-" and (
            dash == 0 or text[dash - 1].isspace() or dash - 1 == last_close
        )
        words = text[start + 1: close].split()
        if words and not negated:
            spans.setdefault(" ".join(words), None)
        i = last_close = close
        i += 1
    return tuple(spans)


#: At most this many spans get an exact pass on one lexical call (PACT
#: lexical-exact-identifier-hits); the rest are reported as dropped.
MAX_EXACT_SPANS = 3
#: Characters stripped from both ends of a query token before the shape test:
#: brackets, quotes and sentence punctuation that surround an identifier in
#: prose (``(CMSDM-353),`` reads as ``CMSDM-353``).
_TOKEN_EDGE = "()[]{}<>'\"`,;!?*"
_TOKEN_TRAIL = ".:"
#: A separator joining letters or digits inside an identifier.
_JOINED = re.compile(r"[A-Za-z0-9][-_./:#=?&@~]+[A-Za-z0-9]")
_LOWER_UPPER = re.compile(r"[a-z][A-Z]")
#: An acronym-led CamelCase step: `HTTPServer`, `IOError`, not `URLs`.
_ACRONYM_WORD = re.compile(r"[A-Z]{2}[a-z]{2}")
_LITERAL_RUN = re.compile(r"[A-Za-z0-9]{3,}")
#: Two letters-only words joined by one `-` or `/`: prose (`follow-up`,
#: `Wi-Fi`, `and/or`, `TCP/IP`), not a name. Three or more parts
#: (`GEN-SIM-RECO`) stay identifiers.
_WORD_PAIR = re.compile(r"[A-Za-z]+[-/][A-Za-z]+")
#: Prose that is identifier-shaped by the rules above but names nothing a
#: grep would find more exactly than BM25: lowercase hyphen chains
#: (`end-to-end`, `state-of-the-art`), dates (`2026-10-06`) and quantities
#: (`1990s`, `500GB`).
_PROSE = re.compile(
    r"[a-z]+(?:-[a-z]+)+|\d{4}-\d{2}-\d{2}(?:T[\d:.]+Z?)?|\d+(?:\.\d+)?[A-Za-z]{1,3}"
)
#: Markdown and bracket punctuation that glues a name to its neighbours.
_GLUE = str.maketrans({"[": " ", "]": " ", "(": " ", ")": " ", "<": " ", ">": " "})


def _identifier_shaped(token: str) -> bool:
    """Whether one query token names something exactly (PACT
    lexical-exact-identifier-hits): a ticket key, dataset path, URL, host,
    dotted file name, snake_case, CamelCase or letter-digit name."""
    if len(token) < 4 or not _LITERAL_RUN.search(token) or _PROSE.fullmatch(token):
        return False
    if _JOINED.search(token) and not _WORD_PAIR.fullmatch(token):
        return True
    if _LOWER_UPPER.search(token) or _ACRONYM_WORD.search(token):
        return True
    has_alpha = any(c.isascii() and c.isalpha() for c in token)
    has_digit = any(c.isdigit() for c in token)
    return has_alpha and has_digit and len(token) >= 5


def _unquoted_text(query: str) -> str:
    """The query with each balanced double-quoted part removed."""
    text = query.translate(_TYPOGRAPHIC_QUOTES)
    out: list[str] = []
    i = 0
    while True:
        start = text.find('"', i)
        close = text.find('"', start + 1) if start >= 0 else -1
        if start < 0 or close < 0:
            out.append(text[i:])
            return " ".join(out)
        out.append(text[i:start])
        i = close + 1


def exact_spans(query: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """``(spans, dropped)``: the spans a lexical query gets an exact pass for.

    PACT lexical-exact-identifier-hits. The quoted spans of
    :func:`quoted_spans` that hold two or more words or are identifier-shaped
    come first, then the identifier-shaped tokens of the
    unquoted text (:func:`_identifier_shaped`), in query order and each once
    (case-insensitively, as grep matches). At most :data:`MAX_EXACT_SPANS` are
    kept; ``dropped`` lists the rest so the response can name them.
    """
    if not isinstance(query, str):
        return (), ()
    found: dict[str, str] = {}
    for span in quoted_spans(query):
        # A quoted single plain word ("move") is no more exact than the word
        # itself: BM25 ranks it, and its exact pass would list arbitrary
        # records of a common word. A quoted phrase or identifier is kept.
        if " " in span or _identifier_shaped(span):
            found.setdefault(span.casefold(), span)
    for raw in _unquoted_text(query).translate(_GLUE).split():
        token = raw.strip(_TOKEN_EDGE).rstrip(_TOKEN_TRAIL).strip(_TOKEN_EDGE)
        if token.endswith(("'s", "\u2019s")):
            token = token[:-2]
        if token.startswith("-"):
            # Lexical search has no exclusion; a leading dash is not part of
            # the name grep would look for.
            token = token.lstrip("-")
        if _identifier_shaped(token):
            found.setdefault(token.casefold(), token)
    spans = tuple(found.values())
    return spans[:MAX_EXACT_SPANS], spans[MAX_EXACT_SPANS:]


#: A file extension ending a span: a dot, then up to 10 letters or digits,
#: one of them a letter (`.sh`, `.py`, `.yaml`, `.7z`; not `.353` or `.10`).
_FILE_EXTENSION = re.compile(r"[^./]\.(?=[0-9]*[A-Za-z])[A-Za-z0-9]{1,10}$")


def filename_shaped(span: str) -> bool:
    """Whether an exact span may name a file by its path (PACT
    filename-exact-hits): no whitespace, not a URL, and it holds a `/` or
    ends in a file extension. Such a span also gets the own-path pass."""
    if not isinstance(span, str) or not span or any(c.isspace() for c in span):
        return False
    if "://" in span:
        return False
    return "/" in span or bool(_FILE_EXTENSION.search(span))


def own_path_tail_query(span: str) -> GrepQuery:
    """The grep query of the own-path pass (PACT filename-exact-hits): one
    positive path term that matches a path equal to `span` or ending in
    `/<span>`, case-insensitively as every path term (`~*`). Its index run is
    the literal's, so the path trigram index narrows it as it narrows
    `path:<span>`. Built here, never parsed: the grammar has no anchored path
    term, and the tool surface gains none."""
    term = _literal_term(span, kind="path", negated=False, query=span)
    anchored = replace(term, pg_regex=f"(^|/){term.pg_regex}$")
    return GrepQuery((), (anchored,))


def strip_quotes(query: str) -> str:
    """The query with every double quote (typographic included) replaced by
    a space: what BM25 ranks when :func:`quoted_spans` found a span."""
    return query.translate(_TYPOGRAPHIC_QUOTES).replace('"', " ")


def phrase_pattern(words: tuple[str, ...] | list[str]) -> re.Pattern[str]:
    """Grep's phrase match in Python: the words in order, any run of ASCII
    whitespace between them, case-insensitive (the ``~*`` of
    :func:`_literal_term`'s PostgreSQL regex)."""
    if not words:
        raise ValueError("a phrase needs at least one word")
    return re.compile(_PHRASE_GAP.join(re.escape(w) for w in words), re.IGNORECASE)


_NEAR_OPERANDS = "NEAR needs a distance and two operands: a NEAR/<n> b"


def parse_grep_query(query: str) -> GrepQuery:
    """Parse a grep query; refuse it with `grep_query_invalid` and the reason."""
    if not isinstance(query, str) or not query.strip():
        raise _invalid("the query is empty", str(query or ""))
    if len(query) > MAX_QUERY_CHARS:
        raise _invalid(f"the query is longer than {MAX_QUERY_CHARS} characters", query)
    items: list[object] = []  # GrepTerm | ("near", n)
    i, n = 0, len(query)
    while i < n:
        if query[i].isspace():
            i += 1
            continue
        negated = False
        if query[i] == "-" and i + 1 < n and not query[i + 1].isspace():
            negated = True
            i += 1
        if query.startswith(PATH_TERM_PREFIX, i):
            start = i + len(PATH_TERM_PREFIX)
            if start < n and query[start] == '"':
                close = query.find('"', start + 1)
                if close < 0:
                    raise _invalid("an unbalanced quote", query)
                value, i = query[start + 1: close], close + 1
            else:
                end = start
                while end < n and not query[end].isspace():
                    end += 1
                value, i = query[start:end], end
                if '"' in value:
                    raise _invalid("a quote must start a phrase", query)
            items.append(_literal_term(value, kind="path", negated=negated, query=query))
            continue
        regex = read_regex_term(query, i)
        if regex is not None:
            pattern, i = regex
            if not pattern:
                raise _invalid("an empty regular expression", query)
            items.append(GrepTerm(
                "regex", pattern, pattern, negated, literal_run(pattern, query=query),
                () if negated else alternation_words(pattern),
            ))
            continue
        if query[i] == '"':
            close = query.find('"', i + 1)
            if close < 0:
                raise _invalid("an unbalanced quote", query)
            items.append(_literal_term(
                query[i + 1: close], kind="phrase", negated=negated, query=query,
            ))
            i = close + 1
            continue
        end = i
        while end < n and not query[end].isspace():
            end += 1
        word = query[i:end]
        i = end
        if '"' in word:
            raise _invalid("a quote must start a phrase", query)
        if word == "NEAR" or word.startswith("NEAR/"):
            near = _NEAR.fullmatch(word)
            if near is None or negated:
                raise _invalid(_NEAR_OPERANDS, query)
            distance = int(near.group(1))
            if distance > MAX_NEAR_DISTANCE:
                raise _invalid(
                    f"a NEAR distance above {MAX_NEAR_DISTANCE} is not supported", query,
                )
            items.append(("near", distance))
            continue
        if "|" in word.strip("|"):
            # Agents write `rollback|revert` meaning either word; as a plain
            # word it would match only that literal text, so it would come back
            # empty and say nothing (#3183). Refuse it and name both forms,
            # keeping an exclusion's `-`.
            sign = "-" if negated else ""
            raise GrepQueryInvalid(
                f"grep query refused: `{sign}{word}` is one plain word containing "
                "'|', which matches that literal text, not either side",
                data={"reason": "bare_alternation", "query": query[:MAX_QUERY_CHARS]},
                fix_hint=(
                    f"write {sign}re:/{_literal_alternation(word)}/ to match any "
                    f'of its parts, or {sign}"{word}" to match the literal text'
                ),
            )
        items.append(_literal_term(word, kind="word", negated=negated, query=query))
    terms: list[GrepTerm] = []
    k = 0
    while k < len(items):
        item = items[k]
        if isinstance(item, tuple):
            raise _invalid(_NEAR_OPERANDS, query)
        follows = items[k + 1] if k + 1 < len(items) else None
        if isinstance(follows, tuple):
            right = items[k + 2] if k + 2 < len(items) else None
            if not isinstance(item, GrepTerm) or not isinstance(right, GrepTerm):
                raise _invalid(_NEAR_OPERANDS, query)
            if item.kind not in ("word", "phrase") or right.kind not in ("word", "phrase"):
                raise _invalid("NEAR operands must be words or quoted phrases", query)
            if item.negated or right.negated:
                raise _invalid("a NEAR operand cannot be negated", query)
            if k + 3 < len(items) and isinstance(items[k + 3], tuple):
                raise _invalid("NEAR takes exactly two operands; AND two NEAR terms instead", query)
            distance = follows[1]
            a, b = item.pg_regex, right.pg_regex
            pg = f"(?:{a}).{{0,{distance}}}(?:{b})|(?:{b}).{{0,{distance}}}(?:{a})"
            terms.append(GrepTerm(
                "near", f"{item.text} NEAR/{distance} {right.text}", pg, False,
                _walk(_P.parse(pg).data, query), (*item.words, *right.words),
                tuple(dict.fromkeys((a, b))), folded=item.folded or right.folded,
            ))
            k += 3
            continue
        assert isinstance(item, GrepTerm)
        terms.append(item)
        k += 1
    if len(terms) > MAX_TERMS:
        raise _invalid(f"more than {MAX_TERMS} terms", query)
    parsed = GrepQuery(
        tuple(t for t in terms if t.kind != "path"),
        tuple(t for t in terms if t.kind == "path"),
    )
    if not parsed.positive and not any(not t.negated for t in parsed.path_terms):
        raise _invalid("a query of exclusions only has nothing to exclude from", query)
    return parsed


#: The scopes that let an unindexable query run (under the timeout).
SCOPE_NAMES = (
    "path_prefix, a path: term of 3+ letters or digits, subtype_filter, "
    "profile, source_family or filter.source"
)


def assert_indexable_or_scoped(parsed: GrepQuery, *, scoped: bool) -> None:
    """The index rule (DESIGN.md section 11): an unscoped query needs one
    positive term the trigram index can narrow; a scoped one runs anyway,
    bounded by the statement timeout. A positive `path:` term of 3 or more
    literal letters or digits is a scope (PACT mcp-grep-rank)."""
    if scoped or parsed.indexable or parsed.path_scoped:
        return
    data = {
        "terms": [t.text for t in (*parsed.positive, *parsed.path_terms) if not t.negated],
        "min_literal_run": MIN_INDEXABLE_RUN,
    }
    if any(t.kind == "regex" for t in parsed.positive):
        raise GrepRegexUnindexable(
            "this pattern has no run of 3 or more literal letters or digits the "
            "index can use",
            data=data,
            fix_hint=(
                "add a run of 3 or more literal letters or digits (for example "
                "[A-Z]+-\\d+ becomes JIRA-\\d+), or scope the call with "
                f"{SCOPE_NAMES}"
            ),
        )
    raise GrepPatternTooShort(
        "every term is shorter than 3 literal letters or digits; the index "
        "cannot narrow it",
        data=data,
        fix_hint=f"add a term of 3 or more letters or digits, or scope the call with {SCOPE_NAMES}",
    )
