"""Where a lexical excerpt is cut: the window holding the most query words.

PACT search-result-shape, DESIGN.md D4 (issue #3071). Before this, an excerpt
anchored on the first raw substring hit of any whitespace-split query token,
stopwords and fragments inside longer words included, so it usually showed
the top of the record rather than the part that matched.

The rule. Words are matched the way the ranker matches them
(`text_config='english'`: stemming and stopword removal). One SQL call per
page lexizes the query (`to_tsvector('english', ...)` over its words of three
or more characters) and the page's candidate words (`ts_lexize('english_stem',
...)`), and the window is chosen in Python over word offsets:

- a query word of one or two characters that is a stopword (`a`, `in`, `of`)
  yields no lexeme and never anchors; any other short word (`c`, `5g`, `id`)
  matches only an equal whole word, case-folded;
- only whole words match, so a fragment inside a longer word never anchors;
- the window of `max_chars` characters holding the most DISTINCT query words
  wins; ties go to the earliest window.

Bound. Only text words sharing their first three characters with a query word
are candidates, and at most `CANDIDATE_WORD_CAP` distinct candidates are
lexized per page. Past the cap the page falls back to whole-word,
stopword-free matching in Python and says so (`excerpt_window_basis`).

Matched words are not marked (decision 3). The fence-safe cut and the
`max_chars` bound stay with the callers, unchanged.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

#: Distinct candidate words one page may lexize.
CANDIDATE_WORD_CAP = 5000
#: Characters a candidate word shares with a query word to be lexized.
PREFIX_CHARS = 3
#: A matched comment or description block up to this many characters is a
#: search row's passage whole, as a short code chunk is (PACT
#: read-passage-rows-3666): a 29-name list in one comment, cut to the
#: 500-character ticket window, could not be counted. The reply-budget fit
#: (response_bounds, D4) cuts such a passage last. PACT read-reply-size-3666:
#: 800 (was 1,500), the most any passage or grep match context holds when
#: the call gives no `max_chars`; a longer block is cut around its match and
#: the reply names `max_chars` for more (Sol graph runs, ERAG-500: replies
#: were 90% of prompt tokens).
UNIT_WHOLE_CHARS = 800

BASIS_LEXIZED = "lexized"
BASIS_FALLBACK = "whole_word_fallback"

#: Whole words: letters and digits (no underscore).
_WORD_RE = re.compile(r"[^\W_]+", re.UNICODE)

#: The fallback's stopword list: PostgreSQL's `english` stopword file, the
#: list the ranker's configuration uses, so the fallback drops the same words.
FALLBACK_STOPWORDS = frozenset(
    """
    i me my myself we our ours ourselves you your yours yourself yourselves he
    him his himself she her hers herself it its itself they them their theirs
    themselves what which who whom this that these those am is are was were be
    been being have has had having do does did doing a an the and but if or
    because as until while of at by for with about against between into
    through during before after above below to from up down in out on off over
    under again further then once here there when where why how all any both
    each few more most other some such no nor not only own same so than too
    very s t can will just don should now
    """.split()
)


@dataclass(frozen=True)
class ExcerptMatcher:
    """How one page's words map to the query words they match.

    `term_of` maps a case-folded text word to the query terms it matches. A
    term is a lexeme (`lex:<lexeme>`) or an exact short word (`word:<w>`).
    """

    basis: str
    term_of: dict[str, frozenset[str]] = field(default_factory=dict)
    exact_words: frozenset[str] = frozenset()
    lexemes: frozenset[str] = frozenset()

    def terms(self, word: str) -> frozenset[str]:
        folded = word.casefold()
        found = set(self.term_of.get(folded, ()))
        if folded in self.exact_words:
            found.add(f"word:{folded}")
        return frozenset(found)

    @property
    def empty(self) -> bool:
        return not self.lexemes and not self.exact_words


def query_words(query: str) -> list[str]:
    return [match.group(0).casefold() for match in _WORD_RE.finditer(query or "")]


def _candidate_words(texts: Iterable[str], prefixes: set[str]) -> set[str]:
    found: set[str] = set()
    for text in texts:
        for match in _WORD_RE.finditer(text or ""):
            word = match.group(0).casefold()
            if len(word) >= PREFIX_CHARS and word[:PREFIX_CHARS] in prefixes:
                found.add(word)
    return found


def fallback_matcher(query: str) -> ExcerptMatcher:
    """Whole-word, stopword-free matching in Python: the bounded fallback."""
    words = {w for w in query_words(query) if w not in FALLBACK_STOPWORDS}
    return ExcerptMatcher(
        basis=BASIS_FALLBACK,
        term_of={w: frozenset({f"word:{w}"}) for w in words},
        exact_words=frozenset(),
        lexemes=frozenset(words),
    )


def build_matcher(
    conn: Any,
    query: str,
    texts: Iterable[str],
    *,
    candidate_cap: int = CANDIDATE_WORD_CAP,
) -> ExcerptMatcher:
    """The page's matcher: one SQL call, or the disclosed fallback past the cap."""
    words = query_words(query)
    long_words = [w for w in words if len(w) >= PREFIX_CHARS]
    short_words = sorted({w for w in words if len(w) < PREFIX_CHARS})
    prefixes = {w[:PREFIX_CHARS] for w in long_words}
    candidates = _candidate_words(texts, prefixes)
    if len(candidates) > candidate_cap or conn is None:
        return fallback_matcher(query)
    lexize = sorted(candidates | set(short_words))
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 'q' AS kind, lexeme AS word, NULL::text[] AS lexemes
              FROM unnest(to_tsvector('english', %(q)s))
            UNION ALL
            SELECT 'w', w, ts_lexize('english_stem', w)
              FROM unnest(%(words)s::text[]) AS w
            """,
            {"q": " ".join(long_words), "words": lexize},
        )
        rows = cur.fetchall()
    query_lexemes = frozenset(str(row[1]) for row in rows if row[0] == "q")
    lexized = {str(row[1]): tuple(row[2] or ()) for row in rows if row[0] == "w"}
    exact = frozenset(w for w in short_words if lexized.get(w))
    term_of: dict[str, frozenset[str]] = {}
    for word in candidates:
        hits = frozenset(
            f"lex:{lexeme}" for lexeme in lexized.get(word, ()) if lexeme in query_lexemes
        )
        if hits:
            term_of[word] = hits
    return ExcerptMatcher(
        basis=BASIS_LEXIZED,
        term_of=term_of,
        exact_words=exact,
        lexemes=query_lexemes,
    )


def best_window_start(text: str, matcher: ExcerptMatcher, budget: int) -> int:
    """Start offset of the `budget`-character window with the most distinct
    query terms; the earliest on a tie; 0 when nothing matches."""
    span = best_match_span(text, matcher, budget)
    if span is None:
        return 0
    # Centre the matched span in the window, never moving a matched word out.
    slack = budget - (span[1] - span[0])
    return max(0, span[0] - max(0, slack) // 2)


def best_match_span(
    text: str, matcher: ExcerptMatcher, budget: int,
) -> Optional[tuple[int, int]]:
    """`(start, end)` of the matched words inside the best `budget`-character
    window (the most distinct query terms, the earliest on a tie), or None
    when no word of `text` matches.

    PACT read-passage-rows-3666: a search row's passage is centred on this
    span, and the span's start says which part of the record (a comment, the
    description) the match is in."""
    hits: list[tuple[int, int, frozenset[str]]] = []
    for match in _WORD_RE.finditer(text):
        terms = matcher.terms(match.group(0))
        if terms:
            hits.append((match.start(), match.end(), terms))
    if not hits:
        return None
    # Two pointers over the hits: `j` is one past the last hit inside the
    # window that starts at hit `i`, and `counts` holds the terms of hits
    # i..j-1. Each hit enters and leaves once, so the scan is linear in the
    # number of hits (it was quadratic on dense matches).
    best_count, best_start, best_span_end = -1, 0, 0
    counts: dict[str, int] = {}
    j = 0
    for i, (start, _end, _terms) in enumerate(hits):
        if j < i:
            j = i
        while j < len(hits) and hits[j][1] - start <= budget:
            for term in hits[j][2]:
                counts[term] = counts.get(term, 0) + 1
            j += 1
        if len(counts) > best_count:
            best_count, best_start = len(counts), start
            best_span_end = hits[j - 1][1] if j > i else start
        if j > i:
            for term in hits[i][2]:
                counts[term] -= 1
                if not counts[term]:
                    del counts[term]
    return best_start, best_span_end


def matched_terms(
    text: str, matcher: ExcerptMatcher, lo: int = 0, hi: Optional[int] = None,
) -> frozenset[str]:
    """The distinct query terms the whole words of `text[lo:hi]` match.

    A word cut by `lo` or `hi` is not counted: only words wholly inside."""
    end = len(text) if hi is None else hi
    found: set[str] = set()
    for match in _WORD_RE.finditer(text, lo, end):
        if (match.start() > lo or lo == 0 or not text[lo - 1].isalnum()) and (
            match.end() < end or end == len(text) or not text[end].isalnum()
        ):
            found |= matcher.terms(match.group(0))
    return frozenset(found)


def centred_cut(
    text: str, span: tuple[int, int], *, lo: int, hi: int, budget: int,
) -> tuple[int, int]:
    """`(start, end)` of at most `budget` characters of `text[lo:hi]` with
    `span` centred in it, moved inside `lo..hi` at either edge.

    A cut that does not reach `lo` or `hi` ends at a word boundary when one
    is within a tenth of the budget, so a passage does not start or stop
    mid-word. The matched span is never cut out: when it is longer than the
    budget, the cut starts at it."""
    if hi - lo <= budget:
        return lo, hi
    span_start = max(lo, min(span[0], hi))
    span_end = max(span_start, min(span[1], hi))
    slack = max(0, budget - (span_end - span_start))
    start = max(lo, span_start - slack // 2)
    end = min(hi, start + budget)
    start = max(lo, end - budget)
    snap = max(1, budget // 10)
    if start > lo:
        space = text.find(" ", start, min(span_start, start + snap))
        if space >= 0:
            start = space + 1
    if end < hi:
        space = text.rfind(" ", max(span_end, end - snap), end)
        if space > start:
            end = space
    return start, end


def window_excerpt(
    text: str, matcher: Optional[ExcerptMatcher], *, max_chars: int,
) -> str:
    """The `max_chars` excerpt of `text` centred on its best window."""
    return window_span(text, matcher, max_chars=max_chars)[1]


def window_span(
    text: str, matcher: Optional[ExcerptMatcher], *, max_chars: int,
) -> tuple[int, str]:
    """`(start, excerpt)`: the excerpt `window_excerpt` returns, and the
    character offset in `text` where its body begins.

    PACT read-model-s5-location: a search hit's location is this offset, so
    the place a reader is sent to is the place the excerpt showed, chosen
    once."""
    if len(text) <= max_chars:
        return 0, text
    if matcher is None or matcher.empty:
        start = 0
    else:
        # Reserve room for both ellipses while choosing, so the window that
        # won is the window shown.
        start = best_window_start(text, matcher, max(1, max_chars - 6))
    if start > 0:
        # The leading "..." takes three characters of the budget, so a window
        # that reaches the end may start three characters later; clamping to
        # `len(text) - max_chars` cut a match at the very end (review S1).
        start = min(start, len(text) - (max_chars - 3))
    prefix = "..." if start > 0 else ""
    # A suffix is needed when the text runs past what the prefix leaves room
    # for; testing `start + max_chars` instead dropped up to three trailing
    # characters with no "..." (re-review).
    suffix = "..." if start + max_chars - len(prefix) < len(text) else ""
    body = text[start: start + max_chars - len(prefix) - len(suffix)]
    return start, f"{prefix}{body}{suffix}"
