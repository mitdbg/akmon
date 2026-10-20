"""Literal record-window assembly after reader-visible occurrence finishing.

This module does not acquire candidates, execute regexes, authorize rows, count
the search universe, or issue cursors. The shared scanner owns those boundaries.
PostgreSQL match events enter here as zero-based half-open character ranges.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any, Mapping, Sequence

import psycopg
from psycopg.rows import tuple_row

from okg.substrate.errors import SubstrateError


class WindowContinuityUnknown(SubstrateError):
    code = "window_continuity_unknown"


class WindowWorkIncomplete(SubstrateError):
    code = "window_work_incomplete"


class WindowWorkBound(SubstrateError):
    code = "window_work_bound"


@dataclass(frozen=True)
class ReadableOccurrence:
    """An actual visible occurrence; positions belong to its edge, not a node."""

    record_id: str
    chunk_id: str
    text: str
    edge_id: str
    char_offset: int | None = None
    char_end: int | None = None
    doc_char_length: int | None = None
    separator_before: str | None = None
    row: Mapping[str, Any] = field(default_factory=dict)
    #: Adapter-proven canonical source stream and authoritative position contract.
    #: Record identity or equal source lengths alone cannot establish this.
    stream_id: str | None = None
    positions_authoritative: bool = False
    #: Leading characters of this text that repeat the previous readable
    #: occurrence's tail (`repeated_head`): a window chunker's overlap. A
    #: match wholly inside them is that occurrence's match, counted there.
    repeated_head: int = 0
    #: The occurrence whose tail the head repeats, and this text's start
    #: less that one's: a head match is a repeat only where that occurrence
    #: has the same match, `repeat_shift` characters further on.
    repeats: str | None = None
    repeat_shift: int = 0

    @property
    def exact(self) -> bool:
        if self.positions_authoritative is not True or not self.stream_id:
            return False
        values = (self.char_offset, self.char_end, self.doc_char_length)
        if not all(isinstance(v, int) and not isinstance(v, bool) for v in values):
            return False
        start, end, length = values
        assert start is not None and end is not None and length is not None
        return 0 <= start <= end <= length and end - start == len(self.text)


def repeated_head(
    previous_text: str, previous_start: Any, previous_end: Any,
    text: str, start: Any, end: Any,
) -> int:
    """How many leading characters of `text` repeat `previous_text`'s tail.

    A window chunker repeats the tail of one chunk at the head of the next.
    The repeat counts only when it is proven, by the rule `inspect` text
    trims by (`record_text.trim_overlap`): both texts carry exact stored
    spans (`end - start == len(text)`), the spans increase and overlap, and
    the previous text really ends with the characters the overlap names.
    Anything else is 0: the texts are read whole, as before."""

    def exact(a: Any, b: Any, body: str) -> bool:
        return (
            isinstance(a, int) and not isinstance(a, bool)
            and isinstance(b, int) and not isinstance(b, bool)
            and 0 <= a <= b and b - a == len(body)
        )

    if not (exact(previous_start, previous_end, previous_text) and exact(start, end, text)):
        return 0
    if not previous_start < start < previous_end:
        return 0
    overlap = previous_end - start
    if overlap > len(text) or not previous_text.endswith(text[:overlap]):
        return 0
    return overlap


@dataclass(frozen=True)
class ChainPiece:
    """A literal slice of a chunk or a stored occurrence separator."""

    occurrence: ReadableOccurrence
    text: str
    start: int
    chunk_start: int | None

    @property
    def end(self) -> int:
        return self.start + len(self.text)


@dataclass
class TextChain:
    record_id: str
    pieces: list[ChainPiece]
    source_start: int | None
    source_end: int | None
    doc_char_length: int | None
    stream_id: str | None
    occurrences: list[ReadableOccurrence]

    @property
    def text(self) -> str:
        return "".join(piece.text for piece in self.pieces)

    @property
    def text_length(self) -> int:
        return self.pieces[-1].end if self.pieces else 0

    def slice(self, start: int, end: int) -> str:
        return "".join(
            piece.text[max(start, piece.start) - piece.start:min(end, piece.end) - piece.start]
            for piece in self.pieces if piece.start < end and piece.end > start
        )


def build_chains(occurrences: Sequence[ReadableOccurrence]) -> list[TextChain]:
    """Build proven continuous chains in the caller's canonical source order.

    Unknown spans, gaps, inconsistent lengths and conflicting overlap start a
    new chain. A source-order label alone does not prove textual adjacency.
    """
    chains: list[TextChain] = []
    record_id: str | None = None
    for occurrence in occurrences:
        if record_id is None:
            record_id = occurrence.record_id
        elif occurrence.record_id != record_id:
            raise ValueError("one chain assembly cannot mix record membership")
        joined = False
        if chains and occurrence.exact:
            chain = chains[-1]
            if (
                chain.source_start is not None
                and chain.source_end is not None
                and chain.doc_char_length == occurrence.doc_char_length
                and chain.stream_id == occurrence.stream_id
                and occurrence.char_offset is not None
                and occurrence.char_end is not None
                and occurrence.char_offset >= chain.source_start
            ):
                overlap = chain.source_end - occurrence.char_offset
                stored_length = chain.text_length
                if overlap >= 0:
                    repeated = min(overlap, len(occurrence.text))
                    at = occurrence.char_offset - chain.source_start
                    if repeated == 0 or chain.slice(at, at + repeated) == occurrence.text[:repeated]:
                        if repeated < len(occurrence.text):
                            chain.pieces.append(ChainPiece(
                                occurrence, occurrence.text[repeated:], stored_length, repeated,
                            ))
                            chain.source_end = occurrence.char_end
                        joined = True
                else:
                    separator = occurrence.separator_before
                    if isinstance(separator, str) and len(separator) == -overlap:
                        chain.pieces.append(ChainPiece(occurrence, separator, stored_length, None))
                        chain.pieces.append(ChainPiece(
                            occurrence, occurrence.text, stored_length + len(separator), 0,
                        ))
                        chain.source_end = occurrence.char_end
                        joined = True
                if joined:
                    chain.occurrences.append(occurrence)
        if not joined:
            chains.append(TextChain(
                occurrence.record_id,
                [ChainPiece(occurrence, occurrence.text, 0, 0)],
                occurrence.char_offset if occurrence.exact else None,
                occurrence.char_end if occurrence.exact else None,
                occurrence.doc_char_length if occurrence.exact else None,
                occurrence.stream_id if occurrence.exact else None,
                [occurrence],
            ))
    return chains


def source_coverage(chains: Sequence[TextChain]) -> str:
    """Describe original source tiling independently of search completion."""
    if not chains:
        return "none"
    if any(c.source_start is None or c.source_end is None for c in chains):
        return "unknown"
    lengths = {c.doc_char_length for c in chains}
    streams = {c.stream_id for c in chains}
    if len(lengths) != 1 or None in lengths or len(streams) != 1 or None in streams:
        return "unknown"
    covered = 0
    for chain in chains:
        assert chain.source_start is not None and chain.source_end is not None
        if chain.source_start < covered:
            return "unknown"
        if chain.source_start > covered:
            return "partial"
        covered = chain.source_end
    length = next(iter(lengths))
    return "complete" if covered == length else "partial"


@dataclass(frozen=True, order=True)
class MatchEvent:
    term: int
    start: int
    end: int


@dataclass(frozen=True)
class TermScan:
    events: tuple[tuple[MatchEvent, ...], ...]
    source_bytes: int
    match_events: int
    elapsed_ms: float


def scan_chain_matches(
    conn: psycopg.Connection,
    terms: Sequence[Any],
    texts: Sequence[str],
    *,
    max_events: int = 10000,
    timeout_ms: int = 2000,
) -> TermScan:
    """Scan parser-compiled PostgreSQL terms with bounded global occurrences.

    Events have PostgreSQL's non-overlapping global-match semantics for each
    term; overlaps between different terms remain distinct. The parser must
    have refused backrefs/lookaround/inline flags before this boundary.
    The event cap bounds admitted evidence; PostgreSQL may enumerate a dense
    pair internally before LIMIT. Source bytes and the deadline bound that work.
    Count and exhaustion of records remain the shared scanner's responsibility.
    """
    for name, value, maximum in (("max_events", max_events, 10000), ("timeout_ms", timeout_ms, 2000)):
        if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= maximum:
            raise ValueError(f"{name} must be an integer in 1..{maximum}")
    if len(terms) > 64 or len(texts) > 5000:
        raise WindowWorkBound("window term/occurrence work bound reached", data={"bound": "terms_or_occurrences"})
    source_bytes = sum(len(text.encode("utf-8")) for text in texts)
    if source_bytes > 8 * 1024 * 1024:
        raise WindowWorkBound("window source byte bound reached", data={"bound": "source_bytes"})
    if any(term.kind == "path" for term in terms):
        raise ValueError("path terms apply to the record path, not its text chains")
    started = time.monotonic()

    def run(sql: str, params: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        remaining = timeout_ms - int((time.monotonic() - started) * 1000)
        if remaining <= 0:
            raise WindowWorkBound("window source time bound reached", data={"bound": "source_time"})
        try:
            with conn.transaction():
                with conn.cursor(row_factory=tuple_row) as cursor:
                    cursor.execute(
                        "SELECT current_setting('statement_timeout'), setting::bigint "
                        "FROM pg_settings WHERE name = 'statement_timeout'"
                    )
                    previous, caller_limit = cursor.fetchone()
                    statement_limit = min(remaining, caller_limit) if caller_limit else remaining
                    cursor.execute(
                        "SELECT set_config('statement_timeout', %s, true)",
                        (f"{statement_limit}ms",),
                    )
                    cursor.execute(sql, params, prepare=False)
                    rows = cursor.fetchall()
                    cursor.execute("SELECT set_config('statement_timeout', %s, true)", (previous,))
                    return rows
        except psycopg.errors.QueryCanceled as exc:
            raise WindowWorkBound(
                "window source time bound reached", data={"bound": "source_time"},
                fix_hint="narrow the record scope or query",
            ) from exc

    if not texts or not terms:
        return TermScan(tuple(() for _ in texts), source_bytes, 0, 0.0)
    patterns = [term.pg_regex for term in terms]
    # The global LIMIT precedes grouping, not one limit per text/term pair.
    # A terminal read below the cap has examined zero-event pairs as well.
    rows = run(
        "WITH limited AS MATERIALIZED ("
        "SELECT t.i::int - 1 AS ti, q.j::int - 1 AS qi, "
        "m.ordinal, char_length(m.parts[1]) AS length "
        "FROM unnest(%s::text[]) WITH ORDINALITY AS t(txt, i) "
        "CROSS JOIN unnest(%s::text[]) WITH ORDINALITY AS q(pattern, j) "
        "CROSS JOIN LATERAL regexp_matches(t.txt, '(' || q.pattern || ')', 'gi') "
        "WITH ORDINALITY AS m(parts, ordinal) LIMIT %s) "
        "SELECT ti, qi, array_agg(length ORDER BY ordinal) "
        "FROM limited GROUP BY ti, qi",
        (list(texts), patterns, max_events + 1),
    )
    lengths_by_pair = {(ti, qi): lengths for ti, qi, lengths in rows}
    total = sum(len(lengths) for lengths in lengths_by_pair.values())
    if total > max_events:
        raise WindowWorkBound(
            "window match event bound reached",
            data={"bound": "match_events", "max_events": max_events},
            fix_hint="narrow the query or record scope",
        )
    nonzero = [pair for pair, lengths in lengths_by_pair.items() if 0 not in lengths]
    zero = [pair for pair, lengths in lengths_by_pair.items() if 0 in lengths]
    spans_by_pair: dict[tuple[int, int], list[tuple[int, int]]] = {}
    if nonzero:
        # Split arrays are allowed only for actual matches below the global cap.
        # Send each source text once per pass, not once per matching pair.
        prefixes_rows = run(
            "WITH inputs AS (SELECT %s::text[] AS texts, %s::text[] AS patterns) "
            "SELECT p.ti, p.qi, ARRAY(SELECT char_length(part) FROM "
            "unnest(regexp_split_to_array(i.texts[p.ti + 1], i.patterns[p.qi + 1], 'i')) AS part) "
            "FROM inputs i CROSS JOIN unnest(%s::int[], %s::int[]) AS p(ti, qi)",
            (list(texts), patterns, [ti for ti, _ in nonzero], [qi for _, qi in nonzero]),
        )
        for ti, qi, prefixes in prefixes_rows:
            lengths = lengths_by_pair[(ti, qi)]
            if len(prefixes) != len(lengths) + 1:
                raise WindowWorkIncomplete("PostgreSQL split/match positions disagree")
            at = 0
            spans = []
            for prefix, length in zip(prefixes, lengths):
                at += prefix
                spans.append((at, at + length))
                at += length
            spans_by_pair[(ti, qi)] = spans
    if zero:
        # Split suppression differs for zero-width matches. Bound the original
        # engine's ordinal positions to the already admitted event inventory.
        zero_rows = run(
            "WITH inputs AS (SELECT %s::text[] AS texts, %s::text[] AS patterns) "
            "SELECT p.ti, p.qi, ARRAY(SELECT ARRAY["
            "regexp_instr(i.texts[p.ti + 1], i.patterns[p.qi + 1], 1, n, 0, 'i') - 1, "
            "regexp_instr(i.texts[p.ti + 1], i.patterns[p.qi + 1], 1, n, 1, 'i') - 1] "
            "FROM generate_series(1, p.count) AS n) "
            "FROM inputs i CROSS JOIN unnest(%s::int[], %s::int[], %s::int[]) AS p(ti, qi, count)",
            (list(texts), patterns, [ti for ti, _ in zero], [qi for _, qi in zero],
             [len(lengths_by_pair[pair]) for pair in zero]),
        )
        for ti, qi, positions in zero_rows:
            spans_by_pair[(ti, qi)] = [(position[0], position[1]) for position in positions]
    chain_events: list[list[MatchEvent]] = [[] for _ in texts]
    if spans_by_pair.keys() != lengths_by_pair.keys():
        raise WindowWorkIncomplete("PostgreSQL omitted an admitted text/term pair")
    for (ti, qi), spans in spans_by_pair.items():
        for start, end in spans:
            if not 0 <= start <= end <= len(texts[ti]):
                raise WindowWorkIncomplete("PostgreSQL returned an invalid match position")
            chain_events[ti].append(MatchEvent(qi, start, end))
    events = tuple(
        tuple(sorted(items, key=lambda event: (event.start, event.end, event.term)))
        for items in chain_events
    )
    elapsed = (time.monotonic() - started) * 1000
    if elapsed > timeout_ms:
        raise WindowWorkBound("window source time bound reached", data={"bound": "source_time"})
    return TermScan(events, source_bytes, total, elapsed)


def record_matches(
    terms: Sequence[Any],
    hits: Sequence[Sequence[MatchEvent]],
    *,
    occurrences_complete: bool,
    continuity_complete: bool,
) -> bool:
    """Evaluate term membership from PostgreSQL events, never Python regexes.

    Exact absence is only over the complete canonical readable occurrence set.
    Unproved boundaries cannot disprove complex cross-boundary text predicates.
    """
    if not occurrences_complete:
        raise WindowWorkIncomplete(
            "record matching requires the complete readable occurrence set",
            fix_hint="narrow the record scope or increase the allowed scan work",
        )
    positive = [i for i, term in enumerate(terms) if not term.negated]
    if not positive:
        raise ValueError("windows require at least one positive text term")
    found = {event.term for events in hits for event in events}
    if any(i in found and term.negated for i, term in enumerate(terms)):
        return False
    for index, term in enumerate(terms):
        if index in found:
            continue
        # A word is a substring match, so it can straddle a chunk boundary
        # exactly as a phrase, NEAR or regex can: none is decidable absent.
        if not continuity_complete and term.kind in {"word", "phrase", "near", "regex"}:
            raise WindowContinuityUnknown(
                "unproven source continuity makes record membership undecidable",
                data={"term_index": index, "term_kind": term.kind},
                fix_hint="use matching='passage' for explicitly chunk-local evidence",
            )
        if not term.negated:
            return False
    return True


@dataclass(frozen=True)
class EvidenceWindow:
    start: int
    end: int
    matches: tuple[MatchEvent, ...]


def merge_windows(
    text_length: int, events: Sequence[MatchEvent], *, context_chars: int,
) -> list[EvidenceWindow]:
    """Merge overlapping/adjacent contexts while retaining all term events."""
    if (
        not isinstance(context_chars, int) or isinstance(context_chars, bool)
        or not 0 <= context_chars <= 4096
    ):
        raise ValueError("context_chars must be an integer in 0..4096")
    if text_length < 0:
        raise ValueError("text length must be nonnegative")
    unique = sorted(set(events), key=lambda event: (event.start, event.end, event.term))
    windows: list[EvidenceWindow] = []
    for event in unique:
        if not 0 <= event.start <= event.end <= text_length:
            raise ValueError("match event lies outside the readable chain")
        start = max(0, event.start - context_chars)
        end = min(text_length, event.end + context_chars)
        if windows and start <= windows[-1].end:
            previous = windows[-1]
            windows[-1] = EvidenceWindow(
                previous.start, max(previous.end, end), (*previous.matches, event),
            )
        else:
            windows.append(EvidenceWindow(start, end, (event,)))
    return windows


def window_fragments(chain: TextChain, start: int, end: int) -> list[dict[str, Any]]:
    """Map a chain range to exact original chunk-local source coordinates.

    The adapter serializes each returned row with existing trust/provenance rules.
    Line numbers are 1-based LF-delimited lines in the original readable chunk.
    Stored separators remain explicit occurrence-edge text, never chunk text.
    """
    if not 0 <= start <= end <= chain.text_length:
        raise ValueError("window range lies outside the readable chain")
    fragments: list[dict[str, Any]] = []
    for piece in chain.pieces:
        lo, hi = max(start, piece.start), min(end, piece.end)
        if lo >= hi:
            continue
        relative = lo - piece.start
        occurrence = piece.occurrence
        fragment: dict[str, Any] = {
            "text": piece.text[relative:relative + hi - lo],
            "chain_range": [lo, hi],
            "record_id": occurrence.record_id,
            "edge_id": occurrence.edge_id,
            "row": occurrence.row,
        }
        if piece.chunk_start is None:
            fragment["kind"] = "separator"
        else:
            chunk_start = piece.chunk_start + relative
            chunk_end = chunk_start + hi - lo
            fragment.update({
                "kind": "chunk", "chunk_id": occurrence.chunk_id,
                "range": [chunk_start, chunk_end],
                "line_range": [
                    1 + occurrence.text.count("\n", 0, chunk_start),
                    1 + occurrence.text.count("\n", 0, max(chunk_start, chunk_end - 1)),
                ],
            })
        fragments.append(fragment)
    return fragments


def supporting_locations(chain: TextChain, start: int, end: int) -> list[dict[str, Any]]:
    """Retain all original occurrence locations, including verified overlap.

    These are attribution aliases, not repeated text or new logical match events.
    Unknown-position chains have one occurrence with chain-local coordinates.
    """
    if not 0 <= start <= end <= chain.text_length:
        raise ValueError("window range lies outside the readable chain")
    out: list[dict[str, Any]] = []
    for occurrence in chain.occurrences:
        offset = 0
        if chain.source_start is not None and occurrence.char_offset is not None:
            offset = occurrence.char_offset - chain.source_start
        lo, hi = max(start, offset), min(end, offset + len(occurrence.text))
        if lo >= hi:
            continue
        out.append({
            "chunk_id": occurrence.chunk_id, "edge_id": occurrence.edge_id,
            "range": [lo - offset, hi - offset], "row": occurrence.row,
        })
    return out
