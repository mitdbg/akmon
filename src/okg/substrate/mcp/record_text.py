"""`inspect(target={"kind":"text"})`: one record's text in reading order.

Issue #2668. An agent that found a ticket or document could read its text only
one chunk per call. This read returns the record's `document_chunk` text as
ordered segments, fills each page up to the call's byte budget, resumes with a
signed cursor, and works under Nomos enforce by filtering and redacting each
row the way every other row read does.

How the pieces fit:

* **Occurrences.** A chunk belongs to a record through a `member_of` edge
  (chunk -> record, which carries the occurrence's position) or a `contains`
  edge (record -> chunk). A chunk node is shared by every record whose text
  contains it, so the node's own `char_offset` names only one occurrence; the
  edge is the authority, and the node is used only when the edge has nothing.
* **Order.** `chunk_ordinal` (emission order, written on every doc-corpus
  `member_of` edge since emission schema 6) when every visible occurrence has
  one; else `char_offset` when every visible occurrence has a distinct one;
  else `chunk_id`, reported as `order_basis: "none"` rather than passed off as
  reading order. Graphs parsed before doc-corpus reparse epoch 4 carry
  `char_offset` 0 on every parser-split chunk, which is why distinctness is
  required.
* **Overlap.** Window chunkers repeat the tail of one chunk at the head of the
  next. A segment is trimmed only when both it and its predecessor carry exact
  offsets, the offsets increase and overlap, and the predecessor's text really
  ends with the characters being cut. Anything else is returned whole, and
  `trimmed_chars` reports every cut.
* **Separators.** A doc-corpus edge may store `separator_before` /
  `separator_after`: the whitespace between two chunks' spans (a PDF's blank
  line between pages) or after the last one. The read puts a separator back
  only when the spans say exactly that many characters are missing there, and
  reports it as `separator_before_chars` / `separator_after_chars`.
* **Coverage.** Positions decide it, never the chunk count alone. `complete`
  means the chunks' exact spans (`char_end - char_offset == len(text)`), after
  trimming and separators, run from 0 to the edge's `doc_char_length` with no
  gap and no leftover overlap: the served text is the document's text.
  `partial` means a known gap: spans that skip text, spans that stop short of
  `doc_char_length`, or (with no usable positions) a `chunk_ordinal` missing
  from 0..`chunk_total`-1. A paragraph repeated inside one record is one
  content-addressed node with one `member_of` edge, so one occurrence never
  reaches the graph; the read sees that gap and cannot restore the copy.
  `unknown` means nothing proves either: no positions (HTML paragraphs,
  structural chunkers), no `doc_char_length`, or positions that overlap
  without a proven repeat. Only a span whose occurrence edge carries
  `doc_char_length` counts: the doc-corpus source writes it only where the
  chunk's text is exactly the document text at that span, so a span that
  names other text (a LaTeX table's source) or predates reparse epoch 4
  proves nothing. Chunks with no span (a PDF table transcription, an image
  or enricher caption) are served in the read too, in their chunk order,
  but neither fill nor open a gap: a read can say `complete` and still carry
  them, because `complete` is about the document's text, not about every
  chunk being text from it.
* **Nomos.** Occurrence edges and chunk nodes are decided once per decision
  class (`filter_rows_by_read_class`): one representative row per class goes
  through the per-row evaluator, and every other row gets the per-row
  filter's answer and attrs (Refs #3002). Hidden rows are dropped and
  redacted attrs keep their marker and are never trimmed. A read's first
  page decides and records the classes; its continuation pages reuse that
  filtered text from `record_text_cache`, keyed by the reader, the policy
  fingerprint and the enforcement mode, and decide nothing again.
  The whole visible list is built before paging,
  so page boundaries, `complete` and the cursor depend only on rows the caller
  may see. Every position this read reports (`chunk_ordinal`, `chunk_total`,
  `char_offset`) is an attribute of a VISIBLE edge, readable already through a
  one-hop `expand`, so a gap it shows discloses nothing new. The one raw-row
  dependency is the occurrence ceiling below, which refuses a record with more
  than `MAX_RECORD_OCCURRENCES` raw occurrences before filtering. A chunk
  target's parent list obeys the same ceiling with the same refusal and adds
  no raw-row signal of its own: `parents_complete` counts visible parents only.
* **A chunk as the target.** Agents reach chunks through search and pass the
  chunk id here. A chunk has no chunks of its own, so the record path would
  answer with an empty list; instead the read returns the chunk's own text as
  one segment (`order_basis: "single_chunk"`) and lists the visible records it
  belongs to under `parents`, each with its position, so the next call can
  read the whole record.
* **Lines.** A line is a line of the text this read serves, in reading
  order, numbered from 1 and split on the line feed character only
  (`number_lines` is the one place that counts). A segment that does not continue the one before it
  starts a new line. The numbers are the source text's own only when the
  spans prove the served text is that text and nothing else
  (`line_basis`); otherwise the reply says they are lines of the shown
  text. They are counted after the read filter, over the text this reader
  is served: a hidden chunk has no lines here, so no line number moves with
  the amount of text a reader cannot see. `lines` on the target reads
  chosen ranges (`line_spans`), each piece naming the lines it holds.
* **No text.** Any other target with no visible chunk text answers
  `coverage: "none"` and an `outcome` that names the node and the next call;
  hidden and absent chunks give the same answer.
"""

from __future__ import annotations

import bisect
import copy
import hashlib
import json
import re
import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

import psycopg

from okg.substrate.attributes import apply_effective_attrs_to_rows
from okg.substrate.mcp.record_text_cache import (
    OverlayCapture,
    RecordDocument,
    cache_key as _text_cache_key,
    current_capture as _overlay_capture,
    get as _text_cache_get,
    put as _text_cache_put,
)
from okg.substrate.errors import NodeNotFound, SubstrateError
from okg.substrate.mcp.continuation import (
    ContinuationTokenInvalid,
    consume_continuation,
    continuation_binding,
    issue_continuation,
    open_continuation,
)
from okg.substrate.mcp.excerpt_window import (
    UNIT_WHOLE_CHARS,
    best_match_span,
    centred_cut,
    matched_terms,
)
from okg.substrate.mcp.provenance import classify_sources, serialize_node_read_row
from okg.substrate.mcp.response_bounds import (
    InvalidOperatorArguments,
    public_call_tool_result_bytes,
)
from okg.substrate.mcp.result_text import (  # noqa: F401  (the vocabularies, re-exported)
    LINE_BASES,
    LINES_OF_SHOWN_TEXT,
    LINES_OF_SOURCE,
    MATCH_LINE_BASES,
    MATCH_LINE_IN_OWN_TEXT,
    MATCH_LINE_IN_PASSAGE,
)
from okg.substrate.mcp.session import Session, assert_session_matches
from okg.substrate.commit_scope import read_pass_scope
from okg.substrate.nomos.read_class import read_class_select_sql
from okg.substrate.nomos.read_filter import (
    filter_node_rows_for_session,
    filter_rows_by_read_class,
)
from okg.substrate.nomos.read_masking import REDACTION_MARKER
from okg.substrate.transcript_trust import UNTRUSTED_CONTENT_ORIGIN_ATTR

#: Raw occurrence rows one record may have. Larger records are refused by
#: name, never truncated: a silently shortened "whole record" is the failure
#: this read exists to remove.
MAX_RECORD_OCCURRENCES = 5_000

#: Bytes kept free beyond the MEASURED size of what `inspect` stamps on after
#: this function returns (the server measures freshness and the advanced
#: generation notice on the live session and adds this margin). The margin
#: covers what varies between the measurement and the real stamp: the
#: freshness age digits and a cursor token that compresses worse than the
#: sample. A direct Python caller that passes no reserve gets this margin
#: plus a flat 1 KiB, the pre-measurement allowance.
TEXT_READ_RESERVE_MARGIN = 256
_DEFAULT_RESERVE_BYTES = 1_024 + TEXT_READ_RESERVE_MARGIN

_CHUNK_SUBTYPE = "document_chunk"
#: Parents one chunk read lists. A boilerplate chunk can belong to many
#: records; the read names the first few visible ones (by node id), and
#: `parents_complete` says whether more VISIBLE parents exist. The raw rows it
#: loads obey `MAX_RECORD_OCCURRENCES`, refused by name like a record's.
MAX_CHUNK_PARENTS = 20
_NODE_COLUMNS = "node_id, subtype, attrs, source, source_record_id, source_revision"
_EDGE_COLUMNS = (
    "e.edge_id, e.src, e.dst, e.edge_type, e.attrs, e.source, "
    "e.source_record_id, e.source_revision"
)


class RecordTextTooLarge(SubstrateError):
    """The record has more raw chunk occurrences than one read will load."""

    code = "record_text_too_large"


class RecordTextBudgetTooSmall(SubstrateError):
    """Not even one character of text fits the call's byte budget."""

    code = "record_text_budget_too_small"


class TextLocationStale(SubstrateError):
    """An `at` location that does not hold at this generation (D10).

    `data.reason` is `chunk_not_found` (no chunk you can read with that id
    here, or not in the record named; a hidden chunk, a chunk of a hidden
    record and a missing one give this same answer), `hash_mismatch` (the
    chunk's text is not the text the location was taken from) or
    `offset_out_of_range`."""

    code = "text_location_stale"


#: Line ranges one text read takes (`lines`).
MAX_LINE_RANGES = 20
#: The largest line number `lines` names. A larger one is refused by name:
#: it could only be a mistake, and its digits would otherwise eat the page.
MAX_LINE_NUMBER = 10_000_000
#: The `last` of an open range (`[first, null]`: to the end of the text).
OPEN_END = MAX_LINE_NUMBER + 1

#: What one page of search matches may load to turn match lines into record
#: lines (`record_match_lines`). One record is loaded only when a size read
#: says it has at most `MATCH_LINE_RECORD_CHUNKS` chunks holding at most
#: `MATCH_LINE_RECORD_BYTES` bytes of text; no further record is loaded once
#: the records loaded for the page hold `MATCH_LINE_SEGMENT_BUDGET` segments
#: or `MATCH_LINE_BYTE_BUDGET` bytes. So a page loads less than the page
#: budget plus one record's cap.
MATCH_LINE_RECORD_CHUNKS = 500
MATCH_LINE_RECORD_BYTES = 1024 * 1024
MATCH_LINE_SEGMENT_BUDGET = 10_000
MATCH_LINE_BYTE_BUDGET = 8 * 1024 * 1024

#: Characters read before an `at` offset, inside its chunk, so the window
#: is around the place rather than starting on it.
AT_LEAD_CHARS = 200

#: The only hash form a location carries (`chunk_text_hash`). Anything else
#: is a malformed argument, refused before any read, not a stale location.
_HASH_FORM = re.compile(r"sha256:[0-9a-f]{64}")


def chunk_text_hash(text: str) -> str:
    """The hash a location carries: SHA-256 of the chunk's text as the reader
    sees it (effective attrs, read filter applied), so a location taken from
    one text is refused against another."""

    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def chunk_location(row: Mapping[str, Any], offset: int) -> Optional[dict[str, Any]]:
    """`{"chunk", "offset", "hash"}` for a search hit on `row`, or None.

    Only a `document_chunk` with text has a location: it is the unit
    `inspect(kind="text")` reads. `row` carries the reader's attrs."""

    if row.get("subtype") != _CHUNK_SUBTYPE:
        return None
    text = _attrs(row).get("text")
    if not isinstance(text, str) or not text or not 0 <= offset < len(text):
        return None
    return {
        "chunk": str(row["node_id"]),
        "offset": int(offset),
        "hash": chunk_text_hash(text),
    }


@dataclass(frozen=True)
class _At:
    chunk: str
    offset: int
    hash: Optional[str]


def _parse_at(at: Any) -> _At:
    if isinstance(at, Mapping):
        chunk, offset, digest = at.get("chunk"), at.get("offset"), at.get("hash")
        if (
            isinstance(chunk, str) and chunk
            and isinstance(offset, int) and not isinstance(offset, bool)
            and offset >= 0
            and (digest is None or (
                isinstance(digest, str) and _HASH_FORM.fullmatch(digest) is not None
            ))
            and set(at) <= {"chunk", "offset", "hash"}
        ):
            return _At(chunk, offset, digest)
    raise InvalidOperatorArguments(
        "inspect text: at must be {chunk, offset[, hash]} with a chunk id, "
        "a character offset of 0 or more and, when given, the hash exactly as "
        "search returned it (sha256: and 64 lowercase hex digits)",
        data={"tool": "inspect", "invalid_fields": ["target.at"], "at": at},
        fix_hint=(
            'pass the location a search hit carries, e.g. at={"chunk":"<chunk id>",'
            '"offset":412,"hash":"sha256:..."}'
        ),
    )


def _stale(at: _At, node_id: Optional[str], session: Session, reason: str) -> TextLocationStale:
    return TextLocationStale(
        f"location {at.chunk}:{at.offset} does not hold at generation "
        f"{session.generation_id} ({reason})",
        data={
            "chunk": at.chunk,
            "offset": at.offset,
            "node_id": node_id,
            "reason": reason,
            "pinned_generation_id": session.generation_id,
        },
        fix_hint=(
            "search again for a fresh location, or read the record from its "
            'start with inspect(target={"kind":"text","node_id":<record>})'
        ),
    )


@dataclass
class _Segment:
    chunk_id: str
    text: str
    source: Optional[str]
    untrusted_origin: Any
    chunk_ordinal: Optional[int]
    chunk_total: Optional[int]
    char_offset: Optional[int]
    char_end: Optional[int]
    heading_path: Optional[str]
    redacted: bool
    trimmed_chars: int = 0
    #: The document's text length, from the occurrence edge.
    doc_char_length: Optional[int] = None
    #: Whitespace the edge stores between the text before and this span.
    separator_before: Optional[str] = None
    #: Whitespace the edge stores after this span, at the document's end.
    separator_after: Optional[str] = None
    #: Characters of `separator_before` / `separator_after` put back into
    #: `text` (`fill_separators`).
    separator_before_chars: int = 0
    separator_after_chars: int = 0
    #: `char_end - char_offset == len(text)` for the text as stored, before
    #: any trim or separator changed it.
    exact_span: bool = False
    #: The line of the record's text this segment's first character is on,
    #: and whether the segment was put on a new line because it does not
    #: continue the one before it (`number_lines`).
    line_start: int = 1
    line_break_before: bool = False

    def __post_init__(self) -> None:
        self.exact_span = (
            self.char_offset is not None
            and self.char_end is not None
            and self.char_end - self.char_offset == len(self.text)
        )


def _int_or_none(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value)
    return None


def _attrs(row: Mapping[str, Any]) -> Mapping[str, Any]:
    attrs = row.get("attrs")
    return attrs if isinstance(attrs, Mapping) else {}


def _checkout(conn: psycopg.Connection, session: Session) -> Any:
    from okg.substrate.graph_store import ChronosGraphStore

    return ChronosGraphStore.from_connection(
        conn, ensure_schema=False
    ).checkout_generation(session.generation_id, branch=session.branch_name)


def _visible_start(
    conn: psycopg.Connection, session: Session, graph: Any, node_id: str,
    *, suggest: bool = True,
) -> dict[str, Any]:
    rows = [
        dict(row)
        for row in graph.query(
            f"SELECT {_NODE_COLUMNS} FROM okg.graph_nodes "
            "WHERE node_id = :node_id LIMIT 1",
            {"node_id": node_id},
        )
    ]
    # The same effective attributes the chunk reads below and inspect(node)
    # judge, so an accepted override is decided the same way everywhere.
    rows = _overlaid(
        conn,
        rows,
        target_kind="node",
        target_id_field="node_id",
        generation_id=session.generation_id,
    )
    visible = filter_node_rows_for_session(conn, session, rows)
    if not visible:
        # Absent and hidden are one answer, as in inspect(node), with the
        # same close-id hint (#3605).
        from okg.substrate.mcp.tools import close_node_ids, node_not_visible

        # `suggest=False` where the caller swallows the refusal: the close-id
        # read would cost a lookup nobody sees.
        raise node_not_visible(
            node_id, session.generation_id,
            close=close_node_ids(conn, session, node_id) if suggest else None,
        )
    return visible[0]


def _occurrence_edges(
    conn: psycopg.Connection, session: Session, graph: Any, node_id: str
) -> list[dict[str, Any]]:
    """Every raw member_of/contains edge between the record and a chunk."""

    edge_class = read_class_select_sql(graph, "okg.graph_edges", "e")
    params = {
        "node_id": node_id,
        "chunk_subtype": _CHUNK_SUBTYPE,
        "raw_limit": MAX_RECORD_OCCURRENCES + 1,
    }
    member = [
        dict(row)
        for row in graph.query(
            f"SELECT {_EDGE_COLUMNS}{edge_class} "
            "FROM okg.graph_edges e JOIN okg.graph_nodes c ON c.node_id = e.src "
            "WHERE e.dst = :node_id AND e.edge_type = 'member_of' "
            "AND c.subtype = :chunk_subtype "
            "ORDER BY e.edge_id LIMIT :raw_limit",
            params,
        )
    ]
    contains = [
        dict(row)
        for row in graph.query(
            f"SELECT {_EDGE_COLUMNS}{edge_class} "
            "FROM okg.graph_edges e JOIN okg.graph_nodes c ON c.node_id = e.dst "
            "WHERE e.src = :node_id AND e.edge_type = 'contains' "
            "AND c.subtype = :chunk_subtype "
            "ORDER BY e.edge_id LIMIT :raw_limit",
            params,
        )
    ]
    chunk_ids = {str(r["src"]) for r in member} | {str(r["dst"]) for r in contains}
    if len(chunk_ids) > MAX_RECORD_OCCURRENCES or max(
        len(member), len(contains)
    ) > MAX_RECORD_OCCURRENCES:
        raise RecordTextTooLarge(
            f"record {node_id!r} has more than {MAX_RECORD_OCCURRENCES} chunk "
            "occurrences; this read loads a record whole and refuses rather "
            "than returning part of it as if it were all",
            data={
                "node_id": node_id,
                "max_occurrences": MAX_RECORD_OCCURRENCES,
                "pinned_generation_id": session.generation_id,
            },
            fix_hint=(
                "search within the record, or page its chunks with "
                'expand(input=<record>, edge_type="contains", direction="out")'
            ),
        )
    rows = _overlaid(
        conn,
        member + contains,
        target_kind="edge",
        target_id_field="edge_id",
        generation_id=session.generation_id,
    )
    # `_segments` places a chunk by a visible member_of edge whenever it has
    # one, so a contains edge matters only for a chunk with none: decide only
    # those (issue #3002; a record's two directions doubled the filter work).
    # The result is the full filter's, minus contains edges no read uses.
    # One pass scope: both calls decide under the one bundle it loads.
    member_rows = [row for row in rows if row.get("edge_type") == "member_of"]
    with read_pass_scope():
        visible = filter_rows_by_read_class(conn, session, member_rows, row_kind="edge")
        placed = {str(row["src"]) for row in visible}
        fallback = [
            row for row in rows
            if row.get("edge_type") != "member_of" and str(row["dst"]) not in placed
        ]
        if fallback:
            visible += filter_rows_by_read_class(
                conn, session, fallback, row_kind="edge"
            )
    return visible


def _overlaid(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    target_kind: str,
    target_id_field: str,
    generation_id: Any,
) -> list[dict[str, Any]]:
    """Project effective attrs onto a build's rows (Refs #3002).

    When the build may be cached, the capture first records the overlay rows
    it is about to read, so the stored digest predates the read.
    """
    capture = _overlay_capture.get()
    if capture is not None:
        capture.before_overlay_read(
            conn, target_kind, [str(row[target_id_field]) for row in rows],
        )
    return apply_effective_attrs_to_rows(
        conn,
        rows,
        target_kind=target_kind,
        target_id_field=target_id_field,
        generation_id=generation_id,
    )


def _visible_chunks(
    conn: psycopg.Connection,
    session: Session,
    graph: Any,
    chunk_ids: list[str],
) -> dict[str, dict[str, Any]]:
    if not chunk_ids:
        return {}
    node_class = read_class_select_sql(graph, "okg.graph_nodes")
    rows = [
        dict(row)
        for row in graph.query(
            f"SELECT {_NODE_COLUMNS}{node_class} FROM okg.graph_nodes "
            "WHERE node_id = ANY(:ids) AND subtype = :chunk_subtype",
            {"ids": chunk_ids, "chunk_subtype": _CHUNK_SUBTYPE},
        )
    ]
    rows = _overlaid(
        conn,
        rows,
        target_kind="node",
        target_id_field="node_id",
        generation_id=session.generation_id,
    )
    return {
        str(row["node_id"]): row
        for row in filter_rows_by_read_class(conn, session, rows, row_kind="node")
    }


def _segments(
    edges: list[dict[str, Any]], chunks: Mapping[str, dict[str, Any]]
) -> list[_Segment]:
    """One segment per visible chunk, positioned by its best visible edge."""

    # A member_of edge carries the occurrence's position; a contains edge is
    # the fallback for producers that write only that direction.
    best: dict[str, dict[str, Any]] = {}
    for edge in edges:
        is_member = edge.get("edge_type") == "member_of"
        chunk_id = str(edge["src"] if is_member else edge["dst"])
        current = best.get(chunk_id)
        if current is None or (
            is_member and current.get("edge_type") != "member_of"
        ):
            best[chunk_id] = edge
    segments: list[_Segment] = []
    for chunk_id, edge in best.items():
        node = chunks.get(chunk_id)
        if node is None:
            continue
        node_attrs = _attrs(node)
        edge_attrs = _attrs(edge)
        redacted = bool(
            node_attrs.get(REDACTION_MARKER) or edge_attrs.get(REDACTION_MARKER)
        )

        # The node's own position names ONE occurrence of a chunk that may
        # be shared by several documents. An edge that carries an ordinal is
        # a doc-corpus occurrence that stores its own position when it has
        # one, so its missing position means "none", not "borrow another
        # document's".
        edge_is_authoritative = _int_or_none(edge_attrs.get("chunk_ordinal")) is not None

        def position(key: str) -> Optional[int]:
            value = _int_or_none(edge_attrs.get(key))
            if value is not None or edge_is_authoritative:
                return value
            return _int_or_none(node_attrs.get(key))

        def separator(key: str) -> Optional[str]:
            # Only the occurrence edge knows what surrounds this occurrence;
            # anything but plain whitespace is not a separator.
            value = edge_attrs.get(key)
            return value if isinstance(value, str) and value.isspace() else None

        text = node_attrs.get("text")
        heading = edge_attrs.get("heading_path") or node_attrs.get("heading_path")
        segments.append(
            _Segment(
                chunk_id=chunk_id,
                text=text if isinstance(text, str) else "",
                source=node.get("source"),
                untrusted_origin=node_attrs.get(UNTRUSTED_CONTENT_ORIGIN_ATTR),
                chunk_ordinal=_int_or_none(edge_attrs.get("chunk_ordinal")),
                chunk_total=_int_or_none(edge_attrs.get("chunk_total")),
                char_offset=position("char_offset"),
                char_end=position("char_end"),
                heading_path=heading if isinstance(heading, str) and heading else None,
                redacted=redacted,
                doc_char_length=_int_or_none(edge_attrs.get("doc_char_length")),
                separator_before=separator("separator_before"),
                separator_after=separator("separator_after"),
            )
        )
    return segments


def _chunk_self_segment(node: Mapping[str, Any]) -> list[_Segment]:
    """The target chunk's own text as one segment (empty when it has none)."""

    node_attrs = _attrs(node)
    text = node_attrs.get("text")
    if not isinstance(text, str) or not text:
        return []
    heading = node_attrs.get("heading_path")
    return [
        _Segment(
            chunk_id=str(node["node_id"]),
            text=text,
            source=node.get("source"),
            untrusted_origin=node_attrs.get(UNTRUSTED_CONTENT_ORIGIN_ATTR),
            # Positions differ per parent; they are reported under `parents`.
            chunk_ordinal=None,
            chunk_total=None,
            char_offset=None,
            char_end=None,
            heading_path=heading if isinstance(heading, str) and heading else None,
            redacted=bool(node_attrs.get(REDACTION_MARKER)),
        )
    ]


def _chunk_parents(
    conn: psycopg.Connection, session: Session, graph: Any, chunk_id: str
) -> tuple[list[dict[str, Any]], bool]:
    """The visible records `chunk_id` belongs to, with its position in each.

    Returns `(parents, complete)`. Both the occurrence edge and the parent
    node must be visible; `complete` is False only when more visible parents
    exist than `MAX_CHUNK_PARENTS`. Every raw occurrence is loaded and filtered
    before anything is counted, so hidden rows never change the answer; a
    chunk with more than `MAX_RECORD_OCCURRENCES` raw occurrences in either
    direction is refused by name, the same ceiling and refusal as a record."""

    params = {"chunk_id": chunk_id, "raw_limit": MAX_RECORD_OCCURRENCES + 1}
    edge_class = read_class_select_sql(graph, "okg.graph_edges", "e")
    node_class = read_class_select_sql(graph, "okg.graph_nodes")
    member = [
        dict(row)
        for row in graph.query(
            f"SELECT {_EDGE_COLUMNS}{edge_class} FROM okg.graph_edges e "
            "WHERE e.src = :chunk_id AND e.edge_type = 'member_of' "
            "ORDER BY e.edge_id LIMIT :raw_limit",
            params,
        )
    ]
    contains = [
        dict(row)
        for row in graph.query(
            f"SELECT {_EDGE_COLUMNS}{edge_class} FROM okg.graph_edges e "
            "WHERE e.dst = :chunk_id AND e.edge_type = 'contains' "
            "ORDER BY e.edge_id LIMIT :raw_limit",
            params,
        )
    ]
    if max(len(member), len(contains)) > MAX_RECORD_OCCURRENCES:
        raise RecordTextTooLarge(
            f"chunk {chunk_id!r} has more than {MAX_RECORD_OCCURRENCES} record "
            "occurrences; this read loads them whole and refuses rather than "
            "listing part of them as if it were all",
            data={
                "node_id": chunk_id,
                "max_occurrences": MAX_RECORD_OCCURRENCES,
                "pinned_generation_id": session.generation_id,
            },
            fix_hint=(
                'inspect(target={"kind":"node","node_id":<this chunk>}) returns '
                'its text in attrs; page its records with expand(input=<this '
                'chunk>, edge_type="member_of", direction="out")'
            ),
        )
    edges = apply_effective_attrs_to_rows(
        conn,
        member + contains,
        target_kind="edge",
        target_id_field="edge_id",
        generation_id=session.generation_id,
    )
    edges = filter_rows_by_read_class(conn, session, edges, row_kind="edge")
    best: dict[str, dict[str, Any]] = {}
    for edge in edges:
        is_member = edge.get("edge_type") == "member_of"
        parent_id = str(edge["dst"] if is_member else edge["src"])
        current = best.get(parent_id)
        if current is None or (
            is_member and current.get("edge_type") != "member_of"
        ):
            best[parent_id] = edge
    if not best:
        return [], True
    rows = [
        dict(row)
        for row in graph.query(
            f"SELECT {_NODE_COLUMNS}{node_class} FROM okg.graph_nodes "
            "WHERE node_id = ANY(:ids)",
            {"ids": sorted(best)},
        )
    ]
    rows = apply_effective_attrs_to_rows(
        conn,
        rows,
        target_kind="node",
        target_id_field="node_id",
        generation_id=session.generation_id,
    )
    visible = {
        str(row["node_id"]): row
        for row in filter_rows_by_read_class(conn, session, rows, row_kind="node")
    }
    parents: list[dict[str, Any]] = []
    for parent_id in sorted(visible):
        edge_attrs = _attrs(best[parent_id])
        entry: dict[str, Any] = {
            "node_id": parent_id,
            "subtype": visible[parent_id].get("subtype"),
            "edge_type": best[parent_id].get("edge_type"),
        }
        for key in ("chunk_ordinal", "chunk_total", "char_offset", "char_end"):
            value = _int_or_none(edge_attrs.get(key))
            if value is not None:
                entry[key] = value
        parents.append(entry)
    complete = len(parents) <= MAX_CHUNK_PARENTS
    return parents[:MAX_CHUNK_PARENTS], complete


def order_segments(segments: list[_Segment]) -> tuple[list[_Segment], str]:
    """Sort into reading order and name the key that decided it."""

    if segments and all(s.chunk_ordinal is not None for s in segments):
        return (
            sorted(segments, key=lambda s: (s.chunk_ordinal, s.chunk_id)),
            "chunk_ordinal",
        )
    ordered_part = [s for s in segments if s.chunk_ordinal is not None]
    if ordered_part:
        # Chunks added after the corpus wrote its ordinals (an enricher's
        # figure caption) carry none. Keep the record's own order for the rest
        # and put the unplaced ones after it, rather than dropping the whole
        # record to id order; the basis says so.
        rest = [s for s in segments if s.chunk_ordinal is None]
        return (
            sorted(ordered_part, key=lambda s: (s.chunk_ordinal, s.chunk_id))
            + sorted(rest, key=lambda s: s.chunk_id),
            "chunk_ordinal_then_unplaced",
        )
    offsets = [s.char_offset for s in segments]
    if (
        segments
        and all(o is not None for o in offsets)
        and len(set(offsets)) == len(offsets)
    ):
        return sorted(segments, key=lambda s: (s.char_offset, s.chunk_id)), "char_offset"
    return sorted(segments, key=lambda s: s.chunk_id), "none"


def _exact(segment: _Segment) -> bool:
    return segment.exact_span


def _tiles(segment: _Segment) -> bool:
    """A readable, non-empty segment whose stored span is its text exactly.

    The span must come with `doc_char_length`: the source writes that only
    where it checked the chunk text against the document text at that span,
    so a same-length span naming other text cannot pass as exact."""

    return (
        segment.exact_span
        and segment.doc_char_length is not None
        and not segment.redacted
        and segment.char_end != segment.char_offset
    )


def trim_overlap(segments: list[_Segment]) -> None:
    """Cut the characters a segment repeats from its predecessor, in place.

    Only a proven repeat is cut: exact offsets on both, increasing, overlapping,
    and the predecessor's text really ends with the characters removed."""

    # Decide every cut against the untrimmed texts first, then apply: a
    # trimmed predecessor no longer matches its own offsets.
    cuts: list[tuple[_Segment, int]] = []
    for previous, current in zip(segments, segments[1:]):
        if previous.redacted or current.redacted:
            continue
        if not (_exact(previous) and _exact(current)):
            continue
        assert previous.char_offset is not None and current.char_offset is not None
        assert previous.char_end is not None
        if not previous.char_offset < current.char_offset < previous.char_end:
            continue
        overlap = previous.char_end - current.char_offset
        if overlap > len(current.text):
            continue
        if not previous.text.endswith(current.text[:overlap]):
            continue
        cuts.append((current, overlap))
    for current, overlap in cuts:
        current.text = current.text[overlap:]
        current.trimmed_chars = overlap


def fill_separators(segments: list[_Segment]) -> None:
    """Put stored separators back into the text, in place.

    A separator goes back only where the spans prove it is missing: between
    the text covered so far and the next exact span when the distance is
    exactly the separator's length, and after the furthest span when the
    document's `doc_char_length` is exactly that far beyond it. Run after
    `trim_overlap`, on segments in reading order."""

    covered = 0
    furthest: Optional[_Segment] = None
    for segment in segments:
        if not _tiles(segment):
            continue
        assert segment.char_offset is not None and segment.char_end is not None
        start = segment.char_offset + segment.trimmed_chars
        sep = segment.separator_before
        if sep is not None and start - covered == len(sep):
            segment.text = sep + segment.text
            segment.separator_before_chars = len(sep)
        if segment.char_end > covered:
            covered = segment.char_end
            furthest = segment
    if furthest is None:
        return
    tail = furthest.separator_after
    length = furthest.doc_char_length
    if tail is not None and length is not None and length - covered == len(tail):
        furthest.text = furthest.text + tail
        furthest.separator_after_chars = len(tail)


def _position_coverage(segments: list[_Segment]) -> str:
    """`complete`, `partial` or `unknown`, from the stored spans alone."""

    tiles = [s for s in segments if _tiles(s)]
    if not tiles:
        return "unknown"
    covered = 0
    gap = overlap = False
    for segment in tiles:
        assert segment.char_offset is not None and segment.char_end is not None
        start = (
            segment.char_offset
            + segment.trimmed_chars
            - segment.separator_before_chars
        )
        if start > covered:
            gap = True
        elif start < covered:
            overlap = True
        covered = max(covered, segment.char_end + segment.separator_after_chars)
    if gap:
        return "partial"
    lengths = {s.doc_char_length for s in tiles}
    if overlap or len(lengths) != 1:
        return "unknown"
    (length,) = lengths
    assert length is not None
    if covered < length:
        return "partial"
    return "complete" if covered == length else "unknown"


def _ordinal_gap(segments: list[_Segment]) -> bool:
    """True when the chunk numbering proves an occurrence is missing."""

    placed = [s for s in segments if s.chunk_ordinal is not None]
    if not placed:
        return False
    totals = {s.chunk_total for s in placed}
    if len(totals) == 1 and None not in totals:
        (total,) = totals
        assert total is not None
        return {s.chunk_ordinal for s in placed} != set(range(total))
    return sorted(s.chunk_ordinal for s in placed) != list(range(len(placed)))


def coverage_of(segments: list[_Segment], order_basis: str) -> str:
    """`complete`, `partial` (a known gap), `unknown`, or `none` (no text).

    Only positions prove `complete`: every chunk arriving (the ordinals) says
    nothing about whether the chunks spell the document. The ordinals can
    still prove a gap when the positions cannot decide."""

    if not segments:
        return "none"
    if order_basis == "single_chunk":
        return "complete"
    verdict = _position_coverage(segments)
    if verdict != "unknown":
        return verdict
    return "partial" if _ordinal_gap(segments) else "unknown"


def _continues(previous: _Segment, current: _Segment) -> bool:
    """`current`'s text starts at the character `previous`'s text ends on."""

    if not (_tiles(previous) and _tiles(current)):
        return False
    assert previous.char_end is not None and current.char_offset is not None
    start = (
        current.char_offset + current.trimmed_chars - current.separator_before_chars
    )
    return start == previous.char_end + previous.separator_after_chars


def number_lines(segments: Sequence[_Segment]) -> int:
    """Set every segment's `line_start`, in place; return the last line.

    The one definition of a line: a line of the text this read serves, in
    reading order, numbered from 1 and split on the line feed character
    only. Segments whose spans run on (`_continues`) are one text, so a line
    may cross from one into the next. Any other segment starts a new line:
    nothing says its first words belong on the line before. Run after
    `trim_overlap` and `fill_separators`, on the segments this reader is
    served."""

    newlines = 0
    #: The text so far ends inside a line.
    open_line = False
    previous: Optional[_Segment] = None
    for segment in segments:
        segment.line_break_before = bool(
            open_line and previous is not None and not _continues(previous, segment)
        )
        if segment.line_break_before:
            newlines += 1
            open_line = False
        segment.line_start = newlines + 1
        if segment.text:
            newlines += segment.text.count("\n")
            open_line = not segment.text.endswith("\n")
        previous = segment
    return newlines + (1 if open_line else 0)


def line_at(segment: _Segment, char: int) -> int:
    """The line the character at `char` of a numbered segment is on."""

    return segment.line_start + segment.text.count("\n", 0, char)


def line_count(segments: Sequence[_Segment]) -> int:
    """The last line number of numbered segments (0 when they hold no text)."""

    for segment in reversed(segments):
        if segment.text:
            return line_at(segment, len(segment.text) - 1)
    return 0


def line_basis(
    segments: Sequence[_Segment], coverage: str, order_basis: str,
) -> dict[str, Any]:
    """What the line numbers of a read are lines of.

    `source` only when the served text is the document's text and nothing
    else: coverage is `complete` and every segment is a span of it. A chunk
    read alone, a read with a gap, and a read that carries chunks the
    document's text does not hold (a table transcription, a caption) all
    number the shown text, and say so. Why is already on the page: its
    `coverage` and `order_basis`."""

    if (
        order_basis != "single_chunk" and coverage == "complete"
        and all(_tiles(segment) for segment in segments)
    ):
        return {"basis": LINES_OF_SOURCE}
    return {"basis": LINES_OF_SHOWN_TEXT}


_LINES_FORM = (
    f"lines must be 1 to {MAX_LINE_RANGES} ranges [[first, last], ...] of whole "
    f"line numbers from 1 to {MAX_LINE_NUMBER}, each with first <= last, in "
    "ascending order and not overlapping; the last range may be [first, null], "
    "from first to the end"
)


def parse_lines(lines: Any, *, field: str = "target.lines") -> tuple[tuple[int, int], ...]:
    """The validated `lines` of a text read, or a refusal naming the form.

    An open end (`[first, null]`) comes back as `OPEN_END`."""

    def whole(value: Any) -> bool:
        return (
            isinstance(value, int) and not isinstance(value, bool)
            and 1 <= value <= MAX_LINE_NUMBER
        )

    ok = isinstance(lines, (list, tuple)) and 1 <= len(lines) <= MAX_LINE_RANGES
    ranges: list[tuple[int, int]] = []
    if ok:
        for item in lines:
            if not (
                isinstance(item, (list, tuple)) and len(item) == 2
                and whole(item[0])
                and (item[1] is None or (whole(item[1]) and item[0] <= item[1]))
                # Nothing follows an open end: it would overlap it.
                and (not ranges or item[0] > ranges[-1][1])
            ):
                ok = False
                break
            ranges.append((item[0], OPEN_END if item[1] is None else item[1]))
    if not ok:
        raise InvalidOperatorArguments(
            f"inspect text: {_LINES_FORM}",
            # The value is not echoed: it may be a number of any length.
            data={"tool": "inspect", "invalid_fields": [field]},
            fix_hint=(
                'pass e.g. target={"kind":"text","node_id":"<record node id>",'
                '"lines":[[1278,1302],[1555,1578]]}'
            ),
        )
    return tuple(ranges)


def asked_lines(ranges: Sequence[tuple[int, int]]) -> list[list[Optional[int]]]:
    """Validated ranges as the caller wrote them (an open end is null)."""

    return [[first, None if last == OPEN_END else last] for first, last in ranges]


def lines_past_end(ranges: Sequence[tuple[int, int]], total: int) -> bool:
    """A range names a line after the last one (an open end names none)."""

    first, last = ranges[-1]
    return first > total if last == OPEN_END else last > total


def parse_line_target(
    lines: Any, at: Any, *, field: str = "target",
) -> Optional[tuple[tuple[int, int], ...]]:
    """A text target's validated `lines`, or None when it names none.

    `lines` beside `at` is refused: one reads chosen lines of the record,
    the other reads on from a search hit. `field` is the target's path in
    the call (a batch item's is `target.items[n]`)."""

    if lines is None:
        return None
    if at is not None:
        raise InvalidOperatorArguments(
            "inspect text: lines and at cannot be combined; lines reads chosen "
            "lines of the record, at reads on from a search hit",
            data={"tool": "inspect", "invalid_fields": [f"{field}.lines", f"{field}.at"]},
            fix_hint=(
                'pass one of them: {"kind":"text","node_id":<record>,'
                '"lines":[[first,last]]} or {"kind":"text","at":<location>}'
            ),
        )
    return parse_lines(lines, field=f"{field}.lines")


#: `matching` on a text read (compops-read-fixes): read only
#: the lines that match, with `context` lines around each, as `rg -n -C`
#: would. The windows are an ordinary line read of the same text, so every
#: number is a line `lines` takes back. At most `MAX_LINE_RANGES` windows a
#: read; `MATCHED_LINES_LISTED` matched line numbers are listed in all.
MAX_MATCHING_CHARS = 200
MAX_MATCHING_CONTEXT = 20
DEFAULT_MATCHING_CONTEXT = 2
MATCHED_LINES_LISTED = 200


@dataclass(frozen=True)
class LineMatching:
    """A validated `matching`: its form as given, how many lines of context."""

    pattern: str
    context: int
    test: Any

    def signed(self) -> dict[str, Any]:
        return {"matching": self.pattern, "context": self.context}


def parse_matching(
    matching: Any, context: Any, *, lines: Any, at: Any, field: str = "target",
) -> Optional[LineMatching]:
    """A text target's validated `matching`, or None when it names none.

    A literal matches a line holding it, in any case; `re:/<regex>/` is a
    regex, also in any case. `matching` reads its own lines, so it takes
    neither `lines` nor `at`."""

    if matching is None:
        if context is not None:
            raise InvalidOperatorArguments(
                "inspect text: context is the lines around each matching line; "
                "it needs matching",
                data={"tool": "inspect", "invalid_fields": [f"{field}.context"]},
                fix_hint='add "matching": "<text or re:/regex/>", or drop context',
            )
        return None
    form = (
        'target={"kind":"text","node_id":"<record>","matching":"<text>" or '
        '"re:/<regex>/","context":<0-' + str(MAX_MATCHING_CONTEXT) + ">}"
    )
    if lines is not None or at is not None:
        raise InvalidOperatorArguments(
            "inspect text: matching reads the lines that match; it cannot be "
            "combined with lines or at",
            data={"tool": "inspect", "invalid_fields": [
                f"{field}.matching", f"{field}.lines" if lines is not None else f"{field}.at",
            ]},
            fix_hint="pass one of them",
        )
    if (
        not isinstance(matching, str) or not matching.strip()
        or len(matching) > MAX_MATCHING_CHARS
    ):
        raise InvalidOperatorArguments(
            f"inspect text: matching is text of 1-{MAX_MATCHING_CHARS} characters",
            data={"tool": "inspect", "invalid_fields": [f"{field}.matching"]},
            fix_hint="pass e.g. " + form,
        )
    if context is None:
        context = DEFAULT_MATCHING_CONTEXT
    if (
        not isinstance(context, int) or isinstance(context, bool)
        or not 0 <= context <= MAX_MATCHING_CONTEXT
    ):
        raise InvalidOperatorArguments(
            f"inspect text: context is a whole number of lines, 0-{MAX_MATCHING_CONTEXT}",
            data={"tool": "inspect", "invalid_fields": [f"{field}.context"]},
            fix_hint="pass e.g. " + form,
        )
    if matching.startswith("re:/") and matching.endswith("/") and len(matching) > 5:
        from okg.substrate.external_backed.live_dispatch import _backtracks

        body = matching[4:-1]
        try:
            compiled = re.compile(body, re.IGNORECASE)
        except re.error as exc:
            raise InvalidOperatorArguments(
                f"inspect text: matching is not a valid regex ({exc.msg})",
                data={"tool": "inspect", "invalid_fields": [f"{field}.matching"]},
                fix_hint="fix the regex, or pass the text itself",
            ) from exc
        if _backtracks(body):
            raise InvalidOperatorArguments(
                "inspect text: matching repeats a group that itself repeats or "
                "branches (e.g. (a+)+ or (a|b)*), which can take very long",
                data={"tool": "inspect", "invalid_fields": [f"{field}.matching"]},
                fix_hint="use a|b at the top level, or a plain repeat",
            )
        test = compiled.search
    else:
        # `a|b` is either text, as `rg 'a|b'` reads it (in one eval, 21
        # of 74 reads wrote it so and matched nothing); `||` stays literal.
        parts = (
            [part.casefold() for part in matching.split("|") if part.strip()]
            if "|" in matching and "||" not in matching else [matching.casefold()]
        ) or [matching.casefold()]

        def test(line: str) -> bool:
            folded = line.casefold()
            return any(part in folded for part in parts)

    return LineMatching(matching, context, test)


def matching_ranges(
    segments: Sequence["_Segment"], matching: LineMatching, total: int,
) -> tuple[tuple[tuple[int, int], ...], dict[str, Any]]:
    """The line ranges a `matching` read serves, and its `matching` block.

    Lines are numbered as every text read numbers them (`number_lines`). A
    window is a matched line with `context` lines each side; windows that
    touch merge. Past `MAX_LINE_RANGES` windows, the block names the first
    line no window holds, so the rest is one more read away."""

    texts: dict[int, str] = {}
    for segment in segments:
        if not segment.text:
            continue
        for offset, piece in enumerate(segment.text.split("\n")):
            number = segment.line_start + offset
            texts[number] = texts.get(number, "") + piece
    matched = [number for number in sorted(texts) if matching.test(texts[number])]
    windows: list[list[int]] = []
    for number in matched:
        first = max(1, number - matching.context)
        last = min(max(total, number), number + matching.context)
        if windows and first <= windows[-1][1] + 1:
            windows[-1][1] = max(windows[-1][1], last)
        else:
            windows.append([first, last])
    shown = windows[:MAX_LINE_RANGES]
    block: dict[str, Any] = {
        "pattern": matching.pattern,
        "context": matching.context,
        "matched": len(matched),
        "matched_lines": matched[:MATCHED_LINES_LISTED],
        "windows": len(windows),
        "windows_shown": len(shown),
    }
    if len(matched) > MATCHED_LINES_LISTED:
        block["matched_lines_cut"] = True
    if len(windows) > len(shown):
        later = [number for number in matched if number > shown[-1][1]]
        block["unshown_from_line"] = later[0]
        block["unshown_matches"] = len(later)
    return tuple((first, last) for first, last in shown), block


@dataclass(frozen=True)
class LineSpan:
    """Characters `start` to `end` of one segment, and the `lines` range
    (by index) that asked for them; -1 on a read that names no lines."""

    segment: int
    start: int
    end: int
    range: int = -1


def _after_newlines(text: str, count: int) -> int:
    """The index just past the `count`-th line feed of `text` (its length
    when it has fewer)."""

    at = 0
    for _ in range(count):
        found = text.find("\n", at)
        if found < 0:
            return len(text)
        at = found + 1
    return at


def line_spans(
    segments: Sequence[_Segment], ranges: Sequence[tuple[int, int]],
) -> list[LineSpan]:
    """The parts of numbered segments that hold the lines of `ranges`.

    A line keeps its line feed. A range past the last line gives what
    exists; a range wholly past it gives nothing. A line that crosses
    segments is in a piece of each."""

    starts = [segment.line_start for segment in segments]
    spans: list[LineSpan] = []
    for number, (first, last) in enumerate(ranges):
        index = max(0, bisect.bisect_left(starts, first) - 1)
        while index < len(segments) and starts[index] <= last:
            segment = segments[index]
            text = segment.text
            if text and line_at(segment, len(text) - 1) >= first:
                begin = _after_newlines(text, first - segment.line_start)
                end = _after_newlines(text, last - segment.line_start + 1)
                if begin < end:
                    spans.append(LineSpan(index, begin, end, number))
            index += 1
    return spans


def spans_from(spans: Sequence[LineSpan], cursor: tuple[int, int]) -> list[LineSpan]:
    """`spans` from a cursor on: the first one that ends after it, cut to
    start there, and every later one."""

    for index, span in enumerate(spans):
        if (span.segment, span.end) > cursor:
            start = max(span.start, cursor[1]) if span.segment == cursor[0] else span.start
            return [LineSpan(span.segment, start, span.end, span.range), *spans[index + 1:]]
    return []


def span_lines(segments: Sequence[_Segment], span: LineSpan, end: int) -> tuple[int, int]:
    """The first and last line the piece of `span` up to `end` holds."""

    segment = segments[span.segment]
    return line_at(segment, span.start), line_at(segment, max(span.start, end - 1))


def chunk_offset_line(
    segments: Sequence[_Segment], chunk_id: str, chunk_text: str, offset: int,
) -> Optional[int]:
    """The record line of character `offset` of a chunk's own text, or None.

    `chunk_text` is the chunk's text as the caller read it. None when the
    chunk is not one of these segments or its text here is another text:
    the caller then has no record line and must say so. A trimmed overlap
    and a restored separator shift the offset as in `_locate`."""

    index = next((i for i, s in enumerate(segments) if s.chunk_id == chunk_id), None)
    if index is None or not 0 <= offset < len(chunk_text):
        return None
    segment = segments[index]
    own_start = segment.separator_before_chars
    own_end = len(segment.text) - segment.separator_after_chars
    if segment.text[own_start:own_end] != chunk_text[segment.trimmed_chars:]:
        return None
    if offset >= segment.trimmed_chars:
        return line_at(segment, offset - segment.trimmed_chars + own_start)
    if index == 0:
        return None
    # The offset is in a window chunker's trimmed overlap: that text is the
    # previous segment's tail.
    previous = segments[index - 1]
    tail = segment.trimmed_chars - offset
    at = len(previous.text) - previous.separator_after_chars - tail
    if at < 0 or previous.text[at:at + tail] != chunk_text[offset:segment.trimmed_chars]:
        return None
    return line_at(previous, at)


def render_piece(
    segment: _Segment,
    *,
    start: int,
    end: int,
    trust: str,
    lines: Optional[tuple[int, int]] = None,
) -> dict[str, Any]:
    """One agent-facing segment piece, fenced when its producer is untrusted.

    `lines` is the first and last line the piece holds, on a `lines` read."""

    attrs: dict[str, Any] = {"text": segment.text[start:end]}
    if segment.heading_path is not None:
        # Headings come from the record's own text, so they are fenced with it.
        # Copied: segments are shared across pages (issue #3002), so a
        # served piece never aliases a cached segment's value.
        attrs["heading_path"] = copy.deepcopy(segment.heading_path)
    if segment.untrusted_origin is not None:
        attrs[UNTRUSTED_CONTENT_ORIGIN_ATTR] = copy.deepcopy(segment.untrusted_origin)
    served = serialize_node_read_row(
        {"node_id": segment.chunk_id, "source": segment.source, "attrs": attrs},
        trust=trust,
    )
    out: dict[str, Any] = {"chunk_id": segment.chunk_id}
    if segment.chunk_ordinal is not None:
        out["chunk_ordinal"] = segment.chunk_ordinal
    if segment.char_offset is not None:
        out["char_offset"] = segment.char_offset
    if segment.char_end is not None:
        out["char_end"] = segment.char_end
    if segment.heading_path is not None:
        out["heading_path"] = served["attrs"]["heading_path"]
    if segment.trimmed_chars and start == 0:
        out["trimmed_chars"] = segment.trimmed_chars
    if segment.separator_before_chars and start == 0:
        out["separator_before_chars"] = segment.separator_before_chars
    if segment.separator_after_chars and end == len(segment.text):
        out["separator_after_chars"] = segment.separator_after_chars
    if start or end < len(segment.text):
        out["text_range"] = [start, end]
    if lines is not None:
        out["lines"] = [lines[0], lines[1]]
    if segment.redacted:
        out["redacted"] = True
    out["text"] = served["attrs"]["text"]
    out["provenance"] = served["provenance"]
    return out


def _json_bytes(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())


@dataclass(frozen=True)
class PreparedRecordText:
    """One reader-visible source preparation; paging copies mutable state.

    Text strings are immutable. Row ceilings do not bound their byte size.
    Batch allocation reuses this preparation without further database reads.
    """

    node_id: str
    payload: dict[str, Any]
    segments: tuple[_Segment, ...]
    trust_by_source: dict[Optional[str], str]
    binding: dict[str, Any]
    cursor: tuple[int, int]
    #: Where a located read's requested extent began (PACT
    #: read-model-s5-location review fix 2); every cursor a page of this
    #: read issues carries it. None for a read from the record's start.
    extent_from: Optional[tuple[int, int]] = None
    #: The validated `lines` ranges of a line read; None for any other read.
    lines: Optional[tuple[tuple[int, int], ...]] = None

    @property
    def prepared_text_bytes(self) -> int:
        return sum(len(segment.text.encode("utf-8")) for segment in self.segments)


def read_record_text(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    continuation_token: Optional[str] = None,
    at: Any = None,
    lines: Any = None,
    matching: Any = None,
    context: Any = None,
    max_bytes: int = 64 * 1024,
    reserve_bytes: int = _DEFAULT_RESERVE_BYTES,
) -> dict[str, Any]:
    """Return one source-attributed page using the existing single-text contract."""
    prepared = prepare_record_text(
        conn, session, node_id, continuation_token=continuation_token,
        at=at, lines=lines, matching=matching, context=context, max_bytes=max_bytes,
    )
    return page_record_text(prepared, max_bytes=max_bytes, reserve_bytes=reserve_bytes)


#: Where a text-less node with no readable document points: its attrs.
NO_TEXT_NODE_MOVE = (
    'inspect(target={"kind":"node","node_id":<this node_id>}) shows its attrs'
)


def _text_twin_move(conn: psycopg.Connection, session: Session, node_id: str) -> str:
    """Where a text-less node's readable text lives (issue #3467).

    A `file:` node's text is only its path and a `symbol:` node has none:
    the readable chunks belong to the `doc:` node derived from the file. Point
    there when a readable one exists, through the file that contains a
    symbol; otherwise to the node's attrs, as before."""
    from okg.substrate.mcp.tools import text_twin_document

    doc_id = text_twin_document(conn, session, node_id)
    if doc_id is None:
        return NO_TEXT_NODE_MOVE
    return (
        "its text is in the document derived from its file; read it with "
        f'inspect(target={{"kind":"text","node_id":{json.dumps(doc_id)}}})'
    )


def _attach_citation(
    conn: psycopg.Connection, session: Session, payload: dict[str, Any],
    record: Mapping[str, Any],
) -> None:
    """Put the record's citation id on a text read (PACT read-names-list-3666).

    Read from the record row the reader may see (redacted by the read
    filter) and fenced by its producer's trust, classified on every call
    as segment trust is. Nothing when the deployment names no attribute or
    the record has no value."""
    from okg.substrate.mcp.citation_id import (
        CITATION_KEY,
        citation_field,
        fenced_citation,
    )

    field = citation_field(record.get("attrs"))
    if not field:
        return
    source = record.get("source")
    trust = classify_sources(
        conn, deployment_name=session.deployment_name, sources=[source],
    ).get(source, "untrusted")
    payload[CITATION_KEY] = fenced_citation(field, {"trust": trust, "source": source})


def prepare_record_text(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    continuation_token: Optional[str] = None,
    at: Any = None,
    lines: Any = None,
    matching: Any = None,
    context: Any = None,
    max_bytes: int = 64 * 1024,
    bind_context: bool = True,
    classify_trust: bool = True,
) -> PreparedRecordText:
    """Prepare normalized reader-visible text and validate its starting cursor.

    The copied at location retains the existing leading-context, optional
    hash and chunk-only semantics. Source rows and trust are read once;
    callers can page this preparation without repeating those reads.
    `lines` makes it a line read: the pages hold those lines only.
    `matching` makes it a line read of the lines that match (and `context`
    lines around each), found in the text this reader is served.
    """

    line_matching = parse_matching(matching, context, lines=lines, at=at)
    ranges = parse_line_target(lines, at)
    location = _parse_at(at) if at is not None else None
    given_node_id = node_id or None
    if location is not None and not node_id:
        node_id = location.chunk
    if not node_id:
        raise InvalidOperatorArguments(
            "inspect text: node_id is required",
            data={"tool": "inspect", "invalid_fields": ["target.node_id"],
                  "node_id": node_id},
            fix_hint=(
                'pass target={"kind":"text","node_id":"<record node id>"}, or '
                "at=<a search hit's location>"
            ),
        )
    assert_session_matches(conn, session)
    # Issue #3002: a later page of the same record reuses the reader-visible
    # text the first page built (`record_text_cache` says when it may).
    # Under Nomos audit and enforce only a continuation page reuses it, so
    # every read's first page decides its classes and records them once.
    # A batch read (no cursor binding) continues through its own batch
    # token and re-prepares each source on every batch page, so it never
    # presents a text cursor: under Nomos it neither reads nor stores an
    # entry.
    key = (
        _text_cache_key(
            conn, session, node_id, classify_trust=classify_trust,
            can_continue=bind_context,
        )
        if location is None else None
    )
    document = (
        _text_cache_get(conn, key, generation_id=session.generation_id)
        if key is not None and (
            continuation_token is not None or not key.reuse_needs_continuation
        )
        else None
    )
    located_text: Optional[str] = None
    if document is None:
        capture = (
            OverlayCapture.start(conn, generation_id=session.generation_id)
            if key is not None else None
        )
        reset = _overlay_capture.set(capture)
        try:
            document, located_text = _read_document(
                conn, session, node_id, location=location,
                given_node_id=given_node_id, classify_trust=classify_trust,
            )
        finally:
            _overlay_capture.reset(reset)
        # A chunk target is one segment whose parents come from rows the
        # overlay check does not cover; it is cheap, so it is never cached.
        if key is not None and capture is not None and not document.is_chunk:
            _text_cache_put(conn, key, document, overlay=capture)
    record = document.record
    is_chunk = document.is_chunk
    parents = [dict(parent) for parent in document.parents]
    parents_complete = document.parents_complete
    segments = list(document.segments)
    order_basis = document.order_basis
    coverage = document.coverage
    at_cursor: Optional[tuple[int, int]] = None
    if location is not None:
        at_cursor = _locate(location, located_text, segments, given_node_id, session)
    matching_block: Optional[dict[str, Any]] = None
    if line_matching is not None and segments:
        ranges, matching_block = matching_ranges(
            segments, line_matching, line_count(segments),
        )

    # New text cursors carry the original response budget, so next alone
    # cannot silently expand a small page to the deployment default. Older
    # signed cursors did not carry it: preserve their legacy binding and
    # caller-supplied continuation_token budget, rather than inventing one.
    projection: dict[str, Any] = {"node_id": node_id, "max_bytes": max_bytes}
    # A line read signs its ranges, so its token continues those lines and
    # no other read; a read with no `lines` binds exactly what it did before.
    signed_lines = (
        line_matching.signed() if line_matching is not None
        else {"lines": asked_lines(ranges)} if ranges is not None else {}
    )
    projection.update(signed_lines)
    if continuation_token is not None:
        previous = open_continuation(continuation_token)["binding"].get("projection")
        if isinstance(previous, dict):
            if "max_bytes" not in previous:
                projection.pop("max_bytes")
            else:
                original_budget = previous["max_bytes"]
                if not isinstance(original_budget, int) or isinstance(original_budget, bool) or original_budget <= 0:
                    raise ContinuationTokenInvalid(
                        "the text continuation has no valid budget",
                        data={"reason": "binding_mismatch", "mismatched": ["target"]},
                    )
                # The legacy continuation_token form may explicitly choose a
                # new page budget. Verify all ORIGINAL signed read bindings
                # before that choice becomes the next page's signed budget.
                projection["max_bytes"] = original_budget
    binding = (continuation_binding(
        conn, session, kind="inspect_text", sort=order_basis, projection=projection,
    ) if bind_context else {})
    seg_index, char_index = at_cursor if at_cursor is not None else (0, 0)
    #: Where a located read's requested extent began. Every cursor of that
    #: read carries it, so each of its pages can say so (review fix 2).
    extent_from: Optional[tuple[int, int]] = at_cursor
    extent_ok = True
    if continuation_token is not None:
        cursor = consume_continuation(continuation_token, expected_binding=binding)
        seg_index = cursor.get("segment")
        char_index = cursor.get("char")
        extent_from, extent_ok = _cursor_from(cursor.get("from"), segments)
        if (
            not isinstance(seg_index, int)
            or not isinstance(char_index, int)
            or isinstance(seg_index, bool)
            or isinstance(char_index, bool)
            or not 0 <= seg_index < len(segments)
            or not 0 <= char_index < max(1, len(segments[seg_index].text))
            or not extent_ok
        ):
            raise ContinuationTokenInvalid(
                "the text continuation has no valid position",
                data={"reason": "cursor_invalid", "pinned_generation_id": session.generation_id},
                fix_hint="restart the text read without a continuation token",
            )

    # The verified read keeps its cursor, node, generation and authority;
    # only the legacy spelling may choose a newly ceiling-checked budget.
    binding = {
        **binding,
        "projection": {"node_id": node_id, "max_bytes": max_bytes, **signed_lines},
    }
    payload: dict[str, Any] = {
        "status": "ok",
        "node_id": node_id,
        "subtype": record.get("subtype"),
        "order_basis": order_basis,
        "coverage": copy.deepcopy(coverage),
        "items": [],
        "complete": True,
        "truncated": False,
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
    }
    _attach_citation(conn, session, payload, record)
    if segments:
        # What a line number of this read means and the last one; the page
        # builder adds the lines the page holds.
        payload["lines"] = {
            **line_basis(segments, coverage, order_basis),
            "total": line_count(segments),
        }
        if ranges:
            payload["lines"]["requested"] = asked_lines(ranges)
        if matching_block is not None:
            payload["matching"] = matching_block
    elif ranges is not None or line_matching is not None:
        # A line read of a record with no readable text: no lines exist.
        payload["lines"] = {
            "basis": LINES_OF_SHOWN_TEXT, "total": 0,
            "requested": asked_lines(ranges or ()), "served": [],
            "past_end": True,
        }
    if location is not None and location.hash is None and continuation_token is None:
        # Review fix 1(a): with no hash the location was checked by its chunk
        # and offset alone, so a changed chunk text would not be detected.
        payload["location_check"] = "offset_only"
    if extent_from is not None and extent_from != (0, 0):
        # Review fix 2: `complete` means the requested extent, from the
        # location's lead on. Say where that extent began and that the text
        # before it was not sent.
        payload["read_from"] = {
            "chunk": segments[extent_from[0]].chunk_id,
            "offset": extent_from[1],
            "text_before_sent": False,
        }

    if is_chunk:
        payload["parents"] = parents
        payload["parents_complete"] = parents_complete
        if parents:
            first = json.dumps(parents[0]["node_id"])
            if segments:
                # Issue #3062: read on through the record from this chunk,
                # not from the record's top.
                here_at: dict[str, Any] = {
                    "chunk": node_id, "offset": location.offset if location else 0,
                }
                if location is not None and location.hash is not None:
                    # Review fix 1(b): the follow-on read keeps the caller's
                    # stale-text check.
                    here_at["hash"] = location.hash
                here = json.dumps(here_at, separators=(",", ":"))
                next_move = (
                    "this is one chunk of a record; read the record on from here with "
                    f'inspect(target={{"kind":"text","node_id":{first},"at":{here}}})'
                )
            else:
                next_move = (
                    "this is one chunk of a record; read the whole record with "
                    f'inspect(target={{"kind":"text","node_id":{first}}})'
                )
        else:
            next_move = (
                "this chunk belongs to no record you can see; "
                'expand(input={"node_id":<this node_id>}) lists its neighbors'
            )
        payload["outcome"] = {"code": "single_chunk", "next_move": next_move}
    if not segments:
        # Never a bare empty list: say why and where to go next. The text
        # names no id or count, so "every chunk hidden" and "no chunks" stay
        # one answer; the node's id and subtype are on the payload itself.
        payload["outcome"] = {
            "code": "no_text_chunks",
            "reason": (
                "this chunk has no text you can read"
                if is_chunk
                else (
                    "this node has no document_chunk text you can read "
                    "(none reaches it through member_of or contains edges)"
                )
            ),
            "next_move": (
                # A text-less chunk that belongs to a visible record still
                # points at that record's text, as a chunk with text does.
                payload["outcome"]["next_move"]
                if is_chunk and parents
                else _text_twin_move(conn, session, node_id)
            ),
        }
        return PreparedRecordText(
            node_id, payload, (), {}, binding, (seg_index, char_index),
            extent_from, ranges,
        )

    if matching_block is not None and not ranges:
        # No line matches: nothing to page. Say so, and what the text holds.
        payload["lines"]["served"] = []
        payload["outcome"] = {
            "code": "no_line_matches",
            "reason": f"no line of this text matches {line_matching.pattern!r}",
            "next_move": (
                "try other words or re:/a|b/; search(method=\"grep\") finds "
                "the records that hold them"
            ),
        }
        return PreparedRecordText(
            node_id, payload, (), {}, binding, (seg_index, char_index),
            extent_from, None,
        )

    # Trust is producer policy, not generation state: an operator can change
    # it at any time, so it is classified on every call, never cached.
    trust_by_source = (classify_sources(
        conn, deployment_name=session.deployment_name,
        sources=(s.source for s in segments),
    ) if classify_trust else {})
    # Segments are read-only from here on (the page builder and batch
    # allocation only read them), so the preparation shares them rather than
    # copying every segment of the record on every page (issue #3002).
    return PreparedRecordText(
        node_id, copy.deepcopy(payload), tuple(segments),
        dict(trust_by_source), copy.deepcopy(binding), (seg_index, char_index),
        extent_from, ranges,
    )


def _read_document(
    conn: psycopg.Connection,
    session: Session,
    node_id: str,
    *,
    location: Optional[_At],
    given_node_id: Optional[str],
    classify_trust: bool,
    graph: Any = None,
) -> tuple[RecordDocument, Optional[str]]:
    """Read the record's reader-visible text: the cursor-independent work.

    Returns the document and, for a located read, the located chunk's text
    as the reader sees it (None when the chunk is not one it can see).
    `graph` is the caller's checkout of the session's generation, when it
    already holds one.
    """
    if graph is None:
        graph = _checkout(conn, session)
    try:
        # A located read swallows the refusal below: no close-id lookup.
        record = (
            _visible_start(conn, session, graph, node_id)
            if location is None
            else _visible_start(conn, session, graph, node_id, suggest=False)
        )
    except NodeNotFound:
        if location is None:
            raise
        # A hidden record, a hidden chunk and a missing one: one answer.
        raise _stale(location, given_node_id, session, "chunk_not_found") from None
    is_chunk = record.get("subtype") == _CHUNK_SUBTYPE
    parents: list[dict[str, Any]] = []
    parents_complete = True
    located_text: Optional[str] = None
    if is_chunk:
        # The target IS a chunk: its text is the answer, not its (absent)
        # chunks. Its parents tell the agent which record to read whole.
        if location is not None and location.chunk != node_id:
            raise _stale(location, given_node_id, session, "chunk_not_found")
        segments = _chunk_self_segment(record)
        order_basis = "single_chunk"
        parents, parents_complete = _chunk_parents(conn, session, graph, node_id)
        text = _attrs(record).get("text")
        located_text = text if isinstance(text, str) else None
    else:
        if location is not None and location.chunk == node_id:
            raise _stale(location, given_node_id, session, "chunk_not_found")
        edges = _occurrence_edges(conn, session, graph, node_id)
        chunk_ids = sorted(
            {
                str(e["src"] if e.get("edge_type") == "member_of" else e["dst"])
                for e in edges
            }
        )
        chunks = _visible_chunks(conn, session, graph, chunk_ids)
        segments, order_basis = order_segments(_segments(edges, chunks))
        if location is not None and location.chunk in chunks:
            text = _attrs(chunks[location.chunk]).get("text")
            located_text = text if isinstance(text, str) else None
        trim_overlap(segments)
        fill_separators(segments)
    # Numbered once, on the text this reader is served, and kept with the
    # document: every page, range read and match line reads these numbers.
    number_lines(segments)
    coverage = coverage_of(segments, order_basis)
    return RecordDocument(
        record=record,
        is_chunk=is_chunk,
        parents=tuple(parents),
        parents_complete=parents_complete,
        segments=tuple(segments),
        order_basis=order_basis,
        coverage=coverage,
    ), located_text

#: A cut page's `text_left` while the page is measured: no real value is
#: longer, so the real block never pushes a measured page over its budget.
_TEXT_LEFT_SAMPLE = {
    "chars": 10**12, "segments": 10**9, "max_bytes_for_rest": 10**9,
    "more_than_one_page": True,
}
#: Pages below the public minimum budget (only in-process callers) carry no
#: `text_left`: its reserve would leave them no room for text.
_TEXT_LEFT_MIN_BUDGET = 4096


def text_left(
    segments: Sequence[Any],
    cursor: Mapping[str, Any],
    *,
    page_bytes: int,
    items: Sequence[Mapping[str, Any]],
    reserve_bytes: int = 0,
    lines_left: Optional[tuple[int, int]] = None,
) -> dict[str, Any]:
    """What a cut text page leaves for the next calls (#3634): the
    characters and segments not yet served, and a `max_bytes` that should
    read all of them in one page. The estimate is measured on this page:
    its envelope, its text's bytes per character and its per-piece
    overhead (fences, provenance), applied to the rest with a 15% margin,
    plus the page's reserve, rounded up to 1 KB and kept between the
    smallest and largest page `next` accepts. When even the largest page
    cannot hold the rest, `more_than_one_page` is true and the value is the
    largest page. Lane X agents stopped at 74% of cut pages, with nothing
    saying how much was left."""
    from okg.substrate.mcp.response_bounds import MAX_RESPONSE_BYTES, MIN_RESPONSE_BYTES

    seg, char = int(cursor["segment"]), int(cursor["char"])
    if lines_left is not None:
        # A line read: the characters and pieces its ranges still hold.
        rest, rest_segments = lines_left
    else:
        rest = max(len(segments[seg].text) - char, 0) + sum(
            len(segments[k].text) for k in range(seg + 1, len(segments))
        )
        rest_segments = len(segments) - seg
    piece_bytes = [_json_bytes(item) + 1 for item in items]
    text_chars = sum(len(str(item.get("text") or "")) for item in items)
    text_bytes = sum(_json_bytes(str(item.get("text") or "")) for item in items)
    envelope = max(page_bytes - sum(piece_bytes), 0)
    per_char = text_bytes / text_chars if text_chars else 2.0
    per_piece = (sum(piece_bytes) - text_bytes) / len(items) if items else 1024
    estimate = (envelope + rest * per_char + rest_segments * per_piece) * 1.15 + reserve_bytes
    size = max(-(-int(estimate) // 1024) * 1024, MIN_RESPONSE_BYTES)
    left: dict[str, Any] = {"chars": rest, "segments": rest_segments}
    if size > MAX_RESPONSE_BYTES:
        left["max_bytes_for_rest"] = MAX_RESPONSE_BYTES
        left["more_than_one_page"] = True
    else:
        left["max_bytes_for_rest"] = size
    return left


def page_record_text(
    prepared: PreparedRecordText,
    *,
    max_bytes: int,
    reserve_bytes: int = _DEFAULT_RESERVE_BYTES,
) -> dict[str, Any]:
    """Page a preparation without changing it or reading the database."""
    payload = copy.deepcopy(prepared.payload)
    if not prepared.segments:
        return payload
    # The source's overlap/separator normalization ran once, and paging only
    # reads segments, so they are shared, not copied per page (issue #3002).
    segments = prepared.segments
    trust_by_source = prepared.trust_by_source
    binding = prepared.binding
    node_id = prepared.node_id
    seg_index, char_index = prepared.cursor
    extent_from = prepared.extent_from
    ranges = prepared.lines
    # What this read still has to serve, in order: on a line read the parts
    # that hold the asked lines from the cursor on, otherwise every segment
    # from the cursor on. A page is a run of these from the first.
    if ranges is not None:
        spans = spans_from(line_spans(segments, ranges), prepared.cursor)
    else:
        spans = [
            LineSpan(i, char_index if i == seg_index else 0, len(segments[i].text))
            for i in range(seg_index, len(segments))
        ]
    line_block = dict(payload["lines"])
    if not spans:
        # A line read whose every range lies past the last line.
        payload["lines"] = _page_lines(line_block, segments, ranges, [], None)
        return payload
    # Measured with a `lines` block no real one is longer than, as
    # `text_left` is: the page's own lines are known only once it is cut.
    if ranges is not None:
        widest = max(line_block["total"], ranges[-1][1])
        payload["lines"] = {
            **line_block,
            "served": [[last, last] for _first, last in ranges],
            "past_end": True,
            "cut": {"last_line": widest, "next_line": widest},
        }
    else:
        payload["lines"] = {
            **line_block, "first": line_block["total"], "last": line_block["total"],
        }

    def cursor_at(segment: int, char: int) -> dict[str, Any]:
        cursor: dict[str, Any] = {"segment": segment, "char": char}
        if extent_from is not None:
            cursor["from"] = list(extent_from)
        return cursor

    def piece(span: LineSpan, end: Optional[int] = None) -> dict[str, Any]:
        segment = segments[span.segment]
        stop = span.end if end is None else end
        return render_piece(
            segment, start=span.start, end=stop,
            trust=trust_by_source.get(segment.source, "untrusted"),
            lines=span_lines(segments, span, stop) if ranges is not None else None,
        )

    budget = max_bytes - reserve_bytes
    # Measure the envelope WITH a cursor, so a page that needs one still fits.
    sample_token = issue_continuation(
        binding,
        cursor=cursor_at(
            max(len(segments) - 1, 0),
            max((len(seg.text) for seg in segments), default=0),
        ),
    )
    left_sample = _TEXT_LEFT_SAMPLE if max_bytes >= _TEXT_LEFT_MIN_BUDGET else None
    envelope = dict(
        payload, complete=False, truncated=True, continuation_token=sample_token,
        **({"text_left": left_sample} if left_sample else {}),
    )
    used = public_call_tool_result_bytes(envelope, tool="inspect", kind="text")
    items: list[dict[str, Any]] = []
    #: Where each item's text ends in its span's segment.
    ends: list[int] = []
    next_cursor: Optional[dict[str, Any]] = None
    for span in spans:
        whole = piece(span)
        cost = _json_bytes(whole) + 1
        if used + cost <= budget:
            items.append(whole)
            ends.append(span.end)
            used += cost
            continue
        if items:
            next_cursor = cursor_at(span.segment, span.start)
            break
        # The first piece alone is over the budget: cut it at a character,
        # measuring the SERVED page with the trial piece in it (issue #3059;
        # #3076 DESIGN 4.1 N-a). A per-piece JSON estimate could size a cut
        # the text form then serves over budget, and `inspect` would refuse
        # the page and lose the cursor.
        if left_sample:
            # Measured with the placeholder, so the cut fits once.
            payload["text_left"] = left_sample
        fit = _cut_to_fit(
            payload, lambda stop, span=span: piece(span, stop),
            start=span.start, end=span.end,
            sample_token=sample_token, budget=budget,
        )
        if fit == 0:
            raise RecordTextBudgetTooSmall(
                "not one character of this record's text fits the byte budget",
                data={"max_bytes": max_bytes, "node_id": node_id},
                fix_hint="call again with a larger max_bytes",
            )
        if ranges is not None:
            # A line read stops after a whole line when one fits.
            fit = whole_lines(segments[span.segment].text, span.start, fit)
        items.append(piece(span, span.start + fit))
        ends.append(span.start + fit)
        next_cursor = cursor_at(span.segment, span.start + fit)
        break

    payload["items"] = items
    if next_cursor is not None:
        payload["complete"] = False
        payload["truncated"] = True
        payload["continuation_token"] = issue_continuation(binding, cursor=next_cursor)
        if left_sample:
            payload["text_left"] = left_sample
    # The estimate above sums per-piece JSON; the served form is the judge.
    # Item k on this page is `spans[k]`, and every item but a lone cut one is
    # its whole span, so dropping items re-points the cursor exactly.
    while public_call_tool_result_bytes(payload, tool="inspect", kind="text") > budget and len(items) > 1:
        items.pop()
        ends.pop()
        payload["items"] = items
        payload["complete"] = False
        payload["truncated"] = True
        payload["continuation_token"] = issue_continuation(
            binding,
            cursor=cursor_at(spans[len(items)].segment, spans[len(items)].start),
        )
        if left_sample:
            payload["text_left"] = left_sample
    if public_call_tool_result_bytes(payload, tool="inspect", kind="text") > budget and len(items) == 1:
        # One piece left and the served page is still over: cut that piece
        # in the served form too, so the page is never refused as over. The
        # cut is checked again with the page's real cursor, which can differ
        # from the sample cursor's length by a byte or two.
        first = spans[0]
        fit = _cut_to_fit(
            payload, lambda stop: piece(first, stop),
            start=first.start, end=first.end,
            sample_token=sample_token, budget=budget,
        )
        if ranges is not None and fit > 0:
            fit = whole_lines(segments[first.segment].text, first.start, fit)
        while True:
            if fit <= 0:
                raise RecordTextBudgetTooSmall(
                    "not one character of this record's text fits the byte budget",
                    data={"max_bytes": max_bytes, "node_id": node_id},
                    fix_hint="call again with a larger max_bytes",
                )
            payload["items"] = [piece(first, first.start + fit)]
            ends = [first.start + fit]
            payload["complete"] = False
            payload["truncated"] = True
            payload["continuation_token"] = issue_continuation(
                binding, cursor=cursor_at(first.segment, first.start + fit)
            )
            if left_sample:
                payload["text_left"] = left_sample
            if public_call_tool_result_bytes(payload, tool="inspect", kind="text") <= budget:
                break
            fit -= max(1, fit // 64)
    token = payload.get("continuation_token")
    cursor = open_continuation(token)["cursor"] if token else None
    # The real blocks, no longer than the measured samples.
    payload["lines"] = _page_lines(
        line_block, segments, ranges, list(zip(spans, ends)), cursor,
    )
    if left_sample and cursor is not None:
        lines_left: Optional[tuple[int, int]] = None
        if ranges is not None:
            # What a line read leaves is the rest of its lines, not of the record.
            remaining = spans_from(
                line_spans(segments, ranges), (cursor["segment"], cursor["char"]),
            )
            lines_left = (
                sum(span.end - span.start for span in remaining), len(remaining),
            )
        payload["text_left"] = text_left(
            segments, cursor,
            page_bytes=public_call_tool_result_bytes(payload, tool="inspect", kind="text"),
            items=payload["items"],
            reserve_bytes=reserve_bytes,
            lines_left=lines_left,
        )
    if (
        prepared.cursor == (0, 0) and ranges is None and extent_from is None
        and payload.get("order_basis") != "single_chunk"
    ):
        # PACT read-passage-rows-3666 (R6): a ticket-shaped record's first
        # page also carries its thread, fitted to what the page leaves.
        block = segments_thread(segments, trust_by_source)
        if block is not None:
            fitted = fit_record_text(
                block,
                lambda candidate: public_call_tool_result_bytes(
                    {**payload, RECORD_TEXT_KEY: candidate}, tool="inspect", kind="text",
                ) <= budget,
            )
            if fitted is not None:
                payload[RECORD_TEXT_KEY] = fitted
    return payload


def whole_lines(text: str, start: int, fit: int) -> int:
    """`fit` characters from `start`, shortened to end just past the last
    line feed among them when there is one."""

    at = text.rfind("\n", start, start + fit)
    return at + 1 - start if at >= 0 else fit


def _page_lines(
    block: Mapping[str, Any],
    segments: Sequence[_Segment],
    ranges: Optional[Sequence[tuple[int, int]]],
    served: Sequence[tuple[LineSpan, int]],
    cursor: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    """A page's `lines` block: `block` (basis, total, the asked ranges) plus
    the lines the page holds.

    `served` is each piece on the page as its span and where its text ends.
    A page read gets `first` and `last`. A line read gets `served`, one
    `[first, last]` per asked range with text on the page; `past_end` when a
    range reaches past the last line; and, when the size limit cut it,
    `cut`: the last line served and the line `next` continues on (the same
    line when the cut fell inside it)."""

    out = dict(block)
    held = [(span, end) for span, end in served if end > span.start]
    if ranges is None:
        if held:
            out["first"] = span_lines(segments, *held[0])[0]
            out["last"] = span_lines(segments, *held[-1])[1]
        elif served:
            out["first"] = out["last"] = segments[served[0][0].segment].line_start
        return out
    by_range: dict[int, list[int]] = {}
    for span, end in held:
        first, last = span_lines(segments, span, end)
        by_range.setdefault(span.range, [first, last])[1] = last
    out["served"] = [by_range[number] for number in sorted(by_range)]
    if lines_past_end(ranges, out["total"]):
        out["past_end"] = True
    if cursor is not None and held:
        out["cut"] = {
            "last_line": span_lines(segments, *held[-1])[1],
            "next_line": line_at(segments[cursor["segment"]], cursor["char"]),
        }
    return out


def _locate(
    location: _At,
    text: Optional[str],
    segments: list[_Segment],
    node_id: Optional[str],
    session: Session,
) -> tuple[int, int]:
    """The `(segment, char)` a read at `location` starts from.

    `text` is the located chunk's text as the reader sees it (None when the
    chunk is not one this read can see); the hash and the offset are checked
    against it. The offset is a position in that text; a window chunker's
    trimmed overlap and a restored separator shift it into the segment."""

    index = next(
        (i for i, s in enumerate(segments) if s.chunk_id == location.chunk), None
    )
    if text is None or index is None:
        raise _stale(location, node_id, session, "chunk_not_found")
    if location.hash is not None and location.hash != chunk_text_hash(text):
        raise _stale(location, node_id, session, "hash_mismatch")
    if location.offset >= max(1, len(text)):
        raise _stale(location, node_id, session, "offset_out_of_range")
    segment = segments[index]
    own_start = segment.separator_before_chars
    hit = location.offset - segment.trimmed_chars + own_start
    if hit < own_start and index > 0:
        # The hit sits in a window chunker's trimmed overlap: that text is the
        # previous segment's tail, so start there, AT_LEAD_CHARS before it,
        # rather than at this chunk's own text where the hit is behind us.
        previous = segments[index - 1]
        back = len(previous.text) - (own_start - hit) - AT_LEAD_CHARS
        return index - 1, min(max(0, back), max(0, len(previous.text) - 1))
    start = max(own_start, hit - AT_LEAD_CHARS)
    return index, min(start, max(0, len(segment.text) - 1))


def _cursor_from(
    value: Any, segments: list[_Segment],
) -> tuple[Optional[tuple[int, int]], bool]:
    """A cursor's `from` (where a located read's extent began) and whether
    it is valid. A cursor with none, from a read that started at the top or
    from an older token, gives `(None, True)`."""

    if value is None:
        return None, True
    if (
        isinstance(value, list) and len(value) == 2
        and all(isinstance(v, int) and not isinstance(v, bool) for v in value)
        and 0 <= value[0] < len(segments)
        and 0 <= value[1] < max(1, len(segments[value[0]].text))
    ):
        return (value[0], value[1]), True
    return None, False


def _cut_to_fit(
    payload: dict[str, Any],
    render: Any,
    *,
    start: int,
    end: int,
    sample_token: str,
    budget: int,
) -> int:
    """The most characters from ``start``, short of ``end``, whose page fits.

    ``render(stop)`` is the piece from ``start`` up to ``stop``. Measured on
    the served page (the call's format, the envelope with a cursor), not on
    a per-piece estimate. Returns 0 when not one character fits.
    """

    low, high, fit = 1, end - start - 1, 0
    while low <= high:
        mid = (low + high) // 2
        trial = render(start + mid)
        page = dict(
            payload,
            items=[trial],
            complete=False,
            truncated=True,
            continuation_token=sample_token,
        )
        if public_call_tool_result_bytes(page, tool="inspect", kind="text") <= budget:
            fit, low = mid, mid + 1
        else:
            high = mid - 1
    return fit


@dataclass(frozen=True)
class MatchPlace:
    """A match in a record: the record, and the text the match was found in
    as the reader was served it, with the match's character offset in it.

    For a match in one of the record's passages, `chunk_id` and
    `chunk_subtype` name the passage and `text` is the passage's text. For a
    match in the record's own `text` attribute, `chunk_id` is None."""

    record_id: str
    chunk_id: Optional[str]
    chunk_subtype: Any
    text: str
    offset: int


def _read_filter_active(session: Session) -> bool:
    """Access rules decide rows on this deployment (audit or enforce)."""

    from okg.substrate.nomos.runtime_enforcement import (
        deployment_nomos_enforcement_mode,
    )

    return deployment_nomos_enforcement_mode(session.deployment_name) != "off"


def own_text_line(segments: Sequence[_Segment], text: str, offset: int) -> Optional[int]:
    """The record line that IS the line of `text` holding `offset`, or None.

    A record's own `text` attribute is not the text `inspect` text reads
    (its chunks). The own line has a record line only when the own text
    holds that whole line exactly once and the chunks hold it exactly once
    too: then the two are the same place, and reading that record line
    returns it. A line the own text repeats could be either place."""

    start = text.rfind("\n", 0, offset) + 1
    end = text.find("\n", offset)
    wanted = text[start:len(text) if end < 0 else end]
    if not wanted.strip() or text.split("\n").count(wanted) != 1:
        return None
    found: Optional[int] = None
    number = 0
    for line in "".join(
        ("\n" if segment.line_break_before else "") + segment.text for segment in segments
    ).split("\n"):
        number += 1
        if line == wanted:
            if found is not None:
                return None
            found = number
    return found


def is_statement_timeout(exc: BaseException) -> bool:
    """`exc` is PostgreSQL cancelling a statement, itself or wrapped: a
    read layer may raise its own error from the driver's."""

    seen: Optional[BaseException] = exc
    for _depth in range(16):
        if seen is None:
            return False
        if isinstance(seen, psycopg.errors.QueryCanceled):
            return True
        seen = seen.__cause__ or seen.__context__
    return False


def _match_record_segments(
    conn: psycopg.Connection, session: Session, record_id: str,
) -> Optional[tuple[_Segment, ...]]:
    """One record's numbered segments for match lines, or None.

    Never inside a transaction block: checking the generation out takes its
    read lease, which commits. The caller bounds the time each statement
    may take on the session (`grep_search._record_lines_in_time`).

    A size read comes first, in the two shapes `_occurrence_edges` reads:
    the record's `member_of` chunks, then its `contains` chunks, each as one
    count and one sum of the chunks' text bytes (computed in the database;
    no text is sent). A record with more than `MATCH_LINE_RECORD_CHUNKS`
    chunks or more than `MATCH_LINE_RECORD_BYTES` bytes of chunk text in
    either direction is not loaded. The read then builds the text as
    `inspect` text does, on the same checkout, without the prepared-text
    cache in either direction: a search never evicts a paged read's entry,
    and its answer does not depend on what the cache holds."""

    graph = _checkout(conn, session)
    return _sized_record_segments(conn, session, graph, record_id)


def _sized_record_segments(
    conn: psycopg.Connection, session: Session, graph: Any, record_id: str,
) -> Optional[tuple[_Segment, ...]]:
    """`_match_record_segments`' reads: the size read, then the text."""

    params = {"node_id": record_id, "chunk_subtype": _CHUNK_SUBTYPE}
    measure = (
        "SELECT count(*) AS chunks, "
        "coalesce(sum(octet_length(c.attrs->>'text')), 0) AS bytes "
        "FROM okg.graph_edges e JOIN okg.graph_nodes c ON c.node_id = "
    )
    for join in (
        "e.src WHERE e.dst = :node_id AND e.edge_type = 'member_of' ",
        "e.dst WHERE e.src = :node_id AND e.edge_type = 'contains' ",
    ):
        rows = [
            dict(row)
            for row in graph.query(measure + join + "AND c.subtype = :chunk_subtype", params)
        ]
        if (
            not rows
            or int(rows[0]["chunks"]) > MATCH_LINE_RECORD_CHUNKS
            or int(rows[0]["bytes"]) > MATCH_LINE_RECORD_BYTES
        ):
            return None
    try:
        document, _located = _read_document(
            conn, session, record_id, location=None, given_node_id=record_id,
            classify_trust=False, graph=graph,
        )
    except (NodeNotFound, RecordTextTooLarge):
        return None
    return None if document.is_chunk else tuple(document.segments)


def record_match_lines(
    conn: psycopg.Connection,
    session: Session,
    places: Sequence[MatchPlace],
    *,
    segment_budget: int = MATCH_LINE_SEGMENT_BUDGET,
    byte_budget: int = MATCH_LINE_BYTE_BUDGET,
    stop_at: Optional[float] = None,
) -> list[Optional[int]]:
    """For each place, the line of its record's text the match is on.

    The line is the one `inspect` text numbers for this reader, so it can be
    passed back as `lines`. None wherever that line cannot be had, and the
    caller must then say what its own line counts: access rules are on, the
    passage is not a `document_chunk`, the reader cannot read the record or
    does not get the passage as part of it, an own-text line is not one
    whole line of both texts, the record is over its size cap, or the
    page's work or time bound is spent.

    Under access-rule audit or enforce no record is read at all: a page of
    matches must not read whole records, or log access decisions, that its
    reader did not ask for.

    Otherwise each record is read at most once per call
    (`_match_record_segments`: a size cap known before loading, no cache),
    and no further record is read once the records read so far hold
    `segment_budget` segments or `byte_budget` bytes of text, once
    `stop_at` (`time.monotonic()` seconds) has passed, or once a statement
    was cancelled by the session's statement timeout, which the caller
    sets. The lines placed
    before that are kept, so the same matches can get a record line on one
    call and none on another; the caller says which each time."""

    if not places or _read_filter_active(session):
        return [None] * len(places)
    assert_session_matches(conn, session)
    documents: dict[str, Optional[tuple[_Segment, ...]]] = {}
    loaded_segments = loaded_bytes = 0
    #: A statement ran into the caller's statement timeout: no more reads.
    out_of_time = False
    lines: list[Optional[int]] = []
    for place in places:
        if place.chunk_id is not None and place.chunk_subtype != _CHUNK_SUBTYPE:
            lines.append(None)
            continue
        if place.record_id not in documents:
            segments: Optional[tuple[_Segment, ...]] = None
            if (
                not out_of_time
                and loaded_segments < segment_budget and loaded_bytes < byte_budget
                and (stop_at is None or time.monotonic() < stop_at)
            ):
                try:
                    segments = _match_record_segments(conn, session, place.record_id)
                except Exception as exc:
                    if not is_statement_timeout(exc):
                        raise
                    # The lines placed so far stand; this record and the
                    # ones after it get none.
                    out_of_time = True
                if segments is not None:
                    loaded_segments += len(segments)
                    loaded_bytes += sum(len(s.text.encode("utf-8")) for s in segments)
            documents[place.record_id] = segments
        segments = documents[place.record_id]
        if segments is None:
            lines.append(None)
        elif place.chunk_id is None:
            lines.append(own_text_line(segments, place.text, place.offset))
        else:
            lines.append(chunk_offset_line(segments, place.chunk_id, place.text, place.offset))
    return lines


# ---------- search-row passages and ticket threads (PACT read-passage-rows-3666) ----------
#
# A search row shows the text around its match, chosen over the record's
# WHOLE text: a ticket's comments are lines inside its later chunks, so a
# window cut from the winning chunk alone, or from the record's own text
# attribute, showed the record's start or a comment without its author.
#
# The text is the one `inspect` kind text serves this reader (`_read_document`
# through `_sized_record_segments`: the same size caps, overlap trim and
# Nomos filtering). A search page reads it only with access rules off, as
# `record_match_lines` does: a page of matches must not read whole records,
# or log access decisions, its reader did not ask for. Otherwise, and past
# the page's size or time bound, a row's passage is cut from its winning
# chunk and says so (`basis`).

#: Characters of a ticket, comment, description or commit passage.
PASSAGE_SHORT_CHARS = 500
#: The most characters any passage holds when the call gives no
#: `max_chars` (PACT read-reply-size-3666; was 1,000 for a document and up to
#: 2,000 for code). `max_chars` still sets any length up to 4,096.
PASSAGE_DEFAULT_MAX_CHARS = 800
#: Characters of a document or code passage.
PASSAGE_LONG_CHARS = PASSAGE_DEFAULT_MAX_CHARS
#: A code or script text up to this long is shown whole.
CODE_WHOLE_CHARS = PASSAGE_DEFAULT_MAX_CHARS
#: Lines of context either side of a longer code text's matched line.
CODE_CONTEXT_LINES = 15
#: Characters a code passage's lines may hold at most (long lines), its
#: signature and docstring lines included.
CODE_MAX_CHARS = PASSAGE_DEFAULT_MAX_CHARS
#: Record text one search page loads at most, and the time it may take.
PASSAGE_PAGE_BYTES = 4 * 1024 * 1024
PASSAGE_PAGE_SECONDS = 2.0

PASSAGE_BASIS_RECORD = "record_text"
PASSAGE_BASIS_PASSAGE = "passage"
PASSAGE_BASIS_OWN = "own_text"

#: `chunk_kind` values whose text is code (shown by lines, whole when short).
CODE_CHUNK_KINDS = frozenset({"code", "script"})

#: One comment line of a thread: `Author 2026-04-21T17:54:32.224+0200: text`,
#: at a line start, optionally after a `field:` label (the first comment of a
#: `recent_comments:` field).
_COMMENT_HEAD = re.compile(
    r"^(?:[a-z][a-z0-9_]{0,40}:[ \t]*)?"
    r"(?P<author>[^\s:][^\n:]{0,119}?)[ \t]+"
    r"(?P<date>\d{4}-\d{2}-\d{2})T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?"
    r"(?:Z|[+-]\d{2}:?\d{2})?:[ \t]?",
    re.M,
)
#: One `field: value` block of a fielded text: a lower-case label at the
#: text's start or after a blank line.
_FIELD_HEAD = re.compile(r"(?:\A|(?<=\n\n))(?P<name>[a-z][a-z0-9_]{0,40}):[ \t]?")


@dataclass(frozen=True)
class ThreadComment:
    author: str
    date: str
    #: Offsets in the text: the comment line's start, its text's start, its end.
    head: int
    start: int
    end: int


@dataclass(frozen=True)
class ThreadField:
    name: str
    start: int
    end: int


@dataclass(frozen=True)
class ThreadParts:
    fields: tuple[ThreadField, ...]
    comments: tuple[ThreadComment, ...]

    def comment_at(self, offset: int) -> Optional[ThreadComment]:
        for comment in self.comments:
            if comment.head <= offset < comment.end:
                return comment
        return None

    def field_at(self, offset: int) -> Optional[ThreadField]:
        for field in self.fields:
            if field.start <= offset < field.end:
                return field
        return None


def thread_parts(text: str) -> ThreadParts:
    """The `field: value` blocks and the dated comment lines of `text`.

    A comment runs from its line to the next comment line, or to the next
    field block, or to the end. A field runs to the next field block; a
    field holding comments ends where its first comment line starts."""
    heads = [(m.start(), m.end(), m.group("name")) for m in _FIELD_HEAD.finditer(text)]
    comments_raw = [
        (m.start(), m.end(), m.group("author").strip(), m.group("date"))
        for m in _COMMENT_HEAD.finditer(text)
    ]
    field_starts = [start for start, _end, _name in heads]
    comment_starts = [start for start, _end, _author, _date in comments_raw]
    comments: list[ThreadComment] = []
    for index, (head, start, author, date) in enumerate(comments_raw):
        end = len(text)
        if index + 1 < len(comments_raw):
            end = comment_starts[index + 1]
        later_field = next((s for s in field_starts if s > head), None)
        if later_field is not None and later_field < end:
            end = later_field
        comments.append(ThreadComment(author=author, date=date, head=head, start=start, end=end))
    fields: list[ThreadField] = []
    for index, (head, start, name) in enumerate(heads):
        end = field_starts[index + 1] if index + 1 < len(heads) else len(text)
        first_comment = next((s for s in comment_starts if head <= s < end), None)
        if first_comment is not None:
            end = first_comment if first_comment > head else start
        fields.append(ThreadField(name=name, start=start, end=end))
    return ThreadParts(fields=tuple(fields), comments=tuple(comments))


@dataclass(frozen=True)
class ChainedText:
    """One record's text in reading order, as `inspect` kind text serves it,
    with the chunk each part came from."""

    text: str
    starts: tuple[int, ...]
    chunk_ids: tuple[str, ...]

    def chunk_at(self, offset: int) -> Optional[str]:
        if not self.starts:
            return None
        index = bisect.bisect_right(self.starts, offset) - 1
        return self.chunk_ids[max(0, index)]


def _runs_on(previous: Optional[_Segment], current: _Segment) -> bool:
    """`current`'s stored span starts exactly where `previous`'s ends.

    Weaker than `_continues`, which also needs `doc_char_length` before it
    numbers lines as the source's: a ticket's window chunks carry exact
    spans (their 200-character overlap is trimmed) but no document length,
    so `number_lines` starts a new line at each chunk. Joining those chunks
    with a line feed split words and comment heads mid-line (a passage
    showed `keep_miniao` and `d=True` on two lines), so the passage text
    joins them as the spans prove they join."""
    if previous is None or previous.redacted or current.redacted:
        return False
    if not (_exact(previous) and _exact(current)):
        return False
    assert previous.char_end is not None and current.char_offset is not None
    return current.char_offset + current.trimmed_chars == previous.char_end


def chain_segments(segments: Sequence[_Segment]) -> ChainedText:
    """The served text of numbered segments, joined as `number_lines` reads
    them (a segment that does not continue the one before starts a line),
    except where the stored spans prove the two run on (`_runs_on`)."""
    parts: list[str] = []
    starts: list[int] = []
    ids: list[str] = []
    length = 0
    previous: Optional[_Segment] = None
    for segment in segments:
        if segment.line_break_before and not _runs_on(previous, segment):
            parts.append("\n")
            length += 1
        previous = segment
        starts.append(length)
        ids.append(segment.chunk_id)
        parts.append(segment.text)
        length += len(segment.text)
    return ChainedText(text="".join(parts), starts=tuple(starts), chunk_ids=tuple(ids))


def search_record_texts(
    conn: psycopg.Connection,
    session: Session,
    record_ids: Sequence[str],
    *,
    byte_budget: int = PASSAGE_PAGE_BYTES,
    seconds: float = PASSAGE_PAGE_SECONDS,
) -> dict[str, ChainedText]:
    """Each record's whole chunk text, for the search rows' passages.

    Empty under access-rule audit or enforce (see the section note), on a
    connection inside a transaction block (the checkout's read lease
    commits), and for a record with no visible chunk text or over the
    `MATCH_LINE_RECORD_*` size caps. No record is read once the page holds
    `byte_budget` bytes or `seconds` have passed; the rows without a text
    here say so through their passage's `basis`."""
    wanted = list(dict.fromkeys(str(record_id) for record_id in record_ids))
    if not wanted or _read_filter_active(session):
        return {}
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        return {}
    assert_session_matches(conn, session)
    graph = _checkout(conn, session)
    stop_at = time.monotonic() + seconds
    loaded = 0
    out: dict[str, ChainedText] = {}
    for record_id in wanted:
        if loaded >= byte_budget or time.monotonic() >= stop_at:
            break
        segments = _sized_record_segments(conn, session, graph, record_id)
        if not segments:
            continue
        chained = chain_segments(segments)
        if not chained.text.strip():
            continue
        out[record_id] = chained
        loaded += len(chained.text.encode("utf-8"))
    return out


def _day(value: str) -> str:
    return value[:10]


def _cut_text(text: str, start: int, end: int, *, lo: int, hi: int) -> str:
    body = text[start:end].strip()
    prefix = "..." if start > lo and text[lo:start].strip() else ""
    suffix = "..." if end < hi and text[end:hi].strip() else ""
    return f"{prefix}{body}{suffix}"


def _code_passage(
    text: str,
    span: Optional[tuple[int, int]],
    *,
    first_line: Optional[int],
    last_line: Optional[int],
    signature: Optional[str],
    docstring: Optional[str],
    chars: Optional[int] = None,
) -> dict[str, Any]:
    lines = text.split("\n")
    whole_chars = CODE_WHOLE_CHARS if chars is None else chars
    max_chars = CODE_MAX_CHARS if chars is None else chars
    if len(text) <= whole_chars:
        end_line = (
            last_line if last_line is not None
            else (first_line + len(lines) - 1 if first_line is not None else None)
        )
        return {
            "text": text.strip("\n"), "whole": True, "where": "lines",
            "start_line": first_line, "end_line": end_line,
        }
    at = text.count("\n", 0, span[0]) if span is not None else 0
    first = max(0, at - CODE_CONTEXT_LINES)
    last = min(len(lines), at + CODE_CONTEXT_LINES + 1)
    body = "\n".join(lines[first:last])
    header: list[str] = []
    if first > 0:
        if isinstance(signature, str) and signature.strip():
            header.append(signature.strip().split("\n")[0])
        if isinstance(docstring, str) and docstring.strip():
            header.append(docstring.strip().split("\n")[0])
        if header:
            header.append("...")
    if chars is None and header:
        # PACT read-reply-size-3666: with no `max_chars` the whole passage,
        # its signature and docstring lines included, holds at most
        # `CODE_MAX_CHARS`; a header longer than half of it is left out.
        room = max_chars - len("\n".join(header)) - 1
        if room < max_chars // 2:
            header, room = [], max_chars
        max_chars = room
    if len(body) > max_chars:
        # Long lines: keep the matched characters, centred. The cut's two
        # `...` marks are inside the budget.
        before = sum(len(line) + 1 for line in lines[:first])
        offset = max(0, (span[0] if span is not None else 0) - before)
        budget = max_chars - (6 if chars is None else 0)
        cut = centred_cut(
            body, (offset, offset + 1), lo=0, hi=len(body), budget=budget,
        )
        body = _cut_text(body, cut[0], cut[1], lo=0, hi=len(body))
    return {
        "text": "\n".join([*header, body]),
        "whole": False,
        "where": "lines",
        "start_line": first_line + first if first_line is not None else None,
        "end_line": first_line + last - 1 if first_line is not None else None,
    }


def _matched_unit(
    text: str,
    matcher: Any,
    span: tuple[int, int],
    parts: ThreadParts,
    budget: int,
) -> tuple[Optional[ThreadComment], Optional[ThreadField], tuple[int, int]]:
    """The comment or description that holds the match, and the span inside it.

    `best_match_span` returns the best window's words from its FIRST hit, and
    on a tie the earliest window wins: a lone query word at the end of one
    comment can start a window whose other words are in the next comment.
    Naming the comment at the span's start then attributed the match to a
    comment that holds almost none of it. Every comment and the description
    block the window touches is a candidate; the one holding the most
    distinct query terms wins (the earliest on a tie), and the span is
    chosen again inside it, so the passage is centred on its own words."""
    candidates: list[tuple[int, int, Optional[ThreadComment], Optional[ThreadField]]] = []
    for comment in parts.comments:
        if comment.head < span[1] and span[0] < comment.end:
            candidates.append((comment.start, comment.end, comment, None))
    for field in parts.fields:
        if (
            field.name == "description" and field.start < span[1]
            and span[0] < field.end
        ):
            candidates.append((field.start, field.end, None, field))
    if not candidates:
        return None, None, span
    candidates.sort(key=lambda unit: unit[0])
    inside = parts.comment_at(span[0]) is not None or any(
        f.name == "description" and f.start <= span[0] < f.end for f in parts.fields
    )
    best: Optional[tuple[int, int, Optional[ThreadComment], Optional[ThreadField]]] = None
    best_count = 0
    if not inside:
        # The window starts outside every unit: the record text before the
        # first unit competes as one more candidate.
        best_count = len(matched_terms(text, matcher, span[0], candidates[0][0]))
    for unit in candidates:
        count = len(matched_terms(text, matcher, unit[0], unit[1]))
        if count > best_count:
            best, best_count = unit, count
    if best is None:
        return None, None, span
    lo, hi, comment, field = best
    inner = best_match_span(text[lo:hi], matcher, max(1, budget - 6))
    if inner is None:
        return comment, field, span
    return comment, field, (lo + inner[0], lo + inner[1])


def _unit_budget(text: str, lo: int, hi: int, chars: Optional[int]) -> int:
    """The characters a matched comment or description block `text[lo:hi]`
    keeps: the caller's `max_chars` when given (D2); else the whole unit
    when it is at most `UNIT_WHOLE_CHARS` long, as a short code chunk is
    whole; else the `PASSAGE_SHORT_CHARS` window centred on the match."""
    if chars is not None:
        return chars
    length = len(text[lo:hi].strip())
    if length <= UNIT_WHOLE_CHARS:
        return max(PASSAGE_SHORT_CHARS, hi - lo)
    return PASSAGE_SHORT_CHARS


def text_passage(
    text: str,
    matcher: Any,
    *,
    budget: int,
    basis: str,
    code: bool = False,
    body: bool = False,
    anchor_required: bool = True,
    first_line: Optional[int] = None,
    last_line: Optional[int] = None,
    signature: Optional[str] = None,
    docstring: Optional[str] = None,
    chunk_at: Optional[Any] = None,
    chars: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    """A search row's matched passage over `text`, or None.

    The window holds the most query words (`excerpt_window.best_match_span`,
    the ranker's lexized matching) and is centred on them. When they are in
    a dated comment line, the passage is that comment's text with its author
    and date; in a `description:` block, that block. A comment or
    description of at most `UNIT_WHOLE_CHARS` is shown whole; a longer one
    is cut to `PASSAGE_SHORT_CHARS` around the match (`_unit_budget`). None when no query word is in `text` and `anchor_required`: a
    passage is never the record's start passed off as the match. `code`
    shows lines (`_code_passage`); `body` marks a commit body. `chars`, the
    caller's `max_chars` (D2, the one size control), replaces every per-type
    size when given."""
    if chars is not None:
        budget = chars
    if not isinstance(text, str) or not text.strip():
        return None
    span = None
    if matcher is not None and not getattr(matcher, "empty", True):
        span = best_match_span(text, matcher, max(1, budget - 6))
    if span is None and anchor_required:
        return None
    passage: dict[str, Any] = {
        "text": "", "whole": False, "where": "body" if body else "record",
        "author": None, "date": None, "start_line": None, "end_line": None,
        "chunk_id": None, "basis": basis,
    }
    if code:
        passage.update(_code_passage(
            text, span, first_line=first_line, last_line=last_line,
            signature=signature, docstring=docstring, chars=chars,
        ))
    else:
        lo, hi = 0, len(text)
        if span is not None and not body:
            parts = thread_parts(text)
            comment, field, span = _matched_unit(text, matcher, span, parts, budget)
            if comment is not None:
                lo, hi = comment.start, comment.end
                budget = _unit_budget(text, lo, hi, chars)
                passage.update(where="comment", author=comment.author, date=_day(comment.date))
            elif field is not None and field.name == "description":
                lo, hi = field.start, field.end
                budget = _unit_budget(text, lo, hi, chars)
                passage["where"] = "description"
        if chars is None and budget > PASSAGE_DEFAULT_MAX_CHARS - 6 and hi - lo > budget:
            # PACT read-reply-size-3666: a cut passage's two `...` marks stay
            # inside the 800 characters a passage holds by default.
            budget = PASSAGE_DEFAULT_MAX_CHARS - 6
        anchor = span if span is not None else (lo, lo)
        start, end = centred_cut(text, anchor, lo=lo, hi=hi, budget=budget)
        passage["text"] = _cut_text(text, start, end, lo=lo, hi=hi)
        passage["whole"] = start <= lo and end >= hi
        if passage["where"] == "record" and passage["whole"]:
            passage["start_line"], passage["end_line"] = first_line, last_line
    if chunk_at is not None:
        passage["chunk_id"] = chunk_at(span[0] if span is not None else 0)
    return passage if passage["text"] else None


def thread_record_text(text: str) -> Optional[dict[str, Any]]:
    """A ticket-shaped text as fields, description and dated comments, or
    None when `text` is not fielded (it must start with a `field:` block)
    or holds neither a description nor a comment.

    The `inspect` form of a ticket (R6): the body once, then the comments
    in order. Every comment is listed here; `fit_record_text` cuts to a
    byte budget."""
    if not isinstance(text, str) or not _FIELD_HEAD.match(text):
        return None
    parts = thread_parts(text)
    fields: dict[str, str] = {}
    description: Optional[str] = None
    for field in parts.fields:
        value = text[field.start:field.end].strip()
        if field.name == "description":
            description = value if description is None else description
        elif value and field.name not in fields:
            fields[field.name] = value
    comments = [
        {
            "author": comment.author,
            "date": _day(comment.date),
            "text": text[comment.start:comment.end].strip(),
        }
        for comment in parts.comments
    ]
    if description is None and not comments:
        return None
    return {
        "fields": fields,
        "description": description,
        "comments": comments,
        "comments_total": len(comments),
        "comments_shown": len(comments),
        "comments_left": 0,
    }


#: The payload key of a ticket-shaped record's thread (`thread_record_text`).
RECORD_TEXT_KEY = "record_text"


def segments_thread(
    segments: Sequence[_Segment], trust_by_source: Mapping[Optional[str], str],
) -> Optional[dict[str, Any]]:
    """The thread of a record's served segments, fenced as a whole when any
    segment is untrusted (its producer, or an untrusted-transcript marker)."""
    block = thread_record_text(chain_segments(segments).text)
    if block is None:
        return None
    untrusted = [
        segment for segment in segments
        if trust_by_source.get(segment.source, "untrusted") == "untrusted"
        or segment.untrusted_origin is not None
    ]
    if untrusted:
        from okg.substrate.mcp.provenance import _fence_attr_value

        source = untrusted[0].source
        block = {
            **block,
            "fields": _fence_attr_value(block["fields"], source=source),
            "description": _fence_attr_value(block["description"], source=source),
            "comments": [
                {
                    **comment,
                    "author": _fence_attr_value(comment["author"], source=source),
                    "text": _fence_attr_value(comment["text"], source=source),
                }
                for comment in block["comments"]
            ],
        }
    return block


def record_thread(
    conn: psycopg.Connection, session: Session, node_id: str,
) -> Optional[dict[str, Any]]:
    """`inspect` kind node's thread of a ticket-shaped record (R6), every
    comment listed; the caller fits it. Read as kind text reads the record
    (`_sized_record_segments`: its size caps and access rules). None for a
    record with no chunk text, one that is not ticket-shaped, one over the
    caps, and on a connection inside a transaction block."""
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        return None
    assert_session_matches(conn, session)
    graph = _checkout(conn, session)
    segments = _sized_record_segments(conn, session, graph, node_id)
    if not segments:
        return None
    trust = classify_sources(
        conn, deployment_name=session.deployment_name,
        sources=(segment.source for segment in segments),
    )
    return segments_thread(segments, trust)


def fit_record_text(
    block: dict[str, Any], fits: Any,
) -> Optional[dict[str, Any]]:
    """`block` with as many comments as `fits(candidate)` accepts.

    The newest comments are kept first (and listed oldest first); a cut
    block says how many were left and, in `comments_left_chars`, the
    characters of their text, so a later cut of the block can still say
    how long the whole thread is. None when not even the block without
    comments fits."""
    from okg.substrate.mcp.response_bounds import record_comment_chars

    comments = list(block.get("comments") or [])
    total = len(comments)
    keep = total
    while keep >= 0:
        shown = comments[total - keep:] if keep else []
        candidate = {
            **block,
            "comments": shown,
            "comments_shown": len(shown),
            "comments_left": total - len(shown),
        }
        if len(shown) < total:
            candidate["comments_left_chars"] = record_comment_chars(
                comments[:total - len(shown)],
            )
        if fits(candidate):
            return candidate
        if keep == 0:
            return None
        # Halve the gap quickly on long threads, one by one near the end.
        keep = keep - 1 if keep <= 8 else keep - max(1, keep // 4)
    return None
