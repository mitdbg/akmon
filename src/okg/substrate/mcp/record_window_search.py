"""`search(method="grep", view="windows")`: literal windows per scoped record.

PACT read-record-windows (issue #3186). The adapter that joins the pure
window core (`record_windows`), the position pager (`record_window_pages`)
and the shared scanner contract (`read_scanner`) to one published read.

What one call does, in order:

1. Refuse an unsupported argument combination before any graph read
   (`validate_window_call`). Windows need `method="grep"`, a record `scope`
   and a query with a positive text term; `where`, `path:` terms,
   `path_prefix`, `offset`, profiles, families and legacy filters are
   refused by name rather than ignored. The rows are fixed: this read takes
   no `select`; the served tool cuts the finished rows to one.
2. For each scoped record the reader can see (absent and hidden read the
   same: no row), read its own text and the passages its subtype declares
   through the pinned catalog's passage relation. A named record is read
   whatever the default search profile holds; one whose content sits in a
   document names that document (`TEXT_ELSEWHERE_KEY`, issue
   #3467). The occurrence statements
   run whole (they refuse past `RAW_OCCURRENCE_SAFETY_CAP` raw rows per record
   instead of stopping early), so the batch carries `whole_sql` enumeration
   evidence and every candidate is finished by the reader's own Nomos filter.
   The call admits `MAX_READABLE_OCCURRENCE_ROWS` readable rows in all and
   stops at `MAX_WINDOW_CALL_SECONDS` of wall-clock work.
3. Decide each occurrence's position authority. Only a real producer
   contract proves positions: the occurrence edge and the record come from
   the same producer, that producer is declared in the deployment's source
   registry as the doc-corpus source (`DOC_CORPUS_LAYOUT`), the edge names
   the record's own source record (repo and path) and revision, the edge
   stores integer `char_offset`/`char_end`/`doc_char_length`, and the
   readable text equals the stored text. Equal lengths or matching
   attributes alone never prove it; generic passage links keep unknown
   positions.
4. Assemble chains, scan PostgreSQL term events, decide membership and
   merge windows (`record_windows`). Record matching refuses
   (`window_continuity_unknown`) when unknown continuity leaves a phrase,
   NEAR or regex term undecidable; passage matching evaluates each readable
   text alone and never claims record-wide evidence.
5. Page the planned windows by actual served bytes (`record_window_pages`),
   with continuation tokens issued and consumed by the shared scanner
   helpers, so every window part stays reachable across pages.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Optional

import psycopg

from okg.substrate.attributes import apply_effective_attrs_to_rows
from okg.substrate.errors import NodeNotFound, SubstrateError
from okg.substrate.mcp.continuation import ContinuationTokenInvalid, continuation_binding
from okg.substrate.mcp.grep_query import GrepQuery, GrepTerm
from okg.substrate.mcp.provenance import classify_sources, serialize_node_read_row
from okg.substrate.mcp.read_scanner import (
    EnumerationEvidence,
    FinishedCandidate,
    ScanBatch,
    ScanContinuationUnavailable,
    consume_scan_continuation,
    finish_scan_batch,
    issue_scan_continuation,
    make_scan_context,
)
from okg.substrate.mcp.record_window_pages import (
    ContinuationTooLarge,
    PlannedRecord,
    PlannedWindow,
    WindowBudgetTooSmall,
    WindowPart,
    page_windows,
)
from okg.substrate.mcp.record_windows import (
    ReadableOccurrence,
    TextChain,
    WindowContinuityUnknown,
    WindowWorkBound,
    build_chains,
    merge_windows,
    record_matches,
    repeated_head,
    scan_chain_matches,
    source_coverage,
    supporting_locations,
    window_fragments,
)
from okg.substrate.mcp.session import Session
from okg.substrate.nomos.read_filter import filter_rows_by_read_class
from okg.substrate.nomos.read_masking import REDACTION_MARKER
from okg.substrate.transcript_trust import UNTRUSTED_CONTENT_ORIGIN_ATTR

WINDOW_VIEW = "windows"
#: Every key a windows row can carry: a record's header (decided or not),
#: the page's `record_id` and the record's `windows`. A served `select`
#: (issue #3666) cuts a finished row to the keys it names; a key missing
#: here cannot be selected, so a new header key is added here too.
WINDOW_ROW_FIELDS: tuple[str, ...] = (
    "node_id", "subtype", "continuity", "source_coverage", "match_count",
    "window_count", "readable_occurrences", "masked_occurrences", "passages",
    "position_basis", "decided", "refusal", "record_id", "windows",
)
#: What a cut windows row always keeps: which record it is, and what says
#: its windows may not be the whole answer (an undecided record, the reason,
#: occurrences masked from this reader, text between passages that was not
#: proven continuous, a part of the source that was not read, passages that
#: are not declared).
WINDOW_ROW_ALWAYS: tuple[str, ...] = (
    "node_id", "subtype", "decided", "refusal", "masked_occurrences",
    "continuity", "source_coverage", "passages",
)


def _row_header(header: dict[str, Any]) -> dict[str, Any]:
    """A record's header, checked against :data:`WINDOW_ROW_FIELDS`.

    Every header is built through here, the conditional keys included, so a
    key added to a header and not to the list fails the first read that
    builds it rather than becoming a key no `select` can name.
    """
    unlisted = sorted(set(header) - set(WINDOW_ROW_FIELDS))
    if unlisted:
        raise AssertionError(f"windows row key(s) not in WINDOW_ROW_FIELDS: {unlisted}")
    return header
MATCHING_MODES = ("record", "passage")
DEFAULT_CONTEXT_CHARS = 200
MAX_CONTEXT_CHARS = 4096
#: Readable occurrences (texts) one call searches across all scoped records.
#: Past it the call refuses by name (`window_work_bound`, bound
#: `occurrences`). Counted after the reader's filter, so hidden passages
#: never move it.
MAX_WINDOW_OCCURRENCES = 5000
#: Readable source text one call scans, in UTF-8 bytes (bound `source_bytes`).
MAX_SOURCE_BYTES = 8 * 1024 * 1024
#: Internal safety cap on the raw occurrence edges one record's statement
#: reads before the reader's filter. It keeps the statement bounded; a record
#: past it refuses as `raw_occurrence_scan` without a count. It counts that
#: record's stored rows, hidden ones included, against one fixed threshold
#: that is not tunable per call, so the refusal is one bit about that record
#: alone ("it stores more than this many occurrence rows"); no choice of the
#: other scoped records moves it.
RAW_OCCURRENCE_SAFETY_CAP = 50_000
#: Readable occurrence rows (after the reader's filter) one call admits across
#: all its records; past it the call refuses (`window_work_bound`, bound
#: `readable_occurrence_rows`). Counting readable rows only means hidden
#: passages never move this boundary, however the scope is composed: a
#: call-wide count of stored rows would let a caller vary the known records in
#: a scope and read the refuse-or-answer flip as a count of hidden rows.
MAX_READABLE_OCCURRENCE_ROWS = 100_000
#: Wall-clock bound on one call's reads and scans, in seconds (bound
#: `call_time`). Checked before each record is read and before each scan, so
#: a scope of records each just under the per-record cap still does bounded
#: work. Wall time includes reading hidden rows, so it is a coarse timing
#: signal, never a count.
MAX_WINDOW_CALL_SECONDS = 10.0
#: Chunk kinds that are not part of a document's running text. Unpositioned,
#: they are searched alone and never judged for stream continuity.
NON_TEXT_CHUNK_KINDS = frozenset({"table", "image_caption"})
#: Bytes every page, first or retried, leaves free beyond this call's
#: measured stamps for what varies between the measurement and the served
#: stamp (freshness age digits, or the 60 s freshness cache refilling inside
#: the call), as the text read reserves `TEXT_READ_RESERVE_MARGIN`.
WINDOW_RESERVE_BYTES = 256
#: Extra room a newly chosen reserve carries so that a later call whose
#: stamps grew by up to this much still keeps the token's reserve, and so
#: cuts a retried page at the same points.
WINDOW_DRIFT_BYTES = 192
#: The one producer layout whose occurrence edges carry authoritative source
#: positions: doc-corpus `member_of` edges with `doc_char_length` (reparse
#: epoch 4). Registry `module` and `class`, compared exactly.
DOC_CORPUS_LAYOUT = ("okg.substrate.library.sources.doc_corpus", "DocCorpusSource")
_POSITION_EDGE = "member_of"
_EDGE_COLUMNS = (
    "e.edge_id, e.src, e.dst, e.edge_type, e.attrs, e.source, "
    "e.source_record_id, e.source_revision"
)
_NODE_COLUMNS = "node_id, subtype, attrs, source, source_record_id, source_revision"


class WindowArgumentUnsupported(SubstrateError):
    """A `view="windows"` call names an argument or value windows do not serve."""

    code = "window_argument_unsupported"


def _refuse(
    argument: str, reason: str, fix_hint: str, *, windows_call: bool = True,
) -> WindowArgumentUnsupported:
    """A windows refusal. `windows_call` is False when the caller did not
    pass `view="windows"`: the message then names the call the caller made,
    not a windows read it did not ask for (PACT tool-text-accuracy)."""
    prefix = "search view='windows'" if windows_call else "search"
    return WindowArgumentUnsupported(
        f"{prefix}: {reason}",
        data={"argument": argument, "view": WINDOW_VIEW if windows_call else None},
        fix_hint=fix_hint,
    )


def validate_window_call(
    *,
    view: Any,
    matching: Any,
    context_chars: Any,
    method: Any,
    record_ids: Optional[Sequence[str]],
    query: Any,
    where: Any = None,
    path_prefix: Any = None,
    offset: Any = 0,
    select: Any = None,
    rank_by: Any = None,
    profile: Any = None,
    source_family: Any = None,
    subtype_filter: Any = None,
    attr_filters: Any = None,
    filter: Any = None,
    max_per_parent: Any = None,
) -> Optional[tuple[str, int]]:
    """Pure. ``None`` for a legacy call, else the applied (matching, context).

    Every refusal happens here, before any graph read: a control that a
    windows read would silently drop reads as a scoped, filtered answer.
    Without a record scope the windows are read from this grep's own top
    `limit` records (#3605, Jason `winscope A`): `record_ids` is then empty.
    """
    if view is None:
        # Grep cards' `match_windows` have a fixed width; `matching` and
        # `context_chars` belong to the windows read of scoped records.
        if matching == "passage" and method == "grep":
            # Grep cards already require every term within one text, a
            # record's own or one passage: the same meaning (#3646).
            matching = None
        if matching == "record" and method == "grep":
            raise _refuse(
                "matching", "matching='record' applies only to view='windows', "
                "which this call did not pass; grep cards already match every "
                "term within one text (matching='passage')",
                # One argument away from a served call (#3605, `winscope A`).
                'add view="windows" to read this grep\'s top records as windows, '
                "or drop matching",
                windows_call=False,
            )
        if matching is not None:
            raise _refuse(
                "matching", "matching applies only to view='windows', which this "
                "call did not pass",
                'add view="windows" to read this grep\'s top records as windows'
                + ("" if method == "grep" else ' (with method="grep")')
                + ", or drop matching",
                windows_call=False,
            )
        if context_chars is not None:
            raise _refuse(
                "context_chars", "context_chars applies only to view='windows', "
                "which this call did not pass"
                + ("; grep cards' match windows have a fixed width"
                   if method == "grep" else "; card previews take max_chars"),
                ('add view="windows" to read this grep\'s top records as windows, '
                 "or drop context_chars" if method == "grep" else
                 'pass max_chars for longer card previews, or use method="grep" '
                 'with view="windows"'),
                windows_call=False,
            )
        return None
    if view != WINDOW_VIEW:
        raise _refuse("view", f"view must be 'windows' or unset, got {view!r}",
                      "drop view, or pass view='windows'")
    if method != "grep":
        raise _refuse("method", f"windows are served on method='grep', not {method!r}",
                      "pass method='grep'")
    if matching is None:
        matching = "record"
    if matching not in MATCHING_MODES:
        raise _refuse("matching", f"matching must be 'record' or 'passage', got {matching!r}",
                      "pass matching='record' or matching='passage'")
    if context_chars is None:
        context_chars = DEFAULT_CONTEXT_CHARS
    if (
        not isinstance(context_chars, int) or isinstance(context_chars, bool)
        or not 0 <= context_chars <= MAX_CONTEXT_CHARS
    ):
        raise _refuse("context_chars",
                      f"context_chars must be an integer in 0..{MAX_CONTEXT_CHARS}",
                      f"pass an integer from 0 to {MAX_CONTEXT_CHARS}")
    refused = (
        ("where", where is not None, "drop where; windows read the scoped records whole"),
        ("path_prefix", path_prefix is not None, "drop path_prefix; scope names the records"),
        ("offset", offset not in (None, 0), "page with the continuation_token instead of offset"),
        ("select", select is not None,
         "omit select here; the served tool cuts the finished rows to a "
         "select of windows row keys"),
        ("rank_by", rank_by not in (None, "records"), "drop rank_by"),
        ("profile", profile is not None, "drop profile; scope names the records"),
        ("source_family", source_family is not None, "drop source_family"),
        ("subtype_filter", subtype_filter is not None, "drop subtype_filter"),
        ("attr_filters", bool(attr_filters), "drop attr_filters"),
        ("filter", bool(filter), "drop filter"),
        ("max_per_parent", max_per_parent is not None, "drop max_per_parent"),
    )
    for argument, present, hint in refused:
        if present:
            raise _refuse(
                argument,
                "select is not taken by the windows read, whose rows are fixed"
                if argument == "select" else f"{argument} is not served with windows",
                hint,
            )
    if not isinstance(query, str) or not query.strip():
        raise _refuse("query", "windows need query text", "pass a grep query")
    # Pure: the grammar refusals and the windows term rules, before any
    # connection opens.
    from okg.substrate.mcp.grep_query import parse_grep_query

    assert_window_query(parse_grep_query(query))
    return str(matching), int(context_chars)


def assert_window_query(parsed: GrepQuery) -> None:
    """Windows need one positive text term and evaluate no `path:` term."""
    if parsed.path_terms:
        raise _refuse("query", "path: terms are not evaluated by windows",
                      "drop the path: term; scope names the records")
    if not parsed.positive:
        raise _refuse("query", "windows need at least one positive text term",
                      "add a word, phrase, NEAR or re:/.../ term")


# ── the producer position contract ────────────────────────────────────────


def deployment_position_producers(deployment_name: Optional[str]) -> frozenset[str]:
    """Producer ids the deployment's source registry declares as doc-corpus.

    The registry entry's `module` and `class` must equal `DOC_CORPUS_LAYOUT`.
    A deployment that cannot be resolved has no declared producers, so every
    position stays unknown: an absent contract never proves continuity.
    """
    if not deployment_name:
        return frozenset()
    from okg.substrate.deployments import DeploymentResolutionError, resolve_deployment
    from okg.substrate.sources.registry import load_source_registry

    try:
        manifest = resolve_deployment(deployment_name)
    except DeploymentResolutionError:
        return frozenset()
    registry_path = getattr(manifest, "source_registry", None)
    if registry_path is None:
        return frozenset()
    entries = load_source_registry(registry_path)
    return frozenset(
        name for name, entry in entries.items()
        if (entry.raw.get("module"), entry.raw.get("class")) == DOC_CORPUS_LAYOUT
    )


def _int(value: Any) -> Optional[int]:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _record_key(value: Any) -> Any:
    """Parse a stored JSON record identity; a non-object reads as None."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return None
    return value if isinstance(value, Mapping) else None


def _same_identity(left: Any, right: Any) -> bool:
    left, right = _record_key(left), _record_key(right)
    return left is not None and right is not None and dict(left) == dict(right)


def position_authority(
    *,
    record: Mapping[str, Any],
    edge: Optional[Mapping[str, Any]],
    raw_edge_attrs: Optional[Mapping[str, Any]],
    chunk_id: str,
    readable_text: str,
    stored_text: Any,
    producers: frozenset[str],
) -> tuple[Optional[str], str]:
    """``(stream_id, basis)``: a stream id only under the producer contract.

    ``basis`` names why positions are or are not authoritative, so the
    response can disclose it. Every check is on identity the pinned graph
    stores (producer, source record, revision) or on the stored text itself.
    """
    if edge is None:
        return None, "no_position_edge"
    source = edge.get("source")
    if source is None or source not in producers:
        return None, "producer_contract_unknown"
    if source != record.get("source"):
        return None, "producer_differs_from_record"
    edge_record = _record_key(edge.get("source_record_id"))
    record_identity = _record_key(record.get("source_record_id"))
    if (
        edge_record is None or record_identity is None
        or edge_record.get("chunk_id") != chunk_id
        or any(edge_record.get(k) != record_identity.get(k) for k in ("repo", "path"))
        or record_identity.get("path") is None
    ):
        return None, "source_record_differs"
    if not _same_identity(edge.get("source_revision"), record.get("source_revision")):
        return None, "revision_differs"
    attrs = edge.get("attrs") if isinstance(edge.get("attrs"), Mapping) else {}
    raw = raw_edge_attrs or {}
    keys = ("char_offset", "char_end", "doc_char_length")
    if any(_int(attrs.get(k)) is None or attrs.get(k) != raw.get(k) for k in keys):
        # An accepted attribute override never lends coordinates.
        return None, "positions_not_stored"
    if attrs.get("separator_before") != raw.get("separator_before"):
        # Nor a separator: an override could fake adjacency across a gap.
        return None, "separator_not_stored"
    if not isinstance(stored_text, str) or stored_text != readable_text:
        return None, "readable_text_differs"
    stream = hashlib.sha256(json.dumps(
        [source, record_identity.get("repo"), record_identity.get("path"),
         _record_key(record.get("source_revision"))],
        sort_keys=True, separators=(",", ":"), default=str,
    ).encode()).hexdigest()[:24]
    return stream, "producer_contract"


# ── reading one record's readable occurrences ─────────────────────────────


@dataclass
class _RecordRead:
    record: dict[str, Any]
    own: Optional[ReadableOccurrence]
    occurrences: list[ReadableOccurrence]
    masked: int
    passages: str
    position_bases: dict[str, int]
    complete: bool
    chunk_texts: dict[str, str]
    sources: dict[str, Any]


def _declared_relation(conn: psycopg.Connection, session: Session, subtype: str) -> Any:
    from okg.substrate.mcp.passage_rollup import (
        PassageRollupUndeclared,
        passage_rollup_declaration,
    )

    try:
        return passage_rollup_declaration(
            conn, catalog_version_id=session.catalog_version_id, subtype=subtype,
        )
    except PassageRollupUndeclared:
        return None


def _redacted(attrs: Any) -> bool:
    return isinstance(attrs, Mapping) and bool(attrs.get(REDACTION_MARKER))


def _charge(budget: list[int], texts: Sequence[str]) -> None:
    """Spend the call's occurrence and source-byte bounds on readable texts.

    Only what this reader reads is counted, so hidden passages never move
    either bound; both are checked before any text is scanned.
    """
    budget[0] -= len(texts)
    budget[1] -= sum(len(text.encode("utf-8")) for text in texts)
    if budget[0] < 0:
        raise WindowWorkBound(
            "window occurrence bound reached",
            data={"bound": "occurrences", "max_occurrences": MAX_WINDOW_OCCURRENCES},
            fix_hint="scope fewer or smaller records",
        )
    if budget[1] < 0:
        raise WindowWorkBound(
            "window source byte bound reached",
            data={"bound": "source_bytes", "max_source_bytes": MAX_SOURCE_BYTES},
            fix_hint="scope fewer or smaller records",
        )


def page_reserve(stamp_bytes: int, token_reserve: Optional[int], max_bytes: int) -> int:
    """Pure. The bytes a page keeps free below ``max_bytes`` for the stamps.

    A token's reserve is kept while this call's measured stamps fit it with
    the full `WINDOW_RESERVE_BYTES` to spare, so a retried page has the same
    spare as a first page and its cut points do not move. Otherwise (a first
    page, or stamps that outgrew the token's reserve) the reserve is the
    measured stamps plus that spare plus `WINDOW_DRIFT_BYTES`; the drift room
    is what lets a later retry, whose stamps gained a few digits, keep the
    token's reserve. A new reserve is still lossless: it only moves the cut.
    """
    need = max(stamp_bytes, 0)
    if token_reserve is not None and need + WINDOW_RESERVE_BYTES <= token_reserve < max_bytes:
        return token_reserve
    return need + WINDOW_RESERVE_BYTES + WINDOW_DRIFT_BYTES


def _check_deadline(deadline: float) -> None:
    """Refuse by name once the call's wall-clock bound has passed."""
    if time.monotonic() >= deadline:
        raise WindowWorkBound(
            "window call time bound reached",
            data={"bound": "call_time", "max_seconds": MAX_WINDOW_CALL_SECONDS},
            fix_hint="scope fewer or smaller records",
        )


def read_record(
    conn: psycopg.Connection,
    session: Session,
    graph: Any,
    record_id: str,
    *,
    context: Any,
    producers: frozenset[str],
    budget: list[int],
) -> Optional[_RecordRead]:
    """One visible record's readable occurrences, or None when not visible."""
    from okg.substrate.mcp.record_text import _visible_start

    try:
        record = _visible_start(conn, session, graph, record_id, suggest=False)
    except NodeNotFound:
        return None
    record_attrs = record.get("attrs") if isinstance(record.get("attrs"), Mapping) else {}
    own: Optional[ReadableOccurrence] = None
    masked = 0
    own_text = record_attrs.get("text")
    if _redacted(record_attrs):
        masked += 1 if isinstance(own_text, str) else 0
    elif isinstance(own_text, str) and own_text:
        own = ReadableOccurrence(
            record_id=record_id, chunk_id=record_id, text=own_text, edge_id="self",
            row={
                "source": record.get("source"), "kind": "own_text",
                UNTRUSTED_CONTENT_ORIGIN_ATTR: record_attrs.get(UNTRUSTED_CONTENT_ORIGIN_ATTR),
            },
        )
    relation = _declared_relation(conn, session, str(record.get("subtype")))
    if relation is None:
        _charge(budget, [own.text] if own is not None else [])
        return _RecordRead(record, own, [], masked, "undeclared", {}, True, {}, {})
    params = {
        "record": record_id, "edge_type": relation.edge_type,
        "child": relation.child_subtype, "position_edge": _POSITION_EDGE,
        "raw_limit": RAW_OCCURRENCE_SAFETY_CAP + 1,
    }
    raw_edges = [dict(row) for row in graph.query(
        f"SELECT {_EDGE_COLUMNS} FROM okg.graph_edges e "
        "JOIN okg.graph_nodes c ON c.node_id = CASE WHEN e.src = :record THEN e.dst ELSE e.src END "
        "WHERE ((e.src = :record AND e.edge_type = :edge_type) "
        "OR (e.dst = :record AND e.edge_type IN (:edge_type, :position_edge))) "
        "AND c.subtype = :child ORDER BY e.edge_id LIMIT :raw_limit",
        params,
    )]
    if len(raw_edges) >= params["raw_limit"]:
        # A statement-size guard on this record's stored rows, hidden ones
        # included, against a fixed threshold; it names no count, so it
        # discloses one bit about this record (it is huge), whatever else the
        # scope holds.
        raise WindowWorkBound(
            "window raw occurrence scan bound reached",
            data={"bound": "raw_occurrence_scan"},
            fix_hint="scope fewer or smaller records",
        )
    raw_attrs = {
        row["edge_id"]: dict(row["attrs"]) if isinstance(row.get("attrs"), Mapping) else {}
        for row in raw_edges
    }
    # The whole statement ran (it refuses past the bound instead of stopping
    # early), so this is terminal enumeration of the record's raw occurrence
    # edges; finishing below decides which the reader may read.
    batch = ScanBatch(
        rows=tuple(raw_edges), count_unit="occurrence_edge", count_set="prefilter_candidates",
        count_basis="exact", total_count=len(raw_edges),
        enumeration=EnumerationEvidence(
            domain="record_occurrence_edges", scope_identity=context.scope_identity,
            method="whole_sql",
        ),
        terminal=True,
    )
    effective = apply_effective_attrs_to_rows(
        conn, [dict(row) for row in raw_edges], target_kind="edge",
        target_id_field="edge_id", generation_id=session.generation_id,
    )
    readable_edges = {
        row["edge_id"]: row
        for row in filter_rows_by_read_class(conn, session, effective, row_kind="edge")
    }
    # The call-wide row bound spends readable rows only.
    budget[2] -= len(readable_edges)
    if budget[2] < 0:
        raise WindowWorkBound(
            "window readable occurrence row bound reached",
            data={"bound": "readable_occurrence_rows",
                  "max_rows": MAX_READABLE_OCCURRENCE_ROWS},
            fix_hint="scope fewer or smaller records",
        )
    child_ids = sorted({
        str(row["dst"] if row["src"] == record_id else row["src"]) for row in raw_edges
    })
    stored_nodes = {
        str(row["node_id"]): dict(row) for row in graph.query(
            f"SELECT {_NODE_COLUMNS} FROM okg.graph_nodes WHERE node_id = ANY(:ids) "
            "AND subtype = :child", {"ids": child_ids, "child": relation.child_subtype},
        )
    } if child_ids else {}
    readable_nodes = {
        str(row["node_id"]): row for row in filter_rows_by_read_class(
            conn, session,
            apply_effective_attrs_to_rows(
                conn, [dict(row) for row in stored_nodes.values()], target_kind="node",
                target_id_field="node_id", generation_id=session.generation_id,
            ),
            row_kind="node",
        )
    }

    def finish(candidate: Mapping[str, Any]) -> Optional[FinishedCandidate]:
        edge = readable_edges.get(candidate["edge_id"])
        if edge is None:
            return None
        return FinishedCandidate(rows=(edge,), auxiliary=(
            {"edge_id": candidate["edge_id"], "raw_attrs": raw_attrs[candidate["edge_id"]]},
        ))

    finished = finish_scan_batch(context, batch, finish,
                                 finished_domain="readable_record_occurrence_edges")
    complete = bool(finished.enumeration and finished.enumeration.finished)
    members: dict[str, dict[str, Any]] = {}
    positions: dict[str, dict[str, Any]] = {}
    for edge in finished.rows:
        member_of_record = edge["dst"] == record_id
        chunk_id = str(edge["src"] if member_of_record else edge["dst"])
        if edge["edge_type"] == relation.edge_type:
            members.setdefault(chunk_id, edge)
        if member_of_record and edge["edge_type"] == _POSITION_EDGE:
            current = positions.get(chunk_id)
            if current is None or str(edge["edge_id"]) < str(current["edge_id"]):
                positions[chunk_id] = edge
    if relation.edge_type == _POSITION_EDGE:
        positions = {k: v for k, v in positions.items() if k in members}
    occurrences: list[ReadableOccurrence] = []
    bases: dict[str, int] = {}
    chunk_texts: dict[str, str] = {}
    sources: dict[str, Any] = {}
    #: Each occurrence's span as stored (never an attribute override), for
    #: the overlap a window chunker repeats (`repeated_head`).
    stored_spans: dict[str, tuple[Optional[int], Optional[int]]] = {}
    for chunk_id in sorted(members):
        node = readable_nodes.get(chunk_id)
        if node is None:
            continue
        node_attrs = node.get("attrs") if isinstance(node.get("attrs"), Mapping) else {}
        member = members[chunk_id]
        if _redacted(node_attrs) or _redacted(member.get("attrs")):
            masked += 1
            continue
        text = node_attrs.get("text")
        if not isinstance(text, str):
            continue
        position = positions.get(chunk_id)
        stored = stored_nodes.get(chunk_id, {}).get("attrs")
        stream, basis = position_authority(
            record=record, edge=position,
            raw_edge_attrs=raw_attrs.get(position["edge_id"]) if position else None,
            chunk_id=chunk_id, readable_text=text,
            stored_text=stored.get("text") if isinstance(stored, Mapping) else None,
            producers=producers,
        )
        bases[basis] = bases.get(basis, 0) + 1
        pattrs = position.get("attrs") if position and isinstance(position.get("attrs"), Mapping) else {}
        separator = pattrs.get("separator_before")
        occurrences.append(ReadableOccurrence(
            record_id=record_id, chunk_id=chunk_id, text=text,
            edge_id=str((position or member)["edge_id"]),
            char_offset=_int(pattrs.get("char_offset")) if stream else None,
            char_end=_int(pattrs.get("char_end")) if stream else None,
            doc_char_length=_int(pattrs.get("doc_char_length")) if stream else None,
            separator_before=separator if stream and isinstance(separator, str)
            and separator.isspace() else None,
            row={
                "source": node.get("source"),
                "chunk_ordinal": _int(pattrs.get("chunk_ordinal")),
                UNTRUSTED_CONTENT_ORIGIN_ATTR: node_attrs.get(UNTRUSTED_CONTENT_ORIGIN_ATTR),
                # The stored kind, as for `separator_before`: an attribute
                # override relabelling running text as a table must not take
                # it out of the stream (and so out of the continuity check).
                "non_text": isinstance(stored, Mapping) and (
                    stored.get("chunk_kind") in NON_TEXT_CHUNK_KINDS
                    or stored.get("chunk_source") == "vision_caption"
                ),
            },
            stream_id=stream, positions_authoritative=stream is not None,
        ))
        chunk_texts[chunk_id] = text
        sources[chunk_id] = node.get("source")
        # The span as this reader reads it, and only when it is the stored one:
        # an override or a redaction never lends a span.
        stored = raw_attrs.get(position["edge_id"], {}) if position else {}
        span_start, span_end = (
            _int(pattrs.get(key)) if pattrs.get(key) == stored.get(key) else None
            for key in ("char_offset", "char_end")
        )
        stored_spans[chunk_id] = (span_start, span_end)
    _charge(budget, [o.text for o in occurrences] + ([own.text] if own is not None else []))
    occurrences.sort(key=lambda o: (
        o.stream_id is None, o.stream_id or "", o.char_offset if o.char_offset is not None else -1,
        o.row.get("chunk_ordinal") if o.row.get("chunk_ordinal") is not None else 1 << 62,
        o.chunk_id,
    ))
    # Issue #3802: the head a window chunker repeats from the occurrence
    # before, proven from the stored spans and the texts themselves; a match
    # inside it that the occurrence before holds at the same place is
    # counted once, there (`plan_record`). "Before" is the previous
    # occurrence with an exact stored span, by offset then id: the order the
    # text condition's SQL pairs passages in (`text_match._with_heads`).
    spanned = sorted(
        (
            (start, o.chunk_id, index)
            for index, o in enumerate(occurrences)
            for start, end in [stored_spans[o.chunk_id]]
            if start is not None and end is not None and 0 <= start <= end
            and end - start == len(o.text)
        ),
    )
    for (before_start, _, before), (at_start, _, at) in zip(spanned, spanned[1:]):
        previous, current = occurrences[before], occurrences[at]
        head = repeated_head(
            previous.text, *stored_spans[previous.chunk_id],
            current.text, *stored_spans[current.chunk_id],
        )
        if head:
            occurrences[at] = replace(
                current, repeated_head=head, repeats=previous.chunk_id,
                repeat_shift=at_start - before_start,
            )
    return _RecordRead(record, own, occurrences, masked, "declared",
                       bases, complete, chunk_texts, sources)


def continuity_state(chains: Sequence[TextChain]) -> str:
    """`single`, `proven` or `unknown` for one record's passage chains.

    Proven means every readable passage of one producer stream joined into
    one chain: contiguous, or across verified overlap or a stored separator.
    A gap between two readable passages (text stored once for a repeated
    paragraph, a hidden passage, or anything else missing) leaves the text
    that crossed it unread, so a term's absence cannot be decided there and
    the verdict is unknown; so is an unpositioned passage among several, or
    a conflicting overlap. Separate streams never adjoin.

    One readable piece is `single` whatever its position authority. The
    partial-coverage rule in `plan_record` needs positions, so for a
    producer without position authority a record whose other passage is
    hidden (or never stored) still reads as `single` and decides absence over
    the one readable passage: hidden equals missing, but neither is seen as
    a gap.
    """
    pieces = sum(len(chain.occurrences) for chain in chains)
    if pieces <= 1:
        return "single"
    if any(chain.stream_id is None for chain in chains):
        return "unknown"
    streams = [str(chain.stream_id) for chain in chains]
    if len(streams) != len(set(streams)):
        return "unknown"
    return "proven"


# ── planning ──────────────────────────────────────────────────────────────


def _match_value(chain_index: int, chain: TextChain, event: Any, term: GrepTerm) -> dict[str, Any]:
    if event.end > event.start:
        located = window_fragments(chain, event.start, event.end)
    else:
        located = []
        for piece in chain.pieces:
            if piece.start <= event.start <= piece.end and piece.chunk_start is not None:
                at = piece.chunk_start + event.start - piece.start
                located = [{"kind": "chunk", "chunk_id": piece.occurrence.chunk_id, "range": [at, at]}]
                break
    # The id names chain, term index and chain range; `source` maps the
    # match to original chunk-local ranges (a separator piece says so).
    return {
        "match_id": f"m:{chain_index}:{event.term}:{event.start}:{event.end}",
        "term": event.term,
        "source": [
            {key: item[key] for key in ("chunk_id", "range", "line_range") if key in item}
            if item.get("kind") == "chunk" else {"separator": item.get("edge_id")}
            for item in located
        ],
    }


def plan_record(
    read: _RecordRead,
    *,
    terms: Sequence[GrepTerm],
    matching: str,
    context_chars: int,
    scan: Callable[[list[str]], Any],
    hard_refusal: bool = True,
) -> tuple[Optional[PlannedRecord], dict[str, int]]:
    """The record's planned windows when it matches, else None.

    Under ``hard_refusal=False`` (a multi-record scope) a record whose
    membership is undecidable returns an undecided header with no windows
    instead of refusing the whole call.
    """
    record_id = str(read.record["node_id"])
    own_chains = build_chains([read.own]) if read.own is not None else []
    if matching == "passage":
        chains = own_chains + [build_chains([o])[0] for o in read.occurrences]
        continuity = "not_needed"
    else:
        # Unpositioned non-text chunks (tables, image captions) are searched
        # alone; they never join the text stream nor decide its continuity.
        aside = [o for o in read.occurrences if o.stream_id is None and o.row.get("non_text")]
        stream = [o for o in read.occurrences if not (o.stream_id is None and o.row.get("non_text"))]
        passage_chains = build_chains(stream) if stream else []
        chains = own_chains + passage_chains + [build_chains([o])[0] for o in aside]
        continuity = continuity_state(passage_chains)
        positioned = [c for c in passage_chains if c.stream_id is not None]
        if positioned and source_coverage(positioned) != "complete":
            # Positioned passages that do not tile the source (text missing
            # or hidden before the first, between, or after the last) leave
            # unread text a term could cross: a hidden passage and a missing
            # one read the same, and neither proves absence.
            continuity = "unknown"
    result = scan([chain.text for chain in chains])
    work = {"source_bytes": result.source_bytes, "match_events": result.match_events}
    if matching != "passage":
        # Issue #3802: a chain that starts with a window chunker's repeated
        # head (an occurrence that did not join the chain before it) can
        # hold the previous occurrence's matches again there. A match is
        # taken back only when the occurrence it repeats has an event of
        # the same term at the same place in the source; any other match
        # there (a `^` at this text's start, which the occurrence before
        # reads mid-text) is this occurrence's own. Passage matching keeps
        # every passage's own evidence whole.
        located: set[tuple[str, int, int, int]] = set()
        for chain, events in zip(chains, result.events):
            for event in events:
                for piece in chain.pieces:
                    if (
                        piece.chunk_start is not None
                        and piece.start <= event.start and event.end <= piece.end
                    ):
                        at = piece.chunk_start + event.start - piece.start
                        located.add((
                            piece.occurrence.chunk_id, event.term, at,
                            at + event.end - event.start,
                        ))
                        break

        def repeated(chain: TextChain, event: Any) -> bool:
            first = chain.occurrences[0]
            shift = first.repeat_shift
            return (
                first.repeats is not None and event.end <= first.repeated_head
                and (first.repeats, event.term, event.start + shift, event.end + shift)
                in located
            )

        result = replace(result, events=tuple(
            tuple(e for e in events if not repeated(chain, e))
            if chain.occurrences and chain.pieces and chain.pieces[0].chunk_start == 0
            and chain.occurrences[0].repeated_head > 0 else events
            for chain, events in zip(chains, result.events)
        ))
    if matching == "passage":
        selected = [
            index for index, events in enumerate(result.events)
            if record_matches(terms, [events], occurrences_complete=True, continuity_complete=True)
        ]
        if not read.complete:
            # Raises window_work_incomplete: absence needs every occurrence.
            record_matches(terms, result.events, occurrences_complete=False,
                           continuity_complete=True)
        if not selected:
            return None, work
    else:
        try:
            matched = record_matches(
                terms, result.events, occurrences_complete=read.complete,
                continuity_complete=continuity != "unknown",
            )
        except WindowContinuityUnknown as exc:
            data = dict(exc.data or {})
            if hard_refusal:
                raise WindowContinuityUnknown(
                    str(exc), data={**data, "record_id": record_id, "continuity": continuity},
                    fix_hint="use matching='passage' for explicitly passage-local evidence",
                ) from exc
            return PlannedRecord(record_id, _row_header({
                "node_id": record_id,
                "subtype": read.record.get("subtype"),
                "decided": False,
                "continuity": continuity,
                "refusal": {
                    "code": WindowContinuityUnknown.code,
                    "term_index": data.get("term_index"),
                    "term_kind": data.get("term_kind"),
                },
            }), ()), work
        if not matched:
            return None, work
        selected = list(range(len(chains)))
    windows: list[PlannedWindow] = []
    match_count = 0
    for index in selected:
        chain = chains[index]
        events = [e for e in result.events[index] if not terms[e.term].negated]
        for window in merge_windows(chain.text_length, events, context_chars=context_chars):
            fragments = window_fragments(chain, window.start, window.end)
            shown = {(f.get("chunk_id"), tuple(f.get("range") or ())) for f in fragments}
            aliases = [
                {"chunk_id": loc["chunk_id"], "edge_id": loc["edge_id"], "range": loc["range"]}
                for loc in supporting_locations(chain, window.start, window.end)
                if (loc["chunk_id"], tuple(loc["range"])) not in shown
            ]
            parts = [WindowPart("match", _match_value(index, chain, e, terms[e.term]))
                     for e in window.matches]
            parts += [WindowPart("location", alias) for alias in aliases]
            for fragment in fragments:
                value = {key: fragment[key] for key in
                         ("kind", "chunk_id", "edge_id", "range", "line_range", "chain_range")
                         if key in fragment}
                if not fragment["text"] and fragment.get("kind") != "chunk":
                    continue
                parts.append(WindowPart("fragment", value, fragment["text"]))
            if not fragments:
                parts.append(WindowPart("fragment", {"kind": "range_only",
                                                     "chain_range": [window.start, window.end]}, ""))
            match_count += len(window.matches)
            windows.append(PlannedWindow(
                identity=f"w:{index}:{window.start}:{window.end}",
                header={
                    "chain_range": [window.start, window.end],
                    "match_count": len(window.matches),
                    "positioned": chain.stream_id is not None,
                },
                parts=tuple(parts),
            ))
    header: dict[str, Any] = {
        "node_id": record_id,
        "subtype": read.record.get("subtype"),
        "continuity": continuity,
        "source_coverage": source_coverage([c for c in chains if c.stream_id is not None])
        if any(c.stream_id is not None for c in chains) else "unknown",
        "match_count": match_count,
        "window_count": len(windows),
        "readable_occurrences": len(read.occurrences) + (1 if read.own else 0),
    }
    if read.masked:
        header["masked_occurrences"] = read.masked
    if read.passages != "declared":
        header["passages"] = read.passages
    if read.position_bases:
        header["position_basis"] = dict(sorted(read.position_bases.items()))
    return PlannedRecord(record_id, _row_header(header), tuple(windows)), work


# ── the read ──────────────────────────────────────────────────────────────


#: The window page key naming, per scoped record whose content sits in a
#: document (a source file or code symbol), that document (issue #3467).
TEXT_ELSEWHERE_KEY = "text_elsewhere"


def _text_elsewhere(
    conn: psycopg.Connection, session: Session, reads: Sequence[_RecordRead],
    *, deadline: float,
) -> list[dict[str, str]]:
    """`[{record_id, document_id}]`, in scope order, for each read record whose
    subtype declares no passage relation and whose content a readable
    document holds (`tools.text_twin_document`: one that contains it, is
    derived from it, or is derived from the file that contains it). Its own
    text, if any, is a path or a name and is still searched. Each lookup
    stops at the first document with chunks, under the call's deadline."""
    from okg.substrate.mcp.tools import text_twin_document

    named: list[dict[str, str]] = []
    for read in reads:
        if read.passages != "undeclared" or read.occurrences or read.masked:
            continue
        _check_deadline(deadline)
        record_id = str(read.record["node_id"])
        document = text_twin_document(conn, session, record_id)
        if document is not None and document != record_id:
            named.append({"record_id": record_id, "document_id": document})
    return named


def search_record_windows(
    conn: psycopg.Connection,
    session: Session,
    query: str,
    parsed: GrepQuery,
    *,
    record_ids: Sequence[str],
    matching: str,
    context_chars: int,
    limit: int,
    max_bytes: int,
    measure: Callable[[dict[str, Any]], int],
    continuation_token: Optional[str] = None,
    producers: Optional[frozenset[str]] = None,
    reserve_bytes: int = 0,
) -> dict[str, Any]:
    """Plan every scoped record's windows, then serve one byte-fitted page.

    ``measure`` returns the page's served bytes; ``reserve_bytes`` is the
    measured size of what the server stamps on after it, which this read
    turns into the reserve kept free below ``max_bytes`` (`page_reserve`).
    """
    from okg.substrate.mcp.record_text import _checkout
    from okg.substrate.mcp.search_where import compile_where

    assert_window_query(parsed)
    if producers is None:
        producers = deployment_position_producers(session.deployment_name)
    ids = list(record_ids)
    projection = {
        "query": query, "records": ids, "matching": matching,
        "context_chars": context_chars, "limit": limit, "max_bytes": max_bytes,
    }
    binding = continuation_binding(
        conn, session, kind="record_windows", sort="record_window_source_order",
        projection=projection,
    )
    context = make_scan_context(
        binding, query=query, scope={"records": ids},
        compiled=compile_where(None, columns={"id": "node_id"}),
        source="record_windows", result_unit="record",
        membership_target="scoped_record", max_bytes=max_bytes,
        ordering_identity="record_window_source_order",
    )
    # A token is verified before any record is read: signature, the whole
    # binding and the scanner's state bound.
    # One wall-clock bound for the whole call's reads and scans.
    call_deadline = time.monotonic() + MAX_WINDOW_CALL_SECONDS
    resumed: dict[str, Any] = {}
    token_reserve: Optional[int] = None
    if continuation_token is not None:
        resumed = consume_scan_continuation(context, continuation_token)
        if set(resumed) != {"position", "reserve"} or _int(resumed["reserve"]) is None:
            raise ContinuationTokenInvalid("invalid record-window cursor fields")
        token_reserve = resumed["reserve"]
    graph = _checkout(conn, session)
    budget = [MAX_WINDOW_OCCURRENCES, MAX_SOURCE_BYTES, MAX_READABLE_OCCURRENCE_ROWS]
    terms = parsed.terms
    reads: list[_RecordRead] = []
    # Issue #3467: a record named in scope is read whatever the default
    # search profile holds; the caller chose it, and `profile` is refused
    # here, so a profile filter would leave no way to read it.
    for record_id in ids:
        _check_deadline(call_deadline)
        read = read_record(conn, session, graph, record_id, context=context,
                           producers=producers, budget=budget)
        if read is None:
            continue
        reads.append(read)
    # Issue #3467: a code record's own text is its path or name; its content
    # is in a document. Name that document rather than answer "no matches"
    # as if the content had been searched. `text_twin_document` reads through
    # the read filter, so a hidden document is never named.
    text_elsewhere = _text_elsewhere(conn, session, reads, deadline=call_deadline)
    texts_total = sum(len(r.occurrences) + (1 if r.own else 0) for r in reads)
    # Readable work only: no count here moves with passages the reader
    # cannot read.
    work = {"source_bytes": 0, "match_events": 0, "readable_occurrences": texts_total}
    remaining_events = [10000]
    # One scanning deadline for the whole call, shared across its records.
    deadline = time.monotonic() + 2.0

    def scan(texts: list[str]) -> Any:
        if remaining_events[0] < 1:
            raise WindowWorkBound("window match event bound reached",
                                  data={"bound": "match_events", "max_events": 10000},
                                  fix_hint="narrow the query or record scope")
        _check_deadline(call_deadline)
        remaining_ms = int((min(deadline, call_deadline) - time.monotonic()) * 1000)
        if remaining_ms < 1:
            raise WindowWorkBound("window source time bound reached",
                                  data={"bound": "source_time"},
                                  fix_hint="narrow the record scope or query")
        result = scan_chain_matches(conn, terms, texts, max_events=remaining_events[0],
                                    timeout_ms=min(remaining_ms, 2000))
        remaining_events[0] -= result.match_events
        return result

    planned: list[PlannedRecord] = []
    undecided: list[str] = []
    # One named record keeps the hard refusal; a multi-record scope reports
    # each undecidable record instead of refusing the others with it.
    hard_refusal = len(ids) == 1
    for read in reads:
        record, record_work = plan_record(read, terms=terms, matching=matching,
                                          context_chars=context_chars, scan=scan,
                                          hard_refusal=hard_refusal)
        work["source_bytes"] += record_work["source_bytes"]
        work["match_events"] += record_work["match_events"]
        if record is not None:
            planned.append(record)
            if record.header.get("decided") is False:
                undecided.append(record.identity)
    trust = classify_sources(
        conn, deployment_name=session.deployment_name,
        sources={src for read in reads for src in read.sources.values()}
        | {read.record.get("source") for read in reads},
    )
    texts = {cid: text for read in reads for cid, text in read.chunk_texts.items()}
    sources = {cid: src for read in reads for cid, src in read.sources.items()}
    for read in reads:
        if read.own is not None:
            texts[str(read.record["node_id"])] = read.own.text
            sources[str(read.record["node_id"])] = read.record.get("source")
    origins = {
        o.chunk_id: o.row.get(UNTRUSTED_CONTENT_ORIGIN_ATTR)
        for read in reads
        for o in [*read.occurrences, *([read.own] if read.own is not None else [])]
    }

    def render_fragment(part: WindowPart, start: int, end: int) -> dict[str, Any]:
        value = dict(part.value)
        text = (part.text or "")[start:end]
        chunk_id = value.get("chunk_id")
        if value.get("kind") == "chunk" and isinstance(value.get("range"), list):
            lo = value["range"][0] + start
            hi = lo + len(text)
            value["range"] = [lo, hi]
            whole = texts.get(str(chunk_id), "")
            value["line_range"] = [
                1 + whole.count("\n", 0, lo), 1 + whole.count("\n", 0, max(lo, hi - 1)),
            ]
        if isinstance(value.get("chain_range"), list) and part.text:
            base = value["chain_range"][0]
            value["chain_range"] = [base + start, base + end]
        attrs: dict[str, Any] = {"text": text}
        origin = origins.get(str(chunk_id))
        if origin is not None:
            attrs[UNTRUSTED_CONTENT_ORIGIN_ATTR] = origin
        source = sources.get(str(chunk_id))
        served = serialize_node_read_row(
            {"node_id": chunk_id, "source": source, "attrs": attrs},
            trust=trust.get(source, "untrusted"),
        )
        value["text"] = served["attrs"]["text"]
        value["provenance"] = served["provenance"]
        return value

    total_windows = sum(len(r.windows) for r in planned)
    total_matches = sum(int(r.header.get("match_count", 0)) for r in planned)
    envelope: dict[str, Any] = {
        "query": query,
        "method": "grep",
        "view": WINDOW_VIEW,
        "matching": matching,
        "context_chars": context_chars,
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        "result_unit": "record",
        "rank_by": "records",
        "scope_applied": {"records": ids},
        # Decided matching records. With an undecided record the true count
        # is at least this, never exactly it.
        "total_count": len(planned) - len(undecided),
        "total_count_basis": "lower_bound" if undecided else "exact",
        # Merged windows and match events, never rendered fragments or pages.
        "window_count": total_windows,
        "match_count": total_matches,
        "evidence_domain": (
            "readable_passages" if matching == "passage" else "readable_record_occurrences"
        ),
        "work": work,
        # The selection qualifier lives in `coverage` only (PACT
        # operator-page-counts-3190): an exact size is `total_count` itself
        # and is omitted; an unproved one is null. Delivery is recomputed
        # per page by the envelope.
        "coverage": {
            "retrieval": (
                {"status": "bounded", "basis": "record_scope_occurrence_scan"}
                if undecided else
                {"status": "exhaustive", "basis": "record_scope_occurrence_scan"}
            ),
            "selection": (
                {"unit": "record", "basis": "matching_records", "size": None}
                if undecided else {"unit": "record", "basis": "matching_records"}
            ),
        },
    }
    if undecided:
        # At most the scope's 100 ids.
        envelope["undecided_records"] = undecided
    if text_elsewhere:
        envelope[TEXT_ELSEWHERE_KEY] = text_elsewhere

    # The page's reserve for per-call stamps travels in the token, so a
    # retried token cuts its page at the same points (`page_reserve`).
    reserve = page_reserve(reserve_bytes, token_reserve, max_bytes)

    def issue(cursor: dict[str, Any], measure_token: Callable[[str], int]) -> str:
        try:
            return issue_scan_continuation(
                context, {**cursor, "reserve": reserve}, measure=measure_token,
            )
        except ScanContinuationUnavailable as exc:
            if exc.reason == "output_bound":
                # This trial page is too large with its token; page smaller.
                raise ContinuationTooLarge(exc.reason) from exc
            raise WindowBudgetTooSmall(
                "a usable window continuation does not fit",
                data={"max_bytes": max_bytes, "reason": exc.reason},
                fix_hint="increase max_bytes",
            ) from exc

    page = page_windows(
        planned, envelope=envelope, binding=context.binding, limit=limit,
        max_bytes=max_bytes, reserve_bytes=reserve,
        measure=measure,
        render_fragment=render_fragment, continuation_token=continuation_token,
        issue_token=issue,
        consume_token=lambda _token: {"position": resumed["position"]},
    )
    return page
