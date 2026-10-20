"""Record reads for the operator UI (PACT operator-ui-search-view).

These composers have no shipped consumer yet: the Search view they were
written for is superseded by the console redesign (PR #1988), and no console
page is wired to them until that redesign lands.

Every composer here receives a session the caller pinned to one published
version and that names the operator UI's read surface
(``graph.read.local_operator``). Rows pass through the same Nomos filter as
``okg search``, with the same row shape the MCP read tools hand it (attributes,
source, source record ID and source revision), and text search IS the shared
``okg search`` lexical search, not a second implementation, so the UI, the CLI
and MCP return the same hits for the same query.

A value the source policy hid, whether masked at import or removed by a
read-time redaction, is reported as hidden, never shown and never dropped. A
record the reader cannot see, or that is absent from the version, is reported
as not available without saying which, and no response names a record the
reader cannot see: listing pages are addressed by position, not by record ID.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Iterable

from okg.substrate.attributes import apply_effective_attrs_to_rows
from okg.substrate.console_views._chronos import checkout
from okg.substrate.console_views.attempts_by_day import (
    parse_checkpoint_created_at,
)
from okg.substrate.console_views.console_count_rollup_read import (
    COUNT_KIND_SUBTYPE,
    console_count_rollup_enabled,
    read_console_count_rollup,
)
from okg.substrate.console_views.producer_identity import source_display_names
from okg.substrate.console_views.record_layout import (
    GENERIC_BODY_ATTR_KEYS,
    PACT_BODY_ATTR_KEYS,
    body_sections_for,
    format_attrs,
    label_for,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.mcp.provenance import FENCE_CLOSE, is_fenced
from okg.substrate.mcp.search import record_identity
from okg.substrate.mcp.session import Session
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
)
from okg.substrate.nomos.read_masking import (
    GOVERNANCE_KEYS,
    MASK_TOKEN,
    REDACTION_MARKER,
    ReadRedactedAttrs,
)
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
from okg.substrate.searchable_text import PRODUCER_TEXT_ATTR, without_producer_text

SEARCH_LIMIT_MAX = 50
LIST_LIMIT_MAX = 100
VERSION_LIMIT_MAX = 50
RELATED_CAP = 40
# A listing page or a related-records list refills after the policy hides
# rows, but never scans without bound: after this many rounds it returns what
# it found and says more may follow.
_LIST_REFILL_ROUNDS = 5
_RELATED_REFILL_ROUNDS = 5
# A chunk page refills after the policy hides rows, on the same bounded terms.
_CHUNK_REFILL_ROUNDS = 5

_NODE_COLUMNS = "node_id, subtype, attrs, source, source_record_id, source_revision"

_ESCAPED_FENCE_CLOSE = "⟦ESCAPED_/UNTRUSTED_DATA⟧"
_ESCAPED_FENCE_PREFIX = "⟦ESCAPED_UNTRUSTED_DATA"
_FENCE_PREFIX = "⟦UNTRUSTED_DATA"


def _unfence(value: Any) -> Any:
    """The producer text inside an untrusted-data fence, for display.

    The shared search fences untrusted text so an agent cannot mistake it for
    instructions. A browser renders it as inert text, so the response carries
    the text itself, with the trust level in its own field.
    """
    if isinstance(value, dict):
        return {key: _unfence(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_unfence(item) for item in value]
    if not isinstance(value, str) or not is_fenced(value):
        return value
    stripped = value.rstrip()
    first_newline = stripped.find("\n")
    body = stripped[first_newline + 1: len(stripped) - len(FENCE_CLOSE)]
    if body.endswith("\n"):
        body = body[:-1]
    return (
        body
        .replace(_ESCAPED_FENCE_PREFIX, _FENCE_PREFIX)
        .replace(_ESCAPED_FENCE_CLOSE, FENCE_CLOSE)
    )


def _is_hidden(value: Any) -> bool:
    return isinstance(value, str) and value.strip() == MASK_TOKEN


def _display_label(node_id: str, subtype: str, attrs: Any) -> str:
    """A record's label, never built from a policy-hidden value or a full path.

    The label is the one field EVERY record read emits -- search hits, listing
    rows, related records, the detail heading -- so a source whose title is the
    file it read would put the operator's home directory on every one of them.
    Masked here, once, rather than at each of those call sites.
    """
    visible = {
        key: value
        for key, value in (attrs or {}).items()
        if not _is_hidden(value)
    } if isinstance(attrs, dict) else {}
    label = label_for(node_id, subtype, visible)
    return _basename(label) if isinstance(label, str) and _looks_absolute(label) else label


def _informative_excerpt(
    excerpt: Any, label: str, attrs: Any,
) -> str | None:
    """The excerpt, or None when it tells the reader nothing new.

    A record whose source carries no prose indexes as its own identity: a CMSSW
    release indexes as ``CMSSW_0_6_X\\n\\nrelease_family`` -- its name, then the
    value of its release_type field. Rendered under a row that already shows
    the label and the type, that is a third line restating the first two.

    An excerpt survives when ANY of its words is something the record does not
    already show: not a word of the label, of an attribute key, or of an
    attribute value. An index often restates an identifier in tokenized form --
    a path indexed again as its own words -- and that clears no bar.

    The one value NOT counted as already shown is the one the excerpt was drawn
    from. An excerpt is a window into some body of text, so counting that text
    would suppress every real excerpt there is.
    """
    if not isinstance(excerpt, str):
        return None
    text = excerpt.strip()
    if not text:
        return None

    def words(value: str) -> set[str]:
        return {part for part in re.split(r"[^0-9a-z]+", value.casefold()) if part}

    # _bounded_excerpt marks a window it cut out of a longer value.
    core = " ".join(text.strip(".").split()).casefold()
    known = words(label)
    if isinstance(attrs, dict):
        for key, value in attrs.items():
            known |= words(str(key))
            rendered = " ".join(str(value).split())
            if core and core in rendered.casefold():
                continue
            known |= words(rendered)
    return text if words(text) - known else None


def _source_identity(row: dict[str, Any]) -> dict[str, Any]:
    """The row's source evidence, UNMASKED -- for callers that mask it themselves.

    Only `compose_record_detail` uses this directly, because it collects the
    masked key names for its own `masked_paths`. Every other emitter goes
    through `emit_identity` below.
    """
    return {
        "source": row.get("source"),
        "source_record_id": record_identity(row.get("source_record_id")),
        "source_revision": record_identity(row.get("source_revision")),
    }


def emit_identity(row: dict[str, Any], *, at: str = "") -> tuple[dict[str, Any], list[str]]:
    """The source evidence a response may carry, with paths masked.

    ONE helper, because the identity is emitted from five places -- the detail
    read, the raw row, each raw edge, an exact search hit and a text search
    hit -- and the search hits used to emit it raw. A source that records its
    record id as the file it read then showed `guide.md` on the record's page
    and the operator's full home directory in the search row for the SAME
    record.
    """
    return mask_paths(_source_identity(row), at=at)


def _source_names(session: Session) -> tuple[dict[str, str], str | None]:
    """The deployment's ownership id -> declared source name map, and why not.

    Read once per request. Every record read carries a row's ``source`` as
    the ownership id (``cern-r3final.twiki-eos``) and Sources names the same
    producer ``twiki_eos``; the reads now serve that declared name beside the
    id as ``source_name`` (null for an id no registry entry declares), and
    ``source_names_unavailable_reason`` when no name could be read at all.
    """
    return source_display_names(session.deployment_name)


def _visible_nodes(
    conn: Any,
    session: Session,
    rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Node rows the reader may see, as MCP returns them.

    The policy filter applies accepted attribute corrections only when the
    policy is on, so the conventional overlay pass runs after it either way:
    it is idempotent for rows the filter already corrected and leaves a
    read-time redaction untouched.
    """
    return _with_corrections(conn, session, filter_node_rows_for_session(conn, session, rows))


def _with_corrections(
    conn: Any,
    session: Session,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return apply_effective_attrs_to_rows(
        conn,
        rows,
        target_kind="node",
        target_id_field="node_id",
        generation_id=session.generation_id,
    )


def _summary_row(row: dict[str, Any]) -> dict[str, Any]:
    node_id = str(row["node_id"])
    subtype = str(row["subtype"])
    return {
        "node_id": node_id,
        "type": subtype,
        "label": _display_label(node_id, subtype, row.get("attrs")),
        "source": row.get("source"),
    }


# The checkpoint metadata this composer reads. Named here, and asserted
# against this function's source by a guard test, so the bounded reader can
# project these keys and leave the rest -- including the checkpoint's
# multi-megabyte `blocked_reason` -- in the database.
VERSION_METADATA_KEYS = ("status", "catalog_version_id")


def _published_at_iso(created_at: Any) -> str | None:
    """A checkpoint's `created_at` as an ISO-8601 string, or None.

    The version list used to read this column through the `okg.v_generations`
    view, which cast it to `timestamptz`; the bounded reader hands back the
    raw column, which is text on the interval backend and a datetime on a
    backend that types it. Both end as the same ISO string the console has
    always been served -- and an unparseable stamp is None, never a guess.
    """
    if created_at is None:
        return None
    if isinstance(created_at, datetime):
        return created_at.isoformat()
    parsed = parse_checkpoint_created_at(created_at)
    return parsed.isoformat() if parsed is not None else None


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def compose_published_versions(
    conn: Any,
    *,
    deployment_name: str,
    branch_name: str,
    limit: int = VERSION_LIMIT_MAX,
) -> dict[str, Any]:
    """The deployment's published versions on one branch, newest first.

    Read through the graph store's BOUNDED generation reader rather than the
    `okg.v_generations` view. The view is defined as
    `(metadata::jsonb) ->> 'kind' = 'okg_generation' AND ... ->> 'status' =
    'published'`, which parses every checkpoint's whole metadata document
    before the LIMIT can discard any of it: 2.7 s on okg-workspace for a
    42-row answer (issue #2202). The reader scans a bounded window of the
    newest checkpoints and parses each scanned document once.

    The served fields are unchanged and come from the same places the view
    read them: `generation_id` is the checkpoint id, `published_at` is the
    checkpoint's `created_at` column, and `catalog_version_id` is the
    metadata key of that name.

    `scanned` and `scan_window` are served because `truncated` alone cannot
    distinguish "there are more versions than the cap" from "this read did
    not look past its window".
    """
    capped = max(1, min(int(limit), VERSION_LIMIT_MAX))
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    # The window has to be able to hold `capped + 1` PUBLISHED versions with
    # blocked attempts interleaved between them, so it is a multiple of the
    # cap rather than the cap itself, and the result states what it scanned.
    recent = store.recent_generations(
        deployment_name=deployment_name,
        branch=branch_name,
        status="published",
        keys=VERSION_METADATA_KEYS,
        scan_window=min(200, max(64, (capped + 1) * 4)),
    )
    rows = recent.rows
    versions = [
        {
            "generation_id": row.checkpoint_id,
            "published_at": _published_at_iso(row.created_at),
            "catalog_version_id": _int_or_none(
                row.metadata.get("catalog_version_id")
            ),
        }
        for row in rows[:capped]
    ]
    return {
        "deployment": deployment_name,
        "branch": branch_name,
        "versions": versions,
        "latest": versions[0]["generation_id"] if versions else None,
        "truncated": len(rows) > capped or recent.window_exhausted,
        # What this bounded read actually looked at. `truncated` says "there
        # is more"; these say how far back the read reached before saying so.
        "scanned": recent.scanned,
        "scan_window": recent.scan_window,
        "window_exhausted": recent.window_exhausted,
    }


def compose_record_types(conn: Any, *, session: Session) -> dict[str, Any]:
    """Record counts per type in the pinned version.

    Counts are totals in the published version before access rules; a reader
    the policy restricts can see fewer records when listing a type.
    """
    if console_count_rollup_enabled():
        counts = read_console_count_rollup(
            conn, generation_id=session.generation_id, kinds={COUNT_KIND_SUBTYPE},
        )
        by_type = {
            str(key): int(value)
            for (kind, key), value in counts.items()
            if kind == COUNT_KIND_SUBTYPE
        }
    else:
        # The scan computes the same totals the rollup holds, so a count stays
        # true with the rollup off rather than degrading to a default.
        graph = checkout(conn, session)
        by_type = {
            str(row["subtype"]): int(row["c"] or 0)
            for row in graph.query(
                # derivation-read-lint: allow #444 flag-off subtype rollup fallback.
                "SELECT subtype, count(*) AS c FROM okg.graph_nodes GROUP BY subtype",
                {},
            )
        }
    types = [
        {"type": name, "count": count}
        for name, count in sorted(by_type.items(), key=lambda item: (-item[1], item[0]))
        if count > 0
    ]
    text_types, text_types_reason = _text_bearing_types(conn, session=session)
    return {
        "types": types,
        "count_basis": "published_total_before_access_rules",
        # Which record types hold text that typing searches, or null with the
        # reason no read could say. Explore's landing named none of them.
        "text_types": text_types,
        "text_types_unavailable_reason": text_types_reason,
    }


#: The record type text is stored in, and the two ways a record is joined to
#: it. The same literals `_CHUNK_EDGE_JOIN` reads a record's content through.
_TEXT_SUBTYPE = "document_chunk"


def _text_bearing_types(
    conn: Any, *, session: Session,
) -> tuple[list[str] | None, str | None]:
    """The record types whose text is stored, from the ontology rollup.

    Text search matches the text of `document_chunk` records, and a record's
    content is the chunks joined to it by `contains` (record -> chunk) or
    `member_of` (chunk -> record). The maintained ontology rollup holds every
    realised `(src_subtype, edge_type, dst_subtype)` at this version, so the
    types on the far side of those two edges are the types that hold text.
    One maintained read of a few hundred rows; no graph scan. When the rollup
    is not maintained for this version the answer is null with its reason,
    never an empty list that would read as "no type stores text".
    """
    from okg.substrate.console_views.ontology_rollup_read import (
        OntologyRollupUnavailable,
        read_ontology_rollup,
    )

    try:
        rollup = read_ontology_rollup(
            conn,
            deployment_name=session.deployment_name,
            generation_id=session.generation_id,
        )
    except OntologyRollupUnavailable as exc:
        return None, f"ontology_rollup_unavailable:{exc.reason}"
    found: set[str] = set()
    for (src, edge, dst), count in rollup.pair_counts().items():
        if count <= 0:
            continue
        if edge == "contains" and dst == _TEXT_SUBTYPE and src != _TEXT_SUBTYPE:
            found.add(src)
        elif edge == "member_of" and src == _TEXT_SUBTYPE and dst != _TEXT_SUBTYPE:
            found.add(dst)
    return sorted(found), None


def _exact_record(
    conn: Any,
    session: Session,
    node_id: str,
    subtype: str | None,
    source: str | None = None,
    *,
    typed_ids: Iterable[str] = (),
) -> list[dict[str, Any]]:
    """The records whose id IS the query, then those whose id is `<type>:<query>`.

    Both are primary-key reads of the pinned version. Returned in that order,
    each row tagged `_exact_tier` (`record_id` or `typed_record_id`).
    """
    graph = checkout(conn, session)
    wanted = [node_id, *[candidate for candidate in typed_ids if candidate != node_id]]
    rows = graph.query(
        f"SELECT {_NODE_COLUMNS} FROM okg.graph_nodes WHERE node_id = ANY(:ids)",
        {"ids": wanted},
    )
    if subtype is not None:
        rows = [row for row in rows if str(row["subtype"]) == subtype]
    # The exact-ID tier obeys the same filters as the text tier: a record the
    # reader filtered out by producer must not reappear because they typed its
    # id. This reads rows by primary key, so the filter costs nothing.
    if source is not None:
        rows = [row for row in rows if row.get("source") == source]
    order = {candidate: position for position, candidate in enumerate(wanted)}
    visible = sorted(
        _visible_nodes(conn, session, rows),
        key=lambda row: order.get(str(row["node_id"]), len(order)),
    )
    for row in visible:
        row["_exact_tier"] = (
            "record_id" if str(row["node_id"]) == node_id else "typed_record_id"
        )
    return visible


#: How many ranked matches the search page is composed from. The shared
#: lexical search ranks at most this many (`mcp/search.py`
#: `_LEXICAL_RANK_CAP`), so asking for the whole window costs one ranked read,
#: the same as asking for one page of it, and lets this read count matches by
#: type and spread the page across types.
SEARCH_RANK_WINDOW = 200


def _catalog_types(conn: Any, session: Session) -> list[str]:
    """The record types the pinned catalog declares, for the `<type>:<q>` tier."""
    catalog_version_id = getattr(session, "catalog_version_id", None)
    if catalog_version_id is None:
        return []
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name FROM okg.subtypes WHERE catalog_version_id = %s",
            (catalog_version_id,),
        )
        return sorted(
            str(row[0] if not isinstance(row, dict) else row["name"])
            for row in cur.fetchall()
        )


def compose_record_search(
    conn: Any,
    *,
    session: Session,
    query: str,
    subtype: str | None = None,
    source: str | None = None,
    limit: int = 20,
    offset: int = 0,
    scope: Any = None,
) -> dict[str, Any]:
    """Search the pinned version: exact tiers first, then shared text search.

    ``source`` restricts the search to one producer id. It is SELECTION, not
    authority: it narrows which rows are ranked and never widens what the
    reader may see, so every row it admits still passes the same policy filter
    as an unfiltered search and a producer the reader may not see returns the
    same empty result as one that does not exist.

    Exact tiers, on the first page only, in this order (each row's ``match``):

    * ``record_id`` -- the record whose id is the query.
    * ``typed_record_id`` -- a record whose id is ``<type>:<query>`` for a type
      the catalog declares or the ranked window holds. Archiv3 names a release
      ``cmssw_release:CMSSW_13_0_0``; the query ``CMSSW_13_0_0`` ranked it
      seventh and said ``exact_match: false``.
    * ``label`` -- a ranked match whose label equals the query, ignoring case.

    The text tier is the shared lexical search's ranked window
    (`SEARCH_RANK_WINDOW` rows). When it holds more than one type, the best
    match of each type leads, in rank order, marked ``promoted: "type_leader"``,
    and the rest follow in rank order: Archiv3's ``CMSSW`` filled its page
    with twenty chunks while 2,415 releases matched. Paging walks this one
    order, so no record repeats or drops between pages. ``paged_count`` is its
    length: the text rows every page draws from, the first page included,
    with the exact rows taken out. ``type_counts`` counts
    the window's matches by type (``type_counts_basis: "ranked_window"``); when
    ``rank_capped`` is true the window is the top of a longer list and a type
    ranked below it is not counted.
    """
    from okg.substrate.mcp.search import _LEXICAL_DEFAULT, search

    q = query.strip()
    capped = max(1, min(int(limit), SEARCH_LIMIT_MAX))
    start = max(0, int(offset))
    # What was actually applied, stated so the caller can show it and detect a
    # mismatch with what it asked for rather than being quietly given another
    # result set.
    applied = {"type": subtype, "source": source}
    if not q:
        return {
            "query": query,
            "type": subtype,
            "filters": applied,
            "results": [],
            "exact_match": False,
            "total_count": 0,
            "paged_count": 0,
            "truncated": False,
            "next_offset": None,
            "rank_capped": False,
            "result_unit": None,
            "ordered_exactly_through": None,
            "next_offset_reason": None,
            "zero_hit_reason": None,
            "type_counts": {},
            "type_counts_basis": "ranked_window",
            "source_names_unavailable_reason": None,
            **_search_scope_disclosure(scope),
        }
    source_names, source_names_reason = _source_names(session)
    payload = search(
        conn,
        session,
        q,
        method="lexical",
        filter={
            key: value
            for key, value in (("subtype", subtype), ("source", source))
            if value
        } or None,
        limit=SEARCH_RANK_WINDOW,
        offset=0,
        select=[*_LEXICAL_DEFAULT, "attrs"],
        scope=scope,
    )
    window: list[dict[str, Any]] = []
    for row in payload.get("results") or []:
        node_id = str(row["node_id"])
        provenance = row.get("provenance") or {}
        subtype_name = str(row.get("subtype"))
        attrs = _unfence(row.get("attrs"))
        label = _display_label(node_id, subtype_name, attrs)
        # The read fences an untrusted row's record identity (#2689); the
        # console masks paths inside it, so it needs the producer's values.
        identity, _keys = emit_identity({
            "source": provenance.get("source"),
            "source_record_id": _unfence(row.get("source_record_id")),
            "source_revision": _unfence(row.get("source_revision")),
        })
        window.append({
            "node_id": node_id,
            "type": subtype_name,
            "label": label,
            **identity,
            "source_name": source_names.get(str(identity.get("source"))),
            "trust": provenance.get("trust"),
            "match": "text",
            "promoted": None,
            "excerpt": _informative_excerpt(_unfence(row.get("excerpt")), label, attrs),
        })
    type_counts: dict[str, int] = {}
    for hit in window:
        type_counts[hit["type"]] = type_counts.get(hit["type"], 0) + 1

    typed_ids = [
        f"{name}:{q}"
        for name in sorted(set(_catalog_types(conn, session)) | set(type_counts))
    ]
    folded = q.casefold()
    # Which ranked matches are exact is decided the same way on EVERY page,
    # without reading anything: the id is the query or `<type>:<query>`, or
    # the label is the query. Those rows leave the text order on every page,
    # so paging is one order whatever page it starts from; they are listed
    # once, on the first page, where the exact tiers lead.
    id_matches = {q, *typed_ids}
    exact_in_window = {
        hit["node_id"] for hit in window
        if hit["node_id"] in id_matches or str(hit["label"]).casefold() == folded
    }
    exact: list[dict[str, Any]] = []
    if start == 0:
        for row in _exact_record(conn, session, q, subtype, source, typed_ids=typed_ids):
            summary = _summary_row(row)
            exact.append({
                **summary, "match": row["_exact_tier"], "promoted": None,
                "excerpt": None, "trust": None, **emit_identity(row)[0],
                "source_name": source_names.get(str(row.get("source"))),
            })
        read_by_id = {row["node_id"] for row in exact}
        for hit in window:
            if hit["node_id"] not in read_by_id and str(hit["label"]).casefold() == folded:
                exact.append({**hit, "match": "label"})
    exact_in_text = bool(exact_in_window)
    ranked = [hit for hit in window if hit["node_id"] not in exact_in_window]
    ordered = ranked
    if len({hit["type"] for hit in ranked}) > 1:
        leaders: list[dict[str, Any]] = []
        seen_types: set[str] = set()
        for hit in ranked:
            if hit["type"] not in seen_types:
                seen_types.add(hit["type"])
                leaders.append({**hit, "promoted": "type_leader"})
        leader_ids = {hit["node_id"] for hit in leaders}
        ordered = leaders + [hit for hit in ranked if hit["node_id"] not in leader_ids]
    page = ordered[start: start + capped]
    truncated = start + capped < len(ordered)
    return {
        "query": q,
        "type": subtype,
        "filters": applied,
        "results": exact + page,
        "exact_match": bool(exact),
        # The shared search's own count of text matches. An exact match is
        # reported by exact_match; when it is also one of the ranked text
        # matches it is listed once, and exact_match_in_text says so.
        "exact_match_in_text": exact_in_text,
        "total_count": payload.get("total_count"),
        # How many rows the pages walk: the ranked text matches with the exact
        # rows taken out, since those are listed on the first page only.
        # `total_count` counts them too, so a page count taken from it offers
        # a last page that holds nothing (issue #2789). Page k holds rows
        # (k-1)*limit to k*limit of this order, the first page also leading
        # with the exact rows.
        "paged_count": len(ordered),
        "truncated": truncated,
        "next_offset": start + capped if truncated else None,
        "rank_capped": bool(payload.get("rank_capped")),
        # Lexical search returns one row per record (`rank_by="records"`). A
        # window filtered to a type whose records have passages ends where its
        # order stops being exact; the reason says so (PACT
        # search-result-shape, D1/D3).
        "result_unit": payload.get("result_unit"),
        "ordered_exactly_through": payload.get("ordered_exactly_through"),
        "next_offset_reason": payload.get("next_offset_reason"),
        "zero_hit_reason": payload.get("zero_hit_reason"),
        "type_counts": type_counts,
        "type_counts_basis": "ranked_window",
        "source_names_unavailable_reason": source_names_reason,
        **_search_scope_disclosure(scope),
    }


def _search_scope_disclosure(scope: Any) -> dict[str, Any]:
    """The search profile the page ran under and the types it left out.

    ``skipped_types`` lists profile types this version's catalog does not
    define (issue #2545): the page cannot hold them, and saying so is what
    keeps a narrower page from reading as a complete one. It is always
    present, empty when nothing was left out.
    """
    searched = getattr(scope, "subtypes", None)
    return {
        "search_profile": getattr(scope, "profile", None),
        # The types the ranked query was restricted to, refinements included;
        # null when the search ran unscoped.
        "searched_types": list(searched) if searched is not None else None,
        "skipped_types": list(getattr(scope, "absent_subtypes", ()) or ()),
        "skipped_types_reason": (
            "not_in_this_version_catalog"
            if getattr(scope, "absent_subtypes", ())
            else None
        ),
    }


def compose_record_list(
    conn: Any,
    *,
    session: Session,
    subtype: str,
    offset: int = 0,
    limit: int = 50,
) -> dict[str, Any]:
    """One page of a type's records in record-ID order, after the policy filter.

    A page is addressed by its position among the type's records in this
    version, counted before the policy filter, never by a record ID: a page
    that ends inside a run of hidden records must not hand the reader a hidden
    record's ID as its cursor. Positions reveal how many records a range
    holds, as MCP's paged reads do; they reveal no record.
    """
    capped = max(1, min(int(limit), LIST_LIMIT_MAX))
    graph = checkout(conn, session)
    page: list[dict[str, Any]] = []
    position = max(0, int(offset))
    for _round in range(_LIST_REFILL_ROUNDS):
        batch_start = position
        rows = graph.query(
            f"SELECT {_NODE_COLUMNS} FROM okg.graph_nodes WHERE subtype = :subtype "
            f"ORDER BY node_id LIMIT {capped + 1} OFFSET {batch_start}",
            {"subtype": subtype},
        )
        visible = {
            str(row["node_id"]): row
            for row in _visible_nodes(conn, session, rows)
        }
        for row in rows:
            if len(page) == capped:
                break
            position += 1
            shown = visible.get(str(row["node_id"]))
            if shown is not None:
                page.append(shown)
        batch_end = batch_start + len(rows)
        type_ended = len(rows) <= capped
        if len(page) == capped:
            # A visible record already checked in this batch follows the page,
            # or the type goes on past the batch; otherwise nothing follows it.
            more = any(
                str(row["node_id"]) in visible for row in rows[position - batch_start:]
            ) or (not type_ended and _row_exists_at(graph, subtype, batch_end))
            break
        if type_ended:
            more = False
            break
    else:
        more = _row_exists_at(graph, subtype, position)
    return {
        "type": subtype,
        "records": [_summary_row(row) for row in page],
        "next_offset": position if more else None,
        "short_page": more and len(page) < capped,
    }


def _row_exists_at(graph: Any, subtype: str, position: int) -> bool:
    return bool(graph.query(
        "SELECT node_id FROM okg.graph_nodes WHERE subtype = :subtype "
        f"ORDER BY node_id LIMIT 1 OFFSET {int(position)}",
        {"subtype": subtype},
    ))


def _edge_exists_at(graph: Any, node_id: str, *, near: str, far: str, position: int) -> bool:
    return bool(graph.query(
        f"SELECT e.edge_id FROM okg.graph_edges e WHERE e.{near} = :id "
        f"ORDER BY e.edge_type, e.{far}, e.edge_id LIMIT 1 OFFSET {int(position)}",
        {"id": node_id},
    ))


# Edges are limited before the join so a heavily connected record does not
# join every neighbour; the LEFT JOIN keeps an edge whose neighbour is absent
# from the version, so a batch's length is its edge count.
_RELATED_SQL = """
WITH selected_edges AS MATERIALIZED (
  SELECT e.edge_id, e.src, e.dst, e.edge_type, e.attrs, e.source,
         e.source_record_id, e.source_revision, e.{far} AS neighbor_id
    FROM okg.graph_edges e
   WHERE e.{near} = :id
   ORDER BY e.edge_type, e.{far}, e.edge_id
   LIMIT {batch} OFFSET {offset}
)
SELECT
  e.edge_id,
  e.src,
  e.dst,
  e.edge_type,
  e.attrs,
  e.source,
  e.source_record_id,
  e.source_revision,
  e.neighbor_id,
  n.subtype AS neighbor_subtype,
  n.attrs AS neighbor_attrs,
  n.source AS neighbor_source,
  n.source_record_id AS neighbor_source_record_id,
  n.source_revision AS neighbor_source_revision
FROM selected_edges e
LEFT JOIN okg.graph_nodes n ON n.node_id = e.neighbor_id
ORDER BY e.edge_type, e.neighbor_id, e.edge_id
"""

_EDGE_ROW_KEYS = (
    "edge_id", "src", "dst", "edge_type", "attrs",
    "source", "source_record_id", "source_revision",
)


def _related(
    conn: Any,
    session: Session,
    graph: Any,
    node_id: str,
    *,
    direction: str,
) -> tuple[list[dict[str, Any]], bool, list[str]]:
    """Related records in one direction, both the edge and the neighbour visible.

    Returns the visible related records, at most ``RELATED_CAP``, whether more
    relationships may follow them, and the identity trails masking rewrote.

    Each row carries the neighbour's own ``source_record_id`` -- the SAME
    identity the search read serves for the same record -- because the label
    is the only other name a row has and a chunk's label is its hash. A
    document's Related tab listed its two chunks as "Document Chunk -
    5fe6bef91614aa4d" and "Document Chunk - eb96b0ba40f4f8f8", which names
    neither the file they are in nor which part of it each one is.

    It costs no statement: the neighbour join already selects the column.
    A neighbour whose source recorded no identity carries no key at all --
    `record_identity` normalizes the empty JSONB object to None, and an empty
    container on the row would read as an identity the client can use.
    """
    near, far = ("src", "dst") if direction == "outgoing" else ("dst", "src")
    related: list[dict[str, Any]] = []
    masked_keys: list[str] = []
    position = 0
    for _round in range(_RELATED_REFILL_ROUNDS):
        batch_start = position
        rows = graph.query(
            _RELATED_SQL.format(
                near=near, far=far, batch=RELATED_CAP + 1, offset=batch_start,
            ),
            {"id": node_id},
        )
        visible_edge_ids = {
            str(edge["edge_id"])
            for edge in filter_edge_rows_for_session(
                conn,
                session,
                [{key: row[key] for key in _EDGE_ROW_KEYS} for row in rows],
            )
        }
        neighbors = _dedupe_nodes(
            {
                "node_id": row["neighbor_id"],
                "subtype": row["neighbor_subtype"],
                "attrs": without_producer_text(row["neighbor_attrs"]),
                "source": row["neighbor_source"],
                "source_record_id": row["neighbor_source_record_id"],
                "source_revision": row["neighbor_source_revision"],
            }
            for row in rows
            if str(row["edge_id"]) in visible_edge_ids
            and row["neighbor_subtype"] is not None
        )
        visible_neighbors = {
            str(row["node_id"]): row
            for row in _visible_nodes(conn, session, neighbors)
        }

        def shown(row: dict[str, Any]) -> bool:
            return (
                str(row["edge_id"]) in visible_edge_ids
                and str(row["neighbor_id"]) in visible_neighbors
            )

        for row in rows:
            if len(related) == RELATED_CAP:
                break
            position += 1
            if not shown(row):
                continue
            neighbor = visible_neighbors[str(row["neighbor_id"])]
            entry: dict[str, Any] = {
                "relationship": str(row["edge_type"]),
                "node_id": str(row["neighbor_id"]),
                "type": str(neighbor["subtype"]),
                "label": _display_label(
                    str(row["neighbor_id"]), str(neighbor["subtype"]), neighbor.get("attrs"),
                ),
            }
            identity = record_identity(neighbor.get("source_record_id"))
            if identity is not None:
                # Masked like every other route the identity leaves by: a
                # source that recorded the file it read would otherwise put
                # the operator's home directory on the Related tab.
                emitted, changed = mask_paths(
                    identity,
                    at=f"related.{direction}[{len(related)}].source_record_id",
                )
                entry["source_record_id"] = emitted
                masked_keys.extend(changed)
            related.append(entry)
        batch_end = batch_start + len(rows)
        direction_ended = len(rows) <= RELATED_CAP
        if len(related) == RELATED_CAP:
            more = any(shown(row) for row in rows[position - batch_start:]) or (
                not direction_ended
                and _edge_exists_at(graph, node_id, near=near, far=far, position=batch_end)
            )
            break
        if direction_ended:
            more = False
            break
    else:
        more = _edge_exists_at(graph, node_id, near=near, far=far, position=position)
    return related, more, masked_keys


# Attributes the source policy writes for its own bookkeeping. The record page
# reports them as source evidence (``source``, ``source_record_id``,
# ``source_revision``, ``policy_redacted``), not as fields of the record. This
# is the exact governance set, not a ``nomos_`` prefix: a source may author an
# attribute with that prefix, and it is a field like any other.
# ``classification`` stays a field: it is the record's own label.
# `PRODUCER_TEXT_ATTR` (issue #2991) repeats the end of `text`; like the
# lineage envelope it is system state, not a field to show.
_POLICY_BOOKKEEPING_KEYS = frozenset({LINEAGE_ATTR, REDACTION_MARKER, PRODUCER_TEXT_ATTR})
# The composed search surrogate: indexed text that repeats the title and body.
_SEARCH_TEXT_KEY = "text"


def _truthy(value: Any) -> bool:
    return value is True or (isinstance(value, str) and value.strip().lower() == "true")


def _read_time_hidden_keys(stored: Any, visible: Any) -> set[str]:
    """Attributes a read-time redaction removed from the record.

    The read filter withholds some values by removing the key outright. Only
    a row the filter actually redacted (``ReadRedactedAttrs``) is compared, and
    against the stored record with accepted corrections applied. Corrections
    only set values, so every such key missing from the redacted row was
    withheld.
    """
    if not isinstance(visible, ReadRedactedAttrs) or not isinstance(stored, dict):
        return set()
    return {
        str(key)
        for key, value in stored.items()
        if key not in visible
        and key not in GOVERNANCE_KEYS
        # The searchable-text copy (issue #2991) is a system key no read
        # shows, redacted or not, so its name is not listed either.
        and key != PRODUCER_TEXT_ATTR
        # An empty value is not shown as a field either way.
        and value is not None and value != ""
    }


def _record_fields_and_sections(
    subtype: str,
    attrs: dict[str, Any],
    *,
    removed_keys: set[str] | frozenset[str] = frozenset(),
    label: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    # The page's own heading, kept before the `label` ATTRIBUTE below reuses
    # the name for a different thing.
    title = (label or "").strip()
    record_attrs = {
        key: value
        for key, value in attrs.items()
        if key not in _POLICY_BOOKKEEPING_KEYS
    }
    label_attr = record_attrs.get("label")
    if (
        label_attr is not None
        and not _is_hidden(label_attr)
        and label_attr in (record_attrs.get("title"), record_attrs.get("name"))
    ):
        record_attrs.pop("label")
    body_keys = PACT_BODY_ATTR_KEYS if subtype == "pact_change" else GENERIC_BODY_ATTR_KEYS
    # A source often writes the record's identity under several keys at once:
    # a CMSSW release arrives as name AND release, both equal to the title the
    # page already shows. Those stay in the payload -- an operator comparing
    # against the source needs to know which keys were written -- but they are
    # flagged so the page can fold them away instead of repeating itself.
    fields = [
        {"key": item["key"], "value": None, "hidden": True, "echoes_title": False}
        if _is_hidden(record_attrs.get(item["key"]))
        else {
            "key": item["key"],
            "value": item["value"],
            "hidden": False,
            "echoes_title": bool(title) and str(item["value"]).strip() == title,
        }
        for item in format_attrs(record_attrs, omit_keys=body_keys)
    ]
    fields.extend(
        {"key": key, "value": None, "hidden": True, "echoes_title": False}
        for key in sorted(removed_keys - set(body_keys))
    )
    hidden_body = {
        key for key in body_keys
        if key in removed_keys or _is_hidden(record_attrs.get(key))
    }
    # Section titles come from the shared body layout. A hidden body value is
    # handed to it as the mask token so it keeps its place and title; hidden
    # identity values are left out so the layout cannot mistake one hidden
    # value for a repeat of another.
    layout_attrs = {
        **{key: value for key, value in record_attrs.items() if not _is_hidden(value)},
        **{key: MASK_TOKEN for key in hidden_body},
    }
    sections = [
        {**section, "value": None, "hidden": True}
        if section["key"] in hidden_body
        else {**section, "hidden": False}
        for section in body_sections_for(subtype, layout_attrs)
    ]
    # The search text repeats the title and body, so it is left out beside a
    # visible body, and a hidden one adds nothing beside other hidden content.
    # A readable search text beside only hidden content is the one readable part.
    text = next((section for section in sections if section["key"] == _SEARCH_TEXT_KEY), None)
    others = [section for section in sections if section["key"] != _SEARCH_TEXT_KEY]
    if text is not None and others and (
        text["hidden"] or any(not section["hidden"] for section in others)
    ):
        sections = others
    # With no body to stand beside it, the search text is shown as the record's
    # Content -- but for a record whose source carries no prose it is the
    # identity written out again, which is not content and is already above.
    elif text is not None and not text["hidden"] and _informative_excerpt(
        text.get("value"), title, record_attrs,
    ) is None:
        sections = others
    return fields, sections


def _dedupe_nodes(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[str, dict[str, Any]] = {}
    for row in rows:
        seen.setdefault(str(row["node_id"]), row)
    return list(seen.values())


# --- Record layers (PACT operator-console-record-layers) --------------------
#
# What this deployment RETAINS decides what these reads can say, and it is less
# than the Record page's mocks assumed. Each constant below is the one place
# its bound is written; the console holds the two it has to agree with
# (`INLINE_PAYLOAD_LIMIT` and `RAW_HEAD_LINES` in
# `console/model/explore/raw.ts`).

#: Stored edge rows one raw read returns. The read also reports the total and
#: this cap, so a capped page is visible as a capped page.
RAW_EDGE_CAP = 100
#: Relationships counted per direction before the tally gives up. Past this the
#: total is null and `*_total_capped` says so: a guessed total is worse than an
#: absent one.
RELATED_TALLY_CEILING = 500
#: Chunks one detail read returns, in stored ordinal order.
CHUNK_CAP = 200
#: A retained payload at most this size is sent whole.
RAW_INLINE_PAYLOAD_MAX_BYTES = 256 * 1024
#: Past that, this many lines are sent as its head, with the line and byte
#: counts, never as the whole payload.
RAW_HEAD_LINES = 200
#: And past THIS the payload is refused by name with its actual size, without
#: being read: a body no reader can use is not worth the memory to assemble.
RAW_PAYLOAD_MAX_BYTES = 8 * 1024 * 1024
#: The same ceiling for retained artifact bytes.
ARTIFACT_MAX_BYTES = 8 * 1024 * 1024
#: Media types the artifact byte route will serve. Raster images only: SVG is a
#: document that can carry script, so it is refused rather than served even
#: though it is an image to a reader.
ARTIFACT_MEDIA_TYPES = frozenset({
    "image/png", "image/jpeg", "image/gif", "image/webp",
})
#: A content address, checked before it reaches a query.
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
#: The attribute a `visual_artifact` carries to point at its retained crop
#: bytes (library/enrichers/visual_caption_enricher.py:97-102).
_CROP_REF_ATTR = "crop_blob_ref"
#: Attributes a source writes to record WHERE the original came from. They are
#: echoed, never opened: see `compose_record_artifact`.
_ORIGINAL_PATH_ATTRS = ("source_pdf_path", "pdf_path", "path", "doc_path")


def _is_content_address(value: Any) -> bool:
    return isinstance(value, str) and bool(_SHA256_RE.fullmatch(value))


#: An absolute filesystem path, in the spellings a source can record one.
#: POSIX (`/a/b`), home-relative (`~/a`, `~user/a`), a Windows drive (`C:\a`),
#: a UNC share (`\\host\share\a`) and a `file://` URL. An http(s) URL and a
#: relative path are NOT matched: a URL is a pointer the reader is meant to
#: follow, and a relative path names no machine and no account.
#: Spaces are allowed inside a segment on purpose: `/Users/<name>/My
#: Documents/a.pdf` is a real path, and a rule that skipped it would leak the
#: very thing this exists to hide. A newline is not: that is two values.
_ABSOLUTE_PATH_RE = re.compile(
    r"""\A(?:
        file:/[^\n\r]* |          # file:/x, file://host/x, file:///x
        /[^\n\r]* |               # POSIX absolute (and //host/x)
        ~/[^\n\r]* |              # home-relative
        ~[^/\n\r]+/[^\n\r]* |     # ~user/...
        [A-Za-z]:/[^\n\r]*        # Windows drive, after normalisation
    )\Z""",
    re.VERBOSE,
)

#: A UNC share. Checked BEFORE backslashes are normalised, because UNC is a
#: backslash form: a forward-slash `//host/x` is a protocol-relative URL, and
#: the old rule folded the two together and called both a path.
_UNC_RE = re.compile(r"\A\\\\[^\\\n\r]+\\[^\n\r]*\Z")

#: What a path is replaced by when it has no basename at all -- `/`, a bare
#: drive, or a `\\?\` prefix with nothing after it. An empty string would read
#: as "this field is empty" rather than "this field was shortened".
PATH_WITHHELD = "[path withheld]"


def _looks_absolute(value: str) -> bool:
    """Whether the WHOLE value is an absolute filesystem path.

    An http(s) URL is deliberately not one: it is a pointer the reader is
    meant to follow, and `citation_url` is the console's "open in source".
    A `file:` URL in any case IS one -- it names a file on a machine.
    """
    candidate = value.strip()
    if not candidate:
        return False
    if _UNC_RE.fullmatch(candidate):
        return True
    lowered = candidate.lower()
    if "://" in candidate and not lowered.startswith("file:"):
        return False
    if lowered.startswith("file:"):
        candidate = "file:" + candidate[len("file:"):]
        return bool(_ABSOLUTE_PATH_RE.fullmatch(candidate.replace("\\", "/")))
    return bool(_ABSOLUTE_PATH_RE.fullmatch(candidate.replace("\\", "/")))


#: What this rule does and does NOT do, stated plainly rather than implied.
#:
#: It is deliberately BROAD: ANY string value beginning with "/" is treated as
#: an absolute path and shortened to its last segment. That catches a
#: root-relative URL path (`/api/v1/records`), a stored regular expression
#: (`/^ab+c$/`) and any other slash-led value, which are rewritten too. The
#: trade is deliberate -- a leaked home directory is worse than a shortened
#: route -- and every key it rewrites is named in the response's
#: `masked_paths`, so the reader can see which values are not verbatim.
#:
#: What it does NOT catch: a path that appears INSIDE free text (a log line, an
#: error message, a prose field). The rule matches a value that IS a path, not
#: one that contains a path; masking inside prose would have to guess where the
#: path ends and would corrupt the text it edited. A source that writes an
#: operator path into the middle of a prose field still publishes it.
PATH_RULE_LIMIT = (
    "any string value that begins with '/' -- or is a ~, drive-letter, UNC or "
    "file: path -- is treated as an absolute path and replaced by its last "
    "segment, so a root-relative URL path or a stored regex is shortened too, "
    "and every rewritten key is named in masked_paths; a path written inside "
    "free text is NOT detected and is served as stored"
)


def _masked_path(value: Any) -> str | None:
    """A recorded path reduced to its basename, for display only.

    An absolute path names the machine, the account and often the person: an
    operator console that echoed one would publish `/Users/<name>/...` into a
    browser. The basename is what identifies the document to a reader, so that
    is all that leaves the server. This value is never resolved, opened or
    passed to any filesystem call -- no read in this module dereferences one.
    """
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    if not trimmed or _is_hidden(trimmed):
        return None
    tail = _basename(trimmed)
    return tail or None


def _basename(value: str) -> str:
    """The last segment of ``value``, or `PATH_WITHHELD` when there is none."""
    stripped = value.strip()
    lowered = stripped.lower()
    if lowered.startswith("file:"):
        stripped = stripped[len("file:"):]
        # file:///x, file://host/x and file:/x all reduce to the same tail.
        stripped = stripped.lstrip("/")
    # Windows extended-length prefix: \\?\C:\a\b and \\?\UNC\host\share\a.
    normalised = stripped.replace("\\", "/")
    if normalised.startswith("//?/"):
        normalised = normalised[len("//?/"):]
        if normalised.lower().startswith("unc/"):
            normalised = normalised[len("unc/"):]
    # A query or fragment is part of a URL, not of the name.
    normalised = normalised.split("?", 1)[0].split("#", 1)[0]
    tail = normalised.rstrip("/").rsplit("/", 1)[-1]
    # A bare drive designator is not a name. `C:\` normalises to `C:/`, whose
    # last segment is `C:` -- which would be served as though it were a
    # filename.
    if re.fullmatch(r"[A-Za-z]:", tail):
        tail = ""
    # `/`, `C:/` and a bare `\\?\` leave nothing to show. Saying so is not the
    # same as returning "", which would read as an empty field.
    return tail or PATH_WITHHELD


def mask_paths(value: Any, *, at: str = "") -> tuple[Any, list[str]]:
    """``value`` with every absolute path replaced by its basename.

    Applied to EVERY record read in this module, not only the artifact read.
    A stored attribute, a nested value inside one, an edge attribute and a
    source-record identity can each carry the operator's home directory, and a
    read that masked one route while serving the same bytes verbatim on
    another has not masked anything.

    Returns the masked value and the dotted trails that changed, so the
    response can say which fields are not verbatim rather than leaving the
    reader to assume a stored row is exactly what is stored.

    KEYS are masked too, and the reported trail uses the MASKED key. A JSONB
    object can be keyed by a path -- a per-file map is the obvious case -- and
    a version that masked only values republished the whole path as the key,
    and then a second time inside `masked_paths`.

    Two keys can mask to the same basename (`/a/x.md` and `/b/x.md`). Neither
    is dropped: the second and later keep their basename with a `~2`, `~3`
    suffix, in the order the object presented them, and each is named in the
    returned trails under the name it was actually emitted as.

    JSONB yields only objects, arrays, strings, numbers, booleans and null, so
    those are what this walks; a tuple is handled because a caller can hand one
    in. A set or a bytes value cannot come out of JSONB, so there is nothing
    here for them and they fall through unchanged rather than being guessed at.
    """
    if isinstance(value, str):
        if _looks_absolute(value) and not _is_hidden(value):
            return _basename(value), [at or "."]
        return value, []
    if isinstance(value, dict):
        masked: dict[Any, Any] = {}
        changed: list[str] = []
        for key, item in value.items():
            emitted_key, key_changed = _masked_key(key, taken=masked)
            trail = f"{at}.{emitted_key}" if at else str(emitted_key)
            masked[emitted_key], keys = mask_paths(item, at=trail)
            if key_changed:
                changed.append(trail)
            changed.extend(keys)
        return masked, changed
    if isinstance(value, (list, tuple)):
        out: list[Any] = []
        changed = []
        for index, item in enumerate(value):
            trail = f"{at}[{index}]" if at else f"[{index}]"
            item_masked, keys = mask_paths(item, at=trail)
            out.append(item_masked)
            changed.extend(keys)
        return out, changed
    return value, []


def _masked_key(key: Any, *, taken: dict[Any, Any]) -> tuple[Any, bool]:
    """The name a key is emitted under, and whether masking changed it."""
    if not isinstance(key, str) or not _looks_absolute(key) or _is_hidden(key):
        return key, False
    base = _basename(key)
    if base not in taken:
        return base, True
    # Two paths, one basename. Dropping either would lose a stored field, and
    # reusing the name would overwrite one with the other.
    suffix = 2
    while f"{base}~{suffix}" in taken:
        suffix += 1
    return f"{base}~{suffix}", True


def _recorded_original_path(attrs: Any) -> str | None:
    """The masked basename of whatever path the source recorded, if any."""
    if not isinstance(attrs, dict):
        return None
    for key in _ORIGINAL_PATH_ATTRS:
        masked = _masked_path(attrs.get(key))
        if masked is not None:
            return masked
    return None


def _visible_record_row(
    conn: Any, session: Session, graph: Any, node_id: str,
) -> dict[str, Any] | None:
    """The record as the reader may see it, or None for absent OR denied.

    The two are one answer on purpose, matching `compose_record_detail`: a
    record the policy hides must be indistinguishable from one that is not in
    this version, or the refusal itself tells the reader the record exists.
    """
    rows = graph.query(
        f"SELECT {_NODE_COLUMNS} FROM okg.graph_nodes WHERE node_id = :id LIMIT 1",
        {"id": node_id},
    )
    visible = filter_node_rows_for_session(conn, session, rows)
    if not visible:
        return None
    return _with_corrections(conn, session, visible)[0]


def _row_is_redacted(row: dict[str, Any]) -> bool:
    attrs = row.get("attrs") or {}
    if isinstance(attrs, ReadRedactedAttrs):
        return True
    return _truthy(attrs.get(REDACTION_MARKER)) or any(
        _is_hidden(value) for value in attrs.values()
    ) if isinstance(attrs, dict) else False


def _not_found_raw() -> dict[str, Any]:
    """The raw read's answer for a record that is absent OR denied.

    Identical bytes for both, by construction: nothing in this value is
    computed from the record, so there is nothing for the two cases to differ
    in.
    """
    return {
        "row": None,
        "edges": [],
        "edge_total": 0,
        "edge_cap": RAW_EDGE_CAP,
        "payload": None,
        "refusal": {
            "reason": "not_found",
            "policy": None,
            "detail": "no such record in this version",
        },
    }


_RAW_EDGE_SQL = """
SELECT e.edge_id, e.src, e.dst, e.edge_type, e.attrs, e.source,
       e.source_record_id, e.source_revision
  FROM okg.graph_edges e
 WHERE e.src = :id OR e.dst = :id
 ORDER BY e.edge_type, e.src, e.dst, e.edge_id
 LIMIT {batch}
"""



def compose_record_raw(
    conn: Any,
    *,
    session: Session,
    node_id: str,
) -> dict[str, Any]:
    """The record's stored row and edge rows, and its payload or a refusal.

    The row served here is the row AFTER the same read filter as the detail
    read. "Raw" means every stored column rather than the page's composed
    fields; it does not mean around the policy. A value the policy masked is
    masked here, and one it removed is absent here.

    The response always carries either a payload or a refusal naming why there
    is none. On this deployment the refusal is the usual answer: the store that
    retains a record's own bytes is the indexed-v2 source authority, which is
    opt-in and has no wired producer, so a legacy source retains identities for
    deletion synthesis and no content at all.
    """
    graph = checkout(conn, session)
    row = _visible_record_row(conn, session, graph, node_id)
    if row is None:
        return _not_found_raw()
    # One row past the cap, so the page can say it was cut without a second
    # count query. The total this answers is "how many, up to cap + 1" -- the
    # most the page ever needs -- and it is the length of rows read, not an
    # aggregate over the graph base.
    fetched = graph.query(
        _RAW_EDGE_SQL.format(batch=RAW_EDGE_CAP + 1), {"id": node_id},
    )
    # The extra row is a PROBE, not a count: it says only that the page was
    # cut. Serving `len(fetched)` as the total printed "100 of 101 shown" for
    # an Archiv3 person with 313 edges. A cut page therefore serves no total
    # and says why; an uncut page's total is exact.
    edges_truncated = len(fetched) > RAW_EDGE_CAP
    edge_total = None if edges_truncated else len(fetched)
    edge_rows = fetched[:RAW_EDGE_CAP]
    source_names, source_names_reason = _source_names(session)
    visible_edges = filter_edge_rows_for_session(
        conn,
        session,
        [{key: edge[key] for key in _EDGE_ROW_KEYS} for edge in edge_rows],
    )
    masked_keys: list[str] = []
    edges = []
    for index, edge in enumerate(visible_edges):
        attrs, changed = mask_paths(
            _unfence(edge.get("attrs")), at=f"edges[{index}].attrs",
        )
        identity, id_changed = mask_paths(
            record_identity(edge.get("source_record_id")),
            at=f"edges[{index}].source_record_id",
        )
        # The revision is masked like the identity: every Archiv3 twiki
        # revision records the absolute EOS root it was read from.
        revision, revision_changed = mask_paths(
            record_identity(edge.get("source_revision")),
            at=f"edges[{index}].source_revision",
        )
        masked_keys.extend(changed)
        masked_keys.extend(id_changed)
        masked_keys.extend(revision_changed)
        edges.append({
            "edge_id": str(edge["edge_id"]),
            "src": str(edge["src"]),
            "dst": str(edge["dst"]),
            "relationship": str(edge["edge_type"]),
            "attrs": without_producer_text(attrs),
            "source": edge.get("source"),
            "source_name": source_names.get(str(edge.get("source"))),
            "source_record_id": identity,
            "source_revision": revision,
        })
    payload, refusal = _retained_payload(conn, session, row)
    # "Raw" is every stored COLUMN, not every stored BYTE: an absolute path is
    # masked here exactly as it is on the artifact read, and the response names
    # the keys it changed so the reader is never told a mangled row is verbatim.
    row_attrs, attr_keys = mask_paths(
        _unfence(dict(row.get("attrs") or {})), at="row.attrs",
    )
    row_identity, identity_keys = mask_paths(
        record_identity(row.get("source_record_id")), at="row.source_record_id",
    )
    row_revision, revision_keys = mask_paths(
        record_identity(row.get("source_revision")), at="row.source_revision",
    )
    masked_keys.extend(attr_keys)
    masked_keys.extend(identity_keys)
    masked_keys.extend(revision_keys)
    return {
        "row": {
            "node_id": str(row["node_id"]),
            "subtype": str(row["subtype"]),
            "attrs": without_producer_text(row_attrs),
            "source": row.get("source"),
            "source_name": source_names.get(str(row.get("source"))),
            "source_record_id": row_identity,
            "source_revision": row_revision,
        },
        "source_names_unavailable_reason": source_names_reason,
        "masked_paths": sorted(masked_keys),
        "masked_paths_rule": PATH_RULE_LIMIT,
        "edges": edges,
        # Null when the page was cut at `edge_cap`: the read does not count
        # past its page, and the one-row probe is not a count.
        "edge_total": edge_total,
        "edges_truncated": edges_truncated,
        "edge_total_unavailable_reason": (
            "more_edges_than_the_page_cap_not_counted"
            if edges_truncated else None
        ),
        "edge_cap": RAW_EDGE_CAP,
        "payload": payload,
        "refusal": refusal,
    }


def _retained_payload(
    conn: Any, session: Session, row: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """The record's retained payload, or the refusal that stands in for it.

    The policy gate is applied BEFORE any payload row is read: the retained
    payload is exactly the bytes a read-time redaction removed, so a record the
    filter redacted has its payload refused rather than fetched and masked.
    """
    if _row_is_redacted(row):
        return None, {
            "reason": "policy_refused",
            "policy": "source read policy",
            "detail": (
                "this record's stored values are withheld from this reader, "
                "and the retained payload is those values"
            ),
        }
    source = row.get("source")
    if not isinstance(source, str) or not source.strip():
        return None, {
            "reason": "not_retained",
            "policy": None,
            "detail": "this record names no source to hold a payload",
        }
    located, miss = _locate_authority_payload(
        conn,
        source=source.strip(),
        record_id=row.get("source_record_id"),
        generation_id=session.generation_id,
    )
    if located is None:
        return None, miss
    sha256, byte_count = located
    if byte_count > RAW_PAYLOAD_MAX_BYTES:
        return None, {
            "reason": "not_retained",
            "policy": None,
            "detail": (
                f"the retained payload is {byte_count} bytes, past the "
                f"{RAW_PAYLOAD_MAX_BYTES}-byte ceiling this read will assemble"
            ),
        }
    # MEASURED in the database before a single payload byte is selected. The
    # `byte_count` column checked above is the row's claim about itself; this
    # is what the chunk rows actually hold, and a row that understates its blob
    # is refused here rather than after the blob has crossed the wire.
    stored = _stored_payload_size(conn, sha256)
    if stored > RAW_PAYLOAD_MAX_BYTES:
        return None, {
            "reason": "not_retained",
            "policy": None,
            "detail": (
                f"the stored payload holds {stored} bytes, larger than its "
                f"recorded {byte_count} and past the "
                f"{RAW_PAYLOAD_MAX_BYTES}-byte ceiling this read will assemble"
            ),
        }
    raw = _payload_bytes(conn, sha256, limit=RAW_PAYLOAD_MAX_BYTES + 1)
    decoded = _as_json_text(raw)
    if decoded is None:
        # The store declares `payload_kind = 'json'`. Bytes that are not valid
        # UTF-8 JSON are not what the column says they are, and serving them
        # under `application/json` -- or handing the console a url that would
        # -- labels them as something they are not.
        return None, {
            "reason": "not_retained",
            "policy": None,
            "detail": (
                "the retained payload is recorded as JSON but its bytes are "
                "not valid UTF-8 JSON, so this read will not serve them"
            ),
        }
    text = decoded if len(raw) <= RAW_INLINE_PAYLOAD_MAX_BYTES else None
    head, line_count = (None, None)
    if text is None:
        lines = decoded.splitlines()
        head, line_count = "\n".join(lines[:RAW_HEAD_LINES]), len(lines)
    return {
        "sha256": sha256,
        "byte_count": byte_count,
        "inline": text,
        "head": head,
        "line_count": line_count,
        # What the pin actually rests on, named so the reader is not left to
        # assume something stronger. See `_locate_authority_payload`.
        "pinning_basis": "bound_before_publish",
        # Addressed by RECORD, never by bare content hash: the gate above is
        # the record's, so a route keyed on the hash alone would serve any
        # payload to anyone who learned its digest.
        "url": (
            "/viz/api/console/records/payload"
            f"?version={session.generation_id}&id={row['node_id']}"
        ),
    }, None


def _canonical_record_ids(value: Any) -> list[str]:
    """Every spelling a stored `source_record_id` could have as an authority key.

    The indexed-v2 authority keys a record by a TEXT record id; a published row
    carries `source_record_id` as JSONB, which is a plain string for some
    sources and an object for others. There is no wired producer to settle the
    convention, so the read tries every spelling it can justify instead of
    picking one and reporting a miss as if the store were empty:

    * the string itself, when the identity is a string;
    * an object as compact sorted JSON, and as JSON with the separators
      `json.dumps` writes by default -- the two forms a writer produces
      depending on whether it passed `separators`.
    """
    import json

    if isinstance(value, str) and value.strip():
        return [value]
    if isinstance(value, dict) and value:
        compact = json.dumps(value, sort_keys=True, separators=(",", ":"))
        spaced = json.dumps(value, sort_keys=True)
        return [compact] if compact == spaced else [compact, spaced]
    return []


def _locate_authority_payload(
    conn: Any, *, source: str, record_id: Any, generation_id: str,
) -> tuple[tuple[str, int] | None, dict[str, Any] | None]:
    """The sealed authority payload for one record, or the refusal that stands in.

    Reads only through `source_record_authority_refs`, which is the public
    binding: a staging authority is private and must never answer a read.

    VERSION PINNING, and its KNOWN LIMIT. `source_record_authority_refs` is
    keyed by source alone, so it holds the authority bound NOW, not the one a
    past generation was built from. The check here is that the binding is older
    than the generation's `published_at`, and a binding younger than that is
    refused by name.

    That is weaker than "the payload this version was built from", and the
    response says so through `pinning_basis: bound_before_publish` rather than
    implying more. The window it does not close: an ingest builds the facts,
    a DIFFERENT ingest rebinds the source's authority, and only then does the
    publish happen. The rebinding is older than the publish, so it passes, and
    the payload belongs to the second ingest while the facts belong to the
    first. The schema records no link from a generation to the authority its
    facts came from -- there is no authority id on the generation and no
    generation on the ref -- so nothing here can close that window. Closing it
    needs a stored link, which is a change to the publisher, not to this read.
    """
    candidates = _canonical_record_ids(record_id)
    if not candidates:
        return None, {
            "reason": "not_retained",
            "policy": None,
            "detail": "this record carries no source-record identity to look up",
        }
    with conn.cursor() as cur:
        # The authority tables are not in every deployment: okg-workspace has
        # no `source_record_authority_refs` at all, and this read answered it
        # with a 500 and `UndefinedTable` -- a database error where the read
        # has a perfectly good refusal to serve. Asked once, before the first
        # statement that names the table, so a deployment without the feature
        # is a named absence and not a broken read.
        cur.execute("SELECT to_regclass('okg.source_record_authority_refs')")
        relation = cur.fetchone()
        if relation is None or relation[0] is None:
            return None, {
                "reason": "not_retained",
                "policy": None,
                "detail": (
                    "this deployment records no source-record authority "
                    "(table not present; migrate to enable)"
                ),
            }
        cur.execute(
            """
            SELECT r.bound_at, g.published_at
              FROM okg.source_record_authority_refs r
              LEFT JOIN okg.v_generations g ON g.id = %s
             WHERE r.source = %s
            """,
            (generation_id, source),
        )
        bound = cur.fetchone()
        if bound is None:
            return None, {
                "reason": "not_retained",
                "policy": None,
                "detail": (
                    "the original payload is kept only for sources that "
                    "store records individually; this source does not"
                ),
            }
        bound_at, published_at = bound[0], bound[1]
        if published_at is None or bound_at is None or bound_at > published_at:
            return None, {
                "reason": "payload_not_pinned_to_version",
                "policy": None,
                "detail": (
                    "the source's retained payload was bound after this "
                    "version was published, so it is not the payload this "
                    "version was built from"
                ),
            }
        cur.execute(
            """
            SELECT p.sha256, p.byte_count, f.field_name, rec.record_id
              FROM okg.source_record_authority_refs r
              JOIN okg.source_record_authority_records rec
                ON rec.authority_id = r.authority_id
               AND rec.record_id = ANY(%s)
              JOIN okg.source_record_authority_components c
                ON c.authority_id = rec.authority_id
               AND c.operation_ordinal = rec.operation_ordinal
               AND c.component_kind = 'primary'
              JOIN okg.source_record_authority_component_fields f
                ON f.authority_id = c.authority_id
               AND f.operation_ordinal = c.operation_ordinal
               AND f.component_kind = c.component_kind
               AND f.component_ordinal = c.component_ordinal
              JOIN okg.source_record_authority_payloads p
                ON p.sha256 = f.payload_sha256
             WHERE r.source = %s
             ORDER BY f.field_name
             LIMIT %s
            """,
            (candidates, source, _PAYLOAD_FIELD_LIMIT),
        )
        found = cur.fetchall()
    if not found:
        # Only what is known. The record may be spelled a third way, the
        # authority may not hold it, or the component may not be primary --
        # this read cannot tell which, and naming a cause it has not checked
        # would be a guess presented as a finding.
        return None, {
            "reason": "not_retained",
            "policy": None,
            "detail": (
                "no matching authority row under either spelling of this "
                "record's source-record identity"
            ),
        }
    spellings = {str(row[3]) for row in found}
    if len(spellings) > 1:
        # Both spellings of this record's identity matched, and they are
        # different authority records. Which one IS this record is not
        # something this read can decide, and calling it a multi-field payload
        # would name the wrong cause.
        return None, {
            "reason": "payload_identity_ambiguous",
            "policy": None,
            "detail": (
                f"{len(spellings)} authority records match this record's "
                "source-record identity under its different spellings, and "
                "this read cannot tell which one is the record"
            ),
        }
    if len(found) > 1:
        # One record's primary component can carry several fields. Picking the
        # alphabetically first and calling it "the payload" would show the
        # reader one field as though it were the record. The NAMES go through
        # the path mask: a field can be named for the file it came from.
        names, _keys = mask_paths(sorted(str(row[2]) for row in found))
        return None, {
            "reason": "payload_has_several_fields",
            "policy": None,
            "detail": (
                "this record's retained payload is "
                f"{len(found)} separate fields "
                f"({', '.join(names)}), and "
                "this read will not present one of them as the record"
            ),
        }
    return (str(found[0][0]), int(found[0][1])), None


#: Enough rows to tell "one field" from "more than one" without reading a wide
#: component's every field to say so.
_PAYLOAD_FIELD_LIMIT = 16


def _stored_payload_size(conn: Any, sha256: str) -> int:
    """How many bytes the chunk rows ACTUALLY hold, measured in the database.

    The `byte_count` column is a claim; this is the measurement. Asked before
    any payload bytes are selected, so a row that understates its blob cannot
    get the blob shipped to this process at all.
    """
    if not _is_content_address(sha256):
        raise ValueError("payload sha256 is not a content address")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT coalesce(sum(octet_length(payload)), 0)
              FROM okg.source_record_authority_payload_chunks
             WHERE sha256 = %s
            """,
            (sha256,),
        )
        found = cur.fetchone()
    return int(found[0]) if found else 0


def _payload_bytes(conn: Any, sha256: str, *, limit: int) -> bytes:
    """A content-addressed payload's chunks, in order, at most ``limit`` bytes.

    The bound is enforced in the DATABASE, not only on the copy. A plain
    psycopg cursor fetches the whole result set when `execute` returns, so an
    earlier version that stopped iterating after `limit` bytes had already
    pulled every chunk across the wire -- the loop was lazy, the transfer was
    not. This reads through a NAMED (server-side) cursor, which fetches in
    batches, so the bytes past the limit stay in the database.

    Callers still check `_stored_payload_size` first and refuse an oversized
    payload by name; this limit is the second line, not the first.
    """
    if not _is_content_address(sha256):
        raise ValueError("payload sha256 is not a content address")
    parts: list[bytes] = []
    size = 0
    # A named cursor needs a name AND a transaction: the record routes run the
    # connection in autocommit, and `DECLARE CURSOR` is refused outside a
    # transaction block. The explicit transaction is what makes the
    # server-side fetch possible at all, so it is not optional here.
    with conn.transaction(), conn.cursor(name=f"okg_payload_{sha256[:16]}") as cur:
        cur.itersize = 8
        cur.execute(
            """
            SELECT payload FROM okg.source_record_authority_payload_chunks
             WHERE sha256 = %s ORDER BY chunk_ordinal
            """,
            (sha256,),
        )
        for part in cur:
            chunk = bytes(part[0])
            parts.append(chunk)
            size += len(chunk)
            if size >= limit:
                break
    return b"".join(parts)[:limit]


def _as_json_text(raw: bytes) -> str | None:
    """``raw`` decoded as the JSON the payload store says it is, or None.

    A deeply nested payload raises `RecursionError` rather than `ValueError`,
    and a huge one can raise `MemoryError`; neither is a ValueError, so both
    escaped this check and became a 500 from a read whose whole job is to say
    "these bytes are not what the column claims". They are that answer.
    """
    import json

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None
    try:
        json.loads(text)
    except (ValueError, RecursionError, MemoryError):
        return None
    return text


def compose_record_artifact(
    conn: Any,
    *,
    session: Session,
    node_id: str,
) -> dict[str, Any]:
    """Whether the record's original file is retained IN this deployment.

    This read NEVER opens a file. A source that read a document off the
    operator's disk records where it read it -- `source_pdf_path` for the
    papers source -- and that path is echoed as its BASENAME and nothing else,
    never resolved and never handed to a filesystem call. Serving a file named
    by a stored attribute would make every source a way to read any file the
    server can read, and would put the operator's home directory in a browser.

    The one store that holds real bytes is `okg.visual_crop_blobs`: figure
    crops, content-addressed, with a stored media type. Everything else is
    reported as not retained, with the byte count and content hash the source
    did record so the reader can still identify the original.
    """
    graph = checkout(conn, session)
    row = _visible_record_row(conn, session, graph, node_id)
    if row is None:
        return {
            "exists": False,
            "reason": "not_found",
            "policy": None,
            "format": None,
            "path": None,
            "detail": "no such record in this version",
        }
    attrs = row.get("attrs") or {}
    fmt = attrs.get("format") if isinstance(attrs, dict) else None
    fmt = fmt if isinstance(fmt, str) and not _is_hidden(fmt) else None
    path = _recorded_original_path(attrs)
    if _row_is_redacted(row):
        return {
            "exists": False,
            "reason": "policy_refused",
            "policy": "source read policy",
            "format": fmt,
            "path": None,
            "detail": "this record's stored values are withheld from this reader",
        }
    ref = attrs.get(_CROP_REF_ATTR) if isinstance(attrs, dict) else None
    if not _is_content_address(ref):
        return {
            "exists": False,
            "reason": "not_retained",
            "policy": None,
            "format": fmt,
            "path": path,
            "detail": (
                "this deployment retains the text extracted from the original, "
                "not the original file"
            ),
        }
    meta = _crop_metadata(conn, str(ref))
    if meta is None:
        return {
            "exists": False,
            "reason": "file_missing",
            "policy": None,
            "format": fmt,
            "path": path,
            "detail": (
                "this record names retained bytes that are no longer in the "
                "crop store"
            ),
        }
    media_type, byte_count = meta
    if media_type not in ARTIFACT_MEDIA_TYPES or byte_count > ARTIFACT_MAX_BYTES:
        return {
            "exists": False,
            "reason": "not_retained",
            "policy": None,
            "format": fmt,
            "path": path,
            "detail": (
                f"the retained bytes are {byte_count} bytes of {media_type}, "
                "which this read does not serve"
            ),
        }
    return {
        "exists": True,
        "media_type": media_type,
        "format": fmt,
        "path": path,
        "byte_count": byte_count,
        "sha256": str(ref),
        "url": (
            "/viz/api/console/records/artifact/bytes"
            f"?version={session.generation_id}&id={node_id}"
        ),
    }


def _crop_metadata(conn: Any, sha256: str) -> tuple[str, int] | None:
    """A retained crop's media type and size, WITHOUT reading its bytes.

    The size decides whether the bytes may be served at all, so it has to be
    known before they are read; selecting `bytes` here would defeat the cap it
    feeds.
    """
    if not _is_content_address(sha256):
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT mime_type, size_bytes FROM okg.visual_crop_blobs "
            "WHERE sha256 = %s",
            (sha256,),
        )
        found = cur.fetchone()
    if found is None:
        return None
    return str(found[0]), int(found[1])


def read_payload_bytes(
    conn: Any,
    *,
    session: Session,
    node_id: str,
) -> tuple[bytes, str] | None:
    """The retained payload the raw read named, as bytes.

    Resolved through `_retained_payload`, so the policy gate that refuses a
    redacted record's payload runs here too and the ceiling that refused an
    oversized one refuses it here as well. A refusal returns None, which the
    route renders exactly as an absent record does.
    """
    graph = checkout(conn, session)
    row = _visible_record_row(conn, session, graph, node_id)
    if row is None:
        return None
    payload, _refusal = _retained_payload(conn, session, row)
    if payload is None:
        return None
    raw = _payload_bytes(
        conn, str(payload["sha256"]), limit=RAW_PAYLOAD_MAX_BYTES + 1,
    )
    # `_retained_payload` already proved these bytes are valid UTF-8 JSON and
    # within the ceiling; re-checking here means the byte route cannot serve a
    # body under `application/json` that the describing read would have
    # refused, even if the two ever drift apart.
    if len(raw) > RAW_PAYLOAD_MAX_BYTES or _as_json_text(raw) is None:
        return None
    return raw, "application/json"


def read_artifact_bytes(
    conn: Any,
    *,
    session: Session,
    node_id: str,
) -> tuple[bytes, str] | None:
    """The retained bytes the artifact read named, with their media type.

    Resolved through `compose_record_artifact`, so the bytes a caller receives
    are the bytes that read authorized, under the same version, the same filter
    and the same media-type and size limits. A record the reader may not see
    returns None here exactly as it does there.
    """
    described = compose_record_artifact(conn, session=session, node_id=node_id)
    if not described.get("exists"):
        return None
    from okg.substrate.library.captioning import crop_store

    try:
        data = crop_store.get_crop(conn, str(described["sha256"]))
    except crop_store.CropBlobMissing:
        return None
    media_type = str(described["media_type"])
    # The declared type is a stored STRING; the bytes are the fact. Serving
    # HTML under `image/png` because a row said so would hand a browser a
    # document this route promised was a picture, and `nosniff` would then
    # make the browser trust the wrong label rather than the right bytes.
    if not _bytes_match_media_type(data, media_type):
        return None
    return data, media_type


#: The leading bytes each servable image type begins with. WebP is a RIFF
#: container, so its tag sits at offset 8 and is checked separately.
_MEDIA_TYPE_SIGNATURES: dict[str, tuple[bytes, ...]] = {
    "image/png": (b"\x89PNG\r\n\x1a\n",),
    "image/jpeg": (b"\xff\xd8\xff",),
    "image/gif": (b"GIF87a", b"GIF89a"),
}


def _bytes_match_media_type(data: bytes, media_type: str) -> bool:
    """Whether ``data`` actually begins like ``media_type`` says it does."""
    if media_type == "image/webp":
        return (
            len(data) >= 12
            and data[:4] == b"RIFF"
            and data[8:12] == b"WEBP"
        )
    signatures = _MEDIA_TYPE_SIGNATURES.get(media_type)
    if not signatures:
        return False
    return any(data.startswith(signature) for signature in signatures)


def _related_tally(
    conn: Any,
    session: Session,
    graph: Any,
    node_id: str,
    *,
    direction: str,
) -> tuple[int | None, list[dict[str, Any]]]:
    """Visible relationships in one direction, counted per relationship type.

    Counted over relationships whose edge AND neighbour the reader may see, so
    the tally discloses nothing the related list hides. Counting the version's
    totals before the access rules would be cheaper and would tell the reader
    how many neighbours are being kept from them.
    """
    near, far = ("src", "dst") if direction == "outgoing" else ("dst", "src")
    rows = graph.query(
        _RELATED_SQL.format(
            near=near, far=far, batch=RELATED_TALLY_CEILING + 1, offset=0,
        ),
        {"id": node_id},
    )
    if len(rows) > RELATED_TALLY_CEILING:
        return None, []
    visible_edge_ids = {
        str(edge["edge_id"])
        for edge in filter_edge_rows_for_session(
            conn,
            session,
            [{key: row[key] for key in _EDGE_ROW_KEYS} for row in rows],
        )
    }
    neighbors = _dedupe_nodes(
        {
            "node_id": row["neighbor_id"],
            "subtype": row["neighbor_subtype"],
            "attrs": without_producer_text(row["neighbor_attrs"]),
            "source": row["neighbor_source"],
            "source_record_id": row["neighbor_source_record_id"],
            "source_revision": row["neighbor_source_revision"],
        }
        for row in rows
        if str(row["edge_id"]) in visible_edge_ids
        and row["neighbor_subtype"] is not None
    )
    visible_neighbors = {
        str(item["node_id"]) for item in _visible_nodes(conn, session, neighbors)
    }
    tally: dict[str, int] = {}
    for row in rows:
        if (
            str(row["edge_id"]) in visible_edge_ids
            and str(row["neighbor_id"]) in visible_neighbors
        ):
            key = str(row["edge_type"])
            tally[key] = tally.get(key, 0) + 1
    counts = [
        {"direction": direction, "relationship": name, "count": count}
        for name, count in sorted(tally.items())
    ]
    return sum(tally.values()), counts


# Ordered by the STORED ordinal in SQL, not after the fact in Python. Ordering
# by `edge_id` and then sorting the page was a bug with a green test: an edge
# id is a hash, so the first CHUNK_CAP rows of a long document were an
# arbitrary subset that the Python sort then put in a convincing order. A
# document past the cap would have shown the reader chunks 3, 17 and 204 as
# though they were its beginning.
#: How a document reaches its chunks, in either of the two shapes the readers
#: record. The git and corpus readers write `member_of` from the chunk to the
#: document; the twiki-eos reader writes `contains` from the document to the
#: chunk. This read matched only the first, so on a graph built by the second
#: -- cern-r3final, the operator's main test graph, where `contains` is 4,768
#: edges and `member_of` 570 -- a document had no chunks at all, `order_basis`
#: was never set, and the Content tab could not list what the document holds.
#:
#: One statement, the same cap. `DISTINCT ON` deduplicates by CHUNK, because a
#: graph that records both directions for one pair would otherwise list the
#: chunk twice; `member_of` wins the tie, so nothing changes for a graph that
#: has it.
_CHUNK_EDGE_JOIN = """
  FROM okg.graph_edges e
  JOIN okg.graph_nodes n
    ON n.node_id = CASE WHEN e.src = :id THEN e.dst ELSE e.src END
 WHERE ((e.dst = :id AND e.edge_type = 'member_of')
        OR (e.src = :id AND e.edge_type = 'contains'))
   AND e.src <> e.dst
   AND n.subtype = 'document_chunk'
"""

_CHUNK_COLUMNS = """
SELECT DISTINCT ON (n.node_id)
       e.edge_id, e.attrs AS edge_attrs, e.src, e.dst, e.edge_type,
       e.source, e.source_record_id, e.source_revision,
       n.node_id AS chunk_id, n.subtype AS chunk_subtype, n.attrs AS chunk_attrs,
       n.source AS chunk_source, n.source_record_id AS chunk_source_record_id,
       n.source_revision AS chunk_source_revision
"""

#: `member_of` first in the DISTINCT ON order, so a pair recorded both ways
#: keeps the edge this read already used and its `chunk_ordinal` with it.
_CHUNK_PREFERENCE = " ORDER BY n.node_id, (e.edge_type = 'member_of') DESC, e.edge_id"

_CHUNKS_SQL = """
SELECT * FROM (""" + _CHUNK_COLUMNS + _CHUNK_EDGE_JOIN + """
   AND {ordinal_ok}
""" + _CHUNK_PREFERENCE + """
) d
 ORDER BY (d.edge_attrs ->> 'chunk_ordinal')::numeric, d.edge_id
 LIMIT {batch} OFFSET {offset}
"""

#: What counts as a usable ordinal, in SQL, so the ordering, the page and the
#: count all agree on it. A JSON number is not enough: `-1` and `1.5` are
#: numbers, the Python side rejected them AFTER the page was cut, and the SQL
#: neither excluded nor counted them. A negative also sorts first, so a single
#: `-1` ate a slot at the head of the page and then vanished from the result.
#
# `coalesce(..., false)` is load-bearing. An edge with NO `chunk_ordinal` key
# gives `jsonb_typeof(NULL)` = NULL, so the whole expression is NULL, and
# `NOT NULL` is NULL rather than true -- which quietly dropped every
# key-less chunk from the "without ordinal" count that exists to find them.
_ORDINAL_OK = (
    "coalesce("
    "jsonb_typeof(e.attrs -> 'chunk_ordinal') = 'number' "
    "AND (e.attrs ->> 'chunk_ordinal') ~ '^[0-9]+$'"
    ", false)"
)

#: Chunk membership rows with no USABLE ordinal. Their rows are fetched, not
#: counted in SQL, because the number the reader is shown has to be the number
#: of chunks THEY cannot see ordered -- counting before the policy filter would
#: tell them how many rows the policy hid, which is exactly what
#: `_related_tally` refuses to do.
_CHUNKS_UNORDERED_SQL = """
SELECT * FROM (""" + _CHUNK_COLUMNS + _CHUNK_EDGE_JOIN + """
   AND NOT ({ordinal_ok})
""" + _CHUNK_PREFERENCE + """
) d
 ORDER BY d.edge_id
 LIMIT {batch}
"""


def _char_offset(attrs: Any) -> int | None:
    """Where this chunk starts in its document, or None if nothing says.

    The SECOND basis this read will order by. Chunkers write the offset as a
    JSON number and, just as often, as a digit string (a document_chunk row
    carrying `"char_offset": "40"` is the ordinary shape), so both are read --
    and both are read AS A NUMBER, because "120" sorts before "40" as text and
    a lexical order here would be a wrong order that looked right.

    A negative or fractional value is not an offset, for the same reason a
    negative is not an ordinal: it cannot place the chunk, so it is refused
    rather than sorted by.
    """
    if not isinstance(attrs, dict):
        return None
    value = attrs.get("char_offset")
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    # `isascii` guards `isdigit`: a superscript "2" is a digit to Python and
    # is not an integer to `int()`, so the check without it raises.
    if isinstance(value, str) and value.isascii() and value.isdigit():
        return int(value)
    return None


def _visible_chunk_rows(
    conn: Any, session: Session, rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """The membership rows whose edge AND chunk this reader may see."""
    if not rows:
        return []
    visible_edge_ids = {
        str(edge["edge_id"])
        for edge in filter_edge_rows_for_session(
            conn,
            session,
            [
                {
                    "edge_id": row["edge_id"], "src": row["src"], "dst": row["dst"],
                    "edge_type": row["edge_type"], "attrs": row["edge_attrs"],
                    "source": row["source"],
                    "source_record_id": row["source_record_id"],
                    "source_revision": row["source_revision"],
                }
                for row in rows
            ],
        )
    }
    visible_chunks = {
        str(item["node_id"]): item
        for item in _visible_nodes(
            conn,
            session,
            [
                {
                    "node_id": row["chunk_id"], "subtype": row["chunk_subtype"],
                    "attrs": row["chunk_attrs"], "source": row["chunk_source"],
                    "source_record_id": row["chunk_source_record_id"],
                    "source_revision": row["chunk_source_revision"],
                }
                for row in rows
            ],
        )
    }
    out = []
    for row in rows:
        if str(row["edge_id"]) not in visible_edge_ids:
            continue
        chunk = visible_chunks.get(str(row["chunk_id"]))
        if chunk is None:
            continue
        out.append({**row, "_chunk": chunk})
    return out


def _record_chunks(
    conn: Any, session: Session, graph: Any, node_id: str,
) -> dict[str, Any] | None:
    """A document's retained chunks, in an order the source actually stored.

    TWO bases, tried in that order, and the result NAMES the one it used in
    `order_basis`:

    - `chunk_ordinal` on the membership edge. Ordered IN SQL, and SQL decides
      what counts as one: a non-negative integer. A number that is negative or
      fractional is not an ordinal, and an earlier version let SQL order by it
      and then dropped it in Python, so a `-1` sorted to the front, ate a slot
      at the head of the page and then disappeared -- and the "no ordinal"
      count did not include it either, so a document of such chunks read as
      having none.
    - `char_offset` on the chunk itself, used only when NO chunk of the record
      has a usable ordinal and EVERY one of them has an offset (issue #2178).
      An offset is a recorded order, and discarding it told the reader a
      document had no placeable content when it plainly did.

    One basis or none: if neither covers every chunk, they are all counted in
    `without_ordinal` and none is listed. Ordering the half that one basis
    covers would show a beginning that is not the beginning, with nothing in
    the response saying which chunks are missing.

    The fallback reads NO extra statement and reaches no further than this
    record's own chunks: it sorts rows the unordered query already fetched,
    and it declines when that query hit its limit, because a capped page is a
    slice whose order is unknowable rather than a document's beginning.

    Every count the reader sees is taken AFTER the policy filter. A count taken
    before it would tell the reader how many rows the policy hid from them,
    which is precisely what `_related_tally` refuses to do; the two must not
    disagree about that.
    """
    ordered: list[dict[str, Any]] = []
    position = 0
    for _round in range(_CHUNK_REFILL_ROUNDS):
        batch = graph.query(
            _CHUNKS_SQL.format(
                ordinal_ok=_ORDINAL_OK, batch=CHUNK_CAP + 1, offset=position,
            ),
            {"id": node_id},
        )
        if not batch:
            break
        position += len(batch)
        ordered.extend(_visible_chunk_rows(conn, session, batch))
        if len(ordered) > CHUNK_CAP or len(batch) <= CHUNK_CAP:
            break
    # Capped is decided on VISIBLE rows: a page is short because the policy
    # hid rows, not because more follow.
    capped = len(ordered) > CHUNK_CAP
    ordered = ordered[:CHUNK_CAP]

    unordered_rows = graph.query(
        _CHUNKS_UNORDERED_SQL.format(ordinal_ok=_ORDINAL_OK, batch=CHUNK_CAP + 1),
        {"id": node_id},
    )
    visible_unordered = _visible_chunk_rows(conn, session, unordered_rows)
    without_ordinal = min(len(visible_unordered), CHUNK_CAP)
    without_ordinal_capped = len(visible_unordered) > CHUNK_CAP

    basis: str | None = "chunk_ordinal" if ordered else None
    # The second basis. Only when the first placed NOTHING -- a record with
    # some ordinals keeps them, because mixing the two would interleave two
    # different orders into one list that says it is a single order.
    if (
        not ordered
        and visible_unordered
        # The query fetches CHUNK_CAP + 1 rows, so at or under the cap means
        # these ARE every unordered chunk, not the first page of them.
        and len(unordered_rows) <= CHUNK_CAP
    ):
        offsets = {
            str(row["_chunk"]["node_id"]): _char_offset(row["_chunk"].get("attrs"))
            for row in visible_unordered
        }
        if all(value is not None for value in offsets.values()):
            basis = "char_offset"
            ordered = sorted(
                visible_unordered,
                key=lambda row: (
                    offsets[str(row["_chunk"]["node_id"])],
                    str(row["_chunk"]["node_id"]),
                ),
            )
            without_ordinal = 0
            without_ordinal_capped = False

    chunks: list[dict[str, Any]] = []
    for row in ordered:
        chunk = row["_chunk"]
        edge_attrs = row["edge_attrs"] if isinstance(row["edge_attrs"], dict) else {}
        chunk_attrs = chunk.get("attrs") or {}
        text = chunk_attrs.get("text") if isinstance(chunk_attrs, dict) else None
        chunks.append({
            "node_id": str(chunk["node_id"]),
            # Null rather than a position: on the offset basis no ordinal was
            # stored, and numbering the list would invent one.
            "ordinal": (
                int(edge_attrs["chunk_ordinal"]) if basis == "chunk_ordinal" else None
            ),
            "char_offset": _char_offset(chunk_attrs),
            "page": _chunk_page(chunk_attrs),
            "heading_path": _heading_path(edge_attrs.get("heading_path")),
            "text": "" if _is_hidden(text) or text is None else str(_unfence(text)),
        })
    if not chunks and not without_ordinal:
        return None
    # Which edge shape reached these chunks. One name when every row this
    # reader can see came the same way, "both" when the graph records the pair
    # in both directions -- never one of the two names when the other was also
    # used, because the page may state this and a single name would be false.
    shapes = {
        str(row["edge_type"]) for row in [*ordered, *visible_unordered]
    }
    chunk_edge = shapes.pop() if len(shapes) == 1 else ("both" if shapes else None)
    return {
        "chunks": chunks,
        "cap": CHUNK_CAP,
        "capped": capped,
        "order_basis": basis,
        "chunk_edge": chunk_edge,
        "without_ordinal": without_ordinal,
        "without_ordinal_capped": without_ordinal_capped,
    }


def _chunk_page(attrs: Any) -> int | None:
    if not isinstance(attrs, dict):
        return None
    value = attrs.get("page_number")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _heading_path(value: Any) -> str | None:
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, list):
        parts = [str(part).strip() for part in value if str(part).strip()]
        return " / ".join(parts) or None
    return None


def compose_record_detail(
    conn: Any,
    *,
    session: Session,
    node_id: str,
) -> dict[str, Any]:
    """One record in the pinned version, with fields, source and related records."""
    graph = checkout(conn, session)
    rows = graph.query(
        f"SELECT {_NODE_COLUMNS} FROM okg.graph_nodes WHERE node_id = :id LIMIT 1",
        {"id": node_id},
    )
    visible = filter_node_rows_for_session(conn, session, rows)
    if not visible:
        return {"found": False, "node_id": node_id}
    # Compare against the corrected stored record, so a corrected value the
    # redaction removed is reported as hidden too.
    stored = _with_corrections(conn, session, [dict(rows[0])])[0]
    removed_keys = _read_time_hidden_keys(stored.get("attrs"), visible[0].get("attrs"))
    row = _with_corrections(conn, session, visible)[0]
    subtype = str(row["subtype"])
    attrs = row.get("attrs") or {}
    # Masked BEFORE formatting (#2130). `format_attrs` flattens a nested
    # value into one string -- `{"path": "/Users/<name>/x.yaml"}` becomes a
    # line that no longer STARTS with `/` -- so masking only the formatted
    # fields served the path inside it. The raw read masks the stored object
    # first, and so does this one now; the formatted pass below stays for a
    # value the formatter itself could compose.
    masked_attrs, attr_keys = mask_paths(dict(attrs), at="attrs")
    fields, sections = _record_fields_and_sections(
        subtype,
        masked_attrs,
        removed_keys=removed_keys,
        label=_display_label(str(row["node_id"]), subtype, attrs),
    )
    outgoing, outgoing_more, outgoing_masked = _related(
        conn, session, graph, node_id, direction="outgoing",
    )
    incoming, incoming_more, incoming_masked = _related(
        conn, session, graph, node_id, direction="incoming",
    )
    outgoing_total, outgoing_tally = _related_tally(
        conn, session, graph, node_id, direction="outgoing",
    )
    incoming_total, incoming_tally = _related_tally(
        conn, session, graph, node_id, direction="incoming",
    )
    chunks = _record_chunks(conn, session, graph, node_id)
    # The same masking rule as the raw and artifact reads. The detail read
    # inherited `format_attrs`, which renders a stored value as it is: a
    # source that recorded `/Users/<name>/inbox/x.pdf` put the operator's home
    # directory on the Fields tab.
    fields, field_keys = mask_paths(fields, at="fields")
    sections, section_keys = mask_paths(sections, at="sections")
    identity, identity_keys = mask_paths(_source_identity(row), at="")
    detail: dict[str, Any] = {
        "found": True,
        "node_id": str(row["node_id"]),
        "type": subtype,
        "label": _display_label(str(row["node_id"]), subtype, attrs),
        **identity,
        "masked_paths": sorted([
            *attr_keys, *field_keys, *section_keys, *identity_keys,
            # A related row's identity is masked too, so the page's own list
            # of what it rewrote has to name those keys as well.
            *outgoing_masked, *incoming_masked,
        ]),
        "masked_paths_rule": PATH_RULE_LIMIT,
        "policy_redacted": (
            _truthy(attrs.get(REDACTION_MARKER))
            or any(field["hidden"] for field in fields)
            or any(section["hidden"] for section in sections)
        ),
        "fields": fields,
        "sections": sections,
        "related": {
            "outgoing": outgoing,
            "incoming": incoming,
            "cap": RELATED_CAP,
            "outgoing_capped": outgoing_more,
            "incoming_capped": incoming_more,
            # Counted over relationships whose edge AND neighbour this reader
            # may see, so a tally never reports a neighbour the lists hide.
            # Null past RELATED_TALLY_CEILING, with the flag saying why.
            "by_relationship": [*outgoing_tally, *incoming_tally],
            "outgoing_total": outgoing_total,
            "incoming_total": incoming_total,
            "outgoing_total_capped": outgoing_total is None,
            "incoming_total_capped": incoming_total is None,
            "tally_ceiling": RELATED_TALLY_CEILING,
        },
    }
    source_names, source_names_reason = _source_names(session)
    detail["source_name"] = source_names.get(str(row.get("source")))
    detail["source_names_unavailable_reason"] = source_names_reason
    # Absent, not empty: a record with no retained chunks has nothing to show,
    # and an empty list would read as "this document has no content".
    if chunks is not None:
        detail["chunks"] = chunks["chunks"]
        detail["chunk_cap"] = chunks["cap"]
        detail["chunks_capped"] = chunks["capped"]
        # Which recorded order this list is in, by name: "chunk_ordinal",
        # "char_offset", or null when no basis covered the whole record and
        # nothing is listed.
        detail["chunk_order_basis"] = chunks["order_basis"]
        # Which edge shape reached them: "member_of" (chunk to document),
        # "contains" (document to chunk), or "both" when the graph records
        # the pair both ways.
        detail["chunk_edge"] = chunks["chunk_edge"]
        detail["chunks_without_ordinal"] = chunks["without_ordinal"]
        detail["chunks_without_ordinal_capped"] = chunks["without_ordinal_capped"]
    return detail
