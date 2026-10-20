"""Search consolidation (M4.2).

One entry point for the three ranked methods (lexical, semantic, and alias).
The MCP server adds the unranked ``attrs`` lookup and identifier-mention
extraction method on the same public operator:

  search(conn, session, query, *, method, filter=None, limit=50)

  method = "lexical"  → generation-pinned text search over the
                        checked-out Chronos graph.
  method = "semantic" → pgvector cosine distance against
                        `graph_nodes.embedding`. Requires an embedder
                        to compute the query vector.
  method = "alias"    → exact match on `okg.aliases` at the
                        pinned catalog_version. Secondary
                        pgvector-similarity score on canonical
                        node's embedding when requested.

All graph hydration/search methods read the Chronos checkpoint for the
session's pinned generation.

**Pin semantics — what the substrate actually guarantees** (M4.5,
Dijkstra review P0 #1):

  Pinned:
    * The RESULT SET (which node_ids pass the filter at this
      pin) is a pure function of the pin. A concurrent publish
      of a later generation cannot make a row appear or
      disappear in a pinned session's results.
    * For `method='alias'`: ranking + scores are pinned too
      (alias is corpus-independent; the catalog_version is
      pinned on the Session).
    * For `method='semantic'`: ranking + scores are pinned to
      the checkpointed embedding values on `okg.graph_nodes`.

Per tasks.md M4.2 acceptance: each method returns ranked results
on fixture queries; `method=alias` prefers exact over similarity.
"""
from __future__ import annotations

import contextlib
import contextvars
import hashlib
import json
import logging
import math
import re
import time
from dataclasses import dataclass, replace
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Sequence, Union

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from okg.substrate.attributes import (
    ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
    DEFAULT_NAMESPACE,
    apply_effective_attrs_to_rows,
)
from okg.substrate.bm25_node_search import (
    RANKED_BM25_MATCH,
    bm25_match_sql,
    drop_repeated_rows,
    branch_point,
    execute_with_custom_plan,
    resolve_node_text_bm25_index,
    steady_bm25_score_sql,
)
from okg.substrate.embeddings import Embedder
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
from okg.substrate.mcp.call_read_cache import cached_read, freeze
from okg.substrate.mcp.envelope import (
    project_row,
    validate_select,
)
from okg.substrate.mcp.evidence import (
    _metadata_for_rows,
    classify_evidence_rows,
    curated_alias_evidence,
)
from okg.substrate.mcp.handle_fields import (
    FIELD_MASKED,
    PRECISION_DATE,
    handle_fields,
    resolve_named_date,
)
from okg.substrate.mcp.result_text import (
    HANDLE_DATE_PRECISION_KEY,
    HANDLE_FIELDS,
    HANDLE_KEY,
    HANDLE_MASKED_KEY,
    LOCATION_KEY,
)
from okg.substrate.mcp.record_text import (
    CODE_CHUNK_KINDS,
    PASSAGE_BASIS_OWN,
    PASSAGE_BASIS_PASSAGE,
    PASSAGE_BASIS_RECORD,
    PASSAGE_LONG_CHARS,
    PASSAGE_SHORT_CHARS,
    ChainedText,
    chunk_location,
    search_record_texts,
    text_passage,
)
from okg.substrate.mcp.handle_fields import normalize_handle_date
from okg.substrate.mcp.provenance import (
    FENCE_CLOSE,
    FENCE_OPEN,
    _fence_attr_value,
    _fence_value,
    escape_embedded_fence_markers,
    is_valid_citation_url,
    serialize_node_read_row,
    serialize_node_read_rows,
)
from okg.substrate.mcp.concept_expansion import (
    ExpandedQuery,
    expand_query,
    expansion_rank,
    tokenize,
)
from okg.substrate.mcp.passage_rollup import (
    PASSAGE_ROLLUP_AGGREGATION,
    PASSAGE_ROLLUP_CHUNK_CAP,
    RANK_BY_PASSAGES,
    RANK_BY_RECORDS,
    RANK_BY_SELF,
    PassageRollupSubtypeRequired,
    assert_rollup_offset_ordered,
    passage_child_declarations,
    fill_passage_rows,
    passage_rollup_candidates,
    fence_position_heading,
    passage_position,
    passage_rollup_declaration,
    resolve_edge_segment,
    resolve_passage_parents,
    validate_rank_by,
)
from okg.substrate.mcp.excerpt_window import (
    ExcerptMatcher,
    build_matcher,
    fallback_matcher,
    window_span,
)
from okg.substrate.mcp.read_affordances import recency_disclosure
from okg.substrate.mcp.grep_query import (
    exact_spans,
    phrase_pattern,
    quoted_spans,
    strip_quotes,
)
from okg.substrate.mcp.record_fold import (
    CHECK_CLASS,
    MATCHING_KEY,
    NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER,
    PHRASE_KEY,
    PHRASE_SPANS_KEY,
    WINNER_KEY,
    RecordsOffsetBeyondExactOrder,
    cutoff,
    declaring_subtypes,
    exact_prefix_length,
    fold_members,
    read_returned_rows,
    resolve_fold_links,
    self_cutoffs,
)
from okg.substrate.mcp.result_diversity import (
    apply_max_per_parent,
    assert_diversity_rank_mode,
)
from okg.substrate.mcp.search_profiles import (
    UNSCOPED,
    ResolvedScope,
    excluded_by_path_globs,
    family_disclosure,
    family_emptiness_disclosure,
    glob_to_like,
    intersect_subtype_filter,
    restrict_scope_to_rank_mode,
    scope_emptiness_disclosure,
)
import okg.substrate.mcp.search_where as search_where
from okg.substrate.mcp.readable_text import keep_readable_text, redaction_possible
from okg.substrate.mcp.record_window_search import validate_window_call
from okg.substrate.mcp.search_views import (
    LIST_ROW_UNIT_KEY,
    refuse_non_record_unit,
)
from okg.substrate.mcp.semantic_readiness import UNKNOWN_PROVENANCE
from okg.substrate.mcp.session import Session, assert_session_matches
from okg.substrate.mcp.tools import _chronos_checkout
from okg.substrate.nomos import read_filter as _read_filter
from okg.substrate.nomos.read_filter import (
    filter_node_rows_for_session,
    filter_rows_by_read_class,
    visible_ids_by_decision_class,
)
from okg.substrate.transform.operators import (
    lower_program_over_chronos,
    semantic_search_program,
)


__all__ = [
    "DEFAULT_PREVIEW_CHARS",
    "MAX_PREVIEW_CHARS",
    "MIN_PREVIEW_CHARS",
    "InvalidPreviewCharBudget",
    "cap_preview_text",
    "search",
    "matches_attr_filters",
    "record_identity",
    "validate_attr_filters",
    "validate_preview_chars",
    "clamp_preview_chars",
]


#: The per-string preview budget (`max_chars`). `DEFAULT_PREVIEW_CHARS` is the
#: value that was hard-coded at three sites before this became a caller
#: control, so the default response is byte-for-byte what it was.
#:
#: `validate_preview_chars` refuses the bounds, for the reason `_validated_budget`
#: refuses `max_bytes`: a silently clamped budget returns a different response
#: from the one the caller asked for. The MCP search tool lowers an over-limit
#: integer instead and says so (`clamp_preview_chars`, issue #3252). The budget counts
#: source text only; trust markers ride on top of it (issue #2670). The floor
#: keeps a preview useful; the ceiling keeps one attribute from consuming a
#: whole response budget on its own.
DEFAULT_PREVIEW_CHARS = 280
MIN_PREVIEW_CHARS = 64
MAX_PREVIEW_CHARS = 4096

#: A fence opener line, as `provenance._fence_value` writes it.
_FENCE_OPENER_LINE = re.compile(r"⟦UNTRUSTED_DATA source=[^\r\n⟧]+⟧")


class InvalidPreviewCharBudget(SubstrateError):
    """`max_chars` is not an integer inside the accepted range."""

    code = "invalid_preview_char_budget"


def validate_preview_chars(value: Any) -> int:
    """Validate `max_chars`. Pure — no connection, no catalog."""
    if value is None:
        return DEFAULT_PREVIEW_CHARS
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidPreviewCharBudget(
            f"search: max_chars must be an integer between "
            f"{MIN_PREVIEW_CHARS} and {MAX_PREVIEW_CHARS}; got "
            f"{type(value).__name__}",
            data={"max_chars": repr(value), "min": MIN_PREVIEW_CHARS,
                  "max": MAX_PREVIEW_CHARS},
            fix_hint=(
                f"pass max_chars between {MIN_PREVIEW_CHARS} and "
                f"{MAX_PREVIEW_CHARS}, or omit it for "
                f"{DEFAULT_PREVIEW_CHARS}"
            ),
        )
    if value < MIN_PREVIEW_CHARS or value > MAX_PREVIEW_CHARS:
        raise InvalidPreviewCharBudget(
            f"search: max_chars must be between {MIN_PREVIEW_CHARS} and "
            f"{MAX_PREVIEW_CHARS}; it is refused rather than silently clamped. "
            f"Got {value}.",
            data={"max_chars": value, "min": MIN_PREVIEW_CHARS,
                  "max": MAX_PREVIEW_CHARS},
            fix_hint=(
                f"pass max_chars between {MIN_PREVIEW_CHARS} and "
                f"{MAX_PREVIEW_CHARS}"
            ),
        )
    return int(value)


def clamp_preview_chars(value: Any) -> tuple[int, Optional[dict[str, int]]]:
    """`max_chars` for the MCP search tool: an over-limit integer is lowered.

    Issue #3252: agents send 4,500 or 5,000 although the schema says 4,096,
    and a refusal costs them the call. An integer above `MAX_PREVIEW_CHARS`
    is lowered to it and the second value says so (`{"requested",
    "applied"}`); the tool serves it as `max_chars_lowered` and a notice, so
    the clamp is never silent. Everything `validate_preview_chars` refuses
    below the ceiling (a non-integer, a value under the floor) is still
    refused.
    """
    if isinstance(value, int) and not isinstance(value, bool) and value > MAX_PREVIEW_CHARS:
        return MAX_PREVIEW_CHARS, {"requested": value, "applied": MAX_PREVIEW_CHARS}
    return validate_preview_chars(value), None


class SearchCompletenessRefused(SubstrateError, ValueError):
    """`completeness` is unknown, or named on a method that cannot honour it."""

    code = "search_completeness_refused"


#: Methods that can read every match of a call or refuse (#3646): lexical
#: reads each scoped input whole; grep and attrs keep a page only when it
#: read every visible match (`require_exact_page`). `bounded`, the current
#: reading of every method, is accepted on all of them.
COMPLETENESS_EXACT_METHODS = ("lexical", "grep", "attrs")


def validate_completeness(value: Any, *, method: str) -> None:
    """Refuse an unknown `completeness`, or `exact` on a method that cannot
    read every match.

    Pure. An option a method cannot honour is refused rather than ignored,
    the rule `expand_concepts` follows: a caller who asked for exact recall
    and got an ordinary page would believe it was exact.
    """
    if value is None:
        return
    data = {"completeness": repr(value), "method": method,
            "values": list(COMPLETENESS_VALUES),
            "supported_methods": list(COMPLETENESS_EXACT_METHODS)}
    if value not in COMPLETENESS_VALUES:
        raise SearchCompletenessRefused(
            f"search: completeness must be one of {list(COMPLETENESS_VALUES)}; "
            f"got {value!r}",
            data=data,
            fix_hint="pass completeness 'bounded' or 'exact', or omit it",
        )
    if value == COMPLETENESS_BOUNDED:
        return
    if method not in COMPLETENESS_EXACT_METHODS:
        raise SearchCompletenessRefused(
            f"search: completeness='exact' reads every match; method={method!r} "
            "has no such set (it ranks by similarity or reads your query text)",
            data=data,
            fix_hint=(
                "omit completeness, or search the same text with "
                "method='lexical' or method='grep' and completeness='exact'"
            ),
        )


#: `completeness` on a served grep or attrs page under `exact` whose match set
#: does not fit one page (issue #3666): the pages together are every match.
COMPLETENESS_EXACT_OVER_PAGES = "exact_over_pages"

#: Why an exact listing is, or is not yet, every match (`listing.reason`).
LISTING_MORE_MATCHES = "more_matches"
LISTING_SCAN_BOUND = "scan_bound"
LISTING_LAST_PAGE = "last_page"
LISTING_REASONS = (LISTING_MORE_MATCHES, LISTING_SCAN_BOUND, LISTING_LAST_PAGE)


def grep_narrowing_hint(*, has_type: bool, has_path_prefix: bool) -> str:
    """How to narrow a grep whose exact listing cannot continue, naming only
    what the call does not already have. Pure."""
    ways = ["a rarer term"]
    if not has_path_prefix:
        ways.append("path_prefix")
    if not has_type:
        ways.append("a where on type")
    return (
        "narrow the search with " + " or ".join(ways) + " so grep reads every "
        "match, or omit completeness for this bounded page"
    )


def _over_pages(page: dict[str, Any], *, complete: bool, reason: str) -> None:
    page["completeness"] = COMPLETENESS_EXACT_OVER_PAGES
    page["listing"] = {"complete": complete, "reason": reason}


#: What checks every record against a lexical row filter: a fact, not a call
#: (filter cannot apply the search text), issue #3666.
WHERE_EVERY_RECORD = "filter with the same where checks every record, without the text match"

#: A different match set that reads every literal occurrence of the text:
#: grep matches characters, not BM25 words (issue #3666).
LITERAL_TEXT_MATCH_CALL = {"method": "grep", "completeness": "exact"}


def _cut_exact_listing(page: dict[str, Any], *, withheld: bool) -> None:
    """Mark an exact lexical page whose read was not exhaustive (#3666).

    `completeness` stays `"exact"`, the requested mode. The page is a
    listing that stops at a scan bound (`listing.reason: "scan_bound"`,
    `complete: false`): lexical reads at most its ranked cap of candidates
    and pages no further. The bound is named as `ranked_cap` when the page
    has one, else by its retrieval bounds (a work cap); under enforced
    access rules (`withheld`) neither is named, since a filled raw read
    would say hidden rows matched."""
    listing: dict[str, Any] = {"complete": False, "reason": LISTING_SCAN_BOUND}
    if not withheld:
        cap = page.get("ranked_cap")
        bounds = ((page.get("coverage") or {}).get("retrieval") or {}).get("bounds")
        if isinstance(cap, dict) and cap:
            listing["ranked_cap"] = dict(cap)
        elif bounds:
            listing["bounds"] = list(bounds)
    listing["literal_text_match"] = dict(LITERAL_TEXT_MATCH_CALL)
    page["listing"] = listing


def require_exact_page(
    page: dict[str, Any], *, method: str, continued: bool = False,
    has_type: bool = False, has_path_prefix: bool = False,
) -> None:
    """Mark a grep or attrs page read under `completeness="exact"`, or
    refuse it when no paging can deliver every match (#3646, #3666).

    A page that is the whole set, and a first grep page that read and
    counted every match, says `completeness: "exact"`, as before. A page of
    a set that continues, or a later page of one, is served and says
    `exact_over_pages` with `listing: {"complete", "reason"}`: the pages,
    followed by `next_offset` or the continuation token until `complete` is
    true, are every match. One page never says it is everything.

    Refused, because following the page's continuation cannot reach the
    rest: an attrs page that stopped at the access-filter scan bound or the
    offset ceiling; a grep page that timed out, stopped at a bound with no
    continuation, or lost a row to the serve-time read check; and a first
    grep page that read every match but could not count them.
    """
    if method == "grep":
        stopped = page.get("page_basis") != "top_of_all_visible_matches"
        if (
            not continued and not stopped
            and page.get("total_count_basis") == "exact"
            and not page.get("recheck_dropped")
        ):
            page["completeness"] = COMPLETENESS_EXACT
            return
        follows = (
            page.get("next_offset") is not None or bool(page.get("continuation_token"))
        )
        if page.get("recheck_dropped"):
            bound = "rows dropped by the read check"
        elif page.get("page_basis") == "timed_out":
            bound = "timed_out"
        elif page.get("continuation_withheld") is not None:
            bound = f"continuation withheld ({page['continuation_withheld']})"
        elif follows and (continued or stopped):
            _over_pages(
                page, complete=False,
                reason=LISTING_SCAN_BOUND if stopped else LISTING_MORE_MATCHES,
            )
            return
        elif continued and not stopped:
            _over_pages(page, complete=True, reason=LISTING_LAST_PAGE)
            return
        elif stopped:
            bound = str(page.get("page_basis"))
        else:
            reasons = page.get("count_basis_reasons") or []
            bound = f"count {page.get('total_count_basis')}" + (
                f" ({', '.join(map(str, reasons))})" if reasons else ""
            )
        hint = grep_narrowing_hint(
            has_type=has_type,
            has_path_prefix=has_path_prefix or page.get("path_prefix") is not None,
        )
    else:
        offset = page.get("offset") or 0
        if offset == 0 and not page.get("truncated") and page.get("next_offset") is None:
            page["completeness"] = COMPLETENESS_EXACT
            page["total_count"] = len(page.get("results") or [])
            page["total_count_basis"] = "exact"
            return
        if page.get("next_offset") is not None:
            _over_pages(page, complete=False, reason=LISTING_MORE_MATCHES)
            return
        if not page.get("truncated") and not page.get("pagination_capped"):
            _over_pages(page, complete=True, reason=LISTING_LAST_PAGE)
            return
        bound = (
            "read_filter_scan_bound"
            if page.get("read_filter_scan_bound") is not None else "offset_ceiling"
        )
        hint = (
            "narrow the where (a type, or another exact attrs.<key> equality) "
            "so the listing can be paged to its end, or omit completeness for "
            "this bounded page"
        )
    raise SearchCompletenessRefused(
        f"search: completeness='exact' cannot be delivered for this "
        f"method={method!r} call, on this page or by paging ({bound})",
        data={"completeness": "'exact'", "method": method, "bound": bound},
        fix_hint=hint,
    )


def _grep_passage_type(
    conn: psycopg.Connection, session: Session, residual: Optional[dict[str, Any]],
) -> Optional[tuple[str, Optional[dict[str, Any]], str]]:
    """`(passage type, the rest of the where, the clause as sent)` when a
    grep `where` names one declared passage type by one top-level `type = X`
    or `type in [X]` (the two read the same, review L1) and no other clause
    reads `type`; else None (issue #3666)."""
    conjuncts = search_where.top_conjuncts(residual)
    typed = [c for c in conjuncts if search_where.names_field(c, "type")]
    if len(typed) != 1 or typed[0].get("field") != "type":
        return None
    clause = typed[0]
    if clause.get("op") == "eq":
        name, sent = clause.get("value"), "where type = `{}`"
    elif clause.get("op") == "in" and len(clause.get("values") or ()) == 1:
        name, sent = clause["values"][0], "where type in [`{}`]"
    else:
        return None
    if not isinstance(name, str) or not passage_child_declarations(
        conn, catalog_version_id=session.catalog_version_id, child_subtypes=[name],
    ):
        return None
    rest = search_where.and_clauses(*(c for c in conjuncts if c is not clause))
    return name, rest, sent.format(name)


def validate_attr_filters(af: Any) -> None:
    """Validate the v1 ``attr_filters`` shape (shared by ``search()`` and the
    MCP tool layer's query-less attrs lookup, per ``expose-mcp-node-lookup``).

    v1 accepts an exact-match dict only: ``dict[str, str|int|bool|None]``.
    ``None`` means "absent or JSON null" (the Python matcher's semantics).
    Raises ``ValueError`` on any other shape.
    """
    if af is None:
        return
    if not isinstance(af, dict):
        raise InvalidOperatorArguments(
            "search: attr_filters must be a dict[str, "
            "str|int|bool|None] (exact-match only in v1); "
            f"got {type(af).__name__}"
        )
    for k, v in af.items():
        if not isinstance(k, str):
            raise InvalidOperatorArguments(
                f"search: attr_filters keys must be str; got {k!r}"
            )
        if not isinstance(v, (str, int, bool, type(None))):
            raise InvalidOperatorArguments(
                "search: attr_filters values must be str|int|"
                f"bool|None (exact match only in v1); got "
                f"{type(v).__name__} for key {k!r}. Range "
                "queries / regex are out of scope for v1."
            )
        # NUL can never match stored data (sanitize-fact-text-at-admission
        # scrubs/rejects it before persistence) and would otherwise reach
        # Postgres as a raw unicode-escape error via Jsonb(...). Reject it
        # loudly here instead.
        if "\x00" in k or (isinstance(v, str) and "\x00" in v):
            raise InvalidOperatorArguments(
                "search: attr_filters keys/values must not contain NUL "
                f"(U+0000); got one in key {k!r}. Stored fact text never "
                "carries NUL (it is scrubbed or rejected at admission)."
            )


def matches_attr_filters(
    row: dict[str, Any],
    *,
    subtype: Optional[str] = None,
    attr_filters: Optional[dict[str, Any]] = None,
) -> bool:
    """Public form of the residual subtype + attr_filters row matcher.

    Evaluates over the row's (effective) ``attrs``: a filter value of
    ``None`` matches an absent key OR a stored JSON null, mirroring
    ``dict.get``. Used by the MCP tool layer's typed attrs lookup so the
    residual re-check shares one source of truth with search's own
    filtering (``expose-mcp-node-lookup``).
    """
    return _matches_filter(
        row, {"subtype": subtype, "attr_filters": attr_filters or {}},
    )


# ---------- the reading keys of a search row (PACT read-passage-rows-3666) ----------
#
# Every search row (lexical, semantic, grep; card or list) carries `head`, the
# record's per-type fields and title, and `read_more`, the exact call that
# reads it; a card row also carries `passage`, the text around its match. A
# `select` adds to a row and never removes them. Values are read from the
# row's effective, Nomos-filtered attrs and its subtype's catalog
# declarations; producer text in them is fenced on an untrusted row exactly
# as `excerpt` is. The key names and one example each are the payload
# contract the text view renders.

#: The `attrs.<key>` names of the running search's `select` (D6), set by
#: `search()` around its method dispatch so every method's rows, grep's
#: included, resolve them where their effective attrs are at hand.
_ATTR_SELECT: contextvars.ContextVar[tuple[str, ...]] = contextvars.ContextVar(
    "okg_search_attr_select", default=(),
)

ROW_HEAD_KEY = "head"
PASSAGE_KEY = "passage"
READ_MORE_KEY = "read_more"
SELECTED_KEY = "selected"
#: The keys a row keeps whatever `select` names.
READING_KEYS = (ROW_HEAD_KEY, PASSAGE_KEY, READ_MORE_KEY, SELECTED_KEY)

_CHUNK_SUBTYPE = "document_chunk"
#: Head fields holding a server-normalized date (never producer text).
_DATE_FIELDS = frozenset({"created", "updated", "resolved", "date", "modified"})
#: Prefix of a selected attribute (`attrs.<key>`, as `where` names one).
ATTR_SELECT_PREFIX = "attrs."
_DAY_PREFIX = re.compile(r"\d{4}-\d{2}-\d{2}(?:$|[T ])")


def split_attr_select(select: Any) -> tuple[list[str], Any]:
    """`(attribute keys, other names)` of a search `select` (D6).

    `attrs.<key>` names one attribute, as `where` does; the rest are row
    fields, validated as before. A select naming only attributes keeps the
    default row (select adds, it never strips the passage): its other names
    come back as None."""
    if not isinstance(select, (list, tuple)):
        return [], select
    keys: list[str] = []
    rest: list[Any] = []
    for name in select:
        if (
            isinstance(name, str) and name.startswith(ATTR_SELECT_PREFIX)
            and name[len(ATTR_SELECT_PREFIX):]
        ):
            key = name[len(ATTR_SELECT_PREFIX):]
            if key not in keys:
                keys.append(key)
        else:
            rest.append(name)
    if keys and not rest:
        return keys, None
    return keys, rest if keys else select


def _attr_text(attrs: Mapping[str, Any], *names: Optional[str]) -> Optional[str]:
    for name in names:
        if not name:
            continue
        value = attrs.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
    return None


def _attr_day(attrs: Mapping[str, Any], *names: Optional[str]) -> Optional[str]:
    for name in names:
        if not name or attrs.get(name) in (None, ""):
            continue
        raw = attrs.get(name)
        iso = normalize_handle_date(raw)
        if iso:
            return iso[:10]
        # A day the date parser does not take whole (an offset written
        # `+0200`) still names its day in its first ten characters.
        if isinstance(raw, str) and _DAY_PREFIX.match(raw.strip()):
            return raw.strip()[:10]
    return None


def _attr_int(attrs: Mapping[str, Any], name: str) -> Optional[int]:
    value = attrs.get(name)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def declares_chunk_passages(meta: Mapping[str, Any]) -> bool:
    """The subtype's catalog declares `document_chunk` passages: its text is
    read with `inspect` kind text."""
    rollup = (meta or {}).get("passage_rollup")
    return isinstance(rollup, Mapping) and rollup.get("child_subtype") == _CHUNK_SUBTYPE


def row_kind(subtype: str, attrs: Mapping[str, Any], meta: Mapping[str, Any]) -> str:
    """The row's head kind, from generic attribute names and the catalog."""
    if subtype == _CHUNK_SUBTYPE:
        return "doc_passage"
    if attrs.get("qualified_name") or attrs.get("signature"):
        return "code"
    if attrs.get("commit_sha"):
        return "commit"
    if (attrs.get("status") or attrs.get("state")) and (
        attrs.get("created") or attrs.get("created_at")
    ):
        return "ticket"
    if subtype == "document":
        return "doc"
    if attrs.get("path") and (meta or {}).get("path_from"):
        return "file"
    return subtype


def _heading(value: Any) -> Optional[str]:
    if isinstance(value, list):
        parts = [str(part).strip() for part in value if str(part).strip()]
        return " > ".join(parts) or None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def row_head(full: Mapping[str, Any], meta: Mapping[str, Any]) -> dict[str, Any]:
    """The row's head: kind, id, per-type fields in display order, title and
    citation url (`ROW_HEAD_KEY`)."""
    attrs = full.get("attrs") if isinstance(full.get("attrs"), Mapping) else {}
    meta = meta or {}
    subtype = str(full.get("subtype") or "")
    kind = row_kind(subtype, attrs, meta)
    fields: list[list[str]] = []

    def add(name: str, value: Optional[str]) -> None:
        if value:
            fields.append([name, value])

    handle = handle_fields(meta, attrs)
    title = handle.title.value
    created_from = meta.get("created_at_from")
    modified_from = meta.get("last_modified_at_from")
    start_line = _attr_int(attrs, "start_line")
    end_line = _attr_int(attrs, "end_line")
    if kind == "ticket":
        add("status", _attr_text(attrs, "status", "state"))
        add("created", _attr_day(attrs, "created", "created_at", created_from))
        add("updated", _attr_day(attrs, "updated", "updated_at", modified_from, "last_modified_at"))
        add("resolved", _attr_day(attrs, "resolved", "resolved_at", "closed_at"))
    elif kind == "commit":
        sha = _attr_text(attrs, "commit_sha")
        add("sha", sha[:7] if sha else None)
        add("repo", _attr_text(attrs, "repo"))
        add("date", _attr_day(attrs, "authored_at", created_from, "committed_at"))
        add("author", _attr_text(attrs, "author_name", "author"))
        title = _attr_text(attrs, "message_subject") or title
    elif kind == "code":
        place = ":".join(
            part for part in (_attr_text(attrs, "repo"), _attr_text(attrs, "path")) if part
        )
        if place and start_line is not None:
            place += f":{start_line}"
            if end_line is not None and end_line != start_line:
                place += f"-{end_line}"
        add("path", place or None)
        title = _attr_text(attrs, "qualified_name") or title
    elif kind == "doc_passage":
        parent = full.get("parent") if isinstance(full.get("parent"), Mapping) else {}
        add("document", (
            _attr_text(attrs, "doc_path")
            or (parent.get("title") if isinstance(parent.get("title"), str) else None)
            or (str(parent["node_id"]) if parent.get("node_id") else None)
        ))
        heading = _heading(attrs.get("heading_path"))
        add("section", f"§ {heading}" if heading else None)
        if start_line is not None:
            add("lines", f"{start_line}-{end_line}" if end_line else str(start_line))
        title = None
    elif kind == "file":
        path = _attr_text(attrs, meta.get("path_from"), "path")
        repo = _attr_text(attrs, "repo")
        add("path", f"{repo}:{path}" if repo and path else path)
        add("modified", _attr_day(attrs, modified_from, "last_modified_at"))
    else:
        add("path", handle.path.value)
        add("date", handle.date.value[:10] if handle.date.value else None)
    citation_url = None
    parent = full.get("parent")
    parent_citation = parent.get("citation") if isinstance(parent, Mapping) else None
    for url in (
        attrs.get("citation_url"),
        attrs.get("url"),
        parent_citation.get("url") if isinstance(parent_citation, Mapping) else None,
    ):
        if is_valid_citation_url(url):
            citation_url = url
            break
    return {
        "kind": kind,
        "id": str(full.get("node_id")),
        "fields": fields,
        "title": title,
        "citation_url": citation_url,
    }


def row_read_more(full: Mapping[str, Any], meta: Mapping[str, Any]) -> dict[str, Any]:
    """The exact call that reads this row's record (`READ_MORE_KEY`): kind
    text for a record whose text is in chunks (a chunk row: its record when
    known), kind node otherwise."""
    node_id = str(full.get("node_id"))
    subtype = str(full.get("subtype") or "")
    if subtype == _CHUNK_SUBTYPE:
        parent = full.get("parent")
        parent_id = parent.get("node_id") if isinstance(parent, Mapping) else None
        target = {"kind": "text", "node_id": str(parent_id or node_id)}
    elif declares_chunk_passages(meta):
        target = {"kind": "text", "node_id": node_id}
    else:
        target = {"kind": "node", "node_id": node_id}
    return {"tool": "inspect", "arguments": {"target": target}}


def row_passage(
    full: Mapping[str, Any],
    meta: Mapping[str, Any],
    *,
    matcher: Optional[ExcerptMatcher],
    chained: Optional[ChainedText] = None,
    winner: Optional[Mapping[str, Any]] = None,
    chars: Optional[int] = None,
    anchor_required: bool = True,
) -> Optional[dict[str, Any]]:
    """The row's matched passage (`PASSAGE_KEY`), or None when no query word
    is in its text.

    Tried in order, the first with a match wins: the record's whole chunk
    text (`chained`), its winning chunk (`winner`), its own `text`. A commit
    shows its body (`message_body`). `chars` is the caller's `max_chars`
    (D2): it replaces the per-type sizes."""
    attrs = full.get("attrs") if isinstance(full.get("attrs"), Mapping) else {}
    subtype = str(full.get("subtype") or "")
    kind = row_kind(subtype, attrs, meta or {})
    code = kind == "code" or str(attrs.get("chunk_kind") or "") in CODE_CHUNK_KINDS
    budget = (
        PASSAGE_LONG_CHARS
        if code or kind in ("doc_passage", "doc", "file") else PASSAGE_SHORT_CHARS
    )
    if kind == "commit":
        return text_passage(
            _attr_text(attrs, "message_body") or "", matcher, budget=PASSAGE_SHORT_CHARS,
            basis=PASSAGE_BASIS_OWN, body=True, anchor_required=False, chars=chars,
        )
    first_line, last_line = _attr_int(attrs, "start_line"), _attr_int(attrs, "end_line")
    tries: list[tuple[str, str, Any, Optional[int], Optional[int]]] = []
    if chained is not None:
        tries.append((chained.text, PASSAGE_BASIS_RECORD, chained.chunk_at, None, None))
    if winner is not None:
        winner_attrs = winner.get("attrs") if isinstance(winner.get("attrs"), Mapping) else {}
        winner_id = str(winner.get("node_id"))
        tries.append((
            str(winner_attrs.get("text") or ""), PASSAGE_BASIS_PASSAGE,
            lambda _offset, winner_id=winner_id: winner_id,
            _attr_int(winner_attrs, "start_line"), _attr_int(winner_attrs, "end_line"),
        ))
    own_text = attrs.get("text")
    if not (isinstance(own_text, str) and own_text.strip()) and kind == "code":
        own_text = "\n".join(
            part for part in (_attr_text(attrs, "signature"), _attr_text(attrs, "docstring"))
            if part
        )
    own_chunk = (lambda _offset, node_id=str(full.get("node_id")): node_id) if (
        subtype == _CHUNK_SUBTYPE
    ) else None
    tries.append((
        own_text if isinstance(own_text, str) else "", PASSAGE_BASIS_OWN, own_chunk,
        first_line, last_line,
    ))
    for index, (text, basis, chunk_at, first, last) in enumerate(tries):
        last_try = index == len(tries) - 1
        passage = text_passage(
            text, matcher, budget=budget, basis=basis, code=code,
            anchor_required=anchor_required or not last_try,
            first_line=first, last_line=last,
            signature=_attr_text(attrs, "signature") if kind == "code" else None,
            docstring=_attr_text(attrs, "docstring") if kind == "code" else None,
            chunk_at=chunk_at, chars=chars,
        )
        if passage is not None:
            return passage
    return None


def _attach_reading_keys(
    row: dict[str, Any],
    full: Mapping[str, Any],
    meta: Mapping[str, Any],
    *,
    attr_keys: Sequence[str] = (),
) -> None:
    """Put `head`, `passage`, `read_more` and `selected` on a serialized row,
    fenced by the row's trust."""
    head = row_head(full, meta)
    passage = full.get(PASSAGE_KEY)
    passage = dict(passage) if isinstance(passage, Mapping) else None
    attrs = full.get("attrs") if isinstance(full.get("attrs"), Mapping) else {}
    selected = {
        f"{ATTR_SELECT_PREFIX}{key}": attrs[key] for key in attr_keys if key in attrs
    }
    provenance = row.get("provenance") if isinstance(row.get("provenance"), Mapping) else {}
    if provenance.get("trust") == "untrusted":
        source = provenance.get("source")
        head["fields"] = [
            [name, value if name in _DATE_FIELDS else _fence_value(value, source=source)]
            for name, value in head["fields"]
        ]
        head["title"] = _fence_value(head["title"], source=source)
        if passage is not None:
            passage["text"] = _fence_value(passage["text"], source=source)
            passage["author"] = _fence_value(passage.get("author"), source=source)
        selected = {key: _fence_attr_value(value, source=source) for key, value in selected.items()}
    row[ROW_HEAD_KEY] = head
    if PASSAGE_KEY in full:
        row[PASSAGE_KEY] = passage
    row[READ_MORE_KEY] = row_read_more(full, meta)
    if attr_keys:
        row[SELECTED_KEY] = selected
        if any(key not in attrs for key in attr_keys):
            # D6: a selected key this record lacks; the reply's unknown-key
            # notice names the keys it has (sorted, at most
            # `_ATTR_KEYS_SHOWN`). Only on such a row, so a page that has
            # every key it asked for carries no key list.
            row[ATTR_KEYS_KEY] = sorted(str(key) for key in attrs)[:_ATTR_KEYS_SHOWN]


#: The row key listing a record's attribute names when a selected
#: `attrs.<key>` is missing on it (D6), and how many it lists.
ATTR_KEYS_KEY = "attr_keys"
_ATTR_KEYS_SHOWN = 24


def _fence_search_rows(
    conn: psycopg.Connection,
    session: Session,
    full_rows: list[dict[str, Any]],
    *,
    select: Optional[list[str]],
    default: tuple[str, ...],
    always: tuple[str, ...],
    scope: Optional[ResolvedScope] = None,
    max_chars: int = DEFAULT_PREVIEW_CHARS,
    metadata: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Project each row, then attach provenance + fence untrusted attrs.

    Each `full_rows` entry carries `source` (the node's producer_id);
    project_row drops it, so it is re-attached before the trust pass
    (PACT fence-untrusted-node-text).

    `source_family` is attached as a TOP-LEVEL sibling rather than inside
    `provenance`, and that placement is a decision, not a convenience. The
    provenance block is not in `_LEXICAL_DEFAULT`, so a family label buried
    there would be invisible on the very card a family-filtered search returns
    — a caller who filtered by authority would have to make a second call with
    a widened `select` to see which authority answered. The field is explicitly
    null (never absent) when the deployment declares no families, and the
    envelope says `source_families_declared: false` once so the null cannot be
    read as "this producer is unclassified".
    """
    metadata = _metadata_for_rows(conn, session, full_rows, metadata=metadata)
    if select is None or HANDLE_KEY in select:
        full_rows = _with_handles(conn, session, full_rows, metadata=metadata)
    classified = classify_evidence_rows(conn, session, full_rows, metadata=metadata)
    projected = [
        {
            **project_row(r, select, default=default, always=always),
            "source": r.get("source"),
        }
        for r in classified
    ]
    families = [
        (scope.family_of(r.get("source")) if scope is not None else None)
        for r in classified
    ]
    # Trust reads the FULL attrs: a card that did not select `attrs` must still
    # see the untrusted-transcript marker (issue #2689).
    serialized = serialize_node_read_rows(
        conn, projected, deployment_name=session.deployment_name,
        marker_attrs=[r.get("attrs") for r in classified],
    )
    for row, family, full in zip(serialized, families, classified):
        excerpt = row.get("excerpt")
        if isinstance(excerpt, str):
            row["excerpt"] = _cap_serialized_excerpt(
                excerpt, max_chars=max_chars,
            )
        if select is None or "source_family" in (select or []):
            row["source_family"] = family
        fence_position_heading(row, attrs=full.get("attrs"))
        _attach_reading_keys(
            row, full, metadata.get(str(full.get("subtype"))) or {},
            attr_keys=_ATTR_SELECT.get(),
        )
    return serialized


def _with_handles(
    conn: psycopg.Connection,
    session: Session,
    rows: list[dict[str, Any]],
    *,
    metadata: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Each row with its handle (`HANDLE_KEY`) when it has anything to say.

    The handle carries optional title/path/date/masked and date provenance:
    a field is present only when the record has it, and `masked` lists the
    fields the read filter hid (`handle_fields.FIELD_MASKED`), so hiding
    stays distinguishable from absence. A record with none of them gets no
    handle key at all, so it costs no bytes.

    PACT read-model-s2-handle (design `read-model` sections 3.6 and 5): the
    text view's handle line needs a record's title, path and date, which the
    default projection does not carry (`attrs` is not in `_LEXICAL_DEFAULT`).
    Read here from the row's attrs as the reader may see them (the
    Nomos-filtered effective attrs this module already reads `position`
    from), through the subtype's catalog declarations
    (`handle_fields.handle_fields`), so a masked or dropped attribute is never
    a title. Title and path are producer text: on an untrusted row
    `serialize_node_read_row` fences them like an excerpt. The date is the
    server's ISO-8601 normalization, a closed grammar. One catalog read per
    page. Returns new row dicts; the input rows are not changed.
    """
    if not rows:
        return rows
    metadata = _metadata_for_rows(conn, session, rows, metadata=metadata)
    out: list[dict[str, Any]] = []
    for row in rows:
        row_metadata = metadata.get(str(row.get("subtype")))
        fields = handle_fields(row_metadata, row.get("attrs"))
        handle: dict[str, Any] = {}
        masked: list[str] = []
        for name in HANDLE_FIELDS:
            field = getattr(fields, name)
            if field.value is not None:
                handle[name] = field.value
            elif field.status == FIELD_MASKED:
                masked.append(name)
        if masked:
            handle[HANDLE_MASKED_KEY] = masked
        if fields.date.value is not None or fields.date.status == FIELD_MASKED:
            handle["date_meaning"] = fields.date_meaning
            handle["date_attr"] = fields.date.attr
        if fields.date.value is not None:
            # The display date of a day-only source is written as that day's
            # midnight; only the catalog contract's `precision` tells it apart
            # from a source that wrote 00:00:00 (PACT read-model-s1-catalog-
            # fields). `handle_fields` does not return it, so read the same
            # declared date through `resolve_named_date`, which never
            # substitutes another meaning. Only the day case is marked: a
            # date without the key names a time of day, the common case, so it
            # costs no bytes. Timezone state is not repeated: an aware value
            # carries its offset, a naive one has none.
            named = resolve_named_date(
                row_metadata, row.get("attrs"), fields.date_meaning,
            )
            if named.value != fields.date.value or named.precision is None:
                raise RuntimeError(
                    "handle date and its named date disagree for "
                    f"{fields.date_meaning!r} on attribute {fields.date.attr!r}"
                )
            if named.precision == PRECISION_DATE:
                handle[HANDLE_DATE_PRECISION_KEY] = PRECISION_DATE
        out.append({**row, HANDLE_KEY: handle} if handle else row)
    return out


def _cap_serialized_excerpt(
    value: str, *, max_chars: int = DEFAULT_PREVIEW_CHARS,
) -> str:
    """Bound a preview to `max_chars` of SOURCE text, preserving fence closure.

    The trust markers do not count against the budget (issue #2670): a
    fenced value keeps its opener and closer whole and only the text between
    them is measured and cut, so an untrusted excerpt carries as much source
    text as a trusted one. Charging the markers left a 280-character
    untrusted excerpt with about 200 characters of text.

    A NAIVE slice is unsafe on fenced text and this is the function that exists
    because of it: cutting can drop the closing `FENCE_CLOSE` line, leaving an
    unterminated untrusted block whose remaining text then reads as trusted
    content to whatever consumes the response. So the opener and the closer
    are preserved and only the BODY is shortened.

    A value counts as fenced only when its first line is a well-formed opener
    and its last line is the closer, split on `\n` alone (the separator
    `_fence_value` writes; `splitlines` would also split on characters a
    producer label may carry). Anything else -- including trusted text that
    merely ends in a closer line -- is cut to `max_chars` in total, so the
    body-only rule cannot be borrowed by an unfenced string.

    The body is escaped BEFORE the cut. `_fence_value` escapes embedded
    markers when it wraps, so a correctly fenced value carries none -- but a
    value that arrives already fenced (idempotent re-entry), or one whose
    producer text contained a forged closer, must not be able to end the block
    early inside the shortened body. Escaping first keeps the kept body within
    budget, and a prefix of escaped text cannot form a marker.
    """
    lines = value.split("\n")
    if (
        len(lines) >= 3
        and lines[-1] == FENCE_CLOSE
        and _FENCE_OPENER_LINE.fullmatch(lines[0]) is not None
    ):
        opening = lines[0]
        body = escape_embedded_fence_markers("\n".join(lines[1:-1]))
        if len(opening) > max_chars:
            # An extreme producer label would make the markers, not the text,
            # the bulk of the preview; the marker size stays bounded.
            opening = FENCE_OPEN.format(source="bounded")
        elif len(body) <= max_chars and body == "\n".join(lines[1:-1]):
            return value
        if len(body) > max_chars:
            body = body[: max_chars - 3] + "..."
        return f"{opening}\n{body}\n{FENCE_CLOSE}"
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 3] + "..."


def cap_preview_text(
    value: str, *, max_chars: int = DEFAULT_PREVIEW_CHARS,
) -> str:
    """Public form of the fence-preserving cut, for the preview budget.

    The attribute preview budget (`server._truncate_attr_value`) MUST use this
    rather than slicing: attribute values on an untrusted node are fenced by
    `provenance._fence_attrs` before they reach it, and a plain slice through
    one produces an unterminated untrusted block.
    """
    return _cap_serialized_excerpt(value, max_chars=max_chars)


# Per-method selectable fields. The public `search()` dispatches
# to one of three implementations; each has its own row shape.
_LEXICAL_FIELDS: tuple[str, ...] = (
    "node_id", "canonical_node_id", "subtype", "attrs", "text_hash", "score",
    "method", "match_kind", "excerpt", "provenance", "citation",
    "evidence_kind", "evidence_kind_provenance", "chunk_kind", "recency",
    "source_record_id", "source_revision", "parent", "rollup_best_chunk_id",
    "matching_passages", "source_family", "position", HANDLE_KEY,
    LOCATION_KEY, "phrase_match",
)
_LEXICAL_DEFAULT: tuple[str, ...] = (
    "node_id", "canonical_node_id", "subtype", "score", "method",
    "match_kind", "excerpt", "citation", "evidence_kind",
    "evidence_kind_provenance", "chunk_kind", "recency",
    "source_record_id", "source_revision", "parent", "rollup_best_chunk_id",
    "matching_passages", "source_family", "position", HANDLE_KEY,
    LOCATION_KEY, "phrase_match",
)
# ALWAYS is the identity floor a caller cannot project away; match context is
# rich but optional, so it stays in DEFAULT. A caller that narrows `select` to
# keep responses small must actually get a small response.
_LEXICAL_ALWAYS: tuple[str, ...] = (
    "node_id", "subtype", "score", "evidence_kind",
    "evidence_kind_provenance",
    # Present only on a quoted search; a narrowed `select` keeps it, so a
    # small response still says which rows hold the quoted text (PACT
    # lexical-quoted-phrases-3064).
    "phrase_match",
)

_SEMANTIC_FIELDS: tuple[str, ...] = (
    "node_id", "subtype", "attrs", "text_hash", "distance", "provenance",
    "embedding_provenance", "evidence_kind", "evidence_kind_provenance",
    "chunk_kind", "source_record_id", "source_revision", "parent",
    "source_family", "position", HANDLE_KEY,
)
# `embedding_provenance` is in the identity floor a caller cannot project away:
# a row ranked against an unverified vector must not be able to present itself
# as a verified one just because the caller narrowed `select` (issue #797).
_SEMANTIC_ALWAYS: tuple[str, ...] = (
    "node_id", "subtype", "distance", "embedding_provenance",
    "evidence_kind", "evidence_kind_provenance",
)

_ALIAS_FIELDS: tuple[str, ...] = (
    "alias", "canonical", "canonical_node_id", "subtype", "scope",
    "match_kind", "method", "excerpt", "node", "evidence_kind",
    "evidence_kind_provenance", "chunk_kind", "source_family", HANDLE_KEY,
)
#: Selected only (issue #3666): the id of the record the alias names, under
#: the name every other search row gives a record's id. The same value as
#: `canonical_node_id`. Not in the default row, which is unchanged.
_ALIAS_SELECT_ONLY: tuple[str, ...] = ("node_id",)
_ALIAS_SELECTABLE: tuple[str, ...] = (*_ALIAS_FIELDS, *_ALIAS_SELECT_ONLY)
_ALIAS_ALWAYS: tuple[str, ...] = (
    "alias", "canonical", "match_kind", "evidence_kind",
    "evidence_kind_provenance",
)


#: The query-less attribute lookup's selectable fields (GitHub #641; the
#: lookup itself is `server._search_attrs_lookup`). They mirror lexical
#: minus the ranking columns.
ATTRS_LOOKUP_FIELDS: tuple[str, ...] = (
    "node_id",
    "subtype",
    "attrs",
    "text_hash",
    "provenance",
    "evidence_kind",
    "evidence_kind_provenance",
    "chunk_kind",
    "source_record_id",
    "source_revision",
    HANDLE_KEY,
    # Served as lexical serves them, selected only (#3646): the row's own
    # id, its source-supplied citation, and its producer's family label.
    "canonical_node_id",
    "citation",
    "source_family",
)

_VALID_METHODS = frozenset({"lexical", "semantic", "alias", "grep"})

#: Lexical retrieval modes (PACT lexical-index-scan-cap-3162, policy option
#: A). `bounded`, the default, serves the native BM25 candidates and says they
#: may be incomplete: one index scan can stop at its 100,000-entry cap before
#: scope filters run. `exact` reads every short read's whole scoped input
#: within a work bound, or refuses.
COMPLETENESS_BOUNDED = "bounded"
COMPLETENESS_EXACT = "exact"
COMPLETENESS_VALUES = (COMPLETENESS_BOUNDED, COMPLETENESS_EXACT)
#: Zero-hit diagnoses inferred from the ranked read matching nothing; a
#: bounded read may have been cut, so it cannot support them.
_READ_DEPENDENT_ZERO_HITS = frozenset({"no_match", "not_tokenized"})
#: What a bounded response tells the caller to do to learn more: rerun the
#: same search with these arguments, from offset 0 (the two modes rank
#: different candidate lists, so a bounded page's offset means nothing under
#: exact). A refusal carries its own narrowing hint, so none is repeated here.
BOUNDED_RECOVERY = {"completeness": COMPLETENESS_EXACT}

#: Upper bounds on search pagination so backend work is bounded independent of
#: attacker-chosen large offsets (semantic/alias materialize offset+limit+1 rows).
_MAX_SEARCH_LIMIT = 1000
_MAX_SEARCH_OFFSET = 10_000


def search(
    conn: psycopg.Connection,
    session: Session,
    query: str,
    *,
    select: Optional[list[str]] = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """`_search_pinned` (its docstring is the contract), with `select`'s
    `attrs.<key>` names resolved into each row's `selected` (D6, PACT
    read-passage-rows-3666). The other names are validated per method as
    before; a select of attributes only keeps the default row.

    A `select` of only id and head fields (`reply_size.select_is_head_only`,
    PACT read-reply-size-3666) serves head lines only: every row, on every
    method, leaves out its passage, match lines and excerpt. Any other
    `select` keeps them as before."""
    from okg.substrate.mcp.reply_size import narrow_row, select_is_head_only

    head_only = select_is_head_only(select)
    attr_keys, select = split_attr_select(select)
    if attr_keys and kwargs.get("method") == "alias":
        raise InvalidOperatorArguments(
            "search: select 'attrs.<key>' is served on method='lexical', "
            "'semantic' and 'grep', not 'alias'",
            data={"method": "alias", "unknown_fields": [
                f"{ATTR_SELECT_PREFIX}{key}" for key in attr_keys
            ]},
            fix_hint="drop the attrs.<key> names, or use method='lexical'",
        )
    token = _ATTR_SELECT.set(tuple(attr_keys))
    try:
        page = _search_pinned(conn, session, query, select=select, **kwargs)
    finally:
        _ATTR_SELECT.reset(token)
    if head_only and isinstance(page, dict) and isinstance(page.get("results"), list):
        page = {**page, "results": [narrow_row(row) for row in page["results"]]}
    return page


def _search_pinned(
    conn: psycopg.Connection,
    session: Session,
    query: str,
    *,
    method: str,
    filter: Optional[dict[str, Any]] = None,
    limit: int = 50,
    offset: int = 0,
    embedder: Optional[Embedder] = None,
    fuzzy: bool = False,
    select: Optional[list[str]] = None,
    scope: Optional[ResolvedScope] = None,
    rank_by: Optional[str] = None,
    embedder_spec: Optional[str] = None,
    embedder_source: Optional[str] = None,
    allow_unverified_provenance: bool = False,
    effective_view_source: Any = None,
    max_chars: Any = None,
    expand_concepts: Optional[bool] = None,
    path_prefix: Optional[str] = None,
    continuation_token: Optional[str] = None,
    completeness: Optional[str] = None,
    where: Any = None,
    record_scope: Any = None,
    delivery_view: Optional[str] = None,
    view: Optional[str] = None,
    matching: Optional[str] = None,
    context_chars: Any = None,
    max_bytes: Optional[int] = None,
    window_measure: Optional[Callable[[dict[str, Any]], int]] = None,
    window_reserve: int = 0,
    reference_members: bool = False,
    exact_pass: bool = False,
    own_path_tail: Optional[str] = None,
    passage_chars: Optional[int] = None,
) -> dict[str, Any]:
    """Pinned search across the three ranker methods and `grep`.

    `passage_chars` (PACT read-passage-rows-3666, D2): the `max_chars` the
    caller passed, which sizes each row's `passage`; None (not passed) keeps
    the per-type sizes. `max_chars` keeps sizing `excerpt` as before.

    `exact_pass` (PACT lexical-exact-identifier-hits): a lexical call's
    first page also carries `exact_matches`, grep's records holding each
    identifier or quoted span of the query (`_exact_matches`). Only the MCP
    tool asks for it; in-process callers never pay for it.

    `own_path_tail` (grep only, in-process only; PACT filename-exact-hits):
    the exact pass's file lookup. Grep then matches the records whose own
    `attrs.path` equals it or ends with `/<it>`, in place of the query's
    terms (`grep_search.search_grep`). The MCP tool does not accept it.

    `reference_members` (#3222): a lexical record search also returns its
    result-reference members under the internal key `_reference_members`,
    `{"members": [...], "passages_excluded": n}` (see `_reference_members`);
    the MCP layer pops it.

    `method="grep"` (PACT mcp-grep-3067) is exact, case-insensitive text
    search, its records ranked by BM25 on the query's literal words (PACT
    mcp-grep-rank); `grep_search.search_grep` holds its contract. `path_prefix`
    (grep only) keeps text units whose `attrs.path`, or whose declared
    record's `attrs.path`, starts with it.

    `filter` accepts:
      - `subtype`: str — restrict to one subtype.
      - `attr_filters`: dict[str, str|int|bool] — JSONB `@>`
        predicate against `attrs`; AND-composes with subtype +
        method-specific predicates. Per
        `add-substrate-attr-filters` (2026-04-27).
    Unknown keys raise.

    `scope` is an ALREADY-RESOLVED deployment search profile
    (`analyst-search-profiles`, issue #797). This function holds only a
    connection and a session, and a profile name can only be resolved
    against a deployment manifest plus the pinned catalog, so resolution
    belongs at the operator surface (the CLI and the MCP tool) and the
    result is passed in here. Passing a bare profile NAME raises rather
    than being resolved on the fly. `scope=None` is unscoped, which is
    also what every in-process caller that never asked for a profile
    gets — a deployment `default_profile` is applied by the operator
    surfaces, not discovered here.

    `embedder` is required for `method="semantic"`.

    `fuzzy=True` (alias method only): if the exact-match tier
    of the alias cascade returns nothing, fall through to
    `okg.fuzzy_resolve_alias` (trigram → Levenshtein) at the
    pinned catalog version. Default `False` preserves the
    legacy exact-match behavior.

    `rank_by="self"` ranks each returned row's text. For lexical search,
    `rank_by="passages"` instead ranks the named parent subtype through its
    pinned catalog-declared passage relation. Undeclared or ambiguous use is
    refused; every envelope reports the applied mode.

    The `scope` also carries the three AUTHORITY-and-DIVERSITY controls added by
    `mcp-authority-diversity-budgets` (issue #1444), resolved at the operator
    surface exactly like the profile: `source_ids` (the producer ids the applied
    source families resolve to), `excluded_path_globs`, and `max_per_parent`.
    All three are SELECTION, not ranking: they decide which rows are ranked and
    which survive, and never change a score or an order. That restriction is
    deliberate — `read_affordances.py` records the standing prohibition on an
    unqualified source-formality prior, with a counterexample where the informal
    source held the answer, so authority here may only be something a deployment
    DECLARES and a caller ASKS for, disclosed in the envelope either way.

    `max_chars` bounds the per-string excerpt/preview budget (default 280); out
    of range it is refused, never clamped.

    `where` is the shared predicate tree (PACT read-model-s3-where-scope,
    `search_where`). Its top-level equality clauses on `type`, `source`
    (lexical) and `attrs.<key>` run exactly as `filter.subtype`, `.source`
    and `.attr_filters` do; everything else is checked three-valued on the
    rows as this reader reads them. In record views a clause applies to the
    record and a passage matches through its record (D6). `record_scope`
    (`{"record": id}` or `{"records": [...]}`, lexical only) restricts the
    ranked read to those records and their declared passages; a record the
    reader cannot see scopes to nothing, exactly as a missing one does.

    `expand_concepts` (lexical only) governs the deployment-declared concept
    groups the resolved `scope` carries (issue #1218). Unset means on, a no-op
    unless the deployment declares `search.concept_groups:`: a query token
    EQUAL to a declared term adds that group's other terms as alternatives, and
    a row found only through an added term ranks BELOW every row carrying a
    term the caller typed. The envelope always reports `concept_expansion`.

    `completeness` (lexical only; PACT lexical-index-scan-cap-3162). Unset or
    `"bounded"` serves the native BM25 candidates; the BM25 index can stop at
    100,000 entries before scope filters run, so `coverage.retrieval` says
    retrieval is bounded and names the recovery, and `total_count` is null
    with basis `unknown`. The top-level `completeness` names the mode. `"exact"` reads each short read's whole scoped input within a
    200,000-row / 5-second bound, or refuses with `lexical_index_scan_cap`.
    Any value on another method is refused, never ignored.
    """
    if method not in _VALID_METHODS:
        raise InvalidOperatorArguments(
            f"search: method must be one of {sorted(_VALID_METHODS)}; "
            f"got {method!r}"
        )
    max_chars = validate_preview_chars(max_chars)
    if passage_chars is not None:
        passage_chars = validate_preview_chars(passage_chars)
    if not isinstance(query, str) or not query:
        raise InvalidOperatorArguments("search: query must be a non-empty string")
    if limit < 1:
        raise InvalidOperatorArguments("search: limit must be >= 1")
    if offset < 0:
        raise InvalidOperatorArguments("search: offset must be >= 0")
    # Bound pagination so backend work cannot be driven by an attacker-chosen
    # large offset/limit. Semantic + alias backends materialize LIMIT
    # offset+limit+1 rows, so an unbounded offset turns a tiny page into a full
    # scan; cap both (per harden-mcp-write-boundaries).
    requested_limit, requested_offset = limit, offset
    limit = min(limit, _MAX_SEARCH_LIMIT)
    offset = min(offset, _MAX_SEARCH_OFFSET)
    pagination_capped = limit != requested_limit or offset != requested_offset

    if scope is not None and not isinstance(scope, ResolvedScope):
        raise ValueError(
            "search: scope must be a ResolvedScope, not "
            f"{type(scope).__name__}. Resolve the profile name at the "
            "operator surface (okg.substrate.mcp.search_profiles."
            "resolve_scope) and pass the result; search() has no manifest "
            "and must not guess a scope."
        )
    scope = scope or UNSCOPED
    # `children_of` (#3363): refused by name where it is not served, before
    # the windows check below could refuse it as a missing record scope.
    children = search_where.normalize_children_scope(record_scope)
    search_where.assert_children_scope_served(
        children, rank_by=rank_by, view=view or delivery_view,
        retain=reference_members,
    )
    if view is not None and scope.max_per_parent is not None and (
        scope.max_per_parent_origin != "profile"
    ):
        # Windows name an explicit per-parent bound as theirs to refuse, as
        # the MCP tool does, before the generic diversity refusal below.
        validate_window_call(
            view=view, matching=matching, context_chars=context_chars, method=method,
            record_ids=search_where.normalize_record_scope(record_scope), query=query,
            max_per_parent=scope.max_per_parent,
        )
    # The rank mode's default depends on where `max_per_parent` came from
    # (PACT search-result-shape, D1 "Diversity"): unset, lexical search ranks
    # `records`, unless the caller passed a bound, which asks for sibling rows
    # and so `self`. A scope built in process with no origin counts as an
    # explicit bound.
    requested_bound_origin = (
        scope.max_per_parent_origin
        if scope.max_per_parent is not None
        else None
    )
    if children is not None:
        rank_by = RANK_BY_SELF
    rank_by = validate_rank_by(
        rank_by,
        method=method,
        max_per_parent_argument=(
            scope.max_per_parent is not None
            and scope.max_per_parent_origin != "profile"
        ),
    )
    # A PROFILE-default bound under `records` is not applied, and the envelope
    # says so; an explicit bound with `records` is refused just below.
    scope, diversity_reason = restrict_scope_to_rank_mode(scope, rank_by)
    # Refused BEFORE any database round-trip, and refused rather than ignored:
    # `rank_by='passages'` and `'records'` already return one row per parent,
    # so a per-parent cap there could never remove a row, and a control that
    # silently does nothing is indistinguishable from one that worked.
    assert_diversity_rank_mode(
        scope.max_per_parent, rank_by=rank_by, method=method,
    )

    filter = filter or {}
    extras = set(filter) - {"subtype", "attr_filters", "source"}
    if extras:
        raise InvalidOperatorArguments(
            f"search: unknown filter keys {sorted(extras)}; "
            f"allowed: subtype, attr_filters, source"
        )
    raw_filter = dict(filter)
    # One predicate: the old filter keys and `where`, top-level equalities
    # lifted into the old keys so both spellings run the same code (D9).
    normalized_where = search_where.normalize_where(where)
    where_plan = search_where.plan_where(
        filter,
        normalized_where,
        lift_source=method == "lexical",
        lift_type=method != "grep",
    )
    filter = dict(where_plan.filter)
    if where_plan.residual is not None:
        filter["where"] = where_plan.residual
    record_ids = search_where.normalize_record_scope(record_scope)
    # `view="windows"` (PACT read-record-windows): grep over a record scope,
    # refused here by name for every argument it would not honour, as the
    # MCP tool refuses them. The named records are the scope: the default
    # profile's subtypes do not narrow it (issue #3467); its families,
    # excluded paths and an explicit per-parent bound would be dropped, so
    # they are refused.
    window_call = validate_window_call(
        view=view, matching=matching, context_chars=context_chars, method=method,
        record_ids=record_ids, query=query, where=where, path_prefix=path_prefix,
        offset=offset, select=select, rank_by=rank_by,
        profile=(scope.profile if scope is not None and scope.excluded_path_globs else None),
        source_family=scope.source_families if scope is not None else None,
        subtype_filter=raw_filter.get("subtype"),
        attr_filters=raw_filter.get("attr_filters"),
        filter={key: value for key, value in raw_filter.items()
                if key not in ("subtype", "attr_filters")},
        max_per_parent=(
            scope.max_per_parent
            if scope is not None and scope.max_per_parent_origin != "profile" else None
        ),
    )
    scoped = record_ids is not None or children is not None
    if scoped and method != "lexical" and window_call is None:
        raise search_where.SearchScopeUnsupported(
            f"search: scope is served on method='lexical' only, not {method!r}",
            data={"method": method, "scope": "records"},
            fix_hint="use method='lexical' with scope, or drop scope",
        )
    if scoped and rank_by == RANK_BY_PASSAGES:
        raise search_where.SearchScopeUnsupported(
            "search: scope is not served with rank_by='passages'",
            data={"method": method, "rank_by": rank_by, "scope": "records"},
            fix_hint="use rank_by='records' or 'self' with scope",
        )
    # `source` names ONE producer id, on the same stored column the family
    # predicate reads. It is allowed only where it reaches the ranked query --
    # today `lexical`, whose paged relation projects `source`. Elsewhere it is
    # REFUSED rather than applied to the page afterwards, for the reason
    # `SourceFamilyNotApplicable` gives: a residual filter under a rank cap
    # silently under-fills the page, and a control that quietly narrows a page
    # is indistinguishable from one that was ignored.
    if filter.get("source") is not None and method not in ("lexical", "grep"):
        raise InvalidOperatorArguments(
            f"search: filter['source'] is not supported on method {method!r}: "
            f"its paged result relation does not project the producer id, so "
            f"the predicate cannot go into the query that pages. Supported: "
            f"lexical."
        )
    # Per add-substrate-attr-filters: validate attr_filters shape.
    # v1: exact-match dict only (str/int/bool/None scalar values).
    validate_attr_filters(filter.get("attr_filters"))

    # Pure-argument validation first (lets argument-only tests
    # pass conn=None / session=None). DB round-trips below.
    #
    # NOTE this stays a PRESENCE check on the embedder object, deliberately.
    # The model-identity derivation happens inside `_search_semantic`, not
    # here: deriving it pre-dispatch would raise on callers that pass a stub
    # embedder while monkeypatching the backends (see
    # tests/substrate/mcp/test_search_pagination_caps.py), which are about
    # pagination and never reach a ranking.
    if method == "semantic":
        if embedder is None and embedder_spec is None:
            raise ValueError(
                "search: method='semantic' requires an embedder or an "
                "embedder_spec"
            )
        if embedder is not None and embedder_spec is not None:
            # Refuse rather than preferring one: a caller who named two
            # embedders has a belief about which is used, and either choice
            # would be wrong half the time.
            raise ValueError(
                "search: pass either embedder or embedder_spec, not both; "
                f"got embedder={type(embedder).__name__} and "
                f"embedder_spec={embedder_spec!r}"
            )
    elif embedder_spec is not None or allow_unverified_provenance:
        # An embedder named for a method that cannot use it is a mistaken
        # belief, not a harmless extra argument.
        raise InvalidOperatorArguments(
            f"search: embedder_spec/allow_unverified_provenance apply only to "
            f"method='semantic'; got method={method!r}"
        )

    if continuation_token is not None and method != "grep":
        raise InvalidOperatorArguments(
            f"search: continuation_token applies only to method='grep'; got "
            f"method={method!r}"
        )
    if path_prefix is not None and method != "grep":
        raise InvalidOperatorArguments(
            f"search: path_prefix applies only to method='grep'; got "
            f"method={method!r}"
        )
    if path_prefix is not None and (not isinstance(path_prefix, str) or not path_prefix):
        raise InvalidOperatorArguments("search: path_prefix must be a non-empty string")
    if own_path_tail is not None and method != "grep":
        raise ValueError(
            f"search: own_path_tail applies only to method='grep'; got "
            f"method={method!r}"
        )

    if method != "lexical" and expand_concepts is not None:
        # Same rule the embedder arguments follow above: an argument named for a
        # method that cannot honour it is a mistaken belief about what ran.
        raise InvalidOperatorArguments(
            f"search: expand_concepts applies only to method='lexical'; got "
            f"method={method!r}"
        )
    validate_completeness(completeness, method=method)

    # Fingerprint check: every tool-entry asserts conn ↔ session
    # matches, preventing accidental cross-DB queries.
    assert_session_matches(conn, session)

    if method == "grep" and filter.get("subtype") is None:
        passage_type = _grep_passage_type(conn, session, where_plan.residual)
        if passage_type is not None:
            # Issue #3666: on grep a `where` on type judges a passage's
            # record (D6), so `type = <a passage type>` could match only
            # passages with no record and answered "0 of 0 · complete" for
            # text its passages hold. It can mean only those passages:
            # served as the subtype filter that lists them, and said so.
            name, rest, sent = passage_type
            filter["subtype"] = name
            if rest is None:
                filter.pop("where", None)
            else:
                filter["where"] = rest
            where_plan = replace(where_plan, residual=rest)
            from okg.substrate.mcp.response_bounds import note_adjusted

            note_adjusted([{
                "rule": "grep_passage_type",
                "sent": sent,
                "served": f"its passages, each judged itself (subtype `{name}`)",
            }])

    # A `subtype` filter outside the profile raises here rather than producing
    # the empty intersection, which no caller could tell apart from "your query
    # matched nothing". Naming a REFINEMENT of a profiled parent is legal: the
    # scope is already closure-expanded.
    intersect_subtype_filter(scope, filter.get("subtype"))
    if method == "grep":
        # Grep's `where` names subtypes for the profile without lifting them
        # (search_where.profile_subtypes): each meets the profile as a
        # subtype filter does, refused rather than answered with nothing.
        for named in search_where.profile_subtypes(where_plan.residual):
            intersect_subtype_filter(scope, named)

    where_context = search_where.load_where_context(
        conn, session, where_plan.residual,
        subtypes=([filter["subtype"]] if filter.get("subtype") else scope.subtypes),
        fold_parents=(
            (method == "lexical" and rank_by == RANK_BY_RECORDS)
            or (method == "grep" and not filter.get("subtype"))
        ),
    )
    if where_context is not None:
        filter["where_context"] = where_context

    if window_call is not None:
        # Windows return their own byte-fitted page: nothing below may add
        # to a page after it was measured, so the branch returns here.
        from okg.substrate.mcp.grep_query import parse_grep_query
        from okg.substrate.mcp.record_window_search import search_record_windows

        if window_measure is None or max_bytes is None:
            raise ValueError("search: view='windows' needs max_bytes and a served-bytes measure")
        if not record_ids:
            # #3605 (`winscope A`): no record scope reads this grep's own top
            # `limit` records. A continuation names the records it was signed
            # with, so a later page reads the same ones; its binding still
            # decides whether the token is good.
            record_ids = _window_hit_records(
                conn, session, query, filter=filter, scope=scope, limit=limit,
                max_chars=max_chars, continuation_token=continuation_token,
            )
            if not record_ids:
                from okg.substrate.mcp.grep_search import search_grep

                # No record matched on the grep's first page: its own page
                # says so. Its grep cursor is dropped: following it with
                # view="windows" would only rerun this first page.
                empty = search_grep(
                    conn, session, query, filter=filter, limit=limit, offset=0,
                    select=None, scope=scope, path_prefix=None, max_chars=max_chars,
                )
                empty.pop("continuation_token", None)
                return empty
        return search_record_windows(
            conn, session, query, parse_grep_query(query),
            record_ids=record_ids, matching=window_call[0],
            context_chars=window_call[1], limit=limit, max_bytes=max_bytes,
            measure=window_measure, continuation_token=continuation_token,
            reserve_bytes=window_reserve,
        )
    if method == "grep":
        from okg.substrate.mcp.grep_search import search_grep

        result = search_grep(
            conn, session, query,
            filter=filter, limit=limit, offset=offset, select=select,
            scope=scope, path_prefix=path_prefix, max_chars=max_chars,
            continuation_token=continuation_token,
            **({"own_path_tail": own_path_tail} if own_path_tail is not None else {}),
            **({"list_view": True} if delivery_view == "list" else {}),
            line_chars=passage_chars,
        )
        if completeness == COMPLETENESS_EXACT:
            require_exact_page(
                result, method="grep", continued=continuation_token is not None,
                # The hint names only what this call lacks (issue #3666).
                has_type=bool(
                    filter.get("subtype")
                    or search_where.profile_subtypes(where_plan.residual)
                ),
                has_path_prefix=path_prefix is not None,
            )
        elif completeness is not None:
            result["completeness"] = completeness
    elif method == "lexical":
        result = _search_lexical(
            conn, session, query,
            filter=filter, limit=limit, offset=offset, select=select,
            scope=scope, rank_by=rank_by,
            effective_view_source=effective_view_source,
            max_chars=max_chars,
            expand_concepts=(
                True if expand_concepts is None else bool(expand_concepts)
            ),
            completeness=completeness or COMPLETENESS_BOUNDED,
            record_ids=record_ids,
            children_of=children,
            include_excerpts=delivery_view != "list",
            list_view=delivery_view == "list",
            reference_members=reference_members,
            passage_chars=passage_chars,
        )
    elif method == "semantic":
        result = _search_semantic(
            conn, session, query, embedder=embedder,
            filter=filter, limit=limit, offset=offset, select=select,
            scope=scope, embedder_spec=embedder_spec,
            embedder_source=embedder_source,
            allow_unverified_provenance=allow_unverified_provenance,
            max_chars=max_chars,
            passage_chars=passage_chars,
        )
    else:  # method == "alias"
        result = _search_alias(
            conn, session, query,
            filter=filter, limit=limit, offset=offset,
            fuzzy=fuzzy, select=select, scope=scope, max_chars=max_chars,
        )
    if exact_pass and method == "lexical" and offset == 0 and not scoped:
        spans, dropped_spans = exact_spans(query)
        if spans:
            exact_filter, exact_where = _exact_pass_selection(
                conn, session, raw_filter, where, normalized_where, where_plan.lifted,
            )
            result[EXACT_MATCHES_KEY] = _exact_matches(
                conn, session, query, spans, dropped_spans,
                filter=exact_filter, where=exact_where, scope=scope,
                max_chars=max_chars, page=result.get("results") or [],
                windows=(
                    delivery_view != "list"
                    and (max_bytes is None or max_bytes >= EXACT_WINDOWS_MIN_BYTES)
                ),
            )
    # Disclosure is unconditional. A scoped page that does not say it was
    # scoped is indistinguishable from a complete one, and that is precisely
    # the state in which an analyst concludes the graph is missing data.
    result["search_profile"] = scope.profile
    result["scoped_subtypes"] = (
        list(scope.subtypes) if scope.subtypes is not None else None
    )
    result["rank_by"] = rank_by
    result["max_chars"] = max_chars
    # The applied predicate, old arguments included, so a filter that did not
    # apply is visible on the page rather than inferred from it. Emitted only
    # when the caller supplied a predicate (`where` or an old filter
    # argument): a call without one carries no new key, so its page costs
    # the bytes it did before `where` existed.
    applied_text = search_where.describe(where_plan.applied)
    if applied_text is not None:
        result["where_applied"] = applied_text
    if where_context is not None:
        # Grep checks the residual on every candidate its scan decides
        # (grep_search's shared-scanner adapter), not on a ranked cap.
        result.update(where_context.disclosure(evaluation_scope=(
            "grep_scanned_candidates" if method == "grep" else "bounded_ranked_candidates"
        )))
    if record_ids is not None:
        # The ids the caller named, not the ones that resolved: a hidden
        # record and a missing one must read the same.
        result["scope_applied"] = {"records": list(record_ids)}
    elif children is not None:
        result["scope_applied"] = children.applied()
    # The authority selection is disclosed UNCONDITIONALLY, for the reason the
    # subtype scope is: a page narrowed by family or by a path exclusion and
    # not saying so is indistinguishable from a complete one, and that is the
    # state in which an analyst concludes the graph is missing data.
    result.update(family_disclosure(scope, method=method))
    if requested_bound_origin is not None:
        result["max_per_parent_origin"] = requested_bound_origin
    if diversity_reason is not None:
        result["diversity_applicable"] = False
        result["diversity_reason"] = diversity_reason
    if scope.is_family_scoped and not result.get("results"):
        # "Nothing in this family matched" and "this family holds nothing at
        # this pin" look identical from an empty page, and the second is what a
        # typo'd producer id produces on a deployment whose producer registry is
        # unpopulated (see `validate_family_sources`). Measure it.
        result.update(family_emptiness_disclosure(conn, session, scope))
    if scope.is_scoped and not result.get("results"):
        # Two very different causes look identical from an empty page — the
        # query matched nothing, or the scope holds nothing. Measure the
        # second rather than leaving it to inference. The keys are ALWAYS set,
        # null when the count could not be taken, because an absent key reads
        # as "the scope is fine".
        result.update(scope_emptiness_disclosure(conn, session, scope))
    if pagination_capped:
        result["pagination_capped"] = True
        result["requested_offset"] = requested_offset
        result["requested_limit"] = requested_limit
    return result


# ---------- windows of a grep's own hits ----------

def _window_hit_records(
    conn: psycopg.Connection,
    session: Session,
    query: str,
    *,
    filter: dict[str, Any],
    scope: Any,
    limit: int,
    max_chars: Any,
    continuation_token: Optional[str],
) -> list[str]:
    """The records a `view="windows"` grep without a record scope reads
    (#3605, Jason `winscope A`): the records its continuation was signed
    with, else this grep's own top `limit` records, in id order as a
    `scope={"records": [...]}` is (`normalize_record_scope`), so the read is
    exactly the scoped one."""
    if continuation_token is not None:
        from okg.substrate.mcp.continuation import (
            ContinuationTokenInvalid,
            open_continuation,
        )

        # The signature is verified here; the windows read then compares the
        # whole binding. A token of another read is refused before any grep.
        binding = open_continuation(continuation_token)["binding"]
        projection = binding.get("projection")
        records = projection.get("records") if isinstance(projection, dict) else None
        if binding.get("kind") != "record_windows" or not isinstance(records, list):
            raise ContinuationTokenInvalid(
                "search view='windows': the continuation_token is not a windows token",
                data={"token_kind": binding.get("kind")},
                fix_hint="pass the continuation_token a windows page returned, or drop it",
            )
        return [str(record) for record in records]
    from okg.substrate.mcp.grep_search import search_grep

    found = search_grep(
        conn, session, query, filter=filter, limit=limit, offset=0,
        select=["node_id"], scope=scope, path_prefix=None, max_chars=max_chars,
    )
    return sorted({str(row["node_id"]) for row in found.get("results") or []})


# ---------- select refusals that say where a field is ----------

def search_select_elsewhere(method: str, *, query: Optional[str] = None) -> dict[str, str]:
    """For a `search` method, each field another method returns and this
    one does not, mapped to the method that returns it (PACT
    select-field-instructions). A refusal says it; nothing is accepted in
    its place. Methods are tried in the order agents use them."""
    from okg.substrate.mcp.grep_search import GREP_FIELDS
    from okg.substrate.mcp.tools import _FIND_MENTIONS_FIELDS

    by_method: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("lexical", (*_LEXICAL_FIELDS, "source_lineage")),
        ("grep", GREP_FIELDS),
        ("alias", _ALIAS_SELECTABLE),
        ("identifier_mentions", _FIND_MENTIONS_FIELDS),
    )
    own = dict(by_method).get(method, ())
    out: dict[str, str] = {}
    if method == "identifier_mentions":
        # Its rows are spans of the caller's own query text, not records
        # (#3646): no record field can mean anything there. Name the call
        # that finds the record a mention's value names.
        # The caller's own query is spelled into the call when it is short
        # enough for one line (`query`, passed by the read).
        same = (
            f"query={json.dumps(query, ensure_ascii=False)}"
            if isinstance(query, str) and 0 < len(query) <= 80
            and query.isprintable() and "\u27e6" not in query
            else "the same query"
        )
        grep = (
            f'search(method="grep", {same})' if same.startswith("query=")
            else 'search(method="grep") with the same query'
        )
        from okg.substrate.mcp.envelope import SelectHint

        span = SelectHint(
            fact="identifier_mentions returns spans of your query text, not records",
            action=(
                f"{grep} finds the records whose text holds it, and "
                'method="alias" with one identifier the record it names'
            ),
            code="rows_are_query_spans",
        )
        for _other, fields in by_method:
            for name in fields:
                if name not in own:
                    out.setdefault(name, span)
        return out
    if method == "attrs":
        # No query, so no matching text to excerpt (#3646).
        out["excerpt"] = (
            "method=attrs has no query to excerpt; select 'attrs', or read "
            'the text with inspect(target={"kind":"text","node_id":<id>})'
        )
    for other, fields in by_method:
        if other == method:
            continue
        for name in fields:
            if name not in own and name not in out:
                out[name] = f"returned by search(method={other}), not method={method}"
    return out


# ---------- exact matches beside a lexical page ----------

#: The response key of the exact pass (PACT lexical-exact-identifier-hits).
EXACT_MATCHES_KEY = "exact_matches"
#: Records listed per span.
EXACT_RECORDS_PER_SPAN = 3
#: Own-path matches read per filename span to choose one record per path
#: (PACT filename-exact-hits): a file and the symbols or other records
#: sharing its path. More than this are counted, and the entry says the
#: choice was made among the first ones read.
EXACT_PATH_CANDIDATES = 25
#: The whole exact pass's time budget, across its spans, in milliseconds.
EXACT_PASS_BUDGET_MS = 2_000
#: A span is not started with less than this much budget left.
_EXACT_MIN_SPAN_MS = 100
#: Below this response budget the block lists ids only, no match windows:
#: nine windows (about 3 KB with fences) would crowd out the ranked page.
EXACT_WINDOWS_MIN_BYTES = 8_192
#: A grep page that read every visible match: only then is "no record
#: holds this text" true.
_GREP_COMPLETE_PAGE = "top_of_all_visible_matches"


def _grep_span(
    conn: psycopg.Connection,
    session: Session,
    span: str,
    *,
    filter: dict[str, Any],
    where: Any,
    scope: ResolvedScope,
    max_chars: Any,
    deadline_at: float,
    own_path: bool = False,
) -> dict[str, Any]:
    """One span's grep: its status, count and records (with their first
    match window), or the status that says why it has none.

    `own_path` (PACT filename-exact-hits) greps the records whose own
    `attrs.path` equals the span or ends with `/<span>` instead of its text,
    reading up to `EXACT_PATH_CANDIDATES` of them, and lists one record per
    distinct path (`_one_record_per_path`). Its records carry
    `matched_on: "path"` and no window; the count is every match. A graph
    where no record carries a path has no such record."""
    from okg.substrate.mcp.grep_search import (
        deadline,
        own_path_reads,
        path_record_subtypes,
    )

    if (deadline_at - time.monotonic()) * 1000 < _EXACT_MIN_SPAN_MS:
        return {"status": "skipped:time_budget"}
    try:
        # No wrapping transaction: under Nomos enforcement grep records its
        # decisions and commits them itself, and each bounded statement
        # already runs in its own savepoint.
        with deadline(deadline_at):
            if own_path:
                found = search(
                    conn, session, span,
                    method="grep", filter=dict(filter), where=where,
                    limit=EXACT_PATH_CANDIDATES, offset=0,
                    select=["node_id", "subtype"],
                    scope=scope, max_chars=max_chars, own_path_tail=span,
                )
                matched = found.get("results") or []
                try:
                    paths = own_path_reads(
                        conn, session, [str(r.get("node_id")) for r in matched])
                    ranked_types = path_record_subtypes(conn, session) if matched else set()
                except SubstrateError as exc:
                    # The matches were read and counted; only the choice of
                    # one record per path stopped. Say how many were read.
                    code = str(getattr(exc, "code", None) or type(exc).__name__)
                    return {
                        "status": "timeout" if "timeout" in code.lower() else f"refused:{code}",
                        "count": found.get("total_count"),
                        "count_basis": found.get("total_count_basis"),
                        "count_unit": found.get("count_unit"),
                        "candidates_read": len(matched),
                    }
            else:
                found = search(
                    conn, session, '"' + span.replace('"', " ") + '"',
                    method="grep", filter=dict(filter), where=where,
                    limit=EXACT_RECORDS_PER_SPAN, offset=0,
                    select=["node_id", "subtype", "match_windows"],
                    scope=scope, max_chars=max_chars,
                )
    except SubstrateError as exc:
        code = str(getattr(exc, "code", None) or type(exc).__name__)
        return {"status": "timeout" if "timeout" in code.lower() else f"refused:{code}"}
    except psycopg.Error as exc:
        return {"status": f"error:{type(exc).__name__}", "database_error": True}
    except Exception as exc:  # noqa: BLE001 - the keyword page must survive
        # A grep defect must not take down the keyword page it rides beside
        # (PACT lexical-exact-identifier-hits, requirement bounded). It is
        # disclosed on the span and logged with its traceback, never hidden.
        _log.exception("search: exact pass grep failed for span %r", span)
        return {"status": f"error:{type(exc).__name__}"}
    records = []
    if own_path:
        records = _one_record_per_path(
            found.get("results") or [], span, paths=paths, ranked_types=ranked_types,
        )[:EXACT_RECORDS_PER_SPAN]
    for row in [] if own_path else found.get("results") or []:
        windows = row.get("match_windows") or []
        records.append({
            "node_id": str(row.get("node_id")),
            "subtype": row.get("subtype"),
            "window": (windows[0].get("excerpt") if windows else None),
        })
    page_basis = found.get("page_basis")
    if records:
        status = "matched"
    elif page_basis == _GREP_COMPLETE_PAGE and found.get("total_count_basis") == "exact":
        status = "no_exact_match"
    elif page_basis == _GREP_COMPLETE_PAGE:
        # Grep read every visible match and found none, but a policy that
        # can redact makes its count a lower bound.
        status = "no_visible_match"
    else:
        # Grep stopped before it read every match (a scan bound or the
        # deadline): an empty page proves nothing.
        status = f"incomplete:{page_basis}"
    out = {
        "status": status,
        "count": found.get("total_count"),
        "count_basis": found.get("total_count_basis"),
        "count_unit": found.get("count_unit"),
        "records": records,
    }
    if own_path and (found.get("truncated") or page_basis != _GREP_COMPLETE_PAGE):
        # The record per path was chosen among the matches read, not all.
        out["candidates_read"] = len(found.get("results") or [])
    return out


def _one_record_per_path(
    rows: Sequence[Mapping[str, Any]],
    span: str,
    *,
    paths: Mapping[str, Optional[str]],
    ranked_types: set[str],
) -> list[dict[str, Any]]:
    """The own-path matches, one record per distinct path (PACT
    filename-exact-hits), best path first.

    Grep's record fold already applies the declared passage relations: a
    passage that matched stands as its record, so no two rows left here are
    a declared record and its passage. Rows sharing a path (compared
    case-insensitively, as the match was) are a file and its code symbols,
    say. Within a path, and between paths, a row is preferred when its path
    has the span's exact case, then when its type makes the path its own
    (`grep_search.path_record_subtypes`), then by node id, only to make the
    order total. A row whose path is unknown to the reader (masked or
    dropped by the read, `grep_search.own_path_reads`) or does not end with
    the span (a record reached through a matching passage) is its own
    group, never exact-case, after every path the reader knows."""
    folded_span = span.casefold()
    best: dict[Any, tuple[tuple[Any, ...], dict[str, Any]]] = {}
    for row in rows:
        node_id = str(row.get("node_id"))
        subtype = row.get("subtype")
        path = paths.get(node_id)
        folded = path.casefold() if isinstance(path, str) else None
        if folded is not None and (folded == folded_span or folded.endswith("/" + folded_span)):
            group: Any = ("path", folded)
            exact = path == span or path.endswith("/" + span)
        else:
            group, exact = ("record", node_id), False
        key = (not exact, group[0] == "record", str(subtype) not in ranked_types, node_id)
        record = {"node_id": node_id, "subtype": subtype, "matched_on": "path"}
        if group not in best or key < best[group][0]:
            best[group] = (key, record)
    return [record for _key, record in sorted(best.values(), key=lambda kv: kv[0])]


def _with_path_matches(
    span: str, path: dict[str, Any], text: dict[str, Any],
) -> dict[str, Any]:
    """One filename span's entry (PACT filename-exact-hits): the text pass's
    status, count and records, with the own-path pass's status and count
    under `path_*` keys and its records listed first. A record both passes
    found is listed once, as a path match; the list keeps at most
    `EXACT_RECORDS_PER_SPAN` records."""
    entry: dict[str, Any] = {"span": span, **text}
    entry["path_status"] = path["status"]
    for key in ("count", "count_basis", "count_unit", "candidates_read"):
        if key in path:
            entry[f"path_{key}"] = path[key]
    path_records = list(path.get("records") or [])
    if path_records or "records" in text:
        listed = {record["node_id"] for record in path_records}
        merged = path_records + [
            record for record in text.get("records") or []
            if record["node_id"] not in listed
        ]
        entry["records"] = merged[:EXACT_RECORDS_PER_SPAN]
    return entry


def _exact_pass_selection(
    conn: psycopg.Connection,
    session: Session,
    raw_filter: dict[str, Any],
    where: Any,
    normalized_where: Optional[dict[str, Any]],
    lifted: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], Any]:
    """The `(filter, where)` the exact pass greps with.

    Lexical lifts a top-level `type = X` out of `where` into its subtype
    filter; when X is a declared passage child the keyword page then ranks
    passages. Grep never lifts it: its `where` judges each passage through
    its parent record (req.mcp-grep-page.where), so the caller's own `where`
    would grep a different unit than the page beside it and report "no
    record holds this text" for a span only passages hold. In that one case
    the pass greps with X as its subtype filter and the `where` without that
    clause. A top-level `type in [X]` naming one type is the same selection
    and is treated the same way, so the pass never greps passages through a
    record-judged clause. Otherwise it greps the caller's own selections.
    """
    if raw_filter.get("subtype") is not None:
        return raw_filter, where
    lifted_type = next(
        (clause for clause in lifted if clause.get("field") == "type"), None,
    )
    if lifted_type is None:
        lifted_type = next(
            (
                clause for clause in search_where.top_conjuncts(normalized_where)
                if clause.get("op") == "in" and clause.get("field") == "type"
                and len(clause.get("values") or []) == 1
                and isinstance(clause["values"][0], str)
            ),
            None,
        )
        if lifted_type is None:
            return raw_filter, where
        subtype = lifted_type["values"][0]
    else:
        subtype = str(lifted_type["value"])
    if not passage_child_declarations(
        conn,
        catalog_version_id=session.catalog_version_id,
        child_subtypes=[subtype],
    ):
        return raw_filter, where
    remaining = search_where.and_clauses(*[
        clause for clause in search_where.top_conjuncts(normalized_where)
        if clause is not lifted_type
    ])
    return {**raw_filter, "subtype": subtype}, remaining


def _exact_matches(
    conn: psycopg.Connection,
    session: Session,
    query: str,
    spans: Sequence[str],
    dropped_spans: Sequence[str],
    *,
    filter: dict[str, Any],
    where: Any,
    scope: ResolvedScope,
    max_chars: Any,
    page: Sequence[Mapping[str, Any]],
    windows: bool,
) -> dict[str, Any]:
    """The records whose text holds each span exactly, beside a lexical page.

    PACT lexical-exact-identifier-hits. BM25 ranks an identifier by the words
    the `english` parser makes of it: a ticket key inside a URL is part of
    one URL token, and a long dataset name splits into words thousands of
    records share, so the record holding it can miss the ranked cap. Grep
    matches the stored text itself. Each span runs as one grep call with
    the caller's own selections (profile, family, subtype, source, `where`),
    so access rules, masking and record folding are grep's. The lexical
    page is not changed; a record already on it is named by its position.

    A filename-shaped span (`grep_query.filename_shaped`; PACT
    filename-exact-hits) first gets an own-path pass: the records whose own
    `attrs.path` equals the span or ends with `/<span>` are listed before the
    records whose text holds it (`_with_path_matches`), so a query naming a
    file lists the file even when more records mention its name than the
    span's slots hold.

    The grep reads run once per MCP call: a byte-fitting rerun of the same
    call reuses them from the call's memo and only renumbers the positions
    against its own page. They share one deadline (`EXACT_PASS_BUDGET_MS`)
    that every grep statement honors. A span that times out, is refused or
    fails is listed with its status, and a database error stops the spans
    after it; the lexical page is returned whatever the pass does.
    """
    from okg.substrate.mcp.grep_query import filename_shaped

    import okg.substrate.mcp.result_references as result_references

    memo = result_references.call_memo()
    memo_key = "exact_matches:" + json.dumps(
        [session.generation_id, query, filter, where, scope.profile,
         sorted(scope.subtypes) if scope.subtypes is not None else None,
         sorted(scope.source_ids) if scope.source_ids is not None else None,
         max_chars],
        sort_keys=True, default=str,
    )
    graded: Optional[list[dict[str, Any]]] = (
        memo.get(memo_key) if memo is not None else None
    )
    if graded is None:
        grep_scope = replace(scope, max_per_parent=None, max_per_parent_origin=None)
        deadline_at = time.monotonic() + EXACT_PASS_BUDGET_MS / 1000.0
        graded = []
        failed = False
        for span in spans:
            if failed:
                graded.append({"span": span, "status": "skipped:after_error"})
                continue
            path: Optional[dict[str, Any]] = None
            if filename_shaped(span):
                path = _grep_span(
                    conn, session, span, filter=filter, where=where, scope=grep_scope,
                    max_chars=max_chars, deadline_at=deadline_at, own_path=True,
                )
                if path.pop("database_error", False):
                    # The session may be unusable: no text pass, no later span.
                    graded.append({"span": span, "status": "skipped:after_error",
                                   "path_status": path["status"]})
                    failed = True
                    continue
            entry = {"span": span, **_grep_span(
                conn, session, span, filter=filter, where=where, scope=grep_scope,
                max_chars=max_chars, deadline_at=deadline_at,
            )}
            # Only a database error stops the spans after it: the session
            # may be unusable. Any other failure is the span's own.
            failed = bool(entry.pop("database_error", False))
            if path is not None:
                entry = _with_path_matches(
                    span, path, {k: v for k, v in entry.items() if k != "span"},
                )
            graded.append(entry)
        if memo is not None:
            memo[memo_key] = graded
    positions = {
        str(row.get("node_id")): index
        for index, row in enumerate(page, start=1)
        if row.get("node_id") is not None
    }
    out_spans: list[dict[str, Any]] = []
    for entry in graded:
        served = {k: v for k, v in entry.items() if k != "records"}
        if "records" in entry:
            served["records"] = []
            for record in entry["records"]:
                listed = {"node_id": record["node_id"], "subtype": record["subtype"]}
                if record.get("matched_on"):
                    listed["matched_on"] = record["matched_on"]
                if record["node_id"] in positions:
                    listed["result_position"] = positions[record["node_id"]]
                elif windows and record.get("window"):
                    listed["window"] = record["window"]
                served["records"].append(listed)
        out_spans.append(served)
    block: dict[str, Any] = {
        "method": "grep",
        "spans": out_spans,
        "records_per_span": EXACT_RECORDS_PER_SPAN,
        "budget_ms": EXACT_PASS_BUDGET_MS,
    }
    if not windows:
        block["windows"] = "omitted"
    if dropped_spans:
        block["dropped_spans"] = list(dropped_spans)
    return block


# ---------- per-method implementations ----------

def _matches_filter(
    row: dict[str, Any],
    filter: dict[str, Any],
    *,
    scope: "ResolvedScope | None" = None,
) -> bool:
    subtype = filter.get("subtype")
    if subtype is not None and row.get("subtype") != subtype:
        return False
    # Re-checked in Python beside the SQL predicate, as the scope's own
    # selections are: the SQL form spends the rank cap correctly, this form is
    # what makes the promise true of the rows actually returned.
    requested_source = filter.get("source")
    if requested_source is not None and row.get("source") != requested_source:
        return False
    if scope is not None and scope.subtypes is not None:
        if row.get("subtype") not in scope.subtypes:
            return False
    if scope is not None and scope.source_ids is not None:
        if row.get("source") not in scope.source_ids:
            return False
    attrs = row.get("attrs") or {}
    if scope is not None and scope.excluded_path_globs:
        # Re-checked over EFFECTIVE attrs. The SQL predicate reads the STORED
        # `attrs->>'path'`, which an effective-attribute overlay can change, so
        # the SQL form spends the rank cap correctly while this form is what
        # makes the promise true of the rows actually returned.
        if excluded_by_path_globs(attrs, scope.excluded_path_globs):
            return False
    attr_filters = filter.get("attr_filters") or {}
    if not all(attrs.get(key) == value for key, value in attr_filters.items()):
        return False
    # The residual `where`: three-valued on the read row, only TRUE admits
    # (D5). Runs wherever attr_filters run, so it inherits their placement:
    # after the Nomos read filter and the effective-attrs overlay.
    return search_where.matches(row, filter.get("where"), filter.get("where_context"))


def _row_filter(filter: dict[str, Any]) -> dict[str, Any]:
    """The selections that describe a returned row: attr_filters + where."""
    out: dict[str, Any] = {"attr_filters": filter.get("attr_filters") or {}}
    if filter.get("where") is not None:
        out["where"] = filter["where"]
        if filter.get("where_context") is not None:
            out["where_context"] = filter["where_context"]
    return out


def _has_row_filter(filter: dict[str, Any]) -> bool:
    return bool(filter.get("attr_filters")) or filter.get("where") is not None


def _scope_sql(
    scope: "ResolvedScope | None",
    params: dict[str, Any],
    *,
    column: str,
) -> str | None:
    """The scope predicate for a ranked query, or None when unscoped.

    Returned as SQL to be AND-ed into the WHERE of the RANKED query, never
    applied to the page afterwards: the rank cap is a SQL LIMIT, so filtering
    after ranking would leave the answering row already evicted by rows the
    caller had scoped out (issue #797).
    """
    if scope is None or scope.subtypes is None:
        return None
    params["scoped_subtypes"] = list(scope.subtypes)
    return f"{column} = ANY(%(scoped_subtypes)s)"


def _family_sql(
    scope: "ResolvedScope | None",
    params: dict[str, Any],
    *,
    column: str,
) -> str | None:
    """The source-family predicate for a ranked query, or None.

    Goes into the RANKED query for exactly the reason `_scope_sql` does: the
    rank cap is a SQL LIMIT, so a family applied to the page afterwards would
    hand back the in-family rows that happened to survive a ranking the
    out-of-family rows had already filled.
    """
    if scope is None or scope.source_ids is None:
        return None
    params["scoped_source_ids"] = list(scope.source_ids)
    return f"{column} = ANY(%(scoped_source_ids)s)"


def _source_sql(
    filter: dict[str, Any],
    params: dict[str, Any],
    *,
    column: str,
) -> str | None:
    """The caller's one-producer predicate for a ranked query, or None.

    Goes into the RANKED query for exactly the reason `_family_sql` does: the
    rank cap is a SQL LIMIT, so a producer applied to the page afterwards would
    hand back the in-source rows that happened to survive a ranking the
    out-of-source rows had already filled. `source` is a stored column, never
    overlaid by the effective-attribute view, so the predicate is exact in SQL.

    This is SELECTION, not authority: it decides which rows are ranked, never
    which rows the reader may see. Every row it admits still passes the nomos
    read filter, so a producer the reader may not see yields the same empty
    result as a producer that does not exist.
    """
    requested = filter.get("source")
    if requested is None:
        return None
    params["filter_source"] = str(requested)
    return f"{column} = %(filter_source)s"


def _path_exclusion_sql(
    scope: "ResolvedScope | None",
    params: dict[str, Any],
    *,
    column: str,
    prefix: str = "xglob",
) -> list[str]:
    """The profile's path exclusions as ranked-query predicates.

    One `NOT (... LIKE ...)` per glob, AND-ed, so a row is excluded when its
    stored `attrs.path` matches ANY of them. A row with no `path` key is kept:
    an exclusion names paths, and a row without one has not been named.

    `column` is the attrs expression for the relation being queried, so the same
    helper serves the segment (`attrs`) and the effective-view join (`av.attrs`).
    """
    if scope is None or not scope.excluded_path_globs:
        return []
    clauses: list[str] = []
    for index, pattern in enumerate(scope.excluded_path_globs):
        key = f"{prefix}_{index}"
        params[key] = glob_to_like(pattern)
        # The `IS NOT NULL` guard is load-bearing, not defensive noise. With
        # `attrs = {"path": null}` the key EXISTS, so `? 'path'` is true, but
        # `->>'path'` is SQL NULL and `NULL LIKE p` is NULL — `NOT NULL` is
        # NULL, which is not true, so the row was DROPPED by this predicate
        # while the Python residual (`excluded_by_path_globs`, which requires a
        # `str`) KEPT it. Two answers for one row, decided by which path ran.
        clauses.append(
            f"NOT ({column} ? 'path' AND ({column}->>'path') IS NOT NULL "
            f"AND ({column}->>'path') LIKE %({key})s ESCAPE '\\')"
        )
    return clauses


def _citation_from_attrs(attrs: Any) -> Optional[dict[str, Any]]:
    """Return the node's SOURCE-SUPPLIED citation, or None.

    Per `source-supplied-search-citations`: a node is citable iff its
    source emitted a non-empty `citation_label`; `citation_url` rides
    along when present. Search never composes a citation from title,
    repo, path, heading, url, or any other attribute — the source is
    the only author of its citation.
    """
    if not isinstance(attrs, dict):
        return None
    label = attrs.get("citation_label")
    if not isinstance(label, str) or not label.strip():
        return None
    citation: dict[str, Any] = {"label": label}
    url = attrs.get("citation_url")
    if is_valid_citation_url(url):
        citation["url"] = url
    return citation


def passage_counts_withheld(session: Session) -> bool:
    """Whether a parent's passage count must be withheld for this session.

    Operator decision for issue #2675: a count is reported only where nothing
    can be hidden. Under Nomos `enforce` it could include passages the caller
    may not read, so it is withheld; `off` and `audit` hide no row.
    """
    from okg.substrate.nomos.runtime_enforcement import (
        deployment_nomos_enforcement_mode,
    )

    return (
        deployment_nomos_enforcement_mode(session.deployment_name) == "enforce"
    )


def record_identity(value: Any) -> Any:
    """The row's stored source-record identity, or None when it carries none.

    `graph_nodes.source_record_id` / `source_revision` are `JSONB NOT NULL
    DEFAULT '{}'`, so a row a producer never attributed holds the EMPTY OBJECT,
    not SQL NULL. Emitting `{}` would put a value-shaped thing on the card that
    an agent could mistake for an identity it can cite or diff, so the empty
    container is normalized to an explicit JSON null and everything else is
    passed through verbatim. Nothing here derives an identity from node_id,
    path, or any other attribute.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list, str)) and not value:
        return None
    return value


def _bounded_excerpt(
    text: str, query: str, *, max_chars: int = DEFAULT_PREVIEW_CHARS,
) -> str:
    if len(text) <= max_chars:
        return text
    terms = [term.casefold() for term in query.split() if term.strip()]
    haystack = text.casefold()
    first_hit = min(
        (idx for term in terms if (idx := haystack.find(term)) >= 0),
        default=0,
    )
    start = max(0, first_hit - max_chars // 3)
    prefix = "..." if start > 0 else ""
    suffix = "..." if start + max_chars < len(text) else ""
    body_budget = max_chars - len(prefix) - len(suffix)
    body = text[start: start + body_budget]
    if start + body_budget >= len(text):
        suffix = ""
    return f"{prefix}{body}{suffix}"


def _preview_winning_passages(
    conn: psycopg.Connection,
    session: Session,
    results: list[dict[str, Any]],
    full_rows: list[dict[str, Any]],
    chunks: dict[str, dict[str, Any]],
    *,
    matcher: ExcerptMatcher,
    max_chars: int,
    generation_id: Any,
    locate: bool = False,
) -> None:
    """Replace each passage-ranked row's excerpt with its winning passage.

    With `locate`, each such row also gets its `location`
    (`LOCATION_KEY`, PACT read-model-s5-location): the winning chunk, the
    offset where this excerpt's window starts in its text, and its text
    hash, from the same window choice as the excerpt.

    A parent such as a `document` carries only locator text (its title and
    path), so an excerpt built from the parent answers nothing; the text that
    ranked it is on the chunk `rollup_best_chunk_id` names (issue #2670).

    The passage is read as a direct hit on it would be: effective attrs at the
    pin (a redacted-attrs result from Nomos is kept as-is), fenced by its OWN
    producer's read trust and labelled with that producer. It is ALSO fenced
    when the parent row is untrusted, so the boundary is never weaker than
    either row's. The text is built from the raw passage and fenced here,
    never by re-fencing the parent's already-serialized excerpt. `results`
    and `full_rows` are index-aligned (`_fence_search_rows` maps 1:1).
    """
    if not locate and not any("excerpt" in row for row in results):
        return
    # Only rows with a winning passage; a folded record whose own text won
    # keeps its own excerpt. `generation_id=None`: the winners are already
    # read-filtered and overlaid.
    indices = [
        i for i, full in enumerate(full_rows) if str(full["node_id"]) in chunks
    ]
    results = [results[i] for i in indices]
    full_rows = [full_rows[i] for i in indices]
    winners = [chunks[str(full["node_id"])] for full in full_rows]
    if generation_id is not None:
        winners = apply_effective_attrs_to_rows(
            conn,
            winners,
            target_kind="node",
            target_id_field="node_id",
            generation_id=generation_id,
        )
    probes = serialize_node_read_rows(
        conn,
        [
            {"source": row.get("source"), "attrs": row.get("attrs")}
            for row in (*full_rows, *winners)
        ],
        deployment_name=session.deployment_name,
    )
    parent_probes = probes[: len(full_rows)]
    chunk_probes = probes[len(full_rows):]
    for row, parent_probe, chunk_probe, chunk in zip(
        results, parent_probes, chunk_probes, winners,
    ):
        text = str((chunk.get("attrs") or {}).get("text") or "")
        start, excerpt = window_span(text, matcher, max_chars=max_chars)
        if locate:
            location = chunk_location(chunk, start)
            if location is not None:
                row[LOCATION_KEY] = location
        if "excerpt" not in row:
            continue
        untrusted = "untrusted" in (
            parent_probe["provenance"]["trust"],
            chunk_probe["provenance"]["trust"],
        )
        fenced = serialize_node_read_row(
            {
                "source": chunk.get("source"),
                "excerpt": excerpt,
            },
            trust="untrusted" if untrusted else "trusted",
        )["excerpt"]
        row["excerpt"] = _cap_serialized_excerpt(fenced, max_chars=max_chars)


def _next_offset(
    *,
    offset: int,
    limit: int,
    truncated: bool,
) -> int | None:
    return offset + limit if truncated else None


#: Lexical search ranks the top-N visible matches, then runs the nomos read filter +
#: effective-attrs overlay + attr_filters over them in Python. total_count + pagination
#: are over that final set; the cap bounds the per-candidate work (an unbounded set
#: would be a full scan + nomos over every match). Relevance past the top couple
#: hundred is noise.
_LEXICAL_RANK_CAP = 200

#: Semantic pagination is over the VISIBLE ordered result set (issue #1438).
#: Read-time filtering can drop rows anywhere in the ranking, so a raw window
#: sized to `offset + limit + 1` cannot fill a deep page. The window grows by
#: `_SEMANTIC_RAW_FETCH_GROWTH` until the visible set is deep enough or the
#: ranking is exhausted; `_SEMANTIC_RAW_FETCH_CAP` bounds that work. The cap
#: sits above `_MAX_SEARCH_OFFSET + _MAX_SEARCH_LIMIT` so the deepest legal
#: page is reachable on an unfiltered corpus; past it the page says so
#: (`rank_capped`) rather than claiming a completeness it never verified.
_SEMANTIC_RAW_FETCH_GROWTH = 4
_SEMANTIC_RAW_FETCH_CAP = 20_000

#: Alias candidates for one exact query string are naturally few, so the
#: candidate set is read WHOLE (up to this cap) and paged over the survivors.
#: Sizing the fetch to the page instead made `truncated` a statement about
#: the window rather than about the result set (issue #1438).
_ALIAS_CANDIDATE_CAP = 500


def _authoritative_alias_target(
    conn: psycopg.Connection,
    session: Session,
    query: str,
    *,
    subtype: str | None,
    scope: Optional[ResolvedScope] = None,
) -> str | None:
    """The canonical an exact alias names at the pinned catalog version.

    `okg.aliases` is keyed `PRIMARY KEY (alias, catalog_version_id)`, so an
    (alias, cv) pair names at most one canonical; this returns that row or None.

    Alias `scope` is honoured exactly as `_search_alias` honours it: a scoped
    alias is invisible to an unscoped query, and visible to a query filtered to
    its own subtype. Without this, a `library`-scoped alias could promote its
    canonical into an unrelated unscoped search.

    A resolved search profile extends that on the same terms — an alias scoped
    to ANY subtype in the profile is admitted. This is a DELIBERATE WIDENING
    relative to `analyst-search-result-context`: a `chunk`-scoped alias now
    promotes under a profile containing `chunk`, where the same query with no
    profile still would not. A profile is a statement about which subtypes the
    caller is asking about, so the aliases scoped to those subtypes are in
    scope by the same argument that admits a single `--subtype`.
    """
    where = ["catalog_version_id = %(cv)s", "alias = %(alias)s"]
    params: dict[str, Any] = {
        "cv": session.catalog_version_id,
        "alias": query,
    }
    scopes = list(scope.subtypes) if (scope and scope.subtypes) else []
    if subtype is not None and subtype not in scopes:
        scopes.append(subtype)
    if not scopes:
        where.append("scope IS NULL")
    else:
        where.append("(scope IS NULL OR scope = ANY(%(alias_scopes)s))")
        params["alias_scopes"] = scopes
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT canonical FROM okg.aliases WHERE {' AND '.join(where)}",
            params,
        )
        row = cur.fetchone()
    return str(row[0]) if row is not None else None


def _hydrate_lexical_alias_target(
    conn: psycopg.Connection,
    session: Session,
    *,
    target: str,
    source: str,
    query: str,
    segment: str,
    branch_point_value: int,
    filter: dict[str, Any],
    scope: Optional[ResolvedScope] = None,
) -> dict[str, Any] | None:
    where = [
        "node_id = %(target)s",
        "deleted = FALSE",
        "live_lo <= %(bp)s",
        "%(bp)s < live_hi",
    ]
    params: dict[str, Any] = {"target": target, "bp": branch_point_value}
    subtype = filter.get("subtype")
    if subtype is not None:
        where.append("subtype = %(subtype)s")
        params["subtype"] = subtype
    # Site 3 of 3. Promotion must not be a side door into a scoped page: a
    # canonical whose subtype is outside the profile stays out, even when the
    # query exactly matches an alias that names it.
    scope_sql = _scope_sql(scope, params, column="subtype")
    if scope_sql is not None:
        where.append(scope_sql)
    # Promotion is a side door into every selection, not only the subtype one:
    # a canonical outside the applied families, or under an excluded path, stays
    # out even when the query exactly matches an alias that names it.
    family_sql = _family_sql(scope, params, column="source")
    if family_sql is not None:
        where.append(family_sql)
    # Promotion is a side door into the caller's producer filter too: a
    # canonical another producer wrote stays out even when the query exactly
    # matches an alias that names it.
    source_sql = _source_sql(filter, params, column="source")
    if source_sql is not None:
        where.append(source_sql)
    where.extend(_path_exclusion_sql(scope, params, column="attrs"))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"SELECT node_id, subtype, attrs, text_hash, source, "
            f"source_record_id, source_revision "
            f"FROM {segment} WHERE {' AND '.join(where)} LIMIT 1",
            params,
        )
        row = cur.fetchone()
    if row is None:
        return None
    visible = filter_node_rows_for_session(conn, session, [row])
    if not visible:
        return None
    effective = apply_effective_attrs_to_rows(
        conn,
        visible,
        target_kind="node",
        target_id_field="node_id",
        generation_id=session.generation_id,
    )[0]
    if not _matches_filter(effective, _row_filter(filter)):
        return None
    text = str((effective.get("attrs") or {}).get("text", ""))
    return {
        **effective,
        "score": -1.0,
        "excerpt": _bounded_excerpt(text, query),
        "match_kind": source,
    }


def _effective_view_lexical_candidates(
    conn: psycopg.Connection,
    session: Session,
    *,
    query: str,
    filter: dict[str, Any],
    segment: str,
    branch_point_value: int,
    exclude_node_ids: set[str],
    scope: Optional[ResolvedScope] = None,
    effective_view_source: Any = None,
    scope_rows: Optional[Union["_RecordScope", "_ChildrenScope"]] = None,
) -> list[dict[str, Any]]:
    attr_filters = filter.get("attr_filters") or {}
    if not attr_filters:
        return []
    terms = [term.strip() for term in query.split() if term.strip()]
    if not terms:
        return []

    if effective_view_source is not None:
        _base_table, from_clause, gen_where, gen_params = (
            effective_view_source(
                conn,
                session.generation_id,
                target_kind="node",
                source_alias="v",
            )
        )
        source_from = from_clause
        source_generation = gen_where
        params: dict[str, Any] = dict(gen_params)
    else:
        # Direct Python callers predating the MCP server seam retain the
        # snapshot-only behavior; the public MCP wrapper always supplies the
        # pinned source resolver above.
        source_from = (
            f"{ATTRIBUTE_EFFECTIVE_VIEW_TABLE} v "
            "JOIN okg.attribute_versions av "
            "ON av.target_kind = v.target_kind "
            "AND av.target_id = v.target_id "
            "AND av.attrs_hash = v.attrs_hash"
        )
        source_generation = "v.generation_id = %(generation_id)s"
        params = {"generation_id": str(session.generation_id)}
    where = [
        "v.target_kind = 'node'",
        source_generation,
        "v.namespace = %(namespace)s",
        "av.attrs ? 'text'",
        "av.attrs @> %(attr_filters)s",
    ]
    params.update({
        "namespace": DEFAULT_NAMESPACE,
        "attr_filters": Jsonb(attr_filters),
        "bp": branch_point_value,
        "limit": _LEXICAL_RANK_CAP,
    })
    subtype = filter.get("subtype")
    if subtype is not None:
        where.append("v.subtype = %(subtype)s")
        params["subtype"] = subtype
    # Site 2 of 3. This query carries its OWN `LIMIT _LEXICAL_RANK_CAP` and its
    # rows are unioned into the lexical candidate set, so without the scope here
    # an `attr_filters` query under a profile would return out-of-scope rows
    # while the envelope reported a scope — making the disclosure a lie.
    scope_sql = _scope_sql(scope, params, column="v.subtype")
    if scope_sql is not None:
        where.append(scope_sql)
    # Same argument as the scope above: this query carries its OWN cap and its
    # rows join the lexical candidate set, so without the family and exclusion
    # predicates here the envelope's disclosure would be a lie about these rows.
    family_sql = _family_sql(scope, params, column="n.source")
    if family_sql is not None:
        where.append(family_sql)
    source_sql = _source_sql(filter, params, column="n.source")
    if source_sql is not None:
        where.append(source_sql)
    where.extend(
        _path_exclusion_sql(scope, params, column="av.attrs", prefix="vglob")
    )
    if scope_rows is not None:
        # This read joins the candidate set with its OWN cap, so a record
        # scope must hold here too or it would add out-of-scope rows.
        where.append(scope_rows.sql(params, column="v.target_id"))
    if exclude_node_ids:
        where.append("v.target_id <> ALL(%(exclude_node_ids)s::text[])")
        params["exclude_node_ids"] = sorted(exclude_node_ids)
    for idx, term in enumerate(terms):
        key = f"term_{idx}"
        where.append(f"(av.attrs->>'text') ILIKE %({key})s")
        params[key] = f"%{term}%"

    where_sql = " AND ".join(where)
    with conn.cursor(row_factory=dict_row) as cur:
        # schema-interval-narrow-split: the snapshot row carries attrs_hash; the
        # payload is materialized through the version-store join.
        cur.execute(
            f"""
            SELECT v.target_id AS node_id,
                   v.subtype,
                   av.attrs,
                   v.raw_text_hash AS text_hash,
                   n.source,
                   n.source_record_id,
                   n.source_revision,
                   -0.000001::float8 AS score
              FROM {source_from}
              LEFT JOIN {segment} n
                ON n.node_id = v.target_id
               AND n.deleted = FALSE
               AND n.live_lo <= %(bp)s
               AND %(bp)s < n.live_hi
             WHERE {where_sql}
             ORDER BY v.target_id
             LIMIT %(limit)s
            """,
            params,
        )
        return cur.fetchall()


#: Rows examined by the zero-hit diagnostic. One bounded read, never a
#: full-table `ILIKE '%needle%'` scan — the genuinely-absent case is the
#: common one, and it is the case that would scan every text-bearing row.
#: When the subtype has more readable rows than this, the diagnostic says
#: so (`zero_hit_scanned: "sample"`) instead of generalizing.
_DIAGNOSTIC_ROW_CAP = 2000


def _diagnose_subtype_zero_hits(
    conn: psycopg.Connection,
    session: Session,
    *,
    segment: str,
    subtype: str,
    query: str,
    branch_point_value: Any,
    generation_id: Any,
) -> tuple[Optional[bool], str, str]:
    """Explain a subtype-filtered lexical search that BM25 did not match.

    Per `index-document-source-file-search` (issue #900). Returns
    `(subtype_indexed, reason, scanned)`:

      no_rows       -> (None, ...)  no row of this subtype is READABLE by
                       this session here. `subtype_indexed` is NULL, never
                       False: "you named a subtype that isn't here" and
                       "this subtype is not searchable" are different
                       facts and must not collapse into one value.
      not_indexed   -> (False, ...) readable rows exist, none carry
                       `attrs.text`. The BM25 index is partial on
                       `attrs ? 'text'`, so no query can ever match. This
                       was `source_file`.
      not_tokenized -> (True, ...)  a readable row's text CONTAINS the
                       query as a substring, yet BM25 did not match it —
                       the text is indexed but the text-search config did
                       not lex the query out of it (a whole path is one
                       `file` lexeme under `english`). This was
                       `document`, and it is why key-presence alone is an
                       inadequate diagnostic.
      no_match      -> (True, ...)  indexed, tokenized, genuinely absent.

    `scanned` is `"all"` when every readable row of the subtype was
    examined, `"sample"` when the cap was hit — in which case
    `not_indexed` and `no_match` are conclusions about the rows examined,
    not about the subtype, and the caller can see that — and `"none"`
    when no row was readable, which qualifies nothing and deliberately
    reveals nothing about how many rows were withheld.

    Two properties this MUST hold, both of which an earlier shape got
    wrong:

    * It reads only what the caller may read. Rows go through
      `filter_node_rows_for_session` and the effective-attrs overlay,
      exactly like the main result path. Probing the raw segment would
      turn this into a substring-existence oracle over policy-denied and
      redacted content.
    * It is never inferred from the subtype's catalog declaration. A
      declaration that was never re-stamped would claim searchable rows
      that do not exist.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT node_id, subtype, attrs, text_hash, source
              FROM {segment}
             WHERE subtype = %(subtype)s
               AND deleted = FALSE
               AND live_lo <= %(bp)s
               AND %(bp)s < live_hi
             ORDER BY node_id
             LIMIT {_DIAGNOSTIC_ROW_CAP + 1}
            """,
            {"subtype": subtype, "bp": branch_point_value},
        )
        sampled = cur.fetchall()

    scanned = "all" if len(sampled) <= _DIAGNOSTIC_ROW_CAP else "sample"
    sampled = sampled[:_DIAGNOSTIC_ROW_CAP]
    readable = filter_node_rows_for_session(conn, session, sampled)
    if not readable:
        # `scanned` is measured on the RAW fetch, so reporting "sample"
        # here would tell a caller who may read nothing that the subtype
        # holds more than the cap — a cardinality leak of exactly the kind
        # the read filter exists to prevent. When nothing is readable
        # there is nothing to qualify, so say nothing.
        return None, "no_rows", "none"
    readable = apply_effective_attrs_to_rows(
        conn, readable, target_kind="node", target_id_field="node_id",
        generation_id=generation_id,
    )

    texts = [
        str((row.get("attrs") or {}).get("text"))
        for row in readable
        if isinstance((row.get("attrs") or {}).get("text"), str)
    ]
    if not texts:
        return False, "not_indexed", scanned

    # Substring probe in Python, not `ILIKE '%needle%'` in SQL: it runs
    # over the already-fetched readable rows (no second, unbounded,
    # leading-wildcard scan) and there are no LIKE metacharacters to
    # escape — an unescaped `_`, as in `Background_Estimation`, is a
    # single-character wildcard and would manufacture a false
    # `not_tokenized`.
    needle = query.strip().casefold()
    if needle and any(needle in text.casefold() for text in texts):
        return True, "not_tokenized", scanned
    return True, "no_match", scanned


def _resolve_expansion(
    query: str,
    *,
    scope: Optional[ResolvedScope],
    rank_by: str,
    expand_concepts: bool,
) -> ExpandedQuery:
    """The concept expansion this lexical search runs under.

    Off, with a stated reason, in three cases: `expand_concepts=False`; no
    groups declared; or `rank_by="passages"`, where the ranked unit is a CHUNK
    and the returned row is its parent, so `_expansion_rank` could not tell an
    exact parent from an expanded one and the exact-beats-expanded guarantee
    would be a claim rather than a fact."""
    if not expand_concepts:
        return ExpandedQuery(
            query=query, applied=False, reason="disabled_by_request",
        )
    if rank_by == RANK_BY_PASSAGES:
        return ExpandedQuery(
            query=query, applied=False, reason="rank_by_passages_unsupported",
        )
    groups = scope.concept_groups if scope is not None else ()
    return expand_query(query, groups)


def _expansion_rank(row: dict[str, Any], expansion: ExpandedQuery) -> int:
    """`0` (exact) unless expansion fired and this row carries none of the
    caller's own tokens.

    An alias-promoted row is always exact: it is on the page because the
    caller's OWN query matched a catalog alias naming it (issue #797), which
    demoting on its body text would silently undo."""
    if not expansion.applied or row.get("match_kind"):
        return 0
    if "original_query_match" in row:
        return 0 if row["original_query_match"] else 1
    text = (row.get("attrs") or {}).get("text", "")
    return expansion_rank(text, expansion.original_terms)


def _phrase_verdict(
    texts: Iterable[str], phrases: Sequence[re.Pattern[str]],
) -> tuple[bool, frozenset[int]]:
    """`(holds_every_span, spans_found)` over the texts a row is served from.

    A row matches when ONE of its texts holds every span (its own ranked text,
    or its winning passage); `spans_found` is every span index any of them
    holds, for the disclosure's unmatched spans (PACT
    lexical-quoted-phrases-3064)."""
    whole = False
    found: set[int] = set()
    for text in texts:
        hits = {i for i, pattern in enumerate(phrases) if pattern.search(text)}
        found |= hits
        whole = whole or len(hits) == len(phrases)
    return whole, frozenset(found)


def _served_text(row: Mapping[str, Any]) -> str:
    return str((row.get("attrs") or {}).get("text", ""))


def _mark_phrase(row: dict[str, Any], phrases: Sequence[re.Pattern[str]]) -> None:
    """Set the row's phrase verdict from its own served text."""
    row[PHRASE_KEY], row[PHRASE_SPANS_KEY] = _phrase_verdict(
        [_served_text(row)], phrases,
    )


def _promote_phrase_prefix(
    conn: psycopg.Connection,
    session: Session,
    rows: list[dict[str, Any]],
    *,
    ordered_exactly_through: int,
    self_ids: set[str],
    head_id: Optional[str],
    phrases: Sequence[re.Pattern[str]],
) -> list[dict[str, Any]]:
    """Phrase-first order inside the filtered fold's exact prefix only.

    The fold's passages were decided per Nomos decision class, not read, so
    their stored text may differ from what a reader is served. Each prefix
    row's winning passage is read here as the reader would see it (masked by
    the read filter when a Nomos bundle can mask, then overlaid) and the
    record's verdict becomes its own ranked text's OR that passage's. Under
    Nomos audit/enforce this class read writes `read` decisions for prefix
    winners the page may not serve (it serves at most `limit` of them);
    the served ones are read row by row again by `read_returned_rows`.

    The prefix is stably re-sorted by phrase rank (an alias head stays
    first); `ordered_exactly_through` is NOT recomputed, and rows past it keep
    today's order, since their rank is not exact (PACT
    lexical-quoted-phrases-3064)."""
    prefix = rows[:ordered_exactly_through]
    winners = [
        {k: v for k, v in row[WINNER_KEY].items() if not k.startswith("_fold")}
        for row in prefix if row.get(WINNER_KEY) is not None
    ]
    served: dict[str, dict[str, Any]] = {}
    if winners:
        read = (
            filter_rows_by_read_class(conn, session, winners, row_kind="node")
            if search_where.masking_possible(session.deployment_name)
            else winners
        )
        read = apply_effective_attrs_to_rows(
            conn, read, target_kind="node", target_id_field="node_id",
            generation_id=session.generation_id,
        )
        served = {str(row["node_id"]): row for row in read}
    for row in prefix:
        texts = [_served_text(row)] if str(row["node_id"]) in self_ids else []
        winner = row.get(WINNER_KEY)
        if winner is not None:
            read_winner = served.get(str(winner["node_id"]))
            if read_winner is not None:
                texts.append(_served_text(read_winner))
        row[PHRASE_KEY], row[PHRASE_SPANS_KEY] = _phrase_verdict(texts, phrases)
    head = 1 if prefix and head_id is not None and str(prefix[0]["node_id"]) == head_id else 0
    ordered = prefix[:head] + sorted(
        prefix[head:], key=lambda row: 0 if row.get(PHRASE_KEY) else 1,
    )
    return ordered + rows[ordered_exactly_through:]


# ---------- rank_by="records" (PACT search-result-shape) ----------

_FOLD_UNFILTERED = "unfiltered"
_FOLD_FILTERED = "filtered"


def _records_shape(
    conn: psycopg.Connection, session: Session, subtype: Optional[str],
) -> tuple[str, Optional[str], Optional[Any]]:
    """`(result_unit, fold_shape, rollup)` of a `records` page.

    No subtype filter: the unfiltered fold over the one ranked read. A filter
    naming a passage subtype asked for passages: nothing folds and every row
    is a passage. A filter naming a subtype that declares a passage relation:
    the filtered two-read fold (D3-i). Any other subtype ranks by its own
    text; there is nothing to fold.
    """
    if subtype is None:
        return "record", _FOLD_UNFILTERED, None
    if passage_child_declarations(
        conn,
        catalog_version_id=session.catalog_version_id,
        child_subtypes=[subtype],
    ):
        return "passage", None, None
    try:
        rollup = passage_rollup_declaration(
            conn,
            catalog_version_id=session.catalog_version_id,
            subtype=subtype,
        )
    except SubstrateError:
        return "record", None, None
    return "record", _FOLD_FILTERED, rollup


@dataclass(frozen=True)
class _RecordScope:
    """A lexical record scope: the readable named records and their passages.

    `record_ids` holds only the named records this reader may see, so a
    hidden record and a missing one leave the same empty set and the page
    cannot tell them apart. `edge_types` are the passage relations the pinned
    catalog declares for those records' subtypes (parent `src`, passage
    `dst`, as `resolve_fold_links` reads them).
    """

    record_ids: tuple[str, ...]
    edge_types: tuple[str, ...]
    edge_segment: str

    def sql(self, params: dict[str, Any], *, column: str) -> str:
        if not self.record_ids:
            return "FALSE"
        params["scope_record_ids"] = list(self.record_ids)
        if not self.edge_types:
            return f"{column} = ANY(%(scope_record_ids)s)"
        params["scope_edge_types"] = list(self.edge_types)
        return (
            f"({column} = ANY(%(scope_record_ids)s) OR {column} IN ("
            f"SELECT se.dst FROM {self.edge_segment} se "
            f"WHERE se.src = ANY(%(scope_record_ids)s) "
            f"AND se.edge_type = ANY(%(scope_edge_types)s) "
            f"AND se.deleted = FALSE AND se.live_lo <= %(bp)s "
            f"AND %(bp)s < se.live_hi))"
        )


def _resolve_record_scope(
    conn: psycopg.Connection,
    session: Session,
    record_ids: Sequence[str],
    *,
    segment: str,
    branch_point_value: Any,
) -> _RecordScope:
    """The readable records among `record_ids` at the pin, and their relations.

    Visibility is decided per Nomos decision class (a count decision): the
    record itself is not returned by this read, only used to scope it. A
    record the reader may not see is dropped here exactly as an id that names
    nothing, so both scope to the empty set.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"SELECT node_id, subtype, attrs, text_hash, source, "
            f"source_record_id, source_revision FROM {segment} "
            f"WHERE node_id = ANY(%(ids)s) AND deleted = FALSE "
            f"AND live_lo <= %(bp)s AND %(bp)s < live_hi ORDER BY node_id",
            {"ids": list(record_ids), "bp": branch_point_value},
        )
        rows = cur.fetchall()
    visible = visible_ids_by_decision_class(conn, session, rows, row_kind="node")
    readable = [row for row in rows if str(row["node_id"]) in visible]
    edge_types: set[str] = set()
    for subtype in sorted({str(row.get("subtype") or "") for row in readable}):
        try:
            rollup = passage_rollup_declaration(
                conn,
                catalog_version_id=session.catalog_version_id,
                subtype=subtype,
            )
        except SubstrateError:
            continue  # the record declares no passages: it scopes to itself
        edge_types.add(rollup.edge_type)
    return _RecordScope(
        record_ids=tuple(sorted(str(row["node_id"]) for row in readable)),
        edge_types=tuple(sorted(edge_types)),
        edge_segment=resolve_edge_segment(conn) if edge_types else "",
    )


@dataclass(frozen=True)
class _ChildrenScope:
    """A lexical `children_of` scope (issue #3363): what one readable parent
    reaches by its outgoing `edge_type` edges, never the parent itself.

    The children are selected in SQL, not listed, so a parent with many
    children costs one subquery. Served under `rank_by='self'` only: each row
    is judged by itself, so a hidden child is dropped by the read filter as
    a record scope's hidden passages are, and nothing folds a child into a
    record outside the scope. The edges are checked for liveness, as the
    record scope's passage edges are. `record_ids` is empty: no named record
    exists for alias promotion to land on.
    """

    parent: str
    edge_type: str
    edge_segment: str
    record_ids: tuple[str, ...] = ()

    def sql(self, params: dict[str, Any], *, column: str) -> str:
        params["scope_parent"] = self.parent
        params["scope_child_edge"] = self.edge_type
        return (
            f"{column} IN (SELECT ce.dst FROM {self.edge_segment} ce "
            f"WHERE ce.src = %(scope_parent)s "
            f"AND ce.edge_type = %(scope_child_edge)s "
            f"AND ce.dst <> %(scope_parent)s AND ce.deleted = FALSE "
            f"AND ce.live_lo <= %(bp)s AND %(bp)s < ce.live_hi)"
        )


def _resolve_children_scope(
    conn: psycopg.Connection,
    session: Session,
    children: search_where.ChildrenScope,
    *,
    segment: str,
    branch_point_value: Any,
) -> Union[_RecordScope, _ChildrenScope]:
    """The scope `children_of` names, for this reader at the pin.

    A parent the reader may not see scopes to the empty set exactly as a
    missing id does (the record scope's rule). For a readable parent an
    unset `edge_type` is its declared passage relation; a parent that
    declares none is refused and asked for one.
    """
    parent_scope = _resolve_record_scope(
        conn, session, [children.parent], segment=segment,
        branch_point_value=branch_point_value,
    )
    if not parent_scope.record_ids:
        return _RecordScope(record_ids=(), edge_types=(), edge_segment="")
    edge_type = children.edge_type or (
        parent_scope.edge_types[0] if parent_scope.edge_types else None
    )
    if edge_type is None:
        raise search_where.SearchScopeUnsupported(
            "search: scope.children_of needs an edge_type: this record "
            "declares no passage relation",
            data={"scope": "children_of"},
            fix_hint='pass scope={"children_of": id, "edge_type": "<edge type>"}',
        )
    return _ChildrenScope(
        parent=children.parent, edge_type=edge_type,
        edge_segment=resolve_edge_segment(conn),
    )


def _selects_on_attrs(
    filter: dict[str, Any], scope: Optional[ResolvedScope],
) -> bool:
    """Whether the page tests rows' attributes (`attr_filters`, path
    exclusions): fetched parents must then be masked first (review B1)."""
    return (
        bool(filter.get("attr_filters"))
        or search_where.reads_attrs(filter.get("where"))
        or bool(scope is not None and scope.excluded_path_globs)
    )


def _fold_unfiltered(
    conn: psycopg.Connection,
    session: Session,
    candidates: list[dict[str, Any]],
    *,
    declared_children: set[str],
    alias_node_id: Optional[str],
    segment: str,
    branch_point_value: Any,
    filter: dict[str, Any],
    scope: Optional[ResolvedScope],
    phrase_first: bool = False,
) -> list[dict[str, Any]]:
    """D1: fold each passage into its record; an unfolded passage stays a row.

    The candidates' relation edges and the parents fetched from outside the
    ranked read are scan-only: decided once per Nomos decision class
    (`count`, decision 142a). The rows the page serves are read row by row
    after paging (`read_returned_rows`). An alias-promoted passage stays a
    passage row at the head and is not folded (rule 8). `attr_filters` describe the returned row: a folded passage's
    record must pass them, an unfolded passage must pass them itself, except
    one kept through a record outside the profile (issue #3466), which is
    judged through that record, as a `where` judges a passage (D6).
    """
    passages = [
        row for row in candidates
        if str(row.get("subtype") or "") in declared_children
        and str(row["node_id"]) != alias_node_id
    ]
    passage_ids = {str(row["node_id"]) for row in passages}
    records = [row for row in candidates if str(row["node_id"]) not in passage_ids]
    if passages:
        links, fetched = resolve_fold_links(
            conn,
            session,
            passages=passages,
            known_parents={str(row["node_id"]): row for row in records},
            node_segment=segment,
            edge_segment=resolve_edge_segment(conn),
            branch_point_value=branch_point_value,
            check=CHECK_CLASS,
            mask_fetched_parents=_selects_on_attrs(filter, scope),
        )
    else:
        links, fetched = {}, {}
    takes = declaring_subtypes(conn, session.catalog_version_id)
    # Issue #3466: a parent the `where` admits but the profile scopes out
    # (a content profile without `document`) keeps its passages as their own
    # rows, matched through it. They skip the own-row check below, which
    # would judge a chunk by a path only its document carries.
    through_parent: set[str] = set()

    # Only the profile's subtype list is relaxed: a parent excluded by path,
    # by producer family or by the `where` still drops its passages.
    subtypes_relaxed = (
        replace(scope, subtypes=None)
        if scope is not None and scope.subtypes is not None else None
    )

    def matched_outside_scope(parent: Mapping[str, Any]) -> bool:
        return (
            subtypes_relaxed is not None
            and filter.get("where") is not None
            and not _matches_filter(dict(parent), filter, scope=scope)
            and _matches_filter(dict(parent), filter, scope=subtypes_relaxed)
        )

    for passage in passages:
        link = links.get(str(passage["node_id"]))
        parent = fetched.get(link.parent_id) if link is not None else None
        if parent is not None and matched_outside_scope(parent):
            through_parent.add(str(passage["node_id"]))
    rows = fold_members(
        records=records,
        passages=passages,
        links=links,
        fetched_parents=fetched,
        parent_admitted=lambda parent: _matches_filter(parent, filter, scope=scope),
        keep_when_parent_matches=matched_outside_scope,
        keep_unfolded=True,
        takes_passages=lambda row: str(row.get("subtype") or "") in takes,
        head_id=alias_node_id,
        # D6: under a residual `where` a passage matches through its record.
        # The old arguments keep today's rule (an unfolded passage is judged
        # by itself), so their pages stay byte-identical (D9).
        drop_when_parent_rejected=filter.get("where") is not None,
        # A quoted search: a passage holding every span wins its record, and
        # such records lead (PACT lexical-quoted-phrases-3064).
        phrase_first=phrase_first,
    )
    if _has_row_filter(filter):
        rows = [
            row for row in rows
            if str(row["node_id"]) in through_parent
            or _matches_filter(row, _row_filter(filter))
        ]
    return rows


def _unfolded_passage_ids(
    conn: psycopg.Connection,
    session: Session,
    page: list[dict[str, Any]],
    *,
    declared_children: set[str],
    promoted_id: Optional[str],
) -> set[str]:
    """Ids of the rows on an unfiltered records page that are passages.

    A folded passage leaves no row of its own, so a row whose type is a
    declared passage child is one the fold kept. `declared_children` was read
    over the ranked candidates; only the alias-promoted row can carry a type
    outside it, and only then is the pinned catalog asked again.
    """
    passage_types = set(declared_children)
    if promoted_id is not None:
        promoted = [
            str(row.get("subtype") or "") for row in page
            if str(row["node_id"]) == promoted_id
        ]
        unknown = sorted(set(promoted) - declared_children)
        if unknown:
            passage_types |= set(passage_child_declarations(
                conn,
                catalog_version_id=session.catalog_version_id,
                child_subtypes=unknown,
            ))
    return {
        str(row["node_id"]) for row in page
        if str(row.get("subtype") or "") in passage_types
    }


#: At most this many copy ids are listed on a kept row (`copies`); when more
#: were collapsed, `copies_total` says how many.
COPIES_LISTED_MAX = 10


def _collapse_exact_duplicates(
    rows: list[dict[str, Any]],
    *,
    self_read: Optional[set[str]],
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    """Collapse a `records` page's exact duplicates (issue #3465, `rank3465 A`).

    Two rows are copies when their type, stored `text_hash` and served text
    are equal: the same file or document chunk published at another path or
    release. The first row in rank order stays; every later copy leaves the
    list and is returned as an id under the kept row's id, in rank order.
    Rows are never reordered.

    Only a row that matched on its own text takes part. A record that
    matched through a passage (a fold winner, or folded matching passages)
    neither collapses nor keeps copies: equal record text says nothing about
    the passages that ranked it. That also excludes every parent a fold
    fetched, whose text is read only after paging, so Nomos off, audit and
    enforce group the same rows. `self_read` names the rows already read row
    by row (None: every row was); their `attrs` are the read-filtered,
    overlaid values the page serves, and a row whose text Nomos masked has
    lost its `text_hash` there. A row with no `text_hash` or empty text never
    collapses.
    """
    kept: list[dict[str, Any]] = []
    copies: dict[str, list[str]] = {}
    heads: dict[tuple[str, str, str], str] = {}
    for row in rows:
        node_id = str(row["node_id"])
        text = _served_text(row)
        text_hash = row.get("text_hash")
        if (
            (self_read is not None and node_id not in self_read)
            or row.get(WINNER_KEY) is not None
            or int(row.get(MATCHING_KEY) or 0) > 0
            or text_hash is None
            or not text.strip()
        ):
            kept.append(row)
            continue
        key = (
            str(row.get("subtype") or ""),
            str(text_hash),
            hashlib.sha256(text.encode("utf-8")).hexdigest(),
        )
        head = heads.get(key)
        if head is None:
            heads[key] = node_id
            kept.append(row)
        else:
            copies.setdefault(head, []).append(node_id)
    return kept, copies


class _FilteredFold:
    """The filtered two-read fold's page source and its exactness facts."""

    __slots__ = (
        "rows", "ordered_exactly_through", "lower_bound", "self_ids",
        "passages_read", "passages_saturated", "passage_read_fallback",
        "expansion_remedy",
    )

    def __init__(
        self,
        *,
        rows: list[dict[str, Any]],
        ordered_exactly_through: int,
        lower_bound: bool,
        self_ids: set[str],
        passages_read: int,
        passages_saturated: bool,
        passage_read_fallback: Optional[str] = None,
        expansion_remedy: Optional[dict[str, Any]] = None,
    ) -> None:
        self.rows = rows
        self.ordered_exactly_through = ordered_exactly_through
        self.lower_bound = lower_bound
        self.self_ids = self_ids
        self.passages_read = passages_read
        self.passages_saturated = passages_saturated
        #: Why a passage read ran the previous full statement, if one did.
        self.passage_read_fallback = passage_read_fallback
        #: The count is a floor only because a read of the EXPANDED query
        #: filled its limit (no typed-word read did), so rerunning with
        #: `expand_concepts=false` once per word reaches the records past the
        #: exact prefix, which narrowing alone may not (review 3, item 2).
        #: The full remedy (`expansion_remedy_args()`), or None.
        self.expansion_remedy = expansion_remedy


def expansion_remedy_args(expansion: ExpandedQuery) -> dict[str, Any]:
    """The arguments that reach records a filled expanded read left past
    the exact prefix: rerun without concept expansion, once per word in
    `rerun_each` (the whitespace words of `concept_expansion.expanded_query`:
    the typed words, then the added terms' words), one call per word.

    The per-word pages are separate rankings: they cannot be merged back
    into the expanded query's order, and together they hold every record
    only when each rerun is itself complete (each word matches fewer
    records than the candidate limit, which an exact rerun's `exhaustive`
    retrieval proves).
    """
    return {
        "expand_concepts": False,
        "rerun_each": list(dict.fromkeys(tokenize(expansion.query))),
    }


_EXPANSION_REMEDY_HINT = (
    "concept expansion filled a candidate read: rerun with "
    "expand_concepts=false once per word in data.remedy.rerun_each to reach "
    "later records (separate rankings, complete only when each rerun is), "
    "or narrow the query so fewer records match"
)


_log = logging.getLogger(__name__)

#: The filtered fold's passage read (PACT mcp-filtered-passage-cost) walks the
#: BM25 index's ordered scan in windows: the first holds `_PASSAGE_FIRST_WINDOW`
#: index rows and each next one `_PASSAGE_WINDOW_GROWTH` times more. Windows
#: count GLOBAL scan positions (every row the index returns, whatever its
#: type), because that is what the index's own cap counts. On the comp-ops
#: clone 1,000 index rows read in about 14 ms, while scoring one passage with
#: `<@>` costs 0.45-0.7 ms; only rows that can be among the best are scored.
_PASSAGE_FIRST_WINDOW = 256
_PASSAGE_WINDOW_GROWTH = 4

#: pg_textsearch 1.3.0 through 1.4.x returns at most this many results from
#: one index scan (`TP_MAX_QUERY_LIMIT`, src/constants.h) and then simply
#: stops: a scan cut there looks exactly like a scan that ran out of matches.
#: The cap counts index results, including entries for dead row versions that
#: PostgreSQL then hides, so the room a read may use is this minus the table's
#: dead row versions (`pg_stat_get_dead_tuples`, PostgreSQL's own count). A
#: read that needs more room runs the previous full statement.
#:
#: pg_textsearch 1.5.0 removed the cut (upstream PR #518): a scan continues
#: until the LIMIT is filled or the matches run out. ops/pg/Dockerfile ships
#: 1.5.1, but a database keeps running its old library until its image is
#: replaced, and CI's database sidecar is a separately pinned image, so the
#: room stays: on 1.5.x it only sends a read that would walk past 100,000
#: positions to the full statement, which is complete on every version.
#: Remove it once no database okg serves runs a library older than 1.5.0
#: (issue #3162).
_PASSAGE_INDEX_SCAN_CAP = 100_000

#: Planner settings for a window: the index's own ordered scan, never a sort,
#: so every window takes its order from the index (as grep's page scan does).
#: JIT stays off: `enable_sort=off` adds a fixed penalty to the estimated cost
#: of any sort a statement keeps (the scoring statement's `ORDER BY s.i`), and
#: window scans with a large LIMIT cost over 100,000 on their own, so both
#: passed the JIT thresholds and PostgreSQL compiled them before reading a row:
#: 185-305 ms each on comp-ops (PACT filtered-passage-rescore-cost). These
#: statements are short, so JIT never pays for itself.
_PASSAGE_INDEX_PLAN = (
    ("enable_seqscan", "off"), ("enable_sort", "off"), ("enable_bitmapscan", "off"),
    ("jit", "off"),
)


#: The largest ratio of a document length to its quantized length in
#: pg_textsearch 1.3.0 through 1.5.1 (`FIELDNORM_DECODE_TABLE`, Lucene's SmallFloat byte4:
#: lengths below 40 exact, then 8 buckets per power of two, so at most 9/8).
_FIELDNORM_MAX_RATIO = 1.125

#: The index's single-term path on written segments stores a document's length
#: in 16 bits (pg_textsearch src/segment/scan.c), so a passage of 65,536 tokens
#: or more can get an index score better than its `<@>` score. A token takes at
#: least one character, so a text shorter than this many characters is safe;
#: a longer one is always scored, never bounded.
_INDEX_LENGTH_SAFE_CHARS = 65_536


def _unread_floor(edge: float) -> float:
    """The lowest score, in index units, a row the scan returns after `edge`
    can have once `<@>` scores it.

    The index orders by its own score (`bm25_get_current_score()`, here
    `s_index`); the previous statement ranked by the `<@>` expression
    (`s_expr`). pg_textsearch 1.3.0 computes both with the same statistics
    and the same BM25 formula; they differ in two ways:

    * the document length `L`. Rows in a written segment: both use the
      quantized length, so the two agree up to float rounding (at most
      1.9e-6 over 1,000 rows on the comp-ops clone). Rows still in the
      in-memory buffer (memtable): the index uses the exact length `L`,
      `<@>` the quantized length `L'`, with `L' <= L <= 9/8 L'`. A term's
      weight `idf * tf * (k1 + 1) / (tf + A + c * L)` (`A, c >= 0`) grows by
      `(tf + A + c * L) / (tf + A + c * L') <= L / L' <= 9/8`;
    * a query of one distinct term repeated `q` times: `<@>` multiplies by
      `q`, the index does not. The factor is the same for every row, so it
      changes no order and no selection, and every comparison here stays in
      index units (`<@>` scores are compared only with `<@>` scores).

    Every weight is positive and a score is minus their sum, so in index
    units `s_expr >= 9/8 * s_index` for every row. An unread row has
    `s_index >= edge`, hence at least `9/8 * edge`, less a float margin.
    This is a bound, not an estimate.
    """
    return _FIELDNORM_MAX_RATIO * edge - _score_rounding(edge)


def _index_score_noise(score: float) -> float:
    """How far two statements' float32 index scores for one row may differ.

    The index computes in float32: the same row came back -15.51657772 in
    one window and -15.51657677 in the previous one, one float32 step
    (2**-20 at that magnitude) apart (issue #3327). This allows about 16
    steps. It must stay far below `_score_rounding`: the order check sees
    the settled row that a move pushed past `OFFSET`, which can score just
    under the edge, so a wider margin would let a real move go unnoticed.
    """
    return max(1e-6, abs(score) * 2.0 ** -20)


def _score_rounding(score: float) -> float:
    """A float margin around a score: hundreds of times the 1.9e-6 gap seen
    between the index's and `<@>`'s scores for the same written row."""
    return max(1e-3, 1e-4 * abs(score))


@dataclass
class _PassageRead:
    """One passage read: the best `_LEXICAL_RANK_CAP` matching passages.

    `index_rows` is the global scan positions read, `rows_scored` the
    candidates scored with `<@>`. `scan` is "index" for the windowed read,
    "full" for the previous statement; `fallback` names why that ran.
    """

    rows: list[dict[str, Any]]
    index_rows: int = 0
    rows_scored: int = 0
    scan: str = "index"
    fallback: Optional[str] = None
    #: The query's visible matches when the full statement was read again
    #: completely (`_IndexScanCap`), else None.
    index_scan_matches: Optional[_ExactLexicalCount] = None


#: Why a passage read ran the previous full statement (disclosed as
#: `passage_read_fallback`).
_FALLBACK_PLAN = "plan_not_index_scan"
_FALLBACK_STATISTICS = "index_statistics_changed"
_FALLBACK_SCAN_CAP = "index_scan_cap"
#: Windowed attempts before a read whose index keeps moving runs the full
#: statement.
_PASSAGE_READ_ATTEMPTS = 3


class _StatisticsChanged(Exception):
    """The index's BM25 statistics, or its order, moved during one read."""


class _OrderMoved(_StatisticsChanged):
    """A window returned a row scoring better than the previous window's
    edge: rows moved in the index order during the read."""


class _ScanCapReached(Exception):
    """The read needs more of the index's scan than its cap guarantees."""


@contextlib.contextmanager
def _index_plan(conn: psycopg.Connection) -> Iterator[psycopg.Cursor]:
    """A cursor whose statements run under `_PASSAGE_INDEX_PLAN`.

    The settings are set for one transaction (a savepoint when the caller
    holds one) and put back before it ends, so they never outlive the
    passage read. Three statements in all, however many windows run.
    """
    names = [name for name, _value in _PASSAGE_INDEX_PLAN]
    current = ", ".join(f"current_setting('{name}') AS {name}" for name in names)
    setter = ", ".join(f"set_config('{name}', %({name})s, true)" for name in names)
    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT {current}")
            previous = cur.fetchone()
            cur.execute(f"SELECT {setter}", dict(_PASSAGE_INDEX_PLAN))
            yield cur
            cur.execute(f"SELECT {setter}", previous)


def _passage_windows_sql(
    *,
    segment: str,
    edge_segment: str,
    index_literal: str,
    keep: Sequence[str],
) -> tuple[str, str, str]:
    """`(index_sql, window_sql, score_sql)` of the windowed passage read.

    `index_sql` is one window of the BM25 index's ordered scan over every
    indexed row (the index's own predicate only), so `OFFSET` and the window
    size count the same global positions as the index's result cap. It has
    the bare `ORDER BY <@>` with no tie-break (a second sort key makes
    PostgreSQL score every row again) and the index's own score; `keep`
    (passage type, visibility at the pin, family, source, path exclusions)
    is a column, not a filter.

    `window_sql` adds the window's facts on every row (row and match counts,
    the worst and best index score, how many rows score strictly better than
    the worst), tests the parent and relation edge once for kept rows only
    (the `LATERAL ... OFFSET 0` keeps the test from being inlined twice), and
    returns kept rows plus one row carrying the facts. `score_sql` computes
    the `<@>` score of chosen passage texts, the number the previous
    statement ranked by (`<@>` reads only the text and the index statistics,
    never the row).

    Both carry `statistics_probe`, the query text scored once per statement:
    it moves whenever the index statistics the scores depend on (document
    count, average length, the query terms' document frequencies) move,
    which are not snapshot-consistent in pg_textsearch (a memtable spill or
    a vacuum changes them for every reader at once).
    """
    order = f"(n.attrs->>'text') <@> to_bm25query(%(q)s, '{index_literal}')"
    probe = (
        f"(SELECT %(q)s::text <@> to_bm25query(%(q)s, '{index_literal}')) "
        f"AS statistics_probe"
    )
    index_sql = (
        f"SELECT n.node_id, n.subtype, n.attrs, n.text_hash, n.source, "
        f"n.source_record_id, n.source_revision, "
        f"bm25_get_current_score() AS index_score, ({' AND '.join(keep)}) AS keep "
        f"FROM {segment} n WHERE n.deleted = FALSE AND n.attrs ? 'text' "
        f"ORDER BY {order} LIMIT %(passage_window)s OFFSET %(passage_offset)s"
    )
    facts = (
        f"SELECT f.*, sum(CASE WHEN f.index_score < f.window_edge THEN 1 ELSE 0 END) "
        f"OVER () AS window_settled, row_number() OVER () AS window_rn FROM ("
        f"SELECT l.*, max(l.index_score) OVER () AS window_edge, "
        f"min(l.index_score) OVER () AS window_low, count(*) OVER () AS window_rows, "
        f"count(*) FILTER (WHERE l.index_score < 0) OVER () AS window_matches "
        f"FROM ({index_sql}) l) f"
    )
    window_sql = (
        f"SELECT w.node_id, w.index_score, w.keep, x.ok, "
        f"CASE WHEN x.ok THEN w.subtype END AS subtype, "
        f"CASE WHEN x.ok THEN w.attrs END AS attrs, "
        f"CASE WHEN x.ok THEN w.text_hash END AS text_hash, "
        f"CASE WHEN x.ok THEN w.source END AS source, "
        f"CASE WHEN x.ok THEN w.source_record_id END AS source_record_id, "
        f"CASE WHEN x.ok THEN w.source_revision END AS source_revision, "
        f"w.window_edge, w.window_low, w.window_rows, w.window_matches, "
        f"w.window_settled, {probe} "
        f"FROM ({facts}) w CROSS JOIN LATERAL (SELECT CASE WHEN w.keep THEN EXISTS ("
        f"SELECT 1 FROM {edge_segment} e JOIN {segment} p ON p.node_id = e.src "
        f"WHERE e.dst = w.node_id AND e.edge_type = %(edge)s "
        f"AND e.deleted = FALSE AND e.live_lo <= %(bp)s AND %(bp)s < e.live_hi "
        f"AND p.subtype = %(parent)s AND p.deleted = FALSE "
        f"AND p.live_lo <= %(bp)s AND %(bp)s < p.live_hi) ELSE FALSE END AS ok "
        f"OFFSET 0) x WHERE w.keep OR w.window_rn = 1"
    )
    # `matched` is the membership test (issue #3232): the `<@>` score of a
    # real match can read 0, so the score ranks and `matched` decides.
    matched = bm25_match_sql(score="s.score", text="s.txt")
    # Scored twice on an old library, keeping the better (issue #3666).
    text_score = steady_bm25_score_sql(f"t.txt <@> to_bm25query(%(q)s, '{index_literal}')")
    score_sql = (
        f"SELECT s.i, s.score, {matched} AS matched, {probe} FROM ("
        f"SELECT t.i, t.txt, {text_score} AS score "
        f"FROM unnest(%(passage_texts)s::text[]) WITH ORDINALITY AS t(txt, i) "
        f"OFFSET 0) s ORDER BY s.i"
    )
    return index_sql, window_sql, score_sql


def _plan_nodes(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield node
    for child in node.get("Plans") or ():
        yield from _plan_nodes(child)


def _passage_index_ordered(
    cur: psycopg.Cursor, index_sql: str, params: dict[str, Any], index_name: str,
) -> bool:
    """Whether PostgreSQL runs a window (under `_index_plan`) as the BM25
    index's own ordered scan with no sort above it, so
    `bm25_get_current_score()` is the row's score in scan order.

    Read from the plan's structure (`EXPLAIN (FORMAT JSON)` node types), never
    its text, which carries the query words, source names and paths.
    """
    execute_with_custom_plan(
        cur, "EXPLAIN (FORMAT JSON) " + index_sql,
        {**params, "passage_window": _PASSAGE_FIRST_WINDOW, "passage_offset": 0},
    )
    document = next(iter(cur.fetchone().values()))
    if isinstance(document, str):
        document = json.loads(document)
    nodes = list(_plan_nodes(document[0]["Plan"]))
    if any(node.get("Node Type") in ("Sort", "Incremental Sort") for node in nodes):
        return False
    return any(
        node.get("Node Type") == "Index Scan"
        and node.get("Index Name") == index_name
        and node.get("Order By")
        for node in nodes
    )


def _ranked_passage_key(row: dict[str, Any]) -> tuple[float, str]:
    return float(row["score"]), str(row["node_id"])


def _filtered_passage_read(
    cur: psycopg.Cursor,
    *,
    window_sql: str,
    score_sql: str,
    params: dict[str, Any],
    scan_room: int,
) -> _PassageRead:
    """The best `_LEXICAL_RANK_CAP` passages, read from the index in windows.

    Windows are read in global index order. A window settles only its rows
    whose index score is strictly better than its worst row's, so rows tied
    at its edge are read again together by the next window (`OFFSET` = the
    rows settled so far): the settled rows are always a prefix of the scan,
    whatever order the index returns ties in. A later window that returns a
    row scoring better than the previous edge, by more than the index's
    float32 noise (`_index_score_noise`), means the order moved under the
    read (a memtable spill improves the spilled rows' index scores, and any
    that jump before `OFFSET` would be skipped): the read restarts.

    Every settled row that is kept and passes the parent test (a candidate)
    has an index score `s`; its `<@>` score, in index units, lies in
    `[_unread_floor(s), s + float]` (see `_unread_floor`). Let `U` be the
    `_LEXICAL_RANK_CAP`-th smallest upper end over the candidates: the true
    cap-th best score is at most `U`. The read stops when the scan ran out
    of matches, or when `U` is strictly below `_unread_floor` of the window's
    edge, the lowest score any unread row can have: no unread row can enter
    the top rows, ties by node id included. Then only candidates whose lower
    end is at most `U` (and every passage too long for the index's 16-bit
    length) are scored with `<@>` in one statement, ranked by
    `(score, node_id)` and capped. The result equals the previous
    statement's `ORDER BY score, node_id LIMIT cap` over the matching
    passages.

    A window that comes back short has run out of matches only if it ended
    before `scan_room` global positions; at `scan_room` the index's cap may
    have cut the scan, so `_ScanCapReached` is raised. Raises
    `_StatisticsChanged` when the index statistics or its order moved.
    """
    candidates: dict[str, dict[str, Any]] = {}
    taken = 0
    window = _PASSAGE_FIRST_WINDOW
    index_rows = 0
    edge: Optional[float] = None
    probe: Optional[float] = None

    def check(rows: list[dict[str, Any]]) -> None:
        nonlocal probe
        if rows:
            current = float(rows[0]["statistics_probe"] or 0.0)
            if probe is not None and current != probe:
                raise _StatisticsChanged()
            probe = current

    def bounded(row: dict[str, Any]) -> bool:
        """Whether the row's index score bounds its `<@>` score."""
        text = (row.get("attrs") or {}).get("text") or ""
        return len(str(text)) < _INDEX_LENGTH_SAFE_CHARS

    def upper_cut() -> float:
        """`U`: the cap-th smallest upper end of a candidate's `<@>` score."""
        uppers = sorted(
            float(row["index_score"]) + _score_rounding(float(row["index_score"]))
            for row in candidates.values() if bounded(row)
        )
        if len(uppers) < _LEXICAL_RANK_CAP:
            return math.inf
        return uppers[_LEXICAL_RANK_CAP - 1]

    def finish() -> _PassageRead:
        limit = upper_cut()
        chosen = [
            row for row in candidates.values()
            if not bounded(row) or _unread_floor(float(row["index_score"])) <= limit
        ]
        if chosen:
            execute_with_custom_plan(cur, score_sql, {
                **params,
                "passage_texts": [
                    str((row.get("attrs") or {}).get("text") or "") for row in chosen
                ],
            })
            scored = cur.fetchall()
            check(scored)
            for row, score in zip(chosen, scored):
                row["score"] = score["score"]
                row["matched"] = score["matched"]
        ranked = sorted(
            (row for row in chosen if row["score"] is not None and row.get("matched")),
            key=_ranked_passage_key,
        )[:_LEXICAL_RANK_CAP]
        out = [
            {key: row[key] for key in _PASSAGE_COLUMNS}
            for row in ranked
        ]
        return _PassageRead(rows=out, index_rows=index_rows, rows_scored=len(chosen))

    while True:
        window = min(window, scan_room - taken)
        if window <= 0:
            raise _ScanCapReached()
        execute_with_custom_plan(
            cur, window_sql,
            {**params, "passage_window": window, "passage_offset": taken},
        )
        rows = cur.fetchall()
        check(rows)
        facts = rows[0] if rows else None
        window_rows = int(facts["window_rows"]) if facts else 0
        index_rows = taken + window_rows
        if facts and edge is not None and (
            float(facts["window_low"]) < edge - _index_score_noise(edge)
        ):
            # A row now scores better than the last window's edge: the
            # index order moved under the read. A gap within the index's
            # float32 noise is rounding, not a move (issue #3327).
            raise _OrderMoved()
        # The index scan returns matches only, so a short window means every
        # match was read -- unless it stopped at the cap's room.
        ran_out = window_rows < window or (
            facts is not None and int(facts["window_matches"]) < window_rows
        )
        if ran_out and taken + window_rows >= scan_room and not (
            facts is not None and int(facts["window_matches"]) < window_rows
        ):
            raise _ScanCapReached()
        kept = [
            row for row in rows
            if row["keep"] and float(row["index_score"] or 0.0) < 0
        ]
        if ran_out:
            settled = kept
        else:
            edge = float(facts["window_edge"])
            settled = [row for row in kept if float(row["index_score"]) < edge]
            taken += int(facts["window_settled"])
        for row in settled:
            if row["ok"]:
                row["score"] = None
                candidates[str(row["node_id"])] = row
        if ran_out:
            return finish()
        assert edge is not None
        if upper_cut() < _unread_floor(edge):
            return finish()
        if taken + window >= scan_room:
            raise _ScanCapReached()
        window *= _PASSAGE_WINDOW_GROWTH


#: The columns a passage row carries, as the previous statement returned them.
_PASSAGE_COLUMNS = (
    "node_id", "subtype", "attrs", "text_hash", "source", "source_record_id",
    "source_revision", "score",
)


def _scan_room(cur: psycopg.Cursor, segment: str) -> int:
    """Global scan positions a read may use: the index's result cap less the
    segment's dead row versions, whose index entries count against the cap
    but are hidden from the read."""
    cur.execute(
        "SELECT pg_stat_get_dead_tuples(%(segment)s::regclass) AS dead",
        {"segment": segment},
    )
    dead = int(cur.fetchone()["dead"] or 0)
    return max(0, _PASSAGE_INDEX_SCAN_CAP - dead)


class LexicalIndexScanCapRefused(SubstrateError):
    """A lexical read the BM25 index's result cap may have cut could not be
    read completely, so the search refuses rather than answer short."""

    code = "lexical_index_scan_cap"


# Exact fallback budgets count scoped text rows, not global index entries.
_EXACT_LEXICAL_WORK_ROWS = 200_000
_EXACT_LEXICAL_TIMEOUT_MS = 5_000
INDEX_SCAN_CAP_MARK = "/* okg:index_scan_cap */"
#: Refusal reasons that say which exact-read bound stopped the read; merged
#: into `_EXACT_BOUND_WITHHELD` when counts are withheld (enforced Nomos).
_EXACT_BOUND_REASONS = frozenset({"scoped_work_limit", "exact_read_timeout"})
_EXACT_BOUND_WITHHELD = "exact_read_bound"


@dataclass(frozen=True)
class _ExactLexicalCount:
    candidates: int
    matches: int
    timeout_ms: int


@contextlib.contextmanager
def _exact_lexical_plan(conn: psycopg.Connection) -> Iterator[tuple[psycopg.Cursor, int]]:
    """Bound the exact read without extending a stricter caller timeout.

    No planner setting is changed: materialization separates input selection
    from scoring. The JSON plan check below rejects a BM25 index scan.
    Transaction/savepoint rollback restores the timeout on cancellation.
    """
    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT current_setting('statement_timeout') AS previous, "
                "setting::integer AS milliseconds FROM pg_settings "
                "WHERE name = 'statement_timeout'"
            )
            previous = cur.fetchone()
            old_ms = int(previous["milliseconds"])
            timeout = min(old_ms, _EXACT_LEXICAL_TIMEOUT_MS) if old_ms else _EXACT_LEXICAL_TIMEOUT_MS
            cur.execute("SELECT set_config('statement_timeout', %s, true)", (str(timeout),))
            yield cur, timeout
            cur.execute(
                "SELECT set_config('statement_timeout', %s, true)", (previous["previous"],)
            )


class _IndexScanCap:
    """Replace potentially short index reads with exact scoped scalar reads.

    Visible/dead population estimates are not exhaustion evidence. A single
    statement guards its own materialized scoped input before scoring it.
    Exceeding the work limit or timeout refuses the whole read.

    Only under `completeness="exact"` (`exact=True`). Under the bounded
    default every read stands as the index returned it and the envelope says
    so; no exact statement runs (policy option A).
    """

    def __init__(self, conn: psycopg.Connection, *, segment: str, index_name: str,
                 withhold_counts: bool = False, exact: bool = True) -> None:
        self.conn = conn
        self.segment = segment
        self.index_name = index_name
        self.literal = index_name.replace("'", "''")
        self.withhold_counts = withhold_counts
        self.exact = exact
        self.mode = COMPLETENESS_EXACT if exact else COMPLETENESS_BOUNDED
        self.reads: dict[str, _ExactLexicalCount] = {}
        #: Reads that returned their full candidate limit, so more matches may
        #: lie past it. Recorded on the RAW read, before Nomos, the
        #: effective-attribute overlay, `attr_filters` or the path re-check
        #: drop rows: a short filtered list is no proof the read was whole.
        self.saturated: set[str] = set()
        #: Reads whose raw read returned its full limit, rows the index
        #: repeated included (issue #3335): repeated rows are dropped before
        #: anything counts them, but they still filled the LIMIT.
        self.filled: set[str] = set()
        #: Reads served by the native BM25 candidate query (every read under
        #: bounded; a filled read under exact).
        self.native: set[str] = set()

    def note(self, name: str, count: Optional[_ExactLexicalCount]) -> None:
        if count is not None:
            self.reads[name] = count

    def record(
        self, name: str, rows: Sequence[Any], *, limit: int,
        count: Optional[_ExactLexicalCount], native: bool = True,
        read_rows: Optional[int] = None,
    ) -> None:
        """Note one read's exact count and whether it filled its limit.

        An exact read that selected `limit` rows is saturated only when more
        than `limit` rows matched; a native read that filled `limit` always
        is, since nothing counted what lay past it. A read of exactly `limit`
        rows therefore counts as filled even when nothing lay past it: over
        cautious (the page says bounded), never a false exhaustive claim.

        `native=False` records a capped read that is not a BM25 read (the
        effective-attribute read): its saturation counts, its basis does not
        make the page's rows BM25 index candidates.

        `read_rows`: how many rows the read returned before its repeated
        rows were dropped (issue #3335); `rows` are the distinct ones. Fill
        is judged on the rows read, so a LIMIT that repeated rows filled
        still says more matches may lie past it."""
        self.note(name, count)
        if count is None and native:
            self.native.add(name)
        read = len(rows) if read_rows is None else read_rows
        if read >= limit:
            self.filled.add(name)
        if read >= limit and (count is None or count.matches > limit):
            self.saturated.add(name)

    def _refuse(self, reason: str, candidates: Optional[int] = None,
                timeout_ms: Optional[int] = None) -> LexicalIndexScanCapRefused:
        if self.withhold_counts and reason in _EXACT_BOUND_REASONS:
            # Under enforced access rules the scoped input has not passed
            # reader checks, so which bound stopped the read is not said:
            # `scoped_work_limit` alone would reveal more than 200,000 stored
            # scoped rows, and a distinct timeout reason would reveal it by
            # elimination. One generic reason covers both.
            reason = _EXACT_BOUND_WITHHELD
        return LexicalIndexScanCapRefused(
            f"search: an exact scoped lexical read could not finish ({reason}); "
            "refusing rather than answer with fewer rows",
            data={"reason": reason,
                  "scoped_candidates": None if self.withhold_counts else candidates,
                  "work_row_limit": _EXACT_LEXICAL_WORK_ROWS,
                  "timeout_ms": timeout_ms, "index": self.index_name},
            fix_hint="narrow the query scope with subtype, source, or record selections",
        )

    def checked(
        self, name: str, rows: list[dict[str, Any]], *, where: Sequence[str],
        limit: int, params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        complete, count = self.read_complete(rows, where=where, limit=limit, params=params)
        self.record(name, complete, limit=limit, count=count)
        return complete

    def read_complete(
        self, rows: list[dict[str, Any]], *, where: Sequence[str], limit: int,
        params: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], Optional[_ExactLexicalCount]]:
        """Keep a filled bounded selection; exhaust a short scoped input or refuse.

        Bounded mode keeps every read as read: the caller is told the page
        may be incomplete instead of paying for a scalar read it did not ask
        for."""
        if not self.exact or len(rows) >= limit:
            return rows, None
        predicates = ["n.deleted = FALSE", "n.attrs ? 'text'", *where]
        columns = ", ".join(f"n.{column}" for column in _PASSAGE_COLUMNS[:-1])
        # Scored twice on an old library, keeping the better (issue #3666).
        score = steady_bm25_score_sql(
            f"(n.attrs->>'text') <@> to_bm25query(%(q)s, '{self.literal}')"
        )
        # Membership, not the score's sign (issue #3232): `<@>` can score a
        # real match 0 when it reads a term's document frequency as 0, so the
        # score only ranks. `matching` keeps every scoped row the test admits,
        # and `exact_matches` counts that same set, so the count and the
        # ranked prefix cover each match, scored 0 or not. A zero-score match
        # sorts after the scored ones; a non-match never enters the set.
        matched = bm25_match_sql(score="score", text="(attrs->>'text')")
        sql = (
            f"{INDEX_SCAN_CAP_MARK} WITH candidates AS MATERIALIZED ("
            f"SELECT n.* FROM {self.segment} n WHERE {' AND '.join(predicates)} "
            f"LIMIT {_EXACT_LEXICAL_WORK_ROWS + 1}), "
            "input_size AS MATERIALIZED (SELECT count(*) AS candidates FROM candidates), "
            f"scored AS MATERIALIZED (SELECT {columns}, {score} AS score "
            "FROM candidates n WHERE (SELECT candidates FROM input_size) "
            f"<= {_EXACT_LEXICAL_WORK_ROWS}), "
            f"matching AS MATERIALIZED (SELECT * FROM scored WHERE {matched}), "
            "match_size AS (SELECT count(*) AS matches FROM matching) "
            "SELECT input_size.candidates AS exact_candidates, match_size.matches AS exact_matches, "
            "ranked.* FROM input_size CROSS JOIN match_size LEFT JOIN LATERAL "
            f"(SELECT * FROM matching ORDER BY score, node_id LIMIT {int(limit)}) ranked ON TRUE "
            "ORDER BY ranked.score, ranked.node_id"
        )
        effective_timeout = None
        try:
            with _exact_lexical_plan(self.conn) as (cur, effective_timeout):
                if not _scalar_lexical_plan(cur, sql, params, self.index_name):
                    raise self._refuse("plan_uses_bm25_index", timeout_ms=effective_timeout)
                execute_with_custom_plan(cur, sql, params)
                result = cur.fetchall()
        except psycopg.errors.QueryCanceled as exc:
            raise self._refuse("exact_read_timeout", timeout_ms=effective_timeout) from exc
        candidates = int(result[0]["exact_candidates"])
        if candidates > _EXACT_LEXICAL_WORK_ROWS:
            raise self._refuse("scoped_work_limit", candidates, effective_timeout)
        count = _ExactLexicalCount(candidates, int(result[0]["exact_matches"]), effective_timeout)
        complete = [
            {key: row[key] for key in _PASSAGE_COLUMNS}
            for row in result if row["node_id"] is not None
        ]
        return complete, count


def _scalar_lexical_plan(
    cur: psycopg.Cursor, sql: str, params: dict[str, Any], index_name: str,
) -> bool:
    """Reject a plan containing the capped BM25 index, including bitmap use."""
    execute_with_custom_plan(cur, "EXPLAIN (FORMAT JSON) " + sql, params)
    document = next(iter(cur.fetchone().values()))
    if isinstance(document, str):
        document = json.loads(document)
    nodes = list(_plan_nodes(document[0]["Plan"]))
    indexes = {node["Index Name"] for node in nodes if node.get("Index Name")}
    if index_name in indexes:
        return False
    if indexes:
        cur.execute(
            "SELECT c.relname FROM pg_class c JOIN pg_am a ON a.oid = c.relam "
            "WHERE c.relname = ANY(%s) AND a.amname = 'bm25'", (list(indexes),)
        )
        if cur.fetchone() is not None:
            return False
    return True


def _fold_filtered(
    conn: psycopg.Connection,
    session: Session,
    candidates: list[dict[str, Any]],
    *,
    rollup: Any,
    query: str,
    ranked_query: str,
    expansion: ExpandedQuery,
    self_saturated: bool,
    self_effective_saturated: bool,
    self_effective_ids: set[str],
    self_expanded_saturated: bool,
    promoted_id: Optional[str],
    segment: str,
    index_literal: str,
    branch_point_value: Any,
    filter: dict[str, Any],
    scope: Optional[ResolvedScope],
    cap: _IndexScanCap,
    phrases: Sequence[re.Pattern[str]] = (),
    text_redaction: bool = False,
) -> _FilteredFold:
    """D3-i: fold the self read and one capped passage read; decision 5 = 5b.

    The passage read is restricted in SQL to passages whose declared parent
    has the filtered subtype, carries the passage-side selections (source,
    family, path exclusions), and runs with a custom plan
    (`execute_with_custom_plan`, PACT mcp-passage-custom-plan):
    psycopg's auto-prepare leads PostgreSQL to a generic plan for this
    BM25-ordered statement that takes about 26 s from its 11th execution on a
    connection (evidence/probe_prepared_plan.py).

    It walks the BM25 index's ordered scan in windows and scores only the
    rows whose parent passes (`_filtered_passage_read`, PACT
    mcp-filtered-passage-cost), so its cost follows the matches, not the
    passages under the filtered type, and it returns the rows the previous
    full statement returned. If the forced plan is not the index's ordered
    scan, the index moved under every windowed attempt, or the read needs
    more of the index's scan than its result cap guarantees, it runs that
    full statement, logs a warning and discloses the reason
    (`passage_read_fallback`).

    The passages, their relation edges and the parents fetched for them are
    decided once per Nomos decision class (`count` decisions). A passage that
    does not fold is dropped. Saturation is counted on raw rows; each
    saturated read's cutoff is its worst VISIBLE score, minus infinity when
    none is visible.
    """
    edge_segment = resolve_edge_segment(conn)
    params: dict[str, Any] = {
        "bp": branch_point_value,
        "child": rollup.child_subtype,
        "edge": rollup.edge_type,
        "parent": rollup.parent_subtype,
    }
    where = [
        "n.deleted = FALSE",
        "n.live_lo <= %(bp)s",
        "%(bp)s < n.live_hi",
        "n.attrs ? 'text'",
        "n.subtype = %(child)s",
    ]
    for clause in (
        _family_sql(scope, params, column="n.source"),
        _source_sql(filter, params, column="n.source"),
    ):
        if clause is not None:
            where.append(clause)
    where.extend(_path_exclusion_sql(scope, params, column="n.attrs"))
    score = f"(n.attrs->>'text') <@> to_bm25query(%(q)s, '{index_literal}')"
    parent_test = (
        f"EXISTS (SELECT 1 FROM {edge_segment} e "
        f"JOIN {segment} p ON p.node_id = e.src "
        f"WHERE e.dst = n.node_id AND e.edge_type = %(edge)s "
        f"AND e.deleted = FALSE AND e.live_lo <= %(bp)s AND %(bp)s < e.live_hi "
        f"AND p.subtype = %(parent)s AND p.deleted = FALSE "
        f"AND p.live_lo <= %(bp)s AND %(bp)s < p.live_hi)"
    )
    passage_sql = (
        f"SELECT * FROM ("
        f"SELECT n.node_id, n.subtype, n.attrs, n.text_hash, n.source, "
        f"n.source_record_id, n.source_revision, {score} AS score "
        f"FROM {segment} n WHERE {' AND '.join(where)} AND {parent_test} "
        f"ORDER BY {score}, n.node_id LIMIT {_LEXICAL_RANK_CAP}"
        f") AS ranked WHERE {RANKED_BM25_MATCH}"
    )

    _index_segment, index_name = resolve_node_text_bm25_index(conn)
    index_sql, window_sql, score_sql = _passage_windows_sql(
        segment=segment,
        edge_segment=edge_segment,
        index_literal=index_literal,
        # The index's own predicate is the scan's WHERE; the rest is a test.
        keep=[
            clause for clause in where
            if clause not in ("n.deleted = FALSE", "n.attrs ? 'text'")
        ],
    )

    def read(q: str, name: str) -> _PassageRead:
        read_params = {**params, "q": q}

        def run() -> _PassageRead:
            reason = _FALLBACK_PLAN
            for _attempt in range(_PASSAGE_READ_ATTEMPTS):
                try:
                    with _index_plan(conn) as cur:
                        if not _passage_index_ordered(
                            cur, index_sql, read_params, index_name,
                        ):
                            break
                        windowed = _filtered_passage_read(
                            cur, window_sql=window_sql, score_sql=score_sql,
                            params=read_params, scan_room=_scan_room(cur, segment),
                        )
                    if len(windowed.rows) >= _LEXICAL_RANK_CAP:
                        return windowed
                    # A short windowed read trusts table statistics (the scan
                    # room) to say the scan ran out; entries they miss count
                    # against the cap too, so it is read completely (PACT
                    # lexical-index-scan-cap-3162).
                    rows, matches = cap.read_complete(
                        windowed.rows, where=[*where, parent_test],
                        limit=_LEXICAL_RANK_CAP, params=read_params,
                    )
                    windowed.rows = rows
                    windowed.index_scan_matches = matches
                    return windowed
                except _StatisticsChanged:
                    reason = _FALLBACK_STATISTICS
                except _ScanCapReached:
                    reason = _FALLBACK_SCAN_CAP
                    break
            _log.warning(
                "filtered passage read: %s on %s; running the full passage "
                "statement",
                reason, index_name,
            )
            with conn.cursor(row_factory=dict_row) as cur:
                execute_with_custom_plan(cur, passage_sql, read_params)
                rows = cur.fetchall()
            # The full statement is cut by the index's result cap like any
            # other BM25 read with a Filter (PACT lexical-index-scan-cap-3162).
            rows, matches = cap.read_complete(
                rows, where=[*where, parent_test], limit=_LEXICAL_RANK_CAP,
                params=read_params,
            )
            return _PassageRead(
                rows=rows, scan="full", fallback=reason, index_scan_matches=matches,
            )

        result = cached_read(
            # The mode is in the key: an exact read differs from a bounded one.
            _read_key(
                conn, session, f"filtered_passage_read:{cap.mode}", window_sql,
                read_params,
            ),
            run,
        )
        cap.record(
            name, result.rows, limit=_LEXICAL_RANK_CAP,
            count=result.index_scan_matches,
        )
        return result

    literal_read = read(query, "passages")
    literal = literal_read.rows
    literal_saturated = len(literal) >= _LEXICAL_RANK_CAP
    for row in literal:
        row["expansion_rank"] = 0
    raw = list(literal)
    expansion_saturated = False
    fallback = literal_read.fallback
    if expansion.applied:
        literal_ids = {str(row["node_id"]) for row in literal}
        # Its own name: the literal read's exact count must survive it.
        added_read = read(ranked_query, "passages_expanded")
        added = added_read.rows
        fallback = fallback or added_read.fallback
        expansion_saturated = len(added) >= _LEXICAL_RANK_CAP
        for row in added:
            if str(row["node_id"]) not in literal_ids:
                row["expansion_rank"] = 1
                raw.append(row)
    if text_redaction:
        # A passage counts only when the reader reads its text as stored
        # (#3377), so a passage whose text is masked or dropped never folds
        # into, or lifts, its record. Decided on the redacted rows, recorded
        # as counts: the page serves none of these rows itself.
        visible = {
            str(row["node_id"]) for row in keep_readable_text(
                raw, filter_node_rows_for_session(conn, session, raw, count=True),
                redaction=True,
            )
        }
    else:
        visible = visible_ids_by_decision_class(conn, session, raw, row_kind="node")
    passages = [row for row in raw if str(row["node_id"]) in visible]

    self_ids = {str(row["node_id"]) for row in candidates}
    links, fetched = resolve_fold_links(
        conn,
        session,
        passages=passages,
        known_parents={str(row["node_id"]): row for row in candidates},
        node_segment=segment,
        edge_segment=edge_segment,
        branch_point_value=branch_point_value,
        check=CHECK_CLASS,
        mask_fetched_parents=_selects_on_attrs(filter, scope),
    )
    # Rule 3: only a parent of the filtered subtype takes a passage here.
    links = {
        pid: link for pid, link in links.items()
        if link.parent_subtype == rollup.parent_subtype
    }
    rows = fold_members(
        records=candidates,
        passages=passages,
        links=links,
        fetched_parents=fetched,
        parent_admitted=lambda parent: _matches_filter(parent, filter, scope=scope),
        keep_unfolded=False,
        head_id=promoted_id,
    )
    cut_self = self_cutoffs(
        candidates,
        literal_saturated=self_saturated,
        effective_saturated=self_effective_saturated,
        effective_ids=self_effective_ids,
    )
    cut_passages = cutoff(
        literal_saturated,
        (float(row["score"]) for row in passages if row["expansion_rank"] == 0),
    )
    # Under exact, an expanded read (self, effective-attribute or passage)
    # that filled its limit may have missed expansion-only matches. They rank
    # after every exact-wording row, so the exact prefix keeps every
    # exact-wording row and ends at the first expanded one (an infinite
    # cutoff), and the count is a floor: the paging fields respect the same
    # saturation as the count (review MEDIUM-2). The bounded default keeps
    # its paging over the retrieved candidates unchanged.
    #
    # Under enforced Nomos (`cap.withhold_counts`) these two exact-mode
    # additions are not made: they read raw saturation measured before Nomos
    # hides rows, and the fold's paging fields are public. The fold then
    # keeps dev's own rules, so it says nothing dev did not; the exact
    # retrieval status says `unknown` instead (review 3, item 1).
    strict = cap.exact and not cap.withhold_counts
    cut_expanded = (
        math.inf
        if strict and (self_expanded_saturated or expansion_saturated)
        else None
    )
    literal_floor = self_saturated or self_effective_saturated or literal_saturated
    lower_bound = (
        literal_floor or expansion_saturated
        or (strict and self_expanded_saturated)
    )
    ordered_exactly_through = exact_prefix_length(
        rows, (*cut_self, cut_passages, cut_expanded),
    )
    if phrases:
        rows = _promote_phrase_prefix(
            conn, session, rows,
            ordered_exactly_through=ordered_exactly_through,
            self_ids=self_ids, head_id=promoted_id, phrases=phrases,
        )
    return _FilteredFold(
        rows=rows,
        ordered_exactly_through=ordered_exactly_through,
        lower_bound=lower_bound,
        self_ids=self_ids,
        passages_read=len(raw),
        passages_saturated=literal_saturated or expansion_saturated,
        passage_read_fallback=fallback,
        # Only when an expanded read alone made the floor. When a typed-word
        # read filled, rerunning without expansion fills it again, so the
        # remedy would not reach the later records: the page names narrowing
        # instead (review 4, LOW). The same rule under enforced Nomos: it
        # tells a caller only whether the typed-word floor stands, which the
        # same search with `expand_concepts=false` already shows on dev.
        expansion_remedy=(
            expansion_remedy_args(expansion)
            if expansion.applied and lower_bound and not literal_floor
            else None
        ),
    )


def _read_key(
    conn: psycopg.Connection, session: Session, kind: str, sql: str,
    params: dict[str, Any],
) -> tuple[Any, ...]:
    """Everything a stored read's rows depend on: the database, the pinned
    session, the statement and its parameters (which carry the branch
    point). Never `limit` or `offset`: no ranked read takes them."""
    info = conn.info
    return (
        kind, info.host, info.port, info.dbname,
        session.deployment_name, str(session.generation_id),
        sql, freeze(params),
    )


def _ranked_read(
    conn: psycopg.Connection, session: Session, sql: str, params: dict[str, Any],
    *, cap: _IndexScanCap, name: str, where: Sequence[str],
) -> list[dict[str, Any]]:
    """One BM25-ordered read with a custom plan, run once per tool call.

    Under the MCP byte-budget wrapper a call reruns the operator at smaller
    limits; this read's rows do not depend on the limit, so the reruns reuse
    them (`call_read_cache`). Outside the wrapper it always runs. A short
    read is read again completely (`_IndexScanCap`); the cached value
    carries that, so a rerun discloses it.

    The index can return one row more than once (issue #3335): repeated
    rows are dropped before the rows are counted or checked for a short
    read, and the fill of the LIMIT is recorded on the rows read
    (`_IndexScanCap.filled`). Under `completeness="exact"` a LIMIT that
    repeats filled is short once they are dropped, so the exact read runs;
    if it refuses, the refusal says the index read held repeated rows
    (`data.index_repeated_rows`), except under withheld counts.
    """
    def read() -> tuple[list[dict[str, Any]], Optional[_ExactLexicalCount], int]:
        with conn.cursor(row_factory=dict_row) as cur:
            execute_with_custom_plan(cur, sql, params)
            rows = cur.fetchall()
        distinct = drop_repeated_rows(rows)
        try:
            complete, matches = cap.read_complete(
                distinct, where=where, limit=_LEXICAL_RANK_CAP, params=params,
            )
        except LexicalIndexScanCapRefused as exc:
            if len(rows) >= _LEXICAL_RANK_CAP > len(distinct) and not cap.withhold_counts:
                # The read filled its LIMIT and only the dropped repeats made
                # it short, so the exact read ran; say so in the refusal.
                exc.data["index_repeated_rows"] = True
                exc.message = (
                    f"{exc.message} (the index read filled its limit with "
                    "repeated rows, which were dropped)"
                )
                exc.args = (exc.message,)
            raise
        # An exact read that replaced the index's rows is the read.
        return complete, matches, len(rows) if matches is None else len(complete)

    # The mode is in the key: an exact read differs from a bounded one.
    rows, matches, read_rows = cached_read(
        _read_key(conn, session, f"ranked_read:{cap.mode}", sql, params), read,
    )
    cap.record(name, rows, limit=_LEXICAL_RANK_CAP, count=matches, read_rows=read_rows)
    return rows


def _page_exact_prefix(
    rows: list[dict[str, Any]],
    *,
    offset: int,
    limit: int,
    ordered_exactly_through: int,
    lower_bound: bool,
    subtype: str,
    expansion_remedy: Optional[dict[str, Any]] = None,
    floor_truncates: bool = False,
) -> tuple[list[dict[str, Any]], bool, Optional[int], Optional[str]]:
    """`(page, truncated, next_offset, next_offset_reason)` of a filtered fold.

    A page never extends past the exact prefix. `offset` at or past it is
    refused only when the count is a lower bound and `offset > 0`; `offset=0`
    always answers (an empty or short page with the reason). With no
    saturated read the whole list is exact and such an offset returns an
    empty page, as today.

    `expansion_remedy`: the floor comes from a filled read of the expanded
    query, so the refusal names this remedy (`expand_concepts=false` and the
    words to rerun one per call) in `data.remedy` and its hint; narrowing
    alone may not help there. `floor_truncates` (exact
    mode): a page that reaches the end of the exact prefix under a
    lower-bound count is truncated even when no retrieved row lies past it
    (every expanded row was filtered out, say): the filled read may hold
    matches past its limit, so `truncated` follows the same saturation as
    the count (review 3, item 3).
    """
    total = len(rows)
    if lower_bound and offset > 0 and offset >= ordered_exactly_through:
        data: dict[str, Any] = {
            "offset": offset,
            "ordered_exactly_through": ordered_exactly_through,
            "subtype": subtype,
        }
        if expansion_remedy:
            data["remedy"] = dict(expansion_remedy)
        raise RecordsOffsetBeyondExactOrder(
            f"offset {offset} is at or past the {ordered_exactly_through} "
            f"{subtype!r} record(s) whose order is exact; records past them "
            f"have no exact rank, so a later page cannot be ordered",
            data=data,
            fix_hint=(
                _EXPANSION_REMEDY_HINT if expansion_remedy else (
                    "page within the exactly ordered records, or narrow the "
                    "query so fewer records match"
                )
            ),
        )
    end = min(offset + limit, ordered_exactly_through)
    page = rows[offset:end] if offset < end else []
    truncated = (
        offset + limit < total or end < total
        or (floor_truncates and lower_bound and end >= ordered_exactly_through)
    )
    if offset + limit < ordered_exactly_through:
        return page, truncated, offset + limit, None
    if truncated or lower_bound:
        return page, truncated, None, NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER
    return page, False, None, None


def _count_basis(
    *,
    rank_by: str,
    rank_capped: bool,
    rollup_chunk_cap_reached: Optional[bool],
    fold: Optional[_FilteredFold],
) -> dict[str, Any]:
    """`total_count_basis` and, when capped, `ranked_cap` (DESIGN.md D2)."""
    if rank_by == RANK_BY_PASSAGES:
        lower = bool(rollup_chunk_cap_reached)
        cap: dict[str, int] = {"passages": PASSAGE_ROLLUP_CHUNK_CAP}
    elif fold is not None:
        lower = fold.lower_bound
        cap = {"rows": _LEXICAL_RANK_CAP, "passages": _LEXICAL_RANK_CAP}
    else:
        lower = bool(rank_capped)
        cap = {"rows": _LEXICAL_RANK_CAP}
    out: dict[str, Any] = {
        "total_count_basis": "lower_bound" if lower else "exact",
    }
    if lower:
        out["ranked_cap"] = cap
    return out


def _lexical_retrieval(
    cap: _IndexScanCap, *, rank_capped: bool, work_capped: bool, count_exact: bool,
) -> dict[str, Any]:
    """The lexical page's `coverage.retrieval` (PACT operator-page-counts-3190
    shape; PACT lexical-index-scan-cap-3162 policy option A).

    Bounded: the native BM25 candidates, which the index's 100,000-entry
    result cap may have cut before scope filters ran, with the recovery.
    Exact: `exhaustive` only when no contributing read returned its full
    limit (`cap.saturated`, measured on raw reads) and the count is exact;
    otherwise bounded by the candidate cap, never inferred from a short page.

    The bounded page names no `index_scan_cap` bound: basis `bm25_index`
    under status `bounded` already says the index scan may have stopped.
    `bounds` appears only for the candidate and work caps.

    Under enforced Nomos (`cap.withhold_counts`) saturation is measured on
    raw reads before Nomos hides rows, so `candidate_cap` would say that 200
    or more rows the caller may not read match. It is never named there:
    bounded keeps only `work_cap` from the rollup's own public flag, and
    exact reports `exhaustive` when no read filled, otherwise `unknown`
    with no reason. What remains is one bit (exhaustive or unknown).
    """
    hidden = cap.withhold_counts
    candidate_capped = bool(rank_capped or (cap.saturated - {"chunks"}))
    chunk_capped = bool(work_capped or "chunks" in cap.saturated)
    bounds: list[str] = []
    if candidate_capped and not hidden:
        bounds.append("candidate_cap")
    if work_capped or (chunk_capped and not hidden):
        bounds.append("work_cap")
    if not cap.exact:
        out: dict[str, Any] = {"status": "bounded", "basis": "bm25_index"}
        if bounds:
            out["bounds"] = bounds
        out["recovery"] = dict(BOUNDED_RECOVERY)
        return out
    if not candidate_capped and not chunk_capped and count_exact:
        return {"status": "exhaustive", "basis": "exact_scalar"}
    if hidden:
        return {"status": "unknown"}
    return {
        # A filled read kept its native fast path, so its rows are BM25
        # index candidates; only a read the exact form served is scalar.
        "status": "bounded",
        "basis": "bm25_index" if cap.native else "exact_scalar",
        "bounds": bounds or ["candidate_cap"],
    }


def _search_lexical(
    conn: psycopg.Connection,
    session: Session,
    query: str,
    *,
    filter: dict[str, Any],
    limit: int,
    offset: int,
    select: Optional[list[str]] = None,
    scope: Optional[ResolvedScope] = None,
    rank_by: str = RANK_BY_SELF,
    effective_view_source: Any = None,
    max_chars: int = DEFAULT_PREVIEW_CHARS,
    expand_concepts: bool = True,
    completeness: str = COMPLETENESS_BOUNDED,
    record_ids: Optional[list[str]] = None,
    children_of: Optional[search_where.ChildrenScope] = None,
    include_excerpts: bool = True,
    list_view: bool = False,
    reference_members: bool = False,
    passage_chars: Optional[int] = None,
) -> dict[str, Any]:
    """Generation-pinned BM25 lexical search.

    Ranks via the pg_textsearch BM25 index over the node segment, filtered to the
    session's pinned-visible rows by the chronos branch point (live_lo <= bp AND bp <
    live_hi). So the M4.5 pin guarantee (result set is a pure function of the pin)
    holds for ANY pin -- no latest-detection / fallback scan needed -- replacing the
    former full-table TransformProgram + client-side scorer (~10s on okg-workspace).
    Resolves the segment + BM25 index from pg_indexes and queries them directly (the
    chronos checkout's sqlglot rewriter cannot parse the <@> operator). The subtype
    filter is pushed into SQL; the nomos read filter, the effective-attrs overlay,
    attr_filters, and pagination run in Python over the top-ranked candidates -- so
    total_count is exact + post-nomos and attr_filters match EFFECTIVE (overlaid)
    attrs. Raises (SearchIndexUnavailable) if the index or branch point is absent --
    no silent fall back to a scan."""
    validate_select(select, _LEXICAL_FIELDS, tool_name="search(method=lexical)",
                    elsewhere=search_select_elsewhere("lexical"))
    # Quoted phrases (PACT lexical-quoted-phrases-3064): grep's phrase rule
    # picks the spans; BM25 ranks the query with its quotes removed. With no
    # span `bm25_query` IS `query`, so an unquoted search is served exactly
    # as before. The raw query stays the alias lookup's key and the echo.
    spans = quoted_spans(query)
    phrases = tuple(phrase_pattern(span.split()) for span in spans)
    bm25_query = strip_quotes(query) if spans else query
    expansion = _resolve_expansion(
        bm25_query, scope=scope, rank_by=rank_by, expand_concepts=expand_concepts,
    )
    # The ONE place the ranked string is chosen: identical to `query` unless a
    # declared group matched.
    ranked_query = expansion.query
    pin = session.generation_id
    segment, index_name = resolve_node_text_bm25_index(conn)
    index_literal = index_name.replace("'", "''")
    exact = completeness == COMPLETENESS_EXACT
    cap = _IndexScanCap(conn, segment=segment, index_name=index_name,
                        withhold_counts=passage_counts_withheld(session),
                        exact=exact)
    bp = branch_point(conn, session)
    subtype = filter.get("subtype")
    # Under `records`: which rows fold, and how (DESIGN.md D1, D3).
    result_unit, fold_shape, filtered_rollup = (
        _records_shape(conn, session, subtype)
        if rank_by == RANK_BY_RECORDS
        else (None, None, None)
    )
    if list_view:
        # A list delivers records: a passage-type subtype filter would serve
        # passages under the records contract, so refuse before ranking.
        refuse_non_record_unit(result_unit=result_unit, subtype=subtype)
    if reference_members and result_unit == "passage":
        # A result reference holds record units only (#3222, D1). The unit
        # is known only from the pinned catalog, so this refuses after that
        # read and before any ranking.
        import okg.substrate.mcp.result_references as result_references

        raise result_references.unsupported(
            "argument", argument="subtype_filter",
            message=f"search: subtype {subtype!r} is a passage unit; a result "
            "reference holds records",
            fix_hint="filter on the record subtype, or drop retain",
        )
    # The subtype the ranked SQL filters on. Under a record scope the named
    # records and their passages ARE the candidate set, so a record subtype
    # that would take the filtered two-read fold folds the scoped read
    # instead, and the subtype becomes a clause on the record (D6).
    sql_subtype = subtype
    scope_rows: Optional[Union[_RecordScope, _ChildrenScope]] = None
    if record_ids is not None or children_of is not None:
        scope_rows = (
            _resolve_record_scope(
                conn, session, record_ids, segment=segment, branch_point_value=bp,
            )
            if children_of is None
            else _resolve_children_scope(
                conn, session, children_of,
                segment=segment, branch_point_value=bp,
            )
        )
        if fold_shape == _FOLD_FILTERED:
            fold_shape, filtered_rollup, sql_subtype = _FOLD_UNFILTERED, None, None
            filter = {
                **filter,
                "where": search_where.and_clauses(
                    {"op": "eq", "field": "type", "value": subtype},
                    filter.get("where"),
                ),
            }

    where = [
        "deleted = FALSE",
        "live_lo <= %(bp)s",
        "%(bp)s < live_hi",
        "attrs ? 'text'",
    ]
    params: dict[str, Any] = {"q": bm25_query, "bp": bp}
    if sql_subtype is not None:
        where.append("subtype = %(subtype)s")
        params["subtype"] = sql_subtype
    # `where` clauses on columns Nomos never changes (id, type, source) join
    # the RANKED query, so the rank cap is spent on rows they admit (D4). Only
    # where each ranked row is judged by itself: in a record view a passage
    # is judged through its record, whose type and id are not the passage's.
    # Clauses on attributes stay out (any attribute may be masked until the
    # reader's view lands) and are checked on read rows below.
    # Issue #3666: whether a row filter is decided only in Python on the
    # ranked candidates: `attr_filters` (an `attrs.<key> = v` lifted out of
    # `where`, or the legacy argument) always is (`_matches_filter`), and so
    # is any `where` part the ranked SQL does not decide whole (attributes,
    # dates, or any clause judged through a record fold). Such a filter is
    # checked on the capped candidates only.
    where_after_ranking = filter.get("where") is not None
    if rank_by == RANK_BY_SELF or (
        rank_by == RANK_BY_RECORDS and fold_shape != _FOLD_UNFILTERED
    ):
        # Compile once through the shared reader-domain boundary. Unsupported
        # attrs/dates remain a TRUE superset and the existing full read check
        # still runs below; OR/NOT cannot spend the cap on a lossy predicate.
        compiled_where = search_where.compile_where(
            filter.get("where"), params=params,
            columns={"id": "node_id", "type": "subtype", "source": "source"},
        )
        where.append(compiled_where.bind_into(params))
        where_after_ranking = (
            where_after_ranking and compiled_where.qualification != "exact"
        )
    row_filter_after_ranking = where_after_ranking or bool(filter.get("attr_filters"))
    if scope_rows is not None:
        where.append(scope_rows.sql(params, column="node_id"))
    # Site 1 of 3. The scope goes into the RANKED query, so the rank cap below
    # is spent entirely on in-scope rows (issue #797). `subtype` is a stored
    # column, never overlaid, so this is a safe SQL-side predicate.
    scope_sql = _scope_sql(scope, params, column="subtype")
    if scope_sql is not None:
        where.append(scope_sql)
    # The authority selection joins the scope in the RANKED query so the rank
    # cap is spent on rows the caller actually asked for. `source` is a stored
    # column, never overlaid, so the family predicate is exact in SQL;
    # `attrs->>'path'` IS overlayable, so the exclusion is ALSO re-checked over
    # effective attrs by `_matches_filter` below.
    family_sql = _family_sql(scope, params, column="source")
    if family_sql is not None:
        where.append(family_sql)
    # The caller's producer filter joins the RANKED query for the same reason
    # the family predicate does: the rank cap is a SQL LIMIT, so a producer
    # applied afterwards would page over a ranking the excluded rows had
    # already filled. `source` is a stored column, so this is exact in SQL and
    # the worst case is today's subtype filter -- one ordered index scan that
    # stops at the score-zero boundary, never a table scan.
    source_sql = _source_sql(filter, params, column="source")
    if source_sql is not None:
        where.append(source_sql)
    exclusion_sql = _path_exclusion_sql(scope, params, column="attrs")
    where.extend(exclusion_sql)
    order_sql = f"(attrs->>'text') <@> to_bm25query(%(q)s, '{index_literal}')"
    # Matches only: BM25 ORDER BY <@> LIMIT pads with 0-score NON-matches when
    # the (filtered) corpus has fewer matches than the limit, so a match test
    # has to run somewhere (scores are negative for a match, 0 for none).
    # `subtype` is a stored column (never overlaid) so it filters in SQL;
    # `attr_filters` are applied AFTER the effective-attrs overlay below, so
    # they match effective values (the pre-BM25 path's semantics).
    #
    # The match test runs OUTSIDE the ranked scan, not inside its WHERE (issue
    # #1924). As a WHERE clause the same `<@>` expression is a restriction the
    # planner costs on its own, and on a real deployment it stopped choosing the
    # BM25 index at all: measured on okg-workspace 2026-09-22 against the
    # 695,052-record `default` branch, `EXPLAIN (ANALYZE, BUFFERS)` showed an
    # Index Scan on the `live_hi` btree with the BM25 score computed as a
    # per-row Filter -- 697,178 rows removed, 10.2 s -- and the same 10.2 s for
    # a query with no match at all, because the cost was the scan rather than
    # the answer. With the test moved to an outer query over the capped ranked
    # page, the planner uses the BM25 index for the ordering it exists to
    # serve: 18 ms for `CMSSW`, 62 ms for `deployment`, 27 ms for `HLT`, 21 ms
    # for a non-word.
    #
    # The two forms select the SAME rows in the SAME order, not approximately.
    # Scores sort ascending, every match is negative and every pad is 0, so the
    # capped page is filled with matches before any pad can enter it, and the
    # outer test can only drop pads the inner ORDER BY had already put last.
    # Verified by comparing ordered node-id lists for those four queries
    # against the live deployment: all four identical, with `deployment` at the
    # saturated 200-row cap -- the case where a mis-ordered cap would show.
    where_sql = " AND ".join(where)
    # One ranked read, shared by the literal pass and the expansion pass so the
    # two cannot drift into different plans. The inner query is the capped,
    # BM25-ordered page; the outer query keeps only the real matches, and it
    # tests the score the inner query already computed rather than recomputing
    # `<@>` per returned row.
    ranked_sql = (
        f"SELECT * FROM ("
        f"SELECT node_id, subtype, attrs, text_hash, source, "
        f"source_record_id, source_revision, "
        f"{order_sql} AS score "
        f"FROM {segment} WHERE {where_sql} "
        f"ORDER BY {order_sql}, node_id LIMIT {_LEXICAL_RANK_CAP}"
        f") AS ranked WHERE {RANKED_BM25_MATCH}"
    )
    if scope_rows is not None:
        # A record scope reads its rows first (a materialized set the planner
        # cannot merge into a BM25 index scan) and scores each one. The index
        # scan would walk the whole index in score order and filter, and
        # pg_textsearch stops a filtered scan at its result limit, so a small
        # scope could silently lose matches; scoring the scoped rows is exact.
        #
        # The match test runs BEFORE the rank cap here (issue #3249). A real
        # match can score 0 (#3232) and then sorts by node id among the
        # scope's 0-score non-matches, so a cap taken first could leave it
        # past position 200 while the page, counting rows after the test,
        # saw fewer than 200 and said nothing. With the test first, the read
        # returns the first `_LEXICAL_RANK_CAP` real matches and fills the
        # cap only when that many matched. `OFFSET 0` keeps the scored rows a
        # subquery, so `<@>` runs once per row; inlined, PostgreSQL computed
        # it twice. No BM25 index scan is involved, so the unscoped read's
        # reason to test outside the scan (#1924) does not apply. On an old
        # library the score is read twice on purpose, keeping the better
        # (`steady_bm25_score_sql`, issue #3666).
        scope_score = steady_bm25_score_sql(
            f"(s.attrs->>'text') <@> to_bm25query(%(q)s, '{index_literal}')"
        )
        ranked_sql = (
            f"WITH scoped AS MATERIALIZED ("
            f"SELECT node_id, subtype, attrs, text_hash, source, "
            f"source_record_id, source_revision "
            f"FROM {segment} WHERE {where_sql}) "
            f"SELECT * FROM ("
            f"SELECT s.*, {scope_score} AS score FROM scoped s "
            f"OFFSET 0"
            f") AS ranked WHERE {RANKED_BM25_MATCH} "
            f"ORDER BY ranked.score, ranked.node_id LIMIT {_LEXICAL_RANK_CAP}"
        )

    rollup = None
    rollup_chunks_scanned: Optional[int] = None
    rollup_chunk_cap_reached: Optional[bool] = None
    rollup_chunk_counts: dict[str, int] = {}
    rollup_best_chunk_ids: dict[str, Any] = {}
    rollup_best_chunks: dict[str, dict[str, Any]] = {}
    # Whether the active policy can redact (#3377), read once per call: every
    # site below that admits a ranked unit keeps it only when its text reads
    # as stored.
    text_redaction = redaction_possible(conn, session)
    if rank_by == RANK_BY_PASSAGES:
        if subtype is None:
            raise PassageRollupSubtypeRequired(
                "search: rank_by='passages' requires a subtype filter naming "
                "the parent subtype to roll up",
                data={"rank_by": rank_by, "filter": dict(filter)},
                fix_hint=(
                    "pass subtype_filter=<parent subtype>, or drop "
                    "rank_by='passages'"
                ),
            )
        rollup = passage_rollup_declaration(
            conn,
            catalog_version_id=session.catalog_version_id,
            subtype=subtype,
        )
        candidates, rollup_chunks_scanned, rollup_chunk_cap_reached = (
            passage_rollup_candidates(
                conn,
                session=session,
                rollup=rollup,
                node_segment=segment,
                edge_segment=resolve_edge_segment(conn),
                order_sql=order_sql,
                params=params,
                # The family filter and the profile's path exclusions apply to
                # the CHUNKS the rollup ranks, not to the parents it returns:
                # the chunk is the ranked unit, so an out-of-selection chunk
                # left in the scan would spend the chunk budget and attribute
                # its score to a parent the caller excluded.
                extra_child_where=tuple(
                    clause for clause in (
                        _family_sql(scope, params, column="source"),
                        _source_sql(filter, params, column="source"),
                    ) if clause is not None
                ) + tuple(exclusion_sql),
                # Only under completeness="exact": the rollup then keeps its
                # single policy_scan_cap + 1 read (PACT
                # passage-rollup-scan-cost). Bounded reads stand as read.
                complete_read=cap.read_complete if cap.exact else None,
                # The final chunk read only, on every rerun, cached or not.
                record_read=lambda rows, **pieces: cap.record("chunks", rows, **pieces),
                # One reading of the Nomos mode for this call: off scans
                # chunk_cap + 1 narrow rows, audit/enforce scan in windows.
                # Read through the filter module's own binding, so the
                # filter and this scan cannot disagree on the mode.
                runtime_required=_read_filter.deployment_requires_nomos_runtime(
                    session.deployment_name,
                ),
                # Chunk, edge and parent reads run once per tool call, keyed
                # like `_ranked_read` (completeness mode included).
                read_key=lambda kind, sql, read_params: _read_key(
                    conn, session, f"{kind}:{cap.mode}", sql, read_params,
                ),
                # A chunk counts only when its text reads as stored (#3377).
                text_redaction=text_redaction,
            )
        )
        rollup_chunk_counts = {
            str(row["node_id"]): int(row.get("rollup_chunk_count") or 0)
            for row in candidates
        }
        # Captured HERE, before the nomos filter / effective-attrs overlay
        # rebuild the row dicts, for the same reason `rollup_chunk_counts` is:
        # the winning chunk is decided by the rollup and must survive the
        # pipeline as a lookup, not as a passenger key.
        rollup_best_chunk_ids = {
            str(row["node_id"]): row.get("rollup_best_chunk_id")
            for row in candidates
        }
        # Popped, not copied: the winning passage row must not ride through
        # the parent's read pipeline. Its excerpt is built after fencing.
        rollup_best_chunks = {
            str(row["node_id"]): row.pop("rollup_best_chunk")
            for row in candidates
        }
    else:
        # Rank self rows, then apply authority and effective attrs in Python.
        candidates = _ranked_read(
            conn, session, ranked_sql, params, cap=cap, name="self", where=where,
        )

    # Preserve the literal query's candidates AND their scores before a
    # separately capped expansion read. Sorting one expanded top-k afterwards
    # cannot recover originals it excluded, or restore their original order.
    original_scores = {str(row["node_id"]): row["score"] for row in candidates}
    expansion_scan_capped = len(candidates) >= _LEXICAL_RANK_CAP or "self" in cap.filled
    # The literal self read's saturation, counted on RAW rows before any
    # filter: the filtered fold's exactness rule needs it (D3-i).
    self_literal_saturated = expansion_scan_capped
    self_effective_saturated = False
    self_effective_ids: set[str] = set()
    if expansion.applied:
        added = _ranked_read(
            conn, session, ranked_sql, {**params, "q": ranked_query},
            cap=cap, name="self_expanded", where=where,
        )
        expansion_scan_capped |= (
            len(added) >= _LEXICAL_RANK_CAP or "self_expanded" in cap.filled
        )
        candidates.extend(
            row for row in added if str(row["node_id"]) not in original_scores
        )

    # Whether the ranker itself matched anything, BEFORE the nomos read
    # filter, the effective-attrs overlay and attr_filters run. The
    # zero-hit diagnostic below is only meaningful when this is empty: if
    # BM25 DID match and the page is empty anyway, the reason is the
    # filtering, and answering "the term is genuinely absent" would be
    # false. Per `index-document-source-file-search` (issue #900).
    bm25_matched = (
        bool(rollup_chunks_scanned)
        if rank_by == RANK_BY_PASSAGES
        else bool(candidates)
    )

    # A ranked unit counts only when the reader reads its text as stored
    # (#3377): a hit on masked or dropped text is dropped here, before any
    # fold or page fact, never re-matched against the masked text. Under
    # `passages` the ranked units are the chunks, checked by the rollup.
    redaction = rank_by != RANK_BY_PASSAGES and text_redaction
    candidates = keep_readable_text(
        candidates, filter_node_rows_for_session(conn, session, candidates),
        redaction=redaction,
    )
    if rank_by != RANK_BY_PASSAGES and search_where.masking_possible(
        session.deployment_name,
    ):
        # Under Nomos the zero-hit diagnosis below asks whether the ranker
        # matched a row the reader may read: a match only hidden rows or
        # unreadable text hold must answer as no match at all (#3377).
        bm25_matched = bool(candidates)
    effective_candidates = (
        []
        if rank_by == RANK_BY_PASSAGES
        else _effective_view_lexical_candidates(
            conn,
            session,
            query=bm25_query,
            filter=filter,
            segment=segment,
            branch_point_value=bp,
            exclude_node_ids=(
                set(original_scores) if expansion.applied
                else {str(row["node_id"]) for row in candidates}
            ),
            scope=scope,
            effective_view_source=effective_view_source,
            scope_rows=scope_rows,
        )
    )
    if rank_by != RANK_BY_PASSAGES:
        # The effective-attribute reads carry their own 200-row LIMIT, so a
        # filled one may hold matches past it like any capped read: the
        # tracker sees it, and with it the exhaustive test and the count
        # basis (review MEDIUM-1). Raw rows, before Nomos.
        cap.record("effective", effective_candidates, limit=_LEXICAL_RANK_CAP,
                   count=None, native=False)
    if effective_candidates:
        original_scores.update({
            str(row["node_id"]): row["score"] for row in effective_candidates
        })
        expansion_scan_capped |= len(effective_candidates) >= _LEXICAL_RANK_CAP
        self_effective_saturated = len(effective_candidates) >= _LEXICAL_RANK_CAP
        self_effective_ids = {str(row["node_id"]) for row in effective_candidates}
        # An original effective-attribute match wins over an expanded stored
        # candidate with the same identity.
        effective_ids = {str(row["node_id"]) for row in effective_candidates}
        candidates = [
            row for row in candidates if str(row["node_id"]) not in effective_ids
        ]
        candidates.extend(keep_readable_text(
            effective_candidates,
            filter_node_rows_for_session(conn, session, effective_candidates),
            redaction=redaction,
        ))
    if expansion.applied:
        added_effective = _effective_view_lexical_candidates(
            conn, session, query=ranked_query, filter=filter, segment=segment,
            branch_point_value=bp,
            exclude_node_ids={str(row["node_id"]) for row in candidates},
            scope=scope, effective_view_source=effective_view_source,
            scope_rows=scope_rows,
        )
        expansion_scan_capped |= len(added_effective) >= _LEXICAL_RANK_CAP
        cap.record("effective_expanded", added_effective, limit=_LEXICAL_RANK_CAP,
                   count=None, native=False)
        candidates.extend(keep_readable_text(
            added_effective,
            filter_node_rows_for_session(conn, session, added_effective),
            redaction=redaction,
        ))
    candidates = apply_effective_attrs_to_rows(
        conn, candidates, target_kind="node", target_id_field="node_id",
        generation_id=pin,
    )
    declared_children: set[str] = set()
    if fold_shape == _FOLD_UNFILTERED:
        declared_children = set(passage_child_declarations(
            conn,
            catalog_version_id=session.catalog_version_id,
            child_subtypes=sorted({str(row.get("subtype") or "") for row in candidates}),
        ))
    if _has_row_filter(filter):
        candidates = [
            row for row in candidates
            if str(row.get("subtype") or "") in declared_children
            or _matches_filter(row, _row_filter(filter))
        ]
    if scope is not None and scope.excluded_path_globs:
        # The SQL predicate above reads the STORED `attrs->>'path'` so the rank
        # cap is spent correctly; this re-check runs over the EFFECTIVE attrs,
        # so an overlay that changed a row's path cannot smuggle it past an
        # exclusion the envelope reports.
        candidates = [
            row for row in candidates
            if not excluded_by_path_globs(
                row.get("attrs"), scope.excluded_path_globs,
            )
        ]

    # Phrase verdicts on the text each candidate is served from: these rows
    # went through the per-row read filter and the effective-attrs overlay,
    # so a span present only in masked text does not match (PACT
    # lexical-quoted-phrases-3064). Under `passages` the verdict is taken on
    # the served page instead (the narrow chunk scan has no text yet).
    if phrases and rank_by != RANK_BY_PASSAGES:
        for row in candidates:
            _mark_phrase(row, phrases)

    # Whether BM25 itself saturated, measured BEFORE alias promotion: injecting a
    # row must not make an unsaturated scan claim its relevance was truncated.
    # Measured on the RAW reads (`expansion_scan_capped`, the scan cap's
    # saturated reads), not only on the list Nomos, the overlay,
    # `attr_filters` and the path re-check left: a read that filled its limit
    # and was then filtered to 30 rows still has matches past its limit, so
    # its count is a floor (PACT lexical-index-scan-cap-3162, review H1).
    #
    # Under enforced Nomos (`cap.withhold_counts`) the raw reads hold rows
    # the caller may not read, so a floor taken from them would say "hidden
    # rows filled this read". There the flag is taken only from the rows
    # that survived Nomos and the filters, as dev takes it on its plain
    # path; dev's expanded path read the raw flag, so this says less than
    # dev there (review 3, item 1).
    rank_capped = (
        bool(rollup_chunk_cap_reached)
        if rank_by == RANK_BY_PASSAGES
        else len(candidates) >= _LEXICAL_RANK_CAP
        if cap.withhold_counts
        else (
            expansion_scan_capped
            or bool(cap.saturated & {"self", "self_expanded"})
            or len(candidates) >= _LEXICAL_RANK_CAP
        )
    )

    # An exact catalog-alias hit is evidence about which node answers the query.
    # Promote the canonical it names to the head of the page so a query phrased
    # as a known alias surfaces the entity, instead of the entity ranking below
    # whatever happened to share its wording (issue #797).
    promoted_id: Optional[str] = None
    alias_target = (
        None
        if rank_by == RANK_BY_PASSAGES
        else _authoritative_alias_target(
            conn, session, query, subtype=filter.get("subtype"), scope=scope,
        )
    )
    if alias_target is not None and scope_rows is not None and (
        alias_target not in scope_rows.record_ids
    ):
        # Promotion must not be a side door out of a record scope.
        alias_target = None
    if alias_target is not None:
        alias_row = _hydrate_lexical_alias_target(
            conn,
            session,
            target=alias_target,
            source="catalog_alias",
            query=query,
            segment=segment,
            branch_point_value=bp,
            filter=filter,
            scope=scope,
        )
        if alias_row is not None:
            best_score = min(
                (
                    float(original_scores[str(row["node_id"])])
                    if expansion.applied else float(row.get("score") or 0.0)
                    for row in candidates
                    if not expansion.applied
                    or str(row["node_id"]) in original_scores
                ),
                default=0.0,
            )
            alias_row["score"] = best_score - 1.0
            promoted_id = str(alias_row["node_id"])
            if phrases:
                _mark_phrase(alias_row, phrases)
            # Replace rather than append: the canonical is often already a BM25
            # candidate, and it must appear once, at the promoted rank.
            by_id = {str(row["node_id"]): row for row in candidates}
            by_id[str(alias_row["node_id"])] = alias_row
            candidates = list(by_id.values())

    # Exact before expanded, as the LEADING sort key. `_expansion_rank` is 0
    # for every row when nothing expanded, so the order is then byte-for-byte
    # the previous one. A score offset could not give this guarantee: a rare
    # added term can out-score a common typed one on IDF alone.
    for row in candidates:
        if expansion.applied:
            row["original_query_match"] = str(row["node_id"]) in original_scores
            if row["original_query_match"] and not row.get("match_kind"):
                row["score"] = original_scores[str(row["node_id"])]
        row["expansion_rank"] = _expansion_rank(row, expansion)
    # A quoted search leads with the phrase rank: rows whose served text holds
    # every span first, the alias-promoted row placed first whatever its text
    # holds. Under the filtered fold the fold sorts by today's key and
    # promotes only inside its exact prefix (`_fold_filtered`).
    phrase_sort = bool(phrases) and fold_shape != _FOLD_FILTERED
    candidates.sort(
        key=lambda row: (
            (
                0 if str(row["node_id"]) == promoted_id
                else (0 if row.get(PHRASE_KEY) else 1)
            ) if phrase_sort else 0,
            int(row.get("expansion_rank") or 0),
            float(row.get("score") or 0.0),
            str(row["node_id"]),
        )
    )

    fold: Optional[_FilteredFold] = None
    # Rows the self read already read row by row; under the unfiltered fold
    # every other served row is read after paging.
    self_read_ids = {str(row["node_id"]) for row in candidates}
    if fold_shape == _FOLD_UNFILTERED:
        candidates = _fold_unfiltered(
            conn,
            session,
            candidates,
            declared_children=declared_children,
            alias_node_id=promoted_id,
            segment=segment,
            branch_point_value=bp,
            filter=filter,
            scope=scope,
            phrase_first=bool(phrases),
        )
    elif fold_shape == _FOLD_FILTERED:
        assert filtered_rollup is not None
        fold = _fold_filtered(
            conn,
            session,
            candidates,
            rollup=filtered_rollup,
            query=bm25_query,
            ranked_query=ranked_query,
            expansion=expansion,
            self_saturated=self_literal_saturated,
            self_effective_saturated=self_effective_saturated,
            self_effective_ids=self_effective_ids,
            self_expanded_saturated=bool(
                cap.saturated & {"self_expanded", "effective_expanded"}
            ),
            promoted_id=promoted_id,
            segment=segment,
            index_literal=index_literal,
            branch_point_value=bp,
            filter=filter,
            scope=scope,
            cap=cap,
            phrases=phrases,
            text_redaction=text_redaction,
        )
        candidates = fold.rows
        rank_capped = fold.lower_bound

    # Exact duplicates (issue #3465, `rank3465 A`): under `records` a row
    # whose type and served text equal a higher-ranked row's leaves the list,
    # and the higher row lists it in `copies`. It runs before the count and
    # paging, so `total_count`, the offset slice, the exact prefix and byte
    # fitting all see one list, and before diversity, so a copy never takes
    # a parent's slot (`total_count_before_diversity` counts after it).
    duplicates_collapsed: Optional[int] = None
    copies_by_id: dict[str, list[str]] = {}
    if rank_by == RANK_BY_RECORDS:
        before_collapse = candidates
        candidates, copies_by_id = _collapse_exact_duplicates(
            candidates,
            self_read=(
                fold.self_ids if fold is not None
                else self_read_ids if fold_shape == _FOLD_UNFILTERED
                else None
            ),
        )
        duplicates_collapsed = len(before_collapse) - len(candidates)
        if fold is not None and duplicates_collapsed:
            # The exact prefix counts the kept rows inside the old prefix: a
            # copy always ranks below its kept row, so the kept rows of the
            # prefix are still the exactly ordered head of the list.
            kept_ids = {str(row["node_id"]) for row in candidates}
            fold.ordered_exactly_through = sum(
                1 for row in before_collapse[:fold.ordered_exactly_through]
                if str(row["node_id"]) in kept_ids
            )

    # Diversity runs AFTER ranking (the ranking is what decides which sibling of
    # a parent is the best one to keep) and BEFORE pagination (suppressing after
    # pagination would under-fill the page). Scores and order are untouched.
    diversity = apply_max_per_parent(
        conn,
        session,
        catalog_version_id=session.catalog_version_id,
        node_segment=segment,
        edge_segment=(
            resolve_edge_segment(conn)
            if (scope is not None and scope.max_per_parent is not None)
            else ""
        ),
        branch_point_value=bp,
        rows=candidates,
        max_per_parent=scope.max_per_parent if scope is not None else None,
    )
    candidates = diversity.rows

    total_count = len(candidates)
    if rank_by == RANK_BY_PASSAGES:
        assert_rollup_offset_ordered(
            offset=offset,
            total_count=total_count,
            subtype=str(subtype),
            chunk_cap_reached=bool(rollup_chunk_cap_reached),
        )
    next_offset_reason: Optional[str] = None
    if fold is not None:
        page, truncated, next_offset, next_offset_reason = _page_exact_prefix(
            candidates,
            offset=offset,
            limit=limit,
            ordered_exactly_through=fold.ordered_exactly_through,
            lower_bound=fold.lower_bound,
            subtype=str(subtype),
            expansion_remedy=fold.expansion_remedy,
            floor_truncates=exact,
        )
        # The rows this page returns that the self read did not already read
        # are read row by row now: `read` decisions and redaction (5b).
        read_returned_rows(conn, session, page=page, already_read=fold.self_ids)
    else:
        truncated = offset + limit < total_count
        page = candidates[offset: offset + limit]
        next_offset = _next_offset(
            offset=offset, limit=limit, truncated=truncated,
        )
        if fold_shape == _FOLD_UNFILTERED:
            read_returned_rows(conn, session, page=page, already_read=self_read_ids)
    # A list delivers no recency annotation (cards and `inspect` do), so it
    # does not read one.
    recency_rows, recency_block = recency_disclosure(
        conn,
        page,
        session=session,
        segment=segment,
        branch_point_value=bp,
        subtypes=[subtype] if subtype is not None else None,
    ) if not list_view else ({}, None)
    # A list names a passage row's parent (issue #3467) and reads parents
    # only when its page holds a declared passage: a page of records pays
    # nothing for it.
    wants_parents = include_excerpts or select is None or (
        "parent" in select
        and (not list_view or any(
            str(row.get("subtype") or "") in declared_children for row in page
        ))
    )
    parent_rows, parent_block = resolve_passage_parents(
        conn,
        session=session,
        page=page,
        node_segment=segment,
        branch_point_value=bp,
        citation_from_attrs=_citation_from_attrs,
        withhold_passage_counts=passage_counts_withheld(session),
    ) if wants_parents else ({}, {})
    if rank_by == RANK_BY_PASSAGES and rollup is not None:
        # A narrow chunk scan (Nomos off) ranked on ids and scores; read the
        # winning chunk of each parent this page returns, once per call
        # (PACT passage-rollup-scan-cost, narrow-rows). A full scan's rows,
        # redacted or not, already carry attrs and are never re-read.
        fill_passage_rows(
            conn,
            [
                rollup_best_chunks[str(row["node_id"])]
                for row in page
                if str(row["node_id"]) in rollup_best_chunks
            ],
            node_segment=segment,
            child_subtype=rollup.child_subtype,
            branch_point_value=params["bp"],
            read_key=lambda kind, sql, read_params: _read_key(
                conn, session, f"{kind}:{cap.mode}", sql, read_params,
            ),
        )
    # The quoted-phrase disclosure (PACT lexical-quoted-phrases-3064), over
    # the candidates whose verdict was checked: every candidate on a self or
    # unfiltered-fold page; the exact prefix (which holds the served page)
    # under the filtered fold; the served page under `passages`, whose
    # verdict is taken here on the winning passage the page is served from.
    phrase_block: Optional[dict[str, Any]] = None
    if phrases:
        if rank_by == RANK_BY_PASSAGES:
            # The winning passage as the preview serves it: overlaid at the
            # pin, as `_preview_winning_passages` overlays it.
            served_chunks = {
                str(chunk["node_id"]): chunk
                for chunk in apply_effective_attrs_to_rows(
                    conn,
                    [
                        dict(rollup_best_chunks[str(row["node_id"])])
                        for row in page
                        if rollup_best_chunks.get(str(row["node_id"])) is not None
                    ],
                    target_kind="node", target_id_field="node_id",
                    generation_id=pin,
                )
            }
            for row in page:
                chunk = rollup_best_chunks.get(str(row["node_id"]))
                served = served_chunks[str(chunk["node_id"])] if chunk is not None else row
                row[PHRASE_KEY], row[PHRASE_SPANS_KEY] = _phrase_verdict(
                    [_served_text(served)], phrases,
                )
            checked = list(page)
        elif fold is not None:
            checked = candidates[:fold.ordered_exactly_through]
        else:
            checked = candidates
        matched = sum(1 for row in checked if row.get(PHRASE_KEY))
        found: set[int] = set()
        for row in checked:
            found |= set(row.get(PHRASE_SPANS_KEY) or ())
        phrase_block = {
            "spans": list(spans),
            "unmatched_spans": [
                span for i, span in enumerate(spans) if i not in found
            ],
            "matched": matched,
            "checked": len(checked),
            "status": "matched" if matched else "no_exact_match",
        }
    # One lexizing call per page places every excerpt on it (D4).
    winners = {
        str(row["node_id"]): row[WINNER_KEY]
        for row in page if row.get(WINNER_KEY) is not None
    }
    matcher = build_matcher(
        conn,
        ranked_query,
        [
            str((row.get("attrs") or {}).get("text", ""))
            for row in (
                *page,
                *winners.values(),
                # Under `passages` the excerpt is cut from the winning chunk
                # (review B2).
                *(
                    rollup_best_chunks[str(row["node_id"])]
                    for row in page
                    if str(row["node_id"]) in rollup_best_chunks
                ),
            )
        ],
    ) if include_excerpts else None
    # PACT read-passage-rows-3666: each card row's matched passage, chosen
    # over its record's whole chunk text where the catalog declares chunk
    # passages (`record_text.search_record_texts`), else over its winning
    # chunk or its own text. Its own matcher, so the page's excerpts keep
    # theirs; one more lexizing call per page.
    passage_meta: dict[str, dict[str, Any]] = {}
    chained_texts: dict[str, ChainedText] = {}
    passage_winners: dict[str, Mapping[str, Any]] = dict(winners)
    passage_matcher: Optional[ExcerptMatcher] = None
    if include_excerpts and page:
        passage_meta = _metadata_for_rows(
            conn, session, page,
            metadata=getattr(filter.get("where_context"), "metadata", None),
        )
        chained_texts = search_record_texts(conn, session, [
            str(row["node_id"]) for row in page
            if declares_chunk_passages(passage_meta.get(str(row.get("subtype"))) or {})
        ])
        if rank_by == RANK_BY_PASSAGES:
            # The winning chunk as `_preview_winning_passages` serves it:
            # overlaid at the pin.
            best = {
                str(chunk["node_id"]): chunk
                for chunk in apply_effective_attrs_to_rows(
                    conn,
                    [
                        dict(rollup_best_chunks[str(row["node_id"])])
                        for row in page if str(row["node_id"]) in rollup_best_chunks
                    ],
                    target_kind="node", target_id_field="node_id", generation_id=pin,
                )
            }
            for row in page:
                chunk = rollup_best_chunks.get(str(row["node_id"]))
                if chunk is not None:
                    passage_winners[str(row["node_id"])] = best.get(str(chunk["node_id"]), chunk)
        passage_matcher = build_matcher(
            conn,
            ranked_query,
            [
                *(str((row.get("attrs") or {}).get("text", "")) for row in page),
                *(
                    str((chunk.get("attrs") or {}).get("text", ""))
                    for chunk in passage_winners.values()
                ),
                *(chained.text for chained in chained_texts.values()),
                *(
                    str((row.get("attrs") or {}).get("message_body", ""))
                    for row in page
                ),
            ],
        )
    full_rows: list[dict[str, Any]] = []
    for row in page:
        text = str((row.get("attrs") or {}).get("text", ""))
        node_id = str(row["node_id"])
        winner = winners.get(node_id)
        # A list (no matcher) shows no excerpt, so it chooses no window.
        start, excerpt = (
            window_span(text, matcher, max_chars=max_chars)
            if matcher is not None else (0, None)
        )
        full_row = {
            "node_id": row["node_id"],
            "canonical_node_id": row["node_id"],
            "subtype": row["subtype"],
            "attrs": row["attrs"],
            **({"excerpt": excerpt} if matcher is not None else {}),
            **({"text_hash": row["text_hash"]} if "text_hash" in row else {}),
            "score": row["score"],
            "method": "lexical",
            "match_kind": row.get("match_kind", "lexical"),
            "source": row.get("source"),
            # ALWAYS present, explicitly null when the producer attributed
            # none. An absent key would read as "this tool does not report
            # source records", which is the state this change ends.
            "source_record_id": record_identity(row.get("source_record_id")),
            "source_revision": record_identity(row.get("source_revision")),
            # Null means the pinned catalog declares no passage relation for
            # this subtype; every other outcome is an object with a `state`.
            "parent": parent_rows.get(node_id),
            # The passage's own place in its record (issue #2675), read from
            # the Nomos-filtered effective attrs BEFORE projection, so it is on
            # the card even when `attrs` is not selected. Null when none. A
            # folded record names its winning passage's place.
            "position": passage_position(
                (winner if winner is not None else row).get("attrs")
            ),
        }
        # ALWAYS present, like the record-identity fields: the key is in the
        # default projection, so emitting it only under `rank_by="passages"`
        # would make its absence mean two different things — "this row was not
        # parent-ranked" and "this tool does not report a winning passage" —
        # which is the ambiguity the never-absent discipline exists to remove.
        # Null on a self-ranked row: no rollup ran, so no chunk won.
        full_row["rollup_best_chunk_id"] = (
            rollup_best_chunk_ids.get(node_id)
            if rank_by == RANK_BY_PASSAGES
            else (str(winner["node_id"]) if winner is not None else None)
        )
        # Readable passages folded into this record; null on a row that is
        # not a record taking passages (an unfolded passage, `self`).
        full_row["matching_passages"] = (
            row.get(MATCHING_KEY) if rank_by == RANK_BY_RECORDS else None
        )
        annotation = recency_rows.get(node_id)
        if annotation is not None:
            full_row["recency"] = annotation
        citation = _citation_from_attrs(row.get("attrs"))
        if citation:
            full_row["citation"] = citation
        if passage_matcher is not None:
            full_row[PASSAGE_KEY] = row_passage(
                full_row, passage_meta.get(str(row.get("subtype"))) or {},
                matcher=passage_matcher, chained=chained_texts.get(node_id),
                winner=passage_winners.get(node_id), chars=passage_chars,
            )
        # A hit on a chunk's own text points into it (PACT
        # read-model-s5-location); a row whose excerpt comes from a winning
        # passage gets that passage's location in `_preview_winning_passages`.
        if winner is None and node_id not in rollup_best_chunks:
            location = chunk_location(row, start)
            if location is not None:
                full_row[LOCATION_KEY] = location
        if phrases:
            # Last, and only on a quoted search: whether the text this row is
            # served from holds every quoted span (PACT
            # lexical-quoted-phrases-3064).
            full_row["phrase_match"] = bool(row.get(PHRASE_KEY))
        full_rows.append(full_row)
    results = _fence_search_rows(
        conn, session, full_rows, select=select,
        default=_LEXICAL_DEFAULT, always=_LEXICAL_ALWAYS,
        scope=scope, max_chars=max_chars,
        # The catalog read the passages already made (one per page): read
        # again only for a type it did not cover.
        metadata=passage_meta or getattr(filter.get("where_context"), "metadata", None),
    )
    if list_view and fold_shape == _FOLD_UNFILTERED:
        # The records fold keeps a passage it could not fold as its own row
        # (no readable record; an alias-promoted passage, rule 8). A list says
        # so on that row instead of passing it off as a record.
        passage_ids = _unfolded_passage_ids(
            conn, session, page,
            declared_children=declared_children, promoted_id=promoted_id,
        )
        for result_row in results:
            if str(result_row.get("node_id")) in passage_ids:
                result_row[LIST_ROW_UNIT_KEY] = "passage"
    if copies_by_id:
        # After fencing, like the list's unit: `select` cannot hide it. Only
        # on a row that has copies; the first `COPIES_LISTED_MAX` in rank
        # order, with `copies_total` when more were collapsed.
        for result_row in results:
            row_copies = copies_by_id.get(str(result_row.get("node_id")))
            if row_copies:
                result_row["copies"] = row_copies[:COPIES_LISTED_MAX]
                if len(row_copies) > COPIES_LISTED_MAX:
                    result_row["copies_total"] = len(row_copies)
    locate =select is None or LOCATION_KEY in select
    if rank_by == RANK_BY_PASSAGES and matcher is not None:
        _preview_winning_passages(
            conn, session, results, full_rows, rollup_best_chunks,
            matcher=matcher, max_chars=max_chars, generation_id=pin,
            locate=locate,
        )
    elif winners and matcher is not None:
        # Folded rows' winners are already read-filtered and overlaid.
        _preview_winning_passages(
            conn, session, results, full_rows, winners,
            matcher=matcher, max_chars=max_chars, generation_id=None,
            locate=locate,
        )
    retained_members = (
        _reference_members(
            conn, session, page, winners=winners, passage_chunks=rollup_best_chunks,
            matcher=matcher, max_chars=max_chars,
            passage_ids=_unfolded_passage_ids(
                conn, session, page,
                declared_children=declared_children, promoted_id=promoted_id,
            ),
        )
        if reference_members else None
    )
    # Per `index-document-source-file-search` (issue #900): a subtype-filtered
    # search that finds nothing returns `total_count: 0` whether the subtype is
    # unsearchable, absent, or simply has no match — three different facts, one
    # indistinguishable result, which sends the caller looking for a better
    # query when no query could ever work. Diagnose the zero-hit case on the
    # rows themselves. Computed only when a subtype filter is present AND the
    # page is empty; a hit is its own explanation.
    #
    # `bm25_matched` is read BEFORE the Nomos read filter, so it may only
    # answer for a subtype as a whole (as it always has). Under a record
    # scope, or a `where` naming `id` (which joins the ranked SQL), it would
    # answer for named records and their hidden passages: whether a hidden
    # record exists and holds a term. No diagnosis there, so a hidden record
    # reads exactly as a missing one.
    narrowed_to_records = scope_rows is not None or search_where.names_field(
        filter.get("where"), "id",
    )
    diagnosis: Optional[tuple[Optional[bool], str, str]] = None
    if (
        subtype is not None and total_count == 0 and not bm25_matched
        and not narrowed_to_records
    ):
        diagnosed_subtype = (
            rollup.child_subtype if rollup is not None else subtype
        )
        diagnosis = _diagnose_subtype_zero_hits(
            conn, session, segment=segment, subtype=diagnosed_subtype,
            query=bm25_query,
            branch_point_value=bp, generation_id=pin,
        )
    payload = {
        "query": query,
        "method": "lexical",
        "generation_id": pin,
        "pinned_generation_id": pin,
        "limit": limit,
        "offset": offset,
        "next_offset": next_offset,
        "results": results,
        "total_count": total_count,
        "truncated": truncated,
        # Each pass has _LEXICAL_RANK_CAP candidates; disclose saturation so
        # (offset past the cap) returning empty is legible rather than silent.
        "rank_capped": rank_capped,
        # Diversity disclosure. `total_count` is the number of rows a caller can
        # PAGE THROUGH, so under diversity it counts the survivors: pagination
        # runs over the diversified list, and a total that still counted
        # suppressed siblings would promise pages that do not exist.
        # `total_count_before_diversity` is the pre-suppression figure, stated
        # so the two are never confused.
        "diversity_applied": diversity.applied,
        "max_per_parent": diversity.max_per_parent,
        "suppressed_sibling_count": diversity.suppressed_sibling_count,
        "total_count_before_diversity": (
            diversity.total_count_before_diversity
            if diversity.applied
            else None
        ),
        "diversity_parents_resolved": (
            diversity.parents_resolved if diversity.applied else None
        ),
        # Rows the declared passage relation could not attach to a parent. They
        # are NEVER suppressed, and the count says how much of the page the cap
        # therefore did not govern.
        "diversity_rows_without_parent": (
            diversity.rows_without_parent if diversity.applied else None
        ),
    }
    if rank_by == RANK_BY_PASSAGES and rollup is not None:
        payload["rank_by_relation"] = {
            "parent_subtype": rollup.parent_subtype,
            "edge_type": rollup.edge_type,
            "child_subtype": rollup.child_subtype,
            "source": "catalog:okg_passage_rollup",
        }
        payload["rank_by_aggregation"] = PASSAGE_ROLLUP_AGGREGATION
        payload["rollup_chunks_scanned"] = rollup_chunks_scanned
        payload["rollup_chunk_cap"] = PASSAGE_ROLLUP_CHUNK_CAP
        payload["rollup_chunk_cap_reached"] = rollup_chunk_cap_reached
        payload["rollup_contributing_chunks"] = {
            str(row["node_id"]): rollup_chunk_counts.get(
                str(row["node_id"]), 0,
            )
            for row in page
        }
        payload["total_count_unit"] = rollup.parent_subtype
        payload["rank_capped_unit"] = "chunks"
    payload.update(_count_basis(
        rank_by=rank_by,
        rank_capped=rank_capped,
        rollup_chunk_cap_reached=rollup_chunk_cap_reached,
        fold=fold,
    ))
    if result_unit is not None:
        payload["result_unit"] = result_unit
    if duplicates_collapsed is not None:
        # Rows removed as exact copies of a higher-ranked row (#3465);
        # `total_count` counts the rows left. Null wherever the count is.
        payload["duplicates_collapsed"] = duplicates_collapsed
    if fold is not None:
        payload["ordered_exactly_through"] = fold.ordered_exactly_through
        if fold.passage_read_fallback is not None:
            # Same rows, slower path: say which and why (PACT
            # mcp-filtered-passage-cost). Absent on the normal path.
            payload["passage_read_fallback"] = fold.passage_read_fallback
    if cap.reads and not cap.withhold_counts:
        # Reads served by the complete form under completeness="exact":
        # every short BM25 read, which the index's result cap may have cut
        # (PACT lexical-index-scan-cap-3162). Absent when every read was
        # full, and always under the bounded default. Absent under enforced
        # Nomos: which reads were short is measured on raw rows before Nomos,
        # so naming them would say whether hidden rows filled a read.
        payload["index_scan_cap_reads"] = list(cap.reads)
        payload["lexical_exact_reads"] = {
            name: {"candidate_rows": count.candidates,
                   "matching_rows": count.matches,
                   "unit": "text_rows", "set": f"{name}_stored_scoped_input",
                   "basis": "exact_scalar",
                   "visibility_basis": "stored_input_non_enforce",
                   "work_row_limit": _EXACT_LEXICAL_WORK_ROWS,
                   "timeout_ms": count.timeout_ms}
            for name, count in cap.reads.items()
        }
    if (
        cap.saturated and payload["total_count_basis"] == "exact"
        and not cap.withhold_counts
    ):
        # A raw read filled its limit, so matches may lie past it even where
        # the path's own rule (a filtered fold that does not track every read)
        # called the count whole (review H1). The count is a floor. Not under
        # enforced Nomos: there the raw read held rows the caller may not
        # read, so the floor would say hidden rows filled it (review 3).
        payload["rank_capped"] = True
        payload.update(_count_basis(
            rank_by=rank_by, rank_capped=True, rollup_chunk_cap_reached=True,
            fold=None,
        ))
        if fold is not None:
            payload["ranked_cap"] = {
                "rows": _LEXICAL_RANK_CAP, "passages": _LEXICAL_RANK_CAP,
            }
    # One public retrieval fact: PACT operator-page-counts-3190's
    # `coverage.retrieval`, pre-filled here and kept by `with_read_coverage`.
    # `completeness` is only the requested mode, which pagination and the
    # renderer read.
    payload["completeness"] = cap.mode
    payload["coverage"] = {
        "retrieval": _lexical_retrieval(
            cap, rank_capped=bool(payload.get("rank_capped")),
            work_capped=bool(rollup_chunk_cap_reached),
            count_exact=payload["total_count_basis"] == "exact",
        ),
    }
    if exact and payload["coverage"]["retrieval"].get("status") != "exhaustive":
        # Issue #3666: an exact read that did not read every match (a read
        # filled the rank cap or the work cap) is still served, as PACT
        # lexical-index-scan-cap-3162 designed, but never as the whole
        # match set: its listing is not complete, and paging stops at the
        # bound. Under enforced access rules no cap is named.
        _cut_exact_listing(payload, withheld=cap.withhold_counts)
    if row_filter_after_ranking and cap.withhold_counts:
        # Issue #3666: under enforced access rules whether the ranked read
        # filled its cap is measured, on some paths, on rows the reader may
        # not see. So the page says the same thing whether or not it did:
        # the filter is checked on the ranked rows only, not shown whole,
        # and names no cap.
        payload["where_checked_on"] = {
            "ranked_rows_only": True, "every_record": WHERE_EVERY_RECORD,
        }
    elif row_filter_after_ranking and payload.get("rank_capped"):
        # Issue #3666: that filter was checked on the capped ranked
        # candidates only, so a record past the cap was never checked. Said
        # where the count is read, with the read that checks every record.
        payload["where_checked_on"] = {
            "ranked_cap": dict(payload.get("ranked_cap") or {"rows": _LEXICAL_RANK_CAP}),
            "every_record": WHERE_EVERY_RECORD,
        }
    count_unknown = not exact or (
        # Exact under enforced Nomos whose proof needed raw (pre-Nomos)
        # saturation: retrieval says `unknown`, and the count, its floor and
        # the exact prefix say no more than that, as on the bounded page.
        # This repeats the status's one bit; it adds none (review 3).
        cap.withhold_counts
        and payload["coverage"]["retrieval"].get("status") == "unknown"
    )
    if count_unknown:
        # Policy option A: the native candidates may have been cut by the
        # index's result cap before scope filters ran, short page or full,
        # and nothing here proves otherwise. No count, no absence claim.
        payload["total_count"] = None
        payload["total_count_basis"] = "unknown"
        payload["total_count_before_diversity"] = None
        if "duplicates_collapsed" in payload:
            payload["duplicates_collapsed"] = None
        if "ordered_exactly_through" in payload:
            # The fold's exact prefix is exact only within candidates a cut
            # read returned; a record past the cut could rank inside it
            # (review M1). Paging still uses it within the candidates.
            payload["ordered_exactly_through"] = None
        if diagnosis is not None and diagnosis[1] in _READ_DEPENDENT_ZERO_HITS:
            # Both start from "BM25 matched nothing", which a possibly cut
            # read cannot establish: `no_match` claims the words are absent,
            # `not_tokenized` blames the text-search config. `no_rows` and
            # `not_indexed` describe the subtype's rows, not the read, and
            # stand.
            diagnosis = None
    if next_offset_reason is not None:
        payload["next_offset_reason"] = next_offset_reason
        if fold is not None and fold.expansion_remedy:
            # A filled read of the expanded query ended the exact order;
            # narrowing may not reach the later records, rerunning without
            # expansion once per word in `rerun_each` does (review 3, item 2).
            payload["next_offset_remedy"] = dict(fold.expansion_remedy)
    if matcher is not None:
        payload["excerpt_window_basis"] = matcher.basis
    if diagnosis is not None:
        payload["subtype_indexed"] = diagnosis[0]
        payload["zero_hit_reason"] = diagnosis[1]
        payload["zero_hit_scanned"] = diagnosis[2]
    if not list_view:
        payload["recency"] = recency_block
    payload["parent_relation"] = parent_block
    # UNCONDITIONAL, like every other disclosure here: a page holding rows the
    # caller's own wording never found, which does not say so, is the page an
    # agent reads as proof its wording was right. An ABSENT key would instead
    # read as "this build cannot expand", a different fact.
    payload["concept_expansion"] = expansion.disclosure()
    if retained_members is not None:
        payload["_reference_members"] = retained_members
    if phrase_block is not None:
        # Only on a quoted search: an unquoted page is unchanged.
        if count_unknown:
            # The page withholds its count (bounded mode, or exact under
            # enforced Nomos whose proof needed raw saturation): the number of
            # candidates checked, and how many matched, would say what the
            # count does not. The status still says whether any matched.
            phrase_block["checked"] = None
            phrase_block["matched"] = None
        payload["phrase"] = phrase_block
    return payload


_REFERENCE_WORD = re.compile(r"[^\W_]+", re.UNICODE)


def _reference_members(
    conn: psycopg.Connection,
    session: Session,
    page: list[dict[str, Any]],
    *,
    winners: dict[str, dict[str, Any]],
    passage_chunks: dict[str, dict[str, Any]],
    matcher: Optional[ExcerptMatcher],
    max_chars: int,
    passage_ids: set[str],
) -> dict[str, Any]:
    """A retained search's result-reference members (#3222 slice 2).

    `page` is the per-row-decided list this search delivers at offset 0: the
    top `limit` VISIBLE rows in rank order, each already read through the
    reader's filter. Lane K's #3369 will return exactly this list from
    `_search_lexical_outcome`; switching to it changes only the `page`
    argument. A member is `{node_id, rank, location}`, never text or attrs.

    A reference holds records only (design D1). `passage_ids` are the page's
    rows the records fold kept as passages (no readable record, an
    alias-promoted passage, or one kept through a record outside the profile,
    issue #3466; `_unfolded_passage_ids`): they are delivered on
    the page but are not members, and their count is returned as
    `passages_excluded` so the drop is disclosed, never silent. The count is
    over the frozen page, rows byte fitting may not deliver included; search
    serves those same rows to this reader, so it says nothing about hidden rows.
    Ranks are consecutive over the members.

    A location comes only from the reader's served, masked view of the row
    that holds the hit (a folded record's winning passage, a passage-ranked
    record's best chunk read through the same filter, else the row itself),
    and only when that masked text still holds a query word: a match only
    inside a masked span stores no location. Only the winning chunks of this
    page's members are read.
    """
    kept = [row for row in page if str(row["node_id"]) not in passage_ids]
    located: dict[str, Optional[dict[str, Any]]] = {}
    member_chunks = {
        str(row["node_id"]): passage_chunks[str(row["node_id"])]
        for row in kept
        if passage_chunks.get(str(row["node_id"]))
    }
    if member_chunks:
        readable = apply_effective_attrs_to_rows(
            conn,
            filter_node_rows_for_session(
                conn, session, [dict(chunk) for chunk in member_chunks.values()],
            ),
            target_kind="node", target_id_field="node_id",
            generation_id=session.generation_id,
        )
        by_id = {str(row["node_id"]): row for row in readable}
        located = {
            parent: by_id.get(str(chunk.get("node_id")))
            for parent, chunk in member_chunks.items()
        }
    members: list[dict[str, Any]] = []
    for rank, row in enumerate(kept, 1):
        node_id = str(row["node_id"])
        if node_id in passage_chunks:
            holder = located.get(node_id)
        else:
            holder = winners.get(node_id) or row
        members.append({
            "node_id": node_id,
            "rank": rank,
            "location": _member_location(holder, matcher, max_chars=max_chars),
        })
    return {"members": members, "passages_excluded": len(page) - len(kept)}


def _member_location(
    row: Optional[dict[str, Any]],
    matcher: Optional[ExcerptMatcher],
    *,
    max_chars: int,
) -> Optional[dict[str, Any]]:
    if row is None or matcher is None or matcher.empty:
        return None
    text = (row.get("attrs") or {}).get("text")
    if not isinstance(text, str) or not text:
        return None
    if not any(matcher.terms(word.group(0)) for word in _REFERENCE_WORD.finditer(text)):
        return None
    start, _excerpt = window_span(text, matcher, max_chars=max_chars)
    return chunk_location(row, start)


def _search_semantic(
    conn: psycopg.Connection,
    session: Session,
    query: str,
    *,
    embedder: Optional[Embedder],
    filter: dict[str, Any],
    limit: int,
    offset: int,
    select: Optional[list[str]] = None,
    scope: Optional[ResolvedScope] = None,
    embedder_spec: Optional[str] = None,
    allow_unverified_provenance: bool = False,
    embedder_source: Optional[str] = None,
    max_chars: int = DEFAULT_PREVIEW_CHARS,
    passage_chars: Optional[int] = None,
) -> dict[str, Any]:
    validate_select(
        select, _SEMANTIC_FIELDS, tool_name="search(method=semantic)",
    )
    from okg.substrate.embeddings.identity import (
        assert_embedder_matches_substrate_width,
        embedder_model_id,
        model_id_for_spec,
    )
    from okg.substrate.mcp.semantic_readiness import (
        assert_rankable,
        probe_semantic_readiness,
    )

    # Measure over the subtypes ACTUALLY RANKED, which is the profile scope
    # INTERSECTED with an explicit `subtype` filter -- not the profile scope
    # alone. An explicit subtype narrows the returned rows (applied by
    # `_matches_filter` below), so measuring the wider profile would report
    # `embedded_nodes > 0` on a page whose own subtype holds no vectors: the
    # partial-coverage confusion this disclosure exists to remove, one level
    # narrower. `intersect_subtype_filter` already established in `search()`
    # that the named subtype lies inside the profile, so intersecting here
    # cannot produce the empty set.
    subtype_filter = filter.get("subtype")
    if subtype_filter is not None:
        scoped_subtypes = [subtype_filter]
    elif scope is not None and scope.subtypes is not None:
        scoped_subtypes = list(scope.subtypes)
    else:
        scoped_subtypes = None

    # Vector sets (PACT openrouter-embeddings-hnsw, design v3.2): when the
    # search set is registered and active, rank its vectors. This is decided
    # before the readiness probes below, which measure the old node columns.
    route = _vector_set_route(
        conn, embedder=embedder, embedder_spec=embedder_spec,
        generation_id=str(session.generation_id),
    )
    if route is not None and route.status == "active":
        return _search_semantic_vector_set(
            conn, session, query, route=route, embedder=embedder,
            embedder_spec=embedder_spec, embedder_source=embedder_source,
            filter=filter, limit=limit, offset=offset, select=select,
            scope=scope, scoped_subtypes=scoped_subtypes,
            allow_unverified_provenance=allow_unverified_provenance,
            max_chars=max_chars, passage_chars=passage_chars,
        )

    # PROBE BEFORE CONSTRUCT (issue #797). The identity is a pure function of
    # the spec, so an unembedded or model-skewed corpus is refused without
    # loading a sentence-transformer from disk (which happens in __init__, not
    # in embed()). Callers passing an already-built embedder have paid that
    # cost already; the probes still run and still refuse for them.
    if embedder_spec is not None:
        query_model_id = model_id_for_spec(embedder_spec)
    else:
        query_model_id = embedder_model_id(embedder)

    readiness = probe_semantic_readiness(
        conn, session, scoped_subtypes=scoped_subtypes,
    )
    assert_rankable(
        readiness,
        query_model_id=query_model_id,
        allow_unverified_provenance=allow_unverified_provenance,
        scoped_subtypes=scoped_subtypes,
    )

    # A model narrower than the column is ranked through its HNSW index
    # (PACT openrouter-embeddings-hnsw), never by scanning every vector. The
    # index holds only this model's rows, so the unverified-provenance opt-in
    # (which admits rows with NO recorded model) cannot be honored on it.
    from okg.substrate.semantic_ann import (
        ann_rank,
        ann_spec_for_model_id,
        require_ann_index,
    )

    ann_spec = ann_spec_for_model_id(query_model_id)
    if ann_spec is not None and allow_unverified_provenance:
        err = SubstrateError(
            "search(semantic): the unverified-provenance opt-in cannot be "
            f"used with {query_model_id!r}, whose ranking reads an index that "
            "holds only rows recorded under that model",
            data={"query_model_id": query_model_id},
            fix_hint=(
                "re-embed the rows with no recorded model, then search "
                "without the opt-in"
            ),
        )
        err.code = "semantic_unverified_provenance_unindexed"
        raise err
    if ann_spec is not None:
        # Refuse a missing index before the query is embedded: with a paid
        # backend that embedding is a paid call.
        require_ann_index(conn, ann_spec)

    if embedder is None:
        from okg.substrate.embeddings.config import (
            EmbeddingConfig,
            build_embedding_execution_plan,
        )

        # No probe call for a paid backend: the query's own embedding below
        # is this search's only paid call.
        embedder = build_embedding_execution_plan(
            EmbeddingConfig(enabled=True, backend=embedder_spec),
            paid_backend="no_probe",
        )

    # The corpus width, not the embedder against itself. The guard this
    # replaced compared the embedder's output length to the embedder's OWN
    # declared dimension -- a tautology over one object that cannot fail for
    # any correctly implemented embedder and that said nothing about whether
    # the vector could be compared to the stored ones.
    assert_embedder_matches_substrate_width(embedder)
    qvec = embedder.embed(query)
    if len(qvec) != embedder.dimension:
        raise SubstrateError(
            f"embedder {type(embedder).__name__} returned vector of "
            f"length {len(qvec)}; expected {embedder.dimension}",
            data={"dim_got": len(qvec), "dim_expected": embedder.dimension},
        )
    # pgvector requires the literal vector syntax `[a,b,c]`; bind
    # as text and cast inside the checked-out Chronos snapshot.
    qstr = "[" + ",".join(f"{v:.6f}" for v in qvec) + "]"
    # Routed: pgvector cosine distance lowers a TransformProgram
    # (akmon.search.semantic) over the nodes_live registered surface; the
    # (subtype/attr) filter and final truncation stay client-side.
    # The scope goes INTO the program, not over its output. The SQL LIMIT here
    # is offset+limit+1, so narrowing subtypes client-side would under-fill or
    # empty a scoped page while in-scope matches sat deeper in the ranking --
    # the same eviction the lexical scope predicate exists to prevent (#797).
    scoped = scope is not None and scope.subtypes is not None
    family_scoped = scope is not None and scope.source_ids is not None
    extra_params: dict[str, Any] = {"qvec": qstr}
    if scoped:
        assert scope is not None and scope.subtypes is not None
        extra_params["scoped_subtypes"] = list(scope.subtypes)
    if family_scoped:
        # Into the program for the same reason the subtypes are: narrowing
        # families client-side could under-fill or empty the visible page while
        # in-family matches sat deeper in the ranking.
        assert scope is not None and scope.source_ids is not None
        extra_params["scoped_source_ids"] = list(scope.source_ids)
    # Pagination is over the VISIBLE ordered result set, not over the raw
    # ranking (issue #1438). Read-time filtering -- Nomos hiding, the
    # effective-attrs overlay, the residual scope/attr predicate -- drops rows
    # anywhere in the ranking, INCLUDING before the first visible row. The
    # fixed `offset + limit + 1` raw window this replaced then applied
    # `offset` a SECOND time, to the survivors: page 1 under-filled while the
    # cursor still advanced by `limit`, so rows between the two were served by
    # no page at all (with two hidden rows ahead of the first visible one and
    # limit=1, every visible row was unreachable). Grow the raw window instead
    # until the visible set is deep enough to answer this page, or until the
    # ranking runs out and the visible set is therefore COMPLETE.
    #
    # This is the same defect #627 fixed for `filter`/`map`; its regression
    # test walked pages only on those operators, and gave semantic search a
    # single-page `truncated` assertion at offset=0 -- which the double-shift
    # cannot fail. The page walks below bind the cursor itself.
    need = offset + limit + 1
    fetch = min(need, _SEMANTIC_RAW_FETCH_CAP)
    text_redaction = redaction_possible(conn, session)
    full_rows: list[dict[str, Any]] = []
    ranking_exhausted = False
    ann_capped = False
    while True:
        if ann_spec is not None:
            ranking = ann_rank(
                conn, session, ann_spec, qvec,
                limit=fetch,
                scoped_subtypes=(
                    list(scope.subtypes)
                    if scoped and scope is not None and scope.subtypes is not None
                    else None
                ),
                scoped_source_ids=(
                    list(scope.source_ids)
                    if family_scoped and scope is not None
                    and scope.source_ids is not None
                    else None
                ),
            )
            raw = ranking.rows
            # The index scan reports exhaustion itself: a short window is the
            # whole ranking only when no admitted row was left unscanned.
            ranking_exhausted = ranking.exhausted
            ann_capped = ranking.capped
        else:
            raw = lower_program_over_chronos(
                semantic_search_program(
                    limit=fetch,
                    scoped_subtypes=scoped,
                    scoped_source_ids=family_scoped,
                ),
                conn,
                generation_id=session.generation_id,
                branch=session.branch_name,
                extra_params=extra_params,
            )
            # Fewer rows than asked for means the ranking itself ended inside
            # this window, so the visible set built from it is the WHOLE
            # visible set -- not a prefix of it.
            ranking_exhausted = len(raw) < fetch
        full_rows = _semantic_visible_rows(
            conn, session, raw, filter=filter, scope=scope,
            redaction=text_redaction,
        )
        if (
            len(full_rows) >= need
            or ranking_exhausted
            # A wider window cannot help a scan that already stopped at its
            # tuple bound; the result is reported truncated below.
            or ann_capped
            or fetch >= _SEMANTIC_RAW_FETCH_CAP
        ):
            break
        fetch = min(
            fetch * _SEMANTIC_RAW_FETCH_GROWTH, _SEMANTIC_RAW_FETCH_CAP
        )

    # The window bound before the visible set could be verified whole.
    rank_capped = (
        not ranking_exhausted and len(full_rows) < need
    ) or ann_capped
    payload = _semantic_page(
        conn, session, query, full_rows,
        ranking_exhausted=ranking_exhausted, fetch=fetch,
        limit=limit, offset=offset, select=select, scope=scope,
        filter=filter, max_chars=max_chars, embedder=embedder,
        embedder_spec=embedder_spec, query_model_id=query_model_id,
        embedder_source=embedder_source, rank_capped=rank_capped,
        passage_chars=passage_chars,
    )
    payload["vector_source"] = "node_columns"
    # Coverage rides on EVERY page, not only empty ones: a thin page over a
    # barely-embedded corpus is the more common and more misleading case.
    # Under enforced access rules the counts would include rows this reader
    # cannot read, so they and the capped flag are withheld -- the set's
    # own vector count included.
    withheld = passage_counts_withheld(session)
    if route is not None:
        # A search set is configured but not registered, inactive, or holds
        # no vector at this pin: the old columns were ranked, and the page
        # says why.
        payload["vector_set"] = route.describe(withhold_counts=withheld)
    if withheld:
        payload["rank_capped"] = None
        payload["counts_withheld"] = "nomos_enforce"
    else:
        payload.update(readiness.envelope_fields())
    if ann_capped and not withheld:
        # The HNSW scan stopped at its tuple bound with admitted rows still
        # unranked: name the bound, so `rank_capped` is not a mystery.
        from okg.substrate.semantic_ann import MAX_SCAN_TUPLES

        payload["rank_capped_by"] = {
            "bound": "hnsw.max_scan_tuples",
            "value": MAX_SCAN_TUPLES,
        }
    if readiness.unknown_provenance_rows and allow_unverified_provenance:
        # Admitted under explicit opt-in -- reported, never blended silently
        # with verified rows.
        payload["unverified_provenance_admitted"] = (
            readiness.unknown_provenance_rows
        )
    return payload


_NO_RANK_CAPPED = object()


def _semantic_page(
    conn: psycopg.Connection,
    session: Session,
    query: str,
    full_rows: list[dict[str, Any]],
    *,
    ranking_exhausted: bool,
    fetch: int,
    limit: int,
    offset: int,
    select: Optional[list[str]],
    scope: Optional[ResolvedScope],
    filter: dict[str, Any],
    max_chars: int,
    embedder: Any,
    embedder_spec: Optional[str],
    query_model_id: Optional[str],
    embedder_source: Optional[str],
    rank_capped: Any = _NO_RANK_CAPPED,
    walk_stopped: bool = False,
    passage_chars: Optional[int] = None,
) -> dict[str, Any]:
    """The semantic page over the visible ranked rows, shared by the
    node-column and the vector-set rankings: the page cut, parents, fenced
    rows and the common envelope. ``rank_capped`` is carried only by the
    node-column page. ``passage_chars`` sizes each row's passage (PACT
    read-passage-rows-3666, D2), as on `_search_semantic`.

    ``walk_stopped`` (vector sets): the index walk ended at its tuple bound,
    so no later page can reach rows this call did not; the page is served
    as it is (the caller says why on ``listing``) and pages on only while
    rows found here remain."""
    pin = session.generation_id
    total_count = len(full_rows)
    # `truncated` is a claim about rows this call did NOT return, so it is
    # true whenever more visible rows were found beyond the page AND whenever
    # the ranking was not exhausted: rows past the fetched window may be
    # visible, and this call never verified that they are not.
    truncated = total_count > offset + limit or not ranking_exhausted
    total_count_basis = "exact" if ranking_exhausted else "lower_bound"
    more_found = total_count > offset + limit
    full_rows = full_rows[offset: offset + limit]
    if truncated and not full_rows and not walk_stopped:
        raise SubstrateError(
            "search(semantic): the bounded rank window could not reach "
            f"offset {offset} through read-time filtering, so there is no "
            "cursor that makes progress",
            data={
                "offset": offset,
                "limit": limit,
                "visible_rows_found": total_count,
                "raw_rows_fetched": fetch,
                "raw_fetch_cap": _SEMANTIC_RAW_FETCH_CAP,
            },
            fix_hint=(
                "narrow the query or filter so the visible matches sit "
                "earlier in the ranking, or page from a smaller offset"
            ),
        )

    # A semantically-retrieved passage needs its citable parent exactly as a
    # lexically-retrieved one does, so the same pinned resolution runs here.
    # The segment resolver is the SEMANTIC one: a missing BM25 index must not
    # deny a working semantic read (the argument `resolve_node_segment` makes).
    # Resolved only for a NON-EMPTY page. `branch_point` is not otherwise on
    # the semantic path, so calling it eagerly would let a segment-registry
    # problem turn an empty semantic page into a refusal it never used to be.
    if full_rows:
        from okg.substrate.mcp.semantic_readiness import resolve_node_segment

        parent_rows, parent_block = resolve_passage_parents(
            conn,
            session=session,
            page=full_rows,
            node_segment=resolve_node_segment(conn),
            branch_point_value=branch_point(conn, session),
            citation_from_attrs=_citation_from_attrs,
            withhold_passage_counts=passage_counts_withheld(session),
        )
        for row in full_rows:
            row["parent"] = parent_rows.get(str(row["node_id"]))
            row["position"] = passage_position(row.get("attrs"))
    else:
        _parent_rows, parent_block = resolve_passage_parents(
            conn,
            session=session,
            page=(),
            node_segment="",
            branch_point_value=None,
            citation_from_attrs=_citation_from_attrs,
            withhold_passage_counts=passage_counts_withheld(session),
        )
    if full_rows:
        # PACT read-passage-rows-3666: a semantic hit holds no lexical match,
        # so its passage is centred on the query's own words when the row's
        # text has them (whole-word matching, no database call) and is the
        # text's start otherwise.
        semantic_meta = _metadata_for_rows(
            conn, session, full_rows,
            metadata=getattr(filter.get("where_context"), "metadata", None),
        )
        semantic_matcher = fallback_matcher(query)
        for row in full_rows:
            row[PASSAGE_KEY] = row_passage(
                row, semantic_meta.get(str(row.get("subtype"))) or {},
                matcher=semantic_matcher, chars=passage_chars, anchor_required=False,
            )
    results = _fence_search_rows(
        conn, session, full_rows, select=select,
        default=_SEMANTIC_FIELDS, always=_SEMANTIC_ALWAYS,
        scope=scope, max_chars=max_chars,
        metadata=(semantic_meta if full_rows else None)
        or getattr(filter.get("where_context"), "metadata", None),
    )
    payload = {
        "query": query,
        "method": "semantic",
        "generation_id": pin,
        "pinned_generation_id": pin,
        "embedder_dimension": embedder.dimension,
        # WHICH embedder ran, and how it was chosen. "semantic search ran" and
        # "semantic search ran against the embedder you meant" are different
        # claims, and only the second is useful (issue #797). These keys are
        # semantic-only, matching `embedder_dimension` above; a lexical or
        # alias envelope carrying them would make their absence meaningless.
        "embedder_spec": embedder_spec,
        "embedder_model_id": query_model_id,
        "embedder_source": embedder_source,
        "embedding_plan_fingerprint": getattr(embedder, "fingerprint", None),
        "limit": limit,
        "offset": offset,
        # Advance by rows actually RETURNED, never by `limit`. Read-time
        # filtering can under-fill a page at the rank cap, and a cursor that
        # assumed `limit` would step straight over the rows it dropped.
        "next_offset": (
            (offset + len(full_rows))
            if truncated and (more_found or not walk_stopped) else None
        ),
        "results": results,
        "total_count": total_count,
        # What `total_count` is a count OF: every visible match when the
        # ranking was exhausted, otherwise a floor. Naming the basis is the
        # difference between a number and a guess.
        "total_count_basis": total_count_basis,
        "selection_basis": "retrieved_candidates",
        "truncated": truncated,
    }
    if rank_capped is not _NO_RANK_CAPPED:
        # The rank window bound before the visible set was verified whole, so
        # `total_count` is a floor and `truncated` is conservative.
        payload["rank_capped"] = rank_capped
    payload["parent_relation"] = parent_block
    return payload


def _semantic_visible_rows(
    conn: psycopg.Connection,
    session: Session,
    raw: list[dict[str, Any]],
    *,
    filter: dict[str, Any],
    scope: Optional[ResolvedScope],
    redaction: bool,
    set_model_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    """The ranked rows the reader may be served, in ranking order.

    ``set_model_id``: the rows were ranked by a vector set, whose vectors
    are all of that registered model (its identity), so each row's
    provenance is that model, never ``unknown``.
    """
    # The ranking is over embeddings of the STORED text: a row counts
    # only when the reader reads that text as stored (#3377).
    rows = keep_readable_text(
        raw, filter_node_rows_for_session(conn, session, raw),
        redaction=redaction,
    )
    rows = apply_effective_attrs_to_rows(
        conn,
        rows,
        target_kind="node",
        target_id_field="node_id",
        generation_id=session.generation_id,
    )
    return [
        {
            "node_id": row["node_id"],
            "subtype": row["subtype"],
            "attrs": row["attrs"],
            **({"text_hash": row["text_hash"]} if "text_hash" in row else {}),
            "distance": float(row["distance"]),
            "source": row.get("source"),
            "source_record_id": record_identity(
                row.get("source_record_id")
            ),
            "source_revision": record_identity(
                row.get("source_revision")
            ),
            # Per-row provenance. A row whose `embedding_model` is NULL
            # only reaches this point under the explicit opt-in, and it is
            # labelled `unknown` rather than blended with the verified
            # rows -- a caller who permitted unverified provenance must
            # still be able to tell WHICH rows it applied to.
            "embedding_provenance": (
                set_model_id or row.get("embedding_model") or UNKNOWN_PROVENANCE
            ),
        }
        for row in rows
        # The scope already ran in SQL; this residual is defence in depth,
        # and keeps the guarantee true if the program is ever lowered
        # differently.
        if _matches_filter(row, filter, scope=scope)
    ]


@dataclass(frozen=True)
class _VectorSetRoute:
    """The search set as configured and as registered in this database."""

    configured: Any  # VectorSetConfig from deployment.yaml (or the spec)
    stored: Any  # VectorSetConfig as registered, or None
    #: "active" (the set serves the read: the pin's watermark carries a
    #: completed fill), "filling" (vectors at the pin, but no backfill of
    #: the set has completed by then), "no_vectors" (registered and active,
    #: no vector at the pin), "inactive" or "unregistered". The page shows
    #: "unfilled" for the first two under enforced access rules.
    status: str
    drift: tuple[str, ...]
    #: Vectors of the set at the pin, for "filling" and "no_vectors".
    vectors_at_pin: Optional[int] = None

    def describe(self, *, withhold_counts: bool) -> dict[str, Any]:
        """The page's ``vector_set`` field. With ``withhold_counts`` (enforced
        access rules) it carries no count: the set's vectors at the pin
        cover documents of nodes this reader may not read. For the same
        reason ``filling`` and ``no_vectors`` (which differ only in whether
        the set holds any vector at the pin) both read ``unfilled`` then."""
        status = self.status
        if withhold_counts and status in ("no_vectors", "filling"):
            status = "unfilled"
        out: dict[str, Any] = {
            "name": self.configured.name,
            "set_id": self.configured.set_id,
            "status": status,
        }
        if self.status in ("active", "no_vectors", "filling"):
            out["config_drift"] = list(self.drift)
        if self.status in ("no_vectors", "filling"):
            vectors = self.vectors_at_pin
            if not withhold_counts:
                out["coverage"] = {
                    # Counted up to count_bound; None means more than that.
                    "vectors_at_pin": (
                        vectors
                        if vectors is None or vectors <= _FILLING_COUNT_BOUND
                        else None
                    ),
                    "count_bound": _FILLING_COUNT_BOUND,
                }
            out["reason"] = (
                "no backfill of the set had completed by this generation; "
                "the node columns were ranked (complete `okg db "
                "embed-backfill`, then publish)"
            )
        return out


def _vector_set_route(
    conn: psycopg.Connection,
    *,
    embedder: Any,
    embedder_spec: Optional[str],
    generation_id: Optional[str] = None,
) -> Optional[_VectorSetRoute]:
    """The search set for this call, or None when it has none.

    The set comes from the embedder's verified plan (the deployment's
    ``embeddings`` block, ``search_set`` or ``main``) or, for an
    ``embedder_spec``, the default set of that backend. An embedder that is
    not a plan (a test stub) has no set and keeps the node-column ranking.
    """
    from okg.substrate.embeddings.config import EmbeddingConfig

    if embedder is not None:
        config = getattr(embedder, "config", None)
        if not isinstance(config, EmbeddingConfig):
            return None
    elif embedder_spec is not None:
        config = EmbeddingConfig(enabled=True, backend=embedder_spec)
    else:
        return None
    return vector_set_route_for_config(conn, config, generation_id=generation_id)


#: Parameters: count limit, generation id, set id. The count stops one past
#: _FILLING_COUNT_BOUND: a page on the node columns pays at most that many
#: index entries for it. A filled pin never uses the count, so it is not run
#: there (a bitmap scan of the set's seq index cannot stop at the LIMIT).
_SET_FILL_AT_PIN_SQL = (
    "SELECT w.filled_vector_seq IS NOT NULL,"
    "       CASE WHEN w.filled_vector_seq IS NULL THEN"
    "       (SELECT count(*) FROM (SELECT 1 FROM okg.text_embeddings AS e"
    "         WHERE e.set_id = w.set_id"
    "           AND e.seq <= COALESCE(w.max_vector_seq, w.max_seq)"
    "         LIMIT %s) AS s) END"
    "  FROM okg.embedding_watermarks AS w "
    " WHERE w.generation_id = %s AND w.set_id = %s"
)


def _set_fill_at_pin(
    conn: psycopg.Connection, set_id: str, generation_id: str,
) -> Optional[tuple[bool, int]]:
    """``(filled, vectors)`` of the set at the pin: whether the pin's
    watermark carries a completed fill (``filled_vector_seq``), and how many
    of the set's vectors are at or below it (counted only when not filled;
    0 when filled). None when the pin has no
    watermark for the set (the set read refuses that, it never falls
    back)."""
    from psycopg.rows import tuple_row

    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            _SET_FILL_AT_PIN_SQL,
            (_FILLING_COUNT_BOUND + 1, str(generation_id), set_id),
        )
        row = cur.fetchone()
    if row is None:
        return None
    filled = bool(row[0])
    # Filled: the count was not run (NULL) and is never read.
    return (filled, 0 if filled else int(row[1]))


#: Vectors counted for a "filling" page's coverage; above it the page says
#: ``vectors_at_pin: null`` with this bound.
_FILLING_COUNT_BOUND = 10_000


def vector_set_route_for_config(
    conn: psycopg.Connection, config: Any, *, generation_id: Optional[str] = None,
) -> Optional[_VectorSetRoute]:
    """The search set of an ``EmbeddingConfig`` and its registration state,
    or None when the config has no set. Builds no embedder, so a caller can
    decide on the route before paying for a backend (``okg search``).

    With ``generation_id``, a registered active set that holds no vector at
    that pin is ``no_vectors``: the read keeps the node columns, so an
    ``okg migrate`` that registers a set before any backfill does not turn a
    working semantic search into empty pages.
    """
    from okg.substrate.embeddings.store import config_drift, load_set

    configured = config.search_vector_set
    if configured is None:
        return None
    loaded = load_set(conn, configured.set_id)
    if loaded is None:
        return _VectorSetRoute(configured, None, "unregistered", ())
    stored, active = loaded
    if not active:
        return _VectorSetRoute(configured, stored, "inactive", ())
    drift = tuple(config_drift(configured, stored))
    if generation_id is not None:
        state = _set_fill_at_pin(conn, stored.set_id, generation_id)
        if state is not None and not state[0]:
            # No completed fill at this pin (Jason, "ehroute yes"): a
            # partial backfill must not flip search to a thin set.
            return _VectorSetRoute(
                configured, stored, "filling" if state[1] else "no_vectors",
                drift, vectors_at_pin=state[1],
            )
    return _VectorSetRoute(configured, stored, "active", drift)


def _reader_predicate_sql(conn: psycopg.Connection, session: Session) -> Optional[str]:
    """The reader's access rules as SQL over the node row ``n`` (rows the
    read filter could serve, with ``text`` not known to be masked), or None
    when the deployment runs no access rules. A superset: the per-row read
    filter still runs on every ranked row."""
    from psycopg import sql as pg_sql

    from okg.substrate.nomos import reader_view
    from okg.substrate.nomos.runtime_enforcement import (
        deployment_requires_nomos_runtime,
    )

    if not deployment_requires_nomos_runtime(session.deployment_name):
        return None
    view = reader_view.open(
        conn, session, row_kind="node", action=None, surface=None,
        nomos_runtime=True,
    )
    predicate = pg_sql.SQL("({}) AND (({}) IS NOT FALSE)").format(
        view.predicate("n"), view.readable("n", "text"),
    )
    return predicate.as_string(conn).replace("%", "%%")


def _search_semantic_vector_set(
    conn: psycopg.Connection,
    session: Session,
    query: str,
    *,
    route: _VectorSetRoute,
    embedder: Any,
    embedder_spec: Optional[str],
    embedder_source: Optional[str],
    filter: dict[str, Any],
    limit: int,
    offset: int,
    select: Optional[list[str]],
    scope: Optional[ResolvedScope],
    scoped_subtypes: Optional[list[str]],
    allow_unverified_provenance: bool,
    max_chars: int,
    passage_chars: Optional[int] = None,
) -> dict[str, Any]:
    """Semantic search over the registered search vector set (design v3.2).

    Every refusal that needs no query vector comes first (watermark, index,
    pgvector version, scope size), so a refused search makes no paid call.
    The scope (subtypes, sources, the reader's access rules, the row-content
    check) is applied before ranking: see
    :func:`okg.substrate.semantic_ann.rank_vector_set`.
    """
    from okg.substrate.embeddings.identity import embedder_model_id
    from okg.substrate.semantic_ann import (
        SemanticQueryEmbeddingFailed,
        prepare_set_read,
        query_vector_for_set,
        rank_vector_set,
    )

    stored = route.stored
    if allow_unverified_provenance:
        err = SubstrateError(
            "search(semantic): the unverified-provenance opt-in applies to "
            "the old node columns; vector set "
            f"{stored.name!r} holds only vectors written for its own documents",
            data={"set": stored.name, "set_id": stored.set_id},
            fix_hint="search without the opt-in",
        )
        err.code = "semantic_unverified_provenance_unindexed"
        raise err
    # The profile's source families go into the scope before ranking, like
    # subtypes (prefilter). An explicit `filter.source` never reaches here:
    # search() refuses it for method="semantic" (InvalidOperatorArguments).
    family_scoped = scope is not None and scope.source_ids is not None
    read = prepare_set_read(
        conn, session, stored,
        scoped_subtypes=scoped_subtypes,
        scoped_source_ids=(
            list(scope.source_ids)
            if family_scoped and scope is not None and scope.source_ids is not None
            else None
        ),
        reader_predicate=_reader_predicate_sql(conn, session),
    )

    if embedder is None:
        from okg.substrate.embeddings.config import build_vector_set_plan

        # No probe call for a paid backend: the query's own embedding below
        # is this search's only paid call. The set's own backend, at the
        # set's width (PACT vector-set-pgvector-widths).
        embedder = build_vector_set_plan(route.configured, paid_backend="no_probe")
    query_model_id = embedder_model_id(embedder)
    if query_model_id != stored.model_id:
        err = SubstrateError(
            f"search(semantic): the query embedder is {query_model_id!r}, but "
            f"vector set {stored.name!r} holds {stored.model_id!r} vectors",
            data={"query_model_id": query_model_id, "set_model_id": stored.model_id},
            fix_hint="search with the set's own backend",
        )
        err.code = "embedding_model_skew"
        raise err
    need = offset + limit + 1
    fetch = min(need, _SEMANTIC_RAW_FETCH_CAP)
    walk_stopped = False
    full_rows: list[dict[str, Any]] = []
    ranking_exhausted = True
    if read.embedded_nodes == 0:
        # Nothing in scope has a vector at this pin: the page is empty
        # whatever the query says, so the query is not embedded (with a paid
        # backend that would be a paid call for nothing).
        pass
    else:
        try:
            raw_vector = embedder.embed(stored.query_prefix + query)
        except SubstrateError as exc:
            if getattr(exc, "code", None) != "embedder_request_failed":
                raise
            raise SemanticQueryEmbeddingFailed(
                "search(semantic): the query could not be embedded: "
                f"{exc}",
                data={"set": stored.name, "model_id": stored.model_id,
                      **dict(getattr(exc, "data", None) or {})},
                fix_hint=(
                    "retry shortly; or search with method='lexical' or "
                    "method='grep', which need no embedding"
                ),
            ) from exc
        qvec = query_vector_for_set(stored, raw_vector)

        text_redaction = redaction_possible(conn, session)
        while True:
            ranking = rank_vector_set(conn, read, qvec, limit=fetch)
            ranking_exhausted = ranking.exhausted
            walk_stopped = ranking.capped
            full_rows = _semantic_visible_rows(
                conn, session, ranking.rows, filter=filter, scope=scope,
                redaction=text_redaction,
                set_model_id=stored.model_id,
            )
            if (
                len(full_rows) >= need
                or ranking_exhausted
                # A wider window cannot help a walk that already stopped at
                # its tuple bound.
                or walk_stopped
                or fetch >= _SEMANTIC_RAW_FETCH_CAP
            ):
                break
            fetch = min(fetch * _SEMANTIC_RAW_FETCH_GROWTH, _SEMANTIC_RAW_FETCH_CAP)

    payload = _semantic_page(
        conn, session, query, full_rows,
        ranking_exhausted=ranking_exhausted, fetch=fetch,
        limit=limit, offset=offset, select=select, scope=scope,
        filter=filter, max_chars=max_chars, embedder=embedder,
        embedder_spec=embedder_spec, query_model_id=query_model_id,
        embedder_source=embedder_source, walk_stopped=walk_stopped,
        passage_chars=passage_chars,
    )
    payload["vector_source"] = "vector_set"
    index = read.index
    withheld = passage_counts_withheld(session)
    payload["vector_set"] = {
        **route.describe(withhold_counts=withheld),
        "index": {
            key: index[key]
            for key in (
                "kind", "status", "index_name", "pgvector_version", "rebuild",
            )
            if key in index
        },
    }
    payload["ranking"] = read.ranking
    if walk_stopped:
        # The index walk stopped at the set's max_scan_tuples before the page
        # was full: nearer eligible nodes may exist past it.
        payload["listing"] = {"complete": False, "reason": LISTING_SCAN_BOUND}
    if withheld:
        # Under enforced access rules a count could include nodes this
        # reader cannot read.
        payload["counts_withheld"] = "nomos_enforce"
    else:
        payload["embedding_coverage"] = {
            # Counted up to `count_bound`; None means more than that.
            "text_nodes": read.text_nodes,
            "embedded_nodes": read.embedded_nodes,
            "count_bound": read.count_bound,
        }
    return payload


def _search_alias(
    conn: psycopg.Connection,
    session: Session,
    query: str,
    *,
    filter: dict[str, Any],
    limit: int,
    offset: int,
    fuzzy: bool = False,
    select: Optional[list[str]] = None,
    scope: Optional[ResolvedScope] = None,
    max_chars: int = DEFAULT_PREVIEW_CHARS,
) -> dict[str, Any]:
    """Alias resolution at the pinned catalog_version.

    Default behavior: exact match against `okg.aliases`. Alias scope is
    matched on the filter's `subtype` (if provided) else NULL
    (global). Scoped matches sort before global on ties. A resolved
    search profile admits aliases scoped to any subtype it covers, on
    the same terms, and confines hydrated canonicals to that set.

    `fuzzy=True`: when the exact-match tier returns nothing,
    fall through to the substrate's `okg.fuzzy_resolve_alias`
    cascade (trigram → Levenshtein) at the same catalog version.
    Each fuzzy hit is hydrated as a canonical node row, with
    `match_kind` set to `'trigram'` or `'levenshtein'`.
    """
    validate_select(
        select, _ALIAS_SELECTABLE, tool_name="search(method=alias)",
        elsewhere=search_select_elsewhere("alias"),
    )
    cv = session.catalog_version_id
    # Candidates are capped ONCE, before hydration, and the page is taken from
    # the survivors (issue #1438). The cap this replaced was `offset + limit +
    # 1` -- a raw window sized for one page -- while `truncated` was computed
    # only from rows that survived hydration and filtering. A single filtered
    # candidate inside that window produced `truncated=False` and
    # `next_offset=null` while later visible aliases sat just past the cap: a
    # completion claim about rows the call never fetched.
    params: dict[str, Any] = {
        "query": query, "cv": cv, "limit": _ALIAS_CANDIDATE_CAP + 1,
    }
    alias_scopes = list(scope.subtypes) if (scope and scope.subtypes) else []
    if "subtype" in filter and filter["subtype"] not in alias_scopes:
        alias_scopes.append(filter["subtype"])
    if alias_scopes:
        scope_clause = "AND (scope = ANY(%(alias_scopes)s) OR scope IS NULL)"
        params["alias_scopes"] = alias_scopes
    else:
        scope_clause = "AND scope IS NULL"
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT alias, canonical, scope
              FROM okg.aliases
             WHERE alias = %(query)s
               AND catalog_version_id = %(cv)s
               {scope_clause}
             ORDER BY scope IS NULL,  -- False (scoped) first
                      canonical        -- total order: OFFSET needs one
             LIMIT %(limit)s
            """,
            params,
        )
        exact_rows = list(cur.fetchall())
        candidates_capped = len(exact_rows) > _ALIAS_CANDIDATE_CAP
        # Rows carry a rank as a fourth element so the candidate list has a
        # TOTAL order before it is paged. `okg.aliases` is keyed by (alias,
        # catalog_version_id), so the exact tier yields at most one row and
        # every rank there is the same; the fuzzy tier below ranks by
        # similarity, where ties are real and OFFSET needs a tiebreak.
        alias_rows = [
            (row[0], row[1], row[2], 0.0)
            for row in exact_rows[:_ALIAS_CANDIDATE_CAP]
        ]
        # Track the cascade tier each row resolved through.
        match_kinds: dict[str, str] = {row[1]: "exact" for row in alias_rows}

        # Fuzzy fallback: only when explicitly opted-in AND
        # exact returned zero. Defers to the substrate cascade
        # SQL function so trigram + Levenshtein run with their
        # GIN-indexed paths.
        if fuzzy and not alias_rows:
            cur.execute(
                """
                SELECT canonical, similarity, method
                  FROM okg.fuzzy_resolve_alias(%(query)s, %(cv)s,
                                               0.6::real, %(limit)s::int)
                """,
                {
                    "query": query,
                    "cv": cv,
                    "limit": _ALIAS_CANDIDATE_CAP + 1,
                },
            )
            fuzzy_rows = list(cur.fetchall())
            candidates_capped = len(fuzzy_rows) > _ALIAS_CANDIDATE_CAP
            for canonical, similarity, method in fuzzy_rows[
                :_ALIAS_CANDIDATE_CAP
            ]:
                # Synthesize a row in the same shape as the exact
                # match — alias + canonical + scope — plus the rank.
                alias_rows.append(
                    (query, canonical, None, -float(similarity or 0.0))
                )
                match_kinds[canonical] = method

    # A total order over the candidates, so the same OFFSET names the same
    # row on every call (issue #1438). The cascade's own `ORDER BY similarity
    # DESC` leaves ties unordered, and an unordered tie under OFFSET repeats
    # or skips rows exactly as an unordered aggregate does.
    alias_rows.sort(key=lambda row: (row[3], str(row[1]), str(row[0])))

    # Hydrate canonical nodes from the pinned Chronos graph so alias results
    # are generation-stable just like lexical and semantic search.
    canonicals = [r[1] for r in alias_rows]
    node_rows: list[dict[str, Any]] = []
    if canonicals:
        graph = _chronos_checkout(conn, session)
        raw_node_rows = graph.query(
            """
            SELECT node_id, subtype, attrs, text_hash, source
              FROM okg.graph_nodes
             WHERE node_id = ANY(:ids)
             ORDER BY node_id
            """,
            {"ids": canonicals},
        )
        visible_node_rows = filter_node_rows_for_session(
            conn, session, raw_node_rows,
        )
        effective_node_rows = apply_effective_attrs_to_rows(
            conn,
            visible_node_rows,
            target_kind="node",
            target_id_field="node_id",
            generation_id=session.generation_id,
        )
        for row in effective_node_rows:
            # The profile confines the CANONICAL too, not only which aliases
            # were admitted: an alias with a NULL (global) scope must not be a
            # side door to a node whose subtype the profile excludes.
            if _matches_filter(row, filter, scope=scope):
                node_rows.append(row)
    node_metadata = _metadata_for_rows(
        conn, session, node_rows,
        metadata=getattr(filter.get("where_context"), "metadata", None),
    )
    node_rows = classify_evidence_rows(conn, session, node_rows, metadata=node_metadata)
    effective_by_id = {row["node_id"]: row for row in node_rows}
    serialized_node_rows = serialize_node_read_rows(
        conn,
        [
            {
                "node_id": row["node_id"],
                "subtype": row["subtype"],
                "attrs": row["attrs"],
                **({"text_hash": row["text_hash"]} if "text_hash" in row else {}),
                "excerpt": _bounded_excerpt(
                    str((row.get("attrs") or {}).get("text", "")), query,
                    max_chars=max_chars,
                ),
                "source": row.get("source"),
                "evidence_kind": row.get("evidence_kind"),
                "evidence_kind_provenance": row.get(
                    "evidence_kind_provenance"
                ),
                "chunk_kind": row.get("chunk_kind"),
            }
            for row in node_rows
        ],
        deployment_name=session.deployment_name,
    )
    # Fencing wraps the excerpt in producer markers, which can push it past the
    # budget. Cap after fencing, as the lexical path does, so the bound holds on
    # the `node` block too and not just on the alias row's own excerpt.
    for row in serialized_node_rows:
        excerpt = row.get("excerpt")
        if isinstance(excerpt, str):
            row["excerpt"] = _cap_serialized_excerpt(
                excerpt, max_chars=max_chars,
            )
        if scope is not None:
            row["source_family"] = scope.family_of(
                (row.get("provenance") or {}).get("source")
            )
    by_id = {r["node_id"]: r for r in serialized_node_rows}

    visible_rows = []
    # `alias_scope` is the row's own scope column; do not shadow the resolved
    # search profile in `scope`.
    for alias, canonical, alias_scope, _rank in alias_rows:
        nr = by_id.get(canonical)
        # A resolved profile narrows exactly as a filter does: a canonical that
        # did not survive the subtype check above is out, and must not come
        # back as a hydration-less row.
        if (filter or (scope and scope.subtypes is not None)) and nr is None:
            continue
        node_block = None
        if nr is not None:
            node_block = nr
        elif canonical not in by_id:
            continue
        alias_evidence = curated_alias_evidence(alias=alias)
        visible_row = {
            "alias": alias,
            "canonical": canonical,
            "canonical_node_id": canonical,
            # Projected away unless `select` names it (`_ALIAS_SELECT_ONLY`).
            "node_id": canonical,
            "subtype": nr.get("subtype") if nr is not None else None,
            "scope": alias_scope,
            "match_kind": match_kinds.get(canonical, "exact"),
            "method": "alias",
            "excerpt": _cap_serialized_excerpt(
                str((nr or {}).get("excerpt") or ""), max_chars=max_chars,
            ),
            # The alias card carries the same family label its canonical node
            # block does, so a family-filtered alias page names its authority
            # without a second call.
            "source_family": (nr or {}).get("source_family"),
            "node": node_block,
            **alias_evidence,
        }
        if nr is not None and nr.get("chunk_kind") is not None:
            visible_row["chunk_kind"] = nr["chunk_kind"]
        visible_rows.append(visible_row)
    # The page is a window on the VISIBLE rows, and `total_count` counts the
    # visible rows this call actually verified. Both are exact unless the
    # candidate cap bound, which the envelope reports rather than hides.
    total_count = len(visible_rows)
    truncated = total_count > offset + limit or candidates_capped
    full_rows = visible_rows[offset: offset + limit]
    if truncated and not full_rows:
        raise SubstrateError(
            "search(alias): the bounded candidate window could not reach "
            f"offset {offset}, so there is no cursor that makes progress",
            data={
                "offset": offset,
                "limit": limit,
                "visible_rows_found": total_count,
                "candidate_cap": _ALIAS_CANDIDATE_CAP,
            },
            fix_hint="page from a smaller offset for this alias query",
        )
    if select is None or HANDLE_KEY in select:
        # The outer alias identity is curated, but a handle comes from its
        # canonical node. Reuse that node's already-resolved trust and attrs;
        # neither a new hydration nor a policy/source lookup is needed.
        canonical_rows = _with_handles(
            conn, session,
            [effective_by_id[row["canonical_node_id"]] for row in full_rows],
            metadata=node_metadata,
        )
        enriched = []
        for row, canonical in zip(full_rows, canonical_rows):
            handle = canonical.get(HANDLE_KEY)
            if handle is not None:
                canonical_read = by_id[row["canonical_node_id"]]
                provenance = canonical_read.get("provenance") or {}
                fenced = serialize_node_read_row(
                    {HANDLE_KEY: handle, "source": canonical.get("source")},
                    trust=provenance.get("trust", "untrusted"),
                    marker_attrs=canonical.get("attrs"),
                )
                row = {**row, HANDLE_KEY: fenced[HANDLE_KEY]}
            enriched.append(row)
        full_rows = enriched
    results = [
        project_row(
            r, select, default=_ALIAS_FIELDS, always=_ALIAS_ALWAYS,
        )
        for r in full_rows
    ]

    payload = {
        "query": query,
        "method": "alias",
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        "catalog_version_id": cv,
        "limit": limit,
        "offset": offset,
        # Advance by rows actually RETURNED, never by `limit`.
        "next_offset": (offset + len(full_rows)) if truncated else None,
        "results": results,
        "total_count": total_count,
        "total_count_basis": (
            "lower_bound" if candidates_capped else "exact"
        ),
        "truncated": truncated,
        # The candidate window bound before every matching alias was read.
        "candidates_capped": candidates_capped,
        "retrieval_exhaustive": not candidates_capped,
    }

    # 2026-04-25 fix (debug-substrate-okg-self Phase 5):
    # if the result set is empty AND the alias table is itself
    # empty at the pinned cv, surface a structured `config`
    # block so the caller can tell "no match" from "no alias
    # table". Empty results from a populated catalog do NOT
    # carry the hint.
    if not results:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.aliases "
                " WHERE catalog_version_id = %(cv)s",
                {"cv": cv},
            )
            n_aliases = cur.fetchone()[0]
        if n_aliases == 0:
            payload["config"] = {
                "aliases_registered": 0,
                "fix_hint": (
                    f"no aliases registered at catalog_version "
                    f"{cv}; install via "
                    f"`okg catalog load --apply` after defining "
                    f"`aliases.json` in the deployment"
                ),
            }

    return payload
