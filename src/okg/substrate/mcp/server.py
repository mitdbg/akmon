"""Substrate MCP server (M4 completion, M4.5-hardened, session-pinned).

Thin FastMCP layer exposing the session-pinned tools in
`okg.substrate.mcp.tools` + `okg.substrate.mcp.search` +
`okg.substrate.mcp.changes` over MCP stdio.

Session model — one default pin per logical MCP transport session:

  Every graph tool takes an optional `generation_id: str | int | None`.
  A non-null value is authoritative for that call and never mutates the
  session default.  On the first omitted/null call over a stateful MCP
  transport, the server resolves latest without the TTL cache and binds that
  published generation to the SDK ServerSession object.  Later omitted calls
  reuse it across pooled database connections.  A deliberate
  `inspect(target={"kind":"session","action":"refresh"})` replaces only that
  default.  Stateless/contextless calls are visibly request-scoped and never
  borrow a process-global fake identity.

Operational hardening (M4.5, design review P1 #7):

  - `psycopg_pool.ConnectionPool` is the connection substrate.
    Every tool call borrows a conn from the pool for its
    duration and returns it; no per-call `psycopg.connect()`.
    Sized to `min_size=2, max_size=16` by default.
  - A short-TTL (500 ms) LRU caches request-scoped `(deployment, None) →
    (gen_id, cv_id, db_fingerprint)` so the latest-pin lookup
    doesn't hit the DB on every direct/contextless call under
    `auto_advance="never"`. Logical-session first bind and refresh bypass it.

Wired tools:
  - Default AKMON read surface: `inspect`, `search`, `expand`,
    `filter`, `map`, `aggregate`, `query`
  - Explicit write/admin launch path: `record`, `propose`,
    `list_sources`, `add_source`, `remove_source`, `source_status`,
    `discover_sources`, `propose_setup`, `commit_setup`,
    `connector_route_status`, `connector_ingest_batch`
  - Deployment extensions and generated program tools are cataloged
    for diagnostics, not registered on this surface.

The earlier fine-grained tools (`describe_graph`, `graph_stats`,
`get_ontology`, `get_node`, `list_neighbors`,
`aggregate_neighbors`, `aggregate_edge_routes`, `traverse_path`,
`find_identifier_mentions`, `run_sql`, `advance_session`,
`preview_advance`, `list_generations_since`, `changes_since`)
were retired by `shrink-mcp-to-operator-surface`; their behavior
is reachable via `inspect`, `expand`, `search`, and `query`.
"""

from __future__ import annotations

from dataclasses import replace
import contextvars
import copy
import hashlib
import json as _json
import inspect
import inspect as _py_inspect
import logging
import os
import re
import uuid
import signal
import threading
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from pathlib import Path
from typing import Annotated, Any, Callable, Literal, Mapping, Optional, Sequence, Union

import psycopg
from okg.substrate import fastyaml
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from okg.substrate.db.connection import connection_kwargs
from okg.substrate.db.pool import release_session_advisory_locks

try:
    from mcp.server.fastmcp import Context, FastMCP
except ImportError as e:  # pragma: no cover - checked at runtime
    raise ImportError(
        f"MCP SDK not available; install 'mcp' in the environment. Original error: {e}"
    ) from e

from okg.substrate.embeddings import Embedder, EmbeddingExecutionPlan
from okg.substrate.errors import MCPPoolUnavailable, SubstrateError
from okg.substrate.mcp.response_bounds import (
    ByteBoundedFastMCP,
    DateCallOverBudget,
    InvalidOperatorArguments,
    MaxBytes,
    adjusted_reserve_bytes,
    call_arguments,
    COUNT_EXACT_NOTE,
    forget_adjusted,
    note_adjusted,
    note_count_may_vary,
    offer_retry_call,
    current_date_call as _current_date_call,
)
from okg.substrate.mcp import attr_filter_source as _attr_filter_source
from okg.substrate.attributes import (
    ATTRIBUTE_EFFECTIVE_TABLE,
    RESERVED_EFFECTIVE_ATTR_KEYS,
    apply_effective_attrs_to_rows,
)
from pydantic import Field as PField

from okg.substrate.mcp.operator_models import (
    DIRECTIONS,
    MAX_AGGREGATE_BY,
    MAX_DEPTH,
    MAX_OFFSET,
    MAX_PAGE_LIMIT,
    MAX_PATH_HOPS,
    MAX_PREVIEW_CHARS,
    AggregateInput,
    AggregateMetric,
    ExpandInput,
    InspectTarget,
    Limit,
    LiveCall,
    Offset,
    PredicateInput,
    Projection,
    SearchFilter,
    SearchMethod,
    SearchScope,
    SetInput,
    as_mapping,
    compact_tool_schemas,
    declared,
    publish_clean_descriptions,
    refuse_substring_contains,
    refuse_unknown_search_filter_keys,
    search_filter_mapping,
)
from okg.substrate.mcp import search_where as _search_where
from okg.substrate.mcp import search_arguments as _search_arguments
import okg.substrate.mcp.result_references as _result_references
from okg.substrate.mcp import live_reader as _live_reader
from okg.substrate.mcp import live_resolver as _live_resolver
from okg.substrate.mcp import live_nodes as _live_nodes
from okg.substrate.mcp import live_result_sets as _live_results
from okg.substrate.mcp import run_steps as _run_steps
from okg.substrate.mcp import live_access_rules as _live_access_rules
from okg.substrate.mcp import continuation as _continuation
from okg.substrate.mcp.session import resolve_generation
from okg.substrate.mcp.record_window_search import (
    validate_window_call as _validate_window_call,
)
from okg.substrate.mcp.operators import COMPOSITION_TOOLS as _COMPOSITION_TOOLS
from okg.substrate.mcp.operators import OPERATOR_TOOLS, WRITE_ADMIN_TOOLS
from okg.substrate.mcp.operator_models import (
    publish_client_readable_schemas,
    publish_narrowed_inspect_target,
)
from okg.substrate.mcp.effective_surface import (
    assert_inspect_kind_served,
    assert_surface_files_agree,
    assert_write_launch_is_served,
    registered_tools_for_launch,
    served_inspect_kinds,
)
from okg.substrate.mcp.tool_profiles import (
    CANONICAL_TOOL_ORDER,
    InstructionSegment,
    InstructionSlot,
    InstructionSurfaceMismatch,
    assert_expansion_is_coherent,
    assert_narrowing_is_servable,
    dataset_card_enabled,
    parse_tool_profile_config,
    render_expansion_instruction,
    render_instructions,
    resolve_expansion_ceiling,
    resolve_tool_surface,
    tools_named_in,
    ToolSurfaceExpansionInvalid,
)
from okg.substrate.mcp.tool_prose import (
    example_subtype_for,
    publish_surface_descriptions,
)
from okg.substrate.mcp.session_tool_surface import (
    SessionToolSurface,
    ToolSurfaceExpansionUnavailable,
    install_session_tool_projection,
)
from okg.substrate.mcp.tracing import make_tracer
from okg.substrate.mcp.tool_registration import (
    apply_admitted_tool_surface,
    catalog_skipped_module_tool,
    register_conditional_admin_tools,
    register_core_read_tools,
    register_source_admin_tools,
    register_write_effect_tools,
)
from okg.substrate.mcp.record import (
    RecordPayloadError,
    lower_record_payload,
    record_prefixes_payload,
    record_templates_payload,
)
from okg.substrate.mcp.provenance import UNTRUSTED_DATA_CONTRACT
from okg.substrate.mcp.result_text import RESULT_TEXT_INSTRUCTION
from okg.substrate.mcp.response_shape import (
    compact_for_wire as _compact_for_wire,
    LINEAGE_SELECT_TOKEN,
    lineage_select_plan as _lineage_select_plan,
    move_lineage_to_rows as _move_lineage_to_rows,
)
from okg.substrate.mcp import (
    Session,
    aggregate_edge as _aggregate_edge,
    aggregate_node as _aggregate_node,
    expand_neighbors as _expand_neighbors,
    expand_path as _expand_path,
    find_identifier_mentions as _find_identifier_mentions,
    get_ontology as _get_ontology,
    inspect_attr_provenance as _inspect_attr_provenance,
    inspect_generation as _inspect_generation,
    inspect_graph as _inspect_graph,
    inspect_node as _inspect_node,
    inspect_subtype as _inspect_subtype,
    inspect_view as _inspect_view,
    open_session,
    parse_path_hops as _parse_path_hops,
    preview_advance as _preview_advance,
    search as _search,
)
from okg.substrate.mcp.envelope import (
    project_row,
    select_retry_from,
    validate_select,
    with_read_coverage,
)
from okg.substrate.mcp.passage_rollup import (
    validate_rank_by as _validate_rank_by,
)
from okg.substrate.mcp import citation_id as _citation_id
from okg.substrate.mcp.search_views import (
    SearchLimit as _SearchLimit,
    grep_list_projection as _grep_list_projection,
    list_projection as _list_projection,
    metadata_projection as _metadata_projection,
    resolve_search_view as _resolve_search_view,
)
from okg.substrate.mcp.read_affordances import (
    sibling_disclosure as _sibling_disclosure,
)
from okg.substrate.mcp.search import search_select_elsewhere as _search_select_elsewhere
from okg.substrate.mcp.select_vocabulary import (
    SEARCH_LINEAGE_METHODS as _SEARCH_LINEAGE_METHODS,
)
from okg.substrate.mcp.tools import EXPAND_SELECT_ELSEWHERE
from okg.substrate.mcp.search import (
    _ALIAS_FIELDS,
    ATTRS_LOOKUP_FIELDS as _ATTRS_LOOKUP_FIELDS_SHARED,
    _citation_from_attrs,
    _LEXICAL_DEFAULT as _LEXICAL_SEARCH_DEFAULT_FIELDS,
    _LEXICAL_FIELDS,
    _SEMANTIC_FIELDS,
    _with_handles,
    matches_attr_filters as _matches_attr_filters,
    record_identity as _record_identity,
    validate_attr_filters as _validate_attr_filters,
)
from okg.substrate.mcp.authority_overrides import refuse_client_policy_overrides
from okg.substrate.mcp.evidence import _metadata_for_rows, classify_evidence_rows
from okg.substrate.mcp.result_text import HANDLE_KEY
from okg.substrate.mcp.session import _db_fingerprint
from okg.substrate.mcp.set_operators import is_set_input, validate_set_scope
from okg.substrate.mcp.logical_session import LogicalSessionRegistry
from okg.substrate.mcp.surface_freshness import (
    mcp_surface_report,
    served_instructions_fields,
    server_info_version,
)
from okg.substrate.mcp.tools import _FIND_MENTIONS_FIELDS
from okg.substrate.mcp.dataset_card import (
    DEPLOYMENT_GUIDANCE_HEADING,
    DatasetFacts,
    card_inputs as _dataset_card_inputs,
    card_unavailable as _dataset_card_unavailable,
    facts_without_database as _dataset_facts_without_database,
    gather_dataset_facts as _gather_dataset_facts,
    render_dataset_card as _render_dataset_card,
)
from okg.substrate.mcp.capability_probe import (
    CAPABILITY_FIXTURE_ENV,
    CapabilityFixtureInvalid,
    SearchSchemaUnreachable,
    assert_search_capability,
    capabilities_for_surface_probe,
    compile_tool_surface,
    compose_instructions,
    probe_capabilities_from_pool,
    render_unavailable_lines,
)
from okg.substrate.mcp.path_diagnostics import (
    InvalidExpandShape,
    InvalidPathDirection,
    InvalidPathGrammar,
    any_link,
    path_authority_summary,
    stamp_path_error,
    validate_one_hop_edge_type,
    validate_path_limit,
)
from okg.substrate.nomos import mcp_subject as _mcp_subject
from okg.substrate.nomos import reader_view as _reader_view
from okg.substrate.nomos.read_masking import MASK_TOKEN, REDACTION_MARKER
from okg.substrate.nomos.read_filter import (
    filter_edge_rows_for_session,
    filter_node_rows_for_session,
)
from okg.substrate.commit_scope import read_pass_scope
from okg.substrate.nomos.runtime_enforcement import (
    deployment_requires_nomos_runtime,
)


log = logging.getLogger(__name__)

#: The refusal of an aggregate under access rules, in plain words (#3666).
AGGREGATE_UNDER_ACCESS_RULES = (
    "aggregate: counting is not available under this deployment's access "
    "rules, because a count could include records you may not read"
)
AGGREGATE_UNDER_ACCESS_RULES_HINT = (
    "list the records with filter or map instead: each page holds only "
    "records you may read, and no page is a total"
)


class _ReferencePredicateUnsupported(Exception):
    """A reference consumer's predicate the effective-view compiler cannot
    lower (#3222). Raised only around the predicate read, so a ValueError
    from any other read is not relabelled as a refusal."""


def _stamp_freshness_outcomes(
    payload: dict[str, Any],
    operator_name: str,
    *,
    conn: Any,
    deployment_name: str,
    generation_id: str | int | None,
) -> None:
    """Attach both freshness signals to every read-shaped response."""
    from okg.substrate.mcp.freshness import (
        READ_OPERATORS,
        publish_freshness,
        source_freshness,
    )

    if operator_name not in READ_OPERATORS:
        return
    if "source_freshness" not in payload:
        payload["source_freshness"] = source_freshness(conn, deployment_name)
    if "publish_freshness" not in payload:
        payload["publish_freshness"] = publish_freshness(
            conn,
            deployment_name,
            generation_id,
        )


def _stamp_unavailable_freshness(
    payload: dict[str, Any],
    *,
    deployment_name: str,
) -> None:
    """Preserve response availability while making probe failure explicit."""
    from okg.substrate.mcp.freshness import unavailable_freshness

    payload.setdefault(
        "source_freshness",
        unavailable_freshness(
            deployment=deployment_name,
            cause="source_freshness_query_failed",
            omitted_work="source_watermark_summary",
        ),
    )
    payload.setdefault(
        "publish_freshness",
        unavailable_freshness(
            deployment=deployment_name,
            cause="publish_freshness_query_failed",
            omitted_work="publish_lag_summary",
        ),
    )


#: The `filter` calls a refusal's schema_hint shows, each one accepted when
#: sent exactly as written (#3666: the old hint taught `value` for
#: and/or/not/in/between, which the predicate model refuses, and left out
#: order_by). `test_operator_surface_integration` sends every one.
FILTER_HINT_EXAMPLES: tuple[dict[str, Any], ...] = (
    {"input": {"kind": "nodes"},
     "predicate": {"op": "eq", "field": "subtype", "value": "document"},
     "order_by": "node_id desc"},
    {"input": {"kind": "nodes"},
     "predicate": {"op": "in", "field": "attrs.status", "values": ["open", "closed"]}},
    {"input": {"kind": "nodes"},
     "predicate": {"op": "between", "field": "attrs.version", "values": ["1.0", "2.0"]}},
    {"input": {"kind": "nodes"},
     "predicate": {"op": "contains", "field": "attrs", "value": {"status": "open"}}},
    {"input": {"kind": "nodes"},
     "predicate": {"op": "and", "clauses": [
         {"op": "suffix", "field": "attrs.path", "value": ".py"},
         {"op": "not", "clause": {"op": "exists", "field": "attrs.owner"}},
     ]}},
    {"input": {"kind": "edges"},
     "predicate": {"op": "eq", "field": "edge_type", "value": "cites"}},
)


def _filter_schema_hint() -> str:
    calls = "; ".join(
        "filter(" + ", ".join(
            f"{name}={_json.dumps(value, separators=(',', ':'))}"
            for name, value in example.items()
        ) + ")"
        for example in FILTER_HINT_EXAMPLES
    )
    return (
        "predicate is the condition tree search takes as where: eq, lt, gt, "
        "prefix, suffix take field and value; in and between take field and "
        "values; exists takes field; and/or take clauses; not takes clause; "
        "contains takes field 'attrs' and an object value; matches takes field "
        "'text' and words or re:/<regex>/. Fields: node_id, "
        "subtype, text_hash (edges: edge_id, src, dst, edge_type) or "
        "attrs.<key>. order_by is '<field>' or '<field> desc'; an attrs.<key> "
        "sorts by its declared type, and a refusal names what sorts. "
        f"Examples: {calls}."
    )


FILTER_SCHEMA_HINT = _filter_schema_hint()

#: How many sortable attributes an order_by refusal names before it counts
#: the rest.
_SORTABLE_NAMED = 20


def _order_by_not_sortable_message(
    key: str, *, kind: str, declared: Mapping[str, str],
) -> str:
    """The order_by refusal for an attribute no index can sort, naming what
    can be sorted on this deployment (#3666: it used to ask the caller to
    "declare its value_type", which a reader cannot do)."""
    from okg.substrate.mcp.operator_sql import EFFECTIVE_VIEW_FIELD_COLUMNS

    fields = ", ".join(EFFECTIVE_VIEW_FIELD_COLUMNS.get(kind, {}))
    names = sorted(declared)
    if not names:
        return (
            f"order_by attrs.{key} cannot be sorted here: it has no declared "
            "type, and this deployment declares no sortable attribute. Sort "
            f"by a field: {fields}"
        )
    shown = ", ".join(f"attrs.{name}" for name in names[:_SORTABLE_NAMED])
    more = len(names) - _SORTABLE_NAMED
    if more > 0:
        shown += f" and {more} more"
    return (
        f"order_by attrs.{key} cannot be sorted here: it has no declared "
        f"type. Sortable: {shown}; or a field: {fields}"
    )


# Request-scoped pin cache TTL. The "latest pin" is explicitly allowed
# to be stale within this window only when no durable MCP session identity
# exists. Logical-session first bind/refresh and explicit generation calls
# bypass it. Only consulted under auto_advance="never".
LATEST_PIN_CACHE_TTL_S = 0.5


# The registered response wrapper sets this for exactly one public core-read
# invocation. ContextVar (rather than a module global) keeps concurrent MCP
# requests isolated while allowing the many internal `_open(...)` call sites to
# consume the SDK request context without threading a plumbing argument through
# every operator helper.
_CURRENT_MCP_CONTEXT: ContextVar[Context | None] = ContextVar(
    "okg_current_mcp_context",
    default=None,
)


# Stable graph columns the attr-operator SQL pushdown may filter on. These are
# the scalar fields the chronos scan exposes that the effective-attrs overlay
# can NEVER mutate (the overlay only writes attrs), so an eq/in/exists clause
# over one of them evaluates identically in SQL (raw graph) and in the Python
# effective-attrs post-filter -- the precondition for pushing it down without
# changing operator results. Mirrors operators._FILTER_SCALAR_FIELDS.
_STABLE_PUSHDOWN_FIELDS: dict[str, frozenset[str]] = {
    "nodes": frozenset({"node_id", "subtype", "text_hash"}),
    "edges": frozenset({"edge_id", "src", "dst", "edge_type"}),
}


# Legacy request-scoped latest-lookup policy grammar:
#   "never"  — default; contextless omitted calls use the TTL cache
#   "always" — contextless omitted calls resolve latest uncached
# `when_behind_by:<N>` is parsed for an actionable retirement error, then
# refused at server construction: it silently replaced explicit pins and is
# incompatible with the authoritative explicit-generation contract.
_AUTO_ADVANCE_DEFAULT = "never"


def add_pagination_hint(
    payload: dict[str, Any],
    operator_name: str,
) -> dict[str, Any]:
    if not payload.get("truncated"):
        return payload
    next_offset = payload.get("next_offset")
    if next_offset is None and payload.get("pagination_capped"):
        # The producer withheld the cursor because the next page lies past
        # its offset ceiling; offset + limit would clamp back to this page.
        return payload
    if next_offset is None and isinstance(payload.get("result_reference"), dict):
        # A retained search pages its reference, not search (#3222, H1).
        return payload
    if (
        next_offset is None and operator_name in {"filter", "map"}
        and payload.get("continuation_token")
    ):
        # A keyset token continues the page (#3666 step 3b).
        return payload
    if next_offset is None:
        offset = payload.get("offset")
        limit = payload.get("limit")
        if isinstance(offset, int) and isinstance(limit, int):
            next_offset = offset + limit
            payload["next_offset"] = next_offset
    if next_offset is None:
        return payload
    payload["pagination"] = {
        "truncated": True,
        "offset": payload.get("offset", 0),
        "limit": payload.get("limit"),
        "next_offset": next_offset,
        "operator": operator_name,
        "parameter": "offset",
    }
    payload["pagination_hint"] = (
        f"Results are truncated. To continue this same result set, call "
        f"{operator_name} again with offset={next_offset} and the same "
        "filters/projection."
    )
    if payload.get("completeness") in ("exact", "exact_over_pages"):
        # The next page of an exact lexical read is exact only if asked for
        # again; a bare offset would page the bounded default instead (PACT
        # lexical-index-scan-cap-3162). An exact listing's next page says
        # whether it is the last one only when asked the same way (#3666).
        payload["pagination"]["completeness"] = "exact"
        payload["pagination_hint"] = (
            payload["pagination_hint"][:-1] + ', and completeness="exact".'
        )
    return payload


# Same pagination bound as the search methods (attacker-chosen offsets must
# not drive unbounded backend work).
EXPAND_MAX_OFFSET = 10_000

#: A bounded operator COUNT (#3201) stops at this many rows, and the count is
#: then a lower bound, or at this statement timeout, and the count is then
#: unknown. Read at call time, so tests can lower them.
_OPERATOR_COUNT_CAP = 100_000
_OPERATOR_COUNT_TIMEOUT_MS = 5_000
#: Issue #3666: the one time budget of a call's date work, in milliseconds.
#: The stored-value read, every page statement, the group count and the
#: bounded total of a call that carries a date condition (or a date period)
#: each run with `statement_timeout` set to what is left of it, so the
#: server cancels the statement; when it is spent the call refuses
#: (`date_call_over_budget`). Module level, read at call time.
_DATE_CALL_BUDGET_MS = 5_000
#: The most of that budget the stored-value read may take. Past it the call
#: goes on comparing ISO-8601 stored values alone (`scope_too_large`).
_DATE_VALUES_READ_BUDGET_MS = 1_500
#: The most of that budget the bounded total of a date call may take. The
#: total reads every match; past this it is unknown, as on any timeout.
_DATE_TOTAL_BUDGET_MS = 1_000
#: With less than this left, no further date statement is issued: a hard one
#: refuses the call, a soft one (the stored-value read, the total) is skipped.
_DATE_STATEMENT_FLOOR_MS = 100
#: A statement of a date phase runs with `statement_timeout` at most this
#: much above the time left when it starts: the setting is refreshed before
#: a statement once it is this stale.
_DATE_TIMEOUT_REFRESH_MS = 50
#: What a page reads before its statement (the registry, the index
#: readiness, the candidate plan) is not date work, and a database that has
#: not served the deployment yet takes seconds over it once. Up to this much
#: of it per call is not charged to the date budget.
_DATE_SETUP_UNCHARGED_MS = 2000
#: A date range's index candidates drive the page only while they are at
#: most this many: past it the page walks its own order and filters, which
#: stops at the page size when most records match. Counted by a bounded
#: probe with `_DATE_PROBE_SHARE_MS` at most; the walk gets
#: `_DATE_WALK_SHARE_MS`, and the candidates take over if it does not
#: finish.
_DATE_WIDE_CANDIDATES = 50_000
_DATE_PROBE_SHARE_MS = 300
_DATE_WALK_SHARE_MS = 1000
#: The level of the one "date call ..." log line (`_log_date_call`): unset
#: is DEBUG. `okg mcp-serve` configures no log handler, so only WARNING and
#: above reach its stderr; `OKG_DATE_CALL_LOG=warning` puts the line there.
DATE_CALL_LOG_ENV = "OKG_DATE_CALL_LOG"
_DATE_CALL_LOG_LEVELS = {
    "debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING,
}
#: `total_count_reason` when a date call's total was not counted inside its
#: share of the budget. The page and what it says of itself are unchanged.
TOTAL_NOT_COUNTED_IN_TIME = "count_not_computed_in_time"
#: Called with each new `DateBudget`; a test or an operator diagnosis reads
#: the statements and timeouts of a call through it.
_DATE_BUDGET_OBSERVER: Optional[Callable[[Any], None]] = None

# Offset ceiling of search's query-less `attrs` lookup. Module level, read at
# call time, so a test can lower it to reach the end of paging on a small
# graph. Past it the lookup withholds `next_offset` and says so.
ATTRS_LOOKUP_MAX_OFFSET = 10_000
#: The statement timeout of the lookup's case-hint read (issue #3666): the
#: hint is optional, so it never adds more than this to a reply.
_CASE_HINT_TIMEOUT_MS = 500

# Issue #3208: under Nomos `audit`/`enforce`, `_effective_view_sql_page`
# (attribute lookup, filter, map) pages over the rows the reader may read, so
# it reads stored matches past the page until one readable row beyond it
# proves a next page. This bounds that read: enough stored rows for the
# deepest lookup page (its offset ceiling plus the 1,000-row page limit) when
# nothing is hidden. A call that reaches it ends paging and says so; only a
# value at least this many stored rows share can reach it. Module level, read
# at call time, so a test can lower it.
READ_FILTER_SCAN_CAP = 10_000 + 1_000 + 1
READ_FILTER_MAX_BATCH = 1_000


#: Read-model slice 7a (PACT read-model-slice7a-predicates): the `force` the
#: page step passes to `reader_view.open` under Nomos `audit`/`enforce`.
#: None takes the class path when the view can; "per_row" places no row, so
#: a clause on a key the row's subtype does not reveal is unknown in SQL and
#: the check on the read row decides; "shadow" also runs the per-row filter
#: next to the class path on the returned rows and refuses any difference
#: (rows, class decisions, readable() against the masks). Module
#: level, read at call time, so a test or an operator diagnosis can set it.
READER_VIEW_FORCE: Optional[str] = None

#: The stored graph row the reader view's SQL reads (class columns, stored
#: attrs for the classification, subtype), joined to each effective row.
_READER_ROW_ALIAS = "reader_row"


def _reader_view_sql(
    conn: Any,
    session: Any,
    *,
    target_kind: str,
    relation: str,
    params: dict[str, Any],
) -> tuple[Any, str, Callable[[str], str]]:
    """Open the reader's view for one effective-attribute page (slice 7a).

    Returns ``(view, join_sql, readable)``. In class mode ``join_sql`` is a
    ``LEFT JOIN LATERAL`` to the row's stored graph version at the session's
    pin (the Chronos segment, ``live_lo <= branch point < live_hi``), which
    carries the two read-class columns, the stored ``attrs`` the
    classification label is read from and the subtype; the effective
    relation has none of the class columns. In any other mode it is empty
    and the view's SQL reads the effective row (only its subtype). A
    missing segment or branch point raises: there is no unfiltered fallback.
    ``readable(key)`` is ``view.readable`` rendered for the psycopg
    ``%(name)s`` statement (a literal ``%`` doubled), undecided (NULL) where
    the join finds no stored version. For an edge it is also made
    undecided (NULL) on a row with an overlay unless the view knows the edge
    is served unredacted: a redacted edge is served its stored value, so the
    overlaid value the SQL sees must not decide it. A redacted node is
    served its overlaid value, the one the SQL tests."""
    from okg.substrate.bm25_node_search import branch_point

    # The caller opens the view only under Nomos audit/enforce, on its one
    # reading of the mode; the view does not read it again.
    view = _reader_view.open(
        conn, session, row_kind=target_kind, action=None, surface=None,
        force=READER_VIEW_FORCE, nomos_runtime=True,
    )
    alias = relation
    join_sql = ""
    if view.mode == "class":
        table = "okg.graph_nodes" if target_kind == "node" else "okg.graph_edges"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT physical_table FROM _chronos_branch_tables "
                "WHERE backend = 'interval' AND table_name = %s",
                (table,),
            )
            found = cur.fetchone()
        if found is None or not found[0]:
            raise SubstrateError(
                "reader view: the Chronos segment of the graph table is not "
                "registered, so the read classes cannot be joined",
                data={"table": table},
                fix_hint="run `okg doctor` on this deployment",
            )
        segment = str(found[0])
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.\"]*", segment):
            raise SubstrateError(
                "reader view: unexpected Chronos segment name",
                data={"table": table, "segment": segment},
            )
        params["reader_bp"] = branch_point(conn, session)
        # The effective relation keys every target by TEXT; graph_edges keys
        # edges by BIGINT. Cast the effective side so the segment's id index
        # still drives the lookup.
        id_match = (
            f"g.node_id = {relation}.target_id" if target_kind == "node"
            else f"g.edge_id = CAST({relation}.target_id AS bigint)"
        )
        subtype = "g.subtype" if target_kind == "node" else "CAST(NULL AS text)"
        alias = _READER_ROW_ALIAS
        join_sql = (
            " LEFT JOIN LATERAL (SELECT TRUE AS present, g.attrs, "
            f"{subtype} AS subtype, g.nomos_read_class, g.nomos_read_class_version "
            f"FROM {segment} g WHERE {id_match} "
            "AND g.deleted = FALSE AND g.live_lo <= %(reader_bp)s "
            f"AND %(reader_bp)s < g.live_hi LIMIT 1) AS {alias} ON TRUE"
        )

    redacts = view.redacts(alias).as_string(conn).replace("%", "%%")

    def readable(key: str) -> str:
        base = view.readable(alias, key).as_string(conn).replace("%", "%%")
        if join_sql:
            # A join miss (no stored version at the pin) leaves every
            # reader_row column NULL, the subtype included, and a NULL
            # subtype would read the key through the legacy classifier,
            # which may reveal a key the row's own subtype masks. Undecided,
            # never revealed (fail closed).
            base = f"(CASE WHEN {alias}.present THEN {base} END)"
        if target_kind != "edge":
            # A node is decided and masked on its EFFECTIVE attrs: the
            # per-row filter applies accepted overlays first
            # (read_filter._enriched_node_rows), so the value the SQL tests
            # is the value a redacted node is served.
            return base
        # An edge is not: the per-row filter decides and masks its STORED
        # attrs, and the later overlay pass keeps a ReadRedactedAttrs result
        # (apply_effective_attrs_to_rows), so a redacted edge is served its
        # stored value while the SQL tests the overlaid one. On an edge with
        # an overlay (raw_attrs IS NOT NULL) a key is therefore decided in SQL
        # only where the view knows the edge is served unredacted; otherwise
        # it is undecided and the check on the served row decides.
        # Value-blind: it reads the class, the classification and whether an
        # overlay exists, never a value.
        return (
            f"(CASE WHEN {relation}.raw_attrs IS NULL OR ({redacts}) IS FALSE "
            f"THEN {base} END)"
        )

    return view, join_sql, readable


def _reader_view_tail(
    *,
    predicate: Any,
    predicate_sql: str,
    readable: Callable[[str], str],
) -> str:
    """``(tree IS TRUE OR <a key the predicate reads is undecided>)`` - the
    part of ``_reader_view_where`` after the view's predicate."""
    keys: set[str] = set()
    whole = False

    def walk(node: Any) -> None:
        nonlocal whole
        if isinstance(node, list):
            for child in node:
                walk(child)
            return
        if not isinstance(node, dict):
            return
        field = node.get("field")
        if isinstance(field, str) and field.startswith("attrs.") and field.split(".", 1)[1]:
            keys.add(field.split(".", 1)[1])
        if field == "attrs":
            value = node.get("value")
            if node.get("op") == "contains" and isinstance(value, dict):
                keys.update(str(key) for key in value)
            elif node.get("op") != "exists":
                whole = True
        walk(node.get("clause"))
        walk(node.get("clauses"))

    walk(predicate)
    undecided = [f"(({readable(key)}) IS NULL)" for key in sorted(keys)]
    if whole:
        undecided.append("TRUE")
    tail = f"(({predicate_sql}) IS TRUE)"
    if undecided:
        tail = f"({tail} OR " + " OR ".join(undecided) + ")"
    return tail


def _reader_view_where(
    *,
    view_predicate: str,
    predicate: Any,
    predicate_sql: str,
    readable: Callable[[str], str],
) -> str:
    """The page's WHERE tail under the reader's view (slice 7a).

    A row is kept when the view's predicate keeps it and either the
    readable-guarded predicate IS TRUE or a key the predicate reads is
    undecided for the row (``readable`` NULL), in which case the read-row
    check decides. A masked value is NULL in the tree, so it can never make
    the tree TRUE, and whether a row is undecided depends only on its class,
    classification and subtype, never on a value. A leaf on the whole
    ``attrs`` object is undecided for every row."""
    if not predicate_sql:
        return f" AND ({view_predicate})"
    tail = _reader_view_tail(
        predicate=predicate, predicate_sql=predicate_sql, readable=readable,
    )
    return f" AND ({view_predicate}) AND {tail}"


#: Issue #3393: under Nomos audit with no active bundle, expand decides a
#: destination in SQL only where its attrs carry a source-lineage envelope
#: that certainly yields a source id, so the per-row filter serves it
#: unredacted; any other destination is undecided on every field.
#:
#: The condition is sufficient, over a row's ``attrs`` (``{rel}``), for the
#: per-row filter to find a source id in its lineage
#: (``source_lineage.normalize_source_lineage`` of the ``nomos_source_lineage``
#: envelope, which accepted overlays never change -
#: ``attributes.RESERVED_EFFECTIVE_ATTR_KEYS``): the envelope is an object
#: whose status is not "malformed" and whose ``sources`` is a non-empty array
#: in which every object carries a string ``source_id`` with a letter or
#: digit (so it survives ``str.strip``) and at least one element is an
#: object. It never holds for a row without lineage. Anything else - a
#: string envelope, ``id`` instead of ``source_id``, lineage only in the
#: ``source`` column - is left undecided (fail closed), not judged. The
#: one-source envelope (every row the comp-ops copy holds) is checked without
#: unnesting the array.
_LINEAGE_SERVES_SQL = (
    "(jsonb_typeof({rel}.attrs -> 'nomos_source_lineage') = 'object'"
    " AND COALESCE({rel}.attrs -> 'nomos_source_lineage' ->> 'status', '') <> 'malformed'"
    " AND jsonb_typeof({rel}.attrs -> 'nomos_source_lineage' -> 'sources') = 'array'"
    " AND CASE jsonb_array_length({rel}.attrs -> 'nomos_source_lineage' -> 'sources')"
    " WHEN 0 THEN FALSE"
    " WHEN 1 THEN (jsonb_typeof({rel}.attrs #> '{{nomos_source_lineage,sources,0,source_id}}')"
    " = 'string' AND ({rel}.attrs #>> '{{nomos_source_lineage,sources,0,source_id}}')"
    " ~ '[A-Za-z0-9]')"
    " ELSE (EXISTS (SELECT 1 FROM jsonb_array_elements("
    "{rel}.attrs -> 'nomos_source_lineage' -> 'sources') AS lineage_source(e)"
    " WHERE jsonb_typeof(e) = 'object')"
    " AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements("
    "{rel}.attrs -> 'nomos_source_lineage' -> 'sources') AS lineage_source(e)"
    " WHERE jsonb_typeof(e) = 'object' AND NOT (jsonb_typeof(e -> 'source_id') = 'string'"
    " AND (e ->> 'source_id') ~ '[A-Za-z0-9]'))) END)"
)


def _expand_destination_placed_sql(view: Any, alias: str, conn: Any, *, relation: str) -> str:
    """Issue #3393: a SQL boolean over expand's destination that is TRUE where
    the reader view knows the destination is served and how, FALSE where it
    knows it is hidden, and NULL (undecided) everywhere else. Decided from the
    stored read class, classification and subtype only, never a value, so an
    undecided destination's stored values cannot decide which edges the SQL
    keeps or how it orders them.

    * class mode, no classification overlay: placed when the view decides the
      row's redaction (a current class and a recognized classification
      label), and then its predicate is exact;
    * class mode with an overlay: placed (served) only for a class every
      probed classification serves unredacted;
    * no_bundle audit: placed (served unredacted) only where the
      destination's attrs carry a lineage envelope the per-row filter
      certainly reads a source id from (``_LINEAGE_SERVES_SQL``); no_bundle
      that serves nothing: FALSE;
    * per_row and any other mode: undecided."""
    def text(composable: Any) -> str:
        return composable.as_string(conn).replace("%", "%%")

    if view.mode == "class":
        redacts = text(view.redacts(alias))
        if view._state.overlay:
            return f"(CASE WHEN ({redacts}) IS FALSE THEN TRUE END)"
        return f"(CASE WHEN ({redacts}) IS NOT NULL THEN ({text(view.predicate(alias))}) END)"
    if view.mode == "no_bundle":
        if not view.serves_unredacted:
            return "FALSE"
        guard = _LINEAGE_SERVES_SQL.format(rel=relation)
        return f"(CASE WHEN {guard} THEN TRUE END)"
    return "CAST(NULL AS boolean)"


#: The operator's switch for `run`'s live steps (#3666, lane ri).
RUN_LIVE_STEPS_ENV = "OKG_MCP_RUN_LIVE_STEPS"


def run_live_steps_enabled(environ: Optional[Mapping[str, str]] = None) -> bool:
    """Whether `run` may make live calls: on unless the operator set
    `OKG_MCP_RUN_LIVE_STEPS` to `0` or `off`. Any other value than those,
    `1`, `on` or unset is refused at start, never read as on."""
    raw = (environ if environ is not None else os.environ).get(RUN_LIVE_STEPS_ENV)
    if raw is None or raw.strip().lower() in ("", "1", "on"):
        return True
    if raw.strip().lower() in ("0", "off"):
        return False
    raise ValueError(
        f"{RUN_LIVE_STEPS_ENV}={raw!r}: set it to 0 or off to turn run's live "
        "steps off, or to 1 or on (the default)"
    )


def _refuse_unbound_live_call(mcp: Any, live: Any) -> None:
    """Refuse `expand(live=...)` when this deployment binds no live relation.

    Issue #2013. Accepting the parameter and returning only graph rows would
    be the silent fallback this repository forbids: the caller asked an
    external system a question and would get a perfectly ordinary-looking
    answer that never asked it.
    """
    from okg.substrate.external_backed.live_dispatch import (
        LiveRelationsUnboundDeployment,
    )

    bindings = getattr(mcp, "_okg_live_bindings", None) or {}
    if bindings:
        return
    relation = (
        live.get("relation") if isinstance(live, Mapping)
        else getattr(live, "relation", None)
    )
    raise LiveRelationsUnboundDeployment(
        f"expand(live=...) asked for live relation {relation!r}, but this "
        f"deployment declares no `agent_surface.live_bindings`, so no live "
        f"relation can be dispatched here. The graph rows are unaffected; "
        f"this call is refused rather than answered with graph rows only, "
        f"which would look like a successful live read that never happened.",
        data={"relation": relation, "bound": [], "issue": 2013},
        fix_hint=(
            "declare the relation on the subtype and bind it in "
            "`agent_surface.live_bindings`, or drop the `live` argument"
        ),
    )


def validate_expand_offset(offset: Optional[int]) -> int:
    """Normalize and bound `expand`'s cursor, refusing rather than clamping.

    A silently clamped offset re-serves page 1 while the caller believes it has
    paged — the same class of silent failure as issue #903, where a falsy
    `offset` silently changed the row shape instead of paging.
    """
    value = int(offset or 0)
    if value < 0:
        raise InvalidOperatorArguments("expand: offset must be >= 0")
    if value > EXPAND_MAX_OFFSET:
        raise InvalidOperatorArguments(
            f"expand: offset must be <= {EXPAND_MAX_OFFSET} (attacker-chosen "
            "offsets must not drive unbounded backend work); narrow the walk "
            "with edge_type or filter instead of paging that deep"
        )
    return value


PREVIEW_TRUNCATE_CHARS = 280

#: Rows an MCP `search` returns when the caller passes no `limit` (PACT
#: mcp-search-compact-results). It was 50, which the 64 KiB response budget
#: then cut to about 40 rows (~68,000 characters) on a real chunk corpus; a
#: chat client re-sends every result on every later turn. A few cards answer
#: most questions, and `next_offset` pages further. The in-process
#: `okg.substrate.mcp.search.search` keeps its own default. 8 (was 10) since
#: PACT read-reply-size-3666, the served default (`search_views.CARDS_LIMIT`):
#: replies were 90% of the Sol graph arm's prompt tokens on ERAG-500.
MCP_SEARCH_DEFAULT_LIMIT = 8


def _assert_method_can_scope(method: str, profile: Optional[str]) -> None:
    """Refuse a profile on a method that cannot honour a subtype scope.

    Pure argument validation, deliberately callable before any connection is
    opened, so this refusal is provable without a database."""
    from okg.substrate.mcp.search_profiles import assert_method_can_scope

    assert_method_can_scope(method, profile)


def _declared_search_names(
    deployment_name: str,
) -> Optional[tuple[tuple[str, ...], tuple[str, ...]]]:
    """`(source families, profiles)` the deployment declares, or None when
    its manifest cannot be read (the search then meets the checks it always
    met, which say why). No database."""
    try:
        from okg.substrate.deployments import resolve_deployment
        from okg.substrate.mcp.search_profiles import parse_search_config

        manifest = resolve_deployment(deployment_name)
        config = parse_search_config(
            getattr(manifest, "search", None),
            manifest_path=getattr(manifest, "deployment_dir", None),
        )
    except Exception:  # noqa: BLE001 - the session's own resolution reports it.
        return None
    if config is None:
        return (), ()
    return tuple(config.source_families or {}), tuple(config.profiles or {})


def _assert_method_can_filter_family(method: str, requested: Any) -> None:
    """Refuse a family filter on a method that cannot carry the predicate."""
    from okg.substrate.mcp.search_profiles import (
        assert_method_can_filter_family,
    )

    assert_method_can_filter_family(method, requested)


def _validate_max_per_parent(value: Any, *, origin: str) -> Optional[int]:
    """Refuse a per-parent bound that is not an integer >= 1."""
    from okg.substrate.mcp.search_profiles import validate_max_per_parent

    return validate_max_per_parent(value, origin=origin)


def _refuse_pin_past_attribute_retention(
    conn: Any, *, generation: Any, branch_id: Any, ordinal: int,
) -> None:
    """Refuse a typed read pinned past the attribute-history window (#2621).

    Unless a deployment declares its own history retention, each publish
    prunes closed attribute intervals older than the newest `keep` generations,
    and no typed reader recomputes them, so a pin below the window finds its
    interval rows missing. That is retention, not a missing backfill: say so
    and name the read that still serves the generation, instead of the
    backfill advice meant for an operator enabling interval reads.

    Decided from what the branch's latest publish recorded it ran
    (`effective_interval_view_prune` in its checkpoint metadata: skipped under
    a declared policy, else its `keep_generations` and protected ordinal), so
    a deployment with its own retention keeps the integrity refusal, and the
    window is the publisher's, not this process's environment. Reads only
    surfaces `okg_mcp` is granted: `okg.v_generation_interval_stab` and the
    checkpoint table this read path already uses.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT stab_ordinal, generation_ref "
            "FROM okg.v_generation_interval_stab "
            "WHERE chronos_branch_id = %s "
            "ORDER BY stab_ordinal DESC LIMIT 64",
            (branch_id,),
        )
        newest = [(int(row[0]), row[1]) for row in cur.fetchall()]
        if not newest:
            return
        cur.execute(
            "SELECT metadata FROM _chronos_branch_interval_checkpoints "
            "WHERE checkpoint_id = %s",
            (newest[0][1],),
        )
        row = cur.fetchone()
    metadata = row[0] if row else None
    if isinstance(metadata, str):
        metadata = _json.loads(metadata or "{}")
    prune = (metadata or {}).get("effective_interval_view_prune")
    if not isinstance(prune, Mapping) or prune.get("skipped"):
        return
    keep = prune.get("keep_generations")
    if not isinstance(keep, int) or keep < 1 or len(newest) < keep:
        return
    ordinals = sorted({value for value, _ref in newest}, reverse=True)
    if len(ordinals) < keep:
        return
    cutoff = ordinals[keep - 1]
    protect = prune.get("protect_from_ord")
    if isinstance(protect, int):
        cutoff = min(cutoff, protect)
    # A pruned interval had `to_ord <= cutoff`, so only a pin below it can
    # have lost rows to the prune.
    if ordinal >= cutoff:
        return
    raise SubstrateError(
        "attribute reads through the interval view (such as filter) are not "
        "served at this generation: it is older than the newest "
        f"{keep} generations whose attribute history is kept for them",
        data={
            "generation_id": generation,
            "branch_id": branch_id,
            "ordinal": ordinal,
            "retained_from_ordinal": cutoff,
            "retention_generations": keep,
        },
        fix_hint=(
            "read at the current generation (omit generation_id) or a newer "
            "one; inspect(target={kind:'node', node_id:...}) with this same "
            "generation_id still returns this generation's values"
        ),
    )


def _target_kind_literal(target_kind: Any) -> str:
    """The SQL literal for an effective-view target kind (issue #3408)."""
    if target_kind not in ("node", "edge"):
        raise ValueError(f"target_kind must be 'node' or 'edge', not {target_kind!r}")
    return f"'{target_kind}'"


def _clamp_preview_chars(value: Any) -> tuple[int, Optional[dict[str, int]]]:
    """Lower an over-limit `max_chars` and say so; refuse the rest (#3252)."""
    from okg.substrate.mcp.search import clamp_preview_chars

    return clamp_preview_chars(value)


def _validate_completeness(value: Any, *, method: str) -> None:
    """Refuse an unknown `completeness`, or one on a non-lexical method."""
    from okg.substrate.mcp.search import validate_completeness

    validate_completeness(value, method=method)


def _family_disclosure(scope: Any, *, method: str) -> dict[str, Any]:
    """The complete authority/diversity disclosure set for one method.

    Used by the non-ranked `search` branches so every method emits the SAME
    keys; see `search_profiles.FAMILY_DISCLOSURE_KEYS`."""
    from okg.substrate.mcp.search_profiles import UNSCOPED, family_disclosure

    return family_disclosure(scope or UNSCOPED, method=method)


#: Predicate ops that survive BOTH scan-predicate renderers — the
#: `_attr_free_pushdown_clauses` conjunct filter and the effective-view
#: `render()`. Anything else is dropped SILENTLY by both, which is fine for a
#: clause Python re-checks but fatal for one that is a caller-visible promise.
#: Kept module-level so the profile-scope clause below can be proven safe
#: without a database.
_PUSHDOWN_SAFE_OPS: frozenset[str] = frozenset({"eq", "in", "exists"})


def _profile_scope_clause(subtypes: Any) -> dict[str, Any]:
    """The scan-predicate clause for a resolved search profile.

    MUST be an `in` node. An `or` tree of `eq`s means the same thing to a
    reader and is dropped by both renderers, so the scope would silently not
    apply while the envelope still reported `scoped_subtypes`."""
    clause = {"op": "in", "field": "subtype", "values": list(subtypes)}
    assert clause["op"] in _PUSHDOWN_SAFE_OPS
    return clause


def _intersect_subtype_filter(scope: Any, subtype_filter: Optional[str]) -> None:
    """Refuse a subtype filter outside the resolved profile. See search.py."""
    from okg.substrate.mcp.search_profiles import intersect_subtype_filter

    intersect_subtype_filter(scope, subtype_filter)


def _scope_emptiness_disclosure(
    conn: Any,
    session: Any,
    scope: Any,
) -> dict[str, Any]:
    from okg.substrate.mcp.search_profiles import scope_emptiness_disclosure

    return scope_emptiness_disclosure(conn, session, scope)


def _expand_sibling_block(handle: Any, node_id: str) -> dict[str, Any]:
    """Resolve sibling disclosure at the session pin and authority."""
    from okg.substrate.bm25_node_search import branch_point
    from okg.substrate.mcp.passage_rollup import resolve_edge_segment
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment

    conn, session = handle.conn, handle.session
    segment = resolve_node_segment(conn)
    bp = branch_point(conn, session)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT node_id, subtype, attrs, source, source_record_id, "
            f"source_revision FROM {segment} "
            f"WHERE node_id = %(node_id)s AND deleted = FALSE "
            f"AND live_lo <= %(bp)s AND %(bp)s < live_hi LIMIT 1",
            {"node_id": node_id, "bp": bp},
        )
        row = cur.fetchone()
    anchor = (
        []
        if row is None
        else filter_node_rows_for_session(
            conn,
            session,
            [
                {
                    "node_id": row[0],
                    "subtype": row[1],
                    "attrs": row[2],
                    "source": row[3],
                    "source_record_id": row[4],
                    "source_revision": row[5],
                }
            ],
        )
    )
    if not anchor:
        return {
            "node_id": node_id,
            "subtype": None,
            "state": "anchor_unreadable",
            "declared_relations": [],
            "neighbours": [],
            "reason": (
                "the anchor node is not readable at this pin, so its sibling "
                "structure cannot be reported. This says nothing about "
                "whether such structure exists."
            ),
        }
    return _sibling_disclosure(
        conn,
        session=session,
        node_id=node_id,
        subtype=str(anchor[0]["subtype"]),
        catalog_version_id=session.catalog_version_id,
        node_segment=segment,
        edge_segment=resolve_edge_segment(conn),
        branch_point_value=bp,
    )


def _nomos_profile_record(
    deployment_name: str,
    requested_profile: Optional[str],
    *,
    subtype_filter: Optional[str] = None,
    method: str = "lexical",
) -> dict[str, Any]:
    """The profile fields for the Nomos resource block, frozen pre-`_open`.

    Resolves the profile that will ACTUALLY apply — which under a deployment
    `default_profile` is not the one the caller named — and its DECLARED
    (un-expanded) subtypes. Decision identity is content-of-access, so a record
    naming a broader read than the one executed is incomplete.

    When the manifest cannot be read, the fields are `None`, NOT `[]`: an empty
    list would say "this profile declares no subtypes", a state the loader
    makes impossible, and would encode an unmeasured signal as a measured one.
    The authoritative resolution runs inside the session and raises there; this
    record-only helper must not pre-empt it with a worse error."""
    from okg.substrate.mcp.search_profiles import PROFILE_APPLICABLE_METHODS

    unknown: dict[str, Any] = {
        "search_profile": requested_profile,
        "declared_profile_subtypes": None,
    }
    unscoped: dict[str, Any] = {
        "search_profile": None,
        "declared_profile_subtypes": None,
    }
    try:
        from okg.substrate.deployments import resolve_deployment
        from okg.substrate.mcp.search_profiles import parse_search_config

        manifest = resolve_deployment(deployment_name)
        config = parse_search_config(getattr(manifest, "search", None))
    except Exception:
        return unknown
    if config is None:
        # No `search:` block: the read is genuinely unscoped, and that IS a
        # measured answer.
        return unscoped
    # These two conditions MUST mirror `resolve_scope` / `resolve_for_surface`
    # exactly. They are the cases where a deployment default does NOT apply, so
    # recording it here would claim a NARROWER access than the one executed and
    # would contradict this same call's envelope.
    if method not in PROFILE_APPLICABLE_METHODS:
        return unscoped
    if requested_profile is None and subtype_filter is not None:
        return unscoped
    applied = requested_profile or config.default_profile
    if applied is None:
        return unscoped
    declared = config.profiles.get(applied)
    return {
        "search_profile": applied,
        "declared_profile_subtypes": (list(declared.subtypes) if declared else None),
    }


def _resolve_mcp_search_scope(
    conn: Any,
    session: Any,
    *,
    deployment_name: str,
    requested_profile: Optional[str],
    subtype_filter: Optional[str],
    method: str,
    requested_families: Any = None,
    requested_max_per_parent: Any = None,
) -> Any:
    """Resolve the deployment search selection for one MCP search call.

    Same manifest block and same resolver the CLI uses, so an agent and an
    operator querying one deployment see one scope, one family set, one set of
    path exclusions and one diversity bound. A deployment that declares
    `search:` whose manifest cannot be resolved RAISES rather than silently
    running unscoped."""
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )
    from okg.substrate.mcp.search_profiles import (
        RESERVED_ALL,
        UNSCOPED,
        ResolvedScope,
        resolve_for_surface,
    )

    def _requested_only() -> bool:
        return (
            requested_profile is None
            and requested_families is None
            and requested_max_per_parent is None
        )

    # The reserved escape hatch stays answerable WITHOUT a manifest, which is
    # the whole point of it: a broken manifest is exactly when a caller needs a
    # way back to the unscoped search. It only short-circuits when `all` is the
    # ONLY thing asked for — a family name still has to be resolved against the
    # deployment's declarations.
    if (
        requested_profile == RESERVED_ALL
        and requested_families is None
        and requested_max_per_parent is None
    ):
        return ResolvedScope(profile=RESERVED_ALL, subtypes=None)
    try:
        manifest = resolve_deployment(deployment_name)
    except DeploymentResolutionError:
        # ONLY "no such deployment" degrades to unscoped. A malformed
        # `search:` block raises DeploymentConfigError at load; swallowing it
        # here would silently unscope every search on a profiled deployment.
        if _requested_only():
            return UNSCOPED
        raise
    if _requested_only() and getattr(manifest, "search", None) is None:
        return UNSCOPED
    return resolve_for_surface(
        conn,
        catalog_version_id=session.catalog_version_id,
        manifest_search_block=getattr(manifest, "search", None),
        manifest_path=getattr(manifest, "deployment_dir", None),
        requested_profile=requested_profile,
        subtype_filter=subtype_filter,
        method=method,
        requested_families=requested_families,
        requested_max_per_parent=requested_max_per_parent,
        deployment_name=getattr(manifest, "name", None) or deployment_name,
    )


#: The envelope's reason token for attribute preview truncation. Deliberately
#: NOT `truncation_reason`: that key means the response byte budget shortened
#: the PAGE (`response_bounds.BYTE_TRUNCATION_REASON`), and `truncated` means
#: there are more rows past this page. Attribute truncation is a third,
#: independent thing — the rows are all here and the page is complete, but
#: individual strings inside them were shortened — and folding it into either
#: existing key would make both unreadable.
ATTRIBUTE_TRUNCATION_REASON = "attribute_preview_budget"


def _truncate_attr_value(
    value: Any,
    *,
    max_chars: int,
    path: str,
    cut: list[str],
) -> Any:
    """Shorten every string inside `value`, recording each one's path.

    Recurses through dicts and lists, mirroring
    `provenance._fence_attr_value`. Before this, only TOP-LEVEL string attrs
    were shortened, so a long body nested one level down
    (`attrs.meta.summary`, `attrs.sections[0].body`) went out at full length
    while `_truncated_fields` reported nothing about it — the budget was
    enforced on the shape of the data rather than on its size.

    Paths are dotted with bracketed list indices (`meta.summary`,
    `sections[0].body`), so a caller can name the field it wants in full.
    """
    if isinstance(value, dict):
        return {
            key: _truncate_attr_value(
                item,
                max_chars=max_chars,
                path=f"{path}.{key}" if path else str(key),
                cut=cut,
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _truncate_attr_value(
                item,
                max_chars=max_chars,
                path=f"{path}[{index}]",
                cut=cut,
            )
            for index, item in enumerate(value)
        ]
    if isinstance(value, str) and len(value) > max_chars:
        # NOT a slice. Attribute values on an untrusted node are wrapped
        # between fence markers by `provenance._fence_attrs` before they reach
        # here, and a plain `value[:n]` cuts the closing marker off — leaving an
        # unterminated untrusted block whose remaining producer text then reads
        # as trusted content. `cap_preview_text` preserves the opener and the
        # closer, shortens only the body, and re-escapes any marker inside what
        # it keeps so a forged early closer cannot end the block.
        # The budget counts source text only (issue #2670): a fenced value
        # whose body fits comes back unchanged and is not reported as cut.
        from okg.substrate.mcp.search import cap_preview_text

        capped = cap_preview_text(value, max_chars=max_chars)
        if capped != value:
            cut.append(path)
        return capped
    return value


def _preview_truncate_results(
    payload: dict[str, Any],
    *,
    max_chars: int = PREVIEW_TRUNCATE_CHARS,
) -> dict[str, Any]:
    """Default prose fields to a `max_chars` preview (budget-the-pact-loop
    req.budget.machine-surfaces). A truncated preview beats a full drop:
    the agent usually needs the gist, and a second call with an explicit
    select fetches full text when it does not. Truncation is marked,
    never silent.

    "Marked" now means marked IN THE ENVELOPE as well as inside the row. The
    previous shape wrote `attrs._truncated_fields` and a `prose_preview` hint
    and left `truncated` at `false`, so a response with shortened attribute
    text was, at the envelope level, indistinguishable from a complete one —
    which is the failure this whole budget contract exists to prevent
    (issue #1444).
    """
    results = payload.get("results")
    if not isinstance(results, list):
        return payload
    rows_truncated = 0
    fields_truncated = 0
    for row in results:
        attrs = row.get("attrs") if isinstance(row, dict) else None
        if not isinstance(attrs, dict):
            continue
        cut: list[str] = []
        for key in list(attrs):
            attrs[key] = _truncate_attr_value(
                attrs[key],
                max_chars=max_chars,
                path=str(key),
                cut=cut,
            )
        if cut:
            # Unchanged key and unchanged shape for the top-level case, so
            # existing readers keep working; nested cuts appear as paths.
            attrs["_truncated_fields"] = sorted(cut)
            rows_truncated += 1
            fields_truncated += len(cut)
    if rows_truncated:
        payload["prose_preview"] = (
            f"string attrs over {max_chars} chars are truncated by default; "
            "pass select=[...] naming the fields you need in full"
        )
        # The structural statement. Distinct from `truncated` (more rows exist)
        # and from `truncation_reason` (the byte budget shrank the page), both
        # of which keep their meanings untouched.
        payload["attribute_truncation"] = {
            "applied": True,
            "reason": ATTRIBUTE_TRUNCATION_REASON,
            "max_chars": max_chars,
            "rows_truncated": rows_truncated,
            "fields_truncated": fields_truncated,
        }
    return payload


# The subject resolver is shared by every MCP path (issue #2343) and lives in
# ``okg.substrate.nomos.mcp_subject`` so ``run_sql`` and ``source_lifecycle``
# can use it without importing this module. The private names stay bound here
# for the callers and tests that already use them.
MCP_PRINCIPAL_UNRESOLVED = _mcp_subject.MCP_PRINCIPAL_UNRESOLVED
_MCP_PRINCIPAL_UNRESOLVED_HINT = _mcp_subject.MCP_PRINCIPAL_UNRESOLVED_HINT
_MCP_PRINCIPAL_UNRESOLVED_MESSAGE = _mcp_subject.MCP_PRINCIPAL_UNRESOLVED_MESSAGE


def _resolve_mcp_read_subject(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
) -> str:
    """The read boundary's subject: the shared resolver, or a named refusal.

    A failure to read the active policy while resolving is an authority
    evaluation failure (``authority_evaluation_failed`` /
    ``authority_unavailable``), never a quiet switch to another subject.
    """
    return _mcp_subject.require_mcp_subject(
        conn, deployment_name=deployment_name, surface="mcp.tool.call",
    )


def resolve_principal_mapping(deployment_name: str) -> Any:
    """The deployment's declared header-to-claim mapping, or None.

    Manifest-only (no database), so a caller can decide whether multi-user
    mode is even declarable before doing any DB work. A deployment that has
    not resolved at all yields None rather than raising: the single-operator
    path must keep working for ad-hoc servers with no manifest, and
    multi-user mode refuses separately on a None mapping.
    """
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )
    from okg.substrate.mcp.principal_mapping import principal_mapping_from_manifest

    try:
        manifest = resolve_deployment(deployment_name)
    except DeploymentResolutionError:
        return None
    return principal_mapping_from_manifest(manifest)


def resolve_oidc_auth(deployment_name: str) -> Any:
    """The deployment's declared ``nomos.oidc_auth`` block, or None.

    Manifest-only, like :func:`resolve_principal_mapping`, so ``run_server``
    can refuse an undeclared OIDC mode before any network or database work.
    """
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )
    from okg.substrate.mcp.oidc_auth import oidc_auth_from_manifest

    try:
        manifest = resolve_deployment(deployment_name)
    except DeploymentResolutionError:
        return None
    return oidc_auth_from_manifest(manifest)


def _persist_principal_refusal(
    conn: psycopg.Connection,
    *,
    error: SubstrateError,
    deployment_name: str,
    surface: str,
    operator_name: str,
    session: Any = None,
    resource: Mapping[str, Any] | None = None,
) -> None:
    """Record one principal refusal in the audit trail, then get out of the way.

    Called on the raising path, so it must never replace the refusal with an
    error of its own: a failure to WRITE the audit row is not a reason to
    turn a refusal into a 500, and the caller is about to raise the real
    refusal regardless. The row itself is the fail-closed artifact -- the
    request is refused whether or not it lands.
    """
    from okg.substrate.mcp.principal_mapping import (
        HEADER_IDENTITY_CHANNEL,
        persist_principal_refusal_decision,
        refusal_reason_code,
    )

    reason_code = refusal_reason_code(error)
    if reason_code is None:  # pragma: no cover - defensive
        return
    data = getattr(error, "data", None) or {}
    try:
        persist_principal_refusal_decision(
            conn,
            deployment_name=deployment_name,
            reason_code=reason_code,
            action=surface,
            scope=f"deployment:{deployment_name}",
            resource={
                "deployment": deployment_name,
                "scope": f"deployment:{deployment_name}",
                "operator_name": operator_name,
                "resource_kind": "mcp_principal_refusal",
                **{
                    key: value
                    for key, value in dict(resource or {}).items()
                    # Keep the identity-bearing resource small and stable:
                    # audit coordinates belong on the target, not in the hash.
                    if key not in {"generation_id", "branch_name", "catalog_version_id"}
                },
            },
            target={
                "kind": "mcp_principal_refusal",
                "tool": operator_name,
                "surface": surface,
                "generation_id": getattr(session, "generation_id", None),
                "branch_name": getattr(session, "branch_name", None),
                "catalog_version_id": getattr(session, "catalog_version_id", None),
            },
            channel=str(data.get("channel") or "") or None,
            identity_channel=str(
                data.get("identity_channel") or HEADER_IDENTITY_CHANNEL
            ),
        )
    except Exception:  # noqa: BLE001 — audit write must not mask the refusal it records.
        log.warning(
            "failed to persist MCP principal refusal (%s) for deployment %r",
            reason_code,
            deployment_name,
            exc_info=True,
        )


def _require_chat_login_agreement(deployment_name: str, oidc_config: Any) -> None:
    """Refuse OIDC mode when the deployment's chat login names another provider.

    Read from the manifest on disk, like :func:`resolve_oidc_auth`. A
    deployment without a chat block, or without ``chat.oidc_login``, is not
    checked: tokens may come from any client of the declared provider.
    """
    from okg.substrate import fastyaml
    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.deployments.chat_block import parse_chat_block
    from okg.substrate.mcp.oidc_auth import require_login_blocks_agree

    dep_dir = find_deployment_dir(deployment_name)
    manifest = (dep_dir / "deployment.yaml") if dep_dir else None
    if manifest is None or not manifest.is_file():
        return
    raw = fastyaml.safe_load(manifest.read_text()) or {}
    chat = parse_chat_block(raw, manifest_path=manifest)
    require_login_blocks_agree(
        oidc_config, getattr(chat, "oidc_login", None), deployment=deployment_name,
    )


def _request_identity_claim(principal_mapping: Any) -> dict[str, Any]:
    """This request's identity claim, from the channel the mode declares.

    Header mode reads the declared principal header, vouched for by the
    transport token (PACT mcp-principal-mapping). OIDC mode reads the bearer
    token this server verified itself (PACT mcp-oidc-auth). Both build the same
    claim shape, so provider normalization, subject binding and the refusals
    after this point are shared -- and the tool gate, the resource gate and
    the read path all ask this one function, so a mode cannot be wired into
    one of them and forgotten in another.
    """
    from okg.substrate.mcp.principal_mapping import (
        current_request_headers,
        current_transport_identity,
        identity_claim_for_request,
    )

    try:
        return identity_claim_for_request(
            principal_mapping,
            headers=current_request_headers(),
            transport_identity=current_transport_identity(),
        )
    except SubstrateError as exc:
        _stamp_identity_channel(exc, principal_mapping)
        raise


def _identity_channel_for(principal_mapping: Any) -> str:
    """How this server's requests carry identity: OIDC bearer or header."""
    from okg.substrate.mcp.oidc_auth import OidcAuthConfig
    from okg.substrate.mcp.principal_mapping import (
        HEADER_IDENTITY_CHANNEL,
        OIDC_IDENTITY_CHANNEL,
    )

    if isinstance(principal_mapping, OidcAuthConfig):
        return OIDC_IDENTITY_CHANNEL
    return HEADER_IDENTITY_CHANNEL


def _stamp_identity_channel(error: SubstrateError, principal_mapping: Any) -> None:
    data = getattr(error, "data", None)
    if isinstance(data, dict):
        data.setdefault("identity_channel", _identity_channel_for(principal_mapping))


def _resolve_principal_to_subject(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    principal_mapping: Any,
    operator_name: str,
    surface: str,
    resource_kind: str,
    target_kind: str,
) -> Any:
    """Bind this request's claim to a Nomos SUBJECT, or raise.

    The one implementation both multi-user entry points use -- the tool gate
    below and ``_require_resource_principal`` inside ``build_server``. It was
    two implementations until an adversarial review found the second one was a
    SHAPE check: it called ``header_claim_for_request`` and stopped, so a
    well-formed header naming a principal bound to nothing
    (``X-OKG-Principal: nobody@example.edu``) was served the schema digest and
    the read-side metadata. ``req.mcp-principal-mapping.fail-closed-principal``
    says a principal that maps to no Nomos subject MUST be refused, with no
    resource carve-out and ``waivers: []``, so the resource path now goes all
    the way to a subject too. Sharing the function is the point: a gate that
    exists twice is a gate that gets fixed once.
    """
    from okg.substrate.mcp.principal_mapping import (
        blocked_principal_error,
        unidentified_principal_error,
    )
    from okg.substrate.nomos import evaluate_identity_claim_from_postgres
    from okg.substrate.nomos.identity import BLOCKED_REASON_CODE

    claim = _request_identity_claim(principal_mapping)
    identity_channel = _identity_channel_for(principal_mapping)
    try:
        result = evaluate_identity_claim_from_postgres(
            conn,
            deployment_name=deployment_name,
            bundle=None,
            claim=claim,
            action=surface,
            resource={
                "deployment": deployment_name,
                "scope": f"deployment:{deployment_name}",
                "operator_name": operator_name,
                "resource_kind": resource_kind,
            },
            scope=f"deployment:{deployment_name}",
            enforcement_point="mcp.principal_gate",
            target={
                "kind": target_kind,
                "tool": operator_name,
                "surface": surface,
            },
        )
        conn.commit()
    except SubstrateError:
        raise
    except Exception as exc:
        # Evaluation FAILED (a severed pooled connection, say). That is
        # not "the principal did not bind" -- reporting it as an identity
        # refusal would blame the caller for the server's problem.
        raise _authority_evaluation_error(surface, exc) from exc
    if result.decision.reason_code == BLOCKED_REASON_CODE:
        raise blocked_principal_error(
            data={
                "tool": operator_name,
                "identity_reason_code": result.decision.reason_code,
                "binding_ids": list(result.binding_ids),
                "identity_channel": identity_channel,
            },
        )
    if not result.resolved_subject:
        raise unidentified_principal_error(
            (
                "the verified token's user and roles map to no Nomos subject "
                "for this deployment"
                if identity_channel != "declared_principal_header"
                else "the principal asserted in the declared header maps to no "
                "Nomos subject for this deployment"
            ),
            channel="no_subject_binding",
            data={
                "tool": operator_name,
                "identity_reason_code": result.decision.reason_code,
                "identity_channel": identity_channel,
            },
            fix_hint=(
                "bind the principal to a subject with `okg nomos binding "
                "add` (`okg nomos binding list` shows disabled and expired "
                "bindings), or correct the principal the fronting "
                "application sends"
            ),
        )
    return result


def _with_multi_user_principal_gate(
    traced: Any,
    *,
    pool: Any,
    deployment_name: str,
    principal_mapping: Any,
) -> Any:
    """Wrap the tracer so no tool runs until a principal RESOLVES.

    Returns a drop-in replacement for ``traced``: same
    ``traced(tool_name)(fn)`` shape, with the principal check in front of the
    traced call. Composing it here rather than into the read path is what
    makes "no principal, no tool call" true by construction -- several
    ``inspect`` kinds return metadata before any session exists, and the
    change-observation kinds build a Session directly through
    ``_open_logical``, so a check that lived only in ``_PooledSession`` would
    leave both reachable.

    The check goes all the way to a SUBJECT, not just to a well-formed
    header. A shape check would accept ``X-OKG-Principal: nobody@evil.test``
    and hand the session-less kinds their answer -- "maps to no Nomos subject
    MUST be REFUSED" is a requirement about binding, not about syntax.

    Everything the check reads is memoized for the rest of the request (the
    scope opened here is the one ``_PooledSession`` joins), so the pooled
    tools' own boundary evaluation re-uses these loads instead of repeating
    them, and its identical ``mcp.tool.call`` decision is content-addressed to
    the same row.
    """

    def _resolve_or_refuse(conn: psycopg.Connection, tool_name: str) -> None:
        """Bind this request's claim to a subject, or raise."""
        _resolve_principal_to_subject(
            conn,
            deployment_name=deployment_name,
            principal_mapping=principal_mapping,
            operator_name=tool_name,
            surface="mcp.tool.call",
            resource_kind="mcp_tool",
            target_kind="mcp_tool_call",
        )

    def _gate(tool_name: str) -> Any:
        traced_decorator = traced(tool_name)

        def _decorate(fn: Any) -> Any:
            traced_fn = traced_decorator(fn)
            signature = inspect.signature(fn)

            @wraps(fn)
            def _guarded(*args: Any, **kwargs: Any) -> Any:
                # One memo scope per tool call. Opened here rather than in
                # _PooledSession so the gate's own reads are shared with the
                # rest of the request; _PooledSession's scope is reentrant and
                # joins this one.
                with read_pass_scope(dedup_identity_claims=True):
                    conn = None
                    try:
                        conn = _pool_getconn(pool)
                        # Channel conflict is decided BEFORE the header is
                        # read, per acc.mcp-principal-mapping.identity-
                        # channel-precedence ("both alone and alongside a
                        # valid header"). NOTE: the same requirement's prose
                        # STATEMENT says the header-missing case should give
                        # the unidentified-principal refusal instead. The two
                        # clauses contradict each other; the acceptance
                        # clause is the one that closes the requirement, so
                        # it is the one implemented, and the conflict is
                        # recorded rather than silently resolved.
                        #
                        # Bound through the signature so a POSITIONAL
                        # `authority` is caught too: kwargs.get would report a
                        # missing header to a caller whose actual mistake was
                        # supplying its own identity.
                        # One rule in one place (#3666, N1): the live-graph
                        # resolver asks the same check before any upstream
                        # call (`_okg_identity_precheck`).
                        refuse_conflicting_identity_channel(
                            tool_name,
                            _bound_authority(signature, args, kwargs),
                            principal_mapping,
                        )
                        _resolve_or_refuse(conn, tool_name)
                    except SubstrateError as exc:
                        if conn is not None:
                            _persist_principal_refusal(
                                conn,
                                error=exc,
                                deployment_name=deployment_name,
                                surface="mcp.tool.call",
                                operator_name=tool_name,
                            )
                        raise
                    finally:
                        if conn is not None:
                            pool.putconn(conn)
                    return traced_fn(*args, **kwargs)

            return _guarded

        return _decorate

    return _gate


def refuse_conflicting_identity_channel(
    tool_name: str, authority: Mapping[str, Any] | None, principal_mapping: Any,
) -> None:
    """Refuse an `authority` that carries its own identity claim on a server
    whose requests carry identity in their channel (header or token).

    Shared by the per-tool gate (`_gate`) and the live-graph resolver, which
    asks it before any upstream call (#3666, N1).
    """
    from okg.substrate.mcp.principal_mapping import (
        conflicting_identity_channel_error,
    )

    if _nomos_identity_claim_from_authority(authority) is not None:
        raise conflicting_identity_channel_error(
            data={
                "tool": tool_name,
                "identity_channel": _identity_channel_for(principal_mapping),
            },
        )


def _bound_authority(
    signature: Any,
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    """The call's ``authority`` argument, however it was passed.

    ``kwargs.get("authority")`` misses a positional call, which would make the
    channel-conflict refusal keyword-only -- a security check that depends on
    the caller's calling convention is not a check.
    """
    if "authority" not in signature.parameters:
        return None
    try:
        bound = signature.bind_partial(*args, **kwargs)
    except TypeError:
        # Let the real call raise the real signature error.
        return kwargs.get("authority")
    value = bound.arguments.get("authority")
    return value if isinstance(value, Mapping) else None


#: Launch-time opt-in for a single-user server to evaluate caller-supplied
#: identity claims. Unset, such a claim is refused.
TRUST_CALLER_IDENTITY_CLAIMS_ENV = "OKG_MCP_TRUST_CALLER_IDENTITY_CLAIMS"


def caller_identity_claim_untrusted_error() -> SubstrateError:
    err = SubstrateError(
        "authority.nomos_identity_claim was supplied by the caller, and this "
        "server has no way to check it: nothing verifies a signature on it, "
        "so any caller could name any identity. Refusing it rather than "
        "evaluating a self-asserted claim",
        data={"reason": "caller_identity_claim_untrusted"},
        fix_hint=(
            "drop authority.nomos_identity_claim; for per-user identity run "
            "the server with --oidc-auth or --multi-user-principals. An "
            "operator who accepts that every caller may choose its identity "
            f"can start a single-user server with {TRUST_CALLER_IDENTITY_CLAIMS_ENV}=1"
        ),
    )
    err.code = "nomos_identity_claim_untrusted"
    return err


def _strip_server_owned_authority(authority: Mapping[str, Any]) -> dict[str, Any]:
    """Drop every ``_nomos*`` key a caller put in ``authority``.

    The server writes ``_nomos_decision`` and ``_nomos_identity`` after it
    evaluates Nomos; they are stored with the admission and echoed in the
    response. A caller-planted value would be stored and echoed as if the
    server had decided it.
    """
    return {
        key: value
        for key, value in dict(authority or {}).items()
        if not str(key).startswith("_nomos")
    }


def _evaluated_classification(
    surface: str,
    authority: Mapping[str, Any] | None,
) -> str:
    """The classification Nomos evaluates for ``surface`` (issue #2891).

    The server-side class is the surface's Nomos mapping, the same class the
    runtime path uses when no classification is passed. The client's
    ``authority.classification`` may only raise it; an unknown or
    incomparable value raises ``DeclaredClassificationInvalid``.
    """
    from okg.substrate.nomos.classification_order import (
        effective_classification,
    )
    from okg.substrate.nomos.vocabulary import surface_mapping_for

    return effective_classification(
        server_classification=surface_mapping_for(surface).classification,
        declared=(authority or {}).get("classification"),
        surface=surface,
    )


def _pre_evaluation_refusal(
    action: str,
    exc: SubstrateError,
) -> dict[str, Any]:
    """Record/propose evidence for a client input refused before Nomos runs.

    Used for a refused declared classification and for a client attempt to
    choose the policy bundle, scope, or resource facts. The refusal comes
    before evaluation, so no verdict exists
    (``evaluated: false``, issue #950).
    """
    return {
        "action": action,
        "evaluated": False,
        "nomos": {
            "code": exc.code,
            "message": exc.message,
            "data": dict(exc.data),
            **({"fix_hint": exc.fix_hint} if exc.fix_hint else {}),
        },
    }


def _nomos_identity_claim_from_authority(
    authority: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if not authority:
        return None
    if "nomos_identity_claim" in authority:
        raw_claim = authority["nomos_identity_claim"]
    elif "identity_claim" in authority:
        raw_claim = authority["identity_claim"]
    else:
        return None
    if raw_claim is None:
        return None
    if not isinstance(raw_claim, Mapping):
        err = SubstrateError("nomos_identity_claim must be an object")
        err.code = "nomos_identity_claim_invalid"
        raise err
    return dict(raw_claim)


def _nomos_access_context_from_authority(
    authority: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if not authority:
        return None
    if "nomos_access_context" in authority:
        raw_context = authority["nomos_access_context"]
    elif "access_context" in authority:
        raw_context = authority["access_context"]
    else:
        return None
    if raw_context is None:
        return None
    if not isinstance(raw_context, Mapping):
        err = SubstrateError("nomos_access_context must be an object")
        err.code = "nomos_access_context_invalid"
        raise err
    return dict(raw_context)


def _parse_auto_advance(spec: str) -> tuple[str, int]:
    """Parse the legacy lookup mode; construction retires ``when_behind``."""
    if spec == "never":
        return ("never", 0)
    if spec == "always":
        return ("always", 0)
    if spec.startswith("when_behind_by:"):
        try:
            n = int(spec.split(":", 1)[1])
        except ValueError:
            raise ValueError(
                f"auto_advance: 'when_behind_by:<N>' requires an "
                f"integer N; got {spec!r}"
            )
        if n < 1:
            raise ValueError(
                f"auto_advance: 'when_behind_by:<N>' requires N >= 1; got {n}"
            )
        return ("when_behind_by", n)
    raise ValueError(
        f"auto_advance must be one of 'never', 'always', "
        f"'when_behind_by:<N>'; got {spec!r}"
    )


def _bounded_int(value: Any, *, default: int, lower: int, upper: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(lower, min(parsed, upper))


#: The ad-hoc deployment name. It has no manifest by design — `okg mcp-serve
#: --dsn ...` uses it — so an unresolvable manifest under this name is the
#: normal state, not a signal.
_ADHOC_DEPLOYMENT_NAME = "default"


def _tool_allowlist_from_env() -> set[str] | None:
    raw = os.environ.get("OKG_MCP_TOOL_ALLOWLIST")
    if raw is None:
        return None
    return {part.strip() for part in raw.split(",") if part.strip()}


def _launch_consent_actor(deployment_name: str) -> str:
    """Server-derived deciding actor for write-mode (launch-consent) admission
    decisions. The caller-supplied authority.actor / payload.actor is NEVER
    honored: a write-mode MCP caller must not be able to forge the deciding
    authority (e.g. actor='operator'). Resolves to the configured operator
    identity (OKG_OPERATOR_EMAIL) when set, else a launch-consent label that
    records the real provenance (harden-mcp-write-boundaries)."""
    operator = (os.environ.get("OKG_OPERATOR_EMAIL") or "").strip()
    if operator:
        return f"operator:{operator}"
    return f"operator:mcp-launch-consent:{deployment_name}"


def _sanitize_caller_admission_mode(authority: dict[str, Any]) -> dict[str, Any]:
    """A write-mode MCP caller MUST NOT self-assert (a) an auto-accept admission
    mode (fast_track) to bypass review, nor (b) the branch its write lands on.
    admission_mode/trust are server- or registry-derived; a caller-asserted
    fast_track in the authority blocks is downgraded to 'review' so a submit
    stays pending review. target_branch_name is SERVER-pinned (from
    `mcp-serve --branch`) and re-injected downstream by
    `_submit_admission_payload`; a caller value is dropped here so an agent on a
    default server cannot route its write onto an arbitrary fork branch
    (harden-mcp-write-boundaries / worktree-fork-writes P2)."""
    out = dict(authority or {})
    out.pop("target_branch_name", None)
    for key in ("producer_policy", "admission_policy"):
        block = out.get(key)
        if isinstance(block, Mapping):
            block = dict(block)
            if str(block.get("admission_mode") or "").lower() == "fast_track":
                block["admission_mode"] = "review"
            out[key] = block
    if str(out.get("admission_mode") or "").lower() == "fast_track":
        out["admission_mode"] = "review"
    return out


def _normalize_promote_intent(
    authority: dict[str, Any],
    branch_name: str,
) -> dict[str, Any]:
    """Normalize a caller's promote-intent (worktree-fork-writes req.promote-intent).

    An agent may mark its FORK write ``"durable"`` -- a candidate for the P3
    merge-back -- versus the default ``"scratch"`` (fork-local working memory that
    dies with the fork). DENY-BY-DEFAULT: strip any caller-supplied value and
    re-inject ``promote_intent="durable"`` ONLY for an explicit durable FORK write;
    everything else (scratch, an unrecognized value, or ANY default-branch write)
    carries no marker, so the merge-back -- which scopes to
    ``promote_intent='durable'`` -- never picks it up. The marker is stamped onto
    the fork facts in ``_emit_review_payload_facts``.
    """
    caller_intent = str(authority.get("promote_intent") or "").strip().lower()
    out = {k: v for k, v in authority.items() if k != "promote_intent"}
    if branch_name and branch_name != "default" and caller_intent == "durable":
        out["promote_intent"] = "durable"
    return out


def _seg(text: str, *requires: str) -> InstructionSlot:
    """One phrasing, served only when every tool it names is served."""
    return InstructionSlot((InstructionSegment(frozenset(requires), text),))


def _alt(*variants: tuple[tuple[str, ...], str]) -> InstructionSlot:
    """Alternative phrasings, first servable one wins.

    How a sentence degrades to a narrower TRUE statement under a narrowed
    surface instead of being dropped whole — the `search`/`filter` pairing
    below is the case that motivates it.
    """
    return InstructionSlot(
        tuple(
            InstructionSegment(frozenset(requires), text) for requires, text in variants
        )
    )


# Node lookup contract (expose-mcp-node-lookup, GitHub #641). Shared by both
# instruction variants so a least-privilege reader always sees the sanctioned
# path/attrs lookup and the not-found signal.
#
# Written as SEGMENTS rather than one string (PACT mcp-tool-surface-profiles,
# issue #1457): each sentence declares the tools it names, so a narrowed
# server drops the sentences it is not entitled to say instead of instructing
# a model to call a tool it just pruned.
NODE_LOOKUP_SLOTS: tuple[InstructionSlot, ...] = (
    # Issue #2676: this slot used to say paths are NOT in node text, which
    # is false for the subtypes that declare
    # `okg_searchable_text_path_fields` (`source_file`, `document`): the
    # write-time stamper appends the path split into words.
    _alt(
        (
            ("search",),
            # PACT dataset-card: whether path words match lexical search is
            # stated by the dataset card, and only when a start-up check on
            # this server's own rows saw it (`dataset_card.PathProbe`).
            "NODE LOOKUP: lexical search matches node text; `source_file` "
            "and `document` nodes carry their path in `attrs.path`. ",
        ),
        (
            ("filter",),
            "NODE LOOKUP: `source_file` and `document` nodes carry their path "
            "in `attrs.path`. ",
        ),
    ),
    _seg(
        "To resolve a node by attrs, call search with only a `where` naming "
        'a type: `search(where={"type": "source_file", "attrs.path": '
        '"src/app.py"})` finds a file node by exact path; '
        '`"attrs.name"` finds symbols by name. ',
        "search",
    ),
    _seg(
        "For a path "
        'tail, use `filter(where={"type": "source_file", "attrs.path": '
        '{"op": "suffix", "value": "dir/file.py"}})`. ',
        "filter",
    ),
    _seg(
        # Restored at the orchestrator's request (PACT
        # publish-paging-args-3666); step 3a had dropped it. Its own effect
        # on agent results is not measured.
        "Node ids are deterministic: `file:<repo>:<path>`, "
        "`commit:<repo>:<sha>`, `doc:<repo>:<path>` (a parsed file's text); "
        'a known id goes straight to `inspect(target={"kind": "node", '
        '"node_id": "<id>"})`. ',
        "inspect",
    ),
    _seg(
        # D8 prompt redesign: "at the pin" was generation jargon.
        "inspect on an id the graph lacks fails with `node_not_found` "
        "(`exists: false`); a found node with sparse attrs still returns "
        "its payload.",
        "inspect",
    ),
)

#: The node-lookup contract as the FULL read surface serves it. Both
#: instruction variants embed this exact text; a narrowed operator surface
#: renders the same slots against its own served set instead.
NODE_LOOKUP_INSTRUCTIONS = render_instructions(
    (NODE_LOOKUP_SLOTS,),
    served=OPERATOR_TOOLS,
)

# Reading contract (issue #2676). Measured on the CMS CompOps benchmark:
# agents read chunks one `inspect` at a time, asked for `attrs.text` and a
# one-hop `target_subtype` (both refused), and never used the path-shaped
# expand that returns every chunk with its text. Issue #2668 then replaced
# that path expand as the whole-document read: it returns chunks in id
# order, is refused over the byte budget with no cursor, and returns nothing
# under Nomos enforce, while `inspect` kind "text" reads in chunk order,
# pages by bytes and filters per chunk. Each sentence names only
# the tools it requires; `tools_named_in` is a word scan, so the wording
# also avoids every tool name (including `record`) outside its own slot.
#
# PACT read-passage-rows-3666 (R8): `select=["node_id","attrs"]` is no longer
# taught as the way to a chunk's full text, nor `select`'s field shape: agents
# selected `attrs` on most searches and got a record's body twice and none of
# its comments. A search row shows the matched passage, the reply names the
# exact read call once (D5); full text is the `inspect` text read below.
# A ticket's text read prints fields, description and dated comments (R6), not
# order_basis/coverage/heading_path, so that sentence names documents only.
READING_SLOTS: tuple[InstructionSlot, ...] = (
    _seg(
        "READING: lexical search returns rows matching any search term, "
        "ranked by BM25 with English stemming. Each row shows the matched "
        "passage, and the reply names the exact call that reads a record; "
        "`max_chars` (up to 4096) lengthens passages. ",
        "search",
    ),
    _seg(
        "For full text, read a whole document (or any node that `contains` "
        "chunks) in reading order: `inspect(target={\"kind\": \"text\", "
        '"node_id": "doc:<repo>:<path>"})` (pages of up to max_bytes, '
        "default 65536; a cut page says how much is left), then "
        "`inspect(next=<token>)` until a page ends `complete`. On a "
        "document, `order_basis` says how chunks were "
        "ordered and `coverage` whether any are missing; each item's "
        "`heading_path` places it in its section. A ticket reads as its "
        "fields, its description and its dated comments; a cut thread names "
        "the call that reads the rest. A `file:` node's text is "
        "only its path; inspecting it lists its `doc:` nodes under "
        "`documents`: read those. ",
        "inspect",
    ),
    _seg("A one-hop expand returns the linked records. ", "expand"),
    _seg(
        "Stop once every part of the question is answered from graph text "
        "you can cite; page on only for a complete count or list."
    ),
)

#: What `inspect(session_start)` keeps of the surface readback (#3470): the
#: running code and the instructions it served. The rest is
#: `inspect(kind="surface")`.
_SESSION_START_SURFACE_KEYS: tuple[str, ...] = (
    "deployment_name", "package_version", "git_revision",
    "instructions_hash", "instructions_bytes",
    "untrusted_data_contract_present", "dataset_card_state",
)

DEFAULT_INSTRUCTIONS = (
    "You have access to an Operational Knowledge Graph (OKG) via "
    "the substrate's pinned-read interface. Every tool reads the "
    "graph at a published `generation_id`. If you omit or pass null "
    "`generation_id`, the first graph call binds the latest published "
    "generation to this logical MCP session and later omitted calls "
    "inherit it. A non-null generation overrides only that call. Use "
    '`inspect(target={"kind":"session","action":"refresh"})` '
    "to deliberately replace the session default with latest. Every "
    "response reports its pin scope and logical session id; a request-"
    "scoped warning means the transport cannot preserve an omitted pin. "
    '`inspect(target={"kind":"graph"})` '
    "returns a bounded overview (subtype/edge-type counts + schema "
    "digest) — use it to learn what the graph contains, not to fetch "
    "data. To find or read specific things use "
    "`search(method='lexical')` for text queries, "
    "`search(method='alias')` for exact abbreviation lookup, and "
    "`search(method='semantic')` for similarity search.\n\n"
    + NODE_LOOKUP_INSTRUCTIONS
    + "\n\n"
    + UNTRUSTED_DATA_CONTRACT
    + " "
    + RESULT_TEXT_INSTRUCTION
)

OPERATOR_INSTRUCTION_PARAGRAPHS: tuple[tuple[InstructionSlot, ...], ...] = (
    (
        # PACT dataset-card: okg-internal prose (the surface's internal name,
        # generation pinning mechanics, session rebinding) is gone. What the
        # graph holds is the dataset card, served above this text.
        _seg(
            # Generated from the SERVED set, never a constant list: the
            # measured defect was a narrowed server still naming expand,
            # filter, map, and query after pruning them.
            "This server serves exactly these tools: {tools}. "
        ),
        _seg(
            '`inspect(target={"kind":"graph"})` returns a bounded '
            "overview (counts + schema digest) of what the graph contains. ",
            "inspect",
        ),
        # Prompt redesign (#3666): "Use search/filter to fetch specific
        # nodes" is gone; NODE LOOKUP and READING say how, per tool.
        # #3666: served `run` was never called while agents repeated one
        # call per value; only a launch that serves `run` says what it is
        # for, and points at `query` only where `query` is served.
        _alt(
            (
                ("run", "query"),
                "Use `run` to build a list or a count over many records; in "
                'one run call "$1" names step 1\'s records. `query` is for '
                "what run cannot express. ",
            ),
            (
                ("run",),
                "Use `run` to build a list or a count over many records; in "
                'one run call "$1" names step 1\'s records. ',
            ),
        ),
    ),
    NODE_LOOKUP_SLOTS,
    READING_SLOTS,
    # Always served. The trust boundary is not a convenience the narrowing
    # may drop: a reader that loses it treats attacker-influenced node text
    # as instructions.
    # The result-text line rides with it (issue #3059): it is how a model
    # reads an escaped line or a quoted value in every text result.
    (_seg(UNTRUSTED_DATA_CONTRACT + " " + RESULT_TEXT_INSTRUCTION),),
)


def operator_instructions_for(served_tools: Iterable[str]) -> str:
    """Render the operator instructions for exactly this served tool set."""
    return render_instructions(
        OPERATOR_INSTRUCTION_PARAGRAPHS,
        served=served_tools,
    )


#: The instructions an UNNARROWED operator surface serves. A narrowed launch
#: renders `operator_instructions_for(<its served set>)` instead.
OPERATOR_INSTRUCTIONS = operator_instructions_for(OPERATOR_TOOLS)


def authored_instructions_for(
    served_tools: Iterable[str],
    *,
    deployment_text: str = "",
    expansion_text: str = "",
) -> str:
    """The part of the served instructions that files and code determine.

    PACT dataset-card: okg's reading and lookup guidance for the served set,
    the expansion sentence when a ceiling exists, then a deployment's own
    instruction file under `DEPLOYMENT_GUIDANCE_HEADING`. The file is
    appended, never a replacement. The dataset card (compiled from the
    database) is served ABOVE this text and is not part of it, so the fleet
    doctor (`surface_fleet.expected_surface`) can recompute this part, and
    its hash, without a database.
    """
    text = operator_instructions_for(served_tools)
    if expansion_text:
        text += "\n\n" + expansion_text
    deployment_text = deployment_text.strip()
    if deployment_text:
        text += "\n\n" + DEPLOYMENT_GUIDANCE_HEADING + "\n\n" + deployment_text
    return text


def _load_readside_metadata(deployment_name: str) -> dict[str, Any]:
    """Load optional deployment-owned MCP/read-side metadata."""
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )

    empty = {
        "deployment": deployment_name,
        "instructions": "",
        "not_ingested": (),
        "audit_questions": "",
        "skill_triggers": {},
        "skills": {},
        "record_prefixes": (),
        "session_start_context": None,
        "mcp": None,
        # THE declared agent surface, already validated at manifest load.
        # `None` here means "no manifest", which `manifest_unresolved` below
        # distinguishes from "resolved and declares no block".
        "agent_surface": None,
        "agent_surface_dir": None,
        # The resolved deployment directory. Server-internal, like the path
        # above: it anchors the `.okg/up` lifecycle records the read-status
        # resources serve, independent of the server's working directory.
        "deployment_dir": None,
        # The manifest's `contract.lifecycle`, or None. Read by the `mcp`
        # live adapter's credential rule, which refuses a personal upstream
        # credential unless this is `prototype` or `demo`.
        "contract_lifecycle": None,
        # The manifest could not be resolved from THIS checkout. Distinct
        # from "resolved and declares no `mcp:` block": the first cannot
        # honor a declared tool bound and must say so, the second has none
        # to honor. Collapsing them is how a declared narrowing turns into a
        # silently full surface.
        "manifest_unresolved": True,
    }
    try:
        manifest = resolve_deployment(deployment_name)
    except DeploymentResolutionError:
        return empty

    def _read_text(path: Path | None) -> str:
        if path is None or not path.is_file():
            return ""
        return path.read_text(encoding="utf-8", errors="replace")

    triggers: dict[str, Any] = {}
    if manifest.skill_triggers and manifest.skill_triggers.is_file():
        data = fastyaml.safe_load(_read_text(manifest.skill_triggers)) or {}
        if isinstance(data, dict):
            triggers = data

    skills: dict[str, str] = {}
    if manifest.skills_dir and manifest.skills_dir.is_dir():
        for path in sorted(manifest.skills_dir.glob("*.md")):
            skills[path.stem] = _read_text(path)

    return {
        "deployment": deployment_name,
        "instructions": _read_text(manifest.mcp_instructions),
        # PACT dataset-card: what the deployment declares it did not ingest,
        # stated by the dataset card. Server-internal, not published.
        "not_ingested": tuple(getattr(manifest, "not_ingested", ()) or ()),
        "audit_questions": _read_text(manifest.audit_questions),
        "skill_triggers": triggers,
        "skills": skills,
        "record_prefixes": tuple(manifest.record_prefixes),
        "session_start_context": manifest.session_start_context,
        # Deployment-declared MCP tool profiles. Already strictly validated at
        # manifest load; re-parsed here into the typed form the launch needs.
        "mcp": getattr(manifest, "mcp", None),
        "agent_surface": getattr(manifest, "agent_surface", None),
        # The directory the surface's declared prompt and instructions paths
        # resolve against. Carried beside the block rather than recomputed:
        # a sealed deployment's manifest may live outside the checkout, and
        # re-deriving the root from the deployment NAME would read a
        # different tree than the one that was validated.
        "agent_surface_dir": (
            manifest.schema_dir.parent
            if getattr(manifest, "schema_dir", None) is not None else None
        ),
        "deployment_dir": getattr(manifest, "deployment_dir", None),
        "contract_lifecycle": _contract_lifecycle(manifest),
        "manifest_unresolved": False,
    }


#: The fields `okg://readside-metadata/<deployment>` publishes. The rest of
#: `_load_readside_metadata`'s dict (filesystem paths, the `mcp:` tool-profile
#: config, the declared agent surface, contract lifecycle, the
#: manifest-resolution flag, record prefixes, session-start context) is
#: server-internal: it configures this launch and is not JSON-serializable.
READSIDE_PUBLISHED_FIELDS: tuple[str, ...] = (
    "deployment",
    "instructions",
    "audit_questions",
    "skill_triggers",
    "skills",
)


def published_readside_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
    """The documented read-side fields of ``metadata``, and nothing else."""
    return {field: metadata.get(field) for field in READSIDE_PUBLISHED_FIELDS}


def readside_metadata_json(metadata: Mapping[str, Any]) -> str:
    """The `okg://readside-metadata/<deployment>` payload as JSON text.

    `skill_triggers` is deployment-authored YAML: an unquoted date loads as
    `datetime.date`, which `json.dumps` refuses. Such values are written as
    their ISO text (`str()`), so one date in a trigger file cannot fail the
    whole resource. Only the published fields are serialized.
    """
    return _json.dumps(
        published_readside_metadata(metadata),
        separators=(",", ":"),
        default=str,
    )


def _contract_lifecycle(manifest: Any) -> str | None:
    """The manifest's `contract.lifecycle` as declared, or None when absent.

    Raw on purpose: `deployment_contract` validates the block for the
    portfolio audit, and the one caller here only asks whether the value is
    one of two names. An undeclared or malformed lifecycle is None, which
    that caller refuses.
    """
    contract = getattr(manifest, "contract", None)
    if not isinstance(contract, Mapping):
        return None
    lifecycle = contract.get("lifecycle")
    return lifecycle if isinstance(lifecycle, str) else None


def _skill_hints_for_subtype(
    metadata: dict[str, Any],
    *,
    subtype: str,
) -> list[dict[str, Any]]:
    triggers = metadata.get("skill_triggers") or {}
    intents = triggers.get("intents") if isinstance(triggers, dict) else None
    if not isinstance(intents, dict):
        return []
    hints: list[dict[str, Any]] = []
    for intent, body in sorted(intents.items()):
        if not isinstance(body, dict):
            continue
        subtypes = body.get("subtypes") or []
        if "*" not in subtypes and subtype not in subtypes:
            continue
        hints.append(
            {
                "intent": intent,
                "skill": body.get("skill"),
                "description": body.get("description"),
            }
        )
    return hints


POOL_ACQUIRE_TIMEOUT_SECONDS_DEFAULT = 5.0

#: Read-pool connections kept out of the request workers' reach (issue
#: #3493): for work that borrows from the read pool outside a request
#: worker -- live-relation call samples finishing in their own executor after
#: a call gave up on them, and the startup and readiness probes. Nothing on the
#: event loop borrows a connection.
MCP_POOL_RESERVE_CONNECTIONS = 2
#: The most read-pool connections one request worker holds at once. A core
#: read holds its session's connection and, inside it, the freshness stamp's
#: (`_stamp_operator`); the principal gates and the trace and call-log writes
#: each hold one, never alongside the read's.
MCP_CONNECTIONS_PER_WORKER = 2


def tool_call_width(max_conns: int) -> int:
    """How many request workers a server with a ``max_conns`` read pool runs.

    Workers times their peak connections, plus the reserve, fits the pool:
    ``width * MCP_CONNECTIONS_PER_WORKER + MCP_POOL_RESERVE_CONNECTIONS <=
    max_conns``. So every worker can hold its peak at once and none waits
    for a connection another worker holds while waiting itself. At the
    default pool of 16 that is (16 - 2) // 2 = 7. Never below one: a pool
    smaller than the reserve still serves, one call at a time.
    """

    return max(
        1, (max_conns - MCP_POOL_RESERVE_CONNECTIONS) // MCP_CONNECTIONS_PER_WORKER,
    )


def _pool_acquire_timeout() -> float:
    try:
        return max(
            float(
                os.environ.get(
                    "OKG_MCP_POOL_TIMEOUT_SECONDS",
                    str(POOL_ACQUIRE_TIMEOUT_SECONDS_DEFAULT),
                )
            ),
            0.1,
        )
    except ValueError:
        return POOL_ACQUIRE_TIMEOUT_SECONDS_DEFAULT


def _conninfo_field(conninfo: str, field: str) -> str | None:
    try:
        from psycopg.conninfo import conninfo_to_dict

        value = conninfo_to_dict(conninfo).get(field)
        return str(value) if value else None
    except Exception:  # noqa: BLE001 - diagnostics only, never blocks
        return None


def _dsn_database(conninfo: str) -> str:
    return _conninfo_field(conninfo, "dbname") or "<unknown>"


def _build_pool(
    *,
    conninfo: str,
    role: str,
    min_size: int,
    max_size: int,
    open: bool,
    name: str,
) -> ConnectionPool:
    """Per `reduce-agent-friction` req.friction.environment-truth: pool
    acquisition fails fast (default 5s, override
    OKG_MCP_POOL_TIMEOUT_SECONDS) and each connection attempt carries the
    same connect_timeout — never psycopg_pool's blind 30s default, which
    hid the 2026-06-11 okg_mcp stale-password wedge. The role/database are
    stamped on the pool so acquisition failures can name them."""
    timeout = _pool_acquire_timeout()
    # `connection_kwargs` is the substrate connection factory both
    # `db.connection.connect` and `db.pool` route through. Hand-writing this
    # dict skipped the session `search_path` pin (issue #1968), the TCP
    # keepalives and the idle-in-transaction bound, so both MCP pools opened
    # sessions unlike every other session okg opens. It also reads the DSN's
    # own `options` first, so an operator's settings survive.
    pool = ConnectionPool(
        conninfo=conninfo,
        min_size=min_size,
        max_size=max_size,
        timeout=timeout,
        kwargs={
            **connection_kwargs(
                dsn=conninfo, connect_timeout=max(int(timeout), 1),
            ),
            "autocommit": True,
        },
        # Validate connections at checkout: after a DB restart the idle
        # connections are dead, and without a check each one is handed to
        # a caller and burns that call with AdminShutdown (issue #950).
        # The probe costs one round-trip per checkout.
        check=ConnectionPool.check_connection,
        # `run_sql` lets an agent call `pg_advisory_lock_shared` and friends;
        # read-only mode does not stop them. Without this reset such a lock
        # outlived the query on the idle pooled connection.
        reset=release_session_advisory_locks,
        open=open,
        name=name,
    )
    # Name the ACTUAL connecting user in failures (an OKG_MCP_RO_DSN /
    # OKG_MCP_RW_DSN override may carry a different user than the default
    # role for this pool).
    pool._okg_role = _conninfo_field(conninfo, "user") or role
    pool._okg_database = _dsn_database(conninfo)
    pool._okg_timeout = timeout
    return pool


def _pool_acquire_error(pool, exc: Exception) -> SubstrateError:
    role = getattr(pool, "_okg_role", "<unknown>")
    database = getattr(pool, "_okg_database", "<unknown>")
    timeout = getattr(pool, "_okg_timeout", POOL_ACQUIRE_TIMEOUT_SECONDS_DEFAULT)
    return MCPPoolUnavailable(
        f"MCP pool acquisition failed after {timeout:g}s as role {role} "
        f"on database {database}",
        data={
            "role": role,
            "database": database,
            "timeout_seconds": timeout,
            "cause": str(exc)[:160] or type(exc).__name__,
        },
        fix_hint=(
            f"connections as {role} cannot be established: check the "
            "role's password/grants (the 2026-06-11 okg_mcp "
            "stale-password wedge) and that the deployment DSN's server "
            "is up — `okg dogfood status --deployment <d> --json` shows "
            "the runtime state"
        ),
    )


def _pool_getconn(pool):
    from psycopg_pool import PoolTimeout

    try:
        return pool.getconn()
    except PoolTimeout as exc:
        raise _pool_acquire_error(pool, exc) from exc


@contextmanager
def _pool_connection(pool):
    from psycopg_pool import PoolTimeout

    try:
        with pool.connection() as conn:
            yield conn
    except PoolTimeout as exc:
        raise _pool_acquire_error(pool, exc) from exc


def _authority_failure_evidence(action: str, err: Exception) -> dict[str, Any]:
    """Evidence for an authority evaluation that could not run (issue #950).

    No verdict was rendered, so the evidence must NOT claim
    `authorized: false` — it carries `evaluated: false` plus the
    underlying cause. The nested code separates infrastructure causes
    (dead/severed DB connection, pool acquisition failure) from every
    other evaluation failure; `_authority_refusal_code` maps them to
    the distinct top-level codes.
    """
    infra = isinstance(err, (psycopg.OperationalError, psycopg.InterfaceError)) or (
        getattr(err, "code", None) == MCPPoolUnavailable.code
    )
    return {
        "action": action,
        "evaluated": False,
        "nomos": {
            "code": (
                "nomos_authority_unavailable"
                if infra
                else "nomos_authority_evaluation_failed"
            ),
            "message": f"{type(err).__name__}: {err}",
        },
    }


def _authority_failure_code(evidence: Mapping[str, Any]) -> str | None:
    """Return the top-level code for an authority evaluation failure."""
    if evidence.get("evaluated") is not False:
        return None
    nomos = evidence.get("nomos")
    nomos = nomos if isinstance(nomos, Mapping) else {}
    if nomos.get("code") == "nomos_authority_unavailable":
        return "authority_unavailable"
    return "authority_evaluation_failed"


def _authority_evaluation_error(surface: str, err: Exception) -> SubstrateError:
    """Build the fail-closed read error for an unevaluated authority check."""
    evidence = _authority_failure_evidence(surface, err)
    code = _authority_failure_code(evidence)
    assert code is not None
    nomos = evidence["nomos"]
    cause = str(nomos.get("message") or "authority evaluation failed")
    if code == "authority_unavailable":
        message = (
            "read authority could not be evaluated because the deployment "
            f"database connection failed ({cause}); this is an infrastructure "
            "failure, not a policy denial — retry once the deployment "
            "database is reachable"
        )
    else:
        message = (
            "read authority could not be evaluated "
            f"({cause}); no policy verdict was rendered"
        )
    error = SubstrateError(
        message,
        data={"surface": surface, "authority": evidence},
        fix_hint=(
            "retry once the deployment database is reachable"
            if code == "authority_unavailable"
            else "inspect the authority evaluation cause; no policy verdict exists"
        ),
    )
    error.code = code
    return error


def _authority_refusal_code(
    evidence: Mapping[str, Any],
    *,
    verdict_message: str,
) -> tuple[str, str]:
    """Map authority evidence to a top-level (code, message) pair.

    Only a rendered decision may surface as `unauthorized_write`
    (`verdict_message`). Evidence with `evaluated: false` means no
    verdict exists: infrastructure causes surface as
    `authority_unavailable`, anything else as
    `authority_evaluation_failed` — fail-closed either way, but a dead
    database is never dressed as a policy denial (issue #950).
    """
    if evidence.get("evaluated") is False:
        nomos = evidence.get("nomos")
        nomos = nomos if isinstance(nomos, Mapping) else {}
        cause = str(nomos.get("message") or "authority evaluation failed")
        if nomos.get("code") == MCP_PRINCIPAL_UNRESOLVED:
            return (MCP_PRINCIPAL_UNRESOLVED, cause)
        if _authority_failure_code(evidence) == "authority_unavailable":
            return (
                "authority_unavailable",
                "write authority could not be evaluated because the "
                f"deployment database connection failed ({cause}); this "
                "is an infrastructure failure, not a policy denial — "
                "retry once the deployment database is reachable",
            )
        return (
            "authority_evaluation_failed",
            "write authority could not be evaluated "
            f"({cause}); no policy verdict was rendered",
        )
    return ("unauthorized_write", verdict_message)


#: scale-operator-attr-reads / req.effective-attrs-view (Layer 2): the result-size
#: ceiling on the fast filter/map row scan. NOT the Layer-1 10K
#: bounded-Python-overlay cap (a CORRECTNESS limit on a per-row Python merge):
#: this is an index scan, so the ceiling is a memory guard set far above any real
#: page (max page is 1000). A scan returning ceiling+1 rows reports
#: `truncated=true` against this bound -- a TRUE signal, never a silent
#: undercount. Aggregate does NOT use it: it counts every matched row in SQL.
#: At module scope (materialize-effective-attrs-scan-once) so the truncation
#: contract can be tested without seeding 200K rows.
_EFFECTIVE_VIEW_SCAN_CEILING = 200_000

#: lane-h-mcp-filter-index-driven: how long a PASSED interval completeness
#: check for the branch-head generation is reused, in seconds. The key already
#: changes on any new generation; the lifetime bounds how long damage to the
#: head's own interval rows (which no publish path makes, but an operator or a
#: bug could) goes unnoticed: the next call after it expires recounts.
_INTERVAL_COMPLETE_CACHE_TTL_S = 60.0


def _interval_complete_cache_clock() -> float:
    """Monotonic clock for the completeness cache; a seam for lifetime tests."""
    return time.monotonic()


def _build_server(
    *,
    dsn: Optional[str] = None,
    deployment_name: str = "default",
    embedder: Optional[Embedder] = None,
    instructions: str = DEFAULT_INSTRUCTIONS,
    min_conns: int = 2,
    max_conns: int | None = None,
    auto_advance: str = _AUTO_ADVANCE_DEFAULT,
    branch_name: str = "default",
    allow_writes: bool = False,
    dsn_authority: Mapping[str, Any] | None = None,
    auth_token: Optional[str] = None,
    multi_user_principals: bool = False,
    principal_mapping: Any = None,
    oidc_verifier: Any = None,
    tool_profile: Optional[str] = None,
    tool_profile_ceiling: Optional[str] = None,
    live_adapters: Mapping[str, Any] | None = None,
    read_only_release: Mapping[str, Any] | None = None,
    image_source: Any = None,
    image_budget: Any = None,
    one_call: bool = False,
    claim_attempted: list[bool] | None = None,
    dataset_card: bool = True,
) -> FastMCP:
    """Construct a FastMCP server wrapping the substrate tools.

    DSN resolution order:
      named deployment > deployment authority (including explicit dsn
      validation); default/ad-hoc server > explicit dsn kwarg > OKG_DSN env.
      Named deployments never fall back to a local-compose default.

    `embedder` is optional; method='semantic' searches require it.
    Without one, semantic searches raise a readable error naming
    the missing dependency.

    `min_conns` / `max_conns` size the underlying connection pool.

    `one_call=True` is for a caller that serves ONE call and closes the pool
    (the `okg` read commands). Its host-wide connection claim is for exactly
    `max_conns`, the most its pool can open, instead of the declared sum of a
    server that stays up, and no heartbeat thread is started: closing the
    pool releases the claim, and a claim left by a killed process stops
    counting after the ledger's stale horizon. A server that stays up keeps
    the default.

    `dataset_card=False` is also for a one-call caller whose tool states no
    part of the card or of the served instructions (every read operator but
    `inspect`, whose `session_start` and `surface` kinds report them). The
    card's facts are then not read, as when a deployment turns the card off;
    no reply of those tools changes. A server that stays up keeps the
    default.

    `claim_attempted` is `build_server`'s: a list this function appends to
    just before it asks for the claim, so a failed build releases only a
    claim this build could have made.

    `auto_advance` is a legacy policy for omitted calls that have no stable
    transport identity. "never" (default) uses the 500ms latest cache there;
    "always" resolves latest uncached there. Stateful MCP sessions ignore that
    distinction after their first uncached bind. ``when_behind_by:<N>`` is
    retired and refused because it replaced authoritative explicit pins; use
    the public inspect-session refresh action instead.

    `auth_token` (PACT mcp-http-auth) is the resolved bearer credential for
    network transports. When supplied, the constructed FastMCP carries a
    static-shared-secret token verifier, which is what makes
    `streamable_http_app()` wrap the /mcp route in the SDK's
    RequireAuthMiddleware. The verifier is a CONSTRUCTOR-ONLY argument in the
    pinned SDK -- `FastMCP.settings` exposes `auth` but not `token_verifier`,
    so authentication cannot be switched on after construction and must be
    wired here. Callers pass the value, not the variable name; resolution and
    the fail-closed refusal for an unresolvable credential live in
    `okg.substrate.mcp.http_auth.resolve_auth_token`. The value is never
    logged, never echoed into an error payload, and never stored on the
    returned instance outside the verifier's own comparison buffer.

    `multi_user_principals` (PACT mcp-principal-mapping) makes every request
    resolve its OWN principal from the deployment's declared header mapping
    instead of the process-wide subject. It is an EXPLICIT server state: it
    is never inferred from whether a header happened to arrive, so a client
    that stops sending the header cannot silently return the server to
    shared-principal behavior. It requires a declared mapping and an identity
    provider that actually checks verifier evidence, both refused here at
    construction rather than per request. Default off: a server built without
    it behaves exactly as before.
    """
    # Quarantine isolation (make-admission-review-load-bearing): the
    # MCP server is the agent-facing read surface, and the quarantine
    # branch holds untrusted-origin facts awaiting operator approval.
    # Serving it to an agent would defeat the admission hold, so a
    # quarantine-pinned server requires an explicit operator opt-in.
    if (
        branch_name.startswith("quarantine")
        and os.environ.get("OKG_MCP_ALLOW_QUARANTINE_BRANCH") != "1"
    ):
        raise SubstrateError(
            f"refusing to serve quarantine branch {branch_name!r} over "
            "MCP: it holds unreviewed untrusted-origin content",
            fix_hint=(
                "review held runs with `okg admission list --status "
                "pending --held` and apply them with `okg "
                "admission decide`; to inspect the quarantine branch "
                "over MCP anyway (operator-only, never the agent's "
                "server), set OKG_MCP_ALLOW_QUARANTINE_BRANCH=1"
            ),
            data={"branch_name": branch_name},
        )
    advance_mode, _advance_threshold = _parse_auto_advance(auto_advance)
    if advance_mode == "when_behind_by":
        raise ValueError(
            "auto_advance='when_behind_by:N' is retired because it can "
            "silently replace an explicit generation_id; use "
            "inspect(target={'kind':'session','action':'refresh'}) to "
            "deliberately replace a logical session default"
        )
    dsn_resolution = (
        dict(dsn_authority)
        if dsn_authority is not None
        else _server_dsn_resolution(dsn, deployment_name)
    )
    effective_dsn = dsn_resolution.get("dsn")
    if not effective_dsn:
        # Tier R (worktree-access-tiers): the old localhost:5433/okg
        # default served WHATEVER database happened to live there - a
        # silent-wrong-graph fallback (P0 class). Fail fast, name the
        # recovery, never impersonate a deployment.
        raise SubstrateError(
            f"cannot resolve a database for deployment "
            f"{deployment_name!r}; refusing to fall back to a "
            f"local-compose default that may be a different graph",
            data={"authority": dsn_resolution.get("authority")},
            fix_hint=(
                f"okg deployment env doctor {deployment_name} --json shows the "
                "deployment DSN authority; MCP role overrides still come from "
                "OKG_MCP_RO_DSN / OKG_MCP_RW_DSN. Pass --dsn explicitly for "
                "ad-hoc servers"
            ),
        )
    effective_dsn = str(effective_dsn)
    # Least-privilege connection model (PACT least-privilege-mcp): the
    # read pool connects as the non-superuser okg_mcp role (reads, audit-log
    # writes, and its own connection reservation through the
    # okg.connection_reservation_* functions; no table write beyond the
    # audit rows), never the owner/superuser DSN. Writes are
    # operator-gated — disabled unless the operator launched the server
    # with write mode on — and when enabled run over a SEPARATE app_rw
    # pool, so the read connection literally cannot mutate the graph.
    read_dsn = _derive_role_dsn(
        effective_dsn,
        role="okg_mcp",
        env_var="OKG_MCP_RO_DSN",
    )
    allow_writes = bool(allow_writes) or os.environ.get("OKG_MCP_ALLOW_WRITES") in {
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    }
    write_dsn = (
        _derive_role_dsn(
            effective_dsn,
            role="app_rw",
            env_var="OKG_MCP_RW_DSN",
        )
        if allow_writes
        else None
    )
    readside_metadata = _load_readside_metadata(deployment_name)
    active_record_prefixes = tuple(readside_metadata.get("record_prefixes") or ())

    # Which tools this launch serves is decided HERE, before the FastMCP
    # object exists, because the instructions are a function of it (PACT
    # mcp-tool-surface-profiles, issue #1457). Resolving after registration
    # would leave the instructions describing a surface the server does not
    # have — the measured defect. `apply_admitted_tool_surface` below then
    # ENFORCES this same set, and the post-condition check proves the two
    # agree rather than assuming it.
    tool_profile_config = parse_tool_profile_config(readside_metadata.get("mcp"))
    # THE declared agent surface (PACT effective-surface-declaration, issue
    # #2012). When a deployment declares one it is the launch's base surface:
    # `served` says what this process registers, on every transport. An
    # explicit `--tools` or `OKG_MCP_TOOL_ALLOWLIST` may still NARROW
    # it — those are operator statements about one launch, not deployment
    # declarations — and `assert_narrowing_is_servable` refuses a selector
    # naming a tool outside it, so a narrowing can never widen.
    #
    # `None` means this deployment declares no block and keeps the frozen
    # legacy path (issue #2067): the `mcp:` profile config above, the
    # `mcp_instructions` file, and the warn-only instructions scan.
    agent_surface = readside_metadata.get("agent_surface")
    # `inspect(kind="image")` (#3046) is served where the deployment declares
    # an `agent_surface.images` block. The block sets the per-call budget and
    # the URL allowlist; a caller-supplied `image_source` (tests, an embedding
    # launch) takes precedence.
    if image_source is None and agent_surface is not None and getattr(agent_surface, "images", None) is not None:
        from okg.substrate.mcp.image_sources import GraphImageSource
        from okg.substrate.mcp.record_image import ImageBudget

        images_config = agent_surface.images
        image_source = GraphImageSource(config=images_config)
        image_budget = image_budget or ImageBudget(
            render_dpi=images_config.render.dpi,
            max_side=images_config.render.max_side,
            max_panels=images_config.budget.max_panels,
            max_bytes=images_config.budget.max_bytes,
        )
    tool_surface = resolve_tool_surface(
        requested_profile=tool_profile,
        env_allowlist=_tool_allowlist_from_env(),
        config=tool_profile_config,
        agent_surface_served=(
            agent_surface.served if agent_surface is not None else None
        ),
        # Scoped to NAMED deployments on purpose. `default` is the documented
        # ad-hoc `--dsn` path that has no manifest by design, so flagging it
        # would fire on every ordinary launch and train operators to ignore
        # the line. A named deployment that will not resolve is different: a
        # declared `mcp.default_tool_profile` may exist and be unreadable, so
        # the wider surface is served without the bound its owner asked for.
        manifest_unresolved=(
            bool(readside_metadata.get("manifest_unresolved", False))
            and deployment_name != _ADHOC_DEPLOYMENT_NAME
        ),
    )

    # The conditional admin surface is resolved HERE rather than at
    # registration because the served tool set — which the instructions
    # enumerate — is not knowable without it. It is a manifest read keyed only
    # by deployment name; the single result is reused at registration so the
    # deployment is not asked twice and cannot answer differently.
    from okg.substrate.mcp.upload_admin import conditional_tool_provisioning

    _conditional = conditional_tool_provisioning(deployment_name)
    _provisioned_conditional: frozenset[str] = _conditional["provisioned"]

    # The FULL set this launch serves, not just its read half. Enumerating
    # only the read tools while `--allow-writes` served eighteen would state
    # something false, which is the defect class this rendering exists to
    # close.
    boundary_tools = set(OPERATOR_TOOLS)
    if agent_surface is not None:
        # `run` (#3666) is new: a deployment that declares its served set
        # serves it only when it names it (`apply_admitted_tool_surface`
        # intersects the same way), so the instructions do not name it.
        boundary_tools -= set(_COMPOSITION_TOOLS) - set(agent_surface.served)
    # Jason, via orchestrator chat 2026-10-13, query-by-default: serve query
    # by default without access rules (issue #3666). Under `audit` or `enforce` (or a
    # mode that cannot be read) `query` is off on every launch path that
    # builds a surface here (mcp-serve, the `okg query` command, every
    # profile and allowlist): arbitrary SQL cannot be filtered by the
    # per-record rules. A call is refused by name
    # (`QueryOffUnderAccessRules`), not answered as an unknown tool.
    query_off_mode = _query_access_rules_mode(deployment_name)
    if query_off_mode is not None:
        boundary_tools.discard("query")
    if allow_writes:
        boundary_tools |= set(WRITE_ADMIN_TOOLS)
        boundary_tools |= set(_provisioned_conditional)
    assert_write_launch_is_served(
        agent_surface, allow_writes=allow_writes, deployment=deployment_name,
    )
    # The instructions and the prompt are scanned against what this
    # deployment declares, at BUILD time, and a name at an affirmative
    # position that the surface withholds refuses the launch (PACT
    # effective-surface-declaration, req.esd.drift-refused). This replaces
    # the warn-only scan below for a deployment that declares the block: a
    # log line nobody reads is not a check, and the instructions are served
    # to every session.
    _surface_dir = readside_metadata.get("agent_surface_dir")
    if agent_surface is not None and _surface_dir is not None:
        # The INSTRUCTIONS file only. The chat system prompt is scanned by
        # `okg deployments lint` and by `okg chat sync`, not here: it belongs
        # to a different client, and scanning it at build time let an edit to
        # chat prose refuse an unrelated stdio or streamable-http launch that
        # never sends that prompt to anyone.
        assert_surface_files_agree(
            agent_surface,
            deployment=deployment_name,
            deployment_dir=Path(_surface_dir),
            scan=("instructions",),
        )
    admitted_tools = set(boundary_tools)
    if tool_surface.tools is not None:
        admitted_tools &= set(tool_surface.tools)
    # What a SESSION starts with. Without an expansion ceiling this is also
    # what the server registers, and the two names describe one set.
    base_tools = tuple(t for t in CANONICAL_TOOL_ORDER if t in admitted_tools)

    # The in-session ceiling (issue #1458). It widens what the server
    # REGISTERS while leaving what a session SEES at `base_tools`, so a
    # caller can reach the wider set without a restart and without changing
    # any other session's list. Every refusal below is launch-time: an
    # unreachable or no-op ceiling must not start a server that advertises
    # `tools.listChanged` it can never honor.
    ceiling_surface = resolve_expansion_ceiling(
        tool_profile_ceiling, base=tool_surface, config=tool_profile_config,
    )
    if ceiling_surface is None:
        registered_admitted = set(admitted_tools)
    else:
        registered_admitted = set(boundary_tools)
        if ceiling_surface.tools is not None:
            registered_admitted &= set(ceiling_surface.tools)
        assert_expansion_is_coherent(
            base_tools=admitted_tools,
            ceiling_tools=registered_admitted,
            base_profile=tool_surface.profile or tool_surface.source,
            ceiling_profile=ceiling_surface.profile,
        )
    served_tools = tuple(
        t for t in CANONICAL_TOOL_ORDER if t in registered_admitted
    )

    # WHICH `inspect` target kinds this launch publishes and routes (PACT
    # effective-surface-declaration, req.esd.inspect-kinds-by-profile).
    #
    # Seventeen kinds sat behind one tool, and `inspect` is the largest item
    # on the wire — 5,148 B of a 19,498 B read surface, 3,098 B of that its
    # input schema. Most of those kinds are unreachable on a read-only
    # launch: the admission ledger, the curation queue, and the two kinds
    # that describe what a `record` call may write, on a surface with no
    # `record` tool.
    #
    # Computed ONCE, here, and used twice: to rewrite the PUBLISHED union
    # after registration (`publish_narrowed_inspect_target`), and by the
    # router guard inside `inspect`. They cannot disagree because they are
    # the same value. The published schema ends up stricter than the
    # validator, which the guard is what makes safe — it refuses exactly the
    # kinds the rewrite hides.
    _kind_tools = set(registered_admitted)
    if agent_surface is not None:
        _kind_tools &= set(agent_surface.served)
    inspect_kind_set = served_inspect_kinds(
        _kind_tools,
        # The write/admin kinds need BOTH the launch flag and a surface that
        # actually serves a write tool. `--allow-writes` on a deployment
        # whose `agent_surface.served` names only read operators registers no
        # write tool, and publishing the admission kinds there would describe
        # a pipeline this surface cannot reach.
        allow_writes=allow_writes and bool(
            _kind_tools & set(WRITE_ADMIN_TOOLS)
        ),
        has_expansion_ceiling=ceiling_surface is not None,
        # `inspect(kind="image")` exists only where the launch was given
        # somewhere to read images from (#3046).
        has_image_source=image_source is not None,
    )
    if ceiling_surface is not None and "surface" not in inspect_kind_set:
        # Its own check, and NOT covered by `assert_expansion_is_coherent`
        # above: that one proves the `inspect` TOOL is in the base surface,
        # which is necessary and no longer sufficient. `inspect` can be
        # served while the `surface` KIND is pruned, and the expansion door
        # would then be advertised and unreachable — the state the ceiling
        # refusals exist to prevent.
        raise ToolSurfaceExpansionInvalid(
            f"the in-session ceiling {ceiling_surface.profile!r} is "
            f"configured, but this surface does not publish the `surface` "
            f"inspect kind, which is the door a session calls to widen "
            f"itself (inspect(target={{'kind': 'surface', 'action': "
            f"'expand'}})). Refusing rather than advertising an unreachable "
            f"expansion.",
            data={"ceiling_profile": ceiling_surface.profile,
                  "inspect_kinds": sorted(inspect_kind_set)},
            fix_hint="drop the ceiling, or serve the surface inspect kind",
        )

    effective_instructions = instructions
    instruction_source = "caller"
    deployment_instructions_pruned_tools: tuple[str, ...] = ()
    deployment_instructions_text = ""
    if instructions == DEFAULT_INSTRUCTIONS:
        # PACT dataset-card. Rendered for what a session STARTS with, not for
        # what the process registers: with a ceiling those differ, and
        # instructions naming a tool the session cannot yet call are the
        # defect this rendering exists to prevent. The expansion sentence is
        # how the wider set becomes reachable instead of merely absent.
        #
        # A deployment's own instruction file is APPENDED below okg's text
        # under a fixed heading; it no longer replaces it. The file is still
        # the operator's text and is not rewritten, but it is WRITTEN against
        # a surface, so when a narrowing removes a tool it names, say so in
        # the readback. A substring scan, and deliberately a warning: it
        # cannot know the author's intent, only that a name appears.
        deployment_instructions_text = str(readside_metadata.get("instructions") or "")
        if deployment_instructions_text.strip():
            instruction_source = "deployment_file"
            deployment_instructions_pruned_tools = tuple(
                name
                for name in tools_named_in(deployment_instructions_text)
                if name not in served_tools
            )
        else:
            instruction_source = "rendered"
        effective_instructions = authored_instructions_for(
            base_tools,
            deployment_text=deployment_instructions_text,
            expansion_text=(
                render_expansion_instruction(
                    base_tools=base_tools, ceiling_tools=served_tools,
                )
                if ceiling_surface is not None else ""
            ),
        )
    surface_freshness = mcp_surface_report(
        deployment_name=deployment_name,
        instructions=effective_instructions,
    )
    # The served tool surface, reported through `inspect(target={"kind":
    # "surface"})` and `session_start`. Without this the profile is knowable
    # only from the launch argv, which the caller cannot see: the restart
    # contract ("to change the tool list, restart the server") is not
    # something an agent can act on unless it can first observe which profile
    # it is talking to. `instructions_name_unserved_tools` is the emitted
    # form of the deployment-instruction warning — a readback nobody can
    # query is not a warning.
    surface_freshness["tool_surface"] = {
        "profile": tool_surface.profile,
        "profile_source": tool_surface.source,
        # What THIS session can call. Without a ceiling it equals the
        # registered set; with one, `inspect(kind=surface)` overlays the
        # calling session's own projection so the readback is never a
        # process-wide answer to a per-session question.
        "served_tools": list(base_tools),
        "registered_tools": list(served_tools),
        "instruction_source": instruction_source,
        "instructions_name_unserved_tools": list(deployment_instructions_pruned_tools),
        "unhonored_manifest_default": tool_surface.unhonored_manifest_default,
        # Issue #1458. True only where the tool list really is fixed for a
        # session's lifetime; a launch with a ceiling can widen one session
        # in place, and says so rather than repeating the old sentence.
        "profile_change_requires_restart": ceiling_surface is None,
        "expansion": {
            "available": ceiling_surface is not None,
            "ceiling_profile": (
                ceiling_surface.profile if ceiling_surface is not None else None
            ),
            "expandable_tools": sorted(set(served_tools) - set(base_tools)),
            "action": (
                "inspect(target={\"kind\": \"surface\", \"action\": \"expand\"})"
                if ceiling_surface is not None
                else None
            ),
            "expanded": False,
            "list_changed_notification_failed": False,
        },
        # Whether clients were told, at initialize, that this server sends
        # notifications/tools/list_changed. Advertised only where it can be
        # true (issue #1458).
        "tool_list_changed_advertised": ceiling_surface is not None,
    }
    # Transport-boundary authentication (PACT mcp-http-auth). Both kwargs are
    # required together by the SDK: `auth` without a verifier and a verifier
    # without `auth` are each a construction-time ValueError, so a half-wired
    # credential cannot produce a silently-unauthenticated server.
    auth_kwargs: dict[str, Any] = {}
    if auth_token is not None and oidc_verifier is not None:
        from okg.substrate.mcp.oidc_auth import oidc_auth_exclusive_error

        raise oidc_auth_exclusive_error("--auth-token-env")
    if auth_token is not None:
        from okg.substrate.mcp.http_auth import (
            StaticTokenVerifier,
            build_auth_settings,
        )

        auth_kwargs = {
            "token_verifier": StaticTokenVerifier(auth_token),
            "auth": build_auth_settings(),
        }
    if oidc_verifier is not None:
        # PACT mcp-oidc-auth. The verified token IS the per-request principal,
        # so OIDC mode is multi-user by construction and goes through the
        # same machinery below: the start-time provider check, the refusal of
        # the write surface, and the principal gates on tools and resources.
        from okg.substrate.mcp.oidc_auth import build_oidc_auth_settings

        auth_kwargs = {
            "token_verifier": oidc_verifier,
            "auth": build_oidc_auth_settings(oidc_verifier.config),
        }
        multi_user_principals = True
        principal_mapping = oidc_verifier.config
    mcp = ByteBoundedFastMCP(
        "okg-substrate", instructions=effective_instructions, **auth_kwargs
    )
    # okg's version and revision, not the SDK's, in the initialize reply: the
    # freshness doctor's only self-report from a launch that withholds
    # `inspect(kind=surface)`.
    mcp._mcp_server.version = server_info_version(
        surface_freshness.get("package_version"),
        surface_freshness.get("git_revision"),
    )
    # okg#3112 step 4a: a removed OKG_ATTR_FILTER_INDEX value is refused on
    # each filtered read, not here -- refusing the launch would stop every
    # read. Say so once at start.
    _attr_filter_source.log_removed_settings()
    logical_sessions = LogicalSessionRegistry()
    mcp._okg_logical_sessions = logical_sessions  # type: ignore[attr-defined]
    # PACT unknown-s8-mcp-readonly (#2035). Re-checked here, not trusted:
    # a malformed value must refuse the launch, never serve writes.
    checked_read_only_release = (
        None if read_only_release is None
        else read_only_release_from_report(read_only_release)
    )
    mcp._okg_read_only_release = checked_read_only_release

    def _logical_identity_from_context(
        context: Context | None,
    ) -> object | None:
        """Return the stable SDK ServerSession, never caller metadata.

        Stateless HTTP still supplies a short-lived ServerSession object, but
        the SDK creates a new one for every HTTP request. Treating it as durable
        would falsely advertise persistence, so the server setting is part of
        the identity check.
        """

        if context is None:
            return None
        try:
            if bool(context.fastmcp.settings.stateless_http):
                return None
            identity = context.session
        except (AttributeError, RuntimeError, ValueError):
            return None
        if not logical_sessions.supports(identity):
            return None
        return identity

    def _current_logical_identity() -> object | None:
        return _logical_identity_from_context(_CURRENT_MCP_CONTEXT.get())

    def _stamp_logical_session_metadata(
        payload: dict[str, Any],
        *,
        context: Context | None,
        requested_generation: object | None,
    ) -> dict[str, Any]:
        """Stamp the public identity/scope contract on one core-read envelope."""

        if checked_read_only_release is not None:
            # PACT unknown-s8-mcp-readonly: every answer from a database
            # behind this code says how far behind, including refusals.
            payload["schema_behind_by"] = checked_read_only_release[
                "schema_behind_by"
            ]
        explicit = requested_generation is not None
        identity = _logical_identity_from_context(context)
        if identity is None:
            payload["logical_session_id"] = None
            payload["pin_scope"] = "explicit_call" if explicit else "request"
            payload["session_default_generation_id"] = None
            if not explicit:
                payload["warning"] = {
                    "code": "mcp_session_identity_unavailable",
                    "message": (
                        "this transport supplies no stable MCP session identity; "
                        "an omitted generation_id is scoped to this request only"
                    ),
                }
            return payload

        snapshot = logical_sessions.snapshot(identity)
        payload["logical_session_id"] = snapshot.logical_session_id
        payload["pin_scope"] = "explicit_call" if explicit else "transport_session"
        payload["session_default_generation_id"] = snapshot.default_generation_id
        return payload

    def _logical_response_envelope(
        _tool_name: str,
        fn: Any,
    ) -> Any:
        """Inject request context internally and stamp every returned dict.

        This wrapper sits outside the byte-bound wrapper, so validation and
        oversize refusals receive the same metadata as successful operators.
        It also means a native byte-bound retry remains one logical invocation:
        the ContextVar stays set across all internal reruns and metadata is
        rendered once from the caller's original generation argument.
        """

        signature = _py_inspect.signature(fn)

        @wraps(fn)
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()
            context = bound.arguments.get("ctx")
            requested_generation = bound.arguments.get("generation_id")
            token = _CURRENT_MCP_CONTEXT.set(context)
            try:
                out = fn(*args, **kwargs)
            finally:
                _CURRENT_MCP_CONTEXT.reset(token)
            if isinstance(out, dict):
                stamped = _stamp_logical_session_metadata(
                    out,
                    context=context,
                    requested_generation=requested_generation,
                )
                # Issue #2674: the compact wire shape, applied AFTER every
                # stamp so the served payload is exactly what the byte bound
                # measured (see `_logical_response_measurement`).
                return _compact_for_wire(
                    with_read_coverage(stamped),
                    {
                        name: as_mapping(value)
                        for name, value in bound.arguments.items()
                        if name in {"select", "target"}
                    },
                )
            return out

        return _wrapped

    def _logical_response_measurement(
        payload: dict[str, Any],
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Render the eventual metadata into byte-bound size calculations."""

        # The bound measures the COMPACT form (issue #2674), so a page it
        # accepts is the page the envelope serves, and a compact page fits
        # more rows under `max_bytes`. `compact_for_wire` is pure: `payload`
        # here is only a shallow copy of the one the envelope will serve.
        return _compact_for_wire(
            with_read_coverage(_stamp_logical_session_metadata(
                payload,
                context=_CURRENT_MCP_CONTEXT.get(),
                requested_generation=arguments.get("generation_id"),
            )),
            arguments,
        )

    # ByteBoundedFastMCP catches typed errors after the Python tool wrapper has
    # re-raised them. Give that outer public-error path the same stamper while
    # preserving the established direct `.fn` exception contract.
    mcp._okg_logical_session_stamper = (  # type: ignore[attr-defined]
        _stamp_logical_session_metadata
    )

    @contextmanager
    def _result_notice_session(
        context: Context | None,
    ) -> Iterator[dict[str, Any] | None]:
        """The served call's per-session notice state, or None without one.

        Issue #3059 (#3076 DESIGN 1.2.3): the stale-source notice prints once
        per logical session and again when the stale set changes. A call
        with no stable session identity gets None and prints it every time.
        """

        identity = _logical_identity_from_context(context)
        if identity is None:
            yield None
            return
        with logical_sessions.result_notices(identity) as state:
            yield state

    mcp._okg_notice_session = _result_notice_session  # type: ignore[attr-defined]

    surface_probe_only = os.environ.get("OKG_MCP_SURFACE_PROBE") in {
        "1",
        "true",
        "TRUE",
    }
    # Width comes from the `mcp.read_pool` concurrency term unless a caller
    # passes one explicitly. NOTE this is PER MCP PROCESS: agent sessions start
    # their own servers, so the host-wide total is a reservation problem, not
    # something a single process can sum. See db/concurrency_budget.py.
    if max_conns is None:
        from okg.substrate.db.concurrency_budget import width

        max_conns = width("mcp.read_pool")
    # A capability fixture is for hermetic builds only (#3151). Refused here,
    # before any connection is reserved or any pool opens.
    if not surface_probe_only and os.environ.get(CAPABILITY_FIXTURE_ENV):
        raise CapabilityFixtureInvalid(
            f"{CAPABILITY_FIXTURE_ENV} is set on a server that opens a "
            "database. Capability fixtures are for hermetic "
            "(OKG_MCP_SURFACE_PROBE) builds only; a live server probes.",
            data={"fixture": os.environ.get(CAPABILITY_FIXTURE_ENV)},
            fix_hint=f"unset {CAPABILITY_FIXTURE_ENV}",
        )
    # Whether THIS server recorded a claim. Only then does closing its pool
    # release the deployment's claim: a probe-mode server, or one whose claim
    # failed, must not release a claim another in-process server holds.
    reservation_claimed = False
    if surface_probe_only:
        min_conns = 0
        max_conns = max(1, max_conns)
    else:
        # Claim these connections host-wide before opening the pool. An MCP
        # server is one of N independently-started processes on a shared
        # Postgres, so it is exactly the case a single process's boot-time sum
        # cannot see; without a registered claim, several agent sessions each
        # start believing the same headroom is theirs.
        #
        # BudgetError and a post-insert lifecycle-owner failure propagate and
        # refuse startup. Pre-acquisition infrastructure failures are logged
        # and startup continues, for the same reason the worker's admission
        # check does: the reservation guards over-declaration, not server
        # liveness, and a read-only MCP surface refusing to start because it
        # could not write a bookkeeping row would be a worse outcome than the
        # risk it manages.
        from okg.substrate.db.capacity import (
            ReservationLifecycleError,
            reserve_process_demand,
        )
        from okg.substrate.db.concurrency_budget import BudgetError

        if claim_attempted is not None:
            claim_attempted.append(True)
        try:
            if one_call:
                # Exactly what this pool can open, with no heartbeat thread:
                # see `one_call` in the docstring.
                reserve_process_demand(
                    read_dsn,
                    application_name=f"okg-mcp-{deployment_name}",
                    deployment=deployment_name,
                    include_mcp=True,
                    mcp_read_role=True,
                    demand=max_conns,
                    heartbeat_interval_seconds=None,
                )
            else:
                reserve_process_demand(
                    read_dsn,
                    application_name=f"okg-mcp-{deployment_name}",
                    deployment=deployment_name,
                    include_mcp=True,
                    mcp_read_role=True,
                )
            reservation_claimed = True
        except (BudgetError, ReservationLifecycleError):
            raise
        except Exception as exc:  # noqa: BLE001 — see comment above
            log.warning(
                "MCP connection reservation could not be recorded (%s); "
                "starting anyway. This does NOT mean the host has room.%s",
                exc,
                _reservation_privilege_fix_hint(exc, deployment_name),
            )

    pool = _build_pool(
        conninfo=read_dsn,
        role="okg_mcp",
        min_size=min_conns,
        max_size=max_conns,
        open=not surface_probe_only,
        name="okg-mcp",
    )
    # Separate write pool (app_rw) — created only when the operator
    # enabled write mode at launch. The read pool above (okg_mcp) cannot
    # mutate the graph; every write tool routes through this pool via
    # `_write_pool_or_refuse`, and refuses when write mode is off.
    write_pool = (
        _build_pool(
            conninfo=str(write_dsn),
            role="app_rw",
            # Under OKG_MCP_SURFACE_PROBE the read pool is already built
            # unopened so the registered surface can be inspected without
            # a database. The write pool has to follow the same rule or a
            # write/admin surface probe still dials Postgres — which is
            # exactly what a hermetic tool-surface test must not do.
            min_size=0 if surface_probe_only else 1,
            max_size=max(2, max_conns // 2),
            open=not surface_probe_only,
            name="okg-mcp-write",
        )
        if write_dsn is not None
        else None
    )
    # Issue #3493: every blocking step of a request (principal gates, the
    # tool, result serving, sync resource reads) runs in a worker thread, at
    # most `tool_call_width(max_conns)` at once. See that function for the
    # arithmetic.
    mcp._okg_tool_call_width = tool_call_width(max_conns)  # type: ignore[attr-defined]
    # Register the pool + a pin cache on the FastMCP instance so
    # tests can inspect them. Prefixed to avoid clashing with any
    # FastMCP internals.
    # Issue #2013. The declared bindings, reachable from `expand(live=...)`.
    # Empty when the deployment declares none, which `expand` turns into a
    # NAMED refusal rather than a silently ignored parameter: a caller that
    # asked for live data and got only graph rows, with no signal, is the
    # silent fallback this repository forbids.
    mcp._okg_live_bindings = (  # type: ignore[attr-defined]
        dict(agent_surface.live_bindings) if agent_surface is not None else {}
    )
    # The operator's switch for `run`'s live steps (#3666, lane ri): off,
    # a run refuses its live steps by name (`run_live_steps_off`) and a live
    # call sent on its own is unaffected.
    mcp._okg_run_live_steps = run_live_steps_enabled()  # type: ignore[attr-defined]
    mcp._okg_agent_surface = agent_surface  # type: ignore[attr-defined]
    # Adapter runners for live relations, by binding kind. The substrate's
    # BUILT-IN runners come first and the caller's `live_adapters` is merged
    # OVER them, which orders the two deliberately:
    #
    #  - production passes nothing and still gets `http` (#2100) and `mcp`,
    #    so a deployment never depends on a caller handing it an adapter;
    #  - `live_adapters` survives as a TEST SEAM, and a test supplying its
    #    own `http` runner still gets its own.
    #
    # A binding whose kind has no runner -- `sql`, `command`, `python` -- is
    # still refused at load below, not discovered on an agent's first call.
    from okg.substrate.external_backed import http_adapter as _http_adapter
    from okg.substrate.external_backed import mcp_adapter as _mcp_adapter
    from okg.substrate.external_backed.live_dispatch import (
        builtin_live_adapters,
    )

    # WHOSE credential an `mcp` binding's server uses upstream is declared,
    # not detected, and checked here for every `kind: mcp` binding -- before
    # any database work and whatever runner a caller supplied, because it is
    # a rule about the deployment, not about a request's shape.
    if agent_surface is not None:
        _mcp_adapter.assert_mcp_credentials_admitted(
            mcp._okg_live_bindings,  # type: ignore[attr-defined]
            lifecycle=readside_metadata.get("contract_lifecycle"),
        )

    mcp._okg_live_adapters = {  # type: ignore[attr-defined]
        **builtin_live_adapters(), **(live_adapters or {}),
    }
    # Per-RELATION runners, keyed separately from the kind table above. A
    # relation's runner closes over its own parsed config and its own
    # resolved secret, so it cannot be shared by kind; and keeping it in its
    # own mapping means a relation that happens to be named `http` cannot
    # shadow the kind entry. Filled by the load hook below, for deployments
    # that bind something.
    mcp._okg_live_relation_runners = {}  # type: ignore[attr-defined]
    # The deployment's live types (#3666, PACT live-graph-rucio-sites), built
    # once by the load hook below. Empty until then, and empty for a
    # deployment that declares none: a read of a type not in it is a stored
    # read, as before.
    from okg.substrate.external_backed.live_types import LiveTypeRegistry

    mcp._okg_live_types = LiveTypeRegistry()  # type: ignore[attr-defined]
    # LOAD-time cross-check (review finding 5). `parse_live_bindings` sees
    # the manifest and not the catalog, so on its own it can never catch a
    # binding for a relation nothing declares, or a declared relation nobody
    # binds. Both refusals lived in `bind_live_relations` with no caller.
    # This is the caller. It runs only for a deployment that actually binds
    # something, so a deployment with an empty slot pays no connection.
    # Runs whenever the deployment declares an `agent_surface` block, not
    # only when it binds something. A manifest that binds NOTHING while its
    # catalog declares a relation is precisely the case `inspect` would
    # advertise something nothing can serve -- checking only non-empty
    # bindings left that hole open. A deployment with no block pays nothing.
    #
    # Skipped under OKG_MCP_SURFACE_PROBE, which builds both pools UNOPENED
    # to measure the tool surface without a database. That mode serves no
    # traffic, so there is no call for this check to protect. Spelled out
    # rather than left as a connection error: a load check that silently did
    # nothing in some mode is the fallback this check exists to prevent.
    if agent_surface is not None and not surface_probe_only:
        from okg.substrate.external_backed.live_dispatch import (
            assert_live_bindings_are_declared,
        )

        from okg.substrate.external_backed.live_relations import (
            LiveRelation,
            register_sample_policies,
        )

        with pool.connection() as _live_check_conn:
            _live_session = open_session(
                _live_check_conn, deployment_name=deployment_name,
            )
            _declared_where = assert_live_bindings_are_declared(
                _live_check_conn,
                _live_session,
                mcp._okg_live_bindings,  # type: ignore[attr-defined]
                adapter_runners=mcp._okg_live_adapters,  # type: ignore[attr-defined]
            )
            # Live types (#3666), checked BEFORE any runner is built: every
            # relation a type names is bound, `agent_surface.live_types`
            # names only live types, prefixes are unique, and no stored node
            # at the pinned generation uses a live type or prefix (one
            # bounded probe per type). A relation declared on two subtypes
            # was already refused by `assert_live_bindings_are_declared`.
            from okg.substrate.external_backed.live_dispatch import (
                build_live_type_registry,
            )

            mcp._okg_live_types = build_live_type_registry(  # type: ignore[attr-defined]
                _live_check_conn,
                _live_session,
                bound=mcp._okg_live_bindings,  # type: ignore[attr-defined]
                served=agent_surface.live_types,
            )
            # STARTUP registration of every bound relation's sample policy.
            # `declared_live_relations` rebuilds each one, which is also
            # where a stored declaration this code cannot honour -- a
            # missing action, `principal_mode: caller` -- fails the BUILD.
            from okg.substrate.external_backed.live_dispatch import (
                declared_live_relations,
            )

            _by_subtype: dict[str, dict[str, LiveRelation]] = {}
            for _relation, _subtype in sorted(_declared_where.items()):
                if _subtype not in _by_subtype:
                    _by_subtype[_subtype] = declared_live_relations(
                        _live_check_conn, _live_session, _subtype,
                    )
                register_sample_policies({
                    _relation: _by_subtype[_subtype][_relation],
                })
            # CONFIG validation, AFTER the loop above has rebuilt every
            # bound relation, and with both the binding and its compiled
            # relation in hand -- which is what makes the cross-checks
            # possible at all: every slot must name a declared argument,
            # every declared argument must be slotted, and every slotted
            # argument must carry a constraint. It returns the bound runners,
            # closures over the secrets it resolved here, once.
            #
            # A deployment whose binding could never produce a legal request
            # does not start.
            mcp._okg_live_relation_runners.update(  # type: ignore[attr-defined]
                _http_adapter.validate_live_binding_configs(
                    mcp._okg_live_bindings,  # type: ignore[attr-defined]
                    {
                        _relation: _by_subtype[_subtype][_relation]
                        for _relation, _subtype in _declared_where.items()
                    },
                    # LOAD-BEARING: the CALLER's adapters only, never the
                    # merged dict. `validate_live_binding_configs` skips a
                    # kind a caller already supplied, because a test passing
                    # its own `http` runner must not be made to satisfy this
                    # parser -- its binding's config is whatever its fake
                    # needs. Passing `mcp._okg_live_adapters` here would
                    # always contain the built-in `http`, so every binding
                    # would look caller-supplied and EVERY load-time config
                    # check would silently stop running.
                    runners=dict(live_adapters or {}),
                )
            )
            # The same load-time contract for `kind: mcp`, with the same
            # caller-adapters-only rule for `runners`.
            mcp._okg_live_relation_runners.update(  # type: ignore[attr-defined]
                _mcp_adapter.validate_mcp_binding_configs(
                    mcp._okg_live_bindings,  # type: ignore[attr-defined]
                    {
                        _relation: _by_subtype[_subtype][_relation]
                        for _relation, _subtype in _declared_where.items()
                    },
                    runners=dict(live_adapters or {}),
                )
            )
    # This server's connection reservation (issue #2280) ends with its read
    # pool: closing the pool releases the claim, and so does the explicit hook
    # run_server calls on shutdown. A caller that builds a server in-process
    # and closes its pool therefore never leaves a live claim behind.
    reservation_application_name = f"okg-mcp-{deployment_name}"

    def _release_reservation() -> None:
        if not reservation_claimed:
            return
        from okg.substrate.db.capacity import release_process_reservations

        release_process_reservations(application_name=reservation_application_name)

    close_read_pool = pool.close

    def _close_read_pool_and_release(*args: Any, **kwargs: Any) -> Any:
        try:
            return close_read_pool(*args, **kwargs)
        finally:
            try:
                _release_reservation()
            finally:
                # Issue #3493: the request workers end with the server too.
                mcp._okg_shutdown_tool_executor()  # type: ignore[attr-defined]

    pool.close = _close_read_pool_and_release  # type: ignore[method-assign]

    # What this server can actually run (#3151 slice 1, decision 1a): probed
    # ONCE, here, and fixed for the life of the process. The record narrows
    # the published `search` schema (`compile_tool_surface`, below), drives
    # the guard at the top of `search`, and adds the generated "not available
    # here" lines to the served instructions. A read that errors refuses the
    # start, naming the read; a timeout publishes the capability as unknown.
    # Every failure from here to the end of this block closes both pools
    # (and so releases the reservation) before it propagates.
    try:
        if surface_probe_only:
            capabilities = capabilities_for_surface_probe(
                embedder_configured=embedder is not None,
                fixture=os.environ.get(CAPABILITY_FIXTURE_ENV) or None,
            )
        else:
            capabilities = probe_capabilities_from_pool(
                pool,
                deployment_name=deployment_name,
                branch_name=branch_name,
                embedder_configured=embedder is not None,
            )
        # Decision 5b: appended on BOTH served paths, okg's rendered text and a
        # deployment's own instructions file (the `agent_surface` block and the
        # legacy `mcp_instructions` key alike). A caller that passes its own
        # `instructions` owns the whole text and gets it verbatim.
        if instruction_source in {"rendered", "deployment_file"}:
            generated_instructions = render_unavailable_lines(
                capabilities, served_tools=base_tools,
            )
        else:
            generated_instructions = ""
        surface_freshness["generated_instructions_bytes"] = len(
            generated_instructions.encode("utf-8")
        )
        # PACT dataset-card: the facts the card states, read ONCE here at the
        # generation the capability probe pinned (the one a new session
        # reads). The card itself is rendered at the end of the build, after
        # the published `search` schema is compiled. Each read is bounded; a
        # failed read leaves its part missing and the card says so.
        not_ingested = tuple(readside_metadata.get("not_ingested") or ())
        dataset_facts: Optional[DatasetFacts] = None
        # PACT dataset-card-switch: `mcp: {dataset_card: false}` turns the
        # card off. Nothing is gathered (no database read, no path check)
        # and no card is served; the rest of the served text is unchanged.
        dataset_card_off_reason: Optional[str] = (
            None if dataset_card_enabled(tool_profile_config)
            else "deployment config mcp.dataset_card: false"
        )
        if dataset_card_off_reason is None and not dataset_card:
            dataset_card_off_reason = (
                "a one-call server whose tool states no part of the card"
            )
        if (
            instruction_source in {"rendered", "deployment_file"}
            and dataset_card_off_reason is None
        ):
            if surface_probe_only:
                dataset_facts = _dataset_facts_without_database(
                    deployment_name=deployment_name,
                    not_ingested=not_ingested,
                    reason=(
                        "this server was built without a database "
                        "(OKG_MCP_SURFACE_PROBE)"
                    ),
                )
            elif capabilities.pinned is None:
                # Nothing pinned: the probe found no published generation,
                # its pin read timed out, or (in tests) a fixture stood in
                # for it. The card says which, and reads nothing.
                dataset_facts = _gather_dataset_facts(
                    None,
                    deployment_name=deployment_name,
                    branch_name=branch_name,
                    pinned=None,
                    not_ingested=not_ingested,
                    no_pin_reason=(
                        capabilities.method("alias").reason
                        if capabilities.mode == "live"
                        else f"the capability record came from {capabilities.mode}"
                    ),
                )
            else:
                dataset_facts = _gather_dataset_facts_for_start(
                    pool,
                    deployment_name=deployment_name,
                    branch_name=branch_name,
                    pinned=capabilities.pinned,
                    not_ingested=not_ingested,
                )
        #: Filled at the end of the build; read by `inspect(session_start)`.
        dataset_card_state: dict[str, Any] = {}
    except BaseException:
        pool.close()
        if write_pool is not None:
            write_pool.close()
        raise
    mcp._okg_capabilities = capabilities  # type: ignore[attr-defined]
    mcp._okg_release_reservation = _release_reservation  # type: ignore[attr-defined]
    mcp._okg_query_off_mode = query_off_mode  # type: ignore[attr-defined]
    # Read again on every `query` call (issue #3666): rules switched on after
    # start refuse it as well.
    mcp._okg_query_mode_deployment = deployment_name  # type: ignore[attr-defined]
    mcp._okg_pool = pool  # type: ignore[attr-defined]
    mcp._okg_write_pool = write_pool  # type: ignore[attr-defined]
    mcp._okg_allow_writes = allow_writes  # type: ignore[attr-defined]
    mcp._okg_dsn_authority = dsn_resolution.get("authority", {})  # type: ignore[attr-defined]
    mcp._okg_surface_freshness = surface_freshness  # type: ignore[attr-defined]
    # Pin cache: deployment_name → (expires_at, gen_id, cv_id, compat_id, fp)
    # (expires_at, gen_id, cv_id, compat_id, db_fingerprint, generation_branch)
    pin_cache: dict[str, tuple[float, str, int, int | None, str, str]] = {}
    mcp._okg_pin_cache = pin_cache  # type: ignore[attr-defined]
    # AKMON Phase A.3 cut 2c: every session this server opens pins
    # to one branch, set at server startup time. Branch-aware reads
    # come from the apply_branch_pin GUC + the layered views. To
    # read a different branch, start a separate mcp-serve with
    # `--branch <name>`; one server = one branch.
    mcp._okg_branch_name = branch_name  # type: ignore[attr-defined]

    # PACT mcp-principal-mapping. Multi-user mode is settled HERE, once, so
    # every later question ("is this server multi-user?") reads a fact rather
    # than re-deriving one. Both refusals below are start-time: an operator
    # who asked for per-user principals and got a server that silently served
    # one shared principal has been told the opposite of the truth.
    if multi_user_principals:
        # A refused start must leave no pool open: build_server has
        # already opened the read pool (and the write pool) by the time
        # these checks run, and an operator who was told the server
        # refused to start should not also be leaking connections.
        try:
            if principal_mapping is None:
                principal_mapping = resolve_principal_mapping(deployment_name)
            if principal_mapping is None:
                raise SubstrateError(
                    "multi-user principal mapping was requested for deployment "
                    f"{deployment_name!r}, but the deployment declares no "
                    "nomos.principal_mapping block, so there is no declared "
                    "header to read a principal from; refusing to start",
                    data={"deployment_name": deployment_name},
                    fix_hint=(
                        "declare nomos.principal_mapping in the deployment "
                        "manifest (provider_id, issuer, audience, subject_claim, "
                        "subject_header, principal_kind)"
                    ),
                )
            if allow_writes:
                # The write path (`_authorize_record_with_nomos`) still resolves
                # identity from the caller-supplied `authority` dict, falling back
                # to a build-time environment subject. Under multi-user mode that
                # is exactly the self-assertion channel and exactly the downgrade
                # this PACT forbids -- on the READ path both are closed, and the
                # write path is out of this change's scope (design.md Plane 3
                # keeps writes on stdio and the CLI). Refusing the combination is
                # the honest boundary; quietly serving per-user reads next to
                # shared-principal writes is not.
                raise SubstrateError(
                    "multi-user principal mapping cannot be combined with the "
                    "write surface: the MCP write path still resolves identity "
                    "from the caller-supplied `authority` argument and from the "
                    "process environment subject, so writes would not be "
                    "attributed to the per-request principal",
                    data={
                        "deployment_name": deployment_name,
                        "allow_writes": True,
                    },
                    fix_hint=(
                        "run a read-only multi-user server (drop --allow-writes "
                        "and OKG_MCP_ALLOW_WRITES) and perform writes over stdio "
                        "or the CLI until the write path is principal-aware"
                    ),
                )
            if surface_probe_only:
                # The provider check below needs a connection. Accepting the mode
                # without it would start a multi-user server whose provider was
                # never verified -- an unchecked delegation.
                raise SubstrateError(
                    "multi-user principal mapping cannot be validated under "
                    "OKG_MCP_SURFACE_PROBE, which starts the server without a "
                    "database connection; refusing rather than starting a "
                    "multi-user server whose identity provider was never checked",
                    data={"deployment_name": deployment_name},
                    fix_hint="drop OKG_MCP_SURFACE_PROBE for multi-user servers",
                )
            if not deployment_requires_nomos_runtime(deployment_name):
                # Without Nomos runtime enforcement the read filter returns rows
                # untouched, so per-user principals would be resolved and then
                # ignored -- a server that reports multi-user and serves
                # everyone everything.
                raise SubstrateError(
                    "multi-user principal mapping was requested for deployment "
                    f"{deployment_name!r}, whose Nomos runtime enforcement is "
                    "off, so per-user principals would be resolved and then "
                    "have no effect on what any caller can read; refusing to "
                    "start",
                    data={"deployment_name": deployment_name},
                    fix_hint=(
                        "set nomos.runtime_enforcement to audit or enforce in "
                        "the deployment manifest"
                    ),
                )
            from okg.substrate.mcp.principal_mapping import (
                validate_mapping_against_providers,
            )
            from okg.substrate.nomos.identity_store import load_identity_providers

            from okg.substrate.mcp.oidc_auth import (
                OidcAuthConfig,
                validate_oidc_auth_against_providers,
            )

            probe_conn = _pool_getconn(pool)
            try:
                validate_against_providers = (
                    validate_oidc_auth_against_providers
                    if isinstance(principal_mapping, OidcAuthConfig)
                    else validate_mapping_against_providers
                )
                validate_against_providers(
                    principal_mapping,
                    load_identity_providers(
                        probe_conn, deployment_name=deployment_name
                    ),
                    deployment_name=deployment_name,
                )
            finally:
                pool.putconn(probe_conn)
        except BaseException:
            pool.close()
            if write_pool is not None:
                write_pool.close()
            raise
    mcp._okg_multi_user_principals = bool(  # type: ignore[attr-defined]
        multi_user_principals
    )
    mcp._okg_principal_mapping = (  # type: ignore[attr-defined]
        principal_mapping if multi_user_principals else None
    )
    # A caller-supplied `authority.nomos_identity_claim` is the caller's own
    # statement of who it is: nothing on this server checks a signature on
    # it, and it may say `verification_status: valid`. Multi-user servers
    # refuse it outright (conflicting_identity_channel). A single-user server
    # refuses it too unless the operator opted in at launch with
    # OKG_MCP_TRUST_CALLER_IDENTITY_CLAIMS=1, which says "every caller that
    # can reach this server may choose its Nomos identity".
    trust_caller_identity_claims = (
        os.environ.get(TRUST_CALLER_IDENTITY_CLAIMS_ENV) == "1"
    )

    def _caller_identity_claim(
        authority: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        claim = _nomos_identity_claim_from_authority(authority)
        if claim is None or multi_user_principals:
            # Multi-user mode refuses a present claim at the gate, with its
            # own audited refusal; it is passed through for that check.
            return claim
        if not trust_caller_identity_claims:
            raise caller_identity_claim_untrusted_error()
        return claim

    # Tool-call tracing — every grouped tool registration gains a thin
    # wrapper that writes one row to okg.mcp_trace_events per call.
    # Identity decorator when OKG_MCP_TRACE_ENABLED=0.
    traced = make_tracer(
        pool,
        deployment_name=deployment_name,
        branch_name=branch_name,
    )
    if multi_user_principals:
        # PACT mcp-principal-mapping: the refusal must cover EVERY tool, not
        # only the ones that open a pooled read session. Several `inspect`
        # kinds return metadata before any session exists, and the
        # change-observation tools use `_open_logical`, which builds a Session
        # directly. Composing the gate into `traced` -- which every
        # registration site applies to every tool function -- makes "no
        # principal, no tool call" true by construction instead of true for
        # the paths someone remembered.
        traced = _with_multi_user_principal_gate(
            traced,
            pool=pool,
            deployment_name=deployment_name,
            principal_mapping=principal_mapping,
        )
    mcp._okg_tracer = traced  # type: ignore[attr-defined]

    def _write_refusal(tool: str) -> dict[str, Any] | None:
        """Return a structured refusal when write mode is off, else None.

        The agent cannot enable writes: write mode is fixed at
        server-launch time by the operator (allow_writes /
        OKG_MCP_ALLOW_WRITES). A self-asserted `authority.allow_write`
        in the tool call has no effect here.
        """
        if write_pool is not None:
            return None
        return {
            "status": "writes_disabled",
            "tool": tool,
            "summary": (
                "This MCP server is read-only. Graph/source writes are "
                "disabled; an agent-supplied authority cannot enable them."
            ),
            "fix_hint": (
                "operator: relaunch `okg mcp-serve` with --allow-writes "
                "(or OKG_MCP_ALLOW_WRITES=1) to enable setup/curation writes"
            ),
        }

    # Cache for the resolved branch_id for this deployment+branch_name
    # pair.  Resolved on first connection; reused for the lifetime of
    # this server.  None until first lookup.
    _branch_id_cache: dict[str, int] = {}

    def _resolve_branch_id_cached(conn: psycopg.Connection) -> int:
        """Resolve (deployment_name, branch_name) → branch_id once
        and cache it for the lifetime of this server.

        Default branches are lazy-materialized (matches the
        open_session pattern); non-default branches must exist.
        """
        cached = _branch_id_cache.get(branch_name)
        if cached is not None:
            return cached
        from okg.substrate.mcp.session import _resolve_branch_id

        branch_id = _resolve_branch_id(
            conn,
            deployment_name,
            branch_name,
        )
        _branch_id_cache[branch_name] = branch_id
        return branch_id

    def _resolve_latest(
        conn: psycopg.Connection,
        *,
        use_cache: bool,
    ) -> tuple[str, int, int | None, str]:
        """Return (gen_id, cv_id, compat_id, generation_branch) for the
        latest-published pin on `deployment_name` AT THIS SERVER'S BRANCH.
        `use_cache=True` honors the request-scoped 500ms TTL; `False` always
        hits the DB for first bind, refresh, or live-picture mode.

        `branch_name` is passed explicitly (issue #1437). Omitting it let the
        resolver fall back to its `default` default, so a fork-configured
        server served the DEFAULT branch's head while labelling the session
        with the fork it was asked for -- every read from the wrong branch,
        with metadata saying otherwise. `pin_cache` stays keyed by deployment
        alone because it lives in this server's closure and `branch_name` is
        fixed for the server's lifetime.
        """
        now = time.monotonic()
        if use_cache:
            entry = pin_cache.get(deployment_name)
            if entry is not None and entry[0] > now:
                return entry[1], entry[2], entry[3], entry[5]
        from okg.substrate.mcp.session import resolve_generation

        (
            gen_id,
            cv_id,
            compat_generation_id,
            generation_branch,
        ) = resolve_generation(
            conn,
            deployment_name,
            branch_name=branch_name,
        )
        fp = _db_fingerprint(conn)
        pin_cache[deployment_name] = (
            now + LATEST_PIN_CACHE_TTL_S,
            gen_id,
            cv_id,
            compat_generation_id,
            fp,
            generation_branch,
        )
        return gen_id, cv_id, compat_generation_id, generation_branch

    def _nomos_runtime_denied(
        conn: psycopg.Connection,
        surface: str,
        outcome: Any,
    ) -> SubstrateError:
        decision = outcome.decision
        message = f"Nomos denied {surface}: {decision.reason_code}"
        data: dict[str, Any] = {
            "surface": surface,
            "decision_id": decision.decision_id,
            "effect": decision.effect,
            "final_effect": outcome.final_effect,
            "reason_code": decision.reason_code,
            "missing_obligations": list(outcome.missing_obligations),
        }
        fix_hint = (
            "activate an appropriate Nomos policy for this deployment "
            "or provide a subject with permission for this MCP read"
        )
        # fix-nomos-read-path-ux (req.readux.deny-names-grants, #640): a
        # read-surface deny names the presented subject and the subjects
        # the ACTIVE bundle grants for that surface, and the fix_hint
        # points at presenting a granted subject when a policy IS active.
        # Read-only enumeration; enrichment failure keeps the legacy hint.
        from okg.substrate.nomos import (
            describe_read_surface_deny,
            is_read_deny_surface,
        )

        if is_read_deny_surface(surface):
            detail = describe_read_surface_deny(
                conn,
                deployment_name=deployment_name,
                scope=f"deployment:{deployment_name}",
                surface=surface,
                presented_subject=decision.subject,
                subject_env_hint=("OKG_MCP_NOMOS_SUBJECT (or OKG_NOMOS_SUBJECT)"),
            )
            data.update(detail["data"])
            if detail["fix_hint"]:
                fix_hint = detail["fix_hint"]
            if detail["summary"]:
                message = f"{message} — {detail['summary']}"
        err = SubstrateError(message, data=data, fix_hint=fix_hint)
        err.code = "nomos_runtime_denied"
        return err

    def _nomos_identity_denied(surface: str, result: Any) -> SubstrateError:
        decision = result.decision
        err = SubstrateError(
            f"Nomos denied {surface}: {decision.reason_code}",
            data={
                "surface": surface,
                "decision_id": decision.decision_id,
                "effect": decision.effect,
                "reason_code": decision.reason_code,
                "resolved_subject": result.resolved_subject,
                "binding_ids": list(result.binding_ids),
                "permission_grant_ids": list(result.permission_grant_ids),
            },
            fix_hint=(
                "provide a valid Nomos identity claim with explicit MCP "
                "boundary and source grants for this deployment"
            ),
        )
        err.code = "nomos_identity_denied"
        return err

    def _authorize_mcp_runtime_boundary(
        conn: psycopg.Connection,
        *,
        session: Session,
        surface: str,
        operator_name: str,
        resource: Mapping[str, Any] | None = None,
        authority: Mapping[str, Any] | None = None,
        identity_claim_override: Mapping[str, Any] | None = None,
    ) -> tuple[tuple[str, ...], str | None]:
        from okg.substrate.nomos import (
            NomosRuntimeRequest,
            deployment_requires_nomos_runtime,
            evaluate_runtime_boundary_with_active_policy,
        )

        # In multi-user mode the header-derived claim is the SOLE identity
        # input; the caller-supplied authority channel was already rejected
        # upstream, so there is nothing here to merge or prefer.
        raw_claim = (
            identity_claim_override
            if identity_claim_override is not None
            else _caller_identity_claim(authority)
        )
        if not deployment_requires_nomos_runtime(deployment_name):
            return (), None
        # The volatile publish coordinates stay OUT of the identity-bearing
        # resource (issue #635: hashing them re-mints every boundary decision
        # once per publish); they are recorded on the audit-only target.
        base_resource = {
            "deployment": deployment_name,
            "scope": f"deployment:{deployment_name}",
            "operator_name": operator_name,
            **dict(resource or {}),
        }
        audit_stamp = {
            "generation_id": session.generation_id,
            "branch_name": session.branch_name,
            "catalog_version_id": session.catalog_version_id,
        }
        decision_ids: list[str] = []
        boundary_targets = (
            ("mcp.tool.call", "mcp_tool_call"),
            (surface, "mcp_graph_read"),
        )
        if raw_claim is not None:
            from okg.substrate.nomos import evaluate_identity_claim_from_postgres

            if identity_claim_override is not None:
                # An omitted verification_status would default to `valid`
                # inside normalize_identity_claim -- an absent signal read as
                # a satisfied one. Refuse before the machinery sees it.
                from okg.substrate.mcp.principal_mapping import (
                    require_explicit_verification_status,
                )

                try:
                    require_explicit_verification_status(raw_claim)
                except SubstrateError as exc:
                    _persist_principal_refusal(
                        conn,
                        error=exc,
                        deployment_name=deployment_name,
                        session=session,
                        surface=surface,
                        operator_name=operator_name,
                        resource=base_resource,
                    )
                    raise
            resolved_subject: str | None = None
            # Every surface's class is settled before any decision row is
            # written, so a refused declaration leaves no partial audit.
            classifications = {
                boundary_surface: _evaluated_classification(
                    boundary_surface, authority
                )
                for boundary_surface, _target_kind in boundary_targets
            }
            for boundary_surface, target_kind in boundary_targets:
                classification = classifications[boundary_surface]
                boundary_resource = dict(base_resource)
                if boundary_surface == "mcp.tool.call":
                    boundary_resource["resource_kind"] = "mcp_tool"
                elif target_kind == "mcp_graph_read":
                    boundary_resource["resource_kind"] = "mcp_graph_read_boundary"
                try:
                    result = evaluate_identity_claim_from_postgres(
                        conn,
                        deployment_name=deployment_name,
                        bundle=None,
                        claim=raw_claim,
                        action=boundary_surface,
                        resource=boundary_resource,
                        scope=f"deployment:{deployment_name}",
                        classification=classification,
                        enforcement_point="mcp.identity_boundary",
                        target={
                            "kind": target_kind,
                            "tool": operator_name,
                            "surface": boundary_surface,
                            **audit_stamp,
                        },
                    )
                except Exception as exc:
                    # The claim evaluator may fail before Nomos renders any
                    # decision (for example, when this pooled connection was
                    # severed by a database restart). Preserve fail-closed
                    # behavior, but report an unavailable/evaluation-failed
                    # authority check rather than a policy denial.
                    raise _authority_evaluation_error(boundary_surface, exc) from exc
                decision_ids.append(result.decision.decision_id)
                conn.commit()
                if identity_claim_override is not None and not result.resolved_subject:
                    # The claim reached no Nomos subject. That is "nobody was
                    # identified", NOT "the policy said no": the existing
                    # nomos.identity.no_subject_binding code is emitted with
                    # effect deny and would read as a policy decision in the
                    # audit record. Record the distinct refusal instead.
                    from okg.substrate.mcp.principal_mapping import (
                        unidentified_principal_error,
                    )

                    refusal = unidentified_principal_error(
                        "the principal asserted in the declared header maps "
                        "to no Nomos subject for this deployment",
                        channel="no_subject_binding",
                        data={
                            "boundary_surface": boundary_surface,
                            "identity_reason_code": (result.decision.reason_code),
                        },
                        fix_hint=(
                            "bind the principal to a subject with `okg "
                            "nomos binding add` (`okg nomos binding list` "
                            "shows disabled and expired bindings), or correct "
                            "the principal the fronting application sends"
                        ),
                    )
                    _persist_principal_refusal(
                        conn,
                        error=refusal,
                        deployment_name=deployment_name,
                        session=session,
                        surface=surface,
                        operator_name=operator_name,
                        resource=base_resource,
                    )
                    raise refusal
                if (
                    result.decision.effect in {"deny", "review", "quarantine"}
                    or not result.resolved_subject
                ):
                    raise _nomos_identity_denied(boundary_surface, result)
                if resolved_subject is None:
                    resolved_subject = result.resolved_subject
                elif resolved_subject != result.resolved_subject:
                    raise _nomos_identity_denied(boundary_surface, result)
            return tuple(decision_ids), resolved_subject

        subject = _resolve_mcp_read_subject(
            conn,
            deployment_name=deployment_name,
        )
        for boundary_surface, target_kind in boundary_targets:
            boundary_resource = dict(base_resource)
            if boundary_surface == "mcp.tool.call":
                boundary_resource["resource_kind"] = "mcp_tool"
            elif target_kind == "mcp_graph_read":
                boundary_resource["resource_kind"] = "mcp_graph_read_boundary"
            try:
                outcome = evaluate_runtime_boundary_with_active_policy(
                    conn,
                    deployment_name=deployment_name,
                    scope=f"deployment:{deployment_name}",
                    request=NomosRuntimeRequest(
                        surface=boundary_surface,
                        subject=subject,
                        resource=boundary_resource,
                    ),
                    target={
                        "kind": target_kind,
                        "tool": operator_name,
                        "surface": boundary_surface,
                        **audit_stamp,
                    },
                    performed_obligations=("audit",),
                )
            except Exception as exc:
                raise _authority_evaluation_error(boundary_surface, exc) from exc
            assert outcome is not None
            decision_ids.append(outcome.decision.decision_id)
            conn.commit()
            if not outcome.allowed:
                raise _nomos_runtime_denied(conn, boundary_surface, outcome)
        return tuple(decision_ids), subject

    class _PooledSession:
        """Borrow one DB connection under an exact call/session pin."""

        def __init__(
            self,
            pin: Optional[object],
            *,
            surface: str,
            operator_name: str,
            resource: Mapping[str, Any] | None = None,
            authority: Mapping[str, Any] | None = None,
            refresh_default: bool = False,
            reference: str | None = None,
        ):
            # `pin` is an explicit generation pin from a caller -- a Chronos
            # generation_id string (or a legacy int), not necessarily an int.
            self.pin = pin
            # A result reference id (#3222): the session opens at its pin,
            # after validation steps 1-8 (`_open_reference`).
            self.reference = reference
            self.reference_entry: Any = None
            # The server-resolved header claim of this request (multi-user
            # only, else None): the one value both a producer and a consumer
            # of a result reference key the reader and the principal on.
            self.request_header_claim: Mapping[str, Any] | None = None
            self.surface = surface
            self.operator_name = operator_name
            self.resource = dict(resource or {})
            self.authority = dict(authority or {})
            self.refresh_default = refresh_default
            self.nomos_identity_claim = _caller_identity_claim(self.authority)
            self.nomos_access_context = _nomos_access_context_from_authority(
                self.authority,
            )
            self.conn: Optional[psycopg.Connection] = None
            self.session: Optional[Session] = None
            self.refresh_old_generation_id: object | None = None
            self.nomos_decision_ids: tuple[str, ...] = ()
            # PACT mcp-principal-mapping: one MCP request is one memo scope.
            # Per-request identity resolution reads the same providers,
            # bindings, grants and policy bundle at the tool boundary and
            # again for every row; the scope makes that one read. It is
            # entered for EVERY request, multi-user or not, because the
            # boundary evaluation has the same shape in both modes.
            self._memo_scope: Any = None

        def _resolve_request_principal(self) -> Mapping[str, Any] | None:
            """The per-request identity claim, or None off multi-user mode.

            Runs before any graph row is read, so a refusal returns zero
            rows by construction rather than by filtering them out again.
            """
            if not multi_user_principals:
                return None
            from okg.substrate.mcp.principal_mapping import (
                conflicting_identity_channel_error,
            )

            assert self.conn is not None
            if self.nomos_identity_claim is not None:
                # Precedence, decided rather than collided: the declared
                # header mapping is the sole identity input, so a
                # self-asserted claim is rejected outright -- not merged,
                # not preferred, and never evaluated (evaluating it would
                # persist a self-assertion as identity evidence).
                error = conflicting_identity_channel_error(
                    data={
                        "tool": self.operator_name,
                        "surface": self.surface,
                        "identity_channel": _identity_channel_for(
                            principal_mapping
                        ),
                    },
                )
                _persist_principal_refusal(
                    self.conn,
                    error=error,
                    deployment_name=deployment_name,
                    surface=self.surface,
                    operator_name=self.operator_name,
                )
                raise error
            try:
                return _request_identity_claim(principal_mapping)
            except SubstrateError as exc:
                _persist_principal_refusal(
                    self.conn,
                    error=exc,
                    deployment_name=deployment_name,
                    surface=self.surface,
                    operator_name=self.operator_name,
                )
                raise

        def _latest_session(self, *, use_cache: bool) -> Session:
            assert self.conn is not None
            (
                gen_id,
                cv_id,
                compat_generation_id,
                generation_branch,
            ) = _resolve_latest(
                self.conn,
                use_cache=use_cache,
            )
            return Session(
                deployment_name=deployment_name,
                generation_id=gen_id,
                catalog_version_id=cv_id,
                db_fingerprint=_db_fingerprint(self.conn),
                branch_name=branch_name,
                branch_id=_resolve_branch_id_cached(self.conn),
                generation_branch_name=generation_branch,
                compat_generation_id=compat_generation_id,
                nomos_access_context=self.nomos_access_context,
                nomos_multi_user=multi_user_principals,
            )

        def _finish_session(
            self,
            header_claim: Mapping[str, Any] | None,
        ) -> None:
            """Apply branch and authority state before a pin becomes visible."""

            assert self.conn is not None
            assert self.session is not None
            from okg.substrate.mcp.session import apply_branch_pin

            apply_branch_pin(self.conn, self.session)
            (
                self.nomos_decision_ids,
                nomos_subject,
            ) = _authorize_mcp_runtime_boundary(
                self.conn,
                session=self.session,
                surface=self.surface,
                operator_name=self.operator_name,
                resource=self.resource,
                authority=self.authority,
                identity_claim_override=header_claim,
            )
            if (
                nomos_subject is not None
                or self.nomos_identity_claim is not None
                or self.nomos_access_context is not None
            ):
                self.session = replace(
                    self.session,
                    nomos_subject=nomos_subject,
                    nomos_identity_claim=self.nomos_identity_claim,
                    nomos_access_context=self.nomos_access_context,
                    nomos_multi_user=multi_user_principals,
                )

        def _open_exact(
            self,
            pin: object,
            header_claim: Mapping[str, Any] | None,
        ) -> None:
            assert self.conn is not None
            self.session = open_session(
                self.conn,
                deployment_name=deployment_name,
                requested_generation=pin,
                branch_name=branch_name,
                nomos_access_context=self.nomos_access_context,
                nomos_multi_user=multi_user_principals,
            )
            self._finish_session(header_claim)

        def _open_reference(
            self,
            header_claim: Mapping[str, Any] | None,
        ) -> None:
            """Validate a result reference and open its pin (design 4).

            Steps 1-4 (form, store, MAC, expiry, this reader's entry) before
            anything is read; 5, the caller's `generation_id` resolved and
            compared with the pin; 6, the pin opened; 7, the binding
            compared; 8, the members' attribute digest compared. Every
            refusal comes before any member row is read.
            """
            assert self.conn is not None
            reader = _continuation.reader_key(
                self.authority, header_claim, multi_user_principals,
            )
            entry = _result_references.store().lookup(self.reference, reader_key=reader)
            self.reference_reader = reader
            pin = entry.binding.get("generation")
            if self.pin is not None:
                try:
                    resolved = resolve_generation(
                        self.conn, deployment_name,
                        requested=self.pin, branch_name=branch_name,
                    )[0]
                except SubstrateError as exc:
                    raise _result_references.invalid("generation_conflict") from exc
                if str(resolved) != str(pin):
                    raise _result_references.invalid("generation_conflict")
            try:
                self.session = open_session(
                    self.conn,
                    deployment_name=deployment_name,
                    requested_generation=pin,
                    branch_name=branch_name,
                    nomos_access_context=self.nomos_access_context,
                    nomos_multi_user=multi_user_principals,
                )
            except SubstrateError as exc:
                raise _result_references.invalid(
                    "generation_unavailable",
                    cause=str(getattr(exc, "code", None) or "generation_unavailable"),
                ) from exc
            self._finish_session(header_claim)
            assert self.session is not None
            field = _result_references.binding_mismatch(
                entry.binding, _reference_binding(self.conn, self.session),
            )
            if field is not None:
                raise _result_references.invalid("binding_mismatch", field=field)
            digest = _member_attribute_digest(
                self.conn, self.session, [m["node_id"] for m in entry.members],
            )
            if digest != entry.binding.get("attributes"):
                raise _result_references.invalid("binding_mismatch", field="attributes")
            self.reference_entry = entry

        def _open_bound_default(
            self,
            identity: object,
            header_claim: Mapping[str, Any] | None,
        ) -> None:
            """Open/bind the logical default with first-bind linearization."""

            with logical_sessions.locked(identity) as lease:
                state = lease.state
                if state.default_generation_id is not None:
                    # Existing calls linearize when they copy this value. The
                    # DB open happens after releasing the state lock; a
                    # concurrent refresh may move the default but cannot alter
                    # this call's copied exact generation.
                    pin = state.default_generation_id
                else:
                    try:
                        # First bind is always uncached and the state lock stays
                        # held through Session construction + authority setup.
                        self.session = self._latest_session(use_cache=False)
                        self._finish_session(header_claim)
                    except Exception:
                        # An earlier validation refusal or explicit-only call
                        # may have created this still-empty state for public
                        # metadata, so `lease.created` is not a safe proxy for
                        # first-bind ownership. Any failed attempt while the
                        # default is empty invalidates the canonical value and
                        # makes every waiter restart lookup.
                        logical_sessions.invalidate_locked(identity, state)
                        raise
                    assert self.session is not None
                    state.default_generation_id = self.session.generation_id
                    return
            self._open_exact(pin, header_claim)

        def _refresh_bound_default(
            self,
            identity: object,
            header_claim: Mapping[str, Any] | None,
        ) -> None:
            """Atomically replace only the logical default after full setup."""

            with logical_sessions.locked(identity) as lease:
                state = lease.state
                old_generation_id = state.default_generation_id
                # Do not write state until every fallible operation succeeds;
                # lookup/open/authority failure therefore preserves the old pin.
                self.session = self._latest_session(use_cache=False)
                self._finish_session(header_claim)
                assert self.session is not None
                state.default_generation_id = self.session.generation_id
                self.refresh_old_generation_id = old_generation_id

        def __enter__(self) -> "_PooledSession":
            waited_from = time.monotonic()
            self.conn = _pool_getconn(pool)
            opened_from = time.monotonic()
            try:
                return self._enter_with_connection()
            finally:
                # #3666: where a date call's time before its first date
                # work went (`_date_budget` takes these as phases).
                clock = _current_date_call()
                if clock is not None:
                    clock.pool_wait_ms += (opened_from - waited_from) * 1000
                    clock.session_open_ms += (time.monotonic() - opened_from) * 1000

        def _enter_with_connection(self) -> "_PooledSession":
            self._memo_scope = read_pass_scope()
            self._memo_scope.__enter__()
            self._run_timeout_prev = None
            self._run_conn_token = None
            try:
                self._open_run_budget()
                header_claim = self._resolve_request_principal()
                self.request_header_claim = header_claim
                if header_claim is not None:
                    self.nomos_identity_claim = header_claim
                identity = _current_logical_identity()
                if self.reference is not None:
                    self._open_reference(header_claim)
                elif self.refresh_default:
                    assert identity is not None
                    self._refresh_bound_default(identity, header_claim)
                elif self.pin is not None:
                    # A non-null caller pin is authoritative under every
                    # supported policy and never writes the logical default.
                    self._open_exact(self.pin, header_claim)
                elif identity is not None:
                    self._open_bound_default(identity, header_claim)
                else:
                    # Honest request-scoped compatibility path. "never" keeps
                    # the legacy TTL cache; "always" is uncached. The public
                    # wrapper labels both as request-scoped and warns that an
                    # omitted pin cannot persist.
                    self.session = self._latest_session(
                        use_cache=advance_mode == "never",
                    )
                    self._finish_session(header_claim)
            except Exception as exc:  # noqa: BLE001 — branch-pin failure must release the pooled conn before propagating; re-raises below.
                _run_steps.note_cancelled(exc)
                try:
                    self._close_run_budget()
                finally:
                    pool.putconn(self.conn)
                self.conn = None
                self._close_memo_scope()
                raise
            return self

        def _open_run_budget(self) -> None:
            """Inside a `run` step (#3666): every statement on this
            connection runs under what is left of the step's budget, and a
            row check reads a set on it."""
            ms = _run_steps.statement_budget_ms()
            if ms is None:
                return
            with self.conn.cursor() as cur:
                cur.execute("SELECT current_setting('statement_timeout')")
                previous = cur.fetchone()[0]
                cur.execute(
                    "SELECT set_config('statement_timeout', %s, false)", (f"{int(ms)}ms",),
                )
            self._run_timeout_prev = previous
            self._run_conn_token = _run_steps.bind_connection(self.conn)
            # The timeout above is per statement; a read that runs several
            # statements is also cut at the budget's deadline itself.
            self._run_watch = _run_steps.watch_deadline(self.conn)

        def _close_run_budget(self) -> None:
            watch, self._run_watch = getattr(self, "_run_watch", None), None
            # Once stop() returns no cancel is sent; one already sent may
            # not have reached the server yet.
            fired = watch.stop() if watch is not None else False
            token, self._run_conn_token = getattr(self, "_run_conn_token", None), None
            if token is not None:
                _run_steps.unbind_connection(token)
            previous, self._run_timeout_prev = getattr(self, "_run_timeout_prev", None), None
            if fired and self.conn is not None:
                # A late cancel could cut the next borrower's statement: the
                # connection is closed, and the pool discards a closed one.
                try:
                    self.conn.close()
                except Exception:  # noqa: BLE001 - already broken; the pool discards it either way.
                    pass
                return
            if previous is None or self.conn is None:
                return
            # A connection whose timeout cannot be put back is closed, and
            # the pool discards a closed connection.
            _run_steps.restore_timeout(self.conn, previous)

        def _close_memo_scope(self) -> None:
            scope, self._memo_scope = self._memo_scope, None
            if scope is not None:
                scope.__exit__(None, None, None)

        def __exit__(self, exc_type, exc, tb) -> None:
            if self.conn is not None:
                if exc is not None:
                    _run_steps.note_cancelled(exc)
                try:
                    self._close_run_budget()
                finally:
                    pool.putconn(self.conn)
                self.conn = None
            self._close_memo_scope()

    def _open(
        pin: Optional[Union[int, str]],
        *,
        surface: str = "graph.read.mcp",
        operator_name: str = "mcp.read",
        resource: Mapping[str, Any] | None = None,
        authority: Mapping[str, Any] | None = None,
        refresh_default: bool = False,
        reference: str | None = None,
    ) -> _PooledSession:
        return _PooledSession(
            pin,
            surface=surface,
            operator_name=operator_name,
            resource=resource,
            authority=authority,
            refresh_default=refresh_default,
            reference=reference,
        )

    def _reference_binding(
        conn: psycopg.Connection, session: Session,
    ) -> dict[str, Any]:
        """What a result reference is bound to besides its reader (D2, H3):
        the same fields a continuation token binds."""
        authority_digest, policy_fingerprint = _continuation.authority_fingerprint(
            conn, session,
        )
        return {
            "deployment": session.deployment_name,
            "database": session.db_fingerprint,
            "branch": session.branch_name,
            "generation": session.generation_id,
            "catalog_version": session.catalog_version_id,
            "authority": authority_digest,
            "policy": policy_fingerprint,
        }

    def _live_binding(
        conn: psycopg.Connection, session: Session,
    ) -> dict[str, Any]:
        """What a retained live set is bound to: the reference binding
        without the generation, since a live answer is not pinned to one."""
        return {
            key: value for key, value in _reference_binding(conn, session).items()
            if key != "generation"
        }

    def _member_attribute_digest(
        conn: psycopg.Connection, session: Session, node_ids: list[str],
    ) -> str:
        """A digest of the members' effective-attribute overlay rows at the
        pin (D10): an observation accepted after production changes it."""
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT target_id, attr_key, value::text FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
                "WHERE target_kind = 'node' AND generation_id = %(rr_generation)s "
                "AND namespace = 'default' "
                "AND target_id = ANY(%(rr_digest_ids)s::text[]) "
                "ORDER BY target_id, attr_key",
                {
                    "rr_generation": str(session.generation_id),
                    "rr_digest_ids": sorted(set(node_ids)),
                },
            )
            rows = [list(row) for row in cur.fetchall()]
        return hashlib.sha256(
            _json.dumps(rows, separators=(",", ":"), default=str).encode("utf-8"),
        ).hexdigest()

    @contextmanager
    def _open_logical(
        reference_generation_id: int | str,
        *,
        branch_name: str = branch_name,
        branch_id: str = "default",
        nomos_subject: str | None = None,
        nomos_identity_claim: Mapping[str, Any] | None = None,
        nomos_access_context: Mapping[str, Any] | None = None,
    ):
        """Borrow a conn and build a Session whose `generation_id`
        is the caller-supplied reference — *not* validated against
        graph_generations.status='published'.

        Used by the change-observation tools (`list_generations_since`,
        `changes_since`) where the input is a "what's new since X?"
        watermark, not a pinned read. The strict open_session()
        flow rejects references to retired / never-published
        generations, which makes those tools unusable for the
        "I last saw gen X, has it advanced?" workflow that they
        exist to serve.
        """
        conn = _pool_getconn(pool)
        try:
            fp = _db_fingerprint(conn)
            session = Session(
                deployment_name=deployment_name,
                generation_id=reference_generation_id,
                catalog_version_id=0,  # unused by these tools
                db_fingerprint=fp,
                branch_name=branch_name,
                branch_id=branch_id,
                nomos_subject=nomos_subject,
                nomos_identity_claim=nomos_identity_claim,
                nomos_access_context=nomos_access_context,
                # The server's multi-user state travels onto EVERY session it
                # builds, not only the pooled ones. Sessions built here reach
                # the read filter through preview_advance ->
                # _visible_delta_counts; leaving the flag at its default
                # would disarm the filter's own refusal for exactly the
                # enforcement-flipped-off case that refusal exists for.
                nomos_multi_user=multi_user_principals,
            )

            class _H:
                pass

            h = _H()
            h.conn = conn  # type: ignore[attr-defined]
            h.session = session  # type: ignore[attr-defined]
            yield h
        finally:
            pool.putconn(conn)

    def _maybe_stamp_advanced(
        handle: _PooledSession,
        out: Any,
    ) -> Any:
        """Legacy call-site shim; implicit pin advancement is retired."""
        del handle
        return out

    # ---------- tool bindings ----------

    def _stamp_operator(payload: dict[str, Any], operator_name: str) -> dict[str, Any]:
        date_budget = _date_budget_in_flight()
        if date_budget is None:
            return _stamp_operator_reply(payload, operator_name)
        # #3666: the end of one run of a call that carried a date condition.
        # What it did after its date work (row fencing, projection) and the
        # stamp below are not under the budget; the log line says what they
        # took.
        date_budget.mark("shape")
        try:
            return _stamp_operator_reply(payload, operator_name)
        finally:
            date_budget.mark("stamp")
            call = _current_date_call()
            if call is not None:
                # The wrapper may run the body again to fit the page; it
                # ends the call's budget when the reply is produced.
                call.served = True
            else:
                date_budget.finish("served")
                _date_budget_of_call.set(None)

    def _stamp_operator_reply(payload: dict[str, Any], operator_name: str) -> dict[str, Any]:
        payload.setdefault("operator_name", operator_name)
        payload = with_read_coverage(payload)
        add_pagination_hint(payload, operator_name)
        # Tier freshness honesty (worktree-access-tiers): every read-shaped
        # response says how old its sources are; a pinned snapshot must not
        # impersonate the present. Best-effort + 60s-cached; never breaks
        # the read.
        from okg.substrate.mcp.freshness import READ_OPERATORS

        if operator_name in READ_OPERATORS:
            gen_id = payload.get("pinned_generation_id") or payload.get("generation_id")
            try:
                with _pool_connection(pool) as conn:
                    _stamp_freshness_outcomes(
                        payload,
                        operator_name,
                        conn=conn,
                        deployment_name=deployment_name,
                        generation_id=gen_id,
                    )
            except Exception:  # noqa: BLE001 - freshness must not break reads.
                _stamp_unavailable_freshness(
                    payload,
                    deployment_name=deployment_name,
                )
        return payload

    def _worst_case_stamped_bytes(
        payload: dict[str, Any],
        operator_name: str,
        arguments: dict[str, Any],
        *,
        tool: str,
        kind: str | None = None,
    ) -> int:
        """Served bytes of ``payload`` under the largest freshness outcome.

        A page cut against a stamp reserve must not move when the freshness
        read fails on a later serve, or fails inside this call after the
        measurement. The payload is stamped once (one freshness read); then
        each freshness block is also measured in the unavailable shape the
        failure path stamps (`_stamp_unavailable_freshness`), alone and both
        together, in the call's own format, and the largest size is
        returned. Pure after the one stamping: no further database read.
        """
        from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes

        stamped = _stamp_operator(dict(payload), operator_name)
        unavailable: dict[str, Any] = {}
        _stamp_unavailable_freshness(unavailable, deployment_name=deployment_name)
        sizes = []
        for swapped in ((), ("source_freshness",), ("publish_freshness",),
                        ("source_freshness", "publish_freshness")):
            shape = dict(stamped)
            for name in swapped:
                if name in shape:
                    shape[name] = unavailable[name]
            sizes.append(public_call_tool_result_bytes(
                _logical_response_measurement(shape, arguments), tool=tool, kind=kind,
            ))
        return max(sizes)

    def _operator_refusal(
        operator_name: str,
        *,
        code: str,
        message: str,
        fallback_operator: str | None = None,
        authority_evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        error: dict[str, Any] = {
            "code": code,
            "message": message,
        }
        if operator_name == "filter":
            error["schema_hint"] = FILTER_SCHEMA_HINT
        elif operator_name == "map":
            error["schema_hint"] = (
                "Use map(input={kind:'nodes'|'edges'}, projection=[<stable field>, ...]) "
                "or a mapping from safe output names to stable fields."
            )
        if fallback_operator is not None:
            registered_tools = set(mcp._tool_manager._tools)  # type: ignore[attr-defined]
            if fallback_operator in registered_tools:
                error["fallback_operator"] = fallback_operator
                error["fix_hint"] = (
                    f"Use {fallback_operator} for this shape until the "
                    f"{operator_name} operator supports it directly."
                )
            else:
                error["fallback_suppressed"] = True
                error["allowed_operators"] = sorted(registered_tools)
                error.setdefault(
                    "fix_hint",
                    (
                        f"Use the supported {operator_name} schema shown in "
                        "schema_hint; this server does not expose a broader "
                        "fallback operator."
                    ),
                )
        if authority_evidence is not None:
            error["authority"] = authority_evidence
        return {
            "operator_name": operator_name,
            "status": "refused",
            "error": error,
        }

    def _semantic_corpus_refusal(
        exc: SubstrateError,
        *,
        method: str,
    ) -> dict[str, Any] | None:
        """Typed operator refusal for a semantic corpus refusal, else None.

        The classification and payload live in
        `mcp.semantic_readiness.semantic_corpus_refusal_error` so they are
        provable without a database; this composes them with the server's own
        refusal envelope. Returns None for anything else so the caller
        re-raises — converting an unrecognised error into a capability
        report would turn a bug into a reassuring message.
        """
        from okg.substrate.mcp.semantic_readiness import (
            semantic_corpus_refusal_error,
        )

        error = semantic_corpus_refusal_error(exc, method=method)
        if error is None:
            return None
        refusal = _operator_refusal(
            "search",
            code=error["code"],
            message=error["message"],
            fallback_operator="search",
        )
        refusal["error"].update(error)
        return refusal

    def _authorize_propose_write(
        *,
        action: str,
        authority: dict[str, Any],
        scope_alternative: str = "okg.propose.write",
    ) -> dict[str, Any] | None:
        # The real write gate is server-launch-time write mode (the
        # `_write_refusal` check in the calling tool): the agent cannot
        # turn writes on. So the agent-supplied `authority` dict is NO
        # LONGER a write-enabler — a self-asserted `allow_write` / scope /
        # principal is ignored (PACT least-privilege-mcp). The only
        # additional in-call gate is a Nomos identity claim, enforced when
        # one is present; otherwise the operator's launch-time consent
        # stands and the write is authorized.
        del scope_alternative  # retained for call-site compatibility
        nomos_evidence = _authorize_record_with_nomos(
            action=action,
            authority=authority,
        )
        if nomos_evidence is not None:
            if nomos_evidence.get("authorized") is True:
                return None
            return nomos_evidence
        return None

    def _authorize_record_with_nomos(
        *,
        action: str,
        authority: dict[str, Any],
    ) -> dict[str, Any] | None:
        # `propose submit` writes an admission just as `record` does, so it
        # crosses the same boundary: policy action mcp.record, with the tool
        # named in the resource. `decide` is the operator acting on an
        # envelope under launch consent and is not re-evaluated here.
        if action == "submit":
            tool_name, record_kind = "propose", "propose"
        elif action.startswith("record:"):
            tool_name, record_kind = "record", action.split(":", 1)[1]
        else:
            return None
        try:
            raw_claim = _caller_identity_claim(authority)
        except SubstrateError as exc:
            # No verdict was rendered — the claim never reached
            # evaluation — so this is `evaluated: false`, not a claimed
            # `authorized: false` (issue #950).
            return {
                "action": action,
                "evaluated": False,
                "nomos": {
                    "code": getattr(exc, "code", "nomos_identity_claim_invalid"),
                    "message": str(exc),
                },
            }
        if raw_claim is None:
            try:
                from okg.substrate.nomos import (
                    NomosRuntimeRequest,
                    deployment_requires_nomos_runtime,
                    evaluate_runtime_boundary_with_active_policy,
                )

                if not deployment_requires_nomos_runtime(deployment_name):
                    return None
                try:
                    classification = _evaluated_classification(
                        "mcp.record", authority
                    )
                except SubstrateError as exc:
                    return _pre_evaluation_refusal(action, exc)
                scope = f"deployment:{deployment_name}"
                resource = {
                    "deployment": deployment_name,
                    "scope": scope,
                    "surface": "mcp",
                    "tool": tool_name,
                    "record_kind": record_kind,
                }
                try:
                    refuse_client_policy_overrides(
                        authority,
                        scope=scope,
                        resource=resource,
                        surface="mcp.record",
                    )
                except SubstrateError as exc:
                    return _pre_evaluation_refusal(action, exc)
                with _pool_connection(pool) as conn:
                    # The same resolution the read boundary uses (#2157,
                    # #2343). Both refusals come before evaluation, so no
                    # decision row is written (evaluated: false).
                    try:
                        subject = _mcp_subject.require_mcp_subject(
                            conn,
                            deployment_name=deployment_name,
                            surface="mcp.record",
                        )
                    except SubstrateError as exc:
                        if exc.code == MCP_PRINCIPAL_UNRESOLVED:
                            # No identity, so no policy question.
                            return {
                                "action": action,
                                "evaluated": False,
                                "nomos": {
                                    "code": MCP_PRINCIPAL_UNRESOLVED,
                                    "message": _MCP_PRINCIPAL_UNRESOLVED_MESSAGE,
                                    "fix_hint": _MCP_PRINCIPAL_UNRESOLVED_HINT,
                                },
                            }
                        if exc.code not in (
                            _mcp_subject.AUTHORITY_UNAVAILABLE,
                            _mcp_subject.AUTHORITY_EVALUATION_FAILED,
                        ):
                            raise
                        # The policy could not be read to resolve the
                        # subject: an authority failure with a bounded
                        # message, not a switch to the login name.
                        return {
                            "action": action,
                            "evaluated": False,
                            "nomos": {
                                "code": (
                                    "nomos_authority_unavailable"
                                    if exc.code
                                    == _mcp_subject.AUTHORITY_UNAVAILABLE
                                    else "nomos_authority_evaluation_failed"
                                ),
                                "message": str(exc),
                            },
                        }
                    outcome = evaluate_runtime_boundary_with_active_policy(
                        conn,
                        deployment_name=deployment_name,
                        scope=scope,
                        request=NomosRuntimeRequest(
                            surface="mcp.record",
                            subject=subject,
                            resource=resource,
                            classification=classification,
                        ),
                        target={
                            "kind": "mcp_action",
                            "tool": tool_name,
                            "record_kind": record_kind,
                            "action": action,
                        },
                        performed_obligations=("audit",),
                    )
                    conn.commit()
                assert outcome is not None
            except Exception as err:  # noqa: BLE001
                return _authority_failure_evidence(action, err)
            decision = outcome.decision.to_dict()
            authority["_nomos_decision"] = decision
            if outcome.allowed:
                authority["actor"] = subject
                authority["subject"] = subject
                authority["producer_id"] = subject
                authority["producer_kind"] = "agent"
                return {"authorized": True, "nomos_decision": decision}
            return {
                "action": action,
                "authorized": False,
                "nomos_decision": decision,
                "required": {
                    "effect": "allow",
                    "policy_action": "mcp.record",
                    "scope": f"deployment:{deployment_name}",
                },
            }

        try:
            from okg.substrate.nomos import evaluate_identity_claim_from_postgres

            # Policy, scope, and resource are server-side (#2891): the
            # deployment's active Postgres bundle (bundle=None below), the
            # deployment scope, and the facts built here. A client that names
            # a bundle file, or a scope or resource fact that differs, is
            # refused before evaluation -- never loaded, never merged.
            scope = f"deployment:{deployment_name}"
            try:
                classification = _evaluated_classification("mcp.record", authority)
            except SubstrateError as exc:
                return _pre_evaluation_refusal(action, exc)
            resource = {
                "deployment": deployment_name,
                "scope": scope,
                "surface": "mcp",
                "tool": tool_name,
                "record_kind": record_kind,
            }
            try:
                refuse_client_policy_overrides(
                    authority,
                    scope=scope,
                    resource=resource,
                    surface="mcp.record",
                )
            except SubstrateError as exc:
                return _pre_evaluation_refusal(action, exc)
            # Nomos evaluation reads policy config (okg_mcp has blanket
            # okg SELECT) and appends two audit rows — nomos_external_claims
            # + nomos_decisions — which okg_mcp is granted INSERT on, just
            # like the mcp_call_log / mcp_trace_events audit writes. So it
            # runs on the read pool: authorization recording is part of the
            # serve path, not a graph mutation.
            with _pool_connection(pool) as conn:
                result = evaluate_identity_claim_from_postgres(
                    conn,
                    deployment_name=deployment_name,
                    bundle=None,
                    claim=raw_claim,
                    action="mcp.record",
                    resource=resource,
                    scope=scope,
                    classification=classification,
                    enforcement_point=f"mcp.{tool_name}",
                    target={
                        "kind": "mcp_action",
                        "tool": tool_name,
                        "record_kind": record_kind,
                        "action": action,
                    },
                )
        except Exception as err:  # noqa: BLE001
            return _authority_failure_evidence(action, err)

        decision = result.decision.to_dict()
        authority["_nomos_decision"] = decision
        authority["_nomos_identity"] = {
            "resolved_subject": result.resolved_subject,
            "binding_ids": list(result.binding_ids),
            "permission_grant_ids": list(result.permission_grant_ids),
            "claim_fingerprint": result.claim.claim_fingerprint,
        }
        if result.decision.effect == "allow":
            if result.resolved_subject:
                authority["actor"] = result.resolved_subject
                authority["subject"] = result.resolved_subject
                authority["producer_id"] = result.resolved_subject
            else:
                # Nomos allowed the write but named nobody: the caller's own
                # producer_id / actor / subject would be recorded as if Nomos
                # had resolved them. Drop them; the server default applies.
                for key in ("actor", "subject", "producer_id"):
                    authority.pop(key, None)
            authority["producer_kind"] = "agent"
            return {"authorized": True, "nomos_decision": decision}
        return {
            "action": action,
            "authorized": False,
            "nomos_decision": decision,
            "nomos_identity": authority["_nomos_identity"],
            "required": {
                "effect": "allow",
                "policy_action": "mcp.record",
                "scope": f"deployment:{deployment_name}",
            },
        }

    def _admission_client_wait_config() -> tuple[float, float]:
        try:
            timeout = float(os.getenv("OKG_MCP_ADMISSION_TIMEOUT_SECONDS", "5"))
        except ValueError:
            timeout = 5.0
        try:
            poll = float(os.getenv("OKG_MCP_ADMISSION_POLL_SECONDS", "0.25"))
        except ValueError:
            poll = 0.25
        return max(timeout, 0.0), max(poll, 0.05)

    def _publish_fork_after_accept(result: dict[str, Any], branch_name: str) -> None:
        """Synchronous publish-on-fork (worktree-fork-writes P2).

        A fast-track accept onto a non-default fork branch applied facts to that
        branch, but a read needs a PUBLISHED generation on the fork (else P1's
        read-through serves the base). The least-privilege ``app_rw`` role CANNOT
        publish, so enqueue a publisher cycle for the fork to the privileged
        worker and wait (bounded) -- synchronous from the agent's view, while the
        MCP stays least-privilege. Best-effort: the facts are already admitted, so
        a publish timeout/error only annotates the result; it never fails the
        write.
        """
        if not branch_name or branch_name == "default":
            return
        if str((result or {}).get("status") or "") != "accepted":
            return  # only fast-track-accepted writes applied facts to the fork
        try:
            from okg.substrate.runtime.admin import (
                client_enqueue_publisher_cycle,
            )

            timeout, poll = _admission_client_wait_config()
            pub = client_enqueue_publisher_cycle(
                deployment=deployment_name,
                dsn=dsn,
                branch_name=branch_name,
                timeout_seconds=timeout,
                poll_interval_seconds=poll,
            )
            summary = pub.summary or {}
            cycle_status = summary.get("status")
            # pub.status is the DBOS WORKFLOW status (SUCCESS/PENDING/ERROR);
            # cycle_status is the publish CYCLE outcome (published/blocked). The
            # fork write is readable ONLY when both succeeded.
            published = pub.status == "SUCCESS" and cycle_status == "published"
            result["fork_publish"] = {
                "branch": branch_name,
                "status": pub.status,
                "cycle_status": cycle_status,
                "published": published,
                "workflow_id": pub.workflow_id,
                "generation_id": summary.get("generation_id"),
                "blocked_reason": summary.get("blocked_reason"),
            }
            if not published:
                # Status honesty (worktree-fork-writes C1/C2): the write was
                # admitted onto the fork but is NOT yet readable (publish pending,
                # blocked, or failed). Downgrade the agent-visible status so a
                # caller reading only `status` does not treat it as readable.
                result["status"] = "accepted_unpublished"
        except Exception as exc:  # noqa: BLE001 -- publish is best-effort; write is admitted.
            result["fork_publish"] = {
                "branch": branch_name,
                "status": "error",
                "published": False,
                "error": str(exc)[:200],
            }
            result["status"] = "accepted_unpublished"

    def _submit_admission_payload(
        *,
        payload: dict[str, Any],
        producer_id: str,
        producer_kind: str,
        intent: str,
        idempotency_key: str | None,
        authority: dict[str, Any],
        check_summary: dict[str, Any] | None,
        branch_name: str = "default",
    ) -> dict[str, Any]:
        # Central choke point for EVERY submit path (record, propose, and the
        # queued admin client path): a caller-asserted fast_track must never
        # change the admission outcome, so downgrade it here regardless of which
        # tool built the authority block (harden-mcp-write-boundaries).
        # _sanitize_caller_admission_mode dropped any caller-supplied
        # target_branch_name above. Now re-pin the write onto the SERVER-pinned
        # session branch (from `mcp-serve --branch`), never a caller-supplied
        # one -- a fork session may not escape its branch, and a caller on a
        # default server may not route its write onto an arbitrary fork branch.
        # Default writes leave target_branch_name unset (NULL) so the default
        # admission path is byte-identical (worktree-fork-writes P2).
        authority = _sanitize_caller_admission_mode(authority or {})
        if branch_name and branch_name != "default":
            authority = {**authority, "target_branch_name": branch_name}
        authority = _normalize_promote_intent(authority, branch_name)
        from okg.substrate.images.values import ImageRefInvalid, ImageValueForged
        from okg.substrate.runtime.admission_workflows import (
            admission_submit_workflow,
            check_review_payload_images,
        )

        # Image values (#3046): a forged or malformed one, or a `file`
        # reference (file images come only from a connector's ingest run),
        # is refused here, as a structured refusal, before the payload
        # reaches a submit worker. `_run_submit` repeats the check for every
        # other caller.
        try:
            check_review_payload_images(payload)
        except (ImageRefInvalid, ImageValueForged) as exc:
            return {
                "deployment": deployment_name,
                "status": "refused",
                "error": exc.to_json()["error"],
            }

        try:
            workflow = admission_submit_workflow()
        except RuntimeError as local_error:
            from okg.substrate.runtime.admin import (
                client_enqueue_admission_submit,
            )

            timeout, poll = _admission_client_wait_config()
            try:
                queued = client_enqueue_admission_submit(
                    deployment=deployment_name,
                    dsn=dsn,
                    payload=payload,
                    producer_id=producer_id,
                    producer_kind=producer_kind,
                    intent=intent,
                    idempotency_key=idempotency_key,
                    authority=authority,
                    check_summary=check_summary,
                    timeout_seconds=timeout,
                    poll_interval_seconds=poll,
                )
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(str(local_error)) from exc
            if queued.output:
                result = {
                    **queued.output,
                    "workflow_id": queued.workflow_id,
                    "workflow_status": queued.status,
                }
            else:
                result = {
                    "deployment": deployment_name,
                    "status": ("queued" if queued.status == "PENDING" else "failed"),
                    "workflow_id": queued.workflow_id,
                    "workflow_status": queued.status,
                    "error": queued.error,
                }
        else:
            result = workflow(
                payload,
                producer_id,
                producer_kind,
                intent,
                idempotency_key,
                authority,
                check_summary,
            )
        _publish_fork_after_accept(result, branch_name)
        return result

    def _decide_admission_payload(
        *,
        admission_id: str,
        decision: str,
        actor: str,
        reason: str | None,
        verdict: dict[str, Any] | None,
    ) -> dict[str, Any]:
        from okg.substrate.runtime.admission_workflows import (
            admission_decide_workflow,
        )

        try:
            workflow = admission_decide_workflow()
        except RuntimeError as local_error:
            from okg.substrate.runtime.admin import (
                client_enqueue_admission_decide,
            )

            timeout, poll = _admission_client_wait_config()
            try:
                queued = client_enqueue_admission_decide(
                    deployment=deployment_name,
                    dsn=dsn,
                    admission_id=admission_id,
                    decision=decision,
                    actor=actor,
                    reason=reason,
                    verdict=verdict,
                    timeout_seconds=timeout,
                    poll_interval_seconds=poll,
                )
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(str(local_error)) from exc
            if queued.output:
                return {
                    **queued.output,
                    "workflow_id": queued.workflow_id,
                    "workflow_status": queued.status,
                }
            return {
                "deployment": deployment_name,
                "admission_id": admission_id,
                "status": "queued" if queued.status == "PENDING" else "failed",
                "workflow_id": queued.workflow_id,
                "workflow_status": queued.status,
                "error": queued.error,
            }
        return workflow(
            admission_id,
            decision,
            actor,
            reason,
            verdict,
        )

    def _resolve_record_reference_edges(lowered: dict[str, Any]) -> None:
        admission_payload = lowered.get("admission_payload")
        if not isinstance(admission_payload, dict):
            return
        edges = list(admission_payload.get("edges") or [])
        if not edges:
            return
        node_ids = {
            str(node.get("node_id"))
            for node in admission_payload.get("nodes") or []
            if isinstance(node, dict) and node.get("node_id")
        }
        external_ids = sorted(
            {
                str(edge.get("dst"))
                for edge in edges
                if isinstance(edge, dict)
                and edge.get("dst")
                and str(edge.get("dst")) not in node_ids
            }
        )
        live_ids: set[str] = set()
        if external_ids:
            conn = _pool_getconn(pool)
            try:
                # Chronos cutover: "does this dst already exist in the live graph?"
                # reads the current default-branch state of okg.graph_nodes via a
                # Chronos checkout.
                from okg.substrate.graph_store import ChronosGraphStore

                graph = ChronosGraphStore.from_connection(
                    conn, ensure_schema=False
                ).checkout()
                live_ids = {
                    str(row["node_id"])
                    for row in graph.query(
                        "SELECT node_id FROM okg.graph_nodes WHERE node_id = ANY(:ids)",
                        {"ids": external_ids},
                    )
                }
            finally:
                pool.putconn(conn)
        kept: list[dict[str, Any]] = []
        unresolved: list[str] = []
        for edge in edges:
            dst = str(edge.get("dst") or "")
            if dst in node_ids or dst in live_ids:
                kept.append(edge)
            elif dst:
                unresolved.append(dst)
        if kept:
            admission_payload["edges"] = kept
        else:
            admission_payload.pop("edges", None)
        if unresolved:
            warnings = list(lowered.get("warnings") or [])
            warnings.extend(
                f"unresolved_record_ref:{node_id}"
                for node_id in sorted(set(unresolved))
            )
            lowered["warnings"] = sorted(set(warnings))
            lowered["edge_count"] = len(kept)
            diagnostics = list(lowered.get("reference_diagnostics") or [])
            diagnostics.extend(
                {
                    "code": "unresolved_record_ref",
                    "reason": "endpoint_not_live",
                    "node_id": node_id,
                }
                for node_id in sorted(set(unresolved))
            )
            lowered["reference_diagnostics"] = diagnostics
            lowered["suppressed_edge_count"] = int(
                lowered.get("suppressed_edge_count") or 0
            ) + len(set(unresolved))
            record_summary = lowered.get("check_summary", {}).get("record")
            if isinstance(record_summary, dict):
                record_summary["edge_count"] = len(kept)
                record_summary["emitted_edge_count"] = len(kept)
                record_summary["suppressed_edge_count"] = lowered[
                    "suppressed_edge_count"
                ]
                record_summary["warnings"] = lowered["warnings"]
                record_summary["reference_diagnostics"] = diagnostics
            for node in admission_payload.get("nodes") or []:
                if not isinstance(node, dict):
                    continue
                attrs = node.get("attrs")
                if isinstance(attrs, dict):
                    attrs["validation_warnings"] = lowered["warnings"]

    def _record_catalog_constraints() -> dict[str, Any]:
        conn = _pool_getconn(pool)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT catalog_version_id
                      FROM okg.catalog_versions
                     WHERE status = 'applied'
                     ORDER BY catalog_version_id DESC
                     LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row is None:
                    return {}
                catalog_version_id = int(row[0])
                cur.execute(
                    """
                    SELECT name
                      FROM okg.subtypes
                     WHERE catalog_version_id = %s
                     ORDER BY name
                    """,
                    (catalog_version_id,),
                )
                active_subtypes = tuple(str(r[0]) for r in cur.fetchall())
                cur.execute(
                    """
                    SELECT parent_subtype, edge_type, child_subtype
                      FROM okg.narrowings
                     WHERE catalog_version_id = %s
                    """,
                    (catalog_version_id,),
                )
                narrowing_triples = tuple(
                    (str(src), str(edge), str(dst)) for src, edge, dst in cur.fetchall()
                )
        except Exception:  # noqa: BLE001 - metadata should not kill startup
            log.exception("failed to load record catalog constraints")
            return {}
        finally:
            pool.putconn(conn)
        return {
            "active_subtypes": active_subtypes,
            "narrowing_triples": narrowing_triples,
            "catalog_version_id": catalog_version_id,
        }

    # SQL-synthesis helpers extracted to mcp/operator_sql.py by
    # `extract-mcp-operator-sql` (2026-05). 8 pure functions — no
    # closures over pool / deployment_name / publication_router /
    # generation_id — so they lift cleanly out of build_server.
    # The names the v1 verb wrappers below call are re-imported as
    # leading-underscore closure locals. Operator predicates compile through
    # `compile_read_predicate` with the effective-attribute lowerer (#3201);
    # no literal renderer is left.
    from okg.substrate.mcp.aggregate_each import (
        facts as _each_facts,
        no_list_refusal as _each_no_list_refusal,
        parse_each_field as _parse_each_field,
        run_within_budget as _run_within_count_budget,
    )
    from okg.substrate.mcp.aggregate_periods import (
        DEADLINE_CHECK_EVERY as _DATE_DEADLINE_CHECK_EVERY,
        MAX_DISTINCT_DATE_ROWS as _MAX_DISTINCT_DATE_ROWS,
        DATE_MEANINGS as _DATE_MEANINGS,
        PeriodReport as _PeriodReport,
        fold_period_groups as _fold_period_groups,
        parse_period_field as _parse_period_field,
        period_not_declared_refusal as _period_not_declared_refusal,
        split_date_filter as _split_date_filter,
        too_many_rows_refusal as _period_too_many_rows_refusal,
    )
    from okg.substrate.mcp.date_conditions import (
        INDEX_ACCESS_RULES as _DATE_INDEX_ACCESS_RULES,
        INDEX_NOT_DECLARED_TS as _DATE_INDEX_NOT_DECLARED_TS,
        INDEX_NOT_NARROWABLE as _DATE_INDEX_NOT_NARROWABLE,
        ISO_ONLY_ACCESS_RULES as _ISO_ONLY_ACCESS_RULES,
        ISO_ONLY_SCOPE_TOO_LARGE as _ISO_ONLY_SCOPE_TOO_LARGE,
        LONG_VALUE as _DATE_LONG_VALUE,
        REMEDY_NAME_A_TYPE as _REMEDY_NAME_A_TYPE,
        REMEDY_NARROWER_CONDITION as _REMEDY_NARROWER_CONDITION,
        DateBudget as _DateBudget,
        names_a_type as _date_names_a_type,
        narrowing_predicate as _date_narrowing_predicate,
        window_keys as _date_window_keys,
        MAX_ODD_DATE_VALUES as _MAX_ODD_DATE_VALUES,
        date_leaf_fields as _date_leaf_fields,
        NAIVE_FORM as _DATE_NAIVE_FORM,
        STRICT_FORM as _DATE_STRICT_FORM,
        DateConditionReport as _DateConditionReport,
        date_leaves as _date_leaves,
        holds_date_condition as _holds_date_condition,
        leaf_windows as _date_leaf_windows,
        not_declared_refusal as _date_not_declared_refusal,
        odd_values_sql as _date_odd_values_sql,
        parse_date_field as _parse_date_field,
        resolve_leaf as _date_resolve_leaf,
        rewrite_date_leaves as _rewrite_date_leaves,
        scoped_subtypes as _date_scoped_subtypes,
        tally_odd_rows as _date_tally_odd_rows,
        too_many_odd_values_refusal as _date_too_many_odd_values_refusal,
        window_leaf_matches as _date_window_leaf_matches,
        window_leaf_verdict as _date_window_leaf_verdict,
        without_date_clauses as _without_date_clauses,
    )
    from okg.substrate.mcp.operator_sql import (
        DATE_WINDOW_OP as _DATE_WINDOW_OP,
        EFFECTIVE_VIEW_FIELD_COLUMNS as _EFFECTIVE_VIEW_FIELD_COLUMNS,
        CompiledPredicate as _CompiledPredicate,
        SqlLeaf as _SqlLeaf,
        as_operator_timestamp as _as_timestamp,
        compile_read_predicate as _compile_read_predicate,
        effective_attr_lowerer as _effective_attr_lowerer,
        effective_field_map_identity as _effective_field_map_identity,
        encode_nonfinite_operands as _encode_nonfinite_operands,
        holds_nul as _holds_nul,
        node_id_from_input as _node_id_from_input,
        scope_kind as _scope_kind,
    )
    from okg.substrate.transform.operators import (
        filter_program as _filter_program,
        lower_program_over_chronos as _lower_program_over_chronos,
        map_program as _map_program,
    )

    #: Row fields that are a digest of the stored record text. The read filter
    #: withholds them on rows whose text the caller may not read, but SQL
    #: evaluates a predicate, sort or group on the stored digest before that
    #: check, so a guessed text could be confirmed through which rows match.
    _TEXT_DIGEST_FIELD_NAMES = frozenset({"text_hash", "raw_text_hash"})

    def _text_digest_clause(clauses: Mapping[str, Any]) -> tuple[str, str] | None:
        """The (field, clause path) of the first text-digest reference, or None.

        Predicates name a field under ``field`` at any depth; ``order_by`` is
        ``'<field> [asc|desc]'``; ``by`` is a list of field names.
        """
        def walk(spec: Any, path: str) -> tuple[str, str] | None:
            if isinstance(spec, Mapping):
                field = spec.get("field")
                if isinstance(field, str) and field in _TEXT_DIGEST_FIELD_NAMES:
                    return field, f"{path}.field"
                for key, value in spec.items():
                    found = walk(value, f"{path}.{key}")
                    if found is not None:
                        return found
            elif isinstance(spec, (list, tuple)):
                for index, value in enumerate(spec):
                    found = walk(value, f"{path}[{index}]")
                    if found is not None:
                        return found
            return None

        for name, spec in clauses.items():
            if name == "order_by":
                token = str(spec).strip().split()[0] if spec else ""
                if token in _TEXT_DIGEST_FIELD_NAMES:
                    return token, "order_by"
                continue
            if name == "by":
                for index, value in enumerate(spec or ()):
                    if isinstance(value, str) and value in _TEXT_DIGEST_FIELD_NAMES:
                        return value, f"by[{index}]"
                continue
            found = walk(spec, name)
            if found is not None:
                return found
        return None

    def _projection_reads_text_digest(projection: Any) -> bool:
        """Whether a projection (or filter ``select``) takes any output column
        from a text digest, under its own name or an alias."""
        if isinstance(projection, Mapping):
            sources: Iterable[Any] = projection.values()
        elif isinstance(projection, (list, tuple)):
            sources = projection
        else:
            return False
        return any(
            isinstance(source, str) and source in _TEXT_DIGEST_FIELD_NAMES
            for source in sources
        )

    def _digest_projection_needs_served_rows(projection: Any) -> bool:
        """Under Nomos, a digest-sourced projection is read on the
        effective-attrs path: it projects each output column from the row the
        read filter served, so a withheld digest is null under any alias. The
        plain scan selects the stored digest straight into the alias."""
        return _projection_reads_text_digest(projection) and (
            deployment_requires_nomos_runtime(deployment_name)
        )

    def _text_digest_refusal(
        operator_name: str, **clauses: Any,
    ) -> tuple[str, dict[str, Any]] | None:
        """Under Nomos audit or enforce, a refusal for any clause that filters,
        sorts or groups on a text digest; None otherwise (Nomos off included).

        Checked before any SQL runs: the digest is of stored text, so a match
        on it would answer for text the read filter may withhold.
        """
        if not deployment_requires_nomos_runtime(deployment_name):
            return None
        found = _text_digest_clause(clauses)
        if found is None:
            return None
        field, clause = found
        message = (
            f"{operator_name}: `{field}` cannot be filtered, sorted or grouped "
            "on while Nomos read checks are active; it is a digest of stored "
            "text the caller may not be allowed to read"
        )
        return message, {"field": field, "clause": clause}

    def _text_digest_operator_refusal(
        operator_name: str, **clauses: Any,
    ) -> dict[str, Any] | None:
        refused = _text_digest_refusal(operator_name, **clauses)
        if refused is None:
            return None
        message, data = refused
        refusal = _operator_refusal(
            operator_name, code="unsupported_operator_form", message=message,
        )
        refusal["error"]["data"] = data
        refusal["error"]["fix_hint"] = (
            "drop the text_hash clause; read the record's text with inspect "
            "instead"
        )
        return refusal

    #: Whether this server's database folds like exactly (loose-match-3666):
    #: read once, by the first like read, and kept while the server runs.
    _like_database: dict[str, Optional[tuple[str, dict[str, Any]]]] = {}

    def _like_database_problem() -> Optional[tuple[str, dict[str, Any]]]:
        if "checked" not in _like_database:
            from okg.substrate.mcp import text_fold

            with _pool_connection(pool) as conn:
                def one_row(sql: str) -> dict[str, Any]:
                    cursor = conn.execute(sql)
                    names = [column.name for column in cursor.description]
                    return dict(zip(names, cursor.fetchone()))

                _like_database["checked"] = text_fold.check_database(one_row)
        return _like_database["checked"]

    def _loose_match_refusal(operator_name: str, *conditions: Any) -> dict[str, Any] | None:
        """A refusal for a like or like_exact test this read cannot serve
        exactly, before any statement of the read runs; None otherwise.

        Under Nomos `audit` or `enforce` the access rules cannot yet decide
        a substring test on the reader's view, so it is refused in one
        sentence and nothing else changes. On a database without the fold's
        built-ins (PostgreSQL below 17, not UTF8, a Unicode version its
        tables do not cover) it is refused by name.
        """
        from okg.substrate.mcp import text_fold

        if not text_fold.holds_like_clause(*conditions):
            return None
        if deployment_requires_nomos_runtime(deployment_name):
            message, data = (
                f"{operator_name}: {text_fold.ACCESS_RULES_MESSAGE}",
                {"reason": "loose_match_under_access_rules"},
            )
        else:
            problem = _like_database_problem()
            if problem is None:
                return None
            message, data = f"{operator_name}: {problem[0]}", problem[1]
        refusal = _operator_refusal(
            operator_name, code="unsupported_operator_form", message=message,
        )
        refusal["error"]["data"] = data
        refusal["error"]["fix_hint"] = "use eq, in, prefix or suffix"
        return refusal

    def _operator_needs_read_compiler(condition: Any) -> bool:
        """Whether a condition compares a field in a way only the read
        compiler runs (issue #3666, step 2c).

        The older translator (`transform.operators._predicate_to_expr`)
        lowers `eq`, `in`, `exists` and `contains` only. A condition with
        `prefix`, `suffix`, `lt`, `gt` or `between` anywhere in it is
        compiled on the effective-attribute relation instead, as one on an
        attribute already is, so `filter` takes on a column every
        comparison search's `where` takes.
        """
        if not isinstance(condition, Mapping):
            return False
        op = condition.get("op")
        if op in ("and", "or"):
            clauses = condition.get("clauses")
            return isinstance(clauses, list) and any(
                _operator_needs_read_compiler(clause) for clause in clauses
            )
        if op == "not":
            return _operator_needs_read_compiler(condition.get("clause"))
        return op in ("prefix", "suffix", "lt", "gt", "between", "like", "like_exact")

    def _operator_refs_attrs(spec: Any) -> bool:
        if isinstance(spec, str):
            return spec == "attrs" or spec.startswith("attrs.")
        if isinstance(spec, dict):
            return any(_operator_refs_attrs(value) for value in spec.values())
        if isinstance(spec, (list, tuple)):
            return any(_operator_refs_attrs(value) for value in spec)
        return False

    # scale-operator-attr-reads / req.effective-attrs-view (Layer 2): the flat
    # effective-attrs snapshot exposes the stable pushdown fields under the
    # column names in operator_sql.EFFECTIVE_VIEW_FIELD_COLUMNS (imported above
    # as _EFFECTIVE_VIEW_FIELD_COLUMNS), shared with the predicate lowerer.

    # schema-interval-narrow-split: interval/snapshot rows carry a bytea
    # ``attrs_hash`` reference instead of inline jsonb payloads; every reader
    # materializes ``attrs``/``raw_attrs`` through this version-store join.
    # USING merges the three key columns, so existing unqualified column
    # references (target_kind from either side; attrs/raw_attrs only on the
    # version side; subtype/namespace/generation predicates only on the base
    # side) keep resolving exactly as before the split.
    _ATTRIBUTE_VERSION_JOIN = (
        "JOIN okg.attribute_versions USING (target_kind, target_id, attrs_hash)"
    )

    # lane-h-mcp-filter-index-driven: passed interval completeness checks.
    #
    # The check below runs three full count(*)s over the pinned stab (2.5 s at
    # 527k nodes, twice per filter call). Its answer for one generation cannot
    # change while that generation is its branch's head, so a PASSED result is
    # remembered for the life of this server, under a key that changes
    # whenever the answer could:
    #
    #   (host, port, dbname)   -- another database is another authority;
    #   generation_id          -- minted fresh per publish (timestamp + random
    #                             suffix), so a republish or any new generation
    #                             is a new key;
    #   target_kind            -- node and edge are separate counts;
    #   branch_id, ordinal     -- re-resolved from okg.v_generation_interval_stab
    #                             on every call, so a re-registered generation
    #                             lands on a new key;
    #   head ordinal           -- only cached while ordinal == head. The only
    #                             deletes of interval rows are the retention
    #                             prune (CLOSED rows with to_ord <= the
    #                             keep-window floor, which is at most the head
    #                             ordinal, while every row stabbing the head has
    #                             to_ord > head) and a re-derivation of the
    #                             same ordinal in one transaction. A later
    #                             publish moves the head, so an older pin
    #                             recounts every call, as before;
    #   expected_count         -- the checkpoint's own node/edge count, re-read
    #                             every call.
    #
    # A refusal or a failure is never stored: the counts run again next call.
    # A stored pass expires after _INTERVAL_COMPLETE_CACHE_TTL_S.
    _INTERVAL_COMPLETE_CACHE_MAX = 256
    _interval_complete_cache: dict[tuple[Any, ...], float] = {}
    _interval_complete_lock = threading.Lock()

    def _interval_complete_cache_key(
        conn: psycopg.Connection,
        *,
        generation: str,
        target_kind: str,
        branch_id: Any,
        ordinal: int,
        head_ordinal: int | None,
        expected_count: int | None,
    ) -> tuple[Any, ...] | None:
        if head_ordinal is None or int(head_ordinal) != int(ordinal):
            return None
        info = conn.info
        return (
            info.host,
            info.port,
            info.dbname,
            generation,
            target_kind,
            str(branch_id),
            int(ordinal),
            int(head_ordinal),
            expected_count,
        )

    def _effective_view_source(
        conn: psycopg.Connection,
        generation_id: Any,
        *,
        target_kind: str | None = None,
        source_alias: str | None = None,
    ) -> tuple[str, str, str, dict[str, Any]]:
        """Pick the effective-view read source + its generation predicate.

        Returns ``(base_table, from_clause, gen_where, gen_params)`` — the
        ``from_clause`` is the base table joined to okg.attribute_versions (the
        payload store, schema-interval-narrow-split); ``base_table`` is the
        bare relation name for existence probes.

        ``source_alias`` is an internal read-model seam for search's lexical
        candidate supplement. When present, both the base relation and the
        generation predicate are qualified with that alias while the payload
        store is exposed as ``av``. The default unqualified form remains the
        established filter/map/aggregate shape.

        When OKG_IVM_INTERVAL_VIEW is on, read the SHARED interval table via a
        half-open stab (``from_ord <= G_ord < to_ord``) pinned to the generation's
        branch. Missing interval authority is a cutover correctness failure, not a
        silent snapshot fallback: an operator asked for the interval path and must
        see a loud error if the table or ordinal is absent. With the flag off, use
        the per-generation snapshot (``generation_id = ...``). Every column the
        readers reference (subtype/src/dst/edge_type/attrs/raw_attrs/raw_text_hash)
        resolves identically over both sources, so only the FROM target and
        generation predicate change. The returned names are fixed literals."""
        gen = str(generation_id)
        if os.environ.get("OKG_IVM_INTERVAL_VIEW", "1") == "1":
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT to_regclass('okg.attribute_effective_view_intervals')"
                )
                if cur.fetchone()[0] is None:
                    raise SubstrateError(
                        "interval effective-attribute reads requested but "
                        "okg.attribute_effective_view_intervals is missing",
                        data={"generation_id": gen},
                        fix_hint=(
                            "run the interval schema migration and one-time "
                            "backfill before enabling OKG_IVM_INTERVAL_VIEW"
                        ),
                    )
                # D2 amendment (a): okg_mcp has no grant on
                # okg.generation_ordinals. The read goes through the
                # TEXT-ref-keyed view, looked up BY the checkpoint ref.
                # An absent view is a provisioning failure, not a reason to
                # fall back to the snapshot: refuse loudly, same as an
                # absent ordinal below.
                cur.execute("SELECT to_regclass('okg.v_generation_interval_stab')")
                if cur.fetchone()[0] is None:
                    raise SubstrateError(
                        "interval effective-attribute reads requested but "
                        "okg.v_generation_interval_stab is missing",
                        data={"generation_id": gen},
                        fix_hint=(
                            "re-apply db/views.sql (okg up) so the TEXT-ref "
                            "generation-ordinal read surface exists"
                        ),
                    )
                cur.execute(
                    "SELECT chronos_branch_id, stab_ordinal "
                    "FROM okg.v_generation_interval_stab "
                    "WHERE generation_ref = %s",
                    (gen,),
                )
                row = cur.fetchone()
                if row is None:
                    raise SubstrateError(
                        "interval effective-attribute reads requested but the "
                        "pinned generation has no interval ordinal",
                        data={"generation_id": gen},
                        fix_hint=(
                            "publish or backfill generation ordinals before "
                            "enabling OKG_IVM_INTERVAL_VIEW"
                        ),
                    )
                branch_id, ordinal = row[0], int(row[1])
                cache_key: tuple[Any, ...] | None = None
                already_complete = False
                if target_kind is not None:
                    expected_count: int | None = None
                    cur.execute(
                        "SELECT to_regclass('_chronos_branch_interval_checkpoints')"
                    )
                    if cur.fetchone()[0] is not None:
                        cur.execute(
                            "SELECT metadata FROM _chronos_branch_interval_checkpoints "
                            "WHERE checkpoint_id = %s",
                            (gen,),
                        )
                        metadata_row = cur.fetchone()
                        if metadata_row is not None:
                            metadata = metadata_row[0]
                            if isinstance(metadata, str):
                                metadata = _json.loads(metadata or "{}")
                            if not isinstance(metadata, Mapping):
                                raise SubstrateError(
                                    "interval effective-attribute checkpoint "
                                    "metadata is not an object",
                                    data={"generation_id": gen},
                                )
                            key = (
                                "node_count" if target_kind == "node" else "edge_count"
                            )
                            raw_expected = metadata.get(key)
                            if raw_expected is not None:
                                expected_count = int(raw_expected)
                    cur.execute(
                        "SELECT max(stab_ordinal) "
                        "FROM okg.v_generation_interval_stab "
                        "WHERE chronos_branch_id = %s",
                        (branch_id,),
                    )
                    head_row = cur.fetchone()
                    cache_key = _interval_complete_cache_key(
                        conn,
                        generation=gen,
                        target_kind=target_kind,
                        branch_id=branch_id,
                        ordinal=ordinal,
                        head_ordinal=head_row[0] if head_row else None,
                        expected_count=expected_count,
                    )
                    with _interval_complete_lock:
                        expires_at = (
                            _interval_complete_cache.get(cache_key)
                            if cache_key is not None
                            else None
                        )
                        already_complete = (
                            expires_at is not None
                            and _interval_complete_cache_clock() < expires_at
                        )
                        if expires_at is not None and not already_complete:
                            _interval_complete_cache.pop(cache_key, None)
                if target_kind is not None and not already_complete:
                    cur.execute(
                        """
                        SELECT
                          (
                            SELECT count(*)
                              FROM okg.attribute_effective_view
                             WHERE generation_id = %s
                               AND namespace = 'default'
                               AND target_kind = %s
                          ) AS snapshot_rows,
                          (
                            SELECT count(*)
                              FROM okg.attribute_effective_view_intervals
                             WHERE branch_id = %s
                               AND namespace = 'default'
                               AND target_kind = %s
                               AND from_ord <= %s
                               AND %s < to_ord
                          ) AS interval_rows,
                          (
                            SELECT count(*)
                              FROM okg.attribute_effective_view_intervals e
                              JOIN okg.attribute_versions av
                                USING (target_kind, target_id, attrs_hash)
                             WHERE e.branch_id = %s
                               AND e.namespace = 'default'
                               AND e.target_kind = %s
                               AND e.from_ord <= %s
                               AND %s < e.to_ord
                          ) AS interval_payload_rows
                        """,
                        (
                            gen,
                            target_kind,
                            branch_id,
                            target_kind,
                            ordinal,
                            ordinal,
                            branch_id,
                            target_kind,
                            ordinal,
                            ordinal,
                        ),
                    )
                    authority_row = cur.fetchone()
                    if authority_row:
                        snapshot_rows = int(authority_row[0] or 0)
                        interval_rows = int(authority_row[1] or 0)
                        interval_payload_rows = int(authority_row[2] or 0)
                        required_rows = (
                            expected_count
                            if expected_count is not None
                            else snapshot_rows
                        )
                    else:
                        required_rows = 0
                        interval_payload_rows = 0
                    if required_rows > 0 and interval_rows < required_rows:
                        _refuse_pin_past_attribute_retention(
                            conn, generation=gen, branch_id=branch_id,
                            ordinal=ordinal,
                        )
                        raise SubstrateError(
                            "interval effective-attribute reads requested but the "
                            "pinned generation has incomplete interval stab rows",
                            data={
                                "generation_id": gen,
                                "target_kind": target_kind,
                                "branch_id": branch_id,
                                "ordinal": ordinal,
                                "expected_rows": required_rows,
                                "interval_rows": interval_rows,
                                "snapshot_rows": snapshot_rows,
                            },
                            fix_hint=(
                                "run the one-time interval backfill for the "
                                "latest full snapshot before enabling "
                                "OKG_IVM_INTERVAL_VIEW"
                            ),
                        )
                    if interval_rows != interval_payload_rows:
                        raise SubstrateError(
                            "interval effective-attribute reads requested but the "
                            "pinned generation has an incomplete attrs_hash payload join",
                            data={
                                "generation_id": gen,
                                "target_kind": target_kind,
                                "branch_id": branch_id,
                                "ordinal": ordinal,
                                "source_rows": interval_rows,
                                "payload_rows": interval_payload_rows,
                            },
                            fix_hint=(
                                "restore every attribute_versions payload referenced "
                                "by the pinned interval rows before enabling "
                                "OKG_IVM_INTERVAL_VIEW"
                            ),
                        )
                    if cache_key is not None:
                        # Both refusals above raise, so reaching here means
                        # the check PASSED; only a pass is ever stored.
                        with _interval_complete_lock:
                            if (
                                len(_interval_complete_cache)
                                >= _INTERVAL_COMPLETE_CACHE_MAX
                            ):
                                _interval_complete_cache.pop(
                                    next(iter(_interval_complete_cache))
                                )
                            _interval_complete_cache[cache_key] = (
                                _interval_complete_cache_clock()
                                + _INTERVAL_COMPLETE_CACHE_TTL_S
                            )
                base_table = "okg.attribute_effective_view_intervals"
                if source_alias:
                    return (
                        base_table,
                        f"{base_table} {source_alias} "
                        "JOIN okg.attribute_versions av "
                        "USING (target_kind, target_id, attrs_hash)",
                        f"{source_alias}.branch_id = %(br)s AND "
                        f"{source_alias}.from_ord <= %(g)s AND "
                        f"%(g)s < {source_alias}.to_ord",
                        {"br": branch_id, "g": ordinal},
                    )
                return (
                    base_table,
                    base_table + " " + _ATTRIBUTE_VERSION_JOIN,
                    "branch_id = %(br)s AND from_ord <= %(g)s AND %(g)s < to_ord",
                    {"br": branch_id, "g": ordinal},
                )
        base_table = "okg.attribute_effective_view"
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (base_table,))
            if cur.fetchone()[0] is None:
                raise SubstrateError(
                    "snapshot effective-attribute reads requested but "
                    "okg.attribute_effective_view is missing",
                    data={"generation_id": gen},
                    fix_hint=(
                        "run the snapshot effective-attribute migration and "
                        "materialize the pinned generation before disabling "
                        "OKG_IVM_INTERVAL_VIEW"
                    ),
                )
            if target_kind is not None:
                cur.execute(
                    """
                    SELECT
                      (
                        SELECT count(*)
                          FROM okg.attribute_effective_view e
                         WHERE e.generation_id = %s
                           AND e.namespace = 'default'
                           AND e.target_kind = %s
                      ) AS source_rows,
                      (
                        SELECT count(*)
                          FROM okg.attribute_effective_view e
                          JOIN okg.attribute_versions av
                            USING (target_kind, target_id, attrs_hash)
                         WHERE e.generation_id = %s
                           AND e.namespace = 'default'
                           AND e.target_kind = %s
                      ) AS payload_rows
                    """,
                    (gen, target_kind, gen, target_kind),
                )
                source_rows, payload_rows = cur.fetchone()
                if int(source_rows or 0) != int(payload_rows or 0):
                    raise SubstrateError(
                        "snapshot effective-attribute reads requested but the "
                        "pinned generation has an incomplete attrs_hash payload join",
                        data={
                            "generation_id": gen,
                            "target_kind": target_kind,
                            "source_rows": int(source_rows or 0),
                            "payload_rows": int(payload_rows or 0),
                        },
                        fix_hint=(
                            "restore every attribute_versions payload referenced "
                            "by the pinned snapshot rows before disabling "
                            "OKG_IVM_INTERVAL_VIEW"
                        ),
                    )
        if source_alias:
            return (
                base_table,
                f"{base_table} {source_alias} "
                "JOIN okg.attribute_versions av "
                "USING (target_kind, target_id, attrs_hash)",
                f"{source_alias}.generation_id = %(gen)s",
                {"gen": gen},
            )
        return (
            base_table,
            base_table + " " + _ATTRIBUTE_VERSION_JOIN,
            "generation_id = %(gen)s",
            {"gen": gen},
        )

    def _effective_view_ready(
        conn: psycopg.Connection,
        generation_id: Any,
        target_kind: str,
    ) -> bool:
        """True iff the chosen effective-attrs source (interval stab or snapshot)
        exists AND carries a row for this generation. A generation with neither
        (published before the table existed, or a deployment mid-migration) -> fall
        back to the Layer-1 chronos-scan + Python-overlay path, never error and
        never return wrong numbers."""
        base_table, from_clause, gen_where, gen_params = _effective_view_source(
            conn,
            generation_id,
            target_kind=target_kind,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (base_table,))
            if cur.fetchone()[0] is None:
                return False
            params = {"tk": target_kind, **gen_params}
            cur.execute(
                f"SELECT 1 FROM {from_clause} "
                f"WHERE target_kind = %(tk)s AND {gen_where} "
                "AND namespace = 'default' LIMIT 1",
                params,
            )
            return cur.fetchone() is not None

    def _effective_view_where(
        kind: str,
        scan_predicate: Any,
    ) -> tuple[str, dict[str, Any]]:
        """Render the SAME attr-free stable-field clauses the Layer-1 pushdown
        carries (subtype/edge_type/node_id/... eq/in/exists, conjoined with
        `and`) to a SQL WHERE over the flat snapshot's real columns, plus ONE
        attrs-aware shape: ``{"op": "contains", "field": "attrs", "value":
        {...}}`` renders to the GIN-indexed ``attrs @> ...`` containment over
        the snapshot's EFFECTIVE attrs (expose-mcp-node-lookup: the typed
        attrs/path lookup pushes its exact-match filters here — safe because
        the stored ``attrs`` column IS raw||overlay, so a containment match is
        a superset of the residual post-Nomos re-check; see the parity note in
        ``_effective_view_fast_rows``). Only callers that re-check the same
        predicate in Python may pass a contains clause; the Layer-1 fallback
        strips it (``_operator_attr_rows``) because the chronos scan's attrs
        are RAW, where a containment push could drop overlay-only matches.
        Anything else is skipped (returns no clause), so the WHERE can only
        ever NARROW to a superset the residual Python post-filter re-checks --
        result parity is preserved exactly as on the Layer-1 path."""
        columns = _EFFECTIVE_VIEW_FIELD_COLUMNS[kind]
        params: dict[str, Any] = {}
        counter = [0]

        def _bind(value: Any) -> str:
            counter[0] += 1
            name = f"p{counter[0]}"
            params[name] = value
            return f"%({name})s"

        def render(pred: Any) -> str | None:
            if pred in (None, True, {}) or not isinstance(pred, dict):
                return None
            op = pred.get("op")
            if op == "and":
                clauses = pred.get("clauses")
                if not isinstance(clauses, list):
                    return None
                parts = [render(c) for c in clauses]
                parts = [p for p in parts if p]
                if not parts:
                    return None
                return "(" + " AND ".join(parts) + ")"
            field = pred.get("field")
            if op == "contains" and field == "attrs":
                value = pred.get("value")
                if not isinstance(value, dict) or not value:
                    return None
                # JSONB @> cannot express the Python matcher's None ==
                # "absent or null" semantics; callers keep None-valued
                # filters residual-only (see _search_attrs_lookup).
                if any(v is None for v in value.values()):
                    return None
                return f"attrs @> {_bind(Jsonb(value))}"
            if not isinstance(field, str) or field not in columns:
                return None
            col = columns[field]
            if op == "eq":
                return f"{col} = {_bind(pred.get('value'))}"
            if op == "in":
                values = pred.get("values")
                if not isinstance(values, list) or not values:
                    return None
                return f"{col} = ANY({_bind(list(values))})"
            if op == "exists":
                return f"{col} IS NOT NULL"
            return None

        rendered = render(scan_predicate)
        if not rendered:
            return "", params
        return " AND " + rendered, params

    # scale-operator-attr-reads / req.effective-attrs-view (Layer 2): the
    # result-size ceiling on the fast filter/map row scan. This is NOT the
    # Layer-1 10K bounded-Python-overlay cap (which was a CORRECTNESS limit on a
    # per-row Python merge): this is an index scan, so the ceiling is a memory
    # guard set far above any real page (max page is 1000). When the scan returns
    # exactly ceiling+1 rows the operator reports `truncated=true` against this
    # bound -- a TRUE truncation signal, never a silent undercount. Aggregate
    # does NOT use this: it counts every matched row in SQL (see
    # `_effective_view_aggregate_groups`).
    # Defined at MODULE scope (above `build_server`) rather than here so the
    # truncation contract is testable without seeding 200K rows; the value and
    # every use of it are unchanged.

    def _declared_registry_for(conn: psycopg.Connection) -> dict[str, str]:
        """The declared-filterable registry, or {} when unavailable.

        Read fresh per operator call rather than cached: a catalog reload
        changes the registry, and a cached one would keep the Python parity
        re-check comparing in a type the SQL side no longer uses. The read is
        a single indexed SELECT over okg.subtypes, which is negligible against
        the scan it replaces.
        """
        from okg.substrate.declared_attr_indexes import declared_filterable_attrs

        try:
            return declared_filterable_attrs(conn)
        except psycopg.Error as exc:
            # NOT a silent fallback: an unreadable catalog means the pushdown
            # is unavailable, and the scan path below returns the SAME rows, so
            # the answer stays correct. What must not happen is that it goes
            # unnoticed -- a deployment quietly back on the 23.9 s path looks
            # like a performance mystery, not a missing grant. Log it loudly
            # and keep serving.
            log.warning(
                "declared-key pushdown unavailable: the declared-filterable "
                "registry could not be read (%s). Reads fall back to the "
                "unindexed scan path -- correct, but without the declared "
                "filter index. Check okg_mcp SELECT on okg.subtypes / "
                "okg.catalog_versions.",
                exc,
            )
            return {}

    def _effective_view_fast_rows(
        conn: psycopg.Connection,
        session: Session,
        *,
        kind: str,
        scan_predicate: Any,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Legacy test seam: read rows from the indexed effective view in SQL.

        Production filter/map/search calls use ``_effective_view_sql_page``;
        this helper remains exposed only for parity tests that compare the
        historical broad-row seam with the new bounded page path. It keeps the
        old result-size ceiling and re-applies live overlays before returning.

        Returns ``(rows, truncated)`` where ``truncated`` is True iff the scan hit
        the result-size CEILING (a real bound, surfaced loudly to the caller),
        never a silent undercount.

        PARITY (req.parity): with Nomos enabled, the shared node filter applies
        accepted overlays before evaluating policy and masking, so both this SQL
        snapshot path and the Layer-1 fallback protect the final effective attrs.
        A redacted row is terminal: the normal downstream overlay pass cannot
        restore classified data. With Nomos disabled, the precomputed effective
        ``attrs`` already equals ``raw || overlay`` and is used directly.

        NO CALLER LIMIT (materialize-effective-attrs-scan-once): this function
        deliberately takes no ``limit``. Callers DO have page sizes, and binding
        one into the SQL below would be WRONG, not merely conservative. The WHERE
        that ``_effective_view_where`` builds is narrowing-only -- it renders the
        clauses it can and SKIPS the rest -- so the result is a SUPERSET that every
        caller re-filters in Python (`filter` at the residual predicate re-check,
        `_search_attrs_lookup` at its `_matches_attr_filters` pass). A ``LIMIT`` over
        a superset does not return the first N members of the FILTERED set; it
        returns an arbitrary N rows of the superset, of which the residual filter may
        keep almost none. Concretely: 150K `source_file` nodes, a caller filtering on
        `attrs.owner == "alice"` matching 30K of them; `attrs.owner` is not a stable
        column so only the subtype clause renders. A 10K bound would return an
        arbitrary 10K rows whose residual survivors may be ZERO -- the caller sees no
        rows for a predicate matching 30K nodes, flagged only as `truncated`, which
        is indistinguishable from "nothing matches". There is also no ORDER BY, so
        WHICH arbitrary rows come back is plan- and heap-order dependent and two
        identical calls can disagree. The ceiling below is the only sound bound here.
        Bounding lower requires the predicate to render EXACTLY with no residual."""
        target_kind = "node" if kind == "nodes" else "edge"
        where, params = _effective_view_where(kind, scan_predicate)
        _base_table, from_clause, gen_where, gen_params = _effective_view_source(
            conn,
            session.generation_id,
            target_kind=target_kind,
        )
        params["tk"] = target_kind
        params.update(gen_params)
        ceiling = _EFFECTIVE_VIEW_SCAN_CEILING
        # presence-means-overlay (PACT bound-okg-workspace-disk-growth): raw_attrs
        # is stored ONLY when it byte-differs from attrs; NULL means "raw == attrs".
        # materialize-effective-attrs-scan-once: `raw_attrs` is selected AS STORED,
        # NOT wrapped in COALESCE(raw_attrs, attrs). Postgres does not deduplicate
        # two target-list entries that evaluate equal, so the COALESCE sent the SAME
        # blob a second time for the no-overlay MAJORITY. The NULL is resolved in
        # Python below by reusing the already-decoded effective object.
        #
        # Still true after schema-interval-narrow-split moved the payload into
        # okg.attribute_versions: that table keeps `attrs NOT NULL` alongside a
        # nullable `raw_attrs` (db/schema.sql), so presence-means-overlay -- and
        # the double-send the COALESCE caused -- survive the split unchanged.
        # Both columns resolve through the version join in ``from_clause``.
        # The LIMIT is deliberately NOT pushed, and the bound stays the scan
        # CEILING it has always been.
        #
        # Pushing it looked free and is not: this function's `limit` is a scan
        # bound (its only production caller passes 10_000), not the caller's
        # page size, and the callers page in Python AFTER a residual re-check.
        # Emitting `LIMIT 10000` would cap a >10K declared-key result at 10,000
        # rows while making the result look complete -- the truncation flag is
        # computed against the ceiling, which a pushed limit never reaches. A
        # caller paging to offset 9,950 would be told `truncated=False` and
        # believe it had seen everything. That is a silent undercount, and the
        # pre-pushdown code did not have it.
        sql = (
            "SELECT target_id, subtype, src, dst, edge_type, "
            "attrs, raw_attrs, raw_text_hash "
            f"FROM {from_clause} "
            f"WHERE target_kind = %(tk)s AND {gen_where} "
            "AND namespace = 'default'" + where + " LIMIT %(lim)s"
        )
        params["lim"] = ceiling + 1
        id_field = "node_id" if kind == "nodes" else "edge_id"
        nomos_on = deployment_requires_nomos_runtime(session.deployment_name)
        rows: list[dict[str, Any]] = []
        raw_seen = 0
        # STREAMED, not fetchall(): a server-side cursor keeps the full client-side
        # row buffer and this built list from ever coexisting. A streaming cursor
        # forbids interleaving another statement on this connection while it is
        # open -- nothing in this loop queries, and the source-lineage filter runs
        # AFTER it. Keep it that way.
        with conn.cursor(row_factory=dict_row) as cur:
            for r in cur.stream(sql, params):
                raw_seen += 1
                if raw_seen > ceiling:
                    # Count the ceiling-plus-first row for `truncated`, but do NOT
                    # build it -- matching the previous `raw = raw[:ceiling]`.
                    break
                effective = r["attrs"]
                stored_raw = r["raw_attrs"]
                # A JSONB column holding an array used to raise inside dict(); with
                # the copy gone it would flow through as a list. Refuse by name.
                if effective is not None and not isinstance(effective, dict):
                    raise SubstrateError(
                        "effective-attrs row has a non-object attrs value",
                        data={
                            "target_id": r["target_id"],
                            "attrs_type": type(effective).__name__,
                        },
                    )
                if stored_raw is not None and not isinstance(stored_raw, dict):
                    raise SubstrateError(
                        "effective-attrs row has a non-object raw_attrs value",
                        data={
                            "target_id": r["target_id"],
                            "raw_attrs_type": type(stored_raw).__name__,
                        },
                    )
                # Build the common filter input from raw attrs. With Nomos enabled,
                # filter_node_rows_for_session applies the accepted overlay before
                # its policy decision and final mask. psycopg decodes each JSONB
                # column into a FRESH object per row that no other row aliases, so
                # these are used directly rather than copied again.
                # When raw_attrs is NULL, raw == effective and these two keys
                # INTENTIONALLY alias one object -- that aliasing is the memory
                # saving. It is safe only because no read-path step mutates either
                # in place (the Nomos filter copies attrs before touching it, and
                # the enforcement-off swap below rebinds rather than mutates). A
                # test asserts the aliasing is not observable; if it ever fails,
                # reinstate the copy HERE rather than deleting the test.
                row = {
                    id_field: r["target_id"],
                    "attrs": (effective or {} if stored_raw is None else stored_raw),
                }
                if kind == "nodes":
                    row["subtype"] = r["subtype"]
                    row["text_hash"] = r["raw_text_hash"]
                else:
                    row["src"] = r["src"]
                    row["dst"] = r["dst"]
                    row["edge_type"] = r["edge_type"]
                # Carry the precomputed effective attrs for the enforcement-off fast
                # path, which can avoid an overlay query entirely.
                row["_effective_attrs"] = effective or {}
                rows.append(row)
        scan_truncated = raw_seen > ceiling
        if kind == "nodes":
            survivors = filter_node_rows_for_session(conn, session, rows)
        else:
            survivors = filter_edge_rows_for_session(conn, session, rows)
        if not nomos_on:
            # Enforcement off: Nomos was a pass-through, raw never redacted, so the
            # precomputed effective attrs == raw || overlay at materialization time.
            # This legacy test seam can be called after a post-publish accepted
            # overlay refreshes the snapshot but not the interval front; apply the
            # authoritative overlay once more so the seam remains parity-safe with
            # the production runtime-source path.
            for row in survivors:
                row["attrs"] = row.pop("_effective_attrs", row.get("attrs") or {})
            survivors = apply_effective_attrs_to_rows(
                conn,
                survivors,
                target_kind=target_kind,
                target_id_field=id_field,
                generation_id=session.generation_id,
            )
            return survivors, scan_truncated
        # Enforcement on: the node filter already evaluated the effective attrs.
        # Drop the private fast-path hint. This conventional downstream overlay
        # pass is idempotent for allowed rows and a no-op for terminal redactions.
        for row in survivors:
            row.pop("_effective_attrs", None)
        survivors = apply_effective_attrs_to_rows(
            conn,
            survivors,
            target_kind=target_kind,
            target_id_field=id_field,
            generation_id=session.generation_id,
        )
        return survivors, scan_truncated

    def _effective_view_runtime_source(
        *,
        base_table: str,
        source_clause: str,
        gen_where: str,
        gen_params: Mapping[str, Any],
        target_kind: str,
        generation_id: Any,
        relation: str,
        params: dict[str, Any],
        base_candidates_sql: str = "",
        base_candidates_in_id_order: bool = False,
        base_candidates_as_array: bool = False,
        target_kind_param: str = "tk",
    ) -> str:
        """Overlay the pinned SQL relation at read time, still inside Postgres.

        ``target_kind_param`` names the parameter the target kind binds to,
        so one statement can join a node relation to an edge relation.

        Accepted observations can be recorded after publish. The per-generation
        snapshot refreshes immediately, while the interval front advances on
        publish. Reapplying the authoritative overlay here keeps all SQL
        predicates exact across both storage modes; ``overlay_attr_keys``
        tells the declared-index candidate set when it must yield to that live
        value.

        ``base_candidates_sql`` (lane-h-mcp-filter-index-driven) is a target-id
        set query that narrows the BASE relation before the overlay lateral
        runs. Without it the lateral ran once per pinned row (527k loops on a
        real graph) before any predicate could apply. A caller may pass it only
        when the set is a superset of the rows its exact predicate can admit;
        the predicate still runs on the survivors.

        ``base_candidates_as_array`` (issue #3194) matches the set as
        ``target_id = ANY(ARRAY(...))``: the set is computed once, first, and
        each id is looked up through the relation's primary key. A caller
        passes it only when the whole set is bounded: the path-prefix set
        (at most ``PATH_PREFIX_CANDIDATE_BOUND`` ids after the probe) united
        only with targets that have a ``path`` overlay. An overlay union over
        any other predicate key is unbounded and keeps ``IN``. With
        ``IN`` the planner may instead hash-join the set against the whole
        pinned relation; measured on a 3,007-record fixture, a two-id set
        read 9,145 buffers through ``IN`` and 67 through ``ANY(ARRAY)``.
        """
        base = f"{relation}_base"
        overlay = f"{relation}_overlay"
        if base_table == "okg.attribute_effective_view_intervals":
            qualified_gen = (
                f"{base}.branch_id = %(br)s AND "
                f"{base}.from_ord <= %(g)s AND %(g)s < {base}.to_ord"
            )
        else:
            # The snapshot source binds its generation as ``gen`` (the
            # interval source binds ``br``/``g``).  Keep the outer
            # qualification on the same parameter name as the inner source;
            # otherwise snapshot-mode reads fail at execute time with an
            # unbound ``g`` placeholder even though the source relation is
            # available.
            qualified_gen = f"{base}.generation_id = %(gen)s"
        params.update(gen_params)
        if base_candidates_sql:
            # OFFSET 0 keeps this subquery a planning unit of its own. Without
            # it Postgres may pull the IN up and join it AFTER the lateral
            # (observed on a small unanalyzed fixture: the lateral ran for every
            # pinned row, then the join discarded them), which is the cost this
            # narrowing exists to remove. Inside the fence the candidates can
            # only drive the pinned rows, and the lateral runs on its output.
            source = (
                f"(SELECT * FROM {source_clause} "
                f"WHERE target_kind = %({target_kind_param})s AND {gen_where} "
                "AND namespace = 'default' "
                + (
                    f"AND target_id = ANY(ARRAY({base_candidates_sql}))"
                    if base_candidates_as_array
                    else f"AND target_id IN ({base_candidates_sql})"
                )
                # When the page is ordered by target_id (the default), hand the
                # outer query candidates already in that order. The lateral
                # nested loop keeps it, so a broad match streams into the
                # LIMIT instead of running the lateral for every candidate and
                # sorting afterwards (measured 1.08 s -> 1.96 s on a 250k-match
                # eq without this, against the unnarrowed scan).
                + (" ORDER BY target_id" if base_candidates_in_id_order else "")
                + " OFFSET 0)"
            )
        else:
            source = f"({source_clause})"
        params.update(
            {
                target_kind_param: target_kind,
                "overlay_gen": str(generation_id),
                "overlay_reserved": sorted(RESERVED_EFFECTIVE_ATTR_KEYS),
            }
        )
        return (
            "(SELECT "
            f"{base}.target_kind, {base}.target_id, {base}.namespace, "
            f"{base}.subtype, {base}.src, {base}.dst, {base}.edge_type, "
            f"({base}.attrs || COALESCE({overlay}.attrs, '{{}}'::jsonb)) AS attrs, "
            f"CASE WHEN {overlay}.attrs IS NULL THEN {base}.raw_attrs "
            f"ELSE COALESCE({base}.raw_attrs, {base}.attrs) END AS raw_attrs, "
            f"{base}.raw_text_hash, "
            f"COALESCE({overlay}.attr_keys, ARRAY[]::text[]) AS overlay_attr_keys "
            f"FROM {source} AS {base} "
            "LEFT JOIN LATERAL ("
            "SELECT jsonb_object_agg(attr_key, value) AS attrs, "
            "array_agg(attr_key) AS attr_keys "
            f"FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
            f"WHERE target_kind = {base}.target_kind "
            f"AND target_id = {base}.target_id "
            "AND generation_id = %(overlay_gen)s "
            f"AND namespace = {base}.namespace "
            "AND attr_key <> ALL(%(overlay_reserved)s::text[])"
            f") AS {overlay} ON TRUE "
            f"WHERE {base}.target_kind = %({target_kind_param})s AND {qualified_gen} "
            f"AND {base}.namespace = 'default') AS {relation}"
        )

    def _effective_view_aggregate_narrow_source(
        *,
        base_table: str,
        gen_where: str,
        gen_params: Mapping[str, Any],
        target_kind: str,
        relation: str,
        params: dict[str, Any],
    ) -> str:
        """The pinned relation without its payload, for an aggregate that
        reads no attrs.

        ``_effective_view_runtime_source`` joins okg.attribute_versions and
        the live overlay so ``attrs`` can be read. An aggregate whose ``by``
        and predicate name only stable columns needs neither: the join is one
        primary-key probe per counted row (76,444 on the CMS copy, 22 ms ->
        177 ms) and the overlay changes only ``attrs``/``raw_attrs``.

        Dropping the join keeps the count exact. Every base row has exactly
        one payload row: ``attribute_versions`` is keyed by the join columns,
        and ``_effective_view_source`` has already refused this generation
        when its source rows and joined payload rows differ.

        Only the stable columns are exposed. A compiled predicate that still
        reads ``attrs`` fails with a missing-column error; it never falls
        back to the payload-joined form.
        """
        params.update(gen_params)
        params["tk"] = target_kind
        return (
            "(SELECT target_kind, target_id, namespace, subtype, src, dst, "
            "edge_type, raw_text_hash "
            f"FROM {base_table} "
            f"WHERE target_kind = %(tk)s AND {gen_where} "
            f"AND namespace = 'default') AS {relation}"
        )

    def _holds_text(spec: Any) -> bool:
        from okg.substrate.mcp.text_match import holds_text_clause

        return holds_text_clause(spec)

    def _text_on_reference(tool: str) -> SubstrateError:
        from okg.substrate.mcp.text_match import TextConditionInvalid

        return TextConditionInvalid(
            f"{tool}: a text condition reads a set by its condition, not a "
            "saved result",
            data={"reason": "text_condition_on_reference"},
            fix_hint=(
                "send the condition the saved result came from, with the text "
                'clause beside it, in where (e.g. {"type": "<subtype>", "text": '
                '{"op": "matches", "value": "..."}})'
            ),
        )

    def _text_rest_ids(
        conn: psycopg.Connection,
        session: Session,
        *,
        predicate: Any,
        cap: int,
        tool: str = "filter",
        member_ids: Sequence[str] | None = None,
    ) -> list[str] | None:
        """The records `predicate` admits, in id order, at most `cap + 1`.

        Issue #3666 (text matching as a condition): the set a text clause is
        checked within, or the matching records narrowed by the rest of the
        condition (`member_ids`). Read in SQL over the effective relation
        with the one operator compiler, as an aggregate counts, so with Nomos
        off it holds exactly the records the operator itself would admit.
        None when the predicate cannot be compiled exactly; the caller
        refuses."""
        _require_effective_view(conn, session, "node")
        params: dict[str, Any] = {}
        base_table, source_clause, gen_where, gen_params = _effective_view_source(
            conn, session.generation_id, target_kind="node",
        )
        relation = "text_rest"
        if member_ids is not None:
            params["rr_member_ids"] = list(member_ids)
        if member_ids is not None or _operator_refs_attrs(predicate):
            from_clause = _effective_view_runtime_source(
                base_table=base_table,
                source_clause=source_clause,
                gen_where=gen_where,
                gen_params=gen_params,
                target_kind="node",
                generation_id=session.generation_id,
                relation=relation,
                params=params,
                base_candidates_sql=(
                    "SELECT unnest(%(rr_member_ids)s::text[])"
                    if member_ids is not None else ""
                ),
            )
        else:
            from_clause = _effective_view_aggregate_narrow_source(
                base_table=base_table,
                gen_where=gen_where,
                gen_params=gen_params,
                target_kind="node",
                relation=relation,
                params=params,
            )
        compiled = _compile_operator_predicate(
            "nodes", predicate, params,
            declared=_declared_registry_for(conn), relation=relation,
        )
        if compiled is None:
            return None
        where_sql = compiled.bind_into(params)
        if member_ids is not None:
            where_sql = (
                f"({where_sql}) AND {relation}.target_id = ANY(%(rr_member_ids)s::text[])"
            )
        params["tm_rest_cap"] = int(cap) + 1
        sql = (
            f"SELECT {relation}.target_id FROM {from_clause} WHERE {where_sql} "
            "ORDER BY 1 LIMIT %(tm_rest_cap)s"
        )
        from okg.substrate.mcp import text_match as tm
        from okg.substrate.mcp.grep_search import _bounded, _Cancelled

        # Under the same statement timeout as the text reads (#3666).
        try:
            rows = _bounded(conn, sql, params)
        except _Cancelled as exc:
            raise tm.timed_out(tool) from exc
        return [str(row["target_id"]) for row in rows]

    def _text_rest_ids_dated(
        conn: psycopg.Connection,
        session: Session,
        *,
        tool: str,
        predicate: Any,
        cap: int,
        member_ids: Sequence[str] | None = None,
        report: Any = None,
    ) -> list[str] | None:
        """`_text_rest_ids` for a condition that may carry a date (#3666).

        A date condition is read the way the operator reads it
        (`_dated_read`: ISO-8601 values first, other stored forms after),
        so the set the text is checked within is the set the operator's
        own page or count then reads. What the date read noted goes into
        `report` when given: a reply that has no page or count read of its
        own (`aggregate(by=["match"])`) states it from there. This read has
        its own date budget, beside the operator's."""
        if not _holds_date_condition(predicate):
            return _text_rest_ids(
                conn, session, predicate=predicate, cap=cap, member_ids=member_ids,
                tool=tool,
            )
        _read_with, ids = _dated_read(
            conn, session, kind="nodes", predicate=predicate, operator=tool,
            report=_DateConditionReport() if report is None else report,
            nomos_runtime=False, budget=None,
            run=lambda read_with: _text_rest_ids(
                conn, session, predicate=read_with, cap=cap, member_ids=member_ids,
                tool=tool,
            ),
        )
        return ids

    def _text_not_compiled(tool: str) -> SubstrateError:
        err = SubstrateError(
            f"{tool}: the condition beside the text clause cannot be compared "
            "exactly in SQL",
            data={"reason": "text_condition_rest_not_compiled"},
            fix_hint="compare a type, a node_id or a declared attrs.<key> beside the text clause",
        )
        err.code = "unsupported_operator_form"
        return err

    def _run_set_predicate(rest: Any, text_ids: list[str] | None) -> Any:
        """A `run` step's set (#3666) when `_text_members` took its text
        clause off: the rest of the condition over the matching ids."""
        if text_ids is None:
            return rest
        # No record's text matches: an empty `in` is refused, so the set is
        # the empty id, which no record has.
        members = {"op": "in", "field": "node_id", "values": list(text_ids) or [""]}
        return {"op": "and", "clauses": [rest, members]} if rest else members

    def _text_members(
        conn: psycopg.Connection,
        session: Session,
        *,
        tool: str,
        kind: str,
        predicate: Any,
        nomos_runtime: bool,
        retry: Callable[[str], dict[str, Any]] | None = None,
    ) -> tuple[Any, Any, list[str] | None, dict[str, Any] | None]:
        """Take a text clause off `predicate` and find the records it admits.

        Returns `(the rest of the condition, the text condition, the ids of
        the records whose text matches, facts for the reply)`; with no text
        clause, `(predicate, None, None, None)`. The ids are every matching
        record the rest of the condition can admit, so the operator, given
        them as its member restriction and the rest as its predicate, pages,
        orders and counts exactly the records the whole condition admits.
        Within `text_match.SCAN_LIMIT` or refused (see `text_match`)."""
        from okg.substrate.mcp import text_match as tm

        rest, condition = tm.split_text_condition(predicate, tool=tool)
        if condition is None:
            return predicate, None, None, None
        ids, facts = _text_ids(
            conn, session, tool=tool, kind=kind, rest=rest, condition=condition,
            nomos_runtime=nomos_runtime, retry=retry,
        )
        return rest, condition, ids, facts

    def _text_ids(
        conn: psycopg.Connection,
        session: Session,
        *,
        tool: str,
        kind: str,
        rest: Any,
        condition: Any,
        nomos_runtime: bool,
        retry: Callable[[str], dict[str, Any]] | None = None,
    ) -> tuple[list[str], dict[str, Any]]:
        """The ids of the records whose text matches `condition` and which
        `rest` can admit, and the facts the reply states (`_text_members`).

        `retry(type)` builds this call narrowed to one record type; a scan
        limit refusal of a call that names no type offers it (`_text_type_retry`)."""
        from okg.substrate.mcp import text_match as tm

        def refused(**kwargs: Any) -> SubstrateError:
            refusal = tm.refuse_scan_limit(tool, **kwargs)
            if tm.type_scope(rest) is None and _holds_date_condition(rest):
                refusal.fix_hint = (
                    f"{refusal.fix_hint}; with a date condition no narrowed "
                    "call is offered: add the type yourself"
                )
            # A dated rest is counted by type only through its own date
            # read; no retry is offered for it.
            if (
                retry is not None and tm.type_scope(rest) is None
                and not _holds_date_condition(rest)
            ):
                _text_type_retry(conn, session, rest=rest, refusal=refusal, tool=tool, retry=retry)
            return refusal

        if kind != "nodes":
            raise tm.TextConditionInvalid(
                f"{tool}: `text` is a field of a record; a set of links has none",
                data={"reason": "text_condition_on_edges"},
                fix_hint=(
                    'match the records instead: filter(where={"text": {"op": '
                    '"matches", "value": "..."}}), then expand from them'
                ),
            )
        if nomos_runtime:
            raise tm.refuse_under_access_rules(tool)
        # Indexed only through a term one of the record's texts holds whole.
        indexable = tm.narrowing_query(condition.parsed) is not None
        rest_given = rest not in (None, {}, True)
        text_over = False
        index_timeout: Optional[SubstrateError] = None
        # PACT read-search-caps-3666: a rest that names its node ids is the
        # set to check; the index read would look at every record in the
        # graph holding the term first (it timed out with 2 ids named).
        if indexable and tm.id_scope(rest) is None:
            from okg.substrate.mcp.grep_search import GrepTimeout

            try:
                ids = tm.matching_record_ids(
                    conn, session, condition, tool=tool, subtypes=tm.type_scope(rest),
                    rest_given=rest_given,
                )
            except GrepTimeout as exc:
                # A short literal ("Own") matches most of the graph's text:
                # its index read can pass the timeout where the rest of the
                # condition holds few records. That set is read instead,
                # and the reply says so (`index_timed_out`).
                if not rest_given:
                    raise
                index_timeout = exc
            else:
                if len(ids) <= tm.SCAN_LIMIT:
                    return ids, {"records_scanned": len(ids), "scan": "index"}
                text_over = True
                if not rest_given:
                    raise refused(
                        indexable=True, text_over=True, rest_over=False, rest_given=False,
                    )
        within = _text_rest_ids_dated(
            conn, session, tool=tool, predicate=rest, cap=tm.SCAN_LIMIT,
        )
        if within is None:
            raise _text_not_compiled(tool)
        if len(within) > tm.SCAN_LIMIT:
            if index_timeout is not None:
                # PACT read-aggregate-variants-3666: every sliced literal
                # read stopped and the rest is too large to read instead:
                # the longest literal is read once with the whole timeout,
                # as before the slices.
                ids = tm.matching_record_ids(
                    conn, session, condition, tool=tool,
                    subtypes=tm.type_scope(rest), whole=True,
                )
                if len(ids) <= tm.SCAN_LIMIT:
                    return ids, {"records_scanned": len(ids), "scan": "index"}
                raise refused(
                    indexable=True, text_over=True, rest_over=True, rest_given=True,
                )
            raise refused(
                indexable=indexable, text_over=text_over, rest_over=True,
                rest_given=rest_given,
            )
        ids = tm.matching_record_ids(conn, session, condition, tool=tool, record_ids=within)
        return ids, {
            "records_scanned": len(within), "scan": "set",
            **({"index_timed_out": True} if index_timeout is not None else {}),
        }

    def _text_type_retry(
        conn: psycopg.Connection,
        session: Session,
        *,
        rest: Any,
        refusal: SubstrateError,
        tool: str,
        retry: Callable[[str], dict[str, Any]],
    ) -> None:
        """Name, on a scan limit refusal, the record types the call could be
        narrowed to, and offer the call narrowed to the largest of them.

        A type whose records the rest of the condition admits number at most
        the scan limit is served exactly (its set is checked whole). Which
        type the caller meant cannot be told, so the offered call is a
        `guess` and the hint says what it assumes. Counted in SQL over the
        effective relation (`_effective_view_aggregate_groups`); nothing is
        offered when that count cannot run."""
        from okg.substrate.mcp import text_match as tm

        grouped = _effective_view_aggregate_groups(
            conn, session, kind="nodes", by=["subtype"], predicate=rest,
        )
        if grouped is None:
            return
        fitting = sorted(
            ((str(key[0]), int(count)) for key, count in grouped[0]
             if key and isinstance(key[0], str) and int(count) <= tm.SCAN_LIMIT),
            key=lambda pair: (-pair[1], pair[0]),
        )
        if not fitting:
            return
        named = ", ".join(f"{name} ({count:,})" for name, count in fitting[:6])
        best = fitting[0][0]
        refusal.fix_hint = (
            f"{refusal.fix_hint}; types within the limit: {named}; if you meant "
            f"{best} records, send the offered call"
        )
        refusal.data["types_within_limit"] = dict(fitting[:10])
        refusal.retry_call = {  # type: ignore[attr-defined]
            "tool": tool, "mode": "guess", "arguments": retry(best),
        }

    def _text_selects(spec: Any, name: str) -> bool:
        if isinstance(spec, (list, tuple)):
            return name in spec
        if isinstance(spec, dict):
            return name in spec.values()
        return False

    def _text_row_fields(
        conn: psycopg.Connection,
        session: Session,
        condition: Any,
        rows: list[dict[str, Any]],
        *,
        tool: str,
        select: Any,
    ) -> None:
        """Put `match`, `match_count` and, when selected, `matches` on each
        page row (#3666). Read for this page's records only."""
        from okg.substrate.mcp import text_match as tm

        with_lines = _text_selects(select, "matches")
        found = tm.record_matches(
            conn, session, condition, [str(row["node_id"]) for row in rows],
            tool=tool, with_lines=with_lines,
        )
        for row in rows:
            held = found.get(str(row["node_id"]))
            row["match"] = held.first if held is not None else None
            row["match_count"] = held.count if held is not None else 0
            if with_lines:
                row["matches"] = held.entries if held is not None else []

    def _expand_text_row_fields(
        conn: psycopg.Connection,
        session: Session,
        condition: Any,
        rows: list[dict[str, Any]],
        *,
        select: Any,
    ) -> None:
        """filter's match fields on expand's record rows (#3666): `match`
        and `match_count` with no `select`; with one, only the match fields
        it names (`matches` only when named). Read for this page's records
        only, as `_text_row_fields` reads filter's page."""
        _text_row_fields(conn, session, condition, rows, tool="expand", select=select)
        if isinstance(select, list):
            for row in rows:
                for name in ("match", "match_count"):
                    if name not in select:
                        row.pop(name, None)

    def _text_default_columns(
        columns: list[str],
        projected: list[dict[str, Any]],
        page_rows: list[dict[str, Any]],
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """The default row with the two match fields after its own (#3666)."""
        extra = ["match", "match_count"]
        return [*columns, *extra], [
            {**row, **{key: source.get(key) for key in extra}}
            for row, source in zip(projected, page_rows)
        ]

    def _text_fact(condition: Any, facts: Mapping[str, Any]) -> dict[str, Any]:
        from okg.substrate.mcp.text_match import MATCHES_CAP

        return {"value": condition.value, "matches_cap": MATCHES_CAP, **facts}

    def _expand_text_filter(
        conn: psycopg.Connection,
        session: Session,
        node_id: str,
        *,
        filter: Any,
        edge_type: Optional[str],
        direction: str,
    ) -> tuple[Any, dict[str, Any] | None, Any]:
        """A one-hop expand's far-end filter with its text clause (#3666)
        replaced by the ids of the far-end records whose text matches.

        The far end of one record's links is read first (at most
        `text_match.SCAN_LIMIT` records of the types the filter requires,
        or refused), their text is checked, and the clause becomes
        `node_id` in those ids, so the expand's own SQL decides, pages and
        counts the rows as for any other filter. Returns the filter, the
        facts for the reply and the text condition (None, None without a
        text clause). Both one-hop shapes use it; a record row then gets
        its match fields (`_text_row_fields`), a link row none."""
        from okg.substrate.mcp import text_match as tm

        rest, condition = tm.split_text_condition(filter, tool="expand")
        if condition is None:
            return filter, None, None
        if deployment_requires_nomos_runtime(session.deployment_name):
            raise tm.refuse_under_access_rules("expand")
        if direction not in ("out", "in"):
            raise InvalidOperatorArguments("expand: direction must be 'out' or 'in'")
        subtypes = tm.type_scope(rest)
        far = tm.neighbour_ids(
            conn, session, node_id, edge_type=edge_type, direction=direction,
            subtypes=subtypes,
        )
        if len(far) > tm.SCAN_LIMIT:
            refusal = tm.refuse_scan_limit(
                "expand", indexable=tm.narrowing_query(condition.parsed) is not None,
                text_over=False,
                rest_over=True, rest_given=True, set_words="the far end of these links",
            )
            narrowers = [] if subtypes is not None else ["target_subtype (or a type in where)"]
            if edge_type is None:
                narrowers.append("edge_type")
            refusal.fix_hint = (
                f"narrow with {' or '.join(narrowers)} to at most "
                f"{tm.SCAN_LIMIT:,} linked records"
                if narrowers else
                f"this record links more than {tm.SCAN_LIMIT:,} records of "
                "that type: match the records with filter instead, then "
                "expand from them"
            )
            raise refusal
        ids = tm.matching_record_ids(conn, session, condition, tool="expand", record_ids=far)
        clause: dict[str, Any] = (
            {"op": "in", "field": "node_id", "values": ids} if ids else
            # No far-end record matches: a condition no record meets, which
            # the expand's SQL and its row check both read as false (an
            # absent far end included).
            {"op": "and", "clauses": [
                {"op": "exists", "field": "node_id"},
                {"op": "not", "clause": {"op": "exists", "field": "node_id"}},
            ]}
        )
        narrowed = clause if rest in (None, {}) else {"op": "and", "clauses": [rest, clause]}
        facts = _text_fact(condition, {"records_scanned": len(far), "scan": "set"})
        return narrowed, facts, condition

    def _effective_view_aggregate_groups(
        conn: psycopg.Connection, session: Session, **arguments: Any,
    ) -> tuple[list[tuple[tuple[Any, ...], int]], str] | None:
        """``_effective_view_aggregate_groups_within``; for a count that
        carries a date condition (``date_budget``), as one phase of its
        date budget (``_within_date_budget``). Any other count goes
        straight through."""
        budget = arguments.get("date_budget")
        if budget is None:
            return _effective_view_aggregate_groups_within(conn, session, **arguments)
        if _date_rerun_spent(budget):
            return _effective_view_aggregate_groups_within(conn, session, **arguments)
        budget.mark("before_count")
        try:
            return _within_date_budget(
                conn, budget,
                lambda: _effective_view_aggregate_groups_within(conn, session, **arguments),
                phase="count_other",
            )
        finally:
            budget.mark("count_other")

    def _effective_view_aggregate_groups_within(
        conn: psycopg.Connection,
        session: Session,
        *,
        kind: str,
        by: list[str],
        predicate: Any,
        member_ids: Sequence[str] | None = None,
        period_report: Any = None,
        period_ranges: Any = None,
        each_report: dict[str, Any] | None = None,
        fingerprint_predicate: Any = ...,
        date_budget: Any = None,
    ) -> tuple[list[tuple[tuple[Any, ...], int]], str] | None:
        """MUST-FIX #1: count an aggregate ENTIRELY in SQL over the indexed
        snapshot -- ``SELECT <by-exprs>, count(*) ... GROUP BY <by-exprs>`` -- so a
        >10K-row subtype is counted COMPLETELY, never truncated to a 10K window.

        ``predicate`` is the whole scope (subtype or edge_type plus the input
        filter), compiled once by the operator compiler (#3201). Returns the
        full ``[(group_key, count)]`` (ordered count desc) and the predicate
        fingerprint, or ``None`` when the aggregate cannot run exactly in SQL:

          * Nomos runtime evaluation is on (per-row visibility/redaction can
            drop or rewrite rows, which a GROUP BY cannot honor); or
          * a `by` field or the predicate is not a stable-column /
            ``attrs.<key>`` expression the compiler lowers exactly.

        The caller refuses on ``None``; there is no per-row fallback.

        ``member_ids`` (#3222) counts only a result reference's stored node
        ids, as an id restriction (see ``_effective_view_sql_page``).

        A ``<period>(<date field>)`` in ``by`` (#3658, ``aggregate_periods``)
        is counted per distinct raw date in SQL and folded into periods by
        the shared date parser; groups then come oldest first, and what the
        fold read goes into ``period_report`` (an ``aggregate_periods.
        PeriodReport``).

        An ``each(attrs.<key>)`` in ``by`` (#3666, ``aggregate_each``) gives
        one group per element of a list attribute, each element once per
        row; what was counted goes into ``each_report`` under the fixed
        names. A count over links or over list elements runs under a
        statement timeout of ``COUNT_BUDGET_MS`` and is refused past it."""
        periods = {}
        each_fields = []
        for field in by:
            parsed_each = _parse_each_field(str(field))
            if parsed_each is not None:
                each_fields.append(parsed_each)
                continue
            parsed_period = _parse_period_field(str(field))
            if parsed_period is not None:
                periods[str(field)] = parsed_period
        if len(each_fields) > 1 or (each_fields and periods):
            raise InvalidOperatorArguments(
                "aggregate: by holds each() beside another each() or a date "
                "period; one call counts one list, with plain fields only",
                data={"by": [str(f) for f in by]},
                fix_hint="send one call per each() key or period",
            )
        each_field = each_fields[0] if each_fields else None
        if deployment_requires_nomos_runtime(session.deployment_name):
            return None
        target_kind = "node" if kind == "nodes" else "edge"
        params: dict[str, Any] = {}
        base_table, source_clause, gen_where, gen_params = _effective_view_source(
            conn,
            session.generation_id,
            target_kind=target_kind,
        )
        relation = "effective_aggregate"
        if member_ids is not None:
            params["rr_member_ids"] = list(member_ids)
        if (
            periods or each_field is not None
            or _operator_refs_attrs(by) or _operator_refs_attrs(predicate)
        ):
            candidates_sql = (
                "SELECT unnest(%(rr_member_ids)s::text[])" if member_ids is not None else ""
            )
            date_keys = _date_window_keys(predicate)
            if date_keys and member_ids is None:
                # #3666: a count that carries a date condition is narrowed
                # like a page, by the ready declared indexes: the targets
                # every narrowing clause admits, or that have an overlay on
                # a key the predicate reads. The exact predicate decides.
                declared_types = _declared_registry_for(conn)
                plan, uncovered = None, []
                # An operand that holds NUL never reaches the planner, as
                # on the page path; the exact condition decides.
                if not _holds_nul(predicate) and (
                    _attr_filter_source.filter_index_mode() == "expression"
                ):
                    plan, uncovered = _attr_filter_source.plan_expression_candidates(
                        conn,
                        generation_id=session.generation_id,
                        predicate=_date_narrowing_predicate(predicate, declared_types),
                        declared=declared_types,
                        target_kind=target_kind,
                    )
                _note_date_index(
                    date_budget, predicate, declared=declared_types,
                    narrowed=plan.attrs if plan is not None else (),
                    uncovered=uncovered,
                )
                if plan is not None:
                    params.update(plan.params)
                    keys: set[str] = set()

                    def attr_keys(node: Any, into: set[str] = keys) -> None:
                        if isinstance(node, dict):
                            name = node.get("field")
                            if isinstance(name, str) and name.startswith("attrs."):
                                into.add(name.split(".", 1)[1])
                            for child in node.values():
                                attr_keys(child)
                        elif isinstance(node, list):
                            for child in node:
                                attr_keys(child)

                    attr_keys(predicate)
                    params["overlay_attr_keys"] = sorted(
                        key for key in keys
                        if "\x00" not in key and key not in RESERVED_EFFECTIVE_ATTR_KEYS
                    )
                    candidates_sql = (
                        f"({plan.sql}) UNION ("
                        f"SELECT overlay_target.target_id "
                        f"FROM {ATTRIBUTE_EFFECTIVE_TABLE} AS overlay_target "
                        "WHERE overlay_target.target_kind = %(tk)s "
                        "AND overlay_target.generation_id = %(overlay_gen)s "
                        "AND overlay_target.namespace = 'default' "
                        "AND overlay_target.attr_key = ANY(%(overlay_attr_keys)s::text[]))"
                    )
            from_clause = _effective_view_runtime_source(
                base_table=base_table,
                source_clause=source_clause,
                gen_where=gen_where,
                gen_params=gen_params,
                target_kind=target_kind,
                generation_id=session.generation_id,
                relation=relation,
                params=params,
                base_candidates_sql=candidates_sql,
            )
        else:
            from_clause = _effective_view_aggregate_narrow_source(
                base_table=base_table,
                gen_where=gen_where,
                gen_params=gen_params,
                target_kind=target_kind,
                relation=relation,
                params=params,
            )

        def _field_sql(field: str) -> str | None:
            if field == "attrs":
                return None  # grouping by the whole attrs blob is unsupported
            if field.startswith("attrs."):
                key = field.split(".", 1)[1]
                if not key:
                    return None
                params_key = f"bk{len(params)}"
                params[params_key] = key
                # ``->`` (NOT ``->>``) keeps the JSONB value so psycopg decodes it
                # to the SAME Python type the per-row path's
                # ``_operator_field_value`` returns (bool/int/str/None), preserving
                # group-key parity. ``->>`` would coerce every value to text and
                # diverge (True -> 'true').
                return f"({relation}.attrs -> %({params_key})s)"
            col = _EFFECTIVE_VIEW_FIELD_COLUMNS[kind].get(field)
            return f"{relation}.{col}" if col else None

        date_attrs: dict[str, dict[str, str]] = {}
        if periods:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT name, metadata FROM okg.subtypes "
                    "WHERE catalog_version_id = %s",
                    (session.catalog_version_id,),
                )
                catalog_meta = {
                    str(name): meta or {} for name, meta in cur.fetchall()
                }
            for text, period in periods.items():
                if period.meaning is None:
                    continue
                meta_key = _DATE_MEANINGS[period.meaning]
                date_attrs[text] = {
                    subtype: str(meta[meta_key]).strip()
                    for subtype, meta in sorted(catalog_meta.items())
                    if isinstance(meta.get(meta_key), str)
                    and str(meta[meta_key]).strip()
                }
                if not date_attrs[text]:
                    raise _period_not_declared_refusal(period)

        def _period_value_sql(text: str) -> str:
            """The period field's stored JSON value, per record type for a
            named date (NULL on a type that declares none)."""
            period = periods[text]
            if period.attr is not None:
                key = f"bk{len(params)}"
                params[key] = period.attr
                return f"({relation}.attrs -> %({key})s)"
            branches = []
            for subtype, attr in date_attrs[text].items():
                s_key, a_key = f"bk{len(params)}", f"bk{len(params) + 1}"
                params[s_key], params[a_key] = subtype, attr
                branches.append(
                    f"WHEN %({s_key})s THEN {relation}.attrs -> %({a_key})s"
                )
            return f"(CASE {relation}.subtype {' '.join(branches)} END)"

        by_exprs: list[str] = []
        each_value = ""
        for field in by:
            if each_field is not None and str(field) == each_field.text:
                key = f"bk{len(params)}"
                params[key] = each_field.attr
                each_value = f"({relation}.attrs -> %({key})s)"
                by_exprs.append("each_element.element")
                continue
            if str(field) in periods:
                value = _period_value_sql(str(field))
                # Raw text and whether it is a JSON string: the fold reads
                # it with the shared date parser (aggregate_periods).
                by_exprs.append(f"({value} #>> '{{}}')")
                by_exprs.append(f"(jsonb_typeof({value}) = 'string')")
                continue
            expr = _field_sql(str(field))
            if expr is None:
                return None
            by_exprs.append(expr)
        if date_attrs:
            by_exprs.append(f"{relation}.subtype")

        compiled = _compile_operator_predicate(
            kind,
            predicate,
            params,
            declared=_declared_registry_for(conn),
            relation=relation,
            fingerprint_of=fingerprint_predicate,
        )
        if compiled is None:
            return None
        where_sql = compiled.bind_into(params)
        if member_ids is not None:
            where_sql = (
                f"({where_sql}) AND {relation}.target_id = ANY(%(rr_member_ids)s::text[])"
            )

        select_list = ", ".join(f"{expr} AS g{i}" for i, expr in enumerate(by_exprs))
        group_list = ", ".join(str(i + 1) for i in range(len(by_exprs)))
        facts_sql = ""
        if each_value:
            v = each_value
            # A list's elements, each once per row and none of them JSON
            # null; a value that is not a list reads as no element here and
            # is left out of the groups by the WHERE below.
            elements = (
                "SELECT DISTINCT each_e.element FROM jsonb_array_elements("
                f"CASE WHEN jsonb_typeof({v}) = 'array' THEN {v} "
                "ELSE '[]'::jsonb END) AS each_e(element) "
                "WHERE each_e.element <> 'null'::jsonb"
            )
            sql = (
                f"SELECT {select_list}, count(*) AS n "
                f"FROM {from_clause} LEFT JOIN LATERAL ({elements}) AS each_element "
                f"ON TRUE WHERE ({where_sql}) AND ({v} IS NULL OR "
                f"jsonb_typeof({v}) IN ('array', 'null')) GROUP BY {group_list}"
            )
            facts_sql = (
                "SELECT count(*), "
                f"count(*) FILTER (WHERE ({v} IS NULL OR jsonb_typeof({v}) IN "
                f"('array', 'null')) AND NOT EXISTS ({elements})), "
                f"count(*) FILTER (WHERE jsonb_typeof({v}) NOT IN ('array', 'null')), "
                f"count(*) FILTER (WHERE jsonb_typeof({v}) = 'array') "
                f"FROM {from_clause} WHERE {where_sql}"
            )
        else:
            sql = (
                f"SELECT {select_list}, count(*) AS n "
                f"FROM {from_clause} "
                f"WHERE {where_sql} GROUP BY {group_list}"
            )
        if periods:
            sql += f" LIMIT {_MAX_DISTINCT_DATE_ROWS + 1}"

        def count_groups() -> tuple[list, Any]:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                facts_row = None
                if facts_sql:
                    cur.execute(facts_sql, params)
                    facts_row = cur.fetchone()
            return rows, facts_row

        # #3666: a count with date work (a date condition or a date period)
        # runs under what is left of its date budget. Any other count over
        # links, or over every element of every list, runs under the count
        # budget (`aggregate_each.COUNT_BUDGET_MS`); past either budget it is
        # refused, never cut. Any other count is as before.
        if date_budget is None:
            if kind == "edges" or each_value:
                fetched, counted = _run_within_count_budget(
                    conn, sql, params, facts_sql, links=kind == "edges",
                )
            else:
                fetched, counted = count_groups()
        else:
            date_budget.mark("count_setup")
            # The groups do not depend on the page's limit: a rerun to fit
            # max_bytes takes the first run's.
            call = _current_date_call()
            groups_key = _date_memo_key("groups", sql, params)
            if call is not None and groups_key in call.pages:
                memo_rows, counted = call.pages[groups_key]
                fetched = list(memo_rows)
            else:
                try:
                    fetched, counted = _under_date_budget(
                        conn, date_budget, "group count", count_groups, phase="group_count",
                    )
                finally:
                    date_budget.mark("group_count")
                if call is not None:
                    call.pages[groups_key] = (list(fetched), counted)
        if each_field is not None and counted is not None:
            rows_counted, rows_with_none, rows_not_a_list, rows_with_list = (
                int(value) for value in counted
            )
            if rows_not_a_list and not rows_with_list:
                raise _each_no_list_refusal(each_field, rows_not_a_list=rows_not_a_list)
            if each_report is not None:
                each_report.update(_each_facts(
                    rows_counted=rows_counted, rows_with_none=rows_with_none,
                    rows_not_a_list=rows_not_a_list,
                ))
        if periods:
            if len(fetched) > _MAX_DISTINCT_DATE_ROWS:
                raise _period_too_many_rows_refusal(len(fetched))
            report = period_report if period_report is not None else _PeriodReport()
            if date_attrs:
                seen = {str(r[-2]) for r in fetched}
                for text, by_subtype in date_attrs.items():
                    report.date_fields[text] = {
                        s: a for s, a in by_subtype.items() if s in seen
                    }
                    if fetched and not report.date_fields[text]:
                        raise _period_not_declared_refusal(periods[text])
            for text, period in periods.items():
                if period.attr is not None:
                    report.date_fields[text] = {"*": period.attr}
            with _date_doing(date_budget, "fold"):
                folded = _fold_period_groups(
                    fetched, by=[str(f) for f in by], periods=periods, report=report,
                    ranges=period_ranges,
                    # (A rerun with the budget spent folds the groups it
                    # kept: nothing is read, so nothing is cut.)
                    check=(
                        date_budget.check
                        if date_budget is not None and not _date_rerun_spent(date_budget)
                        else None
                    ),
                )
            _mark_date_phase("fold", date_budget)
            return folded, compiled.fingerprint
        results: list[tuple[tuple[Any, ...], int]] = []
        for r in fetched:
            key = tuple(r[i] for i in range(len(by_exprs)))
            results.append((key, int(r[-1])))
        # Order IDENTICALLY to the per-row evaluation (count desc, then
        # str(key) asc) so the group page + truncation are deterministic.
        results.sort(key=lambda item: (-item[1], tuple(str(v) for v in item[0])))
        return results, compiled.fingerprint

    def _compile_operator_predicate(
        kind: str,
        predicate: Any,
        params: Mapping[str, Any],
        *,
        declared: Mapping[str, str] | None,
        relation: str | None = None,
        nullable: bool = False,
        readable: Callable[[str], str] | None = None,
        fingerprint_of: Any = ...,
    ) -> _CompiledPredicate | None:
        """Compile an operator predicate over the effective-attribute relation.

        The one compiler (design 3.2a, D16): ``compile_read_predicate`` with
        ``effective_attr_lowerer``. ``params`` seeds the parameter allocator
        and is not modified; merge the result with ``bind_into``. A no-op
        predicate (None/True/{}) compiles to ``TRUE``.

        ``readable`` (read-model slice 7a, Nomos `audit`/`enforce`) is the
        reader view's per-key SQL guard; see ``effective_attr_lowerer``. The
        compile is then three-valued on purpose: the caller admits a row when
        it IS TRUE or a key it reads is undecided.

        ``fingerprint_of`` (#3666) is the predicate as the caller wrote it
        when ``predicate`` is its date rewrite: the fingerprint is then of
        the caller's predicate, so it never depends on a stored value the
        rewrite carries.

        Returns None when any part cannot be lowered exactly and two-valued.
        None is a refusal signal: production callers must not fall back to
        materialising a broad Python row set.
        """
        if predicate in (None, True, {}):
            predicate = None
        elif not isinstance(predicate, dict):
            return None
        identity: dict[str, Any] = {}
        if fingerprint_of is not ...:
            identity["identity_predicate"] = _encode_nonfinite_operands(
                None if fingerprint_of in (None, True, {}) else fingerprint_of
            )
        leaf_lowerer = _effective_attr_lowerer(
            kind, declared=declared, relation=relation, nullable=nullable,
            readable=readable,
        )
        if _run_steps.unresolved_marker(predicate):
            refusal = _run_steps.stale_marker_refusal()
            raise InvalidOperatorArguments(refusal.message, fix_hint=refusal.fix_hint)
        if _run_steps.holds_marker(predicate):
            # `run` (#3666): a reference to an earlier step's set or values
            # lowers to that step's query as a subquery; a reference under
            # `not` is marked so it lowers to a form the planner hashes.
            predicate = _run_steps.mark_negated(predicate)
            plain_lowerer = leaf_lowerer
            attrs_sql = (
                f"COALESCE({relation + '.' if relation else ''}attrs, '{{}}'::jsonb)"
                if nullable else f"{relation + '.' if relation else ''}attrs"
            )

            def leaf_lowerer(node: Any, bound: dict[str, Any], prefix: str) -> Any:
                lowered = _run_steps.lower_marker(
                    node, kind=kind, columns=_EFFECTIVE_VIEW_FIELD_COLUMNS[kind],
                    relation=relation or "", attrs_sql=attrs_sql,
                    readable=readable is not None,
                )
                if lowered is _run_steps.NOT_A_MARKER:
                    return plain_lowerer(node, bound, prefix)
                return None if lowered is None else _SqlLeaf(lowered)

        try:
            compiled = _compile_read_predicate(
                _encode_nonfinite_operands(predicate),
                **identity,
                leaf_lowerer=leaf_lowerer,
                field_map_identity=_effective_field_map_identity(kind, declared)
                + (":reader_view" if readable is not None else ""),
                prefix="pv",
                params=params,
            )
        except ValueError:
            return None
        if compiled.qualification != "exact" or not compiled.truth_exact:
            return None
        return compiled

    #: Sentinel: a statement run "soft" under the date budget was cancelled.
    _DATE_STATEMENT_CANCELLED = object()
    #: #3666: the stored-forms read used its whole per-call share
    #: (`_DATE_VALUES_READ_BUDGET_MS`) and did not finish: the scope is too
    #: large, not merely this call short of time.
    _DATE_FORMS_TOO_LARGE = object()

    def _under_date_budget(
        conn: psycopg.Connection,
        budget: Any,
        label: str,
        run: Callable[[], Any],
        *,
        cap_ms: int | None = None,
        soft: bool = False,
        phase: str | None = None,
    ) -> Any:
        """``run()`` with ``statement_timeout`` set to what is left of the
        call's date budget (#3666), at most ``cap_ms``.

        Set as grep sets its timeout (``grep_search._run``): with
        ``set_config(..., true)`` inside a transaction (a savepoint when the
        caller holds one) and put back afterwards, so it never outlives the
        statement; a cancelled statement rolls that transaction back, which
        restores the setting. With no time left, or when the server cancels
        the statement, the budget's refusal is raised; ``soft`` returns
        ``_DATE_STATEMENT_CANCELLED`` instead, for a statement the call can
        do without.

        ``phase`` names the statement's time (``budget.doing``): when the
        call is refused inside it, the refusal's breakdown says so. The
        phase's own per-statement deadline (``_within_date_budget``) stands
        aside: this statement sets its exact timeout itself.
        """
        remaining = budget.remaining_ms()
        if remaining < int(_DATE_STATEMENT_FLOOR_MS):
            # Too little left to start a statement: decided before it is
            # issued, so the call ends inside its budget.
            if soft:
                return _DATE_STATEMENT_CANCELLED
            raise budget.refusal()
        timeout_ms = max(1, remaining if cap_ms is None else min(remaining, int(cap_ms)))
        budget.statements.append((label, timeout_ms))
        outer_doing, budget.doing = budget.doing, phase or label
        guard = _date_guards.pop(id(conn), None)
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SELECT current_setting('statement_timeout')")
                    previous = cur.fetchone()[0]
                    cur.execute(
                        "SELECT set_config('statement_timeout', %s, true)",
                        (f"{timeout_ms}ms",),
                    )
                result = run()
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT set_config('statement_timeout', %s, true)", (previous,),
                    )
                return result
        except psycopg.errors.QueryCanceled:
            if soft:
                budget.mark(phase or label)
                return _DATE_STATEMENT_CANCELLED
            raise budget.refusal() from None
        finally:
            budget.doing = outer_doing
            if guard is not None:
                _date_guards[id(conn)] = guard

    #: #3666: per connection in a date phase, the check each of its
    #: statements passes before it is sent (`_within_date_budget`).
    _date_guards: dict[int, Callable[[Any, Any], None]] = {}
    _date_guarded_cursors: dict[type, type] = {}

    def _date_guarded_cursor(base: type) -> type:
        """``base`` (a connection's cursor class) with ``execute`` passing
        the connection's date guard first, when it has one."""
        if base in _date_guarded_cursors.values():
            return base
        guarded = _date_guarded_cursors.get(base)
        if guarded is None:
            def execute(self: Any, query: Any, params: Any = None, **options: Any) -> Any:
                guard = _date_guards.get(id(self.connection))
                if guard is not None:
                    guard(self, base.execute)
                return base.execute(self, query, params, **options)

            guarded = type(f"DateDeadline{base.__name__}", (base,), {"execute": execute})
            _date_guarded_cursors[base] = guarded
        return guarded

    @contextmanager
    def _date_doing(budget: Any, phase: str) -> Iterator[None]:
        """Name the work in progress for a refusal's breakdown: Python
        work (a fold, a row loop) that ends by ``budget.check()``."""
        if budget is None:
            yield
            return
        outer, budget.doing = budget.doing, phase
        try:
            yield
        finally:
            budget.doing = outer

    def _within_date_budget(
        conn: psycopg.Connection, budget: Any, run: Callable[[], Any],
        *, phase: str | None = None,
    ) -> Any:
        """``run()`` as one phase of a call's date work (#3666).

        Every statement the phase issues on ``conn``, not only the ones
        ``_under_date_budget`` times one by one (the readiness checks, the
        catalog and planner reads, a candidate probe), runs with
        ``statement_timeout`` at most what is left of the budget when the
        phase starts, so nothing a budgeted call issues can outlive the
        call. A statement the server cancels, and a phase entered with no
        time left, refuse ``date_call_over_budget``. The setting is put
        back when the phase ends.

        ``statement_timeout`` bounds one statement, counted from its own
        start. So the setting follows the clock: before each statement of
        the phase the time left is read again, and once the setting is
        ``_DATE_TIMEOUT_REFRESH_MS`` stale it is set anew (one extra round
        trip, on the same cursor); with less than the floor left the
        statement is not sent and the call is refused. A statement begun
        late in a phase therefore cannot run past the deadline by what the
        phase had left when it began. ``phase`` names the phase's time for
        a refusal's breakdown.
        """
        remaining = budget.remaining_ms()
        if remaining < int(_DATE_STATEMENT_FLOOR_MS):
            raise budget.refusal()
        with conn.cursor() as cur:
            cur.execute("SELECT current_setting('statement_timeout')")
            previous = cur.fetchone()[0]
            cur.execute(
                "SELECT set_config('statement_timeout', %s, false)", (f"{remaining}ms",),
            )
        applied = {"ms": remaining}

        def guard(cursor: Any, plain_execute: Any) -> None:
            left = budget.remaining_ms()
            if left < int(_DATE_STATEMENT_FLOOR_MS):
                raise budget.refusal()
            if left <= applied["ms"] - int(_DATE_TIMEOUT_REFRESH_MS):
                plain_execute(
                    cursor, "SELECT set_config('statement_timeout', %s, false)",
                    (f"{left}ms",),
                )
                applied["ms"] = left

        key = id(conn)
        outer_guard = _date_guards.get(key)
        outer_doing, budget.doing = budget.doing, phase or budget.doing
        cursor_class = conn.cursor_factory
        conn.cursor_factory = _date_guarded_cursor(cursor_class)
        _date_guards[key] = guard
        try:
            return run()
        except psycopg.errors.QueryCanceled:
            raise budget.refusal() from None
        finally:
            if outer_guard is None:
                _date_guards.pop(key, None)
            else:
                _date_guards[key] = outer_guard
            conn.cursor_factory = cursor_class
            budget.doing = outer_doing
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT set_config('statement_timeout', %s, false)", (previous,),
                    )
            except psycopg.Error:
                # The transaction is aborted (a cancelled statement outside
                # a savepoint): its rollback puts the setting back.
                pass

    #: The date budget of a tool body called outside the byte-budget
    #: wrapper (a direct call), for the one log line that says where its
    #: time went (`_log_date_call`). Cleared at tool entry. A call through
    #: the wrapper keeps its budget on the call's clock instead
    #: (`response_bounds.DateCallClock`), which the wrapper drops when the
    #: call ends, however it ends.
    _date_budget_of_call: "contextvars.ContextVar[Any]" = contextvars.ContextVar(
        "okg_date_budget_of_call", default=None,
    )

    def _date_budget_in_flight() -> Any:
        call = _current_date_call()
        return call.budget if call is not None else _date_budget_of_call.get()

    def _date_call_started() -> float:
        """Tool entry: when the call's clock started.

        Through the byte-budget wrapper that is when the wrapper first
        dispatched the call: a rerun of the body to fit a page enters here
        again and gets the same instant, so it takes from the same budget.
        Called directly, the clock starts here and any earlier call's
        budget is forgotten."""
        call = _current_date_call()
        if call is None:
            _date_budget_of_call.set(None)
            return time.monotonic()
        call.entries += 1
        call.served = False
        call.pool_wait_ms = call.session_open_ms = 0.0
        if call.budget is not None:
            # A rerun: since the last run's stamp the wrapper measured the
            # page against max_bytes and chose a smaller limit.
            call.budget.mark("fit")
        return call.started_at

    def _log_date_call(budget: Any, outcome: str) -> None:
        """One debug line per call that carried a date condition: the
        milliseconds of each phase and the total, summed over the reruns of
        the tool body the byte-budget wrapper made (`reruns`). `open` is
        tool entry to the first date work (session open); `rest` is whatever
        followed the last named phase. A served reply is rendered and
        serialised after this line, which it therefore does not count."""
        wanted = os.environ.get(DATE_CALL_LOG_ENV, "debug").strip().lower()
        level = _DATE_CALL_LOG_LEVELS.get(wanted)
        if level is None:
            # Said, not guessed: the line still comes out, where it is seen.
            log.warning(
                "%s=%r is not one of %s; the date call line is logged at WARNING",
                DATE_CALL_LOG_ENV, wanted, sorted(_DATE_CALL_LOG_LEVELS),
            )
            level = logging.WARNING
        log.log(
            level,
            "date call %s: outcome=%s total_ms=%d budget_ms=%d reruns=%d "
            "uncharged_ms=%d phases=%s",
            budget.operator, outcome, budget.elapsed_ms(), budget.total_ms,
            budget.reruns, budget.uncharged_ms, _json.dumps(budget.phases),
        )

    def _mark_date_phase(phase: str, budget: Any = None) -> None:
        budget = budget if budget is not None else _date_budget_in_flight()
        if budget is not None:
            budget.mark(phase)

    def _date_budget(
        operator: str, predicate: Any, *, kind: str, started_at: float | None = None,
    ) -> Any:
        """The budget of ``operator``'s date work, or None when ``predicate``
        (the condition the call compiles) holds no date condition: such a
        call gets no budget and no timeout. A date period in ``by`` is not a
        date condition, nor is a range on a period's own date, which the
        period fold applies; those counts run as before.

        One budget per call, created once. ``started_at`` is when the
        call's clock started (``_date_call_started``): the deadline counts
        from there, so opening the session and the readiness checks before
        the first date statement are inside it. When the byte-budget wrapper
        runs the tool body again to fit a page, the rerun gets the budget
        the first run created, with whatever time is left.
        """
        fields = _date_leaf_fields(predicate)
        if not fields:
            return None
        call = _current_date_call()
        budget = call.budget if call is not None else None
        entry = call.entries if call is not None else None
        created = budget is None
        if created:
            if call is not None:
                started_at = call.started_at
            budget = _DateBudget(
                operator, fields, total_ms=int(_DATE_CALL_BUDGET_MS),
                typed=_date_names_a_type(predicate, kind), kind=kind, started_at=started_at,
            )
            budget.report = _log_date_call
            if call is not None:
                call.budget = budget
            else:
                _date_budget_of_call.set(budget)
        else:
            budget.reruns = max(budget.reruns, call.entries - 1)
        budget.mark("open")
        if call is not None and (call.pool_wait_ms or call.session_open_ms):
            # Of `open`: the wait for a pooled connection and the session.
            waited, opened = round(call.pool_wait_ms), round(call.session_open_ms)
            call.pool_wait_ms = call.session_open_ms = 0.0
            budget.phases["pool_wait"] = budget.phases.get("pool_wait", 0) + waited
            budget.phases["session_open"] = budget.phases.get("session_open", 0) + opened
            budget.phases["open"] = max(0, budget.phases["open"] - waited - opened)
        # Once per run of the tool body.
        if (created or getattr(budget, "observed_entry", None) != entry) and (
            _DATE_BUDGET_OBSERVER is not None
        ):
            _DATE_BUDGET_OBSERVER(budget)
        budget.observed_entry = entry
        return budget

    def _is_over_date_budget(exc: BaseException) -> bool:
        return isinstance(exc, DateCallOverBudget)

    def _date_rerun_spent(budget: Any) -> bool:
        """Whether this is a rerun of the tool body (to fit max_bytes) with
        the call's date budget spent. It differs from the run before it in
        its limit alone, so what it needs was read then and is kept on the
        call's clock: it is not refused on entry. A statement it would
        still have to issue is refused as always."""
        call = _current_date_call()
        return (
            call is not None and call.entries > 1
            and budget.remaining_ms() < int(_DATE_STATEMENT_FLOOR_MS)
        )

    def _date_memo_key(what: str, statement: str, params: Mapping[str, Any]) -> tuple:
        return (what, statement, _json.dumps(
            {k: v for k, v in params.items() if k not in ("page_limit", "page_offset")},
            sort_keys=True, default=str,
        ), params.get("page_offset"))

    def _date_candidates_are_wide(
        conn: psycopg.Connection, budget: Any, candidates_sql: str,
        params: Mapping[str, Any],
    ) -> bool:
        """Whether an index candidate set holds more than
        ``_DATE_WIDE_CANDIDATES`` targets: a count that stops at the bound,
        so it reads at most that many index entries. One that does not
        finish in its share counts as wide. Kept for the call's reruns."""
        call = _current_date_call()
        key = _date_memo_key("probe", candidates_sql, params)
        if call is not None and key in call.pages:
            return bool(call.pages[key])
        bound = int(_DATE_WIDE_CANDIDATES)

        def count() -> int:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT count(*) FROM (SELECT 1 FROM ({candidates_sql}) "
                    f"AS date_candidates LIMIT {bound + 1}) AS date_candidate_probe",
                    dict(params),
                )
                return int(cur.fetchone()[0])

        found = _under_date_budget(
            conn, budget, "candidate probe", count,
            cap_ms=_DATE_PROBE_SHARE_MS, soft=True, phase="candidate_probe",
        )
        budget.mark("candidate_probe")
        wide = found is _DATE_STATEMENT_CANCELLED or found > bound
        if call is not None:
            call.pages[key] = wide
        return wide

    def _note_date_index(
        budget: Any, predicate: Any, *, declared: Mapping[str, str] | None,
        narrowed: Sequence[str], uncovered: Sequence[tuple[str, str]],
        unused: str | None = None,
    ) -> None:
        """Record on ``budget``, per stored attribute of a date condition,
        why no declared timestamp index narrowed the read (nothing for one
        that did), so an over-budget refusal can say so."""
        if budget is None:
            return
        reasons = dict(uncovered)
        for key in _date_window_keys(predicate):
            if key in narrowed:
                budget.date_index.pop(key, None)
            elif unused is not None:
                budget.date_index[key] = unused
            elif key in reasons:
                budget.date_index[key] = reasons[key]
            elif (declared or {}).get(key) != "ts":
                budget.date_index[key] = _DATE_INDEX_NOT_DECLARED_TS
            else:
                budget.date_index[key] = _DATE_INDEX_NOT_NARROWABLE

    #: #3666: what `_dated_read` found a date scope to be, so a later call
    #: or page with the same scope answers as the first did without the
    #: read of stored date forms. Two facts, never a stored value:
    #: `_DATE_SCOPE_TOO_LARGE` (the forms could not be examined in time:
    #: ISO-8601 values alone, with the note), or a mapping with the fields
    #: that store a value with no time zone and the per-field counts of
    #: values that are not dates (nothing stored there changes a row
    #: against the ISO-8601 comparison, so that comparison is exact). The
    #: key is the database, the generation, the kind, the attributes and
    #: types read, and the call's other conditions; a new generation is a
    #: new key. Bounded, in this process only.
    _DATE_SCOPES_MAX = 256
    _DATE_SCOPE_TOO_LARGE = "too_large"
    _date_scopes: dict[tuple[Any, ...], Any] = {}
    _date_scopes_lock = threading.Lock()

    def _resolve_date_conditions(
        conn: psycopg.Connection, session: Session, **arguments: Any,
    ) -> Any:
        """``_resolve_date_conditions_within`` as one phase of the call's
        date budget (``_within_date_budget``); a predicate with no date
        condition is returned as it came."""
        if not _holds_date_condition(arguments["predicate"]):
            return arguments["predicate"]
        if arguments.get("budget") is None:
            arguments["budget"] = _date_budget(
                arguments["operator"], arguments["predicate"], kind=arguments["kind"],
            )
        arguments["budget"].mark("before_resolve")
        try:
            return _within_date_budget(
                conn, arguments["budget"],
                lambda: _resolve_date_conditions_within(conn, session, **arguments),
                phase="resolve_other",
            )
        finally:
            arguments["budget"].mark("resolve_other")

    def _date_plan(
        conn: psycopg.Connection,
        session: Session,
        *,
        kind: str,
        predicate: Any,
        operator: str,
        report: Any,
    ) -> tuple[dict[str, dict[str, list[str]]], dict[str, Any], dict[str, list[Any]]]:
        """What the date conditions of ``predicate`` read (#3666):
        ``(attrs_for, fields, windows_by_field)``, the stored attribute each
        date field names per record type, the parsed fields, and each
        clause's windows. Refuses by name a field no selected type declares,
        a named date on links, an operator a date does not take and a bound
        that is not a date. Reads the catalog only, never a stored value.
        ``report`` is told which bounds had no time zone."""
        fields: dict[str, Any] = {}
        windows_by_field: dict[str, list[Any]] = {}
        for leaf in _date_leaves(predicate):
            parsed = _parse_date_field(
                leaf.get("field"), operator=operator, named_dates=kind == "nodes",
            )
            windows, assumed_utc = _date_leaf_windows(leaf, operator=operator)
            fields[parsed.text] = parsed
            windows_by_field.setdefault(parsed.text, []).append(windows)
            if assumed_utc:
                report.note_bound(parsed.text)
        named = [f for f in fields.values() if f.meaning is not None]
        if named and kind != "nodes":
            raise InvalidOperatorArguments(
                f"{operator}: a named date (date.created, ...) is declared per "
                "record type; edges have none",
                data={"field": named[0].text, "kind": kind},
                fix_hint="name the edge attribute, e.g. date.attrs.<key>",
            )
        attrs_for: dict[str, dict[str, list[str]]] = {}
        if named:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT name, metadata FROM okg.subtypes "
                    "WHERE catalog_version_id = %s",
                    (session.catalog_version_id,),
                )
                catalog_meta = {
                    str(name): meta or {} for name, meta in cur.fetchall()
                }
            scope = _date_scoped_subtypes(predicate)
            for date_field in named:
                meta_key = _DATE_MEANINGS[date_field.meaning]
                by_attr: dict[str, list[str]] = {}
                for subtype, meta in sorted(catalog_meta.items()):
                    attr = meta.get(meta_key) if isinstance(meta, Mapping) else None
                    if not isinstance(attr, str) or not attr.strip():
                        continue
                    if scope is not None and subtype not in scope:
                        continue
                    by_attr.setdefault(attr.strip(), []).append(subtype)
                if not by_attr:
                    raise _date_not_declared_refusal(
                        date_field.text, operator=operator,
                    )
                attrs_for[date_field.text] = by_attr
        for date_field in fields.values():
            if date_field.attr is not None:
                attrs_for[date_field.text] = {date_field.attr: []}
        return attrs_for, fields, windows_by_field

    def _date_strict_predicate(predicate: Any, plan: Any, operator: str) -> Any:
        """``predicate`` comparing ISO-8601 stored values alone: any other
        stored string is unknown, under ``not`` too."""
        return _rewrite_date_leaves(
            predicate, attrs_for=plan[0], also_for={}, operator=operator, strict_only=True,
        )

    def _date_scope_key(
        conn: psycopg.Connection, session: Session, kind: str, attrs_for: Any, rest: Any,
    ) -> tuple[Any, ...]:
        info = conn.info
        return (
            info.host, info.port, info.dbname, str(session.generation_id), kind,
            _json.dumps(attrs_for, sort_keys=True),
            _json.dumps(rest, sort_keys=True, default=str),
        )

    def _date_scope_too_large(report: Any, budget: Any) -> None:
        """The reply's note that ISO-8601 stored values alone were compared
        because the other stored forms were not examined in time."""
        report.iso_only = _ISO_ONLY_SCOPE_TOO_LARGE
        # Only what this caller can still do: a call that names a type is
        # not told to name one.
        report.remedy = _REMEDY_NARROWER_CONDITION if budget.typed else _REMEDY_NAME_A_TYPE
        report.unparseable.clear()
        report.utc_stored.clear()

    _date_strict_re = re.compile(_DATE_STRICT_FORM)

    def _date_forms_change_rows(odd_rows: Any) -> bool:
        """Whether any stored value the read returned is one the ISO-only
        comparison leaves unknown and the full comparison decides: a string
        that is not in the strict form (a date in another form, a word, an
        empty string), or a string too long to be a date. A value SQL
        decides alike in both modes (a number, an impossible strict-form
        date, a strict value with no time zone) does not change a row."""
        if odd_rows is _DATE_STATEMENT_CANCELLED or odd_rows is _DATE_FORMS_TOO_LARGE:
            return True
        for row in odd_rows:
            value, is_string = row[0], bool(row[1])
            if value is None:
                continue
            if value == _DATE_LONG_VALUE or (
                is_string and not _date_strict_re.fullmatch(str(value))
            ):
                return True
        return False

    def _read_date_forms(
        conn: psycopg.Connection,
        session: Session,
        *,
        kind: str,
        predicate: Any,
        operator: str,
        plan: Any,
        report: Any,
        budget: Any,
        candidates_sql: str = "",
        candidate_params: Mapping[str, Any] | None = None,
    ) -> Any:
        """Read the stored values of each date field that SQL does not
        decide by itself (#3666), as distinct values with counts.

        Returns ``(also_for, changes_rows)``: per field and clause, the
        stored strings the one date parser reads inside the clause's
        windows (the leaf matches them by equality), and whether any value
        read would change a row against the ISO-only comparison
        (``_date_forms_change_rows``). ``report`` receives the count of
        values that are not dates and whether a value with no time zone is
        stored, for the records in scope. Returns
        ``_DATE_STATEMENT_CANCELLED`` when the read did not finish inside
        its share of the budget (``_DATE_VALUES_READ_BUDGET_MS``, and what
        is left of the call's), or ``_DATE_FORMS_TOO_LARGE`` when it was cut
        at that share itself (the scope, not the call, is the cause); more than ``MAX_ODD_DATE_VALUES`` distinct
        values refuses the call by name.

        The read is narrowed by what narrows the page itself: the call's
        other conditions, the record types the field reads, and
        ``candidates_sql`` (a result reference's members, expand's far
        endpoints). It keeps only values SQL does not decide and never
        fetches a string longer than ``MAX_DATE_TEXT``. It cannot stop
        early: it groups every record in that scope and no index serves it.
        Its outcome is kept for the reruns of the same call.
        """
        attrs_for, fields, windows_by_field = plan
        target_kind = "node" if kind == "nodes" else "edge"
        _require_effective_view(conn, session, target_kind)
        base_table, source_clause, gen_where, gen_params = _effective_view_source(
            conn, session.generation_id, target_kind=target_kind,
        )
        relation = "effective_dates"
        declared = _declared_registry_for(conn)
        rest = _without_date_clauses(predicate)
        budget.mark("readiness")
        scope_key = _date_scope_key(conn, session, kind, attrs_for, rest)
        # The stored-value reads of all fields share one
        # `_DATE_VALUES_READ_BUDGET_MS` per call, not one each.
        values_read_ms = 0
        # The read is narrowed like a page: by the caller's candidate set,
        # else by the ready declared indexes of the other conditions (the
        # targets every narrowing clause admits, or that have an overlay on
        # a key those conditions read).
        rest_params: dict[str, Any] = {}
        # An operand that holds NUL never reaches the planner, as on the
        # page path: it matches no stored value, and the exact compiled
        # condition decides.
        if not candidates_sql and rest is not None and not _holds_nul(rest) and (
            _attr_filter_source.filter_index_mode() == "expression"
        ):
            rest_plan, _uncovered = _attr_filter_source.plan_expression_candidates(
                conn, generation_id=session.generation_id, predicate=rest,
                declared=declared, target_kind=target_kind, param_prefix="dcr",
            )
            if rest_plan is not None:
                rest_params = dict(rest_plan.params)
                rest_params["dc_overlay_keys"] = sorted(rest_plan.attrs)
                candidates_sql = (
                    f"({rest_plan.sql}) UNION ("
                    f"SELECT overlay_target.target_id "
                    f"FROM {ATTRIBUTE_EFFECTIVE_TABLE} AS overlay_target "
                    "WHERE overlay_target.target_kind = %(tk_dates)s "
                    "AND overlay_target.generation_id = %(overlay_gen)s "
                    "AND overlay_target.namespace = 'default' "
                    "AND overlay_target.attr_key = ANY(%(dc_overlay_keys)s::text[]))"
                )
        budget.mark("planner")
        also_for: dict[str, dict[int, list[str]]] = {}
        changes_rows = False
        for text in fields:
            params: dict[str, Any] = {**dict(candidate_params or {}), **rest_params}
            from_clause = _effective_view_runtime_source(
                base_table=base_table,
                source_clause=source_clause,
                gen_where=gen_where,
                gen_params=gen_params,
                target_kind=target_kind,
                generation_id=session.generation_id,
                relation=relation,
                params=params,
                base_candidates_sql=candidates_sql,
                target_kind_param="tk_dates",
            )
            compiled = _compile_operator_predicate(
                kind, rest, params, declared=declared, relation=relation,
            )
            if compiled is None:
                # A clause beside the date condition does not compile. The
                # caller's own compile of the whole predicate holds that
                # clause too, so it refuses the call; nothing is read here.
                also_for = {}
                break
            where_sql = compiled.bind_into(params)
            by_attr = sorted(attrs_for[text].items())
            if not by_attr[0][1]:
                # date.attrs.<key>: the attribute on every record.
                params["dc_a0"] = by_attr[0][0]
                value_sql = f"({relation}.attrs -> %(dc_a0)s)"
            else:
                branches, types = [], []
                for index, (attr, subtypes) in enumerate(by_attr):
                    params[f"dc_a{index}"] = attr
                    params[f"dc_s{index}"] = sorted(subtypes)
                    types += subtypes
                    branches.append(
                        f"WHEN {relation}.subtype = ANY(%(dc_s{index})s::text[]) "
                        f"THEN {relation}.attrs -> %(dc_a{index})s"
                    )
                value_sql = f"(CASE {' '.join(branches)} END)"
                # Only the record types the field reads.
                params["dc_types"] = sorted(types)
                where_sql = (
                    f"({where_sql}) AND {relation}.subtype = ANY(%(dc_types)s::text[])"
                )
            params["dc_strict"] = _DATE_STRICT_FORM
            params["dc_naive"] = _DATE_NAIVE_FORM
            params["dc_long"] = _DATE_LONG_VALUE
            select_sql, odd_sql = _date_odd_values_sql(
                value_sql, "dc_strict", "dc_naive", "dc_long",
            )
            read_sql = (
                f"SELECT {select_sql}, count(*) FROM {from_clause} "
                f"WHERE ({where_sql}) AND {odd_sql} "
                f"GROUP BY 1, 2 LIMIT {_MAX_ODD_DATE_VALUES + 3}"
            )

            def read_odd_values(sql: str = read_sql, bound: dict[str, Any] = params) -> list:
                with conn.cursor() as cur:
                    cur.execute(sql, bound)
                    return [tuple(row) for row in cur.fetchall()]

            # One read per field per call: a rerun of the tool body to
            # fit a page (same call, another limit) reuses its outcome,
            # held on the call's clock and dropped with it.
            call = _current_date_call()
            read_key = (
                scope_key, text, read_sql,
                _json.dumps(params, sort_keys=True, default=str),
            )
            if call is not None and read_key in call.stored_values:
                odd_rows = call.stored_values[read_key]
            else:
                share_ms = _DATE_VALUES_READ_BUDGET_MS - values_read_ms
                if share_ms < int(_DATE_STATEMENT_FLOOR_MS):
                    # The fields before this one took the call's share.
                    odd_rows = _DATE_FORMS_TOO_LARGE
                else:
                    read_started = budget.elapsed_ms()
                    issued = len(budget.statements)
                    odd_rows = _under_date_budget(
                        conn, budget, f"stored values of {text}", read_odd_values,
                        cap_ms=share_ms, soft=True, phase="stored_values",
                    )
                    values_read_ms += budget.elapsed_ms() - read_started
                    # Cut at its share, not at what the call had left: the
                    # scope is too large. Cut earlier: only this call ran
                    # short (a slow page, slow setup), so nothing is learnt.
                    if (
                        odd_rows is _DATE_STATEMENT_CANCELLED
                        and len(budget.statements) > issued
                        and budget.statements[-1][1] >= share_ms
                    ):
                        odd_rows = _DATE_FORMS_TOO_LARGE
                if call is not None:
                    call.stored_values[read_key] = odd_rows
            budget.mark("stored_values")
            if odd_rows is _DATE_STATEMENT_CANCELLED or odd_rows is _DATE_FORMS_TOO_LARGE:
                return odd_rows
            if sum(
                1 for row in odd_rows if row[0] is not None and row[0] != _DATE_LONG_VALUE
            ) > _MAX_ODD_DATE_VALUES:
                raise _date_too_many_odd_values_refusal(
                    text, operator=operator,
                    holds_dates=_date_tally_odd_rows(text, odd_rows, None),
                )
            _date_tally_odd_rows(text, odd_rows, report)
            changes_rows = changes_rows or _date_forms_change_rows(odd_rows)
            also_for[text] = {
                index: _date_resolve_leaf(windows, odd_rows)
                for index, windows in enumerate(windows_by_field[text])
            }
        return also_for, changes_rows

    def _resolve_date_conditions_within(
        conn: psycopg.Connection,
        session: Session,
        *,
        kind: str,
        predicate: Any,
        operator: str,
        report: Any,
        nomos_runtime: bool,
        candidates_sql: str = "",
        candidate_params: Mapping[str, Any] | None = None,
        budget: Any = None,
    ) -> Any:
        """``predicate`` with its date conditions ready to compile (#3666),
        for a read whose scope the call itself bounds: a result reference's
        members, an anchor's neighbours (``candidates_sql``).

        A clause on ``date.<meaning>`` or ``date.attrs.<key>`` is rewritten
        to the internal date leaf on the stored attribute each record type
        declares (``date_conditions.rewrite_date_leaves``), which the one
        compiler lowers and ``_operator_predicate_matches`` reads. Compile
        the result with ``fingerprint_of=<the caller's predicate>``.

        The stored forms are read first here (``_read_date_forms``): the
        scope is the call's own, so the read is bounded by it. When it does
        not finish in its share the call goes on comparing ISO-8601 stored
        values alone and says so (``date_forms_reason: "scope_too_large"``);
        nothing is remembered. A read over a whole set goes through
        ``_dated_read``, which reads the page first.

        Under Nomos ``audit``/``enforce`` nothing is read: stored values
        would include ones the reader may not see. The leaves are
        ``strict_only`` and the reply says ``date_forms_reason:
        "access_rules"``.
        """
        if not _holds_date_condition(predicate):
            return predicate
        plan = _date_plan(
            conn, session, kind=kind, predicate=predicate, operator=operator, report=report,
        )
        if nomos_runtime:
            report.iso_only = _ISO_ONLY_ACCESS_RULES
            return _date_strict_predicate(predicate, plan, operator)
        if budget is None:
            budget = _date_budget(operator, predicate, kind=kind)
        forms = _read_date_forms(
            conn, session, kind=kind, predicate=predicate, operator=operator, plan=plan,
            report=report, budget=budget, candidates_sql=candidates_sql,
            candidate_params=candidate_params,
        )
        if forms is _DATE_STATEMENT_CANCELLED or forms is _DATE_FORMS_TOO_LARGE:
            _date_scope_too_large(report, budget)
            return _date_strict_predicate(predicate, plan, operator)
        return _rewrite_date_leaves(
            predicate, attrs_for=plan[0], also_for=forms[0], operator=operator,
        )

    def _dated_read(
        conn: psycopg.Connection,
        session: Session,
        *,
        kind: str,
        predicate: Any,
        operator: str,
        report: Any,
        nomos_runtime: bool,
        budget: Any,
        run: Callable[[Any], Any],
    ) -> tuple[Any, Any]:
        """Run a read over a whole set whose condition carries a date
        (#3666): ``(the predicate the result was read with, run's result)``.
        ``run(predicate)`` is the read itself, a page or a group count; it
        must build what it returns afresh each time it is called.

        The read comes first. It runs comparing ISO-8601 stored values
        alone, with all of the call's budget, and is refused
        (``date_call_over_budget``) only when it cannot be produced inside
        it. The stored values in any other form are read after it, with at
        most ``_DATE_VALUES_READ_BUDGET_MS`` of what is left:

        * none of them would change a row (the usual case: every stored
          date is ISO-8601): the first result is the answer, exactly;
        * some would: the read runs once more with them, and that is the
          answer; when there is no time left for it, the first result is
          served and the reply says it compared ISO-8601 values alone
          (``date_forms_reason: "scope_too_large"``);
        * the forms read does not finish: the same note, the same result.

        So a read that fits the budget is never refused for the work that
        follows it, and a result that leaves out a row for its stored form
        always says so.

        What a scope turned out to be is remembered per generation
        (``_date_scopes``), without any stored value: too large (only when
        the forms read was cut at its own share), or holding
        nothing that changes a row (with its counts for the reply). A later
        call or page with the same scope then runs the read once and
        answers as the first did. Compile with ``fingerprint_of=<the
        caller's predicate>``.

        Under Nomos ``audit``/``enforce`` nothing is read: one run,
        ISO-8601 values alone, ``date_forms_reason: "access_rules"``.
        """
        if not _holds_date_condition(predicate):
            return predicate, run(predicate)
        if budget is None:
            budget = _date_budget(operator, predicate, kind=kind)
        call = _current_date_call()
        settled_key = (operator, kind, _json.dumps(predicate, sort_keys=True, default=str))
        settled = call.dated.get(settled_key) if call is not None else None
        if settled is not None:
            # A rerun of this call (another limit, to fit max_bytes): the
            # predicate the run before it read with, and its notes. No
            # plan, no forms read, one read (cut from the rows kept).
            read_with, noted = settled
            report.utc_bounds[:] = noted.utc_bounds
            report.utc_stored[:] = noted.utc_stored
            report.unparseable.clear()
            report.unparseable.update(copy.deepcopy(noted.unparseable))
            report.iso_only, report.remedy = noted.iso_only, noted.remedy
            return read_with, run(read_with)

        def settle(read_with: Any, result: Any) -> tuple[Any, Any]:
            if call is not None:
                call.dated[settled_key] = (read_with, copy.deepcopy(report))
            return read_with, result

        budget.mark("before_plan")
        plan = _within_date_budget(
            conn, budget,
            lambda: _date_plan(
                conn, session, kind=kind, predicate=predicate, operator=operator,
                report=report,
            ),
            phase="plan",
        )
        budget.mark("plan")
        strict = _date_strict_predicate(predicate, plan, operator)
        if nomos_runtime:
            report.iso_only = _ISO_ONLY_ACCESS_RULES
            return settle(strict, run(strict))
        scope_key = _date_scope_key(
            conn, session, kind, plan[0], _without_date_clauses(predicate),
        )
        with _date_scopes_lock:
            known = _date_scopes.get(scope_key)
        if known == _DATE_SCOPE_TOO_LARGE:
            _date_scope_too_large(report, budget)
            return settle(strict, run(strict))
        if known is not None:
            # Nothing stored here changes a row: the counts it was found
            # with go into the reply as they did then.
            for name in known["utc_stored"]:
                report.note_stored(name)
            report.unparseable.update(copy.deepcopy(known["unparseable"]))
            return settle(strict, run(strict))

        def remember(what: Any) -> None:
            with _date_scopes_lock:
                if len(_date_scopes) >= _DATE_SCOPES_MAX:
                    _date_scopes.pop(next(iter(_date_scopes)))
                _date_scopes[scope_key] = what

        def soft(work: Callable[[], Any]) -> Any:
            """``work()`` where running out of the budget is not a refusal:
            ``_DATE_STATEMENT_CANCELLED`` instead, with the connection as it
            was before (a savepoint)."""
            budget.soft = True
            try:
                with conn.transaction():
                    return work()
            except DateCallOverBudget:
                return _DATE_STATEMENT_CANCELLED
            finally:
                budget.soft = False

        first = run(strict)
        found = _DateConditionReport()
        budget.mark("before_forms")
        forms = soft(lambda: _within_date_budget(
            conn, budget,
            lambda: _read_date_forms(
                conn, session, kind=kind, predicate=predicate, operator=operator,
                plan=plan, report=found, budget=budget,
            ),
            phase="forms_other",
        ))
        budget.mark("forms_other")
        if forms is _DATE_STATEMENT_CANCELLED or forms is _DATE_FORMS_TOO_LARGE:
            _date_scope_too_large(report, budget)
            # Only a read cut at its own share says the scope is too large;
            # one squeezed by a slow page or slow setup says nothing about
            # the scope, so later calls try again.
            if forms is _DATE_FORMS_TOO_LARGE:
                remember(_DATE_SCOPE_TOO_LARGE)
            return settle(strict, first)
        also_for, changes_rows = forms
        if not changes_rows:
            for name in found.utc_stored:
                report.note_stored(name)
            report.unparseable.update(found.unparseable)
            remember({
                "utc_stored": list(found.utc_stored),
                "unparseable": copy.deepcopy(found.unparseable),
            })
            return settle(strict, first)
        full = _rewrite_date_leaves(
            predicate, attrs_for=plan[0], also_for=also_for, operator=operator,
        )
        second = soft(lambda: run(full))
        budget.mark("second_read")
        if second is _DATE_STATEMENT_CANCELLED:
            # The forms were read; only this call ran out of time.
            _date_scope_too_large(report, budget)
            return settle(strict, first)
        for name in found.utc_stored:
            report.note_stored(name)
        report.unparseable.update(found.unparseable)
        return settle(full, second)

    def _lookup_exact_date_clause(
        conn: psycopg.Connection, session: Session, where: Any, where_context: Any,
    ) -> Any:
        """The top-level date conditions of a query-less search's `where`
        as an exact SQL clause (#3666), or None when there is none or it
        cannot be made exact; the lookup then narrows by the attribute's
        presence alone, as before, and its check on the read rows decides.

        Search and the set operators compare dates by one rule, so the
        clause is the one filter compiles: strict ISO-8601 stored values in
        SQL, every other stored form read once and matched by equality
        (``_read_date_forms``). Only with Nomos off: under access rules the
        lookup pages by readable rows and reads no stored value. What that
        read counted (values that are not dates, a value with no time zone)
        goes into the reply's ``where_unparseable`` and
        ``dates_read_as_utc``, since the rows it leaves out are no longer
        read."""
        predicate = _search_where.lookup_date_predicate(where, where_context)
        if predicate is None:
            return None
        found = _DateConditionReport()
        budget = _DateBudget(
            "search", _date_leaf_fields(predicate), total_ms=int(_DATE_CALL_BUDGET_MS),
            typed=_date_names_a_type(predicate, "nodes"),
        )
        try:
            plan = _date_plan(
                conn, session, kind="nodes", predicate=predicate, operator="search",
                report=_DateConditionReport(),
            )
            forms = _read_date_forms(
                conn, session, kind="nodes", predicate=predicate, operator="search",
                plan=plan, report=found, budget=budget,
            )
        except InvalidOperatorArguments:
            # No scoped type declares the date, or more stored forms than
            # are compared one by one: not made exact here.
            return None
        if forms is _DATE_STATEMENT_CANCELLED or forms is _DATE_FORMS_TOO_LARGE:
            return None
        for name in found.utc_stored:
            where_context.utc_stored.add(name)
        for name, counted in found.unparseable.items():
            where_context.unparseable_counted[name] = int(counted.get("count") or 0)
        return _rewrite_date_leaves(
            predicate, attrs_for=plan[0], also_for=forms[0], operator="search",
        )

    def _require_effective_view(
        conn: psycopg.Connection,
        session: Session,
        target_kind: str,
    ) -> None:
        """Refuse by name when the pinned generation has no effective-attribute
        relation for ``target_kind``. Never a fallback to another read path."""
        if _effective_view_ready(conn, session.generation_id, target_kind):
            return
        err = SubstrateError(
            "effective-attribute SQL snapshot is unavailable for the pinned generation",
            data={
                "generation_id": session.generation_id,
                "target_kind": target_kind,
            },
            fix_hint=(
                "publish/materialize the effective-attribute snapshot; broad "
                "Python fallback is intentionally disabled"
            ),
        )
        err.code = "unsupported_operator_form"
        raise err

    def _bounded_operator_count(
        conn: psycopg.Connection,
        *,
        from_where_sql: str,
        params: Mapping[str, Any],
        date_budget: Any = None,
    ) -> tuple[int | None, str]:
        """Count the rows of ``FROM ... WHERE ...``, bounded in rows and time.

        ``date_budget`` (#3666): the count of a call that carries a date
        condition takes at most what is left of that call's budget; with
        none left it is not run. Unknown either way, never a refusal: the
        page itself was served.

        ``from_where_sql`` is the page query's own FROM and WHERE, so the
        count is over the same compiled SQL as the page. Returns
        ``(n, "exact")``, ``(cap, "lower_bound")`` past the cap, or
        ``(None, "unknown")`` on a statement timeout -- never zero for a
        count that did not finish. The timeout is set for this statement only
        and restored; a cancelled count rolls back its own savepoint.
        """
        # This call's total can come out exact, a lower bound or unknown: the
        # byte fit sizes for all three (PACT mcp-byte-fit-count-stable).
        note_count_may_vary()
        cap = int(_OPERATOR_COUNT_CAP)
        sql = (
            f"SELECT count(*) FROM (SELECT 1 {from_where_sql} "
            f"LIMIT {cap + 1}) AS bounded_operator_count"
        )
        timeout_ms = int(_OPERATOR_COUNT_TIMEOUT_MS)
        # #3666: the total of a date call does not depend on the page's
        # limit, so a rerun to fit max_bytes takes the first run's outcome,
        # whatever it was.
        call = _current_date_call() if date_budget is not None else None
        total_key = _date_memo_key("total", from_where_sql, params)

        def counted_as(outcome: tuple[int | None, str]) -> tuple[int | None, str]:
            if call is not None:
                call.totals[total_key] = outcome
            return outcome

        if call is not None and total_key in call.totals:
            return call.totals[total_key]
        if date_budget is not None:
            remaining = date_budget.remaining_ms()
            if remaining < int(_DATE_STATEMENT_FLOOR_MS):
                return counted_as((None, "unknown"))
            timeout_ms = max(1, min(timeout_ms, remaining, int(_DATE_TOTAL_BUDGET_MS)))
            date_budget.statements.append(("total", timeout_ms))
            date_budget.mark("row_checks")
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SELECT current_setting('statement_timeout')")
                    previous = cur.fetchone()[0]
                    cur.execute(
                        "SELECT set_config('statement_timeout', %s, true)",
                        (f"{timeout_ms}ms",),
                    )
                    cur.execute(sql, dict(params))
                    counted = int(cur.fetchone()[0])
                    cur.execute(
                        "SELECT set_config('statement_timeout', %s, true)",
                        (previous,),
                    )
        except psycopg.errors.QueryCanceled:
            _mark_date_phase("total", date_budget)
            return counted_as((None, "unknown"))
        _mark_date_phase("total", date_budget)
        if counted > cap:
            return counted_as((cap, "lower_bound"))
        return counted_as((counted, "exact"))

    def _graph_edge_id(target_id: Any) -> Any:
        """An edge's graph id. The effective relation keys edges by the
        bigint ``edge_id`` as text; the graph views, Chronos and `expand`
        emit the integer. Anything else is returned as it is."""
        if isinstance(target_id, str) and target_id.lstrip("-").isdigit():
            return int(target_id)
        return target_id

    def _with_graph_edge_ids(kind: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Rows with integer ``edge_id`` for an edge scope, after every
        predicate check has run on the relation's own values."""
        if kind != "edges":
            return rows
        return [{**row, "edge_id": _graph_edge_id(row.get("edge_id"))} for row in rows]

    def _operator_count_fields(
        conn: psycopg.Connection,
        session: Session,
        *,
        offset: int,
        shown: int,
        page_meta: Mapping[str, Any],
        rest_only: bool = False,
    ) -> dict[str, Any]:
        """`total_count` and its basis for an effective-view operator page.

        ``rest_only``: the page's own FROM and WHERE select only the rows
        from this page on (a keyset page, #3666 step 3b), so the `offset`
        rows before it are added to what they count.

        Exact only when the read check dropped no row from the page and
        either the page read the whole rest of the set (no row past it) or a
        bounded COUNT over the page's own FROM and WHERE finished (#3201).
        When the reader's policy is evaluated (Nomos audit or enforce) or a
        row was dropped, a SQL count could include rows this reader cannot
        see, so the page-only rule of #3190 applies instead. A count that
        timed out is unknown, never zero.
        """
        truncated = bool(page_meta["raw_has_more"])
        dropped = int(page_meta["raw_page_rows"]) != int(shown)
        if dropped or deployment_requires_nomos_runtime(session.deployment_name):
            return _operator_page_count(offset=offset, shown=shown, truncated=truncated)
        if not truncated:
            return {
                "total_count": offset + shown,
                "total_count_basis": "exact",
                "retrieval_exhaustive": True,
            }
        counted, basis = _bounded_operator_count(
            conn,
            from_where_sql=page_meta["from_where_sql"],
            params=page_meta["params"],
            date_budget=page_meta.get("date_budget"),
        )
        if basis == "unknown" and page_meta.get("date_budget") is not None:
            # #3666: the total of a date call has a share of the call's
            # budget; past it the reply says why there is none. The page
            # itself is whole and says of itself what it always says.
            return {"total_count": None, "total_count_basis": "unknown",
                    "total_count_reason": TOTAL_NOT_COUNTED_IN_TIME}
        if basis == "unknown":
            return {"total_count": None, "total_count_basis": "unknown"}
        if rest_only and counted is not None:
            counted += offset
        fields: dict[str, Any] = {"total_count": counted, "total_count_basis": basis}
        if basis == "exact":
            fields["retrieval_exhaustive"] = True
        return fields

    def _named_date_order_refusal(
        conn: psycopg.Connection,
        session: Session,
        *,
        kind: str,
        input: Any,
        order_by: Any,
        nomos_runtime: bool,
        call: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """The refusal of `order_by` on a date field, or None (#3666, 2c).

        An order reads one stored attribute; a named date (`date.created`)
        maps to a different attribute per record type, and its values are
        compared as instants in several stored forms, so it is not ordered
        here. When the call names one type (or writes `date.attrs.<key>`),
        the refusal names the attribute that type stores the date in and
        offers the same call ordered by it: `corrected` when that attribute
        is declared, a `guess` when it is not (it then sorts as text), and
        none under the access rules for an undeclared one.
        """
        if not isinstance(order_by, str) or not order_by.strip():
            return None
        parts = order_by.strip().split()
        field = parts[0]
        if not field.startswith("date.") or len(parts) > 2:
            return None
        rest = field[len("date."):]
        attr: str | None = None
        subtype = input.get("subtype") if isinstance(input, Mapping) else None
        # A direction the order grammar takes; any other word gets no offer
        # (the offered call would be refused again).
        direction_ok = len(parts) == 1 or parts[1].lower() in ("asc", "desc")
        if rest.startswith("attrs.") and rest[len("attrs."):]:
            attr = rest[len("attrs."):]
        elif (
            rest in _DATE_MEANINGS and kind == "nodes" and isinstance(subtype, str) and subtype
            # Under the access rules the catalog's per-type mapping is not
            # read for a reader: the plain refusal, which names nothing.
            and not nomos_runtime
        ):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT metadata FROM okg.subtypes "
                    "WHERE catalog_version_id = %s AND name = %s",
                    (session.catalog_version_id, subtype),
                )
                row = cur.fetchone()
            meta = row[0] if row and isinstance(row[0], Mapping) else {}
            named = meta.get(_DATE_MEANINGS[rest])
            if isinstance(named, str) and named.strip():
                attr = named.strip()
        refusal = _operator_refusal(
            "filter",
            code="unsupported_operator_form",
            message=(
                f"filter: order_by {field} is a date; an order reads one stored "
                "attribute, written attrs.<key>"
            ),
        )
        if attr is None:
            refusal["error"]["fix_hint"] = (
                "order by the attrs.<key> the record type stores this date in; "
                "name one type in input.subtype to be told which"
            )
            return refusal
        declared = _declared_registry_for(conn).get(attr)
        target = f"attrs.{attr}" + (
            f" {parts[1].lower()}" if len(parts) == 2 and direction_ok else ""
        )
        where = f"on {subtype} " if attr is not None and not rest.startswith("attrs.") else ""
        refusal["error"]["fix_hint"] = (
            f"{field} {where}is stored as attrs.{attr}: order_by {target!r}"
            + ("" if declared else " (it has no declared type, so it sorts as text)")
            + ("" if direction_ok else "; a direction is asc or desc")
        )
        if direction_ok and (declared or not nomos_runtime):
            # Every argument the call sent, its pinned generation included.
            arguments, repeat = _offered_arguments(call)
            arguments["order_by"] = target
            refusal["error"]["retry_call"] = {
                "tool": "filter",
                "mode": "corrected" if declared else "guess",
                "arguments": arguments,
                **({"repeat": repeat} if repeat else {}),
            }
        return refusal

    def _declared_bound_refusal(
        predicate: Any, declared: Mapping[str, str], *, call: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """filter's refusal of a range bound of the wrong type for its
        attribute's declared type, before any read (issue #3666, 2c).

        The bound has no lowering, so the read would be refused anyway; here
        the refusal names the declared type and, when every such bound has
        one reading in that type (digits on a `num` attribute, a whole
        number on a `text` one), offers the call with it as a guess. None
        when no bound of `predicate` is of the wrong type.
        """
        from okg.substrate.mcp.operator_sql import declared_bound_mismatch

        mismatch = declared_bound_mismatch(predicate, declared)
        if mismatch is None:
            return None
        message, fix_hint, fixed = mismatch
        refusal = _operator_refusal(
            "filter", code="unsupported_operator_form", message=f"filter: {message}",
        )
        refusal["error"]["fix_hint"] = fix_hint
        if fixed is not None:
            arguments, repeat = _offered_arguments(call)
            arguments["predicate"] = fixed
            refusal["error"]["fix_hint"] = (
                f"{fix_hint}; if {_json.dumps(fixed, ensure_ascii=False)} is meant, "
                "write it so"
            )
            refusal["error"]["retry_call"] = {
                "tool": "filter", "mode": "guess", "arguments": arguments,
                **({"repeat": repeat} if repeat else {}),
            }
        return refusal

    def _offered_arguments(call: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
        """The arguments a refusal's offered call keeps from the call sent:
        every one it sent (its pinned generation and byte budget included),
        except `authority`, which is named in `repeat` and never echoed, as
        the grammar's offers do (issue #3666, 2c)."""
        arguments = {
            key: value for key, value in call.items()
            if value is not None and key != "authority"
            and not (key == "offset" and value == 0)
        }
        return arguments, (["authority"] if call.get("authority") is not None else [])

    def _compared_as_stored(
        predicate: Any, declared: Mapping[str, str] | Callable[[], Mapping[str, str]],
        *, empty: bool = False,
    ) -> dict[str, Any]:
        """`{"compared_as_stored": [...]}` when an `eq` or `in` on a declared
        attribute has an operand of another JSON type, else {} (issue #3666,
        2c). Such a test is served as written; the reply says it matches only
        a stored value of the operand's type. `declared` may be a callable,
        read only when the predicate has such a test."""
        from okg.substrate.mcp.operator_sql import (
            declared_operand_notes,
            text_contains_fields,
            text_contains_notes,
        )

        out: dict[str, Any] = {}
        if text_contains_fields(predicate):
            # #3666: what a text `contains` cannot match (a declared number
            # or true/false always; any attribute on an empty page), so an
            # empty page never reads as "no such text".
            text_notes = text_contains_notes(
                predicate, declared() if callable(declared) else declared, empty=empty,
            )
            if text_notes:
                out["contains_text"] = text_notes

        def has_attr_equality(node: Any) -> bool:
            if isinstance(node, Mapping):
                field = node.get("field")
                if node.get("op") in ("eq", "in") and isinstance(field, str) and (
                    field.startswith("attrs.")
                ):
                    return True
                if node.get("op") == "contains" and field == "attrs":
                    return True
                return any(has_attr_equality(child) for child in node.values())
            if isinstance(node, list):
                return any(has_attr_equality(child) for child in node)
            return False

        if not has_attr_equality(predicate):
            return out
        registry = declared() if callable(declared) else declared
        notes = declared_operand_notes(predicate, registry)
        return {**out, "compared_as_stored": notes} if notes else out

    def _effective_view_order_sql(
        *,
        kind: str,
        order_by: Any,
        params: dict[str, Any],
        declared: Mapping[str, str],
        relation: str | None = None,
        tiebreak: str | None = None,
        readable: Callable[[str], str] | None = None,
        row_guard: str | None = None,
        untyped_as_text: bool = False,
        notes: dict[str, Any] | None = None,
    ) -> str:
        """Return an exact deterministic SQL order or refuse the shape.

        ``untyped_as_text`` (issue #3666, step 2c; read-interface design
        2.5): an ``attrs.<key>`` with no declared type sorts as text, the
        JSON text of each present value (a string without its quotes) in
        code-point order (``COLLATE "C"``), values first, then rows with no
        value, each tie broken by the tiebreak. ``notes["sorted_as_text"]``
        is then set to the field, for the reply's note. Callers pass it only
        with the reader's access rules off; otherwise such a key is refused
        as before.

        ``tiebreak`` is the unique final sort key; the relation's own
        ``target_id`` by default. expand passes its edge id, so ties on the
        destination node's field keep the edge order they always had.

        An edge's ``target_id`` is its bigint graph ``edge_id`` as text, so
        edges sort by it as a number: the order the graph views, Chronos and
        `expand` always used (#3201), not the text order.

        ``readable`` (read-model slice 7a, as in ``effective_attr_lowerer``):
        an attribute order reads the value only where the reader is served
        it, so a masked or undecided value sorts as no value (after the valid
        ones, by the tiebreak), never by what is stored.

        ``row_guard`` (issue #3393, expand under the reader's view) is a SQL
        boolean over the ordered row: where it is not TRUE the row sorts as
        no row at all (every field, stable ones included, reads as absent),
        as expand serves an edge whose destination the reader cannot see.
        """
        prefix = f"{relation}." if relation else ""
        identity = (
            f"({prefix}target_id)::bigint" if kind == "edges" else f"{prefix}target_id"
        )
        last = tiebreak or f"{identity} ASC"
        if order_by in (None, ""):
            return f" ORDER BY {last}"
        parts = str(order_by).strip().split()
        if len(parts) > 2 or (
            len(parts) == 2 and parts[1].lower() not in {"asc", "desc"}
        ):
            raise InvalidOperatorArguments("order_by must be '<field>' or '<field> asc|desc'")
        field = parts[0]
        direction = "DESC" if len(parts) == 2 and parts[1].lower() == "desc" else "ASC"
        stable = _EFFECTIVE_VIEW_FIELD_COLUMNS[kind].get(field)
        if stable is not None:
            column = identity if stable == "target_id" else f"{prefix}{stable}"
            if row_guard is not None:
                column = f"(CASE WHEN {row_guard} THEN {column} END)"
            return f" ORDER BY {column} {direction} NULLS LAST, {last}"
        if not field.startswith("attrs.") or not field.split(".", 1)[1]:
            raise InvalidOperatorArguments("attribute order_by must name attrs.<declared-key>")
        key = field.split(".", 1)[1]
        value_type = declared.get(key)
        if value_type is None and not untyped_as_text:
            raise InvalidOperatorArguments(
                _order_by_not_sortable_message(key, kind=kind, declared=declared)
            )
        key_name = f"ord{len(params)}"
        params[key_name] = key
        json_expr = f"({prefix}attrs -> %({key_name})s)"
        if readable is not None:
            json_expr = f"(CASE WHEN {readable(key)} THEN {json_expr} END)"
        if row_guard is not None:
            json_expr = f"(CASE WHEN {row_guard} THEN {json_expr} END)"
        text_expr = f"({json_expr} #>> '{{}}')"
        if value_type is None:
            # No declared type: every present value sorts by its text.
            # An absent key reads NULL here, so it is not valid either.
            valid = f"jsonb_typeof({json_expr}) <> 'null'"
            value_expr = f'({text_expr} COLLATE "C")'
            if notes is not None:
                notes["sorted_as_text"] = field
        elif value_type == "num":
            valid = f"jsonb_typeof({json_expr}) = 'number'"
            value_expr = f"({text_expr}::numeric)"
        elif value_type == "ts":
            valid = (
                f"jsonb_typeof({json_expr}) = 'string' AND "
                f"pg_input_is_valid({text_expr}, 'timestamp with time zone')"
            )
            value_expr = f"({text_expr}::timestamptz)"
        elif value_type == "text":
            valid = f"jsonb_typeof({json_expr}) = 'string'"
            value_expr = text_expr
        elif value_type == "bool":
            valid = f"jsonb_typeof({json_expr}) = 'boolean'"
            value_expr = f"({text_expr}::boolean)"
        else:
            raise ValueError(f"unsupported declared order type {value_type!r}")
        return (
            f" ORDER BY CASE WHEN {valid} THEN 0 ELSE 1 END ASC, "
            f"CASE WHEN {valid} THEN {value_expr} ELSE NULL END "
            f"{direction} NULLS LAST, {last}"
        )

    def _path_prefix_candidates(
        conn: psycopg.Connection,
        generation_id: Any,
        prefixes: Sequence[str],
        params: dict[str, Any],
        *,
        declared: Mapping[str, str],
        notes: list[dict[str, Any]] | None,
    ) -> str:
        """The node-id candidate set of the lookup's `where` path prefixes
        (issue #3194), or "" when it may not be used.

        The set is read from the graph table, the page from the
        effective-attribute view, so it is a superset only while the two
        hold the same `path`. The expression-index readiness says so: `path`
        must be a declared filterable `text` attribute (so the drift checks
        compare it, ``declared_attr_indexes.run_drift_checks`` and
        ``verify_against_graph``) whose index is ready with no drift flag.
        The read must be at the branch head, the bounded path-prefix index
        must be on the registered node table, and a bounded probe must find
        at most ``PATH_PREFIX_CANDIDATE_BOUND`` candidates for a prefix.
        Each unmet condition adds ``not_covered`` for `path` with its reason
        (``not_declared``, a readiness reason, ``pinned_generation``,
        ``path_index_missing``, ``broad_prefix``) and narrows nothing.
        """
        from okg.substrate import declared_attr_indexes as dai
        from okg.substrate.mcp import grep_search as _grep

        attr = _search_where.PATH_INDEX_ATTR

        def refuse(reason: str) -> str:
            if notes is not None:
                notes.append(_attr_filter_source.not_covered_note([(attr, reason)]))
            return ""

        if declared.get(attr) != "text":
            return refuse("not_declared")
        bp = _attr_filter_source._head_branch_point(conn, generation_id)
        if bp is None:
            return refuse("pinned_generation")
        try:
            with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
                ready, physical_table = dai.reader_readiness(
                    cur, target_kind="node", declared={attr: "text"},
                )
        except psycopg.errors.InsufficientPrivilege:
            return refuse("index_missing")
        if ready.get(attr) is not None:
            return refuse(str(ready[attr]))
        if physical_table is None or not _grep.bounded_path_index_on(conn, physical_table):
            return refuse("path_index_missing")
        from psycopg import sql as pgsql

        segment = "public." + pgsql.Identifier(physical_table).as_string(conn).replace("%", "%%")
        bound = int(_grep.PATH_PREFIX_CANDIDATE_BOUND)
        narrow: list[str] = []
        for prefix in prefixes:
            probe_params: dict[str, Any] = {"pp_bp": bp}
            probe = _grep.path_prefix_probe_sql(
                prefix, probe_params, segment=segment, bp_ref="%(pp_bp)s",
                key="pq", bound=bound,
            )
            with conn.cursor() as cur:
                cur.execute(probe, probe_params, prepare=False)
                if int(cur.fetchone()[0]) <= bound:
                    narrow.append(prefix)
        if len(narrow) < len(prefixes):
            refuse("broad_prefix")
        if not narrow:
            return ""
        params["pp_bp"] = bp
        parts = [
            _grep.path_prefix_candidates_sql(
                prefix, params, segment=segment, bp_ref="%(pp_bp)s", key=f"pp{index}",
            )
            for index, prefix in enumerate(narrow)
        ]
        return " INTERSECT ".join(f"({part})" for part in parts)

    def _effective_view_sql_page(
        conn: psycopg.Connection, session: Session, **arguments: Any,
    ) -> tuple[list[dict[str, Any]], bool]:
        """``_effective_view_sql_page_within``; for a call that carries a
        date condition (``date_budget``), as one phase of its date budget
        (``_within_date_budget``). Any other call goes straight through."""
        budget = arguments.get("date_budget")
        if budget is None:
            return _effective_view_sql_page_within(conn, session, **arguments)
        if _date_rerun_spent(budget):
            try:
                return _effective_view_sql_page_within(conn, session, **arguments)
            finally:
                budget.resume()
                budget.mark("page_other")
        budget.mark("before_page")
        try:
            return _within_date_budget(
                conn, budget,
                lambda: _effective_view_sql_page_within(conn, session, **arguments),
                phase="page_other",
            )
        finally:
            budget.resume()
            budget.mark("page_other")

    def _effective_view_sql_page_within(
        conn: psycopg.Connection,
        session: Session,
        *,
        kind: str,
        predicate: Any,
        limit: int,
        offset: int,
        order_by: Any = None,
        pushdown_notes: list[dict[str, Any]] | None = None,
        page_meta: dict[str, Any] | None = None,
        nomos_runtime: bool | None = None,
        residual: Callable[[list[dict[str, Any]]], list[dict[str, Any]]] | None = None,
        page_facts: dict[str, Any] | None = None,
        member_ids: Sequence[str] | None = None,
        path_prefixes: Sequence[str] = (),
        fingerprint_predicate: Any = ...,
        date_budget: Any = None,
        member_sql: Callable[[dict[str, Any]], str] | None = None,
        after_id: str | None = None,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Select and page in Postgres, then apply Nomos to that page only.

        Under Nomos `off` this is one SQL page: `limit + 1` stored rows at
        `offset`, and the bool says a stored row lies past the page.

        Under `audit`/`enforce` the SQL pages the READER'S VIEW (read-model
        slice 7a; PACT read-model-slice7a-predicates): it keeps only rows
        the view's predicate keeps (``ReaderView.predicate``), and every
        attribute clause and an attribute ``order_by`` read a value only
        where the reader is served it (``ReaderView.readable``), so a clause
        on a hidden or masked value never matches and a stored value never
        decides which rows are scanned. A row whose readability the view
        cannot decide (no current class) is kept whatever its value and the
        read-row check decides it. The value-based candidate narrowing
        (expression and attr_index indexes) is not used there: it would admit
        rows by their stored value. With no active bundle under audit
        (``ReaderView.serves_unredacted``) nothing is masked, so the clauses
        and the narrowing run on this page's predicate as with Nomos off;
        the read-row check still drops rows without source lineage, which
        may only decide whether they count toward the scan bound. (For the
        attribute lookup the predicate itself is built more conservatively
        under any Nomos mode: ``lookup_sql_clauses(masking=True)`` pushes no
        ``id``, title, path or date conjunct.) ``check_bundle_unchanged``
        runs after the final read filter and refuses the read when the
        active bundle differs from the one the view opened with. ``offset`` and the returned bool count
        only rows that passed the read filter and then ``residual`` (the
        caller's own post-policy check on the read row, three-valued, which
        must be idempotent: callers still run it on the page). Stored rows
        are read from the first match in bounded batches until ``offset +
        limit + 1`` such rows are known, the stored rows end, or
        ``READ_FILTER_SCAN_CAP`` rows were read (issue #3208). At that bound
        the bool is true and ``page_facts["read_filter_scan_bound"]`` names
        it: the caller must not offer a cursor. ``READER_VIEW_FORCE`` is the
        view's ``force``.

        ``member_ids`` (#3222) restricts the read to a result reference's
        stored node ids: an id restriction on the relation, never a value
        test, that also narrows the base rows before the overlay join, so
        the work is bounded by the member count. ``member_sql`` (issue #3666)
        is the same restriction with the id set left in SQL: given this
        page's parameter dict it binds its own and returns a one-column
        query of node ids, so a set of any size (the far ends of a hub's
        links) is never materialised. It is an id restriction only; a caller
        passes it only with access rules off, where no row of the set is
        decided per reader.

        ``pushdown_notes`` receives a ``{"pushdown": "not_covered", ...}``
        note when the narrowing was withheld for an attribute the predicate
        reads (issue #3029; the expression index's readiness, okg#3112).
        ``nomos_runtime`` is the caller's one reading of the deployment's
        Nomos mode (true for `audit`/`enforce`) when the caller also shaped
        ``predicate`` from it; the read filter and the overlay step then use
        that same value. None resolves the mode here, once.
        ``page_meta`` receives what a caller needs to count the set (#3201):
        the predicate fingerprint, the page's own FROM/WHERE and parameters
        for a bounded COUNT, the rows the page read before the read check,
        and whether a row lay past the page.
        ``path_prefixes`` (issue #3194, the lookup's `where` path prefix):
        each narrows the candidate set to the node ids the path-prefix index
        admits (``grep_search.path_prefix_candidates_sql``, a superset). Used
        only when the caller read Nomos as off (``nomos_runtime is False``),
        ``OKG_ATTR_FILTER_INDEX`` is not ``off``, and every guard of
        ``_path_prefix_candidates`` holds (declared and ready `path`, branch
        head, bounded index, bounded probe); else nothing narrows and
        ``pushdown_notes`` says why. The exact predicate still decides every
        row.

        ``after_id`` (#3666 step 3b, keyset paging past the offset cap):
        only rows whose id sorts after it, in the default id order; the
        caller passes ``offset=0`` and no ``order_by`` with it.

        ``date_budget`` (#3666, ``_date_budget``): given for a call that
        carries a date condition. Each page statement then runs under what
        is left of it, the bounded total too (``page_meta["date_budget"]``),
        and a statement the server cancels raises the budget's refusal. The
        date leaves that must hold are offered to the candidate planner as
        ranges on their declared ``ts`` attribute
        (``date_conditions.narrowing_predicate``), so a ready index narrows
        the page as it does for ``attrs.<key> between``. The index helps a
        narrow range. A range most records of the scope fall in gets no
        help from it: the candidate set is then most of the scope, the page
        is served from what amounts to a scan, and its total, which reads
        every match, may not finish in its share of the budget. A call
        without a date condition passes None: no timeout, and the planner
        reads its predicate as before.
        """
        target_kind = "node" if kind == "nodes" else "edge"
        if date_budget is not None:
            # #3666: until the page statement, this is not date work.
            date_budget.pause(_DATE_SETUP_UNCHARGED_MS)
        _require_effective_view(conn, session, target_kind)
        declared = _declared_registry_for(conn)
        params: dict[str, Any] = {}
        base_table, source_clause, gen_where, gen_params = _effective_view_source(
            conn,
            session.generation_id,
            target_kind=target_kind,
        )
        _mark_date_phase("setup_view", date_budget)
        relation = "effective_page"
        # One reading of the mode decides the reader view, the read filter and
        # the overlay step below, so they cannot disagree within a call.
        if nomos_runtime is None:
            nomos_runtime = deployment_requires_nomos_runtime(session.deployment_name)
        view = None
        reader_join = ""
        readable_sql: Callable[[str], str] | None = None
        if nomos_runtime:
            view, reader_join, readable_sql = _reader_view_sql(
                conn, session, target_kind=target_kind, relation=relation, params=params,
            )
            if view.serves_unredacted:
                # No active bundle under audit: nothing is masked and every
                # served row carries its effective attrs, so the clauses, the
                # order and the WHERE are the Nomos-off SQL (the view's
                # predicate is TRUE). The read-row check below still drops
                # rows without source lineage.
                readable_sql = None
        # Post-publish accepted overlays can refresh the effective snapshot
        # without changing the graph table the expression indexes cover. A
        # candidate set is therefore safe only for targets whose predicate
        # keys do not have an overlay row at this pin; overlay-touched targets
        # are admitted by the exact effective-attrs predicate alone.
        reserved_overlay_keys = sorted(RESERVED_EFFECTIVE_ATTR_KEYS)

        def _predicate_attr_keys(value: Any) -> set[str]:
            if isinstance(value, dict):
                keys: set[str] = set()
                field = value.get("field")
                if isinstance(field, str) and field.startswith("attrs."):
                    keys.add(field.split(".", 1)[1])
                if value.get("op") == "contains" and value.get("field") == "attrs":
                    contained = value.get("value")
                    if isinstance(contained, dict):
                        keys.update(str(key) for key in contained)
                for child in value.values():
                    keys.update(_predicate_attr_keys(child))
                return keys
            if isinstance(value, list):
                keys: set[str] = set()
                for child in value:
                    keys.update(_predicate_attr_keys(child))
                return keys
            return set()

        path_candidates_sql = ""
        if (
            path_prefixes
            and kind == "nodes"
            and nomos_runtime is False
            # `off` is the switch that turns every candidate set off.
            and _attr_filter_source.filter_index_mode() != "off"
        ):
            path_candidates_sql = _path_prefix_candidates(
                conn, session.generation_id, path_prefixes, params,
                declared=declared, notes=pushdown_notes,
            )
        # A key holding NUL names no stored attribute (the compiler decides it
        # without the database), and NUL never reaches a parameter.
        predicate_attr_keys = sorted(
            key
            for key in (
                (_predicate_attr_keys(predicate)
                 | ({_search_where.PATH_INDEX_ATTR} if path_candidates_sql else set()))
                - set(reserved_overlay_keys)
            )
            if "\x00" not in key
        )
        compiled = _compile_operator_predicate(
            kind,
            predicate,
            params,
            declared=declared,
            relation=relation,
            readable=readable_sql,
            fingerprint_of=fingerprint_predicate,
        )
        if compiled is None:
            from okg.substrate.mcp.operator_sql import declared_bound_mismatch

            # A range bound that does not fit its attribute's declared type
            # is named, with the form that works (issue #3666, 2c).
            mismatch = declared_bound_mismatch(predicate, declared)
            err = SubstrateError(
                mismatch[0] if mismatch else
                "attribute predicate cannot be lowered exactly to SQL",
                data={"kind": kind, "predicate": (
                    predicate if fingerprint_predicate is ... else fingerprint_predicate
                )},
                fix_hint=mismatch[1] if mismatch else (
                    "use a supported predicate shape or declare the ordered "
                    "attribute; broad Python fallback is intentionally disabled"
                ),
            )
            err.code = "unsupported_operator_form"
            raise err
        predicate_sql = compiled.bind_into(params)
        params.update(
            {
                "tk": target_kind,
                "page_limit": max(1, int(limit)) + 1,
                "page_offset": max(0, int(offset)),
            }
        )
        _mark_date_phase("setup_compile", date_budget)
        # okg#3112: OKG_ATTR_FILTER_INDEX picks the candidate source
        # (mcp/attr_filter_source.py); the exact predicate decides every row.
        # Read (and a removed value refused) only when the predicate names an
        # attribute, reserved keys included: a read without one has nothing
        # to narrow.
        filter_mode = (
            _attr_filter_source.filter_index_mode()
            if _predicate_attr_keys(predicate) else "off"
        )
        if nomos_runtime and not (view is not None and view.serves_unredacted):
            # Slice 7a: a candidate set admits rows by their STORED value, so
            # under the reader's view it would let a hidden or masked value
            # (on a row the view cannot place) decide which rows are scanned.
            # With no active bundle under audit nothing is masked and only
            # rows without source lineage are hidden (disclosed limitation
            # case 1), so narrowing stays on as with Nomos off.
            filter_mode = "off"
        expression_plan = None
        # design 2.2 rule 2: a ts bound v4 refuses narrows nothing. Only that
        # attribute's clauses leave the candidate set (dropping a conjunct
        # only widens it); the exact predicate still decides them.
        # The accelerator binds the predicate's operands itself; one that
        # holds NUL narrows nothing (it matches no stored value), and the exact
        # compiled predicate alone decides the page.
        date_keys = _date_window_keys(predicate)
        # #3666: what the planner reads. With a date leaf, the predicate
        # with that leaf as a range on its declared `ts` attribute.
        planner_predicate = (
            _date_narrowing_predicate(predicate, declared) if date_keys else predicate
        )
        narrow_predicate = None if _holds_nul(predicate) else planner_predicate
        if filter_mode != "off" and narrow_predicate is not None:
            refused_ts = _attr_filter_source.ts_refused_attrs(
                conn, _attr_filter_source.pushable_clauses(planner_predicate, declared)
            )
            if refused_ts:
                narrow_predicate = _attr_filter_source.narrowable_predicate(
                    planner_predicate, refused_ts
                )
                if pushdown_notes is not None:
                    pushdown_notes.append(_attr_filter_source.not_covered_note(
                        [(attr, "bound_not_strict_iso") for attr in refused_ts]
                    ))
        _mark_date_phase("setup_ts_bounds", date_budget)
        # okg#3112 step 4a: the ready node expression indexes narrow; every
        # other declared attribute (an edge, an index not ready, a pinned
        # read) is answered by the exact predicate alone and named.
        if filter_mode == "expression" and narrow_predicate is not None:
            expression_plan, uncovered = _attr_filter_source.plan_expression_candidates(
                conn,
                generation_id=session.generation_id,
                predicate=narrow_predicate,
                declared=declared,
                target_kind=target_kind,
                refused=(),
            )
            # Ruling 10-04: a prefix clause on a declared key narrows by no
            # index; say so.
            uncovered = uncovered + [
                (attr, "prefix_not_indexed")
                for attr in _attr_filter_source.prefix_attrs(narrow_predicate, declared)
                if attr not in {a for a, _r in uncovered}
                # The path-prefix index narrows a `path` prefix (issue #3194;
                # the lookup sends the clause too since #3666).
                and not (path_candidates_sql and attr == _search_where.PATH_INDEX_ATTR)
            ]
            if uncovered and pushdown_notes is not None:
                pushdown_notes.append(_attr_filter_source.not_covered_note(uncovered))
            if date_keys:
                _note_date_index(
                    date_budget, predicate, declared=declared,
                    narrowed=expression_plan.attrs if expression_plan is not None else (),
                    uncovered=uncovered,
                )
        elif date_keys:
            _note_date_index(
                date_budget, predicate, declared=declared, narrowed=(), uncovered=(),
                unused=(
                    _DATE_INDEX_ACCESS_RULES
                    if nomos_runtime and not (view is not None and view.serves_unredacted)
                    else _DATE_INDEX_NOT_NARROWABLE
                ),
            )
        _mark_date_phase("setup_index_plan", date_budget)
        # The candidate set DRIVES the page (lane-h-mcp-filter-index-driven):
        # a target passes when every narrowed conjunctive clause admits it, OR
        # it has an overlay row on any predicate key at this generation
        # (overlays land after publish and the graph-table index does not see
        # them). Written as a target-id set on the BASE relation, Postgres
        # reads the index first and runs the overlay lateral only for those
        # candidates. The exact predicate below still decides every survivor,
        # and Nomos still sees the page. Shapes no index narrows render no
        # candidates and keep the unnarrowed query.
        if expression_plan is not None:
            params.update(expression_plan.params)

        def _with_overlay(candidates_sql: str) -> str:
            if not candidates_sql or not predicate_attr_keys:
                return candidates_sql
            params["overlay_attr_keys"] = predicate_attr_keys
            return (
                f"({candidates_sql}) UNION ("
                f"SELECT overlay_target.target_id "
                f"FROM {ATTRIBUTE_EFFECTIVE_TABLE} AS overlay_target "
                "WHERE overlay_target.target_kind = %(tk)s "
                "AND overlay_target.generation_id = %(overlay_gen)s "
                "AND overlay_target.namespace = 'default' "
                "AND overlay_target.attr_key = ANY(%(overlay_attr_keys)s::text[]))"
            )

        def _from(candidates_sql: str, *, as_array: bool = False) -> str:
            narrowed = _with_overlay(candidates_sql)
            if members_sql is not None:
                narrowed = (
                    f"({members_sql}) INTERSECT ({narrowed})" if narrowed else members_sql
                )
            return _effective_view_runtime_source(
                base_table=base_table,
                source_clause=source_clause,
                gen_where=gen_where,
                gen_params=gen_params,
                target_kind=target_kind,
                generation_id=session.generation_id,
                relation=relation,
                params=params,
                base_candidates_sql=narrowed,
                base_candidates_in_id_order=order_by in (None, ""),
                base_candidates_as_array=as_array,
            )

        members_sql: str | None = None
        if member_ids is not None:
            params["rr_member_ids"] = list(member_ids)
            members_sql = "SELECT unnest(%(rr_member_ids)s::text[])"
        elif member_sql is not None:
            members_sql = member_sql(params)

        candidates_sql = expression_plan.sql if expression_plan is not None else ""
        if path_candidates_sql:
            # Overlay-touched targets rejoin through `_with_overlay`, whose
            # keys include `path` (an accepted overlay may set the path).
            candidates_sql = (
                f"({candidates_sql}) INTERSECT ({path_candidates_sql})"
                if candidates_sql else path_candidates_sql
            )
        # The page looks the ids up one by one only while the whole set is
        # bounded: the path-prefix set (at most PATH_PREFIX_CANDIDATE_BOUND
        # after the probe) plus targets with a `path` overlay. Any other key
        # the predicate reads unites every overlay target for that key, which
        # nothing bounds, so that page keeps `IN`.
        def _from_candidates(driving_sql: str) -> str:
            return _from(
                driving_sql,
                as_array=_search_where.path_candidates_as_array(
                    bool(path_candidates_sql), predicate_attr_keys,
                ),
            )

        from_clause = _from_candidates(candidates_sql)
        # Order the complete effective relation in Postgres: an index holds
        # only type-admitted values, so ordering through one as an INNER JOIN
        # would silently drop rows whose ordered attribute is missing or
        # malformed. Predicate narrowing uses the candidate set above.
        order_sql = _effective_view_order_sql(
            kind=kind,
            order_by=order_by,
            params=params,
            declared=declared,
            relation=relation,
            readable=readable_sql,
            # #3666 (2c): an undeclared attribute sorts as text, with the
            # access rules off only; `page_meta` carries the reply's note.
            untyped_as_text=not nomos_runtime,
            notes=page_meta,
        )
        where_sql = f" AND ({predicate_sql})"
        if view is not None and readable_sql is not None:
            where_sql = _reader_view_where(
                view_predicate=view.predicate(_READER_ROW_ALIAS if reader_join else relation)
                .as_string(conn).replace("%", "%%"),
                predicate=predicate,
                predicate_sql=predicate_sql,
                readable=readable_sql,
            )
        # Issue #3408: `target_kind` is a literal ('node' or 'edge', from
        # code), so even a generic plan can prove the intervals table's
        # partial indexes (WHERE target_kind = 'edge') for this page.
        kind_literal = _target_kind_literal(target_kind)
        if member_ids is not None:
            where_sql += f" AND {relation}.target_id = ANY(%(rr_member_ids)s::text[])"
        # A `member_sql` set already restricts the base rows in `from_clause`
        # (`target_id IN (...)`), which the count below reads too.
        if after_id is not None:
            if order_by not in (None, ""):
                raise InvalidOperatorArguments(
                    "keyset paging reads the default id order only"
                )
            # The same key the default order sorts by: an edge's id as a
            # number, a node's as text.
            params["page_after"] = str(after_id)
            where_sql += (
                f" AND ({relation}.target_id)::bigint > %(page_after)s::bigint"
                if kind == "edges"
                else f" AND {relation}.target_id > %(page_after)s"
            )

        def _statement(source_sql: str) -> tuple[str, str]:
            """``(FROM ... WHERE ..., the page statement)`` over one source.
            Whatever drives it, the rows are the exact condition's, in the
            page's one order, cut by LIMIT and OFFSET."""
            scope_sql = (
                f"FROM {source_sql}{reader_join} "
                f"WHERE {relation}.target_kind = {kind_literal} "
                f"AND {relation}.namespace = 'default'"
                + where_sql
            )
            return scope_sql, (
                f"SELECT {relation}.target_id, {relation}.subtype, "
                f"{relation}.src, {relation}.dst, {relation}.edge_type, "
                f"{relation}.attrs, {relation}.raw_attrs, "
                f"{relation}.raw_text_hash "
                + scope_sql
                + order_sql
                + " LIMIT %(page_limit)s OFFSET %(page_offset)s"
            )

        from_where_sql, sql = _statement(from_clause)
        #: What the page was read from, for the total over the same rows.
        served = {"from_where_sql": from_where_sql}
        id_field = "node_id" if kind == "nodes" else "edge_id"

        # #3666: a date range's index candidates drive the page only while
        # they are few. When the range admits most records, building the
        # candidate set (the index range, united with the overlay targets)
        # reads all of them before the first row is returned, whatever the
        # page size. The page then walks its own order (the id) and
        # filters, which stops at the page size; the rows are the same.
        # Only where that order is the page's (no order_by), with Nomos
        # off, over a whole set. If the walk does not finish in its share
        # (matches that all sort late), the candidates read the page, and
        # the scope is remembered as one the walk does not serve.
        walk: tuple[str, str] | None = None
        walk_key: tuple[Any, ...] | None = None
        if (
            date_budget is not None
            and not nomos_runtime
            and expression_plan is not None
            and order_by in (None, "")
            and member_ids is None
            and member_sql is None
            and set(date_keys) & set(expression_plan.attrs)
        ):
            walk = _statement(_from_candidates(path_candidates_sql))
            # The setup is over; the probe below is the call's date work.
            date_budget.resume()
            date_budget.mark("page_setup")
            info = conn.info
            walk_key = (
                "walk", info.host, info.port, info.dbname, str(session.generation_id),
            ) + _date_memo_key("page", walk[1], params)[1:3]
            with _date_scopes_lock:
                walk_is_slow = _date_scopes.get(walk_key) is not None
            if walk_is_slow or not _date_candidates_are_wide(
                conn, date_budget, expression_plan.sql, params,
            ):
                walk = None

        def _fetch(
            statement: str | None = None, *, cap_ms: int | None = None, soft: bool = False,
        ) -> Any:
            statement = sql if statement is None else statement
            if date_budget is None:
                return _fetch_rows(statement)
            # A rerun of the call to fit max_bytes asks for fewer rows of
            # the same statement: they are the first of the rows kept.
            call = _current_date_call() if not nomos_runtime else None
            page_key = _date_memo_key("page", statement, params)
            wanted = int(params["page_limit"])
            if call is not None:
                kept = call.pages.get(page_key)
                if kept is not None and (kept[0] >= wanted or len(kept[1]) < kept[0]):
                    date_budget.resume()
                    date_budget.mark("page_setup")
                    return copy.deepcopy(kept[1][:wanted])
            date_budget.resume()
            date_budget.mark("page_setup")
            try:
                fetched = _under_date_budget(
                    conn, date_budget, "page", lambda: _fetch_rows(statement),
                    phase="page_statement", cap_ms=cap_ms, soft=soft,
                )
            finally:
                date_budget.mark("page_statement")
            if call is not None and fetched is not _DATE_STATEMENT_CANCELLED:
                call.pages[page_key] = (wanted, copy.deepcopy(fetched))
            return fetched

        def _fetch_rows(statement: str) -> list[dict[str, Any]]:
            raw_rows: list[dict[str, Any]] = []
            with conn.cursor(row_factory=dict_row) as cur:
                # A path-prefix LIKE reaches the index only in a plan made with
                # its pattern; a generic plan of a prepared statement cannot.
                cur.execute(statement, params, prepare=False if path_candidates_sql else None)
                for record in cur:
                    effective = record["attrs"]
                    stored_raw = record["raw_attrs"]
                    if effective is not None and not isinstance(effective, dict):
                        raise SubstrateError(
                            "effective-attrs row has a non-object attrs value",
                            data={
                                "target_id": record["target_id"],
                                "attrs_type": type(effective).__name__,
                            },
                        )
                    if stored_raw is not None and not isinstance(stored_raw, dict):
                        raise SubstrateError(
                            "effective-attrs row has a non-object raw_attrs value",
                            data={
                                "target_id": record["target_id"],
                                "raw_attrs_type": type(stored_raw).__name__,
                            },
                        )
                    row = {
                        id_field: record["target_id"],
                        "attrs": (effective or {} if stored_raw is None else stored_raw),
                        "_effective_attrs": effective or {},
                    }
                    if kind == "nodes":
                        row["subtype"] = record["subtype"]
                        row["text_hash"] = record["raw_text_hash"]
                    else:
                        row["src"] = record["src"]
                        row["dst"] = record["dst"]
                        row["edge_type"] = record["edge_type"]
                    raw_rows.append(row)
            return raw_rows


        def _read_filter(
            raw_rows: list[dict[str, Any]], *, count: bool = False,
        ) -> list[dict[str, Any]]:
            if kind == "nodes":
                survivors = filter_node_rows_for_session(
                    conn, session, raw_rows, runtime_required=nomos_runtime,
                    count=count,
                )
            else:
                survivors = filter_edge_rows_for_session(
                    conn, session, raw_rows, runtime_required=nomos_runtime,
                    count=count,
                )
            if not nomos_runtime:
                for row in survivors:
                    row["attrs"] = row.pop("_effective_attrs", row.get("attrs") or {})
                return survivors
            for row in survivors:
                row.pop("_effective_attrs", None)
            return apply_effective_attrs_to_rows(
                conn,
                survivors,
                target_kind=target_kind,
                target_id_field=id_field,
                generation_id=session.generation_id,
            )

        def _record_page_meta(
            *, raw_page_rows: int, raw_has_more: bool,
            last_id: str | None = None, consumed: int = 0,
        ) -> None:
            if page_meta is not None:
                page_meta.update(
                    {
                        "predicate_fingerprint": compiled.fingerprint,
                        "from_where_sql": served["from_where_sql"],
                        "params": dict(params),
                        "raw_page_rows": raw_page_rows,
                        "raw_has_more": raw_has_more,
                        "date_budget": date_budget,
                        # Where a keyset token resumes (#3666 step 3b): the
                        # last row this page consumed, and how many it
                        # consumed in `offset`'s unit.
                        "last_id": last_id,
                        "consumed": consumed,
                    }
                )

        if not nomos_runtime:
            raw_rows = _DATE_STATEMENT_CANCELLED
            if walk is not None:
                raw_rows = _fetch(walk[1], cap_ms=_DATE_WALK_SHARE_MS, soft=True)
                if raw_rows is _DATE_STATEMENT_CANCELLED:
                    if date_budget.remaining_ms() >= int(_DATE_STATEMENT_FLOOR_MS):
                        # It ran and was cut: not a walk this scope serves.
                        with _date_scopes_lock:
                            if len(_date_scopes) >= _DATE_SCOPES_MAX:
                                _date_scopes.pop(next(iter(_date_scopes)))
                            _date_scopes[walk_key] = "walk_slow"
                else:
                    served["from_where_sql"] = walk[0]
            if raw_rows is _DATE_STATEMENT_CANCELLED:
                raw_rows = _fetch()
            raw_has_more = len(raw_rows) > max(1, int(limit))
            raw_rows = raw_rows[: max(1, int(limit))]
            _record_page_meta(
                raw_page_rows=len(raw_rows), raw_has_more=raw_has_more,
                last_id=str(raw_rows[-1][id_field]) if raw_rows else None,
                consumed=len(raw_rows),
            )
            return _read_filter(raw_rows), raw_has_more
        # Nomos `audit`/`enforce`: page over readable rows (see docstring).
        page_limit = max(1, int(limit))
        page_offset = max(0, int(offset))
        need = page_offset + page_limit + 1
        scan_cap = READ_FILTER_SCAN_CAP
        readable: list[dict[str, Any]] = []
        # Rows scanned only to decide the page facts are COUNTED (count-purpose
        # decisions); only the rows returned are READ (M2). The count pass
        # runs on copies so the read pass sees the stored rows untouched.
        raw_by_id: dict[str, dict[str, Any]] = {}
        raw_offset, raw_more, batch = 0, True, need
        bound_reached = False
        #: The last stored row scanned: where a keyset token resumes when the
        #: scan bound stops a page before it finds a row past it (#3666 3b).
        last_scanned: str | None = None
        while raw_more and len(readable) < need:
            if date_budget is not None:
                # Between batches, the read filter runs in Python.
                date_budget.check()
            if raw_offset >= scan_cap:
                bound_reached = True
                break
            size = min(batch, READ_FILTER_MAX_BATCH, scan_cap - raw_offset)
            params["page_limit"] = size + 1
            params["page_offset"] = raw_offset
            raw_rows = _fetch()
            raw_more = len(raw_rows) > size
            for row in raw_rows[:size]:
                raw_by_id[str(row[id_field])] = row
            if raw_rows[:size]:
                last_scanned = str(raw_rows[:size][-1][id_field])
            chunk = _read_filter(copy.deepcopy(raw_rows[:size]), count=True)
            if residual is not None:
                chunk = residual(chunk)
            readable.extend(chunk)
            raw_offset += size
            batch *= 2
        if bound_reached and page_facts is not None:
            page_facts["read_filter_scan_bound"] = scan_cap
            # How many stored rows the page read (issue #3666, not a total).
            page_facts["rows_scanned"] = raw_offset
        has_more = bound_reached or len(readable) > page_offset + page_limit
        page_ids = [
            str(row[id_field]) for row in readable[page_offset: page_offset + page_limit]
        ]
        returned = _read_filter([copy.deepcopy(raw_by_id[row_id]) for row_id in page_ids])
        if view is not None and view.force == "shadow" and page_ids:
            # Shadow: the per-row filter next to the class path on the page's
            # stored rows; any difference (rows, class decisions, readable()
            # against the masks) raises ReaderViewShadowMismatch.
            view.finish([
                {key: value for key, value in raw_by_id[row_id].items()
                 if key != "_effective_attrs"}
                for row_id in page_ids
            ])
        if view is not None:
            # After the last read: the final read filter reloads the bundle,
            # so a bundle activated (no-bundle mode) or changed (class mode)
            # since the view opened would serve rows the SQL matched and
            # ordered on values that bundle now masks. Refuse instead.
            view.check_bundle_unchanged()
        if residual is not None:
            returned = residual(returned)
        # Under audit/enforce the page facts are over readable rows, and
        # `_operator_count_fields` keeps the page-only count rule (#3201).
        _record_page_meta(
            raw_page_rows=len(returned), raw_has_more=has_more,
            last_id=page_ids[-1] if page_ids else None, consumed=len(page_ids),
        )
        if (
            page_meta is not None and bound_reached and last_scanned is not None
            and page_offset <= len(readable) <= page_offset + page_limit
        ):
            # Every readable row the scan found is on this page, and the scan
            # stopped at its bound: the rest of the set lies after the last
            # stored row scanned, and only a keyset read can resume there.
            # When the scan found fewer readable rows than `offset`, it never
            # reached this page's position: a token there would serve later
            # rows as if at that position, so paging stops instead.
            page_meta["scan_resume_id"] = last_scanned
        return returned, has_more

    def _operator_attr_rows(
        conn: psycopg.Connection,
        session: Session,
        *,
        kind: str,
        limit: int,
        scan_predicate: Any = None,
    ) -> tuple[list[dict[str, Any]], bool]:
        # Legacy/test-only compatibility seam. Production attribute filter,
        # map, and search calls route through `_effective_view_sql_page` and
        # never invoke this broad-row helper.
        #
        # `scan_predicate` is the SQL-pushdown filter: it carries the input's
        # subtype/edge_type and any predicate clauses that reference ONLY stable
        # graph fields (never attrs). Pushing it into the chronos scan WHERE means
        # the effective-attrs overlay (apply_effective_attrs_to_rows) and the
        # bounded-Python cap apply to O(matched rows), not a fixed graph-wide 10K
        # window -- so a selective input on a >10K-node graph no longer trips the
        # cap merely because the graph is large. Parity: subtype/edge_type are
        # stable columns the overlay cannot mutate, and only attr-free predicate
        # clauses are pushed, so the SQL WHERE selects exactly the rows the prior
        # Python post-filter kept (the callers still re-run the FULL predicate over
        # the effective attrs). attrs-referencing predicates are NOT pushed and
        # remain the residual Python post-filter, leaving semantics unchanged.
        #
        # scale-operator-attr-reads / req.effective-attrs-view (Layer 2): when the
        # indexed effective-attrs snapshot exists for this generation, read the
        # rows straight from SQL (index scan), with NO 10K cap. Returns
        # ``(rows, truncated)`` where ``truncated`` reflects the real result-size
        # ceiling -- a TRUE signal, never a silent undercount. The Layer-1
        # chronos-scan + apply_effective_attrs_to_rows path below is the verbatim
        # fallback for a pre-table / unmaterialized generation; it caps LOUDLY
        # (raises) and so returns ``(rows, False)``.
        target_kind = "node" if kind == "nodes" else "edge"
        if _effective_view_ready(conn, session.generation_id, target_kind):
            # `limit` is NOT forwarded: the Layer-2 scan cannot honour a caller
            # bound soundly (its WHERE is a superset the caller re-filters in
            # Python -- see the NO CALLER LIMIT note on _effective_view_fast_rows).
            # It stays a Layer-1 correctness cap only.
            return _effective_view_fast_rows(
                conn,
                session,
                kind=kind,
                scan_predicate=scan_predicate,
            )
        scan_limit = max(1, int(limit))
        # Layer-1 scans RAW graph attrs, where an attrs-referencing clause
        # (e.g. the typed lookup's `contains` over effective attrs) could drop
        # rows whose match comes from an accepted overlay. Keep ONLY the
        # stable-field conjuncts here; attrs clauses stay in the caller's
        # residual Python post-filter (expose-mcp-node-lookup).
        layer1_clauses = _attr_free_pushdown_clauses(kind, scan_predicate)
        if not layer1_clauses:
            scan_predicate = None
        elif len(layer1_clauses) == 1:
            scan_predicate = layer1_clauses[0]
        else:
            scan_predicate = {"op": "and", "clauses": layer1_clauses}
        program = None
        if scan_predicate not in (None, True, {}):
            try:
                program, _columns = _filter_program(
                    kind=kind,
                    predicate=scan_predicate,
                    projection=None,
                    limit=scan_limit + 1,
                    offset=0,
                )
            except ValueError:
                # A pushdown clause that cannot lower to SQL must never change
                # results: drop the WHERE and fall back to the unfiltered scan,
                # so the residual Python post-filter remains the source of truth.
                program = None
        if program is None:
            program, _columns = _map_program(
                kind=kind,
                projection=None,
                limit=scan_limit + 1,
                offset=0,
            )
        rows = _lower_program_over_chronos(
            program,
            conn,
            generation_id=session.generation_id,
            branch=session.branch_name,
        )
        if kind == "nodes":
            rows = filter_node_rows_for_session(conn, session, rows)
        else:
            rows = filter_edge_rows_for_session(conn, session, rows)
        if len(rows) > scan_limit:
            raise SubstrateError(
                "effective-attrs operator scan exceeded the bounded "
                "Python overlay limit",
                data={"kind": kind, "limit": scan_limit},
                fix_hint=(
                    "narrow the input scope or use inspect/search; indexed "
                    "effective-attrs SQL views are required for larger "
                    "attr-filter/map/aggregate scans"
                ),
            )
        merged = apply_effective_attrs_to_rows(
            conn,
            rows,
            target_kind="node" if kind == "nodes" else "edge",
            target_id_field="node_id" if kind == "nodes" else "edge_id",
            generation_id=session.generation_id,
        )
        return merged, False

    def _operator_fence_node_rows(
        conn: psycopg.Connection,
        session: Session,
        *,
        kind: str,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if kind != "nodes" or not rows:
            return rows
        from okg.substrate.mcp.provenance import (
            serialize_node_read_rows_at_pin,
        )

        return serialize_node_read_rows_at_pin(
            conn,
            rows,
            deployment_name=session.deployment_name,
            generation_id=session.generation_id,
            branch_name=session.branch_name,
        )

    def _operator_serialize_edge_rows(
        conn: psycopg.Connection,
        session: Session,
        *,
        kind: str,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if kind != "edges" or not rows:
            return rows
        from okg.substrate.mcp.provenance import (
            serialize_edge_read_rows_at_pin,
        )

        return serialize_edge_read_rows_at_pin(
            conn,
            rows,
            deployment_name=session.deployment_name,
            generation_id=session.generation_id,
            branch_name=session.branch_name,
        )

    def _operator_projection_needs_provenance(
        *,
        kind: str,
        projection: Any,
    ) -> bool:
        return kind == "nodes" and (
            projection is None or _operator_refs_attrs(projection)
        )

    def _operator_projection_needs_read_provenance(
        *,
        kind: str,
        projection: Any,
    ) -> bool:
        return kind == "edges" and (
            projection is None or _operator_refs_attrs(projection)
        )

    def _operator_add_provenance_column(
        *,
        columns: list[str],
        projected: list[dict[str, Any]],
        source_rows: list[dict[str, Any]],
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """Label each projected node row with its source's trust.

        The caller decides WHETHER a label is due, through
        `_operator_projection_needs_provenance`: any projection that reads
        attrs (`attrs`, `attrs.<key>`, or an alias of either) carries the
        label. This helper used to re-decide from the projected column
        NAMES and returned early unless one was literally `attrs`, so
        `attrs.text` (column `attrs_text`) came back fenced but unlabelled
        (issue #2306).
        """
        if "provenance" not in columns:
            columns = [*columns, "provenance"]
        for projected_row, source_row in zip(projected, source_rows):
            projected_row["provenance"] = source_row.get("provenance")
        return columns, projected

    def _operator_add_read_provenance_column(
        *,
        columns: list[str],
        projected: list[dict[str, Any]],
        source_rows: list[dict[str, Any]],
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """Label each projected edge row with its source's trust.

        The edge twin of `_operator_add_provenance_column`; the caller
        decides through `_operator_projection_needs_read_provenance`
        (issue #2306).
        """
        if "read_provenance" not in columns:
            columns = [*columns, "read_provenance"]
        for projected_row, source_row in zip(projected, source_rows):
            projected_row["read_provenance"] = source_row.get("read_provenance")
        return columns, projected

    def _range_compare(value: Any, bound: Any, declared_type: Any) -> int | None:
        """Three-way compare for the range operators, or None when unordered.

        None means "these two are not comparable", which the caller turns into
        a non-match. That is the only safe answer: a mistyped or absent value
        must drop out of a range filter rather than being coerced into an
        arbitrary position, and returning None (rather than False) keeps the
        distinction visible at the call site so `lt` and `gt` cannot both
        accidentally match the same unorderable row.

        ``declared_type`` mirrors the type the SQL side compared in, so the two evaluations of the same predicate agree exactly.
        """
        if isinstance(value, bool) or isinstance(bound, bool):
            # JSON booleans are equality-only here; ordering them would make
            # `true > false` meaningful, which no caller intends and SQL never
            # has to answer.
            return None
        if declared_type == "ts":
            left, right = _as_timestamp(value), _as_timestamp(bound)
            if left is None or right is None:
                return None
            return -1 if left < right else (1 if left > right else 0)
        if declared_type == "text":
            if not isinstance(value, str) or not isinstance(bound, str):
                return None
            return -1 if value < bound else (1 if value > bound else 0)
        if declared_type == "num":
            if not isinstance(value, (int, float)) or not isinstance(
                bound, (int, float)
            ):
                return None
            return -1 if value < bound else (1 if value > bound else 0)
        # Undeclared key (the unchanged scan path): compare only within a
        # single JSON type, and never guess that a string is a timestamp --
        # there is no declaration saying it is, so SQL would not have either.
        if isinstance(value, (int, float)) and isinstance(bound, (int, float)):
            return -1 if value < bound else (1 if value > bound else 0)
        if isinstance(value, str) and isinstance(bound, str):
            return -1 if value < bound else (1 if value > bound else 0)
        return None

    def _declared_type_of(field: Any, declared: Any) -> str | None:
        """The declared value_type for an ``attrs.<key>`` field, else None.

        One place decides this so the range arm, the equality arm, and the sort
        key cannot drift into comparing the same attribute in different types.
        """
        if not isinstance(declared, Mapping):
            return None
        if not isinstance(field, str) or not field.startswith("attrs."):
            return None
        return declared.get(field.split(".", 1)[1])

    def _operator_field_value(row: dict[str, Any], field: str) -> Any:
        if field == "attrs":
            return row.get("attrs") or {}
        if field.startswith("attrs."):
            key = field.split(".", 1)[1]
            return (row.get("attrs") or {}).get(key)
        return row.get(field)

    def _operator_predicate_matches(
        row: dict[str, Any],
        predicate: Any,
        *,
        declared: Any = None,
    ) -> bool:
        """Evaluate a v1 predicate over one row's attrs.

        ``declared`` is the ``{attr_id: value_type}`` registry when the caller
        is re-checking rows that came back from a declared-key pushdown. It
        exists so the range operators compare values the SAME way Postgres did:
        a `ts`-declared key compares as timestamps on both sides, a
        `text`-declared key compares as text on both sides. Without it a text
        column whose values merely LOOK like timestamps would be ordered one
        way in SQL and another way here, and the parity re-check would silently
        drop rows that SQL correctly matched.
        """
        if predicate in (None, True) or predicate == {}:
            return True
        if not isinstance(predicate, dict):
            raise InvalidOperatorArguments("predicate must be an object")
        op = predicate.get("op")
        if op == "and":
            clauses = predicate.get("clauses")
            if not isinstance(clauses, list) or not clauses:
                raise InvalidOperatorArguments("and predicate requires non-empty clauses")
            return all(
                _operator_predicate_matches(row, c, declared=declared) for c in clauses
            )
        if op == "or":
            clauses = predicate.get("clauses")
            if not isinstance(clauses, list) or not clauses:
                raise InvalidOperatorArguments("or predicate requires non-empty clauses")
            return any(
                _operator_predicate_matches(row, c, declared=declared) for c in clauses
            )
        if op == "not":
            return not _operator_predicate_matches(
                row, predicate.get("clause"), declared=declared
            )
        if op in ("lt", "gt", "between"):
            # schema-attr-filter-index: the range operators the grammar never
            # had. Deliberately NOT expressed as `not (ge)` or similar: an
            # absent, null, or wrong-typed value must simply fail to match,
            # never flip a negation into a match.
            field = str(predicate.get("field") or "")
            value = _operator_field_value(row, field)
            declared_type = _declared_type_of(field, declared)
            bounds_given = (
                predicate.get("values") if op == "between" else [predicate.get("value")]
            )
            if (
                field == "edge_id" and isinstance(value, str) and value.isdigit()
                and isinstance(bounds_given, list) and bounds_given
                and all(isinstance(b, int) and not isinstance(b, bool) for b in bounds_given)
            ):
                # A link's id read as the number it is, as the lowerer
                # compares an integer range on it (issue #3666).
                value = int(value)
            if op == "between":
                bounds = predicate.get("values")
                if not isinstance(bounds, list) or len(bounds) != 2:
                    raise InvalidOperatorArguments("between predicate requires values [low, high]")
                low = _range_compare(value, bounds[0], declared_type)
                high = _range_compare(value, bounds[1], declared_type)
                # Inclusive on both ends, matching SQL BETWEEN.
                return low is not None and high is not None and low >= 0 and high <= 0
            ordering = _range_compare(value, predicate.get("value"), declared_type)
            if ordering is None:
                return False
            return ordering < 0 if op == "lt" else ordering > 0
        if op == "eq":
            field = str(predicate.get("field") or "")
            value = _operator_field_value(row, field)
            expected = predicate.get("value")
            # A `ts`-declared key is compared as a TIMESTAMP, matching how
            # SQL compared it. Otherwise
            # "2026-08-04T00:00:00+00:00" and "2026-08-04T00:00:00Z" -- the
            # same instant, and equal in SQL -- would fail here, and the parity
            # re-check would drop a row the pushdown correctly matched.
            if _declared_type_of(field, declared) == "ts":
                left, right = _as_timestamp(value), _as_timestamp(expected)
                if left is not None and right is not None:
                    return left == right
            return value == expected
        if op == "in":
            values = predicate.get("values")
            if not isinstance(values, list) or not values:
                raise InvalidOperatorArguments("in predicate requires non-empty values")
            try:
                held = _run_steps.marker_matches(row, predicate, _operator_field_value)
            except _run_steps.RunRefusal as exc:
                raise InvalidOperatorArguments(exc.message, fix_hint=exc.fix_hint) from exc
            if held is not _run_steps.NOT_A_MARKER:
                # `run` (#3666): membership in an earlier step's set.
                return bool(held)
            field = str(predicate.get("field") or "")
            value = _operator_field_value(row, field)
            # Same timestamp-normalization reason as `eq` above: `in` is a
            # disjunction of equalities and must agree with SQL's `= ANY(...)`
            # over the typed column.
            if _declared_type_of(field, declared) == "ts":
                left = _as_timestamp(value)
                if left is not None:
                    return any(
                        left == parsed
                        for parsed in (_as_timestamp(v) for v in values)
                        if parsed is not None
                    )
            return value in values
        if op == "exists":
            field = str(predicate.get("field") or "")
            if field.startswith("attrs."):
                key = field.split(".", 1)[1]
                return key in (row.get("attrs") or {})
            return _operator_field_value(row, field) is not None
        if op == "contains":
            from okg.substrate.mcp.operator_sql import is_text_contains, text_contains
            from okg.substrate.transform.operators import CONTAINS_FORMS_MESSAGE

            value = predicate.get("value")
            if is_text_contains(predicate):
                # Text inside one attribute (issue #3666), as the lowerer
                # compares it: a stored string, ASCII case ignored.
                return text_contains(
                    _operator_field_value(row, str(predicate["field"])), value,
                )
            if str(predicate.get("field") or "") != "attrs" or not isinstance(
                value, dict
            ):
                raise InvalidOperatorArguments(CONTAINS_FORMS_MESSAGE)
            attrs = row.get("attrs") or {}
            return all(attrs.get(key) == val for key, val in value.items())
        if op == "suffix":
            # expose-mcp-node-lookup: string-suffix match, the typed spelling
            # for "find the file whose path ends with .../CMakeLists.txt"
            # without sql_admin. Non-string field values never match.
            value = predicate.get("value")
            if not isinstance(value, str) or not value:
                raise InvalidOperatorArguments("suffix predicate requires a non-empty string value")
            field_value = _operator_field_value(
                row,
                str(predicate.get("field") or ""),
            )
            return isinstance(field_value, str) and field_value.endswith(value)
        if op == "prefix":
            # read-model-s3-where-scope: the string-prefix twin of `suffix`
            # ("every file under slack/"). Non-string values never match.
            value = predicate.get("value")
            if not isinstance(value, str) or not value:
                raise InvalidOperatorArguments("prefix predicate requires a non-empty string value")
            field_value = _operator_field_value(
                row,
                str(predicate.get("field") or ""),
            )
            return isinstance(field_value, str) and field_value.startswith(value)
        if op in ("like", "like_exact"):
            # loose-match-3666: the same fold the lowerer compiles
            # (`text_fold`), so a saved result and a re-checked page agree
            # with the SQL row for row.
            from okg.substrate.mcp import text_fold

            value = predicate.get("value")
            problem = text_fold.operand_problem(op, value)
            field = str(predicate.get("field") or "")
            if problem is None and field == "attrs":
                problem = f"{op} compares one attribute, written attrs.<name>"
            if problem is not None:
                raise InvalidOperatorArguments(f"{op} predicate: {problem}")
            return text_fold.matches(op, _operator_field_value(row, field), value)
        if op == _DATE_WINDOW_OP:
            # A date condition (#3666), as `_resolve_date_conditions` wrote
            # it: the stored value read by the one date parser.
            return _date_window_leaf_matches(
                _operator_field_value(row, str(predicate.get("field") or "")),
                predicate,
            )
        raise InvalidOperatorArguments(
            "unsupported predicate op; v1 supports and, or, not, eq, "
            "in, exists, contains, suffix, prefix, lt, gt, between, like, like_exact"
        )

    def _operator_predicate_verdict(
        row: dict[str, Any],
        predicate: Any,
        *,
        declared: Any = None,
    ) -> Optional[bool]:
        """Kleene three-valued truth of a v1 predicate on one READ row
        (read-model slice 7a). A clause on an attribute the read filter
        withheld from this reader (mask-tokened, or absent from a redacted
        row) is unknown (None), as in the SQL page step and in
        `search_where.evaluate`: it never matches, `not` keeps it unknown,
        and `or` with a true branch is true. Every other leaf is
        `_operator_predicate_matches`. Only a True verdict admits a row."""
        if predicate in (None, True) or predicate == {}:
            return True
        if not isinstance(predicate, dict):
            raise ValueError("predicate must be an object")
        op = predicate.get("op")
        if op in ("and", "or"):
            clauses = predicate.get("clauses")
            if not isinstance(clauses, list) or not clauses:
                raise ValueError(f"{op} predicate requires non-empty clauses")
            verdicts = [
                _operator_predicate_verdict(row, c, declared=declared) for c in clauses
            ]
            decisive = op == "or"
            if any(v is decisive for v in verdicts):
                return decisive
            return None if any(v is None for v in verdicts) else not decisive
        if op == "not":
            inner = _operator_predicate_verdict(
                row, predicate.get("clause"), declared=declared
            )
            return None if inner is None else not inner
        field = str(predicate.get("field") or "")
        attrs = row.get("attrs") or {}
        redacted = isinstance(attrs, Mapping) and attrs.get(REDACTION_MARKER) is True

        def withheld(key: str) -> bool:
            if key in attrs:
                return attrs[key] == MASK_TOKEN
            return redacted

        if op == "contains" and field == "attrs" and isinstance(predicate.get("value"), dict):
            return _operator_predicate_verdict(row, {"op": "and", "clauses": [
                {"op": "eq", "field": f"attrs.{key}", "value": value}
                for key, value in predicate["value"].items()
            ]} if predicate["value"] else None, declared=declared)
        if field.startswith("attrs.") and withheld(field.split(".", 1)[1]):
            return None
        if field == "attrs" and op != "exists" and redacted:
            return None
        if op == _DATE_WINDOW_OP:
            # #3666: under access rules a stored date not in ISO form was
            # not compared; unknown, so `not` does not match it either.
            return _date_window_leaf_verdict(_operator_field_value(row, field), predicate)
        return _operator_predicate_matches(row, predicate, declared=declared)

    def _operator_read_check(
        row: dict[str, Any],
        predicate: Any,
        *,
        declared: Any,
        nomos_runtime: bool,
    ) -> bool:
        """The filter/map check on one served row: two-valued with Nomos
        off (a stored value is the read value), three-valued under
        `audit`/`enforce` (slice 7a: a clause on a withheld value is unknown
        and only a True verdict admits the row)."""
        if nomos_runtime:
            return _operator_predicate_verdict(row, predicate, declared=declared) is True
        return _operator_predicate_matches(row, predicate, declared=declared)

    def _operator_projection(
        kind: str,
        projection: Any,
    ) -> tuple[list[tuple[str, str]], list[str]]:
        if projection is None:
            fields = (
                ["node_id", "subtype", "attrs", "text_hash"]
                if kind == "nodes"
                else ["edge_id", "src", "dst", "edge_type", "attrs"]
            )
            return [(field, field) for field in fields], fields
        if isinstance(projection, list):
            fields = [field for field in projection if isinstance(field, str) and field]
            if len(fields) != len(projection):
                raise InvalidOperatorArguments("projection list entries must be field names")
            return [(field.replace(".", "_"), field) for field in fields], [
                field.replace(".", "_") for field in fields
            ]
        if isinstance(projection, dict):
            pairs: list[tuple[str, str]] = []
            for alias, field in projection.items():
                if (
                    not isinstance(alias, str)
                    or not alias.replace("_", "").isalnum()
                    or not isinstance(field, str)
                ):
                    raise InvalidOperatorArguments(
                        "projection mapping must be {safe_alias: field_name}"
                    )
                pairs.append((alias, field))
            return pairs, [alias for alias, _field in pairs]
        raise InvalidOperatorArguments("projection must be a list or mapping")

    def _operator_project_rows(
        kind: str,
        rows: list[dict[str, Any]],
        projection: Any,
    ) -> tuple[list[str], list[dict[str, Any]]]:
        pairs, columns = _operator_projection(kind, projection)
        return columns, [
            {alias: _operator_field_value(row, field) for alias, field in pairs}
            for row in rows
        ]

    def _operator_page_count(
        *, offset: int, shown: int, truncated: bool,
    ) -> dict[str, Any]:
        """`total_count` and its basis for a filter or map page (#3190).

        A page is read with SQL `OFFSET`, and rows the reader may not see are
        dropped after it, so `offset` counts rows this reader never saw. Only
        a first page says anything about the set: whole (`exact`), or cut
        with more rows behind it (`lower_bound`, rendered `N+`). A later page
        cannot count the set (`unknown`, rendered "count unknown"). Never the
        page size presented as the total (envelope.py's retired convention).
        """
        if offset > 0:
            # An uncut later page is still provably the last one: the scan
            # read one row past it and found none.
            return {"total_count": None, "total_count_basis": "unknown",
                    "delivery_last_page": not truncated}
        return {
            "total_count": shown,
            "total_count_basis": "lower_bound" if truncated else "exact",
            "retrieval_exhaustive": not truncated,
        }

    def _read_filter_bound_notice(bound: int) -> dict[str, Any]:
        """Payload keys for a page that stopped at the read-filter scan bound
        of `_effective_view_sql_page` (#3208): no cursor, and why."""
        notice = (
            f"read {bound:,} stored matches before finding the end of the "
            "rows you may read; paging stops here. Narrow the predicate: "
            'add a type ({"op": "eq", "field": "type", "value": '
            '"<subtype>"}) or another exact attrs.<key> equality.'
        )
        return {
            "pagination_capped": True,
            "read_filter_scan_bound": bound,
            "pagination_notice": notice,
        }

    def _mark_not_a_total(
        payload: dict[str, Any],
        *,
        rows: int,
        page_facts: Mapping[str, Any],
        page_meta: Mapping[str, Any],
        for_a_total: Optional[dict[str, Any]] = None,
    ) -> None:
        """The bounded-page rule (issue #3666): a page that stopped at a
        bound before delivering a row shows no total.

        `0 of 0+` read as "nothing matches" where the read stopped after N
        records. Such a page says `0 on this page of N read, not a total`,
        its count is unknown, and `not_a_total.for_a_total` is a call that
        counts every match, only when the server proves it takes that call.
        """
        if rows or not payload.get("truncated"):
            return
        bound = page_facts.get("read_filter_scan_bound")
        if bound is not None:
            reason = f"the read-filter scan bound ({int(bound):,} stored rows)"
            read = page_facts.get("rows_scanned")
        elif payload.get("pagination_max_offset") is not None:
            reason = f"the paging ceiling (offset {int(payload['pagination_max_offset']):,})"
            read = None
        else:
            reason = f"the page limit ({payload.get('limit')} stored rows)"
            read = page_meta.get("raw_page_rows")
        block: dict[str, Any] = {
            "reason": reason,
            "records_read": int(read) if isinstance(read, int) else None,
        }
        takes = getattr(mcp, "_okg_takes_as_written", None)
        if (
            for_a_total is not None and callable(takes)
            and takes(for_a_total["tool"], dict(for_a_total["arguments"]))
        ):
            block["for_a_total"] = {"mode": "corrected", **for_a_total}
        payload["not_a_total"] = block
        payload["total_count"] = None
        payload["total_count_basis"] = "unknown"
        payload.pop("retrieval_exhaustive", None)

    def _operator_envelope(
        *,
        rows: list[dict[str, Any]],
        columns: list[str],
        page: int,
        offset: int,
        generation_id: str,
        scan_truncated: bool = False,
        page_facts: Optional[dict[str, Any]] = None,
        count_fields: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """A filter/map page. `scan_truncated`: the SQL scan hit its result
        ceiling, so the page is cut even when every row it read fit.
        `page_facts` from `_effective_view_sql_page`: at its read-filter scan
        bound the page offers no cursor and says why (#3208).
        `count_fields`: the count the caller established for the set
        (`_operator_count_fields`); the page-only rule otherwise."""
        truncated = len(rows) > page or scan_truncated
        visible = rows[:page]
        bound = (page_facts or {}).get("read_filter_scan_bound")
        if bound is not None:
            return {
                **_operator_envelope(
                    rows=rows, columns=columns, page=page, offset=offset,
                    generation_id=generation_id, scan_truncated=scan_truncated,
                    count_fields=count_fields,
                ),
                "next_offset": None,
                **_read_filter_bound_notice(bound),
            }
        return {
            "items": [
                {column: row.get(column) for column in columns} for row in visible
            ],
            **(
                dict(count_fields)
                if count_fields is not None
                else _operator_page_count(
                    offset=offset, shown=len(visible), truncated=truncated,
                )
            ),
            "truncated": truncated,
            "limit": page,
            "offset": offset,
            "next_offset": offset + page if truncated else None,
            "pinned_generation_id": generation_id,
            "query_plan_summary": "Chronos checkout + effective attrs",
            "elapsed_ms": 0,
        }

    def _read_admission_state(
        conn: psycopg.Connection,
        *,
        admission_id: str | None,
        limit: int,
    ) -> dict[str, Any]:
        with conn.cursor() as cur:
            if admission_id:
                cur.execute(
                    """
                    SELECT admission_id, deployment_name, producer_id,
                           producer_kind, intent, admission_mode, status,
                           catalog_version_id, source, run_id,
                           target_generation_ref,
                           canonical_generation_ref, idempotency_key,
                           authority, check_summary, verdict,
                           workflow_ref, created_at, updated_at,
                           decided_at, published_at
                      FROM okg.v_admission_envelopes
                     WHERE admission_id = %s
                    """,
                    (admission_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT admission_id, deployment_name, producer_id,
                           producer_kind, intent, admission_mode, status,
                           catalog_version_id, source, run_id,
                           target_generation_ref,
                           canonical_generation_ref, idempotency_key,
                           authority, check_summary, verdict,
                           workflow_ref, created_at, updated_at,
                           decided_at, published_at
                      FROM okg.v_admission_envelopes
                     WHERE deployment_name = %s
                     ORDER BY created_at DESC
                     LIMIT %s
                    """,
                    (deployment_name, max(1, min(int(limit), 100))),
                )
            rows = cur.fetchall()
        columns = [
            "admission_id",
            "deployment_name",
            "producer_id",
            "producer_kind",
            "intent",
            "admission_mode",
            "status",
            "catalog_version_id",
            "source",
            "run_id",
            "target_generation_ref",
            "canonical_generation_ref",
            "idempotency_key",
            "authority",
            "check_summary",
            "verdict",
            "workflow_ref",
            "created_at",
            "updated_at",
            "decided_at",
            "published_at",
        ]
        return {
            "items": [dict(zip(columns, row)) for row in rows],
            "count": len(rows),
            "total_count": len(rows),
            "truncated": False,
        }

    def _read_record_state(
        conn: psycopg.Connection,
        *,
        admission_id: str | None,
        record_id: str | None,
        limit: int,
    ) -> dict[str, Any]:
        columns = [
            "admission_id",
            "deployment_name",
            "producer_id",
            "producer_kind",
            "intent",
            "admission_mode",
            "status",
            "catalog_version_id",
            "source",
            "run_id",
            "target_generation_ref",
            "canonical_generation_ref",
            "idempotency_key",
            "authority",
            "check_summary",
            "verdict",
            "workflow_ref",
            "created_at",
            "updated_at",
            "decided_at",
            "published_at",
        ]
        where = [
            "deployment_name = %s",
            "intent LIKE 'record_%%'",
        ]
        params: list[Any] = [deployment_name]
        if admission_id:
            where.append("admission_id = %s::uuid")
            params.append(admission_id)
        if record_id:
            where.append("check_summary->'record'->>'record_id' = %s")
            params.append(record_id)
        params.append(max(1, min(int(limit), 100)))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT admission_id, deployment_name, producer_id,
                       producer_kind, intent, admission_mode, status,
                       catalog_version_id, source, run_id,
                       target_generation_ref,
                       canonical_generation_ref, idempotency_key,
                       authority, check_summary, verdict,
                       workflow_ref, created_at, updated_at,
                       decided_at, published_at
                  FROM okg.v_admission_envelopes
                 WHERE {" AND ".join(where)}
                 ORDER BY created_at DESC
                 LIMIT %s
                """,
                params,
            )
            rows = cur.fetchall()
        return {
            "items": [dict(zip(columns, row)) for row in rows],
            "count": len(rows),
            "total_count": len(rows),
            "truncated": False,
        }

    def _session_start_card(state: Mapping[str, Any]) -> Optional[dict[str, Any]]:
        if not state:
            return None
        # `inputs` already carries the text's bytes and sha256.
        return {key: value for key, value in state.items() if key != "text"}

    def _render_session_start_context(
        conn: psycopg.Connection,
        session: Session,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        from okg.substrate.mcp.run_sql import run_sql as _run_sql

        config = metadata.get("session_start_context")
        payload: dict[str, Any] = {
            "kind": "session_start",
            "deployment_name": session.deployment_name,
            "generation_id": session.generation_id,
            "pinned_generation_id": session.generation_id,
            "catalog_version_id": session.catalog_version_id,
            "configured": isinstance(config, dict) and bool(config),
            # #3470 (`ssfit A`): the identity of the running server only;
            # the full readback is `inspect(kind="surface")` where served, and
            # `okg doctor --check mcp-surface-freshness`. Whole, it made
            # session_start about 10 KB, refused at the 4 KB agents ask for.
            "surface": {
                key: surface_freshness[key]
                for key in _SESSION_START_SURFACE_KEYS
                if key in surface_freshness
            },

            # PACT dataset-card: the card this server serves, what it was
            # compiled from, and the appended deployment file's hash. None
            # when a caller passed its own instructions (no card is served).
            # The card's text is not repeated (#3470): it is the start of
            # the served instructions, named by `inputs`' bytes and sha256.
            "dataset_card": _session_start_card(dataset_card_state),
            "slices": {},
            "diagnostics": [],
        }
        # Named only where it is served (every launch since #3666, M10; a
        # read launch answers it read-only).
        if "surface" in (getattr(mcp, "_okg_inspect_served_kinds", None) or ()):
            payload["surface_full"] = 'inspect(target={"kind":"surface"})'
        if not isinstance(config, dict) or not config:
            return payload
        slices = config.get("slices") or config.get("queries") or {}
        if not isinstance(slices, dict):
            payload["diagnostics"].append(
                {
                    "code": "invalid_session_start_slices",
                    "message": (
                        "session_start_context.slices must be a mapping "
                        "of slice name to query template"
                    ),
                }
            )
            return payload
        default_limit = _bounded_int(
            config.get("default_limit"),
            default=20,
            lower=1,
            upper=100,
        )
        default_timeout_ms = _bounded_int(
            config.get("timeout_ms"),
            default=5_000,
            lower=100,
            upper=60_000,
        )
        for name, raw in sorted(slices.items()):
            slice_name = str(name)
            if not isinstance(raw, dict):
                payload["diagnostics"].append(
                    {
                        "slice": slice_name,
                        "code": "invalid_session_start_slice",
                        "message": "slice definition must be an object",
                    }
                )
                continue
            sql = raw.get("sql") or raw.get("query")
            if not isinstance(sql, str) or not sql.strip():
                payload["diagnostics"].append(
                    {
                        "slice": slice_name,
                        "code": "missing_session_start_sql",
                        "message": "slice definition must include sql",
                    }
                )
                continue
            limit = _bounded_int(
                raw.get("limit"),
                default=default_limit,
                lower=1,
                upper=100,
            )
            timeout_ms = _bounded_int(
                raw.get("timeout_ms"),
                default=default_timeout_ms,
                lower=100,
                upper=60_000,
            )
            try:
                result = _run_sql(
                    conn,
                    session,
                    sql,
                    row_cap=limit,
                    timeout_ms=timeout_ms,
                )
            except Exception as exc:  # noqa: BLE001 - diagnostic surface
                payload["diagnostics"].append(
                    {
                        "slice": slice_name,
                        "code": getattr(exc, "code", type(exc).__name__),
                        "message": str(exc),
                        "optional": bool(raw.get("optional", True)),
                    }
                )
                continue
            payload["slices"][slice_name] = {
                "columns": result.get("columns", []),
                "rows": result.get("rows", []),
                # run_sql reports the rows in its payload, not a set total
                # (issue #1440). Carry the field it actually emits rather
                # than defaulting a missing total to 0.
                "returned_count": result.get("returned_count", 0),
                "truncated": result.get("truncated", False),
                "row_cap": limit,
            }
        return payload

    def _surface_payload_for_session() -> dict[str, Any]:
        """The served-surface readback, answered for THIS session.

        `surface_freshness` is built once per process. Under a per-session
        projection the served tool list is a per-session fact, so the two
        fields that can differ between sessions are overlaid from the
        calling session's own state rather than reported process-wide.
        """

        payload = dict(surface_freshness)
        view = dict(payload.get("tool_surface") or {})
        registry = session_tool_surface
        if registry is not None:
            identity = _current_logical_identity()
            visible = registry.visible_for(identity)
            view["served_tools"] = [
                name for name in CANONICAL_TOOL_ORDER if name in visible
            ]
            expansion = dict(view.get("expansion") or {})
            expansion["expanded"] = registry.is_expanded(identity)
            expansion["expandable_tools"] = sorted(registry.ceiling - visible)
            expansion["list_changed_notification_failed"] = (
                registry.notification_failed(identity)
            )
            view["expansion"] = expansion
        payload["tool_surface"] = view
        from okg.substrate.mcp.principal_readiness import (
            load_readiness_providers,
            runtime_principal_readiness,
        )

        def load_runtime_providers():
            conn = _pool_getconn(pool)
            try:
                return load_readiness_providers(conn, deployment_name)
            finally:
                pool.putconn(conn)

        payload["principal_readiness"] = runtime_principal_readiness(
            mcp, deployment=deployment_name, load_providers=load_runtime_providers
        )
        return payload

    def _expand_session_tool_surface(target: dict[str, Any]) -> dict[str, Any]:
        """Widen THIS session to the launch's ceiling (issue #1458).

        Only this session changes. The `notifications/tools/list_changed`
        frame is written by the transport layer immediately after this
        result is computed and before the tool response is sent, so the
        status below says `queued` rather than `sent`: a frame this function
        has not yet handed to the session is not a frame it can report as
        delivered. A send that then fails is reported by
        `inspect(target={"kind":"surface"})` as
        `expansion.list_changed_notification_failed`.
        """

        registry = session_tool_surface
        if registry is None:
            return _operator_refusal(
                "inspect",
                code="mcp_tool_surface_expansion_unavailable",
                message=(
                    "this server serves a fixed tool surface: it was launched "
                    "without an in-session ceiling, so no session can widen. "
                    "Relaunch with --tools-ceiling, or with the wider "
                    "--tools."
                ),
            )
        requested = target.get("profile")
        if requested is not None and str(requested) != str(registry.ceiling_profile):
            return _operator_refusal(
                "inspect",
                code="mcp_tool_surface_expansion_profile_mismatch",
                message=(
                    f"this session can expand only to "
                    f"{registry.ceiling_profile!r}, the ceiling this launch "
                    f"serves; {str(requested)!r} was requested. The ceiling is "
                    "an operator decision made at launch and a caller cannot "
                    "raise it."
                ),
            )
        try:
            outcome = registry.expand(_current_logical_identity())
        except ToolSurfaceExpansionUnavailable as exc:
            return _operator_refusal(
                "inspect", code=exc.code, message=str(exc),
            )
        return {
            "kind": "surface_expansion",
            "status": outcome.status,
            "ceiling_profile": registry.ceiling_profile,
            "served_tools": list(outcome.visible_tools),
            "added_tools": list(outcome.added_tools),
            "list_changed_notification": (
                "queued" if outcome.status == "expanded" else "not_needed"
            ),
        }

    def _keyset_next_call(
        tool: str,
        token: str,
        *,
        given: dict[str, Any],
    ) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
        """The `filter` or `map` read a keyset `next` token continues, or a
        refusal (#3666 step 3b). The token signs the read (scope, condition,
        fields), so `next` stands alone: a read-shaping argument beside it
        is refused, not ignored; `limit`, `max_bytes`, `generation_id` and
        `authority` may stay. The operator then compares the whole binding
        (generation, catalog, reader) when it reads the cursor."""
        from okg.substrate.mcp.continuation import route_next

        named = sorted(key for key, value in given.items() if value not in (None, False, 0))
        if named:
            refusal = _operator_refusal(
                tool,
                code="next_with_read_arguments",
                message=(
                    f"{tool}: `next` carries the whole read and cannot be "
                    f"combined with {', '.join(named)}"
                ),
            )
            refusal["error"]["fix_hint"] = (
                f"call {tool}(next=<token>) alone; limit, max_bytes, "
                "generation_id and authority may stay"
            )
            return None, refusal
        binding = route_next(token, tool=tool)
        read = binding.get("projection")
        read = read if isinstance(read, dict) else {}
        return {"generation_id": binding.get("generation"), **read}, None

    def _keyset_binding(h: Any, tool: str, read: Mapping[str, Any]) -> dict[str, Any]:
        from okg.substrate.mcp.continuation import continuation_binding

        return continuation_binding(
            h.conn, h.session, kind=f"{tool}_page", sort="id", projection=dict(read),
        )

    def _keyset_cursor(
        h: Any, tool: str, token: str, read: Mapping[str, Any],
    ) -> tuple[str, int]:
        """The id a keyset page starts after, and the rows before it."""
        from okg.substrate.mcp.continuation import (
            ContinuationTokenInvalid,
            consume_continuation,
        )

        cursor = consume_continuation(token, expected_binding=_keyset_binding(h, tool, read))
        after, position = cursor.get("after"), cursor.get("position")
        if (
            not isinstance(after, str) or isinstance(position, bool)
            or not isinstance(position, int) or position < 0
        ):
            raise ContinuationTokenInvalid(
                "the continuation token has no usable cursor",
                data={"reason": "cursor_invalid"},
                fix_hint="restart the read without a continuation token",
            )
        return after, position

    def _program_orders_by_id(input: Any, projection: Any) -> bool:
        """Whether the filter/map scan program (``_scan_program``, ordered
        by its first projected column) pages in the id order the keyset
        token resumes in (#3666 step 3b review). A projection that starts
        with another column orders by it, which is not even total; such a
        call is read on the effective-view path, in id order, from its
        first page, so a listing never changes order at the offset cap."""
        try:
            kind = _scope_kind(input)
        except ValueError:
            return True
        id_field = "node_id" if kind == "nodes" else "edge_id"
        if projection is None:
            return True
        if isinstance(projection, (list, tuple)):
            return bool(projection) and projection[0] == id_field
        if isinstance(projection, Mapping):
            first = next(iter(projection.values()), None)
            return first == id_field
        return True

    def _keyset_continue(
        h: Any,
        tool: str,
        envelope: dict[str, Any],
        *,
        read: Mapping[str, Any],
        page_meta: Mapping[str, Any],
        keyset: bool,
        ordered: bool,
    ) -> None:
        """Past the offset cap, the next page by a token, never by an offset
        `filter` and `map` refuse (#3666 step 3b; issue #3683 item 3). In the
        default id order, which is total, a signed token resumes after this
        page's last row: `next` alone continues, to the end of the set. With
        `order_by` no token is offered; the page says so and names the
        narrowing. A page read by a token always continues by one.

        Under access rules, a keyset page whose scan stopped at its bound
        with every readable row it found already on the page resumes after
        the last STORED row scanned (`scan_resume_id`), so `next` goes on
        instead of ending. A scan that stopped before reaching the page's
        offset offers no token: the page says paging stops."""
        from okg.substrate.mcp.continuation import issue_continuation

        position = int(envelope.get("offset") or 0) + int(page_meta.get("consumed") or 0)
        keyset_page = keyset or (
            int(envelope.get("offset") or 0) + int(envelope.get("limit") or 0) > MAX_OFFSET
        )
        bound = envelope.get("read_filter_scan_bound")
        resume = page_meta.get("scan_resume_id")
        if keyset_page and not ordered and bound is not None and resume is not None:
            envelope.pop("pagination_capped", None)
            envelope["next_offset"] = None
            envelope["pagination_notice"] = (
                f"read {int(bound):,} stored records past the last row before "
                "this page and found only the rows shown that you can read; "
                "`next` continues after the last record read"
            )
            envelope["continuation_token"] = issue_continuation(
                _keyset_binding(h, tool, read),
                cursor={"after": str(resume), "position": position},
            )
            return
        next_offset = envelope.get("next_offset")
        if next_offset is None or not (keyset or next_offset > MAX_OFFSET):
            return
        envelope["next_offset"] = None
        last_id = page_meta.get("last_id")
        if ordered or last_id is None:
            envelope["pagination_capped"] = True
            envelope["pagination_notice"] = (
                f"the next page starts past offset {MAX_OFFSET:,}, the most "
                "`offset` reaches, and only the default order pages by `next`: "
                "narrow the predicate (for example a range on the ordered "
                "field), or drop order_by"
                if ordered else
                f"the next page starts past offset {MAX_OFFSET:,}, the most "
                "`offset` reaches, and this page read no row to resume after: "
                "narrow the predicate"
            )
            return
        envelope["continuation_token"] = issue_continuation(
            _keyset_binding(h, tool, read),
            cursor={"after": str(last_id), "position": position},
        )

    def _next_call(
        tool: str,
        token: str,
        *,
        given: dict[str, Any],
    ) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
        """The call a `next` token continues, or a refusal.

        PACT read-model-s6-page-next (design `read-model` section 3.5). The
        token's kind routes it (`continuation.NEXT_ROUTES`); its signed
        binding names the read, so `next` stands alone: any read-shaping
        argument beside it is refused rather than silently ignored. The
        operator then compares the rebuilt read with the binding as it does
        for `continuation_token`, so a forged, stale or wrong-kind token is
        refused by name there. Returns `(arguments, None)` or
        `(None, refusal)`; `arguments["generation_id"]` is the token's pin.
        """
        from okg.substrate.mcp.continuation import route_next

        named = sorted(key for key, value in given.items() if value not in (None, False))
        if named:
            refusal = _operator_refusal(
                tool,
                code="next_with_read_arguments",
                message=(
                    f"{tool}: `next` carries the whole read and cannot be "
                    f"combined with {', '.join(named)}"
                ),
            )
            refusal["error"]["fix_hint"] = (
                f"call {tool}(next=<token>) alone; generation_id, authority "
                "may stay; omit max_bytes to resume the signed budget"
            )
            return None, refusal
        binding = route_next(token, tool=tool)
        kind = binding.get("kind")
        projection = binding.get("projection")
        projection = projection if isinstance(projection, dict) else {}
        arguments: dict[str, Any] = {"generation_id": binding.get("generation")}
        if kind == "inspect_text":
            arguments["target"] = {
                "kind": "text",
                "node_id": projection.get("node_id"),
                "continuation_token": token,
            }
            if projection.get("lines") is not None:
                # A line read signs its ranges; `next` reads on in them.
                arguments["target"]["lines"] = projection["lines"]
            if projection.get("matching") is not None:
                # A matching read signs its pattern and context instead.
                arguments["target"]["matching"] = projection["matching"]
                arguments["target"]["context"] = projection.get("context")
        elif kind == "inspect_text_batch":
            arguments["target"] = {
                "kind": "texts",
                "items": projection.get("items"),
                "continuation_token": token,
            }
        elif kind == "inspect_node_batch":
            arguments["target"] = {
                "kind": "nodes",
                "node_ids": projection.get("node_ids"),
                "ref": projection.get("ref"),
                "continuation_token": token,
            }
            # The batch's select is part of the signed read (#3657).
            arguments["select"] = projection.get("select")
        elif kind == "inspect_subtype":
            arguments["target"] = {
                "kind": "subtype",
                "subtype": binding.get("subtype"),
                "detail": projection.get("detail"),
                "page_size": projection.get("page_size"),
                "continuation_token": token,
            }
            # The page budget is part of the read: resume under the same one.
            arguments["max_bytes"] = projection.get("max_bytes")
        else:  # expand_path
            from okg.substrate.mcp.continuation import expand_shape_from_hops

            shape = expand_shape_from_hops(projection.get("hops"))
            if shape is None:
                # A token issued before path bindings recorded their hops, or
                # a malformed hop list: refuse rather than read a different
                # shape.
                from okg.substrate.mcp.continuation import ContinuationTokenInvalid

                raise ContinuationTokenInvalid(
                    "the path continuation token does not record its hops, so "
                    "`next` cannot rebuild the read it continues (target differs)",
                    data={
                        "reason": "binding_mismatch",
                        "mismatched": ["target"],
                        "pinned_generation_id": binding.get("generation"),
                    },
                    fix_hint="restart the read without a continuation token",
                )
            arguments.update(
                input=projection.get("start_node"),
                target_subtype=projection.get("target_subtype"),
                limit=projection.get("limit"),
                select=projection.get("select"),
                continuation_token=token,
                path=shape["path"],
                edge_type=shape["edge_type"],
                direction=shape["direction"],
                depth=shape["depth"],
            )
        return arguments, None

    def inspect(
        target: Optional[InspectTarget] = None,
        generation_id: Optional[Union[int, str]] = None,
        select: Optional[list[str]] = None,
        authority: Optional[dict[str, Any]] = None,
        max_bytes: MaxBytes = None,
        # Last, after every existing parameter: a positional caller binds
        # `authority` fourth, and a security check reads it there.
        next: Optional[str] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Read one named thing.

        `target.kind` picks the subject. Start with
        inspect(target={"kind":"graph"}): record and link type counts.
        `select` and `max_bytes` go beside `target`, not in it:
        inspect(target={"kind":"node","node_id":"<id>"}, select=["attrs"]).
        A `node` over max_bytes comes back with its largest parts cut, each
        named. `node`/`text` take `path` (+`source`) for `node_id`.
        `text` reads a record's whole text in reading order,
        in pages of max_bytes (default 65536, up to 262144); `lines` reads
        a list of [first,last] line ranges; `matching` (text or
        re:/regex/) only matching lines, `context` (2) lines around.
        `next` continues a page at its page size, or at a new max_bytes.
        """
        # A live node or live type (#3666, live-graph-rucio-sites): the
        # resolver read it before this body ran; serve that reply.
        live_reply = _live_nodes.served_node_reply("inspect")
        if live_reply is not None:
            return _stamp_operator(live_reply, "inspect")
        # PACT read-names-list-3666: node and text reads name the record's
        # citation id when this deployment declares `citation_attr`.
        _citation_id.CITATION_ATTR.set(
            _citation_id.deployment_citation_attr(deployment_name)
        )
        if next is not None:
            arguments, refusal = _next_call("inspect", next, given={"target": target, "select": select})
            if refusal is not None:
                return refusal
            target = arguments["target"]
            if "select" in arguments:
                # A batch record read signs its select (#3657).
                select = arguments["select"]
            if generation_id is None:
                generation_id = arguments["generation_id"]
            # The response wrapper restores and validates the signed budget
            # before injecting max_bytes; it also rejects explicit overrides.
        elif target is None:
            refusal = _operator_refusal(
                "inspect",
                code="invalid_operator_arguments",
                message="inspect needs a `target`, or `next` to continue a paged read",
            )
            refusal["error"]["fix_hint"] = (
                'pass target={"kind": ...}, or next=<token> from a page that ended '
                "bound reached"
            )
            return refusal
        # `target` is a validated InspectTarget by the time an MCP call gets
        # here, and `as_mapping` turns it into the mapping the body reads. A
        # direct Python caller can still pass a plain dict, which is what the
        # `.get` calls below expect. The old "must be an object" refusal sat
        # here and could no longer be reached.
        target = as_mapping(target)
        kind = target.get("kind")
        # The kind guard runs BEFORE the dispatch below, and before anything
        # touches the database (PACT effective-surface-declaration,
        # req.esd.inspect-kinds-by-profile). `inspect_kind_set` is the same
        # value the published union is built from, so the schema and this
        # answer cannot disagree.
        #
        # Refusing here rather than letting the narrowed input model reject
        # the call is the point: the model would answer with a schema-shaped
        # message that cannot tell "this kind does not exist" from "this
        # launch withholds it", and the caller's next step differs. That is
        # the same distinction `ToolProfileToolAbsent` preserves one level up,
        # at the tool.
        assert_inspect_kind_served(kind, served_kinds=inspect_kind_set)
        if kind == "session" and target.get("action") is not None:
            action = target.get("action")
            if action != "refresh":
                return _operator_refusal(
                    "inspect",
                    code="unsupported_session_action",
                    message=(
                        "inspect session action must be 'refresh'; open a new "
                        "MCP transport session for a separate analysis"
                    ),
                )
            if generation_id is not None:
                return _operator_refusal(
                    "inspect",
                    code="mcp_session_refresh_generation_conflict",
                    message=(
                        "session refresh replaces the omitted-call default; "
                        "a non-null generation_id is a one-call override and "
                        "cannot be combined with refresh"
                    ),
                )
            if _current_logical_identity() is None:
                return _operator_refusal(
                    "inspect",
                    code="mcp_session_refresh_identity_unavailable",
                    message=(
                        "session refresh requires a stable stateful MCP "
                        "ServerSession identity"
                    ),
                )
            with _open(
                None,
                operator_name="inspect",
                authority=authority,
                refresh_default=True,
            ) as h:
                payload = {
                    "status": "refreshed",
                    "generation_id": h.session.generation_id,
                    "pinned_generation_id": h.session.generation_id,
                    "old_generation_id": h.refresh_old_generation_id,
                    "new_generation_id": h.session.generation_id,
                }
                return _stamp_operator(payload, "inspect")
        if kind == "record_templates":
            constraints = _record_catalog_constraints()
            return _stamp_operator(
                record_templates_payload(
                    target.get("record_kind"),
                    prefixes=active_record_prefixes,
                    active_subtypes=constraints.get("active_subtypes"),
                    narrowing_triples=constraints.get("narrowing_triples"),
                ),
                "inspect",
            )
        if kind == "record_prefixes":
            constraints = _record_catalog_constraints()
            return _stamp_operator(
                record_prefixes_payload(
                    active_record_prefixes,
                    active_subtypes=constraints.get("active_subtypes"),
                    narrowing_triples=constraints.get("narrowing_triples"),
                ),
                "inspect",
            )
        if kind == "session_start":
            with _open(generation_id, authority=authority) as h:
                payload = _render_session_start_context(
                    h.conn,
                    h.session,
                    readside_metadata,
                )
                return _stamp_operator(
                    _maybe_stamp_advanced(h, payload),
                    "inspect",
                )
        if kind == "surface":
            action = target.get("action")
            if action is not None and action != "expand":
                return _operator_refusal(
                    "inspect",
                    code="unsupported_surface_action",
                    message=(
                        "inspect surface action must be 'expand'; omit it to "
                        "read the served surface"
                    ),
                )
            if action == "expand":
                return _stamp_operator(
                    _expand_session_tool_surface(target), "inspect",
                )
            if not allow_writes and session_tool_surface is None:
                # A read launch (#3666, M10): read-only, and only what a
                # caller can send, never the process report a write/admin
                # or ceiling launch serves.
                return _stamp_operator({
                    "status": "ok",
                    "surface": "read_only",
                    "served_tools": list(mcp._okg_served_tools),
                    # One string, one form per line: the text reply prints
                    # it as a block of plain lines, not as quoted JSON.
                    "accepted_not_shown": "\n".join(
                        getattr(mcp, "_okg_lean_hidden_lines", ())
                    ),
                }, "inspect")
            return _stamp_operator(_surface_payload_for_session(), "inspect")
        if kind == "admission":
            # _open_logical takes the reference verbatim (int | str): a
            # chronos TEXT checkpoint ref must not be parsed with int().
            # Admission state is generation-agnostic; 0 remains the
            # "no reference supplied" placeholder echoed back to the caller.
            with _open_logical(generation_id or 0) as h:
                payload = _read_admission_state(
                    h.conn,
                    admission_id=target.get("admission_id"),
                    limit=int(target.get("limit") or 20),
                )
                payload["generation_id"] = h.session.generation_id
                payload["pinned_generation_id"] = h.session.generation_id
                return _stamp_operator(
                    _maybe_stamp_advanced(h, payload),
                    "inspect",
                )
        if kind == "record":
            # Same contract as the admission target above: pass the
            # reference through, never int()-parse a chronos TEXT ref.
            with _open_logical(generation_id or 0) as h:
                result = _read_record_state(
                    h.conn,
                    admission_id=target.get("admission_id"),
                    record_id=target.get("record_id"),
                    limit=int(target.get("limit") or 20),
                )
                result["generation_id"] = h.session.generation_id
                result["pinned_generation_id"] = h.session.generation_id
                return _stamp_operator(
                    _maybe_stamp_advanced(h, result),
                    "inspect",
                )
        if kind == "alias_suggestions":
            from okg.substrate.catalog.aliases import (
                suggest_aliases_from_unresolved_mentions,
            )

            with _open(generation_id, authority=authority) as h:
                payload = {
                    "deployment_name": h.session.deployment_name,
                    "generation_id": h.session.generation_id,
                    "pinned_generation_id": h.session.generation_id,
                    "limit": int(target.get("limit") or 20),
                    "suggestions": suggest_aliases_from_unresolved_mentions(
                        h.conn,
                        limit=int(target.get("limit") or 20),
                    ),
                }
                return _stamp_operator(
                    _maybe_stamp_advanced(h, payload),
                    "inspect",
                )
        batch_ref = target.get("ref") if kind == "nodes" else None
        if batch_ref is not None:
            from okg.substrate.mcp import live_result_sets as _live_sets

            if _live_sets.is_live_id(batch_ref):
                raise InvalidOperatorArguments(
                    "inspect nodes reads graph records; a live result set "
                    "holds external rows, not records",
                    data={"tool": "inspect", "invalid_fields": ["target.nodes.ref"],
                          "validation_kinds": ["argument_check"]},
                    fix_hint='read a live set with filter/map/aggregate input={"ref": id}',
                )
        with _open(
            generation_id, authority=authority,
            reference=str(batch_ref) if batch_ref is not None else None,
        ) as h:
            resolved_path: Optional[dict[str, Any]] = None
            if kind in {"node", "text"} and target.get("path") is not None:
                # #3666 step 3b (Jason 2026-10-12): a file named by its path
                # is read when exactly one record has it; the reply names
                # the id it resolved to, and a token signs that id.
                from okg.substrate.mcp.tools import resolve_target_path

                resolved_id = resolve_target_path(h.conn, h.session, target)
                resolved_path = {
                    "path": target.get("path"),
                    **({"source": target["source"]} if target.get("source") else {}),
                    "node_id": resolved_id,
                }
                target = {
                    **{key: value for key, value in target.items() if key not in {"path", "source"}},
                    "node_id": resolved_id,
                }
            if kind == "graph":
                # `select` NARROWS THE WORK here, not just the payload: an
                # unselected block does not run its whole-graph query
                # (bound-inspect-graph-select).
                payload = _inspect_graph(h.conn, h.session, select)
            elif kind == "coverage":
                from okg.substrate.mcp.source_coverage import inspect_coverage

                # `select` narrows the WORK here too: an unselected block does
                # not run its query, matching the graph target.
                payload = inspect_coverage(h.conn, h.session, select)
            elif kind == "ontology":
                payload = _get_ontology(h.conn, h.session)
                payload.setdefault("generation_id", h.session.generation_id)
                payload.setdefault(
                    "pinned_generation_id",
                    h.session.generation_id,
                )
            elif kind == "subtype":
                # #3076 (decision 5): the deployment's skill hints for a type
                # live here, read once per type, not on every inspect(node).
                # The summary is sized to the call's budget, so the hints'
                # bytes are reserved first; when they cannot fit, the result
                # says they were withheld instead of overrunning the budget.
                hints = (
                    _skill_hints_for_subtype(
                        readside_metadata, subtype=str(target["subtype"]),
                    )
                    if isinstance(target.get("subtype"), str) and target.get("subtype")
                    else []
                )
                budget = int(max_bytes)
                reserve = (
                    len(_json.dumps(hints, separators=(",", ":")).encode("utf-8")) + 128
                    if hints else 0
                )
                attach_hints = bool(hints) and budget - reserve >= max(4096, budget // 2)
                # The `adjusted` note is stamped after this page is sized
                # (issue #3666): its bytes come off the page's budget, down
                # to the smallest budget a page is sized for and no further.
                # Below that the wrapper's own measure decides, and refuses
                # a page the note would push over.
                page_budget = budget - reserve if attach_hints else budget
                page_budget = max(
                    min(page_budget, 4096), page_budget - adjusted_reserve_bytes(),
                )
                from okg.substrate.mcp.subtype_summary import SubtypeSummaryItemTooLarge

                try:
                    payload = _inspect_subtype(
                        h.conn,
                        h.session,
                        target.get("subtype"),
                        continuation_token=target.get("continuation_token"),
                        detail=target.get("detail") or "summary",
                        page_size=(
                            8 if target.get("page_size") is None else target["page_size"]
                        ),
                        # Already the call's resolved budget: the byte-bound
                        # wrapper writes it into `max_bytes` (issue #2156).
                        max_bytes=page_budget,
                    )
                except SubtypeSummaryItemTooLarge as exc:
                    # #3666 step 3b: the refusal carries the same call at a
                    # budget whose page room holds the item, when the server
                    # accepts that budget. A continued read keeps its signed
                    # budget and gets words only.
                    needed_page = getattr(exc, "needed_page_bytes", None)
                    if isinstance(needed_page, int) and not target.get("continuation_token"):
                        from okg.substrate.mcp.response_bounds import (
                            MAX_RESPONSE_BYTES as _max_bytes_ceiling,
                            MIN_RESPONSE_BYTES as _min_bytes,
                        )

                        ceiling = (
                            agent_surface.effective_response_ceiling
                            if agent_surface is not None else None
                        ) or _max_bytes_ceiling
                        retry_budget = max(
                            _min_bytes,
                            needed_page + reserve + adjusted_reserve_bytes(),
                        )
                        if retry_budget <= ceiling:
                            exc.retry_call = {
                                "tool": "inspect", "mode": "corrected",
                                "arguments": {
                                    "target": {
                                        key: value for key, value in target.items()
                                        if value is not None
                                    },
                                    "max_bytes": retry_budget,
                                },
                            }
                    raise
                if hints and isinstance(payload, dict) and payload.get("status") != "refused":
                    if attach_hints:
                        payload.setdefault("skill_hints", hints)
                    else:
                        payload["skill_hints_withheld"] = {
                            "count": len(hints), "reason": "max_bytes",
                        }
            elif kind == "view":
                payload = _inspect_view(
                    h.conn,
                    h.session,
                    str(target.get("name") or target.get("view") or ""),
                )
            elif kind == "texts":
                from okg.substrate.mcp.text_batch import read_text_batch
                from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes

                def batch_measure(value: dict[str, Any]) -> int:
                    # The page's own served form, without the stamps that
                    # vary per call (freshness ages, an unavailable-freshness
                    # block): cut points depend only on the page, so a
                    # retried token gives the same page while the stamps
                    # keep the reserve the token carries.
                    return public_call_tool_result_bytes(value, tool="inspect", kind="texts")

                # The stamps' cost on this session at their largest freshness
                # outcome (fresh or unavailable, read once), so a freshness
                # failure on a retry or inside this call never moves the cut.
                # The batch reader adds its spare and drift room and keeps a
                # token's reserve while the stamps still fit it. An
                # over-budget page is still refused, never served.
                probe = {"status": "ok", "result_kind": "text_batch", "items": [],
                         "generation_id": h.session.generation_id,
                         "pinned_generation_id": h.session.generation_id,
                         "truncated": True, "continuation_token": "x"}
                stamp_cost = max(0, _worst_case_stamped_bytes(
                    probe, "inspect", {"generation_id": generation_id},
                    tool="inspect", kind="texts",
                ) - batch_measure(probe)) + adjusted_reserve_bytes()
                payload = read_text_batch(
                    h.conn, h.session, target.get("items"), max_bytes=int(max_bytes),
                    continuation_token=target.get("continuation_token"), measure=batch_measure,
                    reserve_bytes=stamp_cost,
                )
            elif kind == "nodes":
                from okg.substrate.mcp.node_batch import read_node_batch
                from okg.substrate.mcp.response_shape import (
                    compact_node_record as _batch_record_shape,
                )
                from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes

                def nodes_measure(value: dict[str, Any]) -> int:
                    # The page's served form without its per-call stamps, as
                    # for `texts`: the cut depends only on the page.
                    return public_call_tool_result_bytes(value, tool="inspect", kind="nodes")

                nodes_probe = {"status": "ok", "result_kind": "node_batch", "records": [],
                               "generation_id": h.session.generation_id,
                               "pinned_generation_id": h.session.generation_id,
                               "truncated": True, "continuation_token": "x"}
                nodes_stamp_cost = max(0, _worst_case_stamped_bytes(
                    nodes_probe, "inspect", {"generation_id": generation_id},
                    tool="inspect", kind="nodes",
                ) - nodes_measure(nodes_probe)) + adjusted_reserve_bytes()
                (
                    batch_select,
                    batch_lineage,
                    batch_lineage_drop,
                    _,
                ) = _lineage_select_plan(select)

                # select is checked once, before any record is read, so a
                # refusal names no record and an empty reference still
                # refuses a field the read does not have.
                from okg.substrate.mcp.tools import validate_node_select

                with select_retry_from(select):
                    validate_node_select(batch_select, tool_name="inspect(nodes)")

                def read_batch_node(node_id: str) -> Optional[dict[str, Any]]:
                    # The single-record read, unchanged: a batch record is
                    # what inspect(node) returns for that id (#3657).
                    with select_retry_from(select):
                        record = _inspect_node(
                            h.conn, h.session, node_id, select=batch_select,
                        )
                    if record is None:
                        return None
                    if batch_lineage:
                        return _move_lineage_to_rows(
                            record, drop_attrs=batch_lineage_drop, single_row=True,
                        )
                    # The attributes a single read leaves out of its reply
                    # are left out before the record is measured, so they
                    # take no share of the budget.
                    return _batch_record_shape(record)

                def batch_members(_ref: str) -> list[str]:
                    # The member check every reference reader runs: fewer
                    # readable rows than stored members refuses
                    # `member_missing` with the count only.
                    _reference_rows(h, None)
                    return [str(member["node_id"]) for member in h.reference_entry.members]

                payload = read_node_batch(
                    h.conn, h.session,
                    node_ids=target.get("node_ids"), ref=batch_ref,
                    resolve_ref=batch_members, read_node=read_batch_node,
                    select=select, max_bytes=int(max_bytes),
                    measure=nodes_measure,
                    continuation_token=target.get("continuation_token"),
                    reserve_bytes=nodes_stamp_cost,
                )
            elif kind == "text":
                from okg.substrate.mcp.record_text import (
                    TEXT_READ_RESERVE_MARGIN,
                    read_record_text,
                )
                from okg.substrate.mcp.response_bounds import (
                    public_call_tool_result_bytes,
                )

                # Issue #2668: a record's chunk text in reading order, paged
                # by the call's resolved byte budget (the wrapper writes it
                # into `max_bytes`, as for `subtype`). `inspect` is not
                # byte-pageable in the wrapper, so an over-budget page would
                # be replaced by a refusal and lose its cursor: size the page
                # against what is stamped on AFTER it (freshness, advanced
                # generation notice), measured on this very session.
                probe = {
                    "status": "ok",
                    "generation_id": h.session.generation_id,
                    "pinned_generation_id": h.session.generation_id,
                    "truncated": True,
                }
                # The same three stamps, in the same order, that the page
                # itself receives: advanced-generation notice, operator and
                # freshness, then the logical-session metadata the byte
                # wrapper renders before it measures.
                # Both measures render the served form of an `inspect` text
                # page (issue #3059). In text the renderer is not additive,
                # so the difference is the stamps' cost on an empty page,
                # with the stale notice rendered as if it will print: an
                # upper bound, and `read_record_text` measures the served
                # page itself before it returns.
                stamp_bytes = public_call_tool_result_bytes(
                    _logical_response_measurement(
                        _stamp_operator(
                            _maybe_stamp_advanced(h, dict(
                                probe,
                                # A path read names the id it resolved to.
                                **({"resolved_path": resolved_path} if resolved_path else {}),
                            )), "inspect"
                        ),
                        {"generation_id": generation_id},
                    ),
                    tool="inspect",
                    kind="text",
                ) - public_call_tool_result_bytes(
                    probe, tool="inspect", kind="text",
                )
                payload = read_record_text(
                    h.conn,
                    h.session,
                    str(target.get("node_id") or ""),
                    continuation_token=target.get("continuation_token"),
                    at=target.get("at"),
                    lines=target.get("lines"),
                    matching=target.get("matching"),
                    context=target.get("context"),
                    max_bytes=int(max_bytes),
                    reserve_bytes=(
                        max(0, stamp_bytes) + TEXT_READ_RESERVE_MARGIN
                        + adjusted_reserve_bytes()
                    ),
                )
            elif kind == "image":
                from okg.substrate.mcp.record_image import (
                    ImageBudget,
                    read_record_image,
                )
                from okg.substrate.mcp.response_bounds import attach_images

                # #3046: the figure a node points at, as MCP image content.
                # The bytes leave through `attach_images`, never the payload,
                # so the byte bound and the call trace see metadata only.
                read = read_record_image(
                    h.conn,
                    h.session,
                    str(target.get("node_id") or ""),
                    source=image_source,
                    panel=target.get("panel"),
                    attr=target.get("attr"),
                    budget=image_budget or ImageBudget(),
                )
                payload = read.payload
                if read.images and not attach_images(
                    [(img.data, img.mime_type) for img in read.images]
                ):
                    # Called in-process, not through the MCP call path: say
                    # the images could not be delivered rather than drop them.
                    payload["images_attached"] = 0
                    payload["note"] = "images rendered but this call path cannot carry image content"
            elif kind == "node":
                node_id = str(target.get("node_id") or "")
                # External-backed hydration (issue #83): opt-in via an optional
                # `hydrate` key on the target (True for all backed fields, or a
                # list of field names). Absent => pinned view only, no fetch.
                # `search`/`expand` deliberately do not offer this.
                hydrate = target.get("hydrate")
                probe_gate = None
                probe_principal = h.session.nomos_subject
                if hydrate:
                    from okg.substrate.external_backed.gating import (
                        build_probe_gate,
                    )

                    probe_gate = build_probe_gate(h.session.deployment_name)
                    if probe_principal is None:
                        # The session carries no subject when read
                        # enforcement is off, but the probe bundle is its own
                        # gate. Resolve the caller once, by the same rule as
                        # every MCP path; unresolved stays None and the probe
                        # is refused by name, never decided as the runtime
                        # service (#2361). A policy-read failure propagates.
                        try:
                            probe_principal = _mcp_subject.resolve_mcp_subject(
                                h.conn,
                                deployment_name=h.session.deployment_name,
                            )
                        except SubstrateError:
                            raise
                        except Exception as exc:  # noqa: BLE001 - named below
                            raise _mcp_subject.mcp_subject_lookup_error(
                                "external.probe.invoke", exc,
                            ) from exc
                # `select=["source_lineage", ...]` (issue #2674).
                (
                    node_select,
                    lineage_requested,
                    lineage_drop_attrs,
                    _,
                ) = _lineage_select_plan(select)
                # A select refusal's ready retry names the caller's own list,
                # not the rewritten one (PACT select-fields-by-method).
                with select_retry_from(select):
                    payload = _inspect_node(
                        h.conn,
                        h.session,
                        node_id,
                        select=node_select,
                        hydrate=hydrate,
                        # The probe acts as the caller's resolved subject,
                        # never the runtime service (#2361).
                        principal=probe_principal,
                        probe_gate=probe_gate,
                    )
                if payload is None:
                    from okg.substrate.mcp.tools import close_node_ids, node_not_visible

                    # expose-mcp-node-lookup req.lookup.inspect-not-found:
                    # absence is an EXPLICIT signal (exists: false). A found
                    # node with sparse attrs still returns its normal
                    # payload, so absent and sparse stay distinguishable.
                    # A source-denied node takes this same path, and
                    # inspect(kind="attr") raises the same refusal (#3189).
                    raise node_not_visible(
                        node_id, h.session.generation_id,
                        close=close_node_ids(h.conn, h.session, node_id),
                    )
                if lineage_requested:
                    payload = _move_lineage_to_rows(
                        payload,
                        drop_attrs=lineage_drop_attrs,
                        single_row=True,
                    )
            elif kind in {"attr", "attribute", "attribute_provenance"}:
                payload = _inspect_attr_provenance(
                    h.conn,
                    h.session,
                    target_kind=str(target.get("target_kind") or "node"),
                    target_id=str(
                        target.get("target_id")
                        or target.get("node_id")
                        or target.get("edge_id")
                        or ""
                    ),
                    attr_key=(
                        str(target["attr_key"])
                        if target.get("attr_key") is not None
                        else None
                    ),
                    namespace=str(target.get("namespace") or "default"),
                )
            elif kind == "session":
                payload = h.session.as_dict()
            elif kind == "generation":
                # Chronos-native resolution (issue #1436). The legacy
                # okg.graph_generations lookup this replaced bound the
                # public checkpoint STRING to a BIGSERIAL column, so a
                # valid id — including the session's own pin — raised
                # instead of resolving.
                payload = _inspect_generation(
                    h.conn,
                    h.session,
                    target.get("generation_id"),
                )
            elif kind == "generation_delta":
                from_generation = (
                    target.get("from_generation")
                    or target.get("generation_id")
                    or h.session.generation_id
                )
                with _open_logical(
                    from_generation,
                    branch_name=h.session.branch_name,
                    branch_id=h.session.branch_id,
                    nomos_subject=h.session.nomos_subject,
                    nomos_identity_claim=h.session.nomos_identity_claim,
                    nomos_access_context=h.session.nomos_access_context,
                ) as logical:
                    payload = _preview_advance(
                        logical.conn,
                        logical.session,
                        to=target.get("to", "latest"),
                    )
            else:
                return _operator_refusal(
                    "inspect",
                    code="unsupported_inspect_target",
                    message=(
                        "inspect target kind must be one of graph, coverage, "
                        "ontology, subtype, node, text, image, attr, session, generation, "
                        "generation_delta, admission, "
                        "record, record_templates, record_prefixes, "
                        "session_start, surface, alias_suggestions"
                    ),
                )
            if (
                resolved_path is not None and isinstance(payload, dict)
                and payload.get("status") != "refused"
            ):
                payload["resolved_path"] = resolved_path
            return _stamp_operator(
                _maybe_stamp_advanced(h, payload),
                "inspect",
            )

    def _expand_served_destinations(
        conn: psycopg.Connection,
        session: Session,
        fetched: list[dict[str, Any]],
        *,
        view: Any,
    ) -> dict[str, dict[str, Any]]:
        """The page's destination nodes as the reader is served them, by id
        (issue #3393). Each passes the node read filter as a COUNT (it decides
        whether its edge is served and is not itself returned), then its
        accepted overlays, as filter and map read a node. A destination
        missing from the result is not served: hidden, or absent at the pin.
        ``READER_VIEW_FORCE`` shadow also runs the view's check on them."""
        stored: dict[str, dict[str, Any]] = {}
        for record in fetched:
            dest_id = record["dest_id"]
            if dest_id is None or str(dest_id) in stored:
                continue
            effective = record["dest_attrs"]
            stored_raw = record["dest_raw_attrs"]
            stored[str(dest_id)] = {
                "node_id": dest_id,
                "subtype": record["dest_subtype"],
                "text_hash": record["dest_text_hash"],
                "attrs": effective or {} if stored_raw is None else stored_raw,
                "_effective_attrs": effective or {},
            }
        if not stored:
            return {}
        rows = list(stored.values())
        served = filter_node_rows_for_session(
            conn, session, copy.deepcopy(rows), runtime_required=True, count=True,
        )
        for row in served:
            row.pop("_effective_attrs", None)
        served = apply_effective_attrs_to_rows(
            conn,
            served,
            target_kind="node",
            target_id_field="node_id",
            generation_id=session.generation_id,
        )
        if view is not None and view.force == "shadow":
            view.finish([
                {key: value for key, value in row.items() if key != "_effective_attrs"}
                for row in rows
            ])
        return {str(row["node_id"]): row for row in served}

    def _expand_one_hop_effective(
        conn: psycopg.Connection, session: Session, node_id: str, **arguments: Any,
    ) -> dict[str, Any]:
        """``_expand_one_hop_effective_within``; when the far-end filter
        carries a date condition, as one phase of that call's date budget
        (``_within_date_budget``). Any other expand goes straight through."""
        budget = _date_budget(
            "expand", arguments.get("dest_filter"), kind="nodes",
            started_at=arguments.pop("started_at", None),
        )
        if budget is None:
            return _expand_one_hop_effective_within(conn, session, node_id, **arguments)
        try:
            return _within_date_budget(
                conn, budget,
                lambda: _expand_one_hop_effective_within(
                    conn, session, node_id, date_budget=budget, **arguments),
                phase="expand_other",
            )
        finally:
            budget.mark("expand_other")

    def _expand_one_hop_effective_within(
        conn: psycopg.Connection,
        session: Session,
        node_id: str,
        *,
        edge_type: Optional[str],
        direction: str,
        dest_filter: Optional[dict[str, Any]],
        order_by: Optional[str],
        limit: int,
        offset: int,
        select: Optional[list[str]],
        date_budget: Any = None,
    ) -> dict[str, Any]:
        """One-hop expand with a destination filter or an order (#3201).

        Reads the effective-attribute relation in SQL: edges incident on the
        anchor, LEFT JOINed to their far endpoint's effective attrs when the
        filter or the order needs it. The anchor, edge type and destination
        filter all compile through the operator compiler with bound
        parameters, so the filter tests the same corrected values `inspect`
        serves for that neighbour; it no longer goes through run_sql.

        Emits the SAME edge-shaped rows as `expand_neighbors` and honors the
        same `select` grammar (issue #903). Rows pass the per-row read check
        before they are served. The neighbour's id is `dst` (or `src` for
        direction='in'). OFFSET pages a total order: the caller's order_by
        over the destination, then the edge id.

        Under Nomos `audit`/`enforce` the destination is read as the reader
        would be served it (issue #3393; PACT read-model-slice7a-expand-3393):
        the destination filter and the order run on the reader's view of the
        destination node (``_reader_view_sql``, as filter and map since read
        model slice 7a), so a hidden or masked value never matches or orders.
        The page's destinations then pass the node read filter (as counts:
        they are not returned) and the filter is re-checked three-valued on
        the served destination; an edge is served only when it is TRUE
        there. A destination the reader is not served - hidden, or absent at
        the pin - is wholly withheld: the filter is unknown on it, so its
        edge is never served under a filter (hidden equals absent), and the
        order reads it as no value. With no active bundle under audit
        (``ReaderView.serves_unredacted``) the SQL is the Nomos-off SQL.
        ``check_bundle_unchanged`` runs after the final read.
        """
        from okg.substrate.mcp.set_operators import _serialize_edge_items
        from okg.substrate.mcp.tools import (
            _LIST_NEIGHBORS_ALWAYS,
            _LIST_NEIGHBORS_DEFAULT,
            _LIST_NEIGHBORS_FIELDS,
        )

        if direction not in {"out", "in"}:
            raise InvalidOperatorArguments("expand: direction must be 'out' or 'in'")
        validate_select(select, _LIST_NEIGHBORS_FIELDS, tool_name="expand",
                        elsewhere=EXPAND_SELECT_ELSEWHERE)
        limit, offset = int(limit), int(offset)
        near, far = ("src", "dst") if direction == "out" else ("dst", "src")
        needs_node = bool(dest_filter) or bool(order_by)
        _require_effective_view(conn, session, "edge")
        if needs_node:
            _require_effective_view(conn, session, "node")
        declared = _declared_registry_for(conn)
        # One reading of the mode decides the reader view, the destination
        # check, the read filter and the overlay step (issue #3393).
        nomos_runtime = deployment_requires_nomos_runtime(session.deployment_name)
        edge_rel, node_rel = "expand_edge", "expand_node"
        params: dict[str, Any] = {}
        base_table, source_clause, gen_where, gen_params = _effective_view_source(
            conn, session.generation_id, target_kind="edge",
        )
        from_sql = _effective_view_runtime_source(
            base_table=base_table,
            source_clause=source_clause,
            gen_where=gen_where,
            gen_params=gen_params,
            target_kind="edge",
            generation_id=session.generation_id,
            relation=edge_rel,
            params=params,
            target_kind_param="tk_edge",
        )
        # The anchor's edges alone, before the far endpoint is joined: the
        # far ids of these edges bound the destinations a date condition
        # can compare (#3666).
        edge_from_sql = from_sql
        if needs_node:
            base_table, source_clause, gen_where, gen_params = _effective_view_source(
                conn, session.generation_id, target_kind="node",
            )
            node_from = _effective_view_runtime_source(
                base_table=base_table,
                source_clause=source_clause,
                gen_where=gen_where,
                gen_params=gen_params,
                target_kind="node",
                generation_id=session.generation_id,
                relation=node_rel,
                params=params,
                target_kind_param="tk_node",
            )
            # LEFT JOIN so the filtered and unfiltered paths range over the
            # same edges: an edge whose far endpoint is absent at the pin
            # stays visible, and a filter decides it on empty attrs, exactly
            # as the Python evaluator does.
            from_sql += (
                f" LEFT JOIN {node_from} ON {node_rel}.target_id = {edge_rel}.{far}"
            )
        view = None
        readable_sql: Callable[[str], str] | None = None
        placed: str | None = None
        if nomos_runtime and needs_node:
            # Issue #3393: the destination is read through the reader's view
            # (class mode: the stored-row LATERAL join; a join miss, an
            # absent destination included, is undecided). `placed` is TRUE only where the view
            # knows the destination is served and how; everywhere else every
            # field of it - attributes and stable fields alike - is
            # undecided in SQL, so no stored value of a destination the view
            # cannot place decides which edges are kept or how they sort.
            view, reader_join, readable_sql = _reader_view_sql(
                conn, session, target_kind="node", relation=node_rel, params=params,
            )
            from_sql += reader_join
            placed = _expand_destination_placed_sql(
                view, _READER_ROW_ALIAS if reader_join else node_rel, conn,
                relation=node_rel,
            )
            if view.serves_unredacted:
                # No active bundle under audit: a placed destination is
                # served unredacted, so its clauses are the Nomos-off ones.
                readable_sql = None
        anchor: dict[str, Any] = {"op": "eq", "field": near, "value": node_id}
        # `is not None`, matching expand_neighbors: under truthiness an
        # edge_type of "" filters on one path and is ignored on the other.
        if edge_type is not None:
            anchor = {
                "op": "and",
                "clauses": [
                    anchor,
                    {"op": "eq", "field": "edge_type", "value": str(edge_type)},
                ],
            }
        edge_compiled = _compile_operator_predicate(
            "edges", anchor, params, declared=declared, relation=edge_rel,
        )
        if edge_compiled is None:
            raise ValueError("expand: the anchor or edge_type cannot be compiled")
        edge_sql = edge_compiled.bind_into(params)
        # #3666: date conditions on the far end compile like any other. The
        # stored values read to do so are those of this anchor's neighbours.
        date_report = _DateConditionReport()
        caller_dest_filter = dest_filter
        # One budget for the far-end filter's date work; None without one.
        expand_budget = date_budget
        if _holds_date_condition(dest_filter):
            dest_filter = _resolve_date_conditions(
                conn, session, kind="nodes", predicate=dest_filter,
                operator="expand", report=date_report,
                nomos_runtime=nomos_runtime, budget=expand_budget,
                candidates_sql=(
                    f"SELECT {edge_rel}.{far} FROM {edge_from_sql} "
                    f"WHERE {edge_rel}.target_kind = %(tk_edge)s "
                    f"AND {edge_rel}.namespace = 'default' AND ({edge_sql})"
                ),
                candidate_params=params,
            )
        dest_compiled = _compile_operator_predicate(
            "nodes",
            dest_filter,
            params,
            declared=declared,
            relation=node_rel,
            nullable=True,
            readable=readable_sql,
            fingerprint_of=caller_dest_filter,
        )
        if dest_compiled is None:
            from okg.substrate.mcp.operator_sql import declared_bound_mismatch

            # A range bound of the wrong type for its attribute's declared
            # type is named, with the form that works (issue #3666, 2c).
            mismatch = declared_bound_mismatch(caller_dest_filter, declared)
            err = SubstrateError(
                f"expand: {mismatch[0]}" if mismatch else
                "expand: the destination filter cannot be lowered exactly to SQL",
                data={"filter": caller_dest_filter},
                fix_hint=mismatch[1] if mismatch else (
                    "use a supported predicate shape; broad Python fallback "
                    "is intentionally disabled"
                ),
            )
            err.code = "unsupported_operator_form"
            raise err
        dest_sql = dest_compiled.bind_into(params)
        dest_where = f" AND ({dest_sql})"
        if dest_filter and placed is not None:
            # Drop an edge whose destination the view knows is hidden (it is
            # never served under a filter); keep one whose destination is
            # undecided whatever it stores; on a placed destination keep it
            # where the readable-guarded filter IS TRUE or a key it reads is
            # undecided. The check on the served destination decides.
            if readable_sql is not None:
                tail = _reader_view_tail(
                    predicate=dest_filter, predicate_sql=dest_sql, readable=readable_sql,
                )
                dest_where = (
                    f" AND (({placed}) IS NOT FALSE) AND (({placed}) IS NULL OR {tail})"
                )
            else:
                # No-bundle audit: a placed destination is served unredacted
                # (never FALSE here), so the Nomos-off clause decides it.
                dest_where = f" AND (({dest_sql}) OR ({placed}) IS NULL)"
        order_notes: dict[str, Any] = {}
        order_sql = (
            _effective_view_order_sql(
                kind="nodes",
                order_by=order_by,
                params=params,
                declared=declared,
                relation=node_rel,
                tiebreak=f"({edge_rel}.target_id)::bigint ASC",
                readable=readable_sql,
                row_guard=placed,
                # #3666 (2c): an undeclared attribute sorts as text, with
                # the access rules off only, as on filter.
                untyped_as_text=not nomos_runtime,
                notes=order_notes,
            )
            if order_by
            else f" ORDER BY ({edge_rel}.target_id)::bigint ASC"
        )
        params["page_limit"] = max(1, limit) + 1
        params["page_offset"] = max(0, offset)
        from_where_sql = (
            f"FROM {from_sql} "
            f"WHERE {edge_rel}.target_kind = %(tk_edge)s "
            f"AND {edge_rel}.namespace = 'default' "
            f"AND ({edge_sql})" + dest_where
        )
        # The served-destination check runs under a filter, and under the
        # shadow view also for an order alone (so shadow compares the class
        # path with the per-row filter on every destination the SQL read).
        check_destinations = nomos_runtime and view is not None and (
            bool(dest_filter) or view.force == "shadow"
        )
        dest_columns = (
            f", {node_rel}.target_id AS dest_id, {node_rel}.subtype AS dest_subtype, "
            f"{node_rel}.raw_text_hash AS dest_text_hash, {node_rel}.attrs AS dest_attrs"
            if dest_filter or check_destinations
            else ""
        )
        if check_destinations:
            dest_columns += f", {node_rel}.raw_attrs AS dest_raw_attrs"
        sql = (
            f"SELECT {edge_rel}.target_id, {edge_rel}.src, {edge_rel}.dst, "
            f"{edge_rel}.edge_type, {edge_rel}.attrs, {edge_rel}.raw_attrs"
            + dest_columns
            + f" {from_where_sql}"
            + order_sql
            + " LIMIT %(page_limit)s OFFSET %(page_offset)s"
        )
        def read_page() -> list:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, params)
                return cur.fetchall()

        # #3666: a far-end filter that carries a date condition runs under
        # what is left of its date budget; any other expand is as before.
        if expand_budget is None:
            fetched = read_page()
        else:
            expand_budget.mark("page_setup")
            try:
                fetched = _under_date_budget(
                    conn, expand_budget, "page", read_page, phase="page_statement",
                )
            finally:
                expand_budget.mark("page_statement")
        raw_has_more = len(fetched) > max(1, limit)
        fetched = fetched[: max(1, limit)]
        served_destinations: dict[str, dict[str, Any]] = {}
        if check_destinations:
            served_destinations = _expand_served_destinations(
                conn, session, fetched, view=view,
            )
        raw_rows: list[dict[str, Any]] = []
        for record in fetched:
            if dest_filter and nomos_runtime:
                # Issue #3393: the filter on the destination as the reader is
                # served it, three-valued (a withheld value never matches). A
                # destination not served (hidden, or absent at the pin) is
                # wholly withheld, so the filter is unknown and the edge is
                # not served: hidden and absent answer alike.
                served = (
                    served_destinations.get(str(record["dest_id"]))
                    if record["dest_id"] is not None
                    else None
                )
                if served is None or _operator_predicate_verdict(
                    served, dest_filter, declared=declared
                ) is not True:
                    continue
            elif dest_filter:
                # Bounded parity check on the page only: the destination as
                # the Python evaluator reads it agrees with the compiled SQL.
                destination = {
                    "node_id": record["dest_id"],
                    "subtype": record["dest_subtype"],
                    "text_hash": record["dest_text_hash"],
                    "attrs": record["dest_attrs"] or {},
                }
                if not _operator_predicate_matches(
                    destination, dest_filter, declared=declared
                ):
                    continue
            effective = record["attrs"]
            stored_raw = record["raw_attrs"]
            raw_rows.append(
                {
                    # The graph's bigint edge id, as expand_neighbors emits it.
                    "edge_id": _graph_edge_id(record["target_id"]),
                    "src": record["src"],
                    "dst": record["dst"],
                    "edge_type": record["edge_type"],
                    "attrs": effective or {} if stored_raw is None else stored_raw,
                    "_effective_attrs": effective or {},
                    # Per row, not the requested direction: a self-loop is
                    # "out" on the unfiltered path, and the two must agree.
                    "direction": "out" if record["src"] == node_id else "in",
                }
            )
        rows = filter_edge_rows_for_session(
            conn, session, raw_rows, runtime_required=nomos_runtime,
        )
        if not nomos_runtime:
            for row in rows:
                row["attrs"] = row.pop("_effective_attrs", row.get("attrs") or {})
        else:
            for row in rows:
                row.pop("_effective_attrs", None)
            rows = apply_effective_attrs_to_rows(
                conn,
                rows,
                target_kind="edge",
                target_id_field="edge_id",
                generation_id=session.generation_id,
            )
        # Serialize before projecting, exactly as expand_neighbors does, so
        # `read_provenance` is attached to the full row and survives the same
        # projection rules rather than being back-filled afterwards.
        rows = _serialize_edge_items(conn, session, rows)
        neighbors = []
        for row in rows:
            projected = project_row(
                row, select,
                default=_LIST_NEIGHBORS_DEFAULT,
                always=_LIST_NEIGHBORS_ALWAYS,
            )
            if "attrs" in projected and "read_provenance" not in projected:
                projected["read_provenance"] = row.get("read_provenance")
            neighbors.append(projected)
        from okg.substrate.mcp.tools import attach_neighbor_summaries

        attach_neighbor_summaries(
            conn, session, rows, neighbors, runtime_required=nomos_runtime,
        )
        if view is not None:
            # After the last read (the neighbours' titles): a bundle activated
            # or changed since the view opened would serve edges the SQL kept
            # and ordered on destination values that bundle now masks, and
            # titles read under a different bundle than the page.
            view.check_bundle_unchanged()
        count_fields = _operator_count_fields(
            conn,
            session,
            offset=offset,
            shown=len(neighbors),
            page_meta={
                "from_where_sql": from_where_sql,
                "params": params,
                "raw_page_rows": len(fetched),
                "raw_has_more": raw_has_more,
                "date_budget": expand_budget,
            },
        )
        return {
            "node_id": node_id,
            "direction": direction,
            "neighbors": neighbors,
            "returned_count": len(neighbors),
            **count_fields,
            "offset": offset,
            "limit": limit,
            # Raw positions consumed: a row the read check hid still occupies
            # its OFFSET position, so the next page starts after it.
            "next_offset": offset + len(fetched) if raw_has_more else None,
            "truncated": raw_has_more,
            "generation_id": session.generation_id,
            "pinned_generation_id": session.generation_id,
            "predicate_fingerprint": dest_compiled.fingerprint,
            **date_report.payload(),
            **(
                {"sorted_as_text": order_notes["sorted_as_text"]}
                if order_notes.get("sorted_as_text") else {}
            ),
            **_compared_as_stored(caller_dest_filter, declared),
        }

    def _expand_records_effective(
        conn: psycopg.Connection, session: Session, node_id: str, **arguments: Any,
    ) -> dict[str, Any]:
        """``_expand_records_effective_within``; when the far-record
        condition carries a date condition, as one phase of that call's date
        budget (``_within_date_budget``), as the link-row read does. Any
        other call goes straight through."""
        budget = _date_budget(
            "expand", arguments.get("dest_filter"), kind="nodes",
            started_at=arguments.pop("started_at", None),
        )
        if budget is None:
            return _expand_records_effective_within(conn, session, node_id, **arguments)
        try:
            return _within_date_budget(
                conn, budget,
                lambda: _expand_records_effective_within(
                    conn, session, node_id, date_budget=budget, **arguments),
                phase="expand_other",
            )
        finally:
            budget.mark("expand_other")

    def _expand_records_effective_within(
        conn: psycopg.Connection,
        session: Session,
        node_id: str,
        *,
        edge_type: Optional[str],
        direction: str,
        dest_filter: Optional[dict[str, Any]],
        order_by: Optional[str],
        limit: int,
        offset: int,
        select: Optional[list[str]],
        date_budget: Any = None,
    ) -> dict[str, Any]:
        """The records linked to a node, with a condition or an order on them
        (issue #3666).

        The far ends of the anchor's links are an id set; the page over it
        is the one `filter` reads (`_effective_view_sql_page`). With access
        rules off the set is a subquery (`member_sql`), so nothing is
        budgeted and the count is the SQL count of distinct far records.
        Under audit and enforce it is the far ends of the visible links
        within the link budget (`tools.expand_far_links`, `member_ids`),
        because a link's visibility is decided per row. So the condition and
        the order run on the reader's view of each record, an offset counts
        served records, a record the reader may not see is neither returned
        nor counted, and the count follows `_operator_count_fields`. No
        second access check is made here. Rows are shaped exactly as the
        unconditioned read shapes them (`tools.expand_record_rows`).

        A date condition (`date_budget`, from `_expand_records_effective`)
        is resolved as the link-row read resolves it
        (`_resolve_date_conditions`): the stored date forms are read first,
        over this anchor's far records only, and never remembered; under
        audit and enforce nothing is read and ISO-8601 values alone are
        compared. The page then runs under what is left of the budget, and
        the reply carries the date notes.
        """
        from okg.substrate.mcp.passage_rollup import resolve_neighbor_summaries
        from okg.substrate.mcp.tools import (
            expand_anchor_sql,
            expand_far_links,
            expand_link_attrs,
            expand_page_links,
            expand_read_links,
            expand_record_rows,
            expand_records_link_targets,
            expand_records_page,
            expand_record_wants_attrs,
            validate_expand_record_select,
        )

        validate_expand_record_select(select)
        # Refuses a direction that is not in, out or both, on either path.
        expand_anchor_sql(node_id, edge_type, direction)
        limit, offset = int(limit), int(offset)
        nomos_runtime = deployment_requires_nomos_runtime(session.deployment_name)
        links: dict[str, list[dict[str, Any]]] = {}
        link_counts: Optional[dict[str, int]] = None
        over_budget = False
        members: dict[str, Any]
        if nomos_runtime:
            # Audit and enforce: a link's visibility is decided per row, so
            # the far ends are found in Python within the link budget.
            links, over_budget = expand_far_links(
                conn, session, node_id, edge_type=edge_type, direction=direction,
                runtime_required=nomos_runtime,
            )
            members = {"member_ids": sorted(links)}
        else:
            # Access rules off: the far ends stay a subquery of the page, so
            # the condition, the order, the offset and the count all run in
            # SQL over every far record. No link budget.
            _require_effective_view(conn, session, "edge")

            def far_ends(params: dict[str, Any]) -> str:
                base_table, source_clause, gen_where, gen_params = (
                    _effective_view_source(
                        conn, session.generation_id, target_kind="edge",
                    )
                )
                rel = "expand_link"
                link_from = _effective_view_runtime_source(
                    base_table=base_table,
                    source_clause=source_clause,
                    gen_where=gen_where,
                    gen_params=gen_params,
                    target_kind="edge",
                    generation_id=session.generation_id,
                    relation=rel,
                    params=params,
                    target_kind_param="tk_expand_link",
                )
                params["expand_anchor"] = node_id
                near = {
                    "out": f"{rel}.src = %(expand_anchor)s",
                    "in": f"{rel}.dst = %(expand_anchor)s",
                    "both": (
                        f"({rel}.src = %(expand_anchor)s "
                        f"OR {rel}.dst = %(expand_anchor)s)"
                    ),
                }[direction]
                if edge_type is not None:
                    params["expand_link_type"] = str(edge_type)
                    near += f" AND {rel}.edge_type = %(expand_link_type)s"
                return (
                    f"SELECT CASE WHEN {rel}.src = %(expand_anchor)s "
                    f"THEN {rel}.dst ELSE {rel}.src END "
                    f"FROM {link_from} "
                    f"WHERE {rel}.target_kind = %(tk_expand_link)s "
                    f"AND {rel}.namespace = 'default' AND {near}"
                )

            members = {"member_sql": far_ends}
        declared = _declared_registry_for(conn)
        # #3666: date conditions on the far record compile like any other.
        # The stored forms read to do so are those of this anchor's far
        # records (with access rules off; under them nothing is read).
        date_report = _DateConditionReport()
        caller_dest_filter = dest_filter
        if _holds_date_condition(dest_filter):
            candidate_params: dict[str, Any] = {}
            dest_filter = _resolve_date_conditions(
                conn, session, kind="nodes", predicate=dest_filter,
                operator="expand", report=date_report,
                nomos_runtime=nomos_runtime, budget=date_budget,
                candidates_sql=(
                    "" if nomos_runtime else members["member_sql"](candidate_params)
                ),
                candidate_params=candidate_params,
            )

        def kept(chunk: list[dict[str, Any]]) -> list[dict[str, Any]]:
            if not dest_filter:
                return chunk
            return [
                row for row in chunk
                if _operator_read_check(
                    row, dest_filter, declared=declared, nomos_runtime=nomos_runtime,
                )
            ]

        page_facts: dict[str, Any] = {}
        page_meta: dict[str, Any] = {}
        # An anchor with no visible link still runs the page (over no ids), so
        # its reply has the keys of one whose far records are all hidden.
        records, has_more = _effective_view_sql_page(
            conn,
            session,
            kind="nodes",
            predicate=dest_filter or None,
            fingerprint_predicate=caller_dest_filter or None,
            date_budget=date_budget,
            limit=limit,
            offset=offset,
            order_by=order_by,
            nomos_runtime=nomos_runtime,
            residual=kept,
            page_facts=page_facts,
            page_meta=page_meta,
            **members,
        )
        records = kept(records)
        if not nomos_runtime:
            # The page's own links: each row's shown link and its link count,
            # grouped in SQL for these records only.
            links, link_counts = expand_page_links(
                conn, session, node_id,
                [str(record["node_id"]) for record in records],
                edge_type=edge_type, direction=direction,
            )
        count_fields = _operator_count_fields(
            conn, session, offset=offset, shown=len(records), page_meta=page_meta,
        )
        wanted = set(select or ())
        if expand_record_wants_attrs(select):
            records = _operator_fence_node_rows(
                conn, session, kind="nodes", rows=records,
            )
        titles = resolve_neighbor_summaries(
            conn, session=session,
            node_ids=[str(record["node_id"]) for record in records],
            runtime_required=nomos_runtime,
            # The page read these records once already (its read pass); the
            # titles are theirs, so this pass records no second read.
            count=True,
        )
        expand_read_links(
            conn, session, records, links, runtime_required=nomos_runtime,
        )
        rows = expand_record_rows(
            records, links, select, titles=titles,
            link_attrs=(
                expand_link_attrs(conn, session, records, links)
                if "link.attrs" in wanted else {}
            ),
            link_counts=link_counts,
        )
        payload = expand_records_page(
            session, node_id, direction=direction, rows=rows, limit=limit,
            offset=offset, has_more=has_more, over_budget=over_budget,
            count_fields=count_fields,
        )
        payload["predicate_fingerprint"] = page_meta.get("predicate_fingerprint")
        # Issue #3666: a condition narrows the page, so the counts are the
        # page's own, never the record's whole citation list.
        payload.update(expand_records_link_targets(
            session, rows, titles=titles, runtime=nomos_runtime, complete=False,
        ))
        payload.update(date_report.payload())
        if order_by:
            payload["order_by"] = order_by
        if page_meta.get("sorted_as_text"):
            # #3666 (2c): the order key has no declared type, so the page is
            # in text order; the reply says so, as the link-row read does.
            payload["sorted_as_text"] = page_meta["sorted_as_text"]
        # #3666 (2c): an `eq`/`in` operand of another JSON type than the
        # declared attribute matches only a stored value of its own type.
        payload.update(_compared_as_stored(caller_dest_filter, declared))
        bound = page_facts.get("read_filter_scan_bound")
        if bound is not None:
            # The scan stopped before the end of the rows this reader may
            # read (#3208): no cursor, and the page says why.
            payload["next_offset"] = None
            payload.update(_read_filter_bound_notice(bound))
        return payload

    def _expand_graph(
        input: ExpandInput,
        edge_type: Optional[str] = None,
        direction: declared(str, enum=list(DIRECTIONS)) = "out",
        path: Optional[
            declared(list[str], minItems=1, maxItems=MAX_PATH_HOPS)
        ] = None,
        depth: declared(int, minimum=1, maximum=MAX_DEPTH) = 1,
        target_subtype: Optional[str] = None,
        continuation_token: Optional[str] = None,
        filter: Optional[PredicateInput] = None,
        generation_id: Optional[Union[int, str]] = None,
        limit: declared(int, minimum=1, maximum=MAX_PAGE_LIMIT) = 50,
        offset: declared(int, minimum=0, maximum=MAX_OFFSET) = 0,
        select: Optional[list[str]] = None,
        order_by: Optional[str] = None,
        include_siblings: bool = False,
        authority: Optional[dict[str, Any]] = None,
        ctx: Context | None = None,
        caller_select: Optional[list[str]] = None,
        max_bytes: Optional[int] = None,
    ) -> dict[str, Any]:
        """Follow graph edges from a node through one hop or a bounded path.

        Three shapes, chosen by what is supplied: a typed `path` (hops
        spelled 'edge_type:direction' or '*:direction'), a uniform `depth>1`
        repeating one edge_type/direction, or the 1-hop neighbour lookup.
        Only 1-hop takes `filter`, `offset` and `order_by` (on the
        neighbour node); the others refuse
        them and resume by continuation_token. `caller_select` (the select
        before a `source_lineage` rewrite) and `max_bytes` (the served
        budget) go into a path page's signed binding, so `next` rebuilds the
        same call. `target_subtype` filters only
        the terminal node, so a terminal-empty path stays distinguishable
        from no path.

        `include_siblings=True` adds policy-filtered same-subtype neighbours
        through catalog-declared self-relations, telling undeclared structure
        apart from a declared empty relation.
        """
        date_started = _date_call_started()
        input = as_mapping(input)
        filter = as_mapping(filter)
        node_id = _node_id_from_input(input)
        binding_select = select if caller_select is None else caller_select
        one_hop = path is None and int(depth) <= 1
        # '*' is any link, as in a path hop: the readers take it as None.
        edge_type = any_link(edge_type)
        from okg.substrate.mcp.text_match import holds_text_clause

        if one_hop and direction in ("out", "in") and not holds_text_clause(filter):
            # `run`'s set is the far-end condition every one-hop branch below
            # reads: the far record's type first, then the filter. A far-end
            # filter with a text clause is recorded once that clause is the
            # matched ids (`_expand_text_filter`, below), so a later "$n"
            # reads exactly the expanded set (#3666).
            recorded = filter or None
            if target_subtype is not None:
                of_type = {"op": "eq", "field": "subtype", "value": str(target_subtype)}
                recorded = {"op": "and", "clauses": [of_type, recorded]} if recorded else of_type
            _run_steps.note_set("expand", node_id, edge_type, direction, recorded)
        offset = validate_expand_offset(offset)
        if (filter or offset or order_by) and not one_hop:
            with _open(generation_id, authority=authority) as h:
                raise stamp_path_error(
                    InvalidExpandShape(
                        "expand: filter/offset/order_by apply to the 1-hop shape only",
                        data={
                            "requested_shape": "path" if path is not None else "depth"
                        },
                        fix_hint=(
                            "remove filter/offset/order_by, or use the one-hop shape"
                        ),
                    ),
                    session=h.session,
                    authority=path_authority_summary(h.conn, h.session),
                )
        # Issue #3666: a 1-hop row is the linked record. Only a `select` made
        # of the link's own old field names asks for link rows, and that call
        # runs the two link reads below exactly as it did. The caller's own
        # `select` decides (before any `source_lineage` rewrite), so every
        # page of one call is one shape.
        from okg.substrate.mcp.tools import expand_select_is_links

        if one_hop and not expand_select_is_links(binding_select):
            refuse_substring_contains("expand", filter)
            with _open(generation_id, authority=authority) as h:
                if continuation_token is not None:
                    raise stamp_path_error(
                        InvalidExpandShape(
                            "expand: continuation_token requires path or depth>1",
                            data={"requested_shape": "one_hop"},
                            fix_hint=(
                                "supply path/depth>1 or remove continuation_token; "
                                "a 1-hop read pages by offset"
                            ),
                        ),
                        session=h.session,
                        authority=path_authority_summary(h.conn, h.session),
                    )
                # An edge type the catalog lacks is refused here as on the
                # link routes, never read as an empty page (#3666).
                validate_one_hop_edge_type(h.conn, h.session, edge_type, direction)
                far: Optional[dict[str, Any]] = filter or None
                if target_subtype is not None:
                    of_type = {
                        "op": "eq", "field": "subtype", "value": str(target_subtype),
                    }
                    far = {"op": "and", "clauses": [of_type, far]} if far else of_type
                # A text clause (#3666) becomes `node_id` in the far records
                # whose text matches, read before any record row; under
                # access rules it is refused plainly, before any text is read.
                far, text_facts, text_condition = _expand_text_filter(
                    h.conn, h.session, node_id, filter=far,
                    edge_type=edge_type, direction=direction,
                )
                if text_condition is not None and direction in ("out", "in"):
                    # `run`'s set is the far records the text matched, under
                    # the rest of the far-end condition; never inlined.
                    _run_steps.note_set(
                        "expand", node_id, edge_type, direction, far, text=True,
                    )
                record_select = select
                if text_condition is not None and isinstance(select, list):
                    # The match fields are the server's, not a record column
                    # (as on filter's rows).
                    from okg.substrate.mcp.text_match import MATCH_FIELDS

                    record_select = [
                        name for name in select if name not in MATCH_FIELDS
                    ] or None
                if far or order_by:
                    payload = _expand_records_effective(
                        h.conn,
                        h.session,
                        node_id,
                        edge_type=edge_type,
                        direction=direction,
                        dest_filter=far,
                        order_by=order_by,
                        limit=limit,
                        offset=offset,
                        select=record_select,
                        started_at=date_started,
                    )
                else:
                    from okg.substrate.mcp.tools import expand_records

                    payload = expand_records(
                        h.conn,
                        h.session,
                        node_id,
                        edge_type=edge_type,
                        direction=direction,
                        limit=limit,
                        offset=offset,
                        select=select,
                    )
                if text_condition is not None:
                    _expand_text_row_fields(
                        h.conn, h.session, text_condition, payload["neighbors"],
                        select=select,
                    )
                    payload["text_match"] = text_facts
                if include_siblings:
                    payload["siblings"] = _expand_sibling_block(h, node_id)
                return _stamp_operator(_maybe_stamp_advanced(h, payload), "expand")
        if one_hop and isinstance(select, list):
            # On a link row `link.attrs` is the row's own `attrs`.
            select = ["attrs" if name == "link.attrs" else name for name in select]
        # `offset` is a paging parameter, NOT a routing signal (issue #903).
        # It used to appear in this guard, and because 0 is falsy, page 1 and
        # page 2 of one result set were served by different implementations
        # emitting different row shapes. Routing now depends only on the
        # capabilities a request actually needs — a destination predicate or an
        # ordering — so every page of a result set comes from one place.
        if filter or order_by:
            refuse_substring_contains("expand", filter)
            if target_subtype is not None:
                # The far record's type is part of the far-end condition, on
                # link rows as on record rows (#3666), so the text scan and
                # its hint read the same type, and an ordered call with no
                # other condition keeps only links to that type.
                typed = {"op": "eq", "field": "subtype", "value": str(target_subtype)}
                filter = {"op": "and", "clauses": [typed, filter]} if filter else typed
            with _open(generation_id, authority=authority) as h:
                validate_one_hop_edge_type(h.conn, h.session, edge_type, direction)
                filter, text_facts, text_condition = _expand_text_filter(
                    h.conn, h.session, node_id, filter=filter,
                    edge_type=edge_type, direction=direction,
                )
                if text_condition is not None and one_hop:
                    _run_steps.note_set(
                        "expand", node_id, edge_type, direction, filter, text=True,
                    )
                payload = _expand_one_hop_effective(
                    h.conn,
                    h.session,
                    node_id,
                    edge_type=edge_type,
                    direction=direction,
                    dest_filter=filter,
                    order_by=order_by,
                    limit=limit,
                    offset=offset,
                    select=select,
                    started_at=date_started,
                )
                if text_facts is not None:
                    payload["text_match"] = text_facts
                if include_siblings:
                    payload["siblings"] = _expand_sibling_block(h, node_id)
                return _stamp_operator(
                    _maybe_stamp_advanced(h, payload),
                    "expand",
                )
        with _open(generation_id, authority=authority) as h:
            if path is not None or int(depth) > 1:
                try:
                    validate_path_limit(limit)
                except SubstrateError as exc:
                    raise stamp_path_error(
                        exc,
                        session=h.session,
                        authority=path_authority_summary(h.conn, h.session),
                    ) from exc
            if path is not None:
                try:
                    parsed_hops = _parse_path_hops(
                        path, default_direction=direction,
                    )
                except SubstrateError as exc:
                    raise stamp_path_error(
                        exc,
                        session=h.session,
                        authority=path_authority_summary(h.conn, h.session),
                    ) from exc
                payload = _expand_path(
                    h.conn,
                    h.session,
                    node_id,
                    hops=parsed_hops,
                    target_subtype=target_subtype,
                    limit=limit,
                    select=select,
                    continuation_token=continuation_token,
                    caller_select=binding_select,
                    max_bytes=max_bytes,
                )
            elif int(depth) > 1:
                if direction not in ("out", "in"):
                    raise stamp_path_error(
                        InvalidPathDirection(
                            "expand: hop 0 direction must be 'out' or 'in'",
                            data={
                                "hop_index": 0,
                                "edge_type": edge_type,
                                "direction": direction,
                            },
                            fix_hint="replace the direction with 'out' or 'in'",
                        ),
                        session=h.session,
                        authority=path_authority_summary(h.conn, h.session),
                    )
                if int(depth) > 3:
                    raise stamp_path_error(
                        InvalidPathGrammar(
                            "expand: depth exceeds the maximum of 3 hops",
                            data={"hop_index": 3, "max_hops": 3, "depth": int(depth)},
                            fix_hint="reduce depth to at most 3",
                        ),
                        session=h.session,
                        authority=path_authority_summary(h.conn, h.session),
                    )
                hops = [(edge_type, direction)] * int(depth)
                payload = _expand_path(
                    h.conn,
                    h.session,
                    node_id,
                    hops=hops,
                    target_subtype=target_subtype,
                    limit=limit,
                    select=select,
                    continuation_token=continuation_token,
                    caller_select=binding_select,
                    max_bytes=max_bytes,
                )
            else:
                if target_subtype is not None or continuation_token is not None:
                    raise stamp_path_error(
                        InvalidExpandShape(
                            "expand: target_subtype/continuation_token require path or depth>1",
                            data={"requested_shape": "one_hop"},
                            fix_hint=(
                                "supply path/depth>1 or remove target_subtype and continuation_token"
                                + (
                                    "; to read this record's whole text in order, "
                                    'call inspect(target={"kind":"text","node_id":'
                                    + _json.dumps(node_id)
                                    + "})"
                                    if target_subtype == "document_chunk"
                                    else ""
                                )
                            ),
                        ),
                        session=h.session,
                        authority=path_authority_summary(h.conn, h.session),
                    )
                validate_one_hop_edge_type(h.conn, h.session, edge_type, direction)
                payload = _expand_neighbors(
                    h.conn,
                    h.session,
                    node_id,
                    edge_type=edge_type,
                    direction=direction,
                    limit=limit,
                    offset=offset,
                    select=select,
                )
            if include_siblings:
                payload["siblings"] = _expand_sibling_block(h, node_id)
            return _stamp_operator(_maybe_stamp_advanced(h, payload), "expand")

    def _each_values_from_kept(
        argument: Any, source: Mapping[str, Any], authority: Any,
    ) -> tuple[list[Any], dict[str, Any]]:
        """`each={arg: {"ref": <kept live answer>, "field": <column>}}`: that
        column's distinct values, in row order (a wildcard
        listing feeds per-item size lookups), and the listing's
        relation and arguments, which a total over the answer names."""
        from okg.substrate.external_backed.live_dispatch import LiveArgumentsInvalid

        ref, field = source.get("ref"), source.get("field")
        if set(source) != {"ref", "field"} or not _live_results.is_live_id(ref) \
                or not isinstance(field, str) or not field:
            raise LiveArgumentsInvalid(
                "live.each takes a list of values, or {\"ref\": <kept live "
                "answer id>, \"field\": <its column>}",
                data={"argument": str(argument)},
                fix_hint='e.g. each={"item": {"ref": "l1...", "field": "item"}}',
            )
        state = _live_open("expand", ref_id=str(ref), authority=authority)
        values: list[Any] = []
        seen: set[str] = set()
        for row in state["rows"]:
            value = row.get(field) if isinstance(row, Mapping) else None
            if value is None:
                continue
            text = _json.dumps(value, sort_keys=True, default=str)
            if text not in seen:
                seen.add(text)
                values.append(value)
        if not values:
            raise LiveArgumentsInvalid(
                f"live.each: the kept answer has no value in {field!r}",
                data={"argument": str(argument), "field": field},
                fix_hint="name a column the kept answer's rows carry",
            )
        listing = state["set"]
        return values, {"relation": listing.relation_name, "query": dict(listing.query or {})}

    def _prepare_live_for_expand(
        mcp_server: Any,
        live: Any,
        *,
        node_id: str,
        generation_id: Any,
        authority: Any,
    ) -> Any:
        """THE production call site for a live relation (issue #2013).

        Everything `live_dispatch` exposes is reached from here and nowhere
        else. The previous shape refused the parameter when no binding
        existed and then let `expand` continue as an ordinary graph read,
        which meant a deployment that DID bind a relation silently got graph
        rows back -- the silent fallback this module's own comments forbid.

        Order: refuse an unbound deployment, resolve the relation from the
        node's subtype, validate the arguments; then hand back a prepared
        call. The byte-bound wrapper sizes the graph step with its reserve,
        then runs it exactly once (issue #2141). Each database read opens and
        closes its own connection, so nothing is held across the adapter
        call.
        """
        from okg.substrate.external_backed.live_dispatch import (
            LiveRelationUndeclared,
            build_live_gate,
            declared_live_relations,
            dispatch_live_relation,
            live_reserve_envelope,
            resolve_live_relation,
        )
        from okg.substrate.external_backed.live_relations import (
            BoundLiveRelation,
            compile_args_model,
        )

        _refuse_unbound_live_call(mcp_server, live)
        body = as_mapping(live) or {}
        relation_name = str(body.get("relation") or "")
        # Issue #3923: no live relation is called while access rules are on
        # (audit or enforce) or when their state cannot be told. This is the
        # first thing after the unbound check, before the anchor node is read,
        # the gate is built or any adapter is chosen: zero outside calls. A
        # direct call and a `run` step both come through here.
        _live_access_rules.refuse_live_under_access_rules(
            mcp_server,
            f"expand(live=...) asks the live relation {relation_name!r}",
            relation=relation_name, tool_called="expand",
        )
        args = body.get("args") or {}
        # `each`: one call per value of one argument, one joined answer. The
        # values may be a column of a kept live answer (a wildcard listing).
        each = body.get("each")
        each_from: Optional[dict[str, Any]] = None
        if isinstance(each, Mapping) and len(each) == 1:
            ((each_arg, each_source),) = each.items()
            if isinstance(each_source, Mapping):
                values, each_from = _each_values_from_kept(each_arg, each_source, authority)
                each = {each_arg: values}
        each_key = (
            str(next(iter(each))) if isinstance(each, Mapping) and len(each) == 1 else None
        )
        # Live result sets: keep this call's answer for later reads. A
        # fanned-out answer is always kept: its point is a total over it.
        retain_requested = body.get("retain") is True or each is not None

        bindings = getattr(mcp_server, "_okg_live_bindings", None) or {}
        runners = getattr(mcp_server, "_okg_live_adapters", None) or {}

        # Read 1: the node's subtype and what it declares. Closed before the
        # gate, and long before any adapter runs.
        #
        # `inspect_node` rather than a hand-written SELECT: the graph tables
        # are Chronos views and a raw query does not see the session's pin,
        # so a hand-written one returned "node not visible" for a node the
        # very same call had just expanded.
        from okg.substrate.mcp.tools import inspect_node

        with _open(generation_id, authority=authority) as h:
            # THE pin for the whole call. The relation is resolved here, and
            # the graph step, the envelope and the sample all read at this
            # same generation: the wrapper forces it into the graph step, so
            # a publish between the two reads cannot resolve the relation
            # against one generation and stamp the answer with another.
            pinned_generation_id = h.session.generation_id
            # Only the subtype is read here, so skip the derived-document
            # lookup (issue #2669) and its read-filter decisions.
            row = inspect_node(
                h.conn, h.session, node_id, select=["node_id", "subtype"],
                include_documents=False,
            )
            if row is None:
                raise LiveRelationUndeclared(
                    f"expand(live=...) names node {node_id!r}, which is not "
                    f"visible at the pinned generation, so no subtype "
                    f"declares anything for it.",
                    data={"node_id": node_id, "relation": relation_name,
                          "subtype": None, "declared": []},
                    fix_hint="check the node id and the pinned generation",
                )
            subtype = str(row["subtype"])
            # Sample policies are registered ONCE at load, not here: the
            # registry is process-wide and startup-scoped, and re-declaring
            # per request would let one deployment silently take over a
            # relation name another had registered.
            declared = declared_live_relations(h.conn, h.session, subtype)
            session_id = getattr(h.session, "logical_session_id", None) or str(
                getattr(h.session, "session_id", "") or "mcp"
            )
            # What a retained set is bound to, read here under the same
            # session the anchor was read in: every later read compares it.
            retain_context: dict[str, Any] | None = None
            # `run` (#3666, lane ri): the reader a run's live step belongs
            # to, read under the same session the anchor was read in. The
            # H3 hint memory and a reader's fan-out runs are kept per
            # (reader, logical MCP session) (review H2, M3).
            reader_of_call = _continuation.reader_key(
                authority, h.request_header_claim, multi_user_principals,
            )
            reader_for_run: Optional[str] = (
                reader_of_call if _run_steps.live_prepare_active() else None
            )
            if retain_requested:
                retain_context = {
                    "binding": _live_binding(h.conn, h.session),
                    "reader_key": _continuation.reader_key(
                        authority, h.request_header_claim, multi_user_principals,
                    ),
                    "header_claim": h.request_header_claim,
                }

        # The caller's logical MCP session, the one `logical_session_id`
        # names; None without a stable session (stateless HTTP), and then
        # no H3 hint is given. `session_id` above is not it: the frozen
        # `Session` has no such field, so it was "mcp" for every caller
        # and one user's hint showed another user's values (review H2).
        logical_identity = _current_logical_identity()
        logical_session: Optional[str] = (
            str(logical_sessions.snapshot(logical_identity).logical_session_id)
            if logical_identity is not None else None
        )
        hint_memory: Optional[str] = (
            f"{reader_of_call}:{logical_session}" if logical_session else None
        )

        bound_map = {
            name: BoundLiveRelation(relation=relation, binding=bindings[name])
            for name, relation in declared.items()
            if name in bindings
        }
        bound = resolve_live_relation(
            bound_map, relation_name, subtype=subtype,
        )

        # The RELATION's own runner first. A built-in adapter's runner closes
        # over that relation's parsed config and its load-resolved secret, so
        # it cannot be shared by kind; the kind table is what the load check
        # and the refusal below are written against, and what a test seam
        # overrides. Two mappings rather than one, so a relation named after
        # a kind cannot shadow it.
        relation_runners = getattr(
            mcp_server, "_okg_live_relation_runners", None,
        ) or {}
        adapter = relation_runners.get(relation_name) or runners.get(
            bound.binding.kind,
        )
        if adapter is None:
            from okg.substrate.external_backed.live_dispatch import (
                LiveAdapterUnavailable,
            )

            raise LiveAdapterUnavailable(
                f"live relation {relation_name!r} binds adapter kind "
                f"{bound.binding.kind!r}, for which this runtime has no "
                f"registered runner. Registered kinds are {sorted(runners)}.",
                data={"relation": relation_name, "kind": bound.binding.kind,
                      "registered": sorted(runners)},
                fix_hint="register a runner for this kind, or drop the binding",
            )

        def write_sample(**kwargs: Any) -> Any:
            """The sample, in its OWN short transaction, after the adapter."""
            from okg.substrate.external_backed.call_sample import (
                record_live_relation_call_sample,
            )

            with _open(pinned_generation_id, authority=authority) as sh:
                return record_live_relation_call_sample(
                    sh.conn,
                    deployment_name=deployment_name,
                    tool_name="expand",
                    trace_id=str(uuid.uuid4()),
                    session_id=session_id,
                    **kwargs,
                )

        gate = getattr(mcp_server, "_okg_live_gate", None)
        if gate is None:
            gate = build_live_gate(deployment_name)
            mcp_server._okg_live_gate = gate  # type: ignore[attr-defined]

        args_model = compile_args_model(bound.relation)
        # Validated here, BEFORE the graph step: a malformed argument is a
        # client error, and it must not cost a graph read first.
        reserve = live_reserve_envelope(
            bound=bound, args_model=args_model, args=args,
            retain=retain_requested, each=each,
        )

        def keep_set(raw: Any, base: Mapping[str, Any], context: Mapping[str, Any]) -> Any:
            """Draft a kept set of this call's unwrapped answer, and remember
            the call that asks again, for a refusal once the set is gone."""
            from okg.substrate.external_backed.live_dispatch import (
                resolve_live_principal,
            )

            handle = _live_results.begin_retain(
                raw,
                base=base,
                store=_live_results.live_store(),
                reader_key=context["reader_key"],
                header_claim=context["header_claim"],
                multi_user=multi_user_principals,
                binding=context["binding"],
                relation=bound.relation,
                node_id=node_id,
                # The subject the dispatcher asks the gate about: one owner.
                subject=resolve_live_principal(bound, None),
                deployment_name=deployment_name,
                session_id=session_id,
                each_from=each_from,
            )
            if handle.kept:
                _live_results.remember_call(
                    handle.draft.id, context["reader_key"],
                    _live_results.redo_call(
                        node_id, relation_name, base.get("query") or {}, each_key,
                    ),
                )
            return handle

        def retain_hook(raw: Any, base: Mapping[str, Any]) -> Any:
            """Draft a retained set of this call's unwrapped answer."""
            assert retain_context is not None
            return keep_set(raw, base, retain_context)

        def keep_hook(raw: Any, base: Mapping[str, Any]) -> Any:
            """Keep a CUT answer for its `next` token (#3666, Live build 3).

            Read only when the answer was cut: what the set is bound to, in
            its own short read, after the adapter returned (no connection is
            held across the adapter call). If that read fails, the rows are
            still served and the reply says the answer was not kept
            (`keep_failed`), never a token that cannot be followed."""
            try:
                with _open(pinned_generation_id, authority=authority) as kh:
                    context = {
                        "binding": _live_binding(kh.conn, kh.session),
                        "reader_key": _continuation.reader_key(
                            authority, kh.request_header_claim, multi_user_principals,
                        ),
                        "header_claim": kh.request_header_claim,
                    }
            except Exception as exc:  # noqa: BLE001 — said on the reply, typed.
                log.warning(
                    "live relation %s: reading what a kept answer is bound to "
                    "raised %s; the cut answer is served without a next token",
                    relation_name, type(exc).__name__,
                )
                return _live_results.not_kept("keep_failed")
            return keep_set(raw, base, context)

        def dispatch(
            *, room: Any, pinned: Any, refuse: Optional[str] = None,
        ) -> dict[str, Any]:
            return dispatch_live_relation(
                bound=bound,
                args_model=args_model,
                args=args,
                node_id=node_id,
                gate=gate,
                adapter=adapter,
                write_sample=write_sample,
                generation_id=pinned,
                room=room,
                refuse_before_adapter=refuse,
                retain=retain_hook if retain_requested else None,
                keep=keep_hook,
                each=each,
            )

        class _PreparedExpandLive:
            """One `expand(live=...)`, resolved and validated, not yet run."""

            #: The generation the relation was resolved at; the wrapper
            #: pins the graph step to it.
            generation_id = pinned_generation_id

            # What a `run` live step reads to make its calls itself, in
            # three phases (#3666, lane ri): the gate here, the calls on the
            # wait-only executor, the sample on a call worker. Nothing here
            # opens a connection except `write_sample`.
            run_bound = bound
            run_args_model = args_model
            run_args = args
            run_each = each
            run_node_id = node_id
            # A plain function, as the built-in runners are: kept unbound.
            run_adapter = staticmethod(adapter)
            run_reader_key = reader_for_run
            run_logical_session = logical_session
            run_session_id = session_id

            def run_gate(self) -> Any:
                from okg.substrate.external_backed.live_dispatch import gate_for_call

                return gate_for_call(bound, gate, node_id)

            def run_write_sample(self, **kwargs: Any) -> Any:
                return write_sample(**kwargs)

            def reserve_envelope(self, graph: dict[str, Any]) -> dict[str, Any]:
                # The pin is the graph page's own: generation ids are
                # strings of no fixed length, so a placeholder could
                # undercount the reserve.
                return {
                    **reserve,
                    "pinned_generation_id": graph.get("pinned_generation_id"),
                }

            def refuse(self, reason: str) -> None:
                dispatch(room=None, pinned=pinned_generation_id, refuse=reason)

            def run(self, graph: dict[str, Any], room: Any) -> dict[str, Any]:
                envelope = dispatch(room=room, pinned=pinned_generation_id)
                hint = _live_run_hint(envelope)
                if hint is not None and (room is None or room.fits({**envelope, "run_hint": hint})):
                    envelope["run_hint"] = hint
                return envelope

        def _live_run_hint(envelope: Mapping[str, Any]) -> Optional[dict[str, Any]]:
            """H3 (#3666, lane ri): on a direct call (no `each`) whose
            relation this session asked with one argument changed, the run
            that makes those calls in one. Only where `run` is served."""
            from okg.substrate.external_backed.live_dispatch import (
                validate_live_arguments,
            )
            from okg.substrate.mcp import live_run_hint
            from okg.substrate.mcp.run_tool import _served as _run_served

            if (
                hint_memory is None or each is not None
                or getattr(mcp_server, "_okg_run_live_steps", True) is False
                or not _run_served(mcp_server, "run")
            ):
                return None
            validated = validate_live_arguments(bound, args, args_model=args_model)
            hint = live_run_hint.note_call(
                hint_memory, node_id, relation_name, validated, dict(args or {}),
            )
            # Every direct call is remembered; the hint rides on an answer.
            return hint if envelope.get("state") == "ok" else None

        return _PreparedExpandLive()

    def expand(
        input: Optional[ExpandInput] = None,
        edge_type: Optional[str] = None,
        direction: declared(str, enum=list(DIRECTIONS)) = "out",
        path: Optional[
            declared(list[str], minItems=1, maxItems=MAX_PATH_HOPS)
        ] = None,
        depth: declared(int, minimum=1, maximum=MAX_DEPTH) = 1,
        target_subtype: Optional[str] = None,
        continuation_token: Optional[str] = None,
        filter: Optional[PredicateInput] = None,
        generation_id: Optional[Union[int, str]] = None,
        limit: declared(int, minimum=1, maximum=MAX_PAGE_LIMIT) = 50,
        offset: declared(int, minimum=0, maximum=MAX_OFFSET) = 0,
        select: Optional[list[str]] = None,
        order_by: Optional[str] = None,
        include_siblings: bool = False,
        live: Optional[LiveCall] = None,
        authority: Optional[dict[str, Any]] = None,
        max_bytes: MaxBytes = None,
        # Last, after every existing parameter, so no positional call moves.
        next: Optional[str] = None,
        # The values a live edge takes (`$arg`; #3666). After `next`, so no
        # positional call moves.
        args: Optional[dict[str, Any]] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Follow links from one record to the records on the other end.

        expand(input="<id>", edge_type="<link>", direction="in"). `select`:
        rows are the linked records (node_id, subtype, attrs, provenance;
        1-hop adds link, link.attrs). A 1-hop select of only edge_id, src,
        dst, edge_type, direction (attrs: the link's) returns one row per link.
        Only one hop takes `where` and `order_by` (on the far record).
        path=["<link>:out", "<link>:in"] ('*' for any link) follows up to 3
        hops and returns the records at the end; it resumes by `next` alone.[[if live]]
        `live={relation, args}` also asks one live relation
        [[if tool:inspect]]`inspect` lists [[/if]]for this record's type; its
        rows return in `external_live`, beside the graph rows and never
        merged with them. `each={"<arg>": [values]}` (or {"ref", "field"} of a
        kept answer) asks once per value (up to 1,500) in one kept
        answer[[if tool:aggregate]] to total with
        `aggregate`[[/if]].[[/if]]
        """
        # A live edge (#3666, live-graph-rucio-sites): the resolver followed
        # it before this body ran; serve that reply, paged by `limit`.
        live_reply = _live_nodes.served_node_reply("expand", limit=limit, offset=offset)
        if live_reply is not None:
            return _stamp_operator(live_reply, "expand")
        if args:
            raise InvalidOperatorArguments(
                "expand: `args` carries the values of a live edge's arguments; "
                "this node is a stored record",
                data={"argument": "args"},
                fix_hint="drop args, or expand a live node such as site:<name>",
            )
        if next is not None:
            arguments, refusal = _next_call(
                "expand",
                next,
                given={
                    "input": input,
                    "edge_type": edge_type,
                    "direction": None if direction == "out" else direction,
                    "path": path,
                    "depth": None if int(depth) == 1 else depth,
                    "target_subtype": target_subtype,
                    "continuation_token": continuation_token,
                    "filter": filter,
                    "limit": None if int(limit) == 50 else limit,
                    "offset": offset or None,
                    "select": select,
                    "order_by": order_by,
                    "include_siblings": include_siblings,
                    "live": live,
                },
            )
            if refusal is not None:
                return refusal
            input = arguments["input"]
            path = arguments["path"]
            edge_type = arguments["edge_type"]
            direction = arguments["direction"]
            depth = arguments["depth"]
            target_subtype = arguments["target_subtype"]
            continuation_token = arguments["continuation_token"]
            select = arguments["select"]
            if isinstance(arguments["limit"], int):
                limit = arguments["limit"]
            if generation_id is None:
                generation_id = arguments["generation_id"]
        elif input is None:
            refusal = _operator_refusal(
                "expand",
                code="invalid_operator_arguments",
                message="expand needs an `input` node, or `next` to continue a path read",
            )
            refusal["error"]["fix_hint"] = (
                'pass input={"node_id": ...}, or next=<token> from a page that '
                "ended bound reached"
            )
            return refusal
        digest_refusal = _text_digest_operator_refusal(
            "expand", input=input, filter=filter, order_by=order_by,
        )
        if digest_refusal is not None:
            return digest_refusal
        loose_refusal = _loose_match_refusal("expand", filter)
        if loose_refusal is not None:
            return loose_refusal
        # `select=["source_lineage", ...]` (issue #2674): fetch attrs so the
        # lineage can be read, then hand it back as a row-level field.
        # The caller's own select is what a path page's token records, so a
        # `next` page is rewritten, and shaped, exactly as this one was.
        caller_select = list(select) if isinstance(select, (list, tuple)) else select
        # Step 3a (#3666): when the served call was given a narrower select
        # than the caller wrote, the token signs the caller's own, and a
        # `next` page is planned from it exactly as the first page was.
        from okg.substrate.mcp.response_bounds import call_sent_select
        from okg.substrate.mcp.select_vocabulary import path_select

        if next is not None:
            select = path_select(select)[0]
        else:
            sent_select = call_sent_select()
            if sent_select is not None and (path is not None or int(depth) > 1):
                caller_select = sent_select
        select, lineage_requested, lineage_drop_attrs, _ = _lineage_select_plan(
            select,
            # A 1-hop row is a record whose default has no `attrs` (#3666):
            # the token alone still has to fetch them to read the lineage.
            default_without_attrs=(
                ("node_id", "subtype") if path is None and int(depth) <= 1 else None
            ),
        )
        # A select refusal's ready retry names the caller's own list, not
        # the rewritten one (PACT select-fields-by-method).
        with select_retry_from(caller_select):
            payload = _expand_graph(
                input,
                edge_type=edge_type,
                direction=direction,
                path=path,
                depth=depth,
                target_subtype=target_subtype,
                continuation_token=continuation_token,
                filter=filter,
                generation_id=generation_id,
                limit=limit,
                offset=offset,
                select=select,
                order_by=order_by,
                include_siblings=include_siblings,
                authority=authority,
                ctx=ctx,
                caller_select=caller_select,
                max_bytes=max_bytes,
            )
        if lineage_requested:
            payload = _move_lineage_to_rows(payload, drop_attrs=lineage_drop_attrs)
        if live is not None:
            # The byte-bound wrapper takes `live` out of the call before the
            # graph step and dispatches it itself, exactly once, into the
            # room the graph rows leave (issue #2141). Reaching this line
            # with `live` set means something called the operator around the
            # wrapper, and dispatching here would be a live call with no
            # response budget -- and, under the wrapper's paging, one call
            # per retry.
            raise RuntimeError(
                "expand(live=...) must be dispatched by the byte-bound "
                "wrapper, never by the operator itself",
            )
        return payload

    def _reference_groups(
        conn: psycopg.Connection,
        session: Session,
        *,
        by: list[str],
        ids: list[str],
        period_report: Any = None,
        each_report: dict[str, Any] | None = None,
    ) -> tuple[list[tuple[tuple[Any, ...], int]], Any]:
        """The reference-aggregate function (#3222): count groups over a
        reference's member ids, largest first, through the effective view.

        `aggregate(input={"ref": id})` and a metadata search's `summary`
        (design D3) both call it, so a summary equals the aggregate over its
        reference, refusals included: where the count cannot run exactly in
        SQL it refuses `unsupported_operator_form`, never a Python fallback.
        """
        grouped = _effective_view_aggregate_groups(
            conn, session, kind="nodes", by=by, predicate=None, member_ids=ids,
            period_report=period_report, each_report=each_report,
        ) if ids else ([], None)
        if grouped is None:
            enforced = deployment_requires_nomos_runtime(session.deployment_name)
            err = SubstrateError(
                AGGREGATE_UNDER_ACCESS_RULES if enforced
                else "aggregate: a `by` field of this call cannot be counted",
                data={"kind": "nodes", "by": by, "nomos_enforced": enforced},
                fix_hint=(
                    AGGREGATE_UNDER_ACCESS_RULES_HINT if enforced
                    else "group by subtype or a declared attribute"
                ),
            )
            err.code = "unsupported_operator_form"
            raise err
        return grouped

    def _match_groups(
        h: Any,
        *,
        condition: Any,
        ids: list[str],
        rest: Any,
        facts: Mapping[str, Any],
        by: list[str],
        metrics: list[str],
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        """`aggregate(by=["match"])` (#3666): every distinct matched text over
        the records the whole condition admits, counted exactly.

        `count` is how many of those records hold the text; `occurrences`
        how many times it occurs in them. The records are the matching ids
        narrowed by the rest of the condition in SQL, at most
        `text_match.SCAN_LIMIT` (the ids already are)."""
        from okg.substrate.mcp import text_match as tm

        final = ids
        date_report = _DateConditionReport()
        if rest not in (None, {}, True):
            narrowed = _text_rest_ids_dated(
                h.conn, h.session, tool="aggregate", predicate=rest,
                cap=tm.SCAN_LIMIT, member_ids=ids, report=date_report,
            )
            if narrowed is None:
                raise _text_not_compiled("aggregate")
            final = narrowed
        from okg.substrate.mcp import match_keys as mk

        # PACT read-aggregate-variants-3666: `norm(match)` and
        # `words(match)` merge the variants of one value and list them.
        key = str(by[0])
        groups, holding, count_facts = tm.count_matches(
            h.conn, h.session, condition, final, tool="aggregate", key=key,
        )
        merged = key != mk.MATCH_KEY
        visible = groups[offset: offset + limit + 1]
        truncated = len(visible) > limit
        if merged:
            pairs = mk.may_be_one(group.raw for group in groups)
            if pairs:
                # Named by text when every group is trusted; else by place
                # ("group 2"), so no untrusted text leaves its fence.
                place = {group.raw: i + 1 for i, group in enumerate(groups)}
                plain = not any(group.fenced for group in groups)
                count_facts["may_be_one"] = [
                    [a, b] if plain else [f"group {place[a]}", f"group {place[b]}"]
                    for a, b in pairs
                ]
        payload = {
            "groups": [
                # The column is `match` for every key: `by` says which
                # key grouped it (a parenthesized column name prints
                # quoted twice in the text table).
                {"match": group.text, "count": group.records,
                 "occurrences": group.occurrences,
                 **({"variants": group.variants} if merged else {})}
                for group in visible[:limit]
            ],
            "by": by,
            "metrics": metrics,
            # Every matched text of every record is counted, so the group
            # count is exact.
            "total_count": len(groups),
            "total_count_basis": "exact",
            "retrieval_exhaustive": True,
            "truncated": truncated,
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit if truncated else None,
            "generation_id": h.session.generation_id,
            "pinned_generation_id": h.session.generation_id,
            # What the date conditions read: no other read of this reply
            # states it (#3666).
            **date_report.payload(),
            "text_match": _text_fact(condition, {
                **facts, **count_facts, "records_counted": len(final),
                **({"merged": mk.merged_label(key)} if merged else {}),
                # A record admitted by a phrase across two of its passages
                # holds no matched text; said, so the groups add up.
                "records_without_match": len(final) - holding,
                # Format v10 item C (#3666): plain words match only
                # themselves, so they form one group; said, with the form
                # that groups each distinct matched text.
                **({"one_group_plain_words": True} if (
                    len(groups) == 1
                    and not any(t.kind == "regex" for t in condition.parsed.terms)
                ) else {}),
            }),
        }
        return _stamp_operator(_maybe_stamp_advanced(h, payload), "aggregate")

    def _merge_date_report(
        payload: dict[str, Any], date_report: Any, period_report: Any,
    ) -> None:
        """Add what the date conditions read to an aggregate reply (#3666),
        beside what its period fold read: one `dates_read_as_utc` note and
        one `date_unparseable` map for both."""
        utc = _DateConditionReport(
            utc_bounds=[*date_report.utc_bounds, *period_report.utc_bounds],
            utc_stored=[*date_report.utc_stored, *period_report.utc_stored],
            iso_only=date_report.iso_only,
        ).payload()
        payload.update(utc)
        if date_report.unparseable:
            payload["date_unparseable"] = {
                **date_report.payload()["date_unparseable"],
                **(payload.get("date_unparseable") or {}),
            }

    def _names_a_date_meaning(by: Any) -> bool:
        """Whether `by` holds a period of a NAMED date (date.created, ...).

        Asked of an edge set only, which declares no named date: a malformed
        period refuses here with an example that set accepts (#3666)."""
        parsed = (_parse_period_field(str(f), named_dates=False) for f in by or [])
        return any(p is not None and p.meaning is not None for p in parsed)

    def _has_period_field(by: Any) -> bool:
        """Whether `by` names a `<period>(<date>)` field (#3658). A malformed
        one refuses here, by name, before anything is opened."""
        return any(_parse_period_field(str(f)) is not None for f in by or [])

    def _reference_aggregate(
        *,
        ref_id: Any,
        by: list[str],
        metrics: list[str],
        generation_id: Any,
        limit: int,
        offset: int,
        authority: Any,
    ) -> dict[str, Any]:
        """`aggregate` over a result reference's members (#3222): the
        effective-view groups with the member restriction, so it equals the
        explicit node-id aggregate, and refuses where that one refuses."""
        if not by or len(by) > MAX_AGGREGATE_BY:
            raise InvalidOperatorArguments(f"aggregate: by must name 1..{MAX_AGGREGATE_BY} fields")
        with _open(
            generation_id, operator_name="aggregate", authority=authority,
            reference=str(ref_id),
        ) as h:
            _reference_rows(h, None)
            ids = [str(member["node_id"]) for member in h.reference_entry.members]
            period_report = _PeriodReport()
            each_report: dict[str, Any] = {}
            ordered, fingerprint = _reference_groups(
                h.conn, h.session, by=by, ids=ids, period_report=period_report,
                each_report=each_report,
            )
            visible = ordered[offset: offset + limit + 1]
            truncated = len(visible) > limit
            payload = {
                "groups": [
                    {**{by[i]: key[i] for i in range(len(by))}, "count": count}
                    for key, count in visible[:limit]
                ],
                "by": by,
                "metrics": metrics,
                "total_count": len(ordered),
                "total_count_basis": "exact",
                # Exact over the reference's rows only: never "exhaustive",
                # which reads as "of the whole graph" (the envelope turns
                # this basis into a `result_reference` retrieval bound).
                "selection_basis": "reference",
                "truncated": truncated,
                "limit": limit,
                "offset": offset,
                "next_offset": offset + limit if truncated else None,
                "generation_id": h.session.generation_id,
                "pinned_generation_id": h.session.generation_id,
                "predicate_fingerprint": fingerprint,
                "input": {"ref": ref_id},
                # The count covers the rows the reference holds, which can
                # be fewer than its search matched: said in the reply (a
                # text notice too), so "complete" is never read as "of the
                # whole graph" (#3658 rework).
                "reference_members": len(ids),
                "retrieval_basis": "result_reference_members",
            }
            if _has_period_field(by):
                payload.update(period_report.payload())
            _merge_date_report(payload, _DateConditionReport(), period_report)
            # A count per list element says what it counted (#3666).
            payload.update(each_report)
            return _stamp_operator(_maybe_stamp_advanced(h, payload), "aggregate")

    def aggregate(
        input: AggregateInput,
        # Empty only over a kept live answer: one total (graph sets refuse it).
        by: Annotated[list[str], PField(min_length=0, max_length=MAX_AGGREGATE_BY)],
        metrics: Annotated[
            list[AggregateMetric], PField(min_length=1, max_length=4)
        ],
        generation_id: Optional[Union[int, str]] = None,
        limit: Limit = 50,
        offset: Offset = 0,
        authority: Optional[dict[str, Any]] = None,
        max_bytes: MaxBytes = None,
        predicate: Optional[PredicateInput] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Count a set, per value.

        `by` names 1-4 fields: subtype, attrs.<key>, month(date.created)
        (or day, week, year) or match, e.g.
        aggregate(where={"type":"[[subtype]]"}, by=["attrs.<key>"],
        metrics=["count"]). by=["match"] with where
        {"text":{"op":"matches","value":"re:/<regex>/"}} counts each
        distinct match; norm(match) merges case, space and punctuation
        variants, words(match) word order too; one (group) in the regex
        is the text counted. A kept live answer also takes by=[] (one
        total) and sum/min/max, e.g. "sum(attrs.size)".
        """
        date_started = _date_call_started()
        input = as_mapping(input)
        predicate = as_mapping(predicate)
        digest_refusal = _text_digest_operator_refusal(
            "aggregate", input=input, by=by, metrics=metrics,
        )
        if digest_refusal is not None:
            return digest_refusal
        if isinstance(input, dict) and _live_results.is_live_id(input.get("ref")):
            _refuse_live_aggregate_arguments(by)
            return _live_read(
                "aggregate", ref_id=input["ref"], authority=authority,
                generation_id=generation_id, limit=limit, offset=offset,
                predicate=predicate, by=by, metrics=metrics,
            )
        if predicate not in (None, True, {}):
            raise InvalidOperatorArguments(
                "aggregate: `predicate` narrows a kept live answer "
                "(input={'ref': 'l1...'}); a set of records takes its condition "
                "in `where` or input.filter",
                data={"argument": "predicate"},
            )
        if metrics != ["count"]:
            raise InvalidOperatorArguments(
                "aggregate counts graph records only (metrics=['count']); sum, "
                "min and max read a kept live answer, input={'ref': 'l1...'}"
            )
        if not by:
            raise InvalidOperatorArguments(
                f"aggregate: by must name 1..{MAX_AGGREGATE_BY} fields; only a "
                "kept live answer (input={'ref': 'l1...'}) may omit it for one total"
            )
        loose_refusal = _loose_match_refusal("aggregate", input)
        if loose_refusal is not None:
            return loose_refusal
        if isinstance(input, dict) and "ref" in input and _holds_text(input):
            raise _text_on_reference("aggregate")
        if isinstance(input, dict) and "ref" in input:
            return _reference_aggregate(
                ref_id=input["ref"], by=[str(f) for f in by], metrics=metrics,
                generation_id=generation_id, limit=limit, offset=offset,
                authority=authority,
            )
        if is_set_input(input) and not (
            input.get("kind") == "edges"
            and by == ["src_subtype", "edge_type", "dst_subtype"]
            and not input.get("filter")
            and not input.get("edge_type")
        ):
            # #3201: every set aggregate groups the effective-attribute
            # relation in SQL, whether or not it names attrs, so the same
            # filter counts the same set; none goes through run_sql.
            if not by or len(by) > MAX_AGGREGATE_BY:
                raise InvalidOperatorArguments(f"aggregate: by must name 1..{MAX_AGGREGATE_BY} fields")
            kind = "edges" if input.get("kind") == "edges" else "nodes"
            validate_set_scope(kind, input)
            # #3666: a text condition in the filter, and `match` in by.
            from okg.substrate.mcp import text_match as _tm

            sent_input = input
            text_filter, text_condition = _tm.split_text_condition(
                input.get("filter"), tool="aggregate",
            )
            from okg.substrate.mcp.match_keys import is_match_key

            # PACT read-aggregate-variants-3666: norm(match) and
            # words(match) are `match` with its variants merged.
            match_keys = [str(f) for f in by if is_match_key(str(f))]
            by_match = bool(match_keys)
            said = match_keys[0] if match_keys else "match"
            if by_match and text_condition is None:
                raise _tm.TextConditionInvalid(
                    f"aggregate: `{said}` in by is the text a text condition "
                    "matched; this call has none",
                    data={"by": [str(f) for f in by], "reason": "match_without_text"},
                    fix_hint=(
                        'add the text condition to where: {"text": {"op": '
                        '"matches", "value": "re:/<regex>/"}}'
                    ),
                )
            if by_match and len(by) > 1:
                refusal = _tm.TextConditionInvalid(
                    f"aggregate: `{said}` is counted alone in by",
                    data={"by": [str(f) for f in by], "reason": "match_with_other_fields"},
                    fix_hint=(
                        f'send by=["{said}"]; group the records by other fields '
                        "in a second call"
                    ),
                )
                refusal.retry_call = {
                    "tool": "aggregate", "mode": "corrected",
                    "arguments": {"input": sent_input, "by": [said], "metrics": metrics},
                }
                raise refusal
            if by_match and text_condition is not None:
                # A regex with several capture groups is refused before
                # any text is read (PACT read-aggregate-variants-3666).
                _tm.capture_terms(text_condition.parsed, tool="aggregate")
            if text_condition is not None:
                input = {k: v for k, v in input.items() if k != "filter"}
                if text_filter is not None:
                    input["filter"] = text_filter
            if kind == "edges" and _names_a_date_meaning(by):
                # Refused before anything is opened (#3658).
                raise InvalidOperatorArguments(
                    "aggregate: a named date (date.created, ...) is declared per "
                    "record type; edges have none",
                    data={"by": [str(f) for f in by], "kind": kind},
                    fix_hint="name the edge attribute, e.g. month(date.attrs.<key>)",
                )
            # A date range on a period's own date field is applied by the
            # period fold, exactly (#3658 rework); every other date condition
            # compiles like any other clause (#3666), so a range needs no
            # period in `by`.
            # The call's date budget counts every date condition the caller
            # wrote, a range on a period's own date included (the period
            # fold applies that one, and it is still a date condition).
            # Only a count per period with no date condition has none.
            caller_scope = _compose_scope_predicate(kind=kind, input=input, predicate=None)
            if text_condition is None:
                _run_steps.note_set("records", kind, caller_scope)
            plain_filter, period_ranges = _split_date_filter(input.get("filter"), {
                str(f): p for f in by
                if (p := _parse_period_field(str(f))) is not None
            }, named_dates=kind == "nodes")
            # The date field this input accepts, for the hint below: an edge
            # set refuses a named date (#3666).
            date_example = "date.created" if kind == "nodes" else "date.attrs.<key>"
            if period_ranges:
                input = {k: v for k, v in input.items() if k != "filter"}
                if plain_filter is not None:
                    input["filter"] = plain_filter
            with _open(generation_id, authority=authority) as h:
                target_kind = "node" if kind == "nodes" else "edge"
                _require_effective_view(h.conn, h.session, target_kind)
                scoped_predicate = _compose_scope_predicate(
                    kind=kind,
                    input=input,
                    predicate=None,
                )
                text_ids: list[str] | None = None
                text_facts: dict[str, Any] = {}
                if text_condition is not None:
                    text_ids, text_facts = _text_ids(
                        h.conn, h.session, tool="aggregate", kind=kind,
                        rest=scoped_predicate, condition=text_condition,
                        nomos_runtime=deployment_requires_nomos_runtime(
                            h.session.deployment_name
                        ),
                        retry=lambda subtype: {
                            "input": {**sent_input, "subtype": subtype},
                            "by": [str(f) for f in by], "metrics": metrics,
                        },
                    )
                    # The run set is the counted records: the text matches
                    # under the rest of the condition.
                    _run_steps.note_set(
                        "records", kind, _run_set_predicate(caller_scope, text_ids),
                        text=True,
                    )
                if by_match:
                    return _match_groups(
                        h, condition=text_condition, ids=text_ids or [],
                        rest=scoped_predicate, facts=text_facts, by=by,
                        metrics=metrics, limit=limit, offset=offset,
                    )
                date_report = _DateConditionReport()
                # One budget for the count's date conditions, as the caller
                # wrote them. A count per date period with no date condition
                # has none and runs as before.
                count_budget = _date_budget(
                    "aggregate", caller_scope, kind=kind, started_at=date_started,
                )
                # What a count per list element counted, from the last run.
                each_report: dict[str, Any] = {}

                def count_groups(read_with: Any) -> Any:
                    periods_read = _PeriodReport()
                    each_report.clear()
                    return _effective_view_aggregate_groups(
                        h.conn,
                        h.session,
                        kind=kind,
                        by=[str(f) for f in by],
                        predicate=read_with,
                        fingerprint_predicate=scoped_predicate,
                        period_report=periods_read,
                        period_ranges=period_ranges,
                        date_budget=count_budget,
                        each_report=each_report,
                        member_ids=text_ids,
                    ), periods_read

                if deployment_requires_nomos_runtime(h.session.deployment_name):
                    # Under run-time access rules the count refuses, as
                    # before; nothing is read for it.
                    grouped, period_report = count_groups(scoped_predicate)
                else:
                    # #3666: the count first, then the stored date forms
                    # (`_dated_read`).
                    _read_with, (grouped, period_report) = _dated_read(
                        h.conn, h.session, kind=kind, predicate=scoped_predicate,
                        operator="aggregate", report=date_report,
                        nomos_runtime=False, budget=count_budget, run=count_groups,
                    )
                if grouped is None:
                    from okg.substrate.mcp.operator_sql import declared_bound_mismatch

                    # A range bound of the wrong type for its attribute's
                    # declared type is named (issue #3666, 2c).
                    # (Under access rules the count refuses whatever the
                    # predicate, and says that instead.)
                    mismatch = None if deployment_requires_nomos_runtime(
                        h.session.deployment_name
                    ) else declared_bound_mismatch(
                        scoped_predicate, _declared_registry_for(h.conn),
                    )
                    if mismatch is not None:
                        err = SubstrateError(
                            f"aggregate: {mismatch[0]}",
                            data={"kind": kind, "by": by, "predicate": scoped_predicate},
                            fix_hint=mismatch[1],
                        )
                        err.code = "unsupported_operator_form"
                        raise err
                    enforced = deployment_requires_nomos_runtime(h.session.deployment_name)
                    err = SubstrateError(
                        # Issue #3666: plain words, not "cannot execute
                        # exactly in SQL".
                        AGGREGATE_UNDER_ACCESS_RULES if enforced
                        else "aggregate: a condition or a `by` field of this "
                        "call cannot be counted",
                        data={
                            "kind": kind,
                            "by": by,
                            "predicate": scoped_predicate,
                            "nomos_enforced": enforced,
                        },
                        fix_hint=AGGREGATE_UNDER_ACCESS_RULES_HINT if enforced else (
                            "a filter clause or by field could not be compared "
                            "in SQL: compare a declared attribute, or for a date "
                            'range pass {"op":"between","field":"' + date_example
                            + '","values":["2025-01-01","2025-12-31"]}'
                        ),
                    )
                    err.code = "unsupported_operator_form"
                    raise err
                ordered, fingerprint = grouped
                visible = ordered[offset : offset + limit + 1]
                truncated = len(visible) > limit
                groups = [
                    {
                        **{by[i]: key[i] for i in range(len(by))},
                        "count": count,
                    }
                    for key, count in visible[:limit]
                ]
                payload = {
                    "groups": groups,
                    "by": by,
                    "metrics": metrics,
                    # Every group is computed (SQL GROUP BY over the
                    # whole set), so the group count is exact (#3190).
                    "total_count": len(ordered),
                    "total_count_basis": "exact",
                    "retrieval_exhaustive": True,
                    "truncated": truncated,
                    "limit": limit,
                    "offset": offset,
                    "next_offset": offset + limit if truncated else None,
                    "generation_id": h.session.generation_id,
                    "pinned_generation_id": h.session.generation_id,
                    "predicate_fingerprint": fingerprint,
                }
                if _has_period_field(by):
                    # Which attribute each date period read, and the values
                    # that were not dates (#3658).
                    payload.update(period_report.payload())
                _merge_date_report(payload, date_report, period_report)
                # A count per list element: rows counted, rows with no
                # element, rows holding no list, and that groups overlap.
                payload.update(each_report)
                if text_condition is not None:
                    payload["text_match"] = _text_fact(text_condition, text_facts)
                payload.update(_compared_as_stored(
                    scoped_predicate, lambda: _declared_registry_for(h.conn),
                    empty=not groups,
                ))
                return _stamp_operator(
                    _maybe_stamp_advanced(h, payload),
                    "aggregate",
                )
        if _has_period_field(by):
            raise InvalidOperatorArguments(
                "aggregate: a date period groups a set of records, not one "
                "record's edges",
                data={"by": [str(f) for f in by]},
                fix_hint=(
                    'pass a set input, e.g. input={"kind":"nodes","subtype":'
                    '"<type>"}, by=["month(date.created)"]'
                ),
            )
        if offset:
            raise InvalidOperatorArguments("aggregate: offset is not supported in v1")
        with _open(generation_id, authority=authority) as h:
            if isinstance(input, dict) and input.get("kind") == "edges":
                if by != ["src_subtype", "edge_type", "dst_subtype"]:
                    raise InvalidOperatorArguments(
                        "edge aggregate by must be "
                        "['src_subtype', 'edge_type', 'dst_subtype']"
                    )
                payload = _aggregate_edge(
                    h.conn,
                    h.session,
                    limit=limit,
                )
            else:
                node_id = _node_id_from_input(input)
                if by != ["edge_type", "dst_subtype"]:
                    raise InvalidOperatorArguments(
                        "node aggregate by must be ['edge_type', 'dst_subtype']"
                    )
                payload = _aggregate_node(h.conn, h.session, node_id)
            return _stamp_operator(
                _maybe_stamp_advanced(h, payload),
                "aggregate",
            )

    def query(
        sql: str,
        generation_id: Optional[Union[int, str]] = None,
        row_cap: Annotated[int, PField(ge=1, le=10_000)] = 1_000,
        timeout_ms: Annotated[int, PField(ge=100, le=60_000)] = 5_000,
        authority: Optional[dict[str, Any]] = None,
        max_bytes: MaxBytes = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Read-only SQL over okg.v_* views at one pinned generation.

        The served description is written at server build by
        `okg.substrate.mcp.query_guide` from the dataset card's facts: the
        views and columns, the fields of the largest types, and examples
        that run on this graph (issue #3666).
        """
        return _query_impl(
            sql=sql,
            generation_id=generation_id,
            row_cap=row_cap,
            timeout_ms=timeout_ms,
            authority=authority,
        )

    def _query_impl(
        *,
        sql: str,
        generation_id: Optional[Union[int, str]] = None,
        row_cap: int = 1_000,
        timeout_ms: int = 5_000,
        authority: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        from okg.substrate.mcp.run_sql import run_sql as _run_sql

        with _open(
            generation_id,
            surface="graph.read.sql_admin",
            operator_name="query",
            resource={"row_cap": row_cap, "timeout_ms": timeout_ms},
            authority=authority,
        ) as h:
            payload = _run_sql(
                h.conn,
                h.session,
                sql,
                row_cap=row_cap,
                timeout_ms=timeout_ms,
            )
            return _stamp_operator(_maybe_stamp_advanced(h, payload), "query")

    def _edge_ids_as_text(kind: str, predicate: Any) -> Any:
        """`predicate` with integer `edge_id` operands as text (issue #3476).

        `map` and `expand` serve `edge_id` as an integer, while the
        effective-view relation stores it as text, so an integer operand
        compiled to FALSE and an edge-id filter returned an empty page
        marked complete. Only edge scopes are rewritten, and only `eq` and
        `in`: a range on `edge_id` keeps its integer bounds, which the
        lowerer and the row check compare as numbers (issue #3666; text order
        is not number order). Nothing else in the predicate changes."""
        if kind != "edges":
            return predicate

        def rewrite(node: Any) -> Any:
            if isinstance(node, dict):
                out = {key: rewrite(value) for key, value in node.items()}
                if (
                    out.get("op") == "eq" and out.get("field") == "edge_id"
                    and "value" in out
                ):
                    value = out["value"]
                    if isinstance(value, int) and not isinstance(value, bool):
                        out["value"] = str(value)
                    elif isinstance(value, list):
                        out["value"] = [
                            str(v) if isinstance(v, int) and not isinstance(v, bool) else v
                            for v in value
                        ]
                if (
                    out.get("op") == "in" and out.get("field") == "edge_id"
                    and isinstance(out.get("values"), list)
                ):
                    # `in` reads its operands from `values` (issue #3666).
                    out["values"] = [
                        str(v) if isinstance(v, int) and not isinstance(v, bool) else v
                        for v in out["values"]
                    ]
                return out
            if isinstance(node, list):
                return [rewrite(item) for item in node]
            return node

        return rewrite(predicate)

    def _compose_scope_predicate(
        *,
        kind: str,
        input: Any,
        predicate: Any,
    ) -> Any:
        if not isinstance(input, dict):
            return predicate
        clauses: list[Any] = []
        if kind == "nodes" and input.get("subtype"):
            clauses.append({"op": "eq", "field": "subtype", "value": input["subtype"]})
        if kind == "edges" and input.get("edge_type"):
            clauses.append(
                {"op": "eq", "field": "edge_type", "value": input["edge_type"]}
            )
        if input.get("filter"):
            clauses.append(input["filter"])
        if predicate not in (None, True, {}):
            clauses.append(predicate)
        if not clauses:
            return predicate
        if len(clauses) == 1:
            return clauses[0]
        return {"op": "and", "clauses": clauses}

    def _attr_scan_pushdown_predicate(
        *,
        kind: str,
        input: Any,
        predicates: tuple[Any, ...] = (),
    ) -> Any:
        """Build the attr-operator SQL-pushdown WHERE.

        Pushes the input's subtype (nodes) / edge_type (edges) -- stable graph
        columns the effective-attrs overlay never mutates -- plus any of the
        given `predicates` clauses that reference ONLY stable fields (no attrs).
        Conjunctive (`and`) predicates are split so each attr-free conjunct is
        pushed independently; a clause that touches attrs (anywhere in the tree)
        is dropped from the pushdown and left to the caller's residual Python
        post-filter over the effective attrs.

        CONTRACT: `predicates` MUST list exactly the predicates the caller's
        Python post-filter re-applies against the effective attrs (so the SQL
        push only narrows by clauses the caller re-checks). subtype/edge_type
        come from `input` and the callers re-check those in Python too. The
        result (a v1 predicate or None for no WHERE) is a superset filter: it
        can only keep rows the effective-attrs post-filter would also keep, so
        operator results are unchanged.
        """
        clauses: list[Any] = []
        if isinstance(input, dict):
            if kind == "nodes" and input.get("subtype"):
                clauses.append(
                    {"op": "eq", "field": "subtype", "value": input["subtype"]}
                )
            if kind == "edges" and input.get("edge_type"):
                clauses.append(
                    {"op": "eq", "field": "edge_type", "value": input["edge_type"]}
                )
        for candidate in predicates:
            clauses.extend(_attr_free_pushdown_clauses(kind, candidate))
        if not clauses:
            return None
        if len(clauses) == 1:
            return clauses[0]
        return {"op": "and", "clauses": clauses}

    def _attr_free_pushdown_clauses(kind: str, predicate: Any) -> list[Any]:
        """Return the conjuncts of `predicate` safe to push into the scan WHERE.

        Splits a top-level `and` into its conjuncts and keeps each leaf `eq` /
        `in` / `exists` whose field is a KNOWN stable graph column (node_id /
        subtype / text_hash for nodes; edge_id / src / dst / edge_type for
        edges). Everything else -- any clause referencing attrs, an `or`/`not`
        tree, or a field that is not a lowerable stable column -- is dropped so
        it stays in the caller's residual Python post-filter. This keeps the
        pushdown to clauses whose SQL evaluation is identical to the
        effective-attrs Python evaluation (the overlay only touches attrs, so
        stable-column clauses agree exactly), and never to a field that would
        raise on lowering.
        """
        stable_fields = _STABLE_PUSHDOWN_FIELDS[kind]
        if predicate in (None, True, {}):
            return []
        if not isinstance(predicate, dict):
            return []
        op = predicate.get("op")
        if op == "and":
            sub_clauses = predicate.get("clauses")
            if not isinstance(sub_clauses, list):
                return []
            pushable: list[Any] = []
            for clause in sub_clauses:
                pushable.extend(_attr_free_pushdown_clauses(kind, clause))
            return pushable
        if op not in _PUSHDOWN_SAFE_OPS:
            return []
        field = predicate.get("field")
        if not isinstance(field, str) or field not in stable_fields:
            return []
        return [predicate]

    def _source_lineage_filter_operator_rows(
        *,
        kind: str,
        conn: psycopg.Connection,
        session: Session,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if kind == "nodes":
            from okg.substrate.nomos.read_filter import filter_node_rows_for_session

            return filter_node_rows_for_session(conn, session, rows)
        if kind == "edges":
            from okg.substrate.nomos.read_filter import filter_edge_rows_for_session

            return filter_edge_rows_for_session(conn, session, rows)
        return rows

    def _reference_rows(
        h: Any, predicate: Any, *, date_budget: Any = None,
    ) -> list[dict[str, Any]]:
        """A validated reference's member rows at its pin, in rank order.

        Validation step 9 first: the members are hydrated through the
        effective-view path with the member restriction, and fewer readable
        rows than stored members refuses ``member_missing`` with the count
        only. Then the predicate, if any, is read the same way. Both reads are
        bounded by the member count. The rows are kept for the call's
        byte-fitting reruns (design L6).
        """
        entry = h.reference_entry
        ids = [str(member["node_id"]) for member in entry.members]
        memo = _result_references.call_memo()
        key = "consumer:" + _json.dumps(
            [entry.id, h.session.generation_id, predicate], sort_keys=True, default=str,
        )
        if memo is not None and key in memo:
            return copy.deepcopy(memo[key])
        rank = {node_id: index for index, node_id in enumerate(ids)}
        rows: list[dict[str, Any]] = []
        if ids:
            # #3666: in a call that carries a date condition this read
            # is part of the call's date budget, like the one after it.
            hydrated, _ = _effective_view_sql_page(
                h.conn, h.session, kind="nodes", predicate=None,
                limit=len(ids), offset=0, member_ids=ids,
                date_budget=date_budget,
            )
            readable = {str(row["node_id"]) for row in hydrated}
            if len(readable) < len(ids):
                raise _result_references.invalid(
                    "member_missing", count=len(set(ids) - readable),
                )
            if predicate in (None, True, {}):
                rows = hydrated
            else:
                try:
                    rows, _ = _effective_view_sql_page(
                        h.conn, h.session, kind="nodes", predicate=predicate,
                        limit=len(ids), offset=0, member_ids=ids,
                        date_budget=date_budget,
                    )
                except SubstrateError:
                    raise
                except ValueError as exc:
                    # Only the predicate read: a ValueError from the
                    # hydration read above is a server fault, not a refusal.
                    raise _ReferencePredicateUnsupported(str(exc)) from exc
            rows.sort(key=lambda row: rank[str(row["node_id"])])
        if memo is not None:
            memo[key] = copy.deepcopy(rows)
        return rows

    def _reference_page(
        operator: str,
        *,
        ref_id: Any,
        predicate: Any,
        projection: Any,
        generation_id: Any,
        limit: int,
        offset: int,
        authority: Any,
        order_by: Any = None,
        retain: bool = False,
    ) -> dict[str, Any]:
        """`filter` or `map` over a result reference's members (#3222).

        Always the effective-view path (design H2), whether or not the call
        names an attribute, so it equals the explicit node-id call that takes
        that path. Delivered in the reference's rank order with an exact
        count; `order_by` is refused (D14). `NOT` is two-valued here, as on
        every effective-view read; search's `where` is three-valued.
        """
        if order_by:
            raise _result_references.unsupported(
                "argument", argument="order_by",
                message=f"{operator}: order_by does not apply to a result reference; "
                "its members are delivered in rank order",
                fix_hint="drop order_by, or read the members by node id to order them",
            )
        date_started = _date_call_started()
        page = max(1, min(int(limit), 1000))
        off = max(0, int(offset))
        with _open(
            generation_id, operator_name=operator, authority=authority,
            reference=str(ref_id),
        ) as h:
            # #3666: date conditions compile like any other; the values
            # read to do so are the reference's own members'.
            date_report = _DateConditionReport()
            caller_predicate = predicate
            dated_under_access_rules = False
            reference_budget = _date_budget(
                operator, caller_predicate, kind="nodes", started_at=date_started,
            )
            if _holds_date_condition(predicate):
                dated_under_access_rules = deployment_requires_nomos_runtime(
                    h.session.deployment_name
                )
                predicate = _resolve_date_conditions(
                    h.conn, h.session, kind="nodes", predicate=predicate,
                    operator=operator, report=date_report,
                    nomos_runtime=dated_under_access_rules, budget=reference_budget,
                    candidates_sql="SELECT unnest(%(rr_member_ids)s::text[])",
                    candidate_params={"rr_member_ids": [
                        str(member["node_id"]) for member in h.reference_entry.members
                    ]},
                )
            # Slice 7a: the reference's predicate read goes through the
            # effective-view page with the reader view, like the explicit
            # node-id call, so no Nomos shape needs refusing here.
            try:
                rows = _reference_rows(h, predicate, date_budget=reference_budget)
            except _ReferencePredicateUnsupported as exc:
                # The refusal the explicit node-id call gives for a predicate
                # the effective-view compiler cannot lower (#3222, L1). It
                # comes after the member hydration read; only the substring
                # `contains` form is refused before any SQL.
                return _operator_refusal(
                    operator,
                    code="unsupported_operator_form",
                    message=str(exc),
                    fallback_operator="query",
                )
            except SubstrateError as exc:
                # A refusal that echoes the predicate echoes the caller's,
                # never its date rewrite (#3666).
                if isinstance(getattr(exc, "data", None), dict) and "predicate" in exc.data:
                    exc.data["predicate"] = caller_predicate
                raise
            declared_registry = _declared_registry_for(h.conn)
            if predicate not in (None, True, {}):
                # The same post-read check the explicit path makes.
                # A date condition under access rules is three-valued (a
                # stored date not in ISO form is unknown), as on the
                # explicit path; every other predicate stays two-valued here.
                checked: list[dict[str, Any]] = []
                with _date_doing(reference_budget, "reference_check"):
                    for position, row in enumerate(rows):
                        if reference_budget is not None and (
                            position % _DATE_DEADLINE_CHECK_EVERY == 0
                        ):
                            # #3666: the loop ends with the call's date budget.
                            reference_budget.check()
                        if _operator_read_check(
                            row, predicate, declared=declared_registry,
                            nomos_runtime=dated_under_access_rules,
                        ):
                            checked.append(row)
                rows = checked
                _mark_date_phase("reference_check", reference_budget)
            child: dict[str, Any] | None = None
            if retain:
                child = _reference_child(h, rows, caller_predicate)
            total = len(rows)
            page_rows = _with_graph_edge_ids("nodes", rows[off: off + page])
            if _operator_projection_needs_provenance(kind="nodes", projection=projection):
                page_rows = _operator_fence_node_rows(
                    h.conn, h.session, kind="nodes", rows=page_rows,
                )
                columns, projected = _operator_project_rows("nodes", page_rows, projection)
                columns, projected = _operator_add_provenance_column(
                    columns=columns, projected=projected, source_rows=page_rows,
                )
            else:
                columns, projected = _operator_project_rows("nodes", page_rows, projection)
            subset = predicate not in (None, True, {})
            envelope = _operator_envelope(
                rows=projected,
                columns=columns,
                page=page,
                offset=off,
                generation_id=h.session.generation_id,
                scan_truncated=off + page < total,
                count_fields={
                    "total_count": total,
                    "total_count_basis": "exact",
                    # Exact over the reference's rows only, like the
                    # reference aggregate: never "exhaustive", which reads
                    # as "of the whole graph" (the envelope turns this basis
                    # into a `result_reference` retrieval bound, #3666).
                    "retrieval_basis": "result_reference_members",
                    "selection_basis": "reference_subset" if subset else "reference",
                },
            )
            payload: dict[str, Any] = {
                **envelope,
                "operator_name": operator,
                "input": {"ref": ref_id},
                # How many rows the reference holds, which can be fewer than
                # its search matched; the text reply says so too.
                "reference_members": len(h.reference_entry.members),
            }
            if operator == "filter":
                payload["predicate"] = caller_predicate
            else:
                payload["projection"] = projection
            payload.update(date_report.payload())
            if child is not None:
                payload["result_reference"] = child
            return _stamp_operator(_maybe_stamp_advanced(h, payload), operator)

    def _reference_child(
        h: Any, rows: list[dict[str, Any]], predicate: Any,
    ) -> dict[str, Any]:
        """Draft a child of the validated reference: its members that match,
        in the parent's rank order. The parent is never changed."""
        entry = h.reference_entry
        matching = {str(row["node_id"]) for row in rows}
        members = [
            dict(member) for member in entry.members
            if str(member["node_id"]) in matching
        ]
        binding = {
            **dict(entry.binding),
            "attributes": _member_attribute_digest(
                h.conn, h.session, [m["node_id"] for m in members],
            ),
            "selection": _result_references.selection_fingerprint(
                {"parent": entry.id, "predicate": predicate},
            ),
        }
        child_id = _result_references.draft(
            reader_key=h.reference_reader,
            binding=binding,
            members=members,
            parent=entry.id,
            header_claim=h.request_header_claim,
            multi_user=multi_user_principals,
        )
        return _reference_payload(child_id, parent=entry.id)

    def _reference_payload(ref_id: str, *, parent: str | None) -> dict[str, Any]:
        expires = int(ref_id.split(".")[2])
        return {
            "id": ref_id,
            "parent": parent,
            "order": _result_references.ORDER,
            "expires_at": _result_references.expires_at_text(expires),
        }

    # -- live result sets: reads of a retained live answer ----------------

    def _live_sample(
        retained: Any,
        *,
        operator: str,
        authority: Any,
        generation: Any,
        session_id: str,
        status: str,
        decision: str,
        principal: str,
        payload: Any,
        attrs: Mapping[str, Any],
    ) -> None:
        """One sample row for one read of a retained set, in its own
        transaction, written as the live dispatcher writes its own. A failed
        write fails the read, as it fails a live call."""
        from okg.substrate.external_backed.call_sample import (
            record_live_relation_call_sample,
        )
        from okg.substrate.external_backed.live_dispatch import (
            SAMPLE_FAILURE_EVENT,
            LiveSampleWriteFailed,
        )

        failure: LiveSampleWriteFailed | None = None
        try:
            with _open(generation, authority=authority) as sh:
                record_live_relation_call_sample(
                    sh.conn,
                    deployment_name=retained.deployment_name,
                    tool_name=operator,
                    trace_id=str(uuid.uuid4()),
                    session_id=session_id,
                    relation=retained.relation_name,
                    node_id=retained.node_id,
                    principal=principal,
                    decision=decision,
                    status=status,
                    payload=payload,
                    refetch=retained.refetch if payload is not None else None,
                    attrs=dict(attrs),
                )
        except Exception as exc:  # noqa: BLE001 — typed and raised below.
            # The type only: a database error's text can carry the DSN. The
            # set id is never logged.
            log.error(
                "%s relation=%s node=%s tool=%s error=%s",
                SAMPLE_FAILURE_EVENT, retained.relation_name, retained.node_id,
                operator, type(exc).__name__,
            )
            failure = LiveSampleWriteFailed(
                f"a read of a retained answer of live relation "
                f"{retained.relation_name!r} could not write its call sample "
                f"({type(exc).__name__}); the read fails, as a live call does.",
                data={"relation": retained.relation_name, "node_id": retained.node_id,
                      "adapter_ran": False, "event": SAMPLE_FAILURE_EVENT},
                fix_hint=(
                    "check the call-sample hypertable for the relation's "
                    "retention class and the deployment's write connection"
                ),
            )
        if failure is not None:
            raise failure

    def _live_sample_attrs(retained: Any, outcome: Any) -> dict[str, Any]:
        return {
            # The original call's fingerprint: the link to its sample row.
            "args_fingerprint": retained.args_fingerprint,
            "action": retained.relation.action,
            "adapter_kind": retained.source,
            "reason_code": outcome.reason,
            "adapter_ran": False,
            "served_from": _live_results.SERVED_FROM,
            **({"gate_raised": True} if outcome.raised else {}),
        }

    def _live_regate(retained: Any) -> Any:
        """Ask the gate the expand asked, about the same relation, node and
        subject. Any answer other than allow -- an error, an unresolved
        subject, a missing gate -- is a deny."""
        from okg.substrate.external_backed.live_dispatch import (
            GATE_ERROR_REASON,
            PRINCIPAL_UNRESOLVED_REASON,
            UNRESOLVED_PRINCIPAL,
            GateOutcome,
        )

        relation = retained.relation
        subject = (retained.subject or "").strip()
        if not subject:
            return GateOutcome(
                allowed=False, reason=PRINCIPAL_UNRESOLVED_REASON,
                principal=UNRESOLVED_PRINCIPAL, action=relation.action,
            )
        gate = getattr(mcp, "_okg_live_gate", None)
        if gate is None:
            return GateOutcome(
                allowed=False, reason=GATE_ERROR_REASON, principal=subject,
                action=relation.action, raised=True,
            )
        try:
            outcome = gate(relation, subject, retained.node_id)
        except Exception as exc:  # noqa: BLE001 — fail closed.
            log.warning(
                "live gate raised on a retained read of %r (%s); denying",
                relation.name, type(exc).__name__,
            )
            return GateOutcome(
                allowed=False, reason=GATE_ERROR_REASON, principal=subject,
                action=relation.action, raised=True,
            )
        if not isinstance(outcome, GateOutcome):
            return GateOutcome(
                allowed=False, reason=GATE_ERROR_REASON, principal=subject,
                action=relation.action, raised=True,
            )
        return outcome

    def _live_open(operator: str, *, ref_id: str, authority: Any) -> dict[str, Any]:
        """Look a retained set up and re-check it, once per read call.

        (a) the anchor node is read again under the reader's authority at the
        current default generation; (b) the binding (authority digest, policy
        pin, catalog version, deployment, database, branch) is compared; (c)
        the live gate is asked again. A refusal in (a)-(c) drops the set,
        writes the read's sample row and never calls the adapter. The rows
        are parsed outside the store lock and kept for the call's
        byte-fitting reruns.

        First of all (issue #3923, #3666 review M2): a kept live answer, raw
        or typed, is not served while access rules are on (audit or enforce)
        or when their state cannot be told, not even one kept while they were
        off, as `expand(next=...)` refuses its next page. No row is read.
        """
        from okg.substrate.mcp.tools import inspect_node

        _live_access_rules.refuse_live_under_access_rules(
            mcp, f"{operator} reads a kept live answer",
            tool_called=operator,
        )
        memo = _result_references.call_memo()
        key = "live-read:" + ref_id
        if memo is not None and key in memo:
            return memo[key]
        store = _live_results.live_store()
        with _open(None, operator_name=operator, authority=authority) as h:
            reader = _continuation.reader_key(
                authority, h.request_header_claim, multi_user_principals,
            )
            entry = store.lookup(ref_id, reader_key=reader)
            retained = entry.payload
            generation = h.session.generation_id
            session_id = getattr(h.session, "logical_session_id", None) or str(
                getattr(h.session, "session_id", "") or "mcp"
            )
            refusal: SubstrateError | None = None
            # A live read of a live type (#3666, live-graph-rucio-sites) is
            # bound to its type, not to a stored anchor node: no anchor read.
            typed = isinstance(retained, _live_resolver.LiveTypedSet)
            anchor = None if typed else inspect_node(
                h.conn, h.session, retained.node_id, select=["node_id", "subtype"],
                include_documents=False,
            )
            if typed:
                field = _result_references.binding_mismatch(
                    entry.binding, _live_binding(h.conn, h.session),
                )
                if field is not None:
                    refusal = _live_results.invalid("binding_mismatch", field=field)
            elif anchor is None:
                refusal = _live_results.invalid("binding_mismatch", field="anchor")
            else:
                field = _result_references.binding_mismatch(
                    entry.binding, _live_binding(h.conn, h.session),
                )
                if field is not None:
                    refusal = _live_results.invalid("binding_mismatch", field=field)
        if typed and refusal is None:
            refusal = _live_typed_binding_refusal(retained)
        refusal_reason: str | None = None
        if refusal is not None:
            from okg.substrate.external_backed.live_dispatch import GateOutcome

            refusal_reason = "binding_mismatch"
            outcome = GateOutcome(
                allowed=False, reason="live_result.binding_mismatch",
                principal=retained.subject or "unresolved",
                action=retained.relation.action,
            )
        else:
            outcome = _live_regate_typed(retained) if typed else _live_regate(retained)
            if not outcome.allowed:
                refusal_reason = "live_result_denied"
                refusal = _live_results.LiveResultDenied(
                    f"the live gate did not allow reading the retained answer of "
                    f"relation {retained.relation_name!r}",
                    data={"reason": outcome.reason, "relation": retained.relation_name},
                    fix_hint="the retained answer was dropped; ask whoever owns the "
                    "live policy, or call expand(live=..., retain=true) again",
                )
        if refusal is not None:
            store.discard(ref_id)
            _live_sample(
                retained, operator=operator, authority=authority,
                generation=generation, session_id=session_id,
                status="denied", decision="deny", principal=outcome.principal,
                payload=None,
                attrs={**_live_sample_attrs(retained, outcome),
                       "read_refused": refusal_reason},
            )
            raise refusal
        state = {
            "set": retained,
            "rows": _live_results.decode_rows(retained.rows_json),
            "generation": generation,
            "session_id": session_id,
            "outcome": outcome,
            **({
                "gates": dict(retained.gate_outcomes),
                "coverage": dict(retained.coverage),
                "live_line": retained.live_line,
            } if typed else {}),
        }
        if memo is not None:
            memo[key] = state

        def finished(payload: Any) -> None:
            served_key = "groups" if operator == "aggregate" else "items"
            if isinstance(payload, Mapping) and payload.get("status") != "refused":
                if operator == "expand":
                    # A `next` page of a cut live answer (Live build 3).
                    live = payload.get("external_live")
                    rows = live.get("rows") if isinstance(live, Mapping) else None
                    served = list(rows) if isinstance(rows, list) else []
                else:
                    served = list(payload.get(served_key) or [])
                status = "ok"
            else:
                served, status = [], "error"
            _live_sample(
                retained, operator=operator, authority=authority,
                generation=generation, session_id=session_id,
                status=status, decision="allow", principal=outcome.principal,
                payload=served,
                attrs={
                    **_live_sample_attrs(retained, outcome),
                    "rows_served": len(served),
                    "served_rows_sha256": _live_results.served_rows_sha256(served),
                },
            )

        _result_references.on_finish(finished)
        return state

    # -- live types (#3666, PACT live-graph-rucio-sites): what the resolver
    # -- (`live_resolver`) needs from this server, and the shared pre-check.

    def _live_typed_binding_refusal(retained: Any) -> SubstrateError | None:
        """A kept live-type set read again: the type's declaration and okg's
        logical session must be the ones it was read under (the reader is
        checked by the store lookup). On stateless HTTP the reader alone."""
        registry = getattr(mcp, "_okg_live_types", None)
        live_type = registry.by_subtype(retained.subtype) if registry is not None else None
        if (
            live_type is None
            or _live_resolver.declaration_digest(live_type) != retained.declaration_digest
        ):
            return _live_results.invalid("binding_mismatch", field="declaration")
        if not retained.stateless and retained.logical_session is not None:
            identity = _current_logical_identity()
            current = (
                str(logical_sessions.snapshot(identity).logical_session_id)
                if identity is not None else None
            )
            if current != retained.logical_session:
                return _live_results.invalid("binding_mismatch", field="session")
        return None

    def _live_regate_typed(retained: Any) -> Any:
        """Ask the gate again about every relation the set was read with;
        the first deny (or error) is the answer, else the listing's allow."""
        from okg.substrate.external_backed.live_dispatch import (
            GATE_ERROR_REASON,
            GateOutcome,
            build_live_gate,
            gate_for_call,
        )

        gate = getattr(mcp, "_okg_live_gate", None)
        if gate is None:
            gate = build_live_gate(deployment_name)
            mcp._okg_live_gate = gate  # type: ignore[attr-defined]
        answer = None
        for relation, first in sorted(retained.gate_outcomes.items()):
            if not getattr(first, "allowed", False):
                continue
            bound = retained.bound[relation]
            try:
                outcome = gate_for_call(bound, gate, retained.node_id)
            except Exception as exc:  # noqa: BLE001 — fail closed.
                log.warning(
                    "live gate raised on a kept live-type read of %r (%s); denying",
                    relation, type(exc).__name__,
                )
                outcome = GateOutcome(
                    allowed=False, reason=GATE_ERROR_REASON,
                    principal=getattr(first, "principal", "unresolved"),
                    action=bound.relation.action, raised=True,
                )
            if not isinstance(outcome, GateOutcome) or not outcome.allowed:
                return outcome if isinstance(outcome, GateOutcome) else GateOutcome(
                    allowed=False, reason=GATE_ERROR_REASON,
                    principal=getattr(first, "principal", "unresolved"),
                    action=bound.relation.action, raised=True,
                )
            if relation == retained.relation_name or answer is None:
                answer = outcome
        return answer if answer is not None else GateOutcome(
            allowed=False, reason=GATE_ERROR_REASON, principal="unresolved",
            action=retained.relation.action, raised=True,
        )

    def _refuse_live_aggregate_arguments(by: Any) -> None:
        """`aggregate` over live rows: a date period reads graph records."""
        if _has_period_field(by):
            raise InvalidOperatorArguments(
                "aggregate: a date period reads graph records; a live "
                "result set holds external rows",
                data={"by": [str(f) for f in by]},
                fix_hint='group a live set by its own columns, e.g. by=["attrs.<field>"]',
            )

    def _refuse_live_filter_dates(predicate: Any) -> None:
        """`filter` over live rows: a date field reads a record type's
        declared attribute; a live row has none, so the clause would match
        nothing and read as "no such rows" (#3666)."""
        if _holds_date_condition(predicate):
            raise InvalidOperatorArguments(
                "filter: a date field reads graph records; a live result "
                "set holds external rows",
                data={"reason": "date_condition_on_live_rows"},
                fix_hint=(
                    "compare the live set's own column, e.g. "
                    '{"op":"gt","field":"attrs.<field>","value":"2025-01-01"}'
                    " (compared as text)"
                ),
            )

    def _refuse_live_retain(operator: str, retain: Any) -> None:
        if retain:
            raise _live_results.refuse_argument("retain", operator)

    def _live_argument_precheck(operator: str, arguments: Mapping[str, Any]) -> Any:
        """The tool body's own argument refusals for a live read, before any
        upstream call (#3666, N1). The tool body calls the same functions on
        its kept-set path, so there is one rule in one place. Returns a
        refusal payload (the text-digest refusal, returned as the body
        returns it), raises a typed refusal, or returns None."""
        scope = as_mapping(arguments.get("input"))
        predicate = as_mapping(arguments.get("predicate"))
        if operator == "filter":
            refusal = _text_digest_operator_refusal(
                "filter", input=scope, predicate=predicate,
                order_by=arguments.get("order_by"),
            )
            if refusal is not None:
                return refusal
            _refuse_live_filter_dates(predicate)
            _refuse_live_retain("filter", arguments.get("retain"))
        elif operator == "aggregate":
            refusal = _text_digest_operator_refusal(
                "aggregate", input=scope, by=arguments.get("by"),
                metrics=arguments.get("metrics"),
            )
            if refusal is not None:
                return refusal
            _refuse_live_aggregate_arguments(arguments.get("by"))
        elif operator == "map":
            refusal = _text_digest_operator_refusal("map", input=scope)
            if refusal is not None:
                return refusal
        return None

    mcp._okg_live_argument_precheck = _live_argument_precheck  # type: ignore[attr-defined]

    if multi_user_principals:
        def _identity_precheck(tool_name: str, authority: Any) -> None:
            """The per-tool gate's identity-channel check, asked by the
            live-graph resolver before any upstream call; a refusal is
            recorded as the gate records it."""
            try:
                refuse_conflicting_identity_channel(
                    tool_name, as_mapping(authority), principal_mapping,
                )
            except SubstrateError as exc:
                with _pool_connection(pool) as conn:
                    _persist_principal_refusal(
                        conn, error=exc, deployment_name=deployment_name,
                        surface="mcp.tool.call", operator_name=tool_name,
                    )
                raise

        mcp._okg_identity_precheck = _identity_precheck  # type: ignore[attr-defined]

    def _live_graph_prepare(
        operator: str, *, authority: Any, subtypes: Sequence[str], context: Any,
    ) -> Any:
        """Phase 1 of a live read, on a call worker: the declared relations
        of the types read, the reader, okg's logical session, and the call
        sample writer. One short read; nothing is held after it."""
        from okg.substrate.external_backed.live_dispatch import (
            GATE_ERROR_REASON,
            GateOutcome,
            LiveAdapterUnavailable,
            LiveRelationUndeclared,
            LiveSampleWriteFailed,
            build_live_gate,
            declared_live_relations,
            gate_for_call,
            validate_live_arguments,
        )
        from okg.substrate.external_backed.live_relations import (
            BoundLiveRelation,
            compile_args_model,
        )

        bindings = getattr(mcp, "_okg_live_bindings", None) or {}
        runners = getattr(mcp, "_okg_live_adapters", None) or {}
        relation_runners = getattr(mcp, "_okg_live_relation_runners", None) or {}
        with _open(None, operator_name=operator, authority=authority) as h:
            pinned_generation = h.session.generation_id
            declared: dict[str, Any] = {}
            for subtype in subtypes:
                declared.update(declared_live_relations(h.conn, h.session, subtype))
            reader_of_call = _continuation.reader_key(
                authority, h.request_header_claim, multi_user_principals,
            )
            sample_session = getattr(h.session, "logical_session_id", None) or str(
                getattr(h.session, "session_id", "") or "mcp"
            )
            # What a kept live-type set is bound to and counted against
            # (the keep rule, `live_resolver.seed_call_memo`).
            store_binding = _live_binding(h.conn, h.session)
            claim = h.request_header_claim
        identity = _logical_identity_from_context(context)
        logical = (
            str(logical_sessions.snapshot(identity).logical_session_id)
            if identity is not None else None
        )
        is_stateless = bool(getattr(getattr(mcp, "settings", None), "stateless_http", False))
        live_gate = getattr(mcp, "_okg_live_gate", None)
        if live_gate is None:
            live_gate = build_live_gate(deployment_name)
            mcp._okg_live_gate = live_gate  # type: ignore[attr-defined]
        bound_cache: dict[str, Any] = {}
        models: dict[str, Any] = {}

        class _PreparedLiveRead:
            generation = pinned_generation
            reader_key = reader_of_call
            logical_session = logical
            session_id = sample_session
            stateless = is_stateless
            binding = store_binding
            header_claim = claim
            deployment = deployment_name
            multi_user = bool(multi_user_principals)

            def bound(self, name: str) -> Any:
                if name not in bound_cache:
                    if name not in declared or name not in bindings:
                        raise LiveRelationUndeclared(
                            f"live relation {name!r} is not declared on "
                            f"{list(subtypes)} or not bound on this deployment",
                            data={"relation": name, "subtypes": list(subtypes),
                                  "declared": sorted(declared)},
                            fix_hint="check the live type's declaration and bindings",
                        )
                    bound_cache[name] = BoundLiveRelation(
                        relation=declared[name], binding=bindings[name],
                    )
                    models[name] = compile_args_model(declared[name])
                return bound_cache[name]

            def adapter(self, name: str) -> Any:
                kind = self.bound(name).binding.kind
                runner = relation_runners.get(name) or runners.get(kind)
                if runner is None:
                    raise LiveAdapterUnavailable(
                        f"live relation {name!r} binds adapter kind {kind!r}, for "
                        "which this runtime has no registered runner",
                        data={"relation": name, "kind": kind, "registered": sorted(runners)},
                        fix_hint="register a runner for this kind, or drop the binding",
                    )
                return runner

            def validate(self, name: str, args: Mapping[str, Any]) -> dict[str, Any]:
                bound = self.bound(name)
                return validate_live_arguments(bound, args, args_model=models[name])

            def gate(self, bound: Any, node_id: str) -> Any:
                try:
                    outcome = gate_for_call(bound, live_gate, node_id)
                except Exception as exc:  # noqa: BLE001 — fail closed.
                    log.warning(
                        "live gate raised on a live-type read of %r (%s); denying",
                        bound.relation.name, type(exc).__name__,
                    )
                    outcome = None
                if not isinstance(outcome, GateOutcome):
                    return GateOutcome(
                        allowed=False, reason=GATE_ERROR_REASON,
                        principal="unresolved", action=bound.relation.action,
                        raised=True,
                    )
                return outcome

            def write_samples(self, samples: Sequence[Mapping[str, Any]]) -> None:
                """One call sample per upstream call, in one short write.
                A sample that cannot be written fails the read."""
                from okg.substrate.external_backed.call_sample import (
                    record_live_relation_call_sample,
                )

                try:
                    with _open(pinned_generation, authority=authority) as sh:
                        for sample in samples:
                            record_live_relation_call_sample(
                                sh.conn,
                                deployment_name=deployment_name,
                                tool_name=operator,
                                trace_id=str(uuid.uuid4()),
                                session_id=sample_session,
                                **sample,
                            )
                except Exception as exc:  # noqa: BLE001 — re-raised, typed.
                    relation = samples[0].get("relation") if samples else None
                    raise LiveSampleWriteFailed(
                        f"{operator}: live relation {relation!r} could not write "
                        f"its call samples ({type(exc).__name__}); the read fails: "
                        "an outside call with no record is what the sample "
                        "exists to prevent",
                        data={"relation": relation, "samples": len(samples),
                              "adapter_ran": True},
                        fix_hint=(
                            "check the call-sample table and the deployment's "
                            "write connection"
                        ),
                    ) from None

        return _PreparedLiveRead()

    mcp._okg_live_graph_prepare = _live_graph_prepare  # type: ignore[attr-defined]

    def _live_kept_set(operator: str, ref_id: str, authority: Any) -> Any:
        """The kept live-type set `ref_id` names for this reader, still bound
        to its declaration and session, or None (the tool body then reads or
        refuses it as it always does). Asked by the resolver before it grows
        a set (N6); no upstream call. Refused, as `_live_open` refuses, while
        access rules are on or cannot be told (#3666 review M2)."""
        _live_access_rules.refuse_live_under_access_rules(
            mcp, f"{operator} reads a kept live answer",
            tool_called=operator,
        )
        try:
            with _open(None, operator_name=operator, authority=authority) as h:
                reader = _continuation.reader_key(
                    authority, h.request_header_claim, multi_user_principals,
                )
                entry = _live_results.live_store().lookup(ref_id, reader_key=reader)
                if _result_references.binding_mismatch(
                    entry.binding, _live_binding(h.conn, h.session),
                ) is not None:
                    return None
        except _result_references.ResultReferenceInvalid:
            return None
        retained = entry.payload
        if not isinstance(retained, _live_resolver.LiveTypedSet):
            return None
        if _live_typed_binding_refusal(retained) is not None:
            return None
        return retained

    mcp._okg_live_kept_set = _live_kept_set  # type: ignore[attr-defined]

    def _live_next_gone(
        exc: Any, *, set_id: Optional[str], authority: Any,
    ) -> SubstrateError:
        """A `next` token whose kept answer cannot be read, in plain words,
        with the one call that asks the live system again."""
        from okg.substrate.mcp.continuation import ContinuationTokenInvalid

        data = getattr(exc, "data", None)
        reason = str((data or {}).get("reason") or "unknown")
        call = None
        if set_id is not None:
            try:
                with _open(None, operator_name="expand", authority=authority) as h:
                    reader = _continuation.reader_key(
                        authority, h.request_header_claim, multi_user_principals,
                    )
                call = _live_results.remembered_call(set_id, reader)
            except Exception as lookup_exc:  # noqa: BLE001 — the refusal stands without it.
                log.warning(
                    "live next: reading the reader of a refused token raised %s",
                    type(lookup_exc).__name__,
                )
        message = _live_results.NEXT_GONE_MESSAGES.get(reason) or str(exc)
        from okg.substrate.mcp.result_text import call_text

        written = call_text(call) if call is not None else None
        if written is not None:
            fix_hint = (
                f"call {written} again: it asks the live system anew "
                "and returns a new next token"
            )
        else:
            fix_hint = (
                "call the expand(live=...) that gave this token again: it asks "
                "the live system anew and returns a new next token"
            )
        refusal = ContinuationTokenInvalid(
            message,
            data={"reason": reason, **({"retry_call": call} if written is not None else {})},
            fix_hint=fix_hint,
        )
        return refusal

    def _expand_live_page(
        arguments: Mapping[str, Any],
        measure: Callable[[dict[str, Any]], int],
        budget: int,
    ) -> dict[str, Any]:
        """`expand(next=<token>)` on a cut live answer (#3666, Live build 3).

        The next page of the kept answer, from the row the token names, each
        row with all its fields, as many rows as the reply budget holds. No
        adapter call: the rows are the kept set's. The read re-checks the
        anchor node, the binding and the live gate exactly as `filter` over
        a kept answer does, and writes its own sample row.
        """
        named = sorted(
            key for key, default in (
                ("input", None), ("edge_type", None), ("path", None),
                ("target_subtype", None), ("continuation_token", None),
                ("filter", None), ("select", None), ("order_by", None),
                ("live", None),
            ) if arguments.get(key) not in (default, False)
        ) + [
            key for key, default in (
                ("direction", "out"), ("depth", 1), ("limit", 50), ("offset", 0),
                ("include_siblings", False),
            ) if arguments.get(key) not in (None, default)
        ]
        if named:
            refusal = _operator_refusal(
                "expand",
                code="next_with_read_arguments",
                message=(
                    "expand: `next` carries the whole read and cannot be "
                    f"combined with {', '.join(sorted(named))}"
                ),
            )
            refusal["error"]["fix_hint"] = "call expand(next=<token>) alone"
            return refusal
        # Issue #3923: a live answer is not served while access rules are on
        # (or when their state cannot be told), not even the next page of one
        # kept while they were off.
        _live_access_rules.refuse_live_under_access_rules(
            mcp,
            "expand(next=...) continues a live answer",
            tool_called="expand",
        )
        token = arguments.get("next")
        authority = arguments.get("authority")
        set_id: Optional[str] = _live_results.next_set_id(token)
        try:
            set_id, offset = _live_results.parse_next_token(token)
            state = _live_open("expand", ref_id=set_id, authority=authority)
        except _result_references.ResultReferenceInvalid as exc:
            raise _live_next_gone(exc, set_id=set_id, authority=authority) from None
        except SubstrateError as exc:
            if getattr(exc, "code", None) == "continuation_token_invalid":
                raise _live_next_gone(exc, set_id=set_id, authority=authority) from None
            raise
        retained = state["set"]
        frame = {
            "status": "ok",
            "operator_name": "expand",
            "result_kind": "live_page",
            "node_id": retained.node_id,
        }

        def fits(envelope: dict[str, Any]) -> bool:
            return measure({**frame, "external_live": envelope}) <= budget

        envelope = _live_results.page_envelope(
            retained, state["rows"], offset=offset, fits=fits, set_id=set_id,
        )
        if not envelope.get("rows_returned") and offset < len(state["rows"]):
            return _operator_refusal(
                "expand",
                code="response_byte_budget_exceeded",
                message=(
                    f"expand: row {offset + 1} of the kept live answer does not "
                    f"fit the {budget}-byte reply even with its values cut"
                ),
            )
        return {**frame, "external_live": envelope}

    def _live_served_fields(
        retained: Any, ref_id: str, state: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        fields = _live_served_fields_of(retained, ref_id)
        # A read the live-graph resolver made in this call (#3666): the
        # reply names the set's real id only when it was kept (D8).
        fresh = _live_resolver.fresh_served_fields(state)
        if fresh is not None:
            for key, value in fresh.items():
                if value is None:
                    fields.pop(key, None)
                else:
                    fields[key] = value
        return fields

    def _live_served_fields_of(retained: Any, ref_id: str) -> dict[str, Any]:
        return {
            "input": {"ref": ref_id},
            "served_from": _live_results.SERVED_FROM,
            "trust": "untrusted",
            # What the original call reported. Nested because `rows` is the
            # row-list key of every operator result.
            "original_call": {
                "relation": retained.relation_name,
                "observed_at": retained.observed_at,
                "digest": retained.digest,
                "rows": retained.row_count,
            },
            "query_plan_summary": "retained live answer, in memory",
        }

    def _live_read(
        operator: str,
        *,
        ref_id: Any,
        authority: Any,
        generation_id: Any,
        limit: int,
        offset: int,
        predicate: Any = None,
        projection: Any = None,
        projection_argument: str = "projection",
        by: Any = None,
        metrics: Any = None,
        order_by: Any = None,
        retain: bool = False,
    ) -> dict[str, Any]:
        """`filter`, `map` or `aggregate` over a retained live answer.

        Each row is `{"attrs": row}` as the upstream system returned it;
        predicates use the graph `filter` grammar on `attrs.<field>` and run
        in memory, outside the store lock. Rows keep upstream order. Served
        values are wrapped as untrusted text again on every page.
        """
        order_key: Optional[tuple[str, bool]] = None
        if order_by:
            if operator == "aggregate":
                raise _live_results.refuse_argument("order_by", operator)
            order_key = _live_reader.parse_order_by(order_by)
        _refuse_live_retain(operator, retain)
        if metrics is not None and operator != "aggregate":
            raise _live_results.refuse_argument("metrics", operator)
        _live_results.check_predicate(predicate)
        _live_results.check_projection(projection, argument=projection_argument)
        fields = _live_results.check_by(by) if operator == "aggregate" else []
        parsed_metrics = (
            _live_results.check_metrics(metrics) if operator == "aggregate" else []
        )
        ref = str(ref_id)
        state = _live_open(operator, ref_id=ref, authority=authority)
        if generation_id is not None and str(generation_id) != str(state["generation"]):
            # The anchor is checked at the current default generation; a
            # different pin is a conflict, never silently ignored. (The
            # byte-fitting reruns pass the first page's own generation.)
            raise _live_results.invalid("generation_conflict")
        retained = state["set"]
        rows = state["rows"]
        relation = retained.relation_name
        off = max(0, int(offset))
        # Live typed rows (#3666): record columns, field states, the
        # typo check, then the condition, before any grouping or paging.
        names = _live_reader.names_of(predicate, fields, parsed_metrics)
        _live_reader.check_record_columns(rows, names, operator)
        # Refused as a probable typo only on a set that is the whole listing
        # (a raw kept answer counts as whole); on a narrowed live set the
        # key is noted, with the resolver's own note for the conditions it
        # applied before the body saw the rows.
        keys_absent = _live_reader.check_keys_seen(
            rows, names, operator,
            known=getattr(retained, "declared_keys", ()) or (),
            whole=bool(getattr(retained, "whole_listing", True)),
        )
        keys_absent = _live_reader.merge_keys_absent(
            state.get("keys_absent") if state.get("fresh") else None, keys_absent,
        )
        readable, not_read = _live_reader.split_not_read(rows, names.keys)
        matched, not_comparable = _live_reader.filter_rows(
            readable, predicate,
            lambda row: _operator_predicate_matches(
                {"attrs": row}, predicate, declared=None,
            ),
        )
        if state.get("fresh"):
            # Rows the resolver set aside because a condition it applied
            # could not be decided on them never reach these rows: counted
            # here all the same, with the values it could not compare.
            not_read.absorb(state.get("condition_not_read"))
            not_comparable.update(state.get("condition_not_comparable") or {})
        ids_checked = bool(getattr(retained, "ids_checked", False))
        reader_notes: dict[str, Any] = {}
        if (report_not_read := not_read.report()) is not None:
            # Node ids are built from upstream names: served fenced, as
            # every other upstream value (#3666).
            report_not_read["node_ids"] = _live_results.fence_rows(
                report_not_read["node_ids"], relation,
            )
            reader_notes["not_read"] = report_not_read
        if (report_comparable := _live_reader.not_comparable_report(not_comparable)):
            reader_notes["not_comparable"] = report_comparable
        if keys_absent is not None:
            reader_notes["keys_absent"] = keys_absent
        if operator == "aggregate":
            report = _live_results.group_report(
                [_live_reader.flat_row(row) for row in matched], fields, parsed_metrics,
            )
            groups, skipped = report.groups, report.skipped
            page = max(1, min(int(limit), _live_results.MAX_GROUPS))
            visible = groups[off: off + page + 1]
            truncated = len(visible) > page
            cursor = _live_results.page_cursor(off, page, truncated)
            payload: dict[str, Any] = {
                # A key a row does not carry is the plain `(absent)` group
                # (#3666, lane ri), apart from a stored null.
                "groups": _live_results.fenced_groups(visible[:page], fields, relation),
                "by": fields,
                "metrics": [_live_results.metric_label(m) for m in parsed_metrics],
                # Rows a computed metric left out, per column: never read as 0.
                **({"values_skipped": skipped} if any(
                    sum(reasons.values()) for reasons in skipped.values()
                ) else {}),
                **({"absent_keys": report.absent} if report.absent else {}),
                # A sum that added a non-integer value is not exact.
                **({"sum_not_exact": report.inexact} if report.inexact else {}),
                "rows_counted": len(matched),
                # What the totals cover, printed first.
                "totals_cover": _live_results.totals_cover(retained, len(matched)),
                **reader_notes,
                "total_count": len(groups),
                "groups_total": len(groups),
                "total_count_basis": "exact",
                "retrieval_exhaustive": True,
                "selection_basis": _live_results.SERVED_FROM,
                "truncated": truncated,
                "limit": page,
                "offset": off,
                **cursor,
                "pinned_generation_id": state["generation"],
                **_live_served_fields(retained, ref, state),
            }
            return _stamp_operator(_live_resolver.with_live_line(payload, state), operator)
        page = max(1, min(int(limit), 1000))
        if order_key is not None:
            matched = _live_reader.order_rows(matched, *order_key, operator)
        total = len(matched)
        page_rows = matched[off: off + page]
        # A typed row's upstream fields are `attrs`; a raw row is its own.
        raw_page = [_live_reader.attrs_of(row) for row in page_rows]
        served_page = _live_results.fence_rows(raw_page, relation)
        if projection is None:
            typed = [_live_reader.is_typed(row) for row in page_rows]
            columns = ["node_id", "subtype", "attrs", "live"] if page_rows and all(
                typed
            ) else ["attrs"]
            # Upstream keys in the field states are served neutralised, under
            # the same served name as in `attrs`; a node id built from an
            # unchecked upstream name is served as untrusted text (#3666).
            projected = [
                (
                    {"node_id": _live_results.served_node_id(
                        row["node_id"], relation, checked=ids_checked),
                     "subtype": row["subtype"],
                     "attrs": served,
                     "live": _live_results.served_live(
                         row["live"], dict(zip((str(k) for k in raw), served)),
                     )}
                    if is_typed else {"attrs": served}
                )
                for row, raw, served, is_typed in zip(
                    page_rows, raw_page, served_page, typed,
                )
            ]
        else:
            pairs, columns = _operator_projection("nodes", projection)
            _live_reader.check_record_columns(
                rows, _live_reader.projection_names(projection), operator,
            )
            projected = _live_results.project(
                raw_page, served_page, pairs, records=[
                    {**row, "node_id": _live_results.served_node_id(
                        row.get("node_id"), relation, checked=ids_checked)}
                    if _live_reader.is_typed(row) else row
                    for row in page_rows
                ],
            )
        envelope = _operator_envelope(
            rows=projected,
            columns=columns,
            page=page,
            offset=off,
            generation_id=state["generation"],
            scan_truncated=off + page < total,
            count_fields={
                "total_count": total,
                "total_count_basis": "exact",
                "retrieval_exhaustive": True,
                "selection_basis": _live_results.SERVED_FROM,
            },
        )
        from okg.substrate.external_backed.resolver import neutralised_keys

        payload = {
            **envelope,
            **_live_results.page_cursor(off, page, bool(envelope["truncated"])),
            "operator_name": operator,
            **reader_notes,
            **_live_served_fields(retained, ref, state),
        }
        altered = neutralised_keys([
            *raw_page,
            *(((row.get("live") or {}).get("fields") or {}) for row in page_rows
              if _live_reader.is_typed(row)),
        ])
        if altered:
            # As on the expand: the served rows' keys were rewritten.
            payload["neutralised_keys"] = list(altered)
        if operator == "filter":
            payload["predicate"] = predicate
            if projection is not None:
                payload["select"] = projection
        else:
            payload["projection"] = projection
        return _stamp_operator(_live_resolver.with_live_line(payload, state), operator)

    def filter(
        input: Optional[SetInput] = None,
        predicate: Optional[PredicateInput] = None,
        generation_id: Optional[Union[int, str]] = None,
        limit: Limit = 50,
        offset: Offset = 0,
        select: Optional[list[str]] = None,
        order_by: Optional[str] = None,
        authority: Optional[dict[str, Any]] = None,
        max_bytes: MaxBytes = None,
        retain: bool = False,
        next: Optional[str] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """List every record meeting a condition, in pages.

        filter(where={"type":"[[subtype]]","attrs.path":{"op":"suffix",
        "value":"docker/Dockerfile"}}).
        `contains` is JSON containment on attrs (field 'attrs', value
        {key: exact}), or text inside attrs.<key> (ASCII case ignored);
        `like` matches a substring of a field, ignoring accents and case.
        [[if tool:search]]Use `search` for words, [[/if]]`matches` on
        field `text` for words or re:/regex/ (rows add match, match_count).
        `order_by` is
        '<field>' or '<field> desc', including a declared attrs.<key>.
        Past offset 10000, `next` alone continues.
        """
        keyset: Optional[dict[str, Any]] = None
        if next is not None:
            arguments, refusal = _keyset_next_call(
                "filter", next,
                given={"input": input, "predicate": predicate, "select": select,
                       "order_by": order_by, "offset": offset, "retain": retain},
            )
            if refusal is not None:
                return refusal
            input, predicate, select = (
                arguments["input"], arguments["predicate"], arguments["select"],
            )
            if generation_id is None:
                generation_id = arguments["generation_id"]
            keyset = {"token": next}
        elif input is None:
            refusal = _operator_refusal(
                "filter",
                code="invalid_operator_arguments",
                message="filter needs `input`, or `next` to continue a paged read",
            )
            refusal["error"]["fix_hint"] = (
                'pass input={"kind":"nodes",...} and predicate={...}, or '
                "next=<token> from a page that ended bound reached"
            )
            return refusal
        date_started = _date_call_started()
        input = as_mapping(input)
        predicate = as_mapping(predicate)
        digest_refusal = _text_digest_operator_refusal(
            "filter", input=input, predicate=predicate, order_by=order_by,
        )
        if digest_refusal is not None:
            return digest_refusal
        if isinstance(input, dict) and "ref" in input and (
            _holds_text(predicate) or _holds_text(input)
        ):
            raise _text_on_reference("filter")
        if isinstance(input, dict) and _live_results.is_live_id(input.get("ref")):
            _refuse_live_filter_dates(predicate)
            return _live_read(
                "filter", ref_id=input["ref"], authority=authority,
                generation_id=generation_id, limit=limit, offset=offset,
                predicate=predicate, projection=select, projection_argument="select",
                order_by=order_by, retain=retain,
            )
        loose_refusal = _loose_match_refusal("filter", predicate, input)
        if loose_refusal is not None:
            return loose_refusal
        if isinstance(input, dict) and "ref" in input:
            # The explicit call's own predicate check, before the reference
            # is looked up or any SQL runs (#3222, L1).
            refuse_substring_contains("filter", predicate)
            return _reference_page(
                "filter", ref_id=input["ref"], predicate=predicate, projection=select,
                generation_id=generation_id, limit=limit, offset=offset,
                authority=authority, order_by=order_by, retain=retain,
            )
        if retain:
            raise _result_references.unsupported(
                "argument", argument="retain",
                message="filter: retain makes a child of a result reference; "
                "pass input={'ref': id}",
                fix_hint="drop retain, or filter a reference from search(retain=true)",
            )
        refuse_substring_contains("filter", predicate, input, scope=input)
        page = max(1, min(int(limit), 1000))
        off = max(0, int(offset))
        attr_sensitive = (
            _operator_refs_attrs(predicate)
            or _operator_refs_attrs(input)
            or _operator_refs_attrs(select)
            or _operator_refs_attrs(order_by)
            or _digest_projection_needs_served_rows(select)
            # A date condition reads an attribute too (#3666).
            or _holds_date_condition(predicate)
            or (isinstance(input, dict) and _holds_date_condition(input.get("filter")))
            # A text condition (#3666) narrows through the effective view.
            or _holds_text(predicate)
            or _holds_text(input)
            # A reference to an earlier `run` step is read in SQL (#3666),
            # and one that is not open here is refused there.
            or _run_steps.mentions_marker(predicate)
            or _run_steps.mentions_marker(input)
        )
        # Issue #3666 (2c): a comparison the older translator lacks, on a
        # column of a record or a link, is compiled by the read compiler.
        compiled_only = _operator_needs_read_compiler(predicate) or (
            isinstance(input, dict)
            and _operator_needs_read_compiler(input.get("filter"))
        )
        # #3666 step 3b: a page whose next one starts past the offset cap is
        # read where a keyset token can continue it.
        past_cap = off + page > MAX_OFFSET
        # #3201: an ordered filter reads the same effective-attribute SQL as an
        # attrs filter, so one predicate selects one set with or without
        # order_by. It no longer goes through run_sql.
        if (
            attr_sensitive or order_by or compiled_only or keyset is not None or past_cap
            # One order, the id, on every page a token may continue.
            or not _program_orders_by_id(input, select)
        ):
            try:
                kind = _scope_kind(input)
                validate_set_scope(kind, input)
                if not (attr_sensitive or order_by) and select is not None:
                    # Moved here only for the id order: the scan program's
                    # field check still decides what it refused (as map does).
                    _filter_program(
                        kind=kind, predicate=None, projection=select, limit=1, offset=0,
                    )
            except ValueError as exc:
                return _operator_refusal(
                    "filter",
                    code="unsupported_operator_form",
                    message=str(exc),
                    fallback_operator="query",
                )
            with _open(generation_id, authority=authority) as h:
                nomos_runtime = deployment_requires_nomos_runtime(
                    h.session.deployment_name
                )
                # The call as sent, for an offered call: every read argument,
                # the pinned generation and byte budget included (#3666, 2c).
                sent_call = {
                    "input": input, "predicate": predicate, "generation_id": generation_id,
                    "limit": limit, "offset": offset, "select": select,
                    "order_by": order_by, "authority": authority, "max_bytes": max_bytes,
                }
                date_order = _named_date_order_refusal(
                    h.conn, h.session, kind=kind, input=input, order_by=order_by,
                    nomos_runtime=nomos_runtime,
                    call={k: v for k, v in sent_call.items() if k != "order_by"},
                )
                if date_order is not None:
                    return date_order
                scoped_predicate = _compose_scope_predicate(
                    kind=kind,
                    input=input,
                    predicate=predicate,
                )
                sent_predicate = scoped_predicate
                # #3666: a text clause becomes the ids of the records whose
                # text matches; the rest of the condition runs over them.
                scoped_predicate, text_condition, text_ids, text_facts = _text_members(
                    h.conn, h.session, tool="filter", kind=kind,
                    predicate=scoped_predicate, nomos_runtime=nomos_runtime,
                    retry=lambda subtype: {
                        "input": {**input, "subtype": subtype},
                        "predicate": predicate,
                        **({"select": select} if select is not None else {}),
                        **({"order_by": order_by} if order_by else {}),
                        **({"limit": page} if page != 50 else {}),
                    },
                )
                # `run` (#3666): the step's set is the whole condition, its
                # text clause as the ids whose text matches.
                _run_steps.note_set(
                    "records", kind, _run_set_predicate(scoped_predicate, text_ids),
                    text=text_ids is not None,
                )
                # #3666: date conditions compile like any other. The reply
                # echoes the predicate as the caller wrote it.
                date_report = _DateConditionReport()
                # One budget for the call's date work; None without any.
                page_budget = _date_budget(
                    "filter", scoped_predicate, kind=kind, started_at=date_started,
                )
                declared_registry = _declared_registry_for(h.conn)
                bound_refusal = _declared_bound_refusal(
                    predicate, declared_registry, call=sent_call,
                )
                if bound_refusal is not None:
                    return bound_refusal
                keyset_read = {"input": input, "predicate": predicate, "select": select}
                after_id: Optional[str] = None
                if keyset is not None:
                    after_id, off = _keyset_cursor(h, "filter", keyset["token"], keyset_read)

                def read_page(read_with: Any) -> Any:
                    """One page read with ``read_with``, and what it noted;
                    a page the read path cannot serve is returned as its
                    error, for the refusal below."""
                    notes: list[dict[str, Any]] = []
                    facts: dict[str, Any] = {}
                    meta: dict[str, Any] = {}
                    try:
                        found = _effective_view_sql_page(
                            h.conn,
                            h.session,
                            kind=kind,
                            predicate=read_with,
                            fingerprint_predicate=scoped_predicate,
                            date_budget=page_budget,
                            limit=page,
                            offset=0 if after_id is not None else off,
                            after_id=after_id,
                            order_by=order_by,
                            pushdown_notes=notes,
                            nomos_runtime=nomos_runtime,
                            residual=lambda chunk: [
                                row for row in chunk
                                if _operator_read_check(
                                    row, read_with, declared=declared_registry,
                                    nomos_runtime=nomos_runtime,
                                )
                            ],
                            page_facts=facts,
                            page_meta=meta,
                            member_ids=text_ids,
                        )
                    except ValueError as exc:
                        if _is_over_date_budget(exc):
                            raise
                        return exc
                    return found, notes, facts, meta

                # #3666: the page first, then the stored date forms
                # (`_dated_read`). Issue #3476, as on map: the relation
                # stores a link's id as text, so an integer `edge_id`
                # operand is compared as text (#3666, 2c).
                read_predicate, page_read = _dated_read(
                    h.conn, h.session, kind=kind,
                    predicate=_edge_ids_as_text(kind, scoped_predicate),
                    operator="filter", report=date_report,
                    nomos_runtime=nomos_runtime, budget=page_budget, run=read_page,
                )
                if isinstance(page_read, ValueError):
                    return _operator_refusal(
                        "filter",
                        code="unsupported_operator_form",
                        message=str(page_read),
                        fallback_operator="query",
                    )
                (rows, scan_truncated), pushdown_notes, page_facts, page_meta = page_read
                # Bounded post-policy parity check over the SQL page only.
                rows = [
                    row
                    for row in rows
                    if _operator_read_check(
                        row, read_predicate, declared=declared_registry,
                        nomos_runtime=nomos_runtime,
                    )
                ]
                if text_condition is not None:
                    _text_row_fields(
                        h.conn, h.session, text_condition, rows, tool="filter",
                        select=select,
                    )
                page_rows = _with_graph_edge_ids(kind, rows)
                needs_provenance = _operator_projection_needs_provenance(
                    kind=kind,
                    projection=select,
                )
                needs_read_provenance = _operator_projection_needs_read_provenance(
                    kind=kind,
                    projection=select,
                )
                if needs_provenance:
                    page_rows = _operator_fence_node_rows(
                        h.conn,
                        h.session,
                        kind=kind,
                        rows=page_rows,
                    )
                if needs_read_provenance:
                    page_rows = _operator_serialize_edge_rows(
                        h.conn,
                        h.session,
                        kind=kind,
                        rows=page_rows,
                    )
                columns, projected = _operator_project_rows(
                    kind,
                    page_rows,
                    select,
                )
                if text_condition is not None and select is None:
                    columns, projected = _text_default_columns(
                        columns, projected, page_rows,
                    )
                if needs_provenance:
                    columns, projected = _operator_add_provenance_column(
                        columns=columns,
                        projected=projected,
                        source_rows=page_rows,
                    )
                if needs_read_provenance:
                    columns, projected = _operator_add_read_provenance_column(
                        columns=columns,
                        projected=projected,
                        source_rows=page_rows,
                    )
                envelope = _operator_envelope(
                    rows=projected,
                    columns=columns,
                    page=page,
                    offset=off,
                    generation_id=h.session.generation_id,
                    # MUST-FIX #1: if the underlying SQL scan hit its
                    # result-size ceiling, the result set is genuinely
                    # truncated -- surface it as truncated=true (and the count
                    # as a lower bound) rather than a silent partial page.
                    scan_truncated=scan_truncated,
                    page_facts=page_facts,
                    count_fields=_operator_count_fields(
                        h.conn,
                        h.session,
                        offset=off,
                        shown=len(rows),
                        page_meta=page_meta,
                        rest_only=after_id is not None,
                    ),
                )
                _keyset_continue(
                    h, "filter", envelope, read=keyset_read, page_meta=page_meta,
                    keyset=keyset is not None, ordered=bool(order_by),
                )
                payload = {
                    **envelope,
                    "operator_name": "filter",
                    "input": input,
                    "predicate": sent_predicate,
                    "predicate_fingerprint": page_meta["predicate_fingerprint"],
                    **date_report.payload(),
                }
                if text_condition is not None:
                    payload["text_match"] = _text_fact(text_condition, text_facts or {})
                _mark_not_a_total(
                    payload, rows=len(projected), page_facts=page_facts, page_meta=page_meta,
                )
                if order_by:
                    payload["order_by"] = order_by
                if page_meta.get("sorted_as_text"):
                    # #3666 (2c): the order key has no declared type, so the
                    # page is in text order; the reply says so.
                    payload["sorted_as_text"] = page_meta["sorted_as_text"]
                payload.update(_compared_as_stored(
                    scoped_predicate, declared_registry, empty=not projected,
                ))
                if pushdown_notes:
                    # issue #3029: the declared filter index was not complete
                    # for a predicate attribute, so this page came from the
                    # exact scan. Say so rather than hide the slower path.
                    payload["notes"] = pushdown_notes
                if kind == "nodes" and not off and keyset is None:
                    # Lane ri (CompOps q020): files picked by name name the
                    # grep over file texts, with their count.
                    _attach_names_only(
                        h, payload, predicate=sent_predicate, query=None, method="filter",
                    )
                return _stamp_operator(
                    _maybe_stamp_advanced(h, payload),
                    "filter",
                )
        try:
            kind = _scope_kind(input)
            validate_set_scope(kind, input)
            # filter executes by lowering a TransformProgram (akmon.filter.<scope>)
            # over the nodes_live/edges_live registered surfaces -- a +1 LIMIT lets
            # us report truncation precisely.
            scoped_predicate = _compose_scope_predicate(
                kind=kind,
                input=input,
                predicate=predicate,
            )
            _run_steps.note_set("records", kind, scoped_predicate)
            program, columns = _filter_program(
                kind=kind,
                predicate=scoped_predicate,
                projection=select,
                limit=page + 1,
                offset=off,
            )
        except ValueError as exc:
            return _operator_refusal(
                "filter",
                code="unsupported_operator_form",
                message=str(exc),
                fallback_operator="query",
            )
        with _open(generation_id, authority=authority) as h:
            raw_rows = _lower_program_over_chronos(
                program,
                h.conn,
                generation_id=h.session.generation_id,
                branch=h.session.branch_name,
            )
            # Pre-filter truncated (issue #627): a Nomos-hidden row inside
            # the fetch window must not mislabel the page as complete;
            # enforce-mode pages may under-fill. Offset pagination iterates
            # raw positions, so ONLY this page's raw window (not the +1
            # lookahead row) may be served — the lookahead belongs to the
            # next page and serving it here would double-count it.
            truncated = len(raw_rows) > page
            rows = _source_lineage_filter_operator_rows(
                kind=kind,
                conn=h.conn,
                session=h.session,
                rows=[dict(row) for row in raw_rows[:page]],
            )
            if "attrs" in columns:
                rows = apply_effective_attrs_to_rows(
                    h.conn,
                    rows,
                    target_kind="node" if kind == "nodes" else "edge",
                    target_id_field=("node_id" if kind == "nodes" else "edge_id"),
                    generation_id=h.session.generation_id,
                )
            if kind == "nodes" and "attrs" in columns:
                rows = _operator_fence_node_rows(
                    h.conn,
                    h.session,
                    kind=kind,
                    rows=rows,
                )
                if "provenance" not in columns:
                    columns = [*columns, "provenance"]
            if kind == "edges" and "attrs" in columns:
                rows = _operator_serialize_edge_rows(
                    h.conn,
                    h.session,
                    kind=kind,
                    rows=rows,
                )
                if "read_provenance" not in columns:
                    columns = [*columns, "read_provenance"]
            envelope = {
                "items": [
                    {column: row.get(column) for column in columns} for row in rows
                ],
                **_operator_page_count(
                    offset=off, shown=len(rows), truncated=truncated,
                ),
                "truncated": truncated,
                "limit": page,
                "offset": off,
                "next_offset": off + page if truncated else None,
                "pinned_generation_id": h.session.generation_id,
                "query_plan_summary": "Chronos checkout",
                "elapsed_ms": 0,
            }
            payload = {
                **envelope,
                "operator_name": "filter",
                "input": input,
                "predicate": scoped_predicate,
            }
            return _stamp_operator(
                _maybe_stamp_advanced(h, payload),
                "filter",
            )

    def map(
        input: Optional[SetInput] = None,
        projection: Projection = None,
        generation_id: Optional[Union[int, str]] = None,
        limit: Limit = 50,
        offset: Offset = 0,
        authority: Optional[dict[str, Any]] = None,
        max_bytes: MaxBytes = None,
        next: Optional[str] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Chosen fields of every record in a set.

        map(where={"type":"[[subtype]]"}, select=["node_id","attrs.<key>"]).
        Past offset 10000, `next` alone continues.
        """
        keyset: Optional[dict[str, Any]] = None
        if next is not None:
            arguments, refusal = _keyset_next_call(
                "map", next,
                given={"input": input, "projection": projection, "offset": offset},
            )
            if refusal is not None:
                return refusal
            input, projection = arguments["input"], arguments["projection"]
            if generation_id is None:
                generation_id = arguments["generation_id"]
            keyset = {"token": next}
        elif input is None:
            refusal = _operator_refusal(
                "map",
                code="invalid_operator_arguments",
                message="map needs `input`, or `next` to continue a paged read",
            )
            refusal["error"]["fix_hint"] = (
                'pass input={"kind":"nodes",...}, or next=<token> from a page '
                "that ended bound reached"
            )
            return refusal
        date_started = _date_call_started()
        input = as_mapping(input)
        projection = as_mapping(projection)
        digest_refusal = _text_digest_operator_refusal("map", input=input)
        if digest_refusal is not None:
            return digest_refusal
        if isinstance(input, dict) and "ref" in input and _holds_text(input):
            raise _text_on_reference("map")
        if isinstance(input, dict) and _live_results.is_live_id(input.get("ref")):
            return _live_read(
                "map", ref_id=input["ref"], authority=authority,
                generation_id=generation_id, limit=limit, offset=offset,
                projection=projection,
            )
        loose_refusal = _loose_match_refusal("map", input)
        if loose_refusal is not None:
            return loose_refusal
        if isinstance(input, dict) and "ref" in input:
            return _reference_page(
                "map", ref_id=input["ref"], predicate=None, projection=projection,
                generation_id=generation_id, limit=limit, offset=offset,
                authority=authority,
            )
        refuse_substring_contains("map", input, scope=input)
        page = max(1, min(int(limit), 1000))
        off = max(0, int(offset))
        # A refused date condition (#3666) is raised as it is, with its fix,
        # not folded into the generic refusal below.
        date_refusal: InvalidOperatorArguments | None = None
        try:
            kind = _scope_kind(input)
            validate_set_scope(kind, input)
            # Issue #3476: an input that narrows the set (subtype, edge_type
            # or filter) takes the effective-view path whatever the
            # projection reads. The projection-only program below lowers no
            # WHERE, so it would answer over the whole kind as if complete.
            narrowed = _compose_scope_predicate(
                kind=kind, input=input, predicate=None,
            ) is not None
            if (
                (narrowed or not _program_orders_by_id(input, projection))
                and projection is not None
                and not _operator_refs_attrs(input)
                and not _operator_refs_attrs(projection)
            ):
                # The projection-only program checked every field name; the
                # effective-view path answers an unknown one with nulls. Keep
                # the program's check (a ValueError is refused below as
                # `unsupported_operator_form`), so a scoped call refuses
                # exactly what an unscoped one does. The match fields of a
                # text condition (#3666) are the server's, not a column.
                from okg.substrate.mcp.text_match import MATCH_FIELDS

                checked = projection
                if _holds_text(input) and isinstance(projection, list):
                    checked = [f for f in projection if f not in MATCH_FIELDS] or None
                elif _holds_text(input) and isinstance(projection, dict):
                    checked = {
                        a: f for a, f in projection.items() if f not in MATCH_FIELDS
                    } or None
                if checked is not None:
                    _map_program(kind=kind, projection=checked, limit=1, offset=0)
            if (
                projection is None
                or narrowed
                or _operator_refs_attrs(input)
                or _operator_refs_attrs(projection)
                or _digest_projection_needs_served_rows(projection)
                # #3666 step 3b: a page whose next one starts past the
                # offset cap is read where a keyset token continues it.
                or keyset is not None
                or off + page > MAX_OFFSET
                or not _program_orders_by_id(input, projection)
            ):
                with _open(generation_id, authority=authority) as h:
                    nomos_runtime = deployment_requires_nomos_runtime(
                        h.session.deployment_name
                    )
                    scoped_predicate = _edge_ids_as_text(kind, _compose_scope_predicate(
                        kind=kind,
                        input=input,
                        predicate=None,
                    ))
                    # #3666: a text clause becomes the ids of the records
                    # whose text matches; the rest runs over them.
                    scoped_predicate, text_condition, text_ids, text_facts = _text_members(
                        h.conn, h.session, tool="map", kind=kind,
                        predicate=scoped_predicate, nomos_runtime=nomos_runtime,
                        retry=lambda subtype: {
                            "input": {**input, "subtype": subtype},
                            **({"projection": projection} if projection is not None else {}),
                            **({"limit": page} if page != 50 else {}),
                        },
                    )
                    # #3666: date conditions in input.filter compile like
                    # any other.
                    date_report = _DateConditionReport()
                    caller_predicate = scoped_predicate
                    _run_steps.note_set(
                        "records", kind, _run_set_predicate(caller_predicate, text_ids),
                        text=text_ids is not None,
                    )
                    # One budget for the call's date work; None without any.
                    page_budget = _date_budget(
                        "map", caller_predicate, kind=kind, started_at=date_started,
                    )
                    declared_registry = _declared_registry_for(h.conn)
                    keyset_read = {"input": input, "projection": projection}
                    after_id: Optional[str] = None
                    if keyset is not None:
                        after_id, off = _keyset_cursor(h, "map", keyset["token"], keyset_read)

                    def read_page(read_with: Any) -> Any:
                        """One page read with ``read_with``, and what it
                        noted."""
                        notes: list[dict[str, Any]] = []
                        facts: dict[str, Any] = {}
                        meta: dict[str, Any] = {}
                        found = _effective_view_sql_page(
                            h.conn,
                            h.session,
                            kind=kind,
                            predicate=read_with,
                            fingerprint_predicate=caller_predicate,
                            date_budget=page_budget,
                            limit=page,
                            offset=0 if after_id is not None else off,
                            after_id=after_id,
                            pushdown_notes=notes,
                            nomos_runtime=nomos_runtime,
                            residual=lambda chunk: [
                                row for row in chunk
                                if _operator_read_check(
                                    row, read_with, declared=declared_registry,
                                    nomos_runtime=nomos_runtime,
                                )
                            ],
                            page_facts=facts,
                            page_meta=meta,
                            member_ids=text_ids,
                        )
                        return found, notes, facts, meta

                    # #3666: the page first, then the stored date forms
                    # (`_dated_read`).
                    try:
                        scoped_predicate, page_read = _dated_read(
                            h.conn, h.session, kind=kind, predicate=scoped_predicate,
                            operator="map", report=date_report,
                            nomos_runtime=nomos_runtime, budget=page_budget, run=read_page,
                        )
                    except InvalidOperatorArguments as exc:
                        date_refusal = exc
                        raise
                    (rows, scan_truncated), pushdown_notes, page_facts, page_meta = page_read
                    rows = [
                        row
                        for row in rows
                        if _operator_read_check(
                            row, scoped_predicate, declared=declared_registry,
                            nomos_runtime=nomos_runtime,
                        )
                    ]
                    if text_condition is not None:
                        _text_row_fields(
                            h.conn, h.session, text_condition, rows, tool="map",
                            select=projection,
                        )
                    page_rows = _with_graph_edge_ids(kind, rows)
                    needs_provenance = _operator_projection_needs_provenance(
                        kind=kind,
                        projection=projection,
                    )
                    needs_read_provenance = _operator_projection_needs_read_provenance(
                        kind=kind,
                        projection=projection,
                    )
                    if needs_provenance:
                        page_rows = _operator_fence_node_rows(
                            h.conn,
                            h.session,
                            kind=kind,
                            rows=page_rows,
                        )
                    if needs_read_provenance:
                        page_rows = _operator_serialize_edge_rows(
                            h.conn,
                            h.session,
                            kind=kind,
                            rows=page_rows,
                        )
                    columns, projected = _operator_project_rows(
                        kind,
                        page_rows,
                        projection,
                    )
                    if text_condition is not None and projection is None:
                        columns, projected = _text_default_columns(
                            columns, projected, page_rows,
                        )
                    if needs_provenance:
                        columns, projected = _operator_add_provenance_column(
                            columns=columns,
                            projected=projected,
                            source_rows=page_rows,
                        )
                    if needs_read_provenance:
                        columns, projected = _operator_add_read_provenance_column(
                            columns=columns,
                            projected=projected,
                            source_rows=page_rows,
                        )
                    envelope = _operator_envelope(
                        rows=projected,
                        columns=columns,
                        page=page,
                        offset=off,
                        generation_id=h.session.generation_id,
                        # MUST-FIX #1: a scan that hit its result-size ceiling
                        # is genuinely truncated -- never a silent partial page.
                        scan_truncated=scan_truncated,
                        page_facts=page_facts,
                        count_fields=_operator_count_fields(
                            h.conn,
                            h.session,
                            offset=off,
                            shown=len(rows),
                            page_meta=page_meta,
                            rest_only=after_id is not None,
                        ),
                    )
                    _keyset_continue(
                        h, "map", envelope, read=keyset_read, page_meta=page_meta,
                        keyset=keyset is not None, ordered=False,
                    )
                    payload = {
                        **envelope,
                        "operator_name": "map",
                        "input": input,
                        "projection": projection,
                        "predicate_fingerprint": page_meta["predicate_fingerprint"],
                        **date_report.payload(),
                        **_compared_as_stored(
                            caller_predicate, declared_registry, empty=not projected,
                        ),
                    }
                    if text_condition is not None:
                        payload["text_match"] = _text_fact(text_condition, text_facts or {})
                    _mark_not_a_total(
                        payload, rows=len(projected), page_facts=page_facts,
                        page_meta=page_meta,
                    )
                    if pushdown_notes:
                        payload["notes"] = pushdown_notes
                    return _stamp_operator(
                        _maybe_stamp_advanced(h, payload),
                        "map",
                    )
            # map executes by lowering a TransformProgram (akmon.map.<scope>) over the
            # nodes_live/edges_live registered surfaces -- projection only, no WHERE,
            # so only an input that narrows nothing reaches it (issue #3476).
            program, columns = _map_program(
                kind=kind,
                projection=projection,
                limit=page + 1,
                offset=off,
            )
        except ValueError as exc:
            if exc is date_refusal or _is_over_date_budget(exc):
                raise
            return _operator_refusal(
                "map",
                code="unsupported_operator_form",
                message=str(exc),
                fallback_operator="query",
            )
        with _open(generation_id, authority=authority) as h:
            raw_rows = _lower_program_over_chronos(
                program,
                h.conn,
                generation_id=h.session.generation_id,
                branch=h.session.branch_name,
            )
            # Pre-filter truncated (issue #627): see the filter operator —
            # only this page's raw window is served, never the lookahead row.
            truncated = len(raw_rows) > page
            rows = _source_lineage_filter_operator_rows(
                kind=kind,
                conn=h.conn,
                session=h.session,
                rows=[dict(row) for row in raw_rows[:page]],
            )
            if kind == "nodes" and "attrs" in columns:
                rows = _operator_fence_node_rows(
                    h.conn,
                    h.session,
                    kind=kind,
                    rows=rows,
                )
                if "provenance" not in columns:
                    columns = [*columns, "provenance"]
            if kind == "edges" and "attrs" in columns:
                rows = _operator_serialize_edge_rows(
                    h.conn,
                    h.session,
                    kind=kind,
                    rows=rows,
                )
                if "read_provenance" not in columns:
                    columns = [*columns, "read_provenance"]
            envelope = {
                "items": [
                    {column: row.get(column) for column in columns} for row in rows
                ],
                **_operator_page_count(
                    offset=off, shown=len(rows), truncated=truncated,
                ),
                "truncated": truncated,
                "limit": page,
                "offset": off,
                "next_offset": off + page if truncated else None,
                "pinned_generation_id": h.session.generation_id,
                "query_plan_summary": "Chronos checkout",
                "elapsed_ms": 0,
            }
            payload = {
                **envelope,
                "operator_name": "map",
                "input": input,
                "projection": projection,
            }
            return _stamp_operator(
                _maybe_stamp_advanced(h, payload),
                "map",
            )

    def _write_answer(result: dict[str, Any]) -> dict[str, Any]:
        """Attach the answer a chat user should read to a held-write result.

        PACT ``chat-record-affordance``. The text is composed from the RESULT
        -- the envelope identifier is read out of the row the submit produced,
        never out of the request -- so an answer naming an envelope is an
        answer about an envelope that exists.
        """
        from okg.substrate.mcp.operators import (
            render_write_answer,
            review_location_for,
        )

        result["answer"] = render_write_answer(
            result,
            deployment=deployment_name,
            review_location=review_location_for(deployment_name),
        )
        return result

    def propose(
        action: str,
        payload: Optional[dict[str, Any]] = None,
        authority: Optional[dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
        generation_id: Optional[Union[int, str]] = None,
    ) -> dict[str, Any]:
        """Submit, decide, or inspect admission proposals."""
        normalized = str(action).strip().lower()
        payload = payload or {}
        # Strip caller-asserted auto-accept escalation before the authority is
        # used to build the admission/producer policy, and any `_nomos*` key:
        # only the server writes those.
        authority = _strip_server_owned_authority(
            _sanitize_caller_admission_mode(authority or {})
        )
        if normalized in {"inspect", "show", "list"}:
            with _open(generation_id) as h:
                result = _read_admission_state(
                    h.conn,
                    admission_id=payload.get("admission_id"),
                    limit=int(payload.get("limit") or 20),
                )
                result["generation_id"] = h.session.generation_id
                result["pinned_generation_id"] = h.session.generation_id
                return _stamp_operator(
                    _maybe_stamp_advanced(h, result),
                    "propose",
                )
        # submit/decide mutate admission state — operator-gated.
        refusal = _write_refusal("propose")
        if refusal is not None:
            refused = {**refusal, "operator_name": "propose", "action": normalized}
            return _write_answer(refused) if normalized == "submit" else refused
        authority_evidence = _authorize_propose_write(
            action=normalized,
            authority=authority,
        )
        if authority_evidence is not None:
            refusal_code, refusal_message = _authority_refusal_code(
                authority_evidence,
                verdict_message=(
                    "propose write actions require explicit authority "
                    "independent of MCP tool visibility"
                ),
            )
            refused = _operator_refusal(
                "propose",
                code=refusal_code,
                message=refusal_message,
                authority_evidence=authority_evidence,
            )
            refused["action"] = normalized
            return _write_answer(refused) if normalized == "submit" else refused
        try:
            if normalized == "submit":
                result = _submit_admission_payload(
                    payload=payload,
                    producer_id=str(authority.get("producer_id") or "agent:mcp"),
                    producer_kind=str(authority.get("producer_kind") or "agent"),
                    intent=str(payload.get("intent") or "review_proposal"),
                    idempotency_key=idempotency_key,
                    authority=authority,
                    check_summary=payload.get("check_summary"),
                    branch_name=branch_name,
                )
            elif normalized == "decide":
                result = _decide_admission_payload(
                    admission_id=str(payload.get("admission_id") or ""),
                    decision=str(
                        payload.get("decision") or payload.get("status") or "",
                    ),
                    # Server-derived deciding actor: a caller cannot forge the
                    # deciding authority (e.g. actor='operator'). The launch-time
                    # write-mode consent is the authority; its principal is
                    # resolved server-side, ignoring authority/payload 'actor'.
                    actor=_launch_consent_actor(deployment_name),
                    reason=payload.get("reason"),
                    verdict=payload.get("verdict"),
                )
            else:
                raise ValueError("propose action must be submit, decide, or inspect")
        except RuntimeError as exc:
            refused = {
                "operator_name": "propose",
                "action": normalized,
                "status": "refused",
                "error": {
                    "code": "admission_runtime_unavailable",
                    "message": str(exc),
                    "fix_hint": (
                        "run a DBOS admission worker for this deployment "
                        "or register admission workflows in this process"
                    ),
                },
            }
            return _write_answer(refused) if normalized == "submit" else refused
        result["operator_name"] = "propose"
        result["action"] = normalized
        # Only `submit` produces a HELD envelope. `decide` is an operator
        # acting ON one, and an answer telling them their acceptance is
        # "pending review" would be false.
        return _write_answer(result) if normalized == "submit" else result

    def record(
        kind: str,
        payload: Optional[dict[str, Any]] = None,
        authority: Optional[dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
        generation_id: Optional[Union[int, str]] = None,
        mode: Optional[str] = None,
    ) -> dict[str, Any]:
        """Record typed agent knowledge through admission templates."""
        del generation_id, mode
        payload = payload or {}
        # Strip any caller-asserted fast_track BEFORE it reaches payload lowering
        # or admission submit (mirrors propose; also enforced centrally in
        # _submit_admission_payload). harden-mcp-write-boundaries. Any
        # `_nomos*` key is dropped too: only the server writes those.
        authority = _strip_server_owned_authority(
            _sanitize_caller_admission_mode(authority or {})
        )
        refusal = _write_refusal("record")
        if refusal is not None:
            return {**refusal, "operator_name": "record", "kind": kind}
        authority_evidence = _authorize_propose_write(
            action=f"record:{kind}",
            authority=authority,
            scope_alternative="okg.record.write",
        )
        if authority_evidence is not None:
            refusal_code, refusal_message = _authority_refusal_code(
                authority_evidence,
                verdict_message=(
                    "record writes require explicit authority "
                    "independent of MCP tool visibility"
                ),
            )
            template_payload: dict[str, Any] = {}
            if refusal_code == "unauthorized_write":
                # The template example only makes sense for a rendered
                # policy denial; when authority could not be evaluated
                # the failure is not about the payload (issue #950).
                try:
                    constraints = _record_catalog_constraints()
                    template_payload = record_templates_payload(
                        kind,
                        prefixes=active_record_prefixes,
                        active_subtypes=constraints.get("active_subtypes"),
                        narrowing_triples=constraints.get("narrowing_triples"),
                    )
                except RecordPayloadError:
                    # Fall back to an empty template payload for
                    # unauthorized writes.
                    pass
            return {
                "operator_name": "record",
                "kind": kind,
                "status": "refused",
                "error": {
                    "code": refusal_code,
                    "message": refusal_message,
                    "authority": authority_evidence,
                    **(
                        {"example": template_payload["templates"][0]}
                        if template_payload.get("templates")
                        else {}
                    ),
                },
            }
        try:
            constraints = _record_catalog_constraints()
            lowered = lower_record_payload(
                kind=kind,
                payload=payload,
                authority=authority,
                idempotency_key=idempotency_key,
                prefixes=active_record_prefixes,
                active_subtypes=constraints.get("active_subtypes"),
                narrowing_triples=constraints.get("narrowing_triples"),
            )
        except RecordPayloadError as exc:
            return {
                "operator_name": "record",
                "kind": kind,
                "status": "refused",
                "error": exc.to_error(),
                **(
                    {"nomos_decision": authority["_nomos_decision"]}
                    if "_nomos_decision" in authority
                    else {}
                ),
            }
        _resolve_record_reference_edges(lowered)
        try:
            result = _submit_admission_payload(
                payload=lowered["admission_payload"],
                producer_id=str(authority.get("producer_id") or "agent:mcp"),
                producer_kind=str(authority.get("producer_kind") or "agent"),
                intent=str(lowered["admission_payload"]["intent"]),
                idempotency_key=str(lowered["idempotency_key"]),
                authority=authority,
                check_summary=lowered["check_summary"],
                branch_name=branch_name,
            )
        except RuntimeError as exc:
            return {
                "operator_name": "record",
                "kind": lowered["record_kind"],
                "record_id": lowered["record_id"],
                "status": "refused",
                "error": {
                    "code": "admission_runtime_unavailable",
                    "message": str(exc),
                    "fix_hint": (
                        "run a DBOS admission worker for this deployment "
                        "or register admission workflows in this process"
                    ),
                },
                **(
                    {"nomos_decision": authority["_nomos_decision"]}
                    if "_nomos_decision" in authority
                    else {}
                ),
            }
        return {
            **result,
            "operator_name": "record",
            "kind": lowered["record_kind"],
            "record_id": lowered["record_id"],
            "template_id": lowered["template"]["template_id"],
            "payload_sha256": lowered["payload_sha256"],
            "node_ids": lowered["node_ids"],
            "edge_count": lowered["edge_count"],
            "emitted_edge_count": lowered.get(
                "emitted_edge_count",
                lowered["edge_count"],
            ),
            "suppressed_edge_count": lowered.get("suppressed_edge_count", 0),
            "reference_diagnostics": lowered.get("reference_diagnostics", []),
            "warnings": lowered["warnings"],
            **(
                {"nomos_decision": authority["_nomos_decision"]}
                if "_nomos_decision" in authority
                else {}
            ),
        }

    def _answered(fn: Any) -> Any:
        """Stamp the honest ANSWER onto every result ``record`` returns.

        PACT ``chat-record-affordance``, req.honest-affordance. A chat user
        never sees this dict -- they see whatever the model says about it -- so
        the model is given one true sentence to relay, composed from the result
        the write actually produced. Applied at the ONE choke point every
        return path of ``record`` passes through, because a per-return stamp is
        the stamp somebody forgets on the refusal path, which is exactly the
        path where a wrong answer does damage.

        ``propose`` is NOT wrapped here: two of its three actions are not held
        writes (``inspect`` is a read, ``decide`` is an operator's acceptance),
        and rendering a decide result would tell an operator that their own
        acceptance is "pending review". It stamps its submit path in-line
        instead.
        """

        @wraps(fn)
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            result = fn(*args, **kwargs)
            if not isinstance(result, dict):
                return result
            return _write_answer(result)

        return _wrapped

    if allow_writes:
        register_write_effect_tools(
            mcp,
            traced=traced,
            record_tool=_answered(record),
            propose_tool=propose,
        )

    # Legacy compat aliases start_trace and end_trace were deleted by
    # ``shrink-mcp-to-operator-surface``. The labeled-trace-bracket
    # feature is no longer an MCP tool surface; the underlying
    # set_current_label / clear_current_label / get_current_label
    # helpers remain available for any internal substrate code that
    # wants to inject trace labels directly.

    # Legacy compat aliases describe_graph, graph_stats, agent_context,
    # and get_ontology were deleted by
    # ``shrink-mcp-to-operator-surface``. Their behaviors are folded
    # into ``inspect(target={kind:'graph'})`` (describe_graph and
    # graph_stats) and ``inspect(target={kind:'ontology'})``; the
    # agent-context surface
    # lives in the ``okg.substrate.agent_context`` Python API.

    def _require_resource_principal(resource_name: str) -> None:
        """Refuse an MCP *resource* read by an unidentified caller.

        The tool gate is composed into `traced`, which covers tools only.
        Resources are registered directly with @mcp.resource, so without this
        the schema digest (subtypes, attr keys, edge types, node counts) and
        the read-side metadata would answer a caller with no principal header
        -- and the digest drops its row-derived sections only under `enforce`,
        so an `audit`-mode multi-user deployment would leak per-subtype counts.

        Binds all the way to a SUBJECT, through the same
        ``_resolve_principal_to_subject`` the tool gate uses. It was a shape
        check -- ``header_claim_for_request`` and stop -- until an adversarial
        review found what that admits: a WELL-FORMED header naming a principal
        bound to nothing (``X-OKG-Principal: nobody@example.edu``) parses
        cleanly, so the digest's subtypes, attr keys, edge types and per-subtype
        node counts were served to a caller the deployment never identified.
        ``req.mcp-principal-mapping.fail-closed-principal`` states the rule for
        the header that "maps to no Nomos subject" with no resource carve-out
        and ``waivers: []``; the round trip the old comment was avoiding is the
        price of that rule, and it is paid on resource reads only.
        """
        if not multi_user_principals:
            return

        resolve_conn = None
        try:
            with read_pass_scope(dedup_identity_claims=True):
                resolve_conn = _pool_getconn(pool)
                _resolve_principal_to_subject(
                    resolve_conn,
                    deployment_name=deployment_name,
                    principal_mapping=principal_mapping,
                    operator_name=resource_name,
                    surface="mcp.resource.read",
                    resource_kind="mcp_resource",
                    target_kind="mcp_resource_read",
                )
        except SubstrateError as exc:
            conn = None
            try:
                conn = _pool_getconn(pool)
                _persist_principal_refusal(
                    conn,
                    error=exc,
                    deployment_name=deployment_name,
                    surface="mcp.resource.read",
                    operator_name=resource_name,
                )
            except Exception:  # noqa: BLE001 — audit write must not mask the refusal it records.
                log.warning(
                    "failed to persist MCP principal refusal for resource "
                    "%r on deployment %r",
                    resource_name,
                    deployment_name,
                    exc_info=True,
                )
            finally:
                if conn is not None:
                    pool.putconn(conn)
            raise
        finally:
            # Returned on BOTH paths. The refusal above checks out a second
            # connection for the audit row rather than reusing this one, whose
            # transaction may be aborted; this one still has to go back.
            if resolve_conn is not None:
                pool.putconn(resolve_conn)

    # ---------- Schema digest MCP resource ----------
    # Per `improve-mcp-tool-surface` (2026-04-27, tasks 2.1-2.3).
    # The resource URL is per-deployment so an MCP client connected
    # to multiple OKG servers can disambiguate. Cache: per-server,
    # 1-hour TTL, plus invalidation on catalog reload (a new
    # catalog_version_id at the resolved pin).
    _digest_cache: dict[str, tuple[float, int, str]] = {}
    # value: (expires_at, catalog_version_id_at_build, json_payload)
    _DIGEST_TTL_S: float = 3600.0  # 1 hour

    # Body extracted to mcp/schema_digest_resource.py by Wave D-32
    # (`extract-mcp-schema-digest-resource`). The closure-local
    # wrapper here binds `pool` / `deployment_name` / the cache /
    # `_resolve_latest` / TTL once, so the @mcp.resource registration
    # below stays a thin call into the extracted builder.
    from okg.substrate.mcp.schema_digest_resource import (
        build_digest_payload as _build_digest_payload_impl,
    )

    def _build_digest_payload() -> str:
        return _build_digest_payload_impl(
            pool=pool,
            deployment_name=deployment_name,
            digest_cache=_digest_cache,
            resolve_latest_fn=_resolve_latest,
            digest_ttl_s=_DIGEST_TTL_S,
        )

    @mcp.resource(
        f"okg://schema-digest/{deployment_name}",
        name=f"schema-digest-{deployment_name}",
        description=(
            "Compact ontology summary for this deployment: "
            "subtypes (with attr keys, edge types, node counts), "
            "edge types, and built-in score functions. Fetch at "
            "session open so the agent can write filters and "
            "traversals against the deployment's actual schema "
            "without LinkML grepping. < 5KB per deployment."
        ),
        mime_type="application/json",
    )
    def schema_digest_resource() -> str:
        """Return the cached digest for this deployment as a
        UTF-8 JSON string."""
        _require_resource_principal("schema-digest")
        return _build_digest_payload()

    @mcp.resource(
        f"okg://readside-metadata/{deployment_name}",
        name=f"readside-metadata-{deployment_name}",
        description=(
            "Deployment-owned MCP instructions, skill triggers, "
            "skill templates, and audit questions for read-side "
            "agent behavior."
        ),
        mime_type="application/json",
    )
    def readside_metadata_resource() -> str:
        _require_resource_principal("readside-metadata")
        return readside_metadata_json(readside_metadata)

    from okg.substrate.mcp.deployment_read_surfaces import (
        deployment_state_root,
        register_deployment_read_resources,
    )

    # Anchored at the deployment's repository, not this process's working
    # directory: the agent launcher starts the server from `/`. Only a
    # profile whose record exists now is listed.
    deployment_read_status_resources = register_deployment_read_resources(
        mcp,
        deployment_name=deployment_name,
        state_root=deployment_state_root(readside_metadata.get("deployment_dir")),
        require_principal=_require_resource_principal,
    )

    # Expose for tests.
    mcp._okg_digest_cache = _digest_cache  # type: ignore[attr-defined]
    mcp._okg_build_digest = _build_digest_payload  # type: ignore[attr-defined]
    # The REGISTERED resource callables, principal gate included. The
    # ungated `_okg_build_digest` above is the cache/TTL seam; these are what
    # a client actually reaches, so they are what a gating test must call.
    mcp._okg_build_digest_gated = schema_digest_resource  # type: ignore[attr-defined]
    mcp._okg_readside_metadata_gated = (  # type: ignore[attr-defined]
        readside_metadata_resource
    )
    mcp._okg_deployment_read_status_gated = (  # type: ignore[attr-defined]
        deployment_read_status_resources
    )
    mcp._okg_readside_metadata = readside_metadata  # type: ignore[attr-defined]
    # What `inspect(session_start)` serves as `dataset_card` (filled at the
    # end of the build; PACT dataset-card-switch reads it back without a
    # database).
    mcp._okg_dataset_card_state = dataset_card_state  # type: ignore[attr-defined]
    # Test seam for scale-operator-attr-reads (task.pushdown): lets the parity
    # suite drive the attr-operator scan with an explicit pushdown predicate
    # (subtype/edge_type/stable clauses) and compare it to the unfiltered scan.
    mcp._okg_operator_attr_rows = _operator_attr_rows  # type: ignore[attr-defined]
    mcp._okg_attr_scan_pushdown_predicate = (  # type: ignore[attr-defined]
        _attr_scan_pushdown_predicate
    )
    # Test seam for expose-mcp-node-lookup: lets the lookup suite assert the
    # generated SQL shape (the GIN-indexed `attrs @> ...` containment) that
    # the typed attrs lookup pushes into the effective-view scan WHERE.
    mcp._okg_effective_view_where = _effective_view_where  # type: ignore[attr-defined]
    # Test seam for issue #1186: the alternate snapshot mode must refuse when
    # its requested relation is absent instead of dropping to a Chronos scan.
    mcp._okg_effective_view_source = _effective_view_source  # type: ignore[attr-defined]
    # Test seams for #3201: the operator compiler and the effective relation
    # it compiles over, so the parity suite can evaluate the compiled SQL and
    # the frozen pre-change renderer against the same published rows.
    mcp._okg_compile_operator_predicate = (  # type: ignore[attr-defined]
        _compile_operator_predicate
    )
    mcp._okg_effective_view_runtime_source = (  # type: ignore[attr-defined]
        _effective_view_runtime_source
    )
    mcp._okg_bounded_operator_count = (  # type: ignore[attr-defined]
        _bounded_operator_count
    )
    mcp._okg_effective_view_order_sql = (  # type: ignore[attr-defined]
        _effective_view_order_sql
    )
    mcp._okg_operator_predicate_matches = (  # type: ignore[attr-defined]
        _operator_predicate_matches
    )
    # Test seam for lane-h-mcp-filter-index-driven: the completeness-cache key.
    mcp._okg_interval_complete_cache_key = (  # type: ignore[attr-defined]
        _interval_complete_cache_key
    )

    # Legacy graph-traversal compat aliases describe_subtype, get_node,
    # list_neighbors, aggregate_edge_routes, aggregate_neighbors, and
    # traverse_path were deleted by ``shrink-mcp-to-operator-surface``.
    # Their behaviors live behind the canonical operators:
    #   describe_subtype     → inspect(target={kind:'subtype', subtype:...})
    #   get_node             → inspect(target={kind:'node', node_id:...})
    #   list_neighbors       → expand(input={node_id:...})
    #   aggregate_neighbors  → aggregate(input={node_id:...}, by=[...], metrics=['count'])
    #   aggregate_edge_routes→ aggregate(input={kind:'edges'}, by=[...], metrics=['count'])
    #   traverse_path        → expand(input={node_id:...}, path=[...])
    # The underlying Python helpers (_get_node, _list_neighbors, etc.)
    # are retained because the inspect/expand/aggregate dispatch above
    # delegates to them.

    # expose-mcp-node-lookup (GitHub #641): the query-less typed attrs/path
    # lookup. Its selectable fields live beside the other methods' so the
    # one `select` vocabulary reads them (`select_vocabulary`).
    _ATTRS_LOOKUP_FIELDS: tuple[str, ...] = _ATTRS_LOOKUP_FIELDS_SHARED
    _ATTRS_LOOKUP_DEFAULT: tuple[str, ...] = (
        "node_id",
        "subtype",
        "attrs",
        "provenance",
        "evidence_kind",
        "evidence_kind_provenance",
        "chunk_kind",
        "source_record_id",
        "source_revision",
        HANDLE_KEY,
    )
    _ATTRS_LOOKUP_ALWAYS: tuple[str, ...] = (
        "node_id",
        "subtype",
        "evidence_kind",
        "evidence_kind_provenance",
    )
    # Same pagination bounds as the text search methods (attacker-chosen
    # offsets must not drive unbounded backend work).
    _ATTRS_LOOKUP_MAX_LIMIT = 1000

    def _lookup_as_filter_call(
        *,
        subtype: Optional[str],
        scoped_subtypes: Any,
        attr_filters: Mapping[str, Any],
        where: Optional[dict[str, Any]],
    ) -> Optional[dict[str, Any]]:
        """The `filter` call that selects the lookup's records, or None when a
        condition has no `filter` field (source, title, path, a date).
        `filter` counts every match when nothing can be masked (issue #3666);
        whether the server takes the call is proven by the caller."""
        clauses: list[dict[str, Any]] = []
        if scoped_subtypes:
            clauses.append({"op": "in", "field": "subtype", "values": list(scoped_subtypes)})
        clauses.extend(
            {"op": "eq", "field": f"attrs.{key}", "value": value}
            for key, value in attr_filters.items()
        )
        if where is not None:
            view = _search_where.to_view_predicate(where)
            if view is None:
                return None
            clauses.append(view)
        arguments: dict[str, Any] = {
            "input": {"kind": "nodes", **({"subtype": subtype} if subtype else {})},
        }
        if not clauses:
            return None
        arguments["predicate"] = (
            clauses[0] if len(clauses) == 1 else {"op": "and", "clauses": clauses}
        )
        if not _operator_refs_attrs(arguments["predicate"]):
            # Only a `filter` that reads an attribute pages the effective
            # view, where it counts every match; any other one is a bounded
            # page and would not give a total.
            return None
        if _names_a_date(arguments["predicate"]):
            # `filter` compares a date-typed attribute as an instant, the
            # lookup's read check as text: it could count another set.
            return None
        return {"tool": "filter", "arguments": arguments}

    def _names_a_date(condition: Any) -> bool:
        """Whether a condition reads a date field or compares a value that
        reads as a timestamp, which `filter` compares as an instant."""
        from okg.substrate.mcp.operator_sql import as_operator_timestamp

        if isinstance(condition, dict):
            field = condition.get("field")
            if isinstance(field, str) and field.startswith("date."):
                return True
            operands = [condition.get("value"), *(condition.get("values") or [])]
            if any(
                isinstance(v, str) and as_operator_timestamp(v) is not None for v in operands
            ):
                return True
            return any(
                _names_a_date(child)
                for child in [*(condition.get("clauses") or []), condition.get("clause")]
            )
        return False

    def _case_folded_hint(
        conn: psycopg.Connection,
        session: Session,
        *,
        subtype: Optional[str],
        attr_filters: Mapping[str, Any],
        where: Optional[dict[str, Any]],
        scoped_subtypes: Any = None,
    ) -> Optional[dict[str, Any]]:
        """`did you mean <value>` for a lookup whose one string equality on
        an attribute matched nothing (issue #3666).

        Only with nothing maskable and on a first page (the caller checks),
        and only when the call's one attribute condition is that equality
        beside at most a type and a profile's types: one effective-view read
        of one row whose stored string equals the sent value ignoring case.
        The read runs in its own transaction block under a
        `_CASE_HINT_TIMEOUT_MS` statement timeout, restored after it. A read
        that hits the timeout gives no hint; any other failure is logged and
        gives no hint. Neither refuses the call.
        """
        if where is not None or len(attr_filters) != 1:
            return None
        ((key, sent),) = attr_filters.items()
        if not isinstance(sent, str) or not sent or "\x00" in sent or "\x00" in key:
            return None
        from okg.substrate.mcp.operator_sql import TEXT_CASEFOLD_EQ

        clauses: list[dict[str, Any]] = []
        if subtype:
            clauses.append({"op": "eq", "field": "subtype", "value": subtype})
        if scoped_subtypes:
            clauses.append(_profile_scope_clause(scoped_subtypes))
        clauses.append({"op": TEXT_CASEFOLD_EQ, "field": f"attrs.{key}", "value": sent})
        predicate = clauses[0] if len(clauses) == 1 else {"op": "and", "clauses": clauses}
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SHOW statement_timeout")
                    (previous,) = cur.fetchone()
                    cur.execute(
                        "SELECT set_config('statement_timeout', %s, true)",
                        (f"{_CASE_HINT_TIMEOUT_MS}ms",),
                    )
                rows, _more = _effective_view_sql_page(
                    conn, session, kind="nodes", predicate=predicate,
                    limit=1, offset=0, nomos_runtime=False,
                )
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT set_config('statement_timeout', %s, true)", (previous,),
                    )
        except psycopg.errors.QueryCanceled:
            # The hint's time is up: the page stands without it. Rolling back
            # to the block's savepoint also undid the timeout setting.
            return None
        except Exception:  # noqa: BLE001 - a hint is optional; the page stands.
            log.warning("lookup value hint read failed", exc_info=True)
            return None
        for row in rows:
            stored = (row.get("attrs") or {}).get(key)
            if isinstance(stored, str) and stored != sent and stored.lower() == sent.lower():
                return {"field": f"attrs.{key}", "sent": sent, "stored": stored}
        return None

    def _search_attrs_lookup(
        conn: psycopg.Connection,
        session: Session,
        *,
        filter: dict[str, Any],
        limit: int,
        offset: int,
        select: Optional[list[str]] = None,
        where: Optional[dict[str, Any]] = None,
        scope: Any = None,
        exact_count: bool = False,
    ) -> dict[str, Any]:
        """Resolve nodes by exact attr predicate — no query text, no sql_admin.

        ``exact_count`` (``completeness="exact"``, issue #3666): count every
        match with one bounded SQL COUNT when nothing can be masked and every
        condition of the call is decided in SQL; the page then says its exact
        total and a note says how it was counted.

        The typed replacement for the retired ``search_by_attrs`` /
        raw-SQL ``attrs @>`` recipe (issue #641): a least-privilege reader
        (``mcp_client``) resolves e.g. a file node by its ``path`` attr,
        scoped by subtype, through the indexed effective-attrs read path
        (PR #383). Non-None filters push as exact effective-attribute
        predicates over the pinned SQL relation; the FULL filter (including
        None == absent-or-null) is re-checked in Python over the bounded
        post-Nomos page, so least-privilege redaction and overlays behave
        exactly as on the other read paths.

        ``select`` may name ``attrs.<key>`` (D6, PACT read-passage-rows-3666):
        each row then carries ``selected`` with that attribute's value, as
        the ranked methods' rows do.
        """
        from okg.substrate.mcp.search import (
            ATTR_KEYS_KEY,
            ATTR_SELECT_PREFIX,
            SELECTED_KEY,
            split_attr_select,
        )

        attr_keys, select = split_attr_select(select)
        validate_select(
            select,
            _ATTRS_LOOKUP_FIELDS,
            tool_name="search(method=attrs)",
            elsewhere=_search_select_elsewhere("attrs"),
        )
        attr_filters = dict(filter.get("attr_filters") or {})
        _validate_attr_filters(attr_filters)
        subtype = filter.get("subtype")
        if not attr_filters and where is None and not subtype:
            # A type alone lists that type (issue #3666): the same relation,
            # paged under the reader's access view, as with an attribute
            # condition beside it. No condition at all is still refused.
            raise InvalidOperatorArguments(
                "search: a search with no query lists the records a `where` "
                "selects, and this call has no condition",
                fix_hint=(
                    'add a where, e.g. where={"op": "eq", "field": "type", '
                    '"value": "<record type>"}, or add query text'
                ),
            )
        where_context = _search_where.load_where_context(
            conn, session, where,
            subtypes=([subtype] if subtype else filter.get("subtypes")),
        )
        if limit < 1:
            raise InvalidOperatorArguments("search: limit must be >= 1")
        if offset < 0:
            raise InvalidOperatorArguments("search: offset must be >= 0")
        requested_limit, requested_offset = limit, offset
        max_offset = ATTRS_LOOKUP_MAX_OFFSET
        limit = min(limit, _ATTRS_LOOKUP_MAX_LIMIT)
        offset = min(offset, max_offset)
        pagination_capped = limit != requested_limit or offset != requested_offset
        beyond_cap = requested_offset > max_offset

        clauses: list[Any] = []
        if subtype:
            clauses.append({"op": "eq", "field": "subtype", "value": subtype})
        # A resolved search profile narrows this typed lookup too. It MUST be
        # an `in` node, not an `or` tree: both pushdown renderers drop any
        # `or` (`_attr_free_pushdown_clauses` keeps only eq/in/exists;
        # `_effective_view_where.render` has no `or` branch), so an `or` here
        # would vanish silently and the envelope would report a scope that
        # never ran. Pushdown is narrowing-only by design anyway, so the
        # residual Python filter below re-checks it authoritatively.
        scoped_subtypes = filter.get("subtypes")
        if scoped_subtypes:
            clauses.append(_profile_scope_clause(scoped_subtypes))
        clauses.extend(
            {"op": "eq", "field": f"attrs.{key}", "value": value}
            for key, value in attr_filters.items()
        )
        # The residual `where` narrows the SQL page too, or a match past the
        # first `limit` rows of the relation is never read: exact clauses on
        # policy-invariant columns, and supersets on the stored attribute a
        # catalog title/path/date reads when nothing can be masked. The
        # residual check below still decides every row. The Nomos mode is
        # read ONCE per call: the same value decides which clauses may narrow
        # the SQL page and whether the read filter runs.
        nomos_runtime = _search_where.masking_possible(session.deployment_name)
        path_prefixes: list[str] = []
        if where is not None:
            clauses.extend(_search_where.lookup_sql_clauses(
                where,
                where_context,
                masking=nomos_runtime,
                explicit_subtypes=bool(subtype or scoped_subtypes),
            ))
            # Issue #3194: a `where` path prefix also narrows the page
            # through the path-prefix index, with Nomos off only.
            path_prefixes = _search_where.lookup_path_prefixes(
                where, where_context, masking=nomos_runtime,
            )
            if not nomos_runtime:
                # #3666: the date conditions that must hold, compiled exactly
                # as filter compiles them, so the SQL page holds the records
                # they select and not whichever came first.
                exact_dates = _lookup_exact_date_clause(conn, session, where, where_context)
                if exact_dates is not None:
                    clauses.append(exact_dates)
        scan_predicate: Any = None
        if len(clauses) == 1:
            scan_predicate = clauses[0]
        elif clauses:
            scan_predicate = {"op": "and", "clauses": clauses}

        pushdown_notes: list[dict[str, Any]] = []
        page_facts: dict[str, Any] = {}

        def _attr_filters_decided(row: dict[str, Any]) -> bool:
            # Under Nomos (slice 7a) the recheck is three-valued like the
            # filter/map check: a filter on a key the read filter withheld
            # from this reader (mask-tokened, or absent from a redacted row)
            # is unknown, so it never matches, `None` (absent-or-null) and
            # the mask token included. With Nomos off a stored value is the
            # read value and every key is decided.
            if not nomos_runtime:
                return True
            attrs = row.get("attrs") or {}
            redacted = isinstance(attrs, Mapping) and attrs.get(REDACTION_MARKER) is True
            for key in attr_filters:
                if key in attrs:
                    if attrs[key] == MASK_TOKEN:
                        return False
                elif redacted:
                    return False
            return True

        def _residual_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            # Bounded post-policy parity check over read rows only.
            return [
                row
                for row in rows
                if _attr_filters_decided(row)
                and _matches_attr_filters(
                    row,
                    subtype=subtype,
                    attr_filters=attr_filters,
                )
                # The scope is enforced HERE, not only in the pushdown. Every
                # clause in this path is narrowing-only in SQL and
                # authoritative in Python; relying on the pushdown alone would
                # make the envelope's `scoped_subtypes` a claim the rows do
                # not honour.
                and (not scoped_subtypes or row.get("subtype") in set(scoped_subtypes))
            ]

        def _readable_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            # Under Nomos the page helper counts only rows this keeps (#3208):
            # the same checks the page gets below, `where` on source-hydrated
            # rows (D5). Both are idempotent, so the page runs them again.
            rows = _residual_rows(rows)
            if where is None or not rows:
                return rows
            rows = hydrate_node_source_rows_at_pin(
                conn,
                rows,
                generation_id=session.generation_id,
                branch_name=session.branch_name,
            )
            return [row for row in rows if _search_where.matches(row, where, where_context)]

        from okg.substrate.mcp.provenance import (
            hydrate_node_record_identity_at_pin,
            hydrate_node_source_rows_at_pin,
            serialize_node_read_rows,
        )

        page_meta: dict[str, Any] = {}
        if beyond_cap:
            # The requested page lies past the lookup's offset ceiling. The
            # clamped page would be a DIFFERENT page, so none is read.
            rows, scan_truncated = [], True
        else:
            rows, scan_truncated = _effective_view_sql_page(
                conn,
                session,
                kind="nodes",
                predicate=scan_predicate,
                limit=limit,
                offset=offset,
                pushdown_notes=pushdown_notes,
                nomos_runtime=nomos_runtime,
                residual=_readable_rows,
                page_facts=page_facts,
                path_prefixes=path_prefixes,
                page_meta=page_meta,
            )
        rows = _residual_rows(rows)
        # Every condition decided in SQL, nothing maskable (issue #3666): a
        # COUNT over the page's own FROM and WHERE counts the matches, and
        # the page holds every match of its window.
        from okg.substrate.mcp.operator_sql import as_operator_timestamp

        sql_exact = (
            not nomos_runtime
            and _search_where.lookup_sql_exact(where, masking=False)
            # SQL compares a date-typed attribute as an instant, the read
            # check as text: such an equality is not exact in SQL.
            and all(
                value is not None
                and not (isinstance(value, str) and as_operator_timestamp(value) is not None)
                for value in attr_filters.values()
            )
        )
        truncated = scan_truncated
        read_filter_bound = page_facts.get("read_filter_scan_bound")
        next_offset: Optional[int] = (
            offset + limit if truncated and read_filter_bound is None else None
        )
        # A cursor past the offset ceiling cannot be served: report the end
        # of paging instead of a next_offset that clamps back to this page.
        cursor_capped = beyond_cap or (
            next_offset is not None and next_offset > max_offset
        )
        if cursor_capped:
            next_offset = None
        page = rows
        # Hydrate the pinned node source before evidence classification.  The
        # effective-attrs relation may omit ``source`` even though the node is
        # source-backed in the generation; classification must see that
        # recovered provenance so an otherwise justified mapping is not
        # surfaced as an unsupported/unclassified row.
        page = hydrate_node_source_rows_at_pin(
            conn,
            page,
            generation_id=session.generation_id,
            branch_name=session.branch_name,
        )
        if where is not None:
            # Three-valued on the read rows, source hydrated (D5).
            page = [row for row in page if _search_where.matches(row, where, where_context)]
        # The effective-attrs relation carries no source record identity, so
        # this method would otherwise be the one search shape whose card
        # cannot name its producer record (issue #1444).
        page = hydrate_node_record_identity_at_pin(
            conn,
            page,
            generation_id=session.generation_id,
            branch_name=session.branch_name,
        )
        metadata = _metadata_for_rows(
            conn, session, page,
            metadata=where_context.metadata if where_context is not None else None,
        )
        if select is None or HANDLE_KEY in select:
            page = _with_handles(conn, session, page, metadata=metadata)
        page = classify_evidence_rows(conn, session, page, metadata=metadata)
        # As lexical: the citation from the row's own attrs, before the trust
        # pass fences what it must; the family from the hydrated source.
        page = [
            {**row, "citation": citation}
            if (citation := _citation_from_attrs(row.get("attrs"))) else row
            for row in page
        ]
        families = {
            row.get("node_id"): (
                scope.family_of(row.get("source")) if scope is not None else None
            )
            for row in page
        }
        page = serialize_node_read_rows(
            conn,
            page,
            deployment_name=session.deployment_name,
        )
        results = [
            project_row(
                {
                    "node_id": row.get("node_id"),
                    "subtype": row.get("subtype"),
                    "attrs": row.get("attrs"),
                    **({"text_hash": row["text_hash"]} if "text_hash" in row else {}),
                    "provenance": row.get("provenance"),
                    "evidence_kind": row.get("evidence_kind"),
                    "evidence_kind_provenance": row.get("evidence_kind_provenance"),
                    "chunk_kind": row.get("chunk_kind"),
                    "source_record_id": _record_identity(
                        row.get("source_record_id"),
                    ),
                    "source_revision": _record_identity(
                        row.get("source_revision"),
                    ),
                    **({HANDLE_KEY: row[HANDLE_KEY]} if HANDLE_KEY in row else {}),
                    "canonical_node_id": row.get("node_id"),
                    **({"citation": row["citation"]} if "citation" in row else {}),
                    "source_family": families.get(row.get("node_id")),
                },
                select,
                default=_ATTRS_LOOKUP_DEFAULT,
                always=_ATTRS_LOOKUP_ALWAYS,
            )
            for row in page
        ]
        # PACT read-names-list-3666: the deployment's citation id on every
        # lookup row too. Read from the serialized attrs, which are already
        # fenced on an untrusted row, so it is not fenced again.
        from okg.substrate.mcp.citation_id import CITATION_KEY, citation_field

        for result, row in zip(results, page):
            citation = citation_field(row.get("attrs"))
            if citation:
                result[CITATION_KEY] = citation
        if attr_keys:
            # D6: the values come from the serialized rows, so an untrusted
            # row's value is fenced as its attrs are; a key the record lacks
            # is left out and the row lists the keys it has.
            for result, row in zip(results, page):
                attrs = row.get("attrs") if isinstance(row.get("attrs"), Mapping) else {}
                result[SELECTED_KEY] = {
                    f"{ATTR_SELECT_PREFIX}{key}": attrs[key]
                    for key in attr_keys if key in attrs
                }
                if any(key not in attrs for key in attr_keys):
                    result[ATTR_KEYS_KEY] = sorted(str(key) for key in attrs)[:24]
        pin = session.generation_id
        payload: dict[str, Any] = {
            "query": "",
            "method": "attrs",
            "generation_id": pin,
            "pinned_generation_id": pin,
            "limit": limit,
            "offset": offset,
            "next_offset": next_offset,
            "results": results,
            **_operator_page_count(offset=offset, shown=len(results), truncated=truncated),
            "truncated": truncated,
        }
        if pagination_capped:
            payload["pagination_capped"] = True
            payload["requested_offset"] = requested_offset
            payload["requested_limit"] = requested_limit
        if cursor_capped:
            # No cursor past the ceiling: a clamped offset would serve the
            # same page again and the caller would loop on it. Say where
            # paging stops and how to reach the rest.
            payload["pagination_capped"] = True
            payload["pagination_max_offset"] = max_offset
            payload["pagination_notice"] = (
                f"lookup paging stops at offset {max_offset:,}; rows past "
                "it are not read, so a match there is not reachable by "
                'paging. Narrow the where: add a type ({"op": "eq", "field": '
                '"type", "value": "<subtype>"}) or an exact attrs.<key> '
                "equality."
            )
        if read_filter_bound is not None:
            payload.update(_read_filter_bound_notice(read_filter_bound))
        if pushdown_notes:
            # issue #3029: answered by the exact scan; see _effective_view_sql_page.
            payload["notes"] = pushdown_notes
        counted = False
        if (
            exact_count and sql_exact and page_meta and read_filter_bound is None
            and not cursor_capped
        ):
            fields = _operator_count_fields(
                conn, session, offset=offset, shown=len(rows), page_meta=page_meta,
            )
            if fields.get("total_count_basis") == "exact":
                payload.update(fields)
                counted = True
                payload.setdefault("notes", []).append(dict(COUNT_EXACT_NOTE))
        if where_context is not None:
            payload.update(where_context.disclosure(evaluation_scope="bounded_lookup_sql_page"))
            if not counted:
                # This count is a page survivor count, not all predicate matches.
                payload["total_count_basis"] = "unknown"
                payload["total_count"] = None
        if not counted:
            _mark_not_a_total(
                payload, rows=len(results), page_facts=page_facts, page_meta=page_meta,
                for_a_total=(
                    _lookup_as_filter_call(
                        subtype=subtype, scoped_subtypes=scoped_subtypes,
                        attr_filters=attr_filters, where=where,
                    )
                    if not nomos_runtime else None
                ),
            )
        if not results and not truncated and not nomos_runtime and offset == 0:
            # A first page only: an empty page past the end says nothing
            # about whether the value matches (review of 283fa6ea484).
            hint = _case_folded_hint(
                conn, session, subtype=subtype, attr_filters=attr_filters, where=where,
                scoped_subtypes=scoped_subtypes,
            )
            if hint is not None:
                payload["value_hint"] = hint
        return payload

    def _offer_windows_view(refusal: Any, window_arguments: dict[str, Any]) -> None:
        """For a grep call that names a windows argument without the view.

        Offers the same call with `view="windows"` when proven. When the
        windows read would refuse another argument of the call, the hint
        says which, instead of telling the caller to add the view.
        """
        from okg.substrate.mcp.record_window_search import validate_window_call

        argument = str((getattr(refusal, "data", None) or {}).get("argument") or "")
        sent = call_arguments()
        if sent is not None and offer_retry_call(
            mcp, "search", refusal, {**sent, "view": "windows"},
        ):
            return
        try:
            validate_window_call(view="windows", **window_arguments)
        except SubstrateError as blocked:
            blocker = str((getattr(blocked, "data", None) or {}).get("argument") or "")
            if blocker and blocker != argument:
                refusal.fix_hint = (
                    f"drop {argument}; only view=\"windows\" takes it, and "
                    f"that view does not take this call's {blocker}"
                )

    def search(
        query: Optional[str] = None,
        method: SearchMethod = "lexical",
        where: Optional[PredicateInput] = None,
        scope: Optional[SearchScope] = None,
        subtype_filter: Optional[str] = None,
        attr_filters: Optional[dict[str, Any]] = None,
        filter: Optional[SearchFilter] = None,
        profile: Optional[str] = None,
        source_family: Optional[Union[str, list[str]]] = None,
        max_per_parent: Optional[
            declared(Any, type="integer", minimum=1)
        ] = None,
        rank_by: Optional[Literal["records", "self", "passages"]] = None,
        limit: Optional[_SearchLimit] = None,
        offset: Offset = 0,
        generation_id: Optional[Union[int, str]] = None,
        select: Optional[list[str]] = None,
        authority: Optional[dict[str, Any]] = None,
        allow_unverified_embedding_provenance: bool = False,
        max_bytes: MaxBytes = None,
        max_chars: Optional[
            declared(Any, type="integer", minimum=64, maximum=MAX_PREVIEW_CHARS)
        ] = None,
        expand_concepts: Optional[bool] = None,
        path_prefix: Optional[str] = None,
        continuation_token: Optional[str] = None,
        # `cards`/`list` are delivery views (PACT read-model-s4-list);
        # `windows` is the grep record-windows read (PACT read-record-windows).
        view: Optional[Literal["cards", "list", "windows", "metadata"]] = None,
        matching: Optional[Literal["record", "passage"]] = None,
        context_chars: Optional[
            declared(Any, type="integer", minimum=0, maximum=4096)
        ] = None,
        completeness: Optional[Literal["bounded", "exact"]] = None,
        retain: bool = False,
        summary: Optional[
            Annotated[list[str], PField(min_length=1, max_length=4)]
        ] = None,
        within: Optional[Literal["unit", "record"]] = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Find records by words, best first.

        search(query="<words>", where={"type":"<type>"}, limit=8). A page is
        the best matches, not all of them; completeness="exact" counts every
        match or refuses. Read narrow first: ids and titles
        (select=["node_id","handle"] or a small limit), then read the
        records that matter. Without a type in `where`, the deployment's default
        search types apply. `where` with no query looks records up by field;
        `contains` finds text in attrs.<key>, path or title.[[if tool:inspect]] A
        known id goes to `inspect`.[[/if]]

        method="grep" is exact, case-insensitive: words, "a phrase",
        a NEAR/80 b, -word, re:/<pattern>/, path:<text>. All terms must match
        one text; re:/a|b|c/ for variants. Grep lists one line per record:
        id, title, path, match count, first matching line. within="record": each term anywhere
        in a record, in id order; add select=["node_id","handle"] to list
        every match cheaply.
        `scope={"record": id}` searches one record and its passages (lexical).
        `retain=true` names the page's records as a result reference[[if tool:map|filter|aggregate]]
        for [[tools map/filter/aggregate]] to read[[/if]].
        """
        # PACT read-names-list-3666: every row names the record's citation
        # id when this deployment declares `citation_attr`.
        _citation_id.CITATION_ATTR.set(
            _citation_id.deployment_citation_attr(deployment_name)
        )
        # `filter` is a validated SearchFilter by the time an MCP call gets
        # here (#2793); the body below reads the plain mapping it always did.
        filter = search_filter_mapping(filter)
        # Format v10 item G2: a grep page's token sent back without `method`
        # is read as the grep call it continues (an `adjusted` note says so).
        _sent_arguments = call_arguments()
        method, _continuation_notes = _search_arguments.read_continuation_method(
            method, continuation_token,
            method_sent=None if _sent_arguments is None else "method" in _sent_arguments,
        )
        if _continuation_notes:
            note_adjusted(_continuation_notes)
        # `where` and `scope` (PACT read-model-s3-where-scope): validated and
        # put in canonical form here, before `_open`, so a field search
        # cannot read is refused by name before any database work.
        where = _search_where.normalize_where(as_mapping(where))
        # Issue #3666 step 2b: a source name sent as `source_family`, or a
        # record type sent as `profile`, is read as the `where` condition it
        # means. From here on the call is the corrected call; whether the
        # name exists is checked once the session is open and can only
        # refuse (`_check_scope_reading`).
        scope_declared = (
            _declared_search_names(deployment_name)
            if source_family is not None or profile is not None else None
        )
        scope_reading = _search_arguments.read_scope_arguments(
            source_family=source_family, profile=profile, where=where,
            method=method, query=query, view=view,
            declared_families=scope_declared[0] if scope_declared else None,
            declared_profiles=scope_declared[1] if scope_declared else None,
        )
        if scope_reading.notes:
            where = scope_reading.where
            source_family, profile = scope_reading.source_family, scope_reading.profile
            note_adjusted(scope_reading.notes)

        def _check_scope_reading(conn: Any, session: Any) -> None:
            from okg.substrate.mcp.search_profiles import (
                unknown_profile_refusal,
                unknown_source_refusal,
            )

            assert scope_declared is not None
            if scope_reading.record_type is not None and not (
                _search_arguments.is_record_type(conn, session, scope_reading.record_type)
            ):
                forget_adjusted(scope_reading.notes)
                raise unknown_profile_refusal(
                    scope_reading.record_type, declared=scope_declared[1],
                )
            if scope_reading.source is None:
                return
            try:
                access_rules = _search_where.masking_possible(session.deployment_name)
            except Exception:  # noqa: BLE001 - unknown mode: read nothing row-derived.
                access_rules = True
            names = _search_arguments.load_source_names(
                conn, session, access_rules=access_rules,
            )
            known = names.known(scope_reading.source)
            if known is True:
                return
            forget_adjusted(scope_reading.notes)
            refusal = unknown_source_refusal(
                scope_reading.source, declared=scope_declared[0],
                close=_search_arguments.close_names(scope_reading.source, names.names),
            )
            sent = call_arguments()
            if known is None and sent is not None:
                # The list cannot say: the corrected call is offered as a
                # guess. Nothing here was read from rows, so the refusal is
                # the same whether or not the graph holds such a source.
                candidate = {
                    key: value for key, value in sent.items()
                    if key != "source_family"
                    and not (key == "profile" and scope_reading.record_type is not None)
                }
                candidate["where"] = scope_reading.where
                offer_retry_call(mcp, "search", refusal, candidate, mode="guess")
            raise refusal

        record_ids = _search_where.normalize_record_scope(as_mapping(scope))
        children_scope = _search_where.normalize_children_scope(as_mapping(scope))
        _search_where.assert_children_scope_served(
            children_scope, rank_by=rank_by, view=view,
            retain=bool(retain) or view == "metadata",
        )
        # Pure argument validation, before `_open`: a profile named on a
        # method that cannot honour a subtype scope is refused rather than
        # accepted and discarded, which would hand back an unscoped page to
        # a caller who believes it was scoped.
        _effective_method = (
            "attrs"
            if (
                method == "lexical"
                and not query
                and (
                    attr_filters or (filter or {}).get("attr_filters")
                    or where is not None
                )
            )
            else method
        )
        # PACT read-names-list-3666: a grep call that names no view (and no
        # card or windows argument) is delivered as the names list; the page
        # says so on its first line (`view_implied`).
        from okg.substrate.mcp.search_views import implied_search_view

        _sent_view = view
        view = implied_search_view(
            view, method=_effective_method, within=within,
            arguments={
                "select": select, "matching": matching,
                "context_chars": context_chars, "scope": scope,
                "retain": retain, "summary": summary,
            },
        )
        view_implied = _sent_view is None and view is not None
        # A result reference (#3222 slice 2): every form that cannot produce
        # one is refused by name here, from the arguments alone, before any
        # SQL (design D1, D5).
        # `summary` (#3222 slice 3) is served only with view="metadata":
        # refused by name here, before any SQL.
        refusal = _result_references.summary_refusal(view=view, summary=summary)
        if refusal is not None:
            raise refusal
        metadata_view = view == "metadata"
        reference_wanted = bool(retain) or metadata_view
        if reference_wanted:
            from okg.substrate.mcp.search_views import default_search_limit

            refusal = _result_references.producer_refusal(
                method=_effective_method, rank_by=rank_by, view=view, offset=offset,
                select=select, max_chars=max_chars,
                limit=(
                    limit if limit is not None
                    else default_search_limit(view, _effective_method)
                ),
            )
            if refusal is not None:
                raise refusal
        # The delivery view (PACT read-model-s4-list): refused by name before
        # any database work when the effective method/rank mode cannot serve
        # it, and the omitted `limit` resolved to the view's own default.
        # `windows` is validated by its own read below, so it resolves here
        # as cards do: the default limit and an unchanged select.
        limit, select = _resolve_search_view(
            None if view == "windows" else view,
            method=_effective_method,
            rank_by=rank_by,
            max_per_parent_argument=max_per_parent is not None,
            limit=limit,
            select=select,
        )
        # The start-time probe's guard (#3151 slice 1), FIRST and before any
        # database work. The published schema no longer lists a method or
        # rank_by value this server cannot run, but the validator still
        # accepts every one, so a client with a cached schema reaches this
        # named refusal instead of a schema error. `semantic` keeps its own
        # code and is refused just below from the same record.
        assert_search_capability(
            capabilities, method=_effective_method,
            # A list always ranks records, so a server without records refuses it.
            # The query-less lookup's list ranks nothing (issue #3666).
            rank_by=(
                "records" if view == "list" and _effective_method != "attrs" else rank_by
            ),
        )
        if continuation_token is not None and _effective_method != "grep":
            refusal = _operator_refusal(
                "search",
                code="continuation_token_not_applicable",
                message=(
                    "search: continuation_token applies only to method='grep'; "
                    f"got method={_effective_method!r}"
                ),
            )
            if _search_arguments.continuation_kind(continuation_token) == "grep_page":
                # Format v10 item G2: a grep page's token; offset would read
                # the same candidates again, so it is not offered.
                refusal["error"]["fix_hint"] = (
                    "this token continues a grep page: send it with "
                    "method='grep' and the same query and where"
                )
            else:
                refusal["error"]["fix_hint"] = (
                    "page with offset, or drop continuation_token"
                )
            return refusal
        if path_prefix is not None and _effective_method != "grep":
            # Refused by name rather than as an opaque execution failure: a
            # scope that quietly did not apply reads as a scoped answer.
            refusal = _operator_refusal(
                "search",
                code="path_prefix_not_applicable",
                message=(
                    f"search: path_prefix {_json.dumps(path_prefix)[:200]} is "
                    "read by method='grep' only, where it keeps records whose "
                    f"stored path starts with it; this call is method={_effective_method!r}"
                ),
            )
            refusal["error"]["fix_hint"] = (
                "send the same query and path_prefix with method='grep' for "
                "exact-text matches under that path, or drop path_prefix to "
                "keep this ranked search"
            )
            # Issue #3666: the grep form is offered as a guess, because grep
            # matches exact text, not ranked terms, and it may still refuse
            # a path scope on a graph with no stored path. Offered only when
            # the call holds no condition grep would drop (a `where`, a
            # scope); its `select` and `max_chars` name lexical card fields
            # and are left out, its `max_bytes` is kept.
            sent = call_arguments()
            allowed = {
                "query", "method", "limit", "path_prefix", "rank_by",
                "select", "max_chars", "max_bytes",
            }
            if sent is not None and set(sent) <= allowed:
                probe = SubstrateError(refusal["error"]["message"])
                candidate = {
                    key: sent[key]
                    for key in ("query", "limit", "path_prefix", "max_bytes") if key in sent
                }
                if offer_retry_call(
                    mcp, "search", probe, {**candidate, "method": "grep"}, mode="guess",
                ):
                    refusal["error"]["retry_call"] = probe.retry_call
            return refusal
        if _effective_method == "grep":
            # Refused before any connection opens: the grammar and the regex
            # subset are pure (PACT mcp-grep-3067).
            from okg.substrate.mcp.grep_query import parse_grep_query
            from okg.substrate.mcp.passage_rollup import validate_rank_by

            parse_grep_query(query or "")
            validate_rank_by(rank_by, method="grep")
        if within is not None:
            # PACT read-search-caps-3666: refused by name before any read.
            from okg.substrate.mcp.grep_record import validate_within

            within = validate_within(
                within, method=_effective_method, query=query, view=view,
                record_scope=record_ids is not None or children_scope is not None,
                rank_by=rank_by, completeness=completeness, retain=bool(retain),
                max_per_parent=max_per_parent, offset=offset,
                continuation_token=continuation_token,
            )
        # `view="windows"` (PACT read-record-windows): every argument a
        # windows read would not honour is refused here, before `_open`.
        window_arguments = dict(
            matching=matching, context_chars=context_chars,
            method=_effective_method, record_ids=record_ids, query=query,
            where=where, path_prefix=path_prefix, offset=offset, select=select,
            rank_by=rank_by, profile=profile, source_family=source_family,
            subtype_filter=subtype_filter, attr_filters=attr_filters,
            filter=filter, max_per_parent=max_per_parent,
        )
        try:
            window_call = _validate_window_call(
                view=view if view == "windows" else None, **window_arguments,
            )
        except SubstrateError as window_refusal:
            if (
                view is None and _effective_method == "grep"
                and getattr(window_refusal, "code", None) == "window_argument_unsupported"
            ):
                # Only the windows read takes this argument (issue #3666):
                # the same call with view="windows" is offered when the
                # server proves it serves it. Otherwise the hint names the
                # argument of this call that the windows read refuses, so
                # it never points at a call that would be refused too.
                _offer_windows_view(window_refusal, window_arguments)
            raise
        if (
            (record_ids is not None or children_scope is not None)
            and _effective_method != "lexical" and window_call is None
        ):
            if _effective_method == "grep":
                message = (
                    "search: method='grep' serves scope only with "
                    "view='windows' (each scoped record's literal matching "
                    "windows)"
                )
                hint = (
                    "pass view='windows' (no offset), or rank within "
                    "the records with method='lexical' and the same scope and query"
                )
            elif _effective_method == "identifier_mentions":
                # Its rows are spans of the query text, not records (#3646).
                message = (
                    "search: method='identifier_mentions' reads your query "
                    "text, not records, so scope cannot apply"
                )
                hint = "drop scope"
            else:
                message = (
                    "search: scope is served on method='lexical' with query "
                    f"text, not {_effective_method!r}"
                )
                hint = "pass query text with method='lexical', or drop scope"
            scope_refusal = _search_where.SearchScopeUnsupported(
                message,
                data={"method": _effective_method, "scope": "records"},
                fix_hint=hint,
            )
            if _effective_method == "grep" and view is None:
                # Grep takes a scope in one form only, the windows read.
                sent = call_arguments()
                if sent is not None:
                    offer_retry_call(
                        mcp, "search", scope_refusal, {**sent, "view": "windows"},
                    )
            raise scope_refusal
        if (
            record_ids is not None and rank_by == "passages"
            and _effective_method == "lexical"
        ):
            # Refused here, before the session opens, with the call that
            # ranks the scoped record's passages (issue #3666). `records`
            # would give one card per record instead, so it is a guess.
            scope_refusal = _search_where.SearchScopeUnsupported(
                "search: scope names the records to search and "
                "rank_by='passages' ranks passages of every record, so the "
                "two are not served together",
                data={"method": _effective_method, "rank_by": rank_by, "scope": "records"},
                fix_hint=(
                    "pass rank_by='self' to rank the scoped records' passages "
                    "by their own text, or rank_by='records' for one card per record"
                ),
            )
            sent = call_arguments()
            if sent is not None:
                offer_retry_call(
                    mcp, "search", scope_refusal, {**sent, "rank_by": "self"},
                    mode="guess",
                )
            raise scope_refusal
        if where is not None and _effective_method == "identifier_mentions":
            raise _search_where.WhereUnsupported(
                "search: where does not apply to method='identifier_mentions'",
                data={"method": _effective_method},
                fix_hint="drop where, or use method='lexical'",
            )
        _assert_method_can_scope(_effective_method, profile)
        # Same discipline for the authority and diversity controls: refused
        # before any connection is opened, so a caller who names one where it
        # cannot apply learns that instead of receiving a page that quietly
        # ignored it.
        _assert_method_can_filter_family(_effective_method, source_family)
        _validate_max_per_parent(max_per_parent, origin="search: max_per_parent")
        if max_per_parent is not None and _effective_method != "lexical":
            # Was dropped silently when the scope was narrowed to the method
            # (#3646): refused instead, as the diversity rule says.
            from okg.substrate.mcp.result_diversity import (
                DiversityNotApplicable,
                assert_diversity_rank_mode,
            )

            if _effective_method == "attrs":
                # A `where` with no query runs as attrs: unranked attribute
                # matches, so "use lexical" would name the call just made.
                raise DiversityNotApplicable(
                    "max_per_parent caps ranked rows per parent; a where with "
                    "no query returns unranked attribute matches (method='attrs')",
                    data={"method": "attrs", "max_per_parent": max_per_parent},
                    fix_hint="drop max_per_parent, or add query text to rank the matches",
                )
            assert_diversity_rank_mode(
                max_per_parent, rank_by="self", method=_effective_method,
            )
        # PACT read-passage-rows-3666 (D2): a `max_chars` the caller passed
        # sizes each row's passage; absent, the per-type sizes apply.
        passage_chars_given = max_chars is not None
        max_chars, max_chars_lowered = _clamp_preview_chars(max_chars)

        def _with_lowered(page: dict[str, Any]) -> dict[str, Any]:
            # Issue #3252: the served page names the lowered value.
            if max_chars_lowered is not None and isinstance(page, dict):
                page = {**page, "max_chars_lowered": dict(max_chars_lowered)}
            return page

        # `exact` refused here on identifier_mentions, which never reaches
        # `search()`; `search()` refuses it on semantic/alias itself. Attrs
        # and grep check their page after the read (#3646).
        _validate_completeness(completeness, method=_effective_method)
        if completeness == "exact" and view == "windows":
            # A windows page is cut to its byte budget and counts nothing,
            # so it cannot say it read every match (#3646).
            from okg.substrate.mcp.search import SearchCompletenessRefused

            raise SearchCompletenessRefused(
                "search: completeness='exact' is not served with view='windows' "
                "(a windows page is cut to its byte budget and carries no count)",
                data={"completeness": repr(completeness), "method": _effective_method,
                      "bound": "view='windows'"},
                fix_hint=(
                    "drop completeness to read the windows, or drop view for "
                    "method='grep' with completeness='exact' and its exact count"
                ),
            )
        # `select=["source_lineage", ...]` (issue #2674). Lexical rows carry
        # no `attrs` by default, so the operator is asked for its default
        # projection plus `attrs`; the lineage is then moved to a row-level
        # field and the added `attrs` dropped. A caller who named only the
        # token still gets the default excerpt preview.
        # A select refusal's ready retry names the caller's own list, not
        # the rewritten one (PACT select-fields-by-method).
        caller_select = list(select) if isinstance(select, (list, tuple)) else select
        if _effective_method in _SEARCH_LINEAGE_METHODS:
            (
                select,
                lineage_requested,
                lineage_drop_attrs,
                preview_as_default,
            ) = _lineage_select_plan(
                select,
                default_without_attrs=(
                    _LEXICAL_SEARCH_DEFAULT_FIELDS
                    if _effective_method == "lexical"
                    else None
                ),
            )
        else:
            if isinstance(select, (list, tuple)) and "source_lineage" in select:
                # Refused by name rather than left to the select validator,
                # whose "unknown field" would not say where the token works.
                return _operator_refusal(
                    "search",
                    code="source_lineage_select_unsupported",
                    message=(
                        f"select 'source_lineage' is served on "
                        f"method='lexical' or 'semantic', not "
                        f"{_effective_method!r}; read one node's lineage with "
                        "inspect(target={'kind': 'node', 'node_id': ...}, "
                        "select=['source_lineage'])"
                    ),
                )
            lineage_requested = False
            lineage_drop_attrs = False
            preview_as_default = select is None
        # Issue #641: a query-less call with attr_filters is the typed
        # attrs lookup, not a text search. Route it explicitly so the
        # reported shape `search(attr_filters={...}, subtype_filter=...)`
        # resolves nodes instead of raising "query must be non-empty".
        if (
            method == "lexical"
            and not query
            and (
                attr_filters or (filter or {}).get("attr_filters")
                or where is not None
            )
        ):
            method = "attrs"
        if method == "attrs" and query:
            raise InvalidOperatorArguments(
                "search: method='attrs' is a query-less attrs lookup; "
                "pass attr_filters (+ subtype_filter), not query text. "
                "For text search use method='lexical'."
            )
        if children_scope is not None:
            # Before the default below, which would make it `records`.
            rank_by = "self"
        rank_by = _validate_rank_by(
            rank_by,
            method=method,
            max_per_parent_argument=max_per_parent is not None,
        )
        if method == "semantic" and (
            embedder is None or not capabilities.method("semantic").available
        ):
            refusal = _operator_refusal(
                "search",
                code="semantic_search_unavailable",
                message="search method='semantic' requires an embedder",
                fallback_operator="search",
            )
            refusal["error"]["fallback_method"] = "lexical"
            # The generic fallback hint ("use search for this shape") sends
            # the caller in a circle; the move is the lexical method.
            refusal["error"]["fix_hint"] = "retry with method='lexical'"
            refusal["error"]["capability"] = {
                "method": "semantic",
                "embedder_configured": embedder is not None,
                "status": capabilities.method("semantic").status,
                "reason": capabilities.method("semantic").reason,
                "available_methods": list(capabilities.available_methods()),
            }
            return refusal
        if method not in {"attrs", "identifier_mentions"}:
            # Every remaining method reaches `search()`, which refuses a
            # filter key outside subtype/attr_filters/source. Refused here
            # first, by name and with the remedy, rather than as the opaque
            # execution failure that refusal used to surface as (#2793).
            # `attrs` and `identifier_mentions` never reached that check and
            # are left exactly as they were.
            refuse_unknown_search_filter_keys(filter)
        # A `where` type equality is a subtype filter in every respect
        # (D9): it decides whether the deployment default profile applies and
        # is recorded as the requested subtype, exactly as `subtype_filter`.
        scope_subtype: Any = subtype_filter
        if scope_subtype is None and not (filter or {}).get("subtype"):
            if _effective_method == "grep":
                # Grep keeps `type` in its record-judged `where` (never its
                # subtype filter), but for the profile a top-level `type = X`
                # and `type in [...]` both name subtypes (review M1), so the
                # two spellings decide the profile alike.
                named = _search_where.profile_subtypes(where)
                scope_subtype = (
                    named[0] if len(named) == 1 else (list(named) or None)
                )
            else:
                scope_subtype = _search_where.plan_where(
                    None, where, lift_source=False,
                ).filter.get("subtype")
        retained: dict[str, Any] | None = None
        with _open(
            generation_id,
            operator_name="search",
            resource={
                "query": query or "",
                "method": method,
                "rank_by": rank_by,
                "limit": limit,
                "offset": offset,
                "subtype_filter": scope_subtype,
                # The profile that WILL apply, plus its DECLARED (un-expanded)
                # subtypes. Note `profile` alone is not enough: under a
                # deployment `default_profile` the caller passes None and the
                # read is still scoped, so a record naming the REQUESTED value
                # would describe a broader access than the one executed. The
                # refinement closure is not available here — expanding it needs
                # the pinned catalog version, which only exists inside `_open`
                # — so the resource carries the declared set and the envelope
                # carries the resolved closure.
                **_nomos_profile_record(
                    deployment_name,
                    profile,
                    subtype_filter=scope_subtype,
                    method=method,
                ),
            },
            authority=authority,
        ) as h, select_retry_from(caller_select):
            if scope_reading.notes:
                _check_scope_reading(h.conn, h.session)
            # #3666: a `source` condition reads the source ids this server
            # states once; a repository name (the part before the first `.`
            # of source ids) is read as those ids, with a note
            # (`source_hint.read_repo_names`).
            from okg.substrate.mcp.source_hint import read_repo_names, source_conditions

            source_names: Optional[frozenset[str]] = None
            if source_conditions(where):
                try:
                    access_rules = _search_where.masking_possible(h.session.deployment_name)
                except Exception:  # noqa: BLE001 - unknown mode: read nothing row-derived.
                    access_rules = True
                source_names = _search_arguments.load_source_names(
                    h.conn, h.session, access_rules=access_rules,
                ).names
                where, repo_notes = read_repo_names(where, source_names)
                note_adjusted(repo_notes)
            filt: dict[str, Any] = {}
            if filter:
                filt.update(filter)
            if subtype_filter:
                filt["subtype"] = subtype_filter
            if attr_filters:
                filt["attr_filters"] = attr_filters
            search_scope = _resolve_mcp_search_scope(
                h.conn,
                h.session,
                deployment_name=deployment_name,
                requested_profile=profile,
                subtype_filter=scope_subtype,
                method=method,
                requested_families=source_family,
                requested_max_per_parent=max_per_parent,
            )
            if method == "attrs":
                # `attrs` returns node rows, so the scope narrows it too — as
                # an additional subtype constraint on the typed lookup. This
                # path does NOT go through `search()`, so it must enforce the
                # subtype/profile intersection itself; otherwise a
                # `subtype_filter` outside the profile would AND to nothing and
                # hand back an empty page indistinguishable from "no matches".
                attrs_plan = _search_where.plan_where(
                    {k: v for k, v in filt.items() if k != "source"},
                    where,
                    lift_source=False,
                )
                lifted_subtype = (
                    attrs_plan.filter.get("subtype")
                    if filt.get("subtype") is None else None
                )
                filt = {**filt, **attrs_plan.filter}
                # A subtype lifted from `where` meets the profile exactly as
                # `subtype_filter` does; `filter.subtype` keeps its old path.
                _intersect_subtype_filter(
                    search_scope, subtype_filter or lifted_subtype,
                )
                if search_scope.subtypes is not None:
                    filt["subtypes"] = list(search_scope.subtypes)
                payload = _search_attrs_lookup(
                    h.conn,
                    h.session,
                    filter=filt,
                    limit=limit,
                    offset=offset,
                    select=select,
                    where=attrs_plan.residual,
                    scope=search_scope,
                    exact_count=completeness == "exact",
                )
                if completeness == "exact":
                    # Every visible match on this page, or a refusal (#3646).
                    from okg.substrate.mcp.search import require_exact_page

                    try:
                        require_exact_page(payload, method="attrs")
                    except SubstrateError as exact_refusal:
                        # No paging reaches the rest (issue #3666): offer the
                        # `filter` call that lists and counts the same set,
                        # when the server proves it takes it. Under access
                        # rules `filter` stops at the same bound: no offer.
                        filter_call = (
                            None if _search_where.masking_possible(h.session.deployment_name)
                            else _lookup_as_filter_call(
                                subtype=filt.get("subtype"),
                                scoped_subtypes=filt.get("subtypes"),
                                attr_filters=dict(filt.get("attr_filters") or {}),
                                where=attrs_plan.residual,
                            )
                        )
                        if filter_call is not None:
                            offer_retry_call(
                                mcp, "filter", exact_refusal, filter_call["arguments"],
                            )
                        raise
                elif completeness is not None:
                    payload["completeness"] = completeness
                applied_text = _search_where.describe(attrs_plan.applied)
                if applied_text is not None:
                    payload["where_applied"] = applied_text
                payload["search_profile"] = search_scope.profile
                payload["scoped_subtypes"] = (
                    list(search_scope.subtypes)
                    if search_scope.subtypes is not None
                    else None
                )
                payload["rank_by"] = rank_by
                # No family, exclusion, or diversity control has an enforceable
                # meaning on this path (the effective-attrs relation pages
                # before the producer id is hydrated), so profile-declared ones
                # do NOT apply here. Emit the COMPLETE disclosure key set with
                # each dropped control marked inapplicable, not a partial set:
                # a payload that carried `source_family_applicable` while
                # omitting `excluded_path_globs` would read as "no exclusion
                # was requested" when a profile declared one and this method
                # discarded it. An EXPLICIT `source_family` already raised
                # before `_open`.
                payload.update(_family_disclosure(search_scope, method=method))
                # Same empty-scope disclosure the ranked paths give: an empty
                # page under a scope that holds nothing is a different state
                # from one under a scope that holds plenty, and neither is
                # readable from the page itself.
                if search_scope.subtypes is not None and not payload.get("results"):
                    payload.update(
                        _scope_emptiness_disclosure(
                            h.conn,
                            h.session,
                            search_scope,
                        )
                    )
            elif method == "identifier_mentions":
                if not query:
                    raise InvalidOperatorArguments("search: identifier_mentions requires query text")
                payload = _find_identifier_mentions(
                    h.conn,
                    h.session,
                    query,
                    select=select,
                )
                payload["method"] = method
                mentions = payload.get("mentions", [])
                if isinstance(mentions, list):
                    total = len(mentions)
                    truncated = offset + limit < total
                    page = mentions[offset : offset + limit]
                    payload["mentions"] = page
                    payload["results"] = page
                    payload["total_count"] = total
                    payload["total_count_basis"] = "exact"
                    payload["retrieval_exhaustive"] = True
                    payload["selection_basis"] = "input_set"
                    payload["retrieval_basis"] = "input_text"
                    payload["truncated"] = truncated
                    payload["limit"] = limit
                    payload["offset"] = offset
                    payload["next_offset"] = offset + limit if truncated else None
                else:
                    payload["results"] = mentions
                # A subtype scope has no defined meaning here. Say so as a
                # STATE rather than leaving the caller to infer it from a
                # missing key: a deployment default that quietly does not
                # apply is the fail-open this contract forbids.
                payload["search_profile"] = None
                payload["scoped_subtypes"] = None
                payload["profile_applicable"] = False
                if filt:
                    # An old filter argument the caller passed does not apply
                    # to this method: say so (`where` itself is refused above).
                    payload["where_applied"] = None
                # The complete disclosure set, every control inapplicable. See
                # the `attrs` branch above for why a partial set is worse than
                # none at all.
                payload.update(_family_disclosure(search_scope, method=method))
                payload["rank_by"] = rank_by
            else:
                if not query:
                    raise InvalidOperatorArguments(
                        "search: query must be non-empty. To resolve a "
                        "node by attrs without query text (e.g. by file "
                        "path), pass a where with no query that names a "
                        'type — search(where={"op": "and", "clauses": '
                        '[{"op": "eq", "field": "type", "value": '
                        '"source_file"}, {"op": "eq", "field": "attrs.path", '
                        '"value": ...}]}).'
                    )
                window_measure = None
                window_reserve = 0
                if window_call is not None:
                    from okg.substrate.mcp.response_bounds import (
                        public_call_tool_result_bytes,
                    )

                    def window_measure(page: dict[str, Any]) -> int:
                        # The page's own served form, without the stamps
                        # that vary per call (freshness ages, session
                        # notices): cut points depend only on the page, so
                        # a retried token gives the same page while the
                        # stamps keep their reserved size class.
                        return public_call_tool_result_bytes(
                            _compact_for_wire(
                                with_read_coverage(
                                    {**page, "operator_name": "search"},
                                ),
                                {"select": select},
                            ),
                            tool="search",
                        )

                    def _served(page: dict[str, Any]) -> int:
                        # At the largest freshness outcome, so a freshness
                        # failure on a retry never moves the cut points.
                        return _worst_case_stamped_bytes(
                            _maybe_stamp_advanced(h, _with_lowered(dict(page))), "search",
                            {"generation_id": generation_id, "select": select},
                            tool="search",
                        )

                    # The stamps' measured cost on this session; the windows
                    # read adds its spare and drift room, so a change of a
                    # few digits does not move the page's cut points.
                    probe = {
                        "status": "ok", "method": "grep", "view": "windows",
                        "generation_id": h.session.generation_id,
                        "pinned_generation_id": h.session.generation_id,
                        "results": [], "has_more": True,
                        "continuation_token": "x",
                    }
                    stamp_cost = max(0, _served(probe) - window_measure(probe))
                    # The windows read turns it into the page's reserve and
                    # decides whether a token's reserve still holds it.
                    window_reserve = stamp_cost + adjusted_reserve_bytes()

                memo_key = ""
                if reference_wanted:
                    if not _effective_view_ready(h.conn, h.session.generation_id, "node"):
                        raise _result_references.unsupported(
                            "effective_view_not_ready",
                            message="search: the pinned generation has no effective-"
                            "attribute view, so no consumer could read a reference",
                            fix_hint="search without retain, or publish the effective "
                            "attribute view for this generation",
                        )
                    selection = {
                        "query": query, "where": where,
                        "scope": (
                            record_ids if children_scope is None
                            else children_scope.applied()
                        ),
                        "method": method, "rank_by": rank_by, "profile": profile,
                        "source_family": source_family,
                        "max_per_parent": max_per_parent,
                        "expand_concepts": expand_concepts,
                        "subtype_filter": subtype_filter,
                        "attr_filters": attr_filters, "filter": filter,
                        "completeness": completeness,
                    }
                    memo_key = "search:" + _result_references.selection_fingerprint(selection)
                    memo = _result_references.call_memo()
                    if memo is not None and memo_key in memo:
                        # A byte-fitting rerun: the members were frozen at the
                        # requested limit on the call's first attempt.
                        retained = memo[memo_key]
                    else:
                        retained = {
                            "selection": _result_references.selection_fingerprint(
                                {**selection, "limit": limit},
                            ),
                            "limit": limit,
                        }
                try:
                    payload = _search(
                        h.conn,
                        h.session,
                        query,
                        method=method,
                        reference_members=(
                            retained is not None and "members" not in retained
                        ),
                        # PACT read-passage-rows-3666 (R4): the grep exact
                        # pre-pass is off on lexical pages. It named records
                        # the rows did not hold and missed records they did;
                        # each row's matched passage now shows where it hit.
                        exact_pass=False,
                        passage_chars=max_chars if passage_chars_given else None,
                        # `search()`'s own `view` is the windows read; a
                        # delivery view travels as `delivery_view` below.
                        view="windows" if view == "windows" else None,
                        matching=matching,
                        context_chars=context_chars,
                        max_bytes=int(max_bytes) if max_bytes is not None else None,
                        window_measure=window_measure,
                        window_reserve=window_reserve,
                        filter=filt,
                        limit=limit,
                        offset=offset,
                        embedder=embedder,
                        select=select,
                        scope=search_scope,
                        rank_by=rank_by,
                        effective_view_source=_effective_view_source,
                        # Passed UNCONDITIONALLY, not only when the method is
                        # semantic. Gating on the method here would silently
                        # discard the flag on a lexical call and hand back a
                        # normal page, so an agent that believes it loosened
                        # provenance checking learns nothing -- while the same
                        # mistake raises on the CLI and inside `search()`. That
                        # divergence is the argument-surface form of absence
                        # dressed as agreement; `search()` owns the refusal.
                        allow_unverified_provenance=bool(
                            allow_unverified_embedding_provenance
                        ),
                        max_chars=max_chars,
                        # Unconditional: gating on the method here would
                        # swallow an explicit `expand_concepts` on a semantic
                        # call instead of letting `search()` refuse it.
                        expand_concepts=expand_concepts,
                        path_prefix=path_prefix,
                        continuation_token=continuation_token,
                        # Unconditional for the same reason.
                        completeness=completeness,
                        where=where,
                        record_scope=(
                            {"records": record_ids}
                            if record_ids is not None
                            else children_scope.applied()
                            if children_scope is not None else None
                        ),
                        **({"delivery_view": "list"} if view == "list" else {}),
                        **(
                            {"embedder_source": "server_process"}
                            if method == "semantic"
                            else {}
                        ),
                        **({"within": within} if within is not None else {}),
                    )
                except SubstrateError as exc:
                    # A refusal an agent cannot parse is read as "the graph has
                    # nothing" -- the exact wrong conclusion. The sibling
                    # missing-embedder case above already answers with a typed
                    # operator refusal carrying `fallback_method`; the corpus
                    # refusals introduced by `reachable-semantic-search` must
                    # match that shape rather than escaping as a raw exception
                    # from this tool body.
                    refusal = _semantic_corpus_refusal(exc, method=method)
                    if refusal is None:
                        raise
                    return refusal
                if window_call is not None:
                    # Already fitted to the served bytes; no preview cut.
                    return _stamp_operator(
                        _maybe_stamp_advanced(h, _with_lowered(payload)),
                        "search",
                    )
                produced = payload.pop("_reference_members", None)
                if retained is not None and "members" not in retained:
                    if produced is None:
                        raise _result_references.ResultReferenceInternal(
                            "search: a retained read returned no member list",
                            data={"reason": "contract"},
                            fix_hint="report this server bug",
                        )
                    members = produced["members"]
                    retained["members"] = members
                    retained["passages_excluded"] = int(produced["passages_excluded"])
                    retained["binding"] = {
                        **_reference_binding(h.conn, h.session),
                        "attributes": _member_attribute_digest(
                            h.conn, h.session, [m["node_id"] for m in members],
                        ),
                        "selection": retained["selection"],
                        "kind": "nodes",
                    }
                    memo = _result_references.call_memo()
                    if memo is not None:
                        memo[memo_key] = retained
            if (
                method != "identifier_mentions" and where is not None
                and isinstance(payload, dict) and payload.get("results") == []
            ):
                # #3666: an empty page under a `source` condition names the
                # source ids holding its text (`source_hint`); under a text
                # `contains` on an attribute it says what text never matches.
                from okg.substrate.mcp.operator_sql import text_contains_notes
                from okg.substrate.mcp.source_hint import source_conditions, source_hint

                hint = source_hint(
                    source_conditions(where),
                    source_names if source_names is not None else (),
                )
                if hint is not None:
                    payload["source_hint"] = hint
                text_notes = text_contains_notes(where, None, empty=True)
                if text_notes:
                    payload["contains_text"] = text_notes
            if (
                _effective_method == "grep" and isinstance(payload, dict)
                and view not in ("metadata", "windows")
            ):
                # Format v10 item C (#3666): a cut grep page whose query holds
                # a regex names the aggregate call that counts each distinct
                # match over the same records.
                _attach_count_each_match(
                    h, payload, query=query, where=where, path_prefix=path_prefix,
                    record_scoped=record_ids is not None or children_scope is not None,
                )
            if (
                isinstance(payload, dict) and isinstance(payload.get("results"), list)
                and _effective_method in ("grep", "lexical", "attrs")
                and view not in ("metadata", "windows")
                and not offset and continuation_token is None
                and record_ids is None and children_scope is None
            ):
                # Lane ri (CompOps q020): a read of file names names the grep
                # over file texts, with their count.
                _attach_names_only(
                    h, payload, predicate=where, query=query,
                    method=_effective_method,
                    subtype_filter=subtype_filter or (filter or {}).get("subtype"),
                    path_prefix=path_prefix,
                )
            if lineage_requested:
                payload = _move_lineage_to_rows(
                    payload, drop_attrs=lineage_drop_attrs,
                )
            if preview_as_default:
                # Still gated on `select is None`, DELIBERATELY. The documented
                # escape hatch for full attribute text is "pass select=[...]
                # naming the fields you need in full"; running the preview
                # budget over an explicit `select` that names `attrs` would
                # shorten exactly the text the caller just asked for in full,
                # and would break that contract for every existing caller.
                # `max_chars` is the control for callers who want a different
                # bound without giving up the default projection.
                payload = _preview_truncate_results(
                    payload,
                    max_chars=max_chars,
                )
            if view == "list" and _effective_method == "attrs":
                from okg.substrate.mcp.search_views import attrs_list_projection

                payload = attrs_list_projection(payload, selected=select)
            elif view == "list":
                payload = (
                    _grep_list_projection(payload) if _effective_method == "grep"
                    else _list_projection(payload)
                )
                if view_implied:
                    # The call named no view: the page says the list is
                    # grep's default and how to get the matched lines.
                    payload["view_implied"] = True
            if reference_wanted and retained is not None:
                payload = _retain_search_reference(
                    h, payload, retained, authority, metadata=metadata_view,
                )
            if metadata_view and retained is not None:
                payload = _metadata_search_page(
                    h, payload, retained, summary=summary, max_bytes=max_bytes,
                    generation_id=generation_id,
                )
            return _stamp_operator(
                _maybe_stamp_advanced(h, _with_lowered(payload)),
                "search",
            )

    def _attach_count_each_match(
        h: Any,
        payload: dict[str, Any],
        *,
        query: Any,
        where: Any,
        path_prefix: Optional[str],
        record_scoped: bool,
    ) -> None:
        """Add `count_each_match` to a grep page (Format v10 item C, #3666):
        only when its query holds a regex (`re:/.../`) and its rows are cut.

        The call is built from the caller's own `where` and regex over the
        record types the grep judged (`match_count_hint`): search's
        `title`/`path` are read through the attribute each type declares,
        and a part aggregate cannot take is named on the line with which way
        the count moves. When none of the caller's narrowing carries, no
        call is offered: it would count a different set."""
        from okg.substrate.mcp import match_count_hint as hint
        from okg.substrate.mcp.result_text import _cut

        if not hint.has_regex_term(query) or not _cut(payload):
            return
        if not hint.count_call_offerable(mcp):
            # Under access rules (or a mode that cannot be read) aggregate
            # refuses every count and the text condition is refused: a call
            # the deployment refuses is never named.
            return
        with h.conn.cursor() as cur:
            cur.execute(
                "SELECT name, metadata FROM okg.subtypes WHERE catalog_version_id = %s",
                (h.session.catalog_version_id,),
            )
            metadata = {str(name): meta or {} for name, meta in cur.fetchall()}
        scoped = payload.get("scoped_subtypes")
        call = hint.count_each_match_call(
            str(query), where, metadata=metadata,
            scoped=[str(s) for s in scoped] if isinstance(scoped, list) and scoped else None,
            path_prefix=path_prefix, record_scoped=record_scoped,
        )
        if call is not None:
            payload[hint.COUNT_EACH_MATCH_KEY] = call

    def _attach_names_only(
        h: Any,
        payload: dict[str, Any],
        *,
        predicate: Any,
        query: Any,
        method: str,
        subtype_filter: Any = None,
        path_prefix: Optional[str] = None,
    ) -> None:
        """Add `names_only` to a page that picked files by name (lane ri,
        CompOps q020; `name_only_hint`): the grep over `document` records
        for the same term, counted by running that call once (limit 1)
        under `COUNT_BUDGET_S` in a savepoint on this read's handle, so the
        count is the named call's own `total_count` at the same pin and
        under the same access rules. A named call the server refuses
        (profile, grep grammar) names nothing."""
        import time

        from okg.substrate.mcp import name_only_hint as names
        from okg.substrate.mcp.grep_search import GrepTimeout, deadline

        found = names.plan(
            predicate, query=query, method=method,
            subtype_filter=subtype_filter, path_prefix=path_prefix,
        )
        if found is None:
            return
        total: Optional[int] = None
        basis: Optional[str] = None
        try:
            with h.conn.transaction(), deadline(time.monotonic() + names.COUNT_BUDGET_S):
                scope = _resolve_mcp_search_scope(
                    h.conn, h.session, deployment_name=deployment_name,
                    requested_profile=None, subtype_filter=names.CONTENT_SUBTYPE,
                    method="grep",
                )
                counted = _search(
                    h.conn, h.session, found.query, method="grep", filter={},
                    limit=1, offset=0, select=["node_id"], scope=scope,
                    rank_by=_validate_rank_by(None, method="grep"),
                    max_chars=_clamp_preview_chars(None)[0],
                    path_prefix=found.path_prefix, where=found.canonical_where(),
                    exact_pass=False,
                )
        except GrepTimeout:
            pass
        except (SubstrateError, ValueError):
            return
        else:
            total = counted.get("total_count")
            basis = counted.get("total_count_basis")
        payload[names.NAMES_ONLY_KEY] = names.hint_block(found, total=total, basis=basis)

    def _metadata_search_page(
        h: Any,
        payload: dict[str, Any],
        retained: dict[str, Any],
        *,
        summary: Optional[list[str]],
        max_bytes: Any,
        generation_id: Any,
    ) -> dict[str, Any]:
        """A retained page delivered as `view="metadata"` (#3222 slice 3).

        No row and nothing built from rows (`search_views.metadata_projection`);
        the reference, its exact selected size and the retrieval facts stay.
        `summary` counts groups over the draft members with the
        reference-aggregate function `aggregate(input={"ref": id})` uses
        (`_reference_groups`), so the two agree, refusals included. The
        groups are counted once per call and kept for its byte-fitting
        reruns. When the resolved byte budget cannot hold every kept group,
        the smallest are cut and the cut is named (`fit_summary`), because a
        rerun at a smaller limit would change the reference.
        """
        out = _metadata_projection(payload)
        if not summary:
            return out
        reference_id = str(out["result_reference"]["id"])
        by = [str(field) for field in summary]
        key = "summary:" + _json.dumps(by)
        cached = retained.get(key)
        if cached is None:
            ids = [str(member["node_id"]) for member in retained["members"]]
            cached, _ = _reference_groups(h.conn, h.session, by=by, ids=ids)
            retained[key] = cached
        out["summary"] = _result_references.summary_block(
            cached, by=by, reference_id=reference_id,
        )
        if max_bytes is None:
            return out
        from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes

        def served_bytes(page: dict[str, Any]) -> int:
            return public_call_tool_result_bytes(
                _logical_response_measurement(
                    _stamp_operator(_maybe_stamp_advanced(h, dict(page)), "search"),
                    {"generation_id": generation_id, "select": None},
                ),
                tool="search",
            )

        return _result_references.fit_summary(
            out, budget=int(max_bytes) - adjusted_reserve_bytes(), measure=served_bytes,
            reference_id=reference_id,
        )

    def _retain_search_reference(
        h: Any,
        payload: dict[str, Any],
        retained: dict[str, Any],
        authority: Any,
        *,
        metadata: bool = False,
    ) -> dict[str, Any]:
        """Draft the retained search's reference and say how to page it.

        The members were frozen at the requested limit; a byte-fitted page
        delivers a prefix of them. `next` pages the reference, not search
        (H1): `next_offset` is null and, when this page did not deliver every
        member, `next_offset_reason` is `page_reference` and
        `result_reference.next_offset` is the members delivered, so the rest
        is `map(input={"ref": id}, offset=<that>)`.

        Search's own continuation is never dropped: when search had more
        hits past this page (its `next_offset`) or an exact-order reason and
        remedy, they are kept under `search_next`, so a reference that holds
        every member cannot read as the whole search. A page row the records
        fold left as a passage is not a member; their count is
        `result_reference.passages_excluded` (only when non-zero). The
        selection's exact size is its member count, never a recall claim.

        A `metadata` page delivers no row, so nothing is left to page: no
        `page_reference`, no `search_next`, and its text ends `next: none`
        (review L7).
        """
        members = retained["members"]
        ref_id = _result_references.draft(
            reader_key=_continuation.reader_key(
                authority, h.request_header_claim, multi_user_principals,
            ),
            binding=retained["binding"],
            members=members,
            header_claim=h.request_header_claim,
            multi_user=multi_user_principals,
        )
        out = dict(payload)
        reference = _reference_payload(ref_id, parent=None)
        excluded = int(retained.get("passages_excluded") or 0)
        if excluded:
            reference["passages_excluded"] = excluded
        search_next = {
            "offset": out.get("next_offset"),
            "reason": out.get("next_offset_reason"),
            "remedy": out.get("next_offset_remedy"),
        }
        if any(value is not None for value in search_next.values()) and not metadata:
            out["search_next"] = search_next
        out["next_offset"] = None
        out.pop("next_offset_remedy", None)
        out.pop("next_offset_reason", None)
        member_ids = {str(member["node_id"]) for member in members}
        # Delivered rows are a prefix of the page, so their members are a
        # prefix of the members.
        delivered = sum(
            1 for row in (out.get("results") or [])
            if isinstance(row, dict) and str(row.get("node_id")).strip() in member_ids
        )
        if delivered < len(members) and not metadata:
            out["next_offset_reason"] = "page_reference"
            reference["next_offset"] = delivered
        out["result_reference"] = reference
        coverage = dict(out.get("coverage") or {})
        coverage["selection"] = {
            "unit": "record",
            "basis": "retrieved_candidates",
            "size": len(members),
            "limit": int(retained["limit"]),
        }
        out["coverage"] = coverage
        return out

    # ---------- run (issue #3666, M9) ----------

    def _run_placeholders(sql: str) -> list[str]:
        """The `%(name)s` placeholders of `sql`, in order, as psycopg reads
        them: an escaped `%%` is a literal percent, so text inlined from an
        earlier set (a stored value holding `%(x)s`) is never taken for one."""
        names: dict[str, None] = {}
        i = 0
        while i < len(sql):
            if sql[i] != "%":
                i += 1
                continue
            following = sql[i + 1: i + 2]
            if following == "%":
                i += 2
                continue
            if following == "(":
                end = sql.find(")s", i + 2)
                if end < 0:
                    raise ValueError("a placeholder in composed run SQL is not closed")
                names[sql[i + 2:end]] = None
                i = end + 2
                continue
            raise ValueError("composed run SQL holds a bare percent sign")
        return list(names)

    def _run_literal_sql(conn: psycopg.Connection, sql: str, params: Mapping[str, Any]) -> str:
        """`sql` with its parameters bound into the text, so it runs as
        written and nests in another statement as a subquery."""
        return psycopg.ClientCursor(conn).mogrify(
            sql, {name: params[name] for name in _run_placeholders(sql)},
        )

    def _run_page_meta(
        conn: psycopg.Connection, session: Session, kind: str, predicate: Any,
    ) -> dict[str, Any]:
        """What a filter page over `predicate` reads from, as `filter` reads
        it (date conditions included); one row is fetched."""
        budget = _date_budget("filter", predicate, kind=kind)

        def read(read_with: Any) -> dict[str, Any]:
            meta: dict[str, Any] = {}
            _effective_view_sql_page(
                conn, session, kind=kind, predicate=read_with,
                fingerprint_predicate=predicate, date_budget=budget,
                limit=1, offset=0, nomos_runtime=False, page_meta=meta,
            )
            return meta

        _read_with, meta = _dated_read(
            conn, session, kind=kind, predicate=predicate, operator="filter",
            report=_DateConditionReport(), nomos_runtime=False, budget=budget,
            run=read,
        )
        return meta

    class _RunBackend:
        """`run_tool.RunBackend` over this server's pooled reads."""

        def refused_reason(self) -> Optional[str]:
            if deployment_requires_nomos_runtime(deployment_name):
                return (
                    "run is refused on this deployment because its access "
                    "rules (Nomos audit or enforce) are on"
                )
            return None

        def step_set(
            self, definition: tuple[Any, ...], sets: Any, key: str,
            generation: Any, authority: Any,
        ) -> str:
            with _open(generation, authority=authority) as h:
                _require_effective_view(h.conn, h.session, "node")
                if definition[0] == "records":
                    predicate = definition[2]
                else:
                    _tag, node_id, edge_type, direction, far_filter = definition
                    _require_effective_view(h.conn, h.session, "edge")
                    near, far = ("src", "dst") if direction == "out" else ("dst", "src")
                    clauses: list[dict[str, Any]] = [
                        {"op": "eq", "field": near, "value": node_id},
                    ]
                    if edge_type:
                        clauses.append({"op": "eq", "field": "edge_type", "value": edge_type})
                    edges = _run_page_meta(
                        h.conn, h.session, "edges", {"op": "and", "clauses": clauses},
                    )
                    far_key = f"far{key}"
                    sets.sets[far_key] = _run_steps.StepSet(raw_sql=_run_literal_sql(
                        h.conn, f"SELECT effective_page.{far} " + edges["from_where_sql"],
                        edges["params"],
                    ))
                    far_leaf = {
                        "op": "in", "field": "node_id",
                        "values": [sets.set_marker(far_key)],
                    }
                    predicate = (
                        {"op": "and", "clauses": [far_leaf, far_filter]}
                        if far_filter else far_leaf
                    )
                meta = _run_page_meta(h.conn, h.session, "nodes", predicate)
                return _run_literal_sql(
                    h.conn, "SELECT effective_page.target_id " + meta["from_where_sql"],
                    meta["params"],
                )

        def current_generation(self, authority: Any) -> Any:
            with _open(None, authority=authority) as h:
                return h.session.generation_id

        def ids_set(self, ids: list[str], generation: Any, authority: Any) -> str:
            with _open(generation, authority=authority) as h:
                return _run_literal_sql(
                    h.conn, "SELECT unnest(%(ids)s::text[])", {"ids": list(ids)},
                )

        def values_sql(
            self, sets: Any, key: str, field: str, generation: Any, authority: Any,
        ) -> str:
            column = _run_steps.value_column(field)
            with _open(generation, authority=authority) as h:
                meta = _run_page_meta(h.conn, h.session, "nodes", {
                    "op": "in", "field": "node_id", "values": [sets.set_marker(key)],
                })
                params = dict(meta["params"])
                if column is None:
                    params["run_value_key"] = field.split(".", 1)[1]
                    expr = "effective_page.attrs -> %(run_value_key)s"
                else:
                    expr = f"to_jsonb(effective_page.{column})"
                return _run_literal_sql(
                    h.conn, f"SELECT DISTINCT {expr} " + meta["from_where_sql"], params,
                )

        def materialize(
            self, raw_sql: str, generation: Any, authority: Any, cap: int,
        ) -> Optional[list[Any]]:
            with _open(generation, authority=authority) as h:
                with h.conn.cursor() as cur:
                    cur.execute(
                        f"SELECT run_set.v FROM ({raw_sql}) AS run_set(v) "
                        f"LIMIT {int(cap) + 1}"
                    )
                    rows = [row[0] for row in cur.fetchall()]
            return None if len(rows) > cap else rows

        def _links_sql(self, h: Any, edge_type: str, direction: str) -> str:
            """The `edge_type` links as `(near, far)` id pairs, as SQL with
            its parameters bound in: near is the end a join starts from."""
            _require_effective_view(h.conn, h.session, "edge")
            near, far = ("src", "dst") if direction == "out" else ("dst", "src")
            edges = _run_page_meta(
                h.conn, h.session, "edges",
                {"op": "eq", "field": "edge_type", "value": edge_type},
            )
            return _run_literal_sql(
                h.conn,
                f"SELECT effective_page.{near} AS near, effective_page.{far} AS far "
                + edges["from_where_sql"],
                edges["params"],
            )

        def join_far(
            self, start_sql: str, edge_type: str, direction: str,
            generation: Any, authority: Any,
        ) -> str:
            """The far ends of `edge_type` links from every record of a set."""
            with _open(generation, authority=authority) as h:
                links = self._links_sql(h, edge_type, direction)
            return (
                f"SELECT run_links.far FROM ({links}) AS run_links "
                f"WHERE run_links.near IN ({start_sql})"
            )

        def join_facts(
            self, start_sql: str, edge_type: str, direction: str, ids_shown: int,
            generation: Any, authority: Any,
        ) -> tuple[int, int, list[str]]:
            """(records in the set, of them with no such link, up to
            `ids_shown` of those ids in id order): one read, exact."""
            with _open(generation, authority=authority) as h:
                links = self._links_sql(h, edge_type, direction)
                lacking = (
                    f"SELECT run_start.id FROM (SELECT DISTINCT s.id FROM "
                    f"({start_sql}) AS s(id)) AS run_start WHERE NOT EXISTS ("
                    f"SELECT 1 FROM ({links}) AS run_links "
                    "WHERE run_links.near = run_start.id)"
                )
                with h.conn.cursor() as cur:
                    cur.execute(
                        f"SELECT (SELECT count(DISTINCT s.id) FROM ({start_sql}) AS s(id)), "
                        f"(SELECT count(*) FROM ({lacking}) AS l), "
                        f"ARRAY(SELECT l.id FROM ({lacking}) AS l ORDER BY l.id "
                        f"LIMIT {int(ids_shown)})"
                    )
                    starts, without, ids = cur.fetchone()
            return int(starts), int(without), [str(i) for i in (ids or [])]

        def count(self, raw_sql: str, generation: Any, authority: Any) -> int:
            """One COUNT over a step's set, under the step's budget (the
            pooled read opens with what is left of it as its timeout)."""
            with _open(generation, authority=authority) as h:
                with h.conn.cursor() as cur:
                    cur.execute(f"SELECT count(*) FROM ({raw_sql}) AS run_count")
                    return int(cur.fetchone()[0])

        def row_matches(self, row: dict[str, Any], predicate: Any) -> bool:
            """A held row against a condition on `attrs.<column>` (#3666,
            lane ri), exactly as a kept live answer's rows are filtered."""
            return bool(_operator_predicate_matches({"attrs": row}, predicate, declared=None))

    _run_backend = _RunBackend()
    # Reached through the server so a test can watch every composed read.
    mcp._okg_run_backend = _run_backend  # type: ignore[attr-defined]

    def _run_handler(server: Any, arguments: dict[str, Any]) -> Any:
        from okg.substrate.mcp.run_tool import serve_run

        return serve_run(server, arguments, server._okg_run_backend)

    mcp._okg_run_handler = _run_handler  # type: ignore[attr-defined]

    def _sets_hook(server: Any, name: str, arguments: Any, plain: Any) -> Any:
        # No set outlives its run call (#3666): `$r<n>`, `retain=true` set
        # names and a `$n` outside its run are refused before any SQL.
        from okg.substrate.mcp.run_tool import guard_call

        return guard_call(server, name, arguments, plain, server._okg_run_backend)

    mcp._okg_sets_hook = _sets_hook  # type: ignore[attr-defined]

    def run(
        steps: list,
        generation_id: Optional[Union[int, str]] = None,
        max_bytes: MaxBytes = None,
        ctx: Context | None = None,
    ) -> dict[str, Any]:
        """Up to 8 reads in one call, each over its whole set: steps=[{"<tool>":{<arguments>}}]; in a later step "$n" is step n's records, "$n.<field>" their values.[[if run_live tool:expand]] A live step calls a relation once per value of a column: {"expand":{"input":"<id>","live":{"relation":"<name>","each":{"<arg>":"$1.<column>"}}}}; {"aggregate":{"input":"$2","by":[],"metrics":["count","sum(<column>)"]}} totals its rows; {"minus":["$1","$2"],"on":"<column>"} (or intersect) compares two steps' rows.[[/if]]"""
        raise RuntimeError("run is served by the MCP server's call_tool, not called directly")

    register_core_read_tools(
        mcp,
        traced=traced,
        run_tool=run,
        inspect_tool=inspect,
        search_tool=search,
        expand_tool=expand,
        filter_tool=filter,
        map_tool=map,
        aggregate_tool=aggregate,
        query_tool=query,
        response_envelope=_logical_response_envelope,
        response_measurement=_logical_response_measurement,
        # The EFFECTIVE values, not the raw declarations. A manifest that
        # declares only a ceiling leaves the default unset, and the 64 KiB
        # substrate default would then be refused for exceeding a 32 KiB
        # ceiling on every call that omitted max_bytes. `effective_*` lowers
        # the default to the ceiling in that case. `resolve_budget` applies
        # the default whenever `max_bytes` is omitted or null (issue #2156).
        deployment_default_bytes=(
            agent_surface.effective_response_default
            if agent_surface is not None else None
        ),
        deployment_ceiling_bytes=(
            agent_surface.effective_response_ceiling
            if agent_surface is not None else None
        ),
        # `expand(live=...)`: the wrapper runs the graph step, then this
        # prepared live call exactly once (issue #2141).
        expand_live_step=lambda arguments: _prepare_live_for_expand(
            mcp,
            arguments["live"],
            node_id=_node_id_from_input(as_mapping(arguments.get("input"))),
            generation_id=arguments.get("generation_id"),
            authority=arguments.get("authority"),
        ),
        # `expand(next=<live token>)`: the next page of a cut live answer,
        # from its kept set, with no adapter call (#3666, Live build 3).
        expand_live_page=_expand_live_page,
    )

    # `search_by_attrs` retired by `retire-redundant-mcp-tools`
    # (2026-04-27). Replacement: `mcp-views.md` recipe
    # `SELECT * FROM okg.v_nodes WHERE subtype = ? AND attrs @> ?`.
    # The partial GIN on jsonb_path_ops makes this indexed at the
    # latest pin. Sort/offset/limit happen in the agent's SQL.

    # Legacy compat aliases run_sql, advance_session,
    # find_identifier_mentions, preview_advance, parse_document,
    # extract_entities, chunk_document, suggest_aliases, and
    # register_alias were deleted by ``shrink-mcp-to-operator-surface``.
    # Their behaviors live behind the canonical operators:
    #   run_sql                  → query(sql=...)
    #   advance_session          → deliberate logical-session refresh via
    #                              inspect(kind='session', action='refresh')
    #   find_identifier_mentions → search(method='identifier_mentions')
    #   preview_advance          → inspect(target={kind:'generation_delta'})
    #   parse_document           → Python: parser_for(format).parse(...)
    #   extract_entities         → Python: extractor_bundle.ner.extract(...)
    #   chunk_document           → Python: WindowChunker / extractor_bundle.chunker
    #   suggest_aliases          → inspect(target={kind:'alias_suggestions'})
    #   register_alias           → future propose(action='alias') or
    #                              admin CLI `okg alias add`
    # The underlying Python helpers (_run_sql, _advance_session,
    # _find_identifier_mentions, _preview_advance, etc.) are retained
    # because inspect/expand/aggregate/search/query dispatch and other
    # internal substrate code still call them.

    # ------------------------------------------------------------------
    # Module-gated helpers - `generate_context_packet` (Layer 3B of
    # `add-portable-memex-deployments`).
    #
    # The Engram/Glia-shaped Research Context Packet helper is
    # available in-process iff the deployment loads the `reasoning`
    # module. It is intentionally outside this MCP registry until a
    # future reviewed program surface admits generated program tools,
    # so build_server catalogs it for diagnostics without decorating it
    # as an MCP tool.
    # ------------------------------------------------------------------
    from okg.substrate.deployments import deployment_loads_module

    reasoning_loaded = deployment_loads_module(
        deployment_name,
        "reasoning",
    )
    mcp._okg_reasoning_loaded = reasoning_loaded  # type: ignore[attr-defined]
    mcp._okg_skipped_module_tools = ()  # type: ignore[attr-defined]
    if reasoning_loaded:
        from okg.substrate.library.ontologies.reasoning.derived_artifacts import (
            load_context_packet_definition,
        )

        _context_packet_definition = load_context_packet_definition()
        mcp._okg_context_packet_definition = (  # type: ignore[attr-defined]
            _context_packet_definition
        )
        catalog_skipped_module_tool(
            mcp,
            name="generate_context_packet",
            module="reasoning",
            boundary="generated_program_internal",
            reason=(
                "reasoning context packets require a separately reviewed "
                "generated-program MCP surface"
            ),
        )

    # Source-admin tools (agent-led-mcp-onboarding Phase 4).
    from okg.substrate.mcp import source_admin as _source_admin

    def list_sources_tool(
        deployment: Optional[str] = None,
    ) -> dict[str, Any]:
        """Enumerate sources in the deployment's
        `source_registry.yaml` with per-source status,
        last_ingest_at, and node/edge counts.

        `deployment` defaults to the server's deployment_name (the
        one pinned at startup).
        """
        target = deployment or deployment_name
        with _pool_connection(pool) as _conn:
            return _source_admin.list_sources(_conn, target)

    def add_source_tool(
        short_name: str,
        deployment: Optional[str] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Low-level recovery hook for one source.[[if tool:propose_setup tool:commit_setup]] Normal setup uses
        `propose_setup` and `commit_setup`.[[/if]] Credentialed connectors
        return `needs_connection` instead of per-source credential
        instructions.
        """
        refusal = _write_refusal("add_source")
        if refusal is not None:
            return refusal
        target = deployment or deployment_name
        with _pool_connection(write_pool) as _conn:
            return _source_admin.add_source(
                _conn,
                deployment=target,
                short_name=short_name,
                params=params or {},
            )

    def remove_source_tool(
        short_name: str,
        deployment: Optional[str] = None,
    ) -> dict[str, Any]:
        """Drop one source from the deployment registry.
        Existing nodes from this source remain in the graph."""
        refusal = _write_refusal("remove_source")
        if refusal is not None:
            return refusal
        target = deployment or deployment_name
        with _pool_connection(write_pool) as _conn:
            return _source_admin.remove_source(
                _conn,
                deployment=target,
                short_name=short_name,
            )

    def source_status_tool(
        short_name: str,
        deployment: Optional[str] = None,
    ) -> dict[str, Any]:
        """Inspect one source: connector/auth state, last ingest watermark,
        node + edge counts, registry entry. Used by the setup
        skill to inspect connection readiness and post-add ingest
        progress."""
        target = deployment or deployment_name
        with _pool_connection(pool) as _conn:
            return _source_admin.source_status(
                _conn,
                deployment=target,
                short_name=short_name,
            )

    # Intent-and-approval setup flow
    # (improve-add-source-operator-experience):
    # discover_sources → propose_setup → commit_setup.

    def discover_sources_tool(
        deployment: Optional[str] = None,
    ) -> dict[str, Any]:
        """Walk the operator's filesystem + env + install
        answers and return per-source candidates the agent
        can wire without asking the operator for paths.

        Read-only. Idempotent against an unchanged filesystem
        snapshot."""
        target = deployment or deployment_name
        with _pool_connection(pool) as _conn:
            return _source_admin.discover_sources(
                _conn,
                deployment=target,
            )

    def propose_setup_tool(
        deployment: Optional[str] = None,
        include_threshold: str = "high",
        prefer_paths: Optional[list[str]] = None,
        exclude_paths: Optional[list[str]] = None,
        only: Optional[list[str]] = None,
        project_paths: Optional[list[str]] = None,
        pi_name: Optional[str] = None,
        orcid: Optional[str] = None,
        dblp_pid: Optional[str] = None,
    ) -> dict[str, Any]:
        """Bundle discovered candidates into a single approval-
        shape plan. Returns a `plan_id`[[if tool:commit_setup]] the agent passes to
        `commit_setup` after the operator says "yes."[[else]].[[/if]]

        Narrowing kwargs:
          - `include_threshold`: confidence cutoff (high /
            medium / low) for `would_wire`. Default `high`.
          - `prefer_paths`: substring matches to prefer when
            multiple candidates exist per source.
          - `exclude_paths`: substring exclusions.
          - `only`: narrow to these source_ids / short names.
          - `project_paths`: explicit operator-selected repo paths
            to consider for git/code/OpenSpec/session sources.
          - `pi_name` / `orcid` / `dblp_pid`: identity answers the
            operator gave conversationally.
        """
        target = deployment or deployment_name
        with _pool_connection(pool) as _conn:
            return _source_admin.propose_setup(
                _conn,
                deployment=target,
                include_threshold=include_threshold,
                prefer_paths=prefer_paths,
                exclude_paths=exclude_paths,
                only=only,
                project_paths=project_paths,
                pi_name=pi_name,
                orcid=orcid,
                dblp_pid=dblp_pid,
            )

    def commit_setup_tool(
        plan_id: str,
    ) -> dict[str, Any]:
        """Execute a previously-proposed plan. Calls
        [[if tool:add_source]]`add_source`[[else]]the source add[[/if]] for each entry; aggregates results."""
        refusal = _write_refusal("commit_setup")
        if refusal is not None:
            return refusal
        with _pool_connection(write_pool) as _conn:
            return _source_admin.commit_setup(_conn, plan_id=plan_id)

    def connector_route_status_tool(
        source: str,
        deployment: Optional[str] = None,
    ) -> dict[str, Any]:
        """Report connector readiness for a credentialed source."""
        target = deployment or deployment_name
        with _pool_connection(pool) as _conn:
            return _source_admin.connector_route_status(
                _conn,
                deployment=target,
                source=source,
            )

    def connector_ingest_batch_tool(
        source: str,
        route_id: str,
        records: list[dict[str, Any]],
        provenance: Optional[dict[str, Any]] = None,
        deployment: Optional[str] = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Import normalized records fetched through an agent host
        connector. Dry-run validates and reports fact counts without
        writing."""
        target = deployment or deployment_name
        # dry_run validates only (read); a real import writes facts +
        # publishes, so it needs operator-enabled write mode.
        if not dry_run:
            refusal = _write_refusal("connector_ingest_batch")
            if refusal is not None:
                return refusal
            ingest_pool = write_pool
        else:
            ingest_pool = pool
        with _pool_connection(ingest_pool) as _conn:
            return _source_admin.connector_ingest_batch(
                _conn,
                deployment=target,
                source=source,
                route_id=route_id,
                records=records,
                provenance=provenance or {},
                dry_run=bool(dry_run),
            )

    # Conditional admin surface (PACT chat-upload-source). `upload` is
    # registered only where the deployment registers a Nomos-classified
    # upload source; elsewhere the tool is ABSENT from tools/list rather
    # than present-and-refusing, so a chat client on an unprovisioned
    # deployment never offers an upload affordance that cannot work.
    from okg.substrate.mcp.upload_admin import (
        CONDITIONAL_TOOL_UPLOAD,
        upload_to_staging,
    )

    # `_conditional` was resolved above, before the instructions were
    # rendered, because the served tool set depends on it. It is resolved
    # regardless of `allow_writes`, so the readback distinguishes "this
    # deployment provisions no upload source" from "this server was launched
    # read-only". Gating the resolution on allow_writes would report a fully
    # registered deployment as unprovisioned on a read-only server — a false
    # statement about the deployment.
    mcp._okg_conditional_provisioning_reasons = {  # type: ignore[attr-defined]
        name: dict(reason) for name, reason in _conditional["reasons"].items()
    }

    def upload_tool(
        uploaded_by: str,
        filename: Optional[str] = None,
        content_base64: Optional[str] = None,
        url: Optional[str] = None,
        deployment: Optional[str] = None,
    ) -> dict[str, Any]:
        """Stage an operator file or URL fetch into this deployment's
        registered upload source, so it becomes governed graph content on
        the next ingest and publish cycle.

        Supply either `filename` + `content_base64`, or `url`. A URL is
        fetched through the substrate egress guard: private, loopback,
        link-local, and cloud-metadata addresses are refused, and every
        redirect hop is revalidated.

        There is no chat-side copy and no separate retrieval store: the
        upload's only destinations are this source's staging area and,
        after ingest, the graph.

        INTERIM POSTURE — read before trusting the attribution:
        `uploaded_by` is caller-asserted. This server authenticates the
        CALLING APPLICATION with a bearer token (mcp-http-auth), not the
        individual person, so on a single-token deployment every chat
        user is one principal. Per-user identity arrives with
        `mcp-principal-mapping`; until then `uploaded_by` is a label the
        caller supplied, not a verified identity, and the staging sidecar
        records it as such. On an unauthenticated non-loopback bind the
        whole write/admin surface — this tool included — refuses at
        startup rather than serving.
        """
        refusal = _write_refusal(CONDITIONAL_TOOL_UPLOAD)
        if refusal is not None:
            return refusal
        target = deployment or deployment_name
        return upload_to_staging(
            target,
            uploaded_by=uploaded_by,
            filename=filename,
            content_base64=content_base64,
            url=url,
        )

    if allow_writes:
        register_conditional_admin_tools(
            mcp,
            traced=traced,
            upload_tool=(
                upload_tool
                if CONDITIONAL_TOOL_UPLOAD in _provisioned_conditional
                else None
            ),
        )
        register_source_admin_tools(
            mcp,
            traced=traced,
            list_sources_tool=list_sources_tool,
            add_source_tool=add_source_tool,
            remove_source_tool=remove_source_tool,
            source_status_tool=source_status_tool,
            discover_sources_tool=discover_sources_tool,
            propose_setup_tool=propose_setup_tool,
            commit_setup_tool=commit_setup_tool,
            connector_route_status_tool=connector_route_status_tool,
            connector_ingest_batch_tool=connector_ingest_batch_tool,
        )

    from okg.substrate.mcp.deployment_tools import (
        catalog_deployment_tool_declarations,
    )

    mcp._okg_deployment_tools = []  # type: ignore[attr-defined]
    mcp._okg_skipped_deployment_tools = []  # type: ignore[attr-defined]
    # Deployment `agent_tools` are compatibility/program declarations,
    # not part of the admitted AKMON MCP boundary. Catalog them for
    # diagnostics without importing deployment modules or registering
    # arbitrary top-level tools.
    try:
        mcp._okg_skipped_deployment_tools = (  # type: ignore[attr-defined]
            catalog_deployment_tool_declarations(
                deployment_name=deployment_name,
            )
        )
    except Exception:  # noqa: BLE001 — server build failed mid-init; release the pools before propagating so we don't leak conns.
        pool.close()
        if write_pool is not None:
            write_pool.close()
        raise

    try:
        # The REGISTERED narrowing, which is the ceiling when there is one.
        # The raw profile set is passed, not the boundary-intersected one, so
        # `assert_narrowing_is_servable` can still tell a typo from a tool
        # this launch withholds.
        apply_admitted_tool_surface(
            mcp,
            allow_writes=allow_writes,
            # A DECLARATION is not a launch narrowing.
            #
            # `agent_surface.served` states the whole surface across every
            # launch mode (operator decision, 2026-09-20), so a read-only
            # launch of a deployment declaring the write tools serves
            # `served` INTERSECTED with what the launch admits, not `served`
            # itself. Passing it as an allowlist instead sent it through
            # `assert_narrowing_is_servable`, which correctly refuses a
            # narrowing naming an unregistered tool — and every read launch
            # of such a deployment died on the eleven write tools it had not
            # asked for.
            #
            # The bound is not lost: `agent_surface_served` below narrows the
            # BOUNDARY, so a later `--tools` is still checked against
            # what the declaration leaves and can never widen past it.
            tool_allowlist=(
                ceiling_surface.tools
                if ceiling_surface is not None
                else None
                if tool_surface.source == "agent_surface"
                else tool_surface.tools
            ),
            provisioned_conditional_tools=_provisioned_conditional,
            tool_profile=(
                ceiling_surface.profile
                if ceiling_surface is not None
                else tool_surface.profile
            ),
            tool_profile_source=(
                ceiling_surface.source
                if ceiling_surface is not None
                else tool_surface.source
            ),
            access_rule_withheld=(
                frozenset({"query"}) if query_off_mode is not None else frozenset()
            ),
            agent_surface_served=(
                frozenset(agent_surface.served)
                if agent_surface is not None else None
            ),
        )
        if ceiling_surface is not None and tool_surface.tools is not None:
            # The BASE narrowing is not what registration pruned to, so it
            # gets its own fail-closed check against what actually
            # registered. Without it a base profile naming a withheld or
            # misspelled tool would just start a session one tool smaller,
            # in silence.
            assert_narrowing_is_servable(
                # Less what access rules withhold (issue #3666): the
                # deployment's rule, not a misspelling.
                set(tool_surface.tools) - (
                    {"query"} if query_off_mode is not None else set()
                ),
                registered=set(mcp._tool_manager._tools),  # type: ignore[attr-defined]
                profile=tool_surface.profile,
                source=tool_surface.source,
            )
    except Exception:  # noqa: BLE001 — a narrowing refusal unwinds out of build_server; release the pools first so a refused launch leaks no connections.
        pool.close()
        if write_pool is not None:
            write_pool.close()
        raise

    # Post-condition over the FULL served set, not an assumption. The
    # instructions were rendered from `served_tools` predicted before
    # registration; this proves the server serves exactly that set.
    #
    # What it actually proves, stated precisely: on the NARROWED path the
    # subset relation was already established by
    # `assert_narrowing_is_servable`, so this adds the reverse containment —
    # that nothing admitted went missing. On the UNNARROWED path it is the
    # only check there is. Either way it catches a boundary or provisioning
    # prediction that disagrees with registration.
    #
    # DELIBERATE HARDENING: a non-empty `_okg_missing_admitted_tools` was
    # previously a diagnostic readback only; it now refuses the launch,
    # because the instructions already name the missing tool and a server
    # that tells a model to call an unregistered tool is the exact failure
    # this change exists to prevent. The readback is carried in `data` so the
    # underlying registration defect stays legible.
    actually_served = set(mcp._tool_manager._tools)  # type: ignore[attr-defined]
    if agent_surface is not None:
        # The shared function, asserted as a real post-condition (PACT
        # effective-surface-declaration). `okg chat sync` compares a live
        # tools/list against `registered_tools_for_launch`; if registration
        # does not actually produce that set, sync is checking against a
        # number nothing guarantees. The docstring claimed this held while
        # the only check here compared against `served_tools`, which is
        # derived separately and would agree by coincidence rather than by
        # construction.
        expected_registered = registered_tools_for_launch(
            agent_surface.served, allow_writes=allow_writes,
            deployment=deployment_name,
        )
        if ceiling_surface is None and actually_served != set(expected_registered):
            pool.close()
            if write_pool is not None:
                write_pool.close()
            raise InstructionSurfaceMismatch(
                f"deployment {deployment_name!r} declares an agent_surface "
                f"whose {'write/admin' if allow_writes else 'read-only'} "
                f"launch must register {sorted(expected_registered)}, but "
                f"this server registered {sorted(actually_served)}. "
                "Refusing: `okg chat sync` compares a live tools/list "
                "against the same function, so a registration that does not "
                "match it makes that check meaningless.",
                data={"deployment": deployment_name,
                      "allow_writes": allow_writes,
                      "expected": sorted(expected_registered),
                      "registered": sorted(actually_served)},
                fix_hint=(
                    "check agent_surface.served against the tools this "
                    "launch registers"
                ),
            )
    if actually_served != set(served_tools):
        pool.close()
        if write_pool is not None:
            write_pool.close()
        raise InstructionSurfaceMismatch(
            "MCP instructions were rendered for tools "
            f"{sorted(served_tools)} but the server registered "
            f"{sorted(actually_served)}. Refusing to serve instructions "
            "that describe a different surface than the one served.",
            data={
                "rendered_for": sorted(served_tools),
                "served": sorted(actually_served),
                "missing_admitted_tools": list(
                    getattr(mcp, "_okg_missing_admitted_tools", ())
                ),
            },
            fix_hint="report this as an okg substrate defect",
        )

    # Per-session projection (issue #1458). Installed AFTER the surface
    # post-condition so it can only ever project a registry that was already
    # proved to match the instructions, and only when a ceiling exists: a
    # launch without one keeps the SDK's own list_tools/call_tool path and
    # is told, truthfully, that the tool list never changes.
    session_tool_surface: SessionToolSurface | None = None
    if ceiling_surface is not None:
        session_tool_surface = SessionToolSurface(
            base=frozenset(base_tools),
            ceiling=frozenset(served_tools),
            base_profile=tool_surface.profile or tool_surface.source,
            ceiling_profile=ceiling_surface.profile,
        )
        try:
            install_session_tool_projection(mcp, session_tool_surface)
        except Exception:  # noqa: BLE001 — an un-advertisable expansion must not leak pools.
            pool.close()
            if write_pool is not None:
                write_pool.close()
            raise
    mcp._okg_session_tool_surface_registry = (  # type: ignore[attr-defined]
        session_tool_surface
    )
    mcp._okg_session_base_tools = tuple(base_tools)  # type: ignore[attr-defined]
    # The deployment whose access-rule mode decides whether an `aggregate`
    # call may be offered in a refusal (`_okg_aggregate_refused`).
    mcp._okg_read_deployment_name = deployment_name  # type: ignore[attr-defined]
    # Whether this database folds like exactly: asked before a refusal
    # offers a call with a like test (`_okg_like_served`).
    mcp._okg_like_database_problem = _like_database_problem  # type: ignore[attr-defined]

    if multi_user_principals:
        def _dispatch_principal_gate(tool_name: str) -> None:
            from okg.substrate.mcp.oidc_auth import application_may_list_tools

            if application_may_list_tools(principal_mapping, tool_name):
                # PACT mcp-oidc-auth: an application's token lists the admitted
                # surface and nothing else. There is no principal to resolve,
                # no binding to revoke, and no call this can authorize.
                return
            with read_pass_scope(dedup_identity_claims=True):
                with _pool_connection(pool) as conn:
                    try:
                        result = _resolve_principal_to_subject(
                            conn,
                            deployment_name=deployment_name,
                            principal_mapping=principal_mapping,
                            operator_name=tool_name,
                            surface="mcp.tool.call",
                            resource_kind="mcp_tool",
                            target_kind="mcp_tool_call",
                        )
                        if result.decision.effect != "allow":
                            raise _nomos_identity_denied("mcp.tool.call", result)
                    except SubstrateError as exc:
                        _persist_principal_refusal(
                            conn,
                            error=exc,
                            deployment_name=deployment_name,
                            surface="mcp.tool.call",
                            operator_name=tool_name,
                        )
                        raise

        mcp._okg_dispatch_principal_gate = _dispatch_principal_gate
        mcp._okg_deployment_name = deployment_name
        from okg.substrate.mcp.principal_session import install_dispatch_gate

        install_dispatch_gate(mcp, _dispatch_principal_gate)

    mcp._okg_instruction_pruned_tools = (  # type: ignore[attr-defined]
        deployment_instructions_pruned_tools
    )
    mcp._okg_instruction_source = instruction_source  # type: ignore[attr-defined]
    mcp._okg_unhonored_manifest_default = (  # type: ignore[attr-defined]
        tool_surface.unhonored_manifest_default
    )
    if deployment_instructions_pruned_tools:
        # Emitted, not merely recorded: an unread readback is not a warning.
        log.warning(
            "deployment %s serves instructions naming tool(s) %s that this "
            "launch does not serve (profile=%s, served=%s); the deployment's "
            "own mcp-instructions.md is served unchanged",
            deployment_name,
            ", ".join(deployment_instructions_pruned_tools),
            tool_surface.profile or tool_surface.source,
            ", ".join(served_tools),
        )
    if ceiling_surface is not None and instruction_source == "deployment_file":
        # The deployment's own instruction file is served verbatim apart from
        # the generated capability lines (#3151, decision 5b), which state
        # facts about this server and never rewrite the operator's text. But
        # that text was written without knowing this
        # launch can widen a session, so the served instructions do not name
        # the action. Said out loud rather than left for an agent to discover
        # from a refusal. `inspect(target={"kind":"surface"})` reports the
        # action either way.
        log.warning(
            "deployment %s serves its own mcp-instructions.md, so the served "
            "instructions do not describe the in-session expansion this "
            "launch allows (ceiling=%s, adds %s); callers learn it from "
            "inspect(target={'kind':'surface'}) or from a refusal",
            deployment_name,
            ceiling_surface.profile,
            ", ".join(sorted(set(served_tools) - set(base_tools))),
        )
    if tool_surface.unhonored_manifest_default:
        # See `resolve_tool_surface`: the manifest could not be resolved from
        # this checkout, so a declared default_tool_profile — if there is one
        # — cannot be read. Serving the full surface silently would be the
        # widening this module's contract forbids.
        log.warning(
            "deployment %s could not be resolved from this checkout, so any "
            "declared mcp.default_tool_profile could NOT be honored; serving "
            "the unnarrowed surface (%s tools). Pass --tools "
            "explicitly, or launch from the deployment's install root.",
            deployment_name,
            len(served_tools),
        )

    # Published input schemas carry ~2 KB of SDK-derived titles and null
    # branches across the read surface; every session pays for them at
    # startup and nothing reads them. Dropped here, after the last tool is
    # registered, so the narrowing applies to whatever surface this launch
    # actually serves. See `compact_tool_schemas` for what is NOT dropped.
    # Narrow what `tools/list` SHOWS for `inspect.target`, leaving the
    # validator on the full union so an unserved kind reaches the router
    # guard and is refused by name rather than as a schema mismatch. Before
    # compaction, so the published result is compacted like everything else.
    publish_narrowed_inspect_target(mcp, inspect_kind_set)
    # The same set reaches argument validation, so a kind that is in no
    # inspect union gets the guard's named refusal there instead of
    # `union_tag_invalid` (`_refuse_unknown_inspect_kind_by_name`).
    mcp._okg_inspect_served_kinds = inspect_kind_set
    # The same split for `search` (#3151 slice 1): the published `method`
    # enum, `select` items and `rank_by` list only what the probe found this
    # server can run; the guard at the top of `search` refuses the rest by
    # name. The selectable fields are code constants, recorded here so the
    # readback shows exactly what was published.
    from okg.substrate.mcp.grep_search import GREP_FIELDS as _GREP_FIELDS

    capabilities = capabilities.with_select_fields({
        "lexical": (*_LEXICAL_FIELDS, LINEAGE_SELECT_TOKEN),
        "semantic": (*_SEMANTIC_FIELDS, LINEAGE_SELECT_TOKEN),
        "alias": _ALIAS_FIELDS,
        "identifier_mentions": _FIND_MENTIONS_FIELDS,
        "attrs": _ATTRS_LOOKUP_FIELDS,
        "grep": _GREP_FIELDS,
    })
    mcp._okg_capabilities = capabilities  # type: ignore[attr-defined]
    surface_freshness["capabilities"] = capabilities.to_report()
    try:
        compile_tool_surface(mcp, capabilities)
        _serve_dataset_card(
            mcp,
            facts=dataset_facts,
            off_reason=(
                dataset_card_off_reason
                if instruction_source in {"rendered", "deployment_file"}
                else None
            ),
            authored=effective_instructions,
            generated=generated_instructions,
            served_tools=base_tools,
            text_kind_served="text" in inspect_kind_set,
            deployment_text=deployment_instructions_text,
            surface_freshness=surface_freshness,
            state=dataset_card_state,
        )
    except BaseException:
        pool.close()
        if write_pool is not None:
            write_pool.close()
        raise
    compact_tool_schemas(mcp)
    # Last, over the compacted result: inline every `$ref` and state a
    # top-level `type` on every union, so a client whose parser converts an
    # argument by its declared type (Ollama; issue #2792) receives objects,
    # not JSON strings. Published copy only; validation stays on the models.
    publish_client_readable_schemas(mcp)
    # Published copy only: the read tools show their shown forms; every
    # hidden argument, method and kind is still accepted (#3666, M10).
    from okg.substrate.mcp.lean_surface import publish_lean_read_surface

    publish_lean_read_surface(mcp)
    # Published copy only: each description without its source indentation
    # (PACT tool-description-dedent).
    publish_clean_descriptions(mcp)
    # Then each description's surface-dependent sentences, rendered for what
    # a session starts with (PACT dataset-card, req.dataset-card.tool-prose):
    # no description names a tool or `inspect` kind this surface does not
    # serve, `live=` only where a live relation is declared, and
    # `aggregate`'s example names one of this deployment's own subtypes.
    publish_surface_descriptions(
        mcp,
        session_tools=base_tools,
        kinds=inspect_kind_set,
        live=bool(getattr(mcp, "_okg_live_bindings", None)),
        example_subtype=example_subtype_for(dataset_facts),
        run_live=getattr(mcp, "_okg_run_live_steps", True) is not False,
    )
    # `query`'s description from the same facts (issue #3666): the views,
    # the fields of the largest types and examples that run on this graph.
    from okg.substrate.mcp.query_guide import publish_query_description

    publish_query_description(mcp, dataset_facts)

    return mcp


def _query_access_rules_mode(deployment_name: str) -> Optional[str]:
    """See `effective_surface.query_withheld_by_access_rules` (issue #3666)."""
    from okg.substrate.mcp.effective_surface import query_withheld_by_access_rules

    return query_withheld_by_access_rules(deployment_name)


def _gather_dataset_facts_for_start(
    pool: Any,
    *,
    deployment_name: str,
    branch_name: str,
    pinned: Mapping[str, Any],
    not_ingested: Sequence[str],
) -> DatasetFacts:
    """Gather the card's facts on one pooled connection; never raise.

    A card failure must not stop the server: any error here (the pool, the
    Nomos mode lookup, a bug) is logged and the card says it could not be
    compiled. An unreadable Nomos mode counts as not `off`, so the row-derived
    parts are withheld rather than published.
    """
    from okg.substrate.nomos.runtime_enforcement import (
        deployment_nomos_enforcement_mode,
    )

    try:
        nomos_mode = deployment_nomos_enforcement_mode(deployment_name)
    except Exception:  # noqa: BLE001 - fail closed: withhold row-derived parts
        log.warning(
            "dataset card: the Nomos mode of %s could not be read; withholding "
            "row-derived parts", deployment_name, exc_info=True,
        )
        nomos_mode = "unreadable"
    try:
        with pool.connection() as conn:
            return _gather_dataset_facts(
                conn,
                deployment_name=deployment_name,
                branch_name=branch_name,
                pinned=pinned,
                not_ingested=not_ingested,
                nomos_mode=nomos_mode,
            )
    except Exception as exc:  # noqa: BLE001 - a card failure never stops start
        log.warning(
            "dataset card for %s could not be compiled", deployment_name,
            exc_info=True,
        )
        return _dataset_card_unavailable(
            deployment_name=deployment_name, not_ingested=not_ingested, error=exc,
        )


def _serve_dataset_card(
    mcp: Any,
    *,
    facts: Optional[DatasetFacts],
    off_reason: Optional[str] = None,
    authored: str,
    generated: str,
    served_tools: Sequence[str],
    text_kind_served: bool,
    deployment_text: str,
    surface_freshness: dict[str, Any],
    state: dict[str, Any],
) -> None:
    """Set the served instructions: card, okg's text, the generated lines.

    PACT dataset-card. `authored` is `authored_instructions_for(...)` (okg's
    guidance, the expansion sentence, a deployment file under its heading);
    `generated` is the "not available here" block. A caller that passed its
    own instructions (`facts is None`) is served them verbatim, plus nothing.

    `instructions_hash` stays the hash of the text a client is SENT;
    `authored_instructions_hash` is the part files and code determine, which
    is what the fleet doctor recomputes without a database. The card and the
    generated block depend on the database and are in neither expectation.

    PACT dataset-card-switch: `off_reason` set means the deployment turned the
    card off. The text is served exactly as with the card on, minus the card,
    and the readbacks say it is off and why.
    """
    served_file = deployment_text.strip()
    deployment_guidance = (
        {
            "heading": DEPLOYMENT_GUIDANCE_HEADING,
            "sha256": served_instructions_fields(served_file)["instructions_hash"],
            "bytes": len(served_file.encode("utf-8")),
        }
        if served_file else None
    )
    if off_reason is not None:
        served = compose_instructions(authored, generated)
        lowlevel = getattr(mcp, "_mcp_server", None)
        if lowlevel is None or not hasattr(lowlevel, "instructions"):
            raise SearchSchemaUnreachable(
                "could not reach the MCP SDK's server instructions "
                "(_mcp_server.instructions) to serve the instructions",
                fix_hint="an MCP SDK upgrade likely renamed this internal",
            )
        lowlevel.instructions = served
        surface_freshness.update(served_instructions_fields(served))
        surface_freshness["authored_instructions_hash"] = (
            served_instructions_fields(authored)["instructions_hash"]
        )
        surface_freshness["dataset_card_bytes"] = 0
        surface_freshness["dataset_card_state"] = "off"
        state.update({
            "state": "off",
            "reason": off_reason,
            "deployment_guidance": deployment_guidance,
        })
        return
    if facts is None:
        surface_freshness["authored_instructions_hash"] = surface_freshness[
            "instructions_hash"
        ]
        return
    try:
        card = _render_dataset_card(
            facts, served_tools=served_tools, text_kind_served=text_kind_served,
            run_live=getattr(mcp, "_okg_run_live_steps", True) is not False,
        )
    except Exception as exc:  # noqa: BLE001 - a card failure never stops start
        log.warning("dataset card could not be rendered", exc_info=True)
        facts = _dataset_card_unavailable(
            deployment_name=facts.deployment, not_ingested=facts.not_ingested,
            error=exc,
        )
        card = _render_dataset_card(facts, served_tools=served_tools)
    served = card + "\n\n" + compose_instructions(authored, generated)
    lowlevel = getattr(mcp, "_mcp_server", None)
    if lowlevel is None or not hasattr(lowlevel, "instructions"):
        raise SearchSchemaUnreachable(
            "could not reach the MCP SDK's server instructions "
            "(_mcp_server.instructions) to serve the dataset card",
            fix_hint="an MCP SDK upgrade likely renamed this internal",
        )
    lowlevel.instructions = served
    surface_freshness.update(served_instructions_fields(served))
    surface_freshness["authored_instructions_hash"] = served_instructions_fields(
        authored
    )["instructions_hash"]
    surface_freshness["dataset_card_bytes"] = len(card.encode("utf-8"))
    surface_freshness["dataset_card_state"] = "on"
    inputs = _dataset_card_inputs(facts, card=card)
    state.update({
        "state": "on",
        "text": card,
        "sha256": inputs["sha256"],
        "inputs": inputs,
        # The file text as it is SERVED (stripped, under its heading).
        "deployment_guidance": deployment_guidance,
    })


_READ_ONLY_RELEASE_KEYS = (
    "verdict",
    "schema_behind_by",
    "behind_migrations",
    "verdict_message",
    "fix_hint",
)


def read_only_release_from_report(report: Mapping[str, Any]) -> dict[str, Any]:
    """The part of a `read_only` release report the server acts on.

    PACT unknown-s8-mcp-readonly (#2035). Takes slice 7's release report
    (`evaluate_release_contract`) and keeps what every response and every
    write refusal repeats. It reads the verdict and adds no check of its
    own; it only refuses a value that is not a `read_only` verdict with a
    positive distance, so a server can never start "read-only" without
    knowing how far behind it is. Idempotent on its own output.
    """
    if report.get("verdict") != "read_only":
        raise ValueError(
            "read-only serving needs a release report whose verdict is "
            f"'read_only', not {report.get('verdict')!r}"
        )
    behind_by = report.get("schema_behind_by")
    behind = report.get("behind_migrations")
    if (
        not isinstance(behind_by, int)
        or isinstance(behind_by, bool)
        or behind_by < 1
        or not isinstance(behind, (list, tuple))
        or len(behind) != behind_by
        or not isinstance(report.get("verdict_message"), str)
    ):
        raise ValueError(
            "a read_only release report must carry schema_behind_by >= 1, "
            "the same number of behind_migrations, and a verdict_message; "
            f"got schema_behind_by={behind_by!r}, behind_migrations={behind!r}"
        )
    normalized = {key: report.get(key) for key in _READ_ONLY_RELEASE_KEYS}
    normalized["behind_migrations"] = [str(item) for item in behind]
    return normalized


def build_server(
    *,
    dsn: Optional[str] = None,
    deployment_name: str = "default",
    embedder: Optional[Embedder] = None,
    instructions: str = DEFAULT_INSTRUCTIONS,
    min_conns: int = 2,
    max_conns: int | None = None,
    auto_advance: str = _AUTO_ADVANCE_DEFAULT,
    branch_name: str = "default",
    allow_writes: bool = False,
    dsn_authority: Mapping[str, Any] | None = None,
    auth_token: Optional[str] = None,
    multi_user_principals: bool = False,
    principal_mapping: Any = None,
    oidc_verifier: Any = None,
    tool_profile: Optional[str] = None,
    tool_profile_ceiling: Optional[str] = None,
    live_adapters: Mapping[str, Any] | None = None,
    read_only_release: Mapping[str, Any] | None = None,
    image_source: Any = None,
    image_budget: Any = None,
    one_call: bool = False,
    dataset_card: bool = True,
) -> FastMCP:
    """Build a server while owning any reservation across all setup.

    The auth arguments are forwarded, not defaulted. This wrapper is the ONLY
    door callers use, so a parameter it silently drops is a parameter the
    server never gets: a resolved bearer token that stops here would leave the
    /mcp route unwrapped while every caller believed authentication was on.
    run_server's stage-2 check reads the CONSTRUCTED server precisely to catch
    that, but the forwarding is what makes the posture correct in the first
    place.
    """
    if embedder is not None and not isinstance(embedder, EmbeddingExecutionPlan):
        raise TypeError(
            "build_server(embedder=...) requires a verified "
            "EmbeddingExecutionPlan, not a raw backend"
        )
    claim_attempted: list[bool] = []
    try:
        return _build_server(
            dsn=dsn,
            deployment_name=deployment_name,
            embedder=embedder,
            instructions=instructions,
            min_conns=min_conns,
            max_conns=max_conns,
            auto_advance=auto_advance,
            branch_name=branch_name,
            allow_writes=allow_writes,
            dsn_authority=dsn_authority,
            auth_token=auth_token,
            multi_user_principals=multi_user_principals,
            principal_mapping=principal_mapping,
            oidc_verifier=oidc_verifier,
            tool_profile=tool_profile,
            tool_profile_ceiling=tool_profile_ceiling,
            live_adapters=live_adapters,
            read_only_release=read_only_release,
            image_source=image_source,
            image_budget=image_budget,
            one_call=one_call,
            claim_attempted=claim_attempted,
            dataset_card=dataset_card,
        )
    except BaseException:
        # The reservation is acquired before pools open inside _build_server.
        # Cover every later pool/tool-construction failure, even before a
        # FastMCP object can be returned to run_server's outer finally.
        # A build that failed before it asked for a claim (a probe-mode
        # build, an unresolvable DSN) made none, and must not release the
        # claim another server of this deployment holds in this process.
        if claim_attempted:
            from okg.substrate.db.capacity import release_process_reservations

            release_process_reservations(
                application_name=f"okg-mcp-{deployment_name}",
            )
        raise


def _server_dsn_resolution(
    explicit_dsn: str | None,
    deployment_name: str,
) -> dict[str, Any]:
    if deployment_name and deployment_name != "default":
        from okg.substrate.deployment_authority import (
            deployment_self_hosted_authoritative,
            resolve_deployment_dsn_authority,
        )

        resolution = resolve_deployment_dsn_authority(
            deployment_name,
            explicit_dsn=explicit_dsn,
            local_profile_authoritative=deployment_self_hosted_authoritative(
                deployment_name
            ),
        )
        if resolution.get("dsn") or resolution.get("reason"):
            return resolution
        if not explicit_dsn:
            return {
                "dsn": None,
                "reason": (
                    f"deployment {deployment_name!r} DSN authority is unresolved"
                ),
                "authority": {
                    "mode": "unresolved",
                    "deployment": deployment_name,
                },
            }
    if explicit_dsn:
        return {
            "dsn": explicit_dsn,
            "authority": {
                "mode": "explicit",
                "deployment": deployment_name,
            },
            "target": None,
        }
    env_dsn = os.environ.get("OKG_DSN")
    if env_dsn:
        return {
            "dsn": env_dsn,
            "authority": {
                "mode": "process_env",
                "deployment": deployment_name,
            },
        }
    return {
        "dsn": None,
        "reason": "OKG_DSN is not set",
        "authority": {
            "mode": "default_local_compose",
            "deployment": deployment_name,
        },
    }


def _reservation_privilege_fix_hint(
    exc: BaseException, deployment_name: str | None,
) -> str:
    """Name the repair when the MCP read role cannot record its reservation.

    The read role claims through the ``okg.connection_reservation_*``
    functions that ``okg migrate`` installs, so a database migrated by an
    older release lacks them (``UndefinedFunction``) until the next migrate.
    An explicit ``OKG_MCP_RO_DSN`` role holds only what it was granted, so it
    needs to be a member of ``okg_mcp`` that inherits its privileges
    (``InsufficientPrivilege``). Any other failure gets no hint: guessing one
    would send the operator after the wrong cause.
    """
    import psycopg

    from okg.substrate.db.readiness import migrate_command

    migrate = migrate_command(deployment_name)
    if isinstance(exc, psycopg.errors.UndefinedFunction):
        return (
            " Fix: run `" + migrate + "` to install the MCP role's "
            "connection reservation functions."
        )
    if isinstance(exc, psycopg.errors.InsufficientPrivilege):
        return (
            " Fix: run `" + migrate + "` to grant the MCP role EXECUTE on "
            "its connection reservation functions; if OKG_MCP_RO_DSN names "
            "a custom role, make it an inheriting member of okg_mcp "
            "(`GRANT okg_mcp TO <that role>`, and the role must not be "
            "NOINHERIT)."
        )
    return ""


def _derive_role_dsn(dsn: str, *, role: str, env_var: str) -> str:
    """Return a connection string for `role`, derived from `dsn`.

    Precedence (issue #3273): an explicit `env_var` DSN (operator-managed,
    e.g. a remote DB with a managed role password) wins. Otherwise, for a
    local/loopback database only, the owner-only role secrets file that
    `okg db role-passwords --apply` writes; then the built-in development
    passwords, but only when OKG_ALLOW_BUILTIN_ROLE_PASSWORDS=1 marks a
    single-user machine. Anything else is refused with the command that
    provisions passwords. See `okg.substrate.local_database_roles`.
    """
    from okg.substrate.local_database_roles import resolve_role_dsn

    return resolve_role_dsn(dsn, role=role, env_var=env_var)


def _is_loopback_host(host: str) -> bool:
    """True if `host` only accepts connections from the local machine.

    Uses IP-literal semantics: the whole 127.0.0.0/8 range and ::1 are loopback;
    0.0.0.0 / :: (all interfaces) and any routable address are not. `localhost`
    is the one name treated as loopback -- any other hostname is treated as
    NON-loopback (safe direction), so a name that merely starts with "127."
    cannot slip a public bind past the write guard.
    """
    import ipaddress

    h = host.strip().lower()
    if h == "localhost":
        return True
    try:
        return ipaddress.ip_address(h).is_loopback
    except ValueError:
        return False


#: Public name for the loopback predicate. Consumers outside this module
#: (PACT chat-source-admin's admin-preset transport precondition) must decide
#: "loopback-local to the instance" with the SAME semantics the write guard
#: uses, so there is one definition and no second, drifting copy.
is_loopback_host = _is_loopback_host


def _writes_effectively_enabled(allow_writes: bool) -> bool:
    """Writes are on via the `allow_writes` arg OR OKG_MCP_ALLOW_WRITES.

    Same truthy set as build_server, factored out so the write guard and the
    read-surface warning cannot drift on what "writes are enabled" means.
    """
    return bool(allow_writes) or os.environ.get("OKG_MCP_ALLOW_WRITES") in {
        "1",
        "true",
        "TRUE",
        "yes",
        "YES",
    }


def _write_transport_guard(
    *,
    allow_writes: bool,
    transport: str,
    host: str,
    auth_active: bool = False,
) -> tuple[str, str] | None:
    """Guard the WRITE surface against network exposure.

    Under stdio the write tools are reachable only by the local operator who
    launched the subprocess. Over a network transport that guarantee is gone,
    so the decision turns on whether callers must present a credential.
    Returns None when safe, else a ``(level, message)`` pair where ``level`` is
    ``"refuse"`` (never do this) or ``"warn"`` (allowed, cautioned).

    ``auth_active`` (PACT mcp-http-auth) means OBSERVABLE ON THE SERVED APP: a
    token verifier is installed on the FastMCP instance actually being served,
    so the /mcp route is wrapped in the SDK's RequireAuthMiddleware. It must be
    derived from the constructed server (see
    :func:`okg.substrate.mcp.http_auth.server_has_token_verifier`), never from
    a flag or an environment variable -- a credential that resolved but was
    never wired would otherwise unlock the routable write surface. It defaults
    to False so every existing caller keeps today's no-auth behavior.

    The four WRITE cells of the 2x2x2 decision space:

      non-loopback + no auth -> refuse (message unchanged from before auth)
      non-loopback + auth    -> permitted; callers must present a token
      loopback + no auth     -> warn (message unchanged from before auth)
      loopback + auth        -> permitted, and the warning is SUPPRESSED: its
                                text asserts "the MCP endpoint is
                                unauthenticated", which is now false, and a
                                warning that states a falsehood trains
                                operators to ignore warnings.

    The four READ-ONLY cells return None here (this guard covers the write
    surface only) and are decided by :func:`_readonly_transport_warning`.
    """
    effective = _writes_effectively_enabled(allow_writes)
    if transport == "stdio" or not effective:
        return None
    if not _is_loopback_host(host):
        if auth_active:
            return None
        return (
            "refuse",
            f"refusing to serve the write-enabled MCP surface over {transport} "
            f"bound to non-loopback host {host!r}: the MCP endpoint is "
            "unauthenticated, so this exposes graph mutation to anyone who can "
            "reach the port. Use stdio, bind --host 127.0.0.1, or drop "
            "--allow-writes.",
        )
    if auth_active:
        return None
    return (
        "warn",
        f"serving the write-enabled MCP surface over {transport} on loopback "
        f"{host!r}: reachable by any process sharing this host's network "
        "namespace (e.g. other --network host containers). The MCP endpoint is "
        "unauthenticated; keep it off shared/public hosts.",
    )


def _readonly_transport_warning(
    *,
    allow_writes: bool,
    transport: str,
    host: str,
    auth_active: bool = False,
) -> str | None:
    """Caution an unauthenticated READ surface bound to a routable address.

    The remaining cell of the 2x2x2 decision space the write guard does not
    cover. A read-only server on 0.0.0.0 has always been allowed and stays
    allowed -- it is a supported sidecar shape -- but with no credential check
    anyone who can reach the port can read the whole graph, which is the
    shared-host reader exposure. Returns the warning text, or None when the
    cell is not exposed (stdio, writes on -- the write guard owns that cell and
    its message already covers exposure, loopback, or auth active).
    """
    if transport == "stdio" or _writes_effectively_enabled(allow_writes):
        return None
    if auth_active or _is_loopback_host(host):
        return None
    return (
        f"serving the read-only MCP surface over {transport} bound to "
        f"non-loopback host {host!r} with no authentication: any process that "
        "can reach this port can read the whole graph, including on a shared "
        "host. Require a bearer token with --auth-token-env VARIABLE_NAME, "
        "bind --host 127.0.0.1, or front the bind with a network control."
    )


#: After SIGTERM, how long run_server's unwind may take before the stop guard
#: ends the process anyway. A normal stop unwinds in well under a second. The
#: guard exists for the rare stop (about 1 in 50-100) where asyncio.run()
#: teardown never finishes because a cancelled task is never woken; the client
#: then SIGKILLs the process and run_server's ``finally`` never runs.
MCP_STOP_GUARD_SECONDS = 5.0

#: Log line the watchdog writes before it ends a stuck shutdown. Tests and
#: operators search for it, so it stays stable.
MCP_STOP_GUARD_EXIT_MESSAGE = "okg mcp-serve shutdown did not finish"


class _StopGuard:
    """Release the connection claim on SIGTERM and bound the shutdown.

    ``release`` drops this server's connection reservation. It runs at most
    once to completion: a second caller waits for an in-flight release and
    then returns, so the early release and run_server's ``finally`` never
    race each other into leaving the row behind.

    When ``armed``, a daemon thread waits for :meth:`trigger`. The SIGTERM
    handler calls it; it only writes one byte to a pipe, which takes no lock
    and does no I/O beyond the write, so it is safe inside a signal handler.
    The thread starts the claim release on a daemon thread of its own and
    waits until ``MCP_STOP_GUARD_SECONDS`` after SIGTERM, whether or not the
    release has finished (a paused database cannot hold the deadline). If the
    process is still alive then -- stuck in asyncio teardown, or at
    interpreter exit -- it runs the diagnostics (marker line, thread stacks,
    log flush) on another daemon thread for at most
    ``_STOP_GUARD_DIAGNOSTICS_SECONDS`` and then ends the process with
    ``os._exit`` and the same code a normal SIGTERM exit gives (128 + signal
    number). :meth:`disarm` stops a thread that was never triggered; once
    triggered, the guard stays armed until the process exits, which is what
    SIG_DFL would have done at once.

    ``exit_message`` starts the marker line and ``thread_dump`` says whether
    the thread stacks follow it. ``okg mcp-serve`` keeps the defaults; a
    one-call read command names itself and leaves the stacks out, because
    they name files of the host.
    """

    def __init__(
        self,
        release: Any,
        *,
        armed: bool,
        exit_message: str = MCP_STOP_GUARD_EXIT_MESSAGE,
        thread_dump: bool = True,
    ) -> None:
        self._release_fn = release
        self._exit_message = exit_message
        self._thread_dump = thread_dump
        self._release_lock = threading.Lock()
        self._release_done = False
        self._finished = threading.Event()
        self._exit_code: int | None = None
        self._read_fd: int | None = None
        self._write_fd: int | None = None
        self._thread: threading.Thread | None = None
        if armed:
            self._read_fd, self._write_fd = os.pipe()
            self._thread = threading.Thread(
                target=self._watch,
                name="okg-mcp-stop-guard",
                daemon=True,
            )
            self._thread.start()

    @property
    def triggered(self) -> bool:
        return self._exit_code is not None

    def release(self) -> None:
        with self._release_lock:
            if self._release_done:
                return
            self._release_fn()
            self._release_done = True

    def trigger(self, exit_code: int) -> None:
        """Signal-handler side: record the exit code and wake the thread."""
        if self._exit_code is not None:
            return
        self._exit_code = exit_code
        self._wake()

    def disarm(self) -> None:
        self._finished.set()
        self._wake()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)
            if not thread.is_alive():
                self._close_fds()

    def _wake(self) -> None:
        write_fd = self._write_fd
        if write_fd is None:
            return
        try:
            os.write(write_fd, b"x")
        except OSError:
            pass

    def _close_fds(self) -> None:
        for name in ("_write_fd", "_read_fd"):
            fd = getattr(self, name)
            setattr(self, name, None)
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass

    def _release_quietly(self) -> None:
        try:
            self.release()
        except BaseException as exc:  # noqa: BLE001 - a daemon thread; report and stop
            log.warning(
                "early connection-reservation release on SIGTERM failed: %s; "
                "run_server's own release still runs if shutdown finishes",
                exc,
            )

    def _watch(self) -> None:
        assert self._read_fd is not None
        try:
            os.read(self._read_fd, 1)
        except OSError:
            return
        if self._finished.is_set() or self._exit_code is None:
            return
        # The deadline counts from SIGTERM. Nothing below that can block --
        # the release, a log handler, a full stderr pipe -- may move it.
        deadline = time.monotonic() + MCP_STOP_GUARD_SECONDS
        threading.Thread(
            target=self._release_quietly,
            name="okg-mcp-stop-guard-release",
            daemon=True,
        ).start()
        if self._finished.wait(max(0.0, deadline - time.monotonic())):
            return
        exit_code = self._exit_code
        diagnostics = threading.Thread(
            target=_flush_before_hard_exit,
            args=(
                exit_code, self._release_done, self._exit_message, self._thread_dump,
            ),
            name="okg-mcp-stop-guard-diagnostics",
            daemon=True,
        )
        diagnostics.start()
        diagnostics.join(_STOP_GUARD_DIAGNOSTICS_SECONDS)
        os._exit(exit_code)


#: How long the watchdog lets its diagnostics run before it exits anyway.
_STOP_GUARD_DIAGNOSTICS_SECONDS = 0.5


def _flush_before_hard_exit(
    exit_code: int,
    released: bool,
    exit_message: str = MCP_STOP_GUARD_EXIT_MESSAGE,
    thread_dump: bool = True,
) -> None:
    """Flush what os._exit would drop: marker, thread stacks, logs, stdio.

    Runs on a daemon thread the watchdog does not wait on for long. The
    marker goes straight to fd 2 with os.write, so it takes no logging or
    stream lock and no handler can wrap or restyle it. The thread stacks
    follow it only when ``thread_dump`` is true.
    """
    import faulthandler
    import sys

    claim = "released" if released else "release still in flight"
    marker = (
        f"{exit_message} within {MCP_STOP_GUARD_SECONDS:g}s of "
        f"SIGTERM; connection claim {claim}; exit {exit_code}\n"
    )
    try:
        os.write(2, marker.encode("utf-8", "replace"))
    except OSError:
        pass
    if thread_dump:
        try:
            faulthandler.dump_traceback(file=2, all_threads=True)
        except Exception:  # noqa: BLE001 - diagnostics only; the exit must happen
            pass
    seen: set[int] = set()
    for logger in (logging.getLogger(), log):
        current: logging.Logger | None = logger
        while current is not None:
            for handler in current.handlers:
                if id(handler) in seen:
                    continue
                seen.add(id(handler))
                try:
                    handler.flush()
                except Exception:  # noqa: BLE001 - keep flushing the rest
                    pass
            current = current.parent if current.propagate else None
    for stream in (sys.stderr, sys.stdout):
        try:
            stream.flush()
        except Exception:  # noqa: BLE001 - a closed pipe must not block exit
            pass


@contextmanager
def _unwind_default_sigterm(
    release: Any = None,
    *,
    exit_message: str = MCP_STOP_GUARD_EXIT_MESSAGE,
    thread_dump: bool = True,
):
    """Turn catchable default SIGTERM into normal Python cleanup.

    Preserve an embedding application's custom handler. Network servers may
    replace our temporary handler with their own graceful-stop handler; in that
    case leave their handler in place rather than restoring stale state.

    Yields a :class:`_StopGuard` whose ``release`` the caller's ``finally``
    uses. When this context installed the handler, a SIGTERM first wakes the
    guard, which releases the connection reservation from its own thread and
    ends the process if it is still alive ``MCP_STOP_GUARD_SECONDS`` later;
    the handler then raises SystemExit as before.

    On stdio the guard starts at the SIGTERM itself. On streamable-http and
    sse it does not: uvicorn swaps in its own handler while serving, takes the
    first SIGTERM and drains, and only re-raises the signal to this handler
    after its graceful stop returns. So on HTTP transports the early release
    and the deadline start AFTER uvicorn's drain, and an open session can hold
    that drain indefinitely. That gap is recorded in the PACT notes
    (lane-h-mcp-serve-stop-guard) and is not handled here.

    ``exit_message`` and ``thread_dump`` go to the guard (see
    :class:`_StopGuard`); ``okg mcp-serve`` passes neither.
    """
    release_fn = release if release is not None else (lambda: None)
    guard_options = {"exit_message": exit_message, "thread_dump": thread_dump}
    if threading.current_thread() is not threading.main_thread():
        yield _StopGuard(release_fn, armed=False, **guard_options)
        return
    previous = signal.getsignal(signal.SIGTERM)
    installed = previous is signal.SIG_DFL
    guard = _StopGuard(release_fn, armed=installed, **guard_options)

    def _raise_system_exit(signum, _frame) -> None:
        guard.trigger(128 + signum)
        raise SystemExit(128 + signum)

    if installed:
        signal.signal(signal.SIGTERM, _raise_system_exit)
    try:
        yield guard
    finally:
        if installed and signal.getsignal(signal.SIGTERM) is _raise_system_exit:
            signal.signal(signal.SIGTERM, previous)
        if not guard.triggered:
            guard.disarm()
        else:
            # Do not return while the early release is still in flight: the
            # process may exit right after, and the claim must not outlive
            # it. The guard stays armed. SIGTERM was SIG_DFL, so the process
            # is stopping, and interpreter exit can hang too: with stdin still
            # open, it waits forever on the non-daemon worker thread blocked
            # reading stdin. The bound counts from SIGTERM either way.
            guard.release()


def run_server(
    *,
    transport: str = "stdio",
    host: str = "127.0.0.1",
    port: int = 8100,
    dsn: Optional[str] = None,
    dsn_authority: Mapping[str, Any] | None = None,
    deployment_name: str = "default",
    embedder: Optional[Embedder] = None,
    instructions: str = DEFAULT_INSTRUCTIONS,
    branch_name: str = "default",
    allow_writes: bool = False,
    allowed_hosts: tuple[str, ...] = (),
    auth_token_env: Optional[str] = None,
    multi_user_principals: bool = False,
    oidc_auth: bool = False,
    tool_profile: Optional[str] = None,
    tool_profile_ceiling: Optional[str] = None,
    read_only_release: Mapping[str, Any] | None = None,
) -> None:
    """Transport-agnostic entry point for `okg mcp-serve`.

    Builds the substrate FastMCP server (identical to the stdio path) and
    blocks on its run loop until the client disconnects.

    `transport` selects the FastMCP transport:
      - "stdio" (default): protocol over stdin/stdout; `host`/`port` unused.
        This keeps every existing `okg mcp-serve` / `~/.mcp.json` registration
        byte-for-byte unchanged.
      - "streamable-http": serve the MCP protocol at http://`host`:`port`/mcp
        so clients that are not the direct parent process (e.g. a chatbot in a
        container reaching a host service over the network) can consume the
        read surface.

    `branch_name` (AKMON Phase A.3) pins this server to a single
    substrate branch. To read a different branch, start a separate
    `okg mcp-serve --branch <name>` — one server = one branch.

    `allow_writes` (PACT least-privilege-mcp) enables the graph/source
    write tools over a separate app_rw pool. Default off: the server is
    read-only and the agent cannot self-grant writes. Enabling writes over a
    non-loopback network transport is refused unless callers must present a
    bearer credential (see `_write_transport_guard`).

    `auth_token_env` (PACT mcp-http-auth) names the ENVIRONMENT VARIABLE
    holding the bearer token for network transports; the value never travels
    in argv. It is the sole activation signal -- no ambient variable flips the
    auth posture on its own, so the whole security posture is visible in the
    invocation. A named-but-unresolvable variable refuses to start rather than
    starting unauthenticated, and combining it with stdio is an explicit
    refusal, never a silent no-op.

    `tool_profile_ceiling` (issue #1458) lets each session widen ITSELF, at
    most to that profile, without a restart and without changing any other
    session's tool list. It requires a narrowed launch and a ceiling that
    strictly contains it, and it is what makes this server advertise
    `tools.listChanged: true`; without it the tool list really is fixed for a
    session's lifetime and the server says so.

    `multi_user_principals` (PACT mcp-principal-mapping) resolves each
    request's own principal from the deployment's declared header mapping. It
    supports streamable HTTP, including GET streams. Legacy SSE is refused:
    its SDK POST owner check does not prove fresh claims from the initial GET
    context. Single-operator legacy SSE remains available to Python callers.
    Multi-user mode is refused without an authenticated transport -- a principal header
    asserted by an unauthenticated client is a claim anyone who can reach the
    port may forge -- and refused over stdio, which carries no headers at all.
    Both of those refusals happen before any bind or DB work. A THIRD check
    then re-reads the CONSTRUCTED server, so a flag alone can never enable
    multi-user mode; that one necessarily runs after the pools are open and
    closes them before it raises.

    `oidc_auth` (PACT mcp-oidc-auth) verifies each request's bearer JWT against
    the identity provider declared in ``nomos.oidc_auth`` and resolves the
    per-request principal from it. It is refused over stdio, alongside
    `auth_token_env` or `multi_user_principals`, and without a declared block,
    all before any bind or DB work; the issuer's keys are discovered here too,
    so an unreachable issuer refuses to start instead of refusing every call.

    `read_only_release` (PACT unknown-s8-mcp-readonly, #2035) is the release
    report when its verdict is `read_only`: the database is behind this code
    only by migrations that declare `reads_safe_behind`. The server then
    adds `schema_behind_by` to every read-tool response and refuses every
    other tool with the verdict's sentence (`schema_behind_read_only`).
    ``None`` changes nothing.
    """
    oidc_verifier: Any = None
    if oidc_auth:
        from okg.substrate.mcp.oidc_auth import (
            build_oidc_token_verifier,
            oidc_auth_exclusive_error,
            oidc_auth_stdio_error,
            oidc_auth_undeclared_error,
        )

        if transport == "stdio":
            raise oidc_auth_stdio_error()
        if auth_token_env is not None:
            raise oidc_auth_exclusive_error("--auth-token-env")
        if multi_user_principals:
            raise oidc_auth_exclusive_error("--multi-user-principals")
        oidc_config = resolve_oidc_auth(deployment_name)
        if oidc_config is None:
            raise oidc_auth_undeclared_error(deployment_name)
        # Only `enforce` withholds rows a user's grants do not cover. Under
        # `audit` Nomos records denials but still serves the rows, and under
        # `off` it does not filter at all, so an OIDC server reachable from
        # outside this host would show every admitted user everything. Only a
        # loopback bind (a local test) may run OIDC without `enforce`.
        # Checked before any bind or discovery.
        from okg.substrate.mcp.oidc_auth import oidc_auth_audit_exposed_error
        from okg.substrate.nomos.runtime_enforcement import (
            deployment_nomos_enforcement_mode,
        )

        enforcement_mode = deployment_nomos_enforcement_mode(deployment_name)
        if enforcement_mode != "enforce" and not _is_loopback_host(host):
            raise oidc_auth_audit_exposed_error(
                deployment_name, host=host, mode=enforcement_mode,
            )
        # Before discovery: a chat that signs people in against another
        # provider or client would have every request refused.
        _require_chat_login_agreement(deployment_name, oidc_config)
        oidc_verifier = build_oidc_token_verifier(oidc_config)
    if multi_user_principals:
        # PACT mcp-principal-mapping: refuse BEFORE any bind or DB work, so a
        # refused multi-user start leaves no socket and no pool behind.
        from okg.substrate.mcp.principal_mapping import (
            multi_user_requires_auth_error,
        )

        if transport == "stdio":
            # stdio is exempt from the transport-AUTHENTICATION requirement
            # (its caller is the process that launched it), but it carries no
            # HTTP request and therefore no headers, so there is no channel a
            # declared principal header could arrive on. Refusing states that
            # instead of starting a server that would refuse every request.
            err = SubstrateError(
                "--multi-user-principals is incompatible with --transport "
                "stdio: stdio carries no HTTP request and therefore no "
                "principal header, so no per-request principal could ever be "
                "read. The stdio surface is already single-operator -- its "
                "caller is the process that launched it.",
                data={"transport": "stdio", "multi_user_principals": True},
                fix_hint=(
                    "serve with --transport streamable-http and "
                    "--auth-token-env VARIABLE_NAME for multi-user "
                    "principals, or drop --multi-user-principals for stdio"
                ),
            )
            err.code = "mcp_multi_user_stdio_incompatible"
            raise err
        if transport != "streamable-http":
            raise SubstrateError(
                "multi-user MCP requires streamable-http session ownership; legacy SSE is unsupported",
                fix_hint="use --transport streamable-http",
            )
        if auth_token_env is None:
            raise multi_user_requires_auth_error(transport=transport)
    if auth_token_env is not None and transport == "stdio":
        from okg.substrate.mcp.http_auth import stdio_auth_incompatibility_error

        raise stdio_auth_incompatibility_error(auth_token_env)
    auth_token: Optional[str] = None
    if auth_token_env is not None:
        from okg.substrate.mcp.http_auth import resolve_auth_token

        # Fail closed BEFORE any bind or DB work: a configured credential that
        # cannot be resolved must not degrade into an unauthenticated server.
        auth_token = resolve_auth_token(auth_token_env)
    requested_auth = auth_token is not None or oidc_verifier is not None
    # Two-stage guard. Stage 1 refuses on the REQUESTED posture so a refused
    # start still does no DB work (build_server opens connection pools) -- the
    # property the CLI pre-check and today's callers rely on. It only refuses;
    # warnings are left to stage 2 so they are emitted exactly once.
    preflight = _write_transport_guard(
        allow_writes=allow_writes,
        transport=transport,
        host=host,
        auth_active=requested_auth,
    )
    if preflight is not None and preflight[0] == "refuse":
        raise ValueError(preflight[1])
    def _release_claim() -> None:
        from okg.substrate.db.capacity import release_process_reservations

        release_process_reservations(
            application_name=f"okg-mcp-{deployment_name}",
        )

    with _unwind_default_sigterm(_release_claim) as stop_guard:
        mcp = build_server(
            dsn=dsn,
            dsn_authority=dsn_authority,
            deployment_name=deployment_name,
            embedder=embedder,
            instructions=instructions,
            branch_name=branch_name,
            allow_writes=allow_writes,
            auth_token=auth_token,
            multi_user_principals=multi_user_principals,
            oidc_verifier=oidc_verifier,
            tool_profile=tool_profile,
            tool_profile_ceiling=tool_profile_ceiling,
            read_only_release=read_only_release,
        )
        try:
            # Stage 2 is authoritative and reads the CONSTRUCTED server, not
            # the flag: a credential that resolved but never reached FastMCP
            # leaves the /mcp route unwrapped, and that server must not be
            # treated as authenticated. It runs INSIDE the try so a stage-2
            # refusal still releases this process's capacity reservations.
            from okg.substrate.mcp.http_auth import server_has_token_verifier

            auth_active = server_has_token_verifier(mcp)
            if auth_token is not None and not auth_active:
                raise ValueError(
                    "MCP bearer authentication was configured from environment "
                    f"variable {auth_token_env!r}, but the constructed server "
                    "has no token verifier installed, so the endpoint would "
                    "serve unauthenticated; refusing to start."
                )
            if (multi_user_principals or oidc_verifier is not None) and not auth_active:
                # Authoritative re-check on the CONSTRUCTED server. The
                # pre-build refusal above reads the requested posture; this one
                # reads what is actually being served, so a credential that
                # resolved but never reached FastMCP cannot leave a multi-user
                # server accepting principal headers from unauthenticated
                # callers. Unlike that one it runs AFTER build_server opened
                # the pools, so release them: a refused start must leave
                # nothing behind.
                from okg.substrate.mcp.principal_mapping import (
                    multi_user_requires_auth_error,
                )

                for attribute in ("_okg_pool", "_okg_write_pool"):
                    opened = getattr(mcp, attribute, None)
                    if opened is not None:
                        opened.close()
                raise multi_user_requires_auth_error(transport=transport)
            guard = _write_transport_guard(
                allow_writes=allow_writes,
                transport=transport,
                host=host,
                auth_active=auth_active,
            )
            if guard is not None:
                level, message = guard
                if level == "refuse":
                    raise ValueError(message)
                log.warning(message)
            readonly_warning = _readonly_transport_warning(
                allow_writes=allow_writes,
                transport=transport,
                host=host,
                auth_active=auth_active,
            )
            if readonly_warning is not None:
                log.warning(readonly_warning)
            if transport == "stdio":
                mcp.run()
                return
            # Non-stdio transports bind a socket; FastMCP reads the bind
            # address off mcp.settings, so set it before handing off to run().
            mcp.settings.host = host
            mcp.settings.port = port
            if allowed_hosts:
                from mcp.server.transport_security import (
                    TransportSecuritySettings,
                )

                host_pattern = re.compile(
                    r"^(?:\[[0-9A-Fa-f:]+\]|"
                    r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?)"
                    r"(?::(?:\*|[1-9][0-9]{0,4}))?$"
                )
                invalid_hosts = [
                    value
                    for value in allowed_hosts
                    if host_pattern.fullmatch(value) is None
                ]
                if invalid_hosts:
                    raise ValueError(
                        "invalid MCP allowed Host header value(s): "
                        + ", ".join(repr(value) for value in invalid_hosts)
                    )
                mcp.settings.transport_security = TransportSecuritySettings(
                    enable_dns_rebinding_protection=True,
                    allowed_hosts=list(allowed_hosts),
                    allowed_origins=[],
                )
            # The startup log names the VARIABLE, never its value (PACT
            # mcp-http-auth: the token must not reach logs, help, or evidence).
            log.info(
                "serving MCP over %s at http://%s:%s/mcp (%s)",
                transport,
                host,
                port,
                (
                    "OIDC bearer authentication required; tokens verified "
                    f"against {oidc_verifier.config.issuer}"
                    if oidc_verifier is not None
                    else f"bearer authentication required; token read from ${auth_token_env}"
                    if auth_active
                    else "unauthenticated"
                ),
            )
            mcp.run(transport=transport)
        finally:
            # Idempotent with the stop guard's early release on SIGTERM: one
            # of them deletes the row, the other waits for it and returns.
            stop_guard.release()


def run_stdio(
    *,
    dsn: Optional[str] = None,
    dsn_authority: Mapping[str, Any] | None = None,
    deployment_name: str = "default",
    embedder: Optional[Embedder] = None,
    instructions: str = DEFAULT_INSTRUCTIONS,
    branch_name: str = "default",
    allow_writes: bool = False,
) -> None:
    """Stdio entry point for `okg mcp-serve` — thin delegator to run_server.

    Preserved for existing callers (deployment_entry.run_for_deployment and
    the __main__ dev entry). See run_server for the transport-agnostic path.
    """
    run_server(
        transport="stdio",
        dsn=dsn,
        dsn_authority=dsn_authority,
        deployment_name=deployment_name,
        embedder=embedder,
        instructions=instructions,
        branch_name=branch_name,
        allow_writes=allow_writes,
    )


if __name__ == "__main__":  # pragma: no cover
    # `python -m okg.substrate.mcp.server` for quick dev runs.
    from okg.substrate.bytecode_cache import enter_user_bytecode_cache

    enter_user_bytecode_cache()
    run_stdio()
