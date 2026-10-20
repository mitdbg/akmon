"""Per-source coverage at the session's pinned generation.

Per issue #1444. The question this answers is the one an analyst asks after an
empty page: **is there nothing to find, or is the source that would have
answered me simply not here?** Every existing surface answers a different
question. `search` reports `total_count: 0` identically for both. `inspect`'s
graph target counts subtypes, not sources. `freshness.source_freshness` reads a
LIVE watermark that has no relation to the generation the caller is pinned to.
`list_sources` / `source_status` do report registry state, but they are admin
verbs on the write surface, so the read-only agent that needs the answer cannot
call them.

So this is a READ-side, GENERATION-PINNED, per-source manifest, and its whole
job is to keep four states apart:

  included       the deployment registry declares this source and it has live
                 records at this pin, with the count.
  not_ingested   the registry declares it and it has ZERO live records at this
                 pin. "The graph does not contain this source" — not "your
                 query missed".
  unregistered   records at this pin carry a source the registry does not
                 declare. A renamed or removed source leaves rows behind, and
                 silently omitting them would understate the corpus.
  declared_gap   the deployment itself says this source is knowingly partial.

**Everything reported here is either measured at the pin or read from a stored
column, and the two are labelled.** The run block is the clearest case: nothing
in this schema stores a run OUTCOME as a typed column. `last_run_status` and
`last_error` are JSON keys read out of `okg.source_progress.value`, so a null
`last_error` means "no failure was RECORDED", never "the run succeeded", and
this module says so in the payload rather than letting a null read as health.

**Bounded by construction.** `inspect` is not a pageable operator: an oversized
payload is refused whole, with no cursor to continue from. A manifest that grew
with the source count would therefore turn into a hard refusal on exactly the
deployments that most need it, so the source list is capped, error text is
truncated, and both are disclosed.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, Sequence

import psycopg
from psycopg.rows import dict_row

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.session import Session
from okg.substrate.nomos.runtime_enforcement import (
    deployment_nomos_enforcement_mode,
)


__all__ = [
    "COVERAGE_BLOCKS",
    "COVERAGE_SOURCE_CAP",
    "CoverageConfigInvalid",
    "STATE_DECLARED_GAP",
    "STATE_INCLUDED",
    "STATE_NOT_INGESTED",
    "STATE_UNREGISTERED",
    "inspect_coverage",
    "parse_coverage_config",
]


class CoverageConfigInvalid(SubstrateError):
    """A `coverage:` block in deployment.yaml that does not parse.

    Strict for the same reason `search:` is strict: a silently-ignored
    `known_gap:` (singular) would leave a deployment believing it had disclosed
    a gap it had not, and the manifest would then present a source as fully
    covered on the deployment's own authority.
    """

    code = "coverage_config_invalid"


#: Accepted keys, at each level. Anything else raises.
_COVERAGE_BLOCK_KEYS = frozenset({"known_gaps"})
_KNOWN_GAP_KEYS = frozenset({"source", "description"})

#: Per-source rows one manifest may return. Deployments have at most dozens of
#: sources, so this is not a limit a real registry reaches; it exists because
#: `inspect` is NOT pageable — an oversized payload is refused whole, with no
#: cursor — so an unbounded list would turn a misconfigured registry into a
#: total read failure. `sources_total` and the `states` tally stay exact over
#: EVERY source, so the bound is visible rather than silent.
#:
#: Sized by measurement, not taste: 100 rows with every block selected
#: serialize to 36,135 bytes at the SDK boundary (the same measurement
#: `bound_read_tool` applies), against the 65,536-byte default budget. That
#: leaves room for long deployment/generation identifiers and for a per-row
#: recorded run block without approaching the ceiling.
COVERAGE_SOURCE_CAP = 100

#: Recorded error text one row may carry. Producer-written and unbounded, so it
#: is truncated at the same order the operator error path truncates messages.
_ERROR_TEXT_CAP = 160

#: Live rows the `enforce`-mode per-row count may materialize before refusing.
#: IMPORTED from `inspect(graph)` rather than re-declared, so the two
#: enforcement budgets cannot drift into disagreeing about what "affordable"
#: means: the per-row filter costs roughly one policy evaluation per row on
#: both paths, so the same ceiling applies for the same reason.
from okg.substrate.mcp.tools import (  # noqa: E402
    _INSPECT_GRAPH_EXACT_ROW_BUDGET as COVERAGE_ENFORCE_ROW_BUDGET,
)

#: Selectable blocks. `select` narrows the WORK, not just the payload: an
#: unselected block does not run its query, matching `inspect(graph)`.
_BLOCK_RECORDS = "records"
_BLOCK_RUNS = "runs"
_BLOCK_SPANS = "spans"
_BLOCK_KNOWN_GAPS = "known_gaps"
COVERAGE_BLOCKS = frozenset(
    {_BLOCK_RECORDS, _BLOCK_RUNS, _BLOCK_SPANS, _BLOCK_KNOWN_GAPS}
)

STATE_INCLUDED = "included"
STATE_NOT_INGESTED = "not_ingested"
STATE_UNREGISTERED = "unregistered"
STATE_DECLARED_GAP = "declared_gap"

#: The attribute a time span is measured over. Imported rather than re-spelled
#: so this and the search recency annotation can never order by two different
#: quantities while reporting one word.
from okg.substrate.mcp.read_affordances import RECENCY_ATTRIBUTE  # noqa: E402


def parse_coverage_config(
    raw: Any, *, manifest_path: Any = None,
) -> dict[str, Any] | None:
    """Validate and parse a manifest `coverage:` block. None when absent.

    Shape:

        coverage:
          known_gaps:
            - source: fireflies
              description: transcripts before 2024 were never collected

    Returns `{"known_gaps": [{"source": ..., "description": ...}, ...]}` or
    None. Strict at every level.
    """
    where = f" (Manifest: {manifest_path})" if manifest_path else ""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise CoverageConfigInvalid(
            f"deployment.yaml `coverage:` must be a mapping; got "
            f"{type(raw).__name__}.{where}",
            data={"got_type": type(raw).__name__},
            fix_hint="write `coverage:` as a mapping with a `known_gaps:` list",
        )
    unknown = sorted(set(raw) - _COVERAGE_BLOCK_KEYS)
    if unknown:
        raise CoverageConfigInvalid(
            f"deployment.yaml `coverage:` has unknown key(s) "
            f"{', '.join(repr(k) for k in unknown)}; accepted keys are "
            f"{sorted(_COVERAGE_BLOCK_KEYS)}.{where}",
            data={
                "unknown_keys": unknown,
                "accepted_keys": sorted(_COVERAGE_BLOCK_KEYS),
            },
            fix_hint="fix the key spelling under `coverage:`",
        )
    gaps_raw = raw.get("known_gaps")
    if gaps_raw is None:
        return {"known_gaps": []}
    if not isinstance(gaps_raw, list):
        raise CoverageConfigInvalid(
            f"deployment.yaml `coverage.known_gaps` must be a list; got "
            f"{type(gaps_raw).__name__}.{where}",
            data={"got_type": type(gaps_raw).__name__},
            fix_hint="write `known_gaps:` as a list of {source, description}",
        )
    gaps: list[dict[str, str]] = []
    seen: set[str] = set()
    for index, entry in enumerate(gaps_raw):
        if not isinstance(entry, dict):
            raise CoverageConfigInvalid(
                f"deployment.yaml `coverage.known_gaps[{index}]` must be a "
                f"mapping; got {type(entry).__name__}.{where}",
                data={"index": index, "got_type": type(entry).__name__},
                fix_hint="each known gap is {source: ..., description: ...}",
            )
        extra = sorted(set(entry) - _KNOWN_GAP_KEYS)
        if extra:
            raise CoverageConfigInvalid(
                f"deployment.yaml `coverage.known_gaps[{index}]` has unknown "
                f"key(s) {', '.join(repr(k) for k in extra)}; accepted keys "
                f"are {sorted(_KNOWN_GAP_KEYS)}.{where}",
                data={
                    "index": index,
                    "unknown_keys": extra,
                    "accepted_keys": sorted(_KNOWN_GAP_KEYS),
                },
                fix_hint="fix the key spelling inside the known gap entry",
            )
        source = entry.get("source")
        description = entry.get("description")
        for label, value in (("source", source), ("description", description)):
            if not isinstance(value, str) or not value.strip():
                raise CoverageConfigInvalid(
                    f"deployment.yaml `coverage.known_gaps[{index}].{label}` "
                    f"must be a non-empty string.{where}",
                    data={"index": index, "field": label},
                    fix_hint=(
                        f"set `{label}` on the known gap; a gap that names no "
                        f"source, or states no reason, discloses nothing"
                    ),
                )
        name = str(source).strip()
        if name in seen:
            # Two descriptions of one source's gap would leave a reader with
            # no way to know which is current.
            raise CoverageConfigInvalid(
                f"deployment.yaml `coverage.known_gaps` declares {name!r} "
                f"more than once.{where}",
                data={"source": name},
                fix_hint="declare one known gap per source",
            )
        seen.add(name)
        gaps.append({"source": name, "description": str(description).strip()})
    return {"known_gaps": gaps}


def _declared_sources(deployment: str) -> tuple[list[dict[str, str]], Optional[str]]:
    """`(rows, unavailable_reason)` for the deployment's declared sources.

    Each row carries BOTH names, because they are different strings and the
    difference is the main correctness trap here: the registry key is the
    `source_id` an operator writes (`git-files`), while every graph row, every
    watermark and every progress row is keyed by `ownership_id`
    (`okg-workspace.git-files`). Matching on the wrong one reports a fully
    ingested source as never ingested.
    """
    from okg.substrate.mcp.source_admin import _load_registry_yaml

    try:
        _path, raw = _load_registry_yaml(deployment)
    except Exception as exc:  # noqa: BLE001 - reported, never swallowed
        # A deployment resolved by directory path (the test/tmp shape) or a
        # deployment with no registry file still gets a manifest; it just
        # cannot classify against a declaration, and it SAYS that rather than
        # reporting every measured source as unregistered.
        return [], f"{type(exc).__name__}: {exc}"
    entries = raw.get("sources") or {}
    if not isinstance(entries, dict):
        return [], "source_registry.yaml `sources:` is not a mapping"
    rows: list[dict[str, str]] = []
    for source_id, entry in sorted(entries.items()):
        body = entry if isinstance(entry, dict) else {}
        rows.append(
            {
                "source_id": str(source_id),
                "ownership_id": str(body.get("ownership_id") or source_id),
            }
        )
    return rows, None


def _visible_source_ids(
    conn: psycopg.Connection,
    deployment: str,
    source_ids: Iterable[str],
    *,
    session: Any | None = None,
) -> set[str]:
    """The declared sources this session may see, through the shared gate.

    Imported rather than reimplemented: the source-visibility decision must
    have one definition, or an agent denied a source by `list_sources` could
    still learn of its existence from a coverage manifest.

    The gate is consulted once per declared source. That is one policy
    evaluation per source, and a deployment declares dozens at most, so the
    cost is bounded by the registry rather than by the graph. The per-source
    `conn.commit()` the shared gate used to force is NOT re-issued here; the
    decision writes it performs are committed once by the caller's connection
    state, not once per source.
    """
    from okg.substrate.mcp.source_lifecycle import _source_visible_for_subject

    return {
        source_id
        for source_id in source_ids
        # The session carries the subject the MCP boundary resolved -- the
        # request principal in multi-user mode (issue #2343).
        if _source_visible_for_subject(
            conn, deployment, source_id, session=session,
        )
    }


def _measured_rows(
    conn: psycopg.Connection,
    *,
    node_segment: str,
    branch_point_value: Any,
    with_spans: bool,
) -> list[dict[str, Any]]:
    """Per-source live-record counts at the pin, one aggregate.

    The predicate is the branch-point pair every other pinned read uses, so a
    later publish cannot change what a pinned session reports. The span columns
    ride on the SAME scan, which is why they are cheap: the aggregate already
    walks the live set once.

    **Pre-filter cardinality.** This is a whole-graph aggregate: it counts rows
    without asking whether this session may read them. That is correct ONLY
    when no row can be hidden. `inspect_coverage` therefore calls it only under
    `off`/`audit`, exactly as `inspect(graph)` restricts its own SQL aggregate
    (`_graph_summary_sql`); under `enforce` the counts come from
    `_measured_rows_enforced` or are refused outright.
    """
    span_sql = (
        f", min(attrs ->> '{RECENCY_ATTRIBUTE}') AS span_lo"
        f", max(attrs ->> '{RECENCY_ATTRIBUTE}') AS span_hi"
        f", count(*) FILTER (WHERE attrs ? '{RECENCY_ATTRIBUTE}') AS span_rows"
        if with_spans
        else ""
    )
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT source, count(*) AS live_record_count{span_sql}
              FROM {node_segment}
             WHERE deleted = FALSE
               AND live_lo <= %(bp)s AND %(bp)s < live_hi
             GROUP BY source
             ORDER BY source
            """,
            {"bp": branch_point_value},
        )
        return cur.fetchall()


class CoverageCountsUnavailable(SubstrateError):
    """Exact per-source counts cannot be produced under Nomos enforcement.

    Raised rather than answered with whole-graph numbers. Under `enforce` a
    raw `GROUP BY source` is PRE-FILTER cardinality, and publishing it would
    disclose how many rows each source holds — including sources and rows the
    policy hides. `inspect(graph)` refuses the same question the same way for
    the same reason; this is that refusal, per source.
    """

    code = "coverage_counts_unavailable"


def _measured_rows_enforced(
    conn: psycopg.Connection,
    session: Session,
    *,
    node_segment: str,
    branch_point_value: Any,
    with_spans: bool,
    row_budget: int,
) -> list[dict[str, Any]]:
    """Per-source counts over rows this session may ACTUALLY read.

    The `enforce` counterpart of `_measured_rows`. Hiding is a real per-row
    lineage decision under enforcement, so the rows are materialized, passed
    through the same `filter_node_rows_for_session` every other read uses, and
    counted in Python — the shape `_graph_summary_per_row` uses for the same
    reason. Beyond `row_budget` the per-row filter is the minutes/GBs blowup
    that budget exists to prevent, so the operation REFUSES instead of falling
    back to the unfiltered aggregate.
    """
    from okg.substrate.nomos.read_filter import filter_node_rows_for_session

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT node_id, subtype, attrs, text_hash, source,
                   source_record_id, source_revision
              FROM {node_segment}
             WHERE deleted = FALSE
               AND live_lo <= %(bp)s AND %(bp)s < live_hi
             ORDER BY node_id
             LIMIT %(cap)s
            """,
            {"bp": branch_point_value, "cap": row_budget + 1},
        )
        raw = cur.fetchall()
    if len(raw) > row_budget:
        raise CoverageCountsUnavailable(
            "the graph exceeds the exact-count budget under Nomos enforce "
            "mode, so per-source coverage cannot be measured without either "
            "an unaffordable per-row filter or disclosing pre-filter "
            "cardinality",
            data={
                "row_budget": row_budget,
                "deployment": session.deployment_name,
                "generation_id": session.generation_id,
            },
            fix_hint=(
                "narrow the question with search/filter, which stay exact at "
                "any size, or run coverage on a deployment whose enforcement "
                "mode is off/audit"
            ),
        )
    readable = filter_node_rows_for_session(conn, session, raw)

    grouped: dict[str, dict[str, Any]] = {}
    for row in readable:
        key = str(row.get("source") or "")
        bucket = grouped.setdefault(
            key,
            {
                "source": row.get("source"),
                "live_record_count": 0,
                "span_lo": None,
                "span_hi": None,
                "span_rows": 0,
            },
        )
        bucket["live_record_count"] += 1
        if not with_spans:
            continue
        value = (row.get("attrs") or {}).get(RECENCY_ATTRIBUTE)
        if not isinstance(value, str) or not value.strip():
            continue
        bucket["span_rows"] += 1
        # min/max over the same values the SQL aggregate would have taken, so
        # `_span_block`'s parse-and-validate contract is identical either way.
        if bucket["span_lo"] is None or value < bucket["span_lo"]:
            bucket["span_lo"] = value
        if bucket["span_hi"] is None or value > bucket["span_hi"]:
            bucket["span_hi"] = value
    return [grouped[key] for key in sorted(grouped)]


def _span_block(row: Mapping[str, Any]) -> dict[str, Any]:
    """The measured authored-time span, or an explicit reason it is not one.

    `min`/`max` over a JSONB text value is a LEXICAL ordering. That coincides
    with instant ordering for ISO-8601 strings sharing one offset form and does
    not otherwise, so both endpoints are parsed and the span is reported only
    when they agree; when they do not, the endpoints are dropped and the reason
    is named. A number a caller cannot tell apart from a measured one is worse
    than no number.
    """
    from okg.substrate.mcp.read_affordances import _parse_timestamp

    lo, hi = row.get("span_lo"), row.get("span_hi")
    rows = int(row.get("span_rows") or 0)
    if rows == 0:
        return {
            "attribute": RECENCY_ATTRIBUTE,
            "lo": None,
            "hi": None,
            "rows": 0,
            "ordering": "empty",
        }
    parsed_lo, parsed_hi = _parse_timestamp(lo), _parse_timestamp(hi)
    if parsed_lo is None or parsed_hi is None:
        return {
            "attribute": RECENCY_ATTRIBUTE,
            "lo": None,
            "hi": None,
            "rows": rows,
            "ordering": "unparseable_timestamp",
        }
    if (parsed_lo.utcoffset() is None) != (parsed_hi.utcoffset() is None):
        return {
            "attribute": RECENCY_ATTRIBUTE,
            "lo": None,
            "hi": None,
            "rows": rows,
            "ordering": "mixed_timezone_awareness",
        }
    return {
        "attribute": RECENCY_ATTRIBUTE,
        "lo": lo,
        "hi": hi,
        "rows": rows,
        # Named for what it IS: a text ordering whose endpoints were proved to
        # parse, not an instant-wise min/max over every value.
        "ordering": "lexical_iso8601",
    }


_RUN_COLUMNS = (
    "registry_source_name",
    "source_name",
    "last_run_id",
    "last_run_status",
    "last_run_mode",
    "last_run_completed_scope",
    "last_error",
    "last_reconcile_at",
    "frontier_state",
    "read_visibility_state",
    "record_set_state",
    "record_set_record_count",
)


def _run_rows(
    conn: psycopg.Connection, deployment: str,
) -> tuple[dict[str, dict[str, Any]], Optional[str]]:
    """`(by_source_name, unavailable_reason)` from the frontier-state view.

    This is the only relation in the schema carrying a per-source failure or
    incompleteness signal, and every field of it is derived from JSON inside
    `okg.source_progress.value` — there is no status column, no CHECK and no
    NOT NULL anywhere on that path. The caller labels the block accordingly.
    """
    columns = ", ".join(_RUN_COLUMNS)
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT {columns} FROM okg.v_source_frontier_state "
                f"WHERE deployment = %(deployment)s",
                {"deployment": deployment},
            )
            rows = cur.fetchall()
    except psycopg.Error as exc:
        # An older or partially-migrated deployment has no such view. Report
        # the block unavailable rather than dropping the whole inspect call.
        #
        # Roll back first. On a non-autocommit connection the failed statement
        # leaves the transaction aborted, and every later query in this
        # request — the counts, the parent resolution — would fail with
        # `current transaction is aborted` instead of running. Swallowing the
        # view error without clearing that state converts one missing view
        # into a whole broken response.
        try:
            conn.rollback()
        except psycopg.Error:
            pass
        return {}, f"{type(exc).__name__}: {exc}"
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        for key in (row.get("registry_source_name"), row.get("source_name")):
            if key:
                indexed.setdefault(str(key), dict(row))
    return indexed, None


def _admission_rows(
    conn: psycopg.Connection, deployment: str,
) -> tuple[dict[str, dict[str, Any]], Optional[str]]:
    """`(by_producer_or_source, unavailable_reason)` from admission envelopes.

    `v_source_frontier_state` is derived from `okg.source_progress`, which a
    REFUSED admission never writes. Answering "has this source ever produced
    anything?" from the run block alone therefore returns a false negative --
    `okg-workspace.latex_project` was refused on every run and read as
    `not_ingested` with `run.recorded: false`, indistinguishable from a source
    that had simply never been asked (issue #1323). Indexed by both
    `producer_id` and `source` because a registry entry may declare its own
    `admission_policy.producer_id`. `producer_id` is always indexed, since it
    is an exact identity; the `source` key is indexed only for a row whose
    `producer_kind` is the source's own, because `source` is not a
    source-owned column: entity merge, source retirement and lineage repair
    stamp a source's ownership id while owning `producer_kind: "system"`, and
    attributing their verdicts to the source would be the same misdirection
    this change removes. The guard lives in the indexing rather than in a SQL
    predicate so a source declaring some other producer kind still matches on
    its exact id.
    """
    from okg.substrate.mcp.source_lifecycle import (
        ADMISSION_ADVERSE_STATUSES,
    )

    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (producer_id, source)
                       producer_id, source, producer_kind, status, verdict,
                       GREATEST(created_at, updated_at) AS last_activity_at,
                       count(*) OVER (
                           PARTITION BY producer_id, source
                       ) AS envelope_count,
                       count(*) FILTER (
                           WHERE status = ANY(%(adverse)s)
                       ) OVER (
                           PARTITION BY producer_id, source
                       ) AS adverse_count
                  FROM okg.admission_envelopes
                 WHERE deployment_name = %(deployment)s
                 ORDER BY producer_id, source,
                          GREATEST(created_at, updated_at) DESC
                """,
                {
                    "adverse": list(ADMISSION_ADVERSE_STATUSES),
                    "deployment": deployment,
                },
            )
            rows = cur.fetchall()
    except psycopg.Error as exc:
        # Same discipline as `_run_rows`: clear the aborted transaction so one
        # missing relation does not break every later query in this request.
        try:
            conn.rollback()
        except psycopg.Error:
            pass
        return {}, f"{type(exc).__name__}: {exc}"
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        # `producer_id` is an exact identity and always indexed, so a source
        # declaring a non-`source` producer kind still matches. `source` is
        # indexed only for a row whose kind is the source's own.
        keys = [row.get("producer_id")]
        if str(row.get("producer_kind") or "") == "source":
            keys.append(row.get("source"))
        for key in keys:
            if not key:
                continue
            prior = indexed.get(str(key))
            if prior is None or str(prior.get("last_activity_at") or "") <= str(
                row.get("last_activity_at") or ""
            ):
                indexed[str(key)] = dict(row)
    return indexed, None


def _admission_block(
    admission: Optional[Mapping[str, Any]],
    unavailable: Optional[str],
) -> dict[str, Any]:
    """Admission outcome beside the run block, or why it is unknown."""
    from okg.substrate.mcp.source_admin import _admission_reason
    from okg.substrate.mcp.source_lifecycle import (
        ADMISSION_REFUSAL_STATUSES,
    )

    if unavailable is not None:
        # An unreadable envelope table is NOT evidence that nothing was
        # refused. Say so rather than reporting a clean admission history.
        return {"recorded": False, "unavailable": unavailable}
    if admission is None:
        return {"recorded": False, "unavailable": None}
    status = str(admission.get("status") or "")
    last_activity = admission.get("last_activity_at")
    return {
        "recorded": True,
        "unavailable": None,
        "status": status or None,
        # `blocked` and `quarantined` are adverse but were ADMITTED, so they
        # are counted without being called refusals.
        "refused": status in ADMISSION_REFUSAL_STATUSES,
        "reason_code": _admission_reason(admission.get("verdict"))[0],
        "envelope_count": int(admission.get("envelope_count") or 0),
        "adverse_count": int(admission.get("adverse_count") or 0),
        "last_activity_at": (
            last_activity.isoformat()
            if hasattr(last_activity, "isoformat")
            else last_activity
        ),
    }


def _run_block(
    row: Optional[Mapping[str, Any]],
    admission: Optional[Mapping[str, Any]] = None,
    admission_unavailable: Optional[str] = None,
) -> dict[str, Any]:
    admission_block = _admission_block(admission, admission_unavailable)
    if row is None:
        # `recorded: false` no longer stands alone: a refused admission writes
        # no progress row, so without the admission block beside it this reads
        # as "never asked to run" for a source that ran and was refused.
        return {"recorded": False, "admission": admission_block}
    return {**_recorded_run_block(row), "admission": admission_block}


def _recorded_run_block(row: Mapping[str, Any]) -> dict[str, Any]:
    # `recorded: false` (handled by the caller) GATES the rest, the way
    # `inspect(graph)` gates a block it did not compute. Spelling out eleven
    # nulls would say nothing more and would cost roughly half the byte budget
    # on a many-source deployment — turning the manifest into the
    # whole-payload refusal this shape exists to avoid.
    error = row.get("last_error")
    reconcile = row.get("last_reconcile_at")
    return {
        "recorded": True,
        "last_run_id": row.get("last_run_id"),
        "last_run_status": row.get("last_run_status"),
        "last_run_mode": row.get("last_run_mode"),
        # Tri-state on purpose: null means the run recorded no scope claim,
        # which is not the same as a run that claimed an incomplete scope.
        "last_run_completed_scope": row.get("last_run_completed_scope"),
        "last_error": (
            str(error)[:_ERROR_TEXT_CAP] if error is not None else None
        ),
        "last_reconcile_at": (
            reconcile.isoformat() if hasattr(reconcile, "isoformat")
            else reconcile
        ),
        "frontier_state": row.get("frontier_state"),
        "read_visibility_state": row.get("read_visibility_state"),
        "record_set_state": row.get("record_set_state"),
        "record_set_record_count": row.get("record_set_record_count"),
    }


_RUN_SEMANTICS = (
    "every run field is read from JSON inside okg.source_progress.value; the "
    "schema stores no run-outcome column, so a null last_error means no "
    "failure was RECORDED, never that the run succeeded. recorded:false means "
    "no progress row exists, which a REFUSED admission never writes -- read "
    "run.admission beside it, from okg.admission_envelopes, before concluding "
    "a source was never asked to run"
)
_COUNT_SEMANTICS_UNFILTERED = (
    "live_record_count is the whole-graph count of live nodes at this pin "
    "grouped by the stored `source` column. This deployment's Nomos "
    "enforcement mode is off or audit, neither of which can hide a row, so "
    "the aggregate and the per-row filter would return the same number"
)
_COUNT_SEMANTICS_ENFORCED = (
    "live_record_count counts only the live nodes at this pin that THIS "
    "session may read: under Nomos enforce mode every row went through the "
    "per-row lineage filter before it was counted. A raw aggregate is "
    "pre-filter cardinality and is never reported here, because publishing it "
    "would disclose the size of sources the policy hides"
)


def inspect_coverage(
    conn: psycopg.Connection,
    session: Session,
    select: Optional[Sequence[str]] = None,
    *,
    source_cap: int = COVERAGE_SOURCE_CAP,
) -> dict[str, Any]:
    """The per-source coverage manifest at the session's pinned generation."""
    from okg.substrate.bm25_node_search import branch_point
    from okg.substrate.mcp.envelope import validate_select
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment

    validate_select(
        list(select) if select is not None else None,
        COVERAGE_BLOCKS,
        tool_name="inspect(coverage)",
    )
    blocks = COVERAGE_BLOCKS if not select else frozenset(select)
    deployment = session.deployment_name

    all_declared, registry_unavailable = _declared_sources(deployment)
    visible = _visible_source_ids(
        conn,
        deployment,
        (row["source_id"] for row in all_declared),
        session=session,
    )
    declared = [row for row in all_declared if row["source_id"] in visible]
    # Keyed off the UNFILTERED declaration list, deliberately. This map is what
    # the unregistered pass below subtracts, and building it from the visible
    # list would make a policy-DENIED source fall through that pass and be
    # re-emitted as `unregistered` — complete with its ownership_id and live
    # record count. That is the precise existence leak the visibility gate
    # exists to close, arriving through the back door. A denied source must
    # produce NO row and contribute to NO tally: absence and denial have to be
    # indistinguishable to a least-privilege reader, which is the same rule
    # `inspect(node)` follows when it answers `exists: false` for a
    # source-denied node.
    by_ownership = {row["ownership_id"]: row for row in all_declared}

    # ALWAYS measured, whatever `select` asked for. Every row's `state` is
    # derived from this aggregate, so skipping it would report a fully
    # ingested source as `not_ingested` and drop unregistered sources
    # entirely — the exact false negative this whole target exists to
    # prevent. `select` therefore narrows which FIELDS a row carries (and
    # whether the span columns ride along on this one scan), never whether
    # the classification is measured at all.
    #
    # WHICH measurement runs is a policy question, not a performance one. A raw
    # `GROUP BY source` is pre-filter cardinality; under `enforce` publishing
    # it would disclose the size of sources the policy hides. So the branch
    # mirrors `inspect(graph)`: the cheap aggregate only where nothing can
    # hide, the per-row filtered count under enforcement, and a refusal rather
    # than a fallback when that filter is unaffordable.
    mode = deployment_nomos_enforcement_mode(deployment)
    node_segment = resolve_node_segment(conn)
    branch_point_value = branch_point(conn, session)
    if mode == "enforce":
        measured_rows = _measured_rows_enforced(
            conn,
            session,
            node_segment=node_segment,
            branch_point_value=branch_point_value,
            with_spans=_BLOCK_SPANS in blocks,
            row_budget=COVERAGE_ENFORCE_ROW_BUDGET,
        )
        visibility = "enforced"
    else:
        measured_rows = _measured_rows(
            conn,
            node_segment=node_segment,
            branch_point_value=branch_point_value,
            with_spans=_BLOCK_SPANS in blocks,
        )
        visibility = "unfiltered"
    measured: dict[str, dict[str, Any]] = {
        str(row["source"] or ""): row for row in measured_rows
    }

    runs: dict[str, dict[str, Any]] = {}
    runs_unavailable: Optional[str] = None
    admissions: dict[str, dict[str, Any]] = {}
    admissions_unavailable: Optional[str] = None
    if _BLOCK_RUNS in blocks:
        runs, runs_unavailable = _run_rows(conn, deployment)
        admissions, admissions_unavailable = _admission_rows(conn, deployment)

    gaps: dict[str, str] = {}
    gaps_unavailable: Optional[str] = None
    if _BLOCK_KNOWN_GAPS in blocks:
        gaps, gaps_unavailable = _declared_gaps(deployment)

    rows: list[dict[str, Any]] = []
    for entry in declared:
        source_id, ownership_id = entry["source_id"], entry["ownership_id"]
        hit = measured.get(ownership_id)
        count = int(hit["live_record_count"]) if hit is not None else 0
        row: dict[str, Any] = {
            "source_id": source_id,
            "ownership_id": ownership_id,
            "registered": True,
            "state": STATE_INCLUDED if count else STATE_NOT_INGESTED,
        }
        if _BLOCK_RECORDS in blocks:
            row["live_record_count"] = count
        if _BLOCK_SPANS in blocks:
            row["authored_at_span"] = (
                _span_block(hit) if hit is not None else None
            )
        if _BLOCK_RUNS in blocks:
            row["run"] = _run_block(
                runs.get(source_id) or runs.get(ownership_id),
                admissions.get(source_id) or admissions.get(ownership_id),
                admissions_unavailable,
            )
        if _BLOCK_KNOWN_GAPS in blocks:
            declaration = gaps.get(source_id) or gaps.get(ownership_id)
            # A declared gap is the DEPLOYMENT's claim, kept beside the
            # measurement rather than folded into it: it never changes the
            # measured count and it never becomes the row's only state.
            row["known_gap"] = (
                {"declared_by": "deployment.yaml", "description": declaration}
                if declaration is not None
                else None
            )
            if declaration is not None:
                row["declared_gap"] = True
        rows.append(row)

    # Records whose `source` no source declaration claims. Omitting them would
    # let a renamed source disappear from the corpus description while its rows
    # still answer queries.
    for source_value, hit in sorted(measured.items()):
        if source_value in by_ownership:
            continue
        row = {
            "source_id": None,
            "ownership_id": source_value or None,
            "registered": False,
            "state": STATE_UNREGISTERED,
        }
        if _BLOCK_RECORDS in blocks:
            row["live_record_count"] = int(hit["live_record_count"])
        if _BLOCK_SPANS in blocks:
            row["authored_at_span"] = _span_block(hit)
        if _BLOCK_RUNS in blocks:
            row["run"] = _run_block(
                runs.get(source_value),
                admissions.get(source_value),
                admissions_unavailable,
            )
        if _BLOCK_KNOWN_GAPS in blocks:
            row["known_gap"] = None
        rows.append(row)

    # A gap declared for a source no registry entry and no record names is
    # itself a finding: the deployment is disclosing something about a source
    # this graph does not have.
    if _BLOCK_KNOWN_GAPS in blocks:
        named = {row.get("source_id") for row in rows} | {
            row.get("ownership_id") for row in rows
        }
        for source_id, description in sorted(gaps.items()):
            if source_id in named:
                continue
            rows.append(
                {
                    "source_id": source_id,
                    "ownership_id": None,
                    "registered": False,
                    "state": STATE_DECLARED_GAP,
                    "declared_gap": True,
                    "known_gap": {
                        "declared_by": "deployment.yaml",
                        "description": description,
                    },
                    **({"live_record_count": 0} if _BLOCK_RECORDS in blocks else {}),
                    **({"authored_at_span": None} if _BLOCK_SPANS in blocks else {}),
                    **(
                        {
                            "run": _run_block(
                                None,
                                admissions.get(source_id),
                                admissions_unavailable,
                            )
                        }
                        if _BLOCK_RUNS in blocks
                        else {}
                    ),
                }
            )

    sources_total = len(rows)
    truncated = sources_total > source_cap
    payload: dict[str, Any] = {
        "deployment_name": deployment,
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
        "catalog_version_id": session.catalog_version_id,
        "blocks": sorted(blocks),
        "sources": rows[:source_cap],
        "sources_total": sources_total,
        "source_cap": source_cap,
        "truncated": truncated,
        "total_count": sources_total,
        "states": {
            state: sum(1 for row in rows if row["state"] == state)
            for state in (
                STATE_INCLUDED,
                STATE_NOT_INGESTED,
                STATE_UNREGISTERED,
                STATE_DECLARED_GAP,
            )
        },
        "registry_available": registry_unavailable is None,
        "visibility": visibility,
        "nomos_enforcement_mode": mode,
        "semantics": {
            "counts": (
                _COUNT_SEMANTICS_ENFORCED
                if visibility == "enforced"
                else _COUNT_SEMANTICS_UNFILTERED
            ),
            "states": (
                "included/not_ingested are measured at the pin against the "
                "deployment source registry; unregistered means records carry "
                "a source no registry entry declares; declared_gap is the "
                "deployment's own disclosure, not a measurement"
            ),
        },
    }
    if registry_unavailable is not None:
        payload["registry_unavailable_reason"] = registry_unavailable
    if _BLOCK_RUNS in blocks:
        payload["run_semantics"] = _RUN_SEMANTICS
        payload["runs_available"] = runs_unavailable is None
        if runs_unavailable is not None:
            payload["runs_unavailable_reason"] = runs_unavailable
    if _BLOCK_KNOWN_GAPS in blocks:
        payload["known_gaps_available"] = gaps_unavailable is None
        if gaps_unavailable is not None:
            payload["known_gaps_unavailable_reason"] = gaps_unavailable
    return payload


def _declared_gaps(deployment: str) -> tuple[dict[str, str], Optional[str]]:
    """`({source: description}, unavailable_reason)` from deployment.yaml.

    A deployment that cannot be RESOLVED (no manifest on this checkout, a
    directory-only fixture) is a reportable unavailability. A deployment whose
    `coverage:` block does not PARSE is not: that is a malformed disclosure,
    and degrading it to `known_gaps_available: false` would turn a loud
    configuration error into a quiet "no gaps declared" — which reads as the
    deployment asserting full coverage it never asserted. So
    `CoverageConfigInvalid` is deliberately raised past this handler.
    """
    try:
        from okg.substrate.deployments import resolve_deployment

        manifest = resolve_deployment(deployment)
    except CoverageConfigInvalid:
        # The manifest loader validates the block eagerly; let its refusal out.
        raise
    except Exception as exc:  # noqa: BLE001 - reported, never swallowed
        return {}, f"{type(exc).__name__}: {exc}"
    # Outside the try: a parse refusal here must reach the caller, not be
    # recorded as an unavailable block.
    config = parse_coverage_config(getattr(manifest, "coverage", None))
    if config is None:
        return {}, None
    return {gap["source"]: gap["description"] for gap in config["known_gaps"]}, None
