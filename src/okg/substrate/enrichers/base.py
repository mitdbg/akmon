"""Substrate enricher Protocol.

An `Enricher` is a substrate-library module that derives
`edge_facts` from the current Chronos graph checkout. The publisher
calls each registered enricher after source facts have been applied
and before derived facts are checkpointed, so the enricher's emitted
facts land in the same published graph generation as source-runner
edges.

Contract:

  - `name` — short identifier, used in logging + telemetry.
  - `reads_subtypes` — subtypes the enricher consumes;
    deployment validation refuses to register an enricher
    whose `reads_subtypes` ⊄ deployment's loaded subtypes.
  - `emits_edge_types` — edge_types the enricher produces;
    must be in the active narrowings whitelist (or pass
    through if the deployment runs without strict narrowings).
  - `enrich(conn, generation_id)` — open the supplied
    connection, write rows to `okg.edge_facts` (op='I',
    provenance='derived_deterministic' or 'extracted_model'),
    return an `EnrichResult` with counts.
  - `targeted_backfill_chunk_safe` — optional opt-in flag for
    `okg enricher backfill`. Setting this to True is a stronger
    contract than accepting `IncrementalContext`: the enricher
    must strictly bound both work and emitted edge facts to the
    supplied `incremental.dirty_node_ids`.
  - `atomic_publish_transaction_compatible` — optional, explicit
    opt-in for caller-verified publication. A compatible enricher
    must treat the supplied connection as caller-owned: it must not
    commit, roll back, toggle autocommit, or construct a helper that
    takes transaction ownership. Missing or false refuses atomic
    publication before the cycle starts.

Idempotence: enrichers SHOULD use deterministic `dedupe_key`
values for the facts they emit and pre-filter existing keys
before inserting. If the derived fact is stable across clean
publishes, the key should be stable across clean publishes too;
otherwise the derived-fact apply step will churn the same logical
edge by closing and reopening it in the graph.
Edge_facts is a Timescale hypertable with UNIQUE
(dedupe_key, observed_at).

Failure handling: an enricher that raises is logged but does
not abort the publish. The publisher's job is to project the
deployment's source-of-truth state; derived edges are best-
effort. (A repeatedly-failing enricher should be removed from
the deployment's `enrichers:` block.)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol

import psycopg


__all__ = [
    "EnrichResult",
    "Enricher",
    "EnricherPrelockPlan",
    "IncrementalContext",
]


@dataclass(frozen=True)
class IncrementalContext:
    """Per-enricher incremental dispatch context.

    Per `add-substrate-incremental-publish` (2026-04-28): the
    publisher reads each enricher's
    `okg.enricher_progress(deployment_name, enricher_name)` row
    before invocation, computes the dirty-set per the spec
    contract, and passes this context. The enricher MAY use
    it to scope its main scan with `WHERE node_id =
    ANY(dirty_node_ids)` for small dirty sets, or fall back
    to its full-table scan when `is_first_run` is True or
    `len(dirty_node_ids) > 10_000`.

    Branching contract (also documented in the spec):

      1. `is_first_run is True` OR
         `len(dirty_node_ids) > 10_000` →
         fall back to full-table scan. Above ~10k ids the
         per-row output cost of `ANY(...)` matches or exceeds
         the streaming scan, and above ~100k ids Postgres
         flips the plan to Seq Scan anyway.

      2. `dirty_node_ids` is empty AND not first-run →
         return early with `EnrichResult(n_edges_emitted=0)`.

      3. Otherwise → scope main scan with `WHERE node_id =
         ANY(%(dirty)s)`.

    Fields:

      - `prev_fact_watermark` — `BIGSERIAL fact_id` from
        `okg.node_facts`; the publisher advances this after
        each successful enricher call.
      - `prev_edge_watermark` — `BIGSERIAL fact_id` from
        `okg.edge_facts` (NOT the hash-derived `edge_id`).
      - `dirty_node_ids` — frozenset of node_ids the
        enricher SHOULD process. Drawn from `node_facts
        WHERE fact_id > prev` on normal runs; from
        the graph checkout on first-run
        / forced-rescan / stale-watermark cases (sidesteps
        the 180-day node_facts retention).
      - `is_first_run` — True iff the publisher computed
        dirty from the graph checkout (signals fall-back to
        full-table scan).
      - `last_status` — prior progress status. Publisher-level
        clean skips only consider prior `success` rows.
      - `current_*_watermark` — max fact ids visible before this
        enricher invocation.
      - `prev_catalog_version_id` / `current_catalog_version_id`
        — catalog identity guard for clean skips.
      - `dirty_edge_ids` — Chronos-native edge ids changed in
        the current branch segment. Legacy fact-log mode leaves
        this empty.
      - `dirty_node_attr_changes_by_subtype` — optional
        Chronos-native field-delta hints keyed by
        subtype → node_id → changed attr names. ``None`` means
        the publisher could not prove field-level dirtiness, so
        enrichers must treat relevant dirty nodes as unknown.
      - `change_basis` — diagnostic label for the dirty-set
        source, e.g. `fact_watermark` or `chronos_segment`.
    """

    prev_fact_watermark: int
    prev_edge_watermark: int
    dirty_node_ids: frozenset[str]
    is_first_run: bool
    last_status: str = "never"
    current_fact_watermark: int = 0
    current_edge_watermark: int = 0
    prev_catalog_version_id: int | None = None
    current_catalog_version_id: int | None = None
    dirty_edge_ids: frozenset[int] = field(default_factory=frozenset)
    dirty_node_attr_changes_by_subtype: dict[
        str, dict[str, frozenset[str]]
    ] | None = None
    change_basis: str = "fact_watermark"


@dataclass(frozen=True)
class EnricherPrelockPlan:
    """The edge-fact rows an enricher would emit, computed before the lock.

    Issue 2941 fix 1. An enricher that sets ``prelock_plannable = True``
    offers ``prelock_plan(conn, *, generation_id, incremental)``: it runs
    its resolve step and its own "already said this" check, writes nothing,
    and returns this plan. Under the publish lock the publisher passes the
    plan back to ``enrich(..., prelock_plan=plan)`` only when the graph
    fingerprint and the enricher's ``IncrementalContext`` are unchanged; the
    enricher then runs its "already said this" check again on
    ``candidate_rows`` under the lock and inserts what that check returns
    (stamping ``observed_at`` at insert time) instead of resolving again.
    ``rows`` is the pre-lock answer of that check, a forecast the
    derived-fact apply prepares from.

    ``rows`` use the linkers' emit layout: ``(edge_id, src, dst, edge_type,
    attrs_json, source_record_id_json, source_revision_json, observed_at,
    dedupe_key, run_id)``.
    """

    enricher_name: str
    generation_id: int
    incremental: IncrementalContext | None
    rows: tuple[tuple[Any, ...], ...]
    candidate_rows: tuple[tuple[Any, ...], ...]
    n_skipped: int
    run_id: str | None
    provenance: str = "derived_deterministic"
    confidence: float = 1.0
    timings_ms: Mapping[str, float] = field(default_factory=dict)
    # What the linker's resolve step read, for work it must finish under
    # the lock from fresh state (code_reference's stale-declaration
    # retractions, item 92a). None for linkers without such a step.
    linker_state: Any = None


@dataclass
class EnrichResult:
    """Outcome of one `Enricher.enrich()` call."""

    enricher_name: str
    n_edges_emitted: int
    n_edges_skipped: int = 0
    error: Optional[str] = None
    output_lineage: Optional[Mapping[str, Any]] = None
    timings_ms: Mapping[str, float] = field(default_factory=dict)
    # Per `sanitize-fact-text-at-admission` (issue #638): the enricher
    # emit path applies the shared NUL normalizer; these carry the run's
    # loud totals (0 on clean runs). A rejected emission is NOT counted
    # in n_edges_emitted.
    sanitized_value_count: int = 0
    rejected_fact_count: int = 0
    # P0 no-silent-data-loss: True when the run did NOT fully process
    # its input delta — a per-item LLM call failed, or a candidate
    # failed to emit. The publisher MUST NOT advance the enricher's
    # watermark past the unprocessed inputs (else their typed nodes are
    # silently, permanently lost); it holds the watermark so the next
    # cycle re-reads and retries them. Defaults False, so enrichers that
    # don't set it keep advancing on a clean run exactly as before.
    incomplete: bool = False
    # Count of per-item input (LLM-call) failures folded into
    # `incomplete`; surfaced for observability and run records.
    n_input_failures: int = 0
    # Count of per-item inputs the enricher permanently gave up on this run
    # after exhausting a bounded retry budget (e.g. a figure the VLM always
    # empties on). These do NOT set `incomplete` (the watermark must advance
    # past them), so this field is the visible signal that a P0-adjacent
    # "gave up on an input" happened — otherwise only discoverable by querying
    # the enricher's failure ledger. 0 on runs that quarantine nothing.
    n_quarantined: int = 0
    # Count of the enricher's OWN previously-published nodes it retracted this
    # run because a replacement now supersedes them. A retraction deletes live
    # graph state, so its size is the one number an operator needs to tell a
    # routine re-caption from a mass supersession; without it the deletion is
    # only visible after the fact in the publish delta. 0 on runs that retract
    # nothing, which is every enricher that never retracts.
    n_retracted: int = 0


class Enricher(Protocol):
    name: str
    reads_subtypes: tuple[str, ...]
    reads_edge_types: tuple[str, ...]
    emits_edge_types: tuple[str, ...]
    # Optional maintained-derivation lineage declaration. Older enrichers
    # derive this from reads_subtypes / reads_edge_types; missing selectors
    # force a loud full rerun in the publisher's lineage adapter.
    lineage_dependencies: Mapping[str, Any]
    # Per `add-enricher-required-narrowings` (2026-04-26):
    # enrichers declare every (parent_subtype, edge_type,
    # child_subtype) triple they emit so Publisher init can
    # validate against the active catalog's narrowings and
    # fail loudly at boot rather than mid-projection. Default
    # `()` means "the enricher's emit semantics are not yet
    # declared" — backwards-compat for older enrichers; the
    # validator emits a deprecation note for those.
    requires_narrowings: tuple[tuple[str, str, str], ...]
    # Per `sweep-enricher-liveness` (issue #707): the declared-liveness
    # escape (audit design point (i)). An enricher that runs successfully
    # but is legitimately expected to emit nothing (a genuinely dormant
    # producer) sets `dormant_ok = True` so the console `wired-but-silent`
    # health flag and `okg doctor --check enricher-liveness` suppress it by
    # DECLARATION rather than by silence. Absent (the default False), a
    # wired enricher that has never emitted a single edge since wiring is
    # flagged — that is the #707 blind spot the flag exists to catch.
    dormant_ok: bool
    # Caller-verified publication is fail-closed. This flag is deliberately
    # not implied by accepting a connection: older enrichers may construct a
    # default-autocommit Chronos store on it and thereby commit the outer
    # publication transaction. Only audited implementations set this True.
    atomic_publish_transaction_compatible: bool

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        incremental: Optional[IncrementalContext] = None,
    ) -> EnrichResult:
        raise NotImplementedError
