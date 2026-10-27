"""Publish-stage running-sum maintenance for the console count rollup.

The operator console's count endpoints (the generations panel, the ontology
graph's per-subtype sizing, the per-source sidebar, the per-subtype node count,
the knowledge top-15, the health overview) each express a count as a raw
aggregate over the generation-pinned graph -- a ``count(*)`` / ``GROUP BY`` that
re-scans the ~1M-node reconstructed Chronos graph on every read (the worst,
``generations.py``, runs 12 checkouts x 2 full-graph scans -> >280s).

This module maintains those counts at publish time, into a Chronos-registered
table (``okg.console_count_rollup``), so the publish cost is O(changed) between
forcing events and a read is an O(1) as-of-generation stab. It is the second
maintained derivation registered on the shared kit
(``okg.substrate.derived_artifacts.maintained_derivation``), parallel to the
ontology rollup, with ZERO kit changes (#444 slice 2 / ``console-count-rollup``).

Seven dimensions, one compact table
-----------------------------------
The table carries per-``(dimension_kind, dimension_value)`` counts in the seven
dimensions the console reads:

* ``subtype``        -- nodes grouped by ``graph_nodes.subtype``;
* ``edge_type``      -- edges grouped by ``graph_edges.edge_type``;
* ``source``         -- nodes grouped by ``graph_nodes.source`` (``source IS NOT NULL``);
* ``edge_source``    -- edges grouped by ``graph_edges.source`` (``source IS NOT NULL``);
* ``enricher_node``  -- nodes grouped by their enricher producer (ENRICHER_PRODUCER_SQL_EXPR);
* ``enricher_edge``  -- edges grouped by their enricher producer (same attribution);
* ``source_subtype`` -- nodes grouped by ``(producer, subtype)`` (issue #446: the
  operator Graph view's producer "emits" edges), producer keyed IDENTICALLY to the
  ontology rollup's edge producer (NODE_PRODUCER_SQL_EXPR), value encoded
  ``<producer>\\x1f<subtype>``, domain == the ``source`` dimension's
  (``source IS NOT NULL``, the unattributed bucket included) so the two
  dimensions are present-or-absent together (what the shape-upgrade backfill
  predicate relies on).

The TOTALS dimension (node_count / edge_count) is NOT here -- it is served O(1)
from checkpoint metadata by ``accelerate-operator-read-path``'s
``generation_counts``; this rollup CONSUMES that for totals and adds only the
seven dimensions above (no totals re-derivation).

How the +1/-1 sign is recovered (mirrors the ontology rollup)
-------------------------------------------------------------
The cycle's changed-node / changed-edge ids carry no insert/retract sign. We
recover the sign by PRESENCE at the current branch head:

* a changed node/edge LIVE in the current checkout contributes +1 to its CURRENT
  bucket(s);
* a changed node/edge ABSENT in the current checkout (a delete) contributes -1 to
  the bucket(s) it had at the PRIOR published generation's checkpoint;
* an unchanged-but-touched node/edge (live in both, e.g. an attrs-only change)
  emits +1 (current) and -1 (prior) against the SAME bucket -> nets to zero.

We sum these per ``(dimension_kind, dimension_value)``, add the net deltas onto
the prior rollup rows (carried forward on the branch by Chronos), upsert the new
counts, and DELETE any row whose count reaches zero (so absence == zero).

The forcing set is WIDER than the ontology rollup's
---------------------------------------------------
The presence-based +1/-1 above can only MOVE a bucket when the moving item is in
the changed set under BOTH its old and new key. That holds for an edge-type
change (the edge id is stable, so the +1-current / -1-prior legs re-key it), but
NOT for a node SUBTYPE change or a node SOURCE reattribution: those move the
node's by-subtype / by-source bucket while the node id is unchanged, so the
presence-based legs would see +1(new) and -1(prior) -- which IS correct here
because the node id is in the changed set under both... but recovering the prior
key per node is exactly the fragile move-arithmetic the ontology rollup refuses
to do incrementally. So we FORCE a full recount on any node retype or source
reattribution (both rare ontology/attribution churn), making the by-subtype and
by-source fold legs effectively add/delete-only. The by-edge-type leg owns its
move via the fold (-1 old / +1 new); its force predicate is a belt for the rare
type-churn cycle (kept until the bench proves the fold alone is byte-exact).

A full recount is therefore forced on: first publish / when forced / scope
unavailable / node retype / source reattribution / edge-type change / every Nth
generation (periodic self-heal). All but the last three are the kit's built-in
precedence; the three middle ones are this rollup's ``ForcePredicate`` s.

Self-heal + drift oracle
------------------------
``COUNT_RECOMPUTE_SQL`` is the byte-equality CORRECTNESS oracle AND the
full-recount / self-heal source (one SQL, two roles). A running sum can silently
drift; the periodic self-heal (every ``SELF_HEAL_EVERY_N_GENERATIONS``
generations, driven off the sentinel's carried-forward cadence counter) bounds
any drift to at most that many generations.
"""
from __future__ import annotations

from typing import Any, NamedTuple

from okg.substrate.derived_artifacts.maintained_derivation import (
    FULL_REASON_DIMENSION_SHAPE_UPGRADE,
    FULL_REASON_LARGE_CHANGE_SET,
    FULL_REASON_SCOPE_UNAVAILABLE,
    ChronosRegisteredTable,
    CorrectnessOracle,
    DerivationSpec,
    ForceInputs,
    ForcePredicate,
    MaintainContext,
    MaintainResult,
    SentinelSpec,
    is_large_change_set,
    is_sentinel_row,
    maintain,
    read_cadence_counter,
    read_self_heal_cadence,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    COUNT_KIND_EDGE_SOURCE as KIND_EDGE_SOURCE,
    COUNT_KIND_EDGE_TYPE as KIND_EDGE_TYPE,
    COUNT_KIND_ENRICHER_EDGE as KIND_ENRICHER_EDGE,
    COUNT_KIND_ENRICHER_NODE as KIND_ENRICHER_NODE,
    COUNT_KIND_SOURCE as KIND_SOURCE,
    COUNT_KIND_SOURCE_SUBTYPE as KIND_SOURCE_SUBTYPE,
    COUNT_KIND_SUBTYPE as KIND_SUBTYPE,
    ENRICHER_PREFIX,
    ENRICHER_PRODUCER_FILTER_SQL,
    ENRICHER_PRODUCER_SQL_EXPR,
    PUBLISHER_DERIVED_SOURCE,
    SOURCE_SUBTYPE_SEP,
    UNATTRIBUTED_DERIVED_PRODUCER,
    encode_source_subtype,
)
from okg.substrate.derived_artifacts.derivation_registry import (
    DeclaredDeps,
    DerivationRow,
    DeterminismClass,
    ExecutionPhase,
    FailureMode,
    GovernanceProfile,
    Materialization,
)
from okg.substrate.graph_store import (
    CONSOLE_COUNT_ROLLUP_TABLE,
    DEFAULT_BRANCH,
)

# Periodic self-heal cadence (defense-in-depth): force a FULL recount every N
# generations so any unforeseen drift source is bounded to at most N generations.
# Resolved once at import via the SHARED kit helper, so read and write paths use
# ONE cadence reader. Overridable via OKG_CONSOLE_COUNT_ROLLUP_SELF_HEAL_EVERY (a
# positive integer; non-integer / non-positive values warn and fall back).
SELF_HEAL_EVERY_N_GENERATIONS = read_self_heal_cadence(
    "OKG_CONSOLE_COUNT_ROLLUP_SELF_HEAL_EVERY", default=25
)

# Issue 3955: above this share of the prior node + edge count (and the kit's
# 10,000-id floor) the cycle recounts in full instead of resolving every
# changed id at two checkouts. Measured on a 242k-node, 339k-edge synthetic
# graph after a bulk load (two repeats): the two id-list reads cost 0.09-0.14 s
# at 1 % changed and 0.61-0.64 s at 5 %, against 0.55 s for one full recount,
# so the crossover is just under 5 %. Not measured at a 4.8M-edge scale.
LARGE_CHANGE_SET_PERCENT = 5

# The dimension-kind discriminators (a closed set; the read consumes only the
# kind an offender needs) come from the import-light single source so the maintain +
# read paths cannot drift -- re-exported here under their KIND_* names.


# SQL expression computing a NODE's producer key for the ``source_subtype``
# dimension. Keyed IDENTICALLY to the ontology rollup's edge producer
# (``publisher/ontology_rollup._PRODUCER_EXPR``) so one real producer can never
# split into two keys across the two artifacts the Graph view joins (emits vs
# connects): a direct ``_enricher:%`` source verbatim (edges reach the same key
# via that expr's ELSE arm), a ``_publisher_derived`` row with a non-empty
# recovered linker as ``_enricher:<linker>`` (NO rule_id suffix — deliberately
# coarser than ``ENRICHER_PRODUCER_SQL_EXPR``, which keeps rule_id granularity
# for enricher diagnostics), a ``_publisher_derived`` row without one as the
# explicit unattributed bucket, any other source verbatim. Callers pair it with
# ``source IS NOT NULL``. The key parity is enforced by
# tests/substrate/test_console_count_rollup.py::
# test_source_subtype_producer_key_parity_with_ontology_rollup.
NODE_PRODUCER_SQL_EXPR: str = (
    "CASE "
    f"WHEN source LIKE '{ENRICHER_PREFIX}%' THEN source "
    f"WHEN source = '{PUBLISHER_DERIVED_SOURCE}' "
    "AND NULLIF(source_record_id->>'linker', '') IS NOT NULL "
    f"THEN '{ENRICHER_PREFIX}' || (source_record_id->>'linker') "
    f"WHEN source = '{PUBLISHER_DERIVED_SOURCE}' "
    f"THEN '{UNATTRIBUTED_DERIVED_PRODUCER}' "
    "ELSE source "
    "END"
)

# Full per-dimension recompute over the WHOLE pinned-generation graph. BOTH the
# self-heal / full-recount source AND the correctness oracle the byte-equality
# test forces. NULL handling + ``count(*)`` are kept byte-identical to each
# cut-over read SQL (the slice-1 lesson): the subtype arm has NO ``IS NOT NULL``
# filter (subtype is NOT NULL in the schema and the offender has none); the source
# arm uses ``count(*)`` (== ``count(DISTINCT node_id)`` because node_id is the PK)
# with ``source IS NOT NULL`` (matching the offender); the edge_type arm filters
# ``IS NOT NULL`` for safety (edge_type is NOT NULL, so vacuous, but explicit).
COUNT_RECOMPUTE_SQL = f"""
SELECT '{KIND_SUBTYPE}' AS dimension_kind,
       n.subtype        AS dimension_value,
       count(*)::bigint AS item_count
  FROM okg.graph_nodes n
 GROUP BY n.subtype
UNION ALL
SELECT '{KIND_EDGE_TYPE}', e.edge_type, count(*)::bigint
  FROM okg.graph_edges e
 WHERE e.edge_type IS NOT NULL
 GROUP BY e.edge_type
UNION ALL
SELECT '{KIND_SOURCE}', n.source, count(*)::bigint
  FROM okg.graph_nodes n
 WHERE n.source IS NOT NULL
 GROUP BY n.source
UNION ALL
SELECT '{KIND_EDGE_SOURCE}', e.source, count(*)::bigint
  FROM okg.graph_edges e
 WHERE e.source IS NOT NULL
 GROUP BY e.source
UNION ALL
SELECT '{KIND_ENRICHER_NODE}', {ENRICHER_PRODUCER_SQL_EXPR}, count(*)::bigint
  FROM okg.graph_nodes
 WHERE {ENRICHER_PRODUCER_FILTER_SQL}
 GROUP BY {ENRICHER_PRODUCER_SQL_EXPR}
UNION ALL
SELECT '{KIND_ENRICHER_EDGE}', {ENRICHER_PRODUCER_SQL_EXPR}, count(*)::bigint
  FROM okg.graph_edges
 WHERE {ENRICHER_PRODUCER_FILTER_SQL}
 GROUP BY {ENRICHER_PRODUCER_SQL_EXPR}
UNION ALL
SELECT '{KIND_SOURCE_SUBTYPE}',
       ({NODE_PRODUCER_SQL_EXPR}) || '{SOURCE_SUBTYPE_SEP}' || subtype,
       count(*)::bigint
  FROM okg.graph_nodes
 WHERE source IS NOT NULL
 GROUP BY 2
"""

# Resolve the by-subtype + by-source bucket(s) a set of NODE ids belongs to AT A
# PINNED GENERATION. One row per node live at the pinned generation (a
# tombstoned/absent node produces no row). NULL source contributes to no source
# bucket (matching the recompute's ``source IS NOT NULL``).
_NODE_DIMS_SQL = f"""
SELECT node_id, subtype, source, ({ENRICHER_PRODUCER_SQL_EXPR}) AS enricher,
       ({NODE_PRODUCER_SQL_EXPR}) AS producer
FROM okg.graph_nodes
WHERE node_id = ANY(:node_ids)
"""

# Resolve the by-edge-type + by-source + by-enricher buckets a set of EDGE ids
# belongs to AT A PINNED GENERATION. One row per edge live at the pinned generation.
_EDGE_DIMS_SQL = f"""
SELECT edge_id, edge_type, source, ({ENRICHER_PRODUCER_SQL_EXPR}) AS enricher
FROM okg.graph_edges
WHERE edge_id = ANY(:edge_ids)
"""

# The prior rollup rows currently visible at the branch head (carried forward).
# The running sum adds its net deltas onto these. The sentinel is excluded here
# (it is rewritten unconditionally by the kit driver).
_CURRENT_ROLLUP_SQL = f"""
SELECT dimension_kind, dimension_value, item_count
FROM {CONSOLE_COUNT_ROLLUP_TABLE}
"""


BucketKey = tuple[str, str]


def _bucket_key(row: dict[str, Any]) -> BucketKey:
    return (str(row["dimension_kind"]), str(row["dimension_value"]))


def _row_for(key: BucketKey, count: int) -> dict[str, Any]:
    kind, value = key
    return {"dimension_kind": kind, "dimension_value": value, "item_count": int(count)}


def _key_dict(key: BucketKey) -> dict[str, Any]:
    kind, value = key
    return {"dimension_kind": kind, "dimension_value": value}


def _full_recount_rows(checkout: Any) -> dict[BucketKey, int]:
    rows: dict[BucketKey, int] = {}
    for row in checkout.query(COUNT_RECOMPUTE_SQL):
        key = _bucket_key(row)
        if key[0] == KIND_SOURCE_SUBTYPE and key[1].count(SOURCE_SUBTYPE_SEP) != 1:
            # The SQL arm concatenates producer || 0x1f || subtype and cannot
            # raise on its own; guard here so a subtype (or producer) that
            # somehow contains the separator fails LOUDLY on the full-recount
            # leg too — the incremental leg's encode_source_subtype guard
            # alone would let a full recount write an ambiguous value the
            # oracle then agrees with (a mis-parse rendered as truth).
            raise ValueError(
                f"ambiguous source_subtype dimension_value {key[1]!r}: "
                "expected exactly one 0x1f separator"
            )
        rows[key] = int(row["item_count"] or 0)
    return rows


def _current_rollup_rows(checkout: Any) -> dict[BucketKey, int]:
    rows: dict[BucketKey, int] = {}
    for row in checkout.query(_CURRENT_ROLLUP_SQL):
        if is_sentinel_row(row, _COUNT_SENTINEL):
            continue
        rows[_bucket_key(row)] = int(row["item_count"] or 0)
    return rows


def _changed_buckets(
    checkout: Any, node_ids: list[str], edge_ids: list[int]
) -> dict[BucketKey, int]:
    """Count how many of the changed nodes/edges fall in each (kind, value) bucket
    AT THIS checkout, across all seven dimensions. A node contributes to its
    by-subtype bucket always, and to its by-source / by-(producer, subtype)
    buckets iff ``source IS NOT NULL``; an edge contributes to its by-edge-type
    bucket always and its by-source bucket iff ``source IS NOT NULL``."""
    counts: dict[BucketKey, int] = {}
    if node_ids:
        for row in checkout.query(_NODE_DIMS_SQL, {"node_ids": node_ids}):
            subtype = row["subtype"]
            if subtype is not None:
                key = (KIND_SUBTYPE, str(subtype))
                counts[key] = counts.get(key, 0) + 1
            source = row["source"]
            if source is not None:
                key = (KIND_SOURCE, str(source))
                counts[key] = counts.get(key, 0) + 1
            enricher = row["enricher"]
            if enricher is not None:
                key = (KIND_ENRICHER_NODE, str(enricher))
                counts[key] = counts.get(key, 0) + 1
            producer = row["producer"]
            if producer is not None and subtype is not None:
                # encode_source_subtype raises LOUDLY on a subtype containing the
                # separator — never a silent mis-parse rendered as truth.
                key = (
                    KIND_SOURCE_SUBTYPE,
                    encode_source_subtype(str(producer), str(subtype)),
                )
                counts[key] = counts.get(key, 0) + 1
    if edge_ids:
        for row in checkout.query(_EDGE_DIMS_SQL, {"edge_ids": edge_ids}):
            edge_type = row["edge_type"]
            if edge_type is not None:
                key = (KIND_EDGE_TYPE, str(edge_type))
                counts[key] = counts.get(key, 0) + 1
            source = row["source"]
            if source is not None:
                key = (KIND_EDGE_SOURCE, str(source))
                counts[key] = counts.get(key, 0) + 1
            enricher = row["enricher"]
            if enricher is not None:
                key = (KIND_ENRICHER_EDGE, str(enricher))
                counts[key] = counts.get(key, 0) + 1
    return counts


class _CountChange(NamedTuple):
    """The console count rollup's opaque per-cycle change descriptor.

    The kit never inspects this -- only this rollup's force predicates
    (the three move flags) and its incremental callback (the changed id lists)
    interpret it.
    """

    node_subtype_changed: bool
    source_reattributed: bool
    edge_type_changed: bool
    changed_node_ids: list[str]
    changed_edge_ids: list[int] | None


# The console count rollup declared as a maintained derivation: deterministic (a
# pure function of the sealed generation's graph), Chronos-registered storage, and
# THREE force predicates -- a node retype / source reattribution / edge-type change
# each moves a bucket; the first two are force-owned, the third is a belt over the
# fold's own move handling.
_COUNT_PK = ("dimension_kind", "dimension_value")
_COUNT_SENTINEL = SentinelSpec(pk_columns=_COUNT_PK, cadence_column="item_count")
_COUNT_STORAGE = ChronosRegisteredTable(
    table=CONSOLE_COUNT_ROLLUP_TABLE,
    ensure_fn=lambda store: store.ensure_console_count_rollup_table(),
)
_COUNT_ORACLE = CorrectnessOracle(
    COUNT_RECOMPUTE_SQL, key_columns=_COUNT_PK, value_columns=("item_count",)
)
_COUNT_FORCE = [
    ForcePredicate(
        "node_subtype_changed",
        "node_retype",
        lambda i: bool(i.change_inputs.node_subtype_changed),
    ),
    ForcePredicate(
        "source_reattributed",
        "source_reattribution",
        lambda i: bool(i.change_inputs.source_reattributed),
    ),
    ForcePredicate(
        "edge_type_changed",
        "edge_type_change",
        lambda i: bool(i.change_inputs.edge_type_changed),
    ),
]


def _count_full_recount(ctx: MaintainContext) -> MaintainResult:
    """Self-heal full recount: replace every rollup row with a fresh recompute.

    Reads the fresh recompute + the existing rows from the branch-head checkout and
    returns the upserts/deletes; the kit's driver writes them and the sentinel
    (cadence reset to 0) in one transaction.
    """
    target = _full_recount_rows(ctx.checkout)
    existing = _current_rollup_rows(ctx.checkout)
    upserts = [_row_for(key, count) for key, count in target.items()]
    deletes = [_key_dict(key) for key in existing if key not in target]
    return MaintainResult(
        upserts=upserts,
        deletes=deletes,
        cadence_counter=0,
        dirty_bucket_count=None,
        mode="full_recount",
    )


def _count_incremental(ctx: MaintainContext) -> MaintainResult:
    """Running sum over the cycle's changed nodes/edges (+1 current / -1 prior).

    Advances the cadence counter by one. A no-op cycle returns empty
    upserts/deletes; the kit's driver still stamps the sentinel so a read at this
    generation proves the rollup was maintained (prior rows carried forward by
    Chronos). The by-subtype / by-source legs are add/delete-only here -- a move of
    either is force-routed to a full recount, so this never executes move
    arithmetic for them; the by-edge-type leg's -1-prior / +1-current legs handle
    an edge-type change directly.
    """
    change: _CountChange = ctx.change_inputs
    # De-dup on the incremental dispatch ONLY (a None scope is routed to a full
    # recount as "scope_unavailable", never here).
    node_ids = list(dict.fromkeys(str(n) for n in change.changed_node_ids))
    changed_edge_ids = change.changed_edge_ids or []
    edge_ids = list(dict.fromkeys(int(e) for e in changed_edge_ids))

    # The prior rollup rows the deltas add onto (compact — hundreds of rows).
    # Read up-front: the shape-upgrade check below needs them even on a no-op cycle.
    prior_counts = _current_rollup_rows(ctx.checkout)

    # Dimension-shape upgrade: rows carried forward from a release that predates
    # the source_subtype dimension have `source` buckets but no `source_subtype`
    # buckets (the two dimensions share the `source IS NOT NULL` domain, so on a
    # same-shape table they are present or absent TOGETHER). Folding increments
    # into a missing dimension would accumulate a silently PARTIAL dimension
    # until the periodic self-heal — recompute in full instead, ONCE, with the
    # cause recorded (never an unrecorded full recompute). The backfill makes
    # this predicate false for every subsequent cycle (quiescence).
    prior_kinds = {kind for kind, _value in prior_counts}
    if KIND_SOURCE in prior_kinds and KIND_SOURCE_SUBTYPE not in prior_kinds:
        # Kit contract for a callback-decided full recompute UNDER incremental
        # dispatch: mode stays "incremental", the cause rides full_reason
        # (maintained_derivation.MaintainResult docstring); cadence resets to 0
        # because a genuine full recount just ran (the self-heal clock restarts).
        return _count_full_recount(ctx)._replace(
            mode="incremental",
            full_reason=FULL_REASON_DIMENSION_SHAPE_UPGRADE,
        )

    # A bulk change (issue 3955): resolving every changed id at the current and
    # the prior checkout costs more than one recount of the whole graph. The
    # prior rollup's subtype and edge-type buckets count every node and edge
    # once each, so their sum is the prior graph size.
    prior_total = sum(
        count
        for (kind, _value), count in prior_counts.items()
        if kind in (KIND_SUBTYPE, KIND_EDGE_TYPE)
    )
    if is_large_change_set(
        len(node_ids) + len(edge_ids), prior_total, percent=LARGE_CHANGE_SET_PERCENT
    ):
        return _count_full_recount(ctx)._replace(
            mode="incremental",
            full_reason=FULL_REASON_LARGE_CHANGE_SET,
        )

    # Advance the periodic-self-heal counter (absence implies 0; the entry guard
    # forces a full recount when no prior sentinel exists).
    prior_elapsed = (
        read_cadence_counter(
            ctx.checkout, table=CONSOLE_COUNT_ROLLUP_TABLE, spec=_COUNT_SENTINEL
        )
        or 0
    )
    next_elapsed = prior_elapsed + 1

    # +1 for every changed node/edge LIVE at the current checkout.
    plus = _changed_buckets(ctx.checkout, node_ids, edge_ids)

    # -1 for every changed node/edge's PRIOR-generation bucket. A deleted item is
    # absent from the current checkout, so its bucket is named from the prior
    # published generation's checkpoint. A prior checkpoint that EXISTS but cannot
    # be checked out does NOT silently drop the -1 leg (that would overcount every
    # delete until self-heal) — it recomputes in full with the cause recorded.
    minus: dict[BucketKey, int] = {}
    if ctx.prior_generation_checkpoint is not None:
        try:
            prior_checkout = ctx.store.context.checkout_checkpoint(
                ctx.prior_generation_checkpoint
            )
        except Exception:  # noqa: BLE001 -- unreadable prior checkpoint => full
            return _count_full_recount(ctx)._replace(
                mode="incremental",
                full_reason=FULL_REASON_SCOPE_UNAVAILABLE,
            )
        minus = _changed_buckets(prior_checkout, node_ids, edge_ids)

    # Net delta per (dimension_kind, dimension_value).
    net: dict[BucketKey, int] = {}
    for key, c in plus.items():
        net[key] = net.get(key, 0) + c
    for key, c in minus.items():
        net[key] = net.get(key, 0) - c
    net = {key: d for key, d in net.items() if d != 0}

    if not net:
        return MaintainResult(
            upserts=[],
            deletes=[],
            cadence_counter=next_elapsed,
            dirty_bucket_count=0,
            mode="incremental",
        )

    # prior_counts (read above) is the base the deltas add onto; we index only
    # the touched buckets.
    upserts: list[dict[str, Any]] = []
    deletes: list[dict[str, Any]] = []
    for key, delta in net.items():
        new_count = prior_counts.get(key, 0) + delta
        if new_count <= 0:
            # Bucket emptied (or never existed): DELETE so absence == zero. A
            # negative would mean drift; clamp to delete and let the periodic full
            # recount self-heal (also surfaced by the drift test).
            if key in prior_counts:
                deletes.append(_key_dict(key))
        else:
            upserts.append(_row_for(key, new_count))

    return MaintainResult(
        upserts=upserts,
        deletes=deletes,
        cadence_counter=next_elapsed,
        dirty_bucket_count=len(net),
        mode="incremental",
    )


def maintain_console_count_rollup(
    conn: Any,
    *,
    generation_id: int,
    changed_node_ids: list[str],
    changed_edge_ids: list[int] | None,
    prior_generation_checkpoint: str | None,
    branch: str = DEFAULT_BRANCH,
    force_full: bool = False,
    node_subtype_changed: bool = False,
    source_reattributed: bool = False,
    edge_type_changed: bool = False,
    self_heal_every: int = SELF_HEAL_EVERY_N_GENERATIONS,
) -> dict[str, Any]:
    """Maintain ``okg.console_count_rollup`` for this publish cycle (running sum).

    Parameters
    ----------
    conn:
        The publisher's open cycle connection (writes share the cycle txn).
    generation_id:
        The integer ``target_generation_id`` being published. The rollup rows are
        pinned by the CHECKPOINT the publisher cuts after this returns, not by this
        int; it is recorded in the sentinel's cadence counter on a full recount.
    changed_node_ids:
        Every node id touched this cycle (upserts AND deletes), the union of the
        dirty scope's ``nodes_by_subtype`` values. Drives the by-subtype / by-source
        fold legs.
    changed_edge_ids:
        The cycle's changed edge ids (any type). None means the dirty scope was
        UNAVAILABLE -- we cannot trust an incremental update, so force a full
        recount (loud, never a stale carry-forward).
    prior_generation_checkpoint:
        The CHECKPOINT id of the most-recent PUBLISHED generation strictly before
        this one, used to resolve deleted items' prior buckets. None on the first
        publish.
    force_full:
        Force a full recount + self-heal regardless of the change set.
    node_subtype_changed / source_reattributed / edge_type_changed:
        The three move detectors from the cycle's dirty scope (see module docstring).
        Each forces a full recount.
    self_heal_every:
        Periodic self-heal cadence (in generations). Defaults to
        ``SELF_HEAL_EVERY_N_GENERATIONS``.

    Returns a summary dict (mode, dirty_bucket_count, rows_written, rows_deleted,
    and ``reason`` on a full recount) for the publisher to persist + log.

    A thin adapter over the maintained-derivation kit: it packages the cycle's
    change inputs and delegates the orchestration (ensure / checkout / decide-mode
    / dispatch / one-txn write) to ``maintain``.
    """
    spec = DerivationSpec(
        derivation_id="console_count_rollup",
        sentinel=_COUNT_SENTINEL,
        oracle=_COUNT_ORACLE,
        force_full_predicates=_COUNT_FORCE,
        full_recount=_count_full_recount,
        incremental=_count_incremental,
        self_heal_every=self_heal_every,
    )
    inputs = ForceInputs(
        change_inputs=_CountChange(
            node_subtype_changed=node_subtype_changed,
            source_reattributed=source_reattributed,
            edge_type_changed=edge_type_changed,
            changed_node_ids=changed_node_ids,
            changed_edge_ids=changed_edge_ids,
        ),
        force_full=force_full,
        scope_available=changed_edge_ids is not None,
    )
    return maintain(
        conn,
        spec=spec,
        storage=_COUNT_STORAGE,
        branch=branch,
        generation_id=generation_id,
        prior_generation_checkpoint=prior_generation_checkpoint,
        inputs=inputs,
    )


def _count_change_inputs(dirty_scope: Any) -> _CountChange:
    """The dirty-scope -> console-count change descriptor (the extraction the
    publisher stage method did inline). A None scope or unavailable edge table
    yields ``changed_edge_ids=None`` (forcing a full recount); a node ``__subtype__``
    change -> node_subtype_changed; a node ``__source__`` change (reattribution) ->
    source_reattributed. ``edge_type_changed`` stays False (an edge-type change
    re-keys the edge id, so the fold's -1-old / +1-new legs own that move; the
    predicate stays registered as a belt but is not driven from here)."""
    changed_node_ids: list[str] = []
    changed_edge_ids: list[int] | None
    node_subtype_changed = False
    source_reattributed = False
    if dirty_scope is None:
        changed_edge_ids = None
    else:
        nodes_by_subtype = dirty_scope[0] or {}
        changed_node_ids = [
            str(node_id)
            for node_ids in nodes_by_subtype.values()
            for node_id in node_ids
        ]
        edges_by_type = dirty_scope[1] or {}
        edge_table_available = bool(dirty_scope[2])
        if not edge_table_available:
            changed_edge_ids = None
        else:
            changed_edge_ids = [
                int(edge_id)
                for edge_ids in edges_by_type.values()
                for edge_id in edge_ids
            ]
        attr_changes_by_subtype = dirty_scope[3] if len(dirty_scope) > 3 else None
        if attr_changes_by_subtype:
            for node_changes in attr_changes_by_subtype.values():
                for fields in node_changes.values():
                    field_set = {str(field) for field in fields}
                    if "__subtype__" in field_set:
                        node_subtype_changed = True
                    if "__source__" in field_set:
                        source_reattributed = True
    return _CountChange(
        node_subtype_changed=node_subtype_changed,
        source_reattributed=source_reattributed,
        edge_type_changed=False,
        changed_node_ids=changed_node_ids,
        changed_edge_ids=changed_edge_ids,
    )


def _count_make_inputs(dirty_scope: Any, force_full: bool) -> ForceInputs:
    change = _count_change_inputs(dirty_scope)
    return ForceInputs(
        change_inputs=change,
        force_full=force_full,
        scope_available=change.changed_edge_ids is not None,
    )


def console_count_rollup_row(
    self_heal_every: int = SELF_HEAL_EVERY_N_GENERATIONS,
) -> DerivationRow:
    """The console count rollup as a maintained-derivation registry row.

    DETERMINISTIC (a ``CorrectnessOracle``, no override), Chronos-registered
    side-table storage, maintained pre-checkpoint, fail-soft. Its ``spec`` is
    byte-identical to ``maintain_console_count_rollup``'s and ``make_inputs``
    reproduces the publisher stage method's scope extraction (the three force
    detectors). Its publish-stage timing key is ``console_count_rollup`` (the bench
    reads it back). Enablement (the ``OKG_IVM_CONSOLE_COUNT_ROLLUP`` flag) stays a
    publisher decision -- the row is registered always; the publisher skips it when
    the flag is off, the same gate as today.
    """
    spec = DerivationSpec(
        derivation_id="console_count_rollup",
        sentinel=_COUNT_SENTINEL,
        oracle=_COUNT_ORACLE,
        force_full_predicates=_COUNT_FORCE,
        full_recount=_count_full_recount,
        incremental=_count_incremental,
        self_heal_every=self_heal_every,
    )
    return DerivationRow(
        spec=spec,
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_node", "graph_edge"})),
        materializations=[
            Materialization(
                kind="side_table",
                storage=_COUNT_STORAGE,
                sentinel=_COUNT_SENTINEL,
            )
        ],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
        failure_mode=FailureMode.NON_GATING,
        timing_key="console_count_rollup",
        make_inputs=_count_make_inputs,
    )


__all__ = [
    "COUNT_RECOMPUTE_SQL",
    "KIND_EDGE_SOURCE",
    "KIND_EDGE_TYPE",
    "KIND_ENRICHER_EDGE",
    "KIND_ENRICHER_NODE",
    "KIND_SOURCE",
    "KIND_SOURCE_SUBTYPE",
    "KIND_SUBTYPE",
    "SELF_HEAL_EVERY_N_GENERATIONS",
    "console_count_rollup_row",
    "maintain_console_count_rollup",
]
