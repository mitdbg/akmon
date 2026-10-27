"""Publish-stage running-sum maintenance for the ontology graph rollup.

The operator Graph view's ontology read needs the realised
``(src_subtype, edge_type, dst_subtype)`` edge buckets, with the producing
connector / enricher as a row dimension and the realised edge count. Recomputing
that aggregation on every read double-joins the ~2M reconstructed Chronos edges
(~15-80s on okg-workspace). The OLD materialisation RECOUNTED every dirty bucket
each publish; but the recount SQL filters by a POST-JOIN computed key, so it
joins all edges every publish (disk-spills; NOT bucket-proportional).

This module instead maintains a **running sum** at write time, into a
Chronos-registered table (``okg.ontology_rollup``), so the publish cost is
O(changed edges) and a read is a compact as-of-generation scan.

Where it runs
-------------
A PUBLISH-STAGE hook (``maintain_ontology_rollup``), NOT ``apply_facts`` and NOT
the views framework's ``build_corpus``. The publisher calls it once per cycle on
the deployment's default branch, AFTER every edge of the cycle has been applied
(source + enricher + ``view_cites_node``) and BEFORE the generation checkpoint is
cut — so the checkpoint pins a rollup consistent with the graph it pins.

How the +1/-1 sign is recovered
--------------------------------
The cycle's changed-edge set (``edges_by_type`` from the segment delta) carries
only edge ids by type, with NO insert/retract sign. We recover the sign by
PRESENCE at the current branch head:

* a changed edge that is LIVE in the current checkout is an insert/update → its
  CURRENT ``(src_subtype, edge_type, dst_subtype, producer)`` bucket gets +1;
* a changed edge that is ABSENT/tombstoned in the current checkout is a retract →
  its bucket can only be named from the PRIOR published generation's checkout
  (where it was still live), which gets -1;
* a modify (live in both, e.g. attrs/source change) emits -1 against its prior
  bucket and +1 against its current bucket: nets to zero when the bucket is
  unchanged, and correctly MOVES the count when an endpoint / type / producer
  changed.

We sum these per ``(bucket, producer)``, add the deltas to the prior rollup rows
(carried forward on the branch), upsert the new counts, and DELETE any row whose
count reaches zero (so absence == zero — the emptied-bucket P0 is handled by the
delete, not by a stale carry-forward).

Why a node retype forces a full recount
---------------------------------------
The +1/-1 recovery above only sees edges in the cycle's changed-edge set. But an
edge id is ``uuid5(src|edge_type|dst)`` over node IDS, so a node RETYPE (subtype
change) that leaves a node's incident edges' endpoints / edge_type unchanged does
NOT change those edge ids — the edges are ABSENT from the changed set even though
their ``(src_subtype, edge_type, dst_subtype)`` bucket moved. The incremental
path would silently keep the stale bucket forever (a P0 silent drift). We refuse
to incrementally re-bucket retyped nodes' incident edges (enumerating them is the
fragile path that caused the bug); instead, when the cycle's dirty scope reports
ANY node ``__subtype__`` change, we do a FULL recount for the generation —
guaranteed correct. Retypes are schema/ontology churn (rare), not the data
firehose, so the occasional full recount is the right trade.

Self-heal (drift guard)
-----------------------
A running sum can silently drift. ``ONTOLOGY_ROLLUP_FULL_SQL`` is the
byte-equality oracle AND the self-heal source: on the FIRST publish (no prior
rollup marker), on a retype cycle, when forced, OR once every
``SELF_HEAL_EVERY_N_GENERATIONS`` generations, we do a FULL recount — delete
every rollup row and insert the full grouped double-join — so the rollup
self-heals to ground truth. A cycle that changes no edge writes nothing and the
prior rows remain visible at the new checkpoint automatically (Chronos
carry-forward). A bulk change (``maintained_derivation.is_large_change_set``)
also recounts in full, recorded as ``large_change_set``: resolving hundreds of
thousands of changed ids twice costs more than one recount (issue 3955).

The periodic self-heal is defense-in-depth: nothing else forces a recount on a
live deployment, so an unforeseen future drift source would never heal. The
sentinel row carries the LAST full-recount generation (in its ``edge_count``
field, which the read ignores); when ``current - last_full_recount`` reaches the
cadence we force a recount, bounding any drift to at most that many generations.

Producer attribution
---------------------
``apply_facts`` stamps ``graph_edges.source = '_publisher_derived'`` on every
enricher-derived edge but preserves each edge's ``source_record_id`` verbatim,
and every enricher family stamps ``linker`` there. So the producer is the source
connector for source-connector edges, ``_enricher:<linker>`` for derived edges,
and an explicit ``_publisher_derived:unattributed`` bucket for derived edges with
no recoverable linker (SHOWN, never silently dropped or defaulted).
"""
from __future__ import annotations

from typing import Any, NamedTuple

from okg.substrate.derived_artifacts.maintained_derivation import (
    FULL_REASON_LARGE_CHANGE_SET,
    LARGE_CHANGE_SET_MIN_IDS,
    ChronosRegisteredTable,
    CorrectnessOracle,
    DerivationSpec,
    ForceInputs,
    ForcePredicate,
    MaintainContext,
    MaintainResult,
    SentinelSpec,
    is_large_change_set,
    maintain,
    read_cadence_counter,
    read_self_heal_cadence,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    PUBLISHER_DERIVED_SOURCE,
    SENTINEL_TOKEN,
    UNATTRIBUTED_DERIVED_PRODUCER,
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
    DEFAULT_BRANCH,
    ONTOLOGY_ROLLUP_TABLE,
)

# Periodic self-heal cadence (defense-in-depth): force a FULL recount every N
# generations so any unforeseen drift source is bounded to at most N generations,
# even if a future change introduces one. Resolved once at import via the SHARED kit
# helper, so the read and write paths use ONE cadence reader (no per-consumer copy to
# drift). Overridable via the OKG_ONTOLOGY_ROLLUP_SELF_HEAL_EVERY env var (a positive
# integer; non-integer / non-positive values warn and fall back to the default).
SELF_HEAL_EVERY_N_GENERATIONS = read_self_heal_cadence(
    "OKG_ONTOLOGY_ROLLUP_SELF_HEAL_EVERY", default=25
)

# Issue 3955: above this share of the prior edge count (and the kit's 10,000-id
# floor) the cycle recounts in full instead of resolving every changed edge at
# two checkouts. Measured break-even on a 339k-edge synthetic graph after a bulk
# load (two repeats): the two id-list reads cost 0.63-0.76 s at 10 % of the
# edges changed and 1.08-1.41 s at 20 %, against 0.83-1.03 s for one full
# recount. 15 % sits in that crossover, so either side is at most about 1.2x
# slower than the better path near the bound. Not measured at a 4.8M-edge scale.
LARGE_CHANGE_SET_PERCENT = 15


# UNATTRIBUTED_DERIVED_PRODUCER (the SHOWN, never-defaulted bucket for a derived edge
# whose linker could not be recovered) and PUBLISHER_DERIVED_SOURCE come from the
# import-light single source (derived_artifacts.maintained_derivation_constants), so the
# read and write paths share ONE copy and cannot drift.

# A reserved sentinel row written EVERY time the hook maintains the rollup at a
# generation (full or incremental). Its presence at generation N proves the hook
# ran at N, so a read can FAIL LOUD when it is absent — distinguishing "this
# generation has no rollup" (never maintained) from "the graph genuinely has no
# edges" (a maintained but empty rollup, which still carries the sentinel). The
# PK values use a reserved sentinel token no real subtype / edge type / producer
# can take (they are lowercase identifiers / source names). The row is carried
# forward by Chronos like any other and is filtered out of the read.
#
# The sentinel's ``edge_count`` carries the COUNT OF GENERATIONS SINCE THE LAST
# FULL RECOUNT (a small monotonic counter: 0 right after a full recount, +1 per
# incremental cycle), so the maintain hook can drive the periodic self-heal from
# one carried-forward row. A COUNTER (not a generation id) is deliberate: the
# integer ``target_generation_id`` is a microsecond timestamp, so an
# id-difference cadence is meaningless — counting cycles is robust to how ids are
# minted. The read NEVER interprets the sentinel's edge_count — it only checks the
# row's presence — so reusing the field for cadence state is safe.
_SENTINEL_TOKEN = SENTINEL_TOKEN  # the import-light single source
SENTINEL_SRC = _SENTINEL_TOKEN
SENTINEL_EDGE = _SENTINEL_TOKEN
SENTINEL_DST = _SENTINEL_TOKEN
SENTINEL_PRODUCER = _SENTINEL_TOKEN


def _is_sentinel_row(src: str, edge: str, dst: str, producer: str) -> bool:
    return (
        src == SENTINEL_SRC
        and edge == SENTINEL_EDGE
        and dst == SENTINEL_DST
        and producer == SENTINEL_PRODUCER
    )


# SQL fragment (chronos `:name` params) computing a single edge's producer from
# its ``source`` / ``source_record_id``. Shared by the running-sum + full-recount
# paths so the two can never diverge. NOTE: kept byte-identical (modulo table
# alias) to deployments/okg-workspace/views/ontology_graph_rollup._PRODUCER_EXPR.
_PRODUCER_EXPR = f"""
  CASE
    WHEN e.source = '{PUBLISHER_DERIVED_SOURCE}'
      AND NULLIF(e.source_record_id->>'linker', '') IS NOT NULL
      THEN '_enricher:' || (e.source_record_id->>'linker')
    WHEN e.source = '{PUBLISHER_DERIVED_SOURCE}'
      THEN '{UNATTRIBUTED_DERIVED_PRODUCER}'
    ELSE coalesce(e.source, '_unknown')
  END
"""

# Full grouped double-join — the producer-dimensioned bucket aggregation over the
# WHOLE pinned-generation graph. BOTH the self-heal/full-recount source AND the
# correctness oracle the byte-equality test forces.
ONTOLOGY_ROLLUP_FULL_SQL = f"""
SELECT
  ns.subtype AS src_subtype,
  e.edge_type AS edge_type,
  nd.subtype AS dst_subtype,
  {_PRODUCER_EXPR} AS producer,
  count(*)::bigint AS edge_count
FROM okg.graph_edges e
JOIN okg.graph_nodes ns ON ns.node_id = e.src
JOIN okg.graph_nodes nd ON nd.node_id = e.dst
GROUP BY 1, 2, 3, 4
"""

# Resolve the (bucket, producer) a set of edge ids belongs to AT A PINNED
# GENERATION. Used against BOTH the current checkout (+1 inserts/updates) and the
# prior generation's checkout (-1 retracts). One row per edge that is live at the
# pinned generation (a tombstoned/absent edge simply produces no row).
#
# The changed ids enter as a relation the planner joins and can hash, never as
# ``e.edge_id = ANY(:edge_ids)`` (issue #3955). With the array as a predicate, a
# parallel plan could scan the nodes first and probe the edges once per node,
# with the whole array inside the inner index scan's condition, so every probe
# re-read the array: 325,448 changed edges on a 244,471-node graph ran past
# 1,200 s, where the hashed plan takes about 1 s. ``DISTINCT`` keeps the old
# result for a repeated id: one row per live edge.
_EDGE_BUCKETS_SQL = f"""
SELECT
  e.edge_id AS edge_id,
  ns.subtype AS src_subtype,
  e.edge_type AS edge_type,
  nd.subtype AS dst_subtype,
  {_PRODUCER_EXPR} AS producer
FROM (
  SELECT DISTINCT unnest(CAST(:edge_ids AS BIGINT[])) AS edge_id
) AS changed
JOIN okg.graph_edges e ON e.edge_id = changed.edge_id
JOIN okg.graph_nodes ns ON ns.node_id = e.src
JOIN okg.graph_nodes nd ON nd.node_id = e.dst
"""

# The prior rollup rows currently visible at the branch head (the prior
# generation's rollup, carried forward). The running sum adds its net deltas onto
# these. The sentinel is excluded here (it is rewritten unconditionally).
_CURRENT_ROLLUP_SQL = f"""
SELECT src_subtype, edge_type, dst_subtype, producer, edge_count
FROM {ONTOLOGY_ROLLUP_TABLE}
"""


def _bucket_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row["src_subtype"]),
        str(row["edge_type"]),
        str(row["dst_subtype"]),
        str(row["producer"]),
    )


def _row_for(key: tuple[str, str, str, str], count: int) -> dict[str, Any]:
    src, edge, dst, producer = key
    return {
        "src_subtype": src,
        "edge_type": edge,
        "dst_subtype": dst,
        "producer": producer,
        "edge_count": int(count),
    }


def _key_dict(key: tuple[str, str, str, str]) -> dict[str, Any]:
    src, edge, dst, producer = key
    return {
        "src_subtype": src,
        "edge_type": edge,
        "dst_subtype": dst,
        "producer": producer,
    }


def _full_recount_rows(checkout) -> dict[tuple[str, str, str, str], int]:
    rows: dict[tuple[str, str, str, str], int] = {}
    for row in checkout.query(ONTOLOGY_ROLLUP_FULL_SQL):
        rows[_bucket_key(row)] = int(row["edge_count"] or 0)
    return rows


def _current_rollup_rows(checkout) -> dict[tuple[str, str, str, str], int]:
    rows: dict[tuple[str, str, str, str], int] = {}
    for row in checkout.query(_CURRENT_ROLLUP_SQL):
        key = _bucket_key(row)
        if _is_sentinel_row(*key):
            continue
        rows[key] = int(row["edge_count"] or 0)
    return rows


def _edge_bucket_counts(
    checkout, edge_ids: list[int]
) -> dict[tuple[str, str, str, str], int]:
    """Map each LIVE changed edge at this checkout to its (bucket, producer),
    counting how many of the changed edges fall in each bucket."""
    counts: dict[tuple[str, str, str, str], int] = {}
    if not edge_ids:
        return counts
    for row in checkout.query(_EDGE_BUCKETS_SQL, {"edge_ids": edge_ids}):
        key = _bucket_key(row)
        counts[key] = counts.get(key, 0) + 1
    return counts


def maintain_ontology_rollup(
    conn: Any,
    *,
    generation_id: int,
    changed_edge_ids: list[int] | None,
    prior_generation_checkpoint: str | None,
    branch: str = DEFAULT_BRANCH,
    force_full: bool = False,
    node_subtype_changed: bool = False,
    self_heal_every: int = SELF_HEAL_EVERY_N_GENERATIONS,
) -> dict[str, Any]:
    """Maintain ``okg.ontology_rollup`` for this publish cycle (running sum).

    Parameters
    ----------
    conn:
        The publisher's open cycle connection (writes share the cycle txn).
    generation_id:
        The integer ``target_generation_id`` being published. The rollup rows are
        pinned by the CHECKPOINT the publisher cuts after this returns, not by
        this int; it is recorded in the sentinel as the last-full-recount cadence
        anchor when a full recount runs.
    changed_edge_ids:
        The cycle's changed edge ids (any type, from the segment delta). None
        means the dirty scope was UNAVAILABLE — we cannot trust an incremental
        update, so force a full recount (loud, never a stale carry-forward).
    prior_generation_checkpoint:
        The CHECKPOINT id of the most-recent PUBLISHED generation strictly before
        this one (e.g. ``generation:1234``), used to resolve retracted edges'
        prior buckets (they are tombstoned in the current checkout). None on the
        first publish.
    force_full:
        Force a full recount + self-heal regardless of the change set.
    node_subtype_changed:
        True iff this cycle's dirty scope reports ANY node ``__subtype__`` change
        (a retype). A retype moves a node's incident edges' buckets WITHOUT
        changing those edges' ids, so the edges are absent from
        ``changed_edge_ids`` and the incremental path can never see the move. We
        therefore force a FULL recount on any retype cycle (guaranteed correct;
        retypes are rare).
    self_heal_every:
        Periodic self-heal cadence (in GENERATIONS). When this many incremental
        cycles have elapsed since the last full recount, force a full recount
        (defense-in-depth so any unforeseen drift is bounded to at most this many
        generations). Defaults to ``SELF_HEAL_EVERY_N_GENERATIONS``.

    Returns a summary dict (mode, dirty_bucket_count, rows_written) for logging.

    This is now a thin adapter over the maintained-derivation kit: it packages the
    cycle's change inputs and delegates the orchestration (ensure / checkout /
    decide-mode / dispatch / one-txn write) to ``maintain``. Behaviour is unchanged
    from the pre-extraction running-sum hook -- the running-sum logic lives in the
    ``_rollup_full_recount`` / ``_rollup_incremental`` callbacks below.
    """
    spec = DerivationSpec(
        derivation_id="ontology_graph_rollup",
        sentinel=_ROLLUP_SENTINEL,
        oracle=_ROLLUP_ORACLE,
        force_full_predicates=_ROLLUP_FORCE,
        full_recount=_rollup_full_recount,
        incremental=_rollup_incremental,
        self_heal_every=self_heal_every,
    )
    inputs = ForceInputs(
        change_inputs=_RollupChange(
            changed_edge_ids=changed_edge_ids,
            node_subtype_changed=node_subtype_changed,
        ),
        force_full=force_full,
        scope_available=changed_edge_ids is not None,
    )
    return maintain(
        conn,
        spec=spec,
        storage=_ROLLUP_STORAGE,
        branch=branch,
        generation_id=generation_id,
        prior_generation_checkpoint=prior_generation_checkpoint,
        inputs=inputs,
    )


class _RollupChange(NamedTuple):
    """The ontology rollup's opaque per-cycle change descriptor.

    The kit never inspects this -- only the rollup's force predicate
    (``node_subtype_changed``) and its incremental callback (the changed edge ids)
    interpret it.
    """

    changed_edge_ids: list[int] | None
    node_subtype_changed: bool


# The ontology rollup declared as a maintained derivation: deterministic (a pure
# function of the sealed generation's graph), Chronos-registered storage, and one
# force predicate -- a node retype moves an edge's bucket WITHOUT changing the edge
# id, so it forces a full recount.
_ROLLUP_PK = ("src_subtype", "edge_type", "dst_subtype", "producer")
_ROLLUP_SENTINEL = SentinelSpec(pk_columns=_ROLLUP_PK, cadence_column="edge_count")
_ROLLUP_STORAGE = ChronosRegisteredTable(
    table=ONTOLOGY_ROLLUP_TABLE,
    ensure_fn=lambda store: store.ensure_ontology_rollup_table(),
)
_ROLLUP_ORACLE = CorrectnessOracle(
    ONTOLOGY_ROLLUP_FULL_SQL, key_columns=_ROLLUP_PK, value_columns=("edge_count",)
)
_ROLLUP_FORCE = [
    ForcePredicate(
        "node_subtype_changed",
        "node_retype",
        lambda i: bool(i.change_inputs.node_subtype_changed),
    )
]


def _rollup_full_recount(
    ctx: MaintainContext,
    existing: dict[tuple[str, str, str, str], int] | None = None,
) -> MaintainResult:
    """Self-heal full recount: replace every rollup row with a fresh recount.

    Reads the fresh recount + the existing rows from the branch-head checkout and
    returns the upserts/deletes; the kit's driver writes them and the sentinel (with
    the cadence counter reset to 0, since this IS a full recount) in one transaction.
    A caller that already read the existing rows passes them as ``existing``.
    """
    target = _full_recount_rows(ctx.checkout)
    if existing is None:
        existing = _current_rollup_rows(ctx.checkout)
    upserts = [_row_for(key, count) for key, count in target.items()]
    # Delete any prior row not in the fresh recount (incl. emptied buckets).
    deletes = [_key_dict(key) for key in existing if key not in target]
    return MaintainResult(
        upserts=upserts,
        deletes=deletes,
        cadence_counter=0,
        dirty_bucket_count=None,
        mode="full_recount",
    )


def _rollup_incremental(ctx: MaintainContext) -> MaintainResult:
    """Running sum over the cycle's changed edges (+1 current / -1 prior).

    Advances the cadence counter by one. A no-op cycle returns empty upserts/deletes;
    the kit's driver still stamps the sentinel so a read at this generation proves the
    rollup was maintained (the prior rows are carried forward by Chronos).
    """
    change: _RollupChange = ctx.change_inputs
    # De-dup the changed edge ids on the incremental dispatch ONLY (a None scope is
    # routed to a full recount as "scope_unavailable", never here).
    changed_edge_ids = list(dict.fromkeys(int(e) for e in change.changed_edge_ids))

    # A bulk change (issue 3955): resolving every changed edge at the current
    # and the prior checkout costs more than one recount of the whole graph.
    # Every counted edge sits in exactly one prior bucket, so the bucket sum is
    # the prior edge count. Read only when the floor is reached, so an ordinary
    # cycle pays nothing extra.
    if len(changed_edge_ids) >= LARGE_CHANGE_SET_MIN_IDS:
        existing = _current_rollup_rows(ctx.checkout)
        if is_large_change_set(
            len(changed_edge_ids),
            sum(existing.values()),
            percent=LARGE_CHANGE_SET_PERCENT,
        ):
            # Kit contract for a callback-decided full recount under incremental
            # dispatch: mode stays "incremental", the cause rides full_reason,
            # and the cadence restarts at 0 because a full recount just ran.
            return _rollup_full_recount(ctx, existing)._replace(
                mode="incremental",
                full_reason=FULL_REASON_LARGE_CHANGE_SET,
            )

    # Advance the periodic-self-heal counter (absence implies 0; the entry guard
    # forces a full recount when no prior sentinel exists). A stuck counter would
    # silently disable the periodic self-heal.
    prior_elapsed = (
        read_cadence_counter(
            ctx.checkout, table=ONTOLOGY_ROLLUP_TABLE, spec=_ROLLUP_SENTINEL
        )
        or 0
    )
    next_elapsed = prior_elapsed + 1

    # +1 for every changed edge LIVE at the current checkout (insert/update).
    plus = _edge_bucket_counts(ctx.checkout, changed_edge_ids)

    # -1 for every changed edge's PRIOR-generation bucket. A retracted edge is absent
    # from the current checkout, so its bucket is named from the prior published
    # generation's checkpoint. A modify (live in both) gets both a -1 (prior bucket)
    # here and a +1 (current bucket) above.
    minus: dict[tuple[str, str, str, str], int] = {}
    if ctx.prior_generation_checkpoint is not None:
        try:
            prior_checkout = ctx.store.context.checkout_checkpoint(
                ctx.prior_generation_checkpoint
            )
        except Exception:  # noqa: BLE001 — absent prior checkpoint ⇒ no -1 base
            prior_checkout = None
        if prior_checkout is not None:
            minus = _edge_bucket_counts(prior_checkout, changed_edge_ids)

    # Net delta per (bucket, producer).
    net: dict[tuple[str, str, str, str], int] = {}
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

    # The prior rollup rows the deltas add onto. The rollup is the COMPACT
    # aggregation (bounded by subtypes x edge_types x producers — hundreds to low
    # thousands of rows), so reading it whole is cheap and stays far below O(changed
    # edges); we then index only the touched buckets.
    prior_counts = _current_rollup_rows(ctx.checkout)
    upserts: list[dict[str, Any]] = []
    deletes: list[dict[str, Any]] = []
    for key, delta in net.items():
        new_count = prior_counts.get(key, 0) + delta
        if new_count <= 0:
            # Bucket emptied (or never existed): DELETE so absence == zero. A negative
            # would mean drift; clamp to delete and let the periodic full recount
            # self-heal (also surfaced by the drift test).
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


def _ontology_change_inputs(dirty_scope: Any) -> _RollupChange:
    """The dirty-scope -> ontology change descriptor (the extraction the publisher
    stage method did inline). A None scope or unavailable edge table yields
    ``changed_edge_ids=None`` (forcing a full recount); any node ``__subtype__``
    change yields ``node_subtype_changed=True`` (a retype moves an edge's bucket
    without changing the edge id, so the incremental path can't see it).
    """
    if dirty_scope is None:
        return _RollupChange(changed_edge_ids=None, node_subtype_changed=False)
    edges_by_type = dirty_scope[1] or {}
    edge_table_available = bool(dirty_scope[2])
    if not edge_table_available:
        changed_edge_ids: list[int] | None = None
    else:
        changed_edge_ids = [
            int(edge_id)
            for edge_ids in edges_by_type.values()
            for edge_id in edge_ids
        ]
    node_subtype_changed = False
    attr_changes_by_subtype = dirty_scope[3] if len(dirty_scope) > 3 else None
    if attr_changes_by_subtype:
        node_subtype_changed = any(
            "__subtype__" in {str(field) for field in fields}
            for node_changes in attr_changes_by_subtype.values()
            for fields in node_changes.values()
        )
    return _RollupChange(
        changed_edge_ids=changed_edge_ids,
        node_subtype_changed=node_subtype_changed,
    )


def _ontology_make_inputs(dirty_scope: Any, force_full: bool) -> ForceInputs:
    change = _ontology_change_inputs(dirty_scope)
    return ForceInputs(
        change_inputs=change,
        force_full=force_full,
        scope_available=change.changed_edge_ids is not None,
    )


def ontology_rollup_row(
    self_heal_every: int = SELF_HEAL_EVERY_N_GENERATIONS,
) -> DerivationRow:
    """The ontology rollup as a maintained-derivation registry row.

    DETERMINISTIC (a ``CorrectnessOracle``, no override), Chronos-registered
    side-table storage, maintained pre-checkpoint, fail-soft (a read-acceleration
    artifact must never gate a publish). The ``spec`` is byte-identical to the one
    ``maintain_ontology_rollup`` builds, and ``make_inputs`` reproduces the publisher
    stage method's scope extraction -- so driving this row through the registry is
    equivalent to the hand-wired stage.
    """
    spec = DerivationSpec(
        derivation_id="ontology_graph_rollup",
        sentinel=_ROLLUP_SENTINEL,
        oracle=_ROLLUP_ORACLE,
        force_full_predicates=_ROLLUP_FORCE,
        full_recount=_rollup_full_recount,
        incremental=_rollup_incremental,
        self_heal_every=self_heal_every,
    )
    return DerivationRow(
        spec=spec,
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_edge", "graph_node"})),
        materializations=[
            Materialization(
                kind="side_table",
                storage=_ROLLUP_STORAGE,
                sentinel=_ROLLUP_SENTINEL,
            )
        ],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
        failure_mode=FailureMode.NON_GATING,
        # The publisher's timing-stage name is "ontology_rollup" (the bench reads it
        # back), distinct from the derivation_id "ontology_graph_rollup".
        timing_key="ontology_rollup",
        make_inputs=_ontology_make_inputs,
    )


__all__ = [
    "ONTOLOGY_ROLLUP_FULL_SQL",
    "SELF_HEAL_EVERY_N_GENERATIONS",
    "UNATTRIBUTED_DERIVED_PRODUCER",
    "maintain_ontology_rollup",
    "ontology_rollup_row",
]
