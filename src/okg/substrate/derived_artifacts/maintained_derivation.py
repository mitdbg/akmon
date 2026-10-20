"""The maintained-derivation governance kit.

One reusable kit -- a determinism-parameterized parity oracle, a sentinel +
self-heal cadence, a forcing-trigger framework, and a loud-fail read -- extracted
from the #433 ontology rollup so every maintained derivation (the ontology rollup,
the console count rollup, ...) is governed the same way instead of hand-rolling the
mechanism per consumer (GitHub #444 / ``extract-maintained-derivation-primitive``).

This module holds the consumer-agnostic CORE: it knows nothing about edges,
producers, attributes, or projection_deltas. A derivation supplies a
``SentinelSpec`` (its PK + cadence column), an oracle, a list of ``ForcePredicate``
s, and full-recount / incremental callbacks; the kit orchestrates the rest. The
behaviours here reproduce ``publisher/ontology_rollup.py`` byte-for-byte (the
forcing-reason precedence, the ``(elapsed + 1) >= N`` cadence arithmetic, the
None-vs-0 cadence-counter semantics, and the loud-fail-on-absent-sentinel read) so
re-expressing the rollup on the kit changes no behaviour.

Module-level imports stay light (no ``graph_store``); the few DB-touching helpers
defer that import into their bodies, so the read path can consume the kit without a
module-level ``graph_store`` import.
"""
from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, NamedTuple, Protocol, TypeVar

from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    SENTINEL_TOKEN,
    sentinel_pk_values,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")
EXPLICIT_INTERVAL_OPEN_ORD = (1 << 63) - 1
_EXPLICIT_INTERVAL_INSERT_BATCH_SIZE = 25_000

# A key/value row as the oracle and read helpers see it: an immutable tuple keyed
# by the derivation's key columns, mapping to its value columns.
KeyTuple = tuple[Any, ...]
ValueTuple = tuple[Any, ...]


# --------------------------------------------------------------------------- #
# Data types                                                                  #
# --------------------------------------------------------------------------- #
class OracleResult(NamedTuple):
    """The outcome of a parity check (kept side vs the oracle's ground truth)."""

    ok: bool
    missing_in_kept: list[KeyTuple]  # the oracle has it, the kept side lacks it
    extra_in_kept: list[KeyTuple]    # the kept side has it, the oracle lacks it (drift)
    mismatched: list[KeyTuple]       # same key, different value (drift)


@dataclass(frozen=True)
class SentinelSpec:
    """Identifies a derivation's reserved sentinel row.

    ``pk_columns`` are the table's primary-key columns (every one set to
    ``SENTINEL_TOKEN`` marks the sentinel); ``cadence_column`` is the column whose
    value carries the generations-since-last-full-recount counter (the ontology
    rollup reuses ``edge_count``; the console count rollup names its own).
    """

    pk_columns: Sequence[str]
    cadence_column: str


class ForceInputs(NamedTuple):
    """The cycle inputs a forcing predicate reads.

    ``change_inputs`` is OPAQUE to the kit -- a consumer's own change descriptor
    (the rollup's changed-edge ids + retype flag; the count rollup's
    projection_deltas) -- interpreted only by that consumer's predicates and
    callbacks.
    """

    change_inputs: Any
    force_full: bool
    scope_available: bool


class ForcePredicate(NamedTuple):
    """A named, reason-yielding predicate that can force a full recount.

    The list of predicates is evaluated in registration order; the FIRST that
    fires names the reason. A bool would OR them and discard which one fired,
    which the marginal-cost bench and the per-predicate tests need to observe.
    """

    name: str                            # diagnostic
    reason: str                          # the reason string emitted when it fires
    fires: Callable[[ForceInputs], bool]


class MaintainMode(NamedTuple):
    full: bool
    reason: str | None


# --------------------------------------------------------------------------- #
# Full-recompute cause taxonomy (the recorded-cause vocabulary).              #
# A full recompute with no recorded cause is a defect: every full path — the  #
# kit-level dispatch (decide_maintain_mode) AND a callback's fall-back to full #
# UNDER incremental dispatch — records exactly one of these. The first four    #
# are the kit's dispatch-decided reasons (returned by decide_maintain_mode);   #
# the rest are the extended causes a consumer's incremental callback records    #
# when a change cannot be recomputed bucket-scoped (batch is a recorded        #
# fallback, never the steady state). PACT consistency-claim-graph-binding,     #
# consistency-model.md §9.8.                                                    #
# --------------------------------------------------------------------------- #
FULL_REASON_FORCED = "forced"
FULL_REASON_SCOPE_UNAVAILABLE = "scope_unavailable"
FULL_REASON_FIRST_PUBLISH = "first_publish"
FULL_REASON_SELF_HEAL = "periodic_self_heal"
FULL_REASON_DELETION_SHAPE = "deletion_shape"
FULL_REASON_POLICY_CHANGE = "policy_change"
FULL_REASON_PRECEDENT_SCOPE_UNPROVABLE = "precedent_scope_unprovable"
FULL_REASON_LINKED_PAIR = "linked_pair_cross_bucket"
# The prior generation's persisted repair selections were written under a
# different selection schema version (a deployment upgraded across a change to
# the recorded selection content). Their rows cannot be carried forward, so the
# first cycle after the upgrade recomputes the whole selection set.
FULL_REASON_SELECTION_SCHEMA_UPGRADE = "selection_schema_upgrade"
# The branch-head rollup was maintained under an older DIMENSION SHAPE (a later
# release added a dimension the carried-forward rows lack). Incremental folding
# would accumulate a silently PARTIAL new dimension until the periodic self-heal,
# so the first post-upgrade cycle recomputes in full — once; the backfill makes
# the detection predicate false for every later cycle (quiescence).
FULL_REASON_DIMENSION_SHAPE_UPGRADE = "dimension_shape_upgrade"
FULL_RECOMPUTE_CAUSES: frozenset[str] = frozenset({
    FULL_REASON_FORCED,
    FULL_REASON_SCOPE_UNAVAILABLE,
    FULL_REASON_FIRST_PUBLISH,
    FULL_REASON_SELF_HEAL,
    FULL_REASON_DELETION_SHAPE,
    FULL_REASON_POLICY_CHANGE,
    FULL_REASON_PRECEDENT_SCOPE_UNPROVABLE,
    FULL_REASON_LINKED_PAIR,
    FULL_REASON_SELECTION_SCHEMA_UPGRADE,
    FULL_REASON_DIMENSION_SHAPE_UPGRADE,
})


class MaintainResult(NamedTuple):
    """What a consumer's full-recount / incremental callback returns.

    The callback owns its own ``cadence_counter`` (full -> 0; incremental ->
    prior + 1) and ``dirty_bucket_count`` (full -> None; incremental -> int, 0 on a
    no-op), reproducing the per-dispatch-path differences in ``ontology_rollup.py``.
    The kit writes ``upserts``/``deletes`` and the sentinel in one transaction.

    ``full_reason`` records WHY a callback fell back to a full recompute UNDER
    incremental dispatch (mode stays ``"incremental"``): one member of
    :data:`FULL_RECOMPUTE_CAUSES`, or ``None`` for a genuine bucket-scoped
    incremental / no-op. ``maintain`` surfaces it as ``summary["full_reason"]``
    so a batch fallback is always recorded and queryable, never silent. The
    kit-level dispatch full paths carry their cause through ``mode.reason``
    (also mirrored into ``summary["full_reason"]``); this field is for the
    callback-decided full recomputes the kit cannot see.
    """

    upserts: list[dict[str, Any]]
    deletes: list[dict[str, Any]]
    cadence_counter: int
    dirty_bucket_count: int | None
    mode: str  # "full_recount" | "incremental"
    full_reason: str | None = None


# --------------------------------------------------------------------------- #
# The determinism-parameterized parity oracle                                 #
# --------------------------------------------------------------------------- #
class CorrectnessOracle:
    """Byte-identical CORRECTNESS oracle for a DETERMINISTIC derivation.

    Runs the consumer's full-recompute SQL at a pinned checkout and diffs it
    row-for-row against the kept side. The recompute SQL is ALSO the self-heal
    source (one SQL, two roles), so the oracle and the self-heal can never diverge.
    Both deterministic consumers in scope (the ontology rollup and the console
    count rollup) use this.
    """

    def __init__(
        self,
        recompute_sql: str,
        key_columns: Sequence[str],
        value_columns: Sequence[str],
    ) -> None:
        self.recompute_sql = recompute_sql
        self.key_columns = tuple(key_columns)
        self.value_columns = tuple(value_columns)

    def recompute(self, checkout: Any) -> dict[KeyTuple, ValueTuple]:
        """Ground truth: the full recompute at ``checkout`` keyed by key columns.

        Value tuples are canonicalized (an integral count returned as ``Decimal`` /
        ``bigint`` / ``int`` compares equal), so a derivation is never mis-flagged as
        drift merely because the DB driver typed ``count(*)`` differently from the kept
        side.
        """
        out: dict[KeyTuple, ValueTuple] = {}
        for row in checkout.query(self.recompute_sql):
            key = tuple(row[c] for c in self.key_columns)
            out[key] = _canon_tuple(row[c] for c in self.value_columns)
        return out

    def diff(
        self,
        kept: Mapping[KeyTuple, ValueTuple],
        recomputed: Mapping[KeyTuple, ValueTuple],
    ) -> OracleResult:
        """Compare the kept side to ground truth. ``ok`` iff they are identical.

        Both sides' value tuples are canonicalized before comparison, so a kept value
        and a recomputed value that differ only in numeric DB-driver type are equal.
        """
        kept_c = {k: _canon_tuple(v) for k, v in kept.items()}
        recomputed_c = {k: _canon_tuple(v) for k, v in recomputed.items()}
        missing = [k for k in recomputed_c if k not in kept_c]
        extra = [k for k in kept_c if k not in recomputed_c]
        mismatched = [
            k for k in kept_c if k in recomputed_c and kept_c[k] != recomputed_c[k]
        ]
        ok = not (missing or extra or mismatched)
        return OracleResult(ok, missing, extra, mismatched)


class CallableCorrectnessOracle(CorrectnessOracle):
    """Deterministic correctness oracle backed by a Python recompute callable.

    The governance contract remains the same as ``CorrectnessOracle``: the callable
    receives a sealed checkout and returns the full truth table keyed by the
    derivation's key columns. ``diff`` stays inherited so SQL and callable
    deterministic rows use identical canonical key/value comparison.
    """

    def __init__(
        self,
        recompute: Callable[[Any], Mapping[KeyTuple, ValueTuple]],
        key_columns: Sequence[str],
        value_columns: Sequence[str],
        *,
        label: str = "<callable>",
    ) -> None:
        super().__init__(label, key_columns, value_columns)
        self._recompute_callable = recompute

    def recompute(self, checkout: Any) -> dict[KeyTuple, ValueTuple]:
        out: dict[KeyTuple, ValueTuple] = {}
        for key, value in self._recompute_callable(checkout).items():
            out[tuple(key)] = _canon_tuple(value)
        return out


class ProjectionParityOracle:
    """PROJECTION-PARITY runner for JUDGMENT / nondeterministic producers.

    Compares the kept rows to a REPROJECTION of a FROZEN output (the persisted
    producer output, e.g. cached LLM extraction rows) -- NEVER a re-run of the
    producer. It proves the maintenance is faithful, NOT that the judgment is
    correct. No deterministic consumer in this slice uses it; the kit ships the
    SHAPE (req.reusable-governance-kit) so judgments can register later.
    """

    def __init__(
        self,
        reproject: Callable[[Any, Any], dict[KeyTuple, ValueTuple]],
        key_columns: Sequence[str],
    ) -> None:
        self.reproject = reproject
        self.key_columns = tuple(key_columns)

    def diff(
        self,
        kept: Mapping[KeyTuple, ValueTuple],
        frozen_output: Any,
        checkout: Any,
    ) -> OracleResult:
        """Diff the kept rows against a reprojection of the frozen output.

        Both sides' value tuples are canonicalized (see ``_canon_tuple``), so numeric
        DB-driver type differences do not read as drift.
        """
        reprojected = self.reproject(frozen_output, checkout)
        kept_c = {k: _canon_tuple(v) for k, v in kept.items()}
        reprojected_c = {k: _canon_tuple(v) for k, v in reprojected.items()}
        missing = [k for k in reprojected_c if k not in kept_c]
        extra = [k for k in kept_c if k not in reprojected_c]
        mismatched = [
            k for k in kept_c if k in reprojected_c and kept_c[k] != reprojected_c[k]
        ]
        ok = not (missing or extra or mismatched)
        return OracleResult(ok, missing, extra, mismatched)


Oracle = CorrectnessOracle | ProjectionParityOracle


def _canon_value(value: Any) -> Any:
    """Canonicalize a value for parity comparison: an integral number -- ``int``, a
    ``bigint`` returned as ``int``, ``Decimal('5')``, or ``5.0`` -- compares equal
    regardless of its DB-driver type, so a ``count(*)`` returned as ``Decimal`` does
    not spuriously mismatch an ``int`` kept value. ``bool``, non-integral values, and
    non-numeric values pass through unchanged."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else value
    if isinstance(value, float):
        return int(value) if value.is_integer() else value
    return value


def _canon_tuple(values: Any) -> ValueTuple:
    return tuple(_canon_value(v) for v in values)


def parity_check(
    oracle: CorrectnessOracle,
    checkout: Any,
    kept: Mapping[KeyTuple, ValueTuple],
) -> OracleResult:
    """Run a deterministic derivation's CORRECTNESS oracle: recompute ground truth at
    the pinned ``checkout`` and diff the ``kept`` rows against it.

    The SINGLE entrypoint a consumer's drift test (and a future self-heal parity gate)
    share, so a derivation's recompute SQL has exactly one meaning -- "one SQL, two
    roles": the oracle and the self-heal source can never diverge. ``kept`` maps each
    key tuple to its value tuple (the derivation's maintained rows).
    """
    return oracle.diff(kept, oracle.recompute(checkout))


# --------------------------------------------------------------------------- #
# Self-heal cadence                                                           #
# --------------------------------------------------------------------------- #
def read_self_heal_cadence(env_var: str, default: int = 25) -> int:
    """Resolve the periodic self-heal cadence (in generations).

    Byte-for-byte the semantics of ``ontology_rollup._self_heal_cadence_default``:
    a missing env var -> ``default``; a non-integer or non-positive value ->
    ``logger.warning`` + ``default`` (never silently disable the self-heal).
    """
    raw = os.environ.get(env_var)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        logger.warning(
            "ignoring non-integer %s=%r; using default cadence %d",
            env_var,
            raw,
            default,
        )
        return default
    if value <= 0:
        logger.warning(
            "ignoring non-positive %s=%r; using default cadence %d",
            env_var,
            raw,
            default,
        )
        return default
    return value


def cadence_due(elapsed: int | None, self_heal_every: int) -> bool:
    """True when a periodic full recount is due.

    Preserved byte-for-byte from ``ontology_rollup.py``: the cadence fires when the
    NEXT incremental cycle would push the counter to ``>= self_heal_every`` -- i.e.
    ``elapsed is None or (elapsed + 1) >= self_heal_every`` -- guarded on a positive
    cadence. ``elapsed is None`` (no readable counter) is treated as due.
    """
    if self_heal_every <= 0:
        return False
    return elapsed is None or (elapsed + 1) >= self_heal_every


# --------------------------------------------------------------------------- #
# The forcing-trigger framework                                               #
# --------------------------------------------------------------------------- #
def decide_maintain_mode(
    *,
    has_prior: bool,
    inputs: ForceInputs,
    force_full_predicates: Sequence[ForcePredicate],
    elapsed: int | None,
    self_heal_every: int,
) -> MaintainMode:
    """Decide full-recount vs incremental, preserving the rollup's exact order.

    Evaluation order (reproducing ``ontology_rollup.maintain_ontology_rollup``):
      1. ``inputs.force_full``        -> "forced"
      2. not ``inputs.scope_available`` -> "scope_unavailable"
      3. not ``has_prior``            -> "first_publish"
      4. each ``force_full_predicates`` in registration order -> ``predicate.reason``
         (FIRST match wins; the rollup registers one -- node_retype -- at this slot)
      5. ``cadence_due(elapsed, N)``  -> "periodic_self_heal"
      else incremental (``MaintainMode(full=False, reason=None)``).
    """
    if inputs.force_full:
        return MaintainMode(True, FULL_REASON_FORCED)
    if not inputs.scope_available:
        return MaintainMode(True, FULL_REASON_SCOPE_UNAVAILABLE)
    if not has_prior:
        return MaintainMode(True, FULL_REASON_FIRST_PUBLISH)
    for predicate in force_full_predicates:
        if predicate.fires(inputs):
            return MaintainMode(True, predicate.reason)
    if cadence_due(elapsed, self_heal_every):
        return MaintainMode(True, FULL_REASON_SELF_HEAL)
    return MaintainMode(False, None)


# --------------------------------------------------------------------------- #
# Sentinel helpers (the cadence-counter value is supplied by the caller)      #
# --------------------------------------------------------------------------- #
def sentinel_row_dict(spec: SentinelSpec, cadence_counter: int) -> dict[str, Any]:
    """Build the reserved sentinel row: every PK column == ``SENTINEL_TOKEN`` and
    ``spec.cadence_column`` == the caller-supplied counter (full -> 0; incremental
    -> prior + 1). The kit never computes the counter; the consumer's callback does.
    """
    row = sentinel_pk_values(spec.pk_columns)
    row[spec.cadence_column] = int(cadence_counter)
    return row


def is_sentinel_row(row: Mapping[str, Any], spec: SentinelSpec) -> bool:
    """True iff ``row`` is the reserved sentinel (every PK column == SENTINEL_TOKEN).

    Schema-agnostic: iterates ``spec.pk_columns`` rather than indexing a fixed
    arity, so it works for the rollup's 4-column PK and the count rollup's 2-column
    PK alike. Read and write agree by construction (one ``SENTINEL_TOKEN``).
    """
    return all(row.get(column) == SENTINEL_TOKEN for column in spec.pk_columns)


# --------------------------------------------------------------------------- #
# The loud-fail read (absence only; drift is the oracle's job)                #
# --------------------------------------------------------------------------- #
class MaintainedDerivationUnavailable(Exception):
    """Raised when no maintained value exists for a pinned generation.

    A loud failure on purpose, carrying a ``reason`` discriminator
    (``"checkpoint_absent"`` | ``"table_unregistered"`` | ``"no_sentinel"``): the
    read must NEVER silently fall back to an O(graph) scan, a stale value, or zero.
    Covers ABSENCE only -- a present-but-drifted value is caught by the parity
    oracle and bounded by the self-heal cadence, not by this read.
    """

    def __init__(
        self, generation_id: int | str, reason: str, detail: str = ""
    ) -> None:
        self.generation_id = generation_id
        self.reason = reason
        self.detail = detail
        message = (
            f"maintained derivation unavailable for generation_id="
            f"{generation_id!r} (reason={reason})"
        )
        if detail:
            message = f"{message}: {detail}"
        super().__init__(message)


def partition_sentinel_rows(
    raw_rows: Sequence[Mapping[str, Any]],
    *,
    sentinel_spec: SentinelSpec,
    row_mapper: Callable[[Mapping[str, Any]], T],
    generation_id: int | str,
) -> list[T]:
    """Split a maintained table's rows into the sentinel (proof the hook ran) and
    the data rows, mapping the latter through ``row_mapper``.

    Raises ``MaintainedDerivationUnavailable(reason="no_sentinel")`` when no
    sentinel is present -- the maintain hook never ran for this generation, so the
    read must fail loud rather than return a (possibly spuriously empty) result.
    A maintained-but-empty generation (sentinel present, zero data rows) returns
    ``[]``. Reproduces ``read_ontology_rollup``'s sentinel-separation contract.
    """
    rows: list[T] = []
    sentinel_present = False
    for raw in raw_rows:
        if is_sentinel_row(raw, sentinel_spec):
            sentinel_present = True
            continue
        rows.append(row_mapper(raw))
    if not sentinel_present:
        raise MaintainedDerivationUnavailable(
            generation_id,
            "no_sentinel",
            "the maintain hook never ran for this generation; the read must not "
            "fall back to the O(graph) scan",
        )
    return rows


# --------------------------------------------------------------------------- #
# Storage strategy + DB-coupled helpers + the maintain() driver               #
#                                                                             #
# These touch a Chronos store; they defer the ``graph_store`` import into     #
# their bodies so the read path can import the kit without a module-level     #
# ``graph_store`` import (the import-light contract is module-level only).    #
# --------------------------------------------------------------------------- #
class StorageStrategy(Protocol):
    """What the ``maintain`` driver needs from a derivation's storage.

    The table read/write itself goes through the Chronos store's generic session
    methods (``checkout`` / ``transaction`` / ``upsert_rows`` / ``delete_keys``),
    so a strategy only owns the table identity + idempotent registration.
    """

    table: str

    def ensure_registered(self, store: Any) -> None:
        """Ensure the backing storage is registered before maintenance."""
        raise NotImplementedError


@dataclass(frozen=True)
class ChronosRegisteredTable:
    """Strategy A: a Chronos-REGISTERED table -- branch/fork/as-of reconstruction
    for free; a no-write cycle carries prior rows forward via the interval
    predicate.

    ``ensure_fn`` is the consumer's CREATE-TABLE + ``register_table`` for THIS table
    (the ontology rollup passes ``store.ensure_ontology_rollup_table``), so the DDL
    stays byte-identical; the adapter only generalizes the cheap already-registered
    probe-gate that lets the steady-state publish skip the redundant DDL + commits
    (reproducing ``ontology_rollup._is_rollup_table_registered`` / ``_ensure_registered``).
    """

    table: str
    ensure_fn: Callable[[Any], None]

    def _is_registered(self, store: Any) -> bool:
        try:
            backend = store.context._backend  # interval/metadata Chronos backend
            backend.refresh_registries()
            return self.table in backend.tables
        except Exception:
            logger.exception(
                "failed to probe Chronos table registration: table=%s",
                self.table,
            )
            raise

    def ensure_registered(self, store: Any) -> None:
        if self._is_registered(store):
            return
        try:
            self.ensure_fn(store)
        except Exception:  # noqa: BLE001 -- surfaced by the caller's loud-failure path
            logger.exception("failed to ensure %s is registered", self.table)
            raise


@dataclass(frozen=True)
class GenericChronosKeptTable:
    """Deployment-owned Chronos table strategy backed by explicit DDL.

    Existing substrate tables keep their table-specific ``ensure_*`` wrappers. This
    strategy gives deployments the same create + register path without adding a new
    substrate store method for every kept table.
    """

    table: str
    primary_key: Sequence[str]
    create_table_sql: str
    columns: Sequence[str] | None = None
    upgrade_table_sql: str = ""

    def _registered_meta(self, store: Any) -> Any | None:
        try:
            backend = store.context._backend
            backend.refresh_registries()
            return backend.tables.get(self.table)
        except Exception:
            logger.exception(
                "failed to read Chronos table registration metadata: table=%s",
                self.table,
            )
            raise

    def _validate_registered(self, meta: Any) -> None:
        expected_pk = tuple(str(column) for column in self.primary_key)
        actual_pk = tuple(str(column) for column in getattr(meta, "pk_columns", ()))
        if actual_pk != expected_pk:
            raise ValueError(
                f"{self.table}: registered Chronos primary key {list(actual_pk)!r} "
                f"does not match expected {list(expected_pk)!r}"
            )
        expected_columns = tuple(str(column) for column in (self.columns or ()))
        if expected_columns:
            actual_columns = {
                str(column) for column in getattr(meta, "columns", ())
            }
            missing = [column for column in expected_columns if column not in actual_columns]
            if missing:
                raise ValueError(
                    f"{self.table}: registered Chronos table is missing expected "
                    f"columns {missing!r}"
                )

    def ensure_registered(self, store: Any) -> None:
        if not self.table:
            raise ValueError("generic Chronos kept table requires a table name")
        if not self.primary_key:
            raise ValueError(
                f"{self.table}: generic Chronos kept table requires primary key columns"
            )
        if not self.create_table_sql.strip():
            raise ValueError(f"{self.table}: generic Chronos kept table requires DDL")
        registered = self._registered_meta(store)
        owns_transaction = bool(getattr(store.context, "autocommit", True))
        if registered is not None:
            if self.upgrade_table_sql.strip():
                store.context.db.execute(self.upgrade_table_sql)
                if owns_transaction:
                    store.context.db.commit()
                # Re-registering an existing logical table is Chronos' supported
                # additive-schema upgrade path: it copies newly declared logical
                # columns onto physical storage and refreshes registry metadata.
                store.context.register_table(self.table, list(self.primary_key))
                store.context._backend.refresh_registries()
                registered = self._registered_meta(store)
            self._validate_registered(registered)
            self._ensure_honest_read_view(store)
            return
        db = store.context.db
        try:
            db.execute(self.create_table_sql)
            if self.upgrade_table_sql.strip():
                db.execute(self.upgrade_table_sql)
            if owns_transaction:
                db.commit()
            store.context.register_table(self.table, list(self.primary_key))
            if owns_transaction:
                db.commit()
            registered = self._registered_meta(store)
            if registered is not None:
                self._validate_registered(registered)
            self._ensure_honest_read_view(store)
        except Exception:  # noqa: BLE001 -- surfaced by the caller's loud-failure path
            logger.exception("failed to ensure %s is registered", self.table)
            raise

    def _ensure_honest_read_view(self, store: Any) -> None:
        """Install the ``v_current_<table>`` honest-read view for this kept
        table (#706). A Chronos-registered kept table's base rows are schema
        templates that are never materialized in place, so a bare SQL read
        returns a deceptive zero; the honest-read view raises instead. This is
        driven off the registration path so every kept table is covered without
        a hardcoded table list."""
        from okg.substrate.db.management import ensure_kept_table_honest_read_view

        ensure_kept_table_honest_read_view(store.context.db, self.table)


@dataclass(frozen=True)
class ExplicitIntervalTable:
    """Strategy B: an explicit branch-ordinal interval table.

    This is the non-Chronos storage default extracted from the effective-attribute
    interval work. A row is live for a pinned generation when the generation resolves
    to ``(branch_id, ordinal)`` and the row satisfies ``from_ord <= ordinal < to_ord``.
    Consumers still own their DDL and snapshot source; the strategy owns the common
    close/open/backfill/stab mechanics and the loud dependency on generation ordinals.
    """

    table: str
    snapshot_table: str
    payload_columns: Sequence[str]
    read_columns: Sequence[str]
    target_kinds: Sequence[str] = ("node", "edge")
    target_kind_column: str = "target_kind"
    target_id_column: str = "target_id"
    generation_column: str = "generation_id"
    namespace_column: str = "namespace"
    branch_column: str = "branch_id"
    from_column: str = "from_ord"
    to_column: str = "to_ord"
    materialized_at_column: str | None = "materialized_at"
    open_ordinal: int = EXPLICIT_INTERVAL_OPEN_ORD
    primary_key_columns: Sequence[str] = (
        "target_kind",
        "target_id",
        "namespace",
        "branch_id",
        "from_ord",
    )

    def validate(self) -> None:
        if not self.table:
            raise ValueError("explicit interval table requires a table name")
        if not self.snapshot_table:
            raise ValueError("explicit interval table requires a snapshot table")
        if not self.payload_columns:
            raise ValueError(
                f"{self.table}: explicit interval table requires payload columns"
            )
        if not self.read_columns:
            raise ValueError(
                f"{self.table}: explicit interval table requires read columns"
            )
        if not self.target_kinds:
            raise ValueError(
                f"{self.table}: explicit interval table requires target kinds"
            )


def explicit_interval_sync_temp_name(prefix: str, target_kind: str) -> str:
    safe_kind = "".join(ch if ch.isalnum() else "_" for ch in target_kind)[:32]
    return f"_okg_md_iv_{prefix}_{safe_kind}_{uuid.uuid4().hex[:12]}"


def explicit_interval_payload_unchanged_sql(
    spec: ExplicitIntervalTable,
    *,
    snapshot_alias: str = "s",
    interval_alias: str = "i",
) -> str:
    """The one rule for "this open interval still holds the snapshot's value".

    Every payload column must be ``IS NOT DISTINCT FROM`` its snapshot column.
    The full sync, the dirty close and the effective-view reanchor all build
    their keep-open decision from this predicate, so they cannot disagree about
    what counts as unchanged.
    """
    return " AND ".join(
        f"{snapshot_alias}.{column} IS NOT DISTINCT FROM {interval_alias}.{column}"
        for column in spec.payload_columns
    )


def _first_column_value(row: Any) -> Any:
    if isinstance(row, Mapping):
        return next(iter(row.values()))
    return row[0]


def stage_explicit_snapshot_slice(
    cur: Any,
    *,
    spec: ExplicitIntervalTable,
    target_kind: str,
    params: dict[str, Any],
) -> str:
    """Copy one generation's snapshot rows of one target kind into a temp table.

    Returns the temp table's name. It holds ``target_id`` plus the payload
    columns, has a unique index on ``target_id`` and has been analyzed, so a
    per-interval probe into it is one index lookup whatever the planner
    believes about the shared snapshot table.

    Why this exists (okg#1718): the generation being folded was written moments
    ago, so the shared snapshot table's statistics do not know it yet. The
    planner then estimates one row for ``generation_id = <new>`` and probes the
    snapshot through the ``(generation_id, namespace)`` index once per open
    interval, reading the whole generation each time: 20,030 probes at 2.7 ms
    for 20k targets, and past the 900 s statement timeout at 300k. Copying the
    slice reads the generation once, and the temp table's own statistics are
    exact. It takes no lock on the shared table beyond a read and leaves its
    statistics alone.

    The caller drops the table when done (``DROP TABLE IF EXISTS``); it is a
    session temp table, so a failed transaction discards it too.
    """
    spec.validate()
    snapshot_temp = explicit_interval_sync_temp_name("snap", target_kind)
    target_id = spec.target_id_column
    target_and_payload_cols = ", ".join((target_id, *spec.payload_columns))
    cur.execute(
        f"""
        CREATE TEMP TABLE {snapshot_temp} AS
        SELECT {target_and_payload_cols}
          FROM {spec.snapshot_table}
         WHERE {spec.generation_column} = %(gen)s
           AND {spec.namespace_column} = %(ns)s
           AND {spec.target_kind_column} = %(tk)s
        """,
        params,
    )
    cur.execute(
        f"CREATE UNIQUE INDEX {snapshot_temp}_{target_id}_idx "
        f"ON {snapshot_temp} ({target_id})"
    )
    cur.execute(f"ANALYZE {snapshot_temp}")
    return snapshot_temp


def open_missing_explicit_intervals_from_staged(
    cur: Any,
    *,
    spec: ExplicitIntervalTable,
    params: dict[str, Any],
    staged_snapshot: str,
) -> int:
    """Open an interval for every staged target that has no open interval.

    ``staged_snapshot`` comes from ``stage_explicit_snapshot_slice`` and is
    consumed: rows whose target already has an open interval are deleted from
    it first, then the rest are inserted in keyset batches.

    The two steps are separate on purpose (okg#1718). A single ``INSERT ...
    SELECT ... WHERE NOT EXISTS (open interval)`` reads the table it is writing:
    when the planner thinks that table is empty (the first publish) it re-scans
    it once per inserted row, and every scan also reads the rows the statement
    has inserted so far -- quadratic in the generation's size. Deciding the
    missing set before inserting leaves nothing to re-scan.
    """
    target_id = spec.target_id_column
    cur.execute(
        f"""
        DELETE FROM {staged_snapshot} AS s
         USING {spec.table} AS i
         WHERE i.{spec.branch_column} = %(br)s
           AND i.{spec.namespace_column} = %(ns)s
           AND i.{spec.target_kind_column} = %(tk)s
           AND i.{target_id} = s.{target_id}
           AND i.{spec.to_column} = %(open)s
        """,
        params,
    )
    insert_cols = ", ".join(
        (
            spec.target_kind_column,
            target_id,
            spec.namespace_column,
            spec.branch_column,
            spec.from_column,
            spec.to_column,
            *spec.payload_columns,
        )
    )
    batch_cols = ", ".join((target_id, *spec.payload_columns))
    select_payload = ", ".join(f"s.{column}" for column in spec.payload_columns)
    batch_size = _EXPLICIT_INTERVAL_INSERT_BATCH_SIZE
    inserted_total = 0
    after_target_id: str | None = None
    while True:
        batch_params = {**params, "batch_size": batch_size}
        after_clause = ""
        if after_target_id is not None:
            after_clause = f"WHERE {target_id} > %(after_target_id)s"
            batch_params["after_target_id"] = after_target_id
        cur.execute(
            f"""
            WITH snapshot_batch AS MATERIALIZED (
                SELECT {batch_cols}
                  FROM {staged_snapshot}
                  {after_clause}
                 ORDER BY {target_id}
                 LIMIT %(batch_size)s
            ), inserted AS (
                INSERT INTO {spec.table} ({insert_cols})
                SELECT %(tk)s, s.{target_id}, %(ns)s, %(br)s, %(new)s,
                       %(open)s, {select_payload}
                  FROM snapshot_batch s
                ON CONFLICT ({", ".join(spec.primary_key_columns)}) DO NOTHING
                RETURNING 1
            )
            SELECT (SELECT count(*) FROM inserted) AS inserted_count,
                   (SELECT max({target_id}) FROM snapshot_batch) AS last_id,
                   (SELECT count(*) FROM snapshot_batch) AS batch_count
            """,
            batch_params,
        )
        row = cur.fetchone()
        if isinstance(row, Mapping):
            inserted_count = int(row["inserted_count"])
            last_id = row["last_id"]
            batch_count = int(row["batch_count"])
        else:
            inserted_count = int(row[0])
            last_id = row[1]
            batch_count = int(row[2])
        inserted_total += inserted_count
        if batch_count == 0:
            break
        if last_id is None:
            raise RuntimeError(
                "explicit interval batch did not return its last target id"
            )
        after_target_id = str(last_id)
        if batch_count < batch_size:
            break
    return inserted_total


def close_dirty_explicit_intervals(
    cur: Any,
    *,
    spec: ExplicitIntervalTable,
    target_kind: str,
    params: dict[str, Any],
    close_ids: set[str],
) -> int:
    """Close dirty open intervals without scanning the whole live front.

    Delta-mode snapshots contain only changed targets, so absence is meaningful only
    inside the dirty set. Staging the dirty ids plus the changed snapshot payload
    keeps the close operation O(changed) and avoids planner choices that repeatedly
    compare payloads against a large open front.
    """
    spec.validate()
    if not close_ids:
        return 0
    dirty_temp = explicit_interval_sync_temp_name("dirty", target_kind)
    target_id = spec.target_id_column
    cur.execute(
        f"""
        CREATE TEMP TABLE {dirty_temp} (
          {target_id} TEXT PRIMARY KEY
        )
        """
    )
    with cur.copy(f"COPY {dirty_temp} ({target_id}) FROM STDIN") as copy:
        for raw_target_id in sorted(close_ids):
            copy.write_row((str(raw_target_id),))
    snapshot_temp = stage_explicit_snapshot_slice(
        cur, spec=spec, target_kind=target_kind, params=params
    )
    cur.execute(f"ANALYZE {dirty_temp}")
    changed = f"NOT ({explicit_interval_payload_unchanged_sql(spec)})"
    materialized_set = (
        f", {spec.materialized_at_column} = now()"
        if spec.materialized_at_column
        else ""
    )
    cur.execute(
        f"""
        UPDATE {spec.table} AS i
           SET {spec.to_column} = %(new)s{materialized_set}
          FROM {dirty_temp} d
          LEFT JOIN {snapshot_temp} s ON s.{target_id} = d.{target_id}
         WHERE i.{spec.branch_column} = %(br)s
           AND i.{spec.namespace_column} = %(ns)s
           AND i.{spec.target_kind_column} = %(tk)s
           AND i.{spec.to_column} = %(open)s
           AND i.{target_id} = d.{target_id}
           AND (s.{target_id} IS NULL OR {changed})
        """,
        params,
    )
    closed = int(cur.rowcount or 0)
    cur.execute(f"DROP TABLE IF EXISTS {snapshot_temp}")
    cur.execute(f"DROP TABLE IF EXISTS {dirty_temp}")
    return closed


def _insert_missing_explicit_intervals(
    cur: Any,
    *,
    spec: ExplicitIntervalTable,
    params: dict[str, Any],
    insert_cols: str,
    select_cols: str,
) -> int:
    """Open snapshot rows in bounded keyset batches.

    Initial loads can contain millions of JSON-heavy rows. A single INSERT ... SELECT
    makes the whole load one statement-timeout unit even though the work is naturally
    divisible by target id. Keyset batches preserve one atomic caller transaction while
    bounding each statement and avoiding OFFSET rescans.
    """
    batch_size = _EXPLICIT_INTERVAL_INSERT_BATCH_SIZE
    if batch_size <= 0:
        raise ValueError("explicit interval insert batch size must be positive")
    snapshot_columns = ", ".join(
        (
            spec.target_kind_column,
            spec.target_id_column,
            spec.namespace_column,
            *spec.payload_columns,
        )
    )
    inserted_total = 0
    after_target_id: str | None = None
    while True:
        after_clause = ""
        batch_params = {**params, "batch_size": batch_size}
        if after_target_id is not None:
            after_clause = f"AND {spec.target_id_column} > %(after_target_id)s"
            batch_params["after_target_id"] = after_target_id
        cur.execute(
            f"""
            WITH snapshot_batch AS MATERIALIZED (
                SELECT {snapshot_columns}
                  FROM {spec.snapshot_table}
                 WHERE {spec.generation_column} = %(gen)s
                   AND {spec.namespace_column} = %(ns)s
                   AND {spec.target_kind_column} = %(tk)s
                   {after_clause}
                 ORDER BY {spec.target_id_column}
                 LIMIT %(batch_size)s
            ), inserted AS (
                INSERT INTO {spec.table} ({insert_cols})
                SELECT s.{spec.target_kind_column}, s.{spec.target_id_column},
                       s.{spec.namespace_column}, %(br)s, %(new)s, %(open)s,
                       {select_cols}
                  FROM snapshot_batch s
                 WHERE NOT EXISTS (
                       SELECT 1 FROM {spec.table} i
                        WHERE i.{spec.branch_column} = %(br)s
                          AND i.{spec.namespace_column} = %(ns)s
                          AND i.{spec.target_kind_column} = %(tk)s
                          AND i.{spec.target_id_column} = s.{spec.target_id_column}
                          AND i.{spec.to_column} = %(open)s
                 )
                ON CONFLICT ({", ".join(spec.primary_key_columns)}) DO NOTHING
                RETURNING 1
            )
            SELECT (SELECT count(*) FROM inserted) AS inserted_count,
                   (SELECT max({spec.target_id_column}) FROM snapshot_batch) AS last_id,
                   (SELECT count(*) FROM snapshot_batch) AS batch_count
            """,
            batch_params,
        )
        row = cur.fetchone()
        if isinstance(row, Mapping):
            inserted_count = int(row["inserted_count"])
            last_id = row["last_id"]
            batch_count = int(row["batch_count"])
        else:
            inserted_count = int(row[0])
            last_id = row[1]
            batch_count = int(row[2])
        inserted_total += inserted_count
        if batch_count == 0:
            break
        if last_id is None:
            raise RuntimeError("explicit interval batch did not return its last target id")
        after_target_id = str(last_id)
        if batch_count < batch_size:
            break
    return inserted_total


def sync_explicit_interval_rows_for_generation(
    conn: Any,
    db: Any,
    *,
    spec: ExplicitIntervalTable,
    generation_id: int | str,
    namespace: str = "default",
    dirty_target_ids_by_kind: Mapping[str, Sequence[str] | set[str] | frozenset[str]]
    | None = None,
    grant_fn: Callable[[Any], None] | None = None,
) -> dict[str, Any]:
    """Fold a generation snapshot into an explicit interval table.

    Full mode (``dirty_target_ids_by_kind is None``) expects a full snapshot and can
    close any open interval that is absent or changed. Delta mode expects a
    changed-only snapshot and closes only the dirty union plus freshly snapshotted
    ids, preserving unchanged targets that are absent from the delta snapshot.
    """
    from okg.substrate.graph_store_internal.generation_ordinal import (
        resolve_generation_ordinal_row,
    )

    spec.validate()
    gen = str(generation_id)
    branch_id, new_ord = resolve_generation_ordinal_row(db, gen)
    unchanged = explicit_interval_payload_unchanged_sql(spec)
    insert_cols = ", ".join(
        (
            spec.target_kind_column,
            spec.target_id_column,
            spec.namespace_column,
            spec.branch_column,
            spec.from_column,
            spec.to_column,
            *spec.payload_columns,
        )
    )
    select_cols = ", ".join(f"s.{column}" for column in spec.payload_columns)
    counts: dict[str, dict[str, int]] = {}
    # The production publisher uses an autocommit connection. Keep close/open batches
    # atomic so a timeout cannot expose a partial interval front to a retry or reader.
    with conn.transaction(), conn.cursor() as cur:
        for target_kind in spec.target_kinds:
            params = {
                "new": new_ord,
                "br": branch_id,
                "ns": namespace,
                "tk": target_kind,
                "open": spec.open_ordinal,
                "gen": gen,
            }
            if dirty_target_ids_by_kind is not None:
                dirty_ids = {
                    str(target_id)
                    for target_id in dirty_target_ids_by_kind.get(target_kind, ())
                }
                cur.execute(
                    f"""
                    SELECT {spec.target_id_column}
                      FROM {spec.snapshot_table}
                     WHERE {spec.generation_column} = %(gen)s
                       AND {spec.namespace_column} = %(ns)s
                       AND {spec.target_kind_column} = %(tk)s
                    """,
                    params,
                )
                close_ids = set(dirty_ids)
                close_ids.update(
                    str(_first_column_value(row)) for row in cur.fetchall()
                )
                closed = close_dirty_explicit_intervals(
                    cur,
                    spec=spec,
                    target_kind=target_kind,
                    params=params,
                    close_ids=close_ids,
                )
                opened = _insert_missing_explicit_intervals(
                    cur,
                    spec=spec,
                    params=params,
                    insert_cols=insert_cols,
                    select_cols=select_cols,
                )
            else:
                # Full mode folds a whole generation, so both steps read the
                # staged slice instead of the shared snapshot table, whose
                # statistics do not know this generation yet (okg#1718).
                materialized_set = (
                    f", {spec.materialized_at_column} = now()"
                    if spec.materialized_at_column
                    else ""
                )
                snapshot_temp = stage_explicit_snapshot_slice(
                    cur, spec=spec, target_kind=target_kind, params=params
                )
                cur.execute(
                    f"""
                    UPDATE {spec.table} AS i
                       SET {spec.to_column} = %(new)s{materialized_set}
                     WHERE i.{spec.branch_column} = %(br)s
                       AND i.{spec.namespace_column} = %(ns)s
                       AND i.{spec.target_kind_column} = %(tk)s
                       AND i.{spec.to_column} = %(open)s
                       AND NOT EXISTS (
                           SELECT 1 FROM {snapshot_temp} s
                            WHERE s.{spec.target_id_column} = i.{spec.target_id_column}
                              AND {unchanged}
                       )
                    """,
                    params,
                )
                closed = int(cur.rowcount or 0)
                opened = open_missing_explicit_intervals_from_staged(
                    cur,
                    spec=spec,
                    params=params,
                    staged_snapshot=snapshot_temp,
                )
                cur.execute(f"DROP TABLE IF EXISTS {snapshot_temp}")
            counts[str(target_kind)] = {
                "closed": int(closed),
                "opened": opened,
            }
    if grant_fn is not None:
        grant_fn(conn)
    return {
        "generation_id": gen,
        "branch_id": branch_id,
        "ordinal": new_ord,
        "counts": counts,
        "rows_written": sum(c["closed"] + c["opened"] for c in counts.values()),
    }


def _explicit_interval_backfill_fingerprint_sql(
    spec: ExplicitIntervalTable, alias: str
) -> str:
    payload = " || E'\\x1f' || ".join(
        f"COALESCE({alias}.{col}::text, E'\\x1e')"
        for col in (
            spec.target_kind_column,
            spec.target_id_column,
            spec.namespace_column,
            *spec.payload_columns,
        )
    )
    return f"COALESCE(sum(hashtextextended({payload}, 0)::numeric), 0)"


def _already_backfilled_explicit_interval_result(
    conn: Any,
    *,
    spec: ExplicitIntervalTable,
    generation_id: str,
    branch_id: str,
    ordinal: int,
    namespace: str,
) -> dict[str, Any] | None:
    snap_fp = _explicit_interval_backfill_fingerprint_sql(spec, "s")
    iv_fp = _explicit_interval_backfill_fingerprint_sql(spec, "i")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT count(*) AS rows
              FROM {spec.table}
             WHERE {spec.branch_column} = %s
               AND {spec.namespace_column} = %s
               AND {spec.to_column} = %s
               AND {spec.from_column} <> %s
            """,
            (branch_id, namespace, spec.open_ordinal, ordinal),
        )
        row = cur.fetchone()
        if int(_first_column_value(row)) != 0:
            return None
        counts: dict[str, dict[str, int]] = {}
        for target_kind in spec.target_kinds:
            cur.execute(
                f"""
                SELECT count(*)::bigint AS rows, {snap_fp} AS fingerprint
                  FROM {spec.snapshot_table} AS s
                 WHERE s.{spec.generation_column} = %s
                   AND s.{spec.namespace_column} = %s
                   AND s.{spec.target_kind_column} = %s
                """,
                (generation_id, namespace, target_kind),
            )
            snap_row = cur.fetchone()
            cur.execute(
                f"""
                SELECT count(*)::bigint AS rows, {iv_fp} AS fingerprint
                  FROM {spec.table} AS i
                 WHERE i.{spec.branch_column} = %s
                   AND i.{spec.namespace_column} = %s
                   AND i.{spec.target_kind_column} = %s
                   AND i.{spec.from_column} = %s
                   AND i.{spec.to_column} = %s
                """,
                (branch_id, namespace, target_kind, ordinal, spec.open_ordinal),
            )
            iv_row = cur.fetchone()
            snap_values = tuple(snap_row.values()) if isinstance(snap_row, Mapping) else snap_row
            iv_values = tuple(iv_row.values()) if isinstance(iv_row, Mapping) else iv_row
            if int(snap_values[0]) != int(iv_values[0]) or snap_values[1] != iv_values[1]:
                return None
            counts[str(target_kind)] = {"closed": 0, "opened": int(iv_values[0])}
    return {
        "generation_id": generation_id,
        "branch_id": branch_id,
        "ordinal": ordinal,
        "counts": counts,
        "rows_written": 0,
        "mode": "already_backfilled",
    }


def backfill_explicit_interval_rows_for_generation(
    conn: Any,
    db: Any,
    *,
    spec: ExplicitIntervalTable,
    generation_id: int | str,
    namespace: str = "default",
    grant_fn: Callable[[Any], None] | None = None,
) -> dict[str, Any]:
    """Backfill a full snapshot into the explicit interval table and verify it."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.graph_store_internal.generation_ordinal import (
        resolve_generation_ordinal_row,
    )

    spec.validate()
    gen = str(generation_id)
    branch_id, new_ord = resolve_generation_ordinal_row(db, gen)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT 1
              FROM {spec.table}
             WHERE {spec.branch_column} = %s
               AND {spec.namespace_column} = %s
               AND {spec.to_column} = %s
             LIMIT 1
            """,
            (branch_id, namespace, spec.open_ordinal),
        )
        empty_live_front = cur.fetchone() is None
    if empty_live_front:
        insert_cols = ", ".join(
            (
                spec.target_kind_column,
                spec.target_id_column,
                spec.namespace_column,
                spec.branch_column,
                spec.from_column,
                spec.to_column,
                *spec.payload_columns,
            )
        )
        select_cols = ", ".join(f"s.{column}" for column in spec.payload_columns)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {spec.table} ({insert_cols})
                SELECT s.{spec.target_kind_column}, s.{spec.target_id_column},
                       s.{spec.namespace_column}, %s, %s, %s, {select_cols}
                  FROM {spec.snapshot_table} s
                 WHERE s.{spec.generation_column} = %s
                   AND s.{spec.namespace_column} = %s
                ON CONFLICT ({", ".join(spec.primary_key_columns)}) DO NOTHING
                """,
                (branch_id, new_ord, spec.open_ordinal, gen, namespace),
            )
            rows_written = int(cur.rowcount or 0)
        if grant_fn is not None:
            grant_fn(conn)
        res: dict[str, Any] = {
            "generation_id": gen,
            "branch_id": branch_id,
            "ordinal": new_ord,
            "counts": {},
            "rows_written": rows_written,
            "mode": "empty_initial_load",
        }
    else:
        res = _already_backfilled_explicit_interval_result(
            conn,
            spec=spec,
            generation_id=gen,
            branch_id=branch_id,
            ordinal=new_ord,
            namespace=namespace,
        )
        if res is None:
            res = sync_explicit_interval_rows_for_generation(
                conn,
                db,
                spec=spec,
                generation_id=generation_id,
                namespace=namespace,
                dirty_target_ids_by_kind=None,
                grant_fn=grant_fn,
            )
    with conn.cursor() as cur:
        counts: dict[str, dict[str, int]] = {}
        for target_kind in spec.target_kinds:
            cur.execute(
                f"""
                SELECT count(*)
                  FROM {spec.snapshot_table}
                 WHERE {spec.generation_column} = %s
                   AND {spec.namespace_column} = %s
                   AND {spec.target_kind_column} = %s
                """,
                (gen, namespace, target_kind),
            )
            snap_n = int(_first_column_value(cur.fetchone()))
            cur.execute(
                f"""
                SELECT count(*)
                  FROM {spec.table}
                 WHERE {spec.branch_column} = %s
                   AND {spec.namespace_column} = %s
                   AND {spec.target_kind_column} = %s
                   AND {spec.to_column} = %s
                """,
                (branch_id, namespace, target_kind, spec.open_ordinal),
            )
            open_n = int(_first_column_value(cur.fetchone()))
            if open_n != snap_n:
                raise SubstrateError(
                    "explicit interval backfill incomplete: open-interval count "
                    "!= snapshot row count -- the generation's snapshot is not "
                    "FULL (backfill requires a full snapshot; run before enabling "
                    "the interval read path)",
                    data={
                        "generation_id": gen,
                        "branch": branch_id,
                        "target_kind": target_kind,
                        "open_intervals": open_n,
                        "snapshot_rows": snap_n,
                    },
                )
            counts[str(target_kind)] = {"closed": 0, "opened": open_n}
    if empty_live_front:
        res["counts"] = counts
    return {**res, "backfilled": True}


def explicit_interval_stab_rows(
    conn: Any,
    db: Any,
    *,
    spec: ExplicitIntervalTable,
    generation_id: int | str,
    target_kind: str,
    namespace: str = "default",
) -> list[tuple[Any, ...]]:
    """Return interval rows visible at ``generation_id`` in ``spec.read_columns``."""
    from okg.substrate.graph_store_internal.generation_ordinal import (
        resolve_generation_ordinal_row,
    )

    spec.validate()
    branch_id, g_ord = resolve_generation_ordinal_row(db, str(generation_id))
    cols = ", ".join(spec.read_columns)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {cols}
              FROM {spec.table}
             WHERE {spec.branch_column} = %(br)s
               AND {spec.namespace_column} = %(ns)s
               AND {spec.target_kind_column} = %(tk)s
               AND {spec.from_column} <= %(g)s
               AND %(g)s < {spec.to_column}
            """,
            {"br": branch_id, "ns": namespace, "tk": target_kind, "g": g_ord},
        )
        rows = cur.fetchall()
        return [
            tuple(row[column] for column in spec.read_columns)
            if isinstance(row, Mapping)
            else row
            for row in rows
        ]


def _sentinel_where(spec: SentinelSpec) -> tuple[str, dict[str, Any]]:
    """A ``WHERE every-pk-col == SENTINEL_TOKEN`` clause + params, schema-agnostic."""
    pk = list(spec.pk_columns)
    clause = " AND ".join(f"{column} = :p{i}" for i, column in enumerate(pk))
    params = {f"p{i}": SENTINEL_TOKEN for i in range(len(pk))}
    return clause, params


def read_cadence_counter(checkout: Any, *, table: str, spec: SentinelSpec) -> int | None:
    """Generations since the last full recount, read from the sentinel's cadence
    column at the branch head. ``None`` when no sentinel exists yet (first publish)
    OR the column is NULL -- never normalized to 0 (the gate needs ``None`` distinct
    from 0). Reproduces ``ontology_rollup._generations_since_full_recount``.
    """
    clause, params = _sentinel_where(spec)
    rows = checkout.query(
        f"SELECT {spec.cadence_column} FROM {table} WHERE {clause} LIMIT 1", params
    )
    if not rows:
        return None
    value = rows[0][spec.cadence_column]
    return int(value) if value is not None else None


def has_prior_sentinel(checkout: Any, *, table: str, spec: SentinelSpec) -> bool:
    """True iff a prior sentinel is visible at the branch head (a prior generation
    maintained the derivation, so the running sum has a base). Absence => first
    publish => full recount. Reproduces ``ontology_rollup._has_prior_rollup``.
    """
    clause, params = _sentinel_where(spec)
    rows = checkout.query(f"SELECT 1 FROM {table} WHERE {clause} LIMIT 1", params)
    return bool(rows)


def read_maintained_rows(
    conn: Any,
    *,
    table: str,
    generation_id: int | str,
    select_columns: Sequence[str],
    sentinel_spec: SentinelSpec,
    row_mapper: Callable[[Mapping[str, Any]], T],
    where: str | None = None,
    params: Mapping[str, Any] | None = None,
) -> list[T]:
    """Read a maintained derivation's rows AS-OF a pinned generation, failing loud
    on absence (never an O(graph) scan).

    Reproduces ``read_ontology_rollup``'s three loud-failure reasons:
    ``checkpoint_absent`` (no checkpoint for the generation), ``table_unregistered``
    (the table query fails), and ``no_sentinel`` (the maintain hook never ran). A
    maintained-but-empty generation (sentinel present, zero data rows) returns ``[]``.

    ``where`` (with named ``params``) narrows the statement; it MUST still select
    the sentinel row, or the read refuses with ``no_sentinel``.

    The read writes nothing (issue #2202). It is one statement, run through
    ``ChronosGraphStore.query_generation_once``, which takes no generation read
    lease and instead re-checks the compaction tombstone after the statement, so
    a read-only session can serve it and a page load leaves no pin behind. A
    compacted generation still raises ``GenerationCompacted``: that is a
    retired generation, not a missing derivation, and it propagates as such.
    """
    from okg.substrate.errors import GenerationCompacted
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.read_branch import (
        GenerationCheckpointAbsent,
    )

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    columns = ", ".join(select_columns)
    sql = f"SELECT {columns} FROM {table}"
    if where:
        sql = f"{sql} WHERE {where}"
    try:
        raw = store.query_generation_once(
            generation_id, sql, dict(params) if params is not None else None
        )
    except GenerationCompacted:
        raise
    except GenerationCheckpointAbsent as exc:
        raise MaintainedDerivationUnavailable(
            generation_id, "checkpoint_absent", str(exc)
        ) from exc
    except Exception as exc:  # noqa: BLE001 -- unregistered table => loud failure
        raise MaintainedDerivationUnavailable(
            generation_id, "table_unregistered", str(exc)
        ) from exc

    return partition_sentinel_rows(
        raw,
        sentinel_spec=sentinel_spec,
        row_mapper=row_mapper,
        generation_id=generation_id,
    )


class MaintainContext(NamedTuple):
    """The uniform context the driver passes to a consumer's maintain callbacks.

    Both ``full_recount`` and ``incremental`` take this; each reads what it needs
    (the full path uses only ``checkout``; the incremental path also uses ``store``
    for the prior-generation checkout, ``prior_generation_checkpoint``, and the
    opaque ``change_inputs``).
    """

    checkout: Any
    store: Any
    branch: str
    generation_id: int
    prior_generation_checkpoint: str | None
    change_inputs: Any


@dataclass(frozen=True)
class DerivationSpec:
    """One maintained derivation, declared once. Consumer-specific data + callbacks;
    the kit knows nothing of its meaning.
    """

    derivation_id: str
    sentinel: SentinelSpec
    oracle: Oracle
    force_full_predicates: Sequence[ForcePredicate]
    full_recount: Callable[[MaintainContext], MaintainResult]
    incremental: Callable[[MaintainContext], MaintainResult]
    self_heal_every: int


def maintain(
    conn: Any,
    *,
    spec: DerivationSpec,
    storage: StorageStrategy,
    branch: str,
    generation_id: int,
    prior_generation_checkpoint: str | None,
    inputs: ForceInputs,
    graph_store: Any | None = None,
) -> dict[str, Any]:
    """Maintain ``spec``'s derivation at ``generation_id`` for this publish cycle.

    Orchestration (reproducing ``maintain_ontology_rollup``): ensure the table
    registered -> checkout the branch head -> read whether a prior sentinel exists
    and the cadence counter -> decide full vs incremental (the 5-reason precedence)
    -> dispatch to the consumer's callback (which READS + computes the delta and
    returns a ``MaintainResult``) -> write the upserts/deletes AND the sentinel in
    ONE transaction. The callback owns its cadence-counter value (full -> 0,
    incremental -> prior + 1) and ``dirty_bucket_count``; the kit imposes no uniform
    sentinel-write rule. Returns a logging summary byte-identical to the old one
    (``reason`` present only on a full recount).
    """
    from okg.substrate.graph_store import ChronosGraphStore

    store = graph_store or ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        autocommit=bool(getattr(conn, "autocommit", True)),
    )
    storage.ensure_registered(store)
    checkout = store.checkout(branch)

    has_prior = has_prior_sentinel(checkout, table=storage.table, spec=spec.sentinel)
    elapsed = (
        read_cadence_counter(checkout, table=storage.table, spec=spec.sentinel)
        if has_prior
        else None
    )
    mode = decide_maintain_mode(
        has_prior=has_prior,
        inputs=inputs,
        force_full_predicates=spec.force_full_predicates,
        elapsed=elapsed,
        self_heal_every=spec.self_heal_every,
    )

    ctx = MaintainContext(
        checkout=checkout,
        store=store,
        branch=branch,
        generation_id=generation_id,
        prior_generation_checkpoint=prior_generation_checkpoint,
        change_inputs=inputs.change_inputs,
    )
    result = spec.full_recount(ctx) if mode.full else spec.incremental(ctx)

    session = store.checkout(branch)
    # A caller-owned publisher transaction must remain usable when a
    # NON_GATING derivation fails and the registry deliberately swallows that
    # fault.  The ordinary BranchSession transaction owns its database
    # transaction and rolls it back on failure.  The explicit external mode
    # instead uses a true nested savepoint and never ends the outer publish.
    caller_owned_transaction = not bool(store.context.autocommit)
    with session.transaction(external_transaction=caller_owned_transaction):
        if result.deletes:
            session.delete_keys(storage.table, result.deletes)
        if result.upserts:
            session.upsert_rows(storage.table, result.upserts)
        session.upsert_rows(
            storage.table, [sentinel_row_dict(spec.sentinel, result.cadence_counter)]
        )

    summary: dict[str, Any] = {"mode": result.mode}
    if mode.full:
        summary["reason"] = mode.reason
        # Unify the recorded cause: a kit-dispatched full recompute carries its
        # cause here too, so summary["full_reason"] is the single queryable
        # answer to "why did this run recompute in full" across BOTH the
        # dispatch-decided and callback-decided full paths.
        summary["full_reason"] = mode.reason
    elif result.full_reason is not None:
        # A callback fell back to a full recompute under incremental dispatch
        # (mode == "incremental"); record its cause, never leave it silent.
        summary["full_reason"] = result.full_reason
    summary["rows_written"] = len(result.upserts)
    summary["rows_deleted"] = len(result.deletes)
    summary["dirty_bucket_count"] = result.dirty_bucket_count
    return summary
