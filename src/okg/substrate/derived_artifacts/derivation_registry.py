"""The maintained-derivation registry (#444 slice 3).

Every maintained derivation is a ROW here: the slice-1 kit's ``DerivationSpec``
(sentinel + oracle + forcing predicates + maintain callbacks + cadence) PLUS the
registry-only axes the scheduler and the uniform-governance boundary need --
declared dependencies (the reverse-index keys ``change_frontier`` emits), the
output materialization(s), the determinism class, the governance profile, and the
optional finding-set archetype. One driver iterates the rows instead of the
per-flavor publisher hooks (the two rollups today, then relations + views).

This module is ``derivation_registry`` and MUST NOT be named ``registry`` or live
under ``views/`` -- ``okg.substrate.views.registry`` is the LLM narrative-view
registry (``ViewRegistryEntry``) and the PACT flags that collision twice. The row
EXTENDS ``DerivationSpec`` and the registry ITERATES ``maintain()`` -- it re-mints
neither.
"""
from __future__ import annotations

import enum
import logging
from collections.abc import Callable, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from okg.substrate.derived_artifacts.dependency_index import (
    INDEXED_DEPENDENCY_KINDS,
)
from okg.substrate.derived_artifacts.maintained_derivation import (
    CorrectnessOracle,
    DerivationSpec,
    ForceInputs,
    ProjectionParityOracle,
    SentinelSpec,
    StorageStrategy,
    maintain,
)

logger = logging.getLogger(__name__)


def _default_make_inputs(dirty_scope: Any, force_full: bool) -> ForceInputs:
    """A row with no consumer-specific change descriptor: the kit's force
    predicates + the cross-cutting ``force_full`` decide the mode. ``scope_available``
    is ``dirty_scope is not None`` (a None scope ⇒ loud full recount). Real rows
    (the rollups) override this with their own scope→change_inputs translation."""
    return ForceInputs(
        change_inputs=None,
        force_full=force_full,
        scope_available=dirty_scope is not None,
    )


def _dirty_scope_unavailable_for_row(row: "DerivationRow", dirty_scope: Any) -> bool:
    if dirty_scope is None:
        return True
    if not row.declared_deps.kinds:
        return True
    if "graph_edge" not in row.declared_deps.kinds:
        return False
    try:
        return len(dirty_scope) >= 3 and dirty_scope[2] is False
    except (TypeError, KeyError, IndexError):
        return False


@dataclass(frozen=True)
class PublishCycleDeltas:
    """Typed publish-cycle facts routed from Publisher.cycle to a registry row."""

    branch: str
    generation_id: int
    prior_generation_checkpoint: str | None
    dirty_scope: Any
    force_full: bool
    changed_node_ids: tuple[str, ...] | None
    changed_edge_ids: tuple[int, ...] | None


class DerivationRegistrationError(Exception):
    """A row violated the registry's admission contract (wrong-class oracle,
    override on a deterministic row, an unknown dependency kind, no materialization).
    Registration fails LOUD -- a mis-declared derivation never silently admits.
    """


class DeterminismClass(enum.Enum):
    """The axis that cuts across enricher / view / rollup flavors."""

    # A pure function of a sealed generation: recompute on staleness, NO override.
    DETERMINISTIC = "deterministic"
    # Contestably wrong: carries a per-output override path (supersession; lens/putback).
    JUDGMENT = "judgment"


class ExecutionPhase(enum.Enum):
    """When in the publish cycle the row is maintained -- a contract the registry
    must keep distinct (the rollups run pinned to the checkpoint; views post-publish).
    """

    PRE_CHECKPOINT = "pre_checkpoint"
    POST_PUBLISH = "post_publish"


class FailureMode(enum.Enum):
    """How a row's maintenance failure is handled. The two rollups fail SOFT (a
    broken read-acceleration artifact must never abort an otherwise-valid publish);
    relation/view scheduler tasks fail HARD. The registry preserves the per-row
    asymmetry -- folding rollups under a gating loop would let a broken rollup block
    real publishes.
    """

    NON_GATING = "non_gating"
    GATING = "gating"


@dataclass(frozen=True)
class DeclaredDeps:
    """The reverse-index keys this derivation is invalidated by.

    A subset of ``INDEXED_DEPENDENCY_KINDS`` -- the SAME kinds ``change_frontier``
    emits and the reverse-dependency index joins on (source records, graph node/edge
    ids, component hashes, AND decision/proposal/evidence admission sources). This is
    the scheduler-plannable dependency channel, distinct from the kit's opaque
    ``ForceInputs.change_inputs`` blob.
    """

    kinds: frozenset[str]


@dataclass(frozen=True)
class Materialization:
    """One output target of a row. A row carries a LIST kept in MUTUAL PARITY.

    NOTE (kit limit): ``maintain()`` writes exactly one table per call, so the
    registry ADMITS a list in the schema but the driver rejects ``len > 1`` LOUDLY --
    multi-table maintenance (iterating materializations in one txn) is a later slice.
    Declaring the list here is the forward-compatible surface, NOT a silent
    first-only drop.
    """

    kind: str  # "side_table" | "graph_nodes" | "graph_edges" | "rendered_artifact"
    storage: StorageStrategy
    # Per-table: the kit sentinel reuses a DATA column (ontology: edge_count; count:
    # item_count), so each materialization carries its own sentinel or absence
    # detection breaks for that table.
    sentinel: SentinelSpec


@dataclass(frozen=True)
class OverridePath:
    """The per-output override path of a JUDGMENT row (supersession for enricher
    facts; LENS / PUTBACK for LLM views). ``None`` for a DETERMINISTIC row -- the
    registry enforces override-iff-judgment. Wires existing override functions
    (``prepare_lens_putback`` / ``commit_lens_overlay`` / promotion) through the row.
    """

    prepare: Callable[..., object]
    commit: Callable[..., object]
    promote: Callable[..., object] | None = None


@dataclass(frozen=True)
class GovernanceProfile:
    """The registry-only governance axes layered over the kit spec: the determinism
    CLASS and the per-output OVERRIDE path. The parity ORACLE itself lives on
    ``DerivationSpec.oracle`` (the kit owns it) and is not duplicated here -- the
    registry validates that the spec's oracle is the class appropriate to
    ``determinism`` (correctness for deterministic rows, projection-parity for
    judgments). A provenance/lineage envelope and a loud recompute fallback are
    UNIVERSAL (every row); the override path is present ONLY for judgments.
    """

    determinism: DeterminismClass
    override: OverridePath | None = None
    requires_lineage_envelope: bool = True


@dataclass(frozen=True)
class FindingSet:
    """The observed->resolved finding-set archetype. WRAPS the existing coverage +
    observation lifecycle in ``consistency/observations.py`` (``CoverageReport`` with
    ``universe_key`` + ``coverage_complete``) -- it does NOT re-mint a second coverage
    model. ``coverage_complete`` reading ``False`` from a real ``CoverageReport``
    forces a full pass (the changeset did not cover the universe).
    """

    universe_key: Sequence[str]
    coverage_complete: Callable[[object], bool]


@dataclass(frozen=True)
class DerivationRow:
    """One derivation, declared once: the kit spec + the registry-only axes."""

    spec: DerivationSpec
    declared_deps: DeclaredDeps
    materializations: Sequence[Materialization]
    governance: GovernanceProfile
    execution_phase: ExecutionPhase
    failure_mode: FailureMode = FailureMode.NON_GATING
    finding_set: FindingSet | None = None
    # The publisher timing-stage name for this row (the bench reads it back). May
    # differ from derivation_id (the ontology rollup's id is "ontology_graph_rollup"
    # but its timing key is "ontology_rollup"); None => use derivation_id.
    timing_key: str | None = None
    # The consumer-specific scope -> ForceInputs translation (the body of the old
    # per-flavor publisher stage method): reads this cycle's dirty scope, builds the
    # row's opaque change_inputs + scope_available, and folds in the cross-cutting
    # force_full. Defaults to "no change descriptor" for schema-only rows.
    make_inputs: Callable[[Any, bool], ForceInputs] = _default_make_inputs
    # Deployment-facing context hook. Existing substrate rows can keep using
    # ``make_inputs``; rows that need publish-cycle facts use this typed object.
    make_cycle_inputs: Callable[[PublishCycleDeltas], ForceInputs] | None = None
    # Scheduler-native rows (relations/views) keep their existing manifest publishers
    # instead of writing through the kit's side-table StorageStrategy. The scheduler
    # uses this as an observable adapter boundary; the kit driver ignores it.
    scheduler_kind: str | None = None

    @property
    def timing_stage(self) -> str:
        """The publisher timing-stage name (``timing_key`` or the derivation id)."""
        return self.timing_key or self.spec.derivation_id


def _validate_governance(row: DerivationRow) -> None:
    """Registration fails without the determinism-APPROPRIATE oracle + override.

    A DETERMINISTIC row MUST carry a ``CorrectnessOracle`` and NO override; a JUDGMENT
    row MUST carry a ``ProjectionParityOracle`` and an override path. This closes the
    gap the kit leaves open: the ``Oracle`` union exists but nothing today stops a
    judgment registering a byte-identical-recompute oracle (which would demand a
    re-run of a nondeterministic producer -- forbidden) or a deterministic row
    carrying an override.
    """
    g = row.governance
    oracle = row.spec.oracle
    rid = row.spec.derivation_id
    if g.determinism is DeterminismClass.DETERMINISTIC:
        if not isinstance(oracle, CorrectnessOracle):
            raise DerivationRegistrationError(
                f"{rid}: a DETERMINISTIC row requires a CorrectnessOracle, got "
                f"{type(oracle).__name__}"
            )
        if g.override is not None:
            raise DerivationRegistrationError(
                f"{rid}: a DETERMINISTIC row must not carry an override path"
            )
    else:  # JUDGMENT
        if not isinstance(oracle, ProjectionParityOracle):
            raise DerivationRegistrationError(
                f"{rid}: a JUDGMENT row requires a ProjectionParityOracle, got "
                f"{type(oracle).__name__}"
            )
        if g.override is None:
            raise DerivationRegistrationError(
                f"{rid}: a JUDGMENT row requires an override path"
            )


def _validate_materializations(row: DerivationRow) -> None:
    """Every row needs >=1 materialization; declared dep kinds must be reverse-indexed."""
    rid = row.spec.derivation_id
    if not row.materializations:
        raise DerivationRegistrationError(
            f"{rid}: at least one materialization is required"
        )
    unknown = set(row.declared_deps.kinds) - set(INDEXED_DEPENDENCY_KINDS)
    if unknown:
        raise DerivationRegistrationError(
            f"{rid}: declared dependency kinds not in the reverse index: "
            f"{sorted(unknown)} (allowed: {sorted(INDEXED_DEPENDENCY_KINDS)})"
        )


class DerivationRegistry:
    """The process-global registry the scheduler iterates.

    ``register`` validates the governance + materialization admission contract and
    fails loud on violation. ``rows`` iterates in a DETERMINISTIC order (sorted by
    ``derivation_id``) -- load-bearing: the publish ``graph_hash`` and the
    byte-identical commit-replay both depend on a stable maintenance order.
    """

    def __init__(self) -> None:
        self._rows: dict[str, DerivationRow] = {}

    def register(self, row: DerivationRow) -> None:
        _validate_governance(row)
        _validate_materializations(row)
        self._rows[row.spec.derivation_id] = row

    def rows(self) -> list[DerivationRow]:
        return [self._rows[k] for k in sorted(self._rows)]

    def get(self, derivation_id: str) -> DerivationRow | None:
        return self._rows.get(derivation_id)

    def __contains__(self, derivation_id: object) -> bool:
        return derivation_id in self._rows


# The process-global registry. Consumer adapters register their row at import
# (the ontology-rollup adapter calls REGISTRY.register(ontology_rollup_row()), etc.).
REGISTRY = DerivationRegistry()


def maintain_registered_derivations(
    conn: Any,
    registry: DerivationRegistry,
    *,
    branch: str,
    generation_id: int,
    prior_generation_checkpoint: str | None,
    dirty_scope: Any,
    changed_node_ids: Sequence[str] | None = None,
    changed_edge_ids: Sequence[int] | None = None,
    force_full: bool = False,
    phase: ExecutionPhase | None = None,
) -> dict[str, dict[str, Any] | None]:
    """Maintain every registered derivation row for this publish cycle, replacing the
    per-flavor publisher hooks with one iterated loop.

    For each row (deterministic order), build the row's ``ForceInputs`` from this
    cycle's dirty scope + the cross-cutting ``force_full`` (the blocked-gen force),
    then call the kit's ``maintain()`` with the row's spec + storage. Returns
    ``{derivation_id: summary}`` (the summary the publisher persists into checkpoint
    metadata for the consumers that need it; an explicit ``status=unavailable``
    summary for a row whose maintenance failed under a non-gating
    ``failure_mode``).

    Per-row contracts preserved from the hand-wired stages:
      * ``failure_mode``: NON_GATING logs and returns an explicit unavailable
        summary (a broken read-acceleration artifact never aborts an
        otherwise-valid publish; the read fails loud later); GATING re-raises.
      * ``phase``: when given, only rows whose ``execution_phase`` matches run (the
        rollups are pre-checkpoint; views post-publish — distinct contracts).
      * Multi-materialization maintenance is NOT yet supported (``maintain()`` writes
        one table); a row declaring ``len > 1`` raises LOUDLY rather than silently
        maintaining the first and dropping the rest.
    """
    summaries: dict[str, dict[str, Any] | None] = {}
    for row in registry.rows():
        if phase is not None and row.execution_phase is not phase:
            continue
        summaries[row.spec.derivation_id] = maintain_one_row(
            conn,
            row,
            branch=branch,
            generation_id=generation_id,
            prior_generation_checkpoint=prior_generation_checkpoint,
            dirty_scope=dirty_scope,
            changed_node_ids=changed_node_ids,
            changed_edge_ids=changed_edge_ids,
            force_full=force_full,
        )
    return summaries


def maintain_one_row(
    conn: Any,
    row: DerivationRow,
    *,
    branch: str,
    generation_id: int,
    prior_generation_checkpoint: str | None,
    dirty_scope: Any,
    changed_node_ids: Sequence[str] | None = None,
    changed_edge_ids: Sequence[int] | None = None,
    force_full: bool = False,
    graph_store: Any | None = None,
) -> dict[str, Any] | None:
    """Maintain ONE registry row via the kit ``maintain()``.

    The per-row entry the publisher's timed loop calls (so each row runs inside its
    own ``_publisher_timed_stage(row.timing_stage)`` while the enablement decision
    stays with the publisher, which has the deployment context). Builds the row's
    ``ForceInputs`` from this cycle's dirty scope + ``force_full``, calls
    ``maintain()`` with the row's spec + single materialization's storage, and honors
    the row's ``failure_mode`` (NON_GATING returns an explicit unavailable
    summary; GATING re-raises).
    Rejects ``len(materializations) != 1`` LOUDLY (the kit writes one table).
    """
    if len(row.materializations) != 1:
        raise DerivationRegistrationError(
            f"{row.spec.derivation_id}: the registry driver maintains exactly one "
            f"materialization per row (maintain() writes one table); this row "
            f"declares {len(row.materializations)} -- multi-table maintenance is a "
            f"later slice, not a silent first-only drop"
        )
    storage = row.materializations[0].storage
    cycle = PublishCycleDeltas(
        branch=branch,
        generation_id=generation_id,
        prior_generation_checkpoint=prior_generation_checkpoint,
        dirty_scope=dirty_scope,
        force_full=force_full,
        changed_node_ids=(
            tuple(str(node_id) for node_id in changed_node_ids)
            if changed_node_ids is not None else None
        ),
        changed_edge_ids=(
            tuple(int(edge_id) for edge_id in changed_edge_ids)
            if changed_edge_ids is not None else None
        ),
    )
    inputs = (
        row.make_cycle_inputs(cycle)
        if row.make_cycle_inputs is not None
        else row.make_inputs(dirty_scope, force_full)
    )
    if _dirty_scope_unavailable_for_row(row, dirty_scope) and inputs.scope_available:
        inputs = inputs._replace(scope_available=False)
    # Finding-set archetype: when this cycle's changeset does NOT cover the row's
    # universe (coverage_complete reads False off the change descriptor -- the same
    # CoverageReport.coverage_complete consistency/observations.py already computes),
    # force a full pass. An incomplete-coverage incremental update would miss
    # observed->resolved transitions outside the changeset; the full pass is loud +
    # correct, never a silent partial.
    if row.finding_set is not None and not row.finding_set.coverage_complete(
        inputs.change_inputs
    ):
        inputs = inputs._replace(force_full=True)
    try:
        maintain_kwargs = {
            "spec": row.spec,
            "storage": storage,
            "branch": branch,
            "generation_id": generation_id,
            "prior_generation_checkpoint": prior_generation_checkpoint,
            "inputs": inputs,
        }
        if graph_store is not None:
            maintain_kwargs["graph_store"] = graph_store
        maintenance_transaction = nullcontext()
        if (
            graph_store is not None
            and not bool(graph_store.context.autocommit)
        ):
            transaction = getattr(conn, "transaction", None)
            if not callable(transaction):
                raise RuntimeError(
                    "caller-owned maintained derivation requires savepoint support"
                )
            # Cover the entire row, including registration, reads, and consumer
            # callbacks. A NON_GATING fault is swallowed below only after this
            # savepoint has restored the caller's still-active publish transaction.
            maintenance_transaction = transaction()
        with maintenance_transaction:
            summary = maintain(conn, **maintain_kwargs)
    except Exception as exc:  # noqa: BLE001 -- per-row failure_mode decides gating
        if row.failure_mode is FailureMode.GATING:
            raise
        logger.exception(
            "maintained derivation %s failed; not gating the publish. The read "
            "fails loud only while NO sentinel has ever been written: the table "
            "carries forward, so after a first successful maintain this failure "
            "serves the prior generation's rows. The caller's checkpoint receipt "
            "(absent for this generation) is what forces the next cycle to rebuild",
            row.spec.derivation_id,
        )
        return {
            "status": "unavailable",
            "mode": "unavailable",
            "reason": "maintain_failed",
            "error": repr(exc),
            "dirty_bucket_count": 0,
            "rows_written": 0,
            "rows_deleted": 0,
        }
    logger.info(
        "maintained derivation %s at gen=%s mode=%s dirty_buckets=%s "
        "rows_written=%s rows_deleted=%s",
        row.spec.derivation_id,
        generation_id,
        summary.get("mode"),
        summary.get("dirty_bucket_count"),
        summary.get("rows_written"),
        summary.get("rows_deleted"),
    )
    return summary
