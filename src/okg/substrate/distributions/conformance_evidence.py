"""Build external-conformance evidence blocks from measured values only.

The wire contract in :mod:`okg.substrate.contracts.deployment_product.conformance`
defines the typed proof blocks a receipt carries
(:class:`ConformancePublishEvidence`, :class:`ConformanceReapplyEvidence`,
:class:`ConformanceReadinessEvidence`, :class:`ConformanceMaterializationEvidence`
and :class:`ConformanceSignalEvidence`).  Until this module, nothing
constructed one from a run: the classes existed and every fixture was authored
by hand.

Each producer here takes the run's own measurements and refuses rather than
supply a value it was not given:

* a measurement that is ``None`` refuses ``measurement_missing``;
* a claimed count that does not equal the read-back count refuses
  ``count_disagrees_with_readback``;
* an arm outcome the measurements contradict (a publish that is not
  ``published``, a "no-op" reapply that moved a row, a readiness block that
  claims ready while its doctor is not clean, a materialized asset whose bytes
  differ from the verified package's) refuses with its own reason.

There is no default, no zero fill and no "best effort" value anywhere in this
module.  Refusing is the only alternative to a measured answer, so a receipt
built through these producers cannot claim more than the run observed.

Lock and plan drift is not detected here.  The install plan
(:mod:`okg.substrate.distributions.install_plan`) already refuses
``install_lock_drift``, and lifecycle receipts already record a drift event;
:func:`build_reapply_evidence` binds the exact lock and plan digests those
authorities produced, which is the evidence the wire contract defines for the
``idempotent_reapply`` arm.  A second drift detector would be a second
authority for the same fact.

:class:`ConformanceRecoveryEvidence` has no producer here on purpose.  An
executed update/rollback recovery needs a real host that can interrupt and
restore a sealed release; an inert plan is refused rather than recorded.
Until a host runs it, the ``update_rollback_recovery`` arm is reported blocked
with the environment profile, never passed.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from okg.substrate.contracts.deployment_product.conformance import (
    ConformanceMaterializationEvidence,
    ConformancePublishEvidence,
    ConformanceReadinessEvidence,
    ConformanceReapplyEvidence,
    ConformanceSignalEvidence,
    ConformanceSourceDelta,
)
from okg.substrate.errors import SubstrateError


__all__ = [
    "COUNT_SIGNAL_KINDS",
    "DOCTOR_RESULTS",
    "ConformanceEvidenceError",
    "MeasuredCount",
    "build_count_signal_evidence",
    "build_materialization_evidence",
    "build_publish_evidence",
    "build_readiness_evidence",
    "build_reapply_evidence",
]


#: The doctor outcomes the wire contract accepts.
DOCTOR_RESULTS: tuple[str, ...] = ("clean", "failed", "unavailable")
#: Signal kinds whose evidence is a count rather than a digest.
COUNT_SIGNAL_KINDS: frozenset[str] = frozenset(
    {"source_count", "node_count", "edge_count", "subtype_count"}
)


class ConformanceEvidenceError(SubstrateError):
    """A conformance evidence block could not be built from measurements.

    ``data`` carries ``block`` (the evidence block being built), ``path`` (the
    measurement at fault) and ``reason``.
    """

    code = "conformance_evidence_refused"

    def to_dict(self) -> dict[str, str | None]:
        """The shape ``ConformanceContractError.to_dict`` uses."""

        return {
            "code": self.code,
            "path": str(self.data.get("path")),
            "expected": str(self.data.get("block")),
            "observed": str(self.data.get("reason")),
        }


def _refuse(
    block: str,
    path: str,
    reason: str,
    fix_hint: str,
) -> ConformanceEvidenceError:
    return ConformanceEvidenceError(
        f"{block} evidence at {path} cannot be built from the supplied "
        f"measurements: {reason}",
        data={"block": block, "path": path, "reason": reason},
        fix_hint=fix_hint,
    )


def _measured(block: str, path: str, value: Any) -> Any:
    """Return ``value``, refusing an absent measurement instead of defaulting."""

    if value is None:
        raise _refuse(
            block,
            path,
            "measurement_missing",
            "measure the value and pass it; this producer has no default and "
            "will not derive one from the values it was given",
        )
    return value


@dataclass(frozen=True, slots=True)
class MeasuredCount:
    """One live count read back before and after a measured step.

    ``before`` and ``after`` are what the run actually read out of the graph.
    Either being ``None`` means the run did not measure it, which every
    producer treats as a refusal rather than a zero.
    """

    target: str
    before: int | None
    after: int | None

    def delta(self, block: str) -> int:
        before = _measured(block, f"{self.target}.before", self.before)
        after = _measured(block, f"{self.target}.after", self.after)
        if isinstance(before, bool) or isinstance(after, bool):
            raise _refuse(
                block,
                self.target,
                "measurement_not_an_integer",
                "read back a row count, not a flag",
            )
        if not isinstance(before, int) or not isinstance(after, int):
            raise _refuse(
                block,
                self.target,
                "measurement_not_an_integer",
                "read back a row count, not a formatted or derived value",
            )
        if before < 0 or after < 0:
            raise _refuse(
                block,
                self.target,
                "measurement_negative",
                "a live row count cannot be negative",
            )
        return after - before


def _total_delta(block: str, counts: Mapping[str, MeasuredCount]) -> int:
    if not counts:
        raise _refuse(
            block,
            "counts",
            "measurement_missing",
            "read back at least one live count for the arm being proved",
        )
    return sum(item.delta(block) for item in counts.values())


def _checked_claim(
    block: str,
    path: str,
    *,
    claimed: int | None,
    measured: int,
) -> int:
    claimed = _measured(block, path, claimed)
    if isinstance(claimed, bool) or not isinstance(claimed, int):
        raise _refuse(
            block,
            path,
            "claim_not_an_integer",
            "claim an exact count, not a formatted or derived value",
        )
    if claimed != measured:
        raise _refuse(
            block,
            path,
            "count_disagrees_with_readback",
            f"the run claimed {claimed} but the graph read back {measured}; "
            "record the read-back number or report the arm's real outcome, "
            "never the claim",
        )
    return measured


def _source_deltas(
    block: str,
    *,
    claimed: Mapping[str, int] | None,
    measured: Mapping[str, MeasuredCount] | None,
) -> tuple[ConformanceSourceDelta, ...]:
    claimed = _measured(block, "source_deltas", claimed)
    measured = _measured(block, "source_counts", measured)
    if set(claimed) != set(measured):
        raise _refuse(
            block,
            "source_deltas",
            "source_coverage_mismatch",
            "claim exactly the sources whose emitted rows were read back",
        )
    return tuple(
        ConformanceSourceDelta(
            source_id=source_id,
            delta=_checked_claim(
                block,
                f"source_deltas.{source_id}",
                claimed=claimed[source_id],
                measured=measured[source_id].delta(block),
            ),
        )
        for source_id in sorted(claimed)
    )


def build_publish_evidence(
    *,
    generation_id: str | None,
    generation_status: str | None,
    definition_digest: str | None,
    lock_digest: str | None,
    composition_digest: str | None,
    plan_digest: str | None,
    sealed_release_digest: str | None,
    applied_instance_id: str | None,
    node_counts: Mapping[str, MeasuredCount] | None,
    edge_counts: Mapping[str, MeasuredCount] | None,
    claimed_nodes_live_delta: int | None,
    claimed_edges_live_delta: int | None,
    claimed_source_deltas: Mapping[str, int] | None,
    source_counts: Mapping[str, MeasuredCount] | None,
) -> ConformancePublishEvidence:
    """Prove one published generation from what the run read back.

    ``node_counts`` and ``edge_counts`` are live counts read out of the graph
    before and after the publish, keyed by the subtype or edge type they count;
    the claimed deltas must equal their measured totals.  Every digest and the
    applied instance are the authority the publish actually ran under,
    including the sealed release the deployment-product lifecycle produced: a
    run with no sealed release has not proved this arm, and is refused here
    rather than recorded with a placeholder.
    """

    block = "publish"
    status = _measured(block, "generation_status", generation_status)
    if status != "published":
        raise _refuse(
            block,
            "generation_status",
            "generation_not_published",
            "only a published generation proves the first_publish arm; report "
            "the arm's real outcome instead",
        )
    nodes_live_delta = _checked_claim(
        block,
        "nodes_live_delta",
        claimed=claimed_nodes_live_delta,
        measured=_total_delta(block, _measured(block, "node_counts", node_counts)),
    )
    edges_live_delta = _checked_claim(
        block,
        "edges_live_delta",
        claimed=claimed_edges_live_delta,
        measured=_total_delta(block, _measured(block, "edge_counts", edge_counts)),
    )
    if nodes_live_delta < 1 or edges_live_delta < 1:
        raise _refuse(
            block,
            "nodes_live_delta",
            "publish_added_no_rows",
            "a first publish that adds no live node or edge has not proved "
            "the arm; report it as failed or blocked",
        )
    deltas = _source_deltas(
        block, claimed=claimed_source_deltas, measured=source_counts
    )
    if sum(item.delta for item in deltas) != nodes_live_delta:
        raise _refuse(
            block,
            "source_deltas",
            "source_deltas_disagree_with_nodes",
            "the per-source emitted rows must account for the live node "
            "delta; an unexplained difference is not evidence",
        )
    return ConformancePublishEvidence(
        generation_id=_measured(block, "generation_id", generation_id),
        generation_status="published",
        definition_digest=_measured(block, "definition_digest", definition_digest),
        lock_digest=_measured(block, "lock_digest", lock_digest),
        composition_digest=_measured(block, "composition_digest", composition_digest),
        plan_digest=_measured(block, "plan_digest", plan_digest),
        sealed_release_digest=_measured(
            block, "sealed_release_digest", sealed_release_digest
        ),
        applied_instance_id=_measured(
            block, "applied_instance_id", applied_instance_id
        ),
        nodes_live_delta=nodes_live_delta,
        edges_live_delta=edges_live_delta,
        source_deltas=deltas,
    )


def build_reapply_evidence(
    *,
    generation_id_before: str | None,
    generation_id_after: str | None,
    lock_digest: str | None,
    plan_digest: str | None,
    node_counts: Mapping[str, MeasuredCount] | None,
    edge_counts: Mapping[str, MeasuredCount] | None,
    generations_created: MeasuredCount | None,
    active_generation_pointer_changes: int | None,
    claimed_source_deltas: Mapping[str, int] | None,
    source_counts: Mapping[str, MeasuredCount] | None,
) -> ConformanceReapplyEvidence:
    """Prove a reapply changed nothing, from the counts read either side of it.

    ``lock_digest`` and ``plan_digest`` are the digests the install plan
    resolved; the wire contract requires them to equal the receipt authority's,
    which is how a drifted lock reaches the receipt.  Drift itself is refused
    earlier, by ``install_lock_drift``; this producer records the bound
    identity, it does not re-detect drift.
    """

    block = "reapply"
    before = _measured(block, "generation_id_before", generation_id_before)
    after = _measured(block, "generation_id_after", generation_id_after)
    if before != after:
        raise _refuse(
            block,
            "generation_id_after",
            "reapply_changed_the_active_generation",
            "a reapply that moved the active generation is not idempotent; "
            "report the arm as failed",
        )
    for path, counts in (
        ("node_counts", node_counts),
        ("edge_counts", edge_counts),
    ):
        measured = _total_delta(block, _measured(block, path, counts))
        if measured != 0:
            raise _refuse(
                block,
                path,
                "reapply_mutated_the_graph",
                f"the graph moved by {measured} rows across the reapply; an "
                "idempotent reapply proves zero",
            )
    created = _measured(block, "generations_created", generations_created)
    if created.delta(block) != 0:
        raise _refuse(
            block,
            "generations_created",
            "reapply_created_a_generation",
            "an idempotent reapply creates no generation",
        )
    pointer_changes = _measured(
        block,
        "active_generation_pointer_changes",
        active_generation_pointer_changes,
    )
    if pointer_changes != 0:
        raise _refuse(
            block,
            "active_generation_pointer_changes",
            "reapply_moved_the_active_pointer",
            "an idempotent reapply leaves the active generation pointer alone",
        )
    return ConformanceReapplyEvidence(
        generation_id_before=before,
        generation_id_after=after,
        lock_digest=_measured(block, "lock_digest", lock_digest),
        plan_digest=_measured(block, "plan_digest", plan_digest),
        nodes_live_delta=0,
        edges_live_delta=0,
        generations_created_delta=0,
        active_generation_pointer_changes=0,
        source_deltas=_source_deltas(
            block, claimed=claimed_source_deltas, measured=source_counts
        ),
    )


def build_readiness_evidence(
    *,
    ready: bool | None,
    doctor: str | None,
    queue_depth: int | None,
    dlq_depth: int | None,
    mcp_graph_read: bool | None,
) -> ConformanceReadinessEvidence:
    """Record the readiness, doctor, queue and read proof the run measured.

    ``queue_depth`` and ``dlq_depth`` are depths read from the runtime queue
    authority; an unread depth is ``None`` and refuses, because a zero that was
    never read is indistinguishable in the receipt from an empty queue.  A
    caller whose queue authority reports itself unavailable has not read a
    depth and passes ``None``, never the zeros such a report carries.
    """

    block = "readiness"
    ready = _measured(block, "ready", ready)
    doctor = _measured(block, "doctor", doctor)
    graph_read = _measured(block, "mcp_graph_read", mcp_graph_read)
    for path, depth in (("queue_depth", queue_depth), ("dlq_depth", dlq_depth)):
        if isinstance(depth, bool):
            raise _refuse(
                block,
                path,
                "measurement_not_an_integer",
                "read a queue depth, not a flag",
            )
    if doctor not in DOCTOR_RESULTS:
        raise _refuse(
            block,
            "doctor",
            "doctor_result_unknown",
            f"record one of {', '.join(DOCTOR_RESULTS)}",
        )
    if ready and doctor != "clean":
        raise _refuse(
            block,
            "ready",
            "ready_contradicts_doctor",
            "a deployment whose doctor is not clean is not ready; record the "
            "doctor result the run observed",
        )
    return ConformanceReadinessEvidence(
        ready=bool(ready),
        doctor=doctor,
        queue_depth=_measured(block, "queue_depth", queue_depth),
        dlq_depth=_measured(block, "dlq_depth", dlq_depth),
        mcp_graph_read=bool(graph_read),
    )


def build_materialization_evidence(
    *,
    materialization_digest: str | None,
    declared_asset_digests: Mapping[str, str] | None,
    materialized_asset_digests: Mapping[str, str] | None,
    authoring_tree_present: bool | None,
) -> ConformanceMaterializationEvidence:
    """Prove every selected asset materialized to its verified bytes.

    ``declared_asset_digests`` is read from the verified package manifest and
    ``materialized_asset_digests`` from the files the run materialized, both
    keyed by owner-qualified asset id.  A missing asset refuses
    ``asset_not_materialized`` (the wire contract reports the same defect as
    ``conformance_distribution_declared_asset_missing``); a mutated asset
    refuses ``asset_digest_mismatch``.
    """

    block = "materialization"
    declared = _measured(block, "declared_asset_digests", declared_asset_digests)
    observed = _measured(
        block, "materialized_asset_digests", materialized_asset_digests
    )
    present = _measured(block, "authoring_tree_present", authoring_tree_present)
    if not declared:
        raise _refuse(
            block,
            "declared_asset_digests",
            "measurement_missing",
            "read the selected assets out of the verified package manifest",
        )
    if present:
        raise _refuse(
            block,
            "authoring_tree_present",
            "checkout_not_independent",
            "materialize with the distribution's authoring tree deleted; a "
            "materialization that could still read it proves nothing about "
            "checkout independence",
        )
    for asset_ref in sorted(declared):
        if asset_ref not in observed:
            raise _refuse(
                block,
                f"materialized_asset_digests.{asset_ref}",
                "asset_not_materialized",
                "a manifest-declared asset absent from the materialization is "
                "a distribution failure "
                "(conformance_distribution_declared_asset_missing), not a "
                "passing arm",
            )
        if observed[asset_ref] != declared[asset_ref]:
            raise _refuse(
                block,
                f"materialized_asset_digests.{asset_ref}",
                "asset_digest_mismatch",
                "the materialized bytes differ from the verified package's; "
                "replace the artifact rather than record the difference",
            )
    unexpected = sorted(set(observed) - set(declared))
    if unexpected:
        raise _refuse(
            block,
            f"materialized_asset_digests.{unexpected[0]}",
            "asset_not_declared",
            "the materialization produced an asset the manifest does not "
            "declare",
        )
    return ConformanceMaterializationEvidence(
        materialization_digest=_measured(
            block, "materialization_digest", materialization_digest
        ),
        checkout_independent=True,
        selected_assets_verified=True,
    )


def build_count_signal_evidence(
    *,
    signal_id: str,
    kind: Literal["source_count", "node_count", "edge_count", "subtype_count"],
    target: str,
    claimed_count: int | None,
    measured_count: int | None,
) -> ConformanceSignalEvidence:
    """Bind one requested count expectation to the count the run read back."""

    block = "signal"
    if kind not in COUNT_SIGNAL_KINDS:
        raise _refuse(
            block,
            signal_id,
            "signal_kind_not_counted",
            f"use one of {', '.join(sorted(COUNT_SIGNAL_KINDS))}; a view, "
            "invariant, interface or readiness signal carries a result "
            "digest, not a count",
        )
    observed = _checked_claim(
        block,
        f"{signal_id}.observed_count",
        claimed=claimed_count,
        measured=_measured(block, f"{signal_id}.measured_count", measured_count),
    )
    return ConformanceSignalEvidence(
        signal_id=signal_id,
        kind=kind,
        target=target,
        observed_count=observed,
        positive=observed > 0,
    )
