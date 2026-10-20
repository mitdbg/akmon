"""Substrate contract types.

Authoritative definitions. Cross-milestone contracts in
docs/reference/*.md reference these names; any change here that
breaks a contract signature is a design-level change.

See docs/develop/architecture/pipeline.md#publication-result-types for the narrative version of the
same shapes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ---------- Phase A ----------

class SourceProjectionStatus(str, Enum):
    """Per-source state at Phase A completion."""

    DONE = "done"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    NO_WORK = "no_work"


@dataclass(frozen=True)
class ProjectionDelta:
    """Counts of what the projector inserted and retracted in one
    publisher cycle. Computed in-memory by the projector layer as
    it does the work; surfaced to the publisher so it can fill the
    cycle's summary without re-querying ``nodes_live`` / ``edges_live``.

    Part of ``add-delta-proportional-substrate-work``: eliminates
    the four hot ``SELECT COUNT(*) FROM okg.{nodes,edges}_live WHERE
    valid_{from,to}_gen = %s`` queries that previously fired at
    cycle end (each O(N) seq scan on an unindexed predicate).

    All fields default to 0 so existing test fixtures that construct
    ``PhaseAResult`` directly don't have to know about deltas.
    """

    nodes_added:     int = 0
    nodes_retracted: int = 0
    edges_added:     int = 0
    edges_retracted: int = 0

    def __add__(self, other: "ProjectionDelta") -> "ProjectionDelta":
        return ProjectionDelta(
            nodes_added     = self.nodes_added     + other.nodes_added,
            nodes_retracted = self.nodes_retracted + other.nodes_retracted,
            edges_added     = self.edges_added     + other.edges_added,
            edges_retracted = self.edges_retracted + other.edges_retracted,
        )


@dataclass(frozen=True)
class PhaseAResult:
    """Result of Publisher.await_phase_a(g_next, timeout).

    Invariant: `timed_out` implies NOT `converged`.
    """

    converged: bool
    per_source_status: dict[str, SourceProjectionStatus]
    projection_cursor_by_source: dict[str, int]
    timed_out: bool
    # `add-delta-proportional-substrate-work`: counts of rows
    # inserted/closed in `nodes_live` / `edges_live` during this
    # phase A. Aggregated across all worker batches plus the
    # cycle-level project_retractions / project_new_edges calls.
    # Defaults to an empty delta for backwards-compatible
    # construction by tests.
    delta: ProjectionDelta = field(default_factory=ProjectionDelta)

    def __post_init__(self) -> None:
        # Enforce the exactly-one rule the publisher depends on.
        if self.timed_out and self.converged:
            raise AssertionError(
                "PhaseAResult invariant violated: timed_out and converged "
                "cannot both be True"
            )


# ---------- Phase B ----------

@dataclass(frozen=True)
class GateInvariantResult:
    """Outcome of one invariant during Phase B."""

    name: str
    scope: str  # 'source' | 'cross'
    source: str | None  # set when scope == 'source'
    severity: str  # 'error' | 'warn'
    passed: bool
    violators: list[dict[str, Any]] = field(default_factory=list)
    violator_count: int = 0
    duration_ms: int = 0
    timed_out: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PhaseBResult:
    """Result of Publisher.run_gates(g_next).

    Note: some call sites refer to this as `GateResult`; this is
    the normalized name.
    """

    source_local: dict[str, list[GateInvariantResult]]
    cross_source: list[GateInvariantResult]
    excluded_sources: dict[str, str]  # source -> reason
    blocked: bool
    blocked_reason: dict[str, Any] | None
    total_duration_ms: int
    total_budget_exceeded: bool
    gate_cache_key: dict[str, Any] | None = None
    invariant_summary_override: dict[str, Any] | None = None
    consistency_gates: list[dict[str, Any]] = field(default_factory=list)
    substrate_integrity: list[GateInvariantResult] = field(
        default_factory=list,
    )
    # Issue #3624: invariants not evaluated because a source they require
    # is off in the deployment, each with the sources and the reason. A
    # skipped invariant neither passes nor fails; it is reported here and in
    # the generation's invariant_summary so the skip is never silent.
    skipped_invariants: list[dict[str, Any]] = field(default_factory=list)
    # Invariants that declare requires_sources but were evaluated anyway
    # because the deployment's source registry could not be read.
    unchecked_source_requirements: list[str] = field(default_factory=list)


# Backward-compat alias — some callers named this GateResult.
GateResult = PhaseBResult


# ---------- Generation state ----------

class GenerationStatus(str, Enum):
    PENDING = "pending"
    PUBLISHED = "published"
    BLOCKED = "blocked"


# ---------- Reserved generation handle ----------

@dataclass(frozen=True)
class ReservedGeneration:
    """Handed back from Publisher.reserve().

    Carries the reserved generation id plus the
    advisory-lock-release token the publisher uses to release on
    commit / abort.
    """

    generation_id: int
    deployment_name: str
    catalog_version_id: int
    correlation_id: str
    # Connection holding the advisory lock; callers must not close
    # it out of band. The publisher manages its lifecycle.
    _conn: Any = field(repr=False)
    trace_id: str | None = None


# ---------- Deployment status (wire shape) ----------
#
# DeploymentStatus is a diagnostic JSON document produced by `okg status`.
# `okg metrics` has several mode-specific JSON shapes rather than one shared
# MetricsSnapshot. Neither is a Python dataclass used in flight.
