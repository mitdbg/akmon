"""Phase 2 §2.1 import-surface stubs.

The SDK's public API is enumerated up-front so deployment authors can
write workflow modules against a stable surface from day one. The
*implementations* of mutation helpers, read helpers, external
integration, and stream wrappers land in later phases (§3, §4, §5);
this module supplies clear-failure stubs that name the phase they
land in.

The stubs are real callables (not module-level ``__getattr__``) so
that ``from okg.deployment import X`` succeeds and the call site is
the one that surfaces the not-yet-implemented error. This keeps the
deployment-author IDE / type-checker happy.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


__all__ = [
    "DerivedEdgeSpec",
    "declare_narrowings",
    "declare_sdk_version",
]


def _not_yet(name: str, phase: str) -> Any:
    raise NotImplementedError(
        f"okg.deployment.{name} is not yet implemented; "
        f"lands with {phase} of the SDK build. Track progress in "
        "openspec/changes/build-deployment-sdk/tasks.md."
    )


# Phase 2 §3 mutation helpers (emit_node_fact / emit_edge_fact /
# update_node_attrs / request_escalation / set_progress / cost_budget)
# moved to dedicated implementation modules:
#   _internal.mutations    — emit_*, update_node_attrs, set_progress
#   _internal.cost_budget  — cost_budget context manager
#   _internal.escalation   — request_escalation SDK wrapper
# This module retains only the stubs for §4 / §5 surface that has
# not yet shipped.


# Phase 2 §4 read helpers (read_node / query_at_generation /
# list_published_generations / publisher_generation / pinned_generation /
# latest_published_generation / worker_write_generation) moved to
# _internal.reads.


# Phase 2 §5 external integration helpers (llm_call / wait_for_signal /
# correlate_workflow / find_workflow_by_correlation / request_republish)
# moved to _internal.external. Stream wrappers moved to _internal.streams.


@dataclass(frozen=True)
class DerivedEdgeSpec:
    """Stream-passed derived edge.

    Field shapes are stable as of Phase 2 §2.1; the producer +
    consumer wrappers land in §5.2. Deployment authors can build
    DerivedEdgeSpec instances today; consumption requires §5.2.
    """

    src: str
    dst: str
    edge_type: str
    attrs: dict = field(default_factory=dict)
    dedupe_key: str = ""
    confidence: float = 1.0
    provenance: str = "derived_deterministic"


# Stream-wrapper functions moved to _internal.streams; DerivedEdgeSpec
# is retained here as the import source for backward compatibility.


# ---------------------------------------------------------------------------
# Module-level declarations (Phase 2 §2.3)
# ---------------------------------------------------------------------------


def declare_narrowings(*triples: tuple[str, str, str]) -> None:
    """Validate that each ``(parent_subtype, edge_type, child_subtype)``
    triple is in the active catalog narrowings.

    Lands in Phase 2 §2.3.1. Until then, a no-op so module-level
    ``declare_narrowings(...)`` in deployment workflow code is
    forward-compatible with the eventual strict validation.
    """
    # Per Phase 2 §2.3.1 we want a *boot-time* validation, but
    # implementing that requires DB access at import time (or a
    # deferred registration mechanism). For Phase 2 §2.1 we accept
    # the no-op so SDK skeleton imports work; the strict check lands
    # with §2.3 alongside the catalog narrowing wildcard (§1.1.4).
    _ = triples


def declare_sdk_version(version: str) -> None:
    """Module-scope SDK version stamp.

    Lands in Phase 2 §2.3.2 (manifest cross-version compatibility
    check). For now we accept the call and record nothing — the
    substrate's manifest loader already enforces
    ``runtime.sdk_version`` from ``deployment.yaml``.
    """
    _ = version
