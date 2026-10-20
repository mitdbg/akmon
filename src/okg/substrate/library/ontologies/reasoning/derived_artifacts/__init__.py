"""Reasoning module derived-artifact definitions.

Layer 3B of `add-portable-memex-deployments`. Ships the
`context_packet` derived artifact: a markdown-formatted Engram-
shaped Research Context Packet rendered from the typed graph
state at a pinned generation. Section queries use the
`okg.subtype_ancestors` closure so refinements from any
composed module (engram_runs, an external design-knowledge module, ...) surface
without modifying the definition.
"""
from __future__ import annotations

from okg.substrate.library.ontologies.reasoning.derived_artifacts.context_packet import (
    ContextPacketDefinition,
    ContextPacketResult,
    estimate_tokens,
    experiment_advice,
    load_context_packet_definition,
    problem_class_summary,
    register_context_packet_definition,
    render_context_packet,
    run_context_packet,
    trim_to_token_budget,
)

__all__ = [
    "ContextPacketDefinition",
    "ContextPacketResult",
    "estimate_tokens",
    "experiment_advice",
    "load_context_packet_definition",
    "problem_class_summary",
    "register_context_packet_definition",
    "render_context_packet",
    "run_context_packet",
    "trim_to_token_budget",
]
