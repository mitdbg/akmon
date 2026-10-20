"""Route recommendations — explainable optimizer output for AKMON.

A route is a declarative recommendation that maps a context (file,
change, deployment, …) to one or more suggested tools the agent
should run next. Routes are declared per-deployment in a
``routes.yaml`` manifest and resolved by :func:`recommend_routes`.

This is the AKMON Phase 6.2 kernel: explainable optimizer output,
not hidden MCP behavior. The recommendation surface is pure and
deterministic; the MCP trace event log
(``okg.mcp_trace_events.route_id``) is the loop closure that lets a
deployment measure recommendation→tool-use over time and graduate
patterns into the broader algebra.
"""
from __future__ import annotations

from .manifest import (
    RouteManifest,
    RouteManifestError,
    RouteRule,
    load_manifest,
    load_manifest_for_deployment,
)
from .recommend import (
    RouteRecommendation,
    RouteRecommendationContext,
    recommend_routes,
)

__all__ = [
    "RouteManifest",
    "RouteManifestError",
    "RouteRecommendation",
    "RouteRecommendationContext",
    "RouteRule",
    "load_manifest",
    "load_manifest_for_deployment",
    "recommend_routes",
]
