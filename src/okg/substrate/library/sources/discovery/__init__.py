"""Source discovery — let the agent figure out what to wire
from the operator's machine state instead of asking them.

Per `improve-add-source-operator-experience` (the intent-and-
approval pivot): each `Source` class MAY implement
`discover(env: DiscoveryEnv) -> list[Candidate]` to return
ranked candidate configurations the source could be wired
with. The MCP `discover_sources` / `propose_setup` /
`commit_setup` tools aggregate these into a single
approval-shape plan so the operator's typed inputs reduce to
intent ("set me up") and approval ("yes").

`DiscoveryEnv` is the only filesystem-and-env IO surface
sources see during discovery — bounded scan, auditable, no
direct `os.listdir` / `pathlib.Path.glob`. The MCP tool
reports timing + every IO call discovery made.
"""
from __future__ import annotations

from .candidate import (
    Candidate,
    Confidence,
    ConnectorRoute,
    ConnectorRouteKind,
    ConnectorTrustTier,
    RouteVettingError,
    rank_connector_routes,
    select_connector_route,
    vet_connector_route,
)
from .env import DiscoveryEnv


__all__ = [
    "Candidate",
    "Confidence",
    "ConnectorRoute",
    "ConnectorRouteKind",
    "ConnectorTrustTier",
    "DiscoveryEnv",
    "RouteVettingError",
    "rank_connector_routes",
    "select_connector_route",
    "vet_connector_route",
]
