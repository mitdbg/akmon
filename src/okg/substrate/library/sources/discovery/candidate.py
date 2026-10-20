"""`Candidate` + `Confidence` — the per-source discovery
return shape.

Per the proposal: confidence is a four-value categorical, not
a numeric score. Sources rank candidates within their own
class; `propose_setup` filters by confidence threshold.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


CONFIDENCE_LEVELS = ("high", "medium", "low", "n/a")


class ConnectorRouteKind(str, Enum):
    """How OKG can reach a credentialed third-party source."""

    HOST_CONNECTOR = "host_connector"
    PROVIDER_MCP = "provider_mcp"
    GATEWAY_MCP = "gateway_mcp"
    LOCAL_MCP = "local_mcp"
    CLI_TOOL = "cli_tool"


class ConnectorTrustTier(str, Enum):
    """Trust posture for connector route selection."""

    HOST_MAINTAINED = "host_maintained"
    PROVIDER_MAINTAINED = "provider_maintained"
    LOCAL_USER = "local_user"
    COMMUNITY_LOCAL = "community_local"
    UNKNOWN = "unknown"


_ROUTE_KIND_RANK = {
    ConnectorRouteKind.HOST_CONNECTOR: 50,
    ConnectorRouteKind.PROVIDER_MCP: 40,
    ConnectorRouteKind.GATEWAY_MCP: 30,
    ConnectorRouteKind.LOCAL_MCP: 20,
    ConnectorRouteKind.CLI_TOOL: 10,
}

_TRUST_TIER_RANK = {
    ConnectorTrustTier.HOST_MAINTAINED: 50,
    ConnectorTrustTier.PROVIDER_MAINTAINED: 40,
    ConnectorTrustTier.LOCAL_USER: 30,
    ConnectorTrustTier.COMMUNITY_LOCAL: 10,
    ConnectorTrustTier.UNKNOWN: 0,
}


class Confidence:
    """Confidence levels for discovered candidates.

    - `HIGH`: path exists, looks unambiguously like what the
      source expects, recently active.
    - `MEDIUM`: path exists, plausible, but operator might
      mean something else (multiple candidates).
    - `LOW`: path exists but evidence is thin (small file
      count, dormant for months).
    - `NA`: not applicable (platform mismatch, no env vars,
      install answer absent).
    """

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NA = "n/a"


# Per `harden-connector-route-vetting`: explicit values that
# disqualify a credentialed route. The 2026-06 security audit's
# field failure modes, as a checkable contract: plaintext token
# storage, unauthenticated HTTP transports, archived/stale
# upstreams (the GongRzhe Gmail MCP failure mode), and absent
# security policy.
DISQUALIFYING_VETTING: dict[str, frozenset[str]] = {
    "encryption_at_rest": frozenset({"plaintext"}),
    "transport_auth": frozenset({"none"}),
    "upstream_maintenance": frozenset({"archived", "stale"}),
    "security_policy": frozenset({"absent"}),
}

# Trust tiers that must declare vetting explicitly — an
# `unspecified` vetting field on a community/unknown route is
# itself a failed criterion.
_EXPLICIT_VETTING_TIERS = frozenset({
    ConnectorTrustTier.COMMUNITY_LOCAL,
    ConnectorTrustTier.UNKNOWN,
})


class RouteVettingError(ValueError):
    """Loud refusal: the selected route fails security vetting.

    Never caught to silently fall back to a lower-ranked route —
    a security failure on the policy-preferred route is an
    operator decision, not a ranking detail.
    """

    def __init__(self, route_id: str, failed_criteria: list[str]) -> None:
        self.route_id = route_id
        self.failed_criteria = list(failed_criteria)
        super().__init__(
            f"connector route {route_id!r} refused by security "
            f"vetting: {', '.join(failed_criteria)}"
        )


@dataclass(frozen=True)
class ConnectorRoute:
    """One possible route for a credentialed source.

    Route objects are discovery metadata, not user choices. Setup
    ranks them by trust/friction and either selects one safe read-only
    route or marks the source as awaiting connection/allowlist.
    """

    source: str
    route_id: str
    route_kind: str | ConnectorRouteKind
    provider: str
    label: str
    auth_owner: str
    trust_tier: str | ConnectorTrustTier
    data_path: str
    supports_read: bool = True
    supports_write: bool = False
    allowed_tools: list[str] = field(default_factory=list)
    allowlist_fields: list[str] = field(default_factory=list)
    estimated_setup_effort: str = "medium"
    privacy_summary: str = ""
    requires_operator_action: str = ""
    token_custody: str = "none"
    refresh_model: str = "manual"
    audit_surface: str = "okg logs route metadata and batch summaries"
    status: str = "available"
    # Vetting fields (harden-connector-route-vetting). Values are
    # route-author-declared facts, checked by `vet_connector_route`.
    encryption_at_rest: str = "unspecified"
    transport_auth: str = "unspecified"
    upstream_maintenance: str = "unspecified"
    security_policy: str = "unspecified"

    def __post_init__(self) -> None:
        kind = (
            self.route_kind
            if isinstance(self.route_kind, ConnectorRouteKind)
            else ConnectorRouteKind(str(self.route_kind))
        )
        tier = (
            self.trust_tier
            if isinstance(self.trust_tier, ConnectorTrustTier)
            else ConnectorTrustTier(str(self.trust_tier))
        )
        object.__setattr__(self, "route_kind", kind.value)
        object.__setattr__(self, "trust_tier", tier.value)
        if self.supports_write:
            raise ValueError(
                "ConnectorRoute.supports_write must be false for "
                "first-setup routes"
            )

    @property
    def rank_key(self) -> tuple[int, int, int]:
        """Higher is better for first setup route selection."""
        kind = ConnectorRouteKind(self.route_kind)
        tier = ConnectorTrustTier(self.trust_tier)
        status_bonus = 1 if self.status in {"available", "connected"} else 0
        return (
            status_bonus,
            _ROUTE_KIND_RANK[kind],
            _TRUST_TIER_RANK[tier],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "route_id": self.route_id,
            "route_kind": self.route_kind,
            "provider": self.provider,
            "label": self.label,
            "auth_owner": self.auth_owner,
            "trust_tier": self.trust_tier,
            "data_path": self.data_path,
            "supports_read": self.supports_read,
            "supports_write": self.supports_write,
            "allowed_tools": list(self.allowed_tools),
            "allowlist_fields": list(self.allowlist_fields),
            "estimated_setup_effort": self.estimated_setup_effort,
            "privacy_summary": self.privacy_summary,
            "requires_operator_action": self.requires_operator_action,
            "token_custody": self.token_custody,
            "refresh_model": self.refresh_model,
            "audit_surface": self.audit_surface,
            "status": self.status,
            "encryption_at_rest": self.encryption_at_rest,
            "transport_auth": self.transport_auth,
            "upstream_maintenance": self.upstream_maintenance,
            "security_policy": self.security_policy,
        }


def vet_connector_route(route: ConnectorRoute) -> list[str]:
    """Return the route's failed vetting criteria, named.

    Empty list means the route passes. Explicit disqualifying
    values always fail; `unspecified` additionally fails on
    community/unknown trust tiers, which must declare their
    vetting facts to be selectable.
    """
    failed: list[str] = []
    for field_name, disqualifying in DISQUALIFYING_VETTING.items():
        value = getattr(route, field_name)
        if value in disqualifying:
            failed.append(f"{field_name}={value}")
    tier = ConnectorTrustTier(route.trust_tier)
    if tier in _EXPLICIT_VETTING_TIERS:
        for field_name in DISQUALIFYING_VETTING:
            if getattr(route, field_name) == "unspecified":
                failed.append(
                    f"{field_name}=unspecified (explicit vetting "
                    f"required for {tier.value} tier)"
                )
    return failed


def rank_connector_routes(
    routes: list[ConnectorRoute],
) -> list[ConnectorRoute]:
    """Return safe first-setup routes in policy-preferred order."""
    safe = [
        route for route in routes
        if route.supports_read
        and not route.supports_write
        and route.status in {"available", "connected"}
    ]
    return sorted(safe, key=lambda route: route.rank_key, reverse=True)


def select_connector_route(
    routes: list[ConnectorRoute],
) -> ConnectorRoute | None:
    """Pick the single route setup should propose for a source.

    Raises `RouteVettingError` when the policy-preferred route
    fails security vetting — refusing loudly instead of silently
    falling back to a lower-ranked route (a security failure is
    an operator decision, not a ranking detail).
    """
    ranked = rank_connector_routes(routes)
    if not ranked:
        return None
    winner = ranked[0]
    failed = vet_connector_route(winner)
    if failed:
        raise RouteVettingError(winner.route_id, failed)
    return winner


@dataclass
class Candidate:
    """One configuration the source could be wired with.

    Sources return a `list[Candidate]` from their `discover`
    classmethod; rank order is significant (most-likely
    first). `propose_setup` picks the first candidate whose
    confidence ≥ threshold.

    Fields:
      - `params`: the kwargs that would be passed to
        `add_source(short, params=...)`. For most sources
        this is `{path_key: path_str}`.
      - `path`: optional convenience field for the candidate's
        primary path (UI summaries reference this).
      - `confidence`: `Confidence.HIGH | MEDIUM | LOW | NA`.
      - `evidence`: one short string the agent can speak
        verbatim ("247 markdown files; modified within 7
        days").
      - `summary`: one-liner for the operator-facing plan
        summary. ≤80 chars.
      - `extra`: source-specific structured data (file
        counts by extension, recency, etc.) the agent may
        introspect without parsing the evidence string.
    """

    params: dict[str, Any]
    path: str = ""
    confidence: str = Confidence.NA
    evidence: str = ""
    summary: str = ""
    extra: dict[str, Any] = field(default_factory=dict)
    connector_routes: list[ConnectorRoute] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.confidence not in CONFIDENCE_LEVELS:
            raise ValueError(
                f"Candidate.confidence must be one of "
                f"{CONFIDENCE_LEVELS}; got {self.confidence!r}"
            )
        if len(self.summary) > 80:
            raise ValueError(
                f"Candidate.summary must be ≤80 chars; got "
                f"{len(self.summary)}: {self.summary!r}"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "params": dict(self.params),
            "path": self.path,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "summary": self.summary,
            "extra": dict(self.extra),
            "connector_routes": [
                route.to_dict() for route in self.connector_routes
            ],
        }
