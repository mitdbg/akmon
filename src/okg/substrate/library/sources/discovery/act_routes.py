"""Act-surface routes — the write-capable counterpart to ingest
ConnectorRoutes, per `adopt-act-mcp-routes`.

First-setup ingest routes are structurally read-only
(`ConnectorRoute.__post_init__` rejects `supports_write`); the act
surface — the agent *sending* the email, *creating* the event — is
deliberately a separate type with its own gates:

  1. **Operator approval is per-activation state**, never implied
     by setup: an act route activates only after explicit
     post-setup approval.
  2. **Hardened modes are pinned in the route definition.** The
     2026-06 security audit identified exactly two
     audit-defensible servers and the modes that make them so;
     activating a route with an unmet pinned condition refuses,
     naming the condition. No silent downgrade.

The vetted definitions encode the audit verdicts:
  - taylorwilsdon/google_workspace_mcp: OAuth 2.1 multi-user with
    per-user token isolation; stateless in-memory (or GCS+CMEK)
    credential store; `--read-only` tool scope for first
    activation.
  - softeria/ms-365-mcp-server: OS-keychain (or Azure Key Vault)
    credential store; bearer-auth HTTP transport;
    `--allowed-scopes` tool gating.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


class ActRouteApprovalError(ValueError):
    """Act route activation attempted without operator approval."""


class ActRouteModeError(ValueError):
    """A pinned hardened-mode condition is unmet — refused loudly,
    naming the condition; never silently downgraded."""

    def __init__(
        self,
        route_id: str,
        unmet: dict[str, tuple[str | None, frozenset[str]]],
    ) -> None:
        self.route_id = route_id
        self.unmet = unmet
        details = "; ".join(
            f"{condition}: declared={declared!r}, accepted="
            f"{sorted(accepted)}"
            for condition, (declared, accepted) in sorted(unmet.items())
        )
        super().__init__(
            f"act route {route_id!r} refused: pinned mode "
            f"condition(s) unmet — {details}"
        )


@dataclass(frozen=True)
class ActRoute:
    """One vetted act-surface route. NOT a ConnectorRoute on
    purpose — see module docstring."""

    source: str
    route_id: str
    provider: str
    label: str
    server: str
    # condition -> the set of declared values that satisfy it
    pinned_modes: Mapping[str, frozenset[str]]
    allowed_tools: list[str] = field(default_factory=list)
    token_custody: str = "server"
    audit_surface: str = (
        "OKG logs act-route activations, tool calls, and refusals"
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "route_id": self.route_id,
            "provider": self.provider,
            "label": self.label,
            "server": self.server,
            "pinned_modes": {
                condition: sorted(accepted)
                for condition, accepted in self.pinned_modes.items()
            },
            "allowed_tools": list(self.allowed_tools),
            "token_custody": self.token_custody,
            "audit_surface": self.audit_surface,
        }


VETTED_ACT_ROUTES: dict[str, ActRoute] = {
    "google_workspace": ActRoute(
        source="google-workspace",
        route_id="google-workspace:act_mcp:workspace-mcp",
        provider="google",
        label="Google Workspace act surface (workspace-mcp)",
        server="taylorwilsdon/google_workspace_mcp",
        pinned_modes={
            "auth": frozenset({"oauth2.1_multi_user"}),
            "credential_store": frozenset({
                "stateless_memory", "gcs_cmek",
            }),
            "tool_scope": frozenset({"read_only"}),
        },
        allowed_tools=["calendar_read", "calendar_create"],
        token_custody="workspace-mcp (per-user bearer validation)",
    ),
    "ms365": ActRoute(
        source="ms365",
        route_id="ms365:act_mcp:ms-365-mcp-server",
        provider="microsoft",
        label="Microsoft 365 act surface (ms-365-mcp-server)",
        server="softeria/ms-365-mcp-server",
        pinned_modes={
            "credential_store": frozenset({
                "os_keychain", "azure_key_vault",
            }),
            "transport_auth": frozenset({"bearer"}),
            "tool_scope": frozenset({"allowed_scopes"}),
        },
        allowed_tools=["calendar_read", "calendar_create"],
        token_custody="ms-365-mcp-server (keychain/Key Vault)",
    ),
}


def activate_act_route(
    name: str,
    *,
    declared_modes: Mapping[str, str],
    operator_approved: bool,
) -> ActRoute:
    """Gate an act-route activation.

    Raises `ActRouteApprovalError` without explicit operator
    approval and `ActRouteModeError` (naming each condition) when
    a pinned hardened mode is unmet. Returns the route only when
    both gates pass.
    """
    if name not in VETTED_ACT_ROUTES:
        raise ValueError(
            f"unknown act route {name!r}; vetted routes: "
            f"{sorted(VETTED_ACT_ROUTES)}"
        )
    route = VETTED_ACT_ROUTES[name]
    if not operator_approved:
        raise ActRouteApprovalError(
            f"act route {route.route_id!r} requires explicit "
            "post-setup operator approval; act surfaces are never "
            "activated implicitly by setup"
        )
    unmet: dict[str, tuple[str | None, frozenset[str]]] = {}
    for condition, accepted in route.pinned_modes.items():
        declared = declared_modes.get(condition)
        if declared not in accepted:
            unmet[condition] = (declared, accepted)
    if unmet:
        raise ActRouteModeError(route.route_id, unmet)
    return route
