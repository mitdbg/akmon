"""harden-connector-route-vetting: vetting fields are a checkable
contract — refusal names the criterion, no silent fallback, pilot
routes still select."""
from __future__ import annotations

import pytest

from okg.substrate.library.sources.discovery import (
    ConnectorRoute,
    ConnectorRouteKind,
    ConnectorTrustTier,
    RouteVettingError,
    rank_connector_routes,
    select_connector_route,
    vet_connector_route,
)
from okg.substrate.library.sources.discovery.registry import (
    _gateway_mcp_route,
    _host_connector_route,
    _provider_mcp_route,
)


def _route(**overrides) -> ConnectorRoute:
    base = dict(
        source="testsrc",
        route_id="testsrc:provider_mcp:acme",
        route_kind=ConnectorRouteKind.PROVIDER_MCP,
        provider="acme",
        label="acme route",
        auth_owner="provider",
        trust_tier=ConnectorTrustTier.PROVIDER_MAINTAINED,
        data_path="provider MCP -> McpComposedSource",
        encryption_at_rest="provider_managed",
        transport_auth="oauth",
        upstream_maintenance="active",
        security_policy="present",
    )
    base.update(overrides)
    return ConnectorRoute(**base)


@pytest.mark.parametrize(
    ("override", "expected_criterion"),
    [
        ({"encryption_at_rest": "plaintext"}, "encryption_at_rest=plaintext"),
        ({"transport_auth": "none"}, "transport_auth=none"),
        (
            {"upstream_maintenance": "archived"},
            "upstream_maintenance=archived",
        ),
        ({"upstream_maintenance": "stale"}, "upstream_maintenance=stale"),
        ({"security_policy": "absent"}, "security_policy=absent"),
    ],
)
def test_failing_route_refused_with_named_criterion(
    override, expected_criterion,
) -> None:
    """Each disqualifying value refuses with the criterion named —
    and there is NO silent fallback to a lower-ranked passing route."""
    bad_best = _route(**override)
    lower_ranked_good = _route(
        route_id="testsrc:cli_tool:acme",
        route_kind=ConnectorRouteKind.CLI_TOOL,
    )
    failed = vet_connector_route(bad_best)
    assert failed == [expected_criterion]
    with pytest.raises(RouteVettingError) as exc_info:
        select_connector_route([bad_best, lower_ranked_good])
    assert expected_criterion in str(exc_info.value)
    assert exc_info.value.route_id == bad_best.route_id


def test_community_tier_requires_explicit_vetting() -> None:
    """`unspecified` vetting on a community-tier route is itself a
    failed criterion (the unvetted-MCP-server failure mode)."""
    route = _route(
        trust_tier=ConnectorTrustTier.COMMUNITY_LOCAL,
        encryption_at_rest="unspecified",
        transport_auth="unspecified",
        upstream_maintenance="unspecified",
        security_policy="unspecified",
    )
    failed = vet_connector_route(route)
    assert len(failed) == 4
    assert all("explicit vetting required" in f for f in failed)


def test_provider_tier_unspecified_passes() -> None:
    """Provider/host tiers may leave fields unspecified without
    refusal — only explicit disqualifying values fail there."""
    route = _route(
        encryption_at_rest="unspecified",
        transport_auth="unspecified",
        upstream_maintenance="unspecified",
        security_policy="unspecified",
    )
    assert vet_connector_route(route) == []
    assert select_connector_route([route]) is route


def test_pilot_routes_redeclare_vetting_and_select() -> None:
    """The existing pilot route constructors (host / provider /
    gateway, used by Google, Slack, GitHub) declare explicit vetting
    facts and still select; rank order is unchanged."""
    host = _host_connector_route(
        source_label="gmail",
        allowlist_fields=["labels"],
        operator_action="connect Gmail",
    )
    provider = _provider_mcp_route(
        source_label="slack",
        provider="slack",
        allowed_tools=["search"],
        allowlist_fields=["channels"],
        operator_action="connect Slack",
    )
    gateway = _gateway_mcp_route(
        source_label="slack",
        provider="hermes",
        allowlist_fields=["channels"],
    )
    for route in (host, provider, gateway):
        assert vet_connector_route(route) == [], route.route_id
        assert route.encryption_at_rest != "unspecified"
        assert route.transport_auth != "unspecified"
    ranked = rank_connector_routes([gateway, provider, host])
    assert [r.route_kind for r in ranked] == [
        "host_connector", "provider_mcp", "gateway_mcp",
    ]
    assert select_connector_route([gateway, provider, host]) is host


def test_vetting_fields_serialize() -> None:
    payload = _route().to_dict()
    for field in (
        "encryption_at_rest", "transport_auth",
        "upstream_maintenance", "security_policy",
    ):
        assert field in payload


def test_setup_planning_surfaces_vetting_failure_loudly() -> None:
    """The setup-planning row for a source whose best route fails
    vetting reports setup_state=vetting_failed with the criterion
    named — visible, not a crash and not a silent fallback."""
    from okg.substrate.mcp.setup_planning import (
        _credentialed_connector_row,
    )

    bad = _route(transport_auth="none").to_dict()
    entry = {
        "source_id": "gmail_api",
        "short": "gmail",
        "candidates": [{"connector_routes": [bad]}],
    }
    row = _credentialed_connector_row(entry)
    assert row["setup_state"] == "vetting_failed"
    assert "transport_auth=none" in row["diagnostics"]["reason"]
    assert row["diagnostics"]["vetting_failed_criteria"] == [
        "transport_auth=none",
    ]
    assert row["selected_route"] is None
