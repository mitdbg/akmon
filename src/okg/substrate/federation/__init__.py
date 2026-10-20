"""Federation primitives for linking sovereign OKG deployments."""

from __future__ import annotations

from importlib import import_module
from typing import Any


_EXPORTS = {
    "BoardAddress": "okg.substrate.federation.models",
    "ConnectionRequest": "okg.substrate.federation.connections",
    "DiscoveredCandidate": "okg.substrate.federation.registry",
    "DiscoveryDocument": "okg.substrate.federation.models",
    "DetachedObjectPointer": "okg.substrate.federation.mailbox",
    "FederationValidationError": "okg.substrate.federation.models",
    "InMemoryMailboxStore": "okg.substrate.federation.mailbox",
    "KeyDescriptor": "okg.substrate.federation.models",
    "LocalFederationRegistry": "okg.substrate.federation.registry",
    "MailboxEvent": "okg.substrate.federation.mailbox",
    "MailboxLocator": "okg.substrate.federation.models",
    "MailboxMessageRecord": "okg.substrate.federation.mailbox",
    "MailboxService": "okg.substrate.federation.mailbox",
    "MailboxValidationError": "okg.substrate.federation.mailbox",
    "MemoryObjectStore": "okg.substrate.federation.mailbox",
    "PeerTrustPin": "okg.substrate.federation.registry",
    "PostgresFederationRegistry": "okg.substrate.federation.connections",
    "PostgresMailboxStore": "okg.substrate.federation.mailbox",
    "PullPacketValidation": "okg.substrate.federation.pull",
    "PullSurface": "okg.substrate.federation.models",
    "RouteDecisionEvent": "okg.substrate.federation.registry",
    "RouteRequest": "okg.substrate.federation.registry",
    "assess_exposure_readiness": "okg.substrate.federation.connections",
    "authority_id_from_okg_uri": "okg.substrate.federation.connections",
    "board_address": "okg.substrate.federation.connections",
    "build_pull_packet": "okg.substrate.federation.pull",
    "canonical_discovery_hash": "okg.substrate.federation.models",
    "connection_request_id": "okg.substrate.federation.connections",
    "default_action_for_target": "okg.substrate.federation.connections",
    "pull_surface_address": "okg.substrate.federation.connections",
    "validate_pull_packet": "okg.substrate.federation.pull",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value
