"""Local scoped-trust registry and route checks for OKG federation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from okg.substrate.exchange.hashing import canonical_json_sha256_b64url
from okg.substrate.federation.models import (
    DiscoveryDocument,
    FederationValidationError,
)
from okg.substrate.nomos import CompiledPolicyBundle, DecisionInput, evaluate


FEDERATION_PEER_ACCEPT = "federation.peer.accept"
FEDERATION_MESSAGE_SEND = "federation.message.send"
FEDERATION_MESSAGE_RECEIVE = "federation.message.receive"
FEDERATION_GRAPH_PULL = "federation.graph.pull"
FEDERATION_GRAPH_EXPORT = "federation.graph.export"
FEDERATION_GRAPH_RECEIVE = "federation.graph.receive"


@dataclass(frozen=True)
class DiscoveredCandidate:
    authority_id: str
    discovery_hash: str
    document: DiscoveryDocument
    status: str = "pending"

    def to_dict(self) -> dict[str, Any]:
        return {
            "authority_id": self.authority_id,
            "discovery_hash": self.discovery_hash,
            "status": self.status,
            "document": self.document.to_dict(),
        }


@dataclass(frozen=True)
class PeerTrustPin:
    authority_id: str
    key_fingerprint: str
    allowed_actions: tuple[str, ...]
    allowed_boards: tuple[str, ...] = ()
    allowed_pull_surfaces: tuple[str, ...] = ()
    visibility_tiers: tuple[str, ...] = ("public",)
    status: str = "active"
    nomos_decision_id: str | None = None
    nomos_effect: str = "allow"

    def allows_target(self, *, action: str, target_address: str) -> bool:
        if action in {FEDERATION_MESSAGE_SEND, FEDERATION_MESSAGE_RECEIVE}:
            return target_address in self.allowed_boards
        if action in {FEDERATION_GRAPH_PULL, FEDERATION_GRAPH_EXPORT, FEDERATION_GRAPH_RECEIVE}:
            return target_address in self.allowed_pull_surfaces
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "authority_id": self.authority_id,
            "key_fingerprint": self.key_fingerprint,
            "allowed_actions": list(self.allowed_actions),
            "allowed_boards": list(self.allowed_boards),
            "allowed_pull_surfaces": list(self.allowed_pull_surfaces),
            "visibility_tiers": list(self.visibility_tiers),
            "status": self.status,
            "nomos_decision_id": self.nomos_decision_id,
            "nomos_effect": self.nomos_effect,
        }


@dataclass(frozen=True)
class RouteRequest:
    local_authority_id: str
    remote_authority_id: str
    subject: str
    action: str
    target_address: str
    visibility: str = "public"
    context: Mapping[str, Any] = field(default_factory=dict)

    def to_resource(self, pin: PeerTrustPin | None = None) -> dict[str, Any]:
        resource = {
            "local_authority_id": self.local_authority_id,
            "remote_authority_id": self.remote_authority_id,
            "target_address": self.target_address,
            "visibility": self.visibility,
        }
        if pin is not None:
            resource["key_fingerprint"] = pin.key_fingerprint
        return resource


@dataclass(frozen=True)
class RouteDecisionEvent:
    route_event_id: str
    local_authority_id: str
    remote_authority_id: str
    subject: str
    action: str
    target_address: str
    decision: str
    reason_code: str
    nomos_decision_id: str | None = None
    trust_pin_fingerprint: str | None = None

    @property
    def allowed(self) -> bool:
        return self.decision == "allow"

    def to_dict(self) -> dict[str, Any]:
        return {
            "route_event_id": self.route_event_id,
            "local_authority_id": self.local_authority_id,
            "remote_authority_id": self.remote_authority_id,
            "subject": self.subject,
            "action": self.action,
            "target_address": self.target_address,
            "decision": self.decision,
            "reason_code": self.reason_code,
            "nomos_decision_id": self.nomos_decision_id,
            "trust_pin_fingerprint": self.trust_pin_fingerprint,
        }


@dataclass
class LocalFederationRegistry:
    local_authority_id: str
    candidates: dict[str, DiscoveredCandidate] = field(default_factory=dict)
    trusted_peers: dict[str, PeerTrustPin] = field(default_factory=dict)
    route_events: list[RouteDecisionEvent] = field(default_factory=list)

    def discover(self, document: DiscoveryDocument) -> DiscoveredCandidate:
        candidate = DiscoveredCandidate(
            authority_id=document.authority_id,
            discovery_hash=document.discovery_hash,
            document=document,
        )
        self.candidates[document.authority_id] = candidate
        return candidate

    def accept_peer(
        self,
        *,
        authority_id: str,
        key_fingerprint: str,
        allowed_actions: tuple[str, ...],
        allowed_boards: tuple[str, ...] = (),
        allowed_pull_surfaces: tuple[str, ...] = (),
        visibility_tiers: tuple[str, ...] = ("public",),
        nomos_decision_id: str | None,
        nomos_effect: str = "allow",
    ) -> PeerTrustPin:
        candidate = self.candidates.get(authority_id)
        if candidate is None:
            raise FederationValidationError(
                f"cannot accept undiscovered peer: {authority_id}"
            )
        known_fingerprints = {
            key.fingerprint or "" for key in candidate.document.keys
        }
        if key_fingerprint not in known_fingerprints:
            raise FederationValidationError(
                "accepted key fingerprint is absent from discovery document"
            )
        if not nomos_decision_id:
            raise FederationValidationError(
                "peer acceptance requires a durable Nomos decision id"
            )
        pin = PeerTrustPin(
            authority_id=authority_id,
            key_fingerprint=key_fingerprint,
            allowed_actions=tuple(allowed_actions),
            allowed_boards=tuple(allowed_boards),
            allowed_pull_surfaces=tuple(allowed_pull_surfaces),
            visibility_tiers=tuple(visibility_tiers),
            nomos_decision_id=nomos_decision_id,
            nomos_effect=nomos_effect,
        )
        self.trusted_peers[authority_id] = pin
        return pin

    def disable_peer(self, authority_id: str) -> PeerTrustPin:
        pin = self.trusted_peers.get(authority_id)
        if pin is None:
            raise FederationValidationError(f"unknown trusted peer: {authority_id}")
        disabled = PeerTrustPin(
            authority_id=pin.authority_id,
            key_fingerprint=pin.key_fingerprint,
            allowed_actions=pin.allowed_actions,
            allowed_boards=pin.allowed_boards,
            allowed_pull_surfaces=pin.allowed_pull_surfaces,
            visibility_tiers=pin.visibility_tiers,
            status="disabled",
            nomos_decision_id=pin.nomos_decision_id,
            nomos_effect=pin.nomos_effect,
        )
        self.trusted_peers[authority_id] = disabled
        return disabled

    def route_check(
        self,
        request: RouteRequest,
        *,
        nomos_bundle: CompiledPolicyBundle | None = None,
    ) -> RouteDecisionEvent:
        pin = self.trusted_peers.get(request.remote_authority_id)
        if pin is None:
            return self._record_route(request, "deny", "federation.missing_trust_pin")
        if pin.status != "active":
            return self._record_route(
                request,
                "deny",
                "federation.peer_not_active",
                pin=pin,
            )
        if request.action not in pin.allowed_actions:
            return self._record_route(
                request,
                "deny",
                "federation.action_not_scoped",
                pin=pin,
            )
        if request.visibility not in pin.visibility_tiers:
            return self._record_route(
                request,
                "deny",
                "federation.visibility_not_scoped",
                pin=pin,
            )
        if not pin.allows_target(
            action=request.action,
            target_address=request.target_address,
        ):
            return self._record_route(
                request,
                "deny",
                "federation.target_not_scoped",
                pin=pin,
            )

        if nomos_bundle is not None:
            decision = evaluate(
                nomos_bundle,
                DecisionInput(
                    subject=request.subject,
                    action=request.action,
                    resource=request.to_resource(pin),
                    classification=request.visibility,
                    context=dict(request.context),
                ),
                enforcement_point="federation.route_check",
            )
            if decision.effect != "allow":
                return self._record_route(
                    request,
                    "deny",
                    decision.reason_code,
                    pin=pin,
                    nomos_decision_id=decision.decision_id,
                )
            return self._record_route(
                request,
                "allow",
                decision.reason_code,
                pin=pin,
                nomos_decision_id=decision.decision_id,
            )

        if pin.nomos_effect != "allow":
            return self._record_route(
                request,
                "deny",
                f"federation.acceptance_effect_{pin.nomos_effect}",
                pin=pin,
                nomos_decision_id=pin.nomos_decision_id,
            )
        return self._record_route(
            request,
            "allow",
            "federation.route_allowed_by_trust_pin",
            pin=pin,
            nomos_decision_id=pin.nomos_decision_id,
        )

    def _record_route(
        self,
        request: RouteRequest,
        decision: str,
        reason_code: str,
        *,
        pin: PeerTrustPin | None = None,
        nomos_decision_id: str | None = None,
    ) -> RouteDecisionEvent:
        event = RouteDecisionEvent(
            route_event_id=_route_event_id(request, decision, reason_code),
            local_authority_id=request.local_authority_id,
            remote_authority_id=request.remote_authority_id,
            subject=request.subject,
            action=request.action,
            target_address=request.target_address,
            decision=decision,
            reason_code=reason_code,
            nomos_decision_id=nomos_decision_id,
            trust_pin_fingerprint=pin.key_fingerprint if pin else None,
        )
        self.route_events.append(event)
        return event


def _route_event_id(
    request: RouteRequest,
    decision: str,
    reason_code: str,
) -> str:
    digest = canonical_json_sha256_b64url({
        "local_authority_id": request.local_authority_id,
        "remote_authority_id": request.remote_authority_id,
        "subject": request.subject,
        "action": request.action,
        "target_address": request.target_address,
        "visibility": request.visibility,
        "decision": decision,
        "reason_code": reason_code,
    })
    return "federation:route:" + digest[:32]
