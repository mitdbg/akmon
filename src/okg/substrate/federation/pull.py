"""Generation-pinned federation pull packets built on Poros envelopes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from okg.substrate.exchange.envelope import build_unsigned_envelope, verify_envelope
from okg.substrate.exchange.hashing import canonical_json_sha256_b64url
from okg.substrate.exchange.signing import sign_envelope
from okg.substrate.federation import validation
from okg.substrate.federation.models import FederationValidationError


PULL_PACKET_KIND = "federation.graph.pull.v1"


@dataclass(frozen=True)
class PullPacketValidation:
    packet_hash: str
    origin_authority_id: str
    recipient_authority_id: str
    remote_generation_id: int
    pull_surface_address: str
    catalog_reference: str | None
    node_count: int
    edge_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "packet_hash": self.packet_hash,
            "origin_authority_id": self.origin_authority_id,
            "recipient_authority_id": self.recipient_authority_id,
            "remote_generation_id": self.remote_generation_id,
            "pull_surface_address": self.pull_surface_address,
            "catalog_reference": self.catalog_reference,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
        }


def build_pull_packet(
    *,
    origin_authority_id: str,
    recipient_authority_id: str,
    remote_generation_id: int,
    pull_surface_address: str,
    catalog_reference: str | None,
    nodes: Sequence[Mapping[str, Any]],
    edges: Sequence[Mapping[str, Any]],
    private_key: bytes | str,
    signing_key_id: str,
    graph_profile: str = "context",
    classification: str = "public",
    graph_id: str | None = None,
    surface_discovery_hash: str | None = None,
    redaction_evidence: Mapping[str, Any] | None = None,
    license_evidence: Mapping[str, Any] | None = None,
    remote_export_decision_id: str | None = None,
    message_id: str | None = None,
    ttl_seconds: int = 15 * 60,
) -> dict[str, Any]:
    """Build a signed Poros graph packet for one-shot federation pull."""
    if remote_generation_id <= 0:
        raise FederationValidationError("remote_generation_id must be positive")
    body: dict[str, Any] = {
        "payload_kind": PULL_PACKET_KIND,
        "graph_id": graph_id
        or _default_graph_id(
            origin_authority_id=origin_authority_id,
            pull_surface_address=pull_surface_address,
            remote_generation_id=remote_generation_id,
        ),
        "origin_authority_id": origin_authority_id,
        "origin_generation_id": remote_generation_id,
        "pull_surface_address": pull_surface_address,
        "graph_profile": graph_profile,
        "classification": classification,
        "nodes": [dict(node) for node in nodes],
        "edges": [dict(edge) for edge in edges],
        "redaction_evidence": dict(redaction_evidence or {}),
        "license_evidence": dict(license_evidence or {}),
    }
    if catalog_reference is not None:
        body["catalog_reference"] = catalog_reference
    if surface_discovery_hash is not None:
        body["surface_discovery_hash"] = surface_discovery_hash
    if remote_export_decision_id is not None:
        body["remote_export_decision_id"] = remote_export_decision_id

    envelope = build_unsigned_envelope(
        sender=origin_authority_id,
        recipient=recipient_authority_id,
        body=body,
        message_id=message_id,
        ttl_seconds=ttl_seconds,
    )
    return sign_envelope(envelope, private_key, kid=signing_key_id)


def validate_pull_packet(
    envelope: Mapping[str, Any],
    *,
    public_key: bytes | str,
    expected_origin_authority_id: str,
    expected_recipient_authority_id: str | None = None,
    expected_pull_surface_address: str,
    expected_remote_generation_id: int | None = None,
    expected_catalog_reference: str | None = None,
) -> PullPacketValidation:
    """Verify a pull packet before any local fact insertion."""
    packet = dict(envelope)
    verify_envelope(packet, public_key)
    body = packet.get("body")
    if not isinstance(body, Mapping):
        raise FederationValidationError("pull packet body must be an object")
    if body.get("payload_kind") != PULL_PACKET_KIND:
        raise FederationValidationError("Poros graph envelope is not a pull packet")
    origin = _required_str(body, "origin_authority_id")
    if origin != expected_origin_authority_id or packet.get("sender") != origin:
        raise FederationValidationError("pull packet origin authority mismatch")
    recipient = _required_str(packet, "recipient")
    if (
        expected_recipient_authority_id is not None
        and recipient != expected_recipient_authority_id
    ):
        raise FederationValidationError("pull packet recipient authority mismatch")
    surface = _required_str(body, "pull_surface_address")
    if surface != expected_pull_surface_address:
        raise FederationValidationError("pull packet surface mismatch")
    generation = body.get("origin_generation_id")
    if not isinstance(generation, int) or generation <= 0:
        raise FederationValidationError("pull packet generation must be positive")
    if (
        expected_remote_generation_id is not None
        and generation != expected_remote_generation_id
    ):
        raise FederationValidationError("pull packet generation mismatch")
    catalog_reference = body.get("catalog_reference")
    if catalog_reference is not None and not isinstance(catalog_reference, str):
        raise FederationValidationError("pull packet catalog reference must be string")
    if (
        expected_catalog_reference is not None
        and catalog_reference != expected_catalog_reference
    ):
        raise FederationValidationError("pull packet catalog reference mismatch")
    nodes = body.get("nodes")
    edges = body.get("edges")
    if not isinstance(nodes, list) or not all(isinstance(node, Mapping) for node in nodes):
        raise FederationValidationError("pull packet nodes must be a list of objects")
    if not isinstance(edges, list) or not all(isinstance(edge, Mapping) for edge in edges):
        raise FederationValidationError("pull packet edges must be a list of objects")
    return PullPacketValidation(
        packet_hash="sha256:" + canonical_json_sha256_b64url(packet),
        origin_authority_id=origin,
        recipient_authority_id=recipient,
        remote_generation_id=generation,
        pull_surface_address=surface,
        catalog_reference=catalog_reference,
        node_count=len(nodes),
        edge_count=len(edges),
    )


def _default_graph_id(
    *,
    origin_authority_id: str,
    pull_surface_address: str,
    remote_generation_id: int,
) -> str:
    digest = canonical_json_sha256_b64url({
        "origin_authority_id": origin_authority_id,
        "pull_surface_address": pull_surface_address,
        "remote_generation_id": remote_generation_id,
    })
    return "federation:pull:" + digest[:32]


def _required_str(raw: Mapping[str, Any], key: str) -> str:
    return validation.required_str(
        raw,
        key,
        exc_type=FederationValidationError,
        message=f"missing pull packet field: {key}",
    )
