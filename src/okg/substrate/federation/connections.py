"""Durable connection lifecycle for OKG federation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping
from urllib.parse import urlsplit

from psycopg.types.json import Jsonb

from okg.substrate.exchange.hashing import canonical_json_sha256_b64url
from okg.substrate.federation.models import (
    DiscoveryDocument,
    FederationValidationError,
)
from okg.substrate.federation.registry import (
    FEDERATION_GRAPH_EXPORT,
    FEDERATION_GRAPH_PULL,
    FEDERATION_GRAPH_RECEIVE,
    FEDERATION_MESSAGE_RECEIVE,
    FEDERATION_MESSAGE_SEND,
)
from okg.substrate.db.connection import connect as db_connect


CONNECTION_STATUS_PENDING = "pending"
CONNECTION_STATUS_APPROVED = "approved"
CONNECTION_STATUS_DENIED = "denied"
CONNECTION_STATUS_QUARANTINED = "quarantined"

WORLD_OPEN_EXPOSURE = "world-open"
PILOT_EXPOSURE = "pilot"


@dataclass(frozen=True)
class ConnectionRequest:
    request_id: str
    local_authority_id: str
    requester_authority_id: str
    target_authority_id: str
    requested_action: str
    target_address: str
    requested_visibility: str
    key_fingerprint: str
    discovery_hash: str | None
    status: str
    deployment_name: str = "default"
    reviewer_identity: str | None = None
    nomos_decision_id: str | None = None
    nomos_decision_deployment_name: str = "default"
    decision_evidence: Mapping[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "deployment_name": self.deployment_name,
            "local_authority_id": self.local_authority_id,
            "requester_authority_id": self.requester_authority_id,
            "target_authority_id": self.target_authority_id,
            "requested_action": self.requested_action,
            "target_address": self.target_address,
            "requested_visibility": self.requested_visibility,
            "key_fingerprint": self.key_fingerprint,
            "discovery_hash": self.discovery_hash,
            "status": self.status,
            "reviewer_identity": self.reviewer_identity,
            "nomos_decision_id": self.nomos_decision_id,
            "nomos_decision_deployment_name": self.nomos_decision_deployment_name,
            "decision_evidence": dict(self.decision_evidence or {}),
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
        }


@dataclass(frozen=True)
class StoredCandidate:
    local_authority_id: str
    remote_authority_id: str
    discovery_hash: str
    document: DiscoveryDocument
    status: str
    deployment_name: str = "default"

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment_name": self.deployment_name,
            "local_authority_id": self.local_authority_id,
            "remote_authority_id": self.remote_authority_id,
            "discovery_hash": self.discovery_hash,
            "status": self.status,
            "document": self.document.to_dict(),
        }


@dataclass(frozen=True)
class StoredTrustPin:
    local_authority_id: str
    remote_authority_id: str
    key_fingerprint: str
    allowed_actions: tuple[str, ...]
    allowed_boards: tuple[str, ...]
    allowed_pull_surfaces: tuple[str, ...]
    visibility_tiers: tuple[str, ...]
    status: str
    nomos_decision_id: str | None
    nomos_decision_deployment_name: str
    nomos_effect: str
    decision_evidence: Mapping[str, Any]
    deployment_name: str = "default"

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment_name": self.deployment_name,
            "local_authority_id": self.local_authority_id,
            "remote_authority_id": self.remote_authority_id,
            "key_fingerprint": self.key_fingerprint,
            "allowed_actions": list(self.allowed_actions),
            "allowed_boards": list(self.allowed_boards),
            "allowed_pull_surfaces": list(self.allowed_pull_surfaces),
            "visibility_tiers": list(self.visibility_tiers),
            "status": self.status,
            "nomos_decision_id": self.nomos_decision_id,
            "nomos_decision_deployment_name": self.nomos_decision_deployment_name,
            "nomos_effect": self.nomos_effect,
            "decision_evidence": dict(self.decision_evidence),
        }


@dataclass(frozen=True)
class StoredRouteEvent:
    route_event_id: str
    local_authority_id: str
    remote_authority_id: str
    subject: str
    action: str
    target_address: str
    visibility: str
    decision: str
    reason_code: str
    nomos_decision_id: str | None
    nomos_decision_deployment_name: str
    trust_pin_fingerprint: str | None
    live_row_delta: Mapping[str, int]
    deployment_name: str = "default"
    created_at: datetime | None = None

    @property
    def allowed(self) -> bool:
        return self.decision == "allow"

    def to_dict(self) -> dict[str, Any]:
        return {
            "route_event_id": self.route_event_id,
            "deployment_name": self.deployment_name,
            "local_authority_id": self.local_authority_id,
            "remote_authority_id": self.remote_authority_id,
            "subject": self.subject,
            "action": self.action,
            "target_address": self.target_address,
            "visibility": self.visibility,
            "decision": self.decision,
            "reason_code": self.reason_code,
            "nomos_decision_id": self.nomos_decision_id,
            "nomos_decision_deployment_name": self.nomos_decision_deployment_name,
            "trust_pin_fingerprint": self.trust_pin_fingerprint,
            "live_row_delta": dict(self.live_row_delta),
            "created_at": _iso(self.created_at),
        }


@dataclass(frozen=True)
class ExposureReadiness:
    exposure: str
    ready: bool
    missing: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "exposure": self.exposure,
            "ready": self.ready,
            "missing": list(self.missing),
            "warnings": list(self.warnings),
        }


class PostgresFederationRegistry:
    """Postgres-backed federation registry used by high-level UX commands."""

    def __init__(self, dsn: str, *, deployment_name: str = "default") -> None:
        if not dsn:
            raise FederationValidationError("Postgres DSN is required")
        self.dsn = dsn
        self.deployment_name = deployment_name

    def discover(
        self,
        *,
        local_authority_id: str,
        document: DiscoveryDocument,
    ) -> StoredCandidate:
        with db_connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.federation_discovered_candidates (
                      deployment_name,
                      local_authority_id,
                      remote_authority_id,
                      discovery_hash,
                      document,
                      status
                    )
                    VALUES (%s, %s, %s, %s, %s, 'pending')
                    ON CONFLICT (
                      deployment_name,
                      local_authority_id,
                      remote_authority_id
                    )
                    DO UPDATE SET
                      discovery_hash = EXCLUDED.discovery_hash,
                      document = EXCLUDED.document,
                      updated_at = now()
                    RETURNING
                      local_authority_id,
                      remote_authority_id,
                      discovery_hash,
                      document,
                      status,
                      deployment_name
                    """,
                    (
                        self.deployment_name,
                        local_authority_id,
                        document.authority_id,
                        document.discovery_hash,
                        Jsonb(document.to_dict()),
                    ),
                )
                row = cur.fetchone()
        if row is None:
            raise FederationValidationError("candidate discovery did not return a row")
        return _candidate_from_row(row)

    def get_candidate(
        self,
        *,
        local_authority_id: str,
        remote_authority_id: str,
    ) -> StoredCandidate | None:
        with db_connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT local_authority_id,
                           remote_authority_id,
                           discovery_hash,
                           document,
                           status,
                           deployment_name
                      FROM okg.federation_discovered_candidates
                     WHERE deployment_name = %s
                       AND local_authority_id = %s
                       AND remote_authority_id = %s
                    """,
                    (self.deployment_name, local_authority_id, remote_authority_id),
                )
                row = cur.fetchone()
        return _candidate_from_row(row) if row else None

    def find_candidate_for_uri(
        self,
        *,
        local_authority_id: str,
        okg_uri: str,
    ) -> StoredCandidate | None:
        derived_authority = authority_id_from_okg_uri(okg_uri)
        with db_connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT local_authority_id,
                           remote_authority_id,
                           discovery_hash,
                           document,
                           status,
                           deployment_name
                      FROM okg.federation_discovered_candidates
                     WHERE deployment_name = %s
                       AND local_authority_id = %s
                     ORDER BY updated_at DESC, created_at DESC
                    """,
                    (self.deployment_name, local_authority_id),
                )
                rows = cur.fetchall()
        for row in rows:
            candidate = _candidate_from_row(row)
            if candidate.remote_authority_id == derived_authority:
                return candidate
            if discovery_document_matches_uri(candidate.document, okg_uri):
                return candidate
        return None

    def create_request(
        self,
        *,
        local_authority_id: str,
        requester_authority_id: str,
        target_authority_id: str,
        requested_action: str,
        target_address: str,
        requested_visibility: str,
        key_fingerprint: str,
        discovery_hash: str | None,
    ) -> ConnectionRequest:
        if not key_fingerprint:
            raise FederationValidationError(
                "connection request requires a peer key fingerprint"
            )
        request_id = connection_request_id(
            deployment_name=self.deployment_name,
            local_authority_id=local_authority_id,
            requester_authority_id=requester_authority_id,
            target_authority_id=target_authority_id,
            requested_action=requested_action,
            target_address=target_address,
            requested_visibility=requested_visibility,
            key_fingerprint=key_fingerprint,
            discovery_hash=discovery_hash,
        )
        with db_connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.federation_connection_requests (
                      deployment_name,
                      request_id,
                      local_authority_id,
                      requester_authority_id,
                      target_authority_id,
                      requested_action,
                      target_address,
                      requested_visibility,
                      key_fingerprint,
                      discovery_hash,
                      status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                    ON CONFLICT (deployment_name, request_id) DO NOTHING
                    """,
                    (
                        self.deployment_name,
                        request_id,
                        local_authority_id,
                        requester_authority_id,
                        target_authority_id,
                        requested_action,
                        target_address,
                        requested_visibility,
                        key_fingerprint,
                        discovery_hash,
                    ),
                )
        request = self.get_request(request_id)
        if request is None:
            raise FederationValidationError("connection request was not persisted")
        return request

    def get_request(self, request_id: str) -> ConnectionRequest | None:
        with db_connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT request_id,
                           local_authority_id,
                           requester_authority_id,
                           target_authority_id,
                           requested_action,
                           target_address,
                           requested_visibility,
                           key_fingerprint,
                           discovery_hash,
                           status,
                           reviewer_identity,
                           nomos_decision_id,
                           nomos_decision_deployment_name,
                           decision_evidence,
                           created_at,
                           updated_at,
                           deployment_name
                      FROM okg.federation_connection_requests
                     WHERE deployment_name = %s
                       AND request_id = %s
                    """,
                    (self.deployment_name, request_id),
                )
                row = cur.fetchone()
        return _request_from_row(row) if row else None

    def approve_request(
        self,
        *,
        request_id: str,
        reviewer_identity: str,
        nomos_decision_id: str,
        decision_evidence: Mapping[str, Any] | None = None,
    ) -> tuple[ConnectionRequest, StoredTrustPin]:
        if not nomos_decision_id:
            raise FederationValidationError(
                "connection approval requires a durable Nomos decision id"
            )
        request = self.get_request(request_id)
        if request is None:
            raise FederationValidationError(f"unknown connection request: {request_id}")
        if request.status not in {
            CONNECTION_STATUS_PENDING,
            CONNECTION_STATUS_APPROVED,
        }:
            raise FederationValidationError(
                f"cannot approve connection request in status {request.status}"
            )
        peer_authority_id = peer_authority_for_action(request)
        allowed_boards, allowed_pull_surfaces = scopes_for_request(request)
        evidence = {
            "connection_request_id": request_id,
            **dict(decision_evidence or {}),
        }
        with db_connect(self.dsn, autocommit=True) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    self._upsert_approval_decision(
                        cur,
                        request=request,
                        reviewer_identity=reviewer_identity,
                        nomos_decision_id=nomos_decision_id,
                        evidence=evidence,
                    )
                    cur.execute(
                        """
                        UPDATE okg.federation_connection_requests
                           SET status = 'approved',
                               reviewer_identity = %s,
                               nomos_decision_id = %s,
                               nomos_decision_deployment_name = %s,
                               decision_evidence = %s,
                               updated_at = now()
                         WHERE request_id = %s
                           AND deployment_name = %s
                        """,
                        (
                            reviewer_identity,
                            nomos_decision_id,
                            self.deployment_name,
                            Jsonb(evidence),
                            request_id,
                            self.deployment_name,
                        ),
                    )
                    cur.execute(
                        """
                        INSERT INTO okg.federation_peer_trust_pins (
                          deployment_name,
                          local_authority_id,
                          remote_authority_id,
                          key_fingerprint,
                          allowed_actions,
                          allowed_boards,
                          allowed_pull_surfaces,
                          visibility_tiers,
                          status,
                          nomos_decision_id,
                          nomos_decision_deployment_name,
                          nomos_effect,
                          decision_evidence
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                                'active', %s, %s, 'allow', %s)
                        ON CONFLICT (
                          deployment_name,
                          local_authority_id,
                          remote_authority_id,
                          key_fingerprint
                        )
                        DO UPDATE SET
                          allowed_actions = EXCLUDED.allowed_actions,
                          allowed_boards = EXCLUDED.allowed_boards,
                          allowed_pull_surfaces = EXCLUDED.allowed_pull_surfaces,
                          visibility_tiers = EXCLUDED.visibility_tiers,
                          status = 'active',
                          nomos_decision_id = EXCLUDED.nomos_decision_id,
                          nomos_decision_deployment_name =
                            EXCLUDED.nomos_decision_deployment_name,
                          nomos_effect = 'allow',
                          decision_evidence = EXCLUDED.decision_evidence,
                          updated_at = now()
                        RETURNING
                          local_authority_id,
                          remote_authority_id,
                          key_fingerprint,
                          allowed_actions,
                          allowed_boards,
                          allowed_pull_surfaces,
                          visibility_tiers,
                          status,
                          nomos_decision_id,
                          nomos_decision_deployment_name,
                          nomos_effect,
                          decision_evidence,
                          deployment_name
                        """,
                        (
                            self.deployment_name,
                            request.local_authority_id,
                            peer_authority_id,
                            request.key_fingerprint,
                            [request.requested_action],
                            list(allowed_boards),
                            list(allowed_pull_surfaces),
                            [request.requested_visibility],
                            nomos_decision_id,
                            self.deployment_name,
                            Jsonb(evidence),
                        ),
                    )
                    pin_row = cur.fetchone()
        approved = self.get_request(request_id)
        if approved is None or pin_row is None:
            raise FederationValidationError("connection approval did not persist")
        return approved, _pin_from_row(pin_row)

    def _upsert_approval_decision(
        self,
        cur,
        *,
        request: ConnectionRequest,
        reviewer_identity: str,
        nomos_decision_id: str,
        evidence: Mapping[str, Any],
    ) -> None:
        target = {
            "kind": "federation_connection_request",
            "connection_request_id": request.request_id,
            "local_authority_id": request.local_authority_id,
            "requester_authority_id": request.requester_authority_id,
            "target_authority_id": request.target_authority_id,
            "target_address": request.target_address,
            "requested_action": request.requested_action,
        }
        decision_input = {
            "target": target,
            "reviewer_identity": reviewer_identity,
            "decision_evidence": dict(evidence),
        }
        reason_code = str(
            evidence.get("reason_code") or "federation.connection.approved"
        )
        cur.execute(
            """
            INSERT INTO okg.nomos_decisions (
              deployment_name, decision_id, input_hash, subject, action,
              resource, classification, context, effect, reason_code,
              matched_rule_ids, obligations, enforcement_point, target
            )
            VALUES (
              %s, %s, %s, %s, 'federation.connection.approve',
              %s, 'public', %s, 'allow', %s,
              ARRAY['federation_connection_approval']::TEXT[], '[]'::JSONB,
              'federation.connection.approve', %s
            )
            ON CONFLICT (deployment_name, decision_id)
            DO UPDATE SET target = EXCLUDED.target
            """,
            (
                self.deployment_name,
                nomos_decision_id,
                "sha256:" + canonical_json_sha256_b64url(decision_input),
                reviewer_identity,
                Jsonb(
                    {
                        "kind": "federation_connection",
                        "target_address": request.target_address,
                    }
                ),
                Jsonb({"decision_evidence": dict(evidence)}),
                reason_code,
                Jsonb(target),
            ),
        )

    def deny_request(
        self,
        *,
        request_id: str,
        reviewer_identity: str,
        reason_code: str,
        decision_evidence: Mapping[str, Any] | None = None,
    ) -> ConnectionRequest:
        evidence = {
            "reason_code": reason_code,
            **dict(decision_evidence or {}),
        }
        with db_connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE okg.federation_connection_requests
                       SET status = 'denied',
                           reviewer_identity = %s,
                           decision_evidence = %s,
                           updated_at = now()
                     WHERE request_id = %s
                       AND deployment_name = %s
                    """,
                    (
                        reviewer_identity,
                        Jsonb(evidence),
                        request_id,
                        self.deployment_name,
                    ),
                )
        request = self.get_request(request_id)
        if request is None:
            raise FederationValidationError(f"unknown connection request: {request_id}")
        return request

    def disable_peer(
        self,
        *,
        local_authority_id: str,
        remote_authority_id: str,
        reason_code: str = "federation.peer_disabled",
    ) -> StoredTrustPin:
        with db_connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE okg.federation_peer_trust_pins
                       SET status = 'disabled',
                           decision_evidence =
                             COALESCE(decision_evidence, '{}'::jsonb)
                             || %s::jsonb,
                           updated_at = now()
                     WHERE local_authority_id = %s
                       AND remote_authority_id = %s
                       AND deployment_name = %s
                       AND status = 'active'
                    RETURNING
                      local_authority_id,
                      remote_authority_id,
                      key_fingerprint,
                      allowed_actions,
                      allowed_boards,
                      allowed_pull_surfaces,
                      visibility_tiers,
                      status,
                      nomos_decision_id,
                      nomos_decision_deployment_name,
                      nomos_effect,
                      decision_evidence,
                      deployment_name
                    """,
                    (
                        Jsonb({"disable_reason_code": reason_code}),
                        local_authority_id,
                        remote_authority_id,
                        self.deployment_name,
                    ),
                )
                row = cur.fetchone()
        if row is None:
            raise FederationValidationError(
                f"no active trust pin for peer: {remote_authority_id}"
            )
        return _pin_from_row(row)

    def route_check(
        self,
        *,
        local_authority_id: str,
        remote_authority_id: str,
        subject: str,
        action: str,
        target_address: str,
        visibility: str = "public",
    ) -> StoredRouteEvent:
        pin = self._load_active_pin(
            local_authority_id=local_authority_id,
            remote_authority_id=remote_authority_id,
        )
        if pin is None:
            return self._record_route(
                local_authority_id=local_authority_id,
                remote_authority_id=remote_authority_id,
                subject=subject,
                action=action,
                target_address=target_address,
                visibility=visibility,
                decision="deny",
                reason_code="federation.missing_trust_pin",
                pin=None,
            )
        if pin.status != "active":
            return self._record_route(
                local_authority_id=local_authority_id,
                remote_authority_id=remote_authority_id,
                subject=subject,
                action=action,
                target_address=target_address,
                visibility=visibility,
                decision="deny",
                reason_code="federation.peer_not_active",
                pin=pin,
            )
        if action not in pin.allowed_actions:
            return self._record_route(
                local_authority_id=local_authority_id,
                remote_authority_id=remote_authority_id,
                subject=subject,
                action=action,
                target_address=target_address,
                visibility=visibility,
                decision="deny",
                reason_code="federation.action_not_scoped",
                pin=pin,
            )
        if visibility not in pin.visibility_tiers:
            return self._record_route(
                local_authority_id=local_authority_id,
                remote_authority_id=remote_authority_id,
                subject=subject,
                action=action,
                target_address=target_address,
                visibility=visibility,
                decision="deny",
                reason_code="federation.visibility_not_scoped",
                pin=pin,
            )
        if not _pin_allows_target(pin, action=action, target_address=target_address):
            return self._record_route(
                local_authority_id=local_authority_id,
                remote_authority_id=remote_authority_id,
                subject=subject,
                action=action,
                target_address=target_address,
                visibility=visibility,
                decision="deny",
                reason_code="federation.target_not_scoped",
                pin=pin,
            )
        if pin.nomos_effect != "allow":
            return self._record_route(
                local_authority_id=local_authority_id,
                remote_authority_id=remote_authority_id,
                subject=subject,
                action=action,
                target_address=target_address,
                visibility=visibility,
                decision="deny",
                reason_code=f"federation.acceptance_effect_{pin.nomos_effect}",
                pin=pin,
            )
        return self._record_route(
            local_authority_id=local_authority_id,
            remote_authority_id=remote_authority_id,
            subject=subject,
            action=action,
            target_address=target_address,
            visibility=visibility,
            decision="allow",
            reason_code="federation.route_allowed_by_trust_pin",
            pin=pin,
        )

    def _load_active_pin(
        self,
        *,
        local_authority_id: str,
        remote_authority_id: str,
    ) -> StoredTrustPin | None:
        with db_connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT local_authority_id,
                           remote_authority_id,
                           key_fingerprint,
                           allowed_actions,
                           allowed_boards,
                           allowed_pull_surfaces,
                           visibility_tiers,
                           status,
                           nomos_decision_id,
                           nomos_decision_deployment_name,
                           nomos_effect,
                           decision_evidence,
                           deployment_name
                      FROM okg.federation_peer_trust_pins
                     WHERE deployment_name = %s
                       AND local_authority_id = %s
                       AND remote_authority_id = %s
                     ORDER BY updated_at DESC, created_at DESC
                     LIMIT 1
                    """,
                    (self.deployment_name, local_authority_id, remote_authority_id),
                )
                row = cur.fetchone()
        return _pin_from_row(row) if row else None

    def _record_route(
        self,
        *,
        local_authority_id: str,
        remote_authority_id: str,
        subject: str,
        action: str,
        target_address: str,
        visibility: str,
        decision: str,
        reason_code: str,
        pin: StoredTrustPin | None,
    ) -> StoredRouteEvent:
        event_id = route_event_id(
            deployment_name=self.deployment_name,
            local_authority_id=local_authority_id,
            remote_authority_id=remote_authority_id,
            subject=subject,
            action=action,
            target_address=target_address,
            visibility=visibility,
            decision=decision,
            reason_code=reason_code,
        )
        with db_connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO okg.federation_route_events (
                      deployment_name,
                      route_event_id,
                      local_authority_id,
                      remote_authority_id,
                      subject,
                      action,
                      target_address,
                      visibility,
                      decision,
                      reason_code,
                      nomos_decision_id,
                      nomos_decision_deployment_name,
                      trust_pin_fingerprint,
                      live_row_delta
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (deployment_name, route_event_id) DO UPDATE SET
                      created_at = okg.federation_route_events.created_at
                    RETURNING
                      route_event_id,
                      local_authority_id,
                      remote_authority_id,
                      subject,
                      action,
                      target_address,
                      visibility,
                      decision,
                      reason_code,
                      nomos_decision_id,
                      nomos_decision_deployment_name,
                      trust_pin_fingerprint,
                      live_row_delta,
                      created_at,
                      deployment_name
                    """,
                    (
                        self.deployment_name,
                        event_id,
                        local_authority_id,
                        remote_authority_id,
                        subject,
                        action,
                        target_address,
                        visibility,
                        decision,
                        reason_code,
                        pin.nomos_decision_id if pin else None,
                        pin.nomos_decision_deployment_name
                        if pin else self.deployment_name,
                        pin.key_fingerprint if pin else None,
                        Jsonb({"nodes_live": 0, "edges_live": 0}),
                    ),
                )
                row = cur.fetchone()
        if row is None:
            raise FederationValidationError("route event was not persisted")
        return _route_from_row(row)


def connection_request_id(
    *,
    deployment_name: str = "default",
    local_authority_id: str,
    requester_authority_id: str,
    target_authority_id: str,
    requested_action: str,
    target_address: str,
    requested_visibility: str,
    key_fingerprint: str,
    discovery_hash: str | None,
) -> str:
    payload = {
        "local_authority_id": local_authority_id,
        "requester_authority_id": requester_authority_id,
        "target_authority_id": target_authority_id,
        "requested_action": requested_action,
        "target_address": target_address,
        "requested_visibility": requested_visibility,
        "key_fingerprint": key_fingerprint,
        "discovery_hash": discovery_hash,
    }
    if deployment_name != "default":
        payload["deployment_name"] = deployment_name
    digest = canonical_json_sha256_b64url(payload)
    return "federation:connection:" + digest[:32]


def route_event_id(
    *,
    deployment_name: str = "default",
    local_authority_id: str,
    remote_authority_id: str,
    subject: str,
    action: str,
    target_address: str,
    visibility: str,
    decision: str,
    reason_code: str,
) -> str:
    payload = {
        "local_authority_id": local_authority_id,
        "remote_authority_id": remote_authority_id,
        "subject": subject,
        "action": action,
        "target_address": target_address,
        "visibility": visibility,
        "decision": decision,
        "reason_code": reason_code,
    }
    if deployment_name != "default":
        payload["deployment_name"] = deployment_name
    digest = canonical_json_sha256_b64url(payload)
    return "federation:route:" + digest[:32]


def authority_id_from_okg_uri(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme != "okg" or not parsed.netloc:
        raise FederationValidationError(f"expected okg:// URI, got: {value}")
    return "okg:" + parsed.netloc


def board_address(target: str, board: str) -> str:
    if board.startswith("okg://"):
        return board
    _ensure_okg_uri(target)
    return target.rstrip("/") + "/boards/" + board.strip("/")


def pull_surface_address(target: str, surface: str) -> str:
    if surface.startswith("okg://"):
        return surface
    _ensure_okg_uri(target)
    return target.rstrip("/") + "/pull/" + surface.strip("/")


def discovery_document_matches_uri(document: DiscoveryDocument, okg_uri: str) -> bool:
    base_authority = authority_id_from_okg_uri(okg_uri)
    if document.authority_id == base_authority:
        return True
    prefix = okg_uri.rstrip("/") + "/"
    addresses = [
        *(board.address for board in document.boards),
        *(surface.address for surface in document.pull_surfaces),
        *(mailbox.address for mailbox in document.mailboxes),
    ]
    return any(address == okg_uri or address.startswith(prefix) for address in addresses)


def peer_authority_for_action(request: ConnectionRequest) -> str:
    if request.requested_action in {FEDERATION_MESSAGE_SEND, FEDERATION_GRAPH_PULL}:
        return request.target_authority_id
    if request.requested_action in {
        FEDERATION_MESSAGE_RECEIVE,
        FEDERATION_GRAPH_EXPORT,
        FEDERATION_GRAPH_RECEIVE,
    }:
        return request.requester_authority_id
    raise FederationValidationError(
        f"unsupported federation action: {request.requested_action}"
    )


def scopes_for_request(
    request: ConnectionRequest,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if request.requested_action in {FEDERATION_MESSAGE_SEND, FEDERATION_MESSAGE_RECEIVE}:
        return (request.target_address,), ()
    if request.requested_action in {
        FEDERATION_GRAPH_PULL,
        FEDERATION_GRAPH_EXPORT,
        FEDERATION_GRAPH_RECEIVE,
    }:
        return (), (request.target_address,)
    raise FederationValidationError(
        f"unsupported federation action: {request.requested_action}"
    )


def default_action_for_target(*, board: str | None, surface: str | None) -> str:
    if board:
        return FEDERATION_MESSAGE_SEND
    if surface:
        return FEDERATION_GRAPH_PULL
    raise FederationValidationError("connection target requires --board or --surface")


def assess_exposure_readiness(
    *,
    exposure: str,
    dns_tls: bool = False,
    auth_or_rate_limit: bool = False,
    quotas: bool = False,
    retention: bool = False,
    object_store_encryption: bool = False,
    key_rotation: bool = False,
    audit_logs: bool = False,
    backups: bool = False,
    monitoring: bool = False,
    nomos_receive_policy: bool = False,
    network_restricted: bool = False,
) -> ExposureReadiness:
    if exposure not in {WORLD_OPEN_EXPOSURE, PILOT_EXPOSURE}:
        raise FederationValidationError(f"unknown exposure mode: {exposure}")
    required = {
        "dns_tls": dns_tls,
        "auth_or_rate_limit": auth_or_rate_limit,
        "quotas": quotas,
        "retention": retention,
        "object_store_encryption": object_store_encryption,
        "key_rotation": key_rotation,
        "audit_logs": audit_logs,
        "backups": backups,
        "monitoring": monitoring,
        "nomos_receive_policy": nomos_receive_policy,
    }
    if exposure == WORLD_OPEN_EXPOSURE:
        missing = tuple(name for name, present in required.items() if not present)
        return ExposureReadiness(
            exposure=exposure,
            ready=not missing,
            missing=missing,
            warnings=(),
        )
    warnings = ()
    if not network_restricted:
        warnings = (
            "pilot exposure should be network-restricted or explicitly marked unsafe",
        )
    return ExposureReadiness(
        exposure=exposure,
        ready=network_restricted,
        missing=() if network_restricted else ("network_restricted",),
        warnings=warnings,
    )


def _ensure_okg_uri(value: str) -> None:
    parsed = urlsplit(value)
    if parsed.scheme != "okg" or not parsed.netloc:
        raise FederationValidationError(f"expected okg:// URI, got: {value}")


def _pin_allows_target(
    pin: StoredTrustPin,
    *,
    action: str,
    target_address: str,
) -> bool:
    if action in {FEDERATION_MESSAGE_SEND, FEDERATION_MESSAGE_RECEIVE}:
        return target_address in pin.allowed_boards
    if action in {FEDERATION_GRAPH_PULL, FEDERATION_GRAPH_EXPORT, FEDERATION_GRAPH_RECEIVE}:
        return target_address in pin.allowed_pull_surfaces
    return False


def _candidate_from_row(row: Any) -> StoredCandidate:
    return StoredCandidate(
        local_authority_id=str(row[0]),
        remote_authority_id=str(row[1]),
        discovery_hash=str(row[2]),
        document=DiscoveryDocument.from_mapping(_json_object(row[3])),
        status=str(row[4]),
        deployment_name=str(row[5] or "default"),
    )


def _request_from_row(row: Any) -> ConnectionRequest:
    return ConnectionRequest(
        request_id=str(row[0]),
        local_authority_id=str(row[1]),
        requester_authority_id=str(row[2]),
        target_authority_id=str(row[3]),
        requested_action=str(row[4]),
        target_address=str(row[5]),
        requested_visibility=str(row[6]),
        key_fingerprint=str(row[7]),
        discovery_hash=str(row[8]) if row[8] is not None else None,
        status=str(row[9]),
        reviewer_identity=str(row[10]) if row[10] is not None else None,
        nomos_decision_id=str(row[11]) if row[11] is not None else None,
        nomos_decision_deployment_name=str(row[12] or "default"),
        decision_evidence=_json_object(row[13]),
        created_at=row[14],
        updated_at=row[15],
        deployment_name=str(row[16] or "default"),
    )


def _pin_from_row(row: Any) -> StoredTrustPin:
    return StoredTrustPin(
        local_authority_id=str(row[0]),
        remote_authority_id=str(row[1]),
        key_fingerprint=str(row[2]),
        allowed_actions=tuple(row[3] or ()),
        allowed_boards=tuple(row[4] or ()),
        allowed_pull_surfaces=tuple(row[5] or ()),
        visibility_tiers=tuple(row[6] or ()),
        status=str(row[7]),
        nomos_decision_id=str(row[8]) if row[8] is not None else None,
        nomos_decision_deployment_name=str(row[9] or "default"),
        nomos_effect=str(row[10]),
        decision_evidence=_json_object(row[11]),
        deployment_name=str(row[12] or "default"),
    )


def _route_from_row(row: Any) -> StoredRouteEvent:
    return StoredRouteEvent(
        route_event_id=str(row[0]),
        local_authority_id=str(row[1]),
        remote_authority_id=str(row[2]),
        subject=str(row[3]),
        action=str(row[4]),
        target_address=str(row[5]),
        visibility=str(row[6]),
        decision=str(row[7]),
        reason_code=str(row[8]),
        nomos_decision_id=str(row[9]) if row[9] is not None else None,
        nomos_decision_deployment_name=str(row[10] or "default"),
        trust_pin_fingerprint=str(row[11]) if row[11] is not None else None,
        live_row_delta=_json_object(row[12]),
        created_at=row[13],
        deployment_name=str(row[14] or "default"),
    )


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    raise FederationValidationError("expected JSON object from registry")


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()
