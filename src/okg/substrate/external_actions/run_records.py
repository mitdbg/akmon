"""Authenticated registration facts, independent of worker execution."""

from __future__ import annotations

import re
from typing import Any, Mapping

from .models import Actor
from .signing import domain_digest


def _invalid() -> None:
    from .service import ExternalActionError

    raise ExternalActionError("invalid_run_record", "invalid run record", status=422)


def registration_body(value: Any) -> dict[str, Any]:
    """Normalize only the closed caller-reported registration grammar."""
    fields = {
        "run_id",
        "automation_id",
        "automation_version",
        "correlation_id",
        "reported_distribution",
        "generation_id",
    }
    if not isinstance(value, Mapping) or set(value) != fields:
        _invalid()
    for name in ("run_id", "automation_id", "automation_version", "correlation_id"):
        limit = 64 if name == "automation_version" else 128
        if not isinstance(value[name], str) or not re.fullmatch(
            rf"[A-Za-z0-9._:+\-]{{1,{limit}}}", value[name]
        ):
            _invalid()
    distribution = value["reported_distribution"]
    if not isinstance(distribution, Mapping) or set(distribution) != {
        "name",
        "version",
        "package_digest",
    }:
        _invalid()
    for name in ("name", "version"):
        if not isinstance(distribution[name], str) or not re.fullmatch(
            r"[A-Za-z0-9._:+\-]{1,128}", distribution[name]
        ):
            _invalid()
    digest = distribution["package_digest"]
    if digest is not None and (
        not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
    ):
        _invalid()
    generation = value["generation_id"]
    if (
        not isinstance(generation, str)
        or not generation
        or len(generation.encode("utf-8")) > 1024
        or any(ord(c) < 32 or ord(c) == 127 for c in generation)
    ):
        _invalid()
    return {**value, "reported_distribution": dict(distribution)}


def authorization_digest(
    *,
    transaction_id: str,
    deployment: str,
    owner: str,
    record_id: str,
    proposal_id: str | None,
    request_hash: str,
    operation: str,
) -> tuple[str, dict[str, str]]:
    """Bind each permission to its own resource and this database transaction."""
    kind = (
        "external_action"
        if operation == "external_action.read"
        else "external_automation_run"
    )
    resource_id = proposal_id if kind == "external_action" else record_id
    target = {"kind": kind, "id": resource_id or "", "operation": operation}
    digest = domain_digest(
        "okg-external-automation-authorization-v1",
        {
            "transaction_id": transaction_id,
            "deployment": deployment,
            "owner": owner,
            "record_id": record_id,
            "proposal_id": proposal_id,
            "request_hash": request_hash,
            "operation": operation,
            "target": target,
        },
    )
    return digest, target


class RunRecords:
    """Use the review service's complete verifier/decision/audit cycles."""

    def __init__(self, service: Any):
        self.service = service
        self.store = service.store

    def execute(
        self,
        *,
        operation: str,
        deployment: str,
        actor: Actor,
        body: Any = None,
        record_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> Mapping[str, Any]:
        from .service import ExternalActionError

        if not self.service._is_durable():
            raise ExternalActionError(
                "review_authority_unavailable",
                "durable run storage required",
                status=503,
            )
        if operation == "register":
            payload = registration_body(body)
        elif operation == "link":
            if not isinstance(body, Mapping) or set(body) != {"proposal_id"}:
                _invalid()
            if not isinstance(body["proposal_id"], str) or not re.fullmatch(
                r"[0-9A-HJKMNP-TV-Z]{26}", body["proposal_id"]
            ):
                _invalid()
            payload = dict(body)
        elif operation == "read" and body is None:
            payload = {}
        else:
            _invalid()
        if operation != "register" and (
            not isinstance(record_id, str)
            or not re.fullmatch(r"[0-9A-HJKMNP-TV-Z]{26}", record_id)
        ):
            _invalid()
        if operation != "read":
            from .api import validate_idempotency_key

            validate_idempotency_key(idempotency_key)
        key_hash = (
            self.service._idempotency_hash(idempotency_key) if idempotency_key else None
        )
        request_hash = domain_digest(
            "okg-external-automation-request-v1",
            {
                "operation": operation,
                "deployment": deployment,
                "owner": actor.separation_subject,
                "record_id": record_id,
                "body": payload,
            },
        )
        # A generated candidate id is not part of registration idempotency.
        selected_id = record_id or self.service.id_factory()
        with self.store.lock:
            tx = self.store.run_transaction_context()
            command = {
                "deployment": deployment,
                "owner": actor.separation_subject,
                "record_id": selected_id,
                "request_hash": request_hash,
                "idempotency_key_hash": key_hash,
                "body": payload,
            }
            operations = [f"external_automation.{operation}"]
            if operation == "link":
                operations.append("external_action.read")
            for index, permission in enumerate(operations):
                digest, target = authorization_digest(
                    transaction_id=tx,
                    deployment=deployment,
                    owner=actor.separation_subject,
                    record_id=selected_id,
                    proposal_id=payload.get("proposal_id"),
                    request_hash=request_hash,
                    operation=permission,
                )
                # Always start from the original unbound verified actor. Each
                # completed cycle consumes its own pending verifier assertion.
                verified = self.service._authorize(
                    permission,
                    actor,
                    context={
                        "deployment": deployment,
                        "request_hash": digest,
                        "run_authorization_target": target,
                        "surface": permission,
                    },
                )
                prefix = "primary" if index == 0 else "proposal_read"
                command[f"{prefix}_authentication_audit_id"] = (
                    verified.authentication_audit_id
                )
                command[f"{prefix}_nomos_decision_id"] = verified.nomos_decision_id
            return self.store.run_record_command(operation, command)
