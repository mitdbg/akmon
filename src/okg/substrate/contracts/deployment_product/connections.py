"""Governed provider-connection operations and external readiness receipts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, TypeVar

from pydantic import field_validator, model_validator

from .models import (
    ConnectionBindingDefinition,
    GovernedToolBindingDefinition,
    OwnerQualifiedReference,
    ProductModel,
    ProviderConnectionDefinition,
    SourceBindingDefinition,
)


_T = TypeVar("_T")
_OBSERVATIONAL_PHASES = frozenset(
    {"metadata-plan", "discovery", "setup-proposal", "preflight", "health"}
)
_MAX_RECEIPT_LIFETIME = timedelta(minutes=15)


class ConnectionContractError(ValueError):
    """Stable fail-closed refusal raised before a provider callback is invoked."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class ConnectionOperation(ProductModel):
    """One declared provider operation evaluated at the connection boundary."""

    operation_id: str
    phase: Literal[
        "metadata-plan",
        "discovery",
        "setup-proposal",
        "preflight",
        "health",
        "apply",
    ]
    effect: Literal["observe", "write"]
    required_capabilities: tuple[str, ...] = ()

    @field_validator("operation_id", "required_capabilities")
    @classmethod
    def _nonempty(cls, value: Any) -> Any:
        values = value if isinstance(value, tuple) else (value,)
        if any(not isinstance(item, str) or not item.strip() for item in values):
            raise ValueError("operation identifiers and capabilities must be non-empty")
        if isinstance(value, tuple):
            checked = tuple(sorted(values))
            if len(checked) != len(set(checked)):
                raise ValueError("operation capabilities must be unique")
            return checked
        return value


class ConnectionReadinessReceipt(ProductModel):
    """Short-lived apply-time result kept outside product identity and lock bytes."""

    receipt_id: str
    connection_ref: OwnerQualifiedReference
    contract_digest: str
    capability_digest: str
    status: Literal["ready", "not-ready"]
    issued_at: datetime
    expires_at: datetime

    @field_validator("receipt_id", "contract_digest", "capability_digest")
    @classmethod
    def _sha256_digest(cls, value: str) -> str:
        if len(value) != 71 or not value.startswith("sha256:"):
            raise ValueError("receipt digests must use sha256:<64 lowercase hex>")
        try:
            int(value[7:], 16)
        except ValueError as exc:
            raise ValueError("receipt digests must use sha256:<64 lowercase hex>") from exc
        if value != value.lower():
            raise ValueError("receipt digests must use lowercase hexadecimal")
        return value

    @model_validator(mode="after")
    def _short_lived_and_typed(self) -> "ConnectionReadinessReceipt":
        if self.connection_ref.kind != "provider-connection":
            raise ValueError("readiness receipt must name a provider-connection")
        issued = _utc(self.issued_at)
        expires = _utc(self.expires_at)
        if expires <= issued:
            raise ValueError("readiness receipt expiry must be after issue time")
        if expires - issued > _MAX_RECEIPT_LIFETIME:
            raise ValueError("readiness receipt lifetime must not exceed 15 minutes")
        return self


def connection_contract_digest(connection: ProviderConnectionDefinition) -> str:
    """Digest the stable non-secret connection contract, excluding capabilities."""

    payload = connection.model_dump(
        mode="json",
        by_alias=True,
        exclude={"capabilities"},
    )
    return _digest(payload)


def connection_capability_digest(connection: ProviderConnectionDefinition) -> str:
    """Digest the declared provider capability set independently."""

    return _digest({"capabilities": sorted(connection.capabilities)})


def issue_connection_readiness_receipt(
    connection: ProviderConnectionDefinition,
    binding: ConnectionBindingDefinition
    | SourceBindingDefinition
    | GovernedToolBindingDefinition,
    operation: ConnectionOperation,
    provider_check: Callable[[], bool],
    *,
    issued_at: datetime,
    expires_at: datetime,
) -> ConnectionReadinessReceipt:
    """Run governed observational readiness, then issue a secret-free receipt."""

    if operation.phase not in {"preflight", "health"} or operation.effect != "observe":
        raise ConnectionContractError(
            "connection_readiness_operation_invalid",
            "readiness receipts require an observational preflight or health operation",
        )
    observed = execute_connection_operation(
        connection,
        binding,
        operation,
        provider_check,
    )
    if type(observed) is not bool:
        raise ConnectionContractError(
            "connection_readiness_result_invalid",
            "provider readiness checks must return a boolean",
        )
    status: Literal["ready", "not-ready"] = "ready" if observed else "not-ready"

    contract_digest = connection_contract_digest(connection)
    capability_digest = connection_capability_digest(connection)
    issued = _utc(issued_at)
    expires = _utc(expires_at)
    receipt_id = _digest(
        {
            "connection_ref": connection.reference.model_dump(mode="json", by_alias=True),
            "contract_digest": contract_digest,
            "capability_digest": capability_digest,
            "status": status,
            "issued_at": issued.isoformat(),
            "expires_at": expires.isoformat(),
        }
    )
    return ConnectionReadinessReceipt(
        receipt_id=receipt_id,
        connection_ref=connection.reference,
        contract_digest=contract_digest,
        capability_digest=capability_digest,
        status=status,
        issued_at=issued,
        expires_at=expires,
    )


def verify_connection_readiness_receipt(
    connection: ProviderConnectionDefinition,
    receipt: ConnectionReadinessReceipt,
    *,
    now: datetime,
) -> None:
    """Refuse stale, non-ready, or contract-drifted external readiness."""

    if receipt.connection_ref != connection.reference:
        raise ConnectionContractError(
            "connection_receipt_reference_mismatch",
            "readiness receipt names a different provider connection",
        )
    if receipt.contract_digest != connection_contract_digest(connection):
        raise ConnectionContractError(
            "connection_receipt_contract_mismatch",
            "readiness receipt does not match the non-secret connection contract",
        )
    if receipt.capability_digest != connection_capability_digest(connection):
        raise ConnectionContractError(
            "connection_receipt_capability_mismatch",
            "readiness receipt does not match the connection capabilities",
        )
    if receipt.status != "ready":
        raise ConnectionContractError(
            "connection_not_ready",
            "provider connection did not pass readiness",
        )
    if _utc(now) < _utc(receipt.issued_at):
        raise ConnectionContractError(
            "connection_receipt_from_future",
            "provider connection readiness receipt was issued in the future",
        )
    if _utc(now) >= _utc(receipt.expires_at):
        raise ConnectionContractError(
            "connection_receipt_expired",
            "provider connection readiness receipt has expired",
        )


def execute_connection_operation(
    connection: ProviderConnectionDefinition,
    binding: ConnectionBindingDefinition
    | SourceBindingDefinition
    | GovernedToolBindingDefinition,
    operation: ConnectionOperation,
    provider_call: Callable[[], _T],
    *,
    readiness_receipt: ConnectionReadinessReceipt | None = None,
    now: datetime | None = None,
) -> _T:
    """Authorize and invoke one operation; all refusals precede provider effects."""

    if binding.connection_ref != connection.reference:
        raise ConnectionContractError(
            "connection_binding_mismatch",
            "binding does not reference the supplied provider connection",
        )
    if not binding.enabled:
        raise ConnectionContractError(
            "connection_binding_disabled",
            "disabled bindings cannot invoke provider operations",
        )
    declared = set(connection.capabilities)
    required = set(binding.required_capabilities) | set(operation.required_capabilities)
    missing = sorted(required - declared)
    if missing:
        raise ConnectionContractError(
            "connection_capability_undeclared",
            "provider connection does not declare required capabilities: "
            + ", ".join(missing),
        )
    if operation.phase in _OBSERVATIONAL_PHASES and operation.effect != "observe":
        raise ConnectionContractError(
            "provider_write_effect_refused",
            f"{operation.phase} permits observational provider effects only",
        )
    if operation.phase == "apply":
        if readiness_receipt is None or now is None:
            raise ConnectionContractError(
                "connection_readiness_required",
                "apply requires a current provider connection readiness receipt",
            )
        verify_connection_readiness_receipt(
            connection,
            readiness_receipt,
            now=now,
        )
    return provider_call()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("connection receipt timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _digest(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()
