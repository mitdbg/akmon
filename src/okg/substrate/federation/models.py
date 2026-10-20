"""Typed public discovery and trust-scope models for OKG federation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping
from urllib.parse import urlsplit

from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.hashing import canonical_json_sha256_b64url
from okg.substrate.exchange.readiness import (
    PorosReadinessError,
    public_key_fingerprint,
    validate_authority_id,
)
from okg.substrate.federation import validation


FEDERATION_DISCOVERY_VERSION = 1
VALID_VISIBILITY_TIERS = frozenset({"public", "internal", "restricted", "private"})
VALID_MAILBOX_PROVIDERS = frozenset({
    "generic_queue_object",
    "aws_sqs_s3",
    "cloudflare_queues_r2",
    "local_file",
})
VALID_MAILBOX_ACK_MODES = frozenset({"after_durable_receipt", "manual"})

_SECRET_KEYS = frozenset({
    "api_key",
    "api_token",
    "cookie",
    "cookies",
    "credential",
    "credentials",
    "database_dsn",
    "dsn",
    "password",
    "private_key",
    "private-key",
    "private_key_path",
    "secret",
    "secret_ref",
    "token",
})


class FederationValidationError(ValueError):
    """Raised when federation discovery or route metadata is invalid."""


@dataclass(frozen=True)
class KeyDescriptor:
    kid: str
    public_key: str
    use: str = "sign"
    alg: str = "Ed25519"
    status: str = "active"
    fingerprint: str | None = None

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "KeyDescriptor":
        kid = _required_str(raw, "kid")
        public_key = _required_str(raw, "public_key")
        fingerprint = raw.get("fingerprint")
        if fingerprint is not None and not isinstance(fingerprint, str):
            raise FederationValidationError("key fingerprint must be a string")
        computed = _fingerprint(public_key)
        if fingerprint is not None and fingerprint != computed:
            raise FederationValidationError(
                "key fingerprint does not match public key"
            )
        return cls(
            kid=kid,
            public_key=public_key,
            use=str(raw.get("use") or "sign"),
            alg=str(raw.get("alg") or "Ed25519"),
            status=str(raw.get("status") or "active"),
            fingerprint=computed,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "kid": self.kid,
            "use": self.use,
            "alg": self.alg,
            "public_key": self.public_key,
            "fingerprint": self.fingerprint or _fingerprint(self.public_key),
            "status": self.status,
        }


@dataclass(frozen=True)
class BoardAddress:
    address: str
    visibility: str = "public"
    message_profiles: tuple[str, ...] = ("proposal", "context")
    redaction_policy_ref: str | None = None

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "BoardAddress":
        address = _okg_uri(_required_str(raw, "address"), field="board.address")
        visibility = _visibility(str(raw.get("visibility") or "public"))
        profiles = _string_tuple(raw.get("message_profiles") or ["proposal", "context"])
        return cls(
            address=address,
            visibility=visibility,
            message_profiles=profiles,
            redaction_policy_ref=_optional_str(raw, "redaction_policy_ref"),
        )

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "address": self.address,
            "visibility": self.visibility,
            "message_profiles": list(self.message_profiles),
        }
        if self.redaction_policy_ref:
            out["redaction_policy_ref"] = self.redaction_policy_ref
        return out


@dataclass(frozen=True)
class PullSurface:
    address: str
    visibility: str = "public"
    graph_profile: str = "context"
    catalog_reference: str | None = None
    latest_generation_id: int | None = None
    max_packet_bytes: int | None = None
    redaction_policy_ref: str | None = None
    license: str | None = None

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "PullSurface":
        address = _okg_uri(_required_str(raw, "address"), field="pull.address")
        visibility = _visibility(str(raw.get("visibility") or "public"))
        latest_generation_id = raw.get("latest_generation_id")
        if latest_generation_id is not None and not isinstance(latest_generation_id, int):
            raise FederationValidationError(
                "pull surface latest_generation_id must be an integer"
            )
        max_packet_bytes = raw.get("max_packet_bytes")
        if max_packet_bytes is not None:
            if not isinstance(max_packet_bytes, int) or max_packet_bytes <= 0:
                raise FederationValidationError(
                    "pull surface max_packet_bytes must be a positive integer"
                )
        return cls(
            address=address,
            visibility=visibility,
            graph_profile=str(raw.get("graph_profile") or "context"),
            catalog_reference=_optional_str(raw, "catalog_reference"),
            latest_generation_id=latest_generation_id,
            max_packet_bytes=max_packet_bytes,
            redaction_policy_ref=_optional_str(raw, "redaction_policy_ref"),
            license=_optional_str(raw, "license"),
        )

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "address": self.address,
            "visibility": self.visibility,
            "graph_profile": self.graph_profile,
        }
        if self.catalog_reference is not None:
            out["catalog_reference"] = self.catalog_reference
        if self.latest_generation_id is not None:
            out["latest_generation_id"] = self.latest_generation_id
        if self.max_packet_bytes is not None:
            out["max_packet_bytes"] = self.max_packet_bytes
        if self.redaction_policy_ref is not None:
            out["redaction_policy_ref"] = self.redaction_policy_ref
        if self.license is not None:
            out["license"] = self.license
        return out


@dataclass(frozen=True)
class MailboxLocator:
    address: str
    queue_ref: str
    provider: str = "generic_queue_object"
    object_ref: str | None = None
    max_inline_bytes: int = 100_000
    max_object_bytes: int = 1_000_000_000
    retention_seconds: int | None = None
    ack_mode: str = "after_durable_receipt"
    encryption_required: bool = True

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "MailboxLocator":
        address = _okg_uri(_required_str(raw, "address"), field="mailbox.address")
        provider = str(raw.get("provider") or "generic_queue_object")
        if provider not in VALID_MAILBOX_PROVIDERS:
            raise FederationValidationError(f"unknown mailbox provider: {provider}")
        ack_mode = str(raw.get("ack_mode") or "after_durable_receipt")
        if ack_mode not in VALID_MAILBOX_ACK_MODES:
            raise FederationValidationError(f"unknown mailbox ack mode: {ack_mode}")
        max_inline_bytes = _positive_int(
            raw.get("max_inline_bytes") or 100_000,
            "mailbox max_inline_bytes",
        )
        max_object_bytes = _positive_int(
            raw.get("max_object_bytes") or 1_000_000_000,
            "mailbox max_object_bytes",
        )
        if max_inline_bytes > max_object_bytes:
            raise FederationValidationError(
                "mailbox max_inline_bytes must not exceed max_object_bytes"
            )
        retention_seconds = raw.get("retention_seconds")
        if retention_seconds is not None:
            retention_seconds = _positive_int(
                retention_seconds,
                "mailbox retention_seconds",
            )
        encryption_required = raw.get("encryption_required", True)
        if not isinstance(encryption_required, bool):
            raise FederationValidationError(
                "mailbox encryption_required must be a boolean"
            )
        return cls(
            address=address,
            provider=provider,
            queue_ref=_public_locator(_required_str(raw, "queue_ref"), "queue_ref"),
            object_ref=_optional_public_locator(raw, "object_ref"),
            max_inline_bytes=max_inline_bytes,
            max_object_bytes=max_object_bytes,
            retention_seconds=retention_seconds,
            ack_mode=ack_mode,
            encryption_required=encryption_required,
        )

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "address": self.address,
            "provider": self.provider,
            "queue_ref": self.queue_ref,
            "max_inline_bytes": self.max_inline_bytes,
            "max_object_bytes": self.max_object_bytes,
            "ack_mode": self.ack_mode,
            "encryption_required": self.encryption_required,
        }
        if self.object_ref is not None:
            out["object_ref"] = self.object_ref
        if self.retention_seconds is not None:
            out["retention_seconds"] = self.retention_seconds
        return out


@dataclass(frozen=True)
class DiscoveryDocument:
    authority_id: str
    keys: tuple[KeyDescriptor, ...]
    display_label: str | None = None
    capabilities: tuple[str, ...] = (
        "federation.discovery",
        "federation.message.send",
        "federation.graph.pull",
    )
    boards: tuple[BoardAddress, ...] = ()
    pull_surfaces: tuple[PullSurface, ...] = ()
    mailboxes: tuple[MailboxLocator, ...] = ()
    endpoints: Mapping[str, str] = field(default_factory=dict)
    object_refs: Mapping[str, str] = field(default_factory=dict)
    visibility_tiers: tuple[str, ...] = ("public",)
    latest_public_generation_id: int | None = None
    catalog_reference: str | None = None
    discovery_version: int = FEDERATION_DISCOVERY_VERSION

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "DiscoveryDocument":
        _reject_public_secrets(raw)
        authority_id = _required_str(raw, "authority_id")
        _validate_authority(authority_id)
        keys_raw = raw.get("keys")
        if not isinstance(keys_raw, list) or not keys_raw:
            raise FederationValidationError("discovery requires at least one key")
        keys = tuple(KeyDescriptor.from_mapping(_mapping(item, "keys[]")) for item in keys_raw)
        boards = tuple(
            BoardAddress.from_mapping(_mapping(item, "boards[]"))
            for item in raw.get("boards") or []
        )
        pull_surfaces = tuple(
            PullSurface.from_mapping(_mapping(item, "pull_surfaces[]"))
            for item in raw.get("pull_surfaces") or []
        )
        mailboxes = tuple(
            MailboxLocator.from_mapping(_mapping(item, "mailboxes[]"))
            for item in raw.get("mailboxes") or []
        )
        _assert_unique("board address", [board.address for board in boards])
        _assert_unique("pull surface address", [surface.address for surface in pull_surfaces])
        _assert_unique("mailbox address", [mailbox.address for mailbox in mailboxes])
        endpoints = _string_mapping(raw.get("endpoints") or {}, "endpoints")
        object_refs = _string_mapping(raw.get("object_refs") or {}, "object_refs")
        visibility_tiers = tuple(
            _visibility(item)
            for item in _string_tuple(raw.get("visibility_tiers") or ["public"])
        )
        latest_public_generation_id = raw.get("latest_public_generation_id")
        if (
            latest_public_generation_id is not None
            and not isinstance(latest_public_generation_id, int)
        ):
            raise FederationValidationError(
                "latest_public_generation_id must be an integer"
            )
        return cls(
            authority_id=authority_id,
            display_label=_optional_str(raw, "display_label"),
            keys=keys,
            capabilities=_string_tuple(
                raw.get("capabilities")
                or [
                    "federation.discovery",
                    "federation.message.send",
                    "federation.graph.pull",
                ]
            ),
            boards=boards,
            pull_surfaces=pull_surfaces,
            mailboxes=mailboxes,
            endpoints=endpoints,
            object_refs=object_refs,
            visibility_tiers=visibility_tiers,
            latest_public_generation_id=latest_public_generation_id,
            catalog_reference=_optional_str(raw, "catalog_reference"),
            discovery_version=int(raw.get("discovery_version") or FEDERATION_DISCOVERY_VERSION),
        )

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        out: dict[str, Any] = {
            "discovery_version": self.discovery_version,
            "authority_id": self.authority_id,
            "keys": [key.to_dict() for key in self.keys],
            "capabilities": list(self.capabilities),
            "boards": [board.to_dict() for board in self.boards],
            "pull_surfaces": [surface.to_dict() for surface in self.pull_surfaces],
            "mailboxes": [mailbox.to_dict() for mailbox in self.mailboxes],
            "endpoints": dict(self.endpoints),
            "object_refs": dict(self.object_refs),
            "visibility_tiers": list(self.visibility_tiers),
        }
        if self.display_label is not None:
            out["display_label"] = self.display_label
        if self.latest_public_generation_id is not None:
            out["latest_public_generation_id"] = self.latest_public_generation_id
        if self.catalog_reference is not None:
            out["catalog_reference"] = self.catalog_reference
        if include_hash:
            out["discovery_hash"] = canonical_discovery_hash(out)
        return out

    @property
    def discovery_hash(self) -> str:
        return canonical_discovery_hash(self.to_dict(include_hash=False))


def canonical_discovery_hash(value: Mapping[str, Any]) -> str:
    """Return the stable public hash for a discovery document."""
    payload = dict(value)
    payload.pop("discovery_hash", None)
    payload.pop("signature", None)
    canonical_json_bytes(payload)
    return "sha256:" + canonical_json_sha256_b64url(payload)


def _fingerprint(public_key: str) -> str:
    try:
        return public_key_fingerprint(public_key)
    except PorosReadinessError as exc:
        raise FederationValidationError(str(exc)) from exc


def _validate_authority(value: str) -> None:
    try:
        validate_authority_id(value)
    except PorosReadinessError as exc:
        raise FederationValidationError(str(exc)) from exc


def _required_str(raw: Mapping[str, Any], key: str) -> str:
    return validation.required_str(raw, key, exc_type=FederationValidationError)


def _optional_str(raw: Mapping[str, Any], key: str) -> str | None:
    return validation.optional_str(raw, key, exc_type=FederationValidationError)


def _positive_int(value: Any, field: str) -> int:
    return validation.positive_int(value, field, exc_type=FederationValidationError)


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    return validation.required_mapping(value, field, exc_type=FederationValidationError)


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise FederationValidationError("expected a non-empty string list")
    out: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item:
            raise FederationValidationError("expected a non-empty string list")
        out.append(item)
    return tuple(out)


def _string_mapping(value: Any, field: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise FederationValidationError(f"{field} must be an object")
    out: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str):
            raise FederationValidationError(f"{field} must map strings to strings")
        out[key] = item
    return out


def _visibility(value: str) -> str:
    if value not in VALID_VISIBILITY_TIERS:
        raise FederationValidationError(f"unknown visibility tier: {value}")
    return value


def _okg_uri(value: str, *, field: str) -> str:
    if not value.startswith("okg://") or len(value) <= len("okg://"):
        raise FederationValidationError(f"{field} must be an okg:// address")
    if any(ch.isspace() for ch in value):
        raise FederationValidationError(f"{field} must not contain whitespace")
    return value


def _optional_public_locator(raw: Mapping[str, Any], key: str) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise FederationValidationError(f"{key} must be a non-empty string")
    return _public_locator(value, key)


def _public_locator(value: str, field: str) -> str:
    lowered = value.lower()
    forbidden_markers = (
        "password=",
        "secret=",
        "signature=",
        "sig=",
        "token=",
        "x-amz-signature=",
        "awsaccesskeyid=",
    )
    if any(marker in lowered for marker in forbidden_markers):
        raise FederationValidationError(
            f"{field} must not contain embedded credentials"
        )
    parsed = urlsplit(value)
    if parsed.username or parsed.password:
        raise FederationValidationError(
            f"{field} must not contain embedded credentials"
        )
    if any(ch.isspace() for ch in value):
        raise FederationValidationError(f"{field} must not contain whitespace")
    return value


def _assert_unique(label: str, values: list[str]) -> None:
    seen: set[str] = set()
    for value in values:
        if value in seen:
            raise FederationValidationError(f"duplicate {label}: {value}")
        seen.add(value)


def _reject_public_secrets(value: Any, *, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            lowered = str(key).lower()
            if (
                lowered in _SECRET_KEYS
                or lowered.endswith("_dsn")
                or lowered.endswith("_token")
                or lowered.endswith("_password")
            ):
                raise FederationValidationError(
                    f"public discovery contains secret field at {path}.{key}"
                )
            _reject_public_secrets(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_public_secrets(child, path=f"{path}[{index}]")
