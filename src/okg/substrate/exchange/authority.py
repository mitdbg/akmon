"""Authority discovery documents and local trust pins for Poros."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from okg.substrate.exchange.constants import (
    GRAPH_SUBMIT_TYPE,
    POROS_VERSION,
    SIGNATURE_ALG,
)
from okg.substrate.exchange.errors import PorosTrustError


@dataclass(frozen=True)
class KeyDescriptor:
    kid: str
    use: str
    alg: str
    public_key: str
    status: str = "active"
    created_at: str | None = None
    retired_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "kid": self.kid,
            "use": self.use,
            "alg": self.alg,
            "public_key": self.public_key,
            "status": self.status,
        }
        if self.created_at is not None:
            out["created_at"] = self.created_at
        if self.retired_at is not None:
            out["retired_at"] = self.retired_at
        return out


@dataclass(frozen=True)
class AuthorityDocument:
    authority_id: str
    endpoints: dict[str, str]
    keys: tuple[KeyDescriptor, ...]
    display_name: str | None = None
    capabilities: tuple[str, ...] = (GRAPH_SUBMIT_TYPE, "receipt.signed")
    protocol_versions: tuple[str, ...] = (POROS_VERSION,)
    latest_public_generation: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "poros": POROS_VERSION,
            "authority_id": self.authority_id,
            "endpoints": dict(self.endpoints),
            "protocol_versions": list(self.protocol_versions),
            "capabilities": list(self.capabilities),
            "keys": [key.to_dict() for key in self.keys],
        }
        if self.display_name is not None:
            out["display_name"] = self.display_name
        if self.latest_public_generation is not None:
            out["latest_public_generation"] = dict(self.latest_public_generation)
        return out


@dataclass(frozen=True)
class TrustPin:
    authority_id: str
    kid: str
    public_key: str
    use: str = "sign"
    alg: str = SIGNATURE_ALG

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "TrustPin":
        return cls(
            authority_id=str(value["authority_id"]),
            kid=str(value["kid"]),
            public_key=str(value["public_key"]),
            use=str(value.get("use", "sign")),
            alg=str(value.get("alg", SIGNATURE_ALG)),
        )


@dataclass
class TrustPinRegistry:
    pins: dict[tuple[str, str, str], TrustPin] = field(default_factory=dict)

    @classmethod
    def from_pins(cls, pins: list[TrustPin]) -> "TrustPinRegistry":
        registry = cls()
        for pin in pins:
            registry.add_pin(pin)
        return registry

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | list[dict[str, Any]]) -> "TrustPinRegistry":
        records = value.get("pins", []) if isinstance(value, dict) else value
        return cls.from_pins([TrustPin.from_mapping(record) for record in records])

    def add_pin(self, pin: TrustPin) -> None:
        self.pins[(pin.authority_id, pin.kid, pin.use)] = pin

    def resolve_public_key(
        self,
        *,
        authority_id: str,
        kid: str,
        use: str = "sign",
        alg: str = SIGNATURE_ALG,
    ) -> str:
        if not any(
            key_authority == authority_id and key_use == use
            for key_authority, _, key_use in self.pins
        ):
            raise PorosTrustError(
                "sender authority has no local Poros trust pin",
                reason_code="missing_trust_pin",
                data={"authority_id": authority_id, "use": use},
            )
        pin = self.pins.get((authority_id, kid, use))
        if pin is None:
            raise PorosTrustError(
                "sender signing key id is not pinned",
                reason_code="signing_key_not_pinned",
                data={"authority_id": authority_id, "kid": kid, "use": use},
            )
        if pin.alg != alg:
            raise PorosTrustError(
                "pinned key algorithm does not match requested algorithm",
                reason_code="trust_pin_algorithm_mismatch",
                data={"authority_id": authority_id, "kid": kid},
            )
        return pin.public_key

    def check_discovery_document(self, document: AuthorityDocument | dict[str, Any]) -> None:
        doc = document.to_dict() if isinstance(document, AuthorityDocument) else document
        authority_id = str(doc.get("authority_id", ""))
        for key in doc.get("keys", []):
            lookup = (authority_id, str(key.get("kid")), str(key.get("use", "sign")))
            pin = self.pins.get(lookup)
            if pin is not None and pin.public_key != key.get("public_key"):
                raise PorosTrustError(
                    "discovery key material differs from local trust pin",
                    reason_code="key_material_mismatch",
                    data={
                        "authority_id": authority_id,
                        "kid": key.get("kid"),
                        "use": key.get("use", "sign"),
                    },
                )

    def rotation_candidates(
        self,
        document: AuthorityDocument | dict[str, Any],
    ) -> list[dict[str, Any]]:
        doc = document.to_dict() if isinstance(document, AuthorityDocument) else document
        authority_id = str(doc.get("authority_id", ""))
        pending: list[dict[str, Any]] = []
        for key in doc.get("keys", []):
            lookup = (authority_id, str(key.get("kid")), str(key.get("use", "sign")))
            pin = self.pins.get(lookup)
            if pin is None:
                pending.append(
                    {
                        "status": "pending_rotation",
                        "authority_id": authority_id,
                        "kid": key.get("kid"),
                        "use": key.get("use", "sign"),
                        "alg": key.get("alg"),
                    }
                )
        return pending


def build_authority_document(
    *,
    authority_id: str,
    base_url: str,
    signing_keys: list[KeyDescriptor],
    encryption_keys: list[KeyDescriptor] | None = None,
    display_name: str | None = None,
    capabilities: list[str] | None = None,
    latest_public_generation: dict[str, Any] | None = None,
) -> AuthorityDocument:
    base = base_url.rstrip("/")
    keys = tuple(signing_keys + list(encryption_keys or []))
    return AuthorityDocument(
        authority_id=authority_id,
        display_name=display_name,
        endpoints={"inbox": f"{base}/poros/v1/inbox"},
        capabilities=tuple(capabilities or [GRAPH_SUBMIT_TYPE, "receipt.signed"]),
        keys=keys,
        latest_public_generation=latest_public_generation,
    )
