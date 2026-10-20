"""Poros deployment-readiness manifest and peer registry helpers."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from okg.substrate.exchange.authority import TrustPin
from okg.substrate.exchange.base64url import b64url_decode
from okg.substrate.exchange.constants import SIGNATURE_ALG
from okg.substrate.exchange.errors import PorosError


MANIFEST_VERSION = 1
DEFAULT_MANIFEST_PATH = "poros.manifest.json"
VALID_ROLES = {"sender", "receiver"}
VALID_PEER_STATUS = {"active", "disabled"}
VALID_KEY_STATUS = {"active", "retired", "disabled"}
VALID_REDACTION_MODES = {"public", "audit"}
VALID_ASSURANCE_LEVELS = {"none", "local-demo", "pilot", "production"}
VALID_OBJECT_BACKENDS = {"local"}
REQUIRED_FAILURE_FIXTURES = (
    "replay",
    "untrusted_or_disabled_peer",
    "tampered_payload_or_object",
    "deny",
    "review",
    "quarantine",
    "malformed",
    "transport_failed",
)
_TRANSFER_PROOF_MAX_AGE_SECONDS = {
    "local-demo": 24 * 60 * 60,
    "pilot": 6 * 60 * 60,
    "production": 60 * 60,
}
_FAILURE_FIXTURE_EXPECTATIONS = {
    "replay": {
        "states": {"duplicate"},
        "event_keys": {"duplicate"},
    },
    "untrusted_or_disabled_peer": {
        "states": {"rejected_preflight", "preflight-rejected", "rejected"},
        "event_keys": {"preflight_rejected"},
    },
    "tampered_payload_or_object": {
        "states": {"rejected_preflight", "preflight-rejected", "rejected", "tampered"},
        "event_keys": {"preflight_rejected"},
    },
    "deny": {
        "states": {"deny", "denied"},
        "event_keys": {"nomos_decided"},
    },
    "review": {
        "states": {"review"},
        "event_keys": {"review"},
    },
    "quarantine": {
        "states": {"quarantine", "quarantined"},
        "event_keys": {"quarantined"},
    },
    "malformed": {
        "states": {"malformed", "rejected_preflight", "preflight-rejected"},
        "event_keys": {"preflight_rejected"},
    },
    "transport_failed": {
        "states": {"transport_failed"},
        "event_keys": {"transport_failed"},
    },
}

_AUTHORITY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:[A-Za-z0-9][A-Za-z0-9_.:/@+-]*$")


class PorosReadinessError(ValueError):
    """Structured failure for Poros manifest/readiness operations."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.data = data or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "error",
            "error": str(self),
            "reason_code": self.reason_code,
            "data": self.data,
        }


@dataclass
class PorosKeyRef:
    kid: str
    public_key: str
    status: str = "active"
    private_key_path: str | None = None
    fingerprint: str | None = None
    created_at: str | None = None
    retired_at: str | None = None
    disabled_at: str | None = None
    replaced_by: str | None = None
    rotation_evidence: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "PorosKeyRef":
        if not isinstance(value, dict):
            raise PorosReadinessError(
                "Poros key reference must be an object",
                reason_code="invalid_key_reference",
            )
        kid = _required_string(value, "kid", reason_code="missing_key_id")
        public_key = _required_string(
            value,
            "public_key",
            reason_code="missing_public_key",
        )
        status = str(value.get("status") or "active")
        _validate_key_status(status)
        fingerprint = str(
            value.get("fingerprint") or public_key_fingerprint(public_key)
        )
        expected = public_key_fingerprint(public_key)
        if fingerprint != expected:
            raise PorosReadinessError(
                "Poros key fingerprint does not match public key",
                reason_code="key_fingerprint_mismatch",
                data={"kid": kid, "expected": expected, "actual": fingerprint},
            )
        return cls(
            kid=kid,
            public_key=public_key,
            status=status,
            private_key_path=_optional_string(value, "private_key_path"),
            fingerprint=fingerprint,
            created_at=_optional_string(value, "created_at"),
            retired_at=_optional_string(value, "retired_at"),
            disabled_at=_optional_string(value, "disabled_at"),
            replaced_by=_optional_string(value, "replaced_by"),
            rotation_evidence=(
                dict(value.get("rotation_evidence") or {})
                if isinstance(value.get("rotation_evidence") or {}, dict)
                else {}
            ),
        )

    def to_dict(self, *, public: bool = False) -> dict[str, Any]:
        out: dict[str, Any] = {
            "kid": self.kid,
            "public_key": self.public_key,
            "fingerprint": self.fingerprint or public_key_fingerprint(self.public_key),
            "status": self.status,
        }
        if public:
            if self.private_key_path:
                out["private_key_path_redacted"] = True
        elif self.private_key_path:
            out["private_key_path"] = self.private_key_path
        if self.created_at:
            out["created_at"] = self.created_at
        if self.retired_at:
            out["retired_at"] = self.retired_at
        if self.disabled_at:
            out["disabled_at"] = self.disabled_at
        if self.replaced_by:
            out["replaced_by"] = self.replaced_by
        if self.rotation_evidence:
            out["rotation_evidence"] = dict(self.rotation_evidence)
        return out


@dataclass
class PorosPeer:
    authority_id: str
    status: str = "active"
    endpoint: str | None = None
    display_name: str | None = None
    keys: list[PorosKeyRef] = field(default_factory=list)
    created_at: str | None = None
    updated_at: str | None = None
    disabled_at: str | None = None

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "PorosPeer":
        if not isinstance(value, dict):
            raise PorosReadinessError(
                "Poros peer must be an object",
                reason_code="invalid_peer",
            )
        authority_id = _required_string(
            value,
            "authority_id",
            reason_code="missing_peer_authority",
        )
        validate_authority_id(authority_id)
        status = str(value.get("status") or "active")
        if status not in VALID_PEER_STATUS:
            raise PorosReadinessError(
                "Poros peer status is invalid",
                reason_code="invalid_peer_status",
                data={"authority_id": authority_id, "status": status},
            )
        keys_raw = value.get("keys") or []
        if not isinstance(keys_raw, list):
            raise PorosReadinessError(
                "Poros peer keys must be a list",
                reason_code="invalid_peer_keys",
                data={"authority_id": authority_id},
            )
        keys = [PorosKeyRef.from_mapping(item) for item in keys_raw]
        _assert_unique_key_ids(keys, authority_id=authority_id)
        return cls(
            authority_id=authority_id,
            status=status,
            endpoint=_optional_string(value, "endpoint"),
            display_name=_optional_string(value, "display_name"),
            keys=keys,
            created_at=_optional_string(value, "created_at"),
            updated_at=_optional_string(value, "updated_at"),
            disabled_at=_optional_string(value, "disabled_at"),
        )

    def active_keys(self) -> list[PorosKeyRef]:
        return [key for key in self.keys if key.status == "active"]

    def to_dict(self, *, public: bool = False) -> dict[str, Any]:
        out: dict[str, Any] = {
            "authority_id": self.authority_id,
            "status": self.status,
            "keys": [key.to_dict(public=public) for key in self.keys],
        }
        if self.endpoint:
            out["endpoint"] = self.endpoint
        if self.display_name:
            out["display_name"] = self.display_name
        if self.created_at:
            out["created_at"] = self.created_at
        if self.updated_at:
            out["updated_at"] = self.updated_at
        if self.disabled_at:
            out["disabled_at"] = self.disabled_at
        return out


@dataclass
class PorosManifest:
    authority_id: str
    roles: list[str]
    signing_keys: list[PorosKeyRef]
    peers: list[PorosPeer] = field(default_factory=list)
    receiver_endpoint: str | None = None
    object_store: dict[str, Any] = field(default_factory=dict)
    nomos: dict[str, Any] = field(default_factory=dict)
    redaction: dict[str, Any] = field(default_factory=dict)
    formal_assurance: dict[str, Any] = field(default_factory=dict)
    readiness_evidence: dict[str, Any] = field(default_factory=dict)
    allow_unsigned_compat_admission: bool = False
    created_at: str | None = None
    updated_at: str | None = None
    manifest_version: int = MANIFEST_VERSION

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "PorosManifest":
        if not isinstance(value, dict):
            raise PorosReadinessError(
                "Poros manifest must be a JSON object",
                reason_code="invalid_manifest",
            )
        version = int(value.get("poros_manifest_version") or 0)
        if version != MANIFEST_VERSION:
            raise PorosReadinessError(
                "Unsupported Poros manifest version",
                reason_code="unsupported_manifest_version",
                data={"version": version, "supported": MANIFEST_VERSION},
            )
        authority_id = _required_string(
            value,
            "authority_id",
            reason_code="missing_authority_id",
        )
        validate_authority_id(authority_id)
        roles_raw = value.get("roles")
        if not isinstance(roles_raw, list) or not roles_raw:
            raise PorosReadinessError(
                "Poros manifest must enable at least one role",
                reason_code="missing_enabled_role",
            )
        roles = [str(role) for role in roles_raw]
        invalid_roles = sorted(set(roles) - VALID_ROLES)
        if invalid_roles:
            raise PorosReadinessError(
                "Poros manifest has invalid roles",
                reason_code="invalid_role",
                data={"roles": invalid_roles},
            )
        signing_keys_raw = value.get("signing_keys")
        if not isinstance(signing_keys_raw, list) or not signing_keys_raw:
            raise PorosReadinessError(
                "Poros manifest must reference at least one signing key",
                reason_code="missing_key_reference",
            )
        signing_keys = [PorosKeyRef.from_mapping(item) for item in signing_keys_raw]
        _assert_unique_key_ids(signing_keys, authority_id=authority_id)
        peers_raw = value.get("peers") or []
        if not isinstance(peers_raw, list):
            raise PorosReadinessError(
                "Poros manifest peers must be a list",
                reason_code="invalid_peer_registry",
            )
        manifest = cls(
            authority_id=authority_id,
            roles=roles,
            signing_keys=signing_keys,
            peers=[PorosPeer.from_mapping(item) for item in peers_raw],
            receiver_endpoint=_optional_string(value, "receiver_endpoint"),
            object_store=_validated_object_store(value.get("object_store") or {}),
            nomos=_validated_nomos(value.get("nomos") or {}),
            redaction=_validated_redaction(value.get("redaction") or {}),
            formal_assurance=_validated_formal_assurance(
                value.get("formal_assurance") or {}
            ),
            readiness_evidence=(
                dict(value.get("readiness_evidence") or {})
                if isinstance(value.get("readiness_evidence") or {}, dict)
                else {}
            ),
            allow_unsigned_compat_admission=_parse_unsigned_compat_opt_in(
                value.get("admission")
            ),
            created_at=_optional_string(value, "created_at"),
            updated_at=_optional_string(value, "updated_at"),
            manifest_version=version,
        )
        _assert_unique_peer_authorities(manifest.peers)
        return manifest

    def active_signing_keys(self) -> list[PorosKeyRef]:
        return [key for key in self.signing_keys if key.status == "active"]

    def active_peers(self) -> list[PorosPeer]:
        return [peer for peer in self.peers if peer.status == "active"]

    def peer(self, authority_id: str) -> PorosPeer | None:
        return next(
            (peer for peer in self.peers if peer.authority_id == authority_id),
            None,
        )

    def to_dict(self, *, public: bool = False) -> dict[str, Any]:
        out: dict[str, Any] = {
            "poros_manifest_version": self.manifest_version,
            "authority_id": self.authority_id,
            "roles": list(self.roles),
            "signing_keys": [key.to_dict(public=public) for key in self.signing_keys],
            "peers": [peer.to_dict(public=public) for peer in self.peers],
            "object_store": dict(self.object_store),
            "nomos": dict(self.nomos),
            "redaction": dict(self.redaction),
            "formal_assurance": dict(self.formal_assurance),
            "readiness_evidence": dict(self.readiness_evidence),
        }
        if self.allow_unsigned_compat_admission:
            out["admission"] = {"allow_unsigned_compat": True}
        if self.receiver_endpoint:
            out["receiver_endpoint"] = self.receiver_endpoint
        if self.created_at:
            out["created_at"] = self.created_at
        if self.updated_at:
            out["updated_at"] = self.updated_at
        return out

    def active_trust_pins(self) -> list[TrustPin]:
        pins: list[TrustPin] = []
        for peer in self.active_peers():
            for key in peer.active_keys():
                pins.append(
                    TrustPin(
                        authority_id=peer.authority_id,
                        kid=key.kid,
                        public_key=key.public_key,
                        use="sign",
                        alg=SIGNATURE_ALG,
                    )
                )
        return pins


def _parse_unsigned_compat_opt_in(admission_block: Any) -> bool:
    """Resolve the compat-admission opt-in from a manifest ``admission`` block.

    Fail-closed (remediate-audit-2026-07 point (b)): only a literal boolean
    ``True`` enables compat admission. A missing block, a non-object block, a
    missing/absent flag, or a non-boolean value (e.g. the string ``"true"`` or
    the integer ``1``) all resolve to ``False`` — a missing positive
    attestation is a refusal, never a benign default.
    """
    if not isinstance(admission_block, dict):
        return False
    value = admission_block.get("allow_unsigned_compat")
    return value is True


def resolve_unsigned_compat_admission(manifest: "PorosManifest") -> bool:
    """Return whether the deployment manifest opts into compat admission.

    This is the single resolver the receiver consults before allowing an
    envelope whose producer output signature is empty (``status == "compat"``)
    to be admitted rather than rejected.
    """
    return bool(manifest.allow_unsigned_compat_admission)


def create_manifest(
    *,
    authority_id: str,
    roles: list[str],
    signing_key: PorosKeyRef,
    receiver_endpoint: str | None = None,
    object_store_root: str | None = None,
    nomos_bundle_path: str | None = None,
    formal_level: str = "none",
    redaction_mode: str = "public",
) -> PorosManifest:
    now = _now_iso()
    object_store = {
        "backend": "local",
        "root": object_store_root,
    }
    nomos: dict[str, Any] = {}
    if nomos_bundle_path:
        nomos["bundle_path"] = nomos_bundle_path
        path = Path(nomos_bundle_path).expanduser()
        if path.is_file():
            nomos["bundle_sha256"] = file_sha256(path)
    manifest = PorosManifest(
        authority_id=authority_id,
        roles=roles,
        signing_keys=[signing_key],
        peers=[],
        receiver_endpoint=receiver_endpoint,
        object_store=object_store,
        nomos=nomos,
        redaction={"default_mode": redaction_mode},
        formal_assurance={"level": formal_level},
        readiness_evidence={},
        created_at=now,
        updated_at=now,
    )
    return PorosManifest.from_mapping(manifest.to_dict())


def load_manifest(path: Path | str) -> PorosManifest:
    manifest_path = Path(path).expanduser()
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PorosReadinessError(
            "Poros manifest does not exist",
            reason_code="manifest_not_found",
            data={"path": str(manifest_path)},
        ) from exc
    except json.JSONDecodeError as exc:
        raise PorosReadinessError(
            "Poros manifest is not valid JSON",
            reason_code="invalid_manifest_json",
            data={"path": str(manifest_path)},
        ) from exc
    return PorosManifest.from_mapping(payload)


def save_manifest(
    manifest: PorosManifest,
    path: Path | str,
    *,
    overwrite: bool = True,
) -> None:
    manifest_path = Path(path).expanduser()
    if manifest_path.exists() and not overwrite:
        raise PorosReadinessError(
            "Poros manifest already exists",
            reason_code="manifest_exists",
            data={"path": str(manifest_path)},
        )
    manifest.updated_at = _now_iso()
    validated = PorosManifest.from_mapping(manifest.to_dict())
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(validated.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def add_peer(
    manifest: PorosManifest,
    *,
    authority_id: str,
    kid: str,
    public_key: str,
    endpoint: str | None = None,
    display_name: str | None = None,
    allow_replace: bool = False,
) -> PorosPeer:
    validate_authority_id(authority_id)
    key = PorosKeyRef(
        kid=kid,
        public_key=public_key,
        fingerprint=public_key_fingerprint(public_key),
        created_at=_now_iso(),
    )
    key = PorosKeyRef.from_mapping(key.to_dict())
    peer = manifest.peer(authority_id)
    now = _now_iso()
    if peer is None:
        peer = PorosPeer(
            authority_id=authority_id,
            status="active",
            endpoint=endpoint,
            display_name=display_name,
            keys=[key],
            created_at=now,
            updated_at=now,
        )
        manifest.peers.append(peer)
        return peer
    peer.status = "active"
    peer.disabled_at = None
    if endpoint is not None:
        peer.endpoint = endpoint
    if display_name is not None:
        peer.display_name = display_name
    existing = next((item for item in peer.keys if item.kid == kid), None)
    if existing is None:
        peer.keys.append(key)
    elif existing.public_key == public_key:
        existing.status = "active"
        existing.disabled_at = None
    elif not allow_replace:
        raise PorosReadinessError(
            "Peer key material changed; use rotate-key or --allow-replace",
            reason_code="peer_key_material_changed",
            data={
                "authority_id": authority_id,
                "kid": kid,
                "old_fingerprint": existing.fingerprint,
                "new_fingerprint": key.fingerprint,
            },
        )
    else:
        existing.public_key = key.public_key
        existing.fingerprint = key.fingerprint
        existing.status = "active"
    peer.updated_at = now
    return peer


def disable_peer(manifest: PorosManifest, *, authority_id: str) -> PorosPeer:
    peer = manifest.peer(authority_id)
    if peer is None:
        raise PorosReadinessError(
            "Poros peer is not registered",
            reason_code="peer_not_found",
            data={"authority_id": authority_id},
        )
    now = _now_iso()
    peer.status = "disabled"
    peer.disabled_at = now
    peer.updated_at = now
    return peer


def rotate_peer_key(
    manifest: PorosManifest,
    *,
    authority_id: str,
    old_kid: str,
    new_kid: str,
    public_key: str,
) -> PorosPeer:
    peer = manifest.peer(authority_id)
    if peer is None:
        raise PorosReadinessError(
            "Poros peer is not registered",
            reason_code="peer_not_found",
            data={"authority_id": authority_id},
        )
    old_key = next((key for key in peer.keys if key.kid == old_kid), None)
    if old_key is None:
        raise PorosReadinessError(
            "Old Poros peer key is not registered",
            reason_code="old_peer_key_not_found",
            data={"authority_id": authority_id, "old_kid": old_kid},
        )
    if old_key.status != "active":
        raise PorosReadinessError(
            "Old Poros peer key is not active",
            reason_code="old_peer_key_not_active",
            data={"authority_id": authority_id, "old_kid": old_kid},
        )
    new_fingerprint = public_key_fingerprint(public_key)
    if old_key.fingerprint == new_fingerprint:
        raise PorosReadinessError(
            "New Poros peer key has the same fingerprint as old key",
            reason_code="peer_key_rotation_noop",
            data={"authority_id": authority_id, "old_kid": old_kid},
        )
    now = _now_iso()
    old_key.status = "retired"
    old_key.retired_at = now
    old_key.replaced_by = new_kid
    new_key = next((key for key in peer.keys if key.kid == new_kid), None)
    evidence = {
        "rotated_at": now,
        "old_kid": old_kid,
        "old_fingerprint": old_key.fingerprint,
        "new_kid": new_kid,
        "new_fingerprint": new_fingerprint,
    }
    if new_key is None:
        peer.keys.append(
            PorosKeyRef(
                kid=new_kid,
                public_key=public_key,
                status="active",
                fingerprint=new_fingerprint,
                created_at=now,
                rotation_evidence=evidence,
            )
        )
    else:
        new_key.public_key = public_key
        new_key.status = "active"
        new_key.fingerprint = new_fingerprint
        new_key.rotation_evidence = evidence
    peer.status = "active"
    peer.disabled_at = None
    peer.updated_at = now
    return peer


def trust_pins_payload(manifest: PorosManifest) -> dict[str, Any]:
    return {
        "pins": [
            {
                "authority_id": pin.authority_id,
                "kid": pin.kid,
                "use": pin.use,
                "alg": pin.alg,
                "public_key": pin.public_key,
            }
            for pin in manifest.active_trust_pins()
        ]
    }


def require_sender_manifest(manifest: PorosManifest) -> None:
    if "sender" not in manifest.roles:
        raise PorosReadinessError(
            "Poros manifest does not enable sender role",
            reason_code="sender_role_not_enabled",
            data={"authority_id": manifest.authority_id, "roles": manifest.roles},
        )


def require_receiver_manifest(manifest: PorosManifest) -> None:
    if "receiver" not in manifest.roles:
        raise PorosReadinessError(
            "Poros manifest does not enable receiver role",
            reason_code="receiver_role_not_enabled",
            data={"authority_id": manifest.authority_id, "roles": manifest.roles},
        )


def require_single_active_signing_key(manifest: PorosManifest) -> PorosKeyRef:
    active_keys = manifest.active_signing_keys()
    if len(active_keys) != 1:
        raise PorosReadinessError(
            "Poros manifest must have exactly one active local signing key",
            reason_code="ambiguous_local_signing_key",
            data={
                "authority_id": manifest.authority_id,
                "active_key_count": len(active_keys),
            },
        )
    key = active_keys[0]
    if not key.private_key_path:
        raise PorosReadinessError(
            "Poros active signing key is missing private_key_path",
            reason_code="missing_private_key_path",
            data={"authority_id": manifest.authority_id, "kid": key.kid},
        )
    return key


def require_active_peer(manifest: PorosManifest, authority_id: str) -> PorosPeer:
    peer = manifest.peer(authority_id)
    if peer is None:
        raise PorosReadinessError(
            "Poros peer is not registered",
            reason_code="peer_not_found",
            data={"authority_id": authority_id},
        )
    if peer.status != "active":
        raise PorosReadinessError(
            "Poros peer is disabled",
            reason_code="peer_disabled",
            data={"authority_id": authority_id, "status": peer.status},
        )
    return peer


def require_single_active_peer_key(peer: PorosPeer) -> PorosKeyRef:
    active_keys = peer.active_keys()
    if len(active_keys) != 1:
        raise PorosReadinessError(
            "Poros peer must have exactly one active signing key",
            reason_code="ambiguous_peer_signing_key",
            data={
                "authority_id": peer.authority_id,
                "active_key_count": len(active_keys),
            },
        )
    return active_keys[0]


def readiness_report(
    manifest: PorosManifest,
    *,
    level: str = "local-demo",
    require_transfer_proof: bool | None = None,
) -> dict[str, Any]:
    if level not in VALID_ASSURANCE_LEVELS - {"none"}:
        raise PorosReadinessError(
            "Unsupported Poros readiness level",
            reason_code="invalid_readiness_level",
            data={"level": level},
        )
    checks: list[dict[str, Any]] = []

    def add_check(
        check_id: str,
        ok: bool,
        message: str,
        *,
        data: dict[str, Any] | None = None,
    ) -> None:
        checks.append(
            {
                "id": check_id,
                "ok": bool(ok),
                "message": message,
                "data": data or {},
            }
        )

    add_check(
        "manifest.authority",
        bool(manifest.authority_id),
        "local authority id is configured",
        data={"authority_id": manifest.authority_id},
    )
    add_check(
        "manifest.roles",
        bool(manifest.roles),
        "at least one Poros role is enabled",
        data={"roles": list(manifest.roles)},
    )
    active_keys = manifest.active_signing_keys()
    add_check(
        "local.signing_key",
        bool(active_keys),
        "at least one local active signing key is configured",
        data={"active_key_count": len(active_keys)},
    )
    add_check(
        "local.signing_key_unique",
        len(active_keys) == 1,
        "exactly one local active signing key is configured",
        data={"active_key_count": len(active_keys)},
    )
    object_root = str(manifest.object_store.get("root") or "")
    add_check(
        "object_store.local_root",
        bool(object_root),
        "local object-store root is configured",
        data={"backend": manifest.object_store.get("backend"), "root": object_root},
    )
    active_peers = manifest.active_peers()
    add_check(
        "peers.active",
        bool(active_peers),
        "at least one active peer is pinned",
        data={"active_peer_count": len(active_peers)},
    )
    for peer in manifest.peers:
        active_peer_keys = peer.active_keys()
        add_check(
            f"peer.{_check_id(peer.authority_id)}.active_key",
            peer.status != "active" or len(active_peer_keys) == 1,
            "active peer has exactly one active signing key",
            data={
                "authority_id": peer.authority_id,
                "status": peer.status,
                "active_key_count": len(active_peer_keys),
            },
        )
        for key in peer.keys:
            if key.status == "retired" and key.replaced_by:
                replacement = next(
                    (
                        candidate
                        for candidate in peer.keys
                        if candidate.kid == key.replaced_by
                    ),
                    None,
                )
                add_check(
                    f"peer.{_check_id(peer.authority_id)}.{_check_id(key.kid)}.replacement",
                    replacement is not None and replacement.status == "active",
                    "retired peer key has active replacement evidence",
                    data={
                        "authority_id": peer.authority_id,
                        "kid": key.kid,
                        "replaced_by": key.replaced_by,
                    },
                )
    if "receiver" in manifest.roles:
        add_check(
            "receiver.endpoint",
            bool(manifest.receiver_endpoint),
            "receiver endpoint is configured",
            data={"receiver_endpoint": manifest.receiver_endpoint},
        )
        add_check(
            "receiver.nomos_bundle",
            bool(
                manifest.nomos.get("bundle_path") or manifest.nomos.get("bundle_sha256")
            ),
            "receiver-owned Nomos bundle reference is configured",
            data={
                "bundle_path": manifest.nomos.get("bundle_path"),
                "bundle_sha256": manifest.nomos.get("bundle_sha256"),
                "policy_fingerprint": manifest.nomos.get("policy_fingerprint"),
            },
        )
        add_check(
            "receiver.redaction",
            bool(manifest.redaction.get("default_mode")),
            "receiver redaction default is configured",
            data={"default_mode": manifest.redaction.get("default_mode")},
        )
    should_require_transfer = (
        level in {"local-demo", "pilot", "production"}
        if require_transfer_proof is None
        else require_transfer_proof
    )
    if should_require_transfer:
        database_identities = _evidence_mapping(
            manifest.readiness_evidence.get("database_identities")
        )
        add_check(
            "evidence.separate_databases",
            database_identities.get("separate_databases") is True,
            "sender and receiver database identities are distinct",
            data=database_identities,
        )
        transfer = _evidence_mapping(
            manifest.readiness_evidence.get("latest_transfer_proof")
        )
        transfer_ok = _transfer_proof_ok(transfer, level=level)
        add_check(
            "evidence.transfer_proof",
            transfer_ok,
            "latest Poros transfer proof shows a published transfer",
            data={"latest_transfer_proof": transfer},
        )
    if level in {"pilot", "production"}:
        policy_fingerprint = manifest.nomos.get(
            "policy_fingerprint"
        ) or manifest.nomos.get("bundle_sha256")
        add_check(
            "pilot.nomos_fingerprint",
            bool(policy_fingerprint),
            "active receiver-owned Nomos policy fingerprint is configured",
            data={
                "policy_fingerprint": manifest.nomos.get("policy_fingerprint"),
                "bundle_sha256": manifest.nomos.get("bundle_sha256"),
            },
        )
        add_check(
            "pilot.object_store_access",
            _path_exists(object_root),
            "local object-store root exists",
            data={
                "backend": manifest.object_store.get("backend"),
                "root": object_root,
            },
        )
        object_evidence = _evidence_mapping(
            manifest.readiness_evidence.get("object_store")
        )
        add_check(
            "pilot.object_store_verified",
            object_evidence.get("verified") is True,
            "object-store verification evidence is recorded",
            data=object_evidence,
        )
        service_delivery = _evidence_mapping(
            manifest.readiness_evidence.get("service_delivery")
        )
        add_check(
            "pilot.service_delivery",
            _service_delivery_ok(service_delivery),
            "service-delivery evidence is recorded",
            data=service_delivery,
        )
        fixtures = _evidence_mapping(
            manifest.readiness_evidence.get("failure_fixtures")
        )
        for fixture_name in REQUIRED_FAILURE_FIXTURES:
            fixture = _evidence_mapping(fixtures.get(fixture_name))
            add_check(
                f"pilot.failure_fixture.{fixture_name}",
                _failure_fixture_ok(fixture, fixture_name=fixture_name),
                "failure fixture is durable and non-live",
                data=fixture,
            )
        metrics = _evidence_mapping(manifest.readiness_evidence.get("metrics"))
        add_check(
            "metrics.clean",
            _metrics_clean(metrics),
            "metrics evidence has live runtime authority, no DLQ, and no blocked generations",
            data=metrics,
        )
        doctor = _evidence_mapping(manifest.readiness_evidence.get("doctor"))
        add_check(
            "doctor.clean",
            doctor.get("ok") is True,
            "doctor evidence is clean",
            data=doctor,
        )
        refinement = _evidence_mapping(
            manifest.readiness_evidence.get("service_refinement")
        )
        add_check(
            "formal.service_refinement",
            _refinement_fresh(refinement),
            "fresh service-trace refinement evidence passed",
            data={"service_refinement": refinement},
        )
    if level == "production":
        add_check(
            "production.operator_policy",
            manifest.formal_assurance.get("production_operator_approval") is True,
            "production readiness requires explicit operator policy approval",
        )
    ready = all(check["ok"] for check in checks)
    return {
        "status": "ok" if ready else "not_ready",
        "ready": ready,
        "level": level,
        "authority_id": manifest.authority_id,
        "roles": list(manifest.roles),
        "checks": checks,
        "manifest": manifest.to_dict(public=True),
    }


def _transfer_proof_ok(transfer: dict[str, Any], *, level: str) -> bool:
    if not transfer:
        return False
    state = str(transfer.get("state") or transfer.get("proof_state") or "")
    receiver_generation = _evidence_mapping(transfer.get("receiver_generation"))
    generation_id = str(receiver_generation.get("generation_id") or "")
    live_delta = _live_delta(transfer)
    expected_delta = _explicit_live_delta(transfer.get("expected_live_row_delta"))
    observed_delta = _explicit_live_delta(
        transfer.get("observed_live_row_delta") or transfer.get("live_row_delta")
    )
    read_surface = _evidence_mapping(transfer.get("read_surface"))
    read_surface_kind = str(read_surface.get("kind") or "")
    read_surface_generation_id = str(read_surface.get("generation_id") or "")
    publication = _evidence_mapping(transfer.get("publication"))
    read_surface_matches = (
        read_surface_kind == "chronos_generation"
        and bool(read_surface_generation_id)
        and read_surface_generation_id == generation_id
    )
    row_delta_matches = (
        expected_delta is not None
        and observed_delta is not None
        and observed_delta == expected_delta
        and live_delta == observed_delta
    )
    return all(
        [
            bool(transfer.get("message_id")),
            bool(transfer.get("graph_id")),
            state in {"published", "promoted"},
            str(transfer.get("sender_outbox_status") or "") == "delivered",
            bool(generation_id),
            str(receiver_generation.get("status") or "") == "published",
            _transfer_fresh(transfer, level=level),
            _authorizing_transfer_decision_present(transfer),
            _admission_or_promotion_present(transfer, state=state),
            _published_generation_present(publication, generation_id=generation_id),
            read_surface_matches,
            live_delta["nodes_live"] + live_delta["edges_live"] > 0,
            row_delta_matches,
        ]
    )


def _service_delivery_ok(evidence: dict[str, Any]) -> bool:
    if not evidence:
        return False
    status = str(evidence.get("status") or evidence.get("outbox_status") or "")
    transport = str(evidence.get("transport") or "")
    return (
        status in {"ok", "delivered", "accepted_for_admission", "duplicate"}
        and transport in {"http", "service", "poros_http", "receiver_service_callback"}
        and bool(evidence.get("message_id"))
    )


def _failure_fixture_ok(fixture: dict[str, Any], *, fixture_name: str) -> bool:
    if not fixture:
        return False
    expectation = _FAILURE_FIXTURE_EXPECTATIONS.get(fixture_name)
    if expectation is None:
        return False
    live_delta = _live_delta(fixture)
    state = str(fixture.get("state") or fixture.get("status") or "")
    event_counts = _evidence_mapping(fixture.get("event_counts"))
    durable = fixture.get("durable_evidence") is True
    non_live_state = state in expectation["states"]
    event_count = sum(
        int(event_counts.get(key) or 0) for key in expectation["event_keys"]
    )
    transport_ok = (
        _transport_failure_fixture_ok(fixture)
        if fixture_name == "transport_failed"
        else True
    )
    return all(
        [
            durable,
            bool(fixture.get("message_id")),
            bool(fixture.get("graph_id") or fixture.get("body_sha256")),
            non_live_state,
            event_count > 0,
            transport_ok,
            live_delta["nodes_live"] == 0,
            live_delta["edges_live"] == 0,
        ]
    )


def _metrics_clean(metrics: dict[str, Any]) -> bool:
    if not metrics or "runtime_queue_unavailable" not in metrics:
        return False
    dlq_count = int(metrics.get("dlq_count") or 0)
    blocked = int(metrics.get("generation_blocked_count") or 0)
    runtime_unavailable = int(metrics.get("runtime_queue_unavailable") or 0)
    return dlq_count == 0 and blocked == 0 and runtime_unavailable == 0


def _refinement_fresh(refinement: dict[str, Any]) -> bool:
    if not refinement:
        return False
    status = str(refinement.get("status") or refinement.get("result") or "")
    if status not in {"pass", "ok"}:
        return False
    if refinement.get("fresh") is not True:
        return False
    artifact_path = refinement.get("artifact_path")
    if artifact_path is not None and not _path_exists(str(artifact_path)):
        return False
    return True


def _live_delta(evidence: dict[str, Any]) -> dict[str, int]:
    raw = (
        evidence.get("live_row_delta")
        or evidence.get("row_delta")
        or evidence.get("live_counts")
        or {}
    )
    raw = raw if isinstance(raw, dict) else {}
    return {
        "nodes_live": int(
            raw.get("nodes_live")
            if raw.get("nodes_live") is not None
            else raw.get("nodes_live_delta") or 0
        ),
        "edges_live": int(
            raw.get("edges_live")
            if raw.get("edges_live") is not None
            else raw.get("edges_live_delta") or 0
        ),
    }


def _explicit_live_delta(value: Any) -> dict[str, int] | None:
    if not isinstance(value, dict):
        return None
    if "nodes_live" not in value or "edges_live" not in value:
        return None
    return {
        "nodes_live": int(value.get("nodes_live") or 0),
        "edges_live": int(value.get("edges_live") or 0),
    }


def _transfer_fresh(transfer: dict[str, Any], *, level: str) -> bool:
    freshness = _evidence_mapping(transfer.get("freshness"))
    if not freshness or freshness.get("fresh") is not True:
        return False
    observed_at = _parse_evidence_datetime(
        freshness.get("observed_at")
        or freshness.get("generated_at")
        or transfer.get("observed_at")
        or transfer.get("generated_at")
    )
    if observed_at is None:
        return False
    profile_max_age = _TRANSFER_PROOF_MAX_AGE_SECONDS.get(
        level,
        _TRANSFER_PROOF_MAX_AGE_SECONDS["production"],
    )
    if "max_age_seconds" not in freshness:
        return False
    try:
        declared_max_age = int(freshness.get("max_age_seconds"))
    except (TypeError, ValueError):
        return False
    max_age_seconds = min(profile_max_age, declared_max_age)
    age_seconds = (datetime.now(UTC) - observed_at).total_seconds()
    return -60 <= age_seconds <= max_age_seconds


def _parse_evidence_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _transport_failure_fixture_ok(fixture: dict[str, Any]) -> bool:
    delivery_result = _evidence_mapping(fixture.get("delivery_result"))
    sender_delivery = _evidence_mapping(fixture.get("sender_delivery"))
    outbox = _evidence_mapping(sender_delivery.get("outbox"))
    attempts = sender_delivery.get("attempts")
    if not isinstance(attempts, list) or not attempts or not isinstance(attempts[-1], dict):
        return False
    delivery_attempt = dict(attempts[-1])
    receiver_summary = _evidence_mapping(fixture.get("receiver_summary"))
    outbox_status = str(outbox.get("status") or "")
    delivery_result_outbox_status = str(delivery_result.get("outbox_status") or "")
    if delivery_result_outbox_status and delivery_result_outbox_status != outbox_status:
        return False
    return all(
        [
            bool(outbox),
            str(delivery_attempt.get("status") or "") == "failed",
            str(delivery_attempt.get("reason_code") or "") == "delivery_failed",
            str(delivery_attempt.get("transport") or "") == "http",
            not delivery_attempt.get("receipt_sha256"),
            outbox_status == "terminal_failed",
            receiver_summary.get("inbox_status") in {None, ""},
        ]
    )


def _published_generation_present(
    publication: dict[str, Any],
    *,
    generation_id: str,
) -> bool:
    publication_generation_id = str(
        publication.get("receiver_generation_id")
        or publication.get("generation_id")
        or ""
    )
    return (
        bool(publication_generation_id)
        and publication_generation_id == generation_id
        and str(publication.get("event_type") or "") == "published"
        and str(publication.get("status") or "") == "published"
    )


def _authorizing_transfer_decision_present(transfer: dict[str, Any]) -> bool:
    decision = _evidence_mapping(transfer.get("nomos_decision"))
    if (decision.get("nomos_decision_id") or decision.get("decision_id")) and str(
        decision.get("status") or decision.get("effect") or ""
    ) in {
        "allow",
        "redact",
        "declassify",
    }:
        return True
    promotion = _evidence_mapping(transfer.get("promotion_decision"))
    return bool(
        (promotion.get("nomos_decision_id") or promotion.get("decision_id"))
        and str(promotion.get("status") or promotion.get("effect") or "") == "allow"
    )


def _admission_or_promotion_present(
    transfer: dict[str, Any],
    *,
    state: str,
) -> bool:
    admission = _evidence_mapping(transfer.get("admission"))
    if (
        admission.get("admission_id")
        and str(admission.get("event_type") or "") == "admitted"
        and str(admission.get("status") or "") == "accepted"
    ):
        return True
    promotion = _evidence_mapping(transfer.get("promotion_result"))
    if (
        state == "promoted"
        and str(promotion.get("event_type") or "") == "promoted"
        and str(promotion.get("status") or "") in {"accepted", "published"}
    ):
        return True
    return False


def _evidence_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _path_exists(value: str) -> bool:
    return bool(value) and Path(value).expanduser().exists()


def signing_key_ref(
    *,
    kid: str,
    public_key: str,
    private_key_path: str | None,
) -> PorosKeyRef:
    return PorosKeyRef.from_mapping(
        {
            "kid": kid,
            "public_key": public_key,
            "private_key_path": private_key_path,
            "fingerprint": public_key_fingerprint(public_key),
            "status": "active",
            "created_at": _now_iso(),
        }
    )


def public_key_fingerprint(public_key: str) -> str:
    try:
        raw = b64url_decode(public_key, field="public_key")
    except PorosError as exc:
        raise PorosReadinessError(
            "Ed25519 public key is not valid base64url",
            reason_code="invalid_public_key",
        ) from exc
    if len(raw) != 32:
        raise PorosReadinessError(
            "Ed25519 public key must decode to 32 bytes",
            reason_code="invalid_public_key",
        )
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def file_sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def validate_authority_id(value: str) -> None:
    if not isinstance(value, str) or not value:
        raise PorosReadinessError(
            "Poros authority id is required",
            reason_code="missing_authority_id",
        )
    if not _AUTHORITY_RE.match(value):
        raise PorosReadinessError(
            "Poros authority id must be URI-like, for example okg:lab-a",
            reason_code="invalid_authority_id",
            data={"authority_id": value},
        )


def _validated_object_store(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PorosReadinessError(
            "Poros object_store must be an object",
            reason_code="invalid_object_store",
        )
    backend = str(value.get("backend") or "")
    if backend not in VALID_OBJECT_BACKENDS:
        raise PorosReadinessError(
            "Poros object_store backend is invalid",
            reason_code="invalid_object_store_backend",
            data={"backend": backend},
        )
    root = _optional_string(value, "root")
    if backend == "local" and not root:
        raise PorosReadinessError(
            "Poros local object_store requires root",
            reason_code="missing_object_store_root",
        )
    return {"backend": backend, "root": root}


def _validated_nomos(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PorosReadinessError(
            "Poros nomos config must be an object",
            reason_code="invalid_nomos_config",
        )
    out = dict(value)
    bundle_path = out.get("bundle_path")
    if bundle_path is not None and not isinstance(bundle_path, str):
        raise PorosReadinessError(
            "Poros nomos bundle_path must be a string",
            reason_code="invalid_nomos_bundle_path",
        )
    return out


def _validated_redaction(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PorosReadinessError(
            "Poros redaction config must be an object",
            reason_code="invalid_redaction_config",
        )
    mode = str(value.get("default_mode") or "")
    if mode not in VALID_REDACTION_MODES:
        raise PorosReadinessError(
            "Poros redaction default_mode is invalid",
            reason_code="invalid_redaction_mode",
            data={"default_mode": mode},
        )
    return {"default_mode": mode}


def _validated_formal_assurance(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PorosReadinessError(
            "Poros formal_assurance config must be an object",
            reason_code="invalid_formal_assurance_config",
        )
    level = str(value.get("level") or "none")
    if level not in VALID_ASSURANCE_LEVELS:
        raise PorosReadinessError(
            "Poros formal assurance level is invalid",
            reason_code="invalid_formal_assurance_level",
            data={"level": level},
        )
    out = dict(value)
    out["level"] = level
    return out


def _assert_unique_key_ids(keys: list[PorosKeyRef], *, authority_id: str) -> None:
    seen: set[str] = set()
    for key in keys:
        if key.kid in seen:
            raise PorosReadinessError(
                "Poros key ids must be unique per authority",
                reason_code="duplicate_key_id",
                data={"authority_id": authority_id, "kid": key.kid},
            )
        seen.add(key.kid)


def _assert_unique_peer_authorities(peers: list[PorosPeer]) -> None:
    seen: set[str] = set()
    for peer in peers:
        if peer.authority_id in seen:
            raise PorosReadinessError(
                "Poros peer authority ids must be unique",
                reason_code="duplicate_peer_authority",
                data={"authority_id": peer.authority_id},
            )
        seen.add(peer.authority_id)


def _validate_key_status(status: str) -> None:
    if status not in VALID_KEY_STATUS:
        raise PorosReadinessError(
            "Poros key status is invalid",
            reason_code="invalid_key_status",
            data={"status": status},
        )


def _required_string(
    value: dict[str, Any],
    key: str,
    *,
    reason_code: str,
) -> str:
    raw = value.get(key)
    if not isinstance(raw, str) or not raw:
        raise PorosReadinessError(
            f"Poros manifest field {key} is required",
            reason_code=reason_code,
            data={"field": key},
        )
    return raw


def _optional_string(value: dict[str, Any], key: str) -> str | None:
    raw = value.get(key)
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise PorosReadinessError(
            f"Poros manifest field {key} must be a string",
            reason_code="invalid_manifest_field",
            data={"field": key},
        )
    return raw


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _check_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "value"
