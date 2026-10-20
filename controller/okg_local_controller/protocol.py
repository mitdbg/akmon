"""Frozen, candidate-independent wire types for local lifecycle control."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

DESCRIPTOR_SCHEMA = "okg.local-controller.release-descriptor/v1"
PLAN_SCHEMA = "okg.local-controller.transition-plan/v1"
PROBE_INPUT_SCHEMA = "okg.local-controller.probe-input/v1"
PROBE_ATTESTATION_SCHEMA = "okg.local-controller.probe-attestation/v1"
PROBE_RESULT_SCHEMA = "okg.local-controller.probe-result/v1"
INSTALLATION_SCHEMA = "okg.local-controller.installation/v1"
ACTIVATION_RECEIPT_SCHEMA = "okg.local-controller.activation-receipt/v1"
CAPABILITY_SCHEMA = "okg.local-controller.capability/v1"
UPGRADE_ATTESTATION_SCHEMA = "okg.local-controller.upgrade-attestation/v1"
UPGRADE_EFFECT_SCHEMA = "okg.local-controller.upgrade-effect/v1"
UPGRADE_EFFECT_KIND = "okg.deployment-upgrade/v1"
SMOKE_OUTCOME_SCHEMA = "okg.local-controller.upgrade-smoke-outcome/v1"
OBSERVATION_PROOF_SCHEMA = "okg.local-controller.observation-proof/v1"
EFFECT_REQUEST_SCHEMA = "okg.local-controller.effect-request/v1"
SUPPORTED_PROTOCOL_MAJOR = 1
CONTROLLER_VERSION = 1
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")

CANONICAL_SAGA_EFFECTS: tuple[tuple[str, str, bool], ...] = (
    ("approval", "okg.lifecycle.approval/v1", False),
    ("acquire_enqueue_fence", "okg.lifecycle.acquire-enqueue-fence/v1", False),
    ("drain_workflows", "okg.lifecycle.drain-workflows/v1", False),
    ("stop_processes", "okg.lifecycle.stop-processes/v1", False),
    ("create_backup", "okg.lifecycle.create-backup/v1", False),
    ("reserve_writer_epoch", "okg.lifecycle.reserve-writer-epoch/v1", False),
    ("upgrade", UPGRADE_EFFECT_KIND, True),
    ("activate_release", "okg.lifecycle.activate-release/v1", False),
    ("start_services", "okg.lifecycle.start-services/v1", False),
    ("verify_readiness", "okg.lifecycle.verify-readiness/v1", False),
    ("unfreeze", "okg.lifecycle.unfreeze/v1", False),
)
CANONICAL_EFFECT_KINDS = frozenset(kind for _key, kind, _flag in CANONICAL_SAGA_EFFECTS)
RETIREMENT_EFFECTS: tuple[tuple[str, str], ...] = (
    ("retire_drain", "okg.retirement.drain/v1"),
    ("retire_stop", "okg.retirement.stop/v1"),
    ("retire_withdraw", "okg.retirement.withdraw-publication/v1"),
    ("retire_verify", "okg.retirement.verify/v1"),
)
RETIREMENT_EFFECT_KINDS = frozenset(kind for _key, kind in RETIREMENT_EFFECTS)


class ProtocolError(ValueError):
    pass


class TransitionState(StrEnum):
    PLANNED = "planned"
    APPLYING = "applying"
    AUTHORIZED = "authorized"
    PUBLISHED = "published"
    READY = "ready"
    RECOVERING = "recovering"
    ROLLED_BACK = "rolled_back"
    RETIRING = "retiring"
    RETIRED = "retired"
    BLOCKED = "blocked"


class EffectPhase(StrEnum):
    PLANNED = "planned"
    INTENDED = "intended"
    APPLIED = "applied"
    OBSERVED = "observed"
    COMMITTED = "committed"
    BLOCKED = "blocked"


def canonical_json(value: Any) -> str:
    def normalize(item: Any) -> Any:
        if item is None or isinstance(item, (str, bool, int)):
            return item
        if isinstance(item, float):
            raise ProtocolError("protocol numbers must be integers")
        if isinstance(item, Mapping):
            if any(not isinstance(key, str) for key in item):
                raise ProtocolError("protocol object keys must be strings")
            return {key: normalize(inner) for key, inner in item.items()}
        if isinstance(item, (list, tuple)):
            return [normalize(inner) for inner in item]
        raise ProtocolError(f"unsupported protocol value: {type(item).__name__}")

    return json.dumps(
        normalize(value),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def digest_json(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode()).hexdigest()


def rollback_generation_digest(
    *, predecessor_digest: str, expected_predecessor: str
) -> str:
    """Derive the only staged-generation identity valid for predecessor restore."""

    require_digest(predecessor_digest, "predecessor_digest")
    require_nonempty(expected_predecessor, "expected_predecessor")
    return digest_json(
        {
            "schema": "okg.local-controller.rollback-generation/v1",
            "predecessor_digest": predecessor_digest,
            "expected_predecessor": expected_predecessor,
        }
    )


def digest_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def require_digest(value: str, field: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ProtocolError(f"{field} must be a lowercase sha256 digest")
    return value


def require_nonempty(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProtocolError(f"{field} must be a non-empty string")
    return value


def require_positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ProtocolError(f"{field} must be a positive integer")
    return value


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class ControllerInstallation:
    """Signed, installation-owned binding for the sole local controller ledger."""

    installation_id: str
    deployment: str
    ledger_uuid: str
    ledger_path: str
    controller_trust_root_id: str
    probe_profile_digest: str
    probe_launcher_digest: str
    probe_limits_digest: str
    probe_max_ttl_seconds: int
    signature: str
    schema: str = INSTALLATION_SCHEMA

    @staticmethod
    def _fields() -> set[str]:
        return {
            "schema",
            "installation_id",
            "deployment",
            "ledger_uuid",
            "ledger_path",
            "controller_trust_root_id",
            "probe_profile_digest",
            "probe_launcher_digest",
            "probe_limits_digest",
            "probe_max_ttl_seconds",
            "signature",
        }

    def __post_init__(self) -> None:
        if self.schema != INSTALLATION_SCHEMA:
            raise ProtocolError("controller installation schema is unsupported")
        for field in (
            "installation_id",
            "controller_trust_root_id",
            "probe_profile_digest",
            "probe_launcher_digest",
            "probe_limits_digest",
        ):
            require_digest(getattr(self, field), field)
        require_nonempty(self.deployment, "deployment")
        require_nonempty(self.ledger_uuid, "ledger_uuid")
        require_nonempty(self.ledger_path, "ledger_path")
        if not self.ledger_path.startswith("/"):
            raise ProtocolError("installation ledger_path must be absolute")
        require_positive_int(self.probe_max_ttl_seconds, "probe_max_ttl_seconds")
        require_nonempty(self.signature, "signature")
        if self.installation_id != digest_json(self.identity_payload()):
            raise ProtocolError("controller installation id is invalid")

    def identity_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("signature")
        payload.pop("installation_id")
        return payload

    def unsigned_payload(self) -> dict[str, Any]:
        return {**self.identity_payload(), "installation_id": self.installation_id}

    def payload(self) -> dict[str, Any]:
        return asdict(self)

    def verify(self, signer: AuthoritySigner) -> None:
        if signer.key_id != self.controller_trust_root_id:
            raise ProtocolError("installation belongs to another trust root")
        if not signer.signer_for("installation").verify(
            self.unsigned_payload(), self.signature
        ):
            raise ProtocolError("controller installation signature is invalid")

    @classmethod
    def issue(
        cls,
        *,
        signer: AuthoritySigner,
        deployment: str,
        ledger_uuid: str,
        ledger_path: str,
        probe_profile_digest: str,
        probe_launcher_digest: str,
        probe_limits_digest: str,
        probe_max_ttl_seconds: int,
    ) -> ControllerInstallation:
        identity = {
            "schema": INSTALLATION_SCHEMA,
            "deployment": deployment,
            "ledger_uuid": ledger_uuid,
            "ledger_path": ledger_path,
            "controller_trust_root_id": signer.key_id,
            "probe_profile_digest": probe_profile_digest,
            "probe_launcher_digest": probe_launcher_digest,
            "probe_limits_digest": probe_limits_digest,
            "probe_max_ttl_seconds": probe_max_ttl_seconds,
        }
        installation_id = digest_json(identity)
        unsigned = {**identity, "installation_id": installation_id}
        return cls(
            **unsigned,
            signature=signer.signer_for("installation").sign(unsigned),
        )

    @classmethod
    def from_mapping(
        cls, payload: Mapping[str, Any], *, signer: AuthoritySigner
    ) -> ControllerInstallation:
        if set(payload) != cls._fields():
            raise ProtocolError("controller installation fields are not exact")
        installation = cls(**{field: payload[field] for field in cls._fields()})
        installation.verify(signer)
        return installation


@dataclass(frozen=True)
class ProbeResult:
    """One atomic candidate output plus trusted launcher attestation."""

    output: Mapping[str, Any]
    attestation: ProbeAttestation
    schema: str = PROBE_RESULT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != PROBE_RESULT_SCHEMA:
            raise ProtocolError("probe result schema is unsupported")
        if not isinstance(self.output, Mapping):
            raise ProtocolError("probe result output must be an object")

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> ProbeResult:
        if set(payload) != {"schema", "output", "attestation"}:
            raise ProtocolError("probe result fields are not exact")
        if not isinstance(payload["output"], Mapping) or not isinstance(
            payload["attestation"], Mapping
        ):
            raise ProtocolError("probe result payloads must be objects")
        return cls(
            schema=payload["schema"],
            output=dict(payload["output"]),
            attestation=ProbeAttestation.from_mapping(payload["attestation"]),
        )

    def payload(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "output": dict(self.output),
            "attestation": asdict(self.attestation),
        }


@dataclass(frozen=True)
class ObservationProof:
    subject_kind: str
    subject_digest: str
    evidence_digest: str
    observer_key_id: str
    nonce: str
    issued_at: int
    expires_at: int
    signature: str
    schema: str = OBSERVATION_PROOF_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != OBSERVATION_PROOF_SCHEMA:
            raise ProtocolError("observation proof schema is unsupported")
        require_nonempty(self.subject_kind, "subject_kind")
        require_nonempty(self.nonce, "nonce")
        for field in ("subject_digest", "evidence_digest", "observer_key_id"):
            require_digest(getattr(self, field), field)
        require_positive_int(self.issued_at, "issued_at")
        require_positive_int(self.expires_at, "expires_at")
        if self.expires_at <= self.issued_at:
            raise ProtocolError("observation proof expiry must follow issuance")
        require_nonempty(self.signature, "signature")

    def unsigned_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("signature")
        return payload

    @classmethod
    def issue(
        cls,
        *,
        signer: AuthoritySigner,
        subject_kind: str,
        subject_digest: str,
        evidence: Mapping[str, Any],
        nonce: str,
        ttl_seconds: int = 60,
        now: int | None = None,
    ) -> ObservationProof:
        issued_at = int(time.time()) if now is None else now
        unsigned = {
            "schema": OBSERVATION_PROOF_SCHEMA,
            "subject_kind": subject_kind,
            "subject_digest": subject_digest,
            "evidence_digest": digest_json(evidence),
            "observer_key_id": signer.key_id,
            "nonce": nonce,
            "issued_at": issued_at,
            "expires_at": issued_at + ttl_seconds,
        }
        return cls(**unsigned, signature=signer.sign(unsigned))

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> ObservationProof:
        fields = {
            "schema",
            "subject_kind",
            "subject_digest",
            "evidence_digest",
            "observer_key_id",
            "nonce",
            "issued_at",
            "expires_at",
            "signature",
        }
        if set(payload) != fields:
            raise ProtocolError("observation proof fields are not exact")
        return cls(**{field: payload[field] for field in fields})

    def verify(
        self,
        *,
        signer: AuthoritySigner,
        subject_kind: str,
        subject_digest: str,
        evidence: Mapping[str, Any],
        now: int | None = None,
    ) -> None:
        observed_at = int(time.time()) if now is None else now
        if signer.key_id != self.observer_key_id:
            raise ProtocolError("observation proof key identity is not trusted")
        if self.subject_kind != subject_kind or self.subject_digest != subject_digest:
            raise ProtocolError("observation proof subject binding is invalid")
        if self.evidence_digest != digest_json(evidence):
            raise ProtocolError("observation proof evidence binding is invalid")
        if observed_at < self.issued_at or observed_at >= self.expires_at:
            raise ProtocolError("observation proof is stale")
        if not signer.verify(self.unsigned_payload(), self.signature):
            raise ProtocolError("observation proof signature is invalid")


@dataclass(frozen=True)
class EffectSpec:
    key: str
    kind: str
    request: Mapping[str, Any]
    predecessors: tuple[str, ...] = ()
    irreversible: bool = False

    @property
    def request_digest(self) -> str:
        return digest_json(self.request)

    def payload(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "kind": self.kind,
            "request": dict(self.request),
            "request_digest": self.request_digest,
            "predecessors": list(self.predecessors),
            "irreversible": self.irreversible,
        }


def _validate_effects(effects: Sequence[EffectSpec]) -> tuple[EffectSpec, ...]:
    exact = tuple(effects)
    keys = [effect.key for effect in exact]
    if any(not isinstance(key, str) or not key.strip() for key in keys) or len(
        keys
    ) != len(set(keys)):
        raise ProtocolError("effect keys must be non-empty and unique")
    by_key = {effect.key: effect for effect in exact}
    for effect in exact:
        require_nonempty(effect.kind, f"{effect.key}.kind")
        canonical_json(effect.request)
        if effect.kind == UPGRADE_EFFECT_KIND:
            validate_upgrade_effect_request(effect.request)
        if not isinstance(effect.irreversible, bool):
            raise ProtocolError(f"effect {effect.key!r} irreversible must be boolean")
        for predecessor in effect.predecessors:
            if (
                not isinstance(predecessor, str)
                or predecessor == effect.key
                or predecessor not in by_key
            ):
                raise ProtocolError(
                    f"effect {effect.key!r} has invalid predecessor {predecessor!r}"
                )
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(key: str) -> None:
        if key in visiting:
            raise ProtocolError("effect plan contains a predecessor cycle")
        if key in visited:
            return
        visiting.add(key)
        for predecessor in by_key[key].predecessors:
            visit(predecessor)
        visiting.remove(key)
        visited.add(key)

    for key in keys:
        visit(key)
    return exact


_COMMON_EFFECT_REQUEST_FIELDS = {
    "environment_identity",
    "database_instance_identity",
    "service_plan_digest",
    "queue_scope_digest",
    "promotion_approval_digest",
    "evidence_digest",
    "operation_digest",
}


def _validate_canonical_saga(
    effects: Sequence[EffectSpec],
    *,
    environment_identity: str,
    database_instance_identity: str,
    service_plan_digest: str,
    queue_scope_digest: str,
    promotion_approval_digest: str,
    evidence_digest: str,
) -> tuple[EffectSpec, ...]:
    exact = tuple(effects)
    observed_shape = tuple(
        (effect.key, effect.kind, bool(effect.irreversible)) for effect in exact
    )
    if observed_shape != CANONICAL_SAGA_EFFECTS:
        raise ProtocolError(
            "descriptor must contain the exact canonical lifecycle effect set"
        )
    expected_predecessor: tuple[str, ...] = ()
    expected_identity = {
        "environment_identity": environment_identity,
        "database_instance_identity": database_instance_identity,
        "service_plan_digest": service_plan_digest,
        "queue_scope_digest": queue_scope_digest,
        "promotion_approval_digest": promotion_approval_digest,
        "evidence_digest": evidence_digest,
    }
    for effect in exact:
        if effect.predecessors != expected_predecessor:
            raise ProtocolError(
                f"effect {effect.key!r} violates the canonical total order"
            )
        request = effect.request
        common = {field: request.get(field) for field in expected_identity}
        if common != expected_identity:
            raise ProtocolError(
                f"effect {effect.key!r} does not bind the descriptor identities"
            )
        if effect.kind == UPGRADE_EFFECT_KIND:
            validate_upgrade_effect_request(request)
        else:
            expected_fields = {"schema", *_COMMON_EFFECT_REQUEST_FIELDS}
            if set(request) != expected_fields:
                raise ProtocolError(
                    f"effect {effect.key!r} request fields are not exact"
                )
            if request.get("schema") != EFFECT_REQUEST_SCHEMA:
                raise ProtocolError(
                    f"effect {effect.key!r} request schema is unsupported"
                )
            require_digest(request["operation_digest"], "operation_digest")
        expected_predecessor = (effect.key,)
    return exact


@dataclass(frozen=True)
class ReleaseDescriptor:
    deployment: str
    candidate_digest: str
    predecessor_digest: str
    expected_predecessor: str
    expected_activation_receipt_digest: str
    environment_identity: str
    database_instance_identity: str
    service_plan_digest: str
    queue_scope_digest: str
    promotion_approval_digest: str
    evidence_digest: str
    expected_database_writer_epoch: int
    database_writer_epoch: int
    dbos_application_version: str
    controller_min_version: int
    effects: tuple[EffectSpec, ...]
    schema: str = DESCRIPTOR_SCHEMA
    protocol_major: int = SUPPORTED_PROTOCOL_MAJOR

    def __post_init__(self) -> None:
        if self.schema != DESCRIPTOR_SCHEMA:
            raise ProtocolError(
                "unsupported descriptor envelope; install a compatible "
                "controller package"
            )
        require_positive_int(self.protocol_major, "protocol_major")
        if self.protocol_major != SUPPORTED_PROTOCOL_MAJOR:
            raise ProtocolError(
                "unsupported protocol major; install a compatible controller package"
            )
        require_nonempty(self.deployment, "deployment")
        require_digest(self.candidate_digest, "candidate_digest")
        require_digest(self.predecessor_digest, "predecessor_digest")
        for field in (
            "expected_activation_receipt_digest",
            "environment_identity",
            "database_instance_identity",
            "service_plan_digest",
            "queue_scope_digest",
            "promotion_approval_digest",
            "evidence_digest",
        ):
            require_digest(getattr(self, field), field)
        require_nonempty(self.expected_predecessor, "expected_predecessor")
        require_positive_int(
            self.expected_database_writer_epoch,
            "expected_database_writer_epoch",
        )
        require_positive_int(self.database_writer_epoch, "database_writer_epoch")
        if self.candidate_digest == self.predecessor_digest:
            if self.database_writer_epoch != self.expected_database_writer_epoch:
                raise ProtocolError(
                    "no-op descriptor cannot rotate the database writer epoch"
                )
        elif self.database_writer_epoch <= self.expected_database_writer_epoch:
            raise ProtocolError(
                "candidate database_writer_epoch must be strictly newer than expected"
            )
        require_nonempty(self.dbos_application_version, "dbos_application_version")
        require_positive_int(self.controller_min_version, "controller_min_version")
        exact_effects = _validate_effects(self.effects)
        object.__setattr__(
            self,
            "effects",
            _validate_canonical_saga(
                exact_effects,
                environment_identity=self.environment_identity,
                database_instance_identity=self.database_instance_identity,
                service_plan_digest=self.service_plan_digest,
                queue_scope_digest=self.queue_scope_digest,
                promotion_approval_digest=self.promotion_approval_digest,
                evidence_digest=self.evidence_digest,
            ),
        )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> ReleaseDescriptor:
        required = {
            "schema",
            "protocol_major",
            "deployment",
            "candidate_digest",
            "predecessor_digest",
            "expected_predecessor",
            "expected_activation_receipt_digest",
            "environment_identity",
            "database_instance_identity",
            "service_plan_digest",
            "queue_scope_digest",
            "promotion_approval_digest",
            "evidence_digest",
            "expected_database_writer_epoch",
            "database_writer_epoch",
            "dbos_application_version",
            "controller_min_version",
            "effects",
        }
        missing = required - set(payload)
        unexpected = set(payload) - required
        if missing or unexpected:
            raise ProtocolError(
                "descriptor fields are not exact: "
                f"missing={sorted(missing)}, unexpected={sorted(unexpected)}"
            )
        raw_effects = payload["effects"]
        if not isinstance(raw_effects, list):
            raise ProtocolError("effects must be a list")
        effects: list[EffectSpec] = []
        effect_fields = {
            "key",
            "kind",
            "request",
            "request_digest",
            "predecessors",
            "irreversible",
        }
        for index, item in enumerate(raw_effects):
            if not isinstance(item, Mapping) or set(item) != effect_fields:
                raise ProtocolError(f"effect {index} fields are not exact")
            if not isinstance(item["request"], Mapping):
                raise ProtocolError(f"effect {index} request must be an object")
            if not isinstance(item["predecessors"], list) or any(
                not isinstance(value, str) for value in item["predecessors"]
            ):
                raise ProtocolError(f"effect {index} predecessors must be strings")
            if not isinstance(item["irreversible"], bool):
                raise ProtocolError(f"effect {index} irreversible must be boolean")
            effect = EffectSpec(
                key=item["key"],
                kind=item["kind"],
                request=item["request"],
                predecessors=tuple(item["predecessors"]),
                irreversible=item["irreversible"],
            )
            if item["request_digest"] != effect.request_digest:
                raise ProtocolError(f"effect {effect.key!r} request digest is invalid")
            effects.append(effect)
        return cls(
            schema=payload["schema"],
            protocol_major=payload["protocol_major"],
            deployment=payload["deployment"],
            candidate_digest=payload["candidate_digest"],
            predecessor_digest=payload["predecessor_digest"],
            expected_predecessor=payload["expected_predecessor"],
            expected_activation_receipt_digest=payload[
                "expected_activation_receipt_digest"
            ],
            environment_identity=payload["environment_identity"],
            database_instance_identity=payload["database_instance_identity"],
            service_plan_digest=payload["service_plan_digest"],
            queue_scope_digest=payload["queue_scope_digest"],
            promotion_approval_digest=payload["promotion_approval_digest"],
            evidence_digest=payload["evidence_digest"],
            expected_database_writer_epoch=payload[
                "expected_database_writer_epoch"
            ],
            database_writer_epoch=payload["database_writer_epoch"],
            dbos_application_version=payload["dbos_application_version"],
            controller_min_version=payload["controller_min_version"],
            effects=tuple(effects),
        )

    def payload(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "protocol_major": self.protocol_major,
            "deployment": self.deployment,
            "candidate_digest": self.candidate_digest,
            "predecessor_digest": self.predecessor_digest,
            "expected_predecessor": self.expected_predecessor,
            "expected_activation_receipt_digest": (
                self.expected_activation_receipt_digest
            ),
            "environment_identity": self.environment_identity,
            "database_instance_identity": self.database_instance_identity,
            "service_plan_digest": self.service_plan_digest,
            "queue_scope_digest": self.queue_scope_digest,
            "promotion_approval_digest": self.promotion_approval_digest,
            "evidence_digest": self.evidence_digest,
            "expected_database_writer_epoch": self.expected_database_writer_epoch,
            "database_writer_epoch": self.database_writer_epoch,
            "dbos_application_version": self.dbos_application_version,
            "controller_min_version": self.controller_min_version,
            "effects": [effect.payload() for effect in self.effects],
        }

    @property
    def digest(self) -> str:
        return digest_json(self.payload())


@dataclass(frozen=True)
class TransitionPlan:
    transition_id: str
    ledger_uuid: str
    controller_trust_root_id: str
    descriptor_digest: str
    probe_attestation_id: str
    port_manifest_digest: str
    deployment: str
    candidate_digest: str
    predecessor_digest: str
    expected_predecessor: str
    expected_activation_receipt_digest: str
    environment_identity: str
    database_instance_identity: str
    service_plan_digest: str
    queue_scope_digest: str
    promotion_approval_digest: str
    evidence_digest: str
    staged_generation_digest: str
    capacity_reservation_digest: str
    backup_digest: str
    lifecycle_activation_epoch: int
    expected_database_writer_epoch: int
    database_writer_epoch: int
    dbos_application_version: str
    effect_set_digest: str
    irreversible_boundary: int | None
    staged_capability_expires_at: int
    schema: str = PLAN_SCHEMA

    def __post_init__(self) -> None:
        for field in (
            "descriptor_digest",
            "controller_trust_root_id",
            "probe_attestation_id",
            "port_manifest_digest",
            "candidate_digest",
            "predecessor_digest",
            "capacity_reservation_digest",
            "backup_digest",
            "effect_set_digest",
            "staged_generation_digest",
            "expected_activation_receipt_digest",
            "environment_identity",
            "database_instance_identity",
            "service_plan_digest",
            "queue_scope_digest",
            "promotion_approval_digest",
            "evidence_digest",
        ):
            require_digest(getattr(self, field), field)
        require_nonempty(self.transition_id, "transition_id")
        require_nonempty(self.ledger_uuid, "ledger_uuid")
        require_nonempty(self.deployment, "deployment")
        require_nonempty(self.expected_predecessor, "expected_predecessor")
        if self.lifecycle_activation_epoch <= 0:
            raise ProtocolError("lifecycle_activation_epoch must be positive")
        require_positive_int(
            self.expected_database_writer_epoch,
            "expected_database_writer_epoch",
        )
        if self.database_writer_epoch <= self.expected_database_writer_epoch:
            raise ProtocolError(
                "database_writer_epoch must be strictly newer than expected"
            )
        if self.staged_capability_expires_at <= 0:
            raise ProtocolError("staged_capability_expires_at must be positive")

    def payload(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def digest(self) -> str:
        return digest_json(self.payload())


@dataclass(frozen=True)
class ProbeAttestation:
    attestation_id: str
    nonce: str
    launcher_key_id: str
    issued_at: int
    expires_at: int
    input_digest: str
    input_payload_digest: str
    candidate_digest: str
    sandbox_profile_digest: str
    output_digest: str
    output_payload_digest: str
    launcher_digest: str
    limits_digest: str
    exit_status: int
    elapsed_millis: int
    cpu_limit_seconds: int
    memory_limit_bytes: int
    output_limit_bytes: int
    mutation_canary_before: str
    mutation_canary_after: str
    signature: str
    schema: str = PROBE_ATTESTATION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != PROBE_ATTESTATION_SCHEMA:
            raise ProtocolError("unsupported probe-attestation envelope")
        for field in (
            "attestation_id",
            "launcher_key_id",
            "input_digest",
            "input_payload_digest",
            "candidate_digest",
            "sandbox_profile_digest",
            "output_digest",
            "output_payload_digest",
            "launcher_digest",
            "limits_digest",
            "mutation_canary_before",
            "mutation_canary_after",
        ):
            require_digest(getattr(self, field), field)
        require_nonempty(self.nonce, "nonce")
        require_positive_int(self.issued_at, "issued_at")
        require_positive_int(self.expires_at, "expires_at")
        if self.expires_at <= self.issued_at:
            raise ProtocolError("probe attestation expiry must follow issuance")
        for field in (
            "elapsed_millis",
            "cpu_limit_seconds",
            "memory_limit_bytes",
            "output_limit_bytes",
        ):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ProtocolError(f"{field} must be a non-negative integer")
        if isinstance(self.exit_status, bool) or not isinstance(self.exit_status, int):
            raise ProtocolError("exit_status must be an integer")
        require_nonempty(self.signature, "signature")
        expected_id = digest_json(
            {
                "nonce": self.nonce,
                "launcher_key_id": self.launcher_key_id,
                "candidate_digest": self.candidate_digest,
                "output_payload_digest": self.output_payload_digest,
            }
        )
        if self.attestation_id != expected_id:
            raise ProtocolError("probe attestation id is invalid")

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> ProbeAttestation:
        fields = {
            "schema",
            "attestation_id",
            "nonce",
            "launcher_key_id",
            "issued_at",
            "expires_at",
            "input_digest",
            "input_payload_digest",
            "candidate_digest",
            "sandbox_profile_digest",
            "output_digest",
            "output_payload_digest",
            "launcher_digest",
            "limits_digest",
            "exit_status",
            "elapsed_millis",
            "cpu_limit_seconds",
            "memory_limit_bytes",
            "output_limit_bytes",
            "mutation_canary_before",
            "mutation_canary_after",
            "signature",
        }
        if set(payload) != fields:
            raise ProtocolError("probe-attestation fields are not exact")
        return cls(**{field: payload[field] for field in fields})

    def unsigned_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("signature")
        return payload


@dataclass(frozen=True)
class ActivationReceipt:
    transition_id: str
    ledger_uuid: str
    controller_trust_root_id: str
    deployment: str
    lifecycle_activation_epoch: int
    candidate_digest: str
    staged_generation_digest: str
    expected_predecessor: str
    expected_activation_receipt_digest: str
    expected_database_writer_epoch: int
    database_writer_epoch: int
    dbos_application_version: str
    effect_set_digest: str
    port_manifest_digest: str
    service_plan_digest: str
    authorized_at: str
    signature: str
    schema: str = ACTIVATION_RECEIPT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != ACTIVATION_RECEIPT_SCHEMA:
            raise ProtocolError("activation receipt schema is unsupported")
        require_nonempty(self.transition_id, "transition_id")
        require_nonempty(self.ledger_uuid, "ledger_uuid")
        require_nonempty(self.deployment, "deployment")
        require_nonempty(self.expected_predecessor, "expected_predecessor")
        require_nonempty(self.dbos_application_version, "dbos_application_version")
        require_nonempty(self.authorized_at, "authorized_at")
        require_nonempty(self.signature, "signature")
        for field in (
            "controller_trust_root_id",
            "candidate_digest",
            "staged_generation_digest",
            "expected_activation_receipt_digest",
            "effect_set_digest",
            "port_manifest_digest",
            "service_plan_digest",
        ):
            require_digest(getattr(self, field), field)
        require_positive_int(
            self.lifecycle_activation_epoch, "lifecycle_activation_epoch"
        )
        require_positive_int(
            self.expected_database_writer_epoch,
            "expected_database_writer_epoch",
        )
        require_positive_int(self.database_writer_epoch, "database_writer_epoch")

    def unsigned_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("signature")
        return payload

    @property
    def digest(self) -> str:
        return digest_json(self.unsigned_payload())


@dataclass(frozen=True)
class UpgradeAttestation:
    """Exact, verifier-authenticated upgrade proof consumed once by the ledger."""

    attestation_id: str
    nonce: str
    issuer: str
    issuer_key_id: str
    issued_at: int
    expires_at: int
    environment_identity: str
    database_instance_identity: str
    artifact_digest: str
    reconcile_plan_digest: str
    saga_transition_id: str
    database_writer_epoch: int
    backup_digest: str
    database_state_digest: str
    previous_artifact_digest: str
    candidate_artifact_digest: str
    schema_before_digest: str
    schema_after_digest: str
    migration_manifest_digest: str
    applied_migrations: tuple[str, ...]
    skipped_migrations: tuple[str, ...]
    rollback_compatibility_class: str
    restore_strategy: str
    smoke_outcome: Mapping[str, Any]
    result: str
    no_op_reason: str | None
    content_digest: str
    signature: str
    schema: str = UPGRADE_ATTESTATION_SCHEMA

    @staticmethod
    def _fields() -> set[str]:
        return {
            "schema",
            "attestation_id",
            "nonce",
            "issuer",
            "issuer_key_id",
            "issued_at",
            "expires_at",
            "environment_identity",
            "database_instance_identity",
            "artifact_digest",
            "reconcile_plan_digest",
            "saga_transition_id",
            "database_writer_epoch",
            "backup_digest",
            "database_state_digest",
            "previous_artifact_digest",
            "candidate_artifact_digest",
            "schema_before_digest",
            "schema_after_digest",
            "migration_manifest_digest",
            "applied_migrations",
            "skipped_migrations",
            "rollback_compatibility_class",
            "restore_strategy",
            "smoke_outcome",
            "result",
            "no_op_reason",
            "content_digest",
            "signature",
        }

    def __post_init__(self) -> None:
        if self.schema != UPGRADE_ATTESTATION_SCHEMA:
            raise ProtocolError("upgrade attestation schema is unsupported")
        for field in (
            "environment_identity",
            "database_instance_identity",
            "artifact_digest",
            "reconcile_plan_digest",
            "backup_digest",
            "database_state_digest",
            "previous_artifact_digest",
            "candidate_artifact_digest",
            "schema_before_digest",
            "schema_after_digest",
            "migration_manifest_digest",
            "content_digest",
            "issuer_key_id",
        ):
            require_digest(getattr(self, field), field)
        for field in (
            "nonce",
            "issuer",
            "saga_transition_id",
            "rollback_compatibility_class",
            "restore_strategy",
        ):
            require_nonempty(getattr(self, field), field)
        require_nonempty(self.signature, "signature")
        require_positive_int(self.issued_at, "issued_at")
        require_positive_int(self.expires_at, "expires_at")
        require_positive_int(self.database_writer_epoch, "database_writer_epoch")
        if self.expires_at <= self.issued_at:
            raise ProtocolError("upgrade attestation expiry must follow issuance")
        if self.result not in {"success", "no_op"}:
            raise ProtocolError("upgrade attestation result must be success or no_op")
        if self.result == "no_op":
            require_nonempty(self.no_op_reason, "no_op_reason")
        elif self.no_op_reason is not None:
            raise ProtocolError("successful upgrade cannot declare a no-op reason")
        for field in ("applied_migrations", "skipped_migrations"):
            values = getattr(self, field)
            if any(not isinstance(value, str) or not value for value in values):
                raise ProtocolError(f"{field} must contain non-empty strings")
            if len(values) != len(set(values)):
                raise ProtocolError(f"{field} must not contain duplicates")
        if set(self.applied_migrations) & set(self.skipped_migrations):
            raise ProtocolError("applied and skipped migrations must be disjoint")
        smoke = self.smoke_outcome
        if not isinstance(smoke, Mapping) or set(smoke) != {
            "schema",
            "status",
            "generation_digest",
            "no_work_reason",
        }:
            raise ProtocolError("upgrade smoke outcome fields are not exact")
        if smoke["schema"] != SMOKE_OUTCOME_SCHEMA:
            raise ProtocolError("upgrade smoke outcome schema is unsupported")
        if smoke["status"] not in {"published", "valid_no_work"}:
            raise ProtocolError("upgrade smoke outcome status is not admissible")
        if smoke["status"] == "published":
            require_digest(smoke["generation_digest"], "generation_digest")
            if smoke["no_work_reason"] is not None:
                raise ProtocolError("published smoke cannot declare no-work")
        else:
            if smoke["generation_digest"] is not None:
                raise ProtocolError("valid no-work smoke cannot name a generation")
            require_nonempty(smoke["no_work_reason"], "no_work_reason")
        expected_id = digest_json(
            {
                "nonce": self.nonce,
                "issuer": self.issuer,
                "issuer_key_id": self.issuer_key_id,
                "saga_transition_id": self.saga_transition_id,
                "reconcile_plan_digest": self.reconcile_plan_digest,
            }
        )
        if self.attestation_id != expected_id:
            raise ProtocolError("upgrade attestation id is invalid")
        if self.content_digest != digest_json(self.content_payload()):
            raise ProtocolError("upgrade attestation content digest is invalid")

    def content_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("content_digest")
        payload.pop("signature")
        return payload

    def unsigned_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("signature")
        return payload

    def payload(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def issue(cls, *, signer: AuthoritySigner, **fields: Any) -> UpgradeAttestation:
        unsigned = {
            "schema": UPGRADE_ATTESTATION_SCHEMA,
            "issuer_key_id": signer.key_id,
            **fields,
        }
        unsigned["attestation_id"] = digest_json(
            {
                "nonce": unsigned["nonce"],
                "issuer": unsigned["issuer"],
                "issuer_key_id": unsigned["issuer_key_id"],
                "saga_transition_id": unsigned["saga_transition_id"],
                "reconcile_plan_digest": unsigned["reconcile_plan_digest"],
            }
        )
        normalized = {
            **unsigned,
            "applied_migrations": tuple(unsigned["applied_migrations"]),
            "skipped_migrations": tuple(unsigned["skipped_migrations"]),
        }
        content_digest = digest_json(normalized)
        signed = {**normalized, "content_digest": content_digest}
        return cls(**signed, signature=signer.sign(signed))

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> UpgradeAttestation:
        if set(payload) != cls._fields():
            raise ProtocolError("upgrade attestation fields are not exact")
        values = dict(payload)
        for field in ("applied_migrations", "skipped_migrations"):
            if not isinstance(values[field], list):
                raise ProtocolError(f"{field} must be a list")
            values[field] = tuple(values[field])
        if not isinstance(values["smoke_outcome"], Mapping):
            raise ProtocolError("smoke_outcome must be an object")
        return cls(**values)


def validate_upgrade_effect_request(request: Mapping[str, Any]) -> None:
    fields = {
        "schema",
        "environment_identity",
        "database_instance_identity",
        "service_plan_digest",
        "queue_scope_digest",
        "promotion_approval_digest",
        "evidence_digest",
        "operation_digest",
        "issuer",
        "attestation_key_id",
        "expected_schema_before_digest",
        "migration_manifest_digest",
        "rollback_compatibility_class",
        "restore_strategy",
        "max_attestation_ttl_seconds",
    }
    if set(request) != fields or request.get("schema") != UPGRADE_EFFECT_SCHEMA:
        raise ProtocolError("upgrade effect request fields are not exact")
    for field in (
        "environment_identity",
        "database_instance_identity",
        "service_plan_digest",
        "queue_scope_digest",
        "promotion_approval_digest",
        "evidence_digest",
        "operation_digest",
        "attestation_key_id",
        "expected_schema_before_digest",
        "migration_manifest_digest",
    ):
        require_digest(request[field], field)
    require_nonempty(request["issuer"], "issuer")
    require_nonempty(
        request["rollback_compatibility_class"],
        "rollback_compatibility_class",
    )
    require_nonempty(request["restore_strategy"], "restore_strategy")
    require_positive_int(
        request["max_attestation_ttl_seconds"], "max_attestation_ttl_seconds"
    )


@dataclass(frozen=True)
class MutationCapability:
    transition_id: str
    ledger_uuid: str
    controller_trust_root_id: str
    activation_receipt_digest: str | None
    provisional_plan_digest: str | None
    lifecycle_activation_epoch: int
    database_writer_epoch: int
    effect_set_digest: str
    mode: str
    expires_at: int | None
    token: str
    schema: str = CAPABILITY_SCHEMA

    def __post_init__(self) -> None:
        if self.mode not in {"staged", "active"}:
            raise ProtocolError("capability mode must be staged or active")
        require_digest(self.effect_set_digest, "effect_set_digest")
        require_nonempty(self.ledger_uuid, "ledger_uuid")
        require_digest(self.controller_trust_root_id, "controller_trust_root_id")
        if self.mode == "staged":
            if self.activation_receipt_digest is not None:
                raise ProtocolError(
                    "staged capability cannot bind an activation receipt"
                )
            if self.provisional_plan_digest is None or self.expires_at is None:
                raise ProtocolError(
                    "staged capability requires a plan digest and expiry"
                )
            require_digest(self.provisional_plan_digest, "provisional_plan_digest")
        else:
            if self.activation_receipt_digest is None:
                raise ProtocolError("active capability requires an activation receipt")
            if self.provisional_plan_digest is not None or self.expires_at is not None:
                raise ProtocolError(
                    "active capability cannot retain provisional authority"
                )
            require_digest(self.activation_receipt_digest, "activation_receipt_digest")

    def unsigned_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("token")
        return payload


class TargetPublicationPort(Protocol):
    def publish_authorized_generation(
        self, receipt: ActivationReceipt
    ) -> Mapping[str, Any]:
        raise NotImplementedError

    def observe_authorized_generation(
        self, receipt: ActivationReceipt
    ) -> Mapping[str, Any]:
        raise NotImplementedError

    def observe_current_state(
        self, descriptor: ReleaseDescriptor
    ) -> Mapping[str, Any]:
        raise NotImplementedError

    def observe_retirement(self, receipt: ActivationReceipt) -> Mapping[str, Any]:
        raise NotImplementedError


class AuthoritySigner:
    """Controller-held integrity authority; never passed to candidate code."""

    def __init__(self, key: bytes, *, domain: str = "controller-root") -> None:
        if len(key) < 32:
            raise ProtocolError("authority key must contain at least 32 bytes")
        self._key = bytes(key)
        self.domain = require_nonempty(domain, "authority domain")
        self.key_id = digest_json(
            {"domain": self.domain, "key_material_digest": digest_bytes(self._key)}
        )

    def signer_for(self, purpose: str) -> AuthoritySigner:
        purpose = require_nonempty(purpose, "authority purpose")
        derived = hmac.new(
            self._key,
            ("okg.local-controller.domain:" + purpose).encode(),
            hashlib.sha256,
        ).digest()
        return AuthoritySigner(derived, domain=purpose)

    def sign(self, payload: Mapping[str, Any]) -> str:
        material = canonical_json(payload).encode()
        return (
            "hmac-sha256:" + hmac.new(self._key, material, hashlib.sha256).hexdigest()
        )

    def verify(self, payload: Mapping[str, Any], signature: str) -> bool:
        return hmac.compare_digest(self.sign(payload), signature)

    def receipt(self, **fields: Any) -> ActivationReceipt:
        unsigned = {
            "schema": ACTIVATION_RECEIPT_SCHEMA,
            **fields,
            "authorized_at": fields.get("authorized_at") or utc_now(),
        }
        signature = self.sign(unsigned)
        return ActivationReceipt(signature=signature, **unsigned)

    def capability(
        self,
        *,
        transition_id: str,
        ledger_uuid: str,
        controller_trust_root_id: str,
        receipt_digest: str | None,
        provisional_plan_digest: str | None,
        lifecycle_epoch: int,
        database_writer_epoch: int,
        effect_set_digest: str,
        mode: str,
        expires_at: int | None,
    ) -> MutationCapability:
        unsigned = {
            "schema": CAPABILITY_SCHEMA,
            "transition_id": transition_id,
            "ledger_uuid": ledger_uuid,
            "controller_trust_root_id": controller_trust_root_id,
            "activation_receipt_digest": receipt_digest,
            "provisional_plan_digest": provisional_plan_digest,
            "lifecycle_activation_epoch": lifecycle_epoch,
            "database_writer_epoch": database_writer_epoch,
            "effect_set_digest": effect_set_digest,
            "mode": mode,
            "expires_at": expires_at,
        }
        token = self.sign(unsigned)
        return MutationCapability(token=token, **unsigned)
