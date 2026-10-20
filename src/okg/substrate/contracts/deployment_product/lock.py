"""Deterministic deployment-product locks and pre-apply plan verification.

This module resolves only the Wave 1 product contract.  It does not perform
the Wave 2 typed-asset DAG/semantic merge and it never builds a sealed release.
The existing ``okg.sealed_release/v1`` builder remains executable authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Literal, Protocol

from .models import (
    API_VERSION,
    CONTRACT_VERSION,
    AuthoredDeploymentDefinition,
    CompatibilityDecision,
    LockedConnectionContract,
    OwnerQualifiedReference,
    ProductBundleReference,
    ProvenanceRecord,
    ResolvedLock,
    TypedArtifactIdentity,
)
from .connections import connection_capability_digest, connection_contract_digest
from .validation import ValidatedProductSelection


PLAN_SCHEMA = "okg.deployment_product.plan/v1"
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_FORBIDDEN_LOCK_KEYS = frozenset(
    {
        "artifact_root",
        "evidence",
        "generation",
        "generation_id",
        "generations",
        "host_state",
        "last_known_good",
        "receipt",
        "receipts",
        "runtime_state",
        "sealed_release",
        "sealed_release_ref",
        "secret_material",
        "secret_value",
        "secret_version",
        "secret_version_id",
    }
)
_ACTION_ARGUMENT_KINDS: Mapping[str, Literal["digest", "identifier", "logical_id"]] = {
    "artifact": "logical_id",
    "artifact_id": "digest",
    "bundle_id": "identifier",
    "connection_ref": "identifier",
    "definition_id": "identifier",
    "deployment_id": "identifier",
    "environment_id": "identifier",
    "lock_digest": "digest",
    "operation_id": "identifier",
    "package_id": "identifier",
    "plan_digest": "digest",
    "sealed_release_digest": "digest",
    "source_ref": "logical_id",
    "target": "logical_id",
}


class ArtifactReader(Protocol):
    """Read-only artifact access used before apply-side mutation."""

    def read_bytes(self, logical_id: str) -> bytes:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class LockDiagnostic:
    code: str
    path: str
    expected: str | None
    observed: str | None
    owner: str
    fix_hint: str
    phase: str = "pre_apply"

    def to_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "path": self.path,
            "expected": self.expected,
            "observed": self.observed,
            "owner": self.owner,
            "fix_hint": self.fix_hint,
            "phase": self.phase,
        }


class DeploymentLockError(ValueError):
    """Fail-closed deployment-lock error with stable diagnostics."""

    def __init__(self, message: str, diagnostics: Sequence[LockDiagnostic]) -> None:
        ordered = tuple(sorted(diagnostics, key=lambda item: (item.code, item.path)))
        super().__init__(message)
        self.diagnostics = ordered
        self.code = ordered[0].code if ordered else "deployment_lock_invalid"


@dataclass(frozen=True, slots=True)
class PlanAction:
    action_id: str
    kind: str
    arguments: Mapping[str, Any]

    def __post_init__(self) -> None:
        _action_identifier(self.action_id, field="action_id")
        _action_identifier(self.kind, field="action kind")
        object.__setattr__(
            self,
            "arguments",
            MappingProxyType(validate_action_arguments(self.arguments)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_id": _identifier(self.action_id, field="action_id"),
            "kind": _identifier(self.kind, field="kind"),
            "arguments": _json_value(dict(self.arguments)),
        }


@dataclass(frozen=True, slots=True)
class DeploymentProductPlan:
    lock: ResolvedLock
    lock_digest: str
    definition_digest: str
    artifact_digests: tuple[tuple[str, str], ...]
    expires_at: datetime
    actions: tuple[PlanAction, ...]
    plan_digest: str
    schema_version: str = PLAN_SCHEMA
    api_version: str = API_VERSION

    def payload(self, *, include_plan_digest: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "api_version": self.api_version,
            "lock": _canonical_lock_payload(self.lock),
            "lock_digest": self.lock_digest,
            "definition_digest": self.definition_digest,
            "artifact_digests": [
                {"logical_id": logical_id, "digest": digest}
                for logical_id, digest in self.artifact_digests
            ],
            "expires_at": _utc(self.expires_at).isoformat().replace("+00:00", "Z"),
            "actions": [action.to_dict() for action in self.actions],
        }
        if include_plan_digest:
            payload["plan_digest"] = self.plan_digest
        return payload


@dataclass(frozen=True, slots=True)
class PlanVerification:
    ok: bool
    diagnostics: tuple[LockDiagnostic, ...]
    mutation_allowed: bool


@dataclass(frozen=True, slots=True)
class ApplyAuthorization:
    plan_digest: str
    lock_digest: str
    composition_digest: str


def canonical_json_bytes(value: Any) -> bytes:
    """Encode the contract's strict, deterministic JSON/YAML subset."""

    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def validate_action_arguments(arguments: Mapping[str, Any]) -> dict[str, str]:
    """Validate the closed, secret-free reference/digest action vocabulary."""

    if not isinstance(arguments, Mapping):
        raise ValueError("action arguments must be a mapping")
    result: dict[str, str] = {}
    for key, value in sorted(arguments.items()):
        kind = _ACTION_ARGUMENT_KINDS.get(key)
        if kind is None:
            raise ValueError(
                f"action argument {key!r} is not a registered reference or digest field"
            )
        if not isinstance(value, str):
            raise ValueError(f"action argument {key!r} must be a string {kind}")
        if kind == "digest":
            result[key] = _digest(value, field=f"arguments.{key}")
        elif kind == "logical_id":
            result[key] = _action_identifier(
                _logical_id(value),
                field=f"arguments.{key}",
            )
        else:
            result[key] = _action_identifier(value, field=f"arguments.{key}")
    return result


def authored_definition_digest(definition: AuthoredDeploymentDefinition) -> str:
    validated = AuthoredDeploymentDefinition.model_validate(
        definition.model_dump(mode="python", round_trip=True)
    )
    payload = validated.model_dump(mode="json", by_alias=True)
    # These collections select identities; their YAML list order is not
    # precedence.  Normalize them before hashing so equivalent authored input
    # remains stable across formatters and checkout layouts.
    payload["bundles"] = sorted(
        payload["bundles"],
        key=lambda item: (
            item.get("owner", ""),
            item.get("bundle_id", ""),
            item.get("version") or "",
            item.get("bundle_digest", item.get("digest")) or "",
        ),
    )
    payload["overrides"] = sorted(
        payload["overrides"],
        key=lambda item: (item.get("owner", ""), item.get("slot_id", "")),
    )
    payload["local_assets"] = sorted(
        payload["local_assets"],
        key=lambda item: (item.get("namespace", ""), item.get("asset_id", "")),
    )
    payload["secret_references"] = sorted(
        payload["secret_references"],
        key=lambda item: (
            item.get("provider") or "",
            item.get("scope") or "",
            item.get("secret_id", item.get("ref", "")),
        ),
    )
    payload["connection_references"] = sorted(
        payload["connection_references"],
        key=lambda item: (
            item.get("owner", ""),
            item.get("id", item.get("ref", "")),
            item.get("kind", ""),
            item.get("version") or "",
        ),
    )
    payload["provider_connections"] = sorted(
        payload["provider_connections"],
        key=lambda item: (item.get("owner", ""), item.get("connection_id", "")),
    )
    payload["source_bindings"] = sorted(
        payload["source_bindings"],
        key=lambda item: item.get("binding_id", ""),
    )
    payload["tool_bindings"] = sorted(
        payload["tool_bindings"],
        key=lambda item: item.get("binding_id", ""),
    )
    return sha256_digest(canonical_json_bytes(payload))


def composition_digest(lock: ResolvedLock) -> str:
    """Return the digest over a lock with its self-digest field omitted."""

    return sha256_digest(canonical_json_bytes(_composition_payload(lock)))


def canonicalize_lock(lock: ResolvedLock) -> ResolvedLock:
    """Normalize unordered inputs, refuse collisions, and verify self-digest."""

    validated = ResolvedLock.model_validate(lock.model_dump(mode="python", round_trip=True))
    canonical = _canonical_lock(
        definition_id=validated.definition_id,
        definition_digest=validated.definition_digest,
        product_identity=validated.product_identity,
        distribution_release_ref=validated.distribution_release_ref,
        bundle_refs=validated.bundle_refs,
        asset_refs=validated.asset_refs,
        package_identities=validated.package_identities,
        environment_identities=validated.environment_identities,
        connection_contracts=validated.connection_contracts,
        local_asset_digests=validated.local_asset_digests,
        compatibility_decisions=validated.compatibility_decisions,
        override_provenance=validated.override_provenance,
        provenance=validated.provenance,
        composition_digest_value=validated.composition_digest,
    )
    observed = composition_digest(canonical)
    if observed != canonical.composition_digest:
        raise DeploymentLockError(
            "resolved lock composition digest is invalid",
            (
                LockDiagnostic(
                    code="composition_digest_mismatch",
                    path="composition_digest",
                    expected=canonical.composition_digest,
                    observed=observed,
                    owner="resolver",
                    fix_hint="resolve the authored definition again",
                ),
            ),
        )
    _reject_forbidden_lock_payload(_canonical_lock_payload(canonical))
    return canonical


def canonical_lock_bytes(lock: ResolvedLock) -> bytes:
    """Return canonical lock bytes; compact JSON is a valid YAML subset."""

    return canonical_json_bytes(_canonical_lock_payload(canonicalize_lock(lock)))


def resolve_product_lock(
    selection: ValidatedProductSelection,
    *,
    asset_refs: Sequence[OwnerQualifiedReference] = (),
    package_identities: Sequence[TypedArtifactIdentity] = (),
    environment_identities: Sequence[TypedArtifactIdentity] = (),
    compatibility_decisions: Sequence[CompatibilityDecision] = (),
    local_asset_digests: Mapping[str, str] | None = None,
    override_provenance: Sequence[ProvenanceRecord] = (),
    provenance: Sequence[ProvenanceRecord] = (),
) -> ResolvedLock:
    """Resolve the Wave 1 contract without performing Wave 2 asset composition."""

    stable_selection = ValidatedProductSelection.model_validate(
        selection.model_dump(mode="python", round_trip=True)
    )
    definition = stable_selection.definition
    definition_hash = authored_definition_digest(definition)
    selected_assets = [*stable_selection.distribution_release.asset_refs]
    for bundle in stable_selection.bundles:
        selected_assets.extend(bundle.asset_refs)
    selected_assets.extend(asset_refs)

    locals_from_definition = {
        f"{item.namespace}/{item.asset_id}": item.digest
        for item in definition.local_assets
    }
    for logical_id, digest in (local_asset_digests or {}).items():
        logical = _logical_id(logical_id)
        checked = _digest(digest, field=f"local_asset_digests.{logical}")
        prior = locals_from_definition.get(logical)
        if prior is not None and prior != checked:
            _collision(logical, prior, checked)
        locals_from_definition[logical] = checked

    selected_overrides = [
        item for item in stable_selection.provenance if item.source == "override"
    ]
    selected_overrides.extend(override_provenance)
    resolved_decisions = _derive_compatibility_decisions(
        stable_selection,
        compatibility_decisions,
    )
    resolution_provenance = _resolution_provenance(
        package_identities=package_identities,
        environment_identities=environment_identities,
        compatibility_decisions=resolved_decisions,
    )
    connection_contracts = tuple(
        LockedConnectionContract(
            connection_ref=connection.reference,
            contract_digest=connection_contract_digest(connection),
            capability_digest=connection_capability_digest(connection),
        )
        for connection in definition.provider_connections
    )
    candidate = _canonical_lock(
        definition_id=definition.definition_id,
        definition_digest=definition_hash,
        product_identity=stable_selection.identity,
        distribution_release_ref=stable_selection.distribution_release.reference,
        bundle_refs=tuple(bundle.reference for bundle in stable_selection.bundles),
        asset_refs=tuple(selected_assets),
        package_identities=tuple(package_identities),
        environment_identities=tuple(environment_identities),
        connection_contracts=connection_contracts,
        local_asset_digests=locals_from_definition,
        compatibility_decisions=resolved_decisions,
        override_provenance=tuple(selected_overrides),
        provenance=(*stable_selection.provenance, *resolution_provenance, *provenance),
        composition_digest_value="sha256:" + "0" * 64,
    )
    digest = composition_digest(candidate)
    return canonicalize_lock(candidate.model_copy(update={"composition_digest": digest}))


def make_action_plan(
    lock: ResolvedLock,
    *,
    expires_at: datetime,
    artifact_digests: Mapping[str, str],
    actions: Sequence[PlanAction] = (),
) -> DeploymentProductPlan:
    canonical_lock = canonicalize_lock(lock)
    expiry = _utc(expires_at)
    pins = tuple(
        sorted(
            (
                _logical_id(logical_id),
                _digest(digest, field=f"artifact_digests.{logical_id}"),
            )
            for logical_id, digest in artifact_digests.items()
        )
    )
    ordered_actions = tuple(sorted(actions, key=lambda item: (item.action_id, item.kind)))
    draft = DeploymentProductPlan(
        lock=canonical_lock,
        lock_digest=sha256_digest(canonical_lock_bytes(canonical_lock)),
        definition_digest=canonical_lock.definition_digest,
        artifact_digests=pins,
        expires_at=expiry,
        actions=ordered_actions,
        plan_digest="sha256:" + "0" * 64,
    )
    digest = sha256_digest(canonical_json_bytes(draft.payload(include_plan_digest=False)))
    return replace(draft, plan_digest=digest)


def verify_plan_before_apply(
    plan: DeploymentProductPlan,
    *,
    definition: AuthoredDeploymentDefinition,
    artifacts: ArtifactReader | Mapping[str, bytes],
    now: datetime,
    expected_plan_digest: str,
) -> PlanVerification:
    diagnostics: list[LockDiagnostic] = []
    if plan.schema_version != PLAN_SCHEMA:
        diagnostics.append(
            _diagnostic(
                "plan_schema_unsupported",
                "schema_version",
                PLAN_SCHEMA,
                str(plan.schema_version),
                "create a plan with the supported deployment-product schema",
            )
        )
    if plan.api_version != API_VERSION:
        diagnostics.append(
            _diagnostic(
                "plan_api_unsupported",
                "api_version",
                API_VERSION,
                str(plan.api_version),
                "create a plan with the supported deployment-product API",
            )
        )
    expected = _digest(expected_plan_digest, field="expected_plan_digest")
    recomputed_plan = sha256_digest(
        canonical_json_bytes(plan.payload(include_plan_digest=False))
    )
    if plan.plan_digest != expected or recomputed_plan != plan.plan_digest:
        diagnostics.append(
            _diagnostic(
                "plan_digest_mismatch",
                "plan_digest",
                expected,
                recomputed_plan,
                "recreate and re-approve the exact plan",
            )
        )

    try:
        canonical = canonicalize_lock(plan.lock)
        lock_digest = sha256_digest(canonical_lock_bytes(canonical))
        if lock_digest != plan.lock_digest:
            diagnostics.append(
                _diagnostic(
                    "lock_digest_mismatch",
                    "lock_digest",
                    plan.lock_digest,
                    lock_digest,
                    "resolve the deployment lock again",
                )
            )
    except DeploymentLockError as exc:
        diagnostics.extend(exc.diagnostics)

    current_definition = authored_definition_digest(definition)
    if current_definition != plan.definition_digest:
        diagnostics.append(
            _diagnostic(
                "definition_digest_mismatch",
                "definition",
                plan.definition_digest,
                current_definition,
                "plan the changed authored definition again",
                owner="instance",
            )
        )

    if _utc(now) >= _utc(plan.expires_at):
        diagnostics.append(
            _diagnostic(
                "plan_expired",
                "expires_at",
                plan.expires_at.isoformat(),
                _utc(now).isoformat(),
                "create a fresh plan",
            )
        )

    for logical_id, expected_digest in plan.artifact_digests:
        try:
            raw = _read_artifact(artifacts, logical_id)
        except (KeyError, FileNotFoundError, OSError):
            diagnostics.append(
                _diagnostic(
                    "artifact_missing",
                    logical_id,
                    expected_digest,
                    None,
                    "restore the pinned artifact and plan again",
                    owner="distribution",
                )
            )
            continue
        except TypeError as exc:
            diagnostics.append(
                _diagnostic(
                    "artifact_invalid",
                    logical_id,
                    expected_digest,
                    type(exc).__name__,
                    "repair the artifact reader and plan again",
                    owner="distribution",
                )
            )
            continue
        observed = sha256_digest(raw)
        if observed != expected_digest:
            diagnostics.append(
                _diagnostic(
                    "artifact_digest_mismatch",
                    logical_id,
                    expected_digest,
                    observed,
                    "replace the corrupt artifact and plan again",
                    owner="distribution",
                )
            )

    ordered = tuple(sorted(diagnostics, key=lambda item: (item.code, item.path)))
    return PlanVerification(
        ok=not ordered,
        diagnostics=ordered,
        mutation_allowed=not ordered,
    )


def require_plan_before_apply(
    plan: DeploymentProductPlan,
    *,
    definition: AuthoredDeploymentDefinition,
    artifacts: ArtifactReader | Mapping[str, bytes],
    now: datetime,
    expected_plan_digest: str,
) -> ApplyAuthorization:
    verification = verify_plan_before_apply(
        plan,
        definition=definition,
        artifacts=artifacts,
        now=now,
        expected_plan_digest=expected_plan_digest,
    )
    if not verification.ok:
        raise DeploymentLockError(
            "deployment product plan refused before apply-side mutation",
            verification.diagnostics,
        )
    return ApplyAuthorization(
        plan_digest=plan.plan_digest,
        lock_digest=plan.lock_digest,
        composition_digest=plan.lock.composition_digest,
    )


def _canonical_lock(
    *,
    definition_id: str,
    definition_digest: str,
    product_identity: Any,
    distribution_release_ref: Any,
    bundle_refs: Sequence[ProductBundleReference],
    asset_refs: Sequence[OwnerQualifiedReference],
    package_identities: Sequence[TypedArtifactIdentity],
    environment_identities: Sequence[TypedArtifactIdentity],
    connection_contracts: Sequence[LockedConnectionContract],
    local_asset_digests: Mapping[str, str],
    compatibility_decisions: Sequence[CompatibilityDecision],
    override_provenance: Sequence[ProvenanceRecord],
    provenance: Sequence[ProvenanceRecord],
    composition_digest_value: str,
) -> ResolvedLock:
    if distribution_release_ref.digest is None:
        raise DeploymentLockError(
            "distribution release identity is missing a digest",
            (
                _diagnostic(
                    "artifact_digest_missing",
                    "distribution_release",
                    "sha256:<digest>",
                    None,
                    "publish the distribution release with a content digest",
                    owner="distribution",
                ),
            ),
        )
    bundles = _dedupe_refs(
        bundle_refs,
        key=lambda item: (item.owner, item.bundle_id, item.version or ""),
        label="bundle",
        require_digest=True,
    )
    assets = _dedupe_refs(
        asset_refs,
        key=lambda item: (item.owner, item.ref, item.kind, item.version or ""),
        label="asset",
        require_digest=True,
    )
    packages = _dedupe_refs(
        package_identities,
        key=lambda item: (item.owner, item.ref, item.kind, item.version),
        label="package",
        require_digest=True,
    )
    environments = _dedupe_refs(
        environment_identities,
        key=lambda item: (item.owner, item.ref, item.kind, item.version),
        label="environment",
        require_digest=True,
    )
    connections = _dedupe_connection_contracts(connection_contracts)
    decisions = _dedupe_compatibility_decisions(
        compatibility_decisions
    )
    overrides = _dedupe_provenance(override_provenance)
    all_provenance = _dedupe_provenance(provenance)
    locals_map = {
        _logical_id(key): _digest(value, field=f"local_asset_digests.{key}")
        for key, value in sorted(local_asset_digests.items())
    }
    return ResolvedLock(
        definition_id=_identifier(definition_id, field="definition_id"),
        definition_digest=_digest(definition_digest, field="definition_digest"),
        product_identity=product_identity,
        distribution_release_ref=distribution_release_ref,
        bundle_refs=tuple(bundles),
        asset_refs=tuple(assets),
        package_refs=tuple(packages),
        environment_refs=tuple(environments),
        connection_contracts=connections,
        local_asset_digests=locals_map,
        compatibility_decisions=decisions,
        override_provenance=overrides,
        provenance=all_provenance,
        composition_digest=_digest(
            composition_digest_value,
            field="composition_digest",
        ),
    )


def _derive_compatibility_decisions(
    selection: ValidatedProductSelection,
    explicit: Sequence[CompatibilityDecision],
) -> tuple[CompatibilityDecision, ...]:
    decisions = list(explicit)
    artifacts = [
        (
            selection.distribution_release.owner,
            selection.distribution_release.distribution_id,
            "distribution-release",
            selection.distribution_release.version,
            selection.distribution_release.release_digest,
            selection.distribution_release.compatibility,
        ),
        *(
            (
                bundle.owner,
                bundle.bundle_id,
                "product-bundle",
                bundle.version,
                bundle.bundle_digest,
                bundle.compatibility,
            )
            for bundle in selection.bundles
        ),
    ]
    for owner, artifact_id, kind, version, digest, metadata in artifacts:
        if metadata.contract_versions:
            if CONTRACT_VERSION not in metadata.contract_versions:
                raise DeploymentLockError(
                    "selected product artifact does not support this contract version",
                    (
                        _diagnostic(
                            "compatibility_contract_unsupported",
                            f"{owner}/{kind}/{artifact_id}",
                            CONTRACT_VERSION,
                            ",".join(sorted(metadata.contract_versions)),
                            "select a compatible artifact release",
                            owner=owner,
                        ),
                    ),
                )
            decisions.append(
                CompatibilityDecision(
                    subject=OwnerQualifiedReference(
                        owner=owner,
                        id=f"{artifact_id}:contract:{CONTRACT_VERSION}",
                        kind="contract-version",
                        version=version,
                        digest=digest,
                    ),
                    decision="compatible",
                    reason="contract-version-compatible",
                )
            )
        for capability in sorted(metadata.capabilities):
            decisions.append(
                CompatibilityDecision(
                    subject=OwnerQualifiedReference(
                        owner=owner,
                        id=f"{artifact_id}:capability:{capability}",
                        kind="capability",
                        version=version,
                        digest=digest,
                    ),
                    decision="required",
                    reason="capability-declared",
                )
            )
    return tuple(decisions)


def _resolution_provenance(
    *,
    package_identities: Sequence[TypedArtifactIdentity],
    environment_identities: Sequence[TypedArtifactIdentity],
    compatibility_decisions: Sequence[CompatibilityDecision],
) -> tuple[ProvenanceRecord, ...]:
    records: list[ProvenanceRecord] = []
    for identity in package_identities:
        records.append(
            ProvenanceRecord(
                path=f"package_identities/{identity.owner}/{identity.kind}/{identity.ref}",
                owner=identity.owner,
                reference=identity,
                source="package",
                value_digest=identity.digest,
            )
        )
    for identity in environment_identities:
        records.append(
            ProvenanceRecord(
                path=f"environment_identities/{identity.owner}/{identity.kind}/{identity.ref}",
                owner=identity.owner,
                reference=identity,
                source="environment",
                value_digest=identity.digest,
            )
        )
    for decision in compatibility_decisions:
        subject = decision.subject
        decision_digest = sha256_digest(
            canonical_json_bytes(decision.model_dump(mode="json", by_alias=True))
        )
        records.append(
            ProvenanceRecord(
                path=(
                    f"compatibility_decisions/{subject.owner}/"
                    f"{subject.kind}/{subject.ref}"
                ),
                owner=decision.owner,
                reference=OwnerQualifiedReference(
                    owner=decision.owner,
                    id=f"{subject.owner}:{subject.kind}:{subject.ref}",
                    kind="compatibility-decision",
                    version=decision.reason,
                    digest=decision_digest,
                ),
                source="compatibility",
                value_digest=decision_digest,
            )
        )
    return tuple(records)


def _canonical_lock_payload(lock: ResolvedLock) -> dict[str, Any]:
    return lock.model_dump(mode="json", by_alias=True)


def _composition_payload(lock: ResolvedLock) -> dict[str, Any]:
    payload = _canonical_lock_payload(lock)
    payload.pop("composition_digest", None)
    return payload


def _dedupe_refs(
    values: Sequence[Any],
    *,
    key: Any,
    label: str,
    require_digest: bool = False,
) -> tuple[Any, ...]:
    seen: dict[tuple[Any, ...], Any] = {}
    for item in values:
        identity = key(item)
        digest = getattr(item, "digest", None)
        if require_digest and digest is None:
            raise DeploymentLockError(
                f"{label} identity is missing a digest",
                (
                    _diagnostic(
                        "artifact_digest_missing",
                        ":".join(str(part) for part in identity),
                        "sha256:<digest>",
                        None,
                        f"publish the {label} with a content digest",
                    ),
                ),
            )
        previous = seen.get(identity)
        if previous is not None:
            previous_digest = getattr(previous, "digest", None)
            if previous_digest != digest:
                _collision(":".join(str(part) for part in identity), previous_digest, digest)
            continue
        seen[identity] = item
    return tuple(seen[item] for item in sorted(seen))


def _dedupe_connection_contracts(
    values: Sequence[LockedConnectionContract],
) -> tuple[LockedConnectionContract, ...]:
    seen: dict[tuple[str, str], LockedConnectionContract] = {}
    for item in values:
        identity = (item.connection_ref.owner, item.connection_ref.ref)
        previous = seen.get(identity)
        if previous is not None and previous != item:
            _collision(
                ":".join(identity),
                f"{previous.contract_digest}:{previous.capability_digest}",
                f"{item.contract_digest}:{item.capability_digest}",
            )
        seen[identity] = item
    return tuple(seen[identity] for identity in sorted(seen))


def _dedupe_provenance(values: Sequence[ProvenanceRecord]) -> tuple[ProvenanceRecord, ...]:
    seen: dict[str, ProvenanceRecord] = {}
    for item in values:
        prior = seen.get(item.path)
        if prior is not None and prior != item:
            _collision(item.path, prior.value_digest, item.value_digest)
        seen[item.path] = item
    return tuple(seen[path] for path in sorted(seen))


def _dedupe_compatibility_decisions(
    values: Sequence[CompatibilityDecision],
) -> tuple[CompatibilityDecision, ...]:
    seen: dict[tuple[str, str, str, str | None], CompatibilityDecision] = {}
    for item in values:
        identity = (
            item.subject.owner,
            item.subject.ref,
            item.subject.kind,
            item.subject.version,
        )
        prior = seen.get(identity)
        if prior is not None and prior != item:
            _collision(
                "/".join(part or "_" for part in identity),
                f"{prior.decision}:{prior.reason}",
                f"{item.decision}:{item.reason}",
            )
        seen[identity] = item
    return tuple(seen[identity] for identity in sorted(seen, key=lambda value: tuple(part or "" for part in value)))


def _collision(path: str, expected: str | None, observed: str | None) -> None:
    raise DeploymentLockError(
        "deployment product identity has conflicting content",
        (
            _diagnostic(
                "identity_digest_collision",
                _stable_path(path),
                expected,
                observed,
                "publish one identity with one content digest",
            ),
        ),
    )


def _reject_forbidden_lock_payload(value: Any, *, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]+", "_", str(key).lower()).strip("_")
            if normalized in _FORBIDDEN_LOCK_KEYS:
                raise DeploymentLockError(
                    "runtime, secret, or sealed-release state cannot enter a product lock",
                    (
                        _diagnostic(
                            "forbidden_lock_field",
                            f"{path}.{key}",
                            None,
                            normalized,
                            "remove the non-product field and resolve again",
                        ),
                    ),
                )
            _reject_forbidden_lock_payload(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_forbidden_lock_payload(child, path=f"{path}[{index}]")


def _read_artifact(reader: ArtifactReader | Mapping[str, bytes], logical_id: str) -> bytes:
    if isinstance(reader, Mapping):
        value = reader[logical_id]
    else:
        value = reader.read_bytes(logical_id)
    if not isinstance(value, bytes):
        raise TypeError("artifact readers must return bytes")
    return value


def _diagnostic(
    code: str,
    path: str,
    expected: str | None,
    observed: str | None,
    fix_hint: str,
    *,
    owner: str = "resolver",
) -> LockDiagnostic:
    return LockDiagnostic(
        code=code,
        path=path,
        expected=expected,
        observed=observed,
        owner=owner,
        fix_hint=fix_hint,
    )


def _identifier(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError(f"{field} must be a non-empty stable identifier")
    if any(char in value for char in ("\x00", "\n", "\r")):
        raise ValueError(f"{field} contains an unsupported character")
    return value


def _action_identifier(value: str, *, field: str) -> str:
    text = _identifier(value, field=field)
    tokens = re.sub(r"[^a-z0-9]+", " ", text.lower()).split()
    if set(tokens) & {"credential", "password", "private", "secret", "token"}:
        raise ValueError(f"{field} cannot contain secret-material vocabulary")
    return text


def _logical_id(value: str) -> str:
    text = _identifier(value, field="logical_id")
    if "\\" in text or text.startswith("/") or re.match(r"^[A-Za-z]:", text):
        raise ValueError("logical ids must be POSIX-relative")
    parts = text.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError("logical ids cannot contain empty, '.', or '..' segments")
    return text


def _stable_path(value: str) -> str:
    text = _identifier(value, field="diagnostic path")
    if text.startswith("/") or "\\" in text:
        raise ValueError("diagnostic paths must be stable logical identities")
    return text


def _digest(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise ValueError(f"{field} must be sha256:<64 lowercase hexadecimal characters>")
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("plan timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _json_value(value: Any, *, path: str = "$") -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        if abs(value) > (2**53 - 1):
            raise ValueError(f"integers outside the interoperable JSON range are not canonical at {path}")
        return value
    if isinstance(value, float):
        raise ValueError(f"floating point values are not canonical at {path}")
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, child in value.items():
            if not isinstance(key, str) or key in result:
                raise ValueError(f"JSON object keys must be unique strings at {path}")
            result[key] = _json_value(child, path=f"{path}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [_json_value(child, path=f"{path}[{index}]") for index, child in enumerate(value)]
    raise ValueError(f"unsupported canonical JSON type at {path}: {type(value).__name__}")


__all__ = [
    "ApplyAuthorization",
    "ArtifactReader",
    "DeploymentLockError",
    "DeploymentProductPlan",
    "LockDiagnostic",
    "PLAN_SCHEMA",
    "PlanAction",
    "PlanVerification",
    "authored_definition_digest",
    "canonical_json_bytes",
    "canonical_lock_bytes",
    "canonicalize_lock",
    "composition_digest",
    "make_action_plan",
    "require_plan_before_apply",
    "resolve_product_lock",
    "sha256_digest",
    "verify_plan_before_apply",
    "validate_action_arguments",
]
