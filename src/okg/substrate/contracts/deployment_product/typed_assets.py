"""Declarative typed-asset registry and reference-DAG resolution.

The deployment-product contract is intentionally split in two layers.  The
Wave 1 models describe the identity of a product and its lock; this module
describes the *contents* selected by that product.  A :class:`TypedAsset`
contains only declarative metadata and references.  It is never imported,
discovered from a filesystem, or executed by the resolver.

Resolution is a pure validation step.  The optional mutation callback is
called only after every reference, version, capability, merge, and override
check has passed.  This makes it possible for an apply-side caller to prove
that a refusal happened before its catalog or runtime mutation.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any, Literal, Protocol

from .models import (
    OwnerQualifiedReference,
    ProvenanceRecord,
    ResolvedLock,
    TypedOverrideInput,
)


TYPED_ASSET_SCHEMA = "okg.deployment_product.typed_asset/v1"
TYPED_ASSET_REGISTRY_SCHEMA = "okg.deployment_product.typed_asset_registry/v1"
TYPED_ASSET_GRAPH_SCHEMA = "okg.deployment_product.typed_asset_graph/v1"

# The six validation planes are deliberately declarative and independent.  A
# caller may replace or extend this registry with package-owned kinds, but no
# package can silently introduce a merge rule through an import.
VALIDATION_PLANES = (
    "semantic-catalog",
    "ingestion-projection",
    "derivation-publication",
    "views-read-models",
    "agent-ui",
    "operations-governance",
)

# Core kind vocabulary used by the first-party registry.  Plane membership is
# metadata supplied by a composition layer; kind names remain reusable across
# planes (for example, a declarative policy can govern both views and sources).
CORE_ASSET_KINDS = (
    "ontology",
    "schema",
    "bridge",
    "narrowing",
    "alias",
    "identifier-pattern",
    "concept-seed",
    "identity-policy",
    "catalog-migration",
    "connector",
    "connection-type",
    "source-template",
    "source-binding",
    "extraction-pipeline",
    "link-policy",
    "source",
    "enricher",
    "embedding",
    "refresher",
    "invariant",
    "consistency-gate",
    "derived-producer",
    "view",
    "read-model",
    "lens",
    "dashboard",
    "interface",
    "prompt",
    "skill",
    "playbook",
    "tool",
    "resolver",
    "search",
    "chat-policy",
    "vocabulary",
    "policy",
    "nomos",
    "readiness-policy",
    "doctor-policy",
    "service",
    "storage-requirement",
    "release-policy",
    "conformance-policy",
)

_ID_RE = re.compile(r"^[^\s:;,/\\]+$")
_QUALIFIED_RE = re.compile(r"^[^\s:;,/\\]+:[^\s:;,/\\]+$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_STATES = frozenset({"enabled", "disabled", "tombstone"})
_CARDINALITIES = frozenset({"one", "optional", "many"})
_MERGE_RULES = frozenset({"refuse", "dedupe", "overlay", "replace"})
_LOCK_SOURCES = frozenset({
    "distribution", "bundle", "definition", "local", "package", "environment", "compatibility",
})


def _identifier(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value or not _ID_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a non-empty identifier")
    return value


def _qualified(value: object, *, field_name: str = "qualified_id") -> str:
    if not isinstance(value, str) or not _QUALIFIED_RE.fullmatch(value):
        raise ValueError(f"{field_name} must have the form owner:id")
    owner, asset_id = value.split(":", 1)
    _identifier(owner, field_name=f"{field_name}.owner")
    _identifier(asset_id, field_name=f"{field_name}.id")
    return value


def _digest(value: object, *, field_name: str = "digest") -> str:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be sha256:<64 lowercase hexadecimal characters>")
    return value


def _schema_token(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value or any(char.isspace() for char in value):
        raise ValueError(f"{field_name} must be a non-empty schema token")
    return value


def _tuple_strings(value: object, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, Iterable):
        raise ValueError(f"{field_name} must be a sequence of identifiers")
    return tuple(sorted({_identifier(item, field_name=field_name) for item in value}))


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _value_digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a mapping")
    return MappingProxyType(dict(value))


def _asset_source(asset: "TypedAsset") -> str:
    """Read declared origin; missing origin remains explicitly local."""

    source = asset.provenance.get("source", "local")
    if source not in _LOCK_SOURCES:
        raise ValueError(f"asset provenance source must be one of {sorted(_LOCK_SOURCES)}")
    return str(source)


_MISSING = object()


def _pop_alias(raw: dict[str, Any], *names: str, default: Any = _MISSING, field_name: str) -> Any:
    """Pop one canonical field and its aliases, refusing disagreement."""

    values = [(name, raw.pop(name)) for name in names if name in raw]
    if not values:
        if default is _MISSING:
            raise ValueError(f"{field_name} is required")
        return default
    first_name, first_value = values[0]
    if any(value != first_value for _, value in values[1:]):
        raise ValueError(f"{field_name} aliases disagree")
    return first_value


def _reject_unknown(raw: Mapping[str, Any], *, scope: str) -> None:
    if raw:
        names = ", ".join(sorted(str(key) for key in raw))
        raise ValueError(f"{scope} contains unknown fields: {names}")


@dataclass(frozen=True, slots=True)
class AssetReference:
    """A typed, owner-qualified edge in the asset graph."""

    owner: str
    asset_id: str
    kind: str
    version: str | None = None
    digest: str | None = None
    cardinality: Literal["one", "optional", "many"] = "one"
    required_capabilities: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.owner, field_name="reference.owner")
        _identifier(self.asset_id, field_name="reference.id")
        _identifier(self.kind, field_name="reference.kind")
        if self.version is not None:
            _identifier(self.version, field_name="reference.version")
        if self.digest is not None:
            _digest(self.digest, field_name="reference.digest")
        if self.cardinality not in _CARDINALITIES:
            raise ValueError(f"reference.cardinality must be one of {sorted(_CARDINALITIES)}")
        object.__setattr__(self, "required_capabilities", _tuple_strings(self.required_capabilities, field_name="reference.required_capabilities"))

    @property
    def qualified_id(self) -> str:
        return f"{self.owner}:{self.asset_id}"

    @property
    def id(self) -> str:
        return self.asset_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "owner": self.owner,
            "id": self.asset_id,
            "kind": self.kind,
            "version": self.version,
            "digest": self.digest,
            "cardinality": self.cardinality,
            "required_capabilities": list(self.required_capabilities),
        }

    @classmethod
    def from_value(cls, value: object, *, default_kind: str = "asset") -> "AssetReference":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            qualified = _qualified(value, field_name="reference")
            owner, asset_id = qualified.split(":", 1)
            return cls(owner=owner, asset_id=asset_id, kind=default_kind)
        if not isinstance(value, Mapping):
            raise ValueError("asset references must be mappings")
        raw = dict(value)
        qualified = _pop_alias(raw, "qualified_id", "ref", default=None, field_name="reference.qualified_id")
        raw_id = _pop_alias(raw, "id", "asset_id", default=None, field_name="reference.id")
        owner = _pop_alias(raw, "owner", default=None, field_name="reference.owner")
        if qualified is not None:
            qualified = _qualified(qualified, field_name="reference.qualified_id")
            parsed_owner, parsed_id = qualified.split(":", 1)
            if owner is not None and owner != parsed_owner:
                raise ValueError("reference owner disagrees with qualified_id")
            owner, raw_id = parsed_owner, parsed_id
        kind = _pop_alias(raw, "kind", default=default_kind, field_name="reference.kind")
        version = _pop_alias(raw, "version", default=None, field_name="reference.version")
        digest = _pop_alias(raw, "digest", default=None, field_name="reference.digest")
        cardinality = _pop_alias(raw, "cardinality", default="one", field_name="reference.cardinality")
        capabilities = _pop_alias(
            raw,
            "required_capabilities",
            "capabilities",
            default=(),
            field_name="reference.required_capabilities",
        )
        _reject_unknown(raw, scope="asset reference")
        return cls(
            owner=_identifier(owner, field_name="reference.owner"),
            asset_id=_identifier(raw_id, field_name="reference.id"),
            kind=_identifier(kind, field_name="reference.kind"),
            version=version,
            digest=digest,
            cardinality=cardinality,
            required_capabilities=capabilities,
        )


@dataclass(frozen=True, slots=True)
class OverrideSlot:
    """A declared typed value slot; arbitrary document pointers are absent."""

    slot_id: str
    value_type: str
    owner: str
    schema: Mapping[str, Any] = field(default_factory=dict)
    default: Any = None
    has_default: bool = False

    def __post_init__(self) -> None:
        _identifier(self.slot_id, field_name="override_slot.slot_id")
        _identifier(self.value_type, field_name="override_slot.value_type")
        _identifier(self.owner, field_name="override_slot.owner")
        object.__setattr__(self, "schema", MappingProxyType(dict(self.schema)))

    @property
    def qualified_id(self) -> str:
        return f"{self.owner}:{self.slot_id}"

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "owner": self.owner,
            "slot_id": self.slot_id,
            "value_type": self.value_type,
            "schema": dict(self.schema),
        }
        if self.has_default:
            result["default"] = self.default
        return result

    @classmethod
    def from_value(cls, value: object, *, owner: str) -> "OverrideSlot":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ValueError("override slots must be mappings")
        raw = dict(value)
        slot_id = _pop_alias(raw, "slot_id", "id", field_name="override_slot.slot_id")
        value_type = _pop_alias(raw, "value_type", "type", field_name="override_slot.value_type")
        slot_owner = _pop_alias(raw, "owner", default=owner, field_name="override_slot.owner")
        schema = _pop_alias(raw, "schema", default={}, field_name="override_slot.schema")
        has_default = "default" in raw
        default = raw.pop("default", None)
        _reject_unknown(raw, scope="override slot")
        return cls(
            slot_id=slot_id,
            value_type=value_type,
            owner=slot_owner,
            schema=_mapping(schema, field_name="override_slot.schema"),
            default=default,
            has_default=has_default,
        )


@dataclass(frozen=True, slots=True)
class AssetKindSpec:
    """Closed registry metadata for one declarative asset kind."""

    kind: str
    schema: str
    plane: str
    config_schema: str | None = None
    result_schema: str | None = None
    supported_versions: tuple[str, ...] = ()
    compatibility: Mapping[str, Any] = field(default_factory=dict)
    capabilities: tuple[str, ...] = ()
    reference_kinds: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    reference_cardinality: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    default_merge_rule: Literal["refuse", "dedupe", "overlay", "replace"] | None = None
    merge_identity: str = ""
    failure_rule: Literal["refuse", "dedupe", "overlay", "replace"] | None = None
    override_policy: Literal["none", "declared_slots"] = "none"

    def __post_init__(self) -> None:
        _identifier(self.kind, field_name="kind.kind")
        _schema_token(self.schema, field_name="kind.schema")
        if self.plane not in VALIDATION_PLANES:
            raise ValueError(f"kind.plane must be one of {VALIDATION_PLANES}")
        if self.config_schema is not None:
            _schema_token(self.config_schema, field_name="kind.config_schema")
        if self.result_schema is not None:
            _schema_token(self.result_schema, field_name="kind.result_schema")
        if not self.merge_identity:
            raise ValueError("kind.merge_identity is required")
        _identifier(self.merge_identity, field_name="kind.merge_identity")
        object.__setattr__(self, "supported_versions", _tuple_strings(self.supported_versions, field_name="kind.supported_versions"))
        object.__setattr__(self, "compatibility", MappingProxyType(dict(self.compatibility)))
        object.__setattr__(self, "capabilities", _tuple_strings(self.capabilities, field_name="kind.capabilities"))
        if self.default_merge_rule is None:
            raise ValueError("kind.default_merge_rule is required")
        if self.default_merge_rule not in _MERGE_RULES:
            raise ValueError(f"kind.default_merge_rule must be one of {sorted(_MERGE_RULES)}")
        if self.failure_rule is None:
            raise ValueError("kind.failure_rule is required")
        if self.failure_rule not in _MERGE_RULES:
            raise ValueError(f"kind.failure_rule must be one of {sorted(_MERGE_RULES)}")
        if self.override_policy not in {"none", "declared_slots"}:
            raise ValueError("kind.override_policy must be 'none' or 'declared_slots'")
        normalized = {
            _identifier(key, field_name="reference_kinds.kind"): _tuple_strings(values, field_name="reference_kinds.values")
            for key, values in self.reference_kinds.items()
        }
        object.__setattr__(self, "reference_kinds", MappingProxyType(normalized))
        cardinalities = {
            _identifier(key, field_name="reference_cardinality.kind"): tuple(
                sorted(set(values))
            )
            for key, values in self.reference_cardinality.items()
        }
        for key, values in cardinalities.items():
            if key not in normalized:
                raise ValueError(f"reference_cardinality declares unknown kind {key!r}")
            if not set(values) <= _CARDINALITIES:
                raise ValueError(f"reference_cardinality.{key} contains an unsupported cardinality")
        object.__setattr__(self, "reference_cardinality", MappingProxyType(cardinalities))

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "schema": self.schema,
            "plane": self.plane,
            "config_schema": self.config_schema,
            "result_schema": self.result_schema,
            "supported_versions": list(self.supported_versions),
            "compatibility": dict(self.compatibility),
            "capabilities": list(self.capabilities),
            "reference_kinds": {key: list(value) for key, value in self.reference_kinds.items()},
            "reference_cardinality": {key: list(value) for key, value in self.reference_cardinality.items()},
            "default_merge_rule": self.default_merge_rule,
            "merge_identity": self.merge_identity,
            "failure_rule": self.failure_rule,
            "override_policy": self.override_policy,
        }

    @classmethod
    def from_value(cls, value: object) -> "AssetKindSpec":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ValueError("asset kind declarations must be mappings")
        raw = dict(value)
        kind = _pop_alias(raw, "kind", field_name="kind.kind")
        schema = _pop_alias(raw, "schema", "schema_version", field_name="kind.schema")
        plane = _pop_alias(raw, "plane", field_name="kind.plane")
        config_schema = _pop_alias(raw, "config_schema", default=None, field_name="kind.config_schema")
        result_schema = _pop_alias(raw, "result_schema", default=None, field_name="kind.result_schema")
        supported_versions = _pop_alias(raw, "supported_versions", default=(), field_name="kind.supported_versions")
        compatibility = _pop_alias(raw, "compatibility", default={}, field_name="kind.compatibility")
        capabilities = _pop_alias(raw, "capabilities", default=(), field_name="kind.capabilities")
        reference_kinds = _pop_alias(raw, "reference_kinds", default={}, field_name="kind.reference_kinds")
        reference_cardinality = _pop_alias(raw, "reference_cardinality", default={}, field_name="kind.reference_cardinality")
        default_merge_rule = _pop_alias(raw, "default_merge_rule", "merge_rule", default=None, field_name="kind.default_merge_rule")
        merge_identity = _pop_alias(raw, "merge_identity", field_name="kind.merge_identity")
        failure_rule = _pop_alias(raw, "failure_rule", default=None, field_name="kind.failure_rule")
        override_policy = _pop_alias(raw, "override_policy", default="none", field_name="kind.override_policy")
        _reject_unknown(raw, scope="asset kind")
        return cls(
            kind=kind,
            schema=schema,
            plane=plane,
            config_schema=config_schema,
            result_schema=result_schema,
            supported_versions=supported_versions,
            compatibility=compatibility,
            capabilities=capabilities,
            reference_kinds=reference_kinds,
            reference_cardinality=reference_cardinality,
            default_merge_rule=default_merge_rule,
            merge_identity=merge_identity,
            failure_rule=failure_rule,
            override_policy=override_policy,
        )


@dataclass(frozen=True, slots=True, init=False)
class TypedAsset:
    """One fully identified declarative asset.

    ``asset_id`` and ``id`` are accepted as aliases, as are ``refs`` and
    ``references``.  The canonical identity exposed by the object is always
    ``owner:asset_id``.
    """

    owner: str
    asset_id: str
    kind: str
    plane: str | None
    version: str
    digest: str
    schema: str | None
    references: tuple[AssetReference, ...]
    capabilities: tuple[str, ...]
    compatibility: Mapping[str, Any]
    merge_identity: str
    merge_rule: Literal["refuse", "dedupe", "overlay", "replace"]
    override_slots: tuple[OverrideSlot, ...]
    state: Literal["enabled", "disabled", "tombstone"]
    provenance: Mapping[str, Any]

    def __init__(self, *, owner: str | None = None, asset_id: str | None = None, id: str | None = None,
                 qualified_id: str | None = None, kind: str, plane: str | None = None, version: str, digest: str,
                 schema: str | None = None, references: Sequence[AssetReference | Mapping[str, Any]] = (),
                 refs: Sequence[AssetReference | Mapping[str, Any]] | None = None,
                 capabilities: Sequence[str] = (), compatibility: Mapping[str, Any] | None = None,
                 merge_identity: str | None = None, merge_rule: str | None = None,
                 conflict_rule: str | None = None, failure_rule: str | None = None,
                 override_slots: Sequence[OverrideSlot | Mapping[str, Any]] = (),
                 state: str = "enabled", enabled: bool | None = None,
                 tombstone: bool = False, provenance: Mapping[str, Any] | None = None) -> None:
        if qualified_id is not None:
            qualified_id = _qualified(qualified_id)
            qualified_owner, qualified_asset_id = qualified_id.split(":", 1)
            if owner is not None and owner != qualified_owner:
                raise ValueError("asset owner disagrees with qualified_id")
            if asset_id is not None and asset_id != qualified_asset_id:
                raise ValueError("asset id disagrees with qualified_id")
            owner, asset_id = qualified_owner, qualified_asset_id
        if asset_id is None:
            asset_id = id
        if owner is None and isinstance(asset_id, str) and ":" in asset_id:
            qualified = _qualified(asset_id, field_name="asset.id")
            owner, asset_id = qualified.split(":", 1)
        if owner is None:
            raise ValueError("asset owner is required")
        if asset_id is None:
            raise ValueError("asset id is required")
        if enabled is not None:
            state = "enabled" if enabled else "disabled"
        if tombstone:
            state = "tombstone"
        if state not in _STATES:
            raise ValueError(f"asset state must be one of {sorted(_STATES)}")
        rule = failure_rule or conflict_rule or merge_rule
        if schema is None:
            raise ValueError("asset.schema is required")
        if merge_identity is None:
            raise ValueError("asset.merge_identity is required")
        if rule is None:
            raise ValueError("asset.merge_rule or asset.failure_rule is required")
        if rule not in _MERGE_RULES:
            raise ValueError(f"asset merge_rule must be one of {sorted(_MERGE_RULES)}")
        checked_owner = _identifier(owner, field_name="asset.owner")
        checked_id = _identifier(asset_id, field_name="asset.id")
        checked_kind = _identifier(kind, field_name="asset.kind")
        if plane is not None and plane not in VALIDATION_PLANES:
            raise ValueError(f"asset.plane must be one of {VALIDATION_PLANES}")
        checked_version = _identifier(version, field_name="asset.version")
        checked_digest = _digest(digest, field_name="asset.digest")
        slots = tuple(OverrideSlot.from_value(slot, owner=checked_owner) for slot in override_slots)
        slot_ids = [slot.slot_id for slot in slots]
        if len(slot_ids) != len(set(slot_ids)):
            raise ValueError(f"override slots for {checked_owner}:{checked_id} must be unique")
        edges = tuple(
            sorted(
                (
                    ref if isinstance(ref, AssetReference) else AssetReference.from_value(ref)
                    for ref in (refs if refs is not None else references)
                ),
                key=lambda item: (item.owner, item.asset_id, item.kind, item.version or "", item.digest or "", item.cardinality),
            )
        )
        object.__setattr__(self, "owner", checked_owner)
        object.__setattr__(self, "asset_id", checked_id)
        object.__setattr__(self, "kind", checked_kind)
        object.__setattr__(self, "plane", plane)
        object.__setattr__(self, "version", checked_version)
        object.__setattr__(self, "digest", checked_digest)
        object.__setattr__(self, "schema", _schema_token(schema, field_name="asset.schema"))
        object.__setattr__(self, "references", edges)
        object.__setattr__(self, "capabilities", _tuple_strings(capabilities, field_name="asset.capabilities"))
        object.__setattr__(self, "compatibility", MappingProxyType(dict(compatibility or {})))
        object.__setattr__(self, "merge_identity", _identifier(merge_identity, field_name="asset.merge_identity"))
        object.__setattr__(self, "merge_rule", rule)
        object.__setattr__(self, "override_slots", slots)
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "provenance", MappingProxyType(dict(provenance or {})))

    @property
    def qualified_id(self) -> str:
        return f"{self.owner}:{self.asset_id}"

    @property
    def id(self) -> str:
        return self.asset_id

    @classmethod
    def from_value(cls, value: object) -> "TypedAsset":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ValueError("typed assets must be mappings")
        raw = dict(value)
        owner = _pop_alias(raw, "owner", default=None, field_name="asset.owner")
        asset_id = _pop_alias(raw, "asset_id", "id", default=None, field_name="asset.id")
        qualified_id = _pop_alias(raw, "qualified_id", default=None, field_name="asset.qualified_id")
        kind = _pop_alias(raw, "kind", field_name="asset.kind")
        plane = _pop_alias(raw, "plane", default=None, field_name="asset.plane")
        version = _pop_alias(raw, "version", field_name="asset.version")
        digest = _pop_alias(raw, "digest", field_name="asset.digest")
        schema = _pop_alias(raw, "schema", "schema_version", default=None, field_name="asset.schema")
        references = _pop_alias(raw, "references", "refs", default=(), field_name="asset.references")
        capabilities = _pop_alias(raw, "capabilities", default=(), field_name="asset.capabilities")
        compatibility = _pop_alias(raw, "compatibility", default={}, field_name="asset.compatibility")
        merge_identity = _pop_alias(raw, "merge_identity", default=None, field_name="asset.merge_identity")
        merge_rule = _pop_alias(raw, "merge_rule", "conflict_rule", default=None, field_name="asset.merge_rule")
        failure_rule = _pop_alias(raw, "failure_rule", default=None, field_name="asset.failure_rule")
        override_slots = _pop_alias(raw, "override_slots", "slots", default=(), field_name="asset.override_slots")
        state = _pop_alias(raw, "state", default="enabled", field_name="asset.state")
        enabled = _pop_alias(raw, "enabled", default=None, field_name="asset.enabled")
        tombstone = _pop_alias(raw, "tombstone", default=False, field_name="asset.tombstone")
        provenance = _pop_alias(raw, "provenance", default={}, field_name="asset.provenance")
        _reject_unknown(raw, scope="typed asset")
        return cls(
            owner=owner,
            asset_id=asset_id,
            qualified_id=qualified_id,
            kind=kind,
            plane=plane,
            version=version,
            digest=digest,
            schema=schema,
            references=references,
            capabilities=capabilities,
            compatibility=compatibility,
            merge_identity=merge_identity,
            merge_rule=merge_rule,
            failure_rule=failure_rule,
            override_slots=override_slots,
            state=state,
            enabled=enabled,
            tombstone=tombstone,
            provenance=provenance,
        )

    def reference(self) -> OwnerQualifiedReference:
        return OwnerQualifiedReference(owner=self.owner, id=self.asset_id, kind=self.kind, version=self.version, digest=self.digest)

    @property
    def typed_refs(self) -> tuple[AssetReference, ...]:
        return self.references

    @property
    def conflict_rule(self) -> str:
        return self.merge_rule

    @property
    def failure_rule(self) -> str:
        return self.merge_rule

    def to_dict(self) -> dict[str, Any]:
        return {
            "owner": self.owner,
            "id": self.asset_id,
            "qualified_id": self.qualified_id,
            "kind": self.kind,
            "plane": self.plane,
            "version": self.version,
            "digest": self.digest,
            "schema": self.schema,
            "references": [item.to_dict() for item in self.references],
            "capabilities": list(self.capabilities),
            "compatibility": dict(self.compatibility),
            "merge_identity": self.merge_identity,
            "merge_rule": self.merge_rule,
            "failure_rule": self.failure_rule,
            "override_slots": [item.to_dict() for item in self.override_slots],
            "state": self.state,
            "provenance": dict(self.provenance),
        }


@dataclass(frozen=True, slots=True)
class AssetProvenance:
    path: str
    owner: str
    reference: AssetReference
    value_digest: str
    source: Literal[
        "distribution", "bundle", "definition", "local", "package", "environment",
        "compatibility", "asset", "override", "disable", "tombstone",
    ] = "local"
    old_owner: str | None = None
    new_owner: str | None = None
    old_value_digest: str | None = None
    new_value_digest: str | None = None
    reason: str | None = None
    references: tuple[AssetReference, ...] = ()
    merge_decision: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "owner": self.owner,
            "source": self.source,
            "reference": {
                "owner": self.reference.owner,
                "id": self.reference.asset_id,
                "kind": self.reference.kind,
                "version": self.reference.version,
                "digest": self.reference.digest,
            },
            "value_digest": self.value_digest,
            "old_owner": self.old_owner,
            "new_owner": self.new_owner,
            "old_value_digest": self.old_value_digest,
            "new_value_digest": self.new_value_digest,
            "reason": self.reason,
            "references": [item.to_dict() for item in self.references],
            "merge_decision": self.merge_decision,
        }


@dataclass(frozen=True, slots=True)
class OverrideDecision:
    slot: OverrideSlot
    old_owner: str | None
    new_owner: str
    old_value_digest: str | None
    new_value_digest: str
    reason: str
    references: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "slot_id": self.slot.qualified_id,
            "old_owner": self.old_owner,
            "new_owner": self.new_owner,
            "old_value_digest": self.old_value_digest,
            "new_value_digest": self.new_value_digest,
            "reason": self.reason,
            "references": list(self.references),
        }


@dataclass(frozen=True, slots=True)
class AssetDiagnostic:
    """Stable machine-readable refusal detail."""

    code: str
    chain: tuple[str, ...] = ()
    asset_id: str | None = None
    owner: str | None = None
    expected: str | None = None
    observed: str | None = None
    message: str = ""
    fix_hint: str = ""

    @property
    def path(self) -> str:
        return " -> ".join(self.chain)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "chain": list(self.chain),
            "path": self.path,
            "asset_id": self.asset_id,
            "owner": self.owner,
            "expected": self.expected,
            "observed": self.observed,
            "message": self.message,
            "fix_hint": self.fix_hint,
            "phase": "pre_mutation",
        }


class TypedAssetResolutionError(ValueError):
    """Resolution refusal; diagnostics are sorted and never partially applied."""

    def __init__(self, diagnostics: Sequence[AssetDiagnostic]) -> None:
        ordered = tuple(sorted(diagnostics, key=lambda item: (item.code, item.path, item.asset_id or "")))
        if not ordered:
            raise ValueError("a typed asset resolution error needs diagnostics")
        self.diagnostics = ordered
        primary_codes = (
            "digest_collision",
            "reference_kind_undeclared",
            "reference_cardinality_undeclared",
        )
        self.code = next(
            (code for code in primary_codes for item in ordered if item.code == code),
            ordered[0].code,
        )
        super().__init__("; ".join(item.message or item.code for item in ordered))


# Compatibility aliases make the failure type discoverable under the names
# used by older deployment-product callers.
TypedAssetError = TypedAssetResolutionError
AssetResolutionError = TypedAssetResolutionError


@dataclass(frozen=True, slots=True)
class AssetResolution:
    ordered_assets: tuple[TypedAsset, ...]
    effective_assets: tuple[TypedAsset, ...]
    disabled_assets: tuple[TypedAsset, ...]
    tombstones: tuple[TypedAsset, ...]
    provenance: tuple[AssetProvenance, ...]
    overrides: tuple[OverrideDecision, ...]
    merge_decisions: tuple[Mapping[str, Any], ...]
    composition_digest: str
    schema: str = TYPED_ASSET_GRAPH_SCHEMA

    @property
    def assets(self) -> tuple[TypedAsset, ...]:
        return self.effective_assets

    @property
    def references(self) -> tuple[OwnerQualifiedReference, ...]:
        return tuple(asset.reference() for asset in self.effective_assets)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "asset_records": [asset.to_dict() for asset in self.effective_assets],
            "assets": [asset.qualified_id for asset in self.effective_assets],
            "ordered_assets": [asset.qualified_id for asset in self.ordered_assets],
            "disabled_assets": [asset.qualified_id for asset in self.disabled_assets],
            "tombstones": [asset.qualified_id for asset in self.tombstones],
            "provenance": [item.to_dict() for item in self.provenance],
            "overrides": [item.to_dict() for item in self.overrides],
            "merge_decisions": [dict(item) for item in self.merge_decisions],
            "composition_digest": self.composition_digest,
        }

    def lock_provenance(self) -> tuple[ProvenanceRecord, ...]:
        """Adapt graph history to the Wave 1 lock's canonical shape.

        Wave 1 intentionally has one compact provenance record shape.  Rather
        than adding a competing lock type, relationship history is represented
        as deterministic sibling records: ``.previous`` and ``.reason`` for
        an override, and ``.refs`` plus a versioned ``merge-decision``
        reference for a merge.  Existing readers can therefore continue to
        consume ``path/owner/reference/source/value_digest`` while inspection
        callers see the full history.
        """

        records: list[ProvenanceRecord] = []
        for item in self.provenance:
            source = item.source if item.source in _LOCK_SOURCES or item.source == "override" else "local"
            reference = OwnerQualifiedReference(
                owner=item.reference.owner,
                id=item.reference.asset_id,
                kind=("merge-decision" if item.merge_decision else item.reference.kind),
                version=(item.merge_decision or item.reference.version),
                digest=item.reference.digest,
            )
            records.append(
                ProvenanceRecord(
                    path=item.path,
                    owner=item.owner,
                    reference=reference,
                    source=source,
                    value_digest=item.value_digest,
                )
            )
            for child in item.references:
                records.append(ProvenanceRecord(
                    path=f"{item.path}.refs.{child.owner}:{child.asset_id}",
                    owner=child.owner,
                    reference=OwnerQualifiedReference(
                        owner=child.owner,
                        id=child.asset_id,
                        kind=child.kind,
                        version=child.version,
                        digest=child.digest,
                    ),
                    source=source,
                    value_digest=child.digest or item.value_digest,
                ))
            if item.source == "override":
                if item.old_owner and item.old_value_digest:
                    records.append(ProvenanceRecord(
                        path=f"{item.path}.previous",
                        owner=item.old_owner,
                        reference=OwnerQualifiedReference(
                            owner=item.old_owner,
                            id=f"{item.reference.asset_id}-previous",
                            kind="override-previous",
                            version="v1",
                            digest=item.old_value_digest,
                        ),
                        source="local",
                        value_digest=item.old_value_digest,
                    ))
                if item.reason:
                    reason_digest = _value_digest(item.reason)
                    records.append(ProvenanceRecord(
                        path=f"{item.path}.reason",
                        owner=item.new_owner or item.owner,
                        reference=OwnerQualifiedReference(
                            owner=item.new_owner or item.owner,
                            id=f"{item.reference.asset_id}-reason",
                            kind="override-reason",
                            version="v1",
                            digest=reason_digest,
                        ),
                        source="override",
                        value_digest=reason_digest,
                    ))
        for asset in (*self.disabled_assets, *self.tombstones):
            records.append(
                ProvenanceRecord(
                    path=f"assets.{asset.state}.{asset.qualified_id}",
                    owner=asset.owner,
                    # Wave 1's record is intentionally closed: state is
                    # carried by the sibling path and typed state reference
                    # rather than an unsupported ad-hoc ``reason`` field.
                    reference=OwnerQualifiedReference(
                        owner=asset.owner,
                        id=asset.asset_id,
                        kind="asset-state",
                        version=asset.state,
                        digest=asset.digest,
                    ),
                    source="local",
                    value_digest=asset.digest,
                )
            )
        return tuple(records)


class MutationCallback(Protocol):
    def __call__(self, resolution: AssetResolution) -> object:
        raise NotImplementedError


def _default_kinds() -> tuple[AssetKindSpec, ...]:
    semantic = {
        "ontology", "schema", "bridge", "narrowing", "alias", "identifier-pattern",
        "concept-seed", "identity-policy", "catalog-migration",
    }
    ingestion = {
        "connector", "connection-type", "source-template", "source-binding",
        "extraction-pipeline", "link-policy", "source", "embedding", "refresher",
    }
    derivation = {"enricher", "invariant", "consistency-gate", "derived-producer"}
    views = {"view", "read-model", "lens", "dashboard"}
    agent = {"interface", "prompt", "skill", "playbook", "tool", "resolver", "search", "chat-policy", "vocabulary"}
    return tuple(
        AssetKindSpec(
            kind=kind,
            schema=f"okg.deployment_product.asset.{kind}/v1",
            plane=(
                "semantic-catalog" if kind in semantic else
                "ingestion-projection" if kind in ingestion else
                "derivation-publication" if kind in derivation else
                "views-read-models" if kind in views else
                "agent-ui" if kind in agent else
                "operations-governance"
            ),
            config_schema=f"okg.deployment_product.asset.{kind}.config/v1",
            result_schema=f"okg.deployment_product.asset.{kind}.result/v1",
            supported_versions=("v1",),
            compatibility={"contract": "v1"},
            capabilities=("declarative", "read"),
            reference_kinds={reference_kind: CORE_ASSET_KINDS for reference_kind in CORE_ASSET_KINDS},
            reference_cardinality={reference_kind: tuple(sorted(_CARDINALITIES)) for reference_kind in CORE_ASSET_KINDS},
            default_merge_rule="refuse",
            merge_identity="qualified_id",
            failure_rule="refuse",
            override_policy="declared_slots",
        )
        for kind in CORE_ASSET_KINDS
    )


class TypedAssetRegistry:
    """A closed registry of asset kinds and owner-qualified asset identities."""

    def __init__(self, kinds: Iterable[AssetKindSpec | Mapping[str, Any]] | None = None) -> None:
        self._kinds: dict[str, AssetKindSpec] = {}
        self._assets: dict[str, TypedAsset] = {}
        for kind in (_default_kinds() if kinds is None else kinds):
            self.register_kind(kind)

    @property
    def schema(self) -> str:
        return TYPED_ASSET_REGISTRY_SCHEMA

    @property
    def kinds(self) -> Mapping[str, AssetKindSpec]:
        return MappingProxyType(dict(self._kinds))

    @property
    def assets(self) -> Mapping[str, TypedAsset]:
        return MappingProxyType(dict(self._assets))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "kinds": [self._kinds[key].to_dict() for key in sorted(self._kinds)],
            "assets": [self._assets[key].to_dict() for key in sorted(self._assets)],
        }

    def register_kind(self, kind: AssetKindSpec | Mapping[str, Any]) -> AssetKindSpec:
        spec = AssetKindSpec.from_value(kind)
        prior = self._kinds.get(spec.kind)
        if prior is not None and prior != spec:
            raise TypedAssetResolutionError((AssetDiagnostic(
                code="duplicate_kind",
                asset_id=spec.kind,
                expected=prior.schema,
                observed=spec.schema,
                message=f"asset kind {spec.kind!r} is declared with incompatible schemas",
                fix_hint="register one schema and version policy for the kind",
            ),))
        self._kinds[spec.kind] = spec
        return spec

    def register(self, asset: TypedAsset | Mapping[str, Any]) -> TypedAsset:
        checked = TypedAsset.from_value(asset)
        kind = self._kinds.get(checked.kind)
        if kind is None:
            raise TypedAssetResolutionError((AssetDiagnostic(
                code="unregistered_asset_kind",
                asset_id=checked.qualified_id,
                owner=checked.owner,
                observed=checked.kind,
                message=f"asset {checked.qualified_id!r} uses unregistered kind {checked.kind!r}",
                fix_hint="register the declarative kind before selecting the asset",
            ),))
        if kind.schema and checked.schema not in (None, kind.schema):
            raise TypedAssetResolutionError((AssetDiagnostic(
                code="schema_mismatch",
                asset_id=checked.qualified_id,
                owner=checked.owner,
                expected=kind.schema,
                observed=checked.schema,
                message=f"asset {checked.qualified_id!r} does not match its kind schema",
                fix_hint="publish the asset using the registered schema",
            ),))
        if checked.plane is None:
            raise TypedAssetResolutionError((AssetDiagnostic(
                code="validation_plane_missing",
                asset_id=checked.qualified_id,
                owner=checked.owner,
                expected=kind.plane,
                message=f"asset {checked.qualified_id!r} must declare its validation plane",
                fix_hint=f"set plane to {kind.plane!r}",
            ),))
        if checked.plane != kind.plane:
            raise TypedAssetResolutionError((AssetDiagnostic(
                code="validation_plane_mismatch",
                asset_id=checked.qualified_id,
                owner=checked.owner,
                expected=kind.plane,
                observed=checked.plane,
                message=f"asset {checked.qualified_id!r} is assigned to the wrong validation plane",
                fix_hint=f"set plane to {kind.plane!r}",
            ),))
        declared_source = checked.provenance.get("source", "local")
        if declared_source not in _LOCK_SOURCES:
            raise TypedAssetResolutionError((AssetDiagnostic(
                code="provenance_source_invalid",
                asset_id=checked.qualified_id,
                owner=checked.owner,
                expected=", ".join(sorted(_LOCK_SOURCES)),
                observed=str(declared_source),
                message=f"asset {checked.qualified_id!r} declares an unsupported provenance source",
                fix_hint="declare distribution, bundle, definition, local, package, environment, or compatibility provenance",
            ),))
        if kind.supported_versions and checked.version not in kind.supported_versions:
            raise TypedAssetResolutionError((AssetDiagnostic(
                code="unsupported_version",
                asset_id=checked.qualified_id,
                owner=checked.owner,
                expected=", ".join(kind.supported_versions),
                observed=checked.version,
                message=f"asset {checked.qualified_id!r} uses unsupported version {checked.version!r}",
                fix_hint="migrate the asset to a registered version",
            ),))
        missing_caps = sorted(set(checked.capabilities) - set(kind.capabilities))
        if missing_caps:
            raise TypedAssetResolutionError((AssetDiagnostic(
                code="unsupported_capability",
                asset_id=checked.qualified_id,
                owner=checked.owner,
                expected=", ".join(kind.capabilities),
                observed=", ".join(missing_caps),
                message=f"asset {checked.qualified_id!r} requests unsupported capability {missing_caps[0]!r}",
                fix_hint="register the capability or remove it from the asset",
            ),))
        reference_diagnostics: list[AssetDiagnostic] = []
        for reference in checked.references:
            allowed_kinds = kind.reference_kinds.get(reference.kind)
            if allowed_kinds is None:
                reference_diagnostics.append(AssetDiagnostic(
                    code="reference_kind_undeclared",
                    asset_id=checked.qualified_id,
                    owner=checked.owner,
                    expected=", ".join(sorted(kind.reference_kinds)),
                    observed=reference.kind,
                    message=f"asset {checked.qualified_id!r} references undeclared kind {reference.kind!r}",
                    fix_hint="declare the referenced kind in the registered kind policy",
                ))
            elif allowed_kinds is not None and reference.kind not in allowed_kinds:
                reference_diagnostics.append(AssetDiagnostic(
                    code="reference_kind_unregistered",
                    asset_id=checked.qualified_id,
                    owner=checked.owner,
                    expected=", ".join(allowed_kinds),
                    observed=reference.kind,
                    message=f"asset {checked.qualified_id!r} references disallowed kind {reference.kind!r}",
                    fix_hint="declare the referenced kind in the registered kind policy",
                ))
            allowed_cardinality = kind.reference_cardinality.get(reference.kind)
            if allowed_cardinality is None:
                reference_diagnostics.append(AssetDiagnostic(
                    code="reference_cardinality_undeclared",
                    asset_id=checked.qualified_id,
                    owner=checked.owner,
                    expected=", ".join(sorted(kind.reference_cardinality)),
                    observed=reference.cardinality,
                    message=f"asset {checked.qualified_id!r} has no cardinality policy for {reference.kind!r}",
                    fix_hint="declare the referenced kind's allowed cardinalities",
                ))
            elif allowed_cardinality is not None and reference.cardinality not in allowed_cardinality:
                reference_diagnostics.append(AssetDiagnostic(
                    code="reference_cardinality_invalid",
                    asset_id=checked.qualified_id,
                    owner=checked.owner,
                    expected=", ".join(allowed_cardinality),
                    observed=reference.cardinality,
                    message=f"asset {checked.qualified_id!r} uses unsupported cardinality for {reference.kind!r}",
                    fix_hint="use one of the registered cardinalities for this typed reference",
                ))
        if reference_diagnostics:
            raise TypedAssetResolutionError(reference_diagnostics)
        prior = self._assets.get(checked.qualified_id)
        if prior is not None:
            if prior.digest == checked.digest:
                if prior.to_dict() == checked.to_dict():
                    return prior
                raise TypedAssetResolutionError((AssetDiagnostic(
                    code="duplicate_declaration_mismatch",
                    asset_id=checked.qualified_id,
                    owner=checked.owner,
                    expected="canonical declaration equality",
                    observed="canonical declaration differs",
                    message=f"asset {checked.qualified_id!r} repeats its digest with a different declaration",
                    fix_hint="deduplicate the complete declaration or change the qualified ID",
                ),))
            raise TypedAssetResolutionError((AssetDiagnostic(
                code="digest_collision",
                asset_id=checked.qualified_id,
                owner=checked.owner,
                expected=prior.digest,
                observed=checked.digest,
                message=f"asset {checked.qualified_id!r} is registered with conflicting digests",
                fix_hint="select one content digest for this owner-qualified ID",
            ), AssetDiagnostic(
                code="duplicate_id",
                asset_id=checked.qualified_id,
                owner=checked.owner,
                expected=prior.digest,
                observed=checked.digest,
                message=f"asset {checked.qualified_id!r} is declared more than once",
                fix_hint="deduplicate identical declarations or change the qualified ID",
            )))
        self._assets[checked.qualified_id] = checked
        return checked

    def register_many(self, assets: Iterable[TypedAsset | Mapping[str, Any]]) -> tuple[TypedAsset, ...]:
        # Registration itself is local, but caller-visible state must not be
        # partially changed by a malformed batch.  Validate in a shadow
        # registry and commit only once all entries pass.
        shadow = TypedAssetRegistry(self._kinds.values())
        shadow._assets.update(self._assets)
        checked: list[TypedAsset] = []
        for asset in assets:
            checked.append(shadow.register(asset))
        self._assets = shadow._assets
        return tuple(checked)

    def resolve(
        self,
        roots: Sequence[AssetReference | Mapping[str, Any] | str] | None = None,
        *,
        overrides: Sequence[TypedOverrideInput | Mapping[str, Any]] = (),
        mutation: MutationCallback | None = None,
        mutation_observer: MutationCallback | None = None,
    ) -> AssetResolution:
        """Validate and topologically resolve selected assets.

        ``mutation`` is deliberately invoked after the result has been fully
        built and digest-bound.  Any refusal raises before invocation.
        """

        selected: list[AssetReference] = []
        if roots is None:
            selected = [
                AssetReference(owner=item.owner, asset_id=item.asset_id, kind=item.kind, version=item.version, digest=item.digest)
                for item in sorted(self._assets.values(), key=lambda item: item.qualified_id)
            ]
        else:
            for root in roots:
                if isinstance(root, str):
                    qualified = _qualified(root, field_name="root")
                    owner, asset_id = qualified.split(":", 1)
                    asset = self._assets.get(qualified)
                    selected.append(AssetReference(owner=owner, asset_id=asset_id, kind=asset.kind if asset else "asset", version=asset.version if asset else None, digest=asset.digest if asset else None))
                else:
                    selected.append(AssetReference.from_value(root))
        diagnostics: list[AssetDiagnostic] = []
        ordered: list[TypedAsset] = []
        seen: set[str] = set()
        visiting: list[str] = []

        def visit(reference: AssetReference, chain: tuple[str, ...]) -> None:
            key = reference.qualified_id
            if key in visiting:
                cycle = (*chain, key)
                diagnostics.append(AssetDiagnostic(
                    code="reference_cycle", chain=cycle, asset_id=key,
                    message=f"typed asset reference cycle: {' -> '.join(cycle)}",
                    fix_hint="remove one reference in the cycle",
                ))
                return
            asset = self._assets.get(key)
            if asset is None:
                if reference.cardinality == "optional":
                    return
                missing_chain = (*chain, key)
                diagnostics.append(AssetDiagnostic(
                    code="missing_reference", chain=missing_chain, asset_id=key, owner=reference.owner,
                    expected=reference.digest, message=f"missing typed asset reference: {' -> '.join(missing_chain)}",
                    fix_hint="register the referenced owner-qualified asset",
                ))
                return
            if reference.kind != asset.kind:
                diagnostics.append(AssetDiagnostic(
                    code="reference_kind_mismatch", chain=(*chain, key), asset_id=key, owner=asset.owner,
                    expected=reference.kind, observed=asset.kind,
                    message=f"typed reference {key!r} expects kind {reference.kind!r}, found {asset.kind!r}",
                    fix_hint="correct the typed reference or publish the expected kind",
                ))
            if reference.version is not None and reference.version != asset.version:
                diagnostics.append(AssetDiagnostic(
                    code="unsupported_version", chain=(*chain, key), asset_id=key, owner=asset.owner,
                    expected=reference.version, observed=asset.version,
                    message=f"typed reference chain reaches unsupported version at {key!r}",
                    fix_hint="select the compatible asset version",
                ))
            if reference.digest is not None and reference.digest != asset.digest:
                diagnostics.append(AssetDiagnostic(
                    code="digest_collision", chain=(*chain, key), asset_id=key, owner=asset.owner,
                    expected=reference.digest, observed=asset.digest,
                    message=f"typed reference chain reaches a digest collision at {key!r}",
                    fix_hint="refresh the reference to the selected content digest",
                ))
            required = set(reference.required_capabilities)
            missing = sorted(required - set(asset.capabilities))
            if missing:
                diagnostics.append(AssetDiagnostic(
                    code="unsupported_capability", chain=(*chain, key), asset_id=key, owner=asset.owner,
                    expected=", ".join(sorted(required)), observed=", ".join(sorted(asset.capabilities)),
                    message=f"typed reference chain reaches {key!r} without capability {missing[0]!r}",
                    fix_hint="publish a capability-compatible asset",
                ))
            if asset.state == "disabled":
                if chain:
                    diagnostics.append(AssetDiagnostic(
                        code="disabled_reference", chain=(*chain, key), asset_id=key, owner=asset.owner,
                        message=f"enabled asset references disabled asset {key!r}",
                        fix_hint="enable the asset or make the reference optional",
                    ))
                return
            if asset.state == "tombstone":
                if chain:
                    diagnostics.append(AssetDiagnostic(
                        code="tombstone_reference", chain=(*chain, key), asset_id=key, owner=asset.owner,
                        message=f"enabled asset references tombstoned asset {key!r}",
                        fix_hint="remove the reference or publish a replacement asset",
                    ))
                return
            if key in seen:
                return
            visiting.append(key)
            for child in asset.references:
                visit(child, (*chain, key))
            visiting.pop()
            if key not in seen:
                seen.add(key)
                ordered.append(asset)

        for root in sorted(selected, key=lambda item: item.qualified_id):
            visit(root, ())

        effective = tuple(sorted(ordered, key=lambda item: item.qualified_id))
        # Merge identities are semantic ownership keys.  Their conflict policy
        # is explicit on the asset, never inferred from list order.
        by_merge: dict[tuple[str, str], list[TypedAsset]] = defaultdict(list)
        for asset in effective:
            by_merge[(asset.kind, asset.merge_identity)].append(asset)
        merge_decisions: list[Mapping[str, Any]] = []
        merge_provenance: list[AssetProvenance] = []
        for (kind, identity), grouped in sorted(by_merge.items()):
            grouped = sorted(grouped, key=lambda item: item.qualified_id)
            decision_name: str
            if len(grouped) == 1:
                decision_name = "accepted"
            else:
                digests = {item.digest for item in grouped}
                rules = {item.merge_rule for item in grouped}
                if len(digests) == 1 and rules <= {"dedupe", "overlay", "replace"}:
                    decision_name = "deduped"
                elif rules == {"overlay"}:
                    decision_name = "overlay"
                else:
                    diagnostics.append(AssetDiagnostic(
                        code="merge_collision", chain=tuple(item.qualified_id for item in grouped), asset_id=identity,
                        owner=grouped[0].owner, expected=grouped[0].digest, observed=grouped[-1].digest,
                        message=f"assets claim merge identity {kind}:{identity!s} with incompatible values",
                        fix_hint="declare an explicit overlay/dedupe rule or choose one owner",
                    ))
                    continue
            decision = MappingProxyType({
                "kind": kind,
                "merge_identity": identity,
                "decision": decision_name,
                "assets": tuple(item.qualified_id for item in grouped),
            })
            merge_decisions.append(decision)
            primary = grouped[0]
            merge_provenance.append(AssetProvenance(
                path=f"merges.{kind}.{identity}",
                owner=primary.owner,
                reference=AssetReference(owner=primary.owner, asset_id=primary.asset_id, kind=primary.kind, version=primary.version, digest=primary.digest),
                value_digest=_value_digest(dict(decision)),
                source=_asset_source(primary),
                references=tuple(
                    AssetReference(owner=item.owner, asset_id=item.asset_id, kind=item.kind, version=item.version, digest=item.digest)
                    for item in grouped
                ),
                merge_decision=decision_name,
            ))

        override_decisions, override_provenance = self._resolve_overrides(effective, overrides, diagnostics)
        if diagnostics:
            raise TypedAssetResolutionError(diagnostics)
        provenance = tuple(
            AssetProvenance(
                path=f"assets.{asset.qualified_id}", owner=asset.owner,
                reference=AssetReference(owner=asset.owner, asset_id=asset.asset_id, kind=asset.kind, version=asset.version, digest=asset.digest),
                value_digest=asset.digest,
                source=_asset_source(asset),
            )
            for asset in effective
        ) + tuple(merge_provenance) + tuple(override_provenance)
        payload = {
            "assets": [asset.qualified_id for asset in effective],
            "asset_records": [asset.to_dict() for asset in effective],
            "digests": [asset.digest for asset in effective],
            "disabled": [
                asset.to_dict()
                for asset in sorted(self._assets.values(), key=lambda item: item.qualified_id)
                if asset.state == "disabled"
            ],
            "tombstones": [
                asset.to_dict()
                for asset in sorted(self._assets.values(), key=lambda item: item.qualified_id)
                if asset.state == "tombstone"
            ],
            "overrides": [item.to_dict() for item in override_decisions],
            "merges": [dict(item) for item in merge_decisions],
        }
        result = AssetResolution(
            ordered_assets=tuple(ordered),
            effective_assets=effective,
            disabled_assets=tuple(sorted((item for item in self._assets.values() if item.state == "disabled"), key=lambda item: item.qualified_id)),
            tombstones=tuple(sorted((item for item in self._assets.values() if item.state == "tombstone"), key=lambda item: item.qualified_id)),
            provenance=provenance,
            overrides=override_decisions,
            merge_decisions=tuple(merge_decisions),
            composition_digest=_value_digest(payload),
        )
        callback = mutation if mutation is not None else mutation_observer
        if callback is not None:
            callback(result)
        return result

    def _resolve_overrides(
        self,
        assets: Sequence[TypedAsset],
        overrides: Sequence[TypedOverrideInput | Mapping[str, Any]],
        diagnostics: list[AssetDiagnostic],
    ) -> tuple[tuple[OverrideDecision, ...], tuple[AssetProvenance, ...]]:
        slots: dict[str, OverrideSlot] = {}
        unqualified_slots: dict[str, list[OverrideSlot]] = defaultdict(list)
        for asset in assets:
            for slot in asset.override_slots:
                slots[slot.qualified_id] = slot
                unqualified_slots[slot.slot_id].append(slot)

        # Normalize every structurally valid contribution and resolve its
        # stable owner-qualified slot before validating values or owners.  A
        # duplicate must therefore win consistently over an invalid value or
        # owner, independent of contribution order.
        normalized: list[tuple[OverrideSlot, object, object, object, tuple[AssetDiagnostic, ...]]] = []
        unresolved: list[AssetDiagnostic] = []
        for value in overrides:
            field_diagnostics: list[AssetDiagnostic] = []
            if isinstance(value, TypedOverrideInput):
                slot_id, new_value, owner = value.slot_id, value.value, value.owner
                reason = "instance override"
            elif isinstance(value, Mapping):
                raw = dict(value)
                # Raw document pointers remain a distinct refusal so callers
                # get the corrective hint for the forbidden selection mode.
                # Check them before closed-vocabulary parsing so their
                # diagnostic is not masked by the pointer key itself being
                # unknown.
                if "document_pointer" in raw or "path" in raw:
                    slot_hint = raw.get("slot_id", raw.get("id"))
                    owner_hint = raw.get("owner", "instance")
                    unresolved.append(AssetDiagnostic(
                        code="raw_document_pointer", asset_id=str(slot_hint), owner=str(owner_hint),
                        message="raw document pointers cannot select typed override slots",
                        fix_hint="use the declared stable slot_id",
                    ))
                    continue
                alias_hint = raw.get("slot_id", raw.get("id"))
                alias_values = [raw[name] for name in ("slot_id", "id") if name in raw]
                try:
                    slot_id = _pop_alias(raw, "slot_id", "id", default=None, field_name="override.slot_id")
                except ValueError:
                    unresolved.append(AssetDiagnostic(
                        code="override_alias_disagreement",
                        asset_id=str(alias_hint),
                        owner=str(raw.get("owner", "instance")),
                        expected="slot_id and id must match",
                        observed=", ".join(str(item) for item in alias_values),
                        message="override slot_id and id aliases disagree",
                        fix_hint="provide one slot_id/id value or make both aliases equal",
                    ))
                    continue
                new_value = raw.pop("value", _MISSING)
                owner = raw.pop("owner", "instance")
                reason = raw.pop("reason", "instance override")
                try:
                    _reject_unknown(raw, scope="override mapping")
                except ValueError:
                    names = ", ".join(sorted(str(key) for key in raw))
                    field_diagnostics.append(AssetDiagnostic(
                        code="override_unknown_field",
                        asset_id=str(slot_id),
                        owner=str(owner),
                        expected="id, owner, reason, slot_id, value",
                        observed=names,
                        message=f"override mapping contains unknown fields: {names}",
                        fix_hint="remove unknown fields and use the typed override vocabulary",
                    ))
                if new_value is _MISSING:
                    field_diagnostics.append(AssetDiagnostic(
                        code="override_value_missing",
                        asset_id=str(slot_id),
                        owner=str(owner),
                        expected="value field",
                        observed="missing",
                        message=f"override for slot {slot_id!r} does not declare a value",
                        fix_hint="provide the value field; use null explicitly for a json slot",
                    ))
            else:
                unresolved.append(AssetDiagnostic(
                    code="override_invalid",
                    message="override must be a typed mapping",
                    fix_hint="use slot_id and value",
                ))
                continue

            if not isinstance(slot_id, str):
                unresolved.extend(field_diagnostics)
                unresolved.append(AssetDiagnostic(
                    code="unknown_override_slot", asset_id=str(slot_id), owner=str(owner),
                    message=f"override slot {slot_id!r} is not declared by a selected asset",
                    fix_hint="declare the typed slot before supplying an instance value",
                ))
                continue
            slot = slots.get(slot_id)
            if slot is None:
                candidates = unqualified_slots.get(slot_id, [])
                if len(candidates) > 1:
                    unresolved.extend(field_diagnostics)
                    unresolved.append(AssetDiagnostic(
                        code="ambiguous_override_slot", asset_id=slot_id, owner=str(owner),
                        chain=tuple(item.qualified_id for item in sorted(candidates, key=lambda item: item.qualified_id)),
                        message=f"override slot {slot_id!r} is declared by multiple owners",
                        fix_hint="use the owner-qualified slot ID",
                    ))
                    continue
                if candidates:
                    slot = candidates[0]
            if slot is None:
                unresolved.extend(field_diagnostics)
                unresolved.append(AssetDiagnostic(
                    code="unknown_override_slot", asset_id=str(slot_id), owner=str(owner),
                    message=f"override slot {slot_id!r} is not declared by a selected asset",
                    fix_hint="declare the typed slot before supplying an instance value",
                ))
                continue
            normalized.append((slot, new_value, owner, reason, tuple(field_diagnostics)))

        grouped: dict[str, list[tuple[OverrideSlot, object, object, object, tuple[AssetDiagnostic, ...]]]] = defaultdict(list)
        for item in normalized:
            grouped[item[0].qualified_id].append(item)
        duplicate_slots = sorted(qualified_id for qualified_id, items in grouped.items() if len(items) > 1)
        if duplicate_slots:
            diagnostics.extend(unresolved)
            diagnostics.extend(
                AssetDiagnostic(
                    code="duplicate_override_slot",
                    chain=(qualified_id,),
                    asset_id=qualified_id,
                    owner=qualified_id.split(":", 1)[0],
                    expected="one declaration",
                    observed="duplicate declaration",
                    message=f"override slot {qualified_id!r} is declared more than once",
                    fix_hint="provide one override per owner-qualified slot",
                )
                for qualified_id in duplicate_slots
            )
            return (), ()

        diagnostics.extend(unresolved)
        decisions: list[OverrideDecision] = []
        provenance: list[AssetProvenance] = []
        for slot, new_value, owner, reason, field_diagnostics in normalized:
            diagnostics.extend(field_diagnostics)
            if field_diagnostics:
                continue
            if owner != "instance":
                diagnostics.append(AssetDiagnostic(
                    code="override_owner_invalid", chain=(slot.qualified_id,), asset_id=slot.qualified_id, owner=str(owner),
                    expected="instance", observed=str(owner), message=f"override {slot.qualified_id!r} is not instance-owned",
                    fix_hint="set the override owner to instance",
                ))
            if not _value_matches(new_value, slot.value_type, slot.schema):
                diagnostics.append(AssetDiagnostic(
                    code="override_value_invalid", chain=(slot.qualified_id,), asset_id=slot.qualified_id, owner=slot.owner,
                    expected=slot.value_type, observed=type(new_value).__name__,
                    message=f"override value for {slot.qualified_id!r} does not match {slot.value_type!r}",
                    fix_hint="supply a value accepted by the declared slot schema",
                ))
                continue
            new_digest = _value_digest(new_value)
            old_digest = _value_digest(slot.default) if slot.has_default else None
            decision = OverrideDecision(slot=slot, old_owner=slot.owner if slot.has_default else None, new_owner=str(owner), old_value_digest=old_digest, new_value_digest=new_digest, reason=str(reason), references=(slot.qualified_id,))
            decisions.append(decision)
            provenance.append(AssetProvenance(
                path=f"overrides.{slot.qualified_id}", owner=str(owner),
                reference=AssetReference(owner=slot.owner, asset_id=slot.slot_id, kind="override_slot", version="v1", digest=new_digest),
                value_digest=new_digest,
                new_value_digest=new_digest,
                source="override",
                old_owner=decision.old_owner,
                new_owner=str(owner),
                old_value_digest=old_digest,
                reason=str(reason),
                references=(AssetReference(owner=slot.owner, asset_id=slot.slot_id, kind="override_slot", version="v1", digest=new_digest),),
            ))
        if diagnostics:
            return (), ()
        return tuple(sorted(decisions, key=lambda item: item.slot.qualified_id)), tuple(sorted(provenance, key=lambda item: item.path))


def _value_matches(value: object, value_type: str, schema: Mapping[str, Any]) -> bool:
    """Small closed vocabulary for declarative override slots."""

    types: dict[str, Callable[[object], bool]] = {
        "string": lambda item: isinstance(item, str),
        "identifier": lambda item: isinstance(item, str) and bool(_ID_RE.fullmatch(item)),
        "integer": lambda item: isinstance(item, int) and not isinstance(item, bool),
        "int": lambda item: isinstance(item, int) and not isinstance(item, bool),
        "number": lambda item: isinstance(item, (int, float)) and not isinstance(item, bool),
        "float": lambda item: isinstance(item, (int, float)) and not isinstance(item, bool),
        "boolean": lambda item: isinstance(item, bool),
        "bool": lambda item: isinstance(item, bool),
        "object": lambda item: isinstance(item, Mapping),
        "map": lambda item: isinstance(item, Mapping),
        "mapping": lambda item: isinstance(item, Mapping),
        "dict": lambda item: isinstance(item, Mapping),
        "array": lambda item: isinstance(item, (list, tuple)),
        "list": lambda item: isinstance(item, (list, tuple)),
        "json": lambda item: isinstance(item, (str, int, float, bool, list, tuple, Mapping)) or item is None,
    }
    checker = types.get(value_type)
    if checker is None or not checker(value):
        return False
    if "enum" in schema and value not in schema["enum"]:
        return False
    if "type" in schema and isinstance(schema["type"], str) and schema["type"] != value_type and not (schema["type"] == "integer" and value_type == "int"):
        return False
    return True


def resolve_typed_assets(
    assets: Iterable[TypedAsset | Mapping[str, Any]],
    *,
    kinds: Iterable[AssetKindSpec | Mapping[str, Any]],
    roots: Sequence[AssetReference | Mapping[str, Any] | str] | None = None,
    overrides: Sequence[TypedOverrideInput | Mapping[str, Any]] = (),
    mutation: MutationCallback | None = None,
    mutation_observer: MutationCallback | None = None,
) -> AssetResolution:
    """Convenience API for one-shot declarative graph resolution."""

    registry = TypedAssetRegistry(kinds)
    registry.register_many(assets)
    return registry.resolve(roots, overrides=overrides, mutation=mutation, mutation_observer=mutation_observer)


def resolve_product_lock_with_typed_assets(
    selection: Any,
    resolution: AssetResolution,
    *,
    package_identities: Sequence[Any] = (),
    environment_identities: Sequence[Any] = (),
    compatibility_decisions: Sequence[Any] = (),
    local_asset_digests: Mapping[str, str] | None = None,
) -> ResolvedLock:
    """Feed a resolved graph into the existing Wave 1 lock authority."""

    # Imported lazily to keep this module inert and avoid a lock/graph cycle.
    from .lock import resolve_product_lock

    complete_provenance = resolution.lock_provenance()
    override_provenance = tuple(
        item for item in complete_provenance if item.path.startswith("overrides.")
    )
    return resolve_product_lock(
        selection,
        asset_refs=resolution.references,
        package_identities=package_identities,
        environment_identities=environment_identities,
        compatibility_decisions=compatibility_decisions,
        local_asset_digests=local_asset_digests,
        override_provenance=override_provenance,
        provenance=complete_provenance,
    )


__all__ = [
    "AssetRegistry",
    "AssetDiagnostic",
    "AssetKindSpec",
    "CORE_ASSET_KINDS",
    "VALIDATION_PLANES",
    "AssetProvenance",
    "AssetReference",
    "AssetResolution",
    "AssetResolutionError",
    "CompositionDiagnostic",
    "MutationCallback",
    "OverrideDecision",
    "OverrideSlot",
    "TYPED_ASSET_GRAPH_SCHEMA",
    "TYPED_ASSET_REGISTRY_SCHEMA",
    "TYPED_ASSET_SCHEMA",
    "TypedAsset",
    "TypedAssetGraph",
    "TypedAssetKind",
    "TypedAssetRef",
    "TypedAssetError",
    "TypedAssetRegistry",
    "TypedAssetResolutionError",
    "resolve_product_lock_with_typed_assets",
    "resolve_typed_assets",
    "ReferenceDAGResolver",
]

# Names used by a few deployment integrations.  They are aliases, not
# parallel models, so all callers share one validation and digest contract.
AssetRegistry = TypedAssetRegistry
ReferenceDAGResolver = TypedAssetRegistry
TypedAssetRef = AssetReference
TypedAssetKind = AssetKindSpec
TypedAssetGraph = AssetResolution
CompositionDiagnostic = AssetDiagnostic
