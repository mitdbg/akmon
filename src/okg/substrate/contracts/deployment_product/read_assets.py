"""Read-side specialisation of the canonical deployment-product asset graph.

This module deliberately does not define a second registry.  Read contracts
are carried by :class:`TypedAsset` records and are selected by the Wave 1
``TypedAssetRegistry``.  The helpers below add the read-specific validation
which must complete before an apply-side mutation callback may run.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any, Literal

from .typed_assets import (
    AssetDiagnostic,
    AssetReference,
    AssetResolution,
    TypedAsset,
    TypedAssetRegistry,
    TypedAssetResolutionError,
)


READ_ASSET_CONTRACT_SCHEMA = "okg.deployment_product.read_asset_contract/v1"
READ_ASSET_KINDS = frozenset({
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
})
RUNTIME_MATERIALIZATION_KINDS = frozenset({
    "derived-artifact-manifest",
    "fragment",
    "manifest",
    "materialization-receipt",
    "materialization-result",
    "refresh-receipt",
    "runtime-materialization",
})
_TOKEN_RE = re.compile(r"^\S+$")
_FORBIDDEN_CONFIG_KEYS = frozenset({
    "class",
    "class_path",
    "database_url",
    "dsn",
    "entrypoint",
    "import",
    "module",
    "password",
    "private_key",
    "secret",
    "sys_path",
    "token",
})
_KIND_REFRESH_MODES: Mapping[str, frozenset[str]] = {
    "view": frozenset({"on-demand", "generation", "schedule"}),
    "read-model": frozenset({"on-demand", "generation", "schedule"}),
    "lens": frozenset({"on-demand", "generation"}),
    "dashboard": frozenset({"on-demand", "generation", "schedule"}),
    "interface": frozenset({"on-demand"}),
    "prompt": frozenset({"on-demand"}),
    "skill": frozenset({"on-demand"}),
    "playbook": frozenset({"on-demand"}),
    "tool": frozenset({"on-demand"}),
    "resolver": frozenset({"on-demand"}),
    "search": frozenset({"on-demand"}),
    "chat-policy": frozenset({"on-demand"}),
    "vocabulary": frozenset({"on-demand"}),
}
_DATABASE_OBJECT_KINDS = frozenset({"view", "read-model"})
_KIND_CONFIG_IDENTITIES: Mapping[str, frozenset[str]] = {
    "view": frozenset({"view_id", "view_name"}),
    "read-model": frozenset({"source_relation", "schema"}),
    "lens": frozenset({"lens_id"}),
    "dashboard": frozenset({"dashboard_id"}),
    "interface": frozenset({"interface_id"}),
    "prompt": frozenset({"template"}),
    "skill": frozenset({"instructions"}),
    "playbook": frozenset({"steps"}),
    "tool": frozenset({"capability_ref"}),
    "resolver": frozenset({"target_kind"}),
    "search": frozenset({"index_ref"}),
    "chat-policy": frozenset({"policy_ref"}),
    "vocabulary": frozenset({"terms"}),
}
_REGISTERED_VIEW_RESULT_SLOTS = frozenset({
    "attention_items",
    "fleet",
    "json",
    "markdown",
    "queue_items",
    "relation",
    "source_health",
    "table",
    "view_graph",
})
_KIND_SPECS = TypedAssetRegistry().kinds
_KIND_CAPABILITY_SCHEMAS: Mapping[str, str] = MappingProxyType({
    kind: f"okg.deployment_product.asset.{kind}.capability/v1"
    for kind in READ_ASSET_KINDS
})
_SQL_READ_MODEL_SCHEMAS = (
    "okg.deployment_product.sql_read_model/v1",
    "okg.deployment_product.sql_read_model.result/v1",
    "okg.deployment_product.sql_read_model.capability/v1",
)
_KIND_REFERENCE_KINDS: Mapping[str, frozenset[str]] = {
    "view": frozenset({"view", "read-model", "lens", "search", "vocabulary"}),
    "read-model": frozenset({"read-model", "resolver", "search", "vocabulary"}),
    "lens": frozenset({"view", "read-model", "lens", "search", "vocabulary"}),
    "dashboard": frozenset({"view", "read-model", "lens", "search", "vocabulary"}),
    "interface": READ_ASSET_KINDS,
    "prompt": frozenset({"vocabulary", "tool", "resolver", "search", "chat-policy"}),
    "skill": frozenset({"prompt", "tool", "resolver", "search", "vocabulary", "chat-policy", "skill"}),
    "playbook": frozenset({"prompt", "tool", "resolver", "search", "vocabulary", "chat-policy", "skill", "playbook"}),
    "tool": frozenset({"tool", "resolver", "read-model", "search", "vocabulary", "connection"}),
    "resolver": frozenset({"resolver", "read-model", "search", "vocabulary", "connection", "source"}),
    "search": frozenset({"read-model", "resolver", "search", "vocabulary"}),
    "chat-policy": frozenset({"prompt", "tool", "resolver", "search", "vocabulary", "chat-policy"}),
    "vocabulary": frozenset({"vocabulary"}),
}


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _token(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value or not _TOKEN_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a non-empty token")
    return value


def _validate_json_config(value: object, *, path: str = "config") -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite number")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_json_config(item, path=f"{path}[{index}]")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise ValueError(f"{path} keys must be non-empty strings")
            normalized = key.lower().replace("-", "_")
            if normalized in _FORBIDDEN_CONFIG_KEYS:
                raise ValueError(f"{path}.{key} is forbidden at the read-asset boundary")
            _validate_json_config(item, path=f"{path}.{key}")
        return
    raise ValueError(f"{path} must contain only JSON values")


def _non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _string_list(value: object, *, field_name: str) -> tuple[str, ...]:
    if isinstance(value, str) or not isinstance(value, (list, tuple)) or not value:
        raise ValueError(f"{field_name} must be a non-empty string list")
    return tuple(
        _non_empty_string(item, field_name=field_name)
        for item in value
    )


def _validate_registered_config(kind: str, config: Mapping[str, Any]) -> None:
    """Validate the closed v1 configuration shape registered for each kind."""

    identity_fields = _KIND_CONFIG_IDENTITIES[kind]
    allowed = (
        identity_fields | {"deployment_name", "result_slots"}
        if kind == "view"
        else identity_fields
    )
    unknown = sorted(set(config) - allowed)
    if unknown:
        raise ValueError(
            f"{kind} config contains fields outside its registered schema: "
            + ", ".join(unknown)
        )
    present = set(config) & identity_fields
    if len(present) != 1:
        expected = ", ".join(sorted(identity_fields))
        raise ValueError(
            f"{kind} config requires exactly one registered identity field: {expected}"
        )
    field_name = next(iter(present))
    value = config[field_name]
    if kind in {"playbook", "vocabulary"}:
        _string_list(value, field_name=f"{kind}.{field_name}")
    elif kind == "resolver":
        target_kind = _non_empty_string(value, field_name="resolver.target_kind")
        if target_kind not in READ_ASSET_KINDS:
            raise ValueError("resolver.target_kind must name a registered read asset kind")
    elif kind == "read-model" and field_name == "schema":
        if not isinstance(value, Mapping) or not value:
            raise ValueError("read-model.schema must be a non-empty mapping")
        _validate_json_config(value, path="read-model.schema")
    else:
        _non_empty_string(value, field_name=f"{kind}.{field_name}")
    if kind == "view":
        if "deployment_name" in config:
            _non_empty_string(config["deployment_name"], field_name="view.deployment_name")
        if "result_slots" in config:
            slots = _string_list(config["result_slots"], field_name="view.result_slots")
            unknown_slots = sorted(set(slots) - _REGISTERED_VIEW_RESULT_SLOTS)
            if unknown_slots:
                raise ValueError(
                    "view.result_slots contains unregistered slots: "
                    + ", ".join(unknown_slots)
                )


def _validate_kind_contract(contract: "ReadAssetContract") -> None:
    kind = contract.asset.kind
    sql_contract = contract.config_schema == _SQL_READ_MODEL_SCHEMAS[0]
    if kind not in _KIND_REFRESH_MODES:
        raise ValueError(f"unregistered read asset kind {kind!r}")
    if contract.refresh_policy.mode not in _KIND_REFRESH_MODES[kind]:
        raise ValueError(
            f"{kind} does not support {contract.refresh_policy.mode!r} refresh"
        )
    if contract.database_object is not None and kind not in _DATABASE_OBJECT_KINDS:
        raise ValueError(f"{kind} cannot own a database object")
    kind_spec = _KIND_SPECS[kind]
    if contract.asset.schema != kind_spec.schema:
        raise ValueError(f"asset schema must match the registered {kind} kind")
    if sql_contract:
        observed_schemas = (
            contract.config_schema,
            contract.result_schema,
            contract.capability_schema,
        )
        if observed_schemas != _SQL_READ_MODEL_SCHEMAS:
            raise ValueError("SQL read-model schemas must match the registered SQL contract")
    else:
        if contract.config_schema != kind_spec.config_schema:
            raise ValueError(f"config_schema must match the registered {kind} schema")
        if contract.result_schema != kind_spec.result_schema:
            raise ValueError(f"result_schema must match the registered {kind} schema")
        if contract.capability_schema != _KIND_CAPABILITY_SCHEMAS[kind]:
            raise ValueError(f"capability_schema must match the registered {kind} schema")
    if not set(kind_spec.capabilities) <= set(contract.asset.capabilities):
        raise ValueError(f"{kind} capabilities do not satisfy its registered kind spec")
    if not contract.config:
        raise ValueError(f"{kind} requires a non-empty typed configuration")
    if set(contract.policy_requirements) != {
        "audiences", "tenants", "visible_fields", "decision_ids"
    }:
        raise ValueError("read policy requirements must use the closed typed field set")
    for field_name, values in contract.policy_requirements.items():
        if not isinstance(values, (list, tuple)) or not values or not all(
            isinstance(item, str) and item for item in values
        ):
            raise ValueError(f"read policy {field_name} must be a non-empty string list")
    if not contract.lineage_sources:
        raise ValueError("read assets require at least one typed lineage source")
    illegal_references = sorted(
        {
            reference.kind
            for reference in contract.asset.references
            if reference.kind not in _KIND_REFERENCE_KINDS[kind]
        }
    )
    if illegal_references:
        raise ValueError(
            f"{kind} has illegal reference kinds: {', '.join(illegal_references)}"
        )
    unknown_compatibility = sorted(
        set(contract.asset.compatibility) - {"read_contract", "legacy_adapter"}
    )
    if unknown_compatibility:
        raise ValueError(
            "read asset compatibility contains unregistered fields: "
            + ", ".join(unknown_compatibility)
        )
    compatibility_adapter = contract.asset.provenance.get("adapter")
    if compatibility_adapter == "views_registry.yaml/v1":
        allowed = {
            "anchor_resolver",
            "build_corpus",
            "deployment_name",
            "llm_client",
            "params",
            "reads_edge_types",
            "reads_subtypes",
            "result_slots",
            "tracking_mode",
            "view_def_factory",
            "view_name",
        }
        unknown = sorted(set(contract.config) - allowed)
        if unknown:
            raise ValueError(
                "legacy view adapter config contains unknown fields: " + ", ".join(unknown)
            )
        # The adapter records legacy refs as inert provenance-bearing strings.
        safe_config = {
            key: value
            for key, value in contract.config.items()
            if key not in {"view_def_factory", "build_corpus", "anchor_resolver"}
        }
        _validate_json_config(safe_config)
    elif sql_contract:
        _validate_json_config(contract.config)
    else:
        _validate_json_config(contract.config)
        _validate_registered_config(kind, contract.config)


@dataclass(frozen=True, slots=True)
class ReadRefreshPolicy:
    """Declarative refresh policy; never a scheduler or runtime receipt."""

    mode: Literal["on-demand", "generation", "schedule"] = "on-demand"
    schedule: str | None = None

    def __post_init__(self) -> None:
        if self.mode not in {"on-demand", "generation", "schedule"}:
            raise ValueError("refresh policy mode must be on-demand, generation, or schedule")
        if self.mode == "schedule":
            if not isinstance(self.schedule, str) or not self.schedule.strip():
                raise ValueError("refresh_policy.schedule must be a non-empty string")
        elif self.schedule not in (None, ""):
            raise ValueError("refresh policy schedule is allowed only for schedule mode")

    def to_dict(self) -> dict[str, Any]:
        return {"mode": self.mode, "schedule": self.schedule or None}

    @classmethod
    def from_value(cls, value: object) -> "ReadRefreshPolicy":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ValueError("refresh_policy must be a mapping")
        raw = dict(value)
        unknown = sorted(set(raw) - {"mode", "schedule"})
        if unknown:
            raise ValueError(f"refresh_policy contains unknown fields: {', '.join(unknown)}")
        return cls(mode=raw.get("mode", "on-demand"), schedule=raw.get("schedule"))


@dataclass(frozen=True, slots=True)
class ReadAssetContract:
    """Read-specific contract embedded in one canonical ``TypedAsset``."""

    asset: TypedAsset
    config_schema: str
    result_schema: str
    capability_schema: str
    output_identity: str
    refresh_policy: ReadRefreshPolicy
    policy_requirements: Mapping[str, Any]
    lineage_sources: tuple[str, ...]
    config: Mapping[str, Any] = field(default_factory=dict)
    database_object: str | None = None

    def __post_init__(self) -> None:
        _token(self.config_schema, field_name="config_schema")
        _token(self.result_schema, field_name="result_schema")
        _token(self.capability_schema, field_name="capability_schema")
        _token(self.output_identity, field_name="output_identity")
        if self.database_object is not None:
            _token(self.database_object, field_name="database_object")
        object.__setattr__(self, "config", MappingProxyType(dict(self.config)))
        object.__setattr__(
            self,
            "policy_requirements",
            MappingProxyType(dict(self.policy_requirements)),
        )
        object.__setattr__(
            self,
            "lineage_sources",
            tuple(_token(item, field_name="lineage_source") for item in self.lineage_sources),
        )
        _validate_kind_contract(self)

    @property
    def qualified_id(self) -> str:
        return self.asset.qualified_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": READ_ASSET_CONTRACT_SCHEMA,
            "asset": self.asset.to_dict(),
            "config_schema": self.config_schema,
            "result_schema": self.result_schema,
            "capability_schema": self.capability_schema,
            "output_identity": self.output_identity,
            "refresh_policy": self.refresh_policy.to_dict(),
            "policy_requirements": dict(self.policy_requirements),
            "lineage_sources": list(self.lineage_sources),
            "config": dict(self.config),
            "database_object": self.database_object,
        }

    def expected_asset_digest(self) -> str:
        """Recompute the digest over the authorable declaration bytes."""

        return _digest(_asset_digest_payload(
            owner=self.asset.owner,
            asset_id=self.asset.asset_id,
            kind=self.asset.kind,
            version=self.asset.version,
            references=self.asset.references,
            capabilities=self.asset.capabilities,
            read_metadata={
                "schema": READ_ASSET_CONTRACT_SCHEMA,
                "config_schema": self.config_schema,
                "result_schema": self.result_schema,
                "capability_schema": self.capability_schema,
                "output_identity": self.output_identity,
                "refresh_policy": self.refresh_policy.to_dict(),
                "policy_requirements": dict(self.policy_requirements),
                "lineage_sources": list(self.lineage_sources),
                "config": dict(self.config),
                "database_object": self.database_object,
            },
            provenance=self.asset.provenance,
        ))

    @classmethod
    def from_asset(cls, asset: TypedAsset) -> "ReadAssetContract":
        metadata = asset.compatibility.get("read_contract")
        if not isinstance(metadata, Mapping):
            raise TypedAssetResolutionError((_diagnostic(
                "read_contract_missing",
                asset,
                "read-side asset is missing its read_contract metadata",
                "construct the asset with build_read_asset",
            ),))
        try:
            return cls(
                asset=asset,
                config_schema=metadata["config_schema"],
                result_schema=metadata["result_schema"],
                capability_schema=metadata["capability_schema"],
                output_identity=metadata["output_identity"],
                refresh_policy=ReadRefreshPolicy.from_value(metadata["refresh_policy"]),
                policy_requirements=metadata["policy_requirements"],
                lineage_sources=tuple(metadata["lineage_sources"]),
                config=metadata.get("config") or {},
                database_object=metadata.get("database_object"),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise TypedAssetResolutionError((_diagnostic(
                "read_contract_invalid",
                asset,
                f"read-side asset has invalid read_contract metadata: {exc}",
                "supply the registered configuration, result, capability, and refresh contracts",
            ),)) from exc


@dataclass(frozen=True, slots=True)
class ReadAssetResolution:
    """Read metadata paired with the one canonical Wave 1 resolution."""

    canonical: AssetResolution
    contracts: tuple[ReadAssetContract, ...]

    @property
    def assets(self) -> tuple[TypedAsset, ...]:
        return self.canonical.assets

    @property
    def composition_digest(self) -> str:
        return self.canonical.composition_digest

    def lock_provenance(self):
        return self.canonical.lock_provenance()

    def execution_token(self, asset_id: str) -> "ReadAssetExecutionToken":
        contract = next(
            (item for item in self.contracts if item.qualified_id == asset_id),
            None,
        )
        if contract is None:
            raise ValueError(f"read asset {asset_id!r} is not selected by this resolution")
        if contract.asset.kind == "view":
            runtime_identity = (
                contract.asset.asset_id
                if contract.asset.provenance.get("adapter") == "views_registry.yaml/v1"
                else str(contract.config.get("view_id") or contract.config.get("view_name"))
            )
            allowed_slots = tuple(sorted(
                contract.config.get("result_slots") or ("json", "markdown")
            ))
        else:
            runtime_identity = contract.output_identity
            allowed_slots = ()
        return ReadAssetExecutionToken(
            asset_id=contract.qualified_id,
            asset_digest=contract.asset.digest,
            composition_digest=self.composition_digest,
            policy_digest=_digest(dict(contract.policy_requirements)),
            policy_requirements=contract.policy_requirements,
            lineage_sources=contract.lineage_sources,
            runtime_identity=runtime_identity,
            allowed_slots=allowed_slots,
        )


@dataclass(frozen=True, slots=True)
class ReadAssetExecutionToken:
    """Canonical selection identity required by the native executor."""

    asset_id: str
    asset_digest: str
    composition_digest: str
    policy_digest: str
    policy_requirements: Mapping[str, Any]
    lineage_sources: tuple[str, ...]
    runtime_identity: str
    allowed_slots: tuple[str, ...]


def _diagnostic(
    code: str,
    asset: TypedAsset | None,
    message: str,
    fix_hint: str,
    *,
    chain: tuple[str, ...] = (),
    expected: str | None = None,
    observed: str | None = None,
) -> AssetDiagnostic:
    return AssetDiagnostic(
        code=code,
        chain=chain,
        asset_id=asset.qualified_id if asset else None,
        owner=asset.owner if asset else None,
        expected=expected,
        observed=observed,
        message=message,
        fix_hint=fix_hint,
    )


def _asset_digest_payload(
    *,
    owner: str,
    asset_id: str,
    kind: str,
    version: str,
    references: Sequence[AssetReference | Mapping[str, Any]],
    capabilities: Sequence[str],
    read_metadata: Mapping[str, Any],
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    reference_records = [
        (item if isinstance(item, AssetReference) else AssetReference.from_value(item)).to_dict()
        for item in references
    ]
    return {
        "owner": owner,
        "id": asset_id,
        "kind": kind,
        "version": version,
        "references": sorted(reference_records, key=_canonical_bytes),
        "capabilities": sorted(set(capabilities)),
        "read_contract": dict(read_metadata),
        "provenance": dict(provenance),
    }


def build_read_asset(
    *,
    owner: str,
    asset_id: str,
    kind: str,
    config_schema: str,
    result_schema: str,
    capability_schema: str,
    output_identity: str,
    refresh_policy: ReadRefreshPolicy | Mapping[str, Any],
    policy_requirements: Mapping[str, Any],
    lineage_sources: Sequence[str],
    config: Mapping[str, Any] | None = None,
    references: Sequence[AssetReference | Mapping[str, Any]] = (),
    database_object: str | None = None,
    version: str = "v1",
    capabilities: Sequence[str] = ("declarative", "read"),
    merge_identity: str | None = None,
    merge_rule: str = "refuse",
    provenance: Mapping[str, Any] | None = None,
    compatibility: Mapping[str, Any] | None = None,
) -> ReadAssetContract:
    """Build a digest-bound authorable read asset."""

    if kind in RUNTIME_MATERIALIZATION_KINDS:
        raise TypedAssetResolutionError((AssetDiagnostic(
            code="runtime_materialization_not_authorable",
            asset_id=f"{owner}:{asset_id}",
            owner=owner,
            observed=kind,
            message=f"runtime materialization kind {kind!r} cannot be authored as a product asset",
            fix_hint="author refresh policy on a view/read-model and retain results as generation-bound runtime state",
        ),))
    if kind not in READ_ASSET_KINDS:
        raise TypedAssetResolutionError((AssetDiagnostic(
            code="read_asset_kind_unsupported",
            asset_id=f"{owner}:{asset_id}",
            owner=owner,
            observed=kind,
            message=f"kind {kind!r} is not an authorable read or agent asset",
            fix_hint="use a registered read-side kind",
        ),))
    checked_policy = ReadRefreshPolicy.from_value(refresh_policy)
    checked_config = dict(config or {})
    read_metadata = {
        "schema": READ_ASSET_CONTRACT_SCHEMA,
        "config_schema": _token(config_schema, field_name="config_schema"),
        "result_schema": _token(result_schema, field_name="result_schema"),
        "capability_schema": _token(capability_schema, field_name="capability_schema"),
        "output_identity": _token(output_identity, field_name="output_identity"),
        "refresh_policy": checked_policy.to_dict(),
        "policy_requirements": dict(policy_requirements),
        "lineage_sources": list(lineage_sources),
        "config": checked_config,
        "database_object": (
            _token(database_object, field_name="database_object")
            if database_object is not None else None
        ),
    }
    checked_provenance = dict(provenance or {"source": "local"})
    asset_payload = _asset_digest_payload(
        owner=owner,
        asset_id=asset_id,
        kind=kind,
        version=version,
        references=references,
        capabilities=capabilities,
        read_metadata=read_metadata,
        provenance=checked_provenance,
    )
    combined_compatibility = dict(compatibility or {})
    combined_compatibility["read_contract"] = read_metadata
    asset = TypedAsset(
        owner=owner,
        asset_id=asset_id,
        kind=kind,
        plane=("views-read-models" if kind in {"view", "read-model", "lens", "dashboard"} else "agent-ui"),
        version=version,
        digest=_digest(asset_payload),
        schema=f"okg.deployment_product.asset.{kind}/v1",
        references=references,
        capabilities=capabilities,
        compatibility=combined_compatibility,
        merge_identity=merge_identity or asset_id,
        merge_rule=merge_rule,
        provenance=checked_provenance,
    )
    return ReadAssetContract.from_asset(asset)


def resolve_read_assets(
    declarations: Iterable[ReadAssetContract | TypedAsset],
    *,
    supporting_assets: Iterable[TypedAsset] = (),
    roots: Sequence[AssetReference | Mapping[str, Any] | str] | None = None,
    registry: TypedAssetRegistry | None = None,
    mutation: Callable[[ReadAssetResolution], object] | None = None,
    mutation_observer: Callable[[ReadAssetResolution], object] | None = None,
) -> ReadAssetResolution:
    """Resolve read assets canonically and run read-specific refusal checks."""

    contracts: list[ReadAssetContract] = []
    diagnostics: list[AssetDiagnostic] = []
    for declaration in declarations:
        asset = declaration.asset if isinstance(declaration, ReadAssetContract) else declaration
        if asset.kind in RUNTIME_MATERIALIZATION_KINDS:
            diagnostics.append(_diagnostic(
                "runtime_materialization_not_authorable",
                asset,
                "generation-bound materialization state cannot be selected as an authoring asset",
                "retain the receipt/result outside the product asset graph",
            ))
            continue
        if asset.kind not in READ_ASSET_KINDS:
            diagnostics.append(_diagnostic(
                "read_asset_kind_unsupported",
                asset,
                f"kind {asset.kind!r} is not an authorable read or agent asset",
                "use a registered read-side kind",
            ))
            continue
        try:
            contract = declaration if isinstance(declaration, ReadAssetContract) else ReadAssetContract.from_asset(asset)
            if contract.expected_asset_digest() != asset.digest:
                diagnostics.append(_diagnostic(
                    "read_asset_digest_mismatch",
                    asset,
                    "read-side asset bytes no longer match the selected digest",
                    "rebuild the read asset and refresh every digest-pinned reference",
                    expected=contract.expected_asset_digest(),
                    observed=asset.digest,
                ))
                continue
            contracts.append(contract)
        except TypedAssetResolutionError as exc:
            diagnostics.extend(exc.diagnostics)
    if diagnostics:
        raise TypedAssetResolutionError(diagnostics)

    # Work in a shadow canonical registry so a refused candidate cannot alter a
    # caller-owned registry either.
    canonical_registry = TypedAssetRegistry(
        registry.kinds.values() if registry is not None else None
    )
    if registry is not None:
        canonical_registry.register_many(registry.assets.values())
    support = tuple(supporting_assets)
    canonical_registry.register_many((*support, *(item.asset for item in contracts)))
    selected_roots = roots
    if selected_roots is None:
        selected_roots = tuple(item.asset.qualified_id for item in contracts)
    canonical = canonical_registry.resolve(selected_roots)

    selected_ids = {item.qualified_id for item in canonical.assets}
    contracts_by_id = {item.qualified_id: item for item in contracts}
    for asset in canonical.assets:
        if asset.kind not in READ_ASSET_KINDS:
            continue
        contract = contracts_by_id.get(asset.qualified_id)
        if contract is None:
            try:
                contract = ReadAssetContract.from_asset(asset)
            except TypedAssetResolutionError as exc:
                diagnostics.extend(exc.diagnostics)
                continue
            if contract.expected_asset_digest() != asset.digest:
                diagnostics.append(_diagnostic(
                    "read_asset_digest_mismatch",
                    asset,
                    "selected supporting read asset bytes do not match its digest",
                    "rebuild every supporting/preloaded read asset through build_read_asset",
                    expected=contract.expected_asset_digest(),
                    observed=asset.digest,
                ))
                continue
            contracts_by_id[asset.qualified_id] = contract
    if diagnostics:
        raise TypedAssetResolutionError(diagnostics)
    selected_contracts = tuple(
        sorted(
            (contracts_by_id[item] for item in selected_ids if item in contracts_by_id),
            key=lambda item: item.qualified_id,
        )
    )
    by_database_object: dict[str, list[ReadAssetContract]] = {}
    by_output_identity: dict[str, list[ReadAssetContract]] = {}
    for contract in selected_contracts:
        if contract.database_object:
            by_database_object.setdefault(contract.database_object, []).append(contract)
        by_output_identity.setdefault(contract.output_identity, []).append(contract)

    for object_name, grouped in sorted(by_database_object.items()):
        if len(grouped) > 1:
            owners = tuple(sorted(item.qualified_id for item in grouped))
            diagnostics.append(_diagnostic(
                "database_object_collision",
                grouped[0].asset,
                f"database object {object_name!r} is claimed by {', '.join(owners)}",
                "assign each database object to exactly one read asset",
                chain=owners,
                observed=object_name,
            ))
    for output_id, grouped in sorted(by_output_identity.items()):
        schemas = {item.result_schema for item in grouped}
        if len(grouped) > 1 and len(schemas) > 1:
            owners = tuple(sorted(item.qualified_id for item in grouped))
            diagnostics.append(_diagnostic(
                "output_contract_collision",
                grouped[0].asset,
                f"output identity {output_id!r} has incompatible result schemas across {', '.join(owners)}",
                "use distinct output identities or one compatible result schema",
                chain=owners,
                expected=grouped[0].result_schema,
                observed=grouped[-1].result_schema,
            ))
    if diagnostics:
        raise TypedAssetResolutionError(diagnostics)

    result = ReadAssetResolution(canonical=canonical, contracts=selected_contracts)
    callback = mutation if mutation is not None else mutation_observer
    if callback is not None:
        callback(result)
    return result


__all__ = [
    "READ_ASSET_CONTRACT_SCHEMA",
    "READ_ASSET_KINDS",
    "RUNTIME_MATERIALIZATION_KINDS",
    "ReadAssetContract",
    "ReadAssetExecutionToken",
    "ReadAssetResolution",
    "ReadRefreshPolicy",
    "build_read_asset",
    "resolve_read_assets",
]
