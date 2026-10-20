"""Canonical Wave 1 deployment-product contract models.

The models in this module are deliberately inert.  They describe product
inputs and references; they do not resolve assets, build a sealed release, or
read runtime state.  The latter two operations remain owned by the existing
sealed-release and deployment-lifecycle authorities.
"""

from __future__ import annotations

import re
from copy import deepcopy
from typing import ClassVar, Literal, Mapping, TypeAlias
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)


# Stable public version tokens.  The object-specific schema constants make it
# possible for a reader to identify a value without knowing its Python class;
# the contract and API versions are shared by every object in this package.
CONTRACT_VERSION = "v1"
CONTRACT_SCHEMA = "okg.deployment_product/v1"
API_VERSION = "okg.deployment_product.api/v1"
DEPLOYMENT_PRODUCT_CONTRACT_VERSION = CONTRACT_VERSION
DEPLOYMENT_PRODUCT_SCHEMA = CONTRACT_SCHEMA
DEPLOYMENT_PRODUCT_API_VERSION = API_VERSION

DISTRIBUTION_RELEASE_SCHEMA = "okg.deployment_product.distribution_release/v1"
PRODUCT_BUNDLE_SCHEMA = "okg.deployment_product.product_bundle/v1"
AUTHORED_DEFINITION_SCHEMA = "okg.deployment_product.authored_definition/v1"
RESOLVED_LOCK_SCHEMA = "okg.deployment_product.resolved_lock/v1"
APPLIED_INSTANCE_SCHEMA = "okg.deployment_product.applied_instance/v1"
SEALED_RELEASE_SCHEMA = "okg.sealed_release/v1"

# A few callers use the shorter names when registering generated schemas.
SCHEMA_VERSION = CONTRACT_SCHEMA
OBJECT_MODEL_VERSION = CONTRACT_VERSION


_ID_RE = re.compile(r"^[^\s/\\]+$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_FORBIDDEN_SECRET_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "auth_token",
        "credential",
        "credentials",
        "password",
        "passwd",
        "private_key",
        "secret",
        "secret_material",
        "secret_payload",
        "secret_value",
        "token",
    }
)
_FORBIDDEN_RUNTIME_KEYS = frozenset(
    {
        "evidence",
        "generation",
        "generation_id",
        "generations",
        "generation_history",
        "host_state",
        "last_known_good",
        "last_known_good_history",
        "lkg",
        "receipt",
        "receipts",
        "receipt_history",
        "runtime_state",
        "secret_version",
        "secret_version_id",
        "version_id",
        "watermark",
        "watermarks",
    }
)
_TUPLE_KEYS = frozenset(
    {
        "assets",
        "asset_refs",
        "bundle_ids",
        "bundles",
        "bundle_refs",
        "capabilities",
        "choices",
        "compatibility_decisions",
        "conflicts_with_bundle_ids",
        "connection_references",
        "connection_contracts",
        "contract_versions",
        "environment_identities",
        "environment_refs",
        "affected_assets",
        "diagnostics",
        "provider_connections",
        "local_assets",
        "local_asset_digests",
        "overrides",
        "package_identities",
        "package_refs",
        "next_actions",
        "phase_receipts",
        "preconditions",
        "provenance",
        "questions",
        "requires_bundle_ids",
        "secret_references",
        "source_bindings",
        "tool_bindings",
        "required_capabilities",
        "actions",
    }
)


def _validate_identifier(value: str) -> str:
    if not isinstance(value, str) or not value.strip() or not _ID_RE.fullmatch(value):
        raise ValueError("identifiers must be non-empty strings without whitespace or path separators")
    return value


def _validate_digest(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise ValueError("digests must be sha256:<64 lowercase hexadecimal characters>")
    return value


def _validate_logical_path(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or any(char.isspace() for char in value)
        or "\x00" in value
        or "\\" in value
        or value.startswith("/")
        or re.match(r"^[A-Za-z]:", value)
    ):
        raise ValueError("logical paths must be non-empty POSIX-relative paths")
    segments = value.split("/")
    if any(segment in {"", ".", ".."} for segment in segments):
        raise ValueError("logical paths must not contain empty, '.', or '..' segments")
    return value


def _validate_connection_endpoint(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(char.isspace() for char in value):
        raise ValueError(f"{field} must be a non-empty absolute URI without whitespace")
    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"{field} must be an absolute URI")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(f"{field} must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError(f"{field} must not contain query or fragment state")
    return value


def _scan_for_unsafe_payload(value: object, path: str = "$") -> None:
    """Reject secret material and runtime state even inside JSON value slots.

    Typed override and instance-value maps intentionally remain JSON values,
    so ``extra='forbid'`` alone cannot see an unsafe key nested in one of
    those maps.  This walk is therefore a second, fail-closed boundary.
    Secret *references* are a closed vocabulary and are explicitly allowed;
    version/fingerprint metadata is not.
    """

    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            # Normalize snake_case, kebab-case, camelCase, and PascalCase to
            # one comparison form before checking the fail-closed vocabulary.
            camel_split = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", key_text)
            acronym_split = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", camel_split)
            normalised = re.sub(
                r"[^a-z0-9]+",
                "_",
                acronym_split.lower(),
            ).strip("_")
            if normalised in _FORBIDDEN_RUNTIME_KEYS:
                raise ValueError(f"runtime or secret-version field is not allowed at {path}.{key_text}")
            if normalised in _FORBIDDEN_SECRET_KEYS:
                raise ValueError(f"literal secret field is not allowed at {path}.{key_text}")
            if normalised.endswith(("_password", "_passwd", "_token", "_api_key", "_private_key")):
                raise ValueError(f"literal secret field is not allowed at {path}.{key_text}")
            # ``secret_id``/``secret_ref`` are references, never values.  The
            # nested value is still walked so a malicious object cannot hide
            # a runtime field below a reference-shaped key.
            _scan_for_unsafe_payload(child, f"{path}.{key_text}")
    elif isinstance(value, (list, tuple, set, frozenset)):
        for index, child in enumerate(value):
            _scan_for_unsafe_payload(child, f"{path}[{index}]")


def _normalise_sequence_fields(value: object) -> object:
    """Keep strict scalar validation while accepting ordinary YAML arrays."""

    if isinstance(value, Mapping):
        return {
            key: (
                tuple(_normalise_sequence_fields(item) for item in child)
                if str(key) in _TUPLE_KEYS and isinstance(child, list)
                else _normalise_sequence_fields(child)
            )
            for key, child in value.items()
        }
    if isinstance(value, tuple):
        return tuple(_normalise_sequence_fields(item) for item in value)
    return value


class ProductModel(BaseModel):
    """Shared strict/frozen configuration for every public contract model."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        strict=True,
        validate_assignment=True,
    )

    contract_version: Literal[CONTRACT_VERSION] = CONTRACT_VERSION
    api_version: Literal[API_VERSION] = API_VERSION

    @model_validator(mode="before")
    @classmethod
    def _reject_unsafe_payload(cls, value: object) -> object:
        _scan_for_unsafe_payload(value)
        return _normalise_sequence_fields(value)

    def model_copy(self, *, update: Mapping[str, object] | None = None, deep: bool = False) -> "ProductModel":
        """Copy only through the same strict validator as normal construction.

        Pydantic's default ``model_copy(update=...)`` intentionally trusts the
        update mapping and does not validate it.  That is unsafe at this
        contract boundary: a caller could otherwise smuggle runtime state or a
        literal secret into a frozen model.  Reconstructing from a dumped
        payload also revalidates nested values and unknown keys.
        """

        payload = self.model_dump(mode="python", round_trip=True)
        if update is not None:
            if not isinstance(update, Mapping):
                raise TypeError("model_copy update must be a mapping")
            payload.update(deepcopy(dict(update)) if deep else dict(update))
        return type(self).model_validate(payload)


class VersionedProductModel(ProductModel):
    """Product model with a schema token identifying the concrete object."""

    schema_version: str

    SCHEMA: ClassVar[str] = CONTRACT_SCHEMA

    @field_validator("schema_version")
    @classmethod
    def _schema_token(cls, value: str) -> str:
        if value != cls.SCHEMA:
            raise ValueError(f"schema_version must be {cls.SCHEMA!r}")
        return value

    @property
    def schema(self) -> str:
        """Compatibility accessor matching existing OKG contract payloads."""

        return self.schema_version


OwnerQualifiedRef: TypeAlias = "OwnerQualifiedReference"


class OwnerQualifiedReference(ProductModel):
    """A stable reference whose owner is explicit and never inferred by path."""

    owner: str
    ref: str = Field(alias="id")
    kind: str = "asset"
    version: str | None = None
    digest: str | None = None

    @field_validator("owner", "ref", "kind", "version")
    @classmethod
    def _identifier(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_identifier(value)

    _digest = field_validator("digest")(_validate_digest)

    @property
    def id(self) -> str:
        return self.ref

    @property
    def qualified_id(self) -> str:
        return f"{self.owner}:{self.ref}"


# Names used by downstream integrations.  They intentionally point to one
# type, rather than introducing integration-specific reference models.
StableReference = OwnerQualifiedReference
StableOwnerQualifiedRef = OwnerQualifiedReference
AssetReference = OwnerQualifiedReference


class SecretReference(ProductModel):
    """A locator for secret material; it can never carry the material itself."""

    ref: str = Field(alias="secret_id")
    provider: str | None = None
    scope: str | None = None

    @field_validator("ref", "provider", "scope")
    @classmethod
    def _reference_identifier(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_identifier(value)

    @property
    def secret_id(self) -> str:
        return self.ref


class ConnectionTlsPolicy(ProductModel):
    """Non-secret TLS requirements owned by a provider connection."""

    mode: Literal["system", "pinned"] = "system"
    server_name: str | None = None
    ca_ref: OwnerQualifiedReference | None = None

    @field_validator("server_name")
    @classmethod
    def _server_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_identifier(value)


class ConnectionProxyPolicy(ProductModel):
    """Explicit proxy contract; ambient proxy discovery is never implied."""

    mode: Literal["none", "http", "https", "socks5"] = "none"
    endpoint: str | None = None

    @field_validator("endpoint")
    @classmethod
    def _endpoint(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_connection_endpoint(value, field="proxy endpoint")

    @model_validator(mode="after")
    def _mode_matches_endpoint(self) -> "ConnectionProxyPolicy":
        if self.mode == "none" and self.endpoint is not None:
            raise ValueError("proxy endpoint must be absent when proxy mode is none")
        if self.mode != "none" and self.endpoint is None:
            raise ValueError("proxy endpoint is required when proxy mode is enabled")
        return self


class ProviderDiagnosticDefinition(ProductModel):
    """One observational diagnostic declared by the provider contract."""

    diagnostic_id: str
    operation_id: str
    effect: Literal["observe"] = "observe"

    @field_validator("diagnostic_id", "operation_id")
    @classmethod
    def _identifier(cls, value: str) -> str:
        return _validate_identifier(value)


class ProviderConnectionDefinition(ProductModel):
    """Reusable provider authority containing only non-secret contract plus a locator."""

    connection_id: str
    owner: str
    connection_type: OwnerQualifiedReference
    endpoint: str
    auth_strategy: str
    secret_reference: SecretReference
    tls: ConnectionTlsPolicy = Field(default_factory=ConnectionTlsPolicy)
    proxy: ConnectionProxyPolicy = Field(default_factory=ConnectionProxyPolicy)
    diagnostics: tuple[ProviderDiagnosticDefinition, ...] = ()
    capabilities: tuple[str, ...] = ()

    @field_validator("connection_id", "owner", "auth_strategy")
    @classmethod
    def _identifier(cls, value: str) -> str:
        return _validate_identifier(value)

    @field_validator("endpoint")
    @classmethod
    def _endpoint(cls, value: str) -> str:
        return _validate_connection_endpoint(value, field="provider endpoint")

    @field_validator("capabilities")
    @classmethod
    def _capabilities(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(sorted(_validate_identifier(value) for value in values))
        if len(checked) != len(set(checked)):
            raise ValueError("connection capabilities must be unique")
        return checked

    @model_validator(mode="after")
    def _typed_contract(self) -> "ProviderConnectionDefinition":
        if self.connection_type.kind != "connection-type":
            raise ValueError("connection_type must have kind 'connection-type'")
        diagnostic_ids = [item.diagnostic_id for item in self.diagnostics]
        if len(diagnostic_ids) != len(set(diagnostic_ids)):
            raise ValueError("connection diagnostic ids must be unique")
        return self

    @property
    def reference(self) -> OwnerQualifiedReference:
        return OwnerQualifiedReference(
            owner=self.owner,
            id=self.connection_id,
            kind="provider-connection",
        )


class ConnectionBindingDefinition(ProductModel):
    """Shared binding fields; each binding names exactly one typed connection."""

    binding_id: str
    connection_ref: OwnerQualifiedReference
    enabled: bool = True
    config: Mapping[str, JsonValue] = Field(default_factory=dict)
    readiness_policy: Literal["required", "deferred"] = "required"
    refresh_identity: str
    required_capabilities: tuple[str, ...] = ()

    @field_validator("binding_id", "refresh_identity")
    @classmethod
    def _identifier(cls, value: str) -> str:
        return _validate_identifier(value)

    @field_validator("required_capabilities")
    @classmethod
    def _required_capabilities(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(sorted(_validate_identifier(value) for value in values))
        if len(checked) != len(set(checked)):
            raise ValueError("binding capabilities must be unique")
        return checked

    @model_validator(mode="after")
    def _typed_connection_ref(self) -> "ConnectionBindingDefinition":
        if self.connection_ref.kind != "provider-connection":
            raise ValueError("connection_ref must have kind 'provider-connection'")
        return self


class SourceBindingDefinition(ConnectionBindingDefinition):
    """Independently controlled source bound to a reusable connection."""

    binding_kind: Literal["source"] = "source"
    source_ref: OwnerQualifiedReference

    @model_validator(mode="after")
    def _typed_source_ref(self) -> "SourceBindingDefinition":
        if self.source_ref.kind not in {"source", "source-binding"}:
            raise ValueError("source_ref must have kind 'source' or 'source-binding'")
        return self


class GovernedToolBindingDefinition(ConnectionBindingDefinition):
    """Governed live-tool binding using the same connection contract."""

    binding_kind: Literal["tool"] = "tool"
    tool_ref: OwnerQualifiedReference

    @model_validator(mode="after")
    def _typed_tool_ref(self) -> "GovernedToolBindingDefinition":
        if self.tool_ref.kind != "tool":
            raise ValueError("tool_ref must have kind 'tool'")
        return self


class LockedConnectionContract(ProductModel):
    """Secret-free connection pin retained in resolved product lock bytes."""

    connection_ref: OwnerQualifiedReference
    contract_digest: str
    capability_digest: str

    @model_validator(mode="after")
    def _typed_connection_ref(self) -> "LockedConnectionContract":
        if self.connection_ref.kind != "provider-connection":
            raise ValueError("connection_ref must have kind 'provider-connection'")
        return self

    _contract_digest = field_validator("contract_digest")(_validate_digest)
    _capability_digest = field_validator("capability_digest")(_validate_digest)


class DistributionReleaseReference(ProductModel):
    """Identity-only reference to one immutable distribution release."""

    distribution_id: str
    version: str
    owner: str = "distribution"
    digest: str | None = None

    @field_validator("distribution_id", "version", "owner")
    @classmethod
    def _reference_identifier(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_identifier(value)

    _digest = field_validator("digest")(_validate_digest)


class ProductBundleReference(ProductModel):
    """Identity-only reference to one product bundle."""

    bundle_id: str
    version: str | None = None
    owner: str = "distribution"
    digest: str | None = None

    @field_validator("bundle_id", "version", "owner")
    @classmethod
    def _reference_identifier(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_identifier(value)

    _digest = field_validator("digest")(_validate_digest)


class CompatibilityMetadata(ProductModel):
    """Wave 1 compatibility declarations; asset-DAG semantics are Wave 2."""

    contract_versions: tuple[str, ...] = ()
    capabilities: tuple[str, ...] = ()

    @field_validator("contract_versions", "capabilities")
    @classmethod
    def _identifiers(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_validate_identifier(item) for item in values)


class DistributionRelease(VersionedProductModel):
    """Immutable distribution inputs shared by embedded and external products."""

    SCHEMA: ClassVar[str] = DISTRIBUTION_RELEASE_SCHEMA
    schema_version: Literal[DISTRIBUTION_RELEASE_SCHEMA] = DISTRIBUTION_RELEASE_SCHEMA

    distribution_id: str
    version: str
    origin: Literal["embedded", "external"] = "embedded"
    release_digest: str
    owner: str = "distribution"
    asset_refs: tuple[OwnerQualifiedReference, ...] = Field(default_factory=tuple, alias="assets")
    bundle_ids: tuple[str, ...] = ()
    compatibility: CompatibilityMetadata = Field(default_factory=CompatibilityMetadata)

    @field_validator("distribution_id", "version", "owner")
    @classmethod
    def _identifier(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_identifier(value)

    _release_digest = field_validator("release_digest")(_validate_digest)

    @field_validator("bundle_ids")
    @classmethod
    def _bundle_ids(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(_validate_identifier(value) for value in values))

    @model_validator(mode="after")
    def _unique_assets(self) -> "DistributionRelease":
        keys = [(item.owner, item.ref) for item in self.asset_refs]
        if len(keys) != len(set(keys)):
            raise ValueError("distribution asset references must be unique by owner and id")
        return self

    @property
    def reference(self) -> DistributionReleaseReference:
        return DistributionReleaseReference(
            distribution_id=self.distribution_id,
            version=self.version,
            owner=self.owner,
            digest=self.release_digest,
        )


class ProductBundle(VersionedProductModel):
    """A tested, named selection of assets within one distribution release."""

    SCHEMA: ClassVar[str] = PRODUCT_BUNDLE_SCHEMA
    schema_version: Literal[PRODUCT_BUNDLE_SCHEMA] = PRODUCT_BUNDLE_SCHEMA

    bundle_id: str
    version: str
    distribution_id: str
    distribution_version: str
    owner: str = "distribution"
    bundle_digest: str
    asset_refs: tuple[OwnerQualifiedReference, ...] = Field(default_factory=tuple, alias="assets")
    requires_bundle_ids: tuple[str, ...] = ()
    conflicts_with_bundle_ids: tuple[str, ...] = ()
    compatibility: CompatibilityMetadata = Field(default_factory=CompatibilityMetadata)

    @field_validator(
        "bundle_id",
        "version",
        "distribution_id",
        "distribution_version",
        "owner",
    )
    @classmethod
    def _identifier(cls, value: str) -> str:
        return _validate_identifier(value)

    _bundle_digest = field_validator("bundle_digest")(_validate_digest)

    @field_validator("requires_bundle_ids", "conflicts_with_bundle_ids")
    @classmethod
    def _bundle_ids(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_validate_identifier(value) for value in values)

    @model_validator(mode="after")
    def _unique_assets(self) -> "ProductBundle":
        keys = [(item.owner, item.ref) for item in self.asset_refs]
        if len(keys) != len(set(keys)):
            raise ValueError("bundle asset references must be unique by owner and id")
        return self

    @property
    def reference(self) -> ProductBundleReference:
        return ProductBundleReference(
            bundle_id=self.bundle_id,
            version=self.version,
            owner=self.owner,
            digest=self.bundle_digest,
        )


class TypedOverrideInput(ProductModel):
    """An instance value supplied through a declared, typed override slot."""

    slot_id: str
    value_type: str
    value: JsonValue
    owner: str = "instance"

    @field_validator("slot_id", "value_type", "owner")
    @classmethod
    def _identifier(cls, value: str) -> str:
        return _validate_identifier(value)

    @model_validator(mode="after")
    def _secret_slot(self) -> "TypedOverrideInput":
        slot = self.slot_id.lower()
        if any(token in slot for token in ("password", "token", "secret", "api_key", "private_key")):
            raise ValueError("secret material must be supplied through SecretReference, not an override value")
        return self


OverrideInput = TypedOverrideInput


class LocalAssetDescriptor(ProductModel):
    """Descriptor for an instance-local content-digested asset (no payload)."""

    asset_id: str
    kind: str
    digest: str
    namespace: str = "local"
    source_ref: str | None = None

    @field_validator("asset_id", "kind", "namespace")
    @classmethod
    def _identifier(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_identifier(value)

    @field_validator("source_ref")
    @classmethod
    def _source_ref(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_logical_path(value)

    _asset_digest = field_validator("digest")(_validate_digest)


LocalAsset = LocalAssetDescriptor


class AuthoredDeploymentDefinition(VersionedProductModel):
    """Operator-authored desired state, containing references rather than secrets."""

    SCHEMA: ClassVar[str] = AUTHORED_DEFINITION_SCHEMA
    schema_version: Literal[AUTHORED_DEFINITION_SCHEMA] = AUTHORED_DEFINITION_SCHEMA

    definition_id: str
    distribution_release: DistributionReleaseReference | DistributionRelease = Field(
        alias="distribution_release_ref"
    )
    bundle_refs: tuple[ProductBundleReference | ProductBundle, ...] = Field(
        default_factory=tuple,
        alias="bundles",
    )
    instance_values: Mapping[str, JsonValue] = Field(default_factory=dict)
    overrides: tuple[TypedOverrideInput, ...] = ()
    local_assets: tuple[LocalAssetDescriptor, ...] = ()
    secret_references: tuple[SecretReference, ...] = ()
    connection_references: tuple[OwnerQualifiedReference, ...] = ()
    provider_connections: tuple[ProviderConnectionDefinition, ...] = ()
    source_bindings: tuple[SourceBindingDefinition, ...] = ()
    tool_bindings: tuple[GovernedToolBindingDefinition, ...] = ()

    @field_validator("definition_id")
    @classmethod
    def _identifier(cls, value: str) -> str:
        return _validate_identifier(value)

    @model_validator(mode="before")
    @classmethod
    def _accept_distribution_alias(cls, value: object) -> object:
        if isinstance(value, Mapping):
            data = dict(value)
            if "distribution_release" in data and "distribution_release_ref" not in data:
                data["distribution_release_ref"] = data.pop("distribution_release")
            if "distribution" in data and "distribution_release_ref" not in data:
                data["distribution_release_ref"] = data.pop("distribution")
            if "bundle_refs" in data and "bundles" not in data:
                data["bundles"] = data.pop("bundle_refs")
            return data
        return value

    @model_validator(mode="after")
    def _unique_overrides(self) -> "AuthoredDeploymentDefinition":
        slots = [item.slot_id for item in self.overrides]
        if len(slots) != len(set(slots)):
            raise ValueError("override slot ids must be unique")
        connection_keys = [
            (item.owner, item.connection_id) for item in self.provider_connections
        ]
        if len(connection_keys) != len(set(connection_keys)):
            raise ValueError("provider connection ids must be unique by owner and id")
        binding_ids = [
            item.binding_id for item in (*self.source_bindings, *self.tool_bindings)
        ]
        if len(binding_ids) != len(set(binding_ids)):
            raise ValueError("source and tool binding ids must be globally unique")
        refresh_ids = [
            item.refresh_identity
            for item in (*self.source_bindings, *self.tool_bindings)
        ]
        if len(refresh_ids) != len(set(refresh_ids)):
            raise ValueError("binding refresh identities must be globally unique")
        connections = {
            (item.owner, item.connection_id): item for item in self.provider_connections
        }
        for binding in (*self.source_bindings, *self.tool_bindings):
            key = (binding.connection_ref.owner, binding.connection_ref.ref)
            connection = connections.get(key)
            if connection is None:
                raise ValueError(
                    f"binding {binding.binding_id!r} refers to a missing provider connection"
                )
            missing = set(binding.required_capabilities) - set(connection.capabilities)
            if missing:
                raise ValueError(
                    f"binding {binding.binding_id!r} requires undeclared connection capabilities: "
                    + ", ".join(sorted(missing))
                )
        return self

    @property
    def bundles(self) -> tuple[ProductBundleReference | ProductBundle, ...]:
        return self.bundle_refs


class ProvenanceRecord(ProductModel):
    """Stable owner-qualified provenance for a selected value/reference."""

    path: str
    owner: str
    reference: OwnerQualifiedReference
    source: Literal[
        "distribution",
        "bundle",
        "definition",
        "local",
        "override",
        "package",
        "environment",
        "compatibility",
    ]
    value_digest: str

    @field_validator("owner")
    @classmethod
    def _identifier(cls, value: str) -> str:
        return _validate_identifier(value)

    _logical_path = field_validator("path")(_validate_logical_path)
    _value_digest = field_validator("value_digest")(_validate_digest)


class ProductIdentity(ProductModel):
    """Identity of a selection, independent of checkout or runtime location."""

    distribution_id: str
    distribution_version: str
    bundle_ids: tuple[str, ...]
    definition_id: str
    origin: Literal["embedded", "external"]

    @field_validator("distribution_id", "distribution_version", "definition_id")
    @classmethod
    def _identifier(cls, value: str) -> str:
        return _validate_identifier(value)

    @field_validator("bundle_ids")
    @classmethod
    def _bundle_ids(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(_validate_identifier(value) for value in values))


class TypedArtifactIdentity(OwnerQualifiedReference):
    """Qualified package/environment identity pinned by a resolved lock."""

    kind: str
    version: str
    digest: str

    _identity_digest = field_validator("digest")(_validate_digest)


PackageIdentity = TypedArtifactIdentity
EnvironmentIdentity = TypedArtifactIdentity


class CompatibilityDecision(ProductModel):
    """One explicit compatibility decision retained in a resolved lock."""

    subject: OwnerQualifiedReference
    decision: Literal["accepted", "required", "rejected", "compatible"]
    reason: Literal[
        "bundle-conflict",
        "bundle-dependency-satisfied",
        "capability-declared",
        "capability-missing",
        "capability-satisfied",
        "contract-version-compatible",
        "contract-version-incompatible",
        "operator-approved",
        "tested-bundle-set",
    ]
    owner: str = "resolver"

    @field_validator("owner")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("compatibility decision text must not be empty")
        return value


class ResolvedLock(VersionedProductModel):
    """Secret-free product authority emitted by resolution (Wave 1 shape)."""

    SCHEMA: ClassVar[str] = RESOLVED_LOCK_SCHEMA
    schema_version: Literal[RESOLVED_LOCK_SCHEMA] = RESOLVED_LOCK_SCHEMA

    definition_id: str
    definition_digest: str
    product_identity: ProductIdentity
    distribution_release_ref: DistributionReleaseReference
    bundle_refs: tuple[ProductBundleReference, ...] = ()
    asset_refs: tuple[OwnerQualifiedReference, ...] = ()
    package_identities: tuple[TypedArtifactIdentity, ...] = Field(default_factory=tuple, alias="package_refs")
    environment_identities: tuple[TypedArtifactIdentity, ...] = Field(
        default_factory=tuple,
        alias="environment_refs",
    )
    connection_contracts: tuple[LockedConnectionContract, ...] = ()
    local_asset_digests: Mapping[str, str] = Field(default_factory=dict)
    compatibility_decisions: tuple[CompatibilityDecision, ...] = ()
    override_provenance: tuple[ProvenanceRecord, ...] = ()
    provenance: tuple[ProvenanceRecord, ...] = ()
    composition_digest: str

    @field_validator("definition_id")
    @classmethod
    def _identifier(cls, value: str) -> str:
        return _validate_identifier(value)

    _definition_digest = field_validator("definition_digest")(_validate_digest)
    _composition_digest = field_validator("composition_digest")(_validate_digest)

    @field_validator("local_asset_digests")
    @classmethod
    def _digest_maps(cls, values: Mapping[str, str]) -> Mapping[str, str]:
        return {_validate_logical_path(key): _validate_digest(value) for key, value in values.items()}  # type: ignore[return-value]

    @field_validator("bundle_refs")
    @classmethod
    def _canonical_bundle_order(cls, values: tuple[ProductBundleReference, ...]) -> tuple[ProductBundleReference, ...]:
        return tuple(sorted(values, key=lambda item: (item.owner, item.bundle_id, item.version or "")))

    @model_validator(mode="after")
    def _identity_matches(self) -> "ResolvedLock":
        identity = self.product_identity
        release = self.distribution_release_ref
        if identity.distribution_id != release.distribution_id or identity.distribution_version != release.version:
            raise ValueError("lock product identity does not match its distribution release reference")
        if tuple(item.bundle_id for item in self.bundle_refs) != identity.bundle_ids:
            raise ValueError("lock bundle references do not match product identity")
        refs = [
            (item.connection_ref.owner, item.connection_ref.ref)
            for item in self.connection_contracts
        ]
        if len(refs) != len(set(refs)):
            raise ValueError("lock connection contracts must be unique by owner and id")
        return self


class SealedReleaseReference(VersionedProductModel):
    """Reference to the existing content-addressed ``okg.sealed_release/v1``."""

    SCHEMA: ClassVar[str] = SEALED_RELEASE_SCHEMA
    schema_version: Literal[SEALED_RELEASE_SCHEMA] = Field(
        default=SEALED_RELEASE_SCHEMA,
        alias="schema",
    )

    artifact_id: str
    input_id: str | None = None
    definition_digest: str | None = None
    artifact_root: str | None = None
    authority: Literal[SEALED_RELEASE_SCHEMA] = SEALED_RELEASE_SCHEMA

    @field_validator("input_id", "definition_digest")
    @classmethod
    def _identifier(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_identifier(value)

    _artifact_path = field_validator("artifact_root")(_validate_logical_path)

    _artifact_digest = field_validator("artifact_id")(_validate_digest)
    _input_digest = field_validator("input_id", "definition_digest")(_validate_digest)

    @model_validator(mode="after")
    def _authority_is_existing_release(self) -> "SealedReleaseReference":
        if self.schema_version != SEALED_RELEASE_SCHEMA or self.authority != SEALED_RELEASE_SCHEMA:
            raise ValueError("sealed release references must use the existing okg.sealed_release/v1 authority")
        return self


class AppliedInstance(VersionedProductModel):
    """Applied product identity with references to external runtime state only."""

    SCHEMA: ClassVar[str] = APPLIED_INSTANCE_SCHEMA
    schema_version: Literal[APPLIED_INSTANCE_SCHEMA] = APPLIED_INSTANCE_SCHEMA

    instance_id: str
    definition_id: str
    lock_ref: OwnerQualifiedReference | None = None
    resolved_lock: ResolvedLock | None = None
    sealed_release_ref: SealedReleaseReference = Field(alias="sealed_release")
    runtime_ref: OwnerQualifiedReference | None = None
    journal_id: str | None = None
    operation_id: str | None = None

    @field_validator("instance_id", "definition_id", "journal_id", "operation_id")
    @classmethod
    def _identifier(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_identifier(value)

    @model_validator(mode="after")
    def _lock_matches_definition(self) -> "AppliedInstance":
        if self.resolved_lock is not None and self.resolved_lock.definition_id != self.definition_id:
            raise ValueError("applied instance definition_id does not match its resolved lock")
        return self


# Compatibility spelling used by the parent design and setup surfaces.
DeploymentDefinition = AuthoredDeploymentDefinition


__all__ = [
    "API_VERSION",
    "APPLIED_INSTANCE_SCHEMA",
    "AppliedInstance",
    "AssetReference",
    "AUTHORED_DEFINITION_SCHEMA",
    "AuthoredDeploymentDefinition",
    "CONTRACT_SCHEMA",
    "CONTRACT_VERSION",
    "DEPLOYMENT_PRODUCT_API_VERSION",
    "DEPLOYMENT_PRODUCT_CONTRACT_VERSION",
    "DEPLOYMENT_PRODUCT_SCHEMA",
    "CompatibilityMetadata",
    "CompatibilityDecision",
    "ConnectionBindingDefinition",
    "ConnectionProxyPolicy",
    "ConnectionTlsPolicy",
    "DISTRIBUTION_RELEASE_SCHEMA",
    "DeploymentDefinition",
    "DistributionRelease",
    "DistributionReleaseReference",
    "EnvironmentIdentity",
    "GovernedToolBindingDefinition",
    "LocalAsset",
    "LocalAssetDescriptor",
    "LockedConnectionContract",
    "OBJECT_MODEL_VERSION",
    "OwnerQualifiedRef",
    "OwnerQualifiedReference",
    "OverrideInput",
    "ProductBundle",
    "ProductBundleReference",
    "PRODUCT_BUNDLE_SCHEMA",
    "ProductIdentity",
    "ProductModel",
    "ProviderConnectionDefinition",
    "ProviderDiagnosticDefinition",
    "ProvenanceRecord",
    "PackageIdentity",
    "ResolvedLock",
    "RESOLVED_LOCK_SCHEMA",
    "SCHEMA_VERSION",
    "SEALED_RELEASE_SCHEMA",
    "SealedReleaseReference",
    "SecretReference",
    "StableOwnerQualifiedRef",
    "StableReference",
    "SourceBindingDefinition",
    "TypedOverrideInput",
    "TypedArtifactIdentity",
    "VersionedProductModel",
]
