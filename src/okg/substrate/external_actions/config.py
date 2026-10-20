"""Strict loading of deployment-owned external-action configuration.

The config is trust material, not user input.  Loading is intentionally
explicit: malformed YAML, unknown keys, invalid digests and missing trust
settings raise :class:`ConfigError`; no permissive or unauthenticated default
is installed.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import jsonschema
import yaml

from .signing import SigningError, SigningKey


class ConfigError(ValueError):
    code = "invalid_external_action_config"

    def __init__(self, message: str, *, path: str = "$") -> None:
        super().__init__(message)
        self.path = path


_DEPLOYMENT_RE = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+\-]{0,255}$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_TIMESTAMP_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$")


class _UniqueSafeLoader(yaml.SafeLoader):
    """YAML loader that does not silently choose a duplicate key."""


# ``SafeLoader`` turns RFC-3339 values into ``datetime`` objects.  Contract
# payloads carry their timestamps as strings, and the deployment-product
# canonicalizer intentionally accepts only JSON values.  Keep scalar text
# intact so loading and hashing a YAML plan is byte-equivalent to its JSON
# representation. Copy the inherited resolver table before editing it: mutating
# the shared lists would silently remove timestamp resolution from every other
# SafeLoader/CSafeLoader consumer in the process.
_UniqueSafeLoader.yaml_implicit_resolvers = {
    initial: list(resolvers)
    for initial, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
for _initial, _resolvers in list(_UniqueSafeLoader.yaml_implicit_resolvers.items()):
    _UniqueSafeLoader.yaml_implicit_resolvers[_initial] = [
        (_tag, _regexp)
        for _tag, _regexp in _resolvers
        if _tag != "tag:yaml.org,2002:timestamp"
    ]


def _construct_unique_mapping(loader: _UniqueSafeLoader, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise ConfigError("configuration contains a duplicate key")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueSafeLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping)


@dataclass(frozen=True)
class RequestLimits:
    request_body_bytes_maximum: int = 262_144
    canonical_payload_bytes_maximum: int = 131_072
    json_nesting_depth_maximum: int = 32
    total_object_members_maximum: int = 512
    total_array_items_maximum: int = 512
    total_scalar_values_maximum: int = 4096
    individual_string_utf8_bytes_maximum: int = 16_384
    target_ref_utf8_bytes_maximum: int = 256
    evidence_digest_count_maximum: int = 64
    diff_field_code_count_maximum: int = 128

    def __post_init__(self) -> None:
        for name, value in self.__dict__.items():
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ConfigError(f"{name} must be a positive integer", path=f"$.limits.{name}")
        if self.request_body_bytes_maximum > 262_144:
            raise ConfigError("request body limit loosens the closed contract", path="$.limits.request_body_bytes_maximum")
        if self.canonical_payload_bytes_maximum > 131_072:
            raise ConfigError("canonical payload limit loosens the closed contract", path="$.limits.canonical_payload_bytes_maximum")


@dataclass(frozen=True)
class AuthConfig:
    issuer: str | None = None
    audience: str = "deployment_scoped_review_api"
    allowed_algorithms: tuple[str, ...] = ("EdDSA",)
    max_token_lifetime_seconds: int = 300
    clock_skew_seconds: int = 30
    jwks: Mapping[str, str] = None  # type: ignore[assignment]
    trust_roots: tuple[str, ...] = ()
    trusted_proxy_names: tuple[str, ...] = ()
    static_bearer: bool = False
    development_loopback_only: bool = True

    def __post_init__(self) -> None:
        if self.jwks is None:
            object.__setattr__(self, "jwks", {})
        if not self.audience or not isinstance(self.audience, str):
            raise ConfigError("auth audience is required", path="$.authentication.audience")
        if self.max_token_lifetime_seconds <= 0 or self.max_token_lifetime_seconds > 300:
            raise ConfigError("token lifetime must be in 1..300 seconds", path="$.authentication.max_token_lifetime_seconds")
        if self.clock_skew_seconds < 0 or self.clock_skew_seconds > 300:
            raise ConfigError("clock skew must be in 0..300 seconds", path="$.authentication.clock_skew_seconds")
        if self.static_bearer and not self.development_loopback_only:
            raise ConfigError("static bearer is permitted only for loopback development", path="$.authentication.static_bearer")
        if not self.allowed_algorithms or any(a not in {"EdDSA", "RS256", "ES256"} for a in self.allowed_algorithms):
            raise ConfigError("authentication algorithm is not in the supported allowlist", path="$.authentication.allowed_algorithms")


@dataclass(frozen=True)
class DeploymentProductAuthority:
    """Deployment-owned inputs that authorize an external-action registry.

    The paths are locators only.  The files are parsed and verified at the
    launch boundary; no path or plan digest is treated as authority by itself.
    """

    authored_definition_path: str
    product_plan_path: str
    expected_product_plan_digest: str

    def __post_init__(self) -> None:
        for name, value in (
            ("authored_definition_path", self.authored_definition_path),
            ("product_plan_path", self.product_plan_path),
        ):
            if not isinstance(value, str) or not value or "\x00" in value:
                raise ConfigError(
                    f"{name} must be a non-empty path",
                    path=f"$.deployment_product_authority.{name}",
                )
        if not isinstance(self.expected_product_plan_digest, str) or not _DIGEST_RE.fullmatch(
            self.expected_product_plan_digest
        ):
            raise ConfigError(
                "expected_product_plan_digest must be sha256:<64 lowercase hex>",
                path="$.deployment_product_authority.expected_product_plan_digest",
            )


@dataclass(frozen=True)
class ExternalActionConfig:
    schema: str
    deployment: str
    authentication: AuthConfig
    limits: RequestLimits
    signing_keys: tuple[SigningKey, ...]
    database_dsn: str | None = None
    route_alias: str | None = None
    bind_host: str = "127.0.0.1"
    bind_port: int = 0
    action_registry_path: str | None = None
    service_mode: str | None = None
    policy: Mapping[str, Any] | None = None
    deployment_product_authority: DeploymentProductAuthority | None = None
    verifier_capability_env: str | None = None

    def key_for(self, kid: str, *, purpose: str, at: Any = None) -> SigningKey:
        matches = [key for key in self.signing_keys if key.kid == kid and key.purpose == purpose]
        if len(matches) != 1:
            raise ConfigError("signing key is missing or ambiguous", path="$.signing_keys")
        key = matches[0]
        if key.state == "revoked_compromised":
            raise ConfigError("revoked signing key cannot verify an envelope", path="$.signing_keys")
        return key


def _load_data(source: Mapping[str, Any] | str | Path) -> Mapping[str, Any]:
    if isinstance(source, Mapping):
        data = dict(source)
    else:
        path = Path(source)
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConfigError("configuration cannot be read") from exc
        try:
            data = yaml.load(text, Loader=_UniqueSafeLoader)
        except yaml.YAMLError as exc:
            raise ConfigError("configuration is not valid YAML") from exc
    if not isinstance(data, Mapping):
        raise ConfigError("configuration root must be an object")
    return dict(data)


def _schema_validate(data: Mapping[str, Any]) -> None:
    schema_path = Path(__file__).with_name("external-action-review.schema.json")
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ConfigError("external-action review schema is unavailable") from exc
    try:
        jsonschema.Draft202012Validator(schema).validate(data)
    except jsonschema.ValidationError as exc:
        location = "$" + "".join(f"[{part!r}]" for part in exc.absolute_path)
        raise ConfigError("configuration failed strict schema validation", path=location) from exc


def _require_identifier(value: Any, name: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or not _ID_RE.fullmatch(value):
        raise ConfigError(f"{name} is not a safe ASCII identifier", path=f"$.{name}")
    return value


def load_config(source: Mapping[str, Any] | str | Path) -> ExternalActionConfig:
    """Load and validate ``external-action-review.yaml``."""

    data = _load_data(source)
    _schema_validate(data)
    schema_name = data.get("schema")
    deployment = data.get("deployment")
    if schema_name != "okg.external_action_review.config/v1":
        raise ConfigError("unsupported external-action review config schema", path="$.schema")
    if not isinstance(deployment, str) or not _DEPLOYMENT_RE.fullmatch(deployment):
        raise ConfigError("deployment must be an immutable lowercase authority id", path="$.deployment")

    raw_auth = data["authentication"]
    auth = AuthConfig(
        issuer=raw_auth.get("issuer"),
        audience=raw_auth.get("audience", "deployment_scoped_review_api"),
        allowed_algorithms=tuple(raw_auth.get("allowed_algorithms", ["EdDSA"])),
        max_token_lifetime_seconds=raw_auth.get("max_token_lifetime_seconds", 300),
        clock_skew_seconds=raw_auth.get("clock_skew_seconds", 30),
        jwks=dict(raw_auth.get("jwks", {})),
        trust_roots=tuple(raw_auth.get("trust_roots", [])),
        trusted_proxy_names=tuple(raw_auth.get("trusted_proxy_names", [])),
        static_bearer=raw_auth.get("static_bearer", False),
        development_loopback_only=raw_auth.get("development_loopback_only", True),
    )
    raw_limits = data.get("limits", {})
    limits = RequestLimits(**raw_limits)

    raw_product_authority = data.get("deployment_product_authority")
    product_authority = None
    if raw_product_authority is not None:
        try:
            product_authority = DeploymentProductAuthority(**raw_product_authority)
        except (TypeError, ValueError) as exc:
            if isinstance(exc, ConfigError):
                raise
            raise ConfigError(
                "deployment product authority is invalid",
                path="$.deployment_product_authority",
            ) from exc

    signing_keys: list[SigningKey] = []
    seen_kids: set[str] = set()
    for index, raw_key in enumerate(data["signing_keys"]):
        if raw_key["kid"] in seen_kids:
            raise ConfigError("signing key ids must be unique", path=f"$.signing_keys[{index}].kid")
        seen_kids.add(raw_key["kid"])
        try:
            signing_keys.append(SigningKey(**raw_key))
        except (SigningError, TypeError, ValueError) as exc:
            raise ConfigError("signing key is invalid", path=f"$.signing_keys[{index}]") from exc

    return ExternalActionConfig(
        schema=schema_name,
        deployment=deployment,
        authentication=auth,
        limits=limits,
        signing_keys=tuple(signing_keys),
        database_dsn=data.get("database_dsn"),
        route_alias=data.get("route_alias"),
        bind_host=data.get("bind_host", "127.0.0.1"),
        bind_port=data.get("bind_port", 0),
        action_registry_path=data.get("action_registry_path"),
        service_mode=data.get("service_mode"),
        policy=data.get("policy"),
        deployment_product_authority=product_authority,
        verifier_capability_env=data.get("verifier_capability_env"),
    )


def load_review_config(source: Mapping[str, Any] | str | Path) -> ExternalActionConfig:
    return load_config(source)


def validate_config(source: Mapping[str, Any] | str | Path) -> ExternalActionConfig:
    return load_config(source)


_PLAN_FIELDS = frozenset(
    {
        "schema_version",
        "api_version",
        "lock",
        "lock_digest",
        "definition_digest",
        "artifact_digests",
        "expires_at",
        "actions",
        "plan_digest",
    }
)
_PLAN_ACTION_FIELDS = frozenset({"action_id", "kind", "arguments"})
_PLAN_ARTIFACT_FIELDS = frozenset({"logical_id", "digest"})
_PLAN_SCHEMA = "okg.deployment_product.plan/v1"
_PLAN_API = "okg.deployment_product.api/v1"


def _timestamp(value: Any, *, path: str) -> datetime:
    if not isinstance(value, str) or not _TIMESTAMP_RE.fullmatch(value):
        raise ConfigError("plan timestamp must use six fractional digits and UTC Z", path=path)
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError as exc:
        raise ConfigError("plan timestamp is invalid", path=path) from exc
    return parsed.replace(tzinfo=timezone.utc)


def load_deployment_product_authority(
    authority: DeploymentProductAuthority,
    *,
    deployment: str | None = None,
) -> tuple[Any, Any, Any]:
    """Read and verify one PR #1332-compatible product plan.

    This is deliberately owned by the external-action boundary.  It uses the
    deployment-product canonical bytes and lock canonicalizer, but does not
    invoke asset resolution, connection operations, or any provider adapter.
    The return value is ``(definition, canonical_lock, plan)`` for the action
    contract resolver and startup diagnostics.
    """

    if not isinstance(authority, DeploymentProductAuthority):
        raise ConfigError("deployment product authority is unavailable", path="$.deployment_product_authority")
    try:
        from okg.substrate.contracts.deployment_product.models import (
            API_VERSION,
            AuthoredDeploymentDefinition,
            ResolvedLock,
        )
        from okg.substrate.contracts.deployment_product.lock import (
            DeploymentProductPlan,
            PlanAction,
            PLAN_SCHEMA,
            authored_definition_digest,
            canonical_lock_bytes,
            canonical_json_bytes,
            canonicalize_lock,
            sha256_digest,
        )

        definition_data = _load_data(authority.authored_definition_path)
        definition = AuthoredDeploymentDefinition.model_validate(definition_data)

        plan_data = _load_data(authority.product_plan_path)
        if set(plan_data) != _PLAN_FIELDS:
            unknown = sorted(set(plan_data) - _PLAN_FIELDS)
            missing = sorted(_PLAN_FIELDS - set(plan_data))
            detail = []
            if missing:
                detail.append("missing " + ", ".join(missing))
            if unknown:
                detail.append("unknown " + ", ".join(unknown))
            raise ConfigError(
                "product plan is not a closed mapping (" + "; ".join(detail) + ")",
                path="$.product_plan",
            )
        if plan_data["schema_version"] != PLAN_SCHEMA or plan_data["api_version"] != API_VERSION:
            raise ConfigError("product plan schema or API version is unsupported", path="$.product_plan")

        plan_digest = plan_data["plan_digest"]
        if not isinstance(plan_digest, str) or not _DIGEST_RE.fullmatch(plan_digest):
            raise ConfigError("product plan digest is invalid", path="$.product_plan.plan_digest")
        expected = authority.expected_product_plan_digest
        if plan_digest != expected:
            raise ConfigError("product plan digest does not match expected authority", path="$.deployment_product_authority.expected_product_plan_digest")
        without_plan_digest = dict(plan_data)
        without_plan_digest.pop("plan_digest")
        if sha256_digest(canonical_json_bytes(without_plan_digest)) != plan_digest:
            raise ConfigError("product plan digest is invalid", path="$.product_plan.plan_digest")

        raw_lock = plan_data["lock"]
        if not isinstance(raw_lock, Mapping):
            raise ConfigError("product plan lock must be an object", path="$.product_plan.lock")
        # ProductModel is intentionally strict about tuple-valued fields,
        # while the PR #1332 payload is JSON/YAML and therefore contains
        # arrays.  Normalize only the lock's registered sequence fields at
        # this boundary; nested model validators still reject all unknown or
        # unsafe values.
        raw_lock = dict(raw_lock)
        for field in (
            "bundle_refs",
            "asset_refs",
            "package_refs",
            "environment_refs",
            "connection_contracts",
            "compatibility_decisions",
            "override_provenance",
            "provenance",
        ):
            if isinstance(raw_lock.get(field), list):
                raw_lock[field] = tuple(raw_lock[field])
        product_identity = raw_lock.get("product_identity")
        if isinstance(product_identity, Mapping):
            product_identity = dict(product_identity)
            if isinstance(product_identity.get("bundle_ids"), list):
                product_identity["bundle_ids"] = tuple(product_identity["bundle_ids"])
            raw_lock["product_identity"] = product_identity
        lock = canonicalize_lock(ResolvedLock.model_validate(raw_lock))
        lock_digest = plan_data["lock_digest"]
        if not isinstance(lock_digest, str) or not _DIGEST_RE.fullmatch(lock_digest):
            raise ConfigError("product plan lock digest is invalid", path="$.product_plan.lock_digest")
        if sha256_digest(canonical_lock_bytes(lock)) != lock_digest:
            raise ConfigError("product plan lock digest is invalid", path="$.product_plan.lock_digest")

        definition_digest = plan_data["definition_digest"]
        if not isinstance(definition_digest, str) or not _DIGEST_RE.fullmatch(definition_digest):
            raise ConfigError("product plan definition digest is invalid", path="$.product_plan.definition_digest")
        observed_definition_digest = authored_definition_digest(definition)
        if definition_digest != observed_definition_digest or lock.definition_digest != observed_definition_digest:
            raise ConfigError("product plan definition digest does not match the authored definition and lock", path="$.product_plan.definition_digest")
        if deployment is not None and not isinstance(deployment, str):
            raise ConfigError("deployment authority is invalid", path="$.deployment")

        artifacts = plan_data["artifact_digests"]
        if not isinstance(artifacts, list):
            raise ConfigError("product plan artifact_digests must be a list", path="$.product_plan.artifact_digests")
        artifact_values: list[tuple[str, str]] = []
        for index, item in enumerate(artifacts):
            if not isinstance(item, Mapping) or set(item) != _PLAN_ARTIFACT_FIELDS:
                raise ConfigError("product plan artifact entry is not closed", path=f"$.product_plan.artifact_digests[{index}]")
            logical_id, digest = item["logical_id"], item["digest"]
            if (
                not isinstance(logical_id, str)
                or not logical_id
                or any(char.isspace() for char in logical_id)
                or "\x00" in logical_id
                or "\\" in logical_id
                or logical_id.startswith("/")
                or re.match(r"^[A-Za-z]:", logical_id)
                or any(segment in {"", ".", ".."} for segment in logical_id.split("/"))
            ):
                raise ConfigError("product plan artifact logical_id is invalid", path=f"$.product_plan.artifact_digests[{index}].logical_id")
            if not isinstance(digest, str) or not _DIGEST_RE.fullmatch(digest):
                raise ConfigError("product plan artifact digest is invalid", path=f"$.product_plan.artifact_digests[{index}].digest")
            artifact_values.append((logical_id, digest))
        if artifact_values != sorted(artifact_values) or len({item[0] for item in artifact_values}) != len(artifact_values):
            raise ConfigError("product plan artifacts are not canonical and unique", path="$.product_plan.artifact_digests")

        actions = plan_data["actions"]
        if not isinstance(actions, list):
            raise ConfigError("product plan actions must be a list", path="$.product_plan.actions")
        plan_actions: list[Any] = []
        for index, item in enumerate(actions):
            if not isinstance(item, Mapping) or set(item) != _PLAN_ACTION_FIELDS:
                raise ConfigError("product plan action entry is not closed", path=f"$.product_plan.actions[{index}]")
            try:
                plan_actions.append(PlanAction(action_id=item["action_id"], kind=item["kind"], arguments=item["arguments"]))
            except (TypeError, ValueError) as exc:
                raise ConfigError("product plan action entry is invalid", path=f"$.product_plan.actions[{index}]") from exc
        if [(item.action_id, item.kind) for item in plan_actions] != sorted((item.action_id, item.kind) for item in plan_actions):
            raise ConfigError("product plan actions are not canonical", path="$.product_plan.actions")
        expires_at = _timestamp(plan_data["expires_at"], path="$.product_plan.expires_at")
        plan = DeploymentProductPlan(
            lock=lock,
            lock_digest=lock_digest,
            definition_digest=definition_digest,
            artifact_digests=tuple(artifact_values),
            expires_at=expires_at,
            actions=tuple(plan_actions),
            plan_digest=plan_digest,
            schema_version=plan_data["schema_version"],
            api_version=plan_data["api_version"],
        )
        return definition, lock, plan
    except ConfigError:
        raise
    except (ImportError, AttributeError, OSError, TypeError, ValueError, KeyError) as exc:
        raise ConfigError("deployment product authority is unavailable", path="$.deployment_product_authority") from exc


def load_action_contracts(source: Mapping[str, Any] | str | Path) -> Mapping[str, Any]:
    """Validate a deployment action registry and return its immutable mapping."""

    data = _load_data(source)
    schema_path = Path(__file__).with_name("external-action-contracts.schema.json")
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        jsonschema.Draft202012Validator(schema).validate(data)
    except jsonschema.ValidationError as exc:
        raise ConfigError("action registry failed strict schema validation") from exc
    except (OSError, ValueError) as exc:
        raise ConfigError("action registry schema is unavailable") from exc
    return data


__all__ = [
    "ConfigError",
    "RequestLimits",
    "AuthConfig",
    "DeploymentProductAuthority",
    "ExternalActionConfig",
    "load_config",
    "load_review_config",
    "validate_config",
    "load_action_contracts",
    "load_deployment_product_authority",
]
