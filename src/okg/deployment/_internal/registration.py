"""Connector registration metadata (#1181 slice 4).

Slice 3 gave a connector the shape it *runs* in. This slice gives it the
shape it is *declared* in: one frozen `ConnectorRegistration` an external
package authors per connector, and one pure function turning it into the
source-registry entry the landed strict admission already accepts.

The registration is data, never behaviour. It carries no import hook and
no discovery side effect: a package declares its registrations as typed
`connector` assets (kind ``connector``, plane ``ingestion-projection``,
already in `CORE_ASSET_KINDS`), keyed by the owner-qualified module ids
in #1179's package-v2 `module_declarations`. `as_asset_payload` and
`from_asset_payload` are that round trip. There is deliberately no
`pyproject.toml` entry point and no import-based scan: #1178 forbids
path or import injection, and a wheel that is imported to be discovered
has already run.

Why `configuration_hints`
-------------------------
`strict_source` is `additionalProperties: false`, so SDK-declared
metadata with no registry field of its own — the connector's own
version, its auth modes, its capabilities, the ontology modules it
needs, its SDK window, its deprecation note — rides in the entry's
`configuration_hints` object under one namespaced key. The fields the
registry *does* model (`sync`, `output_signature`, the record-identity
contract, `credential_refs`) map onto their real homes, so admission
sees them exactly as it sees a hand-written entry's.

Enrichers are not a thin sibling and get no `EnricherRegistration`
here. A connector's declaration has a landed target — a
`source_registry.yaml` row admitted by `admit_source_registry_document`.
An enricher's does not: enrichers are a bare class list, are handed a
live `psycopg` connection inside the publisher's transaction, and their
read surface is slice 5's open design question. Declaring their metadata
before that surface exists would fix a shape against an unknown
protocol, so slice 5 owns it.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Mapping, Optional, Sequence

from jsonschema import Draft202012Validator, SchemaError
from packaging.version import InvalidVersion, Version

from okg.substrate.deployment_bootstrap.workflow import schemas_dir
from okg.substrate.errors import (
    ConnectorAssetPayloadInvalid,
    ConnectorAuthModeUnknown,
    ConnectorCapabilityUnknown,
    ConnectorConfigSchemaInvalid,
    ConnectorOutputSignatureInvalid,
    ConnectorRegistrationDeprecated,
    ConnectorRegistrationError,
    ConnectorRequiredModuleUnqualified,
    ConnectorSdkBoundInvalid,
    ConnectorSdkVersionUnsupported,
)

#: Credential custody modes a connector may declare. Each names a landed
#: mechanism: nothing (`none`), an environment reference resolved by
#: `credential_preflight`, a credential file resolved by `file_preflight`,
#: an OS-keychain static token (`library/oauth/static_token.py`), or the
#: authorization-code flow (`library/oauth/flow.py`).
AUTH_MODES = frozenset({
    "none",
    "env_credential",
    "file_credential",
    "keychain_token",
    "oauth2_authorization_code",
})

#: Optional connector behaviours the runner already honours. Each entry is
#: a field the landed protocol or run loop reads, so a declaration can be
#: checked against behaviour rather than believed.
CONNECTOR_CAPABILITIES = frozenset({
    "authoritative_replay",       # ConnectorContext.authoritative_replay
    "cancellation",               # ConnectorContext.cancelled()
    "change_probe",               # ConnectorContext.change_probe_verdict
    "checkpoint_resume",          # ConnectorRun.next_checkpoint
    "partial_health",             # SOURCE_RUN_PARTIAL_STATUS
    "progress_fingerprints",      # ConnectorContext.since_progress
    "record_authority_stream",    # ConnectorRun.record_authority_stream
    "record_set_authority",       # ConnectorRun.record_set
    "scoped_replacement",         # ConnectorRun.record_set_replacement_keys
    "scoped_run",                 # ConnectorContext.sync_scope
})

#: The typed-asset kind a declared registration rides on (package v2).
CONNECTOR_ASSET_KIND = "connector"
_ASSET_SCHEMA = "okg.deployment_product.asset.connector/v1"
#: Namespaced key holding SDK metadata the registry has no field for.
_HINTS_KEY = "okg_connector"


@lru_cache(maxsize=1)
def _subschema_validators() -> tuple[Draft202012Validator, Draft202012Validator]:
    """Validators for the landed `output_signature` and `sync` shapes.

    Built from `schemas/source_registry.schema.json` itself rather than
    restated here, so a registration is refused by the same definition
    strict admission will apply to the entry it produces.
    """
    defs = json.loads(
        (schemas_dir() / "source_registry.schema.json").read_text(
            encoding="utf-8",
        )
    )["$defs"]
    return (
        Draft202012Validator(
            {"$ref": "#/$defs/strict_output_signature", "$defs": defs}
        ),
        Draft202012Validator({"$ref": "#/$defs/strict_sync", "$defs": defs}),
    )


@dataclass(frozen=True)
class SdkVersionRange:
    """The `okg.deployment` window one connector declares it needs.

    #1179's shape, not a range expression: `min_version` is inclusive,
    `max_version_exclusive` is exclusive, and a build is compared by its
    base release, so `1.1.0rc1` and `1.1.0` fall on the same side of every
    bound. A range *string* passes release-level validation silently and
    then fails a membership test it was never meant to answer, which is
    why #1179 rejected one.
    """

    min_version: str
    max_version_exclusive: str

    def __post_init__(self) -> None:
        for field_name in ("min_version", "max_version_exclusive"):
            value = getattr(self, field_name)
            try:
                parsed = Version(value)
            except (InvalidVersion, TypeError) as exc:
                raise ConnectorSdkBoundInvalid(
                    f"sdk_range.{field_name} must be a PEP 440 version, "
                    f"got {value!r}",
                    data={"field": field_name, "value": str(value)},
                ) from exc
            # A bound is compared against a *base release* (`supports`
            # below), so a pre-release, post-release, dev, local or epoch
            # bound could never change a verdict; package v2 refuses one on
            # `okg_compatibility` for that reason and this refuses it here,
            # rather than normalizing it away behind the author's back.
            if (
                parsed.epoch
                or parsed.pre
                or parsed.post
                or parsed.dev
                or parsed.local
                or str(parsed) != value
            ):
                raise ConnectorSdkBoundInvalid(
                    f"sdk_range.{field_name} must be a plain, normalized "
                    f"PEP 440 release version, got {value!r}",
                    data={"field": field_name, "value": str(value)},
                    fix_hint=(
                        "declare the release the window turns on, such as "
                        "'1.1.0'; the running build is compared by its base "
                        "release"
                    ),
                )
        if Version(self.min_version) >= Version(self.max_version_exclusive):
            raise ConnectorRegistrationError(
                "sdk_range.min_version must precede max_version_exclusive",
                data={
                    "min_version": self.min_version,
                    "max_version_exclusive": self.max_version_exclusive,
                },
            )

    def supports(self, sdk_version: str) -> bool:
        """Whether `sdk_version`'s base release falls inside the window."""
        observed = Version(Version(sdk_version).base_version)
        return (
            Version(self.min_version)
            <= observed
            < Version(self.max_version_exclusive)
        )


@dataclass(frozen=True)
class ConnectorRegistration:
    """One connector's declaration, validated the moment it is built.

    Every data defect — an unknown capability or auth mode, a malformed
    `output_signature` or `sync` block, a required module id that is not
    strictly `owner:name`, a `config_schema` that is not a JSON Schema —
    refuses here, so a registration object that exists is one the registry
    mapping can always consume. The two checks that depend on the *running*
    substrate, the SDK window and deprecation, belong to
    `registration_registry_entry`.
    """

    connector_type: str
    version: str
    module: str
    class_name: str
    ownership_id: str
    source_class: str
    trust_label: str
    sdk_range: SdkVersionRange
    config_schema: Mapping[str, Any] = field(default_factory=dict)
    auth_modes: Sequence[str] = ()
    capabilities: Sequence[str] = ()
    required_modules: Sequence[str] = ()
    output_signature: Mapping[str, Any] = field(default_factory=dict)
    sync: Mapping[str, Any] = field(default_factory=dict)
    credential_refs: Sequence[str] = ()
    record_identity_kind: Optional[str] = None
    record_identity_fields: Sequence[str] = ()
    source_revision_kind: Optional[str] = None
    deletion_semantics: Optional[str] = None
    publication_mode: Optional[str] = None
    admission_mode: str = "direct"
    description: Optional[str] = None
    deprecation: Optional[str] = None

    def __post_init__(self) -> None:
        # Normalize the sequence fields to tuples so two registrations
        # authored with a list and a tuple compare equal, and so a declared
        # asset round-trips to an equal object rather than a near-copy.
        for name in (
            "auth_modes",
            "capabilities",
            "required_modules",
            "credential_refs",
            "record_identity_fields",
        ):
            object.__setattr__(self, name, tuple(getattr(self, name)))
        self._closed(
            self.auth_modes, AUTH_MODES, "auth_mode",
            ConnectorAuthModeUnknown,
        )
        self._closed(
            self.capabilities, CONNECTOR_CAPABILITIES, "capability",
            ConnectorCapabilityUnknown,
        )
        for module_id in self.required_modules:
            owner, _, name = str(module_id).partition(":")
            if not owner.strip() or not name.strip() or ":" in name or (
                owner.startswith(".") or name.startswith(".")
            ):
                raise ConnectorRequiredModuleUnqualified(
                    f"connector {self.connector_type!r} requires module "
                    f"{module_id!r}, which is not a strict owner:name id",
                    data={
                        "connector_type": self.connector_type,
                        "module_id": str(module_id),
                        "reason": "expected exactly one colon, owner:name",
                    },
                )
        signature_validator, sync_validator = _subschema_validators()
        self._check(
            signature_validator,
            self.output_signature,
            "output_signature",
            ConnectorOutputSignatureInvalid,
        )
        self._check(
            sync_validator, self.sync, "sync", ConnectorOutputSignatureInvalid,
        )
        if not isinstance(self.config_schema, Mapping):
            raise ConnectorConfigSchemaInvalid(
                f"connector {self.connector_type!r} config_schema must be a "
                "JSON Schema object",
                data={
                    "connector_type": self.connector_type,
                    "reason": f"got {type(self.config_schema).__name__}",
                },
            )
        try:
            Draft202012Validator.check_schema(dict(self.config_schema))
        except SchemaError as exc:
            raise ConnectorConfigSchemaInvalid(
                f"connector {self.connector_type!r} config_schema is not a "
                "valid JSON Schema",
                data={
                    "connector_type": self.connector_type,
                    "reason": exc.message,
                },
            ) from exc

    def _closed(
        self,
        declared: Sequence[str],
        vocabulary: frozenset,
        field_name: str,
        error_cls: type[ConnectorRegistrationError],
    ) -> None:
        """Refuse the first declared value outside a closed vocabulary."""
        for value in declared:
            if value in vocabulary:
                continue
            raise error_cls(
                f"connector {self.connector_type!r} declares unknown "
                f"{field_name} {value!r}",
                data={
                    "connector_type": self.connector_type,
                    field_name: value,
                    "allowed": sorted(vocabulary),
                },
            )

    def _check(
        self,
        validator: Draft202012Validator,
        value: Mapping[str, Any],
        field_name: str,
        error_cls: type[ConnectorRegistrationError],
    ) -> None:
        errors = sorted(
            validator.iter_errors(dict(value)),
            key=lambda err: tuple(str(part) for part in err.absolute_path),
        )
        if not errors:
            return
        first = errors[0]
        raise error_cls(
            f"connector {self.connector_type!r} has a malformed {field_name}: "
            f"{first.message}",
            data={
                "connector_type": self.connector_type,
                "field": field_name,
                "path": ".".join(str(p) for p in first.absolute_path),
                "reason": first.message,
            },
        )

    def as_asset_payload(self) -> dict[str, Any]:
        """Return this registration as one declared package-v2 asset body."""
        return {
            "schema": _ASSET_SCHEMA,
            "kind": CONNECTOR_ASSET_KIND,
            "registration": _canonical(self),
        }

    @classmethod
    def from_asset_payload(
        cls, payload: Mapping[str, Any],
    ) -> "ConnectorRegistration":
        """Rebuild a registration from a declared asset body."""
        if not isinstance(payload, Mapping):
            raise ConnectorAssetPayloadInvalid(
                "connector asset payload must be a mapping",
                data={"payload_type": type(payload).__name__},
            )
        if payload.get("schema") != _ASSET_SCHEMA:
            raise ConnectorAssetPayloadInvalid(
                f"connector asset must declare schema {_ASSET_SCHEMA!r}",
                data={"schema": str(payload.get("schema"))},
            )
        registration = payload.get("registration") or {}
        window = registration.get("sdk_range") if isinstance(
            registration, Mapping
        ) else None
        if not isinstance(registration, Mapping) or not isinstance(
            window, Mapping
        ):
            raise ConnectorAssetPayloadInvalid(
                "connector asset must carry a `registration` object with an "
                "`sdk_range` window",
                data={
                    "registration_type": type(registration).__name__,
                    "sdk_range_type": type(window).__name__,
                },
            )
        body = {key: value for key, value in registration.items()
                if key != "sdk_range"}
        try:
            # A body carrying an unknown, missing or misspelled field would
            # otherwise surface as a bare `TypeError` from the dataclass
            # constructor: uncatchable as a registration refusal, and absent
            # from the error registry an operator reads.
            return cls(sdk_range=SdkVersionRange(**dict(window)), **body)
        except TypeError as exc:
            raise ConnectorAssetPayloadInvalid(
                f"connector asset registration does not match "
                f"ConnectorRegistration: {exc}",
                data={
                    "connector_type": str(body.get("connector_type")),
                    "reason": str(exc),
                    "declared_fields": sorted(str(key) for key in body),
                },
                fix_hint=(
                    "rebuild the asset with `as_asset_payload()` from the "
                    "SDK version the package declares"
                ),
            ) from exc


def _canonical(registration: ConnectorRegistration) -> dict[str, Any]:
    """Return the registration's JSON body, key order fixed by the class."""
    body: dict[str, Any] = {}
    for name in registration.__dataclass_fields__:
        value = getattr(registration, name)
        if name == "sdk_range":
            body[name] = {
                "min_version": value.min_version,
                "max_version_exclusive": value.max_version_exclusive,
            }
        elif isinstance(value, Mapping):
            body[name] = json.loads(json.dumps(value))
        elif isinstance(value, (list, tuple)):
            body[name] = list(value)
        else:
            body[name] = value
    return body


def _running_sdk_version() -> str:
    """Return the live `okg.deployment.__version__`.

    Resolved through `sys.modules` rather than a fresh `from ... import`,
    which would build a *second* package object whenever a test has evicted
    the first — the version read would then disagree with the one the caller
    can see.
    """
    import importlib
    import sys

    package = sys.modules.get("okg.deployment") or importlib.import_module(
        "okg.deployment"
    )
    return str(package.__version__)


def registration_registry_entry(
    registration: ConnectorRegistration,
    *,
    sdk_version: Optional[str] = None,
    allow_deprecated: bool = False,
) -> dict[str, Any]:
    """Return the strict source-registry entry body for `registration`.

    Pure: it opens nothing, imports no packaged code, and reads only the
    registration and the running `okg.deployment.__version__`. The result
    is one `strict_source` body — the value under a key in a
    `source_registry.yaml` `sources:` map — and admits identically to the
    same entry written by hand.
    """
    observed = (
        _running_sdk_version() if sdk_version is None else sdk_version
    )
    if not registration.sdk_range.supports(observed):
        raise ConnectorSdkVersionUnsupported(
            f"connector {registration.connector_type!r} requires okg.deployment "
            f"in [{registration.sdk_range.min_version}, "
            f"{registration.sdk_range.max_version_exclusive}), running "
            f"{observed}",
            data={
                "connector_type": registration.connector_type,
                "sdk_version": observed,
                "min_version": registration.sdk_range.min_version,
                "max_version_exclusive": (
                    registration.sdk_range.max_version_exclusive
                ),
            },
        )
    if registration.deprecation is not None and not allow_deprecated:
        raise ConnectorRegistrationDeprecated(
            f"connector {registration.connector_type!r} is deprecated: "
            f"{registration.deprecation}",
            data={
                "connector_type": registration.connector_type,
                "version": registration.version,
                "deprecation": registration.deprecation,
            },
            fix_hint=(
                "pass allow_deprecated=True to accept the connector as "
                "declared, or migrate to its replacement"
            ),
        )

    hints: dict[str, Any] = {
        "connector_type": registration.connector_type,
        "connector_version": registration.version,
        "sdk_min_version": registration.sdk_range.min_version,
        "sdk_max_version_exclusive": (
            registration.sdk_range.max_version_exclusive
        ),
        "auth_modes": sorted(registration.auth_modes),
        "capabilities": sorted(registration.capabilities),
        "required_modules": sorted(registration.required_modules),
        "config_schema": json.loads(json.dumps(dict(
            registration.config_schema
        ))),
    }
    if registration.deprecation is not None:
        hints["deprecation"] = registration.deprecation

    entry: dict[str, Any] = {
        "module": registration.module,
        "class": registration.class_name,
        "ownership_id": registration.ownership_id,
        "source_class": registration.source_class,
    }
    for key, value in (
        ("record_identity_kind", registration.record_identity_kind),
        ("source_revision_kind", registration.source_revision_kind),
        ("deletion_semantics", registration.deletion_semantics),
        ("publication_mode", registration.publication_mode),
        ("description", registration.description),
    ):
        if value is not None:
            entry[key] = value
    if registration.record_identity_fields:
        entry["record_identity_fields"] = list(
            registration.record_identity_fields
        )
    if registration.credential_refs:
        entry["credential_refs"] = list(registration.credential_refs)
    if registration.sync:
        entry["sync"] = json.loads(json.dumps(dict(registration.sync)))
    entry["admission_policy"] = {
        "producer_id": registration.connector_type,
        "producer_kind": "source",
        "trust_label": registration.trust_label,
        "admission_mode": registration.admission_mode,
        "output_signature": json.loads(json.dumps(dict(
            registration.output_signature
        ))),
    }
    entry["configuration_hints"] = {_HINTS_KEY: hints}
    return entry
