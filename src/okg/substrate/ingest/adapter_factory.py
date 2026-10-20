"""Shared source-adapter construction for ingest and poll producers."""
from __future__ import annotations

import importlib
import hashlib
import inspect
import json
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from okg.substrate.admission import graph_branch_for_admission_mode
from okg.substrate.ingest.source_params import (
    INJECTED_ADAPTER_PARAM_NAMES,
    PROTOCOL_AUTHORITY_PARAM_NAMES,
    RESERVED_ADAPTER_PARAM_NAMES,
    ConstructorParameters,
    check_source_params,
    class_target,
)
from okg.substrate.sources.profiles import PROFILE_NAMES, REGISTRY_ONLY
from okg.substrate.sources.redaction import _key_names_secret
from okg.substrate.sources.registry import (
    STRICT_ADMISSION_CONTRACT,
    SourceEntry,
    SourceRegistryAdmission,
)


RUNNABLE_SOURCE_PROFILE_NAMES = PROFILE_NAMES - {REGISTRY_ONLY}

_MISSING = object()
_NAMED_KEYWORD_PARAMETER_KINDS = frozenset({
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
    inspect.Parameter.KEYWORD_ONLY,
})


@dataclass(frozen=True)
class ConstructedSourceAdapter:
    """One adapter plus immutable registry authority used to execute it."""

    adapter: Any
    executable_profile: str | None
    registry_name: str
    ownership_id: str
    graph_branch: str
    registry_binding_fingerprint: str


@dataclass(frozen=True)
class SourceAdapterPlan:
    """Static, constructor-free binding plan for one strict source."""

    entry: SourceEntry
    adapter_class: Any
    executable_profile: str
    graph_branch: str
    init_params: Mapping[str, Any]
    registry_binding_fingerprint: str


@dataclass(frozen=True)
class AdmittedSourceBindings:
    """All runnable binding plans behind one document barrier."""

    admission: SourceRegistryAdmission
    plans: Mapping[str, SourceAdapterPlan]


def source_param_uses_deployment_dsn_placeholder(value: Any) -> bool:
    """Whether a registry DSN parameter delegates to the active deployment."""
    if not isinstance(value, str):
        return False
    return value.strip() in {"${OKG_DSN}", "$OKG_DSN"}


def _entry_uses_strict_admission(entry: SourceEntry) -> bool:
    """Treat pre-contract entries and test doubles as the legacy path."""
    return (
        getattr(entry, "admission_contract", None)
        == STRICT_ADMISSION_CONTRACT
    )


def source_adapter_graph_branch(entry: SourceEntry) -> str:
    """Return the branch used by SourceRunner for this registry entry."""
    admission_mode = (
        entry.admission_policy.admission_mode
        if entry.admission_policy is not None
        else None
    )
    return graph_branch_for_admission_mode(admission_mode)


def import_source_module(module_name: Any) -> Any:
    """Import one deployment module inside the process-wide mutation lock."""
    from okg.substrate.deployments import deployment_module_import_lock

    with deployment_module_import_lock():
        from okg.substrate.distributions.packaged_release import revalidate_active_release
        revalidate_active_release()
        return importlib.import_module(str(module_name))


def import_source_adapter_class(entry: SourceEntry) -> Any:
    """Resolve the configured adapter class through the shared import path."""
    module_name = entry.raw.get("module")
    class_name = entry.raw.get("class")
    module = import_source_module(module_name)
    return getattr(module, class_name)


def _adapter_admission_error(
    code: str,
    entry: SourceEntry,
    message: str,
    *,
    field: str | None = None,
) -> ValueError:
    """One refusal that names the registry field the operator must edit.

    `field` is the JSON pointer of the offending key inside the source entry,
    so the message locates the defect instead of only naming the source
    (issue #2126, finding F14). The caller that holds the registry path (the
    guided add-source flow, `okg ingest`) prefixes the file.
    """
    pointer = f"/sources/{entry.name}{field or ''}"
    return ValueError(
        f"{code}: source {entry.name!r} at {pointer}: {message}"
    )


def _authored_source_adapter_params(entry: SourceEntry) -> dict[str, Any]:
    if "params" not in entry.raw:
        return {}
    raw_params = entry.raw["params"]
    if not isinstance(raw_params, Mapping):
        raise _adapter_admission_error(
            "source_params_not_mapping",
            entry,
            "params must be a mapping when authored; null is not accepted",
            field="/params",
        )
    return dict(raw_params)


def _named_keyword_parameters(
    entry: SourceEntry,
    adapter_class: Any,
    *,
    authored_params: Mapping[str, Any],
) -> tuple[inspect.Signature, frozenset[str]]:
    try:
        signature = inspect.signature(adapter_class)
    except (TypeError, ValueError) as exc:
        raise _adapter_admission_error(
            "source_constructor_uninspectable",
            entry,
            "adapter signature is not inspectable, so constructor "
            "callability cannot be proven",
            field="/class",
        ) from exc
    return (
        signature,
        frozenset(
            name
            for name, parameter in signature.parameters.items()
            if parameter.kind in _NAMED_KEYWORD_PARAMETER_KINDS
        ),
    )


def source_adapter_executable_profile(
    entry: SourceEntry,
    adapter_class: Any,
) -> str:
    """Read and validate class profile authority without invoking descriptors."""
    configured_profile = entry.source_class
    if configured_profile is None:
        raise _adapter_admission_error(
            "source_class_profile_missing",
            entry,
            "a runnable adapter requires a configured source_class",
            field="/source_class",
        )
    if configured_profile == REGISTRY_ONLY:
        raise _adapter_admission_error(
            "source_registry_only_not_constructible",
            entry,
            "registry_only entries do not have runnable adapters",
            field="/source_class",
        )
    class_profile = inspect.getattr_static(adapter_class, "profile", _MISSING)
    if (
        not isinstance(class_profile, str)
        or class_profile not in RUNNABLE_SOURCE_PROFILE_NAMES
    ):
        raise _adapter_admission_error(
            "source_class_profile_invalid",
            entry,
            "adapter class profile must be one of "
            f"{sorted(RUNNABLE_SOURCE_PROFILE_NAMES)}, got {class_profile!r}",
            field="/class",
        )
    if class_profile != configured_profile:
        raise _adapter_admission_error(
            "source_class_profile_mismatch",
            entry,
            f"registry profile {configured_profile!r} does not match "
            f"class profile {class_profile!r}; set source_class to "
            f"{class_profile!r} with the matching profile tuple, or change "
            "`profile` on the adapter class",
            field="/source_class",
        )
    return class_profile


def source_adapter_init_params(
    entry: SourceEntry,
    *,
    dsn: str,
    deployment: str | None = None,
    graph_branch: str | None = None,
    adapter_class: Any | None = None,
) -> dict[str, Any]:
    """Build adapter kwargs exactly once for ingest and poll callers."""
    if not _entry_uses_strict_admission(entry):
        params = dict(entry.raw.get("params") or {})
        cls = (
            adapter_class
            if adapter_class is not None
            else import_source_adapter_class(entry)
        )
        resolved_graph_branch = (
            graph_branch
            if graph_branch is not None
            else source_adapter_graph_branch(entry)
        )
        try:
            signature = inspect.signature(cls)
        except (TypeError, ValueError):
            return params
        if "dsn" in signature.parameters and (
            "dsn" not in params
            or source_param_uses_deployment_dsn_placeholder(params["dsn"])
        ):
            params["dsn"] = dsn
        if "source_name" in signature.parameters and "source_name" not in params:
            params["source_name"] = entry.ownership_id
        if "graph_branch" in signature.parameters and "graph_branch" not in params:
            params["graph_branch"] = resolved_graph_branch
        if "deployment" in signature.parameters:
            deployment_parameter = signature.parameters["deployment"]
            if (
                deployment is None
                and deployment_parameter.default is inspect.Parameter.empty
            ):
                raise ValueError(
                    "source adapter deployment identity requires worker authority"
                )
            if "deployment" in params:
                declared_deployment = params.pop("deployment")
                if (
                    deployment is not None
                    and declared_deployment not in (None, "", deployment)
                ):
                    raise ValueError(
                        "source adapter deployment identity is worker-owned; "
                        "remove the mismatched deployment from registry params"
                    )
            if deployment is not None:
                params["deployment"] = deployment
        return params

    params = _authored_source_adapter_params(entry)
    non_string_keys = [
        type(key).__name__ for key in params if not isinstance(key, str)
    ]
    if non_string_keys:
        raise _adapter_admission_error(
            "source_param_key_invalid",
            entry,
            "top-level params keys must be constructor parameter names; "
            f"got key types {sorted(non_string_keys)!r}",
            field="/params",
        )
    cls = (
        adapter_class
        if adapter_class is not None
        else import_source_adapter_class(entry)
    )
    resolved_graph_branch = (
        graph_branch
        if graph_branch is not None
        else source_adapter_graph_branch(entry)
    )
    signature, named_parameters = _named_keyword_parameters(
        entry,
        cls,
        authored_params=params,
    )
    problems = check_source_params(
        params,
        ConstructorParameters.from_signature(
            signature, target=class_target(cls),
        ),
    )
    if problems:
        raise ValueError(problems[0].render(entry.name))
    if "dsn" in named_parameters:
        params["dsn"] = dsn
    if "source_name" in named_parameters:
        params["source_name"] = entry.ownership_id
    if "graph_branch" in named_parameters:
        params["graph_branch"] = resolved_graph_branch
    if "deployment" in named_parameters:
        deployment_parameter = signature.parameters["deployment"]
        if (
            deployment is None
            and deployment_parameter.default is inspect.Parameter.empty
        ):
            raise ValueError(
                "source adapter deployment identity requires worker authority"
            )
        if deployment is not None:
            params["deployment"] = deployment
    try:
        signature.bind(**params)
    except TypeError as exc:
        raise _adapter_admission_error(
            "source_constructor_binding_invalid",
            entry,
            f"validated constructor arguments do not bind: {exc}",
            field="/params",
        ) from exc
    return params


def _canonical_binding_value(value: Any) -> Any:
    """Return a type-preserving JSON value for a durable config digest."""
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, int):
        return {"type": "int", "value": str(value)}
    if isinstance(value, float):
        return {"type": "float", "value": value.hex()}
    if isinstance(value, str):
        return {"type": "str", "value": value}
    if isinstance(value, Mapping):
        entries = [
            {
                "key": _canonical_binding_value(key),
                "value": (
                    {"type": "secret"}
                    if isinstance(key, str) and _key_names_secret(key)
                    else _canonical_binding_value(item)
                ),
            }
            for key, item in value.items()
        ]
        entries.sort(
            key=lambda item: json.dumps(
                item["key"],
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return {"type": "mapping", "entries": entries}
    if isinstance(value, list):
        return {
            "type": "list",
            "items": [_canonical_binding_value(item) for item in value],
        }
    if isinstance(value, tuple):
        return {
            "type": "tuple",
            "items": [_canonical_binding_value(item) for item in value],
        }
    raise TypeError(
        "strict adapter parameters must use JSON scalar, mapping, or sequence "
        f"values, got {type(value).__name__}"
    )


def source_adapter_binding_fingerprint(
    entry: SourceEntry,
    *,
    adapter_class: Any | None = None,
) -> str:
    """Hash the complete registry binding without persisting its contents."""
    if not _entry_uses_strict_admission(entry):
        payload = {
            "schema": "okg.source_adapter.binding.v1",
            "source_name": entry.name,
            "ownership_id": entry.ownership_id,
            "module": entry.raw.get("module"),
            "class": entry.raw.get("class"),
            "params": entry.raw.get("params") or {},
        }
        # Completed-scope deletion policy changes the runner's authority: a
        # replay that was previously append-only may now retract records (or
        # vice versa). Keep no-policy legacy bindings byte-for-byte stable,
        # while making an authored deletion policy part of the binding whose
        # change forces an authoritative replay.
        deletion_semantics = getattr(entry, "deletion_semantics", None)
        if deletion_semantics is not None:
            payload["profile_policy"] = {
                "deletion_semantics": deletion_semantics,
            }
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    authored_params = _authored_source_adapter_params(entry)
    payload = {
        "schema": "okg.source_adapter.binding.v2",
        "registry_name": entry.name,
        "ownership_id": entry.ownership_id,
        "module": (
            str(adapter_class.__module__)
            if adapter_class is not None
            else entry.raw.get("module")
        ),
        "class": (
            str(adapter_class.__qualname__)
            if adapter_class is not None
            else entry.raw.get("class")
        ),
        "params": _canonical_binding_value(authored_params),
        "profile": {
            "source_class": entry.source_class,
            "record_identity_kind": entry.record_identity_kind,
            "record_identity_fields": list(entry.record_identity_fields),
            "source_revision_kind": entry.source_revision_kind,
            "deletion_semantics": entry.deletion_semantics,
            "publication_mode": entry.publication_mode,
        },
    }
    # Only a non-default record-authority format joins the binding. Adding the
    # key unconditionally would change the fingerprint of every existing
    # legacy_v1 source and force change-identity re-derivation on deploy.
    if entry.record_authority_format != "legacy_v1":
        payload["profile"]["record_authority_format"] = (
            entry.record_authority_format
        )
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def bind_source_adapter_identity(adapter: Any, entry: SourceEntry) -> str:
    """Attach the opaque registry binding used by runner identity."""
    fingerprint = source_adapter_binding_fingerprint(
        entry,
        adapter_class=type(adapter),
    )
    try:
        setattr(adapter, "__okg_registry_binding_fingerprint__", fingerprint)
    except (AttributeError, TypeError):
        # Frozen/slot-only adapters remain usable; callers pass the returned
        # fingerprint directly to SourceRunner.
        pass
    return fingerprint


def construct_source_adapter(
    entry: SourceEntry,
    *,
    dsn: str,
    deployment: str | None = None,
) -> ConstructedSourceAdapter:
    """Instantiate the exact registry adapter used by SourceRunner."""
    graph_branch = source_adapter_graph_branch(entry)
    cls = import_source_adapter_class(entry)
    if _entry_uses_strict_admission(entry):
        return construct_source_adapter_from_plan(
            plan_source_adapter(
                entry,
                dsn=dsn,
                deployment=deployment,
                adapter_class=cls,
            )
        )
    params = source_adapter_init_params(
        entry,
        dsn=dsn,
        deployment=deployment,
        graph_branch=graph_branch,
        adapter_class=cls,
    )
    adapter = cls(**params)
    binding_fingerprint = bind_source_adapter_identity(adapter, entry)
    return ConstructedSourceAdapter(
        adapter=adapter,
        executable_profile=entry.source_class,
        registry_name=entry.name,
        ownership_id=entry.ownership_id,
        graph_branch=graph_branch,
        registry_binding_fingerprint=binding_fingerprint,
    )


def plan_source_adapter(
    entry: SourceEntry,
    *,
    dsn: str,
    deployment: str | None = None,
    adapter_class: Any | None = None,
) -> SourceAdapterPlan:
    """Build one strict source plan without running its constructor."""
    cls = (
        adapter_class
        if adapter_class is not None
        else import_source_adapter_class(entry)
    )
    profile = source_adapter_executable_profile(entry, cls)
    graph_branch = source_adapter_graph_branch(entry)
    init_params = source_adapter_init_params(
        entry,
        dsn=dsn,
        deployment=deployment,
        graph_branch=graph_branch,
        adapter_class=cls,
    )
    fingerprint = source_adapter_binding_fingerprint(
        entry,
        adapter_class=cls,
    )
    return SourceAdapterPlan(
        entry=entry,
        adapter_class=cls,
        executable_profile=profile,
        graph_branch=graph_branch,
        init_params=MappingProxyType(dict(init_params)),
        registry_binding_fingerprint=fingerprint,
    )


def plan_source_registry_bindings(
    admission: SourceRegistryAdmission,
    *,
    dsn: str,
    deployment: str | None = None,
) -> AdmittedSourceBindings:
    """Plan every runnable strict entry before any constructor can run."""
    plans: dict[str, SourceAdapterPlan] = {}
    if admission.strict:
        for name, entry in sorted(admission.entries.items()):
            if entry.is_registry_only:
                continue
            plans[name] = plan_source_adapter(
                entry,
                dsn=dsn,
                deployment=deployment,
            )
    return AdmittedSourceBindings(
        admission=admission,
        plans=MappingProxyType(plans),
    )


def construct_source_adapter_from_plan(
    plan: SourceAdapterPlan,
) -> ConstructedSourceAdapter:
    """Construct only after the caller has crossed the document barrier."""
    adapter = plan.adapter_class(**dict(plan.init_params))
    post_profile = inspect.getattr_static(
        plan.adapter_class,
        "profile",
        _MISSING,
    )
    if post_profile != plan.executable_profile:
        raise _adapter_admission_error(
            "source_class_profile_mutated",
            plan.entry,
            "adapter construction changed class profile from "
            f"{plan.executable_profile!r} to {post_profile!r}",
            field="/class",
        )
    try:
        setattr(
            adapter,
            "__okg_registry_binding_fingerprint__",
            plan.registry_binding_fingerprint,
        )
    except (AttributeError, TypeError):
        pass
    return ConstructedSourceAdapter(
        adapter=adapter,
        executable_profile=plan.executable_profile,
        registry_name=plan.entry.name,
        ownership_id=plan.entry.ownership_id,
        graph_branch=plan.graph_branch,
        registry_binding_fingerprint=plan.registry_binding_fingerprint,
    )


__all__ = [
    "AdmittedSourceBindings",
    "ConstructedSourceAdapter",
    "INJECTED_ADAPTER_PARAM_NAMES",
    "PROTOCOL_AUTHORITY_PARAM_NAMES",
    "RESERVED_ADAPTER_PARAM_NAMES",
    "RUNNABLE_SOURCE_PROFILE_NAMES",
    "SourceAdapterPlan",
    "bind_source_adapter_identity",
    "construct_source_adapter",
    "construct_source_adapter_from_plan",
    "import_source_adapter_class",
    "import_source_module",
    "plan_source_adapter",
    "plan_source_registry_bindings",
    "source_adapter_graph_branch",
    "source_adapter_binding_fingerprint",
    "source_adapter_executable_profile",
    "source_adapter_init_params",
    "source_param_uses_deployment_dsn_placeholder",
]
