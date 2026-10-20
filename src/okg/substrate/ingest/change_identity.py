"""Versioned runner-side identity for sound source change decisions."""
from __future__ import annotations

import copy
import dataclasses
import dis
import hashlib
import inspect
import json
import re
import types
from dataclasses import dataclass, field
from typing import Any, Mapping

from okg.substrate.library.sources.change_probe import CURSOR_TOKEN_KEY

EFFECTIVE_TOKEN_SCHEMA = "okg.source_runner.effective_change_token.v1"
PIPELINE_SNAPSHOT_SCHEMA = "okg.source_runner.pipeline_identity.v1"
BOOTSTRAP_AUTHORITY_SCHEMA = (
    "okg.source_runner.bootstrap_identity_authority.v2"
)
BOOTSTRAP_AUTHORITY_CURSOR_KEY = "_okg_bootstrap_identity_authority"

_TRUSTED_RUNTIME_MODULES = frozenset({"hashlib", "hmac", "json"})


@dataclass(frozen=True)
class _TrustedRuntimeCallable:
    """A captured callable from an explicitly immutable runtime module."""

    identity: str
    _apply: Any = field(repr=False, compare=False)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._apply(*args, **kwargs)


class _FrozenModuleNamespace:
    """Read-only snapshot of statically referenced module attributes."""

    __slots__ = ("identity", "members", "name")

    def __init__(
        self,
        *,
        name: str,
        identity: str,
        members: Mapping[str, Any],
    ) -> None:
        self.name = name
        self.identity = identity
        self.members = types.MappingProxyType(dict(members))

    def __getattr__(self, name: str) -> Any:
        try:
            return self.members[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


def _secret_name(name: str) -> bool:
    lowered = name.lower()
    return any(
        marker in lowered
        for marker in ("secret", "password", "credential", "token", "key")
    )


def _stable_value(
    value: Any,
    *,
    field_name: str = "",
    depth: int = 0,
    seen: frozenset[int] = frozenset(),
) -> Any:
    """Return a JSON-safe identity without exposing secret material."""
    if depth > 32:
        raise ValueError(
            "pipeline identity exceeds the supported dependency depth"
        )
    if value is None or isinstance(value, (bool, int, float, str)):
        if field_name and _secret_name(field_name) and value is not None:
            return {
                "secret_sha256": hashlib.sha256(
                    str(value).encode("utf-8")
                ).hexdigest()
            }
        return value
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest()}
    if isinstance(value, _TrustedRuntimeCallable):
        return {"trusted_runtime_callable": value.identity}
    if isinstance(value, _FrozenModuleNamespace):
        return {
            "frozen_module": value.name,
            "identity": value.identity,
            "members": _stable_value(
                value.members,
                depth=depth + 1,
                seen=seen,
            ),
        }
    if isinstance(value, types.ModuleType):
        try:
            module_source = inspect.getsource(value).encode("utf-8")
        except (OSError, TypeError):
            module_source = None
        return {
            "module": str(getattr(value, "__name__", "")),
            "source_sha256": (
                hashlib.sha256(module_source).hexdigest()
                if module_source is not None
                else None
            ),
        }
    if isinstance(value, type):
        try:
            class_source = inspect.getsource(value).encode("utf-8")
        except (OSError, TypeError):
            class_source = None
        return {
            "class": f"{value.__module__}.{value.__qualname__}",
            "source_sha256": (
                hashlib.sha256(class_source).hexdigest()
                if class_source is not None
                else None
            ),
        }
    object_id = id(value)
    if object_id in seen:
        return {
            "recursive_ref": (
                f"{type(value).__module__}.{type(value).__qualname__}"
            )
        }
    next_seen = seen | {object_id}
    if isinstance(value, Mapping):
        return {
            str(key): _stable_value(
                item,
                field_name=str(key),
                depth=depth + 1,
                seen=next_seen,
            )
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [
            _stable_value(item, depth=depth + 1, seen=next_seen)
            for item in value
        ]
    if isinstance(value, (set, frozenset)):
        normalized = [
            _stable_value(item, depth=depth + 1, seen=next_seen)
            for item in value
        ]
        return sorted(
            normalized,
            key=lambda item: json.dumps(item, sort_keys=True, default=str),
        )
    if inspect.ismethod(value):
        return {
            "method": _callable_identity(
                value.__func__,
                depth=depth + 1,
                seen=next_seen,
            ),
            "self": _stable_value(
                value.__self__,
                depth=depth + 1,
                seen=next_seen,
            ),
        }
    if inspect.isfunction(value):
        return _callable_identity(
            value,
            depth=depth + 1,
            seen=next_seen,
        )
    if dataclasses.is_dataclass(value):
        return {
            "type": f"{type(value).__module__}.{type(value).__qualname__}",
            "fields": {
                field.name: _stable_value(
                    getattr(value, field.name),
                    field_name=field.name,
                    depth=depth + 1,
                    seen=next_seen,
                )
                for field in dataclasses.fields(value)
            },
        }
    state = getattr(value, "__dict__", None)
    return {
        "type": f"{type(value).__module__}.{type(value).__qualname__}",
        "state": (
            {
                str(key): _stable_value(
                    item,
                    field_name=str(key),
                    depth=depth + 1,
                    seen=next_seen,
                )
                for key, item in sorted(state.items())
                if not str(key).startswith("_cache")
            }
            if isinstance(state, dict)
            else None
        ),
    }


def _referenced_global_accesses(
    code: types.CodeType,
) -> dict[str, tuple[frozenset[str], bool]]:
    """Return statically loaded globals, their attributes, and direct uses."""
    accesses: dict[str, tuple[set[str], bool]] = {}
    pending = [code]
    seen_codes: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen_codes:
            continue
        seen_codes.add(id(current))
        instructions = list(dis.get_instructions(current))
        for index, instruction in enumerate(instructions):
            if (
                instruction.opname in {"LOAD_GLOBAL", "LOAD_NAME"}
                and isinstance(instruction.argval, str)
            ):
                attrs, direct = accesses.setdefault(
                    instruction.argval,
                    (set(), False),
                )
                following = (
                    instructions[index + 1]
                    if index + 1 < len(instructions)
                    else None
                )
                if (
                    following is not None
                    and following.opname in {"LOAD_ATTR", "LOAD_METHOD"}
                    and isinstance(following.argval, str)
                ):
                    attrs.add(following.argval)
                else:
                    accesses[instruction.argval] = (attrs, True)
        pending.extend(
            constant
            for constant in current.co_consts
            if isinstance(constant, types.CodeType)
        )
    return {
        name: (frozenset(attrs), direct)
        for name, (attrs, direct) in sorted(accesses.items())
    }


def _referenced_global_names(code: types.CodeType) -> tuple[str, ...]:
    """Return globals loaded by ``code`` and any nested code it creates."""
    return tuple(_referenced_global_accesses(code))


def _runtime_callable_identity(value: Any) -> str:
    code = getattr(value, "__code__", None)
    try:
        source = inspect.getsource(value)
    except (OSError, TypeError):
        source = None
    return identity_fingerprint({
        "callable": (
            f"{getattr(value, '__module__', '')}."
            f"{getattr(value, '__qualname__', type(value).__qualname__)}"
        ),
        "source_sha256": (
            hashlib.sha256(source.encode("utf-8")).hexdigest()
            if source is not None
            else None
        ),
        "bytecode_sha256": (
            hashlib.sha256(code.co_code).hexdigest()
            if code is not None
            else None
        ),
    })


def _callable_identity(
    value: Any,
    *,
    depth: int,
    seen: frozenset[int],
) -> dict[str, Any]:
    code = getattr(value, "__code__", None)
    closure = getattr(value, "__closure__", None) or ()
    try:
        source = inspect.getsource(value)
    except (OSError, TypeError):
        source = None
    return {
        "callable": (
            f"{getattr(value, '__module__', '')}."
            f"{getattr(value, '__qualname__', type(value).__qualname__)}"
        ),
        "source_sha256": (
            hashlib.sha256(source.encode("utf-8")).hexdigest()
            if source is not None
            else None
        ),
        "bytecode_sha256": (
            hashlib.sha256(code.co_code).hexdigest()
            if code is not None
            else None
        ),
        "defaults": _stable_value(
            getattr(value, "__defaults__", None),
            depth=depth + 1,
            seen=seen,
        ),
        "closure": [
            _stable_value(
                cell.cell_contents,
                depth=depth + 1,
                seen=seen,
            )
            for cell in closure
        ],
        "globals": (
            {
                name: _stable_value(
                    value.__globals__[name],
                    field_name=name,
                    depth=depth + 1,
                    seen=seen,
                )
                for name in _referenced_global_names(code)
                if name in value.__globals__
            }
            if code is not None and hasattr(value, "__globals__")
            else {}
        ),
    }


def identity_fingerprint(value: Any) -> str:
    encoded = json.dumps(
        _stable_value(value),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _closure_cell(value: Any) -> Any:
    return (lambda: value).__closure__[0]


def _freeze_function_dependency(
    value: Any,
    *,
    function_memo: dict[int, Any],
    object_memo: dict[int, Any],
    module_attributes: frozenset[str] | None = None,
) -> Any:
    """Freeze supported function state or refuse a live dynamic dependency."""
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value
    if inspect.isfunction(value):
        return _freeze_function(
            value,
            function_memo=function_memo,
            object_memo=object_memo,
        )
    if inspect.isbuiltin(value):
        return value
    if isinstance(value, types.ModuleType):
        if not module_attributes:
            raise ValueError(
                f"module {value.__name__!r} is used dynamically"
            )
        members: dict[str, Any] = {}
        for name in sorted(module_attributes):
            if not hasattr(value, name):
                raise ValueError(
                    f"module {value.__name__!r} has no attribute {name!r}"
                )
            member = getattr(value, name)
            if (
                value.__name__ in _TRUSTED_RUNTIME_MODULES
                and callable(member)
            ):
                members[name] = _TrustedRuntimeCallable(
                    identity=_runtime_callable_identity(member),
                    _apply=member,
                )
            else:
                members[name] = _freeze_function_dependency(
                    member,
                    function_memo=function_memo,
                    object_memo=object_memo,
                )
        return _FrozenModuleNamespace(
            name=value.__name__,
            identity=identity_fingerprint(value),
            members=members,
        )
    if isinstance(value, type):
        if (
            value.__module__ == "builtins"
            or (
                dataclasses.is_dataclass(value)
                and bool(value.__dataclass_params__.frozen)
            )
        ):
            return value
        raise ValueError(
            "mutable class dependency "
            f"{value.__module__}.{value.__qualname__}"
        )
    if isinstance(value, re.Pattern):
        return value

    object_id = id(value)
    if object_id in object_memo:
        return object_memo[object_id]
    if type(value) is dict:
        frozen_dict: dict[Any, Any] = {}
        object_memo[object_id] = frozen_dict
        for key, item in value.items():
            frozen_key = _freeze_function_dependency(
                key,
                function_memo=function_memo,
                object_memo=object_memo,
            )
            frozen_dict[frozen_key] = _freeze_function_dependency(
                item,
                function_memo=function_memo,
                object_memo=object_memo,
            )
        return frozen_dict
    if type(value) is list:
        frozen_list: list[Any] = []
        object_memo[object_id] = frozen_list
        frozen_list.extend(
            _freeze_function_dependency(
                item,
                function_memo=function_memo,
                object_memo=object_memo,
            )
            for item in value
        )
        return frozen_list
    if type(value) is set:
        frozen_set: set[Any] = set()
        object_memo[object_id] = frozen_set
        frozen_set.update(
            _freeze_function_dependency(
                item,
                function_memo=function_memo,
                object_memo=object_memo,
            )
            for item in value
        )
        return frozen_set
    if type(value) is tuple:
        frozen_tuple = tuple(
            _freeze_function_dependency(
                item,
                function_memo=function_memo,
                object_memo=object_memo,
            )
            for item in value
        )
        object_memo[object_id] = frozen_tuple
        return frozen_tuple
    if type(value) is frozenset:
        frozen_frozenset = frozenset(
            _freeze_function_dependency(
                item,
                function_memo=function_memo,
                object_memo=object_memo,
            )
            for item in value
        )
        object_memo[object_id] = frozen_frozenset
        return frozen_frozenset
    raise ValueError(
        "unsupported live function dependency "
        f"{type(value).__module__}.{type(value).__qualname__}"
    )


def _freeze_function(
    value: Any,
    *,
    function_memo: dict[int, Any] | None = None,
    object_memo: dict[int, Any] | None = None,
) -> Any:
    """Clone a function with recursively detached globals and closures."""
    if function_memo is None:
        function_memo = {}
    if object_memo is None:
        object_memo = {}
    existing = function_memo.get(id(value))
    if existing is not None:
        return existing

    original_globals = value.__globals__
    raw_builtins = original_globals.get("__builtins__", {})
    if isinstance(raw_builtins, types.ModuleType):
        raw_builtins = vars(raw_builtins)
    frozen_globals = {
        "__builtins__": dict(raw_builtins),
        "__name__": original_globals.get("__name__", value.__module__),
        "__package__": original_globals.get("__package__"),
    }
    closure = getattr(value, "__closure__", None)
    frozen_closure = (
        tuple(_closure_cell(None) for _cell in closure)
        if closure else None
    )
    frozen = types.FunctionType(
        value.__code__,
        frozen_globals,
        name=value.__name__,
        argdefs=None,
        closure=frozen_closure,
    )
    function_memo[id(value)] = frozen

    if closure and frozen_closure:
        for original_cell, frozen_cell in zip(
            closure,
            frozen_closure,
            strict=True,
        ):
            frozen_cell.cell_contents = _freeze_function_dependency(
                original_cell.cell_contents,
                function_memo=function_memo,
                object_memo=object_memo,
            )
    frozen.__defaults__ = _freeze_function_dependency(
        value.__defaults__,
        function_memo=function_memo,
        object_memo=object_memo,
    )
    frozen.__kwdefaults__ = _freeze_function_dependency(
        value.__kwdefaults__,
        function_memo=function_memo,
        object_memo=object_memo,
    )
    frozen.__annotations__ = _freeze_function_dependency(
        dict(getattr(value, "__annotations__", {})),
        function_memo=function_memo,
        object_memo=object_memo,
    )
    frozen.__dict__.update(_freeze_function_dependency(
        dict(getattr(value, "__dict__", {})),
        function_memo=function_memo,
        object_memo=object_memo,
    ))
    global_accesses = _referenced_global_accesses(value.__code__)
    for name, (module_attributes, directly_used) in global_accesses.items():
        if name not in original_globals:
            continue
        dependency = original_globals[name]
        try:
            frozen_globals[name] = _freeze_function_dependency(
                dependency,
                function_memo=function_memo,
                object_memo=object_memo,
                module_attributes=(
                    None
                    if directly_used or not isinstance(
                        dependency,
                        types.ModuleType,
                    )
                    else module_attributes
                ),
            )
        except ValueError as exc:
            raise ValueError(
                f"function {value.__qualname__} cannot freeze global {name!r}"
            ) from exc
    frozen.__module__ = value.__module__
    frozen.__qualname__ = value.__qualname__
    frozen.__doc__ = value.__doc__
    return frozen


def _freeze_callable(value: Any) -> tuple[Any, str]:
    """Capture callable behavior independently of later object/class mutation."""
    try:
        if inspect.isfunction(value):
            frozen = _freeze_function(value)
            return frozen, identity_fingerprint(frozen)
        if inspect.ismethod(value):
            frozen_self = copy.deepcopy(value.__self__)
            frozen_function = _freeze_function(value.__func__)
            frozen = types.MethodType(frozen_function, frozen_self)
            return frozen, identity_fingerprint(frozen)
        frozen_self = copy.deepcopy(value)
        method = getattr(frozen_self, "__call__")
        if not inspect.ismethod(method):
            raise TypeError("callable object has no bound Python __call__")
        frozen_function = _freeze_function(method.__func__)
        frozen = types.MethodType(frozen_function, frozen_self)
        return frozen, identity_fingerprint(frozen)
    except Exception as exc:  # noqa: BLE001 - identity must fail closed
        raise ValueError(
            f"pipeline callable {type(value).__qualname__} cannot be frozen"
        ) from exc


@dataclass(frozen=True)
class _FrozenCanonicalizer:
    id_family: str
    identity: str
    _apply: Any = field(repr=False, compare=False)

    def canonicalize(self, raw_id: str) -> str:
        return self._apply(raw_id)


def _freeze_canonicalizer(value: Any) -> Any:
    method = getattr(value, "canonicalize", None)
    if not callable(method):
        return copy.deepcopy(value)
    try:
        frozen_self = copy.deepcopy(value)
        frozen_function = _freeze_function(method.__func__)
        bound = types.MethodType(frozen_function, frozen_self)
    except Exception as exc:  # noqa: BLE001 - identity must fail closed
        raise ValueError(
            f"canonicalizer {type(value).__qualname__} cannot be frozen"
        ) from exc
    return _FrozenCanonicalizer(
        id_family=str(getattr(frozen_self, "id_family", "")),
        identity=identity_fingerprint({
            "state": frozen_self,
            "implementation": frozen_function,
        }),
        _apply=bound,
    )


@dataclass(frozen=True)
class _FrozenPseudonymService:
    identity: str
    _apply: Any = field(repr=False, compare=False)

    def pseudonym(self, **kwargs: Any) -> str:
        return self._apply(**kwargs)


@dataclass(frozen=True)
class _FrozenDetector:
    identity: str
    _apply: Any = field(repr=False, compare=False)

    def detect(self, text: str) -> Any:
        return self._apply(text)


@dataclass(frozen=True)
class FrozenPiiPosture:
    """Run-local PII collaborators detached from mutable runner configuration."""

    store_raw: bool
    classifier: Any
    pseudonym_service: Any
    detector: Any
    identity: str

    @property
    def active(self) -> bool:
        return not self.store_raw


def _freeze_pii_posture(value: Any) -> Any:
    if value is None or isinstance(value, Mapping):
        return copy.deepcopy(value)
    if not hasattr(value, "store_raw"):
        return copy.deepcopy(value)
    store_raw = bool(value.store_raw)
    if store_raw:
        return FrozenPiiPosture(
            store_raw=True,
            classifier=None,
            pseudonym_service=None,
            detector=None,
            identity=identity_fingerprint(value),
        )
    classifier, classifier_identity = _freeze_callable(value.classifier)
    pseudonym_service = None
    pseudonym_identity = None
    if value.pseudonym_service is not None:
        frozen_service = copy.deepcopy(value.pseudonym_service)
        frozen_pseudonym = _freeze_function(
            value.pseudonym_service.pseudonym.__func__
        )
        pseudonym_service = _FrozenPseudonymService(
            identity=identity_fingerprint({
                "state": frozen_service,
                "implementation": frozen_pseudonym,
            }),
            _apply=types.MethodType(
                frozen_pseudonym,
                frozen_service,
            ),
        )
        pseudonym_identity = pseudonym_service.identity
    detector = None
    detector_identity = None
    if value.detector is not None:
        frozen_detector = copy.deepcopy(value.detector)
        frozen_detect = _freeze_function(value.detector.detect.__func__)
        detector = _FrozenDetector(
            identity=identity_fingerprint({
                "state": frozen_detector,
                "implementation": frozen_detect,
            }),
            _apply=types.MethodType(
                frozen_detect,
                frozen_detector,
            ),
        )
        detector_identity = detector.identity
    identity = identity_fingerprint({
        "store_raw": store_raw,
        "classifier": classifier_identity,
        "pseudonym_service": pseudonym_identity,
        "detector": detector_identity,
    })
    return FrozenPiiPosture(
        store_raw=store_raw,
        classifier=classifier,
        pseudonym_service=pseudonym_service,
        detector=detector,
        identity=identity,
    )


def module_code_identity(module: Any) -> dict[str, Any]:
    """Hash a complete transform module, including transitive helper bodies."""
    try:
        source = inspect.getsource(module).encode("utf-8")
    except (OSError, TypeError) as exc:
        raise ValueError(
            f"pipeline module {getattr(module, '__name__', module)!r} "
            "has no inspectable source"
        ) from exc
    return {
        "module": str(getattr(module, "__name__", "")),
        "source_sha256": hashlib.sha256(source).hexdigest(),
    }


@dataclass(frozen=True)
class PipelineIdentitySnapshot:
    """Exact immutable transformation dependencies used by one source run."""

    canonicalizers: tuple[Any, ...]
    volatile_fields: tuple[tuple[str, tuple[str, ...]], ...]
    nomos_policy_fingerprint: str | None
    pii_posture: Any
    registry_binding_fingerprint: str
    transform_fingerprint: str
    fingerprint: str

    def volatile_fields_by_subtype(self) -> dict[str, frozenset[str]]:
        """The frozen volatile fields, per node subtype."""
        return {
            subtype: frozenset(fields) for subtype, fields in self.volatile_fields
        }

    @staticmethod
    def _identity_payload(
        *,
        canonicalizers: tuple[Any, ...],
        volatile_fields: tuple[tuple[str, tuple[str, ...]], ...],
        nomos_policy_fingerprint: str | None,
        pii_posture: Any,
        registry_binding_fingerprint: str,
        transform_fingerprint: str,
    ) -> dict[str, Any]:
        return {
            "schema": PIPELINE_SNAPSHOT_SCHEMA,
            "canonicalizers": canonicalizers,
            "volatile_fields": volatile_fields,
            "nomos_policy_fingerprint": nomos_policy_fingerprint,
            "pii_posture": pii_posture,
            "registry_binding_fingerprint": registry_binding_fingerprint,
            "transform_fingerprint": transform_fingerprint,
        }

    @classmethod
    def build(
        cls,
        *,
        canonicalizers: tuple[Any, ...],
        volatile_fields: tuple[tuple[str, tuple[str, ...]], ...],
        nomos_policy_fingerprint: str | None,
        pii_posture: Any,
        registry_binding_fingerprint: str,
        transform_identity: Any,
    ) -> "PipelineIdentitySnapshot":
        frozen_canonicalizers = tuple(
            _freeze_canonicalizer(value)
            for value in canonicalizers
        )
        frozen_pii_posture = _freeze_pii_posture(pii_posture)
        transform_fingerprint = identity_fingerprint(transform_identity)
        fingerprint = identity_fingerprint(cls._identity_payload(
            canonicalizers=frozen_canonicalizers,
            volatile_fields=volatile_fields,
            nomos_policy_fingerprint=nomos_policy_fingerprint,
            pii_posture=frozen_pii_posture,
            registry_binding_fingerprint=registry_binding_fingerprint,
            transform_fingerprint=transform_fingerprint,
        ))
        return cls(
            canonicalizers=frozen_canonicalizers,
            volatile_fields=volatile_fields,
            nomos_policy_fingerprint=nomos_policy_fingerprint,
            pii_posture=frozen_pii_posture,
            registry_binding_fingerprint=registry_binding_fingerprint,
            transform_fingerprint=transform_fingerprint,
            fingerprint=fingerprint,
        )

    def recomputed_fingerprint(self) -> str:
        """Recompute identity from the exact frozen collaborators in use."""
        return identity_fingerprint(self._identity_payload(
            canonicalizers=self.canonicalizers,
            volatile_fields=self.volatile_fields,
            nomos_policy_fingerprint=self.nomos_policy_fingerprint,
            pii_posture=self.pii_posture,
            registry_binding_fingerprint=self.registry_binding_fingerprint,
            transform_fingerprint=self.transform_fingerprint,
        ))

    def is_intact(self) -> bool:
        """Return whether the captured transform snapshot retained its identity."""
        return self.recomputed_fingerprint() == self.fingerprint


def build_effective_token(
    raw_token: Mapping[str, Any],
    snapshot: PipelineIdentitySnapshot,
) -> dict[str, Any]:
    """Wrap exact raw applied identity with the applied pipeline identity."""
    return {
        "schema": EFFECTIVE_TOKEN_SCHEMA,
        "raw_token": dict(raw_token),
        "pipeline_fingerprint": snapshot.fingerprint,
        "registry_binding_fingerprint": (
            snapshot.registry_binding_fingerprint
        ),
    }


def bootstrap_resume_fingerprint(
    cursor: Mapping[str, Any] | None,
) -> str:
    """Fingerprint the complete source frontier, excluding its own envelope."""
    normalized = dict(cursor or {})
    normalized.pop(BOOTSTRAP_AUTHORITY_CURSOR_KEY, None)
    return identity_fingerprint({"source_cursor": normalized})


def build_bootstrap_authority(
    *,
    raw_token: Mapping[str, Any],
    bindings: Mapping[str, Any],
    cursor_semantics: str,
    resume_cursor: Mapping[str, Any],
    input_frontier_fingerprint: str,
    transition_identity: Mapping[str, str],
    generation: int,
    nonce: str,
) -> dict[str, Any]:
    """Build a self-checking, cursor-bound non-terminal authority envelope."""
    if isinstance(generation, bool) or generation < 1:
        raise ValueError("bootstrap generation must be a positive integer")
    if not nonce:
        raise ValueError("bootstrap nonce must be non-empty")
    if not input_frontier_fingerprint:
        raise ValueError("bootstrap input frontier fingerprint must be non-empty")
    required_transition_fields = {"admission_id", "run_id", "idempotency_key"}
    if set(transition_identity) != required_transition_fields or any(
        not isinstance(transition_identity.get(key), str)
        or not transition_identity[key]
        for key in required_transition_fields
    ):
        raise ValueError(
            "bootstrap transition identity must contain non-empty "
            "admission_id, run_id, and idempotency_key"
        )
    payload = {
        "schema": BOOTSTRAP_AUTHORITY_SCHEMA,
        "generation": generation,
        "nonce": nonce,
        "bindings": dict(bindings),
        "raw_token": dict(raw_token),
        "cursor_semantics": cursor_semantics,
        "input_frontier_fingerprint": input_frontier_fingerprint,
        "transition_identity": dict(transition_identity),
        "resume_fingerprint": bootstrap_resume_fingerprint(resume_cursor),
    }
    return {
        **payload,
        "authority_fingerprint": identity_fingerprint(payload),
    }


def bootstrap_authority_from_cursor(
    cursor: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    """Return a structurally and frontier-valid bootstrap envelope.

    Absence is ordinary. Presence in any malformed, tampered, or cursor-replayed
    shape raises ``ValueError`` so callers cannot silently downgrade it to a
    first-run default.
    """
    if not isinstance(cursor, Mapping):
        return None
    if BOOTSTRAP_AUTHORITY_CURSOR_KEY not in cursor:
        return None
    value = cursor.get(BOOTSTRAP_AUTHORITY_CURSOR_KEY)
    if not isinstance(value, Mapping):
        raise ValueError("bootstrap authority must be a mapping")
    generation = value.get("generation")
    if isinstance(generation, bool) or not isinstance(generation, int):
        raise ValueError("bootstrap authority generation must be an integer")
    if generation < 1:
        raise ValueError("bootstrap authority generation must be positive")
    if value.get("schema") != BOOTSTRAP_AUTHORITY_SCHEMA:
        raise ValueError("bootstrap authority schema is unsupported")
    for field_name in (
        "nonce",
        "cursor_semantics",
        "input_frontier_fingerprint",
        "resume_fingerprint",
        "authority_fingerprint",
    ):
        if not isinstance(value.get(field_name), str) or not value[field_name]:
            raise ValueError(
                f"bootstrap authority {field_name} must be non-empty"
            )
    for field_name in ("bindings", "raw_token", "transition_identity"):
        if not isinstance(value.get(field_name), Mapping) or not value[field_name]:
            raise ValueError(
                f"bootstrap authority {field_name} must be a non-empty mapping"
            )
    transition_identity = value["transition_identity"]
    required_transition_fields = {"admission_id", "run_id", "idempotency_key"}
    if set(transition_identity) != required_transition_fields or any(
        not isinstance(transition_identity.get(key), str)
        or not transition_identity[key]
        for key in required_transition_fields
    ):
        raise ValueError("bootstrap authority transition identity is invalid")
    payload = {
        key: item
        for key, item in value.items()
        if key != "authority_fingerprint"
    }
    if value["authority_fingerprint"] != identity_fingerprint(payload):
        raise ValueError("bootstrap authority fingerprint does not match")
    if value["resume_fingerprint"] != bootstrap_resume_fingerprint(cursor):
        raise ValueError("bootstrap authority resume frontier does not match")
    return value


def effective_token_from_cursor(
    cursor: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if not isinstance(cursor, Mapping):
        return None
    value = cursor.get(CURSOR_TOKEN_KEY)
    if (
        isinstance(value, Mapping)
        and value.get("schema") == EFFECTIVE_TOKEN_SCHEMA
        and isinstance(value.get("raw_token"), Mapping)
    ):
        return value
    return None


def raw_token_cursor(
    cursor: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Return a cursor view whose probe token is the retained raw token."""
    if not isinstance(cursor, Mapping):
        return None
    result = dict(cursor)
    effective = effective_token_from_cursor(cursor)
    if effective is not None:
        result[CURSOR_TOKEN_KEY] = dict(effective["raw_token"])
    return result


__all__ = [
    "BOOTSTRAP_AUTHORITY_CURSOR_KEY",
    "BOOTSTRAP_AUTHORITY_SCHEMA",
    "EFFECTIVE_TOKEN_SCHEMA",
    "PIPELINE_SNAPSHOT_SCHEMA",
    "PipelineIdentitySnapshot",
    "bootstrap_authority_from_cursor",
    "bootstrap_resume_fingerprint",
    "build_bootstrap_authority",
    "build_effective_token",
    "effective_token_from_cursor",
    "identity_fingerprint",
    "module_code_identity",
    "raw_token_cursor",
]
