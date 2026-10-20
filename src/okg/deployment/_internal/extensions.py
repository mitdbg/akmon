"""Pure-data extension SDK exposed to separately packaged executables."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, TypeVar


ExtensionKind = Literal[
    "connector",
    "extractor",
    "enricher",
    "verifier",
    "materializer",
    "tool",
    "resolver",
]
EXTENSION_SDK_VERSION = "1"


@dataclass(frozen=True, slots=True)
class ExtensionRequest:
    """Secret-free invocation delivered by the capability broker."""

    extension_id: str
    binding_id: str
    capabilities: tuple[str, ...]
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _identifier(self.extension_id, "extension_id")
        _identifier(self.binding_id, "binding_id")
        capabilities = tuple(sorted(_identifier(item, "capability") for item in self.capabilities))
        if len(capabilities) != len(set(capabilities)):
            raise ValueError("extension request capabilities must be unique")
        object.__setattr__(self, "capabilities", capabilities)
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))


@dataclass(frozen=True, slots=True)
class ExtensionNode:
    node_id: str
    subtype: str
    attrs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _identifier(self.node_id, "node_id")
        _identifier(self.subtype, "subtype")
        object.__setattr__(self, "attrs", MappingProxyType(dict(self.attrs)))


@dataclass(frozen=True, slots=True)
class ExtensionEdge:
    src: str
    dst: str
    edge_type: str
    attrs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _identifier(self.src, "src")
        _identifier(self.dst, "dst")
        _identifier(self.edge_type, "edge_type")
        object.__setattr__(self, "attrs", MappingProxyType(dict(self.attrs)))


@dataclass(frozen=True, slots=True)
class ExtensionBatch:
    """Declarative output; only the substrate publisher may persist it."""

    nodes: tuple[ExtensionNode, ...] = ()
    edges: tuple[ExtensionEdge, ...] = ()
    observations: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "nodes", tuple(self.nodes))
        object.__setattr__(self, "edges", tuple(self.edges))
        object.__setattr__(self, "observations", MappingProxyType(dict(self.observations)))


_F = TypeVar("_F", bound=Callable[[ExtensionRequest], ExtensionBatch])


def extension_entrypoint(
    extension_id: str,
    *,
    kind: ExtensionKind,
    capabilities: tuple[str, ...] = (),
) -> Callable[[_F], _F]:
    """Declare public entry metadata without registering process-global code."""

    checked_id = _identifier(extension_id, "extension_id")
    checked_caps = tuple(sorted(_identifier(item, "capability") for item in capabilities))
    if len(checked_caps) != len(set(checked_caps)):
        raise ValueError("extension entrypoint capabilities must be unique")

    def decorate(function: _F) -> _F:
        setattr(function, "__okg_extension_id__", checked_id)
        setattr(function, "__okg_extension_kind__", kind)
        setattr(function, "__okg_extension_capabilities__", checked_caps)
        return function

    return decorate


def _identifier(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(char.isspace() for char in value):
        raise ValueError(f"{field_name} must be a non-empty string without whitespace")
    return value


__all__ = [
    "ExtensionBatch",
    "ExtensionEdge",
    "ExtensionKind",
    "ExtensionNode",
    "ExtensionRequest",
    "EXTENSION_SDK_VERSION",
    "extension_entrypoint",
]
