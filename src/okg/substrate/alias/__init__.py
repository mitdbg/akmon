"""Alias-resolution package — pluggable cascade backends.

Per `redesign-alias-resolution` (2026-04-26).
"""
from okg.substrate.alias.backends import (
    EmbeddingAliasBackend,
    EmbeddingBackendNotInstalled,
    RapidfuzzBackend,
    SqlCascadeBackend,
    StaticAliasBackend,
)
from okg.substrate.alias.cascade import (
    AliasResolutionFailed,
    CascadeAliasResolver,
)
from okg.substrate.alias.loader import (
    AliasBackendLoadError,
    load_alias_resolver,
)
from okg.substrate.alias.protocol import (
    AliasBackend,
    AliasMatch,
)


__all__ = [
    "AliasBackend",
    "AliasBackendLoadError",
    "AliasMatch",
    "AliasResolutionFailed",
    "CascadeAliasResolver",
    "EmbeddingAliasBackend",
    "EmbeddingBackendNotInstalled",
    "RapidfuzzBackend",
    "SqlCascadeBackend",
    "StaticAliasBackend",
    "load_alias_resolver",
]
