"""Concrete AliasBackend implementations.

Per `redesign-alias-resolution` (2026-04-26).
"""
from okg.substrate.alias.backends.embedding import (
    EmbeddingAliasBackend,
    EmbeddingBackendNotInstalled,
)
from okg.substrate.alias.backends.rapidfuzz_backend import (
    RapidfuzzBackend,
)
from okg.substrate.alias.backends.sql_cascade import (
    SqlCascadeBackend,
)
from okg.substrate.alias.backends.static import (
    StaticAliasBackend,
)


__all__ = [
    "EmbeddingAliasBackend",
    "EmbeddingBackendNotInstalled",
    "RapidfuzzBackend",
    "SqlCascadeBackend",
    "StaticAliasBackend",
]
