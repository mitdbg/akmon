"""AliasBackend Protocol + AliasMatch dataclass.

Per `redesign-alias-resolution` (2026-04-26).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable


__all__ = ["AliasBackend", "AliasMatch"]


@dataclass(frozen=True)
class AliasMatch:
    canonical: str
    similarity: float
    method: str


@runtime_checkable
class AliasBackend(Protocol):
    """One tier of the alias-resolution cascade.

    The cascade walks backends in declaration order; the first
    backend whose top match's similarity reaches its
    accept_threshold wins. Implementations may consult SQL,
    rapidfuzz, an embedding store, or an in-memory mapping.
    """

    name: str
    accept_threshold: float

    def match(
        self,
        needle: str,
        *,
        entity_type: Optional[str] = None,
    ) -> list[AliasMatch]:
        raise NotImplementedError
