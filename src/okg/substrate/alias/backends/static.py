"""StaticAliasBackend — in-memory exact-match for tests and
small hand-curated alias tables.

Per `redesign-alias-resolution` (2026-04-26).
"""
from __future__ import annotations

from typing import Optional

from okg.substrate.alias.protocol import AliasMatch


__all__ = ["StaticAliasBackend"]


class StaticAliasBackend:
    """Exact-match against a (type, value) → canonical dict."""

    name: str = "static"
    accept_threshold: float = 1.0

    def __init__(
        self,
        mapping: dict[tuple[str, str], str],
    ) -> None:
        for key in mapping:
            if (
                not isinstance(key, tuple)
                or len(key) != 2
                or not all(isinstance(k, str) for k in key)
            ):
                raise ValueError(
                    "StaticAliasBackend mapping keys must be "
                    "(entity_type: str, value: str) tuples; "
                    f"got {key!r}"
                )
        self._m = dict(mapping)

    def match(
        self,
        needle: str,
        *,
        entity_type: Optional[str] = None,
    ) -> list[AliasMatch]:
        if entity_type is None:
            return []
        canonical = self._m.get((entity_type, needle))
        if canonical is None:
            return []
        return [AliasMatch(
            canonical=canonical, similarity=1.0, method="static",
        )]
