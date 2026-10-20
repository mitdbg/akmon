"""RapidfuzzBackend — off-substrate alias resolution against
an explicit candidate list.

Per `redesign-alias-resolution` (2026-04-26). Mirrors
`okg.substrate.library.extraction.linking.local_fuzzy_resolve`
but as a class so CascadeAliasResolver can hold it.
"""
from __future__ import annotations

from typing import Iterable, Optional

from okg.substrate.alias.protocol import AliasMatch


__all__ = ["RapidfuzzBackend"]


class RapidfuzzBackend:
    """In-memory rapidfuzz over an explicit candidate list."""

    name: str = "rapidfuzz"

    def __init__(
        self,
        candidates: Iterable[str],
        *,
        canonical_for: Optional[dict[str, str]] = None,
        accept_threshold: float = 0.7,
        limit: int = 5,
    ) -> None:
        self._candidates = list(candidates)
        self._canonical_for = dict(canonical_for or {})
        self.accept_threshold = float(accept_threshold)
        self._limit = int(limit)

    def match(
        self,
        needle: str,
        *,
        entity_type: Optional[str] = None,
    ) -> list[AliasMatch]:
        if not needle or not self._candidates:
            return []
        from rapidfuzz import fuzz, process

        scored = process.extract(
            needle, self._candidates,
            scorer=fuzz.WRatio, limit=self._limit,
        )
        out: list[AliasMatch] = []
        for cand, score, _idx in scored:
            sim = float(score) / 100.0
            canonical = self._canonical_for.get(cand, cand)
            out.append(AliasMatch(
                canonical=canonical,
                similarity=sim,
                method="rapidfuzz",
            ))
        return out
