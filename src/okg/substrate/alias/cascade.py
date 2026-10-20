"""CascadeAliasResolver — walks AliasBackends in declaration
order; first one whose top match clears its accept_threshold
wins.

Per `redesign-alias-resolution` (2026-04-26).
"""
from __future__ import annotations

from typing import Optional

from okg.extraction import ExtractedEntity
from okg.substrate.errors import SubstrateError
from okg.substrate.alias.protocol import AliasBackend


__all__ = [
    "AliasResolutionFailed",
    "CascadeAliasResolver",
]


class AliasResolutionFailed(SubstrateError):
    code = "alias_resolution_failed"


class CascadeAliasResolver:
    """Resolve an entity by walking a list of AliasBackends.

    The cascade short-circuits at the first backend whose top
    match scores at or above the backend's accept_threshold.
    """

    def __init__(
        self,
        backends: list[AliasBackend],
        *,
        on_empty: str = "drop",
    ) -> None:
        if on_empty not in ("drop", "raise"):
            raise ValueError(
                f"on_empty must be 'drop' or 'raise'; got {on_empty!r}"
            )
        if not backends:
            raise ValueError(
                "CascadeAliasResolver requires at least one backend"
            )
        self._backends = list(backends)
        self._on_empty = on_empty

    def resolve(self, entity: ExtractedEntity) -> Optional[str]:
        per_backend_top: list[tuple[str, Optional[float]]] = []
        for backend in self._backends:
            matches = backend.match(
                entity.value, entity_type=entity.type,
            )
            if not matches:
                per_backend_top.append((backend.name, None))
                continue
            top = max(matches, key=lambda m: m.similarity)
            per_backend_top.append((backend.name, top.similarity))
            if top.similarity >= backend.accept_threshold:
                return top.canonical
        if self._on_empty == "raise":
            raise AliasResolutionFailed(
                f"no backend resolved {entity.value!r} "
                f"(type={entity.type!r}) above its threshold",
                data={
                    "needle": entity.value,
                    "entity_type": entity.type,
                    "per_backend_top_score": [
                        {"backend": name, "top_similarity": score}
                        for name, score in per_backend_top
                    ],
                },
            )
        return None
