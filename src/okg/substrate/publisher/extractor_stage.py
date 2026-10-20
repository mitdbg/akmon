"""Chronos-native extractor stage configuration.

The Publisher no longer owns live-table projection, but it still uses the
projection extractor/router configuration to emit derived edge facts before
checkpointing a Chronos branch. This module keeps that current stage boundary
separate from the retired Phase-A projector orchestration.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

import psycopg

from okg.substrate.projection import (
    AliasResolver,
    EntityExtractor,
    Projector,
)


class ChronosExtractorStage:
    """Configuration and skip policy for Chronos extractor work."""

    def __init__(self, projector: Projector) -> None:
        self.projector = projector

    @classmethod
    def from_config(
        cls,
        dsn: str,
        *,
        deployment_name: str,
        extractor: EntityExtractor | None = None,
        routes: list | None = None,
        extractor_source_subtypes: list[str] | None = None,
        resolver_factory: Callable[[psycopg.Connection], AliasResolver] | None = None,
        projector: Projector | None = None,
    ) -> "ChronosExtractorStage":
        if projector is not None:
            projector.deployment_name = deployment_name
            return cls(projector)
        return cls(Projector(
            dsn,
            extractor=extractor,
            routes=routes,
            extractor_source_subtypes=extractor_source_subtypes,
            resolver_factory=resolver_factory,
            deployment_name=deployment_name,
        ))

    @property
    def tracks_extraction_cache(self) -> bool:
        return bool(getattr(self.projector, "_tracks_extraction_cache", False))

    @property
    def source_subtypes(self) -> tuple[str, ...]:
        return tuple(
            str(subtype)
            for subtype in (
                getattr(self.projector, "_extractor_source_subtypes", ())
                or ()
            )
            if str(subtype)
        )

    @property
    def source_subtype_set(self) -> set[str]:
        return set(getattr(self.projector, "_extractor_source_subtype_set", set()))

    def input_signature(self) -> dict[str, Any] | None:
        if not self.tracks_extraction_cache:
            return None
        return {
            "version": 1,
            "extractor_fingerprint": str(
                getattr(self.projector, "_extractor_fingerprint", ""),
            ),
            "source_subtypes": list(self.source_subtypes),
        }

    def should_run(
        self,
        *,
        previous: Any | None,
        input_signature: dict[str, Any] | None,
        dirty_probe: Callable[[], bool],
    ) -> bool:
        if input_signature is None:
            return False
        if previous is None:
            return True
        previous_metadata = dict(getattr(previous, "metadata", None) or {})
        if previous_metadata.get("extractor_input_signature") != input_signature:
            return True
        return bool(dirty_probe())

    def resolver_factory(self, conn: psycopg.Connection) -> AliasResolver:
        return self.projector._resolver_factory(conn)

    def extract(self, text: str) -> list[Any]:
        return list(self.projector._extractor.extract(text))

    def route(
        self,
        entity: Any,
        *,
        src_node_id: str,
        src_subtype: str,
        resolver: AliasResolver,
        resolver_misses: list[tuple[str, str]] | None = None,
        endpoint_requests: list[Any] | None = None,
    ) -> Any | None:
        return self.projector._router.route(
            entity,
            src_node_id=src_node_id,
            src_subtype=src_subtype,
            resolver=resolver,
            resolver_misses=resolver_misses,
            endpoint_requests=endpoint_requests,
        )

    @property
    def endpoint_routes(self) -> tuple[Any, ...]:
        """Routes that keep links to unloaded targets (issue #3666), read
        from the router table the extractor routes through."""
        return self.projector._router.endpoint_routes
