"""LinkerRouter — maps ExtractedEntity → derived edge spec (M3.3).

Reads the M3.4 LinkerRoute records to decide:
  - which edge_type to emit
  - what the dst node_id is, by substituting only the four
    grammar-allowed fields ({resolved}, {value}, {type}, {src})
    into dst_template
  - what confidence to attach (route override > entity default)

If the entity_type isn't in the routing table, the entity is
dropped silently (legacy behavior preserved).

Resolution model: {resolved} requires an `AliasResolver` callable
that returns the canonical node_id for the entity, or None. None
drops the entity (cannot route without a target). M3.3's bundled
`PgAliasResolver` queries `okg.aliases` at the latest applied
catalog_version. Deployments may inject their own resolver
(e.g. one that consults a deployment-specific NodeCatalog).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Protocol

import psycopg

from okg.extraction import ExtractedEntity
from okg.substrate.catalog.linker_config import LinkerRoute


__all__ = [
    "AliasResolver",
    "DerivedEdge",
    "LinkEndpointRequest",
    "LinkerRouter",
    "PgAliasResolver",
    "StaticAliasResolver",
]


@dataclass(frozen=True)
class DerivedEdge:
    """A routed extractor output, ready to write as edge_facts."""

    src: str
    dst: str
    edge_type: str
    confidence: float
    entity_type: str
    entity_value: str
    #: True when `dst` is a "known by key, not loaded" endpoint (issue #3666).
    endpoint: bool = False


@dataclass(frozen=True)
class LinkEndpointRequest:
    """One endpoint a routed miss needs: its id, subtype and whole attrs."""

    node_id: str
    subtype: str
    attrs: dict


class AliasResolver(Protocol):
    """Resolve an extracted entity to a canonical node_id, or None.

    Implementations may consult okg.aliases, an in-memory map, or
    a remote service. Return None when the entity does not resolve;
    the router will drop it.
    """

    def resolve(self, entity: ExtractedEntity) -> Optional[str]:
        raise NotImplementedError


class StaticAliasResolver:
    """Backwards-compat shim around `CascadeAliasResolver` with a
    single `StaticAliasBackend`.

    Per `redesign-alias-resolution` (2026-04-26).
    """

    def __init__(self, mapping: dict[tuple[str, str], str]) -> None:
        # Local imports avoid an import cycle with the alias
        # package, which itself imports from okg.extraction.
        from okg.substrate.alias.backends.static import (
            StaticAliasBackend,
        )
        from okg.substrate.alias.cascade import CascadeAliasResolver

        self._cascade = CascadeAliasResolver(
            [StaticAliasBackend(mapping)], on_empty="drop",
        )

    def resolve(self, entity: ExtractedEntity) -> Optional[str]:
        return self._cascade.resolve(entity)


class PgAliasResolver:
    """Backwards-compat shim around `CascadeAliasResolver` with a
    single `SqlCascadeBackend`.

    Replaces the old exact-only behavior with the SQL function's
    exact → trigram → Levenshtein cascade. Default
    accept_threshold = 0.6.

    Per `redesign-alias-resolution` (2026-04-26).
    """

    fingerprint = {
        "class": "okg.substrate.projection.router.PgAliasResolver",
        "change_slug_resolution": "exact_change_alias_v1",
    }

    def __init__(self, conn: psycopg.Connection) -> None:
        from okg.substrate.alias.backends.sql_cascade import (
            SqlCascadeBackend,
        )
        from okg.substrate.alias.cascade import CascadeAliasResolver

        self._conn = conn
        self._cascade = CascadeAliasResolver(
            [SqlCascadeBackend(conn)], on_empty="drop",
        )

    def resolve(self, entity: ExtractedEntity) -> Optional[str]:
        if entity.type == "change_slug":
            return self._resolve_change_slug(entity.value)
        return self._cascade.resolve(entity)

    def _resolve_change_slug(self, value: str) -> Optional[str]:
        """OpenSpec change slugs are exact ids, not fuzzy aliases."""
        canonical = f"change:{value}"
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT canonical
                  FROM okg.aliases
                 WHERE alias = %s
                   AND canonical = %s
                   AND catalog_version_id = (
                         SELECT max(catalog_version_id)
                           FROM okg.catalog_versions
                          WHERE status = 'applied'
                       )
                 LIMIT 1
                """,
                (value, canonical),
            )
            row = cur.fetchone()
        return str(row[0]) if row else None


class LinkerRouter:
    """Routes ExtractedEntity → DerivedEdge using LinkerRoute records.

    Construction takes the resolved route list (from
    `okg.substrate.catalog.linker_config.load_linker_config`) and
    builds a lookup keyed by entity_type → src_subtype → route.
    """

    def __init__(self, routes: Iterable[LinkerRoute]) -> None:
        # entity_type → src_subtype → LinkerRoute
        # src_subtype=None for simple routes (matches any subtype)
        self._table: dict[str, dict[Optional[str], LinkerRoute]] = {}
        for r in routes:
            self._table.setdefault(r.entity_type, {})[r.src_subtype] = r

    @property
    def endpoint_routes(self) -> tuple[LinkerRoute, ...]:
        """The routes in the table that declare `on_miss` (issue #3666)."""
        return tuple(
            route
            for by_subtype in self._table.values()
            for route in by_subtype.values()
            if route.on_miss is not None
        )

    def route(
        self,
        entity: ExtractedEntity,
        *,
        src_node_id: str,
        src_subtype: str,
        resolver: AliasResolver,
        resolver_misses: Optional[list[tuple[str, str]]] = None,
        endpoint_requests: Optional[list[LinkEndpointRequest]] = None,
    ) -> Optional[DerivedEdge]:
        """Apply the routing table; return a DerivedEdge or None.

        `endpoint_requests` (issue #3666): when provided and the route
        declares `on_miss`, a resolver miss returns the edge to the
        endpoint id (`DerivedEdge.endpoint` true) and appends the endpoint
        the caller must write. The miss is still appended to
        `resolver_misses`, so its retry row keeps waiting for the real
        record. A caller that passes no list gets None, as before, because
        it would write an edge without its endpoint.

        Returns None when:
          - entity_type isn't in the table
          - by-src-subtype has no match for src_subtype AND no _default
          - dst_template references {resolved} but the resolver
            returns None
          - the resulting dst would equal src_node_id (self-loop
            from a self-mention; legacy behavior)

        `resolver_misses` (fix-projection-alias-identity #685, ruling 1):
        when provided, a RESOLVER MISS on a `{resolved}` route -- a real
        route exists but the target alias has not landed yet -- appends
        `(entity.type, entity.value)`. This is the only retryable None:
        no-route (entity_type / src_subtype not in the table) and
        self-loop drops are NOT recorded, because a route-table change
        already rotates the extractor fingerprint, so those re-extract
        without a retry entry. The caller records the misses in
        okg.link_retry so a later alias landing re-drives extraction.
        """
        by_subtype = self._table.get(entity.type)
        if by_subtype is None:
            return None
        # Resolution preference:
        # 1. Simple route (src_subtype=None)
        # 2. Specific by_src_subtype match
        # 3. by_src_subtype _default
        route = (
            by_subtype.get(None)
            or by_subtype.get(src_subtype)
            or by_subtype.get("_default")
        )
        if route is None:
            return None

        dst = self._substitute(
            route.dst_template,
            entity=entity,
            src_node_id=src_node_id,
            resolver=resolver,
        )
        endpoint = False
        if dst is None:
            # `_substitute` returns None only when a `{resolved}` template
            # got None from the resolver (the other three fields never
            # return None) -- i.e. a resolver miss, the retryable case.
            if resolver_misses is not None:
                resolver_misses.append((entity.type, entity.value))
            spec = route.on_miss
            if spec is None or endpoint_requests is None:
                return None
            dst = spec.node_id(entity.value)
            if dst == src_node_id:
                return None
            endpoint_requests.append(LinkEndpointRequest(
                node_id=dst,
                subtype=spec.subtype,
                attrs=spec.attrs(entity.value),
            ))
            endpoint = True
        if dst == src_node_id:
            return None

        confidence = (
            route.confidence
            if route.confidence is not None
            else entity.confidence
        )
        return DerivedEdge(
            src=src_node_id,
            dst=dst,
            edge_type=route.edge_type,
            confidence=confidence,
            entity_type=entity.type,
            entity_value=entity.value,
            endpoint=endpoint,
        )

    @staticmethod
    def _substitute(
        template: str,
        *,
        entity: ExtractedEntity,
        src_node_id: str,
        resolver: AliasResolver,
    ) -> Optional[str]:
        """Substitute the four grammar-allowed fields. Order:
        {resolved} is computed lazily (only if present) and may
        return None → drop the entity. The other three are pure
        substitutions.
        """
        out = template
        if "{resolved}" in out:
            resolved = resolver.resolve(entity)
            if resolved is None:
                return None
            out = out.replace("{resolved}", resolved)
        if "{value}" in out:
            out = out.replace("{value}", entity.value)
        if "{type}" in out:
            out = out.replace("{type}", entity.type)
        if "{src}" in out:
            out = out.replace("{src}", src_node_id)
        return out
