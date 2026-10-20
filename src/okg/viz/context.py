"""Substrate-native viz context object.

A `VizContext` is the HTTP handler's dependency bundle: a
Postgres connection pool, a pinned `Session`, the deployment
manifest YAML, and the viz config. It does NOT mimic the
legacy `Backbone` surface — callers go through
`okg.viz.reads` for node/edge reads, and through
`okg.substrate.mcp.*` for the tool-shaped operations that the
substrate MCP already exposes (graph_stats, list_neighbors,
aggregate_neighbors, get_node, search).

Handlers borrow a connection from the pool for the duration of
a single HTTP request via `ctx.conn()`. The pool is shared
across requests; the session pin is fixed for the life of the
process (refreshable only via an explicit reload, matching the
stability guarantees the legacy snapshot boot used to provide).
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import psycopg
from psycopg_pool import ConnectionPool

from okg.substrate.mcp.session import (
    Session,
    assert_session_matches,
    open_session,
)

logger = logging.getLogger(__name__)


class CatalogSemanticsUnavailable(RuntimeError):
    """A hierarchy/absorption interpretation could not be established."""

    def __init__(
        self,
        *,
        cause: str,
        omitted_work: list[str],
        deployment: str,
        generation_id: int | str,
    ) -> None:
        super().__init__(cause)
        self.cause = cause
        self.omitted_work = list(omitted_work)
        self.scope = {
            "deployment": deployment,
            "generation_id": generation_id,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "unavailable",
            "cause": self.cause,
            "omitted_work": list(self.omitted_work),
            "scope": dict(self.scope),
            "retryable": True,
            "freshness": "unavailable",
        }


@dataclass
class VizContext:
    """Viz HTTP handler dependency bundle."""

    dsn: str
    deployment_name: str
    manifest: dict[str, Any]
    config: dict[str, Any]
    pool: ConnectionPool
    session: Session
    deployment_dir: Path
    health_snapshot: dict[str, Any] = field(default_factory=dict)
    # A server started with an explicit --generation is an immutable historical
    # reader. Browser cache-bypass headers may still force recomputation, but
    # they must never silently re-pin that process to the latest generation.
    # Unpinned long-running operator servers keep the normal refresh behavior.
    generation_refresh_enabled: bool = True

    # Guards an in-place re-pin of `session` to a newer published generation
    # (see refresh_to_latest); a single attribute swap, but serialized so two
    # concurrent refreshes cannot interleave the re-resolve + cache clears.
    _refresh_lock: Any = field(default_factory=threading.Lock, repr=False)

    # Cached archetype-kind classification. First-call composes the
    # deployment's catalog and applies the default + override
    # mapping; subsequent calls return the cached set.
    _archetype_kinds_cache: dict[str, str] | None = field(default=None)

    def archetype_kinds(self) -> dict[str, str]:
        """Return {archetype: "contains"|"references"} for this
        deployment. Composes the catalog on first call.
        Used by the hierarchy endpoints to filter edges by
        relation-kind. (add-hierarchical-graph-navigation §1)"""
        if self._archetype_kinds_cache is None:
            from okg.substrate.catalog.modules_compose import compose_catalog
            try:
                composed = compose_catalog(self.deployment_dir)
            except Exception as exc:
                raise CatalogSemanticsUnavailable(
                    cause="archetype_catalog_compose_failed",
                    omitted_work=["archetype_relation_kind_classification"],
                    deployment=self.deployment_name,
                    generation_id=self.session.generation_id,
                ) from exc
            kinds: dict[str, str] = {}
            for row in composed.narrowings:
                arch = row.edge_archetype
                if arch in kinds:
                    continue
                kinds[arch] = composed.relation_kind_for_archetype(arch)
            self._archetype_kinds_cache = kinds
        return self._archetype_kinds_cache

    def containment_archetypes(self) -> set[str]:
        """Convenience: just the archetype names classified as
        `contains`."""
        return {a for a, k in self.archetype_kinds().items() if k == "contains"}

    # Cached absorbed-into-parent map. First-call composes the
    # catalog and combines the tier system + containment narrowings
    # to derive which child subtypes are absorbed into which parent
    # subtypes. (add-compound-graph-with-semantic-zoom §1)
    _absorption_cache: dict[str, str] | None = field(default=None)

    def absorbed_subtypes(self) -> dict[str, str]:
        """Return `{ child_subtype: parent_subtype }` for every
        infrastructure-tier subtype that has a contains-narrowing
        from a non-infrastructure parent. Per-deployment override
        `viz.absorb_child: { <subtype>: bool }` wins over the
        auto-derived default."""
        if self._absorption_cache is not None:
            return self._absorption_cache

        viz_cfg = self.config or {}
        tiers = viz_cfg.get("tiers") or {}
        overrides = viz_cfg.get("absorb_child") or {}

        # Auto-derive absorption rules from the narrowing catalog.
        # Two regimes:
        #
        #   (A) Tier-aware (`viz.tiers` configured): child is
        #       `infrastructure` AND parent is not. Strict — uses
        #       only `contains` archetypes.
        #
        #   (B) Tier-less fallback (e.g. af_pubs): infer absorption
        #       from ANY narrowing (contains, references, etc.) when
        #       the child subtype is significantly more populous than
        #       the parent. This catches af_pubs's
        #       `publication --[references]--> section` (61K sections
        #       under 9K publications) which is morally containment
        #       even though the catalog calls it `references`.
        try:
            from okg.substrate.catalog.modules_compose import compose_catalog
            composed = compose_catalog(self.deployment_dir)
            contains_archs = self.containment_archetypes()
            tier_aware = bool(tiers)
            auto: dict[str, str] = {}
            if tier_aware:
                for row in composed.narrowings:
                    if row.edge_archetype not in contains_archs:
                        continue
                    child_tier = tiers.get(row.child_subtype)
                    parent_tier = tiers.get(row.parent_subtype)
                    if child_tier == "infrastructure" and parent_tier != "infrastructure":
                        auto.setdefault(row.child_subtype, row.parent_subtype)
            else:
                # Need per-subtype counts to decide the more-populous
                # side. One quick query into the Chronos graph.
                counts: dict[str, int] = {}
                try:
                    from okg.substrate.graph_store import ChronosGraphStore
                    with self.conn() as conn:
                        graph = ChronosGraphStore.from_connection(
                            conn, ensure_schema=False,
                        ).checkout_generation(
                            self.session.generation_id,
                            branch=self.session.branch_name,
                        )
                        for row in graph.query(
                            """
                            SELECT subtype, count(*) AS n
                              FROM okg.graph_nodes
                          GROUP BY subtype
                            """,
                        ):
                            counts[row["subtype"]] = int(row["n"])
                except Exception as exc:
                    raise CatalogSemanticsUnavailable(
                        cause="absorption_subtype_count_failed",
                        omitted_work=[
                            "subtype_population_counts",
                            "absorption_catalog_rules",
                        ],
                        deployment=self.deployment_name,
                        generation_id=self.session.generation_id,
                    ) from exc
                # Centrality guard: count incoming narrowings per
                # subtype from DISTINCT other subtypes. A subtype with
                # many incoming sources is a hub (e.g. `publication`
                # in af_pubs is referenced by annotations, mentions,
                # views, sections, tables, …). Hubs are forest-roots,
                # not absorbed children — even if they're populous.
                inbound: dict[str, set[str]] = {}
                for row in composed.narrowings:
                    if row.parent_subtype == row.child_subtype:
                        continue
                    inbound.setdefault(row.child_subtype, set()).add(row.parent_subtype)

                # Absorb child under parent when:
                #   - child is at least 1.5× parent's instance count
                #   - child has ≤ 2 distinct incoming-source subtypes
                #     (i.e. it's leaf-y, not a hub)
                for row in composed.narrowings:
                    if row.parent_subtype == row.child_subtype:
                        continue
                    cc = counts.get(row.child_subtype, 0)
                    pc = counts.get(row.parent_subtype, 0)
                    incoming_distinct = len(inbound.get(row.child_subtype, set()))
                    if cc >= max(50, pc * 1.5) and incoming_distinct <= 2:
                        auto.setdefault(row.child_subtype, row.parent_subtype)
        except CatalogSemanticsUnavailable:
            raise
        except Exception as exc:
            raise CatalogSemanticsUnavailable(
                cause="absorption_catalog_compose_failed",
                omitted_work=["absorption_catalog_rules"],
                deployment=self.deployment_name,
                generation_id=self.session.generation_id,
            ) from exc

        # Apply per-deployment overrides.
        result = dict(auto)
        for child, value in overrides.items():
            if value is False:
                result.pop(child, None)
            elif isinstance(value, str):
                result[child] = value
            # value is True with no parent — keep auto-derived default

        self._absorption_cache = result
        return result

    def refresh_to_latest(self) -> int | str:
        """Re-resolve the latest published generation and re-pin the session.

        The read context pins its generation at boot and never advances on its
        own, so a long-running server serves an increasingly stale generation as
        the worker publishes. An explicit operator refresh calls this to pull the
        newest published generation WITHOUT a restart. Read-only (it only opens a
        session); fails loud if no published generation exists. Returns the
        resolved generation id (unchanged when already current).
        """
        if not self.generation_refresh_enabled:
            return self.session.generation_id
        with self._refresh_lock:
            with self.pool.connection() as c:
                c.autocommit = True
                latest = open_session(
                    c,
                    deployment_name=self.deployment_name,
                    requested_generation=None,
                    branch_name=self.session.branch_name,
                )
                from okg.viz.boot import _generation_health_snapshot

                latest_health = _generation_health_snapshot(
                    c,
                    deployment_name=self.deployment_name,
                    generation_id=latest.generation_id,
                )
            if latest.generation_id != self.session.generation_id:
                self.session = latest
                # Generation-scoped derived caches must not survive a re-pin.
                self._absorption_cache = None
                self._archetype_kinds_cache = None
            self.health_snapshot = latest_health
            return self.session.generation_id

    @contextmanager
    def conn(self) -> Iterator[psycopg.Connection]:
        """Borrow a connection from the pool for one request.

        The connection is returned to the pool on exit. Each
        borrowed connection re-asserts the session pin
        (db_fingerprint match) so a pool shared across
        deployments stays safe.
        """
        with self.pool.connection() as c:
            c.autocommit = True
            assert_session_matches(c, self.session)
            yield c

    @property
    def compatibility_generation_id(self) -> int | str:
        """Generation id for SQL artifact/control-plane compatibility tables.

        Chronos-native sessions expose a checkpoint id as
        ``session.generation_id``. Some viz subsystems still read SQL
        artifact/control-plane tables keyed by the publisher-era integer
        generation id. Checkpoint metadata carries that bridge id while the
        remaining read paths are migrated.
        """

        return (
            self.session.compat_generation_id
            if self.session.compat_generation_id is not None
            else self.session.generation_id
        )

    @property
    def legacy_generation_id(self) -> int | str:
        """Deprecated compatibility alias for older callers.

        Active viz route handlers should use ``compatibility_generation_id`` so
        publisher-era table keys stay clearly marked as compatibility bridges.
        """

        return self.compatibility_generation_id

    def close(self) -> None:
        """Shut down the connection pool."""
        try:
            self.pool.close()
        except Exception:  # pragma: no cover - defensive
            logger.warning("viz pool close failed", exc_info=True)
