"""Application-side narrowing check (M5.3, M5.5 doc-honest).

Matches the DB trigger's semantic (see schema.sql
`okg._enforce_narrowing`): a triple
(src_subtype, edge_type, dst_subtype) is PERMITTED iff

  1. No applied catalog_version exists, OR
  2. There are zero narrowings rows at the active cv, OR
  3. The triple appears in `okg.narrowings` at the active cv.

Otherwise refused with `narrowing_violation`.

Property label (von Neumann review §4): this implements
**Property B** (strict whitelist CONDITIONAL on narrowings being
non-empty at the active cv), NOT design.md §867's **Property A**
(strict whitelist unconditionally). "Narrowings enforcement is
on" is a separate deployment-level property — deployments that
want A should assert `require_narrowings=True` at startup, so
an operator who accidentally ships an empty narrowings set gets
a loud failure instead of silent permissiveness.

The writer-path check runs BEFORE edge_facts INSERT so errors
surface with full context (the triple + where it came from).
The DB trigger is the backstop for any writer that bypasses
this function.

Cache:
  The narrowings table is small (hundreds of rows at most per
  design). The checker caches the full set for the active cv
  on a given connection, keyed by cv, so batch checks don't
  round-trip per edge. Invalidated on cv change.
"""
from __future__ import annotations

import logging
from typing import Optional

import psycopg

from okg.substrate.errors import SubstrateError


__all__ = [
    "NarrowingChecker",
    "assert_narrowing_permitted",
]


# Private cache attribute on psycopg.Connection so a single conn
# doesn't refetch narrowings on every edge insert.
_CACHE_ATTR = "_okg_substrate_narrowings_cache"
logger = logging.getLogger(__name__)


class NarrowingChecker:
    """Pre-loaded narrowing whitelist for the active cv.

    Usage:
        checker = NarrowingChecker(conn)
        checker.assert_permitted("page", "references", "concept")

    The whitelist is loaded once per instance; call `refresh()`
    if the catalog changes within a long-lived session.
    """

    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn
        self._cv: Optional[int] = None
        # `_whitelist` is the materialized set of (src, edge, dst)
        # triples allowed at the active cv, expanded through the
        # subtype-refinement closure. Per
        # `add-subtype-refines-to-catalog`: a narrowing declared
        # at an abstract parent matches every concrete refinement
        # via `okg.subtype_ancestors`. We expand once at refresh
        # time and keep the membership check O(1).
        self._whitelist: Optional[set[tuple[str, str, str]]] = None
        self.refresh()

    def refresh(self) -> None:
        """Re-read the narrowings whitelist for the active cv,
        expanded through the subtype-refinement closure.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(catalog_version_id) FROM "
                "okg.catalog_versions WHERE status = 'applied'"
            )
            row = cur.fetchone()
            cv = row[0] if row else None
            self._cv = cv
            if cv is None:
                self._whitelist = None
                return
            # Closure-expanded whitelist: for each narrowing
            # (parent_subtype = P, edge_type = E, child_subtype = C),
            # emit (S, E, D) for every (S, D) where P is an
            # ancestor of S and C is an ancestor of D. When
            # subtype_ancestors contains only depth-0 reflexive
            # rows (no refinements declared), this reduces to the
            # exact narrowings rows — strict superset of the
            # pre-refinement behavior.
            cur.execute(
                """
                SELECT DISTINCT
                       sa_src.subtype_name,
                       n.edge_type,
                       sa_dst.subtype_name
                  FROM okg.narrowings n
                  JOIN okg.subtype_ancestors sa_src
                    ON sa_src.catalog_version_id = n.catalog_version_id
                   AND sa_src.ancestor_name = n.parent_subtype
                  JOIN okg.subtype_ancestors sa_dst
                    ON sa_dst.catalog_version_id = n.catalog_version_id
                   AND sa_dst.ancestor_name = n.child_subtype
                 WHERE n.catalog_version_id = %s
                UNION
                SELECT n.parent_subtype, n.edge_type, n.child_subtype
                  FROM okg.narrowings n
                 WHERE n.catalog_version_id = %s
                """,
                (cv, cv),
            )
            self._whitelist = {tuple(r) for r in cur.fetchall()}

    @property
    def catalog_version_id(self) -> Optional[int]:
        return self._cv

    @property
    def is_active(self) -> bool:
        """True iff the checker would actually refuse anything.

        Mirrors the DB trigger's no-op semantics: without an
        applied cv or without any narrowings at the active cv,
        every triple is permitted.
        """
        return self._whitelist is not None and len(self._whitelist) > 0

    def is_permitted(
        self,
        src_subtype: str,
        edge_type: str,
        dst_subtype: str,
    ) -> bool:
        if not self.is_active:
            return True
        whitelist = self._whitelist
        if whitelist is None:
            raise RuntimeError("active narrowing checker has no whitelist")
        return (src_subtype, edge_type, dst_subtype) in whitelist

    def assert_permitted(
        self,
        src_subtype: str,
        edge_type: str,
        dst_subtype: str,
        *,
        src_node_id: Optional[str] = None,
        dst_node_id: Optional[str] = None,
    ) -> None:
        """Raise SubstrateError(`narrowing_violation`) if refused."""
        if self.is_permitted(src_subtype, edge_type, dst_subtype):
            return
        err = SubstrateError(
            f"narrowing_violation: ({src_subtype}, {edge_type}, "
            f"{dst_subtype}) not in narrowings at "
            f"catalog_version {self._cv}",
            data={
                "src_subtype": src_subtype,
                "edge_type": edge_type,
                "dst_subtype": dst_subtype,
                "catalog_version_id": self._cv,
                "src_node_id": src_node_id,
                "dst_node_id": dst_node_id,
            },
            fix_hint=(
                "add this triple to narrowings.yaml for the active "
                "catalog version, or fix the extractor/route to emit "
                "a triple that's already allowed"
            ),
        )
        err.code = "narrowing_violation"
        raise err


def _current_applied_cv(conn: psycopg.Connection) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(catalog_version_id) FROM "
            "okg.catalog_versions WHERE status = 'applied'"
        )
        row = cur.fetchone()
        return row[0] if row else None


def _checker_for(conn: psycopg.Connection) -> NarrowingChecker:
    """Per-connection cached NarrowingChecker, invalidated on catalog-version
    change (as the module docstring promises).

    The expensive part is the closure-expanded whitelist load; it is reused
    while the applied cv is unchanged. A cheap indexed MAX(applied cv) probe
    detects a catalog apply within a long-lived connection's lifetime and
    rebuilds the whitelist, instead of serving the stale one forever.
    """
    cached = getattr(conn, _CACHE_ATTR, None)
    if cached is not None:
        if cached.catalog_version_id == _current_applied_cv(conn):
            return cached
        cached.refresh()
        return cached
    checker = NarrowingChecker(conn)
    try:
        setattr(conn, _CACHE_ATTR, checker)
    except Exception:  # noqa: BLE001 — connection wrapper may forbid setattr (e.g. __slots__); still return the fresh checker, just lose the cache speedup.
        logger.debug(
            "connection wrapper disallowed narrowing checker cache",
            exc_info=True,
        )
    return checker


def assert_narrowing_permitted(
    conn: psycopg.Connection,
    src_subtype: str,
    edge_type: str,
    dst_subtype: str,
    *,
    src_node_id: Optional[str] = None,
    dst_node_id: Optional[str] = None,
) -> None:
    """Stateless convenience wrapper: build a cached
    NarrowingChecker on the conn and call `assert_permitted`.

    Application-side narrowing enforcement in production is the batch
    `TransformConsistencyGate` (whose `narrowing_allowed` surface mirrors this
    checker's whitelist -- proven equivalent in
    `formal/transform/WriterGateNarrowingEquivalence.tla`); the per-edge live
    backstop is the DB trigger `okg._enforce_narrowing`. This wrapper is a
    doc-honest, richer-error alternative a writer path may call BEFORE INSERTing
    edge_facts so a rejection surfaces with the triple + node IDs in the payload
    rather than only the trigger's text; it is currently exercised by the
    equivalence and cache tests.
    """
    _checker_for(conn).assert_permitted(
        src_subtype, edge_type, dst_subtype,
        src_node_id=src_node_id, dst_node_id=dst_node_id,
    )
