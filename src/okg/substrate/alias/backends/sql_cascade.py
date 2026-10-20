"""SqlCascadeBackend — wraps okg.fuzzy_resolve_alias.

One round-trip; the SQL function does exact → trigram →
Levenshtein internally with `IF FOUND THEN RETURN`
short-circuiting. This backend preserves that semantic
boundary while exposing the result through the AliasBackend
Protocol so non-SQL backends can compose around it.
"""
from __future__ import annotations

from typing import Optional

import psycopg

from okg.substrate.alias.protocol import AliasMatch


__all__ = ["SqlCascadeBackend"]


class SqlCascadeBackend:
    """Substrate-bound alias resolution via the SQL cascade."""

    name: str = "sql_cascade"

    def __init__(
        self,
        conn: psycopg.Connection,
        *,
        accept_threshold: float = 0.6,
        limit: int = 5,
        exact_only: bool = False,
    ) -> None:
        self._conn = conn
        self.accept_threshold = float(accept_threshold)
        self._limit = int(limit)
        self.exact_only = bool(exact_only)
        self._cv: Optional[int] = None

    def _catalog_version(self) -> int:
        if self._cv is not None:
            return self._cv
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            row = cur.fetchone()
        self._cv = row[0] if row and row[0] is not None else 0
        return self._cv

    def match(
        self,
        needle: str,
        *,
        entity_type: Optional[str] = None,
    ) -> list[AliasMatch]:
        if not needle:
            return []
        cv = self._catalog_version()
        with self._conn.cursor() as cur:
            if self.exact_only:
                cur.execute(
                    "SELECT canonical, 1.0::real AS similarity, "
                    "'exact'::text AS method "
                    "FROM okg.aliases "
                    "WHERE alias = %s AND catalog_version_id = %s "
                    "LIMIT %s",
                    (needle, cv, self._limit),
                )
                return [
                    AliasMatch(
                        canonical=r[0],
                        similarity=float(r[1]),
                        method=r[2],
                    )
                    for r in cur.fetchall()
                ]
            cur.execute(
                "SELECT canonical, similarity, method "
                "FROM okg.fuzzy_resolve_alias("
                "  %s, %s, %s::real, %s::int)",
                (needle, cv, self.accept_threshold, self._limit),
            )
            return [
                AliasMatch(
                    canonical=r[0],
                    similarity=float(r[1]),
                    method=r[2],
                )
                for r in cur.fetchall()
            ]
