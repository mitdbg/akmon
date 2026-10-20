"""EmbeddingAliasBackend — cosine-similarity over
`nodes_live.embedding`.

Per `redesign-alias-resolution` (2026-04-26) shipped the slot;
`add-embedding-worker` (2026-04-26) lands the implementation.
"""
from __future__ import annotations

from typing import Optional

import psycopg

from okg.substrate.alias.protocol import AliasMatch
from okg.substrate.embeddings import EmbeddingExecutionPlan
from okg.substrate.errors import SubstrateError


__all__ = ["EmbeddingAliasBackend", "EmbeddingBackendNotInstalled"]


class EmbeddingBackendNotInstalled(SubstrateError):
    """Raised when `EmbeddingAliasBackend` is constructed
    without an `embedder=`. The error code is preserved
    from the prior stub for backwards compatibility; the
    fix-hint updates to the misconfiguration case."""

    code = "embedding_backend_not_installed"


class EmbeddingAliasBackend:
    """Cosine-similarity alias resolution over
    `nodes_live.embedding`.

    Embeds the needle via the injected `embedder`, queries
    Postgres for the closest nodes by cosine distance,
    converts to similarity (`1 - distance`), and returns
    them as `AliasMatch` rows.

    The catalog version is read once and cached (mirrors
    `SqlCascadeBackend`) so the cascade short-circuits stay
    cheap.
    """

    name: str = "embedding"

    def __init__(
        self,
        conn: psycopg.Connection,
        *,
        embedder: Optional[EmbeddingExecutionPlan] = None,
        accept_threshold: float = 0.85,
        limit: int = 5,
        subtype: Optional[str] = None,
    ) -> None:
        if embedder is None:
            raise EmbeddingBackendNotInstalled(
                "EmbeddingAliasBackend requires an embedder",
                fix_hint=(
                    "pass a verified EmbeddingExecutionPlan; use the hash "
                    "backend only for deterministic tests/development"
                ),
            )
        if not isinstance(embedder, EmbeddingExecutionPlan) or not embedder.enabled:
            raise EmbeddingBackendNotInstalled(
                "EmbeddingAliasBackend requires an enabled verified "
                "EmbeddingExecutionPlan",
                fix_hint="build the plan from the deployment's admitted embeddings block",
            )
        self._conn = conn
        self._embedder = embedder
        self._model_id = embedder.model_id
        self.accept_threshold = float(accept_threshold)
        self._limit = int(limit)
        self._subtype = subtype

    def match(
        self,
        needle: str,
        *,
        entity_type: Optional[str] = None,
    ) -> list[AliasMatch]:
        if not needle:
            return []
        vec = self._embedder.embed(needle)
        vecstr = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"
        query_params: dict = {
            "vec": vecstr,
            "limit": self._limit,
            "model_id": self._model_id,
        }
        subtype_clause = ""
        if self._subtype is not None:
            subtype_clause = "AND subtype = :subtype"
            query_params["subtype"] = self._subtype
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            self._conn, ensure_schema=False
        ).checkout()
        # Use the cosine_distance(...) function form, NOT the `<=>` operator: the
        # Chronos checkout query layer mis-parses the pgvector `<=>` operator (it
        # rewrites it to `IS NOT DISTINCT FROM`, yielding `numeric - boolean`).
        # cosine_distance(a, b) == a <=> b. (Same fix as semantic_concept_linker /
        # mcp.search.)
        rows = graph.query(
            f"""
            SELECT node_id,
                   1.0 - cosine_distance(embedding, :vec::vector) AS similarity
             FROM okg.graph_nodes
             WHERE embedding IS NOT NULL
               AND embedding_model = :model_id
               {subtype_clause}
             ORDER BY cosine_distance(embedding, :vec::vector) ASC
             LIMIT :limit
            """,
            query_params,
        )
        return [
            AliasMatch(
                canonical=r["node_id"],
                similarity=float(r["similarity"]),
                method="embedding",
            )
            for r in rows
        ]
