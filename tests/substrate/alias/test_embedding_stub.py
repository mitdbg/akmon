"""EmbeddingAliasBackend — construction-time validation.

Per `redesign-alias-resolution` (2026-04-26) shipped the slot.
Per `add-embedding-worker` (2026-04-26) replaced the stub
`.match()` raise with a real implementation; the
`EmbeddingBackendNotInstalled` error class now fires at
*construction* when no embedder is passed (a misconfiguration).
"""
from __future__ import annotations

import pytest

from okg.substrate.alias import (
    EmbeddingAliasBackend,
    EmbeddingBackendNotInstalled,
)
from okg.substrate.embeddings import (
    EmbeddingConfig,
    HashEmbedder,
    build_embedding_execution_plan,
)


def _hash_plan():
    return build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )


def test_construction_without_embedder_raises():
    with pytest.raises(EmbeddingBackendNotInstalled) as exc_info:
        EmbeddingAliasBackend(conn=None)  # type: ignore[arg-type]
    err = exc_info.value
    assert "verified EmbeddingExecutionPlan" in err.fix_hint


def test_construction_with_raw_unverified_embedder_raises():
    with pytest.raises(EmbeddingBackendNotInstalled, match="verified"):
        EmbeddingAliasBackend(
            conn=None,  # type: ignore[arg-type]
            embedder=HashEmbedder(),  # type: ignore[arg-type]
        )


def test_construction_with_embedder_succeeds():
    """Passing an embedder is the supported path; we exercise
    construction here and leave Postgres-backed `.match()`
    behavior to test_embedding_alias_backend.py."""
    b = EmbeddingAliasBackend(
        conn=None,  # type: ignore[arg-type]
        embedder=_hash_plan(),
    )
    assert b.name == "embedding"
    assert b.accept_threshold == 0.85


def test_match_with_empty_needle_returns_empty():
    """Empty-needle short-circuit doesn't touch the conn."""
    b = EmbeddingAliasBackend(
        conn=None,  # type: ignore[arg-type]
        embedder=_hash_plan(),
    )
    assert b.match("") == []
