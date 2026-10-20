"""SqlCascadeBackend — postgres-backed integration tests."""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.alias import SqlCascadeBackend


pytestmark = pytest.mark.postgres


def _seed_catalog_with_alias(
    conn: psycopg.Connection,
    *,
    alias: str,
    canonical: str,
    scope: str | None = None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.aliases (alias, canonical, scope, "
            "catalog_version_id) VALUES (%s, %s, %s, %s)",
            (alias, canonical, scope, cv),
        )
    return cv


def test_exact_alias_returns_method_exact(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_with_alias(
            conn, alias="alice", canonical="person:alice",
        )
        backend = SqlCascadeBackend(conn)
        matches = backend.match("alice")
    assert matches
    top = max(matches, key=lambda m: m.similarity)
    assert top.canonical == "person:alice"
    assert top.method == "exact"
    assert top.similarity == 1.0


def test_one_typo_returns_method_trigram_or_levenshtein(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_with_alias(
            conn, alias="alpha-bravo-charlie",
            canonical="n:abc",
        )
        backend = SqlCascadeBackend(conn, accept_threshold=0.6)
        # One-character delete: should trigger trigram or
        # Levenshtein tier.
        matches = backend.match("alpha-bravo-charli")
    assert matches
    top = max(matches, key=lambda m: m.similarity)
    assert top.canonical == "n:abc"
    assert top.method in ("trigram", "levenshtein")
    assert top.similarity >= 0.6


def test_exact_only_skips_fuzzy_tiers(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_with_alias(
            conn,
            alias="alpha-bravo-charlie",
            canonical="n:abc",
        )
        backend = SqlCascadeBackend(conn, accept_threshold=0.6, exact_only=True)
        exact = backend.match("alpha-bravo-charlie")
        near = backend.match("alpha-bravo-charli")

    assert exact
    assert max(exact, key=lambda m: m.similarity).canonical == "n:abc"
    assert near == []


def test_no_near_match_returns_empty(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_with_alias(
            conn, alias="alice", canonical="person:alice",
        )
        backend = SqlCascadeBackend(conn, accept_threshold=0.6)
        matches = backend.match("zzzzz_unrelated_text")
    assert matches == []


def test_empty_needle_returns_empty(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_with_alias(
            conn, alias="alice", canonical="person:alice",
        )
        backend = SqlCascadeBackend(conn)
        assert backend.match("") == []
