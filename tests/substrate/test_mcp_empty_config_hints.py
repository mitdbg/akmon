"""Regression test for `debug-substrate-okg-self` Phase 5.

When MCP search / extraction tools return empty AND the
relevant catalog config is itself empty (zero aliases at
the active cv, zero identifier patterns, no NER chain
registered), the response includes a structured `config`
block. Empty results from a populated catalog do NOT.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.mcp import open_session
from okg.substrate.mcp.search import search
from okg.substrate.mcp.tools import find_identifier_mentions


pytestmark = pytest.mark.postgres


def _seed_session(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        (catalog_version_id,) = cur.fetchone()
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn)
    store.checkpoint_generation(
        deployment_name="default",
        branch="default",
        catalog_version_id=catalog_version_id,
        metadata={"correlation_id": "test-seed"},
    )


def test_alias_search_empty_table_includes_hint(clean_runtime: str) -> None:
    """No aliases in catalog → response carries
    `config.aliases_registered: 0` + fix hint."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_session(conn)
        sess = open_session(conn, deployment_name="default")
        result = search(
            conn, sess, "anything",
            method="alias", filter={}, limit=10,
        )
    assert result["results"] == []
    assert "config" in result, (
        f"empty alias table should produce config hint; got {result}"
    )
    assert result["config"]["aliases_registered"] == 0
    assert "okg catalog load" in result["config"]["fix_hint"]


def test_alias_search_populated_table_no_hint(clean_runtime: str) -> None:
    """Aliases exist → response has NO config block."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_session(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "  (alias, canonical, scope, catalog_version_id) "
                "VALUES ('foo', 'concept:foo', NULL, "
                "  (SELECT MAX(catalog_version_id) FROM "
                "    okg.catalog_versions))"
            )
        sess = open_session(conn, deployment_name="default")
        result = search(
            conn, sess, "no_such_alias",
            method="alias", filter={}, limit=10,
        )
    assert result["results"] == []
    # Catalog has 1 alias; the empty result is "no match",
    # not "no table".
    assert "config" not in result, (
        f"populated alias table should NOT produce config hint; "
        f"got {result}"
    )


def test_find_identifier_mentions_empty_table_includes_hint(
    clean_runtime: str,
) -> None:
    """No identifier patterns in catalog → response carries
    `config.patterns_registered: 0` + fix hint."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_session(conn)
        sess = open_session(conn, deployment_name="default")
        result = find_identifier_mentions(
            conn, sess, "DOI:10.1234/abc and orcid 0000-0002-1825-0097",
        )
    assert result["mentions"] == []
    assert "config" in result, (
        f"empty patterns table should produce hint; got {result}"
    )
    assert result["config"]["patterns_registered"] == 0
    assert "identifier_patterns" in result["config"]["fix_hint"]
