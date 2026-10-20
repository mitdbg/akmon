"""MCP-side surface for fuzzy alias + identifier mentions."""
from __future__ import annotations

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.substrate.catalog.identifier_patterns import (
    IdentifierPattern, write_patterns,
)
from okg.substrate.mcp import find_identifier_mentions, search
from okg.substrate.mcp.server import build_server
from okg.substrate.mcp.session import open_session
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _seed_published(dsn: str) -> int:
    """Publish a tiny graph and return the catalog version it pins.

    The explicit test writer clones the seeded template into a
    deployment-bound catalog version, and the published generation pins
    that clone, not the template. Alias and identifier-pattern rows are
    catalog-version scoped, so a test must write them under the version
    the read session resolves. Issue #2554.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            # The unbound template the explicit test writer clones.
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now())"
            )
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(
                node_id="person:alice",
                subtype="person",
                attrs={"name": "alice"},
                source_record_id={"id": 1},
                source_revision={"rev": "a"},
                op="I",
            )
        ])
    publish = Publisher(
        dsn,
        "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert publish["status"] == "published", publish
    with psycopg.connect(dsn, autocommit=True) as conn:
        pinned = open_session(conn, deployment_name="default").catalog_version_id
    return pinned


# ---------- search method='alias' fuzzy=True flag --------------------

def test_alias_exact_default_no_fuzzy_fallback(clean_runtime: str) -> None:
    """When fuzzy=False (default), a needle with no exact match
    returns zero results — no cascade fallthrough."""
    cv = _seed_published(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases (alias, canonical, scope, "
                "catalog_version_id) VALUES (%s, %s, NULL, %s)",
                ("alicee", "person:alice", cv),
            )
        session = open_session(conn, deployment_name="default")
        result = search(
            conn, session, "alice",  # only "alicee" is the alias
            method="alias", filter={}, limit=10,
        )
        # Positive control: the seeded alias is visible to this session, so
        # the empty result above is the no-fallback rule, not a missing row.
        control = search(
            conn, session, "alicee",
            method="alias", filter={}, limit=10,
        )
    assert result["results"] == []
    assert len(control["results"]) == 1
    assert control["results"][0]["canonical"] == "person:alice"
    assert control["results"][0]["match_kind"] == "exact"


def test_alias_fuzzy_true_falls_through_to_cascade(clean_runtime: str) -> None:
    """When fuzzy=True and exact misses, the cascade resolves
    through trigram or Levenshtein."""
    cv = _seed_published(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases (alias, canonical, scope, "
                "catalog_version_id) VALUES (%s, %s, NULL, %s)",
                ("alice", "person:alice", cv),
            )
        session = open_session(conn, deployment_name="default")
        result = search(
            conn, session, "alise",  # 1-edit typo of 'alice'
            method="alias", filter={}, limit=10, fuzzy=True,
        )
    assert len(result["results"]) == 1
    hit = result["results"][0]
    assert hit["canonical"] == "person:alice"
    # Match kind reflects the cascade tier that resolved.
    assert hit["match_kind"] in ("trigram", "levenshtein")


def test_alias_fuzzy_true_still_short_circuits_on_exact(
    clean_runtime: str,
) -> None:
    """Fuzzy doesn't override exact: when exact match is present,
    it wins."""
    cv = _seed_published(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases (alias, canonical, scope, "
                "catalog_version_id) VALUES (%s, %s, NULL, %s)",
                ("alice", "person:alice", cv),
            )
        session = open_session(conn, deployment_name="default")
        result = search(
            conn, session, "alice",
            method="alias", filter={}, limit=10, fuzzy=True,
        )
    assert len(result["results"]) == 1
    assert result["results"][0]["match_kind"] == "exact"


# ---------- find_identifier_mentions function-level + MCP ---------

def test_find_identifier_mentions_returns_mentions(clean_runtime: str) -> None:
    cv = _seed_published(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        write_patterns(conn, [
            IdentifierPattern("jira", "CMS-[0-9]+"),
        ], cv)
        session = open_session(conn, deployment_name="default")
        result = find_identifier_mentions(
            conn, session, "Filed CMS-1234 and CMS-99",
        )
    assert result["catalog_version_id"] == session.catalog_version_id
    assert len(result["mentions"]) == 2
    mentions = sorted(m["value"] for m in result["mentions"])
    assert mentions == ["CMS-1234", "CMS-99"]
    assert all(m["evidence_kind"] == "extracted_entity" for m in result["mentions"])
    assert all("evidence_kind_provenance" in m for m in result["mentions"])


def test_find_identifier_mentions_through_mcp_server_writes_trace(
    clean_runtime: str,
) -> None:
    """End-to-end: invoke through the MCP server's tool dispatch
    and verify a trace row lands."""
    cv = _seed_published(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        write_patterns(conn, [
            IdentifierPattern("jira", "CMS-[0-9]+"),
        ], cv)

    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="default",
    )
    # Migrated from `find_identifier_mentions(text=...)` →
    # `search(query=..., method="identifier_mentions")` per the
    # shrink-mcp-to-operator-surface migration map.
    tool = mcp._tool_manager._tools["search"]  # type: ignore[attr-defined]
    result = tool.fn(query="Filed CMS-99", method="identifier_mentions")
    assert len(result["mentions"]) == 1
    assert result["mentions"][0]["value"] == "CMS-99"
    assert result["mentions"][0]["evidence_kind"] == "extracted_entity"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.mcp_trace_events "
                "WHERE tool_name = 'search'"
            )
            assert cur.fetchone()[0] == 1
