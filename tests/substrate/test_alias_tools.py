"""Per `refine-doc-corpus-extraction` (2026-04-26) Stride F:
operator alias-tools — direct module API + MCP tool gating.

Tests:
- `seed_from_subtype` preview path counts inserts/conflicts
  without writing.
- `seed_from_subtype(apply=True)` writes; idempotent re-run
  reports `inserted_count=0, conflict_count=N`.
- `add_alias` / `remove_alias` round-trip.
- `suggest_aliases_from_unresolved_mentions` ranks by mention
  count.
- MCP `register_alias` write-tool gate (refuses without
  `OKG_AGENT=1`).
- MCP `suggest_aliases` is read-only and returns the same
  ranking.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

import psycopg
import pytest


pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch):
    """These tests assert on the result dictionary, so every call asks for
    the JSON result format (issue #3059: the default is one text block).
    Client sessions send the request _meta key; in-process
    FastMCP.call_tool calls run inside result_format_scope."""
    from okg.substrate.mcp.result_text import result_format_scope
    from tests.substrate.mcp._result_payload import json_client_sessions

    json_client_sessions(monkeypatch)
    with result_format_scope("json"):
        yield


def _tool_payload(res: Any) -> dict[str, Any]:
    """The JSON payload of one ``call_tool`` result, refusing an error result.

    Under mcp 2 ``call_tool`` returns a ``CallToolResult``. A refusal carries
    its envelope in ``structuredContent`` with ``isError`` set and NO text
    content, so joining the text blocks gave ``""`` and every refusal surfaced
    as a bare ``JSONDecodeError`` instead of the refusal that caused it.
    """
    if getattr(res, "isError", False):
        raise AssertionError(f"tool call refused: {res.content!r}")
    structured = getattr(res, "structuredContent", None)
    if isinstance(structured, dict):
        return structured
    contents = res[0] if isinstance(res, tuple) else getattr(res, "content", res)
    return json.loads("; ".join(c.text for c in contents if hasattr(c, "text")))


def _seed_cv_with_open_spec_changes(
    conn: psycopg.Connection, *, n: int = 3,
) -> tuple[int, int]:
    """Seed a fresh applied catalog_version and `n`
    `open_spec_change` nodes (each carrying a `change_id`
    attr). Returns (cv_id, generation_id)."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.subtypes "
            "  (name, catalog_version_id) VALUES "
            "  ('open_spec_change', %s), ('entity_mention', %s)",
            (cv, cv),
        )
        cur.execute(
            "INSERT INTO okg.graph_generations "
            "  (deployment_name, status, published_at, "
            "   catalog_version_id, correlation_id) "
            "VALUES ('test', 'published', now(), %s, 't1') "
            "RETURNING generation_id",
            (cv,),
        )
        gen = cur.fetchone()[0]
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    store = ChronosGraphStore.from_connection(conn)
    try:
        if n:
            store.apply_facts([
                NodeFact(
                    f"change:add-feature-{i}",
                    "open_spec_change",
                    {
                        "change_id": f"add-feature-{i}",
                        "title": f"Title {i}",
                    },
                    {},
                    {},
                )
                for i in range(n)
            ])
        store.checkpoint_generation(
            gen,
            deployment_name="test",
            catalog_version_id=cv,
        )
    finally:
        store.close()
    return cv, gen


def test_seed_from_subtype_preview_then_apply_idempotent(
    clean_runtime: str,
) -> None:
    from okg.substrate.catalog.aliases import seed_from_subtype

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv, _gen = _seed_cv_with_open_spec_changes(conn, n=3)

    # Preview: dry run reports 3 inserts / 0 conflicts.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        preview = seed_from_subtype(
            conn,
            subtype="open_spec_change",
            field="change_id",
            apply=False,
        )
    assert preview.dry_run is True
    assert preview.inserted_count == 3
    assert preview.conflict_count == 0
    assert preview.catalog_version_id == cv
    # Sample contains alias→canonical pairs.
    assert all(
        a.startswith("add-feature-") and c.startswith("change:")
        for (a, c) in preview.sample_inserted
    )

    # Apply: 3 rows land.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        applied = seed_from_subtype(
            conn,
            subtype="open_spec_change",
            field="change_id",
            apply=True,
        )
    assert applied.dry_run is False
    assert applied.inserted_count == 3
    assert applied.conflict_count == 0

    # Idempotent re-run: 0 inserted / 3 conflicts.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        repeat = seed_from_subtype(
            conn,
            subtype="open_spec_change",
            field="change_id",
            apply=True,
        )
    assert repeat.inserted_count == 0
    assert repeat.conflict_count == 3


def test_add_remove_alias_roundtrip(clean_runtime: str) -> None:
    from okg.substrate.catalog.aliases import (
        add_alias,
        list_aliases,
        remove_alias,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_open_spec_changes(conn, n=0)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        added = add_alias(
            conn, alias="foo", canonical="change:bar",
        )
        assert added["inserted"] is True
        # Idempotent re-add.
        again = add_alias(
            conn, alias="foo", canonical="change:bar",
        )
        assert again["inserted"] is False
        # List.
        rows = list_aliases(conn)
        assert any(r["alias"] == "foo" for r in rows)
        # Remove.
        removed = remove_alias(conn, alias="foo")
        assert removed["removed"] is True
        rows2 = list_aliases(conn)
        assert all(r["alias"] != "foo" for r in rows2)


def test_conflicting_add_alias_does_not_wake_retry(clean_runtime: str) -> None:
    from okg.substrate.catalog.aliases import add_alias

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv, _gen = _seed_cv_with_open_spec_changes(conn, n=0)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, canonical, catalog_version_id) VALUES ('foo','node:a',%s)",
                (cv,),
            )
            cur.execute(
                "INSERT INTO okg.link_retry "
                "(deployment_name, branch_name, node_id, entity_type, entity_value) "
                "VALUES ('default','default','page:one','concept','foo')"
            )
        result = add_alias(conn, alias="foo", canonical="node:b")
        assert result["inserted"] is False
        assert result["landed"] is False
        assert result["link_cache_invalidated"] == 0
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ready_at FROM okg.link_retry WHERE entity_value = 'foo'"
            )
            assert cur.fetchone()[0] is None


def test_suggest_aliases_ranks_by_mention_count(
    clean_runtime: str,
) -> None:
    from okg.substrate.catalog.aliases import (
        suggest_aliases_from_unresolved_mentions,
    )

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_open_spec_changes(conn, n=0)
        # Seed mentions: 3 unresolved "add-feature-x", 1
        # unresolved "rare-thing", 1 resolved by canonical
        # `mentions`, and 1 resolved by legacy `references`.
        facts: list[NodeFact | EdgeFact] = []
        for i in range(3):
            facts.append(NodeFact(
                f"mention:af-{i}", "entity_mention",
                {
                    "entity_type": "change_slug",
                    "entity_value": "add-feature-x",
                },
                {}, {},
            ))
        facts.append(NodeFact(
            "mention:rare", "entity_mention",
            {"entity_type": "change_slug", "entity_value": "rare-thing"},
            {}, {},
        ))
        facts.append(NodeFact(
            "change:resolved", "open_spec_change", {}, {}, {},
        ))
        for mention_id, entity_value, edge_type in (
            ("mention:resolved", "10.1/x", "mentions"),
            ("mention:legacy-resolved", "10.2/y", "references"),
        ):
            facts.append(NodeFact(
                mention_id, "entity_mention",
                {"entity_type": "doi", "entity_value": entity_value},
                {}, {},
            ))
            facts.append(EdgeFact(
                mention_id, "change:resolved", edge_type,
            ))
        store = ChronosGraphStore.from_connection(conn)
        try:
            store.apply_facts(facts, source="test", strict_endpoints=True)
        finally:
            store.close()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        suggestions = suggest_aliases_from_unresolved_mentions(
            conn, limit=10,
        )
    values = [s["entity_value"] for s in suggestions]
    # add-feature-x has 3 mentions; rare-thing has 1; resolved
    # mention is excluded.
    assert values[0] == "add-feature-x"
    assert "rare-thing" in values
    assert "10.1/x" not in values
    assert "10.2/y" not in values
    assert suggestions[0]["mention_count"] == 3


# `register_alias` was removed by `shrink-mcp-to-operator-surface`. The
# migration target is a future `propose(action='alias')` or an admin
# CLI; until that lands, alias writes happen through the underlying
# Python helper, not through MCP. The two prior tests that exercised
# the OKG_AGENT gating on `register_alias` are deleted; the gating
# mechanism remains and is covered for other write tools.


def test_mcp_suggest_aliases_returns_unresolved_ranked(
    clean_runtime: str,
) -> None:
    """The MCP read tool surfaces the same ranking as the
    Python API."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_open_spec_changes(conn, n=0)
        store = ChronosGraphStore.from_connection(conn)
        try:
            store.apply_facts(
                [
                    NodeFact(
                        f"mention:s-{i}", "entity_mention",
                        {
                            "entity_type": "change_slug",
                            "entity_value": "popular-slug",
                        },
                        {}, {},
                    )
                    for i in range(2)
                ],
                source="test",
                strict_endpoints=True,
            )
        finally:
            store.close()

    from okg.substrate.mcp.server import build_server

    # `alias_suggestions` is curation-queue state, served only on a
    # write/admin launch (WRITE_ADMIN_INSPECT_KINDS); a read-only server
    # refuses it by name.
    mcp = build_server(
        dsn=clean_runtime,
        deployment_name="test",
        allow_writes=True,
    )

    async def _drive():
        # Migrated from `suggest_aliases(limit=5)` →
        # `inspect(target={kind:'alias_suggestions', limit:5})` per the
        # shrink-mcp-to-operator-surface migration map.
        res = await mcp.call_tool(
            "inspect",
            {"target": {"kind": "alias_suggestions", "limit": 5}},
        )
        return _tool_payload(res)

    payload = asyncio.run(_drive())
    assert payload["limit"] == 5
    assert payload["suggestions"][0]["entity_value"] == "popular-slug"
    assert payload["suggestions"][0]["mention_count"] == 2
