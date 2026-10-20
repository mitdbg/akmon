"""Tests for the declarative cross-source linker."""
from __future__ import annotations

import psycopg
import pytest

from tests.substrate._db_helpers import ensure_published_generation


pytestmark = pytest.mark.postgres


def test_declarative_linker_second_run_emits_zero_edge_facts(
    clean_runtime: str,
) -> None:
    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.linkers.rules import (
        CrossLinkRule,
        RuleEndpoint,
    )

    rule = CrossLinkRule(
        id="same_key",
        src=RuleEndpoint(subtypes=("src_type",), field="key"),
        dst=RuleEndpoint(subtypes=("dst_type",), field="key"),
        matcher="exact",
        edge="references",
        bidirectional=False,
    )
    linker = DeclarativeLinker(rules=[rule])

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        ensure_published_generation(
            conn,
            1,
            deployment_name="declarative-test",
            catalog_version_id=cv,
        )
        for subtype in ("src_type", "dst_type"):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )
        # The linker reads source nodes from the Chronos graph (okg.graph_nodes via
        # _chronos.nodes), so seed them there through the canonical apply_facts write
        # rather than the legacy okg.nodes_live (which the cutover no longer reads).
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact("n:src", "src_type", {"key": "same"}, {"id": "n:src"}, {"v": 1}),
                NodeFact("n:dst", "dst_type", {"key": "same"}, {"id": "n:dst"}, {"v": 1}),
            ],
            source="default", strict_endpoints=True,
        )

        first = linker.enrich(conn, generation_id=2)
        second = linker.enrich(conn, generation_id=3)
        cur.execute(
            "SELECT count(*) FROM okg.edge_facts "
            "WHERE source = '_enricher:declarative:same_key'"
        )
        count = int(cur.fetchone()[0])

    assert first.n_edges_emitted == 1
    assert first.n_edges_skipped == 0
    assert second.n_edges_emitted == 0
    assert second.n_edges_skipped == 1
    assert count == 1


def test_declarative_linker_reuses_endpoint_fetches(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.linkers import declarative as module
    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.linkers.rules import (
        CrossLinkRule,
        RuleEndpoint,
    )

    rules = [
        CrossLinkRule(
            id="same_key_one",
            src=RuleEndpoint(subtypes=("src_type",), field="key"),
            dst=RuleEndpoint(subtypes=("dst_type",), field="key"),
            matcher="exact",
            edge="references",
            bidirectional=False,
        ),
        CrossLinkRule(
            id="same_key_two",
            src=RuleEndpoint(subtypes=("src_type",), field="key"),
            dst=RuleEndpoint(subtypes=("dst_type",), field="key"),
            matcher="exact",
            edge="references",
            bidirectional=False,
        ),
    ]
    linker = DeclarativeLinker(rules=rules)
    calls: list[tuple[str, ...]] = []
    original_query = module._chronos.query

    def counted_query(cur, sql, params=None):
        if "FROM okg.graph_nodes" in sql:
            calls.append(tuple((params or {}).get("subtypes") or ()))
        return original_query(cur, sql, params)

    monkeypatch.setattr(module._chronos, "query", counted_query)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        ensure_published_generation(
            conn,
            1,
            deployment_name="declarative-test",
            catalog_version_id=cv,
        )
        for subtype in ("src_type", "dst_type"):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact("n:src", "src_type", {"key": "same"}, {"id": "n:src"}, {"v": 1}),
                NodeFact("n:dst", "dst_type", {"key": "same"}, {"id": "n:dst"}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )

        result = linker.enrich(conn, generation_id=2)

    assert result.n_edges_emitted == 1
    assert result.n_edges_skipped == 1
    assert calls.count(("src_type",)) == 1
    assert calls.count(("dst_type",)) == 1


def test_declarative_linker_dirty_destination_matches_existing_source(
    clean_runtime: str,
) -> None:
    from okg.substrate.enrichers.base import IncrementalContext
    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.linkers.rules import (
        CrossLinkRule,
        RuleEndpoint,
    )

    rule = CrossLinkRule(
        id="same_key",
        src=RuleEndpoint(subtypes=("src_type",), field="key"),
        dst=RuleEndpoint(subtypes=("dst_type",), field="key"),
        matcher="exact",
        edge="references",
        bidirectional=False,
    )
    linker = DeclarativeLinker(rules=[rule])

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        ensure_published_generation(
            conn,
            1,
            deployment_name="declarative-test",
            catalog_version_id=cv,
        )
        for subtype in ("src_type", "dst_type"):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact("n:src", "src_type", {"key": "same"}, {"id": "n:src"}, {"v": 1}),
                NodeFact("n:dst", "dst_type", {"key": "same"}, {"id": "n:dst"}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )

        result = linker.enrich(
            conn,
            generation_id=2,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset({"n:dst"}),
                is_first_run=False,
                last_status="success",
            ),
        )

    assert result.n_edges_emitted == 1


def test_declarative_linker_dirty_side_is_not_read_when_the_other_side_is_empty(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2941: with no node of the source subtypes, an exact rule has
    no pair, so the dirty destination nodes' fields are not read (on
    okg-workspace this read every dirty code node under the publish lock
    for no output). Once a source node exists, the dirty side is read and
    matched again."""
    from okg.substrate.enrichers.base import IncrementalContext
    from okg.substrate.library.linkers import declarative as module
    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.linkers.rules import (
        CrossLinkRule,
        RuleEndpoint,
    )

    rule = CrossLinkRule(
        id="record_refs_code",
        src=RuleEndpoint(subtypes=("record_type",), field="key"),
        dst=RuleEndpoint(subtypes=("code_type",), field="key"),
        matcher="exact",
        edge="references",
        bidirectional=False,
    )
    linker = DeclarativeLinker(rules=[rule])
    original_query = module._chronos.query
    dirty_reads: list[list[str]] = []

    def counting_query(cur, sql, params=None):
        params = params or {}
        if params.get("subtypes") == ["code_type"] and "node_ids" in params:
            dirty_reads.append(list(params["node_ids"]))
        return original_query(cur, sql, params)

    monkeypatch.setattr(module._chronos, "query", counting_query)

    def enrich(conn: psycopg.Connection, generation_id: int) -> int:
        return linker.enrich(
            conn,
            generation_id=generation_id,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset({"n:code"}),
                is_first_run=False,
                last_status="success",
            ),
        ).n_edges_emitted

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        ensure_published_generation(
            conn,
            1,
            deployment_name="declarative-test",
            catalog_version_id=cv,
        )
        for subtype in ("record_type", "code_type"):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:code", "code_type", {"key": "k"}, {"id": "n:code"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        assert enrich(conn, 2) == 0
        assert dirty_reads == []

        store.apply_facts(
            [NodeFact("n:rec", "record_type", {"key": "k"}, {"id": "n:rec"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        assert enrich(conn, 3) == 1
        assert dirty_reads == [["n:code"]]


def test_declarative_linker_first_run_skips_the_destination_without_sources(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2941: a first run (no dirty scope) matches over the whole
    graph. With no source value it reads no destination; once a source
    exists it reads the destination and emits the pair."""
    from okg.substrate.library.linkers import declarative as module
    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.linkers.rules import (
        CrossLinkRule,
        RuleEndpoint,
    )

    rule = CrossLinkRule(
        id="record_refs_code",
        src=RuleEndpoint(subtypes=("record_type",), field="key"),
        dst=RuleEndpoint(subtypes=("code_type",), field="key"),
        matcher="exact",
        edge="references",
        bidirectional=False,
    )
    original_query = module._chronos.query
    destination_reads: list[str] = []

    def counting_query(cur, sql, params=None):
        params = params or {}
        if params.get("subtypes") == ["code_type"] and "LIMIT 1" not in sql:
            destination_reads.append(sql)
        return original_query(cur, sql, params)

    monkeypatch.setattr(module._chronos, "query", counting_query)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        ensure_published_generation(
            conn,
            1,
            deployment_name="declarative-test",
            catalog_version_id=cv,
        )
        for subtype in ("record_type", "code_type"):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("n:code", "code_type", {"key": "k"}, {"id": "n:code"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        result = DeclarativeLinker(rules=[rule]).enrich(conn, generation_id=2)
        assert result.n_edges_emitted == 0
        assert destination_reads == []

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact("n:rec", "record_type", {"key": "k"}, {"id": "n:rec"}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
        result = DeclarativeLinker(rules=[rule]).enrich(conn, generation_id=3)
    assert result.n_edges_emitted == 1
    assert len(destination_reads) == 1


def test_declarative_linker_dirty_exact_match_uses_bounded_value_lookup(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.enrichers.base import IncrementalContext
    from okg.substrate.library.linkers import declarative as module
    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.linkers.rules import (
        CrossLinkRule,
        RuleEndpoint,
    )

    rule = CrossLinkRule(
        id="tool_use_event_file_path_to_source_file",
        src=RuleEndpoint(subtypes=("tool_use_event",), field="file_path"),
        dst=RuleEndpoint(subtypes=("source_file",), field="path"),
        matcher="exact",
        edge="affects",
        bidirectional=False,
    )
    linker = DeclarativeLinker(rules=[rule])
    original_query = module._chronos.query
    bounded_value_queries = 0

    def guarded_query(cur, sql, params=None):
        nonlocal bounded_value_queries
        params = params or {}
        if (
            params.get("subtypes") == ["source_file"]
            and "node_ids" not in params
            and "FROM okg.graph_nodes" in sql
        ):
            if sql.strip().endswith("LIMIT 1") and "attrs" not in sql:
                pass  # the one-row existence probe (issue #2941), not a scan
            elif any(key.startswith("field_value_") for key in params):
                bounded_value_queries += 1
            else:
                raise AssertionError("unscoped source_file scan")
        return original_query(cur, sql, params)

    monkeypatch.setattr(module._chronos, "query", guarded_query)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        ensure_published_generation(
            conn,
            1,
            deployment_name="declarative-test",
            catalog_version_id=cv,
        )
        for subtype in ("tool_use_event", "source_file"):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )

        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "tool-use:1",
                    "tool_use_event",
                    {"file_path": ".scratch/pr-body.md"},
                    {"id": "tool-use:1"},
                    {"v": 1},
                ),
                NodeFact(
                    "file:repo:.scratch/pr-body.md",
                    "source_file",
                    {"path": ".scratch/pr-body.md", "repo": "repo"},
                    {"id": "file:repo:.scratch/pr-body.md"},
                    {"v": 1},
                ),
                NodeFact(
                    "file:repo:unrelated.md",
                    "source_file",
                    {"path": "unrelated.md", "repo": "repo"},
                    {"id": "file:repo:unrelated.md"},
                    {"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

        result = linker.enrich(
            conn,
            generation_id=2,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset({"tool-use:1"}),
                dirty_node_attr_changes_by_subtype={
                    "tool_use_event": {
                        "tool-use:1": frozenset({"__new__", "file_path"}),
                    },
                },
                is_first_run=False,
                last_status="success",
            ),
        )

    assert result.n_edges_emitted == 1
    assert bounded_value_queries == 1


def test_declarative_linker_dirty_derive_destination_bounds_source_scan(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.enrichers.base import IncrementalContext
    from okg.substrate.library.linkers import declarative as module
    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.linkers.rules import (
        CrossLinkRule,
        DeriveFromPath,
        RuleEndpoint,
    )

    rule = CrossLinkRule(
        id="agent_message_mentions_pact_change_dir",
        src=RuleEndpoint(subtypes=("agent_message",), field="content_excerpt"),
        dst=RuleEndpoint(
            subtypes=("pact_change",),
            field="change_id",
            derive_from_path=DeriveFromPath(
                attr="content_excerpt",
                regex=r".*\bpact/changes/(?:archive/)?([a-zA-Z0-9_-]+)\b",
                capture_field="change_id",
            ),
        ),
        matcher="derive_from_path",
        edge="references",
        bidirectional=False,
    )
    linker = DeclarativeLinker(rules=[rule])
    original_query = module._chronos.query

    def guarded_query(cur, sql, params=None):
        params = params or {}
        if (
            params.get("subtypes") == ["agent_message"]
            and "node_ids" not in params
            and "patterns" not in params
            and "FROM okg.graph_nodes" in sql
        ):
            raise AssertionError("dirty destination caused full agent_message scan")
        return original_query(cur, sql, params)

    monkeypatch.setattr(module._chronos, "query", guarded_query)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        ensure_published_generation(
            conn,
            1,
            deployment_name="declarative-test",
            catalog_version_id=cv,
        )
        for subtype in ("agent_message", "pact_change"):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "agent-message:1",
                    "agent_message",
                    {
                        "content_excerpt": (
                            "See pact/changes/source-sync-publish-path-bottlenecks"
                        ),
                    },
                    {"id": "agent-message:1"},
                    {"v": 1},
                ),
                NodeFact(
                    "agent-message:irrelevant",
                    "agent_message",
                    {"content_excerpt": "unrelated text"},
                    {"id": "agent-message:irrelevant"},
                    {"v": 1},
                ),
                NodeFact(
                    "pact-change:source-sync-publish-path-bottlenecks",
                    "pact_change",
                    {"change_id": "source-sync-publish-path-bottlenecks"},
                    {"id": "pact-change:source-sync-publish-path-bottlenecks"},
                    {"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

        result = linker.enrich(
            conn,
            generation_id=2,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset({
                    "pact-change:source-sync-publish-path-bottlenecks",
                }),
                is_first_run=False,
                last_status="success",
            ),
        )

    assert result.n_edges_emitted == 1


def test_declarative_linker_dirty_derive_destination_skips_unchanged_key(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.enrichers.base import IncrementalContext
    from okg.substrate.library.linkers import declarative as module
    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.linkers.rules import (
        CrossLinkRule,
        DeriveFromPath,
        RuleEndpoint,
    )

    rule = CrossLinkRule(
        id="agent_message_mentions_pact_change_dir",
        src=RuleEndpoint(subtypes=("agent_message",), field="content_excerpt"),
        dst=RuleEndpoint(
            subtypes=("pact_change",),
            field="change_id",
            derive_from_path=DeriveFromPath(
                attr="content_excerpt",
                regex=r".*\bpact/changes/(?:archive/)?([a-zA-Z0-9_-]+)\b",
                capture_field="change_id",
            ),
        ),
        matcher="derive_from_path",
        edge="references",
        bidirectional=False,
    )
    linker = DeclarativeLinker(rules=[rule])
    original_query = module._chronos.query

    def guarded_query(cur, sql, params=None):
        params = params or {}
        if (
            params.get("subtypes") == ["agent_message"]
            and "FROM okg.graph_nodes" in sql
        ):
            raise AssertionError(
                "unchanged destination key scanned agent_message history"
            )
        return original_query(cur, sql, params)

    monkeypatch.setattr(module._chronos, "query", guarded_query)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        ensure_published_generation(
            conn,
            1,
            deployment_name="declarative-test",
            catalog_version_id=cv,
        )
        for subtype in ("agent_message", "pact_change"):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "agent-message:1",
                    "agent_message",
                    {
                        "content_excerpt": (
                            "See pact/changes/source-sync-publish-path-bottlenecks"
                        ),
                    },
                    {"id": "agent-message:1"},
                    {"v": 1},
                ),
                NodeFact(
                    "pact-change:source-sync-publish-path-bottlenecks",
                    "pact_change",
                    {
                        "change_id": "source-sync-publish-path-bottlenecks",
                        "status": "todo",
                    },
                    {"id": "pact-change:source-sync-publish-path-bottlenecks"},
                    {"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

        result = linker.enrich(
            conn,
            generation_id=2,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset({
                    "pact-change:source-sync-publish-path-bottlenecks",
                }),
                is_first_run=False,
                last_status="success",
                dirty_node_attr_changes_by_subtype={
                    "pact_change": {
                        "pact-change:source-sync-publish-path-bottlenecks": (
                            frozenset({"status"})
                        ),
                    },
                },
            ),
        )

    assert result.n_edges_emitted == 0


def test_declarative_linker_dirty_derive_irrelevant_scope_skips_dst_scan(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.enrichers.base import IncrementalContext
    from okg.substrate.library.linkers import declarative as module
    from okg.substrate.library.linkers.declarative import DeclarativeLinker
    from okg.substrate.library.linkers.rules import (
        CrossLinkRule,
        DeriveFromPath,
        RuleEndpoint,
    )

    rule = CrossLinkRule(
        id="agent_message_mentions_pact_change_dir",
        src=RuleEndpoint(subtypes=("agent_message",), field="content_excerpt"),
        dst=RuleEndpoint(
            subtypes=("pact_change",),
            field="change_id",
            derive_from_path=DeriveFromPath(
                attr="content_excerpt",
                regex=r".*\bpact/changes/(?:archive/)?([a-zA-Z0-9_-]+)\b",
                capture_field="change_id",
            ),
        ),
        matcher="derive_from_path",
        edge="references",
        bidirectional=False,
    )
    linker = DeclarativeLinker(rules=[rule])
    original_query = module._chronos.query

    def guarded_query(cur, sql, params=None):
        params = params or {}
        if (
            params.get("subtypes") == ["pact_change"]
            and "FROM okg.graph_nodes" in sql
        ):
            raise AssertionError("irrelevant dirty scope scanned destinations")
        return original_query(cur, sql, params)

    monkeypatch.setattr(module._chronos, "query", guarded_query)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        ensure_published_generation(
            conn,
            1,
            deployment_name="declarative-test",
            catalog_version_id=cv,
        )
        for subtype in ("agent_message", "pact_change", "unrelated_type"):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "unrelated:1",
                    "unrelated_type",
                    {"key": "value"},
                    {"id": "unrelated:1"},
                    {"v": 1},
                ),
                NodeFact(
                    "pact-change:source-sync-publish-path-bottlenecks",
                    "pact_change",
                    {"change_id": "source-sync-publish-path-bottlenecks"},
                    {"id": "pact-change:source-sync-publish-path-bottlenecks"},
                    {"v": 1},
                ),
            ],
            source="default",
            strict_endpoints=True,
        )

        result = linker.enrich(
            conn,
            generation_id=2,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset({"unrelated:1"}),
                is_first_run=False,
                last_status="success",
            ),
        )

    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 0


def test_declarative_linker_dirty_via_source_bounds_via_scan(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.enrichers.base import IncrementalContext
    from okg.substrate.library.linkers import declarative as module
    from okg.substrate.library.linkers.declarative import DeclarativeLinker

    linker = DeclarativeLinker(rules=[_session_tool_pact_rule()])
    _fail_unscoped_tool_use_scan(monkeypatch, module)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_via_rule_graph(conn)
        result = linker.enrich(
            conn,
            generation_id=2,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset({"agent-session:1"}),
                is_first_run=False,
                last_status="success",
            ),
        )

    assert result.n_edges_emitted == 1


def test_declarative_linker_dirty_via_node_bounds_via_scan(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.enrichers.base import IncrementalContext
    from okg.substrate.library.linkers import declarative as module
    from okg.substrate.library.linkers.declarative import DeclarativeLinker

    linker = DeclarativeLinker(rules=[_session_tool_pact_rule()])
    _fail_unscoped_tool_use_scan(monkeypatch, module)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_via_rule_graph(conn)
        result = linker.enrich(
            conn,
            generation_id=2,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset({"tool-use:1"}),
                is_first_run=False,
                last_status="success",
            ),
        )

    assert result.n_edges_emitted == 1


def test_declarative_linker_dirty_via_edge_bounds_via_scan(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.enrichers.base import IncrementalContext
    from okg.substrate.library.linkers import declarative as module
    from okg.substrate.library.linkers.declarative import DeclarativeLinker

    linker = DeclarativeLinker(rules=[_session_tool_pact_rule()])
    _fail_unscoped_tool_use_scan(monkeypatch, module)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        from okg.substrate.library.linkers import _chronos

        _seed_via_rule_graph(conn)
        rows = _chronos.query(
            cur,
            """
            SELECT edge_id
              FROM okg.graph_edges
             WHERE src = 'agent-session:1'
               AND dst = 'tool-use:1'
               AND edge_type = 'contains'
            """
        )
        edge_id = int(rows[0]["edge_id"])
        result = linker.enrich(
            conn,
            generation_id=2,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset(),
                dirty_edge_ids=frozenset({edge_id}),
                is_first_run=False,
                last_status="success",
            ),
        )

    assert result.n_edges_emitted == 1


def test_declarative_linker_dirty_via_destination_bounds_via_scan(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.enrichers.base import IncrementalContext
    from okg.substrate.library.linkers import declarative as module
    from okg.substrate.library.linkers.declarative import DeclarativeLinker

    linker = DeclarativeLinker(rules=[_session_tool_pact_rule()])
    _fail_unscoped_tool_use_scan(monkeypatch, module)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_via_rule_graph(conn)
        result = linker.enrich(
            conn,
            generation_id=2,
            incremental=IncrementalContext(
                prev_fact_watermark=1,
                prev_edge_watermark=1,
                dirty_node_ids=frozenset({
                    "pact-change:source-sync-publish-path-bottlenecks",
                }),
                is_first_run=False,
                last_status="success",
            ),
        )

    assert result.n_edges_emitted == 1


def _session_tool_pact_rule():
    from okg.substrate.library.linkers.rules import (
        CrossLinkRule,
        DeriveFromPath,
        RuleEndpoint,
        ViaEdge,
    )

    return CrossLinkRule(
        id="session_runs_pact_command_for_change_via_tool_use",
        src=RuleEndpoint(
            subtypes=("agent_session",),
            via_edge=ViaEdge(edge_type="contains", dst_subtype="tool_use_event"),
        ),
        dst=RuleEndpoint(
            subtypes=("pact_change",),
            field="change_id",
            derive_from_path=DeriveFromPath(
                attr="command",
                regex=r".*\bokg\s+pact\s+view\s+([a-zA-Z0-9_-]+)\b",
                capture_field="change_id",
            ),
        ),
        matcher="derive_from_path",
        edge="references",
        bidirectional=False,
    )


def _seed_via_rule_graph(conn: psycopg.Connection) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        ensure_published_generation(
            conn,
            1,
            deployment_name="declarative-test",
            catalog_version_id=cv,
        )
        for subtype in ("agent_session", "tool_use_event", "pact_change"):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )

    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(
                "agent-session:1",
                "agent_session",
                {"session_id": "1"},
                {"id": "agent-session:1"},
                {"v": 1},
            ),
            NodeFact(
                "tool-use:1",
                "tool_use_event",
                {"command": "uv run okg pact view source-sync-publish-path-bottlenecks"},
                {"id": "tool-use:1"},
                {"v": 1},
            ),
            NodeFact(
                "tool-use:irrelevant",
                "tool_use_event",
                {"command": "echo nope"},
                {"id": "tool-use:irrelevant"},
                {"v": 1},
            ),
            NodeFact(
                "pact-change:source-sync-publish-path-bottlenecks",
                "pact_change",
                {"change_id": "source-sync-publish-path-bottlenecks"},
                {"id": "pact-change:source-sync-publish-path-bottlenecks"},
                {"v": 1},
            ),
            EdgeFact(
                "agent-session:1",
                "tool-use:1",
                "contains",
                source_record_id={"id": "agent-session:1->tool-use:1"},
                source_revision={"v": 1},
            ),
        ],
        source="default",
        strict_endpoints=True,
    )


def _fail_unscoped_tool_use_scan(
    monkeypatch: pytest.MonkeyPatch,
    module,
) -> None:
    original_query = module._chronos.query

    def guarded_query(cur, sql, params=None):
        params = params or {}
        if (
            params.get("dst_subtype") == "tool_use_event"
            and "node_ids" not in params
            and "FROM okg.graph_nodes" in sql
        ):
            raise AssertionError("unscoped via-node scan for tool_use_event")
        if (
            params.get("subtypes") == ["tool_use_event"]
            and "node_ids" not in params
            and "patterns" not in params
            and not any(key.startswith("field_pattern_") for key in params)
            and "FROM okg.graph_nodes" in sql
        ):
            raise AssertionError("unscoped via-node scan for tool_use_event")
        if (
            params.get("subtypes") == ["pact_change"]
            and "node_ids" not in params
            and not any(key.startswith("field_value_") for key in params)
            and "FROM okg.graph_nodes" in sql
        ):
            raise AssertionError("unscoped dst-node scan for pact_change")
        return original_query(cur, sql, params)

    monkeypatch.setattr(module._chronos, "query", guarded_query)
