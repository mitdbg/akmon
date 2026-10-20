"""M6.7.1 — SourceRunner tests."""
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4

import psycopg
import pytest

import okg.substrate.ingest.runner as runner_mod
from okg.substrate.errors import SubstrateError
from okg.substrate.ingest import SourceRunner, SourceRunContext
from okg.substrate.library.sources.base import NodeFact, SourceRun
from okg.substrate.library.sources.docsite import DocsiteSource
from okg.substrate.library.sources.jira import JiraSource
from okg.substrate.library.sources._fixture import FixtureBackedAdapter
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _write_fixture(tmp_path: Path, doc: dict) -> Path:
    p = tmp_path / "fixture.json"
    p.write_text(json.dumps(doc))
    return p


def _graph_rows(
    dsn: str,
    sql: str,
    params: dict | None = None,
) -> list[dict]:
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        return graph.checkout().query(sql, params or {})
    finally:
        graph.close()


def _generation_rows(
    dsn: str,
    generation_id: str,
    sql: str,
    params: dict | None = None,
) -> list[dict]:
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        return graph.checkout_generation(generation_id).query(sql, params or {})
    finally:
        graph.close()


class _InternalTypeErrorAdapter:
    name = "internal-type-error"
    profile = "discovery_crawl"

    def __init__(self) -> None:
        self.seen_cursors: list[dict | None] = []

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict | None = None,
    ) -> SourceRun:
        del run_id
        self.seen_cursors.append(cursor)
        if cursor is not None:
            raise TypeError("internal cursor handling failed")
        return SourceRun(
            facts=[],
            completed_scope=True,
            next_cursor={"cursor_seen": False},
            run_mode=mode,
        )


class _RunFailingAdapter:
    name = "fail-loud-run"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        del run_id, mode
        raise SubstrateError(
            "fixture source failed during adapter run",
            data={"source": self.name},
        )


class _StreamFailingAdapter:
    name = "fail-loud-stream"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        del run_id

        def _facts():
            raise SubstrateError(
                "fixture source failed while streaming facts",
                data={"source": self.name},
            )
            yield  # pragma: no cover

        return SourceRun(facts=_facts(), completed_scope=True, run_mode=mode)


class _EffectiveModeAdapter:
    name = "effective-mode"
    profile = "append_feed"

    def run(self, run_id: str, *, mode: str = "append") -> SourceRun:
        del run_id, mode
        return SourceRun(
            facts=[],
            completed_scope=True,
            next_cursor={"record_set": {"records": {}}},
            run_mode="scope_complete",
        )


class _NomosSourceAdapter:
    name = "nomos-source"
    profile = "discovery_crawl"

    def __init__(self) -> None:
        self.run_calls = 0

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict | None = None,
    ) -> SourceRun:
        del run_id, cursor
        self.run_calls += 1
        return SourceRun(
            facts=[
                runner_mod.NodeFact(
                    "nomos:source-node",
                    "document",
                    {"text": "should not be written"},
                    {"id": "nomos:source-node"},
                    {"v": 1},
                )
            ],
            completed_scope=True,
            run_mode=mode,
        )


class _NomosLiveSourceAdapter(_NomosSourceAdapter):
    profile = "mutable_api"


class _ReplayPreparationAdapter(_NomosLiveSourceAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.prepare_calls = 0

    def prepare_authoritative_replay(self, *, record_set: object) -> None:
        del record_set
        self.prepare_calls += 1


def test_replay_widening_is_reauthorized_before_adapter_work(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_runner = SourceRunner(
        clean_runtime,
        source_name="replay-authorization",
        deployment_name=None,
    )
    adapter = _ReplayPreparationAdapter()
    scope = {"trigger_kinds": ["poll"]}
    authorizations: list[tuple[str, dict | None]] = []

    def authorize(*, mode: str, sync_scope: dict | None, **kwargs: object) -> None:
        del kwargs
        authorizations.append((mode, sync_scope))
        if len(authorizations) == 2:
            raise SubstrateError("effective replay authority denied")

    monkeypatch.setattr(
        source_runner,
        "_authorize_source_ingest_boundaries",
        authorize,
    )

    with pytest.raises(SubstrateError, match="effective replay authority denied"):
        source_runner.run(
            adapter,
            mode="cursor",
            sync_scope=scope,
            reset_cursor=True,
        )

    assert authorizations == [("cursor", scope), ("reconcile", None)]
    assert adapter.prepare_calls == 0
    assert adapter.run_calls == 0


def test_source_runner_nomos_ingest_denial_blocks_adapter_before_effect(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate_source_policy(clean_runtime, rules=[])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    adapter = _NomosLiveSourceAdapter()

    with pytest.raises(SubstrateError, match="Nomos denied source.ingest"):
        SourceRunner(
            clean_runtime,
            source_name="nomos-source",
            deployment_name="default",
        ).run(adapter, mode="scope_complete")

    assert adapter.run_calls == 0
    assert _nomos_source_decisions(clean_runtime) == [
        ("source.ingest", "deny", "source_ingest"),
    ]


def test_source_runner_nomos_emit_denial_blocks_fact_write_before_effect(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate_source_policy(
        clean_runtime,
        rules=[
            _allow_source_rule("allow_source_ingest", "source.ingest"),
            _allow_source_rule("allow_live_call", "source.live_call"),
        ],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    adapter = _NomosLiveSourceAdapter()

    with pytest.raises(SubstrateError, match="Nomos denied source.emit"):
        SourceRunner(
            clean_runtime,
            source_name="nomos-source",
            deployment_name="default",
        ).run(adapter, mode="scope_complete")

    assert adapter.run_calls == 1
    assert _graph_rows(
        clean_runtime,
        """
        SELECT node_id
          FROM okg.graph_nodes
         WHERE node_id = :node_id
        """,
        {"node_id": "nomos:source-node"},
    ) == []
    assert _nomos_source_decisions(clean_runtime) == [
        ("source.ingest", "allow", "source_ingest"),
        ("source.live_call", "allow", "source_live_call"),
        ("source.emit", "deny", "source_emit"),
    ]


def test_source_runner_nomos_live_call_denial_blocks_adapter_before_effect(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate_source_policy(
        clean_runtime,
        rules=[_allow_source_rule("allow_source_ingest", "source.ingest")],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    adapter = _NomosLiveSourceAdapter()

    with pytest.raises(SubstrateError, match="Nomos denied source.live_call"):
        SourceRunner(
            clean_runtime,
            source_name="nomos-source",
            deployment_name="default",
        ).run(adapter, mode="cursor")

    assert adapter.run_calls == 0
    assert _nomos_source_decisions(clean_runtime) == [
        ("source.ingest", "allow", "source_ingest"),
        ("source.live_call", "deny", "source_live_call"),
    ]


def test_source_runner_nomos_review_live_call_obligation_allows_adapter(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    review_live_call_rule = _allow_source_rule(
        "review_source_live_call",
        "source.live_call",
    )
    review_live_call_rule["reason_code"] = "nomos.test.review_source_live_call"
    review_live_call_rule["obligations"] = [
        {"type": "audit"},
        {"type": "review"},
    ]
    _activate_source_policy(
        clean_runtime,
        rules=[
            _allow_source_rule("allow_source_ingest", "source.ingest"),
            review_live_call_rule,
            _allow_source_rule("allow_source_emit", "source.emit"),
        ],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    adapter = _NomosLiveSourceAdapter()

    SourceRunner(
        clean_runtime,
        source_name="nomos-source",
        deployment_name="default",
    ).run(adapter, mode="cursor")

    assert adapter.run_calls == 1
    assert _nomos_source_decisions(clean_runtime) == [
        ("source.ingest", "allow", "source_ingest"),
        ("source.live_call", "allow", "source_live_call"),
        ("source.emit", "allow", "source_emit"),
    ]


def test_runner_writes_nodes_to_chronos_graph(
    clean_runtime: str, tmp_path: Path,
) -> None:
    fx = _write_fixture(tmp_path, {
        "nodes": [
            {"node_id": "d:1", "subtype": "document",
             "attrs": {"text": "hello"}},
            {"node_id": "d:2", "subtype": "document",
             "attrs": {"text": "world"}},
        ],
        "edges": [],
    })
    adapter = DocsiteSource(fixture_path=str(fx))
    runner = SourceRunner(clean_runtime, source_name="docs")
    result = runner.run(adapter, mode="scope_complete")

    assert result.nodes_appended == 2
    assert result.edges_appended == 0
    assert result.completed_scope is True
    assert result.graph_nodes_by_subtype == {
        "document": ("d:1", "d:2"),
    }
    assert result.graph_node_attr_changes_by_subtype == {
        "document": {
            "d:1": ("__new__", "nomos_source_lineage", "text"),
            "d:2": ("__new__", "nomos_source_lineage", "text"),
        },
    }
    assert result.graph_edges_by_type == {}
    assert result.as_dict()["graph_write_set"] == {
        "schema": "okg.graph_write_set.v1",
        "nodes_by_subtype": {"document": ["d:1", "d:2"]},
        "node_attr_changes_by_subtype": {
            "document": {
                "d:1": ["__new__", "nomos_source_lineage", "text"],
                "d:2": ["__new__", "nomos_source_lineage", "text"],
            },
        },
        "edges_by_type": {},
        "edge_table_available": True,
    }

    rows = _graph_rows(
        clean_runtime,
        """
        SELECT node_id, subtype, source
          FROM okg.graph_nodes
         WHERE source = :source
         ORDER BY node_id
        """,
        {"source": "docs"},
    )
    assert rows == [
        {"node_id": "d:1", "subtype": "document", "source": "docs"},
        {"node_id": "d:2", "subtype": "document", "source": "docs"},
    ]


def test_runner_preserves_requested_and_effective_source_modes(
    clean_runtime: str,
) -> None:
    result = SourceRunner(
        clean_runtime,
        source_name="test.effective-mode",
    ).run(_EffectiveModeAdapter(), mode="append")

    assert result.mode == "append"
    assert result.effective_mode == "scope_complete"
    assert result.as_dict()["effective_mode"] == "scope_complete"


def test_runner_does_not_retry_internal_type_error_without_cursor(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_watermarks
              (source, last_applied_rev)
            VALUES (
              'test.internal-type-error',
              '{"cursor": true}'::jsonb
            )
            """
        )

    adapter = _InternalTypeErrorAdapter()
    with pytest.raises(TypeError, match="internal cursor handling failed"):
        SourceRunner(
            clean_runtime,
            source_name="test.internal-type-error",
            write_fact_log=True,
        ).run(adapter, mode="scope_complete")

    assert adapter.seen_cursors == [{"cursor": True}]


@pytest.mark.parametrize(
    ("source_name", "adapter", "message"),
    [
        (
            "test.fail-loud-run",
            _RunFailingAdapter(),
            "fixture source failed during adapter run",
        ),
        (
            "test.fail-loud-stream",
            _StreamFailingAdapter(),
            "fixture source failed while streaming facts",
        ),
    ],
)
def test_source_runner_propagates_source_failures_without_success_watermark(
    clean_runtime: str,
    source_name: str,
    adapter,
    message: str,
) -> None:
    with pytest.raises(SubstrateError, match=message):
        SourceRunner(
            clean_runtime,
            source_name=source_name,
            write_fact_log=True,
        ).run(adapter, mode="scope_complete")

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (source_name,),
        )
        assert cur.fetchall() == []


def test_runner_accepts_replay_safe_run_context(
    clean_runtime: str, tmp_path: Path,
) -> None:
    fx = _write_fixture(tmp_path, {
        "nodes": [
            {"node_id": "d:ctx", "subtype": "document",
             "attrs": {"text": "stable"}},
        ],
        "edges": [],
    })
    run_id = str(uuid4())
    admission_id = str(uuid4())
    observed_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    context = SourceRunContext(
        run_id=run_id,
        observed_at=observed_at,
        admission_id=admission_id,
        idempotency_key="ctx-key",
    )
    result = SourceRunner(
        clean_runtime,
        source_name="docs_ctx",
        write_fact_log=True,
    ).run(
        DocsiteSource(fixture_path=str(fx)),
        mode="scope_complete",
        run_context=context,
    )

    assert result.run_id == run_id
    assert result.admission_id == admission_id
    assert result.idempotency_key == "ctx-key"
    assert result.as_dict()["admission_id"] == admission_id

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id::text, observed_at
              FROM okg.node_facts
             WHERE source = 'docs_ctx'
            """
        )
        row = cur.fetchone()
    assert row == (run_id, observed_at)


def test_runner_writes_edges_to_chronos_graph(
    clean_runtime: str, tmp_path: Path,
) -> None:
    fx = _write_fixture(tmp_path, {
        "nodes": [
            {"node_id": "t:1", "subtype": "ticket",
             "attrs": {"text": "bug"}},
            {"node_id": "p:alice", "subtype": "person",
             "attrs": {"name": "alice"}},
        ],
        "edges": [
            {"src": "t:1", "dst": "p:alice",
             "edge_type": "reported_by"},
        ],
    })
    adapter = JiraSource(fixture_path=str(fx))
    runner = SourceRunner(clean_runtime, source_name="jira")
    result = runner.run(adapter, mode="cursor")

    assert result.nodes_appended == 2
    assert result.edges_appended == 1
    rows = _graph_rows(
        clean_runtime,
        """
        SELECT src, dst, edge_type, source
          FROM okg.graph_edges
         WHERE source = :source
        """,
        {"source": "jira"},
    )
    assert rows == [{
        "src": "t:1",
        "dst": "p:alice",
        "edge_type": "reported_by",
        "source": "jira",
    }]


def test_runner_advances_source_watermark(
    clean_runtime: str, tmp_path: Path,
) -> None:
    fx = _write_fixture(tmp_path, {"nodes": [], "edges": []})
    adapter = DocsiteSource(fixture_path=str(fx))
    runner = SourceRunner(clean_runtime, source_name="docs_wm")
    runner.run(adapter, mode="scope_complete")

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT last_applied_rev, last_reconcile_at "
            "FROM okg.source_watermarks WHERE source = 'docs_wm'"
        )
        row = cur.fetchone()
    assert row is not None
    # completed_scope=True because DocsiteSource is discovery_crawl
    # and mode=scope_complete → fixture adapter sets the flag.
    _, last_reconcile = row
    assert last_reconcile is not None


def test_runner_dedupes_same_revision(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Two runs with identical (source, node_id, revision) collapse
    at the dedupe_key — COPY would raise if two rows with the same
    key landed. Second run is a no-op INSERT-wise."""
    fx = _write_fixture(tmp_path, {
        "nodes": [{"node_id": "n:a", "subtype": "document",
                   "attrs": {"text": "x"},
                   "source_revision": {"v": 1}}],
        "edges": [],
    })
    adapter = DocsiteSource(fixture_path=str(fx))
    runner = SourceRunner(clean_runtime, source_name="docs_dedupe")
    first = runner.run(adapter, mode="scope_complete")
    # RunResult has no status field (run() raises on failure). The
    # first run's contract is that the single fixture node lands —
    # the precondition that makes `second.nodes_appended == 0` a
    # dedupe proof rather than vacuous.
    assert first.nodes_appended == 1
    assert first.completed_scope is True
    # Per `audit-doc-corpus-chunk-turnover` (2026-04-26):
    # SourceRunner now drops op='I' facts whose
    # `(source, node_id, attrs)` matches the latest persisted
    # row. A second run with identical content writes 0 new
    # node_facts rows. Genuine content changes (different
    # attrs) still pass through.
    second = runner.run(adapter, mode="scope_complete")

    assert second.nodes_appended == 0
    rows = _graph_rows(
        clean_runtime,
        """
        SELECT count(*) AS n
          FROM okg.graph_nodes
         WHERE source = :source AND node_id = :node_id
        """,
        {"source": "docs_dedupe", "node_id": "n:a"},
    )
    assert rows[0]["n"] == 1


def test_runner_dedupes_duplicates_across_flush_chunks(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Streaming flushes must preserve the runner's run-wide
    dedupe contract. A duplicate emitted after a previous chunk
    committed must be dropped before COPY, even when attrs differ
    enough to bypass persisted-attrs dedupe."""
    monkeypatch.setattr(runner_mod, "COPY_CHUNK_ROWS", 1)
    fx = _write_fixture(tmp_path, {
        "nodes": [
            {"node_id": "n:a", "subtype": "document",
             "attrs": {"text": "first", "version": 1},
             "source_record_id": {"id": "a-1"},
             "source_revision": {"v": 1}},
            {"node_id": "n:b", "subtype": "document",
             "attrs": {"text": "middle"},
             "source_record_id": {"id": "b"},
             "source_revision": {"v": 1}},
            {"node_id": "n:a", "subtype": "document",
             "attrs": {"text": "second", "version": 2},
             "source_record_id": {"id": "a-2"},
             "source_revision": {"v": 1}},
        ],
        "edges": [
            {"src": "n:a", "dst": "n:b", "edge_type": "links_to",
             "attrs": {"order": 1},
             "source_record_id": {"id": "e-1"},
             "source_revision": {"v": 1}},
            {"src": "n:b", "dst": "n:a", "edge_type": "links_to",
             "attrs": {"order": 2},
             "source_record_id": {"id": "e-2"},
             "source_revision": {"v": 1}},
            {"src": "n:a", "dst": "n:b", "edge_type": "links_to",
             "attrs": {"order": 3},
             "source_record_id": {"id": "e-3"},
             "source_revision": {"v": 1}},
        ],
    })
    adapter = FixtureBackedAdapter(
        name="fixture",
        profile="discovery_crawl",
        fixture_path=fx,
    )
    result = SourceRunner(
        clean_runtime, source_name="chunked_dedupe",
    ).run(adapter, mode="scope_complete")

    assert result.nodes_appended == 2
    assert result.edges_appended == 2
    assert result.dedupe_drops_node == 1
    assert result.dedupe_drops_edge == 1
    assert result.dedupe_first_dropped_id == "n:a"

    node_counts = {
        row["node_id"]: row["n"]
        for row in _graph_rows(
            clean_runtime,
            """
            SELECT node_id, count(*) AS n
              FROM okg.graph_nodes
             WHERE source = :source
             GROUP BY node_id
            """,
            {"source": "chunked_dedupe"},
        )
    }
    edge_counts = {
        (row["src"], row["dst"], row["edge_type"]): row["n"]
        for row in _graph_rows(
            clean_runtime,
            """
            SELECT src, dst, edge_type, count(*) AS n
              FROM okg.graph_edges
             WHERE source = :source
             GROUP BY src, dst, edge_type
            """,
            {"source": "chunked_dedupe"},
        )
    }
    assert node_counts == {"n:a": 1, "n:b": 1}
    assert edge_counts == {
        ("n:a", "n:b", "links_to"): 1,
        ("n:b", "n:a", "links_to"): 1,
    }


def test_runner_refuses_retractions_for_cursor_mode(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """M3.5 retraction policy: a mutable_api in cursor mode
    cannot emit retractions. The runner consults the guard
    BEFORE writing any fact for the run; policy refusal leaves
    the DB untouched."""
    # The managed-probe contract turns a source's first read into an
    # authoritative reconcile. Establish that baseline first so this test
    # reaches the requested cursor-mode policy boundary on the changed read.
    fx = _write_fixture(tmp_path, {"nodes": [], "edges": []})
    adapter = JiraSource(fixture_path=str(fx))
    runner = SourceRunner(clean_runtime, source_name="jira_refuse")
    runner.run(adapter, mode="cursor")
    fx.write_text(json.dumps({
        "nodes": [
            {"node_id": "t:1", "subtype": "ticket",
             "attrs": {"text": "x"}},
            {"node_id": "t:gone", "subtype": "ticket",
             "attrs": {"text": "y"}, "op": "R"},
        ],
        "edges": [],
    }))
    with pytest.raises(SubstrateError) as exc:
        runner.run(adapter, mode="cursor")
    assert exc.value.code == "retraction_not_permitted"

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.node_facts "
            "WHERE source = 'jira_refuse'"
        )
        assert cur.fetchone()[0] == 0, (
            "refused run must not write any facts"
        )


def test_runner_permits_retractions_in_reconcile_mode(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Same adapter in reconcile mode is authorized to retract."""
    fx = _write_fixture(tmp_path, {
        "nodes": [
            {"node_id": "t:gone", "subtype": "ticket",
             "attrs": {"text": "x"}, "op": "R"},
        ],
        "edges": [],
    })
    adapter = JiraSource(fixture_path=str(fx))
    runner = SourceRunner(clean_runtime, source_name="jira_reconcile")
    result = runner.run(adapter, mode="reconcile")
    assert result.retractions_node == 1

    rows = _graph_rows(
        clean_runtime,
        """
        SELECT count(*) AS n
          FROM okg.graph_nodes
         WHERE source = :source AND node_id = :node_id
        """,
        {"source": "jira_reconcile", "node_id": "t:gone"},
    )
    assert rows[0]["n"] == 0


def test_runner_rejects_unsupported_mode(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """mutable_api does not support scope_complete; the runner
    detects the mismatch when a retraction triggers the guard."""
    # First-run managed identity is replayed in the profile's safe reconcile
    # mode. Seed that identity so the changed read below exercises the
    # explicitly requested (and unsupported) mode.
    fx = _write_fixture(tmp_path, {"nodes": [], "edges": []})
    adapter = JiraSource(fixture_path=str(fx))
    runner = SourceRunner(clean_runtime, source_name="jira_bad_mode")
    runner.run(adapter, mode="cursor")
    fx.write_text(json.dumps({
        "nodes": [
            {"node_id": "t:x", "subtype": "ticket",
             "attrs": {}, "op": "R"},
        ],
        "edges": [],
    }))
    with pytest.raises(SubstrateError) as exc:
        runner.run(adapter, mode="scope_complete")
    # The mapping is wrong for (mutable_api, scope_complete) →
    # runner raises its own error.
    assert "no policy mapping" in exc.value.message.lower()


def test_runner_handles_empty_stream(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """An adapter that yields zero facts is not an error."""
    fx = _write_fixture(tmp_path, {"nodes": [], "edges": []})
    adapter = DocsiteSource(fixture_path=str(fx))
    runner = SourceRunner(clean_runtime, source_name="empty")
    result = runner.run(adapter, mode="scope_complete")
    assert result.nodes_appended == 0
    assert result.edges_appended == 0


def test_runner_emitted_edges_publish_to_generation_checkpoint(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Runner writes graph edits to Chronos; publisher snapshots them."""
    from okg.substrate.publisher import Publisher

    # Seed a catalog_version so Publisher.reserve() finds one.
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.subtypes (name, catalog_version_id) "
            "VALUES ('document', %s)",
            (catalog_version_id,),
        )
        cur.execute(
            """
            INSERT INTO okg.subtype_ancestors
              (catalog_version_id, subtype_name, ancestor_name, depth)
            VALUES (%s, 'document', 'document', 0)
            ON CONFLICT DO NOTHING
            """,
            (catalog_version_id,),
        )
        cur.execute(
            "INSERT INTO okg.narrowings "
            "(parent_subtype, edge_type, child_subtype, catalog_version_id) "
            "VALUES ('document', 'links_to', 'document', %s)",
            (catalog_version_id,),
        )

    fx = _write_fixture(tmp_path, {
        "nodes": [
            {"node_id": "p:1", "subtype": "document",
             "attrs": {"text": "doc 1"}},
            {"node_id": "p:2", "subtype": "document",
             "attrs": {"text": "doc 2"}},
        ],
        "edges": [
            {"src": "p:1", "dst": "p:2", "edge_type": "links_to"},
        ],
    })
    adapter = DocsiteSource(fixture_path=str(fx))
    runner = SourceRunner(clean_runtime, source_name="ds_proj")
    runner.run(adapter, mode="scope_complete")

    rows = _graph_rows(
        clean_runtime,
        """
        SELECT count(*) AS n
          FROM okg.graph_edges
         WHERE src = :src AND dst = :dst AND edge_type = :edge_type
        """,
        {"src": "p:1", "dst": "p:2", "edge_type": "links_to"},
    )
    assert rows[0]["n"] == 1

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published"

    rows = _generation_rows(
        clean_runtime,
        out["generation_id"],
        """
        SELECT src, dst, edge_type
          FROM okg.graph_edges
         WHERE src = :src AND dst = :dst AND edge_type = :edge_type
        """,
        {"src": "p:1", "dst": "p:2", "edge_type": "links_to"},
    )
    assert rows == [{"src": "p:1", "dst": "p:2", "edge_type": "links_to"}]


def test_completed_scope_deletion_publish_keeps_prior_generation_immutable(
    clean_runtime: str,
) -> None:
    from okg.substrate.publisher import Publisher

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.subtypes (name, catalog_version_id) "
            "VALUES ('document', %s)",
            (catalog_version_id,),
        )
        cur.execute(
            """
            INSERT INTO okg.subtype_ancestors
              (catalog_version_id, subtype_name, ancestor_name, depth)
            VALUES (%s, 'document', 'document', 0)
            ON CONFLICT DO NOTHING
            """,
            (catalog_version_id,),
        )

    def _fact(node_id: str) -> NodeFact:
        return NodeFact(
            node_id=node_id,
            subtype="document",
            attrs={"text": node_id},
            source_record_id={"id": node_id},
            source_revision={"rev": "stable"},
        )

    class _CompletedStreamAdapter:
        name = "completed-scope-publish"
        profile = "discovery_crawl"

        def __init__(self) -> None:
            self.runs = [
                [_fact("doc:a"), _fact("doc:b")],
                [_fact("doc:a")],
            ]

        def run(self, run_id: str, *, mode: str = "cursor", **kwargs) -> SourceRun:
            del run_id, kwargs
            return SourceRun(
                facts=self.runs.pop(0),
                completed_scope=True,
                next_cursor={},
                run_mode=mode,
            )

    source = "test.completed-scope-deletion-publish"
    adapter = _CompletedStreamAdapter()
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )

    runner.run(adapter, mode="scope_complete")
    first_publish = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert first_publish["status"] == "published"

    second_run = runner.run(
        adapter,
        mode="reconcile",
        reset_cursor=True,
    )
    second_publish = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert second_publish["status"] == "published"
    assert second_run.retractions_node == 1
    assert second_publish["generation_id"] != first_publish["generation_id"]

    query = """
        SELECT node_id
          FROM okg.graph_nodes
         WHERE source = :source
         ORDER BY node_id
    """
    first_nodes = _generation_rows(
        clean_runtime,
        first_publish["generation_id"],
        query,
        {"source": source},
    )
    second_nodes = _generation_rows(
        clean_runtime,
        second_publish["generation_id"],
        query,
        {"source": source},
    )
    assert [row["node_id"] for row in first_nodes] == ["doc:a", "doc:b"]
    assert [row["node_id"] for row in second_nodes] == ["doc:a"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT op
              FROM okg.node_facts
             WHERE source = %s AND node_id = 'doc:b'
             ORDER BY fact_id DESC
             LIMIT 1
            """,
            (source,),
        )
        assert cur.fetchone() == ("R",)


def _activate_source_policy(
    dsn: str,
    *,
    rules: list[dict[str, object]],
) -> None:
    from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph

    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:source-runner",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules or [_unmatched_source_rule()],
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bundle,
            scope="deployment:default",
            activated_by="test:operator",
        )
        conn.commit()


def _allow_source_rule(rule_id: str, action: str) -> dict[str, object]:
    return {
        "rule_id": rule_id,
        "effect": "allow",
        "actions": [action],
        "subjects": ["source:nomos-source"],
        "match": {
            "deployment": "default",
            "scope": "deployment:default",
        },
        "priority": 100,
        "reason_code": f"nomos.test.{rule_id}",
        "obligations": [{"type": "audit"}],
    }


def _unmatched_source_rule() -> dict[str, object]:
    return {
        "rule_id": "allow_other_source",
        "effect": "allow",
        "actions": ["source.ingest"],
        "subjects": ["source:someone-else"],
        "priority": 1,
        "reason_code": "nomos.test.unmatched_source",
    }


def _nomos_source_decisions(dsn: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT action, effect, target->>'kind'
              FROM okg.nomos_decisions
             WHERE deployment_name = 'default'
               AND action LIKE 'source.%'
             ORDER BY created_at, decision_row_id
            """
        )
        return [(str(a), str(e), str(k)) for a, e, k in cur.fetchall()]
