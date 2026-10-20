from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_tool_use_symbol_linker_publish_connects_enclosing_symbol(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.linkers.tool_use_symbol import (
        ToolUseSymbolLinker,
    )
    from okg.substrate.library.sources.base import NodeFact, SourceRun
    from okg.substrate.publisher import Publisher

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-tool-use-symbol",
        "description": "tool use to symbol linker fixture",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "ci",
            "code_graph",
        ],
    }))
    (dep / "schemas").mkdir()

    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    event_id = "tool_use:fixture:1"
    semantic_event_id = "tool_use:fixture:semantic"
    symbol_id = "symbol:demo:src/app.py:handle_request:30"
    semantic_key = "code-symbol:demo:python:src.app.handle_request:function"

    class FixtureSource:
        name = "fixture"
        profile = "discovery_crawl"

        def run(
            self,
            run_id: str,
            *,
            mode: str,
            cursor: dict[str, Any] | None = None,
        ) -> SourceRun:
            del run_id, mode, cursor

            def facts() -> Iterable[NodeFact]:
                yield NodeFact(
                    node_id="file:demo:src/app.py",
                    subtype="source_file",
                    attrs={"repo": "demo", "path": "src/app.py"},
                    source_record_id={"node_id": "file:demo:src/app.py"},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=symbol_id,
                    subtype="code_symbol",
                    attrs={
                        "repo": "demo",
                        "path": "src/app.py",
                        "kind": "function",
                        "name": "handle_request",
                        "qualified_name": "app.handle_request",
                        "canonical_name": "src.app.handle_request",
                        "semantic_key": semantic_key,
                        "start_line": 30,
                        "end_line": 60,
                    },
                    source_record_id={"node_id": symbol_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=event_id,
                    subtype="tool_use_event",
                    attrs={
                        "event_id": event_id,
                        "tool_name": "exec_command",
                        "file_path": "src/app.py",
                        "command": "sed -n '42,42p' src/app.py",
                    },
                    source_record_id={"node_id": event_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=semantic_event_id,
                    subtype="tool_use_event",
                    attrs={
                        "event_id": semantic_event_id,
                        "tool_name": "exec_command",
                        "symbol_semantic_key": semantic_key,
                    },
                    source_record_id={"node_id": semantic_event_id},
                    source_revision={"rev": 1},
                )

            return SourceRun(facts=facts(), completed_scope=True)

    SourceRunner(clean_runtime, source_name="fixture.tool-symbol").run(
        FixtureSource(),
        mode="scope_complete",
    )

    out = Publisher(
        clean_runtime,
        "test-tool-use-symbol",
        enrichers=[ToolUseSymbolLinker()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-tool-use-symbol"),
    ).cycle()
    assert out["status"] == "published", out

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = [
            (row["src"], row["dst"], row["edge_type"], row["attrs"])
            for row in checkout.query(
                """
                SELECT e.src, e.dst, e.edge_type, e.attrs
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes p
                    ON p.node_id = e.src
                   AND p.subtype = 'tool_use_event'
                  JOIN okg.graph_nodes c
                    ON c.node_id = e.dst
                   AND c.subtype = 'code_symbol'
                 WHERE e.edge_type = 'affects'
                """
            )
        ]
    finally:
        store.close()
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        diagnostics = ToolUseSymbolLinker().diagnostics(conn)
        cur.execute(
            "SELECT count(*) FROM okg.edge_facts "
            "WHERE source = '_enricher:tool_use_symbol'"
        )
        before = int(cur.fetchone()[0])
        second = ToolUseSymbolLinker().enrich(conn, generation_id=999)
        cur.execute(
            "SELECT count(*) FROM okg.edge_facts "
            "WHERE source = '_enricher:tool_use_symbol'"
        )
        after = int(cur.fetchone()[0])

    assert len(rows) == 2
    rows_by_src = {src: (dst, edge_type, attrs) for src, dst, edge_type, attrs in rows}
    dst, edge_type, attrs = rows_by_src[event_id]
    assert (dst, edge_type) == (symbol_id, "affects")
    assert attrs["line_start"] == 42
    assert attrs["line_end"] == 42
    assert attrs["line_match_method"] == "sed_range"
    dst, edge_type, attrs = rows_by_src[semantic_event_id]
    assert (dst, edge_type) == (symbol_id, "affects")
    assert attrs["line_start"] == 0
    assert attrs["line_end"] == 0
    assert attrs["line_match_method"] == "semantic_identity"
    assert attrs["resolution_method"] == "semantic_identity"
    assert second.n_edges_emitted == 0
    assert second.n_edges_skipped == 2
    assert after == before
    assert diagnostics.eligible_events == 2
    assert diagnostics.matched_events == 2
    assert diagnostics.emitted_edges == 2
    assert diagnostics.unmatched_event_count == 0


def test_tool_use_symbol_linker_diagnostics_reports_unmatched(
    clean_runtime: str,
) -> None:
    from okg.substrate.library.linkers.tool_use_symbol import (
        ToolUseSymbolLinker,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "tool_use:fixture:unmatched",
                    "tool_use_event",
                    {"file_path": "src/missing.py", "line": 10},
                    {},
                    {},
                )
            ],
            source="fixture.tool-use",
        )
        diagnostics = ToolUseSymbolLinker().diagnostics(conn)

    assert diagnostics.eligible_events == 1
    assert diagnostics.matched_events == 0
    assert diagnostics.emitted_edges == 0
    assert diagnostics.unmatched_event_count == 1
    assert diagnostics.unmatched_events[0]["reason"] == "no_symbol_span_match"
