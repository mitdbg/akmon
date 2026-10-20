from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_test_symbol_linker_publish_connects_failure_stack_to_symbol(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.linkers.test_symbol import TestSymbolLinker
    from okg.substrate.library.sources.base import EdgeFact, NodeFact, SourceRun
    from okg.substrate.publisher import Publisher

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-test-symbol",
        "description": "test evidence to symbol linker fixture",
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

    test_case_id = "test-case:fixture:test_fail"
    runtime_event_id = "runtime-event:fixture:test_fail"
    stack_frame_id = "stack-frame:fixture:test_fail:0"
    symbol_id = "symbol:demo:src/app.py:handle_request:30"
    test_symbol_id = "symbol:demo:tests/test_app.py:test_fail:10"

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

            def facts() -> Iterable[NodeFact | EdgeFact]:
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
                        "start_line": 30,
                        "end_line": 60,
                    },
                    source_record_id={"node_id": symbol_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id="file:demo:tests/test_app.py",
                    subtype="source_file",
                    attrs={"repo": "demo", "path": "tests/test_app.py"},
                    source_record_id={"node_id": "file:demo:tests/test_app.py"},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=test_symbol_id,
                    subtype="code_symbol",
                    attrs={
                        "repo": "demo",
                        "path": "tests/test_app.py",
                        "kind": "function",
                        "name": "test_fail",
                        "qualified_name": "tests.test_app.test_fail",
                        "start_line": 10,
                        "end_line": 20,
                    },
                    source_record_id={"node_id": test_symbol_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=test_case_id,
                    subtype="test_case",
                    attrs={
                        "classname": "tests.test_app",
                        "name": "test_fail",
                        "status": "failed",
                    },
                    source_record_id={"node_id": test_case_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=runtime_event_id,
                    subtype="runtime_event",
                    attrs={
                        "event_id": runtime_event_id,
                        "repo": "demo",
                        "kind": "test_failure",
                        "status": "failed",
                        "message": "assertion failed",
                    },
                    source_record_id={"node_id": runtime_event_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=stack_frame_id,
                    subtype="stack_frame",
                    attrs={
                        "frame_id": stack_frame_id,
                        "repo": "demo",
                        "path": "src/app.py",
                        "line": 42,
                        "function": "handle_request",
                    },
                    source_record_id={"node_id": stack_frame_id},
                    source_revision={"rev": 1},
                )
                yield EdgeFact(
                    src=runtime_event_id,
                    dst=test_case_id,
                    edge_type="references",
                    source_record_id={"edge": "event-case"},
                    source_revision={"rev": 1},
                )
                yield EdgeFact(
                    src=runtime_event_id,
                    dst=stack_frame_id,
                    edge_type="contains",
                    attrs={"index": 0},
                    source_record_id={"edge": "event-frame"},
                    source_revision={"rev": 1},
                )
                yield EdgeFact(
                    src=stack_frame_id,
                    dst="file:demo:src/app.py",
                    edge_type="points_to",
                    attrs={"line": 42},
                    source_record_id={"edge": "frame-file"},
                    source_revision={"rev": 1},
                )
                yield EdgeFact(
                    src=test_symbol_id,
                    dst=symbol_id,
                    edge_type="calls",
                    attrs={},
                    source_record_id={"edge": "test-calls-symbol"},
                    source_revision={"rev": 1},
                )

            return SourceRun(facts=facts(), completed_scope=True)

    SourceRunner(clean_runtime, source_name="fixture.test-symbol").run(
        FixtureSource(),
        mode="scope_complete",
    )

    out = Publisher(
        clean_runtime,
        "test-test-symbol",
        enrichers=[TestSymbolLinker()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-test-symbol"),
    ).cycle()
    assert out["status"] == "published", out

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = [
            (
                row["src_subtype"],
                row["src"],
                row["edge_type"],
                row["dst_subtype"],
                row["dst"],
            )
            for row in checkout.query(
                """
                SELECT p.subtype AS src_subtype,
                       e.src,
                       e.edge_type,
                       c.subtype AS dst_subtype,
                       e.dst
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes p ON p.node_id = e.src
                  JOIN okg.graph_nodes c ON c.node_id = e.dst
                 WHERE e.src = ANY(:srcs)
                   AND e.dst = :dst
                 ORDER BY 1, 2, 3, 4, 5
                """,
                {
                    "srcs": [test_case_id, runtime_event_id, stack_frame_id],
                    "dst": symbol_id,
                },
            )
        ]
    finally:
        store.close()
    with psycopg.connect(clean_runtime) as conn:
        diagnostics = TestSymbolLinker().diagnostics(conn)

    assert (
        "runtime_event",
        runtime_event_id,
        "references",
        "code_symbol",
        symbol_id,
    ) in rows
    assert (
        "stack_frame",
        stack_frame_id,
        "points_to",
        "code_symbol",
        symbol_id,
    ) in rows
    assert (
        "test_case",
        test_case_id,
        "references",
        "code_symbol",
        symbol_id,
    ) in rows
    assert (
        "test_case",
        test_case_id,
        "exercises",
        "code_symbol",
        symbol_id,
    ) in rows
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        test_symbol_rows = [
            (
                row["src_subtype"],
                row["src"],
                row["edge_type"],
                row["dst_subtype"],
                row["dst"],
            )
            for row in checkout.query(
                """
                SELECT p.subtype AS src_subtype,
                       e.src,
                       e.edge_type,
                       c.subtype AS dst_subtype,
                       e.dst
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes p ON p.node_id = e.src
                  JOIN okg.graph_nodes c ON c.node_id = e.dst
                 WHERE e.src = :src
                   AND e.dst = :dst
                """,
                {"src": test_case_id, "dst": test_symbol_id},
            )
        ]
    finally:
        store.close()
    assert (
        "test_case",
        test_case_id,
        "references",
        "code_symbol",
        test_symbol_id,
    ) in test_symbol_rows
    assert diagnostics.eligible_test_cases == 1
    assert diagnostics.matched_test_cases == 1
    assert diagnostics.eligible_stack_frames == 1
    assert diagnostics.matched_stack_frames == 1
    assert diagnostics.emitted_edges == 5
