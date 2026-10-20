from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_linkers_prefer_semantic_identity_for_moved_symbols(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.linkers.coverage_symbol import CoverageSymbolLinker
    from okg.substrate.library.linkers.data_asset import DataAssetLinker
    from okg.substrate.library.linkers.data_flow_evidence import (
        DataFlowEvidenceLinker,
    )
    from okg.substrate.library.linkers.interface_handler import (
        InterfaceHandlerLinker,
    )
    from okg.substrate.library.linkers.profile_hotspot import ProfileHotspotLinker
    from okg.substrate.library.linkers.security_finding import SecurityFindingLinker
    from okg.substrate.library.linkers.test_symbol import TestSymbolLinker
    from okg.substrate.library.linkers.tool_use_symbol import ToolUseSymbolLinker
    from okg.substrate.library.sources.base import EdgeFact, NodeFact, SourceRun
    from okg.substrate.publisher import Publisher

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-semantic-symbol-resolution",
        "description": "semantic symbol linker fixture",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "ci",
            "code_graph",
            "quality_evidence",
        ],
    }))
    (dep / "schemas").mkdir()

    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    semantic_key = "code-symbol:demo:python:src.app.handle_request:function"
    symbol_id = "symbol:demo:src/app.py:handle_request:90"
    file_id = "file:demo:src/app.py"
    tool_id = "tool-use:fixture:moved"
    stack_frame_id = "stack-frame:fixture:moved:0"
    runtime_event_id = "runtime-event:fixture:moved"
    test_case_id = "test-case:fixture:test_moved"
    data_asset_id = "asset:demo:sql_relation:orders"
    data_flow_id = "flow:semgrep:moved"
    security_id = "security:finding:moved"
    hotspot_id = "profile-hotspot:moved"
    endpoint_id = "endpoint:demo:http:moved"
    coverage_id = "coverage-report:demo:moved"

    stale_line = 42
    moved_start_line = 90

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
                    node_id=file_id,
                    subtype="source_file",
                    attrs={"repo": "demo", "path": "src/app.py"},
                    source_record_id={"node_id": file_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=symbol_id,
                    subtype="code_symbol",
                    attrs={
                        "repo": "demo",
                        "path": "src/app.py",
                        "language": "python",
                        "kind": "function",
                        "symbol_kind": "function",
                        "name": "handle_request",
                        "qualified_name": "handle_request",
                        "canonical_name": "src.app.handle_request",
                        "semantic_key": semantic_key,
                        "semantic_key_version": "okg.code_symbol.semantic_key.v1",
                        "module_name": "src.app",
                        "start_line": moved_start_line,
                        "end_line": moved_start_line + 20,
                    },
                    source_record_id={"node_id": symbol_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=tool_id,
                    subtype="tool_use_event",
                    attrs={
                        "tool_name": "exec_command",
                        "file_path": "src/app.py",
                        "line": stale_line,
                        "symbol_semantic_key": semantic_key,
                    },
                    source_record_id={"node_id": tool_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=stack_frame_id,
                    subtype="stack_frame",
                    attrs={
                        "path": "src/app.py",
                        "line": stale_line,
                        "function": "handle_request",
                        "symbol_semantic_key": semantic_key,
                    },
                    source_record_id={"node_id": stack_frame_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=runtime_event_id,
                    subtype="runtime_event",
                    attrs={"kind": "test_failure", "status": "failed"},
                    source_record_id={"node_id": runtime_event_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=test_case_id,
                    subtype="test_case",
                    attrs={"classname": "tests.test_app", "name": "test_moved"},
                    source_record_id={"node_id": test_case_id},
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
                    source_record_id={"edge": "event-frame"},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=data_asset_id,
                    subtype="data_asset",
                    attrs={
                        "asset_id": data_asset_id,
                        "kind": "sql_relation",
                        "name": "orders",
                        "path": "src/app.py",
                        "line": stale_line,
                        "access_kind": "read",
                        "source_semantic_key": semantic_key,
                    },
                    source_record_id={"node_id": data_asset_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=data_flow_id,
                    subtype="data_flow_observation",
                    attrs={
                        "tool": "semgrep",
                        "rule_id": "demo.moved",
                        "path": "src/app.py",
                        "start_line": stale_line,
                        "asset_hint": "orders",
                        "symbol_semantic_key": semantic_key,
                    },
                    source_record_id={"node_id": data_flow_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=security_id,
                    subtype="security_finding",
                    attrs={
                        "tool": "semgrep",
                        "rule_id": "demo.security",
                        "path": "src/app.py",
                        "start_line": stale_line,
                        "symbol_semantic_key": semantic_key,
                    },
                    source_record_id={"node_id": security_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=hotspot_id,
                    subtype="profile_hotspot",
                    attrs={
                        "profile_run_id": "profile-run:moved",
                        "name": "handle_request",
                        "path": "src/app.py",
                        "line": stale_line,
                        "symbol_semantic_key": semantic_key,
                    },
                    source_record_id={"node_id": hotspot_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=endpoint_id,
                    subtype="interface_endpoint",
                    attrs={
                        "kind": "http_route",
                        "name": "GET /moved",
                        "path": "src/app.py",
                        "start_line": stale_line,
                        "handler_semantic_key": semantic_key,
                    },
                    source_record_id={"node_id": endpoint_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=coverage_id,
                    subtype="coverage_report",
                    attrs={"covered_symbols": {semantic_key: 3}},
                    source_record_id={"node_id": coverage_id},
                    source_revision={"rev": 1},
                )

            return SourceRun(facts=facts(), completed_scope=True)

    SourceRunner(clean_runtime, source_name="fixture.semantic-symbol").run(
        FixtureSource(),
        mode="scope_complete",
    )

    out = Publisher(
        clean_runtime,
        "test-semantic-symbol-resolution",
        enrichers=[
            ToolUseSymbolLinker(),
            TestSymbolLinker(),
            DataAssetLinker(),
            DataFlowEvidenceLinker(),
            SecurityFindingLinker(),
            ProfileHotspotLinker(),
            InterfaceHandlerLinker(),
            CoverageSymbolLinker(),
        ],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-semantic-symbol-resolution"),
    ).cycle()
    assert out["status"] == "published", out

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = {
            (row["src"], row["edge_type"], row["dst"]): row["attrs"]
            for row in checkout.query(
                """
                SELECT src, edge_type, dst, attrs
                  FROM okg.graph_edges
                 WHERE dst = :symbol_id
                    OR src = :symbol_id
                 ORDER BY src, edge_type, dst
                """,
                {"symbol_id": symbol_id},
            )
        }
    finally:
        store.close()

    expected_edges = [
        (tool_id, "affects", symbol_id),
        (stack_frame_id, "points_to", symbol_id),
        (runtime_event_id, "references", symbol_id),
        (test_case_id, "references", symbol_id),
        (symbol_id, "reads", data_asset_id),
        (data_flow_id, "references", symbol_id),
        (security_id, "affects", symbol_id),
        (hotspot_id, "affects", symbol_id),
        (endpoint_id, "routes_to", symbol_id),
        (coverage_id, "references", symbol_id),
    ]
    for edge_key in expected_edges:
        assert edge_key in rows
        assert rows[edge_key]["resolution_method"] == "semantic_identity"
        assert (
            rows[edge_key].get("semantic_match_value") == semantic_key
            or edge_key == (runtime_event_id, "references", symbol_id)
            or edge_key == (test_case_id, "references", symbol_id)
        )

    assert stale_line < moved_start_line
