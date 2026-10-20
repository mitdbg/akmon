from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_interface_handler_linker_publish_connects_entrypoints_to_symbols(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.linkers.interface_handler import (
        InterfaceHandlerLinker,
    )
    from okg.substrate.library.sources.base import NodeFact, SourceRun
    from okg.substrate.publisher import Publisher

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-interface-handler",
        "description": "interface endpoint to handler linker fixture",
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

    cli_endpoint_id = "endpoint:demo:cli:doctor"
    mcp_endpoint_id = "endpoint:demo:mcp:describe"
    http_endpoint_id = "endpoint:demo:http:health"
    cli_symbol_id = "symbol:demo:src/cli.py:_cmd_doctor:10"
    mcp_symbol_id = "symbol:demo:src/server.py:describe_graph:20"
    http_symbol_id = "symbol:demo:src/api.py:health:30"

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
                for symbol_id, path, name, line in (
                    (cli_symbol_id, "src/cli.py", "_cmd_doctor", 10),
                    (mcp_symbol_id, "src/server.py", "describe_graph", 20),
                    (http_symbol_id, "src/api.py", "health", 30),
                ):
                    yield NodeFact(
                        node_id=symbol_id,
                        subtype="code_symbol",
                        attrs={
                            "repo": "demo",
                            "path": path,
                            "kind": "function",
                            "name": name,
                            "qualified_name": name,
                            "start_line": line,
                            "end_line": line + 5,
                        },
                        source_record_id={"node_id": symbol_id},
                        source_revision={"rev": 1},
                    )
                yield NodeFact(
                    node_id=cli_endpoint_id,
                    subtype="interface_endpoint",
                    attrs={
                        "endpoint_id": cli_endpoint_id,
                        "repo": "demo",
                        "kind": "cli_command",
                        "name": "doctor",
                        "path": "src/cli.py",
                        "start_line": 100,
                    },
                    source_record_id={"node_id": cli_endpoint_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=mcp_endpoint_id,
                    subtype="interface_endpoint",
                    attrs={
                        "endpoint_id": mcp_endpoint_id,
                        "repo": "demo",
                        "kind": "mcp_tool",
                        "name": "describe_graph",
                        "path": "src/server.py",
                        "handler": "describe_graph",
                        "start_line": 20,
                    },
                    source_record_id={"node_id": mcp_endpoint_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=http_endpoint_id,
                    subtype="interface_endpoint",
                    attrs={
                        "endpoint_id": http_endpoint_id,
                        "repo": "demo",
                        "kind": "http_route",
                        "name": "GET /health",
                        "path": "src/api.py",
                        "handler_symbol_id": http_symbol_id,
                        "start_line": 30,
                    },
                    source_record_id={"node_id": http_endpoint_id},
                    source_revision={"rev": 1},
                )

            return SourceRun(facts=facts(), completed_scope=True)

    SourceRunner(clean_runtime, source_name="fixture.interface-handler").run(
        FixtureSource(),
        mode="scope_complete",
    )

    out = Publisher(
        clean_runtime,
        "test-interface-handler",
        enrichers=[InterfaceHandlerLinker()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-interface-handler"),
    ).cycle()
    assert out["status"] == "published", out

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = [
            (row["src"], row["edge_type"], row["dst"], row["attrs"])
            for row in checkout.query(
                """
                SELECT e.src, e.edge_type, e.dst, e.attrs
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes p
                    ON p.node_id = e.src
                   AND p.subtype = 'interface_endpoint'
                  JOIN okg.graph_nodes c
                    ON c.node_id = e.dst
                   AND c.subtype = 'code_symbol'
                 WHERE e.edge_type = 'routes_to'
                 ORDER BY e.src
                """
            )
        ]
    finally:
        store.close()
    with psycopg.connect(clean_runtime) as conn:
        diagnostics = InterfaceHandlerLinker().diagnostics(conn)

    triples = {(src, edge_type, dst) for src, edge_type, dst, _attrs in rows}
    assert (cli_endpoint_id, "routes_to", cli_symbol_id) in triples
    assert (mcp_endpoint_id, "routes_to", mcp_symbol_id) in triples
    assert (http_endpoint_id, "routes_to", http_symbol_id) in triples
    assert diagnostics.eligible_endpoints == 3
    assert diagnostics.matched_endpoints == 3
    assert diagnostics.emitted_edges == 3
    assert diagnostics.unmatched_endpoint_count == 0
