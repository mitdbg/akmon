from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_data_asset_linker_publish_connects_symbols_to_touchpoints(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.linkers.data_asset import DataAssetLinker
    from okg.substrate.library.sources.base import NodeFact, SourceRun
    from okg.substrate.publisher import Publisher

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-data-asset",
        "description": "data asset linker fixture",
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

    module_symbol_id = "symbol:demo:src/app.py:module:1"
    function_symbol_id = "symbol:demo:src/app.py:load:20"
    read_asset_id = "asset:demo:file_path:input"
    write_asset_id = "asset:demo:sql_relation:audit_events"
    ref_asset_id = "asset:demo:external_api_host:api"
    config_id = "config:demo:env:OKG_DSN"

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
                    node_id=module_symbol_id,
                    subtype="code_symbol",
                    attrs={
                        "repo": "demo",
                        "path": "src/app.py",
                        "kind": "module",
                        "name": "src.app",
                        "qualified_name": "src.app",
                        "start_line": 1,
                        "end_line": 100,
                    },
                    source_record_id={"node_id": module_symbol_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=function_symbol_id,
                    subtype="code_symbol",
                    attrs={
                        "repo": "demo",
                        "path": "src/app.py",
                        "kind": "function",
                        "name": "load",
                        "qualified_name": "load",
                        "start_line": 20,
                        "end_line": 40,
                    },
                    source_record_id={"node_id": function_symbol_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=read_asset_id,
                    subtype="data_asset",
                    attrs={
                        "asset_id": read_asset_id,
                        "repo": "demo",
                        "kind": "file_path",
                        "name": "data/input.json",
                        "access_kind": "read",
                        "path": "src/app.py",
                        "line": 24,
                        "source_symbol_id": function_symbol_id,
                    },
                    source_record_id={"node_id": read_asset_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=write_asset_id,
                    subtype="data_asset",
                    attrs={
                        "asset_id": write_asset_id,
                        "repo": "demo",
                        "kind": "sql_relation",
                        "name": "audit.events",
                        "access_kind": "write",
                        "path": "src/app.py",
                        "line": 28,
                    },
                    source_record_id={"node_id": write_asset_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=ref_asset_id,
                    subtype="data_asset",
                    attrs={
                        "asset_id": ref_asset_id,
                        "repo": "demo",
                        "kind": "external_api_host",
                        "name": "api.example.test",
                        "access_kind": "reference",
                        "path": "src/app.py",
                        "line": 5,
                    },
                    source_record_id={"node_id": ref_asset_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=config_id,
                    subtype="config_key",
                    attrs={
                        "config_id": config_id,
                        "repo": "demo",
                        "key": "OKG_DSN",
                        "source_kind": "env",
                        "required": False,
                        "path": "src/app.py",
                        "line": 30,
                    },
                    source_record_id={"node_id": config_id},
                    source_revision={"rev": 1},
                )

            return SourceRun(facts=facts(), completed_scope=True)

    SourceRunner(clean_runtime, source_name="fixture.data-asset").run(
        FixtureSource(),
        mode="scope_complete",
    )

    out = Publisher(
        clean_runtime,
        "test-data-asset",
        enrichers=[DataAssetLinker()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-data-asset"),
    ).cycle()
    assert out["status"] == "published", out

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = [
            (row["src"], row["edge_type"], row["dst_subtype"], row["dst"])
            for row in checkout.query(
                """
                SELECT e.src, e.edge_type, d.subtype AS dst_subtype, e.dst
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes s
                    ON s.node_id = e.src
                   AND s.subtype = 'code_symbol'
                  JOIN okg.graph_nodes d
                    ON d.node_id = e.dst
                   AND d.subtype IN ('data_asset', 'config_key')
                 ORDER BY e.src, e.edge_type, d.subtype, e.dst
                """
            )
        ]
    finally:
        store.close()
    with psycopg.connect(clean_runtime) as conn:
        diagnostics = DataAssetLinker().diagnostics(conn)

    assert (function_symbol_id, "reads", "data_asset", read_asset_id) in rows
    assert (function_symbol_id, "writes", "data_asset", write_asset_id) in rows
    assert (module_symbol_id, "references", "data_asset", ref_asset_id) in rows
    assert (function_symbol_id, "references", "config_key", config_id) in rows
    assert diagnostics.eligible_touchpoints == 4
    assert diagnostics.matched_touchpoints == 4
    assert diagnostics.emitted_edges == 4
    assert diagnostics.unmatched_touchpoint_count == 0
