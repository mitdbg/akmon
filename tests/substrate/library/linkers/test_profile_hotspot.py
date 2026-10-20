from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_profile_hotspot_linker_publish_connects_profile_evidence_targets(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.linkers.profile_hotspot import ProfileHotspotLinker
    from okg.substrate.library.sources.base import NodeFact, SourceRun
    from okg.substrate.publisher import Publisher

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-profile-hotspot",
        "description": "profile hotspot linker fixture",
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

    symbol_id = "symbol:demo:src/app.py:publish:10"
    hotspot_id = "profile-hotspot:fixture"
    run_id = "profile-run:demo:run-1"
    benchmark_id = "benchmark:fixture"
    budget_id = "performance-budget:publish-cycle"
    endpoint_id = "endpoint:demo:cli:publish"
    asset_id = "asset:demo:sql_relation:nodes_live"
    criterion_id = "criterion:demo:latency"
    test_case_id = "test-case:benchmark"

    class FixtureSource:
        name = "fixture"
        profile = "discovery_crawl"

        def run(
            self,
            run_id_arg: str,
            *,
            mode: str,
            cursor: dict[str, Any] | None = None,
        ) -> SourceRun:
            del run_id_arg, mode, cursor

            def facts() -> Iterable[NodeFact]:
                rows = [
                    (
                        symbol_id,
                        "code_symbol",
                        {
                            "repo": "demo",
                            "path": "src/app.py",
                            "kind": "function",
                            "name": "publish",
                            "qualified_name": "publish",
                            "start_line": 10,
                            "end_line": 30,
                        },
                    ),
                    (run_id, "profile_run", {"profile_run_id": run_id}),
                    (
                        endpoint_id,
                        "interface_endpoint",
                        {
                            "endpoint_id": endpoint_id,
                            "kind": "cli_command",
                            "name": "publish",
                        },
                    ),
                    (
                        asset_id,
                        "data_asset",
                        {
                            "asset_id": asset_id,
                            "kind": "sql_relation",
                            "name": "okg.nodes_live",
                        },
                    ),
                    (
                        criterion_id,
                        "acceptance_criterion",
                        {"criterion_id": criterion_id, "text": "publish latency"},
                    ),
                    (test_case_id, "test_case", {"name": "test_publish_latency"}),
                    (
                        hotspot_id,
                        "profile_hotspot",
                        {
                            "hotspot_id": hotspot_id,
                            "profile_run_id": run_id,
                            "kind": "query",
                            "name": "publish",
                            "path": "src/app.py",
                            "line": 12,
                            "symbol_hint": "publish",
                            "asset_hint": "nodes_live",
                        },
                    ),
                    (
                        benchmark_id,
                        "benchmark_measurement",
                        {
                            "measurement_id": benchmark_id,
                            "profile_run_id": run_id,
                            "scenario": "publish_cycle",
                            "metric": "duration_ms",
                            "test_case_id": test_case_id,
                            "criterion_id": criterion_id,
                        },
                    ),
                    (
                        budget_id,
                        "performance_budget",
                        {
                            "budget_id": budget_id,
                            "scope": "publish_cycle",
                            "metric": "duration_ms",
                            "criterion_id": criterion_id,
                        },
                    ),
                ]
                for node_id, subtype, attrs in rows:
                    yield NodeFact(
                        node_id=node_id,
                        subtype=subtype,
                        attrs=attrs,
                        source_record_id={"node_id": node_id},
                        source_revision={"rev": 1},
                    )

            return SourceRun(facts=facts(), completed_scope=True)

    SourceRunner(clean_runtime, source_name="fixture.profile-hotspot").run(
        FixtureSource(),
        mode="scope_complete",
    )

    out = Publisher(
        clean_runtime,
        "test-profile-hotspot",
        enrichers=[ProfileHotspotLinker()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-profile-hotspot"),
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
                  JOIN okg.graph_nodes d ON d.node_id = e.dst
                 WHERE e.src = ANY(:srcs)
                 ORDER BY e.src, e.edge_type, d.subtype, e.dst
                """,
                {"srcs": [hotspot_id, benchmark_id, budget_id]},
            )
        ]
    finally:
        store.close()
    with psycopg.connect(clean_runtime) as conn:
        diagnostics = ProfileHotspotLinker().diagnostics(conn)

    assert (hotspot_id, "affects", "code_symbol", symbol_id) in rows
    assert (hotspot_id, "references", "interface_endpoint", endpoint_id) in rows
    assert (hotspot_id, "references", "data_asset", asset_id) in rows
    assert (benchmark_id, "references", "profile_run", run_id) in rows
    assert (benchmark_id, "references", "performance_budget", budget_id) in rows
    assert (benchmark_id, "measured_by", "test_case", test_case_id) in rows
    assert (benchmark_id, "references", "acceptance_criterion", criterion_id) in rows
    assert (budget_id, "budgeted_by", "acceptance_criterion", criterion_id) in rows
    assert diagnostics.eligible_nodes == 3
    assert diagnostics.matched_nodes == 3
    assert diagnostics.emitted_edges == 8
    assert diagnostics.unmatched_node_count == 0
