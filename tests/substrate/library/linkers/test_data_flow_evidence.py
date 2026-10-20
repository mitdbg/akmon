from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_data_flow_evidence_linker_resolves_spans_and_hinted_targets(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.linkers.data_flow_evidence import (
        DataFlowEvidenceLinker,
    )
    from okg.substrate.library.sources.base import NodeFact, SourceRun
    from okg.substrate.publisher import Publisher

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-data-flow-evidence",
        "description": "third-party data-flow evidence linker fixture",
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

    symbol_id = "symbol:demo:src/db.py:write_audit:10"
    observation_id = "flow:semgrep:fixture"
    step_id = "flowstep:fixture:0"
    unresolved_id = "flow:semgrep:unresolved"
    asset_id = "asset:demo:sql_relation:audit_events"
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
                    node_id=symbol_id,
                    subtype="code_symbol",
                    attrs={
                        "repo": "demo",
                        "path": "src/db.py",
                        "kind": "function",
                        "name": "write_audit",
                        "qualified_name": "write_audit",
                        "start_line": 10,
                        "end_line": 30,
                    },
                    source_record_id={"node_id": symbol_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=asset_id,
                    subtype="data_asset",
                    attrs={
                        "asset_id": asset_id,
                        "repo": "demo",
                        "kind": "sql_relation",
                        "name": "audit.events",
                    },
                    source_record_id={"node_id": asset_id},
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
                    },
                    source_record_id={"node_id": config_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=observation_id,
                    subtype="data_flow_observation",
                    attrs={
                        "observation_id": observation_id,
                        "repo": "demo",
                        "tool": "semgrep",
                        "rule_id": "demo.sql.write",
                        "path": "src/db.py",
                        "start_line": 12,
                        "end_line": 18,
                        "asset_hint": "audit.events",
                        "config_key_hint": "OKG_DSN",
                    },
                    source_record_id={"node_id": observation_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=step_id,
                    subtype="data_flow_step",
                    attrs={
                        "step_id": step_id,
                        "observation_id": observation_id,
                        "index": 0,
                        "kind": "taint_sink",
                        "path": "src/db.py",
                        "start_line": 20,
                        "end_line": 20,
                        "asset_hint": "events",
                    },
                    source_record_id={"node_id": step_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=unresolved_id,
                    subtype="data_flow_observation",
                    attrs={
                        "observation_id": unresolved_id,
                        "repo": "demo",
                        "tool": "semgrep",
                        "rule_id": "demo.unresolved",
                        "path": "src/missing.py",
                        "start_line": 99,
                    },
                    source_record_id={"node_id": unresolved_id},
                    source_revision={"rev": 1},
                )

            return SourceRun(facts=facts(), completed_scope=True)

    SourceRunner(clean_runtime, source_name="fixture.data-flow-evidence").run(
        FixtureSource(),
        mode="scope_complete",
    )

    out = Publisher(
        clean_runtime,
        "test-data-flow-evidence",
        enrichers=[DataFlowEvidenceLinker()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-data-flow-evidence"),
    ).cycle()
    assert out["status"] == "published", out

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = [
            (row["src"], row["dst_subtype"], row["dst"])
            for row in checkout.query(
                """
                SELECT e.src, d.subtype AS dst_subtype, e.dst
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes d ON d.node_id = e.dst
                 WHERE e.edge_type = 'references'
                   AND e.src = ANY(:srcs)
                 ORDER BY e.src, d.subtype, e.dst
                """,
                {"srcs": [observation_id, step_id, unresolved_id]},
            )
        ]
    finally:
        store.close()
    with psycopg.connect(clean_runtime) as conn:
        diagnostics = DataFlowEvidenceLinker().diagnostics(conn)

    assert (observation_id, "code_symbol", symbol_id) in rows
    assert (observation_id, "data_asset", asset_id) in rows
    assert (observation_id, "config_key", config_id) in rows
    assert (step_id, "code_symbol", symbol_id) in rows
    assert (step_id, "data_asset", asset_id) in rows
    assert all(src != unresolved_id for src, _subtype, _dst in rows)
    assert diagnostics.eligible_evidence_nodes == 3
    assert diagnostics.matched_evidence_nodes == 2
    assert diagnostics.emitted_edges == 5
    assert diagnostics.unmatched_evidence_count == 1
