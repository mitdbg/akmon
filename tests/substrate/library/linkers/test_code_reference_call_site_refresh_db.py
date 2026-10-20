"""PostgreSQL publish proof for issue #2836: a calls edge follows its call site.

The database-free cases live in test_code_reference_call_site_refresh.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_moved_call_site_refreshes_calls_edge_attrs_across_publishes(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Issue #2836: a call site that moves keeps the calls edge's identity
    (caller, calls, callee) but changes its line and reference id. The next
    publish must rewrite those attributes, and a publish that re-derives the
    same edge must not write a new fact for it."""
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.linkers.code_reference import (
        CodeReferenceResolver,
    )
    from okg.substrate.library.sources.base import NodeFact, SourceRun
    from okg.substrate.publisher import Publisher

    deployment = "test-code-reference-moves"
    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": deployment,
        "description": "calls edge follows its call site",
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
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, compose_catalog(dep))

    caller_id = "symbol:demo:tests/test_mw.py:test_json_write:88"
    callee_id = "symbol:demo:tinydb/database.py:TinyDB:20"

    def symbol(node_id: str, path: str, name: str, line: int) -> NodeFact:
        return NodeFact(
            node_id=node_id,
            subtype="code_symbol",
            attrs={
                "repo": "demo",
                "path": path,
                "kind": "function",
                "name": name,
                "qualified_name": name,
                "start_line": line,
                "end_line": line + 20,
            },
            source_record_id={"node_id": node_id},
            source_revision={"rev": 1},
        )

    def call(line: int, **extra: Any) -> NodeFact:
        node_id = f"reference:demo:tests/test_mw.py:TinyDB:{line}"
        return NodeFact(
            node_id=node_id,
            subtype="code_reference",
            attrs={
                "repo": "demo",
                "path": "tests/test_mw.py",
                "language": "python",
                "name": "TinyDB",
                "qualified_name": "TinyDB",
                "reference_kind": "call",
                "enclosing_symbol_id": caller_id,
                "start_line": line,
                "end_line": line,
                **extra,
            },
            source_record_id={"node_id": node_id},
            source_revision={"rev": 1},
        )

    def gone(line: int) -> NodeFact:
        """Retract the call site that used to be at ``line``."""
        node_id = f"reference:demo:tests/test_mw.py:TinyDB:{line}"
        return NodeFact(
            node_id=node_id,
            subtype="code_reference",
            attrs={},
            source_record_id={"node_id": node_id},
            source_revision={"rev": 1},
            op="R",
        )

    class CallSource:
        name = "fixture"
        profile = "discovery_crawl"

        def __init__(self, calls: list[NodeFact]) -> None:
            self.calls = calls

        def run(
            self,
            run_id: str,
            *,
            mode: str,
            cursor: dict[str, Any] | None = None,
        ) -> SourceRun:
            del run_id, mode, cursor
            facts = [
                symbol(caller_id, "tests/test_mw.py", "test_json_write", 88),
                symbol(callee_id, "tinydb/database.py", "TinyDB", 20),
                *self.calls,
            ]
            return SourceRun(
                facts=iter(facts),
                completed_scope=True,
                run_mode="scope_complete",
            )

    catalog_identity = bind_test_catalog_writer(clean_runtime, deployment)

    def publish(calls: list[NodeFact]) -> dict[str, Any]:
        SourceRunner(clean_runtime, source_name="fixture.call-site").run(
            CallSource(calls),
            mode="scope_complete",
        )
        out = Publisher(
            clean_runtime,
            deployment,
            enrichers=[CodeReferenceResolver()],
            catalog_identity=catalog_identity,
        ).cycle()
        assert out["status"] == "published", out
        return out

    def calls_edges(generation_id: int) -> list[dict[str, Any]]:
        store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
        try:
            checkout = store.checkout_generation(generation_id)
            # The source really replaced the call site: only the current
            # reference node is live, so the edge cannot pick an old one.
            live_references = [
                row["node_id"]
                for row in checkout.query(
                    "SELECT node_id FROM okg.graph_nodes "
                    "WHERE subtype = 'code_reference'"
                )
            ]
            rows = checkout.query(
                """
                SELECT src, dst, attrs
                  FROM okg.graph_edges
                 WHERE edge_type = 'calls'
                """
            )
            edges = [
                {
                    "src": row["src"],
                    "dst": row["dst"],
                    "line": (row["attrs"] or {}).get("line"),
                    "reference_id": (row["attrs"] or {}).get("reference_id"),
                }
                for row in rows
            ]
            assert live_references == [edge["reference_id"] for edge in edges]
            return edges
        finally:
            store.close()

    def linker_fact_count() -> int:
        with psycopg.connect(clean_runtime) as conn:
            row = conn.execute(
                "SELECT count(*) FROM okg.edge_facts "
                "WHERE source = '_enricher:code_reference'"
            ).fetchone()
        assert row is not None
        return int(row[0])

    first = publish([call(106)])
    assert calls_edges(first["generation_id"]) == [{
        "src": caller_id,
        "dst": callee_id,
        "line": 106,
        "reference_id": "reference:demo:tests/test_mw.py:TinyDB:106",
    }]

    # Lines were deleted above the call: same caller, same callee, new line.
    moved = publish([call(93), gone(106)])
    assert calls_edges(moved["generation_id"]) == [{
        "src": caller_id,
        "dst": callee_id,
        "line": 93,
        "reference_id": "reference:demo:tests/test_mw.py:TinyDB:93",
    }]

    # The call site changes in a way that leaves the edge as it is: the
    # linker re-derives the same edge and must not write another fact.
    facts_before = linker_fact_count()
    unchanged = publish([call(93, end_column=40)])
    assert linker_fact_count() == facts_before
    assert calls_edges(unchanged["generation_id"]) == [{
        "src": caller_id,
        "dst": callee_id,
        "line": 93,
        "reference_id": "reference:demo:tests/test_mw.py:TinyDB:93",
    }]

    # The call site moves back: the edge returns to a state the linker has
    # written before, which must still be written again. (The reference
    # carries one new attribute so the source writes a fresh node fact
    # rather than matching its first one.)
    back = publish([call(106, end_column=40), gone(93)])
    assert calls_edges(back["generation_id"]) == [{
        "src": caller_id,
        "dst": callee_id,
        "line": 106,
        "reference_id": "reference:demo:tests/test_mw.py:TinyDB:106",
    }]
