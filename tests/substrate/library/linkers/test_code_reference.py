from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_code_reference_resolver_publish_connects_symbols_and_dependencies(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.linkers.code_reference import (
        CodeReferenceResolver,
    )
    from okg.substrate.library.sources.base import NodeFact, SourceRun
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher import Publisher

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-code-reference",
        "description": "code reference resolver fixture",
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

    caller_id = "symbol:demo:src/caller.py:g:10"
    target_id = "symbol:demo:src/okg/fixture/target.py:f:2"
    target_semantic_key = "code-symbol:demo:python:okg.fixture.target.f:function"
    ambiguous_f_id = "symbol:demo:src/okg/fixture/other.py:f:2"
    imported_target_id = (
        "symbol:demo:src/okg/fixture/target.py:imported_target:8"
    )
    imported_ambiguous_id = (
        "symbol:demo:src/okg/fixture/other.py:imported_target:8"
    )
    module_target_id = (
        "symbol:demo:src/okg/fixture/target.py:module_target:12"
    )
    module_ambiguous_id = (
        "symbol:demo:src/okg/fixture/other.py:module_target:12"
    )
    reference_id = "reference:fixture:f-call"
    imported_reference_id = "reference:fixture:imported-target-call"
    module_reference_id = "reference:fixture:module-target-call"
    import_id = "import:fixture:psycopg"
    from_import_id = "import:fixture:from-target"
    module_import_id = "import:fixture:module-target"
    package_id = "dep:python:psycopg"

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
                    node_id=caller_id,
                    subtype="code_symbol",
                    attrs={
                        "repo": "demo",
                        "path": "src/caller.py",
                        "kind": "function",
                        "name": "g",
                        "qualified_name": "g",
                        "start_line": 10,
                        "end_line": 12,
                    },
                    source_record_id={"node_id": caller_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=target_id,
                    subtype="code_symbol",
                    attrs={
                        "repo": "demo",
                        "path": "src/okg/fixture/target.py",
                        "kind": "function",
                        "name": "f",
                        "qualified_name": "f",
                        "canonical_name": "okg.fixture.target.f",
                        "semantic_key": target_semantic_key,
                        "start_line": 2,
                        "end_line": 4,
                    },
                    source_record_id={"node_id": target_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=ambiguous_f_id,
                    subtype="code_symbol",
                    attrs={
                        "repo": "demo",
                        "path": "src/okg/fixture/other.py",
                        "kind": "function",
                        "name": "f",
                        "qualified_name": "f",
                        "canonical_name": "okg.fixture.other.f",
                        "semantic_key": "code-symbol:demo:python:other.f:function",
                        "start_line": 2,
                        "end_line": 4,
                    },
                    source_record_id={"node_id": ambiguous_f_id},
                    source_revision={"rev": 1},
                )
                for node_id, path, name in (
                    (
                        imported_target_id,
                        "src/okg/fixture/target.py",
                        "imported_target",
                    ),
                    (
                        imported_ambiguous_id,
                        "src/okg/fixture/other.py",
                        "imported_target",
                    ),
                    (
                        module_target_id,
                        "src/okg/fixture/target.py",
                        "module_target",
                    ),
                    (
                        module_ambiguous_id,
                        "src/okg/fixture/other.py",
                        "module_target",
                    ),
                ):
                    yield NodeFact(
                        node_id=node_id,
                        subtype="code_symbol",
                        attrs={
                            "repo": "demo",
                            "path": path,
                            "kind": "function",
                            "name": name,
                            "qualified_name": name,
                            "start_line": 8,
                            "end_line": 10,
                        },
                        source_record_id={"node_id": node_id},
                        source_revision={"rev": 1},
                    )
                yield NodeFact(
                    node_id=reference_id,
                    subtype="code_reference",
                    attrs={
                        "repo": "demo",
                        "path": "src/caller.py",
                        "language": "python",
                        "name": "f",
                        "qualified_name": "f",
                        "reference_kind": "call",
                        "semantic_target_key": target_semantic_key,
                        "enclosing_symbol_id": caller_id,
                        "start_line": 11,
                        "end_line": 11,
                    },
                    source_record_id={"node_id": reference_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=imported_reference_id,
                    subtype="code_reference",
                    attrs={
                        "repo": "demo",
                        "path": "src/caller.py",
                        "language": "python",
                        "name": "imported_alias",
                        "qualified_name": "imported_alias",
                        "reference_kind": "call",
                        "enclosing_symbol_id": caller_id,
                        "start_line": 12,
                        "end_line": 12,
                    },
                    source_record_id={"node_id": imported_reference_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=module_reference_id,
                    subtype="code_reference",
                    attrs={
                        "repo": "demo",
                        "path": "src/caller.py",
                        "language": "python",
                        "name": "module_target",
                        "qualified_name": "target_mod.module_target",
                        "reference_kind": "call",
                        "enclosing_symbol_id": caller_id,
                        "start_line": 13,
                        "end_line": 13,
                    },
                    source_record_id={"node_id": module_reference_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=import_id,
                    subtype="code_import",
                    attrs={
                        "repo": "demo",
                        "path": "src/caller.py",
                        "language": "python",
                        "module": "psycopg",
                        "name": "",
                        "kind": "import",
                        "start_line": 1,
                        "end_line": 1,
                    },
                    source_record_id={"node_id": import_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=from_import_id,
                    subtype="code_import",
                    attrs={
                        "repo": "demo",
                        "path": "src/caller.py",
                        "language": "python",
                        "module": "okg.fixture.target",
                        "name": "imported_target",
                        "alias": "imported_alias",
                        "kind": "from_import",
                        "start_line": 2,
                        "end_line": 2,
                    },
                    source_record_id={"node_id": from_import_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=module_import_id,
                    subtype="code_import",
                    attrs={
                        "repo": "demo",
                        "path": "src/caller.py",
                        "language": "python",
                        "module": "okg.fixture.target",
                        "name": "",
                        "alias": "target_mod",
                        "kind": "import",
                        "start_line": 3,
                        "end_line": 3,
                    },
                    source_record_id={"node_id": module_import_id},
                    source_revision={"rev": 1},
                )
                yield NodeFact(
                    node_id=package_id,
                    subtype="dependency_package",
                    attrs={
                        "package_id": package_id,
                        "name": "psycopg",
                        "ecosystem": "python",
                    },
                    source_record_id={"node_id": package_id},
                    source_revision={"rev": 1},
                )

            return SourceRun(facts=facts(), completed_scope=True)

    SourceRunner(clean_runtime, source_name="fixture.code-reference").run(
        FixtureSource(),
        mode="scope_complete",
    )

    out = Publisher(
        clean_runtime,
        "test-code-reference",
        enrichers=[CodeReferenceResolver()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-code-reference"),
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
                 WHERE e.edge_type IN ('references', 'calls')
                 ORDER BY 1, 2, 3, 4, 5
                """
            )
        ]
    finally:
        store.close()
    with psycopg.connect(clean_runtime) as conn:
        diagnostics = CodeReferenceResolver().diagnostics(conn)

    assert (
        "code_import",
        import_id,
        "references",
        "dependency_package",
        package_id,
    ) in rows
    assert (
        "code_reference",
        reference_id,
        "references",
        "code_symbol",
        target_id,
    ) in rows
    assert (
        "code_symbol",
        caller_id,
        "calls",
        "code_symbol",
        target_id,
    ) in rows
    assert (
        "code_reference",
        imported_reference_id,
        "references",
        "code_symbol",
        imported_target_id,
    ) in rows
    assert (
        "code_symbol",
        caller_id,
        "calls",
        "code_symbol",
        imported_target_id,
    ) in rows
    assert (
        "code_reference",
        module_reference_id,
        "references",
        "code_symbol",
        module_target_id,
    ) in rows
    assert (
        "code_symbol",
        caller_id,
        "calls",
        "code_symbol",
        module_target_id,
    ) in rows
    assert diagnostics.eligible_imports == 3
    assert diagnostics.matched_imports == 1
    assert diagnostics.eligible_references == 3
    assert diagnostics.matched_references == 3
    assert diagnostics.emitted_edges == 7
