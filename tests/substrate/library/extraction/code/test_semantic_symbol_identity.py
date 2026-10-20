from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.library.sources.base import NodeFact
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _symbol_attrs(facts: list[object], qualified_name: str) -> dict[str, object]:
    for fact in facts:
        if (
            isinstance(fact, NodeFact)
            and fact.subtype == "code_symbol"
            and fact.attrs["qualified_name"] == qualified_name
        ):
            return fact.attrs
    raise AssertionError(f"missing code_symbol {qualified_name!r}")


def test_semantic_key_survives_line_move(tmp_path: Path) -> None:
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    path = tmp_path / "src" / "app.py"
    _write(path, "def helper():\n    return 1\n")

    source = CodeStructureSource(repo="demo", repo_root=tmp_path)
    first = _symbol_attrs(
        list(source.run("run-1", mode="cursor").facts),
        "helper",
    )

    _write(path, "\n\n\ndef helper():\n    return 1\n")
    second = _symbol_attrs(
        list(source.run("run-2", mode="cursor").facts),
        "helper",
    )

    assert first["start_line"] != second["start_line"]
    assert first["symbol_id"] != second["symbol_id"]
    assert first["semantic_key"] == second["semantic_key"]
    assert second["canonical_name"] == "src.app.helper"
    assert second["module_name"] == "src.app"
    assert second["identity_provenance"] == "code_structure"
    assert second["legacy_symbol_id"] == second["symbol_id"]


@pytest.mark.postgres
def test_code_structure_publish_populates_semantic_keys(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    import psycopg
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.code_graph import CodeStructureSource
    from okg.substrate.library.sources.git_graph_files import GitFilesSource
    from okg.substrate.publisher import Publisher

    repo = tmp_path / "repo"
    _write(
        repo / "src" / "app.py",
        "def caller():\n    return helper()\n\n\ndef helper():\n    return 1\n",
    )

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-code-semantic-contract",
        "description": "semantic code contract publish fixture",
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

    SourceRunner(clean_runtime, source_name="fixture.git-files").run(
        GitFilesSource(repo="demo", repo_root=repo),
        mode="scope_complete",
    )
    SourceRunner(clean_runtime, source_name="fixture.code-structure").run(
        CodeStructureSource(
            repo="demo",
            repo_root=repo,
            include_exts=[".py"],
        ),
        mode="scope_complete",
    )

    out = Publisher(clean_runtime, "test-code-semantic-contract", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-code-semantic-contract")).cycle()
    assert out["status"] == "published", out

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(out["generation_id"])
        rows = graph.query(
            """
            SELECT
              attrs->>'qualified_name' AS qualified_name,
              attrs->>'semantic_key' AS semantic_key,
              attrs->>'canonical_name' AS canonical_name,
              attrs->>'module_name' AS module_name,
              attrs->>'start_line' AS start_line,
              attrs->>'legacy_symbol_id' AS legacy_symbol_id
            FROM okg.graph_nodes
            WHERE subtype = 'code_symbol'
              AND attrs->>'qualified_name' IN ('caller', 'helper')
            ORDER BY attrs->>'qualified_name'
            """,
        )
    finally:
        store.close()

    by_name = {str(row["qualified_name"]): row for row in rows}
    assert set(by_name) == {"caller", "helper"}
    helper = by_name["helper"]
    assert str(helper["semantic_key"]).startswith("code-symbol:")
    assert helper["canonical_name"] == "src.app.helper"
    assert helper["module_name"] == "src.app"
    assert helper["start_line"] == "5"
    assert str(helper["legacy_symbol_id"]).endswith(":helper:5")
