from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "okg"

LEGACY_GRAPH_TOKENS = (
    "okg.node_facts",
    "okg.edge_facts",
    "okg.nodes_live",
    "okg.edges_live",
    "okg.graph_generations",
    "okg.v_branch_layered_nodes",
    "okg.v_branch_layered_edges",
    "node_facts",
    "edge_facts",
    "nodes_live",
    "edges_live",
    "graph_generations",
    "v_branch_layered_nodes",
    "v_branch_layered_edges",
)


CHRONOS_NATIVE_ENTRYPOINTS = {
    "substrate/graph_store.py": {
        "ChronosGraphStore.apply_facts",
        "ChronosGraphStore.checkout_generation",
        "ChronosGraphStore.checkpoint_generation",
        "ChronosGraphStore.get_generation",
        "ChronosGraphStore.list_generations",
        "ChronosGraphStore.latest_generation",
    },
    "substrate/publisher/publisher.py": {
        "Publisher._cycle_chronos_native",
    },
    "substrate/mcp/session.py": {
        "resolve_generation",
        "open_session",
        "_resolve_branch_id",
        "advance_session",
        "preview_advance",
    },
    "substrate/mcp/search.py": {
        "search",
        "_search_lexical",
        "_search_semantic",
        "_search_alias",
    },
    "substrate/mcp/run_sql.py": {
        "run_sql",
        "_rewrite_graph_views",
    },
    "viz/reads.py": {
        "fetch_node",
        "fetch_nodes",
        "iter_nodes",
        "iter_edges",
        "fetch_edges_within",
        "iter_out_edges",
        "iter_in_edges",
        "count_nodes",
        "count_edges",
        "graph_stats",
    },
}


def _qualnames(tree: ast.AST) -> dict[str, ast.AST]:
    found: dict[str, ast.AST] = {}

    def visit(node: ast.AST, prefix: str = "") -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            name = f"{prefix}.{node.name}" if prefix else node.name
            found[name] = node
            if isinstance(node, ast.ClassDef):
                for child in node.body:
                    visit(child, name)
            return
        for child in ast.iter_child_nodes(node):
            visit(child, prefix)

    visit(tree)
    return found


def _string_literals(node: ast.AST) -> list[str]:
    literals: list[str] = []
    body = getattr(node, "body", [])
    docstring_node = body[0] if body and isinstance(body[0], ast.Expr) else None
    docstring_value = (
        docstring_node.value
        if docstring_node is not None
        and isinstance(docstring_node.value, ast.Constant)
        and isinstance(docstring_node.value.value, str)
        else None
    )
    for child in ast.walk(node):
        if child is docstring_node or child is docstring_value:
            continue
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            literals.append(child.value)
    return literals


def test_chronos_native_entrypoints_do_not_reference_legacy_graph_tables() -> None:
    offenders: list[str] = []
    for rel_path, qualnames in CHRONOS_NATIVE_ENTRYPOINTS.items():
        path = SRC / rel_path
        tree = ast.parse(path.read_text(), filename=str(path))
        found = _qualnames(tree)
        missing = sorted(qualnames - set(found))
        assert not missing, f"{rel_path} missing guarded entrypoints: {missing}"
        for qualname in sorted(qualnames):
            for literal in _string_literals(found[qualname]):
                matched = [token for token in LEGACY_GRAPH_TOKENS if token in literal]
                if matched:
                    offenders.append(f"{rel_path}:{qualname}: {matched}")
    assert not offenders, "\n".join(offenders)


def test_source_runner_uses_chronos_graph_and_audit_fact_log_is_opt_in() -> None:
    from okg.substrate.ingest.runner import SourceRunner

    runner = SourceRunner("postgresql://unused", source_name="static")
    assert runner.write_chronos_graph is True
    assert runner.write_fact_log is False
