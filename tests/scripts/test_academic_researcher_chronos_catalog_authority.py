from __future__ import annotations

import ast
from pathlib import Path


_SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts" / "academic_researcher_chronos_e2e.sh"
)


def _embedded_python() -> str:
    source = _SCRIPT.read_text(encoding="utf-8")
    marker = "uv run python - <<'PY'\n"
    start = source.index(marker) + len(marker)
    end = source.index("\nPY\n", start)
    return source[start:end]


def test_academic_researcher_publishers_use_explicit_deployment_authority() -> None:
    tree = ast.parse(_embedded_python())
    direct_publishers = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "Publisher"
    ]
    factory_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "build_deployment_publisher"
    ]

    assert direct_publishers == []
    assert len(factory_calls) == 2
    required_keywords = {
        "dsn",
        "deployment_name",
        "deployment_dir",
        "command",
        "branch_name",
    }
    assert all(
        {keyword.arg for keyword in call.keywords} == required_keywords
        for call in factory_calls
    )
    branches = {
        ast.unparse(
            next(
                keyword.value
                for keyword in call.keywords
                if keyword.arg == "branch_name"
            )
        )
        for call in factory_calls
    }
    assert branches == {
        "'default'",
        "branch",
    }
