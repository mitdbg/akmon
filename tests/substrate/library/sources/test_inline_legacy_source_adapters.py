from __future__ import annotations

import ast
from pathlib import Path

from tests.pytest.tracked_files import tracked_files

ROOT = Path(__file__).resolve().parents[4]
SOURCE_DIR = ROOT / "src" / "okg" / "substrate" / "library" / "sources"

DELETED_SOURCE_MODULES = {
    "_agent_sessions_legacy",
    "_compute_env_local_legacy",
    "_compute_env_remote_hosts_legacy",
    "_git_graph_files_legacy",
    "_git_graph_history_legacy",
    "_openspec_legacy",
    "legacy_spec",
}

DELETED_SOURCE_FILES = {f"{module}.py" for module in DELETED_SOURCE_MODULES}
DELETED_FULL_MODULES = {
    f"okg.substrate.library.sources.{module}" for module in DELETED_SOURCE_MODULES
}


def _python_files() -> list[Path]:
    roots = [ROOT / "src", ROOT / "tests"]
    files: list[Path] = []
    for root in roots:
        files.extend(
            path
            for path in tracked_files(root, "*.py")
            if ".venv" not in path.parts and "__pycache__" not in path.parts
        )
    return sorted(files)


def test_deleted_legacy_source_adapter_files_are_absent() -> None:
    existing = sorted(
        path.name
        for path in SOURCE_DIR.iterdir()
        if path.name in DELETED_SOURCE_FILES or path.name.endswith("_legacy.py")
    )
    assert existing == []


def test_deleted_legacy_source_modules_are_not_imported() -> None:
    violations: list[str] = []

    for path in _python_files():
        # Every violation below names a deleted module in the source text, so
        # a file naming none cannot hold one. Parse the rest uncached: keeping
        # every tree of src/ and tests/ in the session parse cache held about
        # 3 GiB in one xdist worker, and the fast lane's 8 GiB pod killed it.
        text = path.read_text(encoding="utf-8")
        if not any(module in text for module in DELETED_SOURCE_MODULES):
            continue
        tree = ast.parse(text, filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if (
                        alias.name in DELETED_FULL_MODULES
                        or alias.name.rsplit(".", 1)[-1] in DELETED_SOURCE_MODULES
                    ):
                        violations.append(
                            f"{path.relative_to(ROOT)}:{node.lineno}: import {alias.name}"
                        )
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                imported_names = {alias.name for alias in node.names}
                imports_deleted_name = bool(imported_names & DELETED_SOURCE_MODULES)
                imports_deleted_module = (
                    module in DELETED_FULL_MODULES
                    or module.rsplit(".", 1)[-1] in DELETED_SOURCE_MODULES
                )
                imports_from_source_package = (
                    module == "okg.substrate.library.sources"
                    or module.endswith(".library.sources")
                    or (
                        node.level > 0
                        and path.is_relative_to(SOURCE_DIR)
                        and module == ""
                    )
                )
                if imports_deleted_module or (
                    imports_from_source_package and imports_deleted_name
                ):
                    source = "." * node.level + module
                    imported = ", ".join(sorted(imported_names))
                    violations.append(
                        f"{path.relative_to(ROOT)}:{node.lineno}: "
                        f"from {source} import {imported}"
                    )

    assert violations == []
