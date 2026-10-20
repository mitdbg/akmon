from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pytest

from tests.pytest.py_ast_cache import parse_py
from tests.pytest.tracked_files import tracked_files


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src" / "okg"
TEST_ROOT = REPO_ROOT / "tests"
PLANNING = SRC_ROOT / "substrate" / "ingest" / "planning.py"
SERVICE = SRC_ROOT / "substrate" / "ingest" / "service.py"
INGEST_API = SRC_ROOT / "substrate" / "ingest" / "__init__.py"
FACADE_MODULE = "okg.substrate.ingest.service"
PLANNING_MODULE = "okg.substrate.ingest.planning"
# Current dev is 4,469 lines before this extraction. Keep enough headroom for
# formatting while preserving a material reduction in the stateful facade.
EXPECTED_SERVICE_BUDGET = 4200
EXPECTED_PLANNING_BUDGET = 900
FACADE_SYMBOLS = {
    "IngestPlan",
    "plan_ingest_sources",
    "resolve_predecessor_deps",
    "topo_layers",
    "topo_sort",
}
FACADE_REFERENCES = {f"{FACADE_MODULE}.{symbol}" for symbol in FACADE_SYMBOLS}
ENCODED_CHARACTER_ESCAPE = re.compile(
    r"\\(?:[0-7]{1,3}|N\{|U[0-9A-Fa-f]{8}|u[0-9A-Fa-f]{4}|x[0-9A-Fa-f]{2})"
)
LITERAL_BOUNDARY_GAP = r"""(?:['"\s]|[rRuUbBfF]|\\\r?\n|#[^\r\n]*(?:\r?\n|$))*"""
FACADE_LITERAL_SHAPES = tuple(
    re.compile(
        LITERAL_BOUNDARY_GAP.join(re.escape(character) for character in reference)
    )
    for reference in FACADE_REFERENCES
)
OWNER_SYMBOLS = FACADE_SYMBOLS | {
    "ActivationProbeResult",
    "source_events_with_inherited_path_scope",
    "validate_deferred_source_activation",
}
PRIVATE_OWNER_SYMBOLS = {
    "_inherit_source_sync_event",
    "_predecessor_closure_names",
    "_resolve_dependent_fanout",
    "_resolve_source_names",
    "_source_accepts_path_scope",
    "_source_registry_cycle",
    "_with_dependent_closure",
    "_with_direct_dependents",
    "_with_predecessor_closure",
}
ALLOWED_IMPORTS = {
    "__future__",
    "collections.abc",
    "dataclasses",
    "typing",
    "okg.substrate.errors",
    "okg.substrate.source_sync",
    "okg.substrate.sources.registry",
}
FORBIDDEN_IMPORT_PREFIXES = {
    "database": ("psycopg", "psycopg_pool", "okg.substrate.db"),
    "publisher": ("okg.substrate.publisher",),
    "filesystem_or_process": (
        "glob",
        "os",
        "pathlib",
        "shutil",
        "subprocess",
        "tempfile",
    ),
    "probe_adapter": ("okg.substrate.deployment_lint",),
    "source_execution": (
        "importlib",
        "okg.substrate.ingest.adapter_factory",
        "okg.substrate.ingest.runner",
        "okg.substrate.ingest.source_construction",
        "okg.substrate.library.sources",
    ),
}
FORBIDDEN_BUILTIN_REFERENCES = {
    "__builtins__": "unapproved_effect",
    "__import__": "dynamic_import",
    "breakpoint": "unapproved_effect",
    "compile": "unapproved_effect",
    "eval": "unapproved_effect",
    "exec": "unapproved_effect",
    "input": "unapproved_effect",
    "globals": "unapproved_effect",
    "locals": "unapproved_effect",
    "open": "filesystem_or_process",
    "vars": "unapproved_effect",
}
INTENTIONAL_FACADE_FILES = {
    Path("tests/architecture/test_ingest_planning_boundaries.py"),
}
INTENTIONAL_FACADE_FUNCTIONS = {
    Path("tests/substrate/test_ingest_service.py"): {
        "test_live_planning_characterization_before_move",
    },
    Path("tests/substrate/ingest/test_planning_boundary.py"): {
        "test_service_facade_matches_planning_owner",
    },
}
EXPECTED_COMPATIBILITY_REFS = {
    Path("tests/substrate/test_ingest_service.py"): {
        "attr:IngestPlan",
        "attr:plan_ingest_sources",
    },
    Path("tests/substrate/ingest/test_planning_boundary.py"): {
        "attr:IngestPlan",
        "attr:plan_ingest_sources",
        "attr:resolve_predecessor_deps",
        "attr:topo_layers",
        "attr:topo_sort",
    },
}


@dataclass(frozen=True)
class ImportViolation:
    code: str
    module: str
    line: int
    shape: str


def _relative_module(
    node: ast.ImportFrom,
    *,
    package: str = "okg.substrate.ingest",
) -> str:
    if node.level == 0:
        return node.module or ""
    parts = package.split(".")
    base = parts[: len(parts) - (node.level - 1)]
    if node.module:
        base.extend(node.module.split("."))
    return ".".join(base)


def _classify_import(module: str) -> str | None:
    if module == FACADE_MODULE or module.startswith(f"{FACADE_MODULE}."):
        return "leaf_to_facade"
    for code, prefixes in FORBIDDEN_IMPORT_PREFIXES.items():
        if any(module == prefix or module.startswith(f"{prefix}.") for prefix in prefixes):
            return code
    if module not in ALLOWED_IMPORTS:
        return "unapproved_import"
    return None


def _import_violations(source: str) -> list[ImportViolation]:
    tree = ast.parse(source)
    violations: list[ImportViolation] = []
    for node in ast.walk(tree):
        modules: list[tuple[str, str]] = []
        if (
            isinstance(node, ast.Name)
            and isinstance(node.ctx, ast.Load)
            and node.id in FORBIDDEN_BUILTIN_REFERENCES
        ):
            violations.append(
                ImportViolation(
                    FORBIDDEN_BUILTIN_REFERENCES[node.id],
                    f"builtins.{node.id}",
                    node.lineno,
                    "builtin-reference",
                )
            )
        if isinstance(node, ast.Import):
            modules.extend((alias.name, "import") for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = _relative_module(node)
            modules.append((module, "from"))
            if module == "okg.substrate.ingest":
                modules.extend(
                    (f"{module}.{alias.name}", "from-member")
                    for alias in node.names
                )
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "open"
        ):
            violations.append(
                ImportViolation(
                    "filesystem_or_process",
                    "builtins.open",
                    node.lineno,
                    "call",
                )
            )
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in {"compile", "eval", "exec", "input"}
        ):
            violations.append(
                ImportViolation(
                    "unapproved_effect",
                    f"builtins.{node.func.id}",
                    node.lineno,
                    "call",
                )
            )
        elif isinstance(node, ast.Call) and node.args:
            first = node.args[0]
            if isinstance(node.func, ast.Name) and node.func.id == "__import__":
                if isinstance(first, ast.Constant) and isinstance(first.value, str):
                    modules.append((first.value, "dynamic"))
                else:
                    violations.append(
                        ImportViolation(
                            "dynamic_import",
                            "<nonliteral>",
                            node.lineno,
                            "dynamic",
                        )
                    )
            elif (
                isinstance(first, ast.Constant)
                and isinstance(first.value, str)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "importlib"
                and node.func.attr == "import_module"
            ):
                    modules.append((first.value, "dynamic"))
        for module, shape in modules:
            code = _classify_import(module)
            if code is not None:
                violations.append(
                    ImportViolation(code, module, node.lineno, shape)
                )
    return violations


def _top_level_definitions(tree: ast.Module) -> Counter[str]:
    return Counter(
        node.name
        for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    )


def _duplicate_owner_names(
    planning_tree: ast.Module,
    service_tree: ast.Module,
) -> set[str]:
    planning_definitions = set(_top_level_definitions(planning_tree))
    service_definitions = set(_top_level_definitions(service_tree))
    return (
        OWNER_SYMBOLS | PRIVATE_OWNER_SYMBOLS
    ) & planning_definitions & service_definitions


def _imported_names(tree: ast.Module, module: str) -> set[str]:
    return {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == module
        for alias in node.names
    }


def _all_names(tree: ast.Module) -> set[str]:
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "__all__"
            for target in node.targets
        ):
            continue
        if isinstance(node.value, (ast.List, ast.Tuple)):
            return {
                element.value
                for element in node.value.elts
                if isinstance(element, ast.Constant) and isinstance(element.value, str)
            }
    return set()


def _literal_dict(tree: ast.Module, name: str) -> dict[str, str]:
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            continue
        value = ast.literal_eval(node.value)
        assert isinstance(value, dict)
        return value
    return {}


def _assert_line_budget(source: str, *, limit: int, label: str) -> None:
    observed = len(source.splitlines())
    assert observed <= limit, f"{label}: {observed} lines exceeds {limit}"


def _dotted_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else None
    return None


def _module_package(path: Path) -> str | None:
    try:
        relative = path.resolve().relative_to((REPO_ROOT / "src").resolve())
    except ValueError:
        return None
    return ".".join(relative.parent.parts)


class _ScopeAssignmentCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.assignments: list[ast.Assign | ast.AnnAssign] = []

    def visit_Assign(self, node: ast.Assign) -> None:
        self.assignments.append(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self.assignments.append(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        return

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        return

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        return

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return


def _string_constants(
    tree: ast.Module,
    *,
    inherited: dict[str, str] | None = None,
) -> dict[str, str]:
    collector = _ScopeAssignmentCollector()
    collector.visit(tree)
    assignments: dict[str, ast.AST] = {}
    ambiguous: set[str] = set()
    for node in collector.assignments:
        targets: list[ast.Name] = []
        value: ast.AST | None = None
        if isinstance(node, ast.Assign):
            targets = [
                target for target in node.targets if isinstance(target, ast.Name)
            ]
            value = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            targets = [node.target]
            value = node.value
        if value is None:
            continue
        for target in targets:
            if target.id in assignments:
                ambiguous.add(target.id)
            else:
                assignments[target.id] = value
    for name in ambiguous:
        assignments.pop(name, None)

    shadowed = set(assignments) | ambiguous
    inherited_constants = {
        name: value
        for name, value in (inherited or {}).items()
        if name not in shadowed
    }
    local_constants: dict[str, str] = {}
    changed = True
    while changed:
        changed = False
        for name, value in assignments.items():
            if name in local_constants:
                continue
            resolved = _resolved_string(
                value,
                {**inherited_constants, **local_constants},
            )
            if resolved is None:
                continue
            local_constants[name] = resolved
            changed = True
    return {**inherited_constants, **local_constants}


def _resolved_string(node: ast.AST, constants: dict[str, str]) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _resolved_string(node.left, constants)
        right = _resolved_string(node.right, constants)
        if left is not None and right is not None:
            return left + right
    return None


def _assigned_names(node: ast.Call, tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for candidate in ast.walk(tree):
        if isinstance(candidate, ast.Assign) and candidate.value is node:
            names.update(
                target.id for target in candidate.targets if isinstance(target, ast.Name)
            )
        elif (
            isinstance(candidate, ast.AnnAssign)
            and candidate.value is node
            and isinstance(candidate.target, ast.Name)
        ):
            names.add(candidate.target.id)
    return names


def _parent_map(tree: ast.Module) -> dict[int, ast.AST]:
    return {
        id(child): parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }


def _lexical_constants(
    node: ast.AST,
    *,
    tree: ast.Module,
    parents: dict[int, ast.AST],
    cache: dict[int, dict[str, str]],
) -> dict[str, str]:
    scope: ast.AST = node
    while not isinstance(scope, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef)):
        parent = parents.get(id(scope))
        if parent is None:
            scope = tree
            break
        scope = parent
    cached = cache.get(id(scope))
    if cached is not None:
        return cached
    if isinstance(scope, ast.Module):
        constants = _string_constants(scope)
    else:
        parent = parents.get(id(scope))
        inherited = (
            _lexical_constants(
                parent,
                tree=tree,
                parents=parents,
                cache=cache,
            )
            if parent is not None
            else _string_constants(tree)
        )
        constants = _string_constants(
            ast.Module(body=scope.body, type_ignores=[]),
            inherited=inherited,
        )
    cache[id(scope)] = constants
    return constants


def _facade_consumer_refs(
    path: Path,
    *,
    ignore_functions: set[str] | None = None,
    package: str | None = None,
) -> list[str]:
    tree = parse_py(path)
    try:
        label = str(path.relative_to(REPO_ROOT))
    except ValueError:
        label = str(path)
    aliases: set[str] = set()
    importlib_aliases = {"importlib"}
    dynamic_import_aliases = {"__import__"}
    parents = _parent_map(tree)
    constants_cache: dict[int, dict[str, str]] = {}
    refs: list[str] = []
    ignored_node_ids = {
        id(descendant)
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in (ignore_functions or set())
        for descendant in ast.walk(node)
    }
    for node in ast.walk(tree):
        if id(node) in ignored_node_ids:
            continue
        if isinstance(node, ast.ImportFrom):
            resolved_module = (
                _relative_module(node, package=package or _module_package(path) or "")
                if node.level == 0 or package or _module_package(path)
                else node.module or ""
            )
        else:
            resolved_module = ""
        if isinstance(node, ast.ImportFrom) and resolved_module == FACADE_MODULE:
            for alias in node.names:
                if alias.name == "*":
                    refs.append(f"{label}:{node.lineno}:from:*")
                elif alias.name in FACADE_SYMBOLS:
                    refs.append(f"{label}:{node.lineno}:from:{alias.name}")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == FACADE_MODULE:
                    aliases.add(alias.asname or alias.name)
                elif alias.name == "importlib":
                    importlib_aliases.add(alias.asname or alias.name)
        elif (
            isinstance(node, ast.ImportFrom)
            and resolved_module == "okg.substrate.ingest"
        ):
            for alias in node.names:
                if alias.name == "service":
                    aliases.add(alias.asname or alias.name)
        if isinstance(node, ast.ImportFrom) and resolved_module == "importlib":
            for alias in node.names:
                if alias.name == "import_module":
                    dynamic_import_aliases.add(alias.asname or alias.name)

    assignment_nodes = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        and id(node) not in ignored_node_ids
    ]
    changed = True
    while changed:
        changed = False
        for node in assignment_nodes:
            if isinstance(node, ast.Assign):
                targets = [
                    target.id
                    for target in node.targets
                    if isinstance(target, ast.Name)
                ]
                value = node.value
            else:
                targets = (
                    [node.target.id]
                    if isinstance(node.target, ast.Name)
                    else []
                )
                value = node.value
            dotted_value = _dotted_name(value) if value is not None else None
            if dotted_value in aliases:
                for target in targets:
                    if target not in aliases:
                        aliases.add(target)
                        changed = True
            if dotted_value in importlib_aliases:
                for target in targets:
                    if target not in importlib_aliases:
                        importlib_aliases.add(target)
                        changed = True
            is_dynamic_alias = dotted_value in dynamic_import_aliases
            if isinstance(value, ast.Attribute):
                is_dynamic_alias = is_dynamic_alias or (
                    value.attr == "import_module"
                    and _dotted_name(value.value) in importlib_aliases
                )
            if is_dynamic_alias:
                for target in targets:
                    if target not in dynamic_import_aliases:
                        dynamic_import_aliases.add(target)
                        changed = True

    for node in ast.walk(tree):
        if id(node) in ignored_node_ids or not isinstance(node, ast.Call) or not node.args:
            continue
        function = _dotted_name(node.func)
        is_dynamic_import = function in dynamic_import_aliases
        if not is_dynamic_import and isinstance(node.func, ast.Attribute):
            is_dynamic_import = (
                node.func.attr == "import_module"
                and _dotted_name(node.func.value) in importlib_aliases
            )
        if not is_dynamic_import:
            continue
        module = _resolved_string(
            node.args[0],
            _lexical_constants(
                node,
                tree=tree,
                parents=parents,
                cache=constants_cache,
            ),
        )
        if module == FACADE_MODULE:
            refs.append(f"{label}:{node.lineno}:dynamic:module")
            aliases.update(_assigned_names(node, tree))

    for node in ast.walk(tree):
        if id(node) in ignored_node_ids:
            continue
        if isinstance(node, ast.Attribute) and node.attr in FACADE_SYMBOLS:
            dotted = _dotted_name(node)
            owner = _dotted_name(node.value)
            if dotted and (
                dotted.startswith(f"{FACADE_MODULE}.") or owner in aliases
            ):
                refs.append(f"{label}:{node.lineno}:attr:{node.attr}")
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and _dotted_name(node.args[0]) in aliases
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in FACADE_SYMBOLS
        ):
            refs.append(
                f"{label}:{node.lineno}:getattr:{node.args[1].value}"
            )
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            for symbol in FACADE_SYMBOLS:
                if node.value == f"{FACADE_MODULE}.{symbol}":
                    refs.append(
                        f"{label}:{node.lineno}:string:{symbol}"
                    )
    return sorted(set(refs))


def _could_reference_ingest_service(path: Path) -> bool:
    """Avoid AST-walking files that cannot name the ingest-service facade.

    Dynamic imports stay conservatively included even when the module name is
    assembled from split string fragments. Relative imports need ``service``
    in the source and ``ingest`` in the package path; every absolute shape has
    both names in the source itself.
    """
    source = path.read_text(encoding="utf-8")
    if "import_module" in source or "__import__" in source:
        return True
    if "service" in source and ("ingest" in source or "ingest" in path.parts):
        return True
    return _contains_facade_reference_literal(source)


def _contains_facade_reference_literal(source: str) -> bool:
    """AST-check only sources shaped like encoded or adjacent literals."""
    if not ENCODED_CHARACTER_ESCAPE.search(source) and not any(
        shape.search(source) for shape in FACADE_LITERAL_SHAPES
    ):
        return False
    return any(
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value in FACADE_REFERENCES
        for node in ast.walk(ast.parse(source))
    )


def test_planning_module_owns_every_extracted_definition() -> None:
    planning_tree = parse_py(PLANNING)
    service_tree = parse_py(SERVICE)
    planning_definitions = _top_level_definitions(planning_tree)

    for symbol in OWNER_SYMBOLS | PRIVATE_OWNER_SYMBOLS:
        assert planning_definitions[symbol] == 1, symbol
    assert not _duplicate_owner_names(planning_tree, service_tree)
    assert not {
        name: count
        for name, count in planning_definitions.items()
        if count > 1
    }


def test_planning_imports_only_pure_contract_modules() -> None:
    violations = _import_violations(PLANNING.read_text(encoding="utf-8"))
    assert violations == []


@pytest.mark.parametrize(
    ("source", "expected_code"),
    [
        ("open('/tmp/escape')", "filesystem_or_process"),
        ("module_name = 'x'\n__import__(module_name)", "dynamic_import"),
        ("eval('1 + 1')", "unapproved_effect"),
    ],
)
def test_seeded_builtin_purity_escape_is_rejected(
    source: str,
    expected_code: str,
) -> None:
    codes = {violation.code for violation in _import_violations(source)}
    assert expected_code in codes


@pytest.mark.parametrize(
    ("source", "expected_code"),
    [
        ("file_open = open\nfile_open('/tmp/escape')", "filesystem_or_process"),
        (
            "module_name = 'x'\nloader = __import__\nloader(module_name)",
            "dynamic_import",
        ),
        (
            "loader = __builtins__['__import__']\nloader('x')",
            "unapproved_effect",
        ),
    ],
)
def test_seeded_builtin_alias_escape_is_rejected(
    source: str,
    expected_code: str,
) -> None:
    codes = {violation.code for violation in _import_violations(source)}
    assert expected_code in codes


@pytest.mark.parametrize(
    "source",
    [
        "import okg.substrate.ingest.service as service",
        "from okg.substrate.ingest.service import IngestPlan",
        "from okg.substrate.ingest import service",
        "from . import service",
        "from .service import plan_ingest_sources",
        "import importlib\nimportlib.import_module('okg.substrate.ingest.service')",
        "__import__('okg.substrate.ingest.service')",
    ],
)
def test_seeded_leaf_to_facade_import_is_rejected(source: str) -> None:
    codes = {violation.code for violation in _import_violations(source)}
    assert "leaf_to_facade" in codes


def test_allowed_contract_import_is_not_rejected() -> None:
    assert _import_violations(
        "from okg.substrate.sources.registry import SourceEntry"
    ) == []


def test_service_depends_on_planning_in_one_direction() -> None:
    service_tree = parse_py(SERVICE)
    api_tree = parse_py(INGEST_API)
    api_exports = _literal_dict(api_tree, "_EXPORTS")

    assert FACADE_SYMBOLS <= _imported_names(service_tree, PLANNING_MODULE)
    assert FACADE_SYMBOLS <= api_exports.keys()
    assert all(api_exports[name] == PLANNING_MODULE for name in FACADE_SYMBOLS)
    assert not _imported_names(api_tree, PLANNING_MODULE)
    assert not _imported_names(api_tree, FACADE_MODULE)
    assert "_validate_deferred_source_activation" in _top_level_definitions(
        service_tree
    )
    assert FACADE_SYMBOLS <= _all_names(service_tree)
    assert FACADE_SYMBOLS <= _all_names(api_tree)


def test_planning_import_does_not_load_stateful_ingest_modules() -> None:
    probe = """
import json
import sys

import okg.substrate.ingest as ingest_api
import okg.substrate.ingest.planning

forbidden = (
    "okg.substrate.ingest.runner",
    "okg.substrate.ingest.service",
    "okg.substrate.publisher",
    "psycopg",
)
loaded = sorted(
    name
    for name in sys.modules
    if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
)
print(json.dumps({
    "loaded": loaded,
    "missing_api_names": sorted(set(ingest_api.__all__) - set(dir(ingest_api))),
}))
"""

    completed = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == {
        "loaded": [],
        "missing_api_names": [],
    }


def test_planning_and_service_line_budgets() -> None:
    _assert_line_budget(
        PLANNING.read_text(encoding="utf-8"),
        limit=EXPECTED_PLANNING_BUDGET,
        label=str(PLANNING.relative_to(REPO_ROOT)),
    )
    _assert_line_budget(
        SERVICE.read_text(encoding="utf-8"),
        limit=EXPECTED_SERVICE_BUDGET,
        label=str(SERVICE.relative_to(REPO_ROOT)),
    )


def test_budget_guard_rejects_seeded_overage() -> None:
    _assert_line_budget("\n".join("x" for _ in range(900)), limit=900, label="seed")
    with pytest.raises(AssertionError, match="901 lines exceeds 900"):
        _assert_line_budget(
            "\n".join("x" for _ in range(901)),
            limit=900,
            label="seed",
        )


def test_facade_consumer_detector_rejects_aliases_and_strings(tmp_path: Path) -> None:
    fixture = tmp_path / "consumer.py"
    fixture.write_text(
        "import okg.substrate.ingest.service as svc\n"
        "svc.topo_sort({})\n"
        "TARGET = 'okg.substrate.ingest.service.plan_ingest_sources'\n",
        encoding="utf-8",
    )

    refs = _facade_consumer_refs(fixture)

    assert any(":attr:topo_sort" in ref for ref in refs)
    assert any(":string:plan_ingest_sources" in ref for ref in refs)


def test_facade_consumer_detector_rejects_relative_star_and_dynamic_aliases(
    tmp_path: Path,
) -> None:
    relative_fixture = tmp_path / "relative_consumer.py"
    relative_fixture.write_text(
        "from .service import topo_sort\n"
        "from okg.substrate.ingest.service import *\n",  # noqa: F403
        encoding="utf-8",
    )
    dynamic_fixture = tmp_path / "dynamic_consumer.py"
    dynamic_fixture.write_text(
        "import importlib\n"
        "FACADE = 'okg.substrate.ingest.service'\n"
        "svc = importlib.import_module(FACADE)\n"
        "svc.topo_sort({})\n",
        encoding="utf-8",
    )

    relative_refs = _facade_consumer_refs(
        relative_fixture,
        package="okg.substrate.ingest",
    )
    dynamic_refs = _facade_consumer_refs(dynamic_fixture)

    assert any(":from:topo_sort" in ref for ref in relative_refs)
    assert any(":from:*" in ref for ref in relative_refs)
    assert any(":dynamic:module" in ref for ref in dynamic_refs)
    assert any(":attr:topo_sort" in ref for ref in dynamic_refs)


@pytest.mark.parametrize(
    "source",
    [
        (
            "import okg.substrate.ingest.service as svc\n"
            "owner = svc\n"
            "owner.topo_sort({})\n"
        ),
        (
            "FACADE = 'okg.substrate.ingest.service'\n"
            "loader = __import__\n"
            "svc = loader(FACADE)\n"
            "svc.topo_sort({})\n"
        ),
        (
            "import importlib\n"
            "PREFIX = 'okg.substrate.'\n"
            "FACADE = PREFIX + 'ingest.service'\n"
            "svc = importlib.import_module(FACADE)\n"
            "svc.topo_sort({})\n"
        ),
    ],
)
def test_facade_consumer_detector_rejects_propagated_aliases(
    tmp_path: Path,
    source: str,
) -> None:
    fixture = tmp_path / "propagated_consumer.py"
    fixture.write_text(source, encoding="utf-8")

    refs = _facade_consumer_refs(fixture)

    assert any(":attr:topo_sort" in ref for ref in refs)


@pytest.mark.parametrize(
    "source",
    [
        (
            "import importlib\n"
            "FACADE = 'okg.substrate.ingest.service'\n"
            "il = importlib\n"
            "svc = il.import_module(FACADE)\n"
            "svc.topo_sort({})\n"
        ),
        (
            "import importlib\n"
            "def load():\n"
            "    FACADE = 'okg.substrate.ingest.service'\n"
            "    svc = importlib.import_module(FACADE)\n"
            "    return svc.topo_sort({})\n"
        ),
        (
            "import importlib\n"
            "def load():\n"
            "    PREFIX = 'okg.substrate.'\n"
            "    FACADE = PREFIX + 'ingest.service'\n"
            "    svc = importlib.import_module(FACADE)\n"
            "    return svc.topo_sort({})\n"
        ),
    ],
)
def test_facade_consumer_detector_resolves_lexical_dynamic_imports(
    tmp_path: Path,
    source: str,
) -> None:
    fixture = tmp_path / "lexical_consumer.py"
    fixture.write_text(source, encoding="utf-8")

    refs = _facade_consumer_refs(fixture)

    assert any(":dynamic:module" in ref for ref in refs)
    assert any(":attr:topo_sort" in ref for ref in refs)


def test_facade_scan_prefilter_keeps_detectable_reference_shapes(
    tmp_path: Path,
) -> None:
    fixtures = {
        "absolute.py": (
            "import okg.substrate.ingest.service as svc\nsvc.topo_sort({})\n",
            None,
        ),
        "ingest/relative.py": (
            "from .service import topo_sort\n",
            "okg.substrate.ingest",
        ),
        "dynamic.py": (
            (
                "from importlib import import_module\n"
                "target = 'okg.' + 'substrate.' + 'in' + 'gest.' + 'ser' + 'vice'\n"
                "svc = import_module(target)\n"
                "svc.topo_sort({})\n"
            ),
            None,
        ),
        "adjacent_literal.py": (
            "ref = 'okg.substrate.in' 'gest.service.topo_sort'\n",
            None,
        ),
        "fully_split_literal.py": (
            (
                "ref = ('okg' '.' 'substrate' '.' 'in' 'gest' '.' "
                "'ser' 'vice' '.' 'topo' '_sort')\n"
            ),
            None,
        ),
        "multiline_triple_literal.py": (
            (
                "ref = (\n"
                "    'okg.substrate.in'\n"
                "    # Implicit concatenation ignores this comment.\n"
                "    '''gest.service.topo_sort'''\n"
                ")\n"
            ),
            None,
        ),
        "triple_literal_first.py": (
            (
                "ref = ('''okg.substrate.in'''\n"
                '       """gest.service.topo_sort""")\n'
            ),
            None,
        ),
        "escaped_literal.py": (
            "ref = 'okg.substrate.\\x69ngest.ser\\x76ice.topo_sort'\n",
            None,
        ),
    }
    for relative, (source, package) in fixtures.items():
        fixture = tmp_path / relative
        fixture.parent.mkdir(parents=True, exist_ok=True)
        fixture.write_text(source, encoding="utf-8")
        assert _could_reference_ingest_service(fixture), relative
        assert _facade_consumer_refs(fixture, package=package), relative

    irrelevant = tmp_path / "describe_service.py"
    irrelevant.write_text(
        "def describe_service():\n    return 'healthy'\n",
        encoding="utf-8",
    )
    assert not _could_reference_ingest_service(irrelevant)


def test_duplicate_planning_owner_seed_is_rejected() -> None:
    planning_tree = ast.parse("class IngestPlan:\n    pass\n")
    service_tree = ast.parse("class IngestPlan:\n    pass\n")

    assert _duplicate_owner_names(planning_tree, service_tree) == {"IngestPlan"}
    with pytest.raises(AssertionError, match="IngestPlan"):
        assert not _duplicate_owner_names(planning_tree, service_tree), "IngestPlan"


def test_zero_direct_planning_consumers_require_service_facade() -> None:
    candidates = [
        *tracked_files(SRC_ROOT, "*.py"),
        *tracked_files(TEST_ROOT, "*.py"),
    ]
    violations: list[str] = []
    for relative, expected in EXPECTED_COMPATIBILITY_REFS.items():
        refs = _facade_consumer_refs(REPO_ROOT / relative)
        observed = {":".join(ref.split(":")[-2:]) for ref in refs}
        assert observed == expected, (relative, refs)
    for path in candidates:
        relative = path.relative_to(REPO_ROOT)
        if path == SERVICE or relative in INTENTIONAL_FACADE_FILES:
            continue
        if not _could_reference_ingest_service(path):
            continue
        violations.extend(
            _facade_consumer_refs(
                path,
                ignore_functions=INTENTIONAL_FACADE_FUNCTIONS.get(relative),
            )
        )

    assert violations == []
