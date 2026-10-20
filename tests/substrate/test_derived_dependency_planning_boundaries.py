"""Boundary guard for the derived-artifact dependency-planning owner.

This is the narrow architecture check for Slice 5. It answers one
question: does
`src/okg/substrate/derived_artifacts/dependency_planning.py` stay
effect-free, or has something quietly given it a database, publisher or
scheduler-facade dependency?

Three parts, each able to fail on its own:

1. an AST import scan with an explicit allow-list and forbid-list, with
   seeded violations proving the scanner actually catches them;
2. ownership and direction: each owned symbol defined once, in its
   owner, with the scheduler and the package importing from the owner
   and nothing importing back;
3. a subprocess import probe that executes the owner with the package
   initializer stubbed out.

It lives under `tests/substrate` on purpose: that tree is collected by
the existing no-database fast lane, so the guard runs there without any
lane-file change.
"""
from __future__ import annotations

import ast
import json
import pathlib
import subprocess
import sys
from dataclasses import dataclass

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE_DIR = REPO_ROOT / "src" / "okg" / "substrate" / "derived_artifacts"
OWNER_PATH = PACKAGE_DIR / "dependency_planning.py"
HASHING_PATH = PACKAGE_DIR / "hashing.py"
SCHEDULER_PATH = PACKAGE_DIR / "scheduler.py"
INIT_PATH = PACKAGE_DIR / "__init__.py"

FACADE_MODULE = "okg.substrate.derived_artifacts"
OWNER_MODULE = f"{FACADE_MODULE}.dependency_planning"
HASHING_MODULE = f"{FACADE_MODULE}.hashing"
SCHEDULER_MODULE = f"{FACADE_MODULE}.scheduler"
RELATIONS_MODULE = f"{FACADE_MODULE}.relations"

# What the owner may import: the standard library it actually needs, and
# the one pure module that owns the canonical hash.
ALLOWED_ROOTS = frozenset(
    {"__future__", "time", "dataclasses", "typing", "collections"}
)
ALLOWED_MODULES = frozenset({HASHING_MODULE})

# `hashing` is a leaf: it needs the two stdlib modules that compute the
# digest and nothing in-package at all.
HASHING_ALLOWED_ROOTS = frozenset(
    {"__future__", "hashlib", "json", "typing"}
)

PERSISTENCE_PREFIXES = ("psycopg", "okg.substrate.db", RELATIONS_MODULE)
PUBLISHER_PREFIX = "okg.substrate.publisher"

OWNED_SYMBOLS = frozenset(
    {
        "DerivedArtifactScheduleError",
        "DerivedArtifactDependency",
        "DependencyPlan",
        "build_task_map",
        "require_known_dependencies",
        "topological_order",
        "graph_hash",
        "plan_dependency_graph",
    }
)
HASHING_SYMBOLS = frozenset({"stable_hash", "RELATION_SLOT_NAME"})


@dataclass(frozen=True)
class Violation:
    code: str
    detail: str


def _has_prefix(module: str, prefix: str) -> bool:
    return module == prefix or module.startswith(prefix + ".")


def _classify_module(
    module: str,
    allowed_roots: frozenset[str],
    allowed_modules: frozenset[str],
) -> str | None:
    if module in allowed_modules:
        return None
    if module == FACADE_MODULE:
        return "leaf_to_facade"
    if _has_prefix(module, SCHEDULER_MODULE):
        return "leaf_to_facade"
    for prefix in PERSISTENCE_PREFIXES:
        if _has_prefix(module, prefix):
            return "persistence"
    if _has_prefix(module, PUBLISHER_PREFIX):
        return "publisher_dependency"
    if module.split(".")[0] in allowed_roots:
        return None
    return "unapproved_import"


def _resolved_module(node: ast.ImportFrom) -> str:
    """Resolve a possibly-relative import to an absolute module name."""
    if not node.level:
        return node.module or ""
    # level 1 is the owning package; deeper levels walk upwards.
    base_parts = FACADE_MODULE.split(".")
    if node.level > 1:
        base_parts = base_parts[: -(node.level - 1)]
    base = ".".join(base_parts)
    return f"{base}.{node.module}" if node.module else base


def import_violations(
    source: str,
    *,
    allowed_roots: frozenset[str] = ALLOWED_ROOTS,
    allowed_modules: frozenset[str] = ALLOWED_MODULES,
) -> list[Violation]:
    """Report every forbidden import or effect escape in `source`."""
    tree = ast.parse(source)
    violations: list[Violation] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                code = _classify_module(
                    alias.name, allowed_roots, allowed_modules
                )
                if code is not None:
                    violations.append(Violation(code, alias.name))
        elif isinstance(node, ast.ImportFrom):
            module = _resolved_module(node)
            code = _classify_module(module, allowed_roots, allowed_modules)
            if code is not None:
                violations.append(Violation(code, module))
        elif isinstance(node, ast.Call):
            violations.extend(_call_violations(node))
        elif isinstance(node, ast.Name) and node.id in {
            "__import__",
            "eval",
            "exec",
        }:
            # Catch an aliased escape (`loader = __import__`) as well as a
            # direct call.
            violations.append(
                Violation(
                    "dynamic_import"
                    if node.id == "__import__"
                    else "unapproved_effect",
                    node.id,
                )
            )
        elif isinstance(node, ast.Name) and node.id in {"open", "__builtins__"}:
            violations.append(Violation("filesystem_or_process", node.id))

    return violations


def _call_violations(node: ast.Call) -> list[Violation]:
    target = node.func
    name = None
    if isinstance(target, ast.Name):
        name = target.id
    elif isinstance(target, ast.Attribute):
        name = target.attr
    if name in {"import_module", "__import__"}:
        return [Violation("dynamic_import", name)]
    if name in {"system", "popen", "run", "Popen"}:
        return [Violation("filesystem_or_process", name)]
    return []


def _parse(path: pathlib.Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def _top_level_definitions(tree: ast.Module) -> dict[str, int]:
    counts: dict[str, int] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            counts[node.name] = counts.get(node.name, 0) + 1
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    counts[target.id] = counts.get(target.id, 0) + 1
    return counts


def _imported_names(tree: ast.Module, module: str) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            resolved = _resolved_module(node)
            if resolved == module:
                names.update(alias.name for alias in node.names)
    return names


# --------------------------------------------------------------------
# 1. import scan
# --------------------------------------------------------------------


def test_planning_owner_imports_only_allowed_modules() -> None:
    assert import_violations(OWNER_PATH.read_text(encoding="utf-8")) == []


def test_hashing_owner_imports_only_allowed_modules() -> None:
    assert import_violations(
        HASHING_PATH.read_text(encoding="utf-8"),
        allowed_roots=HASHING_ALLOWED_ROOTS,
        allowed_modules=frozenset(),
    ) == []


@pytest.mark.parametrize(
    ("source", "expected_code"),
    [
        ("import psycopg", "persistence"),
        ("from psycopg import sql", "persistence"),
        ("from psycopg.types.json import Jsonb", "persistence"),
        ("from okg.substrate.db.pool import connect", "persistence"),
        ("from .relations import stable_hash", "persistence"),
        (
            "from okg.substrate.derived_artifacts.relations import stable_hash",
            "persistence",
        ),
        ("from okg.substrate.publisher import Publisher", "publisher_dependency"),
        (
            "import okg.substrate.publisher.native_view_stage",
            "publisher_dependency",
        ),
    ],
)
def test_seeded_persistence_dependency_is_rejected(
    source: str,
    expected_code: str,
) -> None:
    codes = {violation.code for violation in import_violations(source)}
    assert expected_code in codes


@pytest.mark.parametrize(
    "source",
    [
        "from okg.substrate.derived_artifacts import stable_hash",
        "import okg.substrate.derived_artifacts",
        "from . import scheduler",
        "from .scheduler import schedule_derived_artifacts",
        "from okg.substrate.derived_artifacts.scheduler import ScheduleResult",
        "import okg.substrate.derived_artifacts.scheduler as scheduler",
    ],
)
def test_seeded_facade_or_scheduler_import_is_rejected(source: str) -> None:
    codes = {violation.code for violation in import_violations(source)}
    assert "leaf_to_facade" in codes


@pytest.mark.parametrize(
    ("source", "expected_code"),
    [
        ("import importlib\nimportlib.import_module('x')", "dynamic_import"),
        ("__import__('okg.substrate.derived_artifacts')", "dynamic_import"),
        ("loader = __import__\nloader('x')", "dynamic_import"),
        ("open('/tmp/escape')", "filesystem_or_process"),
        ("import subprocess\nsubprocess.run(['ls'])", "filesystem_or_process"),
        ("eval('1 + 1')", "unapproved_effect"),
    ],
)
def test_seeded_effect_escape_is_rejected(
    source: str,
    expected_code: str,
) -> None:
    codes = {violation.code for violation in import_violations(source)}
    assert expected_code in codes


@pytest.mark.parametrize(
    "source",
    [
        "from okg.substrate.derived_artifacts.hashing import stable_hash",
        "from dataclasses import dataclass, field",
        "from collections.abc import Callable, Mapping, Sequence",
        "from typing import Any, Generic, Protocol, TypeVar",
        "import time",
    ],
)
def test_allowed_import_is_not_rejected(source: str) -> None:
    assert import_violations(source) == []


# --------------------------------------------------------------------
# 2. ownership and direction
# --------------------------------------------------------------------


def test_planning_owner_defines_every_owned_symbol_exactly_once() -> None:
    definitions = _top_level_definitions(_parse(OWNER_PATH))

    for symbol in OWNED_SYMBOLS:
        assert definitions.get(symbol) == 1, symbol

    duplicates = {
        name: count for name, count in definitions.items() if count > 1
    }
    assert duplicates == {}


def test_hashing_owner_defines_the_moved_hash_symbols_once() -> None:
    definitions = _top_level_definitions(_parse(HASHING_PATH))

    for symbol in HASHING_SYMBOLS:
        assert definitions.get(symbol) == 1, symbol


def test_scheduler_does_not_redefine_owned_symbols() -> None:
    """A re-export must not become a second definition.

    `tests/architecture/test_no_shadowed_definitions.py` records a past
    shadowing incident in this very file; this keeps the extraction from
    re-creating one.
    """
    definitions = _top_level_definitions(_parse(SCHEDULER_PATH))

    for symbol in OWNED_SYMBOLS | HASHING_SYMBOLS:
        assert symbol not in definitions, symbol


def test_dependency_runs_in_one_direction_only() -> None:
    owner_tree = _parse(OWNER_PATH)
    scheduler_tree = _parse(SCHEDULER_PATH)
    init_tree = _parse(INIT_PATH)

    # The owner imports from `hashing` and from nothing else in-package.
    assert _imported_names(owner_tree, HASHING_MODULE) == {
        "RELATION_SLOT_NAME",
        "stable_hash",
    }
    assert _imported_names(owner_tree, SCHEDULER_MODULE) == set()
    assert _imported_names(owner_tree, RELATIONS_MODULE) == set()
    assert _imported_names(owner_tree, FACADE_MODULE) == set()

    # The scheduler imports the rules from the owner, not the reverse.
    scheduler_from_owner = _imported_names(scheduler_tree, OWNER_MODULE)
    assert {
        "DerivedArtifactDependency",
        "DerivedArtifactScheduleError",
        "build_task_map",
        "plan_dependency_graph",
    } <= scheduler_from_owner

    # The package points at each owner directly.
    assert {
        "DerivedArtifactDependency",
        "DerivedArtifactScheduleError",
    } <= _imported_names(init_tree, OWNER_MODULE)
    assert "stable_hash" in _imported_names(init_tree, HASHING_MODULE)
    assert "stable_hash" not in _imported_names(init_tree, RELATIONS_MODULE)
    assert not {
        "DerivedArtifactDependency",
        "DerivedArtifactScheduleError",
    } & _imported_names(init_tree, SCHEDULER_MODULE)


# --------------------------------------------------------------------
# 3. stub-package import probe
# --------------------------------------------------------------------

_PROBE = """
import json
import pathlib
import sys
import types

import okg.substrate

package_dir = pathlib.Path(sys.argv[1])
stub = types.ModuleType("okg.substrate.derived_artifacts")
stub.__path__ = [str(package_dir)]
sys.modules["okg.substrate.derived_artifacts"] = stub
okg.substrate.derived_artifacts = stub

try:
    import okg.substrate.derived_artifacts.dependency_planning as owner
except ImportError as exc:
    print(json.dumps({"facade_import_error": str(exc)}))
    raise SystemExit(0)

forbidden = (
    "psycopg",
    "okg.substrate.publisher",
    "okg.substrate.db",
    "okg.substrate.derived_artifacts.scheduler",
    "okg.substrate.derived_artifacts.relations",
)
loaded = sorted(
    name
    for name in sys.modules
    if any(name == p or name.startswith(p + ".") for p in forbidden)
)
print(json.dumps({
    "loaded": loaded,
    "owner": owner.__name__,
    "has_entry_point": hasattr(owner, "plan_dependency_graph"),
}))
"""


def test_planning_owner_imports_no_persistence_under_package_stub() -> None:
    """Execute the owner without running the package initializer.

    Importing the owner normally would execute
    `derived_artifacts/__init__.py`, which imports `.scheduler` and
    `.relations` and therefore loads psycopg -- so a plain sys.modules
    check would prove nothing here. Stubbing the package with a
    `__path__` lets the real owner module load while the initializer
    never runs.

    Its limit is deliberate: this proves the owner's own import cone is
    persistence-free. It does not claim that importing the package loads
    no database module; it does.
    """
    completed = subprocess.run(
        [sys.executable, "-c", _PROBE, str(PACKAGE_DIR)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout.strip().splitlines()[-1])

    # A facade import fails under the stub with "cannot import name ...
    # from 'okg.substrate.derived_artifacts' (unknown location)". Report
    # it as what it is rather than letting the probe crash.
    assert "facade_import_error" not in result, (
        "the planning owner imports from the package facade: "
        + str(result.get("facade_import_error"))
    )
    assert result["loaded"] == []
    assert result["owner"] == OWNER_MODULE
    assert result["has_entry_point"] is True


def test_probe_reports_a_seeded_facade_import_instead_of_crashing(
    tmp_path: pathlib.Path,
) -> None:
    """The probe's own failure mode, exercised on a seeded copy."""
    seeded_dir = tmp_path / "derived_artifacts"
    seeded_dir.mkdir()
    (seeded_dir / "__init__.py").write_text(
        "raise AssertionError('initializer must not run')\n",
        encoding="utf-8",
    )
    (seeded_dir / "dependency_planning.py").write_text(
        "from okg.substrate.derived_artifacts import stable_hash\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, "-c", _PROBE, str(seeded_dir)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout.strip().splitlines()[-1])
    assert "facade_import_error" in result
    assert "cannot import name" in result["facade_import_error"]
