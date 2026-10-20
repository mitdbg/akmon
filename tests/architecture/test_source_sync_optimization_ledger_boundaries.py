"""Static guards for the source-sync optimization-ledger boundary.

The owner is inspected as source before it is imported.  That keeps the guard
useful when an effectful dependency or facade back-edge is introduced, while
the subprocess probe proves a clean import does not load the stateful source
sync, database, runtime, CLI, or view surfaces.
"""

from __future__ import annotations

import ast
from collections import defaultdict
from dataclasses import dataclass
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from tests.pytest.tracked_files import tracked_files


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
TEST_ROOT = REPO_ROOT / "tests"
DEPLOYMENTS_ROOT = REPO_ROOT / "deployments"
SUBSTRATE_ROOT = SRC_ROOT / "okg" / "substrate"
OWNER_PATH = SUBSTRATE_ROOT / "source_sync_optimization_ledger.py"
FACADE_PATH = SUBSTRATE_ROOT / "source_sync_microbench.py"
COMPATIBILITY_PATH = TEST_ROOT / "substrate" / "test_source_sync_optimization_ledger.py"
OWNER_MODULE = "okg.substrate.source_sync_optimization_ledger"
FACADE_MODULE = "okg.substrate.source_sync_microbench"

PUBLIC_LEDGER_SYMBOLS = frozenset(
    {
        "HOT_WAVE_SOURCE_ORDER",
        "MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA",
        "MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA",
        "SUBSECOND_LATENCY_BUDGET_MS",
        "build_source_optimization_ledger",
        "rank_source_optimization_rows",
        "source_optimization_ledger_relation_rows",
    }
)
PRIVATE_LEDGER_POLICY_SYMBOLS = frozenset(
    {
        "_amplification_signal",
        "_bottleneck_stage",
        "_dedupe_drop_amplification",
        "_fix_decision",
        "_full_pipeline_proof_from_item",
        "_latency_status",
        "_ledger_next_action",
        "_ledger_next_command",
        "_optimization_enricher_row",
        "_optimization_sort_key",
        "_optimization_source_row",
        "_optimization_status",
        "_proof_status",
        "_root_cause_class",
        "_source_wave",
        "_stage_bottleneck",
        "_stage_timings_from_item",
        "_status_sort_rank",
    }
)
MOVED_LEDGER_SYMBOLS = PUBLIC_LEDGER_SYMBOLS | PRIVATE_LEDGER_POLICY_SYMBOLS

# The facade must keep this list byte-for-byte compatible with current dev.
EXPECTED_FACADE_ALL = (
    "MICROBENCH_INVENTORY_SCHEMA",
    "MICROBENCH_OPTIMIZATION_LEDGER_ROWS_SCHEMA",
    "MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA",
    "MICROBENCH_RUN_SCHEMA",
    "MICROBENCH_VIEW_ROWS_SCHEMA",
    "build_source_optimization_ledger",
    "build_microbenchmark_inventory",
    "build_source_sync_microbenchmark_report",
    "microbenchmark_inventory_relation_rows",
    "source_optimization_ledger_relation_rows",
)

ALLOWED_OWNER_IMPORTS = frozenset(
    {
        "__future__",
        "collections",
        "collections.abc",
        "datetime",
        "typing",
    }
)
FORBIDDEN_IMPORT_PREFIXES: dict[str, tuple[str, ...]] = {
    "leaf_to_facade": (FACADE_MODULE,),
    "database": ("psycopg", "psycopg_pool", "okg.substrate.db"),
    "dbos_or_runtime": ("dbos", "okg.substrate.runtime"),
    "source_execution": (
        "okg.substrate.source_sync",
        "okg.substrate.source_sync_convergence",
        "okg.substrate.source_sync_status",
        "okg.substrate.sources",
    ),
    "ingest_or_publish": (
        "okg.substrate.ingest",
        "okg.substrate.publisher",
    ),
    "cli_or_view": (
        "okg.substrate.cli",
        "okg.substrate.views",
        "okg.views",
        "deployments",
    ),
    "filesystem_or_environment": (
        "glob",
        "os",
        "pathlib",
        "shutil",
        "socket",
        "subprocess",
        "tempfile",
    ),
}
FORBIDDEN_BUILTINS = {
    "__builtins__": "unapproved_effect",
    "__import__": "dynamic_import",
    "breakpoint": "unapproved_effect",
    "compile": "unapproved_effect",
    "eval": "unapproved_effect",
    "exec": "unapproved_effect",
    "globals": "unapproved_effect",
    "input": "unapproved_effect",
    "locals": "unapproved_effect",
    "open": "filesystem_effect",
    "print": "unapproved_effect",
    "setattr": "unapproved_effect",
    "vars": "unapproved_effect",
}
FORBIDDEN_EFFECT_CALL_NAMES = frozenset(
    {
        "commit",
        "connect",
        "cursor",
        "execute",
        "executemany",
        "mkdir",
        "publish",
        "read_bytes",
        "read_text",
        "rename",
        "replace",
        "rollback",
        "run_ingest",
        "unlink",
        "write_bytes",
        "write_text",
    }
)
CLOCK_CALLS = frozenset(
    {
        "datetime.date.today",
        "datetime.datetime.now",
        "datetime.datetime.today",
        "datetime.datetime.utcnow",
        "datetime.now",
        "datetime.today",
        "datetime.utcnow",
        "time.time",
    }
)
POLICY_LITERAL_SENTINELS = frozenset(
    {
        "blocked_inventory",
        "needs_full_pipeline_audit",
        "passing_full_pipeline_benchmark",
        "run_no_change_and_single_delta_benchmarks",
        "okg.source_sync.microbench.optimization_ledger.rows.v1",
        "okg.source_sync.microbench.optimization_ledger.v1",
    }
)


@dataclass(frozen=True)
class Violation:
    code: str
    target: str
    line: int
    shape: str


@dataclass(frozen=True)
class ClockAccess:
    target: str
    line: int
    function: str | None
    node: ast.Call


def _parse(path: Path | str) -> ast.Module:
    path = Path(path)
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _dotted_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else None
    return None


def _relative_module(node: ast.ImportFrom, package: str) -> str:
    if node.level == 0:
        return node.module or ""
    parts = package.split(".")
    base = parts[: len(parts) - node.level + 1]
    if node.module:
        base.extend(node.module.split("."))
    return ".".join(base)


def _is_prefix(module: str, prefix: str) -> bool:
    return module == prefix or module.startswith(f"{prefix}.")


def _classify_module(module: str) -> str | None:
    for code, prefixes in FORBIDDEN_IMPORT_PREFIXES.items():
        if any(_is_prefix(module, prefix) for prefix in prefixes):
            return code
    return None if module in ALLOWED_OWNER_IMPORTS else "unapproved_import"


def _owner_import_and_effect_violations(source: str) -> list[Violation]:
    tree = ast.parse(source)
    violations: list[Violation] = []
    importlib_aliases = {"importlib"}

    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            code = FORBIDDEN_BUILTINS.get(node.id)
            if code:
                violations.append(
                    Violation(code, f"builtins.{node.id}", node.lineno, "builtin")
                )
        elif isinstance(node, ast.Import):
            for item in node.names:
                if item.name == "importlib":
                    importlib_aliases.add(item.asname or item.name)
                code = _classify_module(item.name)
                if code:
                    violations.append(Violation(code, item.name, node.lineno, "import"))
        elif isinstance(node, ast.ImportFrom):
            module = _relative_module(node, "okg.substrate")
            code = _classify_module(module)
            if code:
                violations.append(Violation(code, module, node.lineno, "from"))

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = _dotted_name(node.func) or ""
        if function in {"__import__", "importlib.import_module"} or (
            function.endswith(".import_module")
            and function.rsplit(".", 1)[0] in importlib_aliases
        ):
            target = (
                node.args[0].value
                if node.args and isinstance(node.args[0], ast.Constant)
                else None
            )
            code = (
                _classify_module(target) if isinstance(target, str) else None
            ) or "dynamic_import"
            violations.append(
                Violation(
                    code,
                    target if isinstance(target, str) else "<dynamic>",
                    node.lineno,
                    "dynamic",
                )
            )
        leaf = function.rsplit(".", 1)[-1]
        if leaf in FORBIDDEN_EFFECT_CALL_NAMES:
            violations.append(Violation("effect_call", function, node.lineno, "call"))
    return sorted(set(violations), key=lambda row: (row.line, row.target, row.code))


def _import_aliases(tree: ast.Module) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Import):
            for item in node.names:
                aliases[item.asname or item.name.split(".")[0]] = item.name
        elif isinstance(node, ast.ImportFrom):
            module = _relative_module(node, "okg.substrate")
            for item in node.names:
                aliases[item.asname or item.name] = f"{module}.{item.name}"
    return aliases


def _resolve_dotted(name: str, aliases: dict[str, str]) -> str:
    head, *tail = name.split(".")
    base = aliases.get(head, head)
    return ".".join([base, *tail])


class _ClockVisitor(ast.NodeVisitor):
    def __init__(self, aliases: dict[str, str]) -> None:
        self.aliases = aliases
        self.function_stack: list[str] = []
        self.accesses: list[ClockAccess] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_Call(self, node: ast.Call) -> None:
        dotted = _dotted_name(node.func)
        resolved = _resolve_dotted(dotted, self.aliases) if dotted else ""
        if resolved in CLOCK_CALLS:
            self.accesses.append(
                ClockAccess(
                    target=resolved,
                    line=node.lineno,
                    function=self.function_stack[-1] if self.function_stack else None,
                    node=node,
                )
            )
        self.generic_visit(node)


def _clock_accesses(tree: ast.Module) -> list[ClockAccess]:
    visitor = _ClockVisitor(_import_aliases(tree))
    visitor.visit(tree)
    return visitor.accesses


def _parents(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    return {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }


def _is_generated_at_none_check(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Compare)
        and isinstance(node.left, ast.Name)
        and node.left.id == "generated_at"
        and len(node.ops) == 1
        and isinstance(node.ops[0], (ast.Is, ast.IsNot))
        and len(node.comparators) == 1
        and isinstance(node.comparators[0], ast.Constant)
        and node.comparators[0].value is None
    )


def _clock_policy_violations(tree: ast.Module) -> list[str]:
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    builder = functions.get("build_source_optimization_ledger")
    if not isinstance(builder, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return ["missing builder"]
    kwonly_names = {argument.arg for argument in builder.args.kwonlyargs}
    violations: list[str] = []
    if "generated_at" not in kwonly_names:
        violations.append("generated_at is not keyword-only")

    accesses = _clock_accesses(tree)
    if len(accesses) != 1:
        violations.append(f"expected one clock access, found {len(accesses)}")
    parents = _parents(tree)
    for access in accesses:
        if access.function != "build_source_optimization_ledger":
            violations.append(
                f"clock access at line {access.line} is outside the builder"
            )
            continue
        if access.target != "datetime.datetime.now":
            violations.append(
                f"clock access at line {access.line} is {access.target}, not UTC now"
            )
        clock_arguments = [
            _resolve_dotted(dotted, _import_aliases(tree)) if dotted else ""
            for argument in access.node.args
            for dotted in [_dotted_name(argument)]
        ]
        if clock_arguments != ["datetime.timezone.utc"] or access.node.keywords:
            violations.append(
                f"clock access at line {access.line} does not use timezone.utc"
            )
        cursor: ast.AST | None = access.node
        guarded = False
        while cursor in parents:
            cursor = parents[cursor]
            if cursor is builder:
                break
            if isinstance(cursor, (ast.If, ast.IfExp)) and _is_generated_at_none_check(
                cursor.test
            ):
                guarded = True
                break
        if not guarded:
            violations.append(
                f"clock access at line {access.line} is not in generated_at is None branch"
            )
    return violations


def _top_level_names(tree: ast.Module) -> dict[str, list[ast.AST]]:
    names: dict[str, list[ast.AST]] = defaultdict(list)
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            names[node.name].append(node)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    names[target.id].append(node)
    return names


def _duplicate_moved_definitions(
    owner: ast.Module,
    facade: ast.Module,
) -> set[str]:
    owner_names = _top_level_names(owner)
    facade_names = _top_level_names(facade)
    return {
        symbol
        for symbol in MOVED_LEDGER_SYMBOLS
        if symbol in owner_names and symbol in facade_names
    }


def _facade_regrowth_literals(tree: ast.Module) -> set[str]:
    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value in POLICY_LITERAL_SENTINELS
    }


def _literal_all(tree: ast.Module) -> tuple[str, ...]:
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "__all__"
            for target in node.targets
        ):
            continue
        if isinstance(node.value, (ast.List, ast.Tuple)):
            return tuple(
                item.value
                for item in node.value.elts
                if isinstance(item, ast.Constant) and isinstance(item.value, str)
            )
    return ()


def _constant_strings(tree: ast.Module) -> dict[str, str]:
    values: dict[str, str] = {}

    def resolve(node: ast.AST) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name):
            return values.get(node.id)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = resolve(node.left)
            right = resolve(node.right)
            return left + right if left is not None and right is not None else None
        return None

    changed = True
    while changed:
        changed = False
        for node in tree.body:
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            value = resolve(node.value)
            if value is None:
                continue
            for target in targets:
                if isinstance(target, ast.Name) and values.get(target.id) != value:
                    values[target.id] = value
                    changed = True
    return values


def _is_facade_from_import(node: ast.ImportFrom) -> bool:
    module = node.module or ""
    return module == FACADE_MODULE or (
        node.level > 0 and module.endswith("source_sync_microbench")
    )


def _consumer_refs(path: Path) -> list[str]:
    """Find facade ledger imports, aliases, getattr calls, and strings."""
    tree = _parse(path)
    constants = _constant_strings(tree)
    module_aliases: set[str] = set()
    refs: list[str] = []
    label = (
        str(path.relative_to(REPO_ROOT))
        if path.is_relative_to(REPO_ROOT)
        else str(path)
    )

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for item in node.names:
                if item.name == FACADE_MODULE:
                    module_aliases.add(item.asname or item.name)
        elif isinstance(node, ast.ImportFrom):
            if _is_facade_from_import(node):
                for item in node.names:
                    if item.name in PUBLIC_LEDGER_SYMBOLS or item.name == "*":
                        refs.append(f"{label}:{node.lineno}:from:{item.name}")
            elif (node.module or "") == "okg.substrate":
                for item in node.names:
                    if item.name == "source_sync_microbench":
                        module_aliases.add(item.asname or item.name)

    prefix = f"{FACADE_MODULE}."
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if not isinstance(target, ast.Name):
                continue
            value = constants.get(target.id, "")
            if value.startswith(prefix):
                symbol = value.removeprefix(prefix)
                if symbol in PUBLIC_LEDGER_SYMBOLS:
                    refs.append(f"{label}:{node.lineno}:string:{symbol}")

    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            dotted = _dotted_name(node.value)
            if dotted not in module_aliases:
                continue
            for target in targets:
                if isinstance(target, ast.Name) and target.id not in module_aliases:
                    module_aliases.add(target.id)
                    changed = True

    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            dotted = _dotted_name(node)
            if not dotted:
                continue
            for alias in module_aliases:
                prefix = f"{alias}."
                if dotted.startswith(prefix):
                    symbol = dotted.removeprefix(prefix)
                    if symbol in PUBLIC_LEDGER_SYMBOLS:
                        refs.append(f"{label}:{node.lineno}:attr:{symbol}")
        elif isinstance(node, ast.Call) and _dotted_name(node.func) == "getattr":
            if len(node.args) >= 2:
                module_name = _dotted_name(node.args[0])
                symbol = (
                    node.args[1].value
                    if isinstance(node.args[1], ast.Constant)
                    else None
                )
                if module_name in module_aliases and symbol in PUBLIC_LEDGER_SYMBOLS:
                    refs.append(f"{label}:{node.lineno}:getattr:{symbol}")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            value = constants.get(node.value, node.value)
            if value.startswith(prefix):
                symbol = value.removeprefix(prefix)
                if symbol in PUBLIC_LEDGER_SYMBOLS:
                    refs.append(f"{label}:{node.lineno}:string:{symbol}")
    return sorted(set(refs))


def test_owner_exists_and_is_stdlib_only_without_effects() -> None:
    assert OWNER_PATH.is_file(), f"missing owner module: {OWNER_PATH}"
    source = OWNER_PATH.read_text(encoding="utf-8")
    assert _owner_import_and_effect_violations(source) == []


def test_clean_owner_import_does_not_load_effectful_surfaces() -> None:
    probe = """
import json
import sys

from okg.substrate.source_sync_optimization_ledger import (
    build_source_optimization_ledger,
)

ledger = build_source_optimization_ledger(
    {"sources": [], "enrichers": []},
    generated_at="2026-08-16T12:00:00+00:00",
)
forbidden = (
    "okg.substrate.source_sync_microbench",
    "okg.substrate.source_sync",
    "okg.substrate.source_sync_convergence",
    "okg.substrate.source_sync_status",
    "okg.substrate.sources",
    "okg.substrate.ingest",
    "okg.substrate.publisher",
    "okg.substrate.cli",
    "okg.substrate.runtime",
    "okg.views",
    "psycopg",
    "psycopg_pool",
    "dbos",
)
loaded = sorted(
    name
    for name in sys.modules
    if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
)
print(json.dumps({"loaded": loaded, "generated_at": ledger["generated_at"]}))
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC_ROOT)
    completed = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert json.loads(completed.stdout) == {
        "generated_at": "2026-08-16T12:00:00+00:00",
        "loaded": [],
    }


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("import okg.substrate.source_sync_microbench", "leaf_to_facade"),
        ("import psycopg", "database"),
        ("import dbos", "dbos_or_runtime"),
        ("from okg.substrate.ingest import run_ingest", "ingest_or_publish"),
        ("from pathlib import Path", "filesystem_or_environment"),
        ("open('/tmp/ledger')", "filesystem_effect"),
        ("connection.execute('SELECT 1')", "effect_call"),
        ("eval('1 + 1')", "unapproved_effect"),
    ],
)
def test_seeded_forbidden_import_or_effect_is_rejected(
    source: str,
    expected: str,
) -> None:
    codes = {
        violation.code for violation in _owner_import_and_effect_violations(source)
    }
    assert expected in codes


@pytest.mark.parametrize(
    "source",
    [
        "import importlib\nimportlib.import_module('okg.substrate.source_sync_microbench')",
        "__import__('okg.substrate.source_sync_microbench')",
        "import importlib\nname = 'okg.substrate.' + 'source_sync_microbench'\nimportlib.import_module(name)",
    ],
)
def test_seeded_dynamic_import_is_rejected(source: str) -> None:
    assert _owner_import_and_effect_violations(source)


def test_builder_has_only_the_named_generated_at_clock_branch() -> None:
    assert _clock_policy_violations(_parse(OWNER_PATH)) == []


def test_seeded_extra_or_unguarded_clock_access_is_rejected() -> None:
    valid = ast.parse(
        "from datetime import datetime, timezone\n"
        "def build_source_optimization_ledger(*, generated_at=None):\n"
        "    if generated_at is None:\n"
        "        generated_at = datetime.now(timezone.utc).isoformat()\n"
        "    return generated_at\n"
    )
    assert _clock_policy_violations(valid) == []

    extra = ast.parse(
        ast.unparse(valid) + "\ndef hidden_clock():\n"
        "    return datetime.now(timezone.utc).isoformat()\n"
    )
    assert _clock_policy_violations(extra)

    unguarded = ast.parse(
        "from datetime import datetime, timezone\n"
        "def build_source_optimization_ledger(*, generated_at=None):\n"
        "    return datetime.now(timezone.utc).isoformat()\n"
    )
    assert _clock_policy_violations(unguarded)


def test_ledger_policy_has_one_owner_and_no_facade_regrowth() -> None:
    owner = _parse(OWNER_PATH)
    facade = _parse(FACADE_PATH)
    assert _duplicate_moved_definitions(owner, facade) == set()
    assert _facade_regrowth_literals(facade) == set()


def test_seeded_duplicate_and_literal_regrowth_are_rejected() -> None:
    owner = ast.parse("def _optimization_status():\n    pass\n")
    duplicate = ast.parse("def _optimization_status():\n    pass\n")
    assert _duplicate_moved_definitions(owner, duplicate) == {"_optimization_status"}
    literal = ast.parse("def renamed_policy():\n    return 'blocked_inventory'\n")
    assert _facade_regrowth_literals(literal) == {"blocked_inventory"}


def test_owner_and_reduced_facade_line_budgets() -> None:
    assert len(OWNER_PATH.read_text(encoding="utf-8").splitlines()) <= 1150
    assert len(FACADE_PATH.read_text(encoding="utf-8").splitlines()) <= 3500


def test_facade_exports_are_exact_owner_aliases() -> None:
    owner = importlib.import_module(OWNER_MODULE)
    facade = importlib.import_module(FACADE_MODULE)
    for symbol in PUBLIC_LEDGER_SYMBOLS:
        assert getattr(facade, symbol) is getattr(owner, symbol), symbol
    assert tuple(facade.__all__) == EXPECTED_FACADE_ALL
    assert "rank_source_optimization_rows" not in facade.__all__
    assert facade.rank_source_optimization_rows is owner.rank_source_optimization_rows


def test_seeded_facade_consumer_detector_catches_aliases_and_strings(
    tmp_path: Path,
) -> None:
    fixture = tmp_path / "consumer.py"
    fixture.write_text(
        "import okg.substrate.source_sync_microbench as facade\n"
        "from okg.substrate import source_sync_microbench as package_facade\n"
        "from okg.substrate.source_sync_microbench import "
        "MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA\n"
        "alias = facade\n"
        "alias.build_source_optimization_ledger({})\n"
        "package_facade.rank_source_optimization_rows([])\n"
        "getattr(alias, 'source_optimization_ledger_relation_rows')({})\n"
        "PREFIX = 'okg.substrate.source_sync_microbench.'\n"
        "TARGET = PREFIX + 'build_source_optimization_ledger'\n",
        encoding="utf-8",
    )
    refs = _consumer_refs(fixture)
    assert any(":from:MICROBENCH_OPTIMIZATION_LEDGER_SCHEMA" in ref for ref in refs)
    assert any(":attr:build_source_optimization_ledger" in ref for ref in refs)
    assert any(":attr:rank_source_optimization_rows" in ref for ref in refs)
    assert any(
        ":getattr:source_optimization_ledger_relation_rows" in ref for ref in refs
    )
    assert any(":string:build_source_optimization_ledger" in ref for ref in refs)


def test_zero_ledger_facade_consumers(record_property) -> None:
    record_property(
        "requirements",
        "req.source-sync-optimization-ledger.compatibility "
        "test.source-sync-optimization-ledger.zero-consumers",
    )
    allowed = {
        OWNER_PATH.resolve(),
        FACADE_PATH.resolve(),
        COMPATIBILITY_PATH.resolve(),
        Path(__file__).resolve(),
    }
    candidates = [
        *tracked_files(SRC_ROOT, "*.py"),
        *tracked_files(DEPLOYMENTS_ROOT, "*.py"),
        *tracked_files(TEST_ROOT, "*.py"),
    ]
    violations = [
        ref
        for path in candidates
        if path.resolve() not in allowed
        for ref in _consumer_refs(path)
    ]
    assert violations == [], "ledger-only compatibility consumers remain: " + "; ".join(
        violations
    )
