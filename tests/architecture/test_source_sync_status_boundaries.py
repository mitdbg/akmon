"""Static guards for the pure source-sync status boundary.

The owner is inspected as source rather than imported here.  This keeps the
guard useful when an effectful facade import is accidentally reintroduced and
lets the seeded checks prove that the guard catches the intended regressions.
"""
from __future__ import annotations

import ast
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import os
import subprocess
import sys

import pytest
from tests.pytest.tracked_files import tracked_files


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
TEST_ROOT = REPO_ROOT / "tests"
SUBSTRATE_ROOT = SRC_ROOT / "okg" / "substrate"
OWNER_PATH = SUBSTRATE_ROOT / "source_sync_status.py"
FACADE_PATH = SUBSTRATE_ROOT / "source_sync_producers.py"
OWNER_MODULE = "okg.substrate.source_sync_status"
FACADE_MODULE = "okg.substrate.source_sync_producers"
CONVERGENCE_MODULE = "okg.substrate.source_sync_convergence"
COMPATIBILITY_PATH = TEST_ROOT / "substrate" / "test_source_sync_status_boundary.py"

STATUS_SYMBOLS = frozenset(
    {
        "DEFAULT_PRODUCER_HEALTH_MAX_AGE_SECONDS",
        "SOURCE_SYNC_STATUS_SCHEMA",
        "_SOURCE_SYNC_STALE_BACKLOG_SECONDS",
        "build_source_sync_status_contract",
        "source_sync_status_for_source",
        "expected_source_sync_producers",
        "source_sync_producer_expectations",
        "_expected_source_sync_producers",
        "_append_expected_or_disabled",
        "_runtime_producer_enabled",
        "_producer_liveness_impact",
        "_readiness_required_source_names",
        "_producer_row_is_required",
        "_desired_frontiers_contract",
        "_runtime_liveness_contract",
        "_producer_kind_contract",
        "_runtime_worker_contract",
        "_runtime_service_view",
        "_runtime_service_down",
        "_runtime_service_status",
        "_source_freshness_contract",
        "_source_freshness_for_source",
        "_list_of_mappings",
        "_optional_number",
        "_rollup_status",
        "_producer_observed_at",
        "_parse_datetime",
        "_age_seconds",
    }
)
# These are the deliberately retained effectful facade imports.  Their
# acquisition/transport behavior remains in source_sync_producers; only the
# supplied-input policy symbols above must leave that module's consumers.
EFFECTFUL_FACADE_SYMBOLS = frozenset(
    {
        "handle_webhook_payload",
        "make_webhook_http_server",
        "record_source_sync_producer_health",
        "run_webhook_http_server",
        "run_poll_once",
        "source_sync_producer_health",
        "webhook_events_from_payload",
        "_run_probe",
        "_source_sync_dbos_queue_status",
        "_source_sync_dbos_recoverable_terminal_error",
    }
)

ALLOWED_OWNER_IMPORTS = frozenset(
    {
        "__future__",
        "collections.abc",
        "dataclasses",
        "datetime",
        "enum",
        "functools",
        "itertools",
        "math",
        "typing",
        "typing_extensions",
        "okg.substrate.source_sync",
        "okg.substrate.runtime.health_policy",
    }
)
ALLOWED_CONVERGENCE_MEMBERS = frozenset(
    {"build_convergence_status", "source_convergence_for_source"}
)
FORBIDDEN_IMPORT_PREFIXES: dict[str, tuple[str, ...]] = {
    "leaf_to_facade": (FACADE_MODULE,),
    "database": ("psycopg", "psycopg_pool", "okg.substrate.db"),
    "http": ("http", "requests", "httpx", "aiohttp", "okg.substrate.source_sync_webhook"),
    "polling": (
        "okg.substrate.source_sync_poll",
        "okg.substrate.runtime.source_poll_schedule",
        "okg.substrate.ingest.runner",
        "okg.substrate.watch",
    ),
    "dbos_storage": (
        "dbos",
        "okg.substrate.source_sync_enqueue",
        "okg.substrate.runtime.source_sync_workflow",
        "okg.substrate.runtime.partition_queue",
    ),
    "runtime_services": (
        "okg.substrate.runtime.services",
        "okg.substrate.runtime.publisher_health",
        "okg.substrate.environment_services",
    ),
    "environment_or_filesystem": (
        "os",
        "pathlib",
        "glob",
        "shutil",
        "subprocess",
        "tempfile",
        "socket",
        "okg.substrate.deployments",
        "okg.substrate.runtime.config",
        "okg.substrate.environment_exec",
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
    "open": "filesystem_or_process",
    "vars": "unapproved_effect",
}
CLOCK_CALLS = {
    "datetime.date.today",
    "datetime.datetime.now",
    "datetime.datetime.utcnow",
    "time.time",
}


@dataclass(frozen=True)
class Violation:
    code: str
    target: str
    line: int
    shape: str


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
    if module == CONVERGENCE_MODULE:
        return "convergence_module_not_allowed"
    return None if module in ALLOWED_OWNER_IMPORTS else "unapproved_import"


def _owner_import_violations(source: str) -> list[Violation]:
    tree = ast.parse(source)
    violations: list[Violation] = []
    aliases: dict[str, str] = {}
    clock_aliases: dict[str, str] = {}
    importlib_aliases = {"importlib"}

    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            code = FORBIDDEN_BUILTINS.get(node.id)
            if code:
                violations.append(Violation(code, f"builtins.{node.id}", node.lineno, "builtin"))
        if isinstance(node, ast.Import):
            for item in node.names:
                aliases[item.asname or item.name.split(".")[0]] = item.name
                if item.name == "importlib":
                    importlib_aliases.add(item.asname or item.name)
                if item.name in {"datetime", "time"}:
                    clock_aliases[item.asname or item.name] = item.name
                code = _classify_module(item.name)
                if code:
                    violations.append(Violation(code, item.name, node.lineno, "import"))
            continue
        if not isinstance(node, ast.ImportFrom):
            continue
        module = _relative_module(node, "okg.substrate")
        if module in {"datetime", "time"}:
            for item in node.names:
                clock_aliases[item.asname or item.name] = f"{module}.{item.name}"
        if module == CONVERGENCE_MODULE:
            if node in tree.body:
                violations.append(Violation("convergence_import_not_local", module, node.lineno, "from-module-scope"))
            for item in node.names:
                if item.name not in ALLOWED_CONVERGENCE_MEMBERS:
                    violations.append(Violation("convergence_member_not_allowed", f"{module}.{item.name}", node.lineno, "from-member"))
            continue
        code = _classify_module(module)
        if code:
            violations.append(Violation(code, module, node.lineno, "from"))
        for item in node.names:
            aliases[item.asname or item.name] = f"{module}.{item.name}"

    def resolve(name: str) -> str:
        head, *tail = name.split(".")
        base = aliases.get(head, head)
        return ".".join([base, *tail])

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            function = _dotted_name(node.func)
            if function in {"__import__", "importlib.import_module"} or (
                function and function.endswith(".import_module") and function.rsplit(".", 1)[0] in importlib_aliases
            ):
                target = (
                    node.args[0].value
                    if node.args and isinstance(node.args[0], ast.Constant)
                    else None
                )
                if isinstance(target, str):
                    code = _classify_module(target) or "dynamic_import"
                    violations.append(Violation(code, target, node.lineno, "dynamic"))
                else:
                    violations.append(Violation("dynamic_import", "<dynamic>", node.lineno, "dynamic"))
            resolved = resolve(function) if function else ""
            if resolved in CLOCK_CALLS:
                violations.append(Violation("clock_access", resolved, node.lineno, "call"))
        elif isinstance(node, ast.Attribute):
            dotted = _dotted_name(node)
            if dotted and resolve(dotted) in CLOCK_CALLS:
                violations.append(Violation("clock_access", resolve(dotted), node.lineno, "attribute"))
    return sorted(set(violations), key=lambda row: (row.line, row.target, row.code))


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


def _duplicate_moved_definitions(owner: ast.Module, facade: ast.Module) -> dict[str, tuple[str, str]]:
    owner_names = _top_level_names(owner)
    facade_names = _top_level_names(facade)
    return {
        symbol: (OWNER_MODULE, FACADE_MODULE)
        for symbol in STATUS_SYMBOLS
        if symbol in owner_names and symbol in facade_names
    }


def _string_constants(tree: ast.Module) -> dict[str, str]:
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

    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            value = resolve(node.value)
            if value is not None:
                for target in targets:
                    if isinstance(target, ast.Name):
                        values[target.id] = value
    return values


def _consumer_refs(path: Path) -> list[str]:
    """Find moved-symbol facade references, including qualified strings."""
    tree = _parse(path)
    constants = _string_constants(tree)
    module_aliases: set[str] = set()
    refs: list[str] = []
    label = str(path.relative_to(REPO_ROOT)) if path.is_relative_to(REPO_ROOT) else str(path)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for item in node.names:
                if item.name == FACADE_MODULE:
                    module_aliases.add(item.asname or "source_sync_producers")
        elif isinstance(node, ast.ImportFrom) and (node.module or "") == "okg.substrate":
            for item in node.names:
                if item.name == "source_sync_producers":
                    module_aliases.add(item.asname or item.name)
        elif isinstance(node, ast.ImportFrom) and (node.module or "") == FACADE_MODULE:
            for item in node.names:
                if item.name in STATUS_SYMBOLS or item.name == "*":
                    refs.append(f"{label}:{node.lineno}:from:{item.name}")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            value = constants.get(node.value, node.value)
            prefix = f"{FACADE_MODULE}."
            if value.startswith(prefix) and value.removeprefix(prefix) in STATUS_SYMBOLS:
                        refs.append(f"{label}:{node.lineno}:string:{value.removeprefix(prefix)}")
        elif isinstance(node, ast.Attribute):
            dotted = _dotted_name(node)
            if dotted:
                for alias in module_aliases:
                    if dotted.startswith(f"{alias}.") and dotted.removeprefix(f"{alias}.") in STATUS_SYMBOLS:
                        refs.append(f"{label}:{node.lineno}:attr:{dotted.removeprefix(f'{alias}.')}")
        elif isinstance(node, ast.Assign) and isinstance(node.value, ast.Name):
            if node.value.id in module_aliases:
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        module_aliases.add(target.id)
    return sorted(set(refs))


def _facade_import_symbols(path: Path) -> set[str]:
    tree = _parse(path)
    symbols: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "") == FACADE_MODULE:
            symbols.update(item.name for item in node.names)
    return symbols


def _facade_all(path: Path) -> tuple[str, ...]:
    tree = _parse(path)
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets):
            continue
        if isinstance(node.value, (ast.List, ast.Tuple)):
            return tuple(item.value for item in node.value.elts if isinstance(item, ast.Constant) and isinstance(item.value, str))
    return ()


def test_owner_exists_and_has_exact_pure_import_policy() -> None:
    assert OWNER_PATH.is_file(), f"missing owner module: {OWNER_PATH}"
    assert _owner_import_violations(OWNER_PATH.read_text(encoding="utf-8")) == []


def test_clean_owner_import_does_not_load_effectful_modules() -> None:
    probe = (
        "import sys\n"
        "from okg.substrate.source_sync_status import build_source_sync_status_contract\n"
        "status = build_source_sync_status_contract({\n"
        "    'deployment': 'demo',\n"
        "    'expected_producers': [],\n"
        "    'disabled_producers': [],\n"
        "    'producers': [],\n"
        "    'missing_producers': [],\n"
        "    'stale_producers': [],\n"
        "    'config_warnings': [],\n"
        "    'dbos_queue_depth': {'available': True, 'active_count': 0},\n"
        "    'dlq_count': 0,\n"
        "})\n"
        "assert status['schema'] == 'okg.source_sync.status.v1'\n"
        "for name in ('okg.substrate.source_sync_producers', 'psycopg', 'psycopg_pool', 'dbos', "
        "'okg.substrate.ingest', 'okg.substrate.ingest.runner', 'okg.substrate.ingest.service', "
        "'okg.substrate.publisher', 'okg.substrate.publisher_core', 'okg.substrate.runtime.publisher', "
        "'okg.substrate.runtime.publisher_health', 'okg.substrate.source_sync_enqueue', "
        "'okg.substrate.runtime.bootstrap', 'okg.substrate.runtime.services', "
        "'okg.substrate.runtime.worker', 'okg.substrate.runtime.source_sync_workflow', "
        "'okg.substrate.watch', 'okg.substrate.watch.watcher', 'http', 'http.server', 'http.client'):\n"
        "    assert name not in sys.modules, name\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC_ROOT)
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr or result.stdout


def test_runtime_package_lazy_exports_preserve_purity_and_compatibility() -> None:
    """Check runtime introspection before and after lazy bootstrap access.

    These are deliberately separate subprocesses: importing a bootstrap export
    is expected to load DBOS/psycopg, so it must not weaken the pre-access
    purity assertion.
    """
    runtime_exports = (
        "BootstrapResult",
        "RUNTIME_BLOCK_DEFAULTS",
        "RuntimeConfig",
        "bootstrap_dbos_schema",
        "is_bootstrapped",
    )
    purity_probe = (
        "import sys\n"
        "import okg.substrate.runtime as runtime\n"
        f"expected = {runtime_exports!r}\n"
        "assert tuple(runtime.__all__) == expected\n"
        "assert set(expected) <= set(dir(runtime))\n"
        "assert 'okg.substrate.runtime.bootstrap' not in sys.modules\n"
        "assert 'psycopg' not in sys.modules\n"
        "assert 'dbos' not in sys.modules\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC_ROOT)
    purity = subprocess.run(
        [sys.executable, "-c", purity_probe],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert purity.returncode == 0, purity.stderr or purity.stdout

    compatibility_probe = (
        "import sys\n"
        "import okg.substrate.runtime as runtime\n"
        "from okg.substrate.runtime import RUNTIME_BLOCK_DEFAULTS, RuntimeConfig\n"
        "from okg.substrate.runtime.config import RUNTIME_BLOCK_DEFAULTS as config_defaults, RuntimeConfig as ConfigClass\n"
        "assert RUNTIME_BLOCK_DEFAULTS is config_defaults\n"
        "assert RuntimeConfig is ConfigClass\n"
        "from okg.substrate.runtime import BootstrapResult, bootstrap_dbos_schema, is_bootstrapped\n"
        "from okg.substrate.runtime import bootstrap as bootstrap_module\n"
        "assert BootstrapResult is bootstrap_module.BootstrapResult\n"
        "assert bootstrap_dbos_schema is bootstrap_module.bootstrap_dbos_schema\n"
        "assert is_bootstrapped is bootstrap_module.is_bootstrapped\n"
        "assert runtime.BootstrapResult is bootstrap_module.BootstrapResult\n"
        "assert runtime.bootstrap_dbos_schema is bootstrap_module.bootstrap_dbos_schema\n"
        "assert runtime.is_bootstrapped is bootstrap_module.is_bootstrapped\n"
        "star = {}\n"
        "exec('from okg.substrate.runtime import *', star)\n"
        f"expected = {runtime_exports!r}\n"
        "assert tuple(runtime.__all__) == expected\n"
        "for name in expected:\n"
        "    assert star[name] is getattr(runtime, name), name\n"
        "assert 'okg.substrate.runtime.bootstrap' in sys.modules\n"
    )
    compatibility = subprocess.run(
        [sys.executable, "-c", compatibility_probe],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert compatibility.returncode == 0, compatibility.stderr or compatibility.stdout


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("import okg.substrate.source_sync_producers", "leaf_to_facade"),
        ("from okg.substrate.source_sync_producers import build_source_sync_status_contract", "leaf_to_facade"),
        ("import psycopg", "database"),
        ("from http.server import BaseHTTPRequestHandler", "http"),
        ("import dbos", "dbos_storage"),
        ("from okg.substrate.runtime.services import check", "runtime_services"),
        ("from pathlib import Path", "environment_or_filesystem"),
        ("open('/tmp/escape')", "filesystem_or_process"),
        ("eval('1 + 1')", "unapproved_effect"),
    ],
)
def test_seeded_owner_effect_or_facade_import_is_rejected(source: str, expected: str) -> None:
    assert expected in {violation.code for violation in _owner_import_violations(source)}


@pytest.mark.parametrize(
    "source",
    [
        "import importlib\nimportlib.import_module('okg.substrate.source_sync_producers')",
        "__import__('okg.substrate.source_sync_producers')",
        "import importlib\nMODULE = 'okg.substrate.' + 'source_sync_producers'\nimportlib.import_module(MODULE)",
    ],
)
def test_seeded_dynamic_owner_facade_import_is_rejected(source: str) -> None:
    # Literal and non-literal dynamic imports must both be treated as effects.
    violations = _owner_import_violations(source)
    assert any(violation.code in {"leaf_to_facade", "dynamic_import"} for violation in violations)


def test_seeded_clock_access_queue_and_database_guards_are_rejected() -> None:
    snippets = (
        "from datetime import datetime\nnow = datetime.now()",
        "import datetime\nnow = datetime.datetime.utcnow()",
        "from time import time\nnow = time()",
        "from okg.substrate.runtime.partition_queue import source_sync_queue_name",
        "from okg.substrate.ingest.runner import SourceRunner",
        "import psycopg",
    )
    violations = [_owner_import_violations(snippet) for snippet in snippets]
    assert any(any(row.code == "clock_access" for row in rows) for rows in violations[:3])
    assert "dbos_storage" in {row.code for row in violations[3]}
    assert "polling" in {row.code for row in violations[4]}
    assert "database" in {row.code for row in violations[5]}


def test_only_exact_convergence_members_are_allowed() -> None:
    allowed = _owner_import_violations(
        "def project():\n    from okg.substrate.source_sync_convergence import build_convergence_status, source_convergence_for_source\n"
    )
    assert allowed == []
    assert any(row.code == "convergence_import_not_local" for row in _owner_import_violations(
        "from okg.substrate.source_sync_convergence import build_convergence_status"
    ))
    assert any(row.code == "convergence_member_not_allowed" for row in _owner_import_violations(
        "def project():\n    from okg.substrate.source_sync_convergence import source_inventory_for_registry\n"
    ))


def test_owner_has_no_duplicate_moved_definitions() -> None:
    assert _duplicate_moved_definitions(_parse(OWNER_PATH), _parse(FACADE_PATH)) == {}


def test_seeded_duplicate_moved_definition_is_rejected() -> None:
    owner = ast.parse("def build_source_sync_status_contract():\n    pass\n")
    facade = ast.parse("def build_source_sync_status_contract():\n    pass\n")
    duplicates = _duplicate_moved_definitions(owner, facade)
    assert "build_source_sync_status_contract" in duplicates


def test_owner_and_facade_line_budgets_are_enforced() -> None:
    assert len(OWNER_PATH.read_text(encoding="utf-8").splitlines()) <= 1100
    assert len(FACADE_PATH.read_text(encoding="utf-8").splitlines()) <= 2600


def test_facade_all_and_direct_exports_are_preserved() -> None:
    expected = (
        "DEFAULT_PRODUCER_HEALTH_MAX_AGE_SECONDS",
        "SOURCE_SYNC_STATUS_SCHEMA",
        "build_source_sync_status_contract",
        "expected_source_sync_producers",
        "handle_webhook_payload",
        "make_webhook_http_server",
        "record_source_sync_producer_health",
        "run_webhook_http_server",
        "run_poll_once",
        "source_sync_producer_health",
        "source_sync_status_for_source",
        "webhook_events_from_payload",
    )
    assert _facade_all(FACADE_PATH) == expected


def test_characterization_is_the_only_intentional_facade_consumer() -> None:
    refs = _consumer_refs(COMPATIBILITY_PATH)
    assert any(":from:SOURCE_SYNC_STATUS_SCHEMA" in ref for ref in refs)
    assert any(":from:build_source_sync_status_contract" in ref for ref in refs)
    assert any(":from:source_sync_status_for_source" in ref for ref in refs)


def test_seeded_status_facade_consumer_detector_catches_alias_and_qualified_strings(tmp_path: Path) -> None:
    fixture = tmp_path / "consumer.py"
    fixture.write_text(
        "import okg.substrate.source_sync_producers as producers\n"
        "from okg.substrate import source_sync_producers as package_producers\n"
        "from okg.substrate.source_sync_producers import SOURCE_SYNC_STATUS_SCHEMA\n"
        "alias = producers\n"
        "alias.build_source_sync_status_contract({})\n"
        "package_producers.source_sync_status_for_source({}, 'docs')\n"
        "TARGET = 'okg.substrate.source_sync_producers.build_source_sync_status_contract'\n",
        encoding="utf-8",
    )
    refs = _consumer_refs(fixture)
    assert any(":from:SOURCE_SYNC_STATUS_SCHEMA" in ref for ref in refs)
    assert any(":attr:build_source_sync_status_contract" in ref for ref in refs)
    assert any(":attr:source_sync_status_for_source" in ref for ref in refs)
    assert any(":string:build_source_sync_status_contract" in ref for ref in refs)


def test_zero_source_sync_status_facade_consumers(record_property) -> None:
    record_property(
        "requirements",
        "req.source-sync-status.compatibility "
        "test.source-sync-status.zero-consumers",
    )
    candidates = [*tracked_files(SRC_ROOT, "*.py"), *tracked_files(TEST_ROOT, "*.py")]
    violations: list[str] = []
    own_paths = {FACADE_PATH.resolve(), OWNER_PATH.resolve(), COMPATIBILITY_PATH.resolve(), Path(__file__).resolve()}
    for path in candidates:
        if path.resolve() in own_paths:
            continue
        imported = _facade_import_symbols(path)
        assert imported <= STATUS_SYMBOLS | EFFECTFUL_FACADE_SYMBOLS, (
            f"{path}: facade import is not allowlisted: "
            f"{sorted(imported - STATUS_SYMBOLS - EFFECTFUL_FACADE_SYMBOLS)}"
        )
        unexpected = imported - EFFECTFUL_FACADE_SYMBOLS
        assert not (unexpected & STATUS_SYMBOLS), (
            f"{path}: moved status imports must use {OWNER_MODULE}: "
            f"{sorted(unexpected & STATUS_SYMBOLS)}"
        )
        violations.extend(_consumer_refs(path))
    assert violations == [], "status-only compatibility consumers remain: " + "; ".join(violations)
