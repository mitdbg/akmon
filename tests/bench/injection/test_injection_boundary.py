"""The engine must not contain, or reach into, the corruption injector.

``okg.bench.injection`` plants wrong values into a corpus on purpose. It is
measurement machinery: the benchmark may read the engine, the engine may not
read the corrupter. That direction used to be held by convention, and
convention is exactly what decays the first time a fix is convenient — so it
is held here instead.

Three independent guards, because they fail for different reasons:

* :func:`test_no_injection_package_under_the_substrate` catches the package
  being re-created at its old address.
* :func:`test_no_substrate_module_imports_the_injector` catches a substrate
  module reaching across the boundary by import.
* :func:`test_importing_the_substrate_does_not_load_the_injector` is the
  property the other two exist to protect, checked at runtime rather than by
  reading source: a process that imports the engine has not loaded any
  corruption code.

THE ONE SANCTIONED REFERENCE. ``okg bench`` is the substrate CLI's generic
entry point into the shipped ``okg.bench`` harness; ``bench inject`` and
``bench synth`` are dispatched there identically, each by a deferred,
function-scope import. That dispatcher is allowlisted below — by path, for
function-scope imports only. A module-scope import there fails like anywhere
else, so the deferred-import property (guard three) cannot be lost silently.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SUBSTRATE = _REPO_ROOT / "src" / "okg" / "substrate"

_INJECTOR = "okg.bench.injection"

# The generic `okg bench <verb>` dispatcher. Function-scope imports only.
_CLI_DISPATCHER = _SUBSTRATE / "cli" / "main.py"


def _substrate_modules() -> list[Path]:
    paths = [
        p for p in sorted(_SUBSTRATE.rglob("*.py"))
        if "__pycache__" not in p.parts
    ]
    assert len(paths) > 100, f"substrate scan found only {len(paths)} modules"
    return paths


def _imported_names(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if isinstance(node, ast.ImportFrom):
        return [node.module] if node.module else []
    return []


def _targets_injector(name: str) -> bool:
    return name == _INJECTOR or name.startswith(_INJECTOR + ".")


def test_no_injection_package_under_the_substrate() -> None:
    """No module or package named `injection` may live under the engine."""
    offenders = [
        str(p.relative_to(_REPO_ROOT))
        for p in _SUBSTRATE.rglob("injection*")
        if "__pycache__" not in p.parts
        and (p.is_dir() or p.suffix == ".py")
    ]
    assert not offenders, (
        "corruption machinery reappeared under the substrate: "
        f"{offenders}. The injector belongs under src/okg/bench/injection/; "
        "the engine must not ship code whose purpose is to damage data."
    )


def test_no_substrate_module_imports_the_injector() -> None:
    """No engine module may import the corrupter at module scope, and only
    the generic `okg bench` CLI dispatcher may import it at all."""
    module_scope: list[str] = []
    deferred_elsewhere: list[str] = []

    for path in _substrate_modules():
        tree = ast.parse(path.read_text(), filename=str(path))
        rel = str(path.relative_to(_REPO_ROOT))

        top_level = {id(n) for n in tree.body}
        for node in ast.walk(tree):
            if not any(_targets_injector(n) for n in _imported_names(node)):
                continue
            where = f"{rel}:{node.lineno}"
            if id(node) in top_level:
                module_scope.append(where)
            elif path != _CLI_DISPATCHER:
                deferred_elsewhere.append(where)

    assert not module_scope, (
        "substrate module(s) import the corruption injector at module scope: "
        f"{module_scope}. Benchmark code may depend on the engine; the engine "
        "may not depend on benchmark code."
    )
    assert not deferred_elsewhere, (
        "substrate module(s) outside the generic `okg bench` CLI dispatcher "
        f"reach into the corruption injector: {deferred_elsewhere}. Only "
        f"{_CLI_DISPATCHER.relative_to(_REPO_ROOT)} may dispatch into "
        "okg.bench, and only by deferred import."
    )


def test_importing_the_substrate_does_not_load_the_injector() -> None:
    """The property the source guards protect: importing the engine — up to
    and including its CLI — loads no corruption code into the process."""
    probe = (
        "import sys\n"
        "import okg.substrate\n"
        "import okg.substrate.cli.main\n"
        f"leaked = sorted(m for m in sys.modules if m.startswith({_INJECTOR!r}))\n"
        "print(repr(leaked))\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True, text=True, cwd=_REPO_ROOT, check=True,
    )
    assert proc.stdout.strip() == "[]", (
        "importing the substrate pulled corruption machinery into the "
        f"process: {proc.stdout.strip()}"
    )
