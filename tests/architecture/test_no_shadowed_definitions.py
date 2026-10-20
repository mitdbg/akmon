"""Refuse a second `def` that silently shadows an earlier one in the same scope.

A merge can compose this defect from two correct sides: one branch adds a method,
another branch adds a method of the same name to the same class, and the textual
merge keeps both. Python binds the LAST definition, so the earlier body becomes
dead code and every caller written against its signature either crashes or
silently changes behavior. Three live instances of exactly that were repaired
alongside this test:

* ``Publisher._evaluate_consistency_gates`` was defined twice with incompatible
  signatures. ``Publisher.run_gates`` called the earlier one positionally, so the
  call raised ``TypeError`` against the definition that actually won.
* ``okg.substrate.pact.cli._looks_like_command`` was defined twice with different
  prefix sets. The winning copy dropped version-control and database commands
  from the operator brief and dropped the ``.strip()`` that keeps indented prose
  from rendering as a runnable command.
* ``derived_artifacts.scheduler._persist_bindings_in_cursor`` had a
  byte-identical 68-line second copy. This test found that one.

Ruff's F811 does not report any of them: it is scoped to redefinition of an
unused name within a single module namespace and skips class bodies, so this
needs its own AST pass.

Overload and descriptor pairs are legitimate repeats of a name and are excluded
by decorator.
"""

from __future__ import annotations

import ast
from pathlib import Path
from tests.pytest.tracked_files import tracked_files

REPO_ROOT = Path(__file__).resolve().parents[2]

# Every tree this repository authors and imports. `deployments/` earns its place
# for size (its coordination module alone is 9000+ lines) and `tests/` because a
# shadowed test function is a test that silently stopped running.
SOURCE_ROOTS = (
    REPO_ROOT / "src" / "okg",
    REPO_ROOT / "scripts",
    REPO_ROOT / "deployments",
    REPO_ROOT / "tests",
)

# Trees that are vendored, archived, or generated: not ours to fix.
EXCLUDED_PARTS = frozenset({"archive", "vendor", "node_modules", ".venv"})

# Decorators under which repeating a name is the documented Python idiom rather
# than a shadowed definition.
BENIGN_DECORATORS = frozenset(
    {"overload", "setter", "deleter", "getter", "register"},
)


def _decorator_roots(node: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    """Leading identifier of each decorator (``x.setter`` -> ``x``, and ``setter``)."""
    roots: set[str] = set()
    for decorator in node.decorator_list:
        current: ast.expr = decorator
        if isinstance(current, ast.Call):
            current = current.func
        while isinstance(current, ast.Attribute):
            roots.add(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            roots.add(current.id)
    return roots


def _is_benign(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    return bool(_decorator_roots(node) & BENIGN_DECORATORS)


def _shadowed_definitions(tree: ast.AST) -> list[tuple[str, int, int]]:
    """(name, first line, shadowing line) for each scope-local redefinition.

    Classes count too: a second `class C` in one scope shadows the first exactly
    as a second `def` does.

    KNOWN LIMIT, stated rather than implied: only sibling statements in one
    body are compared. A name defined once in an `if` branch and once in its
    `else`, or once at module scope and once inside a `try`, is a different
    construct -- often deliberate -- and is not reported here.
    """
    found: list[tuple[str, int, int]] = []
    scopes: list[ast.AST] = [tree]
    while scopes:
        scope = scopes.pop()
        seen: dict[str, int] = {}
        for child in getattr(scope, "body", []):
            if isinstance(
                child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
            ):
                if isinstance(child, ast.ClassDef):
                    previous = seen.get(child.name)
                    if previous is not None:
                        found.append((child.name, previous, child.lineno))
                    seen[child.name] = child.lineno
                elif _is_benign(child):
                    # An overload/descriptor group is not a binding this pass
                    # can reason about; stop tracking the name inside it.
                    seen.pop(child.name, None)
                else:
                    previous = seen.get(child.name)
                    if previous is not None:
                        found.append((child.name, previous, child.lineno))
                    seen[child.name] = child.lineno
            if isinstance(
                child,
                (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
            ):
                scopes.append(child)
    return found


def _python_sources() -> list[Path]:
    paths: list[Path] = []
    for root in SOURCE_ROOTS:
        assert root.is_dir(), f"declared source root is missing: {root}"
        # Tracked files, not the disk: a parallel test's scratch file can be
        # listed by a disk walk and deleted before it is read (dev Tests run
        # 36370525406), and an untracked file is not code this repo authors.
        paths.extend(
            path
            for path in tracked_files(root, "*.py")
            if not EXCLUDED_PARTS.intersection(path.parts)
        )
    return sorted(paths)


def test_no_definition_shadows_an_earlier_one_in_the_same_scope() -> None:
    offenders: list[str] = []
    scanned = 0
    for path in _python_sources():
        source = path.read_text(encoding="utf-8")
        try:
            tree = ast.parse(source)
        except SyntaxError as exc:  # pragma: no cover - a parse break is its own red
            raise AssertionError(f"{path} does not parse: {exc}") from exc
        scanned += 1
        relative = path.relative_to(REPO_ROOT)
        for name, first, shadowing in _shadowed_definitions(tree):
            offenders.append(
                f"{relative}:{shadowing} redefines {name!r} first defined "
                f"on line {first}; the earlier body is dead and its callers "
                f"bind to this one",
            )

    assert scanned > 500, f"only {scanned} sources scanned; the roots look wrong"
    assert not offenders, "shadowed definitions:\n" + "\n".join(offenders)
