"""`okg.substrate.mcp` must initialise without depending on a half-built
`okg.substrate.mcp.search`.

Issue #2212: `okg install --profile academic-researcher` exited 1 with

    ImportError: cannot import name 'search' from partially initialized
    module 'okg.substrate.mcp.search' (most likely due to a circular import)

while a plain `from okg.substrate.mcp.search import search` in the same
interpreter succeeded -- the failure depended on the order the installer
imported modules in.

The structural cause: `okg/substrate/mcp/__init__.py` used to bind `search`
from `okg.substrate.mcp.search` FIRST and its other submodules afterwards,
while `search.py` imports some of those same submodules at its own top level.
Such a top-level edge loads a whole sibling subtree in the middle of
`search.py`'s body, long before `search.py` defines `search`. Any re-entry
into the package inside that window runs `__init__.py`'s
`from okg.substrate.mcp.search import search` against a partially initialized
module.

The fix binds `search` LAST in `__init__.py`, which leaves no sibling loading
after it and so no window at all. These tests derive what to watch from
`__init__.py` itself rather than naming submodules, so adding an
`okg.substrate.mcp.*` import after the `search` binding later reopens the
class and turns them red.

They run in fresh subprocess interpreters because the defect is a property of
a cold import graph: once `pytest` has imported the package the window no
longer exists in this process.
"""
from __future__ import annotations

import ast
import subprocess
import sys
import textwrap
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"
PACKAGE_INIT = REPO_ROOT / "src/okg/substrate/mcp/__init__.py"
INSTALL_ENTRY = (
    REPO_ROOT
    / "src/okg/substrate/library/profiles/academic-researcher/cli/install.py"
)
PACKAGE = "okg.substrate.mcp"
SEARCH = "okg.substrate.mcp.search"


def _run_fresh(script: str) -> subprocess.CompletedProcess[str]:
    """Execute `script` in a cold interpreter and return the result."""
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        timeout=300,
        start_new_session=True,
    )


@dataclass(frozen=True)
class _Import:
    """One module-level import statement, resolved to absolute names.

    `module` is the dotted module the statement imports from (`import a.b`
    and `from a.b import c` both give `a.b`); `names` are the names a
    `from ... import` binds, empty for a plain `import`.
    """

    lineno: int
    module: str
    names: tuple[str, ...]


def _is_type_checking(test: ast.expr) -> bool:
    """`if TYPE_CHECKING:` / `if typing.TYPE_CHECKING:` never runs."""
    if isinstance(test, ast.Name):
        return test.id == "TYPE_CHECKING"
    return isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"


def _module_level_statements(body: list[ast.stmt]) -> Iterator[ast.stmt]:
    """Every statement that runs when the module body runs.

    Descends into `if`/`else`, `try`/`except`/`else`/`finally`, `with`,
    loops, `match` cases and class bodies -- all execute at import time --
    but not into function or lambda bodies, and not into the body of an
    `if TYPE_CHECKING:` block (its `else` branch does run).
    """
    for node in body:
        yield node
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if isinstance(node, ast.If):
            if not _is_type_checking(node.test):
                yield from _module_level_statements(node.body)
            yield from _module_level_statements(node.orelse)
        elif isinstance(node, ast.ClassDef):
            yield from _module_level_statements(node.body)
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            yield from _module_level_statements(node.body)
        elif isinstance(node, (ast.For, ast.AsyncFor, ast.While)):
            yield from _module_level_statements(node.body)
            yield from _module_level_statements(node.orelse)
        elif isinstance(node, (ast.Try, ast.TryStar)):
            yield from _module_level_statements(node.body)
            for handler in node.handlers:
                yield from _module_level_statements(handler.body)
            yield from _module_level_statements(node.orelse)
            yield from _module_level_statements(node.finalbody)
        elif isinstance(node, ast.Match):
            for case in node.cases:
                yield from _module_level_statements(case.body)


def _module_file(module: str, src_root: Path) -> Path | None:
    """The source file for dotted `module` under `src_root`, if any."""
    base = src_root.joinpath(*module.split("."))
    for candidate in (base / "__init__.py", base.with_suffix(".py")):
        if candidate.is_file():
            return candidate
    return None


def _module_imports(module: str, src_root: Path) -> list[_Import]:
    """Every module-level import in `module`, relative forms resolved.

    `from . import x` resolves against the module's own package (the module
    itself when it is a package `__init__`), so relative and absolute
    spellings of the same edge compare equal.
    """
    path = _module_file(module, src_root)
    if path is None:
        raise AssertionError(f"no source for {module} under {src_root}")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    parts = module.split(".")
    package_parts = parts if path.name == "__init__.py" else parts[:-1]
    found: list[_Import] = []
    for node in _module_level_statements(tree.body):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.append(_Import(node.lineno, alias.name, ()))
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                keep = len(package_parts) - (node.level - 1)
                if keep <= 0:
                    raise AssertionError(
                        f"{path}:{node.lineno}: relative import climbs "
                        "above the top-level package"
                    )
                base = ".".join(package_parts[:keep])
                target = f"{base}.{node.module}" if node.module else base
            else:
                target = node.module or ""
            names = tuple(alias.name for alias in node.names)
            found.append(_Import(node.lineno, target, names))
    return found


def _loaded_modules(imp: _Import, src_root: Path) -> list[str]:
    """The source modules executing `imp` can load, in order.

    `from pkg import x` loads `pkg` and, when `x` is a submodule, `pkg.x`.
    Only modules with a source file under `src_root` are returned.
    """
    loaded = [imp.module] if _module_file(imp.module, src_root) else []
    for name in imp.names:
        if name != "*" and _module_file(f"{imp.module}.{name}", src_root):
            loaded.append(f"{imp.module}.{name}")
    return loaded


def _package_imports(
    package: str = PACKAGE, src_root: Path = SRC_ROOT
) -> list[tuple[int, str]]:
    """Every `<package>.*` submodule the package `__init__` loads.

    Returns `(lineno, dotted_name)` pairs in source order. Covers absolute
    and relative forms (`from . import x` included) and imports nested in
    module-level blocks. `from <package> import x` counts as loading
    `<package>.x` whether or not `x` is a file: inside the package's own
    `__init__` that is how a submodule is pulled in.
    """
    found: list[tuple[int, str]] = []
    for imp in _module_imports(package, src_root):
        if imp.module == package:
            found.extend(
                (imp.lineno, f"{package}.{name}")
                for name in imp.names
                if name != "*"
            )
        elif imp.module.startswith(f"{package}."):
            found.extend(
                (imp.lineno, module)
                for module in _loaded_modules(imp, src_root)
                if module.startswith(f"{package}.")
            )
    return found


def _in_progress(package: str) -> set[str]:
    """Modules already mid-import while `package`'s `__init__` runs.

    The package and its ancestors are in `sys.modules` before the body
    starts, so importing a submodule does not re-run them.
    """
    parts = package.split(".")
    return {".".join(parts[:i]) for i in range(1, len(parts) + 1)}


def _module_level_closure(
    roots: Iterable[str], package: str = PACKAGE, src_root: Path = SRC_ROOT
) -> set[str]:
    """Every source module reachable from `roots` through module-level
    imports, while `package` is initialising.

    Parent packages of each loaded module count as loaded (their
    `__init__` runs first), except `package` and its ancestors, which are
    already in progress.
    """
    skip = _in_progress(package)
    seen: set[str] = set()
    queue = [root for root in roots if root not in skip]
    while queue:
        module = queue.pop()
        if module in seen or _module_file(module, src_root) is None:
            continue
        seen.add(module)
        parts = module.split(".")
        parents = [".".join(parts[:i]) for i in range(1, len(parts))]
        nxt = [p for p in parents if p not in skip]
        for imp in _module_imports(module, src_root):
            nxt.extend(_loaded_modules(imp, src_root))
        queue.extend(m for m in nxt if m not in seen and m not in skip)
    return seen


def _search_binding_line(
    package: str = PACKAGE, src_root: Path = SRC_ROOT
) -> int:
    """The line of the package `__init__` that binds `search`."""
    search = f"{package}.search"
    for lineno, module in _package_imports(package, src_root):
        if module == search:
            return lineno
    raise AssertionError(
        f"{package}.__init__ no longer imports {search}; "
        "this guard needs updating"
    )


def _late_siblings(
    package: str = PACKAGE, src_root: Path = SRC_ROOT
) -> tuple[str, ...]:
    """`<package>.*` modules the `__init__` imports AFTER `search`.

    These are exactly the modules whose load can be triggered from inside
    `search.py`'s body while `search` itself is still undefined.
    """
    search = f"{package}.search"
    binding = _search_binding_line(package, src_root)
    return tuple(
        dict.fromkeys(
            module
            for lineno, module in _package_imports(package, src_root)
            if lineno > binding and module != search
        )
    )


def _init_siblings() -> tuple[str, ...]:
    """Every `okg.substrate.mcp.*` module `__init__.py` imports."""
    return tuple(
        dict.fromkeys(
            module for _, module in _package_imports() if module != SEARCH
        )
    )


def _late_sibling_overlap(
    package: str = PACKAGE, src_root: Path = SRC_ROOT
) -> list[str]:
    """Late siblings that `search.py`'s module body loads, transitively."""
    late = _late_siblings(package, src_root)
    if not late:
        return []
    search = f"{package}.search"
    reachable = _module_level_closure(
        [
            module
            for imp in _module_imports(search, src_root)
            for module in _loaded_modules(imp, src_root)
        ],
        package,
        src_root,
    )
    return sorted(set(late) & reachable)


def _package_back_imports(
    package: str = PACKAGE, src_root: Path = SRC_ROOT
) -> list[str]:
    """Module-level `from <package> import name` in anything the package
    `__init__` loads, directly or transitively.

    Such a statement runs while the package is only partly initialised. If
    `name` is not bound yet CPython falls back to importing the submodule
    `<package>.name`, so `from okg.substrate.mcp import search` silently
    binds the `search` MODULE instead of the function. A plain
    `import <package>` binds the partial package the same way. Returns
    `"<file>:<line> from <package> import <names>"` or
    `"<file>:<line> import <package>"` findings.

    Not covered (issue #2327): a back-import inside a function that is
    called while the module loads, and `importlib`/`__import__` forms.
    """
    roots = [module for _, module in _package_imports(package, src_root)]
    findings: list[str] = []
    for module in sorted(_module_level_closure(roots, package, src_root)):
        for imp in _module_imports(module, src_root):
            if imp.module != package:
                continue
            rel = _module_file(module, src_root).relative_to(src_root)
            if imp.names:
                statement = f"from {package} import {', '.join(imp.names)}"
            else:
                statement = f"import {package}"
            findings.append(f"{rel}:{imp.lineno} {statement}")
    return findings


def test_package_init_binds_search_after_its_other_siblings() -> None:
    """The structural guard.

    Either `__init__.py` imports no `okg.substrate.mcp.*` submodule after it
    binds `search` -- so there is no window -- or `search.py`'s module body
    reaches none of the ones it does, directly or through other modules.
    """
    overlap = _late_sibling_overlap()
    assert not overlap, (
        f"{PACKAGE_INIT} imports {list(_late_siblings())} after binding "
        f"`search`, and src/okg/substrate/mcp/search.py's module body loads "
        f"{overlap}. That loads those subtrees before `search` is defined, "
        "so a re-entry into the package resolves `from okg.substrate.mcp."
        "search import search` against a partial module (issue #2212). Bind "
        "`search` last in __init__.py, or move these to their use sites."
    )


def test_package_init_closure_has_no_module_level_back_import() -> None:
    """The second cycle shape (issue #2233): nothing the package init loads
    may import names back from `okg.substrate.mcp` at module level.
    """
    closure = _module_level_closure(
        [module for _, module in _package_imports()]
    )
    # Guard the guard: an empty closure would pass vacuously.
    assert {SEARCH, f"{PACKAGE}.session", f"{PACKAGE}.tools"} <= closure
    findings = _package_back_imports()
    assert not findings, (
        f"these modules load while {PACKAGE} initialises and import names "
        f"back from it at module level: {findings}. A name not yet bound "
        "resolves to the submodule of the same name, silently. Import from "
        "the defining submodule instead, or move the import to its use site."
    )


def test_static_closure_covers_every_module_a_cold_import_loads() -> None:
    """Keep the AST model honest: every `okg.*` module a cold
    `import okg.substrate.mcp` actually loads must be in the closure the
    back-import guard walks. A dynamic or otherwise unmodelled import would
    otherwise hide a back-import from it.
    """
    static = _module_level_closure(
        [module for _, module in _package_imports()]
    ) | _in_progress(PACKAGE)
    result = _run_fresh(
        """
        import sys
        before = set(sys.modules)
        import okg.substrate.mcp  # noqa: F401
        for name in sorted(set(sys.modules) - before):
            if name == "okg" or name.startswith("okg."):
                print(name)
        """
    )
    assert result.returncode == 0, result.stderr
    loaded = set(result.stdout.split())
    assert SEARCH in loaded, result.stdout
    missing = sorted(loaded - static)
    assert not missing, (
        f"a cold import of {PACKAGE} loads {missing}, which the static "
        "module-level closure does not model"
    )


def _write_package(root: Path, files: dict[str, str]) -> Path:
    """Write a throwaway package tree under `root`; return `root`."""
    for relative, body in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(body), encoding="utf-8")
    return root


_FIXTURE_CLEAN = {
    "fixpkg/__init__.py": """\
        from fixpkg.session import Session
        from fixpkg.search import search
        """,
    "fixpkg/session.py": "Session = object\n",
    "fixpkg/late.py": "LATE = 1\n",
    "fixpkg/search.py": """\
        import fixpkg.late
        from fixpkg.session import Session

        def search():
            return Session
        """,
}


def _fixture(tmp_path: Path, overrides: dict[str, str] | None = None) -> Path:
    """The clean fixture package with `overrides` (path -> source) applied."""
    return _write_package(tmp_path, {**_FIXTURE_CLEAN, **(overrides or {})})


def test_guard_passes_the_clean_fixture(tmp_path: Path) -> None:
    root = _fixture(tmp_path)
    assert _late_siblings("fixpkg", root) == ()
    assert _late_sibling_overlap("fixpkg", root) == []
    assert _package_back_imports("fixpkg", root) == []


@pytest.mark.parametrize(
    "init_tail",
    [
        pytest.param("from fixpkg.late import LATE\n", id="absolute"),
        pytest.param("from . import late\n", id="relative-bare"),
        pytest.param("from .late import LATE\n", id="relative-module"),
        pytest.param("from fixpkg import late\n", id="from-package"),
        pytest.param(
            "try:\n    from . import late\nexcept ImportError:\n    pass\n",
            id="nested-try",
        ),
        pytest.param(
            "if True:\n    import fixpkg.late\n", id="nested-if"
        ),
    ],
)
def test_guard_catches_every_late_import_shape(
    tmp_path: Path, init_tail: str
) -> None:
    """Each spelling of "import a sibling after `search`" is seen."""
    init = _FIXTURE_CLEAN["fixpkg/__init__.py"]
    root = _fixture(
        tmp_path, {"fixpkg/__init__.py": textwrap.dedent(init) + init_tail}
    )
    assert _late_siblings("fixpkg", root) == ("fixpkg.late",)
    assert _late_sibling_overlap("fixpkg", root) == ["fixpkg.late"]


def test_guard_catches_a_late_sibling_reached_transitively(
    tmp_path: Path,
) -> None:
    """`search.py` reaching the late sibling through another module."""
    init = _FIXTURE_CLEAN["fixpkg/__init__.py"]
    root = _fixture(
        tmp_path,
        {
            "fixpkg/__init__.py": textwrap.dedent(init) + "from . import late\n",
            "fixpkg/search.py": "from fixpkg.helper import H\n\n\ndef search(): ...\n",
            "fixpkg/helper.py": "from .late import LATE as H\n",
        },
    )
    assert _late_sibling_overlap("fixpkg", root) == ["fixpkg.late"]


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(
            "from fixpkg import search\nSession = object\n", id="absolute"
        ),
        pytest.param(
            "from . import search\nSession = object\n", id="relative"
        ),
        pytest.param(
            "try:\n    from . import search\nexcept ImportError:\n    pass\n"
            "Session = object\n",
            id="nested-try",
        ),
        pytest.param(
            "from .sub.deep import X as Session\n",
            id="transitive-relative-parent",
        ),
    ],
)
def test_guard_catches_a_module_level_back_import(
    tmp_path: Path, body: str
) -> None:
    """`from <package> import name` in a module the init loads, any shape."""
    root = _fixture(
        tmp_path,
        {
            "fixpkg/session.py": body,
            "fixpkg/sub/__init__.py": "",
            "fixpkg/sub/deep.py": "from .. import search\nX = object\n",
        },
    )
    findings = _package_back_imports("fixpkg", root)
    assert len(findings) == 1, findings
    assert "from fixpkg import search" in findings[0], findings


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        pytest.param(
            "from fixpkg import search, late\nSession = object\n",
            "fixpkg/session.py:1 from fixpkg import search, late",
            id="from-import",
        ),
        pytest.param(
            "import fixpkg as _m\nSession = object\n",
            "fixpkg/session.py:1 import fixpkg",
            id="plain-import",
        ),
    ],
)
def test_back_import_finding_names_the_statement(
    tmp_path: Path, body: str, expected: str
) -> None:
    """Each finding quotes the statement shape it found, never an empty
    `from <package> import` with no names.
    """
    root = _fixture(tmp_path, {"fixpkg/session.py": body})
    assert _package_back_imports("fixpkg", root) == [expected]


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(
            "def f():\n    from fixpkg import search\nSession = object\n",
            id="function-level",
        ),
        pytest.param(
            "from typing import TYPE_CHECKING\n"
            "if TYPE_CHECKING:\n    from fixpkg import search\n"
            "Session = object\n",
            id="type-checking",
        ),
    ],
)
def test_guard_ignores_imports_that_do_not_run_at_import_time(
    tmp_path: Path, body: str
) -> None:
    root = _fixture(tmp_path, {"fixpkg/session.py": body})
    assert _package_back_imports("fixpkg", root) == []


def test_search_module_body_does_not_load_late_package_siblings() -> None:
    """The runtime guard: with a cold interpreter, no sibling that
    `__init__.py` imports after `search` may load while `search.py` runs.
    """
    late = _late_siblings()
    result = _run_fresh(
        f"""
        SEARCH = {SEARCH!r}
        LATE = {late!r}
        stack = []
        violations = []

        real_import = __builtins__.__import__

        def traced(name, g=None, loc=None, fromlist=(), level=0):
            resolved = name
            if level and g:
                pkg = (g.get("__package__") or "").split(".")
                resolved = ".".join(
                    pkg[: len(pkg) - level + 1] + ([name] if name else [])
                )
            watched = resolved.startswith("okg.")
            if watched:
                if resolved in LATE and SEARCH in stack:
                    violations.append((resolved, list(stack)))
                stack.append(resolved)
            try:
                return real_import(name, g, loc, fromlist, level)
            finally:
                if watched:
                    stack.pop()

        __builtins__.__import__ = traced
        import okg.substrate.mcp  # noqa: F401
        __builtins__.__import__ = real_import
        for name, chain in violations:
            print("VIOLATION", name, "<-", " -> ".join(chain))
        print("violations:", len(violations))
        """
    )
    assert result.returncode == 0, result.stderr
    assert "violations: 0" in result.stdout, result.stdout


@pytest.mark.parametrize("sibling", _init_siblings())
def test_package_reentry_while_a_sibling_loads_still_binds_search(
    sibling: str,
) -> None:
    """Reproduce the reported failure deterministically, once per sibling.

    Evicting `okg.substrate.mcp` from `sys.modules` as a sibling starts to
    load forces `__init__.py` to re-run its
    `from okg.substrate.mcp.search import search`. On the unfixed tree that
    eviction lands inside `search.py`'s body -- `search.py` triggered the
    sibling's load itself -- and the import raises the issue-#2212
    ImportError. Keyed to EVERY sibling, not one, so closing a single window
    cannot pass this.
    """
    result = _run_fresh(
        f"""
        import sys
        from importlib.abc import MetaPathFinder

        fired = []

        class Reenter(MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname == {sibling!r} and not fired:
                    fired.append(fullname)
                    sys.modules.pop("okg.substrate.mcp", None)
                    import okg.substrate.mcp  # noqa: F401
                return None

        sys.meta_path.insert(0, Reenter())
        import okg.substrate.mcp as mcp
        assert fired, "the re-entry hook never fired"
        assert callable(mcp.search), type(mcp.search)
        print("reentry ok")
        """
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "reentry ok" in result.stdout, result.stdout


@pytest.mark.skipif(
    not INSTALL_ENTRY.is_file(), reason="academic-researcher installer entry not present"
)
def test_installer_entry_imported_first_then_search() -> None:
    """Load the academic-researcher installer entry module FIRST in a cold
    interpreter, then take the import the installer's ingest step takes.
    """
    result = _run_fresh(
        f"""
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "okg_academic_researcher_install", {str(INSTALL_ENTRY)!r}
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        from okg.substrate.mcp.search import search
        from okg.substrate.mcp import search as package_search
        assert callable(search), type(search)
        assert package_search is search, package_search
        print("installer-first ok")
        """
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "installer-first ok" in result.stdout, result.stdout
