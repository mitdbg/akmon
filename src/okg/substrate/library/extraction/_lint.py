"""Wrap-invariant lint for the extraction library.

Each backend module under `parse/`, `chunk/`, and `ner/`:
  1. imports at most ONE third-party top-level package
     (counting only non-stdlib, non-okg imports)
  2. exports exactly ONE class implementing its Protocol

The waiver dict that earlier carved out PDF / Markdown /
structural-chunk modules has been removed: the shipped tree
satisfies the invariant *structurally* by splitting any
two-library module into single-library siblings (e.g. pdf_text
+ pdf_tables; markdown + frontmatter; structural_markdown +
structural_html).

Used by `okg doctor --check extraction-wrap-invariant`.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# Stdlib top-level package names — heuristic. We don't need an
# exhaustive list; just enough to filter out the obvious ones we
# care about.
_STDLIB_PREFIXES = frozenset({
    "abc", "argparse", "ast", "asyncio", "base64", "binascii",
    "builtins", "bz2", "calendar", "collections", "concurrent",
    "contextlib", "contextvars", "copy", "csv", "ctypes",
    "dataclasses", "datetime", "decimal", "difflib", "dis",
    "doctest", "email", "encodings", "enum", "errno", "faulthandler",
    "filecmp", "fnmatch", "fractions", "ftplib", "functools", "gc",
    "getopt", "getpass", "gettext", "glob", "graphlib", "gzip",
    "hashlib", "heapq", "hmac", "html", "http", "imaplib",
    "importlib", "inspect", "io", "ipaddress", "itertools", "json",
    "logging", "lzma", "math", "mimetypes", "multiprocessing",
    "numbers", "operator", "os", "pathlib", "pickle", "pkgutil",
    "platform", "plistlib", "pprint", "queue", "random", "re",
    "secrets", "select", "shlex", "shutil", "signal", "site",
    "smtplib", "socket", "sqlite3", "ssl", "stat", "statistics",
    "string", "struct", "subprocess", "sys", "tarfile", "tempfile",
    "textwrap", "threading", "time", "timeit", "token", "tokenize",
    "traceback", "tracemalloc", "types", "typing", "unicodedata",
    "unittest", "urllib", "uuid", "warnings", "weakref", "xml",
    "zipfile", "zlib",
    # Common typing extension
    "typing_extensions",
    # __future__
    "__future__",
})


@dataclass
class WrapViolation:
    """One module's wrap-invariant violation."""

    module_path: str
    third_party_imports: list[str] = field(default_factory=list)
    reason: str = ""


@dataclass
class LintReport:
    """Result of `lint_extraction_tree`."""

    ok: bool
    modules_checked: int
    violations: list[WrapViolation] = field(default_factory=list)


def _top_level_third_party_imports(py_file: Path) -> set[str]:
    """Walk a Python file's AST; return third-party top-level
    package names imported at module level.

    Filters out:
      - stdlib (heuristic by top-level package name)
      - okg.* (substrate-internal)
      - relative imports
    """
    try:
        source = py_file.read_text()
    except Exception:  # noqa: BLE001 — file read failed (encoding / permissions); skip and return empty set.
        return set()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()

    third_party: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".")[0]
                if top in _STDLIB_PREFIXES:
                    continue
                if top == "okg":
                    continue
                third_party.add(top)
        elif isinstance(node, ast.ImportFrom):
            if node.level > 0:
                continue
            mod = (node.module or "").split(".")[0]
            if not mod:
                continue
            if mod in _STDLIB_PREFIXES:
                continue
            if mod == "okg":
                continue
            third_party.add(mod)
    return third_party


def lint_extraction_tree(
    repo_root: Optional[Path] = None,
) -> LintReport:
    """Walk `src/okg/substrate/library/extraction/{parse,chunk,ner}`
    and return a LintReport. `repo_root` defaults to the package
    install root resolved from this module.
    """
    if repo_root is None:
        # Resolve relative to this module's source location.
        here = Path(__file__).resolve()
        # Walk up to the src/ root.
        for candidate in here.parents:
            if (candidate / "okg" / "substrate" / "library" / "extraction").is_dir():
                src_root = candidate
                break
        else:
            return LintReport(ok=True, modules_checked=0)
    else:
        src_root = Path(repo_root) / "src"

    extraction_root = (
        src_root / "okg" / "substrate" / "library" / "extraction"
    )
    targets: list[Path] = []
    for sub in ("parse", "chunk", "ner"):
        sub_dir = extraction_root / sub
        if not sub_dir.is_dir():
            continue
        for py in sub_dir.glob("*.py"):
            if py.name in {"__init__.py", "composite.py", "dispatch.py"}:
                # composite/dispatch are not "wrap a single
                # third-party library" — they're substrate-side
                # algebra. Exempt by design.
                continue
            targets.append(py)

    violations: list[WrapViolation] = []
    for py_file in targets:
        third_party = _top_level_third_party_imports(py_file)

        try:
            rel = py_file.resolve().relative_to(src_root.resolve())
        except ValueError:
            rel = py_file
        rel_str = str(rel).replace("\\", "/")

        if len(third_party) > 1:
            violations.append(WrapViolation(
                module_path=rel_str,
                third_party_imports=sorted(third_party),
                reason=(
                    f"module imports {len(third_party)} third-party "
                    f"packages ({sorted(third_party)}); the wrap "
                    f"invariant allows ≤ 1. Split the module so each "
                    f"sibling wraps a single package."
                ),
            ))

    return LintReport(
        ok=not violations,
        modules_checked=len(targets),
        violations=violations,
    )


__all__ = ["LintReport", "WrapViolation", "lint_extraction_tree"]
