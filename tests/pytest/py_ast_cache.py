"""Session-shared AST parse cache for codebase-scan guard tests.

Several static guards (no-legacy-callers, marker hygiene, no-name-leakage,
chronos-native-static, ...) scan the whole `src/okg` tree by ``ast.parse``-ing
every .py file. Profiling showed the parse is ~2.5s over ~1300 files and was
repeated independently per test (read 0.17s / parse 2.50s / walk 0.72s of a 3.4s
scan). Parsing is deterministic, so this caches it keyed on path -> the whole-tree
parse happens ONCE per test session and every scan test reuses it.

Usage in a scan test (replace ``ast.parse(path.read_text(), ...)``):

    from tests.pytest.py_ast_cache import parse_py
    tree = parse_py(path)        # cached read + parse, shared across scan tests
"""
from __future__ import annotations

import ast
import functools
from pathlib import Path


@functools.lru_cache(maxsize=None)
def parse_py(path: Path) -> ast.Module:
    """Read + ``ast.parse`` ``path``, cached for the whole test session and shared
    across every scan test. Keyed on the path."""
    p = Path(path)
    return ast.parse(p.read_text(encoding="utf-8"), filename=str(p))
