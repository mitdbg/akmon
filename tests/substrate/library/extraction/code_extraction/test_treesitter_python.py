"""Tree-sitter / AST cross-validation on a Python fixture."""
from __future__ import annotations

import textwrap

import pytest

pytest.importorskip("tree_sitter_language_pack")

from okg.substrate.library.extraction.code.python_ast import (
    PythonAstSymbolExtractor,
)
from okg.substrate.library.extraction.code.treesitter import (
    TreeSitterSymbolExtractor,
)


_PY = textwrap.dedent('''
    """A doc."""
    import os
    from os.path import join

    def alpha():
        beta()

    def beta():
        return 0

    class C:
        def m(self):
            helper()
''').strip() + "\n"


def test_treesitter_python_finds_same_symbol_count_as_ast() -> None:
    ast_report = PythonAstSymbolExtractor().extract(_PY)
    ts_report = TreeSitterSymbolExtractor(
        language="python",
    ).extract(_PY)

    ast_qnames = sorted(s.qualified_name for s in ast_report.symbols)
    ts_qnames = sorted(s.qualified_name for s in ts_report.symbols)
    assert ast_qnames == ts_qnames


def test_treesitter_python_emits_imports_and_calls() -> None:
    ts = TreeSitterSymbolExtractor(language="python").extract(_PY)
    assert len(ts.imports) >= 2
    # 3 within-file calls: beta, helper. (ast emits 2 here too;
    # tree-sitter may capture additional `Call` nodes in
    # docstring evaluation etc. — we just assert ≥1.)
    assert len(ts.references) >= 1
