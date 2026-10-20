"""Per `add-code-symbol-extraction` (2026-04-26):
PythonAstSymbolExtractor against a small golden corpus.

Each fixture asserts EXACT counts so AST-walker regressions
are caught immediately.
"""
from __future__ import annotations

import textwrap

import pytest

from okg.substrate.library.extraction.code import (
    CodeExtractorNotFound,
    available_languages,
    code_extractor_for,
)
from okg.substrate.library.extraction.code.python_ast import (
    CodeParseFailed,
    PythonAstSymbolExtractor,
)


def test_python_registered_at_bootstrap() -> None:
    assert "python" in available_languages()
    inst = code_extractor_for("python")
    assert isinstance(inst, PythonAstSymbolExtractor)


def test_unknown_language_raises_code_extractor_not_found() -> None:
    with pytest.raises(CodeExtractorNotFound) as exc_info:
        code_extractor_for("cobol")
    err = exc_info.value
    assert "cobol" in str(err)
    assert "python" in err.data["available"]


def test_top_level_function_emits_one_symbol() -> None:
    src = textwrap.dedent('''
        def f(x, y):
            """sum the args."""
            return x + y
    ''').strip() + "\n"
    rep = PythonAstSymbolExtractor().extract(src)
    assert rep.language == "python"
    assert len(rep.symbols) == 1
    sym = rep.symbols[0]
    assert sym.kind == "function"
    assert sym.name == "f"
    assert sym.qualified_name == "f"
    assert sym.signature == "(x, y)"
    assert "sum the args" in sym.docstring


def test_class_methods_get_class_qualified_names() -> None:
    src = textwrap.dedent('''
        class Foo:
            """doc for Foo."""

            def bar(self, n):
                """method bar."""
                return n + 1

            async def aiter(self):
                yield 1
    ''').strip() + "\n"
    rep = PythonAstSymbolExtractor().extract(src)
    by_qname = {s.qualified_name: s for s in rep.symbols}
    assert "Foo" in by_qname and by_qname["Foo"].kind == "class"
    assert "Foo.bar" in by_qname
    assert by_qname["Foo.bar"].kind == "method"
    assert "Foo.aiter" in by_qname
    assert by_qname["Foo.aiter"].kind == "method"


def test_imports_capture_all_three_kinds() -> None:
    src = textwrap.dedent('''
        import os
        import sys as system
        from os.path import join, dirname as d
        from . import sibling
        from .pkg import inner
    ''').strip() + "\n"
    rep = PythonAstSymbolExtractor().extract(src)
    by_module = sorted(
        (i.module, i.name, i.alias, i.kind) for i in rep.imports
    )
    expected = sorted([
        ("os", "", "", "import"),
        ("sys", "", "system", "import"),
        ("os.path", "join", "", "from_import"),
        ("os.path", "dirname", "d", "from_import"),
        (".", "sibling", "", "relative_import"),
        (".pkg", "inner", "", "relative_import"),
    ])
    assert by_module == expected


def test_within_file_references_emitted_for_calls() -> None:
    src = textwrap.dedent('''
        def alpha():
            beta()
            self.gamma()

        class C:
            def m(self):
                helper()
    ''').strip() + "\n"
    rep = PythonAstSymbolExtractor().extract(src)
    # 3 calls: beta, self.gamma, helper.
    callers = [r.caller_qname for r in rep.references]
    callees = [r.callee_name for r in rep.references]
    assert callers == ["alpha", "alpha", "C.m"]
    assert callees == ["beta", "self.gamma", "helper"]
    # All marked as within-file.
    assert all(
        r.resolution_kind == "within_file" for r in rep.references
    )


def test_idempotent_on_repeated_parse() -> None:
    src = textwrap.dedent('''
        def f(): pass
        class C:
            def m(self): pass
    ''').strip() + "\n"
    extractor = PythonAstSymbolExtractor()
    a = extractor.extract(src)
    b = extractor.extract(src)
    assert a.symbols == b.symbols
    assert a.imports == b.imports
    assert a.references == b.references


def test_syntax_error_raises_code_parse_failed() -> None:
    src = "def f(:\n  pass\n"
    with pytest.raises(CodeParseFailed) as exc_info:
        PythonAstSymbolExtractor().extract(
            src, path="bad.py",
        )
    err = exc_info.value
    assert err.data["path"] == "bad.py"
    assert err.data["lineno"] == 1


def test_nested_function_qname_chains() -> None:
    """Nested defs inside a function get the parent's qname
    as prefix."""
    src = textwrap.dedent('''
        def outer():
            def inner():
                pass
            inner()
    ''').strip() + "\n"
    rep = PythonAstSymbolExtractor().extract(src)
    qnames = sorted(s.qualified_name for s in rep.symbols)
    assert "outer" in qnames
    assert "outer.inner" in qnames
