"""TreeSitter Go fixture."""
from __future__ import annotations

import textwrap

import pytest

pytest.importorskip("tree_sitter_language_pack")

from okg.substrate.library.extraction.code.treesitter import (
    TreeSitterSymbolExtractor,
)


_GO = textwrap.dedent('''
    package main

    import "fmt"

    type Greeter struct {
        Name string
    }

    func (g *Greeter) Hello() {
        fmt.Println("hi " + g.Name)
    }

    func New(name string) *Greeter {
        return &Greeter{Name: name}
    }

    func main() {
        g := New("world")
        g.Hello()
    }
''').strip() + "\n"


def test_go_extracts_functions_methods_types() -> None:
    rep = TreeSitterSymbolExtractor(
        language="go",
    ).extract(_GO, path="main.go")
    qnames = {s.qualified_name for s in rep.symbols}
    # Top-level functions.
    assert "New" in qnames
    assert "main" in qnames
    # Method.
    assert any("Hello" in q for q in qnames)
    # Type.
    assert "Greeter" in qnames


def test_go_extracts_imports() -> None:
    rep = TreeSitterSymbolExtractor(
        language="go",
    ).extract(_GO, path="main.go")
    assert len(rep.imports) >= 1


def test_go_extracts_calls() -> None:
    rep = TreeSitterSymbolExtractor(
        language="go",
    ).extract(_GO, path="main.go")
    callees = [r.callee_name for r in rep.references]
    assert (
        any("Println" in c for c in callees)
        or any("New" in c for c in callees)
    )
