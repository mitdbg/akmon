"""TreeSitter Rust fixture."""
from __future__ import annotations

import textwrap

import pytest

pytest.importorskip("tree_sitter_language_pack")

from okg.substrate.library.extraction.code.treesitter import (
    TreeSitterSymbolExtractor,
)


_RS = textwrap.dedent('''
    use std::fmt;

    pub struct Greeter {
        pub name: String,
    }

    impl Greeter {
        pub fn new(name: &str) -> Self {
            Greeter { name: name.to_string() }
        }
        pub fn hello(&self) {
            println!("hi {}", self.name);
        }
    }

    pub fn main() {
        let g = Greeter::new("world");
        g.hello();
    }
''').strip() + "\n"


def test_rust_extracts_functions_struct_impl() -> None:
    rep = TreeSitterSymbolExtractor(
        language="rust",
    ).extract(_RS, path="main.rs")
    qnames = {s.qualified_name for s in rep.symbols}
    # Top-level fn.
    assert "main" in qnames
    # Struct.
    assert "Greeter" in qnames
    # Methods inside impl.
    assert any("new" in q.split(".")[-1] for q in qnames)
    assert any("hello" in q.split(".")[-1] for q in qnames)


def test_rust_extracts_use_imports() -> None:
    rep = TreeSitterSymbolExtractor(
        language="rust",
    ).extract(_RS, path="main.rs")
    assert len(rep.imports) >= 1


def test_rust_extracts_calls() -> None:
    rep = TreeSitterSymbolExtractor(
        language="rust",
    ).extract(_RS, path="main.rs")
    callees = [r.callee_name for r in rep.references]
    assert any(
        "new" in c or "hello" in c or "println" in c
        for c in callees
    )
