"""TreeSitter C++ fixture (conservative captures)."""
from __future__ import annotations

import textwrap

import pytest

pytest.importorskip("tree_sitter_language_pack")

from okg.substrate.library.extraction.code.treesitter import (
    TreeSitterSymbolExtractor,
)


_CPP = textwrap.dedent('''
    #include <iostream>
    #include <string>

    class Greeter {
    public:
        Greeter(const std::string& name) : name_(name) {}
        void hello() {
            std::cout << "hi " << name_ << std::endl;
        }
    private:
        std::string name_;
    };

    int main() {
        Greeter g("world");
        g.hello();
        return 0;
    }
''').strip() + "\n"


def test_cpp_extracts_class_and_method() -> None:
    rep = TreeSitterSymbolExtractor(
        language="cpp",
    ).extract(_CPP, path="main.cpp")
    qnames = {s.qualified_name for s in rep.symbols}
    assert "Greeter" in qnames
    # `main` (free function) + at least one method captured.
    assert "main" in qnames
    assert any("hello" in q.split(".")[-1] for q in qnames)


def test_cpp_extracts_includes() -> None:
    rep = TreeSitterSymbolExtractor(
        language="cpp",
    ).extract(_CPP, path="main.cpp")
    # Two `#include` directives.
    assert len(rep.imports) == 2
