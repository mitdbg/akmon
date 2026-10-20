"""Code-symbol extraction: function / class / import /
call-graph nodes derived from source files.

Per `add-code-symbol-extraction` (2026-04-26). Three backends
are anticipated:

  - `python_ast` — stdlib `ast`, authoritative for Python.
  - `treesitter` — `tree-sitter-language-pack` for JS/TS/Go/
    Rust/...
  - `lsp` (opt-in) — out-of-process language servers
    (`pyright`, `tsserver`, `gopls`, `rust-analyzer`) for
    cross-file reference resolution.

Each backend declares its `language` and implements
`CodeSymbolExtractor.extract(source, *, path) ->
CodeSymbolReport`.
"""
from okg.substrate.library.extraction.code._protocols import (
    CodeSymbolExtractor,
    CodeSymbolReport,
    Import,
    Reference,
    Symbol,
)
from okg.substrate.library.extraction.code.dispatch import (
    CodeExtractorNotFound,
    available_languages,
    code_extractor_for,
    register_code_extractor,
)
# Eager-import the tree-sitter backend so its bootstrap
# registers JS/TS/Go/Rust/C++ on package import. Per
# `add-treesitter-code-extractor` (2026-04-26). Fail-soft:
# if the language pack isn't installed, registration logs
# and skips; AST backend stays unaffected.
from okg.substrate.library.extraction.code import treesitter  # noqa: F401
from okg.substrate.library.extraction.code.treesitter import (
    TreeSitterNotInstalled,
    TreeSitterQueryMissing,
    TreeSitterSymbolExtractor,
)


__all__ = [
    "CodeExtractorNotFound",
    "CodeSymbolExtractor",
    "CodeSymbolReport",
    "Import",
    "Reference",
    "Symbol",
    "TreeSitterNotInstalled",
    "TreeSitterQueryMissing",
    "TreeSitterSymbolExtractor",
    "available_languages",
    "code_extractor_for",
    "register_code_extractor",
]
