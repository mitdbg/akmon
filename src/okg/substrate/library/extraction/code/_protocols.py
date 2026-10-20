"""Protocols + dataclasses for code-symbol extraction.

A `CodeSymbolExtractor` parses source text in one language and
returns a `CodeSymbolReport(symbols, imports, references)`.
Backends are registered per language via the dispatch module;
deployments choose backends in `extractors.yaml`.

Per `add-code-symbol-extraction` (2026-04-26).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Protocol, runtime_checkable


__all__ = [
    "CodeSymbolExtractor",
    "CodeSymbolReport",
    "Import",
    "Reference",
    "Symbol",
]


@dataclass(frozen=True)
class Symbol:
    """A defined identifier in a source file.

    `kind` ∈ `function | class | method | module_const |
    parameter`. `qualified_name` is the dotted path from the
    module root (e.g. `"ClassName.method_name"`).
    `signature` is a best-effort textual rendering (parameter
    list for callables, base classes for classes); empty
    string when not available. `docstring` is the docstring
    body, empty when none.
    """

    kind: str
    name: str
    qualified_name: str
    span_start: int
    span_end: int
    signature: str = ""
    docstring: str = ""
    start_col: int = 0
    end_col: int = 0
    semantic_key: str = ""
    semantic_key_version: str = ""
    canonical_name: str = ""
    module_name: str = ""
    package_name: str = ""
    container_key: str = ""
    symbol_kind: str = ""
    lsp_symbol_kind: int | None = None
    lsp_symbol_kind_name: str = ""
    visibility: str = ""
    is_exported: bool | None = None
    is_deprecated: bool | None = None
    definition_uri: str = ""
    definition_range: str = ""
    selection_range: str = ""
    identity_provenance: str = ""
    resolver: str = ""
    resolver_version: str = ""
    lsp_spec_version: str = ""
    lsp_method: str = ""
    server_capability: str = ""


@dataclass(frozen=True)
class Import:
    """An import statement.

    `kind` ∈ `import | from_import | relative_import`. `module`
    is the imported module path (`os.path`); `name` is the
    imported name when known (`join` from `from os.path import
    join`); `alias` is the local rebind (`J` from `... as J`)
    or empty string.
    """

    module: str
    name: str = ""
    alias: str = ""
    kind: str = "import"
    span_start: int = 0
    span_end: int = 0
    start_col: int = 0
    end_col: int = 0
    semantic_target_key: str = ""
    resolution_kind: str = ""
    resolution_method: str = ""
    resolver: str = ""
    resolver_version: str = ""
    confidence: str = ""
    lsp_spec_version: str = ""
    lsp_method: str = ""
    server_capability: str = ""


@dataclass(frozen=True)
class Reference:
    """A within-file call/use reference.

    `caller_qname` is the qualified name of the enclosing
    symbol (empty string for module-scope calls).
    `callee_name` is the literal token AT the call site, NOT
    a resolved qualified name — within-file AST extractors
    can't resolve `foo()` to a specific qualified name when
    `foo` shadows. `resolution_kind` flags the resolution
    quality so downstream readers can branch on certainty:
    `within_file | cross_file_lsp | unresolved`.
    """

    caller_qname: str
    callee_name: str
    span_start: int
    span_end: int
    resolution_kind: str = "unresolved"
    start_col: int = 0
    end_col: int = 0
    reference_kind: str = ""
    lexical_name: str = ""
    qualified_name: str = ""
    semantic_target_key: str = ""
    semantic_enclosing_key: str = ""
    resolution_method: str = ""
    resolver: str = ""
    resolver_version: str = ""
    confidence: str = ""
    unresolved_reason: str = ""
    candidate_count: int | None = None
    workspace_root: str = ""
    workspace_fingerprint: str = ""
    source_frontier: str = ""
    lsp_spec_version: str = ""
    lsp_method: str = ""
    server_capability: str = ""


@dataclass
class CodeSymbolReport:
    """Output of one `CodeSymbolExtractor.extract()` call.

    `module_docstring` is the file-level docstring (the literal
    string at the top of the file). Empty string when none. The
    synthesized module-level Symbol in the code_graph source
    sets its `docstring` attr from this field so module-level
    docstring tags (e.g. `@implements_concept: <slug>`) are
    visible to declarative cross-link rules.
    """

    language: str
    symbols: list[Symbol] = field(default_factory=list)
    imports: list[Import] = field(default_factory=list)
    references: list[Reference] = field(default_factory=list)
    module_docstring: str = ""


@runtime_checkable
class CodeSymbolExtractor(Protocol):
    """Per-language code-symbol extractor.

    Backends MUST raise rather than return a degraded report
    on parse failure — silent fallback is a P0 in this
    codebase. Returning an empty report (no symbols / imports
    / references) is allowed for empty source files.
    """

    language: str

    def extract(
        self,
        source: str,
        *,
        path: Optional[str] = None,
    ) -> CodeSymbolReport:
        raise NotImplementedError
