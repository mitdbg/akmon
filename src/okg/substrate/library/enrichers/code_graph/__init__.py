"""Substrate-shipped enrichers for the code_graph module.

Per `cleanup-personal-okg-data-quality` C1 item 5:

Non-Python AST coverage. The `code_structure` source emits AST
nodes (CodeSymbol/CodeImport/CodeReference) for Python files only.
Markdown / YAML / TOML / JSON source_files therefore dominate the
"no AST decomposition" silent category (~75% of files at C1 close).
These enrichers fill the gap with deterministic structural parses:

- `MarkdownStructuralEnricher` — emits `MarkdownHeading` nodes +
  `(source_file contains markdown_heading)` +
  `(markdown_heading contains markdown_heading)` (nesting).
- `YamlStructuralEnricher` — emits `ConfigKey` nodes from
  top-level YAML keys.
- `TomlStructuralEnricher` — emits `ConfigKey` nodes from
  top-level TOML keys / tables.
- `JsonStructuralEnricher` — emits `ConfigKey` nodes from
  top-level JSON object keys.

All four follow the same shape (read source_file rows by suffix,
parse content from disk, emit deterministic facts) so they share
the helper module `_structural.py`.
"""

from okg.substrate.library.enrichers.code_graph.markdown_structural import (
    MarkdownStructuralEnricher,
)
from okg.substrate.library.enrichers.code_graph.config_structural import (
    JsonStructuralEnricher,
    TomlStructuralEnricher,
    YamlStructuralEnricher,
)

__all__ = [
    "MarkdownStructuralEnricher",
    "YamlStructuralEnricher",
    "TomlStructuralEnricher",
    "JsonStructuralEnricher",
]
