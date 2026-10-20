"""Non-Python source-code parser. Wraps `tree_sitter` only.

Per-language grammars (`tree_sitter_python`,
`tree_sitter_javascript`, etc.) install separately because the
bundle is large and most deployments need only one or two.
`GrammarMissing(language)` is raised with a `pip install
tree_sitter_<lang>` fix-hint when the grammar isn't installed.

The `code` format dispatcher in `parse/code_python.py` lazy-
imports this module only when a non-Python language is requested.
"""

from __future__ import annotations

import importlib
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.library.extraction._protocols import (
    ParsedDocument,
)
from okg.substrate.support.env_provenance import (
    annotate_optional_dependency_error,
)


class GrammarMissing(SubstrateError):
    code = "tree_sitter_grammar_missing"


class TreeSitterParser:
    """Generic source-code parser via tree_sitter. Requires the
    per-language grammar package (e.g. `tree_sitter_javascript`)
    to be installed; raises `GrammarMissing` otherwise.
    """

    def __init__(self, language: str) -> None:
        self.language = language

    def parse(self, content: bytes | str, **_: Any) -> ParsedDocument:
        if isinstance(content, str):
            content = content.encode("utf-8")

        pkg_name = f"tree_sitter_{self.language}"
        try:
            grammar_mod = importlib.import_module(pkg_name)
        except ImportError as e:
            # Issue #639: name the interpreter that lacked the package so
            # a uv-tool-shim run is distinguishable from a missing install.
            message, fix_hint, provenance = (
                annotate_optional_dependency_error(
                    f"tree-sitter grammar for language {self.language!r} "
                    f"not installed",
                    f"pip install {pkg_name}",
                    package=pkg_name,
                )
            )
            raise GrammarMissing(
                message,
                data={
                    "language": self.language,
                    "package": pkg_name,
                    **provenance,
                },
                fix_hint=fix_hint,
            ) from e

        from tree_sitter import Language, Parser

        if hasattr(grammar_mod, "language"):
            lang_capsule = grammar_mod.language()
            language = Language(lang_capsule)
        else:
            raise GrammarMissing(
                f"tree-sitter grammar package {pkg_name} does not expose a "
                f"`language()` factory",
                data={"language": self.language, "package": pkg_name},
                fix_hint=(
                    f"upgrade {pkg_name} to a version that exposes "
                    f"`{pkg_name}.language()`"
                ),
            )
        parser = Parser(language)
        tree = parser.parse(content)
        top_nodes = [child.type for child in tree.root_node.children]

        return ParsedDocument(
            text=content.decode("utf-8", errors="replace"),
            metadata={
                "language": self.language,
                "top_node_types": top_nodes,
            },
        )


__all__ = ["GrammarMissing", "TreeSitterParser"]
