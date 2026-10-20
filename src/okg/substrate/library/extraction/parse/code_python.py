"""Python source-code parser. Wraps stdlib `ast` only.

Tree-sitter-backed parsing for non-Python languages lives in
`parse/code_treesitter.py`. The `code` format key dispatches
between them by `language` kwarg; the dispatcher is in this
module because it imports zero third-party packages (stdlib +
okg-internal only).
"""

from __future__ import annotations

import ast
from typing import Any

from okg.substrate.library.extraction._protocols import (
    ParsedChunk,
    ParsedDocument,
)
from okg.substrate.library.extraction.parse.dispatch import register_parser


class PythonAstParser:
    """Python source -> ParsedDocument via stdlib `ast`."""

    def parse(self, content: bytes | str, **_: Any) -> ParsedDocument:
        if isinstance(content, (bytes, bytearray)):
            content = content.decode("utf-8")
        tree = ast.parse(content)

        imports: list[str] = []
        defs: list[str] = []
        classes: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                base = node.module or ""
                for alias in node.names:
                    imports.append(
                        f"{base}.{alias.name}" if base else alias.name
                    )
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                defs.append(node.name)
            elif isinstance(node, ast.ClassDef):
                classes.append(node.name)

        docstring = ast.get_docstring(tree)
        chunks: list[ParsedChunk] = []
        for node in tree.body:
            if isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
            ):
                chunks.append(ParsedChunk(
                    text=ast.get_docstring(node) or "",
                    heading_path=[node.name],
                ))
        return ParsedDocument(
            text=content,
            metadata={
                "imports": imports,
                "defs": defs,
                "classes": classes,
                "docstring": docstring,
                "language": "python",
            },
            chunks=chunks,
        )


class _CodeDispatcher:
    """Dispatches `parse(content, language='python' | ...)` to
    PythonAstParser (Python) or TreeSitterParser (everything else).

    Lazy-imports the tree-sitter backend so deployments that only
    need Python AST parsing don't pay the tree-sitter import cost.
    """

    def parse(
        self,
        content: bytes | str,
        *,
        language: str = "python",
        **kwargs: Any,
    ) -> ParsedDocument:
        if language == "python":
            return PythonAstParser().parse(content, **kwargs)
        from okg.substrate.library.extraction.parse.code_treesitter import (
            TreeSitterParser,
        )
        return TreeSitterParser(language).parse(content, **kwargs)


def _code_parser_factory() -> _CodeDispatcher:
    return _CodeDispatcher()


register_parser("code", _code_parser_factory)


__all__ = ["PythonAstParser"]
