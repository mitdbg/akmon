"""Python code-symbol extractor — stdlib `ast` only.

Authoritative for Python: no grammar drift; runs at parse
speed. Recurses into class bodies so methods get
class-qualified names. Resolves only within-file references;
cross-file resolution requires the LSP backend.

Per `add-code-symbol-extraction` (2026-04-26).
"""
from __future__ import annotations

import ast
from typing import Iterable, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.extraction.code._protocols import (
    CodeSymbolReport,
    Import,
    Reference,
    Symbol,
)
from okg.substrate.library.extraction.code.dispatch import (
    register_code_extractor,
)


__all__ = ["CodeParseFailed", "PythonAstSymbolExtractor"]


class CodeParseFailed(SubstrateError):
    code = "code_parse_failed"


class PythonAstSymbolExtractor:
    """Extract symbols / imports / references from Python via
    stdlib `ast`.

    The walker is iterative (depth-first) over `ast.iter_child_nodes`
    so nested classes / methods land with their dotted qname.
    Module-scope statements are visited before their bodies;
    parameters are NOT emitted as symbols (the proposal lists
    `parameter` as a possible kind but the AST backend defers
    that to LSP).
    """

    language: str = "python"

    def extract(
        self,
        source: str,
        *,
        path: Optional[str] = None,
    ) -> CodeSymbolReport:
        try:
            tree = ast.parse(source)
        except SyntaxError as e:
            raise CodeParseFailed(
                f"python AST parse failed: {e.msg}",
                data={
                    "path": path,
                    "lineno": e.lineno,
                    "offset": e.offset,
                },
                fix_hint=(
                    "the file is not valid Python under the "
                    "running interpreter's syntax level; check "
                    "for syntax errors or version skew"
                ),
            ) from e

        report = CodeSymbolReport(
            language=self.language,
            module_docstring=ast.get_docstring(tree) or "",
        )
        self._walk(tree, qname_prefix="", report=report)
        return report

    # ---------- traversal ----------

    def _walk(
        self,
        parent: ast.AST,
        *,
        qname_prefix: str,
        report: CodeSymbolReport,
    ) -> None:
        for child in ast.iter_child_nodes(parent):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self._emit_function(
                    child,
                    qname_prefix=qname_prefix,
                    report=report,
                )
            elif isinstance(child, ast.ClassDef):
                self._emit_class(
                    child,
                    qname_prefix=qname_prefix,
                    report=report,
                )
            elif isinstance(child, ast.Import):
                self._emit_import_plain(child, report=report)
            elif isinstance(child, ast.ImportFrom):
                self._emit_import_from(child, report=report)
            else:
                # Walk container statements (If/Try/With/Match/...)
                # so a nested class or function gets emitted
                # even if it's inside a conditional block.
                self._walk(
                    child,
                    qname_prefix=qname_prefix,
                    report=report,
                )

    def _emit_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        *,
        qname_prefix: str,
        report: CodeSymbolReport,
    ) -> None:
        # Within a class body the kind becomes `method`.
        kind = "method" if qname_prefix else "function"
        qname = (
            f"{qname_prefix}.{node.name}"
            if qname_prefix else node.name
        )
        sig = self._format_function_signature(node)
        doc = ast.get_docstring(node) or ""
        report.symbols.append(Symbol(
            kind=kind,
            name=node.name,
            qualified_name=qname,
            span_start=int(node.lineno),
            span_end=int(getattr(node, "end_lineno", node.lineno) or 0),
            signature=sig,
            docstring=doc,
        ))
        # References + nested defs inside this function body.
        self._collect_references(
            node.body, caller_qname=qname, report=report,
        )
        self._walk(
            node, qname_prefix=qname, report=report,
        )

    def _emit_class(
        self,
        node: ast.ClassDef,
        *,
        qname_prefix: str,
        report: CodeSymbolReport,
    ) -> None:
        qname = (
            f"{qname_prefix}.{node.name}"
            if qname_prefix else node.name
        )
        bases = ", ".join(
            self._format_expr(b) for b in node.bases
        )
        sig = f"({bases})" if bases else ""
        doc = ast.get_docstring(node) or ""
        report.symbols.append(Symbol(
            kind="class",
            name=node.name,
            qualified_name=qname,
            span_start=int(node.lineno),
            span_end=int(getattr(node, "end_lineno", node.lineno) or 0),
            signature=sig,
            docstring=doc,
        ))
        # Recurse — methods land with class-qualified names.
        self._walk(
            node, qname_prefix=qname, report=report,
        )

    def _emit_import_plain(
        self, node: ast.Import, *, report: CodeSymbolReport,
    ) -> None:
        for alias in node.names:
            report.imports.append(Import(
                module=alias.name,
                name="",
                alias=alias.asname or "",
                kind="import",
                span_start=int(node.lineno),
                span_end=int(getattr(node, "end_lineno", node.lineno) or 0),
            ))

    def _emit_import_from(
        self,
        node: ast.ImportFrom,
        *,
        report: CodeSymbolReport,
    ) -> None:
        # Relative imports carry leading dots in `level`; the
        # module field stays the rightmost segment (or empty
        # for `from . import x`).
        is_relative = bool(node.level)
        kind = "relative_import" if is_relative else "from_import"
        module_path = ("." * (node.level or 0)) + (node.module or "")
        for alias in node.names:
            report.imports.append(Import(
                module=module_path,
                name=alias.name,
                alias=alias.asname or "",
                kind=kind,
                span_start=int(node.lineno),
                span_end=int(getattr(node, "end_lineno", node.lineno) or 0),
            ))

    # ---------- helpers ----------

    def _collect_references(
        self,
        body: Iterable[ast.AST],
        *,
        caller_qname: str,
        report: CodeSymbolReport,
    ) -> None:
        """Walk every Call within the function body and emit a
        Reference per call. Doesn't descend into nested
        FunctionDef / ClassDef bodies — those get their own
        visitor pass with their own `caller_qname`."""
        for node in body:
            for sub in _iter_calls_excluding_nested_defs(node):
                callee = self._format_expr(sub.func)
                if not callee:
                    continue
                report.references.append(Reference(
                    caller_qname=caller_qname,
                    callee_name=callee,
                    span_start=int(sub.lineno),
                    span_end=int(
                        getattr(sub, "end_lineno", sub.lineno) or 0
                    ),
                    resolution_kind="within_file",
                    start_col=int(getattr(sub.func, "col_offset", 0) or 0),
                    end_col=int(getattr(sub.func, "end_col_offset", 0) or 0),
                ))

    def _format_function_signature(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> str:
        args = node.args
        parts: list[str] = []
        # Positional + kwonly only — varargs / kwargs surface
        # via ast.unparse as part of the formatted args list,
        # but we keep a simple comma-joined form for dedup
        # stability.
        for a in args.args:
            parts.append(a.arg)
        if args.vararg:
            parts.append(f"*{args.vararg.arg}")
        for a in args.kwonlyargs:
            parts.append(a.arg)
        if args.kwarg:
            parts.append(f"**{args.kwarg.arg}")
        return f"({', '.join(parts)})"

    def _format_expr(self, expr: ast.AST) -> str:
        """Best-effort textual rendering for `Name` /
        `Attribute` / `Call.func`. Returns "" for shapes the
        backend doesn't understand."""
        if isinstance(expr, ast.Name):
            return expr.id
        if isinstance(expr, ast.Attribute):
            base = self._format_expr(expr.value)
            return f"{base}.{expr.attr}" if base else expr.attr
        if isinstance(expr, ast.Call):
            return self._format_expr(expr.func)
        try:
            return ast.unparse(expr)
        except (AttributeError, ValueError):
            return ""


def _iter_calls_excluding_nested_defs(
    node: ast.AST,
) -> Iterable[ast.Call]:
    """Yield every `ast.Call` reachable from `node` WITHOUT
    descending into nested FunctionDef / ClassDef bodies
    (those get their own pass)."""
    if isinstance(node, ast.Call):
        yield node
    if isinstance(
        node,
        (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef),
    ):
        # Skip — caller will visit this with its own
        # `caller_qname`.
        return
    for child in ast.iter_child_nodes(node):
        yield from _iter_calls_excluding_nested_defs(child)


# Bootstrap registration.
register_code_extractor(
    "python",
    PythonAstSymbolExtractor,
)
