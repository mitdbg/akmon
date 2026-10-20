"""Tree-sitter backed `CodeSymbolExtractor`.

Wraps `tree_sitter_language_pack` (one PyPI dep that bundles
~30 grammars) + per-language `.scm` query files under
`code/queries/`. Each grammar's query declares `@function`,
`@class`, `@method`, `@import`, `@call` captures; the
extractor walks the captures and mints qualified names.

Per `add-treesitter-code-extractor` (2026-04-26).

Not bound to Python by default — `code.dispatch` keeps
`python` mapped to the AST backend (authoritative, no grammar
drift). This module's bootstrap registers tree-sitter for
javascript / typescript / go / rust / cpp.
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Optional

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
from okg.substrate.support.env_provenance import (
    annotate_optional_dependency_error,
    classify_interpreter,
)


log = logging.getLogger(__name__)


__all__ = [
    "TreeSitterNotInstalled",
    "TreeSitterQueryMissing",
    "TreeSitterSymbolExtractor",
]


class TreeSitterNotInstalled(SubstrateError):
    code = "tree_sitter_not_installed"


class TreeSitterQueryMissing(SubstrateError):
    code = "tree_sitter_query_missing"


_QUERIES_DIR = Path(__file__).resolve().parent / "queries"
_JS_LIKE_LANGUAGES = {"javascript", "typescript"}


def _source_text(source_bytes: bytes, node) -> str:
    return source_bytes[node.start_byte:node.end_byte].decode(
        "utf-8", errors="replace",
    )


def _strip_string_literal(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _walk(node):
    yield node
    for child in getattr(node, "named_children", ()):
        yield from _walk(child)


def _normalize_callee_text(callee_text: str, *, split_call: bool = False) -> str:
    text = callee_text.strip()
    if split_call and "(" in text:
        text = text.split("(", 1)[0].strip()
    text = text.replace("?.", ".").replace("?[", "[")
    text = re.sub(r"\.{2,}", ".", text)
    return text.rstrip(".?").strip()


def _call_callee_text(source_bytes: bytes, node) -> tuple[str, bool]:
    function_node = node.child_by_field_name("function")
    if function_node is not None:
        raw = _source_text(source_bytes, function_node)
        if function_node.type == "member_expression":
            object_node = function_node.child_by_field_name("object")
            property_node = function_node.child_by_field_name("property")
            object_text = (
                _source_text(source_bytes, object_node).lstrip()
                if object_node is not None else ""
            )
            if (
                property_node is not None
                and object_text.startswith(("[", "{"))
            ):
                raw = _source_text(source_bytes, property_node)
        split_call = False
    else:
        raw = _source_text(source_bytes, node).split("(", 1)[0].strip()
        split_call = True
    return _normalize_callee_text(raw, split_call=split_call), "?." in raw


def _import_module_from_node(source_bytes: bytes, node) -> str:
    source_node = node.child_by_field_name("source")
    if source_node is not None:
        return _strip_string_literal(_source_text(source_bytes, source_node))
    if node.type == "call_expression":
        args = node.child_by_field_name("arguments")
        if args is not None:
            for child in args.named_children:
                if child.type == "string":
                    return _strip_string_literal(
                        _source_text(source_bytes, child),
                    )
    return _source_text(source_bytes, node).strip().strip(';"\'')


def _imports_for_node(source_bytes: bytes, node) -> list[Import]:
    module = _import_module_from_node(source_bytes, node)
    start = int(node.start_point[0]) + 1
    end = int(node.end_point[0]) + 1
    if node.type == "call_expression":
        return [Import(
            module=module,
            name="",
            alias="",
            kind="import",
            span_start=start,
            span_end=end,
        )]
    imports: list[Import] = []
    import_clause = None
    for child in node.named_children:
        if child.type == "import_clause":
            import_clause = child
            break
    if import_clause is None:
        return [Import(
            module=module,
            name="",
            alias="",
            kind="import",
            span_start=start,
            span_end=end,
        )]

    for child in import_clause.named_children:
        if child.type == "identifier":
            imports.append(Import(
                module=module,
                name="default",
                alias=_source_text(source_bytes, child),
                kind="import",
                span_start=start,
                span_end=end,
            ))
        elif child.type == "namespace_import":
            alias = ""
            for desc in _walk(child):
                if desc.type == "identifier":
                    alias = _source_text(source_bytes, desc)
            imports.append(Import(
                module=module,
                name="*",
                alias=alias,
                kind="import",
                span_start=start,
                span_end=end,
            ))
        elif child.type == "named_imports":
            for spec in child.named_children:
                if spec.type != "import_specifier":
                    continue
                name_node = spec.child_by_field_name("name")
                if name_node is None:
                    continue
                alias_node = spec.child_by_field_name("alias")
                imports.append(Import(
                    module=module,
                    name=_source_text(source_bytes, name_node),
                    alias=(
                        _source_text(source_bytes, alias_node)
                        if alias_node is not None else ""
                    ),
                    kind="from_import",
                    span_start=start,
                    span_end=end,
                ))
    if imports:
        return imports
    return [Import(
        module=module,
        name="",
        alias="",
        kind="import",
        span_start=start,
        span_end=end,
    )]


def _callable_symbol_names(report: CodeSymbolReport) -> set[str]:
    names: set[str] = set()
    for symbol in report.symbols:
        if symbol.kind not in {"function", "method", "class"}:
            continue
        names.add(symbol.name)
        names.add(symbol.qualified_name)
        names.add(symbol.qualified_name.rsplit(".", 1)[-1])
    return names


def _callback_references_for_call(
    source_bytes: bytes,
    node,
    *,
    caller_qname: str,
    callable_names: set[str],
) -> list[Reference]:
    args = node.child_by_field_name("arguments")
    if args is None:
        return []
    out: list[Reference] = []
    for child in args.named_children:
        if child.type != "identifier":
            continue
        name = _source_text(source_bytes, child)
        if name not in callable_names:
            continue
        out.append(Reference(
            caller_qname=caller_qname,
            callee_name=name,
            span_start=int(child.start_point[0]) + 1,
            span_end=int(child.end_point[0]) + 1,
            resolution_kind="callback_argument",
        ))
    return out


def _returned_object_references(
    source_bytes: bytes,
    root,
    *,
    callable_names: set[str],
    enclosing_qname,
) -> list[Reference]:
    out: list[Reference] = []
    shorthand_types = {
        "shorthand_property_identifier",
        "shorthand_property_identifier_pattern",
    }
    for node in _walk(root):
        if node.type != "return_statement":
            continue
        returned = None
        for child in node.named_children:
            if child.type == "object":
                returned = child
                break
        if returned is None:
            continue
        caller_qname = enclosing_qname(node)
        for prop in returned.named_children:
            value_node = None
            if prop.type == "pair":
                value_node = prop.child_by_field_name("value")
            elif prop.type in shorthand_types:
                value_node = prop
            if value_node is None or value_node.type not in {
                "identifier",
                *shorthand_types,
            }:
                continue
            name = _source_text(source_bytes, value_node)
            if name not in callable_names:
                continue
            out.append(Reference(
                caller_qname=caller_qname,
                callee_name=name,
                span_start=int(value_node.start_point[0]) + 1,
                span_end=int(value_node.end_point[0]) + 1,
                resolution_kind="returned_object",
            ))
    return out


def _declaration_names(source_bytes: bytes, declaration) -> list[object]:
    if declaration is None:
        return []
    name_node = declaration.child_by_field_name("name")
    if name_node is not None:
        return [name_node]
    names: list[object] = []
    for node in _walk(declaration):
        if node.type != "variable_declarator":
            continue
        name_node = node.child_by_field_name("name")
        if name_node is not None:
            names.append(name_node)
    return names


def _public_export_references(source_bytes: bytes, root) -> list[Reference]:
    out: list[Reference] = []
    for node in _walk(root):
        if node.type != "export_statement":
            continue
        declaration = node.child_by_field_name("declaration")
        for name_node in _declaration_names(source_bytes, declaration):
            out.append(Reference(
                caller_qname="",
                callee_name=_source_text(source_bytes, name_node),
                span_start=int(name_node.start_point[0]) + 1,
                span_end=int(name_node.end_point[0]) + 1,
                resolution_kind="public_export",
            ))
        for child in node.named_children:
            if child.type != "export_clause":
                continue
            for spec in child.named_children:
                if spec.type != "export_specifier":
                    continue
                name_node = spec.child_by_field_name("name")
                if name_node is None:
                    continue
                out.append(Reference(
                    caller_qname="",
                    callee_name=_source_text(source_bytes, name_node),
                    span_start=int(name_node.start_point[0]) + 1,
                    span_end=int(name_node.end_point[0]) + 1,
                    resolution_kind="public_export",
                ))
    return out


_CACHE_DIR_VARIABLE = "TREE_SITTER_LANGUAGE_PACK_CACHE_DIR"


# Text the pack's own errors carry when the grammar cache is unusable
# (tree-sitter-language-pack 1.17.0, observed or quoted from the native
# module): "Failed to create cache directory <path> ... Permission denied
# (os error 13)" for a cache it cannot create; "Failed to extract <lib>: IO
# error: Permission denied (os error 13)" for an existing cache it cannot
# write; "Refusing to use cache directory ..." for a group-writable or
# foreign-owned one; "Could not determine a cache directory" with no home;
# "Download cache lock error" when the lock file cannot be taken.
_CACHE_FAILURE_MARKERS = ("cache director", "cache lock")
# File-system refusals count only when the text is about the cache or about
# extracting a grammar into it, so a download refused by a firewall with
# EACCES is not misreported as a cache problem.
_CACHE_IO_MARKERS = (
    "permission denied",
    "os error 13",
    "os error 30",
    "read-only file system",
)
_CACHE_IO_CONTEXT = ("cache", "failed to extract")


def _grammar_cache_failure(error: Exception) -> dict[str, str] | None:
    """Classify a grammar-load error as an unusable grammar cache.

    Only the pack's own ``Error`` subclasses are considered; the text must
    carry one of ``_CACHE_FAILURE_MARKERS``, or one of ``_CACHE_IO_MARKERS``
    together with one of ``_CACHE_IO_CONTEXT``. Returns the error data to
    attach, or ``None`` when the failure is something else (an unknown
    language, a network download failure).
    """
    try:
        from tree_sitter_language_pack import Error as PackError
        from tree_sitter_language_pack import cache_dir
    except ImportError:
        return None
    if not isinstance(error, PackError):
        return None
    text = str(error).lower()
    about_cache = any(marker in text for marker in _CACHE_FAILURE_MARKERS)
    cache_io = any(marker in text for marker in _CACHE_IO_MARKERS) and any(
        context in text for context in _CACHE_IO_CONTEXT
    )
    if not (about_cache or cache_io):
        return None
    try:
        location = cache_dir()
    except Exception as exc:  # noqa: BLE001 — report why the path is unknown instead of guessing one.
        location = f"<unknown: cache_dir() raised {exc}>"
    return {
        "grammar_cache_dir": location,
        "cache_dir_variable": _CACHE_DIR_VARIABLE,
    }


def _available_query_languages() -> list[str]:
    if not _QUERIES_DIR.is_dir():
        return []
    return sorted(p.stem for p in _QUERIES_DIR.glob("*.scm"))


class TreeSitterSymbolExtractor:
    """Generic tree-sitter backend.

    Constructor:
      `language` — language id (matches both
      `tree_sitter_language_pack.get_parser(language)` and
      `code/queries/<language>.scm`).

    `extract(source, *, path)` returns a `CodeSymbolReport`.
    Symbol qualified names are built by traversing ancestor
    `class`/`function` captures so methods get
    `Class.method` qnames consistent with the AST backend.
    """

    def __init__(self, *, language: str) -> None:
        self.language = language
        try:
            from tree_sitter_language_pack import (
                get_language,
                get_parser,
            )
        except ImportError as e:
            # Issue #639: name the interpreter that lacked the package —
            # a uv-tool-shim run is otherwise indistinguishable from a
            # missing install.
            message, fix_hint, provenance = (
                annotate_optional_dependency_error(
                    f"tree-sitter-language-pack not installed; "
                    f"required for the `{language}` code extractor",
                    (
                        "install the `code` extra into the interpreter "
                        "above: from a checkout run `uv run --extra code "
                        "--extra quality okg ...`, or `pip install "
                        "tree-sitter-language-pack`"
                    ),
                    extras=("code", "quality"),
                )
            )
            raise TreeSitterNotInstalled(
                message,
                data={"language": language, **provenance},
                fix_hint=fix_hint,
            ) from e

        try:
            self._language = get_language(language)
            self._parser = get_parser(language)
        except Exception as e:  # noqa: BLE001
            cache_data = _grammar_cache_failure(e)
            if cache_data is not None:
                # Issue #2672: the pack downloads grammars into a per-user
                # cache on first use; an unwritable home fails every
                # code-structure source. Name the path and the variable.
                configured = os.environ.get(_CACHE_DIR_VARIABLE)
                action = (
                    f"{_CACHE_DIR_VARIABLE} is set to {configured!r}; "
                    "point it at a usable directory"
                    if configured
                    else f"set {_CACHE_DIR_VARIABLE} to move it"
                )
                summary = (
                    f"tree-sitter grammar for {language!r} could not be "
                    f"loaded: the grammar cache "
                    f"{cache_data['grammar_cache_dir']} cannot be used; "
                    f"{action} ({e})"
                )
                hint = (
                    f"set {_CACHE_DIR_VARIABLE} to a directory this user "
                    "owns and can write, not group- or other-writable "
                    "(grammars are stored under "
                    "<dir>/tree-sitter-language-pack/<version>/libs and "
                    "downloaded on first use, which needs network access); "
                    f"while {_CACHE_DIR_VARIABLE} is unset, XDG_CACHE_HOME "
                    "also moves the default cache on Linux"
                )
            else:
                summary = (
                    f"tree-sitter grammar for {language!r} could "
                    f"not be loaded: {e}"
                )
                hint = (
                    "verify the language is supported by your "
                    "tree-sitter-language-pack version"
                )
            if cache_data is not None:
                # Name the interpreter, but keep the cache hint first: the
                # uv-tool rewrite in annotate_optional_dependency_error would
                # blame missing extras, and reinstalling okg fixes nothing.
                executable, kind = classify_interpreter()
                message = (
                    f"{summary} [running interpreter: {executable}; {kind}]"
                )
                fix_hint = hint
                provenance = {
                    "interpreter": executable,
                    "environment_kind": kind,
                }
            else:
                message, fix_hint, provenance = (
                    annotate_optional_dependency_error(
                        summary,
                        hint,
                        extras=("code", "quality"),
                    )
                )
            raise TreeSitterNotInstalled(
                message,
                data={
                    "language": language,
                    **(cache_data or {}),
                    **provenance,
                },
                fix_hint=fix_hint,
            ) from e

        query_path = _QUERIES_DIR / f"{language}.scm"
        if not query_path.is_file():
            raise TreeSitterQueryMissing(
                f"no tree-sitter query file ships for "
                f"language {language!r}",
                data={
                    "language": language,
                    "query_path_searched": str(query_path),
                    "available": _available_query_languages(),
                },
                fix_hint=(
                    f"add `{query_path}` declaring @function "
                    "/ @class / @import / @call captures, or "
                    "use one of the available languages"
                ),
            )
        try:
            from tree_sitter import Query, QueryCursor
            self._query = Query(
                self._language, query_path.read_text(),
            )
            self._QueryCursor = QueryCursor
        except Exception as e:
            raise TreeSitterQueryMissing(
                f"tree-sitter query file at {query_path} did "
                f"not compile against the {language!r} grammar: "
                f"{e}",
                data={
                    "language": language,
                    "query_path": str(query_path),
                },
                fix_hint=(
                    "fix the .scm file's syntax against the "
                    "language's tree-sitter grammar; check "
                    "captures match the AST node names"
                ),
            ) from e

    # ---------- main extract ----------

    def extract(
        self,
        source: str,
        *,
        path: Optional[str] = None,
    ) -> CodeSymbolReport:
        source_bytes = source.encode("utf-8")
        tree = self._parser.parse(source_bytes)
        root = tree.root_node

        report = CodeSymbolReport(language=self.language)

        # tree-sitter ≥0.22 uses a separate QueryCursor; the
        # cursor's `captures()` returns
        # `dict[capture_name, list[Node]]`.
        try:
            cursor = self._QueryCursor(self._query)
            capture_map = cursor.captures(root)
        except Exception as e:
            raise TreeSitterQueryMissing(
                f"running tree-sitter query for {self.language!r} "
                f"raised: {e}",
                data={"language": self.language, "path": path},
            ) from e

        # Normalize to a list of (node, capture_name) pairs.
        captures_list: list[tuple[object, str]] = []
        if isinstance(capture_map, dict):
            for cap_name, nodes in capture_map.items():
                for n in nodes:
                    captures_list.append((n, cap_name))
        else:
            captures_list = list(capture_map)

        def _node_text(n) -> str:
            return _source_text(source_bytes, n)

        # First pass: build a definition_node_id ->
        # name_node map by pairing `definition.*` captures
        # with their `name.*` siblings. We pair by document
        # order (start_byte) so a class's name doesn't
        # accidentally claim a nested function's
        # definition node.
        def_nodes: list[tuple[int, object, str]] = []
        name_nodes: list[tuple[int, object]] = []
        for n, cap in captures_list:
            if cap.startswith("definition."):
                def_nodes.append((int(n.start_byte), n, cap))
            elif cap.startswith("name.") or cap == "name":
                name_nodes.append((int(n.start_byte), n))
        # For each definition node, find the FIRST name node
        # contained within its byte range (the immediate
        # name field of that definition).
        def_to_name_node: dict[int, object] = {}
        for _, def_n, _cap in def_nodes:
            d_start = int(def_n.start_byte)
            d_end = int(def_n.end_byte)
            best: tuple[int, object] | None = None
            for n_start, name_n in name_nodes:
                if d_start <= n_start < d_end:
                    if best is None or n_start < best[0]:
                        best = (n_start, name_n)
            if best is not None:
                def_to_name_node[def_n.id] = best[1]

        # Second pass: emit symbols.
        # Process CLASSES first so methods inside them find
        # the class qname when walking ancestors. Within
        # each kind, sort by start_byte so qname construction
        # is document-order-stable.
        emitted_symbols: dict[int, str] = {}  # node id -> qname
        seen_references: set[tuple[str, str, int, int, str]] = set()

        def _key(item):
            n, cap = item
            return (
                0 if cap == "definition.class" else 1,
                int(n.start_byte),
            )

        for n, cap in sorted(captures_list, key=_key):
            if cap not in (
                "definition.function",
                "definition.method",
                "definition.class",
            ):
                continue
            name_node = def_to_name_node.get(n.id)
            if name_node is None:
                continue
            local_name = _node_text(name_node)
            qname = self._mint_qname(n, local_name, emitted_symbols)
            emitted_symbols[n.id] = qname
            kind = (
                "method" if cap == "definition.method" else
                ("class" if cap == "definition.class" else
                 "function")
            )
            # If this is a function inside a class,
            # promote to method.
            if kind == "function" and self._inside_class(
                n, emitted_symbols,
            ):
                kind = "method"
            report.symbols.append(Symbol(
                kind=kind,
                name=local_name,
                qualified_name=qname,
                span_start=int(n.start_point[0]) + 1,
                span_end=int(n.end_point[0]) + 1,
                signature="",
                docstring="",
            ))

        callable_names = _callable_symbol_names(report)

        def _append_reference(ref: Reference) -> None:
            key = (
                ref.caller_qname,
                ref.callee_name,
                ref.span_start,
                ref.span_end,
                ref.resolution_kind,
            )
            if key in seen_references:
                return
            seen_references.add(key)
            report.references.append(ref)

        # Third pass: emit imports / references now that
        # all same-file callable definitions are known.
        for n, cap in sorted(captures_list, key=lambda item: int(item[0].start_byte)):
            if cap == "import":
                report.imports.extend(_imports_for_node(source_bytes, n))
            elif cap == "call":
                # The capture is the call expression; the
                # callee text is the function-portion of it.
                callee_text, optional_chain = _call_callee_text(
                    source_bytes,
                    n,
                )
                if not callee_text:
                    continue
                caller_qname = self._enclosing_symbol_qname(
                    n, emitted_symbols,
                )
                _append_reference(Reference(
                    caller_qname=caller_qname,
                    callee_name=callee_text,
                    span_start=int(n.start_point[0]) + 1,
                    span_end=int(n.end_point[0]) + 1,
                    resolution_kind=(
                        "within_file_optional_chain"
                        if optional_chain else "within_file"
                    ),
                ))
                if self.language in _JS_LIKE_LANGUAGES:
                    for ref in _callback_references_for_call(
                        source_bytes,
                        n,
                        caller_qname=caller_qname,
                        callable_names=callable_names,
                    ):
                        _append_reference(ref)

        if self.language in _JS_LIKE_LANGUAGES:
            for ref in _returned_object_references(
                source_bytes,
                root,
                callable_names=callable_names,
                enclosing_qname=lambda node: self._enclosing_symbol_qname(
                    node,
                    emitted_symbols,
                ),
            ):
                _append_reference(ref)
            for ref in _public_export_references(source_bytes, root):
                _append_reference(ref)

        return report

    # ---------- helpers ----------

    def _mint_qname(
        self,
        node,
        local_name: str,
        emitted_symbols: dict[int, str],
    ) -> str:
        """Build a dotted qname by walking up the AST,
        prefixing with any already-emitted enclosing symbol's
        qname."""
        parent = node.parent
        while parent is not None:
            enclosing = emitted_symbols.get(parent.id)
            if enclosing:
                return f"{enclosing}.{local_name}"
            parent = parent.parent
        return local_name

    def _inside_class(
        self, node, emitted_symbols: dict[int, str],
    ) -> bool:
        """True iff any ancestor is an emitted class symbol."""
        # Heuristic: emitted symbols whose qname has no dot
        # are top-level — methods are dotted under their
        # class. We can't tell apart class-vs-function here,
        # so use the AST node type.
        parent = node.parent
        while parent is not None:
            if parent.type in (
                "class_definition", "class_declaration",
                "class_specifier", "class_body",
                "impl_item",
            ):
                return True
            parent = parent.parent
        return False

    def _enclosing_symbol_qname(
        self, node, emitted_symbols: dict[int, str],
    ) -> str:
        parent = node.parent
        while parent is not None:
            qname = emitted_symbols.get(parent.id)
            if qname:
                return qname
            parent = parent.parent
        return ""


# ---------- bootstrap ----------

def _bootstrap() -> None:
    """Register tree-sitter for the non-Python languages we
    ship .scm files for. Python stays bound to the AST
    backend (registered by `code.python_ast`)."""
    for language in (
        "c",
        "javascript",
        "typescript",
        "go",
        "rust",
        "cpp",
    ):
        try:
            # Lazy-construct: the factory creates the
            # extractor on first call to
            # `code_extractor_for(language)`. If the language
            # pack is missing, the lazy construction surfaces
            # the structured error then; the registration
            # itself doesn't fail the import.
            register_code_extractor(
                language,
                lambda *, language=language: TreeSitterSymbolExtractor(
                    language=language,
                ),
            )
        except Exception:  # noqa: BLE001 — one language failing to bootstrap shouldn't take down the rest; log + continue to the next language.
            log.exception(
                "tree-sitter bootstrap registration for "
                "language %r failed (skipped)", language,
            )


_bootstrap()
