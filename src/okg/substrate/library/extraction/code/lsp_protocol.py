"""Normalization helpers for LSP-backed code facts.

The Language Server Protocol is evidence, not the OKG ontology. This module
keeps the protocol vocabulary explicit at the extraction boundary and maps it
into durable code_graph attrs.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

LSP_SPEC_VERSION = "3.17"
SEMANTIC_KEY_VERSION = "okg.code_symbol.semantic_key.v1"

DOCUMENT_SYMBOL_METHOD = "textDocument/documentSymbol"
WORKSPACE_SYMBOL_METHOD = "workspace/symbol"
DEFINITION_METHOD = "textDocument/definition"
DECLARATION_METHOD = "textDocument/declaration"
REFERENCES_METHOD = "textDocument/references"
TYPE_DEFINITION_METHOD = "textDocument/typeDefinition"
IMPLEMENTATION_METHOD = "textDocument/implementation"
CALL_HIERARCHY_PREPARE_METHOD = "textDocument/prepareCallHierarchy"
CALL_HIERARCHY_INCOMING_METHOD = "callHierarchy/incomingCalls"
CALL_HIERARCHY_OUTGOING_METHOD = "callHierarchy/outgoingCalls"
TYPE_HIERARCHY_PREPARE_METHOD = "textDocument/prepareTypeHierarchy"
TYPE_HIERARCHY_SUPERTYPES_METHOD = "typeHierarchy/supertypes"
TYPE_HIERARCHY_SUBTYPES_METHOD = "typeHierarchy/subtypes"
SEMANTIC_TOKENS_FULL_METHOD = "textDocument/semanticTokens/full"
DIAGNOSTIC_METHOD = "textDocument/diagnostic"

LSP_METHODS = frozenset({
    DOCUMENT_SYMBOL_METHOD,
    WORKSPACE_SYMBOL_METHOD,
    DEFINITION_METHOD,
    DECLARATION_METHOD,
    REFERENCES_METHOD,
    TYPE_DEFINITION_METHOD,
    IMPLEMENTATION_METHOD,
    CALL_HIERARCHY_PREPARE_METHOD,
    CALL_HIERARCHY_INCOMING_METHOD,
    CALL_HIERARCHY_OUTGOING_METHOD,
    TYPE_HIERARCHY_PREPARE_METHOD,
    TYPE_HIERARCHY_SUPERTYPES_METHOD,
    TYPE_HIERARCHY_SUBTYPES_METHOD,
    SEMANTIC_TOKENS_FULL_METHOD,
    DIAGNOSTIC_METHOD,
})

RAW_LSP_SHAPE_NAMES = frozenset({
    "TextDocumentIdentifier",
    "VersionedTextDocumentIdentifier",
    "TextDocumentItem",
    "Position",
    "Range",
    "Location",
    "LocationLink",
    "DocumentSymbol",
    "WorkspaceSymbol",
    "SymbolInformation",
    "SymbolKind",
    "Diagnostic",
    "DiagnosticRelatedInformation",
    "SemanticToken",
    "SemanticTokens",
    "CallHierarchyItem",
    "CallHierarchyIncomingCall",
    "CallHierarchyOutgoingCall",
    "TypeHierarchyItem",
    "ClientCapabilities",
    "ServerCapabilities",
})

_SYMBOL_KIND_NAMES: dict[int, str] = {
    1: "File",
    2: "Module",
    3: "Namespace",
    4: "Package",
    5: "Class",
    6: "Method",
    7: "Property",
    8: "Field",
    9: "Constructor",
    10: "Enum",
    11: "Interface",
    12: "Function",
    13: "Variable",
    14: "Constant",
    15: "String",
    16: "Number",
    17: "Boolean",
    18: "Array",
    19: "Object",
    20: "Key",
    21: "Null",
    22: "EnumMember",
    23: "Struct",
    24: "Event",
    25: "Operator",
    26: "TypeParameter",
}

_NORMALIZED_SYMBOL_KINDS: dict[int, str] = {
    1: "file",
    2: "module",
    3: "namespace",
    4: "package",
    5: "class",
    6: "method",
    7: "property",
    8: "field",
    9: "constructor",
    10: "enum",
    11: "interface",
    12: "function",
    13: "variable",
    14: "constant",
    15: "literal",
    16: "literal",
    17: "literal",
    18: "array",
    19: "object",
    20: "key",
    21: "null",
    22: "enum_member",
    23: "struct",
    24: "event",
    25: "operator",
    26: "type_parameter",
}


def normalize_lsp_symbol_kind(value: int | str | None) -> dict[str, Any]:
    """Map an official LSP SymbolKind value into code_graph attrs."""
    try:
        numeric = int(value) if value is not None else 0
    except (TypeError, ValueError):
        numeric = 0
    name = _SYMBOL_KIND_NAMES.get(numeric, "Unknown")
    symbol_kind = _NORMALIZED_SYMBOL_KINDS.get(numeric, "unknown")
    return {
        "kind": symbol_kind,
        "symbol_kind": symbol_kind,
        "lsp_symbol_kind": numeric if numeric else None,
        "lsp_symbol_kind_name": name,
    }


def canonical_symbol_name(
    *,
    module_name: str,
    qualified_name: str,
) -> str:
    """Return the language-level name used for semantic joins."""
    module = module_name.strip()
    qname = qualified_name.strip()
    if not module:
        return qname
    if not qname or qname == module or qname.startswith(f"{module}."):
        return qname or module
    return f"{module}.{qname}"


def semantic_key_for_symbol(
    *,
    repo: str,
    language: str,
    module_name: str,
    qualified_name: str,
    kind: str,
    package_name: str = "",
) -> str:
    """Create a stable key that deliberately excludes line/span evidence."""
    payload = {
        "version": SEMANTIC_KEY_VERSION,
        "repo": repo,
        "language": language,
        "package_name": package_name,
        "module_name": module_name,
        "qualified_name": qualified_name,
        "kind": kind,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8",
        ),
    ).hexdigest()[:24]
    return f"code-symbol:{SEMANTIC_KEY_VERSION}:{digest}"


def lsp_provenance_attrs(
    *,
    method: str,
    server_capability: str,
    lsp_spec_version: str = LSP_SPEC_VERSION,
    resolver: str = "",
    resolver_version: str = "",
) -> dict[str, str]:
    """Attrs attached to facts derived from one LSP method."""
    return {
        "lsp_spec_version": lsp_spec_version,
        "lsp_method": method,
        "server_capability": server_capability,
        "resolver": resolver,
        "resolver_version": resolver_version,
    }


def normalize_document_symbol_attrs(
    symbol: Mapping[str, Any],
    *,
    repo: str,
    language: str,
    module_name: str,
    package_name: str = "",
    container_key: str = "",
    method: str = DOCUMENT_SYMBOL_METHOD,
    server_capability: str = "documentSymbolProvider",
    resolver: str = "",
    resolver_version: str = "",
) -> dict[str, Any]:
    """Normalize a DocumentSymbol-like mapping into CodeSymbol attrs."""
    kind_attrs = normalize_lsp_symbol_kind(symbol.get("kind"))
    qualified_name = str(symbol.get("qualifiedName") or symbol.get("name") or "")
    canonical_name = canonical_symbol_name(
        module_name=module_name,
        qualified_name=qualified_name,
    )
    semantic_key = semantic_key_for_symbol(
        repo=repo,
        language=language,
        module_name=module_name,
        qualified_name=canonical_name,
        kind=str(kind_attrs["kind"]),
        package_name=package_name,
    )
    return {
        "repo": repo,
        "language": language,
        "name": str(symbol.get("name") or ""),
        "qualified_name": qualified_name,
        "canonical_name": canonical_name,
        "semantic_key": semantic_key,
        "semantic_key_version": SEMANTIC_KEY_VERSION,
        "module_name": module_name,
        "package_name": package_name,
        "container_key": container_key,
        "definition_range": _compact_json(symbol.get("range")),
        "selection_range": _compact_json(symbol.get("selectionRange")),
        "identity_provenance": "lsp",
        **kind_attrs,
        **lsp_provenance_attrs(
            method=method,
            server_capability=server_capability,
            resolver=resolver,
            resolver_version=resolver_version,
        ),
    }


def _compact_json(value: Any) -> str:
    if value in (None, ""):
        return ""
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
