from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.library.extraction.code import lsp_protocol


def test_lsp_protocol_shapes_normalize_without_raw_subtype_sprawl(
    tmp_path: Path,
) -> None:
    schema = yaml.safe_load(
        Path(
            "src/okg/substrate/library/ontologies/code_graph/schema.yaml",
        ).read_text(),
    )
    classes = set(schema["classes"])

    assert classes.isdisjoint(lsp_protocol.RAW_LSP_SHAPE_NAMES)

    definition_range = {
        "start": {"line": 10, "character": 4},
        "end": {"line": 20, "character": 5},
    }
    selection_range = {
        "start": {"line": 10, "character": 8},
        "end": {"line": 10, "character": 15},
    }
    attrs = lsp_protocol.normalize_document_symbol_attrs(
        {
            "name": "resolve",
            "qualifiedName": "Resolver.resolve",
            "kind": 6,
            "range": definition_range,
            "selectionRange": selection_range,
        },
        repo="demo",
        language="python",
        module_name="pkg.resolver",
        resolver="pyright",
        resolver_version="1.2.3",
    )

    assert attrs["kind"] == "method"
    assert attrs["symbol_kind"] == "method"
    assert attrs["lsp_symbol_kind"] == 6
    assert attrs["lsp_symbol_kind_name"] == "Method"
    assert attrs["canonical_name"] == "pkg.resolver.Resolver.resolve"
    assert attrs["semantic_key"].startswith("code-symbol:")
    assert attrs["semantic_key_version"] == (
        lsp_protocol.SEMANTIC_KEY_VERSION
    )
    assert attrs["identity_provenance"] == "lsp"
    assert attrs["lsp_spec_version"] == lsp_protocol.LSP_SPEC_VERSION
    assert attrs["lsp_method"] == "textDocument/documentSymbol"
    assert attrs["server_capability"] == "documentSymbolProvider"
    assert attrs["resolver"] == "pyright"
    assert attrs["definition_range"]
    assert attrs["selection_range"]

    official_shape_attrs = lsp_protocol.normalize_document_symbol_attrs(
        {
            "name": "Resolver.resolve",
            "kind": 6,
            "range": definition_range,
            "selectionRange": selection_range,
        },
        repo="demo",
        language="python",
        module_name="pkg.resolver",
        resolver="pyright",
        resolver_version="1.2.3",
    )
    assert official_shape_attrs["canonical_name"] == attrs["canonical_name"]
    assert official_shape_attrs["semantic_key"] == attrs["semantic_key"]

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-lsp-contract",
        "description": "LSP contract ontology fixture",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "ci",
            "code_graph",
        ],
    }))
    (dep / "schemas").mkdir()

    composed = compose_catalog(dep)
    triples = composed.narrowing_triples()
    assert {
        ("code_symbol", "extends", "code_symbol"),
        ("code_symbol", "overrides", "code_symbol"),
        ("code_symbol", "implements", "code_symbol"),
        ("code_symbol", "has_type", "code_symbol"),
        ("code_reference", "has_type", "code_symbol"),
        ("code_import", "references", "code_symbol"),
    } <= triples
