"""The ``embeddings:`` vector-set block (PACT openrouter-embeddings-hnsw, v3.2).

No database. One refusal case per rule, and the identity properties: the set
id changes with model, width, cut, prefixes, provider and document format
version, and with nothing else.
"""
from __future__ import annotations

import copy

import pytest

import okg.substrate.embeddings.config as config_module
from okg.substrate.embeddings.config import (
    DEFAULT_SET_NAME,
    DocumentTemplate,
    EmbeddingConfig,
    EmbeddingConfigError,
    VectorIndexConfig,
    VectorSetConfig,
    _parse_index,
    parse_embedding_config,
)
from okg.substrate.embeddings.store import set_index_name


OR_768 = "openrouter:google/gemini-embedding-2@768"


def _block(**set_overrides) -> dict:
    main = {"backend": OR_768}
    main.update(set_overrides)
    return {"enabled": True, "sets": {"main": main}}


def _parse(block: dict) -> EmbeddingConfig:
    return parse_embedding_config(plural=block)


def test_old_single_backend_form_reads_as_set_main_with_defaults() -> None:
    config = _parse({"enabled": True, "backend": "hash"})
    # The old form's normalized value is unchanged, so every existing reader
    # (and the equality the worker checks) sees what it saw before.
    assert config == EmbeddingConfig(enabled=True, backend="hash")
    (main,) = config.vector_sets
    assert main == VectorSetConfig(name=DEFAULT_SET_NAME, backend="hash")
    assert main.subtypes is None
    assert main.max_chars == 24_000
    assert main.default_template == DocumentTemplate(header=("subtype", "title"), body="text")
    assert main.index == VectorIndexConfig(kind="hnsw", m=16, ef_construction=64,
                                           ef_search=100, max_scan_tuples=20_000)
    assert config.search_vector_set == main


def test_disabled_with_reason_stays_valid_and_has_no_sets() -> None:
    config = _parse({"enabled": False, "reason": "not yet"})
    assert config.vector_sets == () and config.reason == "not yet"
    off = _parse({"enabled": False, "sets": {"main": {"backend": "hash"}}})
    assert off.sets and off.vector_sets == ()


def test_sets_form_parses_every_field() -> None:
    config = _parse({
        "enabled": True,
        "search_set": "main",
        "sets": {
            "main": {
                "backend": OR_768,
                "subtypes": ["jira_issue", "code_file"],
                "max_chars": 8000,
                "document_prefix": "doc: ",
                "query_prefix": "query: ",
                "provider": "google-vertex",
                "document": {
                    "default": {"header": ["subtype", "title"], "body": "text"},
                    "types": {
                        "jira_issue": {"header": ["subtype", "title", "status"], "body": "text"},
                        "code_file": {"header": ["subtype", "path"]},
                    },
                },
                "index": {
                    "kind": "hnsw",
                    "build": {"m": 24, "ef_construction": 80},
                    "search": {"ef_search": 200, "max_scan_tuples": 50000},
                },
            },
            "local": {"backend": "hash", "index": {"kind": "none"}},
        },
    })
    assert config.backend == OR_768 and config.search_set == "main"
    main = config.search_vector_set
    assert main.subtypes == ("code_file", "jira_issue")
    assert main.width == 768 and main.max_chars == 8000
    assert main.template_for("jira_issue").header == ("subtype", "title", "status")
    assert main.template_for("code_file") == DocumentTemplate(header=("subtype", "path"))
    assert main.template_for("other") == main.default_template
    assert main.index == VectorIndexConfig("hnsw", 24, 80, 200, 50000)
    local = next(item for item in config.vector_sets if item.name == "local")
    assert local.width == 2560 and local.index.kind == "none"


@pytest.mark.parametrize(
    ("block", "message"),
    [
        ({"enabled": True, "sets": {"main": {"backend": "hash"}}, "surprise": 1}, "unknown"),
        (_block(colour="red"), "unknown key"),
        (_block(document={"defaults": {}}), "unknown key"),
        (_block(document={"default": {"header": ["title"], "fmt": 1}}), "unknown key"),
        (_block(index={"kind": "hnsw", "probes": 3}), "unknown key"),
        (_block(index={"build": {"m": 16, "lists": 4}}), "unknown key"),
        (_block(index={"search": {"ef": 4}}), "unknown key"),
        ({"enabled": True, "backend": "hash", "sets": {"main": {"backend": "hash"}}}, "both"),
        ({"enabled": True, "backend": "hash", "search_set": "main"}, "needs a 'sets'"),
        ({"enabled": True, "search_set": "other", "sets": {"main": {"backend": "hash"}}}, "names no set"),
        ({"enabled": True, "sets": {"a": {"backend": "hash"}, "b": {"backend": OR_768}}}, "search_set"),
        ({"enabled": True, "sets": {}}, "non-empty"),
        ({"enabled": True, "sets": {"Main": {"backend": "hash"}}}, "must match"),
        ({"enabled": True, "sets": {"main": {}}}, "must name a backend"),
        (_block(backend="word2vec"), "backend is invalid"),
        (_block(backend="openrouter:m@0"), "backend is invalid"),
        (_block(subtypes=[]), "subtypes"),
        (_block(subtypes="some"), "subtypes"),
        (_block(subtypes=["a", "a"]), "twice"),
        (_block(max_chars=0), "max_chars must be in"),
        (_block(max_chars=200_001), "max_chars must be in"),
        (_block(max_chars=True), "must be an integer"),
        (_block(document_prefix=3), "must be a string"),
        (_block(query_prefix="x" * 1001), "at most 1000"),
        ({"enabled": True, "sets": {"main": {"backend": "hash", "provider": "openai"}}}, "only to an openrouter"),
        (_block(provider="bad name!"), "provider name"),
        (_block(document={"default": {"header": "title"}}), "list of attribute names"),
        (_block(document={"default": {"header": [f"a{i}" for i in range(17)]}}), "at most 16"),
        (_block(document={"default": {"header": ["title", "title"]}}), "twice"),
        (_block(document={"default": {"header": ["bad-name"]}}), "not an attribute name"),
        (_block(document={"default": {"header": ["owner_id"]}}), "an id"),
        (_block(document={"default": {"header": ["text_hash"]}}), "a hash"),
        (_block(document={"default": {"header": ["updated_at"]}}), "a timestamp"),
        (_block(document={"default": {"header": ["okg_producer_text"]}}), "system attribute"),
        (_block(document={"default": {"header": ["classification"]}}), "system attribute"),
        (_block(document={"default": {"header": ["text"]}}), "document body"),
        (_block(document={"default": {"body": "summary"}}), "body must be one of"),
        (_block(subtypes=["a"], document={"types": {"b": {"header": ["title"]}}}), "leave out"),
        (_block(index={"kind": "ivfflat"}), "kind must be one of"),
        (_block(index={"kind": "none", "build": {"m": 16}}), "takes no build"),
        (_block(index={"build": {"m": 1}}), "build.m must be in"),
        (_block(index={"build": {"m": 101}}), "build.m must be in"),
        (_block(index={"build": {"m": 16, "ef_construction": 31}}), "at least 2\\*m"),
        (_block(index={"build": {"ef_construction": 1001}}), "ef_construction must be in"),
        (_block(index={"search": {"ef_search": 0}}), "ef_search must be in"),
        (_block(index={"search": {"ef_search": 1001}}), "ef_search must be in"),
        (_block(index={"search": {"max_scan_tuples": 0}}), "max_scan_tuples must be in"),
        (
            {"enabled": True, "search_set": "a",
             "sets": {"a": {"backend": OR_768}, "b": {"backend": OR_768, "subtypes": ["x"]}}},
            "same identity",
        ),
    ],
)
def test_each_rule_refuses(block: dict, message: str) -> None:
    with pytest.raises(EmbeddingConfigError, match=message):
        _parse(block)


def test_duplicate_identity_names_the_deployment_key_and_storage() -> None:
    block = {
        "enabled": True, "search_set": "a",
        "sets": {"a": {"backend": OR_768}, "b": {"backend": OR_768, "subtypes": ["x"]}},
    }
    with pytest.raises(EmbeddingConfigError) as caught:
        _parse(block)
    message = str(caught.value)
    assert message.startswith("deployment.yaml key 'embeddings': sets 'a' and 'b'")
    assert "storage" in message


def test_index_width_bounds() -> None:
    assert _parse_index({}, "w", width=4000).kind == "hnsw"
    with pytest.raises(EmbeddingConfigError, match="at most 4000"):
        _parse_index({}, "w", width=4001)
    assert _parse_index({"kind": "none"}, "w", width=16000).kind == "none"
    with pytest.raises(EmbeddingConfigError, match="at most 16000"):
        _parse_index({"kind": "none"}, "w", width=16001)


def test_identity_ignores_subtypes_templates_and_index() -> None:
    base = _parse(_block()).search_vector_set
    other = _parse(_block(
        subtypes=["jira_issue"],
        document={"default": {"header": ["subtype"]},
                  "types": {"jira_issue": {"header": ["subtype", "status"]}}},
        index={"build": {"m": 32, "ef_construction": 128}, "search": {"ef_search": 300}},
    )).search_vector_set
    assert base.set_id == other.set_id
    assert base.set_id.startswith("es_") and len(base.set_id) == 27


@pytest.mark.parametrize(
    "override",
    [
        {"backend": "openrouter:google/gemini-embedding-2@512"},  # width
        {"backend": "openrouter:openai/text-embedding-3-small@768"},  # model
        {"max_chars": 12_000},
        {"document_prefix": "passage: "},
        {"query_prefix": "query: "},
        {"provider": "google-vertex"},
    ],
)
def test_identity_changes_with_each_identity_field(override: dict) -> None:
    base = _parse(_block()).search_vector_set
    changed = _parse(_block(**override)).search_vector_set
    assert changed.set_id != base.set_id


def test_identity_changes_with_the_document_format_version(monkeypatch) -> None:
    before = _parse(_block()).search_vector_set.set_id
    monkeypatch.setattr(config_module, "DOCUMENT_FORMAT_VERSION", 2)
    assert _parse(_block()).search_vector_set.set_id != before


def test_index_name_follows_build_settings_only() -> None:
    base = _parse(_block()).search_vector_set
    searched = _parse(_block(index={"search": {"ef_search": 400}})).search_vector_set
    rebuilt = _parse(_block(index={"build": {"m": 32, "ef_construction": 64}})).search_vector_set
    assert set_index_name(base) == set_index_name(searched)
    assert set_index_name(base) != set_index_name(rebuilt)
    assert set_index_name(base).startswith("text_embeddings_" + base.set_id[3:])
    assert len(set_index_name(base)) <= 63


def test_parsing_does_not_mutate_the_block() -> None:
    block = _block(document={"types": {"x": {"header": ["subtype", "title"]}}})
    before = copy.deepcopy(block)
    _parse(block)
    assert block == before
