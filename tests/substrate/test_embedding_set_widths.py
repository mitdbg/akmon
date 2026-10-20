"""Vector-set widths follow pgvector's limits (PACT vector-set-pgvector-widths).

No database. A set stores each model at its own width: ``hash@<dims>``,
``openrouter:<model>@<dims>`` above 2560, and a sentence-transformers model's
own width. pgvector's halfvec limits are the only bounds: 4000 dimensions
under an HNSW index, 16000 stored. The old single ``backend:`` form and the
node column keep the 2560 cap, the padding and the width checks.
"""
from __future__ import annotations

import sys
import types

import httpx
import pytest

from okg.substrate.embeddings import HashEmbedder
from okg.substrate.embeddings.config import (
    EmbeddingConfig,
    EmbeddingConfigError,
    VectorSetConfig,
    build_embedding_execution_plan,
    build_vector_set_plan,
    parse_embedding_config,
)
from okg.substrate.embeddings.identity import (
    EMBEDDING_DIMENSION,
    build_embedder,
    model_id_for_spec,
    reset_sentence_transformer_cache,
)
from okg.substrate.embeddings.openrouter import OpenRouterEmbedder
from okg.substrate.errors import SubstrateError


#: The id `hash` with the default settings had before this change; a set
#: of the old spelling keeps it, so no registered set needs a new fill.
HASH_SET_ID = "es_125bf90624f9af6c247b8e89"
MODEL = "google/gemini-embedding-2"


def _sets(backend: str, kind: str = "hnsw") -> dict:
    return {"enabled": True, "sets": {"main": {
        "backend": backend, "index": {"kind": kind},
    }}}


def _set(backend: str, kind: str = "hnsw") -> VectorSetConfig:
    return parse_embedding_config(plural=_sets(backend, kind)).search_vector_set


def _peer(width: int):
    def answer(request):
        import json

        inputs = json.loads(request.content)["input"]
        data = [
            {"index": i, "embedding": HashEmbedder(width).embed(text)}
            for i, text in enumerate(inputs)
        ]
        return httpx.Response(200, json={
            "model": "gemini-embedding-2", "data": data,
            "usage": {"prompt_tokens": 1, "total_tokens": 1, "cost": 0.0},
        })
    return answer


def _openrouter(width: int, *, pad_to_column: bool) -> OpenRouterEmbedder:
    return OpenRouterEmbedder(
        model=MODEL, native_dimension=width, api_key="sk-test",
        transport=httpx.MockTransport(_peer(width)), sleep=lambda _s: None,
        pad_to_column=pad_to_column,
    )


# ── identities: existing sets keep their id ───────────────────────────


def test_plain_hash_keeps_its_set_id_and_hash_at_dims_is_a_new_set() -> None:
    plain = _set("hash")
    assert plain.width == EMBEDDING_DIMENSION
    assert plain.set_id == HASH_SET_ID
    assert _set("hash@2560").set_id == HASH_SET_ID, "hash@2560 is the same model"
    wide = _set("hash@3000")
    assert (wide.model_id, wide.width) == ("hash:3000", 3000)
    assert wide.set_id != HASH_SET_ID
    assert _set("hash@3001").set_id != wide.set_id
    # An OpenRouter set at 768 keeps its id too (computed on the base code).
    assert _set(f"openrouter:{MODEL}@768").set_id == "es_980b29503cabca8841f20386"


def test_a_set_reads_its_width_from_the_backend_spec() -> None:
    assert _set(f"openrouter:{MODEL}@3072", "none").width == 3072
    assert _set(f"openrouter:{MODEL}@4000").width == 4000
    assert _set("hash@768").width == 768


# ── pgvector's halfvec limits ─────────────────────────────────────────


def test_hnsw_above_4000_refuses_naming_pgvectors_halfvec_hnsw_limit() -> None:
    for backend in ("hash@4001", f"openrouter:{MODEL}@5000"):
        with pytest.raises(EmbeddingConfigError) as caught:
            _set(backend, "hnsw")
        message = str(caught.value)
        assert "pgvector's halfvec HNSW index takes at most 4000" in message, message
    assert _set("hash@4000", "hnsw").width == 4000


def test_index_kind_none_takes_up_to_16000() -> None:
    assert _set("hash@4001", "none").width == 4001
    assert _set("hash@16000", "none").width == 16000
    for kind in ("none", "hnsw"):
        with pytest.raises(EmbeddingConfigError) as caught:
            _set("hash@16001", kind)
        assert "pgvector's halfvec type stores at most 16000" in str(caught.value)


def _stored(backend: str, kind: str, storage: str) -> VectorSetConfig:
    block = _sets(backend, kind)
    block["sets"]["main"]["storage"] = storage
    return parse_embedding_config(plural=block).search_vector_set


def test_storage_halfvec_is_the_default_and_keeps_every_id() -> None:
    assert _stored("hash", "hnsw", "halfvec").set_id == HASH_SET_ID
    assert _set("hash").storage == "halfvec"
    # 2560 wide: a vector set takes it only unindexed (HNSW stops at 2000).
    full = _stored("hash", "none", "vector")
    assert full.storage == "vector" and full.set_id != HASH_SET_ID
    assert full.identity()["storage"] == "vector"
    assert (full.vector_column, full.vector_type, full.cosine_opclass) == (
        "embedding_f32", "vector(2560)", "vector_cosine_ops",
    )
    plain = _set("hash@768")
    assert (plain.vector_column, plain.vector_type, plain.cosine_opclass) == (
        "embedding", "halfvec(768)", "halfvec_cosine_ops",
    )
    for bad in ("float32", "bit", ""):
        with pytest.raises(EmbeddingConfigError, match="storage must be one of"):
            _stored("hash", "hnsw", bad)
    with pytest.raises(EmbeddingConfigError, match="storage must be one of"):
        VectorSetConfig(name="main", backend="hash", storage="vector; DROP TABLE x")


def test_vector_storage_follows_pgvectors_vector_limits() -> None:
    assert _stored("hash@2000", "hnsw", "vector").width == 2000
    with pytest.raises(EmbeddingConfigError) as caught:
        _stored("hash@2001", "hnsw", "vector")
    assert "pgvector's vector HNSW index takes at most 2000" in str(caught.value)
    # 2001..4000 under HNSW is a halfvec-only width.
    assert _stored("hash@2001", "hnsw", "halfvec").width == 2001
    assert _stored("hash@16000", "none", "vector").width == 16000
    with pytest.raises(EmbeddingConfigError) as caught:
        _stored("hash@16001", "none", "vector")
    assert "pgvector's vector type stores at most 16000" in str(caught.value)


def test_hash_at_dims_must_be_a_positive_integer() -> None:
    for bad in ("hash@0", "hash@-3", "hash@x", "hash@"):
        with pytest.raises(EmbeddingConfigError):
            _set(bad)


# ── the old single backend form is unchanged ──────────────────────────


def test_the_single_backend_form_keeps_the_2560_cap() -> None:
    with pytest.raises(EmbeddingConfigError, match=r"dims must be in 1\.\.2560; got 3000"):
        parse_embedding_config(plural={"enabled": True, "backend": f"openrouter:{MODEL}@3000"})
    with pytest.raises(EmbeddingConfigError, match="vector-set backend"):
        parse_embedding_config(plural={"enabled": True, "backend": "hash@3000"})
    with pytest.raises(SubstrateError):
        model_id_for_spec("hash@2560")
    assert model_id_for_spec(f"openrouter:{MODEL}@3000", vector_set=True) == (
        f"openrouter:{MODEL}@3000"
    )


def test_the_single_backend_form_still_pads_to_the_node_column(monkeypatch) -> None:
    plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend=f"openrouter:{MODEL}@768"),
        builder=lambda _spec: _openrouter(768, pad_to_column=True),
    )
    assert plan.node_column is True
    assert plan.observed_dimension == EMBEDDING_DIMENSION
    vector = plan.embed("text")
    assert len(vector) == EMBEDDING_DIMENSION and vector[768:] == [0.0] * 1792
    # The node column's default build pads; a set's build does not.
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    assert build_embedder(f"openrouter:{MODEL}@768").dimension == EMBEDDING_DIMENSION
    assert build_embedder(f"openrouter:{MODEL}@768", vector_set=True).dimension == 768
    assert build_embedder(f"openrouter:{MODEL}@3000", vector_set=True).dimension == 3000
    with pytest.raises(ValueError, match=r"1\.\.2560"):
        _openrouter(3000, pad_to_column=True)


# ── plans at a set's own width ────────────────────────────────────────


def test_a_wide_hash_search_set_plan_works_at_its_own_width() -> None:
    config = parse_embedding_config(plural=_sets("hash@3000"))
    plan = build_embedding_execution_plan(config)
    assert plan.node_column is False
    assert (plan.model_id, plan.observed_dimension) == ("hash:3000", 3000)
    assert len(plan.embed("text")) == 3000
    # The 2560-wide hash set keeps the node column's plan, as before.
    plain = build_embedding_execution_plan(parse_embedding_config(plural=_sets("hash")))
    assert plain.node_column is True and plain.observed_dimension == EMBEDDING_DIMENSION


def test_a_wide_openrouter_set_plan_is_unpadded() -> None:
    vector_set = _set(f"openrouter:{MODEL}@3000", "none")
    plan = build_vector_set_plan(
        vector_set, builder=lambda _spec: _openrouter(3000, pad_to_column=False),
    )
    assert plan.observed_dimension == 3000 and plan.node_column is False
    assert len(plan.embed("text")) == 3000
    # A padded backend does not have the set's width: refused, not cut.
    with pytest.raises(SubstrateError) as caught:
        build_vector_set_plan(
            _set(f"openrouter:{MODEL}@768"),
            builder=lambda _spec: _openrouter(768, pad_to_column=True),
        )
    assert caught.value.code == "embedder_dim_mismatch"
    # A 768 search set in the `sets` form keeps the padded node-column plan.
    padded = build_embedding_execution_plan(
        parse_embedding_config(plural=_sets(f"openrouter:{MODEL}@768")),
        builder=lambda _spec: _openrouter(768, pad_to_column=True),
    )
    assert padded.node_column is True and padded.observed_dimension == EMBEDDING_DIMENSION


def test_a_set_plan_width_mismatch_names_the_set_width() -> None:
    import dataclasses

    plan = build_vector_set_plan(_set("hash@3000"))
    # The backend's output changes width after the probe.
    drifted = dataclasses.replace(plan, embedder=HashEmbedder(10))
    with pytest.raises(SubstrateError) as caught:
        drifted.embed("text")
    assert caught.value.code == "embedder_dim_mismatch"
    assert "the vector set, which is 3000 wide" in str(caught.value)
    assert "2560-dimensional column" not in str(caught.value)


def test_a_set_plan_refuses_another_model() -> None:
    with pytest.raises(SubstrateError) as caught:
        build_vector_set_plan(_set("hash@3000"), builder=lambda _spec: HashEmbedder())
    assert caught.value.code == "embedding_model_skew"


# ── sentence-transformers: the model's own width ──────────────────────


class _StandInModel:
    """A sentence-transformers model reporting 768 dimensions; its vectors
    are the hash embedder's at that width."""

    loads: list[str] = []

    def __init__(self, name: str, device=None) -> None:
        self.loads.append(name)

    def get_sentence_embedding_dimension(self) -> int:
        return 768

    def encode(self, texts, **_kwargs):
        return [HashEmbedder(768).embed(text) for text in texts]


@pytest.fixture
def stand_in_model(monkeypatch):
    module = types.ModuleType("sentence_transformers")
    module.SentenceTransformer = _StandInModel  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentence_transformers", module)
    _StandInModel.loads = []
    reset_sentence_transformer_cache()
    yield _StandInModel
    reset_sentence_transformer_cache()


def test_a_sentence_transformers_set_takes_its_width_from_the_model(stand_in_model) -> None:
    config = parse_embedding_config(plural=_sets("sentence-transformers:okg-stand-in"))
    assert stand_in_model.loads == [], "parsing loads no model"
    vector_set = config.search_vector_set
    assert vector_set.width == 768
    plan = build_embedding_execution_plan(config)
    assert plan.node_column is False and plan.observed_dimension == 768
    assert len(plan.embed("text")) == 768
    assert stand_in_model.loads == ["okg-stand-in"], "the model loads once"


def test_the_single_backend_form_still_refuses_a_768_model(stand_in_model) -> None:
    config = parse_embedding_config(
        plural={"enabled": True, "backend": "sentence-transformers:okg-stand-in"},
    )
    (vector_set,) = config.vector_sets
    assert vector_set.width == EMBEDDING_DIMENSION, "no model load for its width"
    with pytest.raises(SubstrateError) as caught:
        build_embedding_execution_plan(config)
    assert caught.value.code == "embedder_dim_mismatch"


# ── the node-column users skip a plan at a set's own width ────────────


def test_should_register_embed_reads_a_wide_set_backend() -> None:
    from types import SimpleNamespace

    from okg.substrate.runtime.services import should_register_embed

    config = parse_embedding_config(plural=_sets("hash@3000"))
    assert should_register_embed(SimpleNamespace(embedding_config=config)) is True
    paid = parse_embedding_config(plural=_sets(f"openrouter:{MODEL}@3000", "none"))
    assert should_register_embed(SimpleNamespace(embedding_config=paid)) is False


def test_alias_resolution_stays_sql_only_for_a_wide_set(tmp_path) -> None:
    import json
    from pathlib import Path

    from okg.substrate.alias.loader import load_alias_resolver
    from tests.substrate.mcp.search_fixture import make_search_deployment

    deployment = Path(make_search_deployment(tmp_path, dsn="postgres://u@localhost/x"))
    manifest = json.loads((deployment / "deployment.yaml").read_text())
    manifest["embeddings"] = _sets("hash@3000")
    (deployment / "deployment.yaml").write_text(json.dumps(manifest))
    plan = build_embedding_execution_plan(parse_embedding_config(plural=_sets("hash@3000")))
    resolver = load_alias_resolver(deployment, conn=object(), embedding_plan=plan)
    assert [type(b).__name__ for b in resolver._backends] == ["SqlCascadeBackend"]


def test_the_node_column_worker_skips_a_wide_search_set(tmp_path) -> None:
    import json
    from pathlib import Path

    from okg.substrate.runtime.embed_workflow import _run_one_embed_batch
    from tests.substrate.mcp.search_fixture import make_search_deployment

    deployment = Path(make_search_deployment(tmp_path, dsn="postgres://u@localhost/x"))
    manifest = json.loads((deployment / "deployment.yaml").read_text())
    manifest["embeddings"] = _sets("hash@3000")
    (deployment / "deployment.yaml").write_text(json.dumps(manifest))
    plan = build_embedding_execution_plan(parse_embedding_config(plural=_sets("hash@3000")))

    def must_not_embed(*_args, **_kwargs):
        raise AssertionError("the node column was embedded at a set's width")

    outcome = _run_one_embed_batch(
        deployment=str(deployment), batch_size=10,
        embed_one=must_not_embed, embedding_plan=plan,
    )
    assert outcome["skipped"] is True
    assert outcome["reason"] == "search_set_width_not_node_column"
    assert outcome["processed"] == 0 and outcome["errors"] == 0


def test_a_declared_embedding_alias_backend_refuses_a_wide_set_plan() -> None:
    from okg.substrate.alias.loader import AliasBackendLoadError, _instantiate_backend

    plan = build_embedding_execution_plan(parse_embedding_config(plural=_sets("hash@3000")))
    with pytest.raises(AliasBackendLoadError, match="node column"):
        _instantiate_backend(
            "okg.substrate.alias.backends.embedding.EmbeddingAliasBackend",
            None, object(), plan,
        )
