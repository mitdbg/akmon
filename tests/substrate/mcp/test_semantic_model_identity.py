"""Cross-model ranking is refused against REAL published vectors (#797).

Every test here establishes skew at MATCHING DIMENSIONS, which is the silent
case: the corrected width guard would catch a dimension mismatch, so a test
built on differing widths would pass through that guard and prove nothing about
model IDENTITY. One case uses two distinct sentence-transformer identities —
the pair the previous class-derived derivation (`SentenceTransformerEmbedder:2560`
for every model) could not tell apart.

Each refusal is paired with a CONTROL on the same published generation showing
the matching embedder ranks normally, so no test can pass merely because the
search returned nothing.

Reaches the substrate through psycopg + `open_session`, NOT `build_server`:
this suite tests the lower-level published-row search path directly.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.embeddings import HashEmbedder, embedder_model_id
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import open_session, search
from tests.substrate.mcp.semantic_fixture import (
    make_search_deployment,
    node,
    publish_with_vectors,
)


pytestmark = pytest.mark.postgres

QUERY = "background estimation"


def _corpus(embedder):
    """Two nodes whose vectors are real, one of them the query vector exactly."""
    return (
        [
            node("chunk:a", "chunk", QUERY),
            node("chunk:b", "chunk", QUERY + " method"),
        ],
        {
            "chunk:a": embedder.embed(QUERY),
            "chunk:b": embedder.embed(QUERY + " method"),
        },
    )


def test_matching_dimension_model_skew_is_refused(
    clean_runtime: str, tmp_path,
) -> None:
    """A corpus written by another model at the SAME width is refused.

    The dimensions match, so the width guard cannot be what catches this; only
    the recorded identity can.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    embedder = HashEmbedder()
    nodes, vectors = _corpus(embedder)
    # A DIFFERENT model, at the substrate width. Named explicitly rather than
    # relying on an arbitrary literal.
    other_model = "sentence-transformers:bge-large-en-v1.5"
    assert other_model != embedder_model_id(embedder)
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk"], nodes=nodes, vectors=vectors,
        model_id=other_model,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(SubstrateError) as exc:
            search(
                conn, session, QUERY, method="semantic", embedder=embedder,
            )
    assert exc.value.code == "embedding_model_skew"
    # BOTH identities and the row count behind each, so the operator learns
    # which embedder to configure -- not merely that something mismatched.
    assert exc.value.data["query_model_id"] == embedder_model_id(embedder)
    assert exc.value.data["corpus_models"] == {other_model: 2}
    assert other_model in str(exc.value)


def test_the_matching_embedder_ranks_the_same_corpus(
    clean_runtime: str, tmp_path,
) -> None:
    """CONTROL for the refusal above: the check is not refusing everything."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    embedder = HashEmbedder()
    nodes, vectors = _corpus(embedder)
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk"], nodes=nodes, vectors=vectors,
        model_id=embedder_model_id(embedder),
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        result = search(
            conn, session, QUERY, method="semantic", embedder=embedder,
        )
    ids = [r["node_id"] for r in result["results"]]
    assert ids, "the matching embedder must rank the corpus"
    # chunk:a's text IS the query, so it is the nearest vector.
    assert ids[0] == "chunk:a"
    assert result["embedder_model_id"] == embedder_model_id(embedder)
    # Verified provenance is labelled as such, per row.
    assert all(
        r["embedding_provenance"] == embedder_model_id(embedder)
        for r in result["results"]
    )


def test_two_sentence_transformer_models_are_distinguished(
    clean_runtime: str, tmp_path,
) -> None:
    """The case the OLD derivation could not see.

    Both identities would have been `SentenceTransformerEmbedder:2560` under
    `f"{type(embedder).__name__}:{embedder.dimension}"`, so the corpus and the
    query would have compared EQUAL and the ranking would have been noise
    reported as relevance. The query embedder is a stub carrying a
    sentence-transformer identity, so no model weights are downloaded.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    hash_embedder = HashEmbedder()
    nodes, vectors = _corpus(hash_embedder)
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk"], nodes=nodes, vectors=vectors,
        model_id="sentence-transformers:all-mpnet-base-v2",
    )

    class _StubSentenceTransformer:
        """Declares a DIFFERENT sentence-transformer identity, same width."""

        dimension = hash_embedder.dimension
        model_id = "sentence-transformers:bge-large-en-v1.5"

        def embed(self, text: str) -> list[float]:
            return hash_embedder.embed(text)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(SubstrateError) as exc:
            search(
                conn, session, QUERY, method="semantic",
                embedder=_StubSentenceTransformer(),
            )
    assert exc.value.code == "embedding_model_skew"
    assert "all-mpnet-base-v2" in str(exc.value)
    assert "bge-large-en-v1.5" in str(exc.value)


def test_null_embedding_model_is_unknown_and_refused_by_default(
    clean_runtime: str, tmp_path,
) -> None:
    """A real vector with NO recorded model is unknown, not verified.

    This is the state a live migration leaves on rows embedded before the
    column existed. Reading it as a match would report a verified model having
    verified nothing, on exactly the rows most likely to carry a stale one.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    embedder = HashEmbedder()
    nodes, vectors = _corpus(embedder)
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk"], nodes=nodes, vectors=vectors,
        model_id=None,  # NULL embedding_model
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(SubstrateError) as exc:
            search(
                conn, session, QUERY, method="semantic", embedder=embedder,
            )
        assert exc.value.code == "embedding_model_skew"
        assert exc.value.data["unknown_provenance_rows"] == 2

        # The opt-in ADMITS them, labels each, and reports the count.
        result = search(
            conn, session, QUERY, method="semantic", embedder=embedder,
            allow_unverified_provenance=True,
        )
    assert [r["node_id"] for r in result["results"]][:1] == ["chunk:a"]
    assert result["unverified_provenance_admitted"] == 2
    assert all(
        r["embedding_provenance"] == "unknown" for r in result["results"]
    )


def test_optin_admits_unknown_but_not_a_known_different_model(
    clean_runtime: str, tmp_path,
) -> None:
    """Unverified is not the same permission as wrong.

    One row unknown, one row from a named other model: the opt-in covers the
    first and must NOT cover the second, or it would be a blanket disable.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    embedder = HashEmbedder()
    nodes, vectors = _corpus(embedder)
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk"], nodes=nodes, vectors=vectors,
        model_id=None,
        per_node_model_id={
            "chunk:a": None,
            "chunk:b": "sentence-transformers:gte-Qwen2-7B-instruct",
        },
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(SubstrateError) as exc:
            search(
                conn, session, QUERY, method="semantic", embedder=embedder,
                allow_unverified_provenance=True,
            )
    assert exc.value.code == "embedding_model_skew"
    assert "gte-Qwen2-7B-instruct" in str(exc.value)


def test_passing_both_an_embedder_and_a_spec_raises(
    clean_runtime: str, tmp_path,
) -> None:
    """A caller who names two embedders has a belief about which is used."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    embedder = HashEmbedder()
    nodes, vectors = _corpus(embedder)
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk"], nodes=nodes, vectors=vectors,
        model_id=embedder_model_id(embedder),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(ValueError, match="not both"):
            search(
                conn, session, QUERY, method="semantic",
                embedder=embedder, embedder_spec="hash",
            )
        # And an embedder spec on a non-semantic method is refused too.
        with pytest.raises(ValueError, match="only to method='semantic'"):
            search(
                conn, session, QUERY, method="lexical", embedder_spec="hash",
            )


def test_the_spec_path_ranks_and_reports_its_resolution(
    clean_runtime: str, tmp_path,
) -> None:
    """`embedder_spec` is the deferred-construction path the CLI uses."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    embedder = HashEmbedder()
    nodes, vectors = _corpus(embedder)
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk"], nodes=nodes, vectors=vectors,
        model_id=embedder_model_id(embedder),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        result = search(
            conn, session, QUERY, method="semantic",
            embedder_spec="hash", embedder_source="argument",
        )
    assert [r["node_id"] for r in result["results"]][:1] == ["chunk:a"]
    assert result["embedder_spec"] == "hash"
    assert result["embedder_source"] == "argument"
    assert result["embedder_model_id"] == embedder_model_id(embedder)


# ── HNSW ranking for a model narrower than the column ─────────────────
# PACT openrouter-embeddings-hnsw. An `openrouter:<model>@<dims>` corpus is
# ranked through a model-scoped HNSW index read directly from the node
# segment under the pin's visibility predicate; without the index the search
# refuses instead of scanning every vector.

OR_MODEL_ID = "openrouter:google/gemini-embedding-2@768"


def _or_vector(seed: int) -> list[float]:
    """A unit 768-wide vector, zero-padded to the column, as the backend writes."""
    import math

    raw = [math.sin(seed * 12.9898 + i * 78.233) for i in range(768)]
    norm = math.sqrt(sum(v * v for v in raw))
    return [v / norm for v in raw] + [0.0] * (2560 - 768)


class _OpenRouterShaped:
    """The query side: the openrouter identity, vectors from a fixed table."""

    dimension = 2560
    model_id = OR_MODEL_ID

    def __init__(self, table: dict[str, list[float]]) -> None:
        self.table = table

    def embed(self, text: str) -> list[float]:
        return self.table[text]


def _or_corpus(count: int, *, prefix: str = "chunk") -> tuple[list, dict, dict]:
    nodes, vectors, by_text = [], {}, {}
    for i in range(count):
        node_id, text = f"{prefix}:{i:04d}", f"{prefix} passage {i}"
        nodes.append(node(node_id, "chunk", text))
        vectors[node_id] = by_text[text] = _or_vector((0 if prefix == "chunk" else 5000) + i)
    return nodes, vectors, by_text


def _hnsw_scans(conn) -> int:
    from okg.substrate.semantic_ann import ANN_INDEX_SUFFIX

    conn.execute("SELECT pg_stat_force_next_flush()")
    row = conn.execute(
        "SELECT coalesce(sum(idx_scan), 0) FROM pg_stat_user_indexes "
        "WHERE indexrelname LIKE %s",
        (f"%{ANN_INDEX_SUFFIX}",),
    ).fetchone()
    return int(row[0])


def test_indexed_model_without_its_index_is_refused(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.semantic_ann import SemanticIndexUnavailable

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes, vectors, by_text = _or_corpus(3)
    publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"], nodes=nodes,
        vectors=vectors, model_id=OR_MODEL_ID,
    )
    embedder = _OpenRouterShaped(by_text)
    embedded: list[str] = []
    original = embedder.embed
    embedder.embed = lambda text: (embedded.append(text), original(text))[1]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(SemanticIndexUnavailable) as exc:
            search(
                conn, session, "chunk passage 1", method="semantic",
                embedder=embedder,
            )
    assert exc.value.code == "semantic_index_unavailable"
    assert exc.value.data["status"] == "missing"
    # Refused before the query is embedded: with a paid backend, no call.
    assert embedded == []


def test_indexed_model_ranks_through_the_hnsw_index(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.semantic_ann import ann_spec_for_model_id, ensure_ann_index

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes, vectors, by_text = _or_corpus(400)
    publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"], nodes=nodes,
        vectors=vectors, model_id=OR_MODEL_ID,
    )
    spec = ann_spec_for_model_id(OR_MODEL_ID)
    built = ensure_ann_index(clean_runtime, spec)
    assert built["action"] == "build" and built["status"] == "present"

    query_text = "chunk passage 137"
    q = by_text[query_text]
    exact = sorted(
        vectors,
        key=lambda nid: (1.0 - sum(a * b for a, b in zip(vectors[nid], q)), nid),
    )[:10]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Current statistics, as autovacuum keeps them. With them the planner
        # alone would read the visibility B-tree and sort these 400 rows; the
        # ranking must still come from the HNSW index.
        conn.execute(f"ANALYZE public.\"{built['physical_table']}\"")
        before = _hnsw_scans(conn)
        session = open_session(conn, deployment_name=deployment)
        result = search(
            conn, session, query_text, method="semantic",
            embedder=_OpenRouterShaped(by_text), limit=10,
        )
        after = _hnsw_scans(conn)
    ids = [r["node_id"] for r in result["results"]]
    assert ids[0] == "chunk:0137"
    # Recall@10 against the exact cosine ranking of the same vectors.
    assert len(set(ids) & set(exact)) >= 9, (ids, exact)
    assert after > before, "the ranking must read the HNSW index"
    assert all(r["embedding_provenance"] == OR_MODEL_ID for r in result["results"])


def test_hnsw_ranking_honours_the_pinned_generation(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.semantic_ann import ann_spec_for_model_id, ensure_ann_index

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    first, first_vectors, by_text = _or_corpus(20)
    gen1 = publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"], nodes=first,
        vectors=first_vectors, model_id=OR_MODEL_ID,
    )
    ensure_ann_index(clean_runtime, ann_spec_for_model_id(OR_MODEL_ID))
    late, late_vectors, late_text = _or_corpus(1, prefix="late")
    by_text.update(late_text)
    gen2 = publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"], nodes=late,
        vectors=late_vectors, model_id=OR_MODEL_ID,
    )
    assert gen1 != gen2
    embedder = _OpenRouterShaped(by_text)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        old = search(
            conn, open_session(conn, deployment_name=deployment, requested_generation=gen1),
            "late passage 0", method="semantic", embedder=embedder, limit=5,
        )
        new = search(
            conn, open_session(conn, deployment_name=deployment, requested_generation=gen2),
            "late passage 0", method="semantic", embedder=embedder, limit=5,
        )
    assert "late:0000" not in [r["node_id"] for r in old["results"]]
    assert [r["node_id"] for r in new["results"]][0] == "late:0000"


def test_ensure_ann_index_reports_builds_and_rebuilds(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.semantic_ann import ann_spec_for_model_id, ensure_ann_index

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes, vectors, _ = _or_corpus(5)
    publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"], nodes=nodes,
        vectors=vectors, model_id=OR_MODEL_ID,
    )
    spec = ann_spec_for_model_id(OR_MODEL_ID)
    assert ensure_ann_index(clean_runtime, spec, apply=False)["action"] == "would_build"
    assert ensure_ann_index(clean_runtime, spec)["action"] == "build"
    assert ensure_ann_index(clean_runtime, spec)["action"] == "none"
    other = ann_spec_for_model_id("openrouter:google/gemini-embedding-2@512")
    dry = ensure_ann_index(clean_runtime, other, apply=False)
    assert dry["status"] == "mismatched" and dry["action"] == "would_rebuild"
    rebuilt = ensure_ann_index(clean_runtime, other)
    assert rebuilt["action"] == "rebuild" and rebuilt["status"] == "present"


def test_unverified_opt_in_is_refused_for_an_indexed_model(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes, vectors, by_text = _or_corpus(2)
    publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"], nodes=nodes,
        vectors=vectors, model_id=OR_MODEL_ID,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(SubstrateError) as exc:
            search(
                conn, session, "chunk passage 1", method="semantic",
                embedder=_OpenRouterShaped(by_text),
                allow_unverified_provenance=True,
            )
    assert exc.value.code == "semantic_unverified_provenance_unindexed"


@pytest.mark.parametrize("block, builds", [
    (None, False),
    ({"enabled": False}, False),
    ({"enabled": True, "backend": "hash"}, False),
    ({"enabled": True, "backend": OR_MODEL_ID}, True),
])
def test_migrate_builds_the_index_only_for_an_indexed_model(
    clean_runtime: str, tmp_path, block, builds,
) -> None:
    """Deployments without an indexed embedding model pay nothing."""
    import json
    from pathlib import Path

    from okg.substrate.db.migrate import _ensure_semantic_ann_index

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes, vectors, _ = _or_corpus(3)
    publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"], nodes=nodes,
        vectors=vectors, model_id=OR_MODEL_ID,
    )
    manifest_path = Path(deployment) / "deployment.yaml"
    manifest = json.loads(manifest_path.read_text())
    if block is not None:
        manifest["embeddings"] = block
    manifest_path.write_text(json.dumps(manifest))
    report = _ensure_semantic_ann_index(clean_runtime, deployment)
    if builds:
        assert report is not None and report["status"] == "present"
    else:
        assert report is None


def _publish_plain(dsn: str, deployment: str) -> dict:
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    result = Publisher(
        dsn, deployment, catalog_identity=bind_test_catalog_writer(dsn, deployment),
    ).cycle()
    assert result["status"] == "published", result
    return result


def test_hnsw_read_restores_planner_settings_on_a_caller_transaction(
    clean_runtime: str, tmp_path,
) -> None:
    """On a connection already in a transaction (`okg search` uses one),
    the ANN read's settings must not outlive its statement."""
    from okg.substrate.semantic_ann import ann_spec_for_model_id, ensure_ann_index

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes, vectors, by_text = _or_corpus(20)
    publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"], nodes=nodes,
        vectors=vectors, model_id=OR_MODEL_ID,
    )
    ensure_ann_index(clean_runtime, ann_spec_for_model_id(OR_MODEL_ID))
    with psycopg.connect(clean_runtime) as conn:  # autocommit off
        conn.execute("SELECT '[1]'::vector")  # load pgvector's settings
        before = conn.execute(
            "SELECT current_setting('enable_sort'), "
            "current_setting('hnsw.ef_search', true), "
            "current_setting('hnsw.iterative_scan', true)"
        ).fetchone()
        session = open_session(conn, deployment_name=deployment)
        result = search(
            conn, session, "chunk passage 3", method="semantic",
            embedder=_OpenRouterShaped(by_text), limit=5,
        )
        assert result["results"][0]["node_id"] == "chunk:0003"
        after = conn.execute(
            "SELECT current_setting('enable_sort'), "
            "current_setting('hnsw.ef_search', true), "
            "current_setting('hnsw.iterative_scan', true)"
        ).fetchone()
    assert after[0] == "on"
    assert after == before


def test_hnsw_ranking_follows_retraction_and_update_after_the_pin(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.semantic_ann import ann_spec_for_model_id, ensure_ann_index

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes, vectors, by_text = _or_corpus(10)
    gen1 = publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"], nodes=nodes,
        vectors=vectors, model_id=OR_MODEL_ID,
    )
    ensure_ann_index(clean_runtime, ann_spec_for_model_id(OR_MODEL_ID))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                # New text: the fact write path stores no vector for it.
                node("chunk:0001", "chunk", "chunk passage 1 rewritten"),
                NodeFact(
                    node_id="chunk:0002", subtype="chunk", attrs={},
                    source_record_id={"id": "chunk:0002"},
                    source_revision={"v": 2}, op="R",
                ),
            ],
            source="system:analyst_search_fixture",
        )
    gen2 = _publish_plain(clean_runtime, deployment)["generation_id"]
    assert gen2 != gen1
    embedder = _OpenRouterShaped(by_text)

    def ranked(generation: str, text: str) -> list[str]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(
                conn, deployment_name=deployment, requested_generation=generation,
            )
            page = search(conn, session, text, method="semantic",
                          embedder=embedder, limit=10)
        return [r["node_id"] for r in page["results"]]

    assert ranked(gen1, "chunk passage 1")[0] == "chunk:0001"
    assert "chunk:0002" in ranked(gen1, "chunk passage 2")
    later_1 = ranked(gen2, "chunk passage 1")
    later_2 = ranked(gen2, "chunk passage 2")
    assert "chunk:0001" not in later_1, "an updated node's old vector must not rank"
    assert "chunk:0002" not in later_2, "a retracted node must not rank"


def test_hnsw_scope_filters_and_the_scan_bound(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Subtype and source-family scope run in the index query; a scan that
    stops at its tuple bound with admitted rows left reports `capped`."""
    import okg.substrate.semantic_ann as ann
    from okg.substrate.semantic_ann import ann_rank, ann_spec_for_model_id, ensure_ann_index

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes, vectors, by_text = _or_corpus(300)
    notes = [node(f"note:{i}", "note", f"note passage {i}") for i in range(3)]
    note_vectors = {f"note:{i}": _or_vector(9000 + i) for i in range(3)}
    publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk", "note"],
        nodes=nodes + notes, vectors={**vectors, **note_vectors},
        model_id=OR_MODEL_ID,
    )
    spec = ann_spec_for_model_id(OR_MODEL_ID)
    ensure_ann_index(clean_runtime, spec)
    q = by_text["chunk passage 5"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        notes_only = ann_rank(conn, session, spec, q, limit=10, scoped_subtypes=["note"])
        assert {r["node_id"] for r in notes_only.rows} == {"note:0", "note:1", "note:2"}
        assert notes_only.exhausted and not notes_only.capped
        family = ann_rank(
            conn, session, spec, q, limit=5,
            scoped_source_ids=["system:analyst_search_fixture"],
        )
        assert family.rows and family.rows[0]["node_id"] == "chunk:0005"
        other = ann_rank(conn, session, spec, q, limit=5, scoped_source_ids=["elsewhere"])
        assert other.rows == [] and other.exhausted and not other.capped
        monkeypatch.setattr(ann, "MAX_SCAN_TUPLES", 1)
        bounded = ann_rank(conn, session, spec, q, limit=10, scoped_subtypes=["note"])
    assert bounded.capped and not bounded.exhausted
    assert len(bounded.rows) < 3


def test_migrate_defers_the_index_until_the_model_has_rows(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.semantic_ann import ann_spec_for_model_id, ensure_ann_index

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes, _vectors, _ = _or_corpus(3)
    publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"], nodes=nodes,
        vectors={}, model_id=None,
    )
    spec = ann_spec_for_model_id(OR_MODEL_ID)
    deferred = ensure_ann_index(clean_runtime, spec, defer_when_empty=True)
    assert deferred["action"] == "deferred" and deferred["status"] == "missing"
    # The operator's explicit build does not defer.
    assert ensure_ann_index(clean_runtime, spec)["action"] == "build"


def test_an_old_pgvector_is_refused(clean_runtime: str, tmp_path, monkeypatch) -> None:
    import okg.substrate.semantic_ann as ann
    from okg.substrate.semantic_ann import SemanticIndexUnavailable

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes, vectors, by_text = _or_corpus(3)
    publish_with_vectors(
        clean_runtime, deployment, subtypes=["chunk"], nodes=nodes,
        vectors=vectors, model_id=OR_MODEL_ID,
    )
    monkeypatch.setattr(ann, "_pgvector_version", lambda _conn: "0.7.4")
    spec = ann.ann_spec_for_model_id(OR_MODEL_ID)
    with pytest.raises(SemanticIndexUnavailable, match="0.8"):
        ann.ensure_ann_index(clean_runtime, spec)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(SemanticIndexUnavailable) as exc:
            search(conn, session, "chunk passage 1", method="semantic",
                   embedder=_OpenRouterShaped(by_text))
    assert exc.value.data["status"] == "unsupported"
