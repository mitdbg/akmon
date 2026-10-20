"""Embedding coverage is measured, disclosed, and refused when zero (#797).

`semantic_search_program` ranks under `WHERE embedding IS NOT NULL`, so before
this contract a graph with no vectors returned an empty page indistinguishable
from "your query matched nothing", and a graph with 3% coverage returned a page
indistinguishable from a complete one.

Two implementation traps are FALSIFIED here rather than left to review:

  * Counting over `okg.nodes_live` (an empty base relation whose live rows are
    in the chronos branch table) would return 0 everywhere and turn the
    zero-coverage refusal into a universal denial.
    `test_partial_coverage_is_disclosed_with_nonzero_counts` asserts a NON-ZERO
    measured count on a real published generation, which that implementation
    cannot produce.
  * Resolving the segment through `resolve_node_text_bm25_index` would couple a
    SEMANTIC refusal to a LEXICAL index the semantic path never used.
    `test_a_missing_lexical_index_does_not_deny_a_working_search` asserts a
    ranking still succeeds with that index dropped.

Reaches the substrate through psycopg + `open_session`, NOT `build_server`:
this suite tests the lower-level published-row search path directly.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.embeddings import HashEmbedder, embedder_model_id
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.search_profiles import ResolvedScope
from tests.substrate.mcp.semantic_fixture import (
    make_search_deployment,
    node,
    publish_with_vectors,
)


pytestmark = pytest.mark.postgres

QUERY = "background estimation"


def test_partial_coverage_is_disclosed_with_nonzero_counts(
    clean_runtime: str, tmp_path,
) -> None:
    """A NON-empty page still reports how much of the corpus was ranked.

    The non-zero embedded count is also the falsifier for a base-relation
    implementation: `SELECT count(*) FROM okg.nodes_live WHERE embedding IS NOT
    NULL` returns 0 on every deployment.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    embedder = HashEmbedder()
    # Two of five carry vectors: the page is genuinely partial.
    nodes = [
        node("chunk:a", "chunk", QUERY),
        node("chunk:b", "chunk", QUERY + " method"),
        node("chunk:c", "chunk", "unembedded one"),
        node("chunk:d", "chunk", "unembedded two"),
        node("chunk:e", "chunk", "unembedded three"),
    ]
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk"], nodes=nodes,
        vectors={
            "chunk:a": embedder.embed(QUERY),
            "chunk:b": embedder.embed(QUERY + " method"),
        },
        model_id=embedder_model_id(embedder),
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        result = search(
            conn, session, QUERY, method="semantic", embedder=embedder,
        )

    assert result["results"], "this must be a NON-empty page"
    coverage = result["embedding_coverage"]
    # Measured, not inferred, and non-zero -- so the count came from the
    # pinned chronos segment and not from the empty base relation.
    assert coverage["embedded_nodes"] == 2
    assert coverage["unembedded_nodes"] == 3
    assert coverage["models"] == {embedder_model_id(embedder): 2}
    # Pagination semantics are untouched by the disclosure.
    assert result["total_count"] == len(result["results"])
    assert result["truncated"] is False


def test_zero_coverage_refuses_instead_of_empty_page(
    clean_runtime: str, tmp_path,
) -> None:
    """A graph with text and no vectors refuses. It does not return [].

    This is the default state of every in-repo deployment, so an empty page
    here would teach an analyst that the graph lacks content it holds.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk"],
        nodes=[
            node("chunk:a", "chunk", QUERY),
            node("chunk:b", "chunk", QUERY + " method"),
        ],
        vectors={},  # nothing embedded
        model_id=None,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(SubstrateError) as exc:
            search(
                conn, session, QUERY, method="semantic",
                embedder=HashEmbedder(),
            )
    assert exc.value.code == "semantic_index_empty"
    # The corpus DOES hold text -- reported, so the operator sees the graph is
    # populated and only the vectors are missing.
    assert exc.value.data["unembedded_nodes"] == 2
    assert exc.value.data["embedded_nodes"] == 0
    assert "okg_embed_batch" in (exc.value.fix_hint or "")


def test_spec_path_refuses_before_constructing_the_embedder(
    clean_runtime: str, tmp_path,
) -> None:
    """An unembedded deployment is not made to load a model first.

    Proven by OBSERVATION (a construction counter), not by the absence of an
    error: "no embedder was constructed" is invisible unless counted, and
    without the counter this test would just duplicate the refusal above.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk"],
        nodes=[node("chunk:a", "chunk", QUERY)],
        vectors={},
        model_id=None,
    )

    from okg.substrate.embeddings import identity

    built: list[str] = []
    real_build = identity.build_embedder

    def _counting_build(spec: str):
        built.append(spec)
        return real_build(spec)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(identity, "build_embedder", _counting_build)
            with pytest.raises(SubstrateError) as exc:
                search(
                    conn, session, QUERY, method="semantic",
                    embedder_spec="hash",
                )
    assert exc.value.code == "semantic_index_empty"
    assert built == [], (
        "the coverage refusal must precede embedder construction on the spec "
        f"path; got {built}"
    )


def test_coverage_is_measured_over_the_scoped_subtypes(
    clean_runtime: str, tmp_path,
) -> None:
    """Under a profile, coverage describes the subtypes actually ranked.

    Whole-graph coverage would report a healthy corpus while the ranked
    subtypes hold no vectors -- the same eviction-shaped confusion
    `analyst-search-profiles` fixed at the ranking layer, one layer down.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    embedder = HashEmbedder()
    # Vectors exist, but ONLY on pact_task -- outside the analyst scope.
    nodes = [
        node("task:a", "pact_task", QUERY),
        node("chunk:a", "chunk", QUERY),
        node("chunk:b", "chunk", QUERY + " method"),
    ]
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"], nodes=nodes,
        vectors={"task:a": embedder.embed(QUERY)},
        model_id=embedder_model_id(embedder),
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        # UNSCOPED control: the graph does hold a vector, so this ranks.
        unscoped = search(
            conn, session, QUERY, method="semantic", embedder=embedder,
        )
        assert unscoped["embedding_coverage"]["embedded_nodes"] == 1

        # SCOPED to chunk: that scope holds NO vectors, so it refuses rather
        # than handing back an empty page that looks like "no matches".
        scope = ResolvedScope(profile="analyst", subtypes=("chunk",))
        with pytest.raises(SubstrateError) as exc:
            search(
                conn, session, QUERY, method="semantic", embedder=embedder,
                scope=scope,
            )
    assert exc.value.code == "semantic_index_empty"
    # Scoped counts, not whole-graph counts: 0 embedded, 2 unembedded chunks.
    assert exc.value.data["embedded_nodes"] == 0
    assert exc.value.data["unembedded_nodes"] == 2
    assert exc.value.data["scoped_subtypes"] == ["chunk"]


def test_scoped_coverage_counts_only_the_scope_on_a_ranking_page(
    clean_runtime: str, tmp_path,
) -> None:
    """The positive direction: a scoped page's counts exclude other subtypes."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    embedder = HashEmbedder()
    nodes = [
        node("task:a", "pact_task", QUERY),
        node("chunk:a", "chunk", QUERY),
    ]
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"], nodes=nodes,
        vectors={
            "task:a": embedder.embed(QUERY),
            "chunk:a": embedder.embed(QUERY),
        },
        model_id=embedder_model_id(embedder),
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        unscoped = search(
            conn, session, QUERY, method="semantic", embedder=embedder,
        )
        scoped = search(
            conn, session, QUERY, method="semantic", embedder=embedder,
            scope=ResolvedScope(profile="analyst", subtypes=("chunk",)),
        )
    assert unscoped["embedding_coverage"]["embedded_nodes"] == 2
    assert scoped["embedding_coverage"]["embedded_nodes"] == 1
    assert [r["node_id"] for r in scoped["results"]] == ["chunk:a"]


def test_coverage_narrows_to_an_explicit_subtype_filter(
    clean_runtime: str, tmp_path,
) -> None:
    """An explicit subtype narrows the measurement, not just the profile does.

    `subtype_filter` narrows the rows actually returned. Measuring only the
    wider profile would report `embedded_nodes > 0` on a page whose own subtype
    holds no vectors -- the same partial-coverage confusion this disclosure
    exists to remove, one level narrower than where the profile fixes it.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    embedder = HashEmbedder()
    # Vectors on `entity` only; `chunk` has text and no vectors.
    nodes = [
        node("entity:a", "entity", QUERY),
        node("chunk:a", "chunk", QUERY),
        node("chunk:b", "chunk", QUERY + " method"),
    ]
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk", "entity"], nodes=nodes,
        vectors={"entity:a": embedder.embed(QUERY)},
        model_id=embedder_model_id(embedder),
    )

    scope = ResolvedScope(profile="analyst", subtypes=("chunk", "entity"))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        # CONTROL: across the whole profile the corpus IS partly embedded.
        wide = search(
            conn, session, QUERY, method="semantic", embedder=embedder,
            scope=scope,
        )
        assert wide["embedding_coverage"]["embedded_nodes"] == 1

        # Narrowed to `chunk`, which holds NO vectors: this must refuse rather
        # than return an empty page while claiming the corpus is embedded.
        with pytest.raises(SubstrateError) as exc:
            search(
                conn, session, QUERY, method="semantic", embedder=embedder,
                scope=scope, filter={"subtype": "chunk"},
            )
    assert exc.value.code == "semantic_index_empty"
    assert exc.value.data["embedded_nodes"] == 0
    assert exc.value.data["unembedded_nodes"] == 2
    assert exc.value.data["scoped_subtypes"] == ["chunk"]


def test_a_missing_lexical_index_does_not_deny_a_working_search(
    clean_runtime: str, tmp_path,
) -> None:
    """No healthy live path may newly refuse.

    The obvious implementation resolves the segment via
    `resolve_node_text_bm25_index`, as `scope_node_counts` does. That would
    couple a SEMANTIC refusal to the LEXICAL BM25 index, so a deployment
    without pg_textsearch -- whose semantic search works today -- would start
    getting `semantic_coverage_unmeasurable`. Dropping the index must leave
    semantic search working.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    embedder = HashEmbedder()
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk"],
        nodes=[node("chunk:a", "chunk", QUERY)],
        vectors={"chunk:a": embedder.embed(QUERY)},
        model_id=embedder_model_id(embedder),
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Precondition: the lexical index exists, and lexical search uses it.
        from okg.substrate.bm25_node_search import (
            SearchIndexUnavailable,
            resolve_node_text_bm25_index,
        )
        _, index_name = resolve_node_text_bm25_index(conn)
        with conn.cursor() as cur:
            # The index lives in whatever schema holds the chronos segment
            # (`public`, not `okg`), so resolve the schema rather than assuming
            # it -- a hardcoded schema makes DROP ... IF EXISTS a silent no-op
            # and the test would pass without ever removing the index.
            cur.execute(
                "SELECT schemaname FROM pg_indexes WHERE indexname = %s",
                (index_name,),
            )
            schema_row = cur.fetchone()
            assert schema_row is not None, "index must exist to be dropped"
            cur.execute(
                f'DROP INDEX {schema_row[0]}."{index_name}"'
            )
        # Now it genuinely cannot be resolved...
        with pytest.raises(SearchIndexUnavailable):
            resolve_node_text_bm25_index(conn)

        # ...and semantic search still ranks, with a measured coverage.
        session = open_session(conn, deployment_name=deployment)
        result = search(
            conn, session, QUERY, method="semantic", embedder=embedder,
        )
    assert [r["node_id"] for r in result["results"]] == ["chunk:a"]
    assert result["embedding_coverage"]["embedded_nodes"] == 1


def test_coverage_keys_are_present_on_every_semantic_page(
    clean_runtime: str, tmp_path,
) -> None:
    """Key PRESENCE, not `get(...) is None`.

    An absent key reads as "coverage is fine", which is the inference this
    disclosure exists to prevent -- and a `get(...) is None` assertion would
    hold against code that emits the key nowhere at all.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    embedder = HashEmbedder()
    publish_with_vectors(
        clean_runtime, deployment,
        subtypes=["chunk"],
        nodes=[node("chunk:a", "chunk", QUERY)],
        vectors={"chunk:a": embedder.embed(QUERY)},
        model_id=embedder_model_id(embedder),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        semantic = search(
            conn, session, QUERY, method="semantic", embedder=embedder,
        )
        # A query that matches no text still gets the disclosure.
        empty_ish = search(
            conn, session, "zzzz-nothing-matches-this", method="semantic",
            embedder=embedder,
        )
        lexical = search(conn, session, QUERY, method="lexical")

    for payload in (semantic, empty_ish):
        assert "embedding_coverage" in payload
        assert "embedder_model_id" in payload
        assert "embedder_spec" in payload
        assert "embedder_source" in payload
    # Semantic-only: a lexical envelope carrying these would make their
    # absence meaningless.
    for key in ("embedding_coverage", "embedder_model_id", "embedder_spec"):
        assert key not in lexical, f"{key} must not appear on a lexical page"
