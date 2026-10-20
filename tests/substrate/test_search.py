"""M4.2 — search(method ∈ {lexical, semantic, alias})."""
from __future__ import annotations

import uuid

import psycopg
import pytest

from okg.substrate.embeddings import HashEmbedder, embedder_model_id
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _u() -> str:
    return uuid.uuid4().hex[:12]


def _seed_catalog_version(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        return cur.fetchone()[0]


def _insert_alias(
    conn: psycopg.Connection, alias: str, canonical: str,
    scope: str | None, cv: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.aliases "
            "  (alias, canonical, scope, catalog_version_id) "
            "VALUES (%s, %s, %s, %s)",
            (alias, canonical, scope, cv),
        )


def _insert_node_facts(
    conn: psycopg.Connection,
    nodes: list[tuple[str, str, str]],  # (node_id, subtype, text)
) -> None:
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts([
        NodeFact(
            node_id=nid,
            subtype=subtype,
            attrs={"text": text},
            source_record_id={"id": nid},
            source_revision={"v": 1},
        )
        for nid, subtype, text in nodes
    ])


def _write_embedding(
    conn: psycopg.Connection, node_id: str, vec: list[float], gen: str,
) -> None:
    """Directly overwrite Chronos graph-node embedding for a fixture.

    Tests call this helper before another publish cycle so the updated
    mutable branch is captured into the next generation checkpoint.

    The recorded `embedding_model` comes from the REAL identity derivation, not
    a literal. These fixtures previously wrote `'test-hash'` while querying
    with `HashEmbedder`, which is model SKEW under
    `reachable-semantic-search` — semantic search now refuses it. Deriving
    the id here means the fixture states the truth about which model wrote the
    vector, and it cannot drift from what the embed workflow would record.
    """
    vecstr = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"
    store = ChronosGraphStore.from_connection(conn)
    graph = store.checkout()
    graph.execute(
        """
        UPDATE okg.graph_nodes
           SET embedding = :embedding::vector,
               embedding_model = :model_id
         WHERE node_id = :node_id
        """,
        {
            "embedding": vecstr,
            "node_id": node_id,
            "model_id": embedder_model_id(HashEmbedder()),
        },
    )


def _force_publish_delta(conn: psycopg.Connection) -> None:
    _insert_node_facts(
        conn,
        [(f"n:publish-delta:{_u()}", "page", "publish delta")],
    )


# ---------- dispatch / validation ----------

def test_search_rejects_invalid_method() -> None:
    with pytest.raises(ValueError, match="method must be"):
        search(None, None, "x", method="bogus")  # type: ignore[arg-type]


def test_search_rejects_empty_query() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        search(None, None, "", method="lexical")  # type: ignore[arg-type]


def test_search_rejects_unknown_filter_key() -> None:
    with pytest.raises(ValueError, match="unknown filter"):
        search(
            None, None, "x", method="lexical",  # type: ignore[arg-type]
            filter={"weird": "y"},
        )


def test_search_semantic_requires_embedder() -> None:
    with pytest.raises(ValueError, match="embedder"):
        search(None, None, "x", method="semantic")  # type: ignore[arg-type]


# ---------- lexical ----------

def test_search_lexical_ranks_by_chronos_text_score(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "alpha"),               # perfect match
            ("n:b", "page", "alpha beta gamma"),    # partial-match doc
            ("n:c", "page", "gamma delta"),         # no match
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = search(conn, s, "alpha", method="lexical")

    ids = [r["node_id"] for r in out["results"]]
    # n:a should rank highest (shortest doc containing the term);
    # n:b also matches; n:c is non-match and excluded.
    assert ids[0] == "n:a"
    assert "n:b" in ids
    assert "n:c" not in ids
    # Scores are non-positive for compatibility with the old
    # lexical envelope.
    assert all(r["score"] <= 0 for r in out["results"])


def test_search_lexical_reads_chronos_checkpoint_not_nodes_live(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "alpha")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        # Lexical search reads the Chronos checkpoint (okg.graph_nodes). The
        # legacy okg.nodes_live table -- which this test once UPDATEd as a decoy
        # to prove search ignored it -- is now DROPPED by the cutover migration,
        # so the decoy is moot: 'alpha' (the seeded text) hits; 'omega' (never
        # in the graph) does not.
        alpha = search(conn, s, "alpha", method="lexical")
        omega = search(conn, s, "omega", method="lexical")

    assert [r["node_id"] for r in alpha["results"]] == ["n:a"]
    assert omega["results"] == []


def test_search_lexical_honors_subtype_filter(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "alpha mention"),
            ("n:b", "concept", "alpha mention here"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = search(
            conn, s, "alpha",
            method="lexical", filter={"subtype": "concept"},
        )
    ids = [r["node_id"] for r in out["results"]]
    assert ids == ["n:b"]


def test_search_lexical_honors_pin(clean_runtime: str) -> None:
    """A session pinned to gen N does not see text added in N+1.

    Note: BM25 scores are corpus-dependent — as the index grows
    across generations, absolute scores for a fixed document
    shift. The PIN invariant the substrate provides is on the
    result SET (which node_ids are visible + their relative
    ranking), not on the numeric score. See search.py docstring.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:a", "page", "alpha in g1")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session_n = open_session(conn, deployment_name="default")
        out_before = search(conn, session_n, "alpha", method="lexical")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _insert_node_facts(conn, [("n:b", "page", "alpha in g2")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        out_after = search(conn, session_n, "alpha", method="lexical")

    # Pinned result set is stable: same ids, same order.
    ids_before = [r["node_id"] for r in out_before["results"]]
    ids_after = [r["node_id"] for r in out_after["results"]]
    assert ids_before == ids_after == ["n:a"]
    # Scores may drift (corpus-dependent); both must still be
    # negative (i.e. real matches).
    assert all(r["score"] < 0 for r in out_before["results"])
    assert all(r["score"] < 0 for r in out_after["results"])


# ---------- semantic (pgvector) ----------

def test_search_semantic_ranks_by_cosine(clean_runtime: str) -> None:
    """Three nodes: two with the query's exact embedding, one with
    a different one. Query ranks exact-match docs first."""
    embedder = HashEmbedder()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "alpha content"),
            ("n:b", "page", "alpha content dup"),
            ("n:c", "page", "totally different"),
        ])
    r = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    g = r["generation_id"]

    # Write embeddings: n:a uses the QUERY text's embedding exactly.
    query = "query keyword alpha"
    qvec = embedder.embed(query)
    close_vec = embedder.embed(query + " slight drift")  # still close-ish
    far_vec = embedder.embed("totally orthogonal wording")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _write_embedding(conn, "n:a", qvec, g)
        _write_embedding(conn, "n:b", close_vec, g)
        _write_embedding(conn, "n:c", far_vec, g)
        _force_publish_delta(conn)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = search(
            conn, s, query, method="semantic", embedder=embedder,
        )
    ids = [r["node_id"] for r in out["results"]]
    # n:a has cosine distance 0 (identical vec). Always first.
    assert ids[0] == "n:a"
    assert out["results"][0]["distance"] == pytest.approx(0.0, abs=1e-6)
    # All three appear (distance < 2 for any two vectors; non-null
    # embeddings all score).
    assert set(ids) == {"n:a", "n:b", "n:c"}
    assert all("evidence_kind" in row for row in out["results"])
    assert all("evidence_kind_provenance" in row for row in out["results"])


def test_search_semantic_skips_null_embeddings(clean_runtime: str) -> None:
    embedder = HashEmbedder()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [
            ("n:a", "page", "a"),
            ("n:b", "page", "b"),
        ])
    r = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    # Only n:a gets an embedding.
    qvec = embedder.embed("query")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _write_embedding(conn, "n:a", qvec, r["generation_id"])
        _force_publish_delta(conn)
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = search(conn, s, "query", method="semantic", embedder=embedder)
    ids = [r["node_id"] for r in out["results"]]
    assert ids == ["n:a"]  # n:b has NULL embedding → excluded


# ---------- alias ----------

def test_search_alias_exact_match(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_catalog_version(conn)
        _insert_alias(conn, "dm", "n:diabetes", None, cv)
        _insert_node_facts(conn, [
            ("n:diabetes", "concept", "Diabetes mellitus"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = search(conn, s, "dm", method="alias")
    assert len(out["results"]) == 1
    r0 = out["results"][0]
    assert r0["alias"] == "dm"
    assert r0["canonical"] == "n:diabetes"
    assert r0["match_kind"] == "exact"
    assert r0["evidence_kind"] == "curated_alias"
    assert r0["node"]["subtype"] == "concept"
    assert r0["node"]["evidence_kind"] == "unknown"


def test_search_alias_subtype_filter_finds_scoped(
    clean_runtime: str,
) -> None:
    """Filter subtype='concept' finds a concept-scoped alias.

    NOTE on schema: `aliases.PRIMARY KEY (alias, catalog_version_id)`
    disallows two rows with the same alias text under one
    catalog version — so the "scoped vs global on the same alias
    name" preference scenario isn't expressible against this
    schema. The tested invariant here is the simpler one: a
    subtype filter includes both scope=NULL and scope=<that
    subtype>, preferring scoped on the ORDER BY.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_catalog_version(conn)
        _insert_alias(conn, "ref_global", "n:global", None, cv)
        _insert_alias(conn, "ref_scoped", "n:concept-ref", "concept", cv)
        _insert_node_facts(conn, [
            ("n:global", "concept", "global"),
            ("n:concept-ref", "concept", "scoped"),
        ])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out_scoped = search(
            conn, s, "ref_scoped", method="alias",
            filter={"subtype": "concept"},
        )
        out_global = search(
            conn, s, "ref_global", method="alias",
            filter={"subtype": "concept"},
        )
    assert [r["canonical"] for r in out_scoped["results"]] == [
        "n:concept-ref"
    ]
    assert [r["canonical"] for r in out_global["results"]] == [
        "n:global"
    ]


def test_search_alias_misses_return_empty(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        _insert_node_facts(conn, [("n:x", "page", "x")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        out = search(conn, s, "no-such-alias", method="alias")
    assert out["results"] == []


def test_search_alias_honors_catalog_version_pin(
    clean_runtime: str,
) -> None:
    """Aliases are scoped to catalog_version_id. A session pinned
    to an older generation sees aliases from THAT generation's
    catalog_version, not the latest."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv1 = _seed_catalog_version(conn)
        _insert_alias(conn, "k", "n:old", None, cv1)
        _insert_node_facts(conn, [("n:old", "page", "old")])
    r1 = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    g1 = r1["generation_id"]

    # Bump catalog + add a different alias mapping under the new
    # catalog_version. (A real reconcile flow would retract the old
    # binding; for this test we just create a new alias under a new
    # cv and confirm the session pin anchors both.)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv2 = _seed_catalog_version(conn)
        _insert_alias(conn, "k", "n:new", None, cv2)
        _insert_node_facts(conn, [("n:new", "page", "new")])
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    # A session pinned to g1 uses cv1's alias (→ n:old), even
    # though cv2's k→n:new exists in the table.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s_old = open_session(
            conn, deployment_name="default", requested_generation=g1,
        )
        out_old = search(conn, s_old, "k", method="alias")
    canonicals_old = [r["canonical"] for r in out_old["results"]]
    assert canonicals_old == ["n:old"]

    # Latest-session sees cv2's alias.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s_new = open_session(conn, deployment_name="default")
        out_new = search(conn, s_new, "k", method="alias")
    canonicals_new = [r["canonical"] for r in out_new["results"]]
    assert canonicals_new == ["n:new"]
