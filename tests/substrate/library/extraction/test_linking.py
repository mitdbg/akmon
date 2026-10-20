"""linking module tests — sql_fuzzy_resolve + local_fuzzy_resolve."""
from __future__ import annotations

import pytest
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


# ---- local_fuzzy_resolve (no DB) ---------------------------------


def test_local_fuzzy_resolve_finds_close_match() -> None:
    from okg.substrate.library.extraction.linking import local_fuzzy_resolve
    matches = local_fuzzy_resolve(
        "alise",
        candidates=["alice", "bob", "carol"],
        threshold=0.5,
    )
    assert len(matches) >= 1
    top = matches[0]
    assert top.canonical == "alice"
    assert top.method == "rapidfuzz"


def test_local_fuzzy_resolve_uses_canonical_for_map() -> None:
    from okg.substrate.library.extraction.linking import local_fuzzy_resolve
    matches = local_fuzzy_resolve(
        "alise",
        candidates=["alice", "bob"],
        canonical_for={"alice": "person:alice", "bob": "person:bob"},
        threshold=0.5,
    )
    assert matches
    assert matches[0].canonical == "person:alice"


def test_local_fuzzy_resolve_threshold_filters() -> None:
    from okg.substrate.library.extraction.linking import local_fuzzy_resolve
    matches = local_fuzzy_resolve(
        "xyz",
        candidates=["alice", "bob"],
        threshold=0.99,
    )
    assert matches == []


def test_local_fuzzy_resolve_empty_candidates() -> None:
    from okg.substrate.library.extraction.linking import local_fuzzy_resolve
    assert local_fuzzy_resolve("foo", candidates=[]) == []


def test_local_fuzzy_resolve_requires_needle() -> None:
    from okg.substrate.library.extraction.linking import local_fuzzy_resolve
    with pytest.raises(ValueError):
        local_fuzzy_resolve("", candidates=["a"])


# ---- sql_fuzzy_resolve (DB-backed; uses 5b5da10's SQL function) ----


@pytest.mark.postgres
def test_sql_fuzzy_resolve_calls_sql_cascade(clean_runtime: str) -> None:
    """End-to-end: open a session, seed an alias, call the
    Python wrapper, get back the SQL cascade's result."""
    import psycopg
    from okg.substrate.library.extraction.linking import sql_fuzzy_resolve
    from okg.substrate.mcp.session import open_session

    # Seed a published generation so open_session works.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            (cv,) = cur.fetchone()
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, canonical, scope, catalog_version_id) "
                "VALUES (%s, %s, NULL, %s)",
                ("alice", "person:alice", cv),
            )
        from okg.substrate.publisher import Publisher
        # Need at least one published generation; insert a node fact
        # so the publish has something to project.
        import uuid
        run_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, op, run_id, dedupe_key, observed_at)
                VALUES
                  ('person:alice', 'person', '{"name":"alice"}'::jsonb,
                   'test', '{"id":1}'::jsonb, '{"rev":"a"}'::jsonb,
                   'I', %s, 'person:alice:I', now())
                """,
                (run_id,),
            )
    from okg.substrate.publisher import Publisher
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        # Exact match → method='exact'.
        out = sql_fuzzy_resolve(conn, session, "alice")
        assert len(out) == 1
        assert out[0].canonical == "person:alice"
        assert out[0].method == "exact"

        # Typo → method='trigram' or 'levenshtein' depending on
        # similarity tier.
        out = sql_fuzzy_resolve(conn, session, "alise")
        assert len(out) == 1
        assert out[0].canonical == "person:alice"
        assert out[0].method in {"trigram", "levenshtein"}


def test_sql_fuzzy_resolve_requires_needle() -> None:
    """Pure-arg validation; doesn't need a DB."""
    from okg.substrate.library.extraction.linking import sql_fuzzy_resolve
    with pytest.raises(ValueError):
        sql_fuzzy_resolve(None, None, "")  # type: ignore[arg-type]
