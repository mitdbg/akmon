"""Regression test for `add-text-attr-declaration`.

The substrate's projector reads each subtype's
`metadata.searchable_text_fields` from `okg.subtypes` and
auto-stamps `attrs.text` from those source fields at
project_batch time. Adapters no longer need to inline
`attrs["text"] = ...`; the substrate guarantees coverage from
the schema declaration.

Tests:

  - Stamp `searchable_text_fields = ['a','b']` on a synthetic
    subtype. Append a node_fact with `attrs={'a':'hello','b':'world'}`.
    After Publisher.cycle, `nodes_live.attrs->>'text' ==
    'hello\\n\\nworld'`.
  - Empty / null source fields are skipped (no leading
    blanks).
  - A producer's own `attrs.text` is appended after the declared
    fields and kept under `okg_producer_text`, not overwritten
    (issue #2991; before it the body was silently dropped).
  - Subtypes without a declaration get `attrs.text` unchanged
    (legacy text from the adapter passes through).
"""
from __future__ import annotations

import json
import uuid

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _seed_cv_with_subtype_metadata(
    conn: psycopg.Connection,
    *,
    subtype: str,
    text_fields: list[str] | None,
) -> int:
    """Seed a fresh applied catalog_version with one subtype
    row whose metadata carries (or omits) the searchable text
    fields. Returns the cv id."""
    metadata = (
        {"searchable_text_fields": text_fields}
        if text_fields else {}
    )
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.subtypes "
            "  (name, metadata, catalog_version_id) "
            "VALUES (%s, %s::jsonb, %s)",
            (subtype, json.dumps(metadata), cv),
        )
    return cv


def _seed_node_fact(
    conn: psycopg.Connection,
    *,
    node_id: str,
    subtype: str,
    attrs: dict,
) -> None:
    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES (%s, %s, %s::jsonb, 'fixture',
                    %s::jsonb, %s::jsonb,
                    now(), %s, %s, 'I')
            """,
            (
                node_id, subtype, json.dumps(attrs),
                json.dumps({"id": node_id}),
                json.dumps({"v": 1, "rev": run_id}),
                f"{run_id}:{node_id}",
                run_id,
            ),
        )

    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype=subtype,
                attrs=dict(attrs),
                source_record_id={"id": node_id},
                source_revision={"v": 1},
            )
        ],
        source="fixture",
    )


def _graph_text_at(dsn: str, generation_id: str, node_id: str) -> str | None:
    store = ChronosGraphStore.connect(dsn)
    try:
        checkout = store.checkout_generation(generation_id)
        rows = checkout.query(
            "SELECT attrs->>'text' AS text FROM okg.graph_nodes "
            "WHERE node_id = %(node_id)s",
            {"node_id": node_id},
        )
        return rows[0]["text"] if rows else None
    finally:
        store.close()


def _graph_texts_at(
    dsn: str, generation_id: str, node_ids: tuple[str, ...],
) -> dict[str, str | None]:
    store = ChronosGraphStore.connect(dsn)
    try:
        checkout = store.checkout_generation(generation_id)
        rows = checkout.query(
            "SELECT node_id, attrs->>'text' AS text FROM okg.graph_nodes "
            "WHERE node_id = ANY(%(node_ids)s)",
            {"node_ids": list(node_ids)},
        )
        return {row["node_id"]: row["text"] for row in rows}
    finally:
        store.close()


def test_projector_stamps_text_from_declared_fields(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="memo",
            text_fields=["a", "b"],
        )
        _seed_node_fact(
            conn, node_id="n:1", subtype="memo",
            attrs={"a": "hello", "b": "world"},
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    text = _graph_text_at(clean_runtime, out["generation_id"], "n:1")
    assert text == "hello\n\nworld", repr(text)


def test_empty_source_fields_skipped(clean_runtime: str) -> None:
    """Empty / null source fields are skipped (no blank
    paragraphs in the joined output)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="memo2",
            text_fields=["a", "missing", "b"],
        )
        _seed_node_fact(
            conn, node_id="n:2", subtype="memo2",
            attrs={"a": "hello", "b": "world"},
        )
        _seed_node_fact(
            conn, node_id="n:3", subtype="memo2",
            attrs={"a": "  ", "b": "world"},  # whitespace only
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    rows = _graph_texts_at(clean_runtime, out["generation_id"], ("n:2", "n:3"))
    assert rows["n:2"] == "hello\n\nworld"
    assert rows["n:3"] == "world"


def test_producer_text_appended_when_declared(
    clean_runtime: str,
) -> None:
    """If the adapter writes `attrs.text = "OLD"` AND the schema declares
    `searchable_text_fields = ['a','b']`, both are searchable: the
    declared fields first, then the producer's body (issue #2991; this
    test asserted the silent overwrite before)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="memo3",
            text_fields=["a", "b"],
        )
        _seed_node_fact(
            conn, node_id="n:4", subtype="memo3",
            attrs={"a": "fresh", "b": "value", "text": "OLD"},
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    text = _graph_text_at(clean_runtime, out["generation_id"], "n:4")
    assert text == "fresh\n\nvalue\n\nOLD"


def test_text_field_is_fallback_not_restamped_source_field(
    clean_runtime: str,
) -> None:
    """If `text` is declared alongside source fields, restamping must
    not fold the prior derived rollup back into itself: a stored stamp
    carries its body under `okg_producer_text`, and a restamp of that exact
    stamp starts from the copy (issue #2991)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="memo_text_fallback",
            text_fields=["a", "text"],
        )
        _seed_node_fact(
            conn, node_id="n:text-fallback",
            subtype="memo_text_fallback",
            attrs={
                "a": "fresh",
                "text": "fresh\n\nbody",
                "okg_producer_text": "body",
            },
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    text = _graph_text_at(
        clean_runtime, out["generation_id"], "n:text-fallback",
    )
    assert text == "fresh\n\nbody"


def test_text_field_only_declaration_keeps_text(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="memo_text_only",
            text_fields=["text"],
        )
        _seed_node_fact(
            conn, node_id="n:text-only", subtype="memo_text_only",
            attrs={"text": "as-emitted"},
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    text = _graph_text_at(clean_runtime, out["generation_id"], "n:text-only")
    assert text == "as-emitted"


def test_no_declaration_leaves_text_unchanged(
    clean_runtime: str,
) -> None:
    """A subtype with no `searchable_text_fields` keeps the
    adapter's `attrs.text` verbatim."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_subtype_metadata(
            conn, subtype="memo4", text_fields=None,
        )
        _seed_node_fact(
            conn, node_id="n:5", subtype="memo4",
            attrs={"text": "as-emitted"},
        )

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    text = _graph_text_at(clean_runtime, out["generation_id"], "n:5")
    assert text == "as-emitted"
