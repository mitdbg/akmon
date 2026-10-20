"""Tests for okg.views.backbone.sql_view_anchor +
render_markdown_table — `add-deployment-sql-views`.

Two layers:

* Unit tests for ``render_markdown_table`` (no DB).
* Postgres-backed tests for ``sql_view_anchor``: pin isolation,
  hash stability, validation guards, custom render callable.
  Uses the existing ``clean_runtime`` fixture from
  ``conftest.py`` (against ``OKG_TEST_DSN``).
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.views.backbone import (
    clear_corpus_cache,
    render_markdown_table,
    sql_view_anchor,
)


# ---------------------------------------------------------------------------
# render_markdown_table — pure-Python unit tests
# ---------------------------------------------------------------------------


class TestRenderMarkdownTable:
    def test_empty_rows_produces_header_only_table(self):
        out = render_markdown_table([], ["a", "b"])
        assert out == "| a | b |\n| --- | --- |\n"

    def test_single_row(self):
        out = render_markdown_table([(1, "x")], ["a", "b"])
        assert out == "| a | b |\n| --- | --- |\n| 1 | x |\n"

    def test_null_cells_render_as_empty_string(self):
        out = render_markdown_table(
            [(1, None, "x")], ["a", "b", "c"],
        )
        assert "| 1 |  | x |" in out

    def test_multiple_rows_preserve_order(self):
        rows = [(1, "a"), (2, "b"), (3, "c")]
        out = render_markdown_table(rows, ["n", "letter"])
        # Body rows appear in insertion order.
        body_lines = [
            line for line in out.splitlines()
            if line.startswith("| ") and "---" not in line
            and "n |" not in line  # skip header
        ]
        assert body_lines == [
            "| 1 | a |",
            "| 2 | b |",
            "| 3 | c |",
        ]

    def test_deterministic_across_calls(self):
        rows = [(1, "a"), (None, "b")]
        a = render_markdown_table(rows, ["x", "y"])
        b = render_markdown_table(rows, ["x", "y"])
        assert a == b


# ---------------------------------------------------------------------------
# sql_view_anchor — guards (no DB needed)
# ---------------------------------------------------------------------------


class TestSqlViewAnchorGuards:
    """The validation guards must fire BEFORE any cursor opens —
    these tests pass a mock connection that records cursor()
    calls to assert no DB work happens for invalid arguments.
    """

    def test_non_int_generation_id_raises_typeerror(self):
        from unittest.mock import MagicMock

        conn = MagicMock(spec=psycopg.Connection)
        with pytest.raises(TypeError, match="generation_id must be an int"):
            sql_view_anchor(
                conn,
                anchor_id="x",
                view_name="okg.v_nodes",
                generation_id="42",  # type: ignore[arg-type]
            )
        # No transaction, no cursor opened.
        assert not conn.transaction.called
        assert not conn.cursor.called

    def test_bool_generation_id_rejected(self):
        from unittest.mock import MagicMock

        conn = MagicMock(spec=psycopg.Connection)
        with pytest.raises(TypeError, match="generation_id must be an int"):
            sql_view_anchor(
                conn,
                anchor_id="x",
                view_name="okg.v_nodes",
                generation_id=True,  # type: ignore[arg-type]
            )

    def test_invalid_view_name_raises_valueerror(self):
        from unittest.mock import MagicMock

        conn = MagicMock(spec=psycopg.Connection)
        with pytest.raises(ValueError, match="does not match"):
            sql_view_anchor(
                conn,
                anchor_id="x",
                view_name="okg.v_nodes; DROP TABLE okg.nodes_live; --",
                generation_id=1,
            )
        assert not conn.transaction.called

    def test_view_name_outside_okg_schema_rejected(self):
        from unittest.mock import MagicMock

        conn = MagicMock(spec=psycopg.Connection)
        with pytest.raises(ValueError):
            sql_view_anchor(
                conn,
                anchor_id="x",
                view_name="public.users",
                generation_id=1,
            )


# ---------------------------------------------------------------------------
# sql_view_anchor — Postgres-backed integration
# ---------------------------------------------------------------------------


def _insert_generation(
    conn: psycopg.Connection,
    *,
    deployment_name: str = "test",
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations
              (deployment_name, status, catalog_version_id,
               correlation_id, published_at)
            VALUES (%s, 'published', 1, %s, now())
            RETURNING generation_id
            """,
            (deployment_name, f"test-{deployment_name}"),
        )
        return int(cur.fetchone()[0])


def _insert_pub(
    conn: psycopg.Connection,
    *,
    node_id: str,
    status: str,
    valid_from_gen: int,
    valid_to_gen: int | None = None,
) -> None:
    # Chronos cutover: the publication node lives in okg.graph_nodes. The
    # caller checkpoints the published generation after seeding so the pin
    # resolves it. (No caller currently retracts a node mid-window.)
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    store.apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype="publication",
                attrs={"pub_number": node_id, "status": status},
                source_record_id={"id": node_id},
                source_revision={"v": 1},
            )
        ],
        source="default",
        strict_endpoints=True,
    )


def _checkpoint(conn: psycopg.Connection, generation_id: int) -> None:
    """Checkpoint the current Chronos branch head as ``generation_id`` so a
    generation pin resolves the seeded graph."""
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    store.checkpoint_generation(generation_id, deployment_name="test")


@pytest.fixture
def conn(clean_runtime):
    with psycopg.connect(clean_runtime, autocommit=True) as c:
        # Ensure the Chronos schema + default branch exist before seed
        # helpers (which use ensure_schema=False) run.
        ChronosGraphStore.from_connection(c, ensure_schema=True)
        yield c


@pytest.fixture(autouse=True)
def _clear_views_cache():
    clear_corpus_cache()
    yield
    clear_corpus_cache()


@pytest.mark.postgres
class TestSqlViewAnchorAgainstSubstrate:
    """The substrate-shipped okg.v_nodes is generation-pinned and
    deployment-agnostic. We use it as the test target so we don't
    have to bring up a deployment-side views.sql for these tests.
    """

    def test_pin_isolates_two_generations(self, conn):
        # gen1 has 2 publications; gen2 has those 2 plus 1 more.
        gen1 = _insert_generation(conn)
        _insert_pub(conn, node_id="pub:a", status="current",
                    valid_from_gen=gen1)
        _insert_pub(conn, node_id="pub:b", status="rescinded",
                    valid_from_gen=gen1)
        _checkpoint(conn, gen1)
        gen2 = _insert_generation(conn)
        _insert_pub(conn, node_id="pub:c", status="current",
                    valid_from_gen=gen2)
        _checkpoint(conn, gen2)

        a1 = sql_view_anchor(
            conn,
            anchor_id="x",
            view_name="okg.v_nodes",
            generation_id=gen1,
            where="WHERE subtype = 'publication'",
        )
        a2 = sql_view_anchor(
            conn,
            anchor_id="x",
            view_name="okg.v_nodes",
            generation_id=gen2,
            where="WHERE subtype = 'publication'",
        )
        # Different content (gen2 has one more pub) → different
        # hash.
        assert a1.content_hash != a2.content_hash
        # gen2 content includes pub:c; gen1 doesn't.
        assert "pub:c" in a2.content
        assert "pub:c" not in a1.content

    def test_same_args_same_generation_yields_equal_hashes(self, conn):
        gen = _insert_generation(conn)
        _insert_pub(conn, node_id="pub:a", status="current",
                    valid_from_gen=gen)
        _checkpoint(conn, gen)
        a1 = sql_view_anchor(
            conn,
            anchor_id="x",
            view_name="okg.v_nodes",
            generation_id=gen,
            where="WHERE subtype = 'publication'",
        )
        a2 = sql_view_anchor(
            conn,
            anchor_id="x",
            view_name="okg.v_nodes",
            generation_id=gen,
            where="WHERE subtype = 'publication'",
        )
        assert a1.content_hash == a2.content_hash
        # Identical content (verbatim equal).
        assert a1.content == a2.content

    def test_where_clause_with_params_filters(self, conn):
        gen = _insert_generation(conn)
        _insert_pub(conn, node_id="pub:a", status="current",
                    valid_from_gen=gen)
        _insert_pub(conn, node_id="pub:b", status="rescinded",
                    valid_from_gen=gen)
        _checkpoint(conn, gen)
        # Filter to only current pubs via parameterized WHERE.
        a = sql_view_anchor(
            conn,
            anchor_id="x",
            view_name="okg.v_nodes",
            generation_id=gen,
            where=(
                "WHERE subtype = 'publication' "
                "AND attrs->>'status' = %s"
            ),
            params=("current",),
        )
        assert "pub:a" in a.content
        assert "pub:b" not in a.content

    def test_custom_render_callable_invoked_with_columns(self, conn):
        gen = _insert_generation(conn)
        _insert_pub(conn, node_id="pub:a", status="current",
                    valid_from_gen=gen)
        _checkpoint(conn, gen)

        captured = {}

        def custom_render(rows, columns):
            captured["rows"] = list(rows)
            captured["columns"] = list(columns)
            return "RENDERED"

        a = sql_view_anchor(
            conn,
            anchor_id="x",
            view_name="okg.v_nodes",
            generation_id=gen,
            where="WHERE subtype = 'publication'",
            render=custom_render,
        )
        assert a.content == "RENDERED"
        assert "node_id" in captured["columns"]
        # At least one row was returned.
        assert len(captured["rows"]) >= 1

    def test_anchor_id_propagates(self, conn):
        gen = _insert_generation(conn)
        _checkpoint(conn, gen)
        a = sql_view_anchor(
            conn,
            anchor_id="my:custom:id",
            view_name="okg.v_nodes",
            generation_id=gen,
        )
        assert a.id == "my:custom:id"
