from __future__ import annotations

from pathlib import Path

_VIEW_DECL = "CREATE OR REPLACE VIEW okg."


def _next_view_after(sql: str, start: int) -> int:
    """Offset of the next view declaration after the one starting at ``start``.

    Slices below are bounded positionally rather than by naming the view that
    happens to come next. Naming it makes an unrelated retirement raise
    ValueError out of ``str.index``, which reads as a test ERROR rather than
    as the contract violation these tests exist to report.
    """
    return sql.index(_VIEW_DECL, sql.index("\n", start))


def test_doc_chunk_view_prefers_membership_occurrence_context() -> None:
    root = Path(__file__).resolve().parents[2]
    sql = (root / "src/okg/substrate/db/views.sql").read_text()
    start = sql.index("CREATE OR REPLACE VIEW okg.v_doc_chunks")
    end = sql.index("CREATE OR REPLACE VIEW okg.v_chunk_sequence", start)
    view = sql[start:end]
    assert "c.attrs->>'chunk_kind' AS chunk_kind" in view
    for field in ("char_offset", "char_end", "char_length"):
        assert f"NULLIF(e.attrs->>'{field}', '')::int" in view
        assert f"NULLIF(c.attrs->>'{field}', '')::int" in view
    assert "COALESCE(e.attrs->>'heading_path'" in view
    assert "COALESCE(e.attrs->>'chunker_name'" in view


def test_doc_chunk_views_append_new_columns_after_existing_contract() -> None:
    root = Path(__file__).resolve().parents[2]
    sql = (root / "src/okg/substrate/db/views.sql").read_text()
    chunks_start = sql.index("CREATE OR REPLACE VIEW okg.v_doc_chunks")
    sequence_start = sql.index("CREATE OR REPLACE VIEW okg.v_chunk_sequence", chunks_start)
    chunks_view = sql[chunks_start:sequence_start]
    # Bounded by "the next view declaration", not by a NAMED one. This slice
    # used to end at `okg.v_changes`; that view was retired
    # (schema-canonical-read-path, decision D6) and the anchor raised
    # ValueError, so the test ERRORED rather than reporting a contract
    # violation. A positional bound cannot be invalidated by retiring an
    # unrelated neighbour.
    sequence_view = sql[sequence_start:_next_view_after(sql, sequence_start)]

    # CREATE OR REPLACE VIEW may append columns, but PostgreSQL rejects moving
    # or renaming an existing column. Keep the established contracts intact.
    chunks_columns = [
        "AS chunk_id",
        "AS doc_id",
        "AS path",
        "AS repo",
        "AS text",
        "AS heading_path",
        "AS char_offset",
        "AS char_length",
        "AS chunker_name",
        "AS content_sha256",
        "AS chunk_kind",
        "AS char_end",
        "AS table_grid_status",
        "AS table_extraction_lane",
        "AS table_grid_error_code",
        "AS chunk_ordinal",
    ]
    assert [chunks_view.index(column) for column in chunks_columns] == sorted(
        chunks_view.index(column) for column in chunks_columns
    )

    sequence_columns = [
        "chunk_id,",
        "doc_id,",
        "path,",
        "char_offset,",
        "char_length,",
        "heading_path,",
        "AS prev_chunk_id",
        "AS next_chunk_id",
        "AS chunk_index",
        "AS doc_chunk_total",
        "chunk_kind,",
        "char_end",
    ]
    assert [sequence_view.index(column) for column in sequence_columns] == sorted(
        sequence_view.index(column) for column in sequence_columns
    )


def test_chunk_sequence_orders_by_ordinal_before_offset() -> None:
    """Chunks with no span store no offset (older graphs store 0 on every
    one), so prev/next must follow the member_of edge's emission order."""
    root = Path(__file__).resolve().parents[2]
    sql = (root / "src/okg/substrate/db/views.sql").read_text()
    chunks_start = sql.index("CREATE OR REPLACE VIEW okg.v_doc_chunks")
    sequence_start = sql.index("CREATE OR REPLACE VIEW okg.v_chunk_sequence", chunks_start)
    chunks_view = sql[chunks_start:sequence_start]
    sequence_view = sql[sequence_start:_next_view_after(sql, sequence_start)]
    assert "NULLIF(e.attrs->>'chunk_ordinal', '')::int AS chunk_ordinal" in chunks_view
    assert (
        "PARTITION BY doc_id ORDER BY chunk_ordinal NULLS LAST, char_offset, chunk_id"
        in sequence_view
    )
