"""Pieced record-set authority against a real PostgreSQL (issue #3008).

A record set above the JSONB threshold is stored as ordered pieces under a
header row. These tests use a piece size of a few hundred bytes so a small
record set spans many pieces.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest

from okg.substrate.ingest import SourceRunner
from okg.substrate.ingest.full_retraction_guard import FullRetractionOverride
from okg.substrate.ingest.record_set_pieces import (
    PIECED_RECORD_SET_MARKER,
    RECORD_SET_PIECES_DDL,
    RecordSetPiecesError,
    dumps_record_set_json,
)
from okg.substrate.library.sources.base import (
    RecordEmission,
    RecordNode,
    RecordSet,
)
from tests.substrate._db_helpers import seed_catalog_version
from tests.substrate.test_source_runner_record_set import (
    _RecordSetAdapter,
    _edge,
    _node,
    _record,
)

pytestmark = pytest.mark.postgres

PIECE = 256


def _big_record_set(n: int = 40) -> RecordSet:
    return RecordSet(records={
        f"docs/{i:03d}.md": RecordEmission(
            primary=RecordNode(
                f"doc:{i}", "concept",
                source_record_id={"path": f"docs/{i:03d}.md", "note": "café"},
            ),
            descendants=[RecordNode(f"chunk:{i}", "concept")],
            edges=[],
        )
        for i in range(n)
    })


@pytest.fixture(autouse=True)
def _no_leftover_blobs(clean_runtime: str) -> None:
    # clean_runtime does not truncate the record-set blob tables, and these
    # tests use fixed source names and digests, so a second session on the
    # same database would otherwise meet the first session's rows.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "DELETE FROM okg.source_record_set_blobs WHERE source LIKE 'test.pieces.%'"
        )
        conn.execute(
            "DELETE FROM okg.source_record_set_blob_pieces WHERE source LIKE 'test.pieces.%'"
        )


@pytest.fixture
def pieced(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES", "1")
    monkeypatch.setenv("OKG_SOURCE_RUNNER_RECORD_SET_JSONB_MAX_BYTES", "1")
    monkeypatch.setenv(
        "OKG_SOURCE_RUNNER_RECORD_SET_PIECE_MAX_BYTES", str(PIECE),
    )


def _header(dsn: str, source: str, sha256: str):
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT record_set, record_set_text, piece_write_id, piece_count,
                   piece_bytes, record_set_summary
              FROM okg.source_record_set_blobs
             WHERE source = %s AND sha256 = %s
            """,
            (source, sha256),
        )
        return cur.fetchone()


def _pieces(dsn: str, write_id) -> list[tuple[int, str]]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT piece_no, piece FROM okg.source_record_set_blob_pieces
             WHERE write_id = %s ORDER BY piece_no
            """,
            (write_id,),
        )
        return cur.fetchall()


def _store(dsn: str, source: str, record_set: RecordSet, summary=None):
    runner = SourceRunner(dsn, source_name=source)
    out = runner._record_set_authority_cursor(
        record_set, summary=summary or {"n": 1}, proof=None,
    )
    return runner, out["record_set_ref"]


def test_record_set_bigger_than_one_piece_stores_and_reads_back(
    clean_runtime: str, pieced: None,
) -> None:
    source = "test.pieces.round-trip"
    record_set = _big_record_set()
    runner, ref = _store(clean_runtime, source, record_set)
    text = dumps_record_set_json(record_set.to_cursor())
    assert ref["sha256"] == hashlib.sha256(text.encode()).hexdigest()

    record_set_json, record_set_text, write_id, count, nbytes, _ = _header(
        clean_runtime, source, ref["sha256"],
    )
    assert record_set_json == PIECED_RECORD_SET_MARKER
    assert record_set_text is None
    assert nbytes == len(text.encode())
    rows = _pieces(clean_runtime, write_id)
    assert count == len(rows) > 5
    assert [no for no, _ in rows] == list(range(count))
    assert all(len(piece.encode()) <= PIECE for _, piece in rows)
    assert "".join(piece for _, piece in rows) == text

    assert runner._external_record_set_ref_reachable(ref) is True
    loaded = runner._load_external_record_set(ref)
    assert RecordSet.from_cursor(loaded) == record_set


def test_pieced_and_single_value_forms_give_the_same_authority(
    clean_runtime: str, pieced: None, monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_set = _big_record_set()
    runner_new, ref_new = _store(clean_runtime, "test.pieces.new", record_set)
    # Single-value form: the whole text in one JSONB header row, as before.
    monkeypatch.setenv("OKG_SOURCE_RUNNER_RECORD_SET_JSONB_MAX_BYTES", str(1 << 20))
    runner_old, ref_old = _store(clean_runtime, "test.pieces.old", record_set)
    old_header = _header(clean_runtime, "test.pieces.old", ref_old["sha256"])
    assert old_header[2] is None and old_header[0] == record_set.to_cursor()

    assert ref_new["sha256"] == ref_old["sha256"]
    assert {k: v for k, v in ref_new.items() if k != "source"} == {
        k: v for k, v in ref_old.items() if k != "source"
    }
    new = runner_new._load_external_record_set(ref_new)
    old = runner_old._load_external_record_set(ref_old)
    assert new == old == record_set.to_cursor()


def test_legacy_record_set_text_row_still_reads(clean_runtime: str) -> None:
    source = "test.pieces.legacy-text"
    record_set = _big_record_set(5)
    text = dumps_record_set_json(record_set.to_cursor())
    digest = hashlib.sha256(text.encode()).hexdigest()
    runner = SourceRunner(clean_runtime, source_name=source)
    runner._ensure_source_record_sets_table()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.source_record_set_blobs
              (source, sha256, record_set, record_set_text)
            VALUES (%s, %s, '{}'::jsonb, %s)
            """,
            (source, digest, text),
        )
    ref = {
        "schema": "okg.source_runner.record_set_ref.v1",
        "storage": "okg.source_record_set_blobs",
        "source": source,
        "sha256": digest,
    }
    assert runner._external_record_set_ref_reachable(ref) is True
    assert runner._load_external_record_set(ref) == record_set.to_cursor()


def _mutate(dsn: str, sql: str, params) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql, params)


def test_missing_piece_fails_closed(clean_runtime: str, pieced: None) -> None:
    runner, ref = _store(clean_runtime, "test.pieces.missing", _big_record_set())
    write_id = _header(clean_runtime, "test.pieces.missing", ref["sha256"])[2]
    _mutate(
        clean_runtime,
        "DELETE FROM okg.source_record_set_blob_pieces WHERE write_id = %s AND piece_no = 2",
        (write_id,),
    )
    with pytest.raises(RecordSetPiecesError):
        runner._external_record_set_ref_reachable(ref)
    with pytest.raises(RecordSetPiecesError):
        runner._load_external_record_set(ref)
    # The prior-record-set path, used by retraction, refuses rather than
    # reading an empty prior.
    with pytest.raises(RecordSetPiecesError):
        runner._prior_record_set({"record_set_ref": ref})


def test_extra_piece_fails_closed(clean_runtime: str, pieced: None) -> None:
    source = "test.pieces.extra"
    runner, ref = _store(clean_runtime, source, _big_record_set())
    _, _, write_id, count, _, _ = _header(clean_runtime, source, ref["sha256"])
    _mutate(
        clean_runtime,
        """
        INSERT INTO okg.source_record_set_blob_pieces (write_id, piece_no, source, piece)
        VALUES (%s, %s, %s, '{}')
        """,
        (write_id, count, source),
    )
    with pytest.raises(RecordSetPiecesError):
        runner._external_record_set_ref_reachable(ref)
    with pytest.raises(RecordSetPiecesError):
        runner._load_external_record_set(ref)


def test_altered_piece_fails_closed(clean_runtime: str, pieced: None) -> None:
    source = "test.pieces.altered"
    runner, ref = _store(clean_runtime, source, _big_record_set())
    write_id = _header(clean_runtime, source, ref["sha256"])[2]
    _mutate(
        clean_runtime,
        """
        UPDATE okg.source_record_set_blob_pieces
           SET piece = overlay(piece placing 'Z' from 3 for 1)
         WHERE write_id = %s AND piece_no = 1
        """,
        (write_id,),
    )
    with pytest.raises(RecordSetPiecesError, match="digest"):
        runner._load_external_record_set(ref)


def test_same_digest_rerun_keeps_one_set_of_pieces_and_refreshes_summary(
    clean_runtime: str, pieced: None,
) -> None:
    source = "test.pieces.rerun"
    record_set = _big_record_set()
    runner, ref = _store(clean_runtime, source, record_set, summary={"n": 1})
    first = _header(clean_runtime, source, ref["sha256"])
    _, ref_again = _store(clean_runtime, source, record_set, summary={"n": 2})
    second = _header(clean_runtime, source, ref_again["sha256"])
    assert ref_again == ref
    assert second[2] == first[2]
    assert second[5] == {"n": 2}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        total = conn.execute(
            "SELECT count(*) FROM okg.source_record_set_blob_pieces WHERE source = %s",
            (source,),
        ).fetchone()[0]
    assert total == first[3]


def test_piece_without_header_is_refused_and_header_delete_cascades(
    clean_runtime: str, pieced: None,
) -> None:
    source = "test.pieces.fk"
    runner, ref = _store(clean_runtime, source, _big_record_set())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO okg.source_record_set_blob_pieces
                      (write_id, piece_no, source, piece)
                    VALUES (gen_random_uuid(), 0, %s, 'x')
                    """,
                    (source,),
                )
        conn.execute(
            "DELETE FROM okg.source_record_set_blobs WHERE source = %s", (source,),
        )
        left = conn.execute(
            "SELECT count(*) FROM okg.source_record_set_blob_pieces WHERE source = %s",
            (source,),
        ).fetchone()[0]
    assert left == 0


def test_failed_piece_write_leaves_nothing_behind(
    clean_runtime: str, pieced: None,
) -> None:
    source = "test.pieces.crash"
    runner = SourceRunner(clean_runtime, source_name=source)

    def chunks():
        yield from [("x" * 100, 100)] * 10
        raise RuntimeError("stream failed mid-write")

    with pytest.raises(RuntimeError):
        runner._store_pieced_record_set(
            chunks(), digest_fn=lambda: "0" * 64, summary={}, proof=None,
        )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        counts = conn.execute(
            """
            SELECT (SELECT count(*) FROM okg.source_record_set_blobs WHERE source = %s),
                   (SELECT count(*) FROM okg.source_record_set_blob_pieces WHERE source = %s)
            """,
            (source, source),
        ).fetchone()
    assert counts == (0, 0)


def test_pieced_prior_drives_retraction_in_a_real_run(
    clean_runtime: str, pieced: None,
) -> None:
    source = "test.pieces.run"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    record = _record(
        "docs/a.md",
        primary="doc:a",
        descendants=("chunk:a",),
        edges=(("doc:a", "contains", "chunk:a"),),
    )
    adapter = _RecordSetAdapter([
        ([
            _node("doc:a"),
            _node("chunk:a"),
            _edge("doc:a", "contains", "chunk:a"),
        ], record),
        ([], RecordSet()),
    ])
    # The second run empties the source on purpose, which the issue #2818
    # guard refuses unless the operator passes `--allow-full-retraction`.
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        full_retraction_override=FullRetractionOverride(
            source=source,
            requested_by="pieced-retraction-test",
            requested_at=datetime.now(timezone.utc),
        ),
    )
    first = runner.run(adapter, mode="scope_complete")
    ref = first.next_cursor["record_set_ref"]
    header = _header(clean_runtime, source, ref["sha256"])
    assert header[2] is not None and header[3] > 1
    second = runner.run(adapter, mode="scope_complete")
    assert second.retractions_node == 2
    assert second.retractions_edge == 1
    override = second.as_dict()["full_retraction_override"]
    assert override["exercised"] is True
    assert override["exercises"][0]["kind"] == "whole_source"


def test_pieces_schema_is_idempotent(clean_runtime: str) -> None:
    migration = (
        Path(__file__).resolve().parents[2]
        / "src/okg/substrate/db/migrations/0035_record_set_blob_pieces.sql"
    )
    with psycopg.connect(clean_runtime) as conn:
        for _ in range(2):
            conn.execute(migration.read_text())
            conn.execute(RECORD_SET_PIECES_DDL)
        conn.rollback()


def test_missing_piece_fails_a_real_scope_complete_run_by_name(
    clean_runtime: str, pieced: None,
) -> None:
    """A short pieced prior never becomes an empty prior in a real run.

    Before the review fix, reachability returned False, the adapter-facing
    cursor dropped the reference, and the run read an empty prior: stale
    records were then never retracted.
    """
    source = "test.pieces.run-missing"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    record = _record(
        "docs/a.md",
        primary="doc:a",
        descendants=("chunk:a",),
        edges=(("doc:a", "contains", "chunk:a"),),
    )
    adapter = _RecordSetAdapter([
        ([
            _node("doc:a"),
            _node("chunk:a"),
            _edge("doc:a", "contains", "chunk:a"),
        ], record),
        ([], RecordSet()),
    ])
    runner = SourceRunner(clean_runtime, source_name=source, write_fact_log=True)
    first = runner.run(adapter, mode="scope_complete")
    write_id = _header(
        clean_runtime, source, first.next_cursor["record_set_ref"]["sha256"],
    )[2]
    _mutate(
        clean_runtime,
        "DELETE FROM okg.source_record_set_blob_pieces WHERE write_id = %s AND piece_no = 1",
        (write_id,),
    )
    with pytest.raises(RecordSetPiecesError):
        runner.run(adapter, mode="scope_complete")
    # The adapter-facing cursor path refuses too, rather than dropping the
    # reference.
    with pytest.raises(RecordSetPiecesError):
        runner._prior_cursor_for_adapter(
            mode="scope_complete", prior_cursor=first.next_cursor,
        )


def test_piece_moved_to_another_source_fails_closed(
    clean_runtime: str, pieced: None,
) -> None:
    """Guards the source filters in the piece read and the reachability count."""
    source = "test.pieces.moved"
    runner, ref = _store(clean_runtime, source, _big_record_set())
    _, _, write_id, count, nbytes, _ = _header(clean_runtime, source, ref["sha256"])
    _mutate(
        clean_runtime,
        """
        UPDATE okg.source_record_set_blob_pieces SET source = 'test.pieces.other'
         WHERE write_id = %s AND piece_no = 1
        """,
        (write_id,),
    )
    # Reachability counts only this source's pieces.
    with pytest.raises(RecordSetPiecesError):
        runner._external_record_set_ref_reachable(ref)
    # count(*) by write_id still matches, so only the source filter in the
    # per-piece read stops the foreign piece being used.
    with pytest.raises(RecordSetPiecesError, match="missing"):
        runner._read_record_set_pieces(
            write_id, digest=ref["sha256"], piece_count=count, piece_bytes=nbytes,
        )


def test_pieced_header_may_not_also_carry_record_set_text(
    clean_runtime: str, pieced: None,
) -> None:
    source = "test.pieces.shape"
    runner, ref = _store(clean_runtime, source, _big_record_set())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                """
                UPDATE okg.source_record_set_blobs SET record_set_text = '{}'
                 WHERE source = %s AND sha256 = %s
                """,
                (source, ref["sha256"]),
            )


def test_same_digest_over_a_legacy_single_value_row_keeps_the_legacy_row(
    clean_runtime: str, pieced: None,
) -> None:
    source = "test.pieces.over-legacy"
    record_set = _big_record_set()
    text = dumps_record_set_json(record_set.to_cursor())
    digest = hashlib.sha256(text.encode()).hexdigest()
    runner = SourceRunner(clean_runtime, source_name=source)
    runner._ensure_source_record_sets_table()
    _mutate(
        clean_runtime,
        """
        INSERT INTO okg.source_record_set_blobs
          (source, sha256, record_set, record_set_text, record_set_summary)
        VALUES (%s, %s, '{}'::jsonb, %s, '{"n": 0}'::jsonb)
        """,
        (source, digest, text),
    )
    _, ref = _store(clean_runtime, source, record_set, summary={"n": 5})
    assert ref["sha256"] == digest
    header = _header(clean_runtime, source, digest)
    assert header[1] == text
    assert header[2] is None
    assert header[5] == {"n": 5}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        left = conn.execute(
            "SELECT count(*) FROM okg.source_record_set_blob_pieces WHERE source = %s",
            (source,),
        ).fetchone()[0]
    assert left == 0
    assert runner._load_external_record_set(ref) == record_set.to_cursor()
