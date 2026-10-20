"""Pieced record-set authority: streaming, piecing and fail-closed reassembly.

Issue #3008. No database: these pin the pure parts that decide whether the
pieced form stores exactly what the single-value form stored.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import pytest

from okg.substrate.ingest.record_set_pieces import (
    PIECED_RECORD_SET_MARKER,
    RecordSetPiecesError,
    dumps_record_set_json,
    iter_pieces,
    iter_record_set_json,
    reassemble_pieces,
)
from okg.substrate.library.sources.base import (
    EdgeKey,
    RecordEmission,
    RecordNode,
    RecordSet,
)


def _emission(key: str, n_edges: int = 2) -> RecordEmission:
    primary = RecordNode(
        node_id=f"doc:{key}",
        subtype="Document",
        source_record_id={"path": key},
        source_revision={"sha": "abc", "when": "2026-09-29"},
    )
    return RecordEmission(
        primary=primary,
        descendants=(
            RecordNode(node_id=f"chunk:{key}", subtype="Chunk"),
        ),
        edges=tuple(
            EdgeKey(
                src=f"doc:{key}",
                dst=f"doc:{key}-{i}",
                edge_type="links",
                attrs={"weight": i / 3, "note": "café ☃", "z": [1, {"b": 2, "a": 1}]},
            )
            for i in range(n_edges)
        ),
    )


def _record_set(n: int) -> RecordSet:
    # Insert out of order: the canonical text sorts record keys.
    keys = [f"docs/{i:04d}.md" for i in range(n)]
    keys.reverse()
    return RecordSet(records={key: _emission(key) for key in keys})


def _joined(value) -> str:
    return "".join(iter_record_set_json(value))


@pytest.mark.parametrize("n", [0, 1, 7, 50])
def test_streamed_record_set_equals_one_shot_json(n: int) -> None:
    record_set = _record_set(n)
    assert _joined(record_set) == dumps_record_set_json(record_set.to_cursor())
    assert _joined(record_set.to_cursor()) == dumps_record_set_json(
        record_set.to_cursor(),
    )


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"records": {}},
        {"records": {"b": {"x": 1}, "a": [1, 2, {"k": "é"}]}},
        {"records": {"a": {"when": datetime(2026, 9, 29, tzinfo=timezone.utc)}}},
        {"z": 1, "a": {"nested": {"deep": {"deeper": [None, True, 1.5]}}}},
        {"records": {1: "non-str key"}},
        {"records": [1, 2, 3]},
    ],
)
def test_streamed_mapping_equals_one_shot_json(value) -> None:
    # Includes default=str values and non-str keys, which fall back to the
    # one-shot encoder for that subtree.
    assert _joined(value) == dumps_record_set_json(value)


def _chunks(text_parts):
    return [(part, len(part.encode("utf-8"))) for part in text_parts]


def test_pieces_are_bounded_ordered_and_rejoin_exactly() -> None:
    record_set = _record_set(40)
    text = _joined(record_set)
    chunks = _chunks(iter_record_set_json(record_set))
    pieces = list(iter_pieces(chunks, 700))
    assert len(pieces) > 3
    assert all(nbytes <= 700 for _, nbytes in pieces)
    assert all(len(piece.encode()) == nbytes for piece, nbytes in pieces)
    assert "".join(piece for piece, _ in pieces) == text


def test_oversized_chunk_is_split_at_code_points() -> None:
    text = "é☃" * 100 + "x" * 50
    pieces = list(iter_pieces(_chunks([text]), 16))
    assert all(nbytes <= 16 for _, nbytes in pieces)
    assert "".join(piece for piece, _ in pieces) == text


def test_piece_size_below_minimum_is_refused() -> None:
    with pytest.raises(ValueError):
        list(iter_pieces(_chunks(["abc"]), 3))


def _stored(text: str, piece_max: int):
    pieces = list(iter_pieces(_chunks([text]), piece_max))
    rows = [(no, piece) for no, (piece, _) in enumerate(pieces)]
    return rows, {
        "source": "s",
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "piece_count": len(rows),
        "piece_bytes": len(text.encode()),
    }


def test_reassembly_returns_original_bytes_and_digest_covers_all_pieces() -> None:
    text = _joined(_record_set(30))
    rows, header = _stored(text, 500)
    assert header["piece_count"] > 1
    joined = reassemble_pieces(rows, **header)
    # The buffer itself, not a copy: json.loads takes it directly.
    assert isinstance(joined, bytearray)
    assert joined == text.encode()
    assert json.loads(reassemble_pieces(rows, **header)) == json.loads(text)


def test_reassembly_refuses_a_missing_piece() -> None:
    rows, header = _stored(_joined(_record_set(30)), 500)
    with pytest.raises(RecordSetPiecesError, match="missing"):
        reassemble_pieces(rows[:-1], **header)
    with pytest.raises(RecordSetPiecesError):
        reassemble_pieces(rows[:1] + rows[2:], **header)


def test_reassembly_refuses_an_extra_or_reordered_piece() -> None:
    rows, header = _stored(_joined(_record_set(30)), 500)
    with pytest.raises(RecordSetPiecesError):
        reassemble_pieces(rows + [(len(rows), "{}")], **header)
    with pytest.raises(RecordSetPiecesError):
        reassemble_pieces([rows[1], rows[0], *rows[2:]], **header)
    with pytest.raises(RecordSetPiecesError):
        reassemble_pieces([rows[0], *rows], **header)


def test_reassembly_refuses_altered_bytes_or_digest() -> None:
    rows, header = _stored(_joined(_record_set(30)), 500)
    altered = list(rows)
    no, piece = altered[1]
    # Same length, different content: only the digest can catch it.
    altered[1] = (no, piece[:-1] + ("0" if piece[-1] != "0" else "1"))
    with pytest.raises(RecordSetPiecesError, match="digest"):
        reassemble_pieces(altered, **header)
    with pytest.raises(RecordSetPiecesError, match="size"):
        reassemble_pieces(rows, **{**header, "piece_bytes": header["piece_bytes"] + 1})


def test_pieced_marker_is_not_a_readable_record_set() -> None:
    # Older readers that load the header's JSONB must refuse it, not read an
    # empty prior record set.
    with pytest.raises(ValueError):
        RecordSet.from_cursor(dict(PIECED_RECORD_SET_MARKER))
