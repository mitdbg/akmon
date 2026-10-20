from __future__ import annotations

import json

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.graph_store_internal import generations

# The fast path reads newest-first in pages of this size (issue #2202): a
# one-row read missed on every request whenever the newest checkpoint was
# filtered out, and each miss cost a full metadata fetch.
PAGE = generations._LATEST_GENERATION_SCAN_PAGE


class _Rows:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[dict[str, object]]:
        return self._rows


class _DB:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> _Rows:
        self.calls.append((sql, params))
        return _Rows(self.rows)


class _Context:
    def __init__(self, db: _DB) -> None:
        self.db = db


def test_latest_generation_fast_path_returns_newest_checkpoint_from_one_page() -> None:
    db = _DB([
        {
            "checkpoint_id": "gen:latest",
            "branch_id": "default",
            "segment_id": "seg:latest",
            "created_at": "2026-06-06T04:00:00+00:00",
            "metadata": json.dumps({
                "kind": "okg_generation",
                "deployment_name": "okg-workspace",
                "status": "published",
            }),
        },
    ])
    store = ChronosGraphStore(_Context(db), owns_context=False)

    checkpoint = store._latest_generation_checkpoint_fast(
        deployment_name="okg-workspace",
        branch="default",
        status="published",
    )

    assert checkpoint is not None
    assert checkpoint.checkpoint_id == "gen:latest"
    assert [call[1] for call in db.calls] == [("default", PAGE, 0)]


def test_latest_generation_fast_path_falls_back_on_mismatch() -> None:
    db = _DB([
        {
            "checkpoint_id": "not-the-generation",
            "branch_id": "default",
            "segment_id": "seg:latest",
            "created_at": "2026-06-06T04:00:00+00:00",
            "metadata": json.dumps({
                "kind": "some_other_checkpoint",
                "deployment_name": "okg-workspace",
                "status": "published",
            }),
        },
    ])
    store = ChronosGraphStore(_Context(db), owns_context=False)

    checkpoint = store._latest_generation_checkpoint_fast(
        deployment_name="okg-workspace",
        branch="default",
        status="published",
    )

    assert checkpoint is None
    # A short page is the end of the branch: no second read.
    assert [call[1] for call in db.calls] == [("default", PAGE, 0)]


def _row(checkpoint_id: str, *, status: str, kind: str = "okg_generation") -> dict[str, object]:
    return {
        "checkpoint_id": checkpoint_id,
        "branch_id": "default",
        "segment_id": f"seg:{checkpoint_id}",
        "created_at": "2026-06-06T04:00:00+00:00",
        "metadata": json.dumps({
            "kind": kind,
            "deployment_name": "okg-workspace",
            "status": status,
        }),
    }


class _PagedDB:
    """Answers LIMIT ? OFFSET ? from a newest-first list, as the table would."""

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.calls: list[tuple[object, ...]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> _Rows:
        self.calls.append(params)
        _branch, limit, offset = params
        return _Rows(self.rows[int(offset):int(offset) + int(limit)])


def test_a_blocked_newest_checkpoint_does_not_defeat_the_fast_path() -> None:
    """Issue #2202: the newest checkpoint is blocked, the caller asks for published."""
    db = _PagedDB([_row("gen:blocked", status="blocked"), _row("gen:published", status="published")])
    store = ChronosGraphStore(_Context(db), owns_context=False)

    checkpoint = store._latest_generation_checkpoint_fast(
        deployment_name="okg-workspace", branch="default", status="published",
    )

    assert checkpoint is not None
    assert checkpoint.checkpoint_id == "gen:published"
    assert db.calls == [("default", PAGE, 0)]


def test_a_match_on_the_second_page_is_found_by_reading_the_next_page() -> None:
    rows = [_row(f"gen:blocked:{i}", status="blocked") for i in range(PAGE)]
    rows.append(_row("gen:published", status="published"))
    db = _PagedDB(rows)
    store = ChronosGraphStore(_Context(db), owns_context=False)

    checkpoint = store._latest_generation_checkpoint_fast(
        deployment_name="okg-workspace", branch="default", status="published",
    )

    assert checkpoint is not None
    assert checkpoint.checkpoint_id == "gen:published"
    assert db.calls == [("default", PAGE, 0), ("default", PAGE, PAGE)]


def test_the_scan_stops_at_its_cap_and_hands_back_to_the_full_read() -> None:
    """None past the cap is the caller's signal to take the full fetch."""
    rows = [_row(f"gen:blocked:{i}", status="blocked") for i in range(PAGE * 4)]
    rows.append(_row("gen:published", status="published"))
    db = _PagedDB(rows)
    store = ChronosGraphStore(_Context(db), owns_context=False)

    checkpoint = store._latest_generation_checkpoint_fast(
        deployment_name="okg-workspace", branch="default", status="published",
        limit=PAGE * 2,
    )

    assert checkpoint is None
    assert db.calls == [("default", PAGE, 0), ("default", PAGE, PAGE)]
