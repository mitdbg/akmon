"""What `console/source-detail` serves, over rows a real deployment stored.

PACT console-producer-found-and-detail-contract (issue #2118).

The read used to answer `found`, which a reader takes to mean "this source
exists" or "this source has records". It meant neither: it was true only when
`okg.source_progress` held a row under that exact source name. On
okg-workspace `_system:native_views` holds 8 published records and the read
answered `found: false` for it. The flag is now `sync_progress_recorded`, the
thing it measures.

The read also parsed the `sync.producer.poll` row with a fallback: a value that
was not JSON became `{}`, and every missing count became `0`. An unreadable
poll therefore printed as "0 queued, 0 errors". It now says the poll could not
be read and why, and a count the poll did not record is null.

The rows below are the ones okg-workspace stored for `git-files`, captured
read-only into this change's evidence directory. The failure cases change one
row's value to a condition that deployment does not hold, named in each test.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.console_views.source_detail import compose_source_detail

# A copy of the leak-checked capture recorded as evidence by PACT
# console-producer-found-and-detail-contract. It lives under tests/fixtures so
# the public copy, which has no change records, runs these tests too; the
# release scan finds nothing in it.
_CAPTURE = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "console_views"
    / "live-source-progress-rows.json"
)

_Row = tuple[str, Any, "datetime | None"]


def _captured_rows(source_name: str) -> list[_Row]:
    doc = json.loads(_CAPTURE.read_text(encoding="utf-8"))
    return [
        (row["key"], row["value"], datetime.fromisoformat(row["updated_at"]))
        for row in doc["rows"]
        if row["source_name"] == source_name
    ]


class _Cursor:
    def __init__(self, rows: list[_Row]) -> None:
        self._rows = rows
        self.params: dict[str, Any] | None = None

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, _sql: str, params: dict[str, Any]) -> None:
        self.params = params

    def fetchall(self) -> list[_Row]:
        return list(self._rows)


class _Conn:
    def __init__(self, rows: list[_Row]) -> None:
        self.cursor_obj = _Cursor(rows)

    def cursor(self) -> _Cursor:
        return self.cursor_obj


def _detail(rows: list[_Row], name: str) -> dict[str, Any]:
    conn = _Conn(rows)
    payload = compose_source_detail(
        conn,
        deployment_name="okg-workspace",
        session=object(),  # type: ignore[arg-type]
        source_name=name,
    )
    assert conn.cursor_obj.params == {"d": "okg-workspace", "s": name}
    return payload


def _with_poll(value: Any) -> list[_Row]:
    """git-files' captured rows with the poll row's value replaced."""
    return [
        (key, value if key == "sync.producer.poll" else stored, at)
        for key, stored, at in _captured_rows("git-files")
    ]


def test_captured_rows_have_a_readable_poll() -> None:
    """The capture is what the rest of this file leans on; pin its shape."""
    rows = _captured_rows("git-files")
    keys = [key for key, _value, _at in rows]
    assert "sync.producer.poll" in keys
    assert len(keys) >= 2
    assert _captured_rows("_system:native_views") == []
    text = _CAPTURE.read_text(encoding="utf-8")
    for leak in ("postgres://", "postgresql://", "/Users/", "/home/", "127.0.0.1"):
        assert leak not in text


def test_real_rows_report_sync_progress_and_the_poll_counts_stored() -> None:
    rows = _captured_rows("git-files")
    stored_poll = json.loads(
        next(value for key, value, _at in rows if key == "sync.producer.poll")
    )

    payload = _detail(rows, "git-files")

    assert "found" not in payload
    assert payload["sync_progress_recorded"] is True
    assert payload["poll_unreadable_reason"] is None
    poll = payload["poll"]
    assert poll["enqueue_count"] == stored_poll["enqueue_count"]
    assert poll["error_count"] == stored_poll["error_count"]
    assert poll["last_run_at"] == stored_poll["last_run_at"]
    # The age is the progress row's own update time, and is named for it.
    assert "last_run_age" not in poll
    assert poll["recorded_age"]
    assert [row["key"] for row in payload["progress"]] == [
        key for key, _value, _at in rows if key != "sync.producer.poll"
    ]


def test_source_with_records_but_no_progress_rows_says_no_sync_progress() -> None:
    """The issue's case: `_system:native_views` holds published records and
    stores no progress rows. The read says what it knows -- no sync progress
    -- and does not claim the source is missing."""
    payload = _detail(_captured_rows("_system:native_views"), "_system:native_views")

    assert payload == {
        "source_name": "_system:native_views",
        "sync_progress_recorded": False,
        "poll": None,
        "poll_unreadable_reason": None,
        "progress": [],
    }


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        ('{"enqueue_count": 3, "error_count"', "poll_value_not_json"),
        ("[1, 2]", "poll_value_not_an_object"),
        ('{"enqueue_count": "many", "error_count": 0}', "poll_count_not_an_integer"),
        ('{"enqueue_count": true, "error_count": 0}', "poll_count_not_an_integer"),
        ('{"enqueue_count": 2, "error_count": -1}', "poll_count_not_an_integer"),
    ],
)
def test_unreadable_poll_is_named_never_zero(value: str, reason: str) -> None:
    payload = _detail(_with_poll(value), "git-files")

    assert payload["poll"] is None
    assert payload["poll_unreadable_reason"] == reason
    # Progress rows exist, so there IS sync progress; the poll row among them
    # simply could not be read.
    assert payload["sync_progress_recorded"] is True
    assert "sync.producer.poll" not in [row["key"] for row in payload["progress"]]


def test_count_the_poll_did_not_record_is_null_not_zero() -> None:
    payload = _detail(
        _with_poll('{"last_run_at": "2026-09-20T15:46:45Z"}'), "git-files"
    )

    assert payload["poll_unreadable_reason"] is None
    assert payload["poll"]["enqueue_count"] is None
    assert payload["poll"]["error_count"] is None
    assert payload["poll"]["last_run_at"] == "2026-09-20T15:46:45Z"


def test_poll_stored_as_jsonb_object_is_read() -> None:
    """psycopg hands a jsonb column back as a dict; the read takes it as is."""
    payload = _detail(_with_poll({"enqueue_count": 4, "error_count": 1}), "git-files")

    assert payload["poll"]["enqueue_count"] == 4
    assert payload["poll"]["error_count"] == 1


def test_failed_progress_read_says_unknown_not_no(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the progress read itself fails, the source diagnostics read keeps
    the rest of its answer and says it does not know -- not that the source
    has no sync progress."""
    from types import SimpleNamespace

    from okg.substrate.console_views import source_diagnostics

    monkeypatch.setattr(
        source_diagnostics,
        "compose_sources",
        lambda *_args, **_kwargs: {
            "sources": [{
                "name": "git-files",
                "source_key": "git-files",
                "in_registry": True,
                "records_in_gen": 11757,
                "edges_in_gen": 11756,
                "fact_counts_available": True,
                "last_sync_age": 3,
            }]
        },
    )
    monkeypatch.setattr(
        source_diagnostics,
        "compose_console_readiness",
        lambda **_kwargs: {"details": {"readiness": {"source_sync_status": {}}}},
    )
    monkeypatch.setattr(
        source_diagnostics,
        "_latest_runs_by_source",
        lambda *_args, **_kwargs: ({"git-files": []}, {"status": "available"}),
    )

    def _progress_read_fails(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("relation okg.source_progress is locked")

    monkeypatch.setattr(source_diagnostics, "compose_source_detail", _progress_read_fails)

    result = source_diagnostics.compose_source_diagnostics(
        object(),
        deployment_name="okg-workspace",
        deployment_dir=Path("."),
        dsn="postgresql://unused",
        session=SimpleNamespace(generation_id="g1"),
        selected_source="git-files",
    )

    progress = result["selected"]["source_progress"]
    assert "found" not in progress
    assert progress["sync_progress_recorded"] is None
    assert progress["unavailable_reason"] == "source_progress_read_failed"
    assert progress["error_type"] == "RuntimeError"
    assert "locked" not in json.dumps(progress)
    assert result["read_authority"]["selected_progress"]["status"] == "unavailable"
    # The published contribution is untouched by the failed progress read.
    assert result["selected"]["row"]["contribution"]["records_in_gen"] == 11757
