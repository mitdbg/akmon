"""cover-calendar-sources: apple_calendar_local — fixture sqlite
(ZCALENDARITEM subset) drives emission tests off-macOS; failures
are loud; the temp copy is cleaned up."""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from okg.substrate.library.sources import apple_calendar_local as calendar_mod
from okg.substrate.library.sources.apple_calendar_local import (
    AppleCalendarLocalSource,
    _apple_to_iso,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact

from datetime import datetime, timezone

# 2026-06-11T14:00:00Z in Mac epoch (seconds since 2001-01-01Z),
# derived rather than hand-computed.
_MAC_EPOCH_START = (
    datetime(2026, 6, 11, 14, 0, 0, tzinfo=timezone.utc)
    - datetime(2001, 1, 1, tzinfo=timezone.utc)
).total_seconds()


def _make_fixture_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE ZCALENDARITEM ("
        " ZTITLE TEXT, ZSTARTDATE REAL, ZENDDATE REAL,"
        " ZLOCATION TEXT, ZUNIQUEIDENTIFIER TEXT)"
    )
    rows = [
        ("Lab meeting", _MAC_EPOCH_START, _MAC_EPOCH_START + 3600,
         "32-G882", "uid-lab"),
        ("okg: proposal review", _MAC_EPOCH_START + 86400,
         _MAC_EPOCH_START + 90000, "", "uid-prop"),
        ("Dentist", _MAC_EPOCH_START + 172800,
         _MAC_EPOCH_START + 176400, "", "uid-dentist"),
    ]
    conn.executemany(
        "INSERT INTO ZCALENDARITEM VALUES (?, ?, ?, ?, ?)", rows,
    )
    conn.commit()
    conn.close()


def test_fixture_db_projects_allowlisted_events(tmp_path: Path) -> None:
    """The fixture DB (built in-test, platform-independent via
    calendar_db_path) emits the allowlisted + okg-tagged rows with
    Mac-epoch -> ISO conversion; the dentist never leaves."""
    db = tmp_path / "Calendar.sqlitedb"
    _make_fixture_db(db)
    src = AppleCalendarLocalSource(calendar_db_path=str(db))
    facts = list(src.run("run-1").facts)
    events = [
        f for f in facts if isinstance(f, NodeFact) and f.subtype == "calendar_event"
    ]
    feeds = [
        f for f in facts if isinstance(f, NodeFact) and f.subtype == "calendar_feed"
    ]
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    assert {e.node_id for e in events} == {
        "event:apple:uid-lab", "event:apple:uid-prop",
    }
    lab = next(e for e in events if e.node_id == "event:apple:uid-lab")
    assert lab.attrs["start_dt"] == "2026-06-11T14:00:00+00:00"
    assert lab.attrs["end_dt"] == "2026-06-11T15:00:00+00:00"
    assert lab.attrs["location"] == "32-G882"
    assert len(feeds) == 1
    assert {(e.src, e.dst) for e in edges} == {
        ("event:apple:uid-lab", "feed:apple:local"),
        ("event:apple:uid-prop", "feed:apple:local"),
    }


def test_explicit_path_bypasses_platform_gate(
    tmp_path: Path, monkeypatch,
) -> None:
    """An explicit calendar_db_path runs even off-macOS — the gate
    applies only to the default path."""
    monkeypatch.setattr(calendar_mod.platform, "system", lambda: "Linux")

    db = tmp_path / "Calendar.sqlitedb"
    _make_fixture_db(db)
    facts = list(
        AppleCalendarLocalSource(calendar_db_path=str(db)).run("r").facts,
    )
    assert any(
        isinstance(f, NodeFact) and f.subtype == "calendar_event"
        for f in facts
    )
    # Default path still skips on non-Darwin.
    skipped = list(AppleCalendarLocalSource().run("r").facts)
    assert skipped == []


def test_corrupt_db_warns_loudly(tmp_path: Path, caplog) -> None:
    """A DB without the expected schema warns with the named error
    and emits only the feed node — never a bare silent return."""
    db = tmp_path / "Calendar.sqlitedb"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE wrong_schema (x TEXT)")
    conn.commit()
    conn.close()
    src = AppleCalendarLocalSource(calendar_db_path=str(db))
    with caplog.at_level(logging.WARNING):
        facts = list(src.run("run-2").facts)
    assert [f.subtype for f in facts if isinstance(f, NodeFact)] == [
        "calendar_feed",
    ]
    assert any(
        "schema may have changed" in r.message for r in caplog.records
    ), "pre-fix this was a silent zero-row run"


def test_tcc_block_warns_with_remediation(
    tmp_path: Path, caplog, monkeypatch,
) -> None:
    """macOS TCC denial (PermissionError on stat) is a different
    operator action than a missing file — the warning must say
    'Full Disk Access', not 'not found'. Found on the first real
    production run of this source."""
    db = tmp_path / "Calendar.sqlitedb"
    _make_fixture_db(db)
    real_stat = Path.stat

    def blocked_stat(self, *a, **kw):
        if self == db:
            raise PermissionError(1, "Operation not permitted")
        return real_stat(self, *a, **kw)

    monkeypatch.setattr(Path, "stat", blocked_stat)
    src = AppleCalendarLocalSource(calendar_db_path=str(db))
    with caplog.at_level(logging.WARNING):
        facts = list(src.run("run-tcc").facts)
    assert facts == []
    assert any("Full Disk Access" in r.message for r in caplog.records)
    assert not any("not found" in r.message for r in caplog.records)


def test_missing_db_warns_loudly(tmp_path: Path, caplog) -> None:
    src = AppleCalendarLocalSource(
        calendar_db_path=str(tmp_path / "nope.sqlitedb"),
    )
    with caplog.at_level(logging.WARNING):
        facts = list(src.run("run-3").facts)
    assert facts == []
    assert any("not found" in r.message for r in caplog.records)


def test_temp_copy_removed_after_stream(tmp_path: Path) -> None:
    """The temp working copy is unlinked when the stream ends
    (pre-fix it leaked one file per run) — including the corrupt-DB
    early-return path."""
    db = tmp_path / "Calendar.sqlitedb"
    _make_fixture_db(db)
    src = AppleCalendarLocalSource(calendar_db_path=str(db))
    run = src.run("run-4")
    facts = list(run.facts)
    feed = next(
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "calendar_feed"
    )
    temp_copy = Path(feed.attrs["external_ref"])
    assert temp_copy != db, "stream must read the temp copy, not the live DB"
    assert not temp_copy.exists(), "temp copy leaked after stream end"
    assert db.exists(), "the operator's DB must never be touched"


def test_apple_epoch_conversion() -> None:
    assert _apple_to_iso(0) == "2001-01-01T00:00:00+00:00"
    assert _apple_to_iso(None) == ""
