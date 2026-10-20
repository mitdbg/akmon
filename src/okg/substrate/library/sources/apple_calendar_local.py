"""apple_calendar_local source — macOS Calendar sqlite reader.

Per `refine-academic-pi-with-comms-and-calendar` Tier C:
reads `~/Library/Calendars/Calendar.sqlitedb` read-only (copy
to temp first to avoid contention with Calendar.app). Same
strict-allowlist filter as calendar_ics. Non-macOS hosts
degrade to zero rows.

Per `cover-calendar-sources`: `calendar_db_path` is injectable
(the platform gate applies only to the default macOS path —
reading a fixture sqlite is platform-independent, which is what
makes this source CI-testable off-macOS); a missing DB or a
failing schema/query warns loudly with the named error instead of
returning silently (a macOS schema change must be a visible
signal, not zero rows); and the temp working copy is removed when
the stream ends.
"""
from __future__ import annotations

import hashlib
import logging
import platform
import re
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Iterable, Optional

from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourceRun,
)

_log = logging.getLogger(__name__)

_DEFAULT_RECURRING_PATTERNS = [
    r"\blab\s+meeting\b",
    r"\bgroup\s+meeting\b",
    r"\b1[:\-\s]+1\b",
    r"\boffice\s+hours\b",
]

_DEFAULT_DB_PATH = "~/Library/Calendars/Calendar.sqlitedb"


class AppleCalendarLocalSource:
    """macOS local Calendar sqlite reader. Linux/Windows: skip
    (unless an explicit `calendar_db_path` is provided)."""

    name: str = "apple_calendar_local"
    profile: str = "mutable_api"

    def __init__(
        self,
        *,
        recurring_patterns: Optional[list[str]] = None,
        calendar_db_path: Optional[str] = None,
        ownership_id: str = "apple_calendar_local",
        **_ignored: Any,
    ) -> None:
        self.patterns = [
            re.compile(p, re.I)
            for p in (recurring_patterns or _DEFAULT_RECURRING_PATTERNS)
        ]
        self.calendar_db_path = calendar_db_path
        self.ownership_id = ownership_id

    def run(
        self, run_id: str, *, mode: str = "cursor", **_ignored: Any,
    ) -> SourceRun:
        if self.calendar_db_path is None and platform.system() != "Darwin":
            # Graceful degradation per the spec — log skip
            # metric, no error. An explicit calendar_db_path
            # bypasses this gate: fixture sqlite reads are
            # platform-independent.
            _log.info(
                "apple_calendar_local: skip:platform_unsupported "
                f"(running on {platform.system()})"
            )
            return SourceRun(facts=iter([]), completed_scope=True)

        sqlite_path = Path(
            self.calendar_db_path or _DEFAULT_DB_PATH
        ).expanduser()
        # Path.is_file() swallows PermissionError, which on macOS
        # means TCC privacy protection — a different operator action
        # than a missing file. Found live on 2026-06-10: the first
        # production run reported "not found" for what was actually
        # an access denial (cover-calendar-sources).
        try:
            sqlite_path.stat()
        except PermissionError:
            _log.warning(
                "apple_calendar_local: macOS privacy protection (TCC) "
                "is blocking access to %s; grant the process running "
                "okg Full Disk Access in System Settings -> Privacy & "
                "Security, then re-run. Zero rows this run.",
                sqlite_path,
            )
            return SourceRun(facts=iter([]), completed_scope=True)
        except FileNotFoundError:
            _log.warning(
                "apple_calendar_local: Calendar DB not found at %s; "
                "zero rows this run",
                sqlite_path,
            )
            return SourceRun(facts=iter([]), completed_scope=True)
        if not sqlite_path.is_file():
            _log.warning(
                "apple_calendar_local: %s exists but is not a file; "
                "zero rows this run",
                sqlite_path,
            )
            return SourceRun(facts=iter([]), completed_scope=True)

        # Copy to temp to avoid Calendar.app contention; the copy
        # is removed when the stream ends (pre-fix it leaked one
        # file per run).
        with tempfile.NamedTemporaryFile(
            suffix=".sqlitedb", delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)
        shutil.copy2(sqlite_path, tmp_path)

        return SourceRun(facts=self._stream(tmp_path), completed_scope=True)

    def _stream(self, sqlite_path: Path) -> Iterable[Any]:
        try:
            feed_id = "feed:apple:local"
            yield NodeFact(
                node_id=feed_id,
                subtype="calendar_feed",
                attrs={
                    "feed_id": feed_id,
                    "name": "macOS Calendar (local)",
                    "source_kind": "apple_local",
                    "external_ref": str(sqlite_path),
                },
                source_record_id={"feed_id": feed_id},
                source_revision={"content_sha": "static"},
            )

            try:
                conn = sqlite3.connect(
                    f"file:{sqlite_path}?mode=ro", uri=True,
                )
                cur = conn.cursor()
                # ZCALENDARITEM is the Apple Calendar table; columns
                # ZTITLE / ZSTARTDATE / ZENDDATE / ZLOCATION /
                # ZALLDAY. Times are Mac epoch (seconds since 2001).
                cur.execute(
                    "SELECT ZTITLE, ZSTARTDATE, ZENDDATE, ZLOCATION, "
                    "ZUNIQUEIDENTIFIER FROM ZCALENDARITEM"
                )
                rows = cur.fetchall()
                conn.close()
            except sqlite3.Error as exc:
                # Loud by contract: a schema change or corrupt DB is
                # a visible signal, never a silent zero-row run.
                _log.warning(
                    "apple_calendar_local: Calendar DB query failed "
                    "(%s: %s) — zero events this run; the macOS "
                    "Calendar schema may have changed",
                    type(exc).__name__,
                    exc,
                )
                return

            for idx, (title, start, end, location, uid) in enumerate(rows):
                if not _allowlisted_text(
                    title or "", "", self.patterns,
                ):
                    continue
                event_id = f"event:apple:{uid or idx}"
                content_hash = hashlib.sha256(
                    f"{title}{start}{end}".encode("utf-8")
                ).hexdigest()
                yield NodeFact(
                    node_id=event_id,
                    subtype="calendar_event",
                    attrs={
                        "event_id": event_id,
                        "title": title or "",
                        "kind": "other",
                        "start_dt": _apple_to_iso(start),
                        "end_dt": _apple_to_iso(end),
                        "location": location or "",
                        "external_ref": uid or "",
                        "feed_ref": feed_id,
                    },
                    source_record_id={"uid": uid or idx},
                    source_revision={"content_sha": content_hash},
                )
                yield EdgeFact(
                    src=event_id, dst=feed_id, edge_type="at_feed",
                    source_record_id={"uid": uid or idx},
                )
                yield ProgressMarker(
                    record_id=str(uid or idx), fingerprint=content_hash,
                )
        finally:
            sqlite_path.unlink(missing_ok=True)


def _allowlisted_text(
    title: str, description: str, patterns: list[re.Pattern],
) -> bool:
    text = title + " " + description
    if "okg:" in text.lower():
        return True
    return any(p.search(text) for p in patterns)


def _apple_to_iso(mac_epoch_seconds: Optional[float]) -> str:
    """Apple's date columns are seconds since 2001-01-01.
    Convert to ISO 8601.
    """
    if mac_epoch_seconds is None:
        return ""
    from datetime import datetime, timedelta, timezone
    base = datetime(2001, 1, 1, tzinfo=timezone.utc)
    return (base + timedelta(seconds=mac_epoch_seconds)).isoformat()
