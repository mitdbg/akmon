"""Reuse one record's reader-visible text across the pages of a read (#3002).

A continuation page of `inspect(target={"kind":"text"})` used to rebuild the
whole record: read every occurrence edge and chunk, project effective attrs,
filter, order and normalize the segments, then copy them, all to serve one
page. Page time grew with the record's size, and a whole read with its square.

This module keeps the cursor-independent part of that work, the record's
`RecordDocument`, in a small per-process LRU, so a later page of the same
record costs its own slice. Page content, cursors and tokens are unchanged:
the page builder reads the same segments it would have rebuilt.

When an entry may be used. All of these hold, or the read rebuilds as before:

* The session's reader is known and fixed for the read: no Nomos identity
  claim and no access context (their resolution reads provider, binding and
  grant state that can change mid-read, and an identity claim expires), and
  a multi-user session carries its request subject.
* The key matches. It names everything that decides what this reader sees:
  database fingerprint and oid (a recreated database of the same name
  restarts its generation ids), deployment, branch, pinned generation,
  catalog version (the subtype redaction classifiers come from it), node id,
  whether source trust was classified, and the Nomos side: the deployment's
  enforcement mode, the active policy bundle's fingerprint (read from the
  database on every call, never memoized; ``None`` when no bundle is
  active), the subject the evaluator records (the session's, else the
  process environment's), the read action and surface, and the multi-user
  flag. A pinned generation's rows do not change. Source trust is producer
  policy that can change at any time, so it is never cached; chunk targets
  are never cached either.
* Under Nomos ``audit`` and ``enforce`` an entry serves only a continuation
  page (the caller passes a signed cursor of a read the first page began).
  The first page of every read rebuilds, so every read decides each of its
  decision classes once and records those decisions; later pages of that
  read reuse the first page's filtered text and record nothing new. The
  decision trail is insert-once by content of access (subject, action,
  policy fingerprint, resource), so for an active bundle the later pages'
  decisions were already rows; with no active bundle each filter call
  writes its own decision, so the about three decisions of every uncached
  page become about three per read (issue #3002's plan).
* Under Nomos a batch read (``can_continue=False``) neither reads nor
  stores an entry. It does continue, through its own batch token, but it
  re-prepares each source on every batch page and never presents a text
  cursor, so no continuation page could reuse what it stored.
* An entry is stored only when the active policy fingerprint read after the
  build equals the one in the key: a bundle activated during the build
  could have decided some rows, and such a document is never kept.
* The effective-attribute overlay rows of every row the build read (the
  record, its occurrence edges and its chunks, hidden ones included) are
  the ones it was built with. Publish writes a generation's overlay
  snapshot once (`materialize_effective_attributes_for_generation`), so for
  a published pin they do not change; every hit still re-reads them with
  indexed queries and rebuilds on any difference, so a repaired snapshot is
  never served stale. The stored digest is taken right BEFORE the build
  reads each set of rows (`OverlayCapture`), so a repair that lands during
  the build leaves a digest that no longer matches, and the next page
  rebuilds.
* Known limit: a policy that flips from A to B and back to A during one
  build passes the fingerprint check after the build, so a document some of
  whose rows B decided can be stored under A's key. An uncached read has
  the same mixed view (its three filter calls each read the policy), and
  the first page served it already.
* Known limit: one `_overlaid` call reads the digest rows, then projects
  the attributes in a second query. An overlay repair that commits between
  the two and is reverted before the next page leaves the current rows
  equal to the digest, so the next page serves text built with the
  repaired value under the original digest. It needs two operator writes
  to one generation's overlay snapshot within milliseconds of each other.

The entry holds the already-filtered document: hidden rows are absent and
redacted attrs are masked, exactly as the build returned them.

``OKG_RECORD_TEXT_CACHE_ENTRIES`` sets the entry cap (default 8; 0 turns the
cache off). Entries are also capped by their total text bytes.
"""
from __future__ import annotations

import hashlib
import os
import threading
from collections import OrderedDict
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

import psycopg

from okg.substrate.attributes import ATTRIBUTE_EFFECTIVE_TABLE
from okg.substrate.errors import SubstrateError

ENTRIES_ENV = "OKG_RECORD_TEXT_CACHE_ENTRIES"
DEFAULT_ENTRIES = 8
#: Total text bytes the cache may hold across entries.
MAX_TEXT_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class RecordDocument:
    """The cursor-independent result of reading one record's text.

    Everything in it is read-only after construction: the page builder and
    batch allocation only read segments, never write them.
    """

    record: dict[str, Any]
    is_chunk: bool
    parents: tuple[dict[str, Any], ...]
    parents_complete: bool
    segments: tuple[Any, ...]
    order_basis: str
    coverage: Any

    @property
    def text_bytes(self) -> int:
        """The entry's size for the cap: segment text plus the record row."""
        record = len(repr(self.record).encode("utf-8"))
        return record + sum(len(segment.text.encode("utf-8")) for segment in self.segments)


@dataclass
class _Entry:
    document: RecordDocument
    #: `(target_kind, target_id)` of every row whose overlay the build read.
    overlay_targets: tuple[tuple[str, str], ...]
    overlay_digest: str
    text_bytes: int


_lock = threading.Lock()
_entries: "OrderedDict[CacheKey, _Entry]" = OrderedDict()


def max_entries() -> int:
    raw = os.environ.get(ENTRIES_ENV, "").strip()
    if not raw:
        return DEFAULT_ENTRIES
    try:
        value = int(raw)
    except ValueError as exc:
        raise SubstrateError(
            f"{ENTRIES_ENV} must be an integer, not {raw!r}",
            data={"value": raw},
        ) from exc
    return max(0, value)


def _database_oid(conn: psycopg.Connection) -> int:
    """The database's oid: a dropped and recreated database of the same name
    gets a new one, while its generation and catalog ids start over."""
    cached = getattr(conn, "_okg_record_text_db_oid", None)
    if cached is not None:
        return cached
    with conn.cursor() as cur:
        cur.execute("SELECT oid FROM pg_database WHERE datname = current_database()")
        row = cur.fetchone()
    oid = int(row[0])
    try:
        conn._okg_record_text_db_oid = oid  # type: ignore[attr-defined]
    except AttributeError:
        pass
    return oid


@dataclass(frozen=True)
class CacheKey:
    """One read's cache key and the Nomos facts its use depends on."""

    parts: tuple[Any, ...]
    deployment_name: str
    #: The deployment's Nomos enforcement mode when the key was made.
    nomos_mode: str
    #: The active policy bundle's fingerprint, or None when none is active.
    policy_fingerprint: Optional[str]

    @property
    def reuse_needs_continuation(self) -> bool:
        """Under audit and enforce only a continuation page reuses an entry."""
        return self.nomos_mode != "off"


def _active_policy_fingerprint(conn: psycopg.Connection, deployment_name: str) -> Optional[str]:
    """The active bundle's fingerprint, read now (never from a memo)."""
    from okg.substrate.nomos.read_filter import _active_policy_fingerprint_now

    return _active_policy_fingerprint_now(
        conn, deployment_name=deployment_name, scope=f"deployment:{deployment_name}",
    )


def cache_key(
    conn: psycopg.Connection,
    session: Any,
    node_id: str,
    *,
    classify_trust: bool,
    can_continue: bool = True,
) -> Optional[CacheKey]:
    """The key for this read, or None when the read may not use the cache.

    Fail closed: a reader the key cannot name exactly gets None. Under Nomos
    only a continuation page reuses an entry, so a batch read
    (``can_continue=False``: it binds no text cursor and re-prepares each
    source on every batch page) gets None before the policy fingerprint is
    read.
    """
    from okg.substrate.nomos.read_filter import (
        nomos_subject_from_env,
        session_read_identity,
    )
    from okg.substrate.nomos.runtime_enforcement import (
        deployment_nomos_enforcement_mode,
    )

    from okg.substrate.mcp.session import Session

    if max_entries() == 0 or not isinstance(session, Session):
        return None
    if (
        session.nomos_identity_claim is not None
        or session.nomos_access_context is not None
    ):
        return None
    subject = session.nomos_subject
    if session.nomos_multi_user and subject is None:
        # The read filter refuses this read; never key it by a fallback.
        return None
    mode = deployment_nomos_enforcement_mode(session.deployment_name)
    fingerprint: Optional[str] = None
    if mode != "off" and not can_continue:
        return None
    if mode != "off":
        fingerprint = _active_policy_fingerprint(conn, session.deployment_name)
        if subject is None:
            subject = nomos_subject_from_env(default_kind="mcp_client")
    action, surface = session_read_identity(session, action=None, surface=None)
    conn_database_oid = _database_oid(conn)
    return CacheKey(
        parts=(
            session.db_fingerprint,
            conn_database_oid,
            session.deployment_name,
            session.branch_id,
            session.branch_name,
            str(session.generation_id),
            session.catalog_version_id,
            node_id,
            bool(classify_trust),
            mode,
            fingerprint,
            subject,
            action,
            surface,
            bool(session.nomos_multi_user),
        ),
        deployment_name=session.deployment_name,
        nomos_mode=mode,
        policy_fingerprint=fingerprint,
    )


def _overlay_rows(
    conn: psycopg.Connection, kind: str, ids: list[str], *, generation_id: Any,
) -> list[tuple[Any, ...]]:
    """The current effective-attribute rows of ``ids``, one tuple per row."""
    if not ids:
        return []
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT target_id, namespace, attr_key, value_hash "
            f"FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
            f"WHERE target_kind = %s AND target_id = ANY(%s) "
            f"AND generation_id = %s",
            (kind, ids, str(generation_id)),
        )
        return [(kind, *row) for row in cur.fetchall()]


def _digest(rows: Iterable[tuple[Any, ...]]) -> str:
    # Sorted here, not by the database's collation, so a digest of the same
    # rows read in pieces (`OverlayCapture`) equals one read at once.
    digest = hashlib.sha256()
    for row in sorted(tuple(str(part) for part in row) for row in rows):
        digest.update(repr(row).encode("utf-8"))
        digest.update(b"\x00")
    return digest.hexdigest()


def overlay_digest(
    conn: psycopg.Connection,
    targets: tuple[tuple[str, str], ...],
    *,
    generation_id: Any,
) -> str:
    """A digest of the current effective-attribute rows of ``targets``."""
    rows: list[tuple[Any, ...]] = []
    for kind in ("node", "edge"):
        ids = [target_id for target_kind, target_id in targets if target_kind == kind]
        rows += _overlay_rows(conn, kind, ids, generation_id=generation_id)
    return _digest(rows)


@dataclass
class OverlayCapture:
    """The overlay rows a build is about to read, taken before it reads them.

    The build calls `before_overlay_read` with each set of ids right before
    it projects their effective attributes. The digest therefore describes
    the rows as they were when the build started reading them: a change that
    lands during the build makes the next `get` miss, never a stale hit.
    """

    generation_id: Any
    present: bool
    targets: list[tuple[str, str]] = field(default_factory=list)
    rows: list[tuple[Any, ...]] = field(default_factory=list)

    @classmethod
    def start(cls, conn: psycopg.Connection, *, generation_id: Any) -> "OverlayCapture":
        return cls(generation_id=generation_id, present=_overlay_table_exists(conn))

    def before_overlay_read(
        self, conn: psycopg.Connection, kind: str, ids: Iterable[str],
    ) -> None:
        seen = set(self.targets)
        new = []
        for target_id in ids:
            target = (kind, str(target_id))
            if target not in seen:
                seen.add(target)
                new.append(target)
        self.targets += new
        if self.present:
            self.rows += _overlay_rows(
                conn, kind, [target_id for _kind, target_id in new],
                generation_id=self.generation_id,
            )

    @property
    def digest(self) -> str:
        return _digest(self.rows) if self.present else ""


#: The capture of the build running in this context, if it may be cached.
current_capture: ContextVar[Optional[OverlayCapture]] = ContextVar(
    "okg_record_text_overlay_capture", default=None,
)


def _overlay_table_exists(conn: psycopg.Connection) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (ATTRIBUTE_EFFECTIVE_TABLE,))
        row = cur.fetchone()
    return bool(row and row[0])


def get(
    conn: psycopg.Connection, key: CacheKey, *, generation_id: Any,
) -> Optional[RecordDocument]:
    """The cached document for ``key`` if its overlay rows are unchanged."""
    with _lock:
        entry = _entries.get(key)
        if entry is not None:
            _entries.move_to_end(key)
    if entry is None:
        return None
    current = (
        overlay_digest(conn, entry.overlay_targets, generation_id=generation_id)
        if _overlay_table_exists(conn) else ""
    )
    if current != entry.overlay_digest:
        with _lock:
            if _entries.get(key) is entry:
                del _entries[key]
        return None
    return entry.document


def put(
    conn: psycopg.Connection,
    key: CacheKey,
    document: RecordDocument,
    *,
    overlay: OverlayCapture,
) -> bool:
    """Keep ``document`` for later pages, evicting the least recently used.

    ``overlay`` is the capture the build filled before each of its overlay
    reads; its digest, never one read after the build, is stored. Returns
    whether it was kept. Under Nomos a document whose build may have seen
    another policy than the key names is not kept.
    """
    limit = max_entries()
    size = document.text_bytes
    if limit == 0 or size > MAX_TEXT_BYTES:
        return False
    if key.nomos_mode != "off" and (
        _active_policy_fingerprint(conn, key.deployment_name) != key.policy_fingerprint
    ):
        return False
    with _lock:
        _entries.pop(key, None)
        _entries[key] = _Entry(document, tuple(overlay.targets), overlay.digest, size)
        total = sum(entry.text_bytes for entry in _entries.values())
        while _entries and (len(_entries) > limit or total > MAX_TEXT_BYTES):
            _old_key, old = _entries.popitem(last=False)
            total -= old.text_bytes
    return True


def clear() -> None:
    """Drop every entry (tests)."""
    with _lock:
        _entries.clear()
