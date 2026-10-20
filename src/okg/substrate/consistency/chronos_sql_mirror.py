"""The post-publish SQL mirror and the generation checkout both check paths use.

Post-publish consistency checks judge ONE published generation. Two things
read the graph for them:

* IR checks query a pinned Chronos checkout session directly; and
* SQL checks that declare a scope frontier (or name the mirror tables) query
  ``okg.chronos_graph_nodes_current`` / ``okg.chronos_graph_edges_current``,
  a PostgreSQL copy of the graph.

Both must read the published generation's checkpoint, never the live branch
head: the head can already hold later writes, and a check that read them
would record findings against a generation that does not contain them.
:func:`checkout_published_generation` is the single way both paths open that
checkpoint. It raises :class:`PostPublishGenerationUnresolved` for an id that
does not resolve; it never falls back to the head.

The mirror is ONE copy per database, shared by the publisher's live
post-publish loop and ``consistency-rerun``
(:func:`okg.substrate.consistency.sql_gate.run_post_publish_consistency_checks`).
Both wrap each SQL check that reads the copy in
:meth:`ChronosSqlMirrorReads.reading`, which takes a session advisory lock
(with a bounded wait), refreshes the copy when needed, and holds the lock
while that one check reads -- so neither path can overwrite the copy while
the other's check is reading it. The refresh stamps the copy with the
checkpoint id it holds (a table comment, written in the refresh transaction),
and every such check verifies that stamp against the generation it is
judging. A lock wait that runs out, a failed refresh, or a wrong stamp is
that check's error; it never aborts a publish.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg import pq, sql
from psycopg.types.json import Jsonb

from okg.substrate.errors import (
    ChronosSqlMirrorMismatch,
    PostPublishGenerationUnresolved,
)

log = logging.getLogger(__name__)

_MIRROR_TABLES = (
    "chronos_graph_nodes_current",
    "chronos_graph_edges_current",
)

#: Session advisory lock (two-key form) that serializes every refresh of the
#: mirror together with the SQL checks that read it.
MIRROR_LOCK_NAMESPACE = "okg.chronos_sql_mirror"
MIRROR_LOCK_KEY = "refresh_and_read"

_MIRROR_STAMP_SCHEMA = "okg.chronos_sql_mirror.v1"


def _unresolved(
    purpose: str,
    generation_id: int | str | None,
    detail: str,
    *,
    error_type: str | None,
) -> PostPublishGenerationUnresolved:
    return PostPublishGenerationUnresolved(
        f"{purpose}: published generation {generation_id!r} {detail}; "
        "post-publish checks never read the live branch head",
        data={
            "generation_id": None if generation_id is None else str(generation_id),
            "purpose": purpose,
            "error_type": error_type,
        },
        fix_hint=(
            "pass the checkpoint id of a retained published generation "
            "(okg generation list)"
        ),
    )


def checkout_published_generation(
    store: Any,
    generation_id: int | str | None,
    *,
    purpose: str,
    deployment_name: str | None = None,
    branch: str = "default",
    expected_target_generation_id: int | None = None,
) -> Any:
    """Open the published Chronos checkpoint ``generation_id`` names, or raise.

    ``generation_id`` is a checkpoint id or the compat integer id stamped in
    the checkpoint's metadata; ``store.resolve_generation`` (the shared
    resolver) maps either to exactly one PUBLISHED checkpoint. ``None`` and
    ``0`` are refused before any lookup: ``checkout_generation`` maps ``0`` to
    the live branch head for pre-checkpoint stages, which is exactly the read
    a post-publish check must not make. Anything that does not resolve raises
    :class:`PostPublishGenerationUnresolved`.

    ``expected_target_generation_id``, when given, is the compat id the
    caller's records key on; the checkpoint's metadata must carry the same
    ``target_generation_id``, so the read and the records cannot name
    different generations.
    """
    checkpoint = resolve_published_checkpoint(
        store,
        generation_id,
        purpose=purpose,
        deployment_name=deployment_name,
        branch=branch,
        expected_target_generation_id=expected_target_generation_id,
    )
    try:
        return store.checkout_generation(checkpoint.checkpoint_id)
    except Exception as exc:
        raise _unresolved(
            purpose,
            generation_id,
            f"could not be opened ({type(exc).__name__}: {exc})",
            error_type=type(exc).__name__,
        ) from exc


def resolve_published_checkpoint(
    store: Any,
    generation_id: int | str | None,
    *,
    purpose: str,
    deployment_name: str | None = None,
    branch: str = "default",
    expected_target_generation_id: int | None = None,
) -> Any:
    """Resolve ``generation_id`` to exactly one published checkpoint, or raise.

    See :func:`checkout_published_generation` for the accepted ids and the
    ``expected_target_generation_id`` cross-check.
    """
    if generation_id is None or str(generation_id) == "0":
        raise _unresolved(
            purpose, generation_id, "is not a checkpoint", error_type=None
        )
    try:
        checkpoint = store.resolve_generation(
            generation_id,
            deployment_name=deployment_name,
            branch=branch,
            status="published",
        )
    except Exception as exc:
        raise _unresolved(
            purpose,
            generation_id,
            f"could not be resolved ({type(exc).__name__}: {exc})",
            error_type=type(exc).__name__,
        ) from exc
    if checkpoint is None:
        raise _unresolved(
            purpose,
            generation_id,
            "matches no published checkpoint"
            + (f" of deployment {deployment_name!r}" if deployment_name else ""),
            error_type=None,
        )
    if expected_target_generation_id is not None:
        recorded = dict(getattr(checkpoint, "metadata", None) or {}).get(
            "target_generation_id"
        )
        if recorded is None or str(recorded) != str(expected_target_generation_id):
            raise _unresolved(
                purpose,
                generation_id,
                f"records target_generation_id {recorded!r}, not the "
                f"{expected_target_generation_id!r} this run is keyed on",
                error_type=None,
            )
    return checkpoint


def release_generation_session(session: Any) -> None:
    """Release the history read lease a generation checkout took, if any."""
    from okg.substrate.history_read_guard import LeasedGenerationSession

    if isinstance(session, LeasedGenerationSession):
        session.close()


def post_publish_checks_need_chronos_sql_mirror(checks: Iterable[Any]) -> bool:
    """True when any check reads the Chronos SQL mirror tables."""
    for check in checks:
        spec = getattr(check, "spec", None)
        if getattr(spec, "scope_frontier", None) is not None:
            return True
        candidate_sql = str(getattr(spec, "candidate_sql", ""))
        coverage_sql = str(getattr(spec, "coverage_sql", ""))
        if any(
            table in candidate_sql or table in coverage_sql
            for table in _MIRROR_TABLES
        ):
            return True
    return False


_NODE_COLUMNS = (
    "node_id", "subtype", "attrs", "text_hash", "embedding_model", "source",
    "source_record_id", "source_revision", "run_id", "observed_at",
)
_EDGE_COLUMNS = (
    "edge_id", "src", "dst", "edge_type", "attrs", "provenance", "confidence",
    "source", "source_record_id", "source_revision", "run_id", "observed_at",
)


def _ensure_mirror_tables(cur: psycopg.Cursor[Any]) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS okg.chronos_graph_nodes_current (
          node_id TEXT PRIMARY KEY,
          subtype TEXT NOT NULL,
          attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
          text_hash TEXT,
          embedding_model TEXT,
          source TEXT,
          source_record_id JSONB NOT NULL DEFAULT '{}'::jsonb,
          source_revision JSONB NOT NULL DEFAULT '{}'::jsonb,
          run_id TEXT,
          observed_at TIMESTAMPTZ
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS okg.chronos_graph_edges_current (
          edge_id BIGINT PRIMARY KEY,
          src TEXT NOT NULL,
          dst TEXT NOT NULL,
          edge_type TEXT NOT NULL,
          attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
          provenance TEXT NOT NULL DEFAULT 'explicit',
          confidence REAL NOT NULL DEFAULT 1.0,
          source TEXT,
          source_record_id JSONB NOT NULL DEFAULT '{}'::jsonb,
          source_revision JSONB NOT NULL DEFAULT '{}'::jsonb,
          run_id TEXT,
          observed_at TIMESTAMPTZ
        )
        """
    )
    for statement in (
        "CREATE INDEX IF NOT EXISTS chronos_graph_nodes_current_subtype_idx "
        "ON okg.chronos_graph_nodes_current (subtype)",
        "CREATE INDEX IF NOT EXISTS chronos_graph_nodes_current_source_family_idx "
        "ON okg.chronos_graph_nodes_current ((attrs->>'source_family'))",
        "CREATE INDEX IF NOT EXISTS chronos_graph_nodes_current_source_idx "
        "ON okg.chronos_graph_nodes_current (source)",
        "CREATE INDEX IF NOT EXISTS chronos_graph_edges_current_src_type_idx "
        "ON okg.chronos_graph_edges_current (src, edge_type)",
        "CREATE INDEX IF NOT EXISTS chronos_graph_edges_current_dst_type_idx "
        "ON okg.chronos_graph_edges_current (dst, edge_type)",
        "CREATE INDEX IF NOT EXISTS chronos_graph_edges_current_src_idx "
        "ON okg.chronos_graph_edges_current (src)",
        "CREATE INDEX IF NOT EXISTS chronos_graph_edges_current_dst_idx "
        "ON okg.chronos_graph_edges_current (dst)",
    ):
        cur.execute(statement)


def _node_row(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        str(row["node_id"]),
        str(row["subtype"]),
        Jsonb(dict(row.get("attrs") or {})),
        row.get("text_hash"),
        row.get("embedding_model"),
        row.get("source"),
        Jsonb(dict(row.get("source_record_id") or {})),
        Jsonb(dict(row.get("source_revision") or {})),
        row.get("run_id"),
        row.get("observed_at"),
    )


def _edge_row(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(row["edge_id"]),
        str(row["src"]),
        str(row["dst"]),
        str(row["edge_type"]),
        Jsonb(dict(row.get("attrs") or {})),
        str(row.get("provenance") or "explicit"),
        float(row.get("confidence", 1.0)),
        row.get("source"),
        Jsonb(dict(row.get("source_record_id") or {})),
        Jsonb(dict(row.get("source_revision") or {})),
        row.get("run_id"),
        row.get("observed_at"),
    )


def refresh_chronos_sql_mirror(
    conn: psycopg.Connection[Any],
    *,
    store: Any,
    generation_id: int | str,
    deployment_name: str,
    branch: str = "default",
) -> str:
    """Replace the Chronos SQL mirror with the rows of ``generation_id``.

    Nodes and edges are read from the SAME checkpoint session, so they
    describe one snapshot (a checkpoint does not move). The replacement --
    truncate, bulk ``COPY``, and the stamp naming the checkpoint -- runs in
    one transaction, so a reader sees either the old copy or the new one,
    never a truncated or half-filled table. Returns the checkpoint id the
    copy now holds.

    Callers that then run SQL checks must hold the mirror lock for the whole
    refresh-and-read span; :func:`chronos_sql_mirror_for_checks` does that.
    """
    purpose = "post-publish SQL mirror refresh"
    checkpoint_id = str(
        resolve_published_checkpoint(
            store,
            generation_id,
            purpose=purpose,
            deployment_name=deployment_name,
            branch=branch,
        ).checkpoint_id
    )
    session = checkout_published_generation(
        store,
        checkpoint_id,
        purpose=purpose,
        deployment_name=deployment_name,
        branch=branch,
    )
    try:
        node_rows = session.query(
            f"SELECT {', '.join(_NODE_COLUMNS)} FROM okg.graph_nodes"
        )
        edge_rows = session.query(
            f"SELECT {', '.join(_EDGE_COLUMNS)} FROM okg.graph_edges"
        )
    finally:
        release_generation_session(session)
    stamp = json.dumps(
        {"schema": _MIRROR_STAMP_SCHEMA, "checkpoint_id": checkpoint_id},
        sort_keys=True,
    )
    with conn.transaction():
        with conn.cursor() as cur:
            _ensure_mirror_tables(cur)
            cur.execute(
                "TRUNCATE okg.chronos_graph_edges_current, "
                "okg.chronos_graph_nodes_current"
            )
            with cur.copy(
                "COPY okg.chronos_graph_nodes_current (node_id, subtype, "
                "attrs, text_hash, embedding_model, source, source_record_id, "
                "source_revision, run_id, observed_at) FROM STDIN"
            ) as copy:
                for row in node_rows:
                    copy.write_row(_node_row(row))
            with cur.copy(
                "COPY okg.chronos_graph_edges_current (edge_id, src, dst, "
                "edge_type, attrs, provenance, confidence, source, "
                "source_record_id, source_revision, run_id, observed_at) "
                "FROM STDIN"
            ) as copy:
                for row in edge_rows:
                    copy.write_row(_edge_row(row))
            # COMMENT takes no bind parameters; the stamp is JSON we built.
            cur.execute(
                sql.SQL(
                    "COMMENT ON TABLE okg.chronos_graph_nodes_current IS {}"
                ).format(sql.Literal(stamp))
            )
            cur.execute("ANALYZE okg.chronos_graph_nodes_current")
            cur.execute("ANALYZE okg.chronos_graph_edges_current")
    return checkpoint_id


def chronos_sql_mirror_generation(conn: psycopg.Connection[Any]) -> str | None:
    """The checkpoint id the mirror holds, from its refresh stamp (or None)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT obj_description(
                     to_regclass('okg.chronos_graph_nodes_current'), 'pg_class'
                   )
            """
        )
        row = cur.fetchone()
    text = row[0] if row else None
    if not text:
        return None
    try:
        stamp = json.loads(text)
    except ValueError:
        return None
    if not isinstance(stamp, dict) or stamp.get("schema") != _MIRROR_STAMP_SCHEMA:
        return None
    value = stamp.get("checkpoint_id")
    return str(value) if value else None


#: How long a mirror-reading check waits for another publish or rerun to
#: finish with the mirror before it reports the wait as its error.
MIRROR_LOCK_WAIT_SECONDS = 120.0
#: Extra statement time beyond the lock wait, so the lock wait -- not the
#: statement timeout -- is what ends a long wait.
_STATEMENT_TIMEOUT_MARGIN_MS = 5000


def _mirror_error(
    message: str,
    *,
    expected: str,
    held: str | None,
    reason: str,
    fix_hint: str,
) -> ChronosSqlMirrorMismatch:
    return ChronosSqlMirrorMismatch(
        message,
        data={
            "expected_checkpoint_id": expected,
            "held_checkpoint_id": held,
            "reason": reason,
        },
        fix_hint=fix_hint,
    )


def _acquire_lock(
    conn: psycopg.Connection[Any], *, wait_seconds: float, expected: str
) -> None:
    """Take the mirror lock, queueing behind earlier waiters for at most
    ``wait_seconds``.

    A blocking ``pg_advisory_lock`` joins PostgreSQL's lock queue, so waiters
    are served in order: a rerun that releases and re-requests the lock
    between back-to-back checks goes behind a publish check already waiting,
    instead of winning every race against a poller. The wait is bounded by a
    transaction-local ``lock_timeout`` (with ``statement_timeout`` raised just
    above it for this one statement), and runs in its own short transaction
    so the connection is not left idle in a transaction; an enclosing
    transaction gets its previous settings back. A wait that runs out is this
    check's reported error, never a cancelled publish.
    """
    wait_ms = max(1, int(wait_seconds * 1000))
    locked = False
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT current_setting('lock_timeout'), "
                    "current_setting('statement_timeout')"
                )
                previous = cur.fetchone()
                cur.execute(
                    "SELECT set_config('lock_timeout', %s, true), "
                    "set_config('statement_timeout', %s, true)",
                    (f"{wait_ms}ms", f"{wait_ms + _STATEMENT_TIMEOUT_MARGIN_MS}ms"),
                )
                cur.execute(
                    "SELECT pg_advisory_lock(hashtext(%s), hashtext(%s))",
                    (MIRROR_LOCK_NAMESPACE, MIRROR_LOCK_KEY),
                )
                locked = True
                cur.execute(
                    "SELECT set_config('lock_timeout', %s, true), "
                    "set_config('statement_timeout', %s, true)",
                    (previous[0], previous[1]),
                )
    except BaseException as exc:
        if locked:
            # The session lock survives a rollback: release it before the
            # error leaves, or it stays held for the life of the connection.
            _unlock_after_failed_acquire(conn, exc)
            raise
        if not isinstance(exc, psycopg.errors.LockNotAvailable):
            raise
        raise _mirror_error(
            "another publish or consistency rerun held the Chronos SQL "
            f"mirror for more than {wait_seconds:g}s; the SQL check was "
            "not run",
            expected=expected,
            held=None,
            reason="lock_wait_timeout",
            fix_hint=(
                "rerun the check once the other run finishes: okg "
                "consistency-rerun --generation <id>"
            ),
        ) from exc


def _unlock_after_failed_acquire(
    conn: psycopg.Connection[Any], error: BaseException
) -> None:
    """Release a lock granted before ``_acquire_lock`` failed. When the
    unlock fails too (a dead connection, whose lock the server frees on
    disconnect), the caller re-raises ``error``; the unlock failure is logged
    and noted on ``error``."""
    try:
        _release_lock(conn)
    except Exception as unlock_exc:
        log.warning(
            "could not release the Chronos SQL mirror lock after a failed "
            "acquire (%s: %s); raising the original error",
            type(unlock_exc).__name__,
            unlock_exc,
        )
        error.add_note(
            "mirror lock release after this error also failed: "
            f"{type(unlock_exc).__name__}: {unlock_exc}"
        )


def _release_lock(conn: psycopg.Connection[Any]) -> None:
    if conn.info.transaction_status == pq.TransactionStatus.INERROR:
        # A failed transaction can never commit; roll it back so the unlock
        # can run. Leaving the lock held would block every later refresh on
        # this database for the life of the connection.
        conn.rollback()
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_unlock(hashtext(%s), hashtext(%s))",
                (MIRROR_LOCK_NAMESPACE, MIRROR_LOCK_KEY),
            )


class ChronosSqlMirrorReads:
    """Per-check access to the shared mirror for one generation's checks.

    ``reading(check)`` wraps one check's evaluation. For a check that does not
    read the mirror it does nothing. For a mirror reader it takes the mirror
    lock (a queued wait with a bounded timeout), refreshes the copy from this
    generation when its stamp names anything else, verifies the stamp, runs the check, and releases the lock. The lock is
    therefore held only while a mirror-reading check runs -- never through
    other checks -- and every failure (lock wait, refresh, stamp) raises
    :class:`ChronosSqlMirrorMismatch` inside that check, where the caller's
    per-check error handling reports it; it never aborts the publish.

    ``refresh=False`` (the publisher's ``sync_chronos_graph`` off) never
    rewrites the copy, but still refuses a copy stamped with another
    generation.
    """

    def __init__(
        self,
        conn: psycopg.Connection[Any],
        checks: Iterable[Any],
        *,
        store: Any,
        generation_id: int | str,
        deployment_name: str,
        branch: str = "default",
        refresh: bool = True,
        lock_wait_seconds: float | None = None,
    ) -> None:
        self._conn = conn
        self._readers = {
            id(check)
            for check in checks
            if post_publish_checks_need_chronos_sql_mirror([check])
        }
        self._store = store
        self._generation_id = generation_id
        self._deployment_name = deployment_name
        self._branch = branch
        self._refresh = refresh
        self._lock_wait_seconds = (
            MIRROR_LOCK_WAIT_SECONDS
            if lock_wait_seconds is None
            else lock_wait_seconds
        )
        self._checkpoint_id: str | None = None

    def _expected_checkpoint(self) -> str:
        if self._checkpoint_id is None:
            self._checkpoint_id = str(
                resolve_published_checkpoint(
                    self._store,
                    self._generation_id,
                    purpose="post-publish SQL mirror check",
                    deployment_name=self._deployment_name,
                    branch=self._branch,
                ).checkpoint_id
            )
        return self._checkpoint_id

    def _prepare(self) -> None:
        """Under the lock: make the copy hold this generation, or raise."""
        expected = self._expected_checkpoint()
        # A checkpoint never changes, so a copy already stamped with it
        # holds it; re-copy only when the stamp names something else.
        if (
            self._refresh
            and chronos_sql_mirror_generation(self._conn) != expected
        ):
            try:
                refresh_chronos_sql_mirror(
                    self._conn,
                    store=self._store,
                    generation_id=expected,
                    deployment_name=self._deployment_name,
                    branch=self._branch,
                )
            except Exception as exc:
                log.exception(
                    "post-publish SQL mirror refresh failed for generation %s",
                    expected,
                )
                raise _mirror_error(
                    "the Chronos SQL mirror refresh for generation "
                    f"{expected!r} failed ({type(exc).__name__}: {exc}); the "
                    "SQL check was not run",
                    expected=expected,
                    held=None,
                    reason="refresh_failed",
                    fix_hint=(
                        "fix the refresh error, then rerun with "
                        "okg consistency-rerun --generation <id>"
                    ),
                ) from exc
        assert_chronos_sql_mirror_generation(self._conn, expected)

    @contextmanager
    def reading(self, check: Any) -> Iterator[None]:
        if id(check) not in self._readers:
            yield
            return
        # Resolve before taking the lock: an unresolvable generation is this
        # check's error and needs no lock.
        try:
            expected = self._expected_checkpoint()
        except PostPublishGenerationUnresolved as exc:
            raise _mirror_error(
                f"the Chronos SQL mirror cannot be checked: {exc.message}",
                expected=str(self._generation_id),
                held=None,
                reason="generation_unresolved",
                fix_hint="pass a retained published generation",
            ) from exc
        _acquire_lock(
            self._conn, wait_seconds=self._lock_wait_seconds, expected=expected
        )
        try:
            self._prepare()
            yield
        finally:
            _release_lock(self._conn)


def assert_chronos_sql_mirror_generation(
    conn: psycopg.Connection[Any],
    checkpoint_id: str,
) -> None:
    """Refuse when the mirror does not hold ``checkpoint_id``."""
    held = chronos_sql_mirror_generation(conn)
    if held != str(checkpoint_id):
        raise _mirror_error(
            "the Chronos SQL mirror holds generation "
            f"{held!r}, not {str(checkpoint_id)!r}; the SQL check would judge "
            "the wrong generation, so it was not run",
            expected=str(checkpoint_id),
            held=held,
            reason="stamp_mismatch",
            fix_hint="rerun the check with okg consistency-rerun --generation <id>",
        )


@contextmanager
def chronos_sql_mirror_for_checks(
    conn: psycopg.Connection[Any],
    checks: Iterable[Any],
    *,
    store: Any,
    generation_id: int | str,
    deployment_name: str,
    branch: str = "default",
    refresh: bool = True,
    lock_wait_seconds: float | None = None,
) -> Iterator[ChronosSqlMirrorReads]:
    """Yield the :class:`ChronosSqlMirrorReads` both check paths share.

    Wrap each check's evaluation in ``mirror.reading(check)``. Nothing is
    locked or refreshed until the first mirror-reading check runs.
    """
    yield ChronosSqlMirrorReads(
        conn,
        checks,
        store=store,
        generation_id=generation_id,
        deployment_name=deployment_name,
        branch=branch,
        refresh=refresh,
        lock_wait_seconds=lock_wait_seconds,
    )
