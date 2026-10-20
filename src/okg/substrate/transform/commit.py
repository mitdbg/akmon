"""Transform program persistence for witness-stamped writers.

Logical transform programs do not commit directly. Production writers stamp
source_program_checksum and persist the producing TransformProgram into the shared
IR ledger so the database-side witness checks can resolve provenance.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from .ir import TransformProgram

#: The setting the okg.{node,edge}_facts.source_program_checksum column default
#: reads (db/schema.sql). A writer that omits the column gets this value.
COMMIT_WITNESS_SETTING = "okg.source_program_checksum"


def persist_transform_program(
    conn, program: TransformProgram, *, deployment_name: str = "test"
) -> str:
    """Persist a content-addressed TransformProgram into the shared IR ledger
    (program_kind='transform') and return its program_id (req.gtir.ledger-reuse)."""
    op_counts: dict[str, int] = {}
    for _block, binding in program.iter_ops():
        op_counts[binding.op.kind.value] = op_counts.get(binding.op.kind.value, 0) + 1
    with conn.cursor() as cur:
        # ON CONFLICT DO NOTHING (not DO UPDATE): re-persisting a content-addressed
        # program is a no-op, and DO NOTHING needs only INSERT -- not UPDATE -- so the
        # runtime role (app_rw) can ledger its witness under an append-only grant
        # (seal-graph-writes keeps the ledger UPDATE-free for the runtime role). On
        # conflict no row is returned, so resolve the existing program_id by checksum.
        cur.execute(
            """
            INSERT INTO okg.ir_programs (
              program_checksum, ir_version, program_kind, effect_summary,
              determinism, op_counts, function_counts, cardinality_summary,
              payload_mode, program_json, program_ref
            ) VALUES (
              %s, %s, 'transform', %s::jsonb, %s, %s::jsonb, '{}'::jsonb,
              '{}'::jsonb, 'full', %s::jsonb, %s
            )
            ON CONFLICT (program_checksum) DO NOTHING
            RETURNING program_id
            """,
            (
                program.checksum(),
                "okg-transform-ir-v1",
                Jsonb({"effect": program.effect.value}),
                program.determinism.value,
                Jsonb(op_counts),
                Jsonb(program.to_dict()),
                program.program_id,
            ),
        )
        row = cur.fetchone()
        if row is not None:
            return str(row[0])
        cur.execute(
            "SELECT program_id FROM okg.ir_programs WHERE program_checksum = %s",
            (program.checksum(),),
        )
        return str(cur.fetchone()[0])


def _autocommit(conn) -> bool:
    # A connection without the attribute (a test double) is treated like the
    # publisher's autocommit connection.
    return bool(getattr(conn, "autocommit", True))


def _transaction_status(conn):
    return getattr(getattr(conn, "info", None), "transaction_status", None)


def _session_scope(conn) -> bool:
    """True when a SET on ``conn`` commits at once: autocommit, and no
    explicit transaction open."""
    return _autocommit(conn) and _transaction_status(conn) in (
        None,
        TransactionStatus.IDLE,
    )


def pin_commit_witness(conn, checksum: str) -> None:
    """Stamp later fact INSERTs on ``conn`` that omit the column with ``checksum``.

    The scope follows the connection's state (issue #2965):

    * Autocommit with no transaction open (the shipped publish and worker
      path): session scope, ``set_config(..., false)``. Every statement is its
      own transaction, so a transaction-scoped value was gone before the next
      INSERT. The SET commits at once, so no later rollback can undo it.
    * Inside a transaction (``Publisher.cycle(conn=...)``, merge-back, the
      enricher backfill, or an explicit BEGIN on an autocommit connection):
      transaction scope, ``set_config(..., true)``. A session SET made inside
      a transaction is undone by a rollback, and so is a later clear -- which
      brought a committed stamp back and let an unrelated write that named no
      program pass as enrichment. A transaction-scoped value ends with its
      transaction, whether it commits or rolls back. A commit or rollback
      inside the writer ends the stamp early and later INSERTs are refused
      (fail closed); callers re-pin before each writer, and
      ``library.linkers._chronos.query`` restores it after a checkout read.
    """
    if not checksum:
        raise ValueError("commit witness checksum must be non-empty")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT set_config(%s, %s, %s)",
            (COMMIT_WITNESS_SETTING, checksum, not _session_scope(conn)),
        )


def release_commit_witness(conn, *, session_scoped: bool | None = None) -> bool:
    """Clear the commit-witness checksum set by :func:`pin_commit_witness`.

    ``session_scoped`` says how the stamp was pinned; by default it is read
    from the connection's current state. Returns True when the clear is
    durable, False when it could only be made inside an open transaction.

    * A transaction-scoped stamp is cleared for the rest of its transaction.
      If that transaction has failed, nothing runs: the stamp ends when the
      failed transaction rolls back.
    * A session-scoped stamp was committed, so no rollback removes it; it is
      cleared outside any transaction. A failed transaction on the connection
      can only roll back, so it is rolled back first. A transaction that is
      open and has not failed is not ended here: the clear runs inside it and
      the function returns False, because rolling that transaction back would
      bring the stamp back.
    """
    if getattr(conn, "closed", False):
        return True
    if session_scoped is None:
        session_scoped = _session_scope(conn)
    status = _transaction_status(conn)
    if not session_scoped:
        if status == TransactionStatus.INERROR:
            return True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config(%s, '', true)", (COMMIT_WITNESS_SETTING,),
            )
        return True
    if status == TransactionStatus.INERROR:
        conn.rollback()
        status = _transaction_status(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT set_config(%s, '', false)", (COMMIT_WITNESS_SETTING,))
    if status == TransactionStatus.INTRANS:
        return False
    if not _autocommit(conn):
        conn.commit()
    return True


def clear_commit_witness_on_return(conn) -> None:
    """Pool reset hook: a returned connection carries no commit witness.

    psycopg_pool calls this with the connection idle (outside any
    transaction). A stamp a borrower left behind -- a writer that pinned and
    never released -- must not reach the next borrower, whose unrelated
    writes would then pass as that program's. The clear commits its own
    transaction.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT set_config(%s, '', false)", (COMMIT_WITNESS_SETTING,))
    if not conn.autocommit:
        conn.commit()


@contextmanager
def commit_witness_session(conn, checksum: str) -> Iterator[str]:
    """Stamp every fact written on ``conn`` inside the block with ``checksum``.

    The one mechanism a publish stage uses so that every enricher, linker and
    extractor INSERT that omits ``source_program_checksum`` is witnessed.
    Session scope on an idle autocommit connection, transaction scope
    otherwise (see :func:`pin_commit_witness`). The clear runs on every exit;
    a session stamp is cleared outside any transaction (see
    :func:`release_commit_witness`). The program behind ``checksum`` must
    already be in okg.ir_programs (:func:`persist_transform_program`).

    Raises RuntimeError, after clearing the stamp, when the block left the
    connection in a state where a session stamp cannot be cleared the normal
    way: autocommit turned off, or an explicit transaction left open.
    """
    session_scoped = _session_scope(conn)
    pin_commit_witness(conn, checksum)
    try:
        yield checksum
    except BaseException:
        release_commit_witness(conn, session_scoped=session_scoped)
        raise
    mode_changed = session_scoped and not _autocommit(conn)
    durable = release_commit_witness(conn, session_scoped=session_scoped)
    if mode_changed:
        raise RuntimeError(
            "commit witness session began on an autocommit connection that "
            "left autocommit before the session ended; the stamp was cleared"
            + ("" if durable else " only inside the open transaction")
        )
    if not durable:
        raise RuntimeError(
            "commit witness session on an autocommit connection ended inside "
            "an explicit transaction; the stamp was cleared only inside it, "
            "and a rollback would bring it back"
        )


__all__ = [
    "COMMIT_WITNESS_SETTING",
    "clear_commit_witness_on_return",
    "commit_witness_session",
    "persist_transform_program",
    "pin_commit_witness",
    "release_commit_witness",
]
