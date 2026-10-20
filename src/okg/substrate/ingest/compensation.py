"""Undo one source run's durable graph writes and application receipts.

Issue #1431: the streaming runner commits a chunk every 100 progress markers
or 10,000 facts, so a run that fails partway — a forbidden retraction, an
adapter error, a rejected admission — has already applied earlier chunks.
This module owns the single compensation used by BOTH the orchestrated ingest
service and `SourceRunner` itself, so a run started outside the orchestrator
(deployment bootstrap, a direct runner caller) is not left half-applied.

The compensation is idempotent: the graph retraction is re-derived from the
rows the run still has applied, so a pass that finds none retracts nothing,
and a receipt is only carried to `compensated` in the same transaction that
removes those rows.

Both paths build the graph store with ``autocommit=False`` (#2152). A caller
that passes no ``graph_conn`` used to get the store default of
``autocommit=True``, which committed the retraction on its own and left the
receipt transition in a separate transaction.

The run's rows are read and retracted a bounded page at a time. A refused
code-structure run of about 1.5 million rows used to load every row into one
list and retract it in one call: the process grew from 3.3 to 28.7 GB, and
the transaction sat idle between statements for up to 93 seconds while Python
prepared that one call, which the 5-minute idle-in-transaction bound can end.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Iterator

from psycopg.rows import dict_row
from okg.substrate.db.connection import connect as db_connect
from okg.substrate.errors import CompensationRowSetChanged, SourceRunUndoTooLarge
from okg.substrate.ingest.run_base import undo_resolve_hint
from okg.substrate.library.sources.base import EdgeFact, NodeFact

#: The most keys written since a run base that one undo may restore while
#: holding the branch write lock (issue #2993 review). Measured on a build host:
#: 20,000 keys 10.4 s, 100,000 keys 89.7 s. Override with the environment
#: variable below; the refusal names it.
RUN_BASE_UNDO_MAX_KEYS = 100_000
UNDO_MAX_KEYS_ENV = "OKG_RUN_BASE_UNDO_MAX_KEYS"


def run_base_undo_max_keys() -> int:
    import os

    raw = os.environ.get(UNDO_MAX_KEYS_ENV)
    if raw is None or not raw.strip():
        return RUN_BASE_UNDO_MAX_KEYS
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{UNDO_MAX_KEYS_ENV} must be a positive integer, got {raw!r}"
        ) from exc
    if value < 1:
        raise ValueError(
            f"{UNDO_MAX_KEYS_ENV} must be a positive integer, got {raw!r}"
        )
    return value


#: Rows read and retracted per page. It matches the runner's own chunk of
#: 10,000 facts, so no single retraction call is larger than a chunk the run
#: itself applied.
RETRACTION_PAGE_ROWS = 10_000

log = logging.getLogger(__name__)


def retract_chronos_run(
    dsn: str,
    *,
    admission_id: str | None,
    source: str,
    run_id: str,
    branch: str = "default",
    graph_conn: Any | None = None,
    page_rows: int = RETRACTION_PAGE_ROWS,
) -> dict[str, Any]:
    """Compensate one rejected run or leave durable blocking evidence.

    Issue #2993: when the run took a run-base checkpoint (a ``SourceRunner``
    does at run start), compensation is an UNDO: every key the run touched
    is written back to its run-base version as an ordinary write on the
    branch, and the run base is deleted in the same transaction. A key
    another writer changed after the run base is not overwritten: the undo
    refuses (``CompensationRowSetChanged``), rolls back, and leaves the
    receipts ``compensating`` and the run base marked undo-pending, on which
    the publisher refuses every publish of the deployment. With no run base
    it falls back, with a logged warning, to retracting the run's own rows
    (``_retract_run_rows``). Returns the undo's counts and timings.

    ``graph_conn`` is the caller's own connection when the caller already
    holds the source-admission fence (the branch advisory lock) inside an
    open transaction: the graph retraction then runs on that same session,
    where the lock is reentrant, and commits with the caller's transaction.
    Opening a second connection there would wait on the caller's own fence
    until ``lock_timeout``.

    Issue #2152: the retraction and the terminal ``compensated`` transition
    run on one connection --- ``effect_conn``, the caller's when
    ``graph_conn`` is given and this routine's own when it is not. That
    shared connection, not the ``transaction()`` wrapper around the pair, is
    what makes the two halves share a fate; the wrapper only gives them a
    clean rollback boundary. A rollback can therefore never leave a receipt
    claiming rows that are still applied. Only the in-flight
    ``compensating`` marker is written and committed separately, on this
    routine's own connection, so it is durable before the retraction starts
    and survives a crash inside it.

    The 30-second ``lock_timeout`` is set twice: once on this routine's own
    connection, and again as ``SET LOCAL`` inside the effect transaction,
    because a caller-supplied ``graph_conn`` comes from the pool with no
    bound of its own. No test asserts the ``SET LOCAL`` directly; every
    compensation the recovery tests drive executes it.

    The retraction reads and applies ``page_rows`` rows at a time, and every
    page runs inside that same one transaction: nothing commits until the
    whole run is retracted and its receipts are ``compensated``. Committing
    page by page would break the shared fate above, and a caller that passes
    ``graph_conn`` owns its transaction, so this routine cannot commit it.
    The cost: the branch write lock the first page's ``apply_facts`` takes
    is held until that commit, across every later page, where the single
    call took it only for its final write phase. The run's keys are read
    before the first page, so the lock is not held during that read.
    """
    from okg.substrate.fact_application import FactBatchRecoveryBlocked
    from okg.substrate.fact_batch import PostgresApplicationReceiptStore
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest.run_base import (
        delete_run_base,
        forget_progress_after_undo,
        invalidate_checkpoint_after_undo,
        mark_undo_pending,
        run_base_checkpoint_id,
        run_base_exists,
    )

    run_base = run_base_checkpoint_id(branch, run_id)
    stats: dict[str, Any] = {"run_base": None}

    with db_connect(dsn) as conn:
        # This runs on a failure path while the caller may still hold rows in
        # the tables below. A bounded wait turns a lock conflict into a named
        # compensation failure instead of an unbounded hang (#802's lesson).
        # This statement bounds this routine's own connection only. The effect
        # transaction below repeats the bound with SET LOCAL, since a
        # caller-supplied graph_conn is pooled and carries no lock_timeout.
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '30s'")
        conn.commit()
        receipt_store = PostgresApplicationReceiptStore(conn)
        keys: list[tuple[Any, Any]] = []
        if admission_id is not None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, chunk_ordinal
                      FROM okg.fact_batch_receipts
                     WHERE admission_id = %s::uuid
                     ORDER BY chunk_ordinal
                    """,
                    (admission_id,),
                )
                keys = list(cur.fetchall())
        receipts = [
            receipt_store.get(admission_id, receipt_run_id, ordinal)
            for receipt_run_id, ordinal in keys
        ]
        if any(receipt is None for receipt in receipts):
            raise FactBatchRecoveryBlocked("compensation receipt disappeared")
        if any(receipt.state == "prepared" for receipt in receipts if receipt):
            raise FactBatchRecoveryBlocked(
                "prepared receipt must be recovered before terminal rejection"
            )
        for receipt in receipts:
            if receipt is not None and receipt.state == "applied":
                receipt_store.transition(receipt.key, "compensating")
        # Issue #2993: the run base is marked undo-pending in the same
        # commit. With the compensating receipts it is the durable "undo
        # unfinished" state the publisher refuses on; only a completed undo
        # removes it.
        has_run_base = run_base_exists(conn, run_base)
        if has_run_base:
            mark_undo_pending(conn, run_base, admission_id=admission_id)
            stats["run_base"] = run_base
        # The compensating state must survive a graph-retraction failure.
        conn.commit()
        # Only a run with chunks still to undo needs its run base. When every
        # receipt is already compensated -- a repeat or racing compensation
        # of a run whose undo finished (#2244) -- there is nothing to refuse.
        if not has_run_base and any(
            receipt is not None and receipt.state in {"applied", "compensating"}
            for receipt in receipts
        ):
            _refuse_missing_run_base(
                conn, admission_id=admission_id, source=source,
                run_id=run_id, branch=branch,
            )

        # One transaction owner for the retraction and the receipt state it
        # attests (#2152). With the caller's connection that owner is the
        # caller's open transaction and `transaction()` below is a savepoint
        # inside it; without one this routine owns the single transaction.
        effect_conn = conn if graph_conn is None else graph_conn
        store = ChronosGraphStore.from_connection(
            effect_conn, ensure_schema=False, autocommit=False,
        )
        effect_receipts = PostgresApplicationReceiptStore(effect_conn)
        graph = store.checkout(branch)
        with effect_conn.transaction():
            # effect_conn may be the caller's pooled connection, which has no
            # lock_timeout of its own; bound the FOR UPDATE that
            # transition() takes below the same way (#802).
            with effect_conn.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = '30s'")
            if has_run_base:
                # Issue #2993: put every key the run touched back to its
                # run-base version, then drop the run base with the undo.
                stats.update(undo_run_to_run_base(
                    store,
                    graph,
                    branch=branch,
                    source=source,
                    run_id=run_id,
                    checkpoint_id=run_base,
                    page_rows=page_rows,
                ))
                # Issue #3592: a checkpoint a later run saved on top of this
                # run's rows no longer matches the graph; the next run must
                # prove its record set again instead of trusting it.
                stats["checkpoint_invalidated"] = invalidate_checkpoint_after_undo(
                    effect_conn, source=source, run_id=run_id,
                    checkpoint_id=run_base,
                )
                stats["progress_keys_forgotten"] = forget_progress_after_undo(
                    effect_conn, checkpoint_id=run_base,
                )
                delete_run_base(effect_conn, run_base, force=True)
            else:
                stats.update(_retract_run_rows(
                    store, graph, branch=branch, source=source,
                    run_id=run_id, page_rows=page_rows,
                ))
            for receipt in receipts:
                if receipt is None:
                    continue
                current = effect_receipts.get(*receipt.key)
                if current is not None and current.state == "compensating":
                    effect_receipts.transition(current.key, "compensated")
    return stats


def _refuse_missing_run_base(
    conn: Any,
    *,
    admission_id: str | None,
    source: str,
    run_id: str,
    branch: str,
) -> None:
    """Refuse a run that applied chunks but has no run base to undo to.

    A run cuts its run base before its first graph write whenever its
    deployment has published on the branch (``run_base.take_run_base``). A
    run that applied chunks there and has none lost it -- it started before
    this code, or something deleted it -- and retracting its own rows would
    leave any rows it deleted or updated wrong (issue #2993 review). Refuse
    instead: the receipts stay ``compensating``, so the publisher refuses
    every publish until an operator resolves it. Before the deployment's
    first published generation no run cuts a run base, and retracting the
    run's rows remains its compensation.
    """
    from okg.substrate.errors import SourceRunBaseMissing
    from okg.substrate.ingest.run_base import branch_has_published_generation

    with conn.cursor() as cur:
        cur.execute(
            "SELECT deployment_name FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (admission_id,),
        )
        row = cur.fetchone()
    deployment = None if row is None else str(row[0])
    published = branch_has_published_generation(
        conn, branch=branch, deployment=deployment,
    )
    conn.rollback()
    if not published:
        return
    raise SourceRunBaseMissing(
        "cannot undo the failed run: it applied chunks but has no run-base "
        "checkpoint to restore",
        data={
            "source": source,
            "run_id": run_id,
            "branch": branch,
            "admission_id": admission_id,
            "deployment": deployment,
        },
        fix_hint=(
            "the failed run cannot be undone exactly because its run-base "
            "checkpoint is gone, so a retry cannot succeed and every publish "
            "and ingest of this deployment is refused. Keep the rows as they "
            "are now with `okg runtime finish-undo --deployment "
            f"{deployment or '<deployment>'} --accept-current --reason "
            "\"<why>\" --apply`, then rerun the source with `okg ingest "
            f"--deployment {deployment or '<deployment>'} --include {source}`"
        ),
    )


def _retract_run_rows(
    store: Any,
    graph: Any,
    *,
    branch: str,
    source: str,
    run_id: str,
    page_rows: int,
) -> dict[str, Any]:
    """The compensation for a run with no run base: retract its rows.

    A run has no run base when it started before this code, ran on a branch
    that did not exist yet, or is compensated by a caller that never ran a
    ``SourceRunner`` (a direct call). This is the compensation from before
    issue #2993: it deletes the rows stamped with the run id and cannot put
    back a row the run deleted or updated, so it says so in the log whenever
    it retracts anything.
    """
    retracted = 0
    # The run's keys are read before the first page is applied, so the
    # branch write lock is not held during that read; rows are then fetched
    # and retracted one bounded page at a time, inside this one transaction.
    # See iter_run_retraction_pages.
    for facts in iter_run_retraction_pages(
        graph,
        source=source,
        run_id=run_id,
        page_rows=page_rows,
    ):
        retracted += len(facts)
        store.apply_facts(
            facts,
            branch=branch,
            source=source,
            run_id=run_id,
        )
    if retracted:
        log.warning(
            "compensation of run %s (source %s, branch %s) found no run-base "
            "checkpoint; it retracted the %d rows the run wrote but cannot "
            "restore rows the run deleted or updated",
            run_id, source, branch, retracted,
        )
    # The run's declarations of shared nodes it did not own have no graph
    # row to retract; delete them, or a later hand-off would write the
    # failed run's content (issue #1901).
    store.withdraw_node_declarations(
        branch=branch,
        sources=[source],
        run_id=run_id,
    )
    # Likewise its declarations of ranked edges another producer's version
    # outranked (item 92a); a row the run owned was retracted above.
    edge_declarations = store.withdraw_edge_declarations(
        branch=branch,
        sources=[source],
        run_id=run_id,
    )
    return {
        "mode": "retract_run_rows",
        "rows_retracted": retracted,
        "edge_declarations_withdrawn": edge_declarations[
            "declarations_withdrawn"
        ],
    }


def _edge_retraction(row: Any) -> EdgeFact:
    # A retraction reads only src, edge_type and dst: apply_facts derives the
    # edge key from them and ignores every other field of an "R" edge fact
    # (graph_store_internal/writes.py, the EdgeFact branch of its fact loop).
    return EdgeFact(
        src=str(row["src"]),
        dst=str(row["dst"]),
        edge_type=str(row["edge_type"]),
        op="R",
    )


def _node_retraction(row: Any) -> NodeFact:
    # A retraction reads only node_id and subtype (the NodeFact branch of the
    # same loop); hand-offs, declarations and incident edges are read from
    # the database, not from the fact.
    return NodeFact(
        node_id=str(row["node_id"]),
        subtype=str(row["subtype"]),
        attrs={},
        source_record_id={},
        source_revision={},
        op="R",
    )


def iter_run_retraction_pages(
    graph: Any,
    *,
    source: str,
    run_id: str,
    page_rows: int = RETRACTION_PAGE_ROWS,
) -> Iterator[list[EdgeFact | NodeFact]]:
    """Yield the retractions of every row one run wrote, a page at a time.

    The first ``next()`` reads only the KEYS of every row the run wrote
    (edge ids, then node ids), before the caller applies anything. That is
    the same one read, at the same point, that the single list used to make,
    so it fixes the retraction set exactly; and because no page has been
    applied yet, the branch write lock ``apply_facts`` takes is not held
    while it runs.

    Each page then fetches its full rows by key, ordered by key, only after
    the caller has applied the page before it: at most one page of rows is in
    memory, and the work between two statements is one page's. Every edge
    page comes before every node page, as in the single list. Applying a
    page changes no row of a later page: an edge page removes only its own
    edges, and a node page removes or hands off only its own nodes and
    deletes edges incident to them, after every edge page is done. The
    fetch keeps the original ``source`` and ``run_id`` filter. A row
    another writer re-attributed after the key read raises
    ``CompensationRowSetChanged`` instead of retracting a partial set; a row
    another compensation already removed is skipped, as the single list's
    apply skipped it (see ``_refuse_changed_rows``).
    """
    if page_rows < 1:
        raise ValueError(f"page_rows must be positive, got {page_rows!r}")
    params: dict[str, Any] = {"source": source, "run_id": run_id}
    edge_ids = sorted(
        int(row["edge_id"]) for row in graph.query(_EDGE_KEYS_SQL, params)
    )
    node_ids = sorted(
        str(row["node_id"]) for row in graph.query(_NODE_KEYS_SQL, params)
    )
    for table, key, keys, sql, exists_sql, build in (
        ("okg.graph_edges", "edge_id", edge_ids, _EDGE_PAGE_SQL,
         _EDGE_EXISTS_SQL, _edge_retraction),
        ("okg.graph_nodes", "node_id", node_ids, _NODE_PAGE_SQL,
         _NODE_EXISTS_SQL, _node_retraction),
    ):
        for start in range(0, len(keys), page_rows):
            wanted = keys[start:start + page_rows]
            rows = graph.query(sql, {**params, "ids": wanted})
            if len(rows) != len(wanted):
                _refuse_changed_rows(
                    graph, table=table, key=key, exists_sql=exists_sql,
                    wanted=wanted, rows=rows, source=source, run_id=run_id,
                )
            page = [build(row) for row in rows]
            del rows
            if page:
                yield page


def _refuse_changed_rows(
    graph: Any,
    *,
    table: str,
    key: str,
    exists_sql: str,
    wanted: list[Any],
    rows: list[Any],
    source: str,
    run_id: str,
) -> None:
    """Decide what a page that came back short means.

    Only another writer, between the key read and this fetch, can change one
    of the page's rows. A row that is simply gone -- another compensation of
    the same run already retracted it -- is a row the single list would also
    have found gone when it applied, and retracting nothing for it is what it
    did; that keeps racing compensations of one run idempotent (#2244). A row
    whose key still exists but no longer belongs to this source and run was
    re-attributed: the single list would have retracted a row it no longer
    wrote, and skipping it would retract a partial set. That refuses, and
    the caller's transaction rolls back.
    """
    missing = sorted(set(wanted) - {row[key] for row in rows}, key=str)
    if not missing:
        return
    changed = sorted(
        {row[key] for row in graph.query(exists_sql, {"ids": missing})},
        key=str,
    )
    if not changed:
        return
    raise CompensationRowSetChanged(
        "a failed run's rows changed during its compensation",
        data={
            "table": table,
            "source": source,
            "run_id": run_id,
            "missing_count": len(changed),
            "missing_sample": [str(item) for item in changed[:5]],
        },
        fix_hint=undo_resolve_hint(None, [source]),
    )


_EDGE_EXISTS_SQL = """
    SELECT edge_id FROM okg.graph_edges WHERE edge_id = ANY(:ids)
"""
_NODE_EXISTS_SQL = """
    SELECT node_id FROM okg.graph_nodes WHERE node_id = ANY(:ids)
"""
_EDGE_KEYS_SQL = """
    SELECT edge_id
      FROM okg.graph_edges
     WHERE source = :source
       AND run_id = :run_id
"""
_NODE_KEYS_SQL = """
    SELECT node_id
      FROM okg.graph_nodes
     WHERE source = :source
       AND run_id = :run_id
"""
_EDGE_PAGE_SQL = """
    SELECT edge_id, src, dst, edge_type
      FROM okg.graph_edges
     WHERE edge_id = ANY(:ids)
       AND source = :source
       AND run_id = :run_id
     ORDER BY edge_id
"""
_NODE_PAGE_SQL = """
    SELECT node_id, subtype
      FROM okg.graph_nodes
     WHERE node_id = ANY(:ids)
       AND source = :source
       AND run_id = :run_id
     ORDER BY node_id
"""


@dataclass
class _NodeUndo:
    """What the undo does to one node row, decided before anything is written."""

    base: dict[str, Any] | None
    head: dict[str, Any] | None
    target: dict[str, Any] | None
    restore: bool = False
    conflict: str | None = None

    @property
    def restores_deleted(self) -> bool:
        return self.restore and self.head is None and self.target is not None


def _apply_undo_page(
    session: Any,
    table: str,
    *,
    upserts: list[dict[str, Any]],
    deletes: list[dict[str, Any]],
) -> None:
    # One function for every page's write, so a test can fail a chosen page
    # and prove the whole undo rolls back.
    if upserts or deletes:
        session.apply_table_edits(table, upserts=upserts, deletes=deletes)


def undo_run_to_run_base(
    store: Any,
    graph: Any,
    *,
    branch: str,
    source: str,
    run_id: str,
    checkpoint_id: str,
    page_rows: int = RETRACTION_PAGE_ROWS,
) -> dict[str, Any]:
    """Write every key one failed run touched back to its run-base version.

    Issue #2993. Runs inside the caller's single compensation transaction,
    after the branch write fence, as ordinary writes on the branch head --
    the same shape as reverting a branch to a generation (issue #2862), but
    limited to the failed run's keys, so another source's writes since the
    run base stay.

    The candidate keys are the run's own: keys with a row version of the
    failed source written by the run, or visible at the run base and closed
    since (see the comment at the candidate scan for why that covers an
    in-place overwrite), plus the nodes whose declaration by the source
    changed and the edges incident to candidate nodes. Found through the
    source-leading indexes before the branch write lock, then decided a
    bounded page at a time in key order. For each key the run-base row ``b``
    and the head row ``h`` decide, per table (``S`` the source, ``R`` the
    run):

    * nodes and edges: ``h`` written by ``R`` -> back to ``b`` (an insert is
      deleted, an update or a delete-and-reinsert restored); ``h`` gone and
      ``b`` belonged to ``S`` -> ``b`` restored (the run deleted it); a
      node ``S`` owned that now belongs to another declarer after ``S``
      withdrew its declaration -> ``b`` restored (the run's own shared-node
      hand-off); ``h`` gone and ``b`` an edge of another source incident to a
      node the run deleted -> ``b`` restored (the run's delete took it);
      anything else of another source is left alone. One exception to the
      first rule: a node the run inserted (no ``b``) that another source
      still declares is handed to the surviving declarer, not deleted
      (issue #3382), and that source's edges onto it stay.
    * node declarations of ``S``: back to ``b``; other sources' are left.
    * ranked-edge declarations (item 92a) of ``S``: back to ``b``; a ranked
      edge ``S`` owned that passed to another declarer when the run withdrew
      ``S``'s declaration -> ``b`` restored (the run's own hand-off); another
      source's declaration the run's node delete purged comes back with the
      edge row it restores.

    A key the run touched that another writer changed after the run base is
    a conflict and is never overwritten: a row of ``S`` last written by
    another run, a node another source handed over after the run base
    (another source's declaration of it changed), or an edge the undo would
    restore onto a node another writer removed. A conflict raises
    ``CompensationRowSetChanged``; the caller's transaction rolls back and
    nothing is written. Edges are decided first, reading the nodes' pre-undo
    state, then nodes, then declarations. Edges written after the run base
    onto a node the undo deletes (derived edges a publish added) are
    deleted with it, as a retraction of that node would.

    Tables other than these four carry no source or run attribution; a
    source run does not write them, so the undo leaves them.
    """
    from chronos_core.branching import BranchingError, TableNotRegisteredError
    from okg.substrate.errors import SubstrateError
    from okg.substrate.graph_store_internal.common import (
        EDGE_TABLE,
        NODE_TABLE,
        quote_ident,
        quote_physical_table,
    )
    from okg.substrate.graph_store_internal.edge_declarers import (
        EDGE_DECLARERS_TABLE,
    )
    from okg.substrate.graph_store_internal.node_declarers import (
        NODE_DECLARERS_TABLE,
        choose_survivor,
        load_declarations,
        node_row_from_declaration,
    )
    from okg.substrate.nomos.read_class import stamp_rows

    if page_rows < 1:
        raise ValueError(f"page_rows must be positive, got {page_rows!r}")
    started = time.perf_counter()
    backend = store.context._backend
    db = store.context.db
    cp = db.execute(
        "SELECT segment_id FROM _chronos_branch_interval_checkpoints "
        "WHERE checkpoint_id = ?",
        (checkpoint_id,),
    ).fetchone()
    parent = None if cp is None else db.execute(
        "SELECT parent_segment_id FROM _chronos_branch_interval_segments "
        "WHERE segment_id = ?",
        (int(cp["segment_id"]),),
    ).fetchone()
    if parent is None or parent["parent_segment_id"] is None:
        raise SubstrateError(
            "cannot undo the failed run: its run-base checkpoint is missing",
            data={"branch": branch, "run_id": run_id, "checkpoint": checkpoint_id},
            fix_hint=undo_resolve_hint(None, [source]),
        )
    base_segment = backend._segment(int(cp["segment_id"]))
    if (
        NODE_DECLARERS_TABLE not in backend.tables
        or EDGE_DECLARERS_TABLE not in backend.tables
    ):
        backend.refresh_registries()

    def table_meta(segment: Any, table: str) -> Any | None:
        try:
            return backend._meta_for_segment(segment, table)
        except TableNotRegisteredError:
            return None

    def lineage(head_id: int) -> None:
        try:
            backend._mutable_segment_ids_after_base(
                head_id, int(parent["parent_segment_id"]),
            )
        except BranchingError as exc:
            raise SubstrateError(
                "cannot undo the failed run: the branch head is no longer "
                "descended from its run-base checkpoint",
                data={
                    "branch": branch, "run_id": run_id,
                    "checkpoint": checkpoint_id,
                },
                fix_hint=undo_resolve_hint(None, [source]),
            ) from exc

    head_id = backend._branch_segment_id(branch)
    lineage(head_id)
    head_segment = backend._segment(head_id)
    metas: dict[str, Any] = {}
    for table in (
        EDGE_TABLE, NODE_TABLE, NODE_DECLARERS_TABLE, EDGE_DECLARERS_TABLE,
    ):
        base_meta = table_meta(base_segment, table)
        head_meta = table_meta(head_segment, table)
        if base_meta is None and head_meta is None:
            continue
        if (
            base_meta is None
            or head_meta is None
            or base_meta.physical_name != head_meta.physical_name
            or base_meta.pk_columns != head_meta.pk_columns
            or base_meta.columns != head_meta.columns
        ):
            raise SubstrateError(
                "cannot undo the failed run: a table's schema changed since "
                "its run-base checkpoint",
                data={"branch": branch, "run_id": run_id, "table": table},
                fix_hint=undo_resolve_hint(None, [source]),
            )
        metas[table] = head_meta
    if NODE_TABLE not in metas:
        return {"mode": "undo_to_run_base", "keys_scanned": {}}

    # The run's keys, found without reading any other source's writes and
    # before the branch write lock is taken (review of issue #2993;
    # option 2). ``b_point`` is the run base's branch point, ``h_point`` the
    # head's now. A key is a candidate when the failed source ``S`` has a row
    # version for it that is either
    #   * written by the run ``R`` (``run_id = R``, after the run base), or
    #   * visible at the run base and no longer visible at the head (it was
    #     updated, deleted or handed off since the run base).
    # The second rule is what keeps an in-place overwrite sound. A writer
    # that writes a key in the same segment right after the run replaces the
    # run's version in place, so no ``run_id = R`` row is left for that key.
    # But the run's own write first closed the key's run-base version, and
    # that closed version is still there: when it was ``S``'s, the key stays
    # a candidate and the decision rules below refuse the other writer's
    # version (``S``'s row changed by another writer). When the run-base
    # version was not ``S``'s (or there was none), the rules keep the other
    # writer's version anyway, so missing that key changes nothing. Both
    # rules read only ``S``'s rows through the source-leading indexes; the
    # scan grows with the failed source's history, never with another
    # source's writes, and it runs before the lock.
    b_point = base_segment.branch_point
    h_point = head_segment.branch_point
    changed_sql = (
        "((run_id = ? AND live_lo > ?) "
        "OR (live_lo <= ? AND ? < live_hi AND live_hi <= ?))"
    )

    def changed_params(point: Any) -> tuple[Any, ...]:
        return (str(run_id), b_point, b_point, b_point, point)

    def source_candidates(table: str) -> list[dict[str, Any]]:
        meta = metas[table]
        pk = list(meta.pk_columns)
        cols = ", ".join(quote_ident(col) for col in pk)
        rows = db.execute(
            f"SELECT DISTINCT {cols} FROM "
            f"{quote_physical_table(meta.physical_name)} "
            f"WHERE source = ? AND {changed_sql} ORDER BY {cols}",
            (source, *changed_params(h_point)),
        ).fetchall()
        return [{col: row[col] for col in pk} for row in rows]

    def source_candidate_count(table: str) -> int:
        meta = metas[table]
        cols = ", ".join(quote_ident(col) for col in meta.pk_columns)
        return int(db.execute(
            f"SELECT count(*) AS n FROM (SELECT DISTINCT {cols} FROM "
            f"{quote_physical_table(meta.physical_name)} "
            f"WHERE source = ? AND {changed_sql}) AS k",
            (source, *changed_params(h_point)),
        ).fetchone()["n"])

    # Refuse above the bound before reading any key list: the undo holds the
    # branch write lock from the lock to its commit.
    max_keys = run_base_undo_max_keys()
    touched = sum(source_candidate_count(table) for table in metas)
    if touched > max_keys:
        raise SourceRunUndoTooLarge(
            "a failed run's undo would touch more keys than one undo may "
            "hold the branch write lock for; nothing was written",
            data={
                "source": source,
                "run_id": run_id,
                "branch": branch,
                "keys_since_run_base": touched,
                "max_keys": max_keys,
            },
            fix_hint=undo_resolve_hint(None, [source]) + (
                f"; or raise {UNDO_MAX_KEYS_ENV} (default "
                f"{RUN_BASE_UNDO_MAX_KEYS}) and retry the undo, accepting "
                "that every writer waits for it"
            ),
        )
    candidates: dict[str, list[dict[str, Any]]] = {
        table: source_candidates(table) for table in metas
    }
    # A node the run handed off shows only through S's withdrawn declaration.
    node_ids = {str(key["node_id"]) for key in candidates[NODE_TABLE]}
    node_ids |= {
        str(key["node_id"]) for key in candidates.get(NODE_DECLARERS_TABLE, [])
    }
    candidates[NODE_TABLE] = [{"node_id": node_id} for node_id in sorted(node_ids)]
    # A ranked edge whose winner the run's declaration changed shows through
    # S's declaration too (item 92a).
    if EDGE_TABLE in metas and EDGE_DECLARERS_TABLE in candidates:
        candidates[EDGE_TABLE] = [
            {"edge_id": edge_id}
            for edge_id in sorted(
                {int(key["edge_id"]) for key in candidates[EDGE_TABLE]}
                | {
                    int(key["edge_id"])
                    for key in candidates[EDGE_DECLARERS_TABLE]
                }
            )
        ]

    scanned_at = time.perf_counter()
    # The undo rewrites ranked edges and their declarations (item 92a): hold
    # the ranking steady, as every such writer does. A ranking change gives
    # way to a caller already holding the branch lock, so the order is safe.
    store._lock_edge_ranking_shared()
    store._lock_branch_write(branch)
    locked_at = time.perf_counter()
    session = store.checkout(branch)
    # Re-read the head under the lock: a publish may have cut a checkpoint
    # since. Decisions below read rows as of this head.
    head_id = backend._branch_segment_id(branch)
    lineage(head_id)
    head_segment = backend._segment(head_id)
    h_point = head_segment.branch_point
    # Edges of any source the run's node writes took or left dangling:
    # incident to a candidate node, and closed or written since the run
    # base. Read under the lock, not with the scan above: another writer
    # (a linker, an in-flight chunk) can commit an edge onto a node the run
    # inserted while the undo waits for the lock, and a raw delete of that
    # node does not cascade (review of issue #2993, round 4). Such an edge is
    # decided by the rule for edges onto a node the undo deletes: it was
    # written after the run base, so it is deleted with the node. Keyed by
    # node id through the src and dst indexes, so it costs the candidate
    # nodes' degree.
    if EDGE_TABLE in metas and node_ids:
        edge_meta = metas[EDGE_TABLE]
        edge_ids = {int(key["edge_id"]) for key in candidates[EDGE_TABLE]}
        ordered = sorted(node_ids)
        for start in range(0, len(ordered), page_rows):
            chunk = ordered[start:start + page_rows]
            rows = db.execute(
                "SELECT DISTINCT edge_id FROM "
                f"{quote_physical_table(edge_meta.physical_name)} "
                "WHERE (src = ANY(?) OR dst = ANY(?)) AND (live_lo > ? "
                "OR (live_lo <= ? AND ? < live_hi AND live_hi <= ?))",
                (chunk, chunk, b_point, b_point, b_point, h_point),
            ).fetchall()
            edge_ids |= {int(row["edge_id"]) for row in rows}
        candidates[EDGE_TABLE] = [
            {"edge_id": edge_id} for edge_id in sorted(edge_ids)
        ]

    def rows_at(
        table: str, keys: list[dict[str, Any]], segment: Any,
    ) -> dict[tuple[Any, ...], dict[str, Any]]:
        # One statement per chunk: a composite key binds one parameter per
        # column, and PostgreSQL refuses more than 65,535 (as in #2862).
        meta = metas[table]
        found: dict[tuple[Any, ...], dict[str, Any]] = {}
        chunk = backend._upsert_batch_chunk_size(meta)
        for start in range(0, len(keys), chunk):
            found.update(backend._rows_by_key_for_keys(
                table, keys[start:start + chunk], segment, meta,
            ))
        return found

    def same(table: str, left: Any, right: Any) -> bool:
        if left is None or right is None:
            return left is None and right is None
        return all(
            backend._storage_values_equal(left.get(col), right.get(col))
            for col in metas[table].columns
        )

    def written_key_pages(table: str) -> Iterator[list[dict[str, Any]]]:
        """The run's candidate keys of ``table``, a page at a time."""
        keys = candidates.get(table, [])
        for start in range(0, len(keys), page_rows):
            yield keys[start:start + page_rows]

    def declaration_keys(node_ids: list[str]) -> list[dict[str, Any]]:
        """Every declaration of these nodes changed since the run base."""
        meta = metas[NODE_DECLARERS_TABLE]
        rows = db.execute(
            "SELECT DISTINCT node_id, source FROM "
            f"{quote_physical_table(meta.physical_name)} "
            "WHERE node_id = ANY(?) AND (live_lo > ? "
            "OR (live_lo <= ? AND ? < live_hi AND live_hi <= ?))",
            (list(node_ids), b_point, b_point, b_point, h_point),
        ).fetchall()
        return [
            {"node_id": row["node_id"], "source": row["source"]} for row in rows
        ]

    def ran(row: dict[str, Any] | None) -> bool:
        return row is not None and str(row.get("run_id")) == str(run_id)

    def owned(row: dict[str, Any] | None) -> bool:
        return row is not None and row.get("source") == source

    def declaration_changes(node_ids: list[str]) -> dict[str, dict[str, bool]]:
        out: dict[str, dict[str, bool]] = {}
        if NODE_DECLARERS_TABLE not in metas or not node_ids:
            return out
        for keys in [declaration_keys(node_ids)]:
            base = rows_at(NODE_DECLARERS_TABLE, keys, base_segment)
            head = rows_at(NODE_DECLARERS_TABLE, keys, head_segment)
            for key in keys:
                key_tuple = (key["node_id"], key["source"])
                b, h = base.get(key_tuple), head.get(key_tuple)
                entry = out.setdefault(
                    str(key["node_id"]),
                    {"ours_withdrawn": False, "other_changed": False},
                )
                if key["source"] == source:
                    entry["ours_withdrawn"] |= b is not None and h is None
                elif not same(NODE_DECLARERS_TABLE, b, h):
                    entry["other_changed"] = True
        return out

    def edge_declaration_rows(
        edge_ids: list[int],
    ) -> tuple[dict[tuple[Any, ...], Any], dict[tuple[Any, ...], Any]]:
        """Base and head rows of every declaration of these ranked edges
        changed since the run base (item 92a)."""
        if EDGE_DECLARERS_TABLE not in metas or not edge_ids:
            return {}, {}
        meta = metas[EDGE_DECLARERS_TABLE]
        rows = db.execute(
            "SELECT DISTINCT edge_id, source FROM "
            f"{quote_physical_table(meta.physical_name)} "
            "WHERE edge_id = ANY(?) AND (live_lo > ? "
            "OR (live_lo <= ? AND ? < live_hi AND live_hi <= ?))",
            (list(edge_ids), b_point, b_point, b_point, h_point),
        ).fetchall()
        keys = [
            {"edge_id": row["edge_id"], "source": row["source"]} for row in rows
        ]
        return (
            rows_at(EDGE_DECLARERS_TABLE, keys, base_segment),
            rows_at(EDGE_DECLARERS_TABLE, keys, head_segment),
        )

    handoff_cache: dict[str, list[str]] = {}
    # Shared nodes the run inserted: handed to a surviving declarer, or
    # deleted because every other declaration has another subtype (the graph
    # writer's `handoffs_refused_subtype`). Sets: a node can be decided
    # more than once (before and under the lock).
    handed_off_ids: set[str] = set()
    refused_subtype_ids: set[str] = set()

    def handoffs_for(
        inserted: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Survivor rows for shared nodes the run inserted (issue #3382).

        A node absent at the run base that the run created, and that
        another source still declares at the head, is asserted by that
        source: restoring "absent" would delete it. The row passes to the
        surviving declarer instead, by the shared-node rule of issue #1901
        (the lowest source name among declarations under the live row's
        subtype), prepared as the graph writer prepares a hand-off.
        """
        if not inserted or NODE_DECLARERS_TABLE not in metas:
            return {}
        declarations = load_declarations(session, list(inserted))
        survivors = []
        for node_id, live in inserted.items():
            others = [
                declaration
                for declaration in declarations.get(node_id, ())
                if str(declaration.get("source") or "") != source
            ]
            survivor = choose_survivor(
                [
                    declaration
                    for declaration in others
                    if str(declaration.get("subtype") or "")
                    == str(live.get("subtype") or "")
                ],
                excluding={source},
            )
            if survivor is not None:
                survivors.append(node_row_from_declaration(survivor))
            elif others:
                refused_subtype_ids.add(node_id)
        if not survivors:
            return {}
        prepared, refused = store._prepare_handoff_rows(
            survivors,
            current_nodes=inserted,
            searchable_fields_cache=handoff_cache,
        )
        out: dict[str, dict[str, Any]] = {}
        for row in prepared:
            node_id = str(row["node_id"])
            full = {**inserted[node_id], **row}
            stamp_rows([full], "node", catalog=store.context.db)
            # The head row's own shape, so the undo writes it as it writes
            # any restored row.
            out[node_id] = {column: full[column] for column in inserted[node_id]}
        refused_subtype_ids.update(str(node_id) for node_id in refused)
        handed_off_ids.update(out)
        return out

    def decide_nodes(node_ids: list[str]) -> dict[str, _NodeUndo]:
        keys = [{"node_id": node_id} for node_id in node_ids]
        base = rows_at(NODE_TABLE, keys, base_segment)
        head = rows_at(NODE_TABLE, keys, head_segment)
        declarations = declaration_changes(node_ids)
        handoffs = handoffs_for({
            node_id: head[(node_id,)]
            for node_id in node_ids
            if base.get((node_id,)) is None and ran(head.get((node_id,)))
        })
        out: dict[str, _NodeUndo] = {}
        for node_id in node_ids:
            b, h = base.get((node_id,)), head.get((node_id,))
            declared = declarations.get(
                node_id, {"ours_withdrawn": False, "other_changed": False},
            )
            decision = _NodeUndo(base=b, head=h, target=h)
            if same(NODE_TABLE, b, h):
                pass
            elif ran(h):
                if b is not None and not owned(b) and declared["other_changed"]:
                    decision.conflict = (
                        "another source handed this shared node over after "
                        "the run started"
                    )
                elif node_id in handoffs:
                    # Inserted by the run, declared since by another source:
                    # that source keeps it (issue #3382).
                    decision.target, decision.restore = handoffs[node_id], True
                else:
                    decision.target, decision.restore = b, True
            elif h is None:
                if owned(b):
                    decision.target, decision.restore = b, True
            elif owned(b):
                if (
                    h.get("source") != source
                    and declared["ours_withdrawn"]
                    and not declared["other_changed"]
                ):
                    # The run withdrew its declaration and the node passed
                    # to a surviving declarer: the run's own hand-off.
                    decision.target, decision.restore = b, True
                else:
                    decision.conflict = (
                        "another writer changed this row after the run started"
                    )
            out[node_id] = decision
        return out

    counts: dict[str, int] = {}
    restored = deleted = 0

    def refuse(table: str, conflicts: list[tuple[Any, str]]) -> None:
        raise CompensationRowSetChanged(
            "a failed run's rows were changed by another writer after the "
            "run started; the undo wrote nothing",
            data={
                "table": table,
                "source": source,
                "run_id": run_id,
                "missing_count": len(conflicts),
                "missing_sample": [str(key) for key, _ in conflicts[:5]],
                "reasons": sorted({reason for _, reason in conflicts}),
                "reason": "changed_after_run_base",
            },
            fix_hint=undo_resolve_hint(None, [source]),
        )

    page_ms: dict[str, list[float]] = {}
    page_started = time.perf_counter()

    def write(table: str, upserts: list, deletes: list) -> None:
        nonlocal restored, deleted, page_started
        _apply_undo_page(session, table, upserts=upserts, deletes=deletes)
        restored += len(upserts)
        deleted += len(deletes)
        now = time.perf_counter()
        page_ms.setdefault(table, []).append(
            round((now - page_started) * 1000, 1)
        )
        page_started = now

    restored_declarations: list[dict[str, Any]] = []
    # Edges first, decided against the nodes' state before the undo.
    if EDGE_TABLE in metas:
        for keys in written_key_pages(EDGE_TABLE):
            counts[EDGE_TABLE] = counts.get(EDGE_TABLE, 0) + len(keys)
            base = rows_at(EDGE_TABLE, keys, base_segment)
            head = rows_at(EDGE_TABLE, keys, head_segment)
            endpoints = sorted({
                str(row[end])
                for row in (*base.values(), *head.values())
                for end in ("src", "dst")
            })
            nodes = decide_nodes(endpoints)
            declared_base, declared_head = edge_declaration_rows(
                [int(key["edge_id"]) for key in keys],
            )
            upserts: list[dict[str, Any]] = []
            deletes: list[dict[str, Any]] = []
            conflicts: list[tuple[Any, str]] = []
            for key in keys:
                b, h = base.get((key["edge_id"],)), head.get((key["edge_id"],))
                if same(EDGE_TABLE, b, h):
                    continue
                if ran(h):
                    target = b
                elif h is None:
                    if not (owned(b) or any(
                        nodes[str(b[end])].restores_deleted
                        for end in ("src", "dst")
                    )):
                        continue
                    target = b
                elif owned(b):
                    # A ranked edge the run withdrew its winning declaration
                    # of, which passed to the next-ranked declarer: the run's
                    # own hand-off, undone when nobody else's declaration of
                    # the edge changed since (item 92a).
                    changed = {
                        declaration_key[1]: (
                            declared_base.get(declaration_key),
                            declared_head.get(declaration_key),
                        )
                        for declaration_key in (
                            set(declared_base) | set(declared_head)
                        )
                        if declaration_key[0] == key["edge_id"]
                    }
                    ours = changed.get(source, (None, None))
                    handed_off = (
                        ours[0] is not None
                        and ours[1] is None
                        and not any(
                            not same(EDGE_DECLARERS_TABLE, before, after)
                            for name, (before, after) in changed.items()
                            if name != source
                        )
                    )
                    if not handed_off:
                        conflicts.append((key["edge_id"], "another writer "
                                          "changed this row after the run "
                                          "started"))
                        continue
                    target = b
                else:
                    target = h
                if target is not None:
                    for end in ("src", "dst"):
                        node = nodes[str(target[end])]
                        if node.target is not None or node.head is None and (
                            node.base is None
                        ):
                            # Present after the undo, or absent at the run
                            # base and now alike (the edge was dangling then).
                            continue
                        if node.base is not None:
                            conflicts.append((
                                key["edge_id"],
                                "the undo would restore an edge onto a node "
                                "another writer removed",
                            ))
                        elif b is None:
                            # Onto a node the undo deletes: it goes with it.
                            target = None
                        break
                if target is None and h is not None:
                    deletes.append(dict(key))
                elif target is not None and not same(EDGE_TABLE, target, h):
                    upserts.append(target)
                if target is not None and target is b:
                    # Other producers' declarations the run's node delete
                    # purged come back with the edge row (item 92a); the
                    # failed source's own are restored below.
                    for declaration_key, before in declared_base.items():
                        if (
                            declaration_key[0] == key["edge_id"]
                            and declaration_key[1] != source
                            and declared_head.get(declaration_key) is None
                        ):
                            restored_declarations.append(before)
            if conflicts:
                refuse(EDGE_TABLE, conflicts)
            write(EDGE_TABLE, upserts, deletes)

    node_rows_restored = 0
    for keys in written_key_pages(NODE_TABLE):
        counts[NODE_TABLE] = counts.get(NODE_TABLE, 0) + len(keys)
        nodes = decide_nodes([str(key["node_id"]) for key in keys])
        conflicts = [
            (node_id, node.conflict)
            for node_id, node in nodes.items()
            if node.conflict is not None
        ]
        if conflicts:
            refuse(NODE_TABLE, conflicts)
        node_upserts = [
            node.target for node in nodes.values()
            if node.restore and node.target is not None
        ]
        node_rows_restored += len(node_upserts)
        write(
            NODE_TABLE,
            node_upserts,
            [{"node_id": node_id} for node_id, node in nodes.items()
             if node.restore and node.target is None and node.head is not None],
        )
    if node_rows_restored:
        # PACT openrouter-embeddings-hnsw: the restored rows bypass the
        # graph writer's document links, and their content may never have
        # been embedded. One link number per active vector set, in this
        # transaction, keeps the worker's fill from carrying its last
        # completion past them. A delete leaves no row to embed.
        from okg.substrate.embeddings.store import mark_node_rows_restored

        mark_node_rows_restored(db)

    if NODE_DECLARERS_TABLE in metas:
        for keys in written_key_pages(NODE_DECLARERS_TABLE):
            counts[NODE_DECLARERS_TABLE] = (
                counts.get(NODE_DECLARERS_TABLE, 0) + len(keys)
            )
            ours = [key for key in keys if key["source"] == source]
            base = rows_at(NODE_DECLARERS_TABLE, ours, base_segment)
            head = rows_at(NODE_DECLARERS_TABLE, ours, head_segment)
            upserts, deletes, conflicts = [], [], []
            for key in ours:
                key_tuple = (key["node_id"], key["source"])
                b, h = base.get(key_tuple), head.get(key_tuple)
                if same(NODE_DECLARERS_TABLE, b, h):
                    continue
                if h is not None and not ran(h):
                    conflicts.append((key_tuple, "another run of this source "
                                      "changed its declaration after the run "
                                      "started"))
                elif b is None:
                    deletes.append(dict(key))
                else:
                    upserts.append(b)
            if conflicts:
                refuse(NODE_DECLARERS_TABLE, conflicts)
            write(NODE_DECLARERS_TABLE, upserts, deletes)

    if EDGE_DECLARERS_TABLE in metas:
        # The failed source's own declarations of ranked edges, back to the
        # run base (item 92a), by the node-declarer rule above.
        for keys in written_key_pages(EDGE_DECLARERS_TABLE):
            counts[EDGE_DECLARERS_TABLE] = (
                counts.get(EDGE_DECLARERS_TABLE, 0) + len(keys)
            )
            ours = [key for key in keys if key["source"] == source]
            base = rows_at(EDGE_DECLARERS_TABLE, ours, base_segment)
            head = rows_at(EDGE_DECLARERS_TABLE, ours, head_segment)
            upserts, deletes, conflicts = [], [], []
            for key in ours:
                key_tuple = (key["edge_id"], key["source"])
                b, h = base.get(key_tuple), head.get(key_tuple)
                if same(EDGE_DECLARERS_TABLE, b, h):
                    continue
                if h is not None and not ran(h):
                    conflicts.append((key_tuple, "another run of this source "
                                      "changed its declaration after the run "
                                      "started"))
                elif b is None:
                    deletes.append(dict(key))
                else:
                    upserts.append(b)
            if conflicts:
                refuse(EDGE_DECLARERS_TABLE, conflicts)
            write(EDGE_DECLARERS_TABLE, upserts, deletes)
        if restored_declarations:
            write(EDGE_DECLARERS_TABLE, restored_declarations, [])

    finished = time.perf_counter()
    return {
        "mode": "undo_to_run_base",
        "keys_scanned": counts,
        "rows_restored": restored,
        "rows_deleted": deleted,
        "nodes_handed_off": len(handed_off_ids),
        "handoffs_refused_subtype": len(refused_subtype_ids),
        "scan_ms_before_lock": round((scanned_at - started) * 1000, 3),
        "keys_since_run_base": touched,
        "page_ms": page_ms,
        "lock_wait_ms": round((locked_at - scanned_at) * 1000, 3),
        "undo_ms_under_lock": round((finished - locked_at) * 1000, 3),
    }


@dataclass(frozen=True)
class SourceStateSnapshot:
    """Pre-run source watermark and progress that a failed run must not keep."""

    deployment: str
    source: str
    watermark: dict[str, Any] | None
    progress_rows: tuple[dict[str, Any], ...]


def snapshot_source_state(
    dsn: str,
    *,
    deployment: str,
    source: str,
) -> SourceStateSnapshot:
    from okg.substrate.db.pool import GENERAL, pooled_connection

    with pooled_connection(dsn, purpose=GENERAL) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT source, cutover_watermark, last_applied_rev,
                       last_published_rev, projection_cursor,
                       last_reconcile_at
                  FROM okg.source_watermarks
                 WHERE source = %s
                """,
                (source,),
            )
            watermark_row = cur.fetchone()
            cur.execute(
                """
                SELECT deployment, source_name, key, value, updated_at
                  FROM okg.source_progress
                 WHERE deployment = %s
                   AND source_name = %s
                   AND key NOT LIKE 'sync.%%'
                 ORDER BY key
                """,
                (deployment, source),
            )
            progress_rows = tuple(dict(row) for row in cur.fetchall())
    return SourceStateSnapshot(
        deployment=deployment,
        source=source,
        watermark=dict(watermark_row) if watermark_row is not None else None,
        progress_rows=progress_rows,
    )


def restore_source_state(
    dsn: str,
    snapshot: SourceStateSnapshot,
    *,
    expected_replay_run_id: str | None = None,
) -> None:
    from okg.substrate.db.pool import GENERAL, pooled_connection

    with pooled_connection(dsn, purpose=GENERAL) as conn:
        with conn.transaction(), conn.cursor() as cur:
            if expected_replay_run_id is not None:
                cur.execute(
                    """
                    SELECT replay_required->>'run_id'
                      FROM okg.source_watermarks
                     WHERE source = %s
                     FOR UPDATE
                    """,
                    (snapshot.source,),
                )
                owner_row = cur.fetchone()
                if owner_row is None or owner_row[0] != expected_replay_run_id:
                    return
            if snapshot.watermark is None:
                cur.execute(
                    """
                    INSERT INTO okg.source_watermarks (source)
                    VALUES (%s)
                    ON CONFLICT (source) DO NOTHING
                    """,
                    (snapshot.source,),
                )
            else:
                watermark = snapshot.watermark
                cur.execute(
                    """
                    INSERT INTO okg.source_watermarks
                      (source, cutover_watermark, last_applied_rev,
                       last_published_rev, projection_cursor, last_reconcile_at)
                    VALUES (%s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s)
                    ON CONFLICT (source) DO UPDATE SET
                      cutover_watermark = EXCLUDED.cutover_watermark,
                      last_applied_rev = EXCLUDED.last_applied_rev,
                      last_published_rev = EXCLUDED.last_published_rev,
                      projection_cursor = EXCLUDED.projection_cursor,
                      last_reconcile_at = EXCLUDED.last_reconcile_at
                    """,
                    (
                        snapshot.source,
                        json.dumps(
                            watermark.get("cutover_watermark") or {},
                            default=str,
                        ),
                        json.dumps(
                            watermark.get("last_applied_rev") or {},
                            default=str,
                        ),
                        json.dumps(
                            watermark.get("last_published_rev") or {},
                            default=str,
                        ),
                        int(watermark.get("projection_cursor") or 0),
                        watermark.get("last_reconcile_at"),
                    ),
                )
            cur.execute(
                """
                DELETE FROM okg.source_progress
                 WHERE deployment = %s
                   AND source_name = %s
                   AND key NOT LIKE 'sync.%%'
                """,
                (snapshot.deployment, snapshot.source),
            )
            if snapshot.progress_rows:
                cur.executemany(
                    """
                    INSERT INTO okg.source_progress
                      (deployment, source_name, key, value, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            row["deployment"],
                            row["source_name"],
                            row["key"],
                            row["value"],
                            row["updated_at"],
                        )
                        for row in snapshot.progress_rows
                    ],
                )
