"""Chronos graph read helpers for deterministic linkers.

These run a Chronos *checkout* query on the caller's connection. The checkout
query RESETS the session's custom GUCs (verified: it clears
``okg.source_program_checksum`` to ``''``). That corrupts a caller that reads the
live graph in the MIDDLE of a witnessed write transaction -- the route-all-writers
contract sets the per-tx ``okg.source_program_checksum`` GUC once and relies on the
column default to stamp the facts it is about to INSERT; a mid-stream Chronos read
would strip that witness, so the facts fail the seal trigger as un-witnessed.

So ``query`` snapshots the OKG session state the caller's write boundary depends on
(the witness GUC, the Chronos branch, the generation pin) and restores it after the
read. (We deliberately do NOT wrap the read in an explicit SAVEPOINT: the Chronos
adapter manages its own transaction state and an outer savepoint conflicts with it.)
"""
from __future__ import annotations

from typing import Any

# OKG session GUCs a witnessed write transaction depends on; the Chronos checkout
# query clears them, so we snapshot and restore them around the read.
_PRESERVED_GUCS = (
    "okg.source_program_checksum",
    "okg.chronos_branch",
    "okg.pin_generation",
)


def query(cur, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Run ``sql`` against the caller's Chronos branch; a failed read yields
    no rows."""
    return _query(cur, sql, params, strict=False)


def query_strict(
    cur,
    sql: str,
    params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Like :func:`query`, but a failed read raises.

    Use it when an empty answer would be taken as a fact about the graph (for
    example "this edge is not live").
    """
    return _query(cur, sql, params, strict=True)


def _query(
    cur,
    sql: str,
    params: dict[str, Any] | None,
    *,
    strict: bool,
) -> list[dict[str, Any]]:
    from okg.substrate.graph_store import (
        DEFAULT_BRANCH,
        ChronosGraphStore,
    )

    conn = cur.connection
    in_tx = not conn.autocommit

    saved: dict[str, str] = {}
    if in_tx:
        with conn.cursor() as state_cur:
            for guc in _PRESERVED_GUCS:
                state_cur.execute("SELECT current_setting(%s, true)", (guc,))
                saved[guc] = state_cur.fetchone()[0]

    branch = saved.get("okg.chronos_branch")
    if not branch:
        with conn.cursor() as state_cur:
            state_cur.execute("SELECT current_setting('okg.chronos_branch', true)")
            branch = state_cur.fetchone()[0]
    branch = branch or DEFAULT_BRANCH

    try:
        store = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=not in_tx,
        )
        rows = list(store.checkout(str(branch)).query(sql, params or {}))
    except Exception:  # noqa: BLE001 — best-effort checkout read; Chronos-unavailable yields empty rows
        if strict:
            raise
        rows = []
    finally:
        # Restore the session state the checkout query reset, so the caller's
        # witnessed write boundary survives the read. Best-effort: if the read
        # aborted the transaction (Chronos unavailable), set_config will no-op.
        if in_tx and saved:
            try:
                with conn.cursor() as state_cur:
                    for guc, val in saved.items():
                        if val not in (None, ""):
                            state_cur.execute(
                                "SELECT set_config(%s, %s, true)", (guc, val)
                            )
            except Exception:  # noqa: BLE001 — best-effort GUC restore in finally
                pass
    return rows


def nodes(cur, subtypes: tuple[str, ...]) -> list[dict[str, Any]]:
    if not subtypes:
        return []
    return query(
        cur,
        """
        SELECT node_id, subtype, attrs
          FROM okg.graph_nodes
         WHERE subtype = ANY(:subtypes)
        """,
        {"subtypes": list(subtypes)},
    )


def edges(
    cur,
    edge_types: tuple[str, ...] | list[str],
) -> list[dict[str, Any]]:
    if not edge_types:
        return []
    return query(
        cur,
        """
        SELECT edge_id, src, edge_type, dst, attrs
          FROM okg.graph_edges
         WHERE edge_type = ANY(:edge_types)
        """,
        {"edge_types": list(edge_types)},
    )
