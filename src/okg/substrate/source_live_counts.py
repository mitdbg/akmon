"""Per-source watermark and published row counts for status reads.

Sources write the Chronos graph, not the legacy fact log: under
chronos-native publishing `okg.node_facts` / `okg.edge_facts` stay empty,
so counting them showed 0 rows for every source. The counts now come from
the console count rollup the publisher maintains, read AS-OF the published
generation: the same generation `okg status` reports totals for, and an
O(1) read instead of a graph scan on every `okg status --watch` tick.
`okg status` and the operator console's system panel share this read.
"""
from __future__ import annotations

from typing import Any

import psycopg

from okg.substrate.console_views.console_count_rollup_read import (
    COUNT_KIND_EDGE_SOURCE,
    COUNT_KIND_SOURCE,
    COUNT_ROLLUP_FLAG,
    ConsoleCountRollupUnavailable,
    console_count_rollup_enabled,
    read_console_count_rollup,
)
from okg.substrate.errors import GenerationCompacted


def source_published_rows(
    conn: Any,
    *,
    generation_id: int | str | None,
    graph_conn: Any | None = None,
) -> tuple[list[dict[str, Any]], dict[str, str] | None]:
    """Rows per source with a watermark or published rows, plus any error.

    Each row has ``source``, ``last_reconcile_at``, ``projection_cursor``,
    ``n_node_facts`` / ``n_edge_facts`` (the source's node and edge rows in
    ``generation_id``; the names predate the graph) and ``last_observed_at``
    (always None: it was read from the retired fact log). With no published
    generation every count is 0. When the generation has no maintained count
    rollup the counts are None and the error names why; the read never falls
    back to scanning the graph.

    ``graph_conn`` reads the rollup when ``conn`` is a wrapper the graph
    store cannot use (the console's query profiler); the watermark read
    still goes through ``conn``.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT source, last_reconcile_at, projection_cursor "
            "FROM okg.source_watermarks"
        )
        watermarks = {row[0]: row[1:] for row in cur.fetchall()}
    counts: dict[tuple[str, str], int] | None = {}
    error: dict[str, str] | None = None
    if generation_id is not None and not console_count_rollup_enabled():
        # With the flag off the publisher carries the old rollup forward,
        # so a read would show stale counts as current.
        counts = None
        error = {
            "code": "source_counts_unavailable",
            "message": f"{COUNT_ROLLUP_FLAG}=0 disables maintained counts",
        }
    elif generation_id is not None:
        rollup_conn = graph_conn if graph_conn is not None else conn
        try:
            counts = read_console_count_rollup(
                rollup_conn,
                generation_id=generation_id,
                kinds={COUNT_KIND_SOURCE, COUNT_KIND_EDGE_SOURCE},
            )
        except ConsoleCountRollupUnavailable as exc:
            counts = None
            error = {
                "code": "source_counts_unavailable",
                "message": (
                    f"generation {generation_id} has no maintained count "
                    f"rollup ({exc.reason}): {exc.detail}"
                ),
            }
        except GenerationCompacted as exc:
            counts = None
            error = {
                "code": "source_counts_unavailable",
                "message": f"generation {generation_id} was compacted: {exc}",
            }
        if counts is None and (
            rollup_conn.info.transaction_status
            == psycopg.pq.TransactionStatus.INERROR
        ):
            # The failed rollup query aborted the transaction; roll it back
            # so the caller's later reads still run. Status reads only.
            rollup_conn.rollback()
    names = set(watermarks)
    if counts:
        names |= {name for _kind, name in counts}
    rows: list[dict[str, Any]] = []
    for source in sorted(names - {None}):
        last_reconcile_at, projection_cursor = watermarks.get(
            source, (None, None)
        )
        rows.append({
            "source": source,
            "last_reconcile_at": last_reconcile_at,
            "projection_cursor": projection_cursor,
            "n_node_facts": (
                None if counts is None
                else counts.get((COUNT_KIND_SOURCE, source), 0)
            ),
            "n_edge_facts": (
                None if counts is None
                else counts.get((COUNT_KIND_EDGE_SOURCE, source), 0)
            ),
            "last_observed_at": None,
        })
    return rows, error
