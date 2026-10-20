"""Worker-write generation allocator (Phase 2 §1.2).

The SDK's mutation helpers (`emit_node_fact`, `emit_edge_fact`,
`update_node_attrs`, plus `request_escalation`'s edge writes) need a
``generation_id`` to attach their writes to. When the writes happen
inside a publisher cycle, the publisher provides the generation. When
the writes happen *outside* a publisher cycle — e.g. from a
``@deployment_scheduled`` workflow or a hook that the publisher just
enqueued — the SDK needs a way to allocate one on its own.

This module supplies that path. The allocator inserts a row into
``okg.graph_generations`` with ``status='worker_write'`` and returns
the ``generation_id``. The status discriminates these rows from
publisher-owned ones so:

  * ``Publisher.reserve()``'s pending-row-cleanup UPDATE (which scopes
    to ``status='pending'``) doesn't touch worker-write rows.
  * Operators can ``GROUP BY status`` to see worker-write activity
    alongside the publisher's history.

The bigserial sequence backing ``generation_id`` is shared between
publisher and worker-write allocators; both call ``nextval`` so IDs
remain globally monotonic. A worker-write at gen 42 is visible to a
reader pinned at ``generation_id >= 42``, regardless of whether the
publisher's next reservation lands at 43 or 44.

Caching across mutation helpers within one ``@deployment_transaction``
context is the SDK helper's job (Phase 2 §2.2.3): the SDK helper holds
a per-context dict keyed on transaction-id and resolves
``worker_write_generation()`` calls inside the same transaction to the
same value. That cache lives in the SDK package, not here.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg


__all__ = [
    "WorkerWriteGeneration",
    "allocate_worker_write_generation",
    "WORKER_WRITE_STATUS",
]


WORKER_WRITE_STATUS = "worker_write"


@dataclass(frozen=True)
class WorkerWriteGeneration:
    """Result of allocating an out-of-cycle generation.

    Attributes
    ----------
    generation_id : int
        The newly-allocated generation id. Writes against this id
        become visible to any reader pinned at the same id or higher.
    deployment_name : str
        The deployment that owns this allocation.
    catalog_version_id : int
        The currently-applied catalog version. Writes at this
        generation_id reference it so the narrowing trigger uses the
        same catalog the publisher would have used for an in-cycle
        write.
    correlation_id : str
        Operator-friendly identifier (``ww:<deployment>:<ts>:<id>``).
        Surfaces in ``okg generation describe`` output so it's clear a
        row was allocated out-of-cycle.
    """

    generation_id: int
    deployment_name: str
    catalog_version_id: int
    correlation_id: str


# ---------------------------------------------------------------------------
# Allocator
# ---------------------------------------------------------------------------


def allocate_worker_write_generation(
    conn: psycopg.Connection,
    *,
    deployment: str,
) -> WorkerWriteGeneration:
    """Allocate a fresh worker-write generation row.

    Runs in a single transaction. Steps:
      1. Look up the currently-applied catalog version.
      2. ``nextval`` on the ``graph_generations.generation_id``
         sequence so we share the publisher's ID space (monotonic).
      3. Compute a correlation id (``ww:<deployment>:<ts>:<id>``).
      4. INSERT the row with ``status='worker_write'`` and the
         allocated id.

    Idempotent under DBOS step retry only through the SDK wrapper's
    step cache — this function itself allocates a fresh row on every
    call. The SDK helper that builds on top of it caches the result
    within a ``@deployment_transaction`` so a retried transaction
    doesn't burn a fresh id on every retry.

    Raises
    ------
    ValueError
        If no applied catalog version exists (the deployment has not
        had its catalog loaded). The SDK surface translates this into
        an actionable error message.
    """
    # Use psycopg3's transaction context manager rather than toggling
    # autocommit. This way the function composes cleanly when called
    # from a caller that already has a transaction open (e.g. the
    # escalation lifecycle, which calls this allocator and then writes
    # facts+live rows all within one logical unit).
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT max(catalog_version_id)
                  FROM okg.catalog_versions
                 WHERE status = 'applied'
                """
            )
            row = cur.fetchone()
            cv = row[0] if row else None
            if cv is None:
                raise ValueError(
                    f"no applied catalog_version for deployment "
                    f"{deployment!r}; load the catalog before issuing "
                    "worker-write generations"
                )

            cur.execute(
                "SELECT nextval(pg_get_serial_sequence("
                "  'okg.graph_generations', 'generation_id'))"
            )
            next_id = cur.fetchone()[0]

            now = datetime.now(timezone.utc)
            correlation_id = (
                f"ww:{deployment}:"
                f"{now.strftime('%Y-%m-%dT%H:%M:%SZ')}:{next_id}"
            )

            cur.execute(
                """
                INSERT INTO okg.graph_generations
                  (generation_id, deployment_name, status,
                   catalog_version_id, correlation_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    next_id, deployment, WORKER_WRITE_STATUS,
                    cv, correlation_id, now,
                ),
            )

    return WorkerWriteGeneration(
        generation_id=next_id,
        deployment_name=deployment,
        catalog_version_id=cv,
        correlation_id=correlation_id,
    )
