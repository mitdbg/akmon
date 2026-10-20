"""Durable retry state for unresolved extractor routes.

The Chronos publisher is the production owner of this state.  The legacy
``Projector`` may use the helpers for compatibility, but ``link_retry`` is not
an extraction cache: rows are work receipts that remain durable until a
published checkpoint acknowledges them.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

import psycopg

#: Two-int advisory lock namespace shared by alias landing
#: (:func:`mark_link_retries_ready_for_aliases`) and miss recording
#: (:func:`record_link_retry_misses`). The second int is the value's stripe.
LINK_RETRY_ALIAS_LOCK_NAMESPACE = 1_155_243_607

#: Every alias value maps to one of this many transaction advisory locks.
#:
#: The lock used to be one key per alias value. Postgres keeps transaction
#: advisory locks in the shared lock table, which holds
#: ``max_locks_per_transaction * (max_connections + max_prepared_transactions)``
#: entries for the whole server (6,400 by default), so a catalog apply that
#: landed more values than that filled the table, and the next backend to need
#: an entry (its own commit, or any other transaction) failed with ``out of
#: shared memory``; the substrate-db CI lane reproduced this on 2026-09-14 with
#: a 12,604-alias seed beside a schema migration. Striping keeps the footprint
#: at most this many entries however many values land. Two callers still
#: exclude each other on the same value, because both derive the stripe from
#: the value with the same function; two different values in one stripe only
#: wait for each other. The cost: a batch of roughly 1,500 distinct values
#: covers every stripe, so a landing that large and a concurrent miss
#: recording serialize against each other for the rest of the transaction
#: that holds them, where before only an exact value collision blocked.
LINK_RETRY_ALIAS_LOCK_STRIPES = 256


def link_retry_alias_lock_stripe(alias_value: str) -> int:
    """The advisory-lock stripe that guards ``alias_value``.

    Stable across processes and future releases (BLAKE2b of the UTF-8 value),
    so a waker and a recorder on different code paths meet on the same lock
    for the same value. The per-value key used before this stripe does not
    exclude against it: a worker still on that build and a catalog apply from
    this one can lose a wakeup, so restart the worker before applying a
    catalog from a newer build.
    """
    digest = hashlib.blake2b(alias_value.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % LINK_RETRY_ALIAS_LOCK_STRIPES


@dataclass(frozen=True, order=True)
class LinkRetryKey:
    deployment_name: str
    branch_name: str
    node_id: str
    entity_type: str
    entity_value: str


def ready_link_retry_node_ids(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch_name: str,
) -> list[str]:
    """Return the complete indexed affected-node set for ready retries."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT node_id
              FROM okg.link_retry
             WHERE deployment_name = %s
               AND branch_name = %s
               AND ready_at IS NOT NULL
             ORDER BY node_id
            """,
            (deployment_name, branch_name),
        )
        return [str(row[0]) for row in cur.fetchall()]


def ready_link_retry_keys_for_nodes(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch_name: str,
    node_ids: Sequence[str],
    include_unready: bool = False,
) -> list[LinkRetryKey]:
    """Return exact ready receipts for source nodes about to be re-extracted.

    ``include_unready`` returns every row of those nodes, ready or not: a
    re-extraction re-observes each mention, so a row it does not record
    again is stale and may be acknowledged with the rest (okg#3272)."""
    ids = list(dict.fromkeys(node_ids))
    if not ids:
        return []
    ready_only = "" if include_unready else "AND ready_at IS NOT NULL"
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT deployment_name, branch_name, node_id, entity_type,
                   entity_value
              FROM okg.link_retry
             WHERE deployment_name = %s
               AND branch_name = %s
               AND node_id = ANY(%s)
               {ready_only}
             ORDER BY node_id, entity_type, entity_value
            """,
            (deployment_name, branch_name, ids),
        )
        return [LinkRetryKey(*map(str, row)) for row in cur.fetchall()]


def queued_extractor_node_ids(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch_name: str,
) -> list[str]:
    """Return the complete explicit historical-repair queue for one scope."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT node_id FROM okg.extractor_retry_queue "
            "WHERE deployment_name = %s AND branch_name = %s ORDER BY node_id",
            (deployment_name, branch_name),
        )
        return [str(row[0]) for row in cur.fetchall()]


def acknowledge_extractor_queue(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch_name: str,
    node_ids: Sequence[str],
) -> int:
    ids = list(dict.fromkeys(node_ids))
    if not ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM okg.extractor_retry_queue "
            "WHERE deployment_name = %s AND branch_name = %s "
            "AND node_id = ANY(%s)",
            (deployment_name, branch_name, ids),
        )
        deleted = cur.rowcount
        cur.execute(
            """
            UPDATE okg.projection_remediation_runs r
               SET status = 'completed', completed_at = now()
             WHERE r.deployment_name = %s
               AND r.branch_name = %s
               AND r.status = 'awaiting_publish'
               AND NOT EXISTS (
                 SELECT 1 FROM okg.extractor_retry_queue q
                  WHERE q.deployment_name = r.deployment_name
                    AND q.branch_name = r.branch_name
               )
            """,
            (deployment_name, branch_name),
        )
        return deleted


#: Misses recorded per transaction by :func:`record_link_retry_misses`.
#:
#: Each batch is one transaction holding the stripe locks of its values, so a
#: concurrent alias landing on one of those stripes waits for at most one
#: batch. Misses are sorted by stripe, and there are about 7,500 misses per
#: stripe at the 1.9-million-row scale this was measured on, so a batch of
#: this size usually holds one or two stripes. Per batch: one lock statement,
#: one alias read, one read of the existing rows, and at most one upsert,
#: which takes the per-row commit (one WAL flush per miss) out of the
#: autocommit publish path. See
#: ``pact/changes/lane-sg-link-retry-batch/design.md``.
LINK_RETRY_RECORD_BATCH_SIZE = 1000


def record_link_retry_misses(
    conn: psycopg.Connection,
    misses: Iterable[LinkRetryKey],
    *,
    seen_by_route: Callable[[str, tuple[int, str]], bool] | None = None,
    batch_size: int = LINK_RETRY_RECORD_BATCH_SIZE,
) -> None:
    """Upsert current resolver misses as unresolved durable work.

    A row that was ready may become unresolved again when an alias is removed
    or quarantined.  Resetting ``ready_at`` is sound: there is no longer a
    resolvable output to acknowledge.

    A miss is recorded ready when an alias with its value exists, i.e. one
    landed between the resolver's miss and this recording. ``seen_by_route``
    (okg#3272) says whether the alias row found now, identified by
    ``(catalog_version_id, xmin)``, is one the route could already see (read
    before it ran): then the miss is the resolver's own answer (an alias of
    another type shares the text, or a resolver that does not consult
    okg.aliases), and the row stays unready. Marking it ready would make it
    ready on every publish, re-extracted, missed again and never
    acknowledged. Any other row (new, re-added after a removal, or in a newer
    catalog version) marks it ready, at worst for one extra cycle. The
    decision is the same live read under the stripe lock as before, with no
    resolver call under the lock, so a landing cannot be lost.

    Misses are recorded ``batch_size`` at a time, each batch in one
    transaction (a savepoint inside a caller's transaction). A row whose key
    exists with the same readiness is left untouched (``ready_at`` and
    ``recorded_at`` keep their values), so a batch that changes nothing
    writes nothing and its commit needs no WAL flush.
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be at least 1, got {batch_size}")
    # Ascending stripe order, the order the waker takes its locks in, so a
    # recorder inside an outer transaction never holds a high stripe while
    # waiting on a low one the waker already holds. The order holds across
    # batches too, so a caller's transaction that spans several batches
    # still acquires its stripes in ascending order.
    rows = sorted(
        set(misses),
        key=lambda row: (link_retry_alias_lock_stripe(row.entity_value), row),
    )
    for start in range(0, len(rows), batch_size):
        _record_link_retry_batch(
            conn, rows[start:start + batch_size], seen_by_route=seen_by_route,
        )


def _record_link_retry_batch(
    conn: psycopg.Connection,
    rows: Sequence[LinkRetryKey],
    *,
    seen_by_route: Callable[[str, tuple[int, str]], bool] | None,
) -> None:
    """Record one batch of distinct misses in one transaction."""
    stripes = sorted({link_retry_alias_lock_stripe(r.entity_value) for r in rows})
    values = sorted({r.entity_value for r in rows})
    with conn.transaction(), conn.cursor() as cur:
        # The lock and the alias recheck are separate statements by design.
        # Under READ COMMITTED, a recorder that waited for an alias landing
        # gets a new snapshot for the recheck and sees the committed alias.
        cur.execute(
            "SELECT pg_advisory_xact_lock(%s, stripe) FROM unnest(%s::int[]) AS stripe",
            (LINK_RETRY_ALIAS_LOCK_NAMESPACE, stripes),
        )
        cur.execute(
            """
            SELECT a.alias, a.catalog_version_id, a.xmin::text
              FROM okg.aliases a
             WHERE a.alias = ANY(%s)
               AND a.catalog_version_id = (
                 SELECT max(catalog_version_id)
                   FROM okg.catalog_versions
                  WHERE status = 'applied'
               )
            """,
            (values,),
        )
        # (alias, catalog_version_id) is the primary key: at most one row
        # per value at the latest applied version.
        alias_rows = {
            str(alias): (int(version), str(xmin))
            for alias, version, xmin in cur.fetchall()
        }
        ready_values = {
            value
            for value, identity in alias_rows.items()
            if not (seen_by_route is not None and seen_by_route(value, identity))
        }
        columns = (
            [r.deployment_name for r in rows],
            [r.branch_name for r in rows],
            [r.node_id for r in rows],
            [r.entity_type for r in rows],
            [r.entity_value for r in rows],
        )
        # Waker, deleter and recorder all take a row's stripe lock before
        # touching it, so this read stays true until the batch commits.
        cur.execute(
            """
            SELECT lr.deployment_name, lr.branch_name, lr.node_id,
                   lr.entity_type, lr.entity_value, lr.ready_at IS NOT NULL
              FROM okg.link_retry lr
              JOIN unnest(%s::text[], %s::text[], %s::text[], %s::text[],
                          %s::text[]) AS k(d, b, n, t, v)
                ON lr.deployment_name = k.d AND lr.branch_name = k.b
               AND lr.node_id = k.n AND lr.entity_type = k.t
               AND lr.entity_value = k.v
            """,
            columns,
        )
        existing = {
            LinkRetryKey(*map(str, row[:5])): bool(row[5]) for row in cur.fetchall()
        }
        changed = [
            (r, r.entity_value in ready_values)
            for r in rows
            if existing.get(r) is not (r.entity_value in ready_values)
        ]
        if not changed:
            return
        # The WHERE keeps the no-rewrite rule even for a row a writer that
        # takes no stripe lock (the legacy Projector) changed since the read.
        # It is defensive only: every production writer of these rows takes
        # the stripe lock first, and no test exercises this branch.
        cur.execute(
            """
            INSERT INTO okg.link_retry
              (deployment_name, branch_name, node_id, entity_type,
               entity_value, ready_at)
            SELECT k.d, k.b, k.n, k.t, k.v,
                   CASE WHEN k.ready THEN now() ELSE NULL END
              FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[],
                          %s::text[], %s::bool[]) AS k(d, b, n, t, v, ready)
            ON CONFLICT
              (deployment_name, branch_name, node_id, entity_type,
               entity_value)
            DO UPDATE SET ready_at = EXCLUDED.ready_at, recorded_at = now()
             WHERE (okg.link_retry.ready_at IS NULL)
                   IS DISTINCT FROM (EXCLUDED.ready_at IS NULL)
            """,
            (
                [r.deployment_name for r, _ in changed],
                [r.branch_name for r, _ in changed],
                [r.node_id for r, _ in changed],
                [r.entity_type for r, _ in changed],
                [r.entity_value for r, _ in changed],
                [ready for _, ready in changed],
            ),
        )


def clear_unobserved_unready_link_retries(
    conn: psycopg.Connection,
    keys: Sequence[LinkRetryKey],
) -> int:
    """Delete the unready rows among ``keys``: rows of a node re-extracted
    now that the extraction did not record again (okg#3272).

    Such a row waits for an alias to land for a mention the node's current
    text no longer has, so it is deleted at once rather than after a
    published checkpoint: a publish that does not publish would otherwise
    keep it until the node next changes. A row woken in the meantime
    (``ready_at`` set) is left for the receipt."""
    return _delete_link_retry_keys(conn, keys, unready_only=True)


def acknowledge_link_retries(
    conn: psycopg.Connection,
    keys: Sequence[LinkRetryKey],
) -> int:
    """Delete exact retry receipts after a published Chronos checkpoint."""
    return _delete_link_retry_keys(conn, keys, unready_only=False)


def _delete_link_retry_keys(
    conn: psycopg.Connection,
    keys: Sequence[LinkRetryKey],
    *,
    unready_only: bool,
) -> int:
    """One set-based DELETE of exactly ``keys`` (optionally only unready)."""
    rows = sorted(set(keys))
    if not rows:
        return 0
    stripes = sorted({link_retry_alias_lock_stripe(r.entity_value) for r in rows})
    with conn.cursor() as cur:
        # The waker's stripe locks first, in its ascending order, so this
        # DELETE and a concurrent waker UPDATE of the same rows serialize
        # instead of deadlocking on row order.
        cur.execute(
            "SELECT pg_advisory_xact_lock(%s, stripe) FROM unnest(%s::int[]) AS stripe",
            (LINK_RETRY_ALIAS_LOCK_NAMESPACE, stripes),
        )
        cur.execute(
            """
            DELETE FROM okg.link_retry lr
             USING unnest(%s::text[], %s::text[], %s::text[], %s::text[],
                          %s::text[]) AS k(d, b, n, t, v)
             WHERE lr.deployment_name = k.d AND lr.branch_name = k.b
               AND lr.node_id = k.n AND lr.entity_type = k.t
               AND lr.entity_value = k.v
               AND (NOT %s OR lr.ready_at IS NULL)
            """,
            (
                [r.deployment_name for r in rows],
                [r.branch_name for r in rows],
                [r.node_id for r in rows],
                [r.entity_type for r in rows],
                [r.entity_value for r in rows],
                unready_only,
            ),
        )
        return cur.rowcount


def mark_link_retries_ready_for_aliases(
    conn: psycopg.Connection,
    alias_values: Iterable[str],
) -> int:
    """Wake retries affected by aliases that actually landed.

    ``link_retry`` rows remain durable.  The compatibility Projector still
    keys its skip policy from ``link_cache``, so invalidate cache rows for the
    same bounded affected-node set; production Publisher scheduling never
    depends on that side effect.
    """
    values = [value for value in dict.fromkeys(alias_values) if value]
    if not values:
        return 0
    # One lock per stripe the batch touches, at most
    # LINK_RETRY_ALIAS_LOCK_STRIPES, taken in ascending order: a function
    # scan over one array yields it in array order and evaluates the volatile
    # lock call per row (the only order such a scan produces, though not a
    # documented guarantee), so the sorted list is the acquisition order.
    stripes = sorted({link_retry_alias_lock_stripe(value) for value in values})
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_advisory_xact_lock(%s, stripe)
              FROM unnest(%s::int[]) AS stripe
            """,
            (LINK_RETRY_ALIAS_LOCK_NAMESPACE, stripes),
        )
        cur.execute(
            """
            WITH marked AS (
              UPDATE okg.link_retry
                 SET ready_at = now()
               WHERE entity_value = ANY(%s)
                 AND ready_at IS NULL
              RETURNING node_id
            )
            DELETE FROM okg.link_cache lc
             USING (SELECT DISTINCT node_id FROM marked) affected
             WHERE lc.node_id = affected.node_id
            """,
            (values,),
        )
        return cur.rowcount
