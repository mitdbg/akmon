"""Serialize ``ALTER ROLE`` on a role across every database of a cluster.

A role is one ``pg_authid`` row shared by every database on the server.
PostgreSQL's ``ALTER ROLE`` rewrites that row without first locking the
role, so two sessions altering the same role at once do not queue: the one
that loses fails with ``tuple concurrently updated`` (issue #1985). Two
deployments bootstrapping on one cluster, an environment-service install
beside a runtime bootstrap, or two test workers can all do that.

An advisory lock cannot serialize them: its key is scoped to the current
database, and these writers connect to different databases. PostgreSQL does
hold a cluster-wide lock on a role while ``COMMENT ON ROLE`` runs: the
statement takes the role's shared-object lock in ``SHARE UPDATE EXCLUSIVE``
mode, which conflicts with itself, and keeps it until the transaction ends.
:func:`lock_roles_for_alter` takes that lock by rewriting each role's comment
with the value it already has, so the role's visible state does not change.

Every writer of a role's ``pg_authid`` row must take the lock in the same
transaction as its ``ALTER ROLE``; a writer that skips it can still collide.
Taking the lock needs the same authority the ``ALTER ROLE`` itself needs
(superuser, or ``CREATEROLE`` with ``ADMIN OPTION`` on the role).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from psycopg import sql
from psycopg.pq import TransactionStatus


def lock_roles_for_alter(conn: Any, roles: Iterable[str]) -> None:
    """Hold each role's cluster-wide lock until the current transaction ends.

    Locks are taken in sorted name order, so two callers locking overlapping
    sets of roles queue rather than deadlock. A role that does not exist is
    refused with :class:`LookupError` rather than skipped: the caller is about
    to alter it.

    The caller must already be inside a transaction (a non-autocommit
    connection, or ``with conn.transaction():`` on an autocommit one), and
    must run its ``ALTER ROLE`` in that same transaction. On an autocommit
    connection outside a transaction each statement commits on its own, the
    lock would be released at once, and this raises instead of pretending.
    """
    if (
        conn.autocommit
        and conn.info.transaction_status != TransactionStatus.INTRANS
    ):
        raise RuntimeError(
            "lock_roles_for_alter needs an open transaction: on an autocommit "
            "connection the role lock would be released before ALTER ROLE runs"
        )
    for role in sorted(set(roles)):
        row = conn.execute(
            "SELECT pg_catalog.shobj_description(oid, 'pg_authid') "
            "FROM pg_catalog.pg_roles WHERE rolname = %s",
            (role,),
        ).fetchone()
        if row is None:
            raise LookupError(f"role {role!r} does not exist; cannot lock it")
        conn.execute(
            sql.SQL("COMMENT ON ROLE {} IS {}").format(
                sql.Identifier(role), sql.Literal(row[0]),
            )
        )


__all__ = ["lock_roles_for_alter"]
