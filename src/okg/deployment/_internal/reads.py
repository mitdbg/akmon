"""Read helpers (Phase 2 §4.1).

Deployment workflow code reads substrate state through these helpers:

  * :func:`read_node` — pinpoint a single node at a generation.
  * :func:`query_at_generation` — arbitrary SELECT against the pin.
  * :func:`list_published_generations` — operator-style pagination.

Plus four "current generation" helpers that explain themselves at the
boundary they're called at:

  * :func:`publisher_generation` — only valid inside a substrate-
    internal publisher cycle. Raises elsewhere.
  * :func:`pinned_generation` — the generation set by the caller via
    :data:`okg.pin_generation` session GUC; falls back to the
    deployment's latest published.
  * :func:`latest_published_generation` — bare lookup over
    ``okg.graph_generations`` of the max ``status='published'``
    row for the deployment.
  * :func:`worker_write_generation` — the active worker-write
    generation (allocated on first call within a
    ``@deployment_transaction``).

The SDK's principle here is: read paths NEVER silently fall back to a
default generation. If the caller is outside any pin context, they
must say which generation they want.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .context import (
    MissingContextError,
    current_context,
    current_worker_write_generation,
)


__all__ = [
    "GenerationRow",
    "NodeRow",
    "latest_published_generation",
    "list_published_generations",
    "pinned_generation",
    "publisher_generation",
    "query_at_generation",
    "read_node",
    "worker_write_generation",
]


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NodeRow:
    """A single ``okg.graph_nodes`` row at a pinned generation."""

    node_id: str
    subtype: str
    attrs: dict[str, Any]


@dataclass(frozen=True)
class GenerationRow:
    """A row from ``okg.graph_generations`` (publisher-owned only)."""

    generation_id: int
    deployment_name: str
    status: str
    catalog_version_id: int
    correlation_id: str
    published_at: datetime | None
    created_at: datetime


# ---------------------------------------------------------------------------
# Single-node read
# ---------------------------------------------------------------------------


def read_node(
    node_id: str,
    *,
    pin: int | None = None,
) -> NodeRow | None:
    """Read a single node at the pinned generation.

    Parameters
    ----------
    node_id : str
        Node to fetch.
    pin : int | None
        Generation id to pin the read at. When None, defaults to the
        active pinned generation (publisher / worker-write / explicit
        pin), in that order; raises if no pin can be inferred.

    Returns
    -------
    NodeRow | None
        The node as it is visible at the pinned generation, read
        through a Chronos checkout. None if the node doesn't exist
        at that pin.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    ctx = current_context()
    pin_id = pin if pin is not None else _resolve_default_pin()
    graph = ChronosGraphStore.from_connection(
        ctx.conn, ensure_schema=False,
    ).checkout_generation(pin_id)
    rows = graph.query(
        """
        SELECT node_id, subtype, attrs
          FROM okg.graph_nodes
         WHERE node_id = :node_id
         LIMIT 1
        """,
        {"node_id": node_id},
    )
    if not rows:
        return None
    row = rows[0]
    return NodeRow(
        node_id=row["node_id"],
        subtype=row["subtype"],
        attrs=row["attrs"] or {},
    )


# ---------------------------------------------------------------------------
# Arbitrary SELECT under pin GUC
# ---------------------------------------------------------------------------


def query_at_generation(
    sql: str,
    params: tuple = (),
    *,
    pin: int | None = None,
) -> list[dict[str, Any]]:
    """Run an arbitrary SELECT pinned to ``pin``.

    Sets the Postgres session GUC ``okg.pin_generation`` for the
    duration of the query so substrate views / functions that consult
    the pin (e.g. the ``okg.v_*`` agent views built atop the GUC)
    see the right generation. Returns rows as a list of dicts keyed
    on column names.

    The SDK does not parse / validate the SQL — read-only enforcement
    is the role split's job (deployment role lacks DELETE / TRUNCATE
    on substrate tables per Phase 0 F4). Tests against a non-role-
    split DB should pass a deployment-friendly DSN.
    """
    ctx = current_context()
    pin_id = pin if pin is not None else _resolve_default_pin()

    with ctx.conn.cursor() as cur:
        # SET LOCAL doesn't accept parameter substitution, so use the
        # set_config() function form which does. is_local=true scopes
        # the GUC to the current (sub)transaction.
        cur.execute(
            "SELECT set_config('okg.pin_generation', %s, true)",
            (str(pin_id),),
        )
        cur.execute(sql, params)
        cols = [d.name for d in cur.description] if cur.description else []
        rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# Generation index
# ---------------------------------------------------------------------------


def list_published_generations(
    deployment: str,
    *,
    since_generation_id: int | None = None,
    limit: int = 100,
) -> list[GenerationRow]:
    """List published generations for a deployment, newest first.

    Parameters
    ----------
    deployment : str
        Deployment name to filter on.
    since_generation_id : int | None
        When set, returns generations strictly greater than this id.
    limit : int, default 100
        Maximum rows; clamped to [1, 10_000].
    """
    if limit < 1:
        limit = 1
    elif limit > 10_000:
        limit = 10_000

    ctx = current_context()
    with ctx.conn.cursor() as cur:
        if since_generation_id is None:
            cur.execute(
                """
                SELECT generation_id, deployment_name, status,
                       catalog_version_id, correlation_id,
                       published_at, created_at
                  FROM okg.graph_generations
                 WHERE deployment_name = %s
                   AND status = 'published'
                 ORDER BY generation_id DESC
                 LIMIT %s
                """,
                (deployment, limit),
            )
        else:
            cur.execute(
                """
                SELECT generation_id, deployment_name, status,
                       catalog_version_id, correlation_id,
                       published_at, created_at
                  FROM okg.graph_generations
                 WHERE deployment_name = %s
                   AND status = 'published'
                   AND generation_id > %s
                 ORDER BY generation_id DESC
                 LIMIT %s
                """,
                (deployment, since_generation_id, limit),
            )
        rows = cur.fetchall()

    return [
        GenerationRow(
            generation_id=r[0],
            deployment_name=r[1],
            status=r[2],
            catalog_version_id=r[3],
            correlation_id=r[4],
            published_at=r[5],
            created_at=r[6],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Generation context helpers (§4.1.4)
# ---------------------------------------------------------------------------


def publisher_generation() -> int:
    """Return the publisher's current pending generation.

    Only valid inside a substrate-internal publisher cycle context.
    Raises ``MissingContextError`` when called from a deployment
    workflow (deployments should use :func:`worker_write_generation`).
    """
    # The publisher's own code allocates a ReservedGeneration and
    # passes it through its phase methods. The SDK has no general way
    # to find that object from a deployment workflow — by design, per
    # Phase 0 F5. So we always raise here.
    raise MissingContextError(
        "publisher_generation() is only valid inside the substrate's "
        "publisher cycle; deployment workflows must use "
        "worker_write_generation() to allocate an out-of-cycle "
        "generation"
    )


def pinned_generation() -> int:
    """Return the generation pinned by the caller, falling back to the
    deployment's latest published generation.

    Read order:

      1. ``okg.pin_generation`` Postgres session GUC if set.
      2. :func:`latest_published_generation` for the active deployment.

    Only an UNSET (or empty) pin falls back to latest published. A pin
    that is set but cannot be interpreted as a generation raises
    :class:`okg.substrate.errors.GenerationPinInvalid` — serving the
    latest generation to a caller who pinned a specific one would be a
    silent wrong answer.

    Use this when a read helper's ``pin`` argument is None and the
    caller has no specific generation in mind — it does what most
    read paths want.
    """
    ctx = current_context()
    with ctx.conn.cursor() as cur:
        # current_setting(name, missing_ok) returns NULL when unset.
        cur.execute(
            "SELECT current_setting('okg.pin_generation', true)"
        )
        row = cur.fetchone()
        pin = row[0] if row else None
    if pin:
        try:
            return int(pin)
        except (ValueError, TypeError):
            # A caller who pinned a SPECIFIC generation must never get a
            # DIFFERENT one back because the pin failed to parse. Chronos
            # TEXT checkpoint refs land here until this path learns to
            # consume them (see views.sql `okg.current_pin()` SCOPE note).
            from okg.substrate.errors import GenerationPinInvalid

            raise GenerationPinInvalid(
                "okg.pin_generation is set to a value this read path "
                f"cannot interpret as a generation: {pin!r}",
                data={"guc": "okg.pin_generation", "value": pin},
                fix_hint=(
                    "set okg.pin_generation to an integer generation id, "
                    "or RESET it to read the latest published generation"
                ),
            )
    return latest_published_generation(ctx.deployment)


def latest_published_generation(deployment: str) -> int:
    """Return the max generation_id with ``status='published'`` for
    the given deployment.

    Raises
    ------
    MissingContextError
        If no published generations exist for the deployment yet —
        operators reading from a fresh deployment must pin explicitly
        or wait for the first publish to land.
    """
    ctx = current_context()
    with ctx.conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(generation_id) FROM okg.graph_generations
             WHERE deployment_name = %s AND status = 'published'
            """,
            (deployment,),
        )
        row = cur.fetchone()
    gen = row[0] if row else None
    if gen is None:
        raise MissingContextError(
            f"deployment {deployment!r} has no published generations; "
            "pin a generation explicitly, or wait for the first publish"
        )
    return int(gen)


def worker_write_generation() -> int:
    """Return the active worker-write generation_id.

    Allocates on first call within the active context (via
    :func:`current_worker_write_generation`); subsequent calls return
    the cached id so dual-mutation within one
    ``@deployment_transaction`` lands at the same generation.
    """
    return current_worker_write_generation()


# ---------------------------------------------------------------------------
# Internal default-pin resolution
# ---------------------------------------------------------------------------


def _resolve_default_pin() -> int:
    """Resolve a pin when the caller passes None.

    Order:
      1. Worker-write generation (active write context).
      2. ``okg.pin_generation`` GUC.
      3. Latest published.

    Raises ``MissingContextError`` if no generation can be inferred.
    """
    try:
        return current_worker_write_generation(allocate_if_missing=False)
    except MissingContextError:
        pass

    return pinned_generation()
