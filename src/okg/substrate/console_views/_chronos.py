"""Shared chronos read helpers for the console_views composers.

Post `chronos-native-graph-cutover`, the graph lives in okg.graph_nodes /
okg.graph_edges behind a Chronos branch checkout — the legacy okg.nodes_live /
okg.edges_live tables are dropped, and their bare okg.v_* passthroughs are empty.
Every console composer must therefore read through a generation-pinned Chronos
checkout (the same IR read path okg.viz.reads and the AKMON operators use), not raw
SQL on the dropped base tables.

These helpers centralize that so each composer migrates uniformly:

    from okg.substrate.console_views._chronos import checkout
    g = checkout(conn, session)
    rows = g.query("SELECT node_id, subtype, attrs FROM okg.graph_nodes WHERE ...", {...})

`session` is the viz request's pinned Session (ctx.session); its generation_id is a
Chronos checkpoint id (a string, e.g. "okg:okg-workspace:branch:default:gen:..."),
NOT the legacy integer — so composers must take the Session, never a bare int.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.graph_store_internal.read_branch import (
    CheckpointMetadataUnreadable,
    checkpoint_metadata_unreadable,
    is_invalid_json_cast,
)
from okg.substrate.mcp.session import Session, assert_session_matches

logger = logging.getLogger(__name__)


class GenerationCountsUnavailable(SubstrateError):
    """The pinned generation's checkpoint does not carry trustworthy node/edge
    counts (checkpoint missing, not the pinned published generation, or counts
    not stamped). User-facing request paths MUST fail loud on this — NEVER fall
    through to a raw live recompute, stale value, default, or zero. A fast wrong
    number is worse than a loud unavailable state."""


def force_custom_plan_for_pinned_read(conn: Any) -> None:
    """Keep rotating-floor partial indexes usable for this request transaction.

    Console reads use the deployment-owner connection, which is shared with
    publisher work and therefore must never receive a *role-wide* setting.
    Transactional callers use ``SET LOCAL``. Console pools intentionally use
    autocommit, so their dedicated connection receives a session setting; it
    persists only on that console connection and cannot affect the publisher's
    separate connection.
    """
    if bool(getattr(conn, "autocommit", False)):
        conn.execute("SET plan_cache_mode = force_custom_plan")
    else:
        conn.execute("SET LOCAL plan_cache_mode = force_custom_plan")


#: The metadata keys a count read needs. Every count guard below reads these
#: three and nothing else, so a caller holding a projected row can apply the
#: same guards without the rest of the checkpoint document.
GENERATION_COUNT_METADATA_KEYS: tuple[str, ...] = ("status", "node_count", "edge_count")

#: Every key `read_checkpoint_fields` may project. A closed list, because the
#: names are written into the statement: a key outside it is refused rather
#: than interpolated. Each is small on every checkpoint okg-workspace holds
#: (under 500 bytes); the document around them can be 62 MB (issue #2546).
CHECKPOINT_FIELD_KEYS: tuple[str, ...] = (
    "kind",
    "deployment_name",
    "branch",
    "status",
    "node_count",
    "edge_count",
    "catalog_version_id",
    "target_generation_id",
    "publisher_configuration",
    "first_publish_budget_warning",
    "nodes_live",
    "edges_live",
    "total_duration_ms",
    "excluded_sources",
)

#: Objects `read_checkpoint_fields` may project INTO, and the keys it may take
#: from each. `invariant_summary` is where the 59 MB node-id list lives
#: (`frontier_node_ids`), so it is never projected whole: only these keys
#: leave the database.
CHECKPOINT_NESTED_FIELD_KEYS: dict[str, tuple[str, ...]] = {
    "invariant_summary": (
        "cross_source",
        "source_local",
        "substrate_integrity",
        "total_duration_ms",
        "total_budget_exceeded",
    ),
    "projection_deltas": (
        "nodes_added",
        "nodes_retracted",
        "edges_added",
        "edges_retracted",
    ),
}

def _unknown_field_keys(
    keys: tuple[str, ...], nested: dict[str, tuple[str, ...]],
) -> list[str]:
    unknown = [key for key in keys if key not in CHECKPOINT_FIELD_KEYS]
    for outer, inner in nested.items():
        allowed = CHECKPOINT_NESTED_FIELD_KEYS.get(outer)
        if allowed is None:
            unknown.append(outer)
            continue
        unknown += [f"{outer}.{key}" for key in inner if key not in allowed]
    return unknown


def read_checkpoint_fields(
    conn: Any,
    checkpoint_id: str,
    keys: tuple[str, ...],
    nested: dict[str, tuple[str, ...]] | None = None,
) -> tuple[Any, dict[str, Any]] | None:
    """``(created_at, {key: value})`` for ONE checkpoint, or None when absent.

    One statement that returns only the named metadata keys. The whole
    document never leaves the database: ``get_generation`` runs ``SELECT *``
    and parses all of it, and a checkpoint on okg-workspace carries 62 MB of
    metadata, 59 MB of it a list of every node id (issue #2546), to answer a
    few small fields. PostgreSQL still parses the document once, server side;
    what moves over the wire is the projection. ``keys=()`` reads the
    ``created_at`` column alone.

    ``nested`` names objects inside the metadata and the keys to take from
    each (`CHECKPOINT_NESTED_FIELD_KEYS`). For each, the result holds a dict of
    the keys that object actually carries -- a key it lacks is absent, not
    None, so ``.get(key, default)`` behaves as on the full document -- or None
    when the metadata holds no object there.

    Values arrive as the Python objects ``json.loads`` would have produced
    (the fields are projected as ``json``), so an integer count is an integer
    here as it was on the full read. This checks no compaction tombstone;
    callers apply the same guards `get_generation` applies. Metadata that
    PostgreSQL cannot parse as JSON raises `CheckpointMetadataUnreadable`,
    never None.
    """
    nested = dict(nested or {})
    unknown = _unknown_field_keys(keys, nested)
    if unknown:
        raise SubstrateError(
            "read_checkpoint_fields was asked for keys outside its contract",
            data={
                "unknown": sorted(unknown),
                "allowed": list(CHECKPOINT_FIELD_KEYS),
                "allowed_nested": {
                    outer: list(inner)
                    for outer, inner in CHECKPOINT_NESTED_FIELD_KEYS.items()
                },
            },
        )
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    if keys or nested:
        columns = [f'm."{key}"' for key in keys]
        declared = [f'"{key}" json' for key in keys]
        joins: list[str] = []
        for index, (outer, inner) in enumerate(nested.items()):
            alias = f"n{index}"
            declared.append(f'"{outer}" json')
            columns.append(
                f"coalesce(json_typeof(m.\"{outer}\") = 'object', false)"
                f' AS "{alias}__object"'
            )
            # Text, not json: an absent key (SQL NULL) stays distinct from a
            # JSON null ('null').
            columns += [
                f'{alias}."{key}"::text AS "{alias}__{key}"' for key in inner
            ]
            inner_declared = ", ".join(f'"{key}" json' for key in inner)
            joins.append(
                "LEFT JOIN LATERAL json_to_record(CASE WHEN json_typeof("
                f"m.\"{outer}\") = 'object' THEN m.\"{outer}\" END)"
                f" AS {alias}({inner_declared}) ON true"
            )
        # LEFT JOIN: a checkpoint whose metadata is SQL NULL is still found.
        sql = f"""
            SELECT t.created_at, {", ".join(columns)}
              FROM _chronos_branch_interval_checkpoints AS t
              LEFT JOIN LATERAL json_to_record(t.metadata::json)
                   AS m({", ".join(declared)}) ON true
              {" ".join(joins)}
             WHERE t.checkpoint_id = ?
            """
    else:
        sql = (
            "SELECT created_at FROM _chronos_branch_interval_checkpoints"
            " WHERE checkpoint_id = ?"
        )
    try:
        row = store.context.db.execute(sql, (checkpoint_id,)).fetchone()
    except Exception as exc:
        if is_invalid_json_cast(exc):
            raise checkpoint_metadata_unreadable(checkpoint_id, exc) from exc
        raise
    if row is None:
        return None
    fields: dict[str, Any] = {key: row[key] for key in keys}
    for index, (outer, inner) in enumerate(nested.items()):
        alias = f"n{index}"
        if not row[f"{alias}__object"]:
            fields[outer] = None
            continue
        fields[outer] = {
            key: json.loads(row[f"{alias}__{key}"])
            for key in inner
            if row[f"{alias}__{key}"] is not None
        }
    return row["created_at"], fields


def read_generation_fields(
    conn: Any,
    generation_id: int | str,
    keys: tuple[str, ...],
    nested: dict[str, tuple[str, ...]] | None = None,
) -> tuple[str, Any, dict[str, Any]]:
    """``(checkpoint_id, created_at, fields)`` for one generation, fail loud.

    `read_checkpoint_fields` behind `get_generation`'s guards: a compacted
    checkpoint raises `GenerationCompacted` (checked before and after the
    read, so one retired mid-read still reports as compacted), and a missing
    one raises `BranchNotFoundError`. Unparseable metadata raises
    `CheckpointMetadataUnreadable`.
    """
    from chronos_core.branching import BranchNotFoundError
    from okg.substrate.history_read_guard import raise_if_compacted

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    db = store.context.db
    checkpoint_id = store._generation_checkpoint_id(generation_id)
    raise_if_compacted(db, checkpoint_id)
    try:
        found = read_checkpoint_fields(conn, checkpoint_id, keys, nested)
    except CheckpointMetadataUnreadable:
        raise
    except Exception:
        raise_if_compacted(db, checkpoint_id)
        raise
    raise_if_compacted(db, checkpoint_id)
    if found is None:
        raise BranchNotFoundError(f"checkpoint:{checkpoint_id}")
    return checkpoint_id, found[0], found[1]


def _raise_counts_unavailable_if_compacted(conn: Any, checkpoint_id: str) -> None:
    """The compaction guard `get_generation` applies, in the counts' own terms."""
    from okg.substrate.history_read_guard import raise_if_compacted

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    try:
        raise_if_compacted(store.context.db, checkpoint_id)
    except Exception as exc:  # noqa: BLE001 - compacted/unreadable => loud
        raise GenerationCountsUnavailable(
            "pinned generation checkpoint could not be resolved",
            data={"generation_id": checkpoint_id},
        ) from exc


def counts_from_projected_metadata(
    conn: Any, checkpoint_id: str, metadata: dict[str, Any],
) -> tuple[int, int]:
    """``(node_count, edge_count)`` from a checkpoint's projected count keys.

    The guards ``generation_counts`` has always applied, on the three keys in
    ``GENERATION_COUNT_METADATA_KEYS`` instead of the whole document: a
    compacted generation, a non-published status, missing counts and
    non-integer counts each raise ``GenerationCountsUnavailable`` with the
    reason the page prints. Nothing here defaults a count.
    """
    _raise_counts_unavailable_if_compacted(conn, checkpoint_id)
    # Status guard: a non-published generation must not serve counts. (Absent
    # status is tolerated for older stamps that still carry counts.)
    status = metadata.get("status")
    if status is not None and status != "published":
        raise GenerationCountsUnavailable(
            "pinned generation is not published",
            data={"generation_id": checkpoint_id, "status": status},
        )
    node_count = metadata.get("node_count")
    edge_count = metadata.get("edge_count")
    if node_count is None or edge_count is None:
        raise GenerationCountsUnavailable(
            "pinned generation checkpoint has no stamped node/edge counts",
            data={"generation_id": checkpoint_id},
        )
    try:
        return int(node_count), int(edge_count)
    except (TypeError, ValueError) as exc:
        raise GenerationCountsUnavailable(
            "stamped node/edge counts are not integers",
            data={"node_count": node_count, "edge_count": edge_count},
        ) from exc


def generation_counts(conn: Any, session: Session) -> tuple[int, int]:
    """``(node_count, edge_count)`` for the session's pinned generation, read
    O(1) from the generation CHECKPOINT METADATA the publisher stamps — NOT a
    ``count(*)`` / ``count(DISTINCT)`` scan of the (multi-GB, bloated) effective
    view — and read as a PROJECTION of the three keys it needs, not the whole
    metadata document (issue #2546).

    Fails loud (``GenerationCountsUnavailable``) when the checkpoint can't be
    resolved (missing or compacted), the generation isn't published, or the
    counts aren't stamped (pre-stamp generations). User-facing request paths
    let this propagate as a loud unavailable state. They must NEVER substitute
    a raw scan or default value (the P0).
    """
    assert_session_matches(conn, session)
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    pinned = store._generation_checkpoint_id(session.generation_id)
    _raise_counts_unavailable_if_compacted(conn, pinned)
    try:
        found = read_checkpoint_fields(conn, pinned, GENERATION_COUNT_METADATA_KEYS)
    except CheckpointMetadataUnreadable as exc:
        raise GenerationCountsUnavailable(
            "pinned generation checkpoint metadata is not readable JSON",
            data={"generation_id": pinned},
        ) from exc
    except Exception as exc:  # noqa: BLE001 - unresolvable checkpoint => loud
        raise GenerationCountsUnavailable(
            "pinned generation checkpoint could not be resolved",
            data={"generation_id": pinned},
        ) from exc
    if found is None:
        # The full read raised `BranchNotFoundError` here, and the page
        # printed "could not be resolved"; the reason is kept.
        raise GenerationCountsUnavailable(
            "pinned generation checkpoint could not be resolved",
            data={"generation_id": pinned},
        ) from LookupError(f"checkpoint:{pinned}")
    # The statement selects by checkpoint id, so the row IS the pinned
    # generation: the identity guard the full read needed holds by construction.
    return counts_from_projected_metadata(conn, pinned, found[1])


def checkout(conn: Any, session: Session):
    """A Chronos checkout pinned to the session's published generation.

    Returns a branch session whose ``.query(sql, params)`` reads
    okg.graph_nodes / okg.graph_edges at the pinned generation. Use ``:name``
    bind params (Chronos style) and inline integer LIMITs."""
    assert_session_matches(conn, session)
    force_custom_plan_for_pinned_read(conn)
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    return store.checkout_generation(
        session.generation_id, branch=session.branch_name,
    )


def generation_published_at(conn: Any, session: Session):
    """The published timestamp of the session's Chronos generation checkpoint,
    failing loudly if it can't be resolved. Replaces the legacy
    ``SELECT published_at FROM okg.graph_generations WHERE generation_id = <int>``
    lookup, which fails on Chronos (the generation id is a string checkpoint id,
    not a bigint).

    Reads the ``created_at`` COLUMN and nothing else. It used to call
    ``get_generation``, which moves the whole metadata document -- 8.7 MB for
    okg-workspace's pinned generation (issue #2546) -- to read one column that
    is not in the document at all. The guards are `get_generation`'s: a
    compacted checkpoint raises `GenerationCompacted`, a missing one
    `BranchNotFoundError`.
    """
    try:
        _checkpoint_id, at, _fields = read_generation_fields(
            conn, session.generation_id, (),
        )
    except Exception:
        logger.exception(
            "failed to resolve Chronos generation timestamp: generation_id=%s",
            session.generation_id,
        )
        raise
    # Chronos checkpoints stamp created_at as an ISO string; callers expect a
    # datetime (freshness math, _humanize_age). Coerce.
    if isinstance(at, str):
        try:
            return datetime.fromisoformat(at)
        except ValueError:
            return None
    return at
