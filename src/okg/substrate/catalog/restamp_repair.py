"""Return nodes the old catalog restamp took over to their source (#3318).

Until issue #3318, a catalog load that changed a subtype's searchable-text
declaration re-applied every stored node of that subtype under the system
source ``_catalog:restamp``. Each rewritten row lost its source, record id,
revision, run id and lineage. Completed-scope retraction deletes only rows
the retracting source owns, so the source that wrote such a node can no
longer retract it.

The restamp no longer does that. This module repairs the rows it already
took over. For each node whose visible row is owned by ``_catalog:restamp``
it reads the branch history for the row version the restamp replaced: the
version whose interval ends exactly where the restamp's version begins
(``live_hi`` equal to its ``live_lo``). A replaced version that was itself a
restamp is followed back the same way. The first version owned by another
source names the original owner, and its record id, revision, run id,
observed time and lineage are written back. The current attrs, including
the restamped searchable text, are kept.

A row is left alone and reported when the history cannot name its owner:

- ``no_earlier_version``: the restamp overwrote the source's version in
  place (both were written in the same branch segment, with no published
  generation between them), history compaction removed it, or the node
  table was rebound to a new physical table since (schema branching); the
  repair reads only the physical table the branch head uses.
- ``ambiguous_earlier_version``: more than one version ends at that point.
- ``earlier_version_deleted`` / ``earlier_version_unattributed``: the
  version found is a deletion marker or carries no source.
- ``earlier_version_no_lineage``: the version found carries no source
  lineage, so the repair would have to invent one.

Known limit: when the source's newest version was overwritten in place but
an older version survives, the older version is the one found. Its source is
the owner unless the node changed owner between the two versions, which the
history cannot show; its record id and revision may be older than the
newest ones, and the source's next emission of the node replaces them.

Repairing ownership is not enough for a node the source stopped emitting
while the restamp owned it (review B1 of #3318). A normal run retracts by
diffing against the record set it stored last time, and that record set no
longer names the node, so the node stays published. Only a run that also
sweeps the graph for the source's stray rows retracts it: a reset-cursor
run of that source alone (``okg ingest --deployment <d> --include <source>
--no-predecessors --inline --reset-cursor``; without ``--no-predecessors``
the source's predecessors would be selected and reset too). The report lists
such nodes per source under ``pending_retraction`` (repaired nodes absent
from that source's stored record set, compared by node id as stored) and
prints that command. A source with no stored record set, or whose record
set or its table cannot be read, is listed under
``pending_retraction_unchecked`` with no command: it needs inspection. The
repair never retracts on a source's behalf. The record set is read with
SELECTs only; the source runner's own loaders would create missing tables.

A dry run writes no graph rows, takes no branch lock and runs no DDL (the
command also rolls its transaction back). With ``apply`` the repair takes
the branch write lock before reading, so no graph writer can change a row
between the read and the rewrite. It is idempotent: a repaired row is no
longer owned by ``_catalog:restamp`` and is not found again.
"""
from __future__ import annotations

import json
from collections import Counter
from typing import Any, Mapping

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store_internal.common import (
    DEFAULT_BRANCH,
    NODE_TABLE,
    quote_physical_table,
)

RESTAMP_SOURCE = "_catalog:restamp"
REPAIR_PRODUCER_ID = "_catalog:restamp_ownership_repair"
REPAIR_INTENT = "catalog_restamp_ownership_repair"
REPORT_SCHEMA = "okg.catalog.restamp_ownership_repair.v1"

# A chain longer than this means repeated restamps of one node; far beyond
# any real catalog history, so a longer one is reported, not followed.
_MAX_CHAIN = 64
_PAGE = 1000


def _json_value(value: Any) -> Any:
    if isinstance(value, (str, bytes, bytearray)):
        return json.loads(value)
    return value


def _json_object(value: Any) -> dict[str, Any]:
    value = _json_value(value)
    return dict(value) if isinstance(value, Mapping) else {}


def _physical_node_table(store: Any, branch: str) -> tuple[str, Any]:
    """The node table's physical name and the branch head's read point."""
    backend = store.context._backend
    if getattr(backend, "name", None) != "interval":
        raise SubstrateError(
            "the restamp ownership repair reads Chronos interval history",
            data={"backend": getattr(backend, "name", None)},
            fix_hint="run it against a PostgreSQL deployment database",
        )
    head = backend._segment(backend._branch_segment_id(branch))
    meta = backend._meta_for_segment(head, NODE_TABLE)
    return quote_physical_table(meta.physical_name), head.branch_point


def _restamp_owned_rows(
    db: Any, table: str, point: Any,
) -> list[dict[str, Any]]:
    rows = db.execute(
        f"SELECT node_id, subtype, attrs, live_lo FROM {table} "
        "WHERE source = ? AND live_lo <= ? AND ? < live_hi "
        "AND deleted = FALSE ORDER BY node_id",
        (RESTAMP_SOURCE, point, point),
    ).fetchall()
    return [
        {
            "node_id": str(row["node_id"]),
            "subtype": str(row["subtype"]),
            "attrs": _json_object(row["attrs"]),
            "live_lo": row["live_lo"],
        }
        for row in rows
    ]


def _versions_by_end(
    db: Any, table: str, node_ids: list[str],
) -> dict[str, dict[Any, list[dict[str, Any]]]]:
    """Every stored version of each node, keyed by where its interval ends."""
    out: dict[str, dict[Any, list[dict[str, Any]]]] = {}
    for start in range(0, len(node_ids), _PAGE):
        page = node_ids[start:start + _PAGE]
        for row in db.execute(
            "SELECT node_id, source, source_record_id, source_revision, "
            f"run_id, observed_at, attrs, live_lo, live_hi, deleted FROM {table} "
            "WHERE node_id = ANY(?)",
            (page,),
        ).fetchall():
            out.setdefault(str(row["node_id"]), {}).setdefault(
                row["live_hi"], [],
            ).append(dict(row))
    return out


def _original_version(
    row: Mapping[str, Any],
    versions: Mapping[Any, list[dict[str, Any]]],
) -> tuple[dict[str, Any] | None, str | None]:
    """Follow replaced versions back to the first one another source owns."""
    begins = row["live_lo"]
    for _ in range(_MAX_CHAIN):
        replaced = versions.get(begins, [])
        if not replaced:
            return None, "no_earlier_version"
        if len(replaced) > 1:
            return None, "ambiguous_earlier_version"
        version = replaced[0]
        if version["deleted"]:
            return None, "earlier_version_deleted"
        source = str(version["source"] or "").strip()
        if not source:
            return None, "earlier_version_unattributed"
        if source != RESTAMP_SOURCE:
            return version, None
        begins = version["live_lo"]
    return None, "restamp_chain_too_long"


def _relation_exists(conn: Any, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (name,))
        return cur.fetchone()[0] is not None


def _stored_record_set_raw(conn: Any, source: str) -> tuple[Any, str | None]:
    """The stored record-set payload of ``source``, read-only (review S5).

    Mirrors where the source runner keeps it -- inline in the watermark
    cursor, in a cursor blob, in ``okg.source_record_sets`` or in
    ``okg.source_record_set_blobs`` (whole or in pieces) -- but runs only
    SELECTs: the runner's loaders create missing tables, which would take a
    table lock. A missing table is reported, never created.
    """
    from okg.substrate.ingest.record_set_pieces import reassemble_pieces
    from okg.substrate.ingest.runner import (
        _CURSOR_BLOB_REF_SCHEMA,
        _CURSOR_BLOB_STORAGE,
        _RECORD_SET_REF_KEY,
    )

    if not _relation_exists(conn, "okg.source_watermarks"):
        return None, "no_record_set_table"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks "
            " WHERE source = %s",
            (source,),
        )
        row = cur.fetchone()
    cursor = row[0] if row else None
    if not isinstance(cursor, Mapping):
        return None, "no_stored_record_set"
    raw = cursor.get("record_set")
    if (
        isinstance(raw, Mapping)
        and raw.get("schema") == _CURSOR_BLOB_REF_SCHEMA
        and raw.get("storage") == _CURSOR_BLOB_STORAGE
    ):
        if not _relation_exists(conn, "okg.source_cursor_blobs"):
            return None, "no_record_set_table"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM okg.source_cursor_blobs "
                " WHERE source = %s AND cursor_key = %s AND sha256 = %s",
                (source, str(raw.get("key") or "record_set"),
                 str(raw.get("sha256") or "")),
            )
            found = cur.fetchone()
        if found is None:
            return None, "record_set_unreadable: cursor blob missing"
        return found[0], None
    if raw is not None:
        return raw, None
    ref = cursor.get(_RECORD_SET_REF_KEY)
    if not isinstance(ref, Mapping):
        return None, "no_stored_record_set"
    if str(ref.get("storage") or "") == "okg.source_record_sets":
        if not _relation_exists(conn, "okg.source_record_sets"):
            return None, "no_record_set_table"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT record_set FROM okg.source_record_sets "
                " WHERE source = %s",
                (source,),
            )
            found = cur.fetchone()
        if found is None:
            return None, "record_set_unreadable: row missing"
        return found[0], None
    if not _relation_exists(conn, "okg.source_record_set_blobs"):
        return None, "no_record_set_table"
    digest = str(ref.get("sha256") or "")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT record_set, record_set_text, piece_write_id, "
            "       piece_count, piece_bytes "
            "  FROM okg.source_record_set_blobs "
            " WHERE source = %s AND sha256 = %s",
            (source, digest),
        )
        found = cur.fetchone()
    if found is None:
        return None, "record_set_unreadable: blob missing"
    record_set, text, write_id, piece_count, piece_bytes = found
    if write_id is None:
        return (json.loads(str(text)) if text else record_set), None
    if not _relation_exists(conn, "okg.source_record_set_blob_pieces"):
        return None, "no_record_set_table"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT piece_no, piece FROM okg.source_record_set_blob_pieces "
            " WHERE write_id = %s AND source = %s ORDER BY piece_no",
            (write_id, source),
        )
        pieces = [(int(no), str(piece)) for no, piece in cur.fetchall()]
    try:
        joined = reassemble_pieces(
            pieces, source=source, sha256=digest,
            piece_count=int(piece_count), piece_bytes=int(piece_bytes),
        )
    except SubstrateError as exc:
        return None, f"record_set_unreadable: {exc}"
    return json.loads(joined), None


def _stored_record_set_node_ids(
    conn: Any, source: str,
) -> tuple[set[str] | None, str | None]:
    """The node ids in ``source``'s stored record set, or why there are none."""
    from okg.substrate.library.sources.base import RecordSet

    raw, why = _stored_record_set_raw(conn, source)
    if raw is None:
        return None, why or "no_stored_record_set"
    try:
        record_set = RecordSet.from_cursor(raw)
    except ValueError as exc:
        return None, f"record_set_unreadable: {exc}"
    node_ids: set[str] = set()
    for emission in record_set.records.values():
        node_ids.update(emission.nodes_by_id())
    return node_ids, None


def _pending_retraction(
    conn: Any,
    resolved: list[tuple[dict[str, Any], dict[str, Any]]],
    *,
    sample_limit: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    by_source: dict[str, list[str]] = {}
    for row, version in resolved:
        by_source.setdefault(str(version["source"]), []).append(row["node_id"])
    pending: dict[str, Any] = {}
    unchecked: dict[str, Any] = {}
    for source, node_ids in sorted(by_source.items()):
        stored, why = _stored_record_set_node_ids(conn, source)
        if stored is None:
            unchecked[source] = {"count": len(node_ids), "reason": why}
            continue
        absent = sorted(node_id for node_id in node_ids if node_id not in stored)
        if absent:
            pending[source] = {
                "count": len(absent),
                "node_ids": absent[:sample_limit],
            }
    return pending, unchecked


def follow_up_command(deployment_name: str, source: str) -> str:
    """The run that retracts a source's stray rows (see module docstring)."""
    return (
        f"okg ingest --deployment {deployment_name} --include {source} "
        "--no-predecessors --inline --reset-cursor"
    )


def repair_restamp_ownership(
    conn: Any,
    *,
    deployment_name: str,
    branch: str = DEFAULT_BRANCH,
    apply: bool = False,
    sample_limit: int = 20,
) -> dict[str, Any]:
    """Report, and with ``apply`` repair, rows owned by ``_catalog:restamp``.

    ``conn`` is a psycopg connection in a transaction; the caller commits
    (after ``apply``) or rolls back. Returns a report with counts per
    original source, per unresolved reason, and the nodes each source must
    still retract. Rows whose original source cannot be determined are
    never written.
    """
    from okg.substrate.fact_application import apply_bounded_fact_batch
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

    store = ChronosGraphStore.from_connection(
        conn, ensure_schema=False, autocommit=False,
    )
    db = store.context.db
    if apply:
        # Held to the end of the caller's transaction, so the rows read below
        # are the rows rewritten (transaction-scoped and re-entrant). A dry
        # run takes no lock and runs only SELECTs (review S5).
        store._lock_branch_write(branch)
    table, point = _physical_node_table(store, branch)
    owned = _restamp_owned_rows(db, table, point)
    history = _versions_by_end(db, table, [row["node_id"] for row in owned])

    resolved: list[tuple[dict[str, Any], dict[str, Any]]] = []
    unresolved_by_reason: Counter[str] = Counter()
    unresolved_samples: list[dict[str, str]] = []
    for row in owned:
        version, reason = _original_version(
            row, history.get(row["node_id"], {}),
        )
        if version is not None and not isinstance(
            _json_object(version["attrs"]).get(LINEAGE_ATTR), Mapping,
        ):
            version, reason = None, "earlier_version_no_lineage"
        if version is None:
            unresolved_by_reason[str(reason)] += 1
            if len(unresolved_samples) < sample_limit:
                unresolved_samples.append({
                    "node_id": row["node_id"],
                    "subtype": row["subtype"],
                    "reason": str(reason),
                })
            continue
        resolved.append((row, version))

    resolved_by_source = Counter(
        str(version["source"]) for _row, version in resolved
    )
    pending, unchecked = _pending_retraction(
        conn, resolved, sample_limit=sample_limit,
    )
    report: dict[str, Any] = {
        "schema": REPORT_SCHEMA,
        "mode": "apply" if apply else "dry_run",
        "deployment": deployment_name,
        "branch": branch,
        "restamp_owned": len(owned),
        "resolved": len(resolved),
        "resolved_by_source": dict(sorted(resolved_by_source.items())),
        "unresolved": sum(unresolved_by_reason.values()),
        "unresolved_by_reason": dict(sorted(unresolved_by_reason.items())),
        "unresolved_samples": unresolved_samples,
        "repaired": 0,
        "repaired_by_source": {},
        "admission_id": None,
        "pending_retraction": pending,
        "pending_retraction_unchecked": unchecked,
        "follow_up": [
            follow_up_command(deployment_name, source)
            for source in sorted(pending)
        ],
    }
    if not apply or not resolved:
        return report

    cache: dict[str, list[str]] = {}
    facts = []
    row_provenance: dict[str, dict[str, Any]] = {}
    for row, version in resolved:
        source = str(version["source"])
        record_id = _json_object(version["source_record_id"])
        revision = _json_object(version["source_revision"])
        lineage = _json_object(version["attrs"])[LINEAGE_ATTR]
        facts.append(NodeFact(
            row["node_id"],
            row["subtype"],
            store._unstamp_stored_attrs(
                row["subtype"], dict(row["attrs"]), cache=cache,
            ),
            record_id,
            revision,
        ))
        row_provenance[row["node_id"]] = {
            "source": source,
            "run_id": version["run_id"],
            "observed_at": version["observed_at"],
            "lineage": lineage,
            "expected_source": RESTAMP_SOURCE,
        }
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            " WHERE status = 'applied'"
        )
        catalog_version_id = cur.fetchone()[0]
    result = apply_bounded_fact_batch(
        conn,
        store,
        facts,
        deployment_name=deployment_name,
        producer_id=REPAIR_PRODUCER_ID,
        producer_kind="system",
        intent=REPAIR_INTENT,
        catalog_version_id=catalog_version_id,
        authority_scope={
            "repair": "restamp_ownership",
            "branch": branch,
            "node_count": len(facts),
        },
        target_branch=branch,
        strict_endpoints=True,
        row_provenance=row_provenance,
    )
    repaired = result.primitive_total("nodes_upserted")
    if repaired != len(facts):
        # Every resolved row changes owner, so the writer can drop none of
        # them as unchanged. A different count means rows were not written
        # as read; the caller must not commit a partial repair.
        raise SubstrateError(
            "the restamp ownership repair wrote a different number of rows "
            "than it resolved",
            data={"resolved": len(facts), "written": repaired},
            fix_hint="nothing was committed; rerun the repair and report it",
        )
    report["repaired"] = repaired
    report["repaired_by_source"] = report["resolved_by_source"]
    report["admission_id"] = str(result.admission_id)
    return report


__all__ = [
    "REPAIR_PRODUCER_ID",
    "follow_up_command",
    "RESTAMP_SOURCE",
    "repair_restamp_ownership",
]
