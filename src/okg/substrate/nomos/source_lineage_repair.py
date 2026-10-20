"""Repair helpers for Nomos source-lineage rollout cutover."""
from __future__ import annotations

from typing import Any, Mapping

from okg.substrate.db.unit_of_work import (
    note_unconfirmed_rollback,
    retry_after_rollback,
    rollback_confirmed,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.nomos.read_class import (
    READ_CLASS_COLUMN,
    READ_CLASS_VERSION_COLUMN,
    graph_carries_read_class,
    stamp_row,
    stamp_rows,
    table_kind,
)
from okg.substrate.nomos.vocabulary import READ_BOUNDARY_RESOURCE_KINDS
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    attrs_with_lineage,
    combine_source_lineages,
    lineage_for_row,
    normalize_source_lineage,
    source_ids_from_lineage,
)

PUBLISHER_DERIVED_SOURCE = "_publisher_derived"
_RETRY_LABEL = "source-lineage repair"
VIEW_REGISTRY_SOURCE = "catalog:views_registry"


def restamp_active_policy_lineage_fingerprints(
    conn,
    *,
    deployment_name: str,
    branch: str = "default",
    scope: str | None = None,
    policy_fingerprint: str | None = None,
    batch_size: int = 5000,
) -> dict[str, Any]:
    """Append the active policy fingerprint to already-valid source lineage.

    This is deliberately narrower than lineage recovery. It only touches rows
    whose lineage is already ``status: ok`` with at least one source id. Missing
    or malformed lineage remains fail-closed for the readiness doctor.
    """

    resolved_scope = scope or f"deployment:{deployment_name}"
    active_policy = _active_policy_bundle(
        conn,
        deployment_name=deployment_name,
        scope=resolved_scope,
    )
    if active_policy is None:
        return {
            "schema": "okg.nomos.source_lineage_policy_fingerprint_repair.v1",
            "ok": False,
            "deployment_name": deployment_name,
            "branch": branch,
            "scope": resolved_scope,
            "reason": "active_policy_missing",
            "nodes": _empty_policy_fingerprint_repair_result(),
            "edges": _empty_policy_fingerprint_repair_result(),
        }
    active_fingerprint = str(active_policy.policy_fingerprint or "")
    if not active_fingerprint:
        return {
            "schema": "okg.nomos.source_lineage_policy_fingerprint_repair.v1",
            "ok": False,
            "deployment_name": deployment_name,
            "branch": branch,
            "scope": resolved_scope,
            "reason": "active_policy_fingerprint_missing",
            "nodes": _empty_policy_fingerprint_repair_result(),
            "edges": _empty_policy_fingerprint_repair_result(),
        }
    if policy_fingerprint and policy_fingerprint != active_fingerprint:
        return {
            "schema": "okg.nomos.source_lineage_policy_fingerprint_repair.v1",
            "ok": False,
            "deployment_name": deployment_name,
            "branch": branch,
            "scope": resolved_scope,
            "reason": "active_policy_fingerprint_mismatch",
            "active_policy_fingerprint": active_fingerprint,
            "requested_policy_fingerprint": policy_fingerprint,
            "nodes": _empty_policy_fingerprint_repair_result(),
            "edges": _empty_policy_fingerprint_repair_result(),
        }
    fingerprint = policy_fingerprint or active_fingerprint
    batch_size = max(1, int(batch_size or 5000))

    from okg.substrate.graph_store import ChronosGraphStore

    active_policy_graph = active_policy.to_dict()
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    node_result = _restamp_active_policy_node_fingerprints(
        store,
        connection=conn,
        branch=branch,
        policy_fingerprint=fingerprint,
        active_policy_graph=active_policy_graph,
        batch_size=batch_size,
    )
    edge_result = _restamp_active_policy_edge_fingerprints(
        store,
        connection=conn,
        branch=branch,
        policy_fingerprint=fingerprint,
        active_policy_graph=active_policy_graph,
        batch_size=batch_size,
    )
    return {
        "schema": "okg.nomos.source_lineage_policy_fingerprint_repair.v1",
        "ok": True,
        "deployment_name": deployment_name,
        "branch": branch,
        "scope": resolved_scope,
        "policy_fingerprint": fingerprint,
        "nodes": node_result,
        "edges": edge_result,
    }


def restamp_publisher_derived_lineage(
    conn,
    *,
    deployment_name: str = "default",
    branch: str = "default",
    batch_size: int = 5000,
) -> dict[str, Any]:
    """Restamp legacy publisher-derived rows with explicit source lineage.

    Older Chronos graph rows predate embedded Nomos source-lineage attrs. Direct
    source rows can safely bridge from their storage ``source`` column, but
    publisher-derived rows would bridge to the synthetic ``_publisher_derived``
    source. This repair rewrites those rows in-place on the branch:

    - view metadata nodes get the explicit ``catalog:views_registry`` source;
    - publisher-derived edges inherit the union of their endpoint lineages;
    - unclassifiable publisher-derived rows become malformed/missing lineage,
      which is fail-closed rather than a compatibility bypass.
    """

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            "WHERE status = 'applied'"
        )
        row = cur.fetchone()
    catalog_version_id = int(row[0]) if row and row[0] is not None else None
    node_result = _restamp_publisher_derived_nodes(
        conn,
        store,
        deployment_name=deployment_name,
        catalog_version_id=catalog_version_id,
        branch=branch,
        batch_size=batch_size,
    )
    edge_result = _restamp_publisher_derived_edges(
        conn,
        store,
        deployment_name=deployment_name,
        catalog_version_id=catalog_version_id,
        branch=branch,
        batch_size=batch_size,
    )
    return {
        "schema": "okg.nomos.source_lineage_repair.v1",
        "ok": True,
        "deployment_name": deployment_name,
        "branch": branch,
        "nodes": node_result,
        "edges": edge_result,
    }


def _active_policy_bundle(
    conn,
    *,
    deployment_name: str,
    scope: str,
):
    from okg.substrate.nomos.store import load_active_policy_bundle

    return load_active_policy_bundle(
        conn,
        deployment_name=deployment_name,
        scope=scope,
    )


def _restamp_active_policy_node_fingerprints(
    store,
    *,
    connection,
    branch: str,
    policy_fingerprint: str,
    active_policy_graph: Mapping[str, Any],
    batch_size: int,
) -> dict[str, Any]:
    scanned = 0
    restamped = 0
    already_current = 0
    skipped_missing_or_malformed = 0
    skipped_uncovered_source = 0
    last_node_id = ""
    while True:
        graph = store.checkout(branch)
        rows = graph.query(
            f"""
            SELECT node_id, subtype, attrs, text_hash, embedding,
                   embedding_model, source, source_record_id,
                   source_revision, run_id, observed_at
              FROM okg.graph_nodes
             WHERE node_id > :last_node_id
             ORDER BY node_id
             LIMIT {int(batch_size)}
            """,
            {"last_node_id": last_node_id},
        )
        if not rows:
            break
        updates: list[dict[str, Any]] = []
        for row in rows:
            scanned += 1
            row = dict(row)
            last_node_id = str(row["node_id"])
            updated = _lineage_with_active_policy_fingerprint(
                row.get("attrs") or {},
                policy_fingerprint=policy_fingerprint,
                active_policy_graph=active_policy_graph,
            )
            if updated == "missing_or_malformed":
                skipped_missing_or_malformed += 1
                continue
            if updated == "uncovered_source":
                skipped_uncovered_source += 1
                continue
            if updated == "already_current":
                already_current += 1
                continue
            row["attrs"] = updated
            updates.append(row)
        if updates:
            _upsert_logical_rows_with_retry(
                store,
                connection=connection,
                branch=branch,
                table="okg.graph_nodes",
                key_column="node_id",
                rows=updates,
            )
            restamped += len(updates)
    return {
        "scanned": scanned,
        "restamped": restamped,
        "already_current": already_current,
        "skipped_missing_or_malformed": skipped_missing_or_malformed,
        "skipped_uncovered_source": skipped_uncovered_source,
    }


def _restamp_active_policy_edge_fingerprints(
    store,
    *,
    connection,
    branch: str,
    policy_fingerprint: str,
    active_policy_graph: Mapping[str, Any],
    batch_size: int,
) -> dict[str, Any]:
    scanned = 0
    restamped = 0
    already_current = 0
    skipped_missing_or_malformed = 0
    skipped_uncovered_source = 0
    last_edge_id = -1
    while True:
        graph = store.checkout(branch)
        rows = graph.query(
            f"""
            SELECT edge_id, src, dst, edge_type, attrs, provenance,
                   confidence, source, source_record_id, source_revision,
                   run_id, observed_at
              FROM okg.graph_edges
             WHERE edge_id > :last_edge_id
             ORDER BY edge_id
             LIMIT {int(batch_size)}
            """,
            {"last_edge_id": last_edge_id},
        )
        if not rows:
            break
        updates: list[dict[str, Any]] = []
        for row in rows:
            scanned += 1
            row = dict(row)
            last_edge_id = int(row["edge_id"])
            updated = _lineage_with_active_policy_fingerprint(
                row.get("attrs") or {},
                policy_fingerprint=policy_fingerprint,
                active_policy_graph=active_policy_graph,
            )
            if updated == "missing_or_malformed":
                skipped_missing_or_malformed += 1
                continue
            if updated == "uncovered_source":
                skipped_uncovered_source += 1
                continue
            if updated == "already_current":
                already_current += 1
                continue
            row["attrs"] = updated
            updates.append(row)
        if updates:
            _upsert_logical_rows_with_retry(
                store,
                connection=connection,
                branch=branch,
                table="okg.graph_edges",
                key_column="edge_id",
                rows=updates,
            )
            restamped += len(updates)
    return {
        "scanned": scanned,
        "restamped": restamped,
        "already_current": already_current,
        "skipped_missing_or_malformed": skipped_missing_or_malformed,
        "skipped_uncovered_source": skipped_uncovered_source,
    }


def _upsert_logical_rows_with_retry(
    store,
    *,
    connection,
    branch: str,
    table: str,
    key_column: str,
    rows: list[dict[str, Any]],
    retry_limit: int = 3,
) -> None:
    """Upsert ``rows``, retrying then splitting on an interval unique violation.

    Splitting a batch is a retry, so it runs only after a confirmed rollback:
    since #2159 a failed Chronos rollback surfaces the original, retryable
    error, and splitting on that connection would write into an unknown
    state (issue #2302). ``connection`` is the psycopg connection under
    ``store``; the unit of work reads its transaction status.
    """

    pending = [rows]
    while pending:
        batch = pending.pop()
        try:
            _upsert_logical_batch(
                store,
                connection=connection,
                branch=branch,
                table=table,
                rows=batch,
                retry_limit=retry_limit,
            )
        except Exception as exc:
            if not _is_retryable_interval_unique_violation(exc):
                raise
            if not rollback_confirmed(exc, connection):
                note_unconfirmed_rollback(exc, connection, label=_RETRY_LABEL)
                raise
            if len(batch) == 1:
                _update_single_attrs(
                    store,
                    branch=branch,
                    table=table,
                    key_column=key_column,
                    row=batch[0],
                )
                continue
            midpoint = max(1, len(batch) // 2)
            pending.append(batch[midpoint:])
            pending.append(batch[:midpoint])


def _upsert_logical_batch(
    store,
    *,
    connection,
    branch: str,
    table: str,
    rows: list[dict[str, Any]],
    retry_limit: int,
) -> None:
    kind = table_kind(table)

    def attempt() -> None:
        if kind is not None:
            # The repaired version's Nomos read class, from its own (new)
            # lineage (PACT lane-h-nomos-read-class-stored), and its catalog
            # row (PACT lane-h-nomos-read-class-sql). Inside the attempt: a
            # retry after a rollback writes the catalog row again.
            stamp_rows(rows, kind, catalog=store.context.db)
        store.checkout(branch).upsert_rows(table, rows)

    retry_after_rollback(
        attempt,
        retryable=_is_retryable_interval_unique_violation,
        connection=connection,
        limit=max(1, retry_limit),
        label=_RETRY_LABEL,
    )


def _update_single_attrs(
    store,
    *,
    branch: str,
    table: str,
    key_column: str,
    row: Mapping[str, Any],
) -> None:
    graph = store.checkout(branch)
    # A Chronos UPDATE keeps every column it does not SET, the read class
    # included, so the class of the new lineage is SET here too; on a table
    # registered without the columns the UPDATE sets attrs alone.
    kind = table_kind(table)
    class_set = ""
    params: dict[str, Any] = {"attrs": row["attrs"]}
    if kind is not None and graph_carries_read_class(graph, table):
        stamped = stamp_row(dict(row), kind, catalog=store.context.db)
        class_set = (
            f", {READ_CLASS_COLUMN} = :read_class"
            f", {READ_CLASS_VERSION_COLUMN} = :read_class_version"
        )
        params["read_class"] = stamped[READ_CLASS_COLUMN]
        params["read_class_version"] = stamped[READ_CLASS_VERSION_COLUMN]
    if table == "okg.graph_nodes" and key_column == "node_id":
        graph.execute(
            f"""
            UPDATE okg.graph_nodes
               SET attrs = :attrs{class_set}
             WHERE node_id = :node_id
            """,
            {**params, "node_id": row["node_id"]},
        )
        return
    if table == "okg.graph_edges" and key_column == "edge_id":
        graph.execute(
            f"""
            UPDATE okg.graph_edges
               SET attrs = :attrs{class_set}
             WHERE edge_id = :edge_id
            """,
            {**params, "edge_id": row["edge_id"]},
        )
        return
    raise ValueError(f"unsupported graph table for attrs update: {table!r}")


def _is_retryable_interval_unique_violation(exc: Exception) -> bool:
    try:
        from chronos_core.branching._runtime import (
            _is_retryable_interval_unique_violation as retryable,
        )

        if retryable(exc):
            return True
    except Exception:
        # Chronos internals are optional here; fall back to text matching below.
        pass
    text = f"{type(exc).__name__}: {exc}"
    return (
        "duplicate key value violates unique constraint" in text
        and "_chronos_b_interval" in text
    )


def _lineage_with_active_policy_fingerprint(
    attrs: Mapping[str, Any],
    *,
    policy_fingerprint: str,
    active_policy_graph: Mapping[str, Any],
) -> dict[str, Any] | str:
    lineage = attrs.get(LINEAGE_ATTR)
    lineage = lineage if isinstance(lineage, Mapping) else None
    source_ids = source_ids_from_lineage(lineage)
    if not source_ids:
        return "missing_or_malformed"
    if not all(
        _active_policy_covers_source_id(active_policy_graph, source_id)
        for source_id in source_ids
    ):
        return "uncovered_source"
    if not _lineage_needs_policy_fingerprint(lineage, policy_fingerprint):
        return "already_current"
    return attrs_with_lineage(
        attrs,
        normalize_source_lineage(
            lineage,
            policy_fingerprint=policy_fingerprint,
        ),
    )


def _lineage_needs_policy_fingerprint(
    lineage: Mapping[str, Any],
    policy_fingerprint: str,
) -> bool:
    for source in lineage.get("sources") or []:
        if not isinstance(source, Mapping):
            continue
        fingerprints = {
            str(fingerprint)
            for fingerprint in source.get("policy_fingerprints") or []
            if fingerprint
        }
        if policy_fingerprint not in fingerprints:
            return True
    return False


def _active_policy_covers_source_id(
    active_policy_graph: Mapping[str, Any],
    source_id: str,
) -> bool:
    rules = active_policy_graph.get("rules")
    if not isinstance(rules, list):
        return False
    for rule in rules:
        if not isinstance(rule, Mapping):
            continue
        if rule.get("effect") not in {"allow", "redact"}:
            continue
        actions = rule.get("actions") or []
        if "*" not in actions and not any(
            str(action).startswith("graph.read")
            for action in actions
        ):
            continue
        match = rule.get("match") or {}
        if not isinstance(match, Mapping):
            continue
        if match.get("resource_kind") in READ_BOUNDARY_RESOURCE_KINDS:
            continue
        if _match_value_contains_source_id(match.get("source_id"), source_id):
            return True
        if _match_value_contains_source_id(match.get("source_ids"), source_id):
            return True
        if _match_value_contains_wildcard(
            match.get("source_id"),
            match.get("source_ids"),
            match.get("source_family"),
            match.get("source_bundle"),
        ):
            return True
        if _source_id_matches_prefix(match.get("source_family"), source_id):
            return True
        if _source_id_matches_prefix(match.get("source_bundle"), source_id):
            return True
    return False


def _match_value_contains_source_id(value: Any, source_id: str) -> bool:
    if isinstance(value, str):
        return value == source_id
    if isinstance(value, (list, tuple, set)):
        return source_id in {str(item) for item in value}
    return False


def _match_value_contains_wildcard(*values: Any) -> bool:
    for value in values:
        if value == "*":
            return True
        if isinstance(value, (list, tuple, set)) and "*" in {
            str(item) for item in value
        }:
            return True
    return False


def _source_id_matches_prefix(value: Any, source_id: str) -> bool:
    prefixes: list[str] = []
    if isinstance(value, str):
        prefixes.append(value)
    elif isinstance(value, (list, tuple, set)):
        prefixes.extend(str(item) for item in value if str(item).strip())
    return any(
        source_id == prefix or source_id.startswith(f"{prefix}:")
        for prefix in prefixes
    )


def _empty_policy_fingerprint_repair_result() -> dict[str, int]:
    return {
        "scanned": 0,
        "restamped": 0,
        "already_current": 0,
        "skipped_missing_or_malformed": 0,
        "skipped_uncovered_source": 0,
    }


def _restamp_publisher_derived_nodes(
    conn,
    store,
    *,
    deployment_name: str,
    catalog_version_id: int | None,
    branch: str,
    batch_size: int,
) -> dict[str, Any]:
    scanned = 0
    restamped = 0
    malformed = 0
    last_node_id = ""
    while True:
        graph = store.checkout(branch)
        rows = graph.query(
            f"""
            SELECT node_id, subtype, attrs, source_record_id, source_revision
              FROM okg.graph_nodes
             WHERE source = :source
               AND node_id > :last_node_id
             ORDER BY node_id
             LIMIT {int(batch_size)}
            """,
            {"source": PUBLISHER_DERIVED_SOURCE, "last_node_id": last_node_id},
        )
        if not rows:
            break
        facts: list[NodeFact] = []
        for row in rows:
            scanned += 1
            last_node_id = str(row["node_id"])
            attrs = dict(row.get("attrs") or {})
            if not _needs_restamp(attrs):
                continue
            lineage = _publisher_derived_node_lineage(row)
            if not source_ids_from_lineage(lineage):
                malformed += 1
            facts.append(
                NodeFact(
                    node_id=str(row["node_id"]),
                    subtype=str(row["subtype"]),
                    attrs=attrs_with_lineage(attrs, lineage),
                    source_record_id=dict(row.get("source_record_id") or {}),
                    source_revision=dict(row.get("source_revision") or {}),
                )
            )
        if facts:
            from okg.substrate.fact_application import apply_bounded_fact_batch

            apply_bounded_fact_batch(
                conn,
                store,
                facts,
                deployment_name=deployment_name,
                producer_id="system:nomos_lineage_repair",
                producer_kind="system",
                intent="publisher_derived_lineage_repair",
                catalog_version_id=catalog_version_id,
                authority_scope={
                    "kind": "nodes",
                    "last_node_id": last_node_id,
                    "batch_size": batch_size,
                },
                target_branch=branch,
                source=PUBLISHER_DERIVED_SOURCE,
                strict_endpoints=False,
            )
            restamped += len(facts)
    return {
        "scanned": scanned,
        "restamped": restamped,
        "malformed": malformed,
    }


def _restamp_publisher_derived_edges(
    conn,
    store,
    *,
    deployment_name: str,
    catalog_version_id: int | None,
    branch: str,
    batch_size: int,
) -> dict[str, Any]:
    scanned = 0
    restamped = 0
    malformed = 0
    last_edge_id = -1
    while True:
        graph = store.checkout(branch)
        rows = graph.query(
            f"""
            SELECT e.edge_id, e.src, e.dst, e.edge_type, e.attrs,
                   e.provenance, e.confidence,
                   e.source_record_id, e.source_revision,
                   src.attrs AS src_attrs, src.source AS src_source,
                   src.source_record_id AS src_source_record_id,
                   src.source_revision AS src_source_revision,
                   dst.attrs AS dst_attrs, dst.source AS dst_source,
                   dst.source_record_id AS dst_source_record_id,
                   dst.source_revision AS dst_source_revision
              FROM okg.graph_edges e
              LEFT JOIN okg.graph_nodes src ON src.node_id = e.src
              LEFT JOIN okg.graph_nodes dst ON dst.node_id = e.dst
             WHERE e.source = :source
               AND e.edge_id > :last_edge_id
             ORDER BY e.edge_id
             LIMIT {int(batch_size)}
            """,
            {"source": PUBLISHER_DERIVED_SOURCE, "last_edge_id": last_edge_id},
        )
        if not rows:
            break
        facts: list[EdgeFact] = []
        for row in rows:
            scanned += 1
            last_edge_id = int(row["edge_id"])
            attrs = dict(row.get("attrs") or {})
            if not _needs_restamp(attrs):
                continue
            lineage = combine_source_lineages([
                _endpoint_lineage(row, prefix="src"),
                _endpoint_lineage(row, prefix="dst"),
            ])
            if not source_ids_from_lineage(lineage):
                malformed += 1
            facts.append(
                EdgeFact(
                    src=str(row["src"]),
                    dst=str(row["dst"]),
                    edge_type=str(row["edge_type"]),
                    attrs=attrs_with_lineage(attrs, lineage),
                    provenance=str(row.get("provenance") or "derived_deterministic"),
                    confidence=float(row.get("confidence") or 1.0),
                    source_record_id=dict(row.get("source_record_id") or {}),
                    source_revision=dict(row.get("source_revision") or {}),
                )
            )
        if facts:
            from okg.substrate.fact_application import apply_bounded_fact_batch

            apply_bounded_fact_batch(
                conn,
                store,
                facts,
                deployment_name=deployment_name,
                producer_id="system:nomos_lineage_repair",
                producer_kind="system",
                intent="publisher_derived_lineage_repair",
                catalog_version_id=catalog_version_id,
                authority_scope={
                    "kind": "edges",
                    "last_edge_id": last_edge_id,
                    "batch_size": batch_size,
                },
                target_branch=branch,
                source=PUBLISHER_DERIVED_SOURCE,
                strict_endpoints=False,
            )
            restamped += len(facts)
    return {
        "scanned": scanned,
        "restamped": restamped,
        "malformed": malformed,
    }


def _needs_restamp(attrs: Mapping[str, Any]) -> bool:
    lineage = attrs.get(LINEAGE_ATTR)
    source_ids = source_ids_from_lineage(
        lineage if isinstance(lineage, Mapping) else None
    )
    return not source_ids or PUBLISHER_DERIVED_SOURCE in source_ids


def _publisher_derived_node_lineage(row: Mapping[str, Any]) -> dict[str, Any]:
    node_id = str(row.get("node_id") or "")
    subtype = str(row.get("subtype") or "")
    if subtype == "view" or node_id.startswith("view:"):
        return normalize_source_lineage(
            source_id=VIEW_REGISTRY_SOURCE,
            source_record_id=row.get("source_record_id"),
            source_revision=row.get("source_revision"),
        )
    return normalize_source_lineage(None)


def _endpoint_lineage(row: Mapping[str, Any], *, prefix: str) -> dict[str, Any]:
    return lineage_for_row(
        row.get(f"{prefix}_attrs") or {},
        source_id=row.get(f"{prefix}_source"),
        source_record_id=row.get(f"{prefix}_source_record_id"),
        source_revision=row.get(f"{prefix}_source_revision"),
    )



# --- Laundered derived lineage (issue #1544) --------------------------------
#
# PR #788 (2026-07-18, commit a083b466b1) changed the publisher's derived
# re-application from the literal source id ``_publisher_derived`` to the id of
# the producer that EMITTED the fact. Both recognition sites in the graph write
# path keyed on that literal, so from then until PR #1535 a derived row was
# stamped with its own EMITTER as its lineage source. Trust then resolved from
# the emitter's declared label instead of the root source's, which is how an
# untrusted root could be laundered trusted in one hop.
#
# ``restamp_publisher_derived_lineage`` above cannot reach these rows: it
# selects ``WHERE source = '_publisher_derived'``, the literal #788 retired.
# The repair below is its sibling for the #788..#1535 window (#1544).

DERIVED_REAPPLICATION_INTENT_PREFIX = "publisher_derived:"
LAUNDERED_SCAN_SCHEMA = "okg.nomos.laundered_derived_lineage_scan.v1"
LAUNDERED_REPAIR_SCHEMA = "okg.nomos.laundered_derived_lineage_repair.v1"

# Producer kinds that EMIT derived facts, and therefore the only kinds whose id
# can appear as the source of a publisher derived re-application.
#
# Deliberately NOT `producer_kind <> 'source'`. Several `system` producers name
# their own id as lineage BY DESIGN and correctly: `_catalog:restamp`,
# `_catalog:concept_seed`, `system:external_backed_collector`. They write
# through `apply_bounded_fact_batch` with `source=producer_id` and no
# re-application flag, so the id in their lineage IS their provenance, and
# `deployment_upgrade._nomos_policy_source_ids` blesses `_catalog:restamp` and
# `system:external_backed_collector` into every deployment Nomos baseline as
# substrate-owned lineage identities. Treating those as laundering would turn
# readiness red on a healthy deployment after its first catalog apply, with a
# repair verb that could never clear it.
DERIVED_OUTPUT_PRODUCER_KINDS = frozenset({
    "agent",
    "enricher",
    "extractor",
    "lens",
})

# Source ids that name a real provenance even though the publisher writes them
# through the derived path. They are never treated as laundering.
_NEVER_LAUNDERED_SOURCE_IDS = frozenset({
    PUBLISHER_DERIVED_SOURCE,
    VIEW_REGISTRY_SOURCE,
})


def derived_producer_inventory(
    conn,
    *,
    deployment_name: str,
) -> dict[str, Any]:
    """Return the producer ids the publisher can have used as a graph source.

    Two INDEPENDENT reads, unioned, so a pruned table cannot silently shrink
    the set to nothing:

    - ``admission_envelopes`` rows whose intent is ``publisher_derived:*``.
      That envelope is written by ``_apply_publisher_derived_fact_batch``
      under the EMITTING producer's id, which is exactly the id that was also
      written into the graph ``source`` column. This is the direct evidence.
    - ``producer_profiles`` rows whose ``producer_kind`` is not ``source``.
      Broader, and it survives envelope pruning.

    When NEITHER table can be read the result is ``measured: False`` with
    ``producer_ids: None``. A caller must not read that as an empty set — an
    absent measurement is not a clean one (#1544).
    """

    envelope_ids: set[str] = set()
    profile_ids: set[str] = set()
    read_errors: list[dict[str, str]] = []
    reads = (
        (
            "admission_envelopes",
            "SELECT DISTINCT producer_id FROM okg.admission_envelopes "
            "WHERE deployment_name = %(deployment_name)s "
            "AND intent LIKE %(intent)s",
            {
                "deployment_name": deployment_name,
                "intent": f"{DERIVED_REAPPLICATION_INTENT_PREFIX}%",
            },
        ),
        (
            "producer_profiles",
            "SELECT DISTINCT producer_id FROM okg.producer_profiles "
            "WHERE deployment_name = %(deployment_name)s "
            "AND producer_kind = ANY(%(kinds)s)",
            {
                "deployment_name": deployment_name,
                "kinds": sorted(DERIVED_OUTPUT_PRODUCER_KINDS),
            },
        ),
    )
    for label, sql, params in reads:
        target = envelope_ids if label == "admission_envelopes" else profile_ids
        try:
            # A savepoint, NOT conn.rollback(): a failed read here must undo
            # only itself. Rolling the whole transaction back would also undo
            # the caller's work — and, when the first read fails because the
            # relation is absent, would restore it in time for the second read
            # to succeed, turning an unavailable inventory into a measured one.
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    for row in cur.fetchall():
                        producer_id = str(row[0] or "").strip()
                        if producer_id:
                            target.add(producer_id)
        except Exception as exc:  # noqa: BLE001 - reported, never swallowed.
            read_errors.append({
                "relation": label,
                "error": type(exc).__name__,
                "message": str(exc),
            })
    if len(read_errors) == len(reads):
        return {
            "measured": False,
            "status": "producer_inventory_unavailable",
            "producer_ids": None,
            "from_admission_envelopes": None,
            "from_producer_profiles": None,
            "errors": read_errors,
        }
    producer_ids = (envelope_ids | profile_ids) - _NEVER_LAUNDERED_SOURCE_IDS
    return {
        "measured": True,
        "status": "measured" if not read_errors else "partial",
        "producer_ids": sorted(producer_ids),
        "from_admission_envelopes": sorted(envelope_ids),
        "from_producer_profiles": sorted(profile_ids),
        "errors": read_errors,
    }


def laundered_row_kind(
    attrs: Mapping[str, Any] | None,
    source: Any,
    *,
    producer_ids: Any,
    source_record_id: Any = None,
    require_declaration: bool = True,
) -> str | None:
    """Classify one graph row against the #788 laundering shape.

    Returns ``"producer_named"`` when the row's lineage names a PRODUCER id as
    a source — the invisible class, because such a row reads as perfectly valid
    lineage everywhere. Returns ``"missing"`` when a producer-attributed row
    carries no usable lineage at all; that class is already visible to the
    readiness missing-lineage count, so the doctor reports it separately rather
    than counting it twice. Returns ``None`` for a row that is not laundered.

    ``require_declaration`` is the structural guard that keeps this from firing
    on correct rows. A row is only a derived RE-APPLICATION if it carries the
    provenance its emitter declared — ``derived_from`` or a ``derived_support``
    member list in ``source_record_id``. A producer-attributed row with NO such
    declaration is a direct EMISSION, and its own producer id is genuinely its
    provenance; ``_catalog:restamp`` rows are the standing example, and the
    Nomos baseline blesses that id as a lineage source. Without this guard the
    check would flag them and no repair could ever clear the flag. It is
    deliberately independent of the producer-id set above, so a set that is too
    broad still cannot produce a false positive.

    Edges carry no declaration of their own; they inherit from their two
    structural endpoints, so the edge scan passes ``require_declaration=False``
    and relies on the endpoints for the same protection.
    """

    source_id = str(source or "").strip()
    if not source_id or source_id not in producer_ids:
        return None
    if require_declaration and not _derived_declaration_ids(source_record_id):
        return None
    lineage = (attrs or {}).get(LINEAGE_ATTR)
    lineage = lineage if isinstance(lineage, Mapping) else None
    lineage_source_ids = source_ids_from_lineage(lineage)
    if not lineage_source_ids:
        return "missing"
    if any(
        lineage_source_id in producer_ids
        for lineage_source_id in lineage_source_ids
    ):
        return "producer_named"
    return None


def count_laundered_derived_rows(
    graph,
    *,
    producer_ids: Any,
    batch_size: int = 5000,
) -> dict[str, dict[str, int]]:
    """Count laundered rows EXACTLY on one graph checkout.

    Deliberately not a sample. The surrounding readiness checks sample the
    first 10000 rows, which is fine for a proportion but not for a health
    flag that must reach zero: a laundered row past the sample boundary would
    make a partial scan report clean, and a partial scan reporting clean is
    the same defect class as this whole issue (#1544).

    Exactness stays affordable because the scan is narrowed to rows whose
    `source` is a producer id — the only rows that can be laundered — rather
    than to the whole graph, and rows are counted and dropped rather than
    held.
    """

    batch_size = max(1, int(batch_size or 5000))
    ids = sorted(producer_ids)
    if not ids:
        return {
            "nodes": {"producer_named": 0, "missing": 0},
            "edges": {"producer_named": 0, "missing": 0},
        }
    result: dict[str, dict[str, int]] = {}
    for label, table, key in (
        ("nodes", "okg.graph_nodes", "node_id"),
        ("edges", "okg.graph_edges", "edge_id"),
    ):
        counts = {"producer_named": 0, "missing": 0}
        last: Any = "" if key == "node_id" else -1
        while True:
            rows = graph.query(
                f"""
                SELECT {key}, attrs, source, source_record_id
                  FROM {table}
                 WHERE source = ANY(:producer_ids)
                   AND {key} > :last
                 ORDER BY {key}
                 LIMIT {int(batch_size)}
                """,
                {"producer_ids": ids, "last": last},
            )
            if not rows:
                break
            for row in rows:
                last = row[key] if key == "edge_id" else str(row[key])
                kind = laundered_row_kind(
                    row.get("attrs") or {},
                    row.get("source"),
                    producer_ids=producer_ids,
                    source_record_id=row.get("source_record_id"),
                    require_declaration=(label == "nodes"),
                )
                if kind is not None:
                    counts[kind] += 1
        result[label] = counts
    return result


def repair_laundered_derived_lineage(
    conn,
    *,
    deployment_name: str = "default",
    branch: str = "default",
    batch_size: int = 5000,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Re-derive the root lineage of derived rows laundered since PR #788.

    A derived NODE's inputs are not inferable from the graph — adjacency is not
    derivation. They are what the emitting producer DECLARED through
    ``emit_node_fact(derived_from=...)``, which the publisher persists in
    ``graph_nodes.source_record_id['derived_from']``. This repair resolves
    those declared ids to their lineage and combines them exactly as
    ``_stamp_publisher_derived_node_lineage`` does on the live write path, so a
    repaired row matches one written after PR #1535.

    A derived EDGE inherits from its two structural endpoints, so it is
    recombined from the (already repaired) endpoint node lineage.

    REFUSES rather than guesses. A node that declared nothing, named an input
    that no longer exists, or sits in a declaration cycle is NOT written; it is
    returned under ``unrepairable`` with a reason and stays fail-closed.
    Inventing a lineage for such a row is precisely the laundering this repair
    exists to undo.

    Idempotent: a repaired row no longer names a producer id, so a second run
    classifies it clean and writes nothing. Only ``attrs`` is rewritten; every
    other column, the ``source`` column included, is preserved, so the row's
    storage attribution stays auditable.

    Memory is bounded by the LAUNDERED set plus the inputs it declares, not by
    the graph: clean rows are counted and dropped.
    """

    from okg.substrate.graph_store import ChronosGraphStore

    batch_size = max(1, int(batch_size or 5000))
    inventory = derived_producer_inventory(
        conn,
        deployment_name=deployment_name,
    )
    base = {
        "schema": LAUNDERED_REPAIR_SCHEMA,
        "deployment_name": deployment_name,
        "branch": branch,
        "dry_run": bool(dry_run),
        "producer_inventory": inventory,
    }
    if not inventory["measured"]:
        return {
            **base,
            "ok": False,
            "complete": False,
            "unrepairable_total": None,
            "reason": inventory["status"],
            "nodes": _empty_laundered_repair_result(),
            "edges": _empty_laundered_repair_result(),
        }
    producer_ids = frozenset(inventory["producer_ids"])
    if not producer_ids:
        return {
            **base,
            "ok": True,
            "complete": True,
            "unrepairable_total": 0,
            "nodes": _empty_laundered_repair_result(),
            "edges": _empty_laundered_repair_result(),
        }
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    node_result, repaired_node_lineage = _repair_laundered_nodes(
        store,
        connection=conn,
        branch=branch,
        producer_ids=producer_ids,
        batch_size=batch_size,
        dry_run=bool(dry_run),
    )
    edge_result = _repair_laundered_edges(
        store,
        connection=conn,
        branch=branch,
        producer_ids=producer_ids,
        batch_size=batch_size,
        dry_run=bool(dry_run),
        repaired_node_lineage=repaired_node_lineage,
    )
    unrepairable = (
        int(node_result["unrepairable"]) + int(edge_result["unrepairable"])
    )
    return {
        **base,
        "ok": True,
        # `ok` says the repair RAN. `complete` says nothing is left laundered.
        # A caller that keys success on `ok` alone would report done while the
        # doctor is still red, so the CLI keys on this instead.
        "complete": unrepairable == 0,
        "unrepairable_total": unrepairable,
        "nodes": node_result,
        "edges": edge_result,
    }


def scan_laundered_derived_lineage(
    conn,
    *,
    deployment_name: str = "default",
    branch: str = "default",
    batch_size: int = 5000,
) -> dict[str, Any]:
    """Report the laundered counts without writing anything."""

    payload = repair_laundered_derived_lineage(
        conn,
        deployment_name=deployment_name,
        branch=branch,
        batch_size=batch_size,
        dry_run=True,
    )
    payload["schema"] = LAUNDERED_SCAN_SCHEMA
    return payload


def _empty_laundered_repair_result() -> dict[str, Any]:
    return {
        "scanned": 0,
        "laundered": 0,
        "repaired": 0,
        "already_clean": 0,
        "unrepairable": 0,
        "unrepairable_rows": [],
    }


def _declared_input_ids(source_record_id: Any) -> list[str]:
    """The inputs the emitter declared through ``derived_from``.

    This is the ONLY thing the repair re-derives lineage from, matching
    ``_stamp_publisher_derived_node_lineage`` on the live write path, so a
    repaired row equals one written after PR #1535.
    """
    if not isinstance(source_record_id, Mapping):
        return []
    declared = source_record_id.get("derived_from")
    if not isinstance(declared, (list, tuple)):
        return []
    return [str(node_id) for node_id in declared if node_id]


def _derived_declaration_ids(source_record_id: Any) -> list[str]:
    """Every node id this row claims to be derived from, by any declaration.

    Broader than ``_declared_input_ids`` on purpose: it also reads the
    ``derived_support`` member list the graph write boundary forces onto
    ``_enricher:*`` rows. Used only to decide WHETHER a row is a derived
    re-application at all, never to re-derive lineage — a support member set is
    a liveness declaration, not an input declaration, and combining the two
    would let one stand in for the other.
    """
    if not isinstance(source_record_id, Mapping):
        return []
    ids = list(_declared_input_ids(source_record_id))
    support = source_record_id.get("derived_support")
    if isinstance(support, Mapping):
        members = support.get("members")
        if isinstance(members, (list, tuple)):
            ids.extend(str(node_id) for node_id in members if node_id)
    return ids


def _repair_laundered_nodes(
    store,
    *,
    connection,
    branch: str,
    producer_ids: frozenset[str],
    batch_size: int,
    dry_run: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Classify, then resolve, then write — in three bounded passes.

    Pass 1 scans only the columns needed to classify and to read declarations.
    It deliberately does NOT select ``embedding``: a node embedding is a
    2560-dimension vector, and the #1544 population is "every derived row in a
    six-week window", so holding whole rows would make peak memory scale with
    the derived graph. Pass 2 resolves the declared inputs and runs the
    fixpoint over ids and lineage envelopes only. Pass 3 re-reads the full rows
    for the ids that actually resolved, one ``batch_size`` chunk at a time, and
    writes each chunk before reading the next.
    """
    scanned = 0
    already_clean = 0
    # node_id -> (kind, declared inputs). Ids and small lists only.
    laundered: dict[str, tuple[str, list[str]]] = {}
    last_node_id = ""
    while True:
        graph = store.checkout(branch)
        rows = graph.query(
            f"""
            SELECT node_id, attrs, source, source_record_id
              FROM okg.graph_nodes
             WHERE node_id > :last_node_id
             ORDER BY node_id
             LIMIT {int(batch_size)}
            """,
            {"last_node_id": last_node_id},
        )
        if not rows:
            break
        for raw in rows:
            scanned += 1
            last_node_id = str(raw["node_id"])
            kind = laundered_row_kind(
                raw.get("attrs") or {},
                raw.get("source"),
                producer_ids=producer_ids,
                source_record_id=raw.get("source_record_id"),
            )
            if kind is None:
                already_clean += 1
                continue
            laundered[last_node_id] = (
                kind,
                _declared_input_ids(raw.get("source_record_id")),
            )

    result = _empty_laundered_repair_result()
    result["scanned"] = scanned
    result["already_clean"] = already_clean
    result["laundered"] = len(laundered)
    if not laundered:
        return result, {}

    # Lineage for declared inputs that are NOT themselves laundered comes
    # straight from storage; laundered inputs are resolved by the fixpoint
    # below, so a chain converges to its ROOT rather than to an intermediate
    # producer. Resolving a laundered input from storage would re-launder it.
    needed_input_ids = sorted({
        node_id
        for _kind, declared in laundered.values()
        for node_id in declared
        if node_id not in laundered
    })
    resolved: dict[str, Any] = {}
    for chunk_start in range(0, len(needed_input_ids), batch_size):
        chunk = needed_input_ids[chunk_start:chunk_start + batch_size]
        graph = store.checkout(branch)
        for row in graph.query(
            """
            SELECT node_id, attrs, source, source_record_id, source_revision
              FROM okg.graph_nodes
             WHERE node_id = ANY(:node_ids)
            """,
            {"node_ids": chunk},
        ):
            resolved[str(row["node_id"])] = lineage_for_row(
                row.get("attrs") or {},
                source_id=row.get("source"),
                source_record_id=row.get("source_record_id"),
                source_revision=row.get("source_revision"),
            )

    unrepairable: list[dict[str, Any]] = []
    repaired_lineage: dict[str, Any] = {}
    pending = dict(laundered)
    while pending:
        ready = [
            node_id for node_id, (_kind, declared) in pending.items()
            if declared and all(dep in resolved for dep in declared)
        ]
        if not ready:
            break
        for node_id in ready:
            _kind, declared = pending.pop(node_id)
            combined = combine_source_lineages(
                [resolved[dep] for dep in declared]
            )
            if not source_ids_from_lineage(combined):
                unrepairable.append({
                    "node_id": node_id,
                    "reason": "declared_inputs_have_no_lineage",
                    "declared_inputs": declared,
                })
                continue
            resolved[node_id] = combined
            repaired_lineage[node_id] = combined
    for node_id, (_kind, declared) in pending.items():
        if not declared:
            reason = "no_declared_inputs"
        elif any(dep in pending for dep in declared):
            reason = "cyclic_or_unresolved_declaration"
        else:
            reason = "declared_input_not_found"
        unrepairable.append({
            "node_id": node_id,
            "reason": reason,
            "declared_inputs": declared,
        })

    if repaired_lineage and not dry_run:
        write_ids = sorted(repaired_lineage)
        for chunk_start in range(0, len(write_ids), batch_size):
            chunk = write_ids[chunk_start:chunk_start + batch_size]
            graph = store.checkout(branch)
            rows = graph.query(
                """
                SELECT node_id, subtype, attrs, text_hash, embedding,
                       embedding_model, source, source_record_id,
                       source_revision, run_id, observed_at
                  FROM okg.graph_nodes
                 WHERE node_id = ANY(:node_ids)
                """,
                {"node_ids": chunk},
            )
            updates = []
            for raw in rows:
                row = dict(raw)
                row["attrs"] = attrs_with_lineage(
                    dict(row.get("attrs") or {}),
                    repaired_lineage[str(row["node_id"])],
                )
                updates.append(row)
            if updates:
                _upsert_logical_rows_with_retry(
                    store,
                    connection=connection,
                    branch=branch,
                    table="okg.graph_nodes",
                    key_column="node_id",
                    rows=updates,
                )
    result["repaired"] = len(repaired_lineage)
    result["unrepairable"] = len(unrepairable)
    result["unrepairable_rows"] = unrepairable
    return result, repaired_lineage


def _repair_laundered_edges(
    store,
    *,
    connection,
    branch: str,
    producer_ids: frozenset[str],
    batch_size: int,
    dry_run: bool,
    repaired_node_lineage: Mapping[str, Any],
) -> dict[str, Any]:
    scanned = 0
    already_clean = 0
    laundered = 0
    repairs: list[dict[str, Any]] = []
    unrepairable: list[dict[str, Any]] = []
    last_edge_id = -1
    while True:
        graph = store.checkout(branch)
        rows = graph.query(
            f"""
            SELECT e.edge_id, e.src, e.dst, e.edge_type, e.attrs,
                   e.provenance, e.confidence, e.source,
                   e.source_record_id, e.source_revision, e.run_id,
                   e.observed_at,
                   src.attrs AS src_attrs, src.source AS src_source,
                   src.source_record_id AS src_source_record_id,
                   src.source_revision AS src_source_revision,
                   dst.attrs AS dst_attrs, dst.source AS dst_source,
                   dst.source_record_id AS dst_source_record_id,
                   dst.source_revision AS dst_source_revision
              FROM okg.graph_edges e
              LEFT JOIN okg.graph_nodes src ON src.node_id = e.src
              LEFT JOIN okg.graph_nodes dst ON dst.node_id = e.dst
             WHERE e.edge_id > :last_edge_id
             ORDER BY e.edge_id
             LIMIT {int(batch_size)}
            """,
            {"last_edge_id": last_edge_id},
        )
        if not rows:
            break
        for raw in rows:
            scanned += 1
            row = dict(raw)
            last_edge_id = int(row["edge_id"])
            # An edge declares nothing itself; it inherits from its two
            # endpoints, so the declaration guard is applied there instead
            # (an endpoint that is not laundered is a usable input).
            kind = laundered_row_kind(
                row.get("attrs") or {},
                row.get("source"),
                producer_ids=producer_ids,
                require_declaration=False,
            )
            if kind is None:
                already_clean += 1
                continue
            laundered += 1
            endpoint_lineages: list[Any] = []
            unresolved: list[str] = []
            for prefix in ("src", "dst"):
                endpoint_id = str(row.get(prefix) or "")
                lineage = repaired_node_lineage.get(endpoint_id)
                if lineage is None:
                    lineage = _endpoint_lineage(row, prefix=prefix)
                    # An endpoint that is itself still laundered cannot be a
                    # trustworthy input: inheriting from it would carry the
                    # producer id straight onto the edge.
                    if laundered_row_kind(
                        row.get(f"{prefix}_attrs") or {},
                        row.get(f"{prefix}_source"),
                        producer_ids=producer_ids,
                        source_record_id=row.get(f"{prefix}_source_record_id"),
                    ) is not None:
                        unresolved.append(endpoint_id)
                        continue
                if not source_ids_from_lineage(lineage):
                    unresolved.append(endpoint_id)
                    continue
                endpoint_lineages.append(lineage)
            if unresolved or len(endpoint_lineages) != 2:
                unrepairable.append({
                    "edge_id": last_edge_id,
                    "src": str(row.get("src") or ""),
                    "dst": str(row.get("dst") or ""),
                    "source": str(row.get("source") or ""),
                    "reason": "endpoint_lineage_unresolved",
                    "unresolved_endpoints": unresolved,
                })
                continue
            combined = combine_source_lineages(endpoint_lineages)
            if not source_ids_from_lineage(combined):
                unrepairable.append({
                    "edge_id": last_edge_id,
                    "src": str(row.get("src") or ""),
                    "dst": str(row.get("dst") or ""),
                    "source": str(row.get("source") or ""),
                    "reason": "combined_endpoint_lineage_malformed",
                    "unresolved_endpoints": [],
                })
                continue
            repairs.append({
                "edge_id": last_edge_id,
                "src": str(row["src"]),
                "dst": str(row["dst"]),
                "edge_type": str(row["edge_type"]),
                "attrs": attrs_with_lineage(
                    dict(row.get("attrs") or {}),
                    combined,
                ),
                "provenance": row.get("provenance"),
                "confidence": row.get("confidence"),
                "source": row.get("source"),
                "source_record_id": row.get("source_record_id"),
                "source_revision": row.get("source_revision"),
                "run_id": row.get("run_id"),
                "observed_at": row.get("observed_at"),
            })
    if repairs and not dry_run:
        for chunk_start in range(0, len(repairs), batch_size):
            _upsert_logical_rows_with_retry(
                store,
                connection=connection,
                branch=branch,
                table="okg.graph_edges",
                key_column="edge_id",
                rows=repairs[chunk_start:chunk_start + batch_size],
            )
    return {
        "scanned": scanned,
        "laundered": laundered,
        "repaired": len(repairs),
        "already_clean": already_clean,
        "unrepairable": len(unrepairable),
        "unrepairable_rows": unrepairable,
    }


__all__ = [
    "DERIVED_REAPPLICATION_INTENT_PREFIX",
    "LAUNDERED_REPAIR_SCHEMA",
    "LAUNDERED_SCAN_SCHEMA",
    "DERIVED_OUTPUT_PRODUCER_KINDS",
    "PUBLISHER_DERIVED_SOURCE",
    "VIEW_REGISTRY_SOURCE",
    "count_laundered_derived_rows",
    "derived_producer_inventory",
    "laundered_row_kind",
    "repair_laundered_derived_lineage",
    "restamp_active_policy_lineage_fingerprints",
    "restamp_publisher_derived_lineage",
    "scan_laundered_derived_lineage",
]
