"""Apply a `ComposedCatalog` to the database under a fresh
catalog_version_id.

`okg catalog load --apply` consumes this. The reconcile path
(`okg.substrate.catalog.reconcile.apply_plan`) is the incremental
diff flow (drop / add narrowings, emit retractions for restrictive
changes); this is the FULL flow used to bootstrap a fresh
deployment or stamp a template install — composes the catalog and inserts
subtypes / narrowings / aliases / identifier_patterns under a fresh pending
catalog_versions row. Relational rows commit together; graph, alias, and
ownership side effects complete while the version remains non-active. Only
then is the row promoted to `applied`. Any post-commit failure records
`rolled_back` and refuses the apply.

Precondition: `compose_catalog()` has already raised
`CatalogModuleMissing` / `BridgeSubtypeUnknown` for any catalog
errors, so by the time we get here the data is structurally
valid. This module's job is the DDL-equivalent INSERT, not
validation.

Per design.md `debug-substrate-okg-self` Phase 6.

Per `add-per-subtype-bm25-fields` (2026-04-26): when an apply
changes a subtype's `searchable_text_fields` annotation vs. the
prior applied catalog version, this module re-stamps every
Chronos graph node of that subtype so the new annotation takes
effect immediately — without forcing a full re-ingest. The
re-stamp re-emits the affected nodes through
`ChronosGraphStore.apply_facts`, which re-runs the same
`_stamp_searchable_text` logic the store applies on every write.
"""
from __future__ import annotations

import logging
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.catalog.modules_compose import ComposedCatalog
from okg.substrate.entity_resolution import normalize_entity_name


from okg.substrate.errors import (
    CatalogMigrationRefused,
    SchemaValidationFailed,
    SubstrateError,
)


__all__ = ["apply_composed_catalog", "CircularRefinementError"]


_log = logging.getLogger(__name__)


class CircularRefinementError(SubstrateError):
    """Raised when subtype `is_a:` declarations form a cycle.

    Single-parent refinement should form a forest of trees. A cycle
    (self-reference or longer) is a schema authoring bug; we catch
    it BEFORE attempting closure population because the iterative
    closure-insert would silently truncate via PK collision and
    leave ancestor sets mis-computed.
    """

    code = "catalog_refinement_cycle"


def _detect_refinement_cycles(composed: "ComposedCatalog") -> None:
    """Walk parent_subtype edges per subtype; raise on cycles."""
    parents = composed.subtype_parent
    for start in sorted(composed.subtypes):
        if start not in parents:
            continue  # root subtype, no walk needed
        seen: list[str] = [start]
        node = parents[start]
        while node is not None:
            if node in seen:
                seen.append(node)
                raise CircularRefinementError(
                    f"subtype refinement cycle detected: "
                    f"{' -> '.join(seen)}",
                    fix_hint=(
                        "Remove the `is_a:` link on the offending "
                        "subtype, or restructure so the refinement "
                        "graph is a forest of trees."
                    ),
                    data={"cycle": list(seen)},
                )
            seen.append(node)
            node = parents.get(node)


def _validate_identifier_patterns(composed: "ComposedCatalog") -> None:
    """Refuse malformed resolver authority before opening a DB transaction."""
    import re

    for index, pattern in enumerate(composed.identifier_patterns):
        raw = pattern.get("regex")
        if not isinstance(raw, str) or not raw:
            raise SchemaValidationFailed(
                "catalog identifier pattern regex must be a non-empty string",
                data={"index": index, "regex": raw},
                fix_hint="repair identifier_patterns.yaml and re-compose the catalog",
            )
        try:
            re.compile(raw)
        except re.error as exc:
            raise SchemaValidationFailed(
                f"invalid catalog identifier regex at index {index}: {exc}",
                data={"index": index, "regex": raw, "reason": str(exc)},
                fix_hint="repair identifier_patterns.yaml and re-compose the catalog",
            ) from exc


def _populate_subtype_ancestors(
    cur: "psycopg.Cursor",
    cv: int,
    composed: "ComposedCatalog",
) -> None:
    """Populate okg.subtype_ancestors closure for `cv`.

    Reflexive depth-0 row for every subtype. Iterative depth-N+1
    rows from depth-N + parent_subtype; bounded by max chain
    depth (realistic: 1-3). Idempotent under
    `ON CONFLICT DO NOTHING`.
    """
    # Depth-0 reflexive rows.
    cur.executemany(
        "INSERT INTO okg.subtype_ancestors "
        "  (catalog_version_id, subtype_name, ancestor_name, depth) "
        "VALUES (%s, %s, %s, 0) "
        "ON CONFLICT DO NOTHING",
        [(cv, s, s) for s in sorted(composed.subtypes)],
    )
    # Depth-N+1 rows: for each existing (subtype, ancestor, N),
    # if ancestor has a parent, add (subtype, ancestor.parent, N+1).
    # Loops until no new rows are inserted.
    depth = 0
    while True:
        cur.execute(
            "INSERT INTO okg.subtype_ancestors "
            "  (catalog_version_id, subtype_name, ancestor_name, depth) "
            "SELECT sa.catalog_version_id, sa.subtype_name, "
            "       s.parent_subtype, sa.depth + 1 "
            "  FROM okg.subtype_ancestors sa "
            "  JOIN okg.subtypes s "
            "    ON s.catalog_version_id = sa.catalog_version_id "
            "   AND s.name = sa.ancestor_name "
            " WHERE sa.catalog_version_id = %s "
            "   AND sa.depth = %s "
            "   AND s.parent_subtype IS NOT NULL "
            " ON CONFLICT DO NOTHING",
            (cv, depth),
        )
        if cur.rowcount == 0:
            break
        depth += 1
        if depth > 32:
            # Defense in depth — cycle detection should have
            # caught this. 32 is well beyond any realistic
            # refinement chain depth.
            raise CircularRefinementError(
                f"subtype_ancestors closure did not converge after "
                f"{depth} iterations — refinement cycle suspected; "
                f"cycle-detection pre-pass should have caught this.",
            )


def apply_composed_catalog(
    conn: psycopg.Connection,
    composed: ComposedCatalog,
    *,
    identity: Any | None = None,
    apply_report: dict[str, Any] | None = None,
) -> int:
    """Apply `composed` under a fresh `catalog_version_id`.

    Returns the new `catalog_version_id`. Raises on any DB error;
    the caller-supplied connection's transaction wraps the work,
    so a failed apply rolls back the version row + every insert
    together.

    Idempotent against already-applied catalogs only via the
    PK / ON CONFLICT clauses on aliases + identifier_patterns
    (subtypes + narrowings always insert under a NEW cv, so they
    don't collide). The flow is "stamp a new cv with this content"
    — re-running creates another cv with identical content. To
    move the deployment FORWARD without churn, prefer
    `okg catalog reconcile`.
    """
    _validate_identifier_patterns(composed)

    if identity is not None:
        from okg.substrate.catalog.ownership import (
            ensure_catalog_ownership_schema,
        )
        ensure_catalog_ownership_schema(conn)

    with conn.transaction(), conn.cursor() as cur:
        if identity is None:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status) "
                "VALUES ('pending') RETURNING catalog_version_id"
            )
        else:
            cur.execute(
                """
                INSERT INTO okg.catalog_versions
                  (status, deployment_name, catalog_fingerprint,
                   schema_fingerprint, writer_fingerprint,
                   writer_provenance, created_by_command)
                VALUES ('pending', %s, %s, %s, %s, %s::jsonb, %s)
                RETURNING catalog_version_id
                """,
                (
                    identity.deployment_name,
                    identity.catalog_fingerprint,
                    identity.schema_fingerprint,
                    identity.writer_fingerprint,
                    Jsonb(identity.provenance),
                    identity.provenance.get("command"),
                ),
            )
        cv = cur.fetchone()[0]

        if composed.subtypes:
            # Stamp `subtype_metadata` (per
            # `add-text-attr-declaration`) into okg.subtypes.metadata
            # so the projector can read `searchable_text_fields`
            # from there at project_batch time. Subtypes without
            # metadata get the default empty `{}`.
            #
            # Per `add-subtype-refines-to-catalog` (2026-05-19):
            # also carry each subtype's optional `parent_subtype`
            # (from LinkML `is_a:` resolved during compose). The
            # DEFERRABLE FK ensures referential integrity at
            # commit even though we INSERT in name-sorted order.
            import json as _json
            cur.executemany(
                "INSERT INTO okg.subtypes "
                "  (name, metadata, parent_subtype, "
                "   catalog_version_id) "
                "VALUES (%s, %s::jsonb, %s, %s)",
                [
                    (
                        s,
                        _json.dumps(
                            composed.subtype_metadata.get(s, {})
                        ),
                        composed.subtype_parent.get(s),  # NULL if root
                        cv,
                    )
                    for s in sorted(composed.subtypes)
                ],
            )
            # Cycle detection: walk parent_subtype edges per
            # subtype; raise if any subtype's chain loops back to
            # itself or visits another subtype already on its
            # walk. Single-parent guarantees this is a tree per
            # subtype's walk; cycles imply mis-authored schemas.
            _detect_refinement_cycles(composed)
            # Populate subtype_ancestors closure (reflexive +
            # transitive). Realistic chain depth is 1-3, so the
            # iterative SQL terminates quickly.
            _populate_subtype_ancestors(cur, cv, composed)

        if composed.narrowings:
            # Multiple bridges / module schemas can declare the
            # same (parent, edge_type, child) triple — the
            # ComposedCatalog keeps each row for provenance, but
            # the DB PK is (parent, edge_type, child, cv). Dedupe
            # on the triple before inserting.
            #
            # fix-projection-alias-identity #710 (ruling 3): stamp attr_rules
            # (the cardinality annotation) alongside the triple. Previously
            # this INSERT omitted the column entirely, so an annotation was
            # lost the moment it was applied -- a silent un-enforcement.
            attr_rules_by_triple = composed.narrowing_attr_rules()
            triples = {
                (n.parent_subtype, n.edge_archetype, n.child_subtype)
                for n in composed.narrowings
            }
            cur.executemany(
                "INSERT INTO okg.narrowings "
                "  (parent_subtype, edge_type, child_subtype, "
                "   attr_rules, catalog_version_id) "
                "VALUES (%s, %s, %s, %s::jsonb, %s)",
                [
                    (
                        p, e, c,
                        _json.dumps(attr_rules_by_triple.get((p, e, c), {})),
                        cv,
                    )
                    for (p, e, c) in triples
                ],
            )

        if composed.identifier_patterns:
            cur.executemany(
                "INSERT INTO okg.identifier_patterns "
                "  (id_type, regex, validator_kind, description, "
                "   catalog_version_id) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (id_type, regex, catalog_version_id) "
                "DO NOTHING",
                [
                    (
                        p["id_type"], p["regex"],
                        p.get("validator_kind"),
                        p.get("description"),
                        cv,
                    )
                    for p in composed.identifier_patterns
                ],
            )

        # Deployment-side invariants. Loaded by the composer
        # from `<deployment>/invariants.yaml` plus
        # `<deployment>/invariants/*.yaml` and installed
        # against the new cv. Phase B reads `okg.invariants`
        # to evaluate per-cycle violations.
        #
        # Per `add-semantic-llm-judge-invariants` (2026-04-29):
        # writes the LLM-judge columns alongside the legacy ones.
        # The bi-conditional CHECK on the table guarantees that
        # the loader's per-kind validation already filled the
        # right column subset; passing all eight here is safe.
        if composed.invariants:
            # Per `add-concept-scoped-invariants` (capability E):
            # `concept_scope` is JSONB; tuple `hop_edges` becomes a
            # JSON list at the cast site so the round-trip is
            # symmetric with `_load_invariants_for_cv`.
            import json as _json

            def _scope_json(inv) -> str | None:
                if inv.concept_scope is None:
                    return None
                cs = dict(inv.concept_scope)
                if isinstance(cs.get("hop_edges"), tuple):
                    cs["hop_edges"] = list(cs["hop_edges"])
                return _json.dumps(cs)

            cur.executemany(
                "INSERT INTO okg.invariants "
                "  (name, severity, scope, source, sql_body, "
                "   timeout_ms, kind, "
                "   candidates_sql, judge_prompt, judge_model, "
                "   sample_cap, fire_on, "
                "   judge_spend_cap_usd, judge_spend_estimate_usd, "
                "   concept_scope, requires_sources, "
                "   catalog_version_id) "
                "VALUES (%s, %s, %s, %s, %s, "
                "        %s, %s, "
                "        %s, %s, %s, "
                "        %s, %s, "
                "        %s, %s, "
                "        %s::jsonb, %s, "
                "        %s)",
                [
                    (
                        inv.name, inv.severity, inv.scope,
                        inv.source, inv.sql_body, inv.timeout_ms,
                        inv.kind,
                        inv.candidates_sql, inv.judge_prompt,
                        inv.judge_model, inv.sample_cap,
                        list(inv.fire_on) if inv.fire_on else None,
                        inv.judge_spend_cap_usd,
                        inv.judge_spend_estimate_usd,
                        _scope_json(inv),
                        # Issue #3624: sources whose being off skips it.
                        (
                            list(inv.requires_sources)
                            if inv.requires_sources else None
                        ),
                        cv,
                    )
                    for inv in composed.invariants
                ],
            )

        # Compute which subtypes' `searchable_text_fields` changed vs
        # the prior applied catalog version. The actual graph re-stamp
        # runs after this relational transaction while the new cv is still
        # pending, so readers cannot observe incomplete catalog authority.
        prior_declarations: dict[str, tuple[list[str], list[str]]] = {}
        changed_subtypes = _subtypes_with_changed_search_fields(
            cur, cv, composed, prior_out=prior_declarations,
        )

    _complete_catalog_application(
        conn,
        composed,
        catalog_version_id=cv,
        changed_subtypes=changed_subtypes,
        prior_declarations=prior_declarations,
        identity=identity,
        apply_report=apply_report,
    )
    return cv


def _complete_catalog_application(
    conn: psycopg.Connection,
    composed: ComposedCatalog,
    *,
    catalog_version_id: int,
    changed_subtypes: list[str],
    identity: Any | None,
    apply_report: dict[str, Any] | None,
    prior_declarations: dict[str, tuple[list[str], list[str]]] | None = None,
) -> None:
    """Complete every required side effect before exposing catalog authority.

    Graph, admission, alias, ownership, and activation writes share one outer
    PostgreSQL transaction. Helpers may use nested transactions for their local
    invariants; on this connection those are savepoints, so a later failure
    removes every earlier effect before the catalog version is durably marked
    ``rolled_back``.
    """
    cv = catalog_version_id
    deployment_name = (
        identity.deployment_name if identity is not None else composed.deployment_name
    )
    stage = "concept_seed"
    alias_seeding: dict[str, Any] | None = None

    try:
        with conn.transaction():
            # okg#3112 step 2 (design section 4.3 rule 1): cancel a
            # declared-attribute index build that holds the graph-index DDL
            # lock, and hold that lock (and the preemption intent) until this
            # transaction ends, so no new build starts under the apply. The
            # cancelled build is dropped on the reconciler's next run and not
            # counted as a failure.
            stage = "declared_attr_index_preemption"
            from okg.substrate.declared_attr_indexes import preempt_in_transaction

            declared_attr_index_preemption = preempt_in_transaction(
                conn, actor="catalog_apply"
            )
            if apply_report is not None:
                apply_report["declared_attr_index_preemption"] = (
                    declared_attr_index_preemption
                )
            stage = "concept_seed"
            if composed.concept_seeds:
                from okg.substrate.catalog.concept_seed import upsert_concept_nodes

                upsert_concept_nodes(
                    conn,
                    catalog_version_id=cv,
                    deployment_name=deployment_name,
                    seeds=composed.concept_seeds,
                )

            stage = "searchable_text_restamp"
            restamp_summary = _restamp_changed_subtypes(
                conn,
                changed_subtypes,
                deployment_name=deployment_name,
                catalog_version_id=cv,
                prior_declarations=prior_declarations,
            )
            if restamp_summary:
                _log.info(
                    "catalog re-stamped existing graph nodes for "
                    "subtypes whose searchable_text_fields changed: %s",
                    restamp_summary,
                )

            stage = "alias_seeding"
            if composed.aliases:
                from okg.substrate.catalog.aliases import (
                    alias_schema_guard,
                    invalidate_link_cache_for_landed_aliases,
                )

                with alias_schema_guard(), conn.cursor() as cur:
                    cur.executemany(
                        "INSERT INTO okg.aliases "
                        "  (alias, normalized_alias, canonical, scope, "
                        "   catalog_version_id) "
                        "VALUES (%s, %s, %s, %s, %s) "
                        "ON CONFLICT (alias, catalog_version_id) DO NOTHING",
                        [
                            (
                                alias["alias"],
                                normalize_entity_name(alias["alias"]),
                                alias["canonical"],
                                alias.get("scope"),
                                cv,
                            )
                            for alias in composed.aliases
                        ],
                    )
                # #685: aliases and their retry wakeups commit together with
                # every other post-definition catalog effect.
                invalidate_link_cache_for_landed_aliases(
                    conn,
                    [alias["alias"] for alias in composed.aliases],
                )

            alias_seeding = _seed_alias_fields_for_catalog(conn, cv)
            if alias_seeding["collision_count"]:
                _log.warning(
                    "catalog apply refused %s ambiguous alias values and quarantined "
                    "%s existing winners: %s",
                    alias_seeding["collision_count"],
                    alias_seeding["quarantined_count"],
                    alias_seeding["collision_samples"],
                )

            stage = "ownership_receipt"
            if identity is not None:
                from okg.substrate.catalog.ownership import mark_catalog_version_seen

                mark_catalog_version_seen(
                    conn,
                    identity=identity,
                    catalog_version_id=cv,
                )

            # okg#3112 step 2: ask for a declared-attribute index run. Only a
            # request row, committed with the apply; the next publisher cycle
            # enqueues the workflow, or its five-minute schedule runs it.
            stage = "declared_attr_index_request"
            from okg.substrate.declared_attr_indexes import record_maintenance_request

            requested = record_maintenance_request(conn, reason="catalog_apply")
            if apply_report is not None:
                apply_report["declared_attr_index_request"] = requested

            stage = "activation"
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE okg.catalog_versions "
                    "   SET status = 'applied', applied_at = now() "
                    " WHERE catalog_version_id = %s "
                    "   AND status = 'pending'",
                    (cv,),
                )
                if cur.rowcount != 1:
                    raise RuntimeError(
                        f"catalog_version {cv} not in 'pending' state; "
                        "activation authority changed during apply"
                    )
    except Exception as exc:
        rollback_error: str | None = None
        rolled_back = False
        try:
            with conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    "UPDATE okg.catalog_versions "
                    "   SET status = 'rolled_back', applied_at = NULL "
                    " WHERE catalog_version_id = %s "
                    "   AND status = 'pending'",
                    (cv,),
                )
                rolled_back = cur.rowcount == 1
        except Exception as rollback_exc:  # noqa: BLE001 - preserve both failures.
            rollback_error = str(rollback_exc)

        cause_data = getattr(exc, "data", None)
        moved = (
            isinstance(cause_data, dict)
            and cause_data.get("reason") == "row_provenance_rows_moved"
        )
        raise CatalogMigrationRefused(
            f"catalog_version {cv} failed during {stage}; catalog is not active",
            data={
                "catalog_version_id": cv,
                "stage": stage,
                "status": "rolled_back" if rolled_back else "pending_or_changed",
                "cause": str(exc),
                **({"rollback_error": rollback_error} if rollback_error else {}),
                **({
                    "reason": "row_provenance_rows_moved",
                    "moved_node_ids": [
                        str(item.get("node_id"))
                        for item in cause_data.get("moved") or []
                        if isinstance(item, dict)
                    ],
                    "moved_count": cause_data.get("moved_count"),
                } if moved else {}),
            },
            fix_hint=(
                "a graph writer changed these nodes during the searchable-text "
                "restamp; pause the worker or retry the load"
                if moved
                else "repair the named stage and retry catalog apply"
            ),
        ) from exc

    if apply_report is not None and alias_seeding is not None:
        apply_report["alias_seeding"] = alias_seeding


def _seed_alias_fields_for_catalog(
    conn: psycopg.Connection,
    catalog_version_id: int,
) -> dict[str, Any]:
    """Seed alias fields for a pending catalog without selecting global state."""
    from okg.substrate.catalog.aliases import seed_from_subtype

    fields: dict[str, dict[str, int]] = {}
    totals = {
        "inserted_count": 0,
        "idempotent_count": 0,
        "collision_count": 0,
        "quarantined_count": 0,
    }
    collision_samples: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, metadata FROM okg.subtypes "
            " WHERE catalog_version_id = %s",
            (catalog_version_id,),
        )
        rows = cur.fetchall()
    for subtype, metadata in rows:
        for field in (metadata or {}).get("alias_fields") or []:
            result = seed_from_subtype(
                conn,
                subtype=subtype,
                field=field,
                apply=True,
                _cv_override=catalog_version_id,
            )
            key = f"{subtype}.{field}"
            fields[key] = {
                "inserted_count": result.inserted_count,
                "idempotent_count": result.idempotent_count,
                "collision_count": result.collision_count,
                "quarantined_count": result.quarantined_count,
            }
            for name in totals:
                totals[name] += fields[key][name]
            for collision in result.collisions:
                if len(collision_samples) >= 50:
                    break
                collision_samples.append(
                    {
                        "subtype": collision.subtype,
                        "field": collision.field,
                        "alias_value": collision.alias_value,
                        "canonicals": collision.canonicals,
                        "distinct_canonical_count": (
                            collision.distinct_canonical_count
                        ),
                    }
                )
    return {
        **totals,
        "fields": fields,
        "collision_samples": collision_samples,
        "collision_samples_truncated": (
            totals["collision_count"] > len(collision_samples)
        ),
    }


def _subtypes_with_changed_search_fields(
    cur: psycopg.Cursor,
    new_cv: int,
    composed: ComposedCatalog,
    *,
    prior_out: dict[str, tuple[list[str], list[str]]] | None = None,
) -> list[str]:
    """Return the subtypes whose searchable-text annotations differ
    between the prior applied catalog version and `new_cv`.

    `prior_out`, when given, receives each changed subtype's PRIOR
    `(searchable_text_fields, searchable_text_path_fields)`: the restamp
    needs the declaration a stored node was stamped under to tell an old
    rollup from a producer body (issue #2991).

    Both `searchable_text_fields` and (per
    `index-document-source-file-search`, issue #900)
    `searchable_text_path_fields` are diffed: adding path fields alone
    changes the stamped rollup, so existing rows need the same re-stamp.

    Pure relational diff (no graph touch). When this is the first
    applied catalog version, every declared annotation counts as a
    change — but there are no graph nodes to re-stamp yet, so the
    re-stamp pass is a no-op.
    """
    if not composed.subtypes:
        return []

    # Find the most recent applied catalog version BEFORE this one.
    cur.execute(
        "SELECT max(catalog_version_id) "
        "  FROM okg.catalog_versions "
        " WHERE status = 'applied' "
        "   AND catalog_version_id < %s",
        (new_cv,),
    )
    prior_cv_row = cur.fetchone()
    prior_cv = prior_cv_row[0] if prior_cv_row else None

    # Read prior fields keyed by subtype. Empty dict when no prior
    # cv exists (first install).
    metadata_keys = ("searchable_text_fields", "searchable_text_path_fields")
    prior_fields_by_subtype: dict[str, tuple[list[str], ...]] = {}
    if prior_cv is not None:
        cur.execute(
            "SELECT name, metadata->'searchable_text_fields', "
            "       metadata->'searchable_text_path_fields' "
            "  FROM okg.subtypes "
            " WHERE catalog_version_id = %s "
            "   AND name = ANY(%s)",
            (prior_cv, sorted(composed.subtypes)),
        )
        for name, fields_jsonb, path_fields_jsonb in cur.fetchall():
            prior_fields_by_subtype[name] = tuple(
                [] if raw is None else list(raw)
                for raw in (fields_jsonb, path_fields_jsonb)
            )

    changed: list[str] = []
    for subtype in sorted(composed.subtypes):
        metadata = composed.subtype_metadata.get(subtype, {})
        new_fields = tuple(
            list(metadata.get(key, [])) for key in metadata_keys
        )
        prior_fields = prior_fields_by_subtype.get(
            subtype, ([], []),
        )
        if new_fields != tuple(list(item) for item in prior_fields):
            changed.append(subtype)
            if prior_out is not None:
                prior_out[subtype] = (
                    list(prior_fields[0]), list(prior_fields[1]),
                )
    return changed


def _restamp_changed_subtypes(
    conn: psycopg.Connection,
    changed_subtypes: list[str],
    *,
    deployment_name: str,
    catalog_version_id: int,
    prior_declarations: dict[str, tuple[list[str], list[str]]] | None = None,
) -> dict[str, int]:
    """Re-stamp `attrs.text` (and recompute `text_hash`) for every
    Chronos graph node of each changed subtype.

    Reads the affected nodes from `okg.graph_nodes` via a Chronos
    checkout and re-emits them through `apply_facts`, which re-runs
    `_stamp_searchable_text` against the now-applied catalog's
    `searchable_text_fields` and recomputes `text_hash`. Returns
    `{subtype: n_nodes_restamped}` for the affected subtypes.

    Each stored row is first turned back into what its producer wrote,
    under the declaration it was stamped with (`prior_declarations`; see
    `unstamp_searchable_text`): a producer body comes back from its
    reserved copy and a plain old rollup is dropped, so the new stamp
    neither loses the body nor reads the old rollup as one (issue #2991).
    A subtype with no prior declaration was never stamped; its rows are
    re-emitted as stored.

    The rewrite changes only the searchable text and its hash (issue
    #3318). Every row keeps its own source, record id, revision, run id,
    observed time and lineage, so the source that wrote a node still owns
    it and its next completed scope can still retract it. `_catalog:restamp`
    owns the admission envelope that records the rewrite, never a row.

    The branch write lock is taken before the first read and held to the
    end of the catalog transaction (review S1 of #3318). Every graph writer
    takes that lock before it writes, so no row can change between the read
    and the rewrite; a writer that arrives meanwhile waits for the catalog
    load to commit and then writes on top of the restamped row.
    """
    if not changed_subtypes:
        return {}

    from okg.substrate.fact_application import apply_bounded_fact_batch
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
    from okg.substrate.searchable_text import unstamp_searchable_text

    store = ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        autocommit=False,
        catalog_version_id=catalog_version_id,
    )
    from okg.substrate.graph_store_internal.common import DEFAULT_BRANCH

    # Transaction-scoped and re-entrant: the rewrite below takes it again.
    store._lock_branch_write(DEFAULT_BRANCH)
    summary: dict[str, int] = {}
    for subtype in changed_subtypes:
        rows = store.checkout().query(
            """
            SELECT node_id, attrs, source, source_record_id,
                   source_revision, run_id, observed_at
              FROM okg.graph_nodes
             WHERE subtype = :subtype
            """,
            {"subtype": subtype},
        )
        if not rows:
            continue
        # OPERATOR COST, per `index-document-source-file-search` (issue
        # #900). Changing a subtype's searchable-text declaration re-stamps
        # every existing node of that subtype, synchronously, inside
        # `okg catalog load --apply`. On okg-workspace that is ~27k
        # `source_file` and ~4k `document` nodes. Each re-stamp rewrites
        # `attrs.text`, hence `text_hash` (projection/projector.py), hence
        # the embedding cache key — so every re-stamped node is a re-embed
        # cache miss on the next embed pass, plus one new chronos row
        # version. That cost is inherent to the declaration change.
        #
        # The write itself is already bounded: `apply_bounded_fact_batch`
        # chunks internally at MAX_FACTS_PER_CHUNK (fact_batch.py). An
        # outer chunk loop here would NOT reduce peak memory — the whole
        # subtype is already materialized by the query above — and would
        # multiply the admission envelope + receipt per subtype, so there
        # deliberately isn't one.
        prior_fields, prior_path_fields = (prior_declarations or {}).get(
            subtype, ([], []),
        )
        # With no new declaration the stamper leaves attrs alone, so an old
        # plain rollup is kept rather than dropped: the pre-#2991 restamp
        # kept it, and dropping it would take the node out of search.
        keep_plain_rollup = not store._searchable_text_fields(subtype)
        facts = [
            NodeFact(
                row["node_id"],
                subtype,
                unstamp_searchable_text(
                    dict(row["attrs"] or {}), prior_fields, prior_path_fields,
                    keep_plain_rollup=keep_plain_rollup,
                ),
                dict(row["source_record_id"] or {}),
                dict(row["source_revision"] or {}),
            )
            for row in rows
        ]
        # Each row keeps the provenance it was read with (issue #3318).
        # The branch lock taken above keeps the rows as read; the writer's
        # `expected_source` check is a backstop for a writer that bypassed it.
        row_provenance = {
            str(row["node_id"]): {
                "source": row["source"],
                "run_id": row["run_id"],
                "observed_at": row["observed_at"],
                "lineage": (row["attrs"] or {}).get(LINEAGE_ATTR),
                "expected_source": row["source"],
            }
            for row in rows
        }
        # `apply_facts` re-stamps searchable text + recomputes
        # text_hash using the latest applied catalog's field set.
        result = apply_bounded_fact_batch(
            conn,
            store,
            facts,
            deployment_name=deployment_name,
            producer_id="_catalog:restamp",
            producer_kind="system",
            intent="catalog_searchable_text_restamp",
            catalog_version_id=catalog_version_id,
            authority_scope={
                "catalog_version_id": catalog_version_id,
                "subtype": subtype,
            },
            strict_endpoints=True,
            row_provenance=row_provenance,
        )
        nodes_upserted = result.primitive_total("nodes_upserted")
        if nodes_upserted:
            summary[subtype] = nodes_upserted

    return summary
