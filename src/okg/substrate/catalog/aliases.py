"""Operator-facing alias management.

Per `refine-doc-corpus-extraction` (2026-04-26): seeding the
alias dictionary stays manual by design — auto-seeding from
arbitrary node attrs would couple the substrate to per-
deployment naming conventions and produce silent false-positive
resolutions. This module ships the bulk-import + ad-hoc
operator helpers behind `okg alias <subcmd>` and the MCP
`register_alias` / `suggest_aliases` tools.

Functions here operate on the ACTIVE catalog version (max
applied `catalog_version_id`); they refuse to mutate when no
catalog is applied.
"""
from __future__ import annotations

import contextlib
import json
from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any, Iterable, Iterator, Optional

import psycopg

from okg.substrate.errors import SchemaValidationFailed, SubstrateError
from okg.substrate.entity_resolution import normalize_entity_name


__all__ = [
    "AliasCollision",
    "AliasNoActiveCatalog",
    "SeedFromSubtypeResult",
    "active_catalog_version_id",
    "add_alias",
    "alias_schema_guard",
    "backfill_normalized_aliases",
    "invalidate_link_cache_for_landed_aliases",
    "list_aliases",
    "list_alias_collisions",
    "remove_alias",
    "seed_all_alias_fields",
    "seed_from_subtype",
    "suggest_aliases_from_unresolved_mentions",
]


class AliasNoActiveCatalog(SubstrateError):
    code = "alias_no_active_catalog"


@contextlib.contextmanager
def alias_schema_guard() -> Iterator[None]:
    """Turn a pre-`normalized_alias` `okg.aliases` shape into an actionable error.

    `normalized_alias` (#796) is added by the 0000 schema snapshot, which
    `okg migrate` re-applies wholesale. Every alias writer names the column
    unconditionally, so code deployed ahead of its migrate would otherwise die
    with a bare psycopg `UndefinedColumn` on catalog apply and `okg alias add`.
    Fail loudly and name the remedy; never fall back to writing an alias row
    with no normalized identity, which would silently shadow the resolver.
    """
    try:
        yield
    except psycopg.errors.UndefinedColumn as exc:
        # Only relabel the column this guard is about. A missing `scope` or a
        # typo in a future edit to the INSERT would otherwise be reported as
        # "run okg migrate", sending the operator down a dead end.
        if "normalized_alias" not in str(exc):
            raise
        raise SchemaValidationFailed(
            "okg.aliases is missing the normalized_alias column",
            data={"table": "okg.aliases", "column": "normalized_alias"},
            fix_hint="run `okg migrate` to re-apply the schema snapshot",
        ) from exc


def backfill_normalized_aliases(
    conn: psycopg.Connection, *, batch_size: int = 5000
) -> int:
    """Populate `normalized_alias` on rows that predate the column (#796).

    `ADD COLUMN` leaves existing rows NULL and no writer updates them in
    place, so on an upgraded deployment every historical alias is invisible to
    the normalized lookup until its catalog is recomposed. Resolution would
    then mint a fresh canonical instead of matching the curated alias — the
    exact duplicate this stage exists to prevent.

    Normalization runs in Python, not SQL: `normalize_entity_name` is NFKC +
    casefold plus Unicode-category filtering, and a hand-rolled SQL equivalent
    would drift from it. A drifting key is worse than a missing one, because
    the two spellings would claim different identities.

    Bounded: rewrites at most `batch_size` rows per statement. A row whose
    name normalizes to the empty string is written as `''`, not left NULL, so
    the scan terminates. Returns the number of rows rewritten.
    """
    total = 0
    while True:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT alias, catalog_version_id FROM okg.aliases "
                "WHERE normalized_alias IS NULL LIMIT %s",
                (batch_size,),
            )
            rows = cur.fetchall()
            if not rows:
                return total
            cur.execute(
                "UPDATE okg.aliases AS a SET normalized_alias = t.normalized "
                "FROM unnest(%s::text[], %s::bigint[], %s::text[]) "
                "     AS t(alias, catalog_version_id, normalized) "
                "WHERE a.alias = t.alias "
                "  AND a.catalog_version_id = t.catalog_version_id",
                (
                    [str(alias) for alias, _ in rows],
                    [int(cv) for _, cv in rows],
                    [normalize_entity_name(str(alias)) for alias, _ in rows],
                ),
            )
            total += len(rows)


def invalidate_link_cache_for_landed_aliases(
    conn: psycopg.Connection,
    alias_values: Iterable[str],
) -> int:
    """Wake extraction retries whose target alias just landed (#685).

    Per `fix-projection-alias-identity` (ruling 1): the projector records a
    resolver miss in `okg.link_retry` when a mentioner is projected before its
    target's alias exists. When an alias value lands (via `add_alias`,
    `seed_from_subtype`, or compose-apply), mark matching retry rows ready.
    Production Publisher cycles use that indexed affected-node set and only
    acknowledge the exact rows after a published Chronos checkpoint. The same
    bounded node set invalidates compatibility ``link_cache`` rows. Returns
    that compatibility invalidation count.

    O(new aliases), never O(corpus): the match is an indexed lookup on
    `link_retry.entity_value`, and only nodes that actually strand on one of
    these values are touched.
    """
    from okg.substrate.projection.retry import mark_link_retries_ready_for_aliases

    return mark_link_retries_ready_for_aliases(conn, alias_values)


@dataclass
class AliasCollision:
    """One ambiguous alias value refused at seed time (#686, ruling 2).

    `alias_value` maps to `distinct_canonical_count` (>1) distinct canonical
    node ids under `(subtype, field)`; it is seeded for NO node. `canonicals`
    is the sorted distinct id list.
    """

    subtype: str
    field: str
    alias_value: str
    canonicals: list[str]
    distinct_canonical_count: int
    scope: Optional[str] = None


@dataclass
class SeedFromSubtypeResult:
    """Outcome of `seed_from_subtype`.

    `dry_run=True` paths populate the counts without writing
    anything; `dry_run=False` writes and the counts reflect the
    actual outcome.

    Per `fix-projection-alias-identity` (ruling 2), `conflict_count` splits:
    `idempotent_count` is values already present at the active cv with the
    SAME canonical (a benign re-run no-op), and `collision_count` is values
    refused because they map to >1 distinct canonical. `conflict_count` is
    retained as `idempotent_count + collision_count` for back-compat.
    `collisions` lists each refused value.
    """

    catalog_version_id: int
    subtype: str
    field: str
    inserted_count: int
    conflict_count: int
    dry_run: bool
    canonical_template: str
    sample_inserted: list[tuple[str, str]]
    idempotent_count: int = 0
    collision_count: int = 0
    quarantined_count: int = 0
    sample_quarantined: list[tuple[str, str]] = dc_field(default_factory=list)
    collisions: list["AliasCollision"] = dc_field(default_factory=list)


def active_catalog_version_id(
    conn: psycopg.Connection,
) -> int:
    """Return the max applied catalog_version_id. Raise
    `AliasNoActiveCatalog` when none exists."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(catalog_version_id) "
            "  FROM okg.catalog_versions WHERE status = 'applied'"
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        raise AliasNoActiveCatalog(
            "no applied catalog_version; alias operations need "
            "an active catalog (run `okg catalog load --apply` "
            "first)",
            fix_hint="okg catalog load --apply --deployment <dir>",
        )
    return int(row[0])


def _render_canonical(
    template: str, attrs: dict[str, Any], node_id: str,
) -> Optional[str]:
    """Format `template` against the row's attrs; return None
    on missing keys (caller skips those rows). The literal
    `{node_id}` is supported as a special key."""
    fmt_attrs = dict(attrs or {})
    fmt_attrs["node_id"] = node_id
    try:
        return template.format(**fmt_attrs)
    except (KeyError, IndexError):
        return None


def seed_from_subtype(
    conn: psycopg.Connection,
    *,
    subtype: str,
    field: str,
    scope: Optional[str] = None,
    canonical_template: Optional[str] = None,
    apply: bool = False,
    sample_limit: int = 10,
    branch_name: str = "default",
    _cv_override: Optional[int] = None,
    _graph_override: Any = None,
) -> SeedFromSubtypeResult:
    """Bulk-import aliases from the current graph nodes of `subtype`.

    For each node of the named subtype (read from the Chronos graph)
    whose `attrs->>field` is non-empty, an alias row is prepared:

      alias     = the field's string value
      canonical = `canonical_template.format(**attrs, node_id=...)`
                  defaulting to the node_id itself when no
                  template is supplied
      scope     = the `scope` arg (None → global alias)

    With `apply=False`, returns the preview — no rows mutate.
    With `apply=True`, inserts rows under the active cv with
    `ON CONFLICT (alias, catalog_version_id) DO NOTHING` so
    re-runs are idempotent (existing rows are counted in
    `idempotent_count`, not errors).

    Per `fix-projection-alias-identity` (#686, ruling 2): a value that maps
    to MORE THAN ONE distinct canonical -- whether two graph nodes in this
    batch share the field value, or the value already exists at the active cv
    with a different canonical -- is a COLLISION. It is refused per value: it
    is seeded for NO node (an ambiguous surface form must resolve to nothing),
    a typed record is persisted in `okg.alias_collisions`, and it is reported
    in the result's `collisions`. Non-colliding values seed normally.
    Exclusion is stable across applies (there is no arbitrary winner to flip).

    `_cv_override` lets a caller (typically `seed_all_alias_fields`)
    pass in a cached catalog_version_id, saving N redundant
    `SELECT max(catalog_version_id)` round-trips when seeding many
    subtypes in one publish cycle.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from psycopg.pq import TransactionStatus

    cv = _cv_override if _cv_override is not None else active_catalog_version_id(conn)
    template = canonical_template or "{node_id}"
    rows: list[tuple[str, str]] = []
    graph = _graph_override
    if graph is None:
        graph = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=bool(
                conn.autocommit
                and conn.info.transaction_status == TransactionStatus.IDLE
            ),
        ).checkout(branch_name)
    from okg.substrate.catalog.linker_config import (
        LINK_ENDPOINT_SOURCE,
        LOAD_STATE_ATTR,
        NOT_LOADED,
    )

    # Issue #3666: a "known by key, not loaded" link endpoint seeds no
    # alias. The landing hook wakes retry rows by alias value alone, so an
    # endpoint alias would resolve its own misses and outlive the endpoint;
    # only the real record, when it lands, may register the key.
    node_rows = graph.query(
        """
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = :subtype
           AND NOT (coalesce(source, '') = :endpoint_source
                    AND coalesce(attrs->>:state_attr, '') = :not_loaded)
        """,
        {
            "subtype": subtype,
            "endpoint_source": LINK_ENDPOINT_SOURCE,
            "state_attr": LOAD_STATE_ATTR,
            "not_loaded": NOT_LOADED,
        },
    )
    for node_row in node_rows:
        node_id = node_row["node_id"]
        attrs = node_row["attrs"]
        value = (attrs or {}).get(field)
        if not value or not isinstance(value, str):
            continue
        canonical = _render_canonical(
            template, attrs, node_id,
        )
        if not canonical:
            continue
        rows.append((value, canonical))

    # value -> set of distinct canonicals proposed this batch.
    proposed: dict[str, set[str]] = {}
    for value, canonical in rows:
        proposed.setdefault(value, set()).add(canonical)

    # Merge in canonicals already seeded at the active cv for these values:
    # a value already mapped to a DIFFERENT canonical is also a collision.
    existing_canonical: dict[str, str] = {}
    if rows:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT alias, canonical FROM okg.aliases "
                " WHERE catalog_version_id = %s AND alias = ANY(%s)",
                (cv, list(proposed.keys())),
            )
            for alias_value, existing in cur.fetchall():
                existing_canonical[alias_value] = existing
                proposed.setdefault(alias_value, set()).add(existing)

    collision_values = {v for v, cs in proposed.items() if len(cs) > 1}
    collisions = [
        AliasCollision(
            subtype=subtype,
            field=field,
            alias_value=v,
            canonicals=sorted(proposed[v]),
            distinct_canonical_count=len(proposed[v]),
            scope=scope,
        )
        for v in sorted(collision_values)
    ]

    # Non-colliding values: exactly one canonical each. Split into
    # would-insert (no row yet at cv) vs idempotent (already present, same
    # canonical). Preserve first-seen order for stable samples.
    noncolliding_pairs: list[tuple[str, str]] = []
    seen: set[str] = set()
    for value, canonical in rows:
        if value in collision_values or value in seen:
            continue
        seen.add(value)
        noncolliding_pairs.append((value, canonical))
    would_insert = [
        (v, c) for (v, c) in noncolliding_pairs if v not in existing_canonical
    ]
    idempotent_count = len(noncolliding_pairs) - len(would_insert)

    if not apply:
        return SeedFromSubtypeResult(
            catalog_version_id=cv,
            subtype=subtype, field=field,
            inserted_count=len(would_insert),
            conflict_count=idempotent_count + len(collisions),
            dry_run=True,
            canonical_template=template,
            sample_inserted=would_insert[:sample_limit],
            idempotent_count=idempotent_count,
            collision_count=len(collisions),
            collisions=collisions,
        )

    # Apply path. Quarantining a late winner, recording the collision, and
    # inserting unambiguous values are one transaction even on an autocommit
    # operator connection. There must never be an observable collision record
    # beside an active arbitrary winner.
    inserted_rows: list[tuple[str, str]] = []
    quarantined_rows: list[tuple[str, str]] = []
    with conn.transaction():
        with conn.cursor() as cur:
            if collision_values:
                cur.execute(
                    """
                    DELETE FROM okg.aliases
                     WHERE catalog_version_id = %s
                       AND alias = ANY(%s)
                    RETURNING alias, canonical
                    """,
                    (cv, sorted(collision_values)),
                )
                quarantined_rows = cur.fetchall()

            if would_insert:
                with alias_schema_guard():
                    cur.execute(
                        """
                        INSERT INTO okg.aliases
                          (alias, normalized_alias, canonical, scope,
                           catalog_version_id)
                        SELECT alias, normalized_alias, canonical, %s, %s
                          FROM unnest(%s::text[], %s::text[], %s::text[])
                               AS t(alias, normalized_alias, canonical)
                         WHERE alias IS NOT NULL
                           AND canonical IS NOT NULL
                        ON CONFLICT (alias, catalog_version_id) DO NOTHING
                        RETURNING alias, canonical
                        """,
                        (
                            scope,
                            cv,
                            [a for a, _ in would_insert],
                            [normalize_entity_name(a) for a, _ in would_insert],
                            [c for _, c in would_insert],
                        ),
                    )
                    inserted_rows = cur.fetchall()

            # Sweep the whole (catalog, subtype, field) surface. A collision
            # can disappear because every former candidate vanished, so stale
            # cleanup cannot be limited to values present in today's batch.
            cur.execute(
                """
                DELETE FROM okg.alias_collisions
                 WHERE catalog_version_id = %s
                   AND subtype = %s AND field = %s
                   AND NOT (alias_value = ANY(%s))
                """,
                (cv, subtype, field, sorted(collision_values)),
            )
            for coll in collisions:
                cur.execute(
                    """
                    INSERT INTO okg.alias_collisions
                      (catalog_version_id, subtype, field, alias_value,
                       scope, canonicals, distinct_canonical_count)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (catalog_version_id, subtype, field, alias_value)
                    DO UPDATE SET
                      scope = EXCLUDED.scope,
                      canonicals = EXCLUDED.canonicals,
                      distinct_canonical_count = EXCLUDED.distinct_canonical_count,
                      detected_at = now()
                    """,
                    (
                        cv, subtype, field, coll.alias_value, scope,
                        json.dumps(coll.canonicals),
                        coll.distinct_canonical_count,
                    ),
                )

        # #685: values that actually landed wake exactly their waiting
        # mentioners. Keep this in the transaction so catalog apply cannot
        # expose an alias before its retry work is ready.
        if inserted_rows:
            invalidate_link_cache_for_landed_aliases(
                conn, [a for a, _ in inserted_rows]
            )

    return SeedFromSubtypeResult(
        catalog_version_id=cv,
        subtype=subtype, field=field,
        inserted_count=len(inserted_rows),
        conflict_count=idempotent_count + len(collisions),
        dry_run=False,
        canonical_template=template,
        sample_inserted=inserted_rows[:sample_limit],
        idempotent_count=idempotent_count,
        collision_count=len(collisions),
        quarantined_count=len(quarantined_rows),
        sample_quarantined=quarantined_rows[:sample_limit],
        collisions=collisions,
    )


def seed_all_alias_fields(
    conn: psycopg.Connection,
    *,
    only_subtypes: Optional[Iterable[str]] = None,
) -> dict[str, Any]:
    """Walk every subtype declared in the active catalog whose
    `okg.subtypes.metadata` carries an `alias_fields` list, and
    seed `okg.aliases` from the current graph nodes of that subtype.

    Per `declarative-cross-source-linker` (2026-04-26): subtypes
    declare `okg_alias_fields` in their LinkML annotations; the
    catalog compose stage stamps that into subtype metadata; this
    helper walks the metadata at the active cv and bulk-imports.

    For each `(subtype, field)` pair, every graph node whose
    `attrs->>field` is non-empty becomes an alias row mapping
    `value -> node_id`. Idempotent on `(alias, cv)`. Re-runs at
    the same cv are no-ops aside from ON CONFLICT.

    `only_subtypes` (optional): when provided, restrict seeding
    to subtypes whose name appears in this set. The publisher
    passes the set of subtypes that had dirty emissions this
    cycle — re-seeding subtypes whose graph set didn't change is
    pure waste (idempotent ON CONFLICT, but still costs the
    SELECT + INSERT round-trips per subtype). With this filter,
    a 1-source-file commit re-seeds 1 subtype instead of all
    ~11 alias-bearing subtypes in the okg-workspace catalog.
    Per `add-per-source-sync-perf-harness` (2026-05-12):
    saves ~50 ms per commit on the hot path.

    Returns an automatic bounded aggregate for catalog-apply telemetry.
    """
    cv = active_catalog_version_id(conn)
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
            (cv,),
        )
        rows = cur.fetchall()
    allow: Optional[set[str]] = (
        set(only_subtypes) if only_subtypes is not None else None
    )
    for subtype, metadata in rows:
        if allow is not None and subtype not in allow:
            continue
        alias_fields = (metadata or {}).get("alias_fields") or []
        for field in alias_fields:
            res = seed_from_subtype(
                conn,
                subtype=subtype,
                field=field,
                apply=True,
                _cv_override=cv,   # skip per-call cv lookup
            )
            key = f"{subtype}.{field}"
            fields[key] = {
                "inserted_count": res.inserted_count,
                "idempotent_count": res.idempotent_count,
                "collision_count": res.collision_count,
                "quarantined_count": res.quarantined_count,
            }
            for name in totals:
                totals[name] += fields[key][name]
            for collision in res.collisions:
                if len(collision_samples) >= 50:
                    break
                collision_samples.append({
                    "subtype": collision.subtype,
                    "field": collision.field,
                    "alias_value": collision.alias_value,
                    "canonicals": collision.canonicals,
                    "distinct_canonical_count": (
                        collision.distinct_canonical_count
                    ),
                })
    return {
        **totals,
        "fields": fields,
        "collision_samples": collision_samples,
        "collision_samples_truncated": (
            totals["collision_count"] > len(collision_samples)
        ),
    }


def list_aliases(
    conn: psycopg.Connection,
    *,
    scope: Optional[str] = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """List aliases at the active cv, optionally filtered by
    scope. Returns a list of dicts with keys `alias`,
    `canonical`, `scope`, `catalog_version_id`."""
    cv = active_catalog_version_id(conn)
    where = ["catalog_version_id = %s"]
    params: list[Any] = [cv]
    if scope is not None:
        where.append("scope IS NOT DISTINCT FROM %s")
        params.append(scope)
    sql = (
        "SELECT alias, canonical, scope, catalog_version_id "
        "  FROM okg.aliases "
        " WHERE " + " AND ".join(where) +
        " ORDER BY alias LIMIT %s"
    )
    params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [
            {
                "alias": a, "canonical": c,
                "scope": s, "catalog_version_id": cv_,
            }
            for (a, c, s, cv_) in cur.fetchall()
        ]


def add_alias(
    conn: psycopg.Connection,
    *,
    alias: str,
    canonical: str,
    scope: Optional[str] = None,
) -> dict[str, Any]:
    """Single-row insert. Idempotent on
    (alias, catalog_version_id) — re-inserting the same alias
    is a no-op (returns `inserted=False`)."""
    cv = active_catalog_version_id(conn)
    with conn.cursor() as cur:
        with alias_schema_guard():
            cur.execute(
                "INSERT INTO okg.aliases "
                "  (alias, normalized_alias, canonical, scope, catalog_version_id) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (alias, catalog_version_id) DO NOTHING",
                (alias, normalize_entity_name(alias), canonical, scope, cv),
            )
        inserted = cur.rowcount == 1
        cur.execute(
            "SELECT canonical FROM okg.aliases "
            "WHERE alias = %s AND catalog_version_id = %s",
            (alias, cv),
        )
        existing = cur.fetchone()
    # #685: wake work only when the requested mapping is the mapping that
    # actually exists.  A conflicting ON CONFLICT no-op did not land.
    landed = bool(existing is not None and str(existing[0]) == canonical)
    invalidated = (
        invalidate_link_cache_for_landed_aliases(conn, [alias])
        if landed else 0
    )
    return {
        "alias": alias, "canonical": canonical, "scope": scope,
        "catalog_version_id": cv, "inserted": inserted, "landed": landed,
        "link_cache_invalidated": invalidated,
    }


def list_alias_collisions(
    conn: psycopg.Connection,
    *,
    catalog_version_id: Optional[int] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """List standing alias-seeding collisions (#686). Defaults to the active
    cv. Each row: subtype, field, alias_value, canonicals, count, scope."""
    cv = (
        catalog_version_id
        if catalog_version_id is not None
        else active_catalog_version_id(conn)
    )
    bounded_limit = max(1, min(int(limit), 1000))
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT subtype, field, alias_value, scope,
                   canonicals, distinct_canonical_count, detected_at
              FROM okg.alias_collisions
             WHERE catalog_version_id = %s
             ORDER BY subtype, field, alias_value
             LIMIT %s
            """,
            (cv, bounded_limit),
        )
        return [
            {
                "catalog_version_id": cv,
                "subtype": s, "field": f, "alias_value": v,
                "scope": sc, "canonicals": canon,
                "distinct_canonical_count": int(n),
                "detected_at": detected,
            }
            for (s, f, v, sc, canon, n, detected) in cur.fetchall()
        ]


def remove_alias(
    conn: psycopg.Connection,
    *,
    alias: str,
) -> dict[str, Any]:
    """Delete the row for `alias` at the active cv. Returns
    `removed=True` when one row was deleted, else False."""
    cv = active_catalog_version_id(conn)
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM okg.aliases "
            " WHERE alias = %s AND catalog_version_id = %s",
            (alias, cv),
        )
        removed = cur.rowcount == 1
    return {
        "alias": alias, "catalog_version_id": cv,
        "removed": removed,
    }


def suggest_aliases_from_unresolved_mentions(
    conn: psycopg.Connection,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Return the top-N unresolved entity_values from the
    `entity_mention` set, ordered by mention count descending.
    "Unresolved" means the mention node has no outgoing
    canonicalization edge in the graph. `mentions` is the canonical
    edge type; `references` is treated as resolved during the
    transition from the earlier declarative-linker edge type.

    Read-only; intended as input to the operator's manual
    alias-population workflow.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False
    ).checkout()
    rows = graph.query(
        """
        WITH unresolved AS (
          SELECT
            m.attrs->>'entity_type' AS entity_type,
            m.attrs->>'entity_value' AS entity_value
            FROM okg.graph_nodes m
           WHERE m.subtype = 'entity_mention'
             AND NOT EXISTS (
               SELECT 1 FROM okg.graph_edges el
                WHERE el.src = m.node_id
                  AND el.edge_type IN ('mentions', 'references')
             )
        )
        SELECT
          entity_type, entity_value, count(*) AS mention_count
          FROM unresolved
         WHERE entity_type IS NOT NULL
           AND entity_value IS NOT NULL
         GROUP BY entity_type, entity_value
         ORDER BY count(*) DESC, entity_value
         LIMIT :limit
        """,
        {"limit": limit},
    )
    return [
        {
            "entity_type": row["entity_type"],
            "entity_value": row["entity_value"],
            "mention_count": int(row["mention_count"]),
        }
        for row in rows
    ]
