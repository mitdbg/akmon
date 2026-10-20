"""Ontology reconcile flow (M5.4).

Per design.md §1607 "Ontology editing and catalog migrations":
  1. Diff proposed catalog against the current applied one.
  2. Classify each change: additive | restrictive | structural | removing.
  3. For non-additive changes, count violators (bounded SQL).
  4. Author picks an action per change in `plan.yaml`.
  5. Apply: one SQL txn per action, followed by one
     `catalog_versions` insert.

SCOPE AT M5.4: narrowings-only diff. subtypes / aliases /
invariants diffs pair with the LinkML loader and are out of
this stride. Narrowings is the most common edit shape and
covers every change class (additive, restrictive, removing).

Actions implemented at M5.4:
  - `apply` (additive only): add the new narrowing row at the
    new catalog_version_id. No data touch.
  - `retract`: for restrictive/removing changes, retract every
    violating graph edge. Each retraction is an op='R' EdgeFact
    applied to the Chronos graph (deletes the edge from
    `okg.graph_edges`) through the `_reconcile` source so it's
    auditable (source_revision carries the target catalog_version).
    Work is bounded by violator count, not graph size.
  - `abort`: skip this change, report, move on.

Deferred:
  - `rename` (structural): rewrite the graph nodes/edges + an
    alias row. Needs the graph-edit path + alias emission.
  - `keep_with_widened_narrowing`: author opts to permit the
    violators by widening the narrowing. Trivial (re-add the
    row), but paired tests with the restrictive path are not
    in this stride.

Retraction-safety guard (M5.5 correction — docstring used to
  promise per-emission; the code calls once per apply_plan):
  Before emitting any retractions, `apply_plan` calls
  `okg.substrate.policy.retraction.assert_retraction_permitted`
  ONCE with `source_class='catalog_edit'` and
  `run_mode=CATALOG_EDIT`. The per-plan call is sufficient —
  the verdict is a pure function of profile + mode, so a
  per-emission call would be identical no-ops. If the guard
  refuses (e.g. a future hardening flag), apply_plan aborts
  BEFORE any retraction is written.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

import psycopg
import yaml
from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError
from okg.substrate.policy.retraction import (
    RunMode,
    assert_retraction_permitted,
)


__all__ = [
    "CatalogDiff",
    "NarrowingChange",
    "ReconcileApplyResult",
    "apply_plan",
    "classify_narrowings",
    "count_violators",
    "emit_plan_yaml",
    "load_plan",
]


# ---------- diff classifier ----------


NarrowingTriple = tuple[str, str, str]   # (parent, edge, child)


@dataclass(frozen=True)
class NarrowingChange:
    """One proposed narrowing edit.

    `kind` is one of:
      - 'additive'     : triple not in current; add it
      - 'restrictive'  : triple in current but removed; existing
                         rows matching it become violators
      - 'removing'     : same as restrictive but semantically
                         stronger (design distinguishes them
                         for reporting). For narrowings the
                         two collapse; keep 'restrictive'.

    Violator count is filled in by `count_violators()`.
    """

    kind: str
    parent: str
    edge: str
    child: str
    violator_count: Optional[int] = None
    current_attr_rules: dict[str, Any] = field(default_factory=dict)
    proposed_attr_rules: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CatalogDiff:
    """Diff of proposed vs current narrowings."""

    changes: list[NarrowingChange]

    @property
    def additive(self) -> list[NarrowingChange]:
        return [c for c in self.changes if c.kind == "additive"]

    @property
    def restrictive(self) -> list[NarrowingChange]:
        return [
            c for c in self.changes
            if c.kind in {"restrictive", "annotation_restrictive"}
        ]

    @property
    def annotation_restrictive(self) -> list[NarrowingChange]:
        return [c for c in self.changes if c.kind == "annotation_restrictive"]

    @property
    def annotation_widening(self) -> list[NarrowingChange]:
        return [c for c in self.changes if c.kind == "annotation_widening"]

    @property
    def is_additive_only(self) -> bool:
        return all(c.kind == "additive" for c in self.changes)


def classify_narrowings(
    current: Iterable[NarrowingTriple],
    proposed: Iterable[NarrowingTriple],
    *,
    current_attr_rules: Optional[dict[NarrowingTriple, dict[str, Any]]] = None,
    proposed_attr_rules: Optional[dict[NarrowingTriple, dict[str, Any]]] = None,
) -> CatalogDiff:
    """Pure function: diff two narrowing triple sets.

    `current` — the set of triples at the currently-applied cv.
    `proposed` — the author's new intended set.

    Returns a CatalogDiff with:
      - additive changes for triples in proposed but not current
      - restrictive changes for triples in current but not proposed
      - no change reported for triples in both
    """
    current_set = {tuple(t) for t in current}
    proposed_set = {tuple(t) for t in proposed}

    changes: list[NarrowingChange] = []
    for t in sorted(proposed_set - current_set):
        changes.append(NarrowingChange(
            kind="additive",
            parent=t[0], edge=t[1], child=t[2],
        ))
    for t in sorted(current_set - proposed_set):
        changes.append(NarrowingChange(
            kind="restrictive",
            parent=t[0], edge=t[1], child=t[2],
        ))
    current_rules = current_attr_rules or {}
    proposed_rules = proposed_attr_rules or {}
    proposed_pair_cardinality: dict[tuple[str, str], set[Optional[str]]] = {}
    for t in proposed_set:
        cardinality = proposed_rules.get(t, {}).get("cardinality")
        if cardinality not in {None, "at_most_one_target"}:
            raise SubstrateError(
                "unsupported narrowing cardinality annotation",
                data={"triple": list(t), "cardinality": cardinality},
            )
        proposed_pair_cardinality.setdefault((t[0], t[1]), set()).add(cardinality)
    conflicts = {
        pair: values
        for pair, values in proposed_pair_cardinality.items()
        if len(values) > 1
    }
    if conflicts:
        pair, values = sorted(conflicts.items())[0]
        raise SubstrateError(
            "all child narrowings for a parent/edge pair must agree on cardinality",
            data={
                "parent": pair[0],
                "edge": pair[1],
                "cardinalities": sorted("absent" if v is None else v for v in values),
            },
        )
    for t in sorted(current_set & proposed_set):
        before = dict(current_rules.get(t, {}))
        after = dict(proposed_rules.get(t, {}))
        if before == after:
            continue
        before_cardinality = before.get("cardinality")
        after_cardinality = after.get("cardinality")
        if (
            before_cardinality is None
            and after_cardinality == "at_most_one_target"
        ):
            changes.append(NarrowingChange(
                kind="annotation_restrictive",
                parent=t[0], edge=t[1], child=t[2],
                current_attr_rules=before,
                proposed_attr_rules=after,
            ))
        elif (
            before_cardinality == "at_most_one_target"
            and after_cardinality is None
        ):
            changes.append(NarrowingChange(
                kind="annotation_widening",
                parent=t[0], edge=t[1], child=t[2],
                current_attr_rules=before,
                proposed_attr_rules=after,
            ))
    return CatalogDiff(changes=changes)


# ---------- violator counting ----------


def _count_edges_matching_triple(
    conn: psycopg.Connection,
    parent: str, edge: str, child: str,
) -> int:
    """Live graph_edges rows whose (src.subtype, edge_type,
    dst.subtype) == (parent, edge, child)."""
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False
    ).checkout()
    rows = graph.query(
        """
        SELECT count(*) AS n
          FROM okg.graph_edges el
          JOIN okg.graph_nodes s
            ON s.node_id = el.src
          JOIN okg.graph_nodes d
            ON d.node_id = el.dst
         WHERE s.subtype = :parent
           AND el.edge_type = :edge
           AND d.subtype = :child
        """,
        {"parent": parent, "edge": edge, "child": child},
    )
    return int(rows[0]["n"])


def _count_cardinality_violator_groups(
    conn: psycopg.Connection,
    parent: str,
    edge: str,
    catalog_version_id: Optional[int] = None,
) -> int:
    """Count violating groups for the declared parent and all descendants."""
    from okg.substrate.graph_store import ChronosGraphStore

    if catalog_version_id is None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT catalog_version_id FROM okg.catalog_versions "
                "WHERE status = 'applied' ORDER BY catalog_version_id DESC LIMIT 1"
            )
            row = cur.fetchone()
        if row is None:
            raise SubstrateError("no active catalog version")
        catalog_version_id = int(row[0])
    graph = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout()
    rows = graph.query(
        """
        SELECT count(*) AS n
          FROM (
            SELECT e.src
              FROM okg.graph_edges e
              JOIN okg.graph_nodes s ON s.node_id = e.src
             WHERE e.edge_type = :edge
               AND (
                 s.subtype = :parent
                 OR EXISTS (
                   SELECT 1
                     FROM okg.subtype_ancestors sa
                    WHERE sa.catalog_version_id = :catalog_version_id
                      AND sa.ancestor_name = :parent
                      AND sa.subtype_name = s.subtype
                 )
               )
             GROUP BY e.src, e.edge_type
            HAVING count(DISTINCT e.dst) > 1
          ) groups
        """,
        {
            "parent": parent,
            "edge": edge,
            "catalog_version_id": int(catalog_version_id),
        },
    )
    return int(rows[0]["n"])


def count_violators(
    conn: psycopg.Connection, diff: CatalogDiff,
) -> CatalogDiff:
    """Fill in `violator_count` on each restrictive/removing
    change. Returns a new CatalogDiff."""
    resolved: list[NarrowingChange] = []
    for c in diff.changes:
        if c.kind in {"additive", "annotation_widening"}:
            resolved.append(c)
            continue
        n = (
            _count_cardinality_violator_groups(conn, c.parent, c.edge)
            if c.kind == "annotation_restrictive"
            else _count_edges_matching_triple(
                conn, c.parent, c.edge, c.child,
            )
        )
        resolved.append(NarrowingChange(
            kind=c.kind,
            parent=c.parent, edge=c.edge, child=c.child,
            violator_count=n,
            current_attr_rules=c.current_attr_rules,
            proposed_attr_rules=c.proposed_attr_rules,
        ))
    return CatalogDiff(changes=resolved)


# ---------- plan emit + load ----------


_VALID_ACTIONS = frozenset({
    "apply", "retract", "keep_with_widened_narrowing",
    "abort",
})


def emit_plan_yaml(diff: CatalogDiff, path: str | Path) -> None:
    """Write `plan.yaml` with one entry per change.

    Additive changes get `action: apply` (auto).
    Non-additive changes get `action: abort` as a SAFE DEFAULT —
    the author must edit to `retract` /
    `keep_with_widened_narrowing` before `--reconcile` will
    touch data.
    """
    entries: list[dict[str, Any]] = []
    for c in diff.changes:
        entry: dict[str, Any] = {
            "change": {
                "additive": "add_narrowing",
                "restrictive": "remove_narrowing",
                "annotation_restrictive": "annotate_narrowing",
                "annotation_widening": "widen_narrowing_annotation",
            }[c.kind],
            "parent": c.parent,
            "edge": c.edge,
            "child": c.child,
        }
        if c.violator_count is not None:
            entry["violators"] = c.violator_count
        if c.kind in {"annotation_restrictive", "annotation_widening"}:
            entry["attr_rules"] = c.proposed_attr_rules
        entry["action"] = (
            "apply" if c.kind == "additive" else "abort"
        )
        entries.append(entry)
    doc = {"reconcile": entries}
    Path(path).write_text(yaml.safe_dump(doc, sort_keys=False))


def load_plan(path: str | Path) -> list[dict[str, Any]]:
    """Read plan.yaml, validate action values, return entries."""
    p = Path(path)
    with p.open("r") as f:
        doc = fastyaml.safe_load(f) or {}
    if not isinstance(doc, dict) or "reconcile" not in doc:
        raise SubstrateError(
            f"plan {p}: missing top-level 'reconcile' key",
            data={"path": str(p)},
        )
    entries = doc["reconcile"]
    if not isinstance(entries, list):
        raise SubstrateError(
            f"plan {p}: 'reconcile' must be a list",
            data={"path": str(p)},
        )
    for i, e in enumerate(entries):
        if not isinstance(e, dict):
            raise SubstrateError(
                f"plan {p}: entry {i} must be a mapping",
                data={"path": str(p), "index": i},
            )
        action = e.get("action")
        if action not in _VALID_ACTIONS:
            raise SubstrateError(
                f"plan {p}: entry {i} has invalid action "
                f"{action!r}; allowed: {sorted(_VALID_ACTIONS)}",
                data={"path": str(p), "index": i, "action": action},
            )
    return entries


# ---------- apply ----------


@dataclass
class ReconcileApplyResult:
    """Outcome of apply_plan(). Captures per-step disposition +
    fact counts so the operator can audit the migration."""

    new_catalog_version_id: int
    applied: list[dict[str, Any]] = field(default_factory=list)
    aborted: list[dict[str, Any]] = field(default_factory=list)
    retractions_emitted: int = 0


def _bump_catalog_version(
    conn: psycopg.Connection, diff_summary: dict, plan: list[dict],
) -> int:
    """Insert a new catalog_versions row in 'applied' state.

    Caller wraps this in the outer reconcile transaction so the
    bump + the data changes commit atomically (or not at all).
    """
    import json
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.catalog_versions
              (status, applied_at, diff_summary, reconcile_plan)
            VALUES ('applied', now(), %s::jsonb, %s::jsonb)
            RETURNING catalog_version_id
            """,
            (json.dumps(diff_summary), json.dumps(plan)),
        )
        return cur.fetchone()[0]


def _copy_current_narrowings_to(
    conn: psycopg.Connection,
    old_cv: int, new_cv: int,
    triples_to_drop: set[NarrowingTriple],
    triples_to_add: set[NarrowingTriple],
    attr_rules_to_add: Optional[dict[NarrowingTriple, dict[str, Any]]] = None,
    attr_rules_to_update: Optional[dict[NarrowingTriple, dict[str, Any]]] = None,
) -> None:
    """Stamp the new cv's narrowings table: copy unchanged from
    old, add new additive rows, skip restrictive ones.

    Precondition (M5.5 per Dijkstra review §6):
    `triples_to_drop` and `triples_to_add` must be DISJOINT.
    classify_narrowings produces them disjoint by construction;
    a hand-crafted plan.yaml could violate this. Asserted.
    """
    overlap = triples_to_drop & triples_to_add
    if overlap:
        raise ValueError(
            f"triples_to_drop and triples_to_add overlap: {overlap}"
        )
    with conn.cursor() as cur:
        # Copy unchanged.
        cur.execute(
            """
            INSERT INTO okg.narrowings
              (parent_subtype, edge_type, child_subtype,
               attr_rules, catalog_version_id)
            SELECT parent_subtype, edge_type, child_subtype,
                   attr_rules, %s
              FROM okg.narrowings
             WHERE catalog_version_id = %s
            """,
            (new_cv, old_cv),
        )
        # Drop the restrictive triples (delete from the copy
        # just inserted).
        for t in triples_to_drop:
            cur.execute(
                """
                DELETE FROM okg.narrowings
                 WHERE catalog_version_id = %s
                   AND parent_subtype = %s
                   AND edge_type      = %s
                   AND child_subtype  = %s
                """,
                (new_cv, t[0], t[1], t[2]),
            )
        # Add the additive triples.
        #
        # fix-projection-alias-identity #710 (ruling 3): carry attr_rules (the
        # cardinality annotation) onto the additive row. Previously this arm
        # inserted the triple only, silently dropping the annotation on
        # reconcile -- a fail-open (a functional edge added via reconcile would
        # never be cardinality-enforced). `attr_rules_to_add` comes from the
        # proposed catalog (ComposedCatalog.narrowing_attr_rules()); an absent
        # entry defaults to '{}' (today's semantics), never omission.
        import json as _json
        rules_by_triple = attr_rules_to_add or {}
        for t in triples_to_add:
            cur.execute(
                """
                INSERT INTO okg.narrowings
                  (parent_subtype, edge_type, child_subtype,
                   attr_rules, catalog_version_id)
                VALUES (%s, %s, %s, %s::jsonb, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    t[0], t[1], t[2],
                    _json.dumps(rules_by_triple.get(t, {})),
                    new_cv,
                ),
            )
        update_rules = attr_rules_to_update or {}
        for t, rules in update_rules.items():
            cur.execute(
                """
                UPDATE okg.narrowings
                   SET attr_rules = %s::jsonb
                 WHERE catalog_version_id = %s
                   AND parent_subtype = %s
                   AND edge_type = %s
                   AND child_subtype = %s
                """,
                (_json.dumps(rules), new_cv, t[0], t[1], t[2]),
            )
            if cur.rowcount != 1:
                raise SubstrateError(
                    "reconcile annotation target is absent from copied catalog",
                    data={"triple": list(t), "catalog_version_id": new_cv},
                )


def _emit_retractions_for_narrowing(
    conn: psycopg.Connection,
    parent: str, edge: str, child: str,
    new_cv: int, run_id: str, deployment_name: str,
) -> int:
    """Retract every graph edge matching (parent, edge, child).

    Chronos cutover: a retraction is an `op='R'` EdgeFact applied to
    the Chronos graph, which deletes the edge from `okg.graph_edges`.
    Returns the count of edges retracted.

    The retraction's audit trail carries source_record_id /
    source_revision referencing the NEW catalog_version so the
    bookkeeping ties retractions to their triggering catalog bump.

    Cross-source deletion: the victim edges are selected purely by
    (parent_subtype, edge_type, child_subtype) — the violating edges
    were written by arbitrary upstream sources (e.g. 'default'), NOT
    by '_reconcile'. So `apply_facts` is called with `source=None`,
    which makes the underlying DELETE match by edge identity
    (src, edge_type, dst) regardless of which source wrote the edge.
    A source-scoped delete (source='_reconcile') would match nothing
    and silently retract zero edges — the catalog narrowing-removal
    would claim success while the violating edges survived. The
    delete stays scoped to exactly the queried victims, so unrelated
    edge types / sources are never touched.

    Policy note: the `catalog_edit` profile + CATALOG_EDIT run mode
    always permits reconcile-emitted retractions. The guard is
    consulted once per apply_plan (not per emission) — see apply_plan's
    call site for rationale.
    """
    from okg.substrate.fact_application import apply_bounded_fact_batch
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    victims = store.checkout().query(
        """
        SELECT el.src, el.dst, el.edge_type
          FROM okg.graph_edges el
          JOIN okg.graph_nodes s
            ON s.node_id = el.src
          JOIN okg.graph_nodes d
            ON d.node_id = el.dst
         WHERE s.subtype = :parent
           AND el.edge_type = :edge
           AND d.subtype = :child
        """,
        {"parent": parent, "edge": edge, "child": child},
    )
    if not victims:
        return 0

    facts = [
        EdgeFact(
            row["src"],
            row["dst"],
            row["edge_type"],
            source_record_id={"catalog_version": new_cv},
            source_revision={"catalog_version": new_cv, "run_id": run_id},
            op="R",
        )
        for row in victims
    ]
    result = apply_bounded_fact_batch(
        conn,
        store,
        facts,
        deployment_name=deployment_name,
        producer_id="system:catalog_reconcile",
        producer_kind="system",
        intent="catalog_reconcile_retraction",
        catalog_version_id=new_cv,
        authority_scope={
            "narrowing": [parent, edge, child],
            "catalog_version_id": new_cv,
        },
        source=None,
        run_id=run_id,
        retractions_allowed=True,
        cross_source_retractions=True,
        strict_endpoints=False,
    )
    return result.primitive_total("edges_deleted")


def apply_plan(
    conn: psycopg.Connection,
    *,
    diff: CatalogDiff,
    plan: list[dict[str, Any]],
    current_catalog_version_id: int,
    deployment_name: str = "default",
    require_retraction_policy: bool = True,
    attr_rules_to_add: Optional[dict[NarrowingTriple, dict[str, Any]]] = None,
) -> ReconcileApplyResult:
    """Apply the reconcile plan in ONE transaction.

    Each entry's action determines what happens:
      - 'apply'  → the additive narrowing is added at the new cv
      - 'retract' → retraction facts are emitted for the
                    matching live edges; the narrowing is
                    removed at the new cv
      - 'keep_with_widened_narrowing' → the narrowing stays
                    (copied to the new cv; equivalent to
                    dropping the restrictive change); no data
                    touch. [M5.4: basic implementation]
      - 'abort' → the change is NOT applied to the new cv
                  (restrictive narrowings stay; additive
                  narrowings are dropped); reported in result.

    Returns a ReconcileApplyResult carrying the new cv and
    per-step disposition.
    """
    # Build the final (triples_to_drop, triples_to_add) sets
    # based on per-entry actions.
    triples_to_drop: set[NarrowingTriple] = set()
    triples_to_add: set[NarrowingTriple] = set()
    attr_rules_to_update: dict[NarrowingTriple, dict[str, Any]] = {}
    applied: list[dict[str, Any]] = []
    aborted: list[dict[str, Any]] = []
    retract_changes: list[NarrowingChange] = []

    # Index diff.changes by (parent, edge, child, kind) so we
    # can cross-reference plan entries.
    change_index: dict[
        tuple[str, str, str, str], NarrowingChange
    ] = {
        (c.parent, c.edge, c.child, c.kind): c
        for c in diff.changes
    }

    # Cardinality is a (parent_subtype, edge_type) contract even though it is
    # stored on each child triple. Require complete, pair-wide disposition so
    # an operator cannot stamp one child while leaving a sibling widened.
    annotation_change_names = {
        "annotation_restrictive": "annotate_narrowing",
        "annotation_widening": "widen_narrowing_annotation",
    }
    for pair in sorted({
        (c.parent, c.edge)
        for c in diff.changes
        if c.kind in annotation_change_names
    }):
        expected = {
            (c.child, annotation_change_names[c.kind])
            for c in diff.changes
            if (c.parent, c.edge) == pair and c.kind in annotation_change_names
        }
        supplied_entries = [
            entry for entry in plan
            if (entry.get("parent"), entry.get("edge")) == pair
            and entry.get("change") in set(annotation_change_names.values())
        ]
        supplied = {
            (entry.get("child"), entry.get("change"))
            for entry in supplied_entries
        }
        actions = {entry.get("action") for entry in supplied_entries}
        if supplied != expected or len(actions) != 1:
            raise SubstrateError(
                "cardinality annotation disposition must cover the whole parent/edge pair",
                data={
                    "parent": pair[0],
                    "edge": pair[1],
                    "expected": sorted([list(item) for item in expected]),
                    "supplied": sorted([list(item) for item in supplied]),
                    "actions": sorted(str(action) for action in actions),
                },
            )

    for entry in plan:
        parent = entry.get("parent")
        edge = entry.get("edge")
        child = entry.get("child")
        action = entry["action"]
        change_kind = {
            "add_narrowing": "additive",
            "remove_narrowing": "restrictive",
            "annotate_narrowing": "annotation_restrictive",
            "widen_narrowing_annotation": "annotation_widening",
        }.get(entry["change"])
        if change_kind is None:
            aborted.append({**entry, "reason": "unknown_change_kind"})
            continue
        key = (parent, edge, child, change_kind)
        c = change_index.get(key)
        if c is None:
            aborted.append({
                **entry,
                "reason": "plan_entry_not_in_diff",
            })
            continue

        if action == "apply":
            if change_kind == "annotation_restrictive":
                live_violators = _count_cardinality_violator_groups(
                    conn, parent, edge, current_catalog_version_id,
                )
                if live_violators:
                    raise SubstrateError(
                        "reconcile refuses at-most-one annotation while "
                        "multi-target groups remain",
                        data={
                            "triple": [parent, edge, child],
                            "multi_target_group_count": live_violators,
                            "required_action": (
                                "record an explicit disposition and remove or "
                                "retype every extra target before retrying"
                            ),
                        },
                    )
                triple = (parent, edge, child)
                rules = dict(c.proposed_attr_rules)
                if not rules:
                    rules = dict((attr_rules_to_add or {}).get(triple, {}))
                attr_rules_to_update[triple] = rules
                applied.append({**entry, "note": "annotation_stamped"})
            elif change_kind != "additive":
                raise SubstrateError(
                    f"reconcile: action 'apply' only valid for "
                    f"additive changes; got {change_kind} for "
                    f"({parent}, {edge}, {child})",
                    data={"entry": entry},
                )
            else:
                triples_to_add.add((parent, edge, child))
                applied.append(entry)

        elif action == "retract":
            if change_kind != "restrictive":
                raise SubstrateError(
                    f"reconcile: action 'retract' only valid for "
                    f"restrictive changes; got {change_kind} for "
                    f"({parent}, {edge}, {child})",
                    data={"entry": entry},
                )
            triples_to_drop.add((parent, edge, child))
            retract_changes.append(c)
            applied.append(entry)

        elif action == "keep_with_widened_narrowing":
            if change_kind == "annotation_widening":
                triple = (parent, edge, child)
                attr_rules_to_update[triple] = dict(c.proposed_attr_rules)
                applied.append({**entry, "note": "annotation_widened"})
            else:
                # The narrowing stays in the new cv. Equivalent to
                # "don't drop it."  No data touch; no additive row
                # either (it's already in the current cv and will
                # copy forward).
                applied.append({**entry, "note": "narrowing_preserved"})

        elif action == "abort":
            if change_kind == "additive":
                # The additive is NOT added to the new cv.
                pass
            else:
                # The restrictive is NOT dropped — narrowing
                # stays (equivalent to keep_with_widened).
                pass
            aborted.append(entry)
        else:
            raise SubstrateError(
                f"reconcile: unreachable action {action!r}",
                data={"entry": entry},
            )

    # One transaction: bump catalog version, copy narrowings to
    # new cv (with drops/adds), emit retraction facts.
    run_id = str(uuid.uuid4())
    diff_summary = {
        "additive": len(diff.additive),
        "restrictive": len([
            change for change in diff.changes
            if change.kind == "restrictive"
        ]),
        "annotation_restrictive": len(diff.annotation_restrictive),
        "annotation_widening": len(diff.annotation_widening),
        "retract_applied": len(retract_changes),
        "aborted": len(aborted),
    }

    # Policy guard: consulted only if we actually emit
    # retractions. Uses the synthetic `catalog_edit` profile
    # (M5.4) — catalog-driven retractions are authored by the
    # operator and always permitted; the call goes through the
    # guard so the policy lives in one place.
    if require_retraction_policy and retract_changes:
        assert_retraction_permitted(
            source_class="catalog_edit",
            run_mode=RunMode.CATALOG_EDIT,
            source_name="_reconcile",
        )

    # M5.5 (Dijkstra review §7): use `with conn.transaction():`
    # for the outer reconcile txn (M2.5 convention). Works in
    # both autocommit=True and autocommit=False modes:
    #   - autocommit=True: opens a new explicit txn
    #   - autocommit=False: nests as a savepoint on the caller's
    #     existing txn, so callers that want a larger unit of
    #     atomicity aren't broken by an inner commit().
    #
    # The relational catalog bump + narrowing copy commit here. The
    # graph-edge retractions run AFTER this transaction (Chronos cutover):
    # Chronos graph edits go through `apply_facts`, which manages its
    # own commit and cannot nest inside this psycopg transaction.
    with conn.transaction():
        new_cv = _bump_catalog_version(
            conn, diff_summary=diff_summary, plan=plan,
        )
        _copy_current_narrowings_to(
            conn,
            old_cv=current_catalog_version_id,
            new_cv=new_cv,
            triples_to_drop=triples_to_drop,
            triples_to_add=triples_to_add,
            attr_rules_to_add=attr_rules_to_add,
            attr_rules_to_update=attr_rules_to_update,
        )

    n_retractions = 0
    for c in retract_changes:
        # Each retract deletes the graph edges matching its change
        # predicate. Bounded by violator count, not graph size
        # (per design §1755).
        n_retractions += _emit_retractions_for_narrowing(
            conn,
            parent=c.parent, edge=c.edge, child=c.child,
            new_cv=new_cv, run_id=run_id,
            deployment_name=deployment_name,
        )

    return ReconcileApplyResult(
        new_catalog_version_id=new_cv,
        applied=applied,
        aborted=aborted,
        retractions_emitted=n_retractions,
    )
