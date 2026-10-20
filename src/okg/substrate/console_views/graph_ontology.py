"""The console Graph view's ontology read: what is declared beside what is populated.

PACT console-graph-views. ``compose_graph_ontology`` answers, for the pinned
version:

* every declared record type at the version's catalog (``okg.subtypes``), its
  archetype and parent, its record count from the maintained count rollup, and
  its declared attributes;
* every declared relationship rule (``okg.narrowings``) with the number of
  relationships the version realises for it, from the maintained ontology
  rollup;
* a field-fill SAMPLE per populated type: the first ``FILL_SAMPLE`` records in
  index order (not random), labelled with its size and method.

Declared attributes are not in the database: the catalog stores type names,
metadata and narrowings, not LinkML attributes. No product function returns
them either -- ``compose_catalog`` reads each class's attributes for their
annotations and keeps none. So ``declared_attributes`` reads the SAME files
``compose_catalog`` composes, found the same way (the deployment's ``modules``
resolved through its module providers, then its own schema directory), and
nothing else. They are the files on disk when the read runs, which can differ
from the catalog version the database holds; the response says so in
``attributes_basis``.

The fill sample reads record attributes before any access rule. It reports
fill rates only, never a value, and only for attributes the schema declares for
the type (names the ontology already publishes), less the attributes the read
policy decides on (``excluded_fill_attributes``).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okg.substrate.console_views._chronos import checkout
from okg.substrate.console_views.graph_views import (
    GraphReadRefused,
    read_counts,
    read_rollup,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    COUNT_KIND_SUBTYPE,
)
from okg.substrate.mcp.session import Session

logger = logging.getLogger(__name__)

#: Records sampled per populated type for the field-fill figures.
FILL_SAMPLE = 1000
FILL_METHOD = "first_1000_by_index"
ATTRIBUTES_BASIS = "deployment_schema_files_at_read_time"

_NARROWING_CARRIER = "EdgeNarrowings"

# The first FILL_SAMPLE records of one type, in whatever order the index hands
# them back, and how many of them carry a non-empty value per attribute key.
# derivation-read-lint: allow PACT console-graph-views bounded 1,000-record sample.
_FILL_SQL = f"""
WITH s AS (SELECT attrs FROM okg.graph_nodes WHERE subtype = :subtype LIMIT {FILL_SAMPLE})
SELECT kv.key AS key,
       count(*) FILTER (
         WHERE kv.value IS NOT NULL
           AND kv.value <> 'null'::jsonb AND kv.value <> '""'::jsonb
           AND kv.value <> '[]'::jsonb AND kv.value <> '{{}}'::jsonb
       ) AS filled,
       (SELECT count(*) FROM s) AS sampled
  FROM s, LATERAL jsonb_each(s.attrs) AS kv
 GROUP BY kv.key
 ORDER BY kv.key
"""

# derivation-read-lint: allow PACT console-graph-views bounded 1,000-record sample.
_SAMPLED_SQL = """
SELECT count(*) AS sampled
  FROM (SELECT 1 FROM okg.graph_nodes WHERE subtype = :subtype LIMIT {sample}) s
""".format(sample=FILL_SAMPLE)


def _snake(name: str) -> str:
    """LinkML class name to subtype name, by ``compose_catalog``'s own conversion."""
    from okg.substrate.catalog.modules_compose import _pascal_to_snake

    return _pascal_to_snake(name)


@dataclass
class _DeclaredClass:
    module: str
    attributes: dict[str, dict[str, Any]] = field(default_factory=dict)
    slots: list[str] = field(default_factory=list)
    slot_usage: dict[str, dict[str, Any]] = field(default_factory=dict)
    parents: list[str] = field(default_factory=list)
    default_range: str = "string"


class DeclaredSchemaUnavailable(Exception):
    """The deployment's schema files could not be found or read."""


def declared_schema_files(deployment_dir: Path | str) -> list[tuple[str, Path]]:
    """``(origin, path)`` for every schema file ``compose_catalog`` composes, in its order."""
    from okg.substrate import fastyaml
    from okg.substrate.catalog.module_providers import resolve_modules
    from okg.substrate.catalog.modules_compose import (
        providers_for_declared_deployment,
    )

    root = Path(deployment_dir)
    manifest_path = root / "deployment.yaml"
    if not manifest_path.is_file():
        raise DeclaredSchemaUnavailable(f"no deployment.yaml in {root.name}")
    with manifest_path.open() as handle:
        manifest = fastyaml.safe_load(handle) or {}
    modules = [str(m) for m in (manifest.get("modules") or [])]
    resolved = resolve_modules(modules, providers_for_declared_deployment(root))
    files = [(module, resolved[module].module_dir / "schema.yaml") for module in modules]
    schema_dir = root / str(manifest.get("schema_dir") or "schemas")
    name = str(manifest.get("name") or root.name)
    if schema_dir.is_dir():
        files.extend(
            (f"deployment:{name}", path) for path in sorted(schema_dir.glob("*.yaml"))
        )
    return files


def _attribute(definition: Any, default_range: str) -> dict[str, Any]:
    definition = definition if isinstance(definition, dict) else {}
    return {
        "required": bool(definition.get("required")),
        "range": str(definition.get("range") or default_range),
    }


def declared_attributes(deployment_dir: Path | str) -> dict[str, tuple[str, list[dict[str, Any]]]]:
    """``subtype -> (module, [{name, required, range}])`` from the deployment's schema files.

    A class declared by more than one module keeps its first module, as
    ``compose_catalog`` does; a deployment schema file adds attributes to a
    class a module declared. Inherited attributes (``is_a`` and ``mixins``) and
    slots come before a class's own, and a class's own declaration wins.
    """
    from okg.substrate import fastyaml

    classes: dict[str, _DeclaredClass] = {}
    slot_defs: dict[str, dict[str, Any]] = {}
    for origin, path in declared_schema_files(deployment_dir):
        try:
            with path.open() as handle:
                schema = fastyaml.safe_load(handle) or {}
        except OSError as exc:
            raise DeclaredSchemaUnavailable(f"cannot read {path.name} of {origin}") from exc
        if not isinstance(schema, dict):
            continue
        default_range = str(schema.get("default_range") or "string")
        for slot_name, slot in (schema.get("slots") or {}).items():
            slot_defs.setdefault(str(slot_name), _attribute(slot, default_range))
        for class_name, definition in (schema.get("classes") or {}).items():
            if class_name == _NARROWING_CARRIER or not isinstance(definition, dict):
                continue
            name = _snake(str(class_name))
            entry = classes.get(name)
            if entry is None:
                entry = classes[name] = _DeclaredClass(module=origin, default_range=default_range)
            elif entry.module != origin and not origin.startswith("deployment:"):
                # A second module declaring the class: the first keeps it.
                continue
            for attr_name, attr in (definition.get("attributes") or {}).items():
                entry.attributes.setdefault(str(attr_name), _attribute(attr, default_range))
            entry.slots.extend(str(s) for s in definition.get("slots") or [])
            for slot_name, usage in (definition.get("slot_usage") or {}).items():
                if isinstance(usage, dict):
                    entry.slot_usage.setdefault(str(slot_name), usage)
            parent = definition.get("is_a")
            for name in ([parent] if parent else []) + list(definition.get("mixins") or []):
                if _snake(str(name)) not in entry.parents:
                    entry.parents.append(_snake(str(name)))

    def resolve(name: str, seen: tuple[str, ...]) -> dict[str, dict[str, Any]]:
        entry = classes.get(name)
        if entry is None or name in seen:
            return {}
        out: dict[str, dict[str, Any]] = {}
        for parent in entry.parents:
            out.update(resolve(parent, seen + (name,)))
        for slot in entry.slots:
            base = dict(slot_defs.get(slot) or {"required": False, "range": entry.default_range})
            usage = entry.slot_usage.get(slot) or {}
            if "required" in usage:
                base["required"] = bool(usage["required"])
            if usage.get("range"):
                base["range"] = str(usage["range"])
            out[slot] = base
        out.update(entry.attributes)
        return out

    return {
        name: (
            entry.module,
            [{"name": key, **value} for key, value in sorted(resolve(name, ()).items())],
        )
        for name, entry in classes.items()
    }


def _catalog_rows(conn: Any, catalog_version_id: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, metadata, parent_subtype FROM okg.subtypes"
            " WHERE catalog_version_id = %s ORDER BY name",
            (catalog_version_id,),
        )
        names = [d.name for d in cur.description]
        subtypes = [dict(zip(names, row)) if not isinstance(row, dict) else row for row in cur.fetchall()]
        cur.execute(
            "SELECT parent_subtype, edge_type, child_subtype FROM okg.narrowings"
            " WHERE catalog_version_id = %s ORDER BY 1, 2, 3",
            (catalog_version_id,),
        )
        names = [d.name for d in cur.description]
        narrowings = [dict(zip(names, row)) if not isinstance(row, dict) else row for row in cur.fetchall()]
    return subtypes, narrowings


def excluded_fill_attributes() -> frozenset[str]:
    """Attributes the fill sample never reports, even when a schema declares them.

    The Nomos read policy decides on ``classification`` and on the
    ``nomos_source_lineage`` envelope; the platform writes the rest of its
    reserved audit keys (``ENTITY_MERGE_RESERVED_AUDIT_ATTRS``). The sample is
    taken before access rules, so a fill rate for a key the policy reads would
    say how many records carry it. Only these and undeclared keys are left out.
    """
    from okg.substrate.catalog.modules_compose import ENTITY_MERGE_RESERVED_AUDIT_ATTRS
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

    return frozenset({"classification", LINEAGE_ATTR, *ENTITY_MERGE_RESERVED_AUDIT_ATTRS})


def _fill(graph: Any, subtype: str, population: int, declared: set[str]) -> dict[str, Any]:
    """Fill rates for the DECLARED attributes of one type, over the sample.

    Undeclared keys are not reported: their names are not public in the
    ontology, and the sample is read before access rules.
    """
    rows = graph.query(_FILL_SQL, {"subtype": subtype})
    if rows:
        sampled = int(rows[0]["sampled"])
    else:
        # Records with no attributes at all yield no key rows; still say how
        # many were sampled rather than implying none were.
        sampled = int(graph.query(_SAMPLED_SQL, {"subtype": subtype})[0]["sampled"])
    rows = [row for row in rows if str(row["key"]) in declared]
    seen = {str(row["key"]) for row in rows}
    return {
        "population": population,
        "sampled": sampled,
        "method": FILL_METHOD,
        "attributes": [
            {
                "name": str(row["key"]),
                "fill_rate": round(int(row["filled"]) / sampled, 3) if sampled else 0.0,
                "declared": str(row["key"]) in declared,
            }
            for row in rows
        ],
        "declared_never_seen": sorted(declared - seen),
    }


def compose_graph_ontology(
    conn: Any,
    *,
    session: Session,
    deployment_name: str,
    deployment_dir: Path | str,
) -> dict[str, Any]:
    """Declared types, attributes and rules beside what the version populates."""
    catalog_version_id = getattr(session, "catalog_version_id", None)
    if catalog_version_id is None:
        raise GraphReadRefused(
            503,
            "catalog_version_unavailable",
            "This version names no catalog version, so its declared types cannot be read.",
            generation_id=session.generation_id,
        )
    counts = read_counts(conn, session, {COUNT_KIND_SUBTYPE})
    population = {
        value: int(n) for (kind, value), n in counts.items() if kind == COUNT_KIND_SUBTYPE
    }
    realised = read_rollup(conn, session).pair_counts()
    try:
        declared = declared_attributes(deployment_dir)
    except Exception as exc:  # noqa: BLE001 -- refused by name, cause logged here
        logger.exception("graph ontology: reading %s's schema files failed", deployment_name)
        raise GraphReadRefused(
            503,
            "declared_schema_unavailable",
            "The deployment's schema files could not be read, so declared attributes are unknown.",
            error_type=type(exc).__name__,
            generation_id=session.generation_id,
        ) from exc
    subtypes, narrowings = _catalog_rows(conn, int(catalog_version_id))
    types = []
    for row in subtypes:
        name = str(row["name"])
        module, attributes = declared.get(name, (None, []))
        types.append({
            "type": name,
            "module": module,
            "archetype": (row.get("metadata") or {}).get("archetype"),
            "parent": row.get("parent_subtype"),
            "count": population.get(name, 0),
            "declared_in_schema": name in declared,
            "attributes": attributes,
        })
    rules = [
        {
            "from": row["parent_subtype"],
            "relationship": row["edge_type"],
            "to": row["child_subtype"],
            "count": realised.get(
                (row["parent_subtype"], row["edge_type"], row["child_subtype"]), 0,
            ),
        }
        for row in narrowings
    ]
    graph = checkout(conn, session)
    excluded = excluded_fill_attributes()
    fill = {}
    for name, n in sorted(population.items(), key=lambda kv: (-kv[1], kv[0])):
        if n <= 0:
            continue
        declared_names = {a["name"] for a in declared.get(name, (None, []))[1]} - excluded
        fill[name] = _fill(graph, name, n, declared_names)
    return {
        "deployment": deployment_name,
        "generation_id": session.generation_id,
        "catalog_version_id": int(catalog_version_id),
        "count_basis": "published_total_before_access_rules",
        "attributes_basis": ATTRIBUTES_BASIS,
        "types": types,
        "rules": rules,
        # Sampled before access rules, over declared attributes only, less
        # the attributes the read policy decides on.
        "fill_basis": "declared_attributes_before_access_rules",
        "fill_excluded_attributes": sorted(excluded),
        "fill": fill,
    }


__all__ = [
    "ATTRIBUTES_BASIS",
    "DeclaredSchemaUnavailable",
    "FILL_METHOD",
    "FILL_SAMPLE",
    "compose_graph_ontology",
    "declared_attributes",
    "declared_schema_files",
]
