"""The Nomos read class stored with each graph row version (PACT
lane-h-nomos-read-class-stored; design
``pact/changes/lane-h-nomos-sql-read-filter/design.md`` sections 3.3 and 4).

A row version's *read class* is a 16-byte digest of every fact of the row that
a read decision can depend on when the active rules name only class-safe keys
(``READ_CLASS_SAFE_RULE_KEYS``) and the session carries no identity claim and
no access context. Two rows with the same class and the same effective
classification get the same decisions, so ``read_class_filter`` decides one
representative per class.

The class is a pure function of the row version's own stored content: its
lineage attribute, its three source columns and its id. It is computed with
the read filter's own resource builders (``_row_resource``,
``_fine_grained_targets``), so the stored class and the read-time projection
read the same facts. Classification is NOT in the class: an accepted attribute
overlay may change it, so the read keys on (class, effective classification).

Storage: ``okg.graph_nodes`` / ``okg.graph_edges`` carry
``nomos_read_class UUID NULL`` (the first 16 bytes of the sha256 of the
canonical class facts, as a UUID) and ``nomos_read_class_version SMALLINT
NULL`` (``CLASS_VOCABULARY_VERSION``). UUID, not BYTEA as design 4.1 first
said: the vendored Chronos interval backend writes rows through
``json.dumps(..., default=str)`` and ``jsonb_populate_record`` in its splice
load paths (``_interval_backend.py`` single-row tail splice, batch load and
mixed batch), where Python ``bytes`` do not survive and a UUID does.

Every writer of a graph row version stamps the two columns with
``stamp_row`` / ``stamp_rows``. A row that is not stamped holds NULL, which the
read treats as "no class": the exact read-time projection decides it.
"""

from __future__ import annotations

import hashlib
import json
import random
import uuid
from collections.abc import Iterable, Mapping, MutableMapping
from typing import Any

import psycopg

from okg.substrate.nomos.core import _PROTECTED_IDENTITY_FACTS
from okg.substrate.nomos.runtime_enforcement import _is_json_compatible
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    lineage_for_row,
    source_ids_from_lineage,
)

#: Bump on ANY change to what ``class_facts`` holds or to the keys the read
#: filter's resource builders put in a resource. A stored class whose version
#: is not this one is ignored by the read (treated like NULL).
CLASS_VOCABULARY_VERSION = 1

#: sha256 of the vocabulary this version was computed under: the resource keys
#: the read filter builds (design 3.2), the class-fact keys and the rule-key
#: allow-list. ``vocabulary_digest()`` recomputes it; a test pins the two
#: equal, so changing a resource builder without bumping
#: ``CLASS_VOCABULARY_VERSION`` (and this digest) fails.
CLASS_VOCABULARY_DIGEST = (
    "8867cf273b32841d622d3706488acf73a7b6215a4fa5f3a2db1d72ae29f6898d"
)

READ_CLASS_COLUMN = "nomos_read_class"
READ_CLASS_VERSION_COLUMN = "nomos_read_class_version"
READ_CLASS_COLUMNS = (READ_CLASS_COLUMN, READ_CLASS_VERSION_COLUMN)

#: Resource keys whose value is the same for every row of one read call: the
#: read's deployment, scope, row kind, audit flag, subject, action and the
#: boundary's additions. They are part of the per-call decision, not the class.
_CALL_CONSTANT_KEYS = frozenset({
    "deployment",
    "scope",
    "kind",
    "audit",
    "subject",
    "action",
    "principal_kind",
    "nomos_surface",
    "nomos_boundary",
    "nomos_read_purpose",
})

# The stub call the class facts are computed under. Every value it sets is a
# call constant (``_CALL_CONSTANT_KEYS``), dropped from the facts below.
_STUB_SCOPE = "deployment:read-class"


class _StubSession:
    deployment_name = "read-class"
    nomos_access_context = None


_STUB_SESSION = _StubSession()


def _class_fact_keys() -> frozenset[str]:
    """Resource keys a class-path decision can read that vary per row.

    The allow-listed rule keys plus the protected-identity facts, minus the
    per-call constants: ``source_ids``, ``source_id``, ``resource_kind``,
    ``classification``, ``source``, ``graph_branch``. (No read resource
    carries ``classification``, ``source`` or ``graph_branch`` today; they are
    kept so a builder that starts carrying one changes the class.)
    """
    from okg.substrate.nomos.read_filter import READ_CLASS_SAFE_RULE_KEYS

    return (READ_CLASS_SAFE_RULE_KEYS | _PROTECTED_IDENTITY_FACTS) - _CALL_CONSTANT_KEYS


_SORTED_CLASS_FACT_KEYS: tuple[str, ...] | None = None
_ABSENT = object()


def _sorted_class_fact_keys() -> tuple[str, ...]:
    """``sorted(_class_fact_keys())``, built once (the key sets are frozen)."""
    global _SORTED_CLASS_FACT_KEYS
    if _SORTED_CLASS_FACT_KEYS is None:
        _SORTED_CLASS_FACT_KEYS = tuple(sorted(_class_fact_keys()))
    return _SORTED_CLASS_FACT_KEYS


def _stored_json(value: Any) -> Any:
    """``value`` as a JSONB column stores and returns it."""
    if value is None:
        return None
    return json.loads(json.dumps(value, default=str))


def _stored_id(row: Mapping[str, Any], kind: str) -> Any:
    value = row.get(f"{kind}_id")
    if value is None:
        return None
    if kind == "edge":
        # BIGINT: a numeric string reads back as an int. Anything else cannot
        # be stored at all, so it is kept as given.
        if isinstance(value, str) and value.lstrip("-").isdigit():
            return int(value)
        return value
    return value if isinstance(value, str) else str(value)


def stored_view(row: Mapping[str, Any], kind: str) -> dict[str, Any]:
    """The class-relevant columns of ``row`` as the database returns them.

    A writer holds Python values (a tuple, a mapping with an int key); the
    read sees them after the JSONB round trip. The class is computed on the
    stored form, so the writer and the reader see the same facts.
    """
    attrs = row.get("attrs") or {}
    lineage_attr = attrs.get(LINEAGE_ATTR) if isinstance(attrs, Mapping) else None
    source = row.get("source")
    return {
        f"{kind}_id": _stored_id(row, kind),
        "attrs": {} if lineage_attr is None else {LINEAGE_ATTR: _stored_json(lineage_attr)},
        "source": source if source is None or isinstance(source, str) else str(source),
        "source_record_id": _stored_json(row.get("source_record_id")),
        "source_revision": _stored_json(row.get("source_revision")),
    }


def class_facts(row: Mapping[str, Any], kind: str) -> dict[str, Any] | None:
    """The canonical class facts of one stored row version (design 3.3).

    ``None`` when the row has no class: no source lineage (the read hides it
    without a decision), or facts that cannot be written as canonical JSON.

    The facts are, for each evaluation the read filter would make for this
    row, the values of the class-fact keys its resource carries and whether
    the resource is JSON-shaped (the boundary refuses a malformed one): one
    per source id, one for the row as a graph object, one per contributing
    source record. They are kept as SETS: a row's outcome does not depend on
    the order of its evaluations (design 3.1).
    """
    if kind not in ("node", "edge"):
        raise ValueError(f"kind must be 'node' or 'edge', not {kind!r}")
    try:
        stored = stored_view(row, kind)
    except (TypeError, ValueError, RecursionError):
        return None
    return _facts_of_stored(stored, kind)


def _facts_of_stored(stored: Mapping[str, Any], kind: str) -> dict[str, Any] | None:
    """``class_facts`` of a row's ``stored_view``: the facts read nothing else."""
    from okg.substrate.nomos.read_filter import (
        _fact_text,
        _fine_grained_targets,
        _row_resource,
    )

    try:
        lineage = lineage_for_row(
            stored["attrs"],
            source_id=stored["source"],
            source_record_id=stored["source_record_id"],
            source_revision=stored["source_revision"],
        )
        source_ids = source_ids_from_lineage(lineage)
        if not source_ids:
            return None
        id_key = f"{kind}_id"
        base = _row_resource(
            _STUB_SESSION,
            scope=_STUB_SCOPE,
            row=stored,
            row_kind=kind,
            id_key=id_key,
            lineage=lineage,
            source_ids=source_ids,
            audit=False,
        )
        keys = _sorted_class_fact_keys()
        # Every target resource is ``{**base, ...}``: a value it shares with
        # ``base`` (the same object) is checked once per row, not once per
        # target -- the deep check of the shared lineage was most of the
        # stamp's cost (issue #2941).
        base_compatible: dict[str, bool] = {}

        def compatible(resource: Mapping[str, Any]) -> bool:
            for key, value in resource.items():
                if not isinstance(key, str):
                    return False
                if base.get(key, _ABSENT) is value:
                    ok = base_compatible.get(key)
                    if ok is None:
                        ok = base_compatible[key] = _is_json_compatible(value)
                else:
                    ok = _is_json_compatible(value)
                if not ok:
                    return False
            return True

        def target(resource: Mapping[str, Any]) -> str:
            return json.dumps(
                [
                    [[key, _fact_text(resource[key])] for key in keys if key in resource],
                    compatible(resource),
                ],
                separators=(",", ":"),
            )

        by_source = sorted({target({**base, "source_id": sid}) for sid in source_ids})
        fine_grained = sorted({
            target(resource) + (":fail_closed" if fail_closed else "")
            for resource, _target, fail_closed in _fine_grained_targets(
                base,
                row=stored,
                row_kind=kind,
                id_key=id_key,
                lineage=lineage,
                surface="read-class",
            )
        })
        return {
            "v": CLASS_VOCABULARY_VERSION,
            "kind": kind,
            "has_lineage": True,
            "source_ids": list(source_ids),
            "by_source": by_source,
            "fine_grained": fine_grained,
        }
    except (TypeError, ValueError, RecursionError):
        return None


def canonical_facts_text(facts: Mapping[str, Any]) -> str:
    """The canonical JSON text the class id is the digest of."""
    return json.dumps(facts, sort_keys=True, separators=(",", ":"))


def class_id_of_facts(facts: Mapping[str, Any]) -> uuid.UUID:
    """The class id of canonical class facts: the first 16 bytes of the
    sha256 of their canonical JSON, as a UUID."""
    text = canonical_facts_text(facts)
    return uuid.UUID(bytes=hashlib.sha256(text.encode("utf-8")).digest()[:16])


def read_class(row: Mapping[str, Any], kind: str) -> uuid.UUID | None:
    """The row version's read class: the first 16 bytes of the sha256 of its
    canonical class facts, as a UUID; ``None`` when it has none."""
    facts = class_facts(row, kind)
    if facts is None:
        return None
    return class_id_of_facts(facts)


def stamp_row(
    row: MutableMapping[str, Any],
    kind: str,
    *,
    catalog: Any = None,
) -> MutableMapping[str, Any]:
    """Set the row's two class columns from its own content, in place.

    A row with no class gets NULL in both, never a previous version's value.
    ``catalog`` is the writer's database handle (a Chronos SQL adapter or a
    psycopg connection, in the transaction that writes the row): the class's
    catalog row is written there first, insert-once (PACT
    lane-h-nomos-read-class-sql), so every stamped class has its catalog row
    when the row commits. ``None`` writes no catalog row.
    """
    stamp_rows((row,), kind, catalog=catalog)
    return row


#: ``(class id, facts)`` of a row's class inputs, keyed by ``_stored_and_key``.
KnownReadClasses = dict[bytes, tuple[uuid.UUID | None, dict[str, Any] | None]]


def _stored_and_key(
    row: Mapping[str, Any], kind: str,
) -> tuple[dict[str, Any] | None, bytes | None]:
    """The row's ``stored_view`` and the sha256 of its exact JSON text
    (None: no key).

    The facts are a function of the stored view alone (``_facts_of_stored``),
    and the stored view holds only JSON values, so two rows with the same
    text have the same class. Key order is kept, not sorted: the text is
    the stored view itself, never a looser equivalence. Only a plain
    string (node) or integer (edge) id gets a key: JSON writes a mapping's
    non-string keys as strings, so another id type could share a text with
    an id of a different class.
    """
    if kind not in ("node", "edge"):
        raise ValueError(f"kind must be 'node' or 'edge', not {kind!r}")
    try:
        stored = stored_view(row, kind)
    except (TypeError, ValueError, RecursionError):
        return None, None
    if type(stored[f"{kind}_id"]) is not (str if kind == "node" else int):
        return stored, None
    try:
        text = kind + ":" + json.dumps(stored, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, RecursionError):
        return stored, None
    return stored, hashlib.sha256(text.encode("utf-8")).digest()


def known_read_classes(
    rows: Iterable[Mapping[str, Any]], kind: str, *, into: KnownReadClasses | None = None,
) -> KnownReadClasses:
    """Compute the classes of ``rows`` ahead of their write (issue #2941).

    A write prepared before the publish lock computes its rows' classes
    here, outside the lock; ``stamp_rows(..., known=...)`` under the lock
    then reuses a class only for a row whose class inputs are still
    byte-for-byte the same, and computes any other row as before. Rows of
    one class share one facts object, so a prepared row costs its 32-byte
    key and an entry, not a copy of the facts: every chunk of a publish is
    prepared before the lock and held until its write.
    """
    known: KnownReadClasses = {} if into is None else into
    shared: dict[uuid.UUID, tuple[uuid.UUID, dict[str, Any]]] = {}
    for row in rows:
        stored, key = _stored_and_key(row, kind)
        if stored is None or key is None or key in known:
            continue
        facts = _facts_of_stored(stored, kind)
        if facts is None:
            known[key] = (None, None)
            continue
        value = class_id_of_facts(facts)
        known[key] = shared.setdefault(value, (value, facts))
    return known


def stamp_rows(
    rows: Iterable[MutableMapping[str, Any]],
    kind: str,
    *,
    catalog: Any = None,
    known: KnownReadClasses | None = None,
) -> dict[uuid.UUID, dict[str, Any]]:
    """``stamp_row`` for each row; one catalog write for the whole batch.

    ``known`` (from ``known_read_classes``) supplies the class of a row
    whose class inputs it already holds; every other row is computed here.
    Returns the stamped classes and their facts.
    """
    classes: dict[uuid.UUID, dict[str, Any]] = {}
    for row in rows:
        if known:
            stored, key = _stored_and_key(row, kind)
            if stored is None:
                value, facts = None, None
            elif key is not None and key in known:
                value, facts = known[key]
            else:
                facts = _facts_of_stored(stored, kind)
                value = None if facts is None else class_id_of_facts(facts)
        else:
            facts = class_facts(row, kind)
            value = None if facts is None else class_id_of_facts(facts)
        row[READ_CLASS_COLUMN] = value
        row[READ_CLASS_VERSION_COLUMN] = None if value is None else CLASS_VOCABULARY_VERSION
        if value is not None:
            classes.setdefault(value, facts)
    if catalog is not None and classes:
        record_read_classes(catalog, classes)
    return classes


# --------------------------------------------------------------------------- #
# The class catalog (PACT lane-h-nomos-read-class-sql, design 4.1)            #
# --------------------------------------------------------------------------- #

#: One row per class ever stamped: its id, its canonical facts and the
#: vocabulary version they were computed under. Content-addressed and
#: append-only (insert-once), so it is not a Chronos table: a class no longer
#: present in a generation costs one extra decision at most.
READ_CLASS_CATALOG_TABLE = "okg.nomos_read_classes"

_CATALOG_PRESENT_ATTR = "_okg_nomos_read_class_catalog_present"


def _catalog_flavor(db: Any) -> str | None:
    """``psycopg`` / ``chronos`` for a handle the catalog can be written on,
    or None for one it cannot (a SQLite Chronos adapter)."""
    if isinstance(db, psycopg.Connection):
        return "psycopg"
    dialect = getattr(db, "dialect", None)
    if dialect == "postgres":
        return "chronos"
    if dialect is not None:
        return None
    raise TypeError(
        f"read-class catalog writes need a psycopg connection or a Chronos "
        f"PostgreSQL adapter, not {type(db).__qualname__}"
    )


def _catalog_execute(db: Any, flavor: str, sql: str, params: tuple[Any, ...]) -> list[Any]:
    """Run one statement; ``sql`` uses ``?`` placeholders. Rows as tuples."""
    if flavor == "psycopg":
        with db.cursor() as cur:
            cur.execute(sql.replace("?", "%s"), params)
            rows = cur.fetchall() if cur.description is not None else []
        return [tuple(row.values()) if isinstance(row, Mapping) else tuple(row) for row in rows]
    cursor = db.execute(sql, params)
    if getattr(cursor, "description", None) is None:
        return []
    return [
        tuple(row.values()) if isinstance(row, Mapping) else tuple(row)
        for row in cursor.fetchall()
    ]


def read_class_catalog_present(db: Any) -> bool:
    """Whether the database carries the catalog table (``okg migrate`` adds
    it). A positive answer is cached on the handle; a negative one is not."""
    if getattr(db, _CATALOG_PRESENT_ATTR, False):
        return True
    flavor = _catalog_flavor(db)
    if flavor is None:
        return False
    (present,) = _catalog_execute(
        db, flavor, f"SELECT to_regclass('{READ_CLASS_CATALOG_TABLE}') IS NOT NULL", (),
    )[0]
    if present:
        try:
            setattr(db, _CATALOG_PRESENT_ATTR, True)
        except (AttributeError, TypeError):
            pass
    return bool(present)


def record_read_classes(db: Any, classes: Mapping[uuid.UUID, Mapping[str, Any]]) -> int:
    """Write the catalog rows of ``classes`` that are not there yet; return
    how many were new.

    Insert-once: an existing class is never rewritten, and two writers that
    stamp the same new class concurrently both succeed with one row. Classes
    already in the catalog (the steady state) are found by one primary-key
    read and take no lock; new ones are inserted in class order, so two
    writers inserting overlapping sets take their key locks in the same
    order. A database without the table (not yet migrated) gets nothing, and
    its reads treat the classes as uncatalogued (the exact path).
    """
    if not classes:
        return 0
    flavor = _catalog_flavor(db)
    if flavor is None or not read_class_catalog_present(db):
        return 0
    ids = sorted(str(value) for value in classes)
    existing = {
        str(row[0])
        for row in _catalog_execute(
            db, flavor,
            f"SELECT CAST(read_class AS text) FROM {READ_CLASS_CATALOG_TABLE}"
            " WHERE read_class = ANY(CAST(? AS uuid[]))",
            (ids,),
        )
    }
    by_id = {str(value): facts for value, facts in classes.items()}
    missing = [class_id for class_id in ids if class_id not in existing]
    if not missing:
        return 0
    for class_id in missing:
        facts = by_id[class_id]
        if str(class_id_of_facts(facts)) != class_id:
            raise ValueError(f"read class {class_id} is not the digest of its facts")
    inserted = _catalog_execute(
        db, flavor,
        f"INSERT INTO {READ_CLASS_CATALOG_TABLE}"
        " (read_class, class_facts, vocabulary_version)"
        " SELECT CAST(t.c AS uuid), CAST(t.f AS jsonb), t.v"
        " FROM unnest(CAST(? AS text[]), CAST(? AS text[]), CAST(? AS integer[]))"
        " WITH ORDINALITY AS t(c, f, v, o)"
        " ORDER BY t.o"
        " ON CONFLICT (read_class) DO NOTHING"
        " RETURNING 1",
        (
            missing,
            [canonical_facts_text(by_id[class_id]) for class_id in missing],
            [int(by_id[class_id].get("v", CLASS_VOCABULARY_VERSION)) for class_id in missing],
        ),
    )
    return len(inserted)


def table_kind(table: str) -> str | None:
    """``node`` / ``edge`` for the two graph tables, else None."""
    return {"okg.graph_nodes": "node", "okg.graph_edges": "edge"}.get(table)


def stored_read_class(row: Mapping[str, Any]) -> uuid.UUID | None:
    """The row's stored class when it is present and current, else None.

    A row read from a database without the columns has neither key; a row
    written before the columns existed, or by a writer that did not stamp,
    holds NULL; a row stamped under another vocabulary version is ignored.
    """
    if row.get(READ_CLASS_VERSION_COLUMN) != CLASS_VOCABULARY_VERSION:
        return None
    value = row.get(READ_CLASS_COLUMN)
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def graph_carries_read_class(graph: Any, table: str) -> bool:
    """Whether a query on this checkout can select the two class columns.

    Read from the checkout's own Chronos table registration (the column list
    its visible-row subquery selects), so a process whose registration
    predates the columns, or a database that was never upgraded, answers
    False and the caller leaves the columns out of its SELECT. No query runs.
    """
    ref = getattr(graph, "_ref", None)
    context = getattr(graph, "_context", None)
    backend = getattr(context, "_backend", None)
    tables = None
    metadata = getattr(ref, "metadata", None)
    if isinstance(metadata, Mapping):
        tables = metadata.get("tables")
    if tables is None:
        tables = getattr(backend, "tables", None)
    meta = tables.get(table) if isinstance(tables, Mapping) else None
    columns = getattr(meta, "columns", None) or ()
    return all(column in columns for column in READ_CLASS_COLUMNS)


def read_class_select_sql(graph: Any, table: str, alias: str | None = None) -> str:
    """``, <alias>.nomos_read_class, <alias>.nomos_read_class_version`` when
    the checkout carries the columns, else the empty string."""
    if not graph_carries_read_class(graph, table):
        return ""
    prefix = f"{alias}." if alias else ""
    return "".join(f", {prefix}{column}" for column in READ_CLASS_COLUMNS)


class ReadClassMismatch(ValueError):
    """A stored class disagrees with the class of the row's own content."""

    def __init__(self, *, kind: str, row_id: Any, stored: Any, computed: Any) -> None:
        super().__init__(
            f"stored Nomos read class of {kind} {row_id!r} is {stored}, but its "
            f"content's class is {computed}"
        )
        self.kind = kind
        self.row_id = row_id
        self.stored = stored
        self.computed = computed


def verify_stored_classes(
    rows: Iterable[Mapping[str, Any]],
    kind: str,
    *,
    sample: int,
    rng: random.Random | None = None,
) -> int:
    """Recompute the class of up to ``sample`` random rows that carry a current
    stored class; raise ``ReadClassMismatch`` on the first disagreement.

    Returns the number of rows checked. ``sample <= 0`` checks nothing.
    """
    if sample <= 0:
        return 0
    stamped = [row for row in rows if stored_read_class(row) is not None]
    if not stamped:
        return 0
    chosen = stamped if len(stamped) <= sample else (rng or random).sample(stamped, sample)
    for row in chosen:
        stored = stored_read_class(row)
        computed = read_class(row, kind)
        if computed != stored:
            raise ReadClassMismatch(
                kind=kind, row_id=row.get(f"{kind}_id"), stored=stored, computed=computed,
            )
    return len(chosen)


#: Rows per table the doctor recomputes (``stored_read_class_report``).
DOCTOR_VERIFY_SAMPLE = 200


def stored_read_class_report(
    graph: Any,
    *,
    sample: int = DOCTOR_VERIFY_SAMPLE,
) -> dict[str, Any]:
    """Stored-class health for one checked-out generation (the doctor's view).

    Per graph table: whether the checkout carries the columns, the row counts
    with a current class, a NULL class and another version's class, and a
    verification sample of up to ``sample`` random stamped rows whose class is
    recomputed from their own content. ``mismatches`` lists every disagreement
    in the sample; the caller reports it as an error.
    """
    report: dict[str, Any] = {
        "vocabulary_version": CLASS_VOCABULARY_VERSION,
        "tables": {},
        "mismatches": [],
    }
    for table, kind in (("okg.graph_nodes", "node"), ("okg.graph_edges", "edge")):
        entry: dict[str, Any] = {"columns_present": graph_carries_read_class(graph, table)}
        report["tables"][table] = entry
        if not entry["columns_present"]:
            continue
        counts = graph.query(
            f"""
            SELECT count(*) AS rows,
                   count(*) FILTER (WHERE {READ_CLASS_COLUMN} IS NULL) AS null_class,
                   count(*) FILTER (
                     WHERE {READ_CLASS_COLUMN} IS NOT NULL
                       AND {READ_CLASS_VERSION_COLUMN} = :version
                   ) AS current_class,
                   count(DISTINCT {READ_CLASS_COLUMN}) AS distinct_classes
              FROM {table}
            """,
            {"version": CLASS_VOCABULARY_VERSION},
        )[0]
        entry.update({key: int(value or 0) for key, value in counts.items()})
        entry["other_version_class"] = (
            entry["rows"] - entry["null_class"] - entry["current_class"]
        )
        id_key = f"{kind}_id"
        rows = graph.query(
            f"""
            SELECT {id_key}, attrs, source, source_record_id, source_revision,
                   {READ_CLASS_COLUMN}, {READ_CLASS_VERSION_COLUMN}
              FROM {table}
             WHERE {READ_CLASS_COLUMN} IS NOT NULL
               AND {READ_CLASS_VERSION_COLUMN} = :version
             ORDER BY random()
             LIMIT :sample
            """,
            {"version": CLASS_VOCABULARY_VERSION, "sample": max(0, int(sample))},
        )
        entry["verified"] = len(rows)
        for row in rows:
            computed = read_class(row, kind)
            if computed != stored_read_class(row):
                report["mismatches"].append({
                    "table": table,
                    id_key: row.get(id_key),
                    "stored": str(row.get(READ_CLASS_COLUMN)),
                    "computed": None if computed is None else str(computed),
                })
    return report


def vocabulary_inventory() -> dict[str, list[str]]:
    """The vocabulary ``CLASS_VOCABULARY_DIGEST`` pins (design 4.6, test T5).

    The keys the read filter's builders put in each kind of resource, built
    for sample rows of both kinds (a by-source target, a graph-object target
    and a source-record target), the class-fact keys and the allow-list.
    """
    from okg.substrate.nomos.read_filter import (
        READ_CLASS_SAFE_RULE_KEYS,
        _fine_grained_targets,
        _row_resource,
    )
    from okg.substrate.nomos.source_lineage import stamp_attrs_with_source_lineage

    inventory: dict[str, set[str]] = {}
    for kind in ("node", "edge"):
        id_key = f"{kind}_id"
        attrs = stamp_attrs_with_source_lineage(
            {}, source_id="src", source_record_id={"path": "a"},
            source_revision={"sha": "1"}, policy_fingerprint="p",
        )
        row = {id_key: "n1" if kind == "node" else 1, "attrs": attrs}
        lineage = lineage_for_row(attrs, source_id=None)
        source_ids = source_ids_from_lineage(lineage)
        base = _row_resource(
            _STUB_SESSION, scope=_STUB_SCOPE, row=row, row_kind=kind, id_key=id_key,
            lineage=lineage, source_ids=source_ids, audit=False,
        )
        inventory.setdefault(f"{kind}.by_source", set()).update({**base, "source_id": "src"})
        for resource, _target, _fail in _fine_grained_targets(
            base, row=row, row_kind=kind, id_key=id_key, lineage=lineage, surface="s",
        ):
            inventory.setdefault(f"{kind}.{resource['resource_kind']}", set()).update(resource)
    sample = class_facts(
        {"node_id": "n1", "attrs": stamp_attrs_with_source_lineage(
            {}, source_id="src", source_record_id={"path": "a"},
            source_revision={"sha": "1"}, policy_fingerprint="p",
        )},
        "node",
    ) or {}
    inventory["class_facts"] = set(sample)
    inventory["class_fact_keys"] = set(_class_fact_keys())
    inventory["safe_rule_keys"] = set(READ_CLASS_SAFE_RULE_KEYS)
    inventory["protected_identity_facts"] = set(_PROTECTED_IDENTITY_FACTS)
    return {name: sorted(keys) for name, keys in sorted(inventory.items())}


def vocabulary_digest() -> str:
    text = json.dumps(
        {"version": CLASS_VOCABULARY_VERSION, "inventory": vocabulary_inventory()},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


__all__ = [
    "READ_CLASS_CATALOG_TABLE",
    "canonical_facts_text",
    "class_id_of_facts",
    "read_class_catalog_present",
    "record_read_classes",
    "CLASS_VOCABULARY_DIGEST",
    "CLASS_VOCABULARY_VERSION",
    "READ_CLASS_COLUMN",
    "READ_CLASS_COLUMNS",
    "READ_CLASS_VERSION_COLUMN",
    "ReadClassMismatch",
    "KnownReadClasses",
    "class_facts",
    "graph_carries_read_class",
    "known_read_classes",
    "read_class",
    "read_class_select_sql",
    "stamp_row",
    "stamp_rows",
    "stored_read_class",
    "stored_view",
    "table_kind",
    "verify_stored_classes",
    "vocabulary_digest",
    "vocabulary_inventory",
]
