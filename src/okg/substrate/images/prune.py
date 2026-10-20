"""Which ``kind = image`` blob rows the graph still references (#3046).

``okg db prune-blobs --kind image`` deletes an image row only when no
retained generation references it (and its grace window has passed; see
``okg.substrate.db.blob_prune``). A reference is the ``png.id`` of a
completed file image value, found exactly where `values.iter_image_values`
finds image values: an attr value, or an element of a list attr value. A
mapping tagged ``okg_image`` nested any deeper is ordinary data and names no
image, so it is not a reference either.

Where the references are read from, and why that is every retained one:

* The Chronos interval storage of ``okg.graph_nodes`` -- every physical
  table registered for it in ``_chronos_branch_tables``, plus any schema
  version's table in ``_chronos_branch_table_schema_versions``. All rows are
  read: history compaction physically deletes the rows of generations it
  retires, so every row still there belongs to a retained generation (or a
  quarantine/workspace branch, or an unpublished write), and is kept.
* The Chronos interval storage of ``okg.graph_node_declarers``. It holds the
  node row each substitute declarer would store; when the owning source
  retracts, a surviving declarer's row becomes the graph row without any
  ingest running, so its images must survive too.
* The plain ``okg.graph_nodes`` / ``okg.graph_node_declarers`` relations, which
  hold rows only on a database whose graph is not (yet) under Chronos.
* ``okg.attribute_effective_attrs`` -- the per-generation overlay values that
  merge over node attrs on read -- and ``okg.attribute_observations``, whose
  candidate values can become effective later without an ingest. Each row
  holds one attr's value, so the value itself or a list element is searched.

``okg.attribute_versions`` and the effective views are derived from the
tables above. Cold-history maintenance drops indexes only, never rows.
"""
from __future__ import annotations

from typing import Any

import psycopg

from okg.substrate.db.blob_prune import (
    BlobKindReferences,
    ReferenceScan,
    ScanSource,
    scan_json_references,
)
from okg.substrate.images.values import IMAGE_TAG, KIND_FILE

__all__ = [
    "ATTRS_PNG_ID_PATHS",
    "IMAGE_REFERENCES",
    "VALUE_PNG_ID_PATHS",
    "image_reference_sources",
    "scan_image_references",
]

# A completed file value whose png.id is a string. Strict mode, so a list is
# never unwrapped implicitly: only the two positions iter_image_values
# reads are searched, and an error (a png that is not an object) only drops
# that value from the filter.
_COMPLETED = (
    f'@.type() == "object" && @.{IMAGE_TAG} == "{KIND_FILE}" '
    '&& @.png.id.type() == "string"'
)

#: Over a node's attrs object: an attr value, then a list attr's elements.
_ATTR_VALUES = 'strict $ ? (@.type() == "object").*'
ATTRS_PNG_ID_PATHS: tuple[str, ...] = (
    f"{_ATTR_VALUES} ? ({_COMPLETED}).png.id",
    f'{_ATTR_VALUES} ? (@.type() == "array")[*] ? ({_COMPLETED}).png.id',
)

#: Over one attr's value: the value itself, then a list value's elements.
VALUE_PNG_ID_PATHS: tuple[str, ...] = (
    f'strict $ ? ({_COMPLETED}).png.id',
    f'strict $ ? (@.type() == "array")[*] ? ({_COMPLETED}).png.id',
)

_NODE_TABLE = "okg.graph_nodes"
_DECLARERS_TABLE = "okg.graph_node_declarers"
_CHRONOS_TABLES = (_NODE_TABLE, _DECLARERS_TABLE)
_OVERLAY_TABLES = ("okg.attribute_effective_attrs", "okg.attribute_observations")


def _exists(cur: psycopg.Cursor[Any], relation: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (relation,))
    return cur.fetchone()[0] is not None


def image_reference_sources(
    cur: psycopg.Cursor[Any],
) -> tuple[list[ScanSource], bool]:
    """The relations image references are read from, and whether the
    graph's node storage is registered with Chronos at all."""
    physical: dict[str, str] = {}
    if _exists(cur, "_chronos_branch_tables"):
        cur.execute(
            """
            SELECT table_name, physical_table FROM _chronos_branch_tables
             WHERE backend = 'interval' AND table_name = ANY(%s)
            """,
            (list(_CHRONOS_TABLES),),
        )
        rows = list(cur.fetchall())
        if _exists(cur, "_chronos_branch_table_schema_versions"):
            cur.execute(
                """
                SELECT table_name, physical_table
                  FROM _chronos_branch_table_schema_versions
                 WHERE backend = 'interval' AND table_name = ANY(%s)
                """,
                (list(_CHRONOS_TABLES),),
            )
            rows.extend(cur.fetchall())
        for table_name, physical_table in rows:
            physical.setdefault(str(physical_table), str(table_name))
    graph_registered = _NODE_TABLE in physical.values()

    sources: list[ScanSource] = []
    for relation in sorted(physical):
        if not _exists(cur, relation):
            # A registry row whose table is gone holds no rows to scan.
            continue
        sources.append(
            ScanSource(
                logical=physical[relation],
                storage="chronos_interval",
                relation=relation,
                column="attrs",
                paths=ATTRS_PNG_ID_PATHS,
            )
        )
    for relation in _CHRONOS_TABLES:
        if _exists(cur, relation):
            sources.append(
                ScanSource(
                    logical=relation,
                    storage="table",
                    relation=relation,
                    column="attrs",
                    paths=ATTRS_PNG_ID_PATHS,
                )
            )
    for relation in _OVERLAY_TABLES:
        if _exists(cur, relation):
            sources.append(
                ScanSource(
                    logical=relation,
                    storage="table",
                    relation=relation,
                    column="value",
                    paths=VALUE_PNG_ID_PATHS,
                )
            )
    return sources, graph_registered


def scan_image_references(
    cur: psycopg.Cursor[Any], batch_rows: int
) -> ReferenceScan:
    """Every image blob id a retained row references."""
    sources, graph_registered = image_reference_sources(cur)
    scan = ReferenceScan(graph_registered=graph_registered)
    for source in sources:
        found, report = scan_json_references(cur, source, batch_rows=batch_rows)
        scan.referenced.update(found)
        scan.sources.append(report)
    return scan


IMAGE_REFERENCES = BlobKindReferences(
    kind="image",
    description="png.id of a completed file image value in node attrs or overlays",
    scan=scan_image_references,
)
