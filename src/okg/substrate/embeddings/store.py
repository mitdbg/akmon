"""Vector-set storage: sets, vectors, document links and pin watermarks.

PACT openrouter-embeddings-hnsw (design v3.2). The tables are created by
``schema.sql`` (``okg migrate``):

* ``okg.embedding_sets`` -- one row per registered set, with its per-set
  ``next_seq`` counter of document links;
* ``okg.embedding_vector_counters`` -- the per-set counter of vectors;
* ``okg.text_embeddings`` -- one vector per ``(set_id, doc_hash)``;
* ``okg.node_documents`` -- which document a node's *content* renders to,
  keyed by ``(set_id, template_hash, node_id, row_hash, seq)``;
* ``okg.embedding_watermarks`` -- the highest link ``seq`` (``max_seq``) and
  vector ``seq`` (``max_vector_seq``) a published generation may see, per set.

Every writer takes its sequence numbers with :func:`take_seq` inside its own
insert transaction: the row lock on the counter makes one set's writers of
that table commit in number order, so the committed value the publisher
reads is a watermark no later commit can slip under. Links and vectors have
separate counters (separate rows): a backfill holds the vector counter
through its HNSW inserts, and the graph writer, which writes links only,
never waits on it. Rows are insert-only (``ON CONFLICT DO NOTHING``).

The old node columns ``embedding`` / ``embedding_model`` are not touched here.
"""
from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, NamedTuple

import psycopg
from psycopg.rows import tuple_row

from okg.substrate.embeddings.config import (
    DocumentTemplate,
    VectorIndexConfig,
    VectorSetConfig,
)
from okg.substrate.embeddings.documents import render_document
from okg.substrate.errors import SubstrateError


__all__ = [
    "ensure_set_index",
    "load_classifications",
    "load_registered_sets",
    "load_set",
    "load_template_records",
    "mark_node_rows_restored",
    "missing_documents",
    "missing_links",
    "node_document_at_pin",
    "record_templates",
    "rebuild_set_index",
    "record_fill_completion",
    "record_watermarks",
    "register_sets",
    "row_hash_expression",
    "seq_advanced_since",
    "set_index_name",
    "take_seq",
    "Watermark",
    "watermark_for",
    "write_node_documents",
    "write_vectors",
]

_LOG = logging.getLogger(__name__)

#: Rows per insert statement when linking documents.
_LINK_BATCH = 5000


def _error(message: str, *, code: str, data: dict[str, Any], fix_hint: str) -> SubstrateError:
    err = SubstrateError(message, data=data, fix_hint=fix_hint)
    err.code = code
    return err


def row_hash_expression(alias: str) -> str:
    """``sha256(subtype || attrs::text)`` over a row aliased ``alias``.

    The one definition the writer (from the values it stores) and the reader
    (from the visible row) both use, so they never disagree on how jsonb
    prints. ``attrs`` is NOT NULL on node rows, so the hash is never NULL.
    """
    if not alias.isidentifier():
        raise ValueError(f"bad SQL alias {alias!r}")
    return (
        f"encode(sha256(convert_to({alias}.subtype || CAST({alias}.attrs AS text), "
        "'UTF8')), 'hex')"
    )


def _tables_present(conn: psycopg.Connection) -> bool:
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT to_regclass('okg.embedding_sets') IS NOT NULL")
        row = cur.fetchone()
    return bool(row and row[0])


def set_index_name(vector_set: VectorSetConfig) -> str:
    """``text_embeddings_<set hex>_<build hash>_hnsw``: derived from the set
    id and the build settings, so a rebuild with new settings gets a new name
    and can be built beside the old index."""
    build = json.dumps(vector_set.index.build_json(), sort_keys=True,
                       separators=(",", ":"))
    build_hash = hashlib.sha256(build.encode("utf-8")).hexdigest()[:8]
    return f"text_embeddings_{vector_set.set_id[3:]}_{build_hash}_hnsw"


# --------------------------------------------------------------------------- #
# Registration                                                                 #
# --------------------------------------------------------------------------- #

def register_sets(
    conn: psycopg.Connection, sets: Sequence[VectorSetConfig],
) -> list[dict[str, Any]]:
    """Register each set; keep the counter of one already registered.

    The identity of a registered set never changes (it is its id); the
    document rules and index settings are updated in place, because they are
    not part of the identity.

    A registration that changes the document rules or reactivates the set
    takes one link number (no link row uses it). The link counter is the
    set's commit-ordered clock: a worker fill compares it with its last
    completion's (``_carry_completion_forward``), so such a change can
    never pass for "nothing changed", whatever the transactions' start
    times. It also makes the next publish cut a new generation.
    """
    from okg.substrate.embeddings.config import (
        EmbeddingConfigError,
        set_width_refusal,
    )

    # Every set's width against pgvector's limits before any row is written:
    # a sentence-transformers set learns its width here, from its model.
    for item in sets:
        refusal = set_width_refusal(item.width, item.index.kind, item.storage)
        if refusal is not None:
            raise EmbeddingConfigError(f"vector set {item.name!r}: {refusal}")
    out: list[dict[str, Any]] = []
    with conn.cursor(row_factory=tuple_row) as cur:
        for item in sets:
            cur.execute(
                """
                INSERT INTO okg.embedding_sets
                  (set_id, name, backend, identity_json, width, documents_json,
                   index_json)
                VALUES (%s, %s, %s, CAST(%s AS jsonb), %s, CAST(%s AS jsonb),
                        CAST(%s AS jsonb))
                ON CONFLICT (set_id) DO UPDATE
                   SET name = EXCLUDED.name,
                       documents_json = EXCLUDED.documents_json,
                       index_json = EXCLUDED.index_json,
                       active = TRUE,
                       next_seq = embedding_sets.next_seq + CASE
                         WHEN embedding_sets.documents_json
                                IS DISTINCT FROM EXCLUDED.documents_json
                           OR NOT embedding_sets.active
                         THEN 1 ELSE 0 END,
                       updated_at = now()
                RETURNING (xmax = 0) AS created, next_seq
                """,
                (
                    item.set_id,
                    item.name,
                    item.backend,
                    json.dumps(item.identity(), sort_keys=True),
                    item.width,
                    json.dumps(item.documents_json(), sort_keys=True),
                    json.dumps(_index_json(item.index), sort_keys=True),
                ),
            )
            created, next_seq = cur.fetchone()
            # A new set's vectors start at 1. A set registered while links
            # and vectors shared one counter got its row from schema.sql.
            cur.execute(
                "INSERT INTO okg.embedding_vector_counters (set_id) VALUES (%s) "
                "ON CONFLICT (set_id) DO NOTHING",
                (item.set_id,),
            )
            out.append({
                "set": item.name,
                "set_id": item.set_id,
                "created": bool(created),
                "max_seq": int(next_seq) - 1,
            })
    return out


def deactivate_other_sets(
    conn: psycopg.Connection, keep: Sequence[VectorSetConfig],
) -> list[str]:
    """Mark every registered set not in ``keep`` inactive; returns their ids.

    An inactive set keeps its vectors, links and watermarks (old pins still
    read them) but takes no new links. Registering it again reactivates it.
    """
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "UPDATE okg.embedding_sets SET active = FALSE, updated_at = now() "
            "WHERE active AND NOT (set_id = ANY(%s)) RETURNING set_id",
            ([item.set_id for item in keep],),
        )
        return sorted(str(row[0]) for row in cur.fetchall())


def load_set(
    conn: psycopg.Connection, set_id: str,
) -> tuple[VectorSetConfig, bool] | None:
    """The registered set as stored (its templates are what the writer used)
    and whether it is active; None when it is not registered."""
    if not _tables_present(conn):
        return None
    from psycopg.rows import dict_row

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT set_id, name, backend, identity_json, documents_json, "
            "index_json, active FROM okg.embedding_sets WHERE set_id = %s",
            (set_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return _set_from_row(row), bool(row["active"])


def config_drift(configured: VectorSetConfig, stored: VectorSetConfig) -> list[str]:
    """Which non-identity settings deployment.yaml and the registered row
    disagree on (``okg migrate`` has not run since the file changed)."""
    drift = []
    if configured.documents_json() != stored.documents_json():
        drift.append("document")
    if _index_json(configured.index) != _index_json(stored.index):
        drift.append("index")
    if configured.name != stored.name:
        drift.append("name")
    return drift


def registered_set(conn: psycopg.Connection, set_id: str) -> dict[str, Any] | None:
    """``{"active": bool, "width": int}`` for a registered set, else None."""
    if not _tables_present(conn):
        return None
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT active, width FROM okg.embedding_sets WHERE set_id = %s",
            (set_id,),
        )
        row = cur.fetchone()
    return None if row is None else {"active": bool(row[0]), "width": int(row[1])}


def _index_json(index: VectorIndexConfig) -> dict[str, Any]:
    return {
        "kind": index.kind,
        "m": index.m,
        "ef_construction": index.ef_construction,
        "ef_search": index.ef_search,
        "max_scan_tuples": index.max_scan_tuples,
    }


def _set_from_row(row: Mapping[str, Any]) -> VectorSetConfig:
    identity = row["identity_json"]
    documents = row["documents_json"]
    index = row["index_json"]

    def template(value: Mapping[str, Any]) -> DocumentTemplate:
        return DocumentTemplate(
            header=tuple(str(item) for item in value["header"]),
            body=str(value["body"]),
        )

    subtypes = documents.get("subtypes")
    return VectorSetConfig(
        name=str(row["name"]),
        backend=str(row["backend"]),
        subtypes=None if subtypes is None else tuple(str(s) for s in subtypes),
        max_chars=int(identity["max_chars"]),
        document_prefix=str(identity["document_prefix"]),
        query_prefix=str(identity["query_prefix"]),
        provider=identity.get("provider"),
        default_template=template(documents["default"]),
        type_templates=tuple(
            (str(name), template(value))
            for name, value in sorted(documents.get("types", {}).items())
        ),
        index=VectorIndexConfig(
            kind=str(index["kind"]),
            m=int(index["m"]),
            ef_construction=int(index["ef_construction"]),
            ef_search=int(index["ef_search"]),
            max_scan_tuples=int(index["max_scan_tuples"]),
        ),
        # The width the set was registered with: a sentence-transformers
        # set read back needs no model load to know it.
        resolved_width=int(identity["width"]),
        # Absent from every identity registered before the choice existed.
        storage=str(identity.get("storage", "halfvec")),
    )


def load_registered_sets(
    conn: psycopg.Connection,
) -> tuple[list[VectorSetConfig], list[str]]:
    """``(current active sets, stale set ids)``. A stale set is one whose
    stored identity no longer hashes to its id under this code (for example a
    new document format version); it takes no new links. Inactive sets
    (:func:`deactivate_other_sets`) are left out."""
    if not _tables_present(conn):
        return [], []
    from psycopg.rows import dict_row

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT set_id, name, backend, identity_json, documents_json, "
            "index_json FROM okg.embedding_sets WHERE active ORDER BY set_id"
        )
        rows = cur.fetchall()
    current: list[VectorSetConfig] = []
    stale: list[str] = []
    for row in rows:
        try:
            item = _set_from_row(row)
            same = item.set_id == str(row["set_id"])
        except (KeyError, TypeError, ValueError, SubstrateError):
            same = False
        if same:
            current.append(item)
        else:
            stale.append(str(row["set_id"]))
    return current, stale


def load_classifications(
    conn: psycopg.Connection,
    subtypes: Iterable[str],
    *,
    catalog_version_id: Any = None,
) -> dict[str, Mapping[str, Any] | None]:
    """Each subtype's declared ``pii_classes`` (None when unannotated).

    With ``catalog_version_id`` it reads that catalog; otherwise, like the
    graph writer's searchable-text lookup, the latest applied catalog that
    declares the subtype. Malformed metadata fails closed.
    """
    names = sorted(set(subtypes))
    if not names:
        return {}
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT to_regclass('okg.subtypes') IS NOT NULL")
        if not cur.fetchone()[0]:
            return {name: None for name in names}
        if catalog_version_id is None:
            cur.execute(
                """
                SELECT DISTINCT ON (s.name) s.name, s.metadata
                  FROM okg.subtypes s
                  JOIN okg.catalog_versions cv
                    ON cv.catalog_version_id = s.catalog_version_id
                 WHERE cv.status = 'applied' AND s.name = ANY(%s)
                 ORDER BY s.name, s.catalog_version_id DESC
                """,
                (names,),
            )
        else:
            cur.execute(
                "SELECT name, metadata FROM okg.subtypes "
                "WHERE catalog_version_id = %s AND name = ANY(%s)",
                (catalog_version_id, names),
            )
        rows = cur.fetchall()
    out: dict[str, Mapping[str, Any] | None] = {name: None for name in names}
    for name, metadata in rows:
        if metadata is None:
            continue
        if not isinstance(metadata, Mapping):
            raise _error(
                "catalog subtype metadata must be a mapping",
                code="embedding_catalog_unreadable",
                data={"subtype": str(name)},
                fix_hint="repair the subtype's catalog metadata",
            )
        if "pii_classes" not in metadata:
            continue
        declared = metadata["pii_classes"]
        if not isinstance(declared, Mapping) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in declared.items()
        ):
            raise _error(
                "catalog subtype pii_classes must map attribute names to classes",
                code="embedding_catalog_unreadable",
                data={"subtype": str(name)},
                fix_hint="repair the subtype's pii_classes declaration",
            )
        out[str(name)] = dict(declared)
    return out


# --------------------------------------------------------------------------- #
# Sequence numbers and writes                                                  #
# --------------------------------------------------------------------------- #

#: The statement that takes from each counter. Whole literal statements, not a
#: table name spliced in, so the write inventory sees the real target table.
_TAKE_SEQ_SQL = {
    "links": (
        "UPDATE okg.embedding_sets SET next_seq = next_seq + %s "
        "WHERE set_id = %s RETURNING next_seq - %s"
    ),
    "vectors": (
        "UPDATE okg.embedding_vector_counters SET next_seq = next_seq + %s "
        "WHERE set_id = %s RETURNING next_seq - %s"
    ),
}


def take_seq(
    conn: psycopg.Connection, set_id: str, count: int, *, counter: str = "links",
) -> int:
    """Reserve ``count`` numbers of ``set_id``'s ``counter`` (``links`` or
    ``vectors``); returns the first.

    Must run inside the caller's insert transaction: the row lock it takes is
    what orders this set's writers of that table by commit. The two
    counters are separate rows, so a vector batch holding its counter never
    blocks a link writer.
    """
    if count < 1:
        raise ValueError("count must be at least 1")
    sql = _TAKE_SEQ_SQL.get(counter)
    if sql is None:
        raise ValueError(f"counter must be links or vectors, not {counter!r}")
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(sql, (int(count), set_id, int(count)))
        row = cur.fetchone()
    if row is None:
        raise _error(
            f"vector set {set_id!r} is not registered",
            code="embedding_set_unregistered",
            data={"set_id": set_id},
            fix_hint="run `okg migrate --deployment <name> --apply`",
        )
    return int(row[0])


def write_vectors(
    conn: psycopg.Connection,
    set_id: str,
    vectors: Sequence[tuple[str, Sequence[float]]],
) -> int:
    """Insert ``(doc_hash, vector)`` rows for one set; returns rows inserted.

    Every vector must have the set's width. Already-stored hashes are kept
    as they are (insert-only).
    """
    if not vectors:
        return 0
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT width, COALESCE(identity_json->>'storage', 'halfvec') "
            "FROM okg.embedding_sets WHERE set_id = %s", (set_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise _error(
            f"vector set {set_id!r} is not registered",
            code="embedding_set_unregistered",
            data={"set_id": set_id},
            fix_hint="run `okg migrate --deployment <name> --apply`",
        )
    width = int(row[0])
    bad = [doc for doc, vec in vectors if len(vec) != width]
    if bad:
        raise _error(
            f"vectors for set {set_id!r} must have {width} values",
            code="embedding_vector_width_mismatch",
            data={"set_id": set_id, "width": width, "doc_hashes": bad[:5]},
            fix_hint="embed with the set's own backend and width",
        )
    statement = _INSERT_VECTORS.get(str(row[1]))
    if statement is None:
        raise _error(
            f"vector set {set_id!r} names unknown storage {row[1]!r}",
            code="embedding_set_storage_unknown",
            data={"set_id": set_id, "storage": row[1]},
            fix_hint="re-register the set with `okg migrate`",
        )
    first = take_seq(conn, set_id, len(vectors), counter="vectors")
    literals = [
        "[" + ",".join(f"{float(v):.7g}" for v in vec) + "]" for _doc, vec in vectors
    ]
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            statement,
            (set_id, first, [doc for doc, _vec in vectors], literals),
        )
        return int(cur.rowcount)


#: One insert per storage type (PACT vector-set-pgvector-widths): a halfvec
#: set's vectors go to ``embedding``, a vector set's to ``embedding_f32``;
#: the table's check keeps exactly one of the two set.
_INSERT_VECTORS = {
    "halfvec": """
        INSERT INTO okg.text_embeddings (set_id, doc_hash, embedding, seq)
        SELECT %s, t.doc_hash, CAST(t.vec AS halfvec), %s + t.ord - 1
          FROM unnest(CAST(%s AS text[]), CAST(%s AS text[]))
               WITH ORDINALITY AS t(doc_hash, vec, ord)
        ON CONFLICT (set_id, doc_hash) DO NOTHING
        """,
    "vector": """
        INSERT INTO okg.text_embeddings (set_id, doc_hash, embedding_f32, seq)
        SELECT %s, t.doc_hash, CAST(t.vec AS vector), %s + t.ord - 1
          FROM unnest(CAST(%s AS text[]), CAST(%s AS text[]))
               WITH ORDINALITY AS t(doc_hash, vec, ord)
        ON CONFLICT (set_id, doc_hash) DO NOTHING
        """,
}


def row_hashes(
    conn: psycopg.Connection, rows: Sequence[tuple[str, str]],
) -> list[str]:
    """``row_hash`` of each ``(subtype, attrs_json)``, in order, computed by
    PostgreSQL (the jsonb of ``attrs_json`` prints as the stored row does).
    One round trip per batch, whatever the number of sets.

    The attrs go as ONE JSON array parameter, not a ``text[]`` of documents:
    on the C4 copy a 1,000-row batch (2 MB of attrs) took 106 ms as a
    ``text[]`` and 34 ms as one JSON array, with the same hashes (an element
    of a parsed jsonb array prints as the same document parsed alone)."""
    out: list[str] = []
    for start in range(0, len(rows), _LINK_BATCH):
        batch = rows[start:start + _LINK_BATCH]
        attrs_array = "[" + ",".join(row[1] for row in batch) + "]"
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                f"""
                SELECT {row_hash_expression('n')}
                  FROM (SELECT s.subtype, a.attrs, a.ord
                          FROM jsonb_array_elements(CAST(%s AS jsonb))
                               WITH ORDINALITY AS a(attrs, ord)
                          JOIN unnest(CAST(%s AS text[]))
                               WITH ORDINALITY AS s(subtype, ord) USING (ord)) AS n
                 ORDER BY n.ord
                """,
                (attrs_array, [row[0] for row in batch]),
            )
            out.extend(str(row[0]) for row in cur.fetchall())
    if len(out) != len(rows):
        raise RuntimeError(
            f"row hashes: {len(out)} hashes for {len(rows)} rows"
        )
    return out


#: A batch of ``(template_hash, node_id, row_hash, doc_hash)`` links passed
#: as one JSON array parameter (:func:`_links_json`) and read back as rows
#: ``t`` with their 1-based position ``t.ord``. One JSON parameter instead
#: of four ``text[]``: about 10 ms less per 1,000-link statement on the C4
#: copy.
_LINKS_FROM_JSON = (
    "(SELECT e.v->>0 AS template_hash, e.v->>1 AS node_id, e.v->>2 AS row_hash,"
    "        e.v->>3 AS doc_hash, e.ord"
    "   FROM jsonb_array_elements(CAST(%s AS jsonb)) WITH ORDINALITY AS e(v, ord)) AS t"
)


def _links_json(links: Sequence[tuple[str, str, str, str]]) -> str:
    return json.dumps([list(link) for link in links])


def missing_links(
    conn: psycopg.Connection,
    set_id: str,
    links: Sequence[tuple[str, str, str, str]],
) -> list[tuple[str, str, str, str]]:
    """The entries of ``links`` to write, in order, each once.

    Each entry is ``(template_hash, node_id, row_hash, doc_hash)``; a link
    is identified by its first three fields. An entry is missing when no
    such link is stored, or when every stored copy has a newer link of
    another template for the same node content: the template became current
    again (T1, T2, then T1), and a newer copy is what lets the reader's
    newest-link rule serve it. Reads only.
    """
    out: list[tuple[str, str, str, str]] = []
    for start in range(0, len(links), _LINK_BATCH):
        batch = links[start:start + _LINK_BATCH]
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                f"""
                SELECT t.template_hash, t.node_id, t.row_hash, t.doc_hash
                  FROM {_LINKS_FROM_JSON}
                 WHERE NOT EXISTS (
                   SELECT 1 FROM okg.node_documents AS d
                    WHERE d.set_id = %s AND d.template_hash = t.template_hash
                      AND d.node_id = t.node_id AND d.row_hash = t.row_hash
                      AND NOT EXISTS (
                        SELECT 1 FROM okg.node_documents AS d2
                         WHERE d2.set_id = d.set_id AND d2.node_id = d.node_id
                           AND d2.row_hash = d.row_hash AND d2.seq > d.seq
                           AND d2.template_hash <> d.template_hash
                      )
                 )
                 ORDER BY t.ord
                """,
                (_links_json(batch), set_id),
            )
            out.extend(tuple(str(v) for v in row) for row in cur.fetchall())
    # One link per (template, node, row) even if the input repeats one.
    return list(dict.fromkeys(out))


def missing_documents(
    conn: psycopg.Connection, set_id: str, doc_hashes: Iterable[str],
) -> set[str]:
    """The hashes among ``doc_hashes`` with no stored vector in the set (at
    any ``seq``). Reads only."""
    wanted = sorted(set(doc_hashes))
    if not wanted:
        return set()
    stored: set[str] = set()
    for start in range(0, len(wanted), _LINK_BATCH):
        batch = wanted[start:start + _LINK_BATCH]
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                "SELECT doc_hash FROM okg.text_embeddings "
                "WHERE set_id = %s AND doc_hash = ANY(%s)",
                (set_id, batch),
            )
            stored.update(str(row[0]) for row in cur.fetchall())
    return set(wanted) - stored


def record_templates(
    conn: psycopg.Connection,
    set_id: str,
    records: Mapping[str, Mapping[str, Any]],
) -> int:
    """Store each ``template_hash -> template_record`` of one set not stored
    yet; returns rows inserted. Writers call it with the links they write, so
    every template a link names can be re-checked by a later read."""
    if not records:
        return 0
    _require_templates_table(conn)
    hashes = sorted(records)
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT template_hash FROM okg.embedding_templates "
            "WHERE set_id = %s AND template_hash = ANY(%s)",
            (set_id, hashes),
        )
        stored = {str(row[0]) for row in cur.fetchall()}
        missing = [h for h in hashes if h not in stored]
        if not missing:
            return 0
        cur.execute(
            """
            INSERT INTO okg.embedding_templates
              (set_id, template_hash, subtype, record_json)
            SELECT %s, t.template_hash, t.subtype, CAST(t.record AS jsonb)
              FROM unnest(CAST(%s AS text[]), CAST(%s AS text[]),
                          CAST(%s AS text[])) AS t(template_hash, subtype, record)
            ON CONFLICT (set_id, template_hash) DO NOTHING
            """,
            (
                set_id,
                missing,
                [str(records[h]["subtype"]) for h in missing],
                [json.dumps(records[h], sort_keys=True) for h in missing],
            ),
        )
        return int(cur.rowcount)


def _require_templates_table(conn: psycopg.Connection) -> None:
    """Refuse when a registered set's database lacks the template records
    table (migrated by an earlier build). Skipping would be silent harm:
    links written without their template record are never served after a
    template change, and a read would miss every older template."""
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute("SELECT to_regclass('okg.embedding_templates') IS NOT NULL")
        row = cur.fetchone()
    if not (row and row[0]):
        raise _error(
            "okg.embedding_templates is missing, so vector-set document links "
            "cannot record or check their templates",
            code="embedding_schema_outdated",
            data={"table": "okg.embedding_templates"},
            fix_hint="run `okg migrate --deployment <name> --apply`",
        )


def load_template_records(
    conn: psycopg.Connection, set_id: str, subtypes: Iterable[str],
) -> list[tuple[str, str, dict[str, Any]]]:
    """``(subtype, template_hash, record)`` of every template stored for the
    set's ``subtypes``. Refuses (``embedding_schema_outdated``) when the
    table is missing."""
    names = sorted(set(subtypes))
    if not names:
        return []
    _require_templates_table(conn)
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT subtype, template_hash, record_json FROM okg.embedding_templates "
            "WHERE set_id = %s AND subtype = ANY(%s) ORDER BY subtype, template_hash",
            (set_id, names),
        )
        return [(str(s), str(h), dict(r)) for s, h, r in cur.fetchall()]


def link_documents(
    conn: psycopg.Connection,
    set_id: str,
    links: Sequence[tuple[str, str, str, str]],
) -> int:
    """Insert the missing links among ``links`` for one set; returns how many.

    Each entry is ``(template_hash, node_id, row_hash, doc_hash)``. Links
    already stored take no sequence number, so a write that changes nothing
    never moves the set's counter (and never makes the publisher refuse a
    reuse for nothing). A link whose template became current again after
    another template's link for the same content gets a newer copy
    (:func:`missing_links`); older rows stay, so older pins read the same.
    """
    written = 0
    for start in range(0, len(links), _LINK_BATCH):
        batch = links[start:start + _LINK_BATCH]
        missing = missing_links(conn, set_id, batch)
        if not missing:
            continue
        first = take_seq(conn, set_id, len(missing))
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                f"""
                INSERT INTO okg.node_documents
                  (set_id, template_hash, node_id, row_hash, doc_hash, seq)
                SELECT %s, t.template_hash, t.node_id, t.row_hash, t.doc_hash,
                       %s + t.ord - 1
                  FROM {_LINKS_FROM_JSON}
                ON CONFLICT (set_id, template_hash, node_id, row_hash, seq)
                DO NOTHING
                """,
                (set_id, first, _links_json(missing)),
            )
            written += int(cur.rowcount)
    return written


def write_node_documents(
    db: Any,
    node_rows: Sequence[Mapping[str, Any]],
    *,
    catalog_version_id: Any = None,
    timings: dict[str, float] | None = None,
) -> int:
    """Link each written node row to its document, for every registered set.

    Called by the graph writer in the node-upsert path of its own write
    transaction, with the rows it just stored (``node_id``, ``subtype``,
    ``attrs``). ``row_hash`` is computed by PostgreSQL from the same
    ``subtype`` and ``attrs`` values the row stores. Nodes a set does not
    cover (another subtype, or no text) get no link. Returns links written.

    Every call that writes a covered row moves the set's link counter, even
    when each of its links is already stored (a node set back to content it
    had before): such a call takes one number with no link. The worker's
    fill carries a completion forward only while that counter is unchanged
    (:func:`okg.substrate.embeddings.backfill._carry_completion_forward`),
    and the content restored may never have been embedded (review P9 SF1).
    The rows reaching here passed the graph writer's no-op filter, so a
    re-write of the same row (same subtype, attributes, source and source
    record) does not get here and takes no number. A write the filter
    passes with the same attributes still takes one: a change of owner, or
    a delete and re-upsert of the node in one batch (the filter skips
    deleted ids).
    """
    if not node_rows:
        return 0
    conn = _psycopg_connection(db)
    if conn is None:
        return 0
    sets, stale = load_registered_sets(conn)
    if timings is not None and stale:
        timings["embedding_documents.stale_sets"] = float(len(stale))
    if not sets:
        return 0
    classifications = load_classifications(
        conn,
        {str(row["subtype"]) for row in node_rows},
        catalog_version_id=catalog_version_id,
    )
    written = 0
    omitted = 0
    # Render per set (local, free); hash each covered row's content once.
    rendered_by_set: list[list[tuple[int, Any]]] = []
    covered: dict[int, None] = {}
    for item in sets:
        per_set: list[tuple[int, Any]] = []
        for position, row in enumerate(node_rows):
            subtype = str(row["subtype"])
            rendered = render_document(
                item, subtype=subtype, attrs=row.get("attrs") or {},
                classification=classifications.get(subtype),
            )
            if rendered is None:
                continue
            omitted += len(rendered.omitted_fields)
            per_set.append((position, rendered))
            covered.setdefault(position, None)
        rendered_by_set.append(per_set)
    if not covered:
        return 0
    positions = list(covered)
    hashes = dict(zip(positions, row_hashes(conn, [
        (str(node_rows[p]["subtype"]), json.dumps(node_rows[p].get("attrs") or {}))
        for p in positions
    ])))
    reused_only = 0
    for item, per_set in zip(sets, rendered_by_set):
        if not per_set:
            continue
        record_templates(conn, item.set_id, {
            rendered.template_hash: rendered.template_record
            for _position, rendered in per_set
        })
        taken = link_documents(conn, item.set_id, [
            (
                rendered.template_hash,
                str(node_rows[position]["node_id"]),
                hashes[position],
                rendered.doc_hash,
            )
            for position, rendered in per_set
        ])
        if taken == 0:
            # Every link was already stored: one number for the whole call,
            # so the worker's carry check still sees a covered row change.
            take_seq(conn, item.set_id, 1)
            reused_only += 1
        written += taken
    if timings is not None:
        timings["embedding_documents.links_written"] = float(written)
        if reused_only:
            timings["embedding_documents.reused_link_ticks"] = float(reused_only)
        if omitted:
            timings["embedding_documents.header_fields_omitted"] = float(omitted)
    return written


def mark_node_rows_restored(db: Any) -> int:
    """Take one link number for every active vector set; returns how many.

    For writers that store node rows without the graph writer's
    :func:`write_node_documents`: a run undo
    (:func:`okg.substrate.ingest.compensation.undo_run_to_run_base`), a
    branch restored to a generation
    (``ChronosGraphGenerationLifecycleMixin._revert_branch_head_to_checkpoint``),
    and a branch merge that lands node rows on its target
    (``ChronosGraphReadBranchMixin._merge_apply_settling_ranked_edges``).
    The rows they restore or copy may never have been embedded, or never linked, so
    the worker's fill must not carry its last completion forward
    (:func:`okg.substrate.embeddings.backfill._carry_completion_forward`).
    One number per operation, not per row, taken inside the caller's
    transaction, in set-id order (the graph writer's order). Takes none on
    a store that is not PostgreSQL, when the vector-set tables are absent
    (a deployment without embeddings), or when no set is active.
    """
    conn = _psycopg_connection(db)
    if conn is None or not _tables_present(conn):
        return 0
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT set_id FROM okg.embedding_sets WHERE active ORDER BY set_id"
        )
        set_ids = [str(row[0]) for row in cur.fetchall()]
    for set_id in set_ids:
        take_seq(conn, set_id, 1)
    return len(set_ids)


def _psycopg_connection(db: Any) -> psycopg.Connection | None:
    """The psycopg connection under a Chronos adapter, or None for a store
    that is not PostgreSQL (the SQLite test adapter)."""
    if isinstance(db, psycopg.Connection):
        return db
    if getattr(db, "dialect", None) != "postgres":
        return None
    conn = getattr(db, "raw_connection", None)
    if not isinstance(conn, psycopg.Connection):
        raise TypeError(
            f"cannot write document links through {type(db).__qualname__}"
        )
    return conn


# --------------------------------------------------------------------------- #
# Watermarks                                                                   #
# --------------------------------------------------------------------------- #

def record_watermarks(
    conn: psycopg.Connection, generation_id: str, *, recorded_by: str,
) -> int:
    """Record every registered set's committed ``next_seq - 1`` of each
    counter (links: ``max_seq``; vectors: ``max_vector_seq``) for
    ``generation_id``, with the newest completed fill's vector bound
    (``filled_vector_seq``, NULL before any; :func:`record_fill_completion`).
    An existing row is kept. Returns rows written."""
    if recorded_by not in ("publisher", "migrate"):
        raise ValueError(f"recorded_by must be publisher or migrate, not {recorded_by!r}")
    if not _tables_present(conn):
        return 0
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            """
            INSERT INTO okg.embedding_watermarks
              (generation_id, set_id, max_seq, max_vector_seq,
               filled_vector_seq, recorded_by)
            SELECT %s, s.set_id, s.next_seq - 1, v.next_seq - 1,
                   (SELECT max(c.max_vector_seq)
                      FROM okg.embedding_fill_completions AS c
                     WHERE c.set_id = s.set_id),
                   %s
              FROM okg.embedding_sets AS s
              JOIN okg.embedding_vector_counters AS v ON v.set_id = s.set_id
            ON CONFLICT (generation_id, set_id) DO NOTHING
            """,
            (str(generation_id), recorded_by),
        )
        return int(cur.rowcount)


#: ``max_seq`` of a completion that no later generation may carry forward:
#: another writer took link numbers while the fill ran (see
#: :func:`record_fill_completion`).
UNCARRIABLE_FILL_SEQ = -1


def record_fill_completion(
    conn: psycopg.Connection,
    set_id: str,
    generation_id: str,
    *,
    links_written: int | None = None,
) -> bool:
    """Record that a backfill of ``set_id`` at ``generation_id`` completed
    with no document left without a vector. Call it after the run's last
    vector commits. The next watermark carries ``max_vector_seq`` (the
    vector counter's committed value); semantic search routes to the set
    only from such a watermark on. Returns whether a row was written (one
    per set and generation).

    ``max_seq`` is the link counter's committed value only when every link
    number taken since ``generation_id``'s watermark is one of this run's
    ``links_written``. Otherwise another writer took links while the fill
    ran (a sync whose rows a later publish shows, not this pin), and
    ``max_seq`` is :data:`UNCARRIABLE_FILL_SEQ`, so the next generation's
    worker fill runs the full pass instead of carrying this completion
    forward. ``links_written=None`` (unknown) records it uncarriable too.
    """
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            """
            INSERT INTO okg.embedding_fill_completions
              (set_id, generation_id, max_seq, max_vector_seq)
            SELECT s.set_id, %(generation)s,
                   CASE WHEN CAST(%(links)s AS bigint) IS NOT NULL
                         AND w.max_seq IS NOT NULL
                         AND s.next_seq - 1 - w.max_seq = CAST(%(links)s AS bigint)
                        THEN s.next_seq - 1
                        ELSE %(uncarriable)s END,
                   v.next_seq - 1
              FROM okg.embedding_sets AS s
              JOIN okg.embedding_vector_counters AS v ON v.set_id = s.set_id
              LEFT JOIN okg.embedding_watermarks AS w
                ON w.set_id = s.set_id AND w.generation_id = %(generation)s
             WHERE s.set_id = %(set_id)s
            ON CONFLICT (set_id, generation_id) DO NOTHING
            """,
            {
                "generation": str(generation_id),
                "links": None if links_written is None else int(links_written),
                "uncarriable": UNCARRIABLE_FILL_SEQ,
                "set_id": set_id,
            },
        )
        return cur.rowcount == 1


def seq_advanced_since(conn: psycopg.Connection, generation_id: str | None) -> int:
    """Sets with vectors or links written, or a fill completed, since
    ``generation_id``'s watermark.

    A set with no watermark row for that generation counts when anything was
    ever written to it. The publisher refuses to reuse a generation while
    this is above zero, or a backfill would never become readable.
    """
    if generation_id is None or not _tables_present(conn):
        return 0
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.embedding_sets AS s
              JOIN okg.embedding_vector_counters AS v ON v.set_id = s.set_id
              LEFT JOIN okg.embedding_watermarks AS w
                ON w.set_id = s.set_id AND w.generation_id = %s
             WHERE s.next_seq - 1 > COALESCE(w.max_seq, 0)
                OR v.next_seq - 1 > COALESCE(w.max_vector_seq, w.max_seq, 0)
                -- A fill completed since: a backfill that found nothing
                -- missing writes no row, yet its completion must reach a
                -- watermark or the route never moves to the set.
                OR (SELECT max(c.max_vector_seq)
                      FROM okg.embedding_fill_completions AS c
                     WHERE c.set_id = s.set_id) IS DISTINCT FROM w.filled_vector_seq
            """,
            (str(generation_id),),
        )
        return int(cur.fetchone()[0])


class Watermark(NamedTuple):
    """A pin's bounds for one set: the highest link ``seq`` and the highest
    vector ``seq`` it reads."""

    links: int
    vectors: int


def watermark_for(
    conn: psycopg.Connection, generation_id: str, set_id: str,
) -> Watermark:
    """The pin's watermark for one set, or a refusal; never "all vectors".

    A row recorded while links and vectors shared one counter has no
    ``max_vector_seq``; its ``max_seq`` bounds both (every vector written
    since has a higher number: the vector counter started at the shared
    counter's value)."""
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            "SELECT max_seq, COALESCE(max_vector_seq, max_seq) "
            "FROM okg.embedding_watermarks "
            "WHERE generation_id = %s AND set_id = %s",
            (str(generation_id), set_id),
        )
        row = cur.fetchone()
    if row is None:
        raise _error(
            "the pinned generation has no vector watermark for this set",
            code="embedding_watermark_missing",
            data={"generation_id": str(generation_id), "set_id": set_id},
            fix_hint="publish once more so the generation records its watermark",
        )
    return Watermark(links=int(row[0]), vectors=int(row[1]))


def node_document_at_pin(
    conn: psycopg.Connection,
    *,
    vector_set: VectorSetConfig,
    generation_id: str,
    node_id: str,
) -> dict[str, Any] | None:
    """The document and vector a node has at a pinned generation.

    Keeps a link only if the node's visible row at the pin hashes to the
    link's ``row_hash`` and both link and vector are at or below the pin's
    watermark. Returns None when the node is not visible; otherwise
    ``{"doc_hash": ... or None, "has_vector": bool}``.
    """
    from types import SimpleNamespace

    from okg.substrate.bm25_node_search import branch_point
    from okg.substrate.graph_store_internal.common import quote_physical_table
    from okg.substrate.semantic_ann import _node_segment

    mark = watermark_for(conn, generation_id, vector_set.set_id)
    bp = branch_point(conn, SimpleNamespace(generation_id=generation_id))
    physical = quote_physical_table(_node_segment(conn))
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            f"""
            SELECT n.subtype, n.attrs, {row_hash_expression('n')} AS row_hash
              FROM {physical} AS n
             WHERE n.node_id = %s AND n.deleted = FALSE
               AND n.live_lo <= %s AND %s < n.live_hi
            """,
            (node_id, bp, bp),
        )
        visible = cur.fetchone()
    if visible is None:
        return None
    subtype, attrs, row_hash = visible
    classification = load_classifications(conn, [str(subtype)])[str(subtype)]
    rendered = render_document(
        vector_set, subtype=str(subtype), attrs=attrs or {},
        classification=classification,
    )
    if rendered is None:
        return {"doc_hash": None, "has_vector": False}
    with conn.cursor(row_factory=tuple_row) as cur:
        cur.execute(
            """
            SELECT d.doc_hash, e.doc_hash IS NOT NULL
              FROM okg.node_documents AS d
              LEFT JOIN okg.text_embeddings AS e
                ON e.set_id = d.set_id AND e.doc_hash = d.doc_hash
               AND e.seq <= %s
             WHERE d.set_id = %s AND d.template_hash = %s
               AND d.node_id = %s AND d.row_hash = %s AND d.seq <= %s
             ORDER BY d.seq DESC
             LIMIT 1
            """,
            (
                mark.vectors, vector_set.set_id, rendered.template_hash,
                node_id, row_hash, mark.links,
            ),
        )
        link = cur.fetchone()
    if link is None:
        return {"doc_hash": None, "has_vector": False}
    return {"doc_hash": str(link[0]), "has_vector": bool(link[1])}


# --------------------------------------------------------------------------- #
# Indexes                                                                      #
# --------------------------------------------------------------------------- #

def ensure_set_index(dsn: str, vector_set: VectorSetConfig) -> dict[str, Any]:
    """Build the set's partial HNSW index when it is missing.

    ``CREATE INDEX CONCURRENTLY`` on an autocommit connection, so writers are
    not blocked. An invalid build under the same name is dropped and rebuilt.
    Another index of this set built with other settings is left alone and
    reported: it keeps serving reads while this build runs, and ``okg db
    embed-index`` drops it once this one is valid.
    """
    from okg.substrate.db.connection import connect as db_connect

    name = set_index_name(vector_set)
    report: dict[str, Any] = {
        "set": vector_set.name,
        "set_id": vector_set.set_id,
        "kind": vector_set.index.kind,
    }
    if vector_set.index.kind == "none":
        report["status"] = "not_indexed"
        return report
    report["index"] = name
    with db_connect(dsn, autocommit=True) as conn:
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                """
                SELECT c.relname, i.indisvalid
                  FROM pg_index i
                  JOIN pg_class c ON c.oid = i.indexrelid
                  JOIN pg_namespace ns ON ns.oid = c.relnamespace
                 WHERE ns.nspname = 'okg'
                   AND c.relname LIKE %s
                """,
                (f"text_embeddings_{vector_set.set_id[3:]}_%",),
            )
            existing = {str(row[0]): bool(row[1]) for row in cur.fetchall()}
            others = sorted(other for other in existing if other != name)
            if others:
                report["other_indexes"] = others
            if existing.get(name) is True:
                report["status"] = "present"
                report["action"] = "none"
                return report
            if name in existing:
                cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS okg.{name}")
                report["dropped_invalid"] = True
            # The set's own column, type and operator class (PACT
            # vector-set-pgvector-widths): halfvec on `embedding`, vector on
            # `embedding_f32`.
            cur.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} "
                "ON okg.text_embeddings USING hnsw "
                f"((CAST({vector_set.vector_column} AS {vector_set.vector_type}))"
                f" {vector_set.cosine_opclass}) "
                f"WITH (m = {int(vector_set.index.m)}, "
                f"ef_construction = {int(vector_set.index.ef_construction)}) "
                f"WHERE set_id = '{vector_set.set_id}'"
            )
    report["status"] = "present"
    report["action"] = "build"
    return report


def index_lock_key(set_id: str) -> str:
    """The text whose ``hashtextextended`` is the set's index-rebuild lock."""
    return f"okg.embed_index:{set_id}"


def rebuild_set_index(dsn: str, vector_set: VectorSetConfig, *, apply: bool) -> dict[str, Any]:
    """``okg db embed-index``: give a registered set the index its current
    settings name, then drop its other indexes.

    The set's index is named from its build settings
    (:func:`set_index_name`). ``okg migrate`` registers new settings and
    builds that index with ``CREATE INDEX CONCURRENTLY`` beside the older
    one; this command builds it only when that build is missing or failed.
    Search stays up meanwhile because the read walks any valid index of the
    set (:func:`okg.substrate.semantic_ann.inspect_set_index`) until the new
    one is valid. The older indexes of the set (other build settings, or
    every index after a change to ``kind: none``) are dropped concurrently
    only after the new index is valid. Refuses
    (``embedding_index_build_running``) while another rebuild of the set
    holds its lock or any index build on ``okg.text_embeddings`` is in
    progress. Without ``apply`` it only reports the actions.
    """
    from psycopg import sql

    from okg.substrate.db.connection import connect as db_connect

    name = None if vector_set.index.kind == "none" else set_index_name(vector_set)
    report: dict[str, Any] = {
        "set": vector_set.name,
        "set_id": vector_set.set_id,
        "kind": vector_set.index.kind,
        "index": name,
        "apply": bool(apply),
    }
    with db_connect(dsn, autocommit=True) as conn:
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                (index_lock_key(vector_set.set_id),),
            )
            held = bool(cur.fetchone()[0])
            cur.execute(
                "SELECT pid FROM pg_stat_progress_create_index "
                "WHERE relid = to_regclass('okg.text_embeddings') "
                "AND datname = current_database() AND pid <> pg_backend_pid() "
                "ORDER BY pid"
            )
            building = [int(row[0]) for row in cur.fetchall()]
            if not held or building:
                raise _error(
                    "an index build of this vector set or on okg.text_embeddings "
                    "is running",
                    code="embedding_index_build_running",
                    data={"set_id": vector_set.set_id, "pids": building,
                          "lock_held_elsewhere": not held},
                    fix_hint=(
                        "wait for it to finish (pg_stat_progress_create_index), "
                        "then rerun"
                    ),
                )
            cur.execute(
                """
                SELECT c.relname, i.indisvalid
                  FROM pg_index i
                  JOIN pg_class c ON c.oid = i.indexrelid
                  JOIN pg_namespace ns ON ns.oid = c.relnamespace
                 WHERE ns.nspname = 'okg'
                   AND i.indrelid = to_regclass('okg.text_embeddings')
                   AND starts_with(c.relname, %s)
                 ORDER BY c.relname
                """,
                (f"text_embeddings_{vector_set.set_id[3:]}_",),
            )
            existing = {str(row[0]): bool(row[1]) for row in cur.fetchall()}
            others = [other for other in existing if other != name]
            actions: list[dict[str, str]] = []
            if name is not None:
                if existing.get(name) is True:
                    step = "keep"
                elif name in existing:
                    step = "rebuild_invalid"
                else:
                    step = "build"
                actions.append({"action": step, "index": name})
            actions.extend({"action": "drop", "index": other} for other in others)
            report["actions"] = actions
            if not apply:
                report["status"] = "dry_run"
                return report
            if name is not None and existing.get(name) is not True:
                # Concurrent build on its own autocommit connection; it
                # raises on failure, and then nothing is dropped.
                ensure_set_index(dsn, vector_set)
            dropped: list[str] = []
            for other in others:
                cur.execute(
                    sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(
                        sql.Identifier("okg", other)
                    )
                )
                dropped.append(other)
    report["dropped"] = dropped
    report["status"] = "not_indexed" if name is None else "present"
    return report
