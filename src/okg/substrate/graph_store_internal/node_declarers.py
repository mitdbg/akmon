"""Who declares a shared node, and who inherits it (issue #1901, #2574 slice 2).

Several sources may declare one node with ``ownership_mode="substitute"``:
every git-history source of a multi-repository deployment emits the same
``author:<email>`` node. The graph row belongs to one of them; the substitute
rule (``_substitute_would_clobber``) keeps the others from taking it. Before
this module, a retraction by the owner deleted the row even while other
sources still declared it (decision on #1901, 2026-09-22: ownership passes to
a surviving declarer; the row is deleted only when no declarer remains).

``okg.graph_node_declarers`` records every substitute declaration, one row per
``(node_id, source)``, holding the node row that declarer would store. It is a
registered Chronos table, like ``okg.ontology_rollup``, so its rows branch,
merge, checkpoint and read as-of a generation together with the graph rows
they describe. A plain table would lose the declarers on every new branch and
merge-back would then carry the resulting deletes to main.

Limit: a declaration is recorded only when the graph writer sees it, which is
on the declarer's next run that emits the node. Declarations made before this
table existed are not backfilled; until each declarer has emitted the node
once more, an owner's retraction still deletes the row as before.

:func:`reconcile_node_declarers` makes every decision from rows read inside
the graph write transaction, after the branch write fence
(``_lock_branch_write``), so two writers on one branch cannot interleave a
declarer lookup with a delete.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from okg.substrate.fact_equivalence import compare_attrs

NODE_DECLARERS_TABLE = "okg.graph_node_declarers"
NODE_DECLARERS_PK = ["node_id", "source"]

CREATE_NODE_DECLARERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS okg.graph_node_declarers (
  node_id           TEXT        NOT NULL,
  source            TEXT        NOT NULL,
  subtype           TEXT        NOT NULL,
  attrs             JSONB       NOT NULL DEFAULT '{}'::jsonb,
  text_hash         TEXT,
  source_record_id  JSONB       NOT NULL DEFAULT '{}'::jsonb,
  source_revision   JSONB       NOT NULL DEFAULT '{}'::jsonb,
  run_id            TEXT,
  observed_at       TIMESTAMPTZ,
  PRIMARY KEY (node_id, source)
)
"""

_DECLARATION_COLUMNS = (
    "node_id",
    "source",
    "subtype",
    "attrs",
    "text_hash",
    "source_record_id",
    "source_revision",
    "run_id",
    "observed_at",
)

_SELECT_DECLARATIONS_SQL = f"""
SELECT {", ".join(_DECLARATION_COLUMNS)}
  FROM {NODE_DECLARERS_TABLE}
 WHERE node_id = ANY(:node_ids)
"""


def _json_object(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def declaration_from_upsert(row: Mapping[str, Any]) -> dict[str, Any]:
    """The declarer-table row for one prepared node upsert.

    ``row`` is a node upsert as the graph writer builds it: attrs already carry
    the searchable text and the declaring source's lineage.
    """
    run_id = row.get("run_id")
    return {
        "node_id": str(row["node_id"]),
        "source": str(row["source"]),
        "subtype": str(row["subtype"]),
        "attrs": _json_object(row.get("attrs")),
        "text_hash": row.get("text_hash"),
        "source_record_id": _json_object(row.get("source_record_id")),
        "source_revision": _json_object(row.get("source_revision")),
        "run_id": None if run_id is None else str(run_id),
        "observed_at": row.get("observed_at"),
    }


def node_row_from_declaration(declaration: Mapping[str, Any]) -> dict[str, Any]:
    """The graph node row that makes ``declaration``'s source the owner."""
    return {
        "node_id": str(declaration["node_id"]),
        "subtype": str(declaration["subtype"]),
        "attrs": _json_object(declaration.get("attrs")),
        "text_hash": declaration.get("text_hash"),
        "embedding": None,
        "embedding_model": None,
        "source": str(declaration["source"]),
        "source_record_id": _json_object(declaration.get("source_record_id")),
        "source_revision": _json_object(declaration.get("source_revision")),
        "run_id": declaration.get("run_id"),
        "observed_at": declaration.get("observed_at"),
    }


def declaration_unchanged(
    new: Mapping[str, Any],
    recorded: Mapping[str, Any] | None,
) -> bool:
    """Whether recording ``new`` would store nothing new.

    The graph writer's own no-op rule: attrs compared with the lineage's
    source revision ignored, plus subtype and record id. A replay of unchanged
    content therefore writes no declarer version.
    """
    if recorded is None:
        return False
    return (
        str(new.get("subtype") or "") == str(recorded.get("subtype") or "")
        and compare_attrs(
            _json_object(new.get("attrs")),
            _json_object(recorded.get("attrs")),
        ).unchanged
        and _json_object(new.get("source_record_id"))
        == _json_object(recorded.get("source_record_id"))
    )


def choose_survivor(
    declarations: Iterable[Mapping[str, Any]],
    *,
    excluding: set[str],
) -> Mapping[str, Any] | None:
    """The declaration that inherits a node, or ``None`` when none remains.

    The lowest source name wins. It depends only on who declares the node, so
    every replay of the same history picks the same owner.
    """
    remaining = [
        declaration
        for declaration in declarations
        if str(declaration["source"]) not in excluding
    ]
    if not remaining:
        return None
    return min(remaining, key=lambda declaration: str(declaration["source"]))


@dataclass
class DeclarerReconciliation:
    """What one graph write does to shared nodes and their declarers."""

    # Node keys to delete: rows this source owns that nobody else declares.
    node_deletes: list[dict[str, Any]] = field(default_factory=list)
    # Node rows to write instead of a delete: the surviving declarer's row.
    handoffs: list[dict[str, Any]] = field(default_factory=list)
    declaration_upserts: list[dict[str, Any]] = field(default_factory=list)
    declaration_deletes: list[dict[str, Any]] = field(default_factory=list)
    # Nodes deleted because every remaining declaration names a subtype
    # other than the live row's.
    refused_subtype: list[str] = field(default_factory=list)


def load_declarations(
    session: Any,
    node_ids: Iterable[str],
) -> dict[str, list[dict[str, Any]]]:
    """Every recorded declaration of ``node_ids`` on the session's branch."""
    ids = sorted({str(node_id) for node_id in node_ids})
    if not ids:
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for row in session.query(_SELECT_DECLARATIONS_SQL, {"node_ids": ids}):
        declaration = {column: row[column] for column in _DECLARATION_COLUMNS}
        out.setdefault(str(declaration["node_id"]), []).append(declaration)
    return out


def reconcile_node_declarers(
    *,
    source: str | None,
    retracted_node_ids: Iterable[str],
    still_declared_node_ids: Iterable[str],
    substitute_rows: Iterable[Mapping[str, Any]],
    current_nodes: Mapping[str, Mapping[str, Any]],
    declarations: Mapping[str, list[Mapping[str, Any]]],
    purge: bool = False,
) -> DeclarerReconciliation:
    """Decide deletes, hand-offs and declarer edits for one graph write.

    All inputs must be read after the branch write fence:

    * ``retracted_node_ids``: nodes this write retracts.
    * ``still_declared_node_ids``: nodes this same write also upserts. The
      source still declares them, so their retraction is a same-write
      replacement that the writer already coalesces; they are left alone.
    * ``substitute_rows``: this write's prepared substitute node upserts,
      including those the substitute rule will refuse.
    * ``current_nodes``: the current graph rows of the retracted nodes.
    * ``declarations``: every recorded declaration of those nodes.

    A retraction withdraws the source's own declaration. When the source owns
    the row, the row passes to the surviving declarer with the lowest source
    name among those whose stored copy has the live row's subtype, or is
    deleted when none remains (listed in ``refused_subtype`` when declarers
    remain but none matches). A retraction by a source that does
    not own the row changes no graph row. An unattributed retraction
    (``source=None``) deletes the row and every declaration of it.

    ``purge`` is for a caller that removes the node itself rather than one
    source's claim on it (entity merge retires the losing node): every
    declaration of the node is deleted and nothing is handed off, so the row
    is deleted when this source owns it (or the write is unattributed).
    """
    result = DeclarerReconciliation()
    still_declared = {str(node_id) for node_id in still_declared_node_ids}
    retracted = sorted(
        {str(node_id) for node_id in retracted_node_ids} - still_declared
    )
    for node_id in retracted:
        node_declarations = list(declarations.get(node_id, ()))
        if source is None or purge:
            result.declaration_deletes.extend(
                {"node_id": node_id, "source": str(declaration["source"])}
                for declaration in node_declarations
            )
        elif any(
            str(declaration["source"]) == source
            for declaration in node_declarations
        ):
            result.declaration_deletes.append(
                {"node_id": node_id, "source": source}
            )
        current = current_nodes.get(node_id)
        if current is None:
            continue
        if source is None:
            result.node_deletes.append({"node_id": node_id})
            continue
        if current.get("source") != source:
            continue
        if purge:
            result.node_deletes.append({"node_id": node_id})
            continue
        # Only a copy under the live row's subtype may inherit it: the live
        # row is the one the catalog last admitted, and a stored copy under
        # another subtype may name one it no longer allows.
        live_subtype = str(current.get("subtype") or "")
        others = [
            declaration
            for declaration in node_declarations
            if str(declaration["source"]) != source
        ]
        survivor = choose_survivor(
            [
                declaration
                for declaration in others
                if str(declaration.get("subtype") or "") == live_subtype
            ],
            excluding={source},
        )
        if survivor is None:
            result.node_deletes.append({"node_id": node_id})
            if others:
                result.refused_subtype.append(node_id)
        else:
            result.handoffs.append(node_row_from_declaration(survivor))
    if source is not None:
        for row in substitute_rows:
            if str(row.get("source") or "") != source:
                continue
            declaration = declaration_from_upsert(row)
            recorded = next(
                (
                    existing
                    for existing in declarations.get(declaration["node_id"], ())
                    if str(existing["source"]) == source
                ),
                None,
            )
            if not declaration_unchanged(declaration, recorded):
                result.declaration_upserts.append(declaration)
    return result


__all__ = [
    "CREATE_NODE_DECLARERS_TABLE_SQL",
    "DeclarerReconciliation",
    "NODE_DECLARERS_PK",
    "NODE_DECLARERS_TABLE",
    "choose_survivor",
    "declaration_from_upsert",
    "declaration_unchanged",
    "load_declarations",
    "node_row_from_declaration",
    "reconcile_node_declarers",
]
