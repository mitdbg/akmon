"""Who declares a shared edge, and whose version the graph stores (item 92a).

Several producers write one edge identity: the code-structure and
code-lsp-resolution sources and the code_reference enricher all write
``calls`` and ``references`` edges keyed by the same ``(src, edge_type, dst)``.
``okg.graph_edges`` keeps one row per edge, so before this module the last
writer won: an incremental publish kept whichever producer wrote last, while a
from-scratch rebuild kept the one that happened to write last there.

For an edge type the deployment ranks (``edge_precedence`` in
``deployment.yaml``), every producer's current version of an edge is kept in
``okg.graph_edge_declarers``, one row per ``(edge_id, source)``, and
``okg.graph_edges`` stores the version of the highest-ranked producer that
currently declares it. The stored row then depends only on who declares the
edge now, not on the order they wrote in. This is the edge counterpart of
``okg.graph_node_declarers`` (issue #1901), and like it the table is a
registered Chronos table, so its rows branch, merge, checkpoint and read
as-of a generation with the graph rows they describe.

The ranking lives in ``deployment.yaml``, but the graph writer is built in
hundreds of places that never see the manifest. ``okg migrate --deployment``
and the runtime worker therefore copy it into the plain table
``okg.edge_precedence`` (:func:`sync_edge_precedence`), and every graph write
reads it from there. An edge type that is not in the table keeps the earlier
path: no declarer rows, last writer wins, source-scoped retraction.

Rules (decided in :func:`reconcile_edge_declarers`, all from rows read inside
the graph write transaction after the branch write fence):

* An insert records or replaces the writing source's declaration; a
  retraction withdraws it. A retraction of an edge the source does not own
  still withdraws its declaration and changes no stored row while another
  producer still declares it.
* The winner is the declaring producer listed first in the type's ranking.
  Two declarers of which one is not ranked is refused, naming both: no tie is
  settled silently.
* No declarer left: the stored row is deleted.
* A node whose row is deleted takes every declaration of its incident ranked
  edges with it; a node a source only retracts withdraws that source's
  declarations of its incident ranked edges (today's source-scoped rule).
* A stored row whose source has no declaration (written before this table
  existed) is adopted as that source's declaration the first time the edge is
  touched, so the transition never drops a row. Declarations made before the
  table existed are otherwise not backfilled: a deployment needs one forced
  full run of every producer of a ranked type (docs/develop/architecture/
  pipeline.md, "Edges several producers write").
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from okg.substrate.errors import SubstrateError
from okg.substrate.fact_equivalence import compare_attrs, edge_columns_differing

EDGE_DECLARERS_TABLE = "okg.graph_edge_declarers"
EDGE_DECLARERS_PK = ["edge_id", "source"]
EDGE_PRECEDENCE_TABLE = "okg.edge_precedence"

CREATE_EDGE_DECLARERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS okg.graph_edge_declarers (
  edge_id           BIGINT      NOT NULL,
  source            TEXT        NOT NULL,
  src               TEXT        NOT NULL,
  dst               TEXT        NOT NULL,
  edge_type         TEXT        NOT NULL,
  attrs             JSONB       NOT NULL DEFAULT '{}'::jsonb,
  provenance        TEXT        NOT NULL DEFAULT 'explicit',
  confidence        REAL        NOT NULL DEFAULT 1.0,
  source_record_id  JSONB       NOT NULL DEFAULT '{}'::jsonb,
  source_revision   JSONB       NOT NULL DEFAULT '{}'::jsonb,
  run_id            TEXT,
  observed_at       TIMESTAMPTZ,
  PRIMARY KEY (edge_id, source)
)
"""

# A plain table, not a Chronos one: it is deployment configuration, not graph
# state, and a ranking change applies to every branch at once.
CREATE_EDGE_PRECEDENCE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS okg.edge_precedence (
  edge_type  TEXT    NOT NULL,
  source     TEXT    NOT NULL,
  rank       INTEGER NOT NULL,
  PRIMARY KEY (edge_type, source)
)
"""

_DECLARATION_COLUMNS = (
    "edge_id",
    "source",
    "src",
    "dst",
    "edge_type",
    "attrs",
    "provenance",
    "confidence",
    "source_record_id",
    "source_revision",
    "run_id",
    "observed_at",
)

# A declaration carries exactly the graph edge row's columns.
_EDGE_ROW_COLUMNS = _DECLARATION_COLUMNS

_SELECT_DECLARATIONS_SQL = f"""
SELECT {", ".join(_DECLARATION_COLUMNS)}
  FROM {EDGE_DECLARERS_TABLE}
 WHERE edge_id = ANY(:edge_ids)
"""


class EdgePrecedenceUnranked(SubstrateError):
    """Two producers declare one ranked edge and one of them has no rank."""


class EdgeRankingChangedDuringWrite(SubstrateError):
    """A graph write was planned with a ranking that changed before its
    fence; nothing was written and the caller's transaction rolled back."""

    code = "edge_ranking_changed_during_write"


class EdgeRankingChangeBusy(SubstrateError):
    """A ranking change gave way to graph writers holding a branch lock;
    nothing was changed and the change can be retried."""

    code = "edge_ranking_change_busy"


# --------------------------------------------------------------------------
# Ranking: deployment.yaml -> okg.edge_precedence -> the graph writer.
# --------------------------------------------------------------------------


def parse_edge_precedence(value: Any) -> dict[str, tuple[str, ...]]:
    """Validate a ``deployment.yaml`` ``edge_precedence`` block.

    Shape: ``{edge_type: [highest-ranked producer, ..., lowest]}``. A producer
    is a source-registry name or ``_enricher:<enricher name>``. ``None`` means
    no edge type is ranked. Raises ``ValueError`` naming the bad entry.
    """
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(
            "edge_precedence must map an edge type to a list of producers, "
            f"got {type(value).__name__}"
        )
    out: dict[str, tuple[str, ...]] = {}
    for edge_type, producers in value.items():
        if not isinstance(edge_type, str) or not edge_type.strip():
            raise ValueError(
                f"edge_precedence has an empty or non-string edge type: {edge_type!r}"
            )
        if not isinstance(producers, list) or not producers:
            raise ValueError(
                f"edge_precedence.{edge_type} must be a non-empty list of "
                "producers, highest rank first"
            )
        names: list[str] = []
        for producer in producers:
            if not isinstance(producer, str) or not producer.strip():
                raise ValueError(
                    f"edge_precedence.{edge_type} has an empty or non-string "
                    f"producer: {producer!r}"
                )
            if producer in names:
                raise ValueError(
                    f"edge_precedence.{edge_type} lists {producer!r} twice; "
                    "a producer has one rank"
                )
            names.append(producer)
        out[edge_type.strip()] = tuple(names)
    return out


def sync_deployment_edge_precedence(
    db: Any,
    deployment_name: str,
) -> dict[str, Any] | None:
    """Copy ``deployment_name``'s ranking into ``okg.edge_precedence``.

    Reads the composed manifest every deployment read resolves
    (``resolve_deployment``, which also composes a sealed definition);
    returns ``None`` when no deployment directory of that name resolves, so
    there is no manifest to copy. A malformed ``edge_precedence`` block
    raises.
    """
    from okg.substrate.deployments import find_deployment_dir, resolve_deployment

    from okg.substrate import fastyaml

    if find_deployment_dir(deployment_name) is None:
        return None
    manifest = resolve_deployment(deployment_name)
    raw = manifest.effective_raw
    try:
        ranking = parse_edge_precedence(raw.get("edge_precedence"))
    except ValueError as exc:
        raise SubstrateError(
            "deployment.yaml edge_precedence is invalid",
            data={"deployment": deployment_name, "error": str(exc)},
            fix_hint="fix edge_precedence in deployment.yaml; run okg deployment lint",
        ) from exc
    identities: dict[str, str] = {}
    registry = raw.get("source_registry")
    if ranking and isinstance(registry, str) and registry.strip():
        registry_path = Path(manifest.deployment_dir) / registry
        document = fastyaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}
        sources = document.get("sources") if isinstance(document, Mapping) else None
        for name, body in (sources or {}).items():
            if isinstance(body, Mapping) and body.get("ownership_id"):
                identities[str(name)] = str(body["ownership_id"])
    return sync_edge_precedence(db, ranking, source_identities=identities)


def apply_deployment_edge_precedence(
    db: Any,
    store: Any,
    deployment_name: str,
) -> dict[str, Any] | None:
    """Copy the deployment's ranking and remove the declarations it no longer
    covers, in the caller's one transaction.

    ``okg migrate --deployment`` and worker start both call this. ``db`` and
    ``store`` must share the connection and transaction, so a purge that
    fails (a frozen paired workspace holding stale rows) leaves the ranking
    unchanged too.

    The ranking lock is taken exclusively first: every path that writes
    ranked edges or declarations holds it shared until its commit, so no
    write planned with the old ranking can commit after this one. Some of
    those paths take a branch lock before the ranking lock, so the purge
    only briefly waits for a branch lock and otherwise gives way
    (:class:`EdgeRankingChangeBusy`, nothing changed) instead of
    deadlocking with them. The purge runs
    on every call, not only when the ranking changed; it reads only stale
    rows (an SQL filter), so a re-run of ``okg migrate`` or a worker restart
    also removes stray declarations a fork from an older checkpoint
    carried in.
    """
    _db_execute(
        db,
        "SELECT pg_advisory_xact_lock(hashtext(?), hashtext(?))",
        ("okg.edge_precedence", "ranking"),
    )
    outcome = sync_deployment_edge_precedence(db, deployment_name)
    if outcome is not None:
        outcome = {
            **outcome,
            **store.purge_declarations_outside_ranking(
                deployment_name=deployment_name,
            ),
        }
    return outcome


def _db_execute(db: Any, sql: str, params: Sequence[Any] = ()) -> Any:
    """Run SQL on a Chronos database adapter (``?`` placeholders) or a
    psycopg connection or cursor (``%s``)."""
    import psycopg

    if isinstance(db, (psycopg.Connection, psycopg.Cursor)):
        return db.execute(sql.replace("?", "%s"), tuple(params))
    return db.execute(sql, tuple(params))


def ensure_edge_precedence_table(db: Any) -> None:
    _db_execute(db, CREATE_EDGE_PRECEDENCE_TABLE_SQL)


def sync_edge_precedence(
    db: Any,
    ranking: Mapping[str, Sequence[str]],
    *,
    source_identities: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Make ``okg.edge_precedence`` hold exactly ``ranking``.

    ``source_identities`` maps a source-registry name to its
    ``ownership_id``. A source writes graph rows under its ownership id when
    the ingest service runs it and under its registry name when a bare
    ``SourceRunner`` does, so a ranked registry name is stored under both, at
    the same rank.

    ``db`` is a psycopg connection or a Chronos database adapter; the caller
    owns the transaction. Returns what changed, so callers can log it. A
    ranking change does not rewrite stored rows: the edges a change affects
    are recomputed the next time a producer writes them, so a change needs
    forced full runs of the producers it names (as the first backfill does).
    """
    ensure_edge_precedence_table(db)
    identities = dict(source_identities or {})
    wanted: dict[tuple[str, str], int] = {}
    for edge_type, producers in ranking.items():
        for rank, source in enumerate(producers):
            for name in (str(source), identities.get(str(source))):
                if name:
                    wanted.setdefault((str(edge_type), name), rank)
    current = {
        (str(row["edge_type"]), str(row["source"])): int(row["rank"])
        for row in _rows(_db_execute(
            db, "SELECT edge_type, source, rank FROM okg.edge_precedence",
        ))
    }
    if current == wanted:
        return {"changed": False, "edge_types": sorted(ranking)}
    _db_execute(db, "DELETE FROM okg.edge_precedence")
    for (edge_type, source), rank in sorted(wanted.items()):
        _db_execute(
            db,
            "INSERT INTO okg.edge_precedence (edge_type, source, rank) "
            "VALUES (?, ?, ?)",
            (edge_type, source, rank),
        )
    return {
        "changed": True,
        "edge_types": sorted(ranking),
        "previous_edge_types": sorted({key[0] for key in current}),
    }


def load_edge_precedence(db: Any) -> dict[str, dict[str, int]]:
    """``{edge_type: {source: rank}}`` from ``okg.edge_precedence``.

    Rank 0 is the highest. A database without the table ranks nothing.
    """
    exists = _rows(_db_execute(
        db, "SELECT to_regclass('okg.edge_precedence') IS NOT NULL AS present",
    ))
    if not exists or not exists[0]["present"]:
        return {}
    out: dict[str, dict[str, int]] = {}
    for row in _rows(_db_execute(
        db, "SELECT edge_type, source, rank FROM okg.edge_precedence",
    )):
        out.setdefault(str(row["edge_type"]), {})[str(row["source"])] = int(
            row["rank"]
        )
    return out


def _rows(result: Any) -> list[Mapping[str, Any]]:
    rows = result.fetchall() if hasattr(result, "fetchall") else list(result)
    out: list[Mapping[str, Any]] = []
    description = getattr(result, "description", None)
    for row in rows:
        if isinstance(row, Mapping):
            out.append(row)
        elif hasattr(row, "keys"):
            out.append({key: row[key] for key in row.keys()})
        elif description is not None:
            out.append({
                column.name: value for column, value in zip(description, row)
            })
        else:  # pragma: no cover - every adapter above names its columns
            raise TypeError(f"cannot read named columns from {type(row).__name__}")
    return out


# --------------------------------------------------------------------------
# Declarations and the winner rule.
# --------------------------------------------------------------------------


def _json_object(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def declaration_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """The declarer-table row for one prepared edge upsert (or a stored row)."""
    run_id = row.get("run_id")
    confidence = row.get("confidence")
    return {
        "edge_id": int(row["edge_id"]),
        "source": str(row["source"]),
        "src": str(row["src"]),
        "dst": str(row["dst"]),
        "edge_type": str(row["edge_type"]),
        "attrs": _json_object(row.get("attrs")),
        "provenance": str(row.get("provenance") or "explicit"),
        "confidence": 1.0 if confidence is None else float(confidence),
        "source_record_id": _json_object(row.get("source_record_id")),
        "source_revision": _json_object(row.get("source_revision")),
        "run_id": None if run_id is None else str(run_id),
        "observed_at": row.get("observed_at"),
    }


def edge_row_from_declaration(declaration: Mapping[str, Any]) -> dict[str, Any]:
    """The graph edge row that makes ``declaration``'s producer the owner."""
    return {column: declaration.get(column) for column in _EDGE_ROW_COLUMNS} | {
        "edge_id": int(declaration["edge_id"]),
        "attrs": _json_object(declaration.get("attrs")),
        "source_record_id": _json_object(declaration.get("source_record_id")),
        "source_revision": _json_object(declaration.get("source_revision")),
    }


def same_edge_content(
    new: Mapping[str, Any],
    stored: Mapping[str, Any] | None,
) -> bool:
    """The graph writer's edge no-op rule (issue #2335), owner included.

    Attrs compared with the lineage's source revision ignored, plus endpoints,
    type, owner, record id, provenance and confidence. Run id, observation
    time and the revision column only describe when it was written.
    """
    if stored is None:
        return False
    return (
        str(new.get("src") or "") == str(stored.get("src") or "")
        and str(new.get("dst") or "") == str(stored.get("dst") or "")
        and str(new.get("edge_type") or "") == str(stored.get("edge_type") or "")
        and str(new.get("source") or "") == str(stored.get("source") or "")
        and compare_attrs(
            _json_object(new.get("attrs")), _json_object(stored.get("attrs")),
        ).unchanged
        and _json_object(new.get("source_record_id"))
        == _json_object(stored.get("source_record_id"))
        and not edge_columns_differing(
            new_provenance=new.get("provenance"),
            new_confidence=new.get("confidence"),
            stored_provenance=stored.get("provenance"),
            stored_confidence=stored.get("confidence"),
        )
    )


def choose_winner(
    declarations: Iterable[Mapping[str, Any]],
    *,
    ranking: Mapping[str, int],
    edge_type: str,
) -> Mapping[str, Any] | None:
    """The declaration the graph stores, or ``None`` when nobody declares it.

    The producer with the lowest rank number wins. A lone declarer wins
    whether ranked or not; two declarers of which one is unranked raise
    :class:`EdgePrecedenceUnranked`, because the ranking cannot order them.
    """
    remaining = list(declarations)
    if not remaining:
        return None
    if len(remaining) == 1:
        return remaining[0]
    unranked = sorted(
        str(declaration["source"])
        for declaration in remaining
        if str(declaration["source"]) not in ranking
    )
    if unranked:
        edge_id = remaining[0].get("edge_id")
        raise EdgePrecedenceUnranked(
            "an edge is declared by several producers and the ranking cannot "
            "order them",
            data={
                "edge_id": None if edge_id is None else int(edge_id),
                "edge_type": edge_type,
                "declaring_sources": sorted(
                    str(declaration["source"]) for declaration in remaining
                ),
                "unranked_sources": unranked,
            },
            fix_hint=(
                f"add {', '.join(unranked)} to edge_precedence.{edge_type} in "
                "deployment.yaml, run okg migrate --deployment <name> --apply, "
                "and restart the worker"
            ),
        )
    # A source ranked under both its registry name and its ownership id
    # shares one rank; the name breaks that tie so every replay agrees.
    return min(
        remaining,
        key=lambda declaration: (
            ranking[str(declaration["source"])], str(declaration["source"]),
        ),
    )


@dataclass
class EdgeDeclarerReconciliation:
    """What one graph write does to ranked edges and their declarers."""

    # Graph rows to write: the winner's version of each changed edge.
    edge_upserts: list[dict[str, Any]] = field(default_factory=list)
    # Graph rows to delete: edges nobody declares any more.
    edge_deletes: list[dict[str, Any]] = field(default_factory=list)
    declaration_upserts: list[dict[str, Any]] = field(default_factory=list)
    declaration_deletes: list[dict[str, Any]] = field(default_factory=list)
    # Rows whose source had no declaration and were adopted as one.
    adopted: list[int] = field(default_factory=list)
    # Edges whose winner changed to another producer in this write.
    handoffs: list[int] = field(default_factory=list)


def reconcile_edge_declarers(
    *,
    source: str | None,
    ranks: Mapping[str, Mapping[str, int]],
    upserts: Iterable[Mapping[str, Any]],
    withdrawn_edge_ids: Iterable[int],
    purged_edge_ids: Iterable[int],
    current_edges: Mapping[int, Mapping[str, Any]],
    declarations: Mapping[int, list[Mapping[str, Any]]],
) -> EdgeDeclarerReconciliation:
    """Decide declarer edits and stored rows for one graph write.

    All inputs must be read after the branch write fence:

    * ``upserts``: this write's prepared rows of ranked edge types. With
      ``source=None`` (an unattributed write) a row replaces every
      declaration of its edge and is stored as written.
    * ``withdrawn_edge_ids``: ranked edges this source stops declaring (its
      retractions, and its incident edges of nodes it retracted). With
      ``source=None`` every declaration of them is removed.
    * ``purged_edge_ids``: edges every declaration of which goes (an endpoint
      node's row is deleted by this write).
    * ``current_edges``: the stored graph rows of every touched edge.
    * ``declarations``: every recorded declaration of every touched edge.
    """
    result = EdgeDeclarerReconciliation()
    upsert_rows = {int(row["edge_id"]): row for row in upserts}
    withdrawn = {int(edge_id) for edge_id in withdrawn_edge_ids} - set(upsert_rows)
    purged = {int(edge_id) for edge_id in purged_edge_ids}
    touched = sorted(set(upsert_rows) | withdrawn | purged)
    for edge_id in touched:
        recorded = {
            str(declaration["source"]): dict(declaration)
            for declaration in declarations.get(edge_id, ())
        }
        current = current_edges.get(edge_id)
        final = dict(recorded)
        row = upsert_rows.get(edge_id)
        current_source = (
            None if current is None or current.get("source") is None
            else str(current["source"])
        )
        if edge_id in purged:
            result.declaration_deletes.extend(
                {"edge_id": edge_id, "source": name} for name in sorted(recorded)
            )
            if current is not None:
                result.edge_deletes.append({"edge_id": edge_id})
            continue
        if source is None:
            # An unattributed write overrides every producer: no declarer is
            # left, and the stored row is exactly what it wrote, or nothing.
            result.declaration_deletes.extend(
                {"edge_id": edge_id, "source": name} for name in sorted(recorded)
            )
            if row is not None:
                if not same_edge_content(row, current):
                    result.edge_upserts.append(dict(row))
            elif current is not None:
                result.edge_deletes.append({"edge_id": edge_id})
            continue
        # A stored row a ranked producer wrote before declarers were recorded
        # speaks for its source until that source writes or withdraws the edge
        # itself. A row of a producer the ranking does not name (or of an
        # unattributed write) is an ordinary last-writer-wins row: a ranked
        # write simply replaces it.
        if (
            current is not None
            and current_source is not None
            and current_source not in recorded
            and current_source != source
            and current_source in ranks.get(_edge_type_of(row, current, recorded), {})
        ):
            adopted = declaration_from_row(current)
            final[current_source] = adopted
            result.declaration_upserts.append(adopted)
            result.adopted.append(edge_id)
        if row is not None:
            declaration = declaration_from_row(row)
            previous = recorded.get(source)
            final[source] = declaration
            if not (
                previous is not None
                and same_edge_content(declaration, previous)
            ):
                result.declaration_upserts.append(declaration)
        elif edge_id in withdrawn:
            final.pop(source, None)
            if source in recorded:
                result.declaration_deletes.append(
                    {"edge_id": edge_id, "source": source}
                )
        _settle(
            edge_id,
            final=final,
            current=current,
            edge_type=_edge_type_of(row, current, recorded),
            ranks=ranks,
            result=result,
        )
    return result


def withdraw_declarations(
    *,
    sources: Iterable[str],
    ranks: Mapping[str, Mapping[str, int]],
    current_edges: Mapping[int, Mapping[str, Any]],
    declarations: Mapping[int, list[Mapping[str, Any]]],
    withdrawn: Iterable[tuple[int, str]],
) -> EdgeDeclarerReconciliation:
    """Remove the ``withdrawn`` ``(edge_id, source)`` declarations of
    ``sources`` and store each affected edge's new winner.

    For removals that do not go through a retraction fact: retiring a source,
    or undoing a run that has no run base. A stored row owned by a source
    outside ``sources`` that has no declaration is adopted first, as in
    :func:`reconcile_edge_declarers`.
    """
    leaving = {str(source) for source in sources}
    removed = {(int(edge_id), str(source)) for edge_id, source in withdrawn}
    result = EdgeDeclarerReconciliation()
    for edge_id in sorted({edge_id for edge_id, _source in removed}):
        recorded = {
            str(declaration["source"]): dict(declaration)
            for declaration in declarations.get(edge_id, ())
        }
        current = current_edges.get(edge_id)
        final = {
            name: declaration
            for name, declaration in recorded.items()
            if (edge_id, name) not in removed
        }
        result.declaration_deletes.extend(
            {"edge_id": edge_id, "source": name}
            for name in sorted(recorded)
            if (edge_id, name) in removed
        )
        current_source = (
            None if current is None or current.get("source") is None
            else str(current["source"])
        )
        if (
            current_source is not None
            and current_source not in recorded
            and current_source not in leaving
            and current_source in ranks.get(_edge_type_of(None, current, recorded), {})
        ):
            adopted = declaration_from_row(current)  # type: ignore[arg-type]
            final[current_source] = adopted
            result.declaration_upserts.append(adopted)
            result.adopted.append(edge_id)
        _settle(
            edge_id,
            final=final,
            current=current,
            edge_type=_edge_type_of(None, current, recorded),
            ranks=ranks,
            result=result,
        )
    return result


def settle_declared_edges(
    *,
    ranks: Mapping[str, Mapping[str, int]],
    edge_ids: Iterable[int],
    current_edges: Mapping[int, Mapping[str, Any]],
    declarations: Mapping[int, list[Mapping[str, Any]]],
) -> EdgeDeclarerReconciliation:
    """Store each edge's winner after its declarations changed in bulk.

    For writes that change declarer rows without going through a producer's
    fact (a branch merge, a rollback re-applying declarations, a ranking
    change that drops a producer). ``edge_ids`` must be edges whose
    declarations changed. A stored row whose producer has no declaration is
    not adopted here: the caller changed the declarations on purpose.

    An edge left with no declaration keeps a stored row that no ranked
    producer wrote (an unattributed write, or a producer the ranking does
    not name); only a row of a ranked producer, whose declaration is gone, is
    deleted.
    """
    result = EdgeDeclarerReconciliation()
    for edge_id in sorted({int(edge_id) for edge_id in edge_ids}):
        recorded = {
            str(declaration["source"]): dict(declaration)
            for declaration in declarations.get(edge_id, ())
        }
        current = current_edges.get(edge_id)
        edge_type = _edge_type_of(None, current, recorded)
        if not recorded and (
            current is None
            or str(current.get("source") or "") not in ranks.get(edge_type, {})
        ):
            continue
        _settle(
            edge_id,
            final=recorded,
            current=current,
            edge_type=_edge_type_of(None, current, recorded),
            ranks=ranks,
            result=result,
        )
    return result


def _edge_type_of(
    row: Mapping[str, Any] | None,
    current: Mapping[str, Any] | None,
    recorded: Mapping[str, Mapping[str, Any]],
) -> str:
    for candidate in (row, current, *recorded.values()):
        if candidate is not None and candidate.get("edge_type"):
            return str(candidate["edge_type"])
    return ""


def _settle(
    edge_id: int,
    *,
    final: Mapping[str, Mapping[str, Any]],
    current: Mapping[str, Any] | None,
    edge_type: str,
    ranks: Mapping[str, Mapping[str, int]],
    result: EdgeDeclarerReconciliation,
) -> None:
    """Store the winner of ``final`` for one edge, or delete its row."""
    winner = choose_winner(
        final.values(), ranking=ranks.get(edge_type, {}), edge_type=edge_type,
    )
    if winner is None:
        if current is not None:
            result.edge_deletes.append({"edge_id": edge_id})
        return
    stored = edge_row_from_declaration(winner)
    if not same_edge_content(stored, current):
        result.edge_upserts.append(stored)
        current_source = None if current is None else current.get("source")
        if current_source is not None and str(current_source) != stored["source"]:
            result.handoffs.append(edge_id)


def load_declarations(
    session: Any,
    edge_ids: Iterable[int],
) -> dict[int, list[dict[str, Any]]]:
    """Every recorded declaration of ``edge_ids`` on the session's branch."""
    ids = sorted({int(edge_id) for edge_id in edge_ids})
    if not ids:
        return {}
    out: dict[int, list[dict[str, Any]]] = {}
    for row in session.query(_SELECT_DECLARATIONS_SQL, {"edge_ids": ids}):
        declaration = {column: row[column] for column in _DECLARATION_COLUMNS}
        out.setdefault(int(declaration["edge_id"]), []).append(declaration)
    return out


__all__ = [
    "apply_deployment_edge_precedence",
    "CREATE_EDGE_DECLARERS_TABLE_SQL",
    "CREATE_EDGE_PRECEDENCE_TABLE_SQL",
    "EDGE_DECLARERS_PK",
    "EDGE_DECLARERS_TABLE",
    "EDGE_PRECEDENCE_TABLE",
    "EdgeDeclarerReconciliation",
    "EdgePrecedenceUnranked",
    "EdgeRankingChangeBusy",
    "EdgeRankingChangedDuringWrite",
    "choose_winner",
    "declaration_from_row",
    "edge_row_from_declaration",
    "ensure_edge_precedence_table",
    "load_declarations",
    "load_edge_precedence",
    "parse_edge_precedence",
    "reconcile_edge_declarers",
    "same_edge_content",
    "settle_declared_edges",
    "sync_deployment_edge_precedence",
    "sync_edge_precedence",
    "withdraw_declarations",
]
