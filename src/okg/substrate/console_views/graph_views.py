"""The console's Graph reads: the type map and which sources link to which.

PACT console-graph-views. The type map and source links live here; one
record's neighbourhood is in ``graph_record.py`` and the ontology read in
``graph_ontology.py``.

* ``compose_graph_types`` reads only the two maintained rollups (the console
  count rollup and the ontology rollup) as of the pinned generation. It never
  scans a node or an edge. An unmaintained rollup is refused by name
  (``GraphReadRefused``), never answered with zeros.
* ``compose_source_links`` has no maintained rollup behind it: it joins every
  edge to both of its records on request, under a statement timeout, and keeps
  the answer in this process for the generation it was computed for. A timeout
  is refused by name with the elapsed time.

Both are aggregates over every record, counted at publish or on request before
any access rule, and say so in ``count_basis``; their routes authorize each read
at the operator boundary before it runs.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any

from okg.substrate.console_views._chronos import checkout
from okg.substrate.console_views.console_count_rollup_read import (
    ConsoleCountRollupUnavailable,
    read_console_count_rollup,
)
from okg.substrate.console_views.ontology_rollup_console import COUNT_BASIS
from okg.substrate.console_views.ontology_rollup_read import (
    OntologyRollupUnavailable,
    read_ontology_rollup,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    COUNT_KIND_EDGE_SOURCE,
    COUNT_KIND_EDGE_TYPE,
    COUNT_KIND_SOURCE,
    COUNT_KIND_SOURCE_SUBTYPE,
    COUNT_KIND_SUBTYPE,
    ENRICHER_PREFIX,
    PUBLISHER_DERIVED_SOURCE,
    UNATTRIBUTED_DERIVED_PRODUCER,
    parse_source_subtype,
)
from okg.substrate.mcp.session import Session

#: The source-links join's statement timeout (measured 8-14 s on okg-workspace).
SOURCE_LINKS_TIMEOUT_MS = 18_000
#: Generations whose source links this process keeps.
SOURCE_LINKS_CACHE_SIZE = 4

#: Producer keys the publisher stamps for view output. Everything else that is
#: not an enricher is a source connector.
_VIEW_PRODUCERS = frozenset({"catalog:views_registry", "_system:native_views"})
_VIEW_PREFIX = "view:"


class GraphReadRefused(Exception):
    """A Graph read that cannot answer, with the HTTP status and body to refuse with.

    ``detail`` is built only from fixed sentences and values this module
    chose, never from an exception's text, so the route may serve it as is.
    """

    def __init__(self, status: int, reason: str, error: str, **extra: Any) -> None:
        super().__init__(reason)
        self.status = status
        self.detail = {"reason": reason, "error": error, **extra}


def producer_kind(producer: str) -> str:
    """``source``, ``enricher`` or ``view`` for a producer key."""
    if (
        producer.startswith(ENRICHER_PREFIX)
        or producer == PUBLISHER_DERIVED_SOURCE
        or producer == UNATTRIBUTED_DERIVED_PRODUCER
    ):
        return "enricher"
    if producer in _VIEW_PRODUCERS or producer.startswith(_VIEW_PREFIX):
        return "view"
    return "source"


def _count_rollup_refusal(exc: ConsoleCountRollupUnavailable, generation_id: Any) -> GraphReadRefused:
    return GraphReadRefused(
        503,
        "count_rollup_unavailable",
        "The maintained count rollup is not available for this generation.",
        rollup="console_count_rollup",
        rollup_reason=str(exc.reason),
        generation_id=generation_id,
    )


def _ontology_rollup_refusal(exc: OntologyRollupUnavailable, generation_id: Any) -> GraphReadRefused:
    return GraphReadRefused(
        503,
        "ontology_rollup_unavailable",
        "The maintained ontology rollup is not available for this generation.",
        rollup="ontology_rollup",
        rollup_reason=str(exc.reason),
        generation_id=generation_id,
    )


def read_counts(conn: Any, session: Session, kinds: set[str]) -> dict[tuple[str, str], int]:
    """The count rollup at the pinned generation, or the named 503."""
    try:
        return read_console_count_rollup(
            conn, generation_id=session.generation_id, kinds=kinds,
        )
    except ConsoleCountRollupUnavailable as exc:
        raise _count_rollup_refusal(exc, session.generation_id) from exc


def read_rollup(conn: Any, session: Session):
    """The ontology rollup at the pinned generation, or the named 503."""
    try:
        return read_ontology_rollup(
            conn,
            deployment_name=session.deployment_name,
            generation_id=session.generation_id,
        )
    except OntologyRollupUnavailable as exc:
        raise _ontology_rollup_refusal(exc, session.generation_id) from exc


def _largest_first(pairs: dict[str, int], key: str) -> list[dict[str, Any]]:
    return [
        {key: name, "count": count}
        for name, count in sorted(pairs.items(), key=lambda kv: (-kv[1], kv[0]))
    ]


def compose_graph_types(conn: Any, *, session: Session, deployment_name: str) -> dict[str, Any]:
    """Record types, relationship types, producers and type patterns, from rollups only."""
    counts = read_counts(conn, session, {
        COUNT_KIND_SUBTYPE,
        COUNT_KIND_EDGE_TYPE,
        COUNT_KIND_SOURCE,
        COUNT_KIND_EDGE_SOURCE,
        COUNT_KIND_SOURCE_SUBTYPE,
    })
    rollup = read_rollup(conn, session)
    by_kind: dict[str, dict[str, int]] = {}
    for (kind, value), count in counts.items():
        by_kind.setdefault(kind, {})[value] = int(count)
    types = _largest_first(by_kind.get(COUNT_KIND_SUBTYPE, {}), "type")
    relationship_types = _largest_first(by_kind.get(COUNT_KIND_EDGE_TYPE, {}), "type")
    node_sources = by_kind.get(COUNT_KIND_SOURCE, {})
    edge_sources = by_kind.get(COUNT_KIND_EDGE_SOURCE, {})
    sources = [
        {
            "source": name,
            "kind": producer_kind(name),
            "records": node_sources.get(name, 0),
            "relationships": edge_sources.get(name, 0),
        }
        for name in set(node_sources) | set(edge_sources)
    ]
    sources.sort(key=lambda row: (-(row["records"] + row["relationships"]), row["source"]))
    records_by_source = []
    for value, count in by_kind.get(COUNT_KIND_SOURCE_SUBTYPE, {}).items():
        source, subtype = parse_source_subtype(value)
        records_by_source.append({"source": source, "type": subtype, "count": count})
    records_by_source.sort(key=lambda row: (-row["count"], row["source"], row["type"]))
    patterns: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rollup.rows:
        key = (row.src_subtype, row.edge_type, row.dst_subtype)
        entry = patterns.setdefault(key, {
            "from": row.src_subtype,
            "relationship": row.edge_type,
            "to": row.dst_subtype,
            "count": 0,
            "producers": [],
        })
        entry["count"] += int(row.edge_count)
        entry["producers"].append({"producer": row.producer, "count": int(row.edge_count)})
    for entry in patterns.values():
        entry["producers"].sort(key=lambda p: (-p["count"], p["producer"]))
    return {
        "deployment": deployment_name,
        "generation_id": session.generation_id,
        "count_basis": COUNT_BASIS,
        "totals": {
            "records": sum(row["count"] for row in types),
            "relationships": sum(row["count"] for row in relationship_types),
        },
        "types": types,
        "relationship_types": relationship_types,
        "sources": sources,
        "records_by_source": records_by_source,
        "patterns": sorted(
            patterns.values(),
            key=lambda p: (-p["count"], p["from"], p["relationship"], p["to"]),
        ),
    }


# --------------------------------------------------------------------------- #
# source links                                                                 #
# --------------------------------------------------------------------------- #
# An edge whose record is absent from the version joins nothing and is not
# counted; a record with no recorded source is counted under a null source.
# derivation-read-lint: allow PACT console-graph-views no rollup yet; timed out, cached per generation.
_SOURCE_LINKS_SQL = """
SELECT s.source AS from_source, d.source AS to_source, e.source AS producer,
       count(*) AS n
  FROM okg.graph_edges e
  JOIN okg.graph_nodes s ON s.node_id = e.src
  JOIN okg.graph_nodes d ON d.node_id = e.dst
 GROUP BY 1, 2, 3
"""


@dataclass
class _SourceLinksCache:
    lock: threading.Lock = field(default_factory=threading.Lock)
    entries: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)

    def clear(self) -> None:
        with self.lock:
            self.entries.clear()


SOURCE_LINKS_CACHE = _SourceLinksCache()


def _is_statement_timeout(exc: BaseException) -> bool:
    import psycopg

    seen: BaseException | None = exc
    while seen is not None:
        if isinstance(seen, psycopg.errors.QueryCanceled):
            return True
        seen = seen.__cause__ or seen.__context__
    return False


def _with_statement_timeout(conn: Any, timeout_ms: int, run: Any) -> Any:
    """Run ``run()`` under a statement timeout, restoring the prior setting."""
    prior = conn.execute("SHOW statement_timeout").fetchone()
    prior_value = prior[0] if not isinstance(prior, dict) else prior["statement_timeout"]
    conn.execute(f"SET statement_timeout = {int(timeout_ms)}")
    try:
        return run()
    finally:
        if not getattr(conn, "autocommit", False):
            conn.rollback()
        conn.execute("SELECT set_config('statement_timeout', %s, false)", (str(prior_value),))


def compose_source_links(
    conn: Any,
    *,
    session: Session,
    deployment_name: str,
    timeout_ms: int | None = None,
) -> dict[str, Any]:
    """Relationships counted by (source of one end, source of the other, producer)."""
    if timeout_ms is None:
        timeout_ms = SOURCE_LINKS_TIMEOUT_MS
    key =(deployment_name, str(session.generation_id))
    # One computation per generation at a time: a second reader waits for the
    # first and is answered from the cache rather than running the join again.
    with SOURCE_LINKS_CACHE.lock:
        cached = SOURCE_LINKS_CACHE.entries.get(key)
        if cached is not None:
            return {**cached, "cached": True}
        graph = checkout(conn, session)
        started = time.perf_counter()
        try:
            rows = _with_statement_timeout(
                conn, timeout_ms, lambda: graph.query(_SOURCE_LINKS_SQL, {}),
            )
        except Exception as exc:
            if not _is_statement_timeout(exc):
                raise
            raise GraphReadRefused(
                503,
                "source_links_unavailable",
                "Counting which sources link to which took longer than the read allows.",
                cause="timeout",
                elapsed_ms=round((time.perf_counter() - started) * 1000),
                timeout_ms=timeout_ms,
                generation_id=session.generation_id,
            ) from exc
        computed_ms = round((time.perf_counter() - started) * 1000)
        result = {
            "deployment": deployment_name,
            "generation_id": session.generation_id,
            "basis": "computed_on_request",
            "count_basis": COUNT_BASIS,
            "computed_ms": computed_ms,
            "rows": sorted(
                (
                    {
                        "from_source": row["from_source"],
                        "to_source": row["to_source"],
                        "producer": row["producer"],
                        "count": int(row["n"]),
                    }
                    for row in rows
                ),
                key=lambda r: (-r["count"], str(r["from_source"]), str(r["to_source"]), str(r["producer"])),
            ),
        }
        SOURCE_LINKS_CACHE.entries[key] = result
        while len(SOURCE_LINKS_CACHE.entries) > SOURCE_LINKS_CACHE_SIZE:
            SOURCE_LINKS_CACHE.entries.pop(next(iter(SOURCE_LINKS_CACHE.entries)))
    return {**result, "cached": False}


__all__ = [
    "GraphReadRefused",
    "SOURCE_LINKS_CACHE",
    "compose_graph_types",
    "compose_source_links",
    "producer_kind",
    "read_counts",
    "read_rollup",
]
