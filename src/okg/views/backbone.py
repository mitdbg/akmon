"""Substrate-native backbone adapter for the views framework.

This module is the single bridge between the views layer and the
Postgres substrate. It does ONE thing: read selected nodes (and
optionally edges) at a generation pin, project them through
deployment-supplied :class:`AnchorBuilder` callables, and return a
``dict[str, Anchor]`` for the framework's
:func:`okg.views.refresh.refresh` to consume.

It deliberately does not:

* Open or pool connections — the caller passes a
  ``psycopg.Connection`` and owns its lifecycle. This matches
  ``okg.substrate.views.Materializer.materialize`` (the
  publish-cycle adapter from ``viz-materialized-view-nodes``)
  which already takes a connection.
* Accept a snapshot-JSON dispatch path — test fixtures build
  ``dict[str, Anchor]`` directly without going through this
  module. The adapter has one job; alternatives belong elsewhere.
* Define a parallel SQL idiom for generation pinning — it reads
  ``okg.graph_nodes`` / ``okg.graph_edges`` through a Chronos
  checkout pinned to the generation, so visibility is injected
  by the checkout rather than by per-table window predicates.

The corpus-memoization layer keys on ``(generation_id,
selectors_key)`` and uses ``functools.lru_cache``. Stage 0 of
``add-incremental-views`` measured the prototype's backbone load
at 95% of fresh-generation latency; the cache eliminates
repeated re-reads when refresh runs against a stable generation
pin within a single process.
"""
from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Callable, Protocol, runtime_checkable

import psycopg

from okg.views.types import Anchor

# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class BackboneError(Exception):
    """Base class for backbone-adapter failures."""


class NoPublishedGenerationError(BackboneError):
    """Raised when a deployment has no published generation."""


# ---------------------------------------------------------------------------
# NodeRow — what the SQL projection hands to AnchorBuilders
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NodeRow:
    """One row read from ``okg.graph_nodes`` at a pinned generation.

    Frozen so AnchorBuilder authors can use ``NodeRow`` as a
    cache key or pass it through pure functions safely. The
    Chronos checkout exposes one row per ``node_id`` (the table's
    primary key), so each node id appears at most once.
    """

    node_id: str
    subtype: str
    attrs: dict[str, Any]
    text_hash: str | None


# ---------------------------------------------------------------------------
# NodeSelector — compiles to SQL, NOT a Python predicate
# ---------------------------------------------------------------------------


@runtime_checkable
class NodeSelector(Protocol):
    """A scope filter that compiles to a SQL ``WHERE`` fragment.

    ``compile()`` returns ``(where_sql, params)`` where
    ``where_sql`` is a SQL fragment that may reference positional
    placeholders (``%s``) and ``params`` is the corresponding
    list of values. Selectors compose with OR: ``corpus_from_okg``
    builds ``WHERE (sel1) OR (sel2) OR ...``.

    The framework does NOT iterate selector predicates in Python
    over already-fetched rows. Filtering happens in Postgres so
    the existing indexes on ``okg.graph_nodes`` are used.
    """

    def compile(self) -> tuple[str, list[Any]]:  # pragma: no cover - Protocol stub
        raise NotImplementedError


@dataclass(frozen=True)
class by_type:
    """Select all nodes of a given subtype.

    Compiles to ``subtype = %s``. Uses the ``okg.graph_nodes``
    subtype index for the lookup.
    """

    subtype: str

    def compile(self) -> tuple[str, list[Any]]:
        return ("subtype = %s", [self.subtype])


@dataclass(frozen=True)
class by_type_and_attrs:
    """Select nodes of a subtype whose ``attrs`` contain the given
    JSON document.

    Compiles to ``subtype = %s AND attrs @> %s::jsonb``. The
    ``attrs @> '{...}'::jsonb`` shape uses Postgres's
    ``jsonb_path_ops`` GIN index on ``okg.graph_nodes(attrs)``
    when applicable.
    """

    subtype: str
    attrs_match: tuple[tuple[str, Any], ...]
    """Tuple of (key, value) pairs for the JSONB containment match.

    Tuple-of-tuples (not dict) so the dataclass stays frozen +
    hashable for use as a memoization key.
    """

    def compile(self) -> tuple[str, list[Any]]:
        # Build the JSONB value at compile time. psycopg3 adapts
        # Python dicts to JSONB automatically when the SQL casts
        # to ::jsonb explicitly.
        import json as _json
        as_dict = dict(self.attrs_match)
        return (
            "subtype = %s AND attrs @> %s::jsonb",
            [self.subtype, _json.dumps(as_dict)],
        )


# ---------------------------------------------------------------------------
# AnchorBuilder Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class AnchorBuilder(Protocol):
    """Project a ``NodeRow`` to zero or more :class:`Anchor` objects.

    Implementations filter internally on ``row.subtype`` (or
    ``row.attrs``) and return ``[]`` for rows they don't handle.
    The framework calls every builder on every row; cheap "not
    my concern" returns are fine at the project's expected scale
    (10³–10⁴ nodes, dozens of builders is unlikely).

    Anchor IDs MUST be deterministic: typically
    ``f"{row.node_id}:<builder-prefix>[:<subkey>]"`` so two views
    that build the same kind of anchor for the same node share
    the same id.
    """

    def __call__(
        self, row: NodeRow,
    ) -> list[Anchor]:  # pragma: no cover - Protocol stub
        raise NotImplementedError


# ---------------------------------------------------------------------------
# resolve_published_generation — copy of the substrate's idiom
# ---------------------------------------------------------------------------


def resolve_published_generation(
    conn: psycopg.Connection,
    deployment_name: str,
) -> int:
    """Return the latest published generation for ``deployment_name``.

    Same SQL the substrate uses elsewhere (see
    ``okg.substrate.cutover.snapshot`` and ``okg.substrate.cli.main``);
    centralized here so callers don't re-implement the lookup.

    Raises :class:`NoPublishedGenerationError` if no published
    generation exists for the deployment.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT generation_id
              FROM okg.graph_generations
             WHERE deployment_name = %s
               AND status = 'published'
             ORDER BY generation_id DESC
             LIMIT 1
            """,
            (deployment_name,),
        )
        row = cur.fetchone()
    if row is None:
        raise NoPublishedGenerationError(
            f"no published generation for deployment {deployment_name!r}; "
            f"check `okg status --deployment {deployment_name}` and run "
            f"`okg run` if there is none yet"
        )
    return int(row[0])


# ---------------------------------------------------------------------------
# corpus_from_okg
# ---------------------------------------------------------------------------


def _checkout(conn: psycopg.Connection, generation_id: int):
    """Open a Chronos checkout pinned to ``generation_id``.

    The Chronos checkpoint injects generation visibility, so reads
    need no per-table generation-window predicate.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    return store.checkout_generation(generation_id)


def _selectors_to_named(
    selectors: Sequence[NodeSelector],
) -> tuple[str, dict[str, Any]]:
    """Combine selector ``compile()`` fragments into one OR clause.

    Each selector emits a positional ``%s`` SQL fragment plus a
    params list. Rewrite each ``%s`` to a uniquely-named ``:p<n>``
    placeholder so the Chronos checkout session (which binds
    ``:name`` params) can execute the combined clause.
    """
    or_fragments: list[str] = []
    params: dict[str, Any] = {}
    counter = 0
    for sel in selectors:
        sql, sel_params = sel.compile()
        rebuilt: list[str] = []
        idx = 0
        for piece in sql.split("%s"):
            rebuilt.append(piece)
            if idx < len(sel_params):
                name = f"p{counter}"
                params[name] = sel_params[idx]
                rebuilt.append(f":{name}")
                counter += 1
                idx += 1
        or_fragments.append(f"({''.join(rebuilt)})")
    return " OR ".join(or_fragments), params


def corpus_from_okg(
    conn: psycopg.Connection,
    *,
    generation_id: int,
    selectors: Sequence[NodeSelector],
    anchor_builders: Sequence[AnchorBuilder],
) -> dict[str, Anchor]:
    """Read the substrate at ``generation_id`` and project anchors.

    Parameters
    ----------
    conn :
        An open ``psycopg.Connection``. The caller owns the
        lifecycle (open / commit / close).
    generation_id :
        Integer pin. The adapter raises ``TypeError`` if this is
        not an int (catches the common bug where a deployment
        name is passed by mistake).
    selectors :
        Compiled to a single ``WHERE (sel1) OR (sel2) OR ...``
        clause and combined with the substrate's canonical
        generation-pin predicate.
    anchor_builders :
        Each builder is invoked on every fetched row; the union
        of their non-empty return lists is the corpus.

    Returns
    -------
    dict[str, Anchor]
        Mapping from anchor id to anchor. Duplicate ids across
        builders are an error (raises ``BackboneError``); they
        usually indicate two builders accidentally claiming the
        same id namespace.
    """
    if not isinstance(generation_id, int) or isinstance(generation_id, bool):
        raise TypeError(
            f"corpus_from_okg: generation_id must be an int, got "
            f"{type(generation_id).__name__}({generation_id!r}). "
            f"If you have a deployment name, call "
            f"resolve_published_generation(conn, deployment_name) first."
        )

    if not selectors:
        # An empty selector list means "select nothing" — return
        # an empty corpus rather than reading every node in the
        # generation (which would be both expensive and surprising).
        return {}

    # Compile selectors → named-param (where_clause, params). The
    # selector Protocol emits positional `%s` fragments + a params
    # list; rewrite each `%s` to a uniquely-named `:p<n>` so the
    # Chronos checkout session (which binds `:name` params) can run
    # the combined OR clause.
    selector_clause, params = _selectors_to_named(selectors)

    # graph_nodes PK is node_id (one row per node at the checkout),
    # so no DISTINCT ON is needed — the Chronos checkout injects the
    # generation's visibility.
    sql = (
        "SELECT node_id, subtype, attrs, text_hash\n"
        "  FROM okg.graph_nodes\n"
        f" WHERE ({selector_clause})\n"
        " ORDER BY node_id"
    )

    graph = _checkout(conn, generation_id)
    rows: list[NodeRow] = []
    for raw in graph.query(sql, params):
        rows.append(NodeRow(
            node_id=raw["node_id"],
            subtype=raw["subtype"],
            attrs=raw["attrs"] or {},
            text_hash=raw["text_hash"],
        ))

    # Project rows → anchors via every registered builder. Builders
    # that don't apply return [] cheaply.
    corpus: dict[str, Anchor] = {}
    for row in rows:
        for builder in anchor_builders:
            for anchor in builder(row):
                if anchor.id in corpus:
                    raise BackboneError(
                        f"anchor id collision: {anchor.id!r} produced "
                        f"by more than one builder for node "
                        f"{row.node_id!r}. Fix: ensure builders' id "
                        f"namespaces don't overlap."
                    )
                corpus[anchor.id] = anchor
    return corpus


# ---------------------------------------------------------------------------
# edges_at_generation — generic over edge_type
# ---------------------------------------------------------------------------


def edges_as_anchors(
    conn: psycopg.Connection,
    *,
    generation_id: int,
    edge_type: str,
) -> dict[str, Anchor]:
    """Project edges of one ``edge_type`` as anchors keyed by edge id.

    Why anchors? The views layer's refresh-byte-reuse property
    (Property A) reasons over anchor content_hashes. A view whose
    findings depend on edge state — e.g. ``supersedure_health``'s
    rules over ``supersedes`` edges — must include those edges in
    the corpus so refresh detects when an edge's attributes change
    or when the edge itself is retracted.

    Edge-anchor convention (deterministic):

    * ``id`` — ``f"edge:{edge_type}:{edge_id}"``.
    * ``content`` — canonical JSON of
      ``{"edge_id": ..., "src": ..., "dst": ..., "attrs": ...}``
      with ``sort_keys=True`` so the hash is stable across
      re-fetches of the same edge.

    Planners that need structured access call ``json.loads`` on
    the anchor's content — see the supersedure_health view for
    an example.
    """
    import json as _json

    edges = edges_at_generation(
        conn, generation_id=generation_id, edge_type=edge_type,
    )
    out: dict[str, Anchor] = {}
    for edge in edges:
        aid = f"edge:{edge_type}:{edge['edge_id']}"
        content = _json.dumps(
            {
                "edge_id": edge["edge_id"],
                "src": edge["src"],
                "dst": edge["dst"],
                "attrs": edge["attrs"],
            },
            sort_keys=True,
        )
        out[aid] = Anchor.from_content(id=aid, content=content)
    return out


def edges_at_generation(
    conn: psycopg.Connection,
    *,
    generation_id: int,
    edge_type: str,
) -> list[dict[str, Any]]:
    """Return all edges of one ``edge_type`` live at ``generation_id``.

    Returned dicts have keys ``edge_id``, ``src``, ``dst``,
    ``edge_type``, ``attrs``. The caller (typically a diagnostic
    view's planner) joins them against the corpus by ``src`` /
    ``dst`` node id.

    Generic over ``edge_type`` so views that need other edge
    classes (citations, references) call the same helper. The
    ``okg.graph_edges`` index on ``(src, edge_type)`` covers
    this query's plan.
    """
    if not isinstance(generation_id, int) or isinstance(generation_id, bool):
        raise TypeError(
            f"edges_at_generation: generation_id must be an int, "
            f"got {type(generation_id).__name__}({generation_id!r})"
        )

    # graph_edges PK is edge_id (one row per edge at the checkout),
    # so no DISTINCT ON is needed — the Chronos checkout injects the
    # generation's visibility.
    sql = (
        "SELECT edge_id, src, dst, edge_type, attrs\n"
        "  FROM okg.graph_edges\n"
        " WHERE edge_type = :edge_type\n"
        " ORDER BY edge_id"
    )
    graph = _checkout(conn, generation_id)
    edges: list[dict[str, Any]] = []
    for raw in graph.query(sql, {"edge_type": edge_type}):
        edges.append({
            "edge_id": int(raw["edge_id"]),
            "src": raw["src"],
            "dst": raw["dst"],
            "edge_type": raw["edge_type"],
            "attrs": raw["attrs"] or {},
        })
    return edges


# ---------------------------------------------------------------------------
# Memoization — per Stage 0 finding
# ---------------------------------------------------------------------------


# A small in-process cache of (generation_id, selectors_key) →
# corpus. Keyed in the function below; the cache itself is just a
# dict because lru_cache requires the wrapped function to take
# only hashable args, and a `psycopg.Connection` is not hashable.
#
# Selectors must be hashable (frozen dataclasses) so they can be
# part of the cache key. The framework's shipped selectors
# (`by_type`, `by_type_and_attrs`) are frozen dataclasses; custom
# selectors should follow the same pattern.

_corpus_cache: dict[tuple[Any, ...], dict[str, Anchor]] = {}


def _selectors_key(
    selectors: Sequence[NodeSelector],
    builders: Sequence[AnchorBuilder],
) -> tuple[Any, ...]:
    """Build a hashable key from selectors + builders.

    Selectors are frozen dataclasses (already hashable). Builders
    are typically module-level functions; ``id(builder)`` is
    stable within a process and uniquely identifies each builder.
    Using ``id`` avoids requiring builders to be hashable.
    """
    return (
        tuple(selectors),
        tuple(id(b) for b in builders),
    )


def corpus_from_okg_cached(
    conn: psycopg.Connection,
    *,
    generation_id: int,
    selectors: Sequence[NodeSelector],
    anchor_builders: Sequence[AnchorBuilder],
) -> dict[str, Anchor]:
    """Memoized variant of :func:`corpus_from_okg`.

    Per Stage 0 finding: the substrate read dominates fresh-
    generation latency. Within one process, repeated reads
    against the same ``(generation_id, selectors, builders)`` are
    a single SQL query.

    The cache is process-local. Different `psycopg.Connection`
    instances see the same cached corpus (the cache is keyed on
    pin + selectors, not on the connection). This is correct
    because, by construction, two open connections at the same
    pin see the same node set: the substrate's MVCC + the
    pin window invariant guarantees it.

    Cache invalidation: the caller controls invalidation by using
    a different `generation_id`. There is no time-based eviction;
    the process holds the cache for its lifetime. Tests can call
    :func:`clear_corpus_cache`.
    """
    key = (generation_id, _selectors_key(selectors, anchor_builders))
    if key not in _corpus_cache:
        _corpus_cache[key] = corpus_from_okg(
            conn,
            generation_id=generation_id,
            selectors=selectors,
            anchor_builders=anchor_builders,
        )
    return _corpus_cache[key]


def clear_corpus_cache() -> None:
    """Clear the process-local corpus cache.

    Useful in tests and in long-running processes that need to
    drop stale corpora after a major substrate event (e.g., a
    catalog version bump that the views framework should treat as
    a different generation).
    """
    _corpus_cache.clear()


# ---------------------------------------------------------------------------
# render_markdown_table — default deterministic renderer for sql_view_anchor
# ---------------------------------------------------------------------------


def render_markdown_table(
    rows: Sequence[Sequence[Any]],
    columns: Sequence[str],
) -> str:
    """Render rows + column names as a pipe-table markdown block.

    Format::

        | col1 | col2 |
        | --- | --- |
        | a | b |

    NULL cells (None / SQL NULL) render as the empty string.
    Cells are converted to ``str()`` verbatim — the renderer does
    NOT escape pipe characters or newlines. Callers who need
    those escaped pre-format their cell values.

    Deterministic: equal ``rows`` + ``columns`` produce
    byte-identical output. Used as the default ``render`` callable
    for :func:`sql_view_anchor`.
    """
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body_lines = []
    for row in rows:
        cells = [
            "" if cell is None else str(cell)
            for cell in row
        ]
        body_lines.append("| " + " | ".join(cells) + " |")
    if body_lines:
        return "\n".join([header, separator, *body_lines]) + "\n"
    # Empty rows: header + separator only, no trailing blank row.
    return "\n".join([header, separator]) + "\n"


# ---------------------------------------------------------------------------
# sql_view_anchor — render a registered SQL view as one corpus anchor
# ---------------------------------------------------------------------------


# View names must match this regex. The substrate's agent-facing
# views all live under the ``okg`` schema with simple identifier
# names; deployment-side views follow the same pattern (e.g.
# ``okg.v_af_pubs_supersedure_summary``). Restricting the
# allowed shape here prevents a malicious caller from smuggling
# subqueries via the ``view_name`` argument that gets composed
# into the SELECT.
_VIEW_NAME_RE = re.compile(r"^okg\.[a-zA-Z_][a-zA-Z0-9_]*$")


# The substrate node/edge views project the (now checkout-only)
# okg.nodes_live / okg.edges_live tables. Map each to the
# branch-visible Chronos table a checkout actually rewrites, so a
# generation pin resolves real rows post-cutover. graph_store.py
# pins these names (NODE_TABLE / EDGE_TABLE).
_VIEW_TO_GRAPH_TABLE: dict[str, str] = {
    "okg.v_nodes": "okg.graph_nodes",
    "okg.v_edges": "okg.graph_edges",
}


def _where_to_named(
    where: str, params: Sequence[Any]
) -> tuple[str, dict[str, Any]]:
    """Rewrite a ``%s``-placeholder WHERE clause to ``:name`` binds.

    The public :func:`sql_view_anchor` contract takes a WHERE
    clause with positional ``%s`` placeholders plus a positional
    ``params`` sequence (the psycopg idiom). The Chronos checkout
    session binds named ``:name`` params instead, so each ``%s``
    becomes a uniquely-named ``:w<n>`` placeholder in order. The
    placeholder count must match ``len(params)`` exactly, else the
    caller passed a mismatched WHERE/params pair.
    """
    if not where:
        if params:
            raise ValueError(
                "sql_view_anchor: params supplied without a WHERE "
                "clause to bind them to."
            )
        return "", {}

    pieces = where.split("%s")
    placeholder_count = len(pieces) - 1
    if placeholder_count != len(params):
        raise ValueError(
            f"sql_view_anchor: WHERE clause has {placeholder_count} "
            f"`%s` placeholder(s) but {len(params)} param(s) were "
            f"supplied; counts must match."
        )

    named: dict[str, Any] = {}
    rebuilt: list[str] = []
    for idx, piece in enumerate(pieces):
        rebuilt.append(piece)
        if idx < placeholder_count:
            name = f"w{idx}"
            named[name] = params[idx]
            rebuilt.append(f":{name}")
    return "".join(rebuilt), named


def sql_view_anchor(
    conn: psycopg.Connection,
    *,
    anchor_id: str,
    view_name: str,
    generation_id: int,
    where: str = "",
    params: Sequence[Any] = (),
    render: Callable[
        [Sequence[Sequence[Any]], Sequence[str]], str
    ] = render_markdown_table,
) -> Anchor:
    """Render a registered ``okg.v_*`` view's result as one anchor.

    Pins ``okg.pin_generation`` to ``generation_id`` (the
    substrate convention since ``add-mcp-run-sql-tool``), runs
    ``SELECT * FROM <view_name>`` with the optional WHERE +
    params appended, passes the result rows + column names
    through ``render`` (default :func:`render_markdown_table`),
    and wraps the rendered string as
    ``Anchor.from_content(id=anchor_id, content=...)``.

    Parameters
    ----------
    conn :
        An open ``psycopg.Connection``. The function reads the
        graph through a Chronos checkout pinned to
        ``generation_id`` (``ChronosGraphStore.from_connection``),
        exactly as :func:`corpus_from_okg` does. The caller owns
        the connection lifecycle; no GUC / ``SET LOCAL`` state is
        left on it.
    anchor_id :
        Id for the resulting Anchor. By convention,
        ``"sql_view:<short_name>"``.
    view_name :
        Fully-qualified ``okg.v_*`` name. Validated against
        ``^okg\\.[a-zA-Z_][a-zA-Z0-9_]*$`` to prevent injection;
        violating values raise ``ValueError`` before any DB work.
        Post-Chronos-cutover the agent-facing ``okg.v_nodes`` /
        ``okg.v_edges`` views over ``okg.nodes_live`` are empty
        outside a checkout, so the requested view is resolved to
        the branch-visible Chronos table it projects
        (``okg.v_nodes`` → ``okg.graph_nodes``, ``okg.v_edges`` →
        ``okg.graph_edges``); the checkout injects the
        generation's visibility. ``view_name`` values without a
        known mapping raise ``ValueError``.
    generation_id :
        Integer pin. Non-int values (including bool, which is a
        subclass of int) raise ``TypeError`` before any cursor
        is opened.
    where :
        Optional ``WHERE`` clause appended to the SELECT.
        Include the ``WHERE`` keyword. Use ``%s`` placeholders
        and pass ``params`` for values; do not interpolate
        user-supplied values into the string directly.
    params :
        Positional parameters for the WHERE clause.
    render :
        Callable that turns ``(rows, columns)`` into a string.
        Default: :func:`render_markdown_table`.

    Returns
    -------
    Anchor
        With ``id=anchor_id`` and content the rendered query
        result. Property A applies: when underlying rows change
        across generations, the anchor's ``content_hash``
        changes and any fragment quoting it refreshes.
    """
    if not isinstance(generation_id, int) or isinstance(generation_id, bool):
        raise TypeError(
            f"sql_view_anchor: generation_id must be an int, got "
            f"{type(generation_id).__name__}({generation_id!r}). "
            f"Pass an integer pin (resolve via "
            f"resolve_published_generation if you have a "
            f"deployment name)."
        )
    if not _VIEW_NAME_RE.match(view_name):
        raise ValueError(
            f"sql_view_anchor: view_name {view_name!r} does not "
            f"match the allowed pattern "
            f"`^okg\\.[a-zA-Z_][a-zA-Z0-9_]*$`. The view_name "
            f"argument is composed into a SELECT statement; "
            f"only simple substrate / deployment view names "
            f"are accepted."
        )

    # The agent-facing okg.v_* views project okg.nodes_live /
    # okg.edges_live, which the Chronos cutover left empty outside
    # a checkout. Resolve the requested view to the branch-visible
    # graph table it summarizes so the checkout (which rewrites
    # only its registered tables) injects the generation's
    # visibility. corpus_from_okg / edges_at_generation read the
    # same tables through the same checkout.
    table = _VIEW_TO_GRAPH_TABLE.get(view_name)
    if table is None:
        raise ValueError(
            f"sql_view_anchor: view_name {view_name!r} has no "
            f"Chronos-table mapping. Only the substrate node/edge "
            f"views are readable through a checkout: "
            f"{sorted(_VIEW_TO_GRAPH_TABLE)}."
        )

    # The okg.v_* WHERE-clause contract uses positional `%s`
    # placeholders + a params sequence; the Chronos checkout
    # session binds `:name` params (see corpus_from_okg /
    # edges_at_generation). Rewrite each `%s` to a uniquely-named
    # `:w<n>` bind, preserving order, so the same WHERE clause
    # filters server-side over the graph table.
    where_clause, named_params = _where_to_named(where, params)

    sql = f"SELECT * FROM {table}"
    if where_clause:
        sql += f" {where_clause}"

    graph = _checkout(conn, generation_id)
    dict_rows = graph.query(sql, named_params)

    # Preserve the (rows, columns) render contract: `SELECT *`
    # column order is the dict-row key order the checkout returns;
    # rows are projected into positional tuples in that order.
    columns = list(dict_rows[0].keys()) if dict_rows else []
    rows = [tuple(row[col] for col in columns) for row in dict_rows]

    content = render(rows, columns)
    return Anchor.from_content(id=anchor_id, content=content)


__all__ = [
    "AnchorBuilder",
    "BackboneError",
    "NoPublishedGenerationError",
    "NodeRow",
    "NodeSelector",
    "by_type",
    "by_type_and_attrs",
    "clear_corpus_cache",
    "corpus_from_okg",
    "corpus_from_okg_cached",
    "edges_as_anchors",
    "edges_at_generation",
    "render_markdown_table",
    "resolve_published_generation",
    "sql_view_anchor",
]
