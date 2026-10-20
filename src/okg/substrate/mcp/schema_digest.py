"""Schema digest — compact deployment ontology summary for MCP
session open.

Per `improve-mcp-tool-surface`, the substrate exposes a digest at
the MCP resource URL `okg://schema-digest/<deployment>` so the
agent learns the deployment's subtypes, attrs, edge types, and
score functions in one round-trip — no LinkML grepping, no
per-subtype `describe_subtype` chain.

The digest's contract:

  {
    "deployment": "<name>",
    "pinned_generation_id": "<text generation id>",
    "subtype_count": <int>,
    "subtypes": {
      "<subtype>": {
        "node_count": <int>,
        "attr_keys": [<str>, ...],   // observed attr keys at the pin
        "edges_out": [<edge_type>, ...],
        "edges_in":  [<edge_type>, ...],
      },
      ...
    },
    "edge_type_count": <int>,
    "edge_types": [<edge_type>, ...],
    "edge_type_counts": {"<edge_type>": <int>, ...},
    "score_functions": [<fn>, ...],   // substrate-shipped vocab
  }

Size budget: < 5KB per deployment. Enforced by the digest
generator dropping low-frequency attr keys (freq < 0.05) and
truncating long attr-key lists. The size-test suite asserts the
budget against `okg-workspace`, `af_pubs`, and a minimal
deployment.

Generation strategy: the digest is generated once per
deployment per server-cache window, NOT per session. The
underlying queries read the most-recently-resolved generation's
Chronos checkpoint (`okg.graph_nodes` / `okg.graph_edges`); the cache invalidates on catalog reload
(detected via `okg.catalog_versions` change).

This module exposes the generator function only. The MCP-server
wiring (resource registration + cache) lives in `server.py`.

Per `improve-mcp-tool-surface` (2026-04-27, tasks 2.1-2.3).
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Callable, Iterator

import psycopg


__all__ = [
    "BUILTIN_SCORE_FUNCTIONS",
    "ATTR_FREQ_FLOOR",
    "DIGEST_STATEMENT_TIMEOUT_MS",
    "MAX_DIGEST_BYTES",
    "fetch_minimal_schema_counts",
    "generate_schema_digest",
    "minimal_digest",
]


# Score functions the substrate ships out of the box. Per the
# rebased proposal §"Layer 2 — Score functions", deployments
# extend this list at deployment-config-validation time; v0 does
# not yet have the per-deployment registry, so the digest
# enumerates the substrate vocabulary alone.
BUILTIN_SCORE_FUNCTIONS: tuple[str, ...] = (
    "bm25",
    "jaccard",
    "recency",
    "score_sum",
    "by_attr_value",
)

# Attr-frequency floor: keys observed on < 5% of a subtype's
# nodes are dropped from the digest. The agent that needs them
# can drill via `describe_subtype`. Floor balances completeness
# against the 5KB budget — a deployment with 200 subtypes ×
# 100 attrs each would blow the budget; the floor + the
# `describe_subtype` lazy-detail tool together cover both.
ATTR_FREQ_FLOOR: float = 0.05

# Per-subtype cap on attr keys included in the digest. Sessions
# that genuinely need rare attrs drill via `describe_subtype`.
# The cap targets the 5KB budget directly: on `okg-workspace`,
# `agent_session` has 27 observed attrs (~550 bytes worth);
# capping at 12 covers the workload's hot path while staying
# within budget. Empirically tuned; revisit if a deployment
# has truly attribute-rich subtypes that the cap excludes.
MAX_ATTR_KEYS_PER_SUBTYPE: int = 12

# Hard upper bound on the JSON-encoded digest size. Server-
# side enforcement; if a generated digest exceeds this, the
# generator raises (caller logs + falls back to a minimal
# digest containing only `subtypes.<name>.node_count`).
#
# The original 5KB budget was a pre-implementation estimate.
# Empirical measurement on `okg-workspace` (35 subtypes, ~180
# attrs across the workload) showed a full digest at 6.3KB
# even with the freq floor + per-subtype attr cap applied.
# 8KB ≈ 2000 tokens (~1% of a 200K context) and gives every
# OKG deployment we ship today room without sacrificing the
# attr_keys / edges contract. Revisit when a deployment
# overflows even this — at which point a per-subtype `weight`
# (drop subtypes with weight < some threshold) is the next
# trim lever.
MAX_DIGEST_BYTES: int = 8_192  # 8KB

# Statement timeout for every digest query, transaction-local (`SET LOCAL`
# via set_config). The full digest's attr-key scan (`jsonb_object_keys` over
# every node) and edge-route join (every edge joined to both endpoints) are
# whole-graph work that had never run on a large graph before the checkout
# read path: they failed at once on the bare views. 5 s bounds one resource
# read. A full digest that hits it raises `ValueError`, so the resource
# serves the minimal digest and names the timeout in `minimal_reason`. The
# count-only path runs under the same bound; if it also times out, the
# error surfaces -- no counts are invented.
DIGEST_STATEMENT_TIMEOUT_MS: int = 5_000


def _generation_reader(
    conn: psycopg.Connection, pinned_generation_id: int | str,
) -> Callable[[str, dict[str, Any] | None], list[dict[str, Any]]]:
    """Run one read statement against the pinned generation's checkpoint.

    The graph tables (`okg.graph_nodes`, `okg.graph_edges`) are Chronos
    checkout surfaces: bare SQL on this connection, and the `okg.v_*` views
    over them, refuse with `okg.require_graph_checkout`. Each statement runs
    through `query_generation_once`, which pins the TEXT generation id
    (`okg:<deployment>:branch:<b>:gen:...`) as given -- no integer cast --
    and writes nothing, so the caller's transaction stays read-only.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(
        conn, ensure_schema=False, autocommit=False,
    )

    def read(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        return store.query_generation_once(pinned_generation_id, sql, params or {})

    return read


def _counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return {str(row[key]): int(row["n"]) for row in rows}


def generate_schema_digest(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    pinned_generation_id: int | str,
) -> dict[str, Any]:
    """Build the digest for `deployment_name` at `pinned_generation_id`.

    Reads the generation's Chronos checkpoint (see `_generation_reader`).
    Caller owns the surrounding transaction; nothing is written.

    Raises `ValueError` if the encoded digest exceeds
    `MAX_DIGEST_BYTES`. The caller may then fall back to a
    minimal digest (subtypes + counts only) and log a warning.
    """
    timeout_ms = int(DIGEST_STATEMENT_TIMEOUT_MS)
    try:
        with _bounded_transaction(conn, timeout_ms):
            read = _generation_reader(conn, pinned_generation_id)

            # ---- subtypes + node counts ----
            # The whole-graph aggregates stay in this function and in
            # `fetch_minimal_schema_counts`: those are the read sites the Nomos
            # bypass audit approves (the resource omits them under `enforce`).
            subtype_counts = _counts(read(
                "SELECT subtype, count(*) AS n FROM okg.graph_nodes "
                "GROUP BY subtype ORDER BY subtype"
            ), "subtype")

            # ---- attr keys per subtype, dropped below ATTR_FREQ_FLOOR ----
            # One SQL pass counts (subtype, key) occurrences; the frequency floor,
            # the per-subtype order (freq DESC, then key) and the cap apply here.
            ranked: dict[str, list[tuple[float, str]]] = {}
            for row in read(
                "SELECT n.subtype, k.attr_key, count(*) AS occurrences "
                "FROM okg.graph_nodes n, jsonb_object_keys(n.attrs) AS k(attr_key) "
                "GROUP BY n.subtype, k.attr_key"
            ):
                subtype = str(row["subtype"])
                total = subtype_counts.get(subtype, 0)
                if total <= 0:
                    continue
                freq = int(row["occurrences"]) / total
                if freq >= ATTR_FREQ_FLOOR:
                    ranked.setdefault(subtype, []).append((freq, str(row["attr_key"])))
            attr_keys_by_subtype: dict[str, list[str]] = {
                subtype: [
                    key for _freq, key in sorted(pairs, key=lambda p: (-p[0], p[1]))
                ][:MAX_ATTR_KEYS_PER_SUBTYPE]
                for subtype, pairs in ranked.items()
            }

            # ---- edge type counts ----
            edge_type_counts = _counts(read(
                "SELECT edge_type, count(*) AS n FROM okg.graph_edges "
                "GROUP BY edge_type ORDER BY edge_type"
            ), "edge_type")

            # ---- edge_types per subtype (in / out) ----
            # Distinct (src_subtype, edge_type, dst_subtype) triples at the pin,
            # pivoted client-side: the result fits in tens of rows.
            edges_out_by_subtype: dict[str, set[str]] = {}
            edges_in_by_subtype: dict[str, set[str]] = {}
            for row in read(
                "SELECT DISTINCT ns.subtype AS src_subtype, e.edge_type, "
                "nd.subtype AS dst_subtype "
                "FROM okg.graph_edges e "
                "JOIN okg.graph_nodes ns ON ns.node_id = e.src "
                "JOIN okg.graph_nodes nd ON nd.node_id = e.dst"
            ):
                edge_type = str(row["edge_type"])
                edges_out_by_subtype.setdefault(str(row["src_subtype"]), set()).add(edge_type)
                edges_in_by_subtype.setdefault(str(row["dst_subtype"]), set()).add(edge_type)

            # ---- compose the digest ----
            subtypes: dict[str, dict[str, Any]] = {}
            for subtype, count in subtype_counts.items():
                subtypes[subtype] = {
                    "node_count": count,
                    "attr_keys": attr_keys_by_subtype.get(subtype, []),
                    "edges_out": sorted(edges_out_by_subtype.get(subtype, ())),
                    "edges_in": sorted(edges_in_by_subtype.get(subtype, ())),
                }

            digest: dict[str, Any] = {
                "deployment": deployment_name,
                "pinned_generation_id": pinned_generation_id,
                "subtype_count": len(subtypes),
                "subtypes": subtypes,
                "edge_type_count": len(edge_type_counts),
                "edge_types": sorted(edge_type_counts),
                "edge_type_counts": edge_type_counts,
                "score_functions": list(BUILTIN_SCORE_FUNCTIONS),
            }
    except psycopg.errors.QueryCanceled as exc:
        raise ValueError(
            f"full schema digest for {deployment_name!r} timed out after "
            f"{timeout_ms} ms (statement_timeout); serving subtype and "
            f"edge-type counts only"
        ) from exc

    # Size-budget enforcement. Encoding once; the MCP layer
    # serializes the same JSON later, so this is the canonical
    # measurement.
    encoded = json.dumps(digest, separators=(",", ":"))
    if len(encoded.encode("utf-8")) > MAX_DIGEST_BYTES:
        raise ValueError(
            f"schema digest for {deployment_name!r} exceeds "
            f"{MAX_DIGEST_BYTES}-byte budget "
            f"({len(encoded.encode('utf-8'))} bytes). "
            f"Caller should fall back to a minimal digest "
            f"(subtype and edge-type counts only)."
        )
    return digest


@contextmanager
def _bounded_transaction(conn: psycopg.Connection, timeout_ms: int) -> Iterator[None]:
    """A savepoint (or transaction) whose statements stop after ``timeout_ms``.

    Inside the caller's transaction `conn.transaction()` is a savepoint, so a
    cancelled statement rolls back only the digest's own work and the caller
    can still run the minimal fallback in the same transaction.
    """
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (f"{int(timeout_ms)}ms",),
            )
        yield


def fetch_minimal_schema_counts(
    conn: psycopg.Connection,
    *,
    pinned_generation_id: int | str,
) -> tuple[dict[str, int], dict[str, int]]:
    """Fetch the count-only digest facts at ``pinned_generation_id``.

    This is the budget-overflow path used by
    `okg://schema-digest/<deployment>`. It avoids attr-key scans and
    edge-route joins, but still gives an agent subtype names, subtype
    counts, edge type names, and edge type counts. It reads the same
    Chronos checkpoint as the full digest.
    """
    with _bounded_transaction(conn, int(DIGEST_STATEMENT_TIMEOUT_MS)):
        read = _generation_reader(conn, pinned_generation_id)
        subtype_counts = _counts(read(
            "SELECT subtype, count(*) AS n FROM okg.graph_nodes "
            "GROUP BY subtype ORDER BY subtype"
        ), "subtype")
        edge_type_counts = _counts(read(
            "SELECT edge_type, count(*) AS n FROM okg.graph_edges "
            "GROUP BY edge_type ORDER BY edge_type"
        ), "edge_type")
    return subtype_counts, edge_type_counts


def minimal_digest(
    *,
    deployment_name: str,
    pinned_generation_id: int | str,
    subtype_counts: dict[str, int],
    edge_type_counts: dict[str, int] | None = None,
    minimal_reason: str | None = None,
) -> dict[str, Any]:
    """Fallback digest: subtype and edge-type counts only.

    Used when the full digest exceeds `MAX_DIGEST_BYTES`. The
    agent gets subtype and edge-type names (so it can drill via
    `describe_subtype` / `aggregate_neighbors`) without attr keys or
    per-subtype edge routes. Strictly smaller; cannot exceed the budget
    for any sane deployment.
    """
    edge_type_counts = edge_type_counts or {}
    return {
        "deployment": deployment_name,
        "pinned_generation_id": pinned_generation_id,
        "subtype_count": len(subtype_counts),
        "subtypes": {
            name: {"node_count": int(c)}
            for name, c in subtype_counts.items()
        },
        "edge_type_count": len(edge_type_counts),
        "edge_types": sorted(edge_type_counts),
        "edge_type_counts": {
            name: int(c)
            for name, c in edge_type_counts.items()
        },
        "score_functions": list(BUILTIN_SCORE_FUNCTIONS),
        "minimal": True,
        "minimal_reason": (
            minimal_reason
            or f"full digest exceeded {MAX_DIGEST_BYTES}-byte budget"
        ),
    }
