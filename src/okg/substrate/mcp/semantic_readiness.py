"""Is this corpus rankable by THIS embedder? (issue #797 item 5)

`analyst-search-profiles` deferred issue 797's semantic item on the grounds
that "the gate needs a capability signal that is not `embeddings.enabled`,
which is a doctor-warning suppressor rather than a statement that vectors
exist". This module is that signal, MEASURED at the pin. It answers two
questions before a ranking runs, and refuses rather than ranking when the
answer is bad:

  * COVERAGE — how many live nodes actually carry a vector.
    `semantic_search_program` ranks under `WHERE embedding IS NOT NULL`, so a
    graph with none returns an empty page indistinguishable from "your query
    matched nothing", and a graph with 3% coverage returns a page
    indistinguishable from a complete one.

  * PROVENANCE — which model wrote those vectors. Ranking a query
    vector against another model's vectors yields cosine distances that are
    noise, presented as relevance in rank order.

Two design points are load-bearing and easy to get backwards.

WHERE THE ROWS ARE. `embedding` and `embedding_model` are DECLARED on
`okg.nodes_live`, but that is an empty base relation — the live rows are
in the chronos branch table it fronts. A naive
``SELECT count(*) FROM okg.nodes_live WHERE embedding IS NOT NULL`` returns 0
on every deployment, which would turn the zero-coverage refusal into a
universal denial. The count is taken over the pinned SEGMENT.

HOW THE SEGMENT IS RESOLVED. Via `_chronos_branch_tables`, NOT via
`resolve_node_text_bm25_index`. `scope_node_counts` resolves the segment the
second way, and copying it here would be a regression: the semantic path
touches no BM25 index today, so routing its probe through `pg_indexes` would
couple a SEMANTIC refusal to a LEXICAL index, and a deployment whose semantic
search works today would start getting `semantic_coverage_unmeasurable`
because pg_textsearch was not loaded. Its ``ORDER BY indexname LIMIT 1`` is a
second reason to avoid it — an alphabetical pick across coexisting
segments would count the wrong table, and this module would promote that
wrong number into a hard denial.

The unmeasurable case REFUSES rather than ranking with a null disclosure:
"coverage could not be verified" and "coverage is fine" must not be the same
observable state. Because the segment resolved here is the same one the
ranking query reads, that refusal is reachable only when the ranking could
not have run either — no healthy path newly refuses.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import psycopg

from okg.substrate.errors import SubstrateError


__all__ = [
    "SemanticReadiness",
    "SEMANTIC_CORPUS_REFUSAL_CODES",
    "UNKNOWN_PROVENANCE",
    "assert_rankable",
    "probe_semantic_readiness",
    "resolve_node_segment",
    "semantic_corpus_refusal_error",
]


#: Corpus-state refusals an MCP agent must be able to READ rather than merely
#: receive as an exception. The search tool answers the sibling condition (no
#: embedder configured) with a typed operator refusal carrying
#: `fallback_method`; these must match that shape, because an agent that cannot
#: parse a refusal reads it as "the graph has nothing" — the exact wrong
#: conclusion.
SEMANTIC_CORPUS_REFUSAL_CODES = frozenset({
    "semantic_index_empty",
    "embedding_model_skew",
    "semantic_coverage_unmeasurable",
    "embedder_dim_mismatch",
    "embedder_identity_undeclared",
})

#: Measured fields worth forwarding into the refusal's `capability` block, so
#: an agent learns WHICH embedder or how much coverage, not merely that
#: something was refused.
_CAPABILITY_KEYS = (
    "embedded_nodes",
    "unembedded_nodes",
    "query_model_id",
    "corpus_models",
    "unknown_provenance_rows",
    "embedder_dimension",
    "substrate_dimension",
)


def semantic_corpus_refusal_error(
    exc: Any, *, method: str,
) -> dict[str, Any] | None:
    """The `error` payload for a semantic corpus refusal, or None.

    Returns None for anything else so the caller RE-RAISES: turning an
    unrecognised error into a capability report would convert a bug into a
    reassuring message, which is the fail-open this contract forbids.

    Module-level (not a closure inside `build_server`) so the classification is
    provable without a database — the per-PR Postgres lane has no `okg_mcp`
    role, so a `build_server` test placed there fails on pool acquisition.
    """
    code = getattr(exc, "code", None)
    if method != "semantic" or code not in SEMANTIC_CORPUS_REFUSAL_CODES:
        return None
    capability: dict[str, Any] = {
        "method": "semantic",
        "embedder_configured": True,
        "refusal_reason": str(code),
    }
    data = getattr(exc, "data", None)
    if isinstance(data, dict):
        for key in _CAPABILITY_KEYS:
            if key in data:
                capability[key] = data[key]
    error: dict[str, Any] = {
        "code": str(code),
        "message": str(exc),
        # `lexical` genuinely works on these corpora — the vectors are
        # missing or unattributable, the TEXT is not — so the fallback is
        # actionable rather than decorative.
        "fallback_method": "lexical",
        "capability": capability,
    }
    fix_hint = getattr(exc, "fix_hint", None)
    if fix_hint:
        error["fix_hint"] = fix_hint
    return error


#: Reported for rows carrying a vector whose `embedding_model` is NULL. The
#: column was added by a live migration that leaves it NULL on rows embedded
#: before it existed, so a row can carry a real vector and an unknown
#: provenance. This is a THIRD state, never folded into "matches".
UNKNOWN_PROVENANCE = "unknown"


@dataclass(frozen=True)
class SemanticReadiness:
    """The measured state of the corpus this ranking would read."""

    #: Live nodes carrying a vector, within the ranked scope.
    embedded: int
    #: Live nodes carrying text but no vector, within the ranked scope.
    unembedded: int
    #: Recorded model identity -> row count. `UNKNOWN_PROVENANCE` keys the
    #: NULL-`embedding_model` rows.
    models: dict[str, int]

    @property
    def unknown_provenance_rows(self) -> int:
        return self.models.get(UNKNOWN_PROVENANCE, 0)

    def envelope_fields(self) -> dict[str, Any]:
        """The disclosure carried on EVERY semantic page, not only empty ones.

        A thin page over a barely-embedded corpus is the more common and more
        misleading case; disclosing only on the empty page would leave it
        silent.
        """
        return {
            "embedding_coverage": {
                "embedded_nodes": self.embedded,
                "unembedded_nodes": self.unembedded,
                "models": dict(self.models),
            },
        }


def resolve_node_segment(conn: psycopg.Connection) -> str:
    """The physical chronos table holding live node rows.

    Reads `_chronos_branch_tables` directly. Deliberately NOT
    `resolve_node_text_bm25_index` — see the module docstring; that would
    make a missing LEXICAL index deny a working SEMANTIC search.
    """
    from okg.substrate.graph_store_internal.common import NODE_TABLE

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT physical_table
                  FROM _chronos_branch_tables
                 WHERE backend = 'interval'
                   AND table_name = %s
                """,
                (NODE_TABLE,),
            )
            row = cur.fetchone()
    except psycopg.Error as exc:
        raise _unmeasurable(
            f"the chronos node segment registry is unreadable: {exc}"
        ) from exc
    if row is None or not row[0]:
        raise _unmeasurable(
            "the chronos node segment is not registered, so the rows a "
            "semantic ranking would read cannot be located"
        )
    return str(row[0])


def _unmeasurable(detail: str) -> SubstrateError:
    err = SubstrateError(
        f"semantic search cannot measure its embedding coverage: {detail}. "
        "Refusing rather than ranking: a page that cannot verify its own "
        "coverage is indistinguishable from one that verified it and found "
        "the corpus healthy.",
        fix_hint=(
            "check that the deployment has a published generation and that "
            "the chronos node segment is registered (`okg doctor`)"
        ),
    )
    err.code = "semantic_coverage_unmeasurable"
    return err


def probe_semantic_readiness(
    conn: psycopg.Connection,
    session: Any,
    *,
    scoped_subtypes: Optional[list[str]] = None,
) -> SemanticReadiness:
    """Measure coverage + provenance over the rows a ranking would read.

    `scoped_subtypes` narrows the measurement to the subtypes actually being
    ranked when a search profile resolved a scope. Measuring the whole graph
    under a profile would report a healthy corpus while the ranked subtypes
    hold no vectors at all — the same eviction-shaped confusion at the
    coverage layer that `analyst-search-profiles` fixed at the ranking layer.

    Raises `semantic_coverage_unmeasurable` rather than returning nulls.
    """
    from okg.substrate.bm25_node_search import SearchIndexUnavailable, branch_point

    segment = resolve_node_segment(conn)
    try:
        bp = branch_point(conn, session)
    except SearchIndexUnavailable as exc:
        # branch_point does NOT depend on the BM25 index (it reads the chronos
        # segment registry), so this is a genuinely unresolvable pin, which is
        # also a pin the ranking query could not have read.
        raise _unmeasurable(
            f"the pinned generation's branch point is unresolvable: {exc}"
        ) from exc

    where = [
        "deleted = FALSE",
        "live_lo <= %(bp)s",
        "%(bp)s < live_hi",
        "attrs ? 'text'",
    ]
    params: dict[str, Any] = {"bp": bp}
    if scoped_subtypes is not None:
        where.append("subtype = ANY(%(scoped_subtypes)s)")
        params["scoped_subtypes"] = list(scoped_subtypes)

    # One bounded aggregate. NULL `embedding_model` is grouped under its own
    # key rather than coalesced into a real identity: coalescing would make an
    # unverified row compare equal to a verified one, which is the precise
    # confusion the provenance check exists to prevent.
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT (embedding IS NOT NULL) AS has_vector,
                       COALESCE(embedding_model, %(unknown)s) AS model,
                       count(*) AS n
                  FROM {segment}
                 WHERE {' AND '.join(where)}
                 GROUP BY 1, 2
                """,
                {**params, "unknown": UNKNOWN_PROVENANCE},
            )
            rows = cur.fetchall()
    except psycopg.Error as exc:
        raise _unmeasurable(
            f"the coverage aggregate over {segment} failed: {exc}"
        ) from exc

    embedded = 0
    unembedded = 0
    models: dict[str, int] = {}
    for has_vector, model, n in rows:
        count = int(n)
        if has_vector:
            embedded += count
            models[str(model)] = models.get(str(model), 0) + count
        else:
            unembedded += count
    return SemanticReadiness(
        embedded=embedded, unembedded=unembedded, models=models,
    )


def assert_rankable(
    readiness: SemanticReadiness,
    *,
    query_model_id: str,
    allow_unverified_provenance: bool = False,
    scoped_subtypes: Optional[list[str]] = None,
) -> None:
    """Refuse a ranking the corpus cannot support. Raises or returns None.

    Two refusals, in the order an operator can act on them.

    `semantic_index_empty` — no vectors at all. This is the default state
    of a deployment that has never run `okg_embed_batch`, so returning an
    empty page here teaches an analyst that the graph lacks content it
    actually holds.

    `embedding_model_skew` — the vectors were written by a model other
    than the one embedding this query, so the distances would be noise. NULL
    provenance is refused on the same path but is a DISTINCT state, escapable
    by explicit opt-in; a KNOWN-different model is never escapable, because
    "I accept unverified provenance" is not the same permission as "I accept a
    model I know is wrong".
    """
    if readiness.embedded == 0:
        scope_note = (
            f" within the {len(scoped_subtypes)} subtype(s) of the active "
            "search profile"
            if scoped_subtypes is not None
            else ""
        )
        err = SubstrateError(
            f"semantic search has nothing to rank: no live node{scope_note} "
            f"carries an embedding at this generation "
            f"({readiness.unembedded} carry text without one). Refusing "
            "rather than returning an empty page, which would be "
            "indistinguishable from the query matching nothing.",
            data={
                "embedded_nodes": 0,
                "unembedded_nodes": readiness.unembedded,
                "scoped_subtypes": scoped_subtypes,
            },
            fix_hint=(
                "populate embeddings with the `okg_embed_batch` workflow "
                "(set `embeddings.enabled: true` and an explicit compatible "
                "`embeddings.backend` in "
                "the deployment manifest), or use --method lexical"
            ),
        )
        err.code = "semantic_index_empty"
        raise err

    known_other = {
        model: n
        for model, n in readiness.models.items()
        if model != UNKNOWN_PROVENANCE and model != query_model_id
    }
    unknown_rows = readiness.unknown_provenance_rows
    unknown_blocks = unknown_rows > 0 and not allow_unverified_provenance

    if known_other or unknown_blocks:
        parts = []
        if known_other:
            parts.append(
                "stored vectors were written by "
                + ", ".join(
                    f"{model!r} ({n} rows)"
                    for model, n in sorted(known_other.items())
                )
            )
        if unknown_blocks:
            parts.append(
                f"{unknown_rows} rows carry a vector with NO recorded model, "
                "so their provenance is unknown rather than verified"
            )
        err = SubstrateError(
            f"semantic search would rank across embedding models: "
            f"the query is embedded by {query_model_id!r} but "
            + "; ".join(parts)
            + ". Cosine distances between vectors from different models are "
            "not comparable, so the ranking would be noise presented as "
            "relevance.",
            data={
                "query_model_id": query_model_id,
                "corpus_models": dict(readiness.models),
                "unknown_provenance_rows": unknown_rows,
            },
            fix_hint=(
                f"query with the embedder that wrote the corpus, re-embed "
                f"with `okg_embed_batch` under {query_model_id!r}"
                + (
                    ", or pass --allow-unverified-embedding-provenance to "
                    "rank the unknown-provenance rows anyway"
                    if unknown_blocks and not known_other
                    else ""
                )
            ),
        )
        err.code = "embedding_model_skew"
        raise err
