"""SemanticConceptLinker — substrate-shipped enricher that
links source nodes to operator-authored Concepts via
section-max cosine similarity.

Per ``add-substrate-semantic-concept-linking``. For each
active Concept node with a populated ``embedding``, finds
sources whose
maximum-section cosine similarity to the concept's
description embedding exceeds the threshold; emits one
``mentions`` ``edge_facts`` row per surviving result with
``confidence = similarity``,
``provenance = derived_semantic``.

Why section-max (not pub-mean): empirically, mean-pool
dilutes a multi-topic source's signal in proportion to its
length. A 600-page F-16 ops manual mentioning ejection
seats in 5 sections has its pub-mean dominated by the
other 595 sections; an aeromedical-evacuation manual where
every section is about emergency aircrew handling beats it
on pub-mean cosine to "ejection seat". Section-max gives
the F-16 manual its rightful 0.7+ from its ejection-seat
sections.

Prerequisites the operator must satisfy before this
enricher emits any edges:
  1. Ship ``<deployment>/concepts/seed.yaml`` and apply the
     catalog so Concept nodes exist in the graph.
  2. Enable deployment ``embeddings`` config and run
     ``okg runtime worker --deployment <name>`` until both Concept
     and section embeddings are populated. The enricher skips with
     a clear warning if either is missing.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.enrichers.base import EnrichResult

__all__ = ["SemanticConceptLinker"]

logger = logging.getLogger(__name__)


class SemanticConceptLinker:
    """Substrate-shipped Enricher emitting mentions edges
    via section-max cosine similarity over pgvector
    embeddings.

    Constructor args (all required except where noted):
      ``source_subtype``  — the subtype that becomes the
                            ``src`` of the emitted mentions
                            edge (e.g. ``'publication'``).
      ``section_subtype`` — the subtype carrying per-chunk
                            embeddings whose ``parent_attr``
                            names the source (e.g.
                            ``'section'``).
      ``parent_attr``     — the section attr that names its
                            parent source (e.g. ``'pub_id'``).
      ``min_similarity``  — cosine threshold below which
                            mentions edges are not emitted.
                            REQUIRED — no silent default
                            (project P0 rule).
      ``top_k_per_concept`` (default 50) — cap on edges
                            emitted per concept per cycle.
    """

    name: str = "okg_semantic_concept_linker"
    emits_edge_types: tuple[str, ...] = ("mentions",)

    def __init__(
        self,
        *,
        source_subtype: str,
        section_subtype: str,
        parent_attr: str,
        min_similarity: float | None = None,
        top_k_per_concept: int = 50,
    ) -> None:
        for arg_name, val in (
            ("source_subtype", source_subtype),
            ("section_subtype", section_subtype),
            ("parent_attr", parent_attr),
        ):
            if not val or not isinstance(val, str):
                raise ValueError(
                    f"SemanticConceptLinker: {arg_name} is required "
                    f"and must be a non-empty string"
                )
        # `parent_attr` is interpolated as a SQL string literal into the
        # Chronos graph read below (the `->>` operator cannot take a bound
        # parameter through Chronos's sqlglot rewrite — it becomes a broken
        # `json_extract_path_text(jsonb, unknown)`). It is operator-supplied
        # config (a JSONB attr name), never graph data, but reject quote
        # characters defensively so it can never break out of the literal.
        if "'" in parent_attr or "\\" in parent_attr:
            raise ValueError(
                "SemanticConceptLinker: parent_attr must be a plain JSONB "
                f"attribute name (no quotes/backslashes), got {parent_attr!r}"
            )
        if min_similarity is None:
            raise ValueError(
                "SemanticConceptLinker: min_similarity is required "
                "(no silent default; project P0 rule). Typical "
                "starting value: 0.5."
            )
        if not isinstance(min_similarity, (int, float)) or \
                not (-1.0 <= float(min_similarity) <= 1.0):
            raise ValueError(
                f"SemanticConceptLinker: min_similarity must be a "
                f"number in [-1, 1], got {min_similarity!r}"
            )
        if not isinstance(top_k_per_concept, int) or \
                top_k_per_concept <= 0:
            raise ValueError(
                f"SemanticConceptLinker: top_k_per_concept must be a "
                f"positive int, got {top_k_per_concept!r}"
            )

        self.source_subtype = source_subtype
        self.section_subtype = section_subtype
        self.parent_attr = parent_attr
        self.min_similarity = float(min_similarity)
        self.top_k_per_concept = int(top_k_per_concept)

        # Required for the publisher's narrowing validator.
        self.reads_subtypes = (source_subtype, section_subtype, "concept")
        self.requires_narrowings = (
            (source_subtype, "mentions", "concept"),
        )

    # ----- enrich --------------------------------------------------

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        **_publisher_kwargs: object,
    ) -> EnrichResult:
        # **_publisher_kwargs absorbs publisher-passed flags
        # (e.g. `incremental=True`) the enricher doesn't
        # consume but the publisher passes uniformly to all
        # enrichers. Without it the publisher's call site
        # raises TypeError on flags this enricher hasn't
        # opted into.
        run_id = str(uuid.uuid4())
        observed_at = datetime.now(timezone.utc)

        # Prerequisite checks — emit zero edges with a clear
        # warning if either side hasn't been embedded yet.
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        n_concepts_with_emb = int(
            graph.query(
                """
                SELECT count(*) AS n
                  FROM okg.graph_nodes
                 WHERE subtype = 'concept'
                   AND embedding IS NOT NULL
                """
            )[0]["n"]
        )
        n_sections_with_emb = int(
            graph.query(
                """
                SELECT count(*) AS n
                  FROM okg.graph_nodes
                 WHERE subtype = :section_subtype
                   AND embedding IS NOT NULL
                """,
                {"section_subtype": self.section_subtype},
            )[0]["n"]
        )

        if n_concepts_with_emb == 0:
            logger.warning(
                "%s: zero Concept nodes have embeddings; "
                "enable embedding and run `okg runtime worker` first. "
                "Skipping.",
                self.name,
            )
            return EnrichResult(
                enricher_name=self.name, n_edges_emitted=0,
            )
        if n_sections_with_emb == 0:
            logger.warning(
                "%s: zero %s nodes have embeddings; "
                "enable embedding and run `okg runtime worker` first. "
                "Skipping.",
                self.name, self.section_subtype,
            )
            return EnrichResult(
                enricher_name=self.name, n_edges_emitted=0,
            )

        logger.info(
            "%s: %d concepts × max ~%d source matches each "
            "(threshold=%.3f, top_k=%d)",
            self.name, n_concepts_with_emb,
            self.top_k_per_concept,
            self.min_similarity, self.top_k_per_concept,
        )

        n_edges = self._emit_links(
            conn,
            generation_id=generation_id,
            observed_at=observed_at,
            run_id=run_id,
        )

        logger.info(
            "%s: emitted %d mentions edges", self.name, n_edges,
        )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=n_edges,
        )

    def _emit_links(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        observed_at: datetime,
        run_id: str,
    ) -> int:
        """For each Concept with an embedding, call the SRF
        and emit top-K mentions edges."""
        n_inserted = 0

        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        concept_ids = [
            row["node_id"]
            for row in graph.query(
                """
                SELECT node_id
                  FROM okg.graph_nodes
                 WHERE subtype = 'concept'
                   AND embedding IS NOT NULL
                 ORDER BY node_id
                """
            )
        ]

        # `parent_attr` is inlined as a SQL string literal (see constructor
        # note): the `->>` / `?` JSONB operators cannot take a bound
        # parameter key through Chronos's sqlglot query rewrite. The
        # constructor rejects quote/backslash characters so this is safe.
        parent_attr_lit = f"'{self.parent_attr}'"
        per_concept_sql = f"""
            WITH concept AS (
              SELECT embedding
                FROM okg.graph_nodes
               WHERE node_id = :concept_id
                 AND subtype = 'concept'
                 AND embedding IS NOT NULL
               LIMIT 1
            ),
            per_source AS (
              SELECT s.attrs->>{parent_attr_lit} AS parent_id,
                     (1 - cosine_distance(s.embedding, c.embedding))::real
                         AS section_sim
                FROM okg.graph_nodes s
                CROSS JOIN concept c
               WHERE s.subtype = :section_subtype
                 AND s.embedding IS NOT NULL
                 AND s.attrs ? {parent_attr_lit}
            )
            SELECT parent_id AS source_node_id,
                   max(section_sim)::real AS similarity
              FROM per_source
             WHERE parent_id IS NOT NULL
             GROUP BY parent_id
            HAVING max(section_sim) >= :min_similarity::real
             ORDER BY max(section_sim) DESC
             LIMIT :top_k
        """
        with conn.cursor() as cur:
            for concept_id in concept_ids:
                hits = [
                    (row["source_node_id"], row["similarity"])
                    for row in graph.query(
                        per_concept_sql,
                        {
                            "concept_id": concept_id,
                            "section_subtype": self.section_subtype,
                            "min_similarity": self.min_similarity,
                            "top_k": self.top_k_per_concept,
                        },
                    )
                ]

                for src_id, sim in hits:
                    edge_id = uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"{src_id}|mentions|{concept_id}|semantic",
                    ).int & ((1 << 63) - 1)
                    edge_dedupe = (
                        f"okg_semantic_concept:{src_id}:{concept_id}:"
                        f"gen:{generation_id}"
                    )
                    attrs = {
                        "match_type": "concept_link",
                        "extraction_method": "semantic_section_max",
                        "evidence": {
                            "similarity": float(sim),
                        },
                    }
                    cur.execute(
                        """
                        INSERT INTO okg.edge_facts
                          (edge_id, src, dst, edge_type, attrs,
                           provenance, confidence, source,
                           source_record_id, source_revision,
                           observed_at, dedupe_key, run_id, op)
                        VALUES (%s, %s, %s, 'mentions',
                                %s::jsonb,
                                'derived_semantic', %s,
                                %s,
                                %s::jsonb, %s::jsonb,
                                %s, %s, %s, 'I')
                        ON CONFLICT (dedupe_key, observed_at)
                          DO NOTHING
                        """,
                        (
                            edge_id, src_id, concept_id,
                            Jsonb(attrs),
                            float(sim),
                            f"_enricher:{self.name}",
                            Jsonb({"src": src_id, "dst": concept_id}),
                            Jsonb({"generation_id": generation_id}),
                            observed_at,
                            edge_dedupe,
                            run_id,
                        ),
                    )
                    if cur.rowcount > 0:
                        n_inserted += 1

        return n_inserted
