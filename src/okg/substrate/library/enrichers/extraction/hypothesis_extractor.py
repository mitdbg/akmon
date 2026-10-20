"""HypothesisExtractor — LLM-driven Hypothesis extraction.

Per ``extend-substrate-extraction-with-claim-evidence-extractors``
spec.md §"Hypothesis Extractor Enricher":

- Reads A1's Introduction + Discussion ``PaperSection`` bodies
  (filtered by section-name match against
  ``PaperSection.section_kind`` enum).
- Emits ``reasoning.Hypothesis`` with ``extracted_from_paper_ref``
  set, ``author_ref`` set, ``provenance: extracted_model``.
- Additionally emits ``Hypothesis predicts_about Concept`` edges
  when the extracted hypothesis names a Concept-shaped target
  (the Concept must resolve via alias lookup; if A3 hasn't
  declared the narrowing slot the trigger rejects the edge —
  logged finding).

Default ``confidence_floor`` per spec: 0.5 (paper hypotheses are
often phrased indirectly — "we conjecture", "we show that" — a
strict floor would miss the long tail).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Iterable

import psycopg

from okg.substrate.library.enrichers import DerivedSupportPolicy
from okg.substrate.library.enrichers.extraction.base import (
    ExtractionCandidate,
    IncompleteExtractionResponse,
    LLMExtractor,
    make_dedupe_key,
)

logger = logging.getLogger(__name__)


__all__ = ["HypothesisExtractor", "parse_llm_hypotheses_response"]


_HYPOTHESIS_SECTION_KINDS = frozenset(
    {
        "introduction",
        "discussion",
        "conclusion",
    }
)


_SYSTEM_PROMPT = """\
You extract paper-level hypotheses from a scientific paper's
Introduction or Discussion section.

A hypothesis is a testable claim the authors stake out — typically
phrased as "we conjecture …", "we show that …", "our hypothesis is
…", or similar. Distinguish from:
- pure observations (those go elsewhere)
- restatements of prior work (cite, do not re-emit)
- methodology descriptions

Return ONLY a JSON array of objects with:
- statement: single-sentence hypothesis statement
- confidence: float in [0, 1]
- predicts_about_concept_names: list of canonical concept names
  the hypothesis predicts something about (e.g. ["self-attention",
  "training stability"]); may be empty.

Rules:
- Emit zero hypotheses (`[]`) when the section is methodology-only
  or background.
- Wrap in ```json``` if useful; the parser strips fences.
"""


def parse_llm_hypotheses_response(
    raw: str,
    *,
    strict: bool = False,
) -> list[dict[str, Any]]:
    """Parse LLM JSON into hypothesis dicts. Tolerates fences;
    validates statement + confidence; coerces concept-name list.
    """

    def invalid(message: str) -> None:
        if strict:
            raise IncompleteExtractionResponse(message)
        logger.warning("hypothesis_extractor: %s", message)

    if not raw:
        invalid("LLM response was empty")
        return []
    text = raw.strip()
    if text.startswith("```"):
        nl = text.find("\n")
        if nl != -1:
            text = text[nl + 1 :]
        if text.endswith("```"):
            text = text[:-3].strip()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        invalid(
            f"LLM response did not parse as JSON: {e}; first 200 chars: {text[:200]!r}"
        )
        return []
    if not isinstance(parsed, list):
        invalid(f"LLM response was not a JSON array; got {type(parsed).__name__}")
        return []
    out: list[dict[str, Any]] = []
    for index, item in enumerate(parsed):
        if not isinstance(item, dict):
            invalid(f"candidate {index} was not an object")
            continue
        stmt = item.get("statement")
        if not isinstance(stmt, str) or not stmt.strip():
            invalid(f"candidate {index} had no non-empty statement")
            continue
        try:
            conf_f = float(item.get("confidence"))
        except (TypeError, ValueError):
            invalid(
                f"candidate {index} had invalid confidence={item.get('confidence')!r}"
            )
            continue
        if not (0.0 <= conf_f <= 1.0):
            invalid(f"candidate {index} had out-of-range confidence={conf_f!r}")
            continue
        concepts = item.get("predicts_about_concept_names") or []
        if not isinstance(concepts, list):
            invalid(f"candidate {index} predicts_about_concept_names was not a list")
            concepts = []
        if any(not isinstance(c, str) or not c.strip() for c in concepts):
            invalid(f"candidate {index} had an invalid predicted concept name")
        concept_names = [
            str(c).strip() for c in concepts if isinstance(c, str) and c.strip()
        ]
        out.append(
            {
                "statement": stmt.strip(),
                "confidence": conf_f,
                "predicts_about_concept_names": concept_names,
            }
        )
    return out


class HypothesisExtractor(LLMExtractor):
    """Emit ``reasoning.Hypothesis`` nodes from A1's Introduction +
    Discussion PaperSection bodies.

    Importing requires the substrate's reasoning module to have
    landed the additive `extracted_from_paper_ref` / `author_ref`
    attrs (per the A2 schema additions in Phase 1).
    """

    name = "hypothesis_extractor"
    reads_subtypes = ("paper_section",)
    emits_edge_types = ("predicts_about",)
    requires_narrowings = (
        ("hypothesis", "predicts_about", "concept"),
        ("hypothesis", "derived_from", "extraction_dropped"),
    )
    emits_typed_subtype = "hypothesis"
    # PACT bind-derived-node-provenance: every node this extractor
    # emits declares the node it was extracted from, so it does not
    # land with an empty (fail-closed, read-invisible) lineage envelope.
    declares_node_provenance = True
    # Every node this extractor emits is derived from the one source node
    # it was handed, so it stays live only while that input does.
    support_policy: DerivedSupportPolicy = "all_members"
    # Per dogfood-canary follow-up gap #5: cheap re-runs only.
    publisher_skip_when_clean = True
    snapshot_replacement_mode = "complete_producer"

    def extract_candidates(
        self,
        conn: psycopg.Connection,
        generation_id: int,
    ) -> Iterable[ExtractionCandidate]:
        if self.llm is None:
            self._record_input_failure(
                "extractor_configuration",
                IncompleteExtractionResponse("no LLM client configured"),
            )
            return
        sections = self._fetch_intro_discussion_sections(conn, generation_id)
        if not sections:
            logger.info(
                "%s: zero Intro/Discussion sections at gen %d; skipping",
                self.name,
                generation_id,
            )
            return
        from okg.substrate.llm.fanout import ModelCallJob

        # Batched dispatch through the shared fan-out; the per-section shape is
        # unchanged. Provider calls overlap, recording stays serial on `conn`.
        usable = [s for s in sections if s["body"]]
        jobs = []
        for section in usable:
            user_prompt = (
                f"Paper id: {section['paper_ref']}\n"
                f"Section id: {section['section_id']} "
                f"(kind={section['section_kind']})\n\n"
                f"---\n{section['body']}\n---\n\n"
                f"Return hypotheses as a JSON array per the system schema."
            )
            jobs.append(
                ModelCallJob(
                    system=_SYSTEM_PROMPT,
                    user=user_prompt,
                    typed_input={
                        "section_id": section["section_id"],
                        "paper_ref": section["paper_ref"],
                        "rendered_user": user_prompt,
                    },
                    item_ref=section["section_id"],
                    fragment_id=f"hypothesis:{section['section_id']}",
                )
            )
        outcomes = self._complete_many(conn, jobs, generation_id=generation_id)

        for section, outcome in zip(usable, outcomes):
            section_id = section["section_id"]
            paper_ref = section["paper_ref"]
            if not outcome.ok:
                # Failed, refused, or no replay record. All three make this
                # complete-producer run incomplete and HOLD the watermark,
                # preserving the prior snapshot. A replay miss is also ledgered
                # as an exclusion; replay never samples fresh.
                continue
            try:
                parsed_hypotheses = parse_llm_hypotheses_response(
                    outcome.content,
                    strict=True,
                )
            except IncompleteExtractionResponse as exc:
                self._record_input_failure(section_id, exc)
                continue
            self._mark_input_parsed(section_id)
            for h in parsed_hypotheses:
                yield ExtractionCandidate(
                    candidate_text=h["statement"],
                    confidence=h["confidence"],
                    source_node_ref=section_id,
                    extra={
                        "paper_ref": paper_ref,
                        "author_ref": section.get("author_ref"),
                        "section_kind": section["section_kind"],
                        "predicts_about_concept_names": h[
                            "predicts_about_concept_names"
                        ],
                    },
                )

    def _fetch_intro_discussion_sections(
        self,
        conn: psycopg.Connection,
        generation_id: int,
    ) -> list[dict[str, Any]]:
        # Per dogfood-canary follow-up (2026-05-19): section nodes
        # don't carry the body text directly — the LaTeX/PDF
        # structural extractors emit body content into CHILD
        # paragraph nodes (via section -contains-> paragraph).
        # The hypothesis_extractor's input is the section's prose;
        # aggregate the child paragraphs' `text` in ordinal order
        # so the extractor has something to reason over.
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        rows = graph.query(
            """
            SELECT s.node_id                AS section_id,
                   s.attrs->>'section_kind' AS section_kind,
                   coalesce(
                       string_agg(
                           p.attrs->>'text',
                           E'\n\n'
                           ORDER BY (p.attrs->>'ordinal')::int
                       ) FILTER (WHERE p.node_id IS NOT NULL),
                       ''
                   )                        AS body,
                   s.attrs->>'paper_ref'    AS paper_ref,
                   s.attrs->>'author_ref'   AS author_ref
              FROM okg.graph_nodes s
              LEFT JOIN okg.graph_edges e
                ON e.src = s.node_id
               AND e.edge_type = 'contains'
              LEFT JOIN okg.graph_nodes p
                ON p.node_id = e.dst
               AND p.subtype = 'paragraph'
             WHERE s.subtype = 'paper_section'
               AND s.attrs->>'section_kind' = ANY(:kinds)
             GROUP BY s.node_id, s.attrs
            """,
            {"kinds": list(_HYPOTHESIS_SECTION_KINDS)},
        )
        return [
            {
                "section_id": str(row["section_id"]),
                "section_kind": str(row["section_kind"] or ""),
                "body": str(row["body"] or ""),
                "paper_ref": str(row["paper_ref"] or ""),
                "author_ref": str(row["author_ref"] or "") or None,
            }
            for row in rows
        ]

    def emit_typed_node(
        self,
        conn: psycopg.Connection,
        candidate: ExtractionCandidate,
        *,
        generation_id: int,
        run_id: str,
        observed_at: datetime,
    ) -> int:
        x = candidate.extra
        paper_ref = x.get("paper_ref")
        if not paper_ref:
            raise IncompleteExtractionResponse(
                "hypothesis candidate had no paper_ref at emission"
            )
        # Deterministic hypothesis_id from (paper, statement).
        import hashlib as _h

        payload = "|".join(
            (
                str(paper_ref),
                candidate.candidate_text,
            )
        ).encode("utf-8")
        slug = _h.sha256(payload).hexdigest()[:16]
        hypothesis_id = f"hyp:extracted-{slug}"
        node_attrs: dict[str, Any] = {
            "hypothesis_id": hypothesis_id,
            "statement": candidate.candidate_text,
            "status": "open",
            "source_subtype": "agent_derived",
            "source_path": f"extractor://{self.name}/{paper_ref}",
            "frontmatter_schema_version": 1,
            "extractor": self.name,
            "extractor_confidence": float(candidate.confidence),
            "extractor_floor_at_emit": self.confidence_floor,
            "provenance": "extracted_model",
            "confidence": float(candidate.confidence),
            "extracted_from_paper_ref": paper_ref,
            "author_ref": x.get("author_ref") or None,
            "created_at": observed_at.isoformat(),
            "last_modified_at": observed_at.isoformat(),
        }
        # Migrated to the LLMExtractor helper (6fe5d07a).
        self._emit_node_fact(
            conn,
            node_id=hypothesis_id,
            subtype="hypothesis",
            attrs=node_attrs,
            dedupe_key=make_dedupe_key(
                "hypothesis_node",
                hypothesis_id,
                self.name,
            ),
            source_record_id={
                "enricher": self.name,
                "section_id": candidate.source_node_ref,
                "paper_ref": paper_ref,
            },
            source_revision={
                "run_id": run_id,
                "generation_id": generation_id,
            },
            # PACT bind-derived-node-provenance: extracted from the section
            # node the extractor was handed.
            derived_from=[candidate.source_node_ref],
            run_id=run_id,
            observed_at=observed_at,
        )
        n_edges = 0
        # Hypothesis predicts_about Concept (per-named-concept).
        # Concept slug derived the same way as concept_extractor —
        # cross-paper dedup matches.
        from okg.substrate.library.enrichers.extraction.concept_extractor import (
            slugify_concept_name,
        )

        for concept_name in x.get("predicts_about_concept_names") or []:
            concept_id = f"concept:{slugify_concept_name(concept_name)}"
            n_edges += self._insert_edge(
                conn,
                src=hypothesis_id,
                dst=concept_id,
                edge_type="predicts_about",
                confidence=float(candidate.confidence),
                run_id=run_id,
                generation_id=generation_id,
                observed_at=observed_at,
            )
        return n_edges

    def _insert_edge(
        self,
        conn: psycopg.Connection,
        *,
        src: str,
        dst: str,
        edge_type: str,
        confidence: float,
        run_id: str,
        generation_id: int,
        observed_at: datetime,
    ) -> int:
        # Migrated to the LLMExtractor helper (6fe5d07a).
        self._emit_edge_fact(
            conn,
            src=src,
            dst=dst,
            edge_type=edge_type,
            attrs={
                "reason": "hypothesis_extractor_emission",
                "extractor": self.name,
            },
            provenance="extracted_model",
            confidence=confidence,
            dedupe_key=make_dedupe_key(
                "hypothesis_edge",
                src,
                edge_type,
                dst,
                self.name,
            ),
            source_record_id={"enricher": self.name},
            source_revision={
                "run_id": run_id,
                "generation_id": generation_id,
            },
            run_id=run_id,
            observed_at=observed_at,
        )
        return 1
