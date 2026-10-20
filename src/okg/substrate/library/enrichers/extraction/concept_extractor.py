"""ConceptExtractor — LLM-driven Concept extraction from A1 anatomy.

Per ``extend-substrate-extraction-with-claim-evidence-extractors``
spec.md §"Concept Extractor Enricher":

- Reads ``Paper.title`` + ``Paper.abstract`` + all
  ``PaperSection.body`` at the pinned generation.
- Prompts the LLM to extract concepts with a relationship
  classification ``{defines, uses, refines}``:
  - ``defines`` — the paper introduces this concept.
  - ``uses`` — the paper references an existing concept.
  - ``refines`` — the paper refines a previously-defined
    concept (carries a `refines_concept_name` reference).
- Above-floor candidates: emit ``concepts.Concept`` NodeFact +
  ``Paper defines/uses Concept`` edge. For ``refines``,
  additionally emit ``Concept refines Concept`` when the
  referenced concept resolves via alias.
- Below-floor candidates emit ``ExtractionDropped`` via base.

Default ``confidence_floor`` per spec: 0.5 (concept extraction
is recall-sensitive; missing a concept costs more than emitting
one slightly-uncertain because operator merges near-duplicates
via the alias path per umbrella D6).

Cross-link rule narrowings required from A3:
  (paper, defines, concept), (paper, uses, concept),
  (concept, refines, concept).
When A3 hasn't landed those narrowings, the substrate's
narrowing trigger rejects the edges at admission and the
enricher logs the rejection. The finding
`findings/2026-05-18-a1-a3-not-yet-implemented.md` records
this dependency.
"""

from __future__ import annotations

import json
import logging
import re
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


__all__ = ["ConceptExtractor", "parse_llm_concepts_response", "slugify_concept_name"]


_VALID_RELATIONSHIPS = frozenset({"defines", "uses", "refines"})


_SYSTEM_PROMPT = """\
You extract concepts from scientific-paper text.

A concept is a named technical idea the paper either:
- defines (paper introduces it — typically in abstract or early sections)
- uses (paper references an existing idea)
- refines (paper extends or restricts a previously-defined concept)

Return ONLY a JSON array of objects with:
- name: short canonical noun phrase (e.g. "two-phase commit",
  "self-attention", "branch coverage")
- relationship: defines | uses | refines
- description: one sentence summarizing the concept
- confidence: float in [0, 1] (your confidence in the extraction)
- refines_concept_name: required if relationship=refines, naming
  the concept being refined; omit otherwise.

Rules:
- Skip generic English nouns ("paper", "method", "result").
- Skip product / company names (those belong elsewhere).
- Emit zero concepts (`[]`) for non-conceptual text.
- Wrap output in ```json``` if useful; the parser strips fences.
"""


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify_concept_name(name: str) -> str:
    """Canonical slug for a concept name: lowercased, alphanumeric
    + hyphens, no leading/trailing hyphens. Multiple papers that
    extract the same name produce the same slug, giving natural
    cross-paper dedup at the alias layer.
    """
    s = _SLUG_RE.sub("-", name.lower()).strip("-")
    return s or "untitled-concept"


def parse_llm_concepts_response(
    raw: str,
    *,
    strict: bool = False,
) -> list[dict[str, Any]]:
    """Parse the LLM's JSON output into a list of concept dicts.

    Tolerates fenced JSON. Returns ``[]`` on malformed output.
    Validates `relationship` against the enum; drops bad rows.
    """

    def invalid(message: str) -> None:
        if strict:
            raise IncompleteExtractionResponse(message)
        logger.warning("concept_extractor: %s", message)

    if not raw:
        invalid("LLM response was empty")
        return []
    text = raw.strip()
    if text.startswith("```"):
        first_nl = text.find("\n")
        if first_nl != -1:
            text = text[first_nl + 1 :]
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
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            invalid(f"candidate {index} had no non-empty name")
            continue
        rel = item.get("relationship")
        if rel not in _VALID_RELATIONSHIPS:
            invalid(f"candidate {index} had unknown relationship={rel!r}")
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
        record = {
            "name": name.strip(),
            "relationship": rel,
            "description": str(item.get("description") or "").strip(),
            "confidence": conf_f,
        }
        if rel == "refines":
            refines_name = item.get("refines_concept_name")
            if not isinstance(refines_name, str) or not refines_name.strip():
                invalid(f"candidate {index} refines without a target concept name")
                continue
            record["refines_concept_name"] = refines_name.strip()
        out.append(record)
    return out


class ConceptExtractor(LLMExtractor):
    """Emit ``concepts.Concept`` nodes + Paper-relationship edges via
    an LLM-driven extractor. See module docstring for inputs +
    defaults + dependencies.
    """

    name = "concept_extractor"
    # Reads Paper-level title/abstract + each PaperSection body.
    reads_subtypes = ("paper", "paper_section")
    # Skip re-running the LLM when the input subtypes haven't
    # changed since the last successful run (no new Paper or
    # PaperSection facts past the prior watermark). Without this,
    # every publish cycle re-invokes the LLM against ALL papers,
    # producing fresh node_facts at fresh observed_at timestamps —
    # the projector then churns `nodes_added: N, nodes_retracted: N`
    # forever. Per dogfood-canary follow-up gap #5 (2026-05-18).
    publisher_skip_when_clean = True
    snapshot_replacement_mode = "complete_producer"
    # The paper-side primary edge uses the LLM's relationship verb
    # for defines/uses; when the LLM says `refines` we anchor the
    # paper with `mentions` (per the catalog: paper->refines->concept
    # is NOT a declared narrowing — refines is concept->concept only)
    # and emit a separate concept->refines->concept edge for the
    # actual refinement claim.
    emits_edge_types = ("defines", "uses", "mentions", "refines")
    requires_narrowings = (
        ("paper", "defines", "concept"),
        ("paper", "uses", "concept"),
        ("paper", "mentions", "concept"),
        ("concept", "refines", "concept"),
    )
    emits_typed_subtype = "concept"
    # PACT bind-derived-node-provenance: every node this extractor
    # emits declares the node it was extracted from, so it does not
    # land with an empty (fail-closed, read-invisible) lineage envelope.
    declares_node_provenance = True
    # Every node this extractor emits is derived from the one source node
    # it was handed, so it stays live only while that input does.
    support_policy: DerivedSupportPolicy = "all_members"

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
        papers = self._fetch_papers(conn, generation_id)
        sections = self._fetch_paper_sections(conn, generation_id)
        if not papers:
            logger.info(
                "%s: zero papers at generation %d; skipping",
                self.name,
                generation_id,
            )
            return
        sections_by_paper: dict[str, list[tuple[str, str]]] = {}
        for paper_ref, section_id, body in sections:
            sections_by_paper.setdefault(paper_ref, []).append((section_id, body))
        logger.info(
            "%s: extracting from %d papers (%d sections) at gen %d",
            self.name,
            len(papers),
            len(sections),
            generation_id,
        )
        from okg.substrate.llm.fanout import ModelCallJob

        # Batched dispatch through the shared fan-out; the per-paper prompt is
        # unchanged. Provider calls overlap, recording stays serial on `conn`.
        prepared: list[tuple[str, ModelCallJob]] = []
        for paper_id, title, abstract in papers:
            paper_sections = sections_by_paper.get(paper_id, [])
            body_blocks = []
            if title:
                body_blocks.append(f"# Title\n{title}")
            if abstract:
                body_blocks.append(f"# Abstract\n{abstract}")
            for sec_id, sec_body in paper_sections:
                if sec_body:
                    body_blocks.append(f"# Section ({sec_id})\n{sec_body}")
            if not body_blocks:
                continue
            user_prompt = (
                f"Paper id: {paper_id}\n\n"
                + "\n\n".join(body_blocks)
                + "\n\nExtract concepts as a JSON array."
            )
            prepared.append(
                (
                    paper_id,
                    ModelCallJob(
                        system=_SYSTEM_PROMPT,
                        user=user_prompt,
                        typed_input={
                            "paper_id": paper_id,
                            "rendered_user": user_prompt,
                        },
                        item_ref=paper_id,
                        fragment_id=f"concept:{paper_id}",
                    ),
                )
            )

        outcomes = self._complete_many(
            conn,
            [job for _, job in prepared],
            generation_id=generation_id,
        )

        for (paper_id, _job), outcome in zip(prepared, outcomes):
            if not outcome.ok:
                # Failed, refused, or no replay record. All three make this
                # complete-producer run incomplete and HOLD the watermark,
                # preserving the prior snapshot. A replay miss is also ledgered
                # as an exclusion; replay never samples fresh.
                continue
            try:
                parsed_concepts = parse_llm_concepts_response(
                    outcome.content,
                    strict=True,
                )
            except IncompleteExtractionResponse as exc:
                self._record_input_failure(paper_id, exc)
                continue
            self._mark_input_parsed(paper_id)
            for c in parsed_concepts:
                extra = {
                    "name": c["name"],
                    "slug": slugify_concept_name(c["name"]),
                    "relationship": c["relationship"],
                    "description": c["description"],
                    "paper_ref": paper_id,
                }
                if c["relationship"] == "refines":
                    extra["refines_concept_name"] = c["refines_concept_name"]
                    extra["refines_concept_slug"] = slugify_concept_name(
                        c["refines_concept_name"]
                    )
                # source_node_ref is the paper id (the candidate's
                # provenance; for dropped records this becomes the
                # source_node_ref).
                yield ExtractionCandidate(
                    candidate_text=f"{c['name']}: {c['description']}",
                    confidence=c["confidence"],
                    source_node_ref=paper_id,
                    extra=extra,
                )

    def _fetch_papers(
        self,
        conn: psycopg.Connection,
        generation_id: int,
    ) -> list[tuple[str, str, str]]:
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        return [
            (str(row["node_id"]), str(row["title"] or ""), str(row["abstract"] or ""))
            for row in graph.query(
                """
                SELECT node_id,
                       attrs->>'title' AS title,
                       attrs->>'abstract' AS abstract
                  FROM okg.graph_nodes
                 WHERE subtype = 'paper'
                """
            )
        ]

    def _fetch_paper_sections(
        self,
        conn: psycopg.Connection,
        generation_id: int,
    ) -> list[tuple[str, str, str]]:
        """Return ``(paper_ref, section_id, body)`` triples.

        Paper-section linkage assumed via ``attrs.paper_ref`` on the
        section subtype (A1 owns the actual schema; this query reads
        the attr by name).
        """
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        return [
            (str(row["paper_ref"] or ""), str(row["node_id"]), str(row["body"] or ""))
            for row in graph.query(
                """
                SELECT attrs->>'paper_ref' AS paper_ref,
                       node_id,
                       attrs->>'body' AS body
                  FROM okg.graph_nodes
                 WHERE subtype = 'paper_section'
                """
            )
            if row["paper_ref"]  # skip orphan sections
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
        slug = candidate.extra["slug"]
        relationship = candidate.extra["relationship"]
        paper_ref = candidate.extra["paper_ref"]
        if relationship not in _VALID_RELATIONSHIPS:
            raise IncompleteExtractionResponse(
                f"concept candidate had invalid emission relationship {relationship!r}"
            )
        concept_id = f"concept:{slug}"
        node_attrs: dict[str, Any] = {
            "concept_id": concept_id,
            "name": candidate.extra["name"],
            "slug": slug,
            "description": candidate.extra["description"],
            "extractor": self.name,
            "extractor_confidence": float(candidate.confidence),
            "extractor_floor_at_emit": self.confidence_floor,
            "provenance": "extracted_model",
            "confidence": float(candidate.confidence),
            "created_at": observed_at.isoformat(),
            "last_modified_at": observed_at.isoformat(),
        }
        # Concept node — same id across papers means natural dedup
        # at the substrate's identity layer (alias prefix is `concept:`).
        # Migrated to use the LLMExtractor helper (6fe5d07a).
        self._emit_node_fact(
            conn,
            node_id=concept_id,
            subtype="concept",
            attrs=node_attrs,
            dedupe_key=make_dedupe_key(
                "concept_node",
                concept_id,
                paper_ref,
                self.name,
            ),
            source_record_id={
                "enricher": self.name,
                "paper_ref": paper_ref,
            },
            source_revision={
                "run_id": run_id,
                "generation_id": generation_id,
            },
            run_id=run_id,
            observed_at=observed_at,
            # PACT bind-derived-node-provenance: the concept is extracted
            # from the paper the candidate came from (paper_ref is the
            # candidate's source_node_ref).
            derived_from=[paper_ref],
        )
        n_edges = 0
        # Paper-side primary edge. The catalog declares
        # paper->defines->concept and paper->uses->concept;
        # `refines` is concept->concept only. When the LLM picks
        # `refines`, anchor the paper to the concept via the
        # honest verb `mentions` (also catalog-declared) and emit
        # the concept->refines->concept claim separately below.
        paper_edge_type = "mentions" if relationship == "refines" else relationship
        n_edges += self._insert_edge(
            conn,
            src=paper_ref,
            dst=concept_id,
            edge_type=paper_edge_type,
            confidence=float(candidate.confidence),
            run_id=run_id,
            generation_id=generation_id,
            observed_at=observed_at,
        )
        # Concept refines Concept (only when relationship=refines)
        if relationship == "refines":
            target_slug = candidate.extra.get("refines_concept_slug")
            if target_slug and target_slug != slug:
                target_id = f"concept:{target_slug}"
                n_edges += self._insert_edge(
                    conn,
                    src=concept_id,
                    dst=target_id,
                    edge_type="refines",
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
                "reason": "concept_extractor_emission",
                "extractor": self.name,
            },
            provenance="extracted_model",
            confidence=confidence,
            dedupe_key=make_dedupe_key(
                "concept_edge",
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
