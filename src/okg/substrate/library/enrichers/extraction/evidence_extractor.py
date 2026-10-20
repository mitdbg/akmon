"""EvidenceExtractor — LLM-driven Evidence extraction.

Per ``extend-substrate-extraction-with-claim-evidence-extractors``
spec.md §"Evidence Extractor Enricher" + design.md D1:

- Reads each ``extraction.Claim`` plus its containing ``Paragraph``
  (A1) plus the ``Citation`` nodes (A1) adjacent in the paper's
  structural neighborhood.
- Emits ``reasoning.Evidence`` nodes (reused subtype per D1, NOT
  a new ``extraction.ExtractedEvidence``) with paper-extraction
  provenance:
  - ``hypothesis_ref`` = the Claim node id (semantics widen from
    "Hypothesis only" to "Hypothesis-shaped target").
  - ``observation_ref`` = the source Paragraph node id.
  - ``support_polarity`` ∈ {supports, contradicts}.
  - ``extracted_from_paper_ref``, ``author_ref``, ``confidence``,
    ``provenance: extracted_model``.
- Cross-link edges:
  - ``Claim supported_by Evidence`` (intra-extraction; this child's
    own narrowing).
  - ``Evidence cites <Citation | Theorem | Table | Figure>``
    (cross-module; depends on A1 having the cited subtypes
    declared. The narrowing slot names come from A1).

Default ``confidence_floor`` per spec: 0.6 (high; evidence is the
load-bearing link; spurious evidence corrupts the reasoning trail).
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


__all__ = ["EvidenceExtractor", "parse_llm_evidence_response"]


_VALID_POLARITIES = frozenset({"supports", "contradicts"})


_SYSTEM_PROMPT = """\
You judge whether candidate citations / theorems / tables / figures
provide evidence for a specific claim.

You are given:
- the claim text
- the containing paragraph
- a list of candidate evidence items (each with id, kind, and a
  short label)

Return ONLY a JSON array of objects with:
- evidence_ref: the id of the evidence item that supports or
  contradicts the claim
- support_polarity: supports | contradicts
- confidence: float in [0, 1] (your confidence that this evidence
  item actually backs the claim)

Rules:
- Emit zero entries (`[]`) when none of the candidates relate to
  the claim.
- One candidate id may appear at most once.
- Wrap in ```json``` if useful; the parser strips fences.
"""


def parse_llm_evidence_response(
    raw: str,
    *,
    strict: bool = False,
) -> list[dict[str, Any]]:
    """Parse the LLM response into evidence-link dicts. Tolerates
    fenced JSON; validates ``support_polarity``; drops bad rows.
    """

    def invalid(message: str) -> None:
        if strict:
            raise IncompleteExtractionResponse(message)
        logger.warning("evidence_extractor: %s", message)

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
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for index, item in enumerate(parsed):
        if not isinstance(item, dict):
            invalid(f"candidate {index} was not an object")
            continue
        ref = item.get("evidence_ref")
        if not isinstance(ref, str) or not ref.strip():
            invalid(f"candidate {index} had no non-empty evidence_ref")
            continue
        ref_clean = ref.strip()
        if ref_clean in seen:
            invalid(f"candidate {index} duplicated evidence_ref={ref_clean!r}")
            continue
        pol = item.get("support_polarity")
        if pol not in _VALID_POLARITIES:
            invalid(f"candidate {index} had unknown support_polarity={pol!r}")
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
        seen.add(ref_clean)
        out.append(
            {
                "evidence_ref": ref_clean,
                "support_polarity": pol,
                "confidence": conf_f,
            }
        )
    return out


class EvidenceExtractor(LLMExtractor):
    """Emit ``reasoning.Evidence`` nodes + Claim-supported_by-Evidence
    + Evidence-cites-(Citation|Theorem|Table|Figure) edges.
    """

    name = "evidence_extractor"
    reads_subtypes = ("claim", "paragraph", "citation", "theorem", "table", "figure")
    emits_edge_types = ("supported_by", "cites")
    requires_narrowings = (
        ("claim", "supported_by", "evidence"),
        ("evidence", "cites", "citation"),
        ("evidence", "cites", "theorem"),
        ("evidence", "cites", "table"),
        ("evidence", "cites", "figure"),
    )
    emits_typed_subtype = "evidence"
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

    def derivation_refs(self, candidate: ExtractionCandidate) -> list[str]:
        """The paragraph read and the citable node selected — NOT the claim.

        PACT bind-derived-node-provenance. This extractor sets
        ``source_node_ref`` to the claim id, which is what the evidence
        ASSERTS, not where it came from; inheriting from it would give this
        row the claim's trust. The evidence is read out of the containing
        paragraph, about the citable node the model picked, so those are its
        real inputs.

        A missing ref fails the WHOLE declaration rather than narrowing it to
        the refs that happen to be present: a partial declaration is exactly
        the selective-omission shape that inherits more trust than it earned,
        and it would arrive silently. Declaring nothing fails closed loudly.
        """
        refs = [candidate.extra.get("paragraph_id"), candidate.extra.get("citable_id")]
        if not all(refs):
            logger.warning(
                "%s: incomplete derivation refs for candidate on claim=%s "
                "(paragraph_id=%r, citable_id=%r); declaring no provenance "
                "rather than a partial one, so the node fails closed",
                self.name,
                candidate.source_node_ref,
                refs[0],
                refs[1],
            )
            return []
        return [str(ref) for ref in refs]

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
        claims = self._fetch_claims_with_paragraphs(conn, generation_id)
        if not claims:
            logger.info(
                "%s: zero claims at generation %d; skipping",
                self.name,
                generation_id,
            )
            return
        from okg.substrate.llm.fanout import ModelCallJob

        # The adjacency lookup reads `conn`, so it stays HERE, before the
        # fan-out — phase 1 must touch no connection. Only the provider calls
        # are batched.
        prepared: list[tuple[dict, list, ModelCallJob]] = []
        for claim in claims:
            paragraph_id = claim["paragraph_id"]
            adjacent = self._fetch_adjacent_citables(
                conn,
                generation_id,
                paragraph_id=paragraph_id,
                paper_ref=claim["paper_ref"],
            )
            if not adjacent:
                continue
            candidates_block = "\n".join(
                f"- id={a['node_id']} kind={a['subtype']} label={a['label']!r}"
                for a in adjacent
            )
            user_prompt = (
                f"Claim: {claim['claim_text']}\n\n"
                f"Containing paragraph (id={paragraph_id}):\n"
                f"{claim['paragraph_body']}\n\n"
                f"Candidate evidence items:\n{candidates_block}\n\n"
                f"Return JSON array per the system schema."
            )
            prepared.append(
                (
                    claim,
                    adjacent,
                    ModelCallJob(
                        system=_SYSTEM_PROMPT,
                        user=user_prompt,
                        typed_input={
                            "claim_id": claim["claim_id"],
                            "paragraph_id": paragraph_id,
                            "rendered_user": user_prompt,
                        },
                        item_ref=claim["claim_id"],
                        fragment_id=f"evidence:{claim['claim_id']}",
                    ),
                )
            )

        outcomes = self._complete_many(
            conn,
            [job for _, _, job in prepared],
            generation_id=generation_id,
        )

        for (claim, adjacent, _job), outcome in zip(prepared, outcomes):
            claim_id = claim["claim_id"]
            if not outcome.ok:
                # Failed, refused, or no replay record. All three make this
                # complete-producer run incomplete and HOLD the watermark,
                # preserving the prior snapshot. A replay miss is also ledgered
                # as an exclusion; replay never samples fresh.
                continue
            try:
                parsed_links = parse_llm_evidence_response(
                    outcome.content,
                    strict=True,
                )
            except IncompleteExtractionResponse as exc:
                self._record_input_failure(claim_id, exc)
                continue
            self._mark_input_parsed(claim_id)
            citables_by_id = {a["node_id"]: a for a in adjacent}
            paragraph_id = claim["paragraph_id"]
            for link in parsed_links:
                ref = link["evidence_ref"]
                citable = citables_by_id.get(ref)
                if citable is None:
                    self._record_input_failure(
                        claim_id,
                        IncompleteExtractionResponse(
                            f"LLM cited unknown evidence_ref {ref!r}"
                        ),
                    )
                    continue
                yield ExtractionCandidate(
                    candidate_text=(
                        f"{link['support_polarity']} of claim "
                        f"{claim_id} via {citable['subtype']} "
                        f"{citable['node_id']}"
                    ),
                    confidence=link["confidence"],
                    source_node_ref=claim_id,
                    extra={
                        "claim_id": claim_id,
                        "paragraph_id": paragraph_id,
                        "citable_id": citable["node_id"],
                        "citable_subtype": citable["subtype"],
                        "support_polarity": link["support_polarity"],
                        "paper_ref": claim["paper_ref"],
                        "author_ref": claim.get("author_ref"),
                    },
                )

    # ------------------------------------------------------------------
    # DB reads
    # ------------------------------------------------------------------

    def _fetch_claims_with_paragraphs(
        self,
        conn: psycopg.Connection,
        generation_id: int,
    ) -> list[dict[str, Any]]:
        """Return Claim rows joined to their containing Paragraph
        (via the `Paragraph contains Claim` edge) and the Paper /
        Author refs from the Paragraph's attrs."""
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        rows = graph.query(
            """
            SELECT c.node_id   AS claim_id,
                   c.attrs->>'text'              AS claim_text,
                   c.attrs->>'extracted_from_paper_ref' AS claim_paper_ref,
                   p.node_id   AS paragraph_id,
                   p.attrs->>'text'              AS paragraph_body,
                   p.attrs->>'paper_ref'         AS paper_ref,
                   p.attrs->>'author_ref'        AS author_ref
              FROM okg.graph_nodes c
              JOIN okg.graph_edges e
                ON e.dst = c.node_id
               AND e.edge_type = 'contains'
              JOIN okg.graph_nodes p
                ON p.node_id = e.src
               AND p.subtype = 'paragraph'
             WHERE c.subtype = 'claim'
            """
        )
        return [
            {
                "claim_id": str(row["claim_id"]),
                "claim_text": str(row["claim_text"] or ""),
                "paragraph_id": str(row["paragraph_id"]),
                "paragraph_body": str(row["paragraph_body"] or ""),
                "paper_ref": str(row["paper_ref"] or row["claim_paper_ref"] or ""),
                "author_ref": str(row["author_ref"] or "") or None,
            }
            for row in rows
        ]

    def _fetch_adjacent_citables(
        self,
        conn: psycopg.Connection,
        generation_id: int,
        *,
        paragraph_id: str,
        paper_ref: str,
    ) -> list[dict[str, Any]]:
        """Return Citation / Theorem / Table / Figure rows the LLM
        should consider as candidate evidence for a claim. Scoped to
        the same Paper (via attrs.paper_ref) so the search space stays
        bounded."""
        if not paper_ref:
            return []
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        rows = graph.query(
            """
            SELECT node_id, subtype,
                   coalesce(
                     attrs->>'label',
                     attrs->>'caption',
                     attrs->>'statement',
                     attrs->>'text',
                     ''
                   ) AS label
              FROM okg.graph_nodes
             WHERE subtype = ANY(:kinds)
               AND attrs->>'paper_ref' = :paper
            """,
            {
                "kinds": ["citation", "theorem", "table", "figure"],
                "paper": paper_ref,
            },
        )
        return [
            {
                "node_id": str(row["node_id"]),
                "subtype": str(row["subtype"]),
                "label": str(row["label"]),
            }
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Emission
    # ------------------------------------------------------------------

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
        if x["support_polarity"] not in _VALID_POLARITIES:
            raise IncompleteExtractionResponse(
                "evidence candidate had invalid emission polarity "
                f"{x['support_polarity']!r}"
            )
        # Deterministic evidence_id from (claim, citable, polarity).
        import hashlib as _h

        payload = "|".join(
            (
                str(x["claim_id"]),
                str(x["citable_id"]),
                str(x["support_polarity"]),
            )
        ).encode("utf-8")
        evidence_id = "ev:" + _h.sha256(payload).hexdigest()[:16]
        node_attrs: dict[str, Any] = {
            "evidence_id": evidence_id,
            "hypothesis_ref": x["claim_id"],
            "observation_ref": x["paragraph_id"],
            "support_polarity": x["support_polarity"],
            "extractor": self.name,
            "extractor_confidence": float(candidate.confidence),
            "extractor_floor_at_emit": self.confidence_floor,
            "provenance": "extracted_model",
            "confidence": float(candidate.confidence),
            "extracted_from_paper_ref": x.get("paper_ref") or None,
            "author_ref": x.get("author_ref") or None,
            "created_at": observed_at.isoformat(),
            "last_modified_at": observed_at.isoformat(),
        }
        # Migrated to the LLMExtractor helper (6fe5d07a).
        self._emit_node_fact(
            conn,
            node_id=evidence_id,
            subtype="evidence",
            attrs=node_attrs,
            dedupe_key=make_dedupe_key(
                "evidence_node",
                evidence_id,
                self.name,
            ),
            source_record_id={
                "enricher": self.name,
                "claim_id": x["claim_id"],
            },
            source_revision={
                "run_id": run_id,
                "generation_id": generation_id,
            },
            run_id=run_id,
            observed_at=observed_at,
            derived_from=self.derivation_refs(candidate),
        )
        n_edges = 0
        # Claim supported_by Evidence (intra-extraction; A2 owns)
        n_edges += self._insert_edge(
            conn,
            src=x["claim_id"],
            dst=evidence_id,
            edge_type="supported_by",
            confidence=float(candidate.confidence),
            run_id=run_id,
            generation_id=generation_id,
            observed_at=observed_at,
        )
        # Evidence cites <Citation | Theorem | Table | Figure>
        n_edges += self._insert_edge(
            conn,
            src=evidence_id,
            dst=x["citable_id"],
            edge_type="cites",
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
                "reason": "evidence_extractor_emission",
                "extractor": self.name,
            },
            provenance="extracted_model",
            confidence=confidence,
            dedupe_key=make_dedupe_key(
                "evidence_edge",
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
