"""ClaimExtractor — LLM-driven Claim extraction from A1 anatomy.

Per ``extend-substrate-extraction-with-claim-evidence-extractors``
spec.md §"Claim Extractor Enricher":

- Reads ``Paragraph.text`` + ``Theorem.statement`` + ``Table.caption``
  (A1 subtypes) at the pinned generation.
- For each input row, prompts the LLM to extract typed Claim
  candidates with ``kind ∈ {definitional, empirical, comparative,
  theoretical, conjectural}`` and per-claim confidence in [0, 1].
- Above-floor candidates emit one ``Claim`` NodeFact + the
  corresponding cross-link edge (``Paragraph contains Claim``,
  ``Theorem states Claim``, ``Table states Claim``).
- Below-floor candidates emit ``ExtractionDropped`` via the base
  class.

Default ``confidence_floor`` per spec: 0.6 (high; claims are
dense; high floor keeps low-quality noise out of the most-traversed
surface). Deployments override in their ``enrichers:`` block.

LLM-cost discipline (per impl brief): unit tests pass a stub LLM
that returns canned JSON; integration tests run against a small
fixture paper with one real LLM call gated on
``OKG_TEST_REAL_LLM=1``.
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


__all__ = ["ClaimExtractor", "parse_llm_claims_response"]


_VALID_KINDS = frozenset(
    {
        "definitional",
        "empirical",
        "comparative",
        "theoretical",
        "conjectural",
    }
)


_SYSTEM_PROMPT = """\
You extract typed claims from scientific-paper text.

Return ONLY a JSON array of objects, each object having:
- text: string (a single self-contained assertion)
- kind: one of definitional / empirical / comparative / theoretical / conjectural
- confidence: float in [0, 1] (your confidence in the extraction)

Rules:
- Each claim must be a single assertion present in the input
  text. Do not paraphrase beyond what the text supports.
- Emit zero claims (an empty JSON array `[]`) when the input is
  a non-claim paragraph (e.g. acknowledgements, table of contents).
- Do not extract opinions, recommendations, or future work.
- Wrap the JSON array in ```json ... ``` if it helps you stay
  grounded; the parser strips fenced blocks before parsing.
"""


def parse_llm_claims_response(
    raw: str,
    *,
    strict: bool = False,
) -> list[dict[str, Any]]:
    """Parse the LLM's response into a list of claim dicts.

    Tolerates:
    - Bare JSON array.
    - Fenced JSON in markdown (```json ... ```).
    - Leading / trailing whitespace.

    Returns ``[]`` for unparseable output (no silent typed-node
    emission). Logs a WARNING for malformed responses.
    """

    def invalid(message: str) -> None:
        if strict:
            raise IncompleteExtractionResponse(message)
        logger.warning("claim_extractor: %s", message)

    if not raw:
        invalid("LLM response was empty")
        return []
    text = raw.strip()
    # Strip ```json ... ``` or ``` ... ``` fences
    if text.startswith("```"):
        # find closing fence
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
        if not isinstance(item.get("text"), str) or not item["text"].strip():
            invalid(f"candidate {index} had no non-empty text")
            continue
        kind = item.get("kind")
        if kind not in _VALID_KINDS:
            invalid(f"candidate {index} had unknown kind={kind!r}")
            continue
        conf = item.get("confidence")
        try:
            conf_f = float(conf)
        except (TypeError, ValueError):
            invalid(f"candidate {index} had invalid confidence={conf!r}")
            continue
        if not (0.0 <= conf_f <= 1.0):
            invalid(f"candidate {index} had out-of-range confidence={conf_f!r}")
            continue
        out.append(
            {
                "text": item["text"].strip(),
                "kind": kind,
                "confidence": conf_f,
            }
        )
    return out


class ClaimExtractor(LLMExtractor):
    """Emit typed ``Claim`` nodes from A1's paragraph / theorem /
    table text via an LLM-driven extractor.

    See module docstring for default floor + dependencies. Deployments
    enable by listing in ``deployment.yaml`` ``enrichers:``:

    ```yaml
    enrichers:
      - class: okg.substrate.library.enrichers.extraction.ClaimExtractor
        config:
          confidence_floor: 0.6
          llm_model: claude-sonnet-4-5-20250929
    ```
    """

    name = "claim_extractor"
    reads_subtypes = ("paragraph", "theorem", "table")
    emits_edge_types = ("contains", "states")
    requires_narrowings = (
        ("paragraph", "contains", "claim"),
        ("theorem", "states", "claim"),
        ("table", "states", "claim"),
    )
    emits_typed_subtype = "claim"
    # PACT bind-derived-node-provenance: every node this extractor
    # emits declares the node it was extracted from, so it does not
    # land with an empty (fail-closed, read-invisible) lineage envelope.
    declares_node_provenance = True
    # Every node this extractor emits is derived from the one source node
    # it was handed, so it stays live only while that input does.
    support_policy: DerivedSupportPolicy = "all_members"
    # Closed response vocabulary, declared into the write envelope
    # (spec §9.3): the claim kind enum is part of what this producer may
    # write, not an incidental parse detail.
    envelope_value_vocabularies = {"kind": tuple(sorted(_VALID_KINDS))}
    # Per dogfood-canary follow-up gap #5: skip re-invoking the LLM
    # when no new paragraph/theorem/table facts arrived since the
    # prior run. Without this every cycle re-emits the same claims
    # with fresh observed_at and churns nodes_added/retracted.
    publisher_skip_when_clean = True
    snapshot_replacement_mode = "complete_producer"

    # The text attribute holding the body the LLM reads, per src
    # subtype. Subclasses / config can override.
    _BODY_ATTR_BY_SUBTYPE = {
        "paragraph": "text",
        "theorem": "statement",
        "table": "caption",
    }
    # Which edge type to emit, per src subtype.
    _EDGE_TYPE_BY_SUBTYPE = {
        "paragraph": "contains",
        "theorem": "states",
        "table": "states",
    }

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
        sources = list(self._fetch_source_rows(conn, generation_id))
        if not sources:
            logger.info(
                "%s: zero source rows at generation %d (A1 not loaded "
                "or no paragraph/theorem/table nodes yet); skipping",
                self.name,
                generation_id,
            )
            return
        logger.info(
            "%s: extracting from %d source rows at generation %d",
            self.name,
            len(sources),
            generation_id,
        )
        from okg.substrate.llm.fanout import ModelCallJob

        # Build the whole batch, then hand it to the shared fan-out: provider
        # round-trips overlap, while outcome recording and input-failure
        # accounting stay serial on this connection. The per-item shape below
        # is unchanged — only the dispatch is batched.
        rows = [(sid, sub, body) for sid, sub, body in sources if body]
        jobs = [
            ModelCallJob(
                system=_SYSTEM_PROMPT,
                user=(
                    f"Source kind: {subtype}\n"
                    f"Source id: {src_node_id}\n"
                    f"\n---\n{body}\n---\n\n"
                    f"Extract typed claims as a JSON array per the system "
                    f"prompt's schema."
                ),
                typed_input={
                    "source_node_id": src_node_id,
                    "source_subtype": subtype,
                    "body": body,
                },
                item_ref=src_node_id,
                fragment_id=f"claim:{src_node_id}",
            )
            for src_node_id, subtype, body in rows
        ]
        outcomes = self._complete_many(conn, jobs, generation_id=generation_id)

        for (src_node_id, subtype, _body), outcome in zip(rows, outcomes):
            if not outcome.ok:
                # The call failed, was refused, or replay had no record. All
                # three make this complete-producer run incomplete and the
                # publisher HOLDS the watermark, preserving the prior snapshot.
                # A replay miss is also ledgered as an exclusion; replay never
                # samples fresh.
                continue
            try:
                parsed_claims = parse_llm_claims_response(
                    outcome.content,
                    strict=True,
                )
            except IncompleteExtractionResponse as exc:
                self._record_input_failure(src_node_id, exc)
                continue
            self._mark_input_parsed(src_node_id)
            for claim in parsed_claims:
                yield ExtractionCandidate(
                    candidate_text=claim["text"],
                    confidence=claim["confidence"],
                    source_node_ref=src_node_id,
                    extra={
                        "kind": claim["kind"],
                        "src_subtype": subtype,
                    },
                )

    def _fetch_source_rows(
        self,
        conn: psycopg.Connection,
        generation_id: int,
    ) -> list[tuple[str, str, str]]:
        """Return ``(node_id, subtype, body)`` rows for paragraph /
        theorem / table at the pinned generation.

        Body field per-subtype follows :attr:`_BODY_ATTR_BY_SUBTYPE`.
        """
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        rows = [
            (
                row["node_id"],
                row["subtype"],
                row["text_attr"],
                row["statement_attr"],
                row["caption_attr"],
            )
            for row in graph.query(
                """
                SELECT node_id, subtype,
                       attrs->>'text' AS text_attr,
                       attrs->>'statement' AS statement_attr,
                       attrs->>'caption' AS caption_attr
                  FROM okg.graph_nodes
                 WHERE subtype = ANY(:subs)
                """,
                {"subs": list(self.reads_subtypes)},
            )
        ]
        out: list[tuple[str, str, str]] = []
        for node_id, subtype, text_attr, statement_attr, caption_attr in rows:
            body_attr = self._BODY_ATTR_BY_SUBTYPE.get(subtype)
            if body_attr == "text":
                body = text_attr
            elif body_attr == "statement":
                body = statement_attr
            elif body_attr == "caption":
                body = caption_attr
            else:
                continue
            if not body:
                continue
            out.append((str(node_id), str(subtype), str(body)))
        return out

    def emit_typed_node(
        self,
        conn: psycopg.Connection,
        candidate: ExtractionCandidate,
        *,
        generation_id: int,
        run_id: str,
        observed_at: datetime,
    ) -> int:
        kind = candidate.extra.get("kind")
        src_subtype = candidate.extra.get("src_subtype")
        if kind not in _VALID_KINDS or src_subtype not in self.reads_subtypes:
            raise IncompleteExtractionResponse(
                "claim candidate had invalid emission payload "
                f"(kind={kind!r}, src_subtype={src_subtype!r})"
            )
        edge_type = self._EDGE_TYPE_BY_SUBTYPE.get(src_subtype, "contains")
        # Deterministic claim_id from (source, text, kind) so the
        # same paragraph + same candidate text reproduces the same
        # node id across runs.
        digest_payload = "|".join(
            (
                candidate.source_node_ref,
                kind,
                candidate.candidate_text,
            )
        ).encode("utf-8")
        import hashlib as _h

        claim_id = "claim:" + _h.sha256(digest_payload).hexdigest()[:16]
        node_attrs: dict[str, Any] = {
            "claim_id": claim_id,
            "text": candidate.candidate_text,
            # Keep the assertion in `text_blob` (the schema's "richer body"
            # field). The projector rebuilds `text` as a searchable rollup of the
            # OTHER declared searchable fields (kind/tags/text_blob), so without a
            # populated text_blob the assertion is dropped from the live graph's
            # `text`. text_blob is un-rolled, giving consumers a clean copy.
            "text_blob": candidate.candidate_text,
            "kind": kind,
            "extractor": self.name,
            "extractor_confidence": float(candidate.confidence),
            "extractor_floor_at_emit": self.confidence_floor,
            "provenance": "extracted_model",
            "confidence": float(candidate.confidence),
            "created_at": observed_at.isoformat(),
            "last_modified_at": observed_at.isoformat(),
        }
        # Migrated to the LLMExtractor helper methods (6fe5d07a)
        # so the projector pattern (deterministic edge_id, source,
        # run_id) lives in one place. Per dogfood-canary follow-up.
        common_provenance = {
            "enricher": self.name,
            "source_node_ref": candidate.source_node_ref,
        }
        common_revision = {
            "run_id": run_id,
            "generation_id": generation_id,
        }
        self._emit_node_fact(
            conn,
            node_id=claim_id,
            subtype="claim",
            attrs=node_attrs,
            dedupe_key=make_dedupe_key("claim", claim_id, self.name),
            source_record_id=common_provenance,
            source_revision=common_revision,
            run_id=run_id,
            observed_at=observed_at,
            # PACT bind-derived-node-provenance: the claim is extracted from
            # the node the extractor was handed.
            derived_from=[candidate.source_node_ref],
        )
        self._emit_edge_fact(
            conn,
            src=candidate.source_node_ref,
            dst=claim_id,
            edge_type=edge_type,
            attrs={
                "reason": "claim_extractor_emission",
                "extractor": self.name,
            },
            provenance="extracted_model",
            confidence=float(candidate.confidence),
            dedupe_key=make_dedupe_key(
                "claim_edge",
                candidate.source_node_ref,
                edge_type,
                claim_id,
            ),
            source_record_id=common_provenance,
            source_revision=common_revision,
            run_id=run_id,
            observed_at=observed_at,
        )
        return 1
