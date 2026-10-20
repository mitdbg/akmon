"""PdfStructuralEnricher — deterministic PDF paper anatomy.

Reads Paper nodes that carry `attrs.source_pdf_path` and have
not yet been decomposed (no `contains` children). Invokes the
configured PDF parser stack and emits sections / paragraphs /
figures / tables / citations when the per-paper confidence is
≥ `PDF_CONFIDENCE_FLOOR` (0.85). Below the floor, emits exactly
one `extraction_error` Observation per S-A1-R3 and zero anatomy
nodes (the all-or-nothing-or-error invariant from S-A1-R6).

Equations and theorems are out of scope for the PDF path until
Nougat-class extractors mature (design.md Q1).

Architecture:

  - The parser backend is pluggable via the `parser` constructor
    argument: any callable `parser(pdf_path: Path) ->
    PdfParseResult` works. The substrate-shipped default
    (`default_pypdfium2_parser`) uses pypdfium2 for body text +
    a simple sectioning heuristic and ships its own confidence
    formula. Tests inject a deterministic mock; deployments
    swap in GROBID + pdffigures2 in a follow-up child.

  - On confidence ≥ floor, the parser's emitted sections /
    paragraphs / etc. are committed via the shared
    `_commit_anatomy` helper (same code path as
    `latex_structural`).

  - On confidence < floor, no anatomy is committed; one
    `extraction_error` Observation is emitted via
    `_emit_extraction_error`.
"""

from __future__ import annotations

import dataclasses
import hashlib
import logging
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import psycopg

from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.derived_node_liveness import (
    resolve_declared_support_policy,
)
from okg.substrate.library.enrichers import DerivedSupportPolicy
from okg.substrate.file_capability import resolve_within_approved_root
from okg.substrate.library.extraction._pdf_budget import (
    DEFAULT_MAX_PDF_BYTES,
    DEFAULT_MAX_PDF_PAGES,
    capped_page_count,
)
from okg.substrate.library.enrichers.scholarly.latex_structural import (
    ParsedAnatomy,
    ParsedEdge,
    ParsedNode,
    _commit_anatomy,
    _emit_extraction_error,
)

__all__ = [
    "PdfStructuralEnricher",
    "PDF_CONFIDENCE_FLOOR",
    "PdfParseResult",
    "EXTRACTOR_NAME",
    "EXTRACTOR_VERSION",
    "default_pypdfium2_parser",
]

logger = logging.getLogger(__name__)


EXTRACTOR_NAME = "pdf_structural"
EXTRACTOR_VERSION = "1.0.0"
PDF_CONFIDENCE_FLOOR = 0.85


@dataclasses.dataclass(frozen=True)
class PdfParseResult:
    """Output of a PDF parser backend.

    `confidence` in [0.0, 1.0]: 1.0 = perfect parse, 0.0 = totally
    unreadable. The enricher's confidence floor logic uses this.
    `anatomy` carries nodes/edges in the same shape as
    `ParsedAnatomy` (sections / paragraphs / figures / tables /
    citations only — equations + theorems out of scope for PDF).
    `reason` is set when confidence < floor; describes the
    specific parser-side failure (`grobid_low_confidence` /
    `figure_extraction_failed` / `reference_parser_no_match` /
    `pdf_unreadable`).
    """

    confidence: float
    anatomy: Optional[ParsedAnatomy] = None
    reason: Optional[str] = None


def default_pypdfium2_parser(
    *,
    pdf_path: Path,
    paper_id: str,
    max_bytes: int = DEFAULT_MAX_PDF_BYTES,
    max_pages: int = DEFAULT_MAX_PDF_PAGES,
) -> PdfParseResult:
    r"""Default substrate-shipped parser: pypdfium2-based.

    Conservative implementation:
      - Extracts per-page text via pypdfium2.
      - Detects sections heuristically (lines that match
        `^\d+(\.\d+)*\s+[A-Z]`).
      - Splits each section into paragraphs at blank-line
        boundaries.
      - Detects bracketed citations heuristically (`[12]`,
        `[Smith et al. 2020]`).
      - No figure / table extraction without pdffigures2.
      - Confidence formula: 1.0 if ≥ 1 section + ≥ 5 paragraphs
        + ≥ 1 citation; 0.6 if any sections detected but other
        signals absent; 0.0 if pdfplumber raises or no text
        extracted. Floor 0.85 means only the "complete" path
        commits anatomy.

    Production deployments swap this for a GROBID + pdffigures2
    backed parser. The contract is the same: returns
    PdfParseResult.
    """
    try:
        import pypdfium2 as pdfium  # type: ignore[import-untyped]
    except ImportError:
        return PdfParseResult(confidence=0.0, reason="pdf_unreadable")

    # Byte budget: reject an over-cap file before opening / parsing it.
    try:
        if pdf_path.stat().st_size > max_bytes:
            return PdfParseResult(confidence=0.0, reason="pdf_too_large")
    except OSError:
        return PdfParseResult(confidence=0.0, reason="pdf_unreadable")

    try:
        pdf = pdfium.PdfDocument(str(pdf_path))
    except Exception:  # noqa: BLE001
        return PdfParseResult(confidence=0.0, reason="pdf_unreadable")

    full_text_parts: list[str] = []
    try:
        # Page budget: iterate at most max_pages, never the whole document.
        pages_to_parse, _ = capped_page_count(len(pdf), max_pages)
        for i in range(pages_to_parse):
            page = pdf[i]
            text_page = page.get_textpage()
            full_text_parts.append(text_page.get_text_range())
            text_page.close()
            page.close()
    except Exception as e:  # noqa: BLE001
        return PdfParseResult(
            confidence=0.0, reason=f"pdf_unreadable: {e}",
        )
    finally:
        try:
            pdf.close()
        except Exception:  # noqa: BLE001
            pass

    full_text = "\n".join(full_text_parts)
    if not full_text.strip():
        return PdfParseResult(confidence=0.0, reason="pdf_unreadable")

    section_pat = re.compile(
        r"^(\d+(?:\.\d+)*)\s+([A-Z][^\n]{0,80})$", re.MULTILINE,
    )
    citation_pat = re.compile(r"\[(\d+(?:\s*,\s*\d+)*)\]")

    section_matches = list(section_pat.finditer(full_text))
    if not section_matches:
        # No sections found — bail out with a clear reason.
        return PdfParseResult(
            confidence=0.5, reason="reference_parser_no_match",
        )

    nodes: list[ParsedNode] = []
    edges: list[ParsedEdge] = []
    para_idx = 0
    cite_idx = 0

    # Section boundaries.
    section_spans: list[tuple[int, int, str, str]] = []
    for i, m in enumerate(section_matches):
        end = (
            section_matches[i + 1].start()
            if i + 1 < len(section_matches)
            else len(full_text)
        )
        section_spans.append((m.start(), end, m.group(1), m.group(2).strip()))

    for idx, (sec_start, sec_end, path, title) in enumerate(section_spans):
        sec_id = f"section:{paper_id}:{idx}"
        nodes.append(ParsedNode(
            node_id=sec_id, subtype="paper_section",
            attrs={
                "paper_section_id": sec_id,
                "paper_ref": f"paper:{paper_id}",
                "path": f"§{path}",
                "ordinal": idx,
                "title": title,
            },
        ))
        edges.append(ParsedEdge(
            src=f"paper:{paper_id}", dst=sec_id, edge_type="contains",
        ))

        body = full_text[sec_start:sec_end]
        # Paragraph split at blank lines.
        para_chunks = [
            chunk.strip() for chunk in re.split(r"\n\s*\n", body)
            if chunk.strip() and not section_pat.match(chunk.strip())
        ]
        for chunk in para_chunks:
            para_id = f"para:{paper_id}:{para_idx}"
            para_idx += 1
            nodes.append(ParsedNode(
                node_id=para_id, subtype="paragraph",
                attrs={
                    "paragraph_id": para_id,
                    "parent_section_ref": sec_id.split(":", 1)[-1],
                    "ordinal": para_idx - 1,
                    "text": chunk,
                },
            ))
            edges.append(ParsedEdge(
                src=sec_id, dst=para_id, edge_type="contains",
            ))
            for m in citation_pat.finditer(chunk):
                cite_node_id = (
                    f"cite:{paper_id}:{cite_idx}:"
                    f"{hashlib.sha256(m.group(1).encode()).hexdigest()[:8]}"
                )
                cite_idx += 1
                lo = max(0, m.start() - 60)
                hi = min(len(chunk), m.end() + 60)
                nodes.append(ParsedNode(
                    node_id=cite_node_id, subtype="citation",
                    attrs={
                        "citation_id": cite_node_id,
                        "citing_section_ref": sec_id.split(":", 1)[-1],
                        "context_text":
                            chunk[lo:hi].replace("\n", " ").strip(),
                    },
                ))
                edges.append(ParsedEdge(
                    src=para_id, dst=cite_node_id, edge_type="contains",
                ))

    n_sections = len(section_spans)
    n_paragraphs = para_idx
    n_citations = cite_idx
    if n_sections >= 1 and n_paragraphs >= 5 and n_citations >= 1:
        confidence = 1.0
    elif n_sections >= 1 and n_paragraphs >= 1:
        confidence = 0.6
    else:
        confidence = 0.3

    anatomy = ParsedAnatomy(
        nodes=nodes,
        edges=edges,
        n_sections=n_sections,
        n_paragraphs=n_paragraphs,
        n_figures=0,
        n_tables=0,
        n_algorithms=0,
        n_equations=0,
        n_theorems=0,
        n_proofs=0,
        n_citations=n_citations,
    )

    if confidence < PDF_CONFIDENCE_FLOOR:
        return PdfParseResult(
            confidence=confidence,
            reason="grobid_low_confidence",
        )
    return PdfParseResult(confidence=confidence, anatomy=anatomy)


class PdfStructuralEnricher:
    """Substrate enricher: PDF → paper anatomy with confidence floor.

    Registered in a deployment's `enrichers:` block:

        enrichers:
          - class: okg.substrate.library.enrichers.scholarly.pdf_structural.PdfStructuralEnricher

    Reads `okg.graph_nodes WHERE subtype='paper'` (via a Chronos
    checkout at the pinned generation) with `attrs.source_pdf_path`
    set; skips Papers that already have at least one `contains`
    edge (already decomposed) AND zero prior `extraction_error`
    Observation (re-runs after parser fix are allowed per S-A1-R3
    scenario 3).
    """

    name: str = "pdf_structural"
    # Same derivation as the LaTeX path: one paper node per parsed anatomy.
    support_policy: DerivedSupportPolicy = "all_members"
    reads_subtypes: tuple[str, ...] = ("paper",)
    emits_edge_types: tuple[str, ...] = ("contains",)
    # Issue #1522: this enricher writes the parsed anatomy as NODES through
    # the shared `_commit_anatomy` helper, and one `extraction_error`
    # observation node through `_emit_extraction_error`. Declaring only edges
    # left the derived write envelope with an empty node surface, so both
    # boot-time node checks — node provenance and the derived-node
    # `support_policy` refusal — returned early and never covered this
    # producer. The node subtypes come from the pluggable parser backend
    # (`parser=`), so the honest declaration is an UNBOUNDED node dimension
    # rather than a snapshot enumeration, matching the LaTeX path that shares
    # the same helper (PACT admit-derived-node-emission).
    # The node dimension is unbounded, but the EDGE surface below is not: a
    # parser backend that emits anatomy outside the seven declared triples
    # (no theorem / proof / algorithm / equation, no nested paper_section)
    # has its nodes admitted and its edges refused, which refuses the batch.
    # A backend that needs those must extend `requires_narrowings` with them.
    unbounded_write_dimensions: tuple[str, ...] = ("nodes",)
    # PACT bind-derived-node-provenance: every node written by
    # `_commit_anatomy` / `_emit_extraction_error` on this path passes
    # `derived_from=[paper_node_id]`.
    declares_node_provenance: bool = True
    requires_narrowings: tuple[tuple[str, str, str], ...] = (
        ("paper", "contains", "paper_section"),
        ("paper_section", "contains", "paragraph"),
        ("paper_section", "contains", "figure"),
        ("paper_section", "contains", "table"),
        ("paragraph", "contains", "citation"),
        ("figure", "contains", "citation"),
        ("table", "contains", "citation"),
    )

    def __init__(
        self,
        *,
        parser: Optional[
            Callable[..., PdfParseResult]
        ] = None,
        confidence_floor: float = PDF_CONFIDENCE_FLOOR,
        approved_roots: Optional[list[Path | str]] = None,
    ) -> None:
        self._parser = parser or default_pypdfium2_parser
        self._floor = confidence_floor
        # Approved corpus/inbox roots a graph `source_pdf_path` must resolve
        # inside before the parser opens it. When configured, an out-of-root
        # path yields an extraction_error and the parser is never invoked (per
        # constrain-graph-file-and-parser-ingestion).
        self._approved_roots: Optional[list[Path]] = (
            [Path(r) for r in approved_roots] if approved_roots else None
        )

    def _resolve_pdf_path(self, pdf_path: str) -> Optional[Path]:
        """Resolve a graph-controlled source_pdf_path inside an approved root.

        Returns the resolved path only if it lands (realpath/symlink-safe) inside
        a configured approved root; otherwise None (the caller emits an
        extraction_error and never opens the file). When NO roots are configured
        this fails CLOSED — it returns None for every path rather than opening an
        arbitrary graph-controlled path — mirroring the viz pub_pdf read side. A
        graph `source_pdf_path` is attacker-influenceable (any ingest path can
        write it), so opening it without containment is the rejected
        silent-insecure-default (constrain-graph-file-and-parser-ingestion;
        adversarial-review remediation)."""
        if not self._approved_roots:
            return None
        return resolve_within_approved_root(pdf_path, self._approved_roots)

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        **_publisher_kwargs: object,
    ) -> EnrichResult:
        run_id = str(uuid.uuid4())
        observed_at = datetime.now(timezone.utc)

        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        rows = [
            (row["node_id"], row["pdf_path"])
            for row in graph.query(
                """
                SELECT n.node_id, n.attrs->>'source_pdf_path' AS pdf_path
                  FROM okg.graph_nodes n
                 WHERE n.subtype = 'paper'
                   AND coalesce(n.attrs->>'source_pdf_path', '') <> ''
                   AND NOT EXISTS (
                       SELECT 1 FROM okg.graph_edges el
                        WHERE el.src = n.node_id
                          AND el.edge_type = 'contains'
                   )
                """
            )
        ]

        if not rows:
            logger.info(
                "%s: zero PDFs needing decomposition at gen %d",
                self.name, generation_id,
            )
            return EnrichResult(enricher_name=self.name, n_edges_emitted=0)

        unconfigured = not self._approved_roots
        if unconfigured:
            # Fail closed: refuse to parse any graph source_pdf_path when no
            # approved corpus root is configured, rather than opening arbitrary
            # paths. Each paper is contained as an extraction_error below; the
            # operator must declare `approved_roots` in the enricher's
            # deployment.yaml config (e.g. the paper_pdf_source inbox_path).
            logger.error(
                "%s: PDF corpus root not configured (approved_roots); refusing "
                "to parse %d graph source_pdf_path value(s) without root "
                "containment — declare approved_roots in deployment.yaml",
                self.name, len(rows),
            )
        n_edges = 0
        n_skipped = 0
        with conn.cursor() as cur:
            for paper_node_id, pdf_path in rows:
                paper_id = paper_node_id.split(":", 1)[-1]
                pdf_path_obj = self._resolve_pdf_path(pdf_path)
                if pdf_path_obj is None:
                    # Unconfigured roots OR a path outside every approved root
                    # (or missing): contain it as a per-paper error; do NOT open
                    # the file.
                    reason = (
                        "pdf_corpus_root_not_configured" if unconfigured
                        else "pdf_path_outside_approved_root"
                    )
                    _emit_extraction_error(
                        cur,
                        paper_node_id=paper_node_id,
                        extractor=EXTRACTOR_NAME,
                        version=EXTRACTOR_VERSION,
                        confidence=0.0,
                        reason=reason,
                        run_id=run_id,
                        generation_id=generation_id,
                        observed_at=observed_at,
                        support_policy=resolve_declared_support_policy(self),
                    )
                    n_skipped += 1
                    continue
                try:
                    result = self._parser(
                        pdf_path=pdf_path_obj, paper_id=paper_id,
                    )
                except Exception as e:  # noqa: BLE001
                    _emit_extraction_error(
                        cur,
                        paper_node_id=paper_node_id,
                        extractor=EXTRACTOR_NAME,
                        version=EXTRACTOR_VERSION,
                        confidence=0.0,
                        reason=f"pdf_unreadable: {e}",
                        run_id=run_id,
                        generation_id=generation_id,
                        observed_at=observed_at,
                        support_policy=resolve_declared_support_policy(self),
                    )
                    n_skipped += 1
                    continue

                if (
                    result.confidence < self._floor
                    or result.anatomy is None
                ):
                    _emit_extraction_error(
                        cur,
                        paper_node_id=paper_node_id,
                        extractor=EXTRACTOR_NAME,
                        version=EXTRACTOR_VERSION,
                        confidence=result.confidence,
                        reason=result.reason or "low_confidence",
                        run_id=run_id,
                        generation_id=generation_id,
                        observed_at=observed_at,
                        support_policy=resolve_declared_support_policy(self),
                    )
                    n_skipped += 1
                    continue

                n_edges += _commit_anatomy(
                    cur,
                    anatomy=result.anatomy,
                    extractor=EXTRACTOR_NAME,
                    extractor_version=EXTRACTOR_VERSION,
                    run_id=run_id,
                    generation_id=generation_id,
                    observed_at=observed_at,
                    paper_node_id=paper_node_id,
                    support_policy=resolve_declared_support_policy(self),
                )

        logger.info(
            "%s: emitted %d edges across %d papers (%d skipped) at gen %d",
            self.name, n_edges, len(rows) - n_skipped, n_skipped,
            generation_id,
        )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=n_edges,
            n_edges_skipped=n_skipped,
        )
