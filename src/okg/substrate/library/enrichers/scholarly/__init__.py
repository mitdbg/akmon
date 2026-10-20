"""Substrate-shipped scholarly enrichers.

Per `extend-substrate-scholarly-with-paper-anatomy`
(umbrella `evolve-memex-to-ai-scientist-substrate` A1):

  - `latex_structural.LatexStructuralEnricher` — deterministic
    LaTeX parser. Reads a Paper whose `attrs.source_tex_path`
    points at a UTF-8 `.tex` file; emits the paper-anatomy
    decomposition (PaperSection / Paragraph / Figure / Table /
    Algorithm / Equation / Theorem / Proof / Citation) plus
    the intra-scholarly containment narrowings. Provenance
    `derived_deterministic`, confidence 1.0. Catastrophic parse
    failures emit a single `extraction_error` Observation
    against the Paper node.

  - `pdf_structural.PdfStructuralEnricher` — deterministic PDF
    parser wrapping pypdfium2 + pymupdf with a confidence floor
    of 0.85. Emits sections / paragraphs / figures / tables /
    citations above the floor; emits a single
    `extraction_error` Observation and zero anatomy nodes
    below. Equations and theorems are out of scope for the PDF
    path until Nougat-class extractors mature (Q1).
"""

from okg.substrate.library.enrichers.scholarly.latex_structural import (
    LatexStructuralEnricher,
    LatexParseError,
    parse_latex,
)
from okg.substrate.library.enrichers.scholarly.pdf_structural import (
    PdfStructuralEnricher,
    PDF_CONFIDENCE_FLOOR,
)

__all__ = [
    "LatexStructuralEnricher",
    "LatexParseError",
    "parse_latex",
    "PdfStructuralEnricher",
    "PDF_CONFIDENCE_FLOOR",
]
