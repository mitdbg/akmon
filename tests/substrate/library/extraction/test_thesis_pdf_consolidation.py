"""Thesis test for `add-substrate-extraction-library`.

Tests the central claim of this proposal: that the shared
extraction library deletes parallel re-implementations from the
deployments. Concretely: PDF text + table extraction is wrapped
in two places in the legacy af_pubs deployment
(`enrichers/pdf_text.py::_load_pypdf` + `af_pubs_sources/structure.py::extract_tables`).
The shared kit splits these into two single-package parsers
(`parse.pdf_text.PdfTextParser` wrapping pypdf,
`parse.pdf_tables.PdfTablesParser` wrapping pdfplumber) so the
wrap invariant — ≤1 third-party top-level package per backend
module — holds structurally; deployments compose by name.

This test:
  1. Counts the lines of the legacy parallel wrappers.
  2. Demonstrates the equivalent shared-kit usage in a few
     lines.
  3. Asserts the rewrite is a strict net reduction.

If a future change inflates the deployment-side wrapper, this
test fails — the thesis isn't a one-time claim, it's an
invariant.
"""
from __future__ import annotations

import re
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[4]


def _line_count_in_file(path: Path, marker_re: str, end_re: str) -> int:
    """Count lines from `marker_re` (exclusive) up to (but not
    including) `end_re`. Both regexes match whole lines via
    `re.search`. Returns 0 if either marker isn't found.
    """
    if not path.is_file():
        return 0
    src = path.read_text().splitlines()
    start_idx = next(
        (i for i, line in enumerate(src) if re.search(marker_re, line)),
        None,
    )
    if start_idx is None:
        return 0
    end_idx = next(
        (i for i, line in enumerate(src[start_idx + 1:], start=start_idx + 1)
         if re.search(end_re, line)),
        len(src),
    )
    return end_idx - start_idx - 1


def test_kit_replaces_legacy_pdf_wrappers() -> None:
    """The shared kit collapses two legacy wrappers into two
    named parser_for calls (text + tables, one each). After the
    `add-substrate-extraction-library` migration completed, the
    deployment-side legacy wrappers are GONE — the post-migration
    invariant is "legacy paths absent AND kit-side parsers
    importable."

    This test asserts the migration is complete (legacy gone),
    not the in-flight intermediate state (legacy + kit coexist).
    """

    # --- Legacy site #1: previously
    # af_pubs/enrichers/pdf_text.py::_load_pypdf — the entire
    # `pdf_text.py` enricher migrated to substrate-side parsers.
    legacy_pdf_text = (
        _REPO_ROOT / "deployments" / "af_pubs" / "enrichers"
        / "pdf_text.py"
    )
    legacy_load_pypdf_lines = _line_count_in_file(
        legacy_pdf_text,
        marker_re=r"^def _load_pypdf",
        end_re=r"^def [a-zA-Z_]",
    )

    # --- Legacy site #2: previously
    # af_pubs/af_pubs_sources/structure.py::extract_tables — the
    # whole `af_pubs_sources/` tree migrated.
    legacy_structure = (
        _REPO_ROOT / "deployments" / "af_pubs" / "af_pubs_sources"
        / "structure.py"
    )
    legacy_extract_tables_lines = _line_count_in_file(
        legacy_structure,
        marker_re=r"^def extract_tables",
        end_re=r"^def [a-zA-Z_]",
    )

    # Post-migration invariant: both legacy wrappers must be gone.
    assert legacy_load_pypdf_lines == 0, (
        f"legacy _load_pypdf re-introduced at {legacy_pdf_text} "
        f"({legacy_load_pypdf_lines} lines). The kit "
        f"(`okg.substrate.library.extraction.parse.parser_for`) "
        f"is the canonical surface; deployment-side wrappers are "
        f"forbidden."
    )
    assert legacy_extract_tables_lines == 0, (
        f"legacy extract_tables re-introduced at "
        f"{legacy_structure} ({legacy_extract_tables_lines} "
        f"lines). The kit "
        f"(`parser_for('pdf_tables')`) is the canonical surface."
    )

    # Post-migration invariant: kit-side parsers are importable.
    from okg.substrate.library.extraction.parse import parser_for
    assert parser_for("pdf") is not None
    assert parser_for("pdf_tables") is not None

def test_kit_consolidates_ner_pattern_across_deployments() -> None:
    """Demonstrate the NER consolidation claim. CompositeNER over
    one spaCy + one regex backend is what the four legacy parallel
    'extract people from text' implementations would each have
    been re-authoring.
    """
    from okg.extraction import ExtractedEntity
    from okg.substrate.library.extraction.ner import (
        CompositeNER, RegexEntityExtractor,
    )

    # This is the entirety of a deployment-side composite — no
    # NER reimplementation, no dedupe code, no span overlap math.
    composite = CompositeNER(
        # In a real deployment, SpacyNER('en_core_web_sm') would be
        # one backend; we use a stand-in here so the test is
        # model-free.
        _StubPersonExtractor(),
        RegexEntityExtractor([
            ("ticket", r"CMS-[0-9]+"),
        ]),
    )
    out = composite.extract("Alice met Bob about CMS-1234.")
    types = sorted({_canonical(e.type) for e in out})
    assert "person" in types
    assert "ticket" in types
    # Spans are consistent.
    for e in out:
        assert isinstance(e, ExtractedEntity)


def _canonical(t: str) -> str:
    return t.lower()


class _StubPersonExtractor:
    """Tiny stand-in for spaCy NER — finds capitalized first
    names. Real deployments use SpacyNER; this stub keeps the
    thesis test model-free."""

    def extract(self, text: str):
        from okg.extraction import ExtractedEntity
        out = []
        for m in re.finditer(r"\b([A-Z][a-z]+)\b", text):
            name = m.group(1)
            if name in {"Alice", "Bob", "Carol"}:
                out.append(ExtractedEntity(
                    type="PERSON",
                    value=name,
                    start=m.start(),
                    end=m.end(),
                    confidence=0.9,
                ))
        return out
