"""Deterministic PDF materialization via typst (config toggle).

Clinical documents are natively a PDF medium; the PDF lane exercises the
real ingestion surface. Determinism recipe (design §7): pinned typst
binary, system-font isolation (``--ignore-system-fonts`` — typst's
embedded fonts only), and creation metadata fixed from the PRF via
``SOURCE_DATE_EPOCH``. When typst is absent the lane raises
:class:`PdfUnavailable` — callers surface it as a VISIBLE named skip,
never a silent pass.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Mapping

from okg.bench.synth.prf import SynthPrf


class PdfUnavailable(RuntimeError):
    """typst is not installed; the PDF lane must skip VISIBLY."""


class PdfError(RuntimeError):
    """typst failed on a document."""


def _typst_binary() -> str:
    binary = shutil.which("typst")
    if binary is None:
        raise PdfUnavailable(
            "typst binary not found on PATH; the PDF lane is skipped "
            "(install typst to materialize PDFs)"
        )
    return binary


def _typ_source(markdown_text: str) -> str:
    """Markdown → minimal typst source (headings + raw lines).

    The materializer preserves the text layer verbatim — the round-trip
    witness re-extracts claims from the PDF text, so content lines pass
    through unstyled; headings map to typst headings.
    """
    lines = ["#set page(margin: 2cm)", '#set text(font: "New Computer Modern", size: 10pt)']
    for line in markdown_text.splitlines():
        if line.startswith("### "):
            lines.append(f"=== {_escape(line[4:])}")
        elif line.startswith("## "):
            lines.append(f"== {_escape(line[3:])}")
        elif line.startswith("# "):
            lines.append(f"= {_escape(line[2:])}")
        elif line.strip():
            lines.append(_escape(line) + " \\")
        else:
            lines.append("")
    return "\n".join(lines) + "\n"


def _escape(text: str) -> str:
    out = []
    for ch in text:
        if ch in "#$*_`@<>[]\\/":
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


def typst_version() -> str:
    binary = _typst_binary()
    result = subprocess.run(
        [binary, "--version"], capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def materialize_pdfs(
    docs: Mapping[str, str], out_dir: Path, *, seed: str
) -> dict[str, Path]:
    """Render every markdown document to a deterministic PDF.

    Returns {doc name -> pdf path}. Raises :class:`PdfUnavailable` when
    typst is absent (visible skip at the caller).
    """
    binary = _typst_binary()
    prf = SynthPrf(seed)
    # fixed, seed-derived creation time inside a sane range (2026-01-01 + up
    # to ~11 days): reproducible bytes without wall-clock dependence.
    epoch = 1767225600 + prf.randint(0, 999_999, "pdf", "epoch")
    out_dir.mkdir(parents=True, exist_ok=True)
    produced: dict[str, Path] = {}
    for doc in sorted(docs):
        typ_path = out_dir / (Path(doc).stem + ".typ")
        pdf_path = out_dir / (Path(doc).stem + ".pdf")
        typ_path.write_text(_typ_source(docs[doc]), encoding="utf-8")
        # minimal env: ambient variables (TYPST_FONT_PATHS, locale) can
        # change output bytes even under --ignore-system-fonts (review
        # finding L11) — cross-machine determinism needs a closed env.
        env = {
            "PATH": os.environ.get("PATH", ""),
            "SOURCE_DATE_EPOCH": str(epoch),
        }
        result = subprocess.run(
            [binary, "compile", "--ignore-system-fonts", str(typ_path), str(pdf_path)],
            capture_output=True,
            text=True,
            env=env,
        )
        if result.returncode != 0:
            raise PdfError(f"typst failed on {doc}: {result.stderr.strip()[:400]}")
        produced[doc] = pdf_path
    return produced
