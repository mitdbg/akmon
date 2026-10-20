"""Tests for PaperPdfSource and OurDraftsSource.

Covers spec scenarios:
  - S-A1-R5 / Scenario: PaperPdfSource respects deployment-configured inbox
  - S-A1-R5 / Scenario: OurDraftsSource respects glob expansion
  - S-A1-R5 / Scenario: Substrate hardcodes no operator-specific path
  - S-A1-R5 (general): re-runs are content-hash-stable / idempotent
"""

from __future__ import annotations

import inspect
import json
import re
from pathlib import Path

import pytest

from okg.substrate.library.sources.scholarly.our_drafts_source import (
    OurDraftsSource,
    compute_draft_id,
)
from okg.substrate.library.sources.scholarly.paper_pdf_source import (
    PaperPdfSource,
    compute_paper_id,
)
from okg.substrate.library.sources.scholarly.scholarly_identity_aliases import (
    IdentityAliasSource,
)


# ----------------------- PaperPdfSource ------------------------- #


def _write_fake_pdf(path: Path, body: bytes) -> Path:
    """Write a small file with a PDF magic header for source-discovery
    tests. Not a real parseable PDF — only the source's existence /
    walk / hashing logic is exercised here; the enricher path is
    tested separately with a real fixture."""
    path.write_bytes(b"%PDF-1.4\n" + body)
    return path


def _write_metadata_pdf(path: Path, *, title: str, subject: str = "") -> Path:
    from pypdf import PdfWriter

    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    metadata = {"/Title": title}
    if subject:
        metadata["/Subject"] = subject
    writer.add_metadata(metadata)
    with path.open("wb") as f:
        writer.write(f)
    return path


def test_paper_pdf_source_emits_one_paper_per_pdf(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    _write_fake_pdf(inbox / "a.pdf", b"a-body")
    _write_fake_pdf(inbox / "b.pdf", b"b-body")
    sub = inbox / "nested"
    sub.mkdir()
    _write_fake_pdf(sub / "c.pdf", b"c-body")

    source = PaperPdfSource(inbox_path=str(inbox))
    run = source.run("run-001", mode="cursor")
    facts = list(run.facts)

    assert len(facts) == 3, facts
    # Subtype + kind + tier per spec.
    for f in facts:
        assert f.subtype == "paper"
        assert f.attrs["kind"] == "research"
        assert f.attrs["tier"] == 1
        # source_pdf_path is what the PdfStructuralEnricher reads.
        assert f.attrs["source_pdf_path"].endswith(".pdf")
    # Paper ids are content-hash-stable + slugged from filename stem.
    ids = sorted(f.node_id for f in facts)
    assert all(re.match(r"paper:[0-9a-f]{16}-[a-z0-9-]+", i) for i in ids)


def test_paper_pdf_source_idempotent_across_runs(tmp_path: Path) -> None:
    """S-A1-R5 Scenario: A second run over the unchanged inbox SHALL
    produce zero new Paper nodes (content-hash-stable identity)."""
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    _write_fake_pdf(inbox / "p.pdf", b"contents")

    source = PaperPdfSource(inbox_path=str(inbox))
    first = list(source.run("run-1", mode="cursor").facts)
    second = list(source.run("run-2", mode="cursor").facts)
    assert {f.node_id for f in first} == {f.node_id for f in second}


def test_paper_pdf_source_empty_inbox_emits_zero(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    source = PaperPdfSource(inbox_path=str(inbox))
    facts = list(source.run("run-empty", mode="cursor").facts)
    assert facts == []


def test_paper_pdf_source_missing_inbox_does_not_crash(tmp_path: Path) -> None:
    """A path that doesn't exist yet shouldn't take down the
    publisher — emit zero papers and let the operator create the
    directory at their leisure."""
    source = PaperPdfSource(inbox_path=str(tmp_path / "does_not_exist"))
    facts = list(source.run("run-missing", mode="cursor").facts)
    assert facts == []


def test_paper_pdf_source_requires_inbox_path() -> None:
    """The class rejects an empty `inbox_path` — no silent default to
    ~/.memex/...; the deployment SHALL supply a value."""
    with pytest.raises(ValueError):
        PaperPdfSource(inbox_path="")


def test_compute_paper_id_is_content_hash_stable(tmp_path: Path) -> None:
    f = tmp_path / "vision.pdf"
    _write_fake_pdf(f, b"hello")
    id1, h1 = compute_paper_id(f)
    id2, h2 = compute_paper_id(f)
    assert id1 == id2
    assert h1 == h2
    assert id1.startswith("paper:")
    assert "vision" in id1


def test_paper_pdf_source_emits_clean_bibliographic_attrs(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    pdf = _write_metadata_pdf(
        inbox / "Curie et al. - 2023 - Emission Rates of Radioactive Salts in Pitchblende.pdf",
        title="Emission Rates of Radioactive Salts in Pitchblende Samples",
        subject="Proc. ACM Manag. Data 2023.1:1-25 https://doi.org/10.0000/curie.2023.1",
    )

    source = PaperPdfSource(inbox_path=str(inbox))
    facts = list(source.run("run-001", mode="cursor").facts)

    assert len(facts) == 1
    attrs = facts[0].attrs
    assert facts[0].node_id == compute_paper_id(pdf)[0]
    assert attrs["title"] == "Emission Rates of Radioactive Salts in Pitchblende Samples"
    assert attrs["source_title"] == (
        "Curie et al. - 2023 - Emission Rates of Radioactive Salts in Pitchblende"
    )
    assert attrs["year"] == "2023"
    assert attrs["display_authors"] == "Curie et al."
    assert attrs["doi"] == "10.0000/curie.2023.1"
    assert attrs["bibliographic_metadata_status"] == "ok"
    assert attrs["bibliographic_metadata_sources"] == ["pdf_metadata_title"]


def test_paper_pdf_source_marks_failed_pdf_metadata_extraction(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    _write_fake_pdf(
        inbox / "Lamport - 1978 - Time Clocks and the Ordering of Events.pdf",
        b"not a parseable pdf",
    )

    source = PaperPdfSource(inbox_path=str(inbox))
    [fact] = list(source.run("run-001", mode="cursor").facts)

    assert fact.attrs["title"] == "Time Clocks and the Ordering of Events"
    assert fact.attrs["year"] == "1978"
    assert fact.attrs["display_authors"] == "Lamport"
    assert fact.attrs["bibliographic_metadata_status"] == "pdf_parse_failed"
    assert "pdf_metadata_error" in fact.attrs


def test_identity_alias_source_links_pdf_to_canonical_paper_by_title_year(
    tmp_path: Path,
) -> None:
    fixture_path = tmp_path / "papers.json"
    fixture_path.write_text(
        json.dumps(
            {
                "nodes": [
                    {
                        "node_id": "paper:pdf",
                        "subtype": "paper",
                        "attrs": {
                            "title": "Emission Rates of Radioactive Salts in Pitchblende Samples",
                            "year": "2023",
                            "source_provider": "paper_pdf",
                        },
                    },
                    {
                        "node_id": "paper:dblp",
                        "subtype": "paper",
                        "attrs": {
                            "title": "Emission Rates of Radioactive Salts in Pitchblende Samples",
                            "year": "2023",
                            "source_provider": "dblp",
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    source = IdentityAliasSource(
        fixture_path=fixture_path,
        target_subtypes=("paper",),
    )
    edges = list(source.run("run-001", mode="scope_complete").facts)

    assert len(edges) == 1
    [edge] = edges
    assert edge.src == "paper:pdf"
    assert edge.dst == "paper:dblp"
    assert edge.edge_type == "same_as"
    assert edge.attrs["identity_kind"] == "title"


# ------------------------ OurDraftsSource ----------------------- #


def test_our_drafts_source_globs_tex_files(tmp_path: Path) -> None:
    drafts = tmp_path / "drafts"
    drafts.mkdir()
    (drafts / "vision.tex").write_text("\\section{Intro}")
    (drafts / "notes.md").write_text("# Notes")
    (drafts / "ignore.pdf").write_bytes(b"%PDF-")

    source = OurDraftsSource(draft_paths=[str(drafts / "*")])
    facts = list(source.run("run-1", mode="cursor").facts)
    # 2 matched files (.tex + .md), 1 ignored (.pdf).
    assert len(facts) == 2
    tex_fact = next(f for f in facts if "source_tex_path" in f.attrs)
    md_fact = next(f for f in facts if "source_md_path" in f.attrs)
    assert tex_fact.attrs["kind"] == "our-draft"
    assert md_fact.attrs["kind"] == "our-draft"


def test_our_drafts_source_accepts_directory(tmp_path: Path) -> None:
    drafts = tmp_path / "drafts"
    drafts.mkdir()
    (drafts / "main.tex").write_text("\\section{x}")
    sub = drafts / "chapter"
    sub.mkdir()
    (sub / "intro.md").write_text("# intro")

    source = OurDraftsSource(draft_paths=[str(drafts)])
    facts = list(source.run("run-1", mode="cursor").facts)
    assert len(facts) == 2


def test_our_drafts_source_requires_paths_list() -> None:
    with pytest.raises(ValueError):
        OurDraftsSource(draft_paths=[])


def test_compute_draft_id_is_content_hash_stable(tmp_path: Path) -> None:
    f = tmp_path / "main.tex"
    f.write_text("\\section{x}")
    id1, h1 = compute_draft_id(f)
    id2, h2 = compute_draft_id(f)
    assert id1 == id2
    assert h1 == h2
    # Hash changes when content changes.
    f.write_text("\\section{y}")
    id3, h3 = compute_draft_id(f)
    assert id3 != id1


# ----------------------- D11 substrate parametricity ----------- #


def test_substrate_sources_hardcode_no_operator_paths() -> None:
    """S-A1-R5 Scenario: substrate hardcodes no operator-specific path.

    Grep test: the source files SHALL NOT mention `~/.memex` or any
    hardcoded operator path. Default values are NOT permitted; the
    deployment supplies them.
    """
    pdf_src = Path(
        inspect.getfile(PaperPdfSource)
    ).read_text("utf-8")
    drafts_src = Path(
        inspect.getfile(OurDraftsSource)
    ).read_text("utf-8")
    for src in (pdf_src, drafts_src):
        assert "~/.memex" not in src
        assert "/Users/" not in src
        assert "/home/" not in src


def test_source_classes_require_path_param_at_construction() -> None:
    """No default value for the path parameters — the deployment
    SHALL supply a value, or construction fails."""
    sig_pdf = inspect.signature(PaperPdfSource)
    assert sig_pdf.parameters["inbox_path"].default is inspect.Parameter.empty
    sig_drafts = inspect.signature(OurDraftsSource)
    assert (
        sig_drafts.parameters["draft_paths"].default
        is inspect.Parameter.empty
    )
