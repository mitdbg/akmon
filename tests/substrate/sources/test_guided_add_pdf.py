"""`add-source` reads the text of PDFs under a local folder (issue #3900).

The packaged default `extractors.yaml` carries a `pdf` parser, so a folder
whose `include` names `**/*.pdf` gets its PDF text read. A PDF with no text
layer (a scan) still parses, so the preview must say so instead of letting it
import as a silent empty record. These checks run the read-only planning half
against a real scaffold deployment directory and real PDF bytes.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.source_add import _render_text
from okg.substrate.deployment_scaffold import create_deployment_scaffold
from okg.substrate.library.extraction.parse.pdf_text import PdfTextParser
from okg.substrate.sources import guided_add
from okg.substrate.sources.guided_add import plan_add_source
from okg.substrate.sources.guided_defaults import EXTRACTORS_YAML
from tests.substrate.sources.pdf_fixtures import make_pdf

PDF_INCLUDE = ["**/*.pdf"]
PAPER = make_pdf(["Search for the Higgs boson", "Decays to two photons"])
SCAN = make_pdf([None, None])


@pytest.fixture
def deployment(tmp_path: Path) -> Path:
    return create_deployment_scaffold(
        "unit", deployments_root=tmp_path / "deployments",
    ).deployment_dir


def _folder(tmp_path: Path, files: dict[str, bytes]) -> Path:
    root = tmp_path / "papers"
    root.mkdir()
    for name, body in files.items():
        (root / name).write_bytes(body)
    return root


def _plan(deployment: Path, root: Path, include: list[str] | None = PDF_INCLUDE):
    settings: dict[str, object] = {"root": str(root)}
    if include is not None:
        settings["include"] = include
    return plan_add_source(
        deployment=str(deployment), name="papers", type_id="local-folder",
        settings=settings, environ={"PATH": os.environ.get("PATH", "")},
    )


def test_the_packaged_defaults_declare_the_pdf_parser() -> None:
    document = yaml.safe_load(EXTRACTORS_YAML.read_text(encoding="utf-8"))
    parsers = {entry["format"]: entry["class"] for entry in document["parser"]}
    assert parsers["pdf"] == (
        "okg.substrate.library.extraction.parse.pdf_text.PdfTextParser"
    )
    # No model-backed stage is switched on by the defaults.
    assert "captioning" not in document
    assert "table_transcription" not in document
    assert document["ner"] == []


def test_the_fixture_pdf_has_a_text_layer_per_page() -> None:
    parsed = PdfTextParser().parse(PAPER)
    assert [c.heading_path for c in parsed.chunks] == [["page 1"], ["page 2"]]
    assert parsed.chunks[0].text == "Search for the Higgs boson"
    assert "parse_warnings" not in parsed.metadata


def test_connectors_can_call_the_pdf_reader_directly() -> None:
    # The public library call other connectors reuse: bytes in, text and
    # per-page chunks out, with no deployment, database or model involved.
    from okg.substrate.library.extraction.parse.pdf_text import (
        PdfTextParser as PublicPdfTextParser,
    )

    parsed = PublicPdfTextParser().parse(make_pdf(["Alpha beta", "Gamma delta"]))
    assert parsed.text == "Alpha beta\n\nGamma delta"
    assert [(c.heading_path, c.text) for c in parsed.chunks] == [
        (["page 1"], "Alpha beta"), (["page 2"], "Gamma delta"),
    ]


def test_a_pdf_with_no_text_layer_says_so_when_parsed() -> None:
    parsed = PdfTextParser().parse(SCAN)
    assert parsed.text.strip() == ""
    assert parsed.metadata["parse_warnings"] == ["pdf_no_text_layer"]


def test_pypdf_font_warnings_are_summarised_not_printed(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    # A physics paper makes pypdf log "fontTools is required ..." per font per
    # page. The parser keeps them off the console and says so once.
    import logging
    from types import SimpleNamespace

    import pypdf

    def noisy_page_text() -> str:
        for _ in range(3):
            logging.getLogger("pypdf").warning(
                "fontTools is required to fully parse the encoding of a font",
            )
        return "Higgs"

    fake = SimpleNamespace(
        pages=[SimpleNamespace(extract_text=noisy_page_text)], metadata=None,
    )
    monkeypatch.setattr(pypdf, "PdfReader", lambda _buf: fake)
    with caplog.at_level(logging.WARNING):
        parsed = PdfTextParser().parse(b"%PDF-1.4 fake")
    assert parsed.metadata["parse_warnings"] == [
        "pdf_reader_warnings:3",
        "pdf_font_encoding_incomplete:fonttools_missing",
        "pdf_reader_warning_example:fontTools is required to fully parse the "
        "encoding of a font",
    ]
    assert not [r for r in caplog.records if r.name == "pypdf"]
    assert logging.getLogger("pypdf").propagate is True
    assert parsed.text == "Higgs"


def test_a_pdf_folder_previews_with_the_pdf_reader_and_no_warning(
    deployment: Path, tmp_path: Path,
) -> None:
    root = _folder(tmp_path, {"a.pdf": PAPER, "sub.pdf": PAPER})
    plan = _plan(deployment, root)
    assert plan.ok, [s.as_dict() for s in plan.steps]
    assert plan.warnings == []
    assert plan.reader_formats == ["pdf"]
    assert plan.file_readers is not None
    assert "PdfTextParser" in plan.file_readers["text"]
    assert plan.preview["scope"]["files_matched"] == 2
    assert "files_without_reader" not in plan.preview["scope"]


def test_the_save_step_adds_the_pdf_reader_to_an_existing_extractors_file(
    deployment: Path, tmp_path: Path,
) -> None:
    (deployment / "extractors.yaml").write_text(
        "parser:\n  - format: markdown\n"
        "    class: okg.substrate.library.extraction.parse.markdown.MarkdownParser\n",
    )
    root = _folder(tmp_path, {"a.pdf": PAPER})
    plan = _plan(deployment, root)
    assert plan.ok and plan.warnings == []
    assert plan.file_readers["action"] == "add"
    assert plan.file_readers["missing"]["parser"] == ["pdf"]
    assert "PdfTextParser" in plan.file_readers["text"]


def test_a_scanned_pdf_is_named_in_the_preview(
    deployment: Path, tmp_path: Path,
) -> None:
    root = _folder(tmp_path, {"paper.pdf": PAPER, "scan1.pdf": SCAN, "scan2.pdf": SCAN})
    plan = _plan(deployment, root)
    assert plan.ok  # a warning, not a refusal
    (warning,) = plan.warnings
    assert warning["kind"] == "pdf_without_text"
    assert warning["no_text_layer_count"] == 2
    assert warning["pdfs_checked"] == 3
    assert warning["count_is_lower_bound"] is False
    assert sorted(warning["example_files"]) == ["scan1.pdf", "scan2.pdf"]
    assert plan.preview["scope"]["pdfs_without_text"] == 2
    text = _render_text(plan.as_dict())
    assert "WARNING: 2 of 3 PDFs checked have no readable text" in text
    assert "ocrmypdf" in text


def test_a_damaged_pdf_is_counted_with_the_scans(
    deployment: Path, tmp_path: Path,
) -> None:
    root = _folder(tmp_path, {"paper.pdf": PAPER, "broken.pdf": b"%PDF-1.4\nnot a pdf"})
    plan = _plan(deployment, root)
    (warning,) = plan.warnings
    assert warning["kind"] == "pdf_without_text"
    assert warning["example_files"] == ["broken.pdf"]
    assert warning["unopenable_files"] == {"broken.pdf": "PdfStreamError"}


def test_the_scan_check_is_bounded_and_says_when_it_stopped_early(
    deployment: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(guided_add, "PDF_TEXT_CHECK_FILES", 1)
    root = _folder(tmp_path, {"a.pdf": SCAN, "b.pdf": SCAN, "c.pdf": SCAN})
    plan = _plan(deployment, root)
    (warning,) = plan.warnings
    assert warning["pdfs_checked"] == 1 and warning["pdfs_matched"] == 3
    assert warning["count_is_lower_bound"] is True
    assert warning["message"].startswith("at least 1 of 1 PDFs checked")


def test_a_folder_of_markdown_is_unchanged(deployment: Path, tmp_path: Path) -> None:
    root = _folder(tmp_path, {"a.md": b"# A\n\nHiggs boson.\n"})
    plan = _plan(deployment, root, None)
    assert plan.ok and plan.warnings == []
    assert plan.reader_formats == ["markdown"]


def test_a_hand_edited_extractors_file_left_unchanged_still_warns(
    deployment: Path, tmp_path: Path,
) -> None:
    # A plural-keys file is never rewritten; the PDF reader is then something
    # the operator adds by hand, and the preview says so as before.
    (deployment / "extractors.yaml").write_text(
        "parsers:\n  - format: text\n"
        "    class: okg.substrate.library.extraction.parse.text.TextParser\n",
    )
    root = _folder(tmp_path, {"a.pdf": PAPER})
    plan = _plan(deployment, root)
    assert plan.ok, [s.as_dict() for s in plan.steps]
    (warning,) = plan.warnings
    assert warning["kind"] == "unreadable_files"
    assert warning["files_by_type"] == {".pdf": 1}
    assert warning["extractors_file_left_unchanged"] is True
    assert "pdf" in warning["fix_hint"]


def test_default_include_on_a_pdf_only_folder_names_the_pdfs_and_the_include(
    deployment: Path, tmp_path: Path,
) -> None:
    root = _folder(tmp_path, {"a.pdf": PAPER, "b.pdf": PAPER, "c.docx": b"PK\x03\x04"})
    plan = _plan(deployment, root, None)  # the default include
    assert not plan.ok
    failed = plan.failed_step
    assert failed is not None and failed.name == "preview"
    assert "the folder holds .pdf: 2, .docx: 1" in failed.cause
    assert failed.fix_hint == 'to read those files, pass --setting \'include=["**/*.pdf"]\''
    assert failed.detail["scope"]["file_types_present"] == {".pdf": 2, ".docx": 1}


def test_an_empty_folder_keeps_the_plain_no_match_message(
    deployment: Path, tmp_path: Path,
) -> None:
    root = _folder(tmp_path, {})
    failed = _plan(deployment, root, None).failed_step
    assert failed is not None
    assert failed.cause == "no files under the folder match the include patterns"
    assert failed.fix_hint == "check the folder path or pass --setting include=..."


def test_a_folder_of_unreadable_types_says_so(
    deployment: Path, tmp_path: Path,
) -> None:
    root = _folder(tmp_path, {"c.docx": b"PK\x03\x04"})
    failed = _plan(deployment, root, None).failed_step
    assert failed is not None and ".docx: 1" in failed.cause
    assert "none of those types is read" in failed.fix_hint


def test_nested_pypdf_captures_restore_the_logger_once() -> None:
    import logging

    from okg.substrate.library.extraction.parse.pdf_text import _capture_pypdf_logs

    logger = logging.getLogger("pypdf")
    assert logger.propagate is True
    with _capture_pypdf_logs():
        with _capture_pypdf_logs():
            assert logger.propagate is False
        assert logger.propagate is False  # the outer parse is still running
    assert logger.propagate is True
    assert logger.handlers == [] or all(
        type(h).__name__ != "_PypdfWarnings" for h in logger.handlers
    )


def test_an_encrypted_pdf_is_named_with_its_error_class(
    deployment: Path, tmp_path: Path,
) -> None:
    import io

    import pypdf

    writer = pypdf.PdfWriter()
    writer.add_blank_page(width=72, height=72)
    writer.encrypt("secret")
    buffer = io.BytesIO()
    writer.write(buffer)
    root = _folder(tmp_path, {"locked.pdf": buffer.getvalue()})
    (warning,) = _plan(deployment, root).warnings
    assert warning["kind"] == "pdf_without_text"
    assert list(warning["unopenable_files"]) == ["locked.pdf"]
    assert warning["unopenable_files"]["locked.pdf"] != ""


def test_a_pdf_over_the_size_cap_is_counted_and_never_read(
    deployment: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.doc_corpus import DocCorpusSource

    cap = DocCorpusSource.max_bytes_per_file
    assert cap == 50 * 1024 * 1024
    root = _folder(tmp_path, {"small.pdf": PAPER})
    big = root / "big.pdf"
    with big.open("wb") as handle:  # sparse: no real disk is used
        handle.write(PAPER)
        handle.truncate(cap + 1)

    def never(*_a: object, **_k: object) -> None:
        raise AssertionError("an over-cap PDF was parsed in the preview")

    real_parse = PdfTextParser.parse

    def parse(self, content, **kwargs):  # noqa: ANN001
        if len(content) > cap:
            never()
        return real_parse(self, content, **kwargs)

    monkeypatch.setattr(PdfTextParser, "parse", parse)
    real_read_bytes = Path.read_bytes

    def read_bytes(self: Path) -> bytes:
        if self.name == "big.pdf":
            never()
        return real_read_bytes(self)

    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    plan = _plan(deployment, root)
    assert plan.ok, [s.as_dict() for s in plan.steps]
    (warning,) = plan.warnings
    assert warning["kind"] == "pdf_too_large"
    assert warning["example_files"] == ["big.pdf"]
    assert warning["max_bytes_per_file"] == cap
    assert "over 50 MiB" in warning["message"]
    assert plan.preview["scope"]["pdfs_over_size_cap"] == 1


def test_an_uppercase_pdf_suffix_gets_the_include_as_found(
    deployment: Path, tmp_path: Path,
) -> None:
    root = _folder(tmp_path, {"A.PDF": PAPER})
    failed = _plan(deployment, root, None).failed_step
    assert failed is not None
    assert failed.fix_hint == 'to read those files, pass --setting \'include=["**/*.PDF"]\''
    # and that include really matches
    assert _plan(deployment, root, ["**/*.PDF"]).ok
