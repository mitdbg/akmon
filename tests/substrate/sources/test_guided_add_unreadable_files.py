"""Files with no parser are named in the add-source preview (issue #3890).

A Word file matched by `include` used to import as a record holding only its
file name, with no warning. These checks run the read-only planning half against a
real scaffold deployment directory.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from okg.substrate.cli import main as cli
from okg.substrate.cli.source_add import _render_text
from okg.substrate.deployment_scaffold import create_deployment_scaffold
from okg.substrate.sources import guided_add
from okg.substrate.sources.guided_add import (
    AddSourceOutcome,
    plan_add_source,
)

DOCX_INCLUDE = ["**/*.docx"]
BOTH_INCLUDE = ["**/*.docx", "**/*.md"]
DOCX = b"PK\x03\x04"


@pytest.fixture
def deployment(tmp_path: Path) -> Path:
    return create_deployment_scaffold(
        "unit", deployments_root=tmp_path / "deployments",
    ).deployment_dir


def _folder(tmp_path: Path, files: dict[str, bytes]) -> Path:
    root = tmp_path / "docs"
    root.mkdir()
    for name, body in files.items():
        (root / name).write_bytes(body)
    return root


def _plan(deployment: Path, root: Path, include: list[str] | None = None):
    settings: dict[str, object] = {"root": str(root)}
    if include is not None:
        settings["include"] = include
    return plan_add_source(
        deployment=str(deployment), name="papers", type_id="local-folder",
        settings=settings, environ={"PATH": os.environ.get("PATH", "")},
    )


def test_all_readable_files_give_no_warning(deployment: Path, tmp_path: Path) -> None:
    root = _folder(tmp_path, {"a.md": b"# A\n\nHiggs boson.\n", "b.txt": b"plain\n"})
    plan = _plan(deployment, root)
    assert plan.ok
    assert plan.warnings == []
    assert "files_without_reader" not in plan.preview["scope"]
    payload = plan.as_dict()
    assert payload["warnings"] == []
    assert "WARNING" not in _render_text(payload)


def test_mixed_folder_warns_with_counts_and_formats(
    deployment: Path, tmp_path: Path,
) -> None:
    root = _folder(tmp_path, {
        "a.md": b"# A\n\nHiggs boson.\n", "p1.docx": DOCX, "p2.docx": DOCX,
    })
    plan = _plan(deployment, root, BOTH_INCLUDE)
    assert plan.ok  # a warning, not a refusal
    (warning,) = plan.warnings
    assert warning["kind"] == "unreadable_files"
    assert warning["files_by_type"] == {".docx": 2}
    assert warning["unreadable_count"] == 2
    assert warning["matched_count"] == 3
    assert any("Markdown" in f for f in warning["readable_formats"])
    assert plan.preview["scope"]["files_without_reader"] == 2
    text = _render_text(plan.as_dict())
    assert "WARNING: 2 of 3 matched files cannot be read (.docx: 2)" in text
    assert "Markdown (.md, .markdown)" in text


def test_no_readable_file_is_refused_with_a_fix_hint(
    deployment: Path, tmp_path: Path,
) -> None:
    root = _folder(tmp_path, {"p1.docx": DOCX, "p2.docx": DOCX})
    plan = _plan(deployment, root, DOCX_INCLUDE)
    assert not plan.ok
    failed = plan.failed_step
    assert failed is not None and failed.name == "preview"
    assert "none of the 2 matched files can be read (.docx: 2)" in failed.cause
    assert "include=" in failed.fix_hint
    assert failed.detail["unreadable_files"]["files_by_type"] == {".docx": 2}


def test_a_deployment_parser_for_the_format_makes_it_readable(
    deployment: Path, tmp_path: Path,
) -> None:
    (deployment / "extractors.yaml").write_text(
        "parser:\n  - format: docx\n"
        "    class: okg.substrate.library.extraction.parse.docx.DocxParser\n",
    )
    root = _folder(tmp_path, {"p1.docx": DOCX})
    plan = _plan(deployment, root, DOCX_INCLUDE)
    assert plan.ok, [s.as_dict() for s in plan.steps]
    assert plan.warnings == []
    assert "files_without_reader" not in plan.preview["scope"]


def test_a_truncated_scan_warns_but_never_refuses(
    deployment: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _folder(tmp_path, {"p1.docx": DOCX, "p2.docx": DOCX, "p3.docx": DOCX})
    monkeypatch.setattr(guided_add, "PREVIEW_SCAN_LIMIT", 1)
    plan = _plan(deployment, root, DOCX_INCLUDE)
    assert plan.ok, [s.as_dict() for s in plan.steps]
    (warning,) = plan.warnings
    assert warning["count_is_lower_bound"] is True
    assert "1 of at least 1" in warning["message"]


def test_uppercase_extension_is_counted_as_its_lowercase_type(
    deployment: Path, tmp_path: Path,
) -> None:
    root = _folder(tmp_path, {"a.md": b"# A\n", "P1.DOCX": DOCX})
    plan = _plan(deployment, root, ["**/*.DOCX", "**/*.md"])
    (warning,) = plan.warnings
    assert warning["files_by_type"] == {".docx": 1}


def test_a_file_with_no_extension_is_read_only_if_its_bytes_say_so(
    deployment: Path, tmp_path: Path,
) -> None:
    root = _folder(tmp_path, {
        "NOTES": b"plain words, nothing to sniff\n",
        "CONFIG": b'{"a": 1}\n',
        "a.md": b"# A\n",
    })
    plan = _plan(deployment, root, ["**/*"])
    (warning,) = plan.warnings
    # CONFIG starts like JSON, so the reader parses it; NOTES does not.
    assert warning["files_by_type"] == {"(no extension)": 1}
    assert warning["matched_count"] == 3


def test_captioning_makes_images_readable(deployment: Path, tmp_path: Path) -> None:
    root = _folder(tmp_path, {"fig.png": b"\x89PNG\r\n\x1a\n"})
    yaml_path = deployment / "extractors.yaml"
    off = guided_add._unreadable_files(root, ["fig.png"], yaml_path, [], lower_bound=False)
    assert off["files_by_type"] == {".png": 1}
    yaml_path.write_text("captioning: {enabled: true, model: x/y}\n")
    on = guided_add._unreadable_files(root, ["fig.png"], yaml_path, [], lower_bound=False)
    assert on["unreadable_count"] == 0
    assert any(f.startswith("images, captioned") for f in on["readable_formats"])


def test_plural_parsers_file_counts_only_declared_formats(
    deployment: Path, tmp_path: Path,
) -> None:
    # `parsers:` is a shape the save step leaves untouched, so it adds nothing:
    # only Markdown is readable, and the "read today" list says so.
    yaml_path = deployment / "extractors.yaml"
    yaml_path.write_text(
        "parsers:\n  - format: markdown\n"
        "    class: okg.substrate.library.extraction.parse.markdown.MarkdownParser\n",
    )
    root = _folder(tmp_path, {"a.md": b"# A\n", "b.txt": b"text\n"})
    info = guided_add._unreadable_files(
        root, ["a.md", "b.txt"], yaml_path, ["markdown", "text"], lower_bound=False,
    )
    assert info["files_by_type"] == {".txt": 1}
    assert info["readable_formats"] == ["Markdown (.md, .markdown)"]


def test_a_file_the_save_step_leaves_unchanged_warns_but_is_not_refused(
    deployment: Path, tmp_path: Path,
) -> None:
    # The plural-keys file is never edited; the save step already warns about
    # it, and the operator can add the Markdown reader by hand. Refusing here
    # would block a flow that has its own warning.
    (deployment / "extractors.yaml").write_text(
        "parsers:\n  - format: text\n"
        "    class: okg.substrate.library.extraction.parse.text.TextParser\n",
    )
    root = _folder(tmp_path, {"a.md": b"# A\n"})
    plan = _plan(deployment, root)
    assert plan.ok, [s.as_dict() for s in plan.steps]
    (warning,) = plan.warnings
    assert warning["files_by_type"] == {".md": 1}
    assert warning["extractors_file_left_unchanged"] is True
    assert warning["fixable_by_hand_count"] == 1


def test_word_only_folder_is_refused_even_with_a_plural_parsers_file(
    deployment: Path, tmp_path: Path,
) -> None:
    # The issue's own case. A hand edit of the plural-keys file could make
    # Markdown or text readable, but nothing makes a Word file readable.
    (deployment / "extractors.yaml").write_text(
        "parsers:\n  - format: text\n"
        "    class: okg.substrate.library.extraction.parse.text.TextParser\n",
    )
    root = _folder(tmp_path, {"p1.docx": DOCX})
    plan = _plan(deployment, root, DOCX_INCLUDE)
    assert not plan.ok
    assert plan.failed_step is not None and plan.failed_step.name == "preview"
    assert "none of the 1 matched files can be read (.docx: 1)" in plan.failed_step.cause


def test_an_extensionless_json_file_gets_its_parser_from_the_save_step(
    deployment: Path, tmp_path: Path,
) -> None:
    # The standard-shape file declares only Markdown. The reader sniffs the
    # file as JSON, so the save step must add the JSON parser, not just the
    # check: the plan's reader formats and the verdict come from one list.
    (deployment / "extractors.yaml").write_text(
        "parser:\n  - format: markdown\n"
        "    class: okg.substrate.library.extraction.parse.markdown.MarkdownParser\n",
    )
    root = _folder(tmp_path, {"CONFIG": b'{"a": 1}\n'})
    plan = _plan(deployment, root, ["**/*"])
    assert plan.ok, [s.as_dict() for s in plan.steps]
    assert plan.reader_formats == ["json"]
    assert plan.warnings == []


def test_captioning_flag_follows_the_readers_truthiness(
    deployment: Path, tmp_path: Path,
) -> None:
    root = _folder(tmp_path, {"fig.png": b"\x89PNG\r\n\x1a\n"})
    yaml_path = deployment / "extractors.yaml"
    yaml_path.write_text("captioning: {enabled: 1, model: x/y}\n")
    info = guided_add._unreadable_files(root, ["fig.png"], yaml_path, [], lower_bound=False)
    assert info["unreadable_count"] == 0


def test_outcome_carries_the_plan_warnings(deployment: Path, tmp_path: Path) -> None:
    root = _folder(tmp_path, {"a.md": b"# A\n\nHiggs.\n", "p1.docx": DOCX})
    plan = _plan(deployment, root, BOTH_INCLUDE)
    outcome = AddSourceOutcome(plan=plan, status="published", steps=[])
    payload = outcome.as_dict()
    assert payload["warnings"] == plan.warnings and payload["warnings"]
    assert "WARNING: 1 of 2 matched files cannot be read" in _render_text(payload)


def test_cli_exits_1_and_explains_a_refusal(
    deployment: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    root = _folder(tmp_path, {"p1.docx": DOCX})
    code = cli.main([
        "deployment", "add-source", "unit", "papers", "--type", "local-folder",
        "--setting", f"root={root}", "--setting", 'include=["**/*.docx"]',
        "--deployments-root", str(deployment.parent),
    ])
    out = capsys.readouterr().out
    assert code == 1
    assert "none of the 1 matched files can be read (.docx: 1)" in out
    assert "Fix: narrow --setting include=" in out
