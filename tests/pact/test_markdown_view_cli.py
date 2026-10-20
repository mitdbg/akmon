from __future__ import annotations

import json
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from okg.substrate.cli.main import main
from okg.substrate.pact.markdown_view import check_markdown_view
from okg.substrate.pact.store import load_manifest, save_manifest
from tests.pact.test_cli import _build_demo, _seed_root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_render_and_check_view_cli(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)

    assert main([
        "pact", "--root", str(root), "view", "demo",
        "--format=markdown", "--json",
    ]) == 0
    rendered = _json_out(capsys)
    assert rendered["path"].endswith("pact.md")
    assert rendered["source_digest"].startswith("sha256:")
    assert (root / "pact" / "changes" / "demo" / "pact.md").is_file()

    checked = check_markdown_view(root, "demo")
    assert checked["status"] == "fresh"


def test_v4_method_metadata_renders_pact_shape(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    manifest = load_manifest(root, "demo")
    assert manifest.pact_version == 4
    manifest.change.method_metadata["pact_shape"] = "full"
    save_manifest(root, manifest)

    assert main([
        "pact", "--root", str(root), "view", "demo",
        "--format=markdown", "--json",
    ]) == 0
    _json_out(capsys)

    text = (root / "pact" / "changes" / "demo" / "pact.md").read_text(
        encoding="utf-8"
    )
    assert "PACT shape: `full`" in text


@pytest.mark.parametrize("json_args", [[], ["--json"]])
def test_render_markdown_view_survives_stdout_redirect_to_same_file(
    tmp_path: Path,
    json_args: list[str],
) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    view_path = root / "pact" / "changes" / "demo" / "pact.md"

    with view_path.open("w", encoding="utf-8") as stdout:
        with redirect_stdout(stdout):
            assert main([
                "pact", "--root", str(root), "view", "demo",
                "--format=markdown",
                *json_args,
            ]) == 0

    assert view_path.read_text(encoding="utf-8").startswith("---\n")
    checked = check_markdown_view(root, "demo")
    assert checked["status"] == "fresh"


def test_status_reports_view_freshness(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    assert main([
        "pact", "--root", str(root), "view", "demo",
        "--format=markdown", "--json",
    ]) == 0

    assert main(["pact", "--root", str(root), "view", "demo", "--format=status", "--json"]) == 0
    status = _json_out(capsys)

    assert status["review_view"]["status"] == "on-demand"
    assert status["human_review"].endswith("pact.md")


def test_check_view_detects_missing_view(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)

    checked = check_markdown_view(root, "demo")

    assert checked["status"] == "missing"
    assert checked["issues"][0]["code"] == "missing_view"
