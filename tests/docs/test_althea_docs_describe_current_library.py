from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs/develop/architecture/overview.md"


def test_althea_page_maps_current_modules_and_responsibilities() -> None:
    text = DOC.read_text(encoding="utf-8")
    package = ROOT / "src/okg/optimizer"
    assert "src/okg/optimizer/" in text
    assert "| Step | Input | Output |" in text
    assert "publication" in text.lower()
    for path in package.glob("*.py"):
        ast.parse(path.read_text(encoding="utf-8"))


def test_althea_page_does_not_present_an_unbuilt_autonomous_service() -> None:
    text = DOC.read_text(encoding="utf-8").lower()
    section = text.split("## optimization trials", 1)[1].split("## where to continue", 1)[0]
    assert "not an autonomous" in section
    assert "does not write" in section
