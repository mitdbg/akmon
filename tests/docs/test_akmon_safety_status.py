"""Safety and support-status checks for state-changing documentation."""

from __future__ import annotations

from pathlib import Path
import re

import yaml


ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"


def _front(path: Path) -> dict[str, object]:
    text = path.read_text(encoding="utf-8")
    return yaml.safe_load(text.split("---\n", 2)[1])


def _pages() -> list[Path]:
    inventory = yaml.safe_load(
        (ROOT / "documentation.yaml").read_text(encoding="utf-8")
    )
    return [
        ROOT / publication["source_dir"] / relative
        for publication in inventory["publications"].values()
        for relative in publication["pages"]
    ]


def test_source_evaluation_avoids_manual_database_mutation() -> None:
    text = (DOCS / "start/evaluate-from-source.md").read_text(encoding="utf-8").lower()
    assert "createdb" not in text
    assert "psql" not in text


def test_canonical_pages_have_machine_readable_identity_and_audience() -> None:
    for path in _pages():
        front = _front(path)
        assert front.get("status", "active") == "active", path
        assert str(front["canonical_id"]).strip(), path
        assert str(front["audience"]).strip(), path


def test_no_canonical_page_is_marked_as_a_stub() -> None:
    for path in _pages():
        text = path.read_text(encoding="utf-8").lower()
        assert "status: stub" not in text, path
        assert not re.search(r"\b(?:stub|unfinished|draft)\b", text[:1000]), path
