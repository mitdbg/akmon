"""Keep the system overview connected to implemented exchange components."""
from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ARCHITECTURE_DOC = REPO_ROOT / "docs/develop/architecture/overview.md"


def _doc_text() -> str:
    return ARCHITECTURE_DOC.read_text(encoding="utf-8")


def test_exchange_section_names_current_packages() -> None:
    text = _doc_text()
    for package in (
        "src/okg/substrate/exchange/",
        "src/okg/substrate/nomos/",
        "src/okg/substrate/federation/",
    ):
        assert package in text


def test_exchange_section_preserves_receiver_boundaries() -> None:
    section = _doc_text().split("## Exchange between deployments", 1)[1].split(
        "## Optimization trials", 1
    )[0]
    assert "| Service | Receives | Returns | Boundary |" in section
    assert "receiver" in section.lower()
    assert "publication" in section.lower()
