"""The search view matrix on the reference page is the served one.

PACT read-model-s4-list (issue #3195): `docs/reference/mcp-results.md`
publishes the method/rank/view matrix and the list budget. Compared here
against `search_views`, which the server and the capability report read, so
the page cannot drift from what the tool accepts.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from okg.substrate.mcp import search_views

ROOT = Path(__file__).resolve().parents[2]


def _section(text: str, heading: str) -> str:
    return text.split(heading, 1)[1].split("\n## ", 1)[0]


def _names(cell: str) -> list[str]:
    return re.findall(r"`([a-z_]+)`", cell)


def test_the_reference_matrix_is_the_served_matrix() -> None:
    text = (ROOT / "docs/reference/mcp-results.md").read_text(encoding="utf-8")
    section = _section(text, "## Search views\n")
    documented: dict[str, dict[str, list[str]]] = {}
    for line in section.splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 3 or cells[0] in ("Method", "---"):
            continue
        views = [view.strip() for view in cells[2].split(",")]
        for method in _names(cells[0]):
            for rank in _names(cells[1]):
                documented.setdefault(method, {})[rank] = views
    served = {
        method: {rank: list(views) for rank, views in modes.items()}
        for method, modes in search_views.VIEW_MATRIX.items()
    }
    assert documented == served
    assert f"defaults to {search_views.LIST_LIMIT}" in section
    assert f"refuses above {search_views.LIST_MAX_LIMIT}" in section
    assert f"`limit` default {search_views.CARDS_LIMIT}" in section
    for field in search_views.LIST_FIELDS:
        assert f"`{field}`" in section


def test_the_refusal_code_is_registered_and_linked() -> None:
    registry = yaml.safe_load((ROOT / "docs/reference/error_codes.yaml").read_text(encoding="utf-8"))
    codes = {
        entry.get("code")
        for entries in registry.values() if isinstance(entries, list)
        for entry in entries if isinstance(entry, dict)
    }
    assert search_views.SearchViewUnsupported.code in codes
    mcp = (ROOT / "docs/reference/mcp.md").read_text(encoding="utf-8")
    assert "(mcp-results.md#search-views)" in _section(mcp, "### Search views\n")
