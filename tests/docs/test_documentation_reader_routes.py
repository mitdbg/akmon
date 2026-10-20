from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def test_docs_home_routes_to_the_manual_and_runbooks_cover_procedures() -> None:
    home = (ROOT / "docs/README.md").read_text(encoding="utf-8")
    runbooks = (ROOT / "docs/runbooks/index.md").read_text(encoding="utf-8")
    for route in (
        "start/concepts.md",
        "start/data-lifecycle.md",
        "use/sources/index.md",
        "use/search/index.md",
        "operate/index.md",
        "develop/architecture/overview.md",
        "reference/cli.md",
        "reference/capabilities.md",
    ):
        assert route in home
    # The public quickstart is the home's first route (public-release-docs,
    # docs-quickstart section); the install guide stays linked for detail.
    assert "start/quickstart.md" in home
    assert home.index("start/quickstart.md") < home.index("start/concepts.md")
    assert "operate/install.md" in home
    # Agent workflow and deployment-specific release runbooks are internal;
    # the published runbook index must not route to them.
    assert "agents/" not in runbooks
    assert "release/" not in runbooks


def test_runbook_index_does_not_route_to_ci_or_quality_procedures() -> None:
    # The CI and QA runbooks left the published site (public-release-docs,
    # docs-structure section), so the published index must not link them.
    runbooks = (ROOT / "docs/runbooks/index.md").read_text(encoding="utf-8")
    assert "quality/" not in runbooks
    assert "ci/" not in runbooks


def test_every_page_has_a_recognized_reader() -> None:
    inventory = yaml.safe_load(
        (ROOT / "documentation.yaml").read_text(encoding="utf-8")
    )
    for relative in inventory["publications"]["product"]["pages"]:
        text = (ROOT / "docs" / relative).read_text(encoding="utf-8")
        front = yaml.safe_load(text.split("---", 2)[1])
        audiences = {
            value.strip()
            for value in str(front.get("audience", "")).split(",")
            if value.strip()
        }
        assert audiences <= {
            "user",
            "data owner",
            "operator",
            "developer",
            "coding agent",
            "maintainer",
            "researcher",
        }, relative
