"""The published site is organized by task and offers no paper-reviewer path.

PACT docs-structure (umbrella public-release-docs, req.public-release-docs.structure).
Pages are found through documentation.yaml and mkdocs.yml, never by a
hard-coded page path.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]

TASK_SECTIONS = [
    "Get started",
    "Connect your data",
    "Connect agents",
    "Run it as a service",
    "Reference",
    "Develop AKMON",
]

# What a reviewer reproduction path looks like on a page: the one-command
# driver, its section heading or anchor, or an address to artifact reviewers.
REVIEWER_PATH = re.compile(
    r"akmon/reproduce\.py|reproduce-the-papers-claims|reproduce the paper|"
    r"artifact[ -]review",
    re.IGNORECASE,
)


def _inventory() -> dict:
    return yaml.safe_load((ROOT / "documentation.yaml").read_text(encoding="utf-8"))


def _publication() -> dict:
    return _inventory()["publications"]["product"]


def _mkdocs() -> dict:
    return yaml.safe_load((ROOT / _publication()["config"]).read_text(encoding="utf-8"))


def _reviewer_findings(name: str, text: str) -> list[str]:
    return [
        f"{name}:{number}: {match.group(0)}"
        for number, line in enumerate(text.splitlines(), 1)
        for match in REVIEWER_PATH.finditer(line)
    ]


def test_top_level_navigation_is_the_six_task_sections_in_order() -> None:
    nav = _mkdocs()["nav"]
    labels = [next(iter(item)) for item in nav if isinstance(item, dict)]
    assert labels == TASK_SECTIONS
    assert all(isinstance(item, dict) for item in nav), "every page sits in a section"


def test_site_home_is_the_first_page_of_get_started() -> None:
    first_section = _mkdocs()["nav"][0]["Get started"]
    (home,) = first_section[0].values()
    assert "/" not in home and Path(home).stem in {"README", "index"}, home


def test_no_published_page_or_readme_offers_a_reviewer_reproduction_path() -> None:
    publication = _publication()
    source = ROOT / publication["source_dir"]
    findings: list[str] = []
    for page in publication["pages"]:
        findings += _reviewer_findings(page, (source / page).read_text(encoding="utf-8"))
    front_page = next(ROOT.glob("README.*"))
    findings += _reviewer_findings(front_page.name, front_page.read_text(encoding="utf-8"))
    assert findings == []


def test_reviewer_scan_flags_a_seeded_reviewer_section() -> None:
    seeded = (
        "## 6. Reproduce the paper's claims (artifact reviewers)\n"
        "uv run python scripts/akmon/reproduce.py --dsn x\n"
    )
    findings = _reviewer_findings("seeded", seeded)
    assert len(findings) == 3, findings
    assert _reviewer_findings("clean", "Reproduce the behavior first.\n") == []


def test_ci_and_quality_runbooks_are_off_the_site() -> None:
    unpublished = set(_publication().get("unpublished") or ())
    assert {"runbooks/ci/", "runbooks/quality/"} <= unpublished
    for page in _publication()["pages"]:
        assert not page.startswith(("runbooks/ci/", "runbooks/quality/")), page
