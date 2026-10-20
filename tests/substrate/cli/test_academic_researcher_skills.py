"""Tests for the academic-researcher agent-skills bundle.

The minimal five-skill set (setup, add-source, troubleshoot,
catch-me-up, author) restored after the 2026-06-09 advisor-demo
de-risk; the original 11-skill bundle was cut as non-essential
by `cut-non-essential-skills`. These guard that the profile
declares the bundle and every skill file parses.
"""
from __future__ import annotations

from pathlib import Path

import frontmatter

from okg.substrate.catalog.profile import load_profile

_PROFILE_DIR = Path(
    "src/okg/substrate/library/profiles/academic-researcher"
)

EXPECTED_SKILLS = {
    "okg-setup",
    "okg-add-source",
    "okg-troubleshoot",
    "catch-me-up",
    "okg-author",
}


def test_profile_declares_skills_dir() -> None:
    """profile.yaml SHALL reference the skills/ bundle so the
    scaffold copies it into each deployment."""
    profile = load_profile(_PROFILE_DIR / "profile.yaml")
    assert profile.skills_dir is not None
    assert profile.skills_dir.is_dir()


def test_expected_skills_present() -> None:
    files = {p.stem for p in (_PROFILE_DIR / "skills").glob("*.md")}
    assert files == EXPECTED_SKILLS


def test_each_skill_has_name_and_description_frontmatter() -> None:
    """Every skill SHALL carry `name:` + `description:` frontmatter
    (the loader contract for MCP-aware agents) with the name
    matching the filename."""
    for path in (_PROFILE_DIR / "skills").glob("*.md"):
        post = frontmatter.load(path)
        assert post.get("name") == path.stem, path
        desc = post.get("description") or ""
        assert len(desc) > 20, f"{path} description too thin"
        assert post.content.strip(), f"{path} has no body"


def test_setup_skill_references_current_operator_tools() -> None:
    """okg-setup SHALL reference the live three-tool flow (not
    retired verbs) and the day-1 ORCID source."""
    text = (_PROFILE_DIR / "skills/okg-setup.md").read_text()
    for tool in ("propose_setup", "commit_setup", "discover_sources"):
        assert tool in text
    assert "orcid-publications" in text
