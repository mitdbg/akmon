"""Static docsite source adapter (M5b library).

Profile: `discovery_crawl`. Walks a docsite (mkdocs, docusaurus,
plain markdown tree) and produces document + section + link
nodes. Production mode crawls the filesystem (or HTTP sitemap)
and retracts pages missing from a completed walk.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from okg.substrate.library.sources._fixture import FixtureBackedAdapter


@dataclass
class DocsiteSource(FixtureBackedAdapter):
    name: str = "docsite"
    profile: str = "discovery_crawl"
    fixture_path: Path | str = ""
    root: Path | str | None = None  # filesystem root for live mode
