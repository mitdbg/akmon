"""Confluence source adapter (M5b library).

Profile: `mutable_api`. Produces document + section nodes from
Confluence pages. Production client uses Confluence REST API
v2 with Basic auth; cursor is the page's `version.when`.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from okg.substrate.library.sources._fixture import FixtureBackedAdapter


@dataclass
class ConfluenceSource(FixtureBackedAdapter):
    name: str = "confluence"
    profile: str = "mutable_api"
    fixture_path: Path | str = ""
    base_url: Optional[str] = None
    email: Optional[str] = None
    api_token: Optional[str] = None
    space_keys: tuple[str, ...] = ()
