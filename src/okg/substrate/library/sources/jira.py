"""Jira source adapter (M5b library).

Profile: `mutable_api`. Produces tickets, comments, epics,
and reporter/assignee people. Cursor mode follows
`updated > :last_applied_rev`; reconcile mode walks an explicit
JQL scope (project/filter) and permits retractions on absence.

Production HTTP client uses JIRA REST API v3 + Basic auth
(email + API token). Cursor watermark is `updated` ISO8601.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from okg.substrate.library.sources._fixture import FixtureBackedAdapter


@dataclass
class JiraSource(FixtureBackedAdapter):
    name: str = "jira"
    profile: str = "mutable_api"
    fixture_path: Path | str = ""
    base_url: Optional[str] = None
    email: Optional[str] = None
    api_token: Optional[str] = None
    jql: Optional[str] = None  # scope for reconcile mode
