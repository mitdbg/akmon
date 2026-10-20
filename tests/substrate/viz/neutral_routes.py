"""Neutral coordination route policy for the viz setup tests.

The setup tests build plans and route registries against the repository root.
Coordination route names are deployment data read from
``.okg/coordination.yaml``; these tests read a neutral copy of that policy
(``tests/fixtures/coordination/neutral_coordination.yaml``) instead, so their
fixtures carry no real machine name.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.pact import project_settings


NEUTRAL_POLICY = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "coordination"
    / "neutral_coordination.yaml"
)
LOCAL_ROUTES = project_settings.LOCAL_CLIENT_ROUTE_NAMES
REMOTE_ROUTES = ("remote-a", "remote-b")
ROUTE_NAMES = LOCAL_ROUTES + REMOTE_ROUTES


def use_neutral_route_policy(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    """Read the neutral policy whenever ``root``'s runtime policy is loaded."""

    real = project_settings.runtime_settings_path
    trusted = Path(root).resolve()

    def runtime_settings_path(repo_root: Path) -> Path:
        if Path(repo_root).resolve() == trusted:
            return NEUTRAL_POLICY
        return real(repo_root)

    monkeypatch.setattr(project_settings, "runtime_settings_path", runtime_settings_path)
