"""The workspace profile's sources carry a poll schedule for the runtime worker.

Without a `sync.poll` block the worker registers no source schedule, so a
plain install with `runtime.enabled: true` never refreshed. Two readers whose
change probe cannot vouch for an unchanged read are left out on purpose: a poll
would re-run them, and every source after git_files, on every tick.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

DEFAULTS = (
    Path(__file__).resolve().parents[2]
    / "src/okg/substrate/library/profiles/workspace-graph/source-defaults"
)
NOT_POLLED = {"codeql_data_flow", "profile_measurements"}


def _entries() -> dict[str, dict]:
    entries: dict[str, dict] = {}
    for path in sorted(DEFAULTS.glob("*.yaml")):
        entries.update(yaml.safe_load(path.read_text(encoding="utf-8")))
    return entries


def test_every_other_source_polls_with_its_change_probe() -> None:
    entries = _entries()
    assert NOT_POLLED <= set(entries)
    polled = sorted(set(entries) - NOT_POLLED)
    assert len(polled) == 13
    for name in polled:
        sync = entries[name]["sync"]
        assert sync["triggers"] == ["poll", "manual", "reconcile"], name
        assert sync["poll"] == {
            "probe": "change_probe",
            "interval_seconds": 300,
        }, name


@pytest.mark.parametrize("name", sorted(NOT_POLLED))
def test_unsafe_probe_sources_are_not_polled(name: str) -> None:
    assert "sync" not in _entries()[name]


def test_backstop_reconcile_runs_daily_for_one_source() -> None:
    """A backstop re-reads its whole source; every 25 minutes was too often.

    Measured on the okg repository: one full code_structure read took over
    30 minutes, so the default 5 x 300 s backstop would never let the worker
    go idle.
    """
    defaults = yaml.safe_load(
        (DEFAULTS.parent / "deployment-defaults.yaml").read_text(encoding="utf-8")
    )
    runtime = defaults["runtime"]
    assert runtime["enabled"] is False
    assert runtime["source_sync"]["poll"] == {
        "backstop_multiplier": 288,
        "backstop_dependent_fanout": "none",
    }
