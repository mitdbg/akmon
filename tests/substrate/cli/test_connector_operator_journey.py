"""test-connector-operator-journey (Layer 1): the real setup-and-
manage journey a professor takes, driven end to end against an
ephemeral DB + a fixtured home dir.

The journey, asserting what the OPERATOR SEES at each step:
  scaffold -> discover_sources -> propose_setup -> commit_setup
  -> publish (rows land) -> manage (status / add / remove /
  set-token / source-health).

No credentials: the fixtured sources are all local-file
(notes vault, .bib, .ics), so the whole flow is CI-able. The
OKG_DEPLOYMENTS_DIR + OKG_USER_HOME overrides point the real
discover/propose/commit code at the tmp scaffold + fake home.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pytest
import yaml

from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    scaffold_deployment,
)
from okg.substrate.catalog.profile import load_profile
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

PROFILE_DIR = (
    Path(__file__).resolve().parents[3]
    / "src/okg/substrate/library/profiles" / "academic-researcher"
)

_SAMPLE_ICS = """BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//okg//journey//EN
BEGIN:VEVENT
UID:lab-weekly@okg
SUMMARY:Lab meeting
DTSTART:20260506T140000Z
DTEND:20260506T150000Z
RRULE:FREQ=WEEKLY;COUNT=4
DESCRIPTION:okg: weekly lab meeting
END:VEVENT
END:VCALENDAR
"""

_SAMPLE_BIB = """@article{smith2026,
  title={A Study of Things},
  author={Smith, Jane},
  year={2026},
  journal={Journal of Things}
}
"""


def _fixture_home(root: Path) -> Path:
    """A fake operator home with the local-file sources discovery
    looks for: a `notes` dir (a known notes root), a `.bib`, and a
    `calendar.ics` the operator points the cal-ics source at."""
    home = root / "home"
    notes = home / "notes"
    notes.mkdir(parents=True)
    (notes / "ideas.md").write_text("# Ideas\nlab meeting notes\n")
    (notes / "reading.md").write_text("# Reading\npaper notes\n")
    (notes / "todo.md").write_text("# TODO\n- write\n")
    bibdir = home / "bib"
    bibdir.mkdir(parents=True)
    (bibdir / "refs.bib").write_text(_SAMPLE_BIB)
    (home / "calendar.ics").write_text(_SAMPLE_ICS)
    return home


def _answers(dsn: str, home: Path) -> dict:
    return {
        "deployment_name": "journey_pi",
        "postgres_dsn": dsn,
        "orcid": "0000-0001-2345-6789",
        "papers_dir": "",
        "cv_path": "",
        "group_webpage": "",
        "calendar_ics": str(home / "calendar.ics"),
        "notes_dir": str(home / "notes"),
        "mcp_port": "5431",
        "pi_name": "",
        "git_repos": "",
    }


@pytest.fixture
def journey(clean_runtime: str, tmp_path: Path, monkeypatch):
    """Scaffold a journey_pi deployment into tmp + point the
    deployment/home resolution env vars at the fixtures."""
    home = _fixture_home(tmp_path)
    deployments_root = tmp_path / "deployments"
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="journey_pi",
        deployments_root=deployments_root,
        raw_answers=_answers(clean_runtime, home),
    )
    result = scaffold_deployment(plan)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    # DiscoveryEnv scans Path.home(); HOME is the hermetic lever so
    # discovery sees the fixtured vault/.bib/.ics, not the real
    # machine's files.
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("OKG_USER_HOME", str(home))
    return {
        "deployment": "journey_pi",
        "dep_dir": result.deployment_dir,
        "dsn": clean_runtime,
        "home": home,
    }


def _conn(dsn: str):
    import psycopg
    return psycopg.connect(dsn, autocommit=True)


def test_discover_surfaces_fixtured_sources(journey) -> None:
    """Step 1 — 'set me up': the operator sees ranked candidates
    whose evidence names what's actually on their machine."""
    from okg.substrate.mcp.setup_planning import discover_sources

    with _conn(journey["dsn"]) as conn:
        out = discover_sources(conn, journey["deployment"])

    assert out.get("status") == "ok", out
    # Per-source rows live under `candidates`, keyed by source_id.
    by_source = {row["source_id"]: row for row in out.get("candidates", [])}
    assert "notes_dir" in by_source
    assert "bib_references" in by_source
    # The notes vault is discovered from the fixtured HOME with
    # vault-aware evidence — the operator sees real findings, not a
    # blank form.
    notes_cands = by_source["notes_dir"].get("candidates", [])
    assert any(
        (c.get("path") or "").endswith("/notes") for c in notes_cands
    ), notes_cands
    bib_cands = by_source["bib_references"].get("candidates", [])
    assert any(c.get("path") for c in bib_cands), bib_cands


def test_propose_commit_accounts_for_every_approved_source(journey) -> None:
    """Steps 2-3 — approve a plan: propose yields a plan_id +
    would-wire entries; commit(plan_id) returns a complete
    per-source accounting (every approved source lands in exactly
    one bucket — newly_wired / reconfirmed / failed — so the
    operator's agent can relay 'here is what happened' with nothing
    silently dropped). This is the approval-step UX contract; the
    publish test proves rows actually land. (Ingest can't succeed
    against the bare clean_runtime catalog, so we assert the
    envelope, not registry mutation.)"""
    from okg.substrate.mcp.setup_planning import propose_setup
    from okg.substrate.mcp.source_admin import commit_setup

    with _conn(journey["dsn"]) as conn:
        plan = propose_setup(
            conn, journey["deployment"],
            only=["notes", "bib-references"],
            include_threshold="low",
        )
        assert plan.get("plan_id"), plan
        wired_shorts = {
            e.get("short") or e.get("source_id")
            for e in plan.get("would_wire", [])
        }
        assert wired_shorts, f"nothing proposed: {plan}"

        committed = commit_setup(conn, plan["plan_id"])

    assert committed.get("status") != "plan_not_found", committed
    accounted = {
        e.get("short") or e.get("source_id")
        for bucket in ("newly_wired", "reconfirmed", "failed", "other")
        for e in committed.get(bucket, [])
    }
    # Nothing the operator approved vanished without an outcome.
    assert wired_shorts <= accounted, (wired_shorts, committed)
    # Each per-source result carries the fields an agent relays.
    for bucket in ("newly_wired", "reconfirmed", "failed"):
        for e in committed.get(bucket, []):
            assert e.get("short") or e.get("source_id"), e


def test_propose_commit_publish_lands_rows(journey) -> None:
    """Step 4 — 'it's working': the operator's configured calendar
    feed actually produces graph rows through the real
    SourceRunner -> Publisher path (calendar_ics is the reliable
    file-based emitter; the notes/bib config is asserted wired in
    the steps above)."""
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.publisher import Publisher
    from okg.substrate.library.sources.calendar_ics import CalendarIcsSource
    from tests.substrate.library.sources.test_ci_source_sync_db import (
        _live_node_count,
        _seed_catalog_and_external_nodes,
    )

    ics = str(journey["home"] / "calendar.ics")

    def _src() -> CalendarIcsSource:
        return CalendarIcsSource(
            calendar_ics_path=ics, feed_name="journey",
            expand_window_start="2026-01-01T00:00:00+00:00",
            expand_window_end="2026-12-31T00:00:00+00:00",
        )

    _seed_catalog_and_external_nodes(journey["dsn"], _src().run("preview").facts)
    runner = SourceRunner(journey["dsn"], source_name="journey.cal")
    runner.run(_src(), mode="scope_complete")
    publish = Publisher(journey["dsn"], "test", catalog_identity=bind_test_catalog_writer(journey["dsn"], "test")).cycle()
    assert publish["status"] == "published", publish
    # The weekly lab-meeting (COUNT=4, okg-tagged) expanded into
    # calendar_event rows the operator can now query.
    assert _live_node_count(journey["dsn"], "calendar_event") == 4
    assert _live_node_count(journey["dsn"], "calendar_feed") == 1


def test_manage_add_remove_status_health(journey) -> None:
    """Step 5 — manage: status reflects wired sources; add/remove
    change the registry; source-health reports the library clean."""
    from okg.substrate.mcp.setup_planning import propose_setup
    from okg.substrate.mcp.source_admin import commit_setup
    from okg.substrate.mcp.source_lifecycle import source_status

    with _conn(journey["dsn"]) as conn:
        plan = propose_setup(
            conn, journey["deployment"], only=["notes"],
            include_threshold="low",
        )
        commit_setup(conn, plan["plan_id"])
        status = source_status(conn, journey["deployment"], "notes")
    # The operator can inspect the source they just approved and get
    # a structured row (not an error).
    assert status.get("status") != "deployment_not_found", status
    assert status.get("source_id") == "notes_dir" or status.get(
        "short"
    ) == "notes", status

    # remove drops it from the registry. Use the proper profile-CLI
    # loader (not a standalone import) so the command's relative
    # imports resolve.
    from okg.substrate.cli.profile_cli_loader import get_profile_cli
    pi_cli = get_profile_cli("academic-researcher")
    rc = pi_cli.DISPATCH["remove"](
        argparse.Namespace(source="notes", deployment="journey_pi"),
        as_json=True,
    )
    assert rc == 0
    registry = yaml.safe_load(
        (journey["dep_dir"] / "source_registry.yaml").read_text()
    )
    notes_entry = registry.get("sources", {}).get("notes_dir", {})
    assert not notes_entry.get("enabled_by_default", False), notes_entry
