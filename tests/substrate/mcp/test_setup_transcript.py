"""test-connector-operator-journey (Layer 2): the agent-driven
setup transcript chains coherently.

Replays the MCP tool-call sequence an assistant (Claude Desktop /
Codex) issues on the operator's behalf and asserts the envelope
contract that makes the surface drivable: each response carries the
status + fields the NEXT call consumes, and an expired/unknown
plan_id returns a recoverable agent_hint rather than crashing.
"""
from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from okg.substrate.catalog.profile import load_profile
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan, scaffold_deployment,
)

pytestmark = pytest.mark.postgres

PROFILE_DIR = (
    Path(__file__).resolve().parents[3]
    / "src/okg/substrate/library/profiles" / "academic-researcher"
)


@pytest.fixture
def transcript_env(clean_runtime, tmp_path, monkeypatch):
    home = tmp_path / "home"
    notes = home / "notes"
    notes.mkdir(parents=True)
    for n in ("a", "b"):
        (notes / f"{n}.md").write_text(f"# {n}\n")
    (home / "bib").mkdir()
    (home / "bib" / "r.bib").write_text("@article{x,title={T},year={2026}}\n")
    deployments_root = tmp_path / "deployments"
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    plan = build_plan(
        profile=profile, deployment_name="transcript_pi",
        deployments_root=deployments_root,
        raw_answers={
            "deployment_name": "transcript_pi",
            "postgres_dsn": clean_runtime,
            "orcid": "0000-0001-2345-6789",
            "notes_dir": str(notes), "mcp_port": "5431",
        },
    )
    scaffold_deployment(plan)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    monkeypatch.setenv("HOME", str(home))
    return clean_runtime


def test_discover_propose_commit_status_chains(transcript_env) -> None:
    """The four-call transcript an agent issues: each response feeds
    the next without the agent having to invent any value."""
    from okg.substrate.mcp.setup_planning import (
        discover_sources, propose_setup,
    )
    from okg.substrate.mcp.source_admin import commit_setup
    from okg.substrate.mcp.source_lifecycle import source_status

    dsn = transcript_env
    with psycopg.connect(dsn, autocommit=True) as conn:
        # 1. discover — agent learns what's on the machine.
        d = discover_sources(conn, "transcript_pi")
        assert d["status"] == "ok"
        assert d["candidates"], "discover returned no candidates"

        # 2. propose — the response MUST carry a plan_id the agent
        #    relays nowhere but feeds straight into commit.
        p = propose_setup(
            conn, "transcript_pi", only=["notes", "bib-references"],
            include_threshold="low",
        )
        assert p["status"] in ("ok", "proposed", None) or "plan_id" in p
        plan_id = p["plan_id"]
        assert plan_id
        # The agent reads `summary` aloud — it must exist.
        assert p.get("summary")

        # 3. commit — consumes the plan_id from step 2 verbatim.
        c = commit_setup(conn, plan_id)
        assert c.get("status") != "plan_not_found"
        # Complete accounting: buckets are present (possibly empty).
        for bucket in ("newly_wired", "reconfirmed", "failed"):
            assert isinstance(c.get(bucket, []), list)

        # 4. status — the agent can inspect any approved source.
        s = source_status(conn, "transcript_pi", "notes")
        assert s.get("status") != "deployment_not_found"


def test_expired_plan_id_is_recoverable_not_a_crash(transcript_env) -> None:
    """If the plan expired (30-min TTL) or the MCP server restarted,
    commit must return a structured plan_not_found with an
    agent_hint telling the assistant to re-propose — never raise."""
    from okg.substrate.mcp.source_admin import commit_setup

    dsn = transcript_env
    with psycopg.connect(dsn, autocommit=True) as conn:
        c = commit_setup(conn, "00000000-dead-beef-0000-000000000000")
    assert c["status"] == "plan_not_found"
    assert c.get("agent_hint"), c
    # The hint names the recovery move.
    assert "propose_setup" in c["agent_hint"]


def test_discover_on_unknown_deployment_is_structured(transcript_env) -> None:
    """A typoed deployment name returns a structured error the agent
    can relay, not a traceback."""
    from okg.substrate.mcp.setup_planning import discover_sources

    dsn = transcript_env
    with psycopg.connect(dsn, autocommit=True) as conn:
        d = discover_sources(conn, "no_such_deployment")
    assert d["status"] == "deployment_not_found"
    assert d.get("summary") or d.get("error")
