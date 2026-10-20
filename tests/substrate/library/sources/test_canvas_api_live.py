"""Env-gated live verification for canvas_api
(add-academic-corpora-sources).

Skipped unless OKG_LIVE_CANVAS=1 with OKG_CANVAS_TOKEN (instructor
self-serve token), OKG_LIVE_CANVAS_BASE_URL, and
OKG_LIVE_CANVAS_COURSES set. MIT Canvas is the natural target;
until a token is minted this is an environment blocker on
task.corpora.canvas, not a waiver.
"""
from __future__ import annotations

import os

import pytest

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.canvas_api import CanvasApiSource

pytestmark = pytest.mark.skipif(
    os.environ.get("OKG_LIVE_CANVAS") != "1",
    reason="live Canvas test is env-gated (OKG_LIVE_CANVAS=1)",
)


def test_live_instructor_token_backfill_publishes() -> None:
    courses = [
        c.strip()
        for c in os.environ.get("OKG_LIVE_CANVAS_COURSES", "").split(",")
        if c.strip()
    ]
    assert courses, "set OKG_LIVE_CANVAS_COURSES to the course allowlist"
    src = CanvasApiSource(
        deployment_name=os.environ.get(
            "OKG_LIVE_CANVAS_DEPLOYMENT", "academic-researcher",
        ),
        canvas_base_url=os.environ["OKG_LIVE_CANVAS_BASE_URL"],
        canvas_courses=courses,
    )
    preflight = src.preflight()
    assert preflight.status == "ok", preflight.as_dict()

    run = src.run("live-backfill")
    facts = list(run.facts)
    course_nodes = [
        f for f in facts if isinstance(f, NodeFact) and f.subtype == "course"
    ]
    assert course_nodes, "allowlisted courses produced zero rows"
    token = os.environ["OKG_CANVAS_TOKEN"]
    from okg.substrate.cli.connector_security import find_token_leaks
    assert find_token_leaks(
        payloads=[("facts", repr(facts))], tokens=[token],
    ) == []
