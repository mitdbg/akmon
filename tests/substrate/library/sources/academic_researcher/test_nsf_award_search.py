"""Emission test for `NsfAwardSearchSource` — proves the source
projects Grant + funder Organization + derived grant-report
Deadlines (with `has_deadline` edges) from a captured NSF Award
Search response, without touching the network.

Part of `academic-pi-source-tail` task.aps.implement: "the other
named sources discover + project".
"""
from __future__ import annotations

from pathlib import Path

from okg.substrate.library.sources.academic_researcher.nsf_award_search import (
    NsfAwardSearchSource,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
)

_FIXTURE = str(Path(__file__).parent / "fixtures" / "nsf_awards_sample.json")


def _facts():
    src = NsfAwardSearchSource(pi_name="Alex Example", fixture_path=_FIXTURE)
    return list(src.run(run_id="r").facts)


def test_emits_funder_org_and_grants() -> None:
    facts = _facts()
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    grants = [n for n in nodes if n.subtype == "grant"]
    orgs = [n for n in nodes if n.subtype == "organization"]

    assert {g.node_id for g in grants} == {
        "grant:nsf:2012345", "grant:nsf:2098765",
    }
    assert all(g.attrs["funder"] == "NSF" for g in grants)
    # Exactly one funder Organization (NSF), idempotent across awards.
    assert [o.node_id for o in orgs] == ["org:nsf"]
    assert orgs[0].attrs["kind"] == "funding_agency"


def test_derives_grant_report_deadlines_with_edges() -> None:
    facts = _facts()
    deadlines = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "deadline"
    ]
    has_deadline = [
        f for f in facts
        if isinstance(f, EdgeFact) and f.edge_type == "has_deadline"
    ]
    assert deadlines, "expected derived quarterly grant-report deadlines"
    assert all(d.attrs["kind"] == "grant_report" for d in deadlines)
    # Every deadline is linked back to its grant.
    assert len(has_deadline) == len(deadlines)
    grant_ids = {"grant:nsf:2012345", "grant:nsf:2098765"}
    assert all(e.src in grant_ids for e in has_deadline)


def test_one_progress_marker_per_award() -> None:
    facts = _facts()
    markers = [f for f in facts if isinstance(f, ProgressMarker)]
    assert {m.record_id for m in markers} == {"2012345", "2098765"}


def test_run_skips_without_pi_name() -> None:
    src = NsfAwardSearchSource()
    assert list(src.run(run_id="r").facts) == []
