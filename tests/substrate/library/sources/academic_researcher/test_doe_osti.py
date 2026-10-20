"""Emission test for `DoeOstiSource` — proves the source projects
Grant (funder=DOE) + the DOE funder Organization from a captured
OSTI response, without touching the network.

Part of `academic-pi-source-tail` task.aps.implement: "the other
named sources discover + project".
"""
from __future__ import annotations

from pathlib import Path

from okg.substrate.library.sources.academic_researcher.doe_osti import DoeOstiSource
from okg.substrate.library.sources.base import NodeFact, ProgressMarker

_FIXTURE = str(Path(__file__).parent / "fixtures" / "osti_sample.json")


def _facts(**kwargs):
    src = DoeOstiSource(fixture_path=_FIXTURE, **kwargs)
    return list(src.run(run_id="r").facts)


def test_emits_doe_org_and_grants() -> None:
    facts = _facts(orcid="0000-0002-1825-0097")
    grants = [
        f for f in facts if isinstance(f, NodeFact) and f.subtype == "grant"
    ]
    orgs = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "organization"
    ]
    assert {g.node_id for g in grants} == {
        "grant:doe:1899999", "grant:doe:1955501",
    }
    assert all(g.attrs["funder"] == "DOE" for g in grants)
    assert [o.node_id for o in orgs] == ["org:doe"]
    assert orgs[0].attrs["kind"] == "funding_agency"


def test_grant_carries_award_id_and_marker() -> None:
    facts = _facts(pi_name="Marie Curie")
    grant = next(
        f for f in facts
        if isinstance(f, NodeFact)
        and f.node_id == "grant:doe:1899999"
    )
    assert grant.attrs["award_id"] == "DE-SC0012345"
    markers = [f for f in facts if isinstance(f, ProgressMarker)]
    assert {m.record_id for m in markers} == {"1899999", "1955501"}


def test_run_skips_without_pi_name_or_orcid() -> None:
    src = DoeOstiSource()
    assert list(src.run(run_id="r").facts) == []
