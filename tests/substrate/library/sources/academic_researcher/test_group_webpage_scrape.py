"""Emission test for `GroupWebpageScrapeSource` — proves the source
projects a Lab node + Advisee candidates (provenance=extracted_model)
+ advisor `advises` edges from a fixture group-page HTML, with
`urlopen` mocked (no network).

Part of `academic-pi-source-tail` task.aps.implement: "the other
named sources discover + project".
"""
from __future__ import annotations

from pathlib import Path
from unittest import mock

from okg.substrate.library.sources.academic_researcher.group_webpage_scrape import (
    GroupWebpageScrapeSource,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact

_HTML = (Path(__file__).parent / "fixtures" / "group_page_sample.html").read_bytes()
_ADVISOR = "0000-0002-1825-0097"
_PATCH = (
    "okg.substrate.library.sources.academic_researcher.group_webpage_scrape"
    ".open_validated_url"
)


class _StubResp:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self) -> bytes:
        return _HTML


def _facts(**kwargs):
    src = GroupWebpageScrapeSource(
        group_webpage="https://lab.example/people",
        lab_name="Graph Systems Lab",
        **kwargs,
    )
    # Stub DNS to a public IP so the SSRF guard (which does a real
    # getaddrinfo) passes for the docs-reserved `lab.example` host —
    # consistent with stubbing urlopen for the fetch itself.
    with (
        mock.patch(_PATCH, return_value=_StubResp()),
        mock.patch(
            "okg.substrate.library.sources._url_safety.socket.getaddrinfo",
            return_value=[(2, 1, 6, "", ("93.184.216.34", 443))],
        ),
    ):
        return list(src.run(run_id="r").facts)


def test_emits_lab_and_advisee_candidates() -> None:
    facts = _facts(advisor_orcid=_ADVISOR)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    labs = [n for n in nodes if n.subtype == "lab"]
    advisees = [n for n in nodes if n.subtype == "advisee"]

    assert len(labs) == 1
    assert labs[0].attrs["name"] == "Graph Systems Lab"
    # The three h3 member names in the fixture.
    names = {a.attrs["display_name"] for a in advisees}
    assert names == {"Alice Researcher", "Bob Builder", "Carol Junior"}
    assert all(a.attrs["_provenance"] == "extracted_model" for a in advisees)


def test_infers_roles_from_member_context() -> None:
    facts = _facts(advisor_orcid=_ADVISOR)
    roles = {
        a.attrs["display_name"]: a.attrs["role"]
        for a in facts
        if isinstance(a, NodeFact) and a.subtype == "advisee"
    }
    assert roles["Alice Researcher"] == "phd"
    assert roles["Bob Builder"] == "postdoc"
    assert roles["Carol Junior"] == "undergrad"


def test_advisor_advises_edges_when_advisor_known() -> None:
    facts = _facts(advisor_orcid=_ADVISOR)
    advises = [
        f for f in facts
        if isinstance(f, EdgeFact) and f.edge_type == "advises"
    ]
    assert len(advises) == 3
    assert all(e.src == f"person:orcid:{_ADVISOR}" for e in advises)
    assert all(e.provenance == "extracted_model" for e in advises)


def test_no_advises_edges_without_advisor() -> None:
    facts = _facts()
    advises = [
        f for f in facts
        if isinstance(f, EdgeFact) and f.edge_type == "advises"
    ]
    assert advises == []


def test_run_skips_without_webpage() -> None:
    src = GroupWebpageScrapeSource()
    assert list(src.run(run_id="r").facts) == []
