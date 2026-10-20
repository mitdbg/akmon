"""Unit tests for `OrcidPublicationsSource` — the day-1 academic-researcher
publication source.

Pins the emission shape (paper / person / organization NodeFacts +
authored_by / affiliated_with EdgeFacts, matching the scholarly
catalog the academic-researcher profile composes), the put-code idempotency
markers, the field-route provenance, and the "no silent fallback"
contract (a configured-but-broken ORCID raises rather than emitting
empty).

Fixtures under `fixtures/orcid_*.json` are minimal but real ORCID
v3.0 response shapes for ORCID's documented example iD
0000-0002-1825-0097 (the record content is invented, for Marie Curie), served to the
source through a mocked `urlopen` so the test needs no network.
"""
from __future__ import annotations

import json
import urllib.error
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from okg.substrate.library.sources.academic_researcher.orcid_publications import (
    OrcidPublicationsSource,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourcePreflightResult,
)


_ORCID = "0000-0002-1825-0097"
_FIXTURES = Path(__file__).parent / "fixtures"


def _fixture_bytes(name: str) -> bytes:
    return (_FIXTURES / name).read_bytes()


class _StubResp:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self) -> bytes:
        return self._payload


def _urlopen_router(routes: dict[str, Any]):
    """Fake urlopen dispatching by URL substring (payload bytes or
    an Exception to raise). Records `urls_called`."""
    urls_called: list[str] = []

    def fake_urlopen(req, *args, timeout=None, **kwargs):  # noqa: ARG001
        url = req.full_url if hasattr(req, "full_url") else str(req)
        urls_called.append(url)
        for key, val in routes.items():
            if key in url:
                if isinstance(val, BaseException):
                    raise val
                return _StubResp(val)
        raise urllib.error.HTTPError(
            url=url, code=404, msg="Not Found", hdrs=None, fp=None,
        )

    fake_urlopen.urls_called = urls_called  # type: ignore[attr-defined]
    return fake_urlopen


# The source now routes through the central egress validator's no-follow opener,
# so patch THAT (patching urllib.request.urlopen no longer intercepts it).
_PATCH_TARGET = (
    "okg.substrate.library.sources.academic_researcher.orcid_publications"
    ".open_validated_url"
)


def _run_with_fixtures(src: OrcidPublicationsSource, **overrides) -> list[Any]:
    """Run the source against the full ORCID fixture trio."""
    routes = {
        "/works": _fixture_bytes("orcid_works.json"),
        "/employments": _fixture_bytes("orcid_employments.json"),
        "/person": _fixture_bytes("orcid_person.json"),
        **overrides,
    }
    fake = _urlopen_router(routes)
    with mock.patch(_PATCH_TARGET, side_effect=fake):
        run = src.run(run_id="r", mode="cursor")
        return list(run.facts)


# ---------- preflight ----------


def test_preflight_ok_with_orcid() -> None:
    res = OrcidPublicationsSource(orcid=_ORCID).preflight()
    assert isinstance(res, SourcePreflightResult)
    assert res.status == "ok"
    assert _ORCID in res.reason


def test_preflight_missing_without_orcid() -> None:
    res = OrcidPublicationsSource().preflight()
    assert res.status == "missing_credential"
    assert "orcid" in res.reason.lower()


def test_run_skips_when_unconfigured() -> None:
    run = OrcidPublicationsSource().run(run_id="r")
    assert list(run.facts) == []
    assert run.completed_scope is True


# ---------- emission shape ----------


def test_emits_pi_person_orgs_papers_and_edges() -> None:
    facts = _run_with_fixtures(OrcidPublicationsSource(orcid=_ORCID))
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    markers = [f for f in facts if isinstance(f, ProgressMarker)]

    by_subtype: dict[str, list[NodeFact]] = {}
    for n in nodes:
        by_subtype.setdefault(n.subtype, []).append(n)

    # 1 PI person, 2 employers, 3 works, and venue graph facts for the
    # two venue/year-bearing works.
    assert len(by_subtype["person"]) == 1
    assert len(by_subtype["organization"]) == 2
    assert len(by_subtype["paper"]) == 3
    assert len(by_subtype["venue"]) == 2
    assert len(by_subtype["conference"]) == 2
    assert len(by_subtype["conference_edition"]) == 2
    # 2 affiliated_with + 3 authored_by + 2 member_of + 2 published_in.
    edge_types = sorted(e.edge_type for e in edges)
    assert edge_types == [
        "affiliated_with", "affiliated_with",
        "authored_by", "authored_by", "authored_by",
        "member_of", "member_of",
        "published_in", "published_in",
    ]
    # One progress marker per work, keyed by put-code.
    assert {m.record_id for m in markers} == {"111111", "222222", "333333"}
    assert all(len(m.fingerprint) == 64 for m in markers)


def test_person_node_is_orcid_keyed() -> None:
    facts = _run_with_fixtures(OrcidPublicationsSource(orcid=_ORCID))
    person = next(
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "person"
    )
    assert person.node_id == f"person:orcid:{_ORCID}"
    assert person.attrs["orcid"] == _ORCID
    assert person.attrs["name"] == "Marie Curie"
    assert person.attrs["display_name"] == "Marie Curie"


def test_org_nodes_are_ror_keyed_with_affiliation_edges() -> None:
    facts = _run_with_fixtures(OrcidPublicationsSource(orcid=_ORCID))
    orgs = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "organization"
    ]
    org_ids = {o.node_id for o in orgs}
    assert org_ids == {"org:ror:00curie01", "org:ror:00curie02"}
    radium = next(o for o in orgs if o.node_id == "org:ror:00curie01")
    assert radium.attrs["name"] == "Radium Institute"
    assert radium.attrs["ror"] == "https://ror.org/00curie01"
    # affiliated_with goes person -> org.
    aff = [
        e for e in facts
        if isinstance(e, EdgeFact) and e.edge_type == "affiliated_with"
    ]
    assert all(e.src == f"person:orcid:{_ORCID}" for e in aff)
    assert {e.dst for e in aff} == org_ids


def test_paper_attrs_and_authored_by_edge() -> None:
    facts = _run_with_fixtures(OrcidPublicationsSource(orcid=_ORCID))
    osdi = next(
        f for f in facts
        if isinstance(f, NodeFact)
        and f.subtype == "paper"
        and f.attrs["orcid_put_code"] == "111111"
    )
    assert osdi.node_id == f"paper:orcid:{_ORCID}:111111"
    assert osdi.attrs["title"].startswith("Polonium:")
    assert osdi.attrs["year"] == "2021"
    assert osdi.attrs["venue"] == "OSDI"
    assert osdi.attrs["doi"] == "10.0000/osdi.2021.curie"
    assert osdi.attrs["source_provider"] == "orcid"
    # authored_by goes paper -> the PI person.
    authored = next(
        e for e in facts
        if isinstance(e, EdgeFact)
        and e.edge_type == "authored_by"
        and e.src == osdi.node_id
    )
    assert authored.dst == f"person:orcid:{_ORCID}"


def test_venue_bearing_works_emit_published_in_edges() -> None:
    facts = _run_with_fixtures(OrcidPublicationsSource(orcid=_ORCID))
    nodes = {
        f.node_id: f
        for f in facts
        if isinstance(f, NodeFact)
    }
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    assert "venue:osdi" in nodes
    assert "conf:osdi" in nodes
    assert "confed:osdi-2021" in nodes
    assert nodes["confed:osdi-2021"].subtype == "conference_edition"
    assert nodes["confed:osdi-2021"].attrs["year"] == "2021"

    osdi_paper = f"paper:orcid:{_ORCID}:111111"
    assert any(
        e.src == osdi_paper
        and e.dst == "confed:osdi-2021"
        and e.edge_type == "published_in"
        for e in edges
    )
    assert any(
        e.src == "confed:osdi-2021"
        and e.dst == "conf:osdi"
        and e.edge_type == "member_of"
        for e in edges
    )

    eurosys_paper = f"paper:orcid:{_ORCID}:222222"
    assert "confed:eurosys-2023" in nodes
    assert any(
        e.src == eurosys_paper
        and e.dst == "confed:eurosys-2023"
        and e.edge_type == "published_in"
        for e in edges
    )


def test_field_route_is_recorded_as_provenance() -> None:
    facts = _run_with_fixtures(OrcidPublicationsSource(orcid=_ORCID))
    routes = {
        f.attrs["orcid_put_code"]: f.attrs["field_route"]
        for f in facts
        if isinstance(f, NodeFact) and f.subtype == "paper"
    }
    # OSDI + EuroSys are recognized CS venues → DBLP enrichment route.
    assert routes["111111"] == "dblp"
    assert routes["222222"] == "dblp"
    # The DOI-less, venue-less tech report → universal OpenAlex fallback.
    assert routes["333333"] == "openalex"


def test_missing_doi_is_empty_not_dropped() -> None:
    facts = _run_with_fixtures(OrcidPublicationsSource(orcid=_ORCID))
    report = next(
        f for f in facts
        if isinstance(f, NodeFact)
        and f.subtype == "paper"
        and f.attrs["orcid_put_code"] == "333333"
    )
    assert report.attrs["doi"] == ""
    assert report.attrs["title"] == "A Tech Report Without a DOI"
    assert not any(
        isinstance(f, EdgeFact)
        and f.edge_type == "published_in"
        and f.src == report.node_id
        for f in facts
    )


def test_physics_employer_routes_to_inspire() -> None:
    """Field route reads the PI's employment discipline: a physics
    institution flips the default route to INSPIRE-HEP."""
    physics_emp = json.dumps({
        "affiliation-group": [{
            "summaries": [{
                "employment-summary": {
                    "put-code": 1,
                    "role-title": "Staff Scientist",
                    "start-date": {"year": {"value": "2020"}},
                    "end-date": None,
                    "organization": {
                        "name": "CERN (European Organization for Nuclear Research)",
                        "address": {"country": "CH"},
                    },
                },
            }],
        }],
    }).encode("utf-8")
    facts = _run_with_fixtures(
        OrcidPublicationsSource(orcid=_ORCID),
        **{"/employments": physics_emp},
    )
    routes = {
        f.attrs["field_route"]
        for f in facts
        if isinstance(f, NodeFact) and f.subtype == "paper"
    }
    # Every paper routes to INSPIRE-HEP once the employer is physics.
    assert routes == {"inspire-hep"}


# ---------- no silent fallback ----------


def test_zero_works_is_empty_success_not_error() -> None:
    """A reachable ORCID with no works still emits the PI + orgs and
    simply yields zero papers — a legitimate empty, not a failure."""
    facts = _run_with_fixtures(
        OrcidPublicationsSource(orcid=_ORCID),
        **{"/works": b'{"group": []}'},
    )
    papers = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "paper"
    ]
    persons = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "person"
    ]
    assert papers == []
    assert len(persons) == 1


def test_bad_orcid_raises_loudly() -> None:
    """A configured ORCID whose works endpoint 404s must raise — the
    publish blocks rather than silently projecting nothing (dogfood
    'no silent fallbacks')."""
    fake = _urlopen_router({
        "/works": urllib.error.HTTPError(
            url="/works", code=404, msg="Not Found", hdrs=None, fp=None,
        ),
    })
    src = OrcidPublicationsSource(orcid="0000-0000-0000-0000")
    with mock.patch(_PATCH_TARGET, side_effect=fake):
        with pytest.raises(urllib.error.HTTPError):
            src.run(run_id="r", mode="cursor")


def test_non_json_response_raises() -> None:
    src = OrcidPublicationsSource(orcid=_ORCID)
    fake = _urlopen_router({"/works": b"<html>not json</html>"})
    with mock.patch(_PATCH_TARGET, side_effect=fake):
        with pytest.raises(ValueError, match="non-JSON"):
            src.run(run_id="r", mode="cursor")


# ---------- idempotency ----------


def test_rerun_is_deterministic() -> None:
    """Same ORCID record → identical node ids + fingerprints across
    runs (put-code keyed; re-runs upsert, never duplicate)."""
    first = _run_with_fixtures(OrcidPublicationsSource(orcid=_ORCID))
    second = _run_with_fixtures(OrcidPublicationsSource(orcid=_ORCID))

    def _ids_and_hashes(facts):
        return sorted(
            (f.node_id, f.source_revision["content_sha"])
            for f in facts if isinstance(f, NodeFact)
        )

    assert _ids_and_hashes(first) == _ids_and_hashes(second)
