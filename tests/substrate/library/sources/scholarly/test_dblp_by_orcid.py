"""Unit tests for `DblpByOrcidSource` — DBLP publication source
with the ORCID-URL → PID → name-search resolution chain
(`agent-led-mcp-onboarding` + `extend-dblp-by-orcid-with-name-fallback`).

The source emits `paper` NodeFacts matching the legacy
`DblpSource` shape. Tests pin both the emission shape and the
resolution chain's branch coverage.
"""
from __future__ import annotations

import ipaddress
import json
import urllib.error
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourcePreflightResult,
)
from okg.substrate.library.sources.scholarly.dblp_by_orcid import (
    DblpByOrcidSource,
)


@pytest.fixture(autouse=True)
def _hermetic_dns():
    """The egress guard (assert_public_http_url) now runs before each DBLP
    fetch. Resolve named hosts (dblp.org) to a public IP so the suite stays
    hermetic/offline, while IP literals resolve to themselves."""
    def fake_getaddrinfo(host, port, *a, **k):
        try:
            ipaddress.ip_address(host)
            return [(2, 1, 6, "", (host, port or 0))]
        except ValueError:
            return [(2, 1, 6, "", ("93.184.216.34", port or 0))]

    with mock.patch(
        "okg.substrate.library.sources._url_safety.socket.getaddrinfo",
        side_effect=fake_getaddrinfo,
    ):
        yield


_FIXTURE = (
    Path(__file__).parent / "fixtures" / "dblp_by_orcid_sample.xml"
)


def _http_404(url: str) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url=url, code=404, msg="Not Found", hdrs=None, fp=None,
    )


class _StubResp:
    """Mimics urlopen's context-managed response object."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self) -> bytes:
        return self._payload


def _urlopen_router(routes: dict[str, Any]):
    """Build a fake urlopen that dispatches by URL substring.

    Each entry in `routes` is keyed by a URL substring; the
    value is either:
      - bytes/str payload → returns a 200 response with that body
      - an Exception → raised when called
    Calls record `urls_called` for assertion.
    """
    urls_called: list[str] = []

    def fake_urlopen(req, *args, timeout=None, **kwargs):  # noqa: ARG001
        url = req.full_url if hasattr(req, "full_url") else str(req)
        urls_called.append(url)
        for key, val in routes.items():
            if key in url:
                if isinstance(val, BaseException):
                    raise val
                payload = (
                    val.encode("utf-8") if isinstance(val, str) else val
                )
                return _StubResp(payload)
        raise _http_404(url)

    fake_urlopen.urls_called = urls_called  # type: ignore[attr-defined]
    return fake_urlopen


def _iter_records(facts) -> list[Any]:
    """Drain a SourceRun.facts iterable."""
    return list(facts)


# ---------- preflight ----------


def test_preflight_ok_with_orcid() -> None:
    src = DblpByOrcidSource(orcid="0000-0000-0000-0001")
    res = src.preflight()
    assert isinstance(res, SourcePreflightResult)
    assert res.status == "ok"
    assert res.required is False


def test_preflight_ok_with_pid_only() -> None:
    src = DblpByOrcidSource(dblp_pid="000/0001")
    res = src.preflight()
    assert res.status == "ok"
    assert "000/0001" in res.reason


def test_preflight_ok_with_name_only() -> None:
    src = DblpByOrcidSource(author_name="Marie Curie")
    res = src.preflight()
    assert res.status == "ok"
    assert "Marie Curie" in res.reason


def test_preflight_missing_when_all_empty() -> None:
    src = DblpByOrcidSource()
    res = src.preflight()
    assert res.status == "missing_credential"
    # The hint names all three configurable fields so the operator
    # can pick whichever they have.
    assert "orcid" in res.reason.lower()
    assert "dblp_pid" in res.reason.lower()
    assert "author_name" in res.reason.lower()


# ---------- _stream parses fixture into Paper NodeFacts ----------


def test_stream_emits_paper_nodefact_per_record(tmp_path: Path) -> None:
    src = DblpByOrcidSource(
        orcid="0000-0000-0000-0001",
        cache_root=str(tmp_path),
    )
    facts = list(src._stream(_FIXTURE.read_bytes()))
    # 4 records * (paper + venue + conference + edition + 2 edges + marker).
    assert len(facts) == 28
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]
    markers = [f for f in facts if isinstance(f, ProgressMarker)]
    assert len(nodes) == 16
    assert len(edges) == 8
    assert len(markers) == 4
    # All node IDs are paper:<dblp_key>.
    ids = {n.node_id for n in nodes if n.subtype == "paper"}
    assert ids == {
        "paper:conf/sosp/AlphaB23",
        "paper:journals/vldb/Alpha22",
        "paper:conf/nips/AlphaG24",
        "paper:journals/jacm/Markup25",
    }
    assert {n.subtype for n in nodes} == {
        "conference",
        "conference_edition",
        "paper",
        "venue",
    }


def test_title_flattens_inline_markup() -> None:
    """DBLP titles sometimes wrap inline markup (e.g. `<i>`,
    `<sup>`). The flattener SHALL preserve the surrounding text
    and inline content in document order — naive `text` reads
    drop everything after the first child element."""
    src = DblpByOrcidSource(
        orcid="0000-0000-0000-0001",
    )
    facts = list(src._stream(_FIXTURE.read_bytes()))
    markup_paper = next(
        f for f in facts
        if isinstance(f, NodeFact)
        and f.node_id == "paper:journals/jacm/Markup25"
    )
    title = markup_paper.attrs["title"]
    # Pre-fix bug: title would be "Inference for" (everything
    # after the first `<i>` dropped). Post-fix: full title with
    # markup flattened.
    assert "Inference for" in title
    assert "T" in title
    assert "shaped Distributions" in title
    assert "R" in title
    assert "n" in title
    assert title.endswith(".")


def test_paper_attrs_match_legacy_dblp_shape() -> None:
    src = DblpByOrcidSource(
        orcid="0000-0000-0000-0001",
    )
    facts = list(src._stream(_FIXTURE.read_bytes()))
    sosp = next(
        f for f in facts
        if isinstance(f, NodeFact)
        and f.node_id == "paper:conf/sosp/AlphaB23"
    )
    attrs = sosp.attrs
    # Keys downstream relies on (matches DblpSource emissions).
    expected_keys = {
        "paper_id", "label", "title", "year", "venue", "authors",
        "doi", "dblp_key", "dblp_url",
        "identity_match_kind", "source_provider", "text",
    }
    assert expected_keys.issubset(attrs.keys())
    assert attrs["title"].startswith("Cache-Coherent Throughput")
    assert attrs["year"] == "2023"
    assert attrs["venue"] == "SOSP"
    assert attrs["authors"] == ["Alpha Beta", "Charlie Delta"]
    assert attrs["dblp_key"] == "conf/sosp/AlphaB23"
    assert attrs["dblp_url"] == "https://dblp.org/rec/conf/sosp/AlphaB23"
    assert attrs["source_provider"] == "dblp"
    assert attrs["identity_match_kind"] == "new"


def test_stream_emits_venue_links_for_venue_year_records() -> None:
    src = DblpByOrcidSource(
        orcid="0000-0000-0000-0001",
    )
    facts = list(src._stream(_FIXTURE.read_bytes()))
    nodes = {
        f.node_id: f
        for f in facts
        if isinstance(f, NodeFact)
    }
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    assert "venue:sosp" in nodes
    assert "conf:sosp" in nodes
    assert "confed:sosp-2023" in nodes
    assert nodes["confed:sosp-2023"].subtype == "conference_edition"
    assert nodes["confed:sosp-2023"].attrs["year"] == "2023"
    assert any(
        e.src == "paper:conf/sosp/AlphaB23"
        and e.dst == "confed:sosp-2023"
        and e.edge_type == "published_in"
        for e in edges
    )
    assert any(
        e.src == "confed:sosp-2023"
        and e.dst == "conf:sosp"
        and e.edge_type == "member_of"
        for e in edges
    )


def test_progress_marker_per_record_carries_content_hash() -> None:
    src = DblpByOrcidSource(
        orcid="0000-0000-0000-0001",
    )
    facts = list(src._stream(_FIXTURE.read_bytes()))
    markers = [f for f in facts if isinstance(f, ProgressMarker)]
    keys = {m.record_id for m in markers}
    assert keys == {
        "conf/sosp/AlphaB23",
        "journals/vldb/Alpha22",
        "conf/nips/AlphaG24",
        "journals/jacm/Markup25",
    }
    # Each fingerprint is a deterministic content hash.
    assert all(len(m.fingerprint) == 64 for m in markers)


# ---------- malformed XML falls back gracefully ----------


def test_stream_handles_xml_parse_error() -> None:
    src = DblpByOrcidSource(
        orcid="0000-0000-0000-0001",
    )
    facts = list(src._stream(b"<?xml version=\"1.0\"?><not-closed"))
    assert facts == []


# ---------- cache write + offline fallback ----------


def test_orcid_url_success_caches_and_returns(tmp_path: Path) -> None:
    src = DblpByOrcidSource(
        orcid="0000-0000-0000-0001", cache_root=str(tmp_path),
    )
    payload = _FIXTURE.read_bytes()
    fake = _urlopen_router({"/orcid/": payload})
    with mock.patch(
        "okg.substrate.library.sources.scholarly.dblp_by_orcid"
        ".open_validated_url",
        side_effect=fake,
    ):
        out = src._fetch_xml()
    assert out == payload
    cached = tmp_path / "0000-0000-0000-0001.xml"
    assert cached.is_file()
    # Only the ORCID URL was hit — no search/pid fallback fired.
    assert len(fake.urls_called) == 1
    assert "/orcid/" in fake.urls_called[0]


def test_orcid_url_404_falls_back_to_name_search(tmp_path: Path) -> None:
    """Real ecosystem case: operator's ORCID isn't linked at DBLP,
    but their name resolves to exactly one author. The source
    SHALL chain ORCID-404 → search → PID-XML transparently."""
    src = DblpByOrcidSource(
        orcid="0000-0002-1825-0097",
        author_name="Marie Curie",
        cache_root=str(tmp_path),
    )
    pid_xml = _FIXTURE.read_bytes()
    search_json = json.dumps({
        "result": {"hits": {
            "@total": "1",
            "hit": [{
                "info": {
                    "author": "Marie Curie",
                    "url": "https://dblp.org/pid/000/0001",
                },
            }],
        }},
    })
    fake = _urlopen_router({
        "/orcid/": _http_404("/orcid/x"),
        "/search/author/api": search_json,
        "/pid/000/0001.xml": pid_xml,
    })
    with mock.patch(
        "okg.substrate.library.sources.scholarly.dblp_by_orcid"
        ".open_validated_url",
        side_effect=fake,
    ):
        out = src._fetch_xml()
    assert out == pid_xml
    # Order matters: ORCID → search → PID.
    assert "/orcid/" in fake.urls_called[0]
    assert "/search/author/api" in fake.urls_called[1]
    assert "/pid/000/0001.xml" in fake.urls_called[2]
    # Cache is ORCID-keyed regardless of which path produced bytes.
    assert (tmp_path / "0000-0002-1825-0097.xml").is_file()


def test_explicit_pid_bypasses_orcid_and_search(tmp_path: Path) -> None:
    src = DblpByOrcidSource(
        orcid="",
        dblp_pid="000/0001",
        author_name="ignored",
        cache_root=str(tmp_path),
    )
    payload = _FIXTURE.read_bytes()
    fake = _urlopen_router({"/pid/000/0001.xml": payload})
    with mock.patch(
        "okg.substrate.library.sources.scholarly.dblp_by_orcid"
        ".open_validated_url",
        side_effect=fake,
    ):
        out = src._fetch_xml()
    assert out == payload
    # Only the PID URL was hit — ORCID and search were skipped.
    assert len(fake.urls_called) == 1
    assert "/pid/000/0001.xml" in fake.urls_called[0]
    # Cache falls back to PID-keyed when ORCID is absent.
    assert (tmp_path / "pid_000_0001.xml").is_file()


def test_name_search_ambiguous_soft_fails(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    src = DblpByOrcidSource(
        orcid="0000-0001-XXXX-YYYY",
        author_name="John Smith",
        cache_root=str(tmp_path),
    )
    search_json = json.dumps({
        "result": {"hits": {"@total": "27", "hit": []}},
    })
    fake = _urlopen_router({
        "/orcid/": _http_404("/orcid/x"),
        "/search/author/api": search_json,
    })
    with caplog.at_level("WARNING"):
        with mock.patch(
            "okg.substrate.library.sources.scholarly.dblp_by_orcid"
            ".open_validated_url",
            side_effect=fake,
        ):
            run = src.run(run_id="r", mode="cursor")
    assert list(run.facts) == []
    assert run.completed_scope is True
    # Operator-actionable fix hint must mention --dblp-pid.
    assert any(
        "27 DBLP authors" in rec.message and "--dblp-pid" in rec.message
        for rec in caplog.records
    )


def test_name_search_zero_hits_soft_fails(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    src = DblpByOrcidSource(
        author_name="Definitely Not A Real Person 99",
        cache_root=str(tmp_path),
    )
    search_json = json.dumps({
        "result": {"hits": {"@total": "0", "hit": []}},
    })
    fake = _urlopen_router({"/search/author/api": search_json})
    with caplog.at_level("WARNING"):
        with mock.patch(
            "okg.substrate.library.sources.scholarly.dblp_by_orcid"
            ".open_validated_url",
            side_effect=fake,
        ):
            run = src.run(run_id="r", mode="cursor")
    assert list(run.facts) == []
    assert any(
        "matched 0 DBLP authors" in rec.message
        for rec in caplog.records
    )


def test_name_search_url_quotes_unicode() -> None:
    """`urllib.parse.quote` percent-encodes UTF-8 — verify the
    search URL the source builds for a name with diacritics."""
    src = DblpByOrcidSource(author_name="Jürgen Schöning")
    search_json = json.dumps({
        "result": {"hits": {"@total": "0", "hit": []}},
    })
    fake = _urlopen_router({"/search/author/api": search_json})
    with mock.patch(
        "okg.substrate.library.sources.scholarly.dblp_by_orcid"
        ".open_validated_url",
        side_effect=fake,
    ):
        src._resolve_pid_from_name("Jürgen Schöning")
    # The quoted name MUST percent-encode the umlauts (UTF-8 → %C3%BC etc.)
    assert any(
        "J%C3%BCrgen%20Sch%C3%B6ning" in u
        for u in fake.urls_called
    )


def test_fetch_falls_back_to_cache_on_network_error(
    tmp_path: Path,
) -> None:
    src = DblpByOrcidSource(
        orcid="0000-0000-0000-0001", cache_root=str(tmp_path),
    )
    cached = tmp_path / "0000-0000-0000-0001.xml"
    payload = _FIXTURE.read_bytes()
    cached.write_bytes(payload)
    with mock.patch(
        "okg.substrate.library.sources.scholarly.dblp_by_orcid"
        ".open_validated_url",
        side_effect=urllib.error.URLError("no route"),
    ):
        out = src._fetch_xml()
    assert out == payload


def test_pid_extraction_from_various_url_shapes() -> None:
    """The PID extractor is stable across http/https + trailing
    slash + with-without -okg suffix variants."""
    from okg.substrate.library.sources.scholarly.dblp_by_orcid import (
        DblpByOrcidSource as Src,
    )
    assert Src._pid_from_author_url("https://dblp.org/pid/000/0001") == "000/0001"
    assert Src._pid_from_author_url("http://dblp.org/pid/000/0001/") == "000/0001"
    assert Src._pid_from_author_url("https://dblp.org/pid/i/IhabFIlyas") == "i/IhabFIlyas"
    assert Src._pid_from_author_url("") == ""
    assert Src._pid_from_author_url("https://dblp.org/author/x") == ""


def test_run_returns_empty_when_all_inputs_blank() -> None:
    src = DblpByOrcidSource()
    run = src.run(run_id="rid", mode="cursor")
    assert list(run.facts) == []
    assert run.completed_scope is True


def test_run_returns_empty_when_all_paths_fail(
    tmp_path: Path,
) -> None:
    """ORCID 404 + ambiguous name → both soft-fail → empty
    SourceRun without raising."""
    src = DblpByOrcidSource(
        orcid="0000-0001-XXXX-YYYY",
        author_name="John Smith",
        cache_root=str(tmp_path / "no-cache-here"),
    )
    search_json = json.dumps({
        "result": {"hits": {"@total": "27", "hit": []}},
    })
    fake = _urlopen_router({
        "/orcid/": _http_404("/orcid/x"),
        "/search/author/api": search_json,
    })
    with mock.patch(
        "okg.substrate.library.sources.scholarly.dblp_by_orcid"
        ".open_validated_url",
        side_effect=fake,
    ):
        run = src.run(run_id="r", mode="cursor")
    assert list(run.facts) == []
    assert run.completed_scope is True
