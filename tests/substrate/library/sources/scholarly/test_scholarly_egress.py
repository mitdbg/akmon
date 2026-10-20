"""Scholarly-adapter egress validation (centralize-egress-url-validation).

The scholarly adapters fetch from operator/config-overridable endpoints and
(for Semantic Scholar) attach an x-api-key. An endpoint override to a private /
loopback / cloud-metadata host must be refused BEFORE any request — and before
the API key is attached — so it cannot exfiltrate the key or reach internal
services.
"""
from __future__ import annotations

from unittest import mock

import pytest

from okg.substrate.library.sources._url_safety import UnsafeFetchURL
from okg.substrate.library.sources.scholarly import openalex, semantic_scholar


def _fake_urlopen(payload: bytes = b'{"data": []}'):
    # The scholarly adapters route through open_validated_url, which builds a
    # no-follow opener and calls opener.open() — patching urllib.request.urlopen
    # no longer intercepts. Patch the opener factory instead. The opener is built
    # only AFTER the initial-URL validation passes, so a refused (private/
    # metadata/loopback) URL never reaches it: `.called` distinguishes
    # allowed (True) from refused (False).
    cm = mock.MagicMock()
    resp = mock.MagicMock()
    resp.read.return_value = payload
    cm.__enter__.return_value = resp
    cm.__exit__.return_value = False
    fake_opener = mock.MagicMock()
    fake_opener.open.return_value = cm
    return mock.patch(
        "okg.substrate.library.sources._url_safety.urllib.request.build_opener",
        return_value=fake_opener,
    )


def test_semantic_scholar_metadata_endpoint_refused_before_key(monkeypatch):
    # Key is present: if the guard failed, it would be sent. The literal
    # metadata IP is rejected by the SSRF guard before any request.
    monkeypatch.setenv("SEMANTIC_SCHOLAR_API_KEY", "sk-secret")
    with _fake_urlopen() as uo:
        with pytest.raises(UnsafeFetchURL):
            semantic_scholar._fetch_json(
                "http://169.254.169.254/graph/v1/paper",
                {"_lookup_id": "DOI:10.1/x"},
            )
        # No request was issued, so the api key was never forwarded.
        assert not uo.called


def test_semantic_scholar_public_endpoint_allowed(monkeypatch):
    monkeypatch.delenv("SEMANTIC_SCHOLAR_API_KEY", raising=False)
    # Literal public IP avoids real DNS; the guard permits it and the (mocked)
    # request returns the parsed payload.
    with _fake_urlopen(b'{"paperId": "abc"}') as uo:
        out = semantic_scholar._fetch_json(
            "https://93.184.216.34/graph/v1/paper",
            {"_lookup_id": "DOI:10.1/x"},
        )
    assert uo.called
    assert out == {"paperId": "abc"}


def test_semantic_scholar_loopback_endpoint_refused(monkeypatch):
    monkeypatch.delenv("SEMANTIC_SCHOLAR_API_KEY", raising=False)
    with _fake_urlopen() as uo:
        with pytest.raises(UnsafeFetchURL):
            semantic_scholar._fetch_json(
                "http://127.0.0.1:8000/graph/v1/paper", {"_lookup_id": "x"},
            )
        assert not uo.called


def test_sibling_openalex_metadata_endpoint_refused():
    # Representative sibling: the same guard sits before the retry loop.
    with _fake_urlopen() as uo:
        with pytest.raises(UnsafeFetchURL):
            openalex._fetch_json("http://169.254.169.254/works", {"filter": "x"})
        assert not uo.called


def test_sibling_openalex_public_endpoint_allowed():
    with _fake_urlopen(b'{"results": []}') as uo:
        out = openalex._fetch_json("https://93.184.216.34/works", {"filter": "x"})
    assert uo.called
    assert out == {"results": []}
