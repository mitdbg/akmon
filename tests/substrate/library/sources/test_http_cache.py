"""Coverage tests for `okg.substrate.library.sources._http_cache.CachedFetcher`.

The cache layer is dependency-injected (`session` is a fake object
with a `.get()` method); tests don't need to monkey-patch anything
global. Cache-hit tests pre-populate `cache_dir`; cache-miss tests
hand `CachedFetcher` a fake session that returns canned
`(status_code, text)` tuples.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from okg.substrate.library.sources._http_cache import (
    CachedFetcher,
    _default_ext_for,
)


def _seed_valid_cache(cache_path: Path, body: str) -> None:
    """Write a body plus its integrity sidecar, mirroring what
    ``CachedFetcher._write_cache`` produces, so a cache-hit is served.

    Per #692 a body with no valid sidecar is treated as corrupt and
    refetched, so cache-hit tests must seed the sidecar too.
    """
    cache_path.write_text(body)
    encoded = body.encode("utf-8")
    cache_path.with_name(cache_path.name + ".meta").write_text(
        json.dumps({
            "sha256": hashlib.sha256(
                encoded, usedforsecurity=False,
            ).hexdigest(),
            "len": len(encoded),
        })
    )


# ---------- _default_ext_for (pure) ----------


def test_default_ext_for_pubs_prefix_returns_json() -> None:
    assert _default_ext_for("pubs_index_2024") == "json"


def test_default_ext_for_json_prefix_returns_json() -> None:
    assert _default_ext_for("json_payload_xyz") == "json"


def test_default_ext_for_unknown_returns_html() -> None:
    assert _default_ext_for("some_other_key") == "html"
    assert _default_ext_for("") == "html"


# ---------- Fake session for tests ----------


@dataclass
class _FakeResponse:
    status_code: int
    text: str = ""


class _FakeSession:
    """Records every call; returns a programmable script of
    responses."""

    def __init__(self, responses: list[_FakeResponse]):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def get(self, url, *, headers=None, params=None, timeout=None):
        self.calls.append({
            "url": url, "headers": headers,
            "params": params, "timeout": timeout,
        })
        if not self._responses:
            raise AssertionError("FakeSession exhausted scripted responses")
        return self._responses.pop(0)


def _expected_cache_path(
    cache_dir: Path, cache_key: str, ext: str = "html",
) -> Path:
    name = hashlib.sha1(cache_key.encode()).hexdigest()[:16]
    return cache_dir / f"{name}.{ext}"


# ---------- cache-hit ----------


def test_cache_hit_returns_file_without_session_call(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cached_path = _expected_cache_path(cache_dir, "k", ext="html")
    _seed_valid_cache(cached_path, "hit body")

    session = _FakeSession([])  # no responses — must not be called
    fetcher = CachedFetcher(session=session, cache_dir=cache_dir)
    out = fetcher.get("https://example/x", cache_key="k")
    assert out == "hit body"
    assert session.calls == []


def test_cache_hit_with_pubs_prefix_uses_json_ext(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cached_path = _expected_cache_path(
        cache_dir, "pubs_index", ext="json",
    )
    _seed_valid_cache(cached_path, '{"ok": true}')

    session = _FakeSession([])
    fetcher = CachedFetcher(session=session, cache_dir=cache_dir)
    out = fetcher.get("https://example/x", cache_key="pubs_index")
    assert out == '{"ok": true}'


# ---------- cache-miss + write ----------


def test_cache_miss_fetches_and_writes(tmp_path):
    cache_dir = tmp_path / "cache"
    session = _FakeSession([_FakeResponse(200, "fresh body")])
    fetcher = CachedFetcher(
        session=session, cache_dir=cache_dir, sleep_seconds=0,
    )
    out = fetcher.get("https://example/x", cache_key="k")
    assert out == "fresh body"
    # Cache file written.
    assert _expected_cache_path(cache_dir, "k").read_text() == "fresh body"
    # Exactly one network call.
    assert len(session.calls) == 1
    assert session.calls[0]["url"] == "https://example/x"


def test_cache_miss_passes_headers_and_params(tmp_path):
    session = _FakeSession([_FakeResponse(200, "ok")])
    fetcher = CachedFetcher(
        session=session, cache_dir=tmp_path / "cache",
        sleep_seconds=0,
    )
    fetcher.get(
        "https://example/x", cache_key="k",
        headers={"User-Agent": "test"},
        params={"q": "1"},
    )
    assert session.calls[0]["headers"] == {"User-Agent": "test"}
    assert session.calls[0]["params"] == {"q": "1"}


# ---------- transient retry ----------


def test_transient_status_retries_then_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "okg.substrate.library.sources._http_cache.time.sleep",
        lambda _s: None,  # don't actually sleep during retries
    )
    session = _FakeSession([
        _FakeResponse(503, ""),
        _FakeResponse(503, ""),
        _FakeResponse(200, "ok now"),
    ])
    fetcher = CachedFetcher(
        session=session, cache_dir=tmp_path / "cache",
        retries=5, sleep_seconds=0,
    )
    out = fetcher.get("https://example/x", cache_key="k")
    assert out == "ok now"
    assert len(session.calls) == 3


def test_session_reset_swaps_session_between_retries(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(
        "okg.substrate.library.sources._http_cache.time.sleep",
        lambda _s: None,
    )
    first = _FakeSession([_FakeResponse(503, "")])
    second = _FakeSession([_FakeResponse(200, "from second")])
    fetcher = CachedFetcher(
        session=first, cache_dir=tmp_path / "cache",
        retries=5, sleep_seconds=0,
        session_reset=lambda: second,
    )
    out = fetcher.get("https://example/x", cache_key="k")
    assert out == "from second"
    # First session got one (503) call; second got the successful 200.
    assert len(first.calls) == 1
    assert len(second.calls) == 1


# ---------- terminal failure ----------


def test_non_transient_error_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "okg.substrate.library.sources._http_cache.time.sleep",
        lambda _s: None,
    )
    session = _FakeSession([_FakeResponse(404, "")])
    fetcher = CachedFetcher(
        session=session, cache_dir=tmp_path / "cache",
        sleep_seconds=0,
    )
    with pytest.raises(RuntimeError, match="404"):
        fetcher.get("https://example/x", cache_key="k")


def test_retries_exhausted_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "okg.substrate.library.sources._http_cache.time.sleep",
        lambda _s: None,
    )
    session = _FakeSession([_FakeResponse(503, "")] * 3)
    fetcher = CachedFetcher(
        session=session, cache_dir=tmp_path / "cache",
        retries=3, sleep_seconds=0,
    )
    with pytest.raises(RuntimeError, match="transient status"):
        fetcher.get("https://example/x", cache_key="k")


def test_transport_exception_retries(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "okg.substrate.library.sources._http_cache.time.sleep",
        lambda _s: None,
    )

    class _RaisingSession:
        def __init__(self):
            self.calls = 0

        def get(self, *args, **kwargs):
            self.calls += 1
            if self.calls < 3:
                raise ConnectionError("transient")
            return _FakeResponse(200, "recovered")

    session = _RaisingSession()
    fetcher = CachedFetcher(
        session=session, cache_dir=tmp_path / "cache",
        retries=5, sleep_seconds=0,
    )
    out = fetcher.get("https://example/x", cache_key="k")
    assert out == "recovered"
    assert session.calls == 3


def test_transport_exception_exhausts_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "okg.substrate.library.sources._http_cache.time.sleep",
        lambda _s: None,
    )

    class _RaisingSession:
        def get(self, *args, **kwargs):
            raise ConnectionError("network down")

    fetcher = CachedFetcher(
        session=_RaisingSession(), cache_dir=tmp_path / "cache",
        retries=2, sleep_seconds=0,
    )
    with pytest.raises(RuntimeError, match="failed after 2 retries"):
        fetcher.get("https://example/x", cache_key="k")
