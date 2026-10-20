"""Regression: CachedFetcher must not serve a torn/truncated cache file.

Issue #692. The old CachedFetcher (1) wrote the response body directly to the
final cache path (``cache_path.write_text(body)``) with no temp-file +
atomic-rename, so a crash mid-write left a truncated file on disk; and (2)
returned any existing file forever (``if cache_path.exists(): return
read_text()``) with no validation, so that truncated file was served as
authoritative on every subsequent run.

The fix writes atomically (temp file + ``os.replace``) with a content-hash
sidecar, and validates the body against the sidecar on read: a file with no
sidecar, or whose bytes do not match the recorded hash/length, is treated as
absent and refetched — never served.
"""
from __future__ import annotations

import hashlib

from okg.substrate.library.sources._http_cache import CachedFetcher


class _Resp:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text


class _Session:
    """Fake requests-like session that records call counts."""

    def __init__(self, body: str) -> None:
        self._body = body
        self.calls = 0

    def get(self, url, headers=None, params=None, timeout=None):
        self.calls += 1
        return _Resp(200, self._body)


def _cache_path(fetcher: CachedFetcher, cache_key: str, ext: str = "html"):
    from pathlib import Path

    cache_name = hashlib.sha1(
        cache_key.encode(), usedforsecurity=False,
    ).hexdigest()[:16]
    return Path(fetcher.cache_dir) / f"{cache_name}.{ext}"


def test_truncated_cache_file_is_detected_and_refetched(tmp_path) -> None:
    session = _Session("FULL-RESPONSE-BODY")
    fetcher = CachedFetcher(
        session=session, cache_dir=tmp_path, sleep_seconds=0.0,
    )

    # Simulate a torn write from a killed process: a bare file with a
    # truncated body and no valid sidecar.
    path = _cache_path(fetcher, "page_1")
    path.write_text("TRUNCA")  # partial, corrupt

    body = fetcher.get("https://example/1", cache_key="page_1")

    # The corrupt file must NOT be served; a live refetch happens.
    assert body == "FULL-RESPONSE-BODY"
    assert session.calls == 1


def test_valid_cache_is_served_without_refetch(tmp_path) -> None:
    session = _Session("HELLO")
    fetcher = CachedFetcher(
        session=session, cache_dir=tmp_path, sleep_seconds=0.0,
    )

    first = fetcher.get("https://example/2", cache_key="page_2")
    assert first == "HELLO"
    assert session.calls == 1

    # Second call is served from the (validated) cache — no new fetch.
    second = fetcher.get("https://example/2", cache_key="page_2")
    assert second == "HELLO"
    assert session.calls == 1


def test_cache_file_tampered_after_write_is_refetched(tmp_path) -> None:
    session = _Session("CORRECT-BODY")
    fetcher = CachedFetcher(
        session=session, cache_dir=tmp_path, sleep_seconds=0.0,
    )

    fetcher.get("https://example/3", cache_key="page_3")
    assert session.calls == 1

    # Corrupt the on-disk body after a good write; the sidecar no longer
    # matches, so a read must refetch rather than serve the tampered bytes.
    path = _cache_path(fetcher, "page_3")
    path.write_text("CORRUPTED")

    body = fetcher.get("https://example/3", cache_key="page_3")
    assert body == "CORRECT-BODY"
    assert session.calls == 2
