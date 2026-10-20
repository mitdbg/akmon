"""CachedFetcher — small substrate-shipped helper for HTTP-fetch
sources that want on-disk response caching + Akamai-friendly
retry-on-403/5xx.

Wraps any `requests.Session`-shaped object (stdlib `requests`,
`curl_cffi.requests.Session`, `httpx.Client` if it's adapted).
Cache key is hashed to a short hex stem; cache extension is
inferred from the supplied `cache_key` prefix when the body
shape is known (`html` for HTML pages, `json` for JSON
endpoints).

Per `migrate-af_pubs-to-substrate-dsn` Phase 2 (2026-04-26):
extracted from `_af_pubs_catalog_fetch._http_get` +
`_af_pubs_obsolete_fetch._fetch_keyword`, both of which had
~50 LOC of identical cache + retry logic. Future fetcher
sources (af_pubs_pdf_text, future github/gitlab fetchers, ...)
should compose this rather than re-rolling the loop.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional


__all__ = ["CachedFetcher"]


logger = logging.getLogger(__name__)


def _sidecar_for(cache_path: Path) -> Path:
    """Path of the integrity sidecar recorded next to a cache body.

    The sidecar carries the body's sha256 + byte length. A body with no
    sidecar (e.g. written by an older CachedFetcher, or a torn write that
    never reached the sidecar step) is treated as unverifiable and
    refetched — never served as authoritative.
    """
    return cache_path.with_name(cache_path.name + ".meta")


def _body_digest(body: str) -> dict[str, Any]:
    encoded = body.encode("utf-8")
    return {
        "sha256": hashlib.sha256(encoded, usedforsecurity=False).hexdigest(),
        "len": len(encoded),
    }


def _atomic_write_text(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` atomically via temp file + ``os.replace``.

    A crash mid-write leaves the temp file, not a truncated ``path``.
    ``os.replace`` is atomic on POSIX and Windows for same-directory renames.
    """
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def _default_ext_for(cache_key: str) -> str:
    """Infer file extension from the cache_key's prefix.

    Conventions (override via `ext=` on .get):
      - keys starting `pubs_` / `json_` → `.json`
      - everything else → `.html`
    """
    if cache_key.startswith(("pubs_", "json_")):
        return "json"
    return "html"


@dataclass
class CachedFetcher:
    """HTTP GET with on-disk cache + retry.

    Construction:
      session         — a session-like object with a .get(...) method.
      cache_dir       — where to store cached response bodies.
      retries         — total attempts on a transient failure (default 5).
      backoff_base    — initial backoff seconds (default 30; doubles per
                        attempt up to backoff_cap).
      backoff_cap     — max single-attempt sleep (default 300).
      sleep_seconds   — polite-pacing pause before every live (uncached)
                        request (default 0.3).
      transient_codes — HTTP status codes considered retryable
                        (default {403, 429, 502, 503, 504}).
      session_reset   — optional zero-arg callable; invoked between
                        retries to swap in a fresh session
                        (Akamai recovery pattern). Should return the
                        new session, which CachedFetcher uses going
                        forward. Pass None to disable.
    """

    session: Any
    cache_dir: Path | str
    retries: int = 5
    backoff_base: float = 30.0
    backoff_cap: float = 300.0
    sleep_seconds: float = 0.3
    transient_codes: frozenset[int] = field(
        default_factory=lambda: frozenset({403, 429, 502, 503, 504}),
    )
    session_reset: Optional[Callable[[], Any]] = None

    def __post_init__(self) -> None:
        self.cache_dir = Path(self.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get(
        self,
        url: str,
        *,
        cache_key: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        ext: Optional[str] = None,
        timeout: float = 60.0,
    ) -> str:
        """GET `url` with caching + retry. Returns the response
        body text. Raises `RuntimeError` on a non-transient
        failure or when retries are exhausted."""
        ext = ext or _default_ext_for(cache_key)
        cache_name = hashlib.sha1(
            cache_key.encode(),
            usedforsecurity=False,
        ).hexdigest()[:16]
        cache_path = Path(self.cache_dir) / f"{cache_name}.{ext}"
        cached = self._read_valid_cache(cache_path)
        if cached is not None:
            return cached

        if self.sleep_seconds:
            time.sleep(self.sleep_seconds)

        last_exc: Optional[Exception] = None
        for attempt in range(self.retries):
            try:
                resp = self.session.get(
                    url, headers=headers, params=params,
                    timeout=timeout,
                )
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "CachedFetcher: %s transport error attempt "
                    "%d/%d: %s",
                    url, attempt + 1, self.retries, exc,
                )
                self._sleep_backoff(attempt)
                continue
            if resp.status_code == 200:
                body = resp.text
                self._write_cache(cache_path, body)
                return body
            if resp.status_code in self.transient_codes:
                logger.warning(
                    "CachedFetcher: %s status=%d attempt %d/%d",
                    url, resp.status_code,
                    attempt + 1, self.retries,
                )
                if self.session_reset is not None:
                    self.session = self.session_reset()
                self._sleep_backoff(attempt)
                continue
            raise RuntimeError(f"GET {url} -> {resp.status_code}")

        if last_exc is not None:
            raise RuntimeError(
                f"GET {url} failed after {self.retries} retries: "
                f"{last_exc}"
            ) from last_exc
        raise RuntimeError(
            f"GET {url} -> transient status after {self.retries} retries"
        )

    def _read_valid_cache(self, cache_path: Path) -> Optional[str]:
        """Return the cached body iff it exists AND validates.

        Validation compares the on-disk body against the sha256 + length
        recorded in the sidecar. A missing body, a missing/malformed
        sidecar, or a hash/length mismatch (a torn or tampered file) all
        return ``None`` so the caller performs a live refetch. The corrupt
        artifacts are removed so a later run does not keep re-reading them.
        """
        if not cache_path.exists():
            return None
        sidecar = _sidecar_for(cache_path)
        if not sidecar.exists():
            # Unverifiable (old-format or torn write). Do not serve it.
            logger.warning(
                "CachedFetcher: cache %s has no integrity sidecar; "
                "treating as invalid and refetching",
                cache_path.name,
            )
            self._discard_cache(cache_path)
            return None
        try:
            body = cache_path.read_text(encoding="utf-8")
            expected = json.loads(sidecar.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            logger.warning(
                "CachedFetcher: cache %s failed to load (%s); refetching",
                cache_path.name, exc,
            )
            self._discard_cache(cache_path)
            return None
        actual = _body_digest(body)
        if (
            expected.get("sha256") != actual["sha256"]
            or expected.get("len") != actual["len"]
        ):
            logger.warning(
                "CachedFetcher: cache %s failed integrity check "
                "(expected len=%s sha=%s, got len=%s sha=%s); refetching",
                cache_path.name,
                expected.get("len"), expected.get("sha256"),
                actual["len"], actual["sha256"],
            )
            self._discard_cache(cache_path)
            return None
        return body

    def _write_cache(self, cache_path: Path, body: str) -> None:
        """Persist ``body`` + its integrity sidecar atomically.

        The body is written first (temp + ``os.replace``), then the
        sidecar. If the process dies between the two, the next read finds
        a body with no sidecar and refetches — it never serves an
        unverified body.
        """
        _atomic_write_text(cache_path, body)
        _atomic_write_text(
            _sidecar_for(cache_path),
            json.dumps(_body_digest(body)),
        )

    @staticmethod
    def _discard_cache(cache_path: Path) -> None:
        for path in (cache_path, _sidecar_for(cache_path)):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except OSError as exc:
                logger.warning(
                    "CachedFetcher: could not remove stale cache %s: %s",
                    path.name, exc,
                )

    def _sleep_backoff(self, attempt: int) -> None:
        delay = min(self.backoff_base * (2 ** attempt), self.backoff_cap)
        time.sleep(delay)
