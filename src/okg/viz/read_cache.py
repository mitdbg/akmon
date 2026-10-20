"""Generation-keyed response cache for the read-only console endpoints.

Every console read is pinned to an IMMUTABLE published generation: a response
for ``(method, path, query, generation_id)`` never changes, so it is perfectly
cacheable keyed by the generation. This module is the in-process store + the
ETag derivation behind the ``cache-console-reads`` middleware in ``app.py``.

Design (see PACT ``operator-ui-cache-and-refresh``):

* The key ALWAYS includes the resolved ``generation_id`` -- a read pinned to a
  new generation can never return a prior generation's cached body (the cache is
  incapable of serving cross-generation).
* The store is a bounded LRU (capped entries) so memory cannot grow without
  limit; the oldest entry is evicted on overflow.
* It is read-only: a hit issues no DB work and no writes.
* The ETag is derived from the same key, so a conditional request
  (``If-None-Match``) returns ``304`` only when BOTH the body and the pinned
  generation match -- it can never 304 a client into a stale generation.
* A refresh bypasses the cache (the request carries ``Cache-Control: no-cache``
  or ``?refresh=1``); the bypass recomputes AND refreshes the stored entry.

Only successful (200) JSON reads are cached. The liveness probe
(``/viz/api/health``) is never cached -- it is a process probe, not a
generation-pinned read.
"""
from __future__ import annotations

import hashlib
import json
import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

# Query parameters the CLIENT adds to any read, that address the transport
# rather than the read: they never reach a route's own parameters and never
# belong in a cache key. `refresh` is the shipped client's bypass flag --
# `withRefreshClient` appends it to every read of a refreshed view -- so it can
# arrive on ANY route, including the ones this cache never stores.
#
# This is the one list. `cache_console_reads` strips these from the cache key,
# and a route that refuses parameter names it does not know must accept them,
# or a Refresh click would be answered with a refusal. Add a global parameter
# here and both follow.
CLIENT_ONLY_PARAMS: frozenset[str] = frozenset({"refresh"})

# The console read paths whose bodies are generation-pinned and cacheable. An
# explicit allowlist (not a blanket ``console/*``) keeps non-read or
# non-deterministic console routes out of the cache. Paths are matched after the
# ``/viz/api/`` prefix.
CACHEABLE_PATHS: frozenset[str] = frozenset(
    {
        "/viz/api/console/health",
        "/viz/api/console/subtype",
    }
)

# Default LRU capacity (entries). One entry per (path, query, generation); a few
# hundred covers every view x a handful of live generations comfortably.
DEFAULT_CAPACITY = 256


@dataclass
class CachedResponse:
    """A stored 200 response body + the content-type to replay it with."""

    body: bytes
    media_type: str
    etag: str


class ReadCache:
    """A small thread-safe bounded LRU keyed by an opaque string.

    Carries hit/miss/eviction counters so a test can assert a second
    identical-generation read is a HIT (the composer ran once) without timing.
    """

    def __init__(self, capacity: int = DEFAULT_CAPACITY) -> None:
        self._capacity = max(1, capacity)
        self._store: "OrderedDict[str, CachedResponse]" = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def get(self, key: str) -> CachedResponse | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self.misses += 1
                return None
            self._store.move_to_end(key)
            self.hits += 1
            return entry

    def put(self, key: str, value: CachedResponse) -> None:
        with self._lock:
            self._store[key] = value
            self._store.move_to_end(key)
            while len(self._store) > self._capacity:
                self._store.popitem(last=False)
                self.evictions += 1

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "entries": len(self._store),
                "capacity": self._capacity,
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
            }


def is_cacheable_path(path: str) -> bool:
    return path in CACHEABLE_PATHS


def cache_key(path: str, query: str, generation_id: object) -> str:
    """The store key: path + normalized query + the pinned generation id.

    ``generation_id`` is stringified so a None (no published generation) is a
    distinct, never-colliding key -- but the middleware does not cache when the
    generation is unresolved, so that case never reaches the store.
    """
    return f"{path}?{query}#{generation_id}"


def make_etag(path: str, query: str, generation_id: object) -> str:
    """A strong ETag over the same key the store uses. Stable for a given
    (path, query, generation) so ``If-None-Match`` revalidation is exact."""
    digest = hashlib.sha256(cache_key(path, query, generation_id).encode()).hexdigest()
    return f'"{digest[:32]}"'


def extract_generation_id(body: bytes) -> Any | None:
    """The top-level ``generation_id`` from a console JSON response body, or None.

    The store keys on the generation the handler ACTUALLY composed (read back from
    the body), not the generation the middleware read before calling the handler.
    That closes the cross-generation poisoning race: if a concurrent refresh swaps
    the shared session's generation mid-compose, the new-generation body is stored
    under the new-generation key, never under the pre-read key. A body without a
    top-level ``generation_id`` returns None and is left uncached (never mis-keyed).
    """
    try:
        payload = json.loads(body)
    except (ValueError, TypeError):
        return None
    if isinstance(payload, dict):
        gen = payload.get("generation_id")
        if gen is not None:
            return gen
    return None


def wants_bypass(cache_control: str | None, refresh_param: str | None) -> bool:
    """True when the request asks to skip the cache (an explicit refresh).

    Honors the standard ``Cache-Control: no-cache`` request directive and an
    explicit ``?refresh=1`` query param.
    """
    if refresh_param is not None and str(refresh_param).lower() in {"1", "true", "yes"}:
        return True
    if cache_control and "no-cache" in cache_control.lower():
        return True
    return False
