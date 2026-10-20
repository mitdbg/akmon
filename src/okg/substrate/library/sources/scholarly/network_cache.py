"""Shared cache/rate-limit helpers for memex network paper sources."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Protocol

from okg.substrate.library.sources.base import EdgeFact, NodeFact, SourceRun


JsonDict = dict[str, Any]
Fact = NodeFact | EdgeFact


class PaperSourceRequest(Protocol):
    """Minimal request contract consumed by cached paper sources."""

    provider_record_id: str
    provider_updated_at: str | None
    query: JsonDict


@dataclass(frozen=True)
class NetworkRequest:
    provider_record_id: str
    query: JsonDict = field(default_factory=dict)
    provider_updated_at: str | None = None


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    writes: int = 0
    skipped_by_watermark: int = 0

    def as_dict(self) -> JsonDict:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "writes": self.writes,
            "skipped_by_watermark": self.skipped_by_watermark,
        }


@dataclass(frozen=True)
class CacheEntry:
    source_name: str
    provider_record_id: str
    provider_updated_at: str | None
    query: JsonDict
    payload: JsonDict


class SourceResponseCache:
    """Filesystem cache keyed by source, query, record id, and timestamp."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser()

    def get_or_fetch(
        self,
        *,
        source_name: str,
        request: PaperSourceRequest,
        fetch: Callable[[], JsonDict],
        stats: CacheStats,
    ) -> CacheEntry:
        path = self.path_for(source_name=source_name, request=request)
        if path.exists():
            stats.hits += 1
            return self._read(path)

        stats.misses += 1
        payload = fetch()
        entry = CacheEntry(
            source_name=source_name,
            provider_record_id=request.provider_record_id,
            provider_updated_at=request.provider_updated_at,
            query=_stable_json_dict(request.query),
            payload=_stable_json_dict(payload),
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(_entry_to_json(entry), sort_keys=True), encoding="utf-8")
        stats.writes += 1
        return entry

    def path_for(self, *, source_name: str, request: PaperSourceRequest) -> Path:
        key = {
            "source_name": source_name,
            "provider_record_id": request.provider_record_id,
            "provider_updated_at": request.provider_updated_at,
            "query": _stable_json_dict(request.query),
        }
        digest = hashlib.sha256(json.dumps(key, sort_keys=True).encode("utf-8")).hexdigest()
        return self.root / source_name / f"{digest}.json"

    def _read(self, path: Path) -> CacheEntry:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return CacheEntry(
            source_name=str(raw["source_name"]),
            provider_record_id=str(raw["provider_record_id"]),
            provider_updated_at=raw.get("provider_updated_at"),
            query=_stable_json_dict(raw.get("query", {})),
            payload=_stable_json_dict(raw.get("payload", {})),
        )


class RateLimiter:
    def __init__(
        self,
        *,
        requests_per_second: float | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.requests_per_second = requests_per_second
        self._sleeper = sleeper
        self._last_request_at: float | None = None

    def wait(self) -> None:
        if not self.requests_per_second or self.requests_per_second <= 0:
            return
        now = time.monotonic()
        if self._last_request_at is not None:
            min_interval = 1.0 / self.requests_per_second
            elapsed = now - self._last_request_at
            if elapsed < min_interval:
                self._sleeper(min_interval - elapsed)
                now = time.monotonic()
        self._last_request_at = now


@dataclass
class CachedNetworkPaperSource:
    """Base class for provider-backed memex paper sources.

    Subclasses implement request discovery, payload fetch, and payload
    projection into substrate facts. The base class owns response cache,
    simple rate limiting, deterministic run stats, and next-cursor
    watermark calculation.
    """

    name: str
    cache_root: str | Path
    requests_per_second: float | None = None
    profile: str = "mutable_api"

    def __post_init__(self) -> None:
        self.cache = SourceResponseCache(self.cache_root)
        self.rate_limiter = RateLimiter(requests_per_second=self.requests_per_second)
        self.last_run_stats: JsonDict = {}

    def iter_requests(self, since_watermark: str | None) -> Iterable[NetworkRequest]:
        raise NotImplementedError

    def fetch_payload(self, request: NetworkRequest) -> JsonDict:
        raise NotImplementedError

    def facts_from_payload(self, request: NetworkRequest, payload: JsonDict) -> Iterable[Fact]:
        raise NotImplementedError

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: JsonDict | None = None,
        since_progress: JsonDict | None = None,
    ) -> SourceRun:
        stats = CacheStats()
        since_watermark = _watermark_from(cursor) or _watermark_from(since_progress)
        requests = list(self.iter_requests(since_watermark))
        eligible_requests: list[NetworkRequest] = []
        next_watermark: str | None = since_watermark
        for request in requests:
            if _not_newer(request.provider_updated_at, since_watermark):
                stats.skipped_by_watermark += 1
                continue
            eligible_requests.append(request)
            next_watermark = _max_watermark(next_watermark, request.provider_updated_at)

        def _facts() -> Iterator[Fact]:
            for request in eligible_requests:
                def _fetch() -> JsonDict:
                    self.rate_limiter.wait()
                    return self.fetch_payload(request)

                entry = self.cache.get_or_fetch(
                    source_name=self.name,
                    request=request,
                    fetch=_fetch,
                    stats=stats,
                )
                yield from self.facts_from_payload(request, entry.payload)

            self.last_run_stats = {
                "run_id": run_id,
                "cache": stats.as_dict(),
                "next_watermark": next_watermark,
            }

        return SourceRun(
            facts=_facts(),
            completed_scope=mode in {"scope_complete", "reconcile"},
            next_cursor={"last_provider_updated_at": next_watermark} if next_watermark else None,
            run_mode=mode,
        )


def _entry_to_json(entry: CacheEntry) -> JsonDict:
    return {
        "source_name": entry.source_name,
        "provider_record_id": entry.provider_record_id,
        "provider_updated_at": entry.provider_updated_at,
        "query": entry.query,
        "payload": entry.payload,
    }


def _stable_json_dict(value: Any) -> JsonDict:
    if isinstance(value, dict):
        return {str(k): _stable_json_value(v) for k, v in sorted(value.items())}
    return {}


def _stable_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return _stable_json_dict(value)
    if isinstance(value, list):
        return [_stable_json_value(v) for v in value]
    return value


def _watermark_from(value: JsonDict | None) -> str | None:
    if not value:
        return None
    watermark = value.get("last_provider_updated_at")
    return str(watermark) if watermark else None


def _not_newer(provider_updated_at: str | None, since_watermark: str | None) -> bool:
    return bool(provider_updated_at and since_watermark and provider_updated_at <= since_watermark)


def _max_watermark(current: str | None, candidate: str | None) -> str | None:
    if candidate is None:
        return current
    if current is None or candidate > current:
        return candidate
    return current
