"""OpenReview paper-source adapter for the Memex OKG."""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourceRun,
)

from ._common import slugify
from ._json_fetch import fetch_json as _shared_fetch_json
from .scholarly_identity_aliases import PaperIdentityIndex
from .network_cache import NetworkRequest, RateLimiter, SourceResponseCache

__all__ = [
    "OpenReviewSource",
    "normalize_openreview_record",
    "openreview_facts_from_record",
]


DEFAULT_VENUES: tuple[dict[str, Any], ...] = (
    {
        "slug": "iclr",
        "name": "ICLR",
        "year": 2025,
        "invitation": "ICLR.cc/2025/Conference/-/Submission",
    },
    {
        "slug": "iclr",
        "name": "ICLR",
        "year": 2024,
        "invitation": "ICLR.cc/2024/Conference/-/Submission",
    },
    {
        "slug": "neurips",
        "name": "NeurIPS",
        "year": 2024,
        "invitation": "NeurIPS.cc/2024/Conference/-/Submission",
    },
    {
        "slug": "neurips",
        "name": "NeurIPS",
        "year": 2023,
        "invitation": "NeurIPS.cc/2023/Conference/-/Submission",
    },
    {
        "slug": "icml",
        "name": "ICML",
        "year": 2024,
        "invitation": "ICML.cc/2024/Conference/-/Submission",
    },
    {
        "slug": "icml",
        "name": "ICML",
        "year": 2023,
        "invitation": "ICML.cc/2023/Conference/-/Submission",
    },
)


@dataclass
class OpenReviewSource:
    endpoint: str = "https://api2.openreview.net"
    auth: str = "optional_token"
    # Per A4 (Phase 1): no memex-specific default. Deployments set
    # cache_root explicitly in source_registry.yaml.
    cache_root: str | Path | None = None
    fixture_path: str | Path | None = None
    venues: list[dict[str, Any]] | tuple[dict[str, Any], ...] = field(
        default_factory=lambda: list(DEFAULT_VENUES)
    )
    # Per A4 (Phase 1): deployment-supplied venue-slug filter. None = all.
    venue_slugs: list[str] | tuple[str, ...] | None = None
    page_size: int = 1000
    max_records_per_venue: int | None = None
    requests_per_second: float | None = 2.0
    max_retries: int = 3
    retry_backoff_seconds: float = 10.0
    watermark_field: str = "mdate"
    target_subtypes: list[str] | tuple[str, ...] = (
        "paper",
        "person",
        "conference",
        "conference_edition",
    )
    dsn: str | None = None
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    _identity_index: "PaperIdentityIndex | None" = field(default=None, init=False, repr=False)

    name: str = "openreview"
    profile: str = "mutable_api"

    def __post_init__(self) -> None:
        if self.venue_slugs is not None:
            slug_set = {s for s in self.venue_slugs}
            available = {v["slug"] for v in self.venues}
            unknown = slug_set - available
            if unknown:
                from okg.substrate.errors import SubstrateError
                raise SubstrateError(
                    f"OpenReviewSource: venue_slugs {sorted(unknown)!r} "
                    f"are not in the venues catalog. Available: "
                    f"{sorted(available)!r}.",
                    fix_hint=(
                        "Add the slug to `venues` (with full dict shape) "
                        "or correct the `venue_slugs` spelling."
                    ),
                    data={"unknown": sorted(unknown), "available": sorted(available)},
                )
            self.venues = [v for v in self.venues if v["slug"] in slug_set]

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict[str, Any] | None = None,
        since_progress: dict[str, Any] | None = None,
    ) -> SourceRun:
        stats: dict[str, Any] = {
            "cache_hits": 0,
            "cache_misses": 0,
            "records": 0,
            "venues": {},
            "pages_skipped_by_progress": 0,
            "pages_completed": 0,
            "canonical_matches": {"doi": 0, "arxiv_id": 0, "title": 0, "new": 0},
        }
        prior_progress = since_progress or {}

        def _facts() -> Iterator[NodeFact | EdgeFact | ProgressMarker]:
            seen_nodes: set[str] = set()
            seen_edges: set[tuple[str, str, str]] = set()
            identity_index = self._load_identity_index()
            if self.fixture_path:
                for record in _load_fixture_records(Path(self.fixture_path)):
                    normalized = normalize_openreview_record(record)
                    default_id = f"paper:openreview:{normalized['forum_id']}"
                    match = identity_index.paper_id_for(normalized, default_id=default_id)
                    normalized["paper_id"] = match.node_id
                    normalized["identity_match_kind"] = match.kind
                    stats["canonical_matches"].setdefault(match.kind, 0)
                    stats["canonical_matches"][match.kind] += 1
                    stats["records"] += 1
                    yield from openreview_facts_from_record(
                        normalized,
                        seen_nodes=seen_nodes,
                        seen_edges=seen_edges,
                    )
            else:
                if self.cache_root is None:
                    from okg.substrate.errors import SubstrateError
                    raise SubstrateError(
                        "OpenReviewSource: cache_root is required when "
                        "fixture_path is not set",
                        fix_hint=(
                            "Set `cache_root: <path>` in the deployment's "
                            "source_registry.yaml or use fixture_path."
                        ),
                    )
                cache = SourceResponseCache(self.cache_root)
                limiter = RateLimiter(requests_per_second=self.requests_per_second)
                for venue in self.venues:
                    venue_slug = _venue_progress_slug(venue)
                    emitted_for_venue = 0
                    for page in self._iter_venue_pages(
                        venue,
                        cache=cache,
                        limiter=limiter,
                        stats=stats,
                        prior_progress=prior_progress,
                    ):
                        for record in _records_from_payload(page.payload):
                            normalized = normalize_openreview_record(record, venue=venue)
                            if not normalized.get("forum_id"):
                                continue
                            default_id = f"paper:openreview:{normalized['forum_id']}"
                            match = identity_index.paper_id_for(normalized, default_id=default_id)
                            normalized["paper_id"] = match.node_id
                            normalized["identity_match_kind"] = match.kind
                            stats["canonical_matches"].setdefault(match.kind, 0)
                            stats["canonical_matches"][match.kind] += 1
                            stats["records"] += 1
                            emitted_for_venue += 1
                            yield from openreview_facts_from_record(
                                normalized,
                                seen_nodes=seen_nodes,
                                seen_edges=seen_edges,
                            )
                            if self.max_records_per_venue and emitted_for_venue >= self.max_records_per_venue:
                                break
                        if self.max_records_per_venue and emitted_for_venue >= self.max_records_per_venue:
                            break
                        stats["pages_completed"] += 1
                        yield ProgressMarker(page.progress_key, page.progress_value)
                    stats["venues"][venue_slug] = emitted_for_venue
            self.last_run_stats = stats

        return SourceRun(
            facts=_facts(),
            completed_scope=mode in {"scope_complete", "reconcile"},
            next_cursor={"source": "openreview", "run_id": run_id},
            run_mode=mode,
        )

    def _iter_venue_pages(
        self,
        venue: dict[str, Any],
        *,
        cache: SourceResponseCache,
        limiter: RateLimiter,
        stats: dict[str, Any],
        prior_progress: dict[str, str],
    ) -> Iterator["_OpenReviewPage"]:
        offset = 0
        progress_slug = _venue_progress_slug(venue)
        invitation = str(venue["invitation"])
        while True:
            request = NetworkRequest(
                provider_record_id=f"{progress_slug}:{offset}",
                query={
                    "invitation": invitation,
                    "limit": self.page_size,
                    "offset": offset,
                },
                provider_updated_at=None,
            )
            progress_key = f"page:{request.provider_record_id}"
            prior_page = _decode_page_progress(
                prior_progress.get(f"record:{progress_key}") or prior_progress.get(progress_key)
            )
            if prior_page is not None:
                stats["pages_skipped_by_progress"] += 1
                sent = int(prior_page.get("sent") or 0)
                total = int(prior_page.get("total") or 0)
                if sent <= 0 or offset + sent >= total:
                    break
                offset += sent
                continue

            before_hits = stats["cache_hits"]
            before_misses = stats["cache_misses"]

            def _fetch() -> dict[str, Any]:
                limiter.wait()
                return _fetch_json(
                    f"{self.endpoint.rstrip('/')}/notes",
                    request.query,
                    max_retries=self.max_retries,
                    retry_backoff_seconds=self.retry_backoff_seconds,
                )

            cache_stats = _AdHocStats()
            entry = cache.get_or_fetch(
                source_name=self.name,
                request=request,
                fetch=_fetch,
                stats=cache_stats,
            )
            stats["cache_hits"] = before_hits + cache_stats.hits
            stats["cache_misses"] = before_misses + cache_stats.misses
            yield _OpenReviewPage(
                payload=entry.payload,
                progress_key=progress_key,
                progress_value=_page_progress_value(request, entry.payload),
            )

            records = _records_from_payload(entry.payload)
            total = _total_records(entry.payload)
            if not records:
                break
            offset += len(records)
            if offset >= total:
                break

    def _load_identity_index(self) -> "PaperIdentityIndex":
        if self._identity_index is None:
            self._identity_index = PaperIdentityIndex.from_dsn(self.dsn)
        return self._identity_index


class _AdHocStats:
    hits: int = 0
    misses: int = 0
    writes: int = 0
    skipped_by_watermark: int = 0


@dataclass(frozen=True)
class _OpenReviewPage:
    payload: dict[str, Any]
    progress_key: str
    progress_value: str


def normalize_openreview_record(
    record: dict[str, Any],
    *,
    venue: dict[str, Any] | None = None,
) -> dict[str, Any]:
    content = record.get("content") or {}
    forum_id = str(record.get("forum") or record.get("id") or "")
    title = _clean_title(_content_value(content, "title"))
    year = str((venue or {}).get("year") or _year_from_record(record) or "")
    venue_name = str((venue or {}).get("name") or _content_value(content, "venue") or "OpenReview")
    venue_slug = str((venue or {}).get("slug") or slugify(venue_name))
    authors = _list_content_value(content, "authors")
    authorids = _list_content_value(content, "authorids")
    doi = _extract_doi(content)
    arxiv_id = _extract_arxiv(content)
    return {
        "forum_id": forum_id,
        "note_id": str(record.get("id") or forum_id),
        "title": title or forum_id,
        "year": year,
        "venue": venue_name,
        "venue_slug": venue_slug,
        "venueid": _content_value(content, "venueid"),
        "decision": _content_value(content, "venue"),
        "abstract": _content_value(content, "abstract"),
        "keywords": _list_content_value(content, "keywords"),
        "authors": _authors(authors, authorids),
        "doi": doi,
        "arxiv_id": arxiv_id,
        "pdf_url": _pdf_url(_content_value(content, "pdf")),
        "openreview_url": f"https://openreview.net/forum?id={forum_id}" if forum_id else "",
        "mdate": record.get("mdate"),
        "odate": record.get("odate"),
        "invitation": (venue or {}).get("invitation") or _first_value(record.get("invitations")),
        "raw": record,
    }


def openreview_facts_from_record(
    record: dict[str, Any],
    *,
    seen_nodes: set[str] | None = None,
    seen_edges: set[tuple[str, str, str]] | None = None,
) -> Iterator[NodeFact | EdgeFact]:
    seen_nodes = seen_nodes if seen_nodes is not None else set()
    seen_edges = seen_edges if seen_edges is not None else set()
    forum_id = str(record["forum_id"])
    paper_id = str(record.get("paper_id") or f"paper:openreview:{forum_id}")
    title = str(record.get("title") or forum_id)
    year = str(record.get("year") or "")
    revision = _revision(record)
    source_record_id = {"source": "openreview", "forum": forum_id}

    yield from _yield_node_once(
        seen_nodes,
        NodeFact(
            node_id=paper_id,
            subtype="paper",
            attrs={
                "paper_id": paper_id,
                "label": title,
                "title": title,
                "year": year,
                "doi": record.get("doi") or "",
                "arxiv_id": record.get("arxiv_id") or "",
                "openreview_forum": forum_id,
                "openreview_note_id": record.get("note_id") or "",
                "openreview_url": record.get("openreview_url") or "",
                "pdf_url": record.get("pdf_url") or "",
                "venueid": record.get("venueid") or "",
                "decision": record.get("decision") or "",
                "keywords": record.get("keywords") or [],
                # Per `emit-paper-abstracts-from-scholarly-sources`:
                # surface the abstract that
                # `normalize_record` already extracted from
                # `content.abstract.value`. Empty string when
                # absent.
                "abstract": str(record.get("abstract") or ""),
                "identity_match_kind": record.get("identity_match_kind") or "new",
                "source_provider": "openreview",
                "text": _paper_text(record),
            },
            source_record_id=source_record_id,
            source_revision=revision,
        ),
    )

    venue_name = str(record.get("venue") or "OpenReview")
    venue_slug = str(record.get("venue_slug") or slugify(venue_name))
    conference_id = f"conf:{venue_slug}"
    edition_id = f"confed:{venue_slug}-{year or 'unknown'}"
    yield from _yield_node_once(
        seen_nodes,
        NodeFact(
            node_id=conference_id,
            subtype="conference",
            attrs={
                "conference_id": conference_id,
                "label": venue_name,
                "name": venue_name,
                "acronym": venue_name,
                "source_provider": "openreview",
                "text": venue_name,
            },
            source_record_id={"source": "openreview", "conference": venue_slug},
            source_revision=revision,
        ),
    )
    yield from _yield_node_once(
        seen_nodes,
        NodeFact(
            node_id=edition_id,
            subtype="conference_edition",
            attrs={
                "edition_id": edition_id,
                "conference_id": conference_id,
                "label": f"{venue_name} {year}".strip(),
                "name": f"{venue_name} {year}".strip(),
                "year": year,
                "source_provider": "openreview",
                "text": f"{venue_name} {year}".strip(),
            },
            source_record_id={"source": "openreview", "conference": venue_slug, "year": year},
            source_revision=revision,
        ),
    )
    yield from _yield_edge_once(
        seen_edges,
        EdgeFact(
            src=conference_id,
            dst=edition_id,
            edge_type="contains",
            attrs={"source_provider": "openreview"},
            source_record_id={**source_record_id, "edge": "conference_contains_edition"},
            source_revision=revision,
        ),
    )
    # Per dogfood-canary DQ audit (2026-05-19): co-emit inverse
    # member_of edge. See inspire_hep for full rationale.
    yield from _yield_edge_once(
        seen_edges,
        EdgeFact(
            src=edition_id,
            dst=conference_id,
            edge_type="member_of",
            attrs={"source_provider": "openreview"},
            source_record_id={**source_record_id, "edge": "edition_member_of_conference"},
            source_revision=revision,
        ),
    )
    yield from _yield_edge_once(
        seen_edges,
        EdgeFact(
            src=paper_id,
            dst=edition_id,
            edge_type="published_in",
            attrs={"source_provider": "openreview"},
            source_record_id={**source_record_id, "edge": "published_in"},
            source_revision=revision,
        ),
    )

    for ordinal, author in enumerate(record.get("authors") or [], start=1):
        name = str(author.get("name") or "").strip()
        if not name:
            continue
        author_id = str(author.get("openreview_id") or "").strip()
        person_key = _person_key(author_id, name)
        person_id = f"person:openreview:{person_key}"
        yield from _yield_node_once(
            seen_nodes,
            NodeFact(
                node_id=person_id,
                subtype="person",
                attrs={
                    "person_id": person_id,
                    "label": name,
                    # Per dogfood-canary DQ audit (2026-05-19): set
                    # both `name` (canonical) and `display_name`.
                    "name": name,
                    "display_name": name,
                    "openreview_id": author_id,
                    "source_provider": "openreview",
                    "text": name,
                },
                source_record_id={"source": "openreview", "person": person_key},
                source_revision=revision,
            ),
        )
        yield from _yield_edge_once(
            seen_edges,
            EdgeFact(
                src=paper_id,
                dst=person_id,
                edge_type="authored_by",
                attrs={"source_provider": "openreview", "author_ordinal": ordinal},
                source_record_id={**source_record_id, "edge": "authored_by", "author": person_key},
                source_revision=revision,
            ),
        )


def _fetch_json(
    endpoint: str,
    query: dict[str, Any],
    *,
    max_retries: int = 3,
    retry_backoff_seconds: float = 2.0,
) -> dict[str, Any]:
    return _shared_fetch_json(
        endpoint,
        query,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        unreachable_error="unreachable OpenReview retry state",
    )


def _load_fixture_records(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict) and "records" in raw:
        return [r for r in raw["records"] if isinstance(r, dict)]
    if isinstance(raw, dict) and "notes" in raw:
        return [r for r in raw["notes"] if isinstance(r, dict)]
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    return _records_from_payload(raw)


def _records_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records = payload.get("notes") or []
    return [r for r in records if isinstance(r, dict)]


def _total_records(payload: dict[str, Any]) -> int:
    try:
        return int(payload.get("count") or len(_records_from_payload(payload)))
    except (TypeError, ValueError):
        return len(_records_from_payload(payload))


def _page_progress_value(request: NetworkRequest, payload: dict[str, Any]) -> str:
    value = {
        "provider_record_id": request.provider_record_id,
        "query": request.query,
        "sent": len(_records_from_payload(payload)),
        "total": _total_records(payload),
    }
    return json.dumps(value, sort_keys=True)


def _decode_page_progress(value: str | None) -> dict[str, Any] | None:
    if not value:
        return None
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, dict) else None


def _venue_progress_slug(venue: dict[str, Any]) -> str:
    year = str(venue.get("year") or "unknown")
    return f"{venue['slug']}:{year}"


def _content_value(content: dict[str, Any], key: str) -> str:
    raw = content.get(key)
    if isinstance(raw, dict) and "value" in raw:
        raw = raw["value"]
    if isinstance(raw, list):
        return str(raw[0]).strip() if raw else ""
    return str(raw).strip() if raw not in (None, "") else ""


def _list_content_value(content: dict[str, Any], key: str) -> list[str]:
    raw = content.get(key)
    if isinstance(raw, dict) and "value" in raw:
        raw = raw["value"]
    if isinstance(raw, list):
        return [str(v).strip() for v in raw if str(v).strip()]
    if raw in (None, ""):
        return []
    return [str(raw).strip()]


def _authors(names: list[str], authorids: list[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for i, name in enumerate(names):
        out.append({"name": name, "openreview_id": authorids[i] if i < len(authorids) else ""})
    return out


def _person_key(author_id: str, name: str) -> str:
    if author_id:
        return slugify(author_id.lstrip("~"))
    return slugify(name)


def _extract_doi(content: dict[str, Any]) -> str:
    for key in ("doi", "DOI"):
        value = _content_value(content, key)
        if value:
            return _norm_doi(value)
    bibtex = _content_value(content, "_bibtex")
    match = re.search(r"\bdoi\s*=\s*[{\"]([^}\"]+)", bibtex, flags=re.IGNORECASE)
    return _norm_doi(match.group(1)) if match else ""


def _extract_arxiv(content: dict[str, Any]) -> str:
    for key in ("arxiv", "arxiv_id", "arxivId"):
        value = _content_value(content, key)
        if value:
            return _norm_arxiv(value)
    return ""


def _pdf_url(raw: str) -> str:
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if raw.startswith("/"):
        return f"https://openreview.net{raw}"
    return raw


def _year_from_record(record: dict[str, Any]) -> str:
    for key in ("pdate", "mdate", "cdate"):
        value = record.get(key)
        if isinstance(value, int) and value > 0:
            return time.strftime("%Y", time.gmtime(value / 1000))
    return ""


def _first_value(raw: Any) -> str:
    if isinstance(raw, list):
        return str(raw[0]) if raw else ""
    return str(raw) if raw not in (None, "") else ""


def _yield_node_once(seen: set[str], fact: NodeFact) -> Iterator[NodeFact]:
    if fact.node_id in seen:
        return
    seen.add(fact.node_id)
    yield fact


def _yield_edge_once(seen: set[tuple[str, str, str]], fact: EdgeFact) -> Iterator[EdgeFact]:
    key = (fact.src, fact.dst, fact.edge_type)
    if key in seen:
        return
    seen.add(key)
    yield fact


def _clean_title(title: str) -> str:
    return " ".join(title.replace("\n", " ").split()).rstrip(".")


def title_fingerprint(title: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()
    return " ".join(cleaned.split())


def _norm_doi(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"^https?://(dx\.)?doi\.org/", "", value)
    return value


def _norm_arxiv(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"^arxiv:", "", value)
    value = re.sub(r"^https?://arxiv\.org/abs/", "", value)
    return value


def _paper_text(record: dict[str, Any]) -> str:
    parts = [str(record.get("title") or "")]
    if record.get("venue") or record.get("year"):
        parts.append(f"{record.get('venue') or ''} {record.get('year') or ''}".strip())
    if record.get("doi"):
        parts.append(f"DOI: {record['doi']}")
    if record.get("arxiv_id"):
        parts.append(f"arXiv: {record['arxiv_id']}")
    if record.get("abstract"):
        parts.append(str(record["abstract"]))
    return "\n".join(p for p in parts if p)


def _revision(record: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return {
        "provider": "openreview",
        "content_sha256": hashlib.sha256(payload).hexdigest(),
        "forum": record.get("forum_id"),
        "mdate": record.get("mdate") or "",
    }
