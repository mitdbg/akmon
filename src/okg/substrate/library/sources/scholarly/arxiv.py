"""arXiv metadata enrichment source for the Memex OKG."""

from __future__ import annotations

import difflib
import hashlib
import json
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET  # noqa: F401 - retained for ET.Element type hints
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator
from urllib.error import HTTPError, URLError

from okg.substrate.library.sources._safe_xml import safe_xml_fromstring
from okg.substrate.library.sources._url_safety import (
    assert_public_http_url,
    open_validated_url,
)
from okg.substrate.library.sources.base import NodeFact, ProgressMarker, SourceRun

from ._common import okg_user_agent
from .scholarly_identity_aliases import PaperIdentityIndex, norm_arxiv, norm_doi
from .network_cache import NetworkRequest, RateLimiter, SourceResponseCache

__all__ = [
    "ArxivSource",
    "ArxivCandidate",
    "arxiv_facts_from_record",
    "normalize_arxiv_entry",
    "parse_arxiv_payload",
]


ATOM_NS = "http://www.w3.org/2005/Atom"
ARXIV_NS = "http://arxiv.org/schemas/atom"
NS = {"atom": ATOM_NS, "arxiv": ARXIV_NS}


@dataclass(frozen=True)
class ArxivCandidate:
    paper_id: str
    title: str
    year: str = ""
    arxiv_id: str = ""
    doi: str = ""
    source_provider: str = ""
    attrs: dict[str, Any] = field(default_factory=dict)


@dataclass
class ArxivSource:
    endpoint: str = "https://export.arxiv.org/api/query"
    auth: str = "none"
    # Per A4 (Phase 1): no memex default; deployment sets explicitly.
    cache_root: str | Path | None = None
    fixture_path: str | Path | None = None
    dsn: str | None = None
    requests_per_second: float | None = 0.1
    max_id_lookups_per_run: int = 300
    max_title_queries_per_run: int = 100
    page_size: int = 5
    title_match_threshold: float = 0.94
    max_year_delta: int = 1
    allowed_primary_categories: list[str] | tuple[str, ...] = ("cs.", "stat.ML")
    candidate_providers: list[str] | tuple[str, ...] = (
        "openalex",
        "openreview",
        "dblp",
        "crossref",
        "legacy_paper_cache",
        "",
    )
    max_retries: int = 3
    retry_backoff_seconds: float = 60.0
    watermark_field: str = "updated"
    target_subtypes: list[str] | tuple[str, ...] = ("paper",)
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    _paper_identity_index: "PaperIdentityIndex | None" = field(default=None, init=False, repr=False)

    name: str = "arxiv"
    profile: str = "mutable_api"

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
            "id_matches": 0,
            "title_matches": 0,
            "no_match": 0,
            "candidates_skipped_by_progress": 0,
            "providers": {},
        }
        prior_progress = since_progress or {}

        def _facts() -> Iterator[NodeFact | ProgressMarker]:
            seen_nodes: set[str] = set()
            paper_index = self._load_paper_identity_index()
            cache = SourceResponseCache(self.cache_root)
            limiter = RateLimiter(requests_per_second=self.requests_per_second)

            if self.fixture_path:
                candidates = _fixture_candidates(Path(self.fixture_path))
                fixture_payloads = _fixture_payloads(Path(self.fixture_path))
            else:
                candidates = self._load_candidates()
                fixture_payloads = {}

            for candidate in candidates:
                candidate_key = _candidate_key(candidate)
                progress_key = f"candidate:{candidate_key}"
                if prior_progress.get(f"record:{progress_key}") or prior_progress.get(progress_key):
                    stats["candidates_skipped_by_progress"] += 1
                    continue
                stats["providers"][candidate.source_provider] = (
                    stats["providers"].get(candidate.source_provider, 0) + 1
                )
                payload = fixture_payloads.get(candidate_key)
                if payload is None:
                    request = self._request_for_candidate(candidate)
                    before_hits = stats["cache_hits"]
                    before_misses = stats["cache_misses"]

                    def _fetch() -> dict[str, Any]:
                        limiter.wait()
                        return _fetch_xml(
                            self.endpoint,
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
                    payload = entry.payload

                match = _match_candidate(
                    candidate,
                    payload,
                    self.title_match_threshold,
                    self.max_year_delta,
                    self.allowed_primary_categories,
                )
                if match is None:
                    stats["no_match"] += 1
                    yield ProgressMarker(progress_key, _candidate_progress_value(candidate, None))
                    continue

                normalized, match_kind = match
                identity_match = paper_index.paper_id_for(normalized, default_id=candidate.paper_id)
                normalized["paper_id"] = candidate.paper_id if identity_match.is_new else identity_match.node_id
                normalized["identity_match_kind"] = identity_match.kind if not identity_match.is_new else match_kind
                normalized["candidate_attrs"] = candidate.attrs
                normalized["candidate_title"] = candidate.title
                normalized["candidate_year"] = candidate.year
                normalized["candidate_source_provider"] = candidate.source_provider
                stats["records"] += 1
                if match_kind == "id":
                    stats["id_matches"] += 1
                else:
                    stats["title_matches"] += 1
                yield from arxiv_facts_from_record(normalized, seen_nodes=seen_nodes)
                yield ProgressMarker(progress_key, _candidate_progress_value(candidate, normalized))

            self.last_run_stats = stats

        return SourceRun(
            facts=_facts(),
            completed_scope=mode in {"scope_complete", "reconcile"},
            next_cursor={"source": "arxiv", "run_id": run_id},
            run_mode=mode,
        )

    def _request_for_candidate(self, candidate: ArxivCandidate) -> NetworkRequest:
        arxiv_id = norm_arxiv(candidate.arxiv_id)
        if arxiv_id:
            return NetworkRequest(
                provider_record_id=f"id:{arxiv_id}",
                query={"id_list": arxiv_id, "max_results": 1},
            )
        return NetworkRequest(
            provider_record_id=f"title:{hashlib.sha256(candidate.title.encode('utf-8')).hexdigest()}",
            query={
                "search_query": f'ti:"{candidate.title}"',
                "start": 0,
                "max_results": self.page_size,
                "sortBy": "relevance",
                "sortOrder": "descending",
            },
        )

    def _load_candidates(self) -> list[ArxivCandidate]:
        if not self.dsn:
            return []
        from okg.substrate.db.connection import connect as db_connect

        from okg.substrate.graph_store import ChronosGraphStore

        candidates: list[ArxivCandidate] = []
        seen: set[str] = set()
        # Chronos cutover: candidate selection reads current
        # default-branch graph state ("which paper nodes need arXiv
        # enrichment now"), so it resolves through a Chronos checkout
        # over okg.graph_nodes; the legacy `valid_to_gen IS NULL` pin is
        # dropped (the checkout session injects visibility).
        with db_connect(self.dsn) as conn:
            graph = ChronosGraphStore.from_connection(
                conn, ensure_schema=False
            ).checkout()
            if self.max_id_lookups_per_run > 0:
                rows = graph.query(
                    """
                    SELECT node_id, attrs
                      FROM okg.graph_nodes
                     WHERE subtype = 'paper'
                       AND coalesce(attrs->>'arxiv_id', '') <> ''
                       AND coalesce(attrs->>'abstract', '') = ''
                       AND coalesce(attrs->>'is_reference_stub', 'false') <> 'true'
                     ORDER BY node_id
                     LIMIT :limit
                    """,
                    {"limit": self.max_id_lookups_per_run},
                )
                for row in rows:
                    candidate = _candidate_from_live(str(row["node_id"]), dict(row["attrs"] or {}))
                    seen.add(candidate.paper_id)
                    candidates.append(candidate)

            for provider in self.candidate_providers:
                if len([c for c in candidates if not c.arxiv_id]) >= self.max_title_queries_per_run:
                    break
                rows = graph.query(
                    """
                    SELECT node_id, attrs
                      FROM okg.graph_nodes
                     WHERE subtype = 'paper'
                       AND coalesce(attrs->>'title', '') <> ''
                       AND coalesce(attrs->>'arxiv_id', '') = ''
                       AND coalesce(attrs->>'is_reference_stub', 'false') <> 'true'
                       AND coalesce(attrs->>'source_provider', '') = :provider
                     ORDER BY node_id
                     LIMIT :limit
                    """,
                    {"provider": provider, "limit": self.max_title_queries_per_run},
                )
                for row in rows:
                    if str(row["node_id"]) in seen:
                        continue
                    candidate = _candidate_from_live(str(row["node_id"]), dict(row["attrs"] or {}))
                    seen.add(candidate.paper_id)
                    candidates.append(candidate)
                    if len([c for c in candidates if not c.arxiv_id]) >= self.max_title_queries_per_run:
                        break
        return candidates

    def _load_paper_identity_index(self) -> PaperIdentityIndex:
        if self._paper_identity_index is None:
            self._paper_identity_index = PaperIdentityIndex.from_dsn(self.dsn)
        return self._paper_identity_index


class _AdHocStats:
    hits: int = 0
    misses: int = 0
    writes: int = 0
    skipped_by_watermark: int = 0


def arxiv_facts_from_record(
    record: dict[str, Any],
    *,
    seen_nodes: set[str] | None = None,
) -> Iterator[NodeFact]:
    seen_nodes = seen_nodes if seen_nodes is not None else set()
    paper_id = str(record["paper_id"])
    prior_attrs = dict(record.get("candidate_attrs") or {})
    title = str(record.get("title") or record.get("candidate_title") or prior_attrs.get("title") or paper_id)
    revision = _revision(record)
    attrs = {
        **prior_attrs,
        "paper_id": paper_id,
        "label": title,
        "title": title,
        "year": str(record.get("year") or record.get("candidate_year") or prior_attrs.get("year") or ""),
        "doi": record.get("doi") or prior_attrs.get("doi") or "",
        "arxiv_id": record.get("arxiv_id") or prior_attrs.get("arxiv_id") or "",
        "arxiv_url": record.get("arxiv_url") or "",
        "pdf_url": record.get("pdf_url") or prior_attrs.get("pdf_url") or "",
        "abstract": record.get("abstract") or prior_attrs.get("abstract") or "",
        "arxiv_primary_category": record.get("primary_category") or "",
        "arxiv_categories": record.get("categories") or [],
        "arxiv_updated": record.get("updated") or "",
        "arxiv_published": record.get("published") or "",
        "identity_match_kind": record.get("identity_match_kind") or "",
        "candidate_source_provider": record.get("candidate_source_provider") or prior_attrs.get("source_provider") or "",
        "source_provider": "arxiv",
        "text": _paper_text(record),
    }
    yield from _yield_node_once(
        seen_nodes,
        NodeFact(
            node_id=paper_id,
            subtype="paper",
            attrs=attrs,
            source_record_id={"source": "arxiv", "arxiv_id": attrs["arxiv_id"], "paper_id": paper_id},
            source_revision=revision,
        ),
    )


def parse_arxiv_payload(payload: dict[str, Any] | str) -> list[dict[str, Any]]:
    xml_text = payload if isinstance(payload, str) else str(payload.get("xml") or "")
    if not xml_text.strip():
        return []
    root = safe_xml_fromstring(xml_text)
    return [normalize_arxiv_entry(entry) for entry in root.findall("atom:entry", NS)]


def normalize_arxiv_entry(entry: ET.Element) -> dict[str, Any]:
    arxiv_url = _text(entry.find("atom:id", NS))
    arxiv_id = _entry_arxiv_id(arxiv_url)
    title = _clean_text(_text(entry.find("atom:title", NS)))
    abstract = _clean_text(_text(entry.find("atom:summary", NS)))
    published = _text(entry.find("atom:published", NS))
    updated = _text(entry.find("atom:updated", NS))
    primary = entry.find("arxiv:primary_category", NS)
    primary_category = str(primary.attrib.get("term") or "") if primary is not None else ""
    categories = [
        str(cat.attrib.get("term") or "")
        for cat in entry.findall("atom:category", NS)
        if str(cat.attrib.get("term") or "")
    ]
    doi = norm_doi(_text(entry.find("arxiv:doi", NS)))
    pdf_url = ""
    for link in entry.findall("atom:link", NS):
        if link.attrib.get("title") == "pdf" or link.attrib.get("type") == "application/pdf":
            pdf_url = str(link.attrib.get("href") or "")
            break
    return {
        "arxiv_id": arxiv_id,
        "arxiv_url": arxiv_url,
        "pdf_url": pdf_url,
        "title": title,
        "abstract": abstract,
        "year": published[:4] if published else "",
        "published": published,
        "updated": updated,
        "primary_category": primary_category,
        "categories": categories,
        "doi": doi,
        "authors": [_clean_text(_text(author.find("atom:name", NS))) for author in entry.findall("atom:author", NS)],
    }


def _fetch_xml(
    endpoint: str,
    query: dict[str, Any],
    *,
    max_retries: int = 3,
    retry_backoff_seconds: float = 2.0,
) -> dict[str, Any]:
    url = f"{endpoint}?{urllib.parse.urlencode(query)}"
    req = urllib.request.Request(url, headers={"User-Agent": okg_user_agent()})
    # Validate the (operator/config-overridable) endpoint URL before the
    # request so an override to a private/loopback/metadata host is refused.
    assert_public_http_url(req.full_url)
    for attempt in range(max_retries + 1):
        try:
            with open_validated_url(req, timeout=60) as response:
                return {"xml": response.read().decode("utf-8")}
        except HTTPError as e:
            if not _retryable_http_status(e.code) or attempt >= max_retries:
                raise
            retry_after = _retry_after_seconds(e)
            sleep_for = retry_after if retry_after is not None else retry_backoff_seconds * (2 ** attempt)
        except URLError:
            if attempt >= max_retries:
                raise
            sleep_for = retry_backoff_seconds * (2 ** attempt)
        time.sleep(sleep_for)
    raise RuntimeError("unreachable arXiv retry state")


def _retryable_http_status(status: int) -> bool:
    return status == 429 or status >= 500


def _retry_after_seconds(error: HTTPError) -> float | None:
    raw = error.headers.get("Retry-After") if error.headers else None
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None


def _match_candidate(
    candidate: ArxivCandidate,
    payload: dict[str, Any],
    title_match_threshold: float,
    max_year_delta: int,
    allowed_primary_categories: tuple[str, ...] | list[str],
) -> tuple[dict[str, Any], str] | None:
    entries = parse_arxiv_payload(payload)
    candidate_arxiv = norm_arxiv(candidate.arxiv_id)
    if candidate_arxiv:
        for entry in entries:
            if norm_arxiv(str(entry.get("arxiv_id") or "")) == candidate_arxiv:
                return entry, "id"
        return None

    candidate_title = _title_fingerprint(candidate.title)
    candidate_year = _safe_int(candidate.year)
    best: tuple[float, dict[str, Any]] | None = None
    for entry in entries:
        if not _category_allowed(str(entry.get("primary_category") or ""), allowed_primary_categories):
            continue
        title = _title_fingerprint(str(entry.get("title") or ""))
        if not title:
            continue
        if candidate_year is not None:
            year = _safe_int(str(entry.get("year") or ""))
            if year is not None and abs(year - candidate_year) > max_year_delta:
                continue
        score = difflib.SequenceMatcher(None, candidate_title, title).ratio()
        if score >= title_match_threshold and (best is None or score > best[0]):
            best = (score, entry)
    if best is None:
        return None
    best[1]["title_match_score"] = best[0]
    return best[1], "title"


def _category_allowed(category: str, allowed: tuple[str, ...] | list[str]) -> bool:
    if not category:
        return True
    return any(category == prefix or category.startswith(prefix) for prefix in allowed)


def _candidate_from_live(node_id: str, attrs: dict[str, Any]) -> ArxivCandidate:
    return ArxivCandidate(
        paper_id=node_id,
        title=str(attrs.get("title") or attrs.get("label") or ""),
        year=str(attrs.get("year") or ""),
        arxiv_id=str(attrs.get("arxiv_id") or ""),
        doi=str(attrs.get("doi") or ""),
        source_provider=str(attrs.get("source_provider") or ""),
        attrs=attrs,
    )


def _fixture_candidates(path: Path) -> list[ArxivCandidate]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    candidates = raw.get("candidates") if isinstance(raw, dict) else []
    return [
        ArxivCandidate(
            paper_id=str(c["paper_id"]),
            title=str(c.get("title") or ""),
            year=str(c.get("year") or ""),
            arxiv_id=str(c.get("arxiv_id") or ""),
            doi=str(c.get("doi") or ""),
            source_provider=str(c.get("source_provider") or ""),
            attrs=dict(c.get("attrs") or {}),
        )
        for c in candidates
        if isinstance(c, dict) and c.get("paper_id")
    ]


def _fixture_payloads(path: Path) -> dict[str, dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    payloads = raw.get("payloads", {}) if isinstance(raw, dict) else {}
    return {str(k): v for k, v in payloads.items() if isinstance(v, dict)}


def _candidate_key(candidate: ArxivCandidate) -> str:
    if candidate.arxiv_id:
        return f"id:{norm_arxiv(candidate.arxiv_id)}"
    return f"title:{hashlib.sha256(candidate.title.encode('utf-8')).hexdigest()}"


def _candidate_progress_value(candidate: ArxivCandidate, record: dict[str, Any] | None) -> str:
    value = {
        "candidate_paper_id": candidate.paper_id,
        "candidate_title": candidate.title,
        "arxiv_id": candidate.arxiv_id,
        "matched_arxiv_id": (record or {}).get("arxiv_id") or "",
        "matched_title": (record or {}).get("title") or "",
    }
    return json.dumps(value, sort_keys=True)


def _entry_arxiv_id(url: str) -> str:
    raw = url.rstrip("/").rsplit("/", 1)[-1]
    return norm_arxiv(raw)


def _yield_node_once(seen: set[str], fact: NodeFact) -> Iterator[NodeFact]:
    if fact.node_id in seen:
        return
    seen.add(fact.node_id)
    yield fact


def _text(element: ET.Element | None) -> str:
    return element.text.strip() if element is not None and element.text else ""


def _clean_text(value: str) -> str:
    return " ".join(value.replace("\n", " ").split()).strip()


def _title_fingerprint(title: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()
    return " ".join(cleaned.split())


def _safe_int(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _paper_text(record: dict[str, Any]) -> str:
    parts = [str(record.get("title") or "")]
    if record.get("year"):
        parts.append(str(record["year"]))
    if record.get("arxiv_id"):
        parts.append(f"arXiv: {record['arxiv_id']}")
    if record.get("doi"):
        parts.append(f"DOI: {record['doi']}")
    if record.get("abstract"):
        parts.append(str(record["abstract"]))
    return "\n".join(p for p in parts if p)


def _revision(record: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return {
        "provider": "arxiv",
        "content_sha256": hashlib.sha256(payload).hexdigest(),
        "arxiv_id": record.get("arxiv_id") or "",
        "updated": record.get("updated") or "",
    }
