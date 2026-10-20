"""Semantic Scholar metadata enrichment source for the Memex OKG."""

from __future__ import annotations

import difflib
import hashlib
import json
import os
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator
from urllib.error import HTTPError, URLError

from okg.substrate.library.sources._url_safety import (
    assert_public_http_url,
    open_validated_url,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourceRun,
)

from ._common import okg_user_agent, slugify
from .scholarly_identity_aliases import (
    PaperIdentityIndex,
    PersonIdentityIndex,
    norm_arxiv,
    norm_doi,
    norm_orcid,
)
from .network_cache import NetworkRequest, RateLimiter, SourceResponseCache

__all__ = [
    "SemanticScholarSource",
    "SemanticScholarCandidate",
    "SemanticScholarRateLimited",
    "normalize_semantic_scholar_paper",
    "semantic_scholar_facts_from_record",
]


DEFAULT_FIELDS = ",".join(
    [
        "paperId",
        "corpusId",
        "url",
        "title",
        "year",
        "abstract",
        "venue",
        "publicationDate",
        "publicationTypes",
        "citationCount",
        "influentialCitationCount",
        "externalIds",
        "authors.authorId",
        "authors.name",
        "authors.externalIds",
        "references.paperId",
        "references.corpusId",
        "references.title",
        "references.year",
        "references.externalIds",
    ]
)


@dataclass(frozen=True)
class SemanticScholarCandidate:
    paper_id: str
    title: str
    year: str = ""
    doi: str = ""
    arxiv_id: str = ""
    semantic_scholar_id: str = ""
    source_provider: str = ""
    attrs: dict[str, Any] = field(default_factory=dict)


class SemanticScholarRateLimited(RuntimeError):
    """Raised after retryable Semantic Scholar 429 retries are exhausted."""

    def __init__(self, *, retry_after: float | None = None) -> None:
        super().__init__("Semantic Scholar rate limit exhausted")
        self.retry_after = retry_after


@dataclass
class SemanticScholarSource:
    endpoint: str = "https://api.semanticscholar.org/graph/v1/paper"
    auth: str = "optional_api_key"
    # Per A4 (Phase 1): no memex default; deployment sets explicitly.
    cache_root: str | Path | None = None
    fixture_path: str | Path | None = None
    dsn: str | None = None
    requests_per_second: float | None = 0.2
    page_size: int = 3
    max_id_lookups_per_run: int = 250
    max_arxiv_lookups_per_run: int = 250
    max_title_queries_per_run: int = 50
    max_batch_size: int = 100
    title_match_threshold: float = 0.94
    max_year_delta: int = 1
    max_references_per_record: int = 40
    max_authors_per_record: int = 50
    candidate_providers: list[str] | tuple[str, ...] = (
        "arxiv_oai",
        "openalex",
        "crossref",
        "openreview",
        "dblp",
        "inspire_hep",
        "legacy_paper_cache",
        "",
    )
    max_retries: int = 3
    retry_backoff_seconds: float = 60.0
    watermark_field: str = "updated"
    target_subtypes: list[str] | tuple[str, ...] = ("paper", "person")
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    _paper_identity_index: "PaperIdentityIndex | None" = field(default=None, init=False, repr=False)
    _person_identity_index: "PersonIdentityIndex | None" = field(default=None, init=False, repr=False)

    name: str = "semantic_scholar"
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
            "doi_matches": 0,
            "arxiv_matches": 0,
            "title_matches": 0,
            "no_match": 0,
            "candidates_skipped_by_progress": 0,
            "author_edges": 0,
            "citation_edges": 0,
            "rate_limited": False,
            "rate_limited_at": "",
            "providers": {},
        }
        prior_progress = since_progress or {}

        def _facts() -> Iterator[NodeFact | EdgeFact | ProgressMarker]:
            seen_nodes: set[str] = set()
            seen_edges: set[tuple[str, str, str]] = set()
            paper_index = self._load_paper_identity_index()
            person_index = self._load_person_identity_index()
            cache = SourceResponseCache(self.cache_root)
            limiter = RateLimiter(requests_per_second=self.requests_per_second)

            if self.fixture_path:
                candidates = _fixture_candidates(Path(self.fixture_path))
                fixture_payloads = _fixture_payloads(Path(self.fixture_path))
            else:
                candidates = self._load_candidates()
                fixture_payloads = {}

            if not self.fixture_path:
                yield from self._run_batch_candidates(
                    candidates,
                    prior_progress=prior_progress,
                    stats=stats,
                    cache=cache,
                    limiter=limiter,
                    paper_index=paper_index,
                    person_index=person_index,
                    seen_nodes=seen_nodes,
                    seen_edges=seen_edges,
                )
                self.last_run_stats = stats
                return

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
                        return _fetch_json(
                            self.endpoint,
                            request.query,
                            auth=self.auth,
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
                if match_kind == "semantic_scholar_id":
                    stats["id_matches"] += 1
                elif match_kind == "doi":
                    stats["doi_matches"] += 1
                elif match_kind == "arxiv_id":
                    stats["arxiv_matches"] += 1
                else:
                    stats["title_matches"] += 1
                for fact in semantic_scholar_facts_from_record(
                    normalized,
                    paper_index=paper_index,
                    person_index=person_index,
                    seen_nodes=seen_nodes,
                    seen_edges=seen_edges,
                    max_references=self.max_references_per_record,
                    max_authors=self.max_authors_per_record,
                ):
                    if isinstance(fact, EdgeFact):
                        if fact.edge_type == "authored_by":
                            stats["author_edges"] += 1
                        elif fact.edge_type == "cites":
                            stats["citation_edges"] += 1
                    yield fact
                yield ProgressMarker(progress_key, _candidate_progress_value(candidate, normalized))

            self.last_run_stats = stats

        return SourceRun(
            facts=_facts(),
            completed_scope=mode in {"scope_complete", "reconcile"},
            next_cursor={"source": "semantic_scholar", "run_id": run_id},
            run_mode=mode,
        )

    def _run_batch_candidates(
        self,
        candidates: list[SemanticScholarCandidate],
        *,
        prior_progress: dict[str, Any],
        stats: dict[str, Any],
        cache: SourceResponseCache,
        limiter: RateLimiter,
        paper_index: PaperIdentityIndex,
        person_index: PersonIdentityIndex,
        seen_nodes: set[str],
        seen_edges: set[tuple[str, str, str]],
    ) -> Iterator[NodeFact | EdgeFact | ProgressMarker]:
        exact: list[SemanticScholarCandidate] = []
        title: list[SemanticScholarCandidate] = []
        for candidate in candidates:
            candidate_key = _candidate_key(candidate)
            progress_key = f"candidate:{candidate_key}"
            if prior_progress.get(f"record:{progress_key}") or prior_progress.get(progress_key):
                stats["candidates_skipped_by_progress"] += 1
                continue
            stats["providers"][candidate.source_provider] = (
                stats["providers"].get(candidate.source_provider, 0) + 1
            )
            if _candidate_lookup_id(candidate):
                exact.append(candidate)
            else:
                title.append(candidate)

        for i in range(0, len(exact), max(1, self.max_batch_size)):
            batch = exact[i : i + max(1, self.max_batch_size)]
            request = _batch_request_for_candidates(batch)
            before_hits = stats["cache_hits"]
            before_misses = stats["cache_misses"]

            def _fetch() -> dict[str, Any]:
                limiter.wait()
                return _fetch_json(
                    self.endpoint,
                    request.query,
                    auth=self.auth,
                    max_retries=self.max_retries,
                    retry_backoff_seconds=self.retry_backoff_seconds,
                )

            cache_stats = _AdHocStats()
            try:
                entry = cache.get_or_fetch(
                    source_name=self.name,
                    request=request,
                    fetch=_fetch,
                    stats=cache_stats,
                )
            except SemanticScholarRateLimited:
                stats["rate_limited"] = True
                stats["rate_limited_at"] = request.provider_record_id
                return
            stats["cache_hits"] = before_hits + cache_stats.hits
            stats["cache_misses"] = before_misses + cache_stats.misses
            payloads = _batch_payloads(entry.payload)
            for candidate, payload in zip(batch, payloads, strict=False):
                yield from self._facts_for_candidate_payload(
                    candidate,
                    payload or {},
                    stats=stats,
                    paper_index=paper_index,
                    person_index=person_index,
                    seen_nodes=seen_nodes,
                    seen_edges=seen_edges,
                )

        for candidate in title:
            request = self._request_for_candidate(candidate)
            before_hits = stats["cache_hits"]
            before_misses = stats["cache_misses"]

            def _fetch_title() -> dict[str, Any]:
                limiter.wait()
                return _fetch_json(
                    self.endpoint,
                    request.query,
                    auth=self.auth,
                    max_retries=self.max_retries,
                    retry_backoff_seconds=self.retry_backoff_seconds,
                )

            cache_stats = _AdHocStats()
            try:
                entry = cache.get_or_fetch(
                    source_name=self.name,
                    request=request,
                    fetch=_fetch_title,
                    stats=cache_stats,
                )
            except SemanticScholarRateLimited:
                stats["rate_limited"] = True
                stats["rate_limited_at"] = request.provider_record_id
                return
            stats["cache_hits"] = before_hits + cache_stats.hits
            stats["cache_misses"] = before_misses + cache_stats.misses
            yield from self._facts_for_candidate_payload(
                candidate,
                entry.payload,
                stats=stats,
                paper_index=paper_index,
                person_index=person_index,
                seen_nodes=seen_nodes,
                seen_edges=seen_edges,
            )

    def _facts_for_candidate_payload(
        self,
        candidate: SemanticScholarCandidate,
        payload: dict[str, Any],
        *,
        stats: dict[str, Any],
        paper_index: PaperIdentityIndex,
        person_index: PersonIdentityIndex,
        seen_nodes: set[str],
        seen_edges: set[tuple[str, str, str]],
    ) -> Iterator[NodeFact | EdgeFact | ProgressMarker]:
        candidate_key = _candidate_key(candidate)
        progress_key = f"candidate:{candidate_key}"
        match = _match_candidate(
            candidate,
            payload,
            self.title_match_threshold,
            self.max_year_delta,
        )
        if match is None:
            stats["no_match"] += 1
            yield ProgressMarker(progress_key, _candidate_progress_value(candidate, None))
            return

        normalized, match_kind = match
        identity_match = paper_index.paper_id_for(normalized, default_id=candidate.paper_id)
        normalized["paper_id"] = candidate.paper_id if identity_match.is_new else identity_match.node_id
        normalized["identity_match_kind"] = identity_match.kind if not identity_match.is_new else match_kind
        normalized["candidate_attrs"] = candidate.attrs
        normalized["candidate_title"] = candidate.title
        normalized["candidate_year"] = candidate.year
        normalized["candidate_source_provider"] = candidate.source_provider
        stats["records"] += 1
        if match_kind == "semantic_scholar_id":
            stats["id_matches"] += 1
        elif match_kind == "doi":
            stats["doi_matches"] += 1
        elif match_kind == "arxiv_id":
            stats["arxiv_matches"] += 1
        else:
            stats["title_matches"] += 1
        for fact in semantic_scholar_facts_from_record(
            normalized,
            paper_index=paper_index,
            person_index=person_index,
            seen_nodes=seen_nodes,
            seen_edges=seen_edges,
            max_references=self.max_references_per_record,
            max_authors=self.max_authors_per_record,
        ):
            if isinstance(fact, EdgeFact):
                if fact.edge_type == "authored_by":
                    stats["author_edges"] += 1
                elif fact.edge_type == "cites":
                    stats["citation_edges"] += 1
            yield fact
        yield ProgressMarker(progress_key, _candidate_progress_value(candidate, normalized))

    def _request_for_candidate(self, candidate: SemanticScholarCandidate) -> NetworkRequest:
        semantic_id = str(candidate.semantic_scholar_id or "").strip()
        if semantic_id:
            return NetworkRequest(
                provider_record_id=f"s2:{semantic_id}",
                query={"_lookup_id": semantic_id, "fields": DEFAULT_FIELDS},
            )
        doi = norm_doi(candidate.doi)
        if doi:
            return NetworkRequest(
                provider_record_id=f"doi:{doi}",
                query={"_lookup_id": f"DOI:{doi}", "fields": DEFAULT_FIELDS},
            )
        arxiv_id = norm_arxiv(candidate.arxiv_id)
        if arxiv_id:
            return NetworkRequest(
                provider_record_id=f"arxiv:{arxiv_id}",
                query={"_lookup_id": f"ARXIV:{arxiv_id}", "fields": DEFAULT_FIELDS},
            )
        return NetworkRequest(
            provider_record_id=f"title:{hashlib.sha256(candidate.title.encode('utf-8')).hexdigest()}",
            query={
                "_search": True,
                "query": candidate.title,
                "limit": self.page_size,
                "fields": DEFAULT_FIELDS,
            },
        )

    def _load_candidates(self) -> list[SemanticScholarCandidate]:
        if not self.dsn:
            return []
        from okg.substrate.db.connection import connect as db_connect

        from okg.substrate.graph_store import ChronosGraphStore

        candidates: list[SemanticScholarCandidate] = []
        seen: set[str] = set()
        # Chronos cutover: candidate selection reads current
        # default-branch graph state ("which paper nodes need Semantic
        # Scholar enrichment now"), so it resolves through a Chronos
        # checkout over okg.graph_nodes; the legacy `valid_to_gen IS
        # NULL` pin is dropped (the checkout session injects
        # visibility). Rows are dicts.
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
                       AND coalesce(attrs->>'doi', '') <> ''
                       AND coalesce(attrs->>'semantic_scholar_id', '') = ''
                       AND coalesce(attrs->>'is_reference_stub', 'false') <> 'true'
                     ORDER BY
                       (coalesce(attrs->>'abstract', '') = '') DESC,
                       node_id
                     LIMIT :limit
                    """,
                    {"limit": self.max_id_lookups_per_run},
                )
                for row in rows:
                    candidate = _candidate_from_live(str(row["node_id"]), dict(row["attrs"] or {}))
                    seen.add(candidate.paper_id)
                    candidates.append(candidate)

            if self.max_arxiv_lookups_per_run > 0:
                rows = graph.query(
                    """
                    SELECT node_id, attrs
                      FROM okg.graph_nodes
                     WHERE subtype = 'paper'
                       AND coalesce(attrs->>'doi', '') = ''
                       AND coalesce(attrs->>'arxiv_id', '') <> ''
                       AND coalesce(attrs->>'semantic_scholar_id', '') = ''
                       AND coalesce(attrs->>'is_reference_stub', 'false') <> 'true'
                     ORDER BY
                       (coalesce(attrs->>'abstract', '') = '') DESC,
                       node_id
                     LIMIT :limit
                    """,
                    {"limit": self.max_arxiv_lookups_per_run},
                )
                for row in rows:
                    if str(row["node_id"]) in seen:
                        continue
                    candidate = _candidate_from_live(str(row["node_id"]), dict(row["attrs"] or {}))
                    seen.add(candidate.paper_id)
                    candidates.append(candidate)

            title_queries = 0
            for provider in self.candidate_providers:
                if title_queries >= self.max_title_queries_per_run:
                    break
                rows = graph.query(
                    """
                    SELECT node_id, attrs
                      FROM okg.graph_nodes
                     WHERE subtype = 'paper'
                       AND coalesce(attrs->>'title', '') <> ''
                       AND coalesce(attrs->>'doi', '') = ''
                       AND coalesce(attrs->>'arxiv_id', '') = ''
                       AND coalesce(attrs->>'semantic_scholar_id', '') = ''
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
                    title_queries += 1
                    if title_queries >= self.max_title_queries_per_run:
                        break
        return candidates

    def _load_paper_identity_index(self) -> PaperIdentityIndex:
        if self._paper_identity_index is None:
            self._paper_identity_index = PaperIdentityIndex.from_dsn(self.dsn)
        return self._paper_identity_index

    def _load_person_identity_index(self) -> PersonIdentityIndex:
        if self._person_identity_index is None:
            self._person_identity_index = PersonIdentityIndex.from_dsn(self.dsn)
        return self._person_identity_index


class _AdHocStats:
    hits: int = 0
    misses: int = 0
    writes: int = 0
    skipped_by_watermark: int = 0


def normalize_semantic_scholar_paper(paper: dict[str, Any]) -> dict[str, Any]:
    external = paper.get("externalIds") if isinstance(paper.get("externalIds"), dict) else {}
    semantic_id = str(paper.get("paperId") or "").strip()
    corpus_id = str(paper.get("corpusId") or "").strip()
    pub_venue = paper.get("publicationVenue") if isinstance(paper.get("publicationVenue"), dict) else {}
    return {
        "semantic_scholar_id": semantic_id,
        "semantic_scholar_corpus_id": corpus_id,
        "semantic_scholar_url": str(paper.get("url") or ""),
        "doi": norm_doi(str(external.get("DOI") or external.get("Doi") or external.get("doi") or "")),
        "arxiv_id": norm_arxiv(str(external.get("ArXiv") or external.get("ARXIV") or external.get("arxiv") or "")),
        "openalex_id": _external_id(external, "OpenAlex"),
        "title": _clean_title(str(paper.get("title") or "")),
        "year": str(paper.get("year") or ""),
        "abstract": str(paper.get("abstract") or ""),
        "venue": str(pub_venue.get("name") or paper.get("venue") or ""),
        "publication_date": str(paper.get("publicationDate") or ""),
        "publication_types": [str(v) for v in paper.get("publicationTypes") or [] if str(v)],
        "citation_count": paper.get("citationCount"),
        "influential_citation_count": paper.get("influentialCitationCount"),
        "authors": _authors(paper.get("authors")),
        "references": _references(paper.get("references")),
        "raw": paper,
    }


def semantic_scholar_facts_from_record(
    record: dict[str, Any],
    *,
    paper_index: PaperIdentityIndex | None = None,
    person_index: PersonIdentityIndex | None = None,
    seen_nodes: set[str] | None = None,
    seen_edges: set[tuple[str, str, str]] | None = None,
    max_references: int = 40,
    max_authors: int = 50,
) -> Iterator[NodeFact | EdgeFact]:
    paper_index = paper_index or PaperIdentityIndex()
    person_index = person_index or PersonIdentityIndex()
    seen_nodes = seen_nodes if seen_nodes is not None else set()
    seen_edges = seen_edges if seen_edges is not None else set()
    paper_id = str(record["paper_id"])
    prior_attrs = dict(record.get("candidate_attrs") or {})
    title = str(record.get("title") or record.get("candidate_title") or prior_attrs.get("title") or paper_id)
    revision = _revision(record)
    source_record_id = {
        "source": "semantic_scholar",
        "semantic_scholar_id": record.get("semantic_scholar_id") or "",
        "paper_id": paper_id,
    }
    paper_attrs = {
        **prior_attrs,
        "paper_id": paper_id,
        "label": title,
        "title": title,
        "year": str(record.get("year") or record.get("candidate_year") or prior_attrs.get("year") or ""),
        "doi": record.get("doi") or prior_attrs.get("doi") or "",
        "arxiv_id": record.get("arxiv_id") or prior_attrs.get("arxiv_id") or "",
        "openalex_id": record.get("openalex_id") or prior_attrs.get("openalex_id") or "",
        "semantic_scholar_id": record.get("semantic_scholar_id") or prior_attrs.get("semantic_scholar_id") or "",
        "semantic_scholar_corpus_id": record.get("semantic_scholar_corpus_id") or "",
        "semantic_scholar_url": record.get("semantic_scholar_url") or "",
        "abstract": record.get("abstract") or prior_attrs.get("abstract") or "",
        "citation_count": record.get("citation_count", prior_attrs.get("citation_count")),
        "semantic_scholar_influential_citation_count": record.get("influential_citation_count"),
        "semantic_scholar_publication_date": record.get("publication_date") or "",
        "semantic_scholar_publication_types": record.get("publication_types") or [],
        "primary_location_source": record.get("venue") or prior_attrs.get("primary_location_source") or "",
        "identity_match_kind": record.get("identity_match_kind") or "",
        "candidate_source_provider": record.get("candidate_source_provider") or prior_attrs.get("source_provider") or "",
        "source_provider": "semantic_scholar",
        "text": _paper_text(record),
    }
    yield from _yield_node_once(
        seen_nodes,
        NodeFact(
            node_id=paper_id,
            subtype="paper",
            attrs=paper_attrs,
            source_record_id=source_record_id,
            source_revision=revision,
        ),
    )

    for ordinal, author in enumerate((record.get("authors") or [])[:max_authors], start=1):
        name = str(author.get("name") or "").strip()
        if not name:
            continue
        default_person_id = _default_person_id(author, name)
        match = person_index.person_id_for(author, default_id=default_person_id)
        person_id = match.node_id
        yield from _yield_node_once(
            seen_nodes,
            NodeFact(
                node_id=person_id,
                subtype="person",
                attrs={
                    "person_id": person_id,
                    "label": name,
                    "display_name": name,
                    "orcid": author.get("orcid") or "",
                    "semantic_scholar_author_id": author.get("semantic_scholar_author_id") or "",
                    "identity_match_kind": match.kind,
                    "source_provider": "semantic_scholar",
                    "text": name,
                },
                source_record_id={
                    "source": "semantic_scholar",
                    "author": author.get("semantic_scholar_author_id") or name,
                },
                source_revision=revision,
            ),
        )
        yield from _yield_edge_once(
            seen_edges,
            EdgeFact(
                src=paper_id,
                dst=person_id,
                edge_type="authored_by",
                attrs={
                    "source_provider": "semantic_scholar",
                    "author_ordinal": ordinal,
                },
                source_record_id={
                    **source_record_id,
                    "edge": "authored_by",
                    "author": author.get("semantic_scholar_author_id") or name,
                },
                source_revision=revision,
            ),
        )

    for ref in (record.get("references") or [])[:max_references]:
        target_id, is_new = _reference_paper_id(ref, paper_index)
        if is_new:
            yield from _yield_node_once(
                seen_nodes,
                NodeFact(
                    node_id=target_id,
                    subtype="paper",
                    attrs={
                        "paper_id": target_id,
                        "label": ref.get("title") or target_id,
                        "title": ref.get("title") or "",
                        "year": ref.get("year") or "",
                        "doi": ref.get("doi") or "",
                        "arxiv_id": ref.get("arxiv_id") or "",
                        "openalex_id": ref.get("openalex_id") or "",
                        "semantic_scholar_id": ref.get("semantic_scholar_id") or "",
                        "semantic_scholar_corpus_id": ref.get("semantic_scholar_corpus_id") or "",
                        "is_reference_stub": True,
                        "source_provider": "semantic_scholar",
                        "text": _paper_text(ref),
                    },
                    source_record_id={
                        "source": "semantic_scholar",
                        "semantic_scholar_id": ref.get("semantic_scholar_id") or "",
                        "stub": True,
                    },
                    source_revision=revision,
                ),
            )
        yield from _yield_edge_once(
            seen_edges,
            EdgeFact(
                src=paper_id,
                dst=target_id,
                edge_type="cites",
                attrs={"source_provider": "semantic_scholar"},
                source_record_id={**source_record_id, "edge": "cites", "target": target_id},
                source_revision=revision,
            ),
        )


def _fetch_json(
    endpoint: str,
    query: dict[str, Any],
    *,
    auth: str = "optional_api_key",
    max_retries: int = 3,
    retry_backoff_seconds: float = 2.0,
) -> dict[str, Any]:
    url = _url_for_request(endpoint, query)
    # The endpoint is operator/config-overridable; validate it is a public
    # http(s) URL BEFORE attaching the API key, so an override to a private /
    # loopback / cloud-metadata host cannot exfiltrate the key or reach internal
    # services (per `centralize-egress-url-validation`).
    assert_public_http_url(url)
    headers = {"User-Agent": okg_user_agent()}
    api_key = os.environ.get("SEMANTIC_SCHOLAR_API_KEY")
    if api_key and auth == "optional_api_key":
        headers["x-api-key"] = api_key
    data: bytes | None = None
    method: str | None = None
    if "_batch_ids" in query:
        headers["Content-Type"] = "application/json"
        data = json.dumps({"ids": list(query.get("_batch_ids") or [])}).encode("utf-8")
        method = "POST"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    for attempt in range(max_retries + 1):
        try:
            with open_validated_url(req, timeout=60) as response:
                raw = json.loads(response.read().decode("utf-8"))
                return {"batch": raw} if isinstance(raw, list) else raw
        except HTTPError as e:
            if e.code == 404:
                return {}
            retry_after = _retry_after_seconds(e)
            if e.code == 429 and attempt >= max_retries:
                raise SemanticScholarRateLimited(retry_after=retry_after) from e
            if not _retryable_http_status(e.code) or attempt >= max_retries:
                raise
            sleep_for = retry_after if retry_after is not None else retry_backoff_seconds * (2 ** attempt)
        except URLError:
            if attempt >= max_retries:
                raise
            sleep_for = retry_backoff_seconds * (2 ** attempt)
        time.sleep(sleep_for)
    raise RuntimeError("unreachable Semantic Scholar retry state")


def _url_for_request(endpoint: str, query: dict[str, Any]) -> str:
    public_query = {k: v for k, v in query.items() if not k.startswith("_")}
    if "_batch_ids" in query:
        base = endpoint.rstrip("/") + "/batch"
        return f"{base}?{urllib.parse.urlencode(public_query)}"
    if query.get("_search"):
        base = endpoint.rstrip("/") + "/search"
        return f"{base}?{urllib.parse.urlencode(public_query)}"
    lookup_id = str(query.get("_lookup_id") or "").strip()
    if lookup_id:
        base = endpoint.rstrip("/") + "/" + urllib.parse.quote(lookup_id, safe=":")
        return f"{base}?{urllib.parse.urlencode(public_query)}"
    return f"{endpoint}?{urllib.parse.urlencode(public_query)}"


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
    candidate: SemanticScholarCandidate,
    payload: dict[str, Any],
    title_match_threshold: float,
    max_year_delta: int,
) -> tuple[dict[str, Any], str] | None:
    results = _results_from_payload(payload)
    candidate_s2 = str(candidate.semantic_scholar_id or "").strip()
    candidate_doi = norm_doi(candidate.doi)
    candidate_arxiv = norm_arxiv(candidate.arxiv_id)
    if candidate_s2:
        for item in results:
            normalized = normalize_semantic_scholar_paper(item)
            if normalized.get("semantic_scholar_id") == candidate_s2:
                return normalized, "semantic_scholar_id"
        return None
    if candidate_doi:
        for item in results:
            normalized = normalize_semantic_scholar_paper(item)
            if normalized.get("doi") == candidate_doi:
                return normalized, "doi"
        return None
    if candidate_arxiv:
        for item in results:
            normalized = normalize_semantic_scholar_paper(item)
            if normalized.get("arxiv_id") == candidate_arxiv:
                return normalized, "arxiv_id"
        return None

    candidate_title = _title_fingerprint(candidate.title)
    candidate_year = _safe_int(candidate.year)
    best: tuple[float, dict[str, Any]] | None = None
    for item in results:
        normalized = normalize_semantic_scholar_paper(item)
        title = _title_fingerprint(str(normalized.get("title") or ""))
        if not title:
            continue
        if candidate_year is not None:
            year = _safe_int(str(normalized.get("year") or ""))
            if year is not None and abs(year - candidate_year) > max_year_delta:
                continue
        score = difflib.SequenceMatcher(None, candidate_title, title).ratio()
        if score >= title_match_threshold and (best is None or score > best[0]):
            best = (score, normalized)
    if best is None:
        return None
    best[1]["title_match_score"] = best[0]
    return best[1], "title"


def _results_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(payload.get("batch"), list):
        return [r for r in payload["batch"] if isinstance(r, dict)]
    if isinstance(payload.get("data"), list):
        return [r for r in payload["data"] if isinstance(r, dict)]
    if isinstance(payload.get("results"), list):
        return [r for r in payload["results"] if isinstance(r, dict)]
    if isinstance(payload.get("paperId"), str):
        return [payload]
    return []


def _candidate_from_live(node_id: str, attrs: dict[str, Any]) -> SemanticScholarCandidate:
    return SemanticScholarCandidate(
        paper_id=node_id,
        title=str(attrs.get("title") or attrs.get("label") or ""),
        year=str(attrs.get("year") or ""),
        doi=str(attrs.get("doi") or ""),
        arxiv_id=str(attrs.get("arxiv_id") or ""),
        semantic_scholar_id=str(
            attrs.get("semantic_scholar_id")
            or attrs.get("semantic_scholar_paper_id")
            or attrs.get("s2_paper_id")
            or ""
        ),
        source_provider=str(attrs.get("source_provider") or ""),
        attrs=attrs,
    )


def _candidate_lookup_id(candidate: SemanticScholarCandidate) -> str:
    semantic_id = str(candidate.semantic_scholar_id or "").strip()
    if semantic_id:
        return semantic_id
    doi = norm_doi(candidate.doi)
    if doi:
        return f"DOI:{doi}"
    arxiv_id = norm_arxiv(candidate.arxiv_id)
    if arxiv_id:
        return f"ARXIV:{arxiv_id}"
    return ""


def _batch_request_for_candidates(candidates: list[SemanticScholarCandidate]) -> NetworkRequest:
    ids = [_candidate_lookup_id(candidate) for candidate in candidates]
    digest = hashlib.sha256(json.dumps(ids, sort_keys=True).encode("utf-8")).hexdigest()
    return NetworkRequest(
        provider_record_id=f"batch:{digest}",
        query={"_batch_ids": ids, "fields": DEFAULT_FIELDS},
    )


def _batch_payloads(payload: dict[str, Any]) -> list[dict[str, Any] | None]:
    raw = payload.get("batch")
    if isinstance(raw, list):
        return [item if isinstance(item, dict) else None for item in raw]
    data = payload.get("data")
    if isinstance(data, list):
        return [item if isinstance(item, dict) else None for item in data]
    return []


def _fixture_candidates(path: Path) -> list[SemanticScholarCandidate]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    candidates = raw.get("candidates") if isinstance(raw, dict) else []
    return [
        SemanticScholarCandidate(
            paper_id=str(c["paper_id"]),
            title=str(c.get("title") or ""),
            year=str(c.get("year") or ""),
            doi=str(c.get("doi") or ""),
            arxiv_id=str(c.get("arxiv_id") or ""),
            semantic_scholar_id=str(c.get("semantic_scholar_id") or ""),
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


def _candidate_key(candidate: SemanticScholarCandidate) -> str:
    if candidate.semantic_scholar_id:
        return f"s2:{candidate.semantic_scholar_id}"
    if candidate.doi:
        return f"doi:{norm_doi(candidate.doi)}"
    if candidate.arxiv_id:
        return f"arxiv:{norm_arxiv(candidate.arxiv_id)}"
    return f"title:{hashlib.sha256(candidate.title.encode('utf-8')).hexdigest()}"


def _candidate_progress_value(candidate: SemanticScholarCandidate, record: dict[str, Any] | None) -> str:
    value = {
        "candidate_paper_id": candidate.paper_id,
        "candidate_title": candidate.title,
        "doi": candidate.doi,
        "arxiv_id": candidate.arxiv_id,
        "matched_semantic_scholar_id": (record or {}).get("semantic_scholar_id") or "",
        "matched_doi": (record or {}).get("doi") or "",
        "matched_arxiv_id": (record or {}).get("arxiv_id") or "",
        "matched_title": (record or {}).get("title") or "",
    }
    return json.dumps(value, sort_keys=True)


def _authors(raw: Any) -> list[dict[str, str]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, str]] = []
    for author in raw:
        if not isinstance(author, dict):
            continue
        name = str(author.get("name") or "").strip()
        if not name:
            continue
        external = author.get("externalIds") if isinstance(author.get("externalIds"), dict) else {}
        out.append(
            {
                "name": name,
                "semantic_scholar_author_id": str(author.get("authorId") or ""),
                "orcid": norm_orcid(str(external.get("ORCID") or external.get("Orcid") or external.get("orcid") or "")),
            }
        )
    return out


def _references(raw: Any) -> list[dict[str, str]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, str]] = []
    for ref in raw:
        if not isinstance(ref, dict):
            continue
        normalized = normalize_semantic_scholar_paper(ref)
        if normalized.get("semantic_scholar_id") or normalized.get("doi") or normalized.get("arxiv_id") or normalized.get("title"):
            out.append(normalized)
    return out


def _reference_paper_id(ref: dict[str, Any], paper_index: PaperIdentityIndex) -> tuple[str, bool]:
    match = paper_index.paper_id_for(ref, default_id=_default_reference_paper_id(ref))
    return match.node_id, match.is_new


def _default_reference_paper_id(ref: dict[str, Any]) -> str:
    semantic_id = str(ref.get("semantic_scholar_id") or "")
    if semantic_id:
        return f"paper:semantic-scholar:{slugify(semantic_id)}"
    doi = norm_doi(str(ref.get("doi") or ""))
    if doi:
        return f"paper:doi:{slugify(doi)}"
    arxiv_id = norm_arxiv(str(ref.get("arxiv_id") or ""))
    if arxiv_id:
        return f"paper:arxiv:{slugify(arxiv_id)}"
    title = str(ref.get("title") or "unknown-reference")
    return f"paper:semantic-scholar-ref:{slugify(title)}"


def _default_person_id(author: dict[str, Any], name: str) -> str:
    orcid = norm_orcid(str(author.get("orcid") or ""))
    if orcid:
        return f"person:{slugify(orcid)}"
    author_id = str(author.get("semantic_scholar_author_id") or "")
    if author_id:
        return f"person:semantic-scholar:{slugify(author_id)}"
    return f"person:{slugify(name)}"


def _external_id(external: dict[str, Any], key: str) -> str:
    for candidate in (key, key.lower(), key.upper()):
        value = external.get(candidate)
        if value:
            return str(value).strip()
    return ""


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
    if record.get("venue") or record.get("year"):
        parts.append(f"{record.get('venue') or ''} {record.get('year') or ''}".strip())
    if record.get("doi"):
        parts.append(f"DOI: {record['doi']}")
    if record.get("arxiv_id"):
        parts.append(f"arXiv: {record['arxiv_id']}")
    if record.get("semantic_scholar_id"):
        parts.append(f"Semantic Scholar: {record['semantic_scholar_id']}")
    if record.get("abstract"):
        parts.append(str(record["abstract"]))
    return "\n".join(p for p in parts if p)


def _revision(record: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return {
        "provider": "semantic_scholar",
        "content_sha256": hashlib.sha256(payload).hexdigest(),
        "semantic_scholar_id": record.get("semantic_scholar_id") or "",
        "corpus_id": record.get("semantic_scholar_corpus_id") or "",
    }
