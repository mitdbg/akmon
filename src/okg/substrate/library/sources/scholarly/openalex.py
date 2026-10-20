"""OpenAlex metadata enrichment source for the Memex OKG."""

from __future__ import annotations

import difflib
import hashlib
import json
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
from .scholarly_identity_aliases import PaperIdentityIndex, PersonIdentityIndex, norm_arxiv, norm_doi, norm_orcid
from .network_cache import NetworkRequest, RateLimiter, SourceResponseCache

__all__ = [
    "OpenAlexSource",
    "OpenAlexCandidate",
    "OpenAlexRateLimited",
    "openalex_facts_from_record",
    "normalize_openalex_work",
    "abstract_from_inverted_index",
]


DEFAULT_SELECT = ",".join(
    [
        "id",
        "doi",
        "ids",
        "title",
        "display_name",
        "publication_year",
        "abstract_inverted_index",
        "concepts",
        "authorships",
        "referenced_works",
        "cited_by_count",
        "updated_date",
        "primary_location",
        "type",
    ]
)


@dataclass(frozen=True)
class OpenAlexCandidate:
    paper_id: str
    title: str
    year: str = ""
    doi: str = ""
    arxiv_id: str = ""
    source_provider: str = ""
    attrs: dict[str, Any] = field(default_factory=dict)


class OpenAlexRateLimited(RuntimeError):
    """Raised when OpenAlex asks us to wait longer than a foreground run should."""

    def __init__(self, *, retry_after: float | None = None) -> None:
        super().__init__("OpenAlex rate limit exhausted")
        self.retry_after = retry_after


@dataclass
class OpenAlexSource:
    endpoint: str = "https://api.openalex.org/works"
    auth: str = "optional_mailto"
    # Per A4 (Phase 1): no memex default; deployment sets explicitly.
    cache_root: str | Path | None = None
    fixture_path: str | Path | None = None
    dsn: str | None = None
    requests_per_second: float | None = 2.0
    page_size: int = 5
    max_doi_lookups_per_run: int = 2500
    max_title_queries_per_run: int = 500
    title_match_threshold: float = 0.94
    max_year_delta: int = 1
    max_references_per_record: int = 40
    max_concepts_per_record: int = 10
    max_authors_per_record: int = 50
    candidate_providers: list[str] | tuple[str, ...] = (
        "crossref",
        "dblp",
        "openreview",
        "inspire_hep",
        "legacy_paper_cache",
        "",
    )
    max_retries: int = 3
    retry_backoff_seconds: float = 60.0
    max_retry_after_seconds: float = 120.0
    watermark_field: str = "updated_date"
    target_subtypes: list[str] | tuple[str, ...] = ("paper", "person", "concept")
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    _paper_identity_index: "PaperIdentityIndex | None" = field(default=None, init=False, repr=False)
    _person_identity_index: "PersonIdentityIndex | None" = field(default=None, init=False, repr=False)

    name: str = "openalex"
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
            "doi_matches": 0,
            "title_matches": 0,
            "no_match": 0,
            "candidates_skipped_by_progress": 0,
            "concept_edges": 0,
            "citation_edges": 0,
            "author_edges": 0,
            "orcid_people": 0,
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
                            max_retries=self.max_retries,
                            retry_backoff_seconds=self.retry_backoff_seconds,
                            max_retry_after_seconds=self.max_retry_after_seconds,
                        )

                    cache_stats = _AdHocStats()
                    try:
                        entry = cache.get_or_fetch(
                            source_name=self.name,
                            request=request,
                            fetch=_fetch,
                            stats=cache_stats,
                        )
                    except OpenAlexRateLimited:
                        stats["rate_limited"] = True
                        stats["rate_limited_at"] = request.provider_record_id
                        self.last_run_stats = stats
                        return
                    stats["cache_hits"] = before_hits + cache_stats.hits
                    stats["cache_misses"] = before_misses + cache_stats.misses
                    payload = entry.payload

                match = _match_candidate(candidate, payload, self.title_match_threshold, self.max_year_delta)
                if match is None:
                    stats["no_match"] += 1
                    yield ProgressMarker(progress_key, _candidate_progress_value(candidate, None))
                    continue

                normalized, match_kind = match
                normalized["candidate_attrs"] = candidate.attrs
                default_paper_id = candidate.paper_id
                identity_match = paper_index.paper_id_for(normalized, default_id=default_paper_id)
                normalized["paper_id"] = candidate.paper_id if identity_match.is_new else identity_match.node_id
                normalized["identity_match_kind"] = identity_match.kind if not identity_match.is_new else match_kind
                normalized["candidate_title"] = candidate.title
                normalized["candidate_year"] = candidate.year
                normalized["candidate_source_provider"] = candidate.source_provider
                stats["records"] += 1
                if match_kind == "doi":
                    stats["doi_matches"] += 1
                else:
                    stats["title_matches"] += 1
                before = (len(seen_edges), len(seen_nodes))
                for fact in openalex_facts_from_record(
                    normalized,
                    person_index=person_index,
                    seen_nodes=seen_nodes,
                    seen_edges=seen_edges,
                    max_references=self.max_references_per_record,
                    max_concepts=self.max_concepts_per_record,
                    max_authors=self.max_authors_per_record,
                ):
                    if isinstance(fact, EdgeFact):
                        if fact.edge_type == "mentions":
                            stats["concept_edges"] += 1
                        elif fact.edge_type == "cites":
                            stats["citation_edges"] += 1
                        elif fact.edge_type == "authored_by":
                            stats["author_edges"] += 1
                    elif isinstance(fact, NodeFact) and fact.subtype == "person" and fact.attrs.get("orcid"):
                        stats["orcid_people"] += 1
                    yield fact
                _ = before
                yield ProgressMarker(progress_key, _candidate_progress_value(candidate, normalized))

            self.last_run_stats = stats

        return SourceRun(
            facts=_facts(),
            completed_scope=mode in {"scope_complete", "reconcile"},
            next_cursor={"source": "openalex", "run_id": run_id},
            run_mode=mode,
        )

    def _request_for_candidate(self, candidate: OpenAlexCandidate) -> NetworkRequest:
        doi = norm_doi(candidate.doi)
        if doi:
            return NetworkRequest(
                provider_record_id=f"doi:{doi}",
                query={"filter": f"doi:{doi}", "per-page": 1, "select": DEFAULT_SELECT},
            )
        return NetworkRequest(
            provider_record_id=f"title:{hashlib.sha256(candidate.title.encode('utf-8')).hexdigest()}",
            query={"search": candidate.title, "per-page": self.page_size, "select": DEFAULT_SELECT},
        )

    def _load_candidates(self) -> list[OpenAlexCandidate]:
        if not self.dsn:
            return []
        from okg.substrate.db.connection import connect as db_connect

        from okg.substrate.graph_store import ChronosGraphStore

        candidates: list[OpenAlexCandidate] = []
        seen: set[str] = set()
        # Chronos cutover: candidate selection reads current
        # default-branch graph state ("which paper nodes need OpenAlex
        # enrichment now"), so it resolves through a Chronos checkout
        # over okg.graph_nodes; the legacy `valid_to_gen IS NULL` pin is
        # dropped (the checkout session injects visibility). Rows are
        # dicts.
        with db_connect(self.dsn) as conn:
            graph = ChronosGraphStore.from_connection(
                conn, ensure_schema=False
            ).checkout()
            if self.max_doi_lookups_per_run > 0:
                rows = graph.query(
                    """
                    SELECT node_id, attrs
                      FROM okg.graph_nodes
                     WHERE subtype = 'paper'
                       AND coalesce(attrs->>'doi', '') <> ''
                       AND coalesce(attrs->>'openalex_id', '') = ''
                       AND coalesce(attrs->>'is_reference_stub', 'false') <> 'true'
                     ORDER BY node_id
                     LIMIT :limit
                    """,
                    {"limit": self.max_doi_lookups_per_run},
                )
                for row in rows:
                    candidate = _candidate_from_live(str(row["node_id"]), dict(row["attrs"] or {}))
                    seen.add(candidate.paper_id)
                    candidates.append(candidate)

            for provider in self.candidate_providers:
                if len([c for c in candidates if not c.doi]) >= self.max_title_queries_per_run:
                    break
                rows = graph.query(
                    """
                    SELECT node_id, attrs
                      FROM okg.graph_nodes
                     WHERE subtype = 'paper'
                       AND coalesce(attrs->>'title', '') <> ''
                       AND coalesce(attrs->>'doi', '') = ''
                       AND coalesce(attrs->>'openalex_id', '') = ''
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
                    if len([c for c in candidates if not c.doi]) >= self.max_title_queries_per_run:
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


def normalize_openalex_work(work: dict[str, Any]) -> dict[str, Any]:
    ids = work.get("ids") if isinstance(work.get("ids"), dict) else {}
    title = _clean_title(str(work.get("title") or work.get("display_name") or ""))
    doi = norm_doi(str(work.get("doi") or ids.get("doi") or ""))
    openalex_id = _openalex_short_id(str(work.get("id") or ids.get("openalex") or ""))
    primary_location = work.get("primary_location") if isinstance(work.get("primary_location"), dict) else {}
    source = primary_location.get("source") if isinstance(primary_location.get("source"), dict) else {}
    return {
        "openalex_id": openalex_id,
        "openalex_url": str(work.get("id") or ids.get("openalex") or ""),
        "doi": doi,
        "arxiv_id": norm_arxiv(str(ids.get("arxiv") or "")),
        "title": title,
        "year": str(work.get("publication_year") or ""),
        "abstract": abstract_from_inverted_index(work.get("abstract_inverted_index")),
        "concepts": _concepts(work.get("concepts")),
        "authors": _authors(work.get("authorships")),
        "referenced_works": _referenced_works(work.get("referenced_works")),
        "cited_by_count": work.get("cited_by_count"),
        "updated_date": str(work.get("updated_date") or ""),
        "type": str(work.get("type") or ""),
        "venue": str(source.get("display_name") or ""),
        "source_id": _openalex_short_id(str(source.get("id") or "")),
        "raw": work,
    }


def openalex_facts_from_record(
    record: dict[str, Any],
    *,
    person_index: PersonIdentityIndex | None = None,
    seen_nodes: set[str] | None = None,
    seen_edges: set[tuple[str, str, str]] | None = None,
    max_references: int = 40,
    max_concepts: int = 10,
    max_authors: int = 50,
) -> Iterator[NodeFact | EdgeFact]:
    seen_nodes = seen_nodes if seen_nodes is not None else set()
    seen_edges = seen_edges if seen_edges is not None else set()
    person_index = person_index or PersonIdentityIndex()
    paper_id = str(record["paper_id"])
    title = str(record.get("title") or record.get("candidate_title") or paper_id)
    revision = _revision(record)
    source_record_id = {"source": "openalex", "openalex_id": record.get("openalex_id") or "", "paper_id": paper_id}
    prior_attrs = dict(record.get("candidate_attrs") or {})
    paper_attrs = {
        **prior_attrs,
        "paper_id": paper_id,
        "label": title,
        "title": title,
        "year": str(record.get("year") or record.get("candidate_year") or prior_attrs.get("year") or ""),
        "doi": record.get("doi") or prior_attrs.get("doi") or "",
        "arxiv_id": record.get("arxiv_id") or prior_attrs.get("arxiv_id") or "",
        "openalex_id": record.get("openalex_id") or "",
        "openalex_url": record.get("openalex_url") or "",
        "openalex_type": record.get("type") or "",
        "abstract": record.get("abstract") or prior_attrs.get("abstract") or "",
        "cited_by_count": record.get("cited_by_count"),
        "primary_location_source": record.get("venue") or prior_attrs.get("primary_location_source") or "",
        "identity_match_kind": record.get("identity_match_kind") or "",
        "candidate_source_provider": record.get("candidate_source_provider") or prior_attrs.get("source_provider") or "",
        "source_provider": "openalex",
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

    for concept in (record.get("concepts") or [])[:max_concepts]:
        concept_id = str(concept.get("concept_id") or "")
        if not concept_id:
            continue
        # Per `backfill-concept-slugs`: every concept node carries
        # a deterministic slug so identity resolution between
        # operator-seeded, source-imported, and extractor-emitted
        # concepts collapses on a single key. Use the shared
        # slugify (kebab-case, non-alphanumeric collapsed to '-').
        _concept_name = concept.get("name") or concept_id
        yield from _yield_node_once(
            seen_nodes,
            NodeFact(
                node_id=concept_id,
                subtype="concept",
                attrs={
                    "name": _concept_name,
                    "slug": slugify(_concept_name),
                    "description": concept.get("description") or concept.get("name") or concept_id,
                    "openalex_id": concept.get("openalex_id") or "",
                    "wikidata": concept.get("wikidata") or "",
                    "level": concept.get("level"),
                    "score": concept.get("score"),
                    "source_provider": "openalex",
                    "text": concept.get("description") or concept.get("name") or concept_id,
                },
                source_record_id={"source": "openalex", "concept": concept.get("openalex_id") or concept_id},
                source_revision=revision,
            ),
        )
        yield from _yield_edge_once(
            seen_edges,
            EdgeFact(
                src=paper_id,
                dst=concept_id,
                edge_type="mentions",
                attrs={
                    "source_provider": "openalex",
                    "score": concept.get("score"),
                    "level": concept.get("level"),
                },
                source_record_id={**source_record_id, "edge": "mentions", "concept": concept_id},
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
                    # Per dogfood-canary DQ audit (2026-05-19): set
                    # both `name` (canonical) and `display_name`.
                    "name": name,
                    "display_name": name,
                    "orcid": author.get("orcid") or "",
                    "openalex_author_id": author.get("openalex_author_id") or "",
                    "identity_match_kind": match.kind,
                    "source_provider": "openalex",
                    "text": name,
                },
                source_record_id={"source": "openalex", "author": author.get("openalex_author_id") or name},
                source_revision=revision,
            ),
        )
        yield from _yield_edge_once(
            seen_edges,
            EdgeFact(
                src=paper_id,
                dst=person_id,
                edge_type="authored_by",
                attrs={"source_provider": "openalex", "author_ordinal": ordinal, "author_position": author.get("position") or ""},
                source_record_id={**source_record_id, "edge": "authored_by", "author": author.get("openalex_author_id") or name},
                source_revision=revision,
            ),
        )
        # Per `emit-person-affiliations-from-scholarly-sources`:
        # emit Organization + affiliated_with for each
        # authorship-time institution.
        for inst in (author.get("institutions") or []):
            inst_name = str(inst.get("name") or "").strip()
            if not inst_name:
                continue
            oa_id = str(inst.get("openalex_institution_id") or "").strip()
            ror = str(inst.get("ror") or "").strip()
            if oa_id:
                org_id = f"org:openalex:{slugify(oa_id)}"
            elif ror:
                org_id = f"org:ror:{slugify(ror.rsplit('/', 1)[-1])}"
            else:
                org_id = f"org:name:{slugify(inst_name)}"
            yield from _yield_node_once(
                seen_nodes,
                NodeFact(
                    node_id=org_id,
                    subtype="organization",
                    attrs={
                        "organization_id": org_id,
                        "name": inst_name,
                        "kind": str(inst.get("kind") or ""),
                        "url": ror or "",
                        "country": str(inst.get("country") or ""),
                        "openalex_institution_id": oa_id,
                        "source_provider": "openalex",
                        "text": inst_name,
                    },
                    source_record_id={"source": "openalex",
                                       "organization": oa_id
                                         or ror
                                         or inst_name},
                    source_revision=revision,
                ),
            )
            yield from _yield_edge_once(
                seen_edges,
                EdgeFact(
                    src=person_id,
                    dst=org_id,
                    edge_type="affiliated_with",
                    attrs={
                        "source_provider": "openalex",
                        "paper_ref": paper_id,
                    },
                    source_record_id={
                        **source_record_id,
                        "edge": "affiliated_with",
                        "person": person_id,
                        "organization": org_id,
                    },
                    source_revision=revision,
                ),
            )

    for ref_id in (record.get("referenced_works") or [])[:max_references]:
        target_id = f"paper:openalex:{slugify(ref_id)}"
        yield from _yield_node_once(
            seen_nodes,
            NodeFact(
                node_id=target_id,
                subtype="paper",
                attrs={
                    "paper_id": target_id,
                    "label": ref_id,
                    "title": "",
                    "openalex_id": ref_id,
                    "is_reference_stub": True,
                    "source_provider": "openalex",
                    "text": f"OpenAlex reference {ref_id}",
                },
                source_record_id={"source": "openalex", "openalex_id": ref_id, "stub": True},
                source_revision=revision,
            ),
        )
        yield from _yield_edge_once(
            seen_edges,
            EdgeFact(
                src=paper_id,
                dst=target_id,
                edge_type="cites",
                attrs={"source_provider": "openalex"},
                source_record_id={**source_record_id, "edge": "cites", "target": ref_id},
                source_revision=revision,
            ),
        )


def abstract_from_inverted_index(raw: Any) -> str:
    if not isinstance(raw, dict) or not raw:
        return ""
    max_pos = -1
    for positions in raw.values():
        if isinstance(positions, list):
            for pos in positions:
                if isinstance(pos, int):
                    max_pos = max(max_pos, pos)
    if max_pos < 0:
        return ""
    words = [""] * (max_pos + 1)
    for word, positions in raw.items():
        if not isinstance(positions, list):
            continue
        for pos in positions:
            if isinstance(pos, int) and 0 <= pos < len(words):
                words[pos] = str(word)
    return " ".join(word for word in words if word)


def _fetch_json(
    endpoint: str,
    query: dict[str, Any],
    *,
    max_retries: int = 3,
    retry_backoff_seconds: float = 2.0,
    max_retry_after_seconds: float = 120.0,
) -> dict[str, Any]:
    url = f"{endpoint}?{urllib.parse.urlencode(query)}"
    req = urllib.request.Request(url, headers={"User-Agent": okg_user_agent()})
    # Validate the (operator/config-overridable) endpoint URL before the
    # request so an override to a private/loopback/metadata host is refused.
    assert_public_http_url(req.full_url)
    for attempt in range(max_retries + 1):
        try:
            with open_validated_url(req, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            retry_after = _retry_after_seconds(e)
            if retry_after is not None and retry_after > max_retry_after_seconds:
                raise OpenAlexRateLimited(retry_after=retry_after) from e
            if e.code == 429 and attempt >= max_retries:
                raise OpenAlexRateLimited(retry_after=retry_after) from e
            if not _retryable_http_status(e.code) or attempt >= max_retries:
                raise
            sleep_for = retry_after if retry_after is not None else retry_backoff_seconds * (2 ** attempt)
        except URLError:
            if attempt >= max_retries:
                raise
            sleep_for = retry_backoff_seconds * (2 ** attempt)
        time.sleep(sleep_for)
    raise RuntimeError("unreachable OpenAlex retry state")


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
    candidate: OpenAlexCandidate,
    payload: dict[str, Any],
    title_match_threshold: float,
    max_year_delta: int,
) -> tuple[dict[str, Any], str] | None:
    results = _results_from_payload(payload)
    candidate_doi = norm_doi(candidate.doi)
    if candidate_doi:
        for item in results:
            normalized = normalize_openalex_work(item)
            if normalized.get("doi") == candidate_doi:
                return normalized, "doi"
        return None

    candidate_title = _title_fingerprint(candidate.title)
    candidate_year = _safe_int(candidate.year)
    best: tuple[float, dict[str, Any]] | None = None
    for item in results:
        normalized = normalize_openalex_work(item)
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
    if isinstance(payload.get("results"), list):
        return [r for r in payload["results"] if isinstance(r, dict)]
    if isinstance(payload.get("id"), str):
        return [payload]
    return []


def _candidate_from_live(node_id: str, attrs: dict[str, Any]) -> OpenAlexCandidate:
    return OpenAlexCandidate(
        paper_id=node_id,
        title=str(attrs.get("title") or attrs.get("label") or ""),
        year=str(attrs.get("year") or ""),
        doi=str(attrs.get("doi") or ""),
        arxiv_id=str(attrs.get("arxiv_id") or ""),
        source_provider=str(attrs.get("source_provider") or ""),
        attrs=attrs,
    )


def _fixture_candidates(path: Path) -> list[OpenAlexCandidate]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    candidates = raw.get("candidates") if isinstance(raw, dict) else []
    return [
        OpenAlexCandidate(
            paper_id=str(c["paper_id"]),
            title=str(c.get("title") or ""),
            year=str(c.get("year") or ""),
            doi=str(c.get("doi") or ""),
            arxiv_id=str(c.get("arxiv_id") or ""),
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


def _candidate_key(candidate: OpenAlexCandidate) -> str:
    if candidate.doi:
        return f"doi:{norm_doi(candidate.doi)}"
    return f"title:{hashlib.sha256(candidate.title.encode('utf-8')).hexdigest()}"


def _candidate_progress_value(candidate: OpenAlexCandidate, record: dict[str, Any] | None) -> str:
    value = {
        "candidate_paper_id": candidate.paper_id,
        "candidate_title": candidate.title,
        "doi": candidate.doi,
        "matched_openalex_id": (record or {}).get("openalex_id") or "",
        "matched_doi": (record or {}).get("doi") or "",
        "matched_title": (record or {}).get("title") or "",
    }
    return json.dumps(value, sort_keys=True)


def _concepts(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for concept in raw:
        if not isinstance(concept, dict):
            continue
        openalex_id = _openalex_short_id(str(concept.get("id") or ""))
        if not openalex_id:
            continue
        name = str(concept.get("display_name") or openalex_id)
        out.append(
            {
                "concept_id": f"concept:openalex:{slugify(openalex_id)}",
                "openalex_id": openalex_id,
                "name": name,
                "description": name,
                "wikidata": str(concept.get("wikidata") or ""),
                "level": concept.get("level"),
                "score": concept.get("score"),
            }
        )
    return out


def _authors(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for authorship in raw:
        if not isinstance(authorship, dict):
            continue
        author = authorship.get("author") if isinstance(authorship.get("author"), dict) else {}
        name = str(author.get("display_name") or "").strip()
        if not name:
            continue
        # Per `emit-person-affiliations-from-scholarly-sources`:
        # surface per-authorship institutions so the per-paper
        # emission can create Organization nodes + affiliated_with
        # edges.
        institutions: list[dict[str, str]] = []
        for inst in (authorship.get("institutions") or []):
            if not isinstance(inst, dict):
                continue
            inst_name = str(inst.get("display_name") or "").strip()
            if not inst_name:
                continue
            institutions.append({
                "name": inst_name,
                "openalex_institution_id": _openalex_short_id(
                    str(inst.get("id") or "")),
                "ror": str(inst.get("ror") or ""),
                "country": str(inst.get("country_code") or ""),
                "kind": str(inst.get("type") or ""),
            })
        out.append(
            {
                "name": name,
                "openalex_author_id": _openalex_short_id(str(author.get("id") or "")),
                "orcid": norm_orcid(str(author.get("orcid") or "")),
                "position": str(authorship.get("author_position") or ""),
                "institutions": institutions,
            }
        )
    return out


def _referenced_works(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for value in raw:
        ref = _openalex_short_id(str(value or ""))
        if ref:
            out.append(ref)
    return out


def _default_person_id(author: dict[str, Any], name: str) -> str:
    orcid = norm_orcid(str(author.get("orcid") or ""))
    if orcid:
        return f"person:{slugify(orcid)}"
    openalex_id = str(author.get("openalex_author_id") or "")
    if openalex_id:
        return f"person:openalex:{slugify(openalex_id)}"
    return f"person:{slugify(name)}"


def _openalex_short_id(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    value = value.rstrip("/").rsplit("/", 1)[-1]
    return value


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
    if record.get("abstract"):
        parts.append(str(record["abstract"]))
    return "\n".join(p for p in parts if p)


def _revision(record: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return {
        "provider": "openalex",
        "content_sha256": hashlib.sha256(payload).hexdigest(),
        "openalex_id": record.get("openalex_id") or "",
        "updated_date": record.get("updated_date") or "",
    }
