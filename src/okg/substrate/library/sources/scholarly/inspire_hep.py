"""INSPIRE HEP paper-source adapter for the Memex OKG."""

from __future__ import annotations

import hashlib
import json
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
from .scholarly_identity_aliases import PaperIdentityIndex, PersonIdentityIndex
from .network_cache import NetworkRequest, RateLimiter, SourceResponseCache

__all__ = [
    "InspireHepSource",
    "inspire_facts_from_record",
    "normalize_inspire_record",
]


DEFAULT_FIELDS = ",".join(
    [
        "control_number",
        "titles",
        "authors",
        "publication_info",
        "dois",
        "arxiv_eprints",
        "abstracts",
        "citation_count",
        "references",
        "document_type",
        "earliest_date",
        "updated",
    ]
)

DEFAULT_VENUES: tuple[dict[str, Any], ...] = (
    {"slug": "ichep", "name": "ICHEP", "queries": ["conference ICHEP and tc c"], "split_by_year": False},
    {"slug": "lattice", "name": "Lattice", "queries": ["conference Lattice and tc c"]},
    {
        "slug": "eps-hep",
        "name": "EPS-HEP",
        "queries": ["conference EPS-HEP and tc c", "conference EPS HEP and tc c"],
        "split_by_year": False,
    },
    {"slug": "dis", "name": "DIS", "queries": ["conference DIS and tc c"], "split_by_year": False},
    {"slug": "lhcp", "name": "LHCP", "queries": ["conference LHCP and tc c"], "split_by_year": False},
    {"slug": "susy", "name": "SUSY", "queries": ["conference SUSY and tc c"], "split_by_year": False},
    {
        "slug": "lepton-photon",
        "name": "Lepton-Photon",
        "queries": ["conference Lepton-Photon and tc c", "conference Lepton Photon and tc c"],
        "split_by_year": False,
    },
    {"slug": "moriond", "name": "Moriond", "queries": ["conference Moriond and tc c"], "split_by_year": False},
)


@dataclass
class InspireHepSource:
    endpoint: str = "https://inspirehep.net/api/literature"
    auth: str = "none"
    # Per A4 (Phase 1): no memex-specific default. Deployments set
    # cache_root in source_registry.yaml.
    cache_root: str | Path | None = None
    fixture_path: str | Path | None = None
    venues: list[dict[str, Any]] | tuple[dict[str, Any], ...] = field(
        default_factory=lambda: list(DEFAULT_VENUES)
    )
    # Per A4 (Phase 1): deployment-supplied venue-slug filter. None = all.
    venue_slugs: list[str] | tuple[str, ...] | None = None
    page_size: int = 1000
    max_records_per_venue: int | None = None
    requests_per_second: float | None = 1.0
    split_by_year: bool = True
    start_year: int = 1950
    end_year: int = 2026
    fields: str = DEFAULT_FIELDS
    max_retries: int = 3
    retry_backoff_seconds: float = 10.0
    watermark_field: str = "updated"
    target_subtypes: list[str] | tuple[str, ...] = (
        "paper",
        "person",
        "conference",
        "conference_edition",
    )
    dsn: str | None = None
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    _paper_identity_index: "PaperIdentityIndex | None" = field(default=None, init=False, repr=False)
    _person_identity_index: "PersonIdentityIndex | None" = field(default=None, init=False, repr=False)

    name: str = "inspire_hep"
    profile: str = "mutable_api"

    def __post_init__(self) -> None:
        if self.venue_slugs is not None:
            slug_set = {s for s in self.venue_slugs}
            available = {v["slug"] for v in self.venues}
            unknown = slug_set - available
            if unknown:
                from okg.substrate.errors import SubstrateError
                raise SubstrateError(
                    f"InspireHepSource: venue_slugs {sorted(unknown)!r} "
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
            "reference_stubs": 0,
            "venues": {},
            "pages_skipped_by_progress": 0,
            "pages_completed": 0,
            "canonical_matches": {"doi": 0, "arxiv_id": 0, "title": 0, "new": 0},
            "person_matches": {"new": 0},
        }
        prior_progress = since_progress or {}

        def _facts() -> Iterator[NodeFact | EdgeFact | ProgressMarker]:
            seen_nodes: set[str] = set()
            seen_stub_nodes: set[str] = set()
            seen_edges: set[tuple[str, str, str]] = set()
            paper_index = self._load_paper_identity_index()
            person_index = self._load_person_identity_index()
            if self.fixture_path:
                for record in _load_fixture_records(Path(self.fixture_path)):
                    normalized = normalize_inspire_record(record)
                    _apply_identity(normalized, paper_index, person_index, stats)
                    stats["records"] += 1
                    before_nodes = len(seen_nodes)
                    yield from inspire_facts_from_record(
                        normalized,
                        seen_nodes=seen_nodes,
                        seen_stub_nodes=seen_stub_nodes,
                        seen_edges=seen_edges,
                    )
                    stats["reference_stubs"] += max(0, len(seen_nodes) - before_nodes - 4)
            else:
                if self.cache_root is None:
                    from okg.substrate.errors import SubstrateError
                    raise SubstrateError(
                        "InspireHepSource: cache_root is required when "
                        "fixture_path is not set",
                        fix_hint=(
                            "Set `cache_root: <path>` in the deployment's "
                            "source_registry.yaml or use fixture_path."
                        ),
                    )
                cache = SourceResponseCache(self.cache_root)
                limiter = RateLimiter(requests_per_second=self.requests_per_second)
                for venue in self.venues:
                    venue_slug = str(venue["slug"])
                    emitted_for_venue = 0
                    for page in self._iter_venue_pages(
                        venue,
                        cache=cache,
                        limiter=limiter,
                        stats=stats,
                        prior_progress=prior_progress,
                    ):
                        for record in _records_from_search_payload(page.payload):
                            normalized = normalize_inspire_record(record, venue=venue)
                            if not normalized.get("inspire_recid"):
                                continue
                            _apply_identity(normalized, paper_index, person_index, stats)
                            stats["records"] += 1
                            emitted_for_venue += 1
                            before_nodes = len(seen_nodes)
                            yield from inspire_facts_from_record(
                                normalized,
                                seen_nodes=seen_nodes,
                                seen_stub_nodes=seen_stub_nodes,
                                seen_edges=seen_edges,
                            )
                            stats["reference_stubs"] += max(0, len(seen_nodes) - before_nodes - 4)
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
            next_cursor={"source": "inspire_hep", "run_id": run_id},
            run_mode=mode,
        )

    def _load_paper_identity_index(self) -> PaperIdentityIndex:
        if self._paper_identity_index is None:
            self._paper_identity_index = PaperIdentityIndex.from_dsn(self.dsn)
        return self._paper_identity_index

    def _load_person_identity_index(self) -> PersonIdentityIndex:
        if self._person_identity_index is None:
            self._person_identity_index = PersonIdentityIndex.from_dsn(self.dsn)
        return self._person_identity_index

    def _iter_venue_pages(
        self,
        venue: dict[str, Any],
        *,
        cache: SourceResponseCache,
        limiter: RateLimiter,
        stats: dict[str, Any],
        prior_progress: dict[str, str],
    ) -> Iterator["_InspirePage"]:
        for query_index, base_query in enumerate(_venue_queries(venue)):
            prefix_slug = venue["slug"] if query_index == 0 else f"{venue['slug']}:q{query_index}"
            split_by_year = bool(venue.get("split_by_year", self.split_by_year))
            if not split_by_year:
                yield from self._iter_search_pages(
                    query=base_query,
                    provider_record_prefix=prefix_slug,
                    cache=cache,
                    limiter=limiter,
                    stats=stats,
                    prior_progress=prior_progress,
                )
                continue

            for year in range(self.end_year, self.start_year - 1, -1):
                query = f"{base_query} and date {year}"
                prefix = f"{prefix_slug}:{year}"
                yield from self._iter_search_pages(
                    query=query,
                    provider_record_prefix=prefix,
                    cache=cache,
                    limiter=limiter,
                    stats=stats,
                    prior_progress=prior_progress,
                )

    def _iter_search_pages(
        self,
        *,
        query: str,
        provider_record_prefix: str,
        cache: SourceResponseCache,
        limiter: RateLimiter,
        stats: dict[str, Any],
        prior_progress: dict[str, str],
    ) -> Iterator["_InspirePage"]:
        page_number = 1
        while True:
            request = NetworkRequest(
                provider_record_id=f"{provider_record_prefix}:{page_number}",
                query={
                    "q": query,
                    "size": self.page_size,
                    "page": page_number,
                    "sort": "mostrecent",
                    "fields": self.fields,
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
                if sent <= 0 or page_number * self.page_size >= total:
                    break
                page_number += 1
                continue

            before_hits = stats["cache_hits"]
            before_misses = stats["cache_misses"]

            def _fetch() -> dict[str, Any]:
                limiter.wait()
                return _fetch_json(
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
            yield _InspirePage(
                payload=entry.payload,
                progress_key=progress_key,
                progress_value=_page_progress_value(request, entry.payload),
            )

            hits = _records_from_search_payload(entry.payload)
            total = _total_hits(entry.payload)
            if not hits:
                break
            if page_number * self.page_size >= total:
                break
            page_number += 1


class _AdHocStats:
    hits: int = 0
    misses: int = 0
    writes: int = 0
    skipped_by_watermark: int = 0


@dataclass(frozen=True)
class _InspirePage:
    payload: dict[str, Any]
    progress_key: str
    progress_value: str


def normalize_inspire_record(
    record: dict[str, Any],
    *,
    venue: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = record.get("metadata", record)
    recid = _optional_str(metadata.get("control_number")) or _optional_str(record.get("id"))
    title = _clean_title(_first_title(metadata.get("titles")))
    publication_info = _first_dict(metadata.get("publication_info"))
    year = _optional_str(publication_info.get("year")) or _optional_str(metadata.get("earliest_date"))[:4]
    venue_name = (venue or {}).get("name") or _optional_str(publication_info.get("journal_volume")) or "INSPIRE HEP"
    venue_slug = (venue or {}).get("slug") or slugify(venue_name)
    doi = _first_value(metadata.get("dois"), key="value")
    arxiv_id = _first_value(metadata.get("arxiv_eprints"), key="value")
    abstract = _first_value(metadata.get("abstracts"), key="value")
    updated = _optional_str(record.get("updated") or metadata.get("updated"))
    cnum = _optional_str(publication_info.get("cnum"))
    return {
        "inspire_recid": recid,
        "title": title or f"INSPIRE {recid}",
        "year": year,
        "venue": str(venue_name),
        "venue_slug": str(venue_slug),
        "edition_key": cnum or f"{venue_slug}-{year or 'unknown'}",
        "doi": doi,
        "arxiv_id": arxiv_id,
        "abstract": abstract,
        "citation_count": metadata.get("citation_count"),
        "document_type": metadata.get("document_type") or [],
        "authors": _authors(metadata.get("authors")),
        "references": _reference_recids(metadata.get("references")),
        "updated": updated,
        "raw": metadata,
    }


def inspire_facts_from_record(
    record: dict[str, Any],
    *,
    seen_nodes: set[str] | None = None,
    seen_stub_nodes: set[str] | None = None,
    seen_edges: set[tuple[str, str, str]] | None = None,
) -> Iterator[NodeFact | EdgeFact]:
    seen_nodes = seen_nodes if seen_nodes is not None else set()
    seen_stub_nodes = seen_stub_nodes if seen_stub_nodes is not None else set()
    seen_edges = seen_edges if seen_edges is not None else set()
    recid = str(record["inspire_recid"])
    paper_id = str(record.get("paper_id") or f"paper:inspire:{recid}")
    title = str(record.get("title") or f"INSPIRE {recid}")
    year = str(record.get("year") or "")
    revision = _revision(record)
    source_record_id = {"source": "inspire_hep", "inspire_recid": recid}

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
                "inspire_recid": recid,
                # Per `emit-paper-abstracts-from-scholarly-sources`:
                # surface the abstract that
                # `normalize_inspire_record` already extracted from
                # `metadata.abstracts[0].value`. Empty string when
                # absent so consumer queries
                # (`coalesce(attrs->>'abstract','') <> ''`) filter
                # consistently.
                "abstract": str(record.get("abstract") or ""),
                "citation_count": record.get("citation_count"),
                "document_type": record.get("document_type") or [],
                "identity_match_kind": record.get("identity_match_kind") or "new",
                "source_provider": "inspire_hep",
                "text": _paper_text(record),
            },
            source_record_id=source_record_id,
            source_revision=revision,
        ),
        seen_stub_nodes=seen_stub_nodes,
    )

    venue_name = str(record.get("venue") or "INSPIRE HEP")
    venue_slug = str(record.get("venue_slug") or slugify(venue_name))
    edition_key = slugify(str(record.get("edition_key") or f"{venue_slug}-{year or 'unknown'}"))
    conference_id = f"conf:{venue_slug}"
    edition_id = f"confed:{edition_key}"
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
                "source_provider": "inspire_hep",
                "text": venue_name,
            },
            source_record_id={"source": "inspire_hep", "conference": venue_slug},
            source_revision=revision,
        ),
        seen_stub_nodes=seen_stub_nodes,
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
                "inspire_cnum": record.get("edition_key") or "",
                "source_provider": "inspire_hep",
                "text": f"{venue_name} {year}".strip(),
            },
            source_record_id={
                "source": "inspire_hep",
                "conference": venue_slug,
                "edition": edition_key,
            },
            source_revision=revision,
        ),
        seen_stub_nodes=seen_stub_nodes,
    )
    yield from _yield_edge_once(
        seen_edges,
        EdgeFact(
            src=conference_id,
            dst=edition_id,
            edge_type="contains",
            attrs={"source_provider": "inspire_hep"},
            source_record_id={**source_record_id, "edge": "conference_contains_edition"},
            source_revision=revision,
        ),
    )
    # Per dogfood-canary DQ audit (2026-05-19): co-emit the
    # inverse `member_of` edge so queries from a
    # conference_edition up to its conference don't have to walk
    # edge_facts in reverse. bib_references already emits this;
    # inspire/openreview/openalex were dropping it, leaving
    # 5,158 of 5,186 conference_editions without member_of.
    yield from _yield_edge_once(
        seen_edges,
        EdgeFact(
            src=edition_id,
            dst=conference_id,
            edge_type="member_of",
            attrs={"source_provider": "inspire_hep"},
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
            attrs={"source_provider": "inspire_hep"},
            source_record_id={**source_record_id, "edge": "published_in"},
            source_revision=revision,
        ),
    )

    for ordinal, author in enumerate(record.get("authors") or [], start=1):
        name = str(author.get("name") or "").strip()
        if not name:
            continue
        person_key = str(author.get("recid") or author.get("bai") or slugify(name)).strip()
        person_id = str(
            author.get("person_id")
            or (f"person:inspire:{person_key}" if author.get("recid") else f"person:{person_key}")
        )
        yield from _yield_node_once(
            seen_nodes,
            NodeFact(
                node_id=person_id,
                subtype="person",
                attrs={
                    "person_id": person_id,
                    "label": name,
                    # Per dogfood-canary DQ audit (2026-05-19): set
                    # both `name` (canonical attr — what
                    # bib_references uses + what most read-side
                    # queries look for) and `display_name` (this
                    # source's historical convention). Without
                    # `name`, the 343K external-source person nodes
                    # were invisible to substrate consumers
                    # filtering on `attrs->>'name'`.
                    "name": name,
                    "display_name": name,
                    "inspire_author_recid": author.get("recid") or "",
                    "inspire_bai": author.get("bai") or "",
                    "identity_match_kind": author.get("identity_match_kind") or "new",
                    "source_provider": "inspire_hep",
                    "text": name,
                },
                source_record_id={"source": "inspire_hep", "person": person_key},
                source_revision=revision,
            ),
            seen_stub_nodes=seen_stub_nodes,
        )
        yield from _yield_edge_once(
            seen_edges,
            EdgeFact(
                src=paper_id,
                dst=person_id,
                edge_type="authored_by",
                attrs={"source_provider": "inspire_hep", "author_ordinal": ordinal},
                source_record_id={**source_record_id, "edge": "authored_by", "author": person_key},
                source_revision=revision,
            ),
        )
        # Per `emit-person-affiliations-from-scholarly-sources`:
        # emit Organization + affiliated_with for each
        # author-time affiliation.
        for aff in (author.get("affiliations") or []):
            aff_name = str(aff.get("name") or "").strip()
            if not aff_name:
                continue
            aff_recid = str(aff.get("recid") or "").strip()
            if aff_recid:
                org_id = f"org:inspire:{aff_recid}"
            else:
                org_id = f"org:name:{slugify(aff_name)}"
            yield from _yield_node_once(
                seen_nodes,
                NodeFact(
                    node_id=org_id,
                    subtype="organization",
                    attrs={
                        "organization_id": org_id,
                        "name": aff_name,
                        "kind": "",
                        "url": "",
                        "inspire_institution_recid": aff_recid,
                        "source_provider": "inspire_hep",
                        "text": aff_name,
                    },
                    source_record_id={"source": "inspire_hep",
                                       "organization": aff_recid
                                         or aff_name},
                    source_revision=revision,
                ),
                seen_stub_nodes=seen_stub_nodes,
            )
            yield from _yield_edge_once(
                seen_edges,
                EdgeFact(
                    src=person_id,
                    dst=org_id,
                    edge_type="affiliated_with",
                    attrs={
                        "source_provider": "inspire_hep",
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

    for ref_recid in record.get("references") or []:
        target_id = f"paper:inspire:{ref_recid}"
        yield from _yield_node_once(
            seen_nodes,
            NodeFact(
                node_id=target_id,
                subtype="paper",
                attrs={
                    "paper_id": target_id,
                    "label": f"INSPIRE {ref_recid}",
                    "title": "",
                    "inspire_recid": str(ref_recid),
                    "is_reference_stub": True,
                    "source_provider": "inspire_hep",
                    "text": f"INSPIRE reference {ref_recid}",
                },
                source_record_id={"source": "inspire_hep", "inspire_recid": str(ref_recid), "stub": True},
                source_revision=revision,
            ),
            seen_stub_nodes=seen_stub_nodes,
        )
        yield from _yield_edge_once(
            seen_edges,
            EdgeFact(
                src=paper_id,
                dst=target_id,
                edge_type="cites",
                attrs={"source_provider": "inspire_hep"},
                source_record_id={**source_record_id, "edge": "cites", "target": str(ref_recid)},
                source_revision=revision,
            ),
        )


def _apply_identity(
    record: dict[str, Any],
    paper_index: PaperIdentityIndex,
    person_index: PersonIdentityIndex,
    stats: dict[str, Any],
) -> None:
    default_paper_id = f"paper:inspire:{record['inspire_recid']}"
    paper_match = paper_index.paper_id_for(record, default_id=default_paper_id)
    record["paper_id"] = paper_match.node_id
    record["identity_match_kind"] = paper_match.kind
    stats["canonical_matches"].setdefault(paper_match.kind, 0)
    stats["canonical_matches"][paper_match.kind] += 1

    for author in record.get("authors") or []:
        name = str(author.get("name") or "").strip()
        if not name:
            continue
        person_key = str(author.get("recid") or author.get("bai") or slugify(name)).strip()
        default_person_id = f"person:inspire:{person_key}" if author.get("recid") else f"person:{person_key}"
        person_match = person_index.person_id_for(author, default_id=default_person_id)
        author["person_id"] = person_match.node_id
        author["identity_match_kind"] = person_match.kind
        stats["person_matches"].setdefault(person_match.kind, 0)
        stats["person_matches"][person_match.kind] += 1


def _venue_queries(venue: dict[str, Any]) -> list[str]:
    queries = venue.get("queries")
    if queries:
        return [str(query) for query in queries]
    return [str(venue["query"])]


def _yield_node_once(
    seen: set[str],
    fact: NodeFact,
    *,
    seen_stub_nodes: set[str],
) -> Iterator[NodeFact]:
    is_stub = bool(fact.attrs.get("is_reference_stub"))
    if fact.node_id in seen:
        if not is_stub and fact.node_id in seen_stub_nodes:
            seen_stub_nodes.discard(fact.node_id)
            yield fact
        return
    seen.add(fact.node_id)
    if is_stub:
        seen_stub_nodes.add(fact.node_id)
    yield fact


def _yield_edge_once(seen: set[tuple[str, str, str]], fact: EdgeFact) -> Iterator[EdgeFact]:
    key = (fact.src, fact.dst, fact.edge_type)
    if key in seen:
        return
    seen.add(key)
    yield fact


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
        unreachable_error="unreachable INSPIRE retry state",
    )


def _load_fixture_records(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict) and "records" in raw:
        return [r for r in raw["records"] if isinstance(r, dict)]
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    return _records_from_search_payload(raw)


def _records_from_search_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    hits = ((payload.get("hits") or {}).get("hits") or [])
    return [h for h in hits if isinstance(h, dict)]


def _total_hits(payload: dict[str, Any]) -> int:
    try:
        return int((payload.get("hits") or {}).get("total") or 0)
    except (TypeError, ValueError):
        return 0


def _sent_hits(payload: dict[str, Any]) -> int:
    return len(_records_from_search_payload(payload))


def _page_progress_value(request: NetworkRequest, payload: dict[str, Any]) -> str:
    value = {
        "provider_record_id": request.provider_record_id,
        "query": request.query,
        "sent": _sent_hits(payload),
        "total": _total_hits(payload),
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


def _first_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, list):
        return raw[0] if raw and isinstance(raw[0], dict) else {}
    return raw if isinstance(raw, dict) else {}


def _first_title(raw: Any) -> str:
    first = _first_dict(raw)
    return _optional_str(first.get("title"))


def _first_value(raw: Any, *, key: str) -> str:
    first = _first_dict(raw)
    return _optional_str(first.get(key))


def _authors(raw: Any) -> list[dict[str, Any]]:
    raw_authors = raw if isinstance(raw, list) else []
    out: list[dict[str, Any]] = []
    for author in raw_authors:
        if not isinstance(author, dict):
            continue
        name = _optional_str(author.get("full_name") or author.get("full_name_unicode_normalized"))
        recid = _optional_str(author.get("recid"))
        if not recid:
            ref = ((author.get("record") or {}).get("$ref") or "")
            recid = _last_path_segment(ref)
        ids = author.get("ids") if isinstance(author.get("ids"), list) else []
        bai = ""
        for item in ids:
            if isinstance(item, dict) and item.get("schema") == "INSPIRE BAI":
                bai = _optional_str(item.get("value"))
                break
        # Per `emit-person-affiliations-from-scholarly-sources`:
        # surface affiliations so the per-paper emission can
        # create Organization nodes + affiliated_with edges.
        affiliations: list[dict[str, str]] = []
        for aff in (author.get("affiliations") or []):
            if not isinstance(aff, dict):
                continue
            aff_name = _optional_str(aff.get("value"))
            if not aff_name:
                continue
            aff_recid = ""
            aff_ref = ((aff.get("record") or {}).get("$ref") or "")
            if aff_ref:
                aff_recid = _last_path_segment(aff_ref)
            affiliations.append({
                "name": aff_name,
                "recid": aff_recid,
            })
        out.append({
            "name": name,
            "recid": recid,
            "bai": bai,
            "affiliations": affiliations,
        })
    return out


def _reference_recids(raw: Any) -> list[str]:
    refs = raw if isinstance(raw, list) else []
    recids: list[str] = []
    seen: set[str] = set()
    for item in refs:
        if not isinstance(item, dict):
            continue
        for ref in _iter_ref_values(item):
            recid = _last_path_segment(ref)
            if recid and recid.isdigit() and recid not in seen:
                seen.add(recid)
                recids.append(recid)
    return recids


def _iter_ref_values(value: Any) -> Iterator[str]:
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "$ref" and isinstance(child, str) and "/api/literature/" in child:
                yield child
            else:
                yield from _iter_ref_values(child)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_ref_values(child)


def _last_path_segment(value: str) -> str:
    if not value:
        return ""
    return value.rstrip("/").rsplit("/", 1)[-1]


def _optional_str(raw: Any) -> str:
    return str(raw).strip() if raw not in (None, "") else ""


def _clean_title(title: str) -> str:
    return " ".join(title.replace("\n", " ").split()).rstrip(".")


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
        "provider": "inspire_hep",
        "content_sha256": hashlib.sha256(payload).hexdigest(),
        "inspire_recid": record.get("inspire_recid"),
        "updated": record.get("updated") or "",
    }
