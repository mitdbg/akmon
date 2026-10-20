"""DBLP paper-source adapter for the Memex OKG."""

from __future__ import annotations
from okg.substrate import fastyaml

import hashlib
import json
import logging
import time
import http.client
import socket
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

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
from .scholarly_identity_aliases import PaperIdentityIndex, PersonIdentityIndex
from .network_cache import NetworkRequest, RateLimiter, SourceResponseCache

__all__ = [
    "DblpSource",
    "dblp_facts_from_record",
    "normalize_dblp_record",
]


DEFAULT_VENUES: tuple[dict[str, Any], ...] = (
    {"slug": "nips", "name": "NeurIPS / NIPS", "query": "venue:NeurIPS:", "key_prefixes": ["conf/nips/", "conf/neurips/"]},
    {"slug": "icml", "name": "ICML", "query": "venue:ICML:", "key_prefixes": ["conf/icml/"]},
    {"slug": "iclr", "name": "ICLR", "query": "venue:ICLR:", "key_prefixes": ["conf/iclr/"]},
    {"slug": "aaai", "name": "AAAI", "query": "venue:AAAI:", "key_prefixes": ["conf/aaai/"]},
    {"slug": "ijcai", "name": "IJCAI", "query": "venue:IJCAI:", "key_prefixes": ["conf/ijcai/"]},
    {"slug": "cvpr", "name": "CVPR", "query": "venue:CVPR:", "key_prefixes": ["conf/cvpr/"]},
    {"slug": "iccv", "name": "ICCV", "query": "venue:ICCV:", "key_prefixes": ["conf/iccv/"]},
    {"slug": "eccv", "name": "ECCV", "query": "venue:ECCV:", "key_prefixes": ["conf/eccv/"]},
    {"slug": "acl", "name": "ACL", "query": "venue:ACL:", "key_prefixes": ["conf/acl/"]},
    {"slug": "emnlp", "name": "EMNLP", "query": "venue:EMNLP:", "key_prefixes": ["conf/emnlp/"]},
    {"slug": "naacl", "name": "NAACL", "query": "venue:NAACL:", "key_prefixes": ["conf/naacl/"]},
    {"slug": "stoc", "name": "STOC", "query": "venue:STOC:", "key_prefixes": ["conf/stoc/"]},
    {"slug": "focs", "name": "FOCS", "query": "venue:FOCS:", "key_prefixes": ["conf/focs/"]},
    {"slug": "soda", "name": "SODA", "query": "venue:SODA:", "key_prefixes": ["conf/soda/"]},
    {"slug": "osdi", "name": "OSDI", "query": "venue:OSDI:", "key_prefixes": ["conf/osdi/"]},
    {"slug": "nsdi", "name": "NSDI", "query": "venue:NSDI:", "key_prefixes": ["conf/nsdi/"]},
    {"slug": "sigcomm", "name": "SIGCOMM", "query": "venue:SIGCOMM:", "key_prefixes": ["conf/sigcomm/"]},
    # Added 2026-05-19 per memex operator scope decision.
    {"slug": "sosp", "name": "SOSP", "query": "venue:SOSP:", "key_prefixes": ["conf/sosp/"]},
    {
        "slug": "usenix-atc",
        "name": "USENIX ATC",
        "query": "venue:USENIX Annual Technical Conference:",
        "queries": [
            "venue:USENIX Annual Technical Conference:",
            "venue:USENIX ATC:",
        ],
        "key_prefixes": ["conf/usenix/"],
    },
    {"slug": "sigmod", "name": "SIGMOD", "query": "venue:SIGMOD Conference:", "key_prefixes": ["conf/sigmod/"]},
    {"slug": "vldb", "name": "VLDB", "query": "venue:VLDB:", "key_prefixes": ["conf/vldb/", "journals/pvldb/"]},
    # Added 2026-05-19 per memex operator scope decision.
    {"slug": "icde", "name": "ICDE", "query": "venue:ICDE:", "key_prefixes": ["conf/icde/"]},
    {"slug": "cidr", "name": "CIDR", "query": "venue:CIDR:", "key_prefixes": ["conf/cidr/"]},
    {"slug": "mlsys", "name": "MLSys", "query": "venue:MLSys:", "key_prefixes": ["conf/mlsys/"]},
    {"slug": "popl", "name": "POPL", "query": "venue:POPL:", "key_prefixes": ["conf/popl/"]},
    {"slug": "oopsla", "name": "OOPSLA", "query": "venue:OOPSLA:", "key_prefixes": ["conf/oopsla/", "journals/pacmpl/"]},
    {"slug": "pldi", "name": "PLDI", "query": "venue:PLDI:", "key_prefixes": ["conf/pldi/"]},
    {"slug": "usenix-sec", "name": "USENIX Security", "query": "venue:USENIX Security:", "key_prefixes": ["conf/uss/"]},
    {
        "slug": "ieee-sp",
        "name": "IEEE S&P",
        "query": "venue:IEEE S&P:",
        "queries": ["venue:IEEE S&P:", "venue:SP:"],
        "key_prefixes": ["conf/sp/"],
    },
    {"slug": "ccs", "name": "CCS", "query": "venue:CCS:", "key_prefixes": ["conf/ccs/"]},
    {"slug": "ndss", "name": "NDSS", "query": "venue:NDSS:", "key_prefixes": ["conf/ndss/"]},
)


@dataclass
class DblpSource:
    endpoint: str = "https://dblp.org/search/publ/api"
    auth: str = "none"
    # Substrate-library default is None (no silent fallback). Deployments
    # SHALL set `cache_root` explicitly in `source_registry.yaml` unless
    # they use `fixture_path` for tests. Per A4 (Phase 1):
    # `add-substrate-bibliographic-sources`.
    cache_root: str | Path | None = None
    fixture_path: str | Path | None = None
    venues: list[dict[str, Any]] | tuple[dict[str, Any], ...] = field(
        default_factory=lambda: list(DEFAULT_VENUES)
    )
    # Per A4 (Phase 1): deployment-supplied list of venue slugs that
    # narrows emission to only those venues. When None (default), all
    # `venues` entries are scanned. When set, the venues catalog is
    # filtered to entries whose slug is in `venue_slugs`. Unknown
    # slugs fail loudly via `__post_init__`.
    venue_slugs: list[str] | tuple[str, ...] | None = None
    page_size: int = 1000
    max_records_per_venue: int | None = None
    requests_per_second: float | None = 0.25
    split_by_year: bool = True
    start_year: int = 2000
    end_year: int = 2026
    max_retries: int = 3
    retry_backoff_seconds: float = 10.0
    watermark_field: str = "updated_at"
    target_subtypes: list[str] | tuple[str, ...] = (
        "paper",
        "person",
        "conference",
        "conference_edition",
    )
    # Per operator scope decision (2026-05-19): by-author scrape
    # mode. When `by_author = True`, after the venue/year scrape
    # completes the source ALSO walks the canonical-author list
    # at `author_aliases_path`. For each canonical author the
    # source queries dblp's author search endpoint, resolves the
    # PID, then fetches the author's publication list and emits
    # facts using the same `normalize_dblp_record` +
    # `dblp_facts_from_record` machinery as the venue path. This
    # catches journal articles + workshops + books the
    # venue-scoped scrape misses.
    by_author: bool = False
    author_aliases_path: str | None = None
    author_search_endpoint: str = (
        "https://dblp.org/search/author/api"
    )
    author_pubs_endpoint_template: str = "https://dblp.org/pid/{pid}.json"
    dsn: str | None = None
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    _paper_identity_index: "PaperIdentityIndex | None" = field(default=None, init=False, repr=False)
    _person_identity_index: "PersonIdentityIndex | None" = field(default=None, init=False, repr=False)

    name: str = "dblp"
    profile: str = "mutable_api"

    # Incremental-source contract (issue #323). DblpSource has three scopes,
    # so the probe is mode-dependent (built in `_build_change_probe`):
    #
    #   - `fixture_path` (tests / air-gapped): local bytes -> the ContentHashProbe
    #     floor over the fixture file. An edit busts a sha; a delete reads as
    #     `absent`. Config folds the venue/identity knobs that change emitted
    #     attrs under unchanged bytes.
    #   - live venue scan (`cache_root`, not `by_author`): the dblp search API is
    #     a mutable upstream with no global etag, so the probe is a bounded
    #     `version_token` MutableApiProbe — for each configured (venue-query x
    #     year) it fetches ONE 1-hit probe page and folds `(query, @total,
    #     top-hit info-hash)` into a membership-and-state digest. `@total`
    #     catches an add (count up) or a delete (count down); the top-hit hash
    #     catches an edit to the most-relevant record. Residual gap: an in-place
    #     edit to a record that does NOT change any query's `@total` and does NOT
    #     surface as a top hit is the watermark residual every mutable-API probe
    #     shares (see MutableApiProbe) — caught by the ungated reconcile pass, and
    #     strictly better than today (this source has no probe and re-reads +
    #     re-resolves identity every cycle).
    #   - live + `by_author`: emission ALSO depends on the local
    #     `author_aliases_path` file and each canonical author's dblp pub list, so
    #     the venue MutableApiProbe alone would be a false-unchanged. The probe
    #     becomes a UnionProbe of (venue MutableApiProbe) + (ContentHashProbe over
    #     the aliases file) + (per-author MutableApiProbe whose token folds each
    #     resolved pid's pub-count + pid-record mdate). changed iff ANY sub-probe
    #     changed.
    #
    # No `now()`-derived value reaches an emitted node/edge attr (verified: the
    # emission path normalize_dblp_record / dblp_facts_from_record / _revision
    # hashes only fetched record content), so a content/version token is sound. A
    # fetch error (network / HTTP / auth / missing file) PROPAGATES out of the
    # version_fn / build_token -> run_probe maps it to changed=True (a full read),
    # never a silent no-change skip.
    change_probe_kind = "mutable_api"

    def __post_init__(self) -> None:
        # Apply venue_slugs filter against the venues catalog.
        if self.venue_slugs is not None:
            slug_set = {s for s in self.venue_slugs}
            available = {v["slug"] for v in self.venues}
            unknown = slug_set - available
            if unknown:
                from okg.substrate.errors import SubstrateError
                raise SubstrateError(
                    f"DblpSource: venue_slugs {sorted(unknown)!r} are "
                    f"not in the venues catalog. Available slugs: "
                    f"{sorted(available)!r}.",
                    fix_hint=(
                        "Either add the missing slug to the `venues` "
                        "catalog parameter (see DEFAULT_VENUES for the "
                        "dict shape), or correct the spelling in "
                        "`venue_slugs`."
                    ),
                    data={"unknown": sorted(unknown), "available": sorted(available)},
                )
            self.venues = [v for v in self.venues if v["slug"] in slug_set]

        # Built LAST so the probe's config + venue scope reflect the
        # venue_slugs-filtered catalog above.
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        """Mode-dependent change-probe (issue #323). See `change_probe_kind`."""
        import sys

        if self.fixture_path is not None:
            from okg.substrate.library.sources.content_hash_probe import (
                ContentHashProbe,
            )

            fixture = Path(self.fixture_path)

            def _items() -> list[tuple[str, Path]]:
                # A vanished fixture is simply absent -> the membership digest
                # reads it as a delete rather than raising.
                return [(str(fixture), fixture)] if fixture.is_file() else []

            return ContentHashProbe(
                content_items=_items,
                config=self._probe_config(),
                emit_targets=[sys.modules[__name__]],
            )

        from okg.substrate.library.sources.mutable_api_probe import MutableApiProbe

        venue_probe = MutableApiProbe(
            version_fn=self._venue_scope_digest,
            config=self._probe_config(),
            emit_targets=[sys.modules[__name__]],
            content_kind="version_token",
        )

        if not (self.by_author and self.author_aliases_path):
            return venue_probe

        # by_author scope: union the venue API probe with the local aliases
        # file (ContentHashProbe) and the per-author dblp pub scope
        # (MutableApiProbe). A change in ANY of the three must re-emit.
        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.union_probe import UnionProbe

        from os.path import expanduser, expandvars

        aliases_path = Path(
            expanduser(expandvars(self.author_aliases_path or ""))
        )

        def _alias_items() -> list[tuple[str, Path]]:
            return (
                [(str(aliases_path), aliases_path)]
                if aliases_path.is_file()
                else []
            )

        aliases_probe = ContentHashProbe(
            content_items=_alias_items,
            config=self._probe_config(),
            emit_targets=[sys.modules[__name__]],
        )

        author_probe = MutableApiProbe(
            version_fn=self._author_scope_digest,
            config=self._probe_config(),
            emit_targets=[sys.modules[__name__]],
            content_kind="version_token",
        )

        return UnionProbe([venue_probe, aliases_probe, author_probe])

    def _probe_config(self) -> dict[str, Any]:
        """Every adapter input that changes which records get fetched / how they
        are emitted, hashed into the token's config component."""
        return {
            "endpoint": self.endpoint,
            "fixture_path": str(self.fixture_path) if self.fixture_path else None,
            "venues": [v.get("slug") for v in self.venues],
            "venue_queries": {
                str(v.get("slug")): _venue_queries(v) for v in self.venues
            },
            "venue_slugs": (
                list(self.venue_slugs) if self.venue_slugs is not None else None
            ),
            "page_size": self.page_size,
            "max_records_per_venue": self.max_records_per_venue,
            "split_by_year": self.split_by_year,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "target_subtypes": list(self.target_subtypes),
            "by_author": self.by_author,
            "author_aliases_path": self.author_aliases_path,
            "author_search_endpoint": self.author_search_endpoint,
            "author_pubs_endpoint_template": self.author_pubs_endpoint_template,
        }

    def _probe_queries(self) -> list[str]:
        """The full set of search queries the venue scan issues — the same
        (venue-query x year) enumeration `_iter_venue_pages` walks, so the probe
        scope matches the read scope."""
        queries: list[str] = []
        for venue in self.venues:
            for base_query in _venue_queries(venue):
                if not self.split_by_year:
                    queries.append(base_query)
                    continue
                for year in range(self.end_year, self.start_year - 1, -1):
                    queries.append(f"{base_query} year:{year}")
        return queries

    def _venue_scope_digest(self) -> str:
        """Bounded `version_token` over the live venue search scope.

        For each configured query, fetch ONE 1-hit probe page and fold
        `(query, @total, top-hit info-hash)` into a sorted membership-and-state
        digest plus the query count. `@total` moves on any add/delete in that
        query's result set; the top-hit hash moves on an edit to the
        most-relevant record. `_fetch_json` raises on network / HTTP / auth
        failure -> the error propagates out of build_token and run_probe forces a
        full read (never a silent no-change skip).
        """
        rows: list[tuple[str, int, str]] = []
        for query in self._probe_queries():
            payload = _fetch_json(
                self.endpoint,
                {"q": query, "format": "json", "h": 1, "f": 0},
                max_retries=self.max_retries,
                retry_backoff_seconds=self.retry_backoff_seconds,
            )
            total = _total_hits(payload)
            hits = _records_from_search_payload(payload)
            top_info = hits[0].get("info") if hits else None
            top_hash = hashlib.sha256(
                json.dumps(top_info, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            rows.append((query, total, top_hash))
        rows.sort()
        digest = hashlib.sha256()
        digest.update(f"venue:n={len(rows)}".encode("utf-8"))
        for query, total, top_hash in rows:
            digest.update(b"\x00")
            digest.update(
                json.dumps([query, total, top_hash], sort_keys=True).encode("utf-8")
            )
        return digest.hexdigest()

    def _author_scope_digest(self) -> str:
        """Bounded `version_token` over the per-author dblp pub scope.

        For each canonical author in the aliases file, resolve the dblp PID and
        fold `(canonical, pid, pub-count, pid-payload-hash)` into a membership
        digest. A new/removed publication moves the pub-count; a content edit on
        the pid record moves the payload hash; a resolution change moves the pid.
        Fetch errors (incl. an unparsable aliases file) propagate -> run_probe
        forces a full read.
        """
        from os.path import expanduser, expandvars

        aliases_path = Path(
            expanduser(expandvars(self.author_aliases_path or ""))
        )
        if not aliases_path.is_file():
            return "author:no-aliases-file"

        with aliases_path.open() as f:
            raw = fastyaml.safe_load(f) or []
        if not isinstance(raw, list):
            # An unparsable / wrong-shape aliases file is covered by the
            # aliases ContentHashProbe in the union; nothing to digest here.
            return "author:aliases-not-a-list"

        cache = SourceResponseCache(self.cache_root)
        limiter = RateLimiter(requests_per_second=self.requests_per_second)
        scratch_stats: dict[str, Any] = {"cache_hits": 0, "cache_misses": 0}

        rows: list[tuple[str, str, int, str]] = []
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            canonical = entry.get("canonical")
            aliases = entry.get("aliases") or []
            if not canonical or not aliases:
                continue
            search_name = max(
                (a for a in aliases if isinstance(a, str)),
                key=len,
                default=str(canonical),
            )
            pid = self._lookup_dblp_pid(
                search_name, cache=cache, limiter=limiter, stats=scratch_stats,
            )
            if not pid:
                rows.append((str(canonical), "", -1, "unresolved"))
                continue
            url = self.author_pubs_endpoint_template.format(pid=pid)
            payload = _fetch_json(
                url, {}, max_retries=self.max_retries,
                retry_backoff_seconds=self.retry_backoff_seconds,
            )
            pubs = _records_from_search_payload(payload)
            if not pubs:
                r = payload.get("r") if isinstance(payload, dict) else None
                if isinstance(r, list):
                    pubs = [item for item in r if isinstance(item, dict)]
            payload_hash = hashlib.sha256(
                json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
            rows.append((str(canonical), str(pid), len(pubs), payload_hash))

        rows.sort()
        digest = hashlib.sha256()
        digest.update(f"author:n={len(rows)}".encode("utf-8"))
        for row in rows:
            digest.update(b"\x00")
            digest.update(json.dumps(list(row), sort_keys=True).encode("utf-8"))
        return digest.hexdigest()

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict[str, Any] | None = None,
        since_progress: dict[str, Any] | None = None,
    ) -> SourceRun:
        stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "records": 0,
            "venues": {},
            "pages_skipped_by_progress": 0,
            "pages_completed": 0,
            "canonical_matches": {"doi": 0, "arxiv_id": 0, "title": 0, "new": 0},
            "person_matches": {"new": 0},
        }
        prior_progress = since_progress or {}

        def _facts() -> Iterator[NodeFact | EdgeFact | ProgressMarker]:
            seen_nodes: set[str] = set()
            seen_edges: set[tuple[str, str, str]] = set()
            paper_index = self._load_paper_identity_index()
            person_index = self._load_person_identity_index()
            if self.fixture_path:
                for record in _load_fixture_records(Path(self.fixture_path)):
                    normalized = normalize_dblp_record(record)
                    _apply_identity(normalized, paper_index, person_index, stats, provider="dblp")
                    stats["records"] += 1
                    yield from dblp_facts_from_record(
                        normalized,
                        seen_nodes=seen_nodes,
                        seen_edges=seen_edges,
                    )
            else:
                if self.cache_root is None:
                    from okg.substrate.errors import SubstrateError
                    raise SubstrateError(
                        "DblpSource: cache_root is required when "
                        "fixture_path is not set",
                        fix_hint=(
                            "Set `cache_root: <path>` in the "
                            "deployment's source_registry.yaml entry "
                            "for this source, or use `fixture_path` "
                            "for fixture-driven tests."
                        ),
                    )
                cache = SourceResponseCache(self.cache_root)
                limiter = RateLimiter(requests_per_second=self.requests_per_second)
                for venue in self.venues:
                    venue_slug = venue["slug"]
                    emitted_for_venue = 0
                    for page in self._iter_venue_pages(
                        venue,
                        cache=cache,
                        limiter=limiter,
                        stats=stats,
                        prior_progress=prior_progress,
                    ):
                        for record in _records_from_search_payload(page.payload):
                            normalized = normalize_dblp_record(record, venue=venue)
                            if not normalized.get("dblp_key"):
                                continue
                            if not _record_matches_venue(normalized, venue):
                                stats.setdefault("records_filtered", 0)
                                stats["records_filtered"] += 1
                                continue
                            _apply_identity(normalized, paper_index, person_index, stats, provider="dblp")
                            stats["records"] += 1
                            emitted_for_venue += 1
                            yield from dblp_facts_from_record(
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

                # Per-author scrape (operator scope decision
                # 2026-05-19): after the venue pass, walk the
                # canonical-author list and fetch each author's
                # full dblp publication record. Catches journal
                # articles + workshops + books the venue scope
                # misses.
                if self.by_author and self.author_aliases_path:
                    yield from self._iter_by_author_pubs(
                        cache=cache,
                        limiter=limiter,
                        stats=stats,
                        seen_nodes=seen_nodes,
                        seen_edges=seen_edges,
                        paper_index=paper_index,
                        person_index=person_index,
                    )
            self.last_run_stats = stats

        return SourceRun(
            facts=_facts(),
            completed_scope=mode in {"scope_complete", "reconcile"},
            next_cursor={"source": "dblp", "run_id": run_id},
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
    ) -> Iterator["_DblpPage"]:
        for query_index, base_query in enumerate(_venue_queries(venue)):
            prefix_slug = venue["slug"] if query_index == 0 else f"{venue['slug']}:q{query_index}"
            if not self.split_by_year:
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
                query = f"{base_query} year:{year}"
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
    ) -> Iterator["_DblpPage"]:
        offset = 0
        while True:
            request = NetworkRequest(
                provider_record_id=f"{provider_record_prefix}:{offset}",
                query={
                    "q": query,
                    "format": "json",
                    "h": self.page_size,
                    "f": offset,
                },
                provider_updated_at=None,
            )
            progress_key = f"page:{request.provider_record_id}"
            prior_page = _decode_page_progress(prior_progress.get(progress_key))
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
            yield _DblpPage(
                payload=entry.payload,
                progress_key=progress_key,
                progress_value=_page_progress_value(request, entry.payload),
            )

            hits = _records_from_search_payload(entry.payload)
            total = _total_hits(entry.payload)
            if not hits:
                break
            offset += len(hits)
            if offset >= total:
                break

    def _iter_by_author_pubs(
        self,
        *,
        cache: SourceResponseCache,
        limiter: RateLimiter,
        stats: dict[str, Any],
        seen_nodes: set[str],
        seen_edges: set[tuple[str, str, str]],
        paper_index: PaperIdentityIndex,
        person_index: PersonIdentityIndex,
    ) -> Iterator[NodeFact | EdgeFact | ProgressMarker]:
        """For each canonical author in author_aliases.yaml, look up
        dblp's PID and fetch the full publication list.

        Per operator scope decision (2026-05-19): catches journal +
        workshop + book publications outside the venue×year scope.
        """
        import yaml as _yaml
        from os.path import expanduser, expandvars
        author_stats: dict[str, Any] = {
            "authors_attempted": 0,
            "authors_resolved": 0,
            "authors_unresolved": 0,
            "publications_emitted": 0,
        }
        stats["by_author"] = author_stats

        aliases_path = Path(
            expanduser(expandvars(self.author_aliases_path or ""))
        )
        if not aliases_path.is_file():
            logger.warning(
                "DblpSource by_author: aliases_path=%s does not exist; "
                "skipping author scrape", aliases_path,
            )
            return

        # author_aliases.yaml is a top-level LIST of
        # {canonical, aliases:[...]} entries.
        try:
            with aliases_path.open() as f:
                raw = fastyaml.safe_load(f) or []
        except _yaml.YAMLError as e:
            logger.warning(
                "DblpSource by_author: failed to parse %s: %s",
                aliases_path, e,
            )
            return

        if not isinstance(raw, list):
            logger.warning(
                "DblpSource by_author: %s is not a list at top level; "
                "skipping (got %s)", aliases_path, type(raw).__name__,
            )
            return

        for entry in raw:
            if not isinstance(entry, dict):
                continue
            canonical = entry.get("canonical")
            aliases = entry.get("aliases") or []
            if not canonical or not aliases:
                continue
            # Prefer the longest/most-detailed name as the search
            # term — dblp does best with full first + middle + last.
            search_name = max(
                (a for a in aliases if isinstance(a, str)),
                key=len,
                default=str(canonical),
            )
            author_stats["authors_attempted"] += 1
            try:
                pid = self._lookup_dblp_pid(
                    search_name, cache=cache, limiter=limiter,
                    stats=stats,
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "DblpSource by_author: PID lookup failed for %r: %s",
                    search_name, e,
                )
                author_stats["authors_unresolved"] += 1
                continue
            if not pid:
                author_stats["authors_unresolved"] += 1
                continue
            author_stats["authors_resolved"] += 1
            # Fetch the pid endpoint and iterate publications.
            try:
                pubs = self._fetch_pid_publications(
                    pid, cache=cache, limiter=limiter, stats=stats,
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "DblpSource by_author: pub-list fetch failed for "
                    "pid=%s (%s): %s", pid, search_name, e,
                )
                continue
            for record in pubs:
                normalized = normalize_dblp_record(record)
                if not normalized.get("dblp_key"):
                    continue
                _apply_identity(
                    normalized, paper_index, person_index, stats,
                    provider="dblp",
                )
                stats["records"] += 1
                author_stats["publications_emitted"] += 1
                yield from dblp_facts_from_record(
                    normalized,
                    seen_nodes=seen_nodes,
                    seen_edges=seen_edges,
                )
            # Per-author progress marker — operator can resume
            # mid-list if a run is interrupted.
            yield ProgressMarker(
                key=f"by_author:{canonical}",
                value=json.dumps({"pid": pid, "name": search_name}),
            )

    def _lookup_dblp_pid(
        self,
        name: str,
        *,
        cache: SourceResponseCache,
        limiter: RateLimiter,
        stats: dict[str, Any],
    ) -> str | None:
        """Search dblp's author endpoint and return the highest-
        scoring PID, or None when no result matches.
        """
        request = NetworkRequest(
            provider_record_id=f"author_search:{slugify(name)}",
            query={"q": name, "format": "json", "h": 5},
            provider_updated_at=None,
        )

        def _fetch() -> dict[str, Any]:
            limiter.wait()
            return _fetch_json(
                self.author_search_endpoint,
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
        stats["cache_hits"] += cache_stats.hits
        stats["cache_misses"] += cache_stats.misses
        hits = _records_from_search_payload(entry.payload)
        if not hits:
            return None
        # Hits are sorted by descending score by dblp's API. Take the
        # first one's `@id` or `info.url` to derive the PID.
        for h in hits:
            info = h.get("info") if isinstance(h, dict) else None
            if not isinstance(info, dict):
                continue
            url = info.get("url") or ""
            # dblp PID URLs look like "https://dblp.org/pid/X/Y"
            if "/pid/" in url:
                return url.split("/pid/", 1)[1].rstrip("/")
            # Fallback: the `@id` field on the hit
            hid = h.get("@id") or ""
            if hid:
                return str(hid)
        return None

    def _fetch_pid_publications(
        self,
        pid: str,
        *,
        cache: SourceResponseCache,
        limiter: RateLimiter,
        stats: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Fetch dblp's pid/<X>/<Y>.json and return its publication
        records in the same shape `_records_from_search_payload`
        produces (a list of `{info: ...}` dicts).
        """
        url = self.author_pubs_endpoint_template.format(pid=pid)
        request = NetworkRequest(
            provider_record_id=f"pid:{pid}",
            query={},
            provider_updated_at=None,
        )

        def _fetch() -> dict[str, Any]:
            limiter.wait()
            # The pid endpoint is a path, not a query — pass empty
            # query and embed the pid in the URL.
            return _fetch_json(
                url, {}, max_retries=self.max_retries,
                retry_backoff_seconds=self.retry_backoff_seconds,
            )

        cache_stats = _AdHocStats()
        entry = cache.get_or_fetch(
            source_name=self.name,
            request=request,
            fetch=_fetch,
            stats=cache_stats,
        )
        stats["cache_hits"] += cache_stats.hits
        stats["cache_misses"] += cache_stats.misses
        # dblp returns the pubs under `result.hits.hit` when fetched
        # via the search URL family, OR under a top-level `r` array
        # when fetched via the pid path. Try both shapes.
        payload = entry.payload
        records = _records_from_search_payload(payload)
        if records:
            return records
        # PID path returns publications under a different shape.
        # Try `r` array.
        r = payload.get("r") if isinstance(payload, dict) else None
        if isinstance(r, list):
            # Each item has `info` already — same shape as venue search.
            return [item for item in r if isinstance(item, dict)]
        return []


class _AdHocStats:
    hits: int = 0
    misses: int = 0
    writes: int = 0
    skipped_by_watermark: int = 0


@dataclass(frozen=True)
class _DblpPage:
    payload: dict[str, Any]
    progress_key: str
    progress_value: str


def normalize_dblp_record(
    record: dict[str, Any],
    *,
    venue: dict[str, str] | None = None,
) -> dict[str, Any]:
    info = record.get("info", record)
    dblp_key = str(info.get("key") or record.get("@id") or "")
    title = _clean_title(str(info.get("title") or ""))
    year = _optional_str(info.get("year"))
    venue_name = _optional_str(info.get("venue")) or (venue or {}).get("name") or ""
    venue_slug = (venue or {}).get("slug") or slugify(venue_name or "unknown")
    doi = _optional_str(info.get("doi"))
    ee = _first_value(info.get("ee"))
    url = _optional_str(info.get("url"))
    authors = _authors(info.get("authors"))
    return {
        "dblp_key": dblp_key,
        "title": title,
        "year": year,
        "venue": venue_name,
        "venue_slug": venue_slug,
        "doi": doi,
        "ee": ee,
        "url": url,
        "authors": authors,
        "raw": info,
    }


def dblp_facts_from_record(
    record: dict[str, Any],
    *,
    seen_nodes: set[str] | None = None,
    seen_edges: set[tuple[str, str, str]] | None = None,
) -> Iterator[NodeFact | EdgeFact]:
    seen_nodes = seen_nodes if seen_nodes is not None else set()
    seen_edges = seen_edges if seen_edges is not None else set()
    dblp_key = str(record["dblp_key"])
    paper_id = str(record.get("paper_id") or f"paper:{dblp_key}")
    title = str(record.get("title") or dblp_key)
    year = str(record.get("year") or "")
    revision = _revision(record)
    source_record_id = {"source": "dblp", "dblp_key": dblp_key}

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
                "dblp_key": dblp_key,
                "dblp_url": record.get("url") or "",
                "ee": record.get("ee") or "",
                "identity_match_kind": record.get("identity_match_kind") or "new",
                "source_provider": "dblp",
                "text": _paper_text(record),
            },
            source_record_id=source_record_id,
            source_revision=revision,
        ),
    )

    venue_name = str(record.get("venue") or "")
    venue_slug = str(record.get("venue_slug") or slugify(venue_name or "unknown"))
    conference_id = f"conf:{venue_slug}"
    edition_id = f"confed:{venue_slug}-{year or 'unknown'}"
    if venue_name:
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
                    "source_provider": "dblp",
                    "text": venue_name,
                },
                source_record_id={"source": "dblp", "conference": venue_slug},
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
                    "source_provider": "dblp",
                    "text": f"{venue_name} {year}".strip(),
                },
                source_record_id={
                    "source": "dblp",
                    "conference": venue_slug,
                    "year": year or "unknown",
                },
                source_revision=revision,
            ),
        )
        yield from _yield_edge_once(
            seen_edges,
            EdgeFact(
                src=conference_id,
                dst=edition_id,
                edge_type="contains",
                attrs={"source_provider": "dblp"},
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
                attrs={"source_provider": "dblp"},
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
                attrs={"source_provider": "dblp"},
                source_record_id={**source_record_id, "edge": "published_in"},
                source_revision=revision,
            ),
        )

    for ordinal, author in enumerate(record.get("authors") or [], start=1):
        name = str(author.get("name") or "").strip()
        if not name:
            continue
        pid = str(author.get("pid") or "").strip()
        person_id = str(author.get("person_id") or (f"person:{pid}" if pid else f"person:{slugify(name)}"))
        yield from _yield_node_once(
            seen_nodes,
            NodeFact(
                node_id=person_id,
                subtype="person",
                attrs={
                    "person_id": person_id,
                    "label": name,
                    "display_name": name,
                    "dblp_pid": pid,
                    "identity_match_kind": author.get("identity_match_kind") or "new",
                    "source_provider": "dblp",
                    "text": name,
                },
                source_record_id={"source": "dblp", "person": pid or name},
                source_revision=revision,
            ),
        )
        yield from _yield_edge_once(
            seen_edges,
            EdgeFact(
                src=paper_id,
                dst=person_id,
                edge_type="authored_by",
                attrs={"source_provider": "dblp", "author_ordinal": ordinal},
                source_record_id={**source_record_id, "edge": "authored_by", "author": pid or name},
                source_revision=revision,
            ),
        )


def _apply_identity(
    record: dict[str, Any],
    paper_index: PaperIdentityIndex,
    person_index: PersonIdentityIndex,
    stats: dict[str, Any],
    *,
    provider: str,
) -> None:
    default_paper_id = f"paper:{record['dblp_key']}"
    paper_match = paper_index.paper_id_for(record, default_id=default_paper_id)
    record["paper_id"] = paper_match.node_id
    record["identity_match_kind"] = paper_match.kind
    stats["canonical_matches"].setdefault(paper_match.kind, 0)
    stats["canonical_matches"][paper_match.kind] += 1

    for author in record.get("authors") or []:
        name = str(author.get("name") or "").strip()
        if not name:
            continue
        pid = str(author.get("pid") or "").strip()
        default_person_id = f"person:{pid}" if pid else f"person:{slugify(name)}"
        person_match = person_index.person_id_for(author, default_id=default_person_id)
        author["person_id"] = person_match.node_id
        author["identity_match_kind"] = person_match.kind
        stats["person_matches"].setdefault(person_match.kind, 0)
        stats["person_matches"][person_match.kind] += 1


def _record_matches_venue(record: dict[str, Any], venue: dict[str, Any]) -> bool:
    key = str(record.get("dblp_key") or "")
    prefixes = venue.get("key_prefixes") or []
    if prefixes:
        return any(key.startswith(str(prefix)) for prefix in prefixes)
    expected_slug = str(venue.get("slug") or "")
    return str(record.get("venue_slug") or "") == expected_slug


def _venue_queries(venue: dict[str, Any]) -> list[str]:
    queries = venue.get("queries")
    if queries:
        return [str(query) for query in queries]
    return [str(venue["query"])]


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


def _fetch_json(
    endpoint: str,
    query: dict[str, Any],
    *,
    max_retries: int = 3,
    retry_backoff_seconds: float = 2.0,
) -> dict[str, Any]:
    # Per dblp DQ work (2026-05-19): exponential backoff with
    # jitter. Without jitter, a swarm of failing requests (across
    # the source's many query pairs) all retry in lock-step and
    # hit the same rate-limit window again — defeating the whole
    # backoff. Add ±20% jitter to break the lock-step.
    import random
    url = f"{endpoint}?{urllib.parse.urlencode(query)}"
    req = urllib.request.Request(url, headers={"User-Agent": okg_user_agent()})
    # Validate the (operator/config-overridable) endpoint URL before the
    # request so an override to a private/loopback/metadata host is refused.
    assert_public_http_url(req.full_url)
    last_exc: BaseException | None = None
    for attempt in range(max_retries + 1):
        try:
            with open_validated_url(req, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            last_exc = e
            if not _retryable_http_status(e.code) or attempt >= max_retries:
                raise
            retry_after = _retry_after_seconds(e)
            base = retry_after if retry_after is not None else (
                retry_backoff_seconds * (2 ** attempt)
            )
        except URLError as e:
            last_exc = e
            if attempt >= max_retries:
                raise
            base = retry_backoff_seconds * (2 ** attempt)
        # Per dblp DQ work (2026-05-19): also retry on bare
        # connection-level errors (RemoteDisconnected,
        # IncompleteRead, socket.timeout). These leak through the
        # urllib URLError wrapping for some failure modes.
        except (
            http.client.RemoteDisconnected,
            http.client.IncompleteRead,
            ConnectionResetError,
            ConnectionAbortedError,
            socket.timeout,
            TimeoutError,
        ) as e:
            last_exc = e
            if attempt >= max_retries:
                raise
            base = retry_backoff_seconds * (2 ** attempt)
        # ±20% jitter; clamp non-negative.
        sleep_for = max(0.0, base * random.uniform(0.8, 1.2))
        time.sleep(sleep_for)
    raise RuntimeError(
        f"unreachable DBLP retry state (last: {type(last_exc).__name__ if last_exc else 'None'})"
    )


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


def _load_fixture_records(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict) and "records" in raw:
        records = raw["records"]
    elif isinstance(raw, list):
        records = raw
    else:
        records = _records_from_search_payload(raw)
    return [r for r in records if isinstance(r, dict)]


def _records_from_search_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    hits = ((payload.get("result") or {}).get("hits") or {}).get("hit") or []
    if isinstance(hits, dict):
        return [hits]
    return [h for h in hits if isinstance(h, dict)]


def _total_hits(payload: dict[str, Any]) -> int:
    value = ((payload.get("result") or {}).get("hits") or {}).get("@total")
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _sent_hits(payload: dict[str, Any]) -> int:
    value = ((payload.get("result") or {}).get("hits") or {}).get("@sent")
    try:
        return int(value)
    except (TypeError, ValueError):
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


def _authors(raw: Any) -> list[dict[str, str]]:
    if isinstance(raw, dict):
        raw_authors = raw.get("author") or []
    else:
        raw_authors = raw or []
    if isinstance(raw_authors, (str, dict)):
        raw_authors = [raw_authors]

    out: list[dict[str, str]] = []
    for author in raw_authors:
        if isinstance(author, str):
            out.append({"name": author, "pid": ""})
        elif isinstance(author, dict):
            name = str(author.get("text") or author.get("#text") or author.get("name") or "")
            pid = str(author.get("@pid") or author.get("pid") or "")
            out.append({"name": name, "pid": pid})
    return out


def _first_value(raw: Any) -> str:
    if isinstance(raw, list):
        return _optional_str(raw[0]) if raw else ""
    return _optional_str(raw)


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
    return "\n".join(p for p in parts if p)


def _revision(record: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return {
        "provider": "dblp",
        "content_sha256": hashlib.sha256(payload).hexdigest(),
        "dblp_key": record.get("dblp_key"),
    }
