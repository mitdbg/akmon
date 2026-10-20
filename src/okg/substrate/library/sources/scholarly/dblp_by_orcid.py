"""dblp_by_orcid source — publication ingest with a multi-step
DBLP author-resolution chain.

Per `agent-led-mcp-onboarding` Phase 1 plus the follow-up
`extend-dblp-by-orcid-with-name-fallback` proposal: zero-config
publications for an operator who has an ORCID, with two
graceful fallbacks for the long tail of authors DBLP hasn't
linked to their ORCID.

Resolution chain (tried in order; first non-empty XML wins):

  1. ORCID URL — `https://dblp.org/orcid/<orcid>.xml`
  2. Explicit PID — `https://dblp.org/pid/<pid>.xml`
  3. Name search → PID URL — search
     `https://dblp.org/search/author/api?q=<name>&format=json`,
     accept iff exactly one hit, then fetch its PID.

The source emits `paper` NodeFacts with the same attribute
shape regardless of which path produced the XML, so downstream
ontology + queries see one publication shape.
"""
from __future__ import annotations

import hashlib
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

from lxml import etree as ET

from ._common import slugify
from okg.substrate.library.sources._safe_xml import safe_lxml_fromstring
from okg.substrate.library.sources._url_safety import (
    assert_public_http_url,
    open_validated_url,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourcePreflightResult,
    SourceRun,
)


_log = logging.getLogger(__name__)


@dataclass
class DblpByOrcidSource:
    """Fetch publications from DBLP for one author.

    Constructor params (the `params:` block of the source-defaults
    YAML — `ownership_id` lives at the registry's top level, not
    here; the runner reads it from `SourceEntry.ownership_id` at
    invoke time):
      - `orcid`: optional. The author's ORCID (e.g.
        `0000-0002-1825-0097`).
      - `dblp_pid`: optional. Explicit DBLP author PID in
        `<X>/<Y>` form (e.g. `000/0001`). Bypasses ORCID + name
        resolution; the disambiguation path for common names.
      - `author_name`: optional. Operator's full name as DBLP
        indexes it. Used as the fallback when the ORCID URL 404s
        and no explicit PID was given.
      - `cache_root`: optional. If provided, the XML response is
        cached under this dir keyed by ORCID (when available)
        or by PID. Cache writes are best-effort.
      - `endpoint`: optional override for the DBLP base URL
        (default `https://dblp.org`). Useful for tests + private
        mirrors.

    At least one of `orcid`, `dblp_pid`, `author_name` must be
    set for the source to produce results. All-empty configs
    soft-fail with a structured warning.
    """

    orcid: str = ""
    dblp_pid: str = ""
    author_name: str = ""
    cache_root: Optional[str] = None
    endpoint: str = "https://dblp.org"

    name: str = "dblp_by_orcid"
    profile: str = "mutable_api"

    def preflight(self, mode: str = "live") -> SourcePreflightResult:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        if not any((self.orcid, self.dblp_pid, self.author_name)):
            return SourcePreflightResult(
                source_name=self.name,
                status="missing_credential",
                mode=mode,
                required=False,
                reason=(
                    "no `orcid`, `dblp_pid`, or `author_name` "
                    "configured; pass --orcid / --pi-name / "
                    "--dblp-pid at install or set in "
                    "source_registry.yaml"
                ),
                checked_at=now,
            )
        configured = []
        if self.orcid:
            configured.append(f"orcid={self.orcid}")
        if self.dblp_pid:
            configured.append(f"dblp_pid={self.dblp_pid}")
        if self.author_name:
            configured.append(f"author_name={self.author_name!r}")
        return SourcePreflightResult(
            source_name=self.name,
            status="ok",
            mode=mode,
            required=False,
            reason="; ".join(configured),
            checked_at=now,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        **_ignored: Any,
    ) -> SourceRun:
        if not any((self.orcid, self.dblp_pid, self.author_name)):
            return SourceRun(facts=iter([]), completed_scope=True)
        try:
            xml_bytes = self._fetch_xml()
        except Exception as e:  # noqa: BLE001
            _log.warning(
                "dblp_by_orcid: could not resolve author "
                "(orcid=%r, pid=%r, name=%r): %s",
                self.orcid, self.dblp_pid, self.author_name, e,
            )
            return SourceRun(facts=iter([]), completed_scope=True)
        if xml_bytes is None:
            return SourceRun(facts=iter([]), completed_scope=True)
        return SourceRun(
            facts=self._stream(xml_bytes),
            completed_scope=True,
        )

    # ---------- resolution chain ----------

    def _fetch_xml(self) -> Optional[bytes]:
        """Walk the resolution chain. Returns the first non-empty
        XML payload from steps 1-3, or falls back to the on-disk
        cache after a transient network failure. Returns None if
        every configured step soft-failed (e.g. ORCID 404, name
        ambiguous, no PID configured) — caller treats None as
        "no publications, no error" and emits an empty SourceRun.
        Re-raises non-404 URLErrors when the cache also misses,
        preserving the legacy network-failure surface.
        """
        cache_path = self._cache_path()
        last_network_error: Optional[urllib.error.URLError] = None

        # Step 1: ORCID URL.
        if self.orcid:
            url = f"{self.endpoint}/orcid/{self.orcid}.xml"
            xml, err = self._fetch_url_soft(url)
            if xml is not None:
                self._maybe_write_cache(cache_path, xml)
                return xml
            if err is not None:
                last_network_error = err

        # Step 2: explicit PID.
        if self.dblp_pid:
            url = f"{self.endpoint}/pid/{self.dblp_pid}.xml"
            xml, err = self._fetch_url_soft(url)
            if xml is not None:
                self._maybe_write_cache(cache_path, xml)
                return xml
            if err is not None:
                last_network_error = err

        # Step 3: name search → PID URL.
        if self.author_name:
            resolved_pid, err = self._resolve_pid_from_name(
                self.author_name,
            )
            if err is not None:
                last_network_error = err
            if resolved_pid:
                url = f"{self.endpoint}/pid/{resolved_pid}.xml"
                xml, err = self._fetch_url_soft(url)
                if xml is not None:
                    self._maybe_write_cache(cache_path, xml)
                    return xml
                if err is not None:
                    last_network_error = err

        # Cache fallback for transient network failures.
        if (
            last_network_error is not None
            and cache_path is not None
            and cache_path.is_file()
        ):
            _log.info(
                "dblp_by_orcid: network failed, using cached XML at %s",
                cache_path,
            )
            return cache_path.read_bytes()

        # Re-raise the last network error so the caller's
        # try/except surfaces it in the operator's logs.
        if last_network_error is not None:
            raise last_network_error
        # All steps soft-failed (404 / zero hits / ambiguous);
        # caller emits an empty SourceRun.
        return None

    def _fetch_url_soft(
        self, url: str,
    ) -> tuple[Optional[bytes], Optional[urllib.error.URLError]]:
        """Fetch a URL. Returns `(bytes, None)` on success,
        `(None, None)` on HTTP 404 (treated as "try next step"),
        `(None, URLError)` on other network failure (caller may
        choose to fall back to cache or raise)."""
        # Validate the URL before the request (private/metadata SSRF guard).
        assert_public_http_url(url)
        req = urllib.request.Request(
            url, headers={"User-Agent": "okg-dblp-by-orcid/1.0"},
        )
        try:
            with open_validated_url(req, timeout=30) as resp:
                return resp.read(), None
        except urllib.error.HTTPError as e:
            if e.code == 404:
                _log.info(
                    "dblp_by_orcid: %s returned 404; trying next step",
                    url,
                )
                return None, None
            return None, e
        except urllib.error.URLError as e:
            return None, e

    def _resolve_pid_from_name(
        self, name: str,
    ) -> tuple[Optional[str], Optional[urllib.error.URLError]]:
        """Resolve an author's DBLP PID from their full name via
        DBLP's `search/author/api`. Accepts the result iff there
        is exactly one hit. Returns `(pid, None)` on success,
        `(None, None)` for zero or many hits (both soft-failures),
        `(None, URLError)` on network failure.
        """
        url = (
            f"{self.endpoint}/search/author/api?"
            f"q={urllib.parse.quote(name)}&format=json"
        )
        body, err = self._fetch_url_soft(url)
        if err is not None:
            return None, err
        if body is None:
            return None, None
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as e:
            _log.warning(
                "dblp_by_orcid: author-search returned non-JSON "
                "for name=%r: %s", name, e,
            )
            return None, None
        hits = (payload.get("result") or {}).get("hits") or {}
        # DBLP returns counts as STRINGS, not ints.
        try:
            total = int(hits.get("@total", "0"))
        except (TypeError, ValueError):
            total = 0
        if total == 0:
            _log.warning(
                "dblp_by_orcid: name %r matched 0 DBLP authors",
                name,
            )
            return None, None
        if total > 1:
            _log.warning(
                "dblp_by_orcid: name %r matched %d DBLP authors; "
                "pass --dblp-pid <X/Y> to disambiguate "
                "(see https://dblp.org/search?q=%s)",
                name, total, urllib.parse.quote(name),
            )
            return None, None
        # @total == 1: pull the lone hit's PID from its URL.
        raw_hits = hits.get("hit") or []
        if not raw_hits:
            return None, None
        info = (raw_hits[0] or {}).get("info") or {}
        author_url = info.get("url") or ""
        pid = self._pid_from_author_url(author_url)
        if not pid:
            _log.warning(
                "dblp_by_orcid: name %r resolved to URL %r but "
                "couldn't extract PID",
                name, author_url,
            )
            return None, None
        _log.info(
            "dblp_by_orcid: name %r → pid %s", name, pid,
        )
        return pid, None

    @staticmethod
    def _pid_from_author_url(url: str) -> str:
        """Extract `<X>/<Y>` PID from a DBLP author URL like
        `https://dblp.org/pid/000/0001` (or `http://`, with or
        without trailing slash). Returns empty string on no match.
        """
        if not url:
            return ""
        # Strip protocol + host; find the `/pid/<X>/<Y>` segment.
        marker = "/pid/"
        idx = url.find(marker)
        if idx < 0:
            return ""
        tail = url[idx + len(marker):].rstrip("/")
        # The PID is the first two path segments; anything beyond
        # is unrelated (DBLP doesn't usually append, but be safe).
        parts = tail.split("/")
        if len(parts) < 2:
            return ""
        return f"{parts[0]}/{parts[1]}"

    def _cache_path(self) -> Optional[Path]:
        if not self.cache_root:
            return None
        # Prefer ORCID-keyed cache for ORCID-resolved operators;
        # fall back to PID-keyed when ORCID is absent (avoids
        # cache collisions across operators on the same machine).
        if self.orcid:
            return Path(self.cache_root).expanduser() / f"{self.orcid}.xml"
        if self.dblp_pid:
            safe_pid = self.dblp_pid.replace("/", "_")
            return (
                Path(self.cache_root).expanduser()
                / f"pid_{safe_pid}.xml"
            )
        return None

    def _maybe_write_cache(
        self, cache_path: Optional[Path], xml: bytes,
    ) -> None:
        if cache_path is None:
            return
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(xml)
        except OSError as e:
            _log.warning(
                "dblp_by_orcid: cache write failed at %s: %s",
                cache_path, e,
            )

    # ---------- emission ----------

    def _stream(self, xml_bytes: bytes) -> Iterable[Any]:
        try:
            # Hardened lxml parse: no entity resolution / network / DTD load, so
            # billion-laughs / XXE inputs raise instead of expanding.
            root = safe_lxml_fromstring(xml_bytes)
        except ET.XMLSyntaxError as e:
            _log.warning("dblp_by_orcid: XML parse failed: %s", e)
            return
        # DBLP returns <dblpperson> wrapping <r> records; each <r>
        # contains exactly one publication child (article,
        # inproceedings, book, incollection, etc.).
        for r_elem in root.findall(".//r"):
            children = list(r_elem)
            if not children:
                continue
            pub = children[0]
            record = _extract_record(pub)
            if not record:
                continue
            yield from _emit_paper(record)

    def get_health(self) -> dict[str, Any]:
        return {
            "orcid": self.orcid,
            "dblp_pid": self.dblp_pid,
            "author_name": self.author_name,
            "endpoint": self.endpoint,
        }


def _extract_record(pub: Any) -> dict[str, Any]:
    """Extract the fields we care about from a DBLP publication
    element."""
    kind = pub.tag  # 'article' | 'inproceedings' | 'book' | ...
    key = pub.attrib.get("key") or ""
    if not key:
        return {}
    title_el = pub.find("title")
    if title_el is not None:
        # DBLP titles sometimes wrap inline markup (e.g. `<i>`,
        # `<sub>`); `itertext()` walks the subtree's text nodes in
        # document order so the flattened string preserves
        # surrounding spaces correctly.
        title = "".join(title_el.itertext()).strip()
    else:
        title = ""
    year_el = pub.find("year")
    year = (year_el.text or "").strip() if year_el is not None else ""
    venue = ""
    for vtag in ("booktitle", "journal"):
        v_el = pub.find(vtag)
        if v_el is not None and v_el.text:
            venue = v_el.text.strip()
            break
    authors = [
        (a.text or "").strip()
        for a in pub.findall("author")
        if a.text
    ]
    doi_el = pub.find("ee")
    doi = (doi_el.text or "").strip() if doi_el is not None else ""
    return {
        "kind": kind,
        "dblp_key": key,
        "title": title,
        "year": year,
        "venue": venue,
        "authors": authors,
        "doi": doi,
        "dblp_url": f"https://dblp.org/rec/{key}",
    }


def _emit_paper(record: dict[str, Any]) -> Iterable[Any]:
    """Build the NodeFact + ProgressMarker for one record."""
    dblp_key = record["dblp_key"]
    paper_id = f"paper:{dblp_key}"
    title = record.get("title", "") or dblp_key
    year = record.get("year", "") or ""
    venue = record.get("venue", "") or ""
    authors = record.get("authors", []) or []
    text_for_embed = (
        f"{title}. {', '.join(authors)}. {venue} {year}."
    ).strip()
    content_hash = hashlib.sha256(
        f"{dblp_key}|{title}|{year}|{venue}".encode("utf-8"),
    ).hexdigest()
    yield NodeFact(
        node_id=paper_id,
        subtype="paper",
        attrs={
            "paper_id": paper_id,
            "label": title,
            "title": title,
            "year": year,
            "venue": venue,
            "authors": authors,
            "doi": record.get("doi", ""),
            "dblp_key": dblp_key,
            "dblp_url": record.get("dblp_url", ""),
            "identity_match_kind": "new",
            "source_provider": "dblp",
            "text": text_for_embed,
        },
        source_record_id={
            "source": "dblp_by_orcid",
            "dblp_key": dblp_key,
        },
        source_revision={"content_sha": content_hash},
    )
    if venue and year:
        venue_id = _venue_node_id(venue)
        conference_id = _conference_id(venue)
        edition_id = _conference_edition_id(venue, year)
        yield NodeFact(
            node_id=venue_id,
            subtype="venue",
            attrs={
                "name": venue,
                "source_provider": "dblp",
                "text": venue,
            },
            source_record_id={
                "source": "dblp_by_orcid",
                "dblp_key": dblp_key,
                "venue": venue,
            },
            source_revision={
                "content_sha": hashlib.sha256(
                    f"{venue_id}|{venue}".encode("utf-8"),
                ).hexdigest(),
            },
        )
        yield NodeFact(
            node_id=conference_id,
            subtype="conference",
            attrs={
                "name": venue,
                "source_provider": "dblp",
                "text": venue,
            },
            source_record_id={
                "source": "dblp_by_orcid",
                "dblp_key": dblp_key,
                "venue": venue,
            },
            source_revision={
                "content_sha": hashlib.sha256(
                    f"{conference_id}|{venue}".encode("utf-8"),
                ).hexdigest(),
            },
        )
        yield NodeFact(
            node_id=edition_id,
            subtype="conference_edition",
            attrs={
                "name": f"{venue} {year}",
                "year": year,
                "source_provider": "dblp",
                "text": f"{venue} {year}",
            },
            source_record_id={
                "source": "dblp_by_orcid",
                "dblp_key": dblp_key,
                "venue": venue,
                "year": year,
            },
            source_revision={
                "content_sha": hashlib.sha256(
                    f"{edition_id}|{venue}|{year}".encode("utf-8"),
                ).hexdigest(),
            },
        )
        yield EdgeFact(
            src=edition_id,
            dst=conference_id,
            edge_type="member_of",
            attrs={"source_provider": "dblp"},
            source_record_id={
                "source": "dblp_by_orcid",
                "dblp_key": dblp_key,
                "venue": venue,
                "year": year,
                "edge": "member_of",
            },
        )
        yield EdgeFact(
            src=paper_id,
            dst=edition_id,
            edge_type="published_in",
            attrs={"source_provider": "dblp"},
            source_record_id={
                "source": "dblp_by_orcid",
                "dblp_key": dblp_key,
                "venue": venue,
                "year": year,
                "edge": "published_in",
            },
        )
    yield ProgressMarker(
        record_id=dblp_key, fingerprint=content_hash,
    )


def _venue_node_id(venue_name: str) -> str:
    return f"venue:{slugify(venue_name)}"


def _conference_id(venue_name: str) -> str:
    return f"conf:{slugify(venue_name)}"


def _conference_edition_id(venue_name: str, year: str) -> str:
    return f"confed:{slugify(venue_name)}-{slugify(year)}"
