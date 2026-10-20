"""group_webpage_scrape source — one-time HTTP + LLM extraction.

Per `refine-academic-pi-with-comms-and-calendar` Tier B:
fetches a configured group webpage URL, LLM-extracts the
member-list + roles + current-project titles. Emits Advisee
candidates (provenance=extracted_model awaiting confirmation),
Lab (one), Person advises Advisee edges.

Scaffold — full LLM integration is the implementation work
that follows. The fast path scrapes common CSAIL / LNS / EECS
patterns using HTML structural heuristics.
"""
from __future__ import annotations

import hashlib
import re
import urllib.request
from typing import Any, Iterable, Optional

from okg.substrate.errors import SubstrateError
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


class GroupWebpageScrapeSource:
    """Group webpage scraper. Scaffold."""

    name: str = "group_webpage_scrape"
    profile: str = "discovery_crawl"

    def __init__(
        self,
        *,
        group_webpage: Optional[str] = None,
        advisor_orcid: Optional[str] = None,
        lab_name: str = "Lab",
        ownership_id: str = "group_webpage_scrape",
        **_ignored: Any,
    ) -> None:
        self.group_webpage = group_webpage
        self.advisor_orcid = advisor_orcid
        self.lab_name = lab_name
        self.ownership_id = ownership_id

    def run(
        self, run_id: str, *, mode: str = "cursor", **_ignored: Any,
    ) -> SourceRun:
        if not self.group_webpage:
            return SourceRun(facts=iter([]), completed_scope=True)

        # SSRF guard BEFORE the network try/except: a rejection must
        # surface loudly (ingest failure), not be swallowed into an
        # empty result. Per `validate-source-fetch-urls`.
        assert_public_http_url(self.group_webpage)
        try:
            req = urllib.request.Request(
                self.group_webpage,
                headers={"User-Agent": "okg-group-scrape/1.0"},
            )
            # open_validated_url re-validates each redirect hop (a 3xx to a
            # private/metadata host is refused), closing the redirect-SSRF gap
            # that a bare urlopen leaves open.
            with open_validated_url(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as exc:  # noqa: BLE001 - keep source context.
            raise SubstrateError(
                "group_webpage_scrape failed to fetch group webpage",
                data={
                    "source": self.name,
                    "url": self.group_webpage,
                },
                fix_hint=(
                    "check the configured group_webpage URL and retry the "
                    "source run"
                ),
            ) from exc

        return SourceRun(
            facts=self._stream(html),
            completed_scope=True,
        )

    def _stream(self, html: str) -> Iterable[Any]:
        content_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()

        # Emit the Lab node.
        lab_id = f"lab:{hashlib.sha256(self.group_webpage.encode('utf-8')).hexdigest()[:12]}"
        yield NodeFact(
            node_id=lab_id,
            subtype="lab",
            attrs={
                "team_id": lab_id,
                "name": self.lab_name,
                "webpage_url": self.group_webpage,
                "_provenance": "extracted_model",
                "_confidence": 0.85,
            },
            source_record_id={"url": self.group_webpage},
            source_revision={"content_sha": content_hash},
        )

        # Extract member names from common patterns (h2/h3 with
        # personal info, or <li> with name + role). This is a
        # heuristic fast path — full LLM integration is a
        # follow-up but the shape is committed.
        for idx, (name, role) in enumerate(_extract_members(html)):
            person_id = f"person:scraped:{content_hash[:12]}:{idx}"
            yield NodeFact(
                node_id=person_id,
                subtype="advisee",
                attrs={
                    "person_id": person_id,
                    "display_name": name,
                    "role": _infer_role(role),
                    "status": "active",
                    "lab_ref": lab_id,
                    "_provenance": "extracted_model",
                    "_confidence": 0.70,
                },
                source_record_id={"url": self.group_webpage, "idx": idx},
                source_revision={"content_sha": content_hash},
            )
            # Advisor → Advisee edge (when advisor known).
            if self.advisor_orcid:
                yield EdgeFact(
                    src=f"person:orcid:{self.advisor_orcid}",
                    dst=person_id,
                    edge_type="advises",
                    provenance="extracted_model",
                    confidence=0.70,
                    source_record_id={
                        "url": self.group_webpage, "idx": idx,
                    },
                )

        yield ProgressMarker(
            record_id=self.group_webpage, fingerprint=content_hash,
        )


_NAME_PATTERNS = [
    # h3/h4 with name as text content
    re.compile(
        r"<h[34][^>]*>\s*([A-Z][a-z]+(?:\s+[A-Z][a-z\-]+){1,3})\s*</h[34]>",
        re.I,
    ),
    # Common bio card pattern: <div class="member"><strong>Name</strong>
    re.compile(
        r"class=\"[^\"]*member[^\"]*\"[^>]*>.*?"
        r"<strong>([A-Z][a-z]+(?:\s+[A-Z][a-z\-]+){1,3})</strong>",
        re.I | re.S,
    ),
]

_ROLE_HINTS = {
    "phd": ["phd", "ph.d", "doctoral"],
    "postdoc": ["postdoc", "post-doc", "research scientist"],
    "masters": ["masters", "m.s.", "ms student"],
    "undergrad": ["undergrad", "bs", "b.s."],
    "visiting": ["visiting", "exchange"],
}


def _extract_members(html: str) -> list[tuple[str, str]]:
    """Returns [(name, surrounding_context)] for each candidate.
    Surrounding context is the 200 chars after the name; used
    for role inference.
    """
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for pat in _NAME_PATTERNS:
        for m in pat.finditer(html):
            name = m.group(1).strip()
            if name in seen:
                continue
            seen.add(name)
            ctx_start = m.end()
            ctx = html[ctx_start:ctx_start + 200]
            out.append((name, ctx))
    return out


def _infer_role(context: str) -> str:
    ctx = context.lower()
    for role, hints in _ROLE_HINTS.items():
        if any(h in ctx for h in hints):
            return role
    return "phd"  # default; operator confirms via okg admission decide
