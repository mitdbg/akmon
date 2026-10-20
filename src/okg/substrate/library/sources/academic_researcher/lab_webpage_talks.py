"""lab_webpage_talks source — HTML talks-page parser.

Per `refine-academic-pi-with-comms-and-calendar` Tier B:
HTTP-fetches the lab webpage's "talks" section, LLM-extracts
talk events. Emits TalkEngagement rows that merge with
CV-derived talks by (title + date) match.

Scaffold — pairs with `cv_ingest_llm` to give two
auto-extraction paths for the same subtype, with the lab
webpage authoritative for talk URLs and recordings.
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
    NodeFact,
    ProgressMarker,
    SourceRun,
)


class LabWebpageTalksSource:
    """Lab webpage talks scraper. Scaffold."""

    name: str = "lab_webpage_talks"
    profile: str = "discovery_crawl"

    def __init__(
        self,
        *,
        talks_url: Optional[str] = None,
        ownership_id: str = "lab_webpage_talks",
        **_ignored: Any,
    ) -> None:
        self.talks_url = talks_url
        self.ownership_id = ownership_id

    def run(
        self, run_id: str, *, mode: str = "cursor", **_ignored: Any,
    ) -> SourceRun:
        if not self.talks_url:
            return SourceRun(facts=iter([]), completed_scope=True)
        # SSRF guard BEFORE the network try/except (loud rejection).
        # Per `validate-source-fetch-urls`.
        assert_public_http_url(self.talks_url)
        try:
            req = urllib.request.Request(
                self.talks_url,
                headers={"User-Agent": "okg-lab-talks/1.0"},
            )
            # open_validated_url re-validates every redirect hop; a bare urlopen
            # follows a 3xx to a private/metadata host past the first-hop check.
            with open_validated_url(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as exc:  # noqa: BLE001 - keep source context.
            raise SubstrateError(
                "lab_webpage_talks failed to fetch talks webpage",
                data={
                    "source": self.name,
                    "url": self.talks_url,
                },
                fix_hint=(
                    "check the configured talks_url and retry the source run"
                ),
            ) from exc
        return SourceRun(facts=self._stream(html), completed_scope=True)

    def _stream(self, html: str) -> Iterable[Any]:
        content_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()
        # Naive scrape: list items with date + title pattern.
        pat = re.compile(
            r"(?P<date>\d{4}(?:-\d{2}){0,2})\s*[-—:]\s*"
            r"(?P<title>[^<\n]{10,200})",
            re.I,
        )
        for idx, m in enumerate(pat.finditer(html)):
            talk_id = f"talk:lab:{content_hash[:12]}:{idx}"
            yield NodeFact(
                node_id=talk_id,
                subtype="talk_engagement",
                attrs={
                    "talk_id": talk_id,
                    "title": m.group("title").strip()[:200],
                    "kind": "invited_talk",
                    "date": m.group("date"),
                    "status": "given",
                    "_provenance": "extracted_model",
                    "_confidence": 0.75,
                },
                source_record_id={"url": self.talks_url, "idx": idx},
                source_revision={"content_sha": content_hash},
            )
        yield ProgressMarker(
            record_id=self.talks_url, fingerprint=content_hash,
        )
