"""mit_course_catalog source — MIT subject catalog scraper.

Per `refine-academic-pi-with-comms-and-calendar` Tier B:
fetches MIT's student catalog page and emits Course rows for
the configured instructor. Non-MIT PIs degrade gracefully
(zero rows + skip metric).

Scaffold — full catalog parsing requires careful HTML/structure
handling; this skeleton emits the right shape from a minimal
catalog-search query.
"""
from __future__ import annotations

import hashlib
import re
import urllib.parse
import urllib.request

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._url_safety import open_validated_url
from typing import Any, Iterable, Optional

from okg.substrate.library.sources.base import (
    NodeFact,
    ProgressMarker,
    SourceRun,
)


_MIT_CATALOG_BASE = "https://student.mit.edu/catalog/search.cgi"


class MitCourseCatalogSource:
    """MIT course catalog source. Scaffold."""

    name: str = "mit_course_catalog"
    profile: str = "mutable_api"

    def __init__(
        self,
        *,
        pi_name: Optional[str] = None,
        institution: str = "MIT",
        ownership_id: str = "mit_course_catalog",
        **_ignored: Any,
    ) -> None:
        self.pi_name = pi_name
        self.institution = institution
        self.ownership_id = ownership_id

    def run(
        self, run_id: str, *, mode: str = "cursor", **_ignored: Any,
    ) -> SourceRun:
        if self.institution != "MIT" or not self.pi_name:
            # Graceful degradation; non-MIT PIs emit nothing.
            return SourceRun(facts=iter([]), completed_scope=True)

        params = {"search": self.pi_name, "style": "verbatim"}
        url = f"{_MIT_CATALOG_BASE}?{urllib.parse.urlencode(params)}"
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "okg-mit-catalog/1.0"},
            )
            with open_validated_url(req, timeout=30) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as exc:  # noqa: BLE001 - keep source context.
            raise SubstrateError(
                "mit_course_catalog failed to fetch MIT catalog",
                data={
                    "source": self.name,
                    "url": url,
                    "pi_name": self.pi_name,
                },
                fix_hint=(
                    "check MIT catalog availability and retry the source run"
                ),
            ) from exc

        return SourceRun(
            facts=self._stream(html),
            completed_scope=True,
        )

    def _stream(self, html: str) -> Iterable[Any]:
        # Naive: find course-code patterns like "6.5830" in the
        # HTML, paired with titles. Full parsing is follow-up.
        content_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()
        course_pat = re.compile(
            r"<a[^>]*>(?P<code>\d+\.\d+[A-Z]?)</a>\s*"
            r"(?P<title>[^<\n]+)",
            re.I,
        )
        for idx, m in enumerate(course_pat.finditer(html)):
            code = m.group("code")
            title = m.group("title").strip()[:200]
            cid = f"course:mit:{code}"
            yield NodeFact(
                node_id=cid,
                subtype="course",
                attrs={
                    "course_id": cid,
                    "code": code,
                    "title": title,
                    "term": "",
                    "role": "instructor",
                    "_provenance": "extracted_model",
                    "_confidence": 0.80,
                },
                source_record_id={"code": code},
                source_revision={"content_sha": content_hash},
            )
        yield ProgressMarker(
            record_id=f"mit:{self.pi_name}", fingerprint=content_hash,
        )
