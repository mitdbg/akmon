"""doe_osti source — public DOE OSTI API.

Per `refine-academic-pi-with-comms-and-calendar` Tier A: same
emission shape as `nsf_award_search` for DOE-funded works.
"""
from __future__ import annotations

import hashlib
import json
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


_OSTI_ENDPOINT = "https://www.osti.gov/api/v1/records"


class DoeOstiSource:
    """Queries DOE OSTI by ORCID (preferred) or author name. Emits
    Grant + Organization (DOE).

    Per dogfood F-6: previously took `pi_name` only and refused
    to run with `pi_name=None`, even when an ORCID was available
    in init_answers. OSTI's search API supports `orcid:` queries
    directly, so we prefer ORCID when set (more precise; no
    name-collision risk) and fall back to `author` text-match
    when only `pi_name` is provided. The source runs whenever
    EITHER is set.
    """

    name: str = "doe_osti"
    profile: str = "mutable_api"

    def __init__(
        self,
        *,
        pi_name: Optional[str] = None,
        orcid: Optional[str] = None,
        fixture_path: Optional[str] = None,
        ownership_id: str = "doe_osti",
        max_records: int = 100,
        **_ignored: Any,
    ) -> None:
        self.pi_name = pi_name
        self.orcid = orcid
        self.fixture_path = fixture_path
        self.ownership_id = ownership_id
        self.max_records = max_records

    def run(
        self, run_id: str, *, mode: str = "cursor", **_ignored: Any,
    ) -> SourceRun:
        # F-6: run as long as ONE of pi_name / orcid is set.
        if not self.pi_name and not self.orcid:
            return SourceRun(facts=iter([]), completed_scope=True)

        records = self._fetch_records()
        return SourceRun(
            facts=self._stream(records),
            completed_scope=True,
        )

    def _fetch_records(self) -> list[dict[str, Any]]:
        if self.fixture_path:
            from pathlib import Path
            with Path(self.fixture_path).open() as f:
                payload = json.load(f)
            return payload if isinstance(payload, list) else []
        # Per F-6: ORCID-first when available (no name-collision
        # risk, exact match on the canonical scholarly identifier);
        # author-name otherwise.
        if self.orcid:
            params = {
                "q": f"orcid:{self.orcid}",
                "rows": str(self.max_records),
            }
        else:
            params = {
                "author": self.pi_name,
                "rows": str(self.max_records),
            }
        url = f"{_OSTI_ENDPOINT}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(
            url, headers={"User-Agent": "okg-doe-osti/1.0"},
        )
        try:
            with open_validated_url(req, timeout=30) as resp:
                payload = json.load(resp)
        except Exception as exc:  # noqa: BLE001 - keep source context.
            raise SubstrateError(
                "doe_osti failed to fetch or parse DOE OSTI response",
                data={
                    "source": self.name,
                    "endpoint": _OSTI_ENDPOINT,
                    "query": "orcid" if self.orcid else "author",
                },
                fix_hint=(
                    "check DOE OSTI API availability and retry the source run"
                ),
            ) from exc
        return payload if isinstance(payload, list) else []

    def _stream(
        self, records: list[dict[str, Any]],
    ) -> Iterable[Any]:
        doe_org_id = "org:doe"
        yield NodeFact(
            node_id=doe_org_id,
            subtype="organization",
            attrs={
                "organization_id": doe_org_id,
                "name": "Department of Energy",
                "kind": "funding_agency",
            },
            source_record_id={"organization_id": doe_org_id},
            source_revision={"content_sha": "static:doe"},
        )

        for rec in records:
            osti_id = rec.get("osti_id") or rec.get("id")
            if not osti_id:
                continue
            grant_id = f"grant:doe:{osti_id}"
            content_hash = hashlib.sha256(
                json.dumps(rec, sort_keys=True).encode("utf-8")
            ).hexdigest()
            yield NodeFact(
                node_id=grant_id,
                subtype="grant",
                attrs={
                    "grant_id": grant_id,
                    "title": rec.get("title", ""),
                    "funder": "DOE",
                    "award_id": rec.get("award_id", osti_id),
                    "period_start": rec.get("publication_date"),
                    "status": "active",
                },
                source_record_id={"osti_id": osti_id},
                source_revision={"content_sha": content_hash},
            )
            yield ProgressMarker(record_id=str(osti_id), fingerprint=content_hash)
