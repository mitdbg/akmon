"""nsf_award_search source — public NSF Award Search API.

Per `refine-academic-pi-with-comms-and-calendar` Tier A:
queries `api.nsf.gov/services/v1/awards.json` filtered by PI
name + ORCID, emits Grant + Person (co-PIs) + Organization
(NSF) + Deadline (derived from quarterly reporting cadence).

No auth required. NOT a silent fallback — an empty response
for a name with NSF history would be a bug; an empty response
for a name with no NSF history is a legitimate empty success.
"""
from __future__ import annotations

import hashlib
import json
import urllib.parse
import urllib.request

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._url_safety import open_validated_url
from datetime import datetime, timedelta
from typing import Any, Iterable, Optional

from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourceRun,
)


_NSF_ENDPOINT = "https://api.nsf.gov/services/v1/awards.json"


class NsfAwardSearchSource:
    """Queries NSF Award Search by PI name. Emits Grant +
    Organization (NSF) + derived Deadline rows.
    """

    name: str = "nsf_award_search"
    profile: str = "mutable_api"

    def __init__(
        self,
        *,
        pi_name: Optional[str] = None,
        orcid: Optional[str] = None,
        fixture_path: Optional[str] = None,
        ownership_id: str = "nsf_award_search",
        max_pages: int = 5,
        **_ignored: Any,
    ) -> None:
        self.pi_name = pi_name
        self.orcid = orcid
        self.fixture_path = fixture_path
        self.ownership_id = ownership_id
        self.max_pages = max_pages

    def run(
        self, run_id: str, *, mode: str = "cursor", **_ignored: Any,
    ) -> SourceRun:
        if not self.pi_name:
            # Per dogfood F-6: NSF Award Search's public API takes
            # firstName/lastName parameters but has no native
            # ORCID lookup, unlike DOE OSTI's `orcid:` query.
            # Resolving ORCID → name via the ORCID public API
            # would let this source run zero-config, but is left
            # to a follow-up. For now, log + skip when only ORCID
            # is set.
            if self.orcid:
                import logging
                logging.getLogger(__name__).info(
                    "nsf_award_search: only ORCID provided "
                    "(%s) — NSF Award Search has no ORCID lookup "
                    "endpoint; set `pi_name` in source_registry.yaml "
                    "(e.g. 'Curie, Marie') to enable. Skipping.",
                    self.orcid,
                )
            return SourceRun(facts=iter([]), completed_scope=True)

        records = self._fetch_records()
        return SourceRun(
            facts=self._stream(records),
            completed_scope=True,
        )

    def _fetch_records(self) -> list[dict[str, Any]]:
        """Returns a list of NSF award records. Reads fixture
        file if `fixture_path` set; otherwise paginates the
        live API.
        """
        if self.fixture_path:
            from pathlib import Path
            with Path(self.fixture_path).open() as f:
                payload = json.load(f)
            return _extract_records(payload)

        # Live API. NSF returns up to 25 per page; we paginate
        # with offset += 25.
        records: list[dict[str, Any]] = []
        first, last = _split_name(self.pi_name)
        for page in range(self.max_pages):
            params = {
                "firstName": first,
                "lastName": last,
                "offset": str(page * 25),
                "rpp": "25",
                "printFields": (
                    "id,title,awardeeName,startDate,expDate,fundsObligatedAmt,"
                    "abstractText,piFirstName,piLastName,coPDPI"
                ),
            }
            url = f"{_NSF_ENDPOINT}?{urllib.parse.urlencode(params)}"
            req = urllib.request.Request(
                url, headers={"User-Agent": "okg-nsf/1.0"}
            )
            try:
                with open_validated_url(req, timeout=30) as resp:
                    payload = json.load(resp)
            except Exception as exc:  # noqa: BLE001 - keep source context.
                raise SubstrateError(
                    "nsf_award_search failed to fetch or parse NSF response",
                    data={
                        "source": self.name,
                        "endpoint": _NSF_ENDPOINT,
                        "page": page,
                    },
                    fix_hint=(
                        "check NSF Award Search API availability and retry "
                        "the source run"
                    ),
                ) from exc
            chunk = _extract_records(payload)
            if not chunk:
                break
            records.extend(chunk)
            if len(chunk) < 25:
                break
        return records

    def _stream(
        self, records: list[dict[str, Any]],
    ) -> Iterable[Any]:
        # Emit NSF as a funder Organization (idempotent).
        nsf_org_id = "org:nsf"
        yield NodeFact(
            node_id=nsf_org_id,
            subtype="organization",
            attrs={
                "organization_id": nsf_org_id,
                "name": "National Science Foundation",
                "kind": "funding_agency",
            },
            source_record_id={"organization_id": nsf_org_id},
            source_revision={"content_sha": "static:nsf"},
        )

        for rec in records:
            award_id = rec.get("id") or rec.get("awardId")
            if not award_id:
                continue
            grant_id = f"grant:nsf:{award_id}"
            content_hash = hashlib.sha256(
                json.dumps(rec, sort_keys=True).encode("utf-8")
            ).hexdigest()

            yield NodeFact(
                node_id=grant_id,
                subtype="grant",
                attrs={
                    "grant_id": grant_id,
                    "title": rec.get("title", ""),
                    "funder": "NSF",
                    "award_id": award_id,
                    "period_start": rec.get("startDate"),
                    "period_end": rec.get("expDate"),
                    "amount_usd": rec.get("fundsObligatedAmt", ""),
                    "status": _status_from_dates(rec),
                },
                source_record_id={"award_id": award_id},
                source_revision={"content_sha": content_hash},
            )

            # Derive quarterly reporting Deadlines between
            # period_start and period_end.
            for q_idx, due in enumerate(_quarterly_deadlines(
                rec.get("startDate"), rec.get("expDate"),
            )):
                deadline_id = f"deadline:nsf:{award_id}-q{q_idx}"
                yield NodeFact(
                    node_id=deadline_id,
                    subtype="deadline",
                    attrs={
                        "deadline_id": deadline_id,
                        "title": f"NSF quarterly report ({rec.get('title','')})",
                        "kind": "grant_report",
                        "due": due,
                        "status": "pending",
                        "subject_ref": grant_id,
                    },
                    source_record_id={"award_id": award_id, "q": q_idx},
                    source_revision={"content_sha": content_hash},
                )
                # Grant has_deadline Deadline
                yield EdgeFact(
                    src=grant_id,
                    dst=deadline_id,
                    edge_type="has_deadline",
                    source_record_id={"award_id": award_id, "q": q_idx},
                )

            yield ProgressMarker(record_id=award_id, fingerprint=content_hash)


def _extract_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """NSF wraps records in `{"response": {"award": [...]}}`."""
    if "response" in payload:
        award = payload["response"].get("award", [])
        return list(award) if isinstance(award, list) else []
    if "award" in payload and isinstance(payload["award"], list):
        return payload["award"]
    return []


def _split_name(full: str) -> tuple[str, str]:
    """Split "Last, First" or "First Last" into (first, last)."""
    if "," in full:
        last, first = full.split(",", 1)
        return first.strip(), last.strip()
    parts = full.strip().split()
    if len(parts) == 1:
        return "", parts[0]
    return parts[0], " ".join(parts[1:])


def _status_from_dates(rec: dict[str, Any]) -> str:
    """Infer grant status from period dates."""
    end = rec.get("expDate")
    if not end:
        return "active"
    try:
        end_dt = datetime.strptime(end, "%m/%d/%Y")
    except (ValueError, TypeError):
        try:
            end_dt = datetime.fromisoformat(end)
        except (ValueError, TypeError):
            return "active"
    now = datetime.now()
    if end_dt < now:
        return "closed"
    return "active"


def _quarterly_deadlines(
    start: Optional[str], end: Optional[str],
) -> list[str]:
    """Approximate quarterly reporting cadence (every 90 days)
    between start and end. Caps at 8 deadlines per grant.
    """
    if not start or not end:
        return []
    try:
        s = _parse_date(start)
        e = _parse_date(end)
    except Exception:  # noqa: BLE001 — operator-supplied date range malformed: skip window-paging (returns []).
        return []
    out: list[str] = []
    cur = s
    while cur < e and len(out) < 8:
        cur = cur + timedelta(days=90)
        out.append(cur.strftime("%Y-%m-%d"))
    return out


def _parse_date(s: str) -> datetime:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
        except (ValueError, TypeError):
            continue
    return datetime.fromisoformat(s)
