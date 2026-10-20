"""orcid_publications source — the day-1 publication win for the
academic-researcher profile.

Reads an operator's public ORCID record (no auth) and projects
their publication tail into the graph:

  - `<endpoint>/v3.0/<orcid>/works`       → Paper nodes
  - `<endpoint>/v3.0/<orcid>/employments` → Organization + affiliated_with
  - `<endpoint>/v3.0/<orcid>/person`      → the PI's Person node

Emission shape matches the shipping scholarly sources
(`openalex`, `dblp_by_orcid`): `paper` / `person` / `organization`
NodeFacts plus `authored_by` (paper -> person) and
`affiliated_with` (person -> organization) EdgeFacts — all
subtypes/edges the academic-researcher catalog inherits by composing the
scholarly module.

Field routing (per the `add-academic-pi-sources` design): each
Paper records a `field_route` provenance attr — `inspire-hep`
(physics), `dblp` (CS venues), or `openalex` (universal
fallback) — derived from the PI's employment discipline + venue.
The route is recorded so a downstream enricher can backfill
co-authors from the field-appropriate source and so a wrong
route is diagnosable; this source itself emits the ORCID-native
record (the PI's own authorship + affiliations), not co-author
fan-out.

NOT a silent fallback: a configured ORCID whose public API call
fails (malformed id -> 404, or a network error) raises so the
publish blocks and `okg metrics` surfaces the source error
(per the dogfood "no silent fallbacks" rule). A *reachable*
ORCID with zero works is a legitimate empty success. An
unconfigured ORCID skips the source entirely.

Idempotency: the ORCID work `put-code` is the stable record id;
re-runs upsert. Per `add-academic-pi-sources` task group 1.
"""
from __future__ import annotations

import hashlib
import json
import re
import urllib.parse
import urllib.request

from okg.substrate.library.sources._url_safety import open_validated_url
from dataclasses import dataclass
from typing import Any, Iterable

from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    SourcePreflightResult,
    SourceRun,
)


_PHYSICS_HINTS = (
    "physics", "inspire", "cern", "particle", "high energy",
    "high-energy", "astrophysic", "cosmolog", "nuclear",
)
_CS_VENUE_HINTS = (
    "sosp", "osdi", "vldb", "sigmod", "nsdi", "nips", "neurips",
    "icml", "iclr", "kdd", "usenix", "acm", "ieee", "popl", "pldi",
    "cvpr", "aaai", "www", "cidr", "icde", "eurosys", "fast", "atc",
)


@dataclass
class OrcidPublicationsSource:
    """Project a PI's ORCID record (works + employments + person).

    Constructor params (the `params:` block of the source-default
    YAML; `ownership_id` is read by the runner from the registry
    entry, not here):
      - `orcid`: the PI's ORCID, `NNNN-NNNN-NNNN-NNNN`. Required
        for the source to produce results; an empty value soft-
        skips (zero facts, no error) the same way the other
        identity sources do.
      - `endpoint`: ORCID public API base (default
        `https://pub.orcid.org`). Overridable for tests + mirrors.
      - `max_works`: cap on works projected per run (default 1000).
    """

    orcid: str = ""
    endpoint: str = "https://pub.orcid.org"
    max_works: int = 1000

    name: str = "orcid_publications"
    profile: str = "mutable_api"

    # ---------- preflight ----------

    def preflight(self, mode: str = "live") -> SourcePreflightResult:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        if not self.orcid:
            return SourcePreflightResult(
                source_name=self.name,
                status="missing_credential",
                mode=mode,
                required=False,
                reason=(
                    "no `orcid` configured; pass --orcid at install "
                    "or set it in source_registry.yaml "
                    "(NNNN-NNNN-NNNN-NNNN)"
                ),
                checked_at=now,
            )
        return SourcePreflightResult(
            source_name=self.name,
            status="ok",
            mode=mode,
            required=False,
            reason=f"orcid={self.orcid}",
            endpoint=self.endpoint,
            checked_at=now,
        )

    # ---------- run ----------

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        **_ignored: Any,
    ) -> SourceRun:
        if not self.orcid:
            # Unconfigured: legitimate skip, not an error.
            return SourceRun(facts=iter([]), completed_scope=True)
        # Fetch the three record sections. `_fetch_json` raises on a
        # hard failure (bad ORCID / network) — we do NOT swallow it:
        # a configured-but-broken source must surface loudly.
        works = self._fetch_json(f"/v3.0/{self.orcid}/works")
        employments = self._fetch_json(f"/v3.0/{self.orcid}/employments")
        person = self._fetch_json(f"/v3.0/{self.orcid}/person")
        return SourceRun(
            facts=self._stream(works, employments, person),
            completed_scope=True,
        )

    def _fetch_json(self, path: str) -> dict[str, Any]:
        url = f"{self.endpoint.rstrip('/')}{path}"
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "okg-orcid-publications/1.0",
            },
        )
        # Network / HTTP errors propagate to the caller (loud).
        with open_validated_url(req, timeout=30) as resp:
            body = resp.read()
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"orcid_publications: {url} returned non-JSON "
                f"(is the ORCID {self.orcid!r} valid?): {e}"
            ) from e
        if not isinstance(payload, dict):
            raise ValueError(
                f"orcid_publications: {url} returned a "
                f"{type(payload).__name__}, expected an object"
            )
        return payload

    # ---------- emission ----------

    def _stream(
        self,
        works: dict[str, Any],
        employments: dict[str, Any],
        person: dict[str, Any],
    ) -> Iterable[Any]:
        seen_nodes: set[str] = set()

        person_id = f"person:orcid:{self.orcid}"
        pi_name = _person_name(person) or self.orcid
        yield NodeFact(
            node_id=person_id,
            subtype="person",
            attrs={
                "person_id": person_id,
                "label": pi_name,
                "name": pi_name,
                "display_name": pi_name,
                "orcid": self.orcid,
                "identity_match_kind": "new",
                "source_provider": "orcid",
                "text": pi_name,
            },
            source_record_id={"source": "orcid_publications", "orcid": self.orcid},
            source_revision={"content_sha": _hash(self.orcid, pi_name)},
        )
        seen_nodes.add(person_id)

        employer_names: list[str] = []
        for emp in _employments(employments):
            org = emp.get("organization") or {}
            org_name = str(org.get("name") or "").strip()
            if not org_name:
                continue
            employer_names.append(org_name)
            org_id = _org_id(org)
            if org_id not in seen_nodes:
                disambig = org.get("disambiguated-organization") or {}
                addr = org.get("address") or {}
                yield NodeFact(
                    node_id=org_id,
                    subtype="organization",
                    attrs={
                        "organization_id": org_id,
                        "name": org_name,
                        "kind": "research_institution",
                        "country": str(addr.get("country") or ""),
                        "ror": str(
                            disambig.get("disambiguated-organization-identifier")
                            or ""
                        ) if str(disambig.get("disambiguation-source") or "").upper() == "ROR" else "",
                        "source_provider": "orcid",
                        "text": org_name,
                    },
                    source_record_id={
                        "source": "orcid_publications",
                        "organization": org_id,
                    },
                    source_revision={"content_sha": _hash(org_id, org_name)},
                )
                seen_nodes.add(org_id)
            yield EdgeFact(
                src=person_id,
                dst=org_id,
                edge_type="affiliated_with",
                attrs={
                    "source_provider": "orcid",
                    "role": str(emp.get("role-title") or ""),
                    "start_year": _date_year(emp.get("start-date")),
                    "end_year": _date_year(emp.get("end-date")),
                },
                source_record_id={
                    "source": "orcid_publications",
                    "orcid": self.orcid,
                    "organization": org_id,
                },
            )

        count = 0
        for work in _work_summaries(works):
            if count >= self.max_works:
                break
            put_code = work.get("put-code")
            if put_code is None:
                continue
            put_code = str(put_code)
            title = _work_title(work)
            if not title:
                continue
            count += 1
            year = _date_year(work.get("publication-date"))
            venue = str((work.get("journal-title") or {}).get("value") or "")
            doi = _work_doi(work)
            work_type = str(work.get("type") or "")
            route = _field_route(employer_names, venue, work_type)
            paper_id = f"paper:orcid:{self.orcid}:{put_code}"
            content_hash = _hash(put_code, title, year, venue, doi)
            text_for_embed = f"{title}. {venue} {year}.".strip()
            source_record_id = {
                "source": "orcid_publications",
                "orcid": self.orcid,
                "put_code": put_code,
            }
            yield NodeFact(
                node_id=paper_id,
                subtype="paper",
                attrs={
                    "paper_id": paper_id,
                    "label": title,
                    "title": title,
                    "year": year,
                    "venue": venue,
                    "doi": doi,
                    "orcid_put_code": put_code,
                    "orcid_work_type": work_type,
                    "field_route": route,
                    "identity_match_kind": "new",
                    "source_provider": "orcid",
                    "text": text_for_embed,
                },
                source_record_id=source_record_id,
                source_revision={"content_sha": content_hash},
            )
            yield EdgeFact(
                src=paper_id,
                dst=person_id,
                edge_type="authored_by",
                attrs={
                    "source_provider": "orcid",
                    "author_ordinal": 1,
                    "author_role": "orcid_owner",
                },
                source_record_id={**source_record_id, "edge": "authored_by"},
            )
            if venue and year:
                venue_id = _venue_node_id(venue)
                conference_id = _conference_id(venue)
                edition_id = _conference_edition_id(venue, year)
                if venue_id not in seen_nodes:
                    yield NodeFact(
                        node_id=venue_id,
                        subtype="venue",
                        attrs={
                            "name": venue,
                            "source_provider": "orcid",
                            "text": venue,
                        },
                        source_record_id={
                            "source": "orcid_publications",
                            "venue": venue,
                        },
                        source_revision={"content_sha": _hash(venue_id, venue)},
                    )
                    seen_nodes.add(venue_id)
                if conference_id not in seen_nodes:
                    yield NodeFact(
                        node_id=conference_id,
                        subtype="conference",
                        attrs={
                            "name": venue,
                            "source_provider": "orcid",
                            "text": venue,
                        },
                        source_record_id={
                            "source": "orcid_publications",
                            "venue": venue,
                        },
                        source_revision={
                            "content_sha": _hash(conference_id, venue),
                        },
                    )
                    seen_nodes.add(conference_id)
                if edition_id not in seen_nodes:
                    yield NodeFact(
                        node_id=edition_id,
                        subtype="conference_edition",
                        attrs={
                            "name": f"{venue} {year}",
                            "year": year,
                            "source_provider": "orcid",
                            "text": f"{venue} {year}",
                        },
                        source_record_id={
                            "source": "orcid_publications",
                            "venue": venue,
                            "year": year,
                        },
                        source_revision={
                            "content_sha": _hash(edition_id, venue, year),
                        },
                    )
                    seen_nodes.add(edition_id)
                    yield EdgeFact(
                        src=edition_id,
                        dst=conference_id,
                        edge_type="member_of",
                        attrs={"source_provider": "orcid"},
                        source_record_id={
                            "source": "orcid_publications",
                            "venue": venue,
                            "year": year,
                            "edge": "member_of",
                        },
                    )
                yield EdgeFact(
                    src=paper_id,
                    dst=edition_id,
                    edge_type="published_in",
                    attrs={"source_provider": "orcid"},
                    source_record_id={
                        **source_record_id,
                        "venue": venue,
                        "year": year,
                        "edge": "published_in",
                    },
                )
            yield ProgressMarker(record_id=put_code, fingerprint=content_hash)


# ---------- ORCID JSON helpers (defensive: the API nests deeply
# and many fields are legitimately null) ----------


def _work_summaries(works: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """ORCID `/works` wraps summaries in `group[].work-summary[]`.

    A work can appear under several external-id groupings; we take
    the first summary of each group (its canonical record)."""
    for group in works.get("group") or []:
        if not isinstance(group, dict):
            continue
        summaries = group.get("work-summary") or []
        if summaries and isinstance(summaries[0], dict):
            yield summaries[0]


def _employments(employments: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """ORCID `/employments` →
    `affiliation-group[].summaries[].employment-summary`."""
    for group in employments.get("affiliation-group") or []:
        if not isinstance(group, dict):
            continue
        for summary in group.get("summaries") or []:
            if not isinstance(summary, dict):
                continue
            emp = summary.get("employment-summary")
            if isinstance(emp, dict):
                yield emp


def _person_name(person: dict[str, Any]) -> str:
    name = person.get("name")
    if not isinstance(name, dict):
        return ""
    given = str((name.get("given-names") or {}).get("value") or "").strip()
    family = str((name.get("family-name") or {}).get("value") or "").strip()
    return " ".join(part for part in (given, family) if part)


def _work_title(work: dict[str, Any]) -> str:
    title = work.get("title")
    if not isinstance(title, dict):
        return ""
    inner = title.get("title")
    if not isinstance(inner, dict):
        return ""
    return str(inner.get("value") or "").strip()


def _work_doi(work: dict[str, Any]) -> str:
    ext = work.get("external-ids")
    if not isinstance(ext, dict):
        return ""
    for ext_id in ext.get("external-id") or []:
        if not isinstance(ext_id, dict):
            continue
        if str(ext_id.get("external-id-type") or "").lower() == "doi":
            return str(ext_id.get("external-id-value") or "").strip()
    return ""


def _date_year(date_obj: Any) -> str:
    if not isinstance(date_obj, dict):
        return ""
    year = date_obj.get("year")
    if isinstance(year, dict):
        return str(year.get("value") or "").strip()
    return ""


def _org_id(org: dict[str, Any]) -> str:
    disambig = org.get("disambiguated-organization") or {}
    if str(disambig.get("disambiguation-source") or "").upper() == "ROR":
        ror = str(disambig.get("disambiguated-organization-identifier") or "")
        tail = ror.rstrip("/").rsplit("/", 1)[-1]
        if tail:
            return f"org:ror:{_slug(tail)}"
    return f"org:name:{_slug(str(org.get('name') or 'unknown'))}"


def _venue_node_id(venue_name: str) -> str:
    return f"venue:{_slug(venue_name)}"


def _conference_id(venue_name: str) -> str:
    return f"conf:{_slug(venue_name)}"


def _conference_edition_id(venue_name: str, year: str) -> str:
    return f"confed:{_slug(venue_name)}-{_slug(year)}"


def _field_route(employer_names: list[str], venue: str, work_type: str) -> str:
    """Record which field-specific source SHOULD enrich this paper.

    A diagnosable provenance hint, not a fetch: physics employers
    → INSPIRE-HEP; recognized CS venues → DBLP; everything else →
    OpenAlex (universal fallback)."""
    employer_text = " ".join(employer_names).lower()
    if any(hint in employer_text for hint in _PHYSICS_HINTS):
        return "inspire-hep"
    venue_lower = venue.lower()
    if any(hint in venue_lower for hint in _CS_VENUE_HINTS):
        return "dblp"
    return "openalex"


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-") or "unknown"


def _hash(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
