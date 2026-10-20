"""canvas_api source — Canvas LMS REST API with strict course allowlist.

Per `add-academic-corpora-sources`: Canvas is the one teaching
surface with professor-self-serve API access (an instructor mints
their own token under Account -> Settings; no admin app
registration). The source ingests the instructor's own courses,
assignments, and announcements:

  - course      -> `course` nodes (course:canvas:<id>)
  - assignment  -> `deadline` nodes (due dates; subject_ref points
                   at the course)
  - announcement-> `document` nodes (Redactor-truncated message)

Strict allowlist: `canvas_courses` lists course codes or ids;
unconfigured means a loud warning + zero rows (mirrors the
gmail-label contract). The Canvas token is a static per-user token
(not OAuth), but per `route-canvas-token-to-keychain` it still gets
OS-keychain custody via the substrate `TokenStore` (provider
`canvas`): resolved param > keychain > OKG_CANVAS_TOKEN env
(warned fallback). Set it with `okg set-token --provider canvas`.
Pagination follows Canvas's RFC-5988 Link headers.

Gradescope/Piazza/Blackboard are deliberately NOT connectors:
no official APIs (per the 2026-06 survey); grades flow in via the
Canvas LTI sync instead.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import urllib.request
from typing import Any, Callable, Iterator, Optional
from urllib.parse import urlparse

from okg.substrate.library.oauth.redaction import Redactor
from okg.substrate.library.sources._url_safety import (
    assert_token_request_url,
    open_validated_url,
)
from okg.substrate.library.sources.base import (
    NodeFact,
    ProgressMarker,
    SourcePreflightResult,
    SourceRun,
)

_log = logging.getLogger(__name__)

# (json_payload, link_header) per GET — injectable for tests.
HttpGet = Callable[[str, str], tuple[Any, str]]


def _default_http_get(url: str, token: str) -> tuple[Any, str]:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
    )
    # Bind the Bearer to the request URL's own host (already validated against
    # the Canvas origin by the caller) so a 3xx cannot redirect the token to
    # another origin; redirects are re-validated, never auto-followed.
    with open_validated_url(
        req, same_origin=urlparse(url).hostname or "", timeout=30,
    ) as resp:
        return (
            json.load(resp),
            resp.headers.get("Link", "") or "",
        )


def _next_link(link_header: str) -> Optional[str]:
    """RFC 5988: `<url>; rel="next", <url>; rel="last", ...`."""
    for part in link_header.split(","):
        match = re.search(r"<([^>]+)>\s*;\s*rel=\"next\"", part)
        if match:
            return match.group(1)
    return None


class CanvasApiSource:
    """Canvas LMS connector. Course allowlist required."""

    name: str = "canvas_api"
    profile: str = "mutable_api"

    def __init__(
        self,
        *,
        deployment_name: str,
        canvas_base_url: Optional[str] = None,
        canvas_courses: Optional[list[str]] = None,
        canvas_token: Optional[str] = None,
        pii_mode: str = "plain",
        max_body_kb: int = 50,
        per_page: int = 100,
        ownership_id: str = "canvas_api",
        http_get: Optional[HttpGet] = None,
        token_store: Optional[Any] = None,
        **_ignored: Any,
    ) -> None:
        self.deployment_name = deployment_name
        self.canvas_base_url = (canvas_base_url or "").rstrip("/")
        self.canvas_courses = [str(c) for c in (canvas_courses or [])]
        self.canvas_token = canvas_token
        self.redactor = Redactor(max_body_kb=max_body_kb, pii_mode=pii_mode)
        self.per_page = per_page
        self.ownership_id = ownership_id
        self._http_get = http_get or _default_http_get
        # Injectable TokenStore for tests; None => real OS keychain.
        self._token_store = token_store

    # Provider key under which the static Canvas token lives in the
    # OS keychain (TokenStore), alongside the OAuth providers.
    TOKEN_PROVIDER = "canvas"
    TOKEN_ENV = "OKG_CANVAS_TOKEN"

    def _token_and_custody(self) -> tuple[Optional[str], str]:
        from okg.substrate.library.oauth.static_token import (
            get_static_token,
        )
        return get_static_token(
            deployment=self.deployment_name,
            provider=self.TOKEN_PROVIDER,
            env_var=self.TOKEN_ENV,
            explicit=self.canvas_token,
            store=self._token_store,
        )

    def _token(self) -> Optional[str]:
        return self._token_and_custody()[0]

    def preflight(self, mode: str = "live") -> SourcePreflightResult:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        if not self.canvas_base_url:
            return SourcePreflightResult(
                source_name=self.name,
                status="missing_credential",
                mode="live",
                reason=(
                    "canvas_base_url not configured (e.g. "
                    "https://canvas.example.org)"
                ),
                checked_at=now,
            )
        token, custody = self._token_and_custody()
        if not token:
            return SourcePreflightResult(
                source_name=self.name,
                status="missing_credential",
                mode="live",
                credential_refs=("canvas (keychain)", "OKG_CANVAS_TOKEN"),
                reason=(
                    "no Canvas token: store one in the keychain with "
                    "`okg set-token --provider canvas --deployment "
                    f"{self.deployment_name}` (instructor self-serve "
                    "token from Account -> Settings -> New Access "
                    "Token), or set OKG_CANVAS_TOKEN as a fallback"
                ),
                checked_at=now,
            )
        return SourcePreflightResult(
            source_name=self.name,
            status="ok",
            mode="live",
            reason=f"base URL + token present (custody: {custody})",
            checked_at=now,
        )

    def run(
        self, run_id: str, *, mode: str = "cursor", **_ignored: Any,
    ) -> SourceRun:
        if not self.canvas_courses:
            _log.warning(
                "canvas_api: no canvas_courses configured; refusing "
                "to ingest (per strict-allowlist contract). Set "
                "`canvas_courses` in source_registry.yaml."
            )
            return SourceRun(facts=iter([]), completed_scope=True)
        if not self.canvas_base_url or not self._token():
            # Unrecoverable config failure — loud, zero rows.
            _log.warning(
                "canvas_api: missing canvas_base_url or token; see "
                "`okg doctor --check source-auth`."
            )
            return SourceRun(facts=iter([]), completed_scope=True)
        return SourceRun(
            facts=self._stream(), completed_scope=True,
        )

    def _pages(self, first_url: str) -> Iterator[Any]:
        url: Optional[str] = first_url
        token = self._token() or ""
        # Bind the Canvas bearer token to the configured Canvas host: validate
        # each URL (the operator-configured first page AND every response-provided
        # RFC-5988 `Link: rel=next` hop) as a public http(s) URL on the Canvas
        # origin before sending the token. A poisoned base or a Link header
        # pointing at 169.254.169.254 / another host is refused loudly, before
        # the credentialed fetch (per `centralize-egress-url-validation`).
        canvas_host = urlparse(self.canvas_base_url).hostname or ""
        while url:
            assert_token_request_url(url, canvas_host)
            payload, link = self._http_get(url, token)
            yield payload
            url = _next_link(link)

    def _stream(self) -> Iterator[Any]:
        base = self.canvas_base_url
        allow = set(self.canvas_courses)
        courses: list[dict[str, Any]] = []
        for page in self._pages(
            f"{base}/api/v1/courses?enrollment_type=teacher"
            f"&per_page={self.per_page}"
        ):
            courses.extend(page or [])

        matched = [
            c for c in courses
            if str(c.get("id")) in allow
            or (c.get("course_code") or "") in allow
        ]
        unmatched = allow - {
            value
            for c in matched
            for value in (str(c.get("id")), c.get("course_code") or "")
        }
        if unmatched:
            _log.warning(
                "canvas_api: allowlisted course(s) not found among "
                "this instructor's courses: %s",
                sorted(unmatched),
            )

        for course in matched:
            yield from self._emit_course(course)

    def _emit_course(self, course: dict[str, Any]) -> Iterator[Any]:
        base = self.canvas_base_url
        cid_raw = str(course["id"])
        course_id = f"course:canvas:{cid_raw}"
        fingerprint = _sha(repr(course))
        yield NodeFact(
            node_id=course_id,
            subtype="course",
            attrs={
                "course_id": course_id,
                "code": course.get("course_code") or cid_raw,
                "title": course.get("name") or "",
                "term": (course.get("term") or {}).get("name", "")
                if isinstance(course.get("term"), dict) else "",
                "role": "instructor",
                "external_ref": cid_raw,
            },
            source_record_id={"course_id": cid_raw},
            source_revision={"content_sha": fingerprint},
        )
        yield ProgressMarker(
            record_id=f"course:{cid_raw}", fingerprint=fingerprint,
        )

        for page in self._pages(
            f"{base}/api/v1/courses/{cid_raw}/assignments"
            f"?per_page={self.per_page}"
        ):
            for assignment in page or []:
                aid = str(assignment.get("id"))
                deadline_id = f"deadline:canvas:{aid}"
                a_fp = _sha(repr(assignment))
                yield NodeFact(
                    node_id=deadline_id,
                    subtype="deadline",
                    attrs={
                        "deadline_id": deadline_id,
                        "title": assignment.get("name") or "",
                        "kind": "teaching",
                        "due": assignment.get("due_at") or "",
                        "status": (
                            "pending"
                            if not assignment.get("locked_for_user")
                            else "closed"
                        ),
                        "subject_ref": course_id,
                        "external_ref": aid,
                    },
                    source_record_id={"assignment_id": aid},
                    source_revision={"content_sha": a_fp},
                )
                yield ProgressMarker(
                    record_id=f"assignment:{aid}", fingerprint=a_fp,
                )

        for page in self._pages(
            f"{base}/api/v1/courses/{cid_raw}/discussion_topics"
            f"?only_announcements=true&per_page={self.per_page}"
        ):
            for ann in page or []:
                ann_id = str(ann.get("id"))
                doc_id = f"doc:canvas:{ann_id}"
                message, body_attrs = self.redactor.redact_body(
                    ann.get("message") or "",
                )
                ann_fp = _sha(repr(ann))
                yield NodeFact(
                    node_id=doc_id,
                    subtype="document",
                    attrs={
                        "doc_id": doc_id,
                        "title": ann.get("title") or "",
                        "text": message,
                        "format": "canvas_announcement",
                        "course_ref": course_id,
                        **body_attrs,
                    },
                    source_record_id={"announcement_id": ann_id},
                    source_revision={"content_sha": ann_fp},
                )
                yield ProgressMarker(
                    record_id=f"announcement:{ann_id}",
                    fingerprint=ann_fp,
                )


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
