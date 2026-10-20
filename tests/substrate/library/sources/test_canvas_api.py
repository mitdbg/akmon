"""add-academic-corpora-sources: canvas_api — courses, assignments,
announcements via canned REST pages with Link-header pagination;
allowlist refusal mirrors the gmail-label contract."""
from __future__ import annotations

import ipaddress
import logging
from typing import Any
from unittest import mock

import pytest

from okg.substrate.library.sources._url_safety import UnsafeFetchURL
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.canvas_api import (
    CanvasApiSource,
    _next_link,
)

_BASE = "https://canvas.test.edu"


@pytest.fixture(autouse=True)
def _hermetic_dns():
    """Resolve named hosts (canvas.test.edu) to a public IP so the egress
    validator's same-origin/public-IP check passes hermetically, while leaving
    IP literals (169.254.169.254, 10.x) to resolve to themselves so the SSRF
    guard still rejects them."""
    def fake_getaddrinfo(host, port, *a, **k):
        try:
            ipaddress.ip_address(host)
            return [(2, 1, 6, "", (host, port or 0))]
        except ValueError:
            return [(2, 1, 6, "", ("93.184.216.34", port or 0))]

    with mock.patch(
        "okg.substrate.library.sources._url_safety.socket.getaddrinfo",
        side_effect=fake_getaddrinfo,
    ):
        yield


def _course(cid: int, code: str, name: str) -> dict[str, Any]:
    return {
        "id": cid,
        "course_code": code,
        "name": name,
        "term": {"name": "2026 Fall"},
    }


class StubHttp:
    """Canned (json, link_header) per URL prefix, with a call log."""

    def __init__(self, pages: dict[str, tuple[Any, str]]) -> None:
        self.pages = pages
        self.calls: list[str] = []

    def __call__(self, url: str, token: str) -> tuple[Any, str]:
        assert token == "tok-123", "token not threaded through"
        self.calls.append(url)
        for key, value in self.pages.items():
            if url == key or url.startswith(key):
                return value
        raise AssertionError(f"unexpected URL in test: {url}")


def _source(stub: StubHttp, **kw: Any) -> CanvasApiSource:
    return CanvasApiSource(
        deployment_name="test",
        canvas_base_url=_BASE,
        canvas_courses=["6.5830"],
        canvas_token="tok-123",
        http_get=stub,
        **kw,
    )


def test_next_link_parses_rfc5988() -> None:
    header = (
        f'<{_BASE}/api/v1/courses?page=2>; rel="next", '
        f'<{_BASE}/api/v1/courses?page=9>; rel="last"'
    )
    assert _next_link(header) == f"{_BASE}/api/v1/courses?page=2"
    assert _next_link('<x>; rel="last"') is None
    assert _next_link("") is None


def test_courses_assignments_announcements_project() -> None:
    """Two course-list pages chained by Link header; only the
    allowlisted course's assignments + announcements are fetched
    and projected."""
    stub = StubHttp({
        f"{_BASE}/api/v1/courses?enrollment_type=teacher": (
            [_course(101, "6.5830", "Database Systems")],
            f'<{_BASE}/api/v1/courses-page2>; rel="next"',
        ),
        f"{_BASE}/api/v1/courses-page2": (
            [_course(202, "6.1040", "Software Design")],
            "",
        ),
        f"{_BASE}/api/v1/courses/101/assignments": (
            [
                {"id": 9001, "name": "Lab 1", "due_at": "2026-09-20T23:59:00Z"},
                {"id": 9002, "name": "Lab 2", "due_at": "2026-10-04T23:59:00Z"},
            ],
            "",
        ),
        f"{_BASE}/api/v1/courses/101/discussion_topics": (
            [{
                "id": 7001,
                "title": "Welcome",
                "message": "<p>Welcome to 6.5830</p>",
            }],
            "",
        ),
    })
    run = _source(stub).run("run-1")
    facts = list(run.facts)
    courses = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "course"]
    deadlines = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "deadline"]
    docs = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "document"]

    assert [c.node_id for c in courses] == ["course:canvas:101"]
    assert courses[0].attrs["code"] == "6.5830"
    assert courses[0].attrs["term"] == "2026 Fall"
    assert courses[0].attrs["role"] == "instructor"
    assert {d.node_id for d in deadlines} == {
        "deadline:canvas:9001", "deadline:canvas:9002",
    }
    assert all(d.attrs["subject_ref"] == "course:canvas:101" for d in deadlines)
    assert all(d.attrs["kind"] == "teaching" for d in deadlines)
    assert [d.node_id for d in docs] == ["doc:canvas:7001"]
    assert docs[0].attrs["course_ref"] == "course:canvas:101"
    # The non-allowlisted course (202) was never expanded.
    assert not any("/courses/202/" in c for c in stub.calls)
    # Both course-list pages were walked (Link header).
    assert any("courses-page2" in c for c in stub.calls)


def test_unconfigured_course_allowlist_refuses_loudly(caplog) -> None:
    src = CanvasApiSource(
        deployment_name="test",
        canvas_base_url=_BASE,
        canvas_token="tok-123",
    )
    with caplog.at_level(logging.WARNING):
        run = src.run("run-2")
        assert list(run.facts) == []
    assert any("canvas_courses" in r.message for r in caplog.records)


def test_unknown_allowlisted_course_warns(caplog) -> None:
    stub = StubHttp({
        f"{_BASE}/api/v1/courses?enrollment_type=teacher": (
            [_course(101, "6.5830", "Database Systems")],
            "",
        ),
        f"{_BASE}/api/v1/courses/101/assignments": ([], ""),
        f"{_BASE}/api/v1/courses/101/discussion_topics": ([], ""),
    })
    src = CanvasApiSource(
        deployment_name="test",
        canvas_base_url=_BASE,
        canvas_courses=["6.5830", "8.962"],
        canvas_token="tok-123",
        http_get=stub,
    )
    with caplog.at_level(logging.WARNING):
        list(src.run("run-3").facts)
    assert any("8.962" in r.message for r in caplog.records)


def test_missing_token_refuses_loudly(caplog, monkeypatch) -> None:
    monkeypatch.delenv("OKG_CANVAS_TOKEN", raising=False)
    src = CanvasApiSource(
        deployment_name="test",
        canvas_base_url=_BASE,
        canvas_courses=["6.5830"],
    )
    with caplog.at_level(logging.WARNING):
        run = src.run("run-4")
        assert list(run.facts) == []
    assert any("token" in r.message for r in caplog.records)
    preflight = src.preflight()
    assert preflight.status == "missing_credential"
    assert "OKG_CANVAS_TOKEN" in preflight.reason


class _StubStore:
    def __init__(self, tokens=None) -> None:
        self._tokens = tokens

    def get(self, *, deployment, provider):
        return self._tokens


def test_token_prefers_keychain_and_preflight_reports_custody(
    monkeypatch,
) -> None:
    """With a token in the (stubbed) keychain, the source reads it —
    not the env var — and preflight reports custody: keychain."""
    monkeypatch.setenv("OKG_CANVAS_TOKEN", "env-should-not-win")
    src = CanvasApiSource(
        deployment_name="test",
        canvas_base_url=_BASE,
        canvas_courses=["6.5830"],
        token_store=_StubStore({"access_token": "keychain-tok"}),
    )
    assert src._token() == "keychain-tok"
    preflight = src.preflight()
    assert preflight.status == "ok"
    assert "custody: keychain" in preflight.reason


def test_token_env_fallback_when_keychain_empty(monkeypatch) -> None:
    monkeypatch.setenv("OKG_CANVAS_TOKEN", "env-tok")
    src = CanvasApiSource(
        deployment_name="test",
        canvas_base_url=_BASE,
        canvas_courses=["6.5830"],
        token_store=_StubStore(None),
    )
    assert src._token() == "env-tok"
    assert "custody: env" in src.preflight().reason


def test_pagination_link_to_metadata_host_is_refused() -> None:
    """A response-provided `Link: rel=next` pointing at the cloud-metadata IP
    is refused before the next credentialed hop (SSRF via pagination)."""
    stub = StubHttp({
        f"{_BASE}/api/v1/courses?enrollment_type=teacher": (
            [_course(101, "6.5830", "DB")],
            '<http://169.254.169.254/api/v1/courses-page2>; rel="next"',
        ),
    })
    with pytest.raises(UnsafeFetchURL):
        list(_source(stub).run("run-ssrf").facts)
    # The malicious next hop was never fetched.
    assert not any("169.254.169.254" in c for c in stub.calls)


def test_same_origin_pagination_is_followed() -> None:
    """A same-origin (Canvas host) next page still paginates and yields rows."""
    stub = StubHttp({
        f"{_BASE}/api/v1/courses?enrollment_type=teacher": (
            [_course(101, "6.5830", "DB")],
            f'<{_BASE}/api/v1/courses-page2>; rel="next"',
        ),
        f"{_BASE}/api/v1/courses-page2": ([], ""),
        f"{_BASE}/api/v1/courses/101/assignments": ([], ""),
        f"{_BASE}/api/v1/courses/101/discussion_topics": ([], ""),
    })
    run = _source(stub).run("run-ok")
    courses = [
        f for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "course"
    ]
    assert [c.node_id for c in courses] == ["course:canvas:101"]
    assert any("courses-page2" in c for c in stub.calls)


def test_announcement_body_redaction() -> None:
    big = "x" * (2 * 1024)
    stub = StubHttp({
        f"{_BASE}/api/v1/courses?enrollment_type=teacher": (
            [_course(101, "6.5830", "DB")],
            "",
        ),
        f"{_BASE}/api/v1/courses/101/assignments": ([], ""),
        f"{_BASE}/api/v1/courses/101/discussion_topics": (
            [{"id": 7002, "title": "Long", "message": big}],
            "",
        ),
    })
    run = _source(stub, max_body_kb=1).run("run-5")
    doc = next(
        f for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "document"
    )
    assert doc.attrs["truncated"] is True
    assert len(doc.attrs["text"].encode("utf-8")) <= 1024
