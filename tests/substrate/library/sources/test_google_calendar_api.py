"""cover-calendar-sources: google_calendar_api — singleEvents +
window in the request, pagination, loud allowlist refusal, kind
inference, and the publish proof."""
from __future__ import annotations

import logging
from typing import Any

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.google_calendar_api import (
    GoogleCalendarApiSource,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

_API = "https://www.googleapis.com/calendar/v3"
_WINDOW = {
    "window_start": "2026-01-01T00:00:00Z",
    "window_end": "2026-12-31T00:00:00Z",
}


def _event(eid: str, *, summary: str, start: str = "2026-06-11T14:00:00Z") -> dict:
    return {
        "id": eid,
        "summary": summary,
        "start": {"dateTime": start},
        "end": {"dateTime": start.replace("T14", "T15")},
        "location": "32-G882",
    }


class StubRefreshHandler:
    def __init__(self, pages: dict[str, Any]) -> None:
        self.pages = pages
        self.calls: list[str] = []

    def authed_request(self, url: str, **_kwargs: Any) -> Any:
        self.calls.append(url)
        for key, value in self.pages.items():
            if url == key or url.startswith(key):
                if isinstance(value, Exception):
                    raise value
                if callable(value):
                    return value(url)
                return value
        raise AssertionError(f"unexpected URL in test: {url}")


def _source(stub: StubRefreshHandler, **kw: Any) -> GoogleCalendarApiSource:
    return GoogleCalendarApiSource(
        deployment_name="test",
        google_calendar_feeds=["prof@example.edu"],
        refresh_handler=stub,  # type: ignore[arg-type]
        **{**_WINDOW, **kw},
    )


def _paged_stub() -> StubRefreshHandler:
    def route(url: str) -> dict:
        if "pageToken=page2" in url:
            return {"items": [
                _event("e3", summary="okg: thesis committee"),
                _event("e4", summary="Dentist"),
            ]}
        return {
            "items": [
                _event("e1", summary="Lab meeting"),
                _event("e2", summary="Office hours"),
            ],
            "nextPageToken": "page2",
        }
    return StubRefreshHandler({
        f"{_API}/calendars/prof%40example.edu/events?": route,
    })


def test_paginated_single_events_project() -> None:
    """Two pages chained by nextPageToken; the request carries
    singleEvents=true + the explicit window (the unexpanded-master
    bug pre-fix); allowlist drops the dentist; kinds inferred."""
    stub = _paged_stub()
    run = _source(stub).run("run-1")
    facts = list(run.facts)
    events = [
        f for f in facts if isinstance(f, NodeFact) and f.subtype == "calendar_event"
    ]
    feeds = [
        f for f in facts if isinstance(f, NodeFact) and f.subtype == "calendar_feed"
    ]
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    assert {e.node_id for e in events} == {
        "event:gcal:e1", "event:gcal:e2", "event:gcal:e3",
    }
    kinds = {e.node_id: e.attrs["kind"] for e in events}
    assert kinds["event:gcal:e1"] == "lab_meeting"
    assert kinds["event:gcal:e2"] == "office_hours"
    assert kinds["event:gcal:e3"] == "committee"
    assert len(feeds) == 1
    assert all(e.dst == feeds[0].node_id for e in edges)

    assert len(stub.calls) == 2, "pagination not walked"
    for url in stub.calls:
        assert "singleEvents=true" in url
        assert "timeMin=2026-01-01T00:00:00Z" in url
        assert "timeMax=2026-12-31T00:00:00Z" in url
    assert "pageToken=page2" in stub.calls[1]


def test_unconfigured_feeds_refuse_loudly(caplog) -> None:
    src = GoogleCalendarApiSource(deployment_name="test")
    with caplog.at_level(logging.WARNING):
        run = src.run("run-2")
        assert list(run.facts) == []
    assert any(
        "google_calendar_feeds" in r.message for r in caplog.records
    ), "pre-fix this was a silent empty run"


def test_one_calendar_failure_does_not_block_others(caplog) -> None:
    boom = RuntimeError("quota exceeded")
    pages: dict[str, Any] = {
        f"{_API}/calendars/bad%40example.edu/events?": boom,
        f"{_API}/calendars/good%40example.edu/events?": {
            "items": [_event("e9", summary="Group meeting")],
        },
    }
    stub = StubRefreshHandler(pages)
    src = GoogleCalendarApiSource(
        deployment_name="test",
        google_calendar_feeds=["bad@example.edu", "good@example.edu"],
        refresh_handler=stub,  # type: ignore[arg-type]
        **_WINDOW,
    )
    with caplog.at_level(logging.ERROR):
        facts = list(src.run("run-3").facts)
    events = [
        f for f in facts if isinstance(f, NodeFact) and f.subtype == "calendar_event"
    ]
    assert [e.node_id for e in events] == ["event:gcal:e9"]
    assert any("bad@example.edu" in r.message for r in caplog.records)


@pytest.mark.postgres
def test_backfill_publishes_row_deltas(clean_runtime: str) -> None:
    """graph_report proof: SourceRunner -> Publisher on the
    ephemeral test DB; calendar rows land under a published
    generation."""
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.publisher import Publisher
    from tests.substrate.library.sources.test_ci_source_sync_db import (
        _live_node_count,
        _seed_catalog_and_external_nodes,
    )

    _seed_catalog_and_external_nodes(
        clean_runtime, _source(_paged_stub()).run("preview").facts,
    )
    runner = SourceRunner(clean_runtime, source_name="test.gcal")
    runner.run(_source(_paged_stub()), mode="scope_complete")
    publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert publish["status"] == "published", publish
    assert _live_node_count(clean_runtime, "calendar_event") == 3
    assert _live_node_count(clean_runtime, "calendar_feed") == 1
