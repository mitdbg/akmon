"""add-msgraph-sources: delta-page walking, allowlist refusal,
deltaLink resumption, stale-token restart, preflight — all against
canned Graph pages (no credentials)."""
from __future__ import annotations

import logging
import urllib.error
from typing import Any

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.outlook_api import (
    OutlookCalendarApiSource,
    OutlookMailApiSource,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

_API = "https://graph.microsoft.com/v1.0"


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
                return value
        raise AssertionError(f"unexpected URL in test: {url}")


def _mail_item(
    mid: str,
    *,
    conv: str,
    subject: str,
    body: str = "hi",
    cc: list[dict[str, Any]] | None = None,
) -> dict:
    return {
        "id": mid,
        "conversationId": conv,
        "subject": subject,
        "receivedDateTime": "2026-06-02T14:00:00Z",
        "bodyPreview": body[:60],
        "body": {"contentType": "text", "content": body},
        "from": {"emailAddress": {"name": "Alice", "address": "alice@example.edu"}},
        "toRecipients": [
            {"emailAddress": {"name": "Bob", "address": "bob@example.edu"}},
        ],
        "ccRecipients": cc or [],
    }


def _event_item(eid: str, *, subject: str, preview: str = "") -> dict:
    return {
        "id": eid,
        "subject": subject,
        "bodyPreview": preview,
        "start": {"dateTime": "2026-06-03T10:00:00", "timeZone": "UTC"},
        "end": {"dateTime": "2026-06-03T11:00:00", "timeZone": "UTC"},
        "location": {"displayName": "32-G882"},
    }


_FOLDERS_PAGE = {
    "value": [
        {"id": "fid-inbox", "displayName": "Inbox"},
        {"id": "fid-grants", "displayName": "Grants"},
    ],
}

_CALENDARS_PAGE = {
    "value": [
        {"id": "cal-1", "name": "Calendar"},
        {"id": "cal-2", "name": "Teaching"},
    ],
}


def _mail_source(stub: StubRefreshHandler, **kw: Any) -> OutlookMailApiSource:
    return OutlookMailApiSource(
        deployment_name="test",
        outlook_folders=["Grants"],
        refresh_handler=stub,  # type: ignore[arg-type]
        **kw,
    )


def _cal_source(stub: StubRefreshHandler, **kw: Any) -> OutlookCalendarApiSource:
    return OutlookCalendarApiSource(
        deployment_name="test",
        outlook_calendars=["Teaching"],
        refresh_handler=stub,  # type: ignore[arg-type]
        window_start="2026-01-01T00:00:00Z",
        window_end="2026-12-31T00:00:00Z",
        **kw,
    )


def _live_node_attrs(dsn: str, node_id: str) -> dict[str, Any]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        latest = store.list_generations(
            deployment_name="test",
            status="published",
        )[-1]
        graph = store.checkout_generation(latest.checkpoint_id)
        rows = graph.query(
            """
            SELECT attrs
              FROM okg.graph_nodes
             WHERE node_id = :node_id
            """,
            {"node_id": node_id},
        )
        assert rows, node_id
        return dict(rows[0]["attrs"] or {})
    finally:
        store.close()


def test_delta_pages_project_mail_and_events() -> None:
    """Mail: a 2-page delta walk (nextLink chain ending in a
    deltaLink) emits emails + body documents + one aggregated
    thread, and the closing deltaLink lands in next_cursor.
    Calendar: allowlisted events emit; non-matching events drop."""
    mail_stub = StubRefreshHandler({
        f"{_API}/me/mailFolders?": _FOLDERS_PAGE,
        f"{_API}/me/mailFolders/fid-grants/messages/delta?": {
            "value": [_mail_item("m1", conv="c1", subject="NSF budget")],
            "@odata.nextLink": f"{_API}/page2",
        },
        f"{_API}/page2": {
            "value": [_mail_item("m2", conv="c1", subject="Re: NSF budget")],
            "@odata.deltaLink": f"{_API}/delta-final-mail",
        },
    })
    run = _mail_source(mail_stub).run("run-1")
    facts = list(run.facts)
    emails = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "email"]
    docs = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "document"]
    threads = [
        f for f in facts if isinstance(f, NodeFact) and f.subtype == "email_thread"
    ]
    assert {e.node_id for e in emails} == {
        "email:outlook:m1", "email:outlook:m2",
    }
    assert len(docs) == 2
    assert [t.node_id for t in threads] == ["thread:outlook:c1"]
    assert threads[0].attrs["message_count"] == 2
    assert threads[0].attrs["participants_domains"] == ["example.edu"]
    assert emails[0].attrs["labels"] == ["Grants"]
    # The deltaLink captured during the walk is visible on the
    # SourceRun the runner already holds (mutable cursor contract).
    assert run.next_cursor == {
        "mail_deltalinks": {"Grants": f"{_API}/delta-final-mail"},
    }
    assert run.completed_scope is True

    cal_stub = StubRefreshHandler({
        f"{_API}/me/calendars?": _CALENDARS_PAGE,
        f"{_API}/me/calendars/cal-2/calendarView/delta?": {
            "value": [
                _event_item("e1", subject="Office hours"),
                _event_item("e2", subject="Dentist"),
            ],
            "@odata.deltaLink": f"{_API}/delta-final-cal",
        },
    })
    cal_run = _cal_source(cal_stub).run("run-2")
    cal_facts = list(cal_run.facts)
    events = [
        f for f in cal_facts
        if isinstance(f, NodeFact) and f.subtype == "calendar_event"
    ]
    feeds = [
        f for f in cal_facts
        if isinstance(f, NodeFact) and f.subtype == "calendar_feed"
    ]
    edges = [f for f in cal_facts if isinstance(f, EdgeFact)]
    assert [e.node_id for e in events] == ["event:outlook:e1"]
    assert events[0].attrs["kind"] == "office_hours"
    assert [f.node_id for f in feeds] == ["feed:outlook:cal-2"]
    assert {(e.src, e.dst) for e in edges} == {
        ("event:outlook:e1", "feed:outlook:cal-2"),
    }
    assert cal_run.next_cursor == {
        "calendar_deltalinks": {"Teaching": f"{_API}/delta-final-cal"},
    }


def test_unconfigured_allowlists_refuse_loudly(caplog) -> None:
    with caplog.at_level(logging.WARNING):
        mail_run = OutlookMailApiSource(deployment_name="test").run("r")
        cal_run = OutlookCalendarApiSource(deployment_name="test").run("r")
        assert list(mail_run.facts) == []
        assert list(cal_run.facts) == []
    messages = [r.message for r in caplog.records]
    assert any("outlook_folders" in m for m in messages)
    assert any("outlook_calendars" in m for m in messages)


def test_unknown_allowlisted_folder_warns(caplog) -> None:
    stub = StubRefreshHandler({
        f"{_API}/me/mailFolders?": _FOLDERS_PAGE,
    })
    src = OutlookMailApiSource(
        deployment_name="test",
        outlook_folders=["Nonexistent"],
        refresh_handler=stub,  # type: ignore[arg-type]
    )
    with caplog.at_level(logging.WARNING):
        run = src.run("r")
        assert list(run.facts) == []
    assert any("Nonexistent" in r.message for r in caplog.records)


def test_deltalink_resume_fetches_only_delta() -> None:
    """With a stored deltaLink, the walk starts AT the deltaLink —
    no fresh full-walk URL is ever called — and the run does not
    claim full-scope authority."""
    delta_url = f"{_API}/stored-deltalink-grants"
    stub = StubRefreshHandler({
        f"{_API}/me/mailFolders?": _FOLDERS_PAGE,
        delta_url: {
            "value": [_mail_item("m9", conv="c9", subject="New one")],
            "@odata.deltaLink": f"{_API}/delta-next",
        },
    })
    run = _mail_source(stub).run(
        "run-3", cursor={"mail_deltalinks": {"Grants": delta_url}},
    )
    facts = list(run.facts)
    emails = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "email"]
    assert [e.node_id for e in emails] == ["email:outlook:m9"]
    assert run.completed_scope is False
    assert run.next_cursor == {
        "mail_deltalinks": {"Grants": f"{_API}/delta-next"},
    }
    assert not any("/messages/delta?" in c for c in stub.calls)
    # Folder resolution + one delta page: work bounded by the delta.
    assert len(stub.calls) == 2


def test_stale_delta_token_restarts_loudly(caplog) -> None:
    """410 Gone on the stored deltaLink → loud warning + full
    delta restart, never a silent empty delta."""
    stale_url = f"{_API}/stale-deltalink"
    gone = urllib.error.HTTPError(
        url=stale_url, code=410, msg="Gone", hdrs=None, fp=None,
    )
    stub = StubRefreshHandler({
        f"{_API}/me/mailFolders?": _FOLDERS_PAGE,
        stale_url: gone,
        f"{_API}/me/mailFolders/fid-grants/messages/delta?": {
            "value": [_mail_item("m1", conv="c1", subject="Backfilled")],
            "@odata.deltaLink": f"{_API}/delta-after-restart",
        },
    })
    with caplog.at_level(logging.WARNING):
        run = _mail_source(stub).run(
            "run-4", cursor={"mail_deltalinks": {"Grants": stale_url}},
        )
        facts = list(run.facts)
    emails = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "email"]
    assert [e.node_id for e in emails] == ["email:outlook:m1"]
    assert any("stale" in r.message for r in caplog.records)
    assert run.next_cursor == {
        "mail_deltalinks": {"Grants": f"{_API}/delta-after-restart"},
    }


def test_non_410_delta_error_propagates() -> None:
    boom = urllib.error.HTTPError(
        url=f"{_API}/x", code=500, msg="Server Error", hdrs=None, fp=None,
    )
    stub = StubRefreshHandler({
        f"{_API}/me/mailFolders?": _FOLDERS_PAGE,
        f"{_API}/me/mailFolders/fid-grants/messages/delta?": boom,
    })
    run = _mail_source(stub).run("run-5")
    with pytest.raises(urllib.error.HTTPError):
        list(run.facts)


def test_preflight_reports_token_state(monkeypatch) -> None:
    """Env vars absent → missing_credential with the env var names
    in the actionable reason (doctor --check source-auth surface)."""
    monkeypatch.delenv("OKG_MSGRAPH_CLIENT_ID", raising=False)
    monkeypatch.delenv("OKG_MSGRAPH_CLIENT_SECRET", raising=False)
    src = OutlookMailApiSource(
        deployment_name="test", outlook_folders=["Inbox"],
    )
    result = src.preflight()
    assert result.status == "missing_credential"
    assert "OKG_MSGRAPH_CLIENT_ID" in result.reason


def test_pii_hashing_on_mail() -> None:
    stub = StubRefreshHandler({
        f"{_API}/me/mailFolders?": _FOLDERS_PAGE,
        f"{_API}/me/mailFolders/fid-grants/messages/delta?": {
            "value": [_mail_item(
                "m1",
                conv="c1",
                subject="S",
                cc=[{
                    "emailAddress": {
                        "name": "Carol",
                        "address": "carol@lab.example.edu",
                    },
                }],
            )],
            "@odata.deltaLink": f"{_API}/dl",
        },
    })
    run = _mail_source(stub, pii_mode="hashed").run("run-6")
    email = next(
        f for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "email"
    )
    assert email.attrs["sender_ref"].startswith("hash:")
    assert all(
        r.startswith("hash:") and r.endswith("@example.edu")
        for r in email.attrs["recipients_refs"]
    )
    assert email.attrs["sender_domain"] == "example.edu"
    assert email.attrs["recipients_domains"] == ["example.edu"]
    assert email.attrs["cc_domains"] == ["lab.example.edu"]


@pytest.mark.postgres
def test_backfill_then_delta_publishes_row_deltas(clean_runtime: str) -> None:
    """graph_report proof through the real runner: backfill publishes
    email nodes_live=2, then the persisted deltaLink flows back and
    a 1-message delta publishes exactly +1."""
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.publisher import Publisher
    from tests.substrate.library.sources.test_ci_source_sync_db import (
        _live_node_count,
        _seed_catalog_and_external_nodes,
    )

    def _backfill_stub() -> StubRefreshHandler:
        return StubRefreshHandler({
            f"{_API}/me/mailFolders?": _FOLDERS_PAGE,
            f"{_API}/me/mailFolders/fid-grants/messages/delta?": {
                "value": [
                    _mail_item("m1", conv="c1", subject="A"),
                    _mail_item("m2", conv="c1", subject="Re: A"),
                ],
                "@odata.deltaLink": f"{_API}/stored-dl",
            },
        })

    _seed_catalog_and_external_nodes(
        clean_runtime, _mail_source(_backfill_stub()).run("preview").facts,
    )
    runner = SourceRunner(clean_runtime, source_name="test.outlook-mail")
    runner.run(_mail_source(_backfill_stub()), mode="scope_complete")
    publish1 = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert publish1["status"] == "published", publish1
    assert _live_node_count(clean_runtime, "email") == 2
    assert _live_node_count(clean_runtime, "email_thread") == 1
    assert _live_node_attrs(clean_runtime, "email:outlook:m1")[
        "sender_domain"
    ] == "example.edu"
    assert _live_node_attrs(clean_runtime, "thread:outlook:c1")[
        "participants_domains"
    ] == ["example.edu"]

    delta_stub = StubRefreshHandler({
        f"{_API}/me/mailFolders?": _FOLDERS_PAGE,
        f"{_API}/stored-dl": {
            "value": [_mail_item("m3", conv="c1", subject="Re: A (new)")],
            "@odata.deltaLink": f"{_API}/next-dl",
        },
    })
    runner.run(_mail_source(delta_stub), mode="cursor")
    # The persisted deltaLink flowed back through the runner.
    assert any("stored-dl" in c for c in delta_stub.calls)
    assert not any("/messages/delta?" in c for c in delta_stub.calls)
    publish2 = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert publish2["status"] == "published", publish2
    assert _live_node_count(clean_runtime, "email") == 3
