"""finish-gmail-source: backfill, incremental cursor, redaction,
EmailThread aggregation — all against a canned-page stub handler
(no credentials; the live path is env-gated in
test_gmail_api_live.py)."""
from __future__ import annotations

import base64
import logging
import urllib.error
from typing import Any

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.gmail_api import GmailApiSource
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


def _b64url(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode("ascii").rstrip("=")


def _message(
    mid: str,
    *,
    thread: str,
    subject: str,
    sender: str = "Alice Adams <alice@example.edu>",
    to: str = "Bob <bob@example.edu>",
    cc: str = "",
    date: str = "Tue, 02 Jun 2026 10:00:00 -0400",
    labels: list[str] | None = None,
    body: str = "hello world",
) -> dict[str, Any]:
    headers = [
        {"name": "Subject", "value": subject},
        {"name": "From", "value": sender},
        {"name": "To", "value": to},
        {"name": "Date", "value": date},
    ]
    if cc:
        headers.append({"name": "Cc", "value": cc})
    return {
        "id": mid,
        "threadId": thread,
        "labelIds": labels if labels is not None else ["Label_okg"],
        "snippet": body[:60],
        "payload": {
            "mimeType": "multipart/alternative",
            "headers": headers,
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {"data": _b64url(body)},
                },
            ],
        },
    }


class StubRefreshHandler:
    """Canned URL -> response map + full call log.

    The call log is the proportionality instrument: tests assert
    which endpoints were hit, not just what facts came out.
    """

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


_API = "https://gmail.googleapis.com/gmail/v1/users/me"


def _backfill_stub(
    *,
    messages: list[dict[str, Any]],
    history_id: str = "9999",
    page_split: int | None = None,
) -> StubRefreshHandler:
    pages: dict[str, Any] = {f"{_API}/profile": {"historyId": history_id}}
    refs = [{"id": m["id"]} for m in messages]
    if page_split is None:
        pages[f"{_API}/messages?q="] = {"messages": refs}
    else:
        first, rest = refs[:page_split], refs[page_split:]
        pages[f"{_API}/messages?q="] = {
            "messages": first, "nextPageToken": "page2",
        }
    for m in messages:
        pages[f"{_API}/messages/{m['id']}?format=full"] = m
    stub = StubRefreshHandler(pages)
    if page_split is not None:
        base = stub.authed_request

        def routed(url: str, **kw: Any) -> Any:
            if "pageToken=page2" in url:
                stub.calls.append(url)
                return {"messages": rest}
            return base(url, **kw)

        stub.authed_request = routed  # type: ignore[method-assign]
    return stub


def _source(stub: StubRefreshHandler, **kwargs: Any) -> GmailApiSource:
    return GmailApiSource(
        deployment_name="test",
        gmail_labels=["Label_okg"],
        refresh_handler=stub,  # type: ignore[arg-type]
        **kwargs,
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


def test_backfill_projects_allowlisted_labels() -> None:
    """Paginated backfill: 3 messages across 2 list pages, one of
    them lacking the allowlisted label (defense-in-depth drop).
    Emits 2 email nodes + body documents + 1 aggregated thread,
    and the run's next_cursor anchors to the profile historyId."""
    msgs = [
        _message("m1", thread="t1", subject="Quals scheduling"),
        _message("m2", thread="t1", subject="Re: Quals scheduling",
                 sender="Bob <bob@example.edu>",
                 date="Tue, 02 Jun 2026 11:00:00 -0400"),
        _message("m3", thread="t2", subject="Newsletter", labels=["INBOX"]),
    ]
    stub = _backfill_stub(messages=msgs, page_split=2)
    run = _source(stub).run("run-1")
    facts = list(run.facts)
    emails = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "email"]
    docs = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "document"]
    threads = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "email_thread"]
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    assert {e.node_id for e in emails} == {"email:gmail:m1", "email:gmail:m2"}
    assert {d.node_id for d in docs} == {"doc:gmail:m1", "doc:gmail:m2"}
    assert [t.node_id for t in threads] == ["thread:gmail:t1"]
    thread = threads[0]
    assert thread.attrs["message_count"] == 2
    assert thread.attrs["subject"] == "Re: Quals scheduling"
    assert thread.attrs["first_date"] < thread.attrs["latest_date"]
    assert {(e.src, e.dst) for e in edges} == {
        ("thread:gmail:t1", "email:gmail:m1"),
        ("thread:gmail:t1", "email:gmail:m2"),
    }
    # Email rows point at their thread + body document.
    assert all(e.attrs["thread_ref"] == "thread:gmail:t1" for e in emails)
    assert all(e.attrs["body_doc_ref"] for e in emails)
    assert threads[0].attrs["participants_domains"] == ["example.edu"]
    # Dates normalized to ISO.
    assert emails[0].attrs["date"].startswith("2026-06-02T")
    assert run.next_cursor == {"gmail_history_id": "9999"}
    # Both list pages were walked.
    assert sum("messages?q=" in c for c in stub.calls) == 2


def test_no_labels_refuses_with_loud_warning(caplog) -> None:
    src = GmailApiSource(deployment_name="test", gmail_labels=[])
    with caplog.at_level(logging.WARNING):
        run = src.run("run-2")
        facts = list(run.facts)
    assert facts == []
    assert any("gmail_labels" in r.message for r in caplog.records)


def test_bodies_truncated_and_pii_hashed() -> None:
    big_body = "x" * (2 * 1024)  # 2KB body vs 1KB cap below
    msgs = [_message(
        "m1",
        thread="t1",
        subject="Big",
        body=big_body,
        cc="Carol <carol@lab.example>",
    )]
    stub = _backfill_stub(messages=msgs)
    run = _source(stub, max_body_kb=1, pii_mode="hashed").run("run-3")
    facts = list(run.facts)
    doc = next(f for f in facts if isinstance(f, NodeFact) and f.subtype == "document")
    email = next(f for f in facts if isinstance(f, NodeFact) and f.subtype == "email")
    assert doc.attrs["truncated"] is True
    assert doc.attrs["original_byte_count"] == 2 * 1024
    assert len(doc.attrs["text"].encode("utf-8")) <= 1024
    # PII hashing: display name hashed, recipient local-part hashed
    # with the domain kept.
    assert email.attrs["sender_ref"].startswith("hash:")
    assert all(
        r.startswith("hash:") and r.endswith("@example.edu")
        for r in email.attrs["recipients_refs"]
    )
    assert email.attrs["sender_domain"] == "example.edu"
    assert email.attrs["recipients_domains"] == ["example.edu"]
    assert email.attrs["cc_domains"] == ["lab.example"]


def test_history_cursor_fetches_only_delta() -> None:
    """With a stored historyId, the run walks history.list and
    fetches only the delta message — no messages.list full
    re-list call is ever issued."""
    delta_msg = _message("m9", thread="t9", subject="New since cursor")
    stub = StubRefreshHandler({
        f"{_API}/profile": {"historyId": "10100"},
        f"{_API}/history?startHistoryId=10000": {
            "history": [
                {"messagesAdded": [{"message": {"id": "m9"}}]},
                # Same id appearing again must not double-fetch.
                {"labelsAdded": [{"message": {"id": "m9"}}]},
            ],
        },
        f"{_API}/messages/m9?format=full": delta_msg,
    })
    run = _source(stub).run(
        "run-4", cursor={"gmail_history_id": "10000"},
    )
    facts = list(run.facts)
    emails = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "email"]
    assert [e.node_id for e in emails] == ["email:gmail:m9"]
    assert run.next_cursor == {"gmail_history_id": "10100"}
    assert not any("messages?q=" in c for c in stub.calls)
    # Proportionality at the call level: profile + 1 history page
    # + 1 message fetch — work bounded by the delta, not the inbox.
    assert len(stub.calls) == 3


def test_stale_history_id_falls_back_loudly(caplog) -> None:
    """history.list 404 on a stale cursor → loud warning + full
    backfill, never a silent empty delta."""
    msg = _message("m1", thread="t1", subject="Backfilled")
    stale = urllib.error.HTTPError(
        url=f"{_API}/history", code=404, msg="Not Found",
        hdrs=None, fp=None,
    )
    stub = StubRefreshHandler({
        f"{_API}/profile": {"historyId": "20000"},
        f"{_API}/history?startHistoryId=123": stale,
        f"{_API}/messages?q=": {"messages": [{"id": "m1"}]},
        f"{_API}/messages/m1?format=full": msg,
    })
    with caplog.at_level(logging.WARNING):
        run = _source(stub).run(
            "run-5", cursor={"gmail_history_id": "123"},
        )
        facts = list(run.facts)
    emails = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "email"]
    assert [e.node_id for e in emails] == ["email:gmail:m1"]
    assert any("stale" in r.message for r in caplog.records)
    assert any("messages?q=" in c for c in stub.calls)


def test_non_404_history_error_propagates() -> None:
    """A non-404 history failure is unrecoverable — it must raise,
    not silently degrade to backfill."""
    boom = urllib.error.HTTPError(
        url=f"{_API}/history", code=500, msg="Server Error",
        hdrs=None, fp=None,
    )
    stub = StubRefreshHandler({
        f"{_API}/profile": {"historyId": "30000"},
        f"{_API}/history?startHistoryId=42": boom,
    })
    with pytest.raises(urllib.error.HTTPError):
        _source(stub).run("run-6", cursor={"gmail_history_id": "42"})


def test_gmail_api_work_proportional_to_delta() -> None:
    """Two rounds: a 6-message backfill, then a 1-message delta.
    The second round's HTTP call count is bounded by the delta
    size (3 calls), not the mailbox size (8 calls in round one)."""
    msgs = [
        _message(f"m{i}", thread=f"t{i}", subject=f"Msg {i}")
        for i in range(6)
    ]
    backfill_stub = _backfill_stub(messages=msgs, history_id="500")
    first = _source(backfill_stub).run("round-1")
    list(first.facts)
    round_one_calls = len(backfill_stub.calls)
    assert round_one_calls == 1 + 1 + 6  # profile + list page + 6 fetches

    delta = _message("m99", thread="t99", subject="The only new one")
    delta_stub = StubRefreshHandler({
        f"{_API}/profile": {"historyId": "510"},
        f"{_API}/history?startHistoryId=500": {
            "history": [{"messagesAdded": [{"message": {"id": "m99"}}]}],
        },
        f"{_API}/messages/m99?format=full": delta,
    })
    second = _source(delta_stub).run("round-2", cursor=first.next_cursor)
    facts = list(second.facts)
    emails = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "email"]
    assert len(emails) == 1
    assert len(delta_stub.calls) == 3
    assert len(delta_stub.calls) < round_one_calls
    # Delta runs never claim full-scope (retraction) authority.
    assert second.completed_scope is False


@pytest.mark.postgres
def test_backfill_then_delta_publishes_row_deltas(clean_runtime: str) -> None:
    """The graph_report proof, end to end through the real runner:
    round 1 backfills 4 messages and publishes (email nodes_live=4,
    threads=2); round 2's source receives the PERSISTED cursor from
    the runner, walks only the history delta, and the next published
    generation shows exactly +1 email."""
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.publisher import Publisher
    from tests.substrate.library.sources.test_ci_source_sync_db import (
        _live_node_count,
        _seed_catalog_and_external_nodes,
    )

    msgs = [
        _message("m1", thread="t1", subject="A"),
        _message("m2", thread="t1", subject="Re: A"),
        _message("m3", thread="t2", subject="B"),
        _message("m4", thread="t2", subject="Re: B"),
    ]
    _seed_catalog_and_external_nodes(
        clean_runtime,
        _source(_backfill_stub(messages=msgs, history_id="500")).run("preview").facts,
    )

    runner = SourceRunner(clean_runtime, source_name="test.gmail-api")
    round1 = runner.run(
        _source(_backfill_stub(messages=msgs, history_id="500")),
        mode="scope_complete",
    )
    assert round1.completed_scope is True
    publish1 = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert publish1["status"] == "published", publish1
    assert _live_node_count(clean_runtime, "email") == 4
    assert _live_node_count(clean_runtime, "email_thread") == 2
    assert _live_node_attrs(clean_runtime, "email:gmail:m1")[
        "sender_domain"
    ] == "example.edu"
    assert _live_node_attrs(clean_runtime, "thread:gmail:t1")[
        "participants_domains"
    ] == ["example.edu"]

    delta = _message("m9", thread="t1", subject="Re: A (new)")
    delta_stub = StubRefreshHandler({
        f"{_API}/profile": {"historyId": "510"},
        f"{_API}/history?startHistoryId=500": {
            "history": [{"messagesAdded": [{"message": {"id": "m9"}}]}],
        },
        f"{_API}/messages/m9?format=full": delta,
    })
    runner.run(_source(delta_stub), mode="cursor")
    # The persisted cursor flowed back: the source asked history
    # for startHistoryId=500, the anchor stored by round 1.
    assert any("startHistoryId=500" in c for c in delta_stub.calls)
    assert not any("messages?q=" in c for c in delta_stub.calls)
    publish2 = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert publish2["status"] == "published", publish2
    assert _live_node_count(clean_runtime, "email") == 5
