"""Outbound Poros delivery + mailbox client egress validation
(centralize-egress-url-validation / enforce-mailbox-poros-authz).

deliver_outbox_item must validate the discovery/operator-provided
receiver_inbox_url through the shared egress validator BEFORE POSTing the signed
envelope: an unsafe (loopback/private/link-local/metadata/non-http) target is
recorded as a terminal failure with reason `unsafe_delivery_url` and never
requested. A real-network MailboxPollingClient likewise rejects an unsafe
base_url at construction.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import pytest

from okg.substrate.exchange import sender_delivery
from okg.substrate.exchange.sender_delivery import (
    PorosOutboxWorkItem,
    deliver_outbox_item,
)
from okg.substrate.federation.mailbox.client import MailboxPollingClient
from okg.substrate.library.sources._url_safety import UnsafeFetchURL

_ITEM = PorosOutboxWorkItem(
    message_id="m1",
    body_sha256="b",
    envelope_sha256="e",
    sender_authority_id="authority:sender",
    recipient_authority_id="authority:recipient",
    sender_generation_id="generation:g1",
    graph_id="graph:g1",
    classification="public",
    envelope={"hello": "world"},
    prior_attempt_count=0,
)

_RESOLUTION = {
    "status": "terminal_failed",
    "max_attempts": 3,
    "attempt_count": 1,
    "next_retry_at": None,
    "terminal_failure_reason": "x",
}


@pytest.fixture
def _no_db(monkeypatch):
    """Stub the two persistence calls so deliver_outbox_item needs no DB."""
    monkeypatch.setattr(
        sender_delivery, "record_delivery_attempt",
        lambda **k: SimpleNamespace(attempt_id=1),
    )
    monkeypatch.setattr(
        sender_delivery, "resolve_outbox_after_attempt",
        lambda **k: dict(_RESOLUTION),
    )


@pytest.mark.parametrize("url", [
    "http://127.0.0.1/inbox",
    "http://169.254.169.254/latest/meta-data",
    "http://10.0.0.5/inbox",
    "file:///etc/passwd",
])
def test_unsafe_receiver_url_refused_without_post(_no_db, monkeypatch, url):
    def _boom(*a, **k):
        raise AssertionError("requests.post must not be called for an unsafe URL")
    monkeypatch.setattr("requests.post", _boom)

    res = deliver_outbox_item(
        dsn="postgres://unused",
        item=_ITEM,
        receiver_inbox_url=url,
        receiver_public_key="pk",
    )
    assert res.reason_code == "unsafe_delivery_url"
    assert res.status == "failed"


def test_public_receiver_url_proceeds_to_post(_no_db, monkeypatch):
    called = {"post": False}

    def _fake_post(target, **kwargs):
        called["post"] = True
        called["allow_redirects"] = kwargs.get("allow_redirects")
        # No signed receipt -> the post path fails downstream, but the point is
        # the egress guard ALLOWED the public URL through to the request.
        return SimpleNamespace(
            status_code=200, headers={}, json=lambda: {"status": "failed"}
        )

    monkeypatch.setattr("requests.post", _fake_post)
    res = deliver_outbox_item(
        dsn="postgres://unused",
        item=_ITEM,
        receiver_inbox_url="https://93.184.216.34/inbox",  # literal public IP
        receiver_public_key="pk",
    )
    assert called["post"] is True
    # Delivery must not chase redirects (the SSRF guard only validated hop 1).
    assert called["allow_redirects"] is False
    assert res.reason_code != "unsafe_delivery_url"


def test_redirect_response_is_terminal_failure_not_followed(_no_db, monkeypatch):
    """A validated public receiver that returns a 3xx must NOT be followed (that
    would re-POST the signed envelope to a redirect-chosen origin). It is a
    terminal delivery failure."""
    def _fake_post(target, **kwargs):
        # 302 to the cloud-metadata endpoint; allow_redirects=False means requests
        # returns the 3xx as-is rather than following it.
        return SimpleNamespace(
            status_code=302,
            headers={"Location": "http://169.254.169.254/latest/meta-data"},
            json=lambda: {"status": "ok"},
        )

    monkeypatch.setattr("requests.post", _fake_post)
    res = deliver_outbox_item(
        dsn="postgres://unused",
        item=_ITEM,
        receiver_inbox_url="https://93.184.216.34/inbox",
        receiver_public_key="pk",
    )
    assert res.reason_code == "unsafe_delivery_url"
    assert res.status == "failed"


def test_mailbox_client_rejects_private_base_url_for_real_client():
    with pytest.raises(UnsafeFetchURL):
        MailboxPollingClient("http://10.0.0.5:8088")  # real client -> validated


def test_mailbox_client_injected_client_bypasses_validation():
    # An injected (stub) client is the test/dev path; base_url is not network-
    # validated because no real connection is opened here.
    c = MailboxPollingClient("http://localhost:8088", client=mock.MagicMock())
    assert c.base_url == "http://localhost:8088"
