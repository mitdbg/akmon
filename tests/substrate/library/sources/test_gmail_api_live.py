"""Env-gated live verification for finish-gmail-source.

Skipped unless OKG_LIVE_GMAIL=1 with a keyring token for the
GMAIL provider and OKG_LIVE_GMAIL_LABELS set (comma-separated
allowlist). Backs the live half of `tp.gmail.live`; the
fixture-tier and DB-publish proofs live in test_gmail_api.py.
"""
from __future__ import annotations

import os

import pytest

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.gmail_api import GmailApiSource

pytestmark = pytest.mark.skipif(
    os.environ.get("OKG_LIVE_GMAIL") != "1",
    reason="live Gmail test is env-gated (OKG_LIVE_GMAIL=1)",
)


def test_live_backfill_then_incremental_publishes() -> None:
    labels = [
        label.strip()
        for label in os.environ.get("OKG_LIVE_GMAIL_LABELS", "").split(",")
        if label.strip()
    ]
    assert labels, "set OKG_LIVE_GMAIL_LABELS to the allowlisted labels"
    deployment = os.environ.get("OKG_LIVE_GMAIL_DEPLOYMENT", "academic-researcher")

    src = GmailApiSource(deployment_name=deployment, gmail_labels=labels)
    preflight = src.preflight()
    assert preflight.status == "ok", preflight.as_dict()

    backfill = src.run("live-backfill")
    facts = list(backfill.facts)
    emails = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "email"]
    threads = [
        f for f in facts if isinstance(f, NodeFact) and f.subtype == "email_thread"
    ]
    assert emails, "allowlisted labels produced zero emails"
    assert threads
    assert backfill.next_cursor and backfill.next_cursor["gmail_history_id"]

    # Immediate incremental run from the fresh anchor: zero or few
    # facts, and the run must not claim full-scope authority.
    delta = src.run("live-delta", cursor=backfill.next_cursor)
    delta_facts = list(delta.facts)
    assert delta.completed_scope is False
    assert len(delta_facts) <= len(facts)
