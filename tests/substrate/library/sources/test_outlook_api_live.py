"""Env-gated live verification for add-msgraph-sources.

Skipped unless OKG_LIVE_MSGRAPH=1 with a keyring token for the
shared `msgraph` provider (tenant-internal Entra app, delegated
Mail.Read + Calendars.Read) and OKG_LIVE_MSGRAPH_FOLDERS set.
Until a consentable M365 tenant is reachable this remains an
environment blocker on task.msgraph.implement, not a waiver.
"""
from __future__ import annotations

import os

import pytest

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.outlook_api import OutlookMailApiSource

pytestmark = pytest.mark.skipif(
    os.environ.get("OKG_LIVE_MSGRAPH") != "1",
    reason="live M365 test is env-gated (OKG_LIVE_MSGRAPH=1)",
)


def test_live_backfill_then_delta_publishes() -> None:
    folders = [
        f.strip()
        for f in os.environ.get("OKG_LIVE_MSGRAPH_FOLDERS", "").split(",")
        if f.strip()
    ]
    assert folders, "set OKG_LIVE_MSGRAPH_FOLDERS to the allowlisted folders"
    deployment = os.environ.get("OKG_LIVE_MSGRAPH_DEPLOYMENT", "academic-researcher")

    src = OutlookMailApiSource(
        deployment_name=deployment, outlook_folders=folders,
    )
    preflight = src.preflight()
    assert preflight.status == "ok", preflight.as_dict()

    backfill = src.run("live-backfill")
    facts = list(backfill.facts)
    emails = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "email"]
    assert emails, "allowlisted folders produced zero emails"
    deltalinks = backfill.next_cursor["mail_deltalinks"]
    assert deltalinks, "no deltaLink captured after the walk"

    delta = src.run("live-delta", cursor=backfill.next_cursor)
    delta_facts = list(delta.facts)
    assert delta.completed_scope is False
    assert len(delta_facts) <= len(facts)
