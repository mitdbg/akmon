"""Connector ingest payload bounds + allowlist membership
(harden-mcp-write-boundaries).

connector_ingest_batch must bound per-record payload (bytes/fields/depth) and
enforce per-record allowlist membership (labels/calendars/channels) against the
route's provenance allowlist, not merely allowlist presence.
"""
from __future__ import annotations

import pytest
import psycopg

from okg.substrate.mcp.connector_ingest import (
    _MAX_RECORD_BYTES,
    _approved_route_ids,
    _assert_record_in_allowlist,
    _record_shape_violation,
    connector_ingest_batch,
    _current_catalog_version_id,
)


# ---- route binding ----


def test_approved_route_ids_include_host_connector():
    routes = _approved_route_ids("gmail_api")
    assert "gmail_api:host_connector" in routes
    assert "evil:route" not in routes


def test_batch_rejects_unapproved_route():
    res = connector_ingest_batch(
        None, "nodeploy", "gmail_api", "evil:route",
        [{"external_id": "m1", "labels": ["INBOX"]}],
        provenance={"allowlist": {"labels": ["INBOX"]}},
        dry_run=True,
    )
    assert res["status"] == "unapproved_route"
    assert res["route_id"] == "evil:route"


def test_batch_accepts_approved_route():
    res = connector_ingest_batch(
        None, "nodeploy", "gmail_api", "gmail_api:host_connector",
        [{"external_id": "m1", "labels": ["INBOX"], "subject": "hi"}],
        provenance={"allowlist": {"labels": ["INBOX"]}},
        dry_run=True,
    )
    assert res["status"] == "dry_run_ok"


# ---- payload bounds (pure) ----


def test_oversized_record_flagged():
    assert _record_shape_violation({"body": "x" * (_MAX_RECORD_BYTES + 1)}) == \
        "record_too_large_bytes"


def test_overdeep_record_flagged():
    d: dict = {}
    cur = d
    for _ in range(12):
        cur["k"] = {}
        cur = cur["k"]
    assert _record_shape_violation(d) == "record_too_deep"


def test_normal_record_not_flagged():
    assert _record_shape_violation(
        {"subject": "hi", "labels": ["INBOX"], "body_text": "hello"}
    ) is None


# ---- allowlist membership (pure) ----


def test_gmail_label_membership():
    # Names an allowlisted label -> ingested (even alongside other labels, which
    # real Gmail messages always carry).
    _assert_record_in_allowlist("gmail_api", {"labels": ["INBOX"]}, {"labels": ["INBOX"]})
    _assert_record_in_allowlist(
        "gmail_api", {"labels": ["INBOX", "IMPORTANT"]}, {"labels": ["INBOX"]},
    )
    # Disjoint labels -> refused.
    with pytest.raises(ValueError, match="no label within the route allowlist"):
        _assert_record_in_allowlist(
            "gmail_api", {"labels": ["SECRET"]}, {"labels": ["INBOX"]},
        )
    # The empty-labels bypass: a record naming NO label must NOT slip past a
    # non-empty allowlist (adversarial-review remediation).
    for empty in ({}, {"labels": []}, {"labels": None}):
        with pytest.raises(ValueError, match="no label within the route allowlist"):
            _assert_record_in_allowlist("gmail_api", empty, {"labels": ["INBOX"]})


def test_slack_channel_membership():
    _assert_record_in_allowlist("slack_api", {"channel_id": "C1"}, {"channels": ["C1"]})
    with pytest.raises(ValueError, match="outside the route allowlist"):
        _assert_record_in_allowlist(
            "slack_api", {"channel_id": "C999"}, {"channels": ["C1"]},
        )


def test_calendar_membership():
    with pytest.raises(ValueError, match="outside the route allowlist"):
        _assert_record_in_allowlist(
            "google_calendar_api", {"calendar_id": "evil@x"}, {"calendars": ["mine@x"]},
        )


# ---- batch integration (dry_run, no DB) ----


def test_batch_rejects_oversized_record():
    res = connector_ingest_batch(
        None, "nodeploy", "gmail_api", "gmail_api:host_connector",
        [{"external_id": "m1", "body_text": "x" * (_MAX_RECORD_BYTES + 1)}],
        provenance={"allowlist": {"labels": ["INBOX"]}},
        dry_run=True,
    )
    assert res["status"] == "record_too_large"
    assert res["reason"] == "record_too_large_bytes"


def test_batch_rejects_out_of_allowlist_label():
    res = connector_ingest_batch(
        None, "nodeploy", "gmail_api", "gmail_api:host_connector",
        [{"external_id": "m1", "labels": ["SECRET"]}],
        provenance={"allowlist": {"labels": ["INBOX"]}},
        dry_run=True,
    )
    assert res["status"] == "validation_failed"
    assert "allowlist" in res["errors"][0]["error"]


def test_batch_in_allowlist_record_passes_validation():
    res = connector_ingest_batch(
        None, "nodeploy", "gmail_api", "gmail_api:host_connector",
        [{"external_id": "m1", "labels": ["INBOX"], "subject": "hi"}],
        provenance={"allowlist": {"labels": ["INBOX"]}},
        dry_run=True,
    )
    assert res["status"] == "dry_run_ok"
    assert res["nodes_would_emit"] >= 1


@pytest.mark.postgres
def test_connector_ingest_pins_latest_applied_catalog(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        applied = int(cur.fetchone()[0])
        cur.execute(
            "INSERT INTO okg.catalog_versions (status) "
            "VALUES ('pending') RETURNING catalog_version_id"
        )
        pending = int(cur.fetchone()[0])
        assert pending > applied
        assert _current_catalog_version_id(conn) == applied
