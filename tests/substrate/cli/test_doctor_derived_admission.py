"""Doctor check for derived producers rejected at admission.

Pinned by PACT ``surface-derived-admission-rejections``. A derived producer
whose entire output is rejected does not block or degrade the publish — the
cycle still reports ``published`` with exit code 0 — so the row counts simply
do not move and the only durable trace is an ``okg.admission_envelopes`` row.
``okg doctor --check derived-admission`` makes that reachable without already
suspecting it: ``ok:true`` when every derived producer was admitted,
``ok:false`` with one finding per rejected (producer, code) pair otherwise.

These tests isolate the check by writing envelopes directly. The publisher
write-path that produces rejected envelopes is covered in
tests/substrate/test_derived_producer_authority.py.
"""
from __future__ import annotations

import argparse
import json
from uuid import uuid4

import psycopg
import pytest


pytestmark = pytest.mark.postgres


def _insert_envelope(
    dsn: str,
    *,
    deployment_name: str,
    producer_id: str,
    producer_kind: str,
    status: str,
    code: str | None,
    record_count: int | None = None,
    decided_at: str = "now",
) -> None:
    """Insert one admission envelope row directly (no Publisher needed)."""
    payload: dict[str, object] = {}
    if code is not None:
        payload["code"] = code
    if record_count is not None:
        payload["record_count"] = record_count
    verdict = json.dumps(payload)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.admission_envelopes
                (admission_id, deployment_name, producer_id, producer_kind,
                 intent, admission_mode, status, verdict, idempotency_key,
                 created_at, decided_at)
            VALUES (%s, %s, %s, %s, 'derived_edge_emission', 'fast_track',
                    %s, %s::jsonb, %s, now(), %s::timestamptz)
            """,
            (
                str(uuid4()),
                deployment_name,
                producer_id,
                producer_kind,
                status,
                verdict,
                str(uuid4()),
                decided_at,
            ),
        )


def _run_check(dsn: str, capsys: pytest.CaptureFixture[str]) -> tuple[int, dict]:
    from okg.substrate.cli.doctor import _doctor_derived_admission

    args = argparse.Namespace(dsn=dsn, deployment=None)
    rc = _doctor_derived_admission(args, as_json=True)
    return rc, json.loads(capsys.readouterr().out)


def test_doctor_clean_table_ok(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Empty table -> ok:true, exit 0. The check is not permanently red."""
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc, parsed = _run_check(clean_runtime, capsys)
    assert rc == 0
    assert parsed["ok"] is True
    assert parsed["rejected_count"] == 0
    assert parsed["findings"] == []


def test_doctor_admitted_producers_ok(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Published / accepted derived envelopes -> ok:true."""
    for status in ("published", "accepted"):
        _insert_envelope(
            clean_runtime,
            deployment_name="default",
            producer_id=f"_enricher:fine_{status}",
            producer_kind="enricher",
            status=status,
            code=None,
        )
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc, parsed = _run_check(clean_runtime, capsys)
    assert rc == 0
    assert parsed["ok"] is True
    assert parsed["findings"] == []


def test_doctor_reports_rejected_derived_producer(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A rejected derived producer is named with its rejection code."""
    _insert_envelope(
        clean_runtime,
        deployment_name="default",
        producer_id="_enricher:claim_lift",
        producer_kind="enricher",
        status="rejected",
        code="derived_output_trust_exceeds_input",
    )
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc, parsed = _run_check(clean_runtime, capsys)

    assert rc == 1
    assert parsed["ok"] is False
    assert parsed["rejected_count"] == 1
    finding = parsed["findings"][0]
    assert finding["producer_id"] == "_enricher:claim_lift"
    assert finding["code"] == "derived_output_trust_exceeds_input"
    assert finding["reason"] == "derived_output_discarded_at_admission"
    assert finding["status"] == "rejected"


def test_doctor_returns_to_green_after_the_producer_is_fixed(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A fixed producer clears the finding on its next admission.

    The check reports a producer only when its LATEST decision is a
    discard. Without that bound the table is append-only, so one bad cycle
    would leave the check red forever with no operator path back to green —
    and a permanently-red check is one nobody reads.
    """
    _insert_envelope(
        clean_runtime,
        deployment_name="default",
        producer_id="_enricher:claim_lift",
        producer_kind="enricher",
        status="rejected",
        code="derived_output_trust_exceeds_input",
        decided_at="2026-01-01T00:00:00Z",
    )
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc, parsed = _run_check(clean_runtime, capsys)
    assert rc == 1, "the rejection should be reported while it is the latest"

    # The producer is fixed and its next cycle is admitted.
    _insert_envelope(
        clean_runtime,
        deployment_name="default",
        producer_id="_enricher:claim_lift",
        producer_kind="enricher",
        status="published",
        code=None,
        decided_at="2026-01-02T00:00:00Z",
    )
    rc, parsed = _run_check(clean_runtime, capsys)
    assert rc == 0
    assert parsed["ok"] is True
    assert parsed["findings"] == []


def test_doctor_reports_the_discarded_record_count(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The finding names how many records were thrown away.

    "Some output was discarded" is not actionable; "412 records were
    discarded" is. The publisher records the count on the verdict.
    """
    _insert_envelope(
        clean_runtime,
        deployment_name="default",
        producer_id="_enricher:claim_lift",
        producer_kind="enricher",
        status="rejected",
        code="derived_output_trust_exceeds_input",
        record_count=412,
    )
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc, parsed = _run_check(clean_runtime, capsys)
    assert rc == 1
    assert parsed["findings"][0]["discarded_record_count"] == 412


def test_doctor_reports_failed_and_quarantined_discards(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A producer that FAILED mid-emit had its output discarded too.

    The derived-fact loader excludes rejected, failed, and quarantined
    runs identically, so reporting only 'rejected' would leave an enricher
    that threw mid-emit looking healthy while its whole output vanished.
    """
    for producer, status in (
        ("_enricher:threw", "failed"),
        ("_enricher:held", "quarantined"),
    ):
        _insert_envelope(
            clean_runtime,
            deployment_name="default",
            producer_id=producer,
            producer_kind="enricher",
            status=status,
            code="derived_producer_failed",
        )
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc, parsed = _run_check(clean_runtime, capsys)

    assert rc == 1
    reported = {f["producer_id"]: f["status"] for f in parsed["findings"]}
    assert reported == {
        "_enricher:threw": "failed",
        "_enricher:held": "quarantined",
    }


def test_doctor_scopes_findings_to_the_deployment(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A rejection in another deployment is not reported here."""
    _insert_envelope(
        clean_runtime,
        deployment_name="somewhere-else",
        producer_id="_enricher:other",
        producer_kind="enricher",
        status="rejected",
        code="derived_output_trust_exceeds_input",
    )
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc, parsed = _run_check(clean_runtime, capsys)
    assert rc == 0
    assert parsed["ok"] is True
    assert parsed["findings"] == []


def test_doctor_ignores_non_derived_producers(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A rejected SOURCE is not a derived-producer finding.

    Source rejections are a different triage path (okg admission / the
    source-health check); folding them in here would make this check a
    generic rejection dashboard rather than the derived-output signal.
    """
    _insert_envelope(
        clean_runtime,
        deployment_name="default",
        producer_id="some-source",
        producer_kind="source",
        status="rejected",
        code="producer_signature_mismatch",
    )
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc, parsed = _run_check(clean_runtime, capsys)
    assert rc == 0
    assert parsed["ok"] is True
    assert parsed["findings"] == []
