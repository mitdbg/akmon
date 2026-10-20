"""Doctor check for post-publish view-edge projection failures.

Pinned by ``surface-post-publish-projection-failures``. The publisher upserts
one row per post-publish view-edge projection run into
``okg.post_publish_projection_status`` (success or failure). ``okg doctor
--check post-publish-projection`` reads that table and reports ``ok:true`` when
no row is ``status='failed'`` for the deployment, ``ok:false`` with one finding
per failed row otherwise; an empty table (freshly-migrated deployment) reports
``ok:true``.

These tests isolate the doctor check by writing rows directly. The publisher's
upsert write-path (the producer of those rows) is covered separately in
tests/substrate/publisher/test_post_publish_projection_status.py.
"""
from __future__ import annotations

import argparse
import json

import psycopg
import pytest


pytestmark = pytest.mark.postgres


def _insert_status(
    dsn: str,
    *,
    deployment_name: str,
    generation_id: int,
    status: str,
    error: str | None,
) -> None:
    """Insert one projection-status row directly (no Publisher needed)."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.post_publish_projection_status
                (deployment_name, generation_id, last_run_at, status, error)
            VALUES (%s, %s, now(), %s, %s)
            """,
            (deployment_name, generation_id, status, error),
        )


def _run_check(dsn: str, capsys: pytest.CaptureFixture[str]) -> tuple[int, dict]:
    from okg.substrate.cli.doctor import _doctor_post_publish_projection

    # No deployment kwarg -> exercises the check's "default" deployment path
    # without going through _resolve_deployment_dsn.
    args = argparse.Namespace(dsn=dsn, deployment=None)
    rc = _doctor_post_publish_projection(args, as_json=True)
    return rc, json.loads(capsys.readouterr().out)


def test_doctor_clean_table_ok(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Empty table -> ok:true, exit 0."""
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc, parsed = _run_check(clean_runtime, capsys)
    assert rc == 0
    assert parsed["ok"] is True
    assert parsed["failed_count"] == 0
    assert parsed["findings"] == []


def test_doctor_all_success_ok(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Only status='success' rows -> ok:true."""
    _insert_status(clean_runtime, deployment_name="default", generation_id=1, status="success", error=None)
    _insert_status(clean_runtime, deployment_name="default", generation_id=2, status="success", error=None)
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc, parsed = _run_check(clean_runtime, capsys)
    assert rc == 0
    assert parsed["ok"] is True
    assert parsed["failed_count"] == 0


def test_doctor_surfaces_failure(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """One status='failed' row -> ok:false with a finding citing generation_id + error."""
    _insert_status(clean_runtime, deployment_name="default", generation_id=10, status="success", error=None)
    _insert_status(
        clean_runtime,
        deployment_name="default",
        generation_id=11,
        status="failed",
        error="RuntimeError: schema mismatch",
    )
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc, parsed = _run_check(clean_runtime, capsys)
    assert rc == 1
    assert parsed["ok"] is False
    assert parsed["failed_count"] == 1
    assert len(parsed["findings"]) == 1
    finding = parsed["findings"][0]
    assert finding["generation_id"] == 11
    assert finding["error"] == "RuntimeError: schema mismatch"
    assert finding["last_run_at"] is not None


def test_doctor_scopes_to_deployment(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A failed row under a different deployment does not trip the default scope."""
    _insert_status(
        clean_runtime,
        deployment_name="other",
        generation_id=99,
        status="failed",
        error="RuntimeError: not mine",
    )
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    rc, parsed = _run_check(clean_runtime, capsys)
    assert rc == 0
    assert parsed["ok"] is True
    assert parsed["deployment"] == "default"
    assert parsed["failed_count"] == 0
