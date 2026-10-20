"""Tests for the disk-usage report (PACT bound-okg-workspace-disk-growth, task 1).

Pure tests cover the stale-counter guard and the marginal-bytes math (no DB).
DB-backed tests assert the report matches a direct psql ground-truth and that an
unreadable DSN raises rather than returning a default/zero.
"""
from __future__ import annotations

import argparse

import psycopg
import pytest

from okg.substrate.db.management import (
    _disk_dead_pct,
    _disk_marginal_from_samples,
    disk_usage_report,
)


# --- pure: stale-counter guard ---------------------------------------------

def test_disk_usage_report_dead_pct_normal():
    pct, stale = _disk_dead_pct(n_live=70, n_dead=30, reltuples=100)
    assert pct == 30.0
    assert stale is False


def test_disk_usage_report_dead_pct_stale_counter_is_not_100pct():
    # n_live=0 while the catalog still estimates rows present -> the counters
    # were reset; we must NOT emit a misleading 100%.
    pct, stale = _disk_dead_pct(n_live=0, n_dead=5_000_000, reltuples=7_000_000)
    assert pct is None
    assert stale is True


def test_disk_usage_report_dead_pct_empty_table():
    pct, stale = _disk_dead_pct(n_live=0, n_dead=0, reltuples=0)
    assert pct is None
    assert stale is False


# --- pure: marginal bytes-per-generation -----------------------------------

def test_disk_usage_report_marginal_two_samples():
    rows = [
        {"generations": 110, "total_bytes": 2_000},
        {"generations": 100, "total_bytes": 1_000},
    ]
    out = _disk_marginal_from_samples(rows)
    assert out["bytes_per_generation"] == 100.0  # (2000-1000)/(110-100)
    assert out["reason"] is None
    assert out["samples_used"] == 2


def test_disk_usage_report_marginal_single_sample_is_not_zero():
    out = _disk_marginal_from_samples([{"generations": 100, "total_bytes": 1_000}])
    assert out["bytes_per_generation"] is None
    assert out["reason"] == "insufficient_history"


def test_disk_usage_report_marginal_no_new_generations():
    rows = [
        {"generations": 100, "total_bytes": 2_000},
        {"generations": 100, "total_bytes": 1_000},
    ]
    out = _disk_marginal_from_samples(rows)
    assert out["bytes_per_generation"] is None
    assert out["reason"] == "no_new_generations"


# --- loud-fail: unreadable DSN ---------------------------------------------

def test_disk_usage_report_unreadable_dsn_raises():
    # An unreachable DSN must raise -- never a zeroed report (P0 never-default).
    with pytest.raises(psycopg.Error):
        disk_usage_report(
            "postgresql://nobody:nobody@127.0.0.1:1/does_not_exist",
            record_sample=False,
        )


# --- DB-backed: matches a direct psql ground-truth -------------------------

@pytest.mark.postgres
def test_disk_usage_report_matches_ground_truth(okg_migrated: str):
    dsn = okg_migrated
    report = disk_usage_report(dsn, deployment_name="okg_test")

    assert report["ok"] is True
    assert report["check"] == "disk"

    # total_bytes matches pg_database_size read on the same DB (within a block;
    # a single recorded sample row cannot move a whole-DB size measurably).
    with psycopg.connect(dsn) as conn:
        truth = int(
            conn.execute(
                "SELECT pg_database_size(current_database())"
            ).fetchone()[0]
        )
    assert report["total_bytes"] > 0
    assert abs(report["total_bytes"] - truth) <= max(16 * 1024 * 1024, truth // 50)

    # The migrated schema has tables, so top_relations is non-empty and shaped.
    assert report["top_relations"], "expected non-empty top_relations"
    first = report["top_relations"][0]
    assert {"schema", "relation", "total_bytes", "table_bytes", "index_bytes"} <= set(first)
    # table_bytes (heap+TOAST) + index_bytes must reconcile to total_bytes --
    # this catches the pg_relation_size-excludes-TOAST mislabel.
    assert first["table_bytes"] + first["index_bytes"] == first["total_bytes"]

    # generation table exists after migrate -> a real int (0 on an empty DB is a
    # true count, not a default).
    assert isinstance(report["generations"], int)

    # All required sections present.
    for key in ("dead_tuples", "compression", "index_usage", "marginal_growth"):
        assert key in report
    assert "unused_index_bytes" in report["index_usage"]


@pytest.mark.postgres
def test_disk_usage_report_records_sample_and_reports_marginal_honestly(
    okg_migrated: str,
):
    dsn = okg_migrated
    # Two runs record two samples; with no new generations between them the
    # marginal is reported as a reason, never a fabricated number.
    disk_usage_report(dsn, deployment_name="okg_test")
    report = disk_usage_report(dsn, deployment_name="okg_test")
    marginal = report["marginal_growth"]
    assert marginal["samples_used"] >= 1
    if marginal["bytes_per_generation"] is None:
        assert marginal["reason"] in {"no_new_generations", "insufficient_history"}


@pytest.mark.postgres
def test_disk_usage_report_deep_mode_scans_relations(okg_migrated: str):
    # deep=True installs pgstattuple (postgres test role is superuser) and scans
    # the largest relations; each entry carries a real source + a note slot.
    report = disk_usage_report(okg_migrated, deployment_name="okg_test", deep=True)
    assert report["deep"] is True
    for entry in report["dead_tuples"]:
        assert entry["source"] == "pgstattuple_approx"
        assert "note" in entry  # None on success, a message on a per-rel failure


# --- the CLI wrappers must loud-fail (not crash) on a bad DSN ---------------
# This is the path that a library-only test misses: the wrapper must convert the
# psycopg error into a structured _emit_error, not an uncaught traceback.

def _bad_dsn_namespace(monkeypatch) -> argparse.Namespace:
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    monkeypatch.delenv("OKG_DSN", raising=False)
    return argparse.Namespace(
        deployment=None,
        dsn="postgresql://nobody:nobody@127.0.0.1:1/does_not_exist",
        deep=False,
        top=20,
    )


def test_disk_usage_report_doctor_wrapper_loud_fails_on_bad_dsn(monkeypatch):
    from okg.substrate.cli.doctor import _doctor_disk

    args = _bad_dsn_namespace(monkeypatch)
    # Must return a non-zero exit via _emit_error, NOT raise an uncaught error.
    rc = _doctor_disk(args, as_json=True)
    assert rc != 0


def test_disk_usage_report_metrics_wrapper_loud_fails_on_bad_dsn(monkeypatch):
    from okg.substrate.cli.metrics import _cmd_metrics_disk

    args = _bad_dsn_namespace(monkeypatch)
    rc = _cmd_metrics_disk(args, as_json=True)
    assert rc != 0


@pytest.mark.postgres
def test_disk_usage_report_excludes_partitioned_parents(okg_migrated: str):
    # A partitioned PARENT has pg_table_size=0 but a non-zero
    # pg_total_relation_size (its partition tree), so it must be excluded -- else
    # it shows a misleading 0-byte row and breaks table+index==total. Its real
    # bytes live in the child partitions (normal 'r' tables).
    dsn = okg_migrated
    parent = "okg_disk_part_test"
    with psycopg.connect(dsn) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {parent} CASCADE")
        conn.execute(
            f"CREATE TABLE {parent} (id bigint, v text) PARTITION BY RANGE (id)"
        )
        conn.execute(
            f"CREATE TABLE {parent}_p0 PARTITION OF {parent} "
            "FOR VALUES FROM (0) TO (1000000)"
        )
        conn.execute(
            f"INSERT INTO {parent} SELECT g, repeat('x', 200) "
            "FROM generate_series(0, 20000) g"
        )
        conn.commit()
    try:
        report = disk_usage_report(dsn, deployment_name="okg_test", top_limit=500)
        names = {r["relation"] for r in report["top_relations"]}
        assert parent not in names  # the 'p' parent is excluded
        # every reported row reconciles, even with a partitioned table present.
        for r in report["top_relations"]:
            assert r["table_bytes"] + r["index_bytes"] == r["total_bytes"]
    finally:
        with psycopg.connect(dsn) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {parent} CASCADE")
            conn.commit()
