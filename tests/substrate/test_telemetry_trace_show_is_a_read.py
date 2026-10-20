"""`okg telemetry trace show` is a read at every flag combination.

It used to refuse `--include-profile` without `--apply`, in a refusal whose own
payload said `mutation_requested: false` and `mutation_started: false` while the
fix hint told the operator to "authorize the write". The guard was not simply
mislabelling a read: the profile report BUILT a bundle when none existed, and
building writes four files under the profile root and inserts artifact rows. So
the command really could write, and was gated for every invocation because of
the case where it would.

Showing a trace now reports an unbuilt profile instead of creating one, so the
command never writes and needs no authority. `okg telemetry profile` builds.
"""
from __future__ import annotations

import argparse
import json
import shlex
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate.cli.telemetry import _telemetry_requires_apply
from okg.substrate.telemetry import (
    TelemetrySpan,
    new_span_id,
    new_trace_id,
    record_span,
)
from okg.substrate.telemetry.operator import trace_show
from okg.substrate.telemetry.profile import build_profile_bundle

pytestmark = pytest.mark.postgres


def _seed_trace(dsn: str, *, deployment: str) -> str:
    """One trace with two spans and no profile bundle."""
    trace_id = new_trace_id()
    now = datetime.now(timezone.utc)
    with psycopg.connect(dsn, autocommit=True) as conn:
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=new_span_id(),
                span_name="publisher.cycle",
                deployment_name=deployment,
                source_name=deployment,
                started_at=now,
                ended_at=now + timedelta(milliseconds=40),
            ),
        )
        record_span(
            conn,
            TelemetrySpan(
                trace_id=trace_id,
                span_id=new_span_id(),
                span_name="source.read",
                deployment_name=deployment,
                source_name=deployment,
                started_at=now,
                ended_at=now + timedelta(milliseconds=5),
            ),
        )
    return trace_id


def _artifact_count(dsn: str, *, deployment: str, trace_id: str) -> int:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.telemetry_artifacts"
            " WHERE deployment_name = %s AND trace_id = %s",
            (deployment, trace_id),
        )
        return int(cur.fetchone()[0])


def _files_under(root: Path) -> set[Path]:
    return {path for path in root.rglob("*") if path.is_file()} if root.exists() else set()


def test_showing_a_trace_with_a_profile_needs_no_apply(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_dir = tmp_path / "profiles"
    monkeypatch.setenv("OKG_PROFILE_DIR", str(profile_dir))
    deployment = "trace-read-built"
    trace_id = _seed_trace(clean_runtime, deployment=deployment)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        build_profile_bundle(conn, deployment=deployment, trace_id=trace_id)

    rc = main([
        "telemetry", "trace", "show",
        "--deployment", deployment, "--dsn", clean_runtime,
        trace_id, "--include-profile", "--json",
    ])

    assert rc == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["profile_report"]["available"] is True
    assert shown["profile_report"]["critical_path"]["span_name"] == "publisher.cycle"


def test_showing_a_trace_without_a_profile_reports_it_instead_of_building(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_dir = tmp_path / "profiles"
    monkeypatch.setenv("OKG_PROFILE_DIR", str(profile_dir))
    deployment = "trace-read-unbuilt"
    trace_id = _seed_trace(clean_runtime, deployment=deployment)
    before_files = _files_under(profile_dir)
    before_artifacts = _artifact_count(
        clean_runtime, deployment=deployment, trace_id=trace_id,
    )

    rc = main([
        "telemetry", "trace", "show",
        "--deployment", deployment, "--dsn", clean_runtime,
        trace_id, "--include-profile", "--json",
    ])

    # It answers, rather than refusing for an authority it does not need.
    assert rc == 0
    report = json.loads(capsys.readouterr().out)["profile_report"]
    assert report["available"] is False
    assert report["unavailable_reason"] == "profile_bundle_not_built"
    # And it names the command that would build one.
    assert "okg telemetry profile" in report["next_check"]
    assert "--apply" in report["next_check"]

    # Nothing was written: this is the property the --apply guard existed for.
    assert _files_under(profile_dir) == before_files
    assert _artifact_count(
        clean_runtime, deployment=deployment, trace_id=trace_id,
    ) == before_artifacts

    # The suggested command is executable guidance. Run that exact argv after
    # supplying this test's private DSN, and prove the explicitly authorized
    # builder performs the write the read command avoided.
    build_argv = shlex.split(report["next_check"])
    assert build_argv[:3] == ["okg", "telemetry", "profile"]
    build_rc = main(build_argv[1:] + ["--dsn", clean_runtime, "--json"])
    assert build_rc == 0
    capsys.readouterr()
    assert _files_under(profile_dir) > before_files
    assert _artifact_count(
        clean_runtime, deployment=deployment, trace_id=trace_id,
    ) > before_artifacts


def test_a_recorded_bundle_whose_file_is_gone_is_told_apart(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_dir = tmp_path / "profiles"
    monkeypatch.setenv("OKG_PROFILE_DIR", str(profile_dir))
    deployment = "trace-read-lost"
    trace_id = _seed_trace(clean_runtime, deployment=deployment)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        build_profile_bundle(conn, deployment=deployment, trace_id=trace_id)
    for path in _files_under(profile_dir):
        path.unlink()

    with psycopg.connect(clean_runtime) as conn:
        shown = trace_show(
            conn, deployment=deployment, trace_id=trace_id, include_profile=True,
        )

    # An operator chasing a missing profile needs to know a row named a file
    # that is no longer there, not merely that nothing was built.
    assert shown["profile_report"]["unavailable_reason"] == "profile_bundle_file_missing"


def test_the_apply_guard_covers_building_and_not_reading() -> None:
    show = argparse.Namespace(
        telemetry_subcmd="trace", trace_subcmd="show", include_profile=True,
    )
    report = argparse.Namespace(telemetry_subcmd="report")
    build = argparse.Namespace(telemetry_subcmd="profile", profile_subcmd=None)
    bench = argparse.Namespace(telemetry_subcmd="bench", bench_subcmd="overhead")
    record = argparse.Namespace(telemetry_subcmd="coverage", record_metrics=True)
    coverage = argparse.Namespace(telemetry_subcmd="coverage", record_metrics=False)

    assert _telemetry_requires_apply(show) is False
    assert _telemetry_requires_apply(report) is False
    assert _telemetry_requires_apply(coverage) is False
    # The commands that do write still need it.
    assert _telemetry_requires_apply(build) is True
    assert _telemetry_requires_apply(bench) is True
    assert _telemetry_requires_apply(record) is True


def test_reporting_on_a_trace_without_a_profile_builds_nothing(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_dir = tmp_path / "profiles"
    monkeypatch.setenv("OKG_PROFILE_DIR", str(profile_dir))
    deployment = "trace-report-unbuilt"
    trace_id = _seed_trace(clean_runtime, deployment=deployment)

    rc = main([
        "telemetry", "report",
        "--deployment", deployment, "--dsn", clean_runtime,
        "--trace-id", trace_id, "--json",
    ])

    assert rc == 0
    report = json.loads(capsys.readouterr().out)
    assert report["schema"] == "okg.telemetry.operator_report.v1"
    assert report["profile"]["available"] is False
    assert _files_under(profile_dir) == set()
    assert _artifact_count(
        clean_runtime, deployment=deployment, trace_id=trace_id,
    ) == 0
