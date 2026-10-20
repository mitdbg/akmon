from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.substrate.conftest import clean_runtime, okg_dsn, okg_migrated  # noqa: F401


ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.postgres


def test_representative_interval_storage_benchmark(
    clean_runtime: str,  # noqa: F811
    tmp_path: Path,
) -> None:
    output = tmp_path / "chronos-interval-storage.json"
    # The benchmark publishes `claim` nodes against the latest applied catalog
    # that declares `claim`. `clean_runtime` keeps whatever catalog earlier
    # tests left, so seed one here rather than depend on test order.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["claim"])
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/benchmark_chronos_interval_storage.py"),
            "--dsn",
            clean_runtime,
            "--out",
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr[-4000:]
    report = json.loads(output.read_text())

    assert report["fixture"]["live_keys"] == 10000
    assert report["fixture"]["generations"] == 40
    assert report["fixture"]["closed_versions_per_selected_key"] == 30
    assert report["fixture"]["interval_rows"] >= 310000
    assert report["fixture"]["physical_relation"].startswith("_chronos_b_interval_")
    assert report["plans"]["current_uses_index"] is True
    assert report["plans"]["recent_pin_uses_index"] is True
    assert report["storage"]["relation_bytes"] > report["storage"]["index_bytes"] > 0
    assert report["storage"]["peak_temporary_disk_headroom_bytes"] == report["storage"]["relation_bytes"]
    assert report["publish"]["p95_ms"] > 0
    assert report["publish"]["wal_bytes"] > 0
    assert report["publish"]["path"].startswith("ChronosGraphStore")
    assert min(report["publish"]["sql_statements_per_generation"]) > 0
    assert set(report["publish"]["executed_compaction_statements_per_generation"]) == {0}
    assert report["publish"]["compaction_statement_delta_per_publish"] == 0
    assert report["compaction"]["applied"] is True
    assert report["compaction"]["rows_after"] < report["compaction"]["rows_before"]
    assert report["compaction"]["wal_bytes"] > 0
    assert report["compaction"]["progress"]["nodes"]["deleted"] > 0
