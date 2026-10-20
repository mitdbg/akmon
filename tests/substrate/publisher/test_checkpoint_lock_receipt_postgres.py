"""A real publish stamps its lock hold into the checkpoint (decision 284a).

`okg deployment ready --checks dogfood` reads publish-capacity evidence from
the latest published generation. This proves the publisher really writes that
evidence and that readiness turns it into a pass with no benchmark at all,
which is what a fresh install has.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import psycopg
import pytest

pytestmark = pytest.mark.postgres


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def _publish_one_source(dsn: str, repo: Path) -> dict[str, Any]:
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.git_graph_files import GitFilesSource
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    SourceRunner(dsn, source_name="lock-receipt.git-files").run(
        GitFilesSource(repo="demo", repo_root=repo), mode="scope_complete",
    )
    return Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()


def test_a_real_publish_stamps_its_lock_hold_and_readiness_passes(
    clean_runtime: str, tmp_path: Path,
) -> None:
    from okg.substrate.cli.deployment import (
        _ready_add_publisher_lock_headroom_check,
        _ready_finalize_payload_state,
        _ready_generation_status,
        _ready_publisher_lock_headroom_status,
    )

    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "T")
    (repo / "README.md").write_text("# demo\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "initial")

    out = _publish_one_source(clean_runtime, repo)
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        generation = _ready_generation_status(conn, "default")
    lock = generation["publisher_lock"]
    assert lock is not None, generation
    assert lock["generation_id"] == out["generation_id"]
    assert lock["measured_until"] == "before_checkpoint_write"
    assert isinstance(lock["hold_ms"], float) and lock["hold_ms"] > 0
    # The stamped hold stops before the checkpoint write, so it is never
    # longer than the full hold the publish returned.
    assert lock["hold_ms"] <= out["timing_details"]["lock"]["hold_ms"]
    assert lock["hard_timeout_ms"] == (
        out["timing_details"]["lock"]["headroom"]["hard_timeout_ms"]
    )

    # A fresh install: no source has a full-pipeline benchmark.
    status = _ready_publisher_lock_headroom_status({"rows": [{
        "row_kind": "source", "name": "git-files", "readiness_required": True,
    }]})
    assert status["status"] == "unmeasured"
    payload: dict[str, Any] = {}
    _ready_add_publisher_lock_headroom_check(
        payload, {"publisher_lock_headroom": status}, lock,
    )
    _ready_finalize_payload_state(payload)
    assert payload["publisher_lock_headroom"]["status"] == "pass", payload
    assert not payload.get("unknowns")
