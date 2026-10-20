"""Active Health composer publication evidence, independent of publisher mutation."""
from datetime import datetime, timezone
from shlex import split
from types import SimpleNamespace

import pytest

from okg.substrate.console_views import health


def projected_reader(cp):
    """`read_generation_fields` over one fake checkpoint, same contract:
    top-level keys it carries; for each nested object, the named keys it
    carries, or None when the metadata holds no object there."""
    def read(conn, generation_id, keys, nested=None):
        assert generation_id == cp.checkpoint_id
        fields = {key: cp.metadata[key] for key in keys if key in cp.metadata}
        for outer, inner in (nested or {}).items():
            value = cp.metadata.get(outer)
            fields[outer] = (
                {key: value[key] for key in inner if key in value}
                if isinstance(value, dict) else None
            )
        return cp.checkpoint_id, cp.created_at, fields
    return read


def checkpoint(identifier, status, **metadata):
    return SimpleNamespace(checkpoint_id=identifier, created_at=datetime(2000, 1, 1, tzinfo=timezone.utc),
                           metadata={"status": status, "node_count": 2, "edge_count": 1, **metadata})


@pytest.mark.parametrize("latest_status", ["blocked", "published", "pending", "unknown"])
def test_latest_outcome_does_not_confuse_historical_block(monkeypatch, latest_status):
    baseline = checkpoint("canonical:baseline", "published")
    history = [baseline, checkpoint("canonical:old-block", "blocked"), checkpoint("canonical:latest", latest_status)]
    def recent_generations(**kw):
        # The bounded reader (issue #2113): newest first, and it never filled
        # its window, so what it returns is the whole history.
        from okg.substrate.graph_store_internal.generations import RecentGenerations

        rows = [row for row in reversed(history)
                if kw["status"] is None or row.metadata["status"] == kw["status"]]
        return RecentGenerations(rows, scanned=len(history), scan_window=kw["scan_window"],
                                 window_exhausted=False)

    def refuse_full_fetch(**kw):
        raise AssertionError("health must not fetch every checkpoint's full metadata")

    # No `get_generation` on the fake: the pinned generation is a projected
    # read now (issue #2546), so a regression to the whole-document read fails.
    store = SimpleNamespace(recent_generations=recent_generations,
                            count_generations=lambda **kw: len(history),
                            list_generations=refuse_full_fetch)
    monkeypatch.setattr(health.ChronosGraphStore, "from_connection", lambda *a, **kw: store)
    monkeypatch.setattr(health, "read_generation_fields", projected_reader(baseline))
    monkeypatch.setattr(health, "_health_source_breakdown", lambda *a, **kw: {})
    monkeypatch.setattr(health, "_resource_snapshot", lambda *a, **kw: {})
    monkeypatch.setattr(health, "_critical_read_prerequisites", lambda *a, **kw: {"prerequisites": []})
    monkeypatch.setattr(health, "_safe_runtime_queue_snapshot", lambda *a: {"queue_depth": {"ingest": 0}, "dlq_count": 0})
    session = SimpleNamespace(generation_id=baseline.checkpoint_id, branch_name="fork", compat_generation_id=7)
    result = health.compose_health(None, deployment_name="test", session=session)
    assert result["generation_id"] == baseline.checkpoint_id
    assert result["latest_attempt"]["status"] == latest_status
    assert any(row["code"] == "generation_blocked" for row in result["checks"]) == (latest_status == "blocked")
    assert result["history_total"] == 3
    # Old publication remains healthy absent a recorded current block.
    if latest_status != "blocked":
        assert result["verdict"] == "healthy"
    history.clear()
    result = health.compose_health(None, deployment_name="test", session=session)
    assert result["latest_attempt"] is None  # fork reads its base, no local attempt


def test_canonical_command_and_missing_delta_evidence():
    cp = checkpoint("canonical:blocked", "blocked", target_generation_id=99,
                    blocked_reason={"reason": "invariant_failed"},
                    projection_deltas={"nodes_added": 3, "edges_added": 0, "edges_retracted": 0})
    row = health._publication_attempt(cp, "deployment with spaces", "fork's branch")
    assert split(row["describe_command"]) == ["okg", "generation", "describe", "canonical:blocked",
        "--deployment", "deployment with spaces", "--branch", "fork's branch"]
    assert row["compat_generation_id"] == 99
    assert row["nodes_delta"] is None
    assert row["edges_delta"] == 0
    assert row["blocked_reason"] == "invariant_failed"
