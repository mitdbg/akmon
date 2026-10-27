"""A source sync's refused run-end publish is covered by a newer generation.

PACT publisher-starvation-4039 (issue #4039). A scheduled publish that ran past
the sync's workflow rows can hold the publish lock while the sync's own run-end
publish waits for it. When that wait ends, the sync's publish is refused, and it
used to end as an ERROR although a newer generation already held its writes.

Now the refusal is satisfied, and the publish entry records
``covered_by_generation``, when a published generation of the branch was cut
after the sync's last committed write. With only an older generation the sync
still fails, as before.
"""
from __future__ import annotations

import pytest

from okg.substrate.graph_store import ChronosGraphStore
from tests.substrate.ingest.test_run_base_undo import (
    AAA_RECORDS,
    DEPLOYMENT,
    _Fixture,
    _records,
)

pytestmark = pytest.mark.postgres


@pytest.fixture
def held_lock_fx(tmp_path, clean_runtime, monkeypatch):
    """The ingest fixture with a first generation, and the deployment's publish
    lock held by a second session (the scheduled publisher mid-publish)."""
    import psycopg

    from okg.substrate.publisher.publisher import _advisory_keys

    fx = _Fixture(tmp_path, clean_runtime, monkeypatch)
    first = fx.ingest()
    assert first.any_failed is False, first.report
    monkeypatch.setenv("OKG_DBOS_PUBLISH_LOCK_WAIT_SECONDS", "1")
    classid, objid = _advisory_keys(DEPLOYMENT)
    holder = psycopg.connect(clean_runtime, autocommit=True)
    assert holder.execute(
        "SELECT pg_try_advisory_lock(%s, %s)", (classid, objid),
    ).fetchone()[0]
    try:
        yield fx
    finally:
        holder.close()


def _cut_generation(dsn: str) -> str:
    """Another publisher cuts a published generation of the branch now."""
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkpoint = store.checkpoint_generation(
            store.new_generation_id(deployment_name=DEPLOYMENT, branch="default"),
            deployment_name=DEPLOYMENT,
        )
    return str(checkpoint)


def _change_aaa(fx: _Fixture) -> None:
    fx.write("aaa", {"records": _records(AAA_RECORDS, changed={4: "sync wrote this"})})


def test_a_refused_run_end_publish_is_covered_by_a_newer_generation(
    held_lock_fx: _Fixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.ingest.service as service
    from okg.substrate.runtime.source_sync_workflow import _failed_publishes

    fx = held_lock_fx
    _change_aaa(fx)
    real_publish = service._publish
    cut: list[str] = []

    def publish_after_another_cuts(options, label, *args, **kwargs):
        # The sync's writes are committed. Another publisher cuts a generation
        # (it holds the lock the sync's publish is now waiting for).
        if label == "commit" and not cut:
            cut.append(_cut_generation(fx.dsn))
        return real_publish(options, label, *args, **kwargs)

    monkeypatch.setattr(service, "_publish", publish_after_another_cuts)

    result = fx.ingest({"aaa"})

    assert len(cut) == 1
    assert result.any_failed is False, result.report["publishes"]
    (entry,) = [p for p in result.report["publishes"] if p["label"] == "commit"]
    assert entry["status"] == "published"
    assert entry["covered_by_generation"] == cut[0]
    assert entry["refused_publish"]["code"] == "publisher_lease_held"
    assert entry["covered_generation_segment"] > entry["writes_done_head_segment"]
    # The sync workflow does not count it as a failed publish.
    assert _failed_publishes(result.report["publishes"]) == []


def _stamp_all_checkpoints(dsn: str, seconds: float, *, only: str | None = None) -> None:
    """Move checkpoint ``created_at`` by ``seconds``: the stamp a process whose
    clock is that far off writes (Chronos stamps with the cutting process's
    clock, not the database's)."""
    import psycopg

    where = "" if only is None else " WHERE checkpoint_id = %s"
    params = (seconds,) if only is None else (seconds, only)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE _chronos_branch_interval_checkpoints SET created_at = "
            "to_char((created_at::timestamptz + make_interval(secs => %s)) "
            "AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"')"
            + where,
            params,
        )


def test_a_generation_stamped_by_a_fast_clock_does_not_cover(
    held_lock_fx: _Fixture,
) -> None:
    """The only generation was cut BEFORE the sync wrote, by a process whose
    clock runs a day ahead, so its stamp is later than the sync's last write.
    A timestamp comparison would call it a cover; the segment chain says it
    was cut earlier, and the refusal stands."""
    fx = held_lock_fx
    _change_aaa(fx)
    _stamp_all_checkpoints(fx.dsn, 86400)

    result = fx.ingest({"aaa"})

    assert result.any_failed is True
    (entry,) = [p for p in result.report["publishes"] if p["label"] == "commit"]
    assert entry["status"] == "fail" and "covered_by_generation" not in entry


def test_a_covering_generation_stamped_by_a_slow_clock_still_covers(
    held_lock_fx: _Fixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A generation cut after the sync's writes by a process whose clock runs a
    day behind has an earlier stamp than the sync's last write. It holds the
    writes and covers the refusal."""
    import okg.substrate.ingest.service as service

    fx = held_lock_fx
    _change_aaa(fx)
    real_publish = service._publish
    cut: list[str] = []

    def publish_after_a_slow_clock_cuts(options, label, *args, **kwargs):
        if label == "commit" and not cut:
            cut.append(_cut_generation(fx.dsn))
            _stamp_all_checkpoints(fx.dsn, -86400, only=cut[0])
        return real_publish(options, label, *args, **kwargs)

    monkeypatch.setattr(service, "_publish", publish_after_a_slow_clock_cuts)

    result = fx.ingest({"aaa"})

    assert result.any_failed is False, result.report["publishes"]
    (entry,) = [p for p in result.report["publishes"] if p["label"] == "commit"]
    assert entry["covered_by_generation"] == cut[0]


def test_a_refused_run_end_publish_with_only_an_older_generation_fails(
    held_lock_fx: _Fixture,
) -> None:
    """The first generation was cut before the sync wrote: it does not hold
    the sync's writes, so the refusal stands."""
    from okg.substrate.runtime.source_sync_workflow import _failed_publishes

    fx = held_lock_fx
    _change_aaa(fx)

    result = fx.ingest({"aaa"})

    assert result.any_failed is True
    (entry,) = [p for p in result.report["publishes"] if p["label"] == "commit"]
    assert entry["status"] == "fail"
    assert entry["code"] == "publisher_lease_held"
    assert "covered_by_generation" not in entry
    assert _failed_publishes(result.report["publishes"]) == [entry]


def test_a_publish_refused_for_another_sync_is_not_covered(
    held_lock_fx: _Fixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the held publish lock is covered. A publish that yielded to
    another sync's priority is a different cause and stays a failure even
    when a newer generation exists."""
    import okg.substrate.ingest.service as service

    fx = held_lock_fx
    _change_aaa(fx)
    real_publish = service._publish

    def refuse_for_priority(options, label, *args, **kwargs):
        if label == "commit":
            _cut_generation(fx.dsn)
            return {
                "label": label, "status": "fail",
                "code": "publisher_lease_held",
                "error": "source-sync publish work is active",
                "error_data": {
                    "reason": "source_sync_active",
                    "blocking_workflow_id": "wf-other-sync",
                    "advisory_key": [1, 2],
                },
            }
        return real_publish(options, label, *args, **kwargs)

    monkeypatch.setattr(service, "_publish", refuse_for_priority)

    result = fx.ingest({"aaa"})

    assert result.any_failed is True
    (entry,) = [p for p in result.report["publishes"] if p["label"] == "commit"]
    assert entry["status"] == "fail" and "covered_by_generation" not in entry
