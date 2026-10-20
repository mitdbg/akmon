from __future__ import annotations

import datetime

import pytest

from okg.substrate.mcp import freshness
from okg.substrate.mcp.server import build_server, _stamp_freshness_outcomes


_GENERATION = "okg:demo:branch:default:gen:20260818T120000000000Z:abc"


class _Cursor:
    def __init__(self, conn: "_Connection") -> None:
        self.conn = conn

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, *_args: object, **_kwargs: object) -> None:
        self.conn.execute_calls += 1
        if self.conn.execute_calls in self.conn.fail_calls:
            raise RuntimeError("injected watermark query failure")

    def fetchall(self):
        return list(self.conn.rows)


class _Connection:
    def __init__(self, rows=None, *, fail_calls=()) -> None:
        self.rows = list(rows or [])
        self.fail_calls = set(fail_calls)
        self.execute_calls = 0

    def cursor(self) -> _Cursor:
        return _Cursor(self)


def _reset_cache() -> None:
    freshness._cache.clear()


def test_each_query_failure_is_typed_on_every_read_operator() -> None:
    for operator in sorted(freshness.READ_OPERATORS):
        for failed_call, field, cause, omitted_work in (
            (1, "source_freshness", "source_freshness_query_failed", "source_watermark_summary"),
            (2, "publish_freshness", "publish_freshness_query_failed", "publish_lag_summary"),
        ):
            _reset_cache()
            payload: dict[str, object] = {}
            _stamp_freshness_outcomes(
                payload,
                operator,
                conn=_Connection(fail_calls={failed_call}),
                deployment_name="demo",
                generation_id=_GENERATION,
            )

            assert payload[field] == {
                "status": "unavailable",
                "cause": cause,
                "omitted_work": [omitted_work],
                "scope": {"deployment": "demo"},
                "retryable": True,
                "freshness": "unavailable",
            }
            other_field = (
                "publish_freshness"
                if field == "source_freshness"
                else "source_freshness"
            )
            assert payload[other_field].get("status") != "unavailable"


def test_corrected_retry_clears_failure_without_cache_reset() -> None:
    completed = datetime.datetime.now(datetime.timezone.utc).isoformat()
    rows = [("source-a", {"completed_at": completed})]
    for failed_call, failed_field in (
        (1, "source_freshness"),
        (2, "publish_freshness"),
    ):
        _reset_cache()
        conn = _Connection(rows=rows, fail_calls={failed_call})
        failed: dict[str, object] = {}
        _stamp_freshness_outcomes(
            failed,
            "inspect",
            conn=conn,
            deployment_name="demo",
            generation_id=_GENERATION,
        )
        assert failed[failed_field]["status"] == "unavailable"

        recovered: dict[str, object] = {}
        _stamp_freshness_outcomes(
            recovered,
            "inspect",
            conn=conn,
            deployment_name="demo",
            generation_id=_GENERATION,
        )
        source = recovered["source_freshness"]
        publish = recovered["publish_freshness"]
        assert isinstance(source, dict) and source["sources_checked"] == 1
        assert isinstance(publish, dict) and "publish_lag_seconds" in publish
        assert source.get("status") != "unavailable"
        assert publish.get("status") != "unavailable"


def test_missing_or_unparseable_generation_is_typed_unavailable() -> None:
    for generation_id in (None, "not-a-generation"):
        payload: dict[str, object] = {}
        _stamp_freshness_outcomes(
            payload,
            "inspect",
            conn=_Connection(),
            deployment_name="demo",
            generation_id=generation_id,
        )

        assert payload["publish_freshness"] == {
            "status": "unavailable",
            "cause": "publish_generation_timestamp_unavailable",
            "omitted_work": ["publish_lag_summary"],
            "scope": {"deployment": "demo"},
            "retryable": True,
            "freshness": "unavailable",
        }


@pytest.mark.postgres
def test_unpinned_inspect_final_response_contains_both_freshness_fields(
    clean_runtime: str,
    okg_mcp_can_log_in: str,
) -> None:
    # okg_mcp logs in through the shared fixture: LOGIN is set once under the
    # cluster role lock and never reverted (tests/substrate/conftest.py,
    # okg_mcp_login_enabled). This test used to set LOGIN itself and restore
    # the value it read, the pattern that raced the other xdist worker.
    #
    # `record_templates` describes what a `record` call may write, so only a
    # surface that serves `record` routes it (effective_surface.
    # RECORD_INSPECT_KINDS); the write/admin launch does.
    mcp = build_server(
        dsn=clean_runtime, deployment_name="default", allow_writes=True
    )
    try:
        inspect = mcp._tool_manager._tools["inspect"].fn
        payload = inspect(
            target={
                "kind": "record_templates",
                "record_kind": "observation",
            },
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert "source_freshness" in payload
    assert payload["publish_freshness"] == {
        "status": "unavailable",
        "cause": "publish_generation_timestamp_unavailable",
        "omitted_work": ["publish_lag_summary"],
        "scope": {"deployment": "default"},
        "retryable": True,
        "freshness": "unavailable",
    }
