"""Console health runtime queue tests."""
from __future__ import annotations

from okg.substrate.console_views.health import _runtime_queue_snapshot


def test_console_health_surfaces_current_runtime_dlq(monkeypatch) -> None:
    # Current dead-lettered runtime work must make console health go red while
    # the raw view continues to expose retained terminal history.
    cur = _FakeQueueCursor(dlq_count=3, terminal_history_count=8)
    monkeypatch.setattr(
        "okg.substrate.runtime_queue_health.current_runtime_dlq",
        lambda _cur: {
            "current_count": 3,
            "historical_count": 0,
            "current_failures": [
                {
                    "workflow_id": f"failed-{index}",
                    "workflow_name": "okg_source_sync",
                    "status": "ERROR",
                    "application_version": None,
                    "responsibility": {},
                }
                for index in range(3)
            ],
        },
    )

    out = _runtime_queue_snapshot(cur)

    assert out["ok"] is False
    assert out["status"] == "dead_letter"
    assert out["dlq_count"] == 3
    assert out["terminal_history_dlq_count"] == 3
    assert out["runtime_failed_count"] == 3
    assert out["terminal_history_count"] == 8
    assert out["queue_depth"]["ingest"] == 0
    assert "okg.v_runtime_queue_health" in "\n".join(cur.queries)


class _FakeQueueCursor:
    def __init__(
        self,
        *,
        dlq_count: int,
        terminal_history_count: int,
    ) -> None:
        self._dlq_count = dlq_count
        self._terminal_history_count = terminal_history_count
        self.queries: list[str] = []

    def execute(self, query: str) -> None:
        self.queries.append(str(query))

    def fetchone(self) -> tuple[bool]:
        return (True,)

    def fetchall(self) -> list[tuple]:
        return [(
            "okg-test-source-sync",
            "failed",
            "available",
            0,
            0,
            0,
            0,
            0,
            self._dlq_count,
            0,
            self._dlq_count,
            self._terminal_history_count,
            0,
            None,
            None,
            600,
            False,
            None,
            None,
            "okg_source_sync",
        )]
