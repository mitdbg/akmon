from __future__ import annotations

from typing import Any

from okg.viz import reads_system


class _Cursor:
    def __init__(self) -> None:
        self.query = ""

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, _params: object = None) -> None:
        self.query = query

    def fetchone(self) -> tuple[int]:
        return (7 if "graph_nodes" in self.query else 5,)

    def fetchall(self) -> list[tuple[str, int]]:
        return [("publication", 7)]


class _CompatibilityConnection:
    def cursor(self) -> _Cursor:
        return _Cursor()


class _FailingStore:
    @classmethod
    def from_connection(cls, *_args: object, **_kwargs: object) -> "_FailingStore":
        return cls()

    def checkout_generation(self, _generation_id: int) -> None:
        raise RuntimeError("injected Chronos checkout failure")


class _Graph:
    def query(self, sql: str) -> list[dict[str, Any]]:
        if "GROUP BY subtype" in sql:
            return [{"subtype": "publication", "n": 11}]
        if "graph_nodes" in sql:
            return [{"n": 11}]
        return [{"n": 9}]


class _HealthyStore:
    @classmethod
    def from_connection(cls, *_args: object, **_kwargs: object) -> "_HealthyStore":
        return cls()

    def checkout_generation(self, _generation_id: int) -> _Graph:
        return _Graph()


def test_live_graph_failure_is_typed_in_system_snapshot_and_retry_recovers(
    monkeypatch,
) -> None:
    conn = _CompatibilityConnection()
    monkeypatch.setattr(reads_system, "ChronosGraphStore", _FailingStore)

    degraded = reads_system.fetch_live_graph(conn, 42)  # type: ignore[arg-type]

    assert degraded == {
        "node_count": 7,
        "edge_count": 5,
        "top_subtypes": [{"subtype": "publication", "count": 7}],
        "status": "degraded",
        "cause": "chronos_generation_read_failed",
        "omitted_work": ["generation_pinned_live_graph_counts"],
        "scope": {"generation_id": 42},
        "retryable": True,
        "freshness": "unavailable",
        "source": "current_graph_tables",
    }

    monkeypatch.setattr(
        reads_system,
        "fetch_generation",
        lambda _conn, _deployment: {
            "published": {"generation_id": 42},
        },
    )
    for name in (
        "fetch_catalog",
        "fetch_sources",
        "fetch_storage",
        "fetch_queue",
        "fetch_projection_backlog",
        "fetch_enrichers",
        "fetch_doctor",
        "fetch_llm_spend",
        "fetch_recent_errors",
        "fetch_profiling",
    ):
        monkeypatch.setattr(reads_system, name, lambda *_args: {})
    snapshot = reads_system.fetch_snapshot(conn, "demo")  # type: ignore[arg-type]
    assert snapshot["live_graph"] == degraded

    monkeypatch.setattr(reads_system, "ChronosGraphStore", _HealthyStore)
    recovered = reads_system.fetch_live_graph(conn, 42)  # type: ignore[arg-type]

    assert recovered["status"] == "available"
    assert recovered["freshness"] == "fresh"
    assert recovered["source"] == "chronos_generation"
    assert recovered["node_count"] == 11
    assert recovered["edge_count"] == 9
    assert "cause" not in recovered
