"""Health, the knowledge overview and producer metrics read a BOUNDED window.

All three used to call `ChronosGraphStore.list_generations`, which has no
LIMIT and no projection: every call moved every generation checkpoint's full
metadata document (88 rows, 131 MB on okg-workspace) to print ten rows, one
number, or a handful of enricher runs (issue #2113).

No database here. Each fake store serves the bounded reader the way the real
one does -- newest first, `window_exhausted` when the scan filled its bound --
and REFUSES `list_generations` unless the test is about the named fallback.
So a composer that still reaches the full fetch fails with that refusal, and
a composer that silently serves a short window when older checkpoints exist
fails on the fallback assertion.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okg.substrate.console_views import health, knowledge, producer_metrics
from okg.substrate.graph_store_internal.generations import RecentGenerations


class _FullFetchRefused(AssertionError):
    """Raised in place of the unbounded full-metadata fetch."""


def _checkpoint(index: int, *, status: str = "published", **metadata):
    return SimpleNamespace(
        checkpoint_id=f"cp:{index:03d}",
        branch_id="default",
        segment_id=index,
        created_at=(
            datetime(2026, 9, 1, tzinfo=timezone.utc) + timedelta(hours=index)
        ).isoformat(),
        metadata={
            "kind": "okg_generation",
            "deployment_name": "demo",
            "status": status,
            **metadata,
        },
    )


class _Store:
    """The bounded reader, served from an ascending in-memory history."""

    def __init__(self, history, *, allow_full_fetch: bool = False,
                 other_rows: int = 0, count=None) -> None:
        self.history = list(history)
        # Checkpoints on the branch that are NOT this deployment's
        # generations: they fill a scan window without matching it.
        self.other_rows = other_rows
        self.allow_full_fetch = allow_full_fetch
        self.count = count
        self.reads: list[dict] = []
        self.full_fetches = 0

    def recent_generations(self, *, deployment_name, branch, status=None,
                           keys=None, scan_window=None, since=None):
        self.reads.append({
            "status": status, "keys": keys, "scan_window": scan_window,
            "since": since,
        })
        window = 200 if scan_window is None else scan_window
        newest = list(reversed(self.history))
        if since is not None:
            newest = [row for row in newest if row.created_at >= since]
        branch_rows = [None] * self.other_rows + newest
        scanned = branch_rows[:window]
        rows = [
            SimpleNamespace(
                checkpoint_id=row.checkpoint_id,
                branch_id=row.branch_id,
                segment_id=row.segment_id,
                created_at=row.created_at,
                metadata={
                    key: row.metadata.get(key)
                    for key in tuple(keys or ()) + ("kind", "deployment_name", "status")
                },
            )
            for row in scanned
            if row is not None
            and (status is None or row.metadata.get("status") == status)
        ]
        return RecentGenerations(
            rows,
            scanned=len(scanned),
            scan_window=window,
            window_exhausted=len(scanned) >= window,
        )

    def list_generations(self, *, deployment_name, branch, status="published"):
        if not self.allow_full_fetch:
            raise _FullFetchRefused("the full-metadata fetch is not allowed here")
        self.full_fetches += 1
        return [
            row for row in self.history
            if status is None or row.metadata.get("status") == status
        ]

    def count_generations(self, *, deployment_name, branch):
        return self.count


# --------------------------------------------------------------- health


def test_health_history_is_the_newest_ten_from_a_bounded_read() -> None:
    store = _Store([
        _checkpoint(i, status="blocked" if i % 3 == 0 else "published")
        for i in range(40)
    ])
    attempts = health._recent_attempts(
        store, deployment_name="demo", branch="default",
    )
    assert [row.checkpoint_id for row in attempts] == [
        f"cp:{i:03d}" for i in range(39, 29, -1)
    ]
    assert store.full_fetches == 0
    assert store.reads[0]["scan_window"] == health.HEALTH_HISTORY_WINDOW
    assert set(store.reads[0]["keys"]) == set(health.HEALTH_HISTORY_METADATA_KEYS)


def test_health_history_takes_the_named_fallback_when_the_window_is_short(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Other checkpoints filled the window: older attempts exist unseen."""
    store = _Store(
        [_checkpoint(i) for i in range(20)], other_rows=5, allow_full_fetch=True,
    )
    with caplog.at_level(logging.WARNING):
        attempts = health._recent_attempts(
            store, deployment_name="demo", branch="default",
        )
    assert store.full_fetches == 1
    assert [row.checkpoint_id for row in attempts] == [
        f"cp:{i:03d}" for i in range(19, 9, -1)
    ]
    assert "health_recent_attempts_outside_window" in caplog.text


def test_health_history_short_but_complete_needs_no_fallback() -> None:
    store = _Store([_checkpoint(i) for i in range(4)])
    attempts = health._recent_attempts(
        store, deployment_name="demo", branch="default",
    )
    assert len(attempts) == 4
    assert store.full_fetches == 0


def test_health_previous_published_is_read_bounded_with_its_count_keys() -> None:
    store = _Store([
        _checkpoint(1, node_count=10, edge_count=4),
        _checkpoint(2, status="blocked", node_count=99, edge_count=99),
        _checkpoint(3, node_count=12, edge_count=5),
    ])
    previous = health._previous_published(
        store, deployment_name="demo", branch="default", generation_id="cp:003",
    )
    assert previous is not None and previous.checkpoint_id == "cp:001"
    assert previous.metadata["node_count"] == 10
    assert previous.metadata["edge_count"] == 4
    assert store.reads[0]["status"] == "published"
    assert set(health.HEALTH_PREVIOUS_METADATA_KEYS) == {
        "node_count", "nodes_live", "edge_count", "edges_live",
    }
    assert set(store.reads[0]["keys"]) == set(health.HEALTH_PREVIOUS_METADATA_KEYS)


def test_health_first_published_generation_has_no_previous() -> None:
    store = _Store([_checkpoint(1), _checkpoint(2, status="blocked")])
    assert health._previous_published(
        store, deployment_name="demo", branch="default", generation_id="cp:001",
    ) is None
    assert store.full_fetches == 0


def test_health_previous_outside_the_window_takes_the_named_fallback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The pinned generation is older than the window reaches."""
    history = [_checkpoint(i, node_count=i) for i in range(80)]
    store = _Store(history, allow_full_fetch=True)
    with caplog.at_level(logging.WARNING):
        previous = health._previous_published(
            store, deployment_name="demo", branch="default",
            generation_id="cp:005",
        )
    assert store.full_fetches == 1
    assert previous is not None and previous.checkpoint_id == "cp:004"
    assert "health_previous_published_outside_window" in caplog.text


def test_health_previous_at_the_window_edge_takes_the_named_fallback() -> None:
    """Pinned is the OLDEST row of an exhausted window: its predecessor is unseen."""
    window = health.HEALTH_PREVIOUS_SCAN_WINDOW
    history = [_checkpoint(i) for i in range(window + 5)]
    pinned = history[5].checkpoint_id  # oldest row of the newest `window`
    store = _Store(history, allow_full_fetch=True)
    previous = health._previous_published(
        store, deployment_name="demo", branch="default", generation_id=pinned,
    )
    assert store.full_fetches == 1
    assert previous is not None and previous.checkpoint_id == "cp:004"


def test_health_history_total_is_an_exact_count_not_a_full_fetch() -> None:
    """`history_total` ("Showing 8 of N") counts the whole history, unbounded."""
    store = _Store([_checkpoint(i) for i in range(3)], count=88)
    assert health._history_total(
        store, deployment_name="demo", branch="default",
    ) == 88
    assert store.full_fetches == 0


def test_health_history_total_names_its_fallback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = _Store(
        [_checkpoint(i) for i in range(3)], count=None, allow_full_fetch=True,
    )
    with caplog.at_level(logging.WARNING):
        total = health._history_total(
            store, deployment_name="demo", branch="default",
        )
    assert total == 3
    assert store.full_fetches == 1
    assert "health_history_total_full_scan" in caplog.text


def test_compose_health_reaches_no_name_the_bounded_rewrite_removed() -> None:
    """The payload must not read a variable the old full fetch defined.

    `history_total` read `len(all_generations)`; routing the history through
    the bounded reader removed that name, and only a composed payload -- not
    the helpers -- would show it. Checked statically so it needs no database.
    """
    import inspect

    source = inspect.getsource(health.compose_health)
    for gone in ("all_generations", "published_generations", "list_generations"):
        assert gone not in source, gone


# ------------------------------------------------------------ knowledge


def test_overview_total_is_an_exact_count_not_a_full_fetch() -> None:
    store = _Store([_checkpoint(i) for i in range(3)], count=88)
    total = knowledge._generation_total(
        store, deployment_name="demo", branch="default",
    )
    assert total == 88
    assert store.full_fetches == 0


def test_overview_total_names_its_fallback_when_the_backend_cannot_count(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = _Store(
        [_checkpoint(i) for i in range(3)], count=None, allow_full_fetch=True,
    )
    with caplog.at_level(logging.WARNING):
        total = knowledge._generation_total(
            store, deployment_name="demo", branch="default",
        )
    assert total == 3
    assert store.full_fetches == 1
    assert "overview_generation_total_full_scan" in caplog.text


# ------------------------------------------------------ producer metrics


def _enricher_checkpoint(index: int, *names: str):
    return _checkpoint(
        index,
        target_generation_id=index,
        enricher_runs=[
            {"producer_id": f"_enricher:{name}", "status": "ok",
             "n_edges_emitted": index, "timings_ms": {"total": 10}}
            for name in names
        ],
    )


def _install(monkeypatch: pytest.MonkeyPatch, store: _Store) -> None:
    monkeypatch.setattr(
        producer_metrics.ChronosGraphStore, "from_connection",
        lambda *args, **kwargs: store,
    )
    monkeypatch.setattr(
        producer_metrics, "_edge_fact_history_for_name",
        lambda *args, **kwargs: [],
    )


def test_enricher_runs_read_a_span_bounded_projected_window(monkeypatch) -> None:
    store = _Store([_enricher_checkpoint(i, "first") for i in range(48)])
    _install(monkeypatch, store)
    start = datetime(2026, 9, 2, 0, tzinfo=timezone.utc)  # hour 24
    end = start + timedelta(hours=12)
    runs = producer_metrics._selected_enricher_runs(
        object(), deployment_name="demo",
        session=SimpleNamespace(branch_name="default"),
        names=["first"], start=start, end=end, limit=50,
    )
    assert [row["n_edges_emitted"] for row in runs["first"]] == list(range(24, 37))
    assert store.full_fetches == 0
    read = store.reads[0]
    assert read["since"] is not None and read["since"] < start.isoformat()
    assert set(read["keys"]) == set(producer_metrics.ENRICHER_RUN_METADATA_KEYS)


def test_enricher_runs_take_the_named_fallback_when_the_window_is_exhausted(
    monkeypatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """More in-span checkpoints than the window: older runs exist unseen."""
    store = _Store(
        [_enricher_checkpoint(i, "first") for i in range(300)],
        allow_full_fetch=True,
    )
    _install(monkeypatch, store)
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=30)
    with caplog.at_level(logging.WARNING):
        runs = producer_metrics._selected_enricher_runs(
            object(), deployment_name="demo",
            session=SimpleNamespace(branch_name="default"),
            names=["first", "never_ran"], start=start, end=end, limit=50,
        )
    assert store.full_fetches == 1
    assert "producer_metrics_enricher_runs_outside_window" in caplog.text
    assert len(runs["first"]) == 50
    assert runs["first"][-1]["n_edges_emitted"] == 299


def test_enricher_runs_satisfied_inside_the_window_need_no_fallback(
    monkeypatch,
) -> None:
    store = _Store([_enricher_checkpoint(i, "first") for i in range(300)])
    _install(monkeypatch, store)
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    runs = producer_metrics._selected_enricher_runs(
        object(), deployment_name="demo",
        session=SimpleNamespace(branch_name="default"),
        names=["first"], start=start, end=start + timedelta(days=30), limit=50,
    )
    assert store.full_fetches == 0
    assert len(runs["first"]) == 50
