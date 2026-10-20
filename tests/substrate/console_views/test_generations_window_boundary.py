"""The oldest row of a BOUNDED window is not the first attempt of a history.

No database: the defect is entirely in what the composer infers from a row
having no predecessor in hand, and a fake store pins that with no publish at
all.

`compose_generations` reads the newest checkpoints through
`ChronosGraphStore.recent_generations`, which stops at a row cap. It then walks
the rows it got and hands each one the row before it as the baseline its delta
is measured against. The oldest row of the window has none -- and the composer
read that as "this is the first generation ever recorded", whose delta IS its
full counts against an empty graph. On any history longer than the window that
is a number nobody measured, printed in the place a measurement goes: the
oldest row of every page claimed to have added the whole graph.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from okg.substrate.console_views.generations import (
    NO_PREDECESSOR_IN_WINDOW,
    compose_generations,
)
from okg.substrate.mcp.session import Session

# A real Session, not a stand-in: the composer re-pins one per checkpoint with
# `dataclasses.replace`, which refuses anything that is not a dataclass.
SESSION = Session(
    deployment_name="demo",
    generation_id="cp:published:40",
    catalog_version_id=1,
    db_fingerprint="fingerprint-for-a-test-with-no-database",
)


class _Store:
    """The slice of ChronosGraphStore `compose_generations` uses.

    It honours the scan window the composer asks for, exactly as the real
    bounded reader does, and reports `window_exhausted` the same way: the scan
    filled its cap, so there may be older checkpoints it never looked at.
    """

    def __init__(self, count: int) -> None:
        self.history = [
            SimpleNamespace(
                checkpoint_id=f"cp:published:{index}",
                created_at=f"2026-09-{(index % 28) + 1:02d}T0{index % 10}:00:00+00:00",
                metadata={
                    "kind": "okg_generation",
                    "deployment_name": "demo",
                    "status": "published",
                },
            )
            for index in range(count)
        ]

    def recent_generations(self, *, scan_window=None, **_kwargs):
        from okg.substrate.graph_store_internal.generations import (
            RecentGenerations,
        )

        window = 200 if scan_window is None else scan_window
        newest_first = list(reversed(self.history))[:window]
        return RecentGenerations(
            newest_first,
            scanned=len(newest_first),
            scan_window=window,
            window_exhausted=len(newest_first) >= window,
        )

    def count_generation_checkpoints(self, **_kwargs):
        return len(self.history)


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch):
    def install(count: int) -> _Store:
        made = _Store(count)
        monkeypatch.setattr(
            "okg.substrate.console_views.generations.ChronosGraphStore",
            SimpleNamespace(from_connection=lambda *_a, **_k: made),
        )
        monkeypatch.setattr(
            "okg.substrate.console_views.generations"
            ".require_console_count_rollup_enabled",
            lambda **_kwargs: None,
        )
        # Every checkpoint holds the same counts, so a real delta is zero and
        # a fabricated first-publish baseline is unmistakable: 1,000 records.
        monkeypatch.setattr(
            "okg.substrate.console_views.generations"
            ".counts_from_projected_metadata",
            lambda *_a, **_k: (1000, 2000),
        )
        monkeypatch.setattr(
            "okg.substrate.console_views.generations.assert_session_matches",
            lambda *_a, **_k: None,
        )
        return made

    return install


def _compose(*, limit: int, cursor: str | None = None):
    return compose_generations(
        object(),
        deployment_name="demo",
        session=SESSION,
        limit=limit,
        cursor=cursor,
    )


def _oldest(payload):
    return payload["generations"][-1]


def test_the_oldest_row_of_an_exhausted_window_reports_no_baseline(store) -> None:
    """A window shorter than the history leaves the question open, so it says so.

    A CURSOR page, because that is the only page that can reach the oldest
    row of the window: an uncursored read sizes its window to twice the page
    it serves, so the window always holds rows older than the page. A cursor
    read opens the window to the reader's own cap (200), and a 260-checkpoint
    history exhausts it -- so the oldest row the window holds has an older
    attempt behind it that this read never saw.
    """
    store(260)
    payload = _compose(limit=3, cursor="cp:published:63")
    assert payload["window_exhausted"] is True
    row = _oldest(payload)
    assert row["delta_available"] is False
    assert row["delta_unavailable_reason"] == NO_PREDECESSOR_IN_WINDOW
    # And no number stands in the place the measurement would have gone.
    assert row["nodes_added"] is None
    assert row["edges_added"] is None
    assert row["nodes_retracted"] is None
    assert row["edges_retracted"] is None


def test_the_first_attempt_of_a_complete_history_still_measures_its_delta(
    store,
) -> None:
    """The other side: an empty baseline IS the fact for the first attempt.

    Five checkpoints and a 32-row window, so the window reached the beginning
    of the history. The oldest row really is the first attempt this branch
    recorded, its delta is its full counts against nothing, and withholding it
    would be the opposite defect.
    """
    store(5)
    payload = _compose(limit=10)
    assert payload["window_exhausted"] is False
    row = _oldest(payload)
    assert row["delta_available"] is True
    assert row["delta_unavailable_reason"] is None
    assert row["nodes_added"] == 1000
    assert row["edges_added"] == 2000


def test_a_row_with_a_predecessor_in_the_window_is_untouched(store) -> None:
    """Every other row measures its delta as it always did."""
    store(260)
    payload = _compose(limit=3, cursor="cp:published:63")
    for row in payload["generations"][:-1]:
        assert row["delta_available"] is True
        assert row["nodes_added"] == 0
        assert row["edges_added"] == 0
