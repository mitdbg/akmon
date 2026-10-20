"""Freshness-honesty for graph reads (worktree-access-tiers)."""

from __future__ import annotations

import datetime

from okg.substrate.mcp.freshness import (
    null_ordering_warning,
    summarize_watermarks,
)

NOW = datetime.datetime(2026, 6, 10, 12, 0, tzinfo=datetime.timezone.utc)


def test_stale_sources_warn_with_ages() -> None:
    rows = [
        ("sessions", "2026-06-09T21:00:00+00:00"),   # 15h - stale
        ("pact", "2026-06-10T11:30:00+00:00"),        # 0.5h - fresh
        ("code_graph", None),                         # never - stale
    ]
    out = summarize_watermarks(rows, now=NOW, threshold_h=6.0)
    assert out["stale_count"] == 2
    stale_sources = {s["source"] for s in out["stale"]}
    assert stale_sources == {"sessions", "code_graph"}
    assert "15.0h" in out["warning"]
    assert "never" in out["warning"]
    assert "not the present" in out["warning"]


def test_stale_examples_are_one_fixed_order_whatever_the_row_order() -> None:
    """#3635: the five stale sources named are the same on every server and
    call. Never polled first, then oldest first by exact age (sources one
    run polled round to the same age), then by name; the database returns
    the rows in no fixed order."""
    import random

    rows = [(f"src{i:02d}", "2026-06-08T00:00:00+00:00") for i in range(8)]
    rows += [("never-b", None), ("never-a", None)]
    # 0.04h older than the rest: rounds to the same 60.0h, still sorts first.
    rows.append(("older", "2026-06-07T23:57:36+00:00"))
    expected = ["never-a", "never-b", "older", "src00", "src01"]
    for seed in range(20):
        shuffled = rows[:]
        random.Random(seed).shuffle(shuffled)
        out = summarize_watermarks(shuffled, now=NOW, threshold_h=6.0)
        assert [s["source"] for s in out["stale"]] == expected
        assert out["stale_count"] == 11
    assert out["stale"][2]["age_hours"] == out["stale"][3]["age_hours"] == 60.0


def test_one_session_prints_the_stale_notice_once_whatever_the_row_order() -> None:
    """#3635: in lane X's run 46 of 152 stale notices were repeats in one
    session, because the five names changed with the row order."""
    import random

    from okg.substrate.mcp.result_text import render

    rows = [(f"src{i:02d}", "2026-06-08T00:00:00+00:00") for i in range(96)]
    session: dict = {}
    printed = 0
    for seed in range(10):
        shuffled = rows[:]
        random.Random(seed).shuffle(shuffled)
        page = {
            "results": [], "total_count": 0, "method": "lexical",
            "pin_scope": "transport_session",
            "source_freshness": summarize_watermarks(shuffled, now=NOW, threshold_h=6.0),
        }
        text = render(page, tool="search", session_state=session)
        printed += "! 96 of 96 sources not polled for over 6h (oldest 60h)" in text
    assert printed == 1


def test_all_fresh_has_no_warning() -> None:
    rows = [("pact", "2026-06-10T11:30:00+00:00")]
    out = summarize_watermarks(rows, now=NOW, threshold_h=6.0)
    assert out["stale_count"] == 0
    assert "warning" not in out


def test_null_sort_key_flagged() -> None:
    # The 2026-06-10 blunder verbatim: ORDER BY an attr that is NULL on
    # every row, read as recency.
    sql = (
        "SELECT n.node_id, n.attrs->>'last_active_at' AS last_active_at "
        "FROM okg.v_nodes n ORDER BY n.attrs->>'last_active_at' DESC"
    )
    warning = null_ordering_warning(
        sql, ["node_id", "last_active_at"],
        [["a", None], ["b", None]],
    )
    assert warning is not None
    assert "arbitrary sample" in warning


def test_populated_sort_key_not_flagged() -> None:
    sql = "SELECT node_id, ended_at FROM okg.v_nodes ORDER BY ended_at DESC"
    assert null_ordering_warning(
        sql, ["node_id", "ended_at"], [["a", "2026-06-09"], ["b", None]],
    ) is None


def test_unselected_sort_key_not_flagged() -> None:
    sql = "SELECT node_id FROM okg.v_nodes ORDER BY created_at"
    assert null_ordering_warning(sql, ["node_id"], [["a"]]) is None


# --- #3666: only a polled source can be stale; the cache is per deployment ---


_REGISTRY = {
    "sources": {
        "tickets": {
            "ownership_id": "demo.tickets",
            "sync": {"triggers": ["poll", "manual"], "poll": {"interval_seconds": 300}},
        },
        "papers": {"ownership_id": "demo.papers", "sync": {"triggers": ["manual"]}},
        "code": {
            "ownership_id": "demo.code",
            "sync": {"triggers": ["fs_event", "manual"], "watch": {"roots": ["."]}},
        },
        "parked": {
            "ownership_id": "demo.parked",
            "enabled_by_default": False,
            "sync": {"triggers": ["poll"], "poll": {"interval_seconds": 300}},
        },
    },
}


class _Cursor:
    def __init__(self, rows: list) -> None:
        self.rows = rows

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, *_args: object, **_kwargs: object) -> None:
        return None

    def fetchall(self) -> list:
        return list(self.rows)


class _Connection:
    def __init__(self, rows: list) -> None:
        self.rows = rows

    def cursor(self) -> _Cursor:
        return _Cursor(self.rows)


def _expire_cache(monkeypatch, freshness, at: float) -> None:
    """A clock far past every cached entry, so a summary cached by an earlier
    test is expired, on a copy of the cache that is put back afterwards (an
    entry stamped with this clock must not outlive the test)."""
    import copy
    import types

    monkeypatch.setattr(freshness, "_cache", copy.deepcopy(freshness._cache))
    monkeypatch.setattr(freshness, "time", types.SimpleNamespace(monotonic=lambda: at))


def _old_rows(*names: str) -> list:
    # Every source last ran 100 hours ago.
    completed = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=100)
    ).isoformat()
    return [(name, {"completed_at": completed}) for name in names]


def test_a_source_with_no_poll_schedule_is_never_called_stale(monkeypatch) -> None:
    from okg.substrate.mcp import freshness, source_admin

    monkeypatch.setattr(
        source_admin, "_load_registry_yaml", lambda deployment: ("x", _REGISTRY),
    )
    _expire_cache(monkeypatch, freshness, 1e12)
    out = freshness.source_freshness(
        _Connection(_old_rows("demo.tickets", "demo.papers", "demo.code", "demo.parked")),
        "demo",
    )
    # Before #3666 all four were named stale; only the polled one can be.
    assert [s["source"] for s in out["stale"]] == ["demo.tickets"]
    assert out["stale_count"] == 1
    assert out["unscheduled"] == 3
    assert out["sources_checked"] == 4
    assert "papers" not in out["warning"] and "code" not in out["warning"]


def test_unknown_schedules_judge_every_source_and_say_so(monkeypatch) -> None:
    """An unreadable registry never reads as fresh: every source is judged,
    as before #3666, and the reply says the schedules were unknown."""
    from okg.substrate.mcp import freshness, source_admin
    from okg.substrate.mcp.result_text import render

    def _missing(deployment: str):
        raise FileNotFoundError("no registry")

    monkeypatch.setattr(source_admin, "_load_registry_yaml", _missing)
    _expire_cache(monkeypatch, freshness, 2e12)
    out = freshness.source_freshness(_Connection(_old_rows("demo.papers")), "demo")
    assert out["stale_count"] == 1
    assert out["schedules"] == "unknown"
    assert "unscheduled" not in out
    assert "poll schedules unknown" in out["warning"]
    page = {"results": [], "total_count": 0, "method": "lexical", "source_freshness": out}
    text = render(page, tool="search", session_state={})
    assert "! 1 of 1 sources not polled for over" in text
    assert "poll schedules unknown" in text


def test_the_freshness_cache_never_answers_for_another_deployment(monkeypatch) -> None:
    from okg.substrate.mcp import freshness, source_admin

    monkeypatch.setattr(
        source_admin, "_load_registry_yaml", lambda deployment: ("x", _REGISTRY),
    )
    _expire_cache(monkeypatch, freshness, 3e12)
    first = freshness.source_freshness(_Connection(_old_rows("demo.tickets")), "demo")
    second = freshness.source_freshness(_Connection([]), "other")
    assert first["stale_count"] == 1
    # Before #3666 the second deployment was served the first one's summary.
    assert second["sources_checked"] == 0
    assert second["stale_count"] == 0
    # Within the TTL each deployment still gets its own cached summary.
    assert freshness.source_freshness(_Connection([]), "demo") is first
    # The same deployment name in another database is another graph.
    import types

    elsewhere = _Connection([])
    elsewhere.info = types.SimpleNamespace(host="h", port=1, dbname="other_db")
    assert freshness.source_freshness(elsewhere, "demo")["sources_checked"] == 0
