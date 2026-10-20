"""Database-free tests of the shared LLM usage read (#2573 slice 2)."""
from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.telemetry.llm_usage import (
    LlmUsage,
    LlmUsageFilters,
    format_cost_usd,
    llm_usage_summary,
)
from okg.substrate.unknown import Unknown


class _RecordingCursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.query: Any = None
        self.params: Any = None

    def execute(self, query: Any, params: Any = None) -> None:
        self.query = query
        self.params = params

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


def _usage(calls: int, unpriced: int, priced: float = 0.25) -> LlmUsage:
    return LlmUsage(group={}, calls=calls, unpriced_calls=unpriced, priced_cost_usd=priced)


@pytest.mark.parametrize(
    ("calls", "unpriced", "priced", "text"),
    [
        (40, 0, 1.2041, "$1.204100"),
        (40, 3, 1.2041, "cost unknown: 3 of 40 unpriced (priced $1.204100)"),
        (40, 40, 0.0, "cost unknown: 40 of 40 unpriced"),
        (0, 0, 0.0, "$0.000000"),
    ],
)
def test_format_cost_follows_decision_d3(
    calls: int, unpriced: int, priced: float, text: str
) -> None:
    assert format_cost_usd(calls, unpriced, priced) == text


def test_cost_is_unknown_when_any_call_is_unpriced() -> None:
    assert _usage(2, 0).cost_usd == 0.25
    assert _usage(2, 1).cost_usd == Unknown("unpriced_calls:1_of_2")
    assert _usage(2, 2, 0.0).cost_usd == Unknown("unpriced_calls:2_of_2")
    with pytest.raises(TypeError):
        f"{_usage(2, 1).cost_usd:.6f}"


def test_empty_group_is_a_real_zero_but_its_latency_is_unknown() -> None:
    empty = _usage(0, 0, 0.0)
    assert empty.cost_usd == 0.0
    assert empty.tokens("total_tokens") == 0
    assert empty.latency_avg_ms == Unknown("no_calls")


def test_missing_tokens_and_latency_are_unknown() -> None:
    usage = LlmUsage(
        group={},
        calls=3,
        unpriced_calls=0,
        priced_cost_usd=0.1,
        recorded_tokens={"total_tokens": 30},
        missing_tokens={"total_tokens": 1},
        latency_missing=2,
        recorded_latency_avg_ms=12.0,
    )
    assert usage.tokens("total_tokens") == Unknown("total_tokens_missing:1_of_3")
    assert usage.latency_avg_ms == Unknown("latency_ms_missing:2_of_3")


def test_query_counts_nulls_and_never_coalesces() -> None:
    row = (2, 1, 0.25, 0, 20, 0, 10, 0, 30, 0, 20.0)
    cur = _RecordingCursor([("m", *row)])
    (usage,) = llm_usage_summary(
        cur,
        filters=LlmUsageFilters(
            deployment="d", generation_id="g", caller_prefix="inv_x:"
        ),
        group_by=("model",),
    )
    text = cur.query.as_string()
    assert "coalesce" not in text.lower()
    assert "count(*) FILTER (WHERE cost_usd IS NULL)" in text
    assert 'GROUP BY "model"' in text
    # The prefix is matched literally: LIKE wildcards in it are escaped.
    assert cur.params == ["d", "g", "inv\\_x:%"]
    assert usage.group == {"model": "m"}
    assert usage.unpriced_calls == 1
    assert usage.as_payload()["cost_usd"] == Unknown("unpriced_calls:1_of_2")


def test_group_by_refuses_an_unlisted_column() -> None:
    with pytest.raises(ValueError, match="cannot group by"):
        llm_usage_summary(
            _RecordingCursor([]),
            filters=LlmUsageFilters(),
            group_by=("cost_usd; DROP TABLE x",),
        )
