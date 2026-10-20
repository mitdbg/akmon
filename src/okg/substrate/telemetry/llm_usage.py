"""One read of ``okg.llm_calls`` usage that never reads a missing value as zero.

Issue #2573 slice 2 (PACT ``unknown-s2-llm-cost``, closes #1739). Seven
queries summed ``okg.llm_calls`` under ``COALESCE(SUM(cost_usd), 0)``, so a
call from a model with no price read back as ``$0`` and a group of such calls
read as free. ``cost_usd``, the three token columns and ``latency_ms`` are all
nullable; this module is the one query over them.

``llm_usage_summary`` counts the rows missing each measure alongside its sum.
A measure is a number only when no row in the group is missing it; otherwise
it is ``Unknown("<measure>:<n>_of_<m>")`` and the payload also carries the sum
of the rows that have it (``priced_cost_usd`` is a lower bound) and the count
of rows that do not. An empty group is a real zero for sums and counts; its
average latency is ``Unknown("no_calls")``.

The SQL is composed with ``psycopg.sql``; nothing is string-formatted.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from psycopg import sql

from okg.substrate.unknown import Unknown

__all__ = [
    "GROUP_COLUMNS",
    "LlmUsage",
    "LlmUsageFilters",
    "format_cost_usd",
    "llm_usage_summary",
]

#: Columns a caller may group by.
GROUP_COLUMNS = frozenset({"deployment_name", "caller", "model"})

_TOKEN_MEASURES = ("prompt_tokens", "completion_tokens", "total_tokens")


@dataclass(frozen=True)
class LlmUsageFilters:
    """Which ``okg.llm_calls`` rows to read. ``None`` means "do not filter"."""

    deployment: str | None = None
    #: ``ts >= now() - since_seconds``.
    since_seconds: int | None = None
    #: ``ts >= window_start`` and ``ts < window_end``.
    window_start: datetime | None = None
    window_end: datetime | None = None
    generation_id: str | None = None
    #: ``caller LIKE <prefix>%`` (the prefix is matched literally).
    caller_prefix: str | None = None
    success_only: bool = True


@dataclass(frozen=True)
class LlmUsage:
    """Usage for one group (or the whole filtered set when ungrouped)."""

    group: Mapping[str, Any]
    calls: int
    unpriced_calls: int
    priced_cost_usd: float
    recorded_tokens: Mapping[str, int] = field(default_factory=dict)
    missing_tokens: Mapping[str, int] = field(default_factory=dict)
    latency_missing: int = 0
    recorded_latency_avg_ms: float | None = None

    @property
    def cost_usd(self) -> float | Unknown:
        if self.unpriced_calls:
            return Unknown(f"unpriced_calls:{self.unpriced_calls}_of_{self.calls}")
        return self.priced_cost_usd

    def tokens(self, measure: str) -> int | Unknown:
        missing = int(self.missing_tokens.get(measure, 0))
        if missing:
            return Unknown(f"{measure}_missing:{missing}_of_{self.calls}")
        return int(self.recorded_tokens.get(measure, 0))

    @property
    def latency_avg_ms(self) -> float | Unknown:
        if self.calls == 0:
            return Unknown("no_calls")
        if self.latency_missing:
            return Unknown(
                f"latency_ms_missing:{self.latency_missing}_of_{self.calls}"
            )
        return float(self.recorded_latency_avg_ms or 0.0)

    def as_payload(self, *, cost_key: str = "cost_usd") -> dict[str, Any]:
        """The wire-ready fields (an ``Unknown`` value goes through ``to_wire``)."""
        payload: dict[str, Any] = dict(self.group)
        payload["calls"] = self.calls
        for measure in ("prompt_tokens", "completion_tokens"):
            payload[measure] = self.tokens(measure)
        payload[cost_key] = self.cost_usd
        payload["priced_cost_usd"] = self.priced_cost_usd
        payload["unpriced_calls"] = self.unpriced_calls
        return payload


def _where(filters: LlmUsageFilters) -> tuple[sql.Composable, list[Any]]:
    clauses: list[sql.Composable] = []
    params: list[Any] = []

    def add(fragment: str, *values: Any) -> None:
        clauses.append(sql.SQL(fragment).format(*[sql.Placeholder()] * len(values)))
        params.extend(values)

    if filters.success_only:
        clauses.append(sql.SQL("success"))
    if filters.deployment is not None:
        add("deployment_name = {}", filters.deployment)
    if filters.since_seconds is not None:
        add("ts >= now() - make_interval(secs => {})", int(filters.since_seconds))
    if filters.window_start is not None:
        add("ts >= {}", filters.window_start)
    if filters.window_end is not None:
        add("ts < {}", filters.window_end)
    if filters.generation_id is not None:
        add("generation_id = {}", str(filters.generation_id))
    if filters.caller_prefix is not None:
        escaped = (
            filters.caller_prefix.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        add("caller LIKE {}", escaped + "%")
    if not clauses:
        return sql.SQL("TRUE"), params
    return sql.SQL(" AND ").join(clauses), params


def _measure_columns() -> list[sql.Composable]:
    cols: list[sql.Composable] = [
        sql.SQL("count(*)"),
        sql.SQL("count(*) FILTER (WHERE cost_usd IS NULL)"),
        sql.SQL("sum(cost_usd)"),
    ]
    for measure in _TOKEN_MEASURES:
        ident = sql.Identifier(measure)
        cols.append(sql.SQL("count(*) FILTER (WHERE {} IS NULL)").format(ident))
        cols.append(sql.SQL("sum({})").format(ident))
    cols.append(sql.SQL("count(*) FILTER (WHERE latency_ms IS NULL)"))
    cols.append(sql.SQL("avg(latency_ms)"))
    return cols


def llm_usage_summary(
    cur: Any,
    *,
    filters: LlmUsageFilters,
    group_by: Sequence[str] = (),
) -> list[LlmUsage]:
    """Read usage from ``okg.llm_calls``, one :class:`LlmUsage` per group.

    Ungrouped, it returns exactly one row (``calls == 0`` when nothing
    matched). Grouped, it returns one row per group present, ordered by the
    group columns. ``group_by`` accepts only :data:`GROUP_COLUMNS`.
    """
    group = tuple(group_by)
    unknown_columns = sorted(set(group) - GROUP_COLUMNS)
    if unknown_columns:
        raise ValueError(f"llm_usage_summary cannot group by {unknown_columns}")
    where, params = _where(filters)
    group_idents = [sql.Identifier(col) for col in group]
    select = sql.SQL(", ").join([*group_idents, *_measure_columns()])
    query = sql.SQL("SELECT {select} FROM okg.llm_calls WHERE {where}").format(
        select=select, where=where
    )
    if group:
        group_list = sql.SQL(", ").join(group_idents)
        query = sql.SQL("{} GROUP BY {} ORDER BY {}").format(
            query, group_list, group_list
        )
    cur.execute(query, params)
    return [_row(group, row) for row in cur.fetchall()]


def _row(group: tuple[str, ...], row: Sequence[Any]) -> LlmUsage:
    width = len(group)
    keys = dict(zip(group, row[:width]))
    values = list(row[width:])
    calls, unpriced, cost = values[0], values[1], values[2]
    recorded: dict[str, int] = {}
    missing: dict[str, int] = {}
    for index, measure in enumerate(_TOKEN_MEASURES):
        missing[measure] = int(values[3 + 2 * index])
        total = values[4 + 2 * index]
        recorded[measure] = int(total) if total is not None else 0
    latency_missing, latency_avg = values[9], values[10]
    return LlmUsage(
        group=keys,
        calls=int(calls),
        unpriced_calls=int(unpriced),
        priced_cost_usd=float(cost) if cost is not None else 0.0,
        recorded_tokens=recorded,
        missing_tokens=missing,
        latency_missing=int(latency_missing),
        recorded_latency_avg_ms=(
            float(latency_avg) if latency_avg is not None else None
        ),
    )


def format_cost_usd(
    calls: int, unpriced_calls: int, priced_cost_usd: float, *, digits: int = 6
) -> str:
    """Render a cost for a person (decision D3 of #2573).

    ``$1.204100`` when every call is priced; ``cost unknown: 3 of 40
    unpriced (priced $1.204100)`` when some are not; ``cost unknown: 40 of
    40 unpriced`` when none is, so an unpriced group never reads as ``$0``.
    """
    priced = f"${priced_cost_usd:.{digits}f}"
    if not unpriced_calls:
        return priced
    text = f"cost unknown: {unpriced_calls} of {calls} unpriced"
    if unpriced_calls < calls:
        text += f" (priced {priced})"
    return text
