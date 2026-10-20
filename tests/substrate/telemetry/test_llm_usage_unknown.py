"""Promise test for #2573 slice 2 (PACT ``unknown-s2-llm-cost``, closes #1739).

An unpriced model call (``okg.llm_calls.cost_usd IS NULL``) used to read back
as ``$0``: seven queries summed the column under ``COALESCE(SUM(cost_usd), 0)``.
Every reader of those queries must now

* report ``unpriced_calls`` for a group holding one priced and one unpriced
  call, and never a total that looks complete; and
* report an all-unpriced group as unknown, never as ``0``.

The readers: ``okg metrics --llm`` (JSON and text), the LLM-judge
``llm_call_stats`` block, the telemetry metrics catalog, the work ledger's
model-call spend, and the viz system snapshot's ``llm_spend`` (totals, top
models, top callers).
"""
from __future__ import annotations

import json
import uuid
from collections.abc import Iterator

import psycopg
import pytest

pytestmark = pytest.mark.postgres

_GENERATION = "s2-generation-1"
_CALLER = "invariants:s2_contradiction"
_MODEL = "s2-model"
_PRICED_USD = 0.25


def _seed(dsn: str, deployment: str, *, costs: list[float | None]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM okg.llm_calls WHERE deployment_name = %s", (deployment,)
        )
        for cost in costs:
            cur.execute(
                """
                INSERT INTO okg.llm_calls
                    (deployment_name, caller, model, prompt_tokens,
                     completion_tokens, total_tokens, cost_usd, latency_ms,
                     success, generation_id)
                VALUES (%s, %s, %s, 10, 5, 15, %s, 20, TRUE, %s)
                """,
                (deployment, _CALLER, _MODEL, cost, _GENERATION),
            )


@pytest.fixture
def mixed(clean_runtime: str) -> Iterator[tuple[str, str]]:
    """One group: one priced call and one unpriced call."""
    deployment = f"s2-mixed-{uuid.uuid4().hex[:8]}"
    _seed(clean_runtime, deployment, costs=[_PRICED_USD, None])
    yield clean_runtime, deployment


@pytest.fixture
def dark(clean_runtime: str) -> Iterator[tuple[str, str]]:
    """One group in which every call is unpriced."""
    deployment = f"s2-dark-{uuid.uuid4().hex[:8]}"
    _seed(clean_runtime, deployment, costs=[None, None])
    yield clean_runtime, deployment


# --------------------------------------------------------------------------- #
# Readers, each returning its payload as it leaves the process.               #
# --------------------------------------------------------------------------- #
def _metrics_json(dsn: str, deployment: str, capsys) -> dict:  # noqa: ANN001
    from okg.substrate.cli.main import main

    rc = main([
        "metrics", "--llm", "--since", "1h", "--deployment", deployment,
        "--dsn", dsn, "--json",
    ])
    out = capsys.readouterr().out
    assert rc == 0, out
    return json.loads(out)


def _metrics_text(dsn: str, deployment: str, capsys) -> str:  # noqa: ANN001
    from okg.substrate.cli.main import main

    rc = main([
        "metrics", "--llm", "--since", "1h", "--deployment", deployment,
        "--dsn", dsn,
    ])
    out = capsys.readouterr().out
    assert rc == 0, out
    return out


def _judge_stats(dsn: str, deployment: str) -> dict:
    from okg.substrate.catalog.invariants_llm import _load_llm_call_stats

    return _load_llm_call_stats(dsn, deployment, generation_id=_GENERATION)


def _catalog_cost(dsn: str, deployment: str) -> dict:
    from okg.substrate.telemetry.catalog import telemetry_metrics_catalog

    with psycopg.connect(dsn) as conn:
        catalog = telemetry_metrics_catalog(
            conn, deployment=deployment, since_seconds=3600
        )
    return {m["name"]: m for m in catalog["metrics"]}["llm.cost.usd"]


def _ledger_measure(dsn: str, deployment: str):  # noqa: ANN202
    from okg.substrate.telemetry.work_ledger import model_call_spend

    with psycopg.connect(dsn) as conn:
        return model_call_spend(
            conn, deployment=deployment, generation_id=_GENERATION
        ).as_measure()


def _viz_spend(dsn: str, deployment: str) -> dict:
    from okg.substrate.unknown import to_wire
    from okg.viz import reads_system

    with psycopg.connect(dsn) as conn:
        # The viz read routes pass every response through to_wire.
        return to_wire({"llm_spend": reads_system.fetch_llm_spend(conn, deployment)})


# --------------------------------------------------------------------------- #
# One priced + one unpriced call per group.                                   #
# --------------------------------------------------------------------------- #
def test_metrics_llm_json_counts_the_unpriced_call(mixed, capsys) -> None:  # noqa: ANN001
    payload = _metrics_json(*mixed, capsys)
    (row,) = payload["rows"]
    for block in (row, payload["totals"]):
        assert block["calls"] == 2
        assert block["unpriced_calls"] == 1
        assert block["priced_cost_usd"] == pytest.approx(_PRICED_USD)
        assert block["cost_usd"] is None
    assert payload["unknown_reasons"] == {
        "rows.0.cost_usd": "unpriced_calls:1_of_2",
        "totals.cost_usd": "unpriced_calls:1_of_2",
    }


def test_metrics_llm_text_never_prints_a_complete_total(mixed, capsys) -> None:  # noqa: ANN001
    text = _metrics_text(*mixed, capsys)
    assert "cost unknown: 1 of 2 unpriced (priced $0.250000)" in text, text
    assert "$0.250000\n" not in text, text


def test_judge_llm_call_stats_count_the_unpriced_call(mixed) -> None:  # noqa: ANN001
    stats = _judge_stats(*mixed)
    block = stats["by_invariant"]["s2_contradiction"]
    for scope in (stats, block):
        assert scope["calls"] == 2
        assert scope["unpriced_calls"] == 1
        assert scope["priced_cost_usd"] == pytest.approx(_PRICED_USD)
        assert scope["total_cost_usd"] is None
    assert stats["unknown_reasons"]["total_cost_usd"] == "unpriced_calls:1_of_2"


def test_metrics_catalog_marks_cost_unknown(mixed) -> None:  # noqa: ANN001
    metric = _catalog_cost(*mixed)
    assert metric["value"] is None
    assert metric["status"] == "unknown"
    assert metric["unavailable_reason"] == "unpriced_calls:1_of_2"
    assert metric["labels"]["unpriced_calls"] == 1
    assert metric["labels"]["priced_cost_usd"] == pytest.approx(_PRICED_USD)


def test_work_ledger_model_calls_cost_is_a_loud_null(mixed) -> None:  # noqa: ANN001
    measure = _ledger_measure(*mixed)
    assert measure.model_calls == 2
    assert measure.cost_usd is None
    assert measure.detail["unpriced_calls"] == 1
    assert measure.detail["priced_cost_usd"] == pytest.approx(_PRICED_USD)
    # A declared unit price must not paper over the measured-but-unpriced cost.
    assert measure.with_unit_price(1.0).cost_usd is None


def test_viz_llm_spend_counts_the_unpriced_call(mixed) -> None:  # noqa: ANN001
    wire = _viz_spend(*mixed)
    spend = wire["llm_spend"]
    rows = [spend, spend["top_models"][0], spend["top_callers"][0]]
    for row in rows:
        assert row["calls"] == 2
        assert row["unpriced_calls"] == 1
        assert row["priced_cost_usd"] == pytest.approx(_PRICED_USD)
        assert row["cost_usd"] is None
    assert wire["unknown_reasons"] == {
        "llm_spend.cost_usd": "unpriced_calls:1_of_2",
        "llm_spend.top_callers.0.cost_usd": "unpriced_calls:1_of_2",
        "llm_spend.top_models.0.cost_usd": "unpriced_calls:1_of_2",
    }


# --------------------------------------------------------------------------- #
# A group in which every call is unpriced is not 0.                            #
# --------------------------------------------------------------------------- #
def test_all_unpriced_group_is_not_zero_in_any_reader(dark, capsys) -> None:  # noqa: ANN001
    dsn, deployment = dark

    data = _metrics_json(dsn, deployment, capsys)
    assert data["rows"][0]["cost_usd"] is None
    assert data["rows"][0]["unpriced_calls"] == 2
    assert data["totals"]["cost_usd"] is None

    text = _metrics_text(dsn, deployment, capsys)
    assert "cost unknown: 2 of 2 unpriced" in text, text
    assert "$0.000000" not in text, text

    stats = _judge_stats(dsn, deployment)
    assert stats["total_cost_usd"] is None
    assert stats["unpriced_calls"] == 2

    metric = _catalog_cost(dsn, deployment)
    assert metric["value"] is None
    assert metric["unavailable_reason"] == "unpriced_calls:2_of_2"

    measure = _ledger_measure(dsn, deployment)
    assert measure.cost_usd is None
    assert measure.detail["unpriced_calls"] == 2

    spend = _viz_spend(dsn, deployment)["llm_spend"]
    assert spend["cost_usd"] is None
    assert spend["top_models"][0]["cost_usd"] is None
    assert spend["top_callers"][0]["cost_usd"] is None
