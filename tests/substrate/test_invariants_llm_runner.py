"""End-to-end coverage tests for `LLMJudgeInvariantRunner`.

The pure helpers (`parse_verdict`, `_strip_code_fences`,
`_validate_spans_against_bodies`) are covered by
`test_invariants_llm.py`. This file covers the runner class:
candidate fetching, prompt rendering, LLM dispatch via a stub,
verdict parsing + counting, span validation, and the Result
shape.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from tests.substrate._db_helpers import ensure_published_generation
from okg.substrate.catalog.invariants_llm import (
    JudgeInvariantResult,
    LLMJudgeInvariantRunner,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact

pytestmark = pytest.mark.postgres


# ---------- canned-verdict stub ----------


class _CannedVerdictStub:
    """LLMClient stub returning the same canned JSON verdict to
    every `complete()` call."""

    model = "stub"

    def __init__(self, verdict_json: str):
        self.verdict_json = verdict_json
        self.calls: list[dict] = []

    def complete(
        self, system, user, *, fragment_id="",
        caller="", generation_id=None,
        outcome_key_digest=None,
    ):
        self.calls.append({
            "system": system, "user": user,
            "fragment_id": fragment_id, "caller": caller,
            "generation_id": generation_id,
        })
        return self.verdict_json


def _make_factory(verdict_json: str):
    """Return a `client_factory(model, dsn, deployment) -> stub`."""
    stub = _CannedVerdictStub(verdict_json)

    def factory(model, dsn, deployment):
        return stub

    factory.stub = stub  # for inspection
    return factory


# ---------- helpers ----------


def _seed_publication(
    conn: psycopg.Connection, node_id: str, body: str, *, gen: int = 1,
) -> None:
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(
                node_id,
                "publication",
                {"body": body},
                {"id": node_id},
                {"v": 1},
            )
        ],
        source="default",
        strict_endpoints=True,
    )
    # Checkpoint the generation AFTER the node lands so
    # `checkout_generation(gen)` sees the seeded body.
    ensure_published_generation(conn, gen)


def _make_invariant_dict(
    *,
    name: str = "test_inv",
    candidates_sql: str,
    judge_prompt_path: str,
    severity: str = "warn",
    sample_cap: int = 5,
    fire_on: tuple[str, ...] = ("inconsistent",),
    timeout_ms: int = 5000,
    judge_model: str = "stub",
):
    return {
        "name": name,
        "kind": "llm_judge",
        "severity": severity,
        "scope": "cross",
        "source": None,
        "candidates_sql": candidates_sql,
        "judge_prompt": judge_prompt_path,
        "judge_model": judge_model,
        "sample_cap": sample_cap,
        "fire_on": list(fire_on),
        "timeout_ms": timeout_ms,
        "concept_scope": None,
    }


# ---------- runner: zero candidates ----------


def test_runner_with_zero_candidates(
    clean_runtime: str, tmp_path,
) -> None:
    """A candidates_sql that returns no rows → result with sampled=0
    and total_candidates=0."""
    prompt = tmp_path / "prompt.txt"
    prompt.write_text("Judge: are these consistent?")
    inv = _make_invariant_dict(
        candidates_sql=(
            "SELECT 'never' AS candidate_id, '{}'::jsonb AS payload "
            "WHERE FALSE"
        ),
        judge_prompt_path=str(prompt),
    )
    factory = _make_factory(json.dumps({
        "verdict": "consistent",
        "rationale": "n/a",
    }))
    runner = LLMJudgeInvariantRunner(
        clean_runtime, "default", inv,
        client_factory=factory,
    )
    result = runner.run(generation_id=1)
    assert isinstance(result, JudgeInvariantResult)
    assert result.sampled == 0
    assert result.total_candidates == 0
    assert factory.stub.calls == []  # no LLM calls


# ---------- runner: candidates_sql shape invalid ----------


def test_runner_rejects_wrong_columns(
    clean_runtime: str, tmp_path,
) -> None:
    """A candidates_sql that returns the wrong columns sets
    skipped_reason='candidates_sql_shape_invalid'."""
    prompt = tmp_path / "prompt.txt"
    prompt.write_text("Judge prompt body")
    inv = _make_invariant_dict(
        candidates_sql="SELECT 'wrong' AS not_candidate_id",
        judge_prompt_path=str(prompt),
    )
    factory = _make_factory(json.dumps({
        "verdict": "consistent", "rationale": "n/a",
    }))
    runner = LLMJudgeInvariantRunner(
        clean_runtime, "default", inv,
        client_factory=factory,
    )
    result = runner.run(generation_id=1)
    assert result.skipped_reason == "candidates_sql_shape_invalid"
    assert factory.stub.calls == []


# ---------- runner: happy path with one consistent candidate ----------


def test_runner_consistent_verdict_no_fire(
    clean_runtime: str, tmp_path,
) -> None:
    """A 'consistent' verdict (not in fire_on) doesn't add to fires
    but increments verdict_counts['consistent']."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_publication(conn, "pub:1", "body text here")

    prompt = tmp_path / "prompt.txt"
    prompt.write_text("Judge prompt")
    inv = _make_invariant_dict(
        candidates_sql=(
            "SELECT 'pub:1'::text AS candidate_id, "
            "       '{\"a\": 1}'::jsonb AS payload"
        ),
        judge_prompt_path=str(prompt),
    )
    factory = _make_factory(json.dumps({
        "verdict": "consistent",
        "rationale": "all checks aligned",
    }))
    runner = LLMJudgeInvariantRunner(
        clean_runtime, "default", inv,
        client_factory=factory,
    )
    result = runner.run(generation_id=1)
    assert result.sampled == 1
    assert result.total_candidates == 1
    assert result.skipped_reason is None
    assert result.fires == []
    assert result.verdict_counts["consistent"] == 1
    assert result.verdict_counts.get("inconsistent", 0) == 0
    # The stub got called once with the right caller string.
    assert len(factory.stub.calls) == 1
    assert factory.stub.calls[0]["caller"] == "invariants:test_inv"


# ---------- runner: inconsistent verdict fires ----------


def test_runner_inconsistent_verdict_fires(
    clean_runtime: str, tmp_path,
) -> None:
    """An 'inconsistent' verdict (in fire_on) lands in fires."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_publication(conn, "pub:1", "body text body text")

    prompt = tmp_path / "prompt.txt"
    prompt.write_text("Judge prompt")
    inv = _make_invariant_dict(
        candidates_sql=(
            "SELECT 'pub:1'::text AS candidate_id, "
            "       '{\"a\": 1}'::jsonb AS payload"
        ),
        judge_prompt_path=str(prompt),
        fire_on=("inconsistent",),
    )
    factory = _make_factory(json.dumps({
        "verdict": "inconsistent",
        "rationale": "spec says X, body says Y",
        "evidence_spans": [
            {"node_id": "pub:1", "byte_start": 0, "byte_end": 9},
        ],
    }))
    runner = LLMJudgeInvariantRunner(
        clean_runtime, "default", inv,
        client_factory=factory,
    )
    result = runner.run(generation_id=1)
    assert result.sampled == 1
    assert result.verdict_counts["inconsistent"] == 1
    assert len(result.fires) == 1
    fire = result.fires[0]
    assert fire["candidate_id"] == "pub:1"
    assert fire["verdict"] == "inconsistent"
    # Evidence span echoed with a preview of the body bytes.
    assert fire["evidence_spans"][0]["preview"] == "body text"


# ---------- runner: spend admission ----------


def _ensure_cost_tables(dsn: str) -> None:
    """The clean_runtime fixture applies substrate schema only; the
    SDK cost tables come from the runtime bootstrap. Mirror
    tests/substrate/runtime/test_sdk_mutations.py's shapes. TRUNCATE on
    every call: these tables are outside clean_runtime's reset set, so
    rows would otherwise accumulate across tests and sessions (which the
    old settlement cap-blackout bug used to hide)."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS okg.deployment_cost_ledger ("
            "scope TEXT NOT NULL, "
            "consumption_id TEXT NOT NULL, "
            "usd_consumed NUMERIC NOT NULL CHECK (usd_consumed >= 0), "
            "workflow_id TEXT, "
            "observed_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
            "PRIMARY KEY (scope, consumption_id))"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS okg.deployment_cost_caps ("
            "scope TEXT PRIMARY KEY, "
            "cap_usd NUMERIC NOT NULL CHECK (cap_usd >= 0), "
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        )
        cur.execute("TRUNCATE okg.deployment_cost_ledger")
        cur.execute("TRUNCATE okg.deployment_cost_caps")


def test_runner_spend_cap_refuses_and_settles(
    clean_runtime: str, tmp_path,
) -> None:
    """A declared judge cap admits calls up to the cap, refuses the
    rest, and settles admitted reservations onto the shared ledger.

    Non-vacuity anchor: before the spend keys were plumbed through the
    loader + gates SELECT, this declaration was unreachable and every
    judge run was spend-unbounded while the code read as capped.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_publication(conn, "pub:1", "body text")
    _ensure_cost_tables(clean_runtime)

    prompt = tmp_path / "prompt.txt"
    prompt.write_text("Judge prompt")
    inv = _make_invariant_dict(
        # Three candidates; cap 0.02 at 0.015/call admits exactly one.
        candidates_sql=(
            "SELECT 'pub:' || g::text AS candidate_id, "
            "       '{}'::jsonb AS payload "
            "FROM generate_series(1, 3) g"
        ),
        judge_prompt_path=str(prompt),
    )
    inv["judge_spend_cap_usd"] = 0.02
    inv["judge_spend_estimate_usd"] = 0.015
    factory = _make_factory(json.dumps({
        "verdict": "consistent",
        "rationale": "fine",
        "evidence_spans": [],
    }))
    runner = LLMJudgeInvariantRunner(
        clean_runtime, "default", inv,
        client_factory=factory,
    )
    result = runner.run(generation_id=1)

    # Exactly one provider call was issued; two were refused into the
    # call-failure accounting (refusals must be visible, not dropped).
    assert len(factory.stub.calls) == 1
    assert result.verdict_counts["consistent"] == 1
    assert result.verdict_counts["call_failure"] == 2

    # The admitted call's reservation settled onto the shared ledger
    # under the invariant's scope, with real provenance.
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT usd_consumed, workflow_id "
            "FROM okg.deployment_cost_ledger WHERE scope = %s",
            ("invariant-judge:test_inv",),
        )
        rows = cur.fetchall()
    assert len(rows) == 1
    assert float(rows[0][0]) == pytest.approx(0.015)
    assert rows[0][1] != "test-wf"


def test_runner_without_spend_declaration_is_unbounded(
    clean_runtime: str, tmp_path,
) -> None:
    """No declaration → every call admitted, nothing charged. The lane
    is honestly unbounded, not silently capped at an invented number."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_publication(conn, "pub:1", "body text")
    _ensure_cost_tables(clean_runtime)

    prompt = tmp_path / "prompt.txt"
    prompt.write_text("Judge prompt")
    inv = _make_invariant_dict(
        # Distinct name: the cost tables outlive clean_runtime's reset,
        # so scopes must not collide across tests in this file.
        name="unbounded_inv",
        candidates_sql=(
            "SELECT 'pub:' || g::text AS candidate_id, "
            "       '{}'::jsonb AS payload "
            "FROM generate_series(1, 3) g"
        ),
        judge_prompt_path=str(prompt),
    )
    factory = _make_factory(json.dumps({
        "verdict": "consistent",
        "rationale": "fine",
        "evidence_spans": [],
    }))
    runner = LLMJudgeInvariantRunner(
        clean_runtime, "default", inv,
        client_factory=factory,
    )
    result = runner.run(generation_id=1)
    assert len(factory.stub.calls) == 3
    assert result.verdict_counts["call_failure"] == 0
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.deployment_cost_ledger "
            "WHERE scope = %s",
            ("invariant-judge:unbounded_inv",),
        )
        assert cur.fetchone()[0] == 0


# ---------- runner: parse-error path ----------


def test_runner_parse_error_path(
    clean_runtime: str, tmp_path,
) -> None:
    """A non-JSON LLM response surfaces as a parse_errors entry."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_publication(conn, "pub:1", "body")

    prompt = tmp_path / "prompt.txt"
    prompt.write_text("Judge prompt")
    inv = _make_invariant_dict(
        candidates_sql=(
            "SELECT 'pub:1'::text AS candidate_id, "
            "       '{}'::jsonb AS payload"
        ),
        judge_prompt_path=str(prompt),
    )
    factory = _make_factory("this is not json")
    runner = LLMJudgeInvariantRunner(
        clean_runtime, "default", inv,
        client_factory=factory,
    )
    result = runner.run(generation_id=1)
    assert result.sampled == 1
    assert result.verdict_counts["parse_error"] == 1
    assert len(result.parse_errors) == 1
    assert result.parse_errors[0]["candidate_id"] == "pub:1"


# ---------- runner: call-failure path ----------


def test_runner_call_failure_path(
    clean_runtime: str, tmp_path,
) -> None:
    """An exception from `client.complete` surfaces as
    call_failures + verdict_counts['call_failure']."""

    class _RaisingStub:
        model = "raising"

        def complete(self, *args, **kwargs):
            raise RuntimeError("transport timeout")

    def factory(model, dsn, deployment):
        return _RaisingStub()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_publication(conn, "pub:1", "body")

    prompt = tmp_path / "prompt.txt"
    prompt.write_text("Judge prompt")
    inv = _make_invariant_dict(
        candidates_sql=(
            "SELECT 'pub:1'::text AS candidate_id, "
            "       '{}'::jsonb AS payload"
        ),
        judge_prompt_path=str(prompt),
    )
    runner = LLMJudgeInvariantRunner(
        clean_runtime, "default", inv, client_factory=factory,
    )
    result = runner.run(generation_id=1)
    assert result.sampled == 1
    assert result.verdict_counts["call_failure"] == 1
    assert len(result.call_failures) == 1
    assert "transport timeout" in result.call_failures[0]["error"]


# ---------- runner: invalid evidence span doesn't fire ----------


def test_runner_invalid_span_recorded_but_does_not_fire(
    clean_runtime: str, tmp_path,
) -> None:
    """An inconsistent verdict whose evidence span is out-of-range
    lands in span_invalid (not fires)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_publication(conn, "pub:1", "short")  # 5 bytes

    prompt = tmp_path / "prompt.txt"
    prompt.write_text("Judge prompt")
    inv = _make_invariant_dict(
        candidates_sql=(
            "SELECT 'pub:1'::text AS candidate_id, "
            "       '{}'::jsonb AS payload"
        ),
        judge_prompt_path=str(prompt),
    )
    factory = _make_factory(json.dumps({
        "verdict": "inconsistent",
        "rationale": "...",
        "evidence_spans": [
            {"node_id": "pub:1", "byte_start": 0, "byte_end": 1000},
        ],
    }))
    runner = LLMJudgeInvariantRunner(
        clean_runtime, "default", inv,
        client_factory=factory,
    )
    result = runner.run(generation_id=1)
    # Verdict is "inconsistent" (in fire_on) so the candidate
    # fires regardless of span validity. Invalid spans are
    # tracked separately in `span_invalid` for visibility.
    assert len(result.fires) == 1
    assert len(result.span_invalid) == 1
    assert result.span_invalid[0]["candidate_id"] == "pub:1"
    assert (
        result.span_invalid[0]["invalid_spans"][0]["reason"]
        .startswith("out_of_range")
    )
    # Fire's evidence_spans list excludes the invalid one.
    assert result.fires[0]["evidence_spans"] == []
