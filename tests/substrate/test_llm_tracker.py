"""Coverage tests for `okg.substrate.llm.tracker`.

Two functions: `extract_usage(response)` (pure, exercises shape
permutations) and `record_llm_call(dsn, ...)` (DB insert + best-
effort no-raise on DB failure). Both are exercised here without
network: synthetic `ModelResponse`-shaped objects feed
`extract_usage`; `record_llm_call` writes to `clean_runtime`.
"""
from __future__ import annotations

from types import SimpleNamespace

import psycopg
import pytest

from okg.substrate.llm.tracker import extract_usage, record_llm_call
from okg.substrate.telemetry import use_trace

pytestmark = pytest.mark.postgres


# ---------- extract_usage: pure ----------


def _ns(**kw):
    """SimpleNamespace shorthand."""
    return SimpleNamespace(**kw)


def test_extract_usage_empty_response_returns_all_none() -> None:
    out = extract_usage(_ns())
    assert out["prompt_tokens"] is None
    assert out["completion_tokens"] is None
    assert out["total_tokens"] is None
    assert out["cost_usd"] is None
    assert out["extras"] == {}


def test_extract_usage_pulls_standard_token_counts() -> None:
    resp = _ns(usage=_ns(
        prompt_tokens=100, completion_tokens=50, total_tokens=150,
    ))
    out = extract_usage(resp)
    assert out["prompt_tokens"] == 100
    assert out["completion_tokens"] == 50
    assert out["total_tokens"] == 150


def test_extract_usage_dict_usage_works_too() -> None:
    """`usage` may be a dict instead of an object."""
    resp = _ns(usage={
        "prompt_tokens": 10,
        "completion_tokens": 20,
        "total_tokens": 30,
    })
    out = extract_usage(resp)
    assert out["prompt_tokens"] == 10
    assert out["completion_tokens"] == 20


def test_extract_usage_anthropic_cache_tokens_in_extras() -> None:
    """Anthropic prompt-cache fields surface in extras."""
    resp = _ns(usage=_ns(
        prompt_tokens=100, completion_tokens=50, total_tokens=150,
        cache_creation_input_tokens=30,
        cache_read_input_tokens=70,
    ))
    out = extract_usage(resp)
    assert out["extras"]["cache_creation_input_tokens"] == 30
    assert out["extras"]["cache_read_input_tokens"] == 70


def test_extract_usage_prompt_tokens_details_in_extras() -> None:
    resp = _ns(usage=_ns(
        prompt_tokens=100, completion_tokens=50, total_tokens=150,
        prompt_tokens_details=_ns(cached_tokens=80, audio_tokens=5),
    ))
    out = extract_usage(resp)
    assert out["extras"]["prompt_cached_tokens"] == 80
    assert out["extras"]["prompt_audio_tokens"] == 5


def test_extract_usage_completion_tokens_details_in_extras() -> None:
    resp = _ns(usage=_ns(
        prompt_tokens=100, completion_tokens=50, total_tokens=150,
        completion_tokens_details=_ns(
            reasoning_tokens=2000, text_tokens=50,
        ),
    ))
    out = extract_usage(resp)
    assert out["extras"]["completion_reasoning_tokens"] == 2000
    assert out["extras"]["completion_text_tokens"] == 50


def test_extract_usage_openrouter_cost_header_wins() -> None:
    """Path 1: OpenRouter-via-litellm header wins over other
    cost sources."""
    resp = _ns(
        usage=_ns(prompt_tokens=10, completion_tokens=5, total_tokens=15),
        _hidden_params={
            "additional_headers": {
                "llm_provider-x-litellm-response-cost": "0.0042",
            },
            "response_cost": 999.0,  # would win path 2; should be ignored
        },
    )
    out = extract_usage(resp)
    assert out["cost_usd"] == 0.0042


def test_extract_usage_litellm_response_cost_wins_when_no_header() -> None:
    """Path 2: litellm middleware's response_cost wins when path 1
    is absent."""
    resp = _ns(
        usage=_ns(prompt_tokens=10, completion_tokens=5, total_tokens=15),
        _hidden_params={"response_cost": 0.005},
    )
    out = extract_usage(resp)
    assert out["cost_usd"] == 0.005


def test_extract_usage_handles_bad_cost_value_gracefully() -> None:
    """Non-numeric cost values fall through to None instead of
    raising."""
    resp = _ns(
        usage=_ns(prompt_tokens=10, completion_tokens=5, total_tokens=15),
        _hidden_params={
            "additional_headers": {
                "llm_provider-x-litellm-response-cost": "not-a-number",
            },
        },
    )
    out = extract_usage(resp)
    assert out["cost_usd"] is None


def test_extract_usage_provenance_fields_in_extras() -> None:
    resp = _ns(
        id="resp_abc",
        system_fingerprint="fp_xyz",
        model="gpt-4o-2024-08-06",
        usage=_ns(prompt_tokens=10, completion_tokens=5, total_tokens=15),
        _hidden_params={
            "custom_llm_provider": "openai",
            "cache_hit": False,
            "api_base": "https://api.openai.com",
            "model_id": "gpt-4o",
        },
    )
    out = extract_usage(resp)
    assert out["extras"]["response_id"] == "resp_abc"
    assert out["extras"]["system_fingerprint"] == "fp_xyz"
    assert out["extras"]["response_model"] == "gpt-4o-2024-08-06"
    assert out["extras"]["custom_llm_provider"] == "openai"
    assert out["extras"]["cache_hit"] is False


def test_extract_usage_never_raises_on_malformed_response() -> None:
    """Even completely malformed input must produce a dict (best-
    effort contract: telemetry must not break the LLM call)."""

    class _Broken:
        @property
        def usage(self):
            raise RuntimeError("boom")

    out = extract_usage(_Broken())
    assert isinstance(out, dict)
    assert out["prompt_tokens"] is None


def test_extract_usage_handles_non_int_token_counts() -> None:
    """Tokens that don't parse to int come back as None."""
    resp = _ns(usage=_ns(
        prompt_tokens="oops", completion_tokens=None, total_tokens=15,
    ))
    out = extract_usage(resp)
    assert out["prompt_tokens"] is None
    assert out["completion_tokens"] is None
    assert out["total_tokens"] == 15


# ---------- record_llm_call: DB insert ----------


def test_record_llm_call_inserts_row(clean_runtime: str) -> None:
    record_llm_call(
        clean_runtime,
        caller="test:caller",
        model="gpt-x",
        fragment_id=None,
        prompt_tokens=10,
        completion_tokens=5,
        total_tokens=15,
        cost_usd=0.01,
        latency_ms=120,
        success=True,
        deployment="default",
        generation_id=1,
        extras={"k": "v"},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT caller, model, prompt_tokens, success, "
            "       extras->>'k' "
            "  FROM okg.llm_calls WHERE caller = 'test:caller'"
        )
        row = cur.fetchone()
    assert row is not None
    caller, model, ptokens, success, k = row
    assert caller == "test:caller"
    assert model == "gpt-x"
    assert ptokens == 10
    assert success is True
    assert k == "v"


def test_record_llm_call_with_failure(clean_runtime: str) -> None:
    record_llm_call(
        clean_runtime,
        caller="test:fail",
        model="gpt-x",
        fragment_id="frag-1",
        prompt_tokens=None,
        completion_tokens=None,
        total_tokens=None,
        cost_usd=None,
        latency_ms=5000,
        success=False,
        error_kind="timeout",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT success, error_kind FROM okg.llm_calls "
            "WHERE caller = 'test:fail'"
        )
        success, error_kind = cur.fetchone()
    assert success is False
    assert error_kind == "timeout"


def test_record_llm_call_accepts_chronos_generation_id(clean_runtime: str) -> None:
    """The spend ledger stores the Chronos ref, same as telemetry (D2).

    Before schema-identity-convergence, `okg.llm_calls.generation_id` was
    BIGINT and the writer coerced any non-numeric ref to NULL, so the
    documented work_ledger <-> llm_calls cost join silently reported zero spend
    for every Chronos generation. The column is now TEXT and both surfaces
    record the identical value.
    """
    generation_id = "okg:default:branch:default:gen:telemetry-test"

    with use_trace() as identity:
        record_llm_call(
            clean_runtime,
            caller="test:chronos-generation",
            model="gpt-x",
            fragment_id=None,
            prompt_tokens=10,
            completion_tokens=5,
            total_tokens=15,
            cost_usd=0.01,
            latency_ms=120,
            success=True,
            deployment="default",
            generation_id=generation_id,
            extras={"k": "v"},
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT generation_id
              FROM okg.llm_calls
             WHERE caller = 'test:chronos-generation'
            """
        )
        llm_calls_generation_id = cur.fetchone()[0]
        cur.execute(
            """
            SELECT generation_id
              FROM okg.telemetry_spans
             WHERE trace_id = %s
               AND span_name = 'llm.test:chronos-generation'
            """,
            (identity.trace_id,),
        )
        telemetry_generation_id = cur.fetchone()[0]

    assert llm_calls_generation_id == generation_id
    assert telemetry_generation_id == generation_id


def test_record_llm_call_swallows_db_error(caplog) -> None:
    """Best-effort contract: a DB failure logs WARNING and returns;
    never raises."""
    import logging

    with caplog.at_level(
        logging.WARNING,
        logger="okg.substrate.llm.tracker",
    ):
        # Unreachable DSN → connect raises → tracker swallows.
        record_llm_call(
            "postgres://nobody@127.0.0.1:1/nope",
            caller="test:db-fail",
            model="gpt-x",
            fragment_id=None,
            prompt_tokens=10,
            completion_tokens=5,
            total_tokens=15,
            cost_usd=0.01,
            latency_ms=10,
            success=True,
        )
    assert any(
        "llm_calls insert failed" in rec.message
        for rec in caplog.records
    )
