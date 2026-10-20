"""Regression: a failing per-item LLM call must NOT silently drop the
item.

Root cause (pre-fix): every LLM-driven extractor wrapped its per-item
``self.llm.complete(...)`` in ``try/except ... continue``. A transient
provider error (e.g. litellm "Provider List" routing) yielded zero
candidates for that item, but the enricher still returned success -> the
publisher advanced the enricher's ``okg.enricher_progress.fact_watermark``
PAST the failed item -> that item sat behind the watermark and was NEVER
retried, permanently and silently losing its typed nodes (a P0
data-loss bug; discovered proving the referee doc-corpus deployment where
a minority of openrouter/gemini calls hit transient provider errors).

Fixed contract (this file): the base :class:`LLMExtractor` routes every
per-item call through :meth:`_complete_tracked`, which records failures.
A run with any recorded failure returns ``EnrichResult(incomplete=True,
n_input_failures=N, error=<loud>)`` while STILL emitting the good items'
typed nodes this cycle. The publisher reads ``incomplete`` and holds the
watermark (see ``tests/substrate/test_enricher_input_failure_holds_watermark.py``).

No DB, no real LLM: a fake client raises for one designated source item.
"""
from __future__ import annotations

import json
from typing import Any, Iterable

from okg.substrate.library.enrichers.extraction.base import (
    ExtractionCandidate,
    LLMExtractor,
    _durable_failure_detail,
)
from okg.views.execution.errors import LLMCallFailed


# ---------------------------------------------------------------------------
# Mock psycopg + fake LLM (no real DB, no provider call)
# ---------------------------------------------------------------------------


class _Cursor:
    def execute(self, *_a: Any, **_k: Any) -> None:
        return None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_a: Any) -> None:
        return None


class _Conn:
    def cursor(self) -> _Cursor:
        return _Cursor()


class _FlakyLLM:
    """``complete`` raises for source ids in ``fail_srcs`` (simulating a
    transient provider-routing error), else returns a canned
    one-candidate JSON array."""

    def __init__(
        self,
        fail_srcs: set[str],
        failures: dict[str, BaseException] | None = None,
    ) -> None:
        self.fail_srcs = set(fail_srcs)
        self.failures = dict(failures or {})
        self.calls: list[str] = []

    def complete(
        self, system_prompt: str, user_prompt: str, **kwargs: Any
    ) -> str:
        fragment_id = str(kwargs.get("fragment_id") or "")
        src = fragment_id.split(":", 1)[-1]
        self.calls.append(src)
        if src in self.failures:
            raise self.failures[src]
        if src in self.fail_srcs:
            raise RuntimeError(
                "litellm.BadRequestError: Provider List routing error "
                "(transient)"
            )
        return '[{"text": "a grounded claim", "confidence": 0.9}]'


class _LineExtractor(LLMExtractor):
    """Minimal extractor: one LLM call per source id, routed through the
    base ``_complete_tracked`` chokepoint. (The five real extractors
    route through ``_keyed_complete``, which shares the same
    record-and-hold failure contract and adds OutcomeKey dispatch.)"""

    # Node provenance rides the base class: LLMExtractor._emit_dropped
    # passes derived_from via derivation_refs. The contract under test here
    # is LLM-failure watermark holding, not admission.
    declares_node_provenance = True

    name = "line_extractor"
    emits_typed_subtype = "claim"

    def __init__(
        self, *, confidence_floor: float, llm: Any, sources: list[str]
    ) -> None:
        super().__init__(
            confidence_floor=confidence_floor,
            llm=llm,
            input_quarantine_max_attempts=3,
        )
        self._sources = list(sources)
        self.emitted: list[str] = []

    def extract_candidates(
        self, conn: Any, generation_id: int
    ) -> Iterable[ExtractionCandidate]:
        for src in self._sources:
            raw = self._complete_tracked(
                "sys",
                f"item {src}",
                item_ref=src,
                fragment_id=f"item:{src}",
            )
            if raw is None:
                continue
            for obj in json.loads(raw):
                yield ExtractionCandidate(
                    candidate_text=obj["text"],
                    confidence=float(obj["confidence"]),
                    source_node_ref=src,
                )

    def emit_typed_node(
        self, conn: Any, candidate: ExtractionCandidate, *,
        generation_id: int, run_id: str, observed_at: Any,
    ) -> int:
        self.emitted.append(candidate.source_node_ref)
        return 1


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_llm_failure_marks_run_incomplete_but_keeps_good_outputs() -> None:
    llm = _FlakyLLM(fail_srcs={"s2"})
    ex = _LineExtractor(
        confidence_floor=0.6, llm=llm, sources=["s1", "s2", "s3"],
    )

    result = ex.enrich(_Conn(), generation_id=7)

    # Every source was attempted (the failed one is not skipped upstream).
    assert llm.calls == ["s1", "s2", "s3"]
    # The good items STILL emit this cycle — a failure of one item does
    # not throw away the whole batch's work.
    assert ex.emitted == ["s1", "s3"]
    assert result.n_edges_emitted == 2

    # The failed item is surfaced loudly and marks the run incomplete so
    # the publisher will HOLD the watermark (not silently drop s2).
    assert result.incomplete is True
    assert result.n_input_failures == 1
    assert result.error is not None
    assert "watermark held" in result.error
    # The extractor tracked exactly which item failed.
    assert [ref for ref, _exc in ex._input_failures] == ["s2"]


def test_llm_failure_preserves_provider_reason_and_holds_watermark() -> None:
    try:
        try:
            raise RuntimeError("boom")
        except RuntimeError as cause:
            raise LLMCallFailed("provider call failed") from cause
    except LLMCallFailed as failure:
        llm = _FlakyLLM(fail_srcs=set(), failures={"s2": failure})

    ex = _LineExtractor(
        confidence_floor=0.6, llm=llm, sources=["s1", "s2"],
    )

    result = ex.enrich(_Conn(), generation_id=7)

    assert result.incomplete is True
    assert result.error is not None
    assert "LLMCallFailed" in result.error
    assert "boom" in result.error
    assert "watermark held" in result.error


def test_durable_failure_detail_walks_nested_causes_to_the_root() -> None:
    outer = LLMCallFailed("outer wrapper")
    middle = ValueError("middle wrapper")
    root = RuntimeError("root provider reason")
    outer.__cause__ = middle
    middle.__cause__ = root

    detail = _durable_failure_detail(outer)

    assert detail == "LLMCallFailed: root provider reason"


def test_durable_failure_detail_has_safe_fallbacks_and_bound() -> None:
    class _BrokenStringError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("broken formatter")

    assert _durable_failure_detail(ValueError()) == "ValueError: ValueError"
    assert _durable_failure_detail(_BrokenStringError()) == (
        "_BrokenStringError: _BrokenStringError"
    )
    assert _durable_failure_detail(
        RuntimeError("  provider\nreason\twith  whitespace  ")
    ) == "RuntimeError: provider reason with whitespace"

    long_message = "provider reason " + ("temporary upstream response; " * 20)
    detail = _durable_failure_detail(RuntimeError(long_message))
    assert detail == "RuntimeError: " + long_message[:160]
    assert len(detail) == len("RuntimeError: ") + 160

    very_long_type = type("E" * 500, (Exception,), {})
    assert len(_durable_failure_detail(very_long_type("reason"))) <= 240


def test_durable_failure_detail_redacts_sensitive_content() -> None:
    cases = (
        ("authorization: Bearer sk-provider-secret-token", "sk-provider-secret-token"),
        ("Authorization: Token short-token", "short-token"),
        ("Authorization: Basic dXNlcjpwYXNz", "dXNlcjpwYXNz"),
        ("Cookie: session-secret", "session-secret"),
        ("Set-Cookie: session-secret", "session-secret"),
        ("Proxy-Authorization: Basic session-secret", "session-secret"),
        ("api_key: short-secret", "short-secret"),
        ("api_key: short,secret", "short,secret"),
        ('headers={"Authorization":"Token abc,def"}', "abc,def"),
        ('headers={"Authorization":"Token abc}def"}', "abc}def"),
        ('headers={"Authorization":"Token abc\\"def"}', 'abc\\"def'),
        ('headers={"Cookie":"session-secret"}', "session-secret"),
        ('request_headers={"X-Internal":"session-secret"}', "session-secret"),
        ('headers="session-secret"', "session-secret"),
        ("headers=Bearer short-token", "short-token"),
        ("request_headers=short-token", "short-token"),
        ("response_headers=short-token", "short-token"),
        ("private_key: short-secret", "short-secret"),
        ("access_key: short-secret", "short-secret"),
        ("secret_key: short-secret", "short-secret"),
        ("authorization_header: Token short-secret", "short-secret"),
        ("password: hunter2", "hunter2"),
        ('{"access_token":"short-secret"}', "short-secret"),
        ('headers={"Authorization": "Bearer short"}', "short"),
        ('headers={"Authorization": "OAuth quoted-secret"}', "quoted-secret"),
    )
    for message, secret in cases:
        detail = _durable_failure_detail(RuntimeError(message))
        assert secret not in detail
        assert "<redacted>" in detail


def test_durable_failure_detail_stops_on_cause_cycles() -> None:
    first = RuntimeError("first")
    second = RuntimeError("second")
    first.__cause__ = second
    second.__cause__ = first

    assert _durable_failure_detail(first) == "RuntimeError: second"


def test_successful_cycle_retries_the_previously_failed_input() -> None:
    llm = _FlakyLLM(fail_srcs={"s2"})
    ex = _LineExtractor(
        confidence_floor=0.6, llm=llm, sources=["s1", "s2"],
    )

    first = ex.enrich(_Conn(), generation_id=7)
    assert first.incomplete is True

    llm.fail_srcs.clear()
    second = ex.enrich(_Conn(), generation_id=8)

    assert second.incomplete is False
    assert second.error is None
    assert llm.calls == ["s1", "s2", "s1", "s2"]
    assert ex.emitted == ["s1", "s1", "s2"]


def test_all_calls_succeed_run_is_complete() -> None:
    llm = _FlakyLLM(fail_srcs=set())
    ex = _LineExtractor(
        confidence_floor=0.6, llm=llm, sources=["s1", "s2"],
    )

    result = ex.enrich(_Conn(), generation_id=7)

    # No failure => the run is complete and the publisher is free to
    # advance the watermark (unchanged behaviour on the happy path).
    assert result.incomplete is False
    assert result.n_input_failures == 0
    assert result.error is None
    assert result.n_edges_emitted == 2
    assert ex.emitted == ["s1", "s2"]


def test_every_item_failing_yields_zero_edges_and_incomplete() -> None:
    llm = _FlakyLLM(fail_srcs={"s1", "s2"})
    ex = _LineExtractor(
        confidence_floor=0.6, llm=llm, sources=["s1", "s2"],
    )

    result = ex.enrich(_Conn(), generation_id=7)

    # Zero emitted, but the run is NOT reported as a clean success — an
    # all-fail cycle must hold the watermark so both items retry.
    assert result.n_edges_emitted == 0
    assert result.incomplete is True
    assert result.n_input_failures == 2
