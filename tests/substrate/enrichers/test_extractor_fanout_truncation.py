"""Parallel extractor truncation is incomplete, loud, and non-destructive.

Issue #913 is a performance fix, but widening provider occupancy also widens
the number of responses that can hit an output cap together.  A qwen reasoning
trace can consume ``max_output_tokens`` and return either an empty named
``TruncatedProviderResponse`` or a non-empty partial body.  Neither may look
like an authoritative empty extraction.
"""

from __future__ import annotations

import threading
import time

import pytest

from okg.substrate.db import concurrency_budget
from okg.substrate.library.enrichers.extraction.claim_extractor import (
    ClaimExtractor,
)
from okg.views.execution.errors import LLMResponseTruncated


class _ConcurrentTruncatingLLM:
    model = "openrouter/qwen/issue-913-test-double"

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self._local = threading.local()
        self._lock = threading.Lock()
        self._active = 0
        self.peak_active = 0

    @property
    def last_call_stats(self):
        return getattr(self._local, "stats", None)

    def complete(self, system: str, user: str, **kwargs) -> str:
        del system, user
        fragment_id = str(kwargs["fragment_id"])
        with self._lock:
            self._active += 1
            self.peak_active = max(self.peak_active, self._active)
        try:
            time.sleep(0.04)
            if self.mode == "mixed":
                if fragment_id in {
                    "claim:paragraph:0",
                    "claim:paragraph:1",
                    "claim:paragraph:2",
                }:
                    raise RuntimeError("provider 503")
                raise LLMResponseTruncated(
                    "TruncatedProviderResponse: fourth failure exhausted "
                    "max_output_tokens"
                )
            if fragment_id == "claim:paragraph:2":
                if self.mode == "empty":
                    raise LLMResponseTruncated(
                        "TruncatedProviderResponse: qwen reasoning trace "
                        "exhausted max_output_tokens before content"
                    )
                self._local.stats = {"cost_usd": 0.0, "truncated": True}
                return (
                    '[{"text":"partial but parseable","confidence":0.9,'
                    '"kind":"empirical"}]'
                )
            self._local.stats = {"cost_usd": 0.0, "truncated": False}
            return '[{"text":"complete claim","confidence":0.9,"kind":"empirical"}]'
        finally:
            with self._lock:
                self._active -= 1


@pytest.mark.parametrize("mode", ["partial", "empty", "mixed"])
def test_parallel_truncation_is_loud_and_does_not_flush_snapshot(
    mode: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    concurrency_budget.clear_overrides()
    concurrency_budget.set_override("enrichment.model_calls", 4)
    try:
        llm = _ConcurrentTruncatingLLM(mode)
        extractor = ClaimExtractor(
            confidence_floor=0.6,
            llm=llm,
            input_quarantine_max_attempts=3,
        )
        rows = [
            (f"paragraph:{i}", "paragraph", f"real corpus body {i}") for i in range(4)
        ]
        extractor._fetch_source_rows = lambda conn, generation_id: rows
        extractor._load_prior_snapshot = lambda conn, generation_id: ([], [])

        flushed = False

        def _forbid_flush(*args, **kwargs):
            nonlocal flushed
            flushed = True
            raise AssertionError("truncated run attempted snapshot replacement")

        extractor._flush_complete_snapshot = _forbid_flush
        result = extractor.enrich(object(), generation_id=7)
    finally:
        concurrency_budget.clear_overrides()

    assert llm.peak_active > 1, "the provider calls did not overlap"
    assert result.incomplete is True
    expected_failures = 4 if mode == "mixed" else 1
    truncated_ref = "paragraph:3" if mode == "mixed" else "paragraph:2"
    assert result.n_input_failures == expected_failures
    assert result.n_edges_emitted == 0
    assert result.output_lineage is not None
    assert result.output_lineage["complete"] is False
    assert result.output_lineage["nodes_retracted"] == 0
    assert result.output_lineage["edges_retracted"] == 0
    assert result.output_lineage["node_assertions_inserted"] == 0
    assert result.output_lineage["edge_assertions_inserted"] == 0
    assert flushed is False
    operator_text = " ".join(
        [str(result.error), str(result.output_lineage.get("failure_reason"))]
    ).lower()
    assert "truncat" in operator_text
    assert truncated_ref in operator_text
    # Non-truncation failures now retain their bounded provider reason, while
    # the specialized truncation detail remains the same stable marker.
    if mode == "mixed":
        assert "provider 503" in operator_text
    assert "truncated response" in operator_text
    assert any(
        truncated_ref in record.getMessage()
        and "truncat" in record.getMessage().lower()
        for record in caplog.records
    )
