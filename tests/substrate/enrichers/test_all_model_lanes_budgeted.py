"""Every model-call lane draws its width from the registry, not a constant.

`req.shared-model-call-fanout.one-helper` is about consolidation, but the
property that actually prevents the shattered state returning is narrower and
decidable: no lane may size itself. The AST guard catches literal widths at
construction sites; these catch the other shape — a default argument or a
constructor field that quietly reintroduces a private width.

Recorded here because the PACT was WRONG about this lane: it listed figure
captioning as one of seven serial sites based on `doc_corpus`, but that path is
discovery-only and the real captioning lives in `VisualCaptionEnricher`, which
was ALREADY parallel and correctly two-phase. Its defect was never seriality —
it was a width of 8 that nothing reconciled.
"""
from __future__ import annotations

import inspect

import pytest

from okg.substrate.db import concurrency_budget as budget


@pytest.fixture(autouse=True)
def _clean():
    budget.clear_overrides()
    yield
    budget.clear_overrides()


def test_caption_enricher_width_defaults_to_the_registry() -> None:
    from okg.substrate.library.enrichers.visual_caption_enricher import (
        VisualCaptionEnricher,
    )

    sig = inspect.signature(VisualCaptionEnricher.__init__)
    assert sig.parameters["max_workers"].default is None, (
        "max_workers has a hardcoded default again; it must fall back to the "
        "`enrichment.model_calls` term so this lane is counted with the others"
    )


def test_caption_enricher_width_follows_an_override() -> None:
    from okg.substrate.library.enrichers.visual_caption_enricher import (
        VisualCaptionEnricher,
    )

    budget.set_override("enrichment.model_calls", 3)
    enricher = VisualCaptionEnricher(model="openai/gpt-4o-mini")
    assert enricher.max_workers == 3

    budget.set_override("enrichment.model_calls", 7)
    assert VisualCaptionEnricher(model="openai/gpt-4o-mini").max_workers == 7


def test_an_explicit_width_still_wins() -> None:
    """A deployment that names a width gets it — the registry is the DEFAULT,
    not an override of the caller."""
    from okg.substrate.library.enrichers.visual_caption_enricher import (
        VisualCaptionEnricher,
    )

    budget.set_override("enrichment.model_calls", 3)
    enricher = VisualCaptionEnricher(
        model="openai/gpt-4o-mini", max_workers=11,
    )
    assert enricher.max_workers == 11


def test_recheck_width_comes_from_the_registry() -> None:
    from okg.views.execution import recheck

    budget.set_override("views.recheck_judge", 5)
    assert recheck._chunk_judge_workers() == 5


def test_a_bad_explicit_width_is_still_refused() -> None:
    """Sourcing the default from the registry must not weaken validation."""
    from okg.substrate.library.enrichers.visual_caption_enricher import (
        VisualCaptionEnricher,
    )

    with pytest.raises(ValueError):
        VisualCaptionEnricher(model="openai/gpt-4o-mini", max_workers=0)


# ---------------------------------------------------------------------------
# Recheck judge lane: spend admission, not just width.
#
# Width bounds occupancy; it does nothing to bound cost. The recheck fan-out
# went through `complete_many` with no `admit`/`settle`, so NOTHING could
# produce a `budget_refused` outcome and the lane was economically unbounded.
# These assert the cap actually stops provider calls, that a refusal is
# distinguishable from a provider failure (an operator must be able to tell
# "raise the cap" from "the provider is down"), and that an undeclared cap
# stays honestly unbounded rather than silently defaulted.
# ---------------------------------------------------------------------------


class _FakeRecheckGraph:
    """Returns N cited chunks for the direct-citation query, nothing else.

    Consistent verdicts need no pdf_text_index, so the localization
    queries are never reached."""

    def __init__(self, n_chunks: int) -> None:
        self._chunks = [
            {
                "node_id": f"chunk:{i}",
                "pub_id": "pub:1",
                "section_id": None,
                "text": f"chunk text {i}",
            }
            for i in range(n_chunks)
        ]

    def query(self, sql: str, params=None):
        if "document_chunk" in sql and "view_cites_node" in sql:
            return [dict(c) for c in self._chunks]
        return []


class _CountingJudge:
    """Stub judge that counts provider calls, thread-safely."""

    model = "stub-judge"

    def __init__(self) -> None:
        import threading

        self._lock = threading.Lock()
        self.calls = 0

    def complete(self, system: str, user: str, **kwargs) -> str:
        with self._lock:
            self.calls += 1
        return (
            '{"verdict": "consistent", "quoted_span": "",'
            ' "severity": "low", "rationale": "ok"}'
        )


def _run_recheck(client, *, n_chunks: int = 5, **spend_kwargs):
    from okg.views.execution.recheck import recheck_fragment

    return recheck_fragment(
        _FakeRecheckGraph(n_chunks),
        deployment_name="test",
        view_id="v1",
        fragment_id="f1",
        original_md="alpha\n",
        edited_md="beta\n",
        llm_client=client,
        **spend_kwargs,
    )


def test_recheck_cap_refusal_limits_provider_calls() -> None:
    """$0.25 at $0.10/call admits exactly 2 of 5 judges; the other 3 are
    refused BEFORE dispatch, so the provider never sees them."""
    judge = _CountingJudge()
    result = _run_recheck(
        judge,
        n_chunks=5,
        recheck_spend_cap_usd=0.25,
        recheck_spend_estimate_usd=0.1,
    )

    assert judge.calls == 2, (
        f"cap of $0.25 at $0.10/call should stop the provider at 2 calls; "
        f"it saw {judge.calls}"
    )
    # k-of-N tolerance: the 2 admitted verdicts still aggregate.
    assert result.verdict == "consistent"
    refused = [
        w for w in result.validation_warnings
        if w.get("kind") == "spend_refused"
    ]
    assert len(refused) == 3
    for w in refused:
        assert "spend cap exhausted" in w["detail"]
        assert "recheck-judge:v1" in w["detail"]


def test_recheck_all_refused_raises_and_names_the_cap() -> None:
    """All-N-refused is a fragment-level failure, and its message must read
    as 'the cap is exhausted', not as a provider outage."""
    from okg.views.execution.llm import LLMCallFailed

    judge = _CountingJudge()
    with pytest.raises(LLMCallFailed, match="spend_refused") as excinfo:
        _run_recheck(
            judge,
            n_chunks=3,
            recheck_spend_cap_usd=0.05,
            recheck_spend_estimate_usd=0.1,
        )

    assert judge.calls == 0, "a refused judge call still hit the provider"
    msg = str(excinfo.value)
    assert "spend cap exhausted" in msg
    assert "$0.0500" in msg, f"the exhausted cap is not named: {msg}"
    assert "recheck-judge:v1" in msg


def test_recheck_undeclared_cap_is_unbounded_and_uncharged() -> None:
    """No declaration = the lane runs unbounded (the pre-existing behavior),
    with every call made and nothing reserved or refused — never a silently
    invented default cap."""
    judge = _CountingJudge()
    result = _run_recheck(judge, n_chunks=5)

    assert judge.calls == 5
    assert result.verdict == "consistent"
    assert result.validation_warnings == []


def test_recheck_half_declared_spend_config_is_refused() -> None:
    """A cap alone cannot reserve; an estimate alone bounds nothing. And the
    refusal must fire before any provider call."""
    from okg.views.execution.recheck import RecheckSpendConfigInvalid

    judge = _CountingJudge()
    with pytest.raises(RecheckSpendConfigInvalid, match="declared together"):
        _run_recheck(judge, recheck_spend_cap_usd=1.0)
    with pytest.raises(RecheckSpendConfigInvalid, match="declared together"):
        _run_recheck(judge, recheck_spend_estimate_usd=0.1)
    assert judge.calls == 0


@pytest.mark.parametrize("bad", [0, -1.0])
def test_recheck_refuses_non_positive_spend_config(bad) -> None:
    from okg.views.execution.recheck import RecheckSpendConfigInvalid

    judge = _CountingJudge()
    with pytest.raises(RecheckSpendConfigInvalid, match="positive"):
        _run_recheck(
            judge,
            recheck_spend_cap_usd=bad,
            recheck_spend_estimate_usd=0.1,
        )
    with pytest.raises(RecheckSpendConfigInvalid, match="positive"):
        _run_recheck(
            judge,
            recheck_spend_cap_usd=1.0,
            recheck_spend_estimate_usd=bad,
        )
    assert judge.calls == 0
