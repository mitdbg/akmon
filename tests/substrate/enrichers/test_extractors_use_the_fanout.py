"""The four LLM extractors dispatch as ONE batch, not N serial calls.

`req.shared-model-call-fanout.one-helper` is only satisfied if the call sites
actually use the helper. A green extractor suite does not show that — the old
serial loop passed the same tests. These assert the property directly: one
`_complete_many` per run carrying every item, and per-item failure isolation
preserved across the batch boundary.
"""

from __future__ import annotations

import pytest

from okg.substrate.library.enrichers.extraction import (
    claim_extractor,
    concept_extractor,
    evidence_extractor,
    hypothesis_extractor,
)


class _Claim(claim_extractor.ClaimExtractor):
    """Captures the jobs handed to the shared fan-out.

    A single subclass, not a mixin beside the extractor: `_complete_many` is
    defined by `LLMExtractor`, so a sibling mixin declaring it too makes which
    one binds a fact about MRO order rather than about this file. Overriding it
    on the subclass says the same thing unambiguously.
    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.batches: list[list] = []
        self.replies: dict[str, str] = {}
        self.failed: set[str] = set()

    def _complete_many(self, conn, jobs, *, generation_id):
        from okg.substrate.llm.fanout import ModelCallOutcome

        self.batches.append(list(jobs))
        out = []
        for job in jobs:
            if job.item_ref in self.failed:
                out.append(
                    ModelCallOutcome(provider_error=RuntimeError("provider 503"))
                )
            else:
                out.append(
                    ModelCallOutcome(content=self.replies.get(job.item_ref, "[]"))
                )
        return out


class _Llm:
    model = "test/model"

    def complete(self, *a, **kw):  # pragma: no cover - must not be reached
        raise AssertionError("extractor bypassed the fan-out")


@pytest.fixture
def claims():
    ex = _Claim(
        confidence_floor=0.5,
        llm=_Llm(),
        input_quarantine_max_attempts=3,
    )
    rows = [
        ("n:p1", "paragraph", "body one"),
        ("n:p2", "paragraph", "body two"),
        ("n:p3", "paragraph", "body three"),
    ]
    ex._fetch_source_rows = lambda conn, gen: rows
    return ex


def test_a_run_dispatches_exactly_one_batch(claims) -> None:
    """One `_complete_many` for the whole run. Three calls would mean the
    extractor is back to per-item dispatch and the fan-out buys nothing."""
    list(claims.extract_candidates(object(), 7))

    assert len(claims.batches) == 1
    assert [j.item_ref for j in claims.batches[0]] == ["n:p1", "n:p2", "n:p3"]


def test_every_job_carries_its_prompt_and_typed_input(claims) -> None:
    """The typed_input feeds the OutcomeKey; dropping a field would silently
    collide keys across different sources."""
    list(claims.extract_candidates(object(), 7))

    job = claims.batches[0][0]
    assert job.item_ref == "n:p1"
    assert "body one" in job.user
    assert job.typed_input["source_node_id"] == "n:p1"
    assert job.fragment_id == "claim:n:p1"


def test_one_failed_item_does_not_drop_the_others(claims) -> None:
    """Per-item isolation must survive batching: the old serial loop `continue`d
    past a failure, and the batch form has to behave identically."""
    claims.failed = {"n:p2"}
    claims.replies = {
        "n:p1": '[{"text": "c1", "confidence": 0.9, "kind": "empirical"}]',
        "n:p3": '[{"text": "c3", "confidence": 0.9, "kind": "empirical"}]',
    }

    out = list(claims.extract_candidates(object(), 7))

    assert [c.candidate_text for c in out] == ["c1", "c3"]
    assert [c.source_node_ref for c in out] == ["n:p1", "n:p3"]


def test_empty_input_dispatches_an_empty_batch(claims) -> None:
    claims._fetch_source_rows = lambda conn, gen: []
    assert list(claims.extract_candidates(object(), 7)) == []


@pytest.mark.parametrize(
    "module,cls_name",
    [
        (claim_extractor, "ClaimExtractor"),
        (concept_extractor, "ConceptExtractor"),
        (evidence_extractor, "EvidenceExtractor"),
        (hypothesis_extractor, "HypothesisExtractor"),
    ],
)
def test_no_extractor_calls_the_serial_chokepoint(module, cls_name) -> None:
    """All four must route through the batch helper. A site left on
    `_keyed_complete` is serial, and its width knob would silently do nothing.
    """
    import inspect

    src = inspect.getsource(getattr(module, cls_name).extract_candidates)
    assert "_complete_many(" in src, f"{cls_name} does not use the fan-out"
    assert "_keyed_complete(" not in src, (
        f"{cls_name} still dispatches through the serial chokepoint"
    )


def test_extractors_do_not_own_a_second_executor() -> None:
    """#925's helper is the sole concurrency layer for these extractors.

    A private pool in any extractor would bypass the shared width, spend,
    replay, and failure-accounting contracts even if the call site still
    mentioned ``_complete_many`` somewhere else.
    """
    import ast
    import inspect

    for module in (
        claim_extractor,
        concept_extractor,
        evidence_extractor,
        hypothesis_extractor,
    ):
        tree = ast.parse(inspect.getsource(module))
        private_imports = [
            node
            for node in ast.walk(tree)
            if (
                isinstance(node, ast.ImportFrom) and node.module == "concurrent.futures"
            )
            or (
                isinstance(node, ast.Import)
                and any(alias.name == "concurrent.futures" for alias in node.names)
            )
        ]
        private_executor_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and (isinstance(node.func, ast.Name) and node.func.id.endswith("Executor"))
        ]
        assert not private_imports, f"{module.__name__} imports a private pool"
        assert not private_executor_calls, (
            f"{module.__name__} constructs an extractor-owned executor"
        )
