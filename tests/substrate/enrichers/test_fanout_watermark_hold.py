"""`_complete_many` end-to-end: one call per item, and nothing skipped silently.

Two defects caught in review are pinned here because both are invisible in
production:

* Phase 2 must not go back through `keyed_complete`, which issues its OWN
  `llm.complete`. Routing through it makes every model call TWICE — doubling
  spend and latency, so the "fan-out" is slower and dearer than the serial
  path it replaces, while every test that only checks content still passes.
* Replay must never sample fresh. A fan-out that calls the provider in replay
  breaks the whole point of replay and silently bills for it.
"""
from __future__ import annotations

import pytest

from okg.substrate.library.enrichers.extraction import base as base_mod
from okg.substrate.llm.fanout import ModelCallJob


class _Llm:
    model = "test/model"

    def __init__(self, fail_on=()):
        self.calls: list[str] = []
        self.digests: list[str | None] = []
        self._fail_on = set(fail_on)

    def complete(self, system, user, **kw):
        self.calls.append(user)
        self.digests.append(kw.get("outcome_key_digest"))
        if user in self._fail_on:
            raise RuntimeError("provider 503")
        return f"content:{user}"


class _Extractor(base_mod.LLMExtractor):
    name = "fanout-probe"
    emits_typed_subtype = "probe"

    def extract_candidates(self, conn, generation_id):
        return []

    def emit_typed_node(self, conn, candidate, *, generation_id, run_id, observed_at):
        return 0


def _jobs(n):
    return [
        ModelCallJob(
            system="sys", user=f"u{i}", typed_input={"i": i}, item_ref=f"item-{i}"
        )
        for i in range(n)
    ]


@pytest.fixture
def extractor():
    return _Extractor(
        confidence_floor=0.5,
        llm=_Llm(),
        input_quarantine_max_attempts=3,
    )


def test_each_item_issues_exactly_one_provider_call(extractor) -> None:
    """The defect this pins: phase 2 re-issuing would double every call."""
    out = extractor._complete_many(object(), _jobs(4), generation_id=7)

    assert len(extractor.llm.calls) == 4, (
        f"expected one provider call per item, got {extractor.llm.calls}"
    )
    assert sorted(extractor.llm.calls) == ["u0", "u1", "u2", "u3"]
    assert [o.content for o in out] == [f"content:u{i}" for i in range(4)]


def test_every_call_carries_its_outcome_key_digest(extractor) -> None:
    """Losing the digest is silent and breaks the call-to-outcome join."""
    extractor._complete_many(object(), _jobs(3), generation_id=7)

    assert len(extractor.llm.digests) == 3
    assert all(d for d in extractor.llm.digests), extractor.llm.digests
    assert len(set(extractor.llm.digests)) == 3, "digests must differ per input"


def test_a_provider_failure_holds_the_watermark(extractor) -> None:
    extractor.llm = _Llm(fail_on={"u1"})
    out = extractor._complete_many(object(), _jobs(3), generation_id=7)

    assert [o.ok for o in out] == [True, False, True]
    refs = [ref for ref, _ in extractor._input_failures]
    assert refs == ["item-1"]


def test_a_failure_is_recorded_once_not_twice(extractor) -> None:
    """Double-recording would inflate the incompleteness signal and make one
    failed item look like several."""
    extractor.llm = _Llm(fail_on={"u0"})
    extractor._complete_many(object(), _jobs(2), generation_id=7)

    assert len(extractor._input_failures) == 1


def test_replay_never_calls_the_provider(extractor) -> None:
    """A replay miss is named without a second store/provider dispatch."""
    extractor.outcomes.mode = "replay"

    out = extractor._complete_many(object(), _jobs(3), generation_id=7)

    assert extractor.llm.calls == [], "replay issued provider calls"
    assert all(o.replay_excluded for o in out)
    assert len(extractor.outcomes.exclusions) == 3


def test_replay_miss_is_excluded_not_failed(extractor) -> None:
    extractor.outcomes.mode = "replay"

    out = extractor._complete_many(object(), _jobs(2), generation_id=7)

    assert all(o.replay_excluded for o in out)
    assert not any(o.holds_watermark for o in out)
    assert extractor._input_failures == []
