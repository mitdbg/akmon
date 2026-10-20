"""Key parity: the serial and fanned-out paths must derive the same OutcomeKey.

This is the failure mode with no error surface. A run recorded under one
derivation and replayed under another misses on EVERY lookup, and the symptom
is a pile of unexplained ReplayExclusions rather than an exception. The
structural defence is that both paths call `_outcome_key`; these tests hold
that structure in place and prove the assertion can fail.
"""
from __future__ import annotations

import inspect

import pytest

from okg.substrate.library.enrichers.extraction import base as base_mod
from okg.substrate.llm.fanout import ModelCallJob


class _Llm:
    model = "test/model"

    def complete(self, system, user, **kw):
        return "content"


class _Extractor(base_mod.LLMExtractor):
    name = "parity-probe"
    emits_typed_subtype = "probe"

    def extract_candidates(self, conn, generation_id):
        return []

    def emit_typed_node(self, conn, candidate, *, generation_id, run_id, observed_at):
        return 0


@pytest.fixture
def extractor():
    return _Extractor(
        confidence_floor=0.5,
        llm=_Llm(),
        input_quarantine_max_attempts=3,
    )


def test_record_through_one_path_replays_through_the_other(extractor) -> None:
    """The assertion the PACT actually mandates: record via the fanned-out
    path, replay via the serial one, and get a HIT.

    Asserting `_outcome_key(x) == _outcome_key(x)` is f(x)==f(x) and proves
    nothing. This drives both real paths against a shared store double, so a
    key derived differently by either one misses and the test fails.
    """
    store: dict[str, str] = {}

    class _Store:
        max_rows = 100

        def record(self, conn, key, content, generation_id=None):
            store[key.digest] = content

        def get(self, conn, key):
            hit = store.get(key.digest)
            if hit is None:
                return None
            return type("Rec", (), {"content": hit})()

    extractor.outcomes.store = _Store()

    jobs = [
        ModelCallJob(
            system="sys", user=f"u{i}", typed_input={"i": i}, item_ref=f"r{i}"
        )
        for i in range(3)
    ]

    # RECORD through the fan-out.
    extractor.outcomes.mode = "record"
    recorded = extractor._complete_many(object(), jobs, generation_id=1)
    assert all(o.ok for o in recorded)
    assert len(store) == 3, "fan-out did not persist one outcome per item"

    # REPLAY through the serial chokepoint, same jobs.
    extractor.outcomes.mode = "replay"
    replayed = extractor._complete_many(object(), jobs, generation_id=1)

    assert all(o.ok for o in replayed), (
        "replay missed outcomes the fan-out recorded — the two paths derive "
        "different OutcomeKeys"
    )
    assert [o.content for o in replayed] == [o.content for o in recorded]
    assert extractor.outcomes.exclusions == []


def test_the_parity_test_would_catch_a_drifting_key(extractor, monkeypatch) -> None:
    """Non-vacuity: perturb the key on one path and the round trip must fail."""
    store: dict[str, str] = {}

    class _Store:
        max_rows = 100

        def record(self, conn, key, content, generation_id=None):
            store[key.digest] = content

        def get(self, conn, key):
            return None if key.digest not in store else type(
                "Rec", (), {"content": store[key.digest]}
            )()

    extractor.outcomes.store = _Store()
    jobs = [ModelCallJob(system="s", user="u", typed_input={"i": 0}, item_ref="r0")]

    extractor.outcomes.mode = "record"
    extractor._complete_many(object(), jobs, generation_id=1)

    real = extractor._outcome_key
    monkeypatch.setattr(
        extractor, "_outcome_key",
        lambda system, typed_input: real(system + "-drifted", typed_input),
    )
    extractor.outcomes.mode = "replay"
    replayed = extractor._complete_many(object(), jobs, generation_id=1)

    assert not replayed[0].ok, "a drifted key still replayed — test is vacuous"


def test_key_changes_when_any_component_changes(extractor) -> None:
    """Proves the assertion above is not vacuous: if the key ignored its
    inputs, parity would hold trivially and prove nothing."""
    base = extractor._outcome_key("system", {"a": 1})

    assert extractor._outcome_key("different system", {"a": 1}) != base
    assert extractor._outcome_key("system", {"a": 2}) != base


def test_serial_chokepoint_does_not_rederive_the_key_inline(extractor) -> None:
    """The structural guarantee. `_keyed_complete` must call `_outcome_key`
    rather than constructing an OutcomeKey itself — an inline copy is exactly
    how the two paths drifted apart before.
    """
    src = inspect.getsource(base_mod.LLMExtractor._keyed_complete)
    assert "_outcome_key(" in src, "serial path stopped using the shared key"
    assert "OutcomeKey(" not in src, (
        "serial path constructs an OutcomeKey inline again; the fanned-out "
        "path derives its own, and they will drift"
    )


def test_fanout_path_uses_the_shared_key(extractor) -> None:
    src = inspect.getsource(base_mod.LLMExtractor._complete_many)
    assert "_outcome_key(" in src
    assert "OutcomeKey(" not in src


def test_model_id_participates_in_the_key(extractor) -> None:
    """Why automatic model downgrade on budget exhaustion is prohibited:
    switching models mid-run silently changes the key, converting recorded
    outcomes into replay misses."""
    before = extractor._outcome_key("system", {"a": 1})
    extractor.llm.model = "test/cheaper-model"
    after = extractor._outcome_key("system", {"a": 1})

    assert before != after
