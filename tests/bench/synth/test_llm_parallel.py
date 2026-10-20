"""Concurrent-extraction witnesses for the LLM lane (no network, no DB).

The lane extracts documents with a bounded thread pool. These witnesses pin the
properties that make concurrency invisible to every downstream reader:

- artifact determinism: a parallel run's responses, claim snapshot, fact
  stream, and failure rows are IDENTICAL to a sequential run's, even when
  completion order inverts submission order;
- real overlap: with workers > 1, more than one document is genuinely in
  flight (a pool of one would satisfy determinism vacuously);
- auth fail-fast: a credentials refusal cancels unstarted documents and raises
  one BLOCKED error — never a per-document failure row, never a full sweep of
  doomed calls;
- per-document failure semantics survive the pool: a failing document is named
  with its attempt count while every other document lands;
- the concurrency knob is an environment variable with loud rejection of
  nonsense, and it never touches config digests (execution knob, not corpus
  semantics).
"""

from __future__ import annotations

import threading
import time as _time

import pytest

from okg.bench.synth.llm_extract import LlmExtractor
from okg.bench.synth.runner import (
    LlmLane,
    RunnerError,
    _default_llm_workers,
)

LABELS = ("Body Weight", "Body Height")


def _doc_text(index: int) -> str:
    return (
        f"# Patient P{index}\n\n## Measurements\n\n"
        f"Weight at this visit was {60 + index}.0 kg.\n"
    )


def _doc_response(text: str) -> str:
    value = text.split("was ")[1].split(".\n")[0]
    return (
        '[{"attribute": "Body Weight", "value": "' + value + '", '
        '"quote": "' + value + '"}]'
    )


def _corpus(tmp_path, count: int):
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    for index in range(count):
        (corpus / f"doc_{index}.md").write_text(_doc_text(index), encoding="utf-8")
    return corpus


def _lane(responder, max_workers: int) -> LlmLane:
    ex = LlmExtractor(labels=LABELS)
    ex._complete = responder  # type: ignore[method-assign] — network-free stub
    return LlmLane(extractor=ex, sleep=lambda _s: None, max_workers=max_workers)


# --------------------------------------------------------------------------- #
# determinism: parallel output == sequential output                           #
# --------------------------------------------------------------------------- #
def test_parallel_artifacts_are_byte_identical_to_sequential(tmp_path) -> None:
    """Completion order is inverted by construction (earlier docs sleep
    longer), and every recorded artifact still comes out in sorted document
    order, identical to the one-worker run."""
    corpus = _corpus(tmp_path, 6)

    def slow_responder(text: str) -> str:
        index = int(text.split("Patient P")[1].split("\n")[0])
        _time.sleep(0.05 * (6 - index))  # doc_0 finishes LAST
        return _doc_response(text)

    sequential = _lane(slow_responder, max_workers=1)
    seq_facts = sequential.extract(corpus, (), phase="clean")
    parallel = _lane(slow_responder, max_workers=4)
    par_facts = parallel.extract(corpus, (), phase="clean")

    assert parallel.responses["clean"] == sequential.responses["clean"]
    assert parallel.claim_snapshots["clean"] == sequential.claim_snapshots["clean"]
    assert parallel.failures["clean"] == sequential.failures["clean"] == []
    assert [f.node_id for f in par_facts] == [f.node_id for f in seq_facts]


class _Usage:
    prompt_tokens = 3
    completion_tokens = 5
    completion_tokens_details = None
    cost = 0.001


class _Response:
    usage = _Usage()


class _CountingLock:
    """A real lock that records every acquisition through it.

    Injected in place of the accumulator's own lock so the witness can assert
    that the mutations went THROUGH a lock, rather than hoping a race shows up.
    """

    def __init__(self) -> None:
        self._inner = threading.Lock()
        self.entries = 0
        self.max_occupancy = 0
        self._occupancy = 0

    def __enter__(self):
        self._inner.acquire()
        self.entries += 1
        self._occupancy += 1
        self.max_occupancy = max(self.max_occupancy, self._occupancy)
        return self

    def __exit__(self, *exc) -> None:
        self._occupancy -= 1
        self._inner.release()


def test_cost_accumulator_mutates_under_a_real_lock() -> None:
    """Every counter mutation goes through a lock that genuinely excludes.

    Hammering threads and asserting the totals does NOT test this: measured
    over 20 runs of a 16x50 hammer with the lock replaced by
    ``contextlib.nullcontext``, zero runs failed — CPython's switch interval
    makes the torn read-modify-write essentially unobservable at this scale.
    So the property is witnessed directly, in two halves that between them
    have no passing configuration without a real lock:

    1. the accumulator's own lock BLOCKS a second thread while held (a
       nullcontext or a no-op stand-in does not);
    2. with an instrumented lock injected, ``observe`` and
       ``bump_off_vocabulary`` are each recorded acquiring it, and the
       critical section is never occupied twice at once.
    """
    from okg.bench.synth.llm_extract import ExtractionCost

    # 1. the shipped lock excludes.
    shipped = ExtractionCost()._lock
    assert shipped.acquire(blocking=False) is True
    try:
        blocked: list[bool] = []
        contender = threading.Thread(
            target=lambda: blocked.append(shipped.acquire(timeout=0.2))
        )
        contender.start()
        contender.join()
        assert blocked == [False], (
            "the accumulator's lock does not exclude a second thread — a torn "
            "read-modify-write would silently under-report spend"
        )
    finally:
        shipped.release()

    # 2. every mutation acquires it.
    cost = ExtractionCost()
    probe = _CountingLock()
    cost._lock = probe  # type: ignore[assignment]

    def hammer() -> None:
        for _ in range(50):
            cost.observe(_Response())
            cost.bump_off_vocabulary()

    threads = [threading.Thread(target=hammer) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert probe.entries == 1600, (
        f"expected 800 observe + 800 bump acquisitions, saw {probe.entries} — "
        "a counter is being mutated outside the lock"
    )
    assert probe.max_occupancy == 1, "two threads inside the critical section"


def test_cost_accumulator_totals_every_counter_across_threads() -> None:
    """16 threads x 50 observations land exactly on every counter the run is
    billed for — including completion_tokens and cost_usd, which an
    under-report costs real money on."""
    from okg.bench.synth.llm_extract import ExtractionCost

    cost = ExtractionCost()

    def hammer() -> None:
        for _ in range(50):
            cost.observe(_Response())
            cost.bump_off_vocabulary()

    threads = [threading.Thread(target=hammer) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert cost.calls == 800
    assert cost.prompt_tokens == 2400
    assert cost.completion_tokens == 4000
    assert cost.reasoning_tokens == 0
    assert cost.cost_usd == pytest.approx(0.8)
    assert cost.off_vocabulary == 800


def test_parallel_run_genuinely_overlaps(tmp_path) -> None:
    """max_workers=4 must put more than one document in flight at once —
    otherwise the determinism witness above passes vacuously."""
    corpus = _corpus(tmp_path, 6)
    lock = threading.Lock()
    active = {"now": 0, "peak": 0}

    def tracking_responder(text: str) -> str:
        with lock:
            active["now"] += 1
            active["peak"] = max(active["peak"], active["now"])
        _time.sleep(0.05)
        with lock:
            active["now"] -= 1
        return _doc_response(text)

    _lane(tracking_responder, max_workers=4).extract(corpus, (), phase="clean")
    assert active["peak"] > 1, "no two documents were ever in flight together"


# --------------------------------------------------------------------------- #
# fail-closed semantics survive the pool                                      #
# --------------------------------------------------------------------------- #
def test_auth_error_cancels_pending_and_raises_blocked(tmp_path) -> None:
    corpus = _corpus(tmp_path, 8)
    lock = threading.Lock()
    started = {"count": 0}

    def auth_refusing_responder(text: str) -> str:
        with lock:
            started["count"] += 1
        if "Patient P0" in text:
            raise RuntimeError("openrouter refused: 401 unauthorized")
        _time.sleep(0.2)
        return _doc_response(text)

    lane = _lane(auth_refusing_responder, max_workers=2)
    with pytest.raises(RunnerError, match="BLOCKED"):
        lane.extract(corpus, (), phase="clean")
    # The bound is workers + a margin of two, not "fewer than 8": with 2 workers
    # only the refusing document and its co-resident can be in flight when the
    # refusal lands, plus at most one pickup per worker before the cancel is
    # observed. Measured over 15 runs the count is 2 or 3; `< 8` tolerated a
    # near-total sweep and is what let this witness read as passing.
    assert started["count"] <= 4, (
        "an auth refusal must cancel unstarted documents, not sweep the "
        f"corpus: {started['count']} of 8 documents started"
    )
    # A credentials refusal is a RUN-level block, never a per-document failure
    # row: recording rows here would let a doomed run look like partial
    # extraction with a few lost documents.
    assert lane.failures.get("clean", []) == []
    assert lane.responses.get("clean", []) == []


def test_failed_document_is_named_while_others_land_in_parallel(tmp_path) -> None:
    corpus = _corpus(tmp_path, 5)

    def flaky_responder(text: str) -> str:
        if "Patient P2" in text:
            raise ValueError("malformed model output")
        return _doc_response(text)

    lane = _lane(flaky_responder, max_workers=4)
    lane.extract(corpus, (), phase="clean")
    assert [row["doc"] for row in lane.failures["clean"]] == ["doc_2.md"]
    assert lane.failures["clean"][0]["scope"] == "document"
    assert lane.failures["clean"][0]["attempts"] == 3
    assert [row["doc"] for row in lane.responses["clean"]] == [
        "doc_0.md", "doc_1.md", "doc_3.md", "doc_4.md",
    ]


# --------------------------------------------------------------------------- #
# the knob                                                                    #
# --------------------------------------------------------------------------- #
def test_concurrency_env_knob_is_loud(monkeypatch) -> None:
    monkeypatch.setenv("OKG_SYNTH_LLM_CONCURRENCY", "3")
    assert _default_llm_workers() == 3
    monkeypatch.setenv("OKG_SYNTH_LLM_CONCURRENCY", "0")
    with pytest.raises(RunnerError, match=">= 1"):
        _default_llm_workers()
    monkeypatch.setenv("OKG_SYNTH_LLM_CONCURRENCY", "many")
    with pytest.raises(RunnerError, match="not an integer"):
        _default_llm_workers()
