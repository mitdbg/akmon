"""Keyed model outcomes — omega (PACT consistency-model-outcome-replay).

Named witness obligations from docs/develop/architecture/consistency.md §9.2:

* ``test_replay_reproduces_recorded_outputs_byte_equal`` — a recorded update
  replayed produces byte-identical extraction outputs, twice, with the model
  client provably not called.
* ``test_missing_outcome_is_loud_exclusion_never_fresh_sample`` — a replay
  miss records a per-step exclusion and raises; fresh sampling never happens
  in replay mode.
* ``test_version_change_never_hits_old_key`` — mutating the prompt content,
  model id, or operator version can never satisfy a lookup recorded under
  the old key; old-key rows age out only via retention.

Plus the retention / first-write-wins / key-purity contract of
``okg.substrate.llm.outcomes`` and the llm_calls digest column.
"""

from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.library.enrichers.extraction.claim_extractor import (  # noqa: E402
    ClaimExtractor,
)
from okg.substrate.llm.outcomes import (  # noqa: E402
    OutcomeKey,
    OutcomeRecorder,
    OutcomeStore,
    ReplayMissError,
    canonical_input_hash,
    prompt_content_hash,
)
from okg.substrate.llm.tracker import record_llm_call  # noqa: E402

_DEP = "fixture-outcomes"

_CLAIMS_JSON = (
    '[{"text": "X causes Y under condition Z.", '
    '"kind": "empirical", "confidence": 0.9}]'
)


class ScriptedClient:
    """Returns a fixed response; counts calls; asserts the digest arrives."""

    model = "scripted-model-1"

    def __init__(self, response: str) -> None:
        self.response = response
        self.calls: list[dict] = []

    def complete(self, system: str, user: str, *, fragment_id: str = "",
                 caller: str = "", generation_id=None,
                 outcome_key_digest: str | None = None) -> str:
        self.calls.append({
            "fragment_id": fragment_id,
            "outcome_key_digest": outcome_key_digest,
        })
        return self.response


class ExplodingClient:
    """A client that must never be called (replay mode's guarantee)."""

    model = "scripted-model-1"

    def __init__(self) -> None:
        self.calls = 0

    def complete(self, *args, **kwargs) -> str:
        self.calls += 1
        raise AssertionError(
            "replay mode called the model client — fresh sampling is banned"
        )


def _key(**overrides) -> OutcomeKey:
    fields = dict(
        step_id="enricher:fixture",
        operator_version="v1",
        model_id="scripted-model-1",
        prompt_hash=prompt_content_hash("SYSTEM PROMPT v1"),
        input_hash=canonical_input_hash({"doc": "d1", "body": "text"}),
    )
    fields.update(overrides)
    return OutcomeKey(**fields)


# --------------------------------------------------------------------------- #
# Key purity and loud construction (DB-free)                                   #
# --------------------------------------------------------------------------- #

def test_outcome_key_is_pure_and_component_sensitive() -> None:
    assert _key().digest == _key().digest
    base = _key().digest
    mutated = [
        _key(step_id="enricher:other"),
        _key(operator_version="v2"),
        _key(model_id="scripted-model-2"),
        _key(prompt_hash=prompt_content_hash("SYSTEM PROMPT v2")),
        _key(input_hash=canonical_input_hash({"doc": "d1", "body": "other"})),
    ]
    digests = {base, *(k.digest for k in mutated)}
    assert len(digests) == 6  # every component is identity-bearing


@pytest.mark.parametrize("field", [
    "step_id", "operator_version", "model_id", "prompt_hash", "input_hash",
])
def test_outcome_key_refuses_missing_components(field) -> None:
    with pytest.raises(ValueError) as excinfo:
        _key(**{field: ""})
    assert field in str(excinfo.value)


def test_replay_recorder_requires_store_and_valid_mode() -> None:
    with pytest.raises(ValueError):
        OutcomeRecorder(store=None, mode="replay")
    with pytest.raises(ValueError):
        OutcomeRecorder(store=None, mode="sample")


def test_store_refuses_unbounded_or_unlabeled_retention() -> None:
    with pytest.raises(ValueError):
        OutcomeStore(deployment=_DEP, max_rows=0)
    with pytest.raises(ValueError):
        OutcomeStore(deployment="", max_rows=10)


def test_keyed_complete_threads_digest_to_client() -> None:
    # Record mode with no store: the key is still computed and the digest
    # still rides the call record — retention is what the policy gates.
    client = ScriptedClient("out")
    recorder = OutcomeRecorder(store=None, mode="record")
    key = _key()
    out = recorder.keyed_complete(
        None, client, key, "SYSTEM PROMPT v1", "user", fragment_id="f1",
    )
    assert out == "out"
    assert client.calls[0]["outcome_key_digest"] == key.digest


# --------------------------------------------------------------------------- #
# Store semantics (postgres)                                                    #
# --------------------------------------------------------------------------- #

@pytest.mark.postgres
def test_first_write_wins_per_key(clean_runtime) -> None:
    store = OutcomeStore(deployment=f"{_DEP}-fww", max_rows=100)
    key = _key()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store.record(conn, key, "first outcome", generation_id=1)
        store.record(conn, key, "second fresh sample", generation_id=2)
        hit = store.get(conn, key)
    assert hit is not None
    assert hit.content == "first outcome"  # omega assigns ONE outcome per key


@pytest.mark.postgres
def test_record_mode_consumes_recorded_outcome_without_resampling(
    clean_runtime,
) -> None:
    """PINS the read-through stance (`outcome-read-through`, amending §9.2).

    REPLACES test_record_mode_samples_fresh_even_when_outcome_recorded,
    which pinned the original fresh-sampling stance. That stance was
    retired after its cost was measured with observed call counts (PR 925
    kill harness): under first-write-wins every repeat sample was billed
    and then DISCARDED by the store, so crash recovery and every publish
    cycle re-billed whole corpora for nothing durable. If this test starts
    failing because record mode resamples again, that is a doctrine
    reversal — update consistency-model.md §9.2 and the
    outcome-read-through PACT with it, not just this test.
    """
    from okg.substrate.llm.outcomes import OutcomeRecorder

    # Unique per run: okg.model_outcomes rows are deployment-scoped and
    # OUTLIVE clean_runtime's reset, and under read-through a stale
    # recording from a prior session would satisfy the "first" call.
    dep = f"{_DEP}-readthrough-{uuid.uuid4().hex[:8]}"
    store = OutcomeStore(deployment=dep, max_rows=100)
    recorder = OutcomeRecorder(store=store, mode="record")
    client = ScriptedClient("first sample")
    key = _key()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        first = recorder.keyed_complete(
            conn, client, key, "SYSTEM PROMPT v1", "user", fragment_id="f1",
        )
        second = recorder.keyed_complete(
            conn, client, key, "SYSTEM PROMPT v1", "user", fragment_id="f1",
        )
        hit = store.get(conn, key)
    assert first == second == "first sample"
    assert len(client.calls) == 1  # the second run consumed the recording
    assert hit is not None and hit.content == "first sample"


@pytest.mark.postgres
def test_fresh_draw_is_an_explicit_act_key_change_or_prune(
    clean_runtime,
) -> None:
    """The resample levers: a changed key component or pruned retention
    forces a fresh provider sample; an unchanged key never does."""
    from okg.substrate.llm.outcomes import OutcomeRecorder

    dep = f"{_DEP}-levers-{uuid.uuid4().hex[:8]}"  # see -readthrough note
    store = OutcomeStore(deployment=dep, max_rows=100)
    recorder = OutcomeRecorder(store=store, mode="record")
    client = ScriptedClient("draw")
    key_v1 = _key()
    key_v2 = _key(operator_version="v2")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        recorder.keyed_complete(
            conn, client, key_v1, "SYSTEM PROMPT v1", "user", fragment_id="f1",
        )
        assert len(client.calls) == 1

        # The negative arm FIRST: an unchanged key never re-calls. Without
        # this assertion the test passes under full doctrine reversal
        # (every other count matches fresh-sampling arithmetic).
        recorder.keyed_complete(
            conn, client, key_v1, "SYSTEM PROMPT v1", "user", fragment_id="f1",
        )
        assert len(client.calls) == 1  # consumed the recording, no new call

        # Lever 1: any key component change (here operator_version).
        recorder.keyed_complete(
            conn, client, key_v2, "SYSTEM PROMPT v1", "user", fragment_id="f1",
        )
        assert len(client.calls) == 2

        # Lever 2: retention pruning removes the recording. There is no
        # delete API by design — pruning IS the retention bound, so a
        # store capped at 1 row prunes v1 when v2 records.
        small = OutcomeStore(deployment=f"{dep}-cap", max_rows=1)
        capped = OutcomeRecorder(store=small, mode="record")
        capped.keyed_complete(
            conn, client, key_v1, "SYSTEM PROMPT v1", "user",
            fragment_id="f1",
        )
        capped.keyed_complete(
            conn, client, key_v2, "SYSTEM PROMPT v1", "user",
            fragment_id="f1",
        )
        assert small.get(conn, key_v1) is None  # pruned oldest-first
        before = len(client.calls)
        capped.keyed_complete(
            conn, client, key_v1, "SYSTEM PROMPT v1", "user",
            fragment_id="f1",
        )
        assert len(client.calls) == before + 1


@pytest.mark.postgres
def test_damaged_content_is_consumed_but_never_frozen(clean_runtime) -> None:
    """Truncated or empty content must not become a permanent recording.

    Under read-through a retained cut-off body would satisfy its key on
    every future run, silently and forever — before read-through, the
    per-cycle re-extraction accidentally self-healed such transients.
    The run still CONSUMES a truncated body (usability is the caller's
    judgment, unchanged); it is simply not retained, so the next run
    re-samples. Empty content is refused at the store boundary.
    """
    from okg.substrate.llm.outcomes import OutcomeRecorder

    class TruncatedClient(ScriptedClient):
        last_call_stats = {"truncated": True, "cost_usd": None}

    dep = f"{_DEP}-damaged-{uuid.uuid4().hex[:8]}"
    store = OutcomeStore(deployment=dep, max_rows=100)
    recorder = OutcomeRecorder(store=store, mode="record")
    client = TruncatedClient('{"cut off mid-')
    key = _key()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        first = recorder.keyed_complete(
            conn, client, key, "SYSTEM PROMPT v1", "user", fragment_id="f1",
        )
        assert first == '{"cut off mid-'      # the run consumes it
        assert store.get(conn, key) is None   # but nothing is frozen
        recorder.keyed_complete(
            conn, client, key, "SYSTEM PROMPT v1", "user", fragment_id="f1",
        )
        assert len(client.calls) == 2         # the next run re-sampled

        # Empty content: refused at the store boundary itself.
        store.record(conn, _key(input_hash=canonical_input_hash(
            {"empty": True})), "", generation_id=1)
        assert store.get(conn, _key(input_hash=canonical_input_hash(
            {"empty": True}))) is None


@pytest.mark.postgres
def test_record_mode_rerun_never_touches_the_provider_through_the_extractor(
    clean_runtime,
) -> None:
    """The fan-out dispatch path reads through too: a full RECORD-mode
    re-run over recorded inputs makes zero provider calls and yields
    identical candidates — the crash-recovery / next-cycle economics the
    kill harness measured at full re-billing before this existed."""
    dep = f"{_DEP}-rt-extractor-{uuid.uuid4().hex[:8]}"  # see -readthrough note
    scripted = ScriptedClient(_CLAIMS_JSON)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        recorded = _candidates(
            _extractor(scripted, replay=False, dep=dep), conn
        )
        assert recorded and scripted.calls

        exploding = ExplodingClient()
        rerun = _candidates(
            _extractor(exploding, replay=False, dep=dep), conn
        )
    assert rerun == recorded
    assert exploding.calls == 0  # record mode consumed the recordings


@pytest.mark.postgres
def test_retention_bound_prunes_oldest_first(clean_runtime) -> None:
    store = OutcomeStore(deployment=f"{_DEP}-ret", max_rows=2)
    keys = [_key(input_hash=canonical_input_hash({"n": i})) for i in range(3)]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for i, key in enumerate(keys):
            store.record(conn, key, f"outcome-{i}", generation_id=i)
        assert store.count(conn) == 2
        assert store.get(conn, keys[0]) is None          # oldest pruned
        assert store.get(conn, keys[1]) is not None
        assert store.get(conn, keys[2]) is not None


@pytest.mark.postgres
def test_version_change_never_hits_old_key(clean_runtime) -> None:
    store = OutcomeStore(deployment=f"{_DEP}-ver", max_rows=100)
    key = _key()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store.record(conn, key, "recorded under v1", generation_id=7)
        # Mutate each version-bearing component in turn: loud miss, never a
        # cross-version hit; the original key still resolves.
        for mutated in (
            _key(prompt_hash=prompt_content_hash("SYSTEM PROMPT v2")),
            _key(model_id="scripted-model-2"),
            _key(operator_version="v2"),
        ):
            assert store.get(conn, mutated) is None
        hit = store.get(conn, key)
    assert hit is not None and hit.content == "recorded under v1"


@pytest.mark.postgres
def test_key_digest_persisted_on_llm_calls(clean_runtime) -> None:
    key = _key()
    record_llm_call(
        clean_runtime,
        caller="enricher:fixture",
        model="scripted-model-1",
        fragment_id="f1",
        prompt_tokens=1, completion_tokens=1, total_tokens=2,
        cost_usd=None, latency_ms=5, success=True,
        deployment=_DEP, generation_id=1,
        outcome_key_digest=key.digest,
    )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT outcome_key_digest FROM okg.llm_calls "
            "WHERE deployment_name = %s ORDER BY id DESC LIMIT 1",
            (_DEP,),
        )
        row = cur.fetchone()
    assert row is not None and row[0] == key.digest


# --------------------------------------------------------------------------- #
# Named witnesses — through the real extractor                                  #
# --------------------------------------------------------------------------- #

_SOURCE_ROWS = [("node:p1", "paragraph", "Some empirical paragraph text.")]


def _extractor(llm, *, replay: bool, dep: str) -> ClaimExtractor:
    ex = ClaimExtractor(
        confidence_floor=0.6,
        llm=llm,
        model_outcomes_max_rows=100,
        model_outcomes_deployment=dep,
        replay_outcomes=replay,
        input_quarantine_max_attempts=3,
    )
    # The graph fetch is not under test; the keyed dispatch is.
    ex._fetch_source_rows = lambda conn, generation_id: list(_SOURCE_ROWS)
    return ex


def _candidates(extractor, conn) -> list[tuple]:
    return [
        (c.candidate_text, c.confidence, c.source_node_ref, tuple(sorted(
            (c.extra or {}).items())))
        for c in extractor.extract_candidates(conn, 1)
    ]


@pytest.mark.postgres
def test_replay_reproduces_recorded_outputs_byte_equal(clean_runtime) -> None:
    # Unique per run: under read-through, a leftover recording from a prior
    # session under a fixed deployment name satisfies the RECORD phase and
    # the scripted client is never consulted (okg.model_outcomes outlives
    # clean_runtime's reset).
    dep = f"{_DEP}-roundtrip-{uuid.uuid4().hex[:8]}"
    scripted = ScriptedClient(_CLAIMS_JSON)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        recorded = _candidates(
            _extractor(scripted, replay=False, dep=dep), conn
        )
        assert recorded, "fixture must yield at least one candidate"
        assert scripted.calls and scripted.calls[0]["outcome_key_digest"]

        exploding = ExplodingClient()
        replayed_once = _candidates(
            _extractor(exploding, replay=True, dep=dep), conn
        )
        replayed_twice = _candidates(
            _extractor(exploding, replay=True, dep=dep), conn
        )

    assert replayed_once == recorded
    assert replayed_twice == recorded
    assert exploding.calls == 0  # the model was provably never consulted


@pytest.mark.postgres
def test_missing_outcome_is_loud_exclusion_never_fresh_sample(
    clean_runtime,
) -> None:
    exploding = ExplodingClient()
    extractor = _extractor(exploding, replay=True, dep=f"{_DEP}-miss")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Nothing recorded for this deployment+key: the per-source loop's
        # failure handling skips the fragment, but the exclusion is recorded
        # BEFORE the raise — and no fresh sample ever happens.
        produced = _candidates(extractor, conn)
    assert produced == []
    assert exploding.calls == 0
    exclusions = extractor.outcomes.exclusions
    assert len(exclusions) == 1
    assert exclusions[0].step_id == "enricher:claim_extractor"
    assert exclusions[0].fragment_id == "claim:node:p1"
    assert exclusions[0].key_digest

    # Recorder-level: the miss is a loud typed error, not a silent skip.
    store = OutcomeStore(deployment=_DEP, max_rows=10)
    recorder = OutcomeRecorder(store=store, mode="replay")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(ReplayMissError):
            recorder.keyed_complete(
                conn, exploding, _key(input_hash=canonical_input_hash(
                    {"never": "recorded"})),
                "SYSTEM PROMPT v1", "user", fragment_id="f-miss",
            )
    assert exploding.calls == 0
