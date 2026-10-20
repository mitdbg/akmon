"""The append-only audit-record artifact (PACT chat-context-audit-filter).

Every assertion here is about a property a later consumer depends on and
cannot re-derive: that a turn produced two joined records, that a record id
addresses exactly one turn forever, that nothing was rewritten, and that an
audit that could not be written failed the turn instead of being skipped.

The failure mode these tests exist to exclude is a green run over an empty
artifact. A test that asserts "no bad record was written" passes trivially
when no record was written at all, so the positive counts are asserted in
the same tests as the negative properties.
"""

from __future__ import annotations

import json
import os
import stat
import threading
from pathlib import Path

import pytest

from okg.substrate.chat.audit_filter import (
    REFUSAL_AUDIT_UNREADABLE,
    REFUSAL_AUDIT_WRITE_FAILED,
    ContextAuditFilter,
    FilterSettings,
    TurnRefused,
)
from okg.substrate.chat.audit_record import (
    AUDIT_SCHEMA_NAME,
    AUDIT_SCHEMA_VERSION,
    PAYLOAD_DIGEST_FIELD,
    PAYLOAD_FIELD,
    STATE_HOME_ENV,
    AuditArtifact,
    GenerationAgreement,
    RecordKind,
    SchemaVersionConflict,
    TaskKind,
    UnknownSchemaVersion,
    canonical_payload,
    payload_sha256,
    read_records,
    resolve_artifact_path,
    resolve_audit_dir,
    resolve_chat_state_dir,
)

DEPLOYMENT = "cooper"
PRESET = "okg-cooper-analyst"
OWUI_VERSION = "0.11.0"


def _settings(tmp_path: Path, **overrides: object) -> FilterSettings:
    base = {
        "deployment": DEPLOYMENT,
        "preset": PRESET,
        "openwebui_version": OWUI_VERSION,
        "compaction_enabled": False,
        "state_home": tmp_path / "operator-state",
    }
    base.update(overrides)
    return FilterSettings(**base)  # type: ignore[arg-type]


def _body(*contents: str, model: str = "qwen3:32b") -> dict[str, object]:
    return {
        "model": model,
        "messages": [{"role": "user", "content": c} for c in contents],
        # Instance internals the model never sees. They must not reach the
        # recorded payload, or the record would claim the model was told
        # things it was not.
        "chat_id": "chat-abc",
        "session_id": "sess-xyz",
        "features": {"web_search": False},
    }


def _lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text("utf-8").splitlines() if line]


# --------------------------------------------------------------------------
# Paths: operator state, never a deployment graph database
# --------------------------------------------------------------------------


def test_artifact_lives_under_operator_state_not_a_deployment_database(
    tmp_path: Path,
) -> None:
    """The umbrella clause: Open WebUI-side state never sits in a graph DB."""
    settings = _settings(tmp_path)
    filt = ContextAuditFilter(settings)

    path = filt.artifact_path

    # Under the declared operator-state root, and a plain file path — not a
    # DSN, not a table, not something a deployment database would hold.
    assert path.is_relative_to(tmp_path / "operator-state")
    assert path.suffix == ".jsonl"
    assert DEPLOYMENT in path.parts and PRESET in path.parts


def test_state_home_env_resolves_when_no_argument_is_given(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The stated resolution rule, exercised on the env branch."""
    monkeypatch.setenv(STATE_HOME_ENV, str(tmp_path / "from-env"))

    resolved = resolve_chat_state_dir(DEPLOYMENT)

    assert resolved == tmp_path / "from-env" / DEPLOYMENT


# --------------------------------------------------------------------------
# One turn, two joined records
# --------------------------------------------------------------------------


def test_inlet_and_completion_records_per_turn_with_schema_version(
    tmp_path: Path,
) -> None:
    filt = ContextAuditFilter(_settings(tmp_path))

    inlet = filt.inlet(_body("what regions exist?"), metadata={"chat_id": "c1", "message_id": "m1"})
    completion = filt.complete_turn(
        inlet["record_id"],
        envelopes=[{"pinned_generation_id": "okg:cooper:gen:1"}],
    )

    records = _lines(filt.artifact_path)
    assert len(records) == 2, "one user turn must produce exactly two records"

    assert inlet["record_kind"] == RecordKind.INLET.value
    assert completion["record_kind"] == RecordKind.COMPLETION.value
    assert completion["record_id"] == inlet["record_id"], "the join key"

    for record in records:
        assert record["schema"] == AUDIT_SCHEMA_NAME
        assert record["schema_version"] == AUDIT_SCHEMA_VERSION

    assert inlet["task_kind"] == TaskKind.USER_TURN.value
    assert inlet["conversation_id"] == "c1"
    assert inlet["message_id"] == "m1"
    assert inlet["deployment"] == DEPLOYMENT
    assert inlet["preset"] == PRESET

    # The digest is over exactly the bytes in the named field, so a consumer
    # can verify it without re-deriving anyone's serialization rules.
    assert inlet[PAYLOAD_DIGEST_FIELD] == payload_sha256(inlet[PAYLOAD_FIELD])
    assert inlet[PAYLOAD_DIGEST_FIELD] != payload_sha256("")

    # The recorded payload is the in-scope subtree and nothing else.
    payload = json.loads(inlet[PAYLOAD_FIELD])
    assert set(payload) == {"messages", "model"}
    assert "session_id" not in inlet[PAYLOAD_FIELD]

    # The observed generation lives on the completion record, where it was
    # knowable — never on the inlet record, which was written first.
    assert completion["observed_generation_id"] == "okg:cooper:gen:1"
    assert "observed_generation_id" not in inlet


def test_completion_record_states_a_turn_that_read_no_graph_rows(
    tmp_path: Path,
) -> None:
    """"No graph read" is stated, not encoded as a missing generation."""
    filt = ContextAuditFilter(_settings(tmp_path))
    inlet = filt.inlet(_body("hello"))

    completion = filt.complete_turn(inlet["record_id"], envelopes=[])

    assert completion["graph_read"] is False
    assert completion["generation_agreement"] == GenerationAgreement.NO_GRAPH_READ.value
    assert completion["observed_generation_id"] is None

    # And the distinguishable neighbour: a read that returned no generation.
    inlet2 = filt.inlet(_body("hello again"))
    completion2 = filt.complete_turn(inlet2["record_id"], envelopes=[{"results": []}])
    assert completion2["graph_read"] is True
    assert completion2["generation_agreement"] == GenerationAgreement.MISSING.value
    assert completion2["observed_generation_id"] is None
    assert (
        completion2["generation_agreement"] != completion["generation_agreement"]
    ), "a turn that read nothing and a read with no generation are different facts"


def test_completion_record_carries_no_trust_level(tmp_path: Path) -> None:
    """Decision B: nothing produces a per-call trust_level, so nothing claims one."""
    filt = ContextAuditFilter(_settings(tmp_path))
    inlet = filt.inlet(_body("hi"))

    completion = filt.complete_turn(
        inlet["record_id"], envelopes=[{"pinned_generation_id": "g"}]
    )

    assert "trust_level" not in completion
    assert "trust_level" not in filt.artifact_path.read_text("utf-8")


# --------------------------------------------------------------------------
# Record identity
# --------------------------------------------------------------------------


def test_record_ids_unique_and_monotonic_across_history_rewrite(
    tmp_path: Path,
) -> None:
    """A rewritten turn is a NEW record, even at the same (chat, message).

    Open WebUI lets a user edit an earlier message and resend, which reuses
    the conversation and message ids. An identity derived from that pair
    would collide, and the collision would look like a duplicate rather than
    like two different payloads sent to the model.
    """
    filt = ContextAuditFilter(_settings(tmp_path))
    meta = {"chat_id": "c1", "message_id": "m1"}

    first = filt.inlet(_body("original question"), metadata=meta)
    second = filt.inlet(_body("edited question"), metadata=dict(meta))

    assert (first["conversation_id"], first["message_id"]) == (
        second["conversation_id"],
        second["message_id"],
    ), "the collision this test is about must actually be present"

    assert first["record_id"] != second["record_id"]
    assert second["record_seq"] > first["record_seq"]
    assert first[PAYLOAD_DIGEST_FIELD] != second[PAYLOAD_DIGEST_FIELD]

    # Injective across artifacts too: a second artifact for a different
    # preset must not re-mint the first artifact's ids.
    other = ContextAuditFilter(_settings(tmp_path, preset="okg-cooper-reader"))
    elsewhere = other.inlet(_body("original question"), metadata=dict(meta))
    assert elsewhere["record_id"] != first["record_id"]


def test_record_ids_do_not_restart_after_the_process_does(tmp_path: Path) -> None:
    """Sequence state is recovered from the artifact, not held in memory."""
    first = ContextAuditFilter(_settings(tmp_path)).inlet(_body("one"))
    # A brand-new filter object stands in for a restarted worker.
    second = ContextAuditFilter(_settings(tmp_path)).inlet(_body("two"))

    assert second["record_seq"] == first["record_seq"] + 1
    assert second["record_id"] != first["record_id"]
    assert second["artifact_id"] == first["artifact_id"]


# --------------------------------------------------------------------------
# Auxiliary calls are labeled, not counted as turns
# --------------------------------------------------------------------------


def test_auxiliary_task_calls_labeled_not_counted_as_turns(tmp_path: Path) -> None:
    """Open WebUI's own model calls must not be audited as the user's turn."""
    filt = ContextAuditFilter(_settings(tmp_path))

    user = filt.inlet(_body("real question"), metadata={"chat_id": "c1"})
    title = filt.inlet(_body("summarize"), metadata={"task": "title_generation"})
    tags = filt.inlet(_body("tag this"), metadata={"task": "tags_generation"})
    summary = filt.inlet(_body("compact this"), metadata={"task": "context_compaction"})
    future = filt.inlet(_body("???"), metadata={"task": "some_future_task"})

    assert user["task_kind"] == TaskKind.USER_TURN.value
    assert title["task_kind"] == TaskKind.TITLE_GENERATION.value
    assert tags["task_kind"] == TaskKind.TAGS_GENERATION.value
    assert summary["task_kind"] == TaskKind.CONTEXT_COMPACTION.value
    # An unrecognized task is auxiliary. Promoting it to a user turn would
    # silently compare the wrong capture in the equality proof the day Open
    # WebUI adds a task.
    assert future["task_kind"] == TaskKind.OTHER_AUXILIARY.value

    turns = [
        r
        for r in _lines(filt.artifact_path)
        if r.get("task_kind") == TaskKind.USER_TURN.value
    ]
    assert len(turns) == 1


# --------------------------------------------------------------------------
# Append-only
# --------------------------------------------------------------------------


def test_records_are_append_only(tmp_path: Path) -> None:
    filt = ContextAuditFilter(_settings(tmp_path))

    first = filt.inlet(_body("one"), metadata={"chat_id": "c1"})
    after_first = filt.artifact_path.read_bytes()
    filt.complete_turn(first["record_id"], envelopes=[{"pinned_generation_id": "g1"}])
    second = filt.inlet(_body("two"), metadata={"chat_id": "c1"})
    filt.complete_turn(second["record_id"], envelopes=[{"pinned_generation_id": "g2"}])

    final = filt.artifact_path.read_bytes()
    assert final.startswith(after_first), "earlier bytes are never rewritten"
    assert len(_lines(filt.artifact_path)) == 4
    assert len(final) > len(after_first)


# --------------------------------------------------------------------------
# Schema version
# --------------------------------------------------------------------------


def test_producer_version_change_starts_new_artifact(tmp_path: Path) -> None:
    """A changed major refuses the old file and opens a new one."""
    settings = _settings(tmp_path)
    old_path = resolve_artifact_path(
        DEPLOYMENT, PRESET, state_home=settings.state_home
    )
    AuditArtifact(path=old_path).append_inlet(
        deployment=DEPLOYMENT,
        preset=PRESET,
        conversation_id="c1",
        message_id="m1",
        task_kind=TaskKind.USER_TURN,
        principal=None,
        model="m",
        provider=None,
        compaction_enabled=False,
        payload=canonical_payload({"messages": [], "model": "m"}),
    )
    old_bytes = old_path.read_bytes()

    # The producer's major moved on.
    future = AuditArtifact(path=old_path, schema_version="2.0")
    with pytest.raises(SchemaVersionConflict):
        future.append_inlet(
            deployment=DEPLOYMENT,
            preset=PRESET,
            conversation_id="c2",
            message_id="m2",
            task_kind=TaskKind.USER_TURN,
            principal=None,
            model="m",
            provider=None,
            compaction_enabled=False,
            payload=canonical_payload({"messages": [], "model": "m"}),
        )
    assert old_path.read_bytes() == old_bytes, "the refusal wrote nothing"

    new_path = resolve_artifact_path(
        DEPLOYMENT, PRESET, schema_version="2.0", state_home=settings.state_home
    )
    assert new_path != old_path
    AuditArtifact(path=new_path, schema_version="2.0").append_inlet(
        deployment=DEPLOYMENT,
        preset=PRESET,
        conversation_id="c2",
        message_id="m2",
        task_kind=TaskKind.USER_TURN,
        principal=None,
        model="m",
        provider=None,
        compaction_enabled=False,
        payload=canonical_payload({"messages": [], "model": "m"}),
    )
    assert len(_lines(new_path)) == 1
    assert old_path.read_bytes() == old_bytes, "no version mixing in one file"

    # And the same major still reuses the same file.
    assert resolve_artifact_path(
        DEPLOYMENT, PRESET, state_home=settings.state_home
    ) == old_path


def test_reader_refuses_an_unrecognized_schema_version(tmp_path: Path) -> None:
    """A refusal, not a best-effort parse and not an empty list."""
    path = tmp_path / "records-001.jsonl"
    path.write_text(
        json.dumps(
            {
                "schema": AUDIT_SCHEMA_NAME,
                "schema_version": "99.0",
                "record_kind": "inlet",
                PAYLOAD_FIELD: "{}",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(UnknownSchemaVersion):
        read_records(path)


def test_reader_refuses_a_file_that_is_not_this_artifact(tmp_path: Path) -> None:
    path = tmp_path / "records-001.jsonl"
    path.write_text(json.dumps({"schema": "something.else"}) + "\n", encoding="utf-8")

    with pytest.raises(UnknownSchemaVersion):
        read_records(path)


def test_reader_refuses_a_missing_artifact(tmp_path: Path) -> None:
    """Absent is a refusal, never an empty read that means "no turns"."""
    with pytest.raises(FileNotFoundError):
        read_records(tmp_path / "does-not-exist.jsonl")


# --------------------------------------------------------------------------
# A failed write fails the turn
# --------------------------------------------------------------------------


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores the mode bits this test sets")
def test_write_failure_fails_the_turn(tmp_path: Path) -> None:
    """An unwritable artifact refuses; it does not answer unaudited."""
    settings = _settings(tmp_path)
    filt = ContextAuditFilter(settings)
    audit_dir = filt.artifact_path.parent
    audit_dir.mkdir(parents=True, exist_ok=True)
    filt.artifact_path.touch()
    filt.artifact_path.chmod(stat.S_IRUSR)  # read-only: append will fail

    try:
        with pytest.raises(TurnRefused) as raised:
            filt.inlet(_body("this must not be answered"))
    finally:
        filt.artifact_path.chmod(stat.S_IRUSR | stat.S_IWUSR)

    assert raised.value.reason == REFUSAL_AUDIT_WRITE_FAILED
    assert str(raised.value), "a refusal must name its reason, never be silent"
    # No partial and no empty-payload record.
    assert filt.artifact_path.read_bytes() == b""


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores the mode bits this test sets")
def test_an_unwritable_directory_refuses_with_a_named_reason(tmp_path: Path) -> None:
    """The lock is opened BEFORE the record is written, so it fails first.

    A read-only audit directory surfaces at the lock open, not at the
    append. Without a wrapper there, the user gets a bare
    "[Errno 30] Read-only file system" with nothing naming the filter or
    the reason — which is what the live measurement caught.
    """
    filt = ContextAuditFilter(_settings(tmp_path))
    audit_dir = filt.artifact_path.parent
    audit_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.chmod(stat.S_IRUSR | stat.S_IXUSR)

    try:
        with pytest.raises(TurnRefused) as raised:
            filt.inlet(_body("this must not be answered"))
    finally:
        audit_dir.chmod(stat.S_IRWXU)

    assert raised.value.reason == REFUSAL_AUDIT_WRITE_FAILED
    assert "audit" in str(raised.value).lower()
    assert not filt.artifact_path.exists()


def test_empty_payload_is_refused_rather_than_recorded(tmp_path: Path) -> None:
    """A record with an empty payload would read as an audited empty prompt."""
    artifact = AuditArtifact(path=tmp_path / "records-001.jsonl")

    with pytest.raises(Exception) as raised:
        artifact.append_inlet(
            deployment=DEPLOYMENT,
            preset=PRESET,
            conversation_id=None,
            message_id=None,
            task_kind=TaskKind.USER_TURN,
            principal=None,
            model=None,
            provider=None,
            compaction_enabled=False,
            payload="",
        )

    assert getattr(raised.value, "reason", "") == "audit-payload-empty"
    assert not (tmp_path / "records-001.jsonl").exists()


# --------------------------------------------------------------------------
# Canonicalization
# --------------------------------------------------------------------------


def test_concurrent_appends_do_not_collide_on_record_id(tmp_path: Path) -> None:
    """The read-then-append race, driven rather than argued about.

    ``_scan`` reads the next sequence and ``_append`` writes it. Without a
    lock spanning both, two writers read the same sequence and mint the SAME
    record_id — and two that each find an empty file mint two artifact ids
    into one file. Two Open WebUI workers, or two replicas over a shared
    volume, are enough. `chat-transcript-source` maps this id to
    `source_record_id`, so a collision lands in the graph.
    """
    workers = 8
    filt = ContextAuditFilter(_settings(tmp_path))
    barrier = threading.Barrier(workers)
    results: list[dict] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def append(index: int) -> None:
        try:
            barrier.wait(timeout=30)
            record = ContextAuditFilter(_settings(tmp_path)).inlet(
                _body(f"concurrent {index}")
            )
            with lock:
                results.append(record)
        except Exception as exc:
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=append, args=(i,)) for i in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert not errors, f"a concurrent append failed: {errors[:2]}"
    assert len(results) == workers, "the race did not actually run"

    records = _lines(filt.artifact_path)
    assert len(records) == workers
    ids = [r["record_id"] for r in records]
    assert len(set(ids)) == workers, f"record_id collision: {sorted(ids)}"
    seqs = sorted(r["record_seq"] for r in records)
    assert seqs == list(range(1, workers + 1)), f"sequence gaps/dupes: {seqs}"
    assert len({r["artifact_id"] for r in records}) == 1, (
        "one artifact must carry exactly one artifact_id"
    )


def test_an_uncommitted_torn_final_line_is_repaired_not_fatal(tmp_path: Path) -> None:
    """A process killed mid-append must not brick the artifact forever.

    A final line with no newline is by definition an append that never
    committed. Refusing every later turn on it — with no stated recovery —
    turns one lost record into a dead deployment.
    """
    filt = ContextAuditFilter(_settings(tmp_path))
    first = filt.inlet(_body("committed"))
    committed = filt.artifact_path.read_bytes()

    # A half-written record, exactly as a kill between write and flush
    # would leave it.
    with filt.artifact_path.open("a", encoding="utf-8") as handle:
        handle.write('{"schema": "okg.chat.audit_record", "record_ki')

    second = filt.inlet(_body("after the tear"))

    records = _lines(filt.artifact_path)
    assert len(records) == 2
    assert records[0]["record_id"] == first["record_id"], "the committed record survived"
    assert second["record_seq"] == first["record_seq"] + 1
    assert filt.artifact_path.read_bytes().startswith(committed)

    # But a corrupt line that is NOT the uncommitted tail still refuses: it
    # was committed once, so something else damaged it.
    text = filt.artifact_path.read_text("utf-8").splitlines(keepends=True)
    text[0] = '{"schema": "okg.chat.audit_record", "broken\n'
    filt.artifact_path.write_text("".join(text), encoding="utf-8")
    with pytest.raises(TurnRefused) as raised:
        filt.inlet(_body("must refuse"))
    # Named, not a bare traceback: a refusal with no reason is the silent
    # skip the contract forbids.
    assert raised.value.reason == REFUSAL_AUDIT_UNREADABLE
    assert str(raised.value)


def test_an_empty_rotation_target_does_not_absorb_every_major(
    tmp_path: Path,
) -> None:
    """An empty file matches any major; preferring it breaks the rule.

    With a major-1 artifact and an empty rotation target present, major 1
    must still resume its OWN file. Otherwise "a rollback resumes that
    major's file" is false exactly when a rotation has been started.
    """
    settings = _settings(tmp_path)
    first = resolve_artifact_path(DEPLOYMENT, PRESET, state_home=settings.state_home)
    ContextAuditFilter(settings).inlet(_body("major one"))
    assert first.exists()

    empty = first.with_name("records-002.jsonl")
    empty.touch()

    assert (
        resolve_artifact_path(DEPLOYMENT, PRESET, state_home=settings.state_home)
        == first
    ), "major 1 must resume its own file, not the empty rotation target"
    # A different major has nothing of its own, so it takes the empty file.
    assert (
        resolve_artifact_path(
            DEPLOYMENT, PRESET, schema_version="7.0", state_home=settings.state_home
        )
        == empty
    )


def test_the_artifact_resolver_refuses_a_foreign_jsonl(tmp_path: Path) -> None:
    """A records-NNN.jsonl that is not this artifact kind is not ours."""
    settings = _settings(tmp_path)
    directory = resolve_audit_dir(DEPLOYMENT, PRESET, state_home=settings.state_home)
    directory.mkdir(parents=True)
    (directory / "records-001.jsonl").write_text(
        json.dumps({"schema": "something.else", "schema_version": "1.0"}) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(UnknownSchemaVersion):
        resolve_artifact_path(DEPLOYMENT, PRESET, state_home=settings.state_home)


def test_the_inlet_record_names_the_raw_task_signal(tmp_path: Path) -> None:
    """"No task key" and "Open WebUI declared a user turn" stay distinct.

    Both derive `task_kind == user_turn`, so the derived field alone cannot
    tell them apart. The raw signal is recorded next to it.
    """
    filt = ContextAuditFilter(_settings(tmp_path))

    absent = filt.inlet(_body("x"), metadata={"chat_id": "c1"})
    declared = filt.inlet(_body("y"), metadata={"task": "title_generation"})

    assert absent["task_kind"] == TaskKind.USER_TURN.value
    assert absent["task_metadata_raw"] is None
    assert declared["task_metadata_raw"] == "title_generation"


def test_canonical_payload_is_key_order_independent_but_value_sensitive(
    tmp_path: Path,
) -> None:
    """Canonicalization defines the comparison; it does not excuse a diff."""
    a = canonical_payload({"model": "m", "messages": [{"role": "user", "content": "x"}]})
    b = canonical_payload({"messages": [{"content": "x", "role": "user"}], "model": "m"})
    assert a == b, "key order is not a difference"

    c = canonical_payload({"model": "m", "messages": [{"role": "user", "content": "y"}]})
    assert a != c, "a content difference is a difference"

    d = canonical_payload({"model": "other", "messages": [{"role": "user", "content": "x"}]})
    assert a != d, "the model identifier is in scope"


def test_canonical_payload_keeps_non_ascii_unescaped(tmp_path: Path) -> None:
    """ensure_ascii=False is part of the pinned form, so assert it holds."""
    payload = canonical_payload({"model": "m", "messages": [{"content": "région"}]})

    assert "région" in payload
    assert "\\u00e9" not in payload
