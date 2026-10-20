"""PACT chat-transcript-source — req.chat-transcript-source.ratings-and-audit-carriage.

Ratings and arena preferences become facts, and the context-audit artifact is
consumed through its own named ``payload`` field. The two refusal tests are the
load-bearing ones: an artifact this reader cannot read must produce NO
``chat_context_record`` fact and NO empty-or-defaulted context anywhere in the
emitted stream. A run that merely drops the record silently fails them.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from okg.substrate.chat.audit_record import (
    AUDIT_SCHEMA_VERSION,
    PAYLOAD_DIGEST_FIELD,
    PAYLOAD_FIELD,
    SOURCE_REVISION,
    payload_sha256,
    resolve_audit_dir,
)
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.chat_sessions import (
    REFUSAL_AUDIT_ARTIFACT_MISSING,
    REFUSAL_AUDIT_SCHEMA_VERSION_UNKNOWN,
    ChatSessionsSource,
)
from tests.substrate.library.sources import _chat_sessions_fixture as fx


INSTANCE = "test-instance"
DEPLOYMENT = "chat-source-test"
PRESET = "workspace"


def _source(tmp_path: Path) -> ChatSessionsSource:
    return ChatSessionsSource(
        app_db_path=tmp_path / "chat" / "app.db",
        instance=INSTANCE,
        deployment=DEPLOYMENT,
        preset=PRESET,
        audit_state_home=tmp_path / "state",
    )


def _run(tmp_path: Path) -> tuple[ChatSessionsSource, list[Any]]:
    source = _source(tmp_path)
    return source, list(source.run("run-1", mode="scope_complete").facts)


def _nodes(facts: list[Any], subtype: str) -> list[NodeFact]:
    return [
        fact for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == subtype
    ]


def test_rating_fact_carries_message_principal_and_timestamp(
    tmp_path: Path,
) -> None:
    fx.build_app_db(tmp_path / "chat" / "app.db")
    fx.write_audit_artifact(
        tmp_path / "state", deployment=DEPLOYMENT, preset=PRESET,
    )
    source, facts = _run(tmp_path)

    ratings = _nodes(facts, "chat_rating")
    assert len(ratings) == 1, [node.node_id for node in ratings]
    attrs = ratings[0].attrs
    ids = fx.node_ids(INSTANCE)
    assert attrs["rated_message_id"] == ids["graph_answer"]
    assert attrs["principal"] == fx.PRINCIPAL
    assert attrs["rating"] == "1"
    assert attrs["model_id"] == fx.MODEL_WITH_GRAPH
    assert attrs["reason"] == "accurate_information"
    assert attrs["rated_at"].startswith("2026-"), attrs["rated_at"]

    # The rating hangs off the message it is about, not off the conversation.
    contains = {
        (edge.src, edge.dst)
        for edge in facts
        if getattr(edge, "edge_type", None) == "contains"
    }
    assert (ids["graph_answer"], ids["rating"]) in contains
    assert source.last_run_stats["ratings_emitted"] == 1


def test_arena_preference_carries_both_models_and_chosen_side(
    tmp_path: Path,
) -> None:
    fx.build_app_db(tmp_path / "chat" / "app.db")
    fx.write_audit_artifact(
        tmp_path / "state", deployment=DEPLOYMENT, preset=PRESET,
    )
    source, facts = _run(tmp_path)

    preferences = _nodes(facts, "arena_preference")
    assert len(preferences) == 1, [node.node_id for node in preferences]
    attrs = preferences[0].attrs
    assert attrs["chosen_model_id"] == fx.MODEL_WITH_GRAPH
    assert attrs["sibling_model_ids"] == fx.MODEL_WITHOUT_GRAPH
    assert set(attrs["compared_model_ids"].split(",")) == {
        fx.MODEL_WITH_GRAPH, fx.MODEL_WITHOUT_GRAPH,
    }
    assert attrs["principal"] == fx.PRINCIPAL
    assert attrs["preferred_at"].startswith("2026-")
    # An arena feedback is NOT also counted as a plain rating: the two carry
    # different meanings and double-counting would bias every leaderboard
    # computed from the graph.
    assert source.last_run_stats["arena_preferences_emitted"] == 1
    assert source.last_run_stats["ratings_emitted"] == 1


def test_context_record_carries_digest_and_pointer_never_raw_payload(
    tmp_path: Path,
) -> None:
    fx.build_app_db(tmp_path / "chat" / "app.db")
    record = fx.write_audit_artifact(
        tmp_path / "state", deployment=DEPLOYMENT, preset=PRESET,
    )
    source, facts = _run(tmp_path)

    contexts = _nodes(facts, "chat_context_record")
    assert len(contexts) == 1, [node.node_id for node in contexts]
    node = contexts[0]
    attrs = node.attrs
    assert attrs["audit_record_id"] == record["record_id"]
    assert attrs["payload_sha256"] == record[PAYLOAD_DIGEST_FIELD]
    assert attrs["payload_sha256"] == payload_sha256(record[PAYLOAD_FIELD])
    assert attrs["audit_schema_version"] == AUDIT_SCHEMA_VERSION
    assert attrs["observed_generation_id"] == "g-000042"
    assert attrs["generation_agreement"] == "single"
    assert attrs["graph_read"] is True
    assert attrs["task_kind"] == "user_turn"

    # The artifact record_id is INJECTIVE, so it maps to source_record_id; the
    # revision is fixed because records are immutable.
    assert node.source_record_id["audit_record_id"] == record["record_id"]
    assert node.source_revision == {"audit_source_revision": SOURCE_REVISION}

    # The BYTES stay in the artifact. Only a bounded excerpt travels.
    full_payload = record[PAYLOAD_FIELD]
    assert len(attrs["payload_excerpt"]) <= source.excerpt_chars
    assert attrs["payload_chars"] == len(full_payload)
    if len(full_payload) > source.excerpt_chars:
        assert attrs["payload_excerpt"] != full_payload

    ids = fx.node_ids(INSTANCE)
    contains = {
        (edge.src, edge.dst)
        for edge in facts
        if getattr(edge, "edge_type", None) == "contains"
    }
    assert (ids["graph_answer"], node.node_id) in contains


def _assert_no_context_anywhere(facts: list[Any]) -> None:
    """No context node, and no empty/defaulted context attribute either."""
    assert _nodes(facts, "chat_context_record") == []
    for fact in facts:
        attrs = getattr(fact, "attrs", {}) or {}
        for key in (
            "payload_sha256",
            "payload_excerpt",
            "audit_record_id",
            "audit_schema_version",
        ):
            assert key not in attrs, (
                f"a defaulted context field {key!r} leaked onto {fact}"
            )


def test_missing_audit_artifact_refuses_with_named_reason(
    tmp_path: Path,
) -> None:
    fx.build_app_db(tmp_path / "chat" / "app.db")
    # No artifact written at all.
    source, facts = _run(tmp_path)

    assert facts, "the run emitted nothing, so the assertion would be vacuous"
    _assert_no_context_anywhere(facts)
    refusal = source.last_run_stats["audit_refusal"]
    assert refusal["reason"] == REFUSAL_AUDIT_ARTIFACT_MISSING
    assert refusal["detail"]
    assert source.last_run_stats["context_records_emitted"] == 0
    # The conversations themselves still ingest: an unreadable audit artifact
    # is not a reason to lose the transcript.
    assert _nodes(facts, "chat_message")


def test_unknown_audit_schema_version_refuses_with_named_reason(
    tmp_path: Path,
) -> None:
    fx.build_app_db(tmp_path / "chat" / "app.db")
    fx.write_audit_artifact(
        tmp_path / "state",
        deployment=DEPLOYMENT,
        preset=PRESET,
        schema_version="7.0",
    )
    audit_dir = resolve_audit_dir(
        DEPLOYMENT, PRESET, state_home=tmp_path / "state",
    )
    written = sorted(audit_dir.glob("records-*.jsonl"))
    assert written, "the fixture wrote no artifact to mis-version"
    head = json.loads(written[0].read_text().splitlines()[0])
    assert head["schema_version"] == "7.0", head

    source, facts = _run(tmp_path)

    assert facts
    _assert_no_context_anywhere(facts)
    refusal = source.last_run_stats["audit_refusal"]
    assert refusal["reason"] == REFUSAL_AUDIT_SCHEMA_VERSION_UNKNOWN
    assert "7.0" in refusal["detail"]
    assert source.last_run_stats["context_records_emitted"] == 0


def test_an_audit_refusal_does_not_retract_context_already_published(
    tmp_path: Path,
) -> None:
    """A READ FAILURE must not erase recorded context.

    Emitting no fact is right. Also dropping the node from the record set is
    not: under a completed scope the runner reads the disappearance as a
    deletion and synthesizes a retraction, so a missing artifact would delete
    the context that WAS recorded. That is worse than "no context was sent" —
    it is "the context that existed is gone".
    """
    from okg.substrate.chat.audit_record import resolve_audit_dir as _dir

    fx.build_app_db(tmp_path / "chat" / "app.db")
    fx.write_audit_artifact(
        tmp_path / "state", deployment=DEPLOYMENT, preset=PRESET,
    )
    source = _source(tmp_path)
    first = source.run("run-1", mode="scope_complete")
    list(first.facts)
    context_ids = {
        node.node_id
        for emission in first.record_set.records.values()
        for node in emission.descendants
        if node.subtype == "chat_context_record"
    }
    assert context_ids, "no context record was published, so this is moot"

    # The artifact goes away between runs.
    for artifact in _dir(DEPLOYMENT, PRESET, state_home=tmp_path / "state").glob(
        "records-*.jsonl"
    ):
        artifact.unlink()

    cursor = dict(first.next_cursor)
    cursor["record_set"] = first.record_set.to_cursor()
    source = _source(tmp_path)
    second = source.run("run-2", mode="scope_complete", cursor=cursor)
    list(second.facts)

    assert source.last_run_stats["audit_refusal"]["reason"] == (
        REFUSAL_AUDIT_ARTIFACT_MISSING
    )
    assert second.record_set is not None
    still_declared = {
        node.node_id
        for emission in second.record_set.records.values()
        for node in emission.descendants
        if node.subtype == "chat_context_record"
    }
    assert still_declared == context_ids, (
        "an audit read failure dropped context records from the record set; "
        "the runner would retract them"
    )
    assert source.last_run_stats["context_records_carried"] == len(context_ids)
    # And the edges that hang them off their messages are carried too.
    carried_edges = {
        (edge.src, edge.dst)
        for emission in second.record_set.records.values()
        for edge in emission.edges
        if edge.dst in context_ids
    }
    assert len(carried_edges) == len(context_ids)


def test_an_audit_refusal_with_unreadable_prior_authority_declines_scope(
    tmp_path: Path,
) -> None:
    """When the prior footprint cannot be read, retraction is suppressed.

    An external (by-reference) record set is not readable from inside the
    adapter, so it cannot carry the context forward. Declaring a completed
    scope anyway would retract; declining it is the fail-safe direction.
    """
    fx.build_app_db(tmp_path / "chat" / "app.db")
    source = _source(tmp_path)
    run = source.run(
        "run-1",
        mode="scope_complete",
        cursor={"record_set_ref": {"kind": "external", "key": "abc"}},
    )
    list(run.facts)

    assert source.last_run_stats["audit_refusal"]
    assert run.completed_scope is False
    assert run.record_set is None
    assert source.last_run_stats["retraction_suppressed"]["reason"] == (
        "audit-refusal-without-readable-prior-record-set"
    )


def test_an_artifact_written_after_ingest_still_emits_its_context(
    tmp_path: Path,
) -> None:
    """The digest sees the artifact, not only the app database.

    The completion record is appended AFTER the inlet by construction, and the
    artifact may appear long after the conversation was first ingested. If the
    per-conversation digest ignored it, the run would say "unchanged" and the
    chat_context_record fact would be declared in the record set and never
    created.
    """
    fx.build_app_db(tmp_path / "chat" / "app.db")
    source = _source(tmp_path)
    first = source.run("run-1", mode="scope_complete")
    assert list(first.facts)
    assert _nodes(list(source.run("probe", mode="scope_complete").facts),
                  "chat_context_record") == []

    fx.write_audit_artifact(
        tmp_path / "state", deployment=DEPLOYMENT, preset=PRESET,
    )
    source = _source(tmp_path)
    second = source.run("run-2", mode="scope_complete",
                        cursor=dict(first.next_cursor))
    facts = list(second.facts)

    emitted = _nodes(facts, "chat_context_record")
    assert emitted, (
        "the audit artifact appeared after ingest and its context record was "
        "never emitted; the conversation digest is blind to the artifact"
    )
    assert source.last_run_stats["context_records_emitted"] == len(emitted)
    declared = {
        node.node_id
        for emission in second.record_set.records.values()
        for node in emission.descendants
        if node.subtype == "chat_context_record"
    }
    assert {node.node_id for node in emitted} == declared, (
        "a node is declared in the record set but was never emitted as a fact"
    )


def test_a_regenerated_turn_supersedes_and_is_counted(tmp_path: Path) -> None:
    """Two inlets for one message: the newest wins, and the loss is counted."""
    fx.build_app_db(tmp_path / "chat" / "app.db")
    fx.write_audit_artifact(
        tmp_path / "state", deployment=DEPLOYMENT, preset=PRESET,
        payload_text="first attempt",
    )
    second_record = fx.write_audit_artifact(
        tmp_path / "state", deployment=DEPLOYMENT, preset=PRESET,
        payload_text="second attempt after regeneration",
    )
    source, facts = _run(tmp_path)

    contexts = _nodes(facts, "chat_context_record")
    assert len(contexts) == 1
    assert contexts[0].attrs["audit_record_id"] == second_record["record_id"]
    warnings = source.last_run_stats["audit_warnings"]
    assert any(
        warning["reason"] == "audit-turn-superseded-by-later-record"
        and warning["count"] == 1
        for warning in warnings
    ), warnings


def test_payload_digest_mismatch_refuses_that_record(tmp_path: Path) -> None:
    """The artifact's own invariant is checked, not assumed.

    ``payload_sha256 == sha256(payload)`` is the contract this source
    republishes. A record that breaks it does not get its digest copied into
    the graph, because that digest would then certify bytes nobody has.
    """
    fx.build_app_db(tmp_path / "chat" / "app.db")
    fx.write_audit_artifact(
        tmp_path / "state", deployment=DEPLOYMENT, preset=PRESET,
    )
    audit_dir = resolve_audit_dir(
        DEPLOYMENT, PRESET, state_home=tmp_path / "state",
    )
    artifact = sorted(audit_dir.glob("records-*.jsonl"))[0]
    lines = artifact.read_text().splitlines()
    head = json.loads(lines[0])
    head[PAYLOAD_DIGEST_FIELD] = "0" * 64
    lines[0] = json.dumps(head, sort_keys=True)
    artifact.write_text("\n".join(lines) + "\n")

    source, facts = _run(tmp_path)
    _assert_no_context_anywhere(facts)
    warnings = source.last_run_stats["audit_warnings"]
    assert warnings[0]["count"] == 1
