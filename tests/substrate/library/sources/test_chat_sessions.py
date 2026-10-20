"""PACT chat-transcript-source — req.chat-transcript-source.adapter-contract.

Hermetic. Every test drives the real adapter over a fixture app database on
the PINNED Open WebUI schema; none of them mocks the reader.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.chat_sessions import (
    FORBIDDEN_TABLES,
    PINNED_APP_DB_REVISION,
    REFUSAL_APP_DB_MISSING,
    REFUSAL_APP_DB_REVISION_UNPINNED,
    REFUSAL_APP_DB_SURFACE_MISSING,
    REFUSAL_CITATION_NOT_ALLOWLISTED,
    REFUSAL_INSTANCE_NOT_INJECTIVE,
    ChatSessionsSource,
    ChatSourceRefused,
)
from tests.substrate._fake_credentials import fake_credential
from tests.substrate.library.sources import _chat_sessions_fixture as fx


INSTANCE = "test-instance"


def _source(tmp_path: Path, **overrides: Any) -> ChatSessionsSource:
    params: dict[str, Any] = {
        "app_db_path": tmp_path / "chat" / "app.db",
        "instance": INSTANCE,
        "deployment": "chat-source-test",
        "preset": "workspace",
        "audit_state_home": tmp_path / "state",
    }
    params.update(overrides)
    return ChatSessionsSource(**params)


def _facts(run: Any) -> list[Any]:
    return list(run.facts)


def _nodes(facts: list[Any], subtype: str) -> list[NodeFact]:
    return [
        fact for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == subtype
    ]


def _edges(facts: list[Any]) -> list[EdgeFact]:
    return [fact for fact in facts if isinstance(fact, EdgeFact)]


def _footprint(record_set: Any) -> dict[str, dict[str, Any]]:
    """Every node id and edge identity a record set declares, per record."""
    return {
        key: {
            "nodes": sorted(
                (node.node_id, node.subtype)
                for node in emission.nodes_by_id().values()
            ),
            "edges": sorted(emission.edges_by_identity()),
        }
        for key, emission in record_set.records.items()
    }


def _prepared(tmp_path: Path) -> ChatSessionsSource:
    fx.build_app_db(tmp_path / "chat" / "app.db")
    fx.write_audit_artifact(
        tmp_path / "state", deployment="chat-source-test", preset="workspace",
    )
    return _source(tmp_path)


def test_run_returns_sourcerun_with_node_and_edge_facts(tmp_path: Path) -> None:
    source = _prepared(tmp_path)
    run = source.run("run-1", mode="scope_complete")
    facts = _facts(run)

    assert _nodes(facts, "chat_session"), facts
    assert _nodes(facts, "chat_message"), facts
    assert _edges(facts), facts
    for fact in facts:
        assert fact.source_record_id, fact
        assert fact.source_revision, fact
        assert set(fact.source_record_id) >= {"instance", "chat_id"}, fact

    assert run.completed_scope is True
    assert run.record_set is not None
    ids = fx.node_ids(INSTANCE)
    assert {node.node_id for node in _nodes(facts, "chat_session")} == {
        ids["graph_session"], ids["secret_session"], ids["arena_session"],
    }
    # `contains` edges hang messages off their conversation.
    contains = {
        (edge.src, edge.dst)
        for edge in _edges(facts) if edge.edge_type == "contains"
    }
    assert (ids["graph_session"], ids["graph_answer"]) in contains


def test_cursor_round_trips_and_unchanged_upstream_does_not_reemit(
    tmp_path: Path,
) -> None:
    source = _prepared(tmp_path)
    first = source.run("run-1", mode="scope_complete")
    first_facts = _facts(first)
    cursor = first.next_cursor
    assert cursor is not None
    assert cursor["instance"] == INSTANCE
    assert cursor["app_db_revision"] == PINNED_APP_DB_REVISION
    assert cursor["conversation_hashes"], cursor

    second = source.run("run-2", mode="scope_complete", cursor=dict(cursor))
    second_facts = _facts(second)

    assert first_facts, "the first run must emit something to compare against"
    assert second_facts == [], (
        "an unchanged app database re-emitted facts; the content-hash cursor "
        "did not round-trip"
    )
    # The record set is still COMPLETE on the no-change run, down to every
    # node id and edge identity. Comparing only the record KEYS would let a
    # conversation declare its session node and nothing else, and an ordinary
    # no-change reconcile would then retract every message, rating,
    # preference, and context record beneath it.
    assert second.record_set is not None
    assert _footprint(second.record_set) == _footprint(first.record_set), (
        "the no-change run declared a different footprint, so the runner "
        "would synthesize retractions for the difference"
    )
    assert second.next_cursor["conversation_hashes"] == (
        cursor["conversation_hashes"]
    )

    # A NEW message must defeat the no-change verdict.
    conn = sqlite3.connect(tmp_path / "chat" / "app.db")
    try:
        conn.execute(
            "INSERT INTO chat_message (id, chat_id, user_id, role, content, "
            "created_at, updated_at) VALUES (?, ?, ?, 'user', ?, ?, ?)",
            (
                f"{fx.CHAT_GRAPH}-added",
                fx.CHAT_GRAPH,
                fx.PRINCIPAL,
                '"and what changed since?"',
                1785878000,
                1785878000,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    third = source.run("run-3", mode="scope_complete", cursor=dict(cursor))
    assert _nodes(_facts(third), "chat_message"), (
        "a new upstream message did not re-emit"
    )


def test_cursor_mode_declares_no_completed_scope_and_no_record_set(
    tmp_path: Path,
) -> None:
    """`completed_scope` is HONEST, not a constant.

    A cursor-mode run has not walked the whole scope, so it must not claim it
    did: the runner treats a completed scope as licence to retract everything
    absent from the record set.
    """
    source = _prepared(tmp_path)
    run = source.run("run-1", mode="cursor")
    assert _facts(run), "the run emitted nothing, so the assertion is vacuous"
    assert run.completed_scope is False
    assert run.record_set is None
    assert run.run_mode == "cursor"

    complete = source.run("run-2", mode="scope_complete")
    _facts(complete)
    assert complete.completed_scope is True
    assert complete.record_set is not None


def test_an_emission_schema_bump_forces_reemission(tmp_path: Path) -> None:
    """A cursor written by an older emitter must not suppress re-emission.

    Otherwise a shape change ships and the graph keeps the old rows forever,
    because the content of the app database never changed.
    """
    source = _prepared(tmp_path)
    first = source.run("run-1", mode="scope_complete")
    _facts(first)
    cursor = dict(first.next_cursor)

    assert _facts(source.run("run-2", mode="scope_complete", cursor=dict(cursor))) == []

    stale = dict(cursor)
    stale["emission_schema_version"] = "0.9"
    assert _nodes(
        _facts(source.run("run-3", mode="scope_complete", cursor=stale)),
        "chat_message",
    ), "an emission-schema bump did not force re-emission"


def test_no_graph_database_connection_is_opened(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Asserted over BEHAVIOR, not over imports.

    Every psycopg entry point raises for the duration of the run, so a source
    that reached for a graph connection fails here instead of passing because
    a module-level import happened to be absent.
    """
    import psycopg

    opened: list[str] = []

    def _forbidden(*args: Any, **kwargs: Any) -> Any:
        opened.append(str(args[:1]))
        raise AssertionError(
            "the chat source opened a graph database connection; it must emit "
            "facts and let the ingest path do the rest"
        )

    monkeypatch.setattr(psycopg, "connect", _forbidden)
    monkeypatch.setattr(psycopg.Connection, "connect", _forbidden)

    source = _prepared(tmp_path)
    facts = _facts(source.run("run-1", mode="scope_complete"))

    assert facts, "the run emitted nothing, so the assertion would be vacuous"
    assert opened == []
    # The constructor declares no `dsn`, so the adapter factory structurally
    # cannot inject one.
    import inspect

    assert "dsn" not in inspect.signature(ChatSessionsSource).parameters


def test_no_fact_carries_credentials_or_session_tokens(tmp_path: Path) -> None:
    source = _prepared(tmp_path)
    facts = _facts(source.run("run-1", mode="scope_complete"))
    assert facts

    blob = repr([
        (getattr(fact, "attrs", {}), fact.source_record_id, fact.source_revision)
        for fact in facts
    ])
    # ONE credential per text path the adapter writes, so a missing redaction
    # call on any single path is attributable. A clean fixture staying clean
    # would prove nothing; this one is dirty on all four.
    for secret, path in (
        ("sk-liveKEYmaterial0123456789", "the message excerpt"),
        ("ghp_titleSECRET0123456789", "the conversation title"),
        (fake_credential("xoxb-", "commentSECRET0123456789"), "the rating comment"),
        ("AKIAPAYLOADSECRET99", "the audit payload excerpt"),
    ):
        assert secret not in blob, f"a credential survived on {path}"
    assert "***" in blob, "nothing was redacted at all"
    # Credentials the instance holds in tables this source must never open.
    assert "sk-instanceADMINkey00001" not in blob
    assert "$2b$12$notarealhashbutshaped" not in blob
    assert "eyJhbGciOiJIUzI1NiJ9" not in blob
    assert "operator@example.org" not in blob, (
        "an email reached a fact; the `user` table must not be read"
    )

    ids = fx.node_ids(INSTANCE)
    secret_message = next(
        node for node in _nodes(facts, "chat_message")
        if node.node_id == ids["secret_message"]
    )
    assert "OPENAI_API_KEY=***" in secret_message.attrs["excerpt"]
    assert secret_message.attrs["redacted_secret_classes"], (
        "the redaction happened but was not reported on the node"
    )
    assert source.last_run_stats["redacted_messages"] >= 1

    # Each of the other three paths is checked positively too, so "the
    # credential is absent" cannot pass by the attribute being absent.
    secret_session = next(
        node for node in _nodes(facts, "chat_session")
        if node.node_id == ids["secret_session"]
    )
    assert "GITHUB_TOKEN=***" in secret_session.attrs["title"]
    rating = next(iter(_nodes(facts, "chat_rating")))
    assert "SLACK_TOKEN=***" in rating.attrs["comment_excerpt"]
    context = next(iter(_nodes(facts, "chat_context_record")))
    assert "AWS_SECRET_ACCESS_KEY=***" in context.attrs["payload_excerpt"]

    # The digest is taken over the REDACTED text, so it never certifies bytes
    # the graph does not hold.
    import hashlib

    from okg.substrate.library.sources._transcript_redaction import (
        scrub_transcript_text,
    )

    raw = (
        "this fails for me:\n\n"
        f"    {fx.PASTED_SECRET} uv run okg mcp-serve\n\n"
        "any idea why?"
    )
    redacted = scrub_transcript_text(raw)
    assert redacted != raw, "the fixture message carries no redactable material"
    assert secret_message.attrs["content_sha256"] == hashlib.sha256(
        redacted.encode("utf-8")
    ).hexdigest(), "content_sha256 was computed over the RAW text"


def test_every_excerpt_is_bounded(tmp_path: Path) -> None:
    """The bound is a real bound, exercised by text that exceeds it."""
    source = _prepared(tmp_path)
    facts = _facts(source.run("run-1", mode="scope_complete"))
    messages = _nodes(facts, "chat_message")
    assert messages

    over_bound = [
        node for node in messages
        if node.attrs["content_chars"] > source.excerpt_chars
    ]
    assert over_bound, (
        "no fixture message exceeds the excerpt bound, so removing the bound "
        "would leave this test green"
    )
    for node in over_bound:
        assert len(node.attrs["excerpt"]) == source.excerpt_chars, node.node_id
        assert node.attrs["excerpt_truncated"] is True, node.node_id
    for node in messages:
        assert len(node.attrs["excerpt"]) <= source.excerpt_chars, node.node_id
        assert len(node.attrs["text"]) <= source.excerpt_chars, node.node_id


def test_a_colon_in_the_instance_label_is_refused(tmp_path: Path) -> None:
    """Node ids must be injective; `a:b`+`c` and `a`+`b:c` would collide."""
    with pytest.raises(ChatSourceRefused) as excinfo:
        _source(tmp_path, instance="okg:workspace")
    assert excinfo.value.reason == REFUSAL_INSTANCE_NOT_INJECTIVE


def test_unresolvable_feedback_is_counted_with_a_named_reason(
    tmp_path: Path,
) -> None:
    """A preference the graph cannot carry is reported, never silently lost."""
    path = fx.build_app_db(tmp_path / "chat" / "app.db")
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "INSERT INTO feedback (id, user_id, version, type, data, meta, "
            "created_at, updated_at) VALUES (?, ?, 0, 'rating', ?, ?, ?, ?)",
            ("fb-orphan", fx.PRINCIPAL, '{"rating": 1}',
             '{"chat_id": "no-such-chat"}', 1785877100, 1785877100),
        )
        conn.execute(
            "INSERT INTO feedback (id, user_id, version, type, data, meta, "
            "created_at, updated_at) VALUES (?, ?, 0, 'rating', ?, ?, ?, ?)",
            ("fb-unresolved-message", fx.PRINCIPAL, '{"rating": -1}',
             json.dumps({"chat_id": fx.CHAT_GRAPH, "message_id": "gone"}),
             1785877200, 1785877200),
        )
        conn.commit()
    finally:
        conn.close()
    fx.write_audit_artifact(
        tmp_path / "state", deployment="chat-source-test", preset="workspace",
    )
    source = _source(tmp_path)
    _facts(source.run("run-1", mode="scope_complete"))

    stats = source.last_run_stats
    assert stats["feedback_dropped"] == 1
    assert stats["feedback_drops"] == [
        {"reason": "feedback-chat-id-unknown", "feedback_id": "fb-orphan"},
    ]
    assert stats["ratings_dropped"] == 1
    assert stats["rating_drops"] == [
        {"reason": "rating-message-unresolved",
         "feedback_id": "fb-unresolved-message"},
    ]
    # The resolvable rating still landed, so the drop counters are not just
    # reporting a wholesale failure.
    assert stats["ratings_emitted"] == 1


def test_forbidden_tables_are_never_queried(tmp_path: Path) -> None:
    """The claim in READ_SURFACE is checked against executed SQL."""
    source = _prepared(tmp_path)
    executed: list[str] = []
    real_connect = sqlite3.connect

    class _Tracing(sqlite3.Connection):
        def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
            executed.append(sql)
            return super().execute(sql, *args, **kwargs)

    def _connect(*args: Any, **kwargs: Any) -> Any:
        kwargs["factory"] = _Tracing
        return real_connect(*args, **kwargs)

    sqlite3.connect = _connect  # type: ignore[assignment]
    try:
        _facts(source.run("run-1", mode="scope_complete"))
    finally:
        sqlite3.connect = real_connect  # type: ignore[assignment]

    assert executed, "no SQL was captured, so this assertion would be vacuous"
    for statement in executed:
        lowered = statement.lower()
        for table in FORBIDDEN_TABLES:
            assert f" {table} " not in f" {lowered} ", (statement, table)
            assert f"'{table}'" not in lowered, (statement, table)


def test_record_set_diff_retracts_removed_conversation(tmp_path: Path) -> None:
    source = _prepared(tmp_path)
    first = source.run("run-1", mode="scope_complete")
    _facts(first)
    assert first.record_set is not None
    ids = fx.node_ids(INSTANCE)
    assert ids["secret_session"] in {
        node.node_id
        for emission in first.record_set.records.values()
        for node in emission.nodes_by_id().values()
    }

    fx.delete_conversation(tmp_path / "chat" / "app.db", fx.CHAT_SECRET)
    second = source.run("run-2", mode="scope_complete", cursor=first.next_cursor)
    _facts(second)

    assert second.completed_scope is True
    assert second.record_set is not None
    assert fx.CHAT_SECRET in first.record_set.records
    assert fx.CHAT_SECRET not in second.record_set.records, (
        "the vanished conversation is still declared, so the runner's diff "
        "would keep its rows published under a completed scope"
    )
    remaining = {
        node.node_id
        for emission in second.record_set.records.values()
        for node in emission.nodes_by_id().values()
    }
    assert ids["secret_session"] not in remaining
    assert ids["graph_session"] in remaining


def test_probe_short_circuit_safe_is_false(tmp_path: Path) -> None:
    source = _prepared(tmp_path)
    assert source.probe_short_circuit_safe is False

    from okg.substrate.ingest.runner import SourceRunner

    assert SourceRunner._adapter_probe_short_circuit_safe(source) is False
    # The reason it is false: this source emits cross-source `references`
    # edges, whose targets its own content probe cannot see.
    edges = _edges(_facts(source.run("run-1", mode="scope_complete")))
    cross_source = [edge for edge in edges if edge.edge_type == "references"]
    assert cross_source, (
        "no cross-source citation was emitted, so probe_short_circuit_safe = "
        "False would be an unexplained constant"
    )
    # Short-circuit acknowledgement additionally needs the applied-token
    # triple. This source claims none of it.
    run = source.run("run-2", mode="scope_complete")
    assert run.applied_raw_token is None
    assert run.applied_token_scope == "unavailable"
    assert run.applied_token_authority == "unavailable"


def test_out_of_allowlist_citation_is_dropped_with_a_named_reason(
    tmp_path: Path,
) -> None:
    source = _prepared(tmp_path)
    edges = _edges(_facts(source.run("run-1", mode="scope_complete")))
    cited = {edge.dst for edge in edges if edge.edge_type == "references"}

    assert fx.CITED_PACT in cited
    assert fx.CITED_FILE in cited
    assert fx.CITED_UNKNOWN not in cited
    drops = source.last_run_stats["citation_drops"]
    assert drops == [{
        "reason": REFUSAL_CITATION_NOT_ALLOWLISTED,
        "node_id": fx.CITED_UNKNOWN,
    }]


def test_unpinned_app_db_schema_revision_refuses_at_startup(
    tmp_path: Path,
) -> None:
    fx.build_app_db(tmp_path / "chat" / "app.db", revision="deadbeefcafe")
    source = _source(tmp_path)

    with pytest.raises(ChatSourceRefused) as excinfo:
        source.run("run-1", mode="scope_complete")
    assert excinfo.value.reason == REFUSAL_APP_DB_REVISION_UNPINNED
    assert "deadbeefcafe" in str(excinfo.value)
    assert source.preflight().status == "endpoint_failed"


def test_missing_app_db_refuses_rather_than_reading_empty(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path)
    with pytest.raises(ChatSourceRefused) as excinfo:
        source.run("run-1", mode="scope_complete")
    assert excinfo.value.reason == REFUSAL_APP_DB_MISSING


def test_pinned_revision_with_drifted_columns_refuses(tmp_path: Path) -> None:
    """A pin that only compares a string is not a pin.

    The revision matches but a column the reader needs is gone: the schema is
    still unknown, and a best-effort parse would publish partial rows.
    """
    path = fx.build_app_db(tmp_path / "chat" / "app.db")
    conn = sqlite3.connect(path)
    try:
        conn.execute("ALTER TABLE chat_message DROP COLUMN model_id")
        conn.commit()
    finally:
        conn.close()

    source = _source(tmp_path)
    with pytest.raises(ChatSourceRefused) as excinfo:
        source.run("run-1", mode="scope_complete")
    assert excinfo.value.reason == REFUSAL_APP_DB_SURFACE_MISSING
    assert "model_id" in str(excinfo.value)


def test_declared_trust_label_is_a_real_lattice_member() -> None:
    """The registry entry's trust_label must exist in the trust lattice.

    Regression for #1316. This source declared ``operator-chat-instance``,
    which is not a member of ``TRUST_LABEL_RANKS``. ``normalize_trust_label``
    raised at the admission stage, so EVERY run was rejected before any data
    was written and the source had never ingested on any deployment.

    The failure mode is what makes this worth a test: admission rejects the
    whole source, so ``okg status`` shows nothing worse than a source that
    never reconciled -- indistinguishable from one that simply had no data.
    """
    from okg.substrate.library.sources.chat_sessions import (
        chat_sessions_source_registry_entry,
    )
    from okg.substrate.producer_authority import (
        TRUST_LABELS,
        normalize_trust_label,
    )

    entry = chat_sessions_source_registry_entry(
        ownership_id="okg-workspace.chat-sessions",
    )

    def _find_trust_labels(node: Any) -> list[str]:
        found: list[str] = []
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "trust_label" and isinstance(value, str):
                    found.append(value)
                found.extend(_find_trust_labels(value))
        elif isinstance(node, list):
            for item in node:
                found.extend(_find_trust_labels(item))
        return found

    labels = _find_trust_labels(entry)
    assert labels, "registry entry declares no trust_label"
    for label in labels:
        assert label in TRUST_LABELS, (
            f"{label!r} is not a trust-lattice member; admission would reject "
            f"every run of this source. Known members: {sorted(TRUST_LABELS)}"
        )
        # Must survive the exact call admission makes.
        normalize_trust_label(label)
