from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path
from typing import Iterable

import psycopg
import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


_FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"
if str(_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FIXTURES_DIR))


pytestmark = pytest.mark.postgres


@pytest.fixture
def fixture_paths(tmp_path):
    from okg_self_micro import materialize  # type: ignore

    return materialize(tmp_path)


def test_agent_sessions_scoped_sync_publishes_only_target_session(
    clean_runtime: str,
    fixture_paths,
) -> None:
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.publisher import Publisher

    projects_root = fixture_paths.sessions_root.parent
    preview = AgentSessionsSource(
        repo="okg",
        claude_projects_root=projects_root,
        since="all",
        cost_estimation=False,
    )
    _seed_catalog_and_external_nodes(clean_runtime, preview.run("preview").facts)

    runner = SourceRunner(clean_runtime, source_name="test.agent-sessions")
    baseline = runner.run(
        AgentSessionsSource(
            repo="okg",
            claude_projects_root=projects_root,
            since="all",
            cost_estimation=False,
        ),
        mode="scope_complete",
    )
    assert baseline.completed_scope is True
    first_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert first_publish["status"] == "published", first_publish

    alpha = fixture_paths.sessions_root / "alpha-001.jsonl"
    with alpha.open("a", encoding="utf-8") as fh:
        fh.write(
            "\n"
            + json.dumps({
                "type": "user",
                "timestamp": "2026-05-05T12:00:00Z",
                "message": {"content": "scoped update"},
            })
        )

    scoped = runner.run(
        AgentSessionsSource(
            repo="okg",
            claude_projects_root=projects_root,
            since="all",
            cost_estimation=False,
        ),
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [
                alpha.relative_to(projects_root).as_posix(),
            ],
        },
    )
    assert scoped.completed_scope is False
    second_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert second_publish["status"] == "published", second_publish

    assert _agent_session_facts_for_run(
        clean_runtime,
        run_id=scoped.run_id,
        source="test.agent-sessions",
    ) == ["session:alpha-001"]
    assert _latest_generation_status(clean_runtime) == "published"
    assert _live_node_count(clean_runtime, "agent_session") >= 2
    assert _queued_jobs_by_kind(clean_runtime).get("ingest", 0) == 0
    assert _dlq_count(clean_runtime) == 0


def test_codex_sessions_scoped_sync_publishes_only_target_session(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.codex_sessions import CodexSessionsSource
    from okg.substrate.publisher import Publisher

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    root = tmp_path / "codex" / "sessions"
    alpha = root / "2026" / "05" / "02" / "alpha.jsonl"
    beta = root / "2026" / "05" / "02" / "beta.jsonl"
    _write_codex_transcript(alpha, session_id="alpha", repo=repo)
    _write_codex_transcript(beta, session_id="beta", repo=repo)

    preview = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=root,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="all",
    )
    _seed_catalog_and_external_nodes(clean_runtime, preview.run("preview").facts)

    runner = SourceRunner(clean_runtime, source_name="test.codex-sessions")
    baseline = runner.run(
        CodexSessionsSource(
            repo="demo",
            codex_sessions_root=root,
            archived_sessions_root=None,
            cwd_filter=[repo],
            since="all",
        ),
        mode="scope_complete",
    )
    assert baseline.completed_scope is True
    first_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert first_publish["status"] == "published", first_publish

    _write_codex_transcript(
        beta,
        session_id="beta",
        repo=repo,
        extra_command="sed -n '1,2p' src/app.py",
    )
    scoped = runner.run(
        CodexSessionsSource(
            repo="demo",
            codex_sessions_root=root,
            archived_sessions_root=None,
            cwd_filter=[repo],
            since="all",
        ),
        mode="scope_incomplete",
        sync_scope={"scope_keys": [beta.relative_to(root).as_posix()]},
    )
    assert scoped.completed_scope is False
    assert scoped.next_cursor is not None
    assert str(alpha.resolve()) in scoped.next_cursor["transcripts"]
    second_publish = Publisher(clean_runtime, "test", catalog_identity=bind_test_catalog_writer(clean_runtime, "test")).cycle()
    assert second_publish["status"] == "published", second_publish

    assert _agent_session_facts_for_run(
        clean_runtime,
        run_id=scoped.run_id,
        source="test.codex-sessions",
    ) == ["session:codex:beta"]
    assert _latest_generation_status(clean_runtime) == "published"
    assert _live_node_count(clean_runtime, "agent_session") == 2
    assert _queued_jobs_by_kind(clean_runtime).get("ingest", 0) == 0
    assert _dlq_count(clean_runtime) == 0


def test_codex_sessions_emit_agent_messages(tmp_path: Path) -> None:
    from okg.substrate.library.sources.codex_sessions import CodexSessionsSource

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    root = tmp_path / "codex" / "sessions"
    transcript = root / "2026" / "05" / "02" / "alpha.jsonl"
    _write_codex_transcript(transcript, session_id="alpha", repo=repo)

    source = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=root,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="all",
        capture_tool_use_events=["exec_command"],
    )
    facts = list(source.run("messages", mode="cursor").facts)
    message_nodes = {
        fact.node_id: fact
        for fact in facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_message"
    }

    assert set(message_nodes) == {"message:codex:alpha:0"}
    attrs = message_nodes["message:codex:alpha:0"].attrs
    assert attrs["session_id"] == "codex:alpha"
    assert attrs["harness"] == "codex"
    assert attrs["role"] == "assistant"
    assert attrs["message_kind"] == "assistant_text"
    assert attrs["content_excerpt"] == "I'll inspect src/app.py."
    assert attrs["tool_call_count"] == 1
    assert len(attrs["content_hash"]) == 64
    assert attrs["redaction_policy"] == "bounded_excerpt_sha256"

    assert any(
        isinstance(fact, EdgeFact)
        and fact.src == "session:codex:alpha"
        and fact.dst == "message:codex:alpha:0"
        and fact.edge_type == "contains"
        for fact in facts
    )
    assert any(
        isinstance(fact, EdgeFact)
        and fact.src == "message:codex:alpha:0"
        and fact.dst == "tool_use:codex:alpha:2:0"
        and fact.edge_type == "contains"
        for fact in facts
    )


def _seed_catalog_and_external_nodes(
    dsn: str,
    facts: Iterable[NodeFact | EdgeFact],
) -> None:
    materialized = list(facts)
    node_subtypes = {
        fact.node_id: fact.subtype
        for fact in materialized
        if isinstance(fact, NodeFact)
    }
    stub_nodes: dict[str, str] = {}
    for fact in materialized:
        if not isinstance(fact, EdgeFact):
            continue
        for node_id in (fact.src, fact.dst):
            if node_id in node_subtypes:
                continue
            subtype = _subtype_for_node_id(node_id)
            assert subtype is not None, node_id
            node_subtypes[node_id] = subtype
            stub_nodes[node_id] = subtype

    narrowings = {
        (node_subtypes[fact.src], fact.edge_type, node_subtypes[fact.dst])
        for fact in materialized
        if isinstance(fact, EdgeFact)
        and fact.src in node_subtypes
        and fact.dst in node_subtypes
    }
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        for subtype in sorted(set(node_subtypes.values())):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) "
                "VALUES (%s, %s)",
                (subtype, cv),
            )
            cur.execute(
                """
                INSERT INTO okg.subtype_ancestors
                  (catalog_version_id, subtype_name, ancestor_name, depth)
                VALUES (%s, %s, %s, 0)
                ON CONFLICT DO NOTHING
                """,
                (cv, subtype, subtype),
            )
        for parent, edge_type, child in sorted(narrowings):
            cur.execute(
                "INSERT INTO okg.narrowings "
                "(parent_subtype, edge_type, child_subtype, "
                " catalog_version_id) "
                "VALUES (%s, %s, %s, %s)",
                (parent, edge_type, child, cv),
            )
        for node_id, subtype in sorted(stub_nodes.items()):
            stub_run_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES (%s, %s, %s::jsonb, 'test.external-stubs',
                        %s::jsonb, %s::jsonb, now(), %s, %s, 'I')
                """,
                (
                    node_id,
                    subtype,
                    json.dumps({"text": node_id}),
                    json.dumps({"node_id": node_id}),
                    json.dumps({"stub": True}),
                    f"stub:{node_id}",
                    stub_run_id,
                ),
            )


def _subtype_for_node_id(node_id: str) -> str | None:
    prefix_map = {
        "agent:": "agent",
        "chapter:": "agent_chapter",
        "change:": "open_spec_change",
        "chip:": "spawned_task_chip",
        "file:": "source_file",
        "host:": "remote_host",
        "message:": "agent_message",
        "pevent:": "permission_event",
        "session:": "agent_session",
        "skill:": "skill",
        "subsession:": "sub_agent_session",
        "tool_use:": "tool_use_event",
    }
    for prefix, subtype in prefix_map.items():
        if node_id.startswith(prefix):
            return subtype
    return None


def _agent_session_facts_for_run(
    dsn: str,
    *,
    run_id: str,
    source: str,
) -> list[str]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE run_id = :run_id
               AND source = :source
               AND subtype = 'agent_session'
             ORDER BY node_id
            """,
            {"run_id": run_id, "source": source},
        )
    return [str(row["node_id"]) for row in rows]


def _latest_generation_status(dsn: str) -> str:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        generations = store.list_generations(
            deployment_name="test",
            status="published",
        )
        assert generations
        return str(generations[-1].metadata["status"])
    finally:
        store.close()


def _live_node_count(dsn: str, subtype: str) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        latest = store.list_generations(
            deployment_name="test",
            status="published",
        )[-1]
        graph = store.checkout_generation(latest.checkpoint_id)
        return int(graph.query(
            """
            SELECT count(*) AS count
              FROM okg.graph_nodes
             WHERE subtype = :subtype
            """,
            {"subtype": subtype},
        )[0]["count"])
    finally:
        store.close()


def _dlq_count(dsn: str) -> int:
    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)
    return int(summary["dlq_count"])


def _queued_jobs_by_kind(dsn: str) -> dict[str, int]:
    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)
    return {
        str(kind): int(count)
        for kind, count in summary["queue_depth"].items()
        if int(count)
    }


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _write_codex_transcript(
    path: Path,
    *,
    session_id: str,
    repo: Path,
    extra_command: str | None = None,
) -> None:
    tool_timestamp = (
        "2026-05-02T12:00:03Z"
        if extra_command is not None
        else "2026-05-02T12:00:02Z"
    )
    rows = [
        {
            "timestamp": "2026-05-02T12:00:00Z",
            "type": "session_meta",
            "payload": {
                "id": session_id,
                "timestamp": "2026-05-02T12:00:00Z",
                "cwd": str(repo),
                "originator": "Codex Desktop",
                "cli_version": "0.126.0",
                "model_provider": "openai",
                "git": {"branch": "main", "commit_hash": "deadbeef"},
            },
        },
        {
            "timestamp": "2026-05-02T12:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "I'll inspect src/app.py."}],
            },
        },
        {
            "timestamp": tool_timestamp,
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "arguments": json.dumps({
                    "cmd": extra_command or "sed -n '1p' src/app.py",
                    "workdir": str(repo),
                }),
            },
        },
    ]
    # Newline-terminated, as every real transcript is (verified: 1713/1713 on
    # this host). An unterminated final line means a writer mid-append, and
    # the reader defers it rather than consuming it -- consuming it put the
    # stop offset mid-line, which made a live session re-parse from zero on
    # every append. Here the final row is the function_call, so an
    # unterminated fixture silently loses its tool event.
    _write(path, "\n".join(json.dumps(row) for row in rows) + "\n")
