"""`okg mcp-serve` serves reads, read-only, behind declared-safe migrations.

PACT unknown-s8-mcp-readonly (umbrella unknown-as-a-value, issue #2573,
closes #2035; decision D2 option 1). Slice 7 gave every surface one release
verdict. Under `read_only` -- the only problem is pending additive
migrations and every one declares `reads_safe_behind: true` -- mcp-serve
still refused to start, so an agent on a branch one migration ahead of a
shared deployment had no graph at all.

The promise test drives the real command against a real PostgreSQL database
whose migration ledger is one migration behind this code:

* when that migration declares `reads_safe_behind`, `okg mcp-serve` starts,
  `inspect`, `search` and `expand` answer from the published generation
  with `schema_behind_by: 1` on every response, and the write and admin
  tools refuse with the skew reason;
* when it does not declare it, mcp-serve refuses to start with slice 7's
  verdict and never builds a server.

The release contract's manifest-side inputs (the declared `release` block
and this code's release artifact) are stubbed; the database state, the
readiness check, the release verdict and the server are real.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
from types import SimpleNamespace
from typing import Any

import anyio
import psycopg
import pytest
from mcp.shared.memory import create_connected_server_and_client_session
from psycopg.types.json import Jsonb

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    bind_test_catalog_writer,
    seed_catalog,
)
from tests.substrate.mcp._result_payload import payload as _payload
from tests.substrate.mcp._result_payload import (
    call_json,
    json_client_sessions,
)

# Allow-listed for real: it only creates a table and an index, so
# `reads_safe_behind_refusal` passes its SQL.

@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch):
    """Program-side test: every call here reads the JSON result format
    (issue #3059), through request `_meta` or the in-process scope."""
    json_client_sessions(monkeypatch)


_BEHIND = "0028_enricher_input_quarantine"
_SNAPSHOT = "0000_schema_snapshot"
_SUBTYPE = "skew_marker"
_ROOT = "skew:root"
_LEAF = "skew:leaf"


def _publish(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[_SUBTYPE],
            narrowings=[(_SUBTYPE, "relates", _SUBTYPE)],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(_ROOT, _SUBTYPE, {"text": "skew-marker root"},
                         {"id": _ROOT}, {"v": 1}),
                NodeFact(_LEAF, _SUBTYPE, {"text": "skew-marker leaf"},
                         {"id": _LEAF}, {"v": 1}),
                EdgeFact(_ROOT, _LEAF, "relates",
                         source_record_id={"id": "edge:skew"},
                         source_revision={"v": 1}),
            ],
            source="unknown-s8",
            strict_endpoints=True,
        )
    result = Publisher(
        dsn, "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert result["status"] == "published", result
    return str(result["generation_id"])


@pytest.fixture
def one_migration_behind(clean_runtime: str, okg_mcp_can_log_in: str):
    """The real ledger without `_BEHIND`, restored afterwards.

    The schema snapshot row keeps this code's real checksum, so the
    fingerprint matches exactly: the only case that starts read-only while
    any fingerprint difference refuses (decision 2026-09-28; per-migration
    fingerprints are follow-up #2797).
    """
    del okg_mcp_can_log_in
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cols = [
            row[0] for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'okg' AND table_name = 'schema_migrations' "
                "ORDER BY ordinal_position"
            ).fetchall()
        ]
        saved = conn.execute(
            f"SELECT {', '.join(cols)} FROM okg.schema_migrations "
            "WHERE migration_id = %s",
            (_BEHIND,),
        ).fetchall()
        assert len(saved) == 1, "fixture database lacks the migration row"
        conn.execute(
            "DELETE FROM okg.schema_migrations WHERE migration_id = %s",
            (_BEHIND,),
        )
    try:
        yield clean_runtime
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(
                f"INSERT INTO okg.schema_migrations ({', '.join(cols)}) "
                f"VALUES ({', '.join(['%s'] * len(cols))})",
                [
                    Jsonb(value) if isinstance(value, (dict, list)) else value
                    for value in saved[0]
                ],
            )


@pytest.fixture
def one_migration_behind_and_drifted(one_migration_behind: str):
    """`one_migration_behind`, plus an older recorded schema snapshot.

    A real pending migration usually edits schema.sql too, so the recorded
    fingerprint differs. schema.sql and views.sql also carry views,
    functions, grants and policies the ledger does not describe, so that
    difference refuses rather than serving old logic without an error.
    """
    with psycopg.connect(one_migration_behind, autocommit=True) as conn:
        (snapshot_checksum,) = conn.execute(
            "SELECT checksum FROM okg.schema_migrations WHERE migration_id = %s",
            (_SNAPSHOT,),
        ).fetchone()
        conn.execute(
            "UPDATE okg.schema_migrations SET checksum = %s "
            "WHERE migration_id = %s",
            (f"recorded-before-{_BEHIND}", _SNAPSHOT),
        )
    try:
        yield one_migration_behind
    finally:
        with psycopg.connect(one_migration_behind, autocommit=True) as conn:
            conn.execute(
                "UPDATE okg.schema_migrations SET checksum = %s "
                "WHERE migration_id = %s",
                (snapshot_checksum, _SNAPSHOT),
            )


def _declare_release(monkeypatch, *, declare_reads_safe: bool) -> None:
    import okg.substrate.db.management as management
    from okg.substrate import deployment_release
    from okg.substrate.deployment_release import (
        ReleaseArtifact,
        ReleaseContract,
    )

    monkeypatch.setattr(
        deployment_release, "resolve_deployment",
        lambda name: SimpleNamespace(release={"channel": "dev"}),
    )
    monkeypatch.setattr(
        deployment_release, "load_release_contract",
        lambda manifest: ReleaseContract(channel="dev", pin=None),
    )
    # This code's real schema snapshot checksum, not None: a None here
    # skipped the snapshot comparison entirely. With the real value, an
    # exact match serves read-only and any drift refuses
    # (`one_migration_behind_and_drifted`).
    code_schema_checksum = management.current_known_migrations()[0].checksum
    monkeypatch.setattr(
        deployment_release, "current_release_artifact",
        lambda manifest: ReleaseArtifact(
            code_revision="code-new",
            schema_checksum=code_schema_checksum,
            catalog_fingerprint=None,
            skill_bundle_hash=None,
        ),
    )
    if declare_reads_safe:
        real = management.current_known_migrations

        def _declared(**kwargs):
            return [
                dataclasses.replace(m, reads_safe_behind=True)
                if m.migration_id == _BEHIND else m
                for m in real(**kwargs)
            ]

        monkeypatch.setattr(management, "current_known_migrations", _declared)


def _run_mcp_serve(monkeypatch, capsys, dsn: str):
    """Run the real command; the stubbed run_server builds the real server."""
    import okg.substrate.cli.dsn_authority as dsn_authority
    import okg.substrate.mcp.server as server_mod
    from okg.substrate.cli import main as cli_main

    launched: list[dict[str, Any]] = []
    monkeypatch.setattr(
        dsn_authority, "resolve_read_dsn",
        lambda args, *, surface: SimpleNamespace(
            dsn=dsn, authority={"mode": "test"}, target="test",
        ),
    )
    monkeypatch.setattr(
        server_mod, "run_server", lambda **kwargs: launched.append(kwargs),
    )
    args = argparse.Namespace(
        deployment="default",
        dsn=None,
        embedder=None,
        branch="default",
        allow_writes=True,
        transport="stdio",
        host="127.0.0.1",
        port=8100,
        allowed_host=[],
        auth_token_env=None,
        auth_token=None,
    )
    code = cli_main._cmd_mcp_serve(args, as_json=True)
    return code, capsys.readouterr().err, launched


def _row_counts(dsn: str) -> dict[str, int]:
    """Rows in every table outside the system schemas.

    `_timescaledb_internal` is skipped. Its own tables (`bgw_job_stat`,
    `bgw_job_stat_history`, `bgw_policy_chunk_stats`) are written by the
    TimescaleDB job scheduler on its own clock, so they can change during
    a read-only serve (CI run 36688589009). Its other tables are hypertable
    chunks, whose rows are still counted here through their parent table
    in the `okg` schema; okg never writes that schema directly.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        tables = conn.execute(
            "SELECT format('%I.%I', table_schema, table_name) "
            "FROM information_schema.tables "
            "WHERE table_type = 'BASE TABLE' "
            "AND table_schema NOT IN "
            "('pg_catalog', 'information_schema', '_timescaledb_internal') "
            "ORDER BY 1"
        ).fetchall()
        return {
            name: conn.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
            for (name,) in tables
        }


def _build_launched(kwargs: dict[str, Any]):
    from okg.substrate.mcp.server import build_server

    return build_server(
        dsn=kwargs["dsn"],
        dsn_authority=kwargs["dsn_authority"],
        deployment_name=kwargs["deployment_name"],
        embedder=kwargs["embedder"],
        branch_name=kwargs["branch_name"],
        allow_writes=kwargs["allow_writes"],
        read_only_release=kwargs["read_only_release"],
        min_conns=1,
        max_conns=2,
    )


@pytest.mark.postgres
def test_declared_safe_behind_serves_reads_and_refuses_writes(
    one_migration_behind: str, monkeypatch, capsys,
) -> None:
    dsn = one_migration_behind
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    generation = _publish(dsn)
    _declare_release(monkeypatch, declare_reads_safe=True)

    from okg.substrate import deployment_release

    # The real code checksum matches the recorded snapshot exactly; the
    # only problem is the one declared migration.
    report = deployment_release.build_release_report(
        deployment="default", dsn=dsn, process_role="mcp",
    )
    assert [p["kind"] for p in report["problems"]] == [
        "schema_migrations_pending"
    ], report["problems"]
    assert report["verdict"] == "read_only", report["verdict_message"]
    assert report["fix_hint"] == "run `okg up default`"

    code, stderr, launched = _run_mcp_serve(monkeypatch, capsys, dsn)
    assert (code, len(launched)) == (0, 1), stderr
    assert "read-only" in stderr and _BEHIND in stderr, stderr
    assert "continuing" not in stderr, stderr
    server = _build_launched(launched[0])

    reads: dict[str, dict] = {}
    writes: dict[str, Any] = {}
    counts: dict[str, dict[str, int]] = {}
    valid_record = {
        "kind": "finding",
        "payload": {
            "statement": "skew-marker root relates to skew-marker leaf.",
            "confidence_kind": "measured",
        },
        "authority": {"producer_id": "agent:unknown-s8"},
    }

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as client:
            listed = {tool.name for tool in (await client.list_tools()).tools}
            assert {"record", "propose", "add_source"} <= listed
            reads["inspect"] = _payload(await client.call_tool(
                "inspect", {"target": {"kind": "graph"}},
            ))
            reads["search"] = _payload(await client.call_tool(
                "search", {"query": "skew-marker", "limit": 10},
            ))
            reads["expand"] = _payload(await client.call_tool(
                "expand", {
                    "input": {"node_id": _ROOT},
                    "edge_type": "relates",
                    "select": ["dst"],
                    "limit": 10,
                },
            ))
            counts["before"] = _row_counts(dsn)
            for tool in ("record", "propose", "add_source"):
                writes[tool] = await client.call_tool(tool, {})
            writes["record_valid"] = await client.call_tool(
                "record", valid_record,
            )
            counts["after"] = _row_counts(dsn)

    try:
        anyio.run(drive)
    finally:
        for attribute in ("_okg_pool", "_okg_write_pool"):
            opened = getattr(server, attribute, None)
            if opened is not None:
                opened.close()

    observed = {
        name: (
            body.get("schema_behind_by"),
            str(body.get("pinned_generation_id")),
            "error" in body,
        )
        for name, body in reads.items()
    }
    assert observed == {
        name: (1, generation, False) for name in ("inspect", "search", "expand")
    }, json.dumps(reads, default=str)[:4000]
    assert reads["inspect"]["node_count"] == 2
    assert [r["node_id"] for r in reads["search"]["results"]]
    assert reads["expand"]["neighbors"]

    for tool, result in writes.items():
        body = _payload(result)
        assert result.isError is True, (tool, body)
        assert body["schema_behind_by"] == 1, (tool, body)
        assert body["error"]["code"] == "schema_behind_read_only", (tool, body)
        assert _BEHIND in body["error"]["message"], (tool, body)
        assert body["error"]["data"]["behind_migrations"] == [_BEHIND]
    # Nothing was written anywhere by the refused calls, the valid one included.
    changed = {
        name: (counts["before"][name], rows)
        for name, rows in counts["after"].items()
        if counts["before"].get(name) != rows
    }
    assert changed == {}, changed


def test_read_only_refuses_every_non_read_tool_before_it_runs() -> None:
    """`upload`, a tool a deployment adds, and a valid write never run.

    Database-free: the refusal is by tool name, so any tool that is not a
    read operator -- the conditional `upload`, or one registered beyond the
    built-in set -- refuses with the skew, whatever its arguments.
    """
    from okg.substrate.mcp.response_bounds import ByteBoundedFastMCP
    from okg.substrate.mcp.server import read_only_release_from_report

    server = ByteBoundedFastMCP("read-only-refusal")
    ran: list[str] = []

    @server.tool(name="upload", structured_output=False)
    def upload(filename: str, content: str) -> dict:
        ran.append("upload")
        return {"stored": filename, "bytes": len(content)}

    @server.tool(name="deployment_widget_write", structured_output=False)
    def deployment_widget_write(widget_id: str, value: int) -> dict:
        ran.append("deployment_widget_write")
        return {"widget_id": widget_id, "value": value}

    @server.tool(name="inspect", structured_output=False)
    def inspect(max_bytes: int = 4_096) -> dict:
        del max_bytes
        ran.append("inspect")
        return {"node_count": 0}

    server._okg_read_only_release = read_only_release_from_report({
        "verdict": "read_only",
        "schema_behind_by": 1,
        "behind_migrations": [_BEHIND],
        "verdict_message": f"read-only; 1 migration behind ({_BEHIND})",
        "fix_hint": "run `okg up default`",
    })
    calls = {
        "upload": {"filename": "notes.txt", "content": "hello"},
        "deployment_widget_write": {"widget_id": "w1", "value": 3},
    }

    async def drive() -> dict[str, Any]:
        results = {
            name: await call_json(server, name, arguments)
            for name, arguments in calls.items()
        }
        results["inspect"] = await call_json(server, "inspect", {})
        return results

    results = anyio.run(drive)
    assert ran == ["inspect"]
    for name in calls:
        body = _payload(results[name])
        assert results[name].isError is True, (name, body)
        assert body["error"]["code"] == "schema_behind_read_only", (name, body)
        assert body["schema_behind_by"] == 1, (name, body)
        assert body["error"]["data"]["behind_migrations"] == [_BEHIND]


@pytest.mark.postgres
def test_a_current_database_adds_no_field(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch,
) -> None:
    """Additive only: a server that is not behind sends no new key."""
    from okg.substrate.mcp.server import build_server

    del okg_mcp_can_log_in
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    _publish(clean_runtime)
    server = build_server(
        dsn=clean_runtime, deployment_name="default", min_conns=1, max_conns=2,
    )
    bodies: list[dict] = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as client:
            bodies.append(_payload(await client.call_tool(
                "inspect", {"target": {"kind": "graph"}},
            )))

    try:
        anyio.run(drive)
    finally:
        server._okg_pool.close()
    assert bodies[0]["node_count"] == 2
    assert "schema_behind_by" not in bodies[0]


@pytest.mark.postgres
def test_undeclared_behind_refuses_to_start_with_the_release_verdict(
    one_migration_behind: str, monkeypatch, capsys,
) -> None:
    from okg.substrate import deployment_release

    _declare_release(monkeypatch, declare_reads_safe=False)
    code, stderr, launched = _run_mcp_serve(
        monkeypatch, capsys, one_migration_behind,
    )
    report = deployment_release.build_release_report(
        deployment="default", dsn=one_migration_behind, process_role="mcp",
    )
    error = json.loads(stderr.strip().splitlines()[-1])["error"]
    assert (code, launched) == (1, []), stderr
    assert report["verdict"] == "refuse"
    assert error["data"]["verdict"] == "refuse"
    assert error["data"]["verdict_reason"] == "schema_migrations_pending"
    assert error["message"] == report["refusal_message"]
    assert _BEHIND in error["message"]


@pytest.mark.postgres
def test_declared_behind_with_fingerprint_drift_refuses_to_start(
    one_migration_behind_and_drifted: str, monkeypatch, capsys,
) -> None:
    """Fail closed: a declared reads-safe migration does not excuse drift."""
    from okg.substrate import deployment_release

    dsn = one_migration_behind_and_drifted
    _declare_release(monkeypatch, declare_reads_safe=True)
    code, stderr, launched = _run_mcp_serve(monkeypatch, capsys, dsn)
    report = deployment_release.build_release_report(
        deployment="default", dsn=dsn, process_role="mcp",
    )
    error = json.loads(stderr.strip().splitlines()[-1])["error"]
    assert (code, launched) == (1, []), stderr
    assert report["verdict"] == "refuse"
    assert "schema_checksum_mismatch" in {
        p["kind"] for p in report["problems"]
    }
    assert error["data"]["verdict"] == "refuse"
    assert error["message"] == report["refusal_message"]
