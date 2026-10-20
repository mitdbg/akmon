from __future__ import annotations

from tests.substrate.mcp._result_payload import (
    json_client_sessions,
    payload as _result_payload,
    result_text as _result_text,
)

import os

import anyio
import psycopg
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from okg.substrate.local_database_roles import LOCAL_DATABASE_ROLE_PASSWORDS
from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes
from okg.substrate.mcp.server import build_server
from tests.substrate.conftest import alter_role_under_cluster_lock
from tests.substrate.mcp.test_mcp_actionable_bounds_e2e import _publish_fixture


pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests assert on the dictionary, so every client call asks for
    the JSON result format (issue #3059: the default is one text block)."""
    json_client_sessions(monkeypatch)


def _payload(result) -> dict:
    payload = _result_payload(result)
    assert "operator_execution_failed" not in _result_text(result)
    return payload


def test_manifest_backed_disposable_generation_serves_real_mcp_matrix() -> None:
    dsn = os.getenv("OKG_ISSUE1221_DISPOSABLE_DSN")
    if not dsn:
        pytest.skip("set OKG_ISSUE1221_DISPOSABLE_DSN for manifest-backed proof")
    deployment = "issue-1221-disposable"
    password = LOCAL_DATABASE_ROLE_PASSWORDS["okg_mcp"]
    with psycopg.connect(dsn, autocommit=True) as admin:
        row = admin.execute(
            "SELECT rolcanlogin FROM pg_roles WHERE rolname = 'okg_mcp'"
        ).fetchone()
        assert row is not None
        was_login = bool(row[0])
    alter_role_under_cluster_lock(
        dsn,
        psycopg.sql.SQL("ALTER ROLE okg_mcp LOGIN PASSWORD {}").format(
            psycopg.sql.Literal(password)
        ),
        lock_role="okg_mcp",
    )

    generation = _publish_fixture(
        dsn,
        deployment_name=deployment,
        seed_catalog_rows=False,
    )
    server = build_server(dsn=dsn, deployment_name=deployment)

    async def drive() -> None:
        try:
            async with create_connected_server_and_client_session(server) as client:
                calls = {
                    "invalid_direction": await client.call_tool(
                        "expand",
                        {
                            "input": {"node_id": "n:start"},
                            "path": ["first:sideways"],
                        },
                    ),
                    "invalid_edge": await client.call_tool(
                        "expand",
                        {
                            "input": {"node_id": "n:start"},
                            "path": ["unknown:out"],
                        },
                    ),
                    "missing": await client.call_tool(
                        "expand",
                        {"input": {"node_id": "n:missing"}, "path": ["first:out"]},
                    ),
                    "first_empty": await client.call_tool(
                        "expand",
                        {"input": {"node_id": "n:start"}, "path": ["empty:out"]},
                    ),
                    "later_empty": await client.call_tool(
                        "expand",
                        {
                            "input": {"node_id": "n:start"},
                            "path": ["first:out", "empty:out"],
                        },
                    ),
                    "terminal_empty": await client.call_tool(
                        "expand",
                        {
                            "input": {"node_id": "n:start"},
                            "path": ["first:out", "second:out"],
                            "target_subtype": "paper",
                        },
                    ),
                    "truncated": await client.call_tool(
                        "expand",
                        {
                            "input": {"node_id": "n:start"},
                            "path": ["first:out"],
                            "limit": 2,
                            "select": ["node_id"],
                        },
                    ),
                    "one_hop": await client.call_tool(
                        "expand",
                        {
                            "input": {"node_id": "n:start"},
                            "edge_type": "first",
                            "limit": 1,
                        },
                    ),
                    "summary": await client.call_tool(
                        "inspect",
                        {
                            "target": {
                                "kind": "subtype",
                                "subtype": "large_doc",
                                "page_size": 8,
                            },
                            "max_bytes": 4_096,
                        },
                    ),
                    "invalid_subtype_detail": await client.call_tool(
                        "inspect",
                        {
                            "target": {
                                "kind": "subtype",
                                "subtype": "large_doc",
                                "detail": "full_everything",
                            },
                            "max_bytes": 4_096,
                        },
                    ),
                }
        finally:
            server._okg_pool.close()

        assert _payload(calls["invalid_direction"])["error"]["code"] == (
            "invalid_path_direction"
        )
        assert _payload(calls["invalid_edge"])["error"]["code"] == (
            "invalid_path_edge_type"
        )
        expected = {
            "missing": "start_not_visible",
            "first_empty": "no_matching_first_hop",
            "later_empty": "empty_intermediate_hop",
            "terminal_empty": "empty_terminal_result",
            "truncated": "nonempty_truncated",
        }
        for name, code in expected.items():
            payload = _payload(calls[name])
            assert payload.get("outcome", {}).get("code") == code, payload
            assert payload["outcome"]["next_move"]
            assert payload["pinned_generation_id"] == generation
        one_hop = _payload(calls["one_hop"])
        assert one_hop["neighbors"]
        assert one_hop["next_offset"] > 0
        summary = _payload(calls["summary"])
        assert summary["pinned_generation_id"] == generation
        assert summary["sample_nodes"] is None
        assert summary["continuation_token"]
        assert public_call_tool_result_bytes(summary) <= 4_096
        invalid_detail = _payload(calls["invalid_subtype_detail"])
        assert calls["invalid_subtype_detail"].isError is True
        assert invalid_detail["error"]["code"] == "invalid_subtype_inspection"

    try:
        anyio.run(drive)
    finally:
        alter_role_under_cluster_lock(
            dsn,
            psycopg.sql.SQL(
                "ALTER ROLE okg_mcp LOGIN"
                if was_login
                else "ALTER ROLE okg_mcp NOLOGIN"
            ),
            lock_role="okg_mcp",
        )
