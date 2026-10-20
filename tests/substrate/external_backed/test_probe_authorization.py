"""req.external-backed-nodes.probe-authorization.

A privileged `probe` passes a deny-by-default gate BEFORE any external call:
denied -> field state `denied` and the adapter is provably NOT invoked;
authorized -> the adapter runs; no policy -> default deny.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.external_backed.handle import HANDLE_ATTR
from okg.substrate.external_backed.resolver import hydrate_node

pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch):
    """These tests assert on the result dictionary, so every call asks for
    the JSON result format (issue #3059: the default is one text block).
    Client sessions send the request _meta key; in-process
    FastMCP.call_tool calls run inside result_format_scope."""
    from okg.substrate.mcp.result_text import result_format_scope
    from tests.substrate.mcp._result_payload import json_client_sessions

    json_client_sessions(monkeypatch)
    with result_format_scope("json"):
        yield

_DEPLOY = "default"
# The caller a probe is decided as (#2361): there is no default subject.
_CALLER = "subject:probe-caller"
_NODE = "node:svc:1"


def _conn(dsn):
    return psycopg.connect(dsn, autocommit=True)


def _priv_attrs():
    return {HANDLE_ATTR: {"status": {
        "current": "probe", "adapter": {"kind": "python"}, "ttl_seconds": 60,
        "cache_policy": "never-cache",
        "access": {"permission": "external.probe.invoke", "secret_refs": []},
    }}}


def _counting_runner(counter):
    def run(conn, spec, node_id):
        counter["n"] += 1
        return ("ok", {"up": True}, None)
    return run


def test_denied_marks_field_and_skips_adapter(clean_runtime: str) -> None:
    counter = {"n": 0}
    with _conn(clean_runtime) as conn:
        overlay = hydrate_node(
            conn, deployment_name=_DEPLOY, node_id=_NODE, attrs=_priv_attrs(),
            live_fields=["status"], gate=lambda f, p: False, principal=_CALLER,
            probe_runners={"python": _counting_runner(counter)},
        )
    f = overlay["fields"]["status"]
    assert f["state"] == "denied"
    assert "value" not in f  # never the adapter output, never a default
    assert counter["n"] == 0


def test_authorized_runs_adapter(clean_runtime: str) -> None:
    counter = {"n": 0}
    with _conn(clean_runtime) as conn:
        overlay = hydrate_node(
            conn, deployment_name=_DEPLOY, node_id=_NODE, attrs=_priv_attrs(),
            live_fields=["status"], gate=lambda f, p: True, principal=_CALLER,
            probe_runners={"python": _counting_runner(counter)},
        )
    assert overlay["fields"]["status"]["state"] == "live"
    assert counter["n"] == 1


def test_no_policy_default_deny(clean_runtime: str) -> None:
    # gate=None -> build_probe_gate(default) -> no bundle -> default deny.
    counter = {"n": 0}
    with _conn(clean_runtime) as conn:
        overlay = hydrate_node(
            conn, deployment_name=_DEPLOY, node_id=_NODE, attrs=_priv_attrs(),
            live_fields=["status"], gate=None, principal=_CALLER,
            probe_runners={"python": _counting_runner(counter)},
        )
    assert overlay["fields"]["status"]["state"] == "denied"
    assert overlay["fields"]["status"]["error"] == {
        "reason": "nomos.default_deny", "principal": _CALLER,
    }
    assert counter["n"] == 0


def test_unresolved_principal_is_refused_by_name_without_the_gate(
    clean_runtime: str,
) -> None:
    """No principal is refused before any gate or adapter runs (#2361)."""

    counter = {"n": 0}
    asked: list[object] = []
    with _conn(clean_runtime) as conn:
        overlay = hydrate_node(
            conn, deployment_name=_DEPLOY, node_id=_NODE, attrs=_priv_attrs(),
            live_fields=["status"],
            gate=lambda f, p: asked.append(p) or True,
            probe_runners={"python": _counting_runner(counter)},
        )
    field = overlay["fields"]["status"]
    assert field["state"] == "denied"
    assert field["error"] == {"reason": "probe_principal_unresolved", "principal": None}
    assert asked == []
    assert counter["n"] == 0

    def cache_rows() -> int:
        with _conn(clean_runtime) as conn:
            return conn.execute(
                "SELECT count(*) FROM okg.external_probe_cache WHERE node_id = %s",
                (_NODE,),
            ).fetchone()[0]

    # Nothing was decided, so nothing is cached; a named caller's denial is.
    assert cache_rows() == 0
    with _conn(clean_runtime) as conn:
        hydrate_node(
            conn, deployment_name=_DEPLOY, node_id=_NODE, attrs=_priv_attrs(),
            live_fields=["status"], gate=lambda f, p: False, principal=_CALLER,
            probe_runners={"python": _counting_runner(counter)},
        )
    assert cache_rows() == 1


def test_mcp_inspect_hydration_decides_the_probe_as_the_caller(
    clean_runtime: str, monkeypatch, tmp_path,
) -> None:
    """``inspect(node, hydrate)`` through the registered tool (#2361).

    The probe bundle allows ``external.probe.invoke`` for
    ``service:okg-runtime`` only. The caller is ``subject:probe-caller``.
    Before the fix the server passed no principal and the gate substituted
    the runtime service, so the probe was allowed and its adapter ran. Now
    the probe is denied as the caller and the adapter never runs.
    """

    import json
    import uuid

    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.external_backed import resolver
    from okg.substrate.external_backed.gating import BUNDLE_FILENAME, PROBE_ACTION
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.mcp.server import build_server
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import (
        SubtypeSpec,
        bind_test_catalog_writer,
        seed_catalog,
    )
    from tests.substrate.external_backed.test_live_relation_call_tool import (
        _deployment_with_bindings,
    )
    from tests.substrate.mcp._result_payload import payload

    node_id = f"node:svc:{uuid.uuid4().hex[:8]}"
    attrs = {HANDLE_ATTR: {"status": {
        "current": "probe",
        "adapter": {"kind": "sql", "config": {"query": "SELECT 1 AS up"}},
        "ttl_seconds": 60, "cache_policy": "never-cache",
        "access": {"permission": PROBE_ACTION, "secret_refs": []},
    }}}
    with _conn(clean_runtime) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec("service")], narrowings=[])
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(node_id, "service", attrs, {"id": node_id}, {"v": 1}),
        ])
    Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    _deployment_with_bindings(tmp_path, {})
    (tmp_path / "default" / BUNDLE_FILENAME).write_text(json.dumps({
        "policy_bundle_id": "probe-runtime-only",
        "policy_version": "1",
        "default_effect": "deny",
        "rules": [{
            "rule_id": "allow-runtime-probe",
            "effect": "allow",
            "priority": 100,
            "actions": [PROBE_ACTION],
            "subjects": ["service:okg-runtime"],
            "match": {"deployment": "default"},
        }],
    }), encoding="utf-8")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    monkeypatch.setenv("OKG_LIVEBIND_DSN", clean_runtime)
    for name in ("OKG_MCP_NOMOS_SUBJECT", "OKG_PRINCIPAL", "OKG_MCP_ALLOW_WRITES"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:probe-caller")
    adapter_calls: list[str] = []
    monkeypatch.setattr(
        resolver, "_run_sql_probe",
        lambda conn, spec, nid: adapter_calls.append(nid) or ("ok", {"up": 1}, None),
    )
    server = build_server(dsn=clean_runtime, deployment_name="default")
    out: dict[str, object] = {}

    async def run() -> None:
        async with create_connected_server_and_client_session(server) as client:
            result = await client.call_tool("inspect", {
                "target": {"kind": "node", "node_id": node_id, "hydrate": ["status"]},
            })
            out["payload"] = payload(result)

    anyio.run(run)
    body = out["payload"]
    assert isinstance(body, dict), body
    field = body["external_live"]["fields"]["status"]
    assert field["state"] == "denied", field
    assert field["error"] == {"reason": "nomos.default_deny", "principal": "subject:probe-caller"}
    assert adapter_calls == []
