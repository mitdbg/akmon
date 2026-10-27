"""Real-publish proof for the cross-source endpoint wedge (task real-publish).

Sources a (owns widget), b (note references widget) and an unrelated c run
through the real ingest service and publisher on a disposable deployment:

1. b before a: b publishes its note, the edge waits;
2. a publishes the widget: the edge is promoted;
3. a retires the widget: the edge returns to waiting, the publish is not
   blocked, and c keeps publishing.

Each step records the published generation, node and edge counts, the waiting
rows by state, ``deferred_edges`` from the run result, the source-sync queue
and dead-letter depth, the database doctor and ``doctor --check
deferred-edges``, and one MCP read. When ``OKG_WEDGE_EVIDENCE_OUT`` names a
file, the steps are written there as JSON (pact evidence).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.db.management import database_health_report
from okg.substrate.runtime_queue_health import summarize_runtime_queue_health
from tests.pytest import endpoint_wedge_fixture as fx
from tests.pytest.source_jobs_inline import use_inline_source_jobs

pytestmark = pytest.mark.postgres


def _payload(result: Any) -> dict[str, Any]:
    if getattr(result, "isError", False):
        return {"refused": str(result.content)}
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict):
        return structured
    contents = result[0] if isinstance(result, tuple) else getattr(
        result, "content", result,
    )
    text = "; ".join(c.text for c in contents if hasattr(c, "text"))
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # The server answers in text; the first line names the generation.
        return {"text": text, "first_line": text.splitlines()[0] if text else ""}


def _mcp_read(dsn: str) -> dict[str, Any]:
    from okg.substrate.mcp.server import build_server

    mcp = build_server(dsn=dsn, deployment_name="svc")

    async def drive() -> dict[str, Any]:
        graph = _payload(await mcp.call_tool(
            "inspect", {"target": {"kind": "graph"}},
        ))
        note = _payload(await mcp.call_tool(
            "inspect", {"target": {"kind": "node", "node_id": "note:1"}},
        ))
        hop = _payload(await mcp.call_tool(
            "expand",
            {"input": {"node_id": "note:1"}, "path": ["references:out"]},
        ))
        return {
            "call": "inspect(node note:1) and expand(note:1, references:out)",
            "node_found": "refused" not in note
            and "node_not_found" not in json.dumps(note, default=str),
            "mentions_widget_1": "widget:1" in json.dumps(hop, default=str),
            "result_first_line": note.get("first_line", ""),
            "graph_overview_first_line": graph.get("first_line", ""),
        }

    return asyncio.run(drive())


def _snapshot(
    dsn: str, capsys: pytest.CaptureFixture[str], step: str, outcome: Any,
    source: str,
) -> dict[str, Any]:
    from okg.substrate.cli.doctor import _doctor_deferred_edges

    publishes = outcome.report["publishes"]
    result = fx.result_for(outcome, source)
    waiting = fx.waiting_rows(dsn)
    by_state: dict[str, int] = {}
    for row in waiting:
        by_state[row["state"]] = by_state.get(row["state"], 0) + 1
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        health = summarize_runtime_queue_health(cur)
    capsys.readouterr()
    exit_code = _doctor_deferred_edges(
        argparse.Namespace(dsn=dsn, deployment=None), as_json=True,
    )
    doctor_edges = json.loads(capsys.readouterr().out)
    doctor_edges.pop("sample", None)
    database = database_health_report(dsn, deployment_name="svc")
    return {
        "step": step,
        "generation_id": str(publishes[-1]["generation_id"]),
        "publish_status": publishes[-1]["status"],
        "counts": {
            **fx.graph_counts(dsn),
            "waiting": len(waiting),
            "waiting_by_state": by_state,
        },
        "deferred_edges": result.get("deferred_edges"),
        "queue_depth": health["active_queue_depth"],
        "dlq_count": health["dlq_count"],
        "doctor_database_ok": bool(database["ok"]),
        "doctor_deferred_edges": {
            "exit_code": exit_code,
            "ok": doctor_edges.get("ok"),
            "total": doctor_edges.get("total"),
            "groups": doctor_edges.get("groups"),
            "oldest_deferred_at": doctor_edges.get("oldest_deferred_at"),
            "warn": doctor_edges.get("warn"),
        },
        "mcp_read": _mcp_read(dsn),
    }


def test_real_publish_sequence(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    use_inline_source_jobs(monkeypatch)
    dep = fx.build_deployment(tmp_path, clean_runtime)
    steps: list[dict[str, Any]] = []

    fx.write_fixture(
        dep, "b", [fx.node("note:1", "note")], [fx.edge("note:1", "widget:1")],
    )
    outcome = fx.ingest(clean_runtime, dep, "b")
    steps.append(_snapshot(
        clean_runtime, capsys, "1 b before a: the edge waits", outcome, "b",
    ))

    keep = fx.node("widget:2", "widget")
    fx.write_fixture(dep, "a", [fx.node("widget:1", "widget"), keep], [])
    outcome = fx.ingest(clean_runtime, dep, "a")
    steps.append(_snapshot(
        clean_runtime, capsys, "2 a publishes the widget: promoted", outcome,
        "a",
    ))

    fx.write_fixture(dep, "a", [keep], [])
    outcome = fx.ingest(clean_runtime, dep, "a")
    steps.append(_snapshot(
        clean_runtime, capsys, "3 a retires the widget: waits again", outcome,
        "a",
    ))

    first, second, third = steps
    assert first["publish_status"] == "published"
    assert first["counts"]["edges"] == 0 and first["counts"]["waiting"] == 1
    assert first["deferred_edges"]["count"] == 1
    assert second["publish_status"] == "published"
    assert second["counts"]["edges"] == 1 and second["counts"]["waiting"] == 0
    assert third["publish_status"] == "published"
    assert third["counts"]["edges"] == 0 and third["counts"]["waiting"] == 1
    assert len({step["generation_id"] for step in steps}) == 3
    assert all(step["dlq_count"] == 0 for step in steps)
    assert all(step["doctor_database_ok"] for step in steps)
    assert all(step["mcp_read"]["node_found"] for step in steps)
    assert [step["mcp_read"]["mentions_widget_1"] for step in steps] == [
        False, True, False,
    ]

    out = os.environ.get("OKG_WEDGE_EVIDENCE_OUT")
    if out:
        Path(out).write_text(
            json.dumps({
                "schema": "okg.cross_source_endpoint_wedge.real_publish.v1",
                "deployment": "svc",
                "sources": {
                    "a": "owns widget", "b": "note references widget",
                    "c": "unrelated",
                },
                "steps": steps,
            }, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
