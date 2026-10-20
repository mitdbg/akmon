from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.mcp import (
    aggregate_edge,
    aggregate_node,
    expand_path,
    inspect_graph,
    inspect_subtype,
    open_session,
    search,
)
from tests.substrate.mcp.test_nomos_source_lineage_mcp import (
    _activate_source_policy,
    _publish_lineage_graph,
)


pytestmark = pytest.mark.postgres


def test_read_surfaces_hide_denied_lineage_counts_and_paths(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        graph = inspect_graph(conn, session)
        node_rollup = aggregate_node(conn, session, "node:public-docs")
        edge_rollup = aggregate_edge(conn, session)
        path = expand_path(
            conn,
            session,
            "node:public-docs",
            hops=[("references", "out")],
        )
        hits = search(conn, session, "docs", method="lexical", limit=10)

    assert graph["node_count"] == 2
    assert graph["edge_count"] == 1
    assert graph["schema"]["subtypes"]["page"]["node_count"] == 2
    assert node_rollup["counts"] == [
        {"edge_type": "references", "dst_subtype": "page", "count": 1}
    ]
    assert node_rollup["total_count"] == 1
    assert edge_rollup["routes"] == [
        {
            "src_subtype": "page",
            "edge_type": "references",
            "dst_subtype": "page",
            "count": 1,
        }
    ]
    assert edge_rollup["total_count"] == 1
    assert path["results"] == []
    assert path["outcome"]["code"] == "visibility_limited"
    assert path["outcome"]["visibility"] == "observed_diagnostics_withheld"
    assert path["total_count"] is None
    assert path["count_state"] == "withheld"
    assert [row["node_id"] for row in hits["results"]] == ["node:public-docs"]
    assert hits["total_count"] is None and hits["total_count_basis"] == "unknown"


def test_inspect_subtype_enforce_hides_denied_lineage(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Under enforce within the budget, a denied source's rows must not
    influence node_count, attr-key frequencies, route summaries, or samples
    (issue #627: inspect_subtype previously bypassed the filter entirely)."""
    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        payload = inspect_subtype(conn, session, "page")

    # Enforce mode exposes declared catalog shape only. Choosing an exact path
    # based on a raw row-count threshold leaks whether hidden rows crossed that
    # threshold, so observed counts/frequencies/samples are withheld uniformly.
    assert payload["node_count"] is None
    assert payload["truncated"] is False
    assert payload["sample_nodes"] is None
    assert payload["attr_keys"] == []
    assert payload["visibility"]["status"] == "observed_statistics_withheld"
    # The route is catalog-declared, not inferred from visible or hidden rows.
    assert payload["edges_out"] == [
        {"edge_type": "references", "dst_subtype": "page", "source": "declared"}
    ]
    assert payload["edges_in"] == [
        {"edge_type": "references", "src_subtype": "page", "source": "declared"}
    ]


def test_aggregate_edge_enforce_hides_denied_lineage(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Under enforce, denied-source edges contribute to no route count, and
    above the budget the counts are omitted rather than leaked."""
    from okg.substrate.mcp import tools

    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        rollup = aggregate_edge(conn, session)
        monkeypatch.setattr(tools, "_AGGREGATE_EDGE_EXACT_ROW_BUDGET", 0)
        omitted = aggregate_edge(conn, session)

    # Two 'references' edges exist; the one into the denied google-drive
    # node must not count.
    assert rollup["routes"] == [
        {
            "src_subtype": "page",
            "edge_type": "references",
            "dst_subtype": "page",
            "count": 1,
        }
    ]
    assert rollup["truncated"] is False
    assert omitted["routes"] is None
    assert omitted["total_count"] is None
    assert omitted["truncated"] is True
    assert omitted.get("note")


def test_inspect_graph_enforce_large_omits_counts(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Above the exact-count budget under enforce mode, inspect(graph) must not
    return whole-graph counts (that would disclose hidden-source cardinality).
    It returns a bounded stub with counts omitted and truncated=True."""
    from okg.substrate.mcp import tools

    _publish_lineage_graph(clean_runtime)
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")
    # Force the large-graph branch on the tiny fixture.
    monkeypatch.setattr(tools, "_INSPECT_GRAPH_EXACT_ROW_BUDGET", 0)

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        graph = inspect_graph(conn, session)

    assert graph["truncated"] is True
    assert graph["node_count"] is None
    assert graph["edge_count"] is None
    assert graph["schema"] is None
    assert "enforce" in graph["visibility"]
    assert graph.get("note")


def test_cli_search_hides_denied_source_lineage(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _publish_lineage_graph(clean_runtime)
    _activate_cli_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")

    from okg.substrate.cli import _read_side
    from okg.substrate.cli.dsn_authority import ReadDsnResolution

    monkeypatch.setattr(
        _read_side,
        "resolve_read_dsn",
        lambda _args, *, surface: ReadDsnResolution(
            dsn=clean_runtime,
            deployment="default",
            authority={"mode": "test", "surface": surface},
            target=None,
        ),
    )

    exit_code = _read_side.cmd_search(
        SimpleNamespace(
            deployment="default",
            dsn=None,
            generation_id=None,
            query="docs",
            method="lexical",
            subtype=None,
            attr_filter=None,
            select=None,
            limit=10,
            offset=0,
        ),
        as_json=True,
    )

    output = capsys.readouterr().out
    payload = json.loads(output)
    assert exit_code == 0
    assert [row["node_id"] for row in payload["results"]] == ["node:public-docs"]
    assert payload["total_count"] is None and payload["total_count_basis"] == "unknown"
    assert payload["nomos_effect"] == "allow"
    assert "google-drive" not in output


def test_source_admin_hides_denied_source_counts_and_freshness(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate_source_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-client")
    _patch_source_admin_registry(monkeypatch)

    from okg.substrate.mcp import source_lifecycle

    with psycopg.connect(clean_runtime) as conn:
        listed = source_lifecycle.list_sources(conn, "default")
        hidden = source_lifecycle.source_status(conn, "default", "google_drive")

    assert [source["source_id"] for source in listed["sources"]] == ["public_docs"]
    assert listed["sources"][0]["node_count"] == 7
    assert listed["sources"][0]["edge_count"] == 3
    assert listed["sources"][0]["last_ingest_at"] == "public-watermark"
    assert listed["counts"] == {"active": 1}
    assert hidden == {
        "status": "not_authorized",
        "deployment": "default",
        "agent_hint": (
            "Nomos source-lineage policy hides this source from the current subject."
        ),
    }


def _patch_source_admin_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    from okg.substrate.mcp import source_lifecycle

    registry = {
        "sources": {
            "google_drive": {
                "ownership_id": "google_drive",
                "enabled_by_default": True,
            },
            "public_docs": {
                "ownership_id": "public_docs",
                "enabled_by_default": True,
            },
        },
    }

    def counts_for(_conn: psycopg.Connection, ownership_id: str) -> dict[str, int]:
        if ownership_id != "public_docs":
            raise AssertionError("hidden source counts must not be queried")
        return {"node_count": 7, "edge_count": 3}

    def watermark_for(
        _conn: psycopg.Connection,
        ownership_id: str,
    ) -> dict[str, str | None]:
        if ownership_id != "public_docs":
            raise AssertionError("hidden source freshness must not be queried")
        return {"last_ingest_at": "public-watermark"}

    # The helper loader is None-guarded per helper (see
    # source_lifecycle._load_source_admin_helpers), so monkeypatched
    # helpers survive the first invocation without any flag.
    monkeypatch.setattr(
        source_lifecycle,
        "_load_registry_yaml",
        lambda _deployment: (Path("source_registry.yaml"), registry),
    )
    monkeypatch.setattr(source_lifecycle, "_watermark_for", watermark_for)
    monkeypatch.setattr(source_lifecycle, "_counts_for", counts_for)
    monkeypatch.setattr(
        source_lifecycle,
        "_preflight_status",
        lambda _source_id, _entry: "active",
    )
    monkeypatch.setattr(source_lifecycle, "_last_run_counts", lambda *_args: None)
    monkeypatch.setattr(source_lifecycle, "_latest_source_progress", lambda *_args: None)
    monkeypatch.setattr(
        source_lifecycle,
        "_aliases_for_registry_source",
        lambda source_id: [source_id],
    )


def _activate_cli_source_policy(dsn: str) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:cli-source-lineage",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_cli_search_boundary",
                "effect": "allow",
                "actions": ["graph.read.cli_search"],
                "subjects": ["subject:mcp-client"],
                "match": {
                    "query": "docs",
                    "method": "lexical",
                },
                "priority": 200,
            },
            {
                "rule_id": "allow_public_docs_mcp_read",
                "effect": "allow",
                "actions": ["graph.read.mcp"],
                "subjects": ["subject:mcp-client"],
                "match": {
                    "deployment": "default",
                    "scope": "deployment:default",
                    "source_id": "public_docs",
                },
                "priority": 100,
            },
        ],
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bundle,
            scope="deployment:default",
            activated_by="test:operator",
        )
        conn.commit()
