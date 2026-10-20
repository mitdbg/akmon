"""The OpenSearch reference deployment, proved against the real container.

PACT `opensearch-reference-integration` (issue #2014), req.ori.walk and the
container-backed half of req.ori.source.

WHY THESE LIVE HERE AND NOT BESIDE THEIR SIBLINGS
-------------------------------------------------
The rest of this change's end-to-end proofs are in
`tests/substrate/e2e/test_opensearch_reference.py` and run in CI against
recorded response bodies. These two cannot: they need a Docker daemon, the
digest-pinned OpenSearch image, and a scratch Postgres.

They were originally kept out of the lanes by the `live_opensearch` marker.
That does not work, and the lane-scope gate is right to refuse it: a changed
test row must be owned by a blocking per-pull-request lane step whose marker
expression ADMITS it, so a row every lane deselects can have no owner at all.
The two ways out of that are both wrong -- let the lane select rows that then
fail for want of a container, or let them skip, which reads as a passed arm
(#2078).

So they moved to `tests/live/`, which exists for exactly this and says so in
its own conftest: not collected unless the operator asks. Absent is honest in
a way that skipped is not.

RUNNING THEM
------------
    export OKG_TEST_DSN=postgres://...@127.0.0.1:5703/okg_test_s20os
    OKG_OPENSEARCH_LIVE=1 pytest tests/live/opensearch -q -s

The container is brought up by the test itself, through the documented
command, and removed afterwards along with its volume. `OKG_TEST_DSN` must
name a DISPOSABLE Postgres: the run drops and recreates
`okg_opensearch_reference` on that server.

Once `OKG_OPENSEARCH_LIVE=1` is set these FAIL rather than skip when the
environment is not ready. Being asked to run and being unable to is a real
failure; it is only before the flag that silence is correct.
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import psycopg
import pytest

# The recorded-response arm's helpers, imported rather than copied: both arms
# must assert the SAME numbers or the diff between them proves nothing, and a
# second copy is how the two drift apart without anyone noticing.
from tests.substrate.e2e.test_opensearch_reference import (
    EXPECTED_EDGES,
    EXPECTED_NODES,
    REFERENCE,
    REPO_ROOT,
    _assert_published_graph,
    _recreate_database,
    _rows,
    _with_database,
)

pytestmark = pytest.mark.live_opensearch


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

#: Charter #2008's exit bound, amended 2026-09-20, in tokens since PACT
#: surface-token-budget (Jason, 2026-10-07, `tokenbudget A`).
#:
#: Neither the EXACT pin nor the ceiling is here. Both live in
#: `tests/substrate/mcp/test_tool_surface_budget.py` as
#: `REFERENCE_READ_SURFACE_BASELINE_TOKENS` and `READ_SURFACE_CEILING_TOKENS`,
#: where CI runs them, and are imported below. What this arm adds is that
#: the number holds on a server built against a REAL database rather than
#: the probe DSN.

#: The seed is deterministic: 40 app-logs documents, one every 15 minutes
#: from a pinned epoch, so an hourly histogram over them is ten buckets of
#: four. A wrong time field or a wrong interval cannot produce this shape.
EXPECTED_HISTOGRAM_BUCKETS = 10
EXPECTED_BUCKET_DOC_COUNT = 4

#: `level` is `warn` on every eighth document, so five of forty.
EXPECTED_WARN_DOCUMENTS = 5

COMPOSE = REFERENCE / "opensearch/docker-compose.yaml"


@pytest.fixture
def reference_server_dsn() -> str:
    """A DISPOSABLE Postgres server this run may drop databases on.

    Read from `OKG_TEST_DSN` rather than from the substrate's `clean_runtime`
    fixture: these proofs live outside `tests/substrate/`, and depending on
    that tree's conftest would tie a container proof to a fixture built for a
    different purpose. FAILS rather than skips when unset -- the flag that
    got us here already said the operator wants these to run.
    """
    dsn = os.environ.get("OKG_TEST_DSN")
    if not dsn:
        pytest.fail(
            "OKG_OPENSEARCH_LIVE=1 was set but OKG_TEST_DSN is not. These "
            "proofs drop and recreate `okg_opensearch_reference` on the "
            "server it names, so point it at a disposable Postgres on "
            "loopback -- never a shared one."
        )
    return dsn


def _cluster_query_total(base_url: str = "http://127.0.0.1:9209") -> int:
    """How many search queries the CLUSTER has served.

    Read from OpenSearch itself, not from the substrate. "A denied call never
    reaches the container" cannot be proved from the returned state: that
    cannot tell a call that never happened from one whose result was thrown
    away. This counter can.
    """
    import urllib.request

    with urllib.request.urlopen(f"{base_url}/_stats/search", timeout=10) as fh:
        body = json.load(fh)
    return int(body["_all"]["total"]["search"]["query_total"])


def _sample_rows(dsn: str) -> list[dict]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT relation, decision, status, policy, payload_sha256, "
            "       attrs, payload "
            "  FROM okg.call_sample_warm ORDER BY sample_id"
        )
        return [
            {
                "relation": r[0], "decision": r[1], "status": r[2],
                "policy": r[3], "payload_sha256": r[4], "attrs": r[5],
                "payload": r[6],
            }
            for r in cur.fetchall()
        ]


def _admission_payload(dsn: str, admission_id: str) -> dict:
    """The STORED review payload, read back rather than trusted.

    `record` reports what it queued; this reads what the database holds, so
    an assertion about the four identifiers is about the durable envelope a
    reviewer will open, not about the operator's return value.
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT payload FROM okg.admission_record_refs "
            " WHERE admission_id = %s::uuid AND ref_kind = 'payload'",
            (admission_id,),
        )
        row = cur.fetchone()
    assert row is not None, f"no stored payload ref for {admission_id}"
    return row[0]


async def _measure_reference_surface():
    """The registered read surface of THIS deployment, measured not guessed."""
    os.environ["OKG_MCP_SURFACE_PROBE"] = "1"
    try:
        from okg.substrate.mcp.surface_budget import measure_surface

        return await measure_surface(deployment="opensearch-reference")
    finally:
        os.environ.pop("OKG_MCP_SURFACE_PROBE", None)


def test_the_documented_command_brings_the_reference_up(
    reference_server_dsn: str,
    capsys,
) -> None:
    """Run `up.sh` against the real container and read the graph it built.

    Not skipped when unconfigured: a skipped arm reads as a passed one,
    and this is the only thing that proves the documented command works
    and that the recorded fixtures still match the real image. Set
    OKG_OPENSEARCH_LIVE=1 to opt in; excluded from automated lanes by
    marker.

    Unlike the recorded arm this does NOT stage a renamed copy. The
    documented command names the real deployment, and `okg deployment up`
    refuses a DSN whose database is not the one the manifest declares --
    correctly, so the only honest way to exercise it is against
    `okg_opensearch_reference` itself, on the disposable server the test
    DSN points at.
    """
    # No env-flag guard here. `tests/live/conftest.py` does not collect
    # this module at all unless OKG_OPENSEARCH_LIVE=1, so a check for it
    # could never fire -- and an unreachable guard reads as protection
    # while providing none, which is the mistake that file records.
    deployment_dsn = _with_database(reference_server_dsn, "okg_opensearch_reference")
    _recreate_database(reference_server_dsn, "okg_opensearch_reference")
    try:
        subprocess.run(
            ["bash", str(REFERENCE / "up.sh")],
            check=True, cwd=REPO_ROOT,
            env={**os.environ, "OKG_DSN": deployment_dsn},
        )

        live = _assert_published_graph(
            deployment_dsn, capsys, "opensearch-reference",
        )
        # The diff that catches a drifted fixture: the live cluster must
        # produce exactly what the recorded bodies do.
        assert live["nodes"] == EXPECTED_NODES
        assert live["edges"] == EXPECTED_EDGES

        doc_counts = _rows(
            deployment_dsn,
            """
            SELECT attrs->>'name' AS name,
                   (attrs->>'doc_count')::int AS docs
              FROM okg.graph_nodes WHERE subtype = 'search_index'
             ORDER BY 1
            """,
        )
        assert {r["name"]: r["docs"] for r in doc_counts} == {
            "app-logs-2026.09": 40,
            "audit-trail-2026.09": 12,
            "ingest-events-2026.09": 25,
        }, doc_counts

        # The declared exclusion actually fired against the real cluster.
        # How MANY plugin indices exist depends on how long the container
        # has been up -- a freshly started one has one, and the
        # query-insights index appears later and mints a new dated one as
        # the clock rolls over -- so the assertion is that the rule fired,
        # not a count that would be a race.
        cluster = _rows(
            deployment_dsn,
            "SELECT (attrs->>'excluded_index_count')::int AS n, "
            "       attrs->'excluded_index_globs' AS globs "
            "  FROM okg.graph_nodes WHERE subtype = 'search_cluster'",
        )
        assert cluster[0]["n"] >= 1, cluster
        globs = cluster[0]["globs"]
        if isinstance(globs, str):
            globs = json.loads(globs)
        assert globs == ["top_queries-*"], globs
    finally:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE), "down", "-v"],
            check=False, cwd=REPO_ROOT,
        )


# ── the live MCP walk (req.ori.walk) ──────────────────────────────────

def test_the_agent_walk_reaches_the_live_cluster_through_mcp(
    reference_server_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """req.ori.walk, driven through `build_server` + `call_tool`.

    Every step is the REGISTERED tool over a real client session, because a
    helper-level test cannot tell "the branches are correct" from "the
    branches are reached" -- which is the defect #2013's own review found in
    itself. And the deny step is counted at the CLUSTER, because the
    returned state cannot distinguish a call that never happened from one
    whose result was discarded.

    The walk enters the graph at the INDEX rather than at the service.
    When it was written, `Service` (shared `service` library module)
    declared no `okg_searchable_text_fields`, so a service node was
    reachable only by id; #2158 has since made its `name` searchable. The
    graph path walked is the same either way, entered from the index: index
    -> its writing services, then the index's live relations.
    """
    # No env-flag guard here. `tests/live/conftest.py` does not collect
    # this module at all unless OKG_OPENSEARCH_LIVE=1, so a check for it
    # could never fire -- and an unreachable guard reads as protection
    # while providing none, which is the mistake that file records.
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.external_backed.live_dispatch import build_live_gate
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.mcp.server import build_server
    from okg.substrate.runtime import admission_workflows

    def payload_of(result):
        structured = getattr(result, "structuredContent", None)
        if structured is not None:
            return structured
        return json.loads(result.content[0].text)

    deployment_dsn = _with_database(reference_server_dsn, "okg_opensearch_reference")
    _recreate_database(reference_server_dsn, "okg_opensearch_reference")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(REPO_ROOT / "deployments"))
    # `record` is made under the MCP client's subject. Reads and record
    # resolve one subject (#2157); it is stated here anyway so the walk
    # depends neither on the baseline default nor on who is logged in.
    monkeypatch.setenv("OKG_MCP_NOMOS_SUBJECT", "subject:mcp_client")

    observed: dict[str, object] = {}
    try:
        subprocess.run(
            ["bash", str(REFERENCE / "up.sh")],
            check=True, cwd=REPO_ROOT,
            env={**os.environ, "OKG_DSN": deployment_dsn},
        )

        # Warm the process-level Chronos registry OUTSIDE any transaction.
        # The admission workflow calls `ChronosGraphStore.from_connection`
        # on its own connection, and under `mock_dbos_context` that
        # connection is inside a psycopg `Transaction`, where Chronos's
        # autocommit-style commit is forbidden. Bootstrapping here means the
        # call inside has nothing left to do.
        with psycopg.connect(deployment_dsn, autocommit=True) as warm:
            ChronosGraphStore.from_connection(warm)

        with mock_dbos_context():
            monkeypatch.setattr(
                admission_workflows, "_resolve_dsn",
                lambda _deployment: deployment_dsn,
            )
            admission_workflows.register_admission_workflows(
                deployment="opensearch-reference",
            )
            server = build_server(
                dsn=deployment_dsn,
                deployment_name="opensearch-reference",
                allow_writes=True,
            )
            # NO `live_adapters`. The runner is the real HTTP adapter
            # (#2100) that `build_server` resolves from the binding, so this
            # dials the container.
            assert sorted(server._okg_live_bindings) == [
                "documents", "histogram",
            ]
            assert sorted(server._okg_live_relation_runners) == [
                "documents", "histogram",
            ]

            async def walk() -> None:
                async with create_connected_server_and_client_session(
                    server,
                ) as client:
                    # 1. Find the index by the name an operator would type.
                    result = await client.call_tool(
                        "search", {"query": "app-logs", "limit": 10},
                    )
                    hits = payload_of(result)["results"]
                    index_node = next(
                        h["node_id"] for h in hits
                        if h["subtype"] == "search_index"
                    )
                    observed["index_node"] = index_node

                    # 2. Who writes to it.
                    result = await client.call_tool("expand", {
                        "input": {"node_id": index_node},
                        "edge_type": "logs_to", "direction": "in",
                        "limit": 10,
                    })
                    observed["writers"] = sorted(
                        n["src"] for n in payload_of(result)["neighbors"]
                    )

                    # 3. What it can be ASKED, with the argument schemas.
                    result = await client.call_tool("inspect", {
                        "target": {"kind": "node", "node_id": index_node},
                    })
                    relations = payload_of(result)["live_relations"]
                    observed["relations"] = relations

                    # 4. The live row query, gated and dispatched.
                    before = _cluster_query_total()
                    result = await client.call_tool("expand", {
                        "input": {"node_id": index_node},
                        "edge_type": "contains", "direction": "in",
                        "limit": 5,
                        "live": {"relation": "documents", "args": {
                            "index": "app-logs-2026.09",
                            "query": "level:warn",
                            "time_window": "now-90d",
                            "limit": 5,
                        }},
                    })
                    body = payload_of(result)
                    observed["documents"] = body["external_live"]
                    observed["documents_queries"] = (
                        _cluster_query_total() - before
                    )

                    # 5. The aggregation.
                    before = _cluster_query_total()
                    result = await client.call_tool("expand", {
                        "input": {"node_id": index_node},
                        "edge_type": "contains", "direction": "in",
                        "limit": 5,
                        "live": {"relation": "histogram", "args": {
                            "index": "app-logs-2026.09",
                            "query": "*",
                            "time_window": "now-90d",
                            "interval": "1h",
                        }},
                    })
                    observed["histogram"] = payload_of(
                        result,
                    )["external_live"]
                    observed["histogram_queries"] = (
                        _cluster_query_total() - before
                    )

                    # 6. DENIED, and counted at the cluster.
                    bundle_less = tmp_path / "no-bundle"
                    (bundle_less / "opensearch-reference").mkdir(parents=True)
                    saved_gate = server._okg_live_gate
                    server._okg_live_gate = build_live_gate(
                        "opensearch-reference",
                        deployments_root=bundle_less,
                    )
                    before = _cluster_query_total()
                    result = await client.call_tool("expand", {
                        "input": {"node_id": index_node},
                        "edge_type": "contains", "direction": "in",
                        "limit": 5,
                        "live": {"relation": "documents", "args": {
                            "index": "app-logs-2026.09",
                            "query": "level:warn",
                            "time_window": "now-90d",
                        }},
                    })
                    body = payload_of(result)
                    observed["denied"] = body["external_live"]
                    observed["denied_queries"] = (
                        _cluster_query_total() - before
                    )
                    # The graph rows are a complete answer to the graph
                    # question and survive a refused live call.
                    observed["denied_neighbors"] = len(body["neighbors"])
                    server._okg_live_gate = saved_gate

                    # 7. The finding, through the REGISTERED `record` tool
                    #    over the same client session, under the
                    #    deployment's own `review` admission mode.
                    #
                    #    Driven through `call_tool` like every other step.
                    #    Reaching into `_tool_manager._tools[...].fn` was
                    #    the earlier shape and it skipped the transport: it
                    #    could not have caught a `record` that failed
                    #    schema validation, or one this launch did not
                    #    register at all.
                    live = observed["documents"]
                    samples = _sample_rows(deployment_dsn)
                    observed["samples"] = samples
                    allowed = [
                        s for s in samples if s["decision"] == "allow"
                    ]
                    result = await client.call_tool("record", {
                        "kind": "finding",
                        "payload": {
                            "statement": (
                                f"app-logs-2026.09 carries "
                                f"{EXPECTED_WARN_DOCUMENTS} warn-level "
                                f"documents in the last 90 days, read live "
                                f"from the reference cluster with "
                                f"{json.dumps(live['query'], sort_keys=True)}."
                            ),
                            "confidence_kind": "measured",
                            # The four links req.ori.walk asks for. They are
                            # ATTRIBUTES, not reference edges, and that is
                            # forced: `mcp/record.py`'s `_prefix_for_id`
                            # resolves a reference only when the node id
                            # carries a declared RECORD prefix, so a finding
                            # cannot point an edge at a source-minted node
                            # like this index. Reported; the identifiers are
                            # carried where they do survive.
                            "scope": str(live["pinned_generation_id"]),
                            "source_artifact": str(observed["index_node"]),
                            "source_hash": str(allowed[0]["payload_sha256"]),
                        },
                        "authority": {
                            "producer_id": "agent:osref-walk",
                            "producer_kind": "agent",
                        },
                    })
                    assert not result.isError, payload_of(result)
                    observed["finding"] = payload_of(result)

            anyio.run(walk)
            server._okg_pool.close()
            if server._okg_write_pool:
                server._okg_write_pool.close()

        # ── assertions ───────────────────────────────────────────────

        assert observed["writers"] == ["osref:service:okg-api"], observed

        relations = observed["relations"]
        assert sorted(relations) == ["documents", "histogram"], relations
        assert sorted(relations["documents"]["args"]) == [
            "index", "limit", "query", "time_window",
        ]
        assert sorted(relations["histogram"]["args"]) == [
            "field", "index", "interval", "query", "time_window",
        ]
        assert relations["documents"]["action"] == (
            "external.opensearch.search"
        )
        assert relations["documents"]["sample_policy"] == "inline"

        docs = observed["documents"]
        assert docs["state"] == "ok", docs
        assert docs["trust"] == "untrusted"
        assert docs["source"] == "http"
        assert len(docs["rows"]) == EXPECTED_WARN_DOCUMENTS, docs["rows"]
        assert observed["documents_queries"] > 0, "the cluster was not dialled"

        hist = observed["histogram"]
        assert hist["state"] == "ok", hist
        assert len(hist["rows"]) == EXPECTED_HISTOGRAM_BUCKETS, hist["rows"]
        assert {r["doc_count"] for r in hist["rows"]} == {
            EXPECTED_BUCKET_DOC_COUNT,
        }, hist["rows"]
        # Fenced with the same primitive the #83 overlay uses: `trust:
        # untrusted` is a label a model may ignore, the markers are not.
        assert "UNTRUSTED_DATA" in json.dumps(hist["rows"]), hist["rows"][:1]
        assert observed["histogram_queries"] > 0

        denied = observed["denied"]
        assert denied["state"] == "denied", denied
        assert denied["error"]["reason"] == (
            "nomos.default_deny.no_active_bundle"
        ), denied
        # THE assertion: the cluster served nothing for the denied call.
        assert observed["denied_queries"] == 0, observed["denied"]
        assert observed["denied_neighbors"] > 0, "graph rows were discarded"

        samples = observed["samples"]
        by_outcome = [(s["relation"], s["decision"], s["status"])
                      for s in samples]
        assert by_outcome == [
            ("documents", "allow", "ok"),
            ("histogram", "allow", "ok"),
            ("documents", "deny", "denied"),
        ], by_outcome
        # The SHAPED payload, not the adapter's raw return, and the same
        # bytes the caller received.
        assert samples[0]["policy"] == "inline"
        assert len(samples[0]["payload"]) == EXPECTED_WARN_DOCUMENTS
        assert samples[0]["attrs"]["adapter_ran"] is True
        assert samples[0]["attrs"]["reason_code"] == (
            "opensearch_reference.live_search_allowed"
        )
        # The denied row proves the negative from the substrate's side, as
        # the cluster counter proves it from the other.
        assert samples[2]["attrs"]["adapter_ran"] is False
        assert samples[2]["payload"] is None

        finding = observed["finding"]
        assert finding["status"] == "pending", finding
        assert finding["admission_mode"] == "review", finding
        assert finding["inserted"] is True, finding
        assert finding["admission_id"], finding
        # All four identifiers reached the STORED envelope.
        envelope = _admission_payload(deployment_dsn, finding["admission_id"])
        blob = json.dumps(envelope, default=str)
        assert str(observed["index_node"]) in blob, blob[:600]
        assert str(docs["pinned_generation_id"]) in blob, blob[:600]
        assert str(samples[0]["payload_sha256"]) in blob, blob[:600]
        assert "level:warn" in blob, blob[:600]

        # The exact pin is asserted hermetically in
        # `test_tool_surface_budget.py`; it is imported rather than restated
        # so the two can never disagree about what this deployment serves.
        from tests.substrate.mcp.test_tool_surface_budget import (
            READ_SURFACE_CEILING_TOKENS,
            REFERENCE_READ_SURFACE_BASELINE_TOKENS,
        )

        measured = anyio.run(_measure_reference_surface)
        assert measured.tool_names == ("expand", "inspect", "search"), measured
        assert measured.total_tokens == REFERENCE_READ_SURFACE_BASELINE_TOKENS, (
            f"the opensearch-reference read surface now serves "
            f"{measured.total_tokens} tokens, not the recorded "
            f"{REFERENCE_READ_SURFACE_BASELINE_TOKENS}. Every session pays "
            f"this before asking anything.\n{measured.breakdown()}"
        )
        assert measured.total_tokens <= READ_SURFACE_CEILING_TOKENS
    finally:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE), "down", "-v"],
            check=False, cwd=REPO_ROOT,
        )
