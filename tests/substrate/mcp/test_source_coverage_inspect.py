"""Executed-test half of task.coverage.pinned-manifest.

Covers req.coverage.pinned-manifest from
pact/changes/mcp-candidate-cards-and-coverage/pact.yaml.

The state this proves is the one an analyst cannot currently reach: `search`
answers `total_count: 0` identically whether the corpus has no match or whether
the source that would have answered was never ingested, and the surfaces that
know the difference are either unpinned (`source_freshness` reads a live
watermark) or on the admin write surface (`list_sources`, `source_status`). So
these tests assert the manifest keeps included / not_ingested / unregistered /
declared_gap apart, stays stable under a pin, and refuses to invent a run
outcome the runtime never stored.
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path

import psycopg
import pytest

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session
from okg.substrate.mcp.source_coverage import (
    STATE_DECLARED_GAP,
    STATE_INCLUDED,
    STATE_NOT_INGESTED,
    STATE_UNREGISTERED,
    CoverageConfigInvalid,
    inspect_coverage,
    parse_coverage_config,
)
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import publish_nodes


pytestmark = pytest.mark.postgres


CHUNK = SubtypeSpec("analysis_chunk", metadata={"searchable_text_fields": ["text"]})
#: `publish_nodes` applies facts under this producer, so a registry entry
#: claiming it is an INGESTED source and any other entry is a declared-but-
#: absent one. Both cases are needed on one manifest.
FIXTURE_PRODUCER = "system:analyst_search_fixture"


def _make_coverage_deployment(root: Path, *, dsn: str, coverage: dict | None) -> str:
    """A deployment declaring three sources, one of which actually ingests.

    Written as JSON (valid YAML), mirroring `search_fixture.make_search_deployment`;
    the returned string is the deployment DIRECTORY, which is what the session's
    `deployment_name` is throughout these fixtures.
    """
    deployment = root / "coverage-analyst"
    (deployment / "schemas").mkdir(parents=True)
    (deployment / "source_registry.yaml").write_text(
        json.dumps(
            {
                "sources": {
                    # Ingests: its ownership_id is the producer the fixture
                    # publishes under.
                    "papers": {"ownership_id": FIXTURE_PRODUCER},
                    # Declared and never ingested.
                    "mailbox": {"ownership_id": "coverage-analyst.mailbox"},
                    "tickets": {},
                }
            }
        ),
        encoding="utf-8",
    )
    manifest = {
        "name": "coverage-analyst",
        "description": "coverage fixture",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "postgres": {"dsn": dsn},
    }
    if coverage is not None:
        manifest["coverage"] = coverage
    (deployment / "deployment.yaml").write_text(
        json.dumps(manifest), encoding="utf-8",
    )
    return str(deployment)


def _node(node_id: str, text: str) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=CHUNK.name,
        attrs={"text": text, "authored_at": "2026-03-04T10:00:00+00:00"},
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


def _by_source(payload: dict) -> dict:
    return {
        (row["source_id"] or row["ownership_id"]): row
        for row in payload["sources"]
    }


# ---------- strict parsing (no database) ----------


def test_an_unknown_coverage_key_raises_rather_than_being_ignored() -> None:
    """A dropped key would leave a deployment believing it disclosed a gap."""
    with pytest.raises(CoverageConfigInvalid) as excinfo:
        parse_coverage_config({"known_gap": [{"source": "a", "description": "b"}]})
    assert "unknown key" in str(excinfo.value)

    with pytest.raises(CoverageConfigInvalid):
        parse_coverage_config(
            {"known_gaps": [{"source": "a", "reason": "typo'd key"}]}
        )
    with pytest.raises(CoverageConfigInvalid):
        parse_coverage_config({"known_gaps": [{"source": "a", "description": ""}]})
    with pytest.raises(CoverageConfigInvalid):
        parse_coverage_config(
            {
                "known_gaps": [
                    {"source": "a", "description": "one"},
                    {"source": "a", "description": "two"},
                ]
            }
        )


def test_an_absent_block_is_legal_and_an_empty_one_parses() -> None:
    assert parse_coverage_config(None) is None
    assert parse_coverage_config({}) == {"known_gaps": []}
    assert parse_coverage_config(
        {"known_gaps": [{"source": "a", "description": "b"}]}
    ) == {"known_gaps": [{"source": "a", "description": "b"}]}


# ---------- the pinned manifest ----------


def test_coverage_separates_ingested_absent_unregistered_and_declared_gaps(
    clean_runtime: str,
    tmp_path,
) -> None:
    deployment = _make_coverage_deployment(
        tmp_path,
        dsn=clean_runtime,
        coverage={
            "known_gaps": [
                {"source": "mailbox", "description": "mailbox was never connected"},
                {"source": "archive", "description": "pre-2020 archive is out of scope"},
            ]
        },
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_node("chunk:one", "alpha"), _node("chunk:two", "beta")],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        payload = inspect_coverage(conn, session)

    rows = _by_source(payload)
    # (a) declared AND present, with the live count at the pin.
    assert rows["papers"]["state"] == STATE_INCLUDED
    assert rows["papers"]["live_record_count"] == 2
    assert rows["papers"]["registered"] is True
    # (b) declared and NOT ingested — the state `search` cannot express.
    assert rows["mailbox"]["state"] == STATE_NOT_INGESTED
    assert rows["mailbox"]["live_record_count"] == 0
    assert rows["tickets"]["state"] == STATE_NOT_INGESTED
    # (e) the deployment's own disclosure, kept beside the measurement.
    assert rows["mailbox"]["known_gap"]["declared_by"] == "deployment.yaml"
    assert "never connected" in rows["mailbox"]["known_gap"]["description"]
    # A gap declared for a source neither the registry nor the graph names is
    # still reported: the deployment is disclosing something real about it.
    assert rows["archive"]["state"] == STATE_DECLARED_GAP
    # An ingested source with no declared gap says so explicitly.
    assert rows["papers"]["known_gap"] is None
    assert payload["states"][STATE_INCLUDED] == 1
    assert payload["states"][STATE_NOT_INGESTED] == 2
    assert payload["truncated"] is False
    assert payload["registry_available"] is True
    assert payload["pinned_generation_id"] == payload["generation_id"]


def test_a_policy_denied_source_appears_nowhere_in_the_payload(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A denied source must not leak back through the `unregistered` pass.

    The visibility gate drops denied sources from the DECLARED list. The bug
    this pins: if the map that the unregistered pass subtracts is built from
    the FILTERED list, a denied-but-declared source stops matching, falls into
    that pass, and is re-emitted with its `ownership_id` and its live record
    count — the exact existence fact the gate exists to withhold, arriving by
    the back door and now labelled `unregistered`, which is also a lie.

    The shared gate `_source_visible_for_subject` is patched rather than a
    policy bundle activated: it is the single definition every source-listing
    surface consults, so denying through it is what a real deny does to this
    code path, without coupling the test to policy-activation plumbing.
    """
    deployment = _make_coverage_deployment(
        tmp_path, dsn=clean_runtime, coverage=None,
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_node("chunk:one", "alpha"), _node("chunk:two", "beta")],
    )

    import okg.substrate.mcp.source_lifecycle as lifecycle

    # `papers` owns the fixture producer, so it is the source that HAS rows —
    # denying it is the case where a leak would carry a real count.
    monkeypatch.setattr(
        lifecycle,
        "_source_visible_for_subject",
        lambda conn, dep, source_id, **_kw: source_id != "papers",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        payload = inspect_coverage(conn, session)

    blob = json.dumps(payload, default=str)
    assert "papers" not in blob, payload
    assert FIXTURE_PRODUCER not in blob, payload
    rows = _by_source(payload)
    assert "papers" not in rows
    assert FIXTURE_PRODUCER not in rows
    # Not re-emitted as `unregistered`, and not counted in any tally.
    assert payload["states"][STATE_UNREGISTERED] == 0
    assert payload["states"][STATE_INCLUDED] == 0
    assert all(row["state"] == STATE_NOT_INGESTED for row in payload["sources"])
    assert payload["sources_total"] == len(payload["sources"])
    # The sources the caller MAY see are still reported.
    assert "mailbox" in rows


def test_records_under_an_undeclared_source_are_reported_not_dropped(
    clean_runtime: str,
    tmp_path,
) -> None:
    """A renamed or removed source leaves rows that still answer queries."""
    deployment = _make_coverage_deployment(
        tmp_path, dsn=clean_runtime, coverage=None,
    )
    # Rewrite the registry so the fixture producer is no longer declared.
    (Path(deployment) / "source_registry.yaml").write_text(
        json.dumps({"sources": {"mailbox": {}}}), encoding="utf-8",
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_node("chunk:stray", "gamma")],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        payload = inspect_coverage(conn, session)

    rows = _by_source(payload)
    assert rows[FIXTURE_PRODUCER]["state"] == STATE_UNREGISTERED
    assert rows[FIXTURE_PRODUCER]["registered"] is False
    assert rows[FIXTURE_PRODUCER]["live_record_count"] == 1
    assert rows["mailbox"]["state"] == STATE_NOT_INGESTED


def test_coverage_is_pinned_to_the_session_generation(
    clean_runtime: str,
    tmp_path,
) -> None:
    """A later publish does not move a pinned session's counts."""
    deployment = _make_coverage_deployment(
        tmp_path, dsn=clean_runtime, coverage=None,
    )
    _cv, first_generation = publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_node("chunk:one", "alpha")],
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_node("chunk:two", "beta"), _node("chunk:three", "delta")],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        pinned = open_session(
            conn,
            deployment_name=deployment,
            requested_generation=first_generation,
        )
        latest = open_session(conn, deployment_name=deployment)
        pinned_payload = inspect_coverage(conn, pinned)
        latest_payload = inspect_coverage(conn, latest)

    assert _by_source(pinned_payload)["papers"]["live_record_count"] == 1
    assert _by_source(latest_payload)["papers"]["live_record_count"] == 3
    assert pinned_payload["pinned_generation_id"] == first_generation


def test_the_run_block_never_reports_health_the_runtime_did_not_store(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Nothing in the schema stores a run OUTCOME as a typed column."""
    deployment = _make_coverage_deployment(
        tmp_path, dsn=clean_runtime, coverage=None,
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_node("chunk:one", "alpha")],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        payload = inspect_coverage(conn, session)

    run = _by_source(payload)["papers"]["run"]
    # This fixture publishes through the Publisher, not a SourceRunner, so no
    # run was ever recorded. `recorded: false` GATES the rest: a null
    # `last_error` beside it would be read as "no failure", which is a claim
    # nothing in the schema supports.
    assert run["recorded"] is False
    assert "last_run_status" not in run
    assert "last_error" not in run
    assert "last_run_completed_scope" not in run
    assert "no failure was RECORDED" in payload["run_semantics"]
    # No envelope either, so the admission block is recorded:false with no
    # unavailable reason -- absence, stated, rather than left implicit.
    assert run["admission"] == {"recorded": False, "unavailable": None}


def test_a_source_refused_at_admission_is_not_reported_as_merely_absent(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Issue #1323. `v_source_frontier_state` is derived from
    `okg.source_progress`, which a REFUSED admission never writes, so
    `run.recorded: false` alone cannot tell "never asked to run" from "ran and
    was refused on every attempt" -- the false negative that hid
    `okg-workspace.latex_project` for its whole enrolment."""
    deployment = _make_coverage_deployment(
        tmp_path, dsn=clean_runtime, coverage=None,
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_node("chunk:one", "alpha")],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            for status in ("rejected", "rejected"):
                cur.execute(
                    """
                    INSERT INTO okg.admission_envelopes
                      (deployment_name, producer_id, producer_kind, intent,
                       admission_mode, status, idempotency_key, source,
                       verdict)
                    VALUES (%s, 'coverage-analyst.mailbox', 'source',
                            'source_run', 'fast_track', %s, %s,
                            'coverage-analyst.mailbox', %s::jsonb)
                    """,
                    (
                        deployment,
                        status,
                        str(uuid.uuid4()),
                        json.dumps({
                            "code": "producer_edge_triple_not_allowed",
                        }),
                    ),
                )
        session = open_session(conn, deployment_name=deployment)
        payload = inspect_coverage(conn, session)

    by_source = _by_source(payload)
    refused = by_source["mailbox"]["run"]
    assert refused["recorded"] is False
    assert refused["admission"]["recorded"] is True
    assert refused["admission"]["refused"] is True
    assert refused["admission"]["status"] == "rejected"
    assert (
        refused["admission"]["reason_code"]
        == "producer_edge_triple_not_allowed"
    )
    assert refused["admission"]["adverse_count"] == 2
    # Non-vacuity: a source with no envelope of its own must NOT inherit the
    # refusal.
    assert by_source["papers"]["run"]["admission"]["recorded"] is False
    assert "run.admission" in payload["run_semantics"]


def test_select_narrows_the_work_and_an_unknown_block_is_refused(
    clean_runtime: str,
    tmp_path,
) -> None:
    from okg.substrate.mcp.envelope import SelectUnknownField

    deployment = _make_coverage_deployment(
        tmp_path, dsn=clean_runtime, coverage=None,
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_node("chunk:one", "alpha")],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        narrowed = inspect_coverage(conn, session, ["records"])
        with pytest.raises(SelectUnknownField):
            inspect_coverage(conn, session, ["reccords"])

    row = _by_source(narrowed)["papers"]
    assert row["live_record_count"] == 1
    # An unselected block does not run its query AND does not emit its key: a
    # key that is neither always-present nor gated is how a zero gets emitted
    # for a block that never ran.
    assert "run" not in row
    assert "authored_at_span" not in row
    assert "known_gap" not in row
    assert narrowed["blocks"] == ["records"]


def test_narrowing_select_never_changes_a_sources_state(
    clean_runtime: str,
    tmp_path,
) -> None:
    """`select` narrows FIELDS, never the classification.

    Regression: when the live-record aggregate was gated on the `records`
    block, `select=["runs"]` skipped it and every ingested source came back
    `not_ingested` — the precise false negative this target exists to prevent,
    delivered by the argument meant to make the call cheaper.
    """
    deployment = _make_coverage_deployment(
        tmp_path, dsn=clean_runtime, coverage=None,
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_node("chunk:one", "alpha")],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        full = inspect_coverage(conn, session)
        runs_only = inspect_coverage(conn, session, ["runs"])
        gaps_only = inspect_coverage(conn, session, ["known_gaps"])

    for payload in (full, runs_only, gaps_only):
        rows = _by_source(payload)
        assert rows["papers"]["state"] == STATE_INCLUDED, payload["blocks"]
        assert rows["mailbox"]["state"] == STATE_NOT_INGESTED
        assert payload["states"] == full["states"]
    # The count itself is still gated: it is a field, not the classification.
    assert "live_record_count" not in _by_source(runs_only)["papers"]


def test_the_span_block_reports_a_measured_range_or_names_why_it_cannot(
    clean_runtime: str,
    tmp_path,
) -> None:
    deployment = _make_coverage_deployment(
        tmp_path, dsn=clean_runtime, coverage=None,
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[
            NodeFact(
                node_id="chunk:early",
                subtype=CHUNK.name,
                attrs={"text": "alpha", "authored_at": "2024-01-02T00:00:00+00:00"},
                source_record_id={"id": "chunk:early"},
                source_revision={"v": 1},
            ),
            NodeFact(
                node_id="chunk:late",
                subtype=CHUNK.name,
                attrs={"text": "beta", "authored_at": "2026-09-09T00:00:00+00:00"},
                source_record_id={"id": "chunk:late"},
                source_revision={"v": 1},
            ),
            # Carries no authored_at at all, so it must not enter the span.
            _bare("chunk:undated"),
        ],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        payload = inspect_coverage(conn, session, ["records", "spans"])

    span = _by_source(payload)["papers"]["authored_at_span"]
    assert span["attribute"] == "authored_at"
    assert span["lo"] == "2024-01-02T00:00:00+00:00"
    assert span["hi"] == "2026-09-09T00:00:00+00:00"
    assert span["rows"] == 2
    # Named for what it IS — a text ordering whose endpoints were proved to
    # parse — not for an instant-wise min/max it never computed.
    assert span["ordering"] == "lexical_iso8601"


def _bare(node_id: str) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=CHUNK.name,
        attrs={"text": "undated"},
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


def test_the_coverage_target_is_reachable_through_the_real_inspect_tool(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drive the registered MCP tool, not just the module function.

    Every other test here calls `inspect_coverage` directly, which proves the
    manifest but proves nothing about the dispatch that reaches it. A target
    kind wired into the wrong branch — or not wired at all — would leave all of
    them green while the tool answered `unsupported_inspect_target`.
    """
    from okg.substrate.mcp.server import build_server

    deployment = _make_coverage_deployment(
        tmp_path, dsn=clean_runtime, coverage=None,
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_node("chunk:one", "alpha")],
    )
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:test-operator")
    mcp = build_server(
        dsn=clean_runtime,
        dsn_authority={
            "dsn": clean_runtime,
            "authority": {"mode": "test_fixture", "deployment": deployment},
            "target": None,
        },
        deployment_name=deployment,
    )
    try:
        payload = mcp._tool_manager._tools["inspect"].fn(
            target={"kind": "coverage"},
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert payload.get("status") != "refused", payload
    assert payload["operator_name"] == "inspect"
    assert payload["pinned_generation_id"]
    rows = _by_source(payload)
    assert rows["papers"]["state"] == STATE_INCLUDED
    assert rows["papers"]["live_record_count"] == 1
    assert rows["mailbox"]["state"] == STATE_NOT_INGESTED


def test_a_reduced_max_bytes_yields_a_typed_refusal_not_a_truncated_manifest(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`inspect` is not pageable, so an over-budget manifest is refused WHOLE.

    This is the behaviour an agent has to be able to plan around, and it is the
    half the earlier byte test never exercised: it measured the payload but
    never actually lowered `max_bytes`, so it proved the manifest was small
    without proving what happens when it is not. Driven through the registered
    tool with `max_bytes` at the 4096-byte floor and enough sources to exceed
    it, the caller gets a typed `response_byte_budget_exceeded` refusal naming
    the budget — never a silently shortened source list, which would read as a
    complete answer about a graph that has fewer sources than it does.
    """
    from okg.substrate.mcp.response_bounds import MIN_RESPONSE_BYTES
    from okg.substrate.mcp.server import build_server

    deployment = _make_coverage_deployment(
        tmp_path, dsn=clean_runtime, coverage=None,
    )
    (Path(deployment) / "source_registry.yaml").write_text(
        json.dumps({"sources": {
            "papers": {"ownership_id": FIXTURE_PRODUCER},
            **{f"source-{index:03d}": {} for index in range(60)},
        }}),
        encoding="utf-8",
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_node("chunk:one", "alpha")],
    )
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:test-operator")
    mcp = build_server(
        dsn=clean_runtime,
        dsn_authority={
            "dsn": clean_runtime,
            "authority": {"mode": "test_fixture", "deployment": deployment},
            "target": None,
        },
        deployment_name=deployment,
    )
    try:
        refused = mcp._tool_manager._tools["inspect"].fn(
            target={"kind": "coverage"}, max_bytes=MIN_RESPONSE_BYTES,
        )
        roomy = mcp._tool_manager._tools["inspect"].fn(
            target={"kind": "coverage"},
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert refused["status"] == "refused", refused
    assert refused["error"]["code"] == "response_byte_budget_exceeded"
    assert refused["truncated"] is True
    assert refused["response_byte_budget"] == MIN_RESPONSE_BYTES
    # No partial data rides along on the refusal.
    assert "sources" not in refused
    # The same call inside the default budget answers normally, so the refusal
    # is about the budget rather than about the payload being malformed.
    assert _by_source(roomy)["papers"]["state"] == STATE_INCLUDED


def test_the_manifest_stays_valid_structured_data_under_the_byte_budget(
    clean_runtime: str,
    tmp_path,
) -> None:
    """`inspect` is not pageable: an oversized payload is refused WHOLE.

    There is no cursor to continue from, so a manifest that grew with the
    source count would become a hard refusal on exactly the deployments that
    most need it. The property is therefore that the payload is bounded BY
    CONSTRUCTION: it fits the default budget, the cap is what bounds it, and
    `sources_total` stays exact so the bound is visible rather than silent.

    Measured against `public_call_tool_result_bytes`, which is the same
    SDK-boundary measurement the byte bound applies.
    """
    from okg.substrate.mcp.response_bounds import (
        DEFAULT_RESPONSE_BYTES,
        public_call_tool_result_bytes,
    )

    deployment = _make_coverage_deployment(
        tmp_path,
        dsn=clean_runtime,
        coverage={
            "known_gaps": [
                {"source": "mailbox", "description": "mailbox was never connected"},
            ]
        },
    )
    # Far more sources than a real deployment declares, so the cap is the
    # thing under test rather than an unreachable ceiling.
    (Path(deployment) / "source_registry.yaml").write_text(
        json.dumps(
            {
                "sources": {
                    "papers": {"ownership_id": FIXTURE_PRODUCER},
                    **{f"source-{index:03d}": {} for index in range(300)},
                }
            }
        ),
        encoding="utf-8",
    )
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[CHUNK],
        nodes=[_node(f"chunk:{index}", "alpha") for index in range(25)],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        payload = inspect_coverage(conn, session)

    # Round-trips as JSON, so nothing in it is an unserializable object.
    assert json.loads(json.dumps(payload, default=str))
    assert public_call_tool_result_bytes(payload) <= DEFAULT_RESPONSE_BYTES
    # Bounded, and the bound is disclosed rather than silently applied.
    assert payload["truncated"] is True
    assert payload["sources_total"] > payload["source_cap"]
    assert len(payload["sources"]) == payload["source_cap"]
    # The state tally counts EVERY source, not just the returned page, so a
    # truncated manifest still answers "is my source here at all?" honestly.
    assert payload["states"][STATE_NOT_INGESTED] == 300
    assert payload["states"][STATE_INCLUDED] == 1
    # `mailbox` is no longer in the rewritten registry, so its declared gap is
    # all that is left of it.
    assert payload["states"][STATE_DECLARED_GAP] == 1
    assert payload["sources_total"] == 302
