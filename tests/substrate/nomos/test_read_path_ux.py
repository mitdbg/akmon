"""fix-nomos-read-path-ux: the Nomos read path is discoverable (GitHub #640/#644).

Four pinned proofs (one per requirement):

- ``test_mcp_default_subject_is_granted`` — an out-of-the-box MCP server
  presents ``subject:mcp_client`` when the ACTIVE bundle is the recognized
  deployment rollout baseline and grants it; explicit env still wins; a
  non-baseline bundle keeps the legacy ``$USER`` default
  (req.readux.mcp-subject-default).
- ``test_deny_names_grants`` — read-surface deny errors name the presented
  subject and the subjects the ACTIVE bundle grants for that surface, and
  the fix_hint points at presenting a granted subject when a policy IS
  active (activate-a-policy only when none is)
  (req.readux.deny-names-grants).
- ``test_readiness_read_probe`` — ``okg deployment ready`` evaluates the
  standard read subjects against the active bundle for the primary read
  surfaces and reports denied ones as a blocker
  (req.readux.readiness-read-probe).
- ``test_unpinned_view_read_is_loud`` — a bare-psql-equivalent SELECT from
  ``okg.v_nodes``/``okg.v_edges`` on a Chronos-native deployment raises the
  descriptive checkout error instead of silently returning 0 rows, while
  MCP run_sql (checkout-managed) keeps working and a non-Chronos empty
  table still reads as honestly empty (req.readux.raw-view-honesty).
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
import yaml

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

FIXTURE_SOURCE = "fixture"
FIXTURE_NODE = "node:readux"


@pytest.fixture(autouse=True)
def _no_subject_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test starts with no explicit Nomos subject exported."""
    for var in ("OKG_NOMOS_SUBJECT", "OKG_MCP_NOMOS_SUBJECT", "OKG_PRINCIPAL"):
        monkeypatch.delenv(var, raising=False)


def _baseline_manifest(
    read_principals: tuple[str, ...] = (
        "human_operator", "mcp_client", "okg_service",
    ),
) -> dict:
    return {
        "nomos": {
            "runtime_enforcement": "enforce",
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
                "read_principals": list(read_principals),
                "write_principals": [
                    "human_operator", "okg_service", "source_adapter",
                ],
                "mcp_write_default": "review",
                "export_policy": "no_export",
                "secret_handling": "secret_ref_required",
                "live_call_default": "deny",
            },
        },
    }


def _seed_baseline(dsn: str, deployment: str, manifest: dict) -> dict:
    """Seed the recognized rollout baseline exactly as `okg up`/provision do."""
    from okg.substrate.nomos import seed_deployment_baseline_nomos_state

    with psycopg.connect(dsn) as conn:
        payload = seed_deployment_baseline_nomos_state(
            conn,
            deployment_name=deployment,
            manifest=manifest,
            source_names=[FIXTURE_SOURCE],
            source_policies={FIXTURE_SOURCE: {"live_call_allowed": False}},
            runtime_source_names=[FIXTURE_SOURCE],
        )
        conn.commit()
    assert payload["ok"] is True, payload
    return payload


def _publish_fixture_graph(dsn: str, *, deployment: str = "default") -> dict:
    """One published node under source `fixture` (Chronos-native publish)."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_id=FIXTURE_NODE,
                    subtype="page",
                    attrs={"text": "nomos read path ux"},
                    source_record_id={"id": FIXTURE_NODE},
                    source_revision={"v": 1},
                )
            ],
            source=FIXTURE_SOURCE,
        )
    result = Publisher(dsn, deployment, catalog_identity=bind_test_catalog_writer(dsn, deployment)).cycle()
    assert result["status"] == "published", result
    return result


def _decision_subjects(dsn: str, deployment: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT action, effect, subject
              FROM okg.nomos_decisions
             WHERE deployment_name = %s
             ORDER BY created_at, decision_row_id
            """,
            (deployment,),
        )
        return [(str(a), str(e), str(s)) for a, e, s in cur.fetchall()]


# ---------------------------------------------------------------------------
# req.readux.mcp-subject-default
# ---------------------------------------------------------------------------


def test_mcp_default_subject_is_granted(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no subject env, okg mcp-serve presents subject:mcp_client under
    the recognized baseline and reads succeed; explicit env wins unchanged;
    a non-baseline active bundle keeps the legacy $USER default."""
    pytest.importorskip("mcp")
    from okg.substrate.mcp.server import build_server

    _publish_fixture_graph(clean_runtime)
    _seed_baseline(clean_runtime, "default", _baseline_manifest())
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("USER", "legacyosuser")

    # 1. Baseline default: no env subject -> subject:mcp_client, read allowed.
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        out = mcp._tool_manager._tools["inspect"].fn(target={"kind": "graph"})
    finally:
        mcp._okg_pool.close()
    assert out["node_count"] == 1
    decisions = _decision_subjects(clean_runtime, "default")
    boundary = [
        (action, effect, subject)
        for action, effect, subject in decisions
        if action in {"mcp.tool.call", "graph.read.mcp"}
    ]
    assert boundary, decisions
    assert all(effect == "allow" for _a, effect, _s in boundary), boundary
    assert all(
        subject == "subject:mcp_client" for _a, _e, subject in boundary
    ), boundary

    # 2. Explicit OKG_MCP_NOMOS_SUBJECT wins unchanged (and stays
    #    fail-closed: an undeclared subject is denied).
    monkeypatch.setenv("OKG_MCP_NOMOS_SUBJECT", "explicit_reader")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(SubstrateError, match="Nomos denied") as exc_info:
            mcp._tool_manager._tools["inspect"].fn(target={"kind": "graph"})
    finally:
        mcp._okg_pool.close()
    assert exc_info.value.data["presented_subject"] == "subject:explicit_reader"
    monkeypatch.delenv("OKG_MCP_NOMOS_SUBJECT")

    # 3. A NON-baseline active bundle keeps the legacy $USER default.
    from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph

    non_baseline = compile_policy_graph({
        "policy_bundle_id": "nomos:test:not-the-baseline",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [{
            "rule_id": "allow_nobody",
            "effect": "allow",
            "actions": ["mcp.tool.call"],
            "subjects": ["subject:someone-else"],
            "priority": 1,
            "reason_code": "nomos.test.unmatched",
        }],
    })
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=non_baseline,
            scope="deployment:default",
            activated_by="subject:nomos-rollout",
        )
        conn.commit()
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(SubstrateError, match="Nomos denied") as exc_info:
            mcp._tool_manager._tools["inspect"].fn(target={"kind": "graph"})
    finally:
        mcp._okg_pool.close()
    assert exc_info.value.data["presented_subject"] == "subject:legacyosuser"


# ---------------------------------------------------------------------------
# req.readux.deny-names-grants
# ---------------------------------------------------------------------------


def test_deny_names_grants(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Denied cli_search and mcp.tool.call under an active baseline name the
    presented subject and the granted subject list; the fix_hint names the
    subject knob; with NO active bundle the activate-a-policy hint stays."""
    pytest.importorskip("mcp")
    from okg.substrate.cli._read_side import _authorize_search_with_nomos
    from okg.substrate.mcp.server import build_server

    _publish_fixture_graph(clean_runtime)
    _seed_baseline(clean_runtime, "default", _baseline_manifest())
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")

    # 1. CLI: denied graph.read.cli_search names subject + grants + knob.
    session = SimpleNamespace(generation_id="gen-x", branch_name="default")
    with psycopg.connect(clean_runtime) as conn:
        with pytest.raises(SubstrateError) as exc_info:
            _authorize_search_with_nomos(
                conn,
                deployment="default",
                session=session,
                query="anything",
                method="lexical",
                limit=10,
                offset=0,
                filter_obj=None,
                select=None,
                access_context=None,
                subject="subject:stranger",
                subject_meta={"source": "env_override", "os_user": "x"},
            )
    err = exc_info.value
    assert err.code == "nomos_runtime_denied"
    assert err.data["surface"] == "graph.read.cli_search"
    assert err.data["presented_subject"] == "subject:stranger"
    granted = err.data["granted_subjects"]
    assert "subject:human_operator" in granted
    assert "subject:mcp_client" in granted
    assert "you presented subject:stranger" in err.message
    assert "subject:human_operator" in err.message
    # A policy IS active: the hint points at the subject knob, not at
    # activating a policy.
    assert "OKG_NOMOS_SUBJECT" in err.fix_hint
    assert "subject:human_operator" in err.fix_hint
    assert not err.fix_hint.startswith("activate")

    # 2. MCP: denied mcp.tool.call names subject + grants + the MCP knob.
    monkeypatch.setenv("OKG_MCP_NOMOS_SUBJECT", "stranger")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(SubstrateError) as exc_info:
            mcp._tool_manager._tools["inspect"].fn(target={"kind": "graph"})
    finally:
        mcp._okg_pool.close()
    err = exc_info.value
    assert err.code == "nomos_runtime_denied"
    assert err.data["surface"] == "mcp.tool.call"
    assert err.data["presented_subject"] == "subject:stranger"
    assert err.data["granted_subjects"] == ["subject:mcp_client"]
    assert "the active policy grants mcp.tool.call to: subject:mcp_client" in (
        err.message
    )
    assert "OKG_MCP_NOMOS_SUBJECT" in err.fix_hint
    monkeypatch.delenv("OKG_MCP_NOMOS_SUBJECT")

    # 3. NO active bundle: the activate-a-policy hint is kept.
    with psycopg.connect(clean_runtime) as conn:
        with pytest.raises(SubstrateError) as exc_info:
            _authorize_search_with_nomos(
                conn,
                deployment="readux-nobundle",
                session=session,
                query="anything",
                method="lexical",
                limit=10,
                offset=0,
                filter_obj=None,
                select=None,
                access_context=None,
                subject="subject:stranger",
                subject_meta={"source": "env_override", "os_user": "x"},
            )
    err = exc_info.value
    assert err.code == "nomos_runtime_denied"
    assert err.data["reason_code"] == "nomos.default_deny.no_active_bundle"
    assert err.data["active_policy_bundle_id"] is None
    assert err.data["granted_subjects"] == []
    assert "activate" in err.fix_hint


# ---------------------------------------------------------------------------
# req.readux.readiness-read-probe
# ---------------------------------------------------------------------------


def test_readiness_read_probe(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Readiness on a seeded enforce-mode deployment reports the read probe
    ok (and `deployment ready` carries the nomos.read_path check); after
    deactivating the bundle the probe names the denied subject/surface as a
    blocker."""
    from okg.substrate.cli.deployment_readiness import (
        _ready_nomos_read_probe_status,
    )

    deployment = "readuxprobe"
    manifest = _baseline_manifest()
    dep_dir = tmp_path / "deployments" / deployment
    dep_dir.mkdir(parents=True)
    (dep_dir / "deployment.yaml").write_text(
        yaml.safe_dump({"name": deployment, **manifest}), encoding="utf-8",
    )

    _publish_fixture_graph(clean_runtime, deployment=deployment)
    _seed_baseline(clean_runtime, deployment, manifest)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")

    with psycopg.connect(clean_runtime) as conn:
        probe = _ready_nomos_read_probe_status(
            conn,
            deployment_name=deployment,
            deployment_dir=dep_dir,
            readiness_profile="dogfood",
        )
    assert probe["ok"] is True, probe
    assert probe["status"] == "pass"
    assert probe["denied"] == []
    surfaces_probed = {
        (row["subject"], row["surface"]) for row in probe["probes"]
    }
    # The manifest read principals at the CLI boundary + mcp_client at the
    # MCP boundaries — the primary read surfaces.
    assert ("subject:human_operator", "graph.read.cli_search") in surfaces_probed
    assert ("subject:mcp_client", "mcp.tool.call") in surfaces_probed
    assert ("subject:mcp_client", "graph.read.mcp") in surfaces_probed
    assert all(row["allowed"] for row in probe["probes"]), probe["probes"]

    # Wired into `okg deployment ready`'s live checks as nomos.read_path.
    from okg.substrate.cli import deployment as deployment_cli

    monkeypatch.setattr(
        deployment_cli,
        "_ready_nomos_source_lineage_status",
        lambda *a, **k: {"ok": True, "mode": "active"},
    )
    live = deployment_cli._deployment_ready_live_checks(
        deployment=deployment,
        deployment_dir=dep_dir,
        dsn=clean_runtime,
        max_active_queue_depth=0,
        max_source_age_seconds=3600,
        readiness_profile="dogfood",
    )
    read_path_checks = [
        check for check in live["checks"] if check["id"] == "nomos.read_path"
    ]
    assert read_path_checks, live["checks"]
    assert read_path_checks[0]["status"] == "pass"

    # Deactivate the bundle: every probe is denied and named as a blocker.
    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.nomos_policy_activations "
                "   SET status = 'superseded', superseded_at = now() "
                " WHERE deployment_name = %s",
                (deployment,),
            )
        conn.commit()
        probe = _ready_nomos_read_probe_status(
            conn,
            deployment_name=deployment,
            deployment_dir=dep_dir,
            readiness_profile="dogfood",
        )
    assert probe["ok"] is False
    assert probe["status"] == "failed"
    assert probe["active_policy_bundle_id"] is None
    denied_pairs = {(row["subject"], row["surface"]) for row in probe["denied"]}
    assert ("subject:mcp_client", "mcp.tool.call") in denied_pairs
    assert ("subject:human_operator", "graph.read.cli_search") in denied_pairs
    assert all(
        finding["code"] == "nomos.runtime.read_subject_denied"
        for finding in probe["findings"]
    )
    assert probe["findings"], probe

    # And it lands as a blocker through the live checks wiring.
    live = deployment_cli._deployment_ready_live_checks(
        deployment=deployment,
        deployment_dir=dep_dir,
        dsn=clean_runtime,
        max_active_queue_depth=0,
        max_source_age_seconds=3600,
        readiness_profile="dogfood",
    )
    read_path_checks = [
        check for check in live["checks"] if check["id"] == "nomos.read_path"
    ]
    assert read_path_checks and read_path_checks[0]["status"] == "blocker"
    assert any(
        blocker["id"] == "nomos.read_path" for blocker in live["blockers"]
    )

    # Enforcement off -> the probe is skipped, not a blocker.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    with psycopg.connect(clean_runtime) as conn:
        probe = _ready_nomos_read_probe_status(
            conn,
            deployment_name=deployment,
            deployment_dir=dep_dir,
            readiness_profile="dogfood",
        )
    assert probe["status"] == "skipped"
    assert probe["ok"] is None


# ---------------------------------------------------------------------------
# req.readux.raw-view-honesty
# ---------------------------------------------------------------------------


def test_unpinned_view_read_is_loud(clean_runtime: str) -> None:
    """On a Chronos-native deployment with a published generation, a bare
    SELECT from okg.v_nodes/okg.v_edges in a fresh session raises the
    descriptive checkout error instead of 0 rows; MCP run_sql (checkout-
    managed) still succeeds; a non-Chronos-registered empty table still
    reads as honestly empty."""
    _publish_fixture_graph(clean_runtime)

    # 1. Bare-psql-equivalent reads raise descriptively, not 0 rows.
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        with pytest.raises(
            psycopg.errors.ObjectNotInPrerequisiteState,
            match="requires a Chronos checkout",
        ) as exc_info:
            cur.execute("SELECT count(*) FROM okg.v_nodes")
        hint = exc_info.value.diag.message_hint or ""
        assert "okg search" in hint
        assert "inspect" in hint
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        with pytest.raises(
            psycopg.errors.ObjectNotInPrerequisiteState,
            match="requires a Chronos checkout",
        ):
            cur.execute("SELECT edge_type, count(*) FROM okg.v_edges GROUP BY 1")

    # 2. MCP run_sql keeps working: it maps okg.v_nodes to the physical
    #    graph table by name and reads through a pinned Chronos checkout.
    from okg.substrate.mcp.run_sql import run_sql
    from okg.substrate.mcp.session import open_session

    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        payload = run_sql(
            conn,
            session,
            "SELECT node_id FROM okg.v_nodes ORDER BY node_id",
        )
    assert payload["rows"] == [[FIXTURE_NODE]]
    assert payload["query_plan_summary"] == "Chronos checkout"

    # 3. "Materialized empty" honesty: without the Chronos registration the
    #    logical table is plain storage, so empty means empty — 0 rows, no
    #    raise (transaction-local unregistration, rolled back).
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM public._chronos_branch_tables "
            "WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')"
        )
        cur.execute("SELECT count(*) FROM okg.v_nodes")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM okg.v_edges")
        assert cur.fetchone()[0] == 0
        conn.rollback()

    # 4. A materialized (non-empty) logical table reads normally — the
    #    guard only fires on the deceptive empty-but-registered state.
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.graph_nodes (node_id, subtype) "
            "VALUES ('materialized:1', 'page')"
        )
        cur.execute("SELECT count(*) FROM okg.v_nodes")
        assert cur.fetchone()[0] == 1
        conn.rollback()


def test_schema_digest_statement_error_surfaces_and_keeps_the_connection(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A digest statement that fails INSIDE the transaction.

    The error must surface unchanged, and the wrapper must roll back before
    restoring autocommit: psycopg refuses to switch autocommit on while a
    transaction is open, so without the rollback in
    `build_digest_payload`'s finally this raises an unrelated
    ProgrammingError that masks the original and leaves the pooled
    connection unusable.
    """
    from okg.substrate.mcp import schema_digest
    from okg.substrate.mcp.schema_digest_resource import build_digest_payload

    published = _publish_fixture_graph(clean_runtime)
    generation_id = str(published["generation_id"])

    def broken_reader(conn, _pinned_generation_id):
        def read(_sql, _params=None):
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM okg.no_such_digest_relation")
            raise AssertionError("unreachable: the statement above fails")

        return read

    monkeypatch.setattr(schema_digest, "_generation_reader", broken_reader)

    with psycopg.connect(clean_runtime) as conn:
        pool = SimpleNamespace(getconn=lambda: conn, putconn=lambda _c: None)
        with pytest.raises(psycopg.errors.UndefinedTable, match="no_such_digest"):
            build_digest_payload(
                pool=pool,
                deployment_name="default",
                digest_cache={},
                resolve_latest_fn=lambda _c, use_cache: (generation_id, 1),
            )
        assert conn.autocommit is True
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1


def test_schema_digest_reads_the_checkout_and_keeps_the_connection(
    clean_runtime: str,
) -> None:
    """The MCP schema-digest resource reads the pinned Chronos checkpoint.

    It used to read okg.v_subtypes/v_nodes on the bare connection, so the
    checkout guard fired on every chronos-native deployment
    (adversarial-review M1 pinned that error; PACT
    mcp-resources-fresh-install replaced the read). Two arms:

    * the published generation's text id returns the digest;
    * an id with no checkpoint raises its own descriptive error, and the
      wrapper rolls back before restoring autocommit, so that error is not
      masked by an unrelated ProgrammingError and the pooled connection
      stays usable.
    """
    import json

    from okg.substrate.graph_store_internal.read_branch import (
        GenerationCheckpointAbsent,
    )
    from okg.substrate.mcp.schema_digest_resource import build_digest_payload

    published = _publish_fixture_graph(clean_runtime)
    generation_id = str(published["generation_id"])

    with psycopg.connect(clean_runtime) as conn:
        pool = SimpleNamespace(
            getconn=lambda: conn,
            putconn=lambda _c: None,
        )
        digest = json.loads(
            build_digest_payload(
                pool=pool,
                deployment_name="default",
                digest_cache={},
                resolve_latest_fn=lambda _c, use_cache: (generation_id, 1),
            )
        )
        assert digest["pinned_generation_id"] == generation_id
        assert digest["subtypes"]["page"]["node_count"] == 1
        assert conn.autocommit is True

        with pytest.raises(GenerationCheckpointAbsent, match="generation:1"):
            build_digest_payload(
                pool=pool,
                deployment_name="default",
                digest_cache={},
                resolve_latest_fn=lambda _c, use_cache: (1, 1),
            )
        # The connection survives for the next pool user: autocommit was
        # restored on a rolled-back (not INERROR) transaction.
        assert conn.autocommit is True
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1
