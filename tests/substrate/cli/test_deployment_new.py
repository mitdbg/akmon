"""Focused coverage for the ``okg deployment new`` scaffold foundation."""
from __future__ import annotations

import json
import subprocess
import uuid
from pathlib import Path
from urllib.parse import quote, urlencode

import psycopg
import pytest
import yaml
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

import okg.substrate.cli.main as cli
from okg.substrate.catalog.invariants import load_invariants
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.deployment_scaffold import (
    build_deployment_scaffold_plan,
    create_deployment_scaffold,
    safe_deployment_slug,
)
from okg.substrate.deployment_lint import lint_deployment
from okg.substrate.deployment_wiring import resolve_deployment_wiring_plan
from okg.substrate.deployments import DeploymentManifest
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import IngestOptions, run_ingest
from okg.substrate.ingest.pii_posture import resolve_deployment_source_pii_posture
from okg.substrate.nomos import audit_deployment_policy
from okg.substrate.nomos.core import DecisionInput, compile_policy_graph, evaluate
from okg.substrate.nomos.store import deployment_baseline_policy_graph
from okg.substrate.sources.registry import load_source_registry
from okg.substrate.views.registry import load_views_registry


def test_scaffold_writes_loader_valid_foundation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The scaffold writes `postgres.dsn: ${OKG_DSN}`, which resolves to None
    # only when OKG_DSN is unset. The self-hosted CI runners export OKG_DSN in
    # the ambient environment (and a sibling test sets it via monkeypatch), so
    # clear it here to assert the unset-default deterministically.
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_CI_DSN", raising=False)
    root = tmp_path / "deployments"

    result = create_deployment_scaffold(
        "Physics Lab OKG",
        deployments_root=root,
    )

    dep = root / "physics-lab-okg"
    assert result.deployment_dir == dep
    assert result.deployment_name == "physics-lab-okg"
    assert dep.is_dir()

    expected_files = {
        "deployment.yaml",
        "source_registry.yaml",
        "invariants.yaml",
        "views_registry.yaml",
        ".env.example",
        "README.md",
        "fixtures/bootstrap_fixture.json",
        "schemas/.gitkeep",
        "schemas/bridges/.gitkeep",
        "workflows/.gitkeep",
        "enrichers/.gitkeep",
        "views/.gitkeep",
    }
    assert {
        path.relative_to(dep).as_posix()
        for path in result.files_written
    } == expected_files
    assert (dep / "schemas").is_dir()
    assert (dep / "schemas" / "bridges").is_dir()
    assert (dep / "fixtures").is_dir()
    assert (dep / "workflows").is_dir()
    assert (dep / "enrichers").is_dir()
    assert (dep / "views").is_dir()

    manifest = DeploymentManifest.from_yaml(dep / "deployment.yaml")
    assert manifest.name == "physics-lab-okg"
    assert manifest.schema_dir == dep / "schemas"
    assert manifest.source_registry == dep / "source_registry.yaml"
    assert manifest.modules == ("document_starter",)
    assert manifest.postgres_dsn is None
    assert (
        yaml.safe_load((dep / "deployment.yaml").read_text())["postgres"]["dsn"]
        == "${OKG_DSN}"
    )
    registry = load_source_registry(dep / "source_registry.yaml")
    assert sorted(registry) == ["bootstrap-fixture"]
    assert registry["bootstrap-fixture"].ownership_id == (
        "physics-lab-okg.bootstrap_fixture"
    )
    normal_lint = lint_deployment(dep, env={})
    source_probe_findings = [
        finding for finding in normal_lint["findings"]
        if finding["code"].startswith("deployment.source_registry.probe")
        or finding["code"] == "deployment.source_registry.poll_probe_unsound"
    ]
    assert source_probe_findings == []
    assert load_invariants(dep / "invariants.yaml") == []
    assert load_views_registry(dep) == []
    catalog = compose_catalog(dep)
    assert catalog.deployment_name == "physics-lab-okg"
    assert catalog.modules == ["document_starter"]
    assert "document" in catalog.subtypes

    for rel in (
        "deployment.yaml",
        "source_registry.yaml",
        "invariants.yaml",
        "views_registry.yaml",
    ):
        assert yaml.safe_load((dep / rel).read_text(encoding="utf-8")) is not None


def test_scaffold_fixture_represents_its_document_section(tmp_path: Path) -> None:
    from okg.substrate.library.sources.bootstrap_fixture import BootstrapFixtureSource
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    dep = create_deployment_scaffold("demo", deployments_root=tmp_path).deployment_dir
    fixture = dep / "fixtures" / "bootstrap_fixture.json"
    source = BootstrapFixtureSource(fixture_path=fixture)
    facts = list(source.run("first", mode="scope_complete").facts)
    nodes = [fact for fact in facts if isinstance(fact, NodeFact)]
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]
    assert [(node.node_id, node.subtype) for node in nodes] == [
        ("demo:doc:1", "document"), ("demo:doc:1:section:1", "section"),
    ]
    document, section = nodes
    assert document.attrs["document_id"] == "doc:1"
    assert section.attrs["section_id"] == "doc:1:section:1"
    assert document.attrs["body"] == (
        f"# {section.attrs['heading']}\n\n{section.attrs['body']}"
    )
    assert len(edges) == 1
    edge = edges[0]
    assert (edge.src, edge.edge_type, edge.dst) == (
        document.node_id, "contains", section.node_id,
    )
    assert [fact.source_record_id for fact in facts] == [
        {"id": "doc:1"}, {"id": "doc:1:section:1"},
        {"id": "doc:1:contains:section:1"},
    ]
    assert all(fact.source_revision == {"v": 1} for fact in facts)
    assert list(source.run("repeat", mode="scope_complete").facts) == facts
    assert "section" in compose_catalog(dep).subtypes


def test_scaffold_json_payload_has_next_commands(tmp_path: Path) -> None:
    result = create_deployment_scaffold("demo", deployments_root=tmp_path / "d")

    payload = result.json_payload()
    json.dumps(payload)

    assert payload["ok"] is True
    assert payload["deployment"]["name"] == "demo"
    assert payload["deployment"]["dir"] == str(tmp_path / "d" / "demo")
    assert payload["clobbered"] is False
    deployment_ref = str(result.deployment_dir)
    assert payload["next_commands"] == [
        f"uv run okg deployment env {deployment_ref} --json",
        f"uv run okg catalog load --deployment {deployment_ref} --json",
        f"uv run okg ingest --deployment {deployment_ref} --json",
        f"uv run okg deployment ready {deployment_ref} --checks general --json",
    ]


def test_scaffold_fixture_policy_passes_real_runtime_validation(tmp_path: Path) -> None:
    dep = create_deployment_scaffold("policy", deployments_root=tmp_path).deployment_dir
    manifest = yaml.safe_load((dep / "deployment.yaml").read_text())
    registry = yaml.safe_load((dep / "source_registry.yaml").read_text())
    audit = audit_deployment_policy(manifest, source_registry=registry)
    assert audit["ok"] is True, audit
    assert audit["source_policy_count"] == 1
    assert "source_policy_defaults" not in manifest["nomos"]
    assert "runtime_enforcement" not in manifest["nomos"]
    source = registry["sources"]["bootstrap-fixture"]
    posture = resolve_deployment_source_pii_posture(
        manifest=manifest, source_name="bootstrap-fixture", source_entry=source,
        env={}, build_detector=False,
    )
    assert posture.store_raw is True
    assert source["source_policy"]["live_call_allowed"] is False
    assert source["source_policy"]["exportability"] == "no_export"

    # A newly added real source cannot inherit the fixture's raw-storage grant.
    unreviewed = {key: value for key, value in source.items() if key != "source_policy"}
    with pytest.raises(SubstrateError, match="failed Nomos audit") as refused:
        resolve_deployment_source_pii_posture(
            manifest=manifest, source_name="unreviewed", source_entry=unreviewed,
            env={}, build_detector=False,
        )
    assert any(
        finding["code"] == "nomos.audit.source_policy_missing"
        for finding in refused.value.data["findings"]
    )

    source["source_policy"].pop("store_raw")
    with pytest.raises(SubstrateError, match="must explicitly declare store_raw"):
        resolve_deployment_source_pii_posture(
            manifest=manifest, source_name="bootstrap-fixture", source_entry=source,
            env={}, build_detector=False,
        )


def test_scaffold_baseline_authorizes_only_required_publisher_writes(tmp_path: Path) -> None:
    dep = create_deployment_scaffold("policy", deployments_root=tmp_path).deployment_dir
    manifest = yaml.safe_load((dep / "deployment.yaml").read_text())
    registry = yaml.safe_load((dep / "source_registry.yaml").read_text())
    source = registry["sources"]["bootstrap-fixture"]
    source_id = source["ownership_id"]
    bundle = compile_policy_graph(deployment_baseline_policy_graph(
        manifest, deployment_name="policy", scope="deployment:policy",
        activated_by="subject:bootstrap-activator",
        source_names=["bootstrap-fixture", source_id],
        runtime_source_names=[source_id],
        source_policies={source_id: source["source_policy"]},
    ))

    def decide(subject: str, action: str, *, deployment="policy", scope="deployment:policy", resource_kind=None):
        resource = {"deployment": deployment, "scope": scope, "source_ids": [source_id]}
        if resource_kind is not None:
            resource["resource_kind"] = resource_kind
        return evaluate(bundle, DecisionInput(
            subject=subject, action=action, resource=resource,
        ))

    for subject in ("okg_service:publisher", "subject:okg_service"):
        decision = decide(subject, "generation.publish")
        assert decision.effect == "allow"
        assert decision.matched_rule_ids == ("rollout_allow_manifest_publisher",)
        assert decide(subject, "generation.publish", deployment="other").effect == "deny"
        assert decide(subject, "generation.publish", scope="deployment:other").effect == "deny"
        for action in ("mcp.record", "source.ingest", "source.emit", "source.live_call",
                       "graph.read.export_like", "policy.activate", "config.activate"):
            assert decide(subject, action).effect == "deny", (subject, action)
    for subject in ("subject:stranger", "subject:human_operator", "subject:source_adapter"):
        assert decide(subject, "generation.publish").effect == "deny"
        assert decide(subject, "mcp.record").effect == "deny"
    for action in ("source.ingest", "source.emit"):
        assert decide(f"source:{source_id}", action).effect == "allow"
        assert decide("source:policy.unregistered", action).effect == "deny"
    assert decide("subject:okg_service", "graph.export",
                  resource_kind="graph_export_boundary").effect == "deny"
    assert decide("subject:mcp_client", "mcp.tool.call",
                  resource_kind="mcp_tool").effect == "deny"


def test_cli_reports_invalid_deployments_root_as_json(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path / "missing"))

    rc = cli.main(["deployment", "new", "demo", "--json"])

    assert rc == 1
    captured = capsys.readouterr()
    payload = json.loads(captured.err)
    assert payload["error"]["message"]
    assert "traceback" not in captured.err.lower()


def test_agent_mode_requires_apply_for_deployment_new(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    root = tmp_path / "deployments"
    monkeypatch.setenv("OKG_AGENT", "1")

    rc = cli.main([
        "deployment",
        "new",
        "agent-demo",
        "--deployments-root",
        str(root),
    ])

    assert rc == 1
    assert not (root / "agent-demo").exists()
    assert "requires --apply" in capsys.readouterr().err

    rc = cli.main([
        "deployment",
        "new",
        "agent-demo",
        "--deployments-root",
        str(root),
        "--apply",
    ])

    assert rc == 0
    assert (root / "agent-demo" / "deployment.yaml").is_file()


@pytest.mark.parametrize(
    "unsafe",
    [
        "",
        "../escape",
        "nested/name",
        "nested\\name",
        ".hidden",
        "okg..workspace",
        "/absolute",
        "bad:name",
    ],
)
def test_scaffold_rejects_path_like_or_unsafe_names(
    tmp_path: Path,
    unsafe: str,
) -> None:
    with pytest.raises(SubstrateError, match="invalid deployment name"):
        create_deployment_scaffold(unsafe, deployments_root=tmp_path)


def test_scaffold_slugifies_human_name_and_defaults_under_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "deployments"

    assert safe_deployment_slug("ACME Knowledge Commons") == (
        "acme-knowledge-commons"
    )
    plan = build_deployment_scaffold_plan(
        "ACME Knowledge Commons",
        deployments_root=root,
    )

    assert plan.deployments_root == root.resolve()
    assert plan.deployment_name == "acme-knowledge-commons"
    assert plan.deployment_dir == root.resolve() / "acme-knowledge-commons"


def test_scaffold_refuses_non_empty_target_without_force_yes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "deployments"
    dep = root / "demo"
    dep.mkdir(parents=True)
    (dep / "keep.txt").write_text("do not delete\n", encoding="utf-8")

    with pytest.raises(SubstrateError, match="already exists"):
        create_deployment_scaffold("demo", deployments_root=root)

    assert (dep / "keep.txt").read_text(encoding="utf-8") == "do not delete\n"

    with pytest.raises(SubstrateError, match="--force requires --yes"):
        create_deployment_scaffold("demo", deployments_root=root, force=True)

    assert (dep / "keep.txt").is_file()


def test_scaffold_force_yes_replaces_existing_non_empty_target(
    tmp_path: Path,
) -> None:
    root = tmp_path / "deployments"
    dep = root / "demo"
    dep.mkdir(parents=True)
    (dep / "stale.txt").write_text("old\n", encoding="utf-8")

    result = create_deployment_scaffold(
        "demo",
        deployments_root=root,
        force=True,
        yes=True,
    )

    assert result.clobbered is True
    assert not (dep / "stale.txt").exists()
    assert (dep / "deployment.yaml").is_file()


def test_scaffold_does_not_write_real_env_or_secrets(tmp_path: Path) -> None:
    dep = create_deployment_scaffold(
        "secrets check",
        deployments_root=tmp_path / "deployments",
    ).deployment_dir

    assert not (dep / ".env").exists()
    assert not (dep / ".okg").exists()
    assert not (dep / ".okg" / "local.env").exists()

    deployment_yaml = (dep / "deployment.yaml").read_text(encoding="utf-8")
    env_example = (dep / ".env.example").read_text(encoding="utf-8")
    readme = (dep / "README.md").read_text(encoding="utf-8")

    assert "${OKG_DSN}" in deployment_yaml
    assert "PASSWORD" in env_example
    assert "Do not commit a real .env file" in env_example
    assert "does not create `.env` or `.okg/local.env`" in readme
    deployment_ref = str(dep)
    assert f"uv run okg deployment env {deployment_ref} --json" in readme
    assert (
        f"uv run okg catalog load --deployment {deployment_ref} --json"
        in readme
    )
    assert f"uv run okg ingest --deployment {deployment_ref} --json" in readme
    assert (
        f"uv run okg deployment ready {deployment_ref} --checks general --json"
        in readme
    )


@pytest.fixture
def scaffold_private_dsn(okg_dsn: str):
    """Own one fresh database; never reset or apply a catalog to the shared DB."""
    options = conninfo_to_dict(okg_dsn)
    options.pop("dbname", None)

    def target(name: str) -> str:
        uri = "postgresql:///" + quote(name, safe="")
        if options:
            uri += "?" + urlencode(options, quote_via=quote)
        assert conninfo_to_dict(uri) == dict(options, dbname=name)
        return uri

    name = "okg_test_scaffold_" + uuid.uuid4().hex
    admin, dsn = target("postgres"), target(name)
    with psycopg.connect(admin, autocommit=True, connect_timeout=15) as conn:
        assert conn.execute("SELECT current_database()").fetchone() == ("postgres",)
        assert conn.execute(
            "SELECT oid FROM pg_database WHERE datname = %s", (name,),
        ).fetchone() is None
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        database_oid = conn.execute(
            "SELECT oid FROM pg_database WHERE datname = %s", (name,),
        ).fetchone()[0]

    from okg.substrate.db import migrate as migration

    original = dict(migration.REQUIRED_EXTENSIONS)
    # pg_cron belongs to the server's configured database, as in hermetic_dsn.
    # No runtime or Nomos enforcement settings are replaced for this test.
    migration.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            assert conn.execute(
                "SELECT current_database(), (SELECT oid FROM pg_database "
                "WHERE datname = current_database())",
            ).fetchone() == (name, database_oid)
        migration.ensure_required_extensions(dsn)
        migration.migrate(dsn)
        yield dsn
    finally:
        from okg.substrate.db.pool import close_all

        close_all()
        migration.REQUIRED_EXTENSIONS.clear()
        migration.REQUIRED_EXTENSIONS.update(original)
        with psycopg.connect(admin, autocommit=True) as conn:
            assert conn.execute("SELECT current_database()").fetchone() == ("postgres",)
            assert conn.execute(
                "SELECT oid FROM pg_database WHERE datname = %s", (name,),
            ).fetchone() == (database_oid,)
            conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
            assert conn.execute(
                "SELECT oid FROM pg_database WHERE datname = %s", (name,),
            ).fetchone() is None


@pytest.mark.postgres
def test_generated_scaffold_can_publish_fixture_source(
    tmp_path: Path,
    scaffold_private_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    record_property,
) -> None:
    clean_runtime = scaffold_private_dsn
    root = tmp_path / "deployments"
    result = create_deployment_scaffold("test", deployments_root=root)
    dep = result.deployment_dir

    deployment_yaml = yaml.safe_load(
        (dep / "deployment.yaml").read_text(encoding="utf-8"),
    )
    # Only test environment authority is supplied. Keep generated policy intact
    # and commit all inputs before catalog ownership is claimed. This is a
    # checkout-backed proof, not the separate clean-wheel installation journey.
    deployment_yaml["release"] = {
        "channel": "dev",
        "pin": {"code_repository": str(Path(__file__).resolve().parents[3])},
    }
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(deployment_yaml, sort_keys=False),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    for arguments in (
        ["init"], ["add", "."],
        ["-c", "user.name=Scaffold test", "-c", "user.email=scaffold@example.invalid",
         "commit", "-m", "Commit generated scaffold test inputs"],
    ):
        subprocess.run(["git", "-C", str(dep), *arguments], check=True, capture_output=True)

    assert cli.main([
        "catalog",
        "load",
        "--deployment",
        str(dep),
        "--json",
    ]) == 0
    catalog_payload = json.loads(capsys.readouterr().out)
    assert catalog_payload["deployment"] == "test"
    assert catalog_payload["applied"] is False
    assert catalog_payload["modules"] == ["document_starter"]
    assert catalog_payload["subtype_count"] >= 3
    assert cli.main([
        "catalog", "load", "--deployment", str(dep), "--apply", "--json",
    ]) == 0
    applied = json.loads(capsys.readouterr().out)
    catalog_version_id = applied["catalog_version_id"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status
                  FROM okg.catalog_versions
                 WHERE catalog_version_id = %s
                """,
                (catalog_version_id,),
            )
            assert cur.fetchone()[0] == "applied"
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.subtypes
                 WHERE catalog_version_id = %s
                   AND name = 'document'
                """,
                (catalog_version_id,),
            )
            assert cur.fetchone()[0] == 1
    assert catalog_version_id > 0

    plan = resolve_deployment_wiring_plan(dep)
    assert plan.ok is True
    assert [component.component_id for component in plan.modules] == [
        "document_starter",
    ]
    sources_by_id = {
        component.component_id: component
        for component in plan.sources
    }
    source_component = sources_by_id["bootstrap-fixture"]
    assert source_component.custom_code is False
    assert source_component.attrs["module"] == (
        "okg.substrate.library.sources.bootstrap_fixture"
    )
    assert source_component.attrs["class"] == "BootstrapFixtureSource"
    assert source_component.attrs["ownership_id"] == "test.bootstrap_fixture"

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="test",
            deployment_dir=dep,
            owner_id="deployment-new-smoke",
            owner_meta={"pact": "issue-252-deployment-scaffold"},
        ),
    )

    assert outcome.any_failed is False, outcome.report
    assert outcome.report["ordered"] == ["bootstrap-fixture"]
    publishes = outcome.report["publishes"]
    assert len(publishes) == 1
    publish = publishes[0]
    assert publish["status"] == "published", publish
    assert publish["nodes_added"] == 2
    assert publish["edges_added"] == 1
    assert str(publish["generation_id"]).startswith("okg:test:")
    assert publish["consistency_gates"][0]["result"] == "evaluated"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkpoint = store.get_generation(publish["generation_id"])
        graph = store.checkout_generation(publish["generation_id"])
        node_rows = graph.query(
            """
            SELECT node_id, subtype
              FROM okg.graph_nodes
             ORDER BY node_id
            """,
        )
        edge_rows = graph.query(
            "SELECT src, edge_type, dst FROM okg.graph_edges"
        )
        with conn.cursor() as cur:
            from okg.substrate.runtime_queue_health import (
                summarize_runtime_queue_health,
            )

            runtime_queue = summarize_runtime_queue_health(cur)
            cur.execute(
                """
                SELECT status, producer_id, target_generation_ref
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND producer_id = 'test.bootstrap_fixture'
                 ORDER BY created_at DESC
                 LIMIT 1
                """,
            )
            admission_status, producer_id, target_generation_ref = cur.fetchone()

    assert checkpoint.metadata["status"] == "published"
    assert node_rows == [
        {"node_id": "test:doc:1", "subtype": "document"},
        {"node_id": "test:doc:1:section:1", "subtype": "section"},
    ]
    assert edge_rows == [{
        "src": "test:doc:1", "edge_type": "contains", "dst": "test:doc:1:section:1",
    }]
    assert publish["node_count"] == 2
    assert publish["edge_count"] == 1
    assert runtime_queue["active_queue_depth"] == 0
    assert runtime_queue["dlq_count"] == 0
    assert admission_status == "published"
    assert producer_id == "test.bootstrap_fixture"
    assert target_generation_ref == publish["generation_id"]

    repeated = run_ingest(IngestOptions(
        dsn=clean_runtime, deployment="test", deployment_dir=dep,
        owner_id="deployment-new-smoke",
    ))
    assert repeated.any_failed is False, repeated.report
    assert repeated.report["results"][0]["nodes_appended"] == 0
    assert repeated.report["results"][0]["edges_appended"] == 0
    assert repeated.report["publishes"] == [{
        "label": "commit", "status": "skipped", "publish_skipped": True,
        "reason": "no_source_work",
    }]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(publish["generation_id"])
        assert graph.query("SELECT node_id, subtype FROM okg.graph_nodes ORDER BY node_id") == node_rows
        assert graph.query("SELECT src, edge_type, dst FROM okg.graph_edges") == edge_rows
    record_property("published_generation", publish["generation_id"])
    record_property("node_count", 2)
    record_property("edge_count", 1)
    record_property("repeat_publish", "skipped:no_source_work")
    # An inline scaffold does not start DBOS. Preserve unavailable health in the
    # receipt instead of treating the zero queue counters as a healthy runtime.
    record_property("runtime_queue_status", runtime_queue["status"])

    readiness_exit = cli.main([
        "deployment",
        "ready",
        str(dep),
        "--checks",
        "general",
        "--json",
    ])
    ready_payload = json.loads(capsys.readouterr().out)
    # Publication is proven above. The scaffold does not bootstrap DBOS or
    # activate Nomos enforcement, so full readiness must remain visibly blocked.
    assert deployment_yaml["runtime"]["enabled"] is False
    assert readiness_exit == 1, ready_payload
    assert ready_payload["ok"] is False
    assert ready_payload["status"] == "unavailable"
    blockers = {item["id"]: item for item in ready_payload["blockers"]}
    assert set(blockers) == {"queues.unavailable", "nomos.source_lineage"}
    assert blockers["queues.unavailable"]["details"]["reason"] == "dbos.workflow_status missing"
    lineage = blockers["nomos.source_lineage"]["details"]
    assert lineage["runtime_enforcement_mode"] == "off"
    assert [error["code"] for error in lineage["errors"]] == [
        "nomos_source_lineage_runtime_bypassed",
    ]
    assert lineage["missing_lineage"]["node_count"] == 0
    assert lineage["missing_lineage"]["edge_count"] == 0
    assert ready_payload["generation"]["count_integrity"]["ok"] is True
    record_property("readiness_blockers", sorted(blockers))
    assert ready_payload["deployment"] == str(dep)
    assert ready_payload["deployment_name"] == "test"
    assert ready_payload["deployment_ref"] == str(dep)
    assert ready_payload["generation"]["generation_id"] == publish["generation_id"]
    assert ready_payload["generation"]["status"] == "published"
    assert ready_payload["generation"]["node_count"] == 2
    assert ready_payload["generation"]["edge_count"] == 1
    assert ready_payload["queues"]["active_queue_depth"] == 0
    assert ready_payload["queues"]["dlq_count"] == 0
