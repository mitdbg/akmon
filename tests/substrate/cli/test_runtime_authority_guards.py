from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import okg.substrate.cli.main as cli
from okg.substrate.cli import admission as admission_cli
from okg.substrate.cli import jobs as jobs_cli
from okg.substrate.errors import SealedReleaseInvalid, SubstrateError


def _json_error(capsys: pytest.CaptureFixture[str]) -> dict:
    captured = capsys.readouterr()
    raw = (captured.err or captured.out).strip().splitlines()[-1]
    return json.loads(raw)["error"]


def _nonarchived_sealed_descriptor(tmp_path, monkeypatch, deployment):
    """Provide real early metadata while each test retains its resolver seam."""
    definition = tmp_path / "definition.yaml"
    definition.write_text(f"name: {deployment}\n")
    descriptor = tmp_path / "environment.yaml"
    descriptor.write_text(
        f"id: {deployment}\ngraph_deployment_name: {deployment}\n"
        "definition:\n  path: definition.yaml\n"
    )
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", str(descriptor))
    monkeypatch.setenv("OKG_RELEASE_ROOT", str(tmp_path))


def test_launcher_worker_authority_failure_precedes_dbos(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.deployment_authority as checkout_authority
    import okg.substrate.runtime.release_authority as release_authority
    from okg.substrate.runtime.config import RuntimeConfig
    from okg.substrate.runtime.worker import run_worker
    from okg.substrate.embeddings import EmbeddingConfig

    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            embedding_config=EmbeddingConfig(enabled=False)
        ),
    )

    def fail_release(**_kwargs):
        raise SealedReleaseInvalid("foreign package root")

    monkeypatch.setattr(
        release_authority,
        "verify_worker_release_authority",
        fail_release,
    )
    monkeypatch.setattr(
        checkout_authority,
        "check_publish_provenance",
        lambda *_args: pytest.fail("sealed failure fell back to checkout authority"),
    )

    assert (
        run_worker(
            deployment="fixture-graph",
            dsn="postgresql://example/fixture",
            runtime=RuntimeConfig(enabled=True),
        )
        == 2
    )


def test_launcher_partial_authority_never_falls_back_to_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.deployment_authority as checkout_authority
    from okg.substrate.runtime.config import RuntimeConfig
    from okg.substrate.runtime.worker import run_worker
    from okg.substrate.embeddings import EmbeddingConfig

    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            embedding_config=EmbeddingConfig(enabled=False)
        ),
    )

    monkeypatch.setenv("OKG_RELEASE_ROOT", "/partial/release")
    monkeypatch.setattr(
        checkout_authority,
        "check_publish_provenance",
        lambda *_args: pytest.fail("partial sealed launch used checkout authority"),
    )

    assert (
        run_worker(
            deployment="fixture-graph",
            dsn="postgresql://example/fixture",
            runtime=RuntimeConfig(enabled=True),
        )
        == 2
    )


def test_run_dbos_enqueue_failure_requires_explicit_inline(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "_resolve_run_context",
        lambda _args: ("postgres://test", "demo", None, True),
    )
    # Issue #2128 answers database readiness before the enqueue, and
    # nothing listens behind `postgres://test`. This case is about the
    # ENQUEUE failure message, so the probe is answered with a migrated
    # database; the gate itself is pinned in
    # `tests/substrate/cli/test_run_dbos_outcome_reporting.py` and
    # against a real server in `tests/substrate/test_migrate.py`.
    from okg.substrate.db import readiness as db_readiness

    monkeypatch.setattr(
        db_readiness,
        "require_database_ready",
        lambda dsn, **kwargs: db_readiness.DatabaseReadiness(
            schema_present=True, schema_applied=True, ledger_present=True
        ),
    )

    def fail_enqueue(**_kwargs):
        raise RuntimeError("dbos offline")

    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_enqueue_publisher_cycle",
        fail_enqueue,
    )

    rc = cli._cmd_run(
        SimpleNamespace(once=True, inline=False, apply=True),
        as_json=True,
    )

    assert rc == 1
    error = _json_error(capsys)
    assert "DBOS-path publisher enqueue failed" in error["message"]
    assert "--inline" in error["message"]
    assert "OKG job" not in error["message"]


def test_runtime_catalog_uses_active_environment_manifest(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = SimpleNamespace(deployment_dir=tmp_path)
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", "/sealed/environment.yaml")
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda deployment: manifest,
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.compose_catalog",
        lambda target: target,
    )

    assert cli._compose_runtime_catalog(
        deployment="fixture-staging",
        deployment_dir=tmp_path,
    ) is manifest


def test_sealed_dsn_authority_cannot_be_overridden_by_caller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", "/sealed/environment.yaml")
    monkeypatch.setenv("OKG_DEPLOYMENT", "fixture-staging")
    monkeypatch.setenv("OKG_DSN", "postgres://sealed-authority")
    args = SimpleNamespace(
        deployment="fixture-staging",
        dsn="postgres://caller-override",
    )

    assert cli._resolve_dsn(args) == "postgres://sealed-authority"


def test_sealed_dsn_authority_refuses_foreign_deployment_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", "/sealed/environment.yaml")
    monkeypatch.setenv("OKG_DEPLOYMENT", "fixture-staging")
    monkeypatch.setenv("OKG_DSN", "postgres://sealed-authority")

    with pytest.raises(SubstrateError, match="outside its active environment"):
        cli._resolve_dsn(
            SimpleNamespace(
                deployment="foreign-deployment",
                dsn="postgres://caller-override",
            )
        )


def test_sealed_dsn_authority_refuses_missing_launcher_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", "/sealed/environment.yaml")
    monkeypatch.setenv("OKG_DEPLOYMENT", "fixture-staging")
    monkeypatch.delenv("OKG_DSN", raising=False)

    with pytest.raises(SubstrateError, match="authoritative OKG_DSN"):
        cli._resolve_dsn(SimpleNamespace(deployment="fixture-staging", dsn=None))


def test_sealed_run_context_cannot_be_redirected_by_caller_dsn(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = SimpleNamespace(
        name="fixture-staging",
        postgres_dsn="postgres://manifest-fallback",
        deployment_dir=tmp_path,
        runtime=SimpleNamespace(enabled=True),
    )
    _nonarchived_sealed_descriptor(tmp_path, monkeypatch, "fixture-staging")
    monkeypatch.setenv("OKG_DEPLOYMENT", "fixture-staging")
    monkeypatch.setenv("OKG_DSN", "postgres://sealed-authority")
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda deployment: manifest,
    )

    dsn, deployment, deployment_dir, runtime_enabled = cli._resolve_run_context(
        SimpleNamespace(
            deployment="fixture-staging",
            dsn="postgres://caller-override",
        )
    )

    assert dsn == "postgres://sealed-authority"
    assert deployment == "fixture-staging"
    assert deployment_dir == tmp_path
    assert runtime_enabled is True


def test_sealed_run_context_refuses_missing_launcher_dsn(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = SimpleNamespace(
        name="fixture-staging",
        postgres_dsn="postgres://manifest-fallback",
        deployment_dir=tmp_path,
        runtime=SimpleNamespace(enabled=True),
    )
    _nonarchived_sealed_descriptor(tmp_path, monkeypatch, "fixture-staging")
    monkeypatch.setenv("OKG_DEPLOYMENT", "fixture-staging")
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda deployment: manifest,
    )

    with pytest.raises(SubstrateError, match="authoritative OKG_DSN"):
        cli._resolve_run_context(
            SimpleNamespace(
                deployment="fixture-staging",
                dsn="postgres://caller-override",
            )
        )


def test_ingest_dbos_enqueue_failure_does_not_call_inline_runner(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source_entry = SimpleNamespace(admission_policy=None)

    monkeypatch.setattr(cli, "_resolve_dsn", lambda _args: "postgres://test")
    monkeypatch.setattr(
        cli,
        "_deployment_runtime_enabled",
        lambda _deployment: True,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _deployment: tmp_path,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.inject_deployment_path",
        lambda _path: None,
    )
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda _path, **_kwargs: {"docs": source_entry},
    )
    monkeypatch.setattr(
        "okg.substrate.ingest.planning.plan_ingest_sources",
        lambda *_args, **_kwargs: SimpleNamespace(ordered=["docs"]),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.check_publish_provenance",
        lambda *_args, **_kwargs: {"allowed": True},
    )

    def fail_inline(*_args, **_kwargs):
        pytest.fail("run_ingest was called after DBOS enqueue failure")

    def fail_enqueue(**_kwargs):
        raise RuntimeError("dbos offline")

    monkeypatch.setattr("okg.substrate.ingest.service.run_ingest", fail_inline)
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_enqueue_source_sync",
        fail_enqueue,
    )

    rc = cli._cmd_ingest(
        SimpleNamespace(
            deployment="demo",
            progress=False,
            inline=False,
            reset_cursor=False,
            include=None,
            exclude=None,
            mode="scope_complete",
            no_predecessors=False,
            include_dependents=False,
            dependent_fanout=None,
            no_publish=False,
            apply=True,
            profile=None,
            profile_collector=(),
            enqueue=False,
        ),
        as_json=True,
    )

    assert rc == 1
    error = _json_error(capsys)
    assert "DBOS-path source-sync enqueue failed" in error["message"]
    assert "--inline" in error["message"]


def test_ingest_path_dbos_enqueue_uses_manifest_name(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Path references stay name-scoped when runtime routes through DBOS."""
    import okg.substrate.runtime.admin as runtime_admin

    target = tmp_path / "deployment-dir"
    target.mkdir()
    manifest_path = target / "deployment.yaml"
    manifest_path.write_text("name: demo\n", encoding="utf-8")
    registry_path = target / "source_registry.yaml"
    source_entry = SimpleNamespace(admission_policy=None)
    captured: dict[str, object] = {}

    monkeypatch.setattr(cli, "_resolve_dsn", lambda _args: "postgres://test")

    def capture_runtime_deployment(deployment):
        captured["runtime_deployment"] = deployment
        return True

    monkeypatch.setattr(
        cli, "_deployment_runtime_enabled", capture_runtime_deployment,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _deployment: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.inject_deployment_path",
        lambda _path: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda deployment: SimpleNamespace(
            name="demo",
            deployment_dir=target,
            source_registry=registry_path,
            sync_root=tmp_path,
            # A real manifest always carries this; the ingest path reads it
            # directly (never via a getattr default) so an incomplete stub
            # cannot silently disable the sync_root refusal (issue #1014).
            sync_root_unresolved=(),
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda _path, **_kwargs: {"docs": source_entry},
    )
    monkeypatch.setattr(
        "okg.substrate.ingest.planning.plan_ingest_sources",
        lambda *_args, **_kwargs: SimpleNamespace(ordered=["docs"]),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.check_publish_provenance",
        lambda *_args, **_kwargs: {"allowed": True},
    )

    def fake_enqueue_source_sync(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            workflow_id="wf-path-demo",
            status="ENQUEUED",
            any_failed=False,
            elapsed_seconds=0.0,
            report=None,
            error=None,
        )

    monkeypatch.setattr(
        runtime_admin,
        "client_enqueue_source_sync",
        fake_enqueue_source_sync,
    )

    rc = cli._cmd_ingest(
        SimpleNamespace(
            deployment=str(manifest_path),
            progress=False,
            inline=False,
            reset_cursor=False,
            include=None,
            exclude=None,
            mode="scope_complete",
            no_predecessors=False,
            include_dependents=False,
            dependent_fanout=None,
            no_publish=False,
            profile=None,
            profile_collector=(),
            force_enrichers=False,
            enqueue=True,
        ),
        as_json=True,
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["workflow_id"] == "wf-path-demo"
    assert captured["runtime_deployment"] == "demo"
    assert captured["deployment"] == "demo"


def test_ingest_dbos_refusal_renders_typed_source_authority_envelope(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import okg.substrate.runtime.admin as runtime_admin

    registry_path = tmp_path / "source_registry.yaml"
    source_entry = SimpleNamespace(admission_policy=None)
    monkeypatch.setattr(cli, "_resolve_dsn", lambda _args: "postgres://test")
    monkeypatch.setattr(cli, "_deployment_runtime_enabled", lambda _name: True)
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _deployment: tmp_path,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.inject_deployment_path",
        lambda _path: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            name="demo",
            deployment_dir=tmp_path,
            source_registry=registry_path,
            sync_root=tmp_path,
            sync_root_unresolved=(),
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda _path, **_kwargs: {"docs": source_entry},
    )
    monkeypatch.setattr(
        "okg.substrate.ingest.planning.plan_ingest_sources",
        lambda *_args, **_kwargs: SimpleNamespace(ordered=["docs"]),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.check_publish_provenance",
        lambda *_args, **_kwargs: {"allowed": True},
    )
    monkeypatch.setattr(
        runtime_admin,
        "client_enqueue_source_sync",
        lambda **_kwargs: SimpleNamespace(
            workflow_id="wf-stale-root",
            status="ERROR",
            any_failed=True,
            elapsed_seconds=0.1,
            report=None,
            error="configured source root disappeared",
            error_code="source_configuration_authority_invalid",
            error_data={
                "configured_path": "/sealed/source-root",
                "reason": "FileNotFoundError: missing",
            },
            error_fix_hint="repair the sealed environment descriptor",
        ),
    )

    rc = cli._cmd_ingest(
        SimpleNamespace(
            deployment="demo",
            progress=False,
            inline=False,
            reset_cursor=False,
            include=None,
            exclude=None,
            mode="scope_complete",
            no_predecessors=False,
            include_dependents=False,
            dependent_fanout=None,
            no_publish=False,
            profile=None,
            profile_collector=(),
            force_enrichers=False,
            enqueue=False,
        ),
        as_json=True,
    )

    assert rc == 1
    error = _json_error(capsys)
    assert error["code"] == "source_configuration_authority_invalid"
    assert error["data"]["configured_path"] == "/sealed/source-root"
    assert error["data"]["workflow_id"] == "wf-stale-root"
    assert error["fix_hint"] == "repair the sealed environment descriptor"


def test_inline_ingest_uses_environment_registered_sync_root(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_entry = SimpleNamespace(admission_policy=None)
    registry_path = tmp_path / "source_registry.yaml"
    canonical_sync_root = tmp_path / "registered-sync-root"
    canonical_sync_root.mkdir()
    sync_root = tmp_path / "registered-sync-root-alias"
    sync_root.symlink_to(canonical_sync_root, target_is_directory=True)
    captured: dict[str, object] = {}

    _nonarchived_sealed_descriptor(tmp_path, monkeypatch, "demo")
    monkeypatch.setattr(cli, "_resolve_dsn", lambda _args: "postgres://test")
    monkeypatch.setattr(cli, "_deployment_runtime_enabled", lambda _name: False)
    monkeypatch.setattr(
        cli,
        "_guard_inline_publish_against_active_source_sync",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _deployment: tmp_path,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.inject_deployment_path",
        lambda _path: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            name="demo",
            deployment_dir=tmp_path,
            source_registry=registry_path,
            sync_root=sync_root,
            sync_root_unresolved=(),
        ),
    )

    def fake_load_registry(path, **kwargs):
        captured["registry_path"] = path
        captured["repo_root_override"] = kwargs.get("repo_root_override")
        return {"docs": source_entry}

    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        fake_load_registry,
    )
    monkeypatch.setattr(
        "okg.substrate.ingest.planning.plan_ingest_sources",
        lambda *_args, **_kwargs: SimpleNamespace(ordered=["docs"]),
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.check_publish_provenance",
        lambda *_args, **_kwargs: {"allowed": True},
    )

    def fake_run_ingest(options):
        captured["options"] = options
        return SimpleNamespace(report={"ok": True}, any_failed=False)

    monkeypatch.setattr("okg.substrate.ingest.service.run_ingest", fake_run_ingest)

    rc = cli._cmd_ingest(
        SimpleNamespace(
            deployment="demo",
            progress=False,
            inline=True,
            reset_cursor=False,
            include=None,
            exclude=None,
            mode="scope_complete",
            no_predecessors=False,
            include_dependents=False,
            dependent_fanout=None,
            no_publish=False,
            profile=None,
            profile_collector=(),
            enqueue=False,
            owner_id=None,
            owner_meta=None,
        ),
        as_json=True,
    )

    assert rc == 0
    assert captured["registry_path"] == registry_path
    assert captured["repo_root_override"] == canonical_sync_root
    assert (
        captured["options"].source_registry_repo_root_override
        == canonical_sync_root
    )


def test_inline_ingest_refuses_missing_registered_sync_root_before_registry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    registry_path = tmp_path / "source_registry.yaml"
    missing_root = tmp_path / "foreign-host-root"
    _nonarchived_sealed_descriptor(tmp_path, monkeypatch, "demo")
    monkeypatch.setattr(cli, "_resolve_dsn", lambda _args: "postgres://test")
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _deployment: tmp_path,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.inject_deployment_path",
        lambda _path: None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            name="demo",
            deployment_dir=tmp_path,
            source_registry=registry_path,
            sync_root=missing_root,
            sync_root_unresolved=(),
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda *_args, **_kwargs: pytest.fail(
            "registry admitted before source-root authority",
        ),
    )

    rc = cli._cmd_ingest(
        SimpleNamespace(
            deployment="demo",
            progress=False,
            inline=True,
            reset_cursor=False,
            include=None,
            exclude=None,
            mode="scope_complete",
            no_predecessors=False,
            include_dependents=False,
            dependent_fanout=None,
            no_publish=False,
            profile=None,
            profile_collector=(),
            enqueue=False,
            owner_id=None,
            owner_meta=None,
        ),
        as_json=True,
    )

    assert rc == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["code"] == ("source_configuration_authority_invalid")
    assert payload["error"]["data"]["configured_path"] == str(missing_root)
    assert "--inline" not in payload["error"]["fix_hint"]


@pytest.mark.parametrize(
    "configured",
    ["~okg-user-that-does-not-exist/source", "/tmp/source\0root"],
)
def test_inline_ingest_refuses_exceptional_manifest_root_with_typed_envelope(
    configured: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.deployments.sync_root import resolve_declared_sync_root

    registry_path = tmp_path / "source_registry.yaml"
    (tmp_path / "deployment.yaml").write_text(
        "name: demo\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "_resolve_dsn", lambda _args: "postgres://test")
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda _deployment: tmp_path,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.inject_deployment_path",
        lambda _path: None,
    )

    def resolve_manifest(_deployment):
        resolution = resolve_declared_sync_root(
            {"sync_root": configured},
            deployment_dir=tmp_path,
        )
        return SimpleNamespace(
            name="demo",
            deployment_dir=tmp_path,
            source_registry=registry_path,
            sync_root=resolution.path,
            sync_root_unresolved=(),
        )

    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        resolve_manifest,
    )
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda *_args, **_kwargs: pytest.fail(
            "registry admitted before source-root authority",
        ),
    )

    rc = cli._cmd_ingest(
        SimpleNamespace(
            deployment="demo",
            progress=False,
            inline=True,
            reset_cursor=False,
            include=None,
            exclude=None,
            mode="scope_complete",
            no_predecessors=False,
            include_dependents=False,
            dependent_fanout=None,
            no_publish=False,
            profile=None,
            profile_collector=(),
            force_enrichers=False,
            enqueue=False,
            owner_id=None,
            owner_meta=None,
        ),
        as_json=True,
    )

    assert rc == 1
    error = _json_error(capsys)
    assert error["code"] == "source_configuration_authority_invalid"
    assert error["data"]["configured_path"]
    assert error["data"]["reason"].startswith(("RuntimeError:", "ValueError:"))


def test_jobs_enqueue_source_dbos_failure_has_repair_hint(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        jobs_cli,
        "resolve_read_dsn",
        lambda _args, *, surface: SimpleNamespace(
            dsn="postgres://test",
            deployment="demo",
            authority={},
            target={},
        ),
    )

    def fail_enqueue(**_kwargs):
        raise RuntimeError("dbos offline")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_source_sync_via_dbos",
        fail_enqueue,
    )

    rc = jobs_cli._cmd_enqueue_source(
        SimpleNamespace(
            runtime_deployment=None,
            source="docs",
            scope_key=None,
            run_mode="scope_complete",
            dependent_fanout="closure",
            run_key=None,
            no_publish=False,
            apply=True,
        ),
        as_json=True,
    )

    assert rc == 1
    error = _json_error(capsys)
    assert "DBOS-path source-sync enqueue failed" in error["message"]
    assert "dogfood status --deployment demo --json" in error["fix_hint"]
    assert "does not fall back" in error["fix_hint"]


def test_admission_submit_dbos_failure_has_repair_hint(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _runtime_enabled(monkeypatch, True)

    def fail_enqueue(**_kwargs):
        raise RuntimeError("dbos offline")

    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_enqueue_admission_submit",
        fail_enqueue,
    )

    rc = admission_cli._cmd_admission_submit(
        SimpleNamespace(
            deployment="demo",
            payload_json=None,
            payload_file=None,
            authority_json=None,
            check_summary_json=None,
            producer_id="agent-1",
            producer_kind="agent",
            intent="review_proposal",
            idempotency_key=None,
            max_wait_seconds=1,
            poll_interval_seconds=0.1,
        ),
        dsn="postgres://test",
        as_json=True,
    )

    assert rc == 1
    error = _json_error(capsys)
    assert "DBOS-path admission submit enqueue failed" in error["message"]
    assert "dogfood status --deployment demo --json" in error["fix_hint"]
    assert "does not fall back" in error["fix_hint"]


def test_admission_submit_rejects_invalid_intent_before_enqueue(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail_enqueue(**_kwargs):
        raise AssertionError("invalid intent reached DBOS enqueue")

    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_enqueue_admission_submit",
        fail_enqueue,
    )

    rc = admission_cli._cmd_admission_submit(
        SimpleNamespace(
            deployment="demo",
            payload_json='{"node":{"node_id":"n:1","subtype":"alpha"}}',
            payload_file=None,
            authority_json=None,
            check_summary_json=None,
            producer_id="agent-1",
            producer_kind="agent",
            intent="not an intent",
            idempotency_key=None,
            max_wait_seconds=1,
            poll_interval_seconds=0.1,
        ),
        dsn="postgres://test",
        as_json=True,
    )

    assert rc == 1
    error = _json_error(capsys)
    assert "intent must match" in error["message"]


def test_runtime_enabled_admission_decide_dbos_failure_has_repair_hint(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            runtime=SimpleNamespace(enabled=True),
        ),
    )

    def fail_enqueue(**_kwargs):
        raise RuntimeError("dbos offline")

    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_enqueue_admission_decide",
        fail_enqueue,
    )

    rc = admission_cli._cmd_admission_decide(
        SimpleNamespace(
            deployment="demo",
            action=None,
            admission_id="adm-1",
            decision="accept",
            actor="operator",
            reason=None,
            verdict_json=None,
            max_wait_seconds=1,
            poll_interval_seconds=0.1,
        ),
        dsn="postgres://test",
        as_json=True,
    )

    assert rc == 1
    error = _json_error(capsys)
    assert "DBOS-path admission decide enqueue failed" in error["message"]
    assert "dogfood status --deployment demo --json" in error["fix_hint"]
    assert "does not fall back" in error["fix_hint"]


def _runtime_enabled(monkeypatch: pytest.MonkeyPatch, enabled: bool) -> None:
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda _deployment: SimpleNamespace(
            runtime=SimpleNamespace(enabled=enabled),
        ),
    )


def _submit_args(**overrides) -> SimpleNamespace:
    values = dict(
        deployment="demo",
        payload_json='{"node":{"node_id":"n:1","subtype":"alpha"}}',
        payload_file=None,
        authority_json=None,
        check_summary_json=None,
        producer_id="agent-1",
        producer_kind="agent",
        intent="review_proposal",
        idempotency_key="key-1",
        max_wait_seconds=1,
        poll_interval_seconds=0.1,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def test_runtime_disabled_admission_submit_runs_inline_without_dbos(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _runtime_enabled(monkeypatch, False)
    calls: list[dict] = []

    def fake_submit(**kwargs):
        calls.append(kwargs)
        return {"admission_id": "adm-inline", "status": "pending"}

    def fail_enqueue(**_kwargs):
        raise AssertionError("runtime-disabled submit reached DBOS enqueue")

    monkeypatch.setattr(
        "okg.substrate.runtime.admission_workflows._run_submit_with_runtime_approver",
        fake_submit,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_enqueue_admission_submit",
        fail_enqueue,
    )

    rc = admission_cli._cmd_admission_submit(
        _submit_args(), dsn="postgres://inline", as_json=True,
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["workflow_status"] == "INLINE"
    assert out["workflow_id"] is None
    assert out["output"]["admission_id"] == "adm-inline"
    assert len(calls) == 1
    call = calls[0]
    # The queued path's body, with the caller's DSN and no runtime approver.
    assert call["dsn"] == "postgres://inline"
    assert call["approver_config"] is None
    assert call["deployment"] == "demo"
    assert call["payload"] == {"node": {"node_id": "n:1", "subtype": "alpha"}}
    assert call["intent"] == "review_proposal"
    assert call["idempotency_key"] == "key-1"


def test_runtime_enabled_admission_submit_still_enqueues_to_dbos(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _runtime_enabled(monkeypatch, True)
    enqueued: list[dict] = []

    def fake_enqueue(**kwargs):
        enqueued.append(kwargs)
        return SimpleNamespace(
            workflow_id="wf-1",
            status="SUCCESS",
            output={"admission_id": "adm-dbos"},
            error=None,
            elapsed_seconds=0.1,
        )

    def fail_submit(**_kwargs):
        raise AssertionError("runtime-enabled submit ran inline")

    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_enqueue_admission_submit",
        fake_enqueue,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admission_workflows._run_submit_with_runtime_approver",
        fail_submit,
    )

    rc = admission_cli._cmd_admission_submit(
        _submit_args(), dsn="postgres://dbos", as_json=True,
    )

    assert rc == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["workflow_status"] == "SUCCESS"
    assert out["workflow_id"] == "wf-1"
    assert [call["dsn"] for call in enqueued] == ["postgres://dbos"]


def test_admission_submit_refuses_when_runtime_policy_is_unresolvable(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def unresolvable(_deployment):
        raise RuntimeError("no deployment named demo")

    def fail(**_kwargs):
        raise AssertionError("submit ran without a resolved runtime policy")

    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment", unresolvable,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admission_workflows._run_submit_with_runtime_approver",
        fail,
    )
    monkeypatch.setattr(
        "okg.substrate.runtime.admin.client_enqueue_admission_submit", fail,
    )

    rc = admission_cli._cmd_admission_submit(
        _submit_args(), dsn="postgres://test", as_json=True,
    )

    assert rc == 1
    error = _json_error(capsys)
    assert "could not resolve admission runtime policy" in error["message"]
    assert "runtime is not enabled" in error["fix_hint"]


def test_runtime_disabled_admission_submit_reports_policy_violation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.producer_authority import ProducerPolicyViolation

    _runtime_enabled(monkeypatch, False)

    def refuse(**_kwargs):
        raise ProducerPolicyViolation(
            code="producer_policy_invalid",
            message="authority.producer_policy producer identity must match",
            verdict={"code": "producer_policy_invalid"},
        )

    monkeypatch.setattr(
        "okg.substrate.runtime.admission_workflows._run_submit_with_runtime_approver",
        refuse,
    )

    rc = admission_cli._cmd_admission_submit(
        _submit_args(), dsn="postgres://inline", as_json=True,
    )

    assert rc == 1
    error = _json_error(capsys)
    assert "producer identity must match" in error["message"]
    assert error["data"]["verdict"] == {"code": "producer_policy_invalid"}
