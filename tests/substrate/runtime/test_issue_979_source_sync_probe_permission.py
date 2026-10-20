from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize("failing_helper", ["intent", "active_workflow"])
def test_probe_failure_blocks_publish_and_corrected_retry_cycles_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    failing_helper: str,
) -> None:
    from okg.substrate import deployments
    from okg.substrate.ingest import service as ingest_service
    from okg.substrate.publisher import deployment_stack as stack_mod
    from okg.substrate.publisher import publisher as publisher_mod
    from okg.substrate.publisher.repository_provenance import RepositoryProvenance
    from okg.substrate.runtime import (
        dbos_enqueue,
        profiling_bridge,
        publisher_workflow,
        source_sync_priority,
        workflow_telemetry,
    )

    effects = {"stack": 0, "cycle": 0}

    class _Stack:
        deployment_name = "issue-979"
        resolution_aware_reuse_enabled = False

        def publisher_kwargs(self):
            return {}

    class _Publisher:
        def __init__(self, *args, source_sync_priority_probe, **kwargs):
            del args, kwargs
            self.source_sync_priority_probe = source_sync_priority_probe

        def cycle(self, *, force_enrichers=False):
            assert force_enrichers is False
            assert self.source_sync_priority_probe() is None
            effects["cycle"] += 1
            return {"generation_id": "gen-1", "status": "published"}

    def _build_stack(**kwargs):
        del kwargs
        effects["stack"] += 1
        return _Stack()

    @contextmanager
    def _profile(**kwargs):
        del kwargs
        yield SimpleNamespace(generation_id=None)

    @contextmanager
    def _trace(**kwargs):
        del kwargs
        yield None

    manifest = SimpleNamespace(deployment_dir=tmp_path)
    monkeypatch.setattr(deployments, "resolve_deployment", lambda _name: manifest)
    monkeypatch.setattr(
        deployments,
        "require_postgres_dsn",
        lambda _manifest, *, deployment: "postgres://unused",
    )
    monkeypatch.setattr(publisher_workflow, "_staging_replay_guard", lambda **_kw: None)
    monkeypatch.setattr(stack_mod, "build_deployment_publisher_stack", _build_stack)
    monkeypatch.setattr(
        stack_mod, "build_runtime_catalog_identity", lambda **_kw: object()
    )
    monkeypatch.setattr(stack_mod, "publisher_kwargs_from_env", lambda: {})
    monkeypatch.setattr(publisher_mod, "Publisher", _Publisher)
    monkeypatch.setattr(
        "okg.substrate.publisher.repository_provenance.tracking_repository_provenance",
        lambda _path: RepositoryProvenance.exact(
            core_revision="1" * 40,
            deployment_revision="2" * 40,
            deployment_tree_sha="3" * 40,
        ),
    )
    monkeypatch.setattr(profiling_bridge, "workflow_profiling_context", _profile)
    monkeypatch.setattr(workflow_telemetry, "dbos_workflow_trace", _trace)
    monkeypatch.setattr(
        ingest_service, "persist_publish_stage_timings", lambda **_kw: None
    )
    # okg#3112 step 4b removed the post-publish attr_index drain (issue
    # 3029); the cycle no longer reports one.
    assert not hasattr(publisher_workflow, "_drain_attr_index_rebuilds_after_cycle")
    # okg#3112 step 2: the declared-attribute index enqueue reports beside
    # the publish too; no database here either.
    asked = {"status": "not_needed", "reason": "not_installed"}
    monkeypatch.setattr(
        publisher_workflow,
        "_enqueue_declared_attr_index_maintenance_after_cycle",
        lambda **_kw: dict(asked),
    )

    if failing_helper == "intent":
        monkeypatch.setattr(
            source_sync_priority,
            "source_sync_admission_intent_workflow_id",
            lambda **_kw: (_ for _ in ()).throw(PermissionError("intent denied")),
        )
        monkeypatch.setattr(
            dbos_enqueue, "active_workflow_id_for_name", lambda **_kw: None
        )
    else:
        monkeypatch.setattr(
            source_sync_priority,
            "source_sync_admission_intent_workflow_id",
            lambda **_kw: None,
        )
        monkeypatch.setattr(
            dbos_enqueue,
            "active_workflow_id_for_name",
            lambda **_kw: (_ for _ in ()).throw(PermissionError("workflow denied")),
        )

    with pytest.raises(RuntimeError, match="publish admission is unavailable") as exc:
        publisher_workflow._run_one_cycle(deployment="issue-979")

    assert isinstance(exc.value.__cause__, PermissionError)
    assert effects == {"stack": 0, "cycle": 0}

    monkeypatch.setattr(
        source_sync_priority,
        "source_sync_admission_intent_workflow_id",
        lambda **_kw: None,
    )
    monkeypatch.setattr(dbos_enqueue, "active_workflow_id_for_name", lambda **_kw: None)

    summary = publisher_workflow._run_one_cycle(deployment="issue-979")

    assert summary == {
        "generation_id": "gen-1",
        "status": "published",
        "declared_attr_index_maintenance": asked,
    }
    assert effects == {"stack": 1, "cycle": 1}
