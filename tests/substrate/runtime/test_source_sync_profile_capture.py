from __future__ import annotations

import json
import time
from contextlib import contextmanager
from types import SimpleNamespace

import pytest


@pytest.fixture(autouse=True)
def _permit_fake_admin_enqueues(monkeypatch: pytest.MonkeyPatch) -> None:
    @contextmanager
    def permit(**_kwargs):
        yield

    monkeypatch.setattr(
        "okg.substrate.runtime.admin.promotion_enqueue_permit",
        permit,
    )
    # #3003: the enqueue takes the source-sync signal on the database; these
    # tests have none. The gating is proven in tests/substrate/test_publisher.py.
    monkeypatch.setattr(
        "okg.substrate.runtime.source_sync_priority."
        "source_sync_admission_intent_lock",
        permit,
    )


def test_profile_drilldown_reports_rootless_speedscope_evidence():
    from okg.substrate.profiling.context import _evidence_for_policy
    from okg.substrate.profiling.policy import (
        DeepCapturePolicy,
        ProfilingPolicy,
    )

    limitations: list[str] = []
    evidence = _evidence_for_policy(
        ProfilingPolicy(
            deep_capture=DeepCapturePolicy(
                collectors=("rootless-speedscope",),
            ),
        ),
        limitations,
        rootless_speedscope_available=True,
    )

    assert evidence["rootless_speedscope"].status == "available"
    assert limitations == []


def test_profile_drilldown_reports_missing_rootless_speedscope_artifact():
    from okg.substrate.profiling.context import _evidence_for_policy
    from okg.substrate.profiling.policy import (
        DeepCapturePolicy,
        ProfilingPolicy,
    )

    limitations: list[str] = []
    evidence = _evidence_for_policy(
        ProfilingPolicy(
            deep_capture=DeepCapturePolicy(
                collectors=("rootless-speedscope",),
            ),
        ),
        limitations,
    )

    assert evidence["rootless_speedscope"].status == "unavailable"
    assert (
        evidence["rootless_speedscope"].reason
        == "rootless sampler produced no artifact"
    )
    assert (
        "rootless_speedscope: rootless sampler produced no artifact"
        in limitations
    )


def test_rootless_speedscope_collector_writes_bundle(monkeypatch, tmp_path):
    from okg.substrate.profiling import profile_stage, profiling_context
    from okg.substrate.profiling.policy import (
        DeepCapturePolicy,
        ProfilingPolicy,
    )

    monkeypatch.setenv("OKG_PROFILE_DIR", str(tmp_path / "profiles"))

    with profiling_context(
        deployment="okg-workspace",
        target_kind="source_sync",
        command="okg_source_sync",
        policy=ProfilingPolicy(
            deep_capture=DeepCapturePolicy(
                collectors=("rootless-speedscope",),
            ),
        ),
        force_deep=True,
    ) as prof:
        prof.source_sync = {
            "sources": ["pact"],
            "run_mode": "scope_complete",
        }
        with profile_stage("source_sync.test"):
            time.sleep(0.03)

    root = tmp_path / "profiles" / "okg-workspace" / prof.profile_run_id
    profile = json.loads((root / "profile.json").read_text())
    speedscope = json.loads((root / "speedscope.json").read_text())

    assert profile["evidence"]["rootless_speedscope"]["status"] == "available"
    assert {
        "path": "speedscope.json",
        "media_type": "application/json",
        "kind": "rootless-speedscope",
    } in profile["artifacts"]
    assert speedscope["$schema"].endswith("file-format-schema.json")
    assert speedscope["profiles"][0]["type"] == "sampled"
    assert speedscope["profiles"][0]["samples"]
    assert len(speedscope["profiles"][0]["samples"]) == len(
        speedscope["profiles"][0]["weights"]
    )


def test_source_sync_admin_enqueue_passes_profile_capture_args(monkeypatch):
    from okg.substrate.runtime import admin

    captured: dict[str, object] = {}

    class _Handle:
        workflow_id = "wf-source-sync-profile"

    class _Workflow:
        status = "SUCCESS"
        output = {"report": {"results": [], "publishes": []}, "any_failed": False}
        error = None

    class _Client:
        def enqueue(self, options, *args):
            captured["options"] = options
            captured["args"] = args
            return _Handle()

        def list_workflows(self, workflow_ids=None):
            return [_Workflow()]

    monkeypatch.setattr(admin, "_build_client", lambda dsn: _Client())
    monkeypatch.setattr(
        admin, "latest_registered_app_version", lambda dsn: None,
    )

    result = admin.client_enqueue_source_sync(
        deployment="okg-workspace",
        dsn="postgres://unused",
        sources=["pact"],
        publish=False,
        profile_force_deep=True,
        profile_collectors=("rootless-speedscope",),
        poll_interval_seconds=0,
    )

    assert result.status == "SUCCESS"
    assert captured["options"] == {
        "queue_name": "okg-okg-workspace-source-sync",
        "workflow_name": "okg_source_sync",
        "priority": 1,
    }
    args = captured["args"]
    assert args[0] == ["pact"]
    assert args[7] is True
    assert args[8] == ["rootless-speedscope"]


def test_source_sync_admin_preserves_typed_terminal_error(monkeypatch):
    from okg.substrate.errors import SourceConfigurationAuthorityInvalid
    from okg.substrate.runtime import admin

    class _Handle:
        workflow_id = "wf-source-sync-authority"

    class _Workflow:
        status = "ERROR"
        output = None
        error = SourceConfigurationAuthorityInvalid(
            "configured source root disappeared",
            data={"configured_path": "/sealed/source-root"},
            fix_hint="repair the sealed launcher",
        )

    class _Client:
        def enqueue(self, _options, *_args):
            return _Handle()

        def list_workflows(self, workflow_ids=None):
            return [_Workflow()]

    monkeypatch.setattr(admin, "_build_client", lambda dsn: _Client())
    monkeypatch.setattr(
        admin, "latest_registered_app_version", lambda dsn: None,
    )

    result = admin.client_enqueue_source_sync(
        deployment="okg-workspace",
        dsn="postgres://unused",
        sources=["pact"],
        poll_interval_seconds=0,
    )

    assert result.error_code == "source_configuration_authority_invalid"
    assert result.error_data == {"configured_path": "/sealed/source-root"}
    assert result.error_fix_hint == "repair the sealed launcher"


def test_source_sync_admin_enqueue_can_return_without_waiting(monkeypatch):
    from okg.substrate.runtime import admin

    captured: dict[str, object] = {}

    class _Handle:
        workflow_id = "wf-source-sync-profile-queued"

    class _Client:
        def enqueue(self, options, *args):
            captured["options"] = options
            captured["args"] = args
            return _Handle()

        def list_workflows(self, workflow_ids=None):  # pragma: no cover
            raise AssertionError("enqueue-only mode must not poll workflows")

    monkeypatch.setattr(admin, "_build_client", lambda dsn: _Client())
    monkeypatch.setattr(
        admin, "latest_registered_app_version", lambda dsn: None,
    )

    result = admin.client_enqueue_source_sync(
        deployment="okg-workspace",
        dsn="postgres://unused",
        sources=["pact"],
        publish=True,
        profile_force_deep=True,
        profile_collectors=("rootless-speedscope",),
        wait_for_result=False,
    )

    assert result.workflow_id == "wf-source-sync-profile-queued"
    assert result.status == "ENQUEUED"
    assert result.any_failed is False
    assert result.report is None
    assert captured["options"] == {
        "queue_name": "okg-okg-workspace-source-sync",
        "workflow_name": "okg_source_sync",
        "priority": 1,
    }


def test_source_sync_admin_enqueue_prioritizes_operator_work(monkeypatch):
    from okg.substrate.runtime import admin

    captured: dict[str, object] = {}

    class _Handle:
        workflow_id = "wf-source-sync-operator"

    class _Client:
        def enqueue(self, options, *args):
            captured["options"] = options
            captured["args"] = args
            return _Handle()

        def list_workflows(self, workflow_ids=None):  # pragma: no cover
            raise AssertionError("enqueue-only mode must not poll workflows")

    monkeypatch.setattr(admin, "_build_client", lambda dsn: _Client())
    monkeypatch.setattr(
        admin, "latest_registered_app_version", lambda dsn: None,
    )

    result = admin.client_enqueue_source_sync(
        deployment="okg-workspace",
        dsn="postgres://unused",
        sources=["pact"],
        publish=False,
        wait_for_result=False,
    )

    assert result.workflow_id == "wf-source-sync-operator"
    assert captured["options"] == {
        "queue_name": "okg-okg-workspace-source-sync",
        "workflow_name": "okg_source_sync",
        "priority": 10,
    }


def test_source_sync_admin_enqueue_passes_live_app_version(monkeypatch):
    from okg.substrate.runtime import admin

    captured: dict[str, object] = {}

    class _Handle:
        workflow_id = "wf-source-sync-versioned"

    class _Client:
        def enqueue(self, options, *args):
            captured["options"] = options
            captured["args"] = args
            return _Handle()

        def list_workflows(self, workflow_ids=None):  # pragma: no cover
            raise AssertionError("enqueue-only mode must not poll workflows")

    monkeypatch.setattr(admin, "_build_client", lambda dsn: _Client())
    monkeypatch.setattr(
        admin, "latest_registered_app_version", lambda dsn: "app-v1",
    )

    result = admin.client_enqueue_source_sync(
        deployment="okg-workspace",
        dsn="postgres://unused",
        sources=["pact"],
        publish=False,
        wait_for_result=False,
    )

    assert result.workflow_id == "wf-source-sync-versioned"
    assert captured["options"] == {
        "queue_name": "okg-okg-workspace-source-sync",
        "workflow_name": "okg_source_sync",
        "priority": 10,
        "app_version": "app-v1",
    }


def test_source_sync_workflow_forces_profile_and_annotates_context(
    monkeypatch,
    tmp_path,
):
    from okg.substrate.ingest.service import IngestRunResult
    from okg.substrate.runtime import source_sync_workflow as workflow

    captured: dict[str, object] = {}

    canonical_source_root = tmp_path / "source-root"
    canonical_source_root.mkdir()
    source_root_alias = tmp_path / "source-root-alias"
    source_root_alias.symlink_to(
        canonical_source_root,
        target_is_directory=True,
    )
    monkeypatch.setattr(
        workflow,
        "_resolve_context_cached",
        lambda deployment: (
            SimpleNamespace(profiling=None, sync_root=source_root_alias),
            "postgres://unused",
            tmp_path,
        ),
    )
    monkeypatch.setattr(
        workflow,
        "_source_sync_start_usefulness",
        lambda **_kwargs: {
            "useful": False,
            "reason": "all_sources_superseded_by_selected_frontiers",
        },
    )

    def fake_run_ingest(options):
        captured["options"] = options
        return IngestRunResult(
            any_failed=False,
            report={"results": [], "publishes": []},
        )

    monkeypatch.setattr(
        "okg.substrate.ingest.service.run_ingest",
        fake_run_ingest,
    )

    @contextmanager
    def fake_workflow_profiling_context(**kwargs):
        prof = SimpleNamespace(
            owner_id=None,
            owner_meta={},
            source_sync={},
        )
        captured["profile_kwargs"] = kwargs
        captured["profile_context"] = prof
        yield prof

    monkeypatch.setattr(
        "okg.substrate.runtime.profiling_bridge.workflow_profiling_context",
        fake_workflow_profiling_context,
    )

    out = workflow._run_one_ingest(
        deployment="okg-workspace",
        sources=["pact"],
        run_mode="scope_complete",
        include_dependents=True,
        dependent_fanout="closure",
        events=[],
        publish=True,
        profile_force_deep=True,
        profile_collectors=("rootless-speedscope",),
        force_enrichers=True,
    )

    assert out["any_failed"] is False
    assert out["source_sync_runtime"]["authority"] == "dbos.workflow_status"
    assert "okg_job_runtime_ledger" not in out["source_sync_runtime"]
    options = captured["options"]
    assert options.profile_force_deep is True
    assert options.force_enrichers is True
    assert (
        options.source_registry_repo_root_override
        == canonical_source_root
    )
    assert options.owner_id == "workflow:source_sync:okg-workspace"
    assert options.owner_meta["profile_force_deep"] is True
    assert options.owner_meta["profile_collectors"] == ["rootless-speedscope"]
    profile_kwargs = captured["profile_kwargs"]
    assert profile_kwargs["force_deep"] is True
    assert profile_kwargs["collectors"] == ("rootless-speedscope",)
    prof = captured["profile_context"]
    assert prof.owner_id == "workflow:source_sync:okg-workspace"
    assert prof.source_sync["sources"] == ["pact"]
    assert prof.source_sync["event_count"] == 0
