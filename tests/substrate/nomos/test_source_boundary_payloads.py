from __future__ import annotations

from types import SimpleNamespace

import okg.substrate.nomos as nomos_pkg
import okg.substrate.nomos.store as nomos_store
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import SourceRun


class _Conn:
    def commit(self) -> None:
        pass


class _Adapter:
    name = "git-history"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor", cursor=None) -> SourceRun:
        del run_id, mode, cursor
        return SourceRun(facts=[], completed_scope=True)


def test_source_runner_nomos_boundary_resource_includes_deployment_and_scope(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def evaluate(conn, *, deployment_name, scope, request, **kwargs):
        captured.update({
            "conn": conn,
            "deployment_name": deployment_name,
            "scope": scope,
            "request": request,
            "kwargs": kwargs,
        })
        return SimpleNamespace(
            allowed=True,
            final_effect="allow",
            decision=SimpleNamespace(reason_code="nomos.test.allow"),
        )

    monkeypatch.setattr(
        nomos_pkg,
        "deployment_requires_nomos_runtime",
        lambda deployment_name: True,
    )
    monkeypatch.setattr(
        nomos_pkg,
        "evaluate_runtime_boundary_with_active_policy",
        evaluate,
    )
    runner = SourceRunner(
        "postgres://unused",
        source_name="git-history",
        deployment_name="okg-ci",
    )
    monkeypatch.setattr(runner, "_acquire_conn", _Conn)

    runner._authorize_nomos_source_boundary(
        surface="source.ingest",
        run_id="run-1",
        mode="scope_complete",
        sync_scope={"kind": "repo"},
        target_kind="source_ingest",
        adapter=_Adapter(),
        source_run_mode="scope_complete",
        completed_scope=True,
    )

    request = captured["request"]
    assert request.resource["deployment"] == "okg-ci"
    assert request.resource["scope"] == "deployment:okg-ci"
    assert request.resource["source"] == "git-history"
    assert request.resource["graph_branch"] == "default"
    assert captured["deployment_name"] == "okg-ci"
    assert captured["scope"] == "deployment:okg-ci"


def test_active_store_source_run_resource_includes_deployment_and_scope(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}
    decision = SimpleNamespace(effect="allow")

    def evaluate(conn, *, deployment_name, scope, request, **kwargs):
        captured.update({
            "conn": conn,
            "deployment_name": deployment_name,
            "scope": scope,
            "request": request,
            "kwargs": kwargs,
        })
        # `authorize_source_run_with_active_policy` now routes the outcome
        # through `_effective_decision_for_callers`, which reads `allowed`
        # (#2123). An allow decision is returned unchanged.
        return SimpleNamespace(allowed=True, decision=decision)

    monkeypatch.setattr(
        nomos_store,
        "evaluate_runtime_boundary_with_active_policy",
        evaluate,
    )

    out = nomos_store.authorize_source_run_with_active_policy(
        _Conn(),
        deployment_name="okg-ci",
        scope="deployment:okg-ci",
        producer_id="source:git-history",
        source="git-history",
        run_id="run-1",
        fact_ref_count=10,
        admission_mode="fast_track",
        graph_branch="quarantine:review",
        require_active=True,
    )

    request = captured["request"]
    assert out is decision
    assert request.resource["deployment"] == "okg-ci"
    assert request.resource["scope"] == "deployment:okg-ci"
    assert request.resource["source"] == "git-history"
    assert request.resource["graph_branch"] == "quarantine:review"
    assert captured["deployment_name"] == "okg-ci"
    assert captured["scope"] == "deployment:okg-ci"
