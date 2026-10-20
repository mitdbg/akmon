from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.pact import (
    PACTCoordinationEventSource,
    PACTSource,
)
from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.store import save_manifest


def test_file_and_graph_lifecycle_status_match(tmp_path: Path) -> None:
    manifest = PactManifest(
        pact_version=0,
        target_pact_version=1,
        change=Change(
            id="demo",
            title="Demo",
            status="proposal",
            rationale="Define demo.",
            root_cause="Need graph parity.",
        ),
        requirements=[
            {
                "id": "req.demo",
                "title": "Demo requirement",
                "statement": "Demo must project.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "projects",
                        "when": ["source runs"],
                        "then": ["facts are emitted"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "source facts",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Project demo",
                "verification": "pytest tests/pact/test_pact_source.py -q",
                "requirements": ["req.demo"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_pact_source.py::test_file_and_graph_lifecycle_status_match",
            }
        ],
    )
    save_manifest(tmp_path, manifest)

    source = PACTSource(repo="okg", pact_root=tmp_path / "pact")
    facts = list(source.run("run-1", mode="reconcile").facts)
    nodes = {fact.node_id: fact for fact in facts if isinstance(fact, NodeFact)}
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]

    change = nodes["pact-change:demo"]
    requirement = nodes["pact-req:demo:req.demo"]
    task = nodes["pact-task:demo:task.demo"]

    assert change.subtype == "pact_change"
    assert change.attrs["target_pact_version"] == 1
    assert change.attrs["proposal_gate"] is True
    assert change.attrs["verification_gate"] is False
    assert requirement.attrs["traceability_status"] == "traceable"
    assert requirement.attrs["satisfaction_status"] == "unsatisfied"
    assert task.subtype == "pact_task"
    assert task.attrs["status"] == "todo"
    assert task.attrs["alignment_status"] == "todo"
    assert any(
        edge.src == "pact-delta:demo:requirements"
        and edge.dst == "pact-req:demo:req.demo"
        and edge.edge_type == "contains"
        for edge in edges
    )


@pytest.mark.parametrize("status", ["todo", "in_progress", "blocked", "done"])
def test_pact_task_projects_exact_authored_status(
    tmp_path: Path,
    status: str,
) -> None:
    manifest = PactManifest(
        pact_version=0,
        target_pact_version=1,
        change=Change(id="status-demo", title="Status demo"),
        requirements=[{
            "id": "req.status",
            "statement": "Status must project exactly.",
            "evidence_policy": {"required": ["planned_test"]},
            "acceptance": [{
                "id": "acc.status",
                "description": "exact authored status",
                "evidence": ["planned_test"],
            }],
        }],
        tasks=[{
            "id": "task.status",
            "title": "Project status",
            "status": status,
            "verification": "pytest tests/pact/test_pact_source.py -q",
            "requirements": ["req.status"],
        }],
        test_plan=[{
            "id": "test.status",
            "requirement": "req.status",
            "test_id": "tests/pact/test_pact_source.py",
        }],
    )
    save_manifest(tmp_path, manifest)
    facts = list(PACTSource(repo="okg", pact_root=tmp_path / "pact").run(
        "run-status", mode="reconcile",
    ).facts)
    task = next(
        fact for fact in facts
        if isinstance(fact, NodeFact)
        and fact.node_id == "pact-task:status-demo:task.status"
    )
    assert task.attrs["status"] == status


def test_pact_source_projects_labels_and_method_metadata(tmp_path: Path) -> None:
    manifest = PactManifest(
        pact_version=0,
        target_pact_version=3,
        change=Change(
            id="workflow-demo",
            title="Workflow Demo",
            labels=["campaign", "agent-workflow"],
            method_pack="native.agent-workflow",
            method_metadata={
                "reporting": {"cadence": "checkpoint"},
            },
        ),
        requirements=[
            {
                "id": "req.workflow",
                "statement": "Workflow metadata must project.",
                "evidence_policy": {"required": ["planned_test"]},
                "acceptance": [
                    {
                        "id": "acc.workflow",
                        "description": "metadata projection",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.plan",
                "title": "Plan workflow",
                "labels": ["planning", "review"],
                "method_metadata": {"state": {"external": "draft"}},
                "verification": "pytest tests/pact/test_pact_source.py -q",
                "requirements": ["req.workflow"],
            },
            {
                "id": "task.execute",
                "title": "Execute workflow",
                "method_pack": "scrum.gastown.jira",
                "verification": "pytest tests/pact/test_pact_source.py -q",
                "requirements": ["req.workflow"],
            },
        ],
        test_plan=[
            {
                "id": "test.workflow",
                "requirement": "req.workflow",
                "test_id": "tests/pact/test_pact_source.py::test_pact_source_projects_labels_and_method_metadata",
            }
        ],
    )
    save_manifest(tmp_path, manifest)

    source = PACTSource(repo="okg", pact_root=tmp_path / "pact")
    facts = list(source.run("run-1", mode="reconcile").facts)
    nodes = {fact.node_id: fact for fact in facts if isinstance(fact, NodeFact)}

    change = nodes["pact-change:workflow-demo"]
    plan = nodes["pact-task:workflow-demo:task.plan"]
    execute = nodes["pact-task:workflow-demo:task.execute"]

    assert change.attrs["labels"] == ["campaign", "agent-workflow"]
    assert change.attrs["method_pack"] == "native.agent-workflow"
    assert change.attrs["method_metadata"] == {
        "reporting": {"cadence": "checkpoint"},
    }
    assert plan.attrs["labels"] == ["planning", "review"]
    assert plan.attrs["method_pack"] == "native.agent-workflow"
    assert plan.attrs["method_metadata"] == {
        "reporting": {"cadence": "checkpoint"},
        "state": {"external": "draft"},
    }
    assert execute.attrs["method_pack"] == "scrum.gastown.jira"
    assert execute.attrs["method_metadata"] == {
        "reporting": {"cadence": "checkpoint"},
    }


def test_pact_method_pack_rejects_blank_ids() -> None:
    with pytest.raises(ValueError, match="method_pack must not be empty"):
        Change(id="workflow-demo", title="Workflow demo", method_pack=" ")


def test_pact_source_projects_dependency_edges(tmp_path: Path) -> None:
    manifest = PactManifest(
        pact_version=0,
        target_pact_version=2,
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "title": "Demo requirement",
                "statement": "Demo must project dependency edges.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "dependency",
                        "when": ["PACTSource runs"],
                        "then": ["task dependency edges are emitted"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "dependency edge",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.prepare",
                "title": "Prepare",
                "status": "done",
                "verification": "pytest tests/pact/test_pact_source.py -q",
                "requirements": ["req.demo"],
            },
            {
                "id": "task.build",
                "title": "Build",
                "verification": "pytest tests/pact/test_pact_source.py -q",
                "requirements": ["req.demo"],
            },
        ],
        dependencies=[
            {
                "from": "task.prepare",
                "to": "task.build",
                "kind": "blocks",
                "rationale": "Build should only start after preparation.",
                "waiver": "waiver.dep",
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_pact_source.py::test_pact_source_projects_dependency_edges",
            }
        ],
        waivers=[
            {
                "id": "waiver.dep",
                "scope": "dependencies[0]",
                "reason": "Projection still records waived dependency metadata.",
            }
        ],
    )
    save_manifest(tmp_path, manifest)

    source = PACTSource(repo="okg", pact_root=tmp_path / "pact")
    facts = list(source.run("run-1", mode="reconcile").facts)
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]

    dependency = next(
        edge
        for edge in edges
        if edge.src == "pact-task:demo:task.prepare"
        and edge.dst == "pact-task:demo:task.build"
    )
    assert dependency.edge_type == "blocks"
    assert dependency.attrs == {
        "dependency_kind": "blocks",
        "from_task": "task.prepare",
        "to_task": "task.build",
        "rationale": "Build should only start after preparation.",
        "waiver": "waiver.dep",
    }


def test_pact_source_projects_proof_alignment_edges(tmp_path: Path) -> None:
    manifest = PactManifest(
        pact_version=0,
        target_pact_version=2,
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "title": "Demo requirement",
                "statement": "Demo must project proof edges.",
                "evidence_policy": {"required": ["pytest", "formal"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "proof",
                        "when": ["PACTSource runs"],
                        "then": ["proof edges are emitted"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "proof graph",
                        "evidence": ["test.demo", "formal.demo", "waiver.req"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Do demo",
                "verification": "pytest tests/pact/test_pact_source.py -q",
                "requirements": ["req.demo"],
                "environment": [
                    {
                        "id": "env.OKG_DSN",
                        "kind": "env_var",
                        "env": "OKG_DSN",
                    }
                ],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_pact_source.py::test_pact_source_projects_proof_alignment_edges",
            }
        ],
        formal=[
            {
                "id": "formal.demo",
                "requirement": "req.demo",
                "property_id": "DemoInvariant",
                "module": "formal/demo/Demo.tla",
                "config": "formal/demo/Demo.cfg",
                "checker_command": "tlc2.TLC formal/demo/Demo.tla",
                "refinement_tests": [
                    "tests/pact/test_pact_source.py::test_pact_source_projects_proof_alignment_edges"
                ],
            }
        ],
        evidence=[
            {
                "id": "ev.pytest.demo",
                "kind": "pytest",
                "status": "pass",
                "refs": ["req.demo", "test.demo", "task.demo"],
                "artifact": "pytest.xml",
            },
            {
                "id": "ev.formal.demo",
                "kind": "formal",
                "status": "pass",
                "refs": ["formal.demo"],
                "artifact": "formal-report.yaml",
            },
        ],
        waivers=[
            {
                "id": "waiver.req",
                "scope": "req.demo",
                "reason": "Owner accepted bounded risk.",
                "owner": "riley",
            }
        ],
    )
    save_manifest(tmp_path, manifest)

    source = PACTSource(repo="okg", pact_root=tmp_path / "pact")
    facts = list(source.run("run-1", mode="reconcile").facts)
    nodes = {fact.node_id: fact for fact in facts if isinstance(fact, NodeFact)}
    edges = {
        (fact.src, fact.edge_type, fact.dst)
        for fact in facts
        if isinstance(fact, EdgeFact)
    }

    req = "pact-req:demo:req.demo"
    test = "pact-verification:demo:test:test.demo"
    formal = "pact-verification:demo:formal:formal.demo"
    pytest_evidence = "pact-verification:demo:evidence:ev.pytest.demo"
    formal_evidence = "pact-verification:demo:evidence:ev.formal.demo"
    waiver = "pact-verification:demo:waiver:waiver.req"
    env_check = "pact-verification:demo:env:task.demo:env.OKG_DSN"

    assert nodes[test].attrs["step_kind"] == "planned_test"
    assert nodes[formal].attrs["step_kind"] == "formal_binding"
    assert nodes[formal].attrs["property_id"] == "DemoInvariant"
    assert nodes[pytest_evidence].attrs["step_kind"] == "evidence"
    assert nodes[formal_evidence].attrs["evidence_kind"] == "formal"
    assert nodes[waiver].attrs["step_kind"] == "waiver"
    assert nodes[waiver].attrs["scope"] == "req.demo"

    assert (req, "contains", test) in edges
    assert (req, "contains", formal) in edges
    assert (req, "contains", pytest_evidence) in edges
    assert (req, "contains", formal_evidence) in edges
    assert (req, "contains", waiver) in edges
    assert ("pact-task:demo:task.demo", "contains", env_check) in edges


def test_pact_source_redacts_local_paths_from_projected_evidence(
    tmp_path: Path,
) -> None:
    current_codex_path = Path.home() / ".codex" / "worktrees" / "okg"
    manifest = PactManifest(
        pact_version=0,
        target_pact_version=4,
        change=Change(
            id="redact-demo",
            title="Redact demo",
            status="proposal",
            rationale="Keep /Users/alice/.codex/worktrees/okg out of graph attrs.",
            root_cause="Evidence commands carry workstation paths.",
        ),
        requirements=[
            {
                "id": "req.redact-demo",
                "statement": "Remote CI graph must avoid workstation paths.",
                "evidence_policy": {"required": ["pytest"]},
            }
        ],
        tasks=[
            {
                "id": "task.redact-demo",
                "title": "Project redacted evidence",
                "verification": (
                    "uv run pytest "
                    "/Users/alice/.codex/worktrees/okg/tests/pact/test_pact_source.py"
                ),
                "requirements": ["req.redact-demo"],
            }
        ],
        evidence=[
            {
                "id": "ev.pytest.redact-demo",
                "kind": "pytest",
                "status": "pass",
                "refs": [
                    "req.redact-demo",
                    "task.redact-demo",
                    str(current_codex_path / "tests/pact/test_pact_source.py"),
                ],
                "summary": (
                    "ran from /Users/alice/.codex/worktrees/okg and "
                    f"{current_codex_path} and passed"
                ),
                "command": (
                    "uv run pytest "
                    "/Users/alice/.codex/worktrees/okg/tests/pact/test_pact_source.py"
                    " --token=ghp_1234567890"
                ),
                "artifact": ("/Users/alice/.codex/worktrees/okg/.artifacts/junit.xml"),
            }
        ],
        waivers=[
            {
                "id": "waiver.redact-demo",
                "scope": "req.redact-demo",
                "reason": "Accepted local path /Users/alice/private/file.txt.",
                "owner": "riley",
            }
        ],
    )
    save_manifest(tmp_path, manifest)

    source = PACTSource(repo="okg", pact_root=tmp_path / "pact")
    facts = list(source.run("run-1", mode="reconcile").facts)
    nodes = {fact.node_id: fact for fact in facts if isinstance(fact, NodeFact)}

    change = nodes["pact-change:redact-demo"]
    verification = nodes["pact-verification:redact-demo:task.redact-demo"]
    evidence = nodes["pact-verification:redact-demo:evidence:ev.pytest.redact-demo"]
    waiver = nodes["pact-verification:redact-demo:waiver:waiver.redact-demo"]
    blob = "\n".join(
        str(value)
        for node in (change, verification, evidence, waiver)
        for value in node.attrs.values()
    )

    assert "/Users/alice" not in blob
    assert str(Path.home()) not in blob
    assert "/.codex/" not in blob
    assert "ghp_1234567890" not in blob
    assert "<path:redacted>" in blob
    assert "<codex-state>" in blob
    assert "<secret:redacted>" in evidence.attrs["command"]


def _slim_manifest(change_id: str, title: str) -> PactManifest:
    return PactManifest(
        pact_version=0,
        change=Change(
            id=change_id,
            title=title,
            status="proposal",
            rationale="r",
            root_cause="rc",
        ),
        requirements=[
            {
                "id": f"req.{change_id}",
                "statement": "must project",
                "evidence_policy": {"required": ["executed_test"]},
            }
        ],
        tasks=[
            {
                "id": f"task.{change_id}",
                "title": "t",
                "verification": "pytest -q",
                "requirements": [f"req.{change_id}"],
            }
        ],
    )


def test_decision_ledger_projection_and_archive_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ledger rows become pact_decision nodes + targets edges; archived
    manifests keep their pact_change nodes (standardize-pact-review-interface)."""
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "on")
    save_manifest(tmp_path, _slim_manifest("active-change", "Active"))
    # Archive layout: pact/archive/<id>/pact.yaml
    archived = _slim_manifest("retired-change", "Retired")
    save_manifest(tmp_path, archived)
    src_dir = tmp_path / "pact" / "changes" / "retired-change"
    dst_dir = tmp_path / "pact" / "archive" / "retired-change"
    dst_dir.parent.mkdir(parents=True, exist_ok=True)
    src_dir.rename(dst_dir)

    rows = [
        {
            "decision_id": 7,
            "change_id": "active-change",
            "verdict": "approve",
            "principal": "riley",
            "authority": "operator",
            "note": "ship it",
            "target": None,
            "from_state": "awaiting_review",
            "to_state": "retired",
            "intent_digest": "sha256:abc",
            "decided_at": "2026-06-10T00:00:00+00:00",
        },
    ]
    monkeypatch.setattr(
        PACTSource,
        "_load_ledger_decision_rows",
        lambda self: (
            {"active-change": rows},
            {"status": "ok", "rows": 1, "watermark": "7:1"},
        ),
    )
    source = PACTSource(repo="okg", pact_root=tmp_path / "pact")
    facts = list(source.run("run-1", mode="reconcile").facts)
    nodes = {fact.node_id: fact for fact in facts if isinstance(fact, NodeFact)}
    edges = {
        (fact.src, fact.edge_type, fact.dst)
        for fact in facts
        if isinstance(fact, EdgeFact)
    }

    decision = nodes["pact-decision:active-change:7"]
    assert decision.subtype == "pact_decision"
    assert decision.attrs["verdict"] == "approve"
    assert decision.attrs["principal"] == "riley"
    assert decision.attrs["to_state"] == "retired"
    assert (
        "pact-decision:active-change:7",
        "targets",
        "pact-change:active-change",
    ) in edges
    assert not any(node.subtype == "coordination_event" for node in nodes.values())
    # Archived change still projects, flagged archived.
    assert nodes["pact-change:retired-change"].attrs["archived"] is True
    assert nodes["pact-change:active-change"].attrs["archived"] is False
    assert source.last_run_stats["decision_ledger"]["status"] == "ok"


def test_coordination_events_project_without_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Draft branch events are event-plane truth even before sync-root git."""
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "on")
    event_rows = [
        {
            "event_id": 21,
            "event_type": "intent",
            "change_id": "draft-only",
            "task_id": "task.emit",
            "worktree_suffix": "wt-draft",
            "worktree_path": "/tmp/wt-draft",
            "branch": "claude/draft-only",
            "head_sha": "b" * 40,
            "session_id": "session-draft",
            "principal": "codex",
            "intent_digest": "sha256:draft",
            "payload": {"derived_state": "in_progress"},
            "occurred_at": "2026-06-11T11:15:00+00:00",
        },
    ]
    monkeypatch.setattr(
        PACTCoordinationEventSource,
        "_load_ledger_coordination_event_rows",
        lambda self: (
            event_rows,
            {
                "status": "ok",
                "rows": 1,
                "watermark": "21:1:sha256-exact-row-snapshot",
                "identity": "21:1:sha256-exact-row-snapshot",
            },
        ),
    )

    source = PACTCoordinationEventSource(repo="okg")
    run = source.run("run-1", mode="append")
    facts = list(run.facts)
    nodes = {fact.node_id: fact for fact in facts if isinstance(fact, NodeFact)}
    edges = [fact for fact in facts if isinstance(fact, EdgeFact)]

    event = nodes["pact-coordination-event:draft-only:21"]
    assert event.subtype == "coordination_event"
    assert event.attrs["event_id"] == "pact-coordination-event:draft-only:21"
    assert event.attrs["event_type"] == "intent"
    assert event.attrs["event_family"] == "pact"
    assert event.attrs["change_id"] == "draft-only"
    assert event.attrs["task_id"] == "task.emit"
    assert event.attrs["event_target"] == "draft-only:task.emit"
    assert event.attrs["actor_id"] == "codex"
    assert event.attrs["actor_session_id"] == "session-draft"
    assert event.attrs["idempotency_key"] == "pact_coordination_events:21"
    assert event.attrs["manifest_present"] is False
    assert event.attrs["text"] == "intent recorded for draft-only:task.emit"
    assert "pact-change:draft-only" not in nodes
    assert not edges
    assert run.record_set is None
    assert run.next_cursor["max_event_id"] == 21
    assert source.last_run_stats["ledger"]["status"] == "ok"


def test_decision_watermark_busts_conservative_full_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A new ledger decision re-emits facts even when no manifest changed."""
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "on")
    save_manifest(tmp_path, _slim_manifest("demo-change", "Demo"))
    ledger: dict = {
        "rows": {},
        "status": {"status": "ok", "rows": 0, "watermark": "0:0"},
    }
    monkeypatch.setattr(
        PACTSource,
        "_load_ledger_decision_rows",
        lambda self: (ledger["rows"], ledger["status"]),
    )
    source = PACTSource(repo="okg", pact_root=tmp_path / "pact")
    first = source.run("run-1", mode="cursor")
    list(first.facts)
    cursor = first.next_cursor

    # PACT remains conservative: unchanged manifest/ledger inputs still run
    # because project policy, proof artifacts, or Git history may have changed.
    second = source.run("run-2", mode="cursor", cursor=cursor)
    assert list(second.facts)
    assert source.last_run_stats["scope_status"] == "full"

    # New decision row -> noop busted, decision facts emitted.
    ledger["rows"] = {
        "demo-change": [
            {
                "decision_id": 1,
                "change_id": "demo-change",
                "verdict": "defer",
                "principal": "riley",
                "authority": "operator",
                "note": None,
                "target": None,
                "from_state": "in_progress",
                "to_state": "in_progress",
                "intent_digest": None,
                "decided_at": "2026-06-10T00:00:00+00:00",
            }
        ]
    }
    ledger["status"] = {"status": "ok", "rows": 1, "watermark": "1:1"}
    third = source.run("run-3", mode="cursor", cursor=second.next_cursor)
    emitted = list(third.facts)
    assert any(
        isinstance(f, NodeFact) and f.node_id == "pact-decision:demo-change:1"
        for f in emitted
    )


def test_derived_state_and_supersedes_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """req.listwork.honest-pact-projection: pact_change carries
    ledger-derived state + decision rollup, and supersession becomes a
    traversable change->change edge from both bases."""
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "on")
    save_manifest(tmp_path, _slim_manifest("old-change", "Old"))
    successor = _slim_manifest("new-change", "New")
    successor.change.rationale = "This supersedes old-change with a composed design."
    save_manifest(tmp_path, successor)
    rows = {
        "old-change": [
            {
                "decision_id": 3,
                "change_id": "old-change",
                "verdict": "supersede",
                "principal": "riley",
                "authority": "operator",
                "note": None,
                "target": "new-change",
                "from_state": "awaiting_review",
                "to_state": "superseded",
                "intent_digest": None,
                "decided_at": "2026-06-11T00:00:00+00:00",
            },
        ],
    }
    monkeypatch.setattr(
        PACTSource,
        "_load_ledger_decision_rows",
        lambda self: (rows, {"status": "ok", "rows": 1, "watermark": "3:1"}),
    )
    source = PACTSource(repo="okg", pact_root=tmp_path / "pact")
    facts = list(source.run("run-1", mode="reconcile").facts)
    nodes = {f.node_id: f for f in facts if isinstance(f, NodeFact)}
    edges = {
        (f.src, f.edge_type, f.dst, f.attrs.get("basis"))
        for f in facts
        if isinstance(f, EdgeFact) and f.edge_type == "supersedes"
    }

    old = nodes["pact-change:old-change"].attrs
    assert old["derived_state"] == "superseded"
    assert old["decision_count"] == 1
    assert old["last_verdict"] == "supersede"
    assert old["last_principal"] == "riley"
    # Ledger verdict: successor supersedes predecessor.
    assert (
        "pact-change:new-change",
        "supersedes",
        "pact-change:old-change",
        "ledger_verdict",
    ) in edges
    # Prose declaration in the successor, same direction.
    assert (
        "pact-change:new-change",
        "supersedes",
        "pact-change:old-change",
        "prose_supersedes",
    ) in edges
    # The undecided sibling derives a live state, not authored status.
    new = nodes["pact-change:new-change"].attrs
    assert new["derived_state"] in {"in_progress", "draft"}
    assert new["decision_count"] == 0


def test_ledger_unavailable_is_loud_not_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "on")
    monkeypatch.setenv("OKG_PACT_GRAPH_DEPLOYMENT", "no-such-deployment")
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.setattr(
        "okg.substrate.pact.decisions.worktree_local_env_dsn", lambda: None
    )
    save_manifest(tmp_path, _slim_manifest("demo-change", "Demo"))
    source = PACTSource(repo="okg", pact_root=tmp_path / "pact")
    facts = list(source.run("run-1", mode="reconcile").facts)
    assert any(
        isinstance(f, NodeFact) and f.node_id == "pact-change:demo-change"
        for f in facts
    )
    ledger_stats = source.last_run_stats["decision_ledger"]
    assert ledger_stats["status"] == "unavailable"
    assert ledger_stats["reason"]
