"""Per `polish-pact-agent-ux` req.polish.evidence-kinds.

The canonical evidence-kind registry: loads from project settings, warns on
kinds outside it, and evidence_policy matching resolves through aliases.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.cli.main import main
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.evidence_kinds import (
    DEFAULT_REGISTRY,
    load_registry,
)
from okg.substrate.pact.store import load_manifest
from okg.substrate.pact.validation import validate_manifest

from tests.private_policy import private_policy_path


def _seed_root(tmp_path: Path, project_yaml: str | None = None) -> Path:
    root = tmp_path
    (root / "pact" / "schemas").mkdir(parents=True)
    (root / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    (root / "pact" / "project.yaml").write_text(
        project_yaml or "pact_version: 0\nproject: okg\n", encoding="utf-8"
    )
    (root / "pact" / "schemas" / "pact.schema.yaml").write_text(
        "pact_schema_version: 0\n", encoding="utf-8"
    )
    return root


def _new_change(root: Path, change_id: str, *, policy: list[str]) -> None:
    assert main([
        "pact", "--root", str(root), "new", change_id,
        "--template", "protocol", "--json",
    ]) == 0
    pact_operations.add_requirement(
        root, change_id, req_id="req.kinds", title="Kinds",
        statement="Kinds must match through aliases.", evidence=policy,
    )
    pact_operations.add_task(
        root, change_id, task_id="task.kinds", title="Prove kinds",
        verification="pytest tests/pact/test_evidence_kind_registry.py -q",
        requirements=["req.kinds"],
    )


def _requirement_status(root: Path, change_id: str) -> dict:
    status = pact_operations.evidence_status(root, change_id)
    return next(r for r in status["requirements"] if r["id"] == "req.kinds")


def test_default_registry_canonicalize_and_expand() -> None:
    assert DEFAULT_REGISTRY.canonicalize("pytest") == "executed_test"
    assert DEFAULT_REGISTRY.canonicalize("graph") == "graph_report"
    assert DEFAULT_REGISTRY.canonicalize("executed_test") == "executed_test"
    assert DEFAULT_REGISTRY.expand("pytest") == {
        "executed_test", "pytest", "junit", "test",
    }
    assert DEFAULT_REGISTRY.expand("graph_report") == {"graph_report", "graph"}
    assert DEFAULT_REGISTRY.is_known("manual")
    assert DEFAULT_REGISTRY.is_known("manual_visual")
    assert not DEFAULT_REGISTRY.is_known("vibes")


def test_manual_is_not_an_alias_of_manual_visual(tmp_path: Path) -> None:
    # The UI gate: manual_visual = named reviewer approval of rendered
    # output. A by-hand non-visual check must never satisfy it.
    assert DEFAULT_REGISTRY.canonicalize("manual") == "manual"
    assert "manual" not in DEFAULT_REGISTRY.expand("manual_visual")
    root = _seed_root(tmp_path)
    _new_change(root, "kinds-ui", policy=["manual_visual"])
    pact_operations.attach_evidence(
        root, "kinds-ui", "req.kinds",
        kind="manual", ref="ran-the-commands-by-hand", status="pass",
    )
    req = _requirement_status(root, "kinds-ui")
    assert "manual_visual" in req["missing"]


def test_registry_picks_up_project_yaml_edits_without_restart(tmp_path: Path) -> None:
    # Long-running worker scenario: edit project.yaml, re-load in the same
    # process, see the new entry (mtime-keyed cache).
    import os
    import time

    root = _seed_root(tmp_path)
    assert not load_registry(root).is_known("chaos_probe")
    path = root / "pact" / "project.yaml"
    path.write_text(
        path.read_text(encoding="utf-8")
        + "evidence_kinds:\n  canonical:\n    - chaos_probe\n",
        encoding="utf-8",
    )
    os.utime(path, ns=(time.time_ns(), time.time_ns()))
    assert load_registry(root).is_known("chaos_probe")


def test_malformed_registry_block_warns_not_silent(tmp_path: Path) -> None:
    root = _seed_root(
        tmp_path,
        project_yaml=(
            "pact_version: 0\nproject: okg\n"
            "evidence_kinds:\n  aliases:\n    - not-a-mapping\n"
        ),
    )
    registry = load_registry(root)
    assert registry.config_errors
    _new_change(root, "kinds-bad", policy=["executed_test"])
    report = validate_manifest(load_manifest(root, "kinds-bad"), repo_root=root)
    assert any(
        i.code == "invalid_evidence_kind_registry" and i.severity == "warning"
        for i in report.issues
    )
    # defaults still active — the failure is loud, not fatal
    assert registry.canonicalize("pytest") == "executed_test"


def test_acceptance_proof_token_matches_through_aliases(tmp_path: Path) -> None:
    # An acceptance proof token `review` must actually MATCH design_review
    # evidence (not just be accepted and then never satisfiable).
    root = _seed_root(tmp_path)
    _new_change(root, "kinds-acc", policy=["review"])
    pact_operations.attach_evidence(
        root, "kinds-acc", "req.kinds",
        kind="design_review", ref="subagent:xyz", status="pass",
    )
    manifest = load_manifest(root, "kinds-acc")
    report = validate_manifest(manifest, repo_root=root)
    assert report.ok
    acceptance = report.alignment.get("acceptance", {})
    acc_rows = [
        row for row in acceptance.values()
        if row.get("requirement") == "req.kinds"
    ]
    assert acc_rows
    assert all(row["satisfaction_status"] == "satisfied" for row in acc_rows)


def test_load_registry_merges_project_yaml(tmp_path: Path) -> None:
    root = _seed_root(
        tmp_path,
        project_yaml=(
            "pact_version: 0\nproject: okg\n"
            "evidence_kinds:\n"
            "  canonical:\n    - chaos_probe\n"
            "  aliases:\n    cp: chaos_probe\n"
        ),
    )
    registry = load_registry(root)
    assert registry.is_known("chaos_probe")
    assert registry.canonicalize("cp") == "chaos_probe"
    assert registry.expand("cp") == {"chaos_probe", "cp"}
    # defaults survive the merge
    assert registry.canonicalize("pytest") == "executed_test"


def test_pytest_policy_satisfied_by_executed_test_evidence(tmp_path: Path) -> None:
    root = _seed_root(tmp_path)
    _new_change(root, "kinds-test", policy=["pytest"])
    pact_operations.attach_evidence(
        root, "kinds-test", "req.kinds",
        kind="executed_test", ref="tests/x.py", status="pass",
    )
    req = _requirement_status(root, "kinds-test")
    assert "pytest" not in req["missing"]


def test_generic_policy_resolves_through_aliases(tmp_path: Path) -> None:
    # `graph` policy + `graph_report` evidence: the generic (non-test,
    # non-formal) branch that used to be exact-string.
    root = _seed_root(tmp_path)
    _new_change(root, "kinds-graph", policy=["graph"])
    pact_operations.attach_evidence(
        root, "kinds-graph", "req.kinds",
        kind="graph_report", ref="reports/g.json", status="pass",
    )
    req = _requirement_status(root, "kinds-graph")
    assert "graph" not in req["missing"]
    # and the reverse direction: canonical policy, alias evidence
    _new_change(root, "kinds-review", policy=["design_review"])
    pact_operations.attach_evidence(
        root, "kinds-review", "req.kinds",
        kind="review", ref="subagent:abc", status="pass",
    )
    status = pact_operations.evidence_status(root, "kinds-review")
    req = next(r for r in status["requirements"] if r["id"] == "req.kinds")
    assert "design_review" not in req["missing"]


def test_unknown_evidence_kind_refuses(tmp_path: Path, capsys) -> None:
    """A kind outside the registry is an ERROR (pact-vacuous-gate-close).

    It resolves to no policy slot, so the row can never be proof — recording
    it under a warning beside `ok: true` was the grandfathering that became
    the steady state. The refusal lands at the WRITE, so the manifest never
    acquires the row; a hand-edit that adds one is refused at the gate.
    """
    from okg.substrate.errors import SubstrateError

    import yaml

    root = _seed_root(tmp_path)
    _new_change(root, "kinds-rogue", policy=["executed_test"])
    with pytest.raises(SubstrateError):
        pact_operations.attach_evidence(
            root, "kinds-rogue", "req.kinds",
            kind="vibes", ref="nowhere", status="pass",
        )
    report = validate_manifest(load_manifest(root, "kinds-rogue"), repo_root=root)
    assert report.ok, "the refused write must leave the manifest untouched"

    # Agents also edit pact.yaml directly, so the gate must refuse the row too.
    path = root / "pact" / "changes" / "kinds-rogue" / "pact.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["evidence"] = [{
        "id": "ev.vibes.req-kinds",
        "kind": "vibes",
        "status": "pass",
        "refs": ["req.kinds"],
        "artifact": "nowhere",
    }]
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    errors = [
        issue for issue in
        validate_manifest(load_manifest(root, "kinds-rogue"), repo_root=root).issues
        if issue.code == "unknown_evidence_kind"
    ]
    assert errors
    assert all(issue.severity == "error" for issue in errors)

    rc = main([
        "pact", "--root", str(root), "gate", "post-edit", "kinds-rogue", "--json",
    ])
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    gate = json.loads(lines[-1])
    assert rc == 1
    assert gate["verdict"] == "refused"
    assert "unknown_evidence_kind" in gate["blocked_by"]


def test_unknown_policy_token_warns(tmp_path: Path) -> None:
    root = _seed_root(tmp_path)
    _new_change(root, "kinds-token", policy=["executed_test"])
    # Agents edit pact.yaml directly — mutate the policy token in place.
    import yaml

    path = root / "pact" / "changes" / "kinds-token" / "pact.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc["requirements"][0]["evidence_policy"]["required"] = ["vibes"]
    path.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    report = validate_manifest(load_manifest(root, "kinds-token"), repo_root=root)
    assert any(
        i.code == "unknown_evidence_kind" and "req.kinds" in i.message
        for i in report.issues
    )


def test_project_yaml_declares_repo_registry() -> None:
    # The repo's own project.yaml carries the registry (the requirement's
    # "single source" clause).
    project_yaml = private_policy_path("pact/project.yaml")
    registry = load_registry(project_yaml.parents[1])
    assert registry.canonicalize("pytest") == "executed_test"
    assert registry.canonicalize("review") == "design_review"
    text = project_yaml.read_text(encoding="utf-8")
    assert "evidence_kinds:" in text
