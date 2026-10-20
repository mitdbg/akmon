"""Per `polish-pact-agent-ux` req.polish.doc-holes.

The audited spec holes are closed in their canonical locations, and v4
manifests stop carrying empty edits:/prose: boilerplate.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.store import load_manifest
from okg.substrate.pact.validation import validate_manifest

from tests.private_policy import is_public_copy, private_policy_path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _seed_root(tmp_path: Path) -> Path:
    root = tmp_path
    (root / "pact" / "schemas").mkdir(parents=True)
    (root / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    (root / "pact" / "project.yaml").write_text(
        "pact_version: 4\n"
        "project: okg\n"
        "pact_defaults:\n"
        "  artifact_format_version: 4\n"
        "  target_pact_version: 4\n",
        encoding="utf-8",
    )
    (root / "pact" / "schemas" / "pact.schema.yaml").write_text(
        "pact_schema_version: 0\n", encoding="utf-8"
    )
    return root


def test_new_manifest_carries_no_empty_edits_or_prose(tmp_path: Path) -> None:
    root = _seed_root(tmp_path)
    assert main([
        "pact", "--root", str(root), "new", "demo",
        "--template", "protocol", "--json",
    ]) == 0
    text = (root / "pact" / "changes" / "demo" / "pact.yaml").read_text(
        encoding="utf-8"
    )
    assert "edits:" not in text
    assert "prose:" not in text
    # and a later normalization write does not reintroduce them
    pact_operations.add_requirement(
        root, "demo", req_id="req.demo", title="Demo",
        statement="Demo must validate.", evidence=["executed_test"],
    )
    text = (root / "pact" / "changes" / "demo" / "pact.yaml").read_text(
        encoding="utf-8"
    )
    assert "edits:" not in text
    assert "prose:" not in text
    report = validate_manifest(load_manifest(root, "demo"), repo_root=root)
    assert report.ok


def test_nonempty_legacy_keys_still_fail_v4_validation(tmp_path: Path) -> None:
    root = _seed_root(tmp_path)
    assert main([
        "pact", "--root", str(root), "new", "demo",
        "--template", "protocol", "--json",
    ]) == 0
    path = root / "pact" / "changes" / "demo" / "pact.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc["edits"] = [{
        "id": "edit.1", "at": "t", "actor": "x", "command": "c",
        "before_digest": "a", "after_digest": "b", "validation": "accepted",
    }]
    doc["prose"] = {"why": "should not be here"}
    path.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    report = validate_manifest(load_manifest(root, "demo"), repo_root=root)
    codes = {i.code for i in report.issues if i.severity == "error"}
    assert "v4_edits_in_file" in codes
    assert "v4_prose_in_file" in codes


def test_agents_md_documents_the_one_gate_and_the_receipt_rules() -> None:
    """The holes this test was written to close, restated for v5.

    It used to require the doc explain the pre-commit hook, the dependency
    contract and `--formal-report`. v5 has no git hook, no `dependencies:`
    edges and no `evidence ingest`, so the questions an agent can no longer
    answer from the doc are different ones: when the single gate refuses,
    and what makes a receipt bind.
    """
    text = private_policy_path("pact/AGENTS.md").read_text(encoding="utf-8")
    # The one gate, its two modes, and that there is no hook.
    assert "okg pact check" in text
    assert "--strict" in text
    assert "there are no git hooks" in text
    # What makes a receipt bind, and what unbinds an approval.
    assert "evidence_digest_mismatch" in text
    assert "approval_stale_code" in text
    # The formal-report attachment is history; the skill still names it.
    skill = (
        REPO_ROOT / "src" / "okg" / "substrate" / "pact" / "skills"
        / "pact" / "SKILL.md"
    ).read_text(encoding="utf-8")
    assert "--formal-report" in skill
    assert "property_id:" in skill
    assert "refinement_tests:" in skill


def test_project_yaml_defines_owner_and_drops_investigation() -> None:
    doc = yaml.safe_load(
        private_policy_path("pact/project.yaml").read_text(encoding="utf-8")
    )
    owner = doc["owner_approval"]
    assert "defined_as" in owner
    assert "principal" in owner["defined_as"]
    assert "investigation" not in doc["lifecycle_states"]


def test_dependency_contract_consistent_in_schema() -> None:
    text = (REPO_ROOT / "pact" / "schemas" / "pact.schema.yaml").read_text(
        encoding="utf-8"
    )
    assert "ONLY dependency" in text
    assert "no ordering is enforced unless an explicit edge is written" in text
    doc = yaml.safe_load(text)
    assert "edits" not in doc["required_top_level"]
    assert "edits" in doc["optional_top_level"]


def test_dependency_docs_match_gate_behavior(tmp_path: Path) -> None:
    # The contract the docs now state, verified against the machinery: with
    # no explicit edges, a later task passes task-done while earlier tasks
    # are still todo.
    root = _seed_root(tmp_path)
    assert main([
        "pact", "--root", str(root), "new", "demo",
        "--template", "protocol", "--json",
    ]) == 0
    pact_operations.add_requirement(
        root, "demo", req_id="req.demo", title="Demo",
        statement="Demo must validate.", evidence=["executed_test"],
    )
    for n in (1, 2):
        pact_operations.add_task(
            root, "demo", task_id=f"task.t{n}", title=f"T{n}",
            verification="pytest -q", requirements=["req.demo"],
        )
    pact_operations.attach_evidence(
        root, "demo", "req.demo",
        kind="executed_test", ref="tests/x.py", status="pass",
    )
    gate = pact_operations.task_completion_gate(root, "demo", "task.t2")
    assert gate["blockers"] == [], "no inferred dependency blocks the later task"
    assert gate["missing_proof"] == []


def test_bootstrap_has_one_canonical_home() -> None:
    skill = (
        REPO_ROOT / "src" / "okg" / "substrate" / "pact" / "skills"
        / "agent-workflow" / "SKILL.md"
    ).read_text(encoding="utf-8")
    claude_md = (REPO_ROOT / "CLAUDE.md").read_text(encoding="utf-8")
    runbook = (
        REPO_ROOT / "docs" / "runbooks" / "agents" / "okg-usage.md"
    ).read_text(encoding="utf-8")
    # The skill owns the start/resume procedure. Other entry points only route.
    assert "## Start or resume" in skill
    assert "okg pact view <pact-id> --format=bootstrap --json" in skill
    for other in (claude_md, runbook):
        assert "agent-workflow" in other
        assert "reason to repeat the same command" not in other


# --- tests/private_policy.py: skip only in the public copy -------------------


def test_private_policy_path_fails_loudly_outside_the_public_copy(
    tmp_path: Path,
) -> None:
    # No tests/mirror_deselect.txt: this is the development repository, so a
    # missing policy file is returned as is and reading it fails the test.
    assert not is_public_copy(tmp_path)
    path = private_policy_path("pact/AGENTS.md", root=tmp_path)
    assert path == tmp_path / "pact" / "AGENTS.md"
    with pytest.raises(FileNotFoundError):
        path.read_text(encoding="utf-8")


def test_private_policy_path_skips_only_in_the_public_copy(tmp_path: Path) -> None:
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "mirror_deselect.txt").write_text("", encoding="utf-8")
    assert is_public_copy(tmp_path)
    with pytest.raises(pytest.skip.Exception, match="pact/AGENTS.md"):
        private_policy_path("pact/AGENTS.md", root=tmp_path)
    # A file that is present is read even in the public copy.
    (tmp_path / "pact").mkdir()
    (tmp_path / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    assert private_policy_path("pact/AGENTS.md", root=tmp_path).is_file()
