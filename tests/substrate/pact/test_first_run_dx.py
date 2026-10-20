"""First-run DX in a FRESH repo (issue #643, PACT `polish-pact-first-run-dx`).

The first PACT authored in a repo without `pact/project.yaml` hit four
papercuts the okg repo never sees because its own project.yaml papers over
them: a `pact_version: 0` starter, alarming/unactionable no-DSN ledger
errors, post-edit warnings that scold the documented lean shape, and
learn-by-example evidence artifact YAML. Each test here drives the CLI
(`okg.substrate.cli.main.main`) against a tmp git repo with NO
pact/project.yaml — this repo's own project.yaml would mask the bugs.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.pact.store import load_manifest
from okg.substrate.pact.validation import CURRENT_ARTIFACT_FORMAT_VERSION
from okg.substrate.pact.workflow_authority import (
    WorkflowAuthorityInput,
    evaluate_workflow_authority,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def _bare_repo(tmp_path: Path) -> Path:
    """A fresh git repo with NO pact/project.yaml (the marius bring-up shape)."""
    root = tmp_path / "fresh"
    root.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t",
         "commit", "-q", "--allow-empty", "-m", "seed"],
        cwd=root, check=True,
    )
    return root


@pytest.fixture
def no_dsn(monkeypatch) -> None:
    """Unset EVERY DSN source the ledger resolver consults.

    - process env ``OKG_DSN``;
    - deployment-manifest interpolation (``OKG_PACT_GRAPH_DEPLOYMENT`` pinned
      to a nonexistent deployment so no deployment.yaml resolves at all —
      the in-tree fallback would otherwise find this repo's okg-workspace);
    - ``.okg/local.env`` overlays and the worktree main-checkout local.env
      fallback (both disabled under pytest unless opted in; the opt-in env
      vars are removed here so the guard holds).

    Also keeps every test in this file off the live ledger database.
    """
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
    monkeypatch.delenv("OKG_LOCAL_ENV", raising=False)
    monkeypatch.delenv("OKG_ENABLE_LOCAL_ENV_FOR_TESTS", raising=False)
    monkeypatch.setenv(
        "OKG_PACT_GRAPH_DEPLOYMENT", "pactdx-no-such-deployment"
    )


def _json_out(capsys) -> dict:
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    return json.loads(lines[-1])


def _manifest_data(root: Path, change_id: str) -> dict:
    return yaml.safe_load(
        (root / "pact" / "changes" / change_id / "pact.yaml").read_text(
            encoding="utf-8"
        )
    )


def _post_edit_full(root: Path, change_id: str, capsys) -> tuple[int, dict]:
    rc = main([
        "pact", "--root", str(root), "gate", "post-edit", change_id,
        "--full", "--json",
    ])
    return rc, _json_out(capsys)


# --- req.pactdx.version-stamp -------------------------------------------


def test_new_stamps_current_version_without_project_yaml(
    tmp_path: Path, capsys, no_dsn
) -> None:
    """`okg pact new` in a repo WITHOUT pact/project.yaml stamps
    pact_version == target_pact_version == the validator's current
    version constant (never 0), scaffolds a non-empty
    requirement/task/test_plan skeleton, and passes `gate post-edit`
    with zero validation errors."""
    root = _bare_repo(tmp_path)

    # Fast tier-1 shape (explicit: bare `new` now writes a v5 manifest).
    assert main([
        "pact", "--root", str(root), "new", "demo", "--title", "Demo",
        "--shape", "fast", "--json",
    ]) == 0
    _json_out(capsys)
    data = _manifest_data(root, "demo")
    assert data["pact_version"] == CURRENT_ARTIFACT_FORMAT_VERSION
    assert data["target_pact_version"] == CURRENT_ARTIFACT_FORMAT_VERSION
    assert data["pact_version"] != 0
    assert data["requirements"], "starter must not have empty stanzas"
    assert data["tasks"], "starter must not have empty stanzas"
    assert data["test_plan"], "starter must not have empty stanzas"

    rc, payload = _post_edit_full(root, "demo", capsys)
    # The manifest validates; the intent registration cannot reach a ledger
    # in this shell, so the gate is DEFERRED (exit 3), not green and not a
    # refusal — nothing was recorded anywhere durable.
    assert rc == 3 and payload["verdict"] == "deferred"
    assert payload["ok"] is False
    assert payload["validation"]["errors"] == []

    # Explicit full (tier-2+) shape: same stamp, and a valid v4-shaped
    # skeleton instead of the old empty stanzas.
    assert main([
        "pact", "--root", str(root), "new", "demo-full", "--shape", "full",
        "--title", "Demo Full", "--json",
    ]) == 0
    _json_out(capsys)
    data = _manifest_data(root, "demo-full")
    assert data["pact_version"] == CURRENT_ARTIFACT_FORMAT_VERSION
    assert data["target_pact_version"] == CURRENT_ARTIFACT_FORMAT_VERSION
    assert data["requirements"], "full starter must not have empty stanzas"
    assert data["tasks"], "full starter must not have empty stanzas"
    assert data["test_plan"], "full starter must not have empty stanzas"
    # The skeleton is wired together, not disjoint placeholders.
    assert data["tasks"][0]["requirements"] == [
        data["requirements"][0]["id"]
    ]
    assert data["test_plan"][0]["requirement"] == (
        data["requirements"][0]["id"]
    )

    rc, payload = _post_edit_full(root, "demo-full", capsys)
    # The manifest validates; the intent registration cannot reach a ledger
    # in this shell, so the gate is DEFERRED (exit 3), not green and not a
    # refusal — nothing was recorded anywhere durable.
    assert rc == 3 and payload["verdict"] == "deferred"
    assert payload["ok"] is False
    assert payload["validation"]["errors"] == []


def test_new_stamps_current_version_with_defaultless_project_yaml(
    tmp_path: Path, capsys, no_dsn
) -> None:
    """Adversarial-review M3: a pact/project.yaml added for OTHER settings
    (e.g. pr_policy) but without a `pact_defaults` block must not resurrect
    the version-0 empty-stanza starter — only DECLARED defaults win."""
    root = _bare_repo(tmp_path)
    settings = root / "pact" / "project.yaml"
    settings.parent.mkdir(parents=True, exist_ok=True)
    settings.write_text("pr_policy:\n  base: dev\n", encoding="utf-8")

    assert main([
        "pact", "--root", str(root), "new", "demo-defaultless",
        "--title", "Demo", "--shape", "fast", "--json",
    ]) == 0
    _json_out(capsys)
    data = _manifest_data(root, "demo-defaultless")
    assert data["pact_version"] == CURRENT_ARTIFACT_FORMAT_VERSION
    assert data["target_pact_version"] == CURRENT_ARTIFACT_FORMAT_VERSION
    assert data["requirements"], "starter must not have empty stanzas"
    assert data["tasks"], "starter must not have empty stanzas"

    rc, payload = _post_edit_full(root, "demo-defaultless", capsys)
    # The manifest validates; the intent registration cannot reach a ledger
    # in this shell, so the gate is DEFERRED (exit 3), not green and not a
    # refusal — nothing was recorded anywhere durable.
    assert rc == 3 and payload["verdict"] == "deferred"
    assert payload["ok"] is False
    assert payload["validation"]["errors"] == []


# --- req.pactdx.ledger-hints --------------------------------------------


def test_no_dsn_ledger_error_names_fixes(
    tmp_path: Path, capsys, no_dsn
) -> None:
    """With every DSN source unset, evidence/gate verbs say plainly that
    no database connection is configured in this shell, name BOTH fixes
    (export OKG_DSN=... and okg deployment env <name> --write-local),
    and never mislabel the state as pact_coordination_disagreement —
    that name stays reserved for actual disagreement."""
    root = _bare_repo(tmp_path)
    assert main([
        "pact", "--root", str(root), "new", "demo", "--title", "Demo",
        "--intent", "Demo must work end to end.",
        "--acceptance", "The demo check passes.",
        "--test-id", "tests/test_demo.py::test_demo",
        "--json",
    ]) == 0
    _json_out(capsys)

    # Evidence verb: ingest defers loudly with the plain-language reason.
    report = root / "graph-report.yaml"
    report.write_text(
        "evidence:\n"
        "- kind: graph_report\n"
        "  status: pass\n"
        "  summary: publish checked\n",
        encoding="utf-8",
    )
    # Exit 3: the evidence row persisted, the ledger write is queued.
    assert main([
        "pact", "--root", str(root), "evidence", "ingest", "demo",
        "--graph-report", str(report), "--json",
    ]) == 3
    payload = _json_out(capsys)
    assert payload["verdict"] == "deferred"
    assert payload["ok"] is False
    assert payload["evidence"] == 1
    assert payload["coordination"] == "deferred"
    reason = payload["ledger_reason"]
    assert "no database connection configured in this shell" in reason
    assert "OKG_DSN" in reason
    assert "deployment env" in reason
    assert "--write-local" in reason
    assert "pact_coordination_disagreement" not in json.dumps(payload)

    # Prove the named test through execution-attested JUnit so the ONLY
    # obstacle left is the missing DSN. Ingest still defers for the same
    # ledger reason (exit 3), after persisting the evidence locally.
    pytest_report = root / "pytest-report.xml"
    pytest_report.write_text(
        '<testsuite><testcase classname="tests.test_demo" '
        'name="test_demo"/></testsuite>',
        encoding="utf-8",
    )
    assert main([
        "pact", "--root", str(root), "evidence", "ingest", "demo",
        "--pytest-report", str(pytest_report), "--json",
    ]) == 3
    _json_out(capsys)

    rc = main([
        "pact", "--root", str(root), "gate", "task-done", "demo",
        "task.demo", "--json",
    ])
    result = _json_out(capsys)
    # Proof complete, ledger unreachable: this is a DEFERRAL, not the
    # refusal it used to report. The task is still not marked done, and
    # the exit code (3) says queued-not-recorded rather than failed.
    assert rc == 3 and result["verdict"] == "deferred"
    assert result["ok"] is False
    assert result["marked"] is False
    assert result["missing_proof"] == []
    assert "ledger_dsn_not_configured" in result["blocked_by"]
    authority = result["workflow_authority"]
    assert "ledger_dsn_not_configured" in authority["blockers"]
    assert "ledger_not_fresh" not in authority["blockers"]
    assert "pact_coordination_disagreement" not in authority["blockers"]
    ledger_authority = authority["ledger_authority"]
    assert ledger_authority["cause"] == "no_dsn_configured"
    assert (
        "no database connection configured in this shell"
        in ledger_authority["reason"]
    )
    guidance = " ".join([
        *result["recommended_commands"],
        *authority["required_live_checks"],
    ])
    assert "OKG_DSN" in guidance
    assert "--write-local" in guidance
    # Complete proof + no-DSN-only refusal still queues the closure intent.
    assert result["closure_intent"]["status"] == "deferred"

    # Reserved name: a reachable-ledger coordination disagreement still
    # reports pact_coordination_disagreement (and not the no-DSN blocker).
    decision = evaluate_workflow_authority(WorkflowAuthorityInput(
        file_fresh=True,
        graph_required=False,
        graph_ready=True,
        graph_fresh=True,
        ledger_fresh=True,
        generation_pin_matches=True,
        evidence_artifacts_fresh=True,
        pact_coordination_agree=False,
    ))
    assert "pact_coordination_disagreement" in decision.blockers
    assert "ledger_dsn_not_configured" not in decision.blockers


# --- req.pactdx.warning-contradiction -----------------------------------


def test_lean_shape_emits_no_completeness_warnings(
    tmp_path: Path, capsys, no_dsn
) -> None:
    """`gate post-edit` on the DOCUMENTED lean shape (evidence_policy
    declared, scenarios/acceptance empty — pact/AGENTS.md's "skip the
    boilerplate") emits zero missing_scenario / missing_acceptance
    warnings; a requirement with NO evidence policy still warns; and
    pact/AGENTS.md states the shipped contract."""
    root = _bare_repo(tmp_path)
    lean_dir = root / "pact" / "changes" / "lean-demo"
    lean_dir.mkdir(parents=True)
    (lean_dir / "pact.yaml").write_text(
        "pact_version: 4\n"
        "target_pact_version: 4\n"
        "change:\n"
        "  id: lean-demo\n"
        "  title: Lean demo\n"
        "  status: proposal\n"
        "  tier: 1\n"
        "  rationale: the documented lean-authoring shape\n"
        "requirements:\n"
        "- id: req.lean.one\n"
        "  title: One\n"
        "  statement: The lean shape must not be scolded.\n"
        "  evidence_policy:\n"
        "    required:\n"
        "    - executed_test\n"
        "  scenarios: []\n"
        "  acceptance: []\n"
        "- id: req.lean.two\n"
        "  title: Two\n"
        "  statement: required_any also declares executable evidence.\n"
        "  evidence_policy:\n"
        "    required_any:\n"
        "    - ui_visual\n"
        "    - ui_interaction\n"
        "  scenarios: []\n"
        "  acceptance: []\n"
        "tasks:\n"
        "- id: task.lean\n"
        "  title: Do the lean work\n"
        "  status: todo\n"
        "  verification: uv run pytest tests/substrate/pact/test_first_run_dx.py -q\n"
        "  requirements:\n"
        "  - req.lean.one\n"
        "  - req.lean.two\n"
        "test_plan:\n"
        "- id: test.req-lean-one.main\n"
        "  requirement: req.lean.one\n"
        "  test_id: tests/substrate/pact/test_first_run_dx.py::test_lean_shape_emits_no_completeness_warnings\n",
        encoding="utf-8",
    )

    rc, payload = _post_edit_full(root, "lean-demo", capsys)
    # The manifest validates; the intent registration cannot reach a ledger
    # in this shell, so the gate is DEFERRED (exit 3), not green and not a
    # refusal — nothing was recorded anywhere durable.
    assert rc == 3 and payload["verdict"] == "deferred"
    assert payload["ok"] is False
    assert payload["validation"]["errors"] == []
    warning_codes = {w["code"] for w in payload["validation"]["warnings"]}
    assert "missing_scenario" not in warning_codes, (
        "post-edit must not scold the documented lean shape"
    )
    assert "missing_acceptance" not in warning_codes, (
        "post-edit must not scold the documented lean shape"
    )

    # A requirement with NO evidence policy still warns (the completeness
    # warnings remain the signal exactly where nothing else covers it).
    bare_dir = root / "pact" / "changes" / "no-policy"
    bare_dir.mkdir(parents=True)
    (bare_dir / "pact.yaml").write_text(
        "pact_version: 4\n"
        "target_pact_version: 4\n"
        "change:\n"
        "  id: no-policy\n"
        "  title: No policy\n"
        "  status: proposal\n"
        "  tier: 1\n"
        "requirements:\n"
        "- id: req.bare\n"
        "  statement: A requirement with no evidence policy at all.\n"
        "tasks:\n"
        "- id: task.bare\n"
        "  title: Bare\n"
        "  status: todo\n"
        "  verification: manual check\n"
        "  requirements:\n"
        "  - req.bare\n",
        encoding="utf-8",
    )
    _rc, payload = _post_edit_full(root, "no-policy", capsys)
    warning_codes = {w["code"] for w in payload["validation"]["warnings"]}
    assert "missing_scenario" in warning_codes
    assert "missing_acceptance" in warning_codes

    # The doc side moved: pact/AGENTS.md documents PACT v5, which has no
    # scenarios or evidence policies; the v4 warning vocabulary lives with
    # the v4 validator only (pact-v5-git-native).


# --- req.pactdx.evidence-template ---------------------------------------


def test_evidence_template_roundtrips_through_ingest(
    tmp_path: Path, capsys, no_dsn
) -> None:
    """`evidence template --kind graph_report` emits a commented skeleton
    that `evidence ingest --graph-report` accepts AS-IS on a scratch
    PACT; `--kind formal_check` emits the strict formal shape that
    ingests once filled with the run's real values (property, digests,
    artifact); unknown kinds error with the canonical kind list.

    The ledger is unreachable here (no DSN): ingest itself succeeds and
    coordination defers — the deferred/no-ledger path is asserted
    explicitly instead of a ledger write."""
    root = _bare_repo(tmp_path)
    assert main([
        "pact", "--root", str(root), "new", "scratch", "--title", "Scratch",
        "--intent", "Scratch proves the template roundtrip.",
        "--acceptance", "Template output ingests unchanged.",
        "--test-id", "tests/test_scratch.py::test_scratch",
        "--json",
    ]) == 0
    _json_out(capsys)

    # graph_report: emitted skeleton ingests as-is.
    assert main([
        "pact", "--root", str(root), "evidence", "template",
        "--kind", "graph_report", "--json",
    ]) == 0
    payload = _json_out(capsys)
    assert payload["ok"] is True
    assert payload["kind"] == "graph_report"
    assert payload["ingest_flag"] == "--graph-report"
    template_text = payload["template"]
    assert template_text.lstrip().startswith("#"), "skeleton is commented"
    reports = root / "reports"
    reports.mkdir()
    graph_report = reports / "graph-report.yaml"
    graph_report.write_text(template_text, encoding="utf-8")
    assert main([
        "pact", "--root", str(root), "evidence", "ingest", "scratch",
        "--graph-report", str(graph_report), "--json",
    ]) == 3
    ingest_payload = _json_out(capsys)
    # No ledger in this shell: the coordination side defers loudly rather
    # than recording (the manifest row below is the ingest proof). The
    # deferral is the top-level verdict now, not a buried `coordination`.
    assert ingest_payload["verdict"] == "deferred"
    assert ingest_payload["ok"] is False
    assert ingest_payload.get("coordination") == "deferred"
    manifest = load_manifest(root, "scratch")
    rows = [ev for ev in manifest.evidence if ev.kind == "graph_report"]
    assert len(rows) == 1
    assert rows[0].refs == ["scratch"], "omitted refs default to the change id"
    assert rows[0].status == "pass"

    rc, post = _post_edit_full(root, "scratch", capsys)
    # Deferred for the same reason as the ingest above: the manifest is clean,
    # the intent registration has no ledger to reach.
    assert rc == 3 and post["verdict"] == "deferred"
    assert post["validation"]["errors"] == []

    # Alias resolution + human mode: `--kind graph` prints the same raw
    # YAML skeleton to stdout (redirectable into a file).
    assert main([
        "pact", "--root", str(root), "evidence", "template",
        "--kind", "graph",
    ]) == 0
    raw = capsys.readouterr().out
    assert raw.lstrip().startswith("#")
    assert "evidence:" in raw
    assert "kind: graph_report" in raw

    # formal_check: bind a real property, then fill the skeleton's
    # commented fields with this run's values — ingest accepts it.
    module = root / "specs" / "model.tla"
    module.parent.mkdir()
    module.write_text("---- MODULE model ----\n====\n", encoding="utf-8")
    config = root / "specs" / "model.cfg"
    config.write_text("INIT Init\n", encoding="utf-8")
    checker = root / "scripts" / "check.py"
    checker.parent.mkdir()
    checker.write_text("print('ok')\n", encoding="utf-8")
    ref_test = root / "tests" / "test_ref.py"
    ref_test.parent.mkdir()
    ref_test.write_text("def test_case():\n    assert True\n", encoding="utf-8")
    checker_command = "python scripts/check.py"
    assert main([
        "pact", "--root", str(root), "evidence", "bind-tla", "scratch",
        "--requirement", "req.scratch", "--model", "specs/model.tla",
        "--property-id", "prop.safety", "--config", "specs/model.cfg",
        "--checker-command", checker_command,
        "--refinement-test", "tests/test_ref.py::test_case",
        "--json",
    ]) == 0
    _json_out(capsys)

    assert main([
        "pact", "--root", str(root), "evidence", "template",
        "--kind", "formal_check", "--json",
    ]) == 0
    formal_payload = _json_out(capsys)
    assert formal_payload["kind"] == "formal_check"
    assert formal_payload["ingest_flag"] == "--formal-report"
    skeleton = yaml.safe_load(formal_payload["template"])
    assert list(skeleton) == ["results"], "shape matches what ingest parses"
    row = skeleton["results"][0]
    # Every strict field ingest checks is named by the skeleton.
    for field in (
        "property_id", "status", "fresh", "command", "artifact",
        "module_digest", "refinement_tests",
    ):
        assert field in row, f"skeleton must name {field}"
    assert row["status"] == "pass"
    assert row["fresh"] is True
    # Fill in this run's real values, as an author would.
    row["property_id"] = "prop.safety"
    row["command"] = checker_command
    row["checker_command"] = checker_command
    row["artifact"] = "checker-output.txt"
    row["module_digest"] = (
        "sha256:" + hashlib.sha256(module.read_bytes()).hexdigest()
    )
    row["config_digest"] = (
        "sha256:" + hashlib.sha256(config.read_bytes()).hexdigest()
    )
    row["refinement_tests"] = ["tests/test_ref.py::test_case"]
    row["summary"] = "prop.safety proved"
    (reports / "checker-output.txt").write_text("ok\n", encoding="utf-8")
    formal_report = reports / "formal-report.yaml"
    formal_report.write_text(yaml.safe_dump(skeleton), encoding="utf-8")
    # Same deferral as the graph-report ingest: the row persists, the ledger
    # write queues behind an unconfigured DSN.
    assert main([
        "pact", "--root", str(root), "evidence", "ingest", "scratch",
        "--formal-report", str(formal_report), "--json",
    ]) == 3
    assert _json_out(capsys)["verdict"] == "deferred"
    manifest = load_manifest(root, "scratch")
    formal_rows = [
        ev for ev in manifest.evidence
        if ev.kind in {"formal", "formal_check"}
        and "formal.prop-safety" in ev.refs
    ]
    assert len(formal_rows) == 1
    assert formal_rows[0].status == "pass"

    rc, post = _post_edit_full(root, "scratch", capsys)
    # Deferred for the same reason as the ingest above: the manifest is clean,
    # the intent registration has no ledger to reach.
    assert rc == 3 and post["verdict"] == "deferred"
    assert post["validation"]["errors"] == []

    # Unknown kind: loud error carrying the canonical kind list from the
    # registry (code defaults + project.yaml extensions).
    assert main([
        "pact", "--root", str(root), "evidence", "template",
        "--kind", "nonsense", "--json",
    ]) == 1
    error = _json_out(capsys)["error"]
    assert "unknown evidence kind" in error["message"]
    kinds = error["data"]["canonical_kinds"]
    assert "graph_report" in kinds
    assert "formal_check" in kinds
    assert "executed_test" in kinds
