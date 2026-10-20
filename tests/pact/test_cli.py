from __future__ import annotations

import json
import subprocess
from pathlib import Path
from threading import Event, Thread

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.errors import SubstrateError
from okg.substrate.pact import cli as pact_cli
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.models import dump_manifest
from okg.substrate.pact.store import load_manifest
from okg.substrate.pact.validation import validate_manifest
from tests.pact.conftest import ingest_passing_named_plan


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


def _seed_v3_root(tmp_path: Path) -> Path:
    root = _seed_root(tmp_path)
    (root / "pact" / "project.yaml").write_text(
        "pact_version: 3\n"
        "project: okg\n"
        "pact_defaults:\n"
        "  artifact_format_version: 3\n"
        "  target_pact_version: 3\n",
        encoding="utf-8",
    )
    return root


def _json_out(capsys):
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_new_change_templates_validate(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)

    for template in ("feature", "bug", "protocol", "migration"):
        change_id = f"demo-{template}"
        rc = main(
            [
                "pact",
                "--root",
                str(root),
                "new-change",
                change_id,
                "--template",
                template,
                "--json",
            ]
        )
        assert rc == 0
        assert (root / "pact" / "changes" / change_id / "pact.yaml").is_file()

        report = validate_manifest(load_manifest(root, change_id))
        assert report.ok


def _write_local_pact_template(root: Path, name: str) -> Path:
    template = root / "pact" / "templates" / name
    template.mkdir(parents=True)
    (template / "pact.yaml").write_text(
        yaml.safe_dump(
            {
                "pact_version": 4,
                "target_pact_version": 4,
                "change": {
                    "id": "analysis-template",
                    "title": "Analysis template title",
                    "status": "proposal",
                    "tier": 2,
                    "rationale": "repository analysis scaffold",
                    "method_metadata": {
                        "pact_shape": "analysis_task",
                        "method": "profile-fit",
                    },
                },
                "requirements": [],
                "tasks": [],
                "dependencies": [],
                "test_plan": [],
                "formal": [],
                "waivers": [],
                "evidence": [],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (template / "README.md").write_bytes(b"analysis instructions\n")
    assets = template / "assets"
    assets.mkdir()
    (assets / "fixture.bin").write_bytes(b"\x00\x01analysis\xff")
    return template


def test_new_change_materializes_repo_local_template(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    template = _write_local_pact_template(root, "analysis-task")

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "new",
            "mass-fit",
            "--template",
            "analysis-task",
            "--title",
            "Fit the mass peak",
            "--json",
        ]
    )

    assert rc == 0
    _json_out(capsys)
    manifest = load_manifest(root, "mass-fit")
    assert manifest.change.id == "mass-fit"
    assert manifest.change.title == "Fit the mass peak"
    assert manifest.change.rationale == "repository analysis scaffold"
    assert manifest.change.method_metadata == {
        "pact_shape": "analysis_task",
        "method": "profile-fit",
    }
    destination = root / "pact" / "changes" / "mass-fit"
    assert (destination / "README.md").read_bytes() == b"analysis instructions\n"
    assert (destination / "assets" / "fixture.bin").read_bytes() == (
        template / "assets" / "fixture.bin"
    ).read_bytes()


def test_new_help_discovers_templates_from_explicit_root(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    _write_local_pact_template(root, "analysis-task")

    with pytest.raises(SystemExit) as exc:
        main(["pact", "--root", str(root), "new", "--help"])

    assert exc.value.code == 0
    assert "analysis-task" in capsys.readouterr().out


@pytest.mark.parametrize(
    "template_name",
    ["../analysis-task", "/tmp/analysis-task", ".hidden", "nested/template"],
)
def test_new_rejects_unsafe_local_template_names_without_partial_change(
    tmp_path: Path,
    capsys,
    template_name: str,
) -> None:
    root = _seed_root(tmp_path)
    change_id = "unsafe-template"

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "new",
            change_id,
            "--template",
            template_name,
            "--json",
        ]
    )

    assert rc == 1
    payload = _json_out(capsys)
    assert payload["error"]["code"] == "substrate_error"
    assert not (root / "pact" / "changes" / change_id).exists()


def test_new_rejects_symlinked_template_content_without_partial_change(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    template = _write_local_pact_template(root, "analysis-task")
    (template / "escape").symlink_to(tmp_path / "outside")

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "new",
            "symlink-template",
            "--template",
            "analysis-task",
            "--json",
        ]
    )

    assert rc == 1
    assert "symlink" in str(_json_out(capsys))
    assert not (root / "pact" / "changes" / "symlink-template").exists()


def test_new_rejects_invalid_local_manifest_without_partial_change(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    template = root / "pact" / "templates" / "broken"
    template.mkdir(parents=True)
    (template / "pact.yaml").write_text("- not\n- a mapping\n", encoding="utf-8")

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "new",
            "broken-template",
            "--template",
            "broken",
            "--json",
        ]
    )

    assert rc == 1
    _json_out(capsys)
    assert not (root / "pact" / "changes" / "broken-template").exists()


def test_new_atomic_publish_failure_leaves_no_partial_destination(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    _write_local_pact_template(root, "analysis-task")
    real_replace = Path.replace

    def fail_atomic_publish(path: Path, target: Path):
        if path.parent == root / "pact" / ".template-staging":
            assert not target.exists()
            raise OSError("induced atomic template publish failure")
        return real_replace(path, target)

    monkeypatch.setattr(Path, "replace", fail_atomic_publish)

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "new",
            "partial-template",
            "--template",
            "analysis-task",
            "--json",
        ]
    )

    assert rc == 1
    payload = _json_out(capsys)
    assert payload["error"]["data"]["exception_type"] == "OSError"
    assert not (root / "pact" / "changes" / "partial-template").exists()
    assert not any((root / "pact" / ".template-staging").iterdir())


def test_builtin_template_wins_repo_local_name_collision(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    _write_local_pact_template(root, "feature")

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "new",
                "builtin-wins",
                "--template",
                "feature",
                "--json",
            ]
        )
        == 0
    )
    _json_out(capsys)

    manifest = load_manifest(root, "builtin-wins")
    assert manifest.change.rationale == "feature change"
    assert manifest.change.method_metadata == {}


def test_agent_workflow_primitives(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)

    assert (
        main(["pact", "--root", str(root), "view", "demo", "--format=status", "--json"])
        == 0
    )
    status = _json_out(capsys)
    assert status["next"][0]["id"] == "task.demo"

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "demo",
                "--for-agent",
                "--json",
            ]
        )
        == 0
    )
    show = _json_out(capsys)
    assert show["requirements"][0]["id"] == "req.demo"

    assert (
        main(["pact", "--root", str(root), "view", "demo", "--format=next", "--json"])
        == 0
    )
    nxt = _json_out(capsys)
    assert nxt["next"][0]["why"]

    # `explain` deleted per `simplify-pact-surface` task 3.
    # `show` covers the same projection for a change; for a specific
    # task / requirement ID, agents read pact.yaml directly. The
    # underlying helper exists in operations / cli helpers for a
    # future `view <id>` builder.


def test_view_renders_each_projection(tmp_path: Path, capsys) -> None:
    """`view --format=X` renders each PACT projection, folding the deleted read
    verbs (status/brief/list/show/next/render/`review packet`).

    The equivalence to those deprecated verbs was proven while they still
    existed (see the build commits); now that they are deleted, `view` is the
    only surface, so this asserts each projection directly.
    """
    root = _seed_root(tmp_path)
    _build_demo(root)

    def _call(argv: list[str]) -> dict:
        assert main(["pact", "--root", str(root), *argv, "--json"]) == 0
        return _json_out(capsys)

    # default (manifest) — folds `show`. The raw dump keeps the TOP LEVEL it
    # had before the envelope; the envelope keys are additive, so
    # `payload["change"]` is still the change object and not its id.
    manifest = _call(["view", "demo"])
    assert manifest["format"] == "manifest"
    assert manifest["ok"] is True and manifest["verdict"] == "recorded"
    assert manifest["change"]["id"] == "demo"
    assert manifest["requirements"][0]["id"] == "req.demo"
    assert _call(["view", "demo", "--for-agent"])["requirements"][0]["id"] == "req.demo"
    # --format=status — folds `status`
    assert _call(["view", "demo", "--format=status"])["next"][0]["id"] == "task.demo"
    # --format=brief — folds `brief`
    assert "operator_brief" in _call(["view", "demo", "--format=brief"])
    # --format=chat-review — chat-native human packet
    assert _call(["view", "demo", "--format=chat-review"])["format"] == "chat-review"
    # --format=next — folds `next`
    assert _call(["view", "demo", "--format=next"])["next"][0]["id"] == "task.demo"
    # --format=markdown — folds `render` (writes pact.md)
    md = _call(["view", "demo", "--format=markdown"])
    assert md["path"].endswith("pact.md")
    assert md["source_digest"].startswith("sha256:")
    # --format=packet — folds `review packet`, both reviewer kinds
    assert (
        _call(["view", "demo", "--format=packet", "--reviewer-kind=ai"])[
            "reviewer_kind"
        ]
        == "ai"
    )
    assert (
        _call(["view", "demo", "--format=packet", "--reviewer-kind=human"])[
            "reviewer_kind"
        ]
        == "human"
    )
    # --format=list (no id) — folds `list`
    assert any(
        c["change"] == "demo" for c in _call(["view", "--format=list"])["changes"]
    )
    # --format=chat-review-queue (no id) — chat-native decision queue
    assert (
        _call(["view", "--format=chat-review-queue"])["format"] == "chat-review-queue"
    )


def test_view_requires_change_id_except_list(tmp_path: Path) -> None:
    """`view` without an id is valid only for `--format=list`."""
    root = _seed_root(tmp_path)
    _build_demo(root)
    # no id + a per-change format -> nonzero exit
    assert main(["pact", "--root", str(root), "view", "--format=status", "--json"]) != 0
    # no id + list -> ok
    assert main(["pact", "--root", str(root), "view", "--format=list", "--json"]) == 0


def test_new_alias_dispatches(tmp_path: Path, capsys) -> None:
    """`okg pact new <id>` (the canonical name) resolves.

    Per `complete-pact-surface-cut` req.new-shape-and-alias: `new` was
    registered as an alias but dispatch keyed only on "new-change", so the
    documented `okg pact new` failed with "unknown pact subcommand". Both
    names now dispatch.
    """
    root = _seed_root(tmp_path)
    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "new",
            "demo-new",
            "--template",
            "feature",
            "--json",
        ]
    )
    assert rc == 0
    payload = _json_out(capsys)
    assert payload["ok"] is True
    assert (root / "pact" / "changes" / "demo-new" / "pact.yaml").is_file()
    # the v0 alias still works too
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "new-change",
                "demo-legacy",
                "--template",
                "feature",
                "--json",
            ]
        )
        == 0
    )


def test_new_shape_fast_creates_fast_path_pact(tmp_path: Path, capsys) -> None:
    """`new --shape=fast` authors a compact fast-path PACT (folds the deleted
    `fast-path new`), carrying the intent / acceptance / task / test args
    through to the manifest."""
    root = _seed_root(tmp_path)
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "new",
                "fp",
                "--shape=fast",
                "--title",
                "Fast demo",
                "--intent",
                "It must do the thing.",
                "--acceptance",
                "the thing is done",
                "--task-title",
                "Do the thing",
                "--test-id",
                "tests/pact/test_cli.py::test_new_shape_fast_creates_fast_path_pact",
                "--json",
            ]
        )
        == 0
    )
    payload = _json_out(capsys)
    assert payload["pact_shape"] == "fast_path"
    assert payload["requirements"] == 1
    assert payload["tasks"] == 1
    assert payload["test_plan"] == 1

    manifest = load_manifest(root, "fp")
    assert manifest.change.method_metadata.get("pact_shape") == "fast_path"
    assert manifest.requirements[0].statement == "It must do the thing."
    assert manifest.tasks[0].title == "Do the thing"


def test_new_shape_fast_scaffolds_placeholders(tmp_path: Path) -> None:
    """Per right-size-pact-authoring req.rightsize.tiers: bare `new` (and
    `--shape=fast` without the fast-path args) emits a fast-shape manifest
    with TODO placeholders instead of refusing."""
    root = _seed_root(tmp_path)
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "new",
                "fp-missing",
                "--shape=fast",
                "--json",
            ]
        )
        == 0
    )
    manifest = load_manifest(root, "fp-missing")
    assert len(manifest.requirements) == 1
    assert len(manifest.tasks) == 1
    assert "TODO" in manifest.requirements[0].statement


def test_evidence_namespace_records_manifest_entries(tmp_path: Path, capsys) -> None:
    """`evidence {attach,waive,bind-tla,approve}` record the right manifest
    entries, folding the deleted attach-evidence / `waiver request` / bind-tla /
    `review approve`. Their parity to those verbs was proven while they existed
    (build commits); now `evidence` is the only surface, so this asserts the
    resulting entries directly.
    """
    root = _seed_root(tmp_path)
    _build_demo(root)
    (root / "specs").mkdir()
    (root / "specs" / "Demo.tla").write_text(
        "---- MODULE Demo ----\n====\n",
        encoding="utf-8",
    )
    (root / "specs" / "Demo.cfg").write_text(
        "INVARIANT Safety\n",
        encoding="utf-8",
    )
    (root / "tests" / "pact").mkdir(parents=True)
    (root / "tests" / "pact" / "test_cli.py").write_text(
        "def test_evidence_namespace_records_manifest_entries():\n    pass\n",
        encoding="utf-8",
    )

    def _run(argv: list[str], *, expect: str = "recorded") -> None:
        rc = main(["pact", "--root", str(root), *argv, "--json"])
        payload = _json_out(capsys)
        assert payload["verdict"] == expect, payload
        assert rc == {"recorded": 0, "deferred": 3}[expect]

    def _last(key: str) -> dict:
        return dict(dump_manifest(load_manifest(root, "demo"))[key][-1])

    # A manual passing pytest claim is refused because it carries no JUnit
    # identity. A non-passing observation still records through attach.
    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "evidence",
            "attach",
            "demo",
            "req.demo",
            "--kind",
            "pytest",
            "--ref",
            "tests/x.py::t",
            "--status",
            "pass",
            "--summary",
            "s",
            "--json",
        ]
    )
    refused = _json_out(capsys)
    assert rc == 1
    assert refused["verdict"] == "refused"
    _run(
        [
            "evidence",
            "attach",
            "demo",
            "req.demo",
            "--kind",
            "pytest",
            "--ref",
            "tests/x.py::t",
            "--status",
            "missing",
            "--summary",
            "s",
        ],
        expect="deferred",
    )
    attached = _last("evidence")
    assert attached["kind"] == "pytest"
    assert attached["status"] == "missing"
    assert "req.demo" in attached["refs"]

    # waive
    _run(
        [
            "evidence",
            "waive",
            "demo",
            "--scope",
            "req.demo",
            "--reason",
            "blocked upstream",
        ]
    )
    assert _last("waivers")["scope"] == "req.demo"

    # bind-tla (a formal binding requires >=1 refinement test)
    _run(
        [
            "evidence",
            "bind-tla",
            "demo",
            "--requirement",
            "req.demo",
            "--model",
            "specs/Demo.tla",
            "--property-id",
            "Safety",
            "--config",
            "specs/Demo.cfg",
            "--checker-command",
            "tlc2.TLC -config specs/Demo.cfg specs/Demo.tla",
            "--refinement-test",
            "tests/pact/test_cli.py::test_evidence_namespace_records_manifest_entries",
        ]
    )
    assert _last("formal")["property_id"] == "Safety"

    # approve (intent approval requires a fresh review view)
    _run(["view", "demo", "--format=markdown"])
    _run(
        ["evidence", "approve", "demo", "--reviewer", "alice", "--decision", "approved"]
    )
    assert _last("evidence")["kind"] == "intent_review"


def test_v3_gate_task_done_marks_on_success(tmp_path: Path, capsys) -> None:
    """`gate task-done` marks the task done on a passing check (folds `task
    done`) and refuses on missing proof (complete-pact-surface-cut
    req.gate-lifecycle).
    """
    # v4 closure is decision-ledger-derived and is covered by
    # test_deferred_closure.py; this retains the v3 compatibility contract.
    root = _seed_v3_root(tmp_path)
    _build_demo(root)

    def _call(argv: list[str]) -> dict:
        main(["pact", "--root", str(root), *argv, "--json"])
        return _json_out(capsys)

    # Current PACT requires report-backed requirement + acceptance proof.
    ingest_passing_named_plan(root, "demo")
    result = _call(["gate", "task-done", "demo", "task.demo"])
    assert result["ok"] is True, result
    assert result["marked"] is True
    assert result["task_status"] == "done"

    # task.demo is now done -> no longer surfaced as a next task.
    status = _call(["view", "demo", "--format=status"])
    assert all(t["id"] != "task.demo" for t in status.get("next", []))


def test_gate_task_done_refuses_missing_proof(tmp_path: Path, capsys) -> None:
    """`gate task-done` refuses (ok False, not marked) when proof is missing."""
    root = _seed_root(tmp_path)
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "new",
                "needsproof",
                "--template",
                "feature",
                "--json",
            ]
        )
        == 0
    )
    _json_out(capsys)
    # Add a requirement requiring real pytest evidence + a task bound to it,
    # then complete-gate with no evidence attached.
    pact_operations.add_requirement(
        root,
        "needsproof",
        req_id="req.np",
        title="Needs proof",
        statement="Must be proven.",
        evidence=["pytest"],
    )
    pact_operations.add_task(
        root,
        "needsproof",
        task_id="task.np",
        title="Prove it",
        verification="pytest tests/pact/test_cli.py -q",
        requirements=["req.np"],
    )
    main(
        [
            "pact",
            "--root",
            str(root),
            "gate",
            "task-done",
            "needsproof",
            "task.np",
            "--json",
        ]
    )
    result = _json_out(capsys)
    assert result["ok"] is False
    assert result["marked"] is False
    assert result["task_status"] != "done"


def test_pre_commit_gate_refuses_tracked_generated_pact_artifacts(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    (root / ".gitignore").write_text(
        "\n".join(
            [
                "pact/changes/*/pact.md",
                "pact/archive/*/pact.md",
                "pact/changes/*/pact.yaml.lock",
                "pact/archive/*/pact.yaml.lock",
                "",
            ]
        ),
        encoding="utf-8",
    )

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "demo",
                "--format=markdown",
                "--json",
            ]
        )
        == 0
    )
    _json_out(capsys)
    for rel_path in [
        "pact/archive/demo/pact.md",
        "pact/changes/demo/pact.yaml.lock",
        "pact/archive/demo/pact.yaml.lock",
    ]:
        path = root / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("generated\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(root), "init"], check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "add",
            "-f",
            "pact/changes/demo/pact.md",
            "pact/archive/demo/pact.md",
            "pact/changes/demo/pact.yaml.lock",
            "pact/archive/demo/pact.yaml.lock",
        ],
        check=True,
        capture_output=True,
    )

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "gate",
            "pre-commit",
            "demo",
            "--json",
        ]
    )
    result = _json_out(capsys)

    assert rc != 0
    assert result["ok"] is False
    guard = result["generated_review_view_guard"]
    assert guard["status"] == "tracked_generated_pact_artifact"
    assert guard["tracked_paths"] == [
        "pact/archive/demo/pact.md",
        "pact/archive/demo/pact.yaml.lock",
        "pact/changes/demo/pact.md",
        "pact/changes/demo/pact.yaml.lock",
    ]
    assert "git rm --cached pact/changes/demo/pact.md" in result["recommended_commands"]


def test_pre_commit_gate_allows_untracked_generated_pact_md(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    (root / ".gitignore").write_text("pact/changes/*/pact.md\n", encoding="utf-8")

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "demo",
                "--format=markdown",
                "--json",
            ]
        )
        == 0
    )
    _json_out(capsys)
    subprocess.run(["git", "-C", str(root), "init"], check=True, capture_output=True)

    rc = main(
        [
            "pact",
            "--root",
            str(root),
            "gate",
            "pre-commit",
            "demo",
            "--json",
        ]
    )
    result = _json_out(capsys)

    assert rc == 0
    assert result["ok"] is True
    assert result["generated_review_view_guard"] == {
        "ok": True,
        "status": "clean",
        "tracked_paths": [],
    }


def test_gate_pre_implementation_refuses_legacy_approval_for_v4(
    tmp_path: Path, capsys
) -> None:
    """V4 proposal authority is the digest-bound decision ledger.

    Legacy ``intent_review`` evidence remains readable history, but cannot
    silently satisfy the proposal boundary or mutate authored status.
    """
    root = _seed_root(tmp_path)
    _build_demo(root, tier=2)

    def _call(argv: list[str]) -> dict:
        main(["pact", "--root", str(root), *argv, "--json"])
        return _json_out(capsys)

    # Before approval: refuse, no advance, status stays proposal.
    refused = _call(["gate", "pre-implementation", "demo"])
    assert refused["ok"] is False
    assert refused["advanced"] is False
    assert refused["status"] == "proposal"

    # A legacy review-view approval does not replace the v4 ledger decision.
    _call(["view", "demo", "--format=markdown"])
    _call(
        ["evidence", "approve", "demo", "--reviewer", "alice", "--decision", "approved"]
    )
    still_refused = _call(["gate", "pre-implementation", "demo"])
    assert still_refused["ok"] is False
    assert still_refused["advanced"] is False
    assert still_refused["authored_status_unchanged"] is True
    assert still_refused["status"] == "proposal"
    assert still_refused["state"] == "proposed"
    assert still_refused["blocked_by"] == ["proposal_decision_required"]


def test_gate_graph_freshness_refresh_folds_sync(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    """`gate graph-freshness --refresh` performs the projection refresh that the
    deprecated `sync` did. In --dry-run it returns the refresh plan without
    running a real ingest.
    """
    # The graph surfaces read the repository's configured PACT graph
    # deployment; this test configures the one it exercises (#2823).
    monkeypatch.setenv("OKG_PACT_GRAPH_DEPLOYMENT", "okg-workspace")
    root = _seed_root(tmp_path)
    monkeypatch.setenv("OKG_WORKSPACE_SYNC_ROOT", str(root))
    _build_demo(root)

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "gate",
                "graph-freshness",
                "demo",
                "--refresh",
                "--dry-run",
                "--json",
            ]
        )
        == 0
    )
    plan = _json_out(capsys)
    assert plan["source"] == "pact"
    assert plan["watch_roots"] == ["pact"]
    assert "pact/changes/demo/pact.yaml" in plan["sync_scope_keys"]
    assert any("--include pact" in command for command in plan["commands"])


def test_gate_graph_freshness_refresh_refuses_foreign_checkout(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)

    def foreign_checkout(*_args, **_kwargs):
        return {
            "allowed": False,
            "mode": "foreign_checkout",
            "reason": "unit test foreign checkout",
            "legal_moves": ["merge this branch, then publish from the sync root"],
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_authority.check_publish_provenance",
        foreign_checkout,
    )

    plan = pact_cli._sync_pact_projection(
        root,
        "demo",
        deployment="okg-workspace",
        dry_run=True,
    )

    assert plan["ok"] is False
    assert plan["status"] == "refused"
    assert plan["provenance"]["mode"] == "foreign_checkout"
    assert plan["ingest"]["skipped"] is True
    assert not any("--include pact" in command for command in plan["commands"])
    assert any("pre-merge demo" in command for command in plan["commands"])


def test_archived_pact_views_and_graph_refresh_use_archive_manifest(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    # The graph surfaces read the repository's configured PACT graph
    # deployment; this test configures the one it exercises (#2823).
    monkeypatch.setenv("OKG_PACT_GRAPH_DEPLOYMENT", "okg-workspace")
    root = _seed_root(tmp_path)
    monkeypatch.setenv("OKG_WORKSPACE_SYNC_ROOT", str(root))
    _build_demo(root)
    archive_dir = root / "pact" / "archive"
    archive_dir.mkdir(parents=True)
    (root / "pact" / "changes" / "demo").rename(archive_dir / "demo")

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "view",
                "demo",
                "--format=status",
                "--json",
            ]
        )
        == 0
    )
    status = _json_out(capsys)
    assert status["change"] == "demo"
    assert status["human_review"].endswith("pact/archive/demo/pact.md")

    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "gate",
                "graph-freshness",
                "demo",
                "--refresh",
                "--dry-run",
                "--json",
            ]
        )
        == 0
    )
    plan = _json_out(capsys)
    assert "pact/archive/demo/pact.yaml" in plan["sync_scope_keys"]
    assert "pact/archive/demo/pact.md" in plan["sync_scope_keys"]
    assert "pact/changes/demo/pact.yaml" not in plan["sync_scope_keys"]


def test_short_edit_verbs_update_manifest(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)

    # A dependency referencing a not-yet-declared task is rejected.
    with pytest.raises(SubstrateError):
        pact_operations.add_dependency(root, "demo", "task.demo", "task.second")

    pact_operations.add_task(
        root,
        "demo",
        task_id="task.second",
        title="Second",
        verification="pytest tests/pact/test_cli.py -q",
        requirements=["req.demo"],
    )
    pact_operations.add_dependency(root, "demo", "task.demo", "task.second")
    pact_operations.update_task_status(
        root,
        "demo",
        "task.second",
        status="blocked",
        reason="waiting for task.demo",
    )
    pact_operations.request_waiver(
        root,
        "demo",
        waiver_id=None,
        scope="dependencies",
        reason="temporary bootstrap",
        owner="owner@example.com",
        expires=None,
    )
    proof = ingest_passing_named_plan(root, "demo")
    evidence_ids = [row.id for row in proof.evidence]
    pact_operations.update_task_status(
        root,
        "demo",
        "task.demo",
        status="done",
        evidence_refs=evidence_ids,
    )

    data = yaml.safe_load(
        (root / "pact" / "changes" / "demo" / "pact.yaml").read_text()
    )
    assert data["dependencies"][0]["from"] == "task.demo"
    assert data["tasks"][0]["status"] == "done"
    assert data["tasks"][1]["status"] == "blocked"
    assert data["waivers"]
    assert data["evidence"]
    assert "edits" not in data


def test_task_done_accepts_multiple_evidence_refs(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)

    for stem in ("first", "second"):
        ingest_passing_named_plan(root, "demo", stem=stem)

    manifest_path = root / "pact" / "changes" / "demo" / "pact.yaml"
    data = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    evidence_ids = [item["id"] for item in data["evidence"]]

    # update_task_status dedups repeated evidence refs (folds `task done`).
    pact_operations.update_task_status(
        root,
        "demo",
        "task.demo",
        status="done",
        evidence_refs=[evidence_ids[0], evidence_ids[0], evidence_ids[1]],
    )

    data = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    assert data["tasks"][0]["status"] == "done"
    assert data["tasks"][0]["evidence"] == evidence_ids
    assert "edits" not in data

    assert validate_manifest(load_manifest(root, "demo")).ok


def test_change_update_sets_change_metadata(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)

    pact_operations.update_change_metadata(
        root,
        "demo",
        title="Updated Demo",
        rationale="Use a command for routine metadata.",
        root_cause="The typed metadata had no CLI mutation path.",
        affected_areas=["pact", "agent-workflow"],
        affected_code=[
            "src/okg/substrate/pact/cli.py",
            "src/okg/substrate/pact/operations.py",
        ],
        migration_notes="Existing manifests remain valid.",
    )

    data = yaml.safe_load(
        (root / "pact" / "changes" / "demo" / "pact.yaml").read_text()
    )
    change = data["change"]
    assert change["title"] == "Updated Demo"
    assert change["rationale"] == "Use a command for routine metadata."
    assert change["root_cause"] == "The typed metadata had no CLI mutation path."
    assert change["affected_areas"] == ["pact", "agent-workflow"]
    assert change["affected_code"] == [
        "src/okg/substrate/pact/cli.py",
        "src/okg/substrate/pact/operations.py",
    ]
    assert change["migration_notes"] == "Existing manifests remain valid."
    assert "edits" not in data


def test_change_update_requires_a_metadata_field(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)

    with pytest.raises(SubstrateError) as exc:
        pact_operations.update_change_metadata(root, "demo")
    assert "requires at least one metadata field" in str(exc.value)


def test_requirement_detail_commands_update_scenarios_and_acceptance(
    tmp_path: Path,
    capsys,
) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)

    pact_operations.upsert_requirement_scenario(
        root,
        "demo",
        "req.demo",
        scenario_id="scn.demo.review",
        title="review-ready requirement",
        given=["an agent is authoring a PACT requirement"],
        when=["the scenario command runs"],
        then=["the requirement contains concrete behavior"],
        and_=["the default boilerplate scenario is replaced"],
    )
    pact_operations.upsert_requirement_acceptance(
        root,
        "demo",
        "req.demo",
        acceptance_id="acc.demo.review",
        description="The generated review view shows the concrete scenario.",
        evidence=["executed_test"],
    )

    data = yaml.safe_load(
        (root / "pact" / "changes" / "demo" / "pact.yaml").read_text()
    )
    req = data["requirements"][0]
    assert req["scenarios"] == [
        {
            "id": "scn.demo.review",
            "title": "review-ready requirement",
            "given": ["an agent is authoring a PACT requirement"],
            "when": ["the scenario command runs"],
            "then": ["the requirement contains concrete behavior"],
            "and": ["the default boilerplate scenario is replaced"],
        }
    ]
    assert req["acceptance"] == [
        {
            "id": "acc.demo.review",
            "description": "The generated review view shows the concrete scenario.",
            "evidence": ["executed_test"],
        }
    ]
    assert "edits" not in data


def test_apply_edit_serializes_parallel_manifest_writes(tmp_path: Path) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    first_started = Event()
    first_release = Event()
    second_done = Event()
    errors: list[BaseException] = []

    def first_mutate(manifest) -> None:
        first_started.set()
        assert first_release.wait(timeout=2)
        manifest.change.affected_areas.append("first")

    def second_mutate(manifest) -> None:
        manifest.change.affected_areas.append("second")

    def run_first() -> None:
        try:
            pact_operations.apply_edit(
                root,
                "demo",
                command="test first",
                affected_ids=["first"],
                mutate=first_mutate,
            )
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    def run_second() -> None:
        try:
            pact_operations.apply_edit(
                root,
                "demo",
                command="test second",
                affected_ids=["second"],
                mutate=second_mutate,
            )
            second_done.set()
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    first = Thread(target=run_first)
    second = Thread(target=run_second)
    first.start()
    assert first_started.wait(timeout=2)
    second.start()
    assert not second_done.wait(timeout=0.15)
    first_release.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert not errors
    assert second_done.is_set()
    manifest = load_manifest(root, "demo")
    assert set(manifest.change.affected_areas) >= {"first", "second"}


def test_edit_plan_apply_serializes_stale_replay_check(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    pact_operations.add_task(
        root,
        "demo",
        task_id="task.second",
        title="Second",
        verification="pytest tests/pact/test_cli.py -q",
        requirements=["req.demo"],
    )
    plan = pact_operations.save_edit_plan(
        root,
        "demo",
        operations=[
            {
                "op": "dep_add",
                "from": "task.demo",
                "to": "task.second",
            }
        ],
    )
    first_apply_started = Event()
    first_apply_release = Event()
    second_done = Event()
    errors: list[BaseException] = []
    original_apply_operation = pact_operations._apply_operation

    def slow_first_apply(manifest, op) -> None:
        if not first_apply_started.is_set():
            first_apply_started.set()
            assert first_apply_release.wait(timeout=2)
        original_apply_operation(manifest, op)

    monkeypatch.setattr(
        pact_operations,
        "_apply_operation",
        slow_first_apply,
    )

    def apply_plan() -> None:
        try:
            pact_operations.apply_edit_plan(root, plan["plan_id"])
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    def apply_plan_and_mark_done() -> None:
        try:
            pact_operations.apply_edit_plan(root, plan["plan_id"])
            second_done.set()
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    first = Thread(target=apply_plan)
    second = Thread(target=apply_plan_and_mark_done)
    first.start()
    assert first_apply_started.wait(timeout=2)
    second.start()
    assert not second_done.wait(timeout=0.15)
    first_apply_release.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert not errors
    assert second_done.is_set()
    data = yaml.safe_load(
        (root / "pact" / "changes" / "demo" / "pact.yaml").read_text()
    )
    dependencies = [
        row
        for row in data["dependencies"]
        if row["from"] == "task.demo" and row["to"] == "task.second"
    ]
    assert len(dependencies) == 1


# `test_loop_record_command_attaches_loop_evidence` and
# `test_loop_record_no_render_skips_review_view` deleted per
# `simplify-pact-surface` task 3: the `loop record` verb was removed
# in favor of the general "write a PACT, attach evidence, commit"
# flow. The PACT self-improvement principle that the loop ceremony
# tried to encode lives in `pact/AGENTS.md` "PACT self-improvement"
# as a single paragraph: prefer subtraction over addition.


def test_edit_plan_token_apply_exact_plan(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    pact_operations.add_task(
        root,
        "demo",
        task_id="task.second",
        title="Second",
        verification="pytest tests/pact/test_cli.py -q",
        requirements=["req.demo"],
    )

    plan = pact_operations.save_edit_plan(
        root,
        "demo",
        operations=[{"op": "dep_add", "from": "task.demo", "to": "task.second"}],
    )
    assert plan["plan_id"]

    pact_operations.apply_edit_plan(root, plan["plan_id"])
    # Idempotent replay: re-applying the saved plan is a no-op.
    replay = pact_operations.apply_edit_plan(root, plan["plan_id"])
    deps = [(d.from_task, d.to_task) for d in replay.dependencies]
    assert deps.count(("task.demo", "task.second")) == 1


def test_v4_edit_plan_rejects_unapplied_plan_after_unrelated_mutation(
    tmp_path: Path,
) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    pact_operations.add_task(
        root,
        "demo",
        task_id="task.second",
        title="Second",
        verification="pytest tests/pact/test_cli.py -q",
        requirements=["req.demo"],
    )
    plan = pact_operations.save_edit_plan(
        root,
        "demo",
        operations=[{"op": "dep_add", "from": "task.demo", "to": "task.second"}],
    )
    pact_operations.update_change_metadata(
        root, "demo", affected_areas=["unrelated-later-edit"]
    )

    with pytest.raises(SubstrateError, match="edit plan is stale"):
        pact_operations.apply_edit_plan(root, plan["plan_id"])

    manifest = load_manifest(root, "demo")
    assert manifest.change.affected_areas == ["unrelated-later-edit"]
    assert manifest.dependencies == []


def test_v4_edit_plan_replay_preserves_unrelated_later_mutation(
    tmp_path: Path,
) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)
    pact_operations.add_task(
        root,
        "demo",
        task_id="task.second",
        title="Second",
        verification="pytest tests/pact/test_cli.py -q",
        requirements=["req.demo"],
    )
    plan = pact_operations.save_edit_plan(
        root,
        "demo",
        operations=[{"op": "dep_add", "from": "task.demo", "to": "task.second"}],
    )
    pact_operations.apply_edit_plan(root, plan["plan_id"])
    pact_operations.update_change_metadata(
        root, "demo", affected_areas=["unrelated-later-edit"]
    )

    replay = pact_operations.apply_edit_plan(root, plan["plan_id"])

    assert replay.change.affected_areas == ["unrelated-later-edit"]
    dependencies = [
        (dependency.from_task, dependency.to_task) for dependency in replay.dependencies
    ]
    assert dependencies.count(("task.demo", "task.second")) == 1


def test_direct_yaml_normalize_preserves_meaning(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _build_demo(root)

    manifest = pact_operations.normalize_manifest(root, "demo")
    assert manifest.change.id == "demo"


def _build_demo(root: Path, *, tier: int = 1) -> None:
    assert (
        main(
            [
                "pact",
                "--root",
                str(root),
                "new-change",
                "demo",
                "--template",
                "protocol",
                "--json",
            ]
        )
        == 0
    )
    manifest = load_manifest(root, "demo")
    manifest.change.tier = tier
    from okg.substrate.pact.store import save_manifest

    save_manifest(root, manifest)
    pact_operations.add_requirement(
        root,
        "demo",
        req_id="req.demo",
        title="Demo",
        statement="Demo must validate.",
        evidence=["planned_test"],
    )
    pact_operations.add_task(
        root,
        "demo",
        task_id="task.demo",
        title="Do demo",
        verification="pytest tests/pact/test_cli.py -q",
        requirements=["req.demo"],
    )
    pact_operations.require_test(
        root,
        "demo",
        "req.demo",
        "tests/pact/test_cli.py::test_agent_workflow_primitives",
    )
