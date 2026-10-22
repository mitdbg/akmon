"""Generated attach hints print the form that matches the change's pact_version.

Issue #3920. A pact_version 5 change takes
``evidence attach <id> --kind <v5 kind> --requirement <req> ...``; the legacy
``<id> <target> --kind <kind> --ref <ref> ...`` form is refused by it. The
repair hints (refused-gate ``next_commands``) and the `evidence template`
fix hint used to print the legacy form to every change.
"""

from __future__ import annotations

import shlex
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main as okg_main
from okg.substrate.errors import SubstrateError
from okg.substrate.pact import repair
from okg.substrate.pact.evidence_templates import evidence_template
from okg.substrate.pact.operations import evidence_status, task_completion_gate
from pact.schema import EVIDENCE_KINDS
from _v5_repo import MANIFEST, commit_all, make_repo, write_manifest

REPO_COMMAND = "uv run okg"
V5_KINDS = ("executed_test", "formal_check", "published_generation", "design_review", "manual")


@pytest.fixture
def v5_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = make_repo(tmp_path)
    (root / "pact").mkdir(exist_ok=True)
    (root / "pact" / "project.yaml").write_text(
        yaml.safe_dump({"command_authority": {"repo_command": REPO_COMMAND}}),
        encoding="utf-8",
    )
    write_manifest(root, dict(MANIFEST))
    commit_all(root, "add demo change")
    # The reroute looks for the manifest relative to the working directory.
    monkeypatch.chdir(root)
    return root


def _run_printed(command: str) -> int:
    argv = shlex.split(command)
    assert argv[:3] == ["uv", "run", "okg"], argv
    return okg_main(argv[3:])


def _evidence_rows(root: Path) -> list[dict]:
    path = root / "pact" / "changes" / "demo" / "pact.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8")).get("evidence") or []


def _assert_v5_form(command: str, *, test_id: str) -> None:
    argv = shlex.split(command)
    attach = argv.index("attach")
    positional = [
        token
        for index, token in enumerate(argv[attach + 1 :], start=attach + 1)
        if not token.startswith("--") and not argv[index - 1].startswith("--")
    ]
    assert positional == ["demo"], command
    options = {
        token: argv[index + 1]
        for index, token in enumerate(argv)
        if token.startswith("--") and index + 1 < len(argv)
    }
    assert options["--kind"] in V5_KINDS, command
    assert options["--requirement"] == "req.demo", command
    # The v5 gate counts any passing row as proof, so the hint names the check.
    assert options["--command"] == f"pytest {test_id}", command
    assert "--ref" not in argv and "--summary" not in argv, command
    assert repair.is_executable_command(command), command
    assert repair.fits_command_guards(command), command


def _backed_by_a_test_plan(root: Path, test_id: str):
    """The v5 manifest view with a test-plan row naming `test_id`.

    The read-only v5 adapter carries no test plan, so the plan is added to the
    adapted view to exercise the branch that has a real test to name.
    """
    from okg.substrate.pact.models import PactManifest
    from okg.substrate.pact.store import load_manifest

    document = load_manifest(root, "demo").model_dump(mode="json")
    document["test_plan"] = [
        {
            "id": "test.demo.probe",
            "requirement": "req.demo",
            "test_id": test_id,
            "evidence": ["pytest"],
        }
    ]
    manifest = PactManifest.model_validate(document)
    assert repair.is_v5_manifest(manifest)
    return manifest


def test_a_slot_backed_by_the_test_plan_prints_the_v5_form_with_its_command(
    v5_repo: Path,
) -> None:
    test_id = "tests/demo/test_probe.py::test_it"
    manifest = _backed_by_a_test_plan(v5_repo, test_id)

    commands = repair.proof_slot_commands(
        manifest,
        "demo",
        ["req.demo:pytest"],
        subject_requirements=["req.demo"],
        prefix=REPO_COMMAND,
    )

    assert len(commands) == 1, commands
    _assert_v5_form(commands[0], test_id=test_id)
    assert "--kind executed_test" in commands[0]


def test_the_printed_v5_command_runs_and_records_its_command(
    v5_repo: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    test_id = "tests/demo/test_probe.py::test_it"
    manifest = _backed_by_a_test_plan(v5_repo, test_id)
    (command,) = repair.proof_slot_commands(
        manifest,
        "demo",
        ["req.demo:executed_test"],
        subject_requirements=["req.demo"],
        prefix=REPO_COMMAND,
    )
    assert _evidence_rows(v5_repo) == []

    assert _run_printed(command) == 0, capsys.readouterr().out

    (row,) = _evidence_rows(v5_repo)
    assert row["requirement"] == "req.demo"
    assert row["kind"] == "executed_test"
    assert row["status"] == "pass"
    assert row["command"] == f"pytest {test_id}"


def test_a_v5_slot_with_no_real_test_prints_no_bare_pass_command(v5_repo: Path) -> None:
    """No test plan names the check, so no runnable attach is offered.

    A bare `--status pass` row is unbacked proof the v5 gate would accept.
    """
    for result in (
        task_completion_gate(v5_repo, "demo", "task.demo"),
        evidence_status(v5_repo, "demo"),
    ):
        assert result["ok"] is False
        assert not [c for c in result["next_commands"] if " evidence attach " in c], result[
            "next_commands"
        ]
        # The refusal says what to do instead.
        assert "--command" in result["why"] and "--artifact" in result["why"], result["why"]
    assert _evidence_rows(v5_repo) == []


def test_the_guidance_applies_to_v5_only(v5_repo: Path) -> None:
    from okg.substrate.pact.store import load_manifest

    v5 = load_manifest(v5_repo, "demo")
    assert repair.v5_unbacked_guidance(v5, []) is not None
    assert repair.v5_unbacked_guidance(v5, ["okg pact evidence attach demo"]) is None


def test_the_legacy_form_is_still_refused_by_a_v5_change(v5_repo: Path) -> None:
    """The reason the hint matters: the old text fails on a v5 change."""
    legacy = (
        f"{REPO_COMMAND} pact evidence attach demo req.demo --kind executed_test "
        "--ref tests/x.py --status pass --json"
    )
    with pytest.raises(SystemExit) as exit_info:
        _run_printed(legacy)
    assert exit_info.value.code != 0
    assert _evidence_rows(v5_repo) == []


def test_a_v4_change_keeps_the_v4_form(tmp_path: Path) -> None:
    from test_refusal_commands import _manifest, _write

    (tmp_path / "pact" / "changes" / "demo").mkdir(parents=True)
    (tmp_path / "pact" / "project.yaml").write_text(
        yaml.safe_dump({"command_authority": {"repo_command": REPO_COMMAND}}),
        encoding="utf-8",
    )
    _write(tmp_path, _manifest())

    result = task_completion_gate(tmp_path, "demo", "task.one")

    attach = [c for c in result["next_commands"] if " evidence attach " in c]
    assert attach
    for command in attach:
        assert "--ref " in command and "--requirement" not in command, command
        assert "--status pass" in command


@pytest.mark.parametrize(
    ("v4_kind", "v5_kind"),
    [
        ("pytest", "executed_test"),
        ("test", "executed_test"),
        ("planned_test", "executed_test"),
        ("executed_test", "executed_test"),
        ("formal", "formal_check"),
        ("graph", "published_generation"),
        ("graph_report", "published_generation"),
        ("review", "design_review"),
        ("manual", "manual"),
        ("benchmark_report", "manual"),
        ("ui_visual", "manual"),
    ],
)
def test_v4_kinds_map_to_a_valid_v5_kind(v4_kind: str, v5_kind: str) -> None:
    assert repair.v5_attach_kind(v4_kind) == v5_kind
    assert v5_kind in EVIDENCE_KINDS


def test_evidence_template_fix_hint_prints_the_v5_form() -> None:
    with pytest.raises(SubstrateError) as caught:
        evidence_template("design_review")
    hint = caught.value.fix_hint
    assert "--kind design_review" in hint
    assert "--requirement" in hint
    assert "--ref" not in hint and "--summary" not in hint
