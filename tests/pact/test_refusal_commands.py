"""`next_commands` is executable, guard-fitting, and state-legal.

`pact-self-repairing-refusals` req.refusal.executable. Three families:

* the COMMAND FUNNEL — prose can never reach `next_commands`;
* the FSM SWEEP — every (state, requested-verdict) pair either is legal here,
  or names an enabling path that REALLY reaches a state where it is legal, or
  is explicitly declared unreachable. Nothing in between;
* the PROOF-SLOT GENERATOR — every shape of `missing_proof`, including the
  composite acceptance-id form, produces the exact attach invocation that
  fills it, with the kind drawn from the requirement's own evidence policy.
"""

from __future__ import annotations

import shlex
from pathlib import Path

import pytest
import yaml

from okg.substrate.pact import fsm, repair
from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.operations import attach_evidence, task_completion_gate
from okg.substrate.pact.store import load_manifest

# --- fixtures --------------------------------------------------------------

REPO_COMMAND = "uv run okg"


def _manifest(change_id: str = "demo", *, acceptance: bool = False) -> dict:
    requirement: dict = {
        "id": "req.one",
        "title": "Requirement one",
        "statement": "The behavior named by req.one must hold.",
        "evidence_policy": {"required": ["executed_test"]},
        "scenarios": [],
        "acceptance": (
            [{"id": "acc.one", "description": "the probe passes", "evidence": []}]
            if acceptance
            else []
        ),
    }
    return {
        "pact_version": 4,
        "target_pact_version": 4,
        "change": {
            "id": change_id,
            "title": "Demo",
            "status": "implementation",
            "rationale": "Test fixture.",
            "root_cause": "Test fixture.",
        },
        "requirements": [requirement],
        "tasks": [
            {
                "id": "task.one",
                "title": "Task one",
                "status": "todo",
                "verification": "uv run pytest tests/demo/test_probe.py -q passes.",
                "requirements": ["req.one"],
                "evidence": [],
                "environment": [],
            }
        ],
        "dependencies": [],
        "test_plan": [
            {
                "id": "test.req-one.probe",
                "requirement": "req.one",
                "test_id": "tests/demo/test_probe.py",
                "evidence": ["pytest"],
            }
        ],
        "formal": [],
        "waivers": [],
        "evidence": [],
    }


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    (tmp_path / "pact" / "changes" / "demo").mkdir(parents=True)
    (tmp_path / "pact" / "project.yaml").write_text(
        yaml.safe_dump({"command_authority": {"repo_command": REPO_COMMAND}}),
        encoding="utf-8",
    )
    return tmp_path


def _write(root: Path, document: dict) -> PactManifest:
    path = root / "pact" / "changes" / document["change"]["id"] / "pact.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return load_manifest(root, document["change"]["id"])


# --- the command funnel ----------------------------------------------------


def test_prose_never_survives_the_command_funnel() -> None:
    """The exact strings the audit found at the head of the list."""
    prose = [
        "uv run pytest tests/bench/test_probe.py -q passes.",  # a verification clause
        "configure PACT decision ledger DSN",
        "git status",  # generic, but a real command: it survives
        "focused tests",
        "refresh coordination gate state for demo task.one",
        "okg pact evidence sync demo --json  # once the ledger is reachable",
    ]
    kept = repair.executable_commands(prose)
    assert kept == ["git status"]


def test_guards_are_the_repo_guards() -> None:
    assert repair.fits_command_guards("okg pact view x --format=status --json")
    assert not repair.fits_command_guards("a && b && c && d")
    assert not repair.fits_command_guards("okg pact view " + "x" * 500)
    # A commented command is not executable AS EMITTED, which is precisely
    # what made the deferred-closure recommendation unrunnable.
    assert not repair.is_executable_command("okg pact evidence sync x  # later")


def test_every_emitted_command_parses_with_shlex() -> None:
    document = _manifest(acceptance=True)
    parsed = PactManifest.model_validate(document)
    commands = repair.proof_slot_commands(
        parsed,
        "demo",
        ["req.one:executed_test", "req.one:acceptance:acc.one"],
        subject_requirements=["req.one"],
        prefix=REPO_COMMAND,
    )
    assert commands
    for command in commands:
        assert shlex.split(command)
        assert repair.fits_command_guards(command)


# --- the FSM sweep ---------------------------------------------------------

STATES = tuple(fsm.load_table()["states"])


@pytest.mark.parametrize("state", STATES)
@pytest.mark.parametrize("verdict", fsm.ALL_VERDICTS)
@pytest.mark.parametrize("is_operator", [False, True])
def test_enabling_path_is_total_and_actually_enables(
    state: str, verdict: str, is_operator: bool
) -> None:
    """Every (state, verdict) pair: legal here, enabled by a real path, or
    explicitly unreachable. A refusal may never name a verdict the next call
    rejects, and may never shrug."""
    path = fsm.enabling_path(state, verdict, is_operator=is_operator)

    if path.legal_here:
        # The claim is checkable: `legal_transition` must not raise.
        fsm.legal_transition(
            state,
            verdict,
            is_operator=is_operator,
            # `supersede` carries a separate guard; supply what it demands so
            # this arm tests legality, not the target guard.
            target="other-change" if verdict == "supersede" else None,
        )
        return

    if not path.reachable:
        assert path.unreachable_reason, (state, verdict)
        return

    # FOLLOW the path through the table and assert it lands somewhere the
    # verdict is legal. A path that does not enable is worse than none.
    # One event can have two arms under different guards (`gate_post_edit_pass`
    # goes to `in_progress` under `tier_0_or_1` and to `proposed` under
    # `tier_2_plus`), so each step must match the arm the path NAMED — and the
    # guard it depends on has to be published, or "reach proposed" is advice
    # the caller cannot act on.
    current = state
    table = fsm.load_table()
    assert len(path.states) == len(path.events) == len(path.guards)
    for event, expected_state, guard in zip(
        path.events, path.states, path.guards, strict=True
    ):
        arms = [
            transition
            for transition in table["transitions"]
            if transition["from"] == current
            and transition["event"] == event
            and transition["to"] == expected_state
        ]
        assert arms, (state, verdict, event, current)
        assert arms[0].get("guard") == guard
        current = expected_state
    assert verdict in fsm.legal_verdicts(current, is_operator=is_operator), (
        state,
        verdict,
        path.to_dict(),
    )
    assert current == path.target_state


def test_the_audited_pair_names_the_open_tasks_not_another_verdict() -> None:
    """approve in in_progress: the refusal that cost 15 round trips."""
    path = fsm.enabling_path("in_progress", "approve", is_operator=True)
    assert path.legal_here is False
    assert path.reachable is True
    assert path.events == ("tasks_terminal",)
    assert path.target_state == "awaiting_review"
    assert "tasks_not_done" in path.blockers
    commands = repair.enabling_commands(
        path, "demo", prefix=REPO_COMMAND, open_tasks=["task.one", "task.two"]
    )
    assert commands == [
        f"{REPO_COMMAND} pact gate task-done demo task.one --json",
        f"{REPO_COMMAND} pact gate task-done demo task.two --json",
    ]


def test_terminal_states_declare_unreachable_rather_than_inventing_a_path() -> None:
    for state in ("superseded", "rejected"):
        path = fsm.enabling_path(state, "approve", is_operator=True)
        assert path.reachable is False
        assert path.unreachable_reason == "terminal_state"


def test_operator_only_verdicts_are_not_reachable_by_transitions() -> None:
    """No sequence of transitions grants operator authority."""
    path = fsm.enabling_path("retired", "reopen", is_operator=False)
    assert path.reachable is False
    assert path.unreachable_reason == "operator_only"


def test_the_guard_error_carries_the_enabling_path() -> None:
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError) as excinfo:
        fsm.legal_transition("in_progress", "approve", is_operator=True)
    data = excinfo.value.data
    # The pre-change data (the guard + the local legal set) is untouched.
    assert data["violated_guard"] == "legal_transition"
    assert "supersede" in data["legal_verdicts_here"]
    # ...and the door is named alongside the wall.
    assert data["enabling_path"]["reachable"] is True
    assert data["enabling_path"]["events"] == ["tasks_terminal"]


def test_legal_decide_commands_never_offer_a_verdict_with_an_unmet_guard() -> None:
    commands = repair.legal_decide_commands(
        "demo", fsm.legal_verdicts("proposed"), prefix=REPO_COMMAND
    )
    joined = " ".join(commands)
    # `supersede` without --target and `escalate` without --to trip guards of
    # their own, so neither may be offered bare.
    assert "--verdict supersede" not in joined
    assert "--verdict escalate" not in joined
    assert f"{REPO_COMMAND} pact decide demo --verdict approve --json" in commands


# --- the proof-slot generator ----------------------------------------------


def test_slot_parsing_covers_every_shape() -> None:
    assert repair.parse_proof_slot("req.one:executed_test").kind == "executed_test"
    composite = repair.parse_proof_slot("req.one:acceptance:acc.one")
    assert composite.acceptance == "acc.one"
    assert composite.target == "acc.one"
    either = repair.parse_proof_slot("req.one:any:graph_report|published_generation")
    assert either.kind == "graph_report"
    stale = repair.parse_proof_slot("req.one:stale_evidence")
    assert stale.note == "stale_evidence"
    assert stale.target == "req.one"


def test_the_kind_comes_from_the_requirements_own_policy() -> None:
    document = _manifest()
    document["requirements"][0]["evidence_policy"] = {"required": ["graph_report"]}
    parsed = PactManifest.model_validate(document)
    command = repair.proof_slot_commands(
        parsed,
        "demo",
        ["req.one:graph_report"],
        subject_requirements=["req.one"],
        prefix=REPO_COMMAND,
    )[0]
    assert "--kind graph_report" in command
    # ...and the ref from the manifest's own test plan, not a placeholder.
    assert "--ref tests/demo/test_probe.py" in command


def test_the_acceptance_id_form_is_emitted_and_actually_closes_the_slot(
    repo: Path,
) -> None:
    """The link no documented command created (a 28-call audited episode)."""
    document = _manifest(acceptance=True)
    _write(repo, document)

    before = task_completion_gate(repo, "demo", "task.one")
    assert before["ok"] is False
    first = before["next_commands"][0]
    assert " acc.one " in first, first
    assert "--kind executed_test" in first

    # Run exactly what was recommended.
    argv = shlex.split(first)
    positional = [
        token
        for index, token in enumerate(argv)
        if not token.startswith("--")
        and not (index and argv[index - 1].startswith("--"))
    ]
    options = {
        token[2:]: argv[index + 1]
        for index, token in enumerate(argv)
        if token.startswith("--") and index + 1 < len(argv)
    }
    attach_evidence(
        repo,
        "demo",
        positional[-1],
        kind=options["kind"],
        ref=options["ref"],
        status="pass",
    )

    manifest = load_manifest(repo, "demo")
    criterion = manifest.requirements[0].acceptance[0]
    assert criterion.evidence, "the attach must WRITE the criterion's proof link"
    after = task_completion_gate(repo, "demo", "task.one")
    assert not after["missing_proof"]
    assert set(after["blocked_by"]) < set(before["blocked_by"])


def test_task_done_next_commands_are_all_executable(repo: Path) -> None:
    _write(repo, _manifest())
    result = task_completion_gate(repo, "demo", "task.one")
    assert result["ok"] is False
    assert result["next_commands"]
    for command in result["next_commands"]:
        assert repair.is_executable_command(command), command
        assert repair.fits_command_guards(command), command
    # The prose that used to lead the list is still REPORTED, just not as a
    # command.
    assert result["repair"]["verification_clause"].startswith("uv run pytest")
    assert result["why"]


# --- the repair path may not fabricate proof --------------------------------


def _two_requirement_manifest(change_id: str = "demo") -> dict:
    """`task.one` proves `req.one` ONLY. `req.two` is another task's contract."""
    document = _manifest(change_id, acceptance=True)
    document["requirements"].append({
        "id": "req.two",
        "title": "Requirement two",
        "statement": "The behavior named by req.two must hold.",
        "evidence_policy": {"required": ["design_review"]},
        "scenarios": [],
        "acceptance": [
            {"id": "acc.two", "description": "the review lands", "evidence": []}
        ],
    })
    document["tasks"].append({
        "id": "task.two",
        "title": "Task two",
        "status": "todo",
        "verification": "uv run pytest tests/demo/test_two.py -q passes.",
        "requirements": ["req.two"],
        "evidence": [],
        "environment": [],
    })
    document["test_plan"].append({
        "id": "test.req-two.probe",
        "requirement": "req.two",
        "test_id": "tests/demo/test_two.py",
        "evidence": ["pytest"],
    })
    return document


def test_a_repair_never_attaches_proof_outside_the_refused_task(repo: Path) -> None:
    """The subject scope, on the exact probe that found this.

    Refusing `task.one` emitted an attach against `acc.two` — a criterion of
    `req.two`, which `task.one` does not prove — because the trailing sweep ran
    over every unlinked criterion in the manifest. Running that command would
    have written proof for another requirement's claim and cleared a blocker
    nobody checked.
    """
    _write(repo, _two_requirement_manifest())
    result = task_completion_gate(repo, "demo", "task.one")

    assert result["ok"] is False
    assert result["next_commands"]
    joined = " ".join(result["next_commands"])
    assert "acc.two" not in joined, result["next_commands"]
    assert "req.two" not in joined, result["next_commands"]
    # ...and the in-scope criterion IS still offered, so this is a narrowing,
    # not a silencing.
    assert any(" acc.one " in command for command in result["next_commands"])


def test_a_repair_never_borrows_another_requirements_artifact(repo: Path) -> None:
    """The slot's `--ref` comes from ITS requirement's plan row or nowhere.

    `artifact_for` matched a bare acceptance id against `test.requirement`,
    which never matches, and then fell through to "the first test in the plan"
    — so a slot for `req.two` was emitted with `req.one`'s artifact.
    """
    document = _two_requirement_manifest()
    parsed = PactManifest.model_validate(document)

    for slot, requirement, expected in (
        ("req.one:acceptance:acc.one", "req.one", "tests/demo/test_probe.py"),
        ("req.two:acceptance:acc.two", "req.two", "tests/demo/test_two.py"),
    ):
        command = repair.proof_slot_commands(
            parsed,
            "demo",
            [slot],
            subject_requirements=[requirement],
            prefix=REPO_COMMAND,
        )[0]
        assert f"--ref {expected}" in command, (slot, command)

    # A requirement the plan says nothing about gets the PLACEHOLDER, never
    # the first artifact lying around.
    document["test_plan"] = []
    bare = PactManifest.model_validate(document)
    command = repair.proof_slot_commands(
        bare,
        "demo",
        ["req.two:acceptance:acc.two"],
        subject_requirements=["req.two"],
        prefix=REPO_COMMAND,
    )[0]
    assert f"--ref {repair.PLACEHOLDER_ARTIFACT}" in command, command


# --- one guard, one repair --------------------------------------------------


def _guard_refusal(repo_root: Path, manifest, verdict: str, *, is_operator: bool):
    """Raise the FSM guard the way `decide_change` does, and wrap it."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.pact.review import _guard_refusal_with_enabling_path

    state = "in_progress"
    try:
        fsm.legal_transition(state, verdict, is_operator=is_operator)
    except SubstrateError as exc:
        return _guard_refusal_with_enabling_path(
            repo_root, manifest, state, verdict, exc,
            is_operator=is_operator, target=None,
        )
    raise AssertionError(f"{verdict} did not violate a guard in {state}")


def test_supersede_without_target_gets_the_corrected_invocation(repo: Path) -> None:
    """A MISSING ARGUMENT is not a missing state.

    Every `legal_transition` refusal was wrapped in an enabling path, so
    "supersede requires --target" was answered with "reach awaiting_review
    first" — advice for a wall the caller has not hit. The repair for a missing
    argument is the same invocation carrying it.
    """
    manifest = _write(repo, _manifest())
    error = _guard_refusal(repo, manifest, "supersede", is_operator=True)

    assert error.data["violated_guard"] == "target_exists"
    assert error.data["missing_argument"] == "--target"
    assert "--target" in error.data["corrected_invocation"]
    assert "--verdict supersede" in error.data["corrected_invocation"]
    assert "--target" in (error.fix_hint or "")
    # The enabling path belongs to the OTHER guard and must not appear here.
    assert "enabling_path" not in error.data


def test_an_operator_only_verdict_names_an_owner_and_a_remedy(repo: Path) -> None:
    """No transition sequence grants authority, so no command is offered."""
    manifest = _write(repo, _manifest())
    error = _guard_refusal(repo, manifest, "reopen", is_operator=False)

    assert error.data["violated_guard"] == "operator_only"
    action = error.data["operator_action"]
    assert action["owner"]
    assert action["in_band_repair"] is False
    assert action["cause"] == "operator_only_verdict"
    assert "reopen" in action["remedy"]
    # An out-of-band remedy is not a command: the list stays empty rather than
    # offering something the next call rejects for the same reason.
    assert error.data["next_commands"] == []


def test_the_ledger_blocker_is_named_as_operator_action_not_re_emitted(
    repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No `okg pact` command clears an unconfigured DSN, so none is offered."""
    _write(repo, _manifest())
    result = task_completion_gate(repo, "demo", "task.one")
    operator_action = result["repair"]["operator_action"]
    assert operator_action is not None
    assert operator_action["in_band_repair"] is False
    assert result["owner"]
    assert result["remedy"]
    # The remedy is CARRIED, in prose, where prose belongs.
    assert result["remedy"] in result["why"]
    assert result["owner"] in result["why"]
    for command in result["next_commands"]:
        assert "evidence sync" not in command


def test_two_verbs_in_one_shell_name_the_same_ledger_remedy(repo: Path) -> None:
    """Same environment, same cause, same fix — from either verb.

    `gate task-done` passed the ledger's CLASSIFIED authority to
    `repair.ledger_remedy`; `evidence attach` had no `workflow_authority` block
    to pass, so it fell through to a defaulted `export OKG_DSN=...`. Two verbs
    then named different fixes for one dead ledger, and only one of them was
    the fix for the actual cause.
    """
    from okg.substrate.pact.cli import _with_closure_intents
    from okg.substrate.pact.envelope import recorded

    _write(repo, _manifest())

    gate = task_completion_gate(repo, "demo", "task.one")
    gate_remedy = gate["repair"]["operator_action"]

    attach_evidence(
        repo,
        "demo",
        "req.one",
        kind="executed_test",
        ref="tests/demo/test_probe.py",
        status="pass",
    )
    attach = _with_closure_intents(
        repo, "demo", recorded(text="attached evidence to demo", change="demo")
    )
    attach_remedy = attach["operator_action"]

    assert attach_remedy["remedy"] == gate_remedy["remedy"]
    assert attach_remedy["owner"] == gate_remedy["owner"]
    # ...and the shared remedy is the one the CAUSE names, not a default.
    assert gate_remedy["remedy_source"] == "cause_fix_hint"
    assert attach_remedy["remedy_source"] == "cause_fix_hint"


def test_a_ledger_remedy_with_no_documented_fix_says_so(repo: Path) -> None:
    """An unknown cause is NAMED, never papered over with a plausible command.

    A defaulted `export OKG_DSN=...` is the fix for one cause emitted for all
    of them; a reader cannot tell it from a diagnosed one, and following it
    when the real cause is an unconfigured deployment wastes the round trip
    this change exists to remove.
    """
    remedy = repair.ledger_remedy({"deployment": "okg-workspace"})
    assert remedy["remedy_source"] == "cause_unknown"
    assert "export OKG_DSN" not in remedy["remedy"]
    assert "okg-workspace" in remedy["remedy"]
    assert remedy["in_band_repair"] is False
