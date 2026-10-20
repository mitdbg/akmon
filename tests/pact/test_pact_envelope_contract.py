"""The envelope contract: one shape, one origin, one verdict, exit codes.

`pact-honest-envelope` req.envelope.verdict. Three separate obligations:

1. `ok` is DERIVED. It cannot be hand-written anywhere in the package, so the
   check is an AST pass (precedent: tests/ci/test_no_silent_fallbacks.py) — a
   string grep is satisfiable by `dict(ok=...)`, and there were 107 hand-built
   `ok` literals before this change.
2. Every verb x {success, deferral, refusal, error} validates against ONE
   published envelope schema.
3. A byte budget may not cost the reader the verdict: overflow trims verbose
   detail, never `ok`, `verdict`, `blocked_by`, `message`, or `text`.
"""

from __future__ import annotations

import ast
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import envelope
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.cli import (
    POST_EDIT_SUMMARY_BUDGET_BYTES,
    _agent_bootstrap,
    _bootstrap_summary,
    _next,
    _post_edit_summary,
    _show,
    _simple_gate,
    _status,
)
from okg.substrate.cli.main import main

PACKAGE = Path(envelope.__file__).parent
# The benchmark measures the package; it is not part of the surface under
# contract, and its synthetic payloads deliberately construct envelopes by hand.
EXCLUDED_DIRS = {"bench"}


# --------------------------------------------------------------------------
# 1. AST: every `ok` originates from the envelope constructor
# --------------------------------------------------------------------------


_VERDICT_KEYS = frozenset({"ok", "verdict"})


def _ok_dict_sites(path: Path) -> list[int]:
    """Line numbers in `path` that write an `ok` (or `verdict`) key by hand.

    Three spellings, because each of them shipped at least once: a dict
    literal, `dict(ok=...)`, and — the one that let a failing BLOCKING gate
    exit 0 — assigning `payload["ok"]` on an already-built envelope, which
    changes `ok` without re-stamping `verdict`.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    lines: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            for key in node.keys:
                if isinstance(key, ast.Constant) and key.value == "ok":
                    lines.append(node.lineno)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "dict"
        ):
            for keyword in node.keywords:
                if keyword.arg == "ok":
                    lines.append(node.lineno)
        elif isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if (
                    isinstance(target, ast.Subscript)
                    and isinstance(target.slice, ast.Constant)
                    and target.slice.value in _VERDICT_KEYS
                ):
                    lines.append(node.lineno)
    return sorted(lines)


def _package_modules() -> list[Path]:
    return sorted(
        path
        for path in PACKAGE.rglob("*.py")
        if not EXCLUDED_DIRS & set(path.relative_to(PACKAGE).parts)
    )


def test_every_ok_in_the_package_comes_from_the_envelope_constructor() -> None:
    offenders = {
        str(path.relative_to(PACKAGE)): _ok_dict_sites(path)
        for path in _package_modules()
        if path.name != "envelope.py" and _ok_dict_sites(path)
    }
    assert offenders == {}, (
        "hand-built `ok` outside envelope.py — route it through "
        "envelope.recorded/deferred/refused/from_ok (top level) or "
        "envelope.component (a nested sub-report):\n" + json.dumps(offenders, indent=2)
    )


def test_the_ast_check_would_catch_a_dict_call_spelling(tmp_path: Path) -> None:
    """A grep for `"ok":` misses `dict(ok=...)`; the AST pass must not."""
    surface = tmp_path / "surface.py"
    surface.write_text("def f():\n    return dict(ok=True, change='x')\n", "utf-8")
    assert _ok_dict_sites(surface) == [2]


def test_the_ast_check_would_catch_a_subscript_patch(tmp_path: Path) -> None:
    """The audited defect's exact spelling: `ok` patched, `verdict` left alone.

    `result["ok"] = bool(result["ok"] and decision_gate["ok"])` at
    cli.py:1085 emitted `verdict: recorded` beside `ok: false` with an empty
    `blocked_by`, and the CLI exits on the verdict — so a pre-merge whose
    BLOCKING decision gate failed exited 0. Neither of the two earlier
    spellings would have caught it.
    """
    surface = tmp_path / "patch.py"
    surface.write_text(
        "def f(result, other):\n"
        "    result['ok'] = bool(result['ok'] and other['ok'])\n"
        "    result['verdict'] = 'recorded'\n"
        "    result['task_status'] = 'done'\n",
        "utf-8",
    )
    assert _ok_dict_sites(surface) == [2, 3]


def test_the_ast_check_scans_more_than_one_module() -> None:
    """A check that scanned an empty file list would pass vacuously."""
    modules = _package_modules()
    assert len(modules) > 5
    assert any(path.name == "cli.py" for path in modules)
    # And envelope.py really does build `ok` by hand — it is the one origin.
    assert _ok_dict_sites(PACKAGE / "envelope.py")


# --------------------------------------------------------------------------
# 2. One schema, every verb x {success, deferral, refusal, error}
# --------------------------------------------------------------------------


def _seed(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
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
    pact_operations.create_fast_path_change(
        root,
        "demo",
        title="Demo",
        intent="Demo must hold.",
        acceptance="The observable check.",
        test_id="tests/pact/test_envelope_contract.py::test_smoke",
    )
    return root


def _envelopes(root: Path) -> dict[str, dict[str, Any]]:
    """One payload per verb surface, spanning all four outcome classes."""
    payloads: dict[str, dict[str, Any]] = {}

    # reads
    payloads["view --format=status"] = _status(root, "demo")
    payloads["view --format=next"] = _next(root, "demo")
    payloads["view --format=manifest"] = _show(root, "demo", for_agent=False)
    payloads["view --format=manifest --for-agent"] = _show(root, "demo", for_agent=True)
    payloads["view --format=bootstrap --full"] = _agent_bootstrap(root, "demo")
    payloads["view --format=bootstrap"] = _bootstrap_summary(
        payloads["view --format=bootstrap --full"]
    )
    payloads["view --format=list"] = _list_changes(root)

    # gates — refusal (unproven task) and the budgeted summary
    full = pact_operations.post_edit_check(root, "demo")
    payloads["gate post-edit --full"] = full
    payloads["gate post-edit"] = _post_edit_summary(full)
    payloads["gate pre-commit"] = _simple_gate(root, "demo", "pre-commit")
    payloads["gate task-done"] = pact_operations.task_completion_gate(
        root, "demo", "task.demo"
    )

    # mutating
    payloads["evidence status"] = pact_operations.evidence_status(root, "demo")
    payloads["evidence sync"] = pact_operations.sync_closure_intents(root, "demo")
    payloads["hooks install"] = pact_operations.install_gate_hooks(
        root, "demo", target="local", dry_run=True
    )

    # Real CLI dispatch arms, one per verb class, driven through
    # `main` -> `cmd_pact` -> `_dispatch`. Calling the operations functions
    # alone leaves the CLI-only logic unswept, and that is exactly where the
    # envelope broke: `gate pre-merge` folds in the decision gate, and
    # `gate task-done` folds in the closure intent — neither exists in the
    # operation.
    for name, argv in _CLI_ARMS:
        payloads[f"cli: {name}"] = _cli(root, argv)[1]

    # error paths
    payloads["error: unknown change"] = envelope.from_error(
        SubstrateError("unknown change", data={"change": "nope"})
    )
    payloads["error: unknown task"] = _refusal(
        lambda: pact_operations.task_completion_gate(root, "demo", "task.nope")
    )
    payloads["error: unknown requirement"] = _refusal(
        lambda: pact_operations.attach_evidence(
            root,
            "demo",
            "req.nope",
            kind="executed_test",
            ref="tests/x.py",
            status="pass",
        )
    )
    payloads["error: invalid yaml"] = _refusal(
        lambda: pact_operations.evidence_status(root, "not-a-change")
    )
    return payloads


_CLI_ARMS: tuple[tuple[str, tuple[str, ...]], ...] = (
    # reading
    ("view --format=status", ("view", "demo", "--format=status")),
    ("view (default: manifest)", ("view", "demo")),
    # gating — including the arm that folds a SECOND blocking gate in
    ("gate post-edit", ("gate", "post-edit", "demo")),
    ("gate pre-merge", ("gate", "pre-merge", "demo")),
    ("gate task-done", ("gate", "task-done", "demo", "task.demo")),
    # mutating
    ("evidence status", ("evidence", "status", "demo")),
    ("hooks install --dry-run", ("hooks", "install", "demo", "--dry-run")),
    ("new", ("new", "envelope-contract-cli-probe", "--shape", "fast")),
    # error path, through the real `except SubstrateError` arm of `cmd_pact`
    ("view (unknown change)", ("view", "does-not-exist")),
)


def _cli(root: Path, argv: tuple[str, ...]) -> tuple[int, dict[str, Any]]:
    """Run one `okg pact` invocation, returning (exit code, JSON payload)."""
    stream = io.StringIO()
    with redirect_stdout(stream):
        rc = main(["pact", "--root", str(root), *argv, "--json"])
    return rc, json.loads(stream.getvalue().splitlines()[-1])


def _list_changes(root: Path) -> dict[str, Any]:
    from okg.substrate.pact.cli import _list_changes as list_changes

    return list_changes(root, include_graph=False)


def _refusal(call) -> dict[str, Any]:
    try:
        call()
    except SubstrateError as err:
        return envelope.from_error(err)
    raise AssertionError("expected a SubstrateError refusal")


@pytest.fixture(scope="module")
def envelopes(tmp_path_factory: pytest.TempPathFactory) -> dict[str, dict[str, Any]]:
    return _envelopes(_seed(tmp_path_factory.mktemp("envelope-contract")))


def test_every_verb_surface_validates_against_the_published_schema(
    envelopes: dict[str, dict[str, Any]],
) -> None:
    problems = {
        name: envelope.schema_violations(payload)
        for name, payload in envelopes.items()
        if envelope.schema_violations(payload)
    }
    assert problems == {}, json.dumps(problems, indent=2)


def test_the_sample_spans_all_four_outcome_classes(
    envelopes: dict[str, dict[str, Any]],
) -> None:
    """A schema sweep over successes only would pass vacuously."""
    verdicts = {payload["verdict"] for payload in envelopes.values()}
    assert verdicts == {"recorded", "deferred", "refused"}, verdicts
    # And the error class specifically — the shape that used to omit `ok`.
    errors = [
        payload for name, payload in envelopes.items() if name.startswith("error:")
    ]
    assert len(errors) >= 4
    for payload in errors:
        assert payload["ok"] is False
        assert payload["verdict"] == "refused"
        assert payload["message"]
        assert "error" in payload


def test_no_output_shape_omits_ok(envelopes: dict[str, dict[str, Any]]) -> None:
    for name, payload in envelopes.items():
        assert "ok" in payload, name
        assert isinstance(payload["ok"], bool), name


def test_ok_is_exactly_verdict_is_recorded(
    envelopes: dict[str, dict[str, Any]],
) -> None:
    for name, payload in envelopes.items():
        assert payload["ok"] is (payload["verdict"] == "recorded"), name


def test_every_non_recorded_envelope_names_a_cause(
    envelopes: dict[str, dict[str, Any]],
) -> None:
    """`blocked_by` is the ONLY cause location, so it may not be empty.

    Refusal reasons used to rotate across `blockers`, `missing_proof`,
    `closure_intent`, and `workflow_authority.blockers`; a reader that checked
    one of them saw `ok: false` beside an empty diagnostic.
    """
    for name, payload in envelopes.items():
        if payload["verdict"] == "recorded":
            continue
        assert payload["blocked_by"], name
        assert all(isinstance(token, str) for token in payload["blocked_by"]), name


def test_informative_detail_survives_alongside_the_envelope(
    envelopes: dict[str, dict[str, Any]],
) -> None:
    """Consumers of the old fields keep working — they are detail, not verdict."""
    assert envelopes["gate task-done"]["missing_proof"] is not None
    assert "blockers" in envelopes["gate task-done"]
    assert "workflow_authority" in envelopes["gate task-done"]
    assert "validation" in envelopes["gate post-edit --full"]
    assert "errors" in envelopes["gate post-edit"]
    assert "warnings" in envelopes["gate post-edit"]
    assert "actionable" in envelopes["view --format=bootstrap"]
    assert "missing_requirements" in envelopes["evidence status"]
    # The raw manifest dump did not MOVE: `--format=manifest` is the default
    # of `okg pact view <id> --json`, so nesting it would have silently
    # changed the type of `payload["change"]` from object to string.
    manifest_payload = envelopes["view --format=manifest"]
    assert manifest_payload["requirements"]
    assert manifest_payload["change"]["id"] == "demo"


def test_constructor_refuses_an_unknown_verdict() -> None:
    with pytest.raises(ValueError):
        envelope.envelope("green", text="nope")


def test_constructor_refuses_detail_that_shadows_an_envelope_key() -> None:
    """Detail may not overwrite the contract — that is how `ok` started lying."""
    with pytest.raises(ValueError):
        envelope.recorded(text="x", ok=True)  # type: ignore[call-arg]


# --------------------------------------------------------------------------
# 3. Exit codes follow the verdict
# --------------------------------------------------------------------------


def test_exit_codes_follow_the_verdict() -> None:
    assert envelope.exit_code(envelope.recorded(text="x")) == 0
    assert (
        envelope.exit_code(envelope.deferred(text="x", blocked_by=["ledger_not_fresh"]))
        == 3
    )
    assert (
        envelope.exit_code(envelope.refused(text="x", blocked_by=["unknown_change"]))
        == 1
    )
    # Published in one place, so a hook can branch on the deferral deliberately.
    assert envelope.EXIT_CODES == {"recorded": 0, "deferred": 3, "refused": 1}
    assert envelope.ENVELOPE_SCHEMA["exit_codes"]["crash"] == 2


def _no_ledger(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ledger-unreachable arm, pinned rather than inherited from the shell."""
    for name in ("OKG_DSN", "OKG_TEST_DSN", "OKG_PACT_GRAPH_DEPLOYMENT"):
        monkeypatch.delenv(name, raising=False)


def test_every_dispatch_arm_exits_on_its_verdict(tmp_path: Path) -> None:
    """The exit code is derived from the payload the arm actually emitted.

    Swept over real `_dispatch` arms, because the defect this pins was
    CLI-only: `gate pre-merge` folded a failing decision gate onto `ok` by
    hand, leaving `verdict: recorded` — and the exit code follows the verdict,
    so a failed BLOCKING gate exited 0.
    """
    root = _seed(tmp_path)
    for name, argv in _CLI_ARMS:
        rc, payload = _cli(root, argv)
        assert rc == envelope.exit_code(payload), (name, rc, payload["verdict"])
        assert payload["ok"] is (rc == 0), name


def test_pre_merge_refuses_when_its_blocking_decision_gate_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """cli.py:1085 in its exact shape.

    `result["ok"] = bool(result["ok"] and decision_gate["ok"])` patched `ok`
    without re-stamping `verdict`, so a pre-merge with an unreachable decision
    ledger emitted `verdict: recorded`, `ok: false`, `blocked_by: []` and
    exited 0 — a failed blocking gate reading as a pass.
    """
    _no_ledger(monkeypatch)
    root = _seed(tmp_path)
    rc, payload = _cli(root, ("gate", "pre-merge", "demo"))

    assert payload["decision_gate"]["ok"] is False
    assert payload["verdict"] == "refused"
    assert payload["ok"] is False
    assert rc == 1
    # The cause is NAMED, in the single cause location.
    assert "decision_gate_unsatisfied" in payload["blocked_by"]
    assert payload["message"]
    # With no ledger there is no history to derive state from, so the gate
    # recommends NOTHING and says why. Deriving from an empty decision list
    # instead is how the nine-occurrence `--verdict approve` came back.
    plan = payload["decide_plan"]
    assert plan["ledger_consulted"] is False, plan
    assert plan["commands"] == [], plan
    assert plan["state"] is None, plan
    assert "ledger" in plan["why"], plan
    for command in payload.get("next_commands") or []:
        assert "pact decide" not in command, command


def test_pre_merge_never_recommends_a_verdict_the_ledger_history_forbids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The nine-occurrence bug, pinned against `decide`'s OWN derivation.

    `gate pre-merge` derived the lifecycle state with an EMPTY decision list
    while `decide` derived it from the ledger, so the two disagreed by
    construction: a change whose recorded history puts it in `in_progress`
    derived as `proposed` here, `approve` looked legal, and the very next call
    answered "verdict approve is not legal in state in_progress".

    The probe is a fixture whose ledger history ALREADY records the tier-2
    go-ahead approval, with an open task. Under the old code that history was
    invisible and `approve` was recommended; under the fixed one the gate
    derives `in_progress` — the same state `decide_change` derives from the
    same history — and names the enabling path instead.
    """
    from okg.substrate.pact import decisions as ledger_mod
    from okg.substrate.pact import review as review_mod
    from okg.substrate.pact.fsm import DecisionEvent

    _no_ledger(monkeypatch)
    root = _seed(tmp_path)
    # tier-2 is what makes the two derivations DISAGREE: without the recorded
    # go-ahead the change sits in `proposed` (where approve is legal), and with
    # it the change is `in_progress` (where it is not). A tier-1 fixture
    # derives `in_progress` either way and would pass against the bug.
    manifest_path = root / "pact" / "changes" / "demo" / "pact.yaml"
    document = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    document["change"]["labels"] = ["tier-2"]
    document["change"]["tier"] = 2
    manifest_path.write_text(
        yaml.safe_dump(document, sort_keys=False), encoding="utf-8"
    )
    from okg.substrate.pact.store import intent_digest

    current_digest = intent_digest(pact_operations.load_manifest(root, "demo"))
    history = [
        DecisionEvent(
            change_id="demo",
            verdict="approve",
            principal="operator:probe",
            authority="operator",
            from_state="proposed",
            to_state="in_progress",
            intent_digest=current_digest,
        )
    ]

    class _Conn:
        def close(self) -> None:
            return None

    monkeypatch.setattr(
        ledger_mod, "require_ledger_connection", lambda **_kwargs: _Conn()
    )
    monkeypatch.setattr(ledger_mod, "load_decisions", lambda *_a, **_k: history)
    monkeypatch.setattr(ledger_mod, "load_intents", lambda *_a, **_k: [])

    rc, payload = _cli(root, ("gate", "pre-merge", "demo"))
    assert rc != 0
    plan = payload["decide_plan"]

    # The independent oracle: what `decide` itself derives from this history —
    # and the proof that the empty-list derivation would have disagreed.
    manifest = pact_operations.load_manifest(root, "demo")
    derived = review_mod.derive_change_state(manifest, history, repo_root=root)
    derived_without_history = review_mod.derive_change_state(
        manifest, [], repo_root=root
    )
    assert derived_without_history.state != derived.state, (
        "probe does not discriminate: the fixture derives the same state with "
        "and without its ledger history"
    )
    assert plan["ledger_consulted"] is True, plan
    assert plan["state"] == derived.state, (plan, derived.state)
    assert plan["decision_count"] == len(history), plan
    # A recorded approval in history must NOT produce an approve
    # recommendation while the work is still open.
    assert "approve" not in plan["legal_verdicts"], plan
    for command in plan["commands"]:
        assert "--verdict approve" not in command, command
    assert plan["enabling_path"]["reachable"] is True, plan
    assert any("gate task-done" in command for command in plan["commands"]), plan


def test_task_done_refuses_a_dangling_ref_rather_than_deferring(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A manifest that does not VALIDATE may not be deferred.

    Deferral asserts the subject holds and only the durable write is queued —
    exit 3, which the blocking hook `hooks install` writes deliberately
    ALLOWS. With a dangling evidence ref and a dead ledger, `task-done`
    returned a closure intent, restated the refusal as `deferred`, and
    OVERWROTE `blocked_by` with the authority blockers, erasing
    `unknown_evidence_ref` entirely.
    """
    from okg.substrate.pact.bench import fixtures as bench_fixtures

    _no_ledger(monkeypatch)
    repo = bench_fixtures.build_repo(tmp_path / "repo")
    manifest = bench_fixtures.proven_manifest("probe")
    manifest["evidence"].append(
        bench_fixtures.evidence_row(
            "ev.executed-test.dangling", refs=["req.does-not-exist"]
        )
    )
    repo.write_change(manifest)

    rc, payload = _cli(repo.root, ("gate", "task-done", "probe", "task.one"))

    assert payload["verdict"] == "refused", payload["blocked_by"]
    assert payload["ok"] is False
    assert rc == 1
    # Both causes survive: the validation error that forced the refusal AND
    # the ledger blocker. `blocked_by` is a union, never an overwrite.
    assert "unknown_evidence_ref" in payload["blocked_by"]
    assert any(token.startswith("ledger_") for token in payload["blocked_by"])
    # And nothing was queued: a refusal has no closure intent to sync.
    assert "closure_intent" not in payload


def test_cli_exit_code_for_a_refusal_is_one(tmp_path: Path, capsys) -> None:
    root = _seed(tmp_path)
    rc = main(["pact", "--root", str(root), "view", "does-not-exist", "--json"])
    assert rc == 1
    payload = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert payload["verdict"] == "refused"
    assert payload["ok"] is False
    assert payload["message"]
    # The old `error` block is still there for every existing reader.
    assert payload["error"]["code"] == "substrate_error"


def test_cli_exit_code_for_a_deferral_is_three(tmp_path: Path, capsys) -> None:
    """A queued ledger write is nonzero AND distinguishable from a refusal.

    `set -e` must stop treating queued-not-recorded as success, and a blocking
    hook must still be able to allow it (operations._hook_script branches on 3).
    """
    root = _seed(tmp_path)
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
            "operator_report",
            "--ref",
            "reports/envelope-contract.md",
            "--status",
            "pass",
            "--summary",
            "envelope contract probe",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert payload["verdict"] == "deferred", payload
    assert payload["ok"] is False
    assert rc == 3
    # The mutation really did persist locally; only the ledger write is queued.
    assert payload["evidence"] == 1
    assert payload["coordination"] == "deferred"


# --------------------------------------------------------------------------
# 4. Size never lies
# --------------------------------------------------------------------------


def test_budget_overflow_trims_detail_and_never_flips_the_verdict() -> None:
    payload = envelope.refused(
        text="post-edit demo: refused errors=40",
        message="the manifest does not validate",
        blocked_by=["missing_evidence_policy"],
        next_commands=["okg pact view demo --format=brief --json"],
        change="demo",
        errors=[{"code": f"e{i}", "path": "x" * 120} for i in range(40)],
        warnings=[{"code": f"w{i}", "path": "y" * 120} for i in range(40)],
    )
    before = json.dumps(payload).encode("utf-8")
    assert len(before) > POST_EDIT_SUMMARY_BUDGET_BYTES

    trimmed = envelope.fit_budget(
        dict(payload),
        POST_EDIT_SUMMARY_BUDGET_BYTES,
        trim=(("errors", 3), ("warnings", 2)),
    )

    # The size problem stayed a size problem.
    assert trimmed["verdict"] == "refused"
    assert trimmed["ok"] is False
    assert trimmed["blocked_by"] == ["missing_evidence_policy"]
    assert trimmed["message"] == "the manifest does not validate"
    assert trimmed["text"] == payload["text"]
    assert trimmed["next_commands"] == payload["next_commands"]
    # And the verbose detail is what shrank, visibly.
    assert len(trimmed["errors"]) == 3
    assert len(trimmed["warnings"]) == 2
    assert trimmed["budget_trimmed"]["budget"] == POST_EDIT_SUMMARY_BUDGET_BYTES


def test_a_recorded_gate_that_overflows_stays_recorded() -> None:
    """The audited defect in its exact shape: overflow used to set ok=False.

    A green gate with a large report became a fake gate failure at
    cli.py:1420-1424 — a byte count flipping a verdict.
    """
    payload = envelope.recorded(
        text="post-edit demo: recorded errors=0",
        change="demo",
        errors=[],
        warnings=[{"code": f"w{i}", "path": "y" * 200} for i in range(40)],
    )
    assert len(json.dumps(payload).encode("utf-8")) > POST_EDIT_SUMMARY_BUDGET_BYTES

    trimmed = envelope.fit_budget(
        dict(payload),
        POST_EDIT_SUMMARY_BUDGET_BYTES,
        trim=(("warnings", 2),),
    )

    assert trimmed["verdict"] == "recorded"
    assert trimmed["ok"] is True
    assert trimmed["blocked_by"] == []
    assert len(trimmed["warnings"]) == 2


def test_a_payload_under_budget_is_untouched() -> None:
    payload = envelope.recorded(text="small", change="demo", errors=[1, 2, 3])
    same = envelope.fit_budget(dict(payload), 4096, trim=(("errors", 1),))
    assert same == payload
    assert "budget_trimmed" not in same
