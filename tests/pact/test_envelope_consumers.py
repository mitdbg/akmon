"""In-repo consumers moved to the envelope, with nothing keying off the old meanings.

`pact-honest-envelope` req.envelope.migration. The risk this pins is silent
breakage: a consumer that still branches on `ok` meaning "the command ran", on
`actionable` as the bootstrap verdict, or on `marked` as the task-done verdict
now reads a field whose meaning moved under it.

The fields themselves are NOT retired — `actionable`, `marked`,
`missing_proof`, `blockers`, `intent_registration`, `coordination` are all
still emitted, still mean what they meant, and are still asserted on. What is
retired is using them as the OUTCOME. So the sweep is written against the
places that decide an outcome: the dispatcher's exit code, the generated
hooks and CI steps, and the agent-facing docs.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

from okg.substrate.pact import envelope, operations

from tests.private_policy import private_policy_path

REPO_ROOT = Path(__file__).resolve().parents[2]
PACT_PACKAGE = Path(envelope.__file__).parent


# --------------------------------------------------------------------------
# The dispatcher's exit code
# --------------------------------------------------------------------------


def test_cmd_pact_derives_its_exit_code_from_the_verdict() -> None:
    """The fail-open default is gone.

    `return 0 if payload.get("ok", True) else 1` mapped any payload WITHOUT an
    `ok` key — the error path, among others — to exit 0. The replacement reads
    the verdict, and every payload now has one.
    """
    source = (PACT_PACKAGE / "cli.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    cmd_pact = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "cmd_pact"
    )
    body = ast.get_source_segment(source, cmd_pact) or ""
    assert 'payload.get("ok", True)' not in body
    assert "envelope.exit_code(payload)" in body
    # The refusal path too: a typed error is exit 1 THROUGH the envelope, not
    # a bare `return 1` that happens to agree today.
    assert "envelope.exit_code(refusal)" in body


def test_exit_codes_are_published_in_exactly_one_place() -> None:
    assert envelope.EXIT_CODES == {"recorded": 0, "deferred": 3, "refused": 1}
    # 2 is argparse's usage-error code and stays that way: "could not produce
    # a verdict" covers both a bad invocation and a crash.
    assert envelope.EXIT_CRASH == 2


# --------------------------------------------------------------------------
# Generated hooks and CI: a deferral must not break a commit or a lane
# --------------------------------------------------------------------------


def _blocking_hook(gate: str) -> str:
    return operations._hook_script("demo", gate, hard_fail=True)


def test_blocking_hooks_treat_a_deferral_as_pass_with_warning() -> None:
    """Explicit, not inherited from `set -e`.

    A blocking hook runs `okg pact gate <hook> <id>`; under the envelope that
    exits 3 when the subject HOLDS and only the decision-ledger write is
    queued. Failing the operator's commit on an unreachable database is the
    opposite of what the deferred verdict exists to express, so the hook
    branches on 3 deliberately.
    """
    for gate in ("pre-commit", "pre-merge", "task-done", "graph-freshness"):
        script = _blocking_hook(gate)
        assert 'if [ "$status" -eq 3 ]' in script, gate
        assert "exit 0" in script, gate
        assert 'exit "$status"' in script, gate
        # And it still fails on a real refusal (1) or a crash (2): the script
        # propagates every other status rather than swallowing it.
        assert "|| true" not in script, gate
        assert "deferred" in script, gate


def test_advisory_hooks_exit_zero_but_still_name_the_verdict() -> None:
    """Warn mode may not block — but it may not hide the verdict either.

    `<cmd> || true` discarded the exit code, so under the envelope a refusal
    and a pass produced identical hook output. The advisory hook now captures
    the status, says on stderr which verdict it saw, and still exits 0.
    """
    import shutil
    import subprocess

    for gate in ("post-edit", "reviewer-brief"):
        script = operations._hook_script("demo", gate, hard_fail=False)
        assert "|| true" not in script, gate
        assert "status=$?" in script, gate
        assert 'if [ "$status" -eq 3 ]' in script, gate
        assert "deferred" in script and "refused" in script, gate
        # Warn mode: never propagates a nonzero status.
        assert 'exit "$status"' not in script, gate
        assert script.rstrip().endswith("exit 0"), gate
        # And the generated shell actually parses.
        sh = shutil.which("sh")
        if sh:
            check = subprocess.run(
                [sh, "-n"], input=script, text=True, capture_output=True
            )
            assert check.returncode == 0, check.stderr


def test_generated_ci_workflow_annotates_a_deferral_instead_of_reddening() -> None:
    workflow = operations._ci_workflow("demo")
    assert workflow.count('if [ "$status" -eq 3 ]') == 3
    assert "::warning::" in workflow
    assert workflow.count('exit "$status"') == 3


# --------------------------------------------------------------------------
# The graph source projector
# --------------------------------------------------------------------------


def test_the_pact_graph_source_reads_manifests_not_verb_output() -> None:
    """Why the projector needs no migration — pinned so it stays true.

    It parses `pact/changes/*/pact.yaml` off disk. If it ever starts shelling
    out to a verb, its output shape becomes this change's problem and this
    test is the prompt to revisit the sweep.
    """
    source = (
        REPO_ROOT / "src" / "okg" / "substrate" / "library" / "sources" / "pact.py"
    )
    assert source.is_file()
    text = source.read_text(encoding="utf-8")
    assert "okg pact" not in text
    assert "subprocess" not in text
    assert "pact.yaml" in text


# --------------------------------------------------------------------------
# Agent-facing docs teach the envelope, not the per-verb field folklore
# --------------------------------------------------------------------------

BUNDLED_SKILL = PACT_PACKAGE / "skills" / "pact" / "SKILL.md"


def test_agents_md_documents_the_v5_envelope_and_exit_codes() -> None:
    """The envelope an agent has to read, as v5 defines it.

    v5 collapsed the shape: no `next_commands`, no `deferred` verdict, no
    verb-class table (every verb returns the same thing), and no `decide`
    whose own `--verdict` needed disambiguating. What is left must still be
    stated in the contract, because an agent that misreads the envelope
    misreads every answer.
    """
    text = private_policy_path("pact/AGENTS.md").read_text(encoding="utf-8")
    for token in (
        "`verdict`",
        "`blocked_by`",
        "`warnings`",
        "`ok` (derived)",
        "exit 0, 1, or 2",
    ):
        assert token in text, token
    # `recorded` and `refused` are the only verdicts, and that is stated.
    assert "never `deferred`" in text
    assert "next_commands" not in text


def test_the_pact_skill_teaches_reading_verdict_not_ok() -> None:
    text = BUNDLED_SKILL.read_text(encoding="utf-8")
    assert "verdict" in text
    assert "blocked_by" in text
    assert "exit 3" in text
    # The old instruction pointed at two of the four rotating cause locations.
    assert "Read `missing_proof` / `blockers`" not in text


def test_installed_skill_copies_match_the_bundle() -> None:
    """Codex and Claude Code read installed copies, not the bundle."""
    bundled = BUNDLED_SKILL.read_text(encoding="utf-8")
    for installed in (
        REPO_ROOT / ".agents" / "skills" / "pact" / "SKILL.md",
        REPO_ROOT / ".claude" / "skills" / "pact" / "SKILL.md",
    ):
        assert installed.is_file(), installed
        assert installed.read_text(encoding="utf-8") == bundled, installed


# --------------------------------------------------------------------------
# The retired spelling: a top-level `verdict` that is not the envelope's
# --------------------------------------------------------------------------

VERDICT_KEY = re.compile(r'^\s*"verdict":', re.MULTILINE)


def test_no_pact_payload_reuses_the_verdict_key_for_a_decision() -> None:
    """`decide --verdict defer` echoed a top-level `verdict: "defer"`.

    That is not one of recorded/deferred/refused, so any reader — including
    the honesty classifier — would have read a decision verdict as an envelope
    verdict. The echo is `decision_verdict` now; only NESTED ledger-event
    payloads still carry the decision's own `verdict`.
    """
    offenders: dict[str, list[int]] = {}
    for path in sorted(PACT_PACKAGE.rglob("*.py")):
        if "bench" in path.parts or path.name == "envelope.py":
            continue
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            keys = {
                key.value
                for key in node.keys
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }
            # A dict that carries `verdict` but not `ok` is either a nested
            # ledger event (legal) or a top-level envelope built by hand
            # (illegal — the AST check in test_pact_envelope_contract catches the
            # `ok` half). What must never appear is a dict pairing `verdict`
            # with the ENVELOPE's siblings while bypassing the constructor.
            if "verdict" in keys and keys & {"blocked_by", "next_commands"}:
                offenders.setdefault(path.name, []).append(node.lineno)
    assert offenders == {}, offenders


def test_decide_all_stale_echoes_the_decision_under_its_own_key() -> None:
    text = (PACT_PACKAGE / "decisions.py").read_text(encoding="utf-8")
    assert text.count('decision_verdict="defer"') == 2
    assert '"verdict": "defer",\n' not in text.replace(
        # the nested coordination-event payload keeps the decision's spelling
        '                                "verdict": "defer",\n',
        "",
    )
