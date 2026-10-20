"""Guard the documented neutral first-result workflow.

The full publish proof belongs in the deployment golden-path lane because it
needs a disposable Postgres service. This local test keeps the command
sequence and its safety contract visible to ordinary docs CI.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import uuid

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate.console_views.health import _critical_read_prerequisites
from okg.substrate.console_views.ontology_rollup_read import read_ontology_rollup
from okg.substrate.graph_store import ChronosGraphStore


ROOT = Path(__file__).resolve().parents[2]
QUICKSTART = ROOT / "docs/start/evaluate-from-source.md"


def test_general_quickstart_has_a_complete_neutral_publish_query_sequence() -> None:
    text = QUICKSTART.read_text(encoding="utf-8")
    required = (
        "tests/fixtures/deployment_bootstrap/records.json",
        "deployment bootstrap plan",
        "deployment bootstrap inspect-source",
        "deployment bootstrap draft",
        'commit -m "Stage AKMON quickstart deployment"',
        "deployment bootstrap verify",
        "deployment bootstrap apply",
        "okg generation describe",
        "okg search",
        "pinned_generation_id",
        "rm -rf",
    )
    for marker in required:
        assert marker in text, marker

    ordered_steps = required[1:7] + ("okg search",)
    positions = [text.index(marker) for marker in ordered_steps]
    assert positions == sorted(positions)
    assert re.search(r"\b(?:records|relationships)\b", text, re.I)
    assert "published" in text.lower()


def _json_output(capsys) -> dict:
    captured = capsys.readouterr()
    output = (captured.out or captured.err).strip()
    assert output, "command produced no JSON output"
    return json.loads(output)


def _documented_okg_commands() -> list[list[str]]:
    """Extract executable ``okg`` commands from the guide's bash blocks."""
    text = QUICKSTART.read_text(encoding="utf-8")
    commands: list[list[str]] = []
    for block in re.findall(r"```bash\n(.*?)\n```", text, re.DOTALL):
        logical_lines = re.sub(r"\\\n\s*", " ", block).splitlines()
        for line in logical_lines:
            if not line.strip().startswith("uv run okg "):
                continue
            tokens = shlex.split(line.strip())
            okg_index = tokens.index("okg")
            commands.append(tokens[okg_index + 1 :])
    return commands


def _documented_command(commands: list[list[str]], *prefix: str) -> list[str]:
    matches = [command for command in commands if command[: len(prefix)] == list(prefix)]
    assert len(matches) == 1, f"expected one documented command for {' '.join(prefix)}"
    return list(matches[0])


def _expand_documented_command(
    command: list[str],
    *,
    deployment: Path,
    deployment_name: str,
    brief: Path,
    dsn: str,
    generation_id: str | None = None,
) -> list[str]:
    replacements = {
        "$OKG_QUICKSTART_DEPLOYMENT": str(deployment),
        "$OKG_QUICKSTART_ROOT/brief.md": str(brief),
        "$OKG_QUICKSTART_DSN": dsn,
        "akmon-quickstart": deployment_name,
    }
    if generation_id is not None:
        replacements["<generation-id>"] = generation_id
    return [replacements.get(token, token) for token in command]


@pytest.mark.postgres
def test_documented_first_result_publishes_and_queries_bundled_data(
    tmp_path: Path,
    clean_runtime: str,
    capsys,
    monkeypatch,
    record_testsuite_property,
) -> None:
    deployment_name = f"docs{tmp_path.name.lower().replace('-', '')}"
    # The documented layout: <root>/deployments/<name> with OKG_DEPLOYMENTS_DIR
    # set, so later commands reach the deployment by name.
    deployments_root = tmp_path / "deployments"
    deployment = deployments_root / deployment_name
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    fixture_dir = deployment / "fixtures"
    fixture_dir.mkdir(parents=True)
    shutil.copy2(
        ROOT / "tests/fixtures/deployment_bootstrap/records.json",
        fixture_dir / "records.json",
    )
    brief = tmp_path / "brief.md"
    brief.write_text(
        "Store neutral records and their relationships so they can be searched.\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_QUICKSTART_DSN", clean_runtime)
    documented = _documented_okg_commands()

    for prefix in (
        ("deployment", "bootstrap", "plan"),
        ("deployment", "bootstrap", "inspect-source"),
        ("deployment", "bootstrap", "draft"),
    ):
        command = _expand_documented_command(
            _documented_command(documented, *prefix),
            deployment=deployment,
            deployment_name=deployment_name,
            brief=brief,
            dsn=clean_runtime,
        )
        assert main(command) == 0
        _json_output(capsys)

    git_env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "OKG fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
        "GIT_COMMITTER_NAME": "OKG fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
    }
    subprocess.run(
        ["git", "init", "-q", "-b", "main"],
        cwd=deployment,
        env=git_env,
        check=True,
    )
    subprocess.run(
        ["git", "add", "-A"], cwd=deployment, env=git_env, check=True,
    )
    subprocess.run(
        ["git", "commit", "-q", "-m", "stage fixture"],
        cwd=deployment,
        env=git_env,
        check=True,
    )
    assert subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=deployment,
        env=git_env,
        check=True,
        capture_output=True,
        text=True,
    ).stdout == ""

    # The substrate lane reuses an ephemeral worker database whose catalog
    # history is intentionally preserved between tests. Claim that disposable
    # catalog for the staged fixture so bootstrap verification exercises the
    # real ownership guard without weakening its production refusal.
    assert main([
        "--json",
        "catalog",
        "ownership",
        "claim",
        "--deployment",
        str(deployment / ".okg_bootstrap" / "staged"),
        "--dsn",
        clean_runtime,
        "--force",
    ]) == 0
    capsys.readouterr()

    verify_command = _expand_documented_command(
        _documented_command(documented, "deployment", "bootstrap", "verify"),
        deployment=deployment,
        deployment_name=deployment_name,
        brief=brief,
        dsn=clean_runtime,
    )
    assert main(verify_command) == 0
    verified = _json_output(capsys)["verification"]
    assert verified["status"] == "verified"
    assert verified["node_row_delta"] > 0
    assert verified["edge_row_delta"] > 0
    assert verified["metrics_snapshot"]["dlq_count"] == 0
    generation_id = verified["generation_id"]
    record_testsuite_property("published_generation_id", generation_id)
    record_testsuite_property("node_row_delta", verified["node_row_delta"])
    record_testsuite_property("edge_row_delta", verified["edge_row_delta"])
    record_testsuite_property("dlq_count", verified["metrics_snapshot"]["dlq_count"])

    # The ontology rollup must be maintained here even though this deployment's
    # views_registry.yaml registers no view (#1843). While the publisher keyed the
    # publish-stage hook on a registered `ontology_graph_rollup` view, every
    # deployment but okg-workspace published generations whose Graph-ontology read
    # then refused with reason `no_sentinel`, and Health showed a permanent urgent
    # "Graph ontology cannot load for the current graph version" check on a graph
    # that was otherwise fine.
    with psycopg.connect(clean_runtime) as rollup_conn:
        rollup = read_ontology_rollup(
            rollup_conn,
            deployment_name=deployment_name,
            generation_id=generation_id,
        )
        readiness = _critical_read_prerequisites(
            rollup_conn,
            deployment_name=deployment_name,
            generation_id=generation_id,
        )
        store = ChronosGraphStore.from_connection(rollup_conn, ensure_schema=False)
        checkpoint_metadata = dict(store.get_generation(generation_id).metadata or {})
    # The publish landed edges (asserted above), so the rollup carries buckets --
    # a sentinel over zero data rows would mean the hook ran but bucketed nothing.
    assert rollup.rows, "maintained ontology rollup has no buckets for a graph with edges"
    ontology = next(
        row for row in readiness["prerequisites"] if row["id"] == "ontology_rollup"
    )
    assert ontology["status"] == "available", ontology
    # No critical read is UNAVAILABLE -- the condition that raises the operator's
    # urgent Health check. Deliberately not `readiness["ready"] is True`: `ready`
    # also goes false for a deliberate opt-out (the count rollup reports
    # "disabled" when OKG_IVM_CONSOLE_COUNT_ROLLUP is off), which would couple
    # this proof to an unrelated flag.
    unavailable = [
        row for row in readiness["prerequisites"] if row["status"] == "unavailable"
    ]
    assert unavailable == [], unavailable

    # The maintained cycle leaves an ontology_rollup RECEIPT in checkpoint
    # metadata. That receipt, not the carried sentinel, is what tells the next
    # cycle the rollup was actually maintained here: the table carries forward,
    # so after a swallowed NON_GATING maintain failure the read would serve this
    # generation's rows and the next cycle would fold incrementally over the gap.
    # An absent receipt forces a full recount instead (#1843).
    receipt = checkpoint_metadata.get("ontology_rollup")
    assert receipt is not None, sorted(checkpoint_metadata)
    # Deliberately not pinned to "full_recount": this lane reuses one database
    # across tests and okg.ontology_rollup is not deployment-scoped, so a prior
    # test's sentinel can make this cycle incremental. What the requirement needs
    # is that a maintained cycle records a real maintain, not an empty dict.
    assert receipt["mode"] in ("full_recount", "incremental"), receipt
    record_testsuite_property("ontology_rollup_buckets", len(rollup.rows))
    record_testsuite_property("ontology_rollup_receipt_mode", receipt["mode"])

    describe_command = _expand_documented_command(
        _documented_command(documented, "generation", "describe"),
        deployment=deployment,
        deployment_name=deployment_name,
        brief=brief,
        dsn=clean_runtime,
        generation_id=generation_id,
    )
    assert main(describe_command) == 0
    assert _json_output(capsys)["status"] == "published"

    apply_command = _expand_documented_command(
        _documented_command(documented, "deployment", "bootstrap", "apply"),
        deployment=deployment,
        deployment_name=deployment_name,
        brief=brief,
        dsn=clean_runtime,
    )
    assert main(apply_command) == 0
    handover = _json_output(capsys)["catalog_handover"]
    # Issue #2867: apply hands the catalog to the applied directory, so the
    # verify-time staged copy no longer owns it.
    assert handover["claimed"] is True
    assert handover["deployment_root"] == str(deployment.resolve())
    assert handover["owner_root"] == str(deployment.resolve())
    assert handover["previous_owner"]["deployment_root"] == str(
        (deployment / ".okg_bootstrap" / "staged").resolve()
    )
    assert handover["catalog_version_id"]
    # The handover ran in the database verify published to (review item 2).
    from okg.substrate.db import dsn as dsn_module

    assert handover["database"]["dbname"] == dsn_module.parse(clean_runtime).params["dbname"]
    subprocess.run(
        ["git", "add", "-A"], cwd=deployment, env=git_env, check=True,
    )
    subprocess.run(
        ["git", "commit", "-q", "-m", "apply fixture"],
        cwd=deployment,
        env=git_env,
        check=True,
    )

    # A second publish from the applied directory is not refused as a
    # foreign writer, and a fork of its result is not refused as built on
    # an older catalog (issue #2867).
    assert main([
        "--json", "run", "--once", "--apply",
        "--deployment", deployment_name, "--dsn", clean_runtime,
    ]) == 0
    second = _json_output(capsys)
    assert second["status"] == "published"
    assert second["generation_id"] and second["generation_id"] != generation_id
    fork = f"experiment:docs-quickstart-{uuid.uuid4().hex[:8]}"
    assert main([
        "--json", "branch", "create", fork, "--role", "experiment",
        "--deployment", deployment_name, "--dsn", clean_runtime,
    ]) == 0
    _json_output(capsys)

    search_command = _expand_documented_command(
        _documented_command(documented, "search"),
        deployment=deployment,
        deployment_name=deployment_name,
        brief=brief,
        dsn=clean_runtime,
        generation_id=generation_id,
    )
    assert main(search_command) == 0
    result = _json_output(capsys)
    assert result["generation_id"] == generation_id
    assert result["results"]


# ---------------------------------------------------------------------------
# AKMON reviewer scripts.
#
# scripts/akmon/reproduce.py publishes the bundled records and runs one script
# per paper claim with every key and provider variable removed. The public
# docs no longer describe it (public-release-docs, docs-structure section), so
# no test pins page text for it; these tests still run those exact scripts as
# subprocesses on a disposable database, then prove each claim script fails
# (exit 1, naming the mismatch) when its expected outcome is absent.

AKMON_SCRIPTS = ROOT / "scripts/akmon"


def _summarize_block():
    import sys

    sys.path.insert(0, str(AKMON_SCRIPTS))
    try:
        from claim_fork_merge_readmission import _summarize_block as summarize
    finally:
        sys.path.remove(str(AKMON_SCRIPTS))
    return summarize


def test_fork_claim_reads_a_landing_block_and_a_staging_gate_block() -> None:
    """merge_back reports a landing block as a stage-prefixed string and a
    staging-gate block as the gate's mapping; the claim reads both."""
    summarize = _summarize_block()
    gate = {
        "reason": "consistency_publish_gate_blocked",
        "failed_consistency_gates": [
            {"narrowing_violation_edge_ids": [7], "gate": "narrowing"},
        ],
    }
    assert summarize(f"land_gate_blocked_rolled_back:{gate}") == (
        "land_gate_blocked_rolled_back", "consistency_publish_gate_blocked", True,
    )
    assert summarize(gate) == (
        "staging_gate", "consistency_publish_gate_blocked", True,
    )
    unnamed = {"reason": "consistency_publish_gate_blocked", "failed_consistency_gates": []}
    assert summarize(unnamed)[2] is False


def _keyless_env() -> dict[str, str]:
    import sys

    sys.path.insert(0, str(AKMON_SCRIPTS))
    try:
        from _common import keyless_environment
    finally:
        sys.path.remove(str(AKMON_SCRIPTS))
    env, _removed = keyless_environment()
    env.pop("OKG_REVIEWER_DSN", None)
    return env


def _script(name: str, *args: str, env: dict[str, str] | None = None):
    import sys

    return subprocess.run(
        [sys.executable, str(AKMON_SCRIPTS / name), *args],
        cwd=ROOT,
        env=env if env is not None else _keyless_env(),
        capture_output=True,
        text=True,
        check=False,
    )


def _pass_line(output: str, label: str) -> str:
    lines = [line for line in output.splitlines() if line.startswith(f"{label} PASS")]
    assert len(lines) == 1, output[-4000:]
    return lines[0]


def test_reviewer_scripts_refuse_to_run_with_a_credential_variable(tmp_path: Path) -> None:
    env = _keyless_env()
    env["EXAMPLE_API_KEY"] = "not-a-real-key"
    result = _script(
        "reviewer_quickstart.py", "--dsn", "postgresql://unused.invalid/none",
        "--workdir", str(tmp_path / "w"), env=env,
    )
    assert result.returncode == 1
    assert "FAIL quickstart: credential variables are set" in result.stdout
    assert "EXAMPLE_API_KEY" in result.stdout
    assert "not-a-real-key" not in result.stdout + result.stderr
    assert not (tmp_path / "w").exists()


def test_reviewer_scripts_report_an_unexpected_error_as_one_fail_line(
    tmp_path: Path,
) -> None:
    # An unreachable database raises deep inside psycopg; the script must
    # still end with its `FAIL <step>` line and exit 1, not a traceback.
    state = tmp_path / "reviewer-state.json"
    state.write_text(json.dumps({
        "deployment_name": "akmon-reviewer",
        "deployment_dir": str(tmp_path / "deployments/akmon-reviewer"),
        "deployments_dir": str(tmp_path / "deployments"),
        "dsn_env": "OKG_REVIEWER_DSN",
        "generation_id": "okg:none",
    }), encoding="utf-8")
    result = _script(
        "claim_lens_round_trip.py", "--state", str(state),
        "--dsn", "postgresql://nobody@127.0.0.1:1/none",
    )
    assert result.returncode == 1
    assert "Traceback" not in result.stdout + result.stderr
    last = result.stdout.strip().splitlines()[-1]
    assert last.startswith("FAIL lens-round-trip: unexpected "), last


def _reproduce_module():
    import importlib
    import sys

    sys.path.insert(0, str(AKMON_SCRIPTS))
    try:
        return importlib.import_module("reproduce")
    finally:
        sys.path.remove(str(AKMON_SCRIPTS))


def _python_step(code: str) -> list[str]:
    import sys

    return [sys.executable, "-c", code]


def _run_reproduce_steps(tmp_path: Path, steps) -> tuple[int, Path, Path]:
    """Run ``steps`` through reproduce.run_steps; return code, log and transcript."""
    reproduce = _reproduce_module()
    log_path = tmp_path / "reproduce.log"
    transcript = tmp_path / "transcript.txt"
    tee = reproduce._Tee(transcript)
    try:
        with log_path.open("w", encoding="utf-8") as log:
            code = reproduce.run_steps(
                [reproduce.Step(*step) for step in steps], env=dict(os.environ),
                tee=tee, log=log, log_path=log_path, transcript=transcript,
            )
    finally:
        tee.close()
    return code, log_path, transcript


@pytest.mark.parametrize(
    ("raw", "secret", "kept"),
    [
        ("postgresql://u:hunter2@db/x", "hunter2", "postgresql://u:***@db/x"),
        ("postgresql://:hunter2@db/x", "hunter2", "@db/x"),
        ("postgresql://u:hun@ter2@db/x", "hun@ter2", "@db/x"),
        ("host=db password=hunter2 dbname=x", "hunter2", "host=db password="),
        ("postgresql://u@db/x?password=hunter2", "hunter2", "postgresql://u@db/x?password="),
        ("PGPASSWORD=hunter2 okg run", "hunter2", "PGPASSWORD="),
        (
            "$ okg migrate --dsn postgresql://postgres:hunter2@127.0.0.1:5432/okg --apply",
            "hunter2",
            "--dsn postgresql://postgres:***@127.0.0.1:5432/okg --apply",
        ),
    ],
)
def test_reviewer_redact_hides_every_database_password_form(
    raw: str, secret: str, kept: str,
) -> None:
    import sys

    sys.path.insert(0, str(AKMON_SCRIPTS))
    try:
        from _common import redact
    finally:
        sys.path.remove(str(AKMON_SCRIPTS))
    masked = redact(raw)
    assert secret not in masked
    assert "ter2" not in masked
    assert kept in masked


def test_reproduce_sends_step_stderr_to_the_log_not_the_terminal(
    tmp_path: Path, capfd,
) -> None:
    noisy = _python_step(
        "import sys\n"
        "for i in range(30):\n"
        "    print(f'noise-{i} postgresql://u:hunter2@db/x', file=sys.stderr)\n"
        "print('STEP PASS value=1')\n"
    )
    code, log_path, transcript = _run_reproduce_steps(
        tmp_path, [("noisy", noisy, "STEP PASS")],
    )
    out, err = capfd.readouterr()
    assert code == 0
    assert "STEP PASS value=1" in out
    assert f"REPRODUCE PASS steps=1/1 transcript={transcript}" in out
    assert "noise-" not in out + err
    assert "noise-" not in transcript.read_text(encoding="utf-8")
    log = log_path.read_text(encoding="utf-8")
    assert all(f"noise-{i} " in log for i in range(30))
    assert "hunter2" not in log
    assert "postgresql://u:***@db/x" in log


def test_reproduce_failure_prints_the_stderr_tail_and_the_log_path(
    tmp_path: Path, capfd,
) -> None:
    reproduce = _reproduce_module()
    never = tmp_path / "third-step-ran"
    failing = _python_step(
        "import sys\n"
        "for i in range(30):\n"
        "    print(f'err-{i}', file=sys.stderr)\n"
        "print('FAIL second: expected outcome absent')\n"
        "raise SystemExit(3)\n"
    )
    steps = [
        ("first", _python_step("import sys; print('quiet-noise', file=sys.stderr); print('A PASS')"), "A PASS"),
        ("second", failing, "B PASS"),
        ("third", _python_step(f"open({str(never)!r}, 'w').close()"), "C PASS"),
    ]
    code, log_path, transcript = _run_reproduce_steps(tmp_path, steps)
    out, err = capfd.readouterr()
    assert code == 1
    assert err == ""
    assert not never.exists()
    assert "quiet-noise" not in out
    lines = out.splitlines()
    assert "FAIL second: expected outcome absent" in lines
    assert (
        f"--- last {reproduce.STDERR_TAIL_LINES} of 30 stderr lines from second "
        f"(full log: {log_path}):"
    ) in lines
    shown = [line for line in lines if line.startswith("  | ")]
    assert shown == [f"  | err-{i}" for i in range(10, 30)]
    assert lines[-1] == (
        f"REPRODUCE FAIL at second (exit 3); transcript: {transcript}; "
        f"stderr log: {log_path}"
    )
    # The transcript keeps the same failure report as the terminal.
    assert transcript.read_text(encoding="utf-8").splitlines()[-4:] == lines[-4:]
    log = log_path.read_text(encoding="utf-8")
    assert "quiet-noise" in log and "err-0" in log and "=== second exit=3" in log


def test_reproduce_failure_with_no_stderr_says_so(tmp_path: Path, capfd) -> None:
    code, log_path, transcript = _run_reproduce_steps(
        tmp_path, [("silent", _python_step("raise SystemExit(1)"), "S PASS")],
    )
    out, _err = capfd.readouterr()
    assert code == 1
    fail_line = (
        f"REPRODUCE FAIL at silent (exit 1); transcript: {transcript}; "
        f"stderr log: {log_path}"
    )
    expected = [f"--- silent wrote nothing to stderr (log: {log_path})", fail_line]
    assert out.splitlines()[-2:] == expected
    assert transcript.read_text(encoding="utf-8").splitlines()[-2:] == expected


def test_reproduce_fails_a_step_that_exits_zero_without_its_pass_line(
    tmp_path: Path, capfd,
) -> None:
    never = tmp_path / "second-step-ran"
    quiet_exit = _python_step(
        "import sys\n"
        "print('warning: claim could not be checked', file=sys.stderr)\n"
        "print('  did some work')\n"
    )
    steps = [
        ("claim", quiet_exit, "CLAIM demo PASS"),
        ("next", _python_step(f"open({str(never)!r}, 'w').close()"), "N PASS"),
    ]
    code, log_path, transcript = _run_reproduce_steps(tmp_path, steps)
    out, _err = capfd.readouterr()
    assert code == 1
    assert not never.exists()
    lines = out.splitlines()
    fail_line = (
        f"REPRODUCE FAIL at claim (exit 0, no 'CLAIM demo PASS' line); "
        f"transcript: {transcript}; stderr log: {log_path}"
    )
    assert lines[-4:] == [
        "--- claim exited 0 but printed no 'CLAIM demo PASS' line",
        f"--- last 1 of 1 stderr lines from claim (full log: {log_path}):",
        "  | warning: claim could not be checked",
        fail_line,
    ]
    assert "REPRODUCE PASS" not in out


def test_reproduce_waits_for_stderr_written_after_the_step_exits(
    tmp_path: Path, capfd,
) -> None:
    # The step exits at once, but a child it started keeps the stderr pipe open
    # and writes to it later. The failure report must still carry that line,
    # which it can only do if reproduce waits for the stderr reader to finish.
    late = _python_step(
        "import subprocess, sys\n"
        "subprocess.Popen([sys.executable, '-c', "
        "\"import sys, time; time.sleep(1.0); print('late-err', file=sys.stderr)\"], "
        "stdout=subprocess.DEVNULL)\n"
        "raise SystemExit(2)\n"
    )
    code, log_path, _transcript = _run_reproduce_steps(tmp_path, [("late", late, "L PASS")])
    out, _err = capfd.readouterr()
    assert code == 1
    assert "  | late-err" in out.splitlines()
    log = log_path.read_text(encoding="utf-8").splitlines()
    assert log.index("late-err") < log.index("=== late exit=2")


def test_reproduce_refuses_a_log_path_it_cannot_use(tmp_path: Path, capfd) -> None:
    reproduce = _reproduce_module()
    dsn = "postgresql://nobody@127.0.0.1:1/none"

    same = tmp_path / "same.txt"
    assert reproduce.main([
        "--dsn", dsn, "--workdir", str(tmp_path / "w1"),
        "--transcript", str(same), "--log", str(same),
    ]) == 1
    out, _err = capfd.readouterr()
    refusal = "REPRODUCE FAIL at setup: the stderr log and the transcript must be different files"
    assert out.splitlines()[-1] == refusal
    assert same.read_text(encoding="utf-8").splitlines()[-1] == refusal

    # A log inside the checkout that git does not ignore would be an untracked
    # file, and every publish refuses a checkout with untracked files.
    inside = ROOT / "reproduce-log-refusal-probe.log"
    transcript = tmp_path / "w2" / "transcript.txt"
    try:
        assert reproduce.main([
            "--dsn", dsn, "--workdir", str(tmp_path / "w2"), "--log", str(inside),
        ]) == 1
        assert not inside.exists()
    finally:
        # Never leave an untracked file in the checkout, even on a regression.
        inside.unlink(missing_ok=True)
    out, _err = capfd.readouterr()
    assert "stderr log is inside this checkout and not git-ignored" in out.splitlines()[-1]
    assert "not git-ignored" in transcript.read_text(encoding="utf-8").splitlines()[-1]

    # A log that cannot be opened is reported in the transcript, not as a traceback.
    blocked = tmp_path / "is-a-directory"
    blocked.mkdir()
    transcript = tmp_path / "w3" / "transcript.txt"
    assert reproduce.main([
        "--dsn", dsn, "--workdir", str(tmp_path / "w3"), "--log", str(blocked),
    ]) == 1
    out, _err = capfd.readouterr()
    last = out.splitlines()[-1]
    assert last.startswith("REPRODUCE FAIL at setup: cannot write the stderr log: "), last
    assert transcript.read_text(encoding="utf-8").splitlines()[-1] == last


def test_reproduce_creates_the_log_directory(tmp_path: Path, monkeypatch, capfd) -> None:
    reproduce = _reproduce_module()
    seen: list[Path] = []

    def fake_run_steps(steps, *, env, tee, log, log_path, transcript):
        seen.append(log_path)
        assert [step.pass_prefix for step in steps] == [
            "QUICKSTART PASS",
            "CLAIM admission-rejection PASS",
            "CLAIM fork-merge-readmission PASS",
            "CLAIM lens-round-trip PASS",
        ]
        return 0

    monkeypatch.setattr(reproduce, "run_steps", fake_run_steps)
    log_path = tmp_path / "nested" / "logs" / "run.log"
    assert reproduce.main([
        "--dsn", "postgresql://nobody@127.0.0.1:1/none",
        "--workdir", str(tmp_path / "w"), "--log", str(log_path),
    ]) == 0
    capfd.readouterr()
    assert seen == [log_path] and log_path.is_file()


def test_claim_scripts_refuse_without_a_quickstart_state_file(tmp_path: Path) -> None:
    for script in (
        "claim_admission_rejection.py",
        "claim_fork_merge_readmission.py",
        "claim_lens_round_trip.py",
    ):
        result = _script(
            script, "--state", str(tmp_path / "missing.json"),
            "--dsn", "postgresql://unused.invalid/none",
        )
        assert result.returncode == 1, script
        assert "reviewer state file not found" in result.stdout, script


@pytest.mark.postgres
def test_reviewer_reproduce_publishes_and_every_claim_passes_then_fails_closed(
    schema_destructive_runtime: str,
    tmp_path: Path,
    record_testsuite_property,
) -> None:
    dsn = schema_destructive_runtime
    workdir = tmp_path / "reviewer"
    result = _script("reproduce.py", "--dsn", dsn, "--workdir", str(workdir))
    output = result.stdout
    assert result.returncode == 0, output[-6000:] + result.stderr[-2000:]
    assert "REPRODUCE PASS steps=4/4" in output
    # The steps' stderr (okg progress lines, crash-evidence records) goes to
    # the log, so the reviewer's terminal gets none of it.
    log_path = workdir / "reproduce.log"
    assert f"stderr log: {log_path} " in output
    assert result.stderr == ""
    assert "okg.source_tail" not in output
    assert log_path.is_file()

    quickstart = _pass_line(output, "QUICKSTART")
    fields = dict(item.split("=", 1) for item in quickstart.split()[2:])
    assert fields["generation"].startswith("okg:")
    # The head after the catalog hand-over republish, not the verify publish.
    assert fields["verified"].startswith("okg:")
    assert fields["generation"] != fields["verified"]
    assert int(fields["nodes"]) > 0
    assert int(fields["edges"]) > 0
    record_testsuite_property("reviewer_generation_id", fields["generation"])
    record_testsuite_property("reviewer_node_count", fields["nodes"])
    record_testsuite_property("reviewer_edge_count", fields["edges"])

    assert _pass_line(output, "CLAIM admission-rejection") == (
        "CLAIM admission-rejection PASS status=rejected "
        "verdict=narrowing_violation facts=0"
    )
    fork = _pass_line(output, "CLAIM fork-merge-readmission")
    assert "merged=okg:" in fork
    # Before #2865 is fixed the violation is caught at landing and rolled
    # back; after it, the staging gate catches it. The script's own checks
    # (blocked with a reason, narrowing violation named, default unchanged,
    # no forbidden edge on default) hold either way before it prints PASS.
    caught = re.search(r"violation=caught_at_(\S+)", fork)
    assert caught, fork
    if caught.group(1) == "landing_and_rolled_back":
        assert "(issue #2865)" in output
    else:
        assert not caught.group(1).startswith("land"), fork
    lens = _pass_line(output, "CLAIM lens-round-trip")
    assert "putget=ok stale=refused promoted=okg:" in lens
    assert "producer_profile=materialized_by_script" in lens
    assert "(edited through a lens)'" in output
    for line in output.splitlines():
        if line.startswith(("CLAIM", "QUICKSTART")):
            record_testsuite_property(line.split(" PASS")[0].replace(" ", "_"), line)

    transcript = (workdir / "transcript.txt").read_text(encoding="utf-8")
    assert "REPRODUCE PASS steps=4/4" in transcript
    password = psycopg.conninfo.conninfo_to_dict(dsn).get("password")
    if password:
        assert f":{password}@" not in transcript
        assert ":***@" in transcript

    state = str(workdir / "quickstart" / "reviewer-state.json")
    name = json.loads(Path(state).read_text(encoding="utf-8"))["deployment_name"]
    document = f"doc:{name}:records:rec-2"

    # Failure cases: each claim script exits 1 and names the missing outcome.
    # An edge the catalog allows (record -> document) is accepted, not
    # rejected, so the admission claim must fail.
    allowed = _script(
        "claim_admission_rejection.py", "--state", state, "--dsn", dsn,
        "--dst", document,
    )
    assert allowed.returncode == 1, allowed.stdout[-3000:]
    assert "FAIL admission-rejection: envelope was not rejected" in allowed.stdout

    # A "stale" value equal to the current title is not stale: the edit
    # commits, so the stale-refusal half must fail.
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        rows = store.checkout("default").query(
            "SELECT attrs->>'title' AS title FROM okg.graph_nodes "
            "WHERE node_id = %(node)s",
            {"node": f"{name}:records:record:rec-1"},
        )
    not_stale = _script(
        "claim_lens_round_trip.py", "--state", state, "--dsn", dsn,
        "--stale-value", rows[0]["title"],
    )
    assert not_stale.returncode == 1, not_stale.stdout[-3000:]
    assert "FAIL lens-round-trip: stale edit was not refused" in not_stale.stdout


@pytest.mark.postgres
def test_reviewer_fork_claim_fails_when_the_second_fork_is_allowed(
    schema_destructive_runtime: str,
    tmp_path: Path,
) -> None:
    """The fork claim runs on its own deployment, so this failure is the one
    under test and not a side effect of the claims reproduce.py ran."""
    dsn = schema_destructive_runtime
    workdir = tmp_path / "reviewer"
    quickstart = _script(
        "reviewer_quickstart.py", "--dsn", dsn, "--workdir", str(workdir),
    )
    assert quickstart.returncode == 0, quickstart.stdout[-4000:]
    state = str(workdir / "reviewer-state.json")
    name = json.loads(Path(state).read_text(encoding="utf-8"))["deployment_name"]

    # A fork edge the catalog allows (record -> document) merges, so the
    # "blocked" half of the claim must fail and say so.
    merges = _script(
        "claim_fork_merge_readmission.py", "--state", state, "--dsn", dsn,
        "--bad-edge-dst", f"doc:{name}:records:rec-2",
    )
    assert merges.returncode == 1, merges.stdout[-3000:]
    assert (
        "FAIL fork-merge-readmission: violating fork was not blocked"
        in merges.stdout
    )
    assert "status=merged" in merges.stdout
