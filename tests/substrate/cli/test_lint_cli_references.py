"""Smoke test for `scripts/lint_cli_references.py`.

Per `align-substrate-cli-docs-and-runbooks` Phase 5: the lint runs
without crashing on the current docs tree. The hard "exit zero"
assertion lands after Phase 3 sweeps the remaining offenders.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
LINT_SCRIPT = REPO_ROOT / "scripts/lint_cli_references.py"


def test_lint_script_exits_zero_on_clean_tree() -> None:
    """After Phase 3 swept the docs, the lint SHALL exit zero
    on the current state. Regressions surface here.
    """
    proc = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), "--quiet"],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    assert proc.returncode == 0, (
        f"lint reported offenders:\n{proc.stdout}\n"
        f"stderr:\n{proc.stderr}"
    )


def test_surface_mode_enforces_exhaustive_audit_and_manifest_only() -> None:
    """The curated hot-path reference is validated, not made exhaustive."""
    proc = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), "--surface"],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    assert proc.returncode == 0, (
        f"surface lint reported offenders:\n{proc.stdout}\n"
        f"stderr:\n{proc.stderr}"
    )
    assert "smoke-manifest entries" in proc.stderr
    assert "audit + manifest entries" not in proc.stderr
    assert "reference" not in proc.stderr


def test_lint_recognizes_new_phase2_verbs(tmp_path) -> None:
    """After Phase 2 added `okg generation list` and
    `okg catalog history`, the lint SHALL NOT flag them.

    The fragment lives in pytest's tmp dir and reaches the lint via
    --extra-file: writing temp files into the shared docs/ tree raced
    concurrently-running docs scanners under xdist (FileNotFoundError
    when the fragment was unlinked mid-glob).
    """
    import textwrap
    fragment_path = tmp_path / "fragment.md"
    fragment_path.write_text(textwrap.dedent("""
        Use `okg generation list --limit 5` to see recent
        generations. For catalog history: `okg catalog history`.
    """), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), "--quiet",
         "--extra-only", "--extra-file", str(fragment_path)],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    offenders_in_fragment = [
        line for line in proc.stdout.splitlines()
        if fragment_path.name in line
    ]
    assert offenders_in_fragment == [], (
        f"lint flagged the new Phase-2 verbs as unknown: "
        f"{offenders_in_fragment}"
    )


def test_lint_ignores_env_var_assignment_and_flags_fictional_verb(tmp_path) -> None:
    """`PGPASSWORD=okg createdb ...` is shell env-var assignment, not an
    `okg createdb` invocation. The lint MUST treat the `okg` after `=` as a
    value and skip it. As a counter-pin: a real `okg <fictional-verb>`
    invocation in the same fragment MUST still be flagged.
    """
    import textwrap
    fragment_path = tmp_path / "fragment.md"
    fragment_path.write_text(textwrap.dedent("""
        Bootstrap the test DB with:

        ```bash
        PGPASSWORD=okg createdb -h localhost -p 5433 -U postgres okg_test
        PGPASSWORD=okg psql -h localhost -p 5433 -U postgres -d okg_test
        ```

        Then run `okg pact-flux --json` for the next step.
    """), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), "--quiet",
         "--extra-only", "--extra-file", str(fragment_path)],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    offenders_in_fragment = [
        line for line in proc.stdout.splitlines()
        if fragment_path.name in line
    ]
    # `PGPASSWORD=okg createdb` and `PGPASSWORD=okg psql` MUST NOT appear.
    assert not any("createdb" in line for line in offenders_in_fragment), (
        f"lint falsely flagged `=okg createdb` as a CLI invocation: "
        f"{offenders_in_fragment}"
    )
    assert not any("psql" in line for line in offenders_in_fragment), (
        f"lint falsely flagged `=okg psql` as a CLI invocation: "
        f"{offenders_in_fragment}"
    )
    # `okg pact-flux` is a real CLI invocation of a fictional verb and
    # MUST still be flagged.
    assert any("pact-flux" in line for line in offenders_in_fragment), (
        f"lint failed to flag the fictional `okg pact-flux` invocation: "
        f"{offenders_in_fragment}"
    )


def test_lint_rejects_fictional_nested_subcommands(tmp_path) -> None:
    """Every argparse command-group level remains authoritative."""
    import textwrap
    fragment_path = tmp_path / "fictional-deep-commands.md"
    fragment_path.write_text(textwrap.dedent("""
        `okg pact fictional-action`
        `okg pact evidence fictional-action`
    """), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), "--quiet",
         "--extra-only", "--extra-file", str(fragment_path)],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    offenders_in_fragment = [
        line for line in proc.stdout.splitlines()
        if fragment_path.name in line
    ]
    assert proc.returncode == 1
    assert any(
        "okg pact fictional-action" in line
        for line in offenders_in_fragment
    ), offenders_in_fragment
    assert any(
        "okg pact evidence fictional-action" in line
        for line in offenders_in_fragment
    ), offenders_in_fragment


def test_lint_rejects_prose_words_used_as_explicit_deep_commands(tmp_path) -> None:
    """Prose allowlist words remain fictional inside command syntax."""
    import textwrap
    fragment_path = tmp_path / "fictional-prose-word-commands.md"
    fragment_path.write_text(textwrap.dedent("""
        Inline: `okg pact evidence stores`

        ```bash
        okg pact evidence deployment
        ```

        - okg pact evidence fictional-action
        1. okg pact evidence fictional-action
        Command: okg pact evidence fictional-action
        - [ ] okg pact evidence fictional-checkbox
        12) okg pact evidence fictional-numbered
        command: okg pact evidence fictional-labelled
    """), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), "--quiet",
         "--extra-only", "--extra-file", str(fragment_path)],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    offenders_in_fragment = [
        line for line in proc.stdout.splitlines()
        if fragment_path.name in line
    ]
    assert proc.returncode == 1
    assert any(
        "okg pact evidence stores" in line
        for line in offenders_in_fragment
    ), offenders_in_fragment
    assert any(
        "okg pact evidence deployment" in line
        for line in offenders_in_fragment
    ), offenders_in_fragment
    assert sum(
        "okg pact evidence fictional-action" in line
        for line in offenders_in_fragment
    ) == 3, offenders_in_fragment
    for fictional in (
        "fictional-checkbox",
        "fictional-numbered",
        "fictional-labelled",
    ):
        assert any(
            f"okg pact evidence {fictional}" in line
            for line in offenders_in_fragment
        ), offenders_in_fragment


def test_lint_preserves_non_command_tokens_and_nested_aliases(tmp_path) -> None:
    """Leaf arguments and command-adjacent prose are not subcommands."""
    import textwrap
    fragment_path = tmp_path / "valid-deep-commands.md"
    fragment_path.write_text(textwrap.dedent("""
        `okg pact evidence attach demo req.demo --kind executed_test`
        `okg pact evidence status demo --json`
        `okg pact new-change demo`
        `okg pact view <change-id> --format=bootstrap`
        `okg pact --help`
        OKG_DEPLOYMENT=demo okg pact gate post-edit demo
        PGPASSWORD=okg createdb -h localhost okg_test
        The okg pact is the repository change workflow.
        The okg pact evidence stores its proof records.
        The okg pact workflow remains available to contributors.
        The okg pact evidence model remains intentionally narrow.
        Use okg pact to manage repository changes.
        Run okg pact evidence as the proof store.
    """), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), "--quiet",
         "--extra-only", "--extra-file", str(fragment_path)],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    offenders_in_fragment = [
        line for line in proc.stdout.splitlines()
        if fragment_path.name in line
    ]
    assert offenders_in_fragment == [], (
        f"lint treated non-command tokens as nested commands: "
        f"{offenders_in_fragment}"
    )


def test_lint_rejects_fictional_fourth_level_subcommand(tmp_path) -> None:
    """A fourth command-group level is validated, not treated as an argument."""
    import textwrap
    fragment_path = tmp_path / "fictional-fourth-level.md"
    fragment_path.write_text(textwrap.dedent("""
        `okg pact coord setup plan`
        `okg pact coord setup fictional-action`
    """), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), "--quiet",
         "--extra-only", "--extra-file", str(fragment_path)],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    offenders_in_fragment = [
        line for line in proc.stdout.splitlines()
        if fragment_path.name in line
    ]
    assert proc.returncode == 1
    assert any(
        "okg pact coord setup fictional-action" in line
        for line in offenders_in_fragment
    ), offenders_in_fragment
    assert not any(
        "okg pact coord setup plan" in line
        for line in offenders_in_fragment
    ), offenders_in_fragment


def test_lint_keeps_prose_followers_prose_in_every_context(tmp_path) -> None:
    """`okg` is also the project name; an English word after it stays prose."""
    import textwrap
    fragment_path = tmp_path / "project-name-prose.md"
    fragment_path.write_text(textwrap.dedent("""
        The `okg sessions` corpus is ingested nightly.
        See `okg repo` layout notes.

        - okg is the substrate.
        1. okg sessions are ingested nightly.

        ```text
        okg deployments are cheap to create.
        ```
    """), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), "--quiet",
         "--extra-only", "--extra-file", str(fragment_path)],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    offenders_in_fragment = [
        line for line in proc.stdout.splitlines()
        if fragment_path.name in line
    ]
    assert offenders_in_fragment == [], (
        f"lint treated the project name in prose as a command: "
        f"{offenders_in_fragment}"
    )


def test_lint_fence_tracking_ignores_nested_info_string_fences(tmp_path) -> None:
    """A closing fence carries no info string, so nested fences do not desync."""
    import textwrap
    fragment_path = tmp_path / "nested-fences.md"
    fragment_path.write_text(textwrap.dedent("""
        ````markdown
        ```bash
        okg pact evidence fictional-inner
        ```
        ````

        The okg pact evidence model remains intentionally narrow.
    """), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(LINT_SCRIPT), "--quiet",
         "--extra-only", "--extra-file", str(fragment_path)],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    offenders_in_fragment = [
        line for line in proc.stdout.splitlines()
        if fragment_path.name in line
    ]
    assert proc.returncode == 1
    assert any(
        "okg pact evidence fictional-inner" in line
        for line in offenders_in_fragment
    ), offenders_in_fragment
    assert not any(
        "evidence model" in line for line in offenders_in_fragment
    ), offenders_in_fragment


def test_lint_connectives_are_never_command_names() -> None:
    """The connective allowlist may not shadow a real argparse choice."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("_lint", LINT_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    names: set[str] = set()

    def walk(tree) -> None:
        for name, children in tree.items():
            names.add(name)
            walk(children)

    walk(module._build_command_tree())
    collisions = sorted(module._PROSE_CONNECTIVES & names)
    assert collisions == [], (
        f"connective allowlist would hide real commands: {collisions}"
    )


def _lint_source_fragment(tmp_path, source: str) -> subprocess.CompletedProcess:
    """Lint one synthetic Python module and nothing else."""
    import textwrap
    module_path = tmp_path / "hint_fragment.py"
    module_path.write_text(textwrap.dedent(source), encoding="utf-8")
    return subprocess.run(
        [sys.executable, str(LINT_SCRIPT), "--quiet", "--extra-only",
         "--extra-file", str(module_path)],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )


def test_source_lint_flags_fictional_fix_hint(tmp_path) -> None:
    """Issue #2201: a source fix hint named `okg catalog apply`, which the
    parser has never registered. The hint below is the pre-#2294 text."""
    proc = _lint_source_fragment(tmp_path, '''
        def refuse(error_type):
            raise error_type(
                "declaration changed",
                fix_hint="run `okg catalog apply` to recompose this catalog",
            )


        def refuse_formatted(error_type, name):
            raise error_type(
                "declaration changed",
                fix_hint=f"run `okg catalog apply --deployment {name}`",
            )


        HINT = {"fix_hint": "okg catalog reconcile --deployment <name>"}
        LOGGED = (
            "the token is in an env var. Move it with: okg pi set-token "
            "--deployment %s --provider %s"
        )
        SPLIT = (
            "revoke with `okg nomos identity binding upsert "
            "--status disabled`"
        )
    ''')
    assert proc.returncode == 1, proc.stdout + proc.stderr
    flagged = proc.stdout.splitlines()
    assert sum("okg catalog apply" in line for line in flagged) == 2, flagged
    for reference in (
        "okg catalog reconcile",
        "okg pi",
        "okg nomos identity",
    ):
        assert any(reference in line for line in flagged), (reference, flagged)


def test_source_lint_accepts_real_command_hints(tmp_path) -> None:
    """Registered commands pass in every command context the rule reads."""
    proc = _lint_source_fragment(tmp_path, '''
        def refuse(error_type, name, verb):
            raise error_type(
                "declaration changed",
                fix_hint=(
                    "run `okg catalog load --deployment <deployment> "
                    "--apply` to recompose this catalog"
                ),
            )


        HINT = {"fix_hint": "okg branch list --deployment <name>"}
        LOGGED = "Move it with: okg set-token --deployment x --provider canvas"
        RUN = f"Run okg generation list --deployment {0} to see it"
        FIELD = f"run `okg {0} apply`"
    ''')
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_source_lint_leaves_prose_about_okg_alone(tmp_path) -> None:
    """`okg` is also the project name; sentences about it are not commands."""
    proc = _lint_source_fragment(tmp_path, '''
        """Module notes: `okg fictional-docstring-verb` is not checked."""


        def arm():
            """Docstrings are developer notes: `okg import-cluster-jobs`."""
            return [
                "okg arm requires a built corpus deployment",
                "okg telemetry requires --deployment or OKG_DEPLOYMENT",
                "report this as an okg substrate defect",
                "no okg pact command clears this blocker",
                "set it to the absolute path of the okg executable",
                "run `uv tool install <okg wheel or source>` first",
            ]
    ''')
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_source_lint_accepts_pact_verbs_dispatched_before_argparse(tmp_path) -> None:
    """`okg pact approve` runs before argparse, so the parser never sees it.

    The lint takes those verbs from the dispatcher's own set, so a hint that
    names one is not reported as fictional.
    """
    proc = _lint_source_fragment(tmp_path, '''
        HINT = {"fix_hint": "okg pact approve <id> --by <operator>"}
        TEXT = "record it with `okg pact approve cli-hint-lint --by Riley`"
    ''')
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_source_lint_leaves_english_after_a_colon_or_verb_alone(tmp_path) -> None:
    """After a colon or "run"/"then"/"try", `okg` can still start a sentence.

    Such a run is checked only when its first word is a real verb or a
    `--flag` follows, so these four sentences pass.
    """
    proc = _lint_source_fragment(tmp_path, '''
        MESSAGES = [
            "Error: okg failed to connect",
            "reason: okg refused the write",
            "then okg publishes the generation",
            "try okg locally before deploying",
        ]
    ''')
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_source_lint_reads_a_command_after_run_or_try(tmp_path) -> None:
    """A bare command after "run"/"try" is checked without backticks."""
    proc = _lint_source_fragment(tmp_path, '''
        FIRST = "Run okg catalog apply to recompose the catalog"
        SECOND = "if that fails, try okg catalog reconcile instead"
    ''')
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "okg catalog apply" in proc.stdout, proc.stdout
    assert "okg catalog reconcile" in proc.stdout, proc.stdout


def test_source_lint_reads_a_command_after_a_shell_prompt(tmp_path) -> None:
    """A `$` prompt marks a command line even with no backticks."""
    proc = _lint_source_fragment(tmp_path, '''
        EXAMPLE = "$ okg catalog apply --deployment demo"
    ''')
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "okg catalog apply" in proc.stdout, proc.stdout


def test_source_lint_reports_the_source_line_of_each_command(tmp_path) -> None:
    """The line is where the command is written, not start + value newlines.

    An escaped newline adds a value line that is not a source line, and
    implicitly joined pieces add source lines the value does not show.
    """
    proc = _lint_source_fragment(tmp_path, '''
        ESCAPED = "first line\\nsecond line: run `okg catalog apply`"
        JOINED = (
            "one piece, "
            "another piece, "
            "then `okg catalog reconcile`"
        )
    ''')
    assert proc.returncode == 1, proc.stdout + proc.stderr
    by_reference = {
        line.split(": ", 1)[1].split("  --")[0]: line.split(":")[1]
        for line in proc.stdout.splitlines()
    }
    # The fragment is dedented; its first line is blank, so ESCAPED is line 2.
    assert by_reference == {
        "okg catalog apply": "2",
        "okg catalog reconcile": "6",
    }, proc.stdout


def test_stale_allowlist_entry_fails_the_scan(monkeypatch, capsys) -> None:
    """An allowlist entry that matches nothing is itself an offender."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "_lint_allowlist_under_test", LINT_SCRIPT,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    target = "src/okg/substrate/cli/parser_groups.py"
    real_entry = (target, "okg bootstrap")
    assert real_entry in module._SOURCE_ALLOWLIST
    argv = ["--quiet", "--extra-only", "--extra-file", str(REPO_ROOT / target)]

    monkeypatch.setattr(
        module, "_SOURCE_ALLOWLIST", {real_entry: "the real refusal"},
    )
    assert module.main(argv) == 0, capsys.readouterr().out

    monkeypatch.setattr(module, "_SOURCE_ALLOWLIST", {
        real_entry: "the real refusal",
        (target, "okg no-such-verb"): "stale on purpose",
    })
    assert module.main(argv) == 1
    out = capsys.readouterr().out
    assert "okg no-such-verb  -- allowlist entry matches nothing" in out, out
