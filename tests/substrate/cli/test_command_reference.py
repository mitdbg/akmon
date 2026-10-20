"""okg commands are built from, and checked against, the CLI's own parser.

Issue #2575. ``okg_command`` refuses a command the parser would reject, and
``test_every_okg_command_call_in_src_matches_the_parser`` reads every call in
``src/`` without running it, so a wrong reference fails here even on a code
path no other test reaches.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

from okg.substrate.cli import command_reference
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.cli.command_reference import (
    Masked,
    check_reference,
    is_credential_option,
    okg_command,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC = REPO_ROOT / "src"
DSN = "postgresql://postgres:hunter2@127.0.0.1:1/okg_probe"
MASKED_DSN = Masked(DSN, "postgresql://postgres:***@127.0.0.1:1/okg_probe")


def _parses(tokens: list[str]) -> None:
    """The real okg parser accepts ``tokens`` (argparse exits 2 if not)."""
    command_reference._parser_tree().parse_args(tokens)


# ---------- building ----------


def test_argv_starts_the_running_okg_and_display_starts_with_okg() -> None:
    command = okg_command("doctor", deployment="lab", check="source-auth")
    tail = ["doctor", "--deployment", "lab", "--check", "source-auth"]
    assert command.argv() == [*running_okg_argv(), *tail]
    assert command.argv()[0] == sys.executable
    assert command.display() == "okg " + " ".join(tail)
    assert str(command) == command.display()
    _parses(tail)


def test_unset_flags_are_left_out_never_printed_as_none() -> None:
    command = okg_command(
        "install", profile="academic-researcher", orcid=None, name="lab",
        dry_run=False, json=True,
    )
    assert command.display() == (
        "okg install --profile academic-researcher --name lab --json"
    )
    assert "None" not in command.display()
    assert "False" not in command.display()


def test_a_masked_value_is_real_in_argv_and_masked_in_display() -> None:
    command = okg_command("install", name="lab", postgres_dsn=MASKED_DSN)
    assert DSN in command.argv()
    assert "hunter2" not in command.display()
    assert "--postgres-dsn 'postgresql://postgres:***@127.0.0.1:1/okg_probe'" in (
        command.display()
    )
    assert "hunter2" not in repr(MASKED_DSN)


def test_value_shapes_follow_the_parser() -> None:
    # An append option repeats; a multi-value option takes one flag.
    appended = okg_command("db cold-history", suffix=["a", "b"])
    assert appended.display().endswith("--suffix a --suffix b")
    many = okg_command("up", include=["x", "y"])
    assert many.display().endswith("--include x y")
    # BooleanOptionalAction: False is meaningful and prints --no-<flag>.
    both = okg_command("deployment up", sync_code=True, sync_deps=False)
    assert "--sync-code" in both.display()
    assert "--no-sync-deps" in both.display()
    # A choice and a positional.
    chosen = okg_command("ingest", deployment="lab", mode="append")
    assert chosen.display().endswith("--mode append")
    positional = okg_command("telemetry profile", "report", trace_id="t1")
    assert positional.display() == "okg telemetry profile report --trace-id t1"
    # Shell metacharacters are quoted for display, raw in argv.
    spaced = okg_command("doctor", deployment="my lab")
    assert spaced.display() == "okg doctor --deployment 'my lab'"
    assert spaced.argv()[-1] == "my lab"
    # A value starting with `-` is joined to its option so argparse cannot
    # read it as another option.
    dashed = okg_command("doctor", deployment="-lab", check="source-auth")
    assert dashed.argv()[-3:] == ["--deployment=-lab", "--check", "source-auth"]
    args = command_reference._parser_tree().parse_args(dashed.argv()[4:])
    assert args.deployment == "-lab"
    # True on an optional-value option is the bare option (its const).
    # `profile=` names the deprecated spelling of `--capture` (decision 310);
    # the builder prints the new spelling, never the deprecated one.
    bare = okg_command("ingest", deployment="lab", profile=True)
    assert bare.display().endswith("--capture")
    assert command_reference._parser_tree().parse_args(
        bare.argv()[4:]
    ).profile is not None
    # Every case above is a command the real parser accepts.
    for command in (appended, many, both, chosen, positional, spaced, bare):
        _parses(command.argv()[len(running_okg_argv()):])


@pytest.mark.parametrize("name", ["lab", "-lab", "--", "a b", "x=y", "instance"])
def test_no_deployment_name_makes_the_install_check_hint_fail(name: str) -> None:
    """The packaged install builds this hint mid-install; a name must never
    turn it into a refusal, and the parser must read the name back."""
    command = okg_command("doctor", deployment=name, check="source-auth")
    args = command_reference._parser_tree().parse_args(command.argv()[4:])
    assert (args.deployment, args.check) == (name, "source-auth")


def test_only_options_whose_value_is_a_credential_must_be_masked() -> None:
    """Pins the rule against the live option names (design review finding:
    a word match forced masking on variable names such as --dsn-env)."""
    import argparse

    options: set[str] = set()

    def walk(parser: argparse.ArgumentParser) -> None:
        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                for sub in action.choices.values():
                    walk(sub)
            options.update(o for o in action.option_strings if o.startswith("--"))

    walk(command_reference._parser_tree())
    secret = {o for o in options if is_credential_option(o)}
    for option in ("--postgres-dsn", "--dsn", "--database-password", "--auth-token"):
        assert option in secret
    for option in (
        "--dsn-env", "--token-env", "--install-dsn-ref", "--client-secret-env",
        "--run-key", "--public-key", "--key-fingerprint",
    ):
        assert option in options and option not in secret


# ---------- refusing ----------


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (lambda: okg_command("doctor sources"), "`sources` is not a command"),
        (lambda: okg_command("nosuchverb"), "has no subcommand `nosuchverb`"),
        (lambda: okg_command("db"), "is a command group"),
        (lambda: okg_command("pact check"), "second parser"),
        (lambda: okg_command("doctor", bogus="x"), "has no option --bogus"),
        (lambda: okg_command("doctor", bogus=None), "has no option --bogus"),
        (lambda: okg_command("install", json="yes"), "is an on/off flag"),
        (lambda: okg_command("doctor", deployment=True), "takes a value"),
        (lambda: okg_command("ingest", deployment="d", mode="sideways"), "does not accept 'sideways'"),
        (lambda: okg_command("doctor", deployment=["a", "b"]), "takes one value"),
        (lambda: okg_command("deployment up", sync_code="yes"), "is on/off"),
        (lambda: okg_command("db drop", cluster_dsn=MASKED_DSN), "requires --name"),
        (lambda: okg_command("db migrate-down"), "takes 1 positional"),
        (lambda: okg_command("doctor", "extra"), "takes 0 positional"),
        (lambda: okg_command("telemetry profile", "sideways"), "does not accept 'sideways'"),
        (lambda: okg_command("db migrate-down", "-1"), "starts with '-'"),
        (lambda: okg_command("install", postgres_dsn=DSN), "carries a credential"),
        (lambda: okg_command("doctor", help=True), "is not a command flag"),
        (
            lambda: okg_command("new-module", "m", in_tree=True, deployment="d"),
            "cannot be used together",
        ),
        (lambda: okg_command("poros proof"), "requires one of"),
        (lambda: okg_command("deployment up", no_sync_code=True), "pass False"),
        (lambda: okg_command("up", include=["-x"]), "starts with '-'"),
        (lambda: okg_command("doctor", deployment=[]), "takes one value"),
        (lambda: okg_command("doctor", generation="abc"), "takes a int"),
        (
            lambda: okg_command("agent-runtime-launch", "echo", runtime="x"),
            "pass-through argument",
        ),
    ],
)
def test_a_command_the_parser_would_reject_is_refused(build, message) -> None:
    with pytest.raises(ValueError) as caught:
        build()
    assert message in str(caught.value)


# ---------- the source scan ----------


def _literal(node: ast.expr) -> object:
    """A literal flag value, or a sentinel when the scan cannot read it."""
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError):
        return _UNREAD


_UNREAD = object()


def _is_okg_command_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    return (isinstance(func, ast.Name) and func.id == "okg_command") or (
        isinstance(func, ast.Attribute) and func.attr == "okg_command"
    )


def scan_source(text: str, filename: str) -> tuple[int, list[str]]:
    """Check every okg_command call in ``text``; return (calls, problems).

    A call the scan cannot read -- a verb path that is not a literal, a
    ``**flags`` spread, or ``okg_command`` imported under another name --
    is a problem, not a skip.
    """
    calls = 0
    problems: list[str] = []
    called: set[int] = set()
    for node in ast.walk(ast.parse(text, filename=filename)):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                if alias.name.rsplit(".", 1)[-1] == "okg_command" and alias.asname:
                    problems.append(
                        f"{filename}:{node.lineno}: import okg_command under "
                        "its own name so the scan can find its calls"
                    )
        if isinstance(node, ast.Call):
            called.add(id(node.func))
        if (
            isinstance(node, ast.Name)
            and node.id == "okg_command"
            and id(node) not in called
        ):
            # `f = okg_command` or `partial(okg_command, ...)` hides the
            # calls from this scan.
            problems.append(
                f"{filename}:{node.lineno}: okg_command used other than by "
                "a direct call; call it directly so the scan can check it"
            )
        if not _is_okg_command_call(node):
            continue
        calls += 1
        where = f"{filename}:{node.lineno}"
        if not node.args or not (
            isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            problems.append(f"{where}: the verb path must be a string literal")
            continue
        if any(keyword.arg is None for keyword in node.keywords):
            problems.append(f"{where}: **flags cannot be checked; name each flag")
            continue
        literal_flags = {}
        for keyword in node.keywords:
            value = _literal(keyword.value)
            if value is not _UNREAD:
                literal_flags[keyword.arg] = value
        positionals = node.args[1:]
        count = (
            None if any(isinstance(arg, ast.Starred) for arg in positionals)
            else len(positionals)
        )
        try:
            check_reference(
                node.args[0].value,
                [keyword.arg for keyword in node.keywords],
                literal_flags,
                count,
            )
        except ValueError as err:
            problems.append(f"{where}: {err}")
    return calls, problems


def scan_tree(root: Path, base: Path) -> tuple[int, list[str]]:
    """Scan every ``*.py`` under ``root`` that names okg_command at all."""
    calls = 0
    problems: list[str] = []
    for path in sorted(root.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        if "okg_command" not in text:
            continue
        found, wrong = scan_source(text, str(path.relative_to(base)))
        calls += found
        problems.extend(wrong)
    return calls, problems


def test_every_okg_command_call_in_src_matches_the_parser() -> None:
    calls, problems = scan_tree(SRC, REPO_ROOT)
    assert problems == []
    # The two sites migrated in #2575; a scan that finds none is broken.
    assert calls >= 2


def test_the_tree_scan_reads_a_file_that_only_aliases_the_builder(
    tmp_path: Path,
) -> None:
    (tmp_path / "hidden.py").write_text(
        "from okg.substrate.cli.command_reference import okg_command as build\n"
        "build('doctor sources')\n"
        "f = okg_command\n",
        encoding="utf-8",
    )
    calls, problems = scan_tree(tmp_path, tmp_path)
    assert calls == 0
    assert [problem.split(":")[1] for problem in problems] == ["1", "3"]


def test_the_source_scan_reports_a_wrong_reference_with_its_line() -> None:
    text = (
        "from okg.substrate.cli.command_reference import okg_command\n"
        "ok = okg_command('doctor', deployment=name, check='source-auth')\n"
        "old = okg_command('doctor sources')\n"
        "typo = okg_command('doctor', deploymnet=name)\n"
        "bad_choice = okg_command('ingest', deployment='d', mode='sideways')\n"
        "leak = okg_command('install', postgres_dsn='postgresql://u:p@h/d')\n"
        "dynamic = okg_command(verb)\n"
        "spread = okg_command('doctor', **flags)\n"
        "from okg.substrate.cli.command_reference import okg_command as c\n"
    )
    calls, problems = scan_source(text, "synthetic.py")
    assert calls == 7
    assert sorted(int(problem.split(":")[1]) for problem in problems) == [
        3, 4, 5, 6, 7, 8, 9,
    ]
    problems.sort(key=lambda problem: int(problem.split(":")[1]))
    assert "`sources` is not a command" in problems[0]
    assert "no option --deploymnet" in problems[1]
    assert "does not accept 'sideways'" in problems[2]
    assert "carries a credential" in problems[3]
    assert "must be a string literal" in problems[4]
    assert "**flags cannot be checked" in problems[5]
    assert "under its own name" in problems[6]
