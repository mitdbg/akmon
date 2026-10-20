"""Renamed `--profile` flags and the `okg profile` -> `okg perf` verb.

Decision 310 ("profile B"): every overloaded `--profile` flag gets a name
that says what it selects. Each old spelling stays one release as a hidden
alias that writes the same argparse `dest` and warns once on stderr.

For every renamed flag this module checks that the new name parses, that
the old name parses to the same value and warns on stderr (stdout stays
clean, so `--json` output stays valid), and that `--help` shows only the
new name. It also covers `okg perf` (`perf list`, `perf show`,
`perf compare`) and its deprecated `okg profile` alias.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli import entry_roles
from okg.substrate.cli.main import _build_parser, main
from okg.substrate.cli.renamed_flags import (
    COMPAT_CHILD_ENV,
    COMPAT_READY_CHECKS_FLAG,
    SILENCE_ENV,
    add_renamed_argument,
    rename_warning,
    renamed_option_strings,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = REPO_ROOT / "tests" / "substrate" / "cli" / "CLI_SMOKE_INDEX.yaml"


# (id, argv before the flag, argv after the flag value, new, old, value, dest)
RENAMES = [
    ("up-target", ["up", "dep"], [], "--target", "--profile", "dev", "profile"),
    ("deployment-up-target", ["deployment", "up", "dep"], [], "--target", "--profile", "dev", "profile"),
    ("backup-verify-target", ["deployment", "backup", "verify", "dep"], [], "--target", "--profile", "dev", "profile"),
    ("read-status-target", ["deployment", "read-status", "dep"], [], "--target", "--profile", "prod", "profile"),
    ("up-checks", ["up", "dep"], [], "--checks", "--readiness-profile", "ci", "readiness_profile"),
    ("deployment-up-checks", ["deployment", "up", "dep"], [], "--checks", "--readiness-profile", "dogfood", "readiness_profile"),
    ("lineage-rollout-checks", ["deployment", "nomos-source-lineage-rollout-audit"], [], "--checks", "--readiness-profile", "ci", "readiness_profile"),
    ("ready-checks", ["deployment", "ready", "dep"], [], "--checks", "--profile", "dogfood", "profile"),
    ("prove-live-checks", ["deployment", "prove-live", "dep"], ["--operation-receipt", "r.json"], "--checks", "--profile", "ci", "profile"),
    ("run-capture", ["run"], [], "--capture", "--profile", "off", "profile"),
    ("ingest-capture", ["ingest", "--deployment", "d"], [], "--capture", "--profile", "deep", "profile"),
    ("run-capture-collector", ["run"], [], "--capture-collector", "--profile-collector", "explain-samples", "profile_collector"),
    ("ingest-capture-collector", ["ingest", "--deployment", "d"], [], "--capture-collector", "--profile-collector", "postgres-flamegraph", "profile_collector"),
    ("agent-runtime-permissions", ["agent-runtime-launch", "--runtime", "claude-code"], [], "--permissions", "--profile", "read-only", "profile"),
    ("search-scope", ["search", "--deployment", "d", "--query", "q"], [], "--scope", "--profile", "analyst", "profile"),
    ("chat-setup-scope", ["chat", "setup", "--deployment", "d"], [], "--scope", "--search-profile", "analyst", "search_profile"),
    ("mcp-tools", ["mcp-serve"], [], "--tools", "--tool-profile", "discovery", "tool_profile"),
    ("mcp-tools-ceiling", ["mcp-serve"], [], "--tools-ceiling", "--tool-profile-ceiling", "reader", "tool_profile_ceiling"),
    ("nomos-fixture-provider", ["nomos", "provider", "fixture"], ["--audience", "a"], "--provider", "--profile", "cern-oidc", "profile"),
]
IDS = [r[0] for r in RENAMES]


def _expected(dest: str, value: str):
    return [value] if dest == "profile_collector" else value


@pytest.mark.parametrize("case", RENAMES, ids=IDS)
def test_new_name_parses_without_warning(case, capsys) -> None:
    _, head, tail, new, _old, value, dest = case
    args = _build_parser().parse_args([*head, new, value, *tail])
    assert getattr(args, dest) == _expected(dest, value)
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


@pytest.mark.parametrize("case", RENAMES, ids=IDS)
def test_old_name_still_parses_and_warns_on_stderr(case, capsys) -> None:
    _, head, tail, new, old, value, dest = case
    args = _build_parser().parse_args([*head, old, value, *tail])
    assert getattr(args, dest) == _expected(dest, value)
    captured = capsys.readouterr()
    assert captured.out == "", "the warning must never reach stdout"
    assert captured.err.strip() == rename_warning(old, new)


@pytest.mark.parametrize("case", RENAMES, ids=IDS)
def test_old_name_equals_form_still_parses(case, capsys) -> None:
    _, head, tail, new, old, value, dest = case
    args = _build_parser().parse_args([*head, f"{old}={value}", *tail])
    assert getattr(args, dest) == _expected(dest, value)
    assert rename_warning(old, new) in capsys.readouterr().err


@pytest.mark.parametrize("case", RENAMES, ids=IDS)
def test_help_shows_new_name_and_hides_old(case, capsys) -> None:
    _, head, _tail, new, old, _value, _dest = case
    # Walk to the subcommand parser named by the leading positional words.
    parser = _build_parser()
    for word in [w for w in head if not w.startswith("-")]:
        sub = next(
            (a for a in parser._actions if isinstance(a, argparse._SubParsersAction)),
            None,
        )
        if sub is None or word not in sub.choices:
            break
        parser = sub.choices[word]
    text = parser.format_help()
    assert new in text, text
    tokens = text.replace("[", " ").replace("]", " ").replace(",", " ").split()
    assert old not in tokens, f"{old} still shown in help:\n{text}"
    assert renamed_option_strings(parser).get(old) == new


def test_old_and_new_defaults_match() -> None:
    """With neither spelling given, the shared dest keeps the old default."""
    parser = _build_parser()
    assert parser.parse_args(["up", "dep"]).profile == "prod"
    assert parser.parse_args(["up", "dep"]).readiness_profile == "general"
    assert parser.parse_args(["deployment", "ready", "dep"]).profile == "general"
    assert parser.parse_args(["deployment", "read-status", "dep"]).profile == "dev"
    assert parser.parse_args(["run"]).profile is None
    assert parser.parse_args(["run"]).profile_collector == []
    assert parser.parse_args(["agent-runtime-launch", "--runtime", "x"]).profile == "dev"


def test_capture_bare_flag_means_deep(capsys) -> None:
    parser = _build_parser()
    assert parser.parse_args(["run", "--capture"]).profile == "deep"
    assert parser.parse_args(["run", "--profile"]).profile == "deep"
    assert rename_warning("--profile", "--capture") in capsys.readouterr().err


def test_repeated_old_flag_warns_once(capsys) -> None:
    args = _build_parser().parse_args([
        "run",
        "--profile-collector", "explain-samples",
        "--profile-collector", "postgres-flamegraph",
    ])
    assert args.profile_collector == ["explain-samples", "postgres-flamegraph"]
    err = capsys.readouterr().err
    assert err.count("is renamed to") == 1, err


def _argv_str(value: str) -> str:
    """A fresh (non-interned) string, as real argv tokens are."""
    return "".join(list(value))


@pytest.mark.parametrize("order", ["old-first", "new-first"])
@pytest.mark.parametrize("case", RENAMES, ids=IDS)
def test_both_spellings_in_one_invocation_are_refused(case, order, capsys) -> None:
    """Failure case: old and new spelling together is a usage error naming
    both, never a silent last-one-wins."""
    _, head, tail, new, old, value, _dest = case
    pair = [[old, _argv_str(value)], [new, _argv_str(value)]]
    if order == "new-first":
        pair.reverse()
    with pytest.raises(SystemExit) as exc:
        _build_parser().parse_args([*head, *pair[0], *pair[1], *tail])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "not allowed with argument" in err, err
    assert old in err and new in err, err


def test_silence_env_turns_the_warning_off(monkeypatch, capsys) -> None:
    monkeypatch.setenv(SILENCE_ENV, "0")
    args = _build_parser().parse_args(["deployment", "ready", "dep", "--profile", "ci"])
    assert args.profile == "ci"
    assert capsys.readouterr().err == ""
    monkeypatch.setenv(SILENCE_ENV, "1")
    _build_parser().parse_args(["deployment", "ready", "dep", "--profile", "ci"])
    assert "is renamed to" in capsys.readouterr().err


def test_compat_child_spelling_is_accepted_by_old_and_new_code(monkeypatch, capsys) -> None:
    """The child argv `okg deployment up` hands to the checked-out release
    pin uses the spelling both sides accept: the deprecated alias of
    `--checks` on this parser is exactly the pre-rename option, and with the
    compat env it parses silently here."""
    parser = _build_parser()
    dep = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    dep = dep.choices["deployment"]
    ready = next(a for a in dep._actions if isinstance(a, argparse._SubParsersAction))
    ready = ready.choices["ready"]
    assert renamed_option_strings(ready)[COMPAT_READY_CHECKS_FLAG] == "--checks"
    for key, value in COMPAT_CHILD_ENV.items():
        monkeypatch.setenv(key, value)
    args = _build_parser().parse_args(
        ["deployment", "ready", "dep", COMPAT_READY_CHECKS_FLAG, "ci"]
    )
    assert args.profile == "ci"
    assert capsys.readouterr().err == ""


def test_helm_probes_use_the_compat_spelling_with_warnings_silenced() -> None:
    """The chart may run an older image (images.okg.tag is independent of
    the chart), so its probes keep the old spelling for this release."""
    chart = REPO_ROOT / "ops" / "kubernetes" / "okg" / "templates"
    statefulset = (chart / "runtime-statefulset.yaml").read_text()
    test_pod = (chart / "tests" / "readiness.yaml").read_text()
    helpers = (chart / "_helpers.tpl").read_text()
    assert statefulset.count("- --profile\n                - {{ .Values.okg.readinessProfile") == 2
    assert "--profile {{ .Values.okg.readinessProfile" in test_pod
    # No command line passes the new spelling (comments may name it).
    for text in (statefulset, test_pod):
        commands = [
            line for line in text.splitlines()
            if not line.lstrip().startswith("#")
        ]
        assert not any("--checks" in line for line in commands)
    # Silenced per probe command only, so `kubectl exec` users still see
    # the deprecation warning.
    assert statefulset.count("- env\n                - OKG_RENAMED_FLAG_WARNINGS=0\n                - okg") == 2
    assert "env OKG_RENAMED_FLAG_WARNINGS=0 okg deployment ready" in test_pod
    assert "OKG_RENAMED_FLAG_WARNINGS" not in helpers
    assert "name: OKG_RENAMED_FLAG_WARNINGS" not in statefulset + test_pod


def test_built_commands_print_the_new_spelling() -> None:
    from okg.substrate.cli.command_reference import okg_command

    command = okg_command("deployment ready", "dep", profile="dogfood")
    assert "--checks dogfood" in command.display()
    assert "--profile" not in command.display()
    assert "--checks" in okg_command("deployment ready", "dep", checks="ci").display()


def test_source_scan_flags_an_old_spelling_keyword() -> None:
    """Failure case: a source reference naming the deprecated keyword."""
    from okg.substrate.cli.command_reference import check_reference

    with pytest.raises(ValueError, match="deprecated spelling; use checks="):
        check_reference("deployment ready", ["profile"], {}, 1)
    check_reference("deployment ready", ["checks"], {"checks": "ci"}, 1)


def test_old_name_keeps_choices(capsys) -> None:
    """Failure case: the alias validates values exactly like the new name."""
    with pytest.raises(SystemExit) as exc:
        _build_parser().parse_args(["up", "dep", "--profile", "staging"])
    assert exc.value.code == 2
    assert "invalid choice: 'staging'" in capsys.readouterr().err


def test_required_flag_error_names_only_the_new_spelling(capsys) -> None:
    """Failure case: `nomos provider fixture` still requires a provider."""
    with pytest.raises(SystemExit) as exc:
        _build_parser().parse_args(["nomos", "provider", "fixture", "--audience", "a"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "--provider" in err
    assert "--profile" not in err


def test_old_flag_keeps_json_stdout_valid(capsys) -> None:
    """End to end: an old spelling with --json leaves stdout parseable."""
    rc = main([
        "nomos", "provider", "fixture", "--profile", "cern-oidc",
        "--audience", "akmon-research", "--json",
    ])
    assert rc == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload
    assert captured.err.strip() == rename_warning("--profile", "--provider")

    rc = main([
        "nomos", "provider", "fixture", "--provider", "cern-oidc",
        "--audience", "akmon-research", "--json",
    ])
    assert rc == 0
    captured_new = capsys.readouterr()
    assert json.loads(captured_new.out) == payload
    assert captured_new.err == ""


def test_helper_rejects_short_options() -> None:
    parser = argparse.ArgumentParser()
    with pytest.raises(ValueError):
        add_renamed_argument(parser, "-t", "--profile", dest="profile")


# ── okg profile -> okg perf ─────────────────────────────────────────────


@pytest.fixture
def _no_captures(monkeypatch, tmp_path):
    from okg.substrate.profiling import artifacts

    monkeypatch.setattr(artifacts, "list_profiles", lambda deployment=None: [])
    monkeypatch.setattr(artifacts, "profile_root", lambda: tmp_path)


def test_perf_list_runs_without_warning(_no_captures, capsys) -> None:
    assert main(["--json", "perf", "list"]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out)
    assert captured.err == ""


def test_profile_verb_still_runs_and_warns(_no_captures, capsys) -> None:
    assert main(["--json", "perf", "list"]) == 0
    new_out = capsys.readouterr().out
    assert main(["--json", "profile", "list"]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == json.loads(new_out)
    assert captured.err.strip() == rename_warning("okg profile", "okg perf")


@pytest.mark.parametrize("verb", ["perf", "profile"])
@pytest.mark.parametrize("sub", ["list", "show", "compare"])
def test_perf_and_profile_share_subcommands(verb, sub) -> None:
    parser = _build_parser()
    top = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    inner = next(
        a for a in top.choices[verb]._actions
        if isinstance(a, argparse._SubParsersAction)
    )
    assert sub in inner.choices


def test_perf_show_unknown_id_names_perf_list(_no_captures, monkeypatch, capsys) -> None:
    """Failure case: the recovery hint names the new verb."""
    from okg.substrate.profiling import artifacts

    monkeypatch.setattr(artifacts, "find_profile", lambda *_a, **_k: None)
    assert main(["--json", "perf", "show", "nope"]) != 0
    captured = capsys.readouterr()
    assert "okg perf list" in captured.out + captured.err


def test_help_lists_perf_and_hides_profile() -> None:
    parser = _build_parser()
    top = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    listed = [a.dest for a in top._choices_actions]
    assert "perf" in listed
    assert "profile" not in listed
    assert "profile" in top.choices, "the old verb must still dispatch"
    usage = parser.format_usage()
    assert ",perf," in usage or "{perf," in usage or ",perf}" in usage
    assert ",profile," not in usage and "{profile," not in usage and ",profile}" not in usage
    # Retired, not internal: the "internal commands" epilog does not name it.
    assert "profile" not in (parser.epilog or "")


def test_renamed_verbs_mirror_the_smoke_manifest() -> None:
    verbs = yaml.safe_load(MANIFEST_PATH.read_text())["verbs"]
    manifest = {
        verb: entry["renamed_to"]
        for verb, entry in verbs.items()
        if isinstance(entry, dict) and "renamed_to" in entry
    }
    assert manifest == entry_roles.RENAMED_VERBS
    for old, new in entry_roles.RENAMED_VERBS.items():
        assert new in verbs, f"renamed verb {old!r} points at missing {new!r}"


def test_builder_refuses_old_and_new_keyword_for_one_option() -> None:
    """Failure case: `profile=` and `checks=` both name `--checks`."""
    from okg.substrate.cli.command_reference import okg_command

    with pytest.raises(ValueError, match="both name --checks"):
        okg_command("deployment ready", "dep", profile="ci", checks="ci")
    with pytest.raises(ValueError, match="both name --checks"):
        okg_command("deployment ready", "dep", profile=None, checks="ci")


def test_backup_verify_recovery_command_uses_compat_spelling(tmp_path) -> None:
    """The printed recovery command must parse on a pinned pre-rename
    checkout too, and the old spelling it uses must parse silently here."""
    import shlex
    from types import SimpleNamespace

    from okg.substrate import deployment_upgrade as du

    ctx = SimpleNamespace(
        options=SimpleNamespace(
            deployment="dep", profile="dev", backup_keep=1, dsn=None,
        ),
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path / "backups",
    )
    command = du._backup_verify_next_command(ctx)
    tokens = shlex.split(command)
    assert tokens[0] == "OKG_RENAMED_FLAG_WARNINGS=0"
    assert "--target" not in tokens
    at = tokens.index("verify")
    assert tokens[at + 2:at + 4] == ["--profile", "dev"]
    parser_args = tokens[tokens.index("deployment"):]
    args = _build_parser().parse_args(parser_args)
    assert args.profile == "dev"
