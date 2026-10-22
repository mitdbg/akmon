"""`okg install` ends with a short summary, not the full payload (#3891, #3848).

Before: the text output dumped the publish proof as a dict, one
`WARN [skipped_extends]` line per optional link with no explanation,
every file written, `run_outcome: no_change` on the run that published,
and a `next:` list that repeated `migrate` and `run --once`. These tests pin
the short form, the `--verbose` full form, and that `--json` keeps its keys.
No database is needed: the renderer reads a payload, and the scaffold test
uses `--no-publish`.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.cli import main as cli
from okg.substrate.cli.install_summary import (
    INFORMATIONAL_WARNING_CODES,
    actionable_warnings,
    agent_failure_line,
    deployments_env_prefix,
    render_install_summary,
    warning_line,
)
from okg.substrate.cli import install_summary
from okg.substrate.support import user_hints


@pytest.fixture(autouse=True)
def _plain_okg_hints(monkeypatch):
    """Render hints the way a user's shell sees them (plain `okg`).

    Under pytest a checkout prints `uv run okg`; test_user_hints.py pins
    when each form applies.
    """
    monkeypatch.setattr(user_hints, "okg_prefix", lambda: "okg")
    monkeypatch.setattr(
        install_summary, "_first_indexed_word", lambda repo_root, excludes: "README",
    )
    monkeypatch.setattr(install_summary, "_git_files_excludes", lambda d: ())


SKIPPED = {
    "code": "skipped_extends",
    "source_module": "code_graph",
    "target_module": "concepts",
    "reason": "target module not loaded",
}


def _published_payload(**overrides):
    payload = {
        "profile": {"name": "workspace-graph", "version": "0.1.0"},
        "deployment": {"name": "tiny", "dir": "/h/.local/share/okg/deployments/tiny"},
        "files_written": ["/h/a", "/h/b"],
        "answers": {
            "postgres_dsn": "postgresql://postgres:***@127.0.0.1:55482/okg",
            "repo_root": "/work/Hello-World",
        },
        "publish": {
            "ok": True,
            "message": "first publish complete",
            "details": {
                "run_outcome": {"outcome": "no_change", "generation_id": "g1"},
                "publish_proof": {
                    "ok": True, "generation_id": "g1",
                    "nodes_added": 14, "edges_added": 15,
                },
            },
        },
        "warnings": [SKIPPED] * 6,
        "next": [
            "okg migrate --deployment tiny --apply",
            "okg run --once --deployment tiny --apply",
            "okg db role-passwords --apply --deployment tiny",
            "okg agent connect --deployment tiny",
        ],
    }
    payload.update(overrides)
    return payload


WAITING_FOR_PASSWORDS = {
    "status": "needs_role_passwords",
    "next_commands": [
        "okg db role-passwords --apply --deployment tiny",
        "okg agent connect --deployment tiny",
    ],
}


def test_summary_is_short_and_names_what_a_newcomer_needs() -> None:
    text = render_install_summary(
        _published_payload(), agents_step=WAITING_FOR_PASSWORDS,
    )
    lines = text.splitlines()
    assert len(lines) <= 10, text
    assert "tiny" in lines[0] and "workspace-graph" in lines[0]
    assert "The graph now holds 14 records and 15 links." in text
    assert "postgresql://postgres:***@127.0.0.1:55482/okg" in text
    assert "okg search --deployment tiny --query README" in text
    assert "okg db role-passwords --apply --deployment tiny" in text
    assert "okg agent connect --deployment tiny" in text


def test_summary_leaves_out_the_internal_detail() -> None:
    text = render_install_summary(
        _published_payload(), agents_step=WAITING_FOR_PASSWORDS,
    )
    for noise in (
        "files_written", "run_outcome", "no_change", "skipped_extends",
        "composition", "okg migrate", "run --once", "uv run",
    ):
        assert noise not in text


def test_summary_without_a_connected_agent_gives_the_one_connect_command() -> None:
    text = render_install_summary(
        _published_payload(), agents_step={"status": "none"},
    )
    assert "Connect an agent: " in text
    assert text.count("okg agent connect --deployment tiny") == 1


def test_summary_reports_a_connected_agent_instead_of_a_command() -> None:
    step = {
        "status": "connected",
        "message": "Connected Claude Code to tiny. Start a new session.\nmore",
    }
    text = render_install_summary(_published_payload(), agents_step=step)
    assert "Connected Claude Code to tiny. Start a new session." in text
    assert "more" not in text
    assert "agent connect" not in text


def test_commands_carry_a_deployments_folder_the_install_chose() -> None:
    prefix = deployments_env_prefix("/data/my deployments", explicit=True)
    assert prefix == "OKG_DEPLOYMENTS_DIR='/data/my deployments' "
    assert deployments_env_prefix("/data/x", explicit=False) == ""
    text = render_install_summary(
        _published_payload(), agents_step={"status": "none"}, env_prefix=prefix,
    )
    assert text.count("OKG_DEPLOYMENTS_DIR='/data/my deployments' okg ") == 2


def test_an_empty_graph_asks_for_a_source_not_a_search() -> None:
    add = "okg deployment add-source tiny notes --type local-folder --apply --yes"
    payload = _published_payload()
    payload["publish"]["no_runnable_sources"] = True
    payload["publish"]["details"]["publish_proof"] = {
        "node_count": 0, "edge_count": 0,
    }
    payload["next"] = [add, "okg doctor --deployment tiny"]
    text = render_install_summary(payload, agents_step={"status": "none"})
    assert "The graph now holds 0 records and 0 links." in text
    assert f"Add one: {add}" in text
    assert "Try a search" not in text


def test_an_install_that_did_not_publish_lists_its_next_steps() -> None:
    payload = _published_payload()
    payload.pop("publish")
    payload["next"] = ["okg migrate --deployment tiny --apply"]
    text = render_install_summary(payload, agents_step={"status": "skipped"})
    assert "Nothing is published yet" in text
    assert "okg migrate --deployment tiny --apply" in text
    assert "Published" not in text


def test_a_publish_the_proof_cannot_count_still_says_it_published() -> None:
    payload = _published_payload()
    payload["publish"]["details"]["publish_proof"] = {"ok": True}
    text = render_install_summary(payload, agents_step={"status": "none"})
    assert "Published the first version of the graph." in text
    assert "0 records" not in text


def test_skipped_extends_is_not_actionable_and_other_warnings_are() -> None:
    failed = {"code": "composition_probe_failed", "message": "boom"}
    assert INFORMATIONAL_WARNING_CODES == {"skipped_extends"}
    assert actionable_warnings([SKIPPED, failed, SKIPPED]) == [failed]
    assert warning_line(SKIPPED) == (
        "WARN [skipped_extends]: code_graph -> concepts: target module not loaded"
    )
    assert warning_line(failed) == "WARN [composition_probe_failed]: boom"


def test_a_refused_agent_connection_gets_a_line_with_its_fix() -> None:
    step = {
        "status": "failed",
        "error": {"code": "cfg_locked", "message": "no", "fix_hint": "quit it"},
    }
    assert agent_failure_line(step) == (
        "Agent connection failed (cfg_locked): no Fix: quit it"
    )
    assert agent_failure_line({"status": "none", "error": None}) is None
    assert agent_failure_line(WAITING_FOR_PASSWORDS) is None


# -- through the real command ----------------------------------------------


def _scaffold_args(tmp_path: Path, **extra) -> SimpleNamespace:
    values = dict(
        command="install",
        profile_name="minimal",
        template=None,
        deployment_name="sumtest",
        deployments_root=str(tmp_path / "deployments"),
        force=False, yes=False, no_publish=True, non_interactive=True,
        no_claude_config=True,
        target=None, repo_root=None, openspec_root=None,
        claude_projects_root=None, notes_root=None, since=None,
        answer_postgres_dsn="postgresql:///sumtest",
    )
    values.update(extra)
    return SimpleNamespace(**values)


def test_install_text_output_is_the_summary_and_verbose_is_the_full_payload(
    tmp_path, capsys,
) -> None:
    assert cli._cmd_init(_scaffold_args(tmp_path), as_json=False) == 0
    short = capsys.readouterr()
    assert "files_written" not in short.out
    assert "Installed graph 'sumtest'" in short.out
    assert "Nothing is published yet" in short.out
    assert len(short.out.splitlines()) <= 10

    rc = cli._cmd_init(
        _scaffold_args(tmp_path, deployment_name="sumtest2", verbose=True),
        as_json=False,
    )
    assert rc == 0
    full = capsys.readouterr()
    assert "files_written:" in full.out
    assert "answers:" in full.out
    assert len(full.out.splitlines()) > len(short.out.splitlines())


def test_json_output_does_not_depend_on_verbose(tmp_path, capsys) -> None:
    """Scripts read `--json`; the new flag changes only the text form."""
    assert cli._cmd_init(_scaffold_args(tmp_path), as_json=True) == 0
    plain = json.loads(capsys.readouterr().out)
    rc = cli._cmd_init(
        _scaffold_args(tmp_path, deployment_name="sumtest3", verbose=True),
        as_json=True,
    )
    assert rc == 0
    verbose = json.loads(capsys.readouterr().out)
    assert set(plain) == set(verbose)
    for key in ("files_written", "composition", "next", "agents"):
        assert key in plain, key


def test_install_accepts_verbose_and_init_shares_it() -> None:
    parser = cli._build_parser(["install"])
    assert parser.parse_args(["install", "--verbose"]).verbose is True
    assert parser.parse_args(["install"]).verbose is False
    assert cli._build_parser(["init"]).parse_args(["init", "--verbose"]).verbose is True


def test_migrate_notice_is_dropped_only_when_quiet(caplog) -> None:
    import logging

    from okg.substrate.cli.init import _migrate_notices

    log = logging.getLogger("okg.substrate.db.migrate")
    notice = _real_migrate_notice()
    other = "migrate: schema is behind"
    handler_records: list[str] = []

    class Collect(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            handler_records.append(record.getMessage())

    handler = Collect()
    log.addHandler(handler)
    try:
        with _migrate_notices(quiet=True):
            log.warning(notice, 1, ["0020_x"])
            log.warning(other)
        assert handler_records == [other]
        handler_records.clear()
        with _migrate_notices(quiet=False):
            log.warning(notice, 1, ["0020_x"])
        assert len(handler_records) == 1
        log.warning(notice, 2, ["0020_x"])  # the filter is gone after the block
        assert len(handler_records) == 2
    finally:
        log.removeHandler(handler)


def _real_migrate_notice() -> str:
    """The format string `migrate()` logs, read from its source.

    Reading it keeps these tests from drifting if the wording changes: the
    filter in `init._migrate_notices` matches on this text.
    """
    import ast

    import okg.substrate.db.migrate as migrate_module

    tree = ast.parse(Path(migrate_module.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "warning"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
            and node.args[0].value.startswith("migrate: %d declared multi-phase")
        ):
            return node.args[0].value
    raise AssertionError("migrate() no longer logs the multi-phase notice")


# -- the round-two review (#3896) ------------------------------------------

REFUSED_CURSOR = {
    "agent": "cursor",
    "reason": "mcp_server_did_not_start",
    "command": "okg agent connect --deployment tiny --agent cursor",
}
ABSENT_CODEX = {
    "agent": "codex",
    "reason": "not found on this machine",
    "command": "okg agent connect --deployment tiny --agent codex",
}


def test_role_password_branch_keeps_the_remote_database_sentence() -> None:
    step = {**WAITING_FOR_PASSWORDS, "not_connected": [
        {"agent": "cursor", "reason": "waits for the role passwords",
         "command": "okg agent connect --deployment tiny --agent cursor"},
        ABSENT_CODEX,
    ]}
    text = render_install_summary(_published_payload(), agents_step=step)
    assert text.count("Cursor waits for the database roles") == 1
    assert "On a database on another host, set the roles up by hand" in text
    assert "step 4, 'A database on another host'" in text
    assert "is not connected" not in text  # the header covers every waiting agent
    assert "Codex" not in text


def test_an_installed_agent_that_was_refused_gets_a_line_and_command() -> None:
    step = {"status": "failed", "error": None,
            "not_connected": [REFUSED_CURSOR, ABSENT_CODEX]}
    text = render_install_summary(_published_payload(), agents_step=step)
    assert (
        "Cursor is not connected (mcp_server_did_not_start). To connect it: "
        "okg agent connect --deployment tiny --agent cursor"
    ) in text
    assert "Codex" not in text  # not installed: nothing to say
    assert "Connect an agent:" not in text


def test_a_connected_agent_still_lists_the_one_that_was_refused() -> None:
    step = {"status": "connected", "message": "Connected Claude Code to tiny.",
            "not_connected": [REFUSED_CURSOR, ABSENT_CODEX]}
    text = render_install_summary(_published_payload(), agents_step=step)
    assert "Connected Claude Code to tiny." in text
    assert "Cursor is not connected (mcp_server_did_not_start)" in text


def test_no_agent_installed_gives_the_generic_connect_command() -> None:
    step = {"status": "none", "not_connected": [ABSENT_CODEX]}
    text = render_install_summary(_published_payload(), agents_step=step)
    assert "Connect an agent: okg agent connect --deployment tiny" in text
    assert "Codex" not in text


def test_skipped_connection_says_why() -> None:
    step = {"status": "skipped", "skip_reason": "--no-claude-config was given",
            "not_connected": []}
    text = render_install_summary(_published_payload(), agents_step=step)
    assert "skipped (--no-claude-config was given)" in text
    assert "okg agent connect --deployment tiny" in text


def test_database_from_an_environment_variable_says_so() -> None:
    payload = _published_payload()
    payload["answers"]["postgres_dsn"] = "${OKG_DSN}"
    text = render_install_summary(payload, agents_step={"status": "none"})
    assert "Database: from $OKG_DSN" in text


def test_sources_waiting_for_approval_get_one_line() -> None:
    payload = _published_payload()
    payload["publish"]["details"]["ingest_selection"] = {
        "runnable": ["git_files"], "awaiting_setup": ["orcid", "papers"],
    }
    text = render_install_summary(payload, agents_step={"status": "none"})
    assert (
        "Sources waiting for your setup approval: orcid, papers." in text
    )
    clean = render_install_summary(
        _published_payload(), agents_step={"status": "none"},
    )
    assert "waiting for your setup approval" not in clean


def test_the_search_term_is_a_word_git_tracks_in_the_repo(
    tmp_path, monkeypatch,
) -> None:
    import subprocess

    monkeypatch.undo()  # drop the fixture's stand-in; use the real lookup
    repo = tmp_path / "Hello-World"
    repo.mkdir()
    (repo / "!.bin").write_text("x")
    for excluded in (".claude/notes.md", "data/aaa.csv", "memories/bbb.md"):
        (repo / excluded).parent.mkdir(exist_ok=True)
        (repo / excluded).write_text("x")
    (repo / "README").write_text("hi")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    globs = (".claude/**", "data/**", "memories/**")
    # `.claude/notes.md` sorts first but the git_files source excludes it.
    assert install_summary._first_indexed_word(str(repo), ()) == "notes"
    assert install_summary._first_indexed_word(str(repo), globs) == "README"
    assert install_summary._first_indexed_word(str(tmp_path / "nope"), ()) is None
    # End to end through the deployment's own registry.
    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "source_registry.yaml").write_text(
        "sources:\n  git_files:\n    params:\n      exclude_globs:\n"
        "      - '.claude/**'\n      - 'data/**'\n      - 'memories/**'\n",
    )
    payload = {"answers": {"repo_root": str(repo)}, "deployment": {"dir": str(dep)}}
    assert install_summary._search_term(payload) == "README"
    # No git_files source or no registry: a placeholder, never a guess.
    assert install_summary._search_term({"answers": {"repo_root": str(repo)}}) == (
        install_summary.SEARCH_PLACEHOLDER
    )
    assert install_summary._search_term({"answers": {}}) == (
        install_summary.SEARCH_PLACEHOLDER
    )


def test_four_installed_agents_waiting_for_passwords_stay_within_ten_lines() -> None:
    waiting = [
        {"agent": agent, "reason": "waits for the role passwords",
         "command": f"okg agent connect --deployment tiny --agent {agent}"}
        for agent in ("claude-code", "claude-desktop", "cursor", "codex")
    ]
    step = {**WAITING_FOR_PASSWORDS, "not_connected": waiting}
    # The longest case: waiting agents plus a source awaiting approval.
    payload = _published_payload()
    payload["publish"]["details"]["ingest_selection"] = {
        "runnable": ["git_files"], "awaiting_setup": ["orcid"],
    }
    text = render_install_summary(payload, agents_step=step)
    assert "Sources waiting for your setup approval: orcid." in text
    assert len(text.splitlines()) <= 10, text
    assert "is not connected" not in text and "are not connected" not in text
    assert (
        "  Claude Code, Claude Desktop, Cursor and Codex wait for the database "
        "roles to get passwords. In order:"
    ) in text


def test_a_failed_connection_reason_is_not_said_twice() -> None:
    item = {"agent": "cursor", "reason": "not connected: mcp_server_did_not_start",
            "command": "okg agent connect --deployment tiny --agent cursor"}
    step = {"status": "failed", "error": None, "not_connected": [item]}
    text = render_install_summary(_published_payload(), agents_step=step)
    assert "Cursor is not connected (mcp_server_did_not_start)." in text
    assert "not connected: " not in text


def test_four_installed_agents_refused_stay_within_ten_lines() -> None:
    refused = [
        {"agent": agent, "reason": "mcp_server_did_not_start",
         "command": f"okg agent connect --deployment tiny --agent {agent}"}
        for agent in ("claude-code", "claude-desktop", "cursor", "codex")
    ]
    step = {"status": "connected", "message": "Connected Claude Code to tiny.",
            "not_connected": refused}
    text = render_install_summary(_published_payload(), agents_step=step)
    assert len(text.splitlines()) <= 10, text


def test_every_warning_the_install_prints_carries_a_fix() -> None:
    from okg.substrate.cli.install_summary import warning_fix

    assert "none needed" in warning_fix(SKIPPED, "tiny")
    probe = {"code": "composition_probe_failed", "message": "boom"}
    fix = warning_fix(probe, "tiny")
    assert "okg doctor --deployment tiny" in fix
    # A stage failure prints its fix after the warning, in the fatal summary.
    assert warning_fix({"code": "first_publish_failed", "message": "x"}, "t") is None


# -- init.py wiring (text mode, a profile that emits skipped_extends) -------


def _workspace_graph_repo(tmp_path: Path) -> Path:
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("x = 1\n")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "-c", "user.email=t@e", "-c", "user.name=t",
         "commit", "-qm", "init"],
        check=True,
    )
    return repo


def _install_workspace_graph(tmp_path, monkeypatch, *flags):
    """Run the real command with the publish stage stubbed; return its kwargs."""
    from okg.substrate.cli import init as init_mod

    seen: dict = {}

    def fake_publish(**kwargs):
        seen.update(kwargs)
        return {"ok": True, "message": "first publish complete", "details": {
            "publish_proof": {"ok": True, "nodes_added": 3, "edges_added": 2},
        }}

    monkeypatch.setattr(init_mod, "_run_init_migrate_and_publish", fake_publish)
    argv = [
        "install", "--profile", "workspace-graph",
        "--deployment-name", "wired",
        "--deployments-root", str(tmp_path / "deployments"),
        "--postgres-dsn", "postgresql://postgres:<password>@127.0.0.1:1/okg_wired",
        "--repo-root", str(_workspace_graph_repo(tmp_path)),
        "--mcp-port", "18891", "--non-interactive", "--no-claude-config",
        *flags,
    ]
    return cli.main(argv), seen


def test_text_mode_quiets_notices_and_hides_skipped_extends(
    tmp_path, monkeypatch, capsys,
) -> None:
    rc, seen = _install_workspace_graph(tmp_path, monkeypatch)
    out = capsys.readouterr()
    assert rc == 0, out.err
    assert seen["quiet_notices"] is True
    assert "skipped_extends" not in out.err
    assert out.err.strip() == ""
    assert "The graph now holds 3 records and 2 links." in out.out


def test_verbose_and_json_keep_the_notices_and_the_warnings(
    tmp_path, monkeypatch, capsys,
) -> None:
    rc, seen = _install_workspace_graph(tmp_path, monkeypatch, "--verbose")
    err = capsys.readouterr().err
    assert rc == 0
    assert seen["quiet_notices"] is False
    assert err.count("WARN [skipped_extends]") == 6
    assert err.count("Fix: none needed") == 6

    (tmp_path / "j").mkdir()
    rc, seen = _install_workspace_graph(tmp_path / "j", monkeypatch, "--json")
    assert rc == 0
    assert seen["quiet_notices"] is False
