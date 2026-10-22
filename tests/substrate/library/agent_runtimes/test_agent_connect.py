"""`okg agent connect` / `okg agent disconnect` without a database.

PACT agent-connect-3412. These rows drive the real CLI against a temporary
HOME with each agent present or absent. The one step that needs a database --
starting the registered entry and listing its tools -- is replaced by a
recording fake here; `test_agent_connect_spawn.py` runs it for real.

Covered tasks: task.connect.command, task.connect.locations,
task.connect.idempotent, task.connect.skills, plus the launcher's new path
flags (req.connect.starts-anywhere) and the refusals of req.connect.ready that
need no database (failed server check, uv-cache interpreter, built-in role
passwords).
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

import pytest

from okg.substrate.cli.main import main
from okg.substrate.local_env import parse_local_env
from okg.substrate.library.agent_runtimes import (
    EphemeralOkgInterpreterError,
    connect,
)
from okg.substrate.library.agent_runtimes import launcher
from okg.substrate.library.agent_runtimes.launcher import (
    AgentLaunchRequest,
    AgentRuntimeBoundaryError,
    bootstrap_agent_runtime_policy,
    build_agent_launch_plan,
)
from okg.substrate.pact.agent_install import sync_agent_skills


_REAL_DESKTOP_RUNNING = connect.desktop_running

DEPLOYMENT = "demo"
KEY = f"okg-{DEPLOYMENT}"
USER_SKILLS = {
    "okg-add-source",
    "okg-ontology-design",
    "okg-provision",
    "okg-query",
    "okg-troubleshoot",
}
DEV_SKILLS = {"pact", "agent-workflow", "dev-process"}
READ_TOOLS = ["aggregate", "expand", "filter", "inspect", "map", "query", "search"]
SECRET_MARKERS = ("postgres://", "postgresql://", "password=", "sk-", "ghp_")


# ---------------------------------------------------------------- fixtures


class World:
    """One temporary user: HOME, PATH, a deployment and the fake server check."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.home = tmp_path / "home"
        self.bin = tmp_path / "bin"
        self.root = tmp_path / "deployments"
        self.deployment_dir = self.root / DEPLOYMENT
        self.elsewhere = tmp_path / "elsewhere"
        self.probes: list[dict] = []
        self.probe_result = connect.ProbeResult(ok=True, tools=tuple(READ_TOOLS))
        self.builtin = False
        self.builtin_rw = False
        self.desktop_running: bool | None = False

    # agent presence -------------------------------------------------------

    def claude_code_config(self, payload: dict | None = None) -> Path:
        path = self.home / ".claude.json"
        path.write_text(json.dumps(payload or {}, indent=2) + "\n")
        return path

    def cursor_config(self, payload: dict | None = None) -> Path:
        path = self.home / ".cursor" / "mcp.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload or {}, indent=2) + "\n")
        return path

    def codex_config(self, text: str = "") -> Path:
        path = self.home / ".codex" / "config.toml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def desktop_config(self, payload: dict | None = None) -> Path:
        if platform.system() == "Darwin":
            path = (
                self.home / "Library" / "Application Support" / "Claude"
                / "claude_desktop_config.json"
            )
        else:
            path = self.home / ".config" / "Claude" / "claude_desktop_config.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload or {}, indent=2) + "\n")
        return path

    def fake_claude(self) -> Path:
        """A `claude` on PATH that records argv and edits ~/.claude.json
        the way `claude mcp add-json` / `claude mcp remove` do."""
        log = self.tmp / "claude-calls.jsonl"
        script = self.bin / "claude"
        script.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            f"log = {str(log)!r}\n"
            "with open(log, 'a') as fh:\n"
            "    fh.write(json.dumps(sys.argv[1:]) + '\\n')\n"
            "path = os.path.join(os.environ['HOME'], '.claude.json')\n"
            "data = json.load(open(path)) if os.path.exists(path) else {}\n"
            "servers = data.setdefault('mcpServers', {})\n"
            "argv = [a for a in sys.argv[1:] if a not in ('--scope', 'user')]\n"
            "if argv[:2] == ['mcp', 'add-json']:\n"
            "    key, body = argv[2], json.loads(argv[3])\n"
            "    if key in servers:\n"
            "        sys.stderr.write('already exists'); sys.exit(1)\n"
            "    servers[key] = {'type': 'stdio', **body}\n"
            "    fail = os.path.join(os.path.dirname(log), 'claude-add-fail')\n"
            "    if os.path.exists(fail) and key == open(fail).read().strip():\n"
            "        os.unlink(fail)\n"
            "        sys.stderr.write('simulated add failure'); sys.exit(1)\n"
            "elif argv[:2] == ['mcp', 'remove']:\n"
            "    servers.pop(argv[2])\n"
            "else:\n"
            "    sys.exit(2)\n"
            "open(path, 'w').write(json.dumps(data, indent=2) + '\\n')\n"
        )
        script.chmod(0o755)
        return log

    # observation ----------------------------------------------------------

    def hashes(self) -> dict[str, str]:
        return {
            str(p.relative_to(self.tmp)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(self.home.rglob("*"))
            if p.is_file()
        }

    def run(self, *argv: str, capsys) -> tuple[int, dict]:
        rc = main(["--json", "agent", *argv])
        out = capsys.readouterr()
        assert out.out.strip(), out.err
        return rc, json.loads(out.out)

    def connect(self, *extra: str, capsys) -> tuple[int, dict]:
        return self.run("connect", "--deployment", DEPLOYMENT, *extra, capsys=capsys)

    def disconnect(self, *extra: str, capsys) -> tuple[int, dict]:
        return self.run("disconnect", "--deployment", DEPLOYMENT, *extra, capsys=capsys)


def _agents(report: dict) -> dict[str, dict]:
    return {row["agent"]: row for row in report["agents"]}


@pytest.fixture
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> World:
    w = World(tmp_path)
    for directory in (w.home, w.bin, w.deployment_dir, w.elsewhere):
        directory.mkdir(parents=True)
    (w.deployment_dir / "deployment.yaml").write_text(
        f"name: {DEPLOYMENT}\npostgres:\n  dsn: ${{OKG_DEMO_DSN}}\n",
        encoding="utf-8",
    )
    for var in (
        "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME",
        "CODEX_HOME", "OKG_ROLE_SECRETS_FILE", "OKG_LOCAL_ENV", "OKG_USER_HOME",
        "OKG_MCP_RO_DSN", "OKG_MCP_RW_DSN", "OKG_DEMO_DSN", "OKG_DSN",
        "OKG_AGENT", "OKG_DEPLOYMENT",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("HOME", str(w.home))
    monkeypatch.setenv("PATH", str(w.bin))
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(w.root))
    monkeypatch.chdir(w.elsewhere)
    monkeypatch.setattr(connect, "SYSTEM_APPLICATION_DIRS", ())
    monkeypatch.setattr(platform, "system", lambda: "Darwin")

    def _probe(entry, *, timeout):
        w.probes.append(entry)
        return w.probe_result

    monkeypatch.setattr(connect, "probe_entry", _probe)
    monkeypatch.setattr(connect, "desktop_running", lambda: w.desktop_running)
    monkeypatch.setattr(
        connect, "builtin_role_password_state",
        lambda paths, role="okg_mcp": w.builtin if role == "okg_mcp" else w.builtin_rw,
    )
    return w


def _child(entry: dict) -> list[str]:
    args = entry["args"]
    return args[args.index("--") + 1:]


def _launcher(entry: dict) -> list[str]:
    args = entry["args"]
    return args[: args.index("--")]


def _flag(args: list[str], name: str) -> str | None:
    return args[args.index(name) + 1] if name in args else None


# ------------------------------------------------- task.connect.command


def test_connect_without_agent_connects_present_and_reports_absent(
    world: World, capsys,
) -> None:
    world.claude_code_config({"numStartups": 3})
    world.cursor_config()

    rc, report = world.connect(capsys=capsys)

    assert rc == 0, report
    rows = _agents(report)
    assert rows["claude-code"]["status"] == "connected"
    assert rows["cursor"]["status"] == "connected"
    assert rows["claude-desktop"]["status"] == "not found"
    assert rows["codex"]["status"] == "not found"
    assert report["profile"] == "read-only"
    # Each distinct entry (one per agent) is started once, before writes.
    assert len(world.probes) == 2
    # The next step says the agent must start a new session to see it.
    steps = " ".join(report["next_steps"])
    assert "new session" in steps


def test_named_agent_that_is_absent_is_not_found_and_exits_1(
    world: World, capsys,
) -> None:
    world.claude_code_config()

    rc, report = world.connect("--agent", "codex", capsys=capsys)

    assert rc == 1
    rows = _agents(report)
    assert list(rows) == ["codex"]
    assert rows["codex"]["status"] == "not found"
    assert not (world.home / ".codex").exists()


def test_named_claude_code_without_the_binary_writes_claude_json_and_says_so(
    world: World, capsys,
) -> None:
    """`okg install` tells the user to run `--agent claude-code`. With no
    `claude` on PATH and no ~/.claude.json yet, that must still connect by
    writing the file, and the output must say what to do next."""
    assert not (world.home / ".claude.json").exists()

    rc, report = world.connect("--agent", "claude-code", capsys=capsys)

    assert rc == 0, report
    row = _agents(report)["claude-code"]
    assert row["status"] == "connected"
    assert row["method"] == "file"
    stored = json.loads((world.home / ".claude.json").read_text())
    assert f"okg-{DEPLOYMENT}" in stored["mcpServers"]
    steps = " ".join(report["next_steps"])
    assert "`claude` is not on PATH" in steps
    assert "install Claude Code" in steps
    assert "no agent was connected" not in steps


def test_unnamed_connect_still_skips_claude_code_without_the_binary(
    world: World, capsys,
) -> None:
    world.cursor_config()

    rc, report = world.connect(capsys=capsys)

    assert rc == 0, report
    assert _agents(report)["claude-code"]["status"] == "not found"
    assert not (world.home / ".claude.json").exists()


def test_dry_run_writes_nothing_and_prints_each_entry(world: World, capsys) -> None:
    world.claude_code_config({"keep": True})
    world.cursor_config()
    world.codex_config('model = "gpt-5"\n')
    world.desktop_config()
    before = world.hashes()

    rc, report = world.connect("--dry-run", capsys=capsys)

    assert rc == 0, report
    assert world.hashes() == before
    assert world.probes == []  # dry run does not start the server either
    assert report["dry_run"] is True
    for name, row in _agents(report).items():
        assert row["status"] == "would connect", (name, row)
        assert row["config_path"]
        assert "agent-runtime-launch" in row["entry"]["args"]
        assert "mcp-serve" in _child(row["entry"])
        assert row["skills"], name
    assert not (world.home / ".claude" / "skills").exists()
    assert not (world.home / ".agents").exists()


def test_dry_run_text_output_names_files_entries_and_skill_folders(
    world: World, capsys,
) -> None:
    world.claude_code_config()
    rc = main(["agent", "connect", "--deployment", DEPLOYMENT, "--dry-run"])
    out = capsys.readouterr().out
    assert rc == 0
    assert str(world.home / ".claude.json") in out
    assert "agent-runtime-launch" in out
    assert str(world.home / ".claude" / "skills") in out


def test_writes_flag_registers_the_write_profile_and_okg_record(
    world: World, capsys,
) -> None:
    world.cursor_config()

    rc, report = world.connect("--agent", "cursor", capsys=capsys)
    assert rc == 0, report
    entry = json.loads((world.home / ".cursor" / "mcp.json").read_text())[
        "mcpServers"
    ][KEY]
    assert _flag(_launcher(entry), "--permissions") == "read-only"
    assert "--allow-writes" not in _child(entry)
    skills = world.home / ".agents" / "skills"
    assert not (skills / "okg-record").exists()

    rc, report = world.connect("--agent", "cursor", "--writes", capsys=capsys)
    assert rc == 0, report
    assert report["profile"] == "deploy"
    entry = json.loads((world.home / ".cursor" / "mcp.json").read_text())[
        "mcpServers"
    ][KEY]
    assert _flag(_launcher(entry), "--permissions") == "deploy"
    assert "--allow-writes" in _child(entry)
    assert (skills / "okg-record" / "SKILL.md").is_file()


def test_disconnect_removes_only_the_okg_entry_and_managed_skills(
    world: World, capsys,
) -> None:
    other = {"command": "/usr/bin/other", "args": ["serve"]}
    world.claude_code_config({"mcpServers": {"other": other}, "theme": "dark"})
    mine = world.home / ".claude" / "skills" / "my-own-skill"
    mine.mkdir(parents=True)
    (mine / "SKILL.md").write_text("---\nname: my-own-skill\n---\nmine\n")

    rc, _ = world.connect("--agent", "claude-code", capsys=capsys)
    assert rc == 0
    skills = world.home / ".claude" / "skills"
    assert {p.name for p in skills.iterdir() if p.is_dir()} == USER_SKILLS | {
        "my-own-skill"
    }

    rc, report = world.disconnect("--agent", "claude-code", capsys=capsys)
    assert rc == 0, report
    assert _agents(report)["claude-code"]["status"] == "disconnected"
    config = json.loads((world.home / ".claude.json").read_text())
    assert config["mcpServers"] == {"other": other}
    assert config["theme"] == "dark"
    assert {p.name for p in skills.iterdir() if p.is_dir()} == {"my-own-skill"}
    assert (mine / "SKILL.md").read_text() == "---\nname: my-own-skill\n---\nmine\n"


def test_disconnect_keeps_a_shared_skill_folder_another_agent_still_uses(
    world: World, capsys,
) -> None:
    world.cursor_config()
    world.codex_config()
    assert world.connect(capsys=capsys)[0] == 0

    rc, report = world.disconnect("--agent", "cursor", capsys=capsys)

    assert rc == 0, report
    assert KEY not in json.loads((world.home / ".cursor" / "mcp.json").read_text()).get(
        "mcpServers", {}
    )
    # Codex still has its entry and reads the same ~/.agents/skills folder.
    assert (world.home / ".agents" / "skills" / "okg-query" / "SKILL.md").is_file()
    assert _agents(report)["cursor"]["skills"]["kept_for"]


def test_agent_verb_is_in_the_cli(capsys) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["agent", "--help"])
    assert exit_info.value.code == 0
    out = capsys.readouterr().out
    assert "connect" in out and "disconnect" in out


# ----------------------------------------------- task.connect.locations


@pytest.mark.parametrize("system", ["Darwin", "Linux"])
def test_each_agent_writes_its_documented_location(
    world: World, capsys, monkeypatch, system: str,
) -> None:
    monkeypatch.setattr(platform, "system", lambda: system)
    world.claude_code_config()
    world.cursor_config()
    world.codex_config()
    desktop = world.desktop_config()

    rc, report = world.connect(capsys=capsys)

    assert rc == 0, report
    rows = _agents(report)
    expected_desktop = (
        world.home / "Library/Application Support/Claude/claude_desktop_config.json"
        if system == "Darwin"
        else world.home / ".config/Claude/claude_desktop_config.json"
    )
    assert desktop == expected_desktop
    assert rows["claude-desktop"]["config_path"] == str(expected_desktop)
    assert rows["claude-code"]["config_path"] == str(world.home / ".claude.json")
    assert rows["cursor"]["config_path"] == str(world.home / ".cursor" / "mcp.json")
    assert rows["codex"]["config_path"] == str(world.home / ".codex" / "config.toml")
    assert rows["claude-code"]["skills"]["root"] == str(world.home / ".claude" / "skills")
    assert rows["cursor"]["skills"]["root"] == str(world.home / ".agents" / "skills")
    assert rows["codex"]["skills"]["root"] == str(world.home / ".agents" / "skills")
    # One folder, written once: the second agent that reads it is told so.
    assert rows["codex"]["skills"].get("shared_with") == "cursor"
    # Cursor also reads Claude Code's folder; connect says so.
    assert any("both" in note for note in rows["cursor"]["notes"])
    # Every config holds a server entry under the deployment key.
    assert KEY in json.loads(expected_desktop.read_text())["mcpServers"]
    assert KEY in tomllib.loads(
        (world.home / ".codex" / "config.toml").read_text()
    )["mcp_servers"]


def test_codex_home_is_honoured(world: World, capsys, monkeypatch) -> None:
    codex_home = world.tmp / "codex-home"
    codex_home.mkdir()
    (codex_home / "config.toml").write_text("")
    monkeypatch.setenv("CODEX_HOME", str(codex_home))

    rc, report = world.connect("--agent", "codex", capsys=capsys)

    assert rc == 0, report
    assert _agents(report)["codex"]["config_path"] == str(codex_home / "config.toml")
    assert KEY in tomllib.loads((codex_home / "config.toml").read_text())["mcp_servers"]
    assert not (world.home / ".codex").exists()


def test_cursor_entry_carries_type_stdio(world: World, capsys) -> None:
    world.cursor_config()
    assert world.connect("--agent", "cursor", capsys=capsys)[0] == 0
    entry = json.loads((world.home / ".cursor" / "mcp.json").read_text())[
        "mcpServers"
    ][KEY]
    assert entry["type"] == "stdio"


def test_cursor_is_present_when_only_its_folder_exists(world: World, capsys) -> None:
    (world.home / ".cursor").mkdir()
    rc, report = world.connect("--agent", "cursor", capsys=capsys)
    assert rc == 0, report
    assert _agents(report)["cursor"]["status"] == "connected"
    assert (world.home / ".cursor" / "mcp.json").is_file()


def test_claude_code_uses_add_json_when_the_binary_is_on_path(
    world: World, capsys,
) -> None:
    log = world.fake_claude()

    rc, report = world.connect("--agent", "claude-code", capsys=capsys)

    assert rc == 0, report
    row = _agents(report)["claude-code"]
    assert row["status"] == "connected"
    assert row["method"] == "claude mcp add-json"
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    assert len(calls) == 1
    call = calls[0]
    assert call[:4] == ["mcp", "add-json", "--scope", "user"]
    assert call[4] == KEY
    assert json.loads(call[5]) == row["entry"]

    # Second run: the stored entry already matches, so nothing is called.
    rc, report = world.connect("--agent", "claude-code", capsys=capsys)
    assert rc == 0
    assert _agents(report)["claude-code"]["status"] == "unchanged"
    assert len(log.read_text().splitlines()) == 1


def test_claude_desktop_gets_skill_zips_and_the_upload_step(
    world: World, capsys,
) -> None:
    world.desktop_config()

    rc, report = world.connect("--agent", "claude-desktop", capsys=capsys)

    assert rc == 0, report
    row = _agents(report)["claude-desktop"]
    assert row["status"] == "connected"
    skills = row["skills"]
    assert skills["status"] == "upload needed"
    assert "Upload" in skills["upload_step"]
    zips = {Path(p).name for p in skills["zips"]}
    assert zips == {f"{name}.zip" for name in USER_SKILLS}
    for path in skills["zips"]:
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
        stem = Path(path).stem
        assert f"{stem}/SKILL.md" in names
    # Restart, not just a new session, for Claude Desktop.
    assert "restart" in " ".join(report["next_steps"]).lower()


# --------------------------------------------- task.connect.idempotent


def test_second_connect_is_unchanged_and_byte_identical(world: World, capsys) -> None:
    world.claude_code_config({"projects": {"/x": {"allowedTools": []}}})
    world.cursor_config()
    world.codex_config()
    world.desktop_config()

    rc, _ = world.connect(capsys=capsys)
    assert rc == 0
    first = world.hashes()

    rc, report = world.connect(capsys=capsys)

    assert rc == 0, report
    assert world.hashes() == first
    for name, row in _agents(report).items():
        assert row["status"] == "unchanged", (name, row)


def test_codex_keeps_other_keys_backs_up_once_and_warns(world: World, capsys) -> None:
    original = (
        "# my settings\n"
        'model = "gpt-5"\n'
        "\n"
        "[mcp_servers.other]\n"
        'command = "/usr/bin/other"\n'
        'args = ["serve"]\n'
    )
    config = world.codex_config(original)

    rc, report = world.connect("--agent", "codex", capsys=capsys)

    assert rc == 0, report
    parsed = tomllib.loads(config.read_text())
    assert parsed["model"] == "gpt-5"
    assert parsed["mcp_servers"]["other"] == {
        "command": "/usr/bin/other", "args": ["serve"],
    }
    assert KEY in parsed["mcp_servers"]
    backups = list(config.parent.glob("config.toml.*backup*"))
    assert len(backups) == 1
    assert backups[0].read_text() == original
    warning = " ".join(report["warnings"])
    assert str(backups[0]) in warning and "comments" in warning

    # A later rewrite keeps the backup: the file now holds okg's entry, and
    # the backup is only refreshed from an okg-free version. That rewrite
    # loses no comments, so it warns about none.
    rc, report = world.connect("--agent", "codex", "--writes", capsys=capsys)
    assert rc == 0, report
    assert backups[0].read_text() == original
    assert len(list(config.parent.glob("config.toml.*backup*"))) == 1
    assert not any("comments" in w for w in report["warnings"])


def test_claude_json_keeps_unrelated_keys(world: World, capsys) -> None:
    payload = {
        "numStartups": 7,
        "projects": {"/repo": {"mcpServers": {"local": {"command": "x"}}}},
        "oauthAccount": {"emailAddress": "someone@example.org"},
    }
    world.claude_code_config(payload)

    assert world.connect("--agent", "claude-code", capsys=capsys)[0] == 0

    after = json.loads((world.home / ".claude.json").read_text())
    for key, value in payload.items():
        assert after[key] == value
    assert list(after["mcpServers"]) == [KEY]


def test_malformed_config_is_refused_untouched_and_others_proceed(
    world: World, capsys,
) -> None:
    cursor = world.home / ".cursor" / "mcp.json"
    cursor.parent.mkdir(parents=True)
    cursor.write_text('{"mcpServers": {')
    world.claude_code_config()
    before = cursor.read_bytes()

    rc, report = world.connect(capsys=capsys)

    assert rc == 1
    rows = _agents(report)
    assert rows["cursor"]["status"] == "refused"
    assert str(cursor) in rows["cursor"]["reason"]
    assert "parse" in rows["cursor"]["reason"].lower()
    assert cursor.read_bytes() == before
    assert rows["claude-code"]["status"] == "connected"


def test_a_non_object_config_is_refused_not_replaced(world: World, capsys) -> None:
    cursor = world.cursor_config()
    cursor.write_text("[1, 2, 3]\n")
    rc, report = world.connect("--agent", "cursor", capsys=capsys)
    assert rc == 1
    assert _agents(report)["cursor"]["status"] == "refused"
    assert cursor.read_text() == "[1, 2, 3]\n"


def test_manual_entry_for_the_same_deployment_is_a_duplicate_until_replace(
    world: World, capsys,
) -> None:
    manual = {
        "command": "/venv/bin/okg",
        "args": ["mcp-serve", "--deployment", DEPLOYMENT],
        "env": {"OKG_LOCAL_ENV": "/x/.okg/local.env"},
    }
    config = world.claude_code_config({"mcpServers": {"okg": manual}})
    before = config.read_bytes()

    rc, report = world.connect("--agent", "claude-code", capsys=capsys)

    assert rc == 1
    row = _agents(report)["claude-code"]
    assert row["status"] == "refused"
    assert "duplicate" in row["reason"] and "'okg'" in row["reason"]
    assert "--replace" in row["reason"]
    assert config.read_bytes() == before

    rc, report = world.connect("--agent", "claude-code", "--replace", capsys=capsys)

    assert rc == 0, report
    servers = json.loads(config.read_text())["mcpServers"]
    assert list(servers) == [KEY]


# ------------------------------------------------- task.connect.skills


def test_user_skills_land_with_a_manifest_and_never_dev_skills(
    world: World, capsys,
) -> None:
    world.claude_code_config()
    world.codex_config()

    assert world.connect(capsys=capsys)[0] == 0

    for root in (world.home / ".claude" / "skills", world.home / ".agents" / "skills"):
        installed = {p.name for p in root.iterdir() if p.is_dir()}
        assert installed == USER_SKILLS, root
        assert not installed & DEV_SKILLS
        manifest = json.loads((root / ".okg-skill-sync.json").read_text())
        assert set(manifest["managed_skills"]) == USER_SKILLS


def test_an_unmanaged_skill_with_a_clashing_name_is_reported_and_kept(
    world: World, capsys,
) -> None:
    world.claude_code_config()
    clash = world.home / ".claude" / "skills" / "okg-query"
    clash.mkdir(parents=True)
    (clash / "SKILL.md").write_text("my own okg-query\n")

    rc, report = world.connect("--agent", "claude-code", capsys=capsys)

    assert rc == 0, report
    skills = _agents(report)["claude-code"]["skills"]
    assert skills["clashed"] == ["okg-query"]
    assert (clash / "SKILL.md").read_text() == "my own okg-query\n"
    manifest = json.loads(
        (world.home / ".claude" / "skills" / ".okg-skill-sync.json").read_text()
    )
    assert "okg-query" not in manifest["managed_skills"]

    rc, _ = world.disconnect("--agent", "claude-code", capsys=capsys)
    assert rc == 0
    assert (clash / "SKILL.md").read_text() == "my own okg-query\n"


def test_sync_never_overwrites_an_unmanaged_skill(tmp_path: Path) -> None:
    canon = tmp_path / "canon"
    for name in ("alpha", "beta"):
        (canon / name).mkdir(parents=True)
        (canon / name / "SKILL.md").write_text(f"---\nname: {name}\n---\nv1\n")
    root = tmp_path / "skills"
    (root / "alpha").mkdir(parents=True)
    (root / "alpha" / "SKILL.md").write_text("theirs\n")

    result = sync_agent_skills(tmp_path, skills_root=root, skills_dir=canon)

    assert result["clashed"] == ["alpha"]
    assert result["added"] == ["beta"]
    assert (root / "alpha" / "SKILL.md").read_text() == "theirs\n"
    manifest = json.loads((root / ".okg-skill-sync.json").read_text())
    assert manifest["managed_skills"] == ["beta"]


# ------------------------------------- req.connect.starts-anywhere (shape)


def test_entry_names_deployment_paths_and_no_secret(
    world: World, capsys, monkeypatch,
) -> None:
    # The repository's .okg/local.env (outside the deployment folder), so the
    # write-enabled profile may use it.
    (world.tmp / ".git").mkdir()
    local_env = world.tmp / ".okg" / "local.env"
    local_env.parent.mkdir()
    local_env.write_text("OKG_DEMO_DSN=postgresql://okg:hunter2@127.0.0.1:5432/demo\n")
    local_env.chmod(0o600)
    secrets = world.home / ".config" / "okg" / "role-secrets" / "loopback-5432.json"
    secrets.parent.mkdir(parents=True)
    secrets.write_text("{}\n")
    secrets.chmod(0o600)
    # Present at connect time; none of them may reach a written config.
    monkeypatch.setenv("OKG_MCP_RO_DSN", "postgresql://okg_mcp:s3cret-ro@db/x")
    monkeypatch.setenv("OKG_MCP_RW_DSN", "postgresql://app_rw:s3cret-rw@db/x")
    monkeypatch.setenv("OKG_DSN", "postgresql://owner:s3cret-owner@db/x")
    world.claude_code_config()
    world.cursor_config()
    world.codex_config()
    world.desktop_config()

    rc, report = world.connect("--writes", capsys=capsys)

    assert rc == 0, report
    assert report["deployment_dir"] == str(world.deployment_dir)
    for row in report["agents"]:
        launcher_args = _launcher(row["entry"])
        assert _flag(launcher_args, "--workspace-root") == str(world.deployment_dir)
        assert _flag(launcher_args, "--deployments-dir") == str(world.root)
        assert _flag(launcher_args, "--local-env") == str(local_env)
        assert _flag(launcher_args, "--role-secrets-file") == str(secrets)
        assert "--allow-builtin-role-passwords" not in launcher_args
        assert os.path.isabs(row["entry"]["command"])
    written = [
        world.home / ".claude.json",
        world.home / ".cursor" / "mcp.json",
        world.home / ".codex" / "config.toml",
        world.home / "Library/Application Support/Claude/claude_desktop_config.json",
    ]
    for path in written:
        text = path.read_text()
        for marker in SECRET_MARKERS + ("s3cret", "hunter2", "OKG_MCP_RO_DSN"):
            assert marker not in text, (path, marker)


def test_without_local_env_or_secrets_the_flags_are_left_out(
    world: World, capsys,
) -> None:
    world.cursor_config()
    rc, report = world.connect("--agent", "cursor", capsys=capsys)
    assert rc == 0, report
    args = _launcher(_agents(report)["cursor"]["entry"])
    assert "--local-env" not in args
    assert "--role-secrets-file" not in args
    assert _flag(args, "--deployments-dir") == str(world.root)


def test_launcher_sets_the_path_bindings_for_the_child_only(tmp_path: Path) -> None:
    root = tmp_path / "deployments"
    root.mkdir()
    local_env = tmp_path / "local.env"
    local_env.write_text("OKG_X=1\n")
    secrets = tmp_path / "secrets.json"
    secrets.write_text("{}\n")
    subject = "subject:agent:demo:codex"
    plan = build_agent_launch_plan(
        bootstrap_agent_runtime_policy(subject_binding_id=subject, deployment="demo"),
        AgentLaunchRequest(
            runtime="codex",
            profile_id="read-only",
            command=("okg", "mcp-serve", "--deployment", "demo"),
            workspace_root=tmp_path,
            subject_binding_id=subject,
            deployment="demo",
            ambient_env={
                "PATH": "/usr/bin",
                "OKG_DEPLOYMENTS_DIR": "/ambient/should/not/pass",
                "OKG_ROLE_SECRETS_FILE": "/ambient/secrets.json",
                "OKG_ALLOW_BUILTIN_ROLE_PASSWORDS": "1",
            },
            deployments_dir=str(root),
            local_env=str(local_env),
            role_secrets_file=str(secrets),
        ),
    )
    assert plan.env["OKG_DEPLOYMENTS_DIR"] == str(root)
    assert plan.env["OKG_LOCAL_ENV"] == str(local_env)
    assert plan.env["OKG_ROLE_SECRETS_FILE"] == str(secrets)
    # The ambient copy of the built-in-password switch is still scrubbed.
    assert "OKG_ALLOW_BUILTIN_ROLE_PASSWORDS" not in plan.env


def test_launcher_without_bindings_keeps_todays_environment(tmp_path: Path) -> None:
    subject = "subject:agent:demo:codex"
    plan = build_agent_launch_plan(
        bootstrap_agent_runtime_policy(subject_binding_id=subject, deployment="demo"),
        AgentLaunchRequest(
            runtime="codex",
            profile_id="read-only",
            command=("okg", "mcp-serve"),
            workspace_root=tmp_path,
            subject_binding_id=subject,
            ambient_env={"PATH": "/usr/bin", "OKG_DEPLOYMENTS_DIR": "/x"},
        ),
    )
    for key in ("OKG_DEPLOYMENTS_DIR", "OKG_LOCAL_ENV", "OKG_ROLE_SECRETS_FILE"):
        assert key not in plan.env


def test_launcher_builtin_password_flag_reaches_the_child(tmp_path: Path) -> None:
    subject = "subject:agent:demo:codex"
    plan = build_agent_launch_plan(
        bootstrap_agent_runtime_policy(subject_binding_id=subject, deployment="demo"),
        AgentLaunchRequest(
            runtime="codex",
            profile_id="read-only",
            command=("okg", "mcp-serve"),
            workspace_root=tmp_path,
            subject_binding_id=subject,
            allow_builtin_role_passwords=True,
        ),
    )
    assert plan.env["OKG_ALLOW_BUILTIN_ROLE_PASSWORDS"] == "1"


@pytest.mark.parametrize("problem", ["missing", "symlink", "wrong-kind", "relative", "other-owner"])
def test_launcher_refuses_a_bad_path_binding(
    tmp_path: Path, monkeypatch, problem: str,
) -> None:
    real = tmp_path / "secrets.json"
    real.write_text("{}\n")
    if problem == "missing":
        value = str(tmp_path / "absent.json")
    elif problem == "symlink":
        link = tmp_path / "link.json"
        link.symlink_to(real)
        value = str(link)
    elif problem == "wrong-kind":
        value = str(tmp_path)  # a directory where a file is required
    elif problem == "relative":
        value = "secrets.json"
    else:
        value = str(real)
        owner = real.stat().st_uid
        monkeypatch.setattr(launcher.os, "geteuid", lambda: owner + 1)
    subject = "subject:agent:demo:codex"
    with pytest.raises(AgentRuntimeBoundaryError) as caught:
        build_agent_launch_plan(
            bootstrap_agent_runtime_policy(subject_binding_id=subject, deployment="demo"),
            AgentLaunchRequest(
                runtime="codex",
                profile_id="read-only",
                command=("okg", "mcp-serve"),
                workspace_root=tmp_path,
                subject_binding_id=subject,
                role_secrets_file=value,
            ),
        )
    assert caught.value.reason_code == "agent_runtime.path_binding_invalid"
    assert caught.value.trace["flag"] == "--role-secrets-file"


def test_launcher_cli_accepts_the_path_flags(tmp_path: Path, capsys) -> None:
    root = tmp_path / "deployments"
    root.mkdir()
    rc = main([
        "--json", "agent-runtime-launch",
        "--runtime", "codex", "--permissions", "read-only",
        "--deployment", "demo", "--workspace-root", str(tmp_path),
        "--deployments-dir", str(root), "--dry-run",
        "--", "okg", "mcp-serve", "--deployment", "demo",
    ])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0, payload
    assert "OKG_DEPLOYMENTS_DIR" in payload["env_keys"]

    rc = main([
        "--json", "agent-runtime-launch",
        "--runtime", "codex", "--permissions", "read-only",
        "--deployment", "demo", "--workspace-root", str(tmp_path),
        "--deployments-dir", str(tmp_path / "nope"), "--dry-run",
        "--", "okg", "mcp-serve",
    ])
    err = json.loads(capsys.readouterr().err)["error"]
    assert rc == 1
    assert err["code"] == "agent_runtime_path_binding_invalid"


# ------------------------------------- req.connect.ready (no database)


def test_a_failed_server_check_writes_nothing_and_names_the_code(
    world: World, capsys,
) -> None:
    world.claude_code_config()
    world.cursor_config()
    world.probe_result = connect.ProbeResult(
        ok=False,
        code="mcp_role_cannot_login",
        message="okg mcp-serve: role okg_mcp cannot log in",
        fix_hint="okg runtime bootstrap --deployment demo",
    )
    before = world.hashes()

    rc = main(["--json", "agent", "connect", "--deployment", DEPLOYMENT])
    err = json.loads(capsys.readouterr().err)["error"]

    assert rc == 1
    assert err["code"] == "mcp_role_cannot_login"
    assert "okg runtime bootstrap --deployment demo" in err["fix_hint"]
    assert world.hashes() == before


def _server_missing_variable() -> connect.ProbeResult:
    """What `okg mcp-serve` prints when the manifest's `${VAR}` is unset."""
    return connect.ProbeResult(
        ok=False,
        code="substrate_error",
        message="deployment DSN environment variables are not set: OKG_DEMO_DSN",
        fix_hint="uv run okg deployment env doctor demo --json",
    )


def test_a_url_variable_only_this_shell_has_names_the_local_env_file(
    world: World, capsys, monkeypatch,
) -> None:
    """Issue #3583: `--postgres-dsn '${OKG_DSN}'` with the URL exported.

    The agent's server never sees connect's shell, so the refusal says the
    URL has to be in the deployment's `.okg/local.env` and gives a command
    that puts it there without the URL on a command line.
    """
    world.claude_code_config()
    monkeypatch.setenv("OKG_DEMO_DSN", "postgresql://owner:hunter2@db.example/x")
    world.probe_result = _server_missing_variable()
    before = world.hashes()

    rc = main(["--json", "agent", "connect", "--deployment", DEPLOYMENT])
    err_text = capsys.readouterr().err
    err = json.loads(err_text)["error"]

    target = world.deployment_dir.resolve() / ".okg" / "local.env"
    assert rc == 1
    assert err["code"] == connect.DSN_VARIABLE_UNSET_CODE
    assert "without your shell's environment" in err["message"]
    assert "OKG_DEMO_DSN is set here" in err["message"]
    assert str(target) in err["message"]
    assert "No agent config was written" in err["message"]
    assert "chmod 600" in err["fix_hint"]
    assert '"OKG_DEMO_DSN=${OKG_DEMO_DSN:?' in err["fix_hint"]
    assert "the same `okg agent connect` command again" in err["fix_hint"]
    assert "env doctor" not in err["fix_hint"]
    assert err["data"]["missing"] == ["OKG_DEMO_DSN"]
    assert err["data"]["local_env"] == str(target)
    assert "hunter2" not in err_text
    assert world.hashes() == before


def _printed_fix(world: World, capsys) -> str:
    main(["--json", "agent", "connect", "--deployment", DEPLOYMENT])
    hint = json.loads(capsys.readouterr().err)["error"]["fix_hint"]
    return hint.split("`")[1]


def _run_fix(command: str, **env: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["/bin/sh", "-c", command], capture_output=True, text=True,
        env={"PATH": "/usr/bin:/bin", **env},
    )


def test_the_local_env_fix_hint_works_in_a_shell(
    world: World, capsys, monkeypatch,
) -> None:
    """Running the printed fix writes a private local env with the URL."""
    world.claude_code_config()
    url = "postgresql://owner:hunter2@db.example/x"
    monkeypatch.setenv("OKG_DEMO_DSN", url)
    world.probe_result = _server_missing_variable()

    done = _run_fix(_printed_fix(world, capsys), OKG_DEMO_DSN=url)

    assert done.returncode == 0, done.stderr
    written = world.deployment_dir / ".okg" / "local.env"
    assert parse_local_env(written) == {"OKG_DEMO_DSN": url}
    assert written.stat().st_mode & 0o077 == 0


def test_the_fix_hint_keeps_an_existing_file_intact_and_makes_it_private(
    world: World, capsys, monkeypatch,
) -> None:
    """An existing file whose last line has no newline, mode 644."""
    existing = world.deployment_dir / ".okg" / "local.env"
    existing.parent.mkdir()
    existing.write_text("OKG_MCP_RO_PASSWORD=x")
    existing.chmod(0o644)
    world.claude_code_config()
    url = "postgresql://owner:hunter2@db.example/x"
    monkeypatch.setenv("OKG_DEMO_DSN", url)
    world.probe_result = _server_missing_variable()

    done = _run_fix(_printed_fix(world, capsys), OKG_DEMO_DSN=url)

    assert done.returncode == 0, done.stderr
    assert parse_local_env(existing) == {
        "OKG_MCP_RO_PASSWORD": "x", "OKG_DEMO_DSN": url,
    }
    assert existing.stat().st_mode & 0o077 == 0


def test_the_fix_hint_stops_instead_of_writing_an_empty_value(
    world: World, capsys, monkeypatch,
) -> None:
    world.claude_code_config()
    monkeypatch.setenv("OKG_DEMO_DSN", "postgresql://owner:hunter2@db.example/x")
    world.probe_result = _server_missing_variable()

    done = _run_fix(_printed_fix(world, capsys))  # not exported where it runs

    written = world.deployment_dir / ".okg" / "local.env"
    assert done.returncode != 0
    assert "OKG_DEMO_DSN is not set in this shell" in done.stderr
    assert "OKG_DEMO_DSN" not in written.read_text()


def test_a_url_variable_read_from_a_profile_file_is_not_called_set_here(
    world: World, capsys, monkeypatch,
) -> None:
    """The CLI loads OKG_LOCAL_ENV into the environment; `$NAME` in the
    user's shell would be empty, so connect must not offer to copy it."""
    other = world.tmp / "other-graph.env"
    other.write_text("OKG_DEMO_DSN=postgresql://owner:hunter2@db.example/x\n")
    monkeypatch.setenv("OKG_LOCAL_ENV", str(other))
    world.claude_code_config()
    world.probe_result = _server_missing_variable()

    rc = main(["--json", "agent", "connect", "--deployment", DEPLOYMENT])
    err = json.loads(capsys.readouterr().err)["error"]

    assert rc == 1
    assert err["code"] == connect.DSN_VARIABLE_UNSET_CODE
    assert err["data"]["set_in_this_shell"] == []
    assert "is set here" not in err["message"]


def test_a_local_env_in_connects_working_directory_does_not_count(
    world: World, capsys, monkeypatch,
) -> None:
    """The registered server starts from `/`: a `.okg/local.env` at the root
    of the repository connect runs in is not one it reads."""
    repo = world.elsewhere
    (repo / "pyproject.toml").write_text("[project]\nname = 'x'\n")
    (repo / ".okg").mkdir()
    (repo / ".okg" / "local.env").write_text(
        "OKG_DEMO_DSN=postgresql://owner:hunter2@db.example/x\n"
    )
    monkeypatch.setenv("OKG_ENABLE_LOCAL_ENV_FOR_TESTS", "1")
    world.claude_code_config()
    world.probe_result = _server_missing_variable()

    rc = main(["--json", "agent", "connect", "--deployment", DEPLOYMENT])
    err = json.loads(capsys.readouterr().err)["error"]

    assert rc == 1
    assert err["code"] == connect.DSN_VARIABLE_UNSET_CODE
    assert err["data"]["local_env"] == str(
        world.deployment_dir.resolve() / ".okg" / "local.env"
    )


def test_a_url_variable_set_nowhere_says_which_line_to_add(
    world: World, capsys,
) -> None:
    world.claude_code_config()
    world.probe_result = _server_missing_variable()

    rc = main(["--json", "agent", "connect", "--deployment", DEPLOYMENT])
    err = json.loads(capsys.readouterr().err)["error"]

    assert rc == 1
    assert err["code"] == connect.DSN_VARIABLE_UNSET_CODE
    assert "is set here" not in err["message"]
    assert "add a line OKG_DEMO_DSN=<database URL> to " in err["fix_hint"]
    assert "the same `okg agent connect` command again" in err["fix_hint"]
    assert err["data"]["set_in_this_shell"] == []


def test_unknown_deployment_is_refused_before_anything_is_written(
    world: World, capsys,
) -> None:
    world.claude_code_config()
    before = world.hashes()
    rc = main(["--json", "agent", "connect", "--deployment", "no-such-graph"])
    err = json.loads(capsys.readouterr().err)["error"]
    assert rc == 1
    assert err["code"] == "deployment_not_found"
    assert world.probes == []
    assert world.hashes() == before


def test_an_interpreter_in_uvs_cache_keeps_its_refusal(
    world: World, capsys, monkeypatch,
) -> None:
    world.claude_code_config()
    before = world.hashes()

    def _ephemeral():
        raise EphemeralOkgInterpreterError(
            interpreter="/cache/uv/env/bin/python", cache_root=Path("/cache/uv"),
        )

    monkeypatch.setattr(launcher, "resolve_okg_binary", _ephemeral)
    rc = main(["--json", "agent", "connect", "--deployment", DEPLOYMENT])
    err = json.loads(capsys.readouterr().err)["error"]
    assert rc == 1
    assert err["code"] == "agent_connect_ephemeral_okg"
    assert "uv tool install" in err["fix_hint"]
    assert world.hashes() == before


def test_builtin_role_passwords_connect_read_only_with_a_warning(
    world: World, capsys,
) -> None:
    world.builtin = True
    world.cursor_config()

    rc, report = world.connect("--agent", "cursor", capsys=capsys)

    assert rc == 0, report
    warning = " ".join(report["warnings"])
    assert "okg db role-passwords --apply --deployment demo" in warning
    args = _launcher(_agents(report)["cursor"]["entry"])
    assert "--allow-builtin-role-passwords" in args
    assert _flag(args, "--permissions") == "read-only"


def test_builtin_role_passwords_refuse_writes(world: World, capsys) -> None:
    world.builtin = True
    world.cursor_config()
    before = world.hashes()
    rc = main(["--json", "agent", "connect", "--deployment", DEPLOYMENT, "--writes"])
    err = json.loads(capsys.readouterr().err)["error"]
    assert rc == 1
    assert err["code"] == "agent_connect_writes_need_role_passwords"
    assert "okg db role-passwords --apply --deployment demo" in err["fix_hint"]
    assert world.hashes() == before


# ------------------------------------------- review fixes (MED1-MED5, LOW)


def test_machine_wide_okg_local_env_is_ignored_with_a_warning(
    world: World, capsys, monkeypatch,
) -> None:
    own = world.deployment_dir / ".okg" / "local.env"
    own.parent.mkdir()
    own.write_text("OKG_DEMO_DSN=postgresql://okg@127.0.0.1:5432/demo\n")
    other = world.tmp / "other-graph.env"
    other.write_text("OKG_DEMO_DSN=postgresql://okg@10.0.0.9:5432/other\n")
    monkeypatch.setenv("OKG_LOCAL_ENV", str(other))
    world.cursor_config()

    rc, report = world.connect("--agent", "cursor", capsys=capsys)

    assert rc == 0, report
    args = _launcher(_agents(report)["cursor"]["entry"])
    assert _flag(args, "--local-env") == str(own)
    assert str(other) not in json.dumps(report["agents"])
    assert any("OKG_LOCAL_ENV" in w and "ignored" in w for w in report["warnings"])


def test_okg_local_env_alone_is_not_written_into_the_entry(
    world: World, capsys, monkeypatch,
) -> None:
    other = world.tmp / "other-graph.env"
    other.write_text("OKG_X=1\n")
    monkeypatch.setenv("OKG_LOCAL_ENV", str(other))
    world.cursor_config()

    rc, report = world.connect("--agent", "cursor", capsys=capsys)

    assert rc == 0, report
    assert "--local-env" not in _launcher(_agents(report)["cursor"]["entry"])
    assert any("OKG_LOCAL_ENV" in w for w in report["warnings"])


def test_failed_claude_add_restores_the_previous_entry(world: World, capsys) -> None:
    world.fake_claude()
    assert world.connect("--agent", "claude-code", capsys=capsys)[0] == 0
    before = json.loads((world.home / ".claude.json").read_text())["mcpServers"][KEY]
    (world.tmp / "claude-add-fail").write_text(KEY)

    rc, report = world.connect("--agent", "claude-code", "--writes", capsys=capsys)

    assert rc == 1
    row = _agents(report)["claude-code"]
    assert row["status"] == "refused"
    assert "simulated add failure" in row["reason"]
    assert "restored" in row["reason"]
    after = json.loads((world.home / ".claude.json").read_text())["mcpServers"]
    assert after[KEY] == before


def test_launcher_refuses_builtin_passwords_for_a_write_profile(tmp_path: Path) -> None:
    subject = "subject:agent:demo:codex"
    with pytest.raises(AgentRuntimeBoundaryError) as caught:
        build_agent_launch_plan(
            bootstrap_agent_runtime_policy(subject_binding_id=subject, deployment="demo"),
            AgentLaunchRequest(
                runtime="codex",
                profile_id="deploy",
                command=("okg", "mcp-serve", "--allow-writes"),
                workspace_root=tmp_path,
                subject_binding_id=subject,
                allow_builtin_role_passwords=True,
            ),
        )
    assert caught.value.reason_code == "agent_runtime.builtin_passwords_with_write"


def test_launcher_records_the_builtin_flag_in_decision_and_report(
    tmp_path: Path, monkeypatch,
) -> None:
    seen: list = []
    real = launcher.evaluate_agent_runtime_decision

    def _spy(bundle, request, **kwargs):
        seen.append(request)
        return real(bundle, request, **kwargs)

    monkeypatch.setattr(launcher, "evaluate_agent_runtime_decision", _spy)
    subject = "subject:agent:demo:codex"
    plan = build_agent_launch_plan(
        bootstrap_agent_runtime_policy(subject_binding_id=subject, deployment="demo"),
        AgentLaunchRequest(
            runtime="codex",
            profile_id="read-only",
            command=("okg", "mcp-serve"),
            workspace_root=tmp_path,
            subject_binding_id=subject,
            allow_builtin_role_passwords=True,
        ),
    )
    assert {"name": "OKG_ALLOW_BUILTIN_ROLE_PASSWORDS", "class": "set_by_launcher_flag"} in [
        dict(row) for row in plan.redacted_env_report
    ]
    assert seen[0].context["allow_builtin_role_passwords"] is True


def test_launcher_refuses_a_group_writable_binding(tmp_path: Path) -> None:
    secrets = tmp_path / "secrets.json"
    secrets.write_text("{}\n")
    secrets.chmod(0o664)
    subject = "subject:agent:demo:codex"
    with pytest.raises(AgentRuntimeBoundaryError) as caught:
        build_agent_launch_plan(
            bootstrap_agent_runtime_policy(subject_binding_id=subject, deployment="demo"),
            AgentLaunchRequest(
                runtime="codex", profile_id="read-only",
                command=("okg", "mcp-serve"), workspace_root=tmp_path / "ws",
                subject_binding_id=subject, role_secrets_file=str(secrets),
            ),
        )
    assert "writable by group or others" in caught.value.trace["problem"]


def _deploy_request(workspace: Path, **bindings) -> AgentLaunchRequest:
    subject = "subject:agent:demo:codex"
    return AgentLaunchRequest(
        runtime="codex", profile_id="deploy", command=("okg", "mcp-serve"),
        workspace_root=workspace, subject_binding_id=subject, **bindings,
    )


def _private(path: Path, text: str = "OKG_X=1\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(0o600)
    return path


def test_write_profile_excludes_the_deployment_credentials_file(tmp_path: Path) -> None:
    workspace = tmp_path / "ws"
    local_env = _private(workspace / ".okg" / "local.env")
    bundle = bootstrap_agent_runtime_policy(
        subject_binding_id="subject:agent:demo:codex", deployment="demo",
    )

    plan = build_agent_launch_plan(
        bundle, _deploy_request(workspace, local_env=str(local_env)),
    )

    assert plan.env["OKG_LOCAL_ENV"] == str(local_env)
    assert plan.denied_write_paths == (os.path.realpath(local_env),)
    assert plan.env["OKG_AGENT_DENIED_WRITE_PATHS"] == os.path.realpath(local_env)
    # The trace is sanitized: a path under a home or temp folder may be
    # recorded as a redacted digest (macOS), so only its presence is pinned.
    assert len(plan.trace["denied_write_paths"]) == 1
    # The write check the child gets: the credentials file is refused, the
    # rest of the workspace is still writable.
    with pytest.raises(AgentRuntimeBoundaryError) as caught:
        launcher.check_agent_write(local_env, plan.env)
    assert caught.value.reason_code == "agent_runtime.write_denied"
    assert caught.value.trace["excluded_from_write_roots"] is True
    launcher.check_agent_write(workspace / "notes.md", plan.env)
    with pytest.raises(AgentRuntimeBoundaryError):
        launcher.check_agent_write(tmp_path / "outside.md", plan.env)


def test_role_secrets_inside_the_write_root_is_excluded_too(tmp_path: Path) -> None:
    workspace = tmp_path / "ws"
    secrets = _private(workspace / "secrets" / "loopback-5432.json", "{}\n")
    plan = build_agent_launch_plan(
        bootstrap_agent_runtime_policy(
            subject_binding_id="subject:agent:demo:codex", deployment="demo",
        ),
        _deploy_request(workspace, role_secrets_file=str(secrets)),
    )
    assert os.path.realpath(secrets) in plan.denied_write_paths
    with pytest.raises(AgentRuntimeBoundaryError):
        launcher.check_agent_write(secrets, plan.env)


def test_binding_check_still_refuses_other_paths_in_the_write_root(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "ws"
    elsewhere = _private(workspace / "config" / "local.env")
    with pytest.raises(AgentRuntimeBoundaryError) as caught:
        build_agent_launch_plan(
            bootstrap_agent_runtime_policy(
                subject_binding_id="subject:agent:demo:codex", deployment="demo",
            ),
            _deploy_request(workspace, local_env=str(elsewhere)),
        )
    assert caught.value.reason_code == "agent_runtime.path_binding_invalid"
    assert "write roots" in caught.value.trace["problem"]


def test_read_only_profile_excludes_nothing(tmp_path: Path) -> None:
    workspace = tmp_path / "ws"
    local_env = _private(workspace / ".okg" / "local.env")
    subject = "subject:agent:demo:codex"
    plan = build_agent_launch_plan(
        bootstrap_agent_runtime_policy(subject_binding_id=subject, deployment="demo"),
        AgentLaunchRequest(
            runtime="codex", profile_id="read-only", command=("okg", "mcp-serve"),
            workspace_root=workspace, subject_binding_id=subject,
            local_env=str(local_env),
        ),
    )
    assert plan.denied_write_paths == ()
    with pytest.raises(AgentRuntimeBoundaryError):
        launcher.check_agent_write(workspace / "notes.md", plan.env)


def test_write_enabled_child_is_refused_a_write_to_local_env(tmp_path: Path) -> None:
    """The real launcher execs a child; the child's write check refuses."""
    import subprocess

    workspace = tmp_path / "ws"
    local_env = _private(workspace / ".okg" / "local.env")
    child = (
        "import sys\n"
        "from okg.substrate.library.agent_runtimes.launcher import "
        "AgentRuntimeBoundaryError, check_agent_write\n"
        "check_agent_write(sys.argv[2])\n"
        "try:\n"
        "    check_agent_write(sys.argv[1])\n"
        "except AgentRuntimeBoundaryError as exc:\n"
        "    print('refused', exc.reason_code)\n"
        "else:\n"
        "    print('allowed')\n"
    )
    done = subprocess.run(
        [
            sys.executable, "-P", "-m", "okg.substrate.cli.main",
            "agent-runtime-launch", "--runtime", "codex",
            "--permissions", "deploy", "--deployment", "demo",
            "--workspace-root", str(workspace), "--local-env", str(local_env),
            "--", sys.executable, "-c", child,
            str(local_env), str(workspace / "notes.md"),
        ],
        cwd="/", env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
        capture_output=True, text=True, timeout=120,
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "refused agent_runtime.write_denied"


def test_launcher_exports_the_resolved_path(tmp_path: Path) -> None:
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    secrets = real_dir / "secrets.json"
    secrets.write_text("{}\n")
    secrets.chmod(0o600)
    alias = tmp_path / "alias"
    alias.symlink_to(real_dir)
    subject = "subject:agent:demo:codex"
    plan = build_agent_launch_plan(
        bootstrap_agent_runtime_policy(subject_binding_id=subject, deployment="demo"),
        AgentLaunchRequest(
            runtime="codex", profile_id="read-only",
            command=("okg", "mcp-serve"), workspace_root=tmp_path / "ws",
            subject_binding_id=subject,
            role_secrets_file=str(alias / "secrets.json"),
        ),
    )
    assert plan.env["OKG_ROLE_SECRETS_FILE"] == os.path.realpath(secrets)


def test_writes_on_the_default_layout_connects_with_the_deployment_local_env(
    world: World, capsys,
) -> None:
    local_env = world.deployment_dir / ".okg" / "local.env"
    local_env.parent.mkdir()
    local_env.write_text("OKG_X=1\n")
    local_env.chmod(0o600)
    world.cursor_config()
    rc, report = world.connect("--agent", "cursor", "--writes", capsys=capsys)
    assert rc == 0, report
    args = _launcher(_agents(report)["cursor"]["entry"])
    assert _flag(args, "--permissions") == "deploy"
    assert _flag(args, "--local-env") == str(local_env)


def test_writes_checks_the_app_rw_password_too(world: World, capsys) -> None:
    world.builtin_rw = True
    world.cursor_config()
    before = world.hashes()
    rc = main(["--json", "agent", "connect", "--deployment", DEPLOYMENT, "--writes"])
    err = json.loads(capsys.readouterr().err)["error"]
    assert rc == 1
    assert err["code"] == "agent_connect_writes_need_role_passwords"
    assert err["data"]["roles"] == ["app_rw"]
    assert world.hashes() == before


def test_install_result_and_freshness_name_a_clash(tmp_path: Path) -> None:
    from okg.substrate.pact.agent_install import (
        agent_skill_freshness,
        install_agent_setup,
    )

    canon = tmp_path / "canon"
    (canon / "alpha").mkdir(parents=True)
    (canon / "alpha" / "SKILL.md").write_text("---\nname: alpha\n---\nv1\n")
    dest = tmp_path / "repo"
    theirs = dest / ".agents" / "skills" / "alpha"
    theirs.mkdir(parents=True)
    (theirs / "SKILL.md").write_text("theirs\n")

    result = install_agent_setup("codex", dest, skills_dir=canon)

    assert result.skills_clashed == ["alpha"]
    payload = result.to_json()
    assert payload["skills_clashed"] == ["alpha"]
    assert "alpha" in payload["text"] and "move it aside" in payload["text"]
    report = agent_skill_freshness(dest, skills_dir=canon, targets=("codex",))
    assert report["clashed"] == [str(theirs.resolve())]
    assert "move aside" in report["fix_hint"]
    assert str(theirs.resolve()) in report["fix_hint"]


@pytest.mark.parametrize(
    ("seconds", "expected"), [(None, 60), (5.0, 60), (30.0, 90), (100.0, 180)],
)
def test_codex_entry_gets_a_startup_timeout(
    world: World, capsys, seconds, expected,
) -> None:
    world.codex_config()
    world.probe_result = connect.ProbeResult(
        ok=True, tools=tuple(READ_TOOLS), seconds=seconds,
    )
    rc, report = world.connect("--agent", "codex", capsys=capsys)
    assert rc == 0, report
    entry = tomllib.loads((world.home / ".codex" / "config.toml").read_text())[
        "mcp_servers"
    ][KEY]
    assert entry["startup_timeout_sec"] == expected


def test_a_longer_codex_timeout_already_written_is_kept(world: World, capsys) -> None:
    world.codex_config()
    world.probe_result = connect.ProbeResult(ok=True, tools=tuple(READ_TOOLS), seconds=40.0)
    assert world.connect("--agent", "codex", capsys=capsys)[0] == 0
    first = world.hashes()
    world.probe_result = connect.ProbeResult(ok=True, tools=tuple(READ_TOOLS), seconds=35.0)
    rc, report = world.connect("--agent", "codex", capsys=capsys)
    assert rc == 0
    assert _agents(report)["codex"]["status"] == "unchanged"
    assert world.hashes() == first


def test_a_slow_start_warns_for_agents_with_no_documented_limit(
    world: World, capsys,
) -> None:
    world.cursor_config()
    world.codex_config()
    world.probe_result = connect.ProbeResult(ok=True, tools=tuple(READ_TOOLS), seconds=12.0)
    rc, report = world.connect(capsys=capsys)
    assert rc == 0, report
    warnings = " ".join(report["warnings"])
    assert "Cursor" in warnings and "12s" in warnings
    assert "Codex does not document" not in warnings


def test_the_server_check_failure_masks_a_connection_password(
    world: World, capsys,
) -> None:
    world.cursor_config()
    world.probe_result = connect.ProbeResult(
        ok=False, code="database_unreachable", message="cannot connect",
        stderr_tail="tried postgresql://owner:hunter2@db.example/x and gave up",
    )
    # Text mode prints the error data as is, so the mask must already be in it.
    rc = main(["agent", "connect", "--deployment", DEPLOYMENT])
    err_text = capsys.readouterr().err
    assert rc == 1
    assert "stderr_tail" in err_text
    assert "hunter2" not in err_text


def test_an_unresolved_database_url_note_masks_a_password(
    world: World, capsys, monkeypatch,
) -> None:
    from okg.substrate import deployment_authority

    def _boom(name, **kwargs):
        raise ValueError("cannot read postgresql://owner:hunter2@db.example/x")

    monkeypatch.setattr(deployment_authority, "resolve_deployment_dsn_authority", _boom)
    world.cursor_config()
    rc, report = world.connect("--agent", "cursor", capsys=capsys)
    assert rc == 0, report
    warnings = " ".join(report["warnings"])
    assert "could not be resolved" in warnings
    assert "hunter2" not in warnings


def test_codex_backup_keeps_the_okg_free_version_through_a_hand_edit(
    world: World, capsys,
) -> None:
    original = "# first\nmodel = \"gpt-5\"\n"
    config = world.codex_config(original)
    assert world.connect("--agent", "codex", capsys=capsys)[0] == 0
    edited = config.read_text() + "# a note added later\n"
    config.write_text(edited)

    rc, report = world.connect("--agent", "codex", "--writes", capsys=capsys)

    assert rc == 0, report
    backups = list(config.parent.glob("config.toml.*backup*"))
    assert len(backups) == 1
    # The hand-edited file still held okg's entry: the backup keeps the last
    # version without one, and the warning says the comment was dropped.
    assert backups[0].read_text() == original
    assert any("comments" in w and str(backups[0]) in w for w in report["warnings"])


def test_okg_free_hand_edit_refreshes_the_backup(world: World, capsys) -> None:
    config = world.codex_config("# first\nmodel = \"gpt-5\"\n")
    assert world.connect("--agent", "codex", capsys=capsys)[0] == 0
    assert world.disconnect("--agent", "codex", capsys=capsys)[0] == 0
    newer = config.read_text() + "# edited while okg was disconnected\n"
    config.write_text(newer)
    assert world.connect("--agent", "codex", capsys=capsys)[0] == 0
    backup = config.with_name("config.toml.okg-backup")
    assert backup.read_text() == newer


@pytest.mark.parametrize("agent", ["claude-code", "claude-desktop", "cursor", "codex"])
def test_connect_then_disconnect_leaves_the_pre_connect_bytes_in_the_backup(
    world: World, capsys, agent: str,
) -> None:
    makers = {
        "claude-code": lambda: world.claude_code_config({"theme": "dark"}),
        "claude-desktop": lambda: world.desktop_config({"mcpServers": {"x": {"command": "y"}}}),
        "cursor": lambda: world.cursor_config({"mcpServers": {"x": {"command": "y"}}}),
        "codex": lambda: world.codex_config("# keep me\nmodel = \"gpt-5\"\n"),
    }
    config = makers[agent]()
    pre_connect = config.read_bytes()

    assert world.connect("--agent", agent, capsys=capsys)[0] == 0
    assert world.disconnect("--agent", agent, capsys=capsys)[0] == 0

    backup = config.with_name(config.name + ".okg-backup")
    assert backup.read_bytes() == pre_connect


def test_disconnect_leaves_an_okg_named_entry_it_did_not_write(
    world: World, capsys,
) -> None:
    manual = {"command": "/venv/bin/okg", "args": ["mcp-serve", "--deployment", "demo"]}
    config = world.cursor_config({"mcpServers": {KEY: manual}})
    before = config.read_bytes()
    rc, report = world.disconnect("--agent", "cursor", capsys=capsys)
    assert rc == 1
    assert _agents(report)["cursor"]["status"] == "refused"
    assert config.read_bytes() == before


def test_disconnect_via_claude_leaves_an_entry_it_did_not_write(
    world: World, capsys,
) -> None:
    log = world.fake_claude()
    manual = {"command": "/venv/bin/okg", "args": ["mcp-serve", "--deployment", "demo"]}
    world.claude_code_config({"mcpServers": {KEY: manual}})
    rc, report = world.disconnect("--agent", "claude-code", capsys=capsys)
    assert rc == 1
    assert _agents(report)["claude-code"]["status"] == "refused"
    assert not log.exists()


def test_dry_run_disconnect_reports_and_keeps_the_desktop_zips(
    world: World, capsys,
) -> None:
    world.desktop_config()
    world.claude_code_config()
    assert world.connect(capsys=capsys)[0] == 0
    zip_dir = world.home / ".local" / "share" / "okg" / "agent-skills" / "claude-desktop"
    assert sorted(p.name for p in zip_dir.iterdir())
    before = world.hashes()

    rc, report = world.disconnect("--dry-run", capsys=capsys)

    assert rc == 0, report
    assert world.hashes() == before
    rows = _agents(report)
    desktop = rows["claude-desktop"]
    assert desktop["status"] == "would disconnect"
    assert {Path(p).name for p in desktop["skills"]["would_remove"]} == {
        f"{name}.zip" for name in USER_SKILLS
    }
    code = rows["claude-code"]
    assert code["status"] == "would disconnect"
    # The entry being removed does not keep its own skills folder alive.
    assert "kept_for" not in code["skills"]
    assert set(code["skills"]["would_remove"]) == USER_SKILLS


# ------------------------------- laptop hand-run bugs (2026-10-06)


DESKTOP_RUNNING_TEXT = "Claude Desktop is running and rewrites its config file from memory"


def test_running_desktop_is_refused_untouched_and_others_connect(
    world: World, capsys,
) -> None:
    world.desktop_running = True
    desktop = world.desktop_config({"mcpServers": {"other": {"command": "x"}}})
    world.claude_code_config()
    before = desktop.read_bytes()

    rc, report = world.connect(capsys=capsys)

    assert rc == 0, report
    rows = _agents(report)
    assert rows["claude-desktop"]["status"] == "refused"
    assert DESKTOP_RUNNING_TEXT in rows["claude-desktop"]["reason"]
    assert (
        "okg agent connect --deployment demo --agent claude-desktop"
        in rows["claude-desktop"]["reason"]
    )
    assert desktop.read_bytes() == before
    zip_dir = world.home / ".local" / "share" / "okg" / "agent-skills"
    assert not zip_dir.exists()
    assert rows["claude-code"]["status"] == "connected"


def test_running_desktop_named_explicitly_exits_1(world: World, capsys) -> None:
    world.desktop_running = True
    desktop = world.desktop_config()
    before = desktop.read_bytes()
    rc, report = world.connect("--agent", "claude-desktop", capsys=capsys)
    assert rc == 1
    assert _agents(report)["claude-desktop"]["status"] == "refused"
    assert desktop.read_bytes() == before


def test_running_desktop_alone_without_agent_exits_1(world: World, capsys) -> None:
    world.desktop_running = True
    world.desktop_config()
    rc, report = world.connect(capsys=capsys)
    assert rc == 1
    assert _agents(report)["claude-desktop"]["status"] == "refused"


def test_disconnect_refuses_while_desktop_runs(world: World, capsys) -> None:
    desktop = world.desktop_config()
    assert world.connect("--agent", "claude-desktop", capsys=capsys)[0] == 0
    world.desktop_running = True
    before = desktop.read_bytes()
    rc, report = world.disconnect("--agent", "claude-desktop", capsys=capsys)
    assert rc == 1
    row = _agents(report)["claude-desktop"]
    assert row["status"] == "refused"
    assert "okg agent disconnect --deployment demo --agent claude-desktop" in row["reason"]
    assert desktop.read_bytes() == before


def test_desktop_write_is_read_back(world: World, capsys, monkeypatch) -> None:
    desktop = world.desktop_config()
    reads: list[Path] = []
    writer_cls = type(connect._writer("claude-desktop"))
    real_load = writer_cls.load_config

    def _spy(self, path):
        reads.append(Path(path))
        return real_load(self, path)

    monkeypatch.setattr(writer_cls, "load_config", _spy)
    rc, report = world.connect("--agent", "claude-desktop", capsys=capsys)
    assert rc == 0, report
    assert _agents(report)["claude-desktop"]["status"] == "connected"
    assert KEY in json.loads(desktop.read_text())["mcpServers"]
    # Read once to plan, once more after the write to confirm the entry.
    assert reads.count(desktop) >= 2


def test_an_entry_missing_on_read_back_is_refused(
    world: World, capsys, monkeypatch,
) -> None:
    desktop = world.desktop_config()
    real_write = connect.atomic_write_text

    def _app_rewrites(path, text, **kwargs):
        real_write(path, text, **kwargs)
        if Path(path) == desktop:  # the running app writes its own copy back
            real_write(path, "{}\n")

    monkeypatch.setattr(connect, "atomic_write_text", _app_rewrites)
    rc, report = world.connect("--agent", "claude-desktop", capsys=capsys)
    assert rc == 1
    row = _agents(report)["claude-desktop"]
    assert row["status"] == "refused"
    assert "reading the file back" in row["reason"]


def test_disconnecting_every_agent_in_one_call_removes_the_shared_skills(
    world: World, capsys,
) -> None:
    """Laptop run: claude-code + claude-desktop + codex disconnected together
    left ~/.agents/skills behind ("still used by codex"). Codex also held an
    academic-installer entry, which starts agent-runtime-launch without
    --deployments-dir; it is not connect's and must not keep the skills."""
    installer_entry = {
        "command": "/venv/bin/python",
        "args": [
            "-P", "-m", "okg.substrate.cli.main", "agent-runtime-launch",
            "--runtime", "codex", "--profile", "read-only",
            "--deployment", "other", "--subject", "subject:agent:other:codex",
            "--workspace-root", "/somewhere", "--", "/venv/bin/python", "-P",
            "-m", "okg.substrate.cli.main", "mcp-serve", "--deployment", "other",
        ],
        "env": {"OKG_DEPLOYMENT": "other"},
    }
    import tomli_w

    world.codex_config(tomli_w.dumps({"mcp_servers": {"okg-other": installer_entry}}))
    world.claude_code_config()
    world.desktop_config()
    assert world.connect(
        "--agent", "claude-code", "--agent", "claude-desktop", "--agent", "codex",
        capsys=capsys,
    )[0] == 0
    zip_dir = world.home / ".local" / "share" / "okg" / "agent-skills" / "claude-desktop"
    assert any(zip_dir.iterdir())

    rc, report = world.disconnect(
        "--agent", "claude-code", "--agent", "claude-desktop", "--agent", "codex",
        capsys=capsys,
    )

    assert rc == 0, report
    rows = _agents(report)
    for agent in ("claude-code", "claude-desktop", "codex"):
        assert rows[agent]["status"] == "disconnected", (agent, rows[agent])
        assert "kept_for" not in (rows[agent]["skills"] or {}), rows[agent]
    for root in (world.home / ".claude" / "skills", world.home / ".agents" / "skills"):
        assert not any(p.is_dir() for p in root.iterdir()), root
        assert not (root / ".okg-skill-sync.json").exists()
    assert not any(zip_dir.iterdir())
    # The installer's entry is untouched.
    codex = tomllib.loads((world.home / ".codex" / "config.toml").read_text())
    assert codex["mcp_servers"] == {"okg-other": installer_entry}


def test_a_foreign_bak_is_ignored_and_okg_writes_its_own_backup(
    world: World, capsys,
) -> None:
    desktop = world.desktop_config({"mcpServers": {"okg-demo-old": {"command": "x"}}})
    foreign = desktop.with_name(desktop.name + ".bak")
    foreign.write_text('{"from": "2026-10-02, not okg"}\n')
    pre_run = desktop.read_text()

    rc, report = world.connect("--agent", "claude-desktop", capsys=capsys)

    assert rc == 0, report
    ours = desktop.with_name(desktop.name + ".okg-backup")
    assert ours.read_text() == pre_run
    assert foreign.read_text() == '{"from": "2026-10-02, not okg"}\n'
    assert any(str(ours) in note for note in _agents(report)["claude-desktop"]["notes"])

    # Disconnect never refreshes it from the file that still holds the entry.
    rc, report = world.disconnect("--agent", "claude-desktop", capsys=capsys)
    assert rc == 0, report
    assert ours.read_text() == pre_run
    assert foreign.read_text() == '{"from": "2026-10-02, not okg"}\n'


def test_every_writer_backs_up_with_the_okg_name(world: World, capsys) -> None:
    from okg.substrate.library.agent_runtimes.registry import KNOWN_RUNTIMES

    world.claude_code_config({"keep": 1})
    world.cursor_config({"keep": 1})
    world.codex_config('keep = 1\n')
    world.desktop_config({"keep": 1})
    before = {rt.name: rt.config_path().read_text() for rt in KNOWN_RUNTIMES}
    for rt in KNOWN_RUNTIMES:
        assert rt.register(deployment="demo", dsn="").status == "registered", rt.name
        cfg = rt.config_path()
        assert cfg.with_name(cfg.name + ".okg-backup").read_text() == before[rt.name]
        assert not cfg.with_name(cfg.name + ".bak").exists()


def _fake_tool(directory: Path, name: str, *, rc: int, stdout: str = "") -> Path:
    log = directory / f"{name}.log"
    fake = directory / name
    fake.write_text(
        f"#!{sys.executable}\nimport json, sys\n"
        f"open({str(log)!r}, 'w').write(json.dumps(sys.argv[1:]))\n"
        f"sys.stdout.write({stdout!r})\nsys.exit({rc})\n"
    )
    fake.chmod(0o755)
    return log


@pytest.fixture
def tools_only_in(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setattr(connect, "TOOL_FALLBACK_DIRS", ())
    return tmp_path


@pytest.mark.parametrize(
    ("system", "expected"),
    [
        # -a: BSD pgrep leaves out its own ancestors otherwise, and connect
        # run from inside Claude Desktop has Desktop as an ancestor.
        ("Darwin", ["-a", "-f", "/Applications/Claude.app/Contents/MacOS/Claude"]),
        # procps leaves out only itself; -x so connect's own
        # `--agent claude-desktop` command line never matches.
        ("Linux", ["-x", "claude-desktop"]),
    ],
)
def test_desktop_detection_pgrep_arguments(
    tools_only_in: Path, monkeypatch, system: str, expected: list[str],
) -> None:
    log = _fake_tool(tools_only_in, "pgrep", rc=1)
    _fake_tool(tools_only_in, "ps", rc=0, stdout="/sbin/launchd\n/usr/bin/login\n")
    monkeypatch.setattr(platform, "system", lambda: system)
    assert _REAL_DESKTOP_RUNNING() is False
    assert json.loads(log.read_text()) == expected


def test_darwin_ps_finds_desktop_that_pgrep_missed(
    tools_only_in: Path, monkeypatch,
) -> None:
    """An ancestor Desktop must be found even if pgrep leaves it out."""
    _fake_tool(tools_only_in, "pgrep", rc=1)
    ps_log = _fake_tool(
        tools_only_in, "ps", rc=0,
        stdout="/sbin/launchd\n/Applications/Claude.app/Contents/MacOS/Claude\n",
    )
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    assert _REAL_DESKTOP_RUNNING() is True
    assert json.loads(ps_log.read_text()) == ["-axo", "comm"]


def test_desktop_helper_process_alone_does_not_count(
    tools_only_in: Path, monkeypatch,
) -> None:
    _fake_tool(tools_only_in, "pgrep", rc=1)
    _fake_tool(
        tools_only_in, "ps", rc=0,
        stdout="/Applications/Claude.app/Contents/Frameworks/Claude Helper.app/x\n",
    )
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    assert _REAL_DESKTOP_RUNNING() is False


def test_desktop_state_unknown_without_pgrep_or_ps(tools_only_in: Path, monkeypatch) -> None:
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    assert _REAL_DESKTOP_RUNNING() is None


def test_an_unreachable_builtin_password_check_is_reported_not_hidden(
    tmp_path: Path,
) -> None:
    """CI's no-silent-fallbacks scan: the check may not swallow every error.
    An unreachable database is recorded as a note the report prints."""
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    paths = connect.DeploymentPaths(
        name="demo", deployment_dir=tmp_path, deployments_dir=tmp_path.parent,
        dsn=f"postgresql://okg:secretpw@127.0.0.1:{port}/demo",
    )
    assert connect.builtin_role_password_state(paths, "okg_mcp") is False
    assert len(paths.check_notes) == 1
    assert "could not check whether role 'okg_mcp'" in paths.check_notes[0]
    assert "secretpw" not in paths.check_notes[0]


# ------------------------- okg install connects agents (PACT why, #3440)


def _assert_only_install_receipt_added(
    world: World, before: dict[str, str], deployment: str = "teamgraph",
) -> None:
    """Install wrote no agent config: every file under HOME is unchanged and
    the one new file is okg's own installed-release receipt for the
    deployment, in the per-user state directory (#3434)."""
    from okg.substrate.deployment_installed_upgrade import RECEIPT_NAME, RECEIPT_SCHEMA

    receipt = str(
        Path("home") / ".local" / "share" / "okg" / "state" / "deployments"
        / deployment / RECEIPT_NAME
    )
    after = world.hashes()
    assert set(after) - set(before) == {receipt}, sorted(set(after) ^ set(before))
    assert {k: v for k, v in after.items() if k != receipt} == before
    payload = json.loads((world.tmp / receipt).read_text())
    assert payload["schema"] == RECEIPT_SCHEMA
    assert payload["deployment"] == deployment
    assert payload["written_by"] == "install"


def _install(world: World, capsys, monkeypatch, *extra: str) -> tuple[int, dict]:
    """The minimal-starter scaffold install, publish stubbed (no database)."""
    rc, out = _install_raw(world, capsys, monkeypatch, "--json", *extra)
    assert out.out.strip(), (rc, out.err[-3000:])
    return rc, json.loads(out.out)


def _install_raw(world: World, capsys, monkeypatch, *extra: str):
    """Run the stubbed install; return (exit code, captured output)."""
    import shutil as _shutil

    from okg.substrate.cli import init as init_module

    # The install asks git for repository provenance; PATH here is only the
    # fake-agent bin folder, so give it git (and nothing agent-like).
    git = _shutil.which("git", path="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin")
    assert git, "git is required by okg install"
    if not (world.bin / "git").exists():
        (world.bin / "git").symlink_to(git)
    monkeypatch.setenv(connect.ENABLE_IN_TESTS_ENV, "1")
    monkeypatch.setattr(
        init_module, "_run_init_migrate_and_publish",
        lambda **kwargs: {"ok": True, "stage": "done", "details": {}},
    )
    rc = main([
        "install", "--deployment-name", "teamgraph",
        "--deployments-root", str(world.root),
        "--postgres-dsn", "postgresql://okg@127.0.0.1:1/teamgraph",
        "--non-interactive", *extra,
    ])
    return rc, capsys.readouterr()


def test_install_connects_the_agents_present(world: World, capsys, monkeypatch) -> None:
    world.claude_code_config()
    world.cursor_config()

    rc, payload = _install(world, capsys, monkeypatch)

    assert rc == 0, payload
    assert payload["profile"]["name"] == "minimal"
    step = payload["agents"]
    assert step["status"] == "connected", step
    assert step["connected"] == ["claude-code", "cursor"]
    assert "okg-teamgraph" in json.loads((world.home / ".claude.json").read_text())[
        "mcpServers"
    ]
    entry = json.loads((world.home / ".cursor" / "mcp.json").read_text())[
        "mcpServers"
    ]["okg-teamgraph"]
    assert _flag(_launcher(entry), "--permissions") == "read-only"
    assert _flag(_launcher(entry), "--deployments-dir") == str(world.root)
    # The message names only connected agents and prints the rest.
    assert "Connected Claude Code, Cursor to teamgraph" in step["message"]
    missing = {item["agent"]: item["command"] for item in step["not_connected"]}
    assert set(missing) == {"claude-desktop", "codex"}
    assert missing["codex"].endswith(
        "okg agent connect --deployment teamgraph --agent codex"
    )
    assert f"OKG_DEPLOYMENTS_DIR={world.root}" in missing["codex"]


def test_install_with_no_claude_config_writes_nothing_and_prints_commands(
    world: World, capsys, monkeypatch,
) -> None:
    world.claude_code_config()
    world.codex_config()
    before = world.hashes()

    rc, payload = _install(world, capsys, monkeypatch, "--no-claude-config")

    assert rc == 0, payload
    _assert_only_install_receipt_added(world, before)
    assert world.probes == []
    step = payload["agents"]
    assert step["status"] == "skipped"
    assert step["connected"] == []
    assert {item["agent"] for item in step["not_connected"]} == set(connect.CONNECT_AGENTS)
    assert "No agent is connected to teamgraph" in step["message"]
    assert "--agent claude-code" in step["message"]


def test_install_before_role_passwords_reports_a_next_step_not_a_failure(
    world: World, capsys, monkeypatch,
) -> None:
    """Issue #3500: right after a first install the MCP roles have no
    password, so connecting always refused with mcp_role_cannot_login and
    the install reported `status: failed`. It is the step after `okg db
    role-passwords --apply`: say so, put both commands in `next`, and do
    not claim a connection."""
    world.cursor_config()
    world.probe_result = connect.ProbeResult(
        ok=False, code="mcp_role_cannot_login",
        message="role okg_mcp cannot log in",
        fix_hint="okg runtime bootstrap --deployment teamgraph",
    )
    before = world.hashes()

    rc, payload = _install(world, capsys, monkeypatch)

    assert rc == 0, payload
    _assert_only_install_receipt_added(world, before)
    step = payload["agents"]
    assert step["status"] == "needs_role_passwords"
    assert step["connected"] == []
    assert step["error"] is None
    assert step["pending"]["code"] == "mcp_role_cannot_login"
    role_step, connect_step = step["next_commands"]
    assert role_step.endswith(
        "okg db role-passwords --apply --deployment teamgraph"
    )
    assert connect_step.endswith("okg agent connect --deployment teamgraph")
    assert role_step in payload["next"] and connect_step in payload["next"]
    assert "failed" not in step["message"]
    assert "waits for the database roles" in step["message"]
    assert "'A database on another host'" in step["message"]
    cursor = next(i for i in step["not_connected"] if i["agent"] == "cursor")
    assert cursor["reason"] == "waits for the role passwords"


def test_install_prints_the_role_password_step_once(
    world: World, capsys, monkeypatch,
) -> None:
    """Human output: one line names the installed agent that waits for the
    role passwords, and the next steps name the role-password command."""
    world.cursor_config()
    world.probe_result = connect.ProbeResult(
        ok=False, code="mcp_role_cannot_login",
        message="role okg_mcp cannot log in", fix_hint=None,
    )

    rc, out = _install_raw(world, capsys, monkeypatch)

    assert rc == 0, out.err[-3000:]
    lines = (out.out + out.err).splitlines()
    waiting = [line for line in lines if "for the database roles" in line]
    assert len(waiting) == 1, waiting
    assert waiting[0].strip().startswith("Cursor waits for the database roles")
    # No line may repeat.
    printed = [line for line in lines if line.strip()]
    assert len(printed) == len(set(printed)), printed
    assert "status: failed" not in out.out + out.err
    assert any(
        line.strip().endswith("okg db role-passwords --apply --deployment teamgraph")
        for line in lines
    ), lines


def test_install_with_a_refused_connect_still_finishes(
    world: World, capsys, monkeypatch,
) -> None:
    """Any other refusal is still reported as a failure with its fix."""
    world.cursor_config()
    world.probe_result = connect.ProbeResult(
        ok=False, code="mcp_server_did_not_start",
        message="the server exited",
        fix_hint="okg mcp-serve --deployment teamgraph",
    )
    before = world.hashes()

    rc, payload = _install(world, capsys, monkeypatch)

    assert rc == 0, payload
    _assert_only_install_receipt_added(world, before)
    step = payload["agents"]
    assert step["status"] == "failed"
    assert step["error"]["code"] == "mcp_server_did_not_start"
    assert "next_commands" not in step
    assert "No agent is connected to teamgraph" in step["message"]
    assert "okg mcp-serve --deployment teamgraph" in step["message"]
    assert any(item["agent"] == "cursor" for item in step["not_connected"])


def test_install_reports_a_running_desktop_with_its_command(
    world: World, capsys, monkeypatch,
) -> None:
    world.desktop_running = True
    desktop = world.desktop_config()
    world.cursor_config()
    before = desktop.read_bytes()

    rc, payload = _install(world, capsys, monkeypatch)

    assert rc == 0, payload
    assert desktop.read_bytes() == before
    step = payload["agents"]
    assert step["connected"] == ["cursor"]
    desktop_row = next(i for i in step["not_connected"] if i["agent"] == "claude-desktop")
    assert DESKTOP_RUNNING_TEXT in desktop_row["reason"]
    assert desktop_row["command"].endswith("--agent claude-desktop")


def test_install_dry_run_reports_the_connect_step_and_writes_nothing(
    world: World, capsys, monkeypatch,
) -> None:
    world.cursor_config()
    before = world.hashes()
    rc, payload = _install(world, capsys, monkeypatch, "--dry-run")
    assert rc == 0, payload
    assert world.hashes() == before
    assert payload["agents"]["would_connect"] == ["cursor"]
    assert any("--agent codex" in c for c in payload["agents"]["would_print"])


def test_install_connect_step_is_off_under_pytest_unless_opted_in(
    world: World, monkeypatch,
) -> None:
    """Install tests elsewhere must not edit the real user's agent configs."""
    world.cursor_config()
    dep = world.root / "teamgraph"
    dep.mkdir()
    (dep / "deployment.yaml").write_text("name: teamgraph\n")
    before = world.hashes()
    monkeypatch.delenv(connect.ENABLE_IN_TESTS_ENV, raising=False)

    step = connect.connect_after_install("teamgraph", deployment_dir=dep)

    assert step["status"] == "skipped"
    assert connect.ENABLE_IN_TESTS_ENV in step["skip_reason"]
    assert world.hashes() == before
    assert world.probes == []


def _codex_entry(world: World) -> dict:
    return tomllib.loads((world.home / ".codex" / "config.toml").read_text())[
        "mcp_servers"
    ][KEY]


def test_codex_read_only_entry_pre_approves_tools_and_writes_entry_does_not(
    world: World, capsys,
) -> None:
    """mcpres B: a headless `codex exec` can call the read tools.

    Codex refuses an MCP tool call it would have to ask about when its
    approval policy is `never`. The read-only server's tools are approved in
    its entry; the write-enabled entry keeps Codex's prompt. A switch either
    way leaves exactly the key the current mode needs.
    """
    world.codex_config()

    rc, report = world.connect("--agent", "codex", capsys=capsys)
    assert rc == 0, report
    assert _codex_entry(world)["default_tools_approval_mode"] == "approve"

    rc, report = world.connect("--agent", "codex", "--writes", capsys=capsys)
    assert rc == 0, report
    assert _agents(report)["codex"]["status"] == "connected"
    assert "default_tools_approval_mode" not in _codex_entry(world)

    rc, report = world.connect("--agent", "codex", capsys=capsys)
    assert rc == 0, report
    assert _agents(report)["codex"]["status"] == "connected"
    assert _codex_entry(world)["default_tools_approval_mode"] == "approve"


def test_only_the_codex_entry_carries_the_approval_key(
    world: World, capsys,
) -> None:
    world.codex_config()
    world.cursor_config()
    rc, report = world.connect("--agent", "codex", "--agent", "cursor", capsys=capsys)
    assert rc == 0, report
    cursor = json.loads((world.home / ".cursor" / "mcp.json").read_text())
    assert "default_tools_approval_mode" not in cursor["mcpServers"][KEY]
