"""`okg agent connect` registrations start from anywhere (PostgreSQL).

PACT agent-connect-3412, task.connect.starts-anywhere and task.connect.ready.

A published standalone deployment lives where `okg install` puts it
(`$HOME/.local/share/okg/deployments/<name>`), with its database URL in the
deployment's `.okg/local.env` and the MCP role's password in
`$HOME/.config/okg/role-secrets/`. `okg agent connect` runs as a separate
process from an unrelated working directory with only PATH, HOME and LANG set
-- no OKG_DEPLOYMENTS_DIR, OKG_TEST_DSN or OKG_MCP_*_DSN. Each of the four
written configs is then read back and its stored command is started the way
an agent starts it: cwd `/`, PATH only, HOME and XDG_* as a real user's. It
must complete an MCP initialize and a tools/list naming the seven read tools.

The refusal rows check that a deployment connect cannot serve leaves every
agent config byte-identical and names the code `okg mcp-serve` reported.
"""
from __future__ import annotations

import hashlib
import json
import os
import queue
import signal
import socket
import subprocess
import sys
import threading
import time
import tomllib
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.local_database_roles import (
    LOCAL_DATABASE_ROLE_PASSWORDS,
    database_target,
    write_role_secrets,
)
from okg.substrate.db import dsn as dsn_module
from tests.substrate.e2e.test_publish_gate_multi_source import (
    _stage_uncommitted_deployment,
)

pytestmark = pytest.mark.postgres

READ_TOOLS = {"inspect", "search", "expand", "filter", "map", "aggregate", "query"}
AGENT_PATH = "/usr/bin:/bin"
SPAWN_TIMEOUT = 120.0
CONNECT_TIMEOUT = 300.0
SECRET_MARKERS = ("postgres://", "postgresql://", "password=", "sk-", "ghp_")


# ------------------------------------------------------------------ helpers


class User:
    """A temporary user account: HOME, an unrelated cwd and the four agents."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.home = tmp_path / "home"
        self.cwd = tmp_path / "somewhere-else"
        self.home.mkdir()
        self.cwd.mkdir()
        self.deployments = self.home / ".local" / "share" / "okg" / "deployments"
        self.configs = {
            "claude-code": self.home / ".claude.json",
            "cursor": self.home / ".cursor" / "mcp.json",
            "codex": self.home / ".codex" / "config.toml",
            "claude-desktop": self._desktop_path(),
        }
        for agent, path in self.configs.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("" if agent == "codex" else "{}\n", encoding="utf-8")

    def _desktop_path(self) -> Path:
        if sys.platform == "darwin":
            return (
                self.home / "Library" / "Application Support" / "Claude"
                / "claude_desktop_config.json"
            )
        return self.home / ".config" / "Claude" / "claude_desktop_config.json"

    def env(self) -> dict[str, str]:
        """What a login shell gives a command: no okg variable at all."""
        return {
            "PATH": AGENT_PATH,
            "HOME": str(self.home),
            "LANG": "C.UTF-8",
            "TMPDIR": str(self.tmp),
        }

    def agent_env(self) -> dict[str, str]:
        """What an agent gives its MCP server: HOME and XDG_* as a real user's."""
        return {
            **self.env(),
            "XDG_CONFIG_HOME": str(self.home / ".config"),
            "XDG_DATA_HOME": str(self.home / ".local" / "share"),
            "XDG_CACHE_HOME": str(self.home / ".cache"),
        }

    def hashes(self) -> dict[str, str]:
        return {
            agent: hashlib.sha256(path.read_bytes()).hexdigest()
            for agent, path in self.configs.items()
        }

    def connect(self, deployment: str, *extra: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [
                sys.executable, "-m", "okg.substrate.cli.main", "--json",
                "agent", "connect", "--deployment", deployment,
                "--agent", "claude-code", "--agent", "claude-desktop",
                "--agent", "cursor", "--agent", "codex", *extra,
            ],
            cwd=self.cwd,
            env=self.env(),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=CONNECT_TIMEOUT,
            start_new_session=True,
        )

    def entries(self, key: str, agents: tuple[str, ...] | None = None) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for agent, path in self.configs.items():
            if agents is not None and agent not in agents:
                continue
            text = path.read_text(encoding="utf-8")
            if agent == "codex":
                out[agent] = tomllib.loads(text)["mcp_servers"][key]
            else:
                out[agent] = json.loads(text)["mcpServers"][key]
        return out


def _reap(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except OSError:
        proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except OSError:
            proc.kill()
        proc.wait(timeout=15)


def _tools_list(entry: dict, env: dict[str, str]) -> tuple[set[str], str]:
    """Start ``entry`` as an agent would and return (tool names, stderr)."""
    proc = subprocess.Popen(
        [entry["command"], *entry["args"]],
        cwd="/",
        env={**env, **{k: str(v) for k, v in (entry.get("env") or {}).items()}},
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    lines: queue.Queue[str | None] = queue.Queue()
    errors: list[str] = []

    def _pump_out() -> None:
        for line in proc.stdout:  # type: ignore[union-attr]
            lines.put(line)
        lines.put(None)

    def _pump_err() -> None:
        for line in proc.stderr:  # type: ignore[union-attr]
            errors.append(line)

    threading.Thread(target=_pump_out, daemon=True).start()
    threading.Thread(target=_pump_err, daemon=True).start()

    def _send(message: dict) -> None:
        proc.stdin.write(json.dumps(message) + "\n")  # type: ignore[union-attr]
        proc.stdin.flush()  # type: ignore[union-attr]

    def _await(request_id: int, deadline: float) -> dict:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(
                    f"no answer to request {request_id}; stderr:\n" + "".join(errors)
                )
            try:
                line = lines.get(timeout=remaining)
            except queue.Empty:
                continue
            if line is None:
                raise AssertionError(
                    f"server exited before answering {request_id} "
                    f"(rc={proc.poll()}); stderr:\n" + "".join(errors)
                )
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if isinstance(message, dict) and message.get("id") == request_id:
                return message

    deadline = time.monotonic() + SPAWN_TIMEOUT
    try:
        _send({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "okg-agent-connect-spawn", "version": "0"},
            },
        })
        init = _await(1, deadline)
        assert "result" in init, init
        _send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        _send({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        listed = _await(2, deadline)
        assert "result" in listed, listed
        return {tool["name"] for tool in listed["result"]["tools"]}, "".join(errors)
    finally:
        _reap(proc)


def _write_local_env(deployment_dir: Path, dsn: str) -> Path:
    local_env = deployment_dir / ".okg" / "local.env"
    local_env.parent.mkdir(parents=True, exist_ok=True)
    local_env.write_text(f"OKG_DSN={dsn}\n", encoding="utf-8")
    local_env.chmod(0o600)
    return local_env


def _secrets_path(user: User, dsn: str) -> Path:
    target = database_target(dsn)
    return (
        user.home / ".config" / "okg" / "role-secrets"
        / f"{target['host']}-{target['port']}.json"
    )


def _record_role_passwords(user: User, dsn: str, *, okg_mcp_password: str) -> Path:
    def _role(role: str, password: str) -> str:
        return dsn_module.parse(dsn).with_params(user=role, password=password).render()

    return write_role_secrets(
        dsn,
        {
            "okg_mcp": _role("okg_mcp", okg_mcp_password),
            "app_rw": _role("app_rw", LOCAL_DATABASE_ROLE_PASSWORDS["app_rw"]),
        },
        path=_secrets_path(user, dsn),
    )


def _publish(
    user: User, dsn: str, capsys, monkeypatch: pytest.MonkeyPatch,
) -> tuple[str, Path]:
    """Publish a standalone deployment into the user's deployments folder."""
    name = f"agc{user.tmp.name.lower().replace('-', '')[-7:]}"
    target = _stage_uncommitted_deployment(
        user.home / ".local" / "share" / "okg", capsys,
        name=name, standalone=True,
    )
    assert target.parent == user.deployments
    manifest_path = target / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest.setdefault("postgres", {})["dsn"] = "${OKG_DSN}"
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")
    _write_local_env(target, dsn)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(user.deployments))
    assert main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(target), "--dsn", dsn, "--force",
    ]) == 0
    capsys.readouterr()
    rc = main(["provision", "--deployment", name, "--dsn", dsn, "--json"])
    out = capsys.readouterr()
    proof = json.loads((out.out or out.err).strip().splitlines()[-1])
    assert rc == 0, proof
    assert proof["publish_proof"]["status"] == "published", proof
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR")
    return name, target


def _error(result: subprocess.CompletedProcess) -> dict:
    text = result.stderr.strip().splitlines()
    for line in reversed(text):
        if line.startswith("{"):
            return json.loads(line)["error"]
    raise AssertionError(f"no JSON error on stderr:\n{result.stderr}\n{result.stdout}")


def _closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def user(tmp_path: Path) -> User:
    return User(tmp_path)


# --------------------------------------------- task.connect.starts-anywhere


def test_every_written_entry_lists_the_read_tools_from_root(
    user: User, clean_runtime: str, okg_mcp_can_log_in: str, capsys, monkeypatch,
) -> None:
    name, target = _publish(user, clean_runtime, capsys, monkeypatch)
    secrets = _record_role_passwords(
        user, clean_runtime,
        okg_mcp_password=LOCAL_DATABASE_ROLE_PASSWORDS["okg_mcp"],
    )

    result = user.connect(name)

    assert result.returncode == 0, (result.stdout, result.stderr)
    report = json.loads(result.stdout)
    assert {row["agent"]: row["status"] for row in report["agents"]} == {
        "claude-code": "connected",
        "claude-desktop": "connected",
        "cursor": "connected",
        "codex": "connected",
    }
    assert report["role_secrets_file"] == str(secrets)
    assert report["warnings"] == [] or all(
        "role-passwords" not in w for w in report["warnings"]
    )

    entries = user.entries(f"okg-{name}")
    for agent, entry in entries.items():
        args = entry["args"]
        launcher = args[: args.index("--")]
        assert launcher[launcher.index("--workspace-root") + 1] == str(target)
        assert launcher[launcher.index("--deployments-dir") + 1] == str(user.deployments)
        assert launcher[launcher.index("--role-secrets-file") + 1] == str(secrets)
        tools, stderr = _tools_list(entry, user.agent_env())
        assert READ_TOOLS <= tools, (agent, sorted(tools), stderr)

    for agent, path in user.configs.items():
        text = path.read_text(encoding="utf-8")
        for marker in SECRET_MARKERS + (LOCAL_DATABASE_ROLE_PASSWORDS["okg_mcp"] + "@",):
            assert marker not in text, (agent, marker)


def test_write_enabled_entry_on_the_default_layout_lists_tools(
    user: User, clean_runtime: str, okg_mcp_can_log_in: str, capsys, monkeypatch,
) -> None:
    """`--writes` with the database URL in `<deployment>/.okg/local.env`.

    Decision `writesenv C`: the write-enabled profile may write the
    deployment folder except its credentials file, so the launcher accepts
    the deployment's own local env file and the server starts.
    """
    from tests.substrate.conftest import ensure_builtin_dev_logins

    ensure_builtin_dev_logins(clean_runtime)
    name, target = _publish(user, clean_runtime, capsys, monkeypatch)
    _record_role_passwords(
        user, clean_runtime,
        okg_mcp_password=LOCAL_DATABASE_ROLE_PASSWORDS["okg_mcp"],
    )
    local_env = target / ".okg" / "local.env"

    result = subprocess.run(
        [
            sys.executable, "-m", "okg.substrate.cli.main", "--json",
            "agent", "connect", "--deployment", name,
            "--agent", "cursor", "--agent", "codex", "--writes",
        ],
        cwd=user.cwd, env=user.env(), stdin=subprocess.DEVNULL,
        capture_output=True, text=True, timeout=CONNECT_TIMEOUT,
        start_new_session=True,
    )

    assert result.returncode == 0, (result.stdout, result.stderr)
    report = json.loads(result.stdout)
    assert report["profile"] == "deploy"
    entries = user.entries(f"okg-{name}", ("cursor", "codex"))
    for agent in ("cursor", "codex"):
        entry = entries[agent]
        launcher = entry["args"][: entry["args"].index("--")]
        assert launcher[launcher.index("--permissions") + 1] == "deploy"
        assert launcher[launcher.index("--local-env") + 1] == str(local_env)
        assert "--allow-writes" in entry["args"]
        tools, stderr = _tools_list(entry, user.agent_env())
        assert READ_TOOLS <= tools, (agent, sorted(tools), stderr)


# ------------------------------------------------------ task.connect.ready


def test_unknown_deployment_writes_nothing(user: User, okg_migrated: str) -> None:
    before = user.hashes()
    result = user.connect("no-such-deployment")
    assert result.returncode != 0
    err = _error(result)
    assert err["code"] == "deployment_not_found", err
    assert err.get("fix_hint")
    assert user.hashes() == before


def test_a_stopped_database_writes_nothing(
    user: User, okg_migrated: str, capsys,
) -> None:
    name = "agcstopped"
    target = _stage_uncommitted_deployment(
        user.home / ".local" / "share" / "okg", capsys,
        name=name, standalone=True,
    )
    manifest_path = target / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest.setdefault("postgres", {})["dsn"] = "${OKG_DSN}"
    manifest_path.write_text(yaml.safe_dump(manifest), encoding="utf-8")
    dead = dsn_module.parse(okg_migrated).with_params(port=str(_closed_port())).render()
    _write_local_env(target, dead)
    before = user.hashes()

    result = user.connect(name)

    assert result.returncode != 0, result.stdout
    err = _error(result)
    assert err["code"] == "database_unreachable", err
    assert err.get("fix_hint"), err
    assert user.hashes() == before


def test_an_mcp_role_that_cannot_log_in_writes_nothing(
    user: User, clean_runtime: str, okg_mcp_can_log_in: str, capsys, monkeypatch,
) -> None:
    name, _target = _publish(user, clean_runtime, capsys, monkeypatch)
    # The recorded password is not the role's: okg_mcp cannot log in with it.
    _record_role_passwords(user, clean_runtime, okg_mcp_password="not-its-password")
    before = user.hashes()

    result = user.connect(name)

    assert result.returncode != 0, result.stdout
    err = _error(result)
    assert err["code"] == "role_password_rejected", err
    assert "okg db role-passwords --apply" in err["fix_hint"], err
    assert user.hashes() == before


def test_builtin_role_passwords_connect_read_only_with_the_fix(
    user: User, clean_runtime: str, okg_mcp_can_log_in: str, capsys, monkeypatch,
) -> None:
    name, _target = _publish(user, clean_runtime, capsys, monkeypatch)
    assert not _secrets_path(user, clean_runtime).exists()

    result = user.connect(name)

    assert result.returncode == 0, (result.stdout, result.stderr)
    report = json.loads(result.stdout)
    assert report["profile"] == "read-only"
    warning = " ".join(report["warnings"])
    assert f"okg db role-passwords --apply --deployment {name}" in warning
    entry = user.entries(f"okg-{name}")["cursor"]
    tools, stderr = _tools_list(entry, user.agent_env())
    assert READ_TOOLS <= tools, (sorted(tools), stderr)

    refused = user.connect(name, "--writes")
    assert refused.returncode != 0
    assert _error(refused)["code"] == "agent_connect_writes_need_role_passwords"


# ------------------------- okg install connects agents (PACT why, #3440)

from tests.substrate.cli.test_minimal_default_install import (  # noqa: E402,F401
    empty_database,
)


def test_minimal_install_connects_a_present_agent_whose_entry_lists_tools(
    user: User,
    empty_database: str,  # noqa: F811 -- the fixture imported above
    okg_mcp_can_log_in: str,
    capsys,
    monkeypatch,
) -> None:
    """A real `okg install` (minimal starter) on an empty database with
    Cursor present: the install's own connect step registers it read-only,
    and the stored entry, started from `/`, lists the read tools."""
    from okg.substrate.library.agent_runtimes import connect

    for var in list(os.environ):
        if var.startswith(("OKG_MCP_", "XDG_")) or var in (
            "OKG_DEPLOYMENTS_DIR", "OKG_ENVIRONMENT_DESCRIPTOR",
            "OKG_NOMOS_RUNTIME_ENFORCEMENT", "CODEX_HOME",
        ):
            monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("HOME", str(user.home))
    monkeypatch.setenv("OKG_DSN", empty_database)
    monkeypatch.setenv(connect.ENABLE_IN_TESTS_ENV, "1")
    monkeypatch.setattr(connect, "desktop_running", lambda: False)
    monkeypatch.chdir(user.cwd)
    # Only Cursor is present: drop the other three agents' files.
    for agent in ("claude-code", "codex", "claude-desktop"):
        user.configs[agent].unlink()
    for folder in (user.home / ".codex", user.configs["claude-desktop"].parent):
        folder.rmdir()
    monkeypatch.setenv("PATH", AGENT_PATH)  # no `claude` or `codex` binary
    monkeypatch.setattr(connect, "SYSTEM_APPLICATION_DIRS", ())
    root = user.tmp / "deployments"

    rc = main([
        "install", "--deployment-name", "teamconnect",
        "--deployments-root", str(root),
        "--postgres-dsn", empty_database,
        "--non-interactive", "--json",
    ])
    out = capsys.readouterr()
    assert out.out.strip(), (rc, out.err[-3000:])
    payload = json.loads(out.out)

    assert rc == 0, payload
    step = payload["agents"]
    assert step["connected"] == ["cursor"], step
    assert "Connected Cursor to teamconnect" in step["message"]
    entry = json.loads(user.configs["cursor"].read_text())["mcpServers"]["okg-teamconnect"]
    launcher_args = entry["args"][: entry["args"].index("--")]
    assert launcher_args[launcher_args.index("--permissions") + 1] == "read-only"
    tools, stderr = _tools_list(entry, user.agent_env())
    assert READ_TOOLS <= tools, (sorted(tools), stderr)
