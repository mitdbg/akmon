"""`okg agent connect` / `okg agent disconnect` (PACT agent-connect-3412).

One command registers an existing deployment's MCP server with Claude Code,
Claude Desktop, Cursor and Codex, and installs the okg user skills where each
agent reads them. It reuses the four writers in this package for the config
locations and file formats, `launcher.build_profiled_mcp_server_entry` for the
entry, and `pact.agent_install.sync_agent_skills` for the skills.

What makes a registration work wherever the agent starts it:

* The entry runs `agent-runtime-launch`, which replaces HOME and
  XDG_CONFIG_HOME and drops XDG_DATA_HOME before it starts `mcp-serve`. So
  connect resolves, once, the absolute paths `mcp-serve` would otherwise look
  up through those variables -- the deployments folder, the deployment's
  `.okg/local.env` and the role-password file -- and writes them as explicit
  launcher flags. `--workspace-root` is the deployment folder, never the
  directory connect happens to run in.
* Before it writes any agent config, connect starts that exact entry the way
  an agent does (cwd `/`, the user's PATH and HOME) and asks it for its tools.
  A server that cannot start refuses the whole connect with the code
  `mcp-serve` reported, and nothing is written.

No written config holds a DSN, a password or a token: only paths and flags.
"""
from __future__ import annotations

import contextlib
import io
import json
import math
import os
import platform
import queue
import re
import shlex
import shutil
import signal
import subprocess
import threading
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

from okg.substrate.db.dsn import redact_text
from okg.substrate.errors import SubstrateError

from . import (
    EPHEMERAL_OKG_FIX_HINT,
    AgentConfigParseError,
    EphemeralOkgInterpreterError,
    atomic_write_text,
    backup_agent_config,
    is_connect_entry,
)


CONNECT_AGENTS: tuple[str, ...] = ("claude-code", "claude-desktop", "cursor", "codex")
AGENT_LABELS = {
    "claude-code": "Claude Code",
    "claude-desktop": "Claude Desktop",
    "cursor": "Cursor",
    "codex": "Codex",
}
#: Bundled skills for people who use a deployment. The okg-development skills
#: (pact, agent-workflow, dev-process) are never installed by connect.
USER_SKILLS: tuple[str, ...] = (
    "okg-add-source",
    "okg-ontology-design",
    "okg-provision",
    "okg-query",
    "okg-troubleshoot",
)
WRITE_SKILLS: tuple[str, ...] = ("okg-record",)
READ_TOOLS: tuple[str, ...] = (
    "inspect", "search", "expand", "filter", "map", "aggregate", "query",
)
#: Where macOS installs apps for every user; tests replace it.
SYSTEM_APPLICATION_DIRS: tuple[Path, ...] = (Path("/Applications"),)
CODEX_BACKUP_SUFFIX = ".okg-backup"  # the same name every agent config uses
#: Codex waits `startup_timeout_sec` (default 10 s) for a server to start.
#: connect writes max(60, 3 x the measured start), capped at 180.
CODEX_STARTUP_FLOOR_SEC = 60
CODEX_STARTUP_CAP_SEC = 180
#: Codex asks before each MCP tool call unless the server entry approves its
#: tools, and a headless `codex exec` (approval policy `never`) refuses any
#: call it would have to ask about. The read-only entry approves okg's tools,
#: which only read; the write-enabled entry keeps Codex's prompt.
CODEX_READ_APPROVAL = ("default_tools_approval_mode", "approve")
#: Agents whose start-up limit is not documented get a warning above this.
SLOW_START_WARNING_SEC = 10.0
SCHEMA = "okg.agent_connect.v1"
DESKTOP_UPLOAD_STEP = (
    "In Claude Desktop open Settings > Capabilities > Skills, choose "
    "Upload skill, and upload each ZIP"
)


# ---------------------------------------------------------------- errors


def _error(
    code: str, message: str, *, fix_hint: str | None = None,
    data: dict[str, Any] | None = None,
) -> SubstrateError:
    err = SubstrateError(message, data=data or {}, fix_hint=fix_hint)
    err.code = code
    return err


# ------------------------------------------------------------- the agents


def _writer(agent: str):
    from .claude_code import ClaudeCodeRuntime
    from .claude_desktop import ClaudeDesktopRuntime
    from .codex_cli import CodexCliRuntime
    from .cursor import CursorRuntime

    return {
        "claude-code": ClaudeCodeRuntime,
        "claude-desktop": ClaudeDesktopRuntime,
        "cursor": CursorRuntime,
        "codex": CodexCliRuntime,
    }[agent]()


def _application_dirs() -> tuple[Path, ...]:
    return (*SYSTEM_APPLICATION_DIRS, Path.home() / "Applications")


def _app_installed(bundle: str) -> bool:
    if platform.system() != "Darwin":
        return False
    return any((root / bundle).exists() for root in _application_dirs())


def agent_presence(agent: str) -> Optional[str]:
    """Why ``agent`` counts as present on this machine, or None.

    Present means its config file exists, or its app or binary is installed;
    a present agent with no config file gets a new one.
    """
    writer = _writer(agent)
    cfg = writer.config_path()
    if cfg is not None and cfg.is_file():
        return f"config file {cfg}"
    if agent == "claude-code":
        found = shutil.which("claude")
        return f"`claude` at {found}" if found else None
    if agent == "claude-desktop":
        if cfg is not None and cfg.parent.is_dir():
            return f"folder {cfg.parent}"
        if _app_installed("Claude.app"):
            return "Claude.app"
        found = shutil.which("claude-desktop")
        return f"`claude-desktop` at {found}" if found else None
    if agent == "cursor":
        if cfg is not None and cfg.parent.is_dir():
            return f"folder {cfg.parent}"
        if _app_installed("Cursor.app"):
            return "Cursor.app"
        found = shutil.which("cursor")
        return f"`cursor` at {found}" if found else None
    if agent == "codex":
        if cfg is not None and cfg.parent.is_dir():
            return f"folder {cfg.parent}"
        found = shutil.which("codex")
        return f"`codex` at {found}" if found else None
    return None


DESKTOP_APP_BINARY = "/Applications/Claude.app/Contents/MacOS/Claude"

#: How `pgrep` finds a running Claude Desktop, per platform.
#:
#: macOS: BSD pgrep leaves out its own ancestors unless given `-a`. When an
#: agent runs `okg agent connect` from inside Claude Desktop (its Code tab),
#: Desktop IS an ancestor, so without `-a` a running Desktop was reported as
#: not running and its entry was written and then lost (laptop, 2026-10-06).
#: Linux: procps pgrep leaves out only itself (its `-a` means "print the
#: command line"), and `-x` matches the exact process name, because `-f
#: claude-desktop` would also match connect's own `--agent claude-desktop`.
DESKTOP_PROCESS_PATTERNS = {
    "Darwin": ("-a", "-f", DESKTOP_APP_BINARY),
    "Linux": ("-x", "claude-desktop"),
}
#: Where `pgrep` and `ps` are looked for when PATH lacks them.
TOOL_FALLBACK_DIRS: tuple[str, ...] = ("/usr/bin", "/bin")


def _tool(name: str) -> Optional[str]:
    found = shutil.which(name)
    if found:
        return found
    for directory in TOOL_FALLBACK_DIRS:
        candidate = Path(directory) / name
        if candidate.is_file():
            return str(candidate)
    return None


def _run_tool(argv: list[str]) -> Optional[subprocess.CompletedProcess]:
    try:
        return subprocess.run(
            argv, capture_output=True, text=True, timeout=10,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def desktop_running() -> Optional[bool]:
    """Whether the Claude Desktop app is running; None when it cannot tell.

    A running app keeps its config in memory and writes it back, so an entry
    written to the file now is lost (seen 2026-10-06 on macOS). On macOS two
    independent checks are made, `pgrep -a -f <app binary>` and the process
    list from `ps -axo comm`; either one finding the app counts.
    """
    system = platform.system()
    pattern = DESKTOP_PROCESS_PATTERNS.get(system)
    if pattern is None:
        return False
    answers: list[bool] = []
    pgrep = _tool("pgrep")
    if pgrep is not None:
        done = _run_tool([pgrep, *pattern])
        if done is not None and done.returncode in (0, 1):
            answers.append(done.returncode == 0)
    if system == "Darwin":
        ps = _tool("ps")
        if ps is not None:
            done = _run_tool([ps, "-axo", "comm"])
            if done is not None and done.returncode == 0:
                answers.append(any(
                    line.strip() == DESKTOP_APP_BINARY
                    for line in done.stdout.splitlines()
                ))
    if any(answers):
        return True
    return False if answers else None


def _desktop_running_reason(deployment: str, action: str) -> str:
    return (
        "Claude Desktop is running and rewrites its config file from memory, "
        "so an entry written now would be lost. Quit Claude Desktop "
        f"completely, then run: okg agent {action} --deployment {deployment} "
        "--agent claude-desktop"
    )


def _desktop_blocked(
    deployment: str, action: str, warnings: list[str],
) -> Optional[str]:
    """The refusal reason when Desktop is running, else None."""
    state = desktop_running()
    if state:
        return _desktop_running_reason(deployment, action)
    if state is None:
        warnings.append(
            "could not tell whether Claude Desktop is running (no usable "
            "`pgrep` or `ps`); if it is, quit it and re-run, or the entry may "
            "be lost"
        )
    return None


# ------------------------------------------------------- deployment paths


@dataclass
class DeploymentPaths:
    name: str
    deployment_dir: Path
    deployments_dir: Path
    local_env: Optional[Path] = None
    local_env_source: Optional[str] = None
    role_secrets_file: Optional[Path] = None
    dsn: Optional[str] = field(default=None, repr=False)
    dsn_note: Optional[str] = None
    #: ``${VAR}`` names in the manifest's database URL that the registered
    #: server will not find (issue #3583).
    dsn_missing: list[str] = field(default_factory=list)
    local_env_note: Optional[str] = None
    check_notes: list[str] = field(default_factory=list)


@contextlib.contextmanager
def _child_like_environment(
    deployments_dir: Path, local_env: Optional[Path],
) -> Iterator[None]:
    """``os.environ`` as `mcp-serve` will see it behind the launcher.

    The launcher drops every ambient OKG_* variable and sets the explicit
    path bindings; `mcp-serve` then loads the local env file. Resolving the
    database URL under the same view keeps connect from finding a URL the
    registered server will not have.
    """
    saved = dict(os.environ)
    try:
        for key in list(os.environ):
            if key.startswith("OKG_"):
                del os.environ[key]
        os.environ["OKG_DEPLOYMENTS_DIR"] = str(deployments_dir)
        if local_env is not None:
            from okg.substrate.local_env import parse_local_env

            os.environ["OKG_LOCAL_ENV"] = str(local_env)
        if local_env is not None and local_env.is_file():
            for key, value in parse_local_env(local_env).items():
                if key.startswith("OKG_") and value:
                    os.environ[key] = value
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


def resolve_deployment_paths(
    name: str, *, deployment_dir: Optional[Path] = None,
) -> DeploymentPaths:
    """Find the deployment and the paths its registered server needs.

    ``deployment_dir`` names the folder directly (``okg install`` passes the
    one it just wrote, which may sit under its own ``--deployments-root``).
    """
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        find_deployment_dir,
    )

    detail = ""
    if deployment_dir is not None:
        found = (
            deployment_dir
            if (deployment_dir / "deployment.yaml").is_file() else None
        )
        if found is None:
            detail = f"{deployment_dir} has no deployment.yaml"
    else:
        try:
            found = find_deployment_dir(name)
        except DeploymentResolutionError as exc:
            found = None
            detail = str(exc)
    if found is None:
        raise _error(
            "deployment_not_found",
            f"okg agent connect: no deployment named {name!r} was found"
            + (f" ({detail})" if detail else ""),
            fix_hint=(
                "run `okg deployments` to list deployment names, or create "
                "one with `okg install`; a deployment outside the default "
                "folder needs OKG_DEPLOYMENTS_DIR set to the folder that "
                "holds it when you run this command"
            ),
            data={"deployment": name},
        )
    deployment_dir = found.resolve()
    paths = DeploymentPaths(
        name=name,
        deployment_dir=deployment_dir,
        deployments_dir=deployment_dir.parent,
    )
    paths.local_env, paths.local_env_source, paths.local_env_note = (
        _find_local_env(deployment_dir)
    )
    try:
        # With no local env file yet, pin the one the deployment would use:
        # the registered server starts from `/` and never reads the file in
        # connect's working directory (issue #3583).
        with _child_like_environment(
            paths.deployments_dir,
            paths.local_env or _default_local_env(deployment_dir),
        ):
            from okg.substrate.deployment_authority import (
                resolve_deployment_dsn_authority,
            )

            resolution = resolve_deployment_dsn_authority(name)
        dsn = resolution.get("dsn")
        if isinstance(dsn, str) and dsn:
            paths.dsn = dsn
        else:
            paths.dsn_note = redact_text(str(resolution.get("reason") or "unresolved"))
            authority = resolution.get("authority")
            missing = authority.get("missing") if isinstance(authority, dict) else None
            if isinstance(missing, list):
                paths.dsn_missing = [str(name) for name in missing if name]
    except Exception as exc:  # noqa: BLE001 -- reported; the server check decides
        paths.dsn_note = redact_text(f"{type(exc).__name__}: {exc}")
    paths.role_secrets_file = _find_role_secrets(paths.dsn)
    return paths


def _find_local_env(
    deployment_dir: Path,
) -> tuple[Optional[Path], Optional[str], Optional[str]]:
    """The deployment's own local env file: ``(path, source, note)``.

    The deployment's ``.okg/local.env`` first, then its repository's. An
    ``OKG_LOCAL_ENV`` in connect's own environment is machine-wide (often set
    for a different graph) and is never written into a registration; when it
    names another file, it is ignored and ``note`` says so.
    """
    chosen: tuple[Optional[Path], Optional[str]] = (None, None)
    own = deployment_dir / ".okg" / "local.env"
    if own.is_file():
        chosen = (own, "deployment")
    else:
        from okg.substrate.local_env import find_repo_root

        repo = find_repo_root(deployment_dir)
        if repo is not None:
            shared = repo / ".okg" / "local.env"
            if shared.is_file():
                chosen = (shared.resolve(), "repository")
    note = None
    explicit = (os.environ.get("OKG_LOCAL_ENV") or "").strip()
    if explicit:
        named = Path(explicit).expanduser().resolve()
        if chosen[0] is None or named != chosen[0].resolve():
            note = (
                f"OKG_LOCAL_ENV={explicit} is set in this shell but is not the "
                "deployment's own local env file ("
                + (str(chosen[0]) if chosen[0] else "the deployment has none")
                + "); it was ignored and is not written into any registration"
            )
    return chosen[0], chosen[1], note


def _find_role_secrets(dsn: Optional[str]) -> Optional[Path]:
    from okg.substrate.local_database_roles import (
        ROLE_SECRETS_FILE_ENV,
        role_secrets_path,
    )

    explicit = (os.environ.get(ROLE_SECRETS_FILE_ENV) or "").strip()
    if explicit:
        path = Path(explicit).expanduser()
    elif dsn:
        try:
            path = role_secrets_path(dsn)
        except (SubstrateError, ValueError):
            return None
    else:
        return None
    path = path if path.is_absolute() else path.resolve()
    return path if path.is_file() else None


_ROLE_ENV = {"okg_mcp": "OKG_MCP_RO_DSN", "app_rw": "OKG_MCP_RW_DSN"}


def builtin_role_password_state(
    paths: DeploymentPaths, role: str = "okg_mcp",
) -> bool:
    """True when the server would have to log in as ``role`` with its public
    built-in password.

    That is a fresh install: a local database, no recorded password for the
    role (no role-password entry, no OKG_MCP_RO_DSN / OKG_MCP_RW_DSN in the
    local env file), and the role still accepts its built-in password.
    ``role`` is ``okg_mcp`` (read) or ``app_rw`` (writes).
    """
    from okg.substrate.local_database_roles import (
        builtin_password_findings,
        dsn_is_local,
        read_role_secrets,
    )

    if not paths.dsn or not dsn_is_local(paths.dsn):
        return False
    if paths.local_env is not None:
        from okg.substrate.local_env import parse_local_env

        if parse_local_env(paths.local_env).get(_ROLE_ENV[role]):
            return False
    if paths.role_secrets_file is not None:
        try:
            if read_role_secrets(paths.dsn, path=paths.role_secrets_file).get(
                role
            ):
                return False
        except SubstrateError as exc:
            # The server check refuses on the same unusable file; say why
            # the built-in password state was not probed.
            paths.check_notes.append(
                f"role-password file {paths.role_secrets_file} is not usable "
                f"({exc.code}); the built-in password check was skipped"
            )
            return False
    import psycopg

    try:
        findings = builtin_password_findings(paths.dsn)
    except (psycopg.Error, SubstrateError, OSError) as exc:
        # Unreachable or refused: the server check that follows reports the
        # database error itself and refuses; this note says why no built-in
        # password check happened.
        paths.check_notes.append(
            f"could not check whether role {role!r} still accepts its "
            f"built-in password ({type(exc).__name__}: "
            f"{redact_text(str(exc)).strip()[:300]})"
        )
        return False
    return any(
        row.get("code") == "builtin_role_password" and row.get("role") == role
        for row in findings
    )


# ------------------------------------------------------------- the entry


def build_entry(
    agent: str,
    paths: DeploymentPaths,
    *,
    writes: bool,
    allow_builtin: bool,
    startup_timeout_sec: Optional[int] = None,
) -> dict[str, Any]:
    from . import launcher

    writer = _writer(agent)
    entry = launcher.build_profiled_mcp_server_entry(
        runtime=writer.name,
        deployment=paths.name,
        dsn="",
        extra_env={"OKG_MCP_ALLOW_WRITES": "1"} if writes else None,
        workspace_root=paths.deployment_dir,
        deployments_dir=paths.deployments_dir,
        local_env=paths.local_env,
        role_secrets_file=paths.role_secrets_file,
        allow_builtin_role_passwords=allow_builtin,
    )
    entry = writer.shape_entry(entry)
    if agent == "codex":
        entry["startup_timeout_sec"] = int(
            startup_timeout_sec or CODEX_STARTUP_FLOOR_SEC
        )
        if not writes:
            key, value = CODEX_READ_APPROVAL
            entry[key] = value
    return entry


def codex_startup_timeout(start_seconds: Optional[float]) -> int:
    """max(60, 3 x the measured start), capped at 180 seconds."""
    if start_seconds is None:
        return CODEX_STARTUP_FLOOR_SEC
    wanted = math.ceil(3 * start_seconds)
    return min(CODEX_STARTUP_CAP_SEC, max(CODEX_STARTUP_FLOOR_SEC, wanted))


# ------------------------------------------------------- the server check


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    tools: tuple[str, ...] = ()
    code: Optional[str] = None
    message: Optional[str] = None
    fix_hint: Optional[str] = None
    stderr_tail: str = ""
    #: Seconds from start until the server answered tools/list.
    seconds: Optional[float] = None


_ERROR_LINE = re.compile(r"^error \[([A-Za-z0-9_.:-]+)\]: (.*)$", re.MULTILINE)
_FIX_LINE = re.compile(r"^\s*fix_hint: (.*)$", re.MULTILINE)


def _probe_environment(entry: dict[str, Any]) -> dict[str, str]:
    env = {
        key: os.environ[key]
        for key in ("PATH", "HOME", "USER", "LOGNAME", "LANG", "TMPDIR", "SHELL")
        if os.environ.get(key)
    }
    env.update({
        key: value for key, value in os.environ.items()
        if key.startswith(("XDG_", "LC_"))
    })
    env.update({str(k): str(v) for k, v in (entry.get("env") or {}).items()})
    return env


def probe_entry(entry: dict[str, Any], *, timeout: float) -> ProbeResult:
    """Start ``entry`` as an agent does and ask it for its tools.

    cwd `/`, the user's PATH/HOME/XDG_* plus the entry's own env. Sends MCP
    initialize, then tools/list, over stdio. A server that exits or stays
    silent is reported with the error code and fix it printed.
    """
    try:
        proc = subprocess.Popen(
            [str(entry["command"]), *[str(a) for a in entry["args"]]],
            cwd="/",
            env=_probe_environment(entry),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
    except OSError as exc:
        return ProbeResult(
            ok=False,
            code="agent_connect_server_failed",
            message=f"the registered command could not be started: {exc}",
            fix_hint="reinstall okg so the interpreter path in the entry exists",
        )
    lines: queue.Queue[Optional[str]] = queue.Queue()
    errors: list[str] = []

    def _pump_out() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            lines.put(line)
        lines.put(None)

    def _pump_err() -> None:
        assert proc.stderr is not None
        for line in proc.stderr:
            errors.append(line)

    started = time.monotonic()
    readers = [
        threading.Thread(target=_pump_out, daemon=True),
        threading.Thread(target=_pump_err, daemon=True),
    ]
    for reader in readers:
        reader.start()
    deadline = time.monotonic() + timeout

    def _send(message: dict[str, Any]) -> bool:
        try:
            assert proc.stdin is not None
            proc.stdin.write(json.dumps(message) + "\n")
            proc.stdin.flush()
            return True
        except (BrokenPipeError, OSError):
            return False

    def _await(request_id: int) -> Optional[dict[str, Any]]:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                line = lines.get(timeout=min(remaining, 1.0))
            except queue.Empty:
                continue
            if line is None:
                return None
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if isinstance(message, dict) and message.get("id") == request_id:
                return message

    try:
        answer: Optional[dict[str, Any]] = None
        if _send({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "okg-agent-connect", "version": "1"},
            },
        }):
            init = _await(1)
            if init is not None and "result" in init:
                _send({"jsonrpc": "2.0", "method": "notifications/initialized"})
                if _send({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}):
                    answer = _await(2)
        elapsed = time.monotonic() - started
    finally:
        _stop(proc)
        for reader in readers:
            reader.join(timeout=5)
    stderr = "".join(errors)
    tail = redact_text("\n".join(stderr.strip().splitlines()[-20:]))
    if answer is not None and isinstance(answer.get("result"), dict):
        tools = tuple(sorted(
            str(tool.get("name"))
            for tool in answer["result"].get("tools") or []
            if isinstance(tool, dict)
        ))
        missing = [name for name in READ_TOOLS if name not in tools]
        if missing:
            return ProbeResult(
                ok=False, tools=tools,
                code="agent_connect_tools_missing",
                message=(
                    "the server started but did not list the read tools "
                    + ", ".join(missing)
                ),
                fix_hint="run `okg doctor --deployment <name>` and fix what it names",
                stderr_tail=tail,
            )
        return ProbeResult(ok=True, tools=tools, stderr_tail=tail, seconds=elapsed)
    envelope = _json_error_envelope(stderr)
    if envelope is not None:
        return ProbeResult(
            ok=False,
            code=str(envelope.get("code") or "agent_connect_server_failed"),
            message=str(envelope.get("message") or ""),
            fix_hint=envelope.get("fix_hint"),
            stderr_tail=tail,
        )
    matches = _ERROR_LINE.findall(stderr)
    if matches:
        code, message = matches[-1]
        fixes = _FIX_LINE.findall(stderr)
        return ProbeResult(
            ok=False, code=code, message=message,
            fix_hint=fixes[-1] if fixes else None, stderr_tail=tail,
        )
    if time.monotonic() >= deadline:
        return ProbeResult(
            ok=False,
            code="agent_connect_server_timeout",
            message=f"the server did not list its tools within {timeout:.0f}s",
            fix_hint="re-run with a larger --probe-timeout, or run "
            "`okg mcp-serve --deployment <name>` by hand to see why it is slow",
            stderr_tail=tail,
        )
    return ProbeResult(
        ok=False,
        code="agent_connect_server_failed",
        message=(
            f"the server exited (status {proc.returncode}) before listing "
            "its tools"
        ),
        fix_hint="run `okg mcp-serve --deployment <name>` by hand and fix "
        "the error it prints",
        stderr_tail=tail,
    )


def _json_error_envelope(stderr: str) -> Optional[dict[str, Any]]:
    """The last ``{"error": {...}}`` line okg printed (agent mode is JSON)."""
    for line in reversed(stderr.splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except ValueError:
            continue
        if isinstance(payload, dict):
            inner = payload.get("error", payload)
            if isinstance(inner, dict) and inner.get("code"):
                return inner
    return None


def _stop(proc: subprocess.Popen) -> None:
    with contextlib.suppress(OSError):
        if proc.stdin is not None:
            proc.stdin.close()
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except OSError:
        proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except OSError:
            proc.kill()
        proc.wait(timeout=10)


# ------------------------------------------------------- config planning


def _without_type(entry: Any) -> Any:
    if isinstance(entry, dict):
        return {k: v for k, v in entry.items() if k != "type"}
    return entry


def _serves_deployment(entry: Any, deployment: str) -> bool:
    """True when an MCP entry starts `mcp-serve --deployment <deployment>`."""
    if not isinstance(entry, dict):
        return False
    args = [str(a) for a in entry.get("args") or []]
    command = str(entry.get("command") or "")
    tokens = [command, *args]
    if "mcp-serve" not in tokens:
        return False
    after = tokens[tokens.index("mcp-serve") + 1:]
    for index, token in enumerate(after):
        if token == "--deployment" and index + 1 < len(after):
            if after[index + 1] == deployment:
                return True
        if token == f"--deployment={deployment}":
            return True
    return False


def _is_okg_connect_entry(key: str, entry: Any) -> bool:
    """True for an entry `okg agent connect` wrote (see is_connect_entry)."""
    return is_connect_entry(key, entry)


@dataclass
class AgentPlan:
    agent: str
    config_path: Optional[Path]
    present: Optional[str] = None
    status: str = "not found"
    reason: Optional[str] = None
    method: str = "file"
    entry: Optional[dict[str, Any]] = None
    data: Optional[dict[str, Any]] = None
    original_text: Optional[str] = None
    duplicates: list[str] = field(default_factory=list)
    replace_existing: bool = False
    previous: dict[str, Any] = field(default_factory=dict)
    #: Refused only because Claude Desktop is running.
    desktop_running: bool = False
    notes: list[str] = field(default_factory=list)
    skills: Optional[dict[str, Any]] = None

    def to_json(self) -> dict[str, Any]:
        return {
            "agent": self.agent,
            "status": self.status,
            "config_path": str(self.config_path) if self.config_path else None,
            "present": self.present,
            "method": self.method,
            "server_key": None,
            "entry": self.entry,
            "reason": self.reason,
            "duplicates": list(self.duplicates),
            "skills": self.skills,
            "notes": list(self.notes),
        }


def _load(writer, cfg: Path) -> tuple[dict[str, Any], Optional[str]]:
    text = cfg.read_text(encoding="utf-8") if cfg.is_file() else None
    return writer.load_config(cfg), text


def _servers(writer, data: dict[str, Any], cfg: Path) -> dict[str, Any]:
    servers = data.get(writer.servers_key)
    if servers is None:
        return {}
    if not isinstance(servers, dict):
        raise AgentConfigParseError(
            cfg, f"`{writer.servers_key}` is a {type(servers).__name__}, not an object",
        )
    return servers


def _plan_agent(
    agent: str, key: str, deployment: str, entry: dict[str, Any], *, replace: bool,
) -> AgentPlan:
    writer = _writer(agent)
    cfg = writer.config_path()
    plan = AgentPlan(agent=agent, config_path=cfg, entry=entry)
    if agent == "claude-code" and shutil.which("claude"):
        plan.method = "claude mcp add-json"
    try:
        data, text = _load(writer, cfg)
        servers = _servers(writer, data, cfg)
    except (AgentConfigParseError, OSError) as exc:
        plan.status = "refused"
        plan.reason = (
            f"{exc}; fix or move the file, then re-run (the file was not changed)"
            if isinstance(exc, AgentConfigParseError)
            else f"{cfg} could not be read: {exc}"
        )
        return plan
    plan.data, plan.original_text = data, text
    plan.duplicates = sorted(
        other for other, value in servers.items()
        if other != key and _serves_deployment(value, deployment)
    )
    if plan.duplicates and not replace:
        names = ", ".join(repr(name) for name in plan.duplicates)
        plan.status = "refused"
        plan.reason = (
            f"duplicate: {cfg} already has {names} serving deployment "
            f"{deployment!r}; it was left alone. Re-run with --replace to "
            f"remove it and register {key!r}"
        )
        return plan
    existing = servers.get(key)
    plan.replace_existing = existing is not None
    plan.previous = {
        name: servers[name]
        for name in [*plan.duplicates, *([key] if existing is not None else [])]
    }
    if (
        isinstance(existing, dict)
        and "startup_timeout_sec" in entry
        and isinstance(existing.get("startup_timeout_sec"), int)
        and entry["startup_timeout_sec"]
        <= existing["startup_timeout_sec"]
        <= CODEX_STARTUP_CAP_SEC
    ):
        # A longer limit already written still covers this start: keep it,
        # so a re-run with a slightly different start time is `unchanged`.
        entry["startup_timeout_sec"] = existing["startup_timeout_sec"]
    if (
        existing is not None
        and _without_type(existing) == _without_type(entry)
        and not plan.duplicates
    ):
        plan.status = "unchanged"
    else:
        plan.status = "pending"
    return plan


def _apply_agent(plan: AgentPlan, key: str, warnings: list[str]) -> None:
    writer = _writer(plan.agent)
    cfg = plan.config_path
    assert cfg is not None and plan.entry is not None and plan.data is not None
    if plan.method == "claude mcp add-json":
        _note_backup(plan, cfg)
        removed: list[str] = []
        for name in plan.previous:
            _run_claude(["mcp", "remove", "--scope", "user", name], plan)
            if plan.status == "refused":
                _restore_claude_entries(plan, removed)
                return
            removed.append(name)
        _run_claude(
            ["mcp", "add-json", "--scope", "user", key, json.dumps(plan.entry)], plan,
        )
        if plan.status == "refused":
            _restore_claude_entries(plan, removed)
            return
        plan.status = "connected"
        return
    data = plan.data
    servers = data.get(writer.servers_key)
    if not isinstance(servers, dict):
        servers = {}
        data[writer.servers_key] = servers
    for name in plan.duplicates:
        servers.pop(name, None)
    servers[key] = plan.entry
    rendered = writer.render_config(data)
    if plan.original_text is not None and rendered == plan.original_text:
        plan.status = "unchanged"
        return
    _note_backup(plan, cfg)
    if plan.agent == "codex":
        _warn_codex_layout(cfg, plan.original_text, writer, warnings)
    atomic_write_text(cfg, rendered)
    try:
        stored = _servers(writer, writer.load_config(cfg), cfg).get(key)
    except (AgentConfigParseError, OSError) as exc:
        stored, problem = None, str(exc)
    else:
        problem = "the entry is missing"
    if stored is None or _without_type(stored) != _without_type(plan.entry):
        plan.status = "refused"
        plan.reason = (
            f"wrote {key!r} to {cfg}, but reading the file back failed ("
            + (problem if stored is None else "the stored entry differs")
            + "); another program may be rewriting it"
        )
        return
    plan.status = "connected"


def _note_backup(plan: AgentPlan, cfg: Path) -> Optional[Path]:
    """Back up ``cfg`` as ``<file>.okg-backup`` and say so on the plan.

    Only a file with no connect-written entry refreshes the backup, so it
    keeps the latest version okg had not touched.
    """
    writer = _writer(plan.agent)
    backup = backup_agent_config(
        cfg, load=writer.load_config, servers_key=writer.servers_key,
    )
    if backup is not None:
        plan.notes.append(f"the previous {cfg.name} is saved at {backup}")
    return backup


def _warn_codex_layout(
    cfg: Path, original: Optional[str], writer, warnings: list[str],
) -> None:
    """Warn when the TOML rewrite drops comments or layout of ``original``."""
    if not original or not original.strip():
        return
    try:
        loses_text = writer.render_config(writer.load_config(cfg)) != original
    except (AgentConfigParseError, OSError):
        loses_text = True
    if loses_text:
        backup = cfg.with_name(cfg.name + CODEX_BACKUP_SUFFIX)
        saved = (
            f"; the last version without an okg entry is saved at {backup}"
            if backup.is_file() else ""
        )
        warnings.append(
            f"Codex: {cfg} was rewritten without its comments and layout "
            f"(every key and value is kept){saved}"
        )


def _restore_claude_entries(plan: AgentPlan, removed: list[str]) -> None:
    """Put back the entries removed before a failed `claude mcp add-json`."""
    failed_reason = plan.reason
    restored: list[str] = []
    lost: list[str] = []
    for name in removed:
        probe = AgentPlan(agent=plan.agent, config_path=plan.config_path)
        _run_claude(
            ["mcp", "add-json", "--scope", "user", name,
             json.dumps(plan.previous[name])],
            probe,
        )
        (lost if probe.status == "refused" else restored).append(name)
    plan.status = "refused"
    plan.reason = failed_reason or "the `claude` command failed"
    if restored:
        plan.reason += "; restored the previous entries " + ", ".join(
            repr(name) for name in restored
        )
    if lost:
        plan.reason += (
            "; could NOT restore " + ", ".join(repr(name) for name in lost)
            + ": re-add them with `claude mcp add-json --scope user`"
        )


def _run_claude(argv: list[str], plan: AgentPlan) -> None:
    claude = shutil.which("claude")
    if claude is None:
        plan.status = "refused"
        plan.reason = "`claude` is no longer on PATH"
        return
    try:
        done = subprocess.run(
            [claude, *argv], capture_output=True, text=True, timeout=120,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        plan.status = "refused"
        plan.reason = f"`claude {argv[0]} {argv[1]}` failed: {exc}"
        return
    if done.returncode != 0:
        plan.status = "refused"
        detail = (done.stderr or done.stdout or "").strip()[-400:]
        plan.reason = (
            f"`claude {argv[0]} {argv[1]}` exited {done.returncode}: {detail}"
        )


# --------------------------------------------------------------- skills


def _skill_names(writes: bool) -> tuple[str, ...]:
    return USER_SKILLS + (WRITE_SKILLS if writes else ())


def _desktop_zip_dir() -> Path:
    from okg.substrate.deployments import _user_data_dir

    return _user_data_dir() / "agent-skills" / "claude-desktop"


def _skill_zip_bytes(skill_dir: Path) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(p for p in skill_dir.rglob("*") if p.is_file()):
            info = zipfile.ZipInfo(
                f"{skill_dir.name}/{path.relative_to(skill_dir).as_posix()}",
                date_time=(1980, 1, 1, 0, 0, 0),
            )
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
    return buffer.getvalue()


def _bundled(names: Sequence[str]) -> list[Path]:
    from okg.substrate.pact.agent_install import bundled_skills

    wanted = set(names)
    return [skill for skill in bundled_skills() if skill.name in wanted]


def _desktop_skills(writes: bool, *, dry_run: bool) -> dict[str, Any]:
    zip_dir = _desktop_zip_dir()
    written: list[str] = []
    changed = False
    for skill in _bundled(_skill_names(writes)):
        target = zip_dir / f"{skill.name}.zip"
        written.append(str(target))
        if dry_run:
            continue
        payload = _skill_zip_bytes(skill)
        if target.is_file() and target.read_bytes() == payload:
            continue
        zip_dir.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".zip.tmp")
        tmp.write_bytes(payload)
        os.replace(tmp, target)
        changed = True
    return {
        "status": "upload needed",
        "zip_dir": str(zip_dir),
        "zips": written,
        "changed": changed,
        "upload_step": DESKTOP_UPLOAD_STEP + f" from {zip_dir}",
    }


def _folder_skills(
    root: Path, writes: bool, *, dry_run: bool,
) -> dict[str, Any]:
    from okg.substrate.pact.agent_install import sync_agent_skills

    names = _skill_names(writes)
    if dry_run:
        return {"root": str(root), "would_install": list(names)}
    result = sync_agent_skills(
        root.parent, skills_root=root, only=names, prune=False,
    )
    return {
        "root": str(root),
        "manifest": result["manifest"],
        "added": list(result["added"]),
        "updated": list(result["updated"]),
        "unchanged": list(result["unchanged"]),
        "clashed": list(result["clashed"]),
    }


# --------------------------------------------------------------- connect


def _selected(agents: Optional[Sequence[str]]) -> tuple[list[str], bool]:
    if not agents:
        return list(CONNECT_AGENTS), False
    out: list[str] = []
    for agent in agents:
        if agent not in CONNECT_AGENTS:
            raise _error(
                "agent_connect_unknown_agent",
                f"unknown agent {agent!r}",
                fix_hint="choose from " + ", ".join(CONNECT_AGENTS),
            )
        if agent not in out:
            out.append(agent)
    return out, True


def _git_ignore_warning(deployment_dir: Path) -> Optional[str]:
    git = shutil.which("git")
    if git is None:
        return None
    probe = deployment_dir / ".okg" / "agent-runtime" / "run"
    try:
        done = subprocess.run(
            [git, "-C", str(deployment_dir), "check-ignore", "-q", str(probe)],
            capture_output=True, text=True, timeout=30, stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if done.returncode == 1:  # inside a repository, and not ignored
        return (
            f"each agent session writes run state under {deployment_dir}/.okg/"
            "agent-runtime/, which this deployment's Git repository does not "
            "ignore; add `.okg/` to its .gitignore so a publish does not "
            "refuse those files as uncommitted"
        )
    return None


#: The refusal when the manifest's database URL names a variable that only
#: connect's own shell has (issue #3583).
DSN_VARIABLE_UNSET_CODE = "agent_connect_dsn_variable_unset"


def _default_local_env(deployment_dir: Path) -> Path:
    """Where a deployment's own local env file goes when it has none yet."""
    return deployment_dir / ".okg" / "local.env"


def _set_in_shell(name: str) -> bool:
    """True when ``name`` came from the caller's shell, not a profile file.

    The CLI loads the working directory's local env (or ``OKG_LOCAL_ENV``)
    into ``os.environ`` before connect runs; a value read from that file is
    not one the user can copy with ``$NAME``.
    """
    value = os.environ.get(name)
    if not value:
        return False
    from okg.substrate.local_env import configured_local_env_path, parse_local_env

    path = configured_local_env_path()
    if not path.is_file():
        return True
    try:
        loaded = parse_local_env(path)
    except (OSError, ValueError):
        # An unreadable or malformed profile was not loaded into the
        # environment either, so the value came from the shell. The printed
        # command still stops (${NAME:?}) if the shell lacks it.
        return True
    return loaded.get(name) != value


def _dsn_variable_unset_error(
    paths: DeploymentPaths, agent: str, result: ProbeResult,
) -> SubstrateError:
    """Say where the database URL has to live for an agent's server to see it.

    An agent starts the server without the user's shell, and the launcher
    drops every ambient OKG_* variable, so a URL the manifest reads from
    ``${OKG_DSN}`` must be in the deployment's local env file. The generic
    "OKG_DSN is not set" left users who had exported it with no way forward.
    """
    names = paths.dsn_missing
    target = paths.local_env or _default_local_env(paths.deployment_dir)
    in_shell = [name for name in names if _set_in_shell(name)]
    listed = ", ".join(names)
    message = (
        f"okg agent connect: the server this command would register for "
        f"{AGENT_LABELS[agent]} cannot find the database URL. "
        f"deployment.yaml reads it from {listed}, and an agent starts the "
        "server without your shell's environment, so a variable exported in "
        "this shell never reaches it"
        + (f" ({', '.join(in_shell)} is set here)" if in_shell else "")
        + f". It has to be in {target}. No agent config was written."
    )
    retry = "then run the same `okg agent connect` command again"
    if in_shell:
        # printf is a shell builtin, so the URL never appears on a command
        # line. The leading newline keeps a last line without one intact;
        # ${VAR:?} stops instead of writing an empty value; chmod also
        # covers a file that already existed with a wider mode.
        quoted = shlex.quote(str(target))
        lines = " && ".join(
            f"printf '\\n%s\\n' \"{name}=${{{name}:?{name} is not set in "
            f"this shell}}\" >> {quoted}"
            for name in in_shell
        )
        fix_hint = (
            f"in this shell, run `mkdir -p {shlex.quote(str(target.parent))} "
            f"&& touch {quoted} && chmod 600 {quoted} && {lines}` to copy "
            f"{', '.join(in_shell)} into that file, readable only by you; "
            f"{retry}"
        )
    else:
        fix_hint = (
            f"add a line {names[0]}=<database URL> to {target} (create it "
            f"with mode 600); {retry}"
        )
    return _error(
        DSN_VARIABLE_UNSET_CODE,
        message,
        fix_hint=fix_hint,
        data={
            "deployment": paths.name,
            "agent": agent,
            "missing": list(names),
            "set_in_this_shell": in_shell,
            "local_env": str(target),
            "server_code": result.code,
            "stderr_tail": redact_text(result.stderr_tail),
        },
    )


def run_connect(
    deployment: str,
    *,
    agents: Optional[Sequence[str]] = None,
    writes: bool = False,
    dry_run: bool = False,
    replace: bool = False,
    probe_timeout: float = 120.0,
    deployment_dir: Optional[Path] = None,
) -> dict[str, Any]:
    """Register ``deployment`` with each agent; return the report.

    Raises :class:`SubstrateError` for a refusal of the whole command (unknown
    deployment, a server that does not start, a uv-cache interpreter, writes
    on built-in role passwords). Per-agent outcomes are in the report.
    """
    from okg.substrate.local_database_roles import rotate_command

    selected, named = _selected(agents)
    paths = resolve_deployment_paths(deployment, deployment_dir=deployment_dir)
    builtin = builtin_role_password_state(paths, "okg_mcp")
    fix_passwords = rotate_command(deployment)
    if writes:
        builtin_roles = [
            role for role, state in (
                ("okg_mcp", builtin),
                ("app_rw", builtin_role_password_state(paths, "app_rw")),
            ) if state
        ]
        if builtin_roles:
            raise _error(
                "agent_connect_writes_need_role_passwords",
                "okg agent connect: the write-enabled server would log in with "
                "the public built-in password of "
                + " and ".join(builtin_roles),
                fix_hint=(
                    f"run `{fix_passwords}`, then re-run this command with --writes"
                ),
                data={"deployment": deployment, "roles": builtin_roles},
            )
    key = f"okg-{deployment}"
    plans: list[AgentPlan] = []
    for agent in selected:
        writer = _writer(agent)
        present = agent_presence(agent)
        if present is None:
            plans.append(AgentPlan(agent=agent, config_path=writer.config_path()))
            continue
        try:
            entry = build_entry(agent, paths, writes=writes, allow_builtin=builtin)
        except EphemeralOkgInterpreterError as exc:
            raise _error(
                "agent_connect_ephemeral_okg",
                f"okg agent connect: {exc}",
                fix_hint=EPHEMERAL_OKG_FIX_HINT,
                data={"interpreter": exc.interpreter, "cache_root": str(exc.cache_root)},
            ) from exc
        plan = _plan_agent(agent, key, deployment, entry, replace=replace)
        plan.present = present
        plans.append(plan)

    warnings: list[str] = []
    for plan in plans:
        if plan.agent == "claude-desktop" and plan.status in ("pending", "unchanged"):
            reason = _desktop_blocked(deployment, "connect", warnings)
            if reason:
                plan.status, plan.reason, plan.desktop_running = "refused", reason, True
    probe: dict[str, Any] = {"status": "not run"}
    runnable = [p for p in plans if p.status in ("pending", "unchanged")]
    if dry_run:
        probe = {"status": "skipped (dry run)"}
    elif runnable:
        # Every distinct entry is started once, exactly as written (they
        # differ per agent in --runtime and the subject).
        seen: dict[str, ProbeResult] = {}
        per_agent: dict[str, float] = {}
        tools: tuple[str, ...] = ()
        for plan in runnable:
            entry = dict(plan.entry or {})
            entry.pop("startup_timeout_sec", None)
            fingerprint = json.dumps(_without_type(entry), sort_keys=True)
            result = seen.get(fingerprint)
            if result is None:
                result = probe_entry(entry, timeout=probe_timeout)
                seen[fingerprint] = result
            if not result.ok:
                if any(
                    name in (result.message or "") for name in paths.dsn_missing
                ):
                    raise _dsn_variable_unset_error(paths, plan.agent, result)
                raise _error(
                    result.code or "agent_connect_server_failed",
                    "okg agent connect: the server this command would register "
                    f"for {AGENT_LABELS[plan.agent]} did not start: "
                    f"{result.message}. No agent config was written.",
                    fix_hint=result.fix_hint,
                    data={
                        "deployment": deployment,
                        "agent": plan.agent,
                        "stderr_tail": redact_text(result.stderr_tail),
                        "tools": list(result.tools),
                    },
                )
            tools = result.tools
            if result.seconds is not None:
                per_agent[plan.agent] = round(result.seconds, 1)
        probe = {
            "status": "passed",
            "tools": list(tools),
            "start_seconds": per_agent,
            "entries_started": len(seen),
        }
        for plan in runnable:
            started = per_agent.get(plan.agent)
            if plan.agent == "codex":
                assert plan.entry is not None
                plan.entry["startup_timeout_sec"] = codex_startup_timeout(started)
                replanned = _plan_agent(
                    plan.agent, key, deployment, plan.entry, replace=replace,
                )
                replanned.present, replanned.notes = plan.present, plan.notes
                plans[plans.index(plan)] = replanned
            elif started is not None and started > SLOW_START_WARNING_SEC:
                warnings.append(
                    f"the server took {started:.0f}s to start; "
                    f"{AGENT_LABELS[plan.agent]} does not document how long it "
                    "waits for an MCP server, so a slower start may make it "
                    "give up. Start a session and check that the okg tools "
                    "appear"
                )

    if builtin:
        warnings.append(
            f"deployment {deployment!r} still uses the public built-in role "
            "passwords, so the server is connected read-only with them; anyone "
            f"who can reach the database can log in as its roles. Run "
            f"`{fix_passwords}`, then re-run `okg agent connect --deployment "
            f"{deployment}`"
        )
    if paths.local_env_note:
        warnings.append(paths.local_env_note)
    warnings.extend(paths.check_notes)
    if paths.dsn is None and paths.dsn_note:
        warnings.append(
            "the database URL could not be resolved here "
            f"({paths.dsn_note}); no role-password file was looked up"
        )
    ignore_warning = _git_ignore_warning(paths.deployment_dir)
    if ignore_warning:
        warnings.append(ignore_warning)

    synced: dict[str, str] = {}
    for plan in plans:
        if plan.status == "not found" or plan.status == "refused":
            continue
        if dry_run:
            plan.status = "unchanged" if plan.status == "unchanged" else "would connect"
        elif plan.status == "pending":
            if plan.agent == "claude-desktop" and desktop_running():
                # Started while the server check ran.
                plan.status = "refused"
                plan.reason = _desktop_running_reason(deployment, "connect")
                plan.desktop_running = True
            else:
                _apply_agent(plan, key, warnings)
        if plan.status == "refused":
            continue
        writer = _writer(plan.agent)
        root = writer.skills_root()
        if root is None:
            plan.skills = _desktop_skills(writes, dry_run=dry_run)
            if plan.skills.get("changed") and plan.status == "unchanged":
                plan.status = "connected"
            continue
        root_key = str(root)
        if root_key in synced:
            plan.skills = {"root": root_key, "shared_with": synced[root_key]}
        else:
            plan.skills = _folder_skills(root, writes, dry_run=dry_run)
            synced[root_key] = plan.agent
            if plan.skills.get("added") or plan.skills.get("updated"):
                if plan.status == "unchanged":
                    plan.status = "connected"
            if plan.skills.get("clashed"):
                plan.notes.append(
                    "skills not installed because a folder with the same name "
                    "that okg did not install is already there: "
                    + ", ".join(plan.skills["clashed"])
                )
    by_agent = {plan.agent: plan for plan in plans}
    cursor = by_agent.get("cursor")
    claude_code = by_agent.get("claude-code")
    claude_skills = Path.home() / ".claude" / "skills" / ".okg-skill-sync.json"
    if cursor is not None and cursor.status not in ("not found", "refused") and (
        (claude_code is not None and claude_code.status not in ("not found", "refused"))
        or claude_skills.is_file()
    ):
        cursor.notes.append(
            "Cursor reads both ~/.agents/skills and ~/.claude/skills, so with "
            "Claude Code connected too it lists each okg skill twice"
        )

    rows = []
    for plan in plans:
        row = plan.to_json()
        row["server_key"] = key
        rows.append(row)
    exit_code = _exit_code(plans, named)
    return {
        "schema": SCHEMA,
        "action": "connect",
        "ok": exit_code == 0,
        "exit_code": exit_code,
        "deployment": deployment,
        "profile": "deploy" if writes else "read-only",
        "dry_run": dry_run,
        "deployment_dir": str(paths.deployment_dir),
        "deployments_dir": str(paths.deployments_dir),
        "local_env": str(paths.local_env) if paths.local_env else None,
        "role_secrets_file": (
            str(paths.role_secrets_file) if paths.role_secrets_file else None
        ),
        "server_check": probe,
        "agents": rows,
        "warnings": warnings,
        "next_steps": _next_steps(plans, deployment, dry_run=dry_run),
    }


def _exit_code(plans: list[AgentPlan], named: bool) -> int:
    """1 for a refusal or a named agent not found, else 0.

    A Claude Desktop refused only because the app is running does not fail a
    run without --agent when another agent succeeded.
    """
    if named and any(plan.status == "not found" for plan in plans):
        return 1
    hard = [p for p in plans if p.status == "refused" and not p.desktop_running]
    if hard:
        return 1
    soft = [p for p in plans if p.status == "refused" and p.desktop_running]
    if soft:
        if named:
            return 1
        done = ("connected", "unchanged", "disconnected", "not connected")
        if not any(p.status in done for p in plans):
            return 1
    return 0


def _next_steps(plans: list[AgentPlan], deployment: str, *, dry_run: bool) -> list[str]:
    if dry_run:
        return [f"run `okg agent connect --deployment {deployment}` without --dry-run"]
    steps: list[str] = []
    done = [p for p in plans if p.status in ("connected", "unchanged")]
    sessions = [
        AGENT_LABELS[p.agent] for p in done if p.agent != "claude-desktop"
    ]
    if sessions:
        steps.append(
            "start a new session in " + ", ".join(sessions)
            + f" to load the okg-{deployment} server and the okg skills"
        )
    desktop = next((p for p in done if p.agent == "claude-desktop"), None)
    if desktop is not None:
        steps.append("quit Claude Desktop completely and restart it to load the server")
        if desktop.skills and desktop.skills.get("upload_step"):
            steps.append(desktop.skills["upload_step"])
    for plan in plans:
        if plan.status == "refused":
            steps.append(f"{AGENT_LABELS[plan.agent]}: {plan.reason}")
    if not steps:
        steps.append(
            "no agent was connected; install one, or name it with --agent"
        )
    return steps


# ------------------------------------------------ connect after an install


#: Under pytest, an install must not edit the real user's agent configs; a
#: test that wants the install's connect step opts in with this variable.
ENABLE_IN_TESTS_ENV = "OKG_ENABLE_AGENT_CONNECT_FOR_TESTS"


def _deployments_dir_prefix(deployments_root: Optional[Path]) -> str:
    return (
        f"OKG_DEPLOYMENTS_DIR={shlex.quote(str(deployments_root))} "
        if deployments_root is not None else ""
    )


def connect_command(deployment: str, agent: str, *, deployments_root: Optional[Path] = None) -> str:
    from okg.substrate.cli.command_reference import okg_command
    from okg.substrate.support.user_hints import hint_command

    return _deployments_dir_prefix(deployments_root) + hint_command(
        okg_command("agent connect", deployment=deployment, agent=agent)
    )


#: Refusals that mean okg's MCP login roles have no password yet. Right after
#: a first install that is the normal state (migration creates the roles
#: without a login), so the install reports connecting as a next step, not
#: as a failure (issue #3500).
ROLE_PASSWORD_PENDING_CODES = frozenset({
    "mcp_role_cannot_login",
    "role_password_not_provisioned",
})

#: ``status`` of the install's connect step while the roles wait for passwords.
NEEDS_ROLE_PASSWORDS = "needs_role_passwords"


def _role_password_next_steps(
    deployment: str, *, deployments_root: Optional[Path] = None,
) -> list[str]:
    """The commands that let the install's connect step go through."""
    from okg.substrate.cli.command_reference import okg_command
    from okg.substrate.support.user_hints import hint_command

    prefix = _deployments_dir_prefix(deployments_root)
    return [
        prefix + hint_command(okg_command(
            "db role-passwords", apply=True, deployment=deployment,
        )),
        prefix + hint_command(okg_command("agent connect", deployment=deployment)),
    ]


def connect_after_install(
    deployment: str,
    *,
    deployment_dir: Path,
    skip_reason: Optional[str] = None,
    deployments_root: Optional[Path] = None,
) -> dict[str, Any]:
    """The step `okg install` runs once the deployment is published.

    Connects, read-only, every agent connect counts as present, and returns
    the command for every agent it did not connect (absent, refused, or a
    running Claude Desktop). It never raises: a refusal or a failed server
    check is reported with its fix, and the install itself still succeeds.
    """
    def cmd(agent: str) -> str:
        return connect_command(deployment, agent, deployments_root=deployments_root)

    out: dict[str, Any] = {
        "status": "skipped",
        "connected": [],
        "not_connected": [],
        "report": None,
        "error": None,
    }
    if skip_reason is None and os.environ.get("PYTEST_CURRENT_TEST") and (
        os.environ.get(ENABLE_IN_TESTS_ENV) != "1"
    ):
        skip_reason = (
            f"running under pytest without {ENABLE_IN_TESTS_ENV}=1; the "
            "real user's agent configs are left alone"
        )
    if skip_reason is not None:
        out["skip_reason"] = skip_reason
        out["not_connected"] = [
            {"agent": a, "reason": skip_reason, "command": cmd(a)}
            for a in CONNECT_AGENTS
        ]
        out["message"] = _install_message(deployment, out)
        return out
    present = [agent for agent in CONNECT_AGENTS if agent_presence(agent)]
    absent = [agent for agent in CONNECT_AGENTS if agent not in present]
    out["not_connected"] = [
        {"agent": a, "reason": "not found on this machine", "command": cmd(a)}
        for a in absent
    ]
    if not present:
        out["status"] = "none"
        out["message"] = _install_message(deployment, out)
        return out
    try:
        report = run_connect(
            deployment, agents=present, writes=False, deployment_dir=deployment_dir,
        )
    except SubstrateError as exc:
        if exc.code in ROLE_PASSWORD_PENDING_CODES:
            # Not a failure: nothing was written, and connecting is the step
            # after `okg db role-passwords --apply`.
            out["status"] = NEEDS_ROLE_PASSWORDS
            out["pending"] = {
                "code": exc.code,
                "reason": redact_text(exc.message),
            }
            out["next_commands"] = _role_password_next_steps(
                deployment, deployments_root=deployments_root,
            )
            out["not_connected"] = [
                {
                    "agent": a,
                    "reason": (
                        "waits for the role passwords" if a in present
                        else "not found on this machine"
                    ),
                    "command": cmd(a),
                }
                for a in CONNECT_AGENTS
            ]
            out["message"] = _install_message(deployment, out)
            return out
        out["status"] = "failed"
        out["error"] = {
            "code": exc.code,
            "message": redact_text(exc.message),
            "fix_hint": exc.fix_hint,
        }
        out["not_connected"] = [
            {
                "agent": a,
                "reason": (
                    f"not connected: {exc.code}" if a in present
                    else "not found on this machine"
                ),
                "command": cmd(a),
            }
            for a in CONNECT_AGENTS
        ]
        out["message"] = _install_message(deployment, out)
        return out
    out["report"] = report
    for row in report["agents"]:
        if row["status"] in ("connected", "unchanged"):
            out["connected"].append(row["agent"])
        else:
            out["not_connected"].append({
                "agent": row["agent"],
                "reason": row.get("reason") or row["status"],
                "command": cmd(row["agent"]),
            })
    order = {agent: index for index, agent in enumerate(CONNECT_AGENTS)}
    out["not_connected"].sort(key=lambda item: order[item["agent"]])
    out["status"] = "connected" if out["connected"] else "none"
    out["message"] = _install_message(deployment, out)
    return out


def _install_message(deployment: str, step: dict[str, Any]) -> str:
    """Name only the agents actually connected; otherwise the commands."""
    lines: list[str] = []
    connected = [AGENT_LABELS[a] for a in step.get("connected") or []]
    if connected:
        lines.append(
            "Connected " + ", ".join(connected) + f" to {deployment}. Start "
            "a new session there (Claude Desktop: quit and reopen it) and say: "
            f"\"set up my OKG for {deployment}\""
        )
    else:
        lines.append(f"No agent is connected to {deployment} yet.")
    if step.get("status") == NEEDS_ROLE_PASSWORDS:
        from okg.substrate.local_database_roles import REMOTE_ROLES_DOC
        from okg.substrate.support.user_hints import doc_reference

        role_step, connect_step = step["next_commands"]
        lines.append(
            "Connecting an agent waits for the database roles the MCP server "
            "logs in as to get passwords. Next, run "
            f"`{role_step}`, then `{connect_step}`. On a database on another "
            "host, set the roles up by hand instead: "
            f"{doc_reference(REMOTE_ROLES_DOC)}, step 4, 'A database on "
            "another host'."
        )
    error = step.get("error")
    if error:
        lines.append(
            f"Agent connection failed ({error['code']}): {error['message']}"
            + (f" Fix: {error['fix_hint']}" if error.get("fix_hint") else "")
        )
    for item in step.get("not_connected") or []:
        lines.append(
            f"{AGENT_LABELS[item['agent']]} is not connected "
            f"({item['reason']}). To connect it: {item['command']}"
        )
    return "\n".join(lines)


def install_connect_plan(
    deployment: str, *, skip: bool, deployments_root: Optional[Path] = None,
) -> dict[str, Any]:
    """What the install's connect step would do (for --dry-run); reads only."""
    present = [] if skip else [a for a in CONNECT_AGENTS if agent_presence(a)]
    return {
        "would_connect": present,
        "would_print": [
            connect_command(deployment, a, deployments_root=deployments_root)
            for a in CONNECT_AGENTS if a not in present
        ],
        "skipped": skip,
    }


# ------------------------------------------------------------ disconnect


def run_disconnect(
    deployment: str,
    *,
    agents: Optional[Sequence[str]] = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Remove the deployment's entry from each agent, and connect's skills."""
    from okg.substrate.pact.agent_install import (
        managed_skill_names,
        remove_managed_skills,
    )

    selected, named = _selected(agents)
    key = f"okg-{deployment}"
    plans: list[AgentPlan] = []
    # Entries this run removes (or, in a dry run, would remove).
    removing: dict[str, str] = {}
    warnings: list[str] = []
    for agent in selected:
        writer = _writer(agent)
        cfg = writer.config_path()
        plan = AgentPlan(agent=agent, config_path=cfg)
        plan.present = agent_presence(agent)
        plans.append(plan)
        if plan.present is None:
            continue
        if agent == "claude-code" and shutil.which("claude"):
            plan.method = "claude mcp remove"
        if agent == "claude-desktop" and not dry_run:
            reason = _desktop_blocked(deployment, "disconnect", warnings)
            if reason:
                plan.status, plan.reason, plan.desktop_running = "refused", reason, True
                continue
        try:
            data, text = _load(writer, cfg)
            servers = _servers(writer, data, cfg)
        except (AgentConfigParseError, OSError) as exc:
            plan.status = "refused"
            plan.reason = f"{exc}; the file was not changed"
            continue
        if key not in servers:
            plan.status = "not connected"
            continue
        if not _is_okg_connect_entry(key, servers[key]):
            plan.status = "refused"
            plan.reason = (
                f"{cfg} has an entry {key!r} that `okg agent connect` did not "
                "write (it does not start agent-runtime-launch); it was left "
                "alone. Remove it by hand if it is no longer wanted"
            )
            continue
        removing[agent] = key
        if dry_run:
            plan.status = "would disconnect"
            continue
        _note_backup(plan, cfg)
        if plan.method == "claude mcp remove":
            _run_claude(["mcp", "remove", "--scope", "user", key], plan)
            if plan.status == "refused":
                continue
        else:
            del servers[key]
            atomic_write_text(cfg, writer.render_config(data))
        plan.status = "disconnected"

    # Skills: a folder is cleaned only when no agent that reads it still has
    # an okg registration (for this or another deployment).
    for plan in plans:
        if plan.status in ("not found", "refused"):
            continue
        writer = _writer(plan.agent)
        root = writer.skills_root()
        if root is None:
            zip_dir = _desktop_zip_dir()
            zips = [
                zip_dir / f"{name}.zip" for name in USER_SKILLS + WRITE_SKILLS
                if (zip_dir / f"{name}.zip").is_file()
            ]
            remaining = _still_registered("claude-desktop", removing)
            if remaining:
                plan.skills = {
                    "zip_dir": str(zip_dir),
                    "kept_for": ["claude-desktop"],
                    "kept_entries": {"claude-desktop": remaining},
                }
                continue
            plan.skills = {
                "zip_dir": str(zip_dir),
                ("would_remove" if dry_run else "removed"): [str(p) for p in zips],
                "upload_step": "remove the okg skills in Claude Desktop "
                "Settings > Capabilities > Skills",
            }
            if not dry_run:
                for path in zips:
                    path.unlink()
            continue
        kept_entries = {
            other: entries
            for other in CONNECT_AGENTS
            if _writer(other).skills_root() == root
            and (entries := _still_registered(other, removing))
        }
        if kept_entries:
            plan.skills = {
                "root": str(root),
                "kept_for": sorted(kept_entries),
                "kept_entries": kept_entries,
            }
            continue
        if dry_run:
            plan.skills = {"root": str(root), "would_remove": managed_skill_names(root)}
            continue
        result = remove_managed_skills(root)
        plan.skills = {
            "root": str(root),
            "removed": list(result["removed"]),
            "modified_kept": list(result["modified"]),
        }

    rows = []
    for plan in plans:
        row = plan.to_json()
        row["server_key"] = key
        rows.append(row)
    exit_code = _exit_code(plans, named)
    steps = ["start a new session in each agent (Claude Desktop: quit and restart)"]
    return {
        "schema": SCHEMA,
        "action": "disconnect",
        "ok": exit_code == 0,
        "exit_code": exit_code,
        "deployment": deployment,
        "dry_run": dry_run,
        "agents": rows,
        "warnings": warnings,
        "next_steps": steps,
    }


def _still_registered(
    agent: str, removing: Optional[dict[str, str]] = None,
) -> list[str]:
    """The connect-written entries left in ``agent``'s config (empty if none).

    ``removing`` names the entry this disconnect removes (or would remove in a
    dry run) for each agent; it does not count.
    """
    ignored = (removing or {}).get(agent)
    writer = _writer(agent)
    cfg = writer.config_path()
    if cfg is None or not cfg.is_file():
        return []
    try:
        data, _text = _load(writer, cfg)
        servers = _servers(writer, data, cfg)
    except (AgentConfigParseError, OSError):
        return ["<config unreadable>"]  # cannot tell: keep the skills
    return sorted(
        k for k, v in servers.items() if k != ignored and _is_okg_connect_entry(k, v)
    )


# ------------------------------------------------------------------ text


def render_text(report: dict[str, Any]) -> str:
    lines: list[str] = []
    action = report["action"]
    head = f"okg agent {action}: deployment {report['deployment']}"
    if action == "connect":
        head += f" ({report['profile']})"
        if report["dry_run"]:
            head += " -- dry run, nothing written"
    lines.append(head)
    if action == "connect":
        check = report["server_check"]
        if check.get("status") == "passed":
            lines.append(
                f"  server check: started from / and listed {len(check['tools'])} "
                "tools"
            )
        else:
            lines.append(f"  server check: {check.get('status')}")
    for row in report["agents"]:
        label = AGENT_LABELS[row["agent"]]
        where = row["config_path"] or "-"
        lines.append(f"  {label:<15} {row['status']:<16} {where}")
        if row.get("method") and row["method"] != "file" and row["status"] != "not found":
            lines.append(f"  {'':<15} via `{row['method']}`")
        if row.get("reason"):
            lines.append(f"  {'':<15} {row['reason']}")
        if report["dry_run"] and row.get("entry"):
            lines.append(f"  {'':<15} entry {row['server_key']}:")
            for text in json.dumps(row["entry"], indent=2).splitlines():
                lines.append(f"  {'':<17}{text}")
        skills = row.get("skills") or {}
        if skills.get("root"):
            detail = ""
            if skills.get("shared_with"):
                detail = f" (already written for {AGENT_LABELS[skills['shared_with']]})"
            elif skills.get("would_install"):
                detail = " would install " + ", ".join(skills["would_install"])
            elif skills.get("kept_for"):
                entries = skills.get("kept_entries") or {}
                detail = " kept: still used by " + ", ".join(
                    f"{AGENT_LABELS.get(a, a)} ({', '.join(entries.get(a, []))})"
                    for a in skills["kept_for"]
                )
            lines.append(f"  {'':<15} skills: {skills['root']}{detail}")
        elif skills.get("zip_dir"):
            lines.append(f"  {'':<15} skill ZIPs: {skills['zip_dir']}")
        for note in row.get("notes") or []:
            lines.append(f"  {'':<15} note: {note}")
    for warning in report.get("warnings") or []:
        lines.append(f"warning: {warning}")
    for step in report.get("next_steps") or []:
        lines.append(f"next: {step}")
    return "\n".join(lines)
