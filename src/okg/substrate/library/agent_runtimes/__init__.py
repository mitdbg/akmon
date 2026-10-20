"""Agent-runtime registry: register an OKG deployment with every
MCP-capable agent runtime detected on the host.

Per `dead-simple-install`: `okg install` no longer hardcodes
Claude Desktop. Each known runtime is an `AgentRuntime` Protocol
implementation living in this module; install iterates the
registry and registers with every runtime whose config file is
present. Adding a new runtime is one new file + one entry in
`registry.KNOWN_RUNTIMES`.

Public surface:

  - `AgentRuntime`           — the Protocol every runtime
                               implementation satisfies.
  - `RegisterResult`         — per-runtime outcome record.
  - `RegisterReport`         — aggregate of one install's
                               results.
  - `register_with_all_detected` — main entry point used by
                                    `okg install`.

Concrete implementations:

  - `ClaudeDesktopRuntime`   — `~/Library/Application Support/Claude/`
                               (macOS), XDG (Linux), `%APPDATA%`
                               (Windows). JSON, `mcpServers`.
  - `CodexCliRuntime`        — `~/.codex/config.toml`. TOML,
                               `[mcp_servers.<name>]` blocks.
  - `CursorRuntime`          — `~/.cursor/mcp.json`. JSON, same
                               shape as Claude Desktop.

All runtimes guarantee idempotent upsert (re-running install
replaces an existing entry by deployment name; no duplicates),
atomic write (write-to-temp, rename) so a concurrent reader
never observes a partial config, and profile-installed launch
through ``agent-runtime-launch`` without raw DSNs in MCP env blocks.
"""
from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Literal, Mapping, Optional, Protocol

from okg.substrate.cli.child_argv import running_okg_argv


# ---------- shared helpers ----------


def resolve_okg_binary() -> tuple[str, list[str]]:
    """Return `(command, prefix_args)` for invoking THIS okg.

    MCP runtimes spawn their server subprocess with a minimal
    PATH (`/usr/bin:/bin:...`) that doesn't include the
    operator's venv, so the command must be an absolute path.

    Issue #2301: the command is the interpreter running this okg
    with `-P -m okg.substrate.cli.main`, never `shutil.which("okg")`.
    The first `okg` on PATH can be a different install (a developer
    checkout) with different code, and the runtime starts the server
    in the operator's project folder, where a bare `-m` would let a
    local `mcp/`, `okg/` or `types.py` shadow the real modules.

    The command is saved in the runtime's config and runs later, so
    it must outlive this process. An interpreter inside uv's cache (a
    `uvx`, `uv run --with` or `uv run --isolated` environment) does not:
    `uv cache prune` or `uv cache clean` can delete it, and the saved
    config then fails with no warning. That case raises
    :class:`EphemeralOkgInterpreterError` instead of writing the entry.
    """
    argv = running_okg_argv()
    cache_root = uv_cache_root()
    interpreter_dir = Path(os.path.realpath(Path(argv[0]).absolute().parent))
    if cache_root is not None and interpreter_dir.is_relative_to(cache_root):
        raise EphemeralOkgInterpreterError(
            interpreter=argv[0], cache_root=cache_root,
        )
    return argv[0], argv[1:]


EPHEMERAL_OKG_FIX_HINT = (
    "install okg as a lasting tool first (`uv tool install <okg wheel "
    "or source>` or `pipx install <okg wheel or source>`), then re-run "
    "this command with that install's `okg`"
)


class EphemeralOkgInterpreterError(RuntimeError):
    """The running okg lives in uv's cache and cannot be saved as a command.

    Issue #2301 follow-up: an agent-runtime MCP entry names the running
    interpreter; one inside uv's cache can be deleted by any cache prune.
    """

    fix_hint = EPHEMERAL_OKG_FIX_HINT

    def __init__(self, *, interpreter: str, cache_root: Path) -> None:
        self.interpreter = interpreter
        self.cache_root = cache_root
        super().__init__(
            f"refusing to save {interpreter} as the agent runtime's okg "
            f"command: it is inside uv's cache ({cache_root}), which "
            "`uv cache prune` or `uv cache clean` can delete; "
            + EPHEMERAL_OKG_FIX_HINT
        )


def uv_cache_root(
    environ: Optional[Mapping[str, str]] = None,
) -> Optional[Path]:
    """uv's cache directory, from uv's documented lookup order.

    ``UV_CACHE_DIR`` first; otherwise ``%LOCALAPPDATA%\\uv\\cache`` on
    Windows, and ``$XDG_CACHE_HOME/uv`` or ``~/.cache/uv`` elsewhere (uv
    uses the XDG layout on macOS too). A ``--cache-dir`` flag or a
    ``cache-dir`` setting in a uv config file is not visible from here.
    Returns the real path, or None when no location can be named.
    """
    env = os.environ if environ is None else environ
    explicit = (env.get("UV_CACHE_DIR") or "").strip()
    if explicit:
        root = Path(explicit).expanduser()
    elif os.name == "nt":
        local = (env.get("LOCALAPPDATA") or "").strip()
        if not local:
            return None
        root = Path(local) / "uv" / "cache"
    else:
        xdg = (env.get("XDG_CACHE_HOME") or "").strip()
        root = (Path(xdg) if xdg else Path.home() / ".cache") / "uv"
    return Path(os.path.realpath(root.absolute()))


def atomic_write_text(path: Path, content: str, *, encoding: str = "utf-8") -> None:
    """Write `content` to `path` atomically: write to a sibling
    temp file, fsync, then `os.replace` onto the target. A
    concurrent reader never observes a partial file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    # `delete=False` so we can replace; we clean up via os.replace.
    fd, tmp_path_str = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )
    tmp_path = Path(tmp_path_str)
    try:
        with os.fdopen(fd, "w", encoding=encoding) as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:  # noqa: BLE001 — atomic write/replace failed; remove the temp file before re-raising so callers see a clean filesystem.
        # Clean up the temp file if anything went wrong.
        try:
            tmp_path.unlink()
        except OSError:
            # Preserve the original write/replace exception; temp cleanup is best-effort.
            pass
        raise


#: okg's own backup name beside an agent config. A `.bak` (or any other
#: backup) okg did not write is never reused or relied on.
AGENT_CONFIG_BACKUP_SUFFIX = ".okg-backup"


def is_connect_entry(key: str, entry: object) -> bool:
    """True for an MCP entry `okg agent connect` wrote.

    Its launcher arguments always carry `--deployments-dir`. The academic
    installer's entries also start `agent-runtime-launch` but never carry that
    flag, so they are not connect's (laptop hand run, 2026-10-06).
    """
    if not key.startswith("okg-") or not isinstance(entry, dict):
        return False
    args = [str(a) for a in entry.get("args") or []]
    if "agent-runtime-launch" not in args:
        return False
    launcher_args = args[: args.index("--")] if "--" in args else args
    return "--deployments-dir" in launcher_args


def backup_agent_config(
    path: Path,
    *,
    load: Callable[[Path], dict],
    servers_key: str,
) -> Optional[Path]:
    """Save ``path`` as ``<path>.okg-backup`` before okg rewrites it.

    The backup always holds the latest version of the file that has no
    `okg agent connect` entry (for any deployment): it is refreshed only from
    such a state, so a connect followed by a disconnect leaves the bytes from
    before the connect, never a version that still held okg's entry. A file
    that does not parse is not backed up here (okg refuses to rewrite it).
    Returns the backup path when one was written now, else None. The copy is
    written atomically with mode 0600: agent configs can hold other servers'
    tokens. A `.bak` or other backup okg did not write is never used.
    """
    if not path.is_file():
        return None
    current = path.read_text(encoding="utf-8")
    if not current.strip():
        return None
    try:
        data = load(path)
    except (AgentConfigParseError, OSError):
        return None
    servers = data.get(servers_key) if isinstance(data, dict) else None
    if isinstance(servers, dict) and any(
        is_connect_entry(str(k), v) for k, v in servers.items()
    ):
        return None
    backup = path.with_name(path.name + AGENT_CONFIG_BACKUP_SUFFIX)
    if backup.is_file() and backup.read_text(encoding="utf-8") == current:
        return None
    atomic_write_text(backup, current)
    return backup


class AgentConfigParseError(ValueError):
    """An agent config file exists but is not a parseable object.

    Such a file is never rewritten (PACT agent-connect-3412): rewriting it
    from an empty object would drop everything the user had in it.
    """

    def __init__(self, path: Path, detail: str) -> None:
        self.path = path
        self.detail = detail
        super().__init__(f"{path} could not be parsed: {detail}")


def load_json_config(path: Path) -> dict:
    """Read a JSON agent config; ``{}`` when absent or empty.

    A file that is not valid JSON, or whose top level is not an object,
    raises :class:`AgentConfigParseError` instead of reading as ``{}``.
    """
    import json

    if not path.is_file():
        return {}
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        return {}
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AgentConfigParseError(path, f"invalid JSON ({exc})") from exc
    if not isinstance(loaded, dict):
        raise AgentConfigParseError(
            path, f"the top level is a {type(loaded).__name__}, not an object",
        )
    return loaded


def render_json_config(data: dict) -> str:
    import json

    return json.dumps(data, indent=2) + "\n"


# ---------- result types ----------


RegistrationStatus = Literal[
    "registered",        # config file present, entry upserted
    "not_detected",      # config file absent — runtime not installed
    "failed",            # config file present but write failed
]


@dataclass
class RegisterResult:
    """Per-runtime outcome record. Returned by every
    `AgentRuntime.register()` call and aggregated into a
    `RegisterReport`.
    """
    runtime: str
    status: RegistrationStatus
    config_path: Optional[Path] = None
    error: Optional[str] = None
    # Modification time of the config file post-write (for the
    # recency-based primary-agent heuristic in
    # `dead-simple-install`'s handoff message).
    config_mtime: Optional[float] = None


@dataclass
class RegisterReport:
    """Aggregate of one install's per-runtime results."""
    results: list[RegisterResult] = field(default_factory=list)

    @property
    def registered(self) -> list[RegisterResult]:
        return [r for r in self.results if r.status == "registered"]

    @property
    def not_detected(self) -> list[RegisterResult]:
        return [r for r in self.results if r.status == "not_detected"]

    @property
    def failed(self) -> list[RegisterResult]:
        return [r for r in self.results if r.status == "failed"]

    def primary_agent(self) -> Optional[str]:
        """Most-recently-modified registered runtime — the
        operator's likely-primary agent for the handoff message.
        Returns None if no runtime was registered.
        """
        registered = self.registered
        if not registered:
            return None
        # Sort by config_mtime desc; ties broken alphabetically
        # for stability in tests.
        registered.sort(
            key=lambda r: (-(r.config_mtime or 0.0), r.runtime),
        )
        return registered[0].runtime

    def secondary_agents(self) -> list[str]:
        """All registered runtimes except the primary, in
        recency order. Empty list if 0 or 1 registered."""
        registered = self.registered
        if len(registered) <= 1:
            return []
        registered.sort(
            key=lambda r: (-(r.config_mtime or 0.0), r.runtime),
        )
        return [r.runtime for r in registered[1:]]


# ---------- Protocol ----------


class AgentRuntime(Protocol):
    """Contract every concrete runtime implementation satisfies.

    All implementations MUST guarantee:
      - Idempotent upsert: re-running `register()` for the same
        deployment replaces the existing entry by name (no
        duplicates).
      - Atomic write: a concurrent reader never observes a
        partial config (use `atomic_write_text`).
      - Honest status: `detect()` returns False ONLY when the
        runtime's canonical config file is absent. `register()`
        returns `failed` (not `not_detected`) when the file is
        present but the write itself failed.
    """

    name: str  # short identifier: "claude-desktop", "codex", etc.

    def detect(self) -> bool:
        """Return True iff this runtime's canonical config file
        exists on the host."""
        raise NotImplementedError

    def config_path(self) -> Optional[Path]:
        """Return the platform-appropriate config-file path for
        this runtime. May return None on unsupported platforms
        (e.g. Claude Desktop on Linux pre-2026)."""
        raise NotImplementedError

    def register(
        self,
        *,
        deployment: str,
        dsn: str,
        extra_env: dict | None = None,
    ) -> RegisterResult:
        """Upsert the MCP server entry for `deployment` into
        this runtime's config. Idempotent; atomic write.

        The entry's `command` SHALL be the result of
        `resolve_okg_binary()` (absolute path), the args SHALL
        call `agent-runtime-launch` before `mcp-serve`, and the
        env SHALL NOT store raw DSNs or token-like values. Non-secret
        capability flags in `extra_env` are translated by the
        launcher entry builder into profile/command shape.
        """
        raise NotImplementedError

    def unregister(self, deployment: str) -> bool:
        """Remove the MCP server entry for `deployment`, if
        present. Returns True if an entry was removed, False if
        none existed. Atomic write.
        """
        raise NotImplementedError
