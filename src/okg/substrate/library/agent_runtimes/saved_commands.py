"""Does each agent runtime's saved okg command still exist?

`okg agent connect` (and `okg install`) saves an absolute command into each
agent runtime's config: the interpreter that ran it, with `-P -m
okg.substrate.cli.main`. If that interpreter later disappears -- the
virtual environment was deleted or moved, okg was reinstalled with another
tool, or uv's cache was pruned -- the agent fails to start the okg server and
nothing in okg said why. Saving refuses an interpreter inside uv's cache;
this check finds a command that was valid when saved and broke later, or was
saved before that refusal existed.

Read-only: it parses the configs and stats the commands, and never starts
them. It reads each config scope on its own (Claude Code keeps user-scope
and per-project entries under the same name), judges only entries okg saved,
and skips a malformed entry that is not okg's rather than failing on it.
"""
from __future__ import annotations

import json
import os
import shutil
import tomllib
from pathlib import Path
from typing import Any, Iterable, Iterator

from . import EPHEMERAL_OKG_FIX_HINT, is_connect_entry, uv_cache_root

SCHEMA = "okg.agent_runtime_saved_commands.v1"
#: The module every okg-saved entry runs (`python -P -m okg.substrate.cli.main`).
OKG_CLI_MODULE = "okg.substrate.cli.main"


def saved_command_report(runtimes: Iterable[Any] | None = None) -> dict[str, Any]:
    """One item per saved okg entry (and command) in each runtime's config."""
    from .registry import KNOWN_RUNTIMES

    cache_root = uv_cache_root()
    runtimes_out: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    for runtime in KNOWN_RUNTIMES if runtimes is None else runtimes:
        path = runtime.config_path()
        row: dict[str, Any] = {
            "runtime": runtime.name,
            "config_path": str(path) if path is not None else None,
        }
        runtimes_out.append(row)
        if path is None or not path.is_file():
            row["status"] = "no_config"
            continue
        try:
            scopes = list(_scopes(path))
        except (OSError, ValueError, RecursionError, tomllib.TOMLDecodeError) as exc:
            row["status"] = "unreadable"
            items.append({
                "runtime": runtime.name,
                "config_path": str(path),
                "ok": False,
                "code": "agent_runtime_config_unreadable",
                "message": (
                    f"{path} cannot be read ({type(exc).__name__}: {exc}), so "
                    "whether its okg entries still run is unknown"
                ),
                "fix_hint": f"fix the syntax of {path}",
            })
            continue
        row["status"] = "read"
        count = 0
        for scope, servers in scopes:
            for key, entry in servers.items():
                if not _okg_saved(str(key), entry):
                    continue
                count += 1
                items.extend(_check_entry(
                    runtime.name, path, scope, str(key), entry, cache_root,
                ))
        row["okg_entries"] = count
    failing = [item for item in items if not item["ok"]]
    return {
        "check": "agent-runtimes",
        "schema": SCHEMA,
        "ok": not failing,
        "status": "ok" if not failing else "broken",
        "uv_cache_root": str(cache_root) if cache_root is not None else None,
        "runtimes": runtimes_out,
        "items": items,
        "failures": failing,
    }


def _scopes(path: Path) -> Iterator[tuple[str, dict[str, Any]]]:
    """(scope, servers) pairs, each scope on its own; non-mappings skipped."""
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".toml":
        document = tomllib.loads(text)
        servers = document.get("mcp_servers") if isinstance(document, dict) else None
        if isinstance(servers, dict):
            yield "mcp_servers", servers
        return
    document = json.loads(text) if text.strip() else {}
    if not isinstance(document, dict):
        return
    servers = document.get("mcpServers")
    if isinstance(servers, dict):
        yield "mcpServers", servers
    projects = document.get("projects")
    if isinstance(projects, dict):
        for project, body in projects.items():
            if isinstance(body, dict) and isinstance(body.get("mcpServers"), dict):
                yield f"projects.{project}.mcpServers", body["mcpServers"]


def _is_connect(key: str, entry: object) -> bool:
    """`is_connect_entry`, safe for an entry whose `args` is not a list."""
    if isinstance(entry, dict) and not isinstance(entry.get("args") or [], list):
        return False
    return is_connect_entry(key, entry)


def _okg_saved(key: str, entry: object) -> bool:
    """An entry okg saved: connect's, or an `okg-*` entry running okg's CLI.

    An `okg-*` entry whose shape cannot be read (not a mapping, or `args`
    not a list) counts, so it is reported as unreadable rather than skipped.
    """
    if _is_connect(key, entry):
        return True
    if not key.startswith("okg-"):
        return False
    if not isinstance(entry, dict):
        return True
    args = entry.get("args")
    if args is not None and not isinstance(args, list):
        return True
    tokens = [str(a) for a in args or []]
    return OKG_CLI_MODULE in tokens or "mcp-serve" in tokens


def _check_entry(
    runtime: str,
    config_path: Path,
    scope: str,
    key: str,
    entry: object,
    cache_root: Path | None,
) -> list[dict[str, Any]]:
    base: dict[str, Any] = {
        "runtime": runtime,
        "config_path": str(config_path),
        "scope": scope,
        "entry": key,
        "ok": False,
    }
    if isinstance(entry, dict) and (
        entry.get("enabled") is False or entry.get("disabled") is True
    ):
        return [{**base, "ok": True, "code": "agent_runtime_entry_disabled"}]
    if (
        not isinstance(entry, dict)
        or not isinstance(entry.get("command"), str)
        or not isinstance(entry.get("args") or [], list)
    ):
        return [{
            **base,
            "code": "agent_runtime_config_unreadable",
            "message": (
                f"the {runtime} entry {key!r} ({scope}) in {config_path} has no "
                "command okg can read"
            ),
            "fix_hint": _fix_hint(runtime, key, entry, config_path, scope),
        }]
    args = [str(a) for a in entry.get("args") or []]
    commands = [entry["command"]]
    # The agent-runtime-launch wrapper starts a second interpreter after `--`.
    if "--" in args:
        split = args.index("--")
        if "agent-runtime-launch" in args[:split] and split + 1 < len(args):
            commands.append(args[split + 1])
    items = []
    for program in dict.fromkeys(commands):
        items.append(_check_command(
            {**base, "command": program}, runtime, key, entry, config_path,
            program, cache_root,
        ))
    return items


def _check_command(
    item: dict[str, Any],
    runtime: str,
    key: str,
    entry: dict[str, Any],
    config_path: Path,
    program: str,
    cache_root: Path | None,
) -> dict[str, Any]:
    where = f"the {runtime} entry {key!r} ({item['scope']}) in {config_path}"
    fix = _fix_hint(runtime, key, entry, config_path, item["scope"])
    if not os.path.isabs(program):
        # The agent starts it with its own (often minimal) PATH, which the
        # doctor cannot see: resolving here would guess.
        item.update(
            code="agent_runtime_command_relative",
            message=(
                f"{where} runs {program!r} by name, so whether the agent can "
                "find it depends on the agent's PATH, which okg cannot see "
                f"(this shell resolves it to {shutil.which(program)})"
            ),
            fix_hint=fix,
        )
        return item
    if not os.path.exists(program):
        item.update(
            code="agent_runtime_command_missing",
            message=(
                f"{where} runs {program}, which no longer exists, so the "
                "agent cannot start the okg server"
            ),
            fix_hint=fix,
        )
        return item
    real = Path(os.path.realpath(program))
    item["resolved_command"] = str(real)
    if real.is_dir() or not os.access(real, os.X_OK):
        item.update(
            code="agent_runtime_command_not_executable",
            message=f"{where} runs {program}, which is not an executable file",
            fix_hint=fix,
        )
        return item
    # A script (an older entry saved the venv's `okg` script) names its
    # interpreter on its first line; a moved venv leaves that path dangling.
    interpreter = _script_interpreter(real)
    if interpreter is not None and not os.path.exists(interpreter):
        item.update(
            code="agent_runtime_command_missing",
            message=(
                f"{where} runs {program}, a script whose interpreter "
                f"{interpreter} no longer exists"
            ),
            fix_hint=fix,
        )
        return item
    # The interpreter's own directory, as `okg agent connect` checks it: a
    # venv's `bin/python` is a symlink into a uv-managed Python outside the
    # cache, so the resolved file alone would hide a cached environment.
    command_dir = Path(os.path.realpath(Path(program).absolute().parent))
    if cache_root is not None and command_dir.is_relative_to(cache_root):
        item.update(
            code="agent_runtime_command_in_uv_cache",
            message=(
                f"{where} runs {program}, inside uv's cache ({cache_root}); "
                "`uv cache prune` or `uv cache clean` can delete it"
            ),
            fix_hint=fix,
        )
        return item
    item.update(ok=True, code="agent_runtime_command_ok")
    return item


def _script_interpreter(path: Path) -> str | None:
    """The absolute interpreter a `#!` script names, or None."""
    try:
        with path.open("rb") as handle:
            first = handle.readline(512)
    except OSError:
        return None
    if not first.startswith(b"#!"):
        return None
    parts = first[2:].decode("utf-8", "replace").split()
    if not parts or not os.path.isabs(parts[0]) or parts[0] == "/usr/bin/env":
        return None
    return parts[0]


def _fix_hint(
    runtime: str, key: str, entry: object, config_path: Path, scope: str = "",
) -> str:
    if scope.startswith("projects."):
        # `okg agent connect` writes only Claude Code's user scope, so a
        # project (local-scope) entry is not something reconnecting fixes.
        project = scope[len("projects."):-len(".mcpServers")]
        return (
            f"this entry is in Claude Code's local scope for {project}, which "
            f"`okg agent connect` does not write: run `claude mcp remove {key} "
            f"-s local` in {project}, or fix its command in {config_path}"
        )
    if _is_connect(key, entry):
        deployment = key.removeprefix("okg-")
        return (
            f"re-run `okg agent connect --deployment {deployment} --agent "
            f"{runtime}` from a lasting okg install; " + EPHEMERAL_OKG_FIX_HINT
        )
    return (
        f"point the entry {key!r} in {config_path} at a lasting okg install, "
        "or re-create it with the command that wrote it; " + EPHEMERAL_OKG_FIX_HINT
    )
