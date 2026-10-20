"""Tests for the `agent_runtimes` registry + the three
concrete runtime impls. Per `dead-simple-install`.

Strategy: every test patches the runtime's `config_path` to a
`tmp_path`-rooted location, so we never touch the operator's
real config files.
"""
from __future__ import annotations

import json
import os
import sys
import tomllib
from pathlib import Path
from unittest import mock

import pytest


from okg.substrate.cli.child_argv import OKG_CLI_MODULE
from okg.substrate.library.agent_runtimes import (
    RegisterReport, RegisterResult, atomic_write_text,
)
from okg.substrate.library.agent_runtimes.claude_code import (
    ClaudeCodeRuntime,
)
from okg.substrate.library.agent_runtimes.claude_desktop import (
    ClaudeDesktopRuntime,
)
from okg.substrate.library.agent_runtimes.codex_cli import (
    CodexCliRuntime,
)
from okg.substrate.library.agent_runtimes.cursor import (
    CursorRuntime,
)
from okg.substrate.library.agent_runtimes.registry import (
    register_with_all_detected, unregister_from_all,
)


# ---------- helpers ----------


def _patch_path(runtime, path: Path):
    """Return a context manager that swaps the runtime's
    `config_path` method to return `path`."""
    return mock.patch.object(
        type(runtime), "config_path", return_value=path,
    )


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_toml(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _assert_profiled_mcp_entry(
    entry: dict,
    *,
    runtime: str,
    deployment: str = "demo",
    profile: str = "read-only",
    allow_writes: bool = False,
) -> None:
    assert entry["command"]
    assert "OKG_DSN" not in entry.get("env", {})
    assert entry["env"]["OKG_DEPLOYMENT"] == deployment
    assert entry["env"]["OKG_AGENT_RUNTIME_PROFILE"] == profile
    args = entry["args"]
    assert "agent-runtime-launch" in args
    assert args[args.index("--runtime") + 1] == runtime
    assert args[args.index("--permissions") + 1] == profile
    assert args[args.index("--deployment") + 1] == deployment
    separator = args.index("--")
    child = args[separator + 1:]
    assert "mcp-serve" in child
    assert child[child.index("--deployment") + 1] == deployment
    assert ("--allow-writes" in child) is allow_writes


DSN = "postgres://postgres:okg@localhost:32773/okg_test"


# ---------- atomic_write_text ----------


def test_atomic_write_text_creates_parent(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "deeper" / "config.json"
    atomic_write_text(target, '{"a": 1}')
    assert target.read_text() == '{"a": 1}'


def test_atomic_write_text_no_partial_file_on_concurrent_read(
    tmp_path: Path,
) -> None:
    """The atomicity contract: a reader either sees the old
    content or the new content, never a half-written file. We
    can't easily simulate the race, but we can verify the
    contract holds in the trivial sequential case + that no
    `*.tmp` files are left behind."""
    target = tmp_path / "config.json"
    target.write_text("old", encoding="utf-8")
    atomic_write_text(target, "new")
    assert target.read_text() == "new"
    leftover = list(tmp_path.glob("config.json.*.tmp"))
    assert leftover == []


# ---------- ClaudeDesktopRuntime ----------


def test_claude_desktop_register_creates_entry(tmp_path: Path) -> None:
    cfg = tmp_path / "claude_desktop_config.json"
    cfg.write_text(json.dumps({}), encoding="utf-8")
    rt = ClaudeDesktopRuntime()
    with _patch_path(rt, cfg):
        result = rt.register(deployment="demo", dsn=DSN)
    assert result.status == "registered"
    parsed = _read_json(cfg)
    assert "okg-demo" in parsed["mcpServers"]
    entry = parsed["mcpServers"]["okg-demo"]
    _assert_profiled_mcp_entry(entry, runtime="claude-desktop")


def test_claude_desktop_register_is_idempotent(tmp_path: Path) -> None:
    """Two consecutive registers SHALL produce exactly one
    entry, not two."""
    cfg = tmp_path / "claude_desktop_config.json"
    cfg.write_text(json.dumps({}), encoding="utf-8")
    rt = ClaudeDesktopRuntime()
    with _patch_path(rt, cfg):
        rt.register(deployment="demo", dsn=DSN)
        rt.register(deployment="demo", dsn=DSN)
    parsed = _read_json(cfg)
    assert list(parsed["mcpServers"].keys()) == ["okg-demo"]


def test_claude_desktop_preserves_other_servers(tmp_path: Path) -> None:
    cfg = tmp_path / "claude_desktop_config.json"
    cfg.write_text(json.dumps({
        "mcpServers": {"someone-else": {"command": "/x"}},
    }), encoding="utf-8")
    rt = ClaudeDesktopRuntime()
    with _patch_path(rt, cfg):
        rt.register(deployment="demo", dsn=DSN)
    parsed = _read_json(cfg)
    assert set(parsed["mcpServers"].keys()) == {
        "someone-else", "okg-demo",
    }


def test_claude_desktop_not_detected_when_config_absent(
    tmp_path: Path,
) -> None:
    cfg = tmp_path / "absent.json"
    rt = ClaudeDesktopRuntime()
    with _patch_path(rt, cfg):
        result = rt.register(deployment="demo", dsn=DSN)
    assert result.status == "not_detected"


def test_claude_desktop_failed_on_invalid_json(tmp_path: Path) -> None:
    cfg = tmp_path / "claude_desktop_config.json"
    cfg.write_text("not valid json{{", encoding="utf-8")
    rt = ClaudeDesktopRuntime()
    with _patch_path(rt, cfg):
        result = rt.register(deployment="demo", dsn=DSN)
    assert result.status == "failed"
    assert result.error is not None


def test_claude_desktop_unregister_removes_entry(tmp_path: Path) -> None:
    cfg = tmp_path / "claude_desktop_config.json"
    cfg.write_text(json.dumps({}), encoding="utf-8")
    rt = ClaudeDesktopRuntime()
    with _patch_path(rt, cfg):
        rt.register(deployment="demo", dsn=DSN)
        assert rt.unregister("demo") is True
        # second call is a no-op (entry gone).
        assert rt.unregister("demo") is False
    parsed = _read_json(cfg)
    assert "okg-demo" not in parsed.get("mcpServers", {})


# ---------- CodexCliRuntime ----------


def test_codex_register_creates_block(tmp_path: Path) -> None:
    cfg = tmp_path / "config.toml"
    cfg.write_text("", encoding="utf-8")
    rt = CodexCliRuntime()
    with _patch_path(rt, cfg):
        result = rt.register(deployment="demo", dsn=DSN)
    assert result.status == "registered"
    parsed = _read_toml(cfg)
    assert "okg-demo" in parsed["mcp_servers"]
    entry = parsed["mcp_servers"]["okg-demo"]
    _assert_profiled_mcp_entry(entry, runtime="codex")


def test_codex_register_is_idempotent(tmp_path: Path) -> None:
    cfg = tmp_path / "config.toml"
    cfg.write_text("", encoding="utf-8")
    rt = CodexCliRuntime()
    with _patch_path(rt, cfg):
        rt.register(deployment="demo", dsn=DSN)
        rt.register(deployment="demo", dsn=DSN)
    parsed = _read_toml(cfg)
    assert list(parsed["mcp_servers"].keys()) == ["okg-demo"]


def test_codex_preserves_other_top_level_tables(tmp_path: Path) -> None:
    """The codex config typically has [marketplaces.*],
    [model_provider.*], etc. Upsert SHALL preserve those."""
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        '[marketplaces.openai-bundled]\n'
        'last_updated = "2026-05-19T23:47:36Z"\n'
        'source = "/foo/bar"\n'
        '\n'
        '[mcp_servers.someone-else]\n'
        'command = "/usr/local/bin/other"\n'
        'args = []\n',
        encoding="utf-8",
    )
    rt = CodexCliRuntime()
    with _patch_path(rt, cfg):
        rt.register(deployment="demo", dsn=DSN)
    parsed = _read_toml(cfg)
    assert "marketplaces" in parsed
    assert parsed["marketplaces"]["openai-bundled"]["source"] == "/foo/bar"
    assert set(parsed["mcp_servers"].keys()) == {
        "someone-else", "okg-demo",
    }


def test_codex_not_detected_when_config_absent(tmp_path: Path) -> None:
    cfg = tmp_path / "absent.toml"
    rt = CodexCliRuntime()
    with _patch_path(rt, cfg):
        result = rt.register(deployment="demo", dsn=DSN)
    assert result.status == "not_detected"


def test_codex_unregister_drops_empty_mcp_servers_table(
    tmp_path: Path,
) -> None:
    """If unregister leaves mcp_servers empty, the table SHALL
    be removed entirely (no orphan `[mcp_servers]` header)."""
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        '[marketplaces.example]\n'
        'source = "/x"\n',
        encoding="utf-8",
    )
    rt = CodexCliRuntime()
    with _patch_path(rt, cfg):
        rt.register(deployment="demo", dsn=DSN)
        removed = rt.unregister("demo")
    assert removed is True
    parsed = _read_toml(cfg)
    assert "mcp_servers" not in parsed
    assert "marketplaces" in parsed


# ---------- CursorRuntime ----------


def test_cursor_register_creates_entry(tmp_path: Path) -> None:
    cfg = tmp_path / "mcp.json"
    cfg.write_text(json.dumps({}), encoding="utf-8")
    rt = CursorRuntime()
    with _patch_path(rt, cfg):
        result = rt.register(deployment="demo", dsn=DSN)
    assert result.status == "registered"
    parsed = _read_json(cfg)
    assert "okg-demo" in parsed["mcpServers"]
    _assert_profiled_mcp_entry(parsed["mcpServers"]["okg-demo"], runtime="cursor")


def test_cursor_register_is_idempotent(tmp_path: Path) -> None:
    cfg = tmp_path / "mcp.json"
    cfg.write_text(json.dumps({}), encoding="utf-8")
    rt = CursorRuntime()
    with _patch_path(rt, cfg):
        rt.register(deployment="demo", dsn=DSN)
        rt.register(deployment="demo", dsn=DSN)
    parsed = _read_json(cfg)
    assert list(parsed["mcpServers"].keys()) == ["okg-demo"]


def test_cursor_not_detected_when_config_absent(tmp_path: Path) -> None:
    cfg = tmp_path / "absent.json"
    rt = CursorRuntime()
    with _patch_path(rt, cfg):
        result = rt.register(deployment="demo", dsn=DSN)
    assert result.status == "not_detected"


# ---------- claude code ----------
# Per `register-claude-code-runtime`: Claude Code is the runtime
# most likely to exist on Linux (Claude Desktop doesn't ship
# there); a Linux PI's first install finished with zero
# registered agents before this runtime existed.


def test_claude_code_register_creates_entry(tmp_path: Path) -> None:
    cfg = tmp_path / ".claude.json"
    cfg.write_text(json.dumps({}), encoding="utf-8")
    rt = ClaudeCodeRuntime()
    with _patch_path(rt, cfg):
        result = rt.register(deployment="demo", dsn=DSN)
    assert result.status == "registered"
    parsed = _read_json(cfg)
    assert "okg-demo" in parsed["mcpServers"]
    _assert_profiled_mcp_entry(
        parsed["mcpServers"]["okg-demo"],
        runtime="claude-code",
    )


def test_claude_code_preserves_unrelated_config(tmp_path: Path) -> None:
    """`~/.claude.json` is Claude Code's live config and carries far
    more than MCP servers (projects, history, settings). The upsert
    SHALL preserve every unrelated key and any pre-existing MCP
    servers."""
    cfg = tmp_path / ".claude.json"
    cfg.write_text(json.dumps({
        "projects": {"/home/pi/research": {"history": ["q1", "q2"]}},
        "theme": "dark",
        "mcpServers": {"other-server": {"command": "x"}},
    }), encoding="utf-8")
    rt = ClaudeCodeRuntime()
    with _patch_path(rt, cfg):
        rt.register(deployment="demo", dsn=DSN)
    parsed = _read_json(cfg)
    assert parsed["projects"] == {
        "/home/pi/research": {"history": ["q1", "q2"]},
    }
    assert parsed["theme"] == "dark"
    assert parsed["mcpServers"]["other-server"] == {"command": "x"}
    assert "okg-demo" in parsed["mcpServers"]


def test_claude_code_register_is_idempotent(tmp_path: Path) -> None:
    cfg = tmp_path / ".claude.json"
    cfg.write_text(json.dumps({}), encoding="utf-8")
    rt = ClaudeCodeRuntime()
    with _patch_path(rt, cfg):
        rt.register(deployment="demo", dsn=DSN)
        rt.register(deployment="demo", dsn=DSN)
    parsed = _read_json(cfg)
    assert list(parsed["mcpServers"].keys()) == ["okg-demo"]


def test_claude_code_not_detected_without_config_or_binary(
    tmp_path: Path,
) -> None:
    cfg = tmp_path / "absent.json"
    rt = ClaudeCodeRuntime()
    with (
        _patch_path(rt, cfg),
        mock.patch(
            "okg.substrate.library.agent_runtimes.claude_code."
            "shutil.which",
            return_value=None,
        ),
    ):
        result = rt.register(deployment="demo", dsn=DSN)
    assert result.status == "not_detected"


def test_claude_code_binary_only_creates_config(tmp_path: Path) -> None:
    """Claude Code only writes `~/.claude.json` on first launch, so
    "installed but never run" must still be detected via the
    `claude` binary on PATH — the live PI case. Registration
    creates the config with just the mcpServers entry; Claude Code
    merges its own fields in on first launch."""
    cfg = tmp_path / ".claude.json"   # does not exist
    rt = ClaudeCodeRuntime()
    with (
        _patch_path(rt, cfg),
        mock.patch(
            "okg.substrate.library.agent_runtimes.claude_code."
            "shutil.which",
            return_value="/usr/local/bin/claude",
        ),
    ):
        assert rt.detect() is True
        result = rt.register(deployment="demo", dsn=DSN)
    assert result.status == "registered"
    parsed = _read_json(cfg)
    _assert_profiled_mcp_entry(
        parsed["mcpServers"]["okg-demo"],
        runtime="claude-code",
    )


def test_claude_code_unregister_removes_only_okg_entry(
    tmp_path: Path,
) -> None:
    cfg = tmp_path / ".claude.json"
    cfg.write_text(json.dumps({
        "mcpServers": {
            "okg-demo": {"command": "okg"},
            "other-server": {"command": "x"},
        },
    }), encoding="utf-8")
    rt = ClaudeCodeRuntime()
    with _patch_path(rt, cfg):
        assert rt.unregister("demo") is True
    parsed = _read_json(cfg)
    assert "okg-demo" not in parsed["mcpServers"]
    assert "other-server" in parsed["mcpServers"]


def test_claude_code_in_known_runtimes() -> None:
    from okg.substrate.library.agent_runtimes.registry import (
        KNOWN_RUNTIMES,
    )
    assert any(rt.name == "claude-code" for rt in KNOWN_RUNTIMES)


# ---------- register_with_all_detected ----------


def test_register_with_all_detected_skips_undetected_runtimes(
    tmp_path: Path,
) -> None:
    """A host with only Claude Desktop config should report
    Claude Desktop as `registered`, codex + cursor as
    `not_detected`. No silent skipping."""
    claude_cfg = tmp_path / "claude_desktop_config.json"
    claude_cfg.write_text(json.dumps({}), encoding="utf-8")
    absent_codex = tmp_path / "codex_config.toml"
    absent_cursor = tmp_path / "cursor_mcp.json"

    claude_rt = ClaudeDesktopRuntime()
    codex_rt = CodexCliRuntime()
    cursor_rt = CursorRuntime()
    with (
        _patch_path(claude_rt, claude_cfg),
        _patch_path(codex_rt, absent_codex),
        _patch_path(cursor_rt, absent_cursor),
    ):
        report = register_with_all_detected(
            deployment="demo", dsn=DSN,
            runtimes=[claude_rt, codex_rt, cursor_rt],
        )

    statuses = {r.runtime: r.status for r in report.results}
    assert statuses == {
        "claude-desktop": "registered",
        "codex": "not_detected",
        "cursor": "not_detected",
    }
    assert len(report.registered) == 1
    assert report.registered[0].runtime == "claude-desktop"


def test_register_with_all_detected_handles_multiple_runtimes(
    tmp_path: Path,
) -> None:
    claude_cfg = tmp_path / "claude_desktop_config.json"
    claude_cfg.write_text(json.dumps({}), encoding="utf-8")
    codex_cfg = tmp_path / "config.toml"
    codex_cfg.write_text("", encoding="utf-8")

    claude_rt = ClaudeDesktopRuntime()
    codex_rt = CodexCliRuntime()
    with (
        _patch_path(claude_rt, claude_cfg),
        _patch_path(codex_rt, codex_cfg),
    ):
        report = register_with_all_detected(
            deployment="demo", dsn=DSN,
            runtimes=[claude_rt, codex_rt],
        )

    assert len(report.registered) == 2
    # Both configs SHALL contain the entry.
    assert "okg-demo" in _read_json(claude_cfg)["mcpServers"]
    assert "okg-demo" in _read_toml(codex_cfg)["mcp_servers"]


def test_register_report_primary_is_most_recently_modified(
    tmp_path: Path,
) -> None:
    """`RegisterReport.primary_agent()` SHALL pick the runtime
    whose config file was most recently modified."""
    import os
    import time

    claude_cfg = tmp_path / "claude_desktop_config.json"
    claude_cfg.write_text(json.dumps({}), encoding="utf-8")
    codex_cfg = tmp_path / "config.toml"
    codex_cfg.write_text("", encoding="utf-8")

    claude_rt = ClaudeDesktopRuntime()
    codex_rt = CodexCliRuntime()
    with (
        _patch_path(claude_rt, claude_cfg),
        _patch_path(codex_rt, codex_cfg),
    ):
        register_with_all_detected(
            deployment="demo", dsn=DSN,
            runtimes=[claude_rt, codex_rt],
        )

    # Both wrote at approximately the same time. We have to
    # backdate one to test the recency heuristic
    # deterministically.
    older = time.time() - 3600
    # The order of writes inside register_with_all_detected is
    # claude → codex. Backdate Claude so codex is the most-recent.
    os.utime(claude_cfg, (older, older))
    # Re-run a single register on codex to update its mtime to
    # "now" relative to Claude's backdated mtime. The previous
    # register set Claude's then-now mtime; both are stored in
    # the report. We need to mirror the new mtime to the report.
    register_with_all_detected(
        deployment="demo", dsn=DSN,
        runtimes=[claude_rt, codex_rt],
    )
    # Now Claude's config mtime (just rewritten by the second
    # register) is fresher than codex's. Wait — that's wrong;
    # both runtimes wrote on the second register. Backdate
    # Claude again post-second-register.
    os.utime(claude_cfg, (older, older))
    # Build a synthetic report from the current mtimes.
    final = RegisterReport(results=[
        RegisterResult(
            runtime="claude-desktop", status="registered",
            config_path=claude_cfg,
            config_mtime=claude_cfg.stat().st_mtime,
        ),
        RegisterResult(
            runtime="codex", status="registered",
            config_path=codex_cfg,
            config_mtime=codex_cfg.stat().st_mtime,
        ),
    ])
    assert final.primary_agent() == "codex"
    assert final.secondary_agents() == ["claude-desktop"]


# ---------- unregister_from_all ----------


def test_unregister_from_all_only_returns_runtimes_with_entry(
    tmp_path: Path,
) -> None:
    """`unregister_from_all` SHALL only list runtimes that
    actually had an entry removed — not runtimes that were
    not_detected."""
    claude_cfg = tmp_path / "claude_desktop_config.json"
    claude_cfg.write_text(json.dumps({}), encoding="utf-8")
    absent_codex = tmp_path / "absent_codex.toml"

    claude_rt = ClaudeDesktopRuntime()
    codex_rt = CodexCliRuntime()
    with (
        _patch_path(claude_rt, claude_cfg),
        _patch_path(codex_rt, absent_codex),
    ):
        claude_rt.register(deployment="demo", dsn=DSN)
        removed = unregister_from_all(
            deployment="demo",
            runtimes=[claude_rt, codex_rt],
        )

    assert removed == ["claude-desktop"]


# ---------- issue #2301: the written command is THIS okg ----------


def _decoy_okg_first_on_path(tmp_path: Path, monkeypatch) -> Path:
    bin_dir = tmp_path / "other-install" / "bin"
    bin_dir.mkdir(parents=True)
    decoy = bin_dir / "okg"
    decoy.write_text("#!/bin/sh\nexit 97\n")
    decoy.chmod(0o755)
    monkeypatch.setenv(
        "PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
    )
    return decoy


@pytest.mark.parametrize("okg_on_path", [True, False])
def test_mcp_config_command_is_the_running_interpreter_with_p(
    tmp_path: Path, monkeypatch, okg_on_path: bool,
) -> None:
    """The MCP entry runs in the operator's project folder.

    With an `okg` on PATH it must still name the running interpreter (a
    PATH `okg` can be another install with other code); without one it
    must still carry `-P`, so a project folder holding `mcp/`, `okg/` or
    `types.py` cannot shadow the real modules in the server child.
    """
    if okg_on_path:
        decoy = _decoy_okg_first_on_path(tmp_path, monkeypatch)
    else:
        monkeypatch.setenv("PATH", str(tmp_path / "empty-bin"))
    cfg = tmp_path / ".claude.json"
    cfg.write_text("{}", encoding="utf-8")  # an existing config = detected
    rt = ClaudeCodeRuntime()
    with _patch_path(rt, cfg):
        result = rt.register(deployment="demo", dsn=DSN)
    assert result.status == "registered"
    entry = _read_json(cfg)["mcpServers"]["okg-demo"]
    _assert_profiled_mcp_entry(entry, runtime="claude-code")
    prefix = ["-P", "-m", OKG_CLI_MODULE]
    assert entry["command"] == sys.executable
    assert Path(entry["command"]).is_absolute()
    args = entry["args"]
    assert args[:4] == [*prefix, "agent-runtime-launch"], args
    child = args[args.index("--") + 1:]
    assert child[:5] == [sys.executable, *prefix, "mcp-serve"], child
    if okg_on_path:
        assert str(decoy) not in json.dumps(entry)


# ---------- a saved command must outlive uv's cache ----------


def test_uv_cache_root_follows_uvs_documented_order(tmp_path: Path) -> None:
    from okg.substrate.library.agent_runtimes import uv_cache_root

    explicit = tmp_path / "explicit"
    assert uv_cache_root({"UV_CACHE_DIR": str(explicit),
                          "XDG_CACHE_HOME": str(tmp_path / "xdg")}) == (
        Path(os.path.realpath(explicit))
    )
    if os.name != "nt":
        assert uv_cache_root({"XDG_CACHE_HOME": str(tmp_path / "xdg")}) == (
            Path(os.path.realpath(tmp_path / "xdg" / "uv"))
        )
        assert uv_cache_root({}) == Path(
            os.path.realpath(Path.home() / ".cache" / "uv"),
        )


def test_an_interpreter_in_uvs_cache_is_refused_by_name(
    tmp_path: Path, monkeypatch,
) -> None:
    """`uvx` / `uv run --with` put sys.executable under uv's cache.

    A cache prune can delete that interpreter, so saving it as the MCP
    command would leave a config that fails later with no warning.
    """
    from okg.substrate.library.agent_runtimes import (
        EphemeralOkgInterpreterError, resolve_okg_binary,
    )
    from okg.substrate.library.agent_runtimes.registry import (
        register_with_all_detected as register_all,
    )

    env_root = Path(sys.executable).absolute().parent.parent
    monkeypatch.setenv("UV_CACHE_DIR", str(env_root))
    with pytest.raises(EphemeralOkgInterpreterError) as caught:
        resolve_okg_binary()
    message = str(caught.value)
    assert "uv tool install" in message and "pipx" in message, message
    assert sys.executable in message, message

    cfg = tmp_path / ".claude.json"
    original = json.dumps({"mcpServers": {"other": {"command": "x"}}})
    cfg.write_text(original, encoding="utf-8")
    rt = ClaudeCodeRuntime()
    with _patch_path(rt, cfg):
        report = register_all(deployment="demo", dsn=DSN, runtimes=[rt])
    (result,) = report.results
    assert result.status == "failed", result
    assert "uv tool install" in (result.error or ""), result
    # Nothing is written: the operator's config is byte-for-byte unchanged.
    assert cfg.read_text(encoding="utf-8") == original


def test_a_venv_whose_bin_links_into_uvs_cache_is_refused(
    tmp_path: Path, monkeypatch,
) -> None:
    """The interpreter's folder is judged after resolving symlinks.

    A venv outside the cache whose ``bin/`` is a symlink into uv's cache
    names an interpreter a cache prune can delete. The interpreter FILE is
    not resolved (a uv tool venv's python links to uv's Python install,
    which is not the cache); only its folder is.
    """
    from okg.substrate.library import agent_runtimes
    from okg.substrate.library.agent_runtimes import (
        EphemeralOkgInterpreterError, resolve_okg_binary,
    )

    cache = tmp_path / "uv-cache"
    cached_bin = cache / "archive-v0" / "abc123" / "bin"
    cached_bin.mkdir(parents=True)
    (cached_bin / "python3").write_text("", encoding="utf-8")
    venv = tmp_path / "project" / ".venv"
    venv.mkdir(parents=True)
    (venv / "bin").symlink_to(cached_bin, target_is_directory=True)
    interpreter = venv / "bin" / "python3"
    monkeypatch.setenv("UV_CACHE_DIR", str(cache))
    monkeypatch.setattr(
        agent_runtimes, "running_okg_argv",
        lambda: [str(interpreter), "-P", "-m", OKG_CLI_MODULE],
    )
    with pytest.raises(EphemeralOkgInterpreterError) as caught:
        resolve_okg_binary()
    assert str(interpreter) in str(caught.value)


def test_an_interpreter_outside_uvs_cache_is_saved(
    tmp_path: Path, monkeypatch,
) -> None:
    from okg.substrate.library.agent_runtimes import resolve_okg_binary

    monkeypatch.setenv("UV_CACHE_DIR", str(tmp_path / "uv-cache"))
    command, prefix = resolve_okg_binary()
    assert command == sys.executable
    assert prefix == ["-P", "-m", OKG_CLI_MODULE]


# ── okg doctor --check agent-runtimes: saved okg commands (#2418) ─────────


class _SavedRuntime:
    """A runtime stand-in: a name and a config file in a temp directory."""

    def __init__(self, name: str, path: Path) -> None:
        self.name = name
        self._path = path

    def config_path(self) -> Path:
        return self._path


def _connect_entry(python: str, *, inner: str | None = None) -> dict:
    """The shape `okg agent connect` saves: the launcher, then `--` and the
    child interpreter running `mcp-serve`."""
    return {
        "command": python,
        "args": [
            "-P", "-m", "okg.substrate.cli.main", "agent-runtime-launch",
            "--runtime", "x", "--deployment", "demo",
            "--deployments-dir", "/deployments", "--",
            inner or python, "-P", "-m", "okg.substrate.cli.main",
            "mcp-serve", "--deployment", "demo",
        ],
    }


def _write_json(path: Path, document: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def _executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def _report(runtime: str, path: Path) -> dict:
    from okg.substrate.library.agent_runtimes.saved_commands import saved_command_report

    return saved_command_report([_SavedRuntime(runtime, path)])


@pytest.fixture
def _uv_cache(tmp_path, monkeypatch) -> Path:
    cache = tmp_path / "uv-cache"
    monkeypatch.setenv("UV_CACHE_DIR", str(cache))
    return cache


def test_a_saved_command_that_no_longer_exists_is_named(tmp_path, _uv_cache) -> None:
    gone = tmp_path / "deleted-venv" / "bin" / "python3"
    config = _write_json(
        tmp_path / "claude_desktop_config.json",
        {"mcpServers": {"okg-demo": _connect_entry(str(gone))}},
    )

    report = _report("claude-desktop", config)

    assert report["ok"] is False
    (item,) = report["items"]  # outer and inner are the same path: one item
    assert (item["runtime"], item["config_path"], item["code"]) == (
        "claude-desktop", str(config), "agent_runtime_command_missing",
    )
    assert str(gone) in item["message"]
    assert "okg agent connect --deployment demo --agent claude-desktop" in item["fix_hint"]
    assert "uv tool install" in item["fix_hint"]


def test_a_saved_command_inside_uv_cache_is_named(tmp_path, _uv_cache) -> None:
    cached = _executable(_uv_cache / "archive-v0" / "abc" / "bin" / "python3")
    config = tmp_path / ".codex" / "config.toml"
    config.parent.mkdir(parents=True)
    entry = _connect_entry(str(cached))
    config.write_text(
        "[mcp_servers.okg-demo]\n"
        f'command = "{cached}"\n'
        f"args = {json.dumps(entry['args'])}\n",
        encoding="utf-8",
    )

    report = _report("codex", config)

    (item,) = report["items"]
    assert item["code"] == "agent_runtime_command_in_uv_cache"
    assert "uv cache" in item["message"]
    assert report["ok"] is False


def test_a_valid_saved_command_passes_and_a_missing_config_is_not_a_failure(
    tmp_path, _uv_cache,
) -> None:
    from okg.substrate.library.agent_runtimes.saved_commands import saved_command_report

    python = _executable(tmp_path / "lasting-venv" / "bin" / "python3")
    config = _write_json(
        tmp_path / ".cursor" / "mcp.json",
        {"mcpServers": {"okg-demo": _connect_entry(str(python))}},
    )

    report = saved_command_report([
        _SavedRuntime("cursor", config),
        _SavedRuntime("claude-code", tmp_path / "absent.json"),
    ])

    assert report["ok"] is True
    (item,) = report["items"]
    assert (item["code"], item["ok"]) == ("agent_runtime_command_ok", True)
    assert [r["status"] for r in report["runtimes"]] == ["read", "no_config"]


def test_a_saved_command_that_is_not_executable_is_named(tmp_path, _uv_cache) -> None:
    plain = tmp_path / "venv" / "bin" / "python3"
    plain.parent.mkdir(parents=True)
    plain.write_text("", encoding="utf-8")
    plain.chmod(0o644)
    config = _write_json(
        tmp_path / "c.json", {"mcpServers": {"okg-demo": _connect_entry(str(plain))}},
    )

    report = _report("claude-code", config)
    (item,) = report["items"]
    assert (item["code"], item["ok"], report["ok"]) == (
        "agent_runtime_command_not_executable", False, False,
    )


def test_each_claude_code_scope_is_checked_on_its_own(tmp_path, _uv_cache) -> None:
    """Review finding: user and project scopes shared one merged dict, so a
    healthy project entry hid a broken user-scope one of the same name."""
    python = _executable(tmp_path / "venv" / "bin" / "python3")
    config = _write_json(tmp_path / ".claude.json", {
        "mcpServers": {"okg-demo": _connect_entry(str(tmp_path / "gone" / "py"))},
        "projects": {"/work/a": {"mcpServers": {"okg-demo": _connect_entry(str(python))}}},
    })

    report = _report("claude-code", config)

    assert report["ok"] is False
    by_scope = {item["scope"]: item["code"] for item in report["items"]}
    assert by_scope == {
        "mcpServers": "agent_runtime_command_missing",
        "projects./work/a.mcpServers": "agent_runtime_command_ok",
    }


def test_other_servers_are_never_judged_or_crash_the_check(tmp_path, _uv_cache) -> None:
    """Review finding: a malformed neighbour crashed the check, and a
    third-party `--deployment` or `okg*` server read as broken."""
    python = _executable(tmp_path / "venv" / "bin" / "python3")
    config = _write_json(tmp_path / ".claude.json", {
        "mcpServers": {
            "other": {"command": "x", "env": ["not", "a", "mapping"]},
            "k8s": {"command": "kubectl-mcp", "args": ["--deployment", "web"]},
            "okgraph": {"command": "npx", "args": ["okgraph-mcp"]},
            "okg-demo": _connect_entry(str(python)),
        },
        "projects": [1, 2],
    })

    report = _report("claude-code", config)

    assert report["ok"] is True
    assert [item["entry"] for item in report["items"]] == ["okg-demo"]


def test_a_relative_command_is_not_called_healthy(tmp_path, _uv_cache) -> None:
    entry = {"command": "okg", "args": ["mcp-serve", "--deployment", "demo"]}
    config = _write_json(tmp_path / "c.json", {"mcpServers": {"okg-demo": entry}})

    report = _report("cursor", config)

    (item,) = report["items"]
    assert (item["code"], item["ok"]) == ("agent_runtime_command_relative", False)
    assert "PATH" in item["message"]
    # Not a connect entry: the hint does not claim connect wrote it.
    assert "okg agent connect" not in item["fix_hint"]


def test_the_launch_wrappers_inner_interpreter_is_checked(tmp_path, _uv_cache) -> None:
    python = _executable(tmp_path / "venv" / "bin" / "python3")
    gone = tmp_path / "old-venv" / "bin" / "python3"
    config = _write_json(tmp_path / "c.json", {
        "mcpServers": {"okg-demo": _connect_entry(str(python), inner=str(gone))},
    })

    report = _report("claude-code", config)

    assert report["ok"] is False
    assert {(i["command"], i["code"]) for i in report["items"]} == {
        (str(python), "agent_runtime_command_ok"),
        (str(gone), "agent_runtime_command_missing"),
    }


def test_a_disabled_entry_is_listed_not_failed(tmp_path, _uv_cache) -> None:
    config = tmp_path / "config.toml"
    entry = _connect_entry(str(tmp_path / "gone" / "py"))
    config.write_text(
        "[mcp_servers.okg-demo]\n"
        f'command = "{entry["command"]}"\n'
        f"args = {json.dumps(entry['args'])}\n"
        "enabled = false\n",
        encoding="utf-8",
    )

    report = _report("codex", config)

    assert report["ok"] is True
    (item,) = report["items"]
    assert item["code"] == "agent_runtime_entry_disabled"


def test_an_unparseable_config_is_unknown_not_ok(tmp_path, _uv_cache) -> None:
    config = tmp_path / ".claude.json"
    config.write_text("{not json", encoding="utf-8")
    report = _report("claude-code", config)
    assert report["ok"] is False
    assert report["items"][0]["code"] == "agent_runtime_config_unreadable"


def test_okg_doctor_check_agent_runtimes_reports_and_exits_1(
    tmp_path, monkeypatch, capsys, _uv_cache,
) -> None:
    from okg.substrate.cli.main import main
    from okg.substrate.library.agent_runtimes import registry

    config = _write_json(
        tmp_path / ".cursor" / "mcp.json",
        {"mcpServers": {"okg-demo": _connect_entry(str(tmp_path / "gone" / "py"))}},
    )
    monkeypatch.setattr(
        registry, "KNOWN_RUNTIMES", [_SavedRuntime("cursor", config)],
    )

    rc = main(["doctor", "--check", "agent-runtimes", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["check"] == "agent-runtimes"
    assert payload["failures"][0]["code"] == "agent_runtime_command_missing"


def test_okg_named_entries_that_are_not_okgs_or_are_malformed(tmp_path, _uv_cache) -> None:
    """Second review: an `okg-*` third-party server is not judged, and an
    `okg-*` entry with unreadable `args` is reported, never a crash."""
    config = _write_json(tmp_path / "c.json", {"mcpServers": {
        "okg-thirdparty": {"command": "npx", "args": ["some-other-mcp"]},
        "okg-broken": {"command": "/bin/sh", "args": 5},
    }})

    report = _report("claude-code", config)

    assert [(i["entry"], i["code"]) for i in report["items"]] == [
        ("okg-broken", "agent_runtime_config_unreadable"),
    ]


def test_a_double_dash_outside_the_launcher_is_not_a_command(tmp_path, _uv_cache) -> None:
    python = _executable(tmp_path / "venv" / "bin" / "python3")
    entry = {"command": str(python), "args": [
        "-m", "okg.substrate.cli.main", "mcp-serve", "--deployment", "x", "--", "foo",
    ]}
    config = _write_json(tmp_path / "c.json", {"mcpServers": {"okg-x": entry}})

    report = _report("claude-code", config)

    assert report["ok"] is True
    assert [i["command"] for i in report["items"]] == [str(python)]


def test_a_project_scope_entry_gets_advice_that_clears_it(tmp_path, _uv_cache) -> None:
    """`okg agent connect` writes only the user scope; reconnecting would
    leave a broken local-scope entry red."""
    config = _write_json(tmp_path / ".claude.json", {"projects": {"/work/a": {
        "mcpServers": {"okg-demo": _connect_entry(str(tmp_path / "gone" / "py"))},
    }}})

    (item,) = _report("claude-code", config)["items"]
    assert item["code"] == "agent_runtime_command_missing"
    assert "claude mcp remove okg-demo -s local" in item["fix_hint"]
    assert "/work/a" in item["fix_hint"]


def test_a_script_whose_interpreter_moved_is_missing(tmp_path, _uv_cache) -> None:
    """An older entry saved the venv's `okg` script; moving the venv leaves
    its first line pointing at an interpreter that is gone."""
    script = tmp_path / "moved-venv" / "bin" / "okg"
    script.parent.mkdir(parents=True)
    script.write_text(f"#!{tmp_path / 'old-venv' / 'bin' / 'python3'}\n", encoding="utf-8")
    script.chmod(0o755)
    entry = {"command": str(script), "args": ["mcp-serve", "--deployment", "demo"]}
    config = _write_json(tmp_path / "c.json", {"mcpServers": {"okg-demo": entry}})

    (item,) = _report("cursor", config)["items"]
    assert item["code"] == "agent_runtime_command_missing"
    assert "old-venv" in item["message"]


def test_a_bin_directory_linked_into_uv_cache_is_in_the_cache(tmp_path, _uv_cache) -> None:
    cached_bin = _executable(_uv_cache / "archive-v0" / "e" / "bin" / "python3").parent
    venv = tmp_path / "venv"
    venv.mkdir()
    (venv / "bin").symlink_to(cached_bin)
    config = _write_json(tmp_path / "c.json", {
        "mcpServers": {"okg-demo": _connect_entry(str(venv / "bin" / "python3"))},
    })

    (item,) = _report("claude-code", config)["items"]
    assert item["code"] == "agent_runtime_command_in_uv_cache"


def test_a_json_entry_marked_disabled_is_listed_not_failed(tmp_path, _uv_cache) -> None:
    entry = {**_connect_entry(str(tmp_path / "gone" / "py")), "disabled": True}
    config = _write_json(tmp_path / "c.json", {"mcpServers": {"okg-demo": entry}})
    report = _report("cursor", config)
    assert report["ok"] is True
    assert report["items"][0]["code"] == "agent_runtime_entry_disabled"


def test_a_deeply_nested_config_is_unreadable_not_a_crash(tmp_path, _uv_cache) -> None:
    config = tmp_path / ".claude.json"
    config.write_text("[" * 100_000 + "]" * 100_000, encoding="utf-8")
    report = _report("claude-code", config)
    assert report["items"][0]["code"] == "agent_runtime_config_unreadable"
