"""req.abx.arms: argv per arm, stream-json accounting, refusal paths."""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.bench.ab.arms import get_driver
from okg.bench.ab.arms.base import RunContext
from okg.bench.ab.arms.claude_code import (
    ClaudeCodeDriver,
    _parse_stream,
    carry_memory_forward,
    reset_shared_workdir,
)
from okg.bench.ab.errors import AbBenchError, ArmDriverError


def _ctx(tmp_path: Path, **over) -> RunContext:
    defaults = dict(
        run_dir=tmp_path / "run",
        data_root=tmp_path / "data",
        model="sonnet",
        task_timeout_s=60.0,
        permission_mode="bypassPermissions",
    )
    defaults.update(over)
    return RunContext(**defaults)


def test_arm_registry_covers_the_three_protocol_arms_plus_echo():
    for name in ("fresh", "native", "okg", "echo"):
        assert get_driver(name).name == name
    with pytest.raises(AbBenchError):
        get_driver("api-metered")


def test_every_claude_arm_blocks_web_tools(tmp_path):
    """The no-web corpus-validity rule is TOOL-level enforcement, not a
    prompt request: every claude arm disallows WebSearch/WebFetch."""
    for mode in ("fresh", "native", "okg"):
        ctx = _ctx(tmp_path, native_memory_mode="shared-workdir")
        if mode == "okg":
            ctx.state["mcp_config_path"] = tmp_path / "mcp.json"
        argv = ClaudeCodeDriver(mode)._build_argv(ctx)
        i = argv.index("--disallowedTools")
        assert argv[i + 1 : i + 3] == ["WebSearch", "WebFetch"]


def test_fresh_argv_is_hermetic(tmp_path):
    driver = ClaudeCodeDriver("fresh")
    argv = driver._build_argv(_ctx(tmp_path))
    assert argv[:2] == ["claude", "-p"]
    assert "--output-format" in argv and "stream-json" in argv
    i = argv.index("--setting-sources")
    assert argv[i + 1] == ""  # no user/project settings, no CLAUDE.md discovery
    assert "--mcp-config" not in argv


def test_native_argv_keeps_ordinary_affordances(tmp_path):
    driver = ClaudeCodeDriver("native")
    argv = driver._build_argv(_ctx(tmp_path, native_memory_mode="shared-workdir"))
    i = argv.index("--setting-sources")
    assert argv[i + 1] == "user,project,local"


def test_okg_argv_mounts_the_run_mcp_strictly(tmp_path):
    mcp = tmp_path / "mcp-config.json"
    mcp.write_text("{}")
    driver = ClaudeCodeDriver("okg")
    ctx = _ctx(tmp_path, corpus={"deployment_name": "d"})
    ctx.state["mcp_config_path"] = mcp
    argv = driver._build_argv(ctx)
    i = argv.index("--mcp-config")
    assert argv[i + 1] == str(mcp)
    assert "--strict-mcp-config" in argv


def test_okg_argv_refuses_without_started_server(tmp_path):
    driver = ClaudeCodeDriver("okg")
    with pytest.raises(ArmDriverError, match="did not start"):
        driver._build_argv(_ctx(tmp_path, corpus={"deployment_name": "d"}))


def test_okg_setup_refuses_without_corpus(tmp_path):
    driver = ClaudeCodeDriver("okg")
    with pytest.raises(ArmDriverError, match="corpus"):
        driver.setup_run(_ctx(tmp_path, corpus=None))


def test_native_setup_refuses_without_memory_mode(tmp_path):
    driver = ClaudeCodeDriver("native")
    with pytest.raises(ArmDriverError, match="native_memory_mode"):
        driver.setup_run(_ctx(tmp_path, native_memory_mode=None))


def test_setup_refuses_missing_claude_binary(tmp_path):
    driver = ClaudeCodeDriver("fresh")
    with pytest.raises(ArmDriverError, match="not on PATH"):
        driver.setup_run(_ctx(tmp_path, claude_bin="claude-definitely-missing"))


def test_parse_stream_accounts_from_the_result_event():
    # Every element is ONE stream-json event; the `+` continues a single
    # JSON object across source lines and is never a missing comma.
    stream = "\n".join(
        [
            '{"type":"system","session_id":"s-1"}',
            '{"type":"assistant","message":{"content":['
            + '{"type":"tool_use","name":"Bash"},{"type":"text","text":"hi"}]}}',
            '{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Edit"}]}}',
            '{"type":"result","subtype":"success","is_error":false,'
            + '"result":"final answer","session_id":"s-1","num_turns":3,'
            + '"total_cost_usd":0.0,'
            + '"usage":{"input_tokens":1200,"output_tokens":345,'
            + '"cache_read_input_tokens":9000,"cache_creation_input_tokens":100}}',
        ]
    )
    metrics, answer, session_id, result_event = _parse_stream(stream, wall_s=12.5)
    assert metrics.input_tokens == 1200
    assert metrics.output_tokens == 345
    assert metrics.cache_read_tokens == 9000
    assert metrics.tool_calls == 2
    assert metrics.num_turns == 3
    assert metrics.wall_s == 12.5
    assert answer == "final answer"
    assert session_id == "s-1"
    assert result_event["subtype"] == "success"


def test_parse_stream_tolerates_truncated_tail():
    stream = '{"type":"assistant","message":{"content":[{"type":"tool_use"}]}}\n{"type":"res'
    metrics, answer, session_id, result_event = _parse_stream(stream, wall_s=1.0)
    assert metrics.tool_calls == 1
    assert result_event is None
    assert answer == ""


def test_shared_workdir_reset_preserves_memory_files(tiny_repo, tmp_path):
    from okg.bench.ab import checkouts

    data_root = tmp_path / "data"
    head = checkouts._run(["git", "-C", str(tiny_repo), "rev-parse", "HEAD"]).strip()
    first = checkouts._run(
        ["git", "-C", str(tiny_repo), "rev-parse", "HEAD~2"]
    ).strip()
    workdir = tmp_path / "shared"
    checkouts.task_workdir(str(tiny_repo), head, workdir, data_root=data_root)
    (workdir / "CLAUDE.md").write_text("remember: the widget frobnicates\n")
    (workdir / "scratch.txt").write_text("task litter\n")
    reset_shared_workdir(workdir, first)
    assert (workdir / "CLAUDE.md").read_text().startswith("remember")
    assert not (workdir / "scratch.txt").exists()
    assert (workdir / "file.txt").read_text() == "revision 0\n"


def test_carry_memory_forward_copies_only_memory_paths(tmp_path):
    prev = tmp_path / "prev"
    nxt = tmp_path / "next"
    (prev / ".claude").mkdir(parents=True)
    nxt.mkdir()
    (prev / "CLAUDE.md").write_text("m\n")
    (prev / ".claude" / "notes.md").write_text("n\n")
    (prev / "code.py").write_text("x\n")
    carried = carry_memory_forward(prev, nxt)
    assert sorted(carried) == [".claude", "CLAUDE.md"]
    assert (nxt / "CLAUDE.md").exists()
    assert (nxt / ".claude" / "notes.md").exists()
    assert not (nxt / "code.py").exists()
