"""Headless Claude Code arm drivers: fresh / native / okg.

All three arms run ``claude -p`` as a subprocess with
``--output-format stream-json`` — runs bill the operator's subscription (the
protocol's #1 budget lever); metrics come from the CLI's own result
accounting, never estimates. The arms differ ONLY in state policy:

- ``fresh``  — empty state each task: per-task isolated CLAUDE_CONFIG_DIR,
  per-task workdir, no settings sources.
- ``native`` — Claude Code's ordinary memory affordances persisted across
  tasks: one run-scoped CLAUDE_CONFIG_DIR (auto-memory accrues) and either a
  shared workdir (git-reset between tasks, CLAUDE.md/.claude preserved) or
  explicit CLAUDE.md carry-forward — ``native_memory_mode`` records which.
- ``okg``    — fresh-style isolation plus the corpus deployment's MCP server
  mounted via ``--mcp-config``/``--strict-mcp-config``; between tasks the
  completed session is ingested and a new generation published
  (``corpus.ingest_sessions_and_publish``).

Credential note: an isolated CLAUDE_CONFIG_DIR must still authenticate
against the operator's subscription. ``_seed_credentials`` copies
``~/.claude/.credentials.json`` when present; on macOS, keychain-stored
OAuth resolves independently of the config dir. A task failing with an
authentication error is a loud ArmDriverError, never a silent skip.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from ..errors import ArmDriverError
from ..model import TaskInstance, TaskMetrics, TaskResult
from .base import ArmDriver, RunContext
from okg.substrate.mcp.operators import OPERATOR_TOOLS


#: What `OKG_MCP_TOOL_SURFACE="operator"` was meant to select. That variable
#: was read by nothing, so these launches silently got the server's default
#: surface instead; `OKG_MCP_TOOL_ALLOWLIST` is the name the runtime honors.
_OPERATOR_SURFACE = ",".join(OPERATOR_TOOLS)

NATIVE_MEMORY_MODES = ("shared-workdir", "carry-forward")
# Files/dirs that ARE the native arm's memory: preserved across the
# between-task git reset (shared-workdir) or copied forward (carry-forward).
MEMORY_PATHS = ("CLAUDE.md", ".claude")


class ClaudeCodeDriver(ArmDriver):
    """Shared driver; ``mode`` selects the state policy."""

    def __init__(self, mode: str) -> None:
        if mode not in ("fresh", "native", "okg"):
            raise ArmDriverError(f"unknown claude-code arm mode {mode!r}")
        self.name = mode
        self.mode = mode

    # -- lifecycle ------------------------------------------------------
    def setup_run(self, ctx: RunContext) -> None:
        # REQUEST validation before ENVIRONMENT validation. A malformed
        # invocation is wrong whether or not `claude` is installed, and its
        # error names the thing the operator typed; the PATH error names the
        # machine. Checking PATH first meant a missing corpus or an invalid
        # memory mode was reported as "claude not on PATH" on any host without
        # the binary — including CI, where it made these very refusals
        # untestable and the lane red for a reason unrelated to either.
        if self.mode == "native" and ctx.native_memory_mode not in NATIVE_MEMORY_MODES:
            raise ArmDriverError(
                f"native arm needs native_memory_mode in {NATIVE_MEMORY_MODES}, "
                f"got {ctx.native_memory_mode!r}"
            )
        if self.mode == "okg":
            corpus = ctx.corpus or {}
            for key in ("deployment_name", "deployment_dir", "dsn"):
                if not corpus.get(key):
                    raise ArmDriverError(
                        f"okg arm requires a built corpus deployment: corpus.{key} "
                        "missing; run `okg-bench corpus build` first"
                    )
        if shutil.which(ctx.claude_bin) is None:
            raise ArmDriverError(
                f"claude binary {ctx.claude_bin!r} not on PATH; the default arms "
                "run headless Claude Code under the operator's subscription"
            )
        if self.mode == "native":
            config_dir = ctx.run_dir / "claude-config"
            _init_config_dir(config_dir)
            ctx.state["config_dir"] = config_dir
        if self.mode == "okg":
            config_dir = ctx.run_dir / "claude-config"
            _init_config_dir(config_dir)
            ctx.state["config_dir"] = config_dir
            # One persistent HTTP MCP server for the whole run: a per-session
            # stdio server boots too slowly (cold `uv run` import) and the
            # session starts with the server still `pending` — the arm would
            # silently run WITHOUT the graph (observed: 0 mcp__okg calls).
            self._start_mcp_server(ctx)

    def teardown_run(self, ctx: RunContext) -> None:
        proc = ctx.state.pop("mcp_server_proc", None)
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
        # Run dirs persist (transcripts, records); the seeded credential
        # copies must not. Scrub every config dir under this run.
        for cred in ctx.run_dir.glob("task-configs/*/.credentials.json"):
            cred.unlink(missing_ok=True)
        (ctx.run_dir / "claude-config" / ".credentials.json").unlink(missing_ok=True)

    def _start_mcp_server(self, ctx: RunContext) -> None:
        import socket
        import time as _time

        assert ctx.corpus is not None
        corpus = ctx.corpus
        okg_repo_root = Path(__file__).resolve().parents[5]
        if not (okg_repo_root / "pyproject.toml").exists():
            raise ArmDriverError(
                f"cannot locate the okg repo root from {__file__} "
                f"(tried {okg_repo_root})"
            )
        port = corpus.get("mcp_port")
        if not port:
            with socket.socket() as s:
                s.bind(("127.0.0.1", 0))
                port = s.getsockname()[1]
        log_path = ctx.run_dir / "mcp-server.log"
        env = dict(os.environ)
        env["OKG_DSN"] = corpus["dsn"]
        env["OKG_DEPLOYMENTS_DIR"] = str(Path(corpus["deployment_dir"]).parent)
        env["OKG_MCP_TOOL_ALLOWLIST"] = _OPERATOR_SURFACE
        with open(log_path, "ab") as log:
            proc = subprocess.Popen(
                [
                    "uv", "run", "--project", str(okg_repo_root), "--extra", "mcp",
                    "okg", "mcp-serve",
                    "--deployment", corpus["deployment_name"],
                    "--transport", "streamable-http",
                    "--host", "127.0.0.1", "--port", str(port),
                ],
                stdout=log,
                stderr=log,
                env=env,
                cwd=str(okg_repo_root),
            )
        deadline = _time.monotonic() + 180
        while _time.monotonic() < deadline:
            if proc.poll() is not None:
                tail = log_path.read_bytes()[-800:].decode("utf-8", "replace")
                raise ArmDriverError(
                    f"corpus MCP server exited {proc.returncode} during startup; "
                    f"log tail:\n{tail}"
                )
            try:
                with socket.create_connection(("127.0.0.1", int(port)), timeout=1):
                    break
            except OSError:
                _time.sleep(1.0)
        else:
            proc.terminate()
            raise ArmDriverError(
                f"corpus MCP server did not accept connections on port {port} "
                f"within 180s; see {log_path}"
            )
        mcp_config_path = ctx.run_dir / "mcp-config.json"
        mcp_config_path.write_text(
            json.dumps(
                {"mcpServers": {"okg": {"type": "http",
                                         "url": f"http://127.0.0.1:{port}/mcp"}}}
            ),
            encoding="utf-8",
        )
        ctx.state["mcp_server_proc"] = proc
        ctx.state["mcp_config_path"] = mcp_config_path

    # -- per task -------------------------------------------------------
    def run_task(
        self, task: TaskInstance, ctx: RunContext, workdir: Path
    ) -> TaskResult:
        if self.mode == "fresh":
            config_dir = ctx.run_dir / "task-configs" / _slug(task.instance_id)
            _init_config_dir(config_dir)
        else:
            config_dir = ctx.state["config_dir"]

        argv = self._build_argv(ctx)
        for extra_dir in task.extra.get("add_dirs", []):
            if not Path(extra_dir).is_dir():
                raise ArmDriverError(
                    f"task {task.instance_id} mounts missing dir {extra_dir!r}"
                )
            argv += ["--add-dir", str(extra_dir)]
        env = dict(os.environ)
        env["CLAUDE_CONFIG_DIR"] = str(config_dir)
        env.pop("ANTHROPIC_API_KEY", None)  # subscription auth only, never metered

        events_path = (
            ctx.run_dir / "transcripts" / f"{_slug(task.instance_id)}.stream.jsonl"
        )
        events_path.parent.mkdir(parents=True, exist_ok=True)

        start = time.monotonic()
        try:
            proc = subprocess.run(
                argv,
                input=task.prompt,
                capture_output=True,
                text=True,
                cwd=str(workdir),
                env=env,
                timeout=ctx.task_timeout_s,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            wall = time.monotonic() - start
            stdout = _decode(exc.stdout)
            events_path.write_text(stdout, encoding="utf-8")
            metrics, answer, session_id, _ = _parse_stream(stdout, wall)
            return TaskResult(
                instance_id=task.instance_id,
                arm=self.name,
                status="timeout",
                answer=answer,
                metrics=metrics,
                session_id=session_id,
                error=f"task timeout after {ctx.task_timeout_s}s",
                detail={"workdir": str(workdir), "config_dir": str(config_dir)},
            )
        wall = time.monotonic() - start
        events_path.write_text(proc.stdout, encoding="utf-8")
        metrics, answer, session_id, result_event = _parse_stream(proc.stdout, wall)
        if proc.returncode != 0 and result_event is None:
            return TaskResult(
                instance_id=task.instance_id,
                arm=self.name,
                status="error",
                answer=answer,
                metrics=metrics,
                session_id=session_id,
                error=(
                    f"claude exited {proc.returncode}: "
                    f"{proc.stderr.strip()[-800:] or '<no stderr>'}"
                ),
                detail={"workdir": str(workdir), "config_dir": str(config_dir)},
            )
        status = "completed"
        error = None
        if result_event is not None and result_event.get("is_error"):
            status = "error"
            error = str(result_event.get("subtype") or "result.is_error")
        return TaskResult(
            instance_id=task.instance_id,
            arm=self.name,
            status=status,
            answer=answer,
            metrics=metrics,
            session_id=session_id,
            error=error,
            detail={"workdir": str(workdir), "config_dir": str(config_dir)},
        )

    def between_tasks(
        self, task: TaskInstance, result: TaskResult, ctx: RunContext
    ) -> dict[str, Any]:
        if self.mode != "okg":
            return {}
        from .. import corpus  # lazy: pulls substrate machinery

        if ctx.corpus is None:
            raise ArmDriverError("okg arm lost its corpus config mid-run")
        generation_id = corpus.ingest_sessions_and_publish(
            corpus_cfg=ctx.corpus,
            sessions_root=Path(ctx.state["config_dir"]) / "projects",
        )
        return {"published_generation": generation_id}

    # -- helpers --------------------------------------------------------
    def _build_argv(self, ctx: RunContext) -> list[str]:
        argv = [
            ctx.claude_bin,
            "-p",  # prompt arrives on stdin
            "--output-format", "stream-json",
            "--verbose",
            "--model", ctx.model,
            "--permission-mode", ctx.permission_mode,
            # Corpus-validity rule, enforced at the TOOL level (a prompt
            # request alone is not enforcement): no arm may consult the live
            # web — snapshots and repos are the only knowledge surface, and
            # FreshQA's freshness claim is meaningless otherwise.
            "--disallowedTools", "WebSearch", "WebFetch",
        ]
        if self.mode in ("fresh", "okg"):
            # No user/project settings: the isolated config dir plus empty
            # setting sources means no CLAUDE.md discovery, no auto-memory
            # carryover. (The task repo's own in-repo files remain equally
            # visible to every arm — that is repo content, not memory.)
            argv += ["--setting-sources", ""]
        else:  # native: ordinary affordances from the run-scoped config dir
            argv += ["--setting-sources", "user,project,local"]
        if self.mode == "okg":
            mcp_config = ctx.state.get("mcp_config_path")
            if not mcp_config:
                raise ArmDriverError(
                    "okg arm has no run MCP config; setup_run did not start the "
                    "corpus MCP server"
                )
            argv += ["--mcp-config", str(mcp_config), "--strict-mcp-config"]
        return argv


# -- native-arm workdir policy (used by the runner) ---------------------


def reset_shared_workdir(workdir: Path, base_commit: str) -> None:
    """shared-workdir mode: park the tree at the next task's base commit,
    dropping all task edits but PRESERVING the memory files."""
    keep: dict[str, bytes] = {}
    claude_dir = workdir / ".claude"
    md = workdir / "CLAUDE.md"
    if md.exists():
        keep["CLAUDE.md"] = md.read_bytes()
    _git(workdir, "checkout", "-f", "-B", "main", base_commit)
    excludes: list[str] = []
    for p in MEMORY_PATHS:
        excludes += ["-e", p]
    _git(workdir, "clean", "-fdx", *excludes)
    if "CLAUDE.md" in keep and not md.exists():
        md.write_bytes(keep["CLAUDE.md"])
    del claude_dir  # preserved by the clean excludes when present


def carry_memory_forward(prev_workdir: Path, next_workdir: Path) -> list[str]:
    """carry-forward mode: copy the memory files into the next task's fresh
    workdir. Returns what was carried (for the task record)."""
    carried: list[str] = []
    for name in MEMORY_PATHS:
        src = prev_workdir / name
        if not src.exists():
            continue
        dst = next_workdir / name
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            dst.write_bytes(src.read_bytes())
        carried.append(name)
    return carried


def _git(workdir: Path, *args: str) -> None:
    proc = subprocess.run(
        ["git", "-C", str(workdir), *args], capture_output=True, text=True, check=False
    )
    if proc.returncode != 0:
        raise ArmDriverError(
            f"git {' '.join(args)} failed in {workdir}: {proc.stderr.strip()}"
        )


def _init_config_dir(config_dir: Path) -> None:
    config_dir.mkdir(parents=True, exist_ok=True)
    config_dir.chmod(0o700)
    _seed_credentials(config_dir)
    _seed_login_state(config_dir)


def _seed_credentials(config_dir: Path) -> None:
    """Make the operator's subscription OAuth available inside the isolated
    CLAUDE_CONFIG_DIR. The macOS keychain item is suffixed per config dir
    (Claude Code-credentials-<hash>), so an isolated dir cannot see the
    default item — export it to the file-based store the CLI also accepts.
    No credential found anywhere = loud failure (never a silent metered
    fallback; there is nothing to fall back to)."""
    dst = config_dir / ".credentials.json"
    if dst.exists():
        return
    file_src = Path.home() / ".claude" / ".credentials.json"
    if file_src.exists():
        dst.write_bytes(file_src.read_bytes())
        dst.chmod(0o600)
        return
    proc = subprocess.run(
        ["security", "find-generic-password", "-s", "Claude Code-credentials", "-w"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode == 0 and proc.stdout.strip():
        dst.write_text(proc.stdout.strip(), encoding="utf-8")
        dst.chmod(0o600)
        return
    raise ArmDriverError(
        "no Claude Code subscription credential found: neither "
        f"{file_src} nor the 'Claude Code-credentials' keychain item exists; "
        "run `claude` interactively once and /login"
    )


def _seed_login_state(config_dir: Path) -> None:
    """The logged-in marker (oauthAccount) lives in ~/.claude.json; an
    isolated config dir needs a minimal copy or the CLI reports
    'Not logged in' despite valid credentials."""
    dst = config_dir / ".claude.json"
    if dst.exists():
        return
    src = Path.home() / ".claude.json"
    if not src.exists():
        raise ArmDriverError(
            f"{src} not found; run `claude` interactively once and /login"
        )
    real = json.loads(src.read_text(encoding="utf-8"))
    seed = {
        k: real[k]
        for k in ("oauthAccount", "hasCompletedOnboarding", "userID")
        if k in real
    }
    if "oauthAccount" not in seed:
        raise ArmDriverError(
            f"{src} has no oauthAccount — the operator is not logged in to a "
            "subscription; run /login"
        )
    dst.write_text(json.dumps(seed), encoding="utf-8")
    dst.chmod(0o600)


def _slug(instance_id: str) -> str:
    return instance_id.replace("/", "__")


def _decode(raw: str | bytes | None) -> str:
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return raw


def _parse_stream(
    stdout: str, wall_s: float
) -> tuple[TaskMetrics, str, str | None, dict[str, Any] | None]:
    """Parse stream-json events into (metrics, final answer, session id,
    result event). Tool calls are counted from assistant tool_use blocks;
    token usage comes from the final result event's own accounting."""
    tool_calls = 0
    answer = ""
    session_id: str | None = None
    result_event: dict[str, Any] | None = None
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue  # partial line from a timeout kill; the tail is lost
        etype = event.get("type")
        if etype == "assistant":
            content = (event.get("message") or {}).get("content") or []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "tool_use":
                    tool_calls += 1
        elif etype == "result":
            result_event = event
            session_id = event.get("session_id") or session_id
            if isinstance(event.get("result"), str):
                answer = event["result"]
        if event.get("session_id") and session_id is None:
            session_id = event["session_id"]
    usage = (result_event or {}).get("usage") or {}
    metrics = TaskMetrics(
        input_tokens=int(usage.get("input_tokens", 0)),
        output_tokens=int(usage.get("output_tokens", 0)),
        cache_read_tokens=int(usage.get("cache_read_input_tokens", 0)),
        cache_creation_tokens=int(usage.get("cache_creation_input_tokens", 0)),
        tool_calls=tool_calls,
        wall_s=wall_s,
        num_turns=(result_event or {}).get("num_turns"),
        cost_usd=(result_event or {}).get("total_cost_usd"),
    )
    return metrics, answer, session_id, result_event
