"""Agent-runtime registry — the list of known runtimes and the
`register_with_all_detected` entry point used by `okg install`.

Per `dead-simple-install`: install iterates `KNOWN_RUNTIMES`,
detects each, registers with the ones whose config files are
present. The aggregate `RegisterReport` is honest about each
runtime's outcome (registered / not_detected / failed) — no
silent skipping.

Adding a new runtime is one new module under
`okg.substrate.library.agent_runtimes` + one append to
`KNOWN_RUNTIMES` below.
"""
from __future__ import annotations

from . import (
    AgentRuntime,
    EphemeralOkgInterpreterError,
    RegisterReport,
    RegisterResult,
)
from .claude_code import ClaudeCodeRuntime
from .claude_desktop import ClaudeDesktopRuntime
from .codex_cli import CodexCliRuntime
from .cursor import CursorRuntime


# The order here is the order they'll be queried + reported.
# Doesn't affect correctness — the recency-based primary-agent
# heuristic re-sorts by config mtime — but stable iteration
# order matters for tests and reproducible install reports.
KNOWN_RUNTIMES: list[AgentRuntime] = [
    ClaudeDesktopRuntime(),
    # Claude Code (the CLI) — the runtime most likely to exist on
    # Linux, where Claude Desktop doesn't ship. Per the
    # 2026-06-09 PI first-install: zero runtimes detected on a
    # Linux workstation.
    ClaudeCodeRuntime(),
    CodexCliRuntime(),
    CursorRuntime(),
]


def register_with_all_detected(
    *,
    deployment: str,
    dsn: str,
    runtimes: list[AgentRuntime] | None = None,
    extra_env: dict | None = None,
) -> RegisterReport:
    """Register `deployment` with every detected runtime in
    `KNOWN_RUNTIMES` (or `runtimes` if provided for testing).

    Each runtime's `register()` method is called unconditionally;
    runtimes whose config files are absent return a
    `not_detected` result, ones with a config file but a write
    failure return `failed`. No exceptions propagate.

    `extra_env` (PACT least-privilege-mcp) is interpreted by each
    runtime's profiled launcher entry builder. Non-secret capability
    flags such as `{"OKG_MCP_ALLOW_WRITES": "1"}` are translated into
    launcher/MCP command shape instead of being persisted as ambient
    credential-bearing env.
    """
    runtimes = runtimes if runtimes is not None else KNOWN_RUNTIMES
    report = RegisterReport()
    for rt in runtimes:
        try:
            result = rt.register(
                deployment=deployment, dsn=dsn, extra_env=extra_env,
            )
        except EphemeralOkgInterpreterError as exc:
            # A detected runtime that would get a command uv can delete:
            # report it failed, with the install fix, and write nothing.
            result = RegisterResult(
                runtime=rt.name,
                status="failed",
                config_path=rt.config_path(),
                error=str(exc),
            )
        report.results.append(result)
    return report


def unregister_from_all(
    *,
    deployment: str,
    runtimes: list[AgentRuntime] | None = None,
) -> list[str]:
    """Remove `deployment` from every runtime that has it.
    Returns the list of runtime names that actually had an
    entry removed (useful for `okg reset` reporting).
    """
    runtimes = runtimes if runtimes is not None else KNOWN_RUNTIMES
    removed: list[str] = []
    for rt in runtimes:
        try:
            if rt.unregister(deployment):
                removed.append(rt.name)
        except OSError:
            # Best-effort cleanup; an unwritable config doesn't
            # block reset.
            pass
    return removed
