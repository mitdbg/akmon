"""Run one okg command in a fresh process with a timer on each start-up step.

Used by `test_read_operator_commands_db.py` to measure where a one-shot read
command spends its time (PACT read-commands-trim-3666). Usage:

    python -P _read_phase_probe.py <okg argv...>

The command runs exactly as `okg <argv...>` does, through
`okg.substrate.cli.main.main`; each step below is wrapped, not replaced, so
the reply, stderr and exit code are the command's own. The times are written
as one JSON object to the file `OKG_READ_PHASE_OUT` names:
`{"rc": <exit code>, "total": <seconds>, "phases": {label: [seconds,
count]}, "dbos_loaded": <bool>}`. A step that did not run is absent. Steps
nest: `build` holds the steps listed under it. `probe set-up` is the import
of the modules the probe wraps before the command starts; in a real command
that time falls inside the step that first imports each.
"""
from __future__ import annotations

import time

STARTED = time.perf_counter()

import functools  # noqa: E402
import importlib  # noqa: E402
import inspect  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import sys  # noqa: E402
from typing import Any  # noqa: E402

OUT_ENV = "OKG_READ_PHASE_OUT"

_TOTALS: dict[str, list[float]] = {}


def _add(label: str, seconds: float) -> None:
    total = _TOTALS.setdefault(label, [0.0, 0])
    total[0] += seconds
    total[1] += 1


def _timed(label: str, fn: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def timed_async(*args: Any, **kwargs: Any) -> Any:
            started = time.perf_counter()
            try:
                return await fn(*args, **kwargs)
            finally:
                _add(label, time.perf_counter() - started)

        return timed_async

    @functools.wraps(fn)
    def timed(*args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter()
        try:
            return fn(*args, **kwargs)
        finally:
            _add(label, time.perf_counter() - started)

    return timed


class _TimedSession:
    """An async context manager whose enter and exit are timed apart."""

    def __init__(self, manager: Any) -> None:
        self._manager = manager

    async def __aenter__(self) -> Any:
        started = time.perf_counter()
        try:
            return await self._manager.__aenter__()
        finally:
            _add("session open", time.perf_counter() - started)

    async def __aexit__(self, *exc: Any) -> Any:
        started = time.perf_counter()
        try:
            return await self._manager.__aexit__(*exc)
        finally:
            _add("session close", time.perf_counter() - started)


#: (module, attribute, label). A class attribute is `Class.method`.
STEPS: tuple[tuple[str, str, str], ...] = (
    ("okg.substrate.cli._read_operators", "operator_arguments", "arguments"),
    ("okg.substrate.cli._read_operators", "search_operator_arguments", "arguments"),
    ("okg.substrate.cli._read_operators", "build_read_server", "build"),
    ("okg.substrate.cli.dsn_authority", "resolve_read_dsn", "build: dsn"),
    ("okg.substrate.db.readiness", "require_database_ready", "build: database ready"),
    ("okg.substrate.db.readiness", "require_mcp_read_dsn_usable", "build: read dsn"),
    ("okg.substrate.local_database_roles", "require_role_dsn_usable", "build: read role"),
    ("okg.substrate.deployment_release", "enforce_release_contract_if_declared",
     "build: release verdict"),
    ("okg.substrate.cli.main", "_report_database_behind", "build: behind note"),
    ("okg.substrate.cli.main", "_mcp_serve_embedding_plan", "build: embedding plan"),
    ("okg.substrate.mcp.server", "build_server", "build: server"),
    ("okg.substrate.mcp.server", "_load_readside_metadata", "server: deployment metadata"),
    ("okg.substrate.mcp.upload_admin", "conditional_tool_provisioning",
     "server: upload-tool check"),
    ("okg.substrate.mcp.server", "mcp_surface_report", "server: surface report"),
    ("okg.substrate.db.capacity", "reserve_process_demand", "server: connection claim"),
    ("okg.substrate.mcp.server", "_build_pool", "server: pool open"),
    ("okg.substrate.mcp.server", "probe_capabilities_from_pool", "server: capability probe"),
    ("okg.substrate.mcp.server", "_gather_dataset_facts_for_start",
     "server: dataset card facts"),
    ("okg.substrate.mcp.server", "_serve_dataset_card", "server: dataset card render"),
    ("mcp.server.fastmcp.tools.base", "Tool.from_function", "server: tool registration"),
    ("okg.substrate.mcp.server", "register_core_read_tools", "server: read tool wrappers"),
    ("okg.substrate.mcp.server", "compile_tool_surface", "server: published schemas"),
    ("okg.substrate.mcp.server", "publish_narrowed_inspect_target", "server: published schemas"),
    ("okg.substrate.mcp.server", "compact_tool_schemas", "server: published schemas"),
    ("okg.substrate.mcp.server", "publish_client_readable_schemas", "server: published schemas"),
    ("okg.substrate.mcp.server", "publish_clean_descriptions", "server: published schemas"),
    ("okg.substrate.mcp.server", "publish_surface_descriptions", "server: published schemas"),
    ("okg.substrate.cli._read_operators", "call_operator", "call"),
    ("okg.substrate.mcp.response_bounds", "ByteBoundedFastMCP.call_tool", "call: tool"),
    ("okg.substrate.mcp.response_bounds", "ByteBoundedFastMCP.list_tools", "call: tools/list"),
    ("okg.substrate.cli._read_operators", "close_server", "close"),
    ("okg.substrate.cli._read_operators", "print_reply", "print"),
)


def _install() -> None:
    for module_name, attribute, label in STEPS:
        started = time.perf_counter()
        loaded = module_name in sys.modules
        module = importlib.import_module(module_name)
        if not loaded:
            _add(f"probe set-up: import {module_name}", time.perf_counter() - started)
        owner_name, _, name = attribute.rpartition(".")
        owner = getattr(module, owner_name) if owner_name else module
        original = inspect.getattr_static(owner, name, None)
        if original is None:
            continue  # a step this version of the code does not have
        if isinstance(original, classmethod):
            bound = getattr(owner, name)
            setattr(owner, name, classmethod(
                lambda cls, *a, _f=_timed(label, bound), **k: _f(*a, **k)
            ))
        else:
            setattr(owner, name, _timed(label, getattr(owner, name)))
    memory = importlib.import_module("mcp.shared.memory")
    connected = memory.create_connected_server_and_client_session

    def timed_session(*args: Any, **kwargs: Any) -> Any:
        return _TimedSession(connected(*args, **kwargs))

    memory.create_connected_server_and_client_session = timed_session


def _exit_mark(label: str) -> Any:
    """A hook that appends `<label> <seconds since the probe started>` to
    `<OKG_READ_PHASE_OUT>.exit`, to time the steps of the exit."""

    def mark() -> None:
        with open(os.environ[OUT_ENV] + ".exit", "a", encoding="utf-8") as handle:
            handle.write(f"{label} {time.perf_counter() - STARTED:.4f}\n")
        if label == "atexit_done" and os.environ.get(HARD_EXIT_ENV) is not None:
            # Measurement only: end the process here, as if the interpreter's
            # finalization (the last garbage collection and the teardown of
            # every module) took no time.
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(int(os.environ[HARD_EXIT_ENV]))

    return mark


#: Measurement only: set to the expected exit code to skip the
#: interpreter's finalization after the last atexit handler.
HARD_EXIT_ENV = "OKG_READ_PHASE_HARD_EXIT"


def main(argv: list[str]) -> int:
    import atexit
    import threading

    # Hooks run in the reverse order they were registered: these two first,
    # so they run last of their kind. Python's exit runs the thread exit
    # hooks (an executor's join), then joins the threads that are not
    # daemons, then runs the atexit handlers.
    atexit.register(_exit_mark("atexit_done"))
    threading._register_atexit(_exit_mark("thread_hooks_done"))  # type: ignore[attr-defined]
    try:
        return _main(argv)
    finally:
        # Registered last, so it runs first of the atexit handlers: after
        # the threads were joined.
        atexit.register(_exit_mark("threads_joined"))


def _main(argv: list[str]) -> int:
    started = time.perf_counter()
    import okg.substrate.cli.main as cli_main

    _add("import: okg command line", time.perf_counter() - started)
    started = time.perf_counter()
    import okg.substrate.mcp.server  # noqa: F401

    _add("import: read server", time.perf_counter() - started)
    started = time.perf_counter()
    _install()
    _add("probe set-up (not part of a command)", time.perf_counter() - started)
    started = time.perf_counter()
    try:
        rc = cli_main.main(argv)
    finally:
        _add("command", time.perf_counter() - started)
    sys.stdout.flush()
    sys.stderr.flush()
    with open(os.environ[OUT_ENV], "w", encoding="utf-8") as handle:
        json.dump({
            "rc": rc,
            "total": time.perf_counter() - STARTED,
            "phases": _TOTALS,
            # Whether the command loaded DBOS, which a read never needs.
            "dbos_loaded": "dbos" in sys.modules,
        }, handle)
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
