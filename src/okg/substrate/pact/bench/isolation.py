"""Isolation contract for the PACT friction benchmark.

The benchmark measures today's PACT behavior. Any ambient state that changes
that behavior — a developer's live ``OKG_DSN`` flipping the ledger arm, a
global git identity changing the fixture's commit sha, a wall clock leaking
into evidence ids — would silently change the numbers. So the harness
REFUSES to run under unexpected environment rather than emit different
numbers, and pins everything it cannot refuse:

* env allowlist — :func:`require_clean_environment` names the offending vars;
* isolated ``HOME`` / ``TMPDIR`` / git config, so no user file is read;
* an injected clock, so evidence ids and ``attached_at`` stamps are fixed;
* pinned DSN resolution, so the ledger and graph arms are chosen by the
  probe, never by whatever database happens to be reachable.

Every seam patched here is a *resolution* choke point, not a result: the real
``_graph_projection_status`` / ``require_ledger_connection`` code still runs
and still produces its own payload. The harness decides only what those
functions are allowed to find.
"""
from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator, Mapping, MutableMapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError

# Vars that steer PACT away from the fixture: ledger/graph targets, principal
# identity, coordination state paths, and git plumbing that would relocate
# the fixture repo or restamp its commits.
DENIED_ENV_EXACT: frozenset[str] = frozenset(
    {
        "AGENT_SESSION_ID",
        "CLAUDE_SESSION_ID",
        "CODEX_SESSION_ID",
        "GIT_AUTHOR_DATE",
        "GIT_AUTHOR_EMAIL",
        "GIT_AUTHOR_NAME",
        "GIT_COMMITTER_DATE",
        "GIT_COMMITTER_EMAIL",
        "GIT_COMMITTER_NAME",
        "GIT_COMMON_DIR",
        "GIT_CONFIG",
        "GIT_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_WORK_TREE",
        "OKG_AGENT_SESSION_ID",
        "OKG_DEPLOYMENTS_DIR",
        "OKG_DSN",
        "OKG_ENABLE_LOCAL_ENV_FOR_TESTS",
        "OKG_PRINCIPAL",
        "OKG_PRINCIPAL_KIND",
        "OKG_TEST_DSN",
        "PACT_ACTOR",
    }
)

DENIED_ENV_PREFIXES: tuple[str, ...] = ("OKG_PACT_",)

# The one carve-out from the prefix rule. `OKG_PACT_DECISION_PROJECTION` is
# read only by the graph SOURCE projector (library/sources/pact.py), never by
# src/okg/substrate/pact/, so it cannot reach the measured path — and the
# repo-wide test conftest sets it on every test, which would otherwise make
# the harness refuse to run inside its own lane.
DENIED_ENV_PREFIX_EXCEPTIONS: frozenset[str] = frozenset(
    {"OKG_PACT_DECISION_PROJECTION"}
)

# Frozen fixture identity. The commit sha of a fixture repo is a function of
# its tree, its message, and these values; pinning them keeps the sha (and
# therefore every payload that quotes it) stable across machines.
FIXTURE_GIT_NAME = "pact-bench"
FIXTURE_GIT_EMAIL = "pact-bench@example.invalid"
FIXTURE_GIT_DATE = "2026-01-01T00:00:00+00:00"

FIXED_STAMP = "20260101T000000000000Z"
FIXED_NOW = "2026-01-01T00:00:00+00:00"

_LEDGER_WITHHELD_REASON = (
    "bench isolation: no decision-ledger DSN is offered to the harness"
)
_GRAPH_WITHHELD_REASON = (
    "bench isolation: no graph-projection DSN is offered to the harness"
)
_STUB_DSN = "postgres://pact-bench/stub"


class IsolationRefusal(SubstrateError):
    """The ambient environment would change the measured numbers."""


def denied_environment(env: Mapping[str, str] | None = None) -> list[str]:
    """Names of set env vars that would change what the harness measures."""
    source = os.environ if env is None else env
    hits = {
        name
        for name in source
        if name in DENIED_ENV_EXACT
        or (
            name not in DENIED_ENV_PREFIX_EXCEPTIONS
            and any(name.startswith(prefix) for prefix in DENIED_ENV_PREFIXES)
        )
    }
    return sorted(hits)


def require_clean_environment(env: Mapping[str, str] | None = None) -> None:
    """Refuse to run under environment the harness does not control."""
    offenders = denied_environment(env)
    if offenders:
        raise IsolationRefusal(
            "pact bench refuses to run: environment would change the measured "
            f"numbers ({', '.join(offenders)})",
            data={"offending_env": offenders},
            fix_hint=(
                "unset "
                + " ".join(offenders)
                + " (or run through tests/pact/bench, which applies the "
                "isolation contract)"
            ),
        )


def scrub_environment(env: MutableMapping[str, str] | None = None) -> list[str]:
    """Remove the denied vars; return what was removed.

    The pytest entry applies the isolation contract this way so the lane is
    reproducible on a developer machine with a live DSN exported. The removed
    names are returned rather than swallowed, and never enter ``metrics.json``
    (they would make the report machine-dependent).
    """
    target = os.environ if env is None else env
    offenders = denied_environment(target)
    for name in offenders:
        del target[name]
    return offenders


@dataclass(frozen=True)
class FixedClock:
    """The only clock the harness lets PACT see."""

    stamp_value: str = FIXED_STAMP
    now_value: str = FIXED_NOW

    def stamp(self) -> str:
        return self.stamp_value

    def now(self) -> str:
        return self.now_value


@dataclass
class RuntimeControls:
    """Per-probe authority arms, flipped by the caller between calls."""

    # "withheld" is the no-DSN reality the acceptance requires (runs with no
    # DSN). "fresh"/"missing" drive the graph-authority rows through the real
    # status logic with a stubbed projection row instead of a live graph.
    graph_projection: str = "withheld"
    graph_generation_id: int = 4242
    fixture_root: Path | None = None
    subprocess_calls: list[str] = field(default_factory=list)


# The controls the innermost `isolated_runtime` installed. Published so a
# harness that REPLAYS a call against a COPY of the fixture can repoint the
# stubbed projection at that copy: the stub reads the manifest off
# `controls.fixture_root`, so a replay against a copy was answered with the
# ORIGINAL manifest's state digest — the graph arm then judged an edit it could
# not see, and the repair looked like it changed nothing.
_ACTIVE: list[RuntimeControls] = []


def active_controls() -> RuntimeControls | None:
    return _ACTIVE[-1] if _ACTIVE else None


@contextlib.contextmanager
def graph_projection_as(arm: str | None) -> Iterator[RuntimeControls | None]:
    """Run under a named graph-projection arm for the duration.

    A call recorded while the projection was ``fresh`` must be REPLAYED under
    ``fresh``: replaying it under the ambient arm answered a pin-mismatch
    refusal with the missing-projection blockers instead, so the cause set
    changed for a reason that had nothing to do with the repair.
    """
    controls = active_controls()
    if controls is None or arm is None:
        yield controls
        return
    previous = controls.graph_projection
    controls.graph_projection = arm
    try:
        yield controls
    finally:
        controls.graph_projection = previous


@contextlib.contextmanager
def fixture_root_redirected(root: Path) -> Iterator[RuntimeControls | None]:
    """Point the projection stub at ``root`` for the duration.

    A no-op when no runtime is installed, so a caller outside
    :func:`isolated_runtime` behaves the same as before.
    """
    controls = active_controls()
    if controls is None:
        yield None
        return
    previous = controls.fixture_root
    controls.fixture_root = root
    try:
        yield controls
    finally:
        controls.fixture_root = previous


class _PatchStack:
    def __init__(self) -> None:
        self._undo: list[tuple[Any, str, Any]] = []

    def set(self, obj: Any, name: str, value: Any) -> None:
        self._undo.append((obj, name, getattr(obj, name)))
        setattr(obj, name, value)

    def unwind(self) -> None:
        while self._undo:
            obj, name, original = self._undo.pop()
            setattr(obj, name, original)


def _isolated_env(home: Path) -> dict[str, str]:
    tmp = home / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    return {
        "HOME": str(home),
        "TMPDIR": str(tmp),
        "XDG_CACHE_HOME": str(home / "cache"),
        "XDG_CONFIG_HOME": str(home / "config"),
        # /dev/null is git's documented "no config file" value; without it git
        # reads the developer's ~/.gitconfig and the fixture sha drifts.
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_AUTHOR_NAME": FIXTURE_GIT_NAME,
        "GIT_AUTHOR_EMAIL": FIXTURE_GIT_EMAIL,
        "GIT_AUTHOR_DATE": FIXTURE_GIT_DATE,
        "GIT_COMMITTER_NAME": FIXTURE_GIT_NAME,
        "GIT_COMMITTER_EMAIL": FIXTURE_GIT_EMAIL,
        "GIT_COMMITTER_DATE": FIXTURE_GIT_DATE,
        "PACT_ACTOR": "pact-bench",
    }


@contextlib.contextmanager
def isolated_env(home: Path) -> Iterator[dict[str, str]]:
    """Swap in the fixture HOME/tmp/git identity for the duration."""
    overrides = _isolated_env(home)
    saved = {name: os.environ.get(name) for name in overrides}
    os.environ.update(overrides)
    try:
        yield overrides
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _ledger_resolution(**_kwargs: Any) -> dict[str, Any]:
    return {
        "dsn": None,
        "deployment": "pact-bench",
        "reason": _LEDGER_WITHHELD_REASON,
        "next_action": "configure_okg_dsn",
        "no_dsn_configured": True,
        "authority": {"source": "pact-bench-isolation"},
        "target": None,
    }


@contextlib.contextmanager
def isolated_runtime(
    controls: RuntimeControls,
    *,
    clock: FixedClock | None = None,
) -> Iterator[RuntimeControls]:
    """Pin clock, ledger DSN, graph DSN, and bootstrap subprocesses.

    Nothing here fakes a *result*: the ledger still raises its own refusal,
    the graph status function still computes its own verdict. Only the inputs
    those functions are allowed to discover are pinned.
    """
    from okg.substrate.pact import cli as pact_cli
    from okg.substrate.pact import closure_intents, decisions, operations

    fixed = clock or FixedClock()
    stack = _PatchStack()

    def _graph_dsn_resolution(
        deployment: str,
        *,
        explicit_dsn: str | None = None,
    ) -> dict[str, Any]:
        del explicit_dsn
        if controls.graph_projection == "withheld":
            return {
                "dsn": None,
                "reason": _GRAPH_WITHHELD_REASON,
                "next_action": "configure_okg_dsn",
                "authority": {"source": "pact-bench-isolation"},
                "target": None,
                "deployment": deployment,
            }
        return {
            "dsn": _STUB_DSN,
            "authority": {"source": "pact-bench-isolation"},
            "target": deployment,
            "deployment": deployment,
        }

    def _graph_row(dsn: str, change_id: str) -> dict[str, Any] | None:
        del dsn
        if controls.graph_projection != "fresh":
            return None
        root = controls.fixture_root
        if root is None:
            raise SubstrateError("bench graph stub has no fixture root")
        from okg.substrate.pact.store import load_manifest, state_digest

        manifest = load_manifest(root, change_id)
        return {
            "projected_state_digest": state_digest(manifest),
            "projected_generation_id": controls.graph_generation_id,
        }

    def _no_subprocess(
        command: list[str],
        *,
        cwd: Path,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        del cwd, timeout_seconds
        text = " ".join(command)
        controls.subprocess_calls.append(text)
        return {
            "ok": False,
            "command": text,
            "returncode": None,
            "json": None,
            "stdout": "",
            "stderr": "",
            "error": "bench isolation: repo subprocesses are not run",
        }

    try:
        stack.set(operations, "_stamp", fixed.stamp)
        stack.set(operations, "_now", fixed.now)
        stack.set(closure_intents, "_now", fixed.now)
        stack.set(decisions, "resolve_ledger_dsn", _ledger_resolution)
        stack.set(pact_cli, "_graph_projection_dsn_resolution", _graph_dsn_resolution)
        stack.set(pact_cli, "_load_graph_projection_row", _graph_row)
        stack.set(pact_cli, "_bootstrap_run_json_command", _no_subprocess)
        _ACTIVE.append(controls)
        yield controls
    finally:
        if _ACTIVE and _ACTIVE[-1] is controls:
            _ACTIVE.pop()
        stack.unwind()
