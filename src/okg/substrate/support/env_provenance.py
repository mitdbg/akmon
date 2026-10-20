"""Interpreter/environment provenance for optional-dependency diagnostics.

GitHub issue #639: an operator ran the documented ``uv run --extra code
--extra quality okg ...`` from a checkout whose ``.venv`` lacked the ``okg``
console script; uv fell through to PATH and silently executed the globally
installed uv *tool* shim (``~/.local/bin/okg`` ->
``~/.local/share/uv/tools/okg/...``) WITHOUT the extras. The resulting
``TreeSitterNotInstalled`` never named the interpreter that lacked the
package, so the wrong-interpreter trap was indistinguishable from a missing
install.

This module is the single shared classifier for "which Python environment is
this process actually running in":

* ``uv tool environment`` — ``sys.executable`` lives under a uv tools
  directory. uv installs tools into ``$UV_TOOL_DIR`` when set, else
  ``$XDG_DATA_HOME/uv/tools``, else ``~/.local/share/uv/tools`` (POSIX,
  including macOS) / ``%APPDATA%\\uv\\tools`` (Windows) — every default
  layout contains an adjacent ``uv/tools`` path-segment pair.
* ``project venv`` — a virtual environment (``sys.prefix !=
  sys.base_prefix``) that is not a uv tools path. Note: ``uvx`` ephemeral
  environments live under the uv *cache* dir and classify here; the
  persistent-shim trap of #639 is specifically the tools dir.
* ``system interpreter`` — everything else.

The real PATH-fallthrough of #639 is not reproducible in-repo (this repo's
pyproject has shipped the ``okg`` entry point since the initial commit), so
tests simulate the shim classification via monkeypatched ``sys.executable``.

Stdlib-only on purpose: it is imported from extractor/source error paths and
from the deployment env doctor, and must never itself fail from a missing
optional dependency.
"""
from __future__ import annotations

import importlib.metadata
import os
import sys
from pathlib import Path, PurePath
from typing import Any, Mapping, Sequence


UV_TOOL_ENVIRONMENT = "uv tool environment"
PROJECT_VENV = "project venv"
SYSTEM_INTERPRETER = "system interpreter"

#: The repo-authoritative invocation, per pact/project.yaml
#: ``command_authority.repo_command``.
REPO_COMMAND = "uv run okg"


def classify_interpreter(
    executable: str | None = None,
    *,
    prefix: str | None = None,
    base_prefix: str | None = None,
    env: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    """Classify the running Python environment.

    Returns ``(executable, kind)`` where ``kind`` is one of
    :data:`UV_TOOL_ENVIRONMENT`, :data:`PROJECT_VENV`, or
    :data:`SYSTEM_INTERPRETER`. All parameters default to the live
    ``sys``/``os.environ`` values so monkeypatching ``sys.executable`` (etc.)
    is picked up at call time.
    """
    executable = sys.executable if executable is None else executable
    prefix = sys.prefix if prefix is None else prefix
    base_prefix = sys.base_prefix if base_prefix is None else base_prefix
    env = os.environ if env is None else env
    if _is_uv_tool_executable(executable, env):
        return executable, UV_TOOL_ENVIRONMENT
    if prefix != base_prefix:
        return executable, PROJECT_VENV
    return executable, SYSTEM_INTERPRETER


def annotate_optional_dependency_error(
    message: str,
    fix_hint: str | None,
    *,
    extras: Sequence[str] = (),
    package: str | None = None,
) -> tuple[str, str | None, dict[str, str]]:
    """Augment an optional-dependency error with interpreter provenance.

    Returns ``(message, fix_hint, data)``:

    * ``message`` always gains the interpreter path and classified kind, so
      wrong-interpreter runs are distinguishable from missing installs.
    * When the interpreter is a uv tool environment, ``fix_hint`` is
      rewritten to say the command resolved to the globally installed tool
      (not the checkout venv) and to give BOTH fix forms — reinstall the
      tool with the extras (``uv tool install "okg[...]"``) and run from a
      checkout (``uv run --extra ... okg ...``). The original hint is kept
      at the end. Otherwise the caller's hint passes through unchanged.
    * ``data`` carries ``interpreter`` / ``environment_kind`` for the JSON
      error envelope; merge it into the error's ``data`` payload.

    ``extras`` names the okg extras that provide the missing dependency;
    ``package`` names a bare PyPI package for dependencies that are not an
    okg extra (e.g. per-language tree-sitter grammars).
    """
    executable, kind = classify_interpreter()
    annotated = f"{message} [running interpreter: {executable}; {kind}]"
    data = {"interpreter": executable, "environment_kind": kind}
    if kind != UV_TOOL_ENVIRONMENT:
        return annotated, fix_hint, data

    head = (
        "this okg command resolved to the globally installed uv tool "
        "environment, not the checkout venv, so extras installed for the "
        "checkout (`uv run --extra ...`) did not apply to it."
    )
    if extras:
        bracket = ",".join(extras)
        flags = " ".join(f"--extra {name}" for name in extras)
        fixes = (
            f' Fix either way: reinstall the global tool with the extras '
            f'(`uv tool install "okg[{bracket}]"`), or run from a checkout '
            f"(`uv run {flags} okg ...`)."
        )
    elif package:
        fixes = (
            f" Fix either way: add the package to the global tool "
            f"(`uv tool install okg --with {package}`), or run from a "
            f"checkout (`uv run okg ...`) with {package} installed in the "
            f"checkout venv."
        )
    else:
        fixes = (
            " Fix either way: install the missing dependency into that tool "
            "environment, or run from a checkout (`uv run okg ...`)."
        )
    tail = f" Original hint: {fix_hint}" if fix_hint else ""
    return annotated, head + fixes + tail, data


def interpreter_provenance_report(
    *,
    executable: str | None = None,
    prefix: str | None = None,
    base_prefix: str | None = None,
    env: Mapping[str, str] | None = None,
    cwd: str | Path | None = None,
) -> dict[str, Any]:
    """The interpreter-provenance block for ``okg deployment env doctor``.

    Reports which environment the doctor itself is running from — the
    surface built for environment confusion (issue-255) gains the one fact
    that exposes the #639 shim-vs-venv trap before a source fails.
    """
    resolved_executable, kind = classify_interpreter(
        executable,
        prefix=prefix,
        base_prefix=base_prefix,
        env=env,
    )
    return {
        "executable": resolved_executable,
        "environment_kind": kind,
        "okg_version": okg_version(),
        "repo_command_authority": repo_command_authority(
            resolved_executable,
            cwd=cwd,
        ),
    }


def repo_command_authority(
    executable: str | None = None,
    *,
    cwd: str | Path | None = None,
) -> dict[str, Any]:
    """Whether this invocation matches the repo command authority.

    Heuristic: when run from inside an okg checkout, the repo-authoritative
    command (:data:`REPO_COMMAND`, per pact/project.yaml) executes the
    checkout's ``.venv`` interpreter. ``matches`` is ``None`` when not run
    from a checkout (there is no authority to match), otherwise whether
    ``executable`` lives under ``<checkout>/.venv``.
    """
    executable = sys.executable if executable is None else executable
    root = find_checkout_root(cwd)
    if root is None:
        return {
            "expected_command": REPO_COMMAND,
            "in_checkout": False,
            "checkout_root": None,
            "checkout_venv": None,
            "matches": None,
        }
    venv_dir = root / ".venv"
    matches = _is_within(executable, venv_dir)
    return {
        "expected_command": REPO_COMMAND,
        "in_checkout": True,
        "checkout_root": str(root),
        "checkout_venv": str(venv_dir),
        "matches": matches,
    }


def find_checkout_root(cwd: str | Path | None = None) -> Path | None:
    """Locate the enclosing okg checkout, if any.

    Markers, per directory walking upward from ``cwd``: the PACT settings
    file ``pact/project.yaml`` (which declares the repo command authority),
    or a ``pyproject.toml`` next to ``src/okg`` (worktrees and vendored
    checkouts).
    """
    start = Path.cwd() if cwd is None else Path(cwd)
    try:
        start = start.resolve()
    except OSError:  # pragma: no cover - unresolvable cwd
        return None
    for candidate in (start, *start.parents):
        if (candidate / "pact" / "project.yaml").is_file():
            return candidate
        if (
            (candidate / "pyproject.toml").is_file()
            and (candidate / "src" / "okg").is_dir()
        ):
            return candidate
    return None


def okg_version() -> str | None:
    """The installed okg package version, or None when unresolvable."""
    try:
        return importlib.metadata.version("okg")
    except importlib.metadata.PackageNotFoundError:
        return None


def _is_uv_tool_executable(executable: str, env: Mapping[str, str]) -> bool:
    """True when ``executable`` lives inside a uv tools directory.

    Detection order: containment in ``$UV_TOOL_DIR`` when set, then the
    adjacent ``uv/tools`` path-segment pair every default uv layout uses
    (``~/.local/share/uv/tools``, ``$XDG_DATA_HOME/uv/tools``,
    ``%APPDATA%\\uv\\tools``).
    """
    exe = PurePath(executable)
    tool_dir = env.get("UV_TOOL_DIR")
    if tool_dir:
        try:
            if exe.is_relative_to(PurePath(tool_dir)):
                return True
        except ValueError:  # pragma: no cover - mixed path flavors
            pass
    parts = [part.lower() for part in exe.parts]
    return any(
        parts[i] == "uv" and parts[i + 1] == "tools"
        for i in range(len(parts) - 1)
    )


def _is_within(executable: str, directory: Path) -> bool:
    """Pure-path containment check tolerant of unresolvable fakes."""
    try:
        exe = Path(executable)
        if exe.is_relative_to(directory):
            return True
        # Resolve both sides so symlinked checkouts (e.g. /tmp vs
        # /private/tmp on macOS) still match. resolve() is safe on
        # nonexistent paths — it only normalizes.
        return exe.resolve().is_relative_to(directory.resolve())
    except (OSError, ValueError):  # pragma: no cover - defensive
        return False


__all__ = [
    "PROJECT_VENV",
    "REPO_COMMAND",
    "SYSTEM_INTERPRETER",
    "UV_TOOL_ENVIRONMENT",
    "annotate_optional_dependency_error",
    "classify_interpreter",
    "find_checkout_root",
    "interpreter_provenance_report",
    "okg_version",
    "repo_command_authority",
]
