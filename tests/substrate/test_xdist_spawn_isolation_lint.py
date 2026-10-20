"""Enforcement lint: test-suite subprocess spawns must be session-isolated.

remediate-audit-2026-07 / task.audit-2026-07.enforce-lane-determinism (#703).

Root cause of the fast-linux nondeterminism: a test spawned a long-lived child
process (`http.server`, `npm run …:dev`) with `subprocess.Popen` but WITHOUT
`start_new_session=True`. Such a child lands in the pytest-xdist worker's own
session/process group and inherits the worker's stdin — which under xdist is the
execnet control channel to the controller. A signal delivered to (or by) that
child then reaches the worker group, surfacing as the intermittent
``<WorkerController gwN> received keyboard-interrupt`` that made the required
lane nondeterministic.

The durable fix is a convention, enforced here so the disease cannot regrow:
every ``subprocess.Popen`` in the test tree MUST pass ``start_new_session=True``
so the child leads its own process group and can never signal the runner.
``subprocess.run`` / ``check_output`` are exempt — they block and reap the child
inline, so no backgrounded child outlives the call holding the control channel.
"""
from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest
from tests.pytest.tracked_files import tracked_files


TESTS_ROOT = Path(__file__).resolve().parents[1]


def _call_is_popen(node: ast.Call) -> bool:
    """True if ``node`` calls ``subprocess.Popen`` or a bare ``Popen``."""
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr == "Popen"
    if isinstance(func, ast.Name):
        return func.id == "Popen"
    return False


def _has_new_session(node: ast.Call) -> bool:
    """True if the call passes ``start_new_session=True`` (constant) or
    forwards ``**kwargs`` we cannot introspect (conservatively allowed)."""
    for kw in node.keywords:
        if kw.arg is None:
            # ``**spawn_kwargs`` — cannot statically verify; allow.
            return True
        if kw.arg == "start_new_session":
            value = kw.value
            return isinstance(value, ast.Constant) and value.value is True
    return False


def unguarded_popen_sites(root: Path, paths: list[Path] | None = None) -> list[str]:
    """Return ``path:line`` for every Popen call in ``root`` lacking session isolation.

    ``paths`` narrows the scan to given files under ``root``; the repository
    scan passes the tracked files so a parallel test's scratch file cannot be
    listed and then vanish before it is read.
    """
    violations: list[str] = []
    for path in paths if paths is not None else sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _call_is_popen(node):
                if not _has_new_session(node):
                    rel = path.relative_to(root.parent)
                    violations.append(f"{rel}:{node.lineno}")
    return violations


def test_no_unguarded_subprocess_popen_in_test_tree() -> None:
    """Every test-suite ``subprocess.Popen`` isolates its session (#703)."""
    violations = unguarded_popen_sites(TESTS_ROOT, tracked_files(TESTS_ROOT, "*.py"))
    assert violations == [], (
        "subprocess.Popen without start_new_session=True can signal the "
        "pytest-xdist worker group and make a lane nondeterministic (#703). "
        "Add start_new_session=True (and detach stdio from the worker) at:\n  "
        + "\n  ".join(violations)
    )


# ---------------------------------------------------------------------------
# Failing-fixture proof: the scanner flags the banned pattern and passes the
# compliant one. Without this proof the lint is worthless.
# ---------------------------------------------------------------------------

_BANNED = '''
import subprocess
proc = subprocess.Popen(["sleep", "1"])
proc.wait()
'''

_COMPLIANT = '''
import subprocess
proc = subprocess.Popen(
    ["sleep", "1"],
    stdin=subprocess.DEVNULL,
    start_new_session=True,
)
proc.wait()
'''

_COMPLIANT_KWARGS = '''
import subprocess
proc = subprocess.Popen(["sleep", "1"], **spawn_kwargs)
'''

_EXEMPT_RUN = '''
import subprocess
subprocess.run(["true"], check=True)
'''


def _scan_source(source: str, tmp_path: Path) -> list[str]:
    module = tmp_path / "candidate.py"
    module.write_text(source, encoding="utf-8")
    return unguarded_popen_sites(tmp_path)


def test_scanner_flags_unguarded_popen(tmp_path: Path) -> None:
    assert _scan_source(_BANNED, tmp_path), "scanner must flag a Popen without start_new_session"


def test_scanner_passes_session_isolated_popen(tmp_path: Path) -> None:
    assert _scan_source(_COMPLIANT, tmp_path) == []


def test_scanner_allows_forwarded_kwargs(tmp_path: Path) -> None:
    assert _scan_source(_COMPLIANT_KWARGS, tmp_path) == []


def test_scanner_ignores_blocking_subprocess_run(tmp_path: Path) -> None:
    assert _scan_source(_EXEMPT_RUN, tmp_path) == []


@pytest.mark.parametrize("source", [_BANNED, _COMPLIANT])
def test_scanner_never_raises_on_valid_python(source: str, tmp_path: Path) -> None:
    _scan_source(source, tmp_path)  # smoke: no exception


@pytest.mark.skipif(
    not hasattr(os, "getpgid"), reason="process groups are POSIX-only"
)
def test_session_isolated_spawn_leaves_worker_group() -> None:
    """Runtime proof of the property the lint enforces (#703).

    A child spawned with ``start_new_session=True`` leads its OWN process group,
    so it is not a member of the pytest-xdist worker's group and no signal to
    that child's group can reach the worker. This is the exact behavior the
    supervision / dev-server spawns now rely on.
    """
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        assert os.getpgid(child.pid) != os.getpgid(0)
        assert os.getpgid(child.pid) == child.pid  # child is its own group leader
    finally:
        child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)
