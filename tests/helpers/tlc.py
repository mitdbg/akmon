"""Shared TLC discovery and launch-failure detection for model contract tests.

Tests that drive TLC directly (mutation guards, liveness witnesses) used to
hardcode ``<repo>/.tools/tla2tools.jar`` and gate only on ``java`` being on
PATH.  That path is gitignored, so in a fresh checkout the launch failed, the
``returncode != 0`` assertion passed for the wrong reason, and the guard's
invariant-text assertion failed with a red that pointed at the model instead
of at the missing tool (issue #1354).

This module gives those tests the same jar resolution the standalone formal
checkers use (``find_tla2tools_jar``, which honours ``TLA2TOOLS_JAR``), an
explicit skip reason naming the provisioning path, and a launch check that
separates "TLC never started" from "TLC ran and did (not) report the
violation".

Coverage honesty: on a machine or runner with no jar, the guards that use
this module do NOT run.  The skip reason says so out loud rather than the
suite reporting a misleading pass or a misleading model failure.
"""
from __future__ import annotations

from pathlib import Path
import shutil
import subprocess

import pytest

from okg.substrate.formal_tlc import find_tla2tools_jar

REPO_ROOT = Path(__file__).resolve().parents[2]

JAVA = shutil.which("java")
TLA_JAR = find_tla2tools_jar(REPO_ROOT)
TLC_AVAILABLE = JAVA is not None and TLA_JAR is not None

# TLC prints this banner as its first stdout line on every successful start.
# Its absence means the JVM never reached ``tlc2.TLC`` -- a missing or
# unreadable jar, or a JVM too old for the jar's class file version.
TLC_BANNER = "TLC2 Version"

# Both branches name the provisioning path. A skip that says only "no java"
# leaves the reader with no next step, and lets a guard over the skip reason
# pass while naming nothing -- the same "passes for the wrong reason" shape
# this module exists to remove.
_NO_JAVA = (
    "TLC guard skipped: Java is required to run TLC but was not found on "
    "PATH. Install a JDK, then set TLA2TOOLS_JAR or run "
    "scripts/bootstrap_formal_admission.py for the pinned jar. "
    "The TLC-driven guards in this file do NOT run without it."
)
_NO_JAR = (
    "TLC guard skipped: TLC is required but no tla2tools.jar was found. Set "
    "TLA2TOOLS_JAR or run scripts/bootstrap_formal_admission.py. "
    "The TLC-driven guards in this file do NOT run without it."
)


def tlc_skip_reason() -> str:
    """Explicit, provisioning-naming reason for skipping a TLC-driven guard."""
    if JAVA is None:
        return _NO_JAVA
    return _NO_JAR


def assert_tlc_ran(result: subprocess.CompletedProcess[str]) -> None:
    """Fail loudly, naming the jar, when TLC never started.

    Call this before asserting on invariant-violation text so a launch
    failure can never masquerade as a guard that stopped detecting its
    mutant.
    """
    if TLC_BANNER in result.stdout:
        return
    pytest.fail(
        "TLC failed to launch; the guard assertions below were not "
        f"exercised. jar={TLA_JAR} java={JAVA} exit={result.returncode}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
