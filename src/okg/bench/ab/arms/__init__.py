"""Arm-driver registry. The three protocol arms are fixed; ``echo`` is the
offline test arm. A fourth arm = one entry here."""

from __future__ import annotations

from ..errors import AbBenchError
from ..model import ARM_NAMES
from .base import ArmDriver, RunContext  # noqa: F401  (public surface)
from .claude_code import ClaudeCodeDriver
from .echo import EchoDriver


def arm_names() -> list[str]:
    return list(ARM_NAMES)


def get_driver(name: str) -> ArmDriver:
    if name == "echo":
        return EchoDriver()
    if name in ("fresh", "native", "okg"):
        return ClaudeCodeDriver(name)
    raise AbBenchError(f"unknown arm {name!r}; known: {', '.join(arm_names())}")
