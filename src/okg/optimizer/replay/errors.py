"""Named refusals for the replay runner.

Every way a replay can refuse to start or stop part-way has a stable code, so
an operator (or a test) can tell a spend cap from a pin mismatch without
reading prose. Nothing here carries question, answer or key text.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class ReplayProblem:
    """One refusal: a stable code, a message, and a JSON pointer into the config."""

    code: str
    message: str
    pointer: str = ""

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message, "pointer": self.pointer}


class ReplayError(RuntimeError):
    """The replay cannot go on. ``codes`` names every problem found."""

    def __init__(self, problems: Sequence[ReplayProblem] | ReplayProblem) -> None:
        if isinstance(problems, ReplayProblem):
            problems = [problems]
        if not problems:
            raise ValueError("ReplayError needs at least one problem")
        self.problems = tuple(problems)
        first = self.problems[0]
        more = f" (+{len(self.problems) - 1} more)" if len(self.problems) > 1 else ""
        super().__init__(f"{first.code}: {first.message}{more}")

    @property
    def codes(self) -> tuple[str, ...]:
        return tuple(p.code for p in self.problems)


def refuse(code: str, message: str, pointer: str = "") -> ReplayError:
    return ReplayError(ReplayProblem(code, message, pointer))


class CapReached(ReplayError):
    """A model call would pass the per-run or total spend cap: the run stops."""


class SpendUnknown(ReplayError):
    """A reply carried no usable token usage, so its cost cannot be computed."""


class ProviderStop(ReplayError):
    """The model provider refused in a way that would repeat (key, quota, request shape)."""


class SurfaceChanged(ReplayError):
    """What the server serves, or the generation it reads, changed during the run."""


#: Exit codes of ``python -m okg.optimizer.replay``.
EXIT_OK = 0
EXIT_REFUSED = 2
EXIT_CAP = 3
EXIT_INCOMPLETE = 4
EXIT_INTERRUPTED = 130
