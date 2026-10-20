"""Core dataclasses shared by adapters, arms, runner, and reports."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

ARM_NAMES = ("fresh", "native", "okg", "echo")


@dataclass(frozen=True)
class TaskInstance:
    """One benchmark task, benchmark-agnostic.

    ``gold`` is the sealed scoring payload (reference answer, gold patch,
    FAIL_TO_PASS, bug-inducing commit, ...). It is never surfaced to an arm:
    the runner hands arms only ``prompt`` plus the provisioned workdir.
    """

    instance_id: str
    benchmark: str
    prompt: str
    repo: str | None = None  # "owner/name"
    base_commit: str | None = None
    created_at: str | None = None  # ISO 8601, from the dataset
    cutoff: str | None = None  # leakage cutoff (usually == created_at)
    gold: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)

    def public_dict(self) -> dict[str, Any]:
        """The instance as arms/manifests may see it: gold stays sealed."""
        d = asdict(self)
        d.pop("gold")
        return d


@dataclass(frozen=True)
class TaskMetrics:
    """Per-task accounting, from the driver's own measurement (never estimated)."""

    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_creation_tokens: int
    tool_calls: int
    wall_s: float
    num_turns: int | None = None
    cost_usd: float | None = None  # as reported by the CLI; None under subscription


@dataclass(frozen=True)
class TaskResult:
    """What an arm driver returns for one task."""

    instance_id: str
    arm: str
    status: str  # "completed" | "error" | "timeout"
    answer: str  # final assistant text (QA benchmarks score this)
    metrics: TaskMetrics
    patch: str | None = None  # git diff produced in the workdir, if any
    session_id: str | None = None
    error: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Score:
    """One task's score. ``value`` in [0,1] when scored; None when unscored.

    An unscored task carries the reason loudly; it is never averaged in as 0.
    """

    instance_id: str
    value: float | None
    kind: str  # e.g. "resolve", "llm_judge", "scenario", "bic_match"
    detail: dict[str, Any] = field(default_factory=dict)
    unscored_reason: str | None = None


def dump_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))
