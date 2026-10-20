from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


SCHEMA_VERSION = 1

EvidenceStatus = Literal["available", "unavailable", "degraded"]


@dataclass
class Evidence:
    status: EvidenceStatus
    reason: str | None = None


@dataclass
class ArtifactRef:
    path: str
    media_type: str
    kind: str


@dataclass
class StageTiming:
    name: str
    started_at_ns: int
    duration_ns: int
    parent: str | None = None
    attrs: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float:
        return self.duration_ns / 1_000_000


@dataclass
class QueryTiming:
    fingerprint: str
    stage: str | None
    duration_ns: int
    rowcount: int | None = None
    sql_shape: str | None = None
    backend_pid: int | None = None
    started_at_ns: int | None = None

    @property
    def duration_ms(self) -> float:
        return self.duration_ns / 1_000_000


@dataclass
class ProfileSummary:
    bottleneck_class: str
    top_stage: str | None
    top_stage_ms: float
    top_query: str | None
    db_round_trip_ms: float
    non_db_ms: float
    # Deterministic count of DB round-trips for this run (machine-load
    # immune, unlike db_round_trip_ms). Sourced from ProfileContext, not
    # re-derived from timings. count-based-perf-gate / req.counts.emit.
    query_count: int = 0


@dataclass
class ProfileRun:
    profile_run_id: str
    deployment: str
    target_kind: str
    command: str
    started_at: str
    ended_at: str | None = None
    duration_ms: float | None = None
    schema_version: int = SCHEMA_VERSION
    # Authoritative DB round-trip count, always populated (independent of
    # whether detailed `queries` were collected). count-based-perf-gate.
    query_count: int = 0
    generation_id: int | None = None
    request_path: str | None = None
    owner_id: str | None = None
    owner_meta: dict[str, Any] = field(default_factory=dict)
    source_sync: dict[str, Any] = field(default_factory=dict)
    policy: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Evidence] = field(default_factory=dict)
    artifacts: list[ArtifactRef] = field(default_factory=list)
    summary: ProfileSummary | None = None
    stages: list[StageTiming] = field(default_factory=list)
    queries: list[QueryTiming] = field(default_factory=list)
    backend_pids: list[int] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["stages"] = [
            {**asdict(s), "duration_ms": s.duration_ms}
            for s in self.stages
        ]
        data["queries"] = [
            {**asdict(q), "duration_ms": q.duration_ms}
            for q in self.queries
        ]
        return data
