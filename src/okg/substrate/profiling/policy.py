from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Iterable


PROFILE_COLLECTORS = {
    "rootless-speedscope",
    "postgres-flamegraph",
    "explain-samples",
}

_COLLECTOR_ALIASES = {
    "flamegraph": "rootless-speedscope",
    "python-flamegraph": "rootless-speedscope",
    "python_flamegraph": "rootless-speedscope",
    "python-speedscope": "rootless-speedscope",
    "python_speedscope": "rootless-speedscope",
    "rootless_speedscope": "rootless-speedscope",
    "speedscope": "rootless-speedscope",
}


def normalize_collector(value: object) -> str | None:
    collector = str(value or "").strip()
    if not collector:
        return None
    collector = _COLLECTOR_ALIASES.get(collector, collector)
    return collector if collector in PROFILE_COLLECTORS else None


def normalize_collectors(values: Iterable[object]) -> tuple[str, ...]:
    collectors: list[str] = []
    seen: set[str] = set()
    for raw in values:
        collector = normalize_collector(raw)
        if collector is None or collector in seen:
            continue
        collectors.append(collector)
        seen.add(collector)
    return tuple(collectors)


@dataclass(frozen=True)
class LightweightPolicy:
    enabled: bool = True
    ring_buffer_events: int = 10_000
    slow_query_ms: int = 250
    wait_sample_hz: float = 2.0
    live_snapshot_stale_seconds: int = 60


@dataclass(frozen=True)
class DeepCapturePolicy:
    mode: str = "manual"  # off | manual | on_slow | sampled
    targets: tuple[str, ...] = ("publish", "mcp", "viz-request")
    slow_publish_ms: int = 30_000
    slow_mcp_ms: int = 1_000
    slow_viz_request_ms: int = 1_000
    sample_rate: float = 0.0
    collectors: tuple[str, ...] = ()
    max_bundles: int = 20


@dataclass(frozen=True)
class ProfilingPolicy:
    lightweight: LightweightPolicy = field(default_factory=LightweightPolicy)
    deep_capture: DeepCapturePolicy = field(default_factory=DeepCapturePolicy)

    def to_dict(self) -> dict[str, Any]:
        return {
            "lightweight": asdict(self.lightweight),
            "deep_capture": asdict(self.deep_capture),
        }


def policy_from_config(raw: dict[str, Any] | None) -> ProfilingPolicy:
    raw = raw or {}
    lw_raw = dict(raw.get("lightweight") or {})
    dc_raw = dict(raw.get("deep_capture") or {})
    collectors: list[str] = []
    for key, collector in (
        ("flamegraph", "python-flamegraph"),
        ("python_flamegraph", "python-flamegraph"),
        ("postgres_flamegraph", "postgres-flamegraph"),
        ("explain_samples", "explain-samples"),
    ):
        if dc_raw.pop(key, False):
            normalized = normalize_collector(collector)
            if normalized:
                collectors.append(normalized)
    if "collectors" in dc_raw:
        collectors.extend(str(c) for c in (dc_raw.pop("collectors") or ()))
    mode = str(dc_raw.get("mode", "manual"))
    if mode not in {"off", "manual", "on_slow", "sampled"}:
        mode = "manual"
    targets = tuple(dc_raw.get("targets") or ("publish", "mcp", "viz-request"))
    dc_raw["mode"] = mode
    dc_raw["targets"] = targets
    dc_raw["collectors"] = normalize_collectors(collectors)
    return ProfilingPolicy(
        lightweight=LightweightPolicy(**{
            k: v for k, v in lw_raw.items()
            if k in LightweightPolicy.__dataclass_fields__
        }),
        deep_capture=DeepCapturePolicy(**{
            k: v for k, v in dc_raw.items()
            if k in DeepCapturePolicy.__dataclass_fields__
        }),
    )


def resolve_policy(
    raw: dict[str, Any] | None,
    *,
    profile: str | bool | None = None,
    collectors: Iterable[str] = (),
) -> ProfilingPolicy:
    base = policy_from_config(raw)
    dc = base.deep_capture
    mode = dc.mode
    if profile is True:
        mode = "manual"
    elif isinstance(profile, str):
        if profile in {"", "deep", "true", "1"}:
            mode = "manual"
        elif profile == "off":
            mode = "off"
    merged_collectors = normalize_collectors((*dc.collectors, *collectors))
    return ProfilingPolicy(
        lightweight=base.lightweight,
        deep_capture=DeepCapturePolicy(
            mode=mode,
            targets=dc.targets,
            slow_publish_ms=dc.slow_publish_ms,
            slow_mcp_ms=dc.slow_mcp_ms,
            slow_viz_request_ms=dc.slow_viz_request_ms,
            sample_rate=dc.sample_rate,
            collectors=merged_collectors,
            max_bundles=dc.max_bundles,
        ),
    )
