"""Load, check, and scale the production-scale targets in ``targets.yaml``.

The targets file is the single place the generator's numbers live (see its
header for what ``recorded``, ``derived`` and ``guessed`` mean). This module
reads it, refuses a file whose totals disagree with each other, and produces
the scaled plan the builder writes.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

TARGETS_PATH = Path(__file__).with_name("targets.yaml")
TARGETS_SCHEMA = "okg.bench.workspace_scale.targets.v1"


class TargetsError(ValueError):
    """The targets file is malformed or internally inconsistent."""


@dataclass(frozen=True)
class EdgeTarget:
    source: str
    src_subtype: str
    edge_type: str
    dst_subtype: str
    count: int

    @property
    def triple(self) -> tuple[str, str, str]:
        return (self.src_subtype, self.edge_type, self.dst_subtype)


@dataclass(frozen=True)
class HistoryProfile:
    name: str
    publishes: int
    record_churn_rounds: float
    relationship_churn_rounds: float


@dataclass(frozen=True)
class Targets:
    """The parsed scale-1.0 targets."""

    raw: Mapping[str, Any]
    deployment: str
    nodes: dict[str, dict[str, int]]
    publisher_nodes: dict[str, dict[str, int]]
    edges: tuple[EdgeTarget, ...]
    enricher_edges: dict[str, dict[str, Any]]
    totals: dict[str, int]
    relationship_sources: dict[str, int]
    relationship_type_totals: dict[str, int]
    search_term: str
    search_records: int
    search_minimum_candidates: int
    text_chars: int
    profiles: dict[str, HistoryProfile]
    source_progress_rows: dict[str, int]

    def record_total(self) -> int:
        return sum(sum(v.values()) for v in self.nodes.values()) + sum(
            sum(v.values()) for v in self.publisher_nodes.values()
        )

    def relationship_total(self) -> int:
        return sum(e.count for e in self.edges) + sum(
            int(e["count"]) for e in self.enricher_edges.values()
        )

    def record_types(self) -> set[str]:
        types: set[str] = set()
        for by_subtype in (*self.nodes.values(), *self.publisher_nodes.values()):
            types.update(by_subtype)
        return types

    def relationship_types(self) -> set[str]:
        return {e.edge_type for e in self.edges} | {
            str(e["edge_type"]) for e in self.enricher_edges.values()
        }


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    if key not in mapping:
        raise TargetsError(f"targets file is missing {where}.{key}")
    return mapping[key]


def _counts(block: Mapping[str, Any], where: str) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for source, by_subtype in block.items():
        if source == "basis":
            continue
        if not isinstance(by_subtype, Mapping):
            raise TargetsError(f"{where}.{source} must map record type to count")
        out[str(source)] = {}
        for subtype, count in by_subtype.items():
            if not isinstance(count, int) or count < 0:
                raise TargetsError(f"{where}.{source}.{subtype} must be a count")
            out[str(source)][str(subtype)] = count
    return out


def load_targets(path: Path | None = None) -> Targets:
    """Parse ``targets.yaml`` and refuse it when its totals disagree."""
    path = path or TARGETS_PATH
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping) or raw.get("schema") != TARGETS_SCHEMA:
        raise TargetsError(f"{path} is not a {TARGETS_SCHEMA} targets file")
    edges: list[EdgeTarget] = []
    for source, rows in _require(raw, "edges", "").items():
        if source == "basis":
            continue
        for row in rows:
            if not (isinstance(row, list) and len(row) == 4):
                raise TargetsError(
                    f"edges.{source} rows are [src, edge_type, dst, count]: {row!r}"
                )
            src, edge_type, dst, count = row
            if not isinstance(count, int) or count < 0:
                raise TargetsError(f"edges.{source} count must be a count: {row!r}")
            edges.append(EdgeTarget(str(source), str(src), str(edge_type), str(dst), count))
    enricher_edges = {
        str(k): dict(v)
        for k, v in _require(raw, "enricher_edges", "").items()
        if k not in {"basis", "tolerance"}
    }
    history = _require(raw, "history", "")
    profiles = {
        str(name): HistoryProfile(
            name=str(name),
            publishes=int(_require(p, "publishes", f"history.profiles.{name}")),
            record_churn_rounds=float(p.get("record_churn_rounds", 0.0)),
            relationship_churn_rounds=float(p.get("relationship_churn_rounds", 0.0)),
        )
        for name, p in _require(history, "profiles", "history").items()
    }
    search = _require(raw, "search", "")
    runtime = _require(raw, "runtime", "")
    targets = Targets(
        raw=raw,
        deployment=str(_require(raw, "deployment", "")),
        nodes=_counts(_require(raw, "nodes", ""), "nodes"),
        publisher_nodes=_counts(_require(raw, "publisher_nodes", ""), "publisher_nodes"),
        edges=tuple(edges),
        enricher_edges=enricher_edges,
        totals={
            k: int(v) for k, v in _require(raw, "totals", "").items() if k != "basis"
        },
        relationship_sources={
            str(k): int(v)
            for k, v in _require(raw, "relationship_sources", "").items()
            if k != "basis"
        },
        relationship_type_totals={
            str(k): int(v)
            for k, v in _require(raw, "relationship_types", "").items()
            if k != "basis"
        },
        search_term=str(_require(search, "term", "search")),
        search_records=int(_require(search, "records_with_term", "search")),
        search_minimum_candidates=int(_require(search, "minimum_candidates", "search")),
        text_chars=int(_require(_require(raw, "attributes", ""), "text_chars", "attributes")),
        profiles=profiles,
        source_progress_rows={
            str(k): int(v)
            for k, v in _require(runtime, "source_progress_rows", "runtime").items()
        },
    )
    check_consistency(targets)
    return targets


def check_consistency(targets: Targets) -> None:
    """Refuse targets whose parts disagree with the recorded totals."""
    problems: list[str] = []
    if targets.record_total() != targets.totals["records"]:
        problems.append(
            f"records sum to {targets.record_total()}, totals.records is "
            f"{targets.totals['records']}"
        )
    if targets.relationship_total() != targets.totals["relationships"]:
        problems.append(
            f"relationships sum to {targets.relationship_total()}, "
            f"totals.relationships is {targets.totals['relationships']}"
        )
    if len(targets.record_types()) != targets.totals["record_types"]:
        problems.append(
            f"{len(targets.record_types())} record types, totals.record_types "
            f"is {targets.totals['record_types']}"
        )
    if len(targets.relationship_types()) != targets.totals["relationship_types"]:
        problems.append(
            f"{len(targets.relationship_types())} relationship types, "
            f"totals.relationship_types is {targets.totals['relationship_types']}"
        )
    record_sources = set(targets.nodes) | set(targets.publisher_nodes)
    if len(record_sources) != targets.totals["record_sources"]:
        problems.append(
            f"{len(record_sources)} record sources, totals.record_sources is "
            f"{targets.totals['record_sources']}"
        )
    known_types = targets.record_types()
    for edge in targets.edges:
        for subtype in (edge.src_subtype, edge.dst_subtype):
            if subtype not in known_types:
                problems.append(
                    f"edge {edge.source} {edge.triple} names record type "
                    f"{subtype!r}, which no source in `nodes` writes"
                )
    # The derived triple split must reproduce both recorded totals exactly.
    by_source: Counter[str] = Counter()
    by_type: Counter[str] = Counter()
    for edge in targets.edges:
        by_source[edge.source] += edge.count
        by_type[edge.edge_type] += edge.count
    for producer, spec in targets.enricher_edges.items():
        by_source[producer] += int(spec["count"])
        by_type[str(spec["edge_type"])] += int(spec["count"])
    for label, derived, recorded in (
        ("relationship source", by_source, targets.relationship_sources),
        ("relationship type", by_type, targets.relationship_type_totals),
    ):
        if dict(derived) != dict(recorded):
            diff = {
                key: (derived.get(key, 0), recorded.get(key, 0))
                for key in set(derived) | set(recorded)
                if derived.get(key, 0) != recorded.get(key, 0)
            }
            problems.append(
                f"per-{label} edge totals (derived, recorded) disagree: {diff}"
            )
    if problems:
        raise TargetsError("inconsistent targets: " + "; ".join(problems))


def scale_count(count: int, scale: float) -> int:
    """Scale one count; a count present at 1.0 stays at least 1."""
    if count <= 0:
        return 0
    return max(1, round(count * scale))


@dataclass(frozen=True)
class ScaledPlan:
    """The counts one build writes, after scaling."""

    scale: float
    profile: HistoryProfile
    nodes: dict[str, dict[str, int]]
    publisher_nodes: dict[str, dict[str, int]]
    edges: tuple[EdgeTarget, ...]
    enricher_edges: dict[str, int]
    search_records: int
    source_progress_rows: dict[str, int]
    record_churn: int
    relationship_churn: int
    notes: list[str] = field(default_factory=list)

    def record_total(self) -> int:
        return sum(sum(v.values()) for v in self.nodes.values())

    def edge_total(self) -> int:
        return sum(e.count for e in self.edges)

    def expected_nodes_by_subtype(self) -> Counter[str]:
        out: Counter[str] = Counter()
        for by in (*self.nodes.values(), *self.publisher_nodes.values()):
            out.update(by)
        return out


def scaled_plan(targets: Targets, *, scale: float, profile: str) -> ScaledPlan:
    if not scale > 0:
        raise TargetsError(f"scale must be positive, got {scale!r}")
    if profile not in targets.profiles:
        raise TargetsError(
            f"unknown history profile {profile!r}; choose one of "
            f"{sorted(targets.profiles)}"
        )
    prof = targets.profiles[profile]
    nodes = {
        source: {st: scale_count(n, scale) for st, n in by.items()}
        for source, by in targets.nodes.items()
    }
    edges = tuple(
        EdgeTarget(e.source, e.src_subtype, e.edge_type, e.dst_subtype, scale_count(e.count, scale))
        for e in targets.edges
    )
    record_total = sum(sum(v.values()) for v in nodes.values())
    edge_total = sum(e.count for e in edges)
    return ScaledPlan(
        scale=scale,
        profile=prof,
        nodes=nodes,
        # The publisher's own records do not scale: the deployment declares
        # its native views, the generator does not choose how many.
        publisher_nodes={k: dict(v) for k, v in targets.publisher_nodes.items()},
        edges=edges,
        enricher_edges={
            k: scale_count(int(v["count"]), scale)
            for k, v in targets.enricher_edges.items()
        },
        search_records=scale_count(targets.search_records, scale),
        # Progress rows are per-source bookkeeping, not corpus volume.
        source_progress_rows=dict(targets.source_progress_rows),
        record_churn=round(prof.record_churn_rounds * record_total),
        relationship_churn=round(prof.relationship_churn_rounds * edge_total),
    )


__all__ = [
    "EdgeTarget",
    "HistoryProfile",
    "ScaledPlan",
    "TARGETS_PATH",
    "Targets",
    "TargetsError",
    "check_consistency",
    "load_targets",
    "scale_count",
    "scaled_plan",
]
