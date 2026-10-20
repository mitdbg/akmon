"""Coding-context density contract.

This validator is intentionally snapshot-based. A caller can feed it a
live-derived JSON snapshot or a captured baseline file; the contract logic
does not need DB access and is easy to exercise in unit tests.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okg.substrate import fastyaml


@dataclass(frozen=True)
class CodingContextCheckResult:
    field: str
    passed: bool
    expected: Any
    actual: Any
    message: str = ""


@dataclass(frozen=True)
class CodingContextResult:
    deployment_name: str
    generation_id: int | None
    passed: bool
    field_results: list[CodingContextCheckResult] = field(
        default_factory=list
    )

    def failures(self) -> list[CodingContextCheckResult]:
        return [r for r in self.field_results if not r.passed]

    def summary(self) -> str:
        if self.passed:
            return (
                f"{self.deployment_name} @ generation "
                f"{self.generation_id}: PASS "
                f"({len(self.field_results)} checks)"
            )
        failures = self.failures()
        body = "\n".join(
            f"  - {r.field}: {r.message}" for r in failures
        )
        return (
            f"{self.deployment_name} @ generation {self.generation_id}: "
            f"FAIL ({len(failures)} of {len(self.field_results)} checks)"
            f"\n{body}"
        )


@dataclass(frozen=True)
class CodingContextContract:
    deployment_name: str
    max_containment_edge_rate: float
    max_embed_lag: int
    embedding_required: bool
    min_unresolved_mention_resolved_rate: float
    required_subtypes: dict[str, int]
    required_edge_patterns: dict[str, int]
    required_languages: list[str]
    min_symbols_per_language: int
    max_empty_attr_rates: dict[str, float]
    optional_source_status: dict[str, list[str]]
    required_evidence_status: dict[str, list[str]]
    max_agent_memory_quality_counts: dict[str, int]

    @classmethod
    def from_yaml(cls, path: Path) -> CodingContextContract:
        raw = fastyaml.safe_load(path.read_text()) or {}
        missing = [
            key for key in (
                "deployment_name",
                "max_containment_edge_rate",
                "max_embed_lag",
                "min_unresolved_mention_resolved_rate",
                "required_subtypes",
                "required_edge_patterns",
                "required_languages",
                "min_symbols_per_language",
                "optional_source_status",
                "required_evidence_status",
            ) if key not in raw
        ]
        if missing:
            raise ValueError(
                f"{path}: coding context contract missing required "
                f"keys: {missing}"
            )
        return cls(
            deployment_name=str(raw["deployment_name"]),
            max_containment_edge_rate=float(
                raw["max_containment_edge_rate"]
            ),
            max_embed_lag=int(raw["max_embed_lag"]),
            embedding_required=bool(raw.get("embedding_required", True)),
            min_unresolved_mention_resolved_rate=float(
                raw["min_unresolved_mention_resolved_rate"]
            ),
            required_subtypes={
                str(k): int(v)
                for k, v in dict(raw["required_subtypes"]).items()
            },
            required_edge_patterns={
                str(k): int(v)
                for k, v in dict(raw["required_edge_patterns"]).items()
            },
            required_languages=[
                str(x) for x in raw["required_languages"]
            ],
            min_symbols_per_language=int(raw["min_symbols_per_language"]),
            max_empty_attr_rates={
                str(k): float(v)
                for k, v in dict(
                    raw.get("max_empty_attr_rates") or {}
                ).items()
            },
            optional_source_status={
                str(k): [str(x) for x in v]
                for k, v in dict(raw["optional_source_status"]).items()
            },
            required_evidence_status={
                str(k): [str(x) for x in v]
                for k, v in dict(raw["required_evidence_status"]).items()
            },
            max_agent_memory_quality_counts={
                str(k): int(v)
                for k, v in dict(
                    raw.get("max_agent_memory_quality_counts") or {}
                ).items()
            },
        )


def check_coding_context_contract(
    contract: CodingContextContract,
    snapshot: dict[str, Any],
) -> CodingContextResult:
    deployment = str(snapshot.get("deployment", ""))
    if deployment != contract.deployment_name:
        raise ValueError(
            f"contract is for {contract.deployment_name!r}, snapshot "
            f"is for {deployment!r}"
        )

    results: list[CodingContextCheckResult] = []
    summary = dict(snapshot.get("summary") or {})

    _append_max(
        results,
        "summary.containment_edge_rate",
        actual=_lookup(summary, "containment_edge_rate"),
        expected=contract.max_containment_edge_rate,
    )
    if contract.embedding_required:
        _append_max(
            results,
            "summary.embed_lag",
            actual=_lookup(summary, "embed_lag"),
            expected=contract.max_embed_lag,
        )
    unresolved = dict(summary.get("unresolved_mentions") or {})
    _append_min(
        results,
        "summary.unresolved_mentions.rate_resolved",
        actual=_lookup(unresolved, "rate_resolved"),
        expected=contract.min_unresolved_mention_resolved_rate,
    )

    subtype_counts = _subtype_counts(snapshot)
    for subtype, minimum in contract.required_subtypes.items():
        _append_min(
            results,
            f"subtype:{subtype}",
            actual=subtype_counts.get(subtype, 0),
            expected=minimum,
        )

    edge_patterns = _edge_pattern_counts(snapshot)
    for pattern, minimum in contract.required_edge_patterns.items():
        _append_min(
            results,
            f"edge_pattern:{pattern}",
            actual=edge_patterns.get(pattern, 0),
            expected=minimum,
        )

    language_counts = dict(snapshot.get("language_symbol_counts") or {})
    for language in contract.required_languages:
        _append_min(
            results,
            f"language_symbol_counts:{language}",
            actual=language_counts.get(language, 0),
            expected=contract.min_symbols_per_language,
        )

    empty_attr_rates = dict(snapshot.get("empty_attr_rates") or {})
    for subtype, maximum in contract.max_empty_attr_rates.items():
        _append_max(
            results,
            f"empty_attr_rate:{subtype}",
            actual=empty_attr_rates.get(subtype),
            expected=maximum,
        )

    statuses = dict(snapshot.get("optional_source_status") or {})
    for source_name, allowed in contract.optional_source_status.items():
        actual = statuses.get(source_name)
        passed = actual in set(allowed)
        results.append(CodingContextCheckResult(
            field=f"optional_source_status:{source_name}",
            passed=passed,
            expected=allowed,
            actual=actual,
            message=(
                ""
                if passed
                else f"status {actual!r} not one of {allowed}"
            ),
        ))

    evidence = dict(snapshot.get("evidence_status") or {})
    for evidence_name, allowed in contract.required_evidence_status.items():
        actual = evidence.get(evidence_name)
        passed = actual in set(allowed)
        results.append(CodingContextCheckResult(
            field=f"evidence_status:{evidence_name}",
            passed=passed,
            expected=allowed,
            actual=actual,
            message=(
                ""
                if passed
                else f"status {actual!r} not one of {allowed}"
            ),
        ))

    agent_memory = dict(snapshot.get("agent_memory_quality") or {})
    for metric_name, maximum in contract.max_agent_memory_quality_counts.items():
        _append_max(
            results,
            f"agent_memory_quality:{metric_name}",
            actual=agent_memory.get(metric_name, 0),
            expected=maximum,
        )

    return CodingContextResult(
        deployment_name=deployment,
        generation_id=_generation_id(snapshot),
        passed=all(r.passed for r in results),
        field_results=results,
    )


def _lookup(mapping: dict[str, Any], key: str) -> Any:
    return mapping.get(key)


def _append_min(
    results: list[CodingContextCheckResult],
    field: str,
    *,
    actual: Any,
    expected: int | float,
) -> None:
    actual_number = _number_or_none(actual)
    passed = actual_number is not None and actual_number >= expected
    results.append(CodingContextCheckResult(
        field=field,
        passed=passed,
        expected=expected,
        actual=actual,
        message=(
            ""
            if passed
            else f"{actual!r} is below minimum {expected}"
        ),
    ))


def _append_max(
    results: list[CodingContextCheckResult],
    field: str,
    *,
    actual: Any,
    expected: int | float,
) -> None:
    actual_number = _number_or_none(actual)
    passed = actual_number is not None and actual_number <= expected
    results.append(CodingContextCheckResult(
        field=field,
        passed=passed,
        expected=expected,
        actual=actual,
        message=(
            ""
            if passed
            else f"{actual!r} exceeds maximum {expected}"
        ),
    ))


def _number_or_none(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _subtype_counts(snapshot: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in snapshot.get("top_subtypes") or []:
        subtype = row.get("subtype")
        if subtype:
            counts[str(subtype)] = int(row.get("nodes") or 0)
    for subtype, count in dict(snapshot.get("subtype_counts") or {}).items():
        counts[str(subtype)] = int(count)
    for subtype, count in dict(snapshot.get("target_gaps") or {}).items():
        if subtype.endswith("_nodes"):
            counts[subtype.removesuffix("_nodes")] = int(count)
    return counts


def _edge_pattern_counts(snapshot: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in snapshot.get("top_edge_patterns") or []:
        pattern = row.get("pattern")
        if pattern:
            counts[str(pattern)] = int(row.get("count") or 0)
    for pattern, count in dict(snapshot.get("edge_pattern_counts") or {}).items():
        counts[str(pattern)] = int(count)
    return counts


def _generation_id(snapshot: dict[str, Any]) -> int | str | None:
    # Chronos-native: the generation id is a STRING checkpoint id (e.g.
    # "generation:N" or "okg:<deployment>:branch:...:gen:..."); do NOT int()-cast it.
    return snapshot.get("generation_id")
