"""Stability-contract validator.

Per the `deployment-qa` capability (proposal `qa-stabilize-main`):
``check_contract`` compares a deployment's
``tests/<deployment>/stability_contract.yaml`` against a fresh
``QAGateMetrics`` snapshot and the active catalog state, and returns
a structured ``ContractResult`` with per-field pass/fail.

The validator is read-only — it never mutates the contract, the
metrics, or the database.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okg.substrate import fastyaml

from okg.substrate.qa.metrics import QAGateMetrics


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass
class FieldResult:
    """One per contract field. `actual` and `expected` use whatever
    natural representation the field has (counts, sets, lists)."""

    field: str
    passed: bool
    expected: Any
    actual: Any
    message: str = ""


@dataclass
class ContractResult:
    deployment_name: str
    generation_id: int | str
    passed: bool
    field_results: list[FieldResult] = field(default_factory=list)

    def failures(self) -> list[FieldResult]:
        return [r for r in self.field_results if not r.passed]

    def summary(self) -> str:
        if self.passed:
            return (
                f"{self.deployment_name} @ generation "
                f"{self.generation_id}: PASS "
                f"({len(self.field_results)} fields checked)"
            )
        fails = self.failures()
        head = (
            f"{self.deployment_name} @ generation "
            f"{self.generation_id}: FAIL "
            f"({len(fails)} of {len(self.field_results)} fields)"
        )
        body = "\n".join(
            f"  - {r.field}: {r.message}" for r in fails
        )
        return head + "\n" + body


# ---------------------------------------------------------------------------
# Contract loading
# ---------------------------------------------------------------------------


@dataclass
class StabilityContract:
    """Parsed `tests/<deployment>/stability_contract.yaml`."""

    deployment_name: str
    expected_sources: list[str]
    expected_subtypes: list[str]
    expected_edge_types: list[str]
    min_nodes_live: int
    min_edges_live: int
    max_publish_wallclock_seconds: float
    required_views: list[str]
    required_invariants: list[str]
    viz_smoke_pages: list[str]

    @classmethod
    def from_yaml(cls, path: Path) -> StabilityContract:
        raw = fastyaml.safe_load(path.read_text()) or {}
        missing = [
            k for k in (
                "deployment_name",
                "expected_sources",
                "expected_subtypes",
                "expected_edge_types",
                "min_nodes_live",
                "min_edges_live",
                "max_publish_wallclock_seconds",
                "required_views",
                "required_invariants",
                "viz_smoke_pages",
            ) if k not in raw
        ]
        if missing:
            raise ValueError(
                f"{path}: stability_contract.yaml missing required "
                f"keys: {missing}"
            )
        return cls(
            deployment_name=str(raw["deployment_name"]),
            expected_sources=[str(x) for x in raw["expected_sources"]],
            expected_subtypes=[str(x) for x in raw["expected_subtypes"]],
            expected_edge_types=[
                str(x) for x in raw["expected_edge_types"]
            ],
            min_nodes_live=int(raw["min_nodes_live"]),
            min_edges_live=int(raw["min_edges_live"]),
            max_publish_wallclock_seconds=float(
                raw["max_publish_wallclock_seconds"]
            ),
            required_views=[str(x) for x in raw["required_views"]],
            required_invariants=[
                str(x) for x in raw["required_invariants"]
            ],
            viz_smoke_pages=[
                str(x) for x in raw["viz_smoke_pages"]
            ],
        )


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


@dataclass
class CatalogState:
    """The slice of catalog state the validator needs.

    Provided by the caller (typically the gate runbook) so the
    validator stays free of DB I/O. The runbook produces this from
    the same DSN the metrics snapshot used.
    """

    active_subtypes: set[str]
    active_edge_types: set[str]
    registered_sources: set[str]
    registered_views: set[str]
    invariant_status: dict[str, str]  # name -> 'passed' / 'failed' / 'errored'


def _check_set_subset(
    field_name: str,
    expected: list[str],
    actual: set[str],
) -> FieldResult:
    expected_set = set(expected)
    missing = sorted(expected_set - actual)
    passed = not missing
    msg = (
        ""
        if passed
        else f"{len(missing)} expected {field_name} not in active "
             f"set: {missing[:5]}"
        + ("..." if len(missing) > 5 else "")
    )
    return FieldResult(
        field=field_name,
        passed=passed,
        expected=sorted(expected_set),
        actual=sorted(actual),
        message=msg,
    )


def check_contract(
    contract: StabilityContract,
    metrics: QAGateMetrics,
    catalog: CatalogState,
) -> ContractResult:
    """Compare a contract against a metrics snapshot + catalog state.

    Pure: no DB I/O, no mutation. Returns a structured result.
    """
    if contract.deployment_name != metrics.deployment_name:
        # Mismatch is a hard error, not a soft fail.
        raise ValueError(
            f"contract is for {contract.deployment_name!r}, snapshot "
            f"is for {metrics.deployment_name!r}"
        )

    results: list[FieldResult] = []

    # Lower bounds.
    results.append(FieldResult(
        field="min_nodes_live",
        passed=metrics.nodes_live_delta >= contract.min_nodes_live,
        expected=contract.min_nodes_live,
        actual=metrics.nodes_live_delta,
        message=(
            ""
            if metrics.nodes_live_delta >= contract.min_nodes_live
            else f"nodes_live_delta {metrics.nodes_live_delta} < "
                 f"min {contract.min_nodes_live}"
        ),
    ))
    results.append(FieldResult(
        field="min_edges_live",
        passed=metrics.edges_live_delta >= contract.min_edges_live,
        expected=contract.min_edges_live,
        actual=metrics.edges_live_delta,
        message=(
            ""
            if metrics.edges_live_delta >= contract.min_edges_live
            else f"edges_live_delta {metrics.edges_live_delta} < "
                 f"min {contract.min_edges_live}"
        ),
    ))

    # Upper bound.
    wallclock_ok = (
        metrics.publish_wallclock_seconds
        <= contract.max_publish_wallclock_seconds
    )
    results.append(FieldResult(
        field="max_publish_wallclock_seconds",
        passed=wallclock_ok,
        expected=contract.max_publish_wallclock_seconds,
        actual=metrics.publish_wallclock_seconds,
        message=(
            ""
            if wallclock_ok
            else f"publish_wallclock_seconds "
                 f"{metrics.publish_wallclock_seconds:.1f} > "
                 f"max {contract.max_publish_wallclock_seconds:.1f}"
        ),
    ))

    # Catalog-state subset checks.
    results.append(_check_set_subset(
        "expected_sources",
        contract.expected_sources,
        catalog.registered_sources,
    ))
    results.append(_check_set_subset(
        "expected_subtypes",
        contract.expected_subtypes,
        catalog.active_subtypes,
    ))
    results.append(_check_set_subset(
        "expected_edge_types",
        contract.expected_edge_types,
        catalog.active_edge_types,
    ))
    results.append(_check_set_subset(
        "required_views",
        contract.required_views,
        catalog.registered_views,
    ))

    # Invariants: each required invariant must report status 'passed'.
    for inv in contract.required_invariants:
        status = catalog.invariant_status.get(inv, "missing")
        passed = status == "passed"
        results.append(FieldResult(
            field=f"invariant:{inv}",
            passed=passed,
            expected="passed",
            actual=status,
            message=(
                ""
                if passed
                else f"invariant {inv!r} status={status!r}"
            ),
        ))

    # `excluded_sources` must be empty (anything excluded by Phase B
    # gates is a contract violation; the deployment is not shippable
    # if a source had to be cut to publish).
    excluded_ok = not metrics.excluded_sources
    results.append(FieldResult(
        field="excluded_sources",
        passed=excluded_ok,
        expected=[],
        actual=list(metrics.excluded_sources),
        message=(
            ""
            if excluded_ok
            else f"sources excluded by Phase B: "
                 f"{metrics.excluded_sources}"
        ),
    ))

    # `okg doctor` must exit clean.
    doctor_ok = metrics.doctor_exit_code == 0
    results.append(FieldResult(
        field="doctor_exit_code",
        passed=doctor_ok,
        expected=0,
        actual=metrics.doctor_exit_code,
        message=(
            ""
            if doctor_ok
            else f"okg doctor exited {metrics.doctor_exit_code}"
        ),
    ))

    return ContractResult(
        deployment_name=metrics.deployment_name,
        generation_id=metrics.generation_id,
        passed=all(r.passed for r in results),
        field_results=results,
    )
