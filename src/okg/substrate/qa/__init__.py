"""QA-gate substrate support.

Per the `deployment-qa` capability (proposal `qa-stabilize-main`):
this module exposes the canonical health-metrics dataclass
``QAGateMetrics`` and the contract validator ``check_contract``.
The public surface is small — these primitives are consumed by
``okg metrics --qa-snapshot`` (CLI) and by per-deployment QA-gate
runbooks. No deployment-specific code lives here.
"""
from __future__ import annotations

from okg.substrate.qa.metrics import QAGateMetrics, snapshot
from okg.substrate.qa.contract import (
    CatalogState,
    ContractResult,
    FieldResult,
    StabilityContract,
    check_contract,
)
from okg.substrate.qa.coding_context import (
    CodingContextCheckResult,
    CodingContextContract,
    CodingContextResult,
    check_coding_context_contract,
)

__all__ = [
    "QAGateMetrics",
    "snapshot",
    "StabilityContract",
    "CatalogState",
    "ContractResult",
    "FieldResult",
    "check_contract",
    "CodingContextCheckResult",
    "CodingContextContract",
    "CodingContextResult",
    "check_coding_context_contract",
]
