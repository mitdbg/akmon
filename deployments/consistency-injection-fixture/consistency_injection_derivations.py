"""Maintained-derivation registry for the injection-benchmark ring-0 fixture.

Registers the two claim-model derivations so a real publish cycle
materializes ``okg.claim_effective`` (tier 1) and ``okg.claim_projection``
(tier 2/3) under this deployment's label. Referenced by ``deployment.yaml``'s
``maintained_derivations.registry: consistency_injection_derivations:registry``.
The module name is deliberately distinctive so it cannot collide with another
deployment's modules on ``sys.path``.
"""

from __future__ import annotations

from okg.substrate.derived_artifacts.claim_effective_derivation import (
    claim_effective_derivation_row,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (
    claim_projection_derivation_row,
)
from okg.substrate.derived_artifacts.derivation_registry import DerivationRegistry

DEPLOYMENT = "consistency-injection-fixture"


def registry() -> DerivationRegistry:
    """The fixture's maintained-derivation registry (claim plane)."""
    reg = DerivationRegistry()
    reg.register(claim_effective_derivation_row())
    reg.register(claim_projection_derivation_row(deployment=DEPLOYMENT))
    return reg
