"""Maintained-derivation registry for the synthetic-corpus smoke deployment.

Registers the claim-model derivations so a real publish cycle materializes
``okg.claim_effective`` and ``okg.claim_projection`` under this
deployment's label. Referenced by ``deployment.yaml``'s
``maintained_derivations.registry``. Distinct module name so it cannot
collide with another deployment's modules on ``sys.path``.
"""

from __future__ import annotations

from okg.substrate.derived_artifacts.claim_effective_derivation import (
    claim_effective_derivation_row,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (
    claim_projection_derivation_row,
)
from okg.substrate.derived_artifacts.derivation_registry import DerivationRegistry

DEPLOYMENT = "synthetic-corpus-smoke"


def registry() -> DerivationRegistry:
    """The smoke deployment's maintained-derivation registry (claim plane)."""
    reg = DerivationRegistry()
    reg.register(claim_effective_derivation_row())
    reg.register(claim_projection_derivation_row(deployment=DEPLOYMENT))
    return reg
