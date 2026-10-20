"""Maintained-derivation registry for the differential-consistency fixture.

Registers the two claim-model derivations so a real publish cycle materializes
their kept tables (``okg.claim_effective`` tier 1, then ``okg.claim_projection``
tier 2/3 — the registry runs them in sorted derivation_id order, tier 1 first)
and, because ``claim_projection_derivation_row`` is given this deployment's
label, records the selected repair (r*) into ``okg.repair_selections`` on every
full recompute (PACT consistency-repair-objective, task.repair.materialize).

Referenced by ``deployment.yaml``'s
``maintained_derivations.registry: consistency_fixture_derivations:registry``.
The module name is deliberately distinctive (not a generic ``checks`` package)
so it cannot collide with another deployment's modules on ``sys.path``.
"""
from __future__ import annotations

from okg.substrate.derived_artifacts.claim_effective_derivation import (
    claim_effective_derivation_row,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (
    claim_projection_derivation_row,
)
from okg.substrate.derived_artifacts.derivation_registry import DerivationRegistry

DEPLOYMENT = "consistency-fixture"


def registry() -> DerivationRegistry:
    """The deployment's maintained-derivation registry (claim plane)."""
    reg = DerivationRegistry()
    reg.register(claim_effective_derivation_row())
    reg.register(claim_projection_derivation_row(deployment=DEPLOYMENT))
    return reg
