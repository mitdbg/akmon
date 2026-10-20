"""Manifest-fingerprint census coupling test (bind-deployment-identity R1).

Every top-level manifest key in ``deployments._KNOWN_TOP_LEVEL_KEYS`` must be
classified in ``manifest_identity`` as behavior-bearing (IN) or not (OUT) —
exactly one of the two — so the fingerprint's coverage cannot silently rot
when a new manifest block is introduced.

This test is a pure set computation with NO database dependency. It lives in
its own unmarked file (moved out of the postgres-marked
``test_deployment_identity_continuity.py`` per PACT
``per-pr-manifest-census-gate``) precisely so the per-PR fast lane collects
it: ``deployment_up`` and ``host_storage`` shipped unclassified and the
resulting failure sat hidden in the nightly-only sweep until found by hand
(issue #829 work). Do not add a ``postgres`` marker here — that would demote
the census back to nightly-only coverage.
"""
from __future__ import annotations

from okg.substrate.catalog.manifest_identity import (
    census_gap,
    census_overlap,
)


def test_manifest_fingerprint_census_partitions_all_known_keys() -> None:
    from okg.substrate.deployments import _KNOWN_TOP_LEVEL_KEYS

    assert census_overlap() == set(), (
        "manifest fingerprint keys must be IN xor OUT, never both"
    )
    gap = census_gap(_KNOWN_TOP_LEVEL_KEYS)
    assert gap == set(), (
        "every known top-level manifest key must be classified as "
        f"behavior-bearing (IN) or not (OUT); unclassified: {sorted(gap)}. "
        "Classify the key in manifest_identity.py (IN if it selects what "
        "publishes or syncs, OUT if it is ontology/ops/read-side)."
    )
