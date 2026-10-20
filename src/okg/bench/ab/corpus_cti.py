"""CTI corpus: one dated snapshot -> published OKG deployment.

Unlike the repo corpus builder (``corpus.py``), the CTI corpus has no git
cutoff: the leakage boundary is the SNAPSHOT ITSELF — a directory of
operator-downloaded dumps (NVD CVE feeds, MITRE CWE XML, ATT&CK STIX
bundle), frozen at a recorded date, whose sha256s form the freeze manifest
every arm shares. Vanilla arms mount the same directory read-only; nothing
is fetched at run time by any arm.

This module is now a thin compatibility surface over ``corpus_domain``:
the CTI deployment is expressed there as one VerticalSpec among six, so
there is a single provisioning implementation rather than a per-vertical
codepath. ``okg-bench corpus build-cti`` keeps working unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .corpus_domain import CTI, DomainCorpusSpec
from .corpus_domain import build as _build_vertical

EXPECTED_FAMILIES = tuple(CTI.expected_families)


@dataclass(frozen=True)
class CtiCorpusSpec:
    snapshot_dir: Path
    snapshot_date: str  # operator-stated freeze date, ISO (recorded, not inferred)
    deployment_name: str
    dsn: str
    corpus_root: Path


def build(spec: CtiCorpusSpec) -> Path:
    """Build the CTI corpus via the shared spec-driven path."""
    return _build_vertical(
        DomainCorpusSpec(
            vertical=CTI.name,
            snapshot_dir=spec.snapshot_dir,
            snapshot_date=spec.snapshot_date,
            deployment_name=spec.deployment_name,
            dsn=spec.dsn,
            corpus_root=spec.corpus_root,
        )
    )
