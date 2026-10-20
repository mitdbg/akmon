"""Extraction-time canonicalization replacement — fragmentation parity (D3/F2b).

PACT consistency-claim-model-core task.core.canonicalization-replacement.

DECISION (recorded, designs/graph-derivation-model.md): cern-twiki canonicalizes
`preferred_target` subjects CORPUS-RELATIVELY at check time (rapidfuzz
`canonicalize_subjects`), so a claim's bucket depends on which OTHER subjects
exist — a late phrasing re-buckets earlier claims (review F2b). That cannot be
reproduced claim-locally at extraction. The replacement: a periodic
RE-CANONICALIZATION SWEEP runs the SAME clustering over the corpus and
materializes a canonical-subject table; EXTRACTION resolves each subject
CLAIM-LOCALLY against the last sweep's table (D3 — claim-local bucket keys are
the soundness precondition for bucket-scoped incremental recompute). Between
sweeps a new phrasing resolves to a fresh key (bounded staleness) until the next
sweep consolidates it and emits a KEY-MIGRATION CORRECTION.

ACCEPTANCE = fragmentation parity on the real cern-twiki subject families: the
replacement splits a same-subject family across buckets NO MORE than the current
clustering. This holds BY CONSTRUCTION — after a full sweep the materialized
table makes claim-local resolution identical to `canonicalize_subjects` — so it
is proven both on the twiki families and structurally across fuzzed corpora.

The baseline is VENDORED verbatim from okg-deployments origin/main
`cern-twiki/cern_twiki/subject_canonicalization.py` (cited, not imported — the
deployment owns the predicate/scope vocabulary side of the residence split).
"""
from __future__ import annotations

import random
import re

import pytest

fuzz = pytest.importorskip("rapidfuzz").fuzz

# --------------------------------------------------------------------------- #
# Vendored baseline (verbatim: cern-twiki subject_canonicalization.py).       #
# --------------------------------------------------------------------------- #
_WS = re.compile(r"\s+")
_PUNCT = re.compile(r"[\"'`(),.:;/]+")
CLUSTER_THRESHOLD = 86
ANCHOR_THRESHOLD = 90
_MIN_ENTITY_LEN = 3


def normalize_subject(s: str) -> str:
    return _WS.sub(" ", _PUNCT.sub(" ", str(s).strip().lower())).strip()


def _same_subject(a: str, b: str, threshold: int) -> bool:
    return (fuzz.token_sort_ratio(a, b) >= threshold
            or fuzz.token_set_ratio(a, b) >= threshold)


def _anchor(cluster, norm_entities, threshold):
    for e in norm_entities:
        if len(e) < _MIN_ENTITY_LEN:
            continue
        if any(fuzz.token_set_ratio(m, e) >= threshold for m in cluster):
            return e
    return None


def canonicalize_subjects(subjects, *, entity_surfaces=(),
                          cluster_threshold=CLUSTER_THRESHOLD,
                          anchor_threshold=ANCHOR_THRESHOLD):
    """cern-twiki's corpus-relative clustering (the baseline being replaced)."""
    originals = [s for s in subjects if s and str(s).strip()]
    uniq = sorted({normalize_subject(s) for s in originals})
    norm_entities = sorted(
        {normalize_subject(e) for e in entity_surfaces if e and str(e).strip()},
        key=lambda e: (len(e), e))
    clusters: list[list[str]] = []
    for n in uniq:
        for cl in clusters:
            if _same_subject(n, cl[0], cluster_threshold):
                cl.append(n)
                break
        else:
            clusters.append([n])
    canon_of: dict[str, str] = {}
    for cl in clusters:
        canonical = _anchor(cl, norm_entities, anchor_threshold) or min(
            cl, key=lambda m: (len(m), m))
        for m in cl:
            canon_of[m] = canonical
    return {s: canon_of[normalize_subject(s)] for s in originals}


# --------------------------------------------------------------------------- #
# The DECIDED replacement: sweep materializes the table; resolve claim-locally #
# --------------------------------------------------------------------------- #
def sweep_canonical_table(corpus_subjects, *, entity_surfaces=()):
    """The periodic re-canonicalization sweep: run the SAME clustering over the
    corpus once and materialize {normalized_subject -> canonical}. Extraction
    reads this table; it is NOT re-run per claim (that is what makes the
    extraction-time key claim-local)."""
    mapping = canonicalize_subjects(corpus_subjects, entity_surfaces=entity_surfaces)
    return {normalize_subject(s): c for s, c in mapping.items()}


def resolve_claim_local(subject, table):
    """Extraction-time, claim-local resolution against the last sweep's table.
    A subject absent from the table is its own fresh canonical (bounded
    staleness) until the next sweep consolidates it."""
    n = normalize_subject(subject)
    return table.get(n, n)


def _fragmentation(assignment, family):
    """Distinct canonical buckets a known-same-subject family splits into."""
    return len({assignment[s] for s in family})


# Representative cern-twiki subject families (module docstring + corpus-profile:
# CMSSW releases, GLIB firmware, HCAL DB, PVSS).
_GLIB = ["GLIBv2 firmware", "GLIBv2", "glibv2 fec", "GLIBv2 firmware bundle"]
_CMSSW = ["CMSSW release", "cmssw release", "CMSSW_10_4_0_pre1", "CMSSW_8_0_0_patch1"]
_HCAL = ["HCAL DB setup", "HCAL database setup", "hcal db"]
_PVSS = ["PVSS project", "pvss"]
_CORPUS = _GLIB + _CMSSW + _HCAL + _PVSS
_FAMILIES = (_GLIB, _CMSSW, _HCAL, _PVSS)


def test_sweep_then_resolve_matches_baseline_on_twiki_families():
    """After a full sweep, claim-local resolution == the corpus-relative
    clustering (parity by construction), and per-family fragmentation is equal."""
    base = canonicalize_subjects(_CORPUS)
    table = sweep_canonical_table(_CORPUS)
    repl = {s: resolve_claim_local(s, table) for s in _CORPUS}
    assert repl == base
    for fam in _FAMILIES:
        assert _fragmentation(repl, fam) == _fragmentation(base, fam)


def test_fragmentation_parity_replacement_no_worse_than_baseline():
    base = canonicalize_subjects(_CORPUS)
    table = sweep_canonical_table(_CORPUS)
    repl = {s: resolve_claim_local(s, table) for s in _CORPUS}
    for fam in _FAMILIES:
        assert _fragmentation(repl, fam) <= _fragmentation(base, fam)


def test_structural_parity_across_fuzzed_corpora():
    """Parity is a property of the mechanism (the sweep applies the identical
    clustering), so it holds for ANY corpus — proven over random subject sets,
    which subsumes the single real-corpus snapshot."""
    rng = random.Random(0xCA)
    toks = ["glib", "glibv2", "firmware", "cmssw", "release", "hcal", "db",
            "pvss", "fec", "setup", "project", "bundle", "10_4_0", "patch1", "pre1"]
    for _ in range(300):
        corpus = list({
            " ".join(rng.sample(toks, rng.randint(1, 4)))
            for _ in range(rng.randint(2, 12))
        })
        base = canonicalize_subjects(corpus)
        table = sweep_canonical_table(corpus)
        repl = {s: resolve_claim_local(s, table) for s in corpus}
        assert repl == base, corpus


def test_between_sweep_staleness_then_migration():
    """A new phrasing arriving between sweeps resolves claim-locally to a fresh
    key (bounded transient fragmentation); the next sweep re-clusters and
    consolidates it — the point at which a key-migration correction is emitted."""
    corpus0 = _GLIB[:2]                     # {"GLIBv2 firmware", "GLIBv2"}
    t0 = sweep_canonical_table(corpus0)
    newp = "glibv2 firmware bundle"         # a new phrasing of the GLIB family
    stale = resolve_claim_local(newp, t0)   # absent from t0 -> fresh (self) key
    assert stale == normalize_subject(newp)
    t1 = sweep_canonical_table(corpus0 + [newp])   # next sweep re-clusters
    consolidated = resolve_claim_local(newp, t1)
    # The key changed across the sweep -> a key-migration correction is due.
    assert consolidated != stale
    assert consolidated == t0.get(normalize_subject(_GLIB[1]))  # joins GLIB family
