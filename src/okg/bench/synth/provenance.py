"""The provenance-group / majority-flavor plan (synth-provenance-diversity).

CORE module (substrate-free): it computes a pure, deterministic plan over the
sealed gold + trace, so the plan is sealed into the manifest and every witness
checks engine output against SEALED gold rather than re-deriving anything. The
RUNNER (runner.py) reads the plan back and stamps the ``provenance_group`` attr
onto the extracted claim facts at the ingest seam (exactly like the precedence
arm stamps), never rendering it into a document.

WHY this exists. The capstone measured the engine's P0: preserved majorities
mis-repair on basis ``dependency_adjusted_support`` — copies count as independent
support, so two corrupted renderings of one lie beat one truthful counterpart.
The coming engine fix is INDEPENDENCE WEIGHTING: support should count DIVERSE
provenance, not raw claim multiplicity. Today the benchmark cannot grade that
fix, because its majorities carry no provenance structure. This plants the
distinction:

* COPY-majority — the corrupted renderings of one site share ONE provenance
  group (one origin, echoed). Under independence weighting the group counts once,
  so the majority collapses to a 1-vs-1 tie and the lie is no longer repaired-to.
* INDEPENDENT-majority — each corrupted rendering carries a DISTINCT provenance
  group (genuinely diverse sources). The majority is real evidence and keeps
  settling as it does today.

The provenance attribute is a DECLARED AXIS, not a ranking signal: the engine
reads no such attr today (proven by a stamped-vs-unstamped publish parity
witness), and the per-arm scope condition must explicitly ADMIT it (an undeclared
ranking-signal stamp still fails loudly).
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping, Sequence

from okg.bench.synth.arms import _bucket_claims

PROVENANCE_PLAN_SCHEMA = "okg.synth.provenance-plan.v1"

# The single claim attr the provenance group enters on at ingest. NOT a ranking
# signal (never in resolution.PRECEDENCE_SIGNAL_ATTRS); admitted as a declared
# axis by assert_arm_signal_scope's provenance_admitted flag.
PROVENANCE_GROUP_ATTR = "provenance_group"

# The two majority flavors gold labels per site.
FLAVOR_COPY = "copy"
FLAVOR_INDEPENDENT = "independent"
FLAVORS = (FLAVOR_COPY, FLAVOR_INDEPENDENT)

# How flavors are assigned across a run's majority sites. ``alternate`` splits the
# sorted majority corruption ids so a single run carries BOTH flavors with a
# balanced denominator each; ``copy`` / ``independent`` force one flavor (for a
# single-flavor witness). Byte-equal to the config regex in config.ProvenanceAxis.
SPLIT_ALTERNATE = "alternate"
SPLIT_COPY = "copy"
SPLIT_INDEPENDENT = "independent"
SPLIT_MODES = (SPLIT_ALTERNATE, SPLIT_COPY, SPLIT_INDEPENDENT)


def _flavor_for(split: str, index: int) -> str:
    """The flavor of the ``index``-th majority site under ``split``."""
    if split == SPLIT_COPY:
        return FLAVOR_COPY
    if split == SPLIT_INDEPENDENT:
        return FLAVOR_INDEPENDENT
    # alternate: even → copy, odd → independent (deterministic over sorted ids)
    return FLAVOR_COPY if index % 2 == 0 else FLAVOR_INDEPENDENT


def _disabled_plan() -> dict[str, Any]:
    return {
        "schema": PROVENANCE_PLAN_SCHEMA,
        "enabled": False,
        "flavor_split": "",
        "flavor_by_corruption": {},
        "group_by_claim": {},
    }


def build_provenance_plan(
    *,
    enabled: bool,
    flavor_split: str,
    gold_corruptions: Sequence[Mapping[str, Any]],
    trace: Sequence[Mapping[str, Any]],
    patients: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """The sealed provenance plan: which majority site gets which flavor, and
    which extracted claim gets which provenance group.

    Deterministic over the sorted majority corruption ids. A majority site is a
    corruption rendered as a corrupted value more than once — its
    ``corruption_id`` appears in more than one gold-corruption row (the gold list
    holds one row per corrupted RENDERING). Its corrupted-rendering claims are the
    ``corrupted=True`` claims of its target fact (every rendering of one corruption
    shares that fact's id).

    * COPY flavor: all of the site's corrupted claims map to ONE group
      (``pg:<corruption_id>``).
    * INDEPENDENT flavor: each corrupted claim maps to a DISTINCT group
      (``pg:<corruption_id>:<doc>`` — one document, one origin).

    The clean counterpart's claim is left ungrouped: the group tags only the
    corrupted renderings whose multiplicity the independence fix must discount.

    ``enabled`` False (the default for every existing preset) returns the empty
    plan, so the run stamps nothing and the ingest is byte-for-byte unchanged
    except the config_digest the new axis moves.
    """
    if not enabled:
        return _disabled_plan()

    counts = Counter(str(site["corruption_id"]) for site in gold_corruptions)
    majority_ids = sorted(cid for cid, n in counts.items() if n > 1)
    fact_by_cid = {
        str(site["corruption_id"]): str(site["fact_id"]) for site in gold_corruptions
    }

    # The corrupted-rendering claims of each fact, from the SAME occurrence-keyed
    # bucketing the deterministic parser and the arm plan use — never a divergent
    # copy, which would silently mis-key the group.
    corrupted_by_fact: dict[str, list[dict[str, Any]]] = {}
    for claims in _bucket_claims(trace, patients).values():
        for claim in claims:
            if claim.get("corrupted"):
                corrupted_by_fact.setdefault(str(claim["fact_id"]), []).append(claim)

    flavor_by_corruption: dict[str, str] = {}
    group_by_claim: dict[str, str] = {}
    for index, cid in enumerate(majority_ids):
        flavor = _flavor_for(flavor_split, index)
        flavor_by_corruption[cid] = flavor
        fact_id = fact_by_cid[cid]
        claims = sorted(
            corrupted_by_fact.get(fact_id, ()), key=lambda c: str(c["claim_id"])
        )
        for claim in claims:
            claim_id = str(claim["claim_id"])
            if flavor == FLAVOR_COPY:
                group_by_claim[claim_id] = f"pg:{cid}"
            else:
                group_by_claim[claim_id] = f"pg:{cid}:{claim['doc']}"

    return {
        "schema": PROVENANCE_PLAN_SCHEMA,
        "enabled": True,
        "flavor_split": flavor_split,
        "flavor_by_corruption": dict(sorted(flavor_by_corruption.items())),
        "group_by_claim": dict(sorted(group_by_claim.items())),
    }
