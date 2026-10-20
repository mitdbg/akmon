"""task.llm.resolution witnesses: the resolution loader and the
repair-reachability FALSIFICATION, asserted on real engine rows.

The headline claim is negative — the engine cannot repair — so these
witnesses assert it directly on `bucket_resolution` rows rather than
inferring it from a zero score, and FAIL loudly if repair ever becomes
reachable (the intended outcome of the follow-on engine PACT).

One real publish serves every assertion: the lane's postgres budget is
shared, and a publish per assertion would buy granularity we do not need.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.config import load_config  # noqa: E402
from okg.bench.synth.resolution import (  # noqa: E402
    OUTCOME_CANONICAL,
    OUTCOME_CONTESTED,
    STATE_MIS_REPAIRED,
    STATE_REPAIRED,
    STATE_UNADJUDICATED,
    adjudicate,
    corruption_bucket_map,
    corruption_renderings,
    falsification_report,
    load_resolutions,
    synth_slug,
)
from okg.substrate.consistency.claim_model.precedence import (  # noqa: E402
    SIGNAL_INDEPENDENT_SUPPORT,
)
from okg.bench.synth.runner import detection_state, run_synth_benchmark  # noqa: E402
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"


def _install_catalog(dsn: str, tmp_path: Path) -> None:
    d = tmp_path / "catalog-scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    (d / "schemas").mkdir()
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, compose_catalog(d))
    from tests.pytest.catalog_fixture_builders import apply_explicit_test_catalog
    apply_explicit_test_catalog(dsn, _DEP_DIR)


def _equivalent(a: str, b: str) -> bool:
    """Declared equivalence for this witness: exact after case/space fold."""
    return " ".join(str(a).lower().split()) == " ".join(str(b).lower().split())


@pytest.mark.postgres
def test_repair_reachability_falsification(clean_runtime: str, tmp_path: Path) -> None:
    dsn = clean_runtime
    _install_catalog(dsn, tmp_path)
    # k=(1,3) so the corpus holds BOTH conflicting buckets and
    # single-rendering corruptions (which the engine never adjudicates).
    config = load_config({
        "seed": "resolution-witness", "world_path": "world_smoke",
        "redundancy": {"renderings_per_fact_min": 1, "renderings_per_fact_max": 3},
        "benign_variation": {"unit_rescale_rate": 0.0},
        "scale": {"patients": 8, "max_facts_per_patient": 40},
        "corruption": {"classes": {
            "value_perturbation": {"count": 4, "magnitude": 0.3, "visibility_corrupted": 1},
            "polarity_flip": {"count": 2, "visibility_corrupted": 1},
        }},
    })
    result = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "run", config=config
    )
    assert result["corrupted"]["status"] == "published", "needs a REAL generation"
    manifest = result["manifest"]
    gen = result["corrupted"]["generation_id"]
    state = detection_state(dsn, gen)
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(gen)
        resolutions = load_resolutions(checkout, state.claims)
        # Effective claims carry the (predicate, key, value, source) needed to
        # compute bucket support below. state.claims is a {claim_id: value}
        # map and cannot answer it.
        effective_claims = list(checkout.query(
            "SELECT predicate, normalized_key, value, source_id "
            "FROM okg.claim_effective WHERE row_kind = 'effective_claim'", {}
        ))

    # (a) the loader sees rows the finding-only loader cannot, with members
    #     taken from ranked_claim_ids (member_claim_ids is NULL on these rows)
    assert resolutions, "a corrupted corpus must produce bucket_resolution rows"
    for res in resolutions:
        assert res.outcome in (OUTCOME_CANONICAL, OUTCOME_CONTESTED)
        assert res.ranked_claim_ids, f"{res.bucket}: members from ranked_claim_ids"

    # (b) THE FALSIFICATION RETIRED ITSELF, exactly as designed.
    #
    # It asserted every adjudicated bucket ties, and named two possible causes
    # for its own failure: (1) the corpus now yields a strict support majority,
    # which is correct behaviour and retires it; (2) the lane leaked a
    # precedence signal, which would be a defect. Cause (1) is what happened:
    # the clinical_v1 pack gained the referral_letter genre, which lifted
    # realized redundancy from effective_k {2: 146, 3: 4} to k=3 for every
    # fact, so buckets can finally carry a majority for corroboration to settle.
    #
    # The leak guard is the half still worth keeping, so it is kept and
    # SHARPENED: buckets may now settle, but ONLY on corroboration. Any other
    # decisive basis means this lane leaked a signal (an extracted note date
    # into effective_date being the live hazard), which would settle buckets
    # while looking like added realism.
    report = falsification_report(resolutions)
    settled = [r for r in resolutions if r.outcome == OUTCOME_CANONICAL]
    assert settled, (
        "repair is expected to be REACHABLE on this corpus now that the pack "
        "renders facts into three documents; nothing settled, so either the "
        "engine regressed or the corpus lost its redundancy"
    )
    # `repair_reachable` IS `bool(canonical)`, so re-asserting it right after
    # `assert settled` restates the line above. What the report can actually get
    # wrong is its TALLIES, so those are checked against the test's own count.
    assert report["canonical_resolutions"] == len(settled)
    assert report["resolution_rows"] == len(resolutions)

    leaked = [r for r in settled if r.basis != SIGNAL_INDEPENDENT_SUPPORT]
    assert leaked == [], (
        "a bucket settled on something OTHER than corroboration: "
        + ", ".join(f"{r.bucket}->{r.basis}" for r in leaked[:3])
        + ". This lane must admit no precedence signal but corroboration - a "
          "leaked effective_date or source_authority settles buckets while "
          "looking like realism, and would silently change what every repair "
          "number in this lane means."
    )

    # The settled buckets must be exactly those with a strict support majority:
    # the engine's outcome is cross-checked against the CORPUS shape, so a
    # settle that does not follow from the evidence fails here.
    support: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for claim in effective_claims:
        support[
            (claim["predicate"], claim["normalized_key"], claim["value"])
        ].add(claim["source_id"] or "")
    counts_by_bucket: dict[tuple[str, str], list[int]] = defaultdict(list)
    for (pred, key, _value), sources in support.items():
        counts_by_bucket[(pred, key)].append(len(sources))
    multi_value = {b: c for b, c in counts_by_bucket.items() if len(c) > 1}
    assert multi_value, (
        "NO bucket carries two distinct values, so the majority check below "
        "would hold vacuously — this corpus produced no conflict at all. "
        "Check the publish and the bucket keys before trusting any result "
        "from this run."
    )
    with_majority = {
        f"{p}:{k}"
        for (p, k), counts in counts_by_bucket.items()
        if len(counts) > 1 and max(counts) > sorted(counts)[-2]
    }
    assert with_majority, (
        "no bucket has a strict support majority, so a settled bucket could "
        "not be explained by corroboration - the assertion above would be "
        "checking nothing"
    )
    assert {r.bucket for r in settled} <= with_majority, (
        "a bucket settled WITHOUT a strict support majority: "
        f"{sorted({r.bucket for r in settled} - with_majority)[:3]}"
    )

    # (c) a corruption with no counterpart is UN-ADJUDICATED, not mis-repaired
    buckets = corruption_bucket_map(manifest)
    k_by_corruption = corruption_renderings(manifest)
    resolved_buckets = {r.bucket for r in resolutions}
    singles = [cid for cid, k in k_by_corruption.items() if k == 1]
    assert singles, (
        "this seed produced no single-rendering corruption, so the "
        "un-adjudicated claim would be witnessed by nothing - a silent skip. "
        "Pin a seed/config that yields at least one k=1 corruption."
    )
    for corruption_id in singles:
        assert buckets[corruption_id] not in resolved_buckets, (
            f"{corruption_id}: a single-rendering corruption must produce NO "
            f"resolution row (un-adjudicated, not canonical-to-corrupted)"
        )

    # (d) DETECTED and REPAIRED are independent; the tally is exhaustive
    clean = {s["corruption_id"]: s["pre_text"] for s in manifest["gold"]["corruptions"]}
    detected = frozenset(
        f.bucket_key for f in state.findings if f.channel in ("definite", "possible")
    )
    out = adjudicate(
        corruption_buckets=buckets, clean_values=clean, resolutions=resolutions,
        detected_buckets=detected, equivalent=_equivalent,
    )
    tally = out["tally"]
    assert sum(tally.values()) == out["total"], "states must be exhaustive"
    assert tally[STATE_UNADJUDICATED] >= len(singles)
    # Repair is reachable now, so the tally must be able to READ it. The old
    # assertion here was `repaired == 0`; keeping it would have re-pinned the
    # retired falsification through the back door.
    assert tally[STATE_REPAIRED] > 0, (
        "buckets settled on corroboration, so at least one corruption should "
        f"score repaired: {tally}"
    )
    # Corroboration settles on the MAJORITY value, and in this lane the clean
    # value holds it (the corrupted value is rendered into fewer documents), so
    # a mis-repair here would mean the engine settled against its own evidence.
    # The majority-wrong direction is measured deliberately in
    # tests/bench/synth/test_majority_arms.py, not left to chance here.
    assert tally[STATE_MIS_REPAIRED] == 0, (
        f"clean value holds the majority in this lane, so nothing should "
        f"mis-repair: {tally}"
    )


def test_lane_emits_no_precedence_signals() -> None:
    """SCOPE CONDITION of the falsification, asserted at the emission point.

    The result holds only while no published claim carries a decisive `≻`
    signal. Clinical notes carry DATES and `effective_date` is decisive, so an
    extractor that helpfully stamps a note date would resolve buckets
    canonically and silently destroy the falsification while looking like
    realism. Witnessed here (no DB) so a future failure is diagnosed as "the
    lane leaked a signal" rather than "a producer appeared".
    """
    from okg.bench.synth.resolution import (
        PRECEDENCE_SIGNAL_ATTRS,
        SOURCE_CLASS_ATTR,
    )
    from okg.bench.synth.runner import CanonicalizingClaimSource

    corpus = _REPO_ROOT / "deployments" / "consistency-injection-fixture" / "corpus"
    source = CanonicalizingClaimSource(repo_root=corpus, policies=())
    claims = [f for f in source.run("scope-check").facts
              if f.subtype == "consistency_claim"]
    assert claims, "fixture corpus must yield claims"

    # (1) no ranking signal present
    for fact in claims:
        leaked = PRECEDENCE_SIGNAL_ATTRS & set(fact.attrs)
        assert leaked == set(), (
            f"{fact.node_id}: emits precedence signal(s) {sorted(leaked)}; the "
            f"falsification's scope condition requires none"
        )
    # (2) source_class uniform — a precedent can only discriminate if it varies
    classes = {f.attrs.get(SOURCE_CLASS_ATTR) for f in claims}
    assert len(classes) == 1, (
        f"source_class must be UNIFORM for the falsification to hold "
        f"(an operator precedent could otherwise settle a bucket); saw {classes}"
    )


def test_slug_matches_the_graph_grammar() -> None:
    """The one slug rule must equal the extractor's, or every bucket lookup
    silently misses (no DB needed)."""
    from okg.bench.injection.claim_corpus_source import _slug

    for text in ["Body Weight", "Patient Abram53 Hackett68", "O'Brien-Smith",
                 "  Doubled  Spaces  ", "HbA1c %"]:
        assert synth_slug(text) == _slug(text)
