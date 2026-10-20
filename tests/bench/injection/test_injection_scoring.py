"""Scorer witnesses (task.bench.scorer).

Real gold (a full v1-library injection over the operator fixture corpus) is
scored against constructed pipeline states, pinning the verification clause:

- stage attribution: un-extracting one family charges extraction recovery;
  removing one family's finding (candidates intact) charges verdict accuracy
  while candidate recall is unchanged;
- scorecard denominators equal the gold's expected counts exactly;
- the two targeting modes' denominators stay separate and the visibility gap
  is reported; recall is reported per hardness tier;
- false-positive accounting excludes the clean baseline B0; suppressed hard
  negatives that flag report per suppression mechanism (era-conditional);
- the ledger carries per-stage tokens/cost/retries/latency (tokens, cost,
  retries zero in the LLM-free ring; latency recorded) in the volatile
  annex, and the deterministic core contains no generation ids, timestamps,
  or latencies.
"""

from __future__ import annotations


import pytest

from okg.bench.injection import (
    ClaimSnapshot,
    CorpusSnapshot,
    ExpectedOutcome,
    Facets,
    InjectionError,
    MutationRecord,
    RunConfig,
    Span,
    run_injection,
)
from okg.bench.injection.scoring import (
    FindingView,
    PipelineState,
    StageLedgerEntry,
    _assert_core_is_deterministic,
    build_scorecard,
    scorecard_core_bytes,
)
from tests.bench.injection.test_injection_operators import (
    tuned_operators,
    CLAIMS,
    GENERATION,
    OPS_ALPHA,
    OPS_BETA,
    OPS_MIRROR,
    SUPPRESSED_TWINS,
)

_B0_FINDING = FindingView(
    finding_id="b0-legacy", channel="definite", bucket_key="legacy:thing"
)

# An earlier mutation in the same document shifted offsets by +10 between
# the original corpus (span) and the mutated document (inverse_span) — the
# evidence contract is mutated-document coordinates (review finding A1).
_INDEPENDENT_RECORD = MutationRecord(
    mutation_id="okgmut_" + "e" * 32,
    family_id="okgfam_" + "e" * 32,
    mode="independent",
    doc="ops_alpha.md",
    span=Span(start=60, end=70),
    pre_text="2026-03-14",
    post_text="2027-03-14",
    operator_id="test.independent",
    operator_version="1",
    params={},
    facets=Facets("contradiction", "date", "intra_document", "time", "wrong_fact"),
    hardness_tier="t1_literal",
    expected=ExpectedOutcome(kind="evidence_overlap"),
    inverse_span=Span(start=70, end=80),
    inverse_text="2026-03-14",
)


@pytest.fixture(scope="module")
def gold(tmp_path_factory) -> tuple[MutationRecord, ...]:
    root = tmp_path_factory.mktemp("scoring") / "clean"
    root.mkdir()
    (root / "ops_alpha.md").write_text(OPS_ALPHA, encoding="utf-8")
    (root / "ops_mirror.md").write_text(OPS_MIRROR, encoding="utf-8")
    (root / "ops_beta.md").write_text(OPS_BETA, encoding="utf-8")
    result = run_injection(
        corpus=CorpusSnapshot.load(root),
        claims=ClaimSnapshot.from_rows(GENERATION, CLAIMS),
        operators=tuned_operators(),
        config=RunConfig(seed=42, out_dir=root.parent / "out"),
        stability_probe=lambda doc, original, mutated: True,
    )
    return result.mutations + (_INDEPENDENT_RECORD,)


def _perfect_states(
    gold: tuple[MutationRecord, ...]
) -> tuple[PipelineState, PipelineState]:
    """A pipeline that recovers everything, plus the persistent B0 finding."""
    clean_claims = {claim.claim_id: claim.value for claim in CLAIMS}
    mutated_claims = dict(clean_claims)
    candidates: set[frozenset[str]] = set()
    findings: list[FindingView] = [_B0_FINDING]
    for record in gold:
        expected = record.expected
        if expected.claim_logical_id:
            if expected.kind == "claim_absent":
                mutated_claims.pop(expected.claim_logical_id, None)
            else:
                mutated_claims[expected.claim_logical_id] = record.post_text
        finding_id = f"f-{record.mutation_id[-8:]}"
        if expected.kind == "hard_negative":
            if expected.flagged_today:
                findings.append(
                    FindingView(
                        finding_id=finding_id,
                        channel=expected.channel_today or "definite",
                        bucket_key=expected.bucket_key,
                    )
                )
            continue
        if record.mode == "independent":
            findings.append(
                FindingView(
                    finding_id=finding_id,
                    channel="definite",
                    evidence=((record.doc, record.inverse_span),),
                )
            )
            continue
        if expected.kind == "conflict" and expected.counterpart_claim_id:
            pair = frozenset(
                {expected.claim_logical_id, expected.counterpart_claim_id}
            )
            candidates.add(pair)
            findings.append(
                FindingView(
                    finding_id=finding_id,
                    channel=expected.channel_today or "definite",
                    claim_ids=pair,
                    bucket_key=expected.bucket_key,
                )
            )
        elif expected.kind in ("malformed_value", "broken_reference"):
            findings.append(
                FindingView(
                    finding_id=finding_id,
                    channel="definite",
                    bucket_key=expected.bucket_key,
                )
            )
    clean = PipelineState(claims=clean_claims, findings=(_B0_FINDING,))
    mutated = PipelineState(
        claims=mutated_claims,
        candidates=frozenset(candidates),
        findings=tuple(findings),
    )
    return clean, mutated


_LEAKAGE_OK = {"schema": "okg.injection.leakage-check.v1", "status": "pass"}


def _score(gold, clean, mutated, **kwargs):
    kwargs.setdefault("leakage_pass", _LEAKAGE_OK)
    return build_scorecard(gold=gold, clean=clean, mutated=mutated, **kwargs)


def test_perfect_run_recovers_everything_and_denominators_are_exact(gold):
    clean, mutated = _perfect_states(gold)
    core = _score(gold, clean, mutated)["core"]
    assert core["gold_count"] == len(gold)
    for stage in ("extraction", "candidate", "verdict", "routing"):
        tally = core["stages"][stage]
        assert tally["recovered"] == tally["denominator"], stage
        assert tally["missed"] == []
    expected_extraction = sum(
        1
        for record in gold
        if record.mode == "graph"
        and record.expected.kind != "hard_negative"
        and record.expected.claim_logical_id
    )
    assert core["stages"]["extraction"]["denominator"] == expected_extraction
    expected_pairs = sum(
        1
        for record in gold
        if record.expected.kind == "conflict"
        and record.expected.claim_logical_id
        and record.expected.counterpart_claim_id
    )
    assert core["stages"]["candidate"]["denominator"] == expected_pairs
    assert core["false_positives"]["count"] == 0
    assert core["false_positives"]["baseline_excluded"] == 1
    hard = core["hard_negatives"]
    assert hard["denominator"] == sum(
        1 for record in gold if record.expected.kind == "hard_negative"
    )
    assert hard["recovered"] == hard["denominator"]
    assert core["end_to_end"]["graph_rate"] == 1.0
    assert core["end_to_end"]["independent_rate"] == 1.0
    assert core["end_to_end"]["visibility_gap"] == 0.0


def test_extraction_miss_is_charged_to_extraction(gold):
    clean, mutated = _perfect_states(gold)
    victim = next(
        record for record in gold if record.operator_id == "injection.numeric_value"
    )
    claims = dict(mutated.claims)
    claims[victim.expected.claim_logical_id] = victim.pre_text  # never re-extracted
    degraded = PipelineState(
        claims=claims, candidates=mutated.candidates, findings=mutated.findings
    )
    core = _score(gold, clean, degraded)["core"]
    assert victim.mutation_id in core["stages"]["extraction"]["missed"]
    assert victim.mutation_id not in core["stages"]["verdict"]["missed"]
    assert core["dispositions"][victim.mutation_id]["extraction"] is False
    perfect_core = _score(gold, clean, mutated)["core"]
    assert (
        core["stages"]["candidate"]["denominator"]
        == perfect_core["stages"]["candidate"]["denominator"] - 1
    )


def test_verdict_miss_leaves_candidate_recall_unchanged(gold):
    clean, mutated = _perfect_states(gold)
    victim = next(
        record for record in gold if record.operator_id == "injection.status_inversion"
    )
    findings = tuple(
        finding
        for finding in mutated.findings
        if finding.bucket_key != victim.expected.bucket_key
    )
    degraded = PipelineState(
        claims=mutated.claims, candidates=mutated.candidates, findings=findings
    )
    core = _score(gold, clean, degraded)["core"]
    perfect_core = _score(gold, clean, mutated)["core"]
    assert victim.mutation_id in core["stages"]["verdict"]["missed"]
    assert (
        core["stages"]["candidate"]["recovered"]
        == perfect_core["stages"]["candidate"]["recovered"]
    )
    assert core["dispositions"][victim.mutation_id]["candidate"] is True
    assert core["dispositions"][victim.mutation_id]["verdict"] is False


def test_false_positives_exclude_baseline_and_spurious_counts(gold):
    clean, mutated = _perfect_states(gold)
    spurious = FindingView(
        finding_id="f-spurious", channel="definite", bucket_key="unrelated:thing"
    )
    noisy = PipelineState(
        claims=mutated.claims,
        candidates=mutated.candidates,
        findings=mutated.findings + (spurious,),
    )
    core = _score(gold, clean, noisy)["core"]
    assert core["false_positives"]["finding_ids"] == ["f-spurious"]
    assert "b0-legacy" not in core["false_positives"]["finding_ids"]


def test_flagged_suppressed_twin_reports_per_mechanism(gold):
    clean, mutated = _perfect_states(gold)
    twin = next(
        record for record in gold if record.operator_id == "injection.hn_rounding"
    )
    leak = FindingView(
        finding_id="f-rounding-leak",
        channel="definite",
        bucket_key=twin.expected.bucket_key,
    )
    degraded = PipelineState(
        claims=mutated.claims,
        candidates=mutated.candidates,
        findings=mutated.findings + (leak,),
    )
    core = _score(gold, clean, degraded)["core"]
    assert twin.mutation_id in core["hard_negatives"]["missed"]
    mechanism = SUPPRESSED_TWINS[twin.operator_id]
    assert core["hard_negatives"]["fp_by_mechanism"][mechanism] == ["f-rounding-leak"]


def test_modes_and_tiers_are_reported_separately(gold):
    clean, mutated = _perfect_states(gold)
    blind = PipelineState(
        claims=mutated.claims,
        candidates=mutated.candidates,
        findings=tuple(finding for finding in mutated.findings if not finding.evidence),
    )
    core = _score(gold, clean, blind)["core"]
    assert set(core["modes"]) == {"graph", "independent"}
    assert core["modes"]["independent"]["denominator"] == 1
    assert core["end_to_end"]["independent_rate"] == 0.0
    assert core["end_to_end"]["visibility_gap"] == 1.0
    tiers = core["tiers"]
    assert sum(tally["denominator"] for tally in tiers.values()) == len(gold)
    assert set(tiers) == {"t1_literal", "t2_reexpression", "t3_paraphrase"}


def test_independent_evidence_matches_mutated_coordinates_only(gold):
    clean, mutated = _perfect_states(gold)
    core = _score(gold, clean, mutated)["core"]
    assert core["dispositions"][_INDEPENDENT_RECORD.mutation_id]["recovered"] is True
    # Evidence at the ORIGINAL-corpus span must NOT match: the pipeline cites
    # the mutated documents it ingested, and offsets shift between spaces.
    findings = tuple(
        FindingView(
            finding_id=finding.finding_id,
            channel=finding.channel,
            claim_ids=finding.claim_ids,
            bucket_key=finding.bucket_key,
            evidence=(
                ((_INDEPENDENT_RECORD.doc, _INDEPENDENT_RECORD.span),)
                if finding.evidence
                else ()
            ),
        )
        for finding in mutated.findings
    )
    stale_coords = PipelineState(
        claims=mutated.claims, candidates=mutated.candidates, findings=findings
    )
    core = _score(gold, clean, stale_coords)["core"]
    assert core["dispositions"][_INDEPENDENT_RECORD.mutation_id]["recovered"] is False


def test_families_score_as_units_and_expose_matched_findings(gold):
    clean, mutated = _perfect_states(gold)
    core = _score(gold, clean, mutated)["core"]
    families = core["families"]
    assert sum(family["members"] for family in families.values()) == len(gold)
    by_family: dict[str, list] = {}
    for record in gold:
        by_family.setdefault(record.family_id, []).append(record)
    stale_family = next(
        record.family_id
        for record in gold
        if record.operator_id == "injection.stale_copied_metadata"
    )
    assert families[stale_family]["members"] == 2
    assert families[stale_family]["recovered"] == 2
    assert len(families[stale_family]["matched_finding_ids"]) == 2
    for family_id, records in by_family.items():
        assert families[family_id]["members"] == len(records)


def test_ledger_lives_in_annex_and_core_is_volatile_free(gold):
    clean, mutated = _perfect_states(gold)
    ledger = (
        StageLedgerEntry(stage="extraction", latency_ms=12.5),
        StageLedgerEntry(stage="verdict", latency_ms=3.25),
    )
    scorecard = _score(
        gold, clean, mutated, ledger=ledger, run_binding_ref="run-binding.json"
    )
    annex = scorecard["annex"]
    assert [entry["stage"] for entry in annex["ledger"]] == ["extraction", "verdict"]
    assert all(
        entry["tokens"] == 0 and entry["cost_usd"] == 0.0 and entry["retries"] == 0
        for entry in annex["ledger"]
    )
    assert all(entry["latency_ms"] > 0 for entry in annex["ledger"])
    core_bytes = scorecard_core_bytes(scorecard)
    for token in (b"latency", b"generation", b"timestamp"):
        assert token not in core_bytes
    again = _score(
        gold, clean, mutated, ledger=ledger, run_binding_ref="run-binding.json"
    )
    assert scorecard_core_bytes(again) == core_bytes
    with pytest.raises(InjectionError):
        _assert_core_is_deterministic({"generation_id": "g-1"})
    with pytest.raises(InjectionError):
        _assert_core_is_deterministic({"nested": {"latency_ms": 1}})


def test_empty_gold_is_loud(gold):
    clean, mutated = _perfect_states(gold)
    with pytest.raises(InjectionError):
        build_scorecard(
            gold=(), clean=clean, mutated=mutated, leakage_pass=_LEAKAGE_OK
        )
