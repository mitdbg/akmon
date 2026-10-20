"""Injection-core witnesses (PACT consistency-injection-benchmark, task.bench.injector).

DB-free and wall-clock-free: drives the injection engine over a temp corpus
and an in-memory generation-pinned claim snapshot. Pins the verification
clause of task.bench.injector:

- double-run byte identity of the mutated tree + answer-key artifacts;
- seed-disjoint mutation-id sets;
- graph-conditioned sites drawn from the pinned clean generation, each
  carrying a claim-level expectation;
- reject-with-reason (never silent, never applied) for ambiguous
  localization, byte pre-image mismatch, identity-unstable insertion, and
  b0 overlap;
- source snapshot byte-unchanged after injection;
- sealed manifest (schema forbids run outcomes) and schema-validated rows;
- exact inverses: applying them to the mutated text restores the original.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pytest

from okg.bench.injection import (
    ClaimRow,
    ClaimSnapshot,
    CorpusSnapshot,
    ExpectedOutcome,
    Facets,
    InjectionError,
    KeyedPRF,
    MutationPlan,
    RunConfig,
    Site,
    Span,
    run_injection,
)
from okg.bench.injection.manifest import (
    BENCHMARK_MANIFEST_SCHEMA_V1,
    MUTATION_ROW_SCHEMA_V1,
    validate_row,
)
from okg.bench.injection.types import claim_site_id

GENERATION = "g-clean-0001"

DOC_A = """# Systems

## Alpha
Version: 4.2
Status: active
Limit: alpha-beta

## Beta
Version: 7.7
Threshold: 12 and 12
"""

DOC_B = """# Systems

## Alpha
Version: 4.2
"""

DOC_C = """# Notes

Nothing asserted here.
"""

CLAIMS = (
    ClaimRow("cl_alpha_limit_full", "limit", "alpha", "alpha-beta", "Systems > Alpha", "known", "docA.md"),
    ClaimRow("cl_alpha_limit_part", "sub_limit", "alpha", "beta", "Systems > Alpha", "known", "docA.md"),
    ClaimRow("cl_alpha_status", "status", "alpha", "active", "Systems > Alpha", "known", "docA.md"),
    ClaimRow("cl_alpha_ver_a", "version", "alpha", "4.2", "Systems > Alpha", "known", "docA.md"),
    ClaimRow("cl_alpha_ver_b", "version", "alpha", "4.2", "Systems > Alpha", "known", "docB.md"),
    ClaimRow("cl_beta_thresh", "threshold", "beta", "12", "Systems > Beta", "known", "docA.md"),
    ClaimRow("cl_beta_ver", "version", "beta", "7.7", "Systems > Beta", "known", "docA.md"),
)


def test_claim_snapshot_digest_includes_typed_semantics() -> None:
    base = ClaimRow("claim:x", "p", "k", "v", "S", "known", "docA.md")
    typed = ClaimRow(
        "claim:x",
        "p",
        "k",
        "v",
        "S",
        "known",
        "docA.md",
        '{"modality":"pending"}',
    )
    assert ClaimSnapshot.from_rows("g", [base]).digest() != (
        ClaimSnapshot.from_rows("g", [typed]).digest()
    )


def _facets() -> Facets:
    return Facets(
        relation="contradiction",
        object="number",
        scope="intra_document",
        semantic_control="none",
        consequence="wrong_fact",
    )


@dataclass(frozen=True)
class ValueFlipOperator:
    """In-place value rewrite of version/threshold claims (identity-preserving)."""

    operator_id: str = "test.value_flip"
    version: str = "1"

    def sites(self, corpus: CorpusSnapshot, claims: ClaimSnapshot) -> Iterable[Site]:
        for claim in claims.claims:
            if claim.predicate in ("version", "threshold"):
                yield Site(
                    site_id=claim_site_id(claim), mode="graph", doc=claim.source_doc, claim=claim
                )

    def plan(self, site: Site, doc_text: str, span: Span, prf: KeyedPRF) -> MutationPlan:
        assert site.claim is not None
        replacement = prf.choice(f"flip:{site.site_id}", ["9.9", "8.8", "6.1"])
        return MutationPlan(
            site=site,
            span=span,
            pre_text=site.claim.value,
            post_text=replacement,
            params={"replacement": replacement},
            facets=_facets(),
            hardness_tier="t1_literal",
            expected=ExpectedOutcome(
                kind="conflict",
                claim_logical_id=site.claim.claim_id,
                bucket_key=f"{site.claim.predicate}:{site.claim.normalized_key}",
                channel_today="definite",
                locality="internal",
            ),
        )


@dataclass(frozen=True)
class BadPreimageOperator:
    """Emits a plan whose pre_text disagrees with the file bytes (must be gated)."""

    operator_id: str = "test.bad_preimage"
    version: str = "1"

    def sites(self, corpus: CorpusSnapshot, claims: ClaimSnapshot) -> Iterable[Site]:
        for claim in claims.claims:
            if claim.predicate == "status":
                yield Site(
                    site_id=claim_site_id(claim), mode="graph", doc=claim.source_doc, claim=claim
                )

    def plan(self, site: Site, doc_text: str, span: Span, prf: KeyedPRF) -> MutationPlan:
        return MutationPlan(
            site=site,
            span=span,
            pre_text="WRONG!",  # same length as "active", wrong bytes
            post_text="retired",
            params={},
            facets=_facets(),
            hardness_tier="t1_literal",
            expected=ExpectedOutcome(kind="conflict"),
        )


@dataclass(frozen=True)
class InsertDuplicateOperator:
    """Inserts a conflicting same-(predicate, key) assertion (assertion-count-changing)."""

    operator_id: str = "test.insert_dup"
    version: str = "1"

    def sites(self, corpus: CorpusSnapshot, claims: ClaimSnapshot) -> Iterable[Site]:
        for claim in claims.claims:
            if claim.claim_id == "cl_alpha_ver_a":
                yield Site(
                    site_id=claim_site_id(claim), mode="graph", doc=claim.source_doc, claim=claim
                )

    def plan(self, site: Site, doc_text: str, span: Span, prf: KeyedPRF) -> MutationPlan:
        assert site.claim is not None
        insert_at = doc_text.index("\n", span.end)
        return MutationPlan(
            site=site,
            span=Span(start=insert_at, end=insert_at),
            pre_text="",
            post_text="\nVersion: 5.0",
            params={"inserted": "5.0"},
            facets=_facets(),
            hardness_tier="t1_literal",
            expected=ExpectedOutcome(
                kind="conflict",
                counterpart_claim_id=site.claim.claim_id,
                bucket_key=f"{site.claim.predicate}:{site.claim.normalized_key}",
                channel_today="definite",
                locality="internal",
            ),
            family_key="insert-dup-alpha",
            changes_assertion_count=True,
        )


@pytest.fixture()
def clean_corpus(tmp_path: Path) -> CorpusSnapshot:
    root = tmp_path / "clean"
    root.mkdir()
    (root / "docA.md").write_text(DOC_A, encoding="utf-8")
    (root / "docB.md").write_text(DOC_B, encoding="utf-8")
    (root / "docC.md").write_text(DOC_C, encoding="utf-8")
    return CorpusSnapshot.load(root)


@pytest.fixture()
def claim_snapshot() -> ClaimSnapshot:
    return ClaimSnapshot.from_rows(GENERATION, CLAIMS)


def _run(corpus, claims, out_dir, *, seed=7, operators=None, probe=lambda d, o, m: True, b0=()):
    ops = operators if operators is not None else [
        ValueFlipOperator(),
        BadPreimageOperator(),
        InsertDuplicateOperator(),
    ]
    return run_injection(
        corpus=corpus,
        claims=claims,
        operators=ops,
        config=RunConfig(seed=seed, out_dir=out_dir, b0_claim_ids=frozenset(b0)),
        stability_probe=probe,
    )


def _reasons(result) -> dict[str, str]:
    return {rejection.site_id: rejection.reason for rejection in result.rejections}


def test_double_run_byte_identity(clean_corpus, claim_snapshot, tmp_path):
    first = _run(clean_corpus, claim_snapshot, tmp_path / "out1")
    second = _run(clean_corpus, claim_snapshot, tmp_path / "out2")
    assert (
        CorpusSnapshot.load(first.mutated_corpus_dir).tree_digest
        == CorpusSnapshot.load(second.mutated_corpus_dir).tree_digest
    )
    for name in ("mutations.jsonl", "rejected_sites.jsonl", "benchmark-manifest.json"):
        assert (first.out_dir / name).read_bytes() == (second.out_dir / name).read_bytes()


def test_seed_disjoint_mutation_ids(clean_corpus, claim_snapshot, tmp_path):
    one = _run(clean_corpus, claim_snapshot, tmp_path / "s1", seed=1)
    two = _run(clean_corpus, claim_snapshot, tmp_path / "s2", seed=2)
    ids_one = {record.mutation_id for record in one.mutations}
    ids_two = {record.mutation_id for record in two.mutations}
    assert ids_one and ids_two
    assert ids_one.isdisjoint(ids_two)


def test_graph_sites_carry_claim_expectations_and_generation_pin(
    clean_corpus, claim_snapshot, tmp_path
):
    result = _run(clean_corpus, claim_snapshot, tmp_path / "out")
    assert result.manifest["targeting"]["claims_digest"] == claim_snapshot.digest()
    assert result.mutations
    for record in result.mutations:
        assert record.mode == "graph"
        expected = record.expected
        assert expected.bucket_key
        assert expected.claim_logical_id or expected.counterpart_claim_id


def test_ambiguous_localization_rejected_not_mutated(clean_corpus, claim_snapshot, tmp_path):
    result = _run(clean_corpus, claim_snapshot, tmp_path / "out")
    assert _reasons(result)["claim:cl_beta_thresh"] == "ambiguous_match"
    mutated = (result.mutated_corpus_dir / "docA.md").read_text(encoding="utf-8")
    assert "Threshold: 12 and 12" in mutated


def test_preimage_mismatch_rejected_not_mutated(clean_corpus, claim_snapshot, tmp_path):
    result = _run(clean_corpus, claim_snapshot, tmp_path / "out")
    assert _reasons(result)["claim:cl_alpha_status"] == "preimage_mismatch"
    mutated = (result.mutated_corpus_dir / "docA.md").read_text(encoding="utf-8")
    assert "Status: active" in mutated


def test_identity_unstable_insertion_rejected(clean_corpus, claim_snapshot, tmp_path):
    no_probe = _run(clean_corpus, claim_snapshot, tmp_path / "p0", probe=None)
    assert _reasons(no_probe)["claim:cl_alpha_ver_a"] == "identity_unstable"
    failing = _run(clean_corpus, claim_snapshot, tmp_path / "p1", probe=lambda d, o, m: False)
    assert _reasons(failing)["claim:cl_alpha_ver_a"] == "identity_unstable"
    passing = _run(clean_corpus, claim_snapshot, tmp_path / "p2")
    inserted = [r for r in passing.mutations if r.operator_id == "test.insert_dup"]
    assert len(inserted) == 1
    mutated = (passing.mutated_corpus_dir / "docA.md").read_text(encoding="utf-8")
    assert "Version: 5.0" in mutated


def test_source_snapshot_unchanged(clean_corpus, claim_snapshot, tmp_path):
    before = clean_corpus.tree_digest
    _run(clean_corpus, claim_snapshot, tmp_path / "out")
    assert CorpusSnapshot.load(clean_corpus.root).tree_digest == before


def test_b0_overlap_rejected(clean_corpus, claim_snapshot, tmp_path):
    result = _run(clean_corpus, claim_snapshot, tmp_path / "out", b0=("cl_beta_ver",))
    assert _reasons(result)["claim:cl_beta_ver"] == "b0_overlap"
    mutated = (result.mutated_corpus_dir / "docA.md").read_text(encoding="utf-8")
    assert "Version: 7.7" in mutated


def test_manifest_is_sealed_against_run_outcomes(clean_corpus, claim_snapshot, tmp_path):
    result = _run(clean_corpus, claim_snapshot, tmp_path / "out")
    manifest_bytes = (result.out_dir / "benchmark-manifest.json").read_bytes()
    assert b"timestamp" not in manifest_bytes
    validate_row(result.manifest, BENCHMARK_MANIFEST_SCHEMA_V1)
    with pytest.raises(InjectionError):
        validate_row(
            {**result.manifest, "mutated_generation_id": "g-later"},
            BENCHMARK_MANIFEST_SCHEMA_V1,
        )
    with pytest.raises(InjectionError):
        validate_row({**result.manifest, "timestamp": "now"}, BENCHMARK_MANIFEST_SCHEMA_V1)


def test_rows_validate_and_corruption_is_loud(clean_corpus, claim_snapshot, tmp_path):
    result = _run(clean_corpus, claim_snapshot, tmp_path / "out")
    row = result.mutations[0].to_json()
    validate_row(row, MUTATION_ROW_SCHEMA_V1)
    corrupted = {key: value for key, value in row.items() if key != "facets"}
    with pytest.raises(InjectionError):
        validate_row(corrupted, MUTATION_ROW_SCHEMA_V1)
    with pytest.raises(InjectionError):
        validate_row({**row, "answer_key_note": "leak"}, MUTATION_ROW_SCHEMA_V1)


def test_inverse_round_trip_restores_original(clean_corpus, claim_snapshot, tmp_path):
    result = _run(clean_corpus, claim_snapshot, tmp_path / "out")
    by_doc: dict[str, list] = {}
    for record in result.mutations:
        by_doc.setdefault(record.doc, []).append(record)
    assert by_doc
    for doc, records in by_doc.items():
        mutated = (result.mutated_corpus_dir / doc).read_text(encoding="utf-8")
        for record in records:
            assert mutated[record.inverse_span.start : record.inverse_span.end] == record.post_text
        restored = mutated
        for record in sorted(records, key=lambda r: r.inverse_span.start, reverse=True):
            restored = (
                restored[: record.inverse_span.start]
                + record.inverse_text
                + restored[record.inverse_span.end :]
            )
        assert restored == clean_corpus.read_text(doc)


def test_existing_out_dir_is_refused(clean_corpus, claim_snapshot, tmp_path):
    out = tmp_path / "occupied"
    out.mkdir()
    with pytest.raises(InjectionError):
        _run(clean_corpus, claim_snapshot, out)


@dataclass(frozen=True)
class OverlapProbeOperator:
    """Two different-bucket claims whose value spans nest ("alpha-beta" ⊃ "beta")."""

    operator_id: str = "test.overlap_probe"
    version: str = "1"

    def sites(self, corpus: CorpusSnapshot, claims: ClaimSnapshot) -> Iterable[Site]:
        for claim in claims.claims:
            if claim.predicate in ("limit", "sub_limit"):
                yield Site(
                    site_id=claim_site_id(claim), mode="graph", doc=claim.source_doc, claim=claim
                )

    def plan(self, site: Site, doc_text: str, span: Span, prf: KeyedPRF) -> MutationPlan:
        assert site.claim is not None
        return MutationPlan(
            site=site,
            span=span,
            pre_text=site.claim.value,
            post_text=site.claim.value.upper(),
            params={},
            facets=_facets(),
            hardness_tier="t1_literal",
            expected=ExpectedOutcome(kind="conflict", claim_logical_id=site.claim.claim_id),
        )


def test_span_overlap_is_rejected(clean_corpus, claim_snapshot, tmp_path):
    result = _run(
        clean_corpus, claim_snapshot, tmp_path / "out", operators=[OverlapProbeOperator()]
    )
    assert [record.expected.claim_logical_id for record in result.mutations] == [
        "cl_alpha_limit_full"
    ]
    assert _reasons(result)["claim:cl_alpha_limit_part"] == "span_overlap"
    mutated = (result.mutated_corpus_dir / "docA.md").read_text(encoding="utf-8")
    assert "Limit: ALPHA-BETA" in mutated


def test_zero_width_span_overlap_semantics():
    insertion = Span(start=5, end=5)
    assert insertion.overlaps(Span(start=5, end=5))
    assert insertion.overlaps(Span(start=4, end=6))
    assert not insertion.overlaps(Span(start=6, end=8))
    assert not Span(start=2, end=5).overlaps(Span(start=5, end=8))
