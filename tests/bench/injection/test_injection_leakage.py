"""Leakage-firewall witnesses (task.bench.leakage).

Pins the verification clause: planting an answer-key token in the mutated
corpus fails the leakage gate closed (no artifact written) and the scorer
refuses; deleting the leakage-pass artifact makes the scorer refuse; the
clean path passes with the canary absent from captured pipeline inputs;
post-pass tampering with any bound artifact is detected on load.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.injection import (
    ClaimSnapshot,
    CorpusSnapshot,
    InjectionError,
    RunConfig,
    run_injection,
)
from okg.bench.injection.leakage import (
    LEAKAGE_FILENAME,
    load_leakage_pass,
    run_leakage_check,
)
from okg.bench.injection.scoring import (
    FindingView,
    PipelineState,
    build_scorecard,
)
from tests.bench.injection.test_injection_core import (
    CLAIMS,
    DOC_A,
    DOC_B,
    DOC_C,
    GENERATION,
    ValueFlipOperator,
)


@pytest.fixture()
def injection_run(tmp_path: Path):
    root = tmp_path / "clean"
    root.mkdir()
    (root / "docA.md").write_text(DOC_A, encoding="utf-8")
    (root / "docB.md").write_text(DOC_B, encoding="utf-8")
    (root / "docC.md").write_text(DOC_C, encoding="utf-8")
    return run_injection(
        corpus=CorpusSnapshot.load(root),
        claims=ClaimSnapshot.from_rows(GENERATION, CLAIMS),
        operators=[ValueFlipOperator()],
        config=RunConfig(seed=9, out_dir=tmp_path / "out"),
    )


def test_clean_path_passes_and_scorer_accepts(injection_run):
    corpus = CorpusSnapshot.load(injection_run.mutated_corpus_dir)
    captured = [corpus.read_bytes(relpath) for relpath, _ in corpus.files]
    artifact_path = run_leakage_check(injection_run.out_dir, captured_inputs=captured)
    assert artifact_path.is_file()
    canary = injection_run.manifest["canary"]
    for chunk in captured:
        assert canary.encode() not in chunk
    leakage_pass = load_leakage_pass(injection_run.out_dir)
    assert leakage_pass["status"] == "pass"
    assert leakage_pass["scanned_token_count"] >= len(injection_run.mutations) + 1
    gold = injection_run.mutations
    clean = PipelineState(claims={c.claim_id: c.value for c in CLAIMS})
    mutated_claims = dict(clean.claims)
    findings = []
    for record in gold:
        expected = record.expected
        mutated_claims[expected.claim_logical_id] = record.post_text
        if expected.counterpart_claim_id:
            findings.append(
                FindingView(
                    finding_id=f"f-{record.mutation_id[-8:]}",
                    channel="definite",
                    claim_ids=frozenset(
                        {expected.claim_logical_id, expected.counterpart_claim_id}
                    ),
                    bucket_key=expected.bucket_key,
                )
            )
    scorecard = build_scorecard(
        gold=gold,
        clean=clean,
        mutated=PipelineState(claims=mutated_claims, findings=tuple(findings)),
        leakage_pass=leakage_pass,
    )
    assert scorecard["core"]["gold_count"] == len(gold)


def test_planted_corpus_token_fails_closed(injection_run):
    victim = injection_run.mutations[0]
    target = injection_run.mutated_corpus_dir / victim.doc
    target.write_text(
        target.read_text(encoding="utf-8") + f"\nnote: {victim.mutation_id}\n",
        encoding="utf-8",
    )
    with pytest.raises(InjectionError, match="leaked into mutated corpus"):
        run_leakage_check(injection_run.out_dir)
    assert not (injection_run.out_dir / LEAKAGE_FILENAME).exists()


def test_canary_in_captured_inputs_fails_closed(injection_run):
    canary = injection_run.manifest["canary"]
    with pytest.raises(InjectionError, match="captured pipeline input"):
        run_leakage_check(
            injection_run.out_dir,
            captured_inputs=[b"prompt prefix " + canary.encode() + b" suffix"],
        )
    assert not (injection_run.out_dir / LEAKAGE_FILENAME).exists()


def test_scorer_refuses_without_pass_artifact(injection_run):
    with pytest.raises(InjectionError, match="refuses without one"):
        load_leakage_pass(injection_run.out_dir)
    clean = PipelineState(claims={})
    with pytest.raises(InjectionError, match="refuses to emit a scorecard"):
        build_scorecard(
            gold=injection_run.mutations,
            clean=clean,
            mutated=clean,
            leakage_pass=None,
        )


def test_post_pass_tamper_is_detected(injection_run):
    run_leakage_check(injection_run.out_dir)
    mutations_path = injection_run.out_dir / "mutations.jsonl"
    rows = mutations_path.read_text(encoding="utf-8").splitlines()
    doctored = json.loads(rows[0])
    doctored["hardness_tier"] = "t2_reexpression"
    rows[0] = json.dumps(doctored, sort_keys=True, separators=(",", ":"))
    mutations_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    with pytest.raises(InjectionError, match="digests do not match"):
        load_leakage_pass(injection_run.out_dir)
