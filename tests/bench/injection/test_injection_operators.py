"""Operator-library witnesses (PACT consistency-injection-benchmark, task.bench.operators).

Runs the full v1 built-in library over a fixture corpus and asserts, per the
task verification clause:

- every #749 family has at least one positive and one mechanism-named
  hard-negative twin, and every operator produced at least one accepted
  mutation (no vacuous family);
- each twin's era-conditional gold is exact under today's vocabulary:
  suppressed twins declare no-definite (``channel_today="no_conflict"``),
  expected-flagged-today twins declare their exact today channel plus the
  post-sibling channel;
- a test-defined external operator registers via YAML module+class+params
  and runs without core edits (and registry failures are loud);
- every operator round-trips: applying the recorded inverses to the mutated
  documents restores the original bytes exactly.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from okg.bench.injection import (
    ClaimRow,
    ClaimSnapshot,
    CorpusSnapshot,
    InjectionError,
    RunConfig,
    run_injection,
)
from okg.bench.injection.operators import (
    load_operators,
    load_operators_yaml,
)

GENERATION = "g-clean-0002"

OPS_ALPHA = """# Alpha Service

## Deployment
Version: 4.2
Memory: 512 MB
Released: 2026-03-14
Certified: 2026-01-15
Uptime: 99.9
Endpoint: https://svc.example/api/v1/alpha
Primary node: DB-01
Backup node: cache01
Runbook: RB-7
Upgrade: planned
Status: active

## Configuration
retry_limit: 5
timeout_s: 30

## Burst mode
timeout_s: 15

## Procedure
1. Stop the ingest daemon.
2. Apply the schema migration.
3. Restart the ingest daemon.
4. Clear the local cache if disk pressure is high.

## Metadata
Reviewed: 2026-05-01
Owner: alpha-team
"""

OPS_MIRROR = """# Alpha Service

## Deployment
Version: 4.2
Memory: 512 MB
Released: 2026-03-14
Certified: 2026-01-15
Uptime: 99.9
Primary node: DB-01
Backup node: cache01
Runbook: RB-7
Upgrade: planned
Status: active

## Configuration
timeout_s: 30

## Metadata
Reviewed: 2026-05-01
Owner: alpha-team
"""

OPS_BETA = """# Beta Service

## Deployment
Version: 9.1
Status: retired
"""

_A = "ops_alpha.md"
_M = "ops_mirror.md"
_B = "ops_beta.md"
_AD = "Alpha Service > Deployment"
_MD = "Alpha Service > Deployment"

CLAIMS = (
    ClaimRow("cl_version_alpha_a", "version", "alpha", "4.2", _AD, "known", _A),
    ClaimRow("cl_version_alpha_m", "version", "alpha", "4.2", _MD, "known", _M),
    ClaimRow("cl_version_beta", "version", "beta", "9.1", "Beta Service > Deployment", "known", _B),
    ClaimRow("cl_memory_alpha_a", "memory", "alpha", "512 MB", _AD, "known", _A),
    ClaimRow("cl_memory_alpha_m", "memory", "alpha", "512 MB", _MD, "known", _M),
    ClaimRow("cl_released_alpha", "released", "alpha", "2026-03-14", _AD, "known", _A),
    ClaimRow("cl_released_alpha_m", "released", "alpha", "2026-03-14", _MD, "known", _M),
    ClaimRow("cl_certified_alpha_a", "certified", "alpha", "2026-01-15", _AD, "known", _A),
    ClaimRow("cl_certified_alpha_m", "certified", "alpha", "2026-01-15", _MD, "known", _M),
    ClaimRow("cl_uptime_alpha_a", "uptime", "alpha", "99.9", _AD, "known", _A),
    ClaimRow("cl_uptime_alpha_m", "uptime", "alpha", "99.9", _MD, "known", _M),
    ClaimRow(
        "cl_endpoint_alpha",
        "endpoint",
        "alpha",
        "https://svc.example/api/v1/alpha",
        _AD,
        "known",
        _A,
    ),
    ClaimRow("cl_primary_alpha_a", "primary_node", "alpha", "DB-01", _AD, "known", _A),
    ClaimRow("cl_primary_alpha_m", "primary_node", "alpha", "DB-01", _MD, "known", _M),
    ClaimRow("cl_backup_alpha_a", "backup_node", "alpha", "cache01", _AD, "known", _A),
    ClaimRow("cl_backup_alpha_m", "backup_node", "alpha", "cache01", _MD, "known", _M),
    ClaimRow("cl_runbook_alpha_a", "runbook", "alpha", "RB-7", _AD, "known", _A),
    ClaimRow("cl_runbook_alpha_m", "runbook", "alpha", "RB-7", _MD, "known", _M),
    ClaimRow("cl_upgrade_alpha_a", "upgrade", "alpha", "planned", _AD, "known", _A),
    ClaimRow("cl_upgrade_alpha_m", "upgrade", "alpha", "planned", _MD, "known", _M),
    ClaimRow("cl_status_alpha_a", "status", "alpha", "active", _AD, "known", _A),
    ClaimRow("cl_status_alpha_m", "status", "alpha", "active", _MD, "known", _M),
    ClaimRow("cl_status_beta", "status", "beta", "retired", "Beta Service > Deployment", "known", _B),
    ClaimRow("cl_retry_alpha", "config", "retry_limit", "5", "Alpha Service > Configuration", "known", _A),
    ClaimRow("cl_timeout_alpha_a", "config", "timeout_s", "30", "Alpha Service > Configuration", "known", _A),
    ClaimRow("cl_timeout_alpha_m", "config", "timeout_s", "30", "Alpha Service > Configuration", "known", _M),
    ClaimRow("cl_timeout_burst", "config", "timeout_s_burst", "15", "Alpha Service > Burst mode", "known", _A),
    ClaimRow("cl_step_1", "step", "step_1", "Stop the ingest daemon.", "Alpha Service > Procedure", "known", _A),
    ClaimRow("cl_step_2", "step", "step_2", "Apply the schema migration.", "Alpha Service > Procedure", "known", _A),
    ClaimRow("cl_step_3", "step", "step_3", "Restart the ingest daemon.", "Alpha Service > Procedure", "known", _A),
    ClaimRow(
        "cl_step_4",
        "step",
        "step_4",
        "Clear the local cache if disk pressure is high.",
        "Alpha Service > Procedure",
        "known",
        _A,
    ),
    ClaimRow("cl_reviewed_alpha_a", "reviewed", "alpha", "2026-05-01", "Alpha Service > Metadata", "known", _A),
    ClaimRow("cl_reviewed_alpha_m", "reviewed", "alpha", "2026-05-01", "Alpha Service > Metadata", "known", _M),
    ClaimRow("cl_owner_alpha_a", "owner", "alpha", "alpha-team", "Alpha Service > Metadata", "known", _A),
    ClaimRow("cl_owner_alpha_m", "owner", "alpha", "alpha-team", "Alpha Service > Metadata", "known", _M),
)

POSITIVE_FAMILIES = {
    "injection.numeric_value": "conflict",
    "injection.date_transpose": "conflict",
    "injection.identifier_subst": "conflict",
    "injection.url_swap": "broken_reference",
    "injection.config_swap": "conflict",
    "injection.status_inversion": "conflict",
    "injection.stale_copied_metadata": "conflict",
    "injection.cross_doc_update": "conflict",
    "injection.temporal_mutation": "conflict",
    "injection.malformed_value": "malformed_value",
    "injection.omitted_step": "claim_absent",
}

SUPPRESSED_TWINS = {
    "injection.hn_rounding": "value_compatibility",
    "injection.hn_alias": "canonicalization",
    "injection.hn_different_entity": "bucketing",
    "injection.hn_conditional": "scope_relation",
    "injection.hn_optional_step_removed": "scope_relation",
}

FLAGGED_TODAY_TWINS = {
    "injection.hn_succession": "precedence",
    "injection.hn_plan_vs_observation": "modality_control",
}


def tuned_operators():
    """The full v1 library tuned to THIS module's fixture corpus.

    Core operators ship neutral (empty targeting) since ship-okg-bench; the
    params below replicate the former class defaults so the coverage
    guarantees of this suite are unchanged.
    """
    from okg.bench.injection.operators.library import (
        ConfigSwapOperator,
        CrossDocUpdateOperator,
        DateTransposeOperator,
        HnAliasOperator,
        HnConditionalOperator,
        HnDifferentEntityOperator,
        HnOptionalStepRemovedOperator,
        HnPlanVsObservationOperator,
        HnRoundingOperator,
        HnSuccessionOperator,
        IdentifierSubstOperator,
        MalformedValueOperator,
        NumericValueOperator,
        OmittedStepOperator,
        StaleCopiedMetadataOperator,
        StatusInversionOperator,
        TemporalMutationOperator,
        UrlSwapOperator,
    )

    return (
        NumericValueOperator(predicates=("memory",), counterpart_docs=(_M,)),
        DateTransposeOperator(predicates=("released",), counterpart_docs=(_M,)),
        IdentifierSubstOperator(
            predicates=("primary_node",), source_docs=(_A,),
            counterpart_docs=(_M,), alternatives=("DB-02", "DB-03"),
        ),
        UrlSwapOperator(predicates=("endpoint",)),
        ConfigSwapOperator(
            predicates=("config",), keys=("timeout_s",), source_docs=(_A,),
            counterpart_docs=(_M,), donor_key="retry_limit",
        ),
        StatusInversionOperator(
            predicates=("status",), source_docs=(_A,), counterpart_docs=(_M,)
        ),
        StaleCopiedMetadataOperator(
            predicates=("version", "reviewed"), source_docs=(_A,),
            counterpart_docs=(_M,),
        ),
        CrossDocUpdateOperator(
            predicates=("owner",), source_docs=(_M,), counterpart_docs=(_A,)
        ),
        TemporalMutationOperator(
            predicates=("certified",), source_docs=(_M,), counterpart_docs=(_A,)
        ),
        MalformedValueOperator(predicates=("config",), keys=("retry_limit",)),
        OmittedStepOperator(predicates=("step",), keys=("step_2",)),
        HnRoundingOperator(
            predicates=("uptime",), source_docs=(_M,), counterpart_docs=(_A,)
        ),
        HnAliasOperator(
            predicates=("backup_node",), source_docs=(_M,), counterpart_docs=(_A,)
        ),
        HnDifferentEntityOperator(predicates=("version",), source_docs=(_B,)),
        HnConditionalOperator(predicates=("config",), keys=("timeout_s_burst",)),
        HnSuccessionOperator(
            predicates=("runbook",), source_docs=(_M,), counterpart_docs=(_A,)
        ),
        HnPlanVsObservationOperator(
            predicates=("upgrade",), source_docs=(_M,), counterpart_docs=(_A,)
        ),
        HnOptionalStepRemovedOperator(predicates=("step",), keys=("step_4",)),
    )


@pytest.fixture()
def corpus(tmp_path: Path) -> CorpusSnapshot:
    root = tmp_path / "clean"
    root.mkdir()
    (root / _A).write_text(OPS_ALPHA, encoding="utf-8")
    (root / _M).write_text(OPS_MIRROR, encoding="utf-8")
    (root / _B).write_text(OPS_BETA, encoding="utf-8")
    return CorpusSnapshot.load(root)


@pytest.fixture()
def claims() -> ClaimSnapshot:
    return ClaimSnapshot.from_rows(GENERATION, CLAIMS)


@pytest.fixture()
def library_run(corpus, claims, tmp_path):
    return run_injection(
        corpus=corpus,
        claims=claims,
        operators=tuned_operators(),
        config=RunConfig(seed=42, out_dir=tmp_path / "out"),
        stability_probe=lambda doc, original, mutated: True,
    )


def test_every_operator_family_produces_a_mutation(library_run):
    produced = {record.operator_id for record in library_run.mutations}
    expected = set(POSITIVE_FAMILIES) | set(SUPPRESSED_TWINS) | set(FLAGGED_TODAY_TWINS)
    assert produced == expected
    assert not library_run.rejections, [r.to_json() for r in library_run.rejections]


def test_default_constructed_operators_target_nothing(corpus, claims):
    """Neutral core (ship-okg-bench): with no registry params, every operator
    enumerates ZERO sites — visible loudly in the coverage guard above, which
    would fail if a tuned family ever regressed to relying on defaults."""
    from okg.bench.injection.operators.library import (
        DEFAULT_V1_OPERATOR_TYPES,
    )

    for operator_type in DEFAULT_V1_OPERATOR_TYPES:
        operator = operator_type()
        assert list(operator.sites(corpus, claims)) == [], operator.operator_id


def test_positive_families_declare_engine_vocabulary_gold(library_run):
    by_operator = {}
    for record in library_run.mutations:
        by_operator.setdefault(record.operator_id, []).append(record)
    for operator_id, kind in POSITIVE_FAMILIES.items():
        for record in by_operator[operator_id]:
            assert record.expected.kind == kind, operator_id
            assert record.expected.bucket_key
            assert record.expected.claim_logical_id
            if record.expected.kind == "conflict":
                assert record.expected.channel_today == "definite"


def test_suppressed_twins_declare_no_definite_today(library_run):
    for record in library_run.mutations:
        if record.operator_id in SUPPRESSED_TWINS:
            expected = record.expected
            assert expected.kind == "hard_negative"
            assert expected.mechanism == SUPPRESSED_TWINS[record.operator_id]
            assert expected.channel_today == "no_conflict"
            assert expected.flagged_today is False


def test_flagged_today_twins_declare_exact_today_channel(library_run):
    seen = set()
    for record in library_run.mutations:
        if record.operator_id in FLAGGED_TODAY_TWINS:
            seen.add(record.operator_id)
            expected = record.expected
            assert expected.kind == "hard_negative"
            assert expected.mechanism == FLAGGED_TODAY_TWINS[record.operator_id]
            assert expected.flagged_today is True
            assert expected.channel_today == "definite"
            assert expected.channel_post_siblings in ("resolved_canonical", "no_conflict")
    assert seen == set(FLAGGED_TODAY_TWINS)


def test_stale_copy_mutations_share_one_family(library_run):
    family_ids = {
        record.family_id
        for record in library_run.mutations
        if record.operator_id == "injection.stale_copied_metadata"
    }
    assert len(family_ids) == 1
    records = [
        record
        for record in library_run.mutations
        if record.operator_id == "injection.stale_copied_metadata"
    ]
    assert len(records) == 2


def test_every_mutation_round_trips(library_run, corpus):
    by_doc: dict[str, list] = {}
    for record in library_run.mutations:
        by_doc.setdefault(record.doc, []).append(record)
    for doc, records in by_doc.items():
        mutated = (library_run.mutated_corpus_dir / doc).read_text(encoding="utf-8")
        restored = mutated
        for record in sorted(records, key=lambda r: r.inverse_span.start, reverse=True):
            assert mutated[record.inverse_span.start : record.inverse_span.end] == record.post_text
            restored = (
                restored[: record.inverse_span.start]
                + record.inverse_text
                + restored[record.inverse_span.end :]
            )
        assert restored == corpus.read_text(doc)


def test_library_run_is_deterministic(corpus, claims, tmp_path):
    first = run_injection(
        corpus=corpus,
        claims=claims,
        operators=tuned_operators(),
        config=RunConfig(seed=42, out_dir=tmp_path / "d1"),
        stability_probe=lambda doc, original, mutated: True,
    )
    second = run_injection(
        corpus=corpus,
        claims=claims,
        operators=tuned_operators(),
        config=RunConfig(seed=42, out_dir=tmp_path / "d2"),
        stability_probe=lambda doc, original, mutated: True,
    )
    for name in ("mutations.jsonl", "benchmark-manifest.json"):
        assert (first.out_dir / name).read_bytes() == (second.out_dir / name).read_bytes()


def test_bucket_collision_is_rejected(corpus, claims, tmp_path):
    from okg.bench.injection.operators.library import StatusInversionOperator

    first = StatusInversionOperator(
        predicates=("status",), source_docs=(_A,), counterpart_docs=(_M,)
    )
    second = StatusInversionOperator(
        operator_id="test.second_status_flip",
        predicates=("status",),
        source_docs=(_M,),
        counterpart_docs=(_A,),
    )
    result = run_injection(
        corpus=corpus,
        claims=claims,
        operators=[first, second],
        config=RunConfig(seed=42, out_dir=tmp_path / "out"),
    )
    reasons = {(r.operator_id, r.reason) for r in result.rejections}
    assert ("test.second_status_flip", "bucket_collision") in reasons
    assert {record.operator_id for record in result.mutations} == {"injection.status_inversion"}


EXTERNAL_OPERATOR_MODULE = '''
"""External operator defined outside okg core (registry test)."""
from dataclasses import dataclass

from okg.bench.injection.operators.library import ClaimValueOperator
from okg.bench.injection.types import ClaimRow, ExpectedOutcome, Facets


@dataclass(frozen=True)
class ExternalStatusFlip(ClaimValueOperator):
    operator_id: str = "external.status_flip"
    version: str = "7"
    predicates: tuple = ("status",)
    source_docs: tuple = ("ops_beta.md",)

    def rewrite(self, claim, prf, tag):
        return "active", {"from": claim.value}

    def facets(self, claim):
        return Facets("contradiction", "status", "intra_document", "none", "wrong_fact")

    def expected(self, claim, counterpart):
        return self._conflict(claim, counterpart)
'''


def test_external_operator_registers_via_yaml(corpus, claims, tmp_path, monkeypatch):
    module_dir = tmp_path / "ext"
    module_dir.mkdir()
    (module_dir / "external_injection_ops.py").write_text(
        textwrap.dedent(EXTERNAL_OPERATOR_MODULE), encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(module_dir))
    registry = tmp_path / "operators.yaml"
    registry.write_text(
        textwrap.dedent(
            """
            operators:
              - module: external_injection_ops
                class: ExternalStatusFlip
                params: {}
            """
        ),
        encoding="utf-8",
    )
    operators = load_operators_yaml(registry)
    assert [op.operator_id for op in operators] == ["external.status_flip"]
    result = run_injection(
        corpus=corpus,
        claims=claims,
        operators=list(operators),
        config=RunConfig(seed=3, out_dir=tmp_path / "out"),
    )
    assert [record.operator_id for record in result.mutations] == ["external.status_flip"]
    mutated = (result.mutated_corpus_dir / _B).read_text(encoding="utf-8")
    assert "Status: active" in mutated


def test_registry_failures_are_loud():
    with pytest.raises(InjectionError):
        load_operators({"operators": []})
    with pytest.raises(InjectionError):
        load_operators({"operators": [{"module": "no.such.module", "class": "X"}]})
    with pytest.raises(InjectionError):
        load_operators(
            {
                "operators": [
                    {
                        "module": "okg.bench.injection.operators.library",
                        "class": "NoSuchOperator",
                    }
                ]
            }
        )


def test_registry_params_override_targeting(corpus, claims, tmp_path):
    operators = load_operators(
        {
            "operators": [
                {
                    "module": "okg.bench.injection.operators.library",
                    "class": "StatusInversionOperator",
                    "params": {
                        "predicates": ["status"],
                        "source_docs": ["ops_mirror.md"],
                        "counterpart_docs": ["ops_alpha.md"],
                    },
                }
            ]
        }
    )
    result = run_injection(
        corpus=corpus,
        claims=claims,
        operators=list(operators),
        config=RunConfig(seed=5, out_dir=tmp_path / "out"),
    )
    assert len(result.mutations) == 1
    assert result.mutations[0].doc == _M
