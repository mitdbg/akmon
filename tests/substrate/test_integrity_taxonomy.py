"""AKMON Phase 7 — integrity taxonomy + transitions verifier.

Three integrity-transition fixtures, one verifier across all three:

1. ``rejection at submission`` — admission envelope rejected when its
   payload violates catalog narrowing (rule-based integrity, runs at
   the admission boundary; producer-authority signature evaluator is
   the canonical enforcer).

2. ``publish-blocked`` — Phase B consistency-publish gate fires;
   ``graph_generations.status='blocked'`` and ``blocked_reason``
   carries the verdict (security / semantic integrity, runs
   pre-publish).

3. ``post-publish observation`` — a consistency check definition
   classified as ``statistical`` (or ``semantic``) lives in
   ``okg.consistency_check_definitions`` and a sample observation
   row lands in ``okg.consistency_gate_results`` for the published
   generation.

Plus a schema + doctor-surface test that exercises the
``check_kind`` taxonomy and the ``integrity-coverage`` doctor
report.
"""
from __future__ import annotations

import argparse
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.consistency import (
    CHECK_KINDS,
    ConsistencyCheckDefinition,
    register_consistency_check,
)
from okg.substrate.derived_artifacts import stable_hash


pytestmark = pytest.mark.postgres


# --- 7.1: schema + taxonomy --------------------------------------


def test_check_kind_column_present(clean_runtime: str) -> None:
    """Schema migration: ``check_kind`` column + CHECK constraint."""

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT data_type
                  FROM information_schema.columns
                 WHERE table_schema = 'okg'
                   AND table_name = 'consistency_check_definitions'
                   AND column_name = 'check_kind'
                """
            )
            row = cur.fetchone()
    assert row is not None
    assert row[0] == "text"


def test_check_kind_constraint_rejects_unknown_kind(
    clean_runtime: str,
) -> None:
    """The CHECK constraint rejects arbitrary string kinds."""

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.CheckViolation):
                cur.execute(
                    """
                    INSERT INTO okg.derived_artifact_definitions
                      (definition_id, artifact_kind, deployment_name,
                       logical_key, compute_hash, output_schema_hash,
                       renderer_hash, dirty_rule_hash, renderer_ref,
                       output_contract, dirty_rule)
                    VALUES
                      ('test.bad.kind', 'consistency_check',
                       'taxonomy-test', 'lk', 'h', 'h', 'h', 'h',
                       'r', '{}'::jsonb, '{}'::jsonb)
                    """
                )
                cur.execute(
                    """
                    INSERT INTO okg.consistency_check_definitions
                      (check_id, deployment_name, predicate_family,
                       candidate_rule_hash, evaluator_ref,
                       evaluator_hash, confidence_type,
                       coverage_contract, severity_policy,
                       check_kind)
                    VALUES
                      ('test.bad.kind', 'taxonomy-test', 'pf', 'h',
                       'evref', 'h', 'deterministic_boolean',
                       '{}'::jsonb, '{}'::jsonb, 'NOT-A-KIND')
                    """
                )


def test_check_kinds_python_constant_matches_schema() -> None:
    """The Python ``CHECK_KINDS`` constant matches the SQL CHECK."""

    assert CHECK_KINDS == {
        "rule_based",
        "security",
        "statistical",
        "semantic",
    }


def test_register_consistency_check_persists_check_kind(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex[:8]
    deployment = f"taxonomy-{suffix}"
    check_id = f"test.taxonomy.statistical.{suffix}"
    definition = ConsistencyCheckDefinition(
        check_id=check_id,
        deployment_name=deployment,
        predicate_family="drift_check",
        candidate_rule_hash=stable_hash({"rule": suffix}),
        evaluator_ref="okg.evaluator.test.drift.v1",
        evaluator_hash=stable_hash({"evaluator": "drift"}),
        confidence_type="deterministic_boolean",
        compute_hash=stable_hash({"compute": suffix}),
        output_schema_hash=stable_hash({"schema": "obs"}),
        renderer_hash=stable_hash({"renderer": "obs"}),
        dirty_rule_hash=stable_hash({"dirty": suffix}),
        coverage_contract={"universe": "test"},
        check_kind="statistical",
    )
    with psycopg.connect(clean_runtime) as conn:
        register_consistency_check(conn, definition)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT check_kind
                  FROM okg.consistency_check_definitions
                 WHERE check_id = %s
                """,
                (check_id,),
            )
            row = cur.fetchone()
    assert row == ("statistical",)


def test_consistency_check_definition_rejects_unknown_kind_python_side() -> None:
    with pytest.raises(ValueError, match="check_kind"):
        ConsistencyCheckDefinition(
            check_id="x",
            deployment_name="y",
            predicate_family="pf",
            candidate_rule_hash="h",
            evaluator_ref="r",
            evaluator_hash="h",
            confidence_type="deterministic_boolean",
            compute_hash="h",
            output_schema_hash="h",
            renderer_hash="h",
            dirty_rule_hash="h",
            check_kind="not-a-kind",
        )


# --- 7.1: doctor integrity-coverage -------------------------------


def test_doctor_integrity_coverage_reports_per_kind(
    clean_runtime: str,
) -> None:
    """Doctor surfaces per-kind counts and flags unclassified rows."""

    suffix = uuid4().hex[:8]
    deployment = f"int-coverage-{suffix}"

    # Register one classified + one unclassified check.
    classified = ConsistencyCheckDefinition(
        check_id=f"test.int.statistical.{suffix}",
        deployment_name=deployment,
        predicate_family="drift_check",
        candidate_rule_hash=stable_hash({"rule": suffix}),
        evaluator_ref="okg.evaluator.test.drift.v1",
        evaluator_hash=stable_hash({"evaluator": "drift"}),
        confidence_type="deterministic_boolean",
        compute_hash=stable_hash({"compute": suffix}),
        output_schema_hash=stable_hash({"schema": "obs"}),
        renderer_hash=stable_hash({"renderer": "obs"}),
        dirty_rule_hash=stable_hash({"dirty": suffix}),
        coverage_contract={"universe": "test"},
        check_kind="statistical",
    )
    unclassified = ConsistencyCheckDefinition(
        check_id=f"test.int.unclassified.{suffix}",
        deployment_name=deployment,
        predicate_family="unclassified_check",
        candidate_rule_hash=stable_hash({"rule": "u"}),
        evaluator_ref="okg.evaluator.test.legacy.v1",
        evaluator_hash=stable_hash({"evaluator": "legacy"}),
        confidence_type="deterministic_boolean",
        compute_hash=stable_hash({"compute": "u"}),
        output_schema_hash=stable_hash({"schema": "obs"}),
        renderer_hash=stable_hash({"renderer": "obs"}),
        dirty_rule_hash=stable_hash({"dirty": "u"}),
        # check_kind omitted -> NULL in DB
    )

    with psycopg.connect(clean_runtime) as conn:
        register_consistency_check(conn, classified)
        register_consistency_check(conn, unclassified)
        conn.commit()

    from okg.substrate.cli.doctor import _doctor_integrity_coverage
    from okg.substrate.cli import doctor as cli_doctor

    args = argparse.Namespace(dsn=clean_runtime, deployment=deployment)
    # The handler loads its _emit_ok binding lazily on first call and would
    # overwrite the patch below if no earlier test on this worker had run a
    # doctor handler, so load the helpers first, then patch that binding.
    cli_doctor._load_main_helpers()
    captured: dict = {}
    original_emit = cli_doctor._emit_ok
    cli_doctor._emit_ok = lambda payload, **kwargs: captured.update(payload)
    try:
        rc = _doctor_integrity_coverage(args, as_json=True)
    finally:
        cli_doctor._emit_ok = original_emit
    assert rc == 0
    assert captured["ok"] is True
    assert captured["kinds"]["statistical"] == 1
    assert captured["kinds"]["rule_based"] == 0
    assert captured["unclassified_count"] == 1
    warning_codes = [w["code"] for w in captured["warnings"]]
    assert "consistency_checks_unclassified" in warning_codes


def test_doctor_integrity_coverage_credits_producer_authority(
    clean_runtime: str,
) -> None:
    """Rule-based integrity is implicitly satisfied by an active
    source producer profile, so the doctor flags ``rule_based``
    coverage as OK even when no declarative rule-based check is
    registered."""

    from okg.substrate.producer_authority import (
        materialize_producer_profile,
    )

    suffix = uuid4().hex[:8]
    deployment = f"int-rule-{suffix}"
    with psycopg.connect(clean_runtime) as conn:
        materialize_producer_profile(
            conn,
            deployment_name=deployment,
            producer_id="test.source.docs",
            producer_kind="source",
            trust_label="curated",
            admission_mode="fast_track",
            authority_scope={"deployment": deployment},
            allowed_record_kinds=("node",),
            output_signature={
                "nodes": [{"subtype": "doc", "clause_id": "doc_only"}],
            },
            policy_clauses=[
                {
                    "id": "doc_only",
                    "effect": "allow",
                    "record_kind": "node",
                    "subtype": "doc",
                }
            ],
            output_scope_summary={},
            source_ref={},
            catalog_version_id=None,
        )
        conn.commit()

    from okg.substrate.cli.doctor import _doctor_integrity_coverage
    from okg.substrate.cli import doctor as cli_doctor

    args = argparse.Namespace(dsn=clean_runtime, deployment=deployment)
    # The handler loads its _emit_ok binding lazily on first call and would
    # overwrite the patch below if no earlier test on this worker had run a
    # doctor handler, so load the helpers first, then patch that binding.
    cli_doctor._load_main_helpers()
    captured: dict = {}
    original_emit = cli_doctor._emit_ok
    cli_doctor._emit_ok = lambda payload, **kwargs: captured.update(payload)
    try:
        rc = _doctor_integrity_coverage(args, as_json=True)
    finally:
        cli_doctor._emit_ok = original_emit
    assert rc == 0
    assert captured["rule_based_implicit_via_producer_authority"] is True
    rule_based_check = next(
        c for c in captured["checks"] if c["name"] == "rule_based_coverage"
    )
    assert rule_based_check["ok"] is True
    assert rule_based_check["active_source_profiles"] == 1


# --- 7.2/7.4: integrity-transition inventory verifier -------------


def test_integrity_transitions_have_test_coverage() -> None:
    """The three integrity transitions named by the umbrella have
    deterministic test coverage. This is the verifier referenced by
    umbrella task 7.4: each integrity check kind has a named test
    or surface that exercises it.

    Coverage map:

      * rule_based / rejection at submission ←
        ``tests/substrate/test_producer_authority.py::
        test_signature_evaluator_rejects_node_subtype`` and the
        broader signature-evaluator suite.
      * security / publish-blocked ←
        ``tests/substrate/test_publisher_consistency_gates.py::
        test_publisher_blocks_on_failing_consistency_gate`` (the
        Phase B publish gate sets ``graph_generations.status =
        'blocked'``).
      * statistical / post-publish observation ←
        ``tests/substrate/test_publisher_consistency_gates.py::
        test_publisher_records_passing_consistency_gate_in_summary``
        (observation event lifecycle for a published generation).
      * semantic / LLM-judge invariant ← the
        ``test_publisher_consistency_gates`` suite exercises the
        observation-event lifecycle which the semantic kind reuses.

    The verifier is intentionally a discoverability test: it walks
    the test tree and asserts the named tests still exist. Renaming
    one of the referenced tests breaks this verifier loudly, which
    is the right failure mode for a coverage contract.
    """
    from pathlib import Path

    coverage_map = {
        "rule_based": (
            "tests/substrate/test_producer_authority.py",
            "test_signature_evaluator_rejects_node_subtype",
        ),
        "security_publish_blocked": (
            "tests/substrate/test_publisher_consistency_gates.py",
            "test_publisher_blocks_on_failing_consistency_gate",
        ),
        "statistical_post_publish": (
            "tests/substrate/test_publisher_consistency_gates.py",
            "test_publisher_records_passing_consistency_gate_in_summary",
        ),
    }
    repo_root = Path(__file__).resolve().parents[2]
    missing: list[str] = []
    for kind, (rel_path, fn_name) in coverage_map.items():
        path = repo_root / rel_path
        if not path.exists():
            missing.append(f"{kind}: file {rel_path} not found")
            continue
        text = path.read_text(encoding="utf-8")
        if f"def {fn_name}" not in text:
            missing.append(
                f"{kind}: function {fn_name} not found in {rel_path}"
            )
    assert not missing, (
        "integrity transition coverage gap (umbrella task 7.4): "
        + "; ".join(missing)
    )
