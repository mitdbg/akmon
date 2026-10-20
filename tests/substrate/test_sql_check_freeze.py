"""The SQL-blob check path is frozen (consistency-checks-as-ir,
task.ir.freeze-sql-path).

Loading any deterministic_sql check warns with a migration pointer naming the
PACT; deployment lint BLOCKS a NEW SQL check (any check_id outside the landed
fleet's grandfather allowlist) while the existing fleet passes with the warning
only; both messages name the IR authoring surface and the equivalence harness.
No database needed — the loader and lint are static.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.consistency import load_post_publish_consistency_checks
from okg.substrate.consistency.sql_gate import (
    FROZEN_SQL_CHECK_ALLOWLIST,
    load_consistency_gates,
)
from okg.substrate.deployment_lint import lint_deployment
from tests.private_deployments import name_params

_REPO_ROOT = Path(__file__).resolve().parents[2]

_MINIMAL_CANDIDATE_SQL = """
    SELECT
      'bucket:demo'::text AS input_scope,
      'block:demo'::text AS blocking_rule_hash,
      jsonb_build_object('subject', 'demo') AS participant_roles,
      'demo_predicate'::text AS predicate_key,
      'stable_source_anchor_v1'::text AS generation_policy,
      true AS emit_observation,
      'demo_normalized'::text AS normalized_predicate,
      jsonb_build_object('v', 1) AS normalized_values,
      'low'::text AS severity,
      1.0::numeric AS confidence,
      '{}'::jsonb AS evidence
"""

_MINIMAL_COVERAGE_SQL = """
    SELECT
      ('demo:' || %(target_generation_id)s::text) AS universe_key,
      'exact_postgres'::text AS coverage_basis,
      true AS coverage_complete,
      1::bigint AS eligible_input_count,
      0::bigint AS evaluator_invocation_count,
      1::bigint AS all_pairs_upper_bound
"""


def _sql_check(check_id: str) -> dict:
    return {
        "name": "demo_check",
        "type": "deterministic_sql",
        "check_id": check_id,
        "predicate_family": "demo_family",
        "confidence_type": "deterministic_boolean",
        "candidate_sql": _MINIMAL_CANDIDATE_SQL,
        "coverage_sql": _MINIMAL_COVERAGE_SQL,
        "timeout_ms": 3000,
    }


def _write_deployment(tmp_path: Path, doc: dict) -> Path:
    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump(doc), encoding="utf-8")
    return dep


def test_loading_sql_checks_warns_with_migration_pointer(tmp_path: Path) -> None:
    dep = _write_deployment(tmp_path, {
        "name": "demo",
        "post_publish_consistency_checks": [_sql_check("demo.some_check.v1")],
    })
    with pytest.warns(DeprecationWarning, match="consistency-checks-as-ir") as rec:
        checks = load_post_publish_consistency_checks(dep)
    assert len(checks) == 1
    text = str(rec[0].message)
    assert "ir_consistency_checks" in text
    assert "okg.substrate.consistency.equivalence" in text


def test_loading_blocking_gates_warns_too(tmp_path: Path) -> None:
    dep = _write_deployment(tmp_path, {
        "name": "demo",
        "consistency_gates": [_sql_check("demo.some_gate.v1")],
    })
    with pytest.warns(DeprecationWarning, match="frozen SQL path"):
        gates = load_consistency_gates(dep)
    assert len(gates) == 1


def _freeze_findings(report: dict) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {"blocker": [], "warning": []}
    for finding in report["findings"]:
        if finding["code"] == "deployment.consistency.new_sql_check":
            out["blocker"].append(finding)
        if finding["code"] == "deployment.consistency.sql_check_frozen":
            out["warning"].append(finding)
    return out


def test_lint_blocks_a_new_sql_check(tmp_path: Path) -> None:
    dep = _write_deployment(tmp_path, {
        "name": "demo",
        "post_publish_consistency_checks": [_sql_check("demo.brand_new.v1")],
    })
    report = lint_deployment(dep)
    freeze = _freeze_findings(report)
    assert len(freeze["blocker"]) == 1
    assert report["ok"] is False
    blocker = freeze["blocker"][0]
    assert blocker["severity"] == "blocker"
    # The message names the IR authoring surface and the equivalence harness.
    joined = " ".join(
        str(blocker.get(k) or "") for k in ("message", "why", "next_action", "expected")
    )
    assert "ir_consistency_checks" in joined
    assert "okg.substrate.consistency.equivalence" in joined
    assert "consistency-checks-as-ir" in joined


def test_lint_warns_only_for_grandfathered_fleet_check(tmp_path: Path) -> None:
    dep = _write_deployment(tmp_path, {
        "name": "cern-twiki",
        "post_publish_consistency_checks": [
            _sql_check("cern_twiki.snapshot_hygiene.v1"),
        ],
    })
    report = lint_deployment(dep)
    freeze = _freeze_findings(report)
    assert freeze["blocker"] == []
    assert len(freeze["warning"]) == 1
    assert freeze["warning"][0]["severity"] == "warning"


def test_reusing_a_grandfathered_id_elsewhere_is_still_blocked(
    tmp_path: Path,
) -> None:
    """The allowlist is (deployment, check_id) pairs: a NEW deployment cannot
    evade the freeze by copying a grandfathered fleet id."""
    dep = _write_deployment(tmp_path, {
        "name": "demo",
        "post_publish_consistency_checks": [
            _sql_check("cern_twiki.snapshot_hygiene.v1"),
        ],
    })
    freeze = _freeze_findings(lint_deployment(dep))
    assert len(freeze["blocker"]) == 1
    assert freeze["warning"] == []


@pytest.mark.parametrize(
    "deployment", name_params("retired_prototype")
)
def test_retired_prototypes_declare_no_sql_checks(deployment: str) -> None:
    """consistency-deployment-currency (task.dep.prototype-disposition) retired
    the retired in-repo prototype's deterministic_sql gates, so the real
    in-repo prototype now declares NO SQL checks — neither a freeze warning nor
    a new-check blocker. Their allowlist pairs were dropped accordingly, so a
    re-added SQL check would (correctly) blocker."""
    report = lint_deployment(
        deployment, deployments_root=_REPO_ROOT / "deployments"
    )
    freeze = _freeze_findings(report)
    assert freeze["blocker"] == []
    assert freeze["warning"] == []
    # The dropped pairs are gone from the allowlist; cern-twiki's remain.
    allowlisted_deployments = {dep for dep, _cid in FROZEN_SQL_CHECK_ALLOWLIST}
    assert deployment not in allowlisted_deployments
