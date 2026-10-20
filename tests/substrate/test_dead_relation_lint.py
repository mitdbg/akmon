"""Deployment lint flags consistency-gate SQL bound to a retired live-graph
relation (consistency-framework-soundness, task.surface.lint).

The chronos-native publisher no longer maintains okg.nodes_live/okg.edges_live
(they exist but stay empty), so a gate reading them scans an empty table and
silently passes — a false-negative publish gate. Lint names the check, the
dead relation, and the maintained surface (okg.graph_nodes/okg.graph_edges) +
the IR-migration fix. A NEW blocking gate is a BLOCKER; the landed fleet's
grandfathered checks (also migrating off the frozen SQL path) are a WARNING so
the fleet's lint is not double-blocked. Static — no database.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.deployment_lint import (
    build_ready_explain_diagnostics,
    lint_deployment,
)
from tests.private_deployments import name

_REPO_ROOT = Path(__file__).resolve().parents[2]

# The real gate SQL shape of a retired in-repo prototype deployment: a candidate_sql that reads the
# retired live tables (FROM okg.nodes_live ... JOIN okg.edges_live e ...).
_RETIRED_PROTOTYPE_SQL = """
    WITH sources AS (
      SELECT n.node_id, n.attrs->>'source_family' AS fam
        FROM okg.nodes_live n
       WHERE n.subtype = 'document'
    ),
    linkage AS (
      SELECT e.src, e.dst
        FROM okg.edges_live e
        JOIN sources s ON s.node_id = e.src
    )
    SELECT
      'bucket:posture'::text AS input_scope,
      'block:posture'::text AS blocking_rule_hash,
      jsonb_build_object('subject', node_id) AS participant_roles,
      'source_posture'::text AS predicate_key,
      'stable_source_anchor_v1'::text AS generation_policy,
      true AS emit_observation,
      'posture_missing'::text AS normalized_predicate,
      jsonb_build_object('node_id', node_id) AS normalized_values,
      'high'::text AS severity,
      1.0::numeric AS confidence,
      '{}'::jsonb AS evidence
      FROM sources
"""

_CLEAN_SQL = _RETIRED_PROTOTYPE_SQL.replace(
    "okg.nodes_live", "okg.graph_nodes"
).replace("okg.edges_live", "okg.graph_edges")


def _blocking_gate(check_id: str, sql: str) -> dict:
    return {
        "name": "posture_gate",
        "type": "deterministic_sql",
        "check_id": check_id,
        "predicate_family": "source_boundary_posture",
        "confidence_type": "deterministic_boolean",
        "candidate_sql": sql,
        "coverage_sql": (
            "SELECT ('u:' || %(target_generation_id)s::text) AS universe_key, "
            "'exact_postgres'::text AS coverage_basis, true AS coverage_complete, "
            "1::bigint AS eligible_input_count, "
            "0::bigint AS evaluator_invocation_count, "
            "1::bigint AS all_pairs_upper_bound"
        ),
        "timeout_ms": 3000,
    }


def _write(tmp_path: Path, doc: dict) -> Path:
    dep = tmp_path / "dep"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text(yaml.safe_dump(doc), encoding="utf-8")
    return dep


def _dead_findings(report: dict) -> list[dict]:
    return [
        f for f in report["findings"]
        if f["code"] == "deployment.consistency.dead_relation_read"
    ]


def test_new_blocking_gate_reading_dead_relation_is_a_blocker(
    tmp_path: Path,
) -> None:
    dep = _write(tmp_path, {
        "name": "demo",
        "consistency_gates": [
            _blocking_gate("demo.posture.v1", _RETIRED_PROTOTYPE_SQL),
        ],
    })
    report = lint_deployment(dep)
    dead = _dead_findings(report)
    assert len(dead) == 1
    finding = dead[0]
    assert finding["severity"] == "blocker"
    assert report["ok"] is False
    # Names the check, BOTH dead relations, and the maintained replacement.
    joined = " ".join(
        str(finding.get(k) or "")
        for k in ("message", "why", "observed", "expected", "next_action")
    )
    assert "demo.posture.v1" in finding["message"]
    assert "okg.nodes_live" in joined and "okg.edges_live" in joined
    assert "okg.graph_nodes" in joined and "okg.graph_edges" in joined
    assert "ir_consistency_checks" in joined
    assert finding["details"]["dead_relations"] == [
        "okg.edges_live", "okg.nodes_live",
    ]


def test_clean_gate_reading_maintained_surface_has_no_finding(
    tmp_path: Path,
) -> None:
    dep = _write(tmp_path, {
        "name": "demo",
        "consistency_gates": [_blocking_gate("demo.posture.v1", _CLEAN_SQL)],
    })
    assert _dead_findings(lint_deployment(dep)) == []


def test_commented_out_dead_read_is_not_a_false_positive(
    tmp_path: Path,
) -> None:
    """A merely commented-out reference must NOT draw a blocker: line comments
    are stripped before scanning."""
    commented = _CLEAN_SQL.replace(
        "FROM okg.graph_nodes",
        "FROM okg.graph_nodes  -- was okg.nodes_live before the cutover",
    )
    dep = _write(tmp_path, {
        "name": "demo",
        "consistency_gates": [_blocking_gate("demo.posture.v1", commented)],
    })
    assert _dead_findings(lint_deployment(dep)) == []


def test_grandfathered_landed_check_is_warning_not_blocker(
    tmp_path: Path,
) -> None:
    # cern-twiki is the only remaining grandfathered (deployment, check_id) pair
    # in the SQL freeze allowlist (a retired in-repo prototype and a since-deleted
    # in-repo deployment were retired by consistency-deployment-currency) — a
    # dead read there warns, so
    # cern-twiki is not double-blocked while its cross-repo IR re-authoring
    # (task.dep.twiki-ir-reauthor) lands.
    dep = _write(tmp_path, {
        "name": "cern-twiki",
        "consistency_gates": [
            _blocking_gate(
                "cern_twiki.snapshot_hygiene.v1",
                _RETIRED_PROTOTYPE_SQL,
            ),
        ],
    })
    report = lint_deployment(dep)
    dead = _dead_findings(report)
    assert len(dead) == 1
    assert dead[0]["severity"] == "warning"
    assert dead[0]["details"]["grandfathered"] is True


def test_retired_prototype_dead_read_is_now_a_blocker(tmp_path: Path) -> None:
    """consistency-deployment-currency (task.dep.prototype-disposition) dropped
    the retired in-repo prototype from the grandfather allowlist, so a NEW
    dead-relation read by it — under its OWN retired check_id — is now a
    BLOCKER, no longer a grandfathered warning."""
    for deployment, check_id in (
        (name("retired_prototype"), name("retired_prototype_check")),
    ):
        dep = _write(tmp_path / deployment, {
            "name": deployment,
            "consistency_gates": [_blocking_gate(check_id, _RETIRED_PROTOTYPE_SQL)],
        })
        report = lint_deployment(dep)
        dead = _dead_findings(report)
        assert len(dead) == 1, (deployment, dead)
        assert dead[0]["severity"] == "blocker", deployment
        assert dead[0]["details"]["grandfathered"] is False, deployment
        assert report["ok"] is False, deployment


def test_dead_relation_finding_surfaces_on_ready_explain_doctor(
    tmp_path: Path,
) -> None:
    """The clause's doctor surfacing: `deployment ready --explain` folds lint
    findings into its diagnostics envelope (build_ready_explain_diagnostics).
    A new gate's blocker propagates (ok=False); a grandfathered gate's warning
    surfaces too (the clause's 'surfaces the warning')."""
    new_dep = _write(tmp_path / "new", {
        "name": "demo",
        "consistency_gates": [_blocking_gate("demo.posture.v1", _RETIRED_PROTOTYPE_SQL)],
    })
    new_env = build_ready_explain_diagnostics(
        {"deployment": "demo", "blockers": [], "warnings": [], "checks": []},
        lint_report=lint_deployment(new_dep),
    )
    new_dead = [
        f for f in new_env["findings"]
        if f["code"] == "deployment.consistency.dead_relation_read"
    ]
    assert new_dead and new_dead[0]["severity"] == "blocker"
    assert new_env["ok"] is False  # the blocker propagates to the doctor view

    gf_dep = _write(tmp_path / "gf", {
        "name": "cern-twiki",
        "consistency_gates": [_blocking_gate(
            "cern_twiki.snapshot_hygiene.v1",
            _RETIRED_PROTOTYPE_SQL,
        )],
    })
    gf_env = build_ready_explain_diagnostics(
        {"deployment": "cern-twiki", "blockers": [], "warnings": [],
         "checks": []},
        lint_report=lint_deployment(gf_dep),
    )
    gf_dead = [
        f for f in gf_env["findings"]
        if f["code"] == "deployment.consistency.dead_relation_read"
    ]
    assert gf_dead and gf_dead[0]["severity"] == "warning"


def test_retired_prototypes_have_no_dead_relation_reads() -> None:
    """consistency-deployment-currency retired the in-repo prototype's
    gates (their vacuous dead-relation consistency_gates blocks were
    removed from the manifest), so it reads no dead relation anymore — zero
    dead-relation findings, the disposition proof."""
    for deployment in (name("retired_prototype"),):
        report = lint_deployment(
            deployment, deployments_root=_REPO_ROOT / "deployments"
        )
        assert _dead_findings(report) == [], f"{deployment}: {report['findings']}"
