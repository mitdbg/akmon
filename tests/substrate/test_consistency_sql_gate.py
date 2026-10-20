"""Deployment-configured deterministic SQL consistency gates."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
import yaml

from okg.substrate.consistency import (
    ConsistencyGateLoadError,
    DeterministicSqlConsistencyGate,
    SqlConsistencyGateSpec,
    load_consistency_gates,
    load_post_publish_consistency_checks,
    register_consistency_check,
    summarize_consistency_gate_health,
)
from okg.substrate.cli.main import _cmd_consistency_rerun, _cmd_generation
from okg.substrate.derived_artifacts import (
    InlineViewOutput,
    RelationArtifactDefinition,
    RelationRow,
    ViewArtifactDefinition,
    publish_relation_artifact,
    publish_view_artifact,
    stable_hash,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _write_manifest(tmp_path: Path, doc: dict) -> Path:
    out = tmp_path / "dep"
    out.mkdir()
    (out / "deployment.yaml").write_text(yaml.safe_dump(doc))
    return out


def _write_deployments_root(tmp_path: Path, doc: dict) -> Path:
    root = tmp_path / "deployments"
    out = root / "default"
    out.mkdir(parents=True)
    (out / "deployment.yaml").write_text(yaml.safe_dump(doc))
    return root


def _coverage_sql(*, evaluator_invocation_count: int = 0) -> str:
    return f"""
        SELECT
          ('claim_chunks:' || %(target_generation_id)s::text) AS universe_key,
          'exact_postgres'::text AS coverage_basis,
          true AS coverage_complete,
          2::bigint AS eligible_input_count,
          {evaluator_invocation_count}::bigint AS evaluator_invocation_count,
          1::bigint AS all_pairs_upper_bound
    """


def _base_gate_doc(**overrides: object) -> dict:
    gate = {
        "name": "numeric_conflicts_gate",
        "type": "deterministic_sql",
        "check_id": "test.sql.numeric-conflicts",
        "predicate_family": "numeric_claim_conflict",
        "confidence_type": "deterministic_boolean",
        "candidate_sql": """
            SELECT
              'bucket:fuel'::text AS input_scope,
              'block:fuel'::text AS blocking_rule_hash,
              jsonb_build_object(
                'left', 'claim:fuel:a',
                'right', 'claim:fuel:b'
              ) AS participant_roles,
              'numeric_equal_under_condition'::text AS predicate_key,
              'stable_source_anchor_v1'::text AS generation_policy,
              true AS emit_observation,
              'bingo_fuel_minimum_lb_equal'::text AS normalized_predicate,
              jsonb_build_object('left', 2000, 'right', 1500)
                AS normalized_values,
              'high'::text AS severity,
              1.0::numeric AS confidence,
              jsonb_build_object('spans', jsonb_build_array('page:12'))
                AS evidence
        """,
        "coverage_sql": _coverage_sql(evaluator_invocation_count=1),
        "timeout_ms": 3000,
    }
    gate.update(overrides)
    return {"name": "test", "consistency_gates": [gate]}


def _generation_id() -> int:
    return 40_000 + (uuid4().int % 1_000_000)


def _insert_generation(
    conn: psycopg.Connection,
    generation_id: int,
    *,
    deployment_name: str = "test",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations (
              generation_id, deployment_name, status, catalog_version_id,
              published_at, correlation_id
            ) VALUES (
              %(generation_id)s, %(deployment_name)s, 'published', 1,
              now(), %(correlation_id)s
            )
            """,
            {
                "generation_id": generation_id,
                "deployment_name": deployment_name,
                "correlation_id": f"test:sql-gate:{generation_id}",
            },
        )
    conn.commit()


def test_loader_builds_deterministic_sql_gate(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, _base_gate_doc())

    gates = load_consistency_gates(dep)

    assert len(gates) == 1
    assert isinstance(gates[0], DeterministicSqlConsistencyGate)
    assert gates[0].name == "numeric_conflicts_gate"


def test_loader_builds_scope_frontier_sql_gate(tmp_path: Path) -> None:
    dep = _write_manifest(
        tmp_path,
        _base_gate_doc(
            scope_frontier={
                "source": "test.docs",
                "cursor_subject_ids_field": "changed_node_ids",
            },
        ),
    )

    gates = load_consistency_gates(dep)

    frontier = gates[0].spec.scope_frontier
    assert frontier is not None
    assert frontier.source == "test.docs"
    assert frontier.cursor_subject_ids_field == "changed_node_ids"
    assert frontier.require_scoped_cursor is True


def test_loader_builds_relation_input_sql_gate(tmp_path: Path) -> None:
    dep = _write_manifest(
        tmp_path,
        _base_gate_doc(
            relation_inputs={
                "current_publication_facts": {
                    "view_id": "af_pubs.publication_overview",
                    "slot_name": "current_publication_facts",
                }
            },
            candidate_sql="""
                SELECT
                  'bucket:none'::text AS input_scope,
                  'block:none'::text AS blocking_rule_hash,
                  jsonb_build_object('publication', 'publication:none')
                    AS participant_roles,
                  'current_publication_missing_body'::text AS predicate_key,
                  'stable_source_anchor_v1'::text AS generation_policy
                FROM {{relation:current_publication_facts}} r
                WHERE false
            """,
            coverage_sql="""
                SELECT
                  'current_publication_facts'::text AS universe_key,
                  'exact_postgres'::text AS coverage_basis,
                  true AS coverage_complete,
                  count(*)::bigint AS eligible_input_count,
                  0::bigint AS evaluator_invocation_count
                FROM {{relation:current_publication_facts}} r
                WHERE false
            """,
        ),
    )

    gates = load_consistency_gates(dep)

    assert gates[0].spec.relation_inputs[
        "current_publication_facts"
    ].slot_name == "current_publication_facts"


def test_loader_builds_post_publish_sql_check(tmp_path: Path) -> None:
    doc = _base_gate_doc()
    doc["post_publish_consistency_checks"] = doc.pop("consistency_gates")
    dep = _write_manifest(tmp_path, doc)

    checks = load_post_publish_consistency_checks(dep)

    assert len(checks) == 1
    assert checks[0].blocks_publish is False
    assert checks[0].execution_phase == "post_publish"
    assert checks[0].trigger_kind == "post_publish"


def test_loader_rejects_unknown_relation_placeholder(tmp_path: Path) -> None:
    dep = _write_manifest(
        tmp_path,
        _base_gate_doc(
            candidate_sql="""
                SELECT
                  'bucket:none'::text AS input_scope,
                  'block:none'::text AS blocking_rule_hash,
                  jsonb_build_object('publication', 'publication:none')
                    AS participant_roles,
                  'current_publication_missing_body'::text AS predicate_key,
                  'stable_source_anchor_v1'::text AS generation_policy
                FROM {{relation:missing_input}} r
            """,
        ),
    )

    with pytest.raises(ConsistencyGateLoadError) as exc_info:
        load_consistency_gates(dep)

    assert exc_info.value.data["unknown_relation_inputs"] == ["missing_input"]


def test_loader_returns_empty_without_block(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, {"name": "test"})
    assert load_consistency_gates(dep) == []


def test_loader_rejects_write_shaped_sql(tmp_path: Path) -> None:
    dep = _write_manifest(
        tmp_path,
        _base_gate_doc(
            candidate_sql="UPDATE okg.nodes_live SET node_id = node_id",
        ),
    )

    with pytest.raises(ConsistencyGateLoadError) as exc_info:
        load_consistency_gates(dep)

    assert exc_info.value.data["field"] == "candidate_sql"
    assert exc_info.value.data["token"] == "UPDATE"


def test_loader_threads_check_kind_to_definition(tmp_path: Path) -> None:
    # check_kind is accepted by the SQL-gate loader and threaded into the check
    # definition (consistency-gate-durability task.checkkind.loader) so doctor's
    # taxonomy audit sees a real kind, not a NULL legacy row.
    dep = _write_manifest(tmp_path, _base_gate_doc(check_kind="rule_based"))

    gates = load_consistency_gates(dep)

    assert gates[0].spec.check_kind == "rule_based"
    assert gates[0].spec.definition("test").check_kind == "rule_based"


def test_loader_omitted_check_kind_stays_legacy_none(tmp_path: Path) -> None:
    # Absent check_kind registers legacy/NULL (auditable), not an error.
    dep = _write_manifest(tmp_path, _base_gate_doc())

    gates = load_consistency_gates(dep)

    assert gates[0].spec.check_kind is None
    assert gates[0].spec.definition("test").check_kind is None


def test_loader_rejects_invalid_check_kind(tmp_path: Path) -> None:
    # An invalid kind fails LOUDLY at load (ConsistencyGateLoadError), not a
    # downstream ValueError from ConsistencyCheckDefinition.
    dep = _write_manifest(tmp_path, _base_gate_doc(check_kind="not-a-kind"))

    with pytest.raises(ConsistencyGateLoadError) as exc_info:
        load_consistency_gates(dep)

    assert exc_info.value.data["check_kind"] == "not-a-kind"


def test_loader_declared_check_kind_registers_non_null(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    # End-to-end: a config-declared check with a kind lands a NON-NULL
    # consistency_check_definitions row (before, the loader dropped check_kind,
    # so every config-declared check registered as NULL legacy).
    dep = _write_manifest(tmp_path, _base_gate_doc(check_kind="statistical"))
    definition = load_consistency_gates(dep)[0].spec.definition("test")

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
                (definition.check_id,),
            )
            row = cur.fetchone()

    assert row == ("statistical",)


def test_cli_reruns_post_publish_checks_for_existing_generation(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    doc = _base_gate_doc()
    doc["post_publish_consistency_checks"] = doc.pop("consistency_gates")
    deployments_root = _write_deployments_root(tmp_path, doc)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(
            conn,
            generation_id,
            deployment_name="default",
        )

    # Chronos-native rerun resolution: the compat integer id must map to a
    # published Chronos checkpoint (target_generation_id metadata); the
    # legacy okg.graph_generations row alone is no longer resolvable.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.checkpoint_generation(
            generation_id,
            deployment_name="default",
            catalog_version_id=1,
            status="published",
        )

    rc = _cmd_consistency_rerun(
        argparse.Namespace(
            command="consistency-rerun",
            deployment="default",
            generation=generation_id,
            dsn=clean_runtime,
            json=True,
        ),
        as_json=True,
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["generation_id"] == generation_id
    assert payload["check_count"] == 1
    assert payload["checks"][0]["result"] == "evaluated"
    assert payload["checks"][0]["blocks_publish"] is False

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT status
              FROM okg.graph_generations
             WHERE generation_id = %(generation_id)s
            """,
            {"generation_id": generation_id},
        )
        generation_status = cur.fetchone()[0]
        cur.execute(
            """
            SELECT result, blocks_publish, reason
              FROM okg.consistency_gate_results
             WHERE check_id = 'test.sql.numeric-conflicts'
               AND target_generation_id = %(generation_id)s
            """,
            {"generation_id": generation_id},
        )
        gate_row = cur.fetchone()

    assert generation_status == "published"
    assert gate_row == ("evaluated", False, "open_observations")


def test_chronos_native_post_publish_sql_check_reads_current_checkout(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    doc = {
        "name": "default",
        "post_publish_consistency_checks": [{
            "name": "chronos_current_checkout_gate",
            "type": "deterministic_sql",
            "check_id": "test.sql.chronos-current-checkout",
            "predicate_family": "chronos_current_checkout",
            "confidence_type": "deterministic_boolean",
            "candidate_sql": """
                SELECT
                  ('node:' || node_id)::text AS input_scope,
                  'block:chronos-current-checkout'::text
                    AS blocking_rule_hash,
                  jsonb_build_object('node', node_id) AS participant_roles,
                  'chronos_current_checkout_visible'::text AS predicate_key,
                  'generation_live_view_v1'::text AS generation_policy,
                  true AS emit_observation,
                  'chronos_current_checkout_visible'::text
                    AS normalized_predicate,
                  jsonb_build_object('node_id', node_id) AS normalized_values,
                  'low'::text AS severity,
                  1.0::numeric AS confidence,
                  jsonb_build_object('node_id', node_id) AS evidence
                FROM okg.chronos_graph_nodes_current
                WHERE node_id = 'doc:chronos-current'
            """,
            "coverage_sql": """
                SELECT
                  ('chronos_current_checkout:' ||
                    %(target_generation_id)s::text) AS universe_key,
                  'exact_postgres'::text AS coverage_basis,
                  true AS coverage_complete,
                  count(*)::bigint AS eligible_input_count,
                  count(*)::bigint AS candidate_count,
                  0::bigint AS evaluator_invocation_count,
                  count(*)::bigint AS all_pairs_upper_bound
                FROM okg.chronos_graph_nodes_current
                WHERE node_id = 'doc:chronos-current'
            """,
            "timeout_ms": 3000,
        }],
    }
    dep = _write_manifest(tmp_path, doc)
    checks = load_post_publish_consistency_checks(dep)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [
                NodeFact(
                    "doc:chronos-current",
                    "document",
                    {"text": "visible to post publish sql"},
                    {"id": "doc:chronos-current"},
                    {"rev": "1"},
                )
            ],
            source="fixture",
        )

    out = Publisher(
        clean_runtime,
        "default",
        post_publish_consistency_checks=checks,
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert out["status"] == "published"
    check = out["post_publish_consistency_checks"][0]
    assert check["result"] == "evaluated"
    assert check["blocks_publish"] is False
    assert check["open_observation_count"] == 1
    assert check["coverage_complete"] is True

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.chronos_graph_nodes_current
             WHERE node_id = 'doc:chronos-current'
            """
        )
        mirrored_node_count = cur.fetchone()[0]
        cur.execute(
            """
            SELECT result, blocks_publish, open_observation_count
              FROM okg.consistency_gate_results
             WHERE check_id = 'test.sql.chronos-current-checkout'
            """
        )
        gate_row = cur.fetchone()
        cur.execute(
            """
            SELECT indexname
              FROM pg_indexes
             WHERE schemaname = 'okg'
               AND tablename IN (
                 'chronos_graph_nodes_current',
                 'chronos_graph_edges_current'
               )
            """
        )
        mirror_indexes = {row[0] for row in cur.fetchall()}
        cur.execute(
            """
            SELECT relname, reltuples
              FROM pg_class
             WHERE oid IN (
               'okg.chronos_graph_nodes_current'::regclass,
               'okg.chronos_graph_edges_current'::regclass
             )
            """
        )
        mirror_estimates = dict(cur.fetchall())

    assert mirrored_node_count == 1
    assert gate_row == ("evaluated", False, 1)
    assert {
        "chronos_graph_nodes_current_pkey",
        "chronos_graph_nodes_current_subtype_idx",
        "chronos_graph_nodes_current_source_family_idx",
        "chronos_graph_edges_current_pkey",
        "chronos_graph_edges_current_src_type_idx",
        "chronos_graph_edges_current_dst_type_idx",
    } <= mirror_indexes
    assert mirror_estimates["chronos_graph_nodes_current"] >= 0
    assert mirror_estimates["chronos_graph_edges_current"] >= 0


def test_publisher_blocks_on_configured_sql_gate(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    dep = _write_manifest(tmp_path, _base_gate_doc())
    gates = load_consistency_gates(dep)

    out = Publisher(
        clean_runtime,
        "default",
        consistency_gates=gates,
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert out["status"] == "blocked"
    assert out["phase_b_blocked"] is True
    assert out["consistency_gates"][0]["result"] == "evaluated"
    assert out["consistency_gates"][0]["open_observation_count"] == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.observation_current
             WHERE check_id = 'test.sql.numeric-conflicts'
               AND finding_status = 'open'
            """
        )
        open_count = cur.fetchone()[0]
        cur.execute(
            """
            SELECT result, coverage_complete, open_observation_count
              FROM okg.consistency_gate_results
             WHERE check_id = 'test.sql.numeric-conflicts'
            """
        )
        gate_row = cur.fetchone()

    assert open_count == 1
    assert gate_row == ("evaluated", True, 1)


def test_publisher_passes_configured_sql_gate_with_complete_empty_coverage(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    dep = _write_manifest(
        tmp_path,
        _base_gate_doc(
            candidate_sql="""
                SELECT
                  'bucket:none'::text AS input_scope,
                  'block:none'::text AS blocking_rule_hash,
                  jsonb_build_object('left', 'claim:none:a')
                    AS participant_roles,
                  'numeric_equal_under_condition'::text AS predicate_key,
                  'stable_source_anchor_v1'::text AS generation_policy
                WHERE false
            """,
            coverage_sql=_coverage_sql(evaluator_invocation_count=0),
        ),
    )
    gates = load_consistency_gates(dep)

    out = Publisher(
        clean_runtime,
        "default",
        consistency_gates=gates,
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert out["status"] == "published"
    assert out["phase_b_blocked"] is False
    assert out["consistency_gates"][0]["result"] == "evaluated"
    assert out["consistency_gates"][0]["preflight"]["candidate_row_count"] == 0


def test_sql_gate_consumes_published_view_relation_input(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    generation_id = _generation_id()
    view_id = f"test.publication_overview.{uuid4().hex}"
    relation_id = f"{view_id}.current_publication_facts"
    relation_definition = RelationArtifactDefinition(
        definition_id=f"relation:{relation_id}",
        deployment_name="test",
        logical_relation_id=relation_id,
        compute_hash=stable_hash(
            {"relation_id": relation_id, "compute": "test"}
        ),
        output_schema_hash=stable_hash(
            {
                "logical_relation_id": relation_id,
                "columns": [
                    "node_id",
                    "pub_number",
                    "title",
                    "has_body_text",
                ],
            }
        ),
        renderer_hash=stable_hash({"renderer": "jsonb-relation"}),
        dirty_rule_hash=stable_hash(
            {"scope_key": "overview:current_publications"}
        ),
        output_contract={
            "logical_relation_id": relation_id,
            "columns": [
                "node_id",
                "pub_number",
                "title",
                "has_body_text",
            ],
        },
        dirty_rule={"scope_key": "overview:current_publications"},
    )
    view_definition = ViewArtifactDefinition(
        definition_id=f"view:{view_id}",
        deployment_name="test",
        view_id=view_id,
        view_name="Publication Overview",
        compute_hash=stable_hash({"view_id": view_id, "compute": "test"}),
        output_schema_hash=stable_hash(
            {"slots": ["current_publication_facts"]}
        ),
        renderer_hash=stable_hash({"renderer": "relation-ref"}),
        dirty_rule_hash=stable_hash({"scope_key": "overview"}),
        output_contract={"slots": ["current_publication_facts"]},
        dirty_rule={"scope_key": "overview"},
    )
    dep = _write_manifest(
        tmp_path,
        _base_gate_doc(
            name="current_publication_body_gate",
            check_id="test.sql.current-publication-body",
            predicate_family="publication_metadata_completeness",
            relation_inputs={
                "current_publication_facts": {
                    "view_id": view_id,
                    "slot_name": "current_publication_facts",
                }
            },
            candidate_sql="""
                SELECT
                  ('publication:' || r.row_key)::text AS input_scope,
                  'block:current_publication_body'::text
                    AS blocking_rule_hash,
                  jsonb_build_object('publication', r.row_key)
                    AS participant_roles,
                  'current_publication_missing_body'::text AS predicate_key,
                  'stable_source_anchor_v1'::text AS generation_policy,
                  true AS emit_observation,
                  'current_publication_missing_body'::text
                    AS normalized_predicate,
                  jsonb_build_object(
                    'node_id', r.row_key,
                    'pub_number', r.row_data->>'pub_number'
                  ) AS normalized_values,
                  'medium'::text AS severity,
                  1.0::numeric AS confidence,
                  jsonb_build_object(
                    'source', 'current_publication_facts',
                    'scope_key', r.scope_key
                  ) AS evidence
                FROM {{relation:current_publication_facts}} r
                WHERE r.definition_id =
                        %(current_publication_facts_definition_id)s
                  AND r.target_generation_id =
                        %(current_publication_facts_target_generation_id)s
                  AND COALESCE(
                        (r.row_data->>'has_body_text')::boolean,
                        false
                      ) = false
            """,
            coverage_sql="""
                SELECT
                  ('current_publication_facts:' ||
                    %(target_generation_id)s::text) AS universe_key,
                  'exact_postgres'::text AS coverage_basis,
                  true AS coverage_complete,
                  count(*)::bigint AS eligible_input_count,
                  count(*) FILTER (
                    WHERE COALESCE(
                      (r.row_data->>'has_body_text')::boolean,
                      false
                    ) = false
                  )::bigint AS candidate_count,
                  0::bigint AS evaluator_invocation_count,
                  count(*)::bigint AS all_pairs_upper_bound
                FROM {{relation:current_publication_facts}} r
                WHERE r.definition_id =
                        %(current_publication_facts_definition_id)s
                  AND r.target_generation_id =
                        %(current_publication_facts_target_generation_id)s
            """,
        ),
    )
    gate = load_consistency_gates(dep)[0]

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        relation = publish_relation_artifact(
            conn,
            relation_definition,
            target_generation_id=generation_id,
            rows=[
                RelationRow(
                    scope_key="overview:current_publications",
                    row_key="publication:afi10-999",
                    row_data={
                        "node_id": "publication:afi10-999",
                        "pub_number": "AFI 10-999",
                        "title": "Current Test Publication",
                        "has_body_text": False,
                    },
                )
            ],
        )
        view = publish_view_artifact(
            conn,
            view_definition,
            target_generation_id=generation_id,
            outputs=[
                InlineViewOutput.relation_ref(
                    slot_name="current_publication_facts",
                    upstream_definition_id=relation_definition.definition_id,
                    logical_relation_id=relation_id,
                    storage_ref={
                        "schema": relation.schema_name,
                        "relation": relation.relation_name,
                        "target_generation_id": generation_id,
                        "upstream_manifest_id": str(relation.manifest_id),
                        "upstream_target_generation_id": generation_id,
                    },
                    row_count=relation.row_count,
                )
            ],
        )

        evaluation = gate.evaluate(
            conn,
            target_generation_id=generation_id,
            catalog_version_id=1,
            deployment_name="test",
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dependency_manifest_ids, metrics
                  FROM okg.derived_artifact_runs
                 WHERE definition_id = 'test.sql.current-publication-body'
                   AND target_generation_id = %(generation_id)s
                """,
                {"generation_id": generation_id},
            )
            dependency_ids, metrics = cur.fetchone()
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.observation_current
                 WHERE check_id = 'test.sql.current-publication-body'
                   AND finding_status = 'open'
                """
            )
            open_count = cur.fetchone()[0]

    assert evaluation.result == "evaluated"
    assert evaluation.open_observation_count == 1
    assert evaluation.preflight["relation_inputs"][
        "current_publication_facts"
    ]["relation_definition_id"] == relation_definition.definition_id
    assert open_count == 1
    assert set(dependency_ids) == {view.manifest_id, relation.manifest_id}
    assert metrics["relation_inputs"]["current_publication_facts"][
        "relation_manifest_id"
    ] == str(relation.manifest_id)


def test_publisher_blocks_configured_sql_gate_missing_coverage(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    dep = _write_manifest(
        tmp_path,
        _base_gate_doc(
            candidate_sql="""
                SELECT
                  'bucket:none'::text AS input_scope,
                  'block:none'::text AS blocking_rule_hash,
                  jsonb_build_object('left', 'claim:none:a')
                    AS participant_roles,
                  'numeric_equal_under_condition'::text AS predicate_key,
                  'stable_source_anchor_v1'::text AS generation_policy
                WHERE false
            """,
            coverage_sql=_coverage_sql(evaluator_invocation_count=0)
            + " WHERE false",
        ),
    )
    gates = load_consistency_gates(dep)

    out = Publisher(
        clean_runtime,
        "default",
        consistency_gates=gates,
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert out["status"] == "blocked"
    assert out["phase_b_blocked"] is True
    assert out["consistency_gates"][0]["result"] == "blocked_error"
    assert out["consistency_gates"][0]["reason"] == "gate_error"

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, error->>'message'
              FROM okg.derived_artifact_runs
             WHERE definition_id = 'test.sql.numeric-conflicts'
            """
        )
        run_row = cur.fetchone()

    assert run_row == (
        "failed",
        "coverage_sql must return exactly one row; got 0",
    )


def test_generation_describe_surfaces_consistency_gate_cause(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    dep = _write_manifest(tmp_path, _base_gate_doc())
    gates = load_consistency_gates(dep)
    out = Publisher(
        clean_runtime,
        "default",
        consistency_gates=gates,
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert out["status"] == "blocked"

    rc = _cmd_generation(
        argparse.Namespace(
            command="generation",
            subcmd="describe",
            generation_id=out["generation_id"],
            dsn=clean_runtime,
            deployment=None,
            json=True,
        ),
        as_json=True,
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert rc == 0
    assert payload["status"] == "blocked"
    consistency = payload["consistency"]
    assert consistency["gate_count"] == 1
    assert consistency["blocked_gate_count"] == 1
    gate = consistency["gates"][0]
    assert gate["gate_name"] == "numeric_conflicts_gate"
    assert gate["result"] == "evaluated"
    assert gate["reason"] == "open_observations"
    assert gate["coverage"]["coverage_basis"] == "exact_postgres"
    assert gate["coverage"]["eligible_input_count"] == 2
    assert gate["open_observation_count"] == 1
    assert gate["open_observations"][0]["input_scope"] == "bucket:fuel"
    assert gate["health"]["consecutive_blocked"] == 1
    assert gate["health"]["degraded"] is False


def test_operational_backpressure_short_circuits_degraded_sql_gate(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    dep = _write_manifest(
        tmp_path,
        _base_gate_doc(
            candidate_sql="""
                SELECT
                  'bucket:none'::text AS input_scope,
                  'block:none'::text AS blocking_rule_hash,
                  jsonb_build_object('left', 'claim:none:a')
                    AS participant_roles,
                  'numeric_equal_under_condition'::text AS predicate_key,
                  'stable_source_anchor_v1'::text AS generation_policy
                WHERE false
            """,
            coverage_sql=_coverage_sql(evaluator_invocation_count=0)
            + " WHERE false",
            backpressure={
                "lookback": 5,
                "max_consecutive_blocked": 1,
                "action": "block_without_evaluator",
            },
        ),
    )
    gates = load_consistency_gates(dep)

    outputs = [
        Publisher(
            clean_runtime,
            "default",
            consistency_gates=gates,
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle()
        for _ in range(3)
    ]

    assert [out["status"] for out in outputs] == [
        "blocked",
        "blocked",
        "blocked",
    ]
    assert outputs[0]["consistency_gates"][0]["reason"] == "gate_error"
    assert outputs[1]["consistency_gates"][0]["reason"] == "gate_error"
    assert (
        outputs[2]["consistency_gates"][0]["reason"]
        == "gate_degraded_backpressure"
    )
    assert outputs[2]["consistency_gates"][0]["preflight"][
        "backpressure_health"
    ]["degraded"] is True

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT target_generation_id, status, error->>'message', metrics
              FROM okg.derived_artifact_runs
             WHERE definition_id = 'test.sql.numeric-conflicts'
             ORDER BY target_generation_id
            """
        )
        run_rows = cur.fetchall()
        cur.execute(
            """
            SELECT reason, details
              FROM okg.consistency_gate_results
             WHERE check_id = 'test.sql.numeric-conflicts'
             ORDER BY target_generation_id
            """
        )
        gate_rows = cur.fetchall()

    assert [row[1] for row in run_rows] == ["failed", "failed", "blocked"]
    assert run_rows[2][2] is None
    assert run_rows[2][3]["backpressure"]["health"]["degraded"] is True
    assert [row[0] for row in gate_rows] == [
        "gate_error",
        "gate_error",
        "gate_degraded_backpressure",
    ]
    assert gate_rows[2][1]["backpressure"]["health"][
        "consecutive_operational_blocked"
    ] == 2


# --- consistency-gate-durability task.backpressure.probe --------------------
#
# The backpressure policy is a PROBING BREAKER, not a latch: a degraded gate
# short-circuits, but every `probe_every` short-circuits it RE-RUNS its evaluator
# once (a probe). A passing probe clears degradation with no config change or row
# deletion; a failing probe re-enters short-circuit (exactly one probe per
# interval). Probe rows are identifiable (is_probe) and counted separately.

_CANDIDATE_NONE_SQL = """
    SELECT
      'bucket:none'::text AS input_scope,
      'block:none'::text AS blocking_rule_hash,
      jsonb_build_object('left', 'claim:none:a') AS participant_roles,
      'numeric_equal_under_condition'::text AS predicate_key,
      'stable_source_anchor_v1'::text AS generation_policy
    WHERE false
"""


def _probe_spec(
    *,
    failing: bool = False,
    gated: bool = False,
    probe_every: int = 2,
) -> SqlConsistencyGateSpec:
    coverage = _coverage_sql(evaluator_invocation_count=0)
    if failing:
        # 0 coverage rows -> "coverage_sql must return exactly one row" -> the
        # evaluator errors operationally (a real, recurring failure).
        coverage = coverage + "\n        WHERE false"
    elif gated:
        # Evaluator health is gated by a session-temp control table: a present
        # row => 0 coverage rows => operational error; empty => 1 row => passes.
        # Lets a test drive the gate degraded, then CLEAR the failure in place.
        coverage = (
            coverage
            + "\n        WHERE NOT EXISTS (SELECT 1 FROM pg_temp.probe_fail_control)"
        )
    return SqlConsistencyGateSpec(
        name="probe_gate",
        check_id="test.sql.probe-gate",
        predicate_family="numeric_claim_conflict",
        confidence_type="deterministic_boolean",
        candidate_sql=_CANDIDATE_NONE_SQL,
        coverage_sql=coverage,
        max_consecutive_blocked=1,
        health_lookback=20,
        backpressure_action="block_without_evaluator",
        backpressure_probe_every=probe_every,
    )


def test_backpressure_probe_recovers_degraded_gate_without_config_change(
    clean_runtime: str,
) -> None:
    spec = _probe_spec(gated=True, probe_every=2)
    gate = DeterministicSqlConsistencyGate(spec, blocks_publish=True)
    definition = spec.definition("test")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE probe_fail_control (x int)")
            cur.execute("INSERT INTO probe_fail_control VALUES (1)")  # fail mode

        # Drive degraded THROUGH THE REAL GATE via two consecutive operational
        # blocks (the evaluator errors while the control row is present, max=1).
        gate.evaluate(conn, target_generation_id=200, catalog_version_id=1,
                      deployment_name="test")
        gate.evaluate(conn, target_generation_id=201, catalog_version_id=1,
                      deployment_name="test")
        pre = summarize_consistency_gate_health(
            conn, check_id=definition.check_id, gate_name=gate.name,
            lookback=20, max_consecutive_blocked=1, probe_every=2,
        )
        assert pre.degraded is True

        # Clear the underlying failure IN PLACE -- no config change, no gate-row
        # deletion (only the evaluator's own control state changes).
        with conn.cursor() as cur:
            cur.execute("DELETE FROM probe_fail_control")

        # Two short-circuits, then the third run PROBES (probe_every=2) and, with
        # the failure now cleared, the probe re-runs the evaluator and passes.
        first = gate.evaluate(conn, target_generation_id=202, catalog_version_id=1,
                              deployment_name="test")
        second = gate.evaluate(conn, target_generation_id=203, catalog_version_id=1,
                               deployment_name="test")
        probe = gate.evaluate(conn, target_generation_id=204, catalog_version_id=1,
                              deployment_name="test")

        recovered = summarize_consistency_gate_health(
            conn, check_id=definition.check_id, gate_name=gate.name,
            lookback=20, max_consecutive_blocked=1, probe_every=2,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT target_generation_id, result, reason, is_probe
                  FROM okg.consistency_gate_results
                 WHERE check_id = %s AND gate_name = %s
                   AND target_generation_id IN (202, 203, 204)
                 ORDER BY target_generation_id
                """,
                (definition.check_id, gate.name),
            )
            rows = cur.fetchall()

    # The two short-circuits skipped the evaluator; NOT probes.
    assert first.is_probe is False and second.is_probe is False
    assert rows[0][2] == "gate_degraded_backpressure" and rows[0][3] is False
    assert rows[1][2] == "gate_degraded_backpressure" and rows[1][3] is False
    # The third run is a PROBE that re-ran the (now-passing) evaluator.
    assert probe.is_probe is True
    assert probe.result == "evaluated"
    assert rows[2][1] == "evaluated" and rows[2][3] is True
    # Degradation cleared with NO config change and NO row deletion.
    assert recovered.degraded is False
    assert recovered.probe_run_count == 1
    assert recovered.probe_pass_count == 1
    assert recovered.probe_block_count == 0


def test_backpressure_failing_probe_reenters_short_circuit_one_per_interval(
    clean_runtime: str,
) -> None:
    spec = _probe_spec(failing=True, probe_every=2)
    gate = DeterministicSqlConsistencyGate(spec, blocks_publish=True)
    definition = spec.definition("test")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # 8 runs: 2 real operational blocks, then short-circuits with a probe
        # every `probe_every` (=2). Every probe fails (evaluator still broken).
        for i in range(8):
            gate.evaluate(
                conn, target_generation_id=300 + i, catalog_version_id=1,
                deployment_name="test",
            )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT target_generation_id, result, reason, is_probe
                  FROM okg.consistency_gate_results
                 WHERE check_id = %s AND gate_name = %s
                 ORDER BY target_generation_id
                """,
                (definition.check_id, gate.name),
            )
            rows = cur.fetchall()
        health = summarize_consistency_gate_health(
            conn, check_id=definition.check_id, gate_name=gate.name,
            lookback=20, max_consecutive_blocked=1, probe_every=2,
        )

    reasons = [r[2] for r in rows]
    is_probe = [r[3] for r in rows]
    # 2 real errors, 2 short-circuits, a probe, 2 short-circuits, a probe.
    assert is_probe == [False, False, False, False, True, False, False, True]
    assert reasons == [
        "gate_error", "gate_error",
        "gate_degraded_backpressure", "gate_degraded_backpressure",
        "gate_error",  # probe #1 -- re-ran the evaluator, still failed
        "gate_degraded_backpressure", "gate_degraded_backpressure",
        "gate_error",  # probe #2
    ]
    # Every probe re-ran the evaluator and blocked operationally (a failing probe).
    probe_results = [r[1] for r, p in zip(rows, is_probe) if p]
    assert probe_results == ["blocked_error", "blocked_error"]
    # Exactly one probe per interval: two probes, two short-circuits between them.
    assert health.probe_run_count == 2
    assert health.probe_block_count == 2
    assert health.probe_pass_count == 0
    # Still degraded (the failure never cleared) but never permanently latched.
    assert health.degraded is True


def test_backpressure_probe_every_parsed_from_config(tmp_path: Path) -> None:
    dep = _write_manifest(
        tmp_path,
        _base_gate_doc(backpressure={
            "lookback": 10,
            "max_consecutive_blocked": 1,
            "action": "block_without_evaluator",
            "probe_every": 3,
        }),
    )
    gate = load_consistency_gates(dep)[0]
    assert gate.spec.backpressure_probe_every == 3
    assert gate._probe_every() == 3


def test_backpressure_probe_every_invalid_raises(tmp_path: Path) -> None:
    dep = _write_manifest(
        tmp_path,
        _base_gate_doc(backpressure={"probe_every": 0}),
    )
    with pytest.raises(ConsistencyGateLoadError):
        load_consistency_gates(dep)


def test_backpressure_probe_every_env_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No config probe_every -> resolve from the env override, else the default.
    dep = _write_manifest(tmp_path, _base_gate_doc())
    gate = load_consistency_gates(dep)[0]
    assert gate.spec.backpressure_probe_every is None
    monkeypatch.setenv("OKG_CONSISTENCY_BACKPRESSURE_PROBE_EVERY", "7")
    assert gate._probe_every() == 7
    monkeypatch.delenv("OKG_CONSISTENCY_BACKPRESSURE_PROBE_EVERY")
    assert gate._probe_every() == 5  # DEFAULT_BACKPRESSURE_PROBE_EVERY


def test_backpressure_probe_every_nonpositive_spec_falls_back_to_default() -> None:
    """Review F2: the config-FILE loader rejects <=0, but a spec constructed
    directly in code with a non-positive probe_every must NOT silently disable
    probing — _probe_every honors the docstring and falls back to the default
    (the real opt-out is action: run, not a zero cadence)."""
    for bad in (0, -1):
        spec = _probe_spec(gated=True, probe_every=bad)
        gate = DeterministicSqlConsistencyGate(spec, blocks_publish=True)
        assert gate._probe_every() == 5  # DEFAULT, not the non-positive value


def test_generation_describe_surfaces_backpressure_health(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    dep = _write_manifest(
        tmp_path,
        _base_gate_doc(
            candidate_sql="""
                SELECT
                  'bucket:none'::text AS input_scope,
                  'block:none'::text AS blocking_rule_hash,
                  jsonb_build_object('left', 'claim:none:a')
                    AS participant_roles,
                  'numeric_equal_under_condition'::text AS predicate_key,
                  'stable_source_anchor_v1'::text AS generation_policy
                WHERE false
            """,
            coverage_sql=_coverage_sql(evaluator_invocation_count=0)
            + " WHERE false",
            backpressure={
                "lookback": 5,
                "max_consecutive_blocked": 1,
                "action": "block_without_evaluator",
            },
        ),
    )
    gates = load_consistency_gates(dep)
    outputs = [
        Publisher(
            clean_runtime,
            "default",
            consistency_gates=gates,
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        ).cycle()
        for _ in range(3)
    ]

    rc = _cmd_generation(
        argparse.Namespace(
            command="generation",
            subcmd="describe",
            generation_id=outputs[-1]["generation_id"],
            dsn=clean_runtime,
            deployment=None,
            json=True,
        ),
        as_json=True,
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    consistency = payload["consistency"]
    assert consistency["degraded_gate_count"] == 1
    gate = consistency["gates"][0]
    assert gate["reason"] == "gate_degraded_backpressure"
    assert gate["backpressure"]["action"] == "block_without_evaluator"
    assert gate["health"]["degraded"] is True
    assert gate["health"]["consecutive_operational_blocked"] == 3


def test_frontierless_check_reading_scope_context_runs_full(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """The cern-twiki shape: check SQL references the pg_temp scope tables but
    declares NO scope_frontier. The staging must create the tables with a real
    scope_active=false row — an absent table errors the run, and an EMPTY
    scope_context NULLs both CTE branches into a silent zero-row check."""
    doc = {
        "name": "test",
        "post_publish_consistency_checks": [{
            "name": "frontierless_scope_context_check",
            "type": "deterministic_sql",
            "check_id": f"test.frontierless.{uuid4().hex[:8]}",
            "predicate_family": "frontierless",
            "confidence_type": "deterministic_boolean",
            "candidate_sql": """
                WITH scope_context AS MATERIALIZED (
                  SELECT scope_active
                    FROM pg_temp.okg_sql_gate_scope_context
                   LIMIT 1
                ),
                scoped AS (
                  SELECT s.subject_id AS node_id
                    FROM pg_temp.okg_sql_gate_subject_scope s
                   WHERE (SELECT scope_active FROM scope_context)
                ),
                full_rows AS (
                  SELECT 'row:one'::text AS node_id
                   WHERE NOT (SELECT scope_active FROM scope_context)
                ),
                rows_union AS (
                  SELECT * FROM scoped UNION ALL SELECT * FROM full_rows
                )
                SELECT
                  ('frontierless:' || node_id) AS input_scope,
                  'block:frontierless'::text AS blocking_rule_hash,
                  jsonb_build_object('subject', node_id) AS participant_roles,
                  'frontierless_present'::text AS predicate_key,
                  'stable_source_anchor_v1'::text AS generation_policy,
                  true AS emit_observation,
                  'frontierless_row'::text AS normalized_predicate,
                  jsonb_build_object('node_id', node_id) AS normalized_values,
                  'low'::text AS severity,
                  1.0::numeric AS confidence,
                  '{}'::jsonb AS evidence
                  FROM rows_union
            """,
            "coverage_sql": """
                SELECT
                  ('frontierless:' || %(target_generation_id)s::text)
                    AS universe_key,
                  'exact_postgres'::text AS coverage_basis,
                  true AS coverage_complete,
                  1::bigint AS eligible_input_count,
                  0::bigint AS evaluator_invocation_count,
                  1::bigint AS all_pairs_upper_bound
            """,
            "timeout_ms": 3000,
        }],
    }
    dep_dir = _write_manifest(tmp_path, doc)
    checks = load_post_publish_consistency_checks(dep_dir)
    assert len(checks) == 1
    generation_id = _generation_id()
    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id, deployment_name="default")
        evaluation = checks[0].evaluate(
            conn,
            target_generation_id=generation_id,
            catalog_version_id=1,
            deployment_name="default",
        )
    # The full-path CTE actually ran: one offending row, not a silent zero.
    assert evaluation.preflight["candidate_row_count"] == 1
    frontier = evaluation.preflight["scope_frontier"]
    assert frontier["configured"] is False
    assert frontier["scope_active"] is False
    assert frontier["reason"] == "not_configured"
