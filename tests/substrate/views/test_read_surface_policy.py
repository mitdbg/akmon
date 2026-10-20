from __future__ import annotations

from dataclasses import replace
from itertools import combinations, product
import json
from uuid import uuid4

import psycopg
from psycopg import sql as psycopg_sql
import pytest
from hypothesis import given, strategies as st

from okg.substrate.contracts.deployment_product import build_read_asset, resolve_read_assets
from okg.substrate.derived_artifacts import stable_hash
from okg.substrate.derived_artifacts.output_lineage import source_record_lineage
from okg.substrate.derived_artifacts.relations import (
    LINEAGE_STATUS_AVAILABLE,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.publisher import Publisher
from okg.substrate.views import ViewMaterializer
from okg.substrate.views.read_policy import (
    ReadPolicyRequirements,
    ReadSurfacePolicyError,
    SqlValueParameter,
    build_sql_lineage_envelope,
    build_sql_read_model_asset,
    compose_non_widening_policy,
    execute_sql_read_query,
    materialize_sql_view,
    materialize_and_publish_sql_read_model,
    read_governed_native_view,
    require_non_widening_policy,
    require_result_shape_migration,
    require_sql_lineage_closure,
    sql_source_policy_name,
    validate_sql_read_model,
)
from okg.views.assembly import assemble as default_assemble
from okg.views.planners.base import QuoteSpec, ViewDefinition
from okg.views.types import Anchor
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _source_asset(relation: str = "wave4_dist.source_rows"):
    return build_read_asset(
        owner="wave4",
        asset_id=relation.replace(".", "-"),
        kind="read-model",
        config_schema="okg.deployment_product.asset.read-model.config/v1",
        result_schema="okg.deployment_product.asset.read-model.result/v1",
        capability_schema="okg.deployment_product.asset.read-model.capability/v1",
        output_identity=f"source:{relation}",
        database_object=relation,
        refresh_policy={"mode": "generation"},
        policy_requirements=_policy().to_dict(),
        lineage_sources=[f"fixture:{relation}"],
        config={"source_relation": relation},
        provenance={"source": "distribution"},
    )


def _policy(**overrides):
    values = {
        "audiences": frozenset({"operator", "agent"}),
        "tenants": frozenset({"tenant-a"}),
        "visible_fields": frozenset({"tenant_id", "public_value", "row_count"}),
        "decision_ids": frozenset({"nomos:decision:source"}),
    }
    values.update(overrides)
    return ReadPolicyRequirements(**values)


def _sql_contract(
    statement: str,
    *,
    namespace: str = "wave4_dist",
    source_relation: str | None = None,
    parameters=(),
    result_columns=None,
    policy=None,
    maintenance_role: str = "wave4_maint",
    application_role: str = "wave4_app",
):
    source_relation = source_relation or f"{namespace}.source_rows"
    source = _source_asset(source_relation)
    policy = policy or _policy(
        audiences=frozenset({"operator", "agent", application_role})
    )
    contract = build_sql_read_model_asset(
        owner="wave4",
        asset_id="tenant-summary",
        sql=statement,
        distribution_namespace=namespace,
        object_name="tenant_summary",
        source_assets={source_relation: source},
        source_policies={source_relation: policy},
        source_policy_enforcements={source_relation: "row-security"},
        parameters=parameters,
        result_columns=result_columns or {"tenant_id": "text", "row_count": "bigint"},
        policy=policy,
        maintenance_role=maintenance_role,
        application_role=application_role,
        provenance={"source": "distribution"},
    )
    return source, contract


def test_safe_sql_is_ast_validated_and_lineage_closed() -> None:
    source, contract = _sql_contract(
        "WITH visible AS ("
        "SELECT tenant_id FROM wave4_dist.source_rows "
        "WHERE tenant_id OPERATOR(pg_catalog.=) $1"
        ") SELECT tenant_id, pg_catalog.count(*) AS row_count "
        "FROM visible GROUP BY tenant_id",
        parameters=(SqlValueParameter(1, "tenant", "text"),),
    )
    plan = validate_sql_read_model(
        contract,
        source_assets={"wave4_dist.source_rows": source},
        parameter_values=("tenant-a",),
    )
    envelope = build_sql_lineage_envelope(
        plan,
        source_records=(source_record_lineage("wave4.fixture", {"id": "row-1"}),),
        graph_generation_id=42,
        catalog_version_id=7,
        output_digest="sha256:" + "a" * 64,
    )

    assert plan.asset_id == contract.qualified_id
    assert plan.source_relations == ("wave4_dist.source_rows",)
    assert plan.parameters[0].value_type == "text"
    assert envelope.metadata["source_relations"] == ["wave4_dist.source_rows"]
    assert envelope.decision_ids == ("nomos:decision:source",)


def test_source_policies_are_digest_bound_and_projected_fields_cannot_exceed_them() -> None:
    source, contract = _sql_contract(
        "SELECT secret_value AS public_value FROM wave4_dist.source_rows "
        "WHERE tenant_id OPERATOR(pg_catalog.=) 'tenant-a'",
        result_columns={"public_value": "text"},
    )
    with pytest.raises(ReadSurfacePolicyError) as caught:
        validate_sql_read_model(
            contract,
            source_assets={"wave4_dist.source_rows": source},
        )
    assert caught.value.code == "sql_source_field_policy_mismatch"

    tampered = replace(
        contract,
        config={
            **contract.config,
            "source_relations": {
                "wave4_dist.source_rows": {
                    **contract.config["source_relations"]["wave4_dist.source_rows"],
                    "policy": _policy(tenants=frozenset({"tenant-b"})).to_dict(),
                }
            },
        },
    )
    with pytest.raises(Exception):
        validate_sql_read_model(
            tampered,
            source_assets={"wave4_dist.source_rows": source},
        )


UNSAFE_SQL = (
    ("SELECT * FROM wave4_dist.source_rows; SELECT 1", "sql_multi_statement"),
    ("INSERT INTO wave4_dist.source_rows VALUES ('x')", "sql_statement_forbidden"),
    ("COPY wave4_dist.source_rows TO STDOUT", "sql_statement_forbidden"),
    ("GRANT SELECT ON wave4_dist.source_rows TO public", "sql_statement_forbidden"),
    ("CREATE EXTENSION dblink", "sql_statement_forbidden"),
    ("CREATE TEMP TABLE stolen AS SELECT 1", "sql_statement_forbidden"),
    ("SELECT * INTO wave4_dist.stolen FROM wave4_dist.source_rows", "sql_select_into_forbidden"),
    ("SELECT * FROM wave4_dist.source_rows FOR UPDATE", "sql_row_lock_forbidden"),
    (
        "WITH changed AS (DELETE FROM wave4_dist.source_rows RETURNING tenant_id) "
        "SELECT tenant_id, pg_catalog.count(*) AS row_count FROM changed GROUP BY tenant_id",
        "sql_data_modifying_cte",
    ),
    ("SELECT random() AS tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.source_rows", "sql_function_forbidden"),
    ("SELECT nextval('seq') AS tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.source_rows", "sql_function_forbidden"),
    ("SELECT wave4_dist.user_fn(tenant_id) AS tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.source_rows GROUP BY tenant_id", "sql_function_forbidden"),
    ("SELECT tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.source_rows WHERE tenant_id @@ 'x' GROUP BY tenant_id", "sql_operator_forbidden"),
    ("SELECT tenant_id, pg_catalog.count(*) AS row_count FROM other_namespace.source_rows GROUP BY tenant_id", "sql_cross_namespace_relation"),
    ("SELECT tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.undeclared GROUP BY tenant_id", "sql_relation_lineage_mismatch"),
    ("SELECT 'postgres://user:password@host/db' AS tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.source_rows", "sql_literal_secret"),
    ("SELECT tenant_id, pg_catalog.count(*) AS row_count FROM %(table)s GROUP BY tenant_id", "sql_text_substitution_forbidden"),
    ("SELECT tenant_id, pg_catalog.count(*) AS row_count FROM :identifier GROUP BY tenant_id", "sql_text_substitution_forbidden"),
    ('SELECT tenant_id, pg_catalog.count(*) AS row_count FROM "WAVE4_DIST"."SOURCE_ROWS" GROUP BY tenant_id', "sql_quoted_identifier_forbidden"),
    # sql-safety-without-pglast: the sqlglot checker keeps every refusal above
    # and closes these too.
    ("SELECT tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.source_rows /* a */ ; /* b */ DELETE FROM wave4_dist.source_rows", "sql_multi_statement"),
    ("SELECT tenant_id, pg_catalog.count(*) AS row_count FROM (VALUES ('a')) v(tenant_id) GROUP BY tenant_id", "sql_values_source_forbidden"),
    ("SELECT tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.source_rows WHERE tenant_id = 'a' GROUP BY tenant_id", "sql_operator_forbidden"),
    ("SELECT tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.source_rows WHERE tenant_id OPERATOR(public.=) 'a' GROUP BY tenant_id", "sql_operator_forbidden"),
    ("SELECT tenant_id, count(*) AS row_count FROM wave4_dist.source_rows GROUP BY tenant_id", "sql_function_forbidden"),
    ("SELECT tenant_id, pg_catalog.pg_sleep(1) AS row_count FROM wave4_dist.source_rows GROUP BY tenant_id", "sql_function_forbidden"),
    ("SELECT tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.source_rows WHERE tenant_id OPERATOR(pg_catalog.=) (SELECT 'a') GROUP BY tenant_id", "sql_ast_node_forbidden"),
    ("SELECT tenant_id::regclass AS tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.source_rows GROUP BY tenant_id", "sql_cast_forbidden"),
    ("SELECT tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.source_rows WHERE tenant_id OPERATOR(pg_catalog.=) '\\' GROUP BY tenant_id", "sql_parse_invalid"),
    ("SELECT tenant_id, pg_catalog.count(*) AS row_count FROM wave4_dist.source_rows GROUP BY tenant_id UNION SELECT tenant_id, pg_catalog.count(*) FROM wave4_dist.source_rows GROUP BY tenant_id", "sql_set_operation_forbidden"),
    # `b` is a later sibling: PostgreSQL resolves it as a real relation.
    ("WITH a AS (SELECT tenant_id FROM b), b AS (SELECT tenant_id FROM wave4_dist.source_rows) SELECT tenant_id, pg_catalog.count(*) AS row_count FROM a GROUP BY tenant_id", "sql_relation_lineage_mismatch"),
)


# Test ids use the index and refusal code only. SQL text in a test id reaches
# the CI affected-test plan, and the fake DSN literal above matches a secret
# mask, which makes GitHub drop the whole plan output.
UNSAFE_SQL_IDS = tuple(f"{index:02d}-{code}" for index, (_, code) in enumerate(UNSAFE_SQL))


@pytest.mark.parametrize(("statement", "expected_code"), UNSAFE_SQL, ids=UNSAFE_SQL_IDS)
def test_unsafe_sql_matrix_refuses_before_mutation(
    statement: str,
    expected_code: str,
) -> None:
    called = False
    source_relation = (
        "other_namespace.source_rows"
        if "other_namespace.source_rows" in statement
        else "wave4_dist.source_rows"
    )
    source, contract = _sql_contract(statement, source_relation=source_relation)

    def observe(_plan) -> None:
        nonlocal called
        called = True

    with pytest.raises(ReadSurfacePolicyError) as caught:
        validate_sql_read_model(
            contract,
            source_assets={source_relation: source},
            mutation_observer=observe,
        )

    assert caught.value.code == expected_code
    assert called is False


def test_read_model_plan_keeps_checked_text_binds_and_columns() -> None:
    """R13/R15/R16 of sql-safety-without-pglast: positional binds and source
    columns are collected from the tree, and the view body is the checked
    text itself, not a re-rendering."""
    statement = (
        "/* lead */ SELECT tenant_id, pg_catalog.count(*) AS row_count "
        "FROM wave4_dist.source_rows "
        "WHERE tenant_id OPERATOR(pg_catalog.=) $1 GROUP BY tenant_id ; -- tail"
    )
    source, contract = _sql_contract(
        statement, parameters=(SqlValueParameter(1, "tenant", "text"),),
    )
    plan = validate_sql_read_model(
        contract,
        source_assets={"wave4_dist.source_rows": source},
        parameter_values=("tenant-a",),
    )
    assert plan.normalized_sql == (
        "SELECT tenant_id, pg_catalog.count(*) AS row_count "
        "FROM wave4_dist.source_rows "
        "WHERE tenant_id OPERATOR(pg_catalog.=) $1 GROUP BY tenant_id"
    )
    assert [p.position for p in plan.parameters] == [1]


@pytest.mark.parametrize(("expression", "expected"), [
    ("(tenant_id = 'tenant-a'::text)", {"tenant-a"}),
    ("((tenant_id = 'a'::text) OR (tenant_id = 'b'::text))", {"a", "b"}),
    ("(tenant_id OPERATOR(pg_catalog.=) 'a'::pg_catalog.text)", {"a"}),
])
def test_policy_tenant_values_reads_closed_tenant_equalities(expression, expected) -> None:
    from okg.substrate.views.read_policy import _policy_tenant_values

    assert _policy_tenant_values(expression) == frozenset(expected)


@pytest.mark.parametrize("expression", [
    "(tenant_id OPERATOR(public.=) 'a'::text)",
    "((tenant_id = 'a'::text) AND (tenant_id = 'b'::text))",
    "(tenant_id = 'a'::varchar)",
    "(tenant_id = (('a'::text)::text))",
    "(other = 'a'::text)",
    "(tenant_id = current_setting('x'))",
    "true",
])
def test_policy_tenant_values_refuses_anything_else(expression) -> None:
    from okg.substrate.views.read_policy import _policy_tenant_values

    with pytest.raises(ReadSurfacePolicyError) as caught:
        _policy_tenant_values(expression)
    assert caught.value.code == "sql_source_policy_expression_invalid"


def test_bind_parameters_are_values_only_and_type_checked() -> None:
    source, contract = _sql_contract(
        "SELECT tenant_id, pg_catalog.count(*) AS row_count "
        "FROM wave4_dist.source_rows "
        "WHERE tenant_id OPERATOR(pg_catalog.=) $1 GROUP BY tenant_id",
        parameters=(SqlValueParameter(1, "tenant", "text"),),
    )

    with pytest.raises(ReadSurfacePolicyError) as caught:
        validate_sql_read_model(
            contract,
            source_assets={"wave4_dist.source_rows": source},
            parameter_values=(123,),
        )
    assert caught.value.code == "sql_parameter_type_mismatch"

    with pytest.raises(ValueError, match="data-value binds only"):
        SqlValueParameter(1, "table", "identifier")


_MEMBERS = st.sets(st.sampled_from(("a", "b", "c")), max_size=3).map(frozenset)


@given(_MEMBERS, _MEMBERS, _MEMBERS, _MEMBERS, _MEMBERS, _MEMBERS)
def test_policy_composition_is_never_wider_than_any_source(
    audiences_a,
    audiences_b,
    tenants_a,
    tenants_b,
    fields_a,
    fields_b,
) -> None:
    first = ReadPolicyRequirements(audiences_a, tenants_a, fields_a, frozenset({"d1"}))
    second = ReadPolicyRequirements(audiences_b, tenants_b, fields_b, frozenset({"d2"}))
    output = compose_non_widening_policy((first, second))

    assert output.audiences <= first.audiences and output.audiences <= second.audiences
    assert output.tenants <= first.tenants and output.tenants <= second.tenants
    assert output.visible_fields <= first.visible_fields and output.visible_fields <= second.visible_fields
    assert output.decision_ids == {"d1", "d2"}
    require_non_widening_policy((first, second), output)


def test_policy_lattice_is_exhaustive_and_associative_across_three_hops() -> None:
    members = ("a", "b")
    subsets = tuple(
        frozenset(choice)
        for size in range(len(members) + 1)
        for choice in combinations(members, size)
    )
    for first_set, second_set, third_set in product(subsets, repeat=3):
        first = ReadPolicyRequirements(
            first_set, first_set, first_set, frozenset({"d1"})
        )
        second = ReadPolicyRequirements(
            second_set, second_set, second_set, frozenset({"d2"})
        )
        third = ReadPolicyRequirements(
            third_set, third_set, third_set, frozenset({"d3"})
        )
        left = compose_non_widening_policy(
            (compose_non_widening_policy((first, second)), third)
        )
        right = compose_non_widening_policy(
            (first, compose_non_widening_policy((second, third)))
        )
        expected = first_set & second_set & third_set
        assert left == right
        assert left.audiences == expected
        assert left.tenants == expected
        assert left.visible_fields == expected
        assert left.decision_ids == {"d1", "d2", "d3"}
        require_non_widening_policy((first, second, third), left)


def test_policy_widening_result_drift_and_lineage_gap_refuse() -> None:
    source_policy = _policy(audiences=frozenset({"operator"}))
    widened = _policy(audiences=frozenset({"operator", "public"}))
    with pytest.raises(ReadSurfacePolicyError) as caught:
        require_non_widening_policy((source_policy,), widened)
    assert caught.value.code == "read_policy_widening"

    with pytest.raises(ReadSurfacePolicyError) as caught:
        require_result_shape_migration(
            (("tenant_id", "text"),),
            (("tenant_id", "text"), ("row_count", "bigint")),
            migration_id=None,
        )
    assert caught.value.code == "sql_result_migration_required"
    require_result_shape_migration(
        (("tenant_id", "text"),),
        (("tenant_id", "text"), ("row_count", "bigint")),
        migration_id="migration:add-row-count",
    )

    source, contract = _sql_contract(
        "SELECT tenant_id, pg_catalog.count(*) AS row_count "
        "FROM wave4_dist.source_rows "
        "WHERE tenant_id OPERATOR(pg_catalog.=) 'tenant-a' GROUP BY tenant_id"
    )
    plan = validate_sql_read_model(
        contract,
        source_assets={"wave4_dist.source_rows": source},
    )
    envelope = build_sql_lineage_envelope(
        plan,
        source_records=(source_record_lineage("wave4.fixture", {"id": "row-1"}),),
        graph_generation_id=42,
        catalog_version_id=7,
        output_digest="sha256:" + "b" * 64,
    )
    incomplete = replace(envelope, metadata={**envelope.metadata, "source_relations": []})
    with pytest.raises(ReadSurfacePolicyError) as caught:
        require_sql_lineage_closure(plan, incomplete)
    assert caught.value.code == "sql_lineage_closure_incomplete"


def _native_materializer(
    view_id: str,
    asset_id: str,
    asset_digest: str,
    asset_execution_token,
) -> ViewMaterializer:
    def planner(corpus: dict[str, Anchor], _view_def: ViewDefinition):
        return [QuoteSpec(id="summary", anchor_id="summary", display_label="Summary")]

    def assembler(view) -> str:
        return "# Policy-preserving native view\n\n" + default_assemble(view)

    def factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=planner,
            assembler=assembler,
            view_def_hash=stable_hash({"view_id": view_id, "asset_digest": asset_digest}),
        )

    def corpus(_conn, *, generation_id: int):
        return {
            "summary": Anchor.from_content(
                "summary",
                f"secret masked payload from generation {generation_id}",
            )
        }

    return ViewMaterializer(
        view_def_factory=factory,
        build_corpus=corpus,
        name=view_id,
        asset_id=asset_id,
        asset_digest=asset_digest,
        asset_execution_token=asset_execution_token,
    )


def test_postgres_roles_native_sql_publication_policy_and_lineage(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex[:8]
    namespace = f"wave4_dist_{suffix}"
    maint_role = f"wave4_maint_{suffix}"
    app_role = f"wave4_app_{suffix}"
    denied_role = f"wave4_denied_{suffix}"
    source_relation = f"{namespace}.source_rows"
    native_view_id = f"test.wave4.policy.{suffix}"

    native_contract = build_read_asset(
        owner="wave4",
        asset_id=f"native-{suffix}",
        kind="view",
        config_schema="okg.deployment_product.asset.view.config/v1",
        result_schema="okg.deployment_product.asset.view.result/v1",
        capability_schema="okg.deployment_product.asset.view.capability/v1",
        output_identity=f"native:{suffix}",
        refresh_policy={"mode": "generation"},
        policy_requirements={
            "audiences": [app_role],
            "tenants": ["tenant-a"],
            "visible_fields": ["refresh_stats"],
            "decision_ids": [f"nomos:decision:{suffix}"],
        },
        lineage_sources=[f"fixture:native:{suffix}"],
        config={
            "view_id": native_view_id,
            "deployment_name": "default",
            "result_slots": ["json"],
        },
        provenance={"source": "distribution"},
    )
    native_resolution = resolve_read_assets([native_contract])
    native_result = Publisher(
        clean_runtime,
        "default",
        materializers=[_native_materializer(
            native_view_id,
            native_contract.qualified_id,
            native_contract.asset.digest,
            native_resolution.execution_token(native_contract.qualified_id),
        )],
        coordination_mode="dbos",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert native_result["status"] == "published"
    assert native_result["views"][0]["asset_id"] == native_contract.qualified_id
    native_token = native_resolution.execution_token(native_contract.qualified_id)
    with psycopg.connect(clean_runtime) as conn:
        native_read = read_governed_native_view(
            conn,
            contract=native_contract,
            execution_token=native_token,
            deployment_name="default",
            view_id=native_view_id,
            slot_name="json",
            audience=app_role,
            tenant="tenant-a",
        )
        assert "secret" not in str(native_read.payload)
        assert set(native_read.payload) == {"refresh_stats"}
        assert native_read.lineage_sources == (f"fixture:native:{suffix}",)
        assert native_read.decision_ids == (f"nomos:decision:{suffix}",)
        with pytest.raises(ReadSurfacePolicyError) as caught:
            read_governed_native_view(
                conn,
                contract=native_contract,
                execution_token=native_token,
                deployment_name="default",
                view_id=native_view_id,
                slot_name="json",
                audience=denied_role,
                tenant="tenant-a",
            )
        assert caught.value.code == "native_read_policy_denied"
        with pytest.raises(ReadSurfacePolicyError) as caught:
            read_governed_native_view(
                conn,
                contract=native_contract,
                execution_token=native_token,
                deployment_name="other-deployment",
                view_id=native_view_id,
                slot_name="json",
                audience=app_role,
                tenant="tenant-a",
            )
        assert caught.value.code == "native_read_deployment_mismatch"
        with pytest.raises(ReadSurfacePolicyError) as caught:
            read_governed_native_view(
                conn,
                contract=native_contract,
                execution_token=native_token,
                deployment_name="default",
                view_id=f"{native_view_id}.other",
                slot_name="json",
                audience=app_role,
                tenant="tenant-a",
            )
        assert caught.value.code == "native_read_view_mismatch"
        with pytest.raises(ReadSurfacePolicyError) as caught:
            read_governed_native_view(
                conn,
                contract=native_contract,
                execution_token=native_token,
                deployment_name="default",
                view_id=native_view_id,
                slot_name="private",
                audience=app_role,
                tenant="tenant-a",
            )
        assert caught.value.code == "native_read_slot_not_allowed"

    with psycopg.connect(clean_runtime, autocommit=True) as setup:
        with setup.cursor() as cur:
            for role in (maint_role, app_role, denied_role):
                cur.execute(
                    psycopg_sql.SQL("CREATE ROLE {} NOLOGIN").format(
                        psycopg_sql.Identifier(role)
                    )
                )
            cur.execute(
                psycopg_sql.SQL("CREATE SCHEMA {}")
                .format(psycopg_sql.Identifier(namespace))
            )
            cur.execute(
                psycopg_sql.SQL(
                    "CREATE TABLE {}.source_rows (tenant_id text, public_value text, secret_value text)"
                ).format(psycopg_sql.Identifier(namespace))
            )
            cur.execute(
                psycopg_sql.SQL("CREATE TABLE {}.undeclared_rows (value text)")
                .format(psycopg_sql.Identifier(namespace))
            )
            cur.execute(
                psycopg_sql.SQL(
                    "INSERT INTO {}.source_rows VALUES "
                    "('tenant-a', 'visible-a', 'secret-a'), "
                    "('tenant-b', 'visible-b', 'secret-b')"
                ).format(psycopg_sql.Identifier(namespace))
            )
            cur.execute(
                psycopg_sql.SQL("GRANT USAGE, CREATE ON SCHEMA {} TO {}")
                .format(psycopg_sql.Identifier(namespace), psycopg_sql.Identifier(maint_role))
            )
            cur.execute(
                psycopg_sql.SQL("GRANT USAGE ON SCHEMA {} TO {}, {}")
                .format(
                    psycopg_sql.Identifier(namespace),
                    psycopg_sql.Identifier(app_role),
                    psycopg_sql.Identifier(denied_role),
                )
            )
            cur.execute(
                psycopg_sql.SQL("GRANT SELECT ON {}.source_rows TO {}")
                .format(psycopg_sql.Identifier(namespace), psycopg_sql.Identifier(maint_role))
            )
            cur.execute(
                psycopg_sql.SQL(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} "
                    "GRANT SELECT ON TABLES TO {}"
                ).format(
                    psycopg_sql.Identifier(maint_role),
                    psycopg_sql.Identifier(namespace),
                    psycopg_sql.Identifier(app_role),
                )
            )
            cur.execute(
                psycopg_sql.SQL(
                    "ALTER TABLE {}.source_rows ENABLE ROW LEVEL SECURITY; "
                    "ALTER TABLE {}.source_rows FORCE ROW LEVEL SECURITY"
                )
                .format(
                    psycopg_sql.Identifier(namespace),
                    psycopg_sql.Identifier(namespace),
                )
            )

    try:
        source_policy = ReadPolicyRequirements(
            audiences=frozenset({app_role}),
            tenants=frozenset({"tenant-a"}),
            visible_fields=frozenset({"tenant_id", "public_value"}),
            decision_ids=frozenset({f"nomos:decision:{suffix}"}),
        )
        source = _source_asset(source_relation)
        contract = build_sql_read_model_asset(
            owner="wave4",
            asset_id=f"sql-{suffix}",
            sql=(
                f"SELECT tenant_id, public_value FROM {source_relation} "
                "WHERE tenant_id OPERATOR(pg_catalog.=) 'tenant-a'"
            ),
            distribution_namespace=namespace,
            object_name="tenant_public",
            source_assets={source_relation: source},
            source_policies={source_relation: source_policy},
            source_policy_enforcements={source_relation: "row-security"},
            result_columns={"tenant_id": "text", "public_value": "text"},
            policy=source_policy,
            maintenance_role=maint_role,
            application_role=app_role,
            provenance={"source": "distribution"},
        )
        plan = validate_sql_read_model(
            contract,
            source_assets={source_relation: source},
        )

        with psycopg.connect(clean_runtime) as conn:
            with pytest.raises(ReadSurfacePolicyError) as caught:
                materialize_sql_view(conn, plan)
        assert caught.value.code == "sql_source_policy_attestation_invalid"

        with psycopg.connect(clean_runtime, autocommit=True) as setup:
            with setup.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL(
                        "CREATE POLICY {} ON {}.source_rows "
                        "FOR SELECT TO {} USING (true)"
                    ).format(
                        psycopg_sql.Identifier(sql_source_policy_name(plan, source_relation)),
                        psycopg_sql.Identifier(namespace),
                        psycopg_sql.Identifier(maint_role),
                    )
                )

        with psycopg.connect(clean_runtime) as conn:
            with pytest.raises(ReadSurfacePolicyError) as caught:
                materialize_sql_view(conn, plan)
        assert caught.value.code == "sql_source_policy_expression_invalid"

        with psycopg.connect(clean_runtime, autocommit=True) as setup:
            with setup.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL("DROP POLICY {} ON {}.source_rows").format(
                        psycopg_sql.Identifier(sql_source_policy_name(plan, source_relation)),
                        psycopg_sql.Identifier(namespace),
                    )
                )
                cur.execute(
                    psycopg_sql.SQL(
                        "CREATE FUNCTION {}.always_true(text, text) RETURNS boolean "
                        "LANGUAGE sql IMMUTABLE AS 'SELECT true'"
                    ).format(psycopg_sql.Identifier(namespace))
                )
                cur.execute(
                    psycopg_sql.SQL(
                        "CREATE OPERATOR {}.= ("
                        "LEFTARG = text, RIGHTARG = text, PROCEDURE = {}.always_true)"
                    ).format(
                        psycopg_sql.Identifier(namespace),
                        psycopg_sql.Identifier(namespace),
                    )
                )
                cur.execute(
                    psycopg_sql.SQL(
                        "CREATE POLICY {} ON {}.source_rows FOR SELECT TO {} USING "
                        "(tenant_id OPERATOR({}.=) 'tenant-a')"
                    ).format(
                        psycopg_sql.Identifier(sql_source_policy_name(plan, source_relation)),
                        psycopg_sql.Identifier(namespace),
                        psycopg_sql.Identifier(maint_role),
                        psycopg_sql.Identifier(namespace),
                    )
                )

        with psycopg.connect(clean_runtime) as conn:
            with pytest.raises(ReadSurfacePolicyError) as caught:
                materialize_sql_view(conn, plan)
        assert caught.value.code == "sql_source_policy_expression_invalid"

        with psycopg.connect(clean_runtime, autocommit=True) as setup:
            with setup.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL("DROP POLICY {} ON {}.source_rows").format(
                        psycopg_sql.Identifier(sql_source_policy_name(plan, source_relation)),
                        psycopg_sql.Identifier(namespace),
                    )
                )
                cur.execute(
                    psycopg_sql.SQL("DROP OPERATOR {}.= (text, text)").format(
                        psycopg_sql.Identifier(namespace)
                    )
                )
                cur.execute(
                    psycopg_sql.SQL("DROP FUNCTION {}.always_true(text, text)").format(
                        psycopg_sql.Identifier(namespace)
                    )
                )
                cur.execute(
                    psycopg_sql.SQL(
                        "CREATE POLICY {} ON {}.source_rows "
                        "FOR SELECT TO {} USING "
                        "(tenant_id OPERATOR(pg_catalog.=) 'tenant-a')"
                    ).format(
                        psycopg_sql.Identifier(sql_source_policy_name(plan, source_relation)),
                        psycopg_sql.Identifier(namespace),
                        psycopg_sql.Identifier(maint_role),
                    )
                )
                cur.execute(
                    psycopg_sql.SQL("GRANT SELECT ON {}.undeclared_rows TO {}").format(
                        psycopg_sql.Identifier(namespace),
                        psycopg_sql.Identifier(maint_role),
                    )
                )

        with psycopg.connect(clean_runtime) as conn:
            with pytest.raises(ReadSurfacePolicyError) as caught:
                materialize_sql_view(conn, plan)
        assert caught.value.code == "sql_role_authority_invalid"

        with psycopg.connect(clean_runtime, autocommit=True) as setup:
            with setup.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL("REVOKE SELECT ON {}.undeclared_rows FROM {}").format(
                        psycopg_sql.Identifier(namespace),
                        psycopg_sql.Identifier(maint_role),
                    )
                )
                cur.execute(
                    psycopg_sql.SQL("GRANT SELECT ON {}.undeclared_rows TO PUBLIC").format(
                        psycopg_sql.Identifier(namespace)
                    )
                )

        with psycopg.connect(clean_runtime) as conn:
            with pytest.raises(ReadSurfacePolicyError) as caught:
                materialize_sql_view(conn, plan)
        assert caught.value.code == "sql_role_authority_invalid"

        with psycopg.connect(clean_runtime, autocommit=True) as setup:
            with setup.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL("REVOKE SELECT ON {}.undeclared_rows FROM PUBLIC").format(
                        psycopg_sql.Identifier(namespace)
                    )
                )
                cur.execute(
                    psycopg_sql.SQL("CREATE TYPE {}.hostile_type AS ENUM ('x')").format(
                        psycopg_sql.Identifier(namespace)
                    )
                )
                cur.execute(
                    psycopg_sql.SQL(
                        "ALTER TABLE {}.source_rows ADD COLUMN hostile {}.hostile_type"
                    ).format(
                        psycopg_sql.Identifier(namespace),
                        psycopg_sql.Identifier(namespace),
                    )
                )

        with psycopg.connect(clean_runtime) as conn:
            with pytest.raises(ReadSurfacePolicyError) as caught:
                materialize_sql_view(conn, plan)
        assert caught.value.code == "sql_source_type_forbidden"

        with psycopg.connect(clean_runtime, autocommit=True) as setup:
            with setup.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL("ALTER TABLE {}.source_rows DROP COLUMN hostile").format(
                        psycopg_sql.Identifier(namespace)
                    )
                )
                cur.execute(
                    psycopg_sql.SQL("DROP TYPE {}.hostile_type").format(
                        psycopg_sql.Identifier(namespace)
                    )
                )

        checkpoint_id = native_result["generation_id"]
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT (metadata::jsonb)->>'target_generation_id', "
                "       (metadata::jsonb)->>'status', "
                "       (metadata::jsonb)->>'catalog_version_id' "
                "FROM public._chronos_branch_interval_checkpoints "
                "WHERE checkpoint_id = %s",
                (checkpoint_id,),
            )
            target_generation, checkpoint_status, generation_catalog_version = cur.fetchone()
        assert checkpoint_status == "published"
        generation_id = int(target_generation)
        source_records = (source_record_lineage(
            "wave4.fixture",
            {"relation": source_relation, "tenant": "tenant-a"},
        ),)

        with psycopg.connect(clean_runtime) as conn:
            with pytest.raises(ReadSurfacePolicyError) as caught:
                materialize_and_publish_sql_read_model(
                    conn,
                    plan,
                    deployment_name="default",
                    source_records=source_records,
                    graph_generation_id=generation_id + 1_000_000_000,
                    catalog_version_id=generation_catalog_version,
                )
        assert caught.value.code == "sql_generation_not_published"

        with psycopg.connect(clean_runtime, autocommit=True) as admin:
            with admin.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public._chronos_branch_interval_checkpoints (
                        checkpoint_id, branch_id, segment_id, created_at, metadata
                    ) VALUES (%s, 'default', 0, now()::text, %s)
                    """,
                    (
                        f"okg:default:branch:default:blocked:{suffix}",
                        json.dumps({
                            "kind": "okg_generation",
                            "deployment_name": "default",
                            "branch": "default",
                            "status": "blocked",
                            "catalog_version_id": int(generation_catalog_version),
                            "target_generation_id": generation_id + 1,
                        }),
                    ),
                )
                blocked_generation_id = generation_id + 1
        with psycopg.connect(clean_runtime) as conn:
            with pytest.raises(ReadSurfacePolicyError) as caught:
                materialize_and_publish_sql_read_model(
                    conn,
                    plan,
                    deployment_name="default",
                    source_records=source_records,
                    graph_generation_id=blocked_generation_id,
                    catalog_version_id=generation_catalog_version,
                )
        assert caught.value.code == "sql_generation_not_published"

        with psycopg.connect(clean_runtime) as conn:
            publication_receipt = materialize_and_publish_sql_read_model(
                conn,
                plan,
                deployment_name="default",
                source_records=source_records,
                graph_generation_id=generation_id,
                catalog_version_id=generation_catalog_version,
            )
        sql_result = publication_receipt.read_result
        assert publication_receipt.materialization.asset_id == contract.qualified_id
        assert sql_result.execution_role == app_role
        assert [dict(row) for row in sql_result.rows] == [
            {"tenant_id": "tenant-a", "public_value": "visible-a"}
        ]

        refreshed_generation_id = generation_id + 2
        with psycopg.connect(clean_runtime, autocommit=True) as checkpoint_conn:
            ChronosGraphStore.from_connection(checkpoint_conn).checkpoint_generation(
                refreshed_generation_id,
                deployment_name="default",
                catalog_version_id=int(generation_catalog_version),
                status="published",
            )
        with psycopg.connect(clean_runtime) as conn:
            refreshed_receipt = materialize_and_publish_sql_read_model(
                conn,
                plan,
                deployment_name="default",
                source_records=source_records,
                graph_generation_id=refreshed_generation_id,
                catalog_version_id=generation_catalog_version,
            )
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT target_generation_id FROM okg.derived_output_manifests "
                "WHERE manifest_id = %s",
                (refreshed_receipt.publication.manifest_id,),
            )
            assert cur.fetchone()[0] == refreshed_generation_id
        assert (
            refreshed_receipt.materialization.view_definition_digest
            == publication_receipt.materialization.view_definition_digest
        )

        with psycopg.connect(clean_runtime, autocommit=True) as admin:
            with admin.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL("SET ROLE {}").format(
                        psycopg_sql.Identifier(maint_role)
                    )
                )
                cur.execute(
                    psycopg_sql.SQL(
                        "CREATE OR REPLACE VIEW {}.tenant_public "
                        "WITH (security_barrier=true) AS "
                        "SELECT tenant_id, public_value FROM {}.source_rows "
                        "WHERE public_value IS NOT NULL"
                    ).format(
                        psycopg_sql.Identifier(namespace),
                        psycopg_sql.Identifier(namespace),
                    )
                )
                cur.execute("RESET ROLE")
        with psycopg.connect(clean_runtime) as conn:
            with pytest.raises(ReadSurfacePolicyError) as caught:
                execute_sql_read_query(conn, plan, refreshed_receipt.materialization)
        assert caught.value.code == "sql_output_identity_invalid"
        with psycopg.connect(clean_runtime) as conn:
            current_materialization = materialize_sql_view(conn, plan)

        with psycopg.connect(clean_runtime, autocommit=True) as admin:
            with admin.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL("GRANT SELECT ON {}.tenant_public TO PUBLIC").format(
                        psycopg_sql.Identifier(namespace)
                    )
                )
        with psycopg.connect(clean_runtime) as conn:
            with pytest.raises(ReadSurfacePolicyError) as caught:
                execute_sql_read_query(conn, plan, current_materialization)
        assert caught.value.code == "sql_output_acl_invalid"
        with psycopg.connect(clean_runtime, autocommit=True) as admin:
            with admin.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL("REVOKE SELECT ON {}.tenant_public FROM PUBLIC").format(
                        psycopg_sql.Identifier(namespace)
                    )
                )

        with psycopg.connect(clean_runtime) as conn:
            with conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL("SET LOCAL ROLE {}")
                    .format(psycopg_sql.Identifier(app_role))
                )
                cur.execute(
                    psycopg_sql.SQL("SELECT tenant_id, public_value FROM {}.tenant_public")
                    .format(psycopg_sql.Identifier(namespace))
                )
                assert cur.fetchall() == [("tenant-a", "visible-a")]

        with psycopg.connect(clean_runtime) as conn:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                with conn.transaction(), conn.cursor() as cur:
                    cur.execute(
                        psycopg_sql.SQL("SET LOCAL ROLE {}")
                        .format(psycopg_sql.Identifier(denied_role))
                    )
                    cur.execute(
                        psycopg_sql.SQL("SELECT * FROM {}.tenant_public")
                        .format(psycopg_sql.Identifier(namespace))
                    )
        with psycopg.connect(clean_runtime) as conn:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                with conn.transaction(), conn.cursor() as cur:
                    cur.execute(
                        psycopg_sql.SQL("SET LOCAL ROLE {}")
                        .format(psycopg_sql.Identifier(maint_role))
                    )
                    cur.execute(
                        psycopg_sql.SQL("SELECT * FROM {}.undeclared_rows")
                        .format(psycopg_sql.Identifier(namespace))
                    )

        published = publication_receipt.publication
        assert published.lineage_status == LINEAGE_STATUS_AVAILABLE
        assert published.lineage_envelope_count == 1
        assert published.row_count == 1
    finally:
        with psycopg.connect(clean_runtime, autocommit=True) as cleanup:
            with cleanup.cursor() as cur:
                cur.execute(
                    psycopg_sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE")
                    .format(psycopg_sql.Identifier(namespace))
                )
                for role in (denied_role, app_role, maint_role):
                    cur.execute(
                        psycopg_sql.SQL("DROP ROLE IF EXISTS {}")
                        .format(psycopg_sql.Identifier(role))
                    )
