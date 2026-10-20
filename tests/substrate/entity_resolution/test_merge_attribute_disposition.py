"""Issue #1043: merging two nodes must not quietly shrink their citations.

`grounded_in` is a list of the text spans that support an entity. Retiring one
of two duplicate nodes does not make that node's supporting text stop existing,
so the merged node has to keep both lists.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.projection.remediation import (
    plan_entity_merge,
    remediate_entity_merge,
)


pytestmark = pytest.mark.postgres

DEPLOYMENT = "entity-merge-attrs-test"
SURVIVOR = "concept:vv"
RETIRED = "concept:vv-minted"
SURVIVOR_CHUNK = "chunk:08fc05a04b81ed35"
RETIRED_CHUNK = "chunk:00b4640c789cabf2"


def _seed(
    conn: psycopg.Connection,
    *,
    declare: dict[str, str] | None = None,
    survivor_attrs: dict | None = None,
    retired_attrs: dict | None = None,
    survivor_source: str = "concept-source",
) -> int:
    from chronos_core.branching._common import clear_all_registry_caches
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache

    clear_all_registry_caches()
    clear_schema_ensured_cache()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions "
            "(status, applied_at, deployment_name) "
            "VALUES ('applied', now(), %s) RETURNING catalog_version_id",
            (DEPLOYMENT,),
        )
        cv = int(cur.fetchone()[0])
        if declare is not None:
            # Same place `pii_classes` lands: the composer writes this from a
            # LinkML `annotations.merge_semantics` declaration.
            cur.execute(
                "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
                "VALUES (%s, %s::jsonb, %s)",
                ("concept", psycopg.types.json.Jsonb({"merge_semantics": declare}), cv),
            )
    store = ChronosGraphStore.from_connection(conn)
    store.context.ensure_metadata()
    store.ensure_schema()
    store.ensure_branch("default", from_branch="main")
    store.apply_facts(
        [
            NodeFact(
                SURVIVOR,
                "concept",
                survivor_attrs
                if survivor_attrs is not None
                else {"name": "VV", "grounded_in": [SURVIVOR_CHUNK]},
                {"id": SURVIVOR},
                {"v": 1},
            )
        ],
        branch="default",
        source=survivor_source,
    )
    store.apply_facts(
        [
            NodeFact(
                RETIRED,
                "concept",
                retired_attrs
                if retired_attrs is not None
                else {"name": "VV", "grounded_in": [RETIRED_CHUNK]},
                {"id": RETIRED},
                {"v": 1},
            )
        ],
        branch="default",
        source="concept-source",
    )
    store.checkpoint_generation(
        branch="default",
        deployment_name=DEPLOYMENT,
        catalog_version_id=cv,
        status="published",
        metadata={"fixture": "entity-merge-attrs"},
    )
    return cv


def _node_attrs(conn: psycopg.Connection, node_id: str) -> dict:
    graph = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout(
        "default"
    )
    rows = list(
        graph.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": node_id},
        )
    )
    assert rows, f"{node_id} is not live"
    return dict(rows[0]["attrs"])


def test_union_keeps_both_citation_sets_in_the_graph(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            attribute_dispositions={"grounded_in": "union"},
        )
        grounded = next(
            item
            for item in plan.attribute_conflicts
            if item["attribute"] == "grounded_in"
        )
        assert grounded["disposition"] == "union"
        assert sorted(grounded["merged"]) == sorted([SURVIVOR_CHUNK, RETIRED_CHUNK])

        remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan.plan_digest,
            attribute_dispositions={"grounded_in": "union"},
            apply=True,
        )
        # Read the merged node back from the graph, not from the plan.
        assert sorted(_node_attrs(conn, SURVIVOR)["grounded_in"]) == sorted(
            [SURVIVOR_CHUNK, RETIRED_CHUNK]
        )


def test_catalog_declaration_supplies_the_disposition(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, declare={"grounded_in": "union"})
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
        )
        grounded = next(
            item
            for item in plan.attribute_conflicts
            if item["attribute"] == "grounded_in"
        )
        assert grounded["disposition"] == "union"


def test_accept_survivor_attrs_does_not_override_a_declared_union(
    clean_runtime: str,
) -> None:
    """The flag every existing caller passes must not undo the declaration."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(
            conn,
            declare={"grounded_in": "union"},
            survivor_attrs={
                "name": "VV_2018",
                "grounded_in": [SURVIVOR_CHUNK],
            },
            retired_attrs={"name": "VV", "grounded_in": [RETIRED_CHUNK]},
        )
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        by_attribute = {
            item["attribute"]: item for item in plan.attribute_conflicts
        }
        assert by_attribute["grounded_in"]["disposition"] == "union"
        # An attribute nobody declared still takes the survivor's value.
        assert by_attribute["name"]["disposition"] == "keep_survivor"


def test_undeclared_attribute_still_refuses_the_merge(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
        )
        assert {
            item["disposition"] for item in plan.attribute_conflicts
        } == {"unresolved"}
        with pytest.raises(SubstrateError, match="unresolved node attribute"):
            remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                expected_plan_digest=plan.plan_digest,
                apply=True,
            )


def test_union_on_a_non_list_attribute_refuses_at_plan_time(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(
            conn,
            survivor_attrs={"name": "VV_2018", "grounded_in": [SURVIVOR_CHUNK]},
            retired_attrs={"name": "VV", "grounded_in": [RETIRED_CHUNK]},
        )
        with pytest.raises(SubstrateError, match="not a list on both sides"):
            plan_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                attribute_dispositions={"name": "union"},
            )


def test_keep_retiree_takes_the_other_nodes_value(clean_runtime: str) -> None:
    """Issue #1043's secondary case: the survivor's name can be the worse one."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(
            conn,
            survivor_attrs={"name": "VV_2018", "grounded_in": [SURVIVOR_CHUNK]},
            retired_attrs={"name": "VV", "grounded_in": [SURVIVOR_CHUNK]},
        )
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            attribute_dispositions={"name": "keep_retiree"},
        )
        remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan.plan_digest,
            attribute_dispositions={"name": "keep_retiree"},
            apply=True,
        )
        assert _node_attrs(conn, SURVIVOR)["name"] == "VV"


def test_plan_digest_is_reproducible_and_carries_the_dispositions(
    clean_runtime: str,
) -> None:
    """A dry-run digest has to survive the replan inside --apply."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
        first = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            attribute_dispositions={"grounded_in": "union"},
        )
        second = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            attribute_dispositions={"grounded_in": "union"},
        )
        assert first.plan_digest == second.plan_digest
        # A different disposition is a different plan, so a digest approved for
        # one cannot be spent on the other.
        other = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert other.plan_digest != first.plan_digest


def test_a_survivor_write_that_would_shrink_provenance_is_refused(
    clean_runtime: str,
) -> None:
    """The guard has to fire through a real merge, not only as a predicate.

    A publisher-derived survivor carries an envelope naming two sources. The
    write path rebuilds a node's envelope from the batch source, so rewriting
    this node's attributes would leave it naming fewer — losing provenance as a
    side effect of merging citations. The merge refuses instead.
    """
    from okg.substrate.nomos.source_lineage import (
        LINEAGE_ATTR,
        combine_source_lineages,
        normalize_source_lineage,
    )

    two_sources = combine_source_lineages(
        [
            normalize_source_lineage(
                None,
                source_id="source-a",
                source_record_id={"chunk": SURVIVOR_CHUNK},
                source_revision={"v": 1},
            ),
            normalize_source_lineage(
                None,
                source_id="source-b",
                source_record_id={"chunk": RETIRED_CHUNK},
                source_revision={"v": 1},
            ),
        ]
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(
            conn,
            survivor_attrs={
                "name": "VV",
                "grounded_in": [SURVIVOR_CHUNK],
                LINEAGE_ATTR: two_sources,
            },
            survivor_source="_publisher_derived",
        )
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            attribute_dispositions={"grounded_in": "union"},
        )
        assert [item["kind"] for item in plan.survivor_write_refusals] == [
            "survivor_lineage_narrowing"
        ]
        assert sorted(
            plan.survivor_write_refusals[0]["lost_source_ids"]
        ) == ["source-a", "source-b"]

        before = _node_attrs(conn, SURVIVOR)
        with pytest.raises(SubstrateError, match="narrow survivor node lineage"):
            remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                expected_plan_digest=plan.plan_digest,
                attribute_dispositions={"grounded_in": "union"},
                apply=True,
            )
        assert _node_attrs(conn, SURVIVOR) == before


def test_accept_survivor_attrs_never_silently_takes_one_side_of_a_list(
    clean_runtime: str,
) -> None:
    """A blanket "take the survivor's value" is fine for a scalar and is a
    silent citation loss for a list, so a differing list always needs its own
    disposition — including on a deployment that declares nothing at all."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(
            conn,
            survivor_attrs={"name": "VV_2018", "grounded_in": [SURVIVOR_CHUNK]},
            retired_attrs={"name": "VV", "grounded_in": [RETIRED_CHUNK]},
        )
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        by_attribute = {
            item["attribute"]: item["disposition"]
            for item in plan.attribute_conflicts
        }
        assert by_attribute["grounded_in"] == "unresolved"
        assert by_attribute["name"] == "keep_survivor"
        with pytest.raises(SubstrateError, match="unresolved node attribute"):
            remediate_entity_merge(
                conn,
                survivor_node_id=SURVIVOR,
                retired_node_id=RETIRED,
                deployment_name=DEPLOYMENT,
                expected_plan_digest=plan.plan_digest,
                accept_survivor_attributes=True,
                apply=True,
            )


def _merge_cli(clean_runtime: str, *extra: str) -> list[str]:
    return [
        "remediate",
        "entity-merge",
        "--survivor",
        SURVIVOR,
        "--retire",
        RETIRED,
        "--deployment-name",
        DEPLOYMENT,
        "--dsn",
        clean_runtime,
        "--json",
        *extra,
    ]


def test_cli_merge_attr_flag_plans_and_applies_a_union(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
    assert main(_merge_cli(clean_runtime, "--merge-attr", "grounded_in=union")) == 0
    dry = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    plan = dry["details"]["plan"]
    assert plan["attribute_dispositions"] == {"grounded_in": "union"}

    assert (
        main(
            _merge_cli(
                clean_runtime,
                "--merge-attr",
                "grounded_in=union",
                "--expected-plan-digest",
                plan["plan_digest"],
                "--apply",
            )
        )
        == 0
    )
    capsys.readouterr()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert sorted(_node_attrs(conn, SURVIVOR)["grounded_in"]) == sorted(
            [SURVIVOR_CHUNK, RETIRED_CHUNK]
        )


def test_cli_rejects_a_malformed_merge_attr_argument(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn)
    assert main(_merge_cli(clean_runtime, "--merge-attr", "grounded_in")) != 0
    assert "NAME=DISPOSITION" in capsys.readouterr().err


def test_keep_survivor_only_plan_writes_no_survivor_node(
    clean_runtime: str,
) -> None:
    """The pre-existing path must be byte-for-byte what it was."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Scalar conflicts only: `--accept-survivor-attrs` deliberately does not
        # cover a differing list.
        _seed(
            conn,
            survivor_attrs={"name": "VV_2018", "grounded_in": [SURVIVOR_CHUNK]},
            retired_attrs={"name": "VV", "grounded_in": [SURVIVOR_CHUNK]},
        )
        plan = plan_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            accept_survivor_attributes=True,
        )
        assert plan.survivor_attrs_merged is None
        before = _node_attrs(conn, SURVIVOR)
        remediate_entity_merge(
            conn,
            survivor_node_id=SURVIVOR,
            retired_node_id=RETIRED,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan.plan_digest,
            accept_survivor_attributes=True,
            apply=True,
        )
        assert _node_attrs(conn, SURVIVOR) == before
