"""Writer-path and TransformConsistencyGate narrowing equivalence."""

from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.catalog import NarrowingChecker  # noqa: E402
from okg.substrate.consistency import TransformConsistencyGate  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore, _edge_id  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.transform import narrowing_allowed_binding  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    SubtypeSpec,
    seed_catalog,
)


pytestmark = pytest.mark.postgres


def _refinement_subtypes() -> list[SubtypeSpec]:
    return [
        SubtypeSpec("entity"),
        SubtypeSpec("work_item", parent="entity"),
        SubtypeSpec("ticket", parent="work_item"),
        SubtypeSpec("bug", parent="ticket"),
        SubtypeSpec("person", parent="entity"),
        SubtypeSpec("engineer", parent="person"),
        SubtypeSpec("team", parent="entity"),
    ]


def _gate_allowed_set(
    conn: psycopg.Connection,
    catalog_version_id: int,
) -> set[tuple[str, str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT src_subtype, edge_type, dst_subtype
              FROM {narrowing_allowed_binding(catalog_version_id)} allowed
            """
        )
        return {tuple(row) for row in cur.fetchall()}


def test_writer_and_gate_allowed_sets_match_refinement_closure(
    clean_runtime: str,
) -> None:
    subtypes = _refinement_subtypes()
    subtype_names = [spec.name for spec in subtypes]
    edge_types = ["assigned_to", "blocks", "mentions"]
    narrowings = [
        ("work_item", "assigned_to", "person"),
        ("ticket", "blocks", "ticket"),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(
            conn,
            subtypes=subtypes,
            narrowings=narrowings,
            aliases=[("human", "person", "subtype")],
        )
        checker = NarrowingChecker(conn)
        assert checker.catalog_version_id == cv
        gate_allowed = _gate_allowed_set(conn, cv)

        writer_allowed = {
            (src, edge, dst)
            for src in subtype_names
            for edge in edge_types
            for dst in subtype_names
            if checker.is_permitted(src, edge, dst)
        }

    assert writer_allowed == gate_allowed
    assert ("bug", "assigned_to", "engineer") in gate_allowed
    assert ("bug", "blocks", "bug") in gate_allowed
    assert ("team", "assigned_to", "engineer") not in gate_allowed
    assert ("ticket", "mentions", "person") not in gate_allowed
    assert checker.is_permitted("work_item", "assigned_to", "human") is False
    assert ("work_item", "assigned_to", "human") not in gate_allowed
    assert not any("human" in triple for triple in gate_allowed)


def test_writer_gate_allowed_sets_require_same_catalog_version(
    clean_runtime: str,
) -> None:
    subtypes = _refinement_subtypes()
    subtype_names = [spec.name for spec in subtypes]
    edge_types = ["assigned_to", "blocks", "mentions"]
    base_narrowings = [
        ("work_item", "assigned_to", "person"),
        ("ticket", "blocks", "ticket"),
    ]
    current_narrowings = [
        *base_narrowings,
        ("team", "assigned_to", "engineer"),
    ]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        old_cv = seed_catalog(
            conn,
            subtypes=subtypes,
            narrowings=base_narrowings,
        )
        current_cv = seed_catalog(
            conn,
            subtypes=subtypes,
            narrowings=current_narrowings,
        )
        checker = NarrowingChecker(conn)
        assert checker.catalog_version_id == current_cv

        writer_allowed = {
            (src, edge, dst)
            for src in subtype_names
            for edge in edge_types
            for dst in subtype_names
            if checker.is_permitted(src, edge, dst)
        }
        gate_current = _gate_allowed_set(conn, current_cv)
        gate_old = _gate_allowed_set(conn, old_cv)

    assert writer_allowed == gate_current
    assert writer_allowed != gate_old
    assert ("team", "assigned_to", "engineer") in writer_allowed
    assert ("team", "assigned_to", "engineer") not in gate_old


def test_writer_and_gate_agree_under_incomplete_reflexive_closure(
    clean_runtime: str,
) -> None:
    """An incomplete subtype_ancestors closure must not split writer and gate.

    The writer (NarrowingChecker) and the DB trigger admit a declared
    (parent, edge, child) narrowing triple via a raw-rows arm even when the
    reflexive subtype_ancestors self-row is absent (e.g. a subtype created
    before the depth-0 closure backfill). Before the gate binding gained the same
    raw-rows UNION it used only the descendant-closure JOIN, so a missing
    reflexive row made the gate REJECT a triple the writer ACCEPTS -- a spurious
    publish-block. This asserts the surfaces agree on the declared triple with the
    reflexive self-row removed; it fails against the closure-only gate binding.
    """
    subtypes = [SubtypeSpec("entity"), SubtypeSpec("work_item", parent="entity")]
    narrowings = [("work_item", "assigned_to", "entity")]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=subtypes, narrowings=narrowings)
        # Drop the reflexive self-row for the narrowing's parent subtype,
        # leaving subtype_ancestors closure incomplete for work_item.
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.subtype_ancestors "
                "WHERE catalog_version_id = %s "
                "  AND ancestor_name = %s AND subtype_name = %s",
                (cv, "work_item", "work_item"),
            )
        checker = NarrowingChecker(conn)
        gate_allowed = _gate_allowed_set(conn, cv)

    triple = ("work_item", "assigned_to", "entity")
    # The writer admits the declared triple via its raw-rows arm...
    assert checker.is_permitted(*triple) is True
    # ...and the gate must too (raw-rows UNION), or it spuriously blocks it.
    assert triple in gate_allowed


def test_transform_gate_noops_narrowing_when_catalog_has_no_rows(
    clean_runtime: str,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    src, dst = f"noop:src:{suffix}", f"noop:dst:{suffix}"
    branch = f"narrowing-noop-{suffix}"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["page"], narrowings=[])
        checker = NarrowingChecker(conn)
        assert checker.is_active is False
        assert checker.is_permitted("page", "unlisted", "page") is True

        store = ChronosGraphStore.from_connection(conn)
        store.ensure_branch(branch, from_branch="main")
        store.apply_facts(
            [
                NodeFact(src, "page", {}, {"id": src}, {"v": 1}),
                NodeFact(dst, "page", {}, {"id": dst}, {"v": 1}),
                EdgeFact(
                    src,
                    dst,
                    "unlisted",
                    source_record_id={"id": "edge"},
                    source_revision={"v": 1},
                ),
            ],
            branch=branch,
            strict_endpoints=True,
        )
        gen = store.checkpoint_generation(
            deployment_name="narrowing-equivalence",
            branch=branch,
            catalog_version_id=cv,
            metadata={"correlation_id": f"narrowing-noop:{suffix}"},
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation = TransformConsistencyGate().evaluate(
            conn,
            target_generation_id=gen,
            catalog_version_id=cv,
            deployment_name="narrowing-equivalence",
            branch_name=branch,
        )

    assert evaluation.result == "evaluated"
    assert evaluation.blocks_publish is False
    assert evaluation.details["narrowing_violation_edge_ids"] == []
    assert _edge_id(src, "unlisted", dst) not in (
        evaluation.details["narrowing_violation_edge_ids"]
    )
