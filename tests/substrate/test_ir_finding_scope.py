"""Per-item finding scopes + triage emission for IR checks
(consistency-checks-as-ir, task.ir.finding-scope-and-triage-emission).

Clears the two migration blockers the equivalence-harness review recorded:
the executor's subject-only input_scope collapsed per-link findings onto one
scope (one remaining bad link held every sibling finding open), and program-
projected triage payload/evidence never reached the observation store. Proven
through the REAL executor across two published generations: a page with two
bad links holds two open observations; fixing one link resolves exactly that
observation.
"""

from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency import (  # noqa: E402
    IrConsistencyCheckSpec,
    IrPostPublishCheck,
)
from okg.substrate.consistency.ir_check import (  # noqa: E402
    IrCheckLoadError,
    _validate_program,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.transform import (  # noqa: E402
    Binding,
    Block,
    Effect,
    Op,
    OpKind,
    Terminator,
    TerminatorKind,
    TransformProgram,
)
from okg.substrate.transform.expr import (  # noqa: E402
    and_,
    cast,
    col,
    eq,
    func,
    json_member,
    like,
    lit,
)
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _t(value: str) -> dict:
    # Explicit ::text — bare parameters in SELECT lists / jsonb_build_object
    # are untyped under the extended protocol.
    return cast(lit(value), "text")


def _check_attrs(**overrides) -> dict:
    attrs = {
        "predicate_key": "page_links_resolve",
        "normalized_predicate": "bad_link",
        "participant_roles": {"page": "node_id"},
        "severity": "medium",
        "normalized_value_columns": ["node_id", "link"],
        "finding_scope_columns": ["link"],
        "payload_column": "payload",
        "evidence_column": "evidence",
    }
    attrs.update(overrides)
    return attrs


def per_link_program(**attr_overrides) -> TransformProgram:
    links = func(
        "coalesce", json_member(col("n.attrs"), "links"), cast(lit("[]"), "jsonb")
    )
    payload = func(
        "jsonb_build_object",
        _t("issue_kind"), _t("bad_link"),
        _t("target"), col("link"),
    )
    evidence = func(
        "jsonb_build_object",
        _t("source_field"), _t("links"),
        _t("target"), col("link"),
    )
    return TransformProgram(
        program_id="fixture.finding_scope.bad_link",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": _check_attrs(**attr_overrides)},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(
                        Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}),
                        output="n",
                    ),
                    Binding(
                        Op(OpKind.UNNEST, {"expr": links, "field": "link"}),
                        output="exploded", inputs=("n",),
                    ),
                    Binding(
                        Op(OpKind.WHERE, {"expr": and_(
                            eq(col("n.subtype"), lit("page")),
                            like(col("link"), "bad:%"),
                        )}),
                        output="flagged", inputs=("exploded",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": {
                            "node_id": col("n.node_id"),
                            "link": col("link"),
                            "payload": payload,
                            "evidence": evidence,
                        }}),
                        output="offending", inputs=("flagged",),
                    ),
                    Binding(
                        Op(OpKind.EMIT, {"target": "offending"}),
                        inputs=("offending",),
                    ),
                ),
                terminator=Terminator(
                    TerminatorKind.RETURN, {"values": ["offending"]}
                ),
            ),
        ),
    )


def _spec(program: TransformProgram) -> IrConsistencyCheckSpec:
    return IrConsistencyCheckSpec(
        name="bad_link_ir",
        check_id=f"fixture.bad_link.{uuid.uuid4().hex[:8]}",
        predicate_family="fixture_links",
        confidence_type="deterministic_boolean",
        program=program,
        program_ref="tests.fixture.per_link_program",
    )


# --------------------------------------------------------------------------- #
# Loader validation                                                           #
# --------------------------------------------------------------------------- #

def test_loader_accepts_the_declarations() -> None:
    _validate_program(_spec(per_link_program()), where="test")


@pytest.mark.parametrize("bad", [
    {"finding_scope_columns": []},
    {"finding_scope_columns": "link"},
    {"finding_scope_columns": [1]},
    {"payload_column": 7},
    {"evidence_column": ""},
])
def test_loader_rejects_malformed_declarations(bad: dict) -> None:
    with pytest.raises(IrCheckLoadError):
        _validate_program(_spec(per_link_program(**bad)), where="test")


# --------------------------------------------------------------------------- #
# Live: per-item resolution granularity + triage payload through the executor #
# --------------------------------------------------------------------------- #

def _publish(dsn: str) -> int:
    """Publish and return the compat INTEGER generation id."""
    from okg.substrate.consistency.ir_executor import _compat_generation_int

    out = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert out["status"] == "published", out
    with psycopg.connect(dsn, autocommit=True) as conn:
        return _compat_generation_int(
            conn,
            out["generation_id"],
            deployment_name="default",
            branch_name="default",
        )


def _apply_page(dsn: str, node_id: str, links: list[str], revision: int) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        if revision == 1:
            seed_catalog(conn, subtypes=["page"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(node_id, "page", {"links": links},
                      {"id": node_id}, {"v": revision})],
            source="default",
            strict_endpoints=True,
        )


def _open_findings(conn, check_id: str) -> dict[str, str]:
    """open input_scope -> its link (from normalized_values)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT input_scope, normalized_values->>'link'
              FROM okg.observation_current
             WHERE check_id = %s AND finding_status = 'open'
            """,
            (check_id,),
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def test_per_item_scopes_resolve_individually_and_carry_payload(
    clean_runtime: str,
) -> None:
    page = f"page:{uuid.uuid4().hex[:8]}"
    spec = _spec(per_link_program())
    check = IrPostPublishCheck(spec)

    _apply_page(clean_runtime, page, ["bad:x", "bad:y"], revision=1)
    gen1 = _publish(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        check.evaluate(
            conn,
            target_generation_id=gen1,
            catalog_version_id=1,
            deployment_name="default",
        )
        open1 = _open_findings(conn, spec.check_id)
        # Two findings on ONE subject, each with its own scope.
        assert sorted(open1.values()) == ["bad:x", "bad:y"]
        assert len(set(open1.keys())) == 2
        for scope in open1:
            assert scope.startswith(f"ir:bad_link:{page}:")
        # The program-projected payload AND evidence reached the store.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT payload->>'issue_kind', payload->>'target',
                       evidence->>'source_field', evidence->>'target'
                  FROM okg.observation_events
                 WHERE check_id = %s AND finding_event = 'observed'
                """,
                (spec.check_id,),
            )
            triage = {row for row in cur.fetchall()}
        assert triage == {
            ("bad_link", "bad:x", "links", "bad:x"),
            ("bad_link", "bad:y", "links", "bad:y"),
        }

    # Fix ONE link; the other stays bad.
    _apply_page(clean_runtime, page, ["bad:x"], revision=2)
    gen2 = _publish(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        check.evaluate(
            conn,
            target_generation_id=gen2,
            catalog_version_id=1,
            deployment_name="default",
        )
        open2 = _open_findings(conn, spec.check_id)
        # Exactly the fixed link resolved; the sibling finding stays open.
        assert list(open2.values()) == ["bad:x"]
        assert set(open2.keys()) < set(open1.keys())


def test_missing_declared_column_errors_loudly(clean_runtime: str) -> None:
    """A declared payload column the program does not project is an authoring
    bug — the run fails, never a silent default."""
    program = per_link_program(payload_column="not_projected")
    check = IrPostPublishCheck(_spec(program))
    page = f"page:{uuid.uuid4().hex[:8]}"
    _apply_page(clean_runtime, page, ["bad:z"], revision=1)
    gen = _publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation = check.evaluate(
            conn,
            target_generation_id=gen,
            catalog_version_id=1,
            deployment_name="default",
        )
        assert evaluation.result == "blocked_error"
        assert evaluation.reason == "gate_error"
        # The authoring error is named in the persisted gate details.
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT details FROM okg.consistency_gate_results
                 WHERE run_id = %s
                """,
                (evaluation.run_id,),
            )
            details = cur.fetchone()[0]
        assert "not_projected" in str(details)
        # Zero observation rows: the emission is all-or-nothing.
        assert _open_findings(conn, check.spec.check_id) == {}


@pytest.mark.parametrize("bad_attrs, expect", [
    # payload column projected but not a jsonb OBJECT (text link value)
    ({"payload_column": "link"}, "must be a jsonb object"),
    # declared scope column the program never projects
    ({"finding_scope_columns": ["nope"]}, "'nope'"),
])
def test_bad_declarations_block_the_run_loudly(
    clean_runtime: str, bad_attrs: dict, expect: str,
) -> None:
    check = IrPostPublishCheck(_spec(per_link_program(**bad_attrs)))
    page = f"page:{uuid.uuid4().hex[:8]}"
    _apply_page(clean_runtime, page, ["bad:q"], revision=1)
    gen = _publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation = check.evaluate(
            conn,
            target_generation_id=gen,
            catalog_version_id=1,
            deployment_name="default",
        )
        assert evaluation.result == "blocked_error"
        with conn.cursor() as cur:
            cur.execute(
                "SELECT details FROM okg.consistency_gate_results "
                "WHERE run_id = %s",
                (evaluation.run_id,),
            )
            assert expect in str(cur.fetchone()[0])


def test_json_keys_with_dangerous_characters_are_rejected_at_lowering() -> None:
    """The sqlglot session rewriter re-renders arrow keys WITHOUT
    re-escaping, so quoted/backslashed keys are rejected loudly at lowering
    instead of escaped."""
    from okg.substrate.transform.expr import json_get, render_sql

    for key in ("o'brien", "back\\slash", "nul\x00byte"):
        with pytest.raises(ValueError, match="json key"):
            render_sql(json_get(col("n.attrs"), key), {})
    with pytest.raises(ValueError, match="string literal"):
        render_sql({"json_get": [col("n.attrs"), 5]}, {})
