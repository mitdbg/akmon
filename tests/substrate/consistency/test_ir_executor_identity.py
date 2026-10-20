"""IR executor refuses undeclared finding identity (#691).

An IR consistency check keys each finding's ``observation_id`` off the
``normalized_value_columns`` it declares. When that declaration is
undeclared/empty the executor used to fall back to ``sorted(row)`` — the
WHOLE offender row — so a volatile projected column (a timestamp, a rowcount)
churned ``observation_id`` every generation and minted phantom duplicate
findings for the same real problem.

Convention (design.md Section 4 point (c), banned-grammar row 3): an identity
key is a function of every input that can change the computed value; if the
closure cannot be captured the computation REFUSES rather than storing a value
under a lossy key. There is no ``or sorted(row)`` fallback. A check that DOES
declare its columns keeps byte-identical behavior.

The refusal is a typed ``UndeclaredFindingIdentity`` naming the check and the
missing declaration. ``IrPostPublishCheck.evaluate`` catches it (its existing
broad handler) and surfaces the honest ``blocked_error`` gate state with the
exception class recorded — the same loud contract every other authoring bug in
``_emit`` (missing scope/payload columns) already follows.
"""
from __future__ import annotations

import uuid

import psycopg
import pytest

from okg.substrate.consistency import (
    IrConsistencyCheckSpec,
    IrPostPublishCheck,
)
from okg.substrate.consistency.ir_executor import _normalized_values
from okg.substrate.errors import UndeclaredFindingIdentity
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.transform import (
    Binding,
    Block,
    Effect,
    Op,
    OpKind,
    Terminator,
    TerminatorKind,
    TransformProgram,
)
from okg.substrate.transform.expr import col, eq, lit
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


# --------------------------------------------------------------------------- #
# Minimal IR check fixture (subject-local: WHERE subtype='page')              #
# --------------------------------------------------------------------------- #


def _check_attrs(*, declare_value_columns: bool) -> dict:
    attrs = {
        "predicate_key": "page_present",
        "normalized_predicate": "page_present",
        "participant_roles": {"page": "node_id"},
        "severity": "medium",
    }
    if declare_value_columns:
        attrs["normalized_value_columns"] = ["node_id"]
    return attrs


def _subject_local_program(*, declare_value_columns: bool) -> TransformProgram:
    return TransformProgram(
        program_id="fixture.ir_identity.subject_local",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": _check_attrs(declare_value_columns=declare_value_columns)},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(
                        Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}),
                        output="n",
                    ),
                    Binding(
                        Op(OpKind.WHERE, {"expr": eq(col("n.subtype"), lit("page"))}),
                        output="bad",
                        inputs=("n",),
                    ),
                    Binding(
                        Op(
                            OpKind.SELECT,
                            {"project": {"node_id": col("n.node_id")}},
                        ),
                        output="offending",
                        inputs=("bad",),
                    ),
                    Binding(
                        Op(OpKind.EMIT, {"target": "assertion_input"}),
                        inputs=("offending",),
                    ),
                ),
                terminator=Terminator(
                    TerminatorKind.RETURN, {"values": ["offending"]}
                ),
            ),
        ),
    )


def _ir_spec(*, declare_value_columns: bool) -> IrConsistencyCheckSpec:
    return IrConsistencyCheckSpec(
        name="ir_identity_page_ir",
        check_id=f"fixture.ir_identity.{uuid.uuid4().hex[:8]}",
        predicate_family="fixture_structure",
        confidence_type="deterministic_boolean",
        program=_subject_local_program(declare_value_columns=declare_value_columns),
        program_ref="tests.fixture.ir_identity_subject_local",
    )


def _seed_one_page_and_publish(dsn: str) -> str:
    sfx = uuid.uuid4().hex[:8]
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"], narrowings=[("page", "links_to", "page")])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(f"page:{sfx}:solo", "page", {}, {"id": sfx}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )
    out = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert out["status"] == "published", out
    return out["generation_id"]


# --------------------------------------------------------------------------- #
# End-to-end: the executor refuses undeclared finding identity                #
# --------------------------------------------------------------------------- #


@pytest.mark.postgres
def test_undeclared_normalized_value_columns_evaluate_is_refused(
    clean_runtime: str,
) -> None:
    """A check with NO ``normalized_value_columns`` must not key finding
    identity on the whole offender row. ``evaluate`` surfaces the honest
    ``blocked_error`` state with the typed refusal (``UndeclaredFindingIdentity``)
    recorded — never an ``evaluated`` result built on a ``sorted(row)`` identity.

    Pre-fix this returns ``result == 'evaluated'`` (the offender is stored with a
    whole-row identity), so this test reproduces #691."""
    gen = _seed_one_page_and_publish(clean_runtime)
    spec = _ir_spec(declare_value_columns=False)
    check = IrPostPublishCheck(spec)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation = check.evaluate(
            conn,
            target_generation_id=gen,
            catalog_version_id=1,
            deployment_name="default",
        )

    assert evaluation.result == "blocked_error", evaluation
    assert evaluation.result != "evaluated"
    err = evaluation.details["error"]
    assert err["type"] == "UndeclaredFindingIdentity", err
    # The refusal names the check and the missing declaration.
    assert spec.check_id in err["message"], err
    assert "normalized_value_columns" in err["message"], err


@pytest.mark.postgres
def test_declared_normalized_value_columns_still_evaluates(
    clean_runtime: str,
) -> None:
    """The guard leaves the declared-columns path byte-identical: a check that
    DOES declare ``normalized_value_columns`` still evaluates to a real finding,
    not a refusal."""
    gen = _seed_one_page_and_publish(clean_runtime)
    spec = _ir_spec(declare_value_columns=True)
    check = IrPostPublishCheck(spec)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation = check.evaluate(
            conn,
            target_generation_id=gen,
            catalog_version_id=1,
            deployment_name="default",
        )

    # Declared closure -> real measurement, never the undeclared-identity refusal.
    assert evaluation.result != "blocked_error", evaluation
    if evaluation.details.get("error"):
        assert evaluation.details["error"].get("type") != "UndeclaredFindingIdentity"


# --------------------------------------------------------------------------- #
# Unit: the identity helper refuses undeclared, is stable when declared        #
# (DB-free — the pure closure semantics the executor calls per offender row)   #
# --------------------------------------------------------------------------- #


def test_normalized_values_refuses_undeclared_closure() -> None:
    """No declared columns -> a typed refusal naming the check and the missing
    declaration. There is no ``sorted(row)`` fallback (banned-grammar row 3)."""
    row = {"node_id": "page:x", "scanned_at": "2026-07-14T00:00:00Z"}
    for undeclared in (None, [], ()):
        with pytest.raises(UndeclaredFindingIdentity) as excinfo:
            _normalized_values(row, undeclared, check_id="check.demo")
        msg = str(excinfo.value)
        assert "check.demo" in msg
        assert "normalized_value_columns" in msg
        assert excinfo.value.data == {"check_id": "check.demo"}


def test_normalized_values_declared_is_stable_across_volatile_columns() -> None:
    """The whole point of the declaration: identity folds ONLY the declared
    columns, so a volatile projected column (``scanned_at`` here) that changes
    every generation does NOT change the finding's identity closure. This is the
    churn #691 removes — proven byte-identical."""
    declared = ["node_id"]
    gen1 = {"node_id": "page:x", "scanned_at": "2026-07-14T00:00:00Z"}
    gen2 = {"node_id": "page:x", "scanned_at": "2026-07-15T23:59:59Z"}

    v1 = _normalized_values(gen1, declared, check_id="check.demo")
    v2 = _normalized_values(gen2, declared, check_id="check.demo")

    assert v1 == v2 == {"node_id": "page:x"}
    # The volatile column never entered the identity closure.
    assert "scanned_at" not in v1


def test_normalized_values_declared_matches_prior_sorted_row_for_full_closure() -> None:
    """Byte-compat guard: when the declared columns ARE the whole row, the result
    is identical to the retired ``sorted(row)`` computation — the fix only removes
    the UNDECLARED fallback, it does not change declared behavior."""
    row = {"b": 2, "a": 1, "c": 3}
    declared_full = sorted(row)  # what the old fallback would have iterated
    assert _normalized_values(row, declared_full, check_id="check.demo") == {
        "a": 1,
        "b": 2,
        "c": 3,
    }


# --------------------------------------------------------------------------- #
# As-shipped guard: the live okg-workspace IR checks declare their identity    #
# (DB-free — loads the deployment programs so the #691 refusal can never       #
#  silently convert a shipped check into refuse-on-offense)                    #
# --------------------------------------------------------------------------- #


def test_shipped_okgworkspace_ir_checks_declare_identity() -> None:
    """Every IR consistency check wired into the live okg-workspace dogfood
    deployment MUST declare ``normalized_value_columns``. Post-#691 an undeclared
    check no longer falls back to ``sorted(row)`` — it REFUSES the moment it finds
    an offender, hiding the very defect it exists to catch. This guard fails at
    CI time (no DB needed) if a shipped program omits the declaration."""
    import importlib.util
    import pathlib

    path = (
        pathlib.Path(__file__).parents[3]
        / "deployments/okg-workspace/checks/structural_ir.py"
    )
    spec = importlib.util.spec_from_file_location("okgws_structural_ir", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.PROGRAMS, "okg-workspace ships no IR programs"
    for factory in module.PROGRAMS:
        program = factory()
        declared = program.attrs["check"].get("normalized_value_columns")
        assert declared, (
            f"{factory.__name__}: shipped okg-workspace IR check must declare "
            "normalized_value_columns (else #691 refuses on first offender)"
        )
