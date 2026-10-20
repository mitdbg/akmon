"""Per-assignment finding identity (PACT consistency-finding-identity).

req.identity.declared-or-refused: an IR check whose emission contract does not
declare ``normalized_value_columns`` is refused AT LOAD with an authoring
error naming the check and the missing field; the executor's projection-
derived ``sorted(row)`` fallback is DELETED (ir_executor + the equivalence
harness's mirror), so declared columns are the only identity path — a spec
that skips the loader hits the executor's named backstop error, never a
projection-derived identity.

req.identity.per-assignment: with identity columns declared, a finding's
identity is a pure function of (check, satisfying assignment) — invariant
under projection reordering, addition of undeclared columns, and label-only
edits (severity / finding_event). Witnessed two ways:

* **Row level, definition held fixed:** the executor's own identity helpers
  (``_normalized_values_for_row`` + ``_finding_scope_digest`` +
  ``observation_id_for``) produce byte-identical observation ids across
  row-shape mutations, and distinct ids for distinct assignments.
* **Executor level, through the real lifecycle:** re-running mutated programs
  (reordered projection / extra undeclared column / severity relabel) against
  an unchanged graph leaves the OPEN finding set unchanged — same logical
  identity (input_scope, normalized_values), stable open count, and ZERO
  ``resolved`` events, i.e. none of the resolve-and-reopen churn the fallback
  caused. (Program mutations intentionally re-version the check's structural
  hashes; the durable finding identity is the logical key the current-state
  projection dedupes on, and a pure projection REORDER does not even change
  the program checksum — those observation ids are byte-stable.)

Two distinct satisfying assignments on one subject yield two findings when
``finding_scope_columns`` declares the assignment scope.
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
from okg.substrate.consistency.ir_executor import (  # noqa: E402
    GENERATION_POLICY,
    _finding_scope_digest,
    _normalized_values_for_row,
)
from okg.substrate.consistency.observations import (  # noqa: E402
    ConsistencyCandidate,
    candidate_id_for,
    observation_id_for,
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


def _t(value: str) -> dict:
    # Explicit ::text — bare parameters in SELECT lists are untyped under the
    # extended protocol.
    return cast(lit(value), "text")


def _check_attrs(**overrides) -> dict:
    attrs = {
        "predicate_key": "page_links_resolve",
        "normalized_predicate": "bad_link",
        "participant_roles": {"page": "node_id"},
        "severity": "medium",
        # The declared assignment: the subject and the offending link.
        "normalized_value_columns": ["node_id", "link"],
        # One page can hold several bad links — one finding per assignment.
        "finding_scope_columns": ["link"],
    }
    attrs.update(overrides)
    # An explicit None override means "drop the key entirely" (an authoring
    # omission, not a null declaration).
    return {k: v for k, v in attrs.items() if v is not None}


def _program(
    *,
    reorder_projection: bool = False,
    extra_column: bool = False,
    severity: str = "medium",
    **attr_overrides,
) -> TransformProgram:
    """The per-link bad-link program, with identity-IRRELEVANT mutations.

    The projection deliberately carries label literals (``severity``,
    ``finding_event``) — the exact columns the deleted sorted(row) fallback
    used to absorb into finding identity.
    """
    links = func(
        "coalesce", json_member(col("n.attrs"), "links"), cast(lit("[]"), "jsonb")
    )
    pairs = [
        ("node_id", col("n.node_id")),
        ("link", col("link")),
        ("severity", _t(severity)),
        ("finding_event", _t("observed")),
    ]
    if extra_column:
        pairs.append(("debug_note", _t("undeclared debug noise")))
    if reorder_projection:
        pairs = list(reversed(pairs))
    return TransformProgram(
        program_id="fixture.finding_identity.bad_link",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": _check_attrs(severity=severity, **attr_overrides)},
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
                        Op(OpKind.SELECT, {"project": dict(pairs)}),
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


def _spec(program: TransformProgram, check_id: str) -> IrConsistencyCheckSpec:
    return IrConsistencyCheckSpec(
        name="bad_link_identity_ir",
        check_id=check_id,
        predicate_family="fixture_links",
        confidence_type="deterministic_boolean",
        program=program,
        program_ref="tests.fixture.finding_identity_program",
    )


def _fresh_check_id() -> str:
    return f"fixture.finding_identity.{uuid.uuid4().hex[:8]}"


# --------------------------------------------------------------------------- #
# req.identity.declared-or-refused — refusal at load, named                    #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("undeclared", [
    {"normalized_value_columns": None},      # key absent entirely
    {"normalized_value_columns": []},        # declared but empty
    {"normalized_value_columns": "link"},    # not a list
    {"normalized_value_columns": [1]},       # not column names
])
def test_undeclared_identity_columns_are_refused_at_load(undeclared) -> None:
    check_id = "fixture.finding_identity.refused.v1"
    spec = _spec(_program(**undeclared), check_id)
    with pytest.raises(IrCheckLoadError) as excinfo:
        _validate_program(spec, where="test")
    message = str(excinfo.value)
    # The authoring error names the check and the missing field.
    assert check_id in message
    assert "normalized_value_columns" in message


def test_declared_identity_columns_pass_load() -> None:
    _validate_program(
        _spec(_program(), "fixture.finding_identity.ok.v1"), where="test"
    )


# --------------------------------------------------------------------------- #
# req.identity.per-assignment — row level, definition held fixed               #
# --------------------------------------------------------------------------- #

def _row_identity(spec: IrConsistencyCheckSpec, row: dict) -> tuple[str, str, dict]:
    """(input_scope, observation_id, normalized_values) for one offender row,
    computed with the executor's own identity helpers (the same calls
    ``IrPostPublishCheck._emit`` makes per row)."""
    definition = spec.definition("default")
    check_attrs = spec.program.attrs["check"]
    roles = {
        str(role): str(row[column])
        for role, column in check_attrs["participant_roles"].items()
    }
    subject = roles[check_attrs.get("subject_role") or next(iter(roles))]
    input_scope = (
        f"ir:{check_attrs['normalized_predicate']}:{subject}:"
        + _finding_scope_digest(
            row, check_attrs["finding_scope_columns"], check_id=spec.check_id
        )
    )
    normalized_values = _normalized_values_for_row(
        row, check_attrs["normalized_value_columns"], check_id=spec.check_id
    )
    candidate_id = candidate_id_for(definition, ConsistencyCandidate(
        input_scope=input_scope,
        blocking_rule_hash=spec.program.checksum(),
        participant_roles=roles,
        predicate_key=str(check_attrs["predicate_key"]),
        generation_policy=GENERATION_POLICY,
    ))
    observation_id = observation_id_for(
        definition,
        candidate_id=candidate_id,
        input_scope=input_scope,
        participant_roles=roles,
        normalized_predicate=str(check_attrs["normalized_predicate"]),
        normalized_values=normalized_values,
        source_revision_policy=GENERATION_POLICY,
    )
    return input_scope, observation_id, normalized_values


def test_observation_id_is_a_pure_function_of_the_declared_assignment() -> None:
    spec = _spec(_program(), "fixture.finding_identity.pure.v1")
    base_row = {
        "node_id": "page:1",
        "link": "bad:x",
        "severity": "medium",
        "finding_event": "observed",
    }
    mutations = [
        dict(reversed(list(base_row.items()))),            # projection reorder
        {**base_row, "debug_note": "undeclared noise"},    # extra column
        {**base_row, "severity": "high",                   # label-only relabel
         "finding_event": "resolved_candidate"},
    ]
    base = _row_identity(spec, base_row)
    for mutated in mutations:
        assert _row_identity(spec, mutated) == base
    # Identity payload is exactly the declared columns — labels never leak in.
    assert base[2] == {"node_id": "page:1", "link": "bad:x"}

    # A DIFFERENT satisfying assignment on the same subject is a different
    # finding: new scope, new observation id.
    other = _row_identity(spec, {**base_row, "link": "bad:y"})
    assert other[0] != base[0]
    assert other[1] != base[1]


def test_undeclared_or_unprojected_columns_error_loudly_at_row_level() -> None:
    # No declaration (a spec that skipped the loader): the executor's backstop
    # names the check and the field — never a projection-derived identity.
    with pytest.raises(ValueError, match="'c.v1'.*normalized_value_columns"):
        _normalized_values_for_row({"node_id": "n"}, None, check_id="c.v1")
    with pytest.raises(ValueError, match="normalized_value_columns"):
        _normalized_values_for_row({"node_id": "n"}, [], check_id="c.v1")
    # A declared column the program does not project is an authoring bug.
    with pytest.raises(ValueError, match="'link'"):
        _normalized_values_for_row(
            {"node_id": "n"}, ["node_id", "link"], check_id="c.v1"
        )


# --------------------------------------------------------------------------- #
# req.identity.per-assignment — through the real executor + lifecycle          #
# --------------------------------------------------------------------------- #

def _publish(dsn: str) -> int:
    """Publish and return the compat INTEGER generation id.

    The publisher's cycle output carries it directly; resolving the Chronos
    checkpoint string back through the generation resolver is unnecessary
    (and fragile against shared-test-DB state)."""
    out = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert out["status"] == "published", out
    return int(out["compat_generation_id"])


def _apply_page(dsn: str, node_id: str, links: list[str]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact(node_id, "page", {"links": links}, {"id": node_id}, {"v": 1})],
            source="default",
            strict_endpoints=True,
        )


def _open_rows(conn, check_id: str) -> dict[str, dict]:
    """open input_scope -> {observation_id, normalized_values, severity}."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT input_scope, observation_id, normalized_values, severity
              FROM okg.observation_current
             WHERE check_id = %s AND finding_status = 'open'
            """,
            (check_id,),
        )
        return {
            row[0]: {
                "observation_id": row[1],
                "normalized_values": row[2],
                "severity": row[3],
            }
            for row in cur.fetchall()
        }


def _resolved_event_count(conn, check_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.observation_events "
            "WHERE check_id = %s AND finding_event = 'resolved'",
            (check_id,),
        )
        return int(cur.fetchone()[0])


def _evaluate(conn, spec: IrConsistencyCheckSpec, generation: int) -> None:
    evaluation = IrPostPublishCheck(spec).evaluate(
        conn,
        target_generation_id=generation,
        catalog_version_id=1,
        deployment_name="default",
    )
    assert evaluation.result != "blocked_error", evaluation.details


@pytest.mark.postgres
def test_projection_mutations_never_churn_the_open_finding_set(
    clean_runtime: str,
) -> None:
    """Reordered projection / extra undeclared column / severity relabel over
    an UNCHANGED graph: the open finding set keeps its per-assignment identity
    — same scopes, same normalized_values, stable count — and not one
    ``resolved`` event is stamped (the fallback's resolve-and-reopen churn).
    A pure reorder leaves even the stored observation ids byte-identical."""
    check_id = _fresh_check_id()
    page = f"page:{uuid.uuid4().hex[:8]}"
    _apply_page(clean_runtime, page, ["bad:x", "bad:y"])
    gen = _publish(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _evaluate(conn, _spec(_program(), check_id), gen)
        baseline = _open_rows(conn, check_id)
        assert len(baseline) == 2  # one finding per assignment (two bad links)
        logical = {
            scope: row["normalized_values"] for scope, row in baseline.items()
        }

        # (a) projection REORDER: the canonical program checksum is order-
        # stable, so the stored observation ids are byte-identical.
        _evaluate(conn, _spec(_program(reorder_projection=True), check_id), gen)
        after_reorder = _open_rows(conn, check_id)
        assert after_reorder == baseline

        # (b) extra UNDECLARED column: identity ignores it — same scopes, same
        # normalized_values (no debug column absorbed), same open count.
        _evaluate(conn, _spec(_program(extra_column=True), check_id), gen)
        after_extra = _open_rows(conn, check_id)
        assert {
            s: r["normalized_values"] for s, r in after_extra.items()
        } == logical
        assert all(
            "debug_note" not in r["normalized_values"]
            for r in after_extra.values()
        )

        # (c) severity RELABEL: a label-only edit — identity unchanged, the
        # label updates in place on the same findings.
        _evaluate(conn, _spec(_program(severity="high"), check_id), gen)
        after_relabel = _open_rows(conn, check_id)
        assert {
            s: r["normalized_values"] for s, r in after_relabel.items()
        } == logical
        assert {r["severity"] for r in after_relabel.values()} == {"high"}

        # No resolve-and-reopen churn across ANY mutation: zero resolved
        # events, and still exactly one open finding per assignment.
        assert _resolved_event_count(conn, check_id) == 0
        assert len(after_relabel) == 2


@pytest.mark.postgres
def test_two_assignments_on_one_subject_are_two_findings(
    clean_runtime: str,
) -> None:
    """finding_scope_columns declares the assignment scope, so two distinct
    satisfying assignments on ONE subject hold two independently-keyed open
    findings — per-subject granularity never collapses them into one."""
    check_id = _fresh_check_id()
    page = f"page:{uuid.uuid4().hex[:8]}"
    _apply_page(clean_runtime, page, ["bad:x", "bad:y"])
    gen = _publish(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _evaluate(conn, _spec(_program(), check_id), gen)
        open_rows = _open_rows(conn, check_id)
        assert len(open_rows) == 2
        # Distinct scopes on the same subject prefix...
        for scope in open_rows:
            assert scope.startswith(f"ir:bad_link:{page}:")
        # ...distinct observation ids, and the assignment column tells them apart.
        ids = {row["observation_id"] for row in open_rows.values()}
        assert len(ids) == 2
        assert {
            row["normalized_values"]["link"] for row in open_rows.values()
        } == {"bad:x", "bad:y"}
        assert all(
            row["normalized_values"]["node_id"] == page
            for row in open_rows.values()
        )


@pytest.mark.postgres
def test_spec_that_skipped_the_loader_blocks_the_run_loudly(
    clean_runtime: str,
) -> None:
    """The deleted fallback has no runtime replacement: a directly-constructed
    spec without normalized_value_columns fails its run with the named
    authoring error and stores NOTHING — never a projection-derived identity."""
    check_id = _fresh_check_id()
    page = f"page:{uuid.uuid4().hex[:8]}"
    _apply_page(clean_runtime, page, ["bad:z"])
    gen = _publish(clean_runtime)

    spec = _spec(_program(normalized_value_columns=None), check_id)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation = IrPostPublishCheck(spec).evaluate(
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
            details = str(cur.fetchone()[0])
        assert "normalized_value_columns" in details
        assert _open_rows(conn, check_id) == {}
