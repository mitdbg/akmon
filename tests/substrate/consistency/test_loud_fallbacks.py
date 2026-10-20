"""Loud-fallback classification + honest crash-time reporting.

PACT ``consistency-engine-hardening``, task.impl.loud-fallbacks
(req.impl.loud-fallbacks). Every consistency-engine fallback must record the
exception CLASS it fell back from, and a post-publish check crash must never
report a synthesized all-zero summary as if it were a measurement — it reports
an explicit ``blocked_error`` state distinguishable from "ran and found
nothing", and never reads back a different run's persisted row (that
misattribution read-back was removed as net-deceptive, 2026-07-07). Both
post-publish paths — the rerun function and the publisher's live inline loop —
build that summary through ``build_post_publish_crash_summary`` so they cannot
drift.

The fallback DECISIONS are unchanged (full sweep / best-effort drop /
generation-stays-published); only their DIAGNOSABILITY is proven here. The
former branch-head checkout fallback is gone: post-publish checks read the
published generation or fail with ``PostPublishGenerationUnresolved``. Test names carry "fallback" or "crash" so the PACT's
``-k "fallback or crash"`` selector picks them up.
"""
from __future__ import annotations

import importlib
import json
import logging
import uuid
from uuid import UUID, uuid4

import psycopg
import psycopg.sql
import pytest

from okg.substrate.consistency import (
    CandidateCoverageReport,
    ConsistencyCandidate,
    ConsistencyCheckDefinition,
    IrConsistencyCheckSpec,
    IrPostPublishCheck,
    ObservationEvent,
    append_observation_event,
    evaluate_consistency_publish_gate,
    observation_id_for,
    record_candidate,
    record_candidate_coverage,
    start_consistency_run,
)
from okg.substrate.consistency.sql_gate import (
    load_consistency_gates,
    run_one_post_publish_check,
    run_post_publish_consistency_checks,
)
from okg.substrate.derived_artifacts import stable_hash
from okg.substrate.errors import PostPublishGenerationUnresolved
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
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
from okg.substrate.transform.expr import and_, col, eq, json_exists, lit
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

IR_EXECUTOR_LOGGER = "okg.substrate.consistency.ir_executor"
SQL_GATE_LOGGER = "okg.substrate.consistency.sql_gate"


# --------------------------------------------------------------------------- #
# Minimal IR check fixture (subject-local: WHERE subtype='page')              #
# --------------------------------------------------------------------------- #

_CHECK_ATTRS = {
    "predicate_key": "page_present",
    "normalized_predicate": "page_present",
    "participant_roles": {"page": "node_id"},
    "severity": "medium",
    "normalized_value_columns": ["node_id"],
}


def _subject_local_program(extra: dict | None = None) -> TransformProgram:
    where = eq(col("n.subtype"), lit("page"))
    if extra is not None:
        where = and_(where, extra)
    return TransformProgram(
        program_id="fixture.loud_fallbacks.subject_local",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": dict(_CHECK_ATTRS)},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(
                        Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}),
                        output="n",
                    ),
                    Binding(
                        Op(OpKind.WHERE, {"expr": where}),
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


def _ir_spec(
    *, name: str = "loud_fallback_page_ir", extra: dict | None = None
) -> IrConsistencyCheckSpec:
    return IrConsistencyCheckSpec(
        name=name,
        check_id=f"fixture.loud_fallback.{uuid.uuid4().hex[:8]}",
        predicate_family="fixture_structure",
        confidence_type="deterministic_boolean",
        program=_subject_local_program(extra),
        program_ref="tests.fixture.loud_fallback_subject_local",
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
# Site 1 (ir_executor): previous-generation checkout load -> full-scan        #
# --------------------------------------------------------------------------- #


@pytest.mark.postgres
def test_previous_generation_load_fallback_logs_exception_class(
    clean_runtime: str,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the previous-generation checkout raises, the run still widens to a
    full scan (decision unchanged) AND logs the originating exception class so
    "why did this run go full instead of scoped" is answerable."""
    gen1 = _seed_one_page_and_publish(clean_runtime)
    spec = _ir_spec()
    check = IrPostPublishCheck(spec)

    # Inject a deterministic checkpoint-load failure for the previous generation
    # only; the target checkout (a real published generation) passes through.
    sentinel_previous = 999_999
    real_checkout_generation = ChronosGraphStore.checkout_generation

    def _patched(self, generation_id, **kwargs):  # type: ignore[no-untyped-def]
        if str(generation_id) == str(sentinel_previous):
            raise KeyError("injected: no checkpoint for previous generation")
        return real_checkout_generation(self, generation_id, **kwargs)

    monkeypatch.setattr(ChronosGraphStore, "checkout_generation", _patched)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with caplog.at_level(logging.INFO, logger=IR_EXECUTOR_LOGGER):
            evaluation = check.evaluate(
                conn,
                target_generation_id=gen1,
                catalog_version_id=1,
                deployment_name="default",
                previous_generation_id=sentinel_previous,
            )

    # Decision unchanged: the run completed (a full sweep, since no previous
    # session could be loaded to derive a scope).
    assert evaluation is not None

    recs = [
        r
        for r in caplog.records
        if r.name == IR_EXECUTOR_LOGGER and r.levelno == logging.INFO
    ]
    assert recs, "expected an INFO record from the previous-session fallback"
    msg = recs[-1].getMessage()
    # The exception CLASS (not just a generic message) is recorded.
    assert "KeyError" in msg, msg
    assert str(sentinel_previous) in msg, msg
    assert "full scan" in msg, msg


# --------------------------------------------------------------------------- #
# Site 2 (ir_executor): generation checkout -- no branch-head fallback        #
# --------------------------------------------------------------------------- #


def test_crash_summary_keeps_a_substrate_errors_code_and_data() -> None:
    """A check with no refusal recorder still reports why: the crash summary
    carries the error's code and data (for a mirror refusal, its reason)."""
    from okg.substrate.consistency.sql_gate import build_post_publish_crash_summary
    from okg.substrate.errors import ChronosSqlMirrorMismatch

    exc = ChronosSqlMirrorMismatch(
        "refresh failed",
        data={
            "expected_checkpoint_id": "cp",
            "held_checkpoint_id": None,
            "reason": "refresh_failed",
        },
    )
    summary = build_post_publish_crash_summary(
        gate_name="mirror_reader",
        target_generation_id=1,
        exc=exc,
        elapsed_ms=0,
        logger=logging.getLogger(__name__),
    )
    assert summary["result"] == "blocked_error"
    assert summary["details"]["error_code"] == "chronos_sql_mirror_mismatch"
    assert summary["details"]["error_data"]["reason"] == "refresh_failed"
    plain = build_post_publish_crash_summary(
        gate_name="plain",
        target_generation_id=1,
        exc=RuntimeError("boom"),
        elapsed_ms=0,
        logger=logging.getLogger(__name__),
    )
    assert plain["details"] == {"error_type": "RuntimeError", "error": "boom"}


def test_generation_checkout_refuses_branch_head_fallback() -> None:
    """``_checkout`` used to fall back to the live branch head when the id did
    not open a checkpoint. The publisher passed its compat clock id, so every
    post-publish IR check silently judged the head. An unresolvable id now
    raises a named, registered error and never reads the head; ``0`` (the
    pre-checkpoint "working head" sentinel) is refused before any checkout."""

    head_reads: list[str] = []

    class _Checkpoint:
        checkpoint_id = "okg:default:branch:default:gen:retired"

    class _FakeStore:
        def __init__(self, resolved):
            self.resolved = resolved

        def resolve_generation(self, generation_id, **kwargs):
            return self.resolved

        def checkout_generation(self, generation_id):  # positional, as called
            raise KeyError(f"no checkpoint for generation {generation_id!r}")

        def checkout(self, branch="default"):
            head_reads.append(branch)
            return f"branch-head-session:{branch}"

    # The id matches no published checkpoint.
    with pytest.raises(PostPublishGenerationUnresolved) as exc_info:
        IrPostPublishCheck._checkout(_FakeStore(None), 424_242)
    assert exc_info.value.code == "post_publish_generation_unresolved"
    assert exc_info.value.data["generation_id"] == "424242"

    # The id resolves, but the checkpoint cannot be opened.
    with pytest.raises(PostPublishGenerationUnresolved) as exc_info:
        IrPostPublishCheck._checkout(_FakeStore(_Checkpoint()), 424_242)
    assert exc_info.value.data["error_type"] == "KeyError"
    assert isinstance(exc_info.value.__cause__, KeyError)

    # 0 is the pre-checkpoint "working head" sentinel: refused outright.
    with pytest.raises(PostPublishGenerationUnresolved):
        IrPostPublishCheck._checkout(_FakeStore(_Checkpoint()), 0)
    assert head_reads == []


# --------------------------------------------------------------------------- #
# Post-publish checks read the PUBLISHED generation, not the live head        #
# --------------------------------------------------------------------------- #

_MIRROR_PAGE_SQL_CHECK = {
    "name": "published_generation_page_mirror_gate",
    "type": "deterministic_sql",
    "check_id": "test.sql.published-generation-page-mirror",
    "predicate_family": "published_generation_mirror",
    "confidence_type": "deterministic_boolean",
    "candidate_sql": """
        SELECT
          ('node:' || node_id)::text AS input_scope,
          'block:published-generation-mirror'::text AS blocking_rule_hash,
          jsonb_build_object('node', node_id) AS participant_roles,
          'published_generation_page_visible'::text AS predicate_key,
          'generation_live_view_v1'::text AS generation_policy,
          true AS emit_observation,
          'published_generation_page_visible'::text AS normalized_predicate,
          jsonb_build_object('node_id', node_id) AS normalized_values,
          'low'::text AS severity,
          1.0::numeric AS confidence,
          jsonb_build_object('node_id', node_id) AS evidence
        FROM okg.chronos_graph_nodes_current
        WHERE subtype = 'page'
    """,
    "coverage_sql": """
        SELECT
          ('published_generation_mirror:' ||
            %(target_generation_id)s::text) AS universe_key,
          'exact_postgres'::text AS coverage_basis,
          true AS coverage_complete,
          count(*)::bigint AS eligible_input_count,
          count(*)::bigint AS candidate_count,
          0::bigint AS evaluator_invocation_count,
          count(*)::bigint AS all_pairs_upper_bound
        FROM okg.chronos_graph_nodes_current
        WHERE subtype = 'page'
    """,
    "timeout_ms": 3000,
}


def _mirror_sql_check(tmp_path):
    import yaml

    from okg.substrate.consistency import load_post_publish_consistency_checks

    dep = tmp_path / "published_generation_dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "default",
            "post_publish_consistency_checks": [dict(_MIRROR_PAGE_SQL_CHECK)],
        })
    )
    (check,) = load_post_publish_consistency_checks(dep)
    return check


HEAD_ONLY_FLAG = "head_only_flag"


def _head_only_flag_ir_check() -> IrPostPublishCheck:
    """Flags a page carrying HEAD_ONLY_FLAG: a finding only a head read sees."""
    return IrPostPublishCheck(
        _ir_spec(
            name="loud_fallback_head_only_flag_ir",
            extra=json_exists(col("n.attrs"), HEAD_ONLY_FLAG),
        )
    )


def _page_fact(node_id: str) -> NodeFact:
    return NodeFact(node_id, "page", {}, {"id": node_id}, {"v": 1})


def _publish_then_write_page_to_head(
    dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    checks: list,
) -> tuple[dict, str, str]:
    """Publish one page, and write a SECOND page to the live branch head after
    the checkpoint is taken but before the post-publish checks run.

    The second page is a real head write that the published generation does
    not contain. A check that reads the head counts two pages; a check that
    reads the published generation counts one.
    """
    from tests.pytest.catalog_fixture_builders import seed_catalog

    sfx = uuid.uuid4().hex[:8]
    published_page = f"page:{sfx}:published"
    head_page = f"page:{sfx}:head-only"
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"], narrowings=[("page", "links_to", "page")])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_page_fact(published_page)], source="default", strict_endpoints=True
        )

    original = Publisher._finalize_lens_promotion_generation_refs
    head_writes: list[str] = []

    def _finalize_then_write_head(self, conn, **kwargs):  # type: ignore[no-untyped-def]
        original(self, conn, **kwargs)
        # Runs after checkpoint_generation and before the post-publish block.
        # A separate, committed write: any later read of the branch head --
        # from this connection or another -- sees the head-only page.
        with psycopg.connect(dsn, autocommit=True) as writer:
            writer.execute("SET lock_timeout = '60s'")
            ChronosGraphStore.from_connection(
                writer, ensure_schema=False
            ).apply_facts(
                [
                    _page_fact(head_page),
                    # A new head revision of the published page carrying a
                    # flag the published generation does not have.
                    NodeFact(
                        published_page,
                        "page",
                        {HEAD_ONLY_FLAG: True},
                        {"id": published_page},
                        {"v": 2},
                    ),
                ],
                source="default",
                strict_endpoints=True,
            )
        # Prove a head read from the publisher's own connection sees it.
        head = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        head_ids = _page_ids(head.checkout(self.branch_name))
        assert head_page in head_ids, head_ids
        head_writes.append(head_page)

    monkeypatch.setattr(
        Publisher,
        "_finalize_lens_promotion_generation_refs",
        _finalize_then_write_head,
    )
    out = Publisher(
        dsn,
        "default",
        post_publish_consistency_checks=checks,
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    monkeypatch.setattr(
        Publisher, "_finalize_lens_promotion_generation_refs", original
    )
    assert out["status"] == "published", out
    assert head_writes == [head_page]
    return out, published_page, head_page


def _page_ids(session) -> list[str]:
    return sorted(
        str(row["node_id"])
        for row in session.query(
            "SELECT node_id FROM okg.graph_nodes WHERE subtype = 'page'"
        )
    )


def _open_finding_node_ids(conn, check_id: str) -> list[str]:
    """Node ids of the open findings a check recorded (its offending rows)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT normalized_values->>'node_id'
              FROM okg.observation_current
             WHERE check_id = %s AND finding_status = 'open'
             ORDER BY 1
            """,
            (check_id,),
        )
        return [str(row[0]) for row in cur.fetchall()]


def _read_pin_count(conn) -> int:
    """History read leases still held (a checkout that was never released)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.history_generation_pins "
            "WHERE pin_name LIKE 'read:%%'"
        )
        return int(cur.fetchone()[0])


def _mirror_page_ids(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT node_id FROM okg.chronos_graph_nodes_current "
            "WHERE subtype = 'page' ORDER BY node_id"
        )
        return [str(row[0]) for row in cur.fetchall()]


@pytest.mark.postgres
def test_live_post_publish_checks_read_published_generation_not_head(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The live publish path: both the IR check (Chronos checkout) and the SQL
    check (the chronos_graph_*_current mirror) judge the generation just
    published. A page written to the head after the checkpoint is invisible to
    both, although the head really holds it."""
    ir_check = IrPostPublishCheck(_ir_spec())
    flag_check = _head_only_flag_ir_check()
    sql_check = _mirror_sql_check(tmp_path)
    out, published_page, head_page = _publish_then_write_page_to_head(
        clean_runtime, monkeypatch, [ir_check, flag_check, sql_check]
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # The flag exists only on the head revision of a page in this
        # publish's scope: a head read would record one finding.
        assert _open_finding_node_ids(conn, flag_check.spec.check_id) == []
        # Every generation checkout the publish and its checks took was
        # released (counted before this test opens a checkout of its own).
        assert _read_pin_count(conn) == 0
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        # The head write is real: the branch head holds both pages ...
        assert _page_ids(store.checkout("default")) == sorted(
            [published_page, head_page]
        )
        # ... and the published generation holds only the first.
        assert _page_ids(store.checkout_generation(out["generation_id"])) == [
            published_page
        ]
        # The SQL mirror was copied from the published generation.
        assert _mirror_page_ids(conn) == [published_page]
        # The IR check's recorded offender is the published page only; a
        # head read would also have recorded the head-only page.
        assert _open_finding_node_ids(conn, ir_check.spec.check_id) == [
            published_page
        ]
        assert _open_finding_node_ids(conn, sql_check.spec.check_id) == [
            published_page
        ]

    by_name = {s["gate_name"]: s for s in out["post_publish_consistency_checks"]}
    ir_summary = by_name[ir_check.name]
    sql_summary = by_name[sql_check.name]
    flag_summary = by_name[flag_check.name]
    assert flag_summary["result"] == "evaluated", flag_summary
    assert flag_summary["open_observation_count"] == 0, flag_summary
    assert ir_summary["result"] == "evaluated", ir_summary
    assert ir_summary["open_observation_count"] == 1, ir_summary
    assert sql_summary["result"] == "evaluated", sql_summary
    assert sql_summary["open_observation_count"] == 1, sql_summary


@pytest.mark.postgres
def test_rerun_recopies_mirror_and_reads_named_generation(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``consistency-rerun`` refreshes the SQL mirror from the generation it
    rechecks, and the IR check reads that generation's checkpoint. Before the
    rerun the mirror is made to hold the head (two pages); afterwards it holds
    the named generation (one page), and both checks count one."""
    ir_check = IrPostPublishCheck(_ir_spec())
    sql_check = _mirror_sql_check(tmp_path)
    out, published_page, head_page = _publish_then_write_page_to_head(
        clean_runtime, monkeypatch, [ir_check, sql_check]
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Stale mirror: make it describe the head, not the named generation,
        # and stamp it as another generation (a copy already stamped with the
        # named checkpoint is trusted and not re-copied).
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.chronos_graph_nodes_current (node_id, subtype) "
                "VALUES (%s, 'page') ON CONFLICT DO NOTHING",
                (head_page,),
            )
            cur.execute(
                psycopg.sql.SQL(
                    "COMMENT ON TABLE okg.chronos_graph_nodes_current IS {}"
                ).format(
                    psycopg.sql.Literal(
                        json.dumps(
                            {
                                "schema": "okg.chronos_sql_mirror.v1",
                                "checkpoint_id": "the-live-head",
                            }
                        )
                    )
                )
            )
        assert _mirror_page_ids(conn) == sorted([published_page, head_page])

        report = run_post_publish_consistency_checks(
            conn,
            [ir_check, sql_check],
            deployment_name="default",
            generation_id=out["generation_id"],
        )
        assert _mirror_page_ids(conn) == [published_page]
        # The rerun released every generation checkout it took.
        assert _read_pin_count(conn) == 0

    assert report["native_generation_id"] == out["generation_id"]
    by_name = {s["gate_name"]: s for s in report["checks"]}
    assert by_name[ir_check.name]["result"] == "evaluated", by_name
    assert by_name[ir_check.name]["open_observation_count"] == 1, by_name
    assert by_name[sql_check.name]["result"] == "evaluated", by_name
    assert by_name[sql_check.name]["open_observation_count"] == 1, by_name


@pytest.mark.postgres
def test_unresolvable_generation_fails_ir_check_and_mirror_refresh(
    clean_runtime: str,
) -> None:
    """An id that opens no checkpoint fails loudly on both read paths: the IR
    check records a failed run naming PostPublishGenerationUnresolved (no
    findings from the head), and the mirror refresh raises."""
    from okg.substrate.consistency.chronos_sql_mirror import (
        refresh_chronos_sql_mirror,
    )

    _seed_one_page_and_publish(clean_runtime)
    bogus = "generation:does-not-exist"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation = IrPostPublishCheck(_ir_spec()).evaluate(
            conn,
            target_generation_id=970_202,
            checkout_generation_id=bogus,
            catalog_version_id=1,
            deployment_name="default",
        )
        assert evaluation.result == "blocked_error"
        assert evaluation.open_observation_count == 0
        assert (
            evaluation.details["error"]["type"]
            == "PostPublishGenerationUnresolved"
        ), evaluation.details

        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        with pytest.raises(PostPublishGenerationUnresolved) as exc_info:
            refresh_chronos_sql_mirror(
                conn, store=store, generation_id=bogus, deployment_name="default"
            )
        assert exc_info.value.data["generation_id"] == bogus


@pytest.mark.postgres
def test_ir_checkout_and_mirror_refresh_release_their_read_leases(
    clean_runtime: str,
) -> None:
    """An IR check and a mirror refresh each open a generation checkout, which
    takes a history read lease; both release it when they finish."""
    from okg.substrate.consistency.chronos_sql_mirror import (
        refresh_chronos_sql_mirror,
    )

    generation = _seed_one_page_and_publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _read_pin_count(conn) == 0
        evaluation = IrPostPublishCheck(_ir_spec()).evaluate(
            conn,
            target_generation_id=generation,
            catalog_version_id=1,
            deployment_name="default",
        )
        assert evaluation.result == "evaluated", evaluation.details
        assert _read_pin_count(conn) == 0
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        refresh_chronos_sql_mirror(
            conn, store=store, generation_id=generation, deployment_name="default"
        )
        assert _read_pin_count(conn) == 0


@pytest.mark.postgres
def test_ir_check_refuses_checkout_and_record_ids_naming_different_generations(
    clean_runtime: str,
) -> None:
    """When both ids are given, the checkpoint read must be the generation the
    records are keyed on (its metadata target_generation_id)."""
    generation = _seed_one_page_and_publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        evaluation = IrPostPublishCheck(_ir_spec()).evaluate(
            conn,
            target_generation_id=970_303,
            checkout_generation_id=generation,
            catalog_version_id=1,
            deployment_name="default",
        )
    assert evaluation.result == "blocked_error"
    assert (
        evaluation.details["error"]["type"] == "PostPublishGenerationUnresolved"
    ), evaluation.details
    assert "970303" in evaluation.details["error"]["message"]


def _mirror_lock_held_by_backend(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
            "AND pid = pg_backend_pid()"
        )
        return int(cur.fetchone()[0])


def _other_session_can_take_mirror_lock(dsn: str) -> bool:
    """One non-blocking attempt from a fresh session; releases on success."""
    from okg.substrate.consistency.chronos_sql_mirror import (
        MIRROR_LOCK_KEY,
        MIRROR_LOCK_NAMESPACE,
    )

    with psycopg.connect(dsn, autocommit=True) as other:
        got = other.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s), hashtext(%s))",
            (MIRROR_LOCK_NAMESPACE, MIRROR_LOCK_KEY),
        ).fetchone()[0]
        if got:
            other.execute(
                "SELECT pg_advisory_unlock(hashtext(%s), hashtext(%s))",
                (MIRROR_LOCK_NAMESPACE, MIRROR_LOCK_KEY),
            )
        return bool(got)


@pytest.mark.postgres
def test_mirror_reads_refuse_wrong_generation_and_report_refresh_failure(
    clean_runtime: str,
    tmp_path,
) -> None:
    """The copy is stamped with the checkpoint it holds. A wrong stamp is
    refused, a failed refresh is reported inside the mirror-reading check
    only, other checks are untouched, and the lock is released afterwards."""
    from okg.substrate.consistency.chronos_sql_mirror import (
        ChronosSqlMirrorReads,
        assert_chronos_sql_mirror_generation,
        chronos_sql_mirror_generation,
        refresh_chronos_sql_mirror,
    )
    from okg.substrate.errors import ChronosSqlMirrorMismatch

    first = _seed_one_page_and_publish(clean_runtime)
    second = _seed_one_page_and_publish(clean_runtime)
    sql_check = _mirror_sql_check(tmp_path)
    ir_check = IrPostPublishCheck(_ir_spec())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        held = refresh_chronos_sql_mirror(
            conn, store=store, generation_id=first, deployment_name="default"
        )
        assert held == first
        assert chronos_sql_mirror_generation(conn) == first
        with pytest.raises(ChronosSqlMirrorMismatch) as exc_info:
            assert_chronos_sql_mirror_generation(conn, second)
        assert exc_info.value.data == {
            "expected_checkpoint_id": second,
            "held_checkpoint_id": first,
            "reason": "stamp_mismatch",
        }

        mirror = ChronosSqlMirrorReads(
            conn,
            [ir_check, sql_check],
            store=store,
            generation_id="generation:does-not-exist",
            deployment_name="default",
        )
        with mirror.reading(ir_check):  # does not read the mirror: untouched
            pass
        with pytest.raises(ChronosSqlMirrorMismatch) as exc_info:
            with mirror.reading(sql_check):
                pytest.fail("a mirror reader ran without a usable copy")
        assert exc_info.value.data["reason"] == "generation_unresolved"
        # The copy and its stamp are untouched, and no lock is left behind.
        assert chronos_sql_mirror_generation(conn) == first
        assert _mirror_lock_held_by_backend(conn) == 0

        # sync_chronos_graph off: no refresh, but a copy of another
        # generation is still refused.
        no_refresh = ChronosSqlMirrorReads(
            conn,
            [sql_check],
            store=store,
            generation_id=second,
            deployment_name="default",
            refresh=False,
        )
        with pytest.raises(ChronosSqlMirrorMismatch) as exc_info:
            with no_refresh.reading(sql_check):
                pytest.fail("read a copy of another generation")
        assert exc_info.value.data["reason"] == "stamp_mismatch"
        assert chronos_sql_mirror_generation(conn) == first
        assert _mirror_lock_held_by_backend(conn) == 0


@pytest.mark.postgres
def test_mirror_read_refuses_a_copy_rewritten_behind_its_back(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the copy ends up holding another generation after the refresh (a
    writer that ignored the lock), the stamp check at the read refuses it."""
    import okg.substrate.consistency.chronos_sql_mirror as mirror_mod
    from okg.substrate.errors import ChronosSqlMirrorMismatch

    first = _seed_one_page_and_publish(clean_runtime)
    second = _seed_one_page_and_publish(clean_runtime)
    sql_check = _mirror_sql_check(tmp_path)
    real_refresh = mirror_mod.refresh_chronos_sql_mirror

    def _refresh_then_overwrite(conn, **kwargs):  # type: ignore[no-untyped-def]
        real_refresh(conn, **kwargs)
        # An unlocked writer copies the OTHER generation over it.
        return real_refresh(conn, **{**kwargs, "generation_id": first})

    monkeypatch.setattr(
        mirror_mod, "refresh_chronos_sql_mirror", _refresh_then_overwrite
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        mirror = mirror_mod.ChronosSqlMirrorReads(
            conn,
            [sql_check],
            store=ChronosGraphStore.from_connection(conn, ensure_schema=False),
            generation_id=second,
            deployment_name="default",
        )
        with pytest.raises(ChronosSqlMirrorMismatch) as exc_info:
            with mirror.reading(sql_check):
                pytest.fail("read a copy of another generation")
        assert exc_info.value.data == {
            "expected_checkpoint_id": second,
            "held_checkpoint_id": first,
            "reason": "stamp_mismatch",
        }
        assert _mirror_lock_held_by_backend(conn) == 0


@pytest.mark.postgres
def test_later_mirror_reader_recopies_after_another_run_moved_the_copy(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Between two mirror-reading checks of one run the lock is free, so
    another run may re-copy the mirror to its generation. The later reader
    sees the moved stamp and copies this run's generation back before
    reading."""
    from okg.substrate.consistency.chronos_sql_mirror import (
        ChronosSqlMirrorReads,
        chronos_sql_mirror_generation,
        refresh_chronos_sql_mirror,
    )

    first = _seed_one_page_and_publish(clean_runtime)
    second = _seed_one_page_and_publish(clean_runtime)
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    reader_a = _mirror_sql_check(tmp_path / "a")
    reader_b = _mirror_sql_check(tmp_path / "b")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        mirror = ChronosSqlMirrorReads(
            conn,
            [reader_a, reader_b],
            store=store,
            generation_id=second,
            deployment_name="default",
        )
        with mirror.reading(reader_a):
            assert chronos_sql_mirror_generation(conn) == second
            assert len(_mirror_page_ids(conn)) == 2
        # Another run (unlocked here, as between two readers) moves the copy.
        refresh_chronos_sql_mirror(
            conn, store=store, generation_id=first, deployment_name="default"
        )
        with mirror.reading(reader_b):
            assert chronos_sql_mirror_generation(conn) == second
            assert len(_mirror_page_ids(conn)) == 2
        assert _mirror_lock_held_by_backend(conn) == 0


@pytest.mark.postgres
def test_mirror_already_stamped_with_the_checkpoint_is_not_recopied(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A checkpoint never changes, so a copy stamped with it is reused: only
    a copy stamped with something else is re-copied."""
    import okg.substrate.consistency.chronos_sql_mirror as mirror_mod

    generation = _seed_one_page_and_publish(clean_runtime)
    sql_check = _mirror_sql_check(tmp_path)
    calls: list[str] = []
    real_refresh = mirror_mod.refresh_chronos_sql_mirror

    def _counting_refresh(conn, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(str(kwargs["generation_id"]))
        return real_refresh(conn, **kwargs)

    monkeypatch.setattr(mirror_mod, "refresh_chronos_sql_mirror", _counting_refresh)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        for _ in range(2):
            mirror = mirror_mod.ChronosSqlMirrorReads(
                conn,
                [sql_check],
                store=store,
                generation_id=generation,
                deployment_name="default",
            )
            with mirror.reading(sql_check):
                assert mirror_mod.chronos_sql_mirror_generation(conn) == generation
    # The first run copied (the copy held nothing or another generation); the
    # second found the stamp already naming this checkpoint.
    assert calls == [generation]


@pytest.mark.postgres
def test_publish_check_is_not_starved_by_a_rerun_of_back_to_back_checks(
    clean_runtime: str,
    tmp_path,
) -> None:
    """Waiters are served in order. A rerun runs ten slow mirror-reading
    checks back to back, freeing the lock only for an instant between them;
    a check that starts waiting meanwhile gets the lock after the rerun's
    current check, not after the whole rerun (or never)."""
    import threading
    import time as _time

    from okg.substrate.consistency.chronos_sql_mirror import (
        MIRROR_LOCK_KEY,
        ChronosSqlMirrorReads,
        chronos_sql_mirror_generation,
    )

    first = _seed_one_page_and_publish(clean_runtime)
    second = _seed_one_page_and_publish(clean_runtime)

    class _SlowReader:
        def __init__(self, inner):  # type: ignore[no-untyped-def]
            self.inner = inner
            self.spec = inner.spec
            self.name = inner.name
            self.blocks_publish = False

        def evaluate(self, conn, **kwargs):  # type: ignore[no-untyped-def]
            _time.sleep(0.8)
            return self.inner.evaluate(conn, **kwargs)

    slow_checks = []
    for index in range(10):
        check_dir = tmp_path / f"slow-{index}"
        check_dir.mkdir()
        slow_checks.append(_SlowReader(_mirror_sql_check(check_dir)))
    rerun_state: dict[str, object] = {}

    def _rerun() -> None:
        try:
            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                rerun_state["report"] = run_post_publish_consistency_checks(
                    conn, slow_checks, deployment_name="default", generation_id=first
                )
        except BaseException as exc:  # noqa: BLE001 - surfaced by the assertions
            rerun_state["error"] = exc
        rerun_state["finished_at"] = _time.monotonic()

    rerun_thread = threading.Thread(target=_rerun, daemon=True)
    rerun_thread.start()
    deadline = _time.monotonic() + 60
    with psycopg.connect(clean_runtime, autocommit=True) as watcher:
        while True:  # wait until the rerun holds the mirror lock
            held = watcher.execute(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
                "AND granted AND objid = hashtext(%s)::oid",
                (MIRROR_LOCK_KEY,),
            ).fetchone()[0]
            if held:
                break
            assert _time.monotonic() < deadline, "the rerun never took the lock"
            _time.sleep(0.05)

    publish_check_dir = tmp_path / "publish"
    publish_check_dir.mkdir()
    publish_check = _mirror_sql_check(publish_check_dir)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        mirror = ChronosSqlMirrorReads(
            conn,
            [publish_check],
            store=ChronosGraphStore.from_connection(conn, ensure_schema=False),
            generation_id=second,
            deployment_name="default",
            lock_wait_seconds=5.0,
        )
        started = _time.monotonic()
        with mirror.reading(publish_check):
            waited = _time.monotonic() - started
            assert chronos_sql_mirror_generation(conn) == second
            rerun_still_running = rerun_thread.is_alive()
    rerun_thread.join(timeout=120)
    assert not rerun_thread.is_alive()
    assert "error" not in rerun_state, rerun_state.get("error")
    # Served after at most the rerun's current check (0.8 s plus a copy),
    # while the rerun still had checks left to run.
    assert waited < 5.0, waited
    assert rerun_still_running is True
    report = rerun_state["report"]
    assert all(
        summary["result"] == "evaluated"
        for summary in report["checks"]  # type: ignore[index]
    ), report


@pytest.mark.postgres
def test_mirror_lock_is_released_after_a_failed_transaction(
    clean_runtime: str,
    tmp_path,
) -> None:
    """A check that leaves a non-autocommit connection in a failed
    transaction still releases the mirror lock: the transaction is rolled
    back first, so the unlock can run."""
    from okg.substrate.consistency.chronos_sql_mirror import (
        ChronosSqlMirrorReads,
    )

    generation = _seed_one_page_and_publish(clean_runtime)
    sql_check = _mirror_sql_check(tmp_path)
    with psycopg.connect(clean_runtime) as conn:  # not autocommit
        mirror = ChronosSqlMirrorReads(
            conn,
            [sql_check],
            store=ChronosGraphStore.from_connection(conn, ensure_schema=False),
            generation_id=generation,
            deployment_name="default",
        )
        with mirror.reading(sql_check):
            assert _other_session_can_take_mirror_lock(clean_runtime) is False
            with pytest.raises(psycopg.errors.DivisionByZero):
                conn.execute("SELECT 1 / 0")
            assert (
                conn.info.transaction_status
                == psycopg.pq.TransactionStatus.INERROR
            )
        assert _mirror_lock_held_by_backend(conn) == 0
        assert _other_session_can_take_mirror_lock(clean_runtime) is True


class _CancelRestoreAfterLock:
    """Connection proxy whose cursors raise ``QueryCanceled`` on the
    statement that restores the timeouts after the lock is granted."""

    def __init__(self, conn) -> None:
        self._conn = conn
        self.lock_granted = False

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def cursor(self, *args, **kwargs):
        proxy = self
        real = self._conn.cursor(*args, **kwargs)

        class _Cursor:
            def __enter__(self):
                real.__enter__()
                return self

            def __exit__(self, *exc):
                return real.__exit__(*exc)

            def __getattr__(self, name):
                return getattr(real, name)

            def execute(self, query, params=None):
                if proxy.lock_granted and "set_config" in query:
                    raise psycopg.errors.QueryCanceled("injected cancel")
                result = real.execute(query, params)
                if "pg_advisory_lock(" in query:
                    proxy.lock_granted = True
                return result

        return _Cursor()


@pytest.mark.postgres
def test_mirror_lock_is_released_when_acquire_fails_after_the_grant(
    clean_runtime: str,
) -> None:
    """A failure after ``pg_advisory_lock`` is granted (here a cancel on the
    timeout restore) releases the session lock and raises the original
    error; a rollback alone would leave the lock held."""
    from okg.substrate.consistency.chronos_sql_mirror import _acquire_lock

    with psycopg.connect(clean_runtime) as conn:  # not autocommit
        proxy = _CancelRestoreAfterLock(conn)
        with pytest.raises(psycopg.errors.QueryCanceled, match="injected"):
            _acquire_lock(proxy, wait_seconds=5.0, expected="unused")
        assert proxy.lock_granted is True
        assert _mirror_lock_held_by_backend(conn) == 0
        assert _other_session_can_take_mirror_lock(clean_runtime) is True


@pytest.mark.postgres
def test_publish_reports_mirror_lock_wait_per_check_and_still_publishes(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Another session holds the mirror lock for the whole publish. The
    publish still returns published; the SQL check that reads the mirror
    reports the lock wait as its error, and the IR check runs normally."""
    import okg.substrate.consistency.chronos_sql_mirror as mirror_mod
    from okg.substrate.consistency.chronos_sql_mirror import (
        MIRROR_LOCK_KEY,
        MIRROR_LOCK_NAMESPACE,
    )
    from tests.pytest.catalog_fixture_builders import seed_catalog

    monkeypatch.setattr(mirror_mod, "MIRROR_LOCK_WAIT_SECONDS", 2.0)
    sql_check = _mirror_sql_check(tmp_path)
    ir_check = IrPostPublishCheck(_ir_spec())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"], narrowings=[("page", "links_to", "page")])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_page_fact(f"page:{uuid.uuid4().hex[:8]}:locked")],
            source="default",
            strict_endpoints=True,
        )
    with psycopg.connect(clean_runtime, autocommit=True) as holder:
        holder.execute(
            "SELECT pg_advisory_lock(hashtext(%s), hashtext(%s))",
            (MIRROR_LOCK_NAMESPACE, MIRROR_LOCK_KEY),
        )
        out = Publisher(
            clean_runtime,
            "default",
            post_publish_consistency_checks=[ir_check, sql_check],
            catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
            hard_statement_timeout_ms=3000,
        ).cycle()
        holder.execute(
            "SELECT pg_advisory_unlock(hashtext(%s), hashtext(%s))",
            (MIRROR_LOCK_NAMESPACE, MIRROR_LOCK_KEY),
        )

    assert out["status"] == "published", out
    by_name = {s["gate_name"]: s for s in out["post_publish_consistency_checks"]}
    assert by_name[ir_check.name]["result"] == "evaluated", by_name
    sql_summary = by_name[sql_check.name]
    assert sql_summary["result"] == "blocked_error", sql_summary
    error = sql_summary["details"]["error"]
    assert error["type"] == "ChronosSqlMirrorMismatch", error
    assert error["code"] == "chronos_sql_mirror_mismatch", error
    assert error["data"]["reason"] == "lock_wait_timeout", error
    assert "held the Chronos SQL mirror" in error["message"]
    # The refusal is durable: a failed run and a blocked_error gate row.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error->>'type' FROM okg.derived_artifact_runs "
                "WHERE run_id = %s",
                (sql_summary["run_id"],),
            )
            assert cur.fetchone() == ("failed", "ChronosSqlMirrorMismatch")
            cur.execute(
                "SELECT result FROM okg.consistency_gate_results WHERE run_id = %s",
                (sql_summary["run_id"],),
            )
            assert cur.fetchone() == ("blocked_error",)


@pytest.mark.postgres
def test_rerun_cannot_swap_the_mirror_under_a_publish_check(
    clean_runtime: str,
    tmp_path,
) -> None:
    """While a publish's SQL check reads the mirror, a rerun of an older
    generation waits for it. A rerun that finished just before the check
    (leaving the copy on the older generation) is re-copied over. Either way
    the publish's SQL check judges its own generation (two pages), and the
    rerun then judges the older one (one page)."""
    import threading
    import time as _time

    from okg.substrate.consistency.chronos_sql_mirror import (
        chronos_sql_mirror_generation,
    )

    real_sql_check = _mirror_sql_check(tmp_path)
    first = _seed_one_page_and_publish(clean_runtime)
    rerun_result: dict[str, object] = {}
    observed: dict[str, object] = {}

    def _rerun() -> None:
        try:
            rerun_dir = tmp_path / f"rerun-{uuid.uuid4().hex[:8]}"
            rerun_dir.mkdir()
            with psycopg.connect(clean_runtime, autocommit=True) as conn:
                rerun_result["report"] = run_post_publish_consistency_checks(
                    conn,
                    [_mirror_sql_check(rerun_dir)],
                    deployment_name="default",
                    generation_id=first,
                )
        except BaseException as exc:  # noqa: BLE001 - surfaced by the assertions
            rerun_result["error"] = exc

    # 1. A rerun of the older generation completes BEFORE the publish's SQL
    #    check, leaving the copy on the older generation.
    class _RerunFirstCheck:
        name = "rerun_before_sql_check"

        def evaluate(self, conn, **kwargs):  # type: ignore[no-untyped-def]
            _rerun()
            with psycopg.connect(clean_runtime, autocommit=True) as other:
                observed["copy_before_sql_check"] = (
                    chronos_sql_mirror_generation(other)
                )
            raise RuntimeError("interleave hook (not a real check)")

    # 2. A second rerun starts WHILE the publish's SQL check holds the copy.
    rerun_thread = threading.Thread(target=_rerun, daemon=True)

    class _ReadWhileRerunStarts:
        """The real SQL check, which starts a rerun mid-read and waits long
        enough for an unguarded rerun to swap the copy before reading."""

        def __init__(self, inner):  # type: ignore[no-untyped-def]
            self.inner = inner
            self.spec = inner.spec
            self.name = inner.name
            self.blocks_publish = False

        def evaluate(self, conn, **kwargs):  # type: ignore[no-untyped-def]
            rerun_thread.start()
            _time.sleep(3.0)
            observed["rerun_blocked"] = rerun_thread.is_alive()
            return self.inner.evaluate(conn, **kwargs)

    from tests.pytest.catalog_fixture_builders import seed_catalog

    second_page = f"page:{uuid.uuid4().hex[:8]}:second"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page"], narrowings=[("page", "links_to", "page")])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_page_fact(second_page)], source="default", strict_endpoints=True
        )
    sql_check = _ReadWhileRerunStarts(real_sql_check)
    out = Publisher(
        clean_runtime,
        "default",
        post_publish_consistency_checks=[_RerunFirstCheck(), sql_check],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    rerun_thread.join(timeout=300)
    assert not rerun_thread.is_alive(), "rerun never acquired the mirror lock"
    assert "error" not in rerun_result, rerun_result.get("error")

    assert out["status"] == "published", out
    assert observed["copy_before_sql_check"] == first
    assert observed["rerun_blocked"] is True
    by_name = {s["gate_name"]: s for s in out["post_publish_consistency_checks"]}
    publish_sql = by_name[sql_check.name]
    assert publish_sql["result"] == "evaluated", publish_sql
    # Two pages: the publish judged its own generation, not the rerun's.
    assert publish_sql["open_observation_count"] == 2, publish_sql

    (rerun_sql,) = rerun_result["report"]["checks"]  # type: ignore[index]
    assert rerun_sql["result"] == "evaluated", rerun_sql
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert chronos_sql_mirror_generation(conn) == first
        assert len(_mirror_page_ids(conn)) == 1


# --------------------------------------------------------------------------- #
# Site 3 (sql_gate): candidate-stage drop after a gate error                  #
# --------------------------------------------------------------------------- #


def _sql_gate(tmp_path):
    import yaml

    gate_doc = {
        "name": "loud_fallback_sql_gate",
        "type": "deterministic_sql",
        "check_id": "test.loud_fallback.sql",
        "predicate_family": "numeric_claim_conflict",
        "confidence_type": "deterministic_boolean",
        "candidate_sql": """
            SELECT
              'bucket:x'::text AS input_scope,
              'block:x'::text AS blocking_rule_hash,
              jsonb_build_object('left', 'a', 'right', 'b') AS participant_roles,
              'p'::text AS predicate_key,
              'stable_source_anchor_v1'::text AS generation_policy,
              true AS emit_observation,
              'p_norm'::text AS normalized_predicate,
              jsonb_build_object('left', 1, 'right', 2) AS normalized_values,
              'high'::text AS severity,
              1.0::numeric AS confidence,
              jsonb_build_object('spans', jsonb_build_array('s')) AS evidence
        """,
        "coverage_sql": """
            SELECT
              ('cov:' || %(target_generation_id)s::text) AS universe_key,
              'exact_postgres'::text AS coverage_basis,
              true AS coverage_complete,
              1::bigint AS eligible_input_count,
              1::bigint AS evaluator_invocation_count,
              1::bigint AS all_pairs_upper_bound
        """,
        "timeout_ms": 3000,
    }
    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "test", "consistency_gates": [gate_doc]})
    )
    return load_consistency_gates(dep)[0]


@pytest.mark.postgres
def test_candidate_stage_drop_fallback_logs_exception_class_at_warning(
    clean_runtime: str,
    tmp_path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The best-effort candidate-stage drop after a gate error keeps its
    behavior (swallow + keep failing the run) but now logs the DROP failure's
    exception CLASS at WARNING (was DEBUG with only exc_info)."""
    gate = _sql_gate(tmp_path)

    # Force the run to fail early (distinct class), then force the cleanup DROP
    # to fail with a DIFFERENT class so the assertion proves the WARNING names
    # the DROP failure specifically, not the run failure.
    def _raise_resolve(*args, **kwargs):
        raise ValueError("injected run failure before staging")

    def _raise_drop(*args, **kwargs):
        raise RuntimeError("injected candidate-stage drop failure")

    monkeypatch.setattr(gate, "_resolve_relation_inputs", _raise_resolve)
    monkeypatch.setattr(gate, "_drop_candidate_stage", _raise_drop)

    with psycopg.connect(clean_runtime) as conn:
        with caplog.at_level(logging.WARNING, logger=SQL_GATE_LOGGER):
            evaluation = gate.evaluate(
                conn,
                target_generation_id=970_101,
                catalog_version_id=1,
                deployment_name="test",
            )

    # Decision unchanged: the run still fails (blocked_error) and records the
    # ORIGINAL run failure (ValueError), not the cleanup failure.
    assert evaluation.result == "blocked_error"
    assert evaluation.details["error"]["type"] == "ValueError"

    recs = [
        r
        for r in caplog.records
        if r.name == SQL_GATE_LOGGER and r.levelno == logging.WARNING
    ]
    assert recs, "expected a WARNING record from the candidate-stage drop fallback"
    msg = recs[-1].getMessage()
    # The DROP failure's class is in the MESSAGE (assertable at a normal level).
    assert "RuntimeError" in msg, msg
    assert "candidate stage" in msg, msg


# --------------------------------------------------------------------------- #
# Site 4 (sql_gate): post-publish check crash — read back real rows           #
# --------------------------------------------------------------------------- #


def _definition() -> ConsistencyCheckDefinition:
    return ConsistencyCheckDefinition(
        check_id=f"test.loud_fallback.persisted.{uuid4().hex[:8]}",
        deployment_name="test",
        predicate_family="numeric_claim_conflict",
        candidate_rule_hash=stable_hash({"blocking": "subject+unit"}),
        evaluator_ref="okg.evaluator.sql.numeric_conflict.v1",
        evaluator_hash=stable_hash({"evaluator": "numeric_conflict_v1"}),
        confidence_type="deterministic_boolean",
        compute_hash=stable_hash({"candidate_sql": "claim_pairs"}),
        output_schema_hash=stable_hash({"outputs": "observations"}),
        renderer_hash=stable_hash({"renderer": "observation-json"}),
        dirty_rule_hash=stable_hash({"scope": "claim_bucket"}),
        coverage_contract={"universe": "claim_chunks", "coverage": "exact_sql"},
    )


def _candidate() -> ConsistencyCandidate:
    return ConsistencyCandidate(
        input_scope="bucket:x",
        blocking_rule_hash=stable_hash({"block": "x"}),
        participant_roles={"left": "claim:x:a", "right": "claim:x:b"},
        predicate_key="numeric_equal_under_condition",
        generation_policy="stable_source_anchor_v1",
    )


def _observation_event(
    definition: ConsistencyCheckDefinition, candidate_id: str
) -> ObservationEvent:
    participants = {"left": "claim:x:a", "right": "claim:x:b"}
    observation_id = observation_id_for(
        definition,
        candidate_id=candidate_id,
        input_scope="bucket:x",
        participant_roles=participants,
        normalized_predicate="x_equal",
        normalized_values={"left": 1, "right": 2},
        source_revision_policy="stable_source_anchor_v1",
    )
    return ObservationEvent(
        observation_id=observation_id,
        candidate_id=candidate_id,
        input_scope="bucket:x",
        predicate_key="numeric_equal_under_condition",
        normalized_predicate="x_equal",
        normalized_values={"left": 1, "right": 2},
        participant_roles=participants,
        finding_event="observed",
        severity="high",
        confidence=1,
        evidence={"spans": ["s"]},
    )


def _seed_published_generation(dsn: str) -> tuple[str, int]:
    """A published Chronos generation (no full publish cycle needed)."""
    deployment_name = f"test-loud-fallbacks-{uuid4().hex[:8]}"
    target_generation_id = 990_001
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact("doc:lf:one", "document", {"text": "one"}, {"id": "one"}, {"rev": 1}),
                NodeFact("doc:lf:two", "document", {"text": "two"}, {"id": "two"}, {"rev": 1}),
                EdgeFact(
                    "doc:lf:one",
                    "doc:lf:two",
                    "references",
                    source_record_id={"id": "one-two"},
                    source_revision={"rev": 1},
                ),
            ],
            source="test.loud_fallbacks",
            run_id="run:test-loud-fallbacks",
        )
        store.checkpoint_generation(
            None,
            deployment_name=deployment_name,
            catalog_version_id=17,
            status="published",
            metadata={
                "target_generation_id": target_generation_id,
                "publisher": "test_loud_fallbacks",
                "node_count": 2,
                "edge_count": 1,
                "invariant_summary": {
                    "substrate_integrity": {"checks": {}},
                    "total_duration_ms": 0,
                    "total_budget_exceeded": False,
                },
                "excluded_sources": [],
                "publish_wallclock_seconds": 1.5,
            },
        )
    return deployment_name, target_generation_id


class _RaisingCheck:
    """A post-publish check whose evaluate always raises (mid-flight crash)."""

    def __init__(self, name: str) -> None:
        self.name = name

    def evaluate(
        self,
        conn,
        *,
        target_generation_id,
        catalog_version_id,
        deployment_name,
        scope_subject_ids=None,
    ):
        raise RuntimeError("mid-evaluate crash")


class _OrphanThenRaiseCheck:
    """A post-publish check that starts a run at its DURABLE definition id
    (leaving it non-terminal) then crashes — exercises the live path's runs-block
    orphan detection, which must key off ``spec.check_id``, not the display
    ``spec.name``, or it wrong-zeros the orphan gauge."""

    def __init__(self, spec: IrConsistencyCheckSpec) -> None:
        self.spec = spec
        self.name = spec.name  # display gate_name; deliberately != spec.check_id

    def evaluate(
        self,
        conn,
        *,
        target_generation_id,
        catalog_version_id,
        deployment_name,
        **kwargs,
    ):
        start_consistency_run(
            conn,
            self.spec.definition(deployment_name),
            target_generation_id=int(target_generation_id),
            trigger_kind="post_publish",
        )
        raise RuntimeError("orphaned then crashed")


def _persist_real_gate_row(
    dsn: str,
    *,
    gate_name: str,
    target_generation_id: int,
) -> ConsistencyCheckDefinition:
    """Persist a REAL consistency_gate_results row (evaluated, one open finding)
    for ``(gate_name, target_generation_id)`` — the partial measurement a check
    would have left before crashing."""
    definition = _definition()
    with psycopg.connect(dsn, autocommit=True) as conn:
        run_id: UUID = start_consistency_run(
            conn, definition, target_generation_id=target_generation_id
        )
        candidate_id = record_candidate(
            conn,
            definition,
            target_generation_id=target_generation_id,
            run_id=run_id,
            candidate=_candidate(),
        )
        record_candidate_coverage(
            conn,
            definition,
            target_generation_id=target_generation_id,
            run_id=run_id,
            report=CandidateCoverageReport(
                universe_key=f"claim_chunks:{target_generation_id}",
                coverage_basis="exact_postgres",
                coverage_complete=True,
                eligible_input_count=2,
                candidate_count=1,
                candidate_count_by_block={"x": 1},
                evaluator_invocation_count=1,
            ),
        )
        append_observation_event(
            conn,
            definition,
            target_generation_id=target_generation_id,
            run_id=run_id,
            event=_observation_event(definition, candidate_id),
        )
        evaluation = evaluate_consistency_publish_gate(
            conn,
            definition,
            target_generation_id=target_generation_id,
            run_id=run_id,
            gate_name=gate_name,
            blocks_publish=False,
        )
    # Sanity: the row we persisted is a real "evaluated" measurement with an
    # open finding, so a recovered summary showing zeros would be a regression.
    assert evaluation.result == "evaluated"
    assert evaluation.open_observation_count == 1
    return definition


@pytest.mark.postgres
def test_post_publish_crash_does_not_misattribute_a_prior_runs_row(
    clean_runtime: str,
) -> None:
    """Anti-misattribution guard (adversarial-review regression): a DIFFERENT
    run's persisted gate row at the same generation must NEVER be surfaced as
    this crashed run's measurement. The real check types persist their own
    blocked_error row atomically and only propagate to this handler when that
    persist itself FAILED — so on propagation this run has no committed row, and
    reading back the latest row for (gate_name, generation) would surface a
    foreign run's measurement (e.g. the publish-time row a consistency-rerun
    targets), labeled as recovered — worse than zeros. The crash path must
    report the honest explicit error state instead."""
    deployment_name, target_generation_id = _seed_published_generation(
        clean_runtime
    )
    gate_name = "loud_fallback_prior_row_gate"
    # A DIFFERENT run persists a real "evaluated" / open=1 row at this generation
    # (its evaluation.result=="evaluated", open==1 is asserted in the helper).
    _persist_real_gate_row(
        clean_runtime,
        gate_name=gate_name,
        target_generation_id=target_generation_id,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        report = run_post_publish_consistency_checks(
            conn,
            [_RaisingCheck(gate_name)],
            deployment_name=deployment_name,
            generation_id=target_generation_id,
        )

    (summary,) = report["checks"]
    # The crashed run measured NOTHING — the prior run's evaluated/open=1 must
    # not leak in; the summary is the honest explicit error state.
    assert summary["result"] == "blocked_error"
    assert summary["result"] != "evaluated"
    assert summary["open_observation_count"] == 0
    assert summary["newly_opened_count"] == 0
    assert "recovered_from_persisted_row" not in summary
    assert summary["reason"] == "post_publish_check_exception"
    assert summary["details"]["error_type"] == "RuntimeError"


@pytest.mark.postgres
def test_post_publish_check_crash_reports_explicit_error_state(
    clean_runtime: str,
) -> None:
    """A crashed post-publish check yields the explicit error state — result
    'blocked_error' (distinguishable from an evaluated zero-findings run) with
    the exception class recorded — never a synthesized measurement."""
    deployment_name, target_generation_id = _seed_published_generation(
        clean_runtime
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        report = run_post_publish_consistency_checks(
            conn,
            [_RaisingCheck("never_ran_gate")],
            deployment_name=deployment_name,
            generation_id=target_generation_id,
        )

    (summary,) = report["checks"]
    assert summary["result"] == "blocked_error"
    assert summary["result"] != "evaluated"
    assert summary["reason"] == "post_publish_check_exception"
    assert summary["open_observation_count"] == 0
    assert summary["details"]["error_type"] == "RuntimeError"
    assert summary["details"]["error"] == "mid-evaluate crash"
    # Zeros here are the honest "crashed before measuring" state, not a claim
    # that the check ran and found nothing.
    assert summary["open_observation_count"] == 0
    assert summary["newly_opened_count"] == 0
    assert summary["coverage_complete"] is None
    assert "recovered_gate_result" not in summary["details"]


@pytest.mark.postgres
def test_publisher_cycle_post_publish_check_crash_reports_error_state(
    clean_runtime: str,
) -> None:
    """The LIVE publish path — ``Publisher.cycle`` -> ``_cycle_chronos_native``'s
    inline post-publish loop — reports the honest explicit error state when a
    post-publish check crashes: ``result='blocked_error'``, never a synthesized
    zero-count measurement and never a read-back "recovered" summary.

    This is the publish-path twin of the rerun-path crash guards above. Both now
    build the crash summary through ``build_post_publish_crash_summary``, so the
    live inline loop can no longer silently drift from the hardened contract —
    its previous copy diverged (it even swallowed the exception with no log
    line) precisely because it was a second, independent implementation.
    """
    pub = Publisher(
        clean_runtime,
        "test-pub",
        post_publish_consistency_checks=[
            _RaisingCheck("live_publish_crash_gate"),
        ],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"),
    )
    result = pub.cycle()

    # The generation still publishes — post-publish checks are non-blocking.
    assert result["status"] == "published"
    assert result["blocked_reason"] is None

    (summary,) = result["post_publish_consistency_checks"]
    # Honest error state, NOT a zero-count success: consumers key off `result`.
    assert summary["result"] == "blocked_error"
    assert summary["result"] != "evaluated"
    assert summary["gate_name"] == "live_publish_crash_gate"
    assert summary["reason"] == "post_publish_check_exception"
    assert summary["details"]["error_type"] == "RuntimeError"
    assert summary["details"]["error"] == "mid-evaluate crash"
    # Zeros are the "crashed before measuring" sentinel, not a measurement.
    assert summary["open_observation_count"] == 0
    assert summary["newly_opened_count"] == 0
    assert summary["resolved_count"] == 0
    assert summary["coverage_complete"] is None
    # The rejected read-back must never reappear on the live path either.
    assert "recovered_from_persisted_row" not in summary
    assert "recovered_gate_result" not in summary["details"]


@pytest.mark.postgres
def test_publisher_and_rerun_crash_summaries_do_not_drift(
    clean_runtime: str,
) -> None:
    """Cross-path DRIFT GUARD: the same crashing check, run through BOTH
    post-publish entry points — the publisher's live ``.cycle()`` inline loop
    (``_cycle_chronos_native``) and the standalone rerun function
    (``run_post_publish_consistency_checks``, used by ``okg consistency-rerun``)
    — must produce the SAME crash summary. Both route through
    ``build_post_publish_crash_summary``; this test goes red if either path is
    ever changed/hardened without the other. That one-sided drift is exactly
    what let this task's premise go stale (the rerun path was hardened while the
    live inline copy silently lagged), so the invariant is now enforced, not
    hoped for.

    ``target_generation_id`` (path-specific id labeling) and ``elapsed_ms``
    (wall-clock) are excluded, as is the ``runs`` block's own embedded
    ``target_generation_id``. The rest of the ``runs`` multiplicity block (#603)
    is now attached by BOTH paths (via ``attach_post_publish_run_multiplicity``)
    and IS compared here.
    """
    gate = "drift_guard_crash_gate"

    # Path B — rerun path, against an independently-published generation
    # (seeded exactly as the sibling rerun-path tests do).
    deployment_name, target_generation_id = _seed_published_generation(
        clean_runtime
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        report = run_post_publish_consistency_checks(
            conn,
            [_RaisingCheck(gate)],
            deployment_name=deployment_name,
            generation_id=target_generation_id,
        )
    (rerun_summary,) = report["checks"]

    # Path A — live publish path (Publisher.cycle -> _cycle_chronos_native).
    live = Publisher(
        clean_runtime,
        "test-pub",
        post_publish_consistency_checks=[_RaisingCheck(gate)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"),
    ).cycle()
    assert live["status"] == "published"
    (live_summary,) = live["post_publish_consistency_checks"]

    def _core(summary: dict[str, object]) -> dict[str, object]:
        core = dict(summary)
        core.pop("target_generation_id", None)  # path-specific id labeling
        core.pop("elapsed_ms", None)  # wall-clock
        runs = core.get("runs")
        if isinstance(runs, dict):
            runs = dict(runs)
            runs.pop("target_generation_id", None)  # generation-specific
            core["runs"] = runs
        return core

    # Same crashing check, two entry points → identical honest crash summary.
    assert _core(live_summary) == _core(rerun_summary)
    assert live_summary["result"] == rerun_summary["result"] == "blocked_error"


@pytest.mark.postgres
def test_publisher_cycle_post_publish_summary_carries_runs_block(
    clean_runtime: str,
) -> None:
    """The live publish path (Publisher.cycle -> _cycle_chronos_native) attaches
    the operator-visible run-multiplicity block (#603) to each post-publish
    summary — parity with the rerun path — keyed off the DURABLE definition id.

    A check that starts a run then crashes leaves a 'running' orphan; the live
    summary's ``runs`` block must surface it (``runs_non_terminal >= 1``). Keying
    off the display gate_name (``spec.name`` != ``spec.check_id``) would
    wrong-zero the orphan gauge — so this proves the live loop passes the check
    (with its durable ``spec``), not just the summary, to the shared builder.
    """
    spec = _ir_spec()
    assert spec.name != spec.check_id  # the whole point of the durable-id keying

    result = Publisher(
        clean_runtime,
        "test-pub",
        post_publish_consistency_checks=[_OrphanThenRaiseCheck(spec)],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"),
    ).cycle()
    assert result["status"] == "published"

    (summary,) = result["post_publish_consistency_checks"]
    assert summary["result"] == "blocked_error"  # crashed after starting a run
    runs = summary["runs"]
    # Keyed off the DURABLE id — sees the orphan; the gate_name would give 0.
    assert runs["definition_id"] == spec.check_id
    assert runs["definition_id"] != spec.name
    assert runs["runs_total"] >= 1, runs
    assert runs["runs_non_terminal"] >= 1, runs
    assert runs["by_status"].get("running", 0) >= 1, runs


@pytest.mark.postgres
def test_post_publish_per_check_runner_is_single_source(
    clean_runtime: str,
) -> None:
    """Both post-publish entry points delegate to ONE per-check runner
    (``run_one_post_publish_check``): evaluate (via a supplied closure) ->
    summarize -> honest crash summary -> runs block. Driven directly here on the
    crash path — a closure that raises yields ``result='blocked_error'`` WITH the
    runs block attached — the same body the publisher's live loop and the rerun
    function now share (behavioral cross-path equivalence is guarded by
    ``test_publisher_and_rerun_crash_summaries_do_not_drift``)."""
    deployment_name, gen = _seed_published_generation(clean_runtime)
    check = _RaisingCheck("runner_single_source_gate")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        summary = run_one_post_publish_check(
            conn,
            check,
            target_generation_id=gen,
            evaluate=lambda: check.evaluate(
                conn,
                target_generation_id=gen,
                catalog_version_id=17,
                deployment_name=deployment_name,
            ),
            logger=logging.getLogger("test.runner"),
        )

    # The evaluate closure raised -> honest crash summary from the shared builder.
    assert summary["result"] == "blocked_error"
    assert summary["gate_name"] == "runner_single_source_gate"
    assert summary["details"]["error_type"] == "RuntimeError"
    assert "recovered_from_persisted_row" not in summary
    # ...and the SAME runner attached the runs block (one body: crash + runs).
    assert summary["runs"]["definition_id"] == "runner_single_source_gate"
    assert summary["runs"]["runs_total"] == 0  # no run started
    assert summary["runs"]["runs_non_terminal"] == 0


@pytest.mark.postgres
def test_post_publish_runner_records_explicit_marker_on_runs_count_failure(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the run-multiplicity count itself fails, the runner must record a
    LOUD explicit error marker on ``summary['runs']`` — never silent zeros and
    never fatal to the (non-blocking) post-publish caller. Guards the intentional
    unification behavior (the rerun path previously PROPAGATED a count failure;
    now, via the shared runner, both paths record the marker) so a future edit
    cannot silently revert it."""
    sql_gate_mod = importlib.import_module("okg.substrate.consistency.sql_gate")

    def _boom(*_args, **_kwargs):
        raise RuntimeError("runs count boom")

    # The runner resolves attach_* as a module global, so patching it here is
    # what run_one_post_publish_check will call.
    monkeypatch.setattr(
        sql_gate_mod, "attach_post_publish_run_multiplicity", _boom
    )

    deployment_name, gen = _seed_published_generation(clean_runtime)
    check = _RaisingCheck("runs_count_failure_gate")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        summary = run_one_post_publish_check(
            conn,
            check,
            target_generation_id=gen,
            evaluate=lambda: check.evaluate(
                conn,
                target_generation_id=gen,
                catalog_version_id=17,
                deployment_name=deployment_name,
            ),
            logger=logging.getLogger("test.runner"),
        )

    # The crash summary is still produced; the runs failure is a loud marker,
    # NOT silent zeros and NOT a raise that escapes the runner.
    assert summary["result"] == "blocked_error"
    assert summary["runs"] == {
        "error_type": "RuntimeError",
        "error": "runs count boom",
    }
