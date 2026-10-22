"""No-op reuse is refused while sweepable staged source admissions exist.

close-refused-publish-redrive-gap (#828), defense-in-depth requirement: the
only reachable coexistence of reuse eligibility with a staged accepted
source envelope is a feedback-crash leftover (feedback application is
best-effort in the worker), and re-serving the previous generation in that
state would strand the envelope's facts while the status-keyed sweep
false-stamps it published. The guard lives inside the single constructor
both reuse returns flow through, so no reuse path can bypass it.
"""
from __future__ import annotations

import json
import uuid

import psycopg
import pytest

from okg.substrate.admission import apply_generation_feedback
from okg.substrate.publisher import Publisher
from tests.substrate.test_publisher import (
    _insert_concept_fact,
    _seed_concept_catalog,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _insert_staged_source_admission(
    dsn: str,
    *,
    deployment: str,
    source: str,
) -> str:
    admission_id = str(uuid.uuid4())
    run_id = str(uuid.uuid4())
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.admission_envelopes
              (admission_id, deployment_name, producer_id, producer_kind,
               intent, admission_mode, status, source, run_id,
               idempotency_key)
            VALUES
              (%s::uuid, %s, %s, 'source', 'append_source_facts',
               'fast_track', 'accepted', %s, %s::uuid, %s)
            """,
            (
                admission_id, deployment, source, source, run_id,
                f"staged:{admission_id}",
            ),
        )
        cur.execute(
            """
            INSERT INTO okg.admission_record_refs
              (admission_id, ref_key, ref_kind, source, run_id, payload)
            VALUES
              (%s::uuid, %s, 'payload', %s, %s::uuid, %s::jsonb)
            """,
            (
                admission_id,
                f"chronos_graph_run:{source}:{run_id}",
                source,
                run_id,
                json.dumps({
                    "kind": "chronos_graph_run",
                    "source": source,
                    "run_id": run_id,
                }),
            ),
        )
    return admission_id


def _envelope_status(dsn: str, admission_id: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (admission_id,),
        )
        return cur.fetchone()[0]


def test_feedback_crash_leftover_refuses_reuse(clean_runtime: str) -> None:
    """The discriminating unit case: a zero-delta cycle that WOULD reuse
    must instead checkpoint for real while an un-swept accepted source
    envelope with refs exists, recording the named invalidation reason;
    once feedback sweeps the envelope, reuse resumes (no livelock)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:first")

    pub = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    first = pub.cycle()
    assert first["status"] == "published"

    # Control: with nothing staged, the zero-delta second cycle reuses.
    control = pub.cycle()
    assert control.get("reused_generation") is True
    assert control["generation_id"] == first["generation_id"]

    # Feedback-crash leftover: an accepted source envelope with refs that
    # survived the published checkpoint (its feedback never applied).
    staged_id = _insert_staged_source_admission(
        clean_runtime, deployment="test-pub", source="test-pub.staged",
    )

    guarded = pub.cycle()
    assert guarded["status"] == "published"
    assert guarded.get("reused_generation") is not True
    assert guarded["generation_id"] != control["generation_id"]
    noop_reuse = (guarded.get("timing_details") or {}).get("noop_reuse")
    assert noop_reuse == {
        "reused": False,
        "reason": "staged_source_admissions_present",
        "owner": "admission",
    }

    # Production applies feedback after every cycle; mirror it, which
    # sweeps the leftover envelope (payload refs sweep on any published
    # generation of the deployment).
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test-pub",
            generation_id=guarded["generation_id"],
        )
    assert _envelope_status(clean_runtime, staged_id) == "published"

    # Swept set is empty again: reuse resumes -- one real cycle cleared
    # the condition, no real-publish-forever livelock.
    healed = pub.cycle()
    assert healed.get("reused_generation") is True
    assert healed["generation_id"] == guarded["generation_id"]


def test_staged_refused_repaired_cycle_recovers_rows(
    clean_runtime: str,
) -> None:
    """Incident regression (#828 shape): facts staged on the branch by a
    run whose publish never happened, with an accepted envelope left
    behind. The next real cycle checkpoints a generation whose live rows
    include the staged facts and the envelope is swept to published."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:first")

    pub = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    first = pub.cycle()
    assert first["status"] == "published"
    first_nodes = int(first["node_count"])

    # The staged state (#828 shape): graph rows already applied to the
    # chronos branch head by the refused run (nothing rolled them back)
    # plus the accepted envelope with refs.
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).apply_facts(
            [NodeFact("n:staged-by-refused-run", "concept", {}, {}, {})],
            source="test-pub.staged",
        )
    staged_id = _insert_staged_source_admission(
        clean_runtime, deployment="test-pub", source="test-pub.staged",
    )

    recovered = pub.cycle()
    assert recovered["status"] == "published"
    assert recovered.get("reused_generation") is not True
    assert recovered["generation_id"] != first["generation_id"]
    # Real substrate output: the staged fact is live in the recovered
    # generation (row delta, not just status).
    assert int(recovered["node_count"]) == first_nodes + 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test-pub",
            generation_id=recovered["generation_id"],
        )
    assert _envelope_status(clean_runtime, staged_id) == "published"


def test_zero_net_graph_writes_checkpoint_instead_of_reusing(
    clean_runtime: str,
) -> None:
    """#3625: rows written and retracted since the last generation net to
    zero, but they still sit in the branch head. Reusing the previous
    generation would leave its feedback refused with
    ``intervening_graph_write``, so the cycle must cut a real checkpoint,
    feedback must apply to it, and reuse resumes afterwards."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        _seed_concept_catalog(cur)
        _insert_concept_fact(cur, "n:first")

    pub = Publisher(clean_runtime, "test-pub", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-pub"))
    first = pub.cycle()
    assert first["status"] == "published"

    # A row added then retracted: two writes, zero net change.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.apply_facts(
            [NodeFact("n:undone", "concept", {}, {}, {})],
            source="test-pub.zero-net",
        )
        store.apply_facts(
            [NodeFact("n:undone", "concept", {}, {}, {}, op="R")],
            source="test-pub.zero-net",
        )

    second = pub.cycle()
    assert second["status"] == "published"
    assert second.get("reused_generation") is not True
    assert second["generation_id"] != first["generation_id"]
    assert second["node_count"] == first["node_count"]
    assert second["noop_reuse_invalidation_reason"] == "zero_net_graph_writes"

    # The check that refused this publish before the fix.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        try:
            apply_generation_feedback(
                conn,
                deployment_name="test-pub",
                generation_id=second["generation_id"],
            )
        except SubstrateError as refused:  # pragma: no cover - failure path
            raise AssertionError(refused.data) from refused

    # The checkpoint sealed the rows: the next idle cycle reuses it.
    third = pub.cycle()
    assert third.get("reused_generation") is True
    assert third["generation_id"] == second["generation_id"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test-pub",
            generation_id=third["generation_id"],
        )
