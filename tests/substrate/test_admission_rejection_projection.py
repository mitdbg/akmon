"""Rejection safety at the projection layer (strengthen-formal-models wave 2).

The add-admission-envelope design promises: "a rejected admission cannot make
its governed payload visible on the default published graph path." The
2026-07-01 formal audit (F3) flagged this as potentially unenforced for NODE
facts because the publisher's derived-fact loader admission-gates only edge
facts. These tests pin the mechanisms that actually enforce the clause, so the
admission model's rejection-safety invariant mirrors production:

1. Fast-track submits are check-before-emit inside one transaction: a
   rejecting payload leaves ZERO committed facts, so there is nothing for any
   downstream stage to project.
2. Derived producers (enricher/extractor/lens) DO reach the
   rejected-with-committed-facts state (their verdict lands after their facts
   commit), so the derived-fact loader excludes both fact kinds of a
   rejected/failed/quarantined producer run — edges via the fact-range
   allow-list, nodes via the (source, run_id) envelope deny-list added by
   this wave (the audit-confirmed F3 leak).
3. No post-acceptance rejection path exists for fast_track: `admission decide`
   refuses non-review/quarantine envelopes, so an accepted fast-track
   envelope's facts can never retroactively become rejection-governed.

(Review/quarantine-mode isolation via the quarantine branch is covered by
tests/substrate/test_admission_quarantine.py. The legacy
`okg.projectable_node_facts` / `projectable_edge_facts` views were RETIRED by
`schema-canonical-read-path`: the chronos-native cutover had already reduced
them to zero-row stubs, and they had no node-side consumers. The loader
deny-list above is the live enforcement, which the absence test below pins.)
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.errors import SubstrateError

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "review-test"
_PRODUCER = "agent:fast"


def _submit_node(admission_workflows, node: dict):
    """Fast-track-submit a single node via the real admission path."""
    return admission_workflows._run_submit(
        deployment=_DEPLOYMENT,
        payload={"nodes": [node]},
        producer_id=_PRODUCER,
        producer_kind="agent",
        intent="record_observation",
        idempotency_key=f"rejection-projection-{node['node_id']}",
        authority={
            "producer_policy": {
                "producer_id": _PRODUCER,
                "producer_kind": "agent",
                "trust_label": "fast-track-trusted",
                "admission_mode": "fast_track",
                "allowed_record_kinds": ["node"],
                "output_signature": {
                    "nodes": [
                        {"subtype": node["subtype"], "clause_id": "allow"},
                    ],
                },
                "policy_clauses": [
                    {
                        "id": "allow",
                        "effect": "allow",
                        "record_kind": "node",
                        "subtype": node["subtype"],
                    },
                ],
            },
        },
        check_summary={"record": {"kind": "observation"}},
    )


def test_rejected_fast_track_submit_leaves_nothing_to_project(
    clean_runtime: str,
    monkeypatch,
) -> None:
    """Mechanism 1: check-before-emit. A payload that trips a rejection check
    (agent-authored transcript subtype) rejects the envelope BEFORE any fact
    insert, so the projection layer never sees governed rows at all."""
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows, "_resolve_dsn", lambda _d: clean_runtime,
    )

    rejected = _submit_node(
        admission_workflows,
        {"node_id": "probe:reject", "subtype": "agent_session",
         "attrs": {"text": "t"}},
    )
    assert rejected["status"] == "rejected"
    assert rejected.get("payload_rejected") is True

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.node_facts WHERE source = %s",
            (_PRODUCER,),
        )
        assert int(cur.fetchone()[0]) == 0


def test_legacy_projectable_views_are_gone(clean_runtime: str) -> None:
    """The projectable views are RETIRED, not stubbed.

    This test used to assert the opposite shape: that the chronos-native
    cutover had replaced okg.projectable_node_facts / projectable_edge_facts
    with `WHERE false` zero-row stubs, and that the stub must not "grow rows
    again". Pinning a stub pins a granted view that structurally cannot answer
    — the defect `schema-canonical-read-path` set out to remove — so the views
    are deleted instead (design §2.1), on fresh installs by their absence from
    views.sql and on existing ones by migration
    0024_canonical_read_path_view_cleanup.

    Nothing about rejection safety moves. It never lived in these views: the
    live enforcement is the derived-fact loader's (source, run_id) envelope
    deny-list, tested below. If either view is ever reintroduced, the
    enforcement story changed and this module's mechanism map must be
    revisited.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        for relation in (
            "okg.projectable_node_facts",
            "okg.projectable_edge_facts",
        ):
            cur.execute("SELECT to_regclass(%s)", (relation,))
            assert cur.fetchone()[0] is None, (
                f"{relation} is back; rejection safety for committed derived "
                "facts is the loader deny-list, so re-adding this view means "
                "re-deciding where enforcement lives"
            )


def _insert_derived_fact_pair(conn, *, source: str, run_id: str) -> tuple[int, int]:
    """One node fact + one edge fact for a derived producer, as the enrichers
    commit them during a publish cycle (before any admission verdict)."""
    from okg.substrate.runtime.admission_workflows import (
        _ADMISSION_CHECKSUM,
        _ADMISSION_PROGRAM,
    )
    from okg.substrate.transform.commit import persist_transform_program

    # Commit-witness: the fact tables' source_program_checksum default reads
    # this per-session GUC (same pattern as the admission emit path).
    persist_transform_program(conn, _ADMISSION_PROGRAM)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT set_config('okg.source_program_checksum', %s, false)",
            (_ADMISSION_CHECKSUM,),
        )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES
              ('derived:probe-node', 'alpha', '{}'::jsonb, %(source)s,
               '{}'::jsonb, '{}'::jsonb, now(), %(source)s || ':node',
               %(run_id)s::uuid, 'I')
            RETURNING fact_id
            """,
            {"source": source, "run_id": run_id},
        )
        node_fact_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO okg.edge_facts
              (edge_id, src, dst, edge_type, attrs, provenance, confidence,
               source, source_record_id, source_revision, observed_at,
               dedupe_key, run_id, op)
            VALUES
              (1, 'derived:probe-node', 'derived:probe-dst', 'links_to',
               '{}'::jsonb, 'derived_deterministic', 1.0, %(source)s, '{}'::jsonb,
               '{}'::jsonb, now(), %(source)s || ':edge', %(run_id)s::uuid,
               'I')
            RETURNING fact_id
            """,
            {"source": source, "run_id": run_id},
        )
        edge_fact_id = int(cur.fetchone()[0])
    return node_fact_id, edge_fact_id


def test_rejected_derived_producer_facts_are_not_loaded(
    clean_runtime: str,
) -> None:
    """Derived producers (enricher/extractor/lens) commit facts DURING the
    publish cycle and receive their admission verdict afterwards
    (Publisher._admit... at publisher.py:1500-1665 can set status=rejected
    with facts already committed, and rejection performs no fact cleanup).
    The derived-fact loader must therefore exclude BOTH fact kinds of a
    rejected producer run: edges via the fact-range allow-list, nodes via the
    source/run deny-list. Before the wave-2 fix, the node branch had no
    admission clause at all (audit finding F3)."""
    import uuid as _uuid

    from okg.substrate.admission import attach_fact_range_refs, ensure_envelope
    from okg.substrate.publisher.publisher import Publisher
    from okg.substrate.admission import FactRangeRef

    source = "_enricher:probe"
    run_id = str(_uuid.uuid4())
    publisher = Publisher(clean_runtime, _DEPLOYMENT)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        node_fact_id, edge_fact_id = _insert_derived_fact_pair(
            conn, source=source, run_id=run_id,
        )
        envelope = ensure_envelope(
            conn,
            deployment_name=_DEPLOYMENT,
            producer_id=source,
            producer_kind="enricher",
            intent="derived_edge_emission",
            admission_mode="fast_track",
            status="rejected",
            idempotency_key=f"derived-probe-{run_id}",
            source=source,
            run_id=run_id,
            authority={},
            check_summary={},
            workflow_ref={},
        )
        attach_fact_range_refs(
            conn,
            admission_id=envelope.admission_id,
            refs=[FactRangeRef(
                ref_kind="edge_fact_range",
                ref_key=f"{source}:{run_id}",
                source=source,
                run_id=run_id,
                fact_id_min=edge_fact_id,
                fact_id_max=edge_fact_id,
                record_count=1,
            )],
            workflow_ref={},
        )

        loaded = publisher._load_unapplied_derived_facts(
            conn,
            min_node_fact_id=node_fact_id - 1,
            min_edge_fact_id=edge_fact_id - 1,
        )

    loaded_node_ids = {
        getattr(fact, "node_id", None) for fact in loaded
    }
    loaded_edges = {
        (getattr(fact, "src", None), getattr(fact, "dst", None))
        for fact in loaded
    }
    assert ("derived:probe-node", "derived:probe-dst") not in loaded_edges, (
        "rejected derived edge facts must not load (fact-range allow-list)"
    )
    assert "derived:probe-node" not in loaded_node_ids, (
        "rejected derived NODE facts must not load: the producer's admission "
        "was rejected after its facts were committed, and rejection safety "
        "must hold for both fact kinds"
    )


def test_derived_facts_without_envelope_stay_loadable(
    clean_runtime: str,
) -> None:
    """Fail-open contract of the deny-list: a derived run with NO admission
    envelope at all is still loadable — governance is per-emission, not a
    precondition for projection, and only explicit rejection/failure/
    quarantine revokes loadability."""
    import uuid as _uuid

    from okg.substrate.publisher.publisher import Publisher

    source = "_enricher:ungoverned"
    run_id = str(_uuid.uuid4())
    publisher = Publisher(clean_runtime, _DEPLOYMENT)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        node_fact_id, edge_fact_id = _insert_derived_fact_pair(
            conn, source=source, run_id=run_id,
        )
        loaded = publisher._load_unapplied_derived_facts(
            conn,
            min_node_fact_id=node_fact_id - 1,
            min_edge_fact_id=edge_fact_id - 1,
        )

    loaded_node_ids = {getattr(fact, "node_id", None) for fact in loaded}
    assert "derived:probe-node" in loaded_node_ids, (
        "an envelope-less derived run must stay loadable (fail-open); the "
        "deny-list only excludes explicitly rejected/failed/quarantined runs"
    )


@pytest.mark.parametrize("enforcement_mode", ["audit", "off"])
def test_rejected_derived_facts_load_under_off_enforcement(
    clean_runtime: str,
    enforcement_mode: str,
) -> None:
    """Under `audit`/`off` enforcement, admission is log-and-allow / boundary-
    skipped and MUST NOT gate the served graph — for NODES as well as edges.
    #572 relaxed only the edge allow-list, leaving a rejected producer's NODES
    dropped by the deny-list; that stranded the unconditionally-projected edges
    (dangling endpoints) and made the whole derivation invisible. So under off
    BOTH the rejected producer's node and edge facts must load. (The `enforce`
    deny-list is covered by test_rejected_derived_producer_facts_are_not_loaded.)
    """
    import uuid as _uuid

    from okg.substrate.admission import (
        FactRangeRef,
        attach_fact_range_refs,
        ensure_envelope,
    )
    from okg.substrate.publisher.publisher import Publisher

    source = "_enricher:probe_off"
    run_id = str(_uuid.uuid4())
    publisher = Publisher(clean_runtime, _DEPLOYMENT)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        node_fact_id, edge_fact_id = _insert_derived_fact_pair(
            conn, source=source, run_id=run_id,
        )
        envelope = ensure_envelope(
            conn,
            deployment_name=_DEPLOYMENT,
            producer_id=source,
            producer_kind="enricher",
            intent="derived_edge_emission",
            admission_mode="fast_track",
            status="rejected",
            idempotency_key=f"derived-probe-off-{run_id}",
            source=source,
            run_id=run_id,
            authority={},
            check_summary={},
            workflow_ref={},
        )
        attach_fact_range_refs(
            conn,
            admission_id=envelope.admission_id,
            refs=[FactRangeRef(
                ref_kind="edge_fact_range",
                ref_key=f"{source}:{run_id}",
                source=source,
                run_id=run_id,
                fact_id_min=edge_fact_id,
                fact_id_max=edge_fact_id,
                record_count=1,
            )],
            workflow_ref={},
        )

        loaded = publisher._load_unapplied_derived_facts(
            conn,
            min_node_fact_id=node_fact_id - 1,
            min_edge_fact_id=edge_fact_id - 1,
            enforcement_mode=enforcement_mode,
        )

    loaded_node_ids = {getattr(fact, "node_id", None) for fact in loaded}
    loaded_edges = {
        (getattr(fact, "src", None), getattr(fact, "dst", None))
        for fact in loaded
    }
    assert "derived:probe-node" in loaded_node_ids, (
        "under off, a rejected derived producer's NODE facts must still load "
        "(log-and-allow: admission does not gate the served graph); without "
        "this the enricher's edges project but its nodes vanish"
    )
    assert ("derived:probe-node", "derived:probe-dst") in loaded_edges, (
        "under off, a rejected derived producer's EDGE facts must load (#572)"
    )


def _publish_sweep(dsn: str) -> None:
    from okg.substrate.admission import _mark_generation_published

    with psycopg.connect(dsn, autocommit=True) as conn:
        _mark_generation_published(
            conn,
            generation={
                "deployment_name": _DEPLOYMENT,
                "branch_name": "default",
                "generation_id": "999",
                "generation_int": 999,
                "fact_watermark": 10_000_000,
                "edge_fact_watermark": 10_000_000,
            },
            workflow_ref={},
        )


def test_publish_sweep_requires_decision_for_review_envelopes(
    clean_runtime: str,
) -> None:
    """The publish sweep's mode guard (wave-2 fix): refs attach for ANY mode
    with output, so a pending REVIEW envelope with watermark-covered refs
    must NOT be auto-published — it awaits `okg admission decide`, which
    requires pending. A pending DIRECT envelope with the same refs IS swept
    to published (publish decisiveness, the decided audit-F1 behavior)."""
    import uuid as _uuid

    from okg.substrate.admission import (
        FactRangeRef,
        attach_fact_range_refs,
        ensure_envelope,
    )

    statuses: dict[str, str] = {}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for mode, node_id_suffix in (("review", "held"), ("direct", "swept")):
            source = f"probe_sweep_{mode}"
            run_id = str(_uuid.uuid4())
            envelope = ensure_envelope(
                conn,
                deployment_name=_DEPLOYMENT,
                producer_id=source,
                producer_kind="source",
                intent="source_sync",
                admission_mode=mode,
                status="pending",
                idempotency_key=f"sweep-probe-{mode}",
                source=source,
                run_id=run_id,
                authority={},
                check_summary={},
                workflow_ref={},
            )
            attach_fact_range_refs(
                conn,
                admission_id=envelope.admission_id,
                refs=[FactRangeRef(
                    ref_kind="node_fact_range",
                    ref_key=f"{source}:{run_id}",
                    source=source,
                    run_id=run_id,
                    fact_id_min=1,
                    fact_id_max=1,
                    record_count=1,
                )],
                workflow_ref={},
            )
            statuses[mode] = envelope.admission_id

    _publish_sweep(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        for mode, expected in (("review", "pending"), ("direct", "published")):
            cur.execute(
                """
                SELECT status FROM okg.admission_envelopes
                 WHERE admission_id = %s::uuid
                """,
                (statuses[mode],),
            )
            actual = cur.fetchone()[0]
            assert actual == expected, (
                f"{mode}-mode pending envelope with watermark-covered refs "
                f"must be {expected} after the publish sweep, got {actual}"
            )


def test_admission_decide_refuses_fast_track_envelopes(
    clean_runtime: str,
    monkeypatch,
) -> None:
    """Mechanism 3: no post-acceptance rejection path. An accepted fast-track
    envelope cannot be retroactively rejected through the decide surface, so
    accepted-and-projected facts cannot silently become rejection-governed."""
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows, "_resolve_dsn", lambda _d: clean_runtime,
    )

    accepted = _submit_node(
        admission_workflows,
        {"node_id": "probe:final", "subtype": "alpha", "attrs": {"text": "b"}},
    )
    assert accepted["status"] == "accepted"

    with pytest.raises(SubstrateError, match="review/quarantine"):
        admission_workflows._run_decide(
            deployment=_DEPLOYMENT,
            admission_id=accepted["admission_id"],
            decision="reject",
            actor="operator:test",
            reason="post-hoc rejection must be refused",
            verdict=None,
            dsn=clean_runtime,
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT status FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (accepted["admission_id"],),
        )
        assert cur.fetchone()[0] == "accepted"
