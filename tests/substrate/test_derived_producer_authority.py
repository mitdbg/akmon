from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.admission import (
    apply_generation_feedback,
)
from okg.substrate.catalog.linker_config import LinkerRoute
from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.enrichers.derived_edges import (
    DerivedEdgeCandidate,
    insert_deterministic_edges,
    mint_edge_id,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    attrs_with_lineage,
    normalize_source_lineage,
)
from okg.substrate.producer_authority import (
    SYNTHETIC_PROFILE_ORIGIN,
    ProducerPolicyViolation,
    load_profile_by_fingerprint,
    load_profile_for_producer,
    materialize_producer_profile,
)
from okg.substrate.projection import RegexEntityExtractor
from okg.substrate.publisher import Publisher
from okg.substrate.publisher.deployment_stack import (
    load_derived_producer_policies,
    load_projection_extractor,
    load_projection_extractor_source_subtypes,
    load_projection_routes,
)
from okg.substrate.cli.doctor import (
    _derived_admission_findings,
    _derived_admission_producer_inventory,
    _derived_producer_output_state,
    _withheld_enricher_findings,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.pytest.admission_envelope_reads import (  # noqa: E402
    AmbiguousAdmissionLookup,
    fetch_admission_envelope,
)


pytestmark = pytest.mark.postgres


def _pin_enforce_mode(
    monkeypatch: pytest.MonkeyPatch,
    dsn: str,
) -> None:
    """Pin the Nomos runtime tri-state to ``enforce`` so the publisher's
    derived-fact projection gate actually gates.

    Since 1e627efca the served graph excludes rejected derived EDGES
    only under ``enforce`` — ``audit``/``off`` (the bare test-deployment
    default) record the admission verdict but still project the edge
    (audit-mode coverage lives in test_publisher_chronos_native_delta).
    These tests exist to prove the fail-closed path, so they pin the
    mode at the runtime_enforcement resolver. Caveat before copying
    this pattern: okg.substrate.nomos.store value-binds the resolver
    at import time, so store-evaluated boundaries still see the real
    mode — inert for "enforce" (the store evaluator treats everything
    except "audit" as strict) but a pin of "audit" would NOT get
    log-and-allow semantics there.

    Enforce mode also arms the ``generation.publish`` runtime boundary
    (default-deny with no active bundle), so an allow-publisher bundle
    is activated for the fixture deployment — the assertions under test
    are the DERIVED-authority verdicts, not the publish gate.
    """
    from okg.substrate.nomos import (
        activate_policy_bundle,
        compile_policy_graph,
    )

    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:derived-authority",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [{
            "rule_id": "allow_publisher_publish",
            "effect": "allow",
            "actions": ["generation.publish"],
            "subjects": ["okg_service:publisher"],
            "priority": 1,
            "reason_code": "nomos.test.allow_generation_publish",
        }],
    })
    # Activate BEFORE pinning enforce: under enforce, policy.activate
    # itself is a default-deny runtime boundary (bootstrap
    # chicken-and-egg), and this bundle is fixture plumbing, not the
    # contract under test.
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="test",
            bundle=bundle,
            scope="deployment:test",
            activated_by="test:operator",
        )
        conn.commit()
    monkeypatch.setattr(
        "okg.substrate.nomos.runtime_enforcement."
        "deployment_nomos_enforcement_mode",
        lambda _deployment_name: "enforce",
    )


def _graph_edge_count(
    dsn: str,
    generation_id: str,
    *,
    src: str,
    edge_type: str,
    dst: str,
) -> int:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        checkout = store.checkout_generation(generation_id)
        rows = checkout.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_edges
             WHERE src = :src
               AND edge_type = :edge_type
               AND dst = :dst
            """,
            {"src": src, "edge_type": edge_type, "dst": dst},
        )
        return int(rows[0]["n"])
    finally:
        store.close()


class _AllowedEnricher:
    name = "authority_allowed"
    reads_subtypes = ("auth_src",)
    reads_edge_types = ()
    emits_edge_types = ("auth_allows",)
    requires_narrowings = (("auth_src", "auth_allows", "auth_dst"),)

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id, incremental
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:dst",
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


class _RejectedEnricher:
    name = "authority_rejected"
    reads_subtypes = ("auth_src",)
    reads_edge_types = ()
    emits_edge_types = ("auth_allows",)
    requires_narrowings = (("auth_src", "auth_allows", "auth_dst"),)

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id, incremental
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_denies",
                        dst="auth:dst",
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


class _RuleSourceEnricher:
    name = "authority_family"
    reads_subtypes = ("auth_src",)
    reads_edge_types = ()
    emits_edge_types = ("auth_allows",)
    requires_narrowings = (("auth_src", "auth_allows", "auth_dst"),)

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id, incremental
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}:rule_one",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:dst",
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


class _LegacyNoLineageEnricher:
    name = "authority_legacy_no_lineage"
    reads_subtypes = ("auth_src",)
    reads_edge_types = ()
    emits_edge_types = ("auth_allows",)
    requires_narrowings = (("auth_src", "auth_allows", "auth_dst"),)

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id, incremental
        source = f"_enricher:{self.name}"
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs,
                   provenance, confidence, source,
                   source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES (%s, 'auth:src', 'auth:dst', 'auth_allows',
                        '{}'::jsonb, 'derived_deterministic', 1.0, %s,
                        '{}'::jsonb, '{}'::jsonb,
                        now(), %s, %s, 'I')
                """,
                (
                    mint_edge_id("auth:src", "auth_allows", "auth:dst"),
                    source,
                    f"{source}:legacy:{uuid4()}:I",
                    str(uuid4()),
                ),
            )
        return EnrichResult(self.name, n_edges_emitted=1, n_edges_skipped=0)


class _DerivedNodeEmitter:
    """Cycle 1: a trusted deterministic enricher that lands a derived NODE.

    The node it emits (``auth:mid``) is applied to the graph by the
    publisher. Since PR #788 that write is attributed to THIS producer
    rather than to a shared ``_publisher_derived`` bucket, but it is still
    a re-application of an already admitted fact, so the node keeps the
    lineage it declared (#1534). The next cycle reads it back as an input.
    """

    name = "authority_derived_emitter"
    reads_subtypes = ("auth_src",)
    reads_edge_types = ()
    emits_edge_types = ("auth_allows",)
    emits_typed_subtype = "auth_mid"
    # Boot-time contract (PACT bind-derived-node-provenance): a
    # node-emitting enricher must declare it supplies provenance.
    declares_node_provenance = True
    requires_narrowings = (("auth_src", "auth_allows", "auth_mid"),)
    explicit_edge_lineage = False
    share_edge_run_id = False

    def enrich(self, conn, *, generation_id, incremental=None):
        del incremental
        from datetime import datetime, timezone

        from okg.substrate.library.enrichers._fact_emitters import (
            emit_node_fact,
        )

        run_id = str(uuid4())
        emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id="auth:mid",
            subtype="auth_mid",
            attrs={"text": "derived middle"},
            dedupe_key=f"_enricher:{self.name}:auth:mid:I",
            source_record_id={},
            source_revision={"generation_id": str(generation_id)},
            run_id=run_id,
            observed_at=datetime.now(timezone.utc),
            # PACT bind-derived-node-provenance: declare what this node was
            # derived from. auth:src is a rank-3 source node, so the derived
            # node inherits its lineage instead of landing empty.
            derived_from=["auth:src"],
            support_policy="all_members",
        )
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:mid",
                        attrs=(
                            {
                                LINEAGE_ATTR: normalize_source_lineage(
                                    "auth_fixture"
                                ),
                            }
                            if self.explicit_edge_lineage
                            else {}
                        ),
                    ),
                ],
                run_id=run_id if self.share_edge_run_id else None,
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


class _ColdSameCycleNodeEmitter(_DerivedNodeEmitter):
    """Issue #841 fixture with truthful, predeclared edge input lineage.

    The explicit lineage isolates same-cycle subtype admission from the
    independent fail-closed lineage/trust gate. Default emitter fixtures keep
    their historical behavior.
    """

    name = "authority_cold_same_cycle_emitter"
    explicit_edge_lineage = True
    share_edge_run_id = True


class _DerivedNodeReader:
    """Cycle 2: a trusted deterministic enricher reading the derived node.

    Its edge endpoints are ``auth:mid`` (emitted by
    :class:`_DerivedNodeEmitter` and applied to the graph by the publisher)
    and ``auth:dst`` (a trusted source node). This enricher inherits its
    input trust from ``auth:mid``'s LINEAGE, that is from the roots that
    node declared it was derived from — not from whatever producer id the
    publisher wrote it under (#1534). Where the root is untrusted, this
    enricher's rank-3 deterministic output is capped at rank 0 and the
    envelope is rejected (issue #781).
    """

    name = "authority_derived_reader"
    reads_subtypes = ("auth_mid",)
    reads_edge_types = ()
    emits_edge_types = ("auth_allows",)
    requires_narrowings = (("auth_mid", "auth_allows", "auth_dst"),)

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id, incremental
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:mid",
                        edge_type="auth_allows",
                        dst="auth:dst",
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


def test_rejection_summary_is_counted_in_the_cycle_outcome(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wholly-rejected derived producer is legible in the outcome.

    PACT surface-derived-admission-rejections. The cycle status stays
    ``published`` (the contract the rest of this suite pins); what changes
    is that the rejection is a counted key instead of an untyped element
    of ``enrichers``. Runs through a plain in-process Publisher, i.e. the
    inline path with no DBOS context, because that is where the phase-event
    channel is a silent no-op.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_RejectedEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    summary = out["derived_rejections"]
    producers = {item["producer_id"]: item for item in summary["producers"]}

    assert out["status"] == "published"
    assert summary["rejected_producer_count"] == 1
    assert summary["discarded_record_count"] >= 1
    rejected = producers["_enricher:authority_rejected"]
    assert rejected["code"] == "producer_edge_triple_not_allowed"
    assert rejected["discarded_record_count"] >= 1


def test_rejection_summary_is_empty_when_every_producer_is_admitted(
    clean_runtime: str,
) -> None:
    """The summary is not a permanently-red signal."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_AllowedEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    assert out["status"] == "published"
    assert out["derived_rejections"]["rejected_producer_count"] == 0
    assert out["derived_rejections"]["discarded_record_count"] == 0
    assert out["derived_rejections"]["producers"] == []


def test_derived_input_trust_resolves_to_originating_producer(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #781: an enricher reading the graph's own derived rows.

    Cycle 1 lands ``auth:mid`` from a trusted deterministic enricher.
    Cycle 2's enricher reads it. Both producers are rank 3 and every real
    upstream source is rank 3, so the read MUST be admitted and its edge
    MUST reach the live graph.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)

    emit_out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_DerivedNodeEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test",
            generation_id=emit_out["generation_id"],
        )

    read_out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_DerivedNodeReader()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test",
            generation_id=read_out["generation_id"],
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status,
                       verdict->>'code',
                       verdict->'producer_policy'->'trust_lattice'
                         ->>'least_input_trust_label',
                       verdict->'producer_policy'->'trust_lattice'
                         ->'input_sources'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_enricher:authority_derived_reader'
                """
            )
            admission = cur.fetchone()

    live_edges = _graph_edge_count(
        clean_runtime,
        read_out["generation_id"],
        src="auth:mid",
        edge_type="auth_allows",
        dst="auth:dst",
    )

    status, code, least_input, input_sources = admission
    reported = {str(item.get("source")) for item in (input_sources or [])}

    assert status in {"accepted", "published"}, (
        f"derived-input read was {status} ({code}); "
        f"least_input_trust_label={least_input}, input_sources={reported}"
    )
    # Neither fail-closed sentinel survives: the derived node now carries
    # real inherited provenance instead of an empty envelope.
    assert "_publisher_derived" not in reported
    assert "__malformed_lineage__" not in reported
    # The input resolves to the true ROOT source, not to the intermediate
    # enricher's own declared label. Note the honest scope of that claim:
    # trust is bounded by what the last hop DECLARED it derived from, which
    # the substrate does not verify (see the limit documented on
    # _stamp_publisher_derived_node_lineage). This pins that a truthful
    # declaration resolves transitively to the root; it does not prove a
    # dishonest one cannot inherit trust it did not earn.
    assert reported == {"auth_fixture"}
    assert least_input == "repo-local-trusted"
    assert live_edges == 1


def test_publisher_reapplication_keeps_the_root_lineage_it_was_given(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A published derived node names its ROOT source, not its emitter.

    Regression for #1534. The publisher writes an accepted derived
    fact-log row into the checkpoint under the id of the producer that
    emitted it (PR #788). That write is a RE-APPLICATION of an already
    attributed fact, so it must keep the lineage the row carries. When it
    instead stamped the re-applying producer as the source id, every
    reader of the node saw the emitting enricher as a root source and
    resolved trust from that enricher's own declared label — which is how
    an untrusted root gets laundered through one hop.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_DerivedNodeEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()
    assert out["status"] == "published"

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = checkout.query(
            """
            SELECT attrs->'nomos_source_lineage' AS lineage
              FROM okg.graph_nodes
             WHERE node_id = 'auth:mid'
            """,
            {},
        )
    finally:
        store.close()

    assert rows, "the derived node should be in the published checkpoint"
    lineage = rows[0]["lineage"] or {}
    assert lineage.get("status") == "ok", lineage
    assert lineage.get("source_ids") == ["auth_fixture"], lineage
    assert "_enricher:authority_derived_emitter" not in (
        lineage.get("source_ids") or []
    ), lineage


def test_reapplication_envelope_is_checked_and_reaches_a_terminal_state(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The publisher's own re-application envelope is a real envelope.

    Issue #1545, defect 1. `publisher_derived:*` was recorded as a
    fast-track admission with an EMPTY check_summary and no record ref,
    so it recorded neither what was checked nor whether the facts it
    applied ever reached a checkpoint: it sat at `accepted` forever, and
    `admission_conformance` counted it as a fast-track admission with no
    producer-policy evidence.

    NOTE for anyone extending this file: scope envelope queries by
    INTENT. The publisher records its own `publisher_derived:*` envelope
    under the EMITTING producer's id, so `producer_id` alone matches both
    that and the producer's emission envelope.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_DerivedNodeEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()
    assert out["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test",
            generation_id=out["generation_id"],
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status,
                       check_summary ? 'admission_policy',
                       check_summary #>> '{producer_policy,profile_fingerprint}',
                       check_summary #>> '{admission_policy,derived_reapplication}',
                       target_generation_ref
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'publisher_derived:pre_view'
                   AND producer_id = '_enricher:authority_derived_emitter'
                """
            )
            row = cur.fetchone()

    assert row is not None, (
        "the publisher should record a re-application envelope"
    )
    status, has_policy, fingerprint, reapplication, generation_ref = row
    assert has_policy is True, "check_summary must name the admission policy"
    assert fingerprint, "check_summary must cite the producer profile"
    assert reapplication == "true"
    assert status == "published", (
        f"re-application envelope stopped at {status!r} — it carries no "
        "record ref, so the generation sweep cannot see it"
    )
    assert generation_ref == str(out["generation_id"])


def test_reapplication_profile_cannot_win_a_trust_lookup(
    clean_runtime: str,
) -> None:
    """A re-application must not mint trust its producer never earned.

    Issue #1545, defect 2, and a laundering vector independent of #1534.

    The publisher applies an accepted derived fact-log row under the
    EMITTING producer's id. `apply_bounded_fact_batch` mints a
    `producer_profiles` row for that id so the write has a FactBatch
    header to cite, and it used to stamp the hardcoded default
    `derived-deterministic-trusted` (rank 3) on it.

    `load_profile_by_fingerprint` answers an unfingerprinted lookup by
    `active` first and then `updated_at DESC`. For a producer whose
    DECLARED profile is `compat` — the shape here — the freshly minted
    row therefore won, and a trust lookup for that producer resolved to
    rank 3 instead of the `less-trusted` rank 1 the deployment granted.

    Two independent defences are asserted: the minted row now carries the
    DECLARED label, and a synthetic row can no longer win the ordering.
    """
    # A LENS, not an `_enricher:*` id: the derived-node support
    # declaration guard is scoped to that prefix and would refuse the
    # bare fixture write before the mint under test could happen. The
    # re-application path is identical for either producer kind.
    producer_id = "_lens:laundry_probe"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            catalog_version_id = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO okg.subtypes (name, metadata, catalog_version_id)
                VALUES ('auth_mid', '{}'::jsonb, %s)
                """,
                (catalog_version_id,),
            )
        # What the deployment actually GRANTED this producer: compat
        # authority (no output signature) at rank 1.
        declared = materialize_producer_profile(
            conn,
            deployment_name="test",
            producer_id=producer_id,
            producer_kind="lens",
            trust_label="less-trusted",
            admission_mode="fast_track",
            authority_scope={"source_family": producer_id},
            allowed_record_kinds=("node", "edge"),
            output_signature={},
            policy_clauses=(),
            output_scope_summary={"summary": "declared authority"},
            source_ref={"fixture": "test_derived_producer_authority"},
            catalog_version_id=catalog_version_id,
        )
        assert declared.status == "compat"

        publisher = Publisher(
            clean_runtime,
            "test",
            catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
        )
        store = ChronosGraphStore.from_connection(conn)
        try:
            publisher._apply_publisher_derived_fact_batch(
                conn,
                store,
                [
                    NodeFact(
                        "auth:mid",
                        "auth_mid",
                        {"text": "re-applied derived node"},
                        {},
                        {},
                    ),
                ],
                catalog_version_id=catalog_version_id,
                gate_target_id=1,
                stage="pre_view",
                previous_generation_ref=None,
                producer_id=producer_id,
                producer_kind="lens",
                source=producer_id,
                run_id=uuid4(),
            )
        finally:
            store.close()

        resolved = load_profile_by_fingerprint(
            conn,
            deployment_name="test",
            producer_id=producer_id,
            profile_fingerprint=None,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trust_label, source_ref->>'profile_origin'
                  FROM okg.producer_profiles
                 WHERE deployment_name = 'test'
                   AND producer_id = %s
                 ORDER BY trust_label
                """,
                (producer_id,),
            )
            minted = cur.fetchall()

    assert resolved.trust_label == "less-trusted", (
        f"an unfingerprinted trust lookup resolved to "
        f"{resolved.trust_label!r} — a label this producer never earned"
    )
    assert resolved.profile_fingerprint == declared.profile_fingerprint
    assert "derived-deterministic-trusted" not in {
        str(label) for label, _origin in minted
    }, minted
    assert {"bounded_fact_batch"} == {
        origin for _label, origin in minted if origin is not None
    }, minted



def _materialize_synthetic_legacy_profile(
    conn: psycopg.Connection,
    *,
    producer_id: str,
    trust_label: str,
) -> str:
    """A row in the PRE-MARKER machine-minted shape.

    Written by hand rather than through ``materialize_producer_profile``
    because the point is the shape rows had BEFORE ``profile_origin``
    existed: ``source_ref`` of exactly ``{"workflow", "producer_id"}`` and
    nothing else.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.producer_profiles
              (deployment_name, producer_id, producer_kind, trust_label,
               admission_mode, authority_scope, allowed_record_kinds,
               output_signature, policy_clauses, output_scope_summary,
               source_ref, catalog_version_id, profile_fingerprint,
               status, catalog_evidence)
            VALUES
              ('test', %(producer_id)s, 'enricher', %(trust_label)s,
               'fast_track', '{}'::jsonb, ARRAY['node','edge'],
               '{}'::jsonb, '[]'::jsonb, '{}'::jsonb,
               %(source_ref)s::jsonb, NULL, %(fingerprint)s,
               'compat', '{}'::jsonb)
            RETURNING profile_fingerprint
            """,
            {
                "producer_id": producer_id,
                "trust_label": trust_label,
                "source_ref": json.dumps({
                    "workflow": "publisher_derived:pre_view",
                    "producer_id": producer_id,
                }),
                "fingerprint": f"legacy-synthetic-{uuid4()}",
            },
        )
        return str(cur.fetchone()[0])


def test_a_legacy_machine_minted_profile_cannot_win_a_trust_lookup(
    clean_runtime: str,
) -> None:
    """The pre-marker rows are recognised too, or the fix is forward-only.

    Issue #1545. ``profile_origin`` did not exist when these rows were
    written, so a deployment that has ALREADY minted a laundered row would
    keep resolving to it forever if the ordering recognised only newly
    marked rows. The legacy arm identifies them by their exact two-key
    ``source_ref`` shape.

    The legacy row is written LAST so it wins ``updated_at DESC``, which is
    exactly how it used to beat the declared profile.
    """
    producer_id = "_enricher:legacy_laundry"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        declared = materialize_producer_profile(
            conn,
            deployment_name="test",
            producer_id=producer_id,
            producer_kind="enricher",
            trust_label="less-trusted",
            admission_mode="fast_track",
            authority_scope={"source_family": producer_id},
            allowed_record_kinds=("node", "edge"),
            output_signature={},
            policy_clauses=(),
            output_scope_summary={"summary": "declared authority"},
            source_ref={"fixture": "test_derived_producer_authority"},
            catalog_version_id=None,
        )
        _materialize_synthetic_legacy_profile(
            conn,
            producer_id=producer_id,
            trust_label="derived-deterministic-trusted",
        )

        resolved = load_profile_by_fingerprint(
            conn,
            deployment_name="test",
            producer_id=producer_id,
            profile_fingerprint=None,
        )

    assert resolved.trust_label == "less-trusted", resolved.trust_label
    assert resolved.profile_fingerprint == declared.profile_fingerprint


def test_a_declared_profile_carrying_both_keys_is_not_called_synthetic(
    clean_runtime: str,
) -> None:
    """The legacy arm is an EXACT shape, not a containment test.

    Issue #1545. A declared profile may legitimately carry ``workflow`` and
    ``producer_id`` in its ``source_ref`` alongside its own references. If
    the legacy arm matched on containment, that real declaration would be
    demoted below every machine row — the fix inverted.
    """
    producer_id = "_enricher:declared_with_workflow_ref"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        declared = materialize_producer_profile(
            conn,
            deployment_name="test",
            producer_id=producer_id,
            producer_kind="enricher",
            trust_label="less-trusted",
            admission_mode="fast_track",
            authority_scope={"source_family": producer_id},
            allowed_record_kinds=("node", "edge"),
            output_signature={},
            policy_clauses=(),
            output_scope_summary={"summary": "declared authority"},
            source_ref={
                "workflow": "publisher_derived:pre_view",
                "producer_id": producer_id,
                "deployment_manifest": "deployments/test/manifest.yaml",
            },
            catalog_version_id=None,
        )

        resolved = load_profile_by_fingerprint(
            conn,
            deployment_name="test",
            producer_id=producer_id,
            profile_fingerprint=None,
            declared_only=True,
        )

    assert resolved.profile_fingerprint == declared.profile_fingerprint


def test_declared_only_filters_the_fingerprinted_lookup_too(
    clean_runtime: str,
) -> None:
    """declared_only must never be accepted and then ignored.

    Issue #1545. The fingerprinted arm answers a caller that already knows
    which row it wants. If it ignored declared_only, a caller asking for
    DECLARED authority would silently receive a machine-minted row whenever
    it happened to hold that row's fingerprint.
    """
    producer_id = "_enricher:fingerprinted_synthetic"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        synthetic = materialize_producer_profile(
            conn,
            deployment_name="test",
            producer_id=producer_id,
            producer_kind="enricher",
            trust_label="derived-deterministic-trusted",
            admission_mode="fast_track",
            authority_scope={"source_family": producer_id},
            allowed_record_kinds=("node", "edge"),
            output_signature={},
            policy_clauses=(),
            output_scope_summary={},
            source_ref={
                "workflow": "publisher_derived:pre_view",
                "producer_id": producer_id,
            },
            catalog_version_id=None,
            profile_origin=SYNTHETIC_PROFILE_ORIGIN,
        )

        # Without declared_only the row is still findable by fingerprint.
        assert load_profile_by_fingerprint(
            conn,
            deployment_name="test",
            producer_id=producer_id,
            profile_fingerprint=synthetic.profile_fingerprint,
        ).profile_fingerprint == synthetic.profile_fingerprint

        with pytest.raises(ProducerPolicyViolation) as excinfo:
            load_profile_by_fingerprint(
                conn,
                deployment_name="test",
                producer_id=producer_id,
                profile_fingerprint=synthetic.profile_fingerprint,
                declared_only=True,
            )

    assert excinfo.value.code == "producer_profile_missing"


def test_a_producer_with_only_a_machine_row_supplies_no_input_trust(
    clean_runtime: str,
) -> None:
    """A system batch cannot supply its own input trust label.

    Issue #1545, defect 2 read from the consuming side. `_system:native_views`
    and any other producer the publisher has re-applied facts for owns a
    machine-minted profile and nothing else. Resolving input trust from that
    row would let the publisher's own bookkeeping authorise a derived edge
    at rank 3. The lookup asks for DECLARED authority, so it resolves as
    missing and the input is treated untrusted.
    """
    producer_id = "_system:native_views"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        materialize_producer_profile(
            conn,
            deployment_name="test",
            producer_id=producer_id,
            producer_kind="workflow",
            trust_label="derived-deterministic-trusted",
            admission_mode="fast_track",
            authority_scope={"source_family": producer_id},
            allowed_record_kinds=("node", "edge"),
            output_signature={},
            policy_clauses=(),
            output_scope_summary={},
            source_ref={
                "workflow": "publisher_derived:view",
                "producer_id": producer_id,
            },
            catalog_version_id=None,
            profile_origin=SYNTHETIC_PROFILE_ORIGIN,
        )

        with pytest.raises(ProducerPolicyViolation) as excinfo:
            load_profile_for_producer(
                conn,
                deployment_name="test",
                producer_id=producer_id,
                allow_source_family_fallback=True,
                declared_only=True,
            )

    assert excinfo.value.code == "producer_profile_missing", (
        "a machine-minted row answered a DECLARED-authority lookup"
    )


class _SelectiveDeclarationEmitter:
    """Declares ONE of the two trusted-and-untrusted inputs in the batch.

    ``auth:mid`` declares only ``auth:src``; ``auth:tainted`` (from an
    unprofiled, therefore untrusted, source) also sits in the graph. Used
    to pin that the stamp inherits from the DECLARED inputs only, and does
    not sweep in every node it happens to have loaded.
    """

    name = "authority_selective_emitter"
    reads_subtypes = ("auth_src",)
    reads_edge_types = ()
    emits_edge_types = ("auth_allows",)
    emits_typed_subtype = "auth_mid"
    # Boot-time contract (PACT bind-derived-node-provenance): a
    # node-emitting enricher must declare it supplies provenance.
    declares_node_provenance = True
    requires_narrowings = (("auth_src", "auth_allows", "auth_mid"),)

    def enrich(self, conn, *, generation_id, incremental=None):
        del incremental
        from datetime import datetime, timezone

        from okg.substrate.library.enrichers._fact_emitters import (
            emit_node_fact,
        )

        emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id="auth:mid",
            subtype="auth_mid",
            attrs={"text": "selective"},
            dedupe_key=f"_enricher:{self.name}:auth:mid:I",
            source_record_id={},
            source_revision={"generation_id": str(generation_id)},
            run_id=str(uuid4()),
            observed_at=datetime.now(timezone.utc),
            derived_from=["auth:src"],
            support_policy="all_members",
        )
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:mid",
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


def test_derived_node_inherits_only_its_declared_inputs(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The stamp is selective, not a union of everything available.

    Distinguishes the real implementation from a laundering one: an
    implementation that stamped every node with every source it had loaded
    would satisfy the happy-path test but would also hand this node the
    untrusted source it never declared. The envelope must name exactly the
    declared input's source.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)
        # A second, UNPROFILED (untrusted) source node in the same graph.
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("auth:tainted", "auth_src", {"text": "tainted"}, {}, {})],
            source="untrusted_fixture",
            run_id=str(uuid4()),
        )

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_SelectiveDeclarationEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = checkout.query(
            """
            SELECT attrs->'nomos_source_lineage' AS lineage
              FROM okg.graph_nodes
             WHERE node_id = 'auth:mid'
            """,
            {},
        )
    finally:
        store.close()

    lineage = rows[0]["lineage"] or {}
    assert lineage.get("status") == "ok", lineage
    # Exactly the declared input's source — the undeclared untrusted source
    # must NOT appear.
    assert lineage.get("source_ids") == ["auth_fixture"], lineage
    assert "untrusted_fixture" not in (lineage.get("source_ids") or [])


class _SameCycleChainEmitter:
    """Emits a two-link derived chain inside ONE cycle.

    ``auth:mid`` derives from the trusted source node ``auth:src``; then
    ``auth:mid2`` derives from ``auth:mid``. The publisher applies every
    enricher's facts in a single ``apply_facts`` batch, so both land
    together and the second link's input is a node whose own provenance is
    still being resolved in the same pass. The chain must still converge to
    the root source rather than fail closed on ordering.
    """

    name = "authority_chain_emitter"
    reads_subtypes = ("auth_src",)
    reads_edge_types = ()
    emits_edge_types = ("auth_allows",)
    emits_typed_subtype = "auth_mid"
    # Boot-time contract (PACT bind-derived-node-provenance): a
    # node-emitting enricher must declare it supplies provenance.
    declares_node_provenance = True
    requires_narrowings = (("auth_src", "auth_allows", "auth_mid"),)

    def enrich(self, conn, *, generation_id, incremental=None):
        del incremental
        from datetime import datetime, timezone

        from okg.substrate.library.enrichers._fact_emitters import (
            emit_node_fact,
        )

        now = datetime.now(timezone.utc)
        run_id = str(uuid4())
        # Emitted SECOND-link-first on purpose: if resolution depended on
        # batch order rather than dependency order, this ordering breaks it.
        emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id="auth:mid2",
            subtype="auth_mid",
            attrs={"text": "second link"},
            dedupe_key=f"_enricher:{self.name}:auth:mid2:I",
            source_record_id={},
            source_revision={"generation_id": str(generation_id)},
            run_id=run_id,
            observed_at=now,
            derived_from=["auth:mid"],
            support_policy="all_members",
        )
        emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id="auth:mid",
            subtype="auth_mid",
            attrs={"text": "first link"},
            dedupe_key=f"_enricher:{self.name}:auth:mid:I",
            source_record_id={},
            source_revision={"generation_id": str(generation_id)},
            run_id=run_id,
            observed_at=now,
            derived_from=["auth:src"],
            support_policy="all_members",
        )
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:mid",
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


def test_same_cycle_derived_chain_resolves_to_the_root_source(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A multi-link derived chain built in ONE batch reaches its root.

    Regression pin for a dependency-order hazard: resolution seeded from
    every node in the batch would hand the second link the first link's
    still-empty envelope, failing the chain closed for no reason. The
    second link must inherit the ROOT source, transitively.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_SameCycleChainEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = checkout.query(
            """
            SELECT node_id, attrs->'nomos_source_lineage' AS lineage
              FROM okg.graph_nodes
             WHERE node_id IN ('auth:mid', 'auth:mid2')
             ORDER BY node_id
            """,
            {},
        )
    finally:
        store.close()

    lineage = {str(row["node_id"]): (row["lineage"] or {}) for row in rows}

    # Both links carry well-formed provenance naming the true root source.
    for node_id in ("auth:mid", "auth:mid2"):
        assert lineage[node_id].get("status") == "ok", (
            f"{node_id} lineage={lineage[node_id]}"
        )
        assert lineage[node_id].get("source_ids") == ["auth_fixture"], (
            f"{node_id} lineage={lineage[node_id]}"
        )


def test_catalog_filter_preserves_an_unbounded_node_dimension(
    clean_runtime: str,
) -> None:
    """The catalog filter must not silently drop an unbounded dimension.

    A structural enricher declares its node surface UNBOUNDED, so it has no
    node clauses to filter. Recomputing allowed_record_kinds from clause
    counts alone drops "node" here and undoes the same fix applied when the
    policy was first derived — leaving the producer rejected as
    producer_record_kind_not_allowed against its own declared envelope, with
    its entire node output discarded.
    """
    from okg.substrate.library.enrichers.scholarly.latex_structural import (
        LatexStructuralEnricher,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            catalog_version_id = int(cur.fetchone()[0])

        policy = Publisher._enricher_policy(
            object(),
            LatexStructuralEnricher(),
            producer_id="_enricher:latex_structural",
        )
        policy.pop("_filter_unloaded_catalog_subtypes", False)
        filtered = Publisher._filter_policy_to_catalog_subtypes(
            conn,
            policy=policy,
            catalog_version_id=catalog_version_id,
        )

    # The catalog is empty, so every edge clause is filtered out — but the
    # unbounded node dimension must survive regardless.
    assert "node" in filtered["allowed_record_kinds"], (
        "the unbounded node dimension was dropped by the catalog filter"
    )
    # And the signature must NOT collapse to {}. Admission refuses an empty
    # signature as producer_signature_missing BEFORE it consults unbounded
    # dimensions, so collapsing here rejects the producer outright with its
    # declaration powerless to save it — the opposite of the graceful
    # degradation this filter exists to provide.
    assert filtered["output_signature"], (
        "signature collapsed to empty; admission will reject this producer "
        "as producer_signature_missing before it ever sees `unbounded`"
    )


class _UntrustedRootNodeEmitter(_DerivedNodeEmitter):
    """Hop 1 of the laundering chain: derives from an UNTRUSTED root.

    Declares itself deterministic (rank 3) and emits a node derived from
    ``auth:tainted``, whose source has no producer profile and is therefore
    untrusted. The question the two-hop test asks is whether hop 2 can then
    read this node and be admitted at rank 3.
    """

    name = "authority_untrusted_root_emitter"

    def enrich(self, conn, *, generation_id, incremental=None):
        del incremental
        from datetime import datetime, timezone

        from okg.substrate.library.enrichers._fact_emitters import (
            emit_node_fact,
        )

        emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id="auth:mid",
            subtype="auth_mid",
            attrs={"text": "from an untrusted root"},
            dedupe_key=f"_enricher:{self.name}:auth:mid:I",
            source_record_id={},
            source_revision={"generation_id": str(generation_id)},
            run_id=str(uuid4()),
            observed_at=datetime.now(timezone.utc),
            derived_from=["auth:tainted"],
            support_policy="all_members",
        )
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:mid",
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


def test_two_hop_chain_cannot_launder_an_untrusted_root(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Derived trust is bounded by the root, transitively.

    PACT formalize-trust-lattice-soundness. The property is MULTI-HOP and
    no single-hop test can falsify it: hop 1 (a rank-3 deterministic
    producer) reads an untrusted node and emits a derived node; hop 2 reads
    hop 1's node. If the substrate attributed hop 1's own declared label to
    its output, hop 2 would be admitted at rank 3 and an untrusted root
    would have been laundered trusted in two steps.

    The defence is that a derived node inherits its ROOT sources rather
    than the label of the producer that last touched it, so the untrusted
    root travels the whole chain.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)
        # An UNPROFILED source: no producer profile => untrusted (rank 0).
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("auth:tainted", "auth_src", {"text": "tainted"}, {}, {})],
            source="untrusted_fixture",
            run_id=str(uuid4()),
        )

    hop1 = Publisher(
        clean_runtime,
        "test",
        enrichers=[_UntrustedRootNodeEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test",
            generation_id=hop1["generation_id"],
        )

    Publisher(
        clean_runtime,
        "test",
        enrichers=[_DerivedNodeReader()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT status,
                   verdict->>'code',
                   verdict->'producer_policy'->'trust_lattice'
                     ->>'least_input_trust_label',
                   verdict->'producer_policy'->'trust_lattice'
                     ->'input_sources'
              FROM okg.admission_envelopes
             WHERE deployment_name = 'test'
               AND intent = 'derived_edge_emission'
               AND producer_id = '_enricher:authority_derived_reader'
            """
        )
        status, code, least_input, input_sources = cur.fetchone()

    assert status == "rejected", (
        f"hop 2 was {status} ({code}) — an untrusted root was laundered "
        f"trusted through one intermediate producer"
    )
    assert code == "derived_output_trust_exceeds_input"
    assert least_input == "untrusted"
    # Pin WHICH input made it untrusted. "untrusted" is also what a
    # missing profile resolves to, so without this the test would still
    # pass if hop 2 had resolved the intermediate ENRICHER id and simply
    # found no profile for it — the right verdict for the wrong reason.
    reported = {str(item.get("source")) for item in (input_sources or [])}
    assert "untrusted_fixture" in reported, reported
    assert "_enricher:authority_untrusted_root_emitter" not in reported, (
        reported
    )


class _OutOfSignatureNodeEmitter(_DerivedNodeEmitter):
    """Emits a node subtype outside its own declared write envelope.

    Declares ``auth_mid`` (inherited) but emits ``auth_dst``. Before
    node-emission admission this was governed by nothing at all: a
    node-only producer could emit any subtype and it projected
    unconditionally.
    """

    name = "authority_out_of_signature_node_emitter"

    def enrich(self, conn, *, generation_id, incremental=None):
        del incremental
        from datetime import datetime, timezone

        from okg.substrate.library.enrichers._fact_emitters import (
            emit_node_fact,
        )

        emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id="auth:rogue",
            subtype="auth_dst",  # NOT the declared emits_typed_subtype
            attrs={"text": "out of envelope"},
            dedupe_key=f"_enricher:{self.name}:auth:rogue:I",
            source_record_id={},
            source_revision={"generation_id": str(generation_id)},
            run_id=str(uuid4()),
            observed_at=datetime.now(timezone.utc),
            derived_from=["auth:src"],
            support_policy="all_members",
        )
        return EnrichResult(self.name, n_edges_emitted=0, n_edges_skipped=0)


def test_node_only_producer_is_admitted_against_its_signature(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A node-only producer is no longer governed by nothing.

    PACT admit-derived-node-emission. Derived admission was edge-only, so
    a producer emitting only nodes never had an admission envelope at all —
    it could emit any subtype, outside any declared signature, and the
    loader's rejected-run deny-list had nothing to act on.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_OutOfSignatureNodeEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, verdict->>'code'
              FROM okg.admission_envelopes
             WHERE deployment_name = 'test'
               AND intent = 'derived_node_emission'
               AND producer_id =
                   '_enricher:authority_out_of_signature_node_emitter'
            """
        )
        admission = cur.fetchone()

    assert admission is not None, (
        "a node-only producer must now get an admission envelope; before "
        "this change it got none and was governed by nothing"
    )
    status, code = admission
    assert status == "rejected"
    assert code == "producer_node_subtype_not_allowed"

    # The verdict is not the point — the point is that the rejected node
    # does NOT reach the served graph. The loader's deny-list matches on
    # (source, run_id), so this also pins that the envelope's run_id lines
    # up with the emitted facts'. If it did not, the envelope would be a
    # decoration and the gate would have zero security value.
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = checkout.query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = 'auth:rogue'",
            {},
        )
    finally:
        store.close()
    assert rows == [], (
        "the rejected node reached the served graph — the admission "
        "envelope did not gate projection"
    )


def test_in_signature_node_emission_is_admitted(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gate is not simply refusing every node emitter."""
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)

    Publisher(
        clean_runtime,
        "test",
        enrichers=[_DerivedNodeEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT status
              FROM okg.admission_envelopes
             WHERE deployment_name = 'test'
               AND intent = 'derived_node_emission'
               AND producer_id = '_enricher:authority_derived_emitter'
            """
        )
        status = cur.fetchone()

    assert status is not None
    assert status[0] in {"accepted", "published"}


class _RestatingEmitter(_DerivedNodeEmitter):
    """Re-emits an existing node as a new version OF ITSELF.

    The operator-promote path does this: a node is rewritten with new attrs
    while keeping the provenance it already had. Declaring its own id must
    resolve to the STORED prior version, not be treated as a cycle.

    It keeps ``_DerivedNodeEmitter``'s producer name deliberately. Since
    PR #788 the publisher applies a derived fact under the id of the
    producer that emitted it, so the derived-node ownership guard added in
    ee8a534b58 refuses a REPLACEMENT of ``auth:mid`` by a second, differently
    named enricher. Restating your own node is the scenario under test;
    cross-producer replacement is a different contract and is refused.
    """

    def enrich(self, conn, *, generation_id, incremental=None):
        del incremental
        from datetime import datetime, timezone

        from okg.substrate.library.enrichers._fact_emitters import (
            emit_node_fact,
        )

        emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id="auth:mid",
            subtype="auth_mid",
            attrs={"text": "restated"},
            dedupe_key=f"_enricher:{self.name}:auth:mid:restated:I",
            source_record_id={},
            source_revision={"generation_id": str(generation_id)},
            run_id=str(uuid4()),
            observed_at=datetime.now(timezone.utc),
            derived_from=["auth:mid"],
            support_policy="all_members",
        )
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:mid",
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


def test_restated_node_keeps_the_provenance_it_already_had(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Declaring a prior version of yourself resolves from the store.

    Without the stored-version fallback this reads as an unresolvable
    self-cycle, and a node re-emitted with new attrs would silently lose
    lineage it legitimately held — turning a provenance fix into a
    provenance regression for the operator-promote path.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)

    first = Publisher(
        clean_runtime,
        "test",
        enrichers=[_DerivedNodeEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test",
            generation_id=first["generation_id"],
        )

    second = Publisher(
        clean_runtime,
        "test",
        enrichers=[_RestatingEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(second["generation_id"])
        rows = checkout.query(
            """
            SELECT attrs->'nomos_source_lineage' AS lineage,
                   attrs->>'text' AS text
              FROM okg.graph_nodes
             WHERE node_id = 'auth:mid'
            """,
            {},
        )
    finally:
        store.close()

    lineage = rows[0]["lineage"] or {}
    assert rows[0]["text"] == "restated", "the restated attrs should have landed"
    assert lineage.get("status") == "ok", lineage
    assert lineage.get("source_ids") == ["auth_fixture"], lineage


class _ForeignRestatingEmitter(_RestatingEmitter):
    """Rewrites another producer's derived node under its own identity."""

    name = "authority_foreign_restating_emitter"


def test_a_second_producer_cannot_replace_a_derived_node(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cross-producer replacement of a derived node id is refused.

    The sibling test above restates a node under its OWN producer. This
    pins the other half of that contract, which the ownership guard added
    in ee8a534b58 enforces and which only bites now that the publisher
    writes a derived fact under the id of the producer that emitted it: a
    derived node id is one authoritative row, not an implicit merge point.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)

    first = Publisher(
        clean_runtime,
        "test",
        enrichers=[_DerivedNodeEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()
    assert first["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test",
            generation_id=first["generation_id"],
        )

    with pytest.raises(SubstrateError) as excinfo:
        Publisher(
            clean_runtime,
            "test",
            enrichers=[_ForeignRestatingEmitter()],
            catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
        ).cycle()

    assert "already owned by a different producer" in str(excinfo.value)
    collisions = (excinfo.value.data or {}).get("collisions") or []
    assert [item["node_id"] for item in collisions] == ["auth:mid"], collisions


class _UnresolvableDeclarationEmitter(_DerivedNodeEmitter):
    """Declares an input that does not exist in the graph."""

    name = "authority_unresolvable_emitter"

    def enrich(self, conn, *, generation_id, incremental=None):
        del incremental
        from datetime import datetime, timezone

        from okg.substrate.library.enrichers._fact_emitters import (
            emit_node_fact,
        )

        emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id="auth:mid",
            subtype="auth_mid",
            attrs={"text": "unresolvable"},
            dedupe_key=f"_enricher:{self.name}:auth:mid:I",
            source_record_id={},
            source_revision={"generation_id": str(generation_id)},
            run_id=str(uuid4()),
            observed_at=datetime.now(timezone.utc),
            derived_from=["auth:src", "auth:does-not-exist"],
            support_policy="all_members",
        )
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:mid",
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


def test_unresolvable_declared_input_fails_the_whole_envelope_closed(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One unresolvable input poisons the envelope; it does not narrow.

    The dangerous failure would be silently narrowing to the inputs that
    DID resolve — that would quietly upgrade a node whose real provenance
    is partly unknown to a clean, fully-trusted envelope. It must stay
    malformed instead.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_UnresolvableDeclarationEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = checkout.query(
            """
            SELECT attrs->'nomos_source_lineage' AS lineage
              FROM okg.graph_nodes
             WHERE node_id = 'auth:mid'
            """,
            {},
        )
    finally:
        store.close()

    lineage = rows[0]["lineage"] or {}
    assert lineage.get("status") == "malformed", lineage


def test_rejection_summary_is_not_carried_across_cycles(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The census is per-cycle, not cumulative for a reused Publisher.

    A terminal envelope is skipped on re-run, so without a per-cycle reset
    the second cycle emits no verdict of its own yet still reports the
    first cycle's rejection — a count that looks current and is entirely
    historical.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)

    publisher = Publisher(
        clean_runtime,
        "test",
        enrichers=[_RejectedEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    )
    first = publisher.cycle()
    second = publisher.cycle()

    assert first["derived_rejections"]["rejected_producer_count"] == 1
    assert second["derived_rejections"]["rejected_producer_count"] == 0


class _UndeclaredNodeEmitter(_DerivedNodeEmitter):
    """Same as the emitter above, but declares no input provenance."""

    name = "authority_undeclared_emitter"

    def enrich(self, conn, *, generation_id, incremental=None):
        del incremental
        from datetime import datetime, timezone

        from okg.substrate.library.enrichers._fact_emitters import (
            emit_node_fact,
        )

        emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id="auth:mid",
            subtype="auth_mid",
            attrs={"text": "derived middle"},
            dedupe_key=f"_enricher:{self.name}:auth:mid:I",
            source_record_id={},
            source_revision={"generation_id": str(generation_id)},
            run_id=str(uuid4()),
            observed_at=datetime.now(timezone.utc),
            support_policy="all_members",
            # deliberately no derived_from
        )
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:mid",
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


def test_undeclared_derived_node_provenance_is_refused_before_insert(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provenance is required at the write boundary, never invented.

    An enricher that declares nothing about what it derived a node from is
    rejected before the node fact enters the ledger. The substrate does not
    infer support from graph adjacency, even though ``auth:mid`` would sit next
    to a trusted ``auth:src``; doing so would launder trust.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)

    Publisher(
        clean_runtime,
        "test",
        enrichers=[_UndeclaredNodeEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.node_facts
             WHERE source = '_enricher:authority_undeclared_emitter'
               AND node_id = 'auth:mid'
            """
        )
        fact_count = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT last_status
              FROM okg.enricher_progress
             WHERE deployment_name = 'test'
               AND enricher_name = 'authority_undeclared_emitter'
            """
        )
        last_status = cur.fetchone()[0]

    assert fact_count == 0
    assert last_status == "failed:ValueError"


def _seed_derived_chain_catalog_and_nodes(conn: psycopg.Connection) -> None:
    """Catalog + trusted source nodes for the derived-input-trust chain.

    ``auth:src`` and ``auth:dst`` come from a rank-3 source; ``auth:mid``
    is left for the cycle-1 enricher to derive.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = int(cur.fetchone()[0])
        cur.executemany(
            """
            INSERT INTO okg.subtypes (name, metadata, catalog_version_id)
            VALUES (%s, '{}'::jsonb, %s)
            """,
            [
                ("auth_src", catalog_version_id),
                ("auth_mid", catalog_version_id),
                ("auth_dst", catalog_version_id),
            ],
        )
        cur.executemany(
            """
            INSERT INTO okg.subtype_ancestors
              (catalog_version_id, subtype_name, ancestor_name, depth)
            VALUES (%s, %s, %s, 0)
            ON CONFLICT DO NOTHING
            """,
            [
                (catalog_version_id, "auth_src", "auth_src"),
                (catalog_version_id, "auth_mid", "auth_mid"),
                (catalog_version_id, "auth_dst", "auth_dst"),
            ],
        )
        cur.executemany(
            """
            INSERT INTO okg.narrowings
              (parent_subtype, edge_type, child_subtype, catalog_version_id)
            VALUES (%s, 'auth_allows', %s, %s)
            """,
            [
                ("auth_src", "auth_mid", catalog_version_id),
                ("auth_mid", "auth_dst", catalog_version_id),
            ],
        )
    store = ChronosGraphStore.from_connection(conn)
    for node_id, subtype in (("auth:src", "auth_src"), ("auth:dst", "auth_dst")):
        store.apply_facts(
            [NodeFact(node_id, subtype, {"text": node_id}, {}, {})],
            source="auth_fixture",
            run_id=str(uuid4()),
        )
    _materialize_input_profile(
        conn,
        producer_id="auth_fixture",
        trust_label="repo-local-trusted",
    )


def test_cold_publish_admits_same_cycle_node_and_edge(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #841: the first cycle admits an edge to its pending node."""
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_derived_chain_catalog_and_nodes(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.graph_nodes "
                "WHERE node_id = 'auth:mid'"
            )
            assert int(cur.fetchone()[0]) == 0

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_ColdSameCycleNodeEmitter()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test",
            generation_id=out["generation_id"],
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status,
                       verdict->>'code',
                       verdict->'producer_policy'->'matched_clause_ids'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_enricher:authority_cold_same_cycle_emitter'
                """
            )
            admission = cur.fetchone()

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        node_rows = checkout.query(
            """
            SELECT subtype
              FROM okg.graph_nodes
             WHERE node_id = 'auth:mid'
            """,
            {},
        )
    finally:
        store.close()

    live_edges = _graph_edge_count(
        clean_runtime,
        out["generation_id"],
        src="auth:src",
        edge_type="auth_allows",
        dst="auth:mid",
    )

    assert out["status"] == "published"
    assert out["derived_rejections"]["rejected_producer_count"] == 0
    assert out["derived_rejections"]["discarded_record_count"] == 0
    assert admission == (
        "published",
        None,
        [
            "_enricher:authority_cold_same_cycle_emitter:"
            "auth_src:auth_allows:auth_mid"
        ],
    )
    assert node_rows == [{"subtype": "auth_mid"}]
    assert live_edges == 1


class _CountingRegexEntityExtractor(RegexEntityExtractor):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.calls = 0

    def extract(self, text):
        self.calls += 1
        return super().extract(text)


def test_publisher_admits_and_projects_matching_enricher_output(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_AllowedEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test",
            generation_id=out["generation_id"],
        )
        with conn.cursor() as cur:
            live_edges = _graph_edge_count(
                clean_runtime,
                out["generation_id"],
                src="auth:src",
                edge_type="auth_allows",
                dst="auth:dst",
            )
            # Scoped to the EMISSION intent: since PR #788 the publisher
            # also records its own `publisher_derived:*` application
            # envelope under the emitting producer's id, so a query keyed
            # on producer_id alone matches two rows (#1534).
            cur.execute(
                """
                SELECT status, producer_kind,
                       check_summary->'producer_policy'->>'status',
                       check_summary->'producer_policy'->>'profile_fingerprint'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_enricher:authority_allowed'
                """
            )
            *admission, admitted_fingerprint = cur.fetchone()
            admission = tuple(admission)
            # The profile the admission actually cited, not whichever row
            # for this producer the scan happened to return first.
            cur.execute(
                """
                SELECT status
                  FROM okg.v_producer_profiles
                 WHERE deployment_name = 'test'
                   AND producer_id = '_enricher:authority_allowed'
                   AND profile_fingerprint = %s
                """,
                (admitted_fingerprint,),
            )
            profile_status = cur.fetchone()[0]

    assert out["status"] == "published"
    assert live_edges == 1
    assert admission == ("published", "enricher", "active")
    assert profile_status == "active"


def test_publisher_rejects_out_of_signature_enricher_output(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_RejectedEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            live_edges = _graph_edge_count(
                clean_runtime,
                out["generation_id"],
                src="auth:src",
                edge_type="auth_denies",
                dst="auth:dst",
            )
            cur.execute(
                """
                SELECT status, verdict->>'code'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_enricher:authority_rejected'
                """
            )
            admission = cur.fetchone()

    assert out["status"] == "published"
    assert live_edges == 0
    assert admission == ("rejected", "producer_edge_triple_not_allowed")


def test_derived_output_cannot_exceed_input_trust(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(
            conn,
            src_source="less_trusted_input",
            dst_source="less_trusted_input",
        )
        _materialize_input_profile(
            conn,
            producer_id="less_trusted_input",
            trust_label="less-trusted",
        )

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_AllowedEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            live_edges = _graph_edge_count(
                clean_runtime,
                out["generation_id"],
                src="auth:src",
                edge_type="auth_allows",
                dst="auth:dst",
            )
            cur.execute(
                """
                SELECT status, verdict->>'code',
                       verdict #>> '{producer_policy,trust_lattice,
                                      least_input_trust_label}',
                       verdict #>> '{producer_policy,trust_lattice,
                                      output_trust_label}'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_enricher:authority_allowed'
                """
            )
            admission = cur.fetchone()

    assert out["status"] == "published"
    assert live_edges == 0
    assert admission == (
        "rejected",
        "derived_output_trust_exceeds_input",
        "less-trusted",
        "derived-deterministic-trusted",
    )


def test_multi_input_derived_trust_uses_lowest_input(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(
            conn,
            src_source="trusted_input",
            dst_source="less_trusted_input",
        )
        _materialize_input_profile(
            conn,
            producer_id="trusted_input",
            trust_label="repo-local-trusted",
        )
        _materialize_input_profile(
            conn,
            producer_id="less_trusted_input",
            trust_label="less-trusted",
        )

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_AllowedEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Scoped to the EMISSION intent. The publisher also records
            # its own `publisher_derived:*` application envelope under this
            # producer's id, so producer_id alone matches TWO rows and
            # fetchone() returns whichever the heap scan reaches first. That
            # made this test fail intermittently on dev with
            # ('accepted', None, None) -- the publisher_derived row's exact
            # shape -- which reads like a trust-resolution bug and is not
            # one (#1534, #1545).
            cur.execute(
                """
                SELECT status, verdict->>'code',
                       verdict #>> '{producer_policy,trust_lattice,
                                      least_input_trust_label}',
                       (verdict #> '{producer_policy,trust_lattice,
                                     input_trust_labels}')::text
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_enricher:authority_allowed'
                """
            )
            admission = cur.fetchone()

    assert out["status"] == "published"
    assert admission[0:3] == (
        "rejected",
        "derived_output_trust_exceeds_input",
        "less-trusted",
    )
    assert "less-trusted" in admission[3]
    assert "repo-local-trusted" in admission[3]


def test_derived_trust_verdict_is_read_from_the_edge_emission_envelope(
    clean_runtime: str,
) -> None:
    """The same producer carries more than one admission envelope, and only
    one of them holds the trust verdict.

    In THIS fixture a cycle that emits a derived edge writes two rows for
    ``_enricher:authority_allowed``: ``intent = 'derived_edge_emission'``
    (publisher.py:2236 -- rejected, carrying producer_policy.trust_lattice) and
    ``intent = 'publisher_derived:pre_view'`` (publisher.py:8371 -- accepted,
    no verdict code). Both carry the same ``deployment_name`` and
    ``producer_id``, so a read keyed on that pair alone matches both, and
    ``fetchone()`` returns whichever row the chosen plan yields first. Two more
    intents can collide the same way and are merely inert here:
    ``derived_node_emission`` (publisher.py:2039) because these enrichers emit
    no derived nodes, and ``publisher_derived:view`` (publisher.py:10267)
    because the view stage finds no facts above the pre_view watermark. So the
    count below is "two in this fixture", not "two per cycle".

    That is not hypothetical. On dev at 3c77fc15c
    ``test_multi_input_derived_trust_uses_lowest_input`` read the pre_view row
    and failed with ``('accepted', None, None)`` in substrate-db-linux (run
    33780127412), while the trust rejection was recorded correctly the whole
    time; the same file passed alone and had passed on the merged branch's own
    head. Nothing in the publish path changed -- git log b700ab98..3c77fc15c
    touches no file under src/okg/substrate/publisher/ or
    src/okg/substrate/producer_authority.py. The merge only changed which
    modules share an xdist worker, and with them the plan for a two-row read.

    WHAT THIS TEST DOES AND DOES NOT DO. It pins the HAZARD, not its every use:
    that the ambiguous pair really matches two rows, that the decoy really is
    the ``('accepted', None, None)`` tuple dev reported, and that the
    intent-qualified read matches exactly one row and returns the rejection. It
    does NOT catch a sibling test that drops its own ``intent`` filter -- only
    that test's own assertion can do that. If this test ever reds because the
    decoy is gone, the qualification in the sibling tests has become
    unnecessary rather than wrong.

    Deliberately NOT pinned by ordering: asserting that the two orderings
    disagree would make this test depend on heap layout, which is the very
    thing under indictment.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(
            conn,
            src_source="trusted_input",
            dst_source="less_trusted_input",
        )
        _materialize_input_profile(
            conn,
            producer_id="trusted_input",
            trust_label="repo-local-trusted",
        )
        _materialize_input_profile(
            conn,
            producer_id="less_trusted_input",
            trust_label="less-trusted",
        )

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_AllowedEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    verdict_projection = """
        SELECT status, verdict->>'code',
               verdict #>> '{producer_policy,trust_lattice,
                              least_input_trust_label}'
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            # admission-envelope-lint: allow issue #1556 -- this read MEANS
            # to match every envelope of the producer; it is the census the
            # rest of the test argues from.
            cur.execute(
                """
                SELECT intent, status
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND producer_id = '_enricher:authority_allowed'
                 ORDER BY intent
                """
            )
            intents = cur.fetchall()

            # admission-envelope-lint: allow issue #1556 -- reproduces the
            # unqualified read on purpose, to show it is ambiguous.
            cur.execute(
                verdict_projection
                + """
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND producer_id = '_enricher:authority_allowed'
                """
            )
            unqualified = sorted(cur.fetchall())

            cur.execute(
                verdict_projection
                + """
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_enricher:authority_allowed'
                """
            )
            qualified = cur.fetchall()

    assert out["status"] == "published"
    # A LIST, not a dict: a dict would silently collapse a second row carrying
    # the same intent, which is the nondeterminism this change is about.
    assert intents == [
        ("derived_edge_emission", "rejected"),
        ("publisher_derived:pre_view", "accepted"),
    ]
    # The unqualified read really is ambiguous, and the decoy really is the
    # tuple dev failed with.
    assert unqualified == [
        ("accepted", None, None),
        ("rejected", "derived_output_trust_exceeds_input", "less-trusted"),
    ]
    # Qualified by intent it matches exactly one row, so no ordering can change
    # what fetchone() returns.
    assert qualified == [
        ("rejected", "derived_output_trust_exceeds_input", "less-trusted"),
    ]


def _seed_unrelated_envelopes(
    cur: psycopg.Cursor,
    *,
    producer_id: str,
) -> None:
    """Rows a correct lookup must not pick up.

    Two decoys carry the producer id under a DIFFERENT deployment (so a
    lookup that forgets the deployment matches both), and two more name a
    different producer in this deployment under each emission intent.
    Together they make the envelope table look like a long-lived one
    rather than a freshly truncated fixture, so "matches exactly one row"
    cannot hold by accident.
    """
    cur.execute(
        """
        INSERT INTO okg.admission_envelopes
          (deployment_name, producer_id, producer_kind, intent,
           admission_mode, status, idempotency_key, verdict)
        VALUES
          ('ambiguous', %(producer_id)s, 'enricher',
           'derived_edge_emission', 'fast_track', 'accepted',
           'decoy-ambiguous-1', '{}'::jsonb),
          ('ambiguous', %(producer_id)s, 'enricher',
           'derived_edge_emission', 'fast_track', 'rejected',
           'decoy-ambiguous-2', '{}'::jsonb),
          ('test', '_enricher:authority_decoy', 'enricher',
           'derived_node_emission', 'fast_track', 'accepted',
           'decoy-other-intent', '{}'::jsonb),
          ('test', '_enricher:authority_decoy', 'enricher',
           'derived_edge_emission', 'fast_track', 'accepted',
           'decoy-other-producer', '{}'::jsonb)
        """,
        {"producer_id": producer_id},
    )


def _unqualified_first_envelope(
    cur: psycopg.Cursor,
    producer_id: str,
    *,
    order_by: str,
) -> tuple:
    """What the pre-#1556 lookup shape returns, under a CHOSEN row order.

    Kept in ONE place as the thing the ordering test compares against.
    Real code must never read this way -- that is what
    ``fetch_admission_envelope`` and the CI gate are for.
    """
    # admission-envelope-lint: allow issue #1556 -- reproduces the
    # unqualified read on purpose, so the test can show it is ambiguous.
    cur.execute(
        f"""
        SELECT status,
               verdict->>'code',
               verdict #>> '{{producer_policy,trust_lattice,
                              least_input_trust_label}}'
          FROM okg.admission_envelopes
         WHERE deployment_name = 'test'
           AND producer_id = %s
         ORDER BY {order_by}
         LIMIT 1
        """,
        (producer_id,),
    )
    return tuple(cur.fetchone())


def test_derived_admission_lookup_is_stable_across_envelope_row_order(
    clean_runtime: str,
) -> None:
    """The verdict reads the same in EITHER row order (#1556).

    ``test_derived_trust_verdict_is_read_from_the_edge_emission_envelope``
    above pins the hazard: two rows match a producer-keyed read, and one
    of them is the ``('accepted', None, None)`` tuple dev failed with in
    run 33780127412. This pins what follows from it. Both rows are equal
    on every column of the index such a read uses, so their order is
    their heap position of the moment; the two orders are taken here
    explicitly, by ctid, on a table that already holds unrelated
    envelopes.

    The unqualified read answers DIFFERENTLY in the two orders -- that is
    the defect, stated as an executable fact rather than a story about a
    CI run. Read through ``fetch_admission_envelope`` it does not move,
    and a call that would match both rows raises instead of choosing one.

    No ordering is FORCED on the stored rows: making a test rearrange the
    heap would make it depend on the very thing under indictment.
    """
    producer = "_enricher:authority_allowed"
    columns = (
        "status",
        "verdict->>'code'",
        "verdict #>> '{producer_policy,trust_lattice,"
        "least_input_trust_label}'",
    )
    rejection = (
        "rejected",
        "derived_output_trust_exceeds_input",
        "less-trusted",
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(
            conn,
            src_source="trusted_input",
            dst_source="less_trusted_input",
        )
        _materialize_input_profile(
            conn,
            producer_id="trusted_input",
            trust_label="repo-local-trusted",
        )
        _materialize_input_profile(
            conn,
            producer_id="less_trusted_input",
            trust_label="less-trusted",
        )

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_AllowedEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()
    assert out["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            _seed_unrelated_envelopes(cur, producer_id=producer)

            unqualified = [
                _unqualified_first_envelope(cur, producer, order_by=order)
                for order in ("ctid", "ctid DESC")
            ]
            qualified = [
                fetch_admission_envelope(
                    cur,
                    columns=columns,
                    intent="derived_edge_emission",
                    deployment_name="test",
                    producer_id=producer,
                    order_by=order,
                )
                for order in ("ctid", "ctid DESC")
            ]

            # A call that would match both rows refuses rather than
            # returning an arbitrary one of them.
            with pytest.raises(AmbiguousAdmissionLookup):
                fetch_admission_envelope(
                    cur,
                    columns=columns,
                    intent="derived_edge_emission",
                    producer_id=producer,
                    deployment_name="ambiguous",
                )
            # And a call that names no envelope is refused outright.
            with pytest.raises(ValueError):
                fetch_admission_envelope(
                    cur,
                    columns=columns,
                    intent="",
                    deployment_name="test",
                    producer_id=producer,
                )

    assert sorted(unqualified) == [
        ("accepted", None, None),
        rejection,
    ], unqualified
    assert unqualified[0] != unqualified[1], (
        "the two row orders returned the same envelope, so this test "
        f"never exercised the ambiguity: {unqualified}"
    )
    assert qualified == [rejection, rejection]


def test_derived_output_with_missing_input_profile_fails_closed(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(
            conn,
            src_source="missing_input_profile",
            dst_source="missing_input_profile",
        )

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_AllowedEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            live_edges = _graph_edge_count(
                clean_runtime,
                out["generation_id"],
                src="auth:src",
                edge_type="auth_allows",
                dst="auth:dst",
            )
            cur.execute(
                """
                SELECT status, verdict->>'code',
                       verdict #>> '{producer_policy,trust_lattice,
                                      least_input_trust_label}',
                       (verdict #> '{producer_policy,trust_lattice,
                                     input_sources}')::text
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_enricher:authority_allowed'
                """
            )
            admission = cur.fetchone()

    assert out["status"] == "published"
    assert live_edges == 0
    assert admission[0:3] == (
        "rejected",
        "derived_output_trust_exceeds_input",
        "untrusted",
    )
    assert "missing_profile_treated_untrusted" in admission[3]
    assert "missing_input_profile" in admission[3]


def test_derived_output_without_discoverable_input_trust_fails_closed(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(
            conn,
            src_source=None,
            dst_source=None,
        )

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_LegacyNoLineageEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            live_edges = _graph_edge_count(
                clean_runtime,
                out["generation_id"],
                src="auth:src",
                edge_type="auth_allows",
                dst="auth:dst",
            )
            cur.execute(
                """
                SELECT status, verdict->>'code',
                       verdict #>> '{producer_policy,trust_lattice,reason}',
                       verdict #>> '{producer_policy,trust_lattice,status}'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_enricher:authority_legacy_no_lineage'
                """
            )
            admission = cur.fetchone()

    assert out["status"] == "published"
    assert live_edges == 0
    assert admission == (
        "rejected",
        "derived_output_trust_inputs_missing",
        "no_profiled_inputs",
        "rejected",
    )


def test_endpoint_trust_lookup_uses_active_chronos_branch(
    clean_runtime: str,
) -> None:
    branch = "experiment:trust"
    source = "_enricher:branch_lookup"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        store = ChronosGraphStore.from_connection(conn)
        try:
            store.ensure_branch(
                branch,
                from_branch="default",
                metadata={"kind": "okg_branch", "role": "experiment"},
            )
            store.apply_facts(
                [
                    NodeFact(
                        "auth:src",
                        "auth_src",
                        {"text": "branch source"},
                        {},
                        {},
                    ),
                    NodeFact(
                        "auth:dst",
                        "auth_dst",
                        {"text": "branch destination"},
                        {},
                        {},
                    ),
                ],
                branch=branch,
                source="branch_input",
                run_id=str(uuid4()),
            )
        finally:
            store.close()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs,
                   provenance, confidence, source,
                   source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES (%s, 'auth:src', 'auth:dst', 'auth_allows',
                        '{}'::jsonb, 'derived_deterministic', 1.0, %s,
                        '{}'::jsonb, '{}'::jsonb,
                        now(), %s, %s, 'I')
                RETURNING fact_id
                """,
                (
                    mint_edge_id("auth:src", "auth_allows", "auth:dst"),
                    source,
                    f"{source}:branch:{uuid4()}:I",
                    str(uuid4()),
                ),
            )
            fact_id = int(cur.fetchone()[0])

        assert Publisher._chronos_endpoint_source_ids_for_trust_check(
            conn,
            branch_name=branch,
            source=source,
            fact_id_min=fact_id,
            fact_id_max=fact_id,
        ) == ({"branch_input"}, False)


def test_endpoint_trust_fallback_reads_lineage_not_the_writing_producer(
    clean_runtime: str,
) -> None:
    """The lower fallback must resolve ROOTS, not the last writer.

    Issue #1545, defect 3. When an edge fact carries no lineage of its
    own, trust is resolved from its endpoint nodes. That lookup used to
    read `graph_nodes.source`, which for a derived node is the enricher
    that emitted it -- so an enricher's own declared label came back as
    the label of its own input, which is the #1534 laundering shape one
    layer down.

    Here the endpoint node is WRITTEN by `relabelling_writer` but
    carries a lineage envelope naming the real root `untrusted_fixture`.
    A source-reading fallback answers `relabelling_writer`; a
    lineage-reading one answers `untrusted_fixture`.
    """
    source = "_enricher:lineage_fallback"
    writer = "relabelling_writer"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        store = ChronosGraphStore.from_connection(conn)
        try:
            store.apply_facts(
                [
                    NodeFact(
                        "auth:src",
                        "auth_src",
                        attrs_with_lineage(
                            {"text": "derived from an untrusted root"},
                            normalize_source_lineage(
                                source_id="untrusted_fixture",
                            ),
                        ),
                        {},
                        {},
                    ),
                    NodeFact(
                        "auth:dst",
                        "auth_dst",
                        attrs_with_lineage(
                            {"text": "derived from an untrusted root"},
                            normalize_source_lineage(
                                source_id="untrusted_fixture",
                            ),
                        ),
                        {},
                        {},
                    ),
                ],
                source=writer,
                run_id=str(uuid4()),
                # Same flag the publisher's re-application uses: the rows
                # keep the lineage they arrive with instead of being
                # restamped with the writing producer's id (#1534).
                derived_reapplication=True,
            )
        finally:
            store.close()
        fact_id = _insert_bare_edge_fact(conn, source=source)

        resolved, malformed = (
            Publisher._chronos_endpoint_source_ids_for_trust_check(
                conn,
                branch_name="default",
                source=source,
                fact_id_min=fact_id,
                fact_id_max=fact_id,
            )
        )

    assert resolved == {"untrusted_fixture"}, resolved
    assert writer not in resolved, resolved
    assert malformed is False


def test_endpoint_trust_fallback_fails_closed_on_unreadable_lineage(
    clean_runtime: str,
) -> None:
    """An endpoint whose lineage cannot be read is untrusted, not absent.

    Issue #1545. A node stamped with a lineage envelope that
    normalization rejects tells us its provenance is BROKEN, which is a
    strictly worse thing to know than knowing nothing. The fallback
    reports it so the caller can inject the synthetic untrusted input,
    instead of silently contributing no source and letting the edge be
    judged on whatever other endpoint happened to be readable.
    """
    source = "_enricher:malformed_lineage_fallback"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        store = ChronosGraphStore.from_connection(conn)
        try:
            store.apply_facts(
                [
                    NodeFact(
                        "auth:src",
                        "auth_src",
                        _BROKEN_LINEAGE_ATTRS,
                        {},
                        {},
                    ),
                    NodeFact(
                        "auth:dst",
                        "auth_dst",
                        _BROKEN_LINEAGE_ATTRS,
                        {},
                        {},
                    ),
                ],
                source="broken_writer",
                run_id=str(uuid4()),
                derived_reapplication=True,
            )
        finally:
            store.close()
        fact_id = _insert_bare_edge_fact(conn, source=source)

        resolved, malformed = (
            Publisher._chronos_endpoint_source_ids_for_trust_check(
                conn,
                branch_name="default",
                source=source,
                fact_id_min=fact_id,
                fact_id_max=fact_id,
            )
        )

    assert malformed is True
    assert resolved == set(), resolved


#: A lineage envelope that normalization REJECTS: it declares a source
#: entry with no source id at all, so it names a provenance it cannot
#: resolve. Distinct from having no lineage attribute, which is merely
#: unknown provenance.
_BROKEN_LINEAGE_ATTRS = {
    LINEAGE_ATTR: {
        "schema": "okg.nomos.source_lineage.v1",
        "status": "ok",
        "sources": [{"note": "declared a source with no id"}],
    },
    "text": "broken",
}


def _insert_bare_edge_fact(
    conn: psycopg.Connection,
    *,
    source: str,
) -> int:
    """One auth:src -> auth:dst edge fact carrying NO lineage attribute."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.edge_facts
              (edge_id, src, dst, edge_type, attrs,
               provenance, confidence, source,
               source_record_id, source_revision,
               observed_at, dedupe_key, run_id, op)
            VALUES (%s, 'auth:src', 'auth:dst', 'auth_allows',
                    '{}'::jsonb, 'derived_deterministic', 1.0, %s,
                    '{}'::jsonb, '{}'::jsonb,
                    now(), %s, %s, 'I')
            RETURNING fact_id
            """,
            (
                mint_edge_id("auth:src", "auth_allows", "auth:dst"),
                source,
                f"{source}:{uuid4()}:I",
                str(uuid4()),
            ),
        )
        return int(cur.fetchone()[0])


def test_deployment_stack_loads_manifest_extractor_authority(tmp_path) -> None:
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        """
name: authority-test
derived_producers:
  extractors:
    ticket:
      trust_label: derived-deterministic-trusted
      admission_mode: fast_track
      authority_scope:
        source_family: _extractor:ticket
      allowed_record_kinds: [edge]
      output_signature:
        edges:
          - src_subtype: page
            edge_type: mentions
            dst_subtype: ticket
            clause_id: allow_ticket_mentions
      policy_clauses:
        - id: allow_ticket_mentions
          effect: allow
          record_kind: edge
          triple: [page, mentions, ticket]
"""
    )

    policies = load_derived_producer_policies(deployment)

    assert sorted(policies) == ["_extractor:ticket"]
    assert policies["_extractor:ticket"]["producer_kind"] == "extractor"
    assert (
        policies["_extractor:ticket"]["source_ref"]["producer_id"]
        == "_extractor:ticket"
    )


def test_okg_workspace_declares_live_change_slug_extractor() -> None:
    deployment = Path(__file__).resolve().parents[2] / (
        "deployments/okg-workspace"
    )

    extractor = load_projection_extractor(deployment)
    source_subtypes = load_projection_extractor_source_subtypes(deployment)
    routes = load_projection_routes(deployment)
    policies = load_derived_producer_policies(deployment)

    assert extractor is not None
    assert source_subtypes == ["entity_mention"]
    assert extractor.extract("add-derived-producer-authority")[0].type == (
        "change_slug"
    )
    assert routes is not None
    assert [
        (r.entity_type, r.src_subtype, r.edge_type, r.dst_template)
        for r in routes
    ] == [("change_slug", "entity_mention", "mentions", "{resolved}")]
    assert (
        policies["_extractor:change_slug"]["policy_clauses"][0]["triple"]
        == ["entity_mention", "mentions", "open_spec_change"]
    )


def test_publisher_materializes_declared_extractor_profile_without_output(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_extractor_catalog_and_facts(conn)

    out = Publisher(
        clean_runtime,
        "test",
        derived_producer_policies={
            "_extractor:ticket": _ticket_extractor_policy(),
        },
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, output_signature->'edges'->0->>'clause_id'
              FROM okg.producer_profiles
             WHERE deployment_name = 'test'
               AND producer_id = '_extractor:ticket'
               -- DECLARED rows only. `updated_at DESC` over an unfiltered
               -- set is the very ordering issue #1545 is about: a
               -- machine-minted row written later would win it and this
               -- test would assert on authority nobody declared.
               AND NOT source_ref ? 'profile_origin'
             ORDER BY updated_at DESC
             LIMIT 1
            """
        )
        profile = cur.fetchone()

    assert out["status"] in {"no_op", "published"}
    assert profile == ("active", "allow_ticket_mentions")


def test_rule_level_enricher_source_falls_back_to_parent_profile(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_RuleSourceEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT producer_id, source,
                       check_summary->'producer_lookup'
                         ->>'source_family_fallback'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND source = '_enricher:authority_family:rule_one'
                """
            )
            admission = cur.fetchone()

    assert out["status"] == "published"
    assert admission == (
        "_enricher:authority_family",
        "_enricher:authority_family:rule_one",
        "true",
    )


def test_dbos_mock_records_derived_verdict_trace_for_accept_and_reject(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.deployment.testing import mock_dbos_context

    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)

    with mock_dbos_context() as mock:
        out = Publisher(
            clean_runtime,
            "test",
            coordination_mode="dbos",
            enrichers=[_AllowedEnricher(), _RejectedEnricher()],
            catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
        ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test",
            generation_id=out["generation_id"],
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT producer_id, status,
                       workflow_ref->>'workflow_id'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND producer_kind = 'enricher'
                   AND intent IN (
                     'derived_edge_emission', 'derived_node_emission'
                   )
                 ORDER BY producer_id
                """
            )
            envelopes = cur.fetchall()
            cur.execute(
                """
                SELECT e.producer_id, r.ref_kind, r.record_count
                  FROM okg.admission_envelopes e
                  JOIN okg.admission_record_refs r
                    ON r.admission_id = e.admission_id
                 WHERE e.deployment_name = 'test'
                   AND e.producer_kind = 'enricher'
                   AND e.intent IN (
                     'derived_edge_emission', 'derived_node_emission'
                   )
                 ORDER BY e.producer_id, r.ref_kind
                """
            )
            refs = cur.fetchall()
            live_allowed = _graph_edge_count(
                clean_runtime,
                out["generation_id"],
                src="auth:src",
                edge_type="auth_allows",
                dst="auth:dst",
            )
            live_rejected = _graph_edge_count(
                clean_runtime,
                out["generation_id"],
                src="auth:src",
                edge_type="auth_denies",
                dst="auth:dst",
            )

    verdicts = mock.events["derived_producer_verdicts"]["events"]
    by_producer = {event["producer_id"]: event for event in verdicts}

    assert out["status"] == "published"
    assert envelopes == [
        ("_enricher:authority_allowed", "published", "mock-wf-1"),
        ("_enricher:authority_rejected", "rejected", "mock-wf-1"),
    ]
    assert refs == [
        ("_enricher:authority_allowed", "edge_fact_range", 1),
        ("_enricher:authority_rejected", "edge_fact_range", 1),
    ]
    assert live_allowed == 1
    assert live_rejected == 0
    assert by_producer["_enricher:authority_allowed"]["status"] == "accepted"
    assert by_producer["_enricher:authority_rejected"]["status"] == "rejected"
    assert mock.events["derived_producer_verdict"]["status"] == "rejected"


def test_publisher_admits_and_projects_matching_extractor_output(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_extractor_catalog_and_facts(conn)
        _materialize_ticket_extractor_profile(conn)

    out = Publisher(
        clean_runtime,
        "test",
        extractor=RegexEntityExtractor([("ticket", r"(TICKET-\d+)")]),
        routes=[
            LinkerRoute(
                entity_type="ticket",
                src_subtype=None,
                edge_type="mentions",
                dst_template="ticket:{value}",
            ),
        ],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_generation_feedback(
            conn,
            deployment_name="test",
            generation_id=out["generation_id"],
        )
        with conn.cursor() as cur:
            live_edges = _graph_edge_count(
                clean_runtime,
                out["generation_id"],
                src="doc:ticket",
                edge_type="mentions",
                dst="ticket:TICKET-1",
            )
            # Scoped to the EMISSION intent: the publisher also records
            # its own `publisher_derived:*` application envelope under
            # this producer's id (#1534).
            cur.execute(
                """
                SELECT status,
                       check_summary->'producer_policy'
                         ->>'profile_fingerprint'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_extractor:ticket'
                """
            )
            admission = cur.fetchone()

    assert out["status"] == "published"
    assert live_edges == 1
    assert admission[0] == "published"
    assert admission[1].startswith("sha256:")


def test_publisher_skips_chronos_extractor_when_inputs_clean(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_extractor_catalog_and_facts(conn)
        _materialize_ticket_extractor_profile(conn)

    extractor = _CountingRegexEntityExtractor([("ticket", r"(TICKET-\d+)")])
    publisher = Publisher(
        clean_runtime,
        "test",
        extractor=extractor,
        extractor_source_subtypes=["page"],
        routes=[
            LinkerRoute(
                entity_type="ticket",
                src_subtype=None,
                edge_type="mentions",
                dst_template="ticket:{value}",
            ),
        ],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    )

    first = publisher.cycle()
    second = publisher.cycle()

    assert first["status"] == "published"
    assert first["extractor_edges_emitted"] == 1
    assert extractor.calls == 1
    assert second["status"] == "published"
    assert second["noop"] is True
    assert second["reused_generation"] is True
    assert second["extractor_edges_emitted"] == 0
    assert second["noop_reuse_stage"] == "pre_enrichment_delta_probe"
    assert "extractor_dirty_input_check" in second["publish_skipped_stages"]


def test_projection_extractor_subtype_filter_bounds_live_extraction(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_extractor_catalog_and_facts(conn)
        _materialize_ticket_extractor_profile(conn)

    out = Publisher(
        clean_runtime,
        "test",
        extractor=RegexEntityExtractor([("ticket", r"(TICKET-\d+)")]),
        extractor_source_subtypes=["entity_mention"],
        routes=[
            LinkerRoute(
                entity_type="ticket",
                src_subtype=None,
                edge_type="mentions",
                dst_template="ticket:{value}",
            ),
        ],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            extractor_edges = _graph_edge_count(
                clean_runtime,
                out["generation_id"],
                src="doc:ticket",
                edge_type="mentions",
                dst="ticket:TICKET-1",
            )
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_extractor:ticket'
                """
            )
            extractor_envelopes = int(cur.fetchone()[0])

    assert out["status"] == "published"
    assert extractor_edges == 0
    assert extractor_envelopes == 0


def test_publisher_rejects_out_of_signature_extractor_output(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_extractor_catalog_and_facts(conn)
        _materialize_ticket_extractor_profile(conn)

    out = Publisher(
        clean_runtime,
        "test",
        extractor=RegexEntityExtractor([("ticket", r"(TICKET-\d+)")]),
        routes=[
            LinkerRoute(
                entity_type="ticket",
                src_subtype=None,
                edge_type="blocks",
                dst_template="ticket:{value}",
            ),
        ],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            live_edges = _graph_edge_count(
                clean_runtime,
                out["generation_id"],
                src="doc:ticket",
                edge_type="blocks",
                dst="ticket:TICKET-1",
            )
            cur.execute(
                """
                SELECT status, verdict->>'code'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_extractor:ticket'
                """
            )
            admission = cur.fetchone()

    assert out["status"] == "published"
    assert live_edges == 0
    assert admission == ("rejected", "producer_edge_triple_not_allowed")


def _seed_authority_catalog_and_nodes(
    conn: psycopg.Connection,
    *,
    src_source: str | None = "auth_fixture",
    dst_source: str | None = "auth_fixture",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO okg.subtypes (name, metadata, catalog_version_id)
            VALUES
              ('auth_src', '{}'::jsonb, %s),
              ('auth_dst', '{}'::jsonb, %s)
            """,
            (catalog_version_id, catalog_version_id),
        )
        cur.executemany(
            """
            INSERT INTO okg.subtype_ancestors
              (catalog_version_id, subtype_name, ancestor_name, depth)
            VALUES (%s, %s, %s, 0)
            ON CONFLICT DO NOTHING
            """,
            [
                (catalog_version_id, "auth_src", "auth_src"),
                (catalog_version_id, "auth_dst", "auth_dst"),
            ],
        )
        cur.execute(
            """
            INSERT INTO okg.narrowings
              (parent_subtype, edge_type, child_subtype, catalog_version_id)
            VALUES ('auth_src', 'auth_allows', 'auth_dst', %s)
            """,
            (catalog_version_id,),
        )
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(
                "auth:src",
                "auth_src",
                {"text": "source"},
                {},
                {},
            ),
        ],
        source=src_source,
        run_id=str(uuid4()),
    )
    store.apply_facts(
        [
            NodeFact(
                "auth:dst",
                "auth_dst",
                {"text": "destination"},
                {},
                {},
            ),
        ],
        source=dst_source,
        run_id=str(uuid4()),
    )
    if "auth_fixture" in {src_source, dst_source}:
        _materialize_input_profile(
            conn,
            producer_id="auth_fixture",
            trust_label="repo-local-trusted",
        )


def _materialize_input_profile(
    conn: psycopg.Connection,
    *,
    producer_id: str,
    trust_label: str,
) -> None:
    materialize_producer_profile(
        conn,
        deployment_name="test",
        producer_id=producer_id,
        producer_kind="source",
        trust_label=trust_label,
        admission_mode="fast_track",
        authority_scope={"source_family": producer_id},
        allowed_record_kinds=(),
        output_signature={},
        policy_clauses=(),
        output_scope_summary={"summary": "input trust fixture"},
        source_ref={"fixture": "test_derived_producer_authority"},
        catalog_version_id=None,
    )


def _seed_extractor_catalog_and_facts(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO okg.subtypes (name, metadata, catalog_version_id)
            VALUES
              ('page', '{}'::jsonb, %s),
              ('ticket', '{}'::jsonb, %s)
            """,
            (catalog_version_id, catalog_version_id),
        )
        cur.executemany(
            """
            INSERT INTO okg.subtype_ancestors
              (catalog_version_id, subtype_name, ancestor_name, depth)
            VALUES (%s, %s, %s, 0)
            ON CONFLICT DO NOTHING
            """,
            [
                (catalog_version_id, "page", "page"),
                (catalog_version_id, "ticket", "ticket"),
            ],
        )
        cur.execute(
            """
            INSERT INTO okg.narrowings
              (parent_subtype, edge_type, child_subtype, catalog_version_id)
            VALUES ('page', 'mentions', 'ticket', %s)
            """,
            (catalog_version_id,),
        )
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(
                "doc:ticket",
                "page",
                {"text": "See TICKET-1"},
                {},
                {},
            ),
            NodeFact(
                "ticket:TICKET-1",
                "ticket",
                {"text": "target"},
                {},
                {},
            ),
        ],
        source="extractor_fixture",
        run_id=str(uuid4()),
    )
    _materialize_input_profile(
        conn,
        producer_id="extractor_fixture",
        trust_label="repo-local-trusted",
    )


def _materialize_ticket_extractor_profile(
    conn: psycopg.Connection,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
            """
        )
        catalog_version_id = int(cur.fetchone()[0])
    policy = _ticket_extractor_policy()
    materialize_producer_profile(
        conn,
        deployment_name="test",
        producer_id=str(policy["producer_id"]),
        producer_kind=str(policy["producer_kind"]),
        trust_label=str(policy["trust_label"]),
        admission_mode=str(policy["admission_mode"]),
        authority_scope=dict(policy["authority_scope"]),
        allowed_record_kinds=tuple(policy["allowed_record_kinds"]),
        output_signature=dict(policy["output_signature"]),
        policy_clauses=tuple(dict(c) for c in policy["policy_clauses"]),
        output_scope_summary=dict(policy["output_scope_summary"]),
        source_ref=dict(policy["source_ref"]),
        catalog_version_id=catalog_version_id,
    )


def _ticket_extractor_policy() -> dict:
    return {
        "producer_id": "_extractor:ticket",
        "producer_kind": "extractor",
        "trust_label": "derived-deterministic-trusted",
        "admission_mode": "fast_track",
        "authority_scope": {"source_family": "_extractor:ticket"},
        "allowed_record_kinds": ["edge"],
        "output_signature": {
            "edges": [
                {
                    "src_subtype": "page",
                    "edge_type": "mentions",
                    "dst_subtype": "ticket",
                    "clause_id": "allow_ticket_mentions",
                },
            ],
        },
        "policy_clauses": [
            {
                "id": "allow_ticket_mentions",
                "effect": "allow",
                "record_kind": "edge",
                "triple": ["page", "mentions", "ticket"],
            },
        ],
        "output_scope_summary": {"summary": "ticket mention extraction"},
        "source_ref": {"fixture": "test_derived_producer_authority"},
    }


class _DanglingEndpointEnricher:
    """Two admissible edges and one whose src node was never minted.

    This is the shape issue #1330 reported from okg-workspace, where
    ``_enricher:tool_use_symbol`` anchored edges to ``tool_use:codex:*`` node
    ids that no source produces any more.
    """

    name = "authority_dangling_endpoint"
    reads_subtypes = ("auth_src",)
    reads_edge_types = ()
    emits_edge_types = ("auth_allows",)
    requires_narrowings = (("auth_src", "auth_allows", "auth_dst"),)

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id, incremental
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:dst",
                    ),
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:dst2",
                    ),
                    DerivedEdgeCandidate(
                        src="auth:never-minted",
                        edge_type="auth_allows",
                        dst="auth:dst",
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


class _SilentEnricher:
    """A wired enricher whose trigger condition never matches."""

    name = "authority_silent"
    reads_subtypes = ("auth_src",)
    reads_edge_types = ()
    emits_edge_types = ("auth_allows",)
    requires_narrowings = (("auth_src", "auth_allows", "auth_dst"),)

    def enrich(self, conn, *, generation_id, incremental=None):
        del conn, generation_id, incremental
        return EnrichResult(self.name, n_edges_emitted=0, n_edges_skipped=0)


def _seed_second_destination(conn: psycopg.Connection) -> None:
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [NodeFact("auth:dst2", "auth_dst", {"text": "second"}, {}, {})],
        source="auth_fixture",
        run_id=str(uuid4()),
    )


def test_dangling_endpoint_rejection_names_the_node_and_its_collateral(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #1330 end to end through the publisher.

    Before: the envelope recorded ``producer_edge_triple_not_allowed`` /
    "edge record does not match producer signature", and the admissible
    records it discarded were nowhere in the record. After: the code names a
    missing endpoint, the message names the absent node id, and the verdict
    states both the offending count and the collateral count.

    Enforce mode is pinned because that is the mode in which a refusal
    actually withholds the output; it also keeps the producer from acquiring
    a later ``publisher_derived:pre_view`` envelope, which the existing
    latest-envelope-per-producer discard query would otherwise read instead
    of the refusal.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _seed_second_destination(conn)

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_DanglingEndpointEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()
    live_edges = _graph_edge_count(
        clean_runtime,
        out["generation_id"],
        src="auth:src",
        edge_type="auth_allows",
        dst="auth:dst",
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status,
                       verdict->>'code',
                       verdict->>'message',
                       (verdict->>'offending_record_count')::bigint,
                       (verdict->>'collateral_record_count')::bigint,
                       verdict->'admission_record_counts',
                       check_summary->'emitted_record_counts',
                       verdict->'producer_policy'->'missing_endpoints'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id =
                       '_enricher:authority_dangling_endpoint'
                """
            )
            row = cur.fetchone()
        findings = _derived_admission_findings(conn, "test")
        inventory = {
            str(entry["producer_id"]): entry
            for entry in _derived_admission_producer_inventory(conn, "test")
        }

    assert row is not None
    (
        status,
        code,
        message,
        offending,
        collateral,
        admission_counts,
        emitted_counts,
        missing_endpoints,
    ) = row
    assert status == "rejected"
    assert code == "producer_edge_endpoint_missing"
    assert "auth:never-minted" in message
    assert "not resolvable in" in message
    assert "node-fact window" in message
    assert offending == 1
    assert collateral == 2
    assert admission_counts == {
        "record_kind": "edge",
        "emitted": 3,
        "admitted": 0,
        "rejected": 3,
        "gating_mode": "enforce",
        "refusal_withholds_output": True,
    }
    # The refusal really did withhold the output in this mode, which is what
    # licenses the discarded label below.
    assert live_edges == 0
    assert emitted_counts == {"node": 0, "edge": 3}
    assert missing_endpoints == [
        {"role": "src", "node_id": "auth:never-minted"},
    ]

    finding = next(
        item for item in findings
        if item["producer_id"] == "_enricher:authority_dangling_endpoint"
    )
    assert finding["code"] == "producer_edge_endpoint_missing"
    assert finding["offending_record_count"] == 1
    assert finding["collateral_record_count"] == 2
    assert finding["discarded_record_count"] == 3

    producer = inventory["_enricher:authority_dangling_endpoint"]
    assert producer["state"] == "output_discarded_at_admission"
    intent = producer["intents"][0]
    assert intent["offending_record_count"] == 1
    assert intent["collateral_record_count"] == 2
    assert intent["gating_mode"] == "enforce"


def test_zero_output_enricher_is_distinguishable_from_a_dropped_one(
    clean_runtime: str,
) -> None:
    """Issue #1331: "derived nothing" and "output dropped" are now separate.

    Both enrichers publish without failing the cycle, and before this change
    the recorded difference between them was an absence: the silent one wrote
    no envelope, and the discarded one wrote an envelope with no counts. The
    doctor derived-admission reader now names both states from one read.

    This case deliberately runs in the bare test-deployment enforcement mode
    (audit/off), where the derived-fact loader projects a refused producer
    anyway. The refusal there withheld nothing, so the reader must NOT call
    it discarded output -- the live edge count below is what makes that a
    claim about reality rather than about the ledger.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _seed_second_destination(conn)

    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[_SilentEnricher(), _DanglingEndpointEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()
    live_edges = _graph_edge_count(
        clean_runtime,
        out["generation_id"],
        src="auth:src",
        edge_type="auth_allows",
        dst="auth:dst",
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        inventory = {
            str(entry["producer_id"]): entry
            for entry in _derived_admission_producer_inventory(conn, "test")
        }

    silent = inventory["_enricher:authority_silent"]
    assert silent["state"] == "emitted_nothing"
    assert silent["emitted_record_count"] == 0
    assert silent["lifetime_edges_emitted"] == 0
    assert silent["intents"] == []

    dropped = inventory["_enricher:authority_dangling_endpoint"]
    assert dropped["emitted_record_count"] == 3
    assert dropped["admitted_record_count"] == 0
    assert dropped["rejected_record_count"] == 3
    assert [intent["status"] for intent in dropped["intents"]] == ["rejected"]
    assert dropped["gating_modes"] == ["audit"] or (
        dropped["gating_modes"] == ["off"]
    )
    assert dropped["state"] == "refused_at_admission_not_gated"
    assert live_edges == 1


def test_admitted_enricher_output_is_counted_on_its_envelope(
    clean_runtime: str,
) -> None:
    """Issue #1331: the counts are recorded for a healthy producer too.

    An accepted envelope that carried no count was the other half of the gap:
    with output recorded, "published N records" can be compared against the
    live edges that resulted.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)

    Publisher(
        clean_runtime,
        "test",
        enrichers=[_AllowedEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT check_summary->'emitted_record_counts',
                       verdict->'admission_record_counts'
                  FROM okg.admission_envelopes
                 WHERE deployment_name = 'test'
                   AND intent = 'derived_edge_emission'
                   AND producer_id = '_enricher:authority_allowed'
                """
            )
            row = cur.fetchone()
        inventory = {
            str(entry["producer_id"]): entry
            for entry in _derived_admission_producer_inventory(conn, "test")
        }

    assert row is not None
    assert row[0] == {"node": 0, "edge": 1}
    assert row[1] == {
        "record_kind": "edge",
        "emitted": 1,
        "admitted": 1,
        "rejected": 0,
        "gating_mode": "off",
        "refusal_withholds_output": False,
    }
    admitted = inventory["_enricher:authority_allowed"]
    assert admitted["state"] == "output_admitted"
    assert admitted["emitted_record_count"] == 1
    assert admitted["admitted_record_count"] == 1


class _FamilyMixedEnricher:
    """One producer, two rule sources: one admissible, one dangling.

    A derived producer writes one envelope per (source, run, fact range), so
    a family producer holds several envelopes for one producer id in a single
    cycle. This is the shape that lets an accepted sibling hide a refused one.
    """

    name = "authority_family_mixed"
    reads_subtypes = ("auth_src",)
    reads_edge_types = ()
    emits_edge_types = ("auth_allows",)
    requires_narrowings = (("auth_src", "auth_allows", "auth_dst"),)

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id, incremental
        inserted = 0
        with conn.cursor() as cur:
            for source_suffix, candidate in (
                (
                    "rule_ok",
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:dst",
                    ),
                ),
                (
                    "rule_dangling",
                    DerivedEdgeCandidate(
                        src="auth:never-minted",
                        edge_type="auth_allows",
                        dst="auth:dst2",
                    ),
                ),
            ):
                result = insert_deterministic_edges(
                    cur,
                    source=f"_enricher:{self.name}:{source_suffix}",
                    candidates=[candidate],
                )
                inserted += result.inserted
        return EnrichResult(self.name, n_edges_emitted=inserted)


def test_a_refused_sibling_envelope_is_not_hidden_by_an_accepted_one(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #1331: the inventory aggregates every emission envelope.

    Reading only the newest envelope per producer would report this producer
    as fully admitted or fully discarded depending on which of its two
    envelopes was decided last -- the exact ambiguity the counts exist to
    remove.
    """
    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _seed_second_destination(conn)

    Publisher(
        clean_runtime,
        "test",
        enrichers=[_FamilyMixedEnricher()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        inventory = {
            str(entry["producer_id"]): entry
            for entry in _derived_admission_producer_inventory(conn, "test")
        }

    producer = inventory["_enricher:authority_family_mixed"]
    assert producer["envelope_count"] == 2
    assert producer["emitted_record_count"] == 2
    assert producer["admitted_record_count"] == 1
    assert producer["rejected_record_count"] == 1
    assert producer["state"] == "output_partially_discarded_at_admission"
    assert sorted(
        str(intent["status"]) for intent in producer["intents"]
    ) == ["accepted", "rejected"]


def test_an_enricher_that_emitted_without_an_envelope_is_not_called_silent(
    clean_runtime: str,
) -> None:
    """A progress row that measures output is never reported as zero output.

    The inventory initialises an emitted count of zero for a producer with no
    emission envelope. If the state were read off that initial value, an
    enricher with a nonzero lifetime count and no envelope in this read would
    be labelled emitted_nothing -- a row that contradicts itself.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.enricher_progress
                  (deployment_name, enricher_name, fact_watermark,
                   edge_watermark, last_status, lifetime_edges_emitted)
                VALUES ('test', 'authority_ghost', 0, 0, 'success', 4200)
                """
            )
        inventory = {
            str(entry["producer_id"]): entry
            for entry in _derived_admission_producer_inventory(conn, "test")
        }

    ghost = inventory["_enricher:authority_ghost"]
    assert ghost["envelope_count"] == 0
    assert ghost["lifetime_edges_emitted"] == 4200
    assert ghost["state"] == "emitted_without_admission_record"


# ---- Issue #1528: an enricher is withheld while its input source is held ----


class _SourceReadingEnricher:
    """An enricher that declares the source it derives from.

    The #1330 shape: it reads a source's nodes and emits an edge against
    them. If that source's run is held in quarantine, its nodes are on the
    `quarantine:*` branch and this edge would point at a node the served
    graph does not have.
    """

    name = "authority_reads_source"
    reads_subtypes = ("auth_src",)
    reads_edge_types = ()
    reads_sources = ("auth_fixture",)
    emits_edge_types = ("auth_allows",)
    requires_narrowings = (("auth_src", "auth_allows", "auth_dst"),)

    def __init__(self) -> None:
        self.enrich_calls = 0

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id, incremental
        self.enrich_calls += 1
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src",
                        edge_type="auth_allows",
                        dst="auth:dst",
                    ),
                ],
            )
        return EnrichResult(
            self.name,
            n_edges_emitted=result.inserted,
            n_edges_skipped=result.skipped,
        )


def _seed_progress_watermark(
    conn: psycopg.Connection,
    *,
    enricher_name: str,
) -> int:
    """Seed a NON-ZERO enricher watermark with unconsumed input past it.

    Issue #1528 F7. `_seed_authority_catalog_and_nodes` writes its nodes
    straight into the Chronos graph, so `okg.node_facts` is empty and the
    publisher's watermark is 0 on both the advancing and the holding branch --
    an assertion that the watermark "stayed 0" would pass either way and prove
    nothing. Here two facts are written and the progress row is parked on the
    FIRST, so `max(fact_id)` is strictly greater than the watermark: the
    advancing branch MUST move the row and the holding branch MUST leave it.
    Returns the seeded watermark.
    """
    fact_ids: list[int] = []
    with conn.cursor() as cur:
        for _ in range(2):
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES ('auth:src', 'auth_src', '{}'::jsonb, 'auth_fixture',
                        '{}'::jsonb, '{}'::jsonb, now(), %s,
                        gen_random_uuid(), 'I')
                RETURNING fact_id
                """,
                (f"wm-{uuid4()}",),
            )
            fact_ids.append(int(cur.fetchone()[0]))
        watermark = fact_ids[0]
        assert max(fact_ids) > watermark, (
            "seeded facts must leave input past the watermark under test"
        )
        cur.execute(
            """
            INSERT INTO okg.enricher_progress
              (deployment_name, enricher_name, fact_watermark, edge_watermark,
               generation_id, last_status)
            VALUES ('test', %s, %s, 0, 0, 'success')
            ON CONFLICT (deployment_name, enricher_name) DO UPDATE
              SET fact_watermark = EXCLUDED.fact_watermark
            """,
            (enricher_name, watermark),
        )
    return watermark


def _max_node_fact_id(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT coalesce(max(fact_id), 0) FROM okg.node_facts")
        return int(cur.fetchone()[0])


def _record_source_envelope(
    conn: psycopg.Connection,
    *,
    deployment: str,
    source: str,
    admission_mode: str = "review",
    status: str = "pending",
    age_seconds: int = 0,
    applied: bool = False,
) -> str:
    """Record the source-admission envelope one ingest run leaves behind.

    Shaped like `ingest/service.py:2904-2915`: `intent='source_sync'`, one
    envelope PER RUN (the idempotency key folds in a replay key), and the
    status the finalizer actually writes. A `review`-mode run is finalized
    `pending`, NOT `quarantined` -- `ingest/service.py:3403-3423` returns
    `status='pending'` and only the operator decision map
    (`runtime/admission_workflows.py:902`) ever writes `quarantined`.

    `age_seconds` backdates `created_at` so a test can order several runs of
    the same source and pin which one the gate reads.

    `applied` stamps the verdict the way the accept path does
    (`runtime/admission_workflows.py:1070-1092` merges `node_facts_inserted`
    / `edge_facts_inserted` into the envelope verdict after
    `_apply_quarantined_source_run` copies the rows onto `default`). That
    stamp is the only positive per-envelope evidence that a held run reached
    `default`, and issue #1528 F1 makes it the release signal.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.admission_envelopes
              (deployment_name, producer_id, producer_kind, intent,
               admission_mode, status, source, run_id, idempotency_key,
               created_at, verdict)
            VALUES (%s, %s, 'source', 'source_sync', %s,
                    %s, %s, gen_random_uuid(), %s,
                    now() - make_interval(secs => %s), %s::jsonb)
            RETURNING admission_id::text
            """,
            (
                deployment, source, admission_mode, status, source,
                f"run-{uuid4()}", age_seconds,
                (
                    '{"node_facts_inserted": 2, "edge_facts_inserted": 0}'
                    if applied else "{}"
                ),
            ),
        )
        return str(cur.fetchone()[0])


def _progress_row(dsn: str, enricher_name: str) -> tuple | None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT last_status, fact_watermark, edge_watermark,
                       consecutive_failure_count, lifetime_edges_emitted
                  FROM okg.enricher_progress
                 WHERE deployment_name = 'test'
                   AND enricher_name = %s
                """,
                (enricher_name,),
            )
            return cur.fetchone()


def _release_through_admission_decide(dsn: str, admission_id: str) -> dict:
    """Release a held source run the way production does.

    Drives the REAL `admission_decide_workflow` accept path (`_run_decide`),
    not `transition_envelope`. That distinction is the point: per
    `runtime/admission_workflows.py:950-958` an `accept` on an envelope whose
    status is already `quarantined` short-circuits to `already_decided`
    WITHOUT transitioning, so a test that transitioned the row by hand would
    prove a release that production cannot perform. The accept path here runs
    `_apply_quarantined_source_run` (a no-op for a run with no rows parked on
    the quarantine branch) and then transitions the envelope to `accepted`.

    WHAT THIS DOES AND DOES NOT PROVE (issue #1528 F4, disclosed rather than
    overclaimed). The envelope here is hand-inserted by
    `_record_source_envelope` and has no rows parked on `quarantine:review`,
    so `_apply_quarantined_source_run` finds no facts and copies nothing.

    Proven: the ADMISSION state machine releases the hold -- the real accept
    path runs, the envelope reaches `accepted`, and the publisher's predicate
    then lets the enricher run and advance its watermark. That is the
    publisher-side contract this change owns.

    NOT proven here: that the apply actually MOVES rows from
    `quarantine:review` onto `default`. Parking a real row needs a producer
    profile whose fingerprint matches the envelope's
    `check_summary.producer_policy`, an authority scope covering the subtype,
    and a `FactBatchApplication` receipt -- well past a cheap addition to this
    fixture, and it would be re-testing
    `_apply_quarantined_source_run`, which
    `make-admission-review-load-bearing` already covers. The gap is that this
    file does not independently re-verify the apply itself.
    """
    from okg.substrate.runtime import admission_workflows

    with psycopg.connect(dsn, autocommit=True) as conn:
        return admission_workflows._run_decide(
            deployment="test",
            admission_id=admission_id,
            decision="accept",
            actor="test-operator",
            reason="issue-1528 release",
            verdict=None,
            # `_run_decide` resolves a DSN before it looks at `connection`;
            # give it this deployment's runtime rather than the operator's.
            dsn=dsn,
            connection=conn,
        )


def test_enricher_is_withheld_while_its_input_source_is_held(
    clean_runtime: str,
) -> None:
    """Issue #1528 (follow-up to #1330), the transitive hold step.

    Before: the publisher excluded a held producer's OWN output but ran every
    enricher regardless of whether the source it declares had been admitted
    onto `default`. An enricher reading a held source kept emitting edges
    against nodes the served graph never received -- the #1330 incident,
    111,940 records including an edge to a nonexistent node.

    After: the enricher does not run at all while its declared input source's
    latest run is held; the withholding is recorded with a reason that names
    the source, its watermark is NOT advanced, and the next publish after the
    held run is approved runs it over the same delta. Each of those is
    asserted, because a gate that merely produced silence would be the
    original defect in a new place.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        admission_id = _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
        )
        # Issue #1528 F7: give the hold something real to protect. Seed a
        # non-zero watermark and node_facts BEYOND it, so the advancing branch
        # and the holding branch would write DIFFERENT values. Without this
        # both branches write 0 and the watermark assertion proves nothing.
        seeded_watermark = _seed_progress_watermark(
            conn, enricher_name=enricher.name,
        )

    catalog_identity = bind_test_catalog_writer(clean_runtime, "test")
    out = Publisher(
        clean_runtime,
        "test",
        enrichers=[enricher],
        catalog_identity=catalog_identity,
    ).cycle()

    # 1. It never ran, so it emitted nothing.
    assert enricher.enrich_calls == 0
    withheld_edges = _graph_edge_count(
        clean_runtime,
        out["generation_id"],
        src="auth:src",
        edge_type="auth_allows",
        dst="auth:dst",
    )
    assert withheld_edges == 0

    held_progress = _progress_row(clean_runtime, enricher.name)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        inventory = {
            str(entry["producer_id"]): entry
            for entry in _derived_admission_producer_inventory(conn, "test")
        }
        current_watermark = _max_node_fact_id(conn)

    # 2. The reason is recorded and names the source that caused it.
    assert held_progress is not None
    assert held_progress[0].startswith("withheld:source_held_for_admission:")
    assert "auth_fixture" in held_progress[0]

    # 3. F7: the watermark was HELD, not advanced. There is unconsumed input
    #    past the seeded mark -- `_current_fact_watermarks` reads strictly
    #    greater than the row -- so an advancing branch would have moved it.
    #    It did not.
    assert current_watermark > seeded_watermark, (
        "fixture must leave unconsumed facts past the watermark, or the "
        "hold assertion below is vacuous"
    )
    assert held_progress[1] == seeded_watermark
    # Withholding is not a failure: it must not accumulate toward the
    # consecutive-failure alarm, and it produced no output to count.
    assert held_progress[3] == 0
    assert held_progress[4] == 0
    # And it is not reported as a failing enricher.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.enricher_progress
                 WHERE deployment_name = 'test'
                   AND last_status LIKE 'failed:%%'
                """
            )
            assert cur.fetchone()[0] == 0

    # 4. The doctor names the state, distinct from "derived nothing".
    withheld = inventory[f"_enricher:{enricher.name}"]
    assert withheld["state"] == "withheld_source_held_for_admission"
    assert withheld["state"] != "emitted_nothing"

    # 5. Release through the REAL admission decision, then publish again.
    decided = _release_through_admission_decide(clean_runtime, admission_id)
    assert decided["status"] == "accepted"

    released = Publisher(
        clean_runtime,
        "test",
        enrichers=[enricher],
        catalog_identity=catalog_identity,
    ).cycle()

    assert enricher.enrich_calls == 1
    assert _graph_edge_count(
        clean_runtime,
        released["generation_id"],
        src="auth:src",
        edge_type="auth_allows",
        dst="auth:dst",
    ) == 1

    released_progress = _progress_row(clean_runtime, enricher.name)
    assert released_progress is not None
    assert released_progress[0] == "success"
    # The hold cleared itself: no operator cleanup, no leftover withheld state.
    assert not released_progress[0].startswith("withheld:")


def test_a_rejected_source_run_does_not_withhold_forever(
    clean_runtime: str,
) -> None:
    """Issue #1528 F3: a terminal run releases the enricher.

    `_drop_quarantined_source_run` retracts a rejected run's rows from the
    quarantine branch -- they are never coming to `default`. Holding the
    enricher on them would wedge it on a run that will never return, so
    `rejected` and `failed` must not withhold.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
            status="rejected",
        )

    catalog_identity = bind_test_catalog_writer(clean_runtime, "test")
    Publisher(
        clean_runtime,
        "test",
        enrichers=[enricher],
        catalog_identity=catalog_identity,
    ).cycle()

    assert enricher.enrich_calls == 1
    progress = _progress_row(clean_runtime, enricher.name)
    assert progress is not None
    assert not progress[0].startswith("withheld:")


def test_an_older_held_run_does_not_pin_the_enricher(
    clean_runtime: str,
) -> None:
    """Issue #1528 F3: the gate reads the LATEST run, not any run.

    Envelopes are per-run (`ingest/service.py:2842-2853` folds `replay_key`
    into the idempotency key), so a source accumulates one row per sync. A
    `SELECT DISTINCT ... WHERE status = ...` over all of them let a single
    historical held run pin the enricher forever, no matter how many later
    runs were admitted onto `default`. Only the newest envelope describes the
    source's CURRENT state, so only it decides.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        # An old run that was never decided ...
        _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
            status="pending", age_seconds=3600,
        )
        # ... superseded by a newer run that WAS admitted onto `default`.
        _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
            admission_mode="fast_track", status="accepted", age_seconds=0,
        )

    catalog_identity = bind_test_catalog_writer(clean_runtime, "test")
    Publisher(
        clean_runtime,
        "test",
        enrichers=[enricher],
        catalog_identity=catalog_identity,
    ).cycle()

    assert enricher.enrich_calls == 1
    progress = _progress_row(clean_runtime, enricher.name)
    assert progress is not None
    assert not progress[0].startswith("withheld:")


def test_a_newer_held_run_withholds_despite_an_older_admitted_run(
    clean_runtime: str,
) -> None:
    """Issue #1528 F3, the other direction: recency must not weaken the gate.

    A source that has synced cleanly for months and whose LATEST run is held
    is exactly the #1330 shape. Scoping to the latest envelope must still
    withhold here, or the F3 fix would have traded a wedge for a hole.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
            admission_mode="fast_track", status="accepted", age_seconds=3600,
        )
        _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
            status="pending", age_seconds=0,
        )

    catalog_identity = bind_test_catalog_writer(clean_runtime, "test")
    Publisher(
        clean_runtime,
        "test",
        enrichers=[enricher],
        catalog_identity=catalog_identity,
    ).cycle()

    assert enricher.enrich_calls == 0
    progress = _progress_row(clean_runtime, enricher.name)
    assert progress is not None
    assert progress[0].startswith("withheld:source_held_for_admission:")


def test_another_deployments_held_run_does_not_withhold(
    clean_runtime: str,
) -> None:
    """Issue #1528 F3 / issue #419: the read is scoped to this deployment.

    A held run belonging to a DIFFERENT deployment says nothing about what is
    on this deployment's `default` branch. Cross-deployment bleed is the #419
    contamination shape and must not reappear through this gate.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _record_source_envelope(
            conn, deployment="some-other-deployment", source="auth_fixture",
            status="pending",
        )

    catalog_identity = bind_test_catalog_writer(clean_runtime, "test")
    Publisher(
        clean_runtime,
        "test",
        enrichers=[enricher],
        catalog_identity=catalog_identity,
    ).cycle()

    assert enricher.enrich_calls == 1
    progress = _progress_row(clean_runtime, enricher.name)
    assert progress is not None
    assert not progress[0].startswith("withheld:")


def test_withheld_state_is_read_only_from_the_recorded_reason() -> None:
    """Issue #1528: the doctor state is measured, never inferred.

    `withheld_source_held_for_admission` is reached only from the reason the
    publisher wrote to `enricher_progress.last_status`. An entry that is
    byte-identical except for that reason is still `emitted_nothing`, so the
    two observations cannot be confused -- and a stale accepted envelope from
    an earlier cycle does not report the withheld producer as healthy.
    """
    def _entry(last_status: str | None, **over: object) -> dict[str, object]:
        entry: dict[str, object] = {
            "producer_id": "_enricher:x",
            "producer_kind": "enricher",
            "envelope_count": 0,
            "intents": [],
            "emitted_record_count": 0,
            "emitted_node_record_count": 0,
            "emitted_edge_record_count": 0,
            "admitted_record_count": 0,
            "rejected_record_count": 0,
            "counts_complete": True,
            "gating_modes": [],
            "lifetime_edges_emitted": 0,
            "last_status": last_status,
        }
        entry.update(over)
        return entry

    withheld = _entry("withheld:source_held_for_admission:auth_fixture")
    assert (
        _derived_producer_output_state(withheld)
        == "withheld_source_held_for_admission"
    )

    ran_and_derived_nothing = _entry("success")
    assert (
        _derived_producer_output_state(ran_and_derived_nothing)
        == "emitted_nothing"
    )

    # An earlier cycle's accepted envelope must not report the currently
    # withheld producer as healthy.
    stale_accepted = _entry(
        "withheld:source_held_for_admission:auth_fixture",
        envelope_count=1,
        emitted_record_count=3,
        admitted_record_count=3,
        lifetime_edges_emitted=3,
    )
    assert (
        _derived_producer_output_state(stale_accepted)
        == "withheld_source_held_for_admission"
    )
    assert (
        _derived_producer_output_state(
            _entry(
                "success",
                envelope_count=1,
                emitted_record_count=3,
                admitted_record_count=3,
                lifetime_edges_emitted=3,
            )
        )
        == "output_admitted"
    )


# --- Issue #1528 F1: a swept envelope must not release the hold ---------------


def _publish_once(dsn: str, enricher) -> None:
    Publisher(
        dsn,
        "test",
        enrichers=[enricher],
        catalog_identity=bind_test_catalog_writer(dsn, "test"),
    ).cycle()


@pytest.mark.parametrize("swept_status", ["published", "blocked"])
def test_a_publish_sweep_does_not_release_a_quarantine_mode_hold(
    clean_runtime: str,
    swept_status: str,
) -> None:
    """Issue #1528 F1, the blocking review finding. THE regression test.

    A source whose policy sets `admission_mode='quarantine'` routes its rows
    to `quarantine:review`, but ingest's fence is
    `if admission_mode == "review"` (`ingest/service.py:3402`), so the run
    falls through to `transition_envelope(status='accepted')` with its rows
    still off `default`. Refs are attached just BEFORE that fence
    (`ingest/service.py:3209`), and the envelope's `target_branch_name` is
    never set for a source sync, so it reads as `default`.

    That anomalous `quarantine`/`accepted` envelope therefore satisfies both
    publish sweeps: `_mark_generation_published` (`admission.py:1637-1649`)
    and `_mark_generation_blocked` (`admission.py:1740-1751`) share the guard
    `admission_mode NOT IN ('review','quarantine') OR status = 'accepted'`.
    One publish cycle later the LATEST envelope reads
    `quarantine`/`published` -- or `blocked` -- and the first version of this
    gate, which held only on `pending`/`quarantined`/(`quarantine`+`accepted`),
    RELEASED it. The enricher then ran against rows still stranded off
    `default`: the #1330 shape, reintroduced by the very mechanism meant to
    prevent it.

    For `quarantine` mode no status is proof of an apply, so all of them hold.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
            admission_mode="quarantine", status=swept_status,
        )

    _publish_once(clean_runtime, enricher)

    assert enricher.enrich_calls == 0, (
        f"a quarantine-mode envelope swept to {swept_status} still has its "
        "rows off `default`; releasing on it is the #1330 defect"
    )
    progress = _progress_row(clean_runtime, enricher.name)
    assert progress is not None
    assert progress[0].startswith("withheld:source_held_for_admission:")
    assert "auth_fixture" in progress[0]


def test_a_quarantine_mode_accepted_envelope_still_withholds(
    clean_runtime: str,
) -> None:
    """Issue #1528 F1: `accepted` is not proof for a `quarantine`-mode run.

    This is the state ingest actually leaves behind for such a source (the
    fall-through at `ingest/service.py:3424`), before any publish sweep
    touches it. No apply ever ran: `_apply_quarantined_source_run` fires only
    from the decide workflow, and that path returns `already_decided` for an
    `accepted` source_sync envelope
    (`runtime/admission_workflows.py:968-985`).
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
            admission_mode="quarantine", status="accepted",
        )

    _publish_once(clean_runtime, enricher)

    assert enricher.enrich_calls == 0
    progress = _progress_row(clean_runtime, enricher.name)
    assert progress is not None
    assert progress[0].startswith("withheld:source_held_for_admission:")


@pytest.mark.parametrize("mode", ["review", "quarantine"])
def test_a_dropped_run_releases_in_either_held_mode(
    clean_runtime: str,
    mode: str,
) -> None:
    """Issue #1528 F1: `rejected`/`failed` are terminal in BOTH held modes.

    `_drop_quarantined_source_run` retracts the run from the quarantine
    branch, so its rows are never coming. Holding the enricher on them would
    wedge it forever on a run that cannot return -- the failure mode the
    stricter `quarantine` predicate must not introduce.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
            admission_mode=mode, status="failed",
        )

    _publish_once(clean_runtime, enricher)

    assert enricher.enrich_calls == 1
    progress = _progress_row(clean_runtime, enricher.name)
    assert progress is not None
    assert not progress[0].startswith("withheld:")


def test_an_apply_stamped_verdict_releases_a_quarantine_mode_hold(
    clean_runtime: str,
) -> None:
    """Issue #1528 F1: the one positive release signal, in the strict mode.

    Because no STATUS is proof for a `quarantine`-mode run, the predicate
    would otherwise wedge such a source permanently. The accept path stamps
    `node_facts_inserted` into the envelope verdict after
    `_apply_quarantined_source_run` copies the rows onto `default`
    (`runtime/admission_workflows.py:1070-1092`), and that stamp is
    per-envelope evidence the rows actually landed. It releases the hold.

    This also means the gate self-heals the day the upstream ingest fence is
    fixed to finalize a `quarantine` run as `pending`: the decide workflow
    will stamp the verdict and this predicate needs no further change.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
            admission_mode="quarantine", status="published", applied=True,
        )

    _publish_once(clean_runtime, enricher)

    assert enricher.enrich_calls == 1
    progress = _progress_row(clean_runtime, enricher.name)
    assert progress is not None
    assert not progress[0].startswith("withheld:")


@pytest.mark.parametrize("swept_status", ["published", "blocked"])
def test_review_mode_releases_on_a_swept_status(
    clean_runtime: str,
    swept_status: str,
) -> None:
    """Issue #1528 F1: the deliberate asymmetry between the two held modes.

    A `review`-mode run CANNOT reach `published`/`blocked` with its rows off
    `default`: ingest finalizes it `pending` (`ingest/service.py:3402-3423`),
    the publish sweeps refuse a non-`accepted` review envelope by their mode
    guard, and the only routes to `accepted` are an operator accept (which
    applies the rows) or the zero-fact path (`ingest/service.py:3196`, which
    accepts a run that wrote nothing and attaches no refs). So for `review`
    mode a status past `pending`/`quarantined` IS proof, and holding on it
    would wedge a source that behaved correctly.

    Pinned in the opposite direction from the `quarantine` test above so the
    asymmetry is a decision on the record, not an accident of the predicate.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
            admission_mode="review", status=swept_status,
        )

    _publish_once(clean_runtime, enricher)

    assert enricher.enrich_calls == 1
    progress = _progress_row(clean_runtime, enricher.name)
    assert progress is not None
    assert not progress[0].startswith("withheld:")


def test_a_quarantined_review_envelope_is_reported_as_wedged_not_decidable(
    clean_runtime: str,
) -> None:
    """Issue #1528 F2/F3: doctor names the hold AND its real release path.

    `okg doctor --check derived-admission` used to key `ok` off rejections
    alone, so a withheld enricher exited 0 with nothing said about it. It now
    reports an advisory `withheld` entry -- without reddening the check,
    because withholding is the gate working -- that names the enricher, the
    held source, the source's admission state, and what actually clears it.

    A `quarantined` envelope is the wedged case: per
    `runtime/admission_workflows.py:948-985` an accept short-circuits to
    `already_decided`, and so does a reject on a non-`pending` envelope, so
    `okg admission decide` cannot move it in either direction.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
            admission_mode="review", status="quarantined",
        )

    _publish_once(clean_runtime, enricher)
    assert enricher.enrich_calls == 0

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        withheld = _withheld_enricher_findings(conn, "test")

    assert len(withheld) == 1
    finding = withheld[0]
    assert finding["enricher_name"] == enricher.name
    assert finding["held_sources"] == ["auth_fixture"]
    assert finding["severity"] == "warning"
    assert finding["reason"] == "withheld_source_held_for_admission"
    assert finding["held_source_states"] == {
        "auth_fixture": {"admission_mode": "review", "status": "quarantined"},
    }
    # The release path tells the truth rather than offering a no-op command.
    assert "already_decided" in str(finding["release_path"])
    assert "non-held admission mode" in str(finding["release_path"])


@pytest.mark.parametrize("mode", ["review", "quarantine"])
def test_a_decidable_hold_is_reported_with_the_command_that_works(
    clean_runtime: str,
    mode: str,
) -> None:
    """Issue #1528 F3, the other direction: a real `pending` envelope in
    either held mode IS decidable, and doctor must still offer `okg admission
    decide` for it. Issue #1752: ingest now finalizes a `quarantine`-mode run
    `pending` too, so doctor must not call that one wedged."""
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _record_source_envelope(
            conn, deployment="test", source="auth_fixture",
            admission_mode=mode, status="pending",
        )

    _publish_once(clean_runtime, enricher)
    assert enricher.enrich_calls == 0

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        withheld = _withheld_enricher_findings(conn, "test")

    assert len(withheld) == 1
    assert "okg admission decide" in str(withheld[0]["release_path"])
    assert "already_decided" not in str(withheld[0]["release_path"])


def test_a_healthy_deployment_reports_no_withheld_findings(
    clean_runtime: str,
) -> None:
    """The advisory list must be EMPTY when nothing is held, or every
    deployment would carry a permanent warning and operators would learn to
    ignore the section."""
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)

    _publish_once(clean_runtime, enricher)
    assert enricher.enrich_calls == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _withheld_enricher_findings(conn, "test") == []


# --- Issue #1528 F5: an unresolvable declaration must refuse, not pass --------


def test_an_unregistered_declared_source_refuses_instead_of_passing_vacuously(
    clean_runtime: str,
    monkeypatch,
) -> None:
    """Issue #1528 F5: a declaration that matches nothing is a REFUSAL.

    `reads_sources` is free-form text, but ingest writes
    `admission_envelopes.source = entry.ownership_id`
    (`ingest/service.py:2914`) and registry name != ownership_id for real
    sources -- a `code_repos` block generates `git-files-<slug>` owned by
    `<slug>.git-files` (`sources/registry.py:806-812`). A typo, or a name
    used where an ownership id was needed, matches ZERO envelopes: the hold
    query returns nothing, the enricher runs, and the gate is silently inert.
    That is the same class of silence as #1330 and must fail closed.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        # A real held run exists for the OWNERSHIP id ...
        _record_source_envelope(
            conn, deployment="test", source="fixture.auth",
            admission_mode="review", status="pending",
        )

    # ... but the enricher declares `auth_fixture`, which the registry does
    # not know at all. Without validation the hold above would be missed.
    monkeypatch.setattr(
        Publisher, "_source_alias_index",
        lambda self: {"auth-fixture": "fixture.auth", "fixture.auth": "fixture.auth"},
    )
    _publish_once(clean_runtime, enricher)

    assert enricher.enrich_calls == 0, (
        "an enricher whose declared source resolves to nothing cannot be "
        "gated, so it must not run"
    )
    progress = _progress_row(clean_runtime, enricher.name)
    assert progress is not None
    assert progress[0].startswith("withheld:unknown_declared_source:")
    assert "auth_fixture" in progress[0]
    # A refusal is not a failure: it must not feed the failure alarm.
    assert progress[3] == 0

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        withheld = _withheld_enricher_findings(conn, "test")
    assert len(withheld) == 1
    assert withheld[0]["reason"] == "unknown_declared_source"
    assert "source_registry.yaml" in str(withheld[0]["release_path"])


def test_a_registry_name_declaration_is_normalized_to_the_ownership_id(
    clean_runtime: str,
    monkeypatch,
) -> None:
    """Issue #1528 F5, the accepting direction: the registry NAME works.

    An author naturally writes the source's registry name in `reads_sources`,
    but only the ownership id appears in `admission_envelopes.source`. The
    alias index accepts either and normalizes to the ownership id, so this
    declaration -- which would have matched nothing verbatim -- correctly
    finds the held run and withholds.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)
        _record_source_envelope(
            conn, deployment="test", source="fixture.auth",
            admission_mode="review", status="pending",
        )

    # `auth_fixture` is the registry NAME here; `fixture.auth` is its owner.
    monkeypatch.setattr(
        Publisher, "_source_alias_index",
        lambda self: {"auth_fixture": "fixture.auth", "fixture.auth": "fixture.auth"},
    )
    _publish_once(clean_runtime, enricher)

    assert enricher.enrich_calls == 0
    progress = _progress_row(clean_runtime, enricher.name)
    assert progress is not None
    assert progress[0].startswith("withheld:source_held_for_admission:")
    # The OWNERSHIP id is what the hold names, because that is what the
    # admission ledger actually contains.
    assert "fixture.auth" in progress[0]


def test_an_unloadable_registry_skips_validation_rather_than_refusing(
    clean_runtime: str,
    monkeypatch,
) -> None:
    """Issue #1528 F5, the stated gap. `_source_alias_index` returns None when
    no `source_registry.yaml` is loadable -- test harnesses and ephemeral
    deployments. Refusing every source-declaring enricher there would wedge
    publishing over a missing FILE rather than a real declaration defect, so
    validation is skipped and the declaration is used verbatim.

    Pinned because it is a hole: a deployment whose registry cannot be read
    gets the old ungated behaviour for an unresolvable id.
    """
    enricher = _SourceReadingEnricher()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)

    monkeypatch.setattr(Publisher, "_source_alias_index", lambda self: None)
    _publish_once(clean_runtime, enricher)

    assert enricher.enrich_calls == 1
    progress = _progress_row(clean_runtime, enricher.name)
    assert progress is not None
    assert not progress[0].startswith("withheld:")
