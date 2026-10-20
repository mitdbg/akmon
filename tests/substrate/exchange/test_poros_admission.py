from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest

from okg.substrate.exchange import (
    PorosInbox,
    TrustPin,
    TrustPinRegistry,
    build_unsigned_envelope,
    derive_ed25519_public_key,
    poros_candidate_to_admission_payload,
    sign_envelope,
    submit_poros_candidate_to_admission,
)
from okg.substrate.exchange.canonical_json import canonical_json_bytes
from okg.substrate.exchange.constants import LEGACY_BUNDLE_SUBMIT_TYPE
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres

SENDER_PRIVATE = b"\x31" * 32
SENDER_PUBLIC = derive_ed25519_public_key(SENDER_PRIVATE)
RECEIVER_PRIVATE = b"\x32" * 32


# ---------------------------------------------------------------------------
# Chronos-cutover live-graph read helpers.
#
# okg.nodes_live/edges_live were dropped; the live graph is the Chronos
# default-branch checkout over okg.graph_nodes/graph_edges, and the
# per-generation snapshot is checkout_generation(<gen>). These helpers
# replace the legacy ``SELECT ... FROM okg.nodes_live`` reads. The Chronos
# checkout has no ``valid_from_gen``/``valid_to_gen`` columns: a node being
# live "at" generation G is expressed as presence in
# checkout_generation(G), and "currently live" (legacy ``valid_to_gen IS
# NULL``) is presence in the default-branch checkout.
# ---------------------------------------------------------------------------


def _live_node_count(dsn: str, node_id: str) -> int:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout("default")
        rows = graph.query(
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": node_id},
        )
    return int(rows[0]["n"] or 0)


def _latest_published_generation_id(dsn: str) -> int | None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(generation_id)
              FROM okg.graph_generations
             WHERE status = 'published'
            """
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _published_node_count(dsn: str, node_id: str) -> int:
    """Presence of ``node_id`` in the last *published* generation snapshot.

    The Chronos default-branch checkout surfaces admitted-but-unpublished
    facts, so the pre-publish "not yet visible" check must read the published
    snapshot, not the default-branch HEAD.
    """
    generation_id = _latest_published_generation_id(dsn)
    if generation_id is None:
        return 0
    # Generation 0 is both the relational bootstrap row and the publisher's
    # explicit working-head sentinel. It has no published Chronos checkpoint,
    # so it cannot be used as a pinned visibility proof.
    if generation_id == 0:
        return 0
    try:
        present = _node_at_generation(
            dsn, generation_id=generation_id, node_id=node_id
        ) is not None
    except KeyError:
        # The bootstrap generation (gen 0) is `published` but has no Chronos
        # checkpoint to snapshot — it cannot contain the node, so it is absent.
        return 0
    return 1 if present else 0


def _node_at_generation(
    dsn: str, *, generation_id: int, node_id: str
) -> dict | None:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(generation_id)
        rows = graph.query(
            """
            SELECT subtype, attrs
              FROM okg.graph_nodes
             WHERE node_id = :node_id
            """,
            {"node_id": node_id},
        )
    return rows[0] if rows else None


def _live_node_attrs(dsn: str, node_id: str) -> dict | None:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout("default")
        rows = graph.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": node_id},
        )
    return rows[0]["attrs"] if rows else None


def _live_conflict_assertion_rows(dsn: str) -> list[dict]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout("default")
        return graph.query(
            """
            SELECT node_id, attrs, attrs->>'message_id' AS message_id
              FROM okg.graph_nodes
             WHERE subtype = 'poros_conflict_assertion'
               AND attrs->>'conflict_kind' = 'node_attrs'
            """
        )


def _graph_body(message_id: str) -> dict:
    node_id = f"poros:test:{message_id}"
    return {
        "graph_id": "poros:graph:poros-admission",
        "origin_authority_id": "okg:lab-a",
        "origin_generation_id": 7,
        "classification": "public",
        "graph_profile": "context",
        "root_node_ids": [node_id],
        "nodes": [
            {
                "node_id": node_id,
                "subtype": "context_packet",
                "attrs": {"text": "Poros admission test"},
            }
        ],
        "edges": [],
    }


def _unsafe_graph_body(message_id: str) -> dict:
    return {
        "graph_id": "poros:graph:unsafe",
        "origin_authority_id": "okg:lab-a",
        "origin_generation_id": 7,
        "classification": "public",
        "graph_profile": "context",
        "root_node_ids": [f"poros:test:{message_id}:a"],
        "nodes": [
            {
                "node_id": f"poros:test:{message_id}:a",
                "subtype": "domain_entity",
                "attrs": {"text": "source"},
            },
            {
                "node_id": f"poros:test:{message_id}:b",
                "subtype": "domain_entity",
                "attrs": {"text": "target"},
            },
        ],
        "edges": [
            {
                "src": f"poros:test:{message_id}:a",
                "dst": f"poros:test:{message_id}:b",
                "edge_type": "disallowed_relation",
                "src_subtype": "domain_entity",
                "dst_subtype": "domain_entity",
            }
        ],
    }


def _legacy_bundle_body(message_id: str) -> dict:
    return {
        "bundle_id": "kbundle:poros-admission",
        "origin_authority_id": "okg:lab-a",
        "origin_generation_id": 7,
        "classification": "public",
        "node_facts": [
            {
                "node_id": f"poros:test:{message_id}",
                "subtype": "context_packet",
                "attrs": {"text": "Poros admission test"},
            }
        ],
        "edge_facts": [],
    }


def _candidate(
    message_id: str = "poros-admission-1",
    *,
    legacy: bool = False,
    unsafe: bool = False,
):
    body = (
        _legacy_bundle_body(message_id)
        if legacy
        else (
            _unsafe_graph_body(message_id)
            if unsafe
            else _graph_body(message_id)
        )
    )
    return _candidate_from_body(
        message_id=message_id,
        body=body,
        message_type=LEGACY_BUNDLE_SUBMIT_TYPE if legacy else "graph.submit",
    )


def _candidate_from_body(
    *,
    message_id: str,
    body: dict,
    message_type: str = "graph.submit",
):
    unsigned = build_unsigned_envelope(
        sender="okg:lab-a",
        recipient="okg:mit",
        message_id=message_id,
        created_at=datetime.now(UTC),
        ttl_seconds=900,
        message_type=message_type,
        body=body,
    )
    envelope = sign_envelope(unsigned, SENDER_PRIVATE, kid="lab-a-sign-1")
    inbox = PorosInbox(
        receiver_authority_id="okg:mit",
        trust_registry=TrustPinRegistry.from_pins(
            [
                TrustPin(
                    authority_id="okg:lab-a",
                    kid="lab-a-sign-1",
                    public_key=SENDER_PUBLIC,
                )
            ]
        ),
        receiver_private_key=RECEIVER_PRIVATE,
        receiver_signing_kid="mit-sign-1",
    )
    result = inbox.handle_bytes(canonical_json_bytes(envelope))
    assert result.status == "accepted_for_admission"
    assert result.admission_candidate is not None
    return result.admission_candidate


def test_poros_graph_converts_to_admission_payload() -> None:
    candidate = _candidate("poros-convert-1")

    payload = poros_candidate_to_admission_payload(candidate)

    assert payload["nodes"][0]["node_id"] == "poros:test:poros-convert-1"
    assert payload["poros"]["graph_id"] == "poros:graph:poros-admission"
    assert payload["poros"]["graph_profile"] == "context"
    assert payload["poros"]["root_node_ids"] == ["poros:test:poros-convert-1"]
    assert payload["nodes"][0]["source_revision"]["poros"]["message_id"] == (
        "poros-convert-1"
    )
    assert payload["poros"]["body_sha256"] == candidate.body_sha256


def test_legacy_bundle_alias_normalizes_to_graph_payload() -> None:
    candidate = _candidate("poros-legacy-convert-1", legacy=True)

    payload = poros_candidate_to_admission_payload(candidate)

    assert payload["nodes"][0]["node_id"] == "poros:test:poros-legacy-convert-1"
    assert payload["poros"]["graph_id"] == "kbundle:poros-admission"
    assert payload["poros"]["legacy_bundle_id"] == "kbundle:poros-admission"
    assert payload["poros"]["graph_profile"] == "fact_bundle"


def test_poros_candidate_persists_admission_then_publishes(
    clean_runtime: str,
) -> None:
    candidate = _candidate("poros-publish-1")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            admitted = submit_poros_candidate_to_admission(
                conn,
                deployment_name="default",
                candidate=candidate,
            )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, intent, authority->'poros'->>'message_id',
                       authority->'poros'->>'graph_id'
                  FROM okg.admission_envelopes
                 WHERE admission_id = %s::uuid
                """,
                (admitted.admission_id,),
            )
            envelope_row = cur.fetchone()
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.admission_record_refs
                 WHERE admission_id = %s::uuid
                """,
                (admitted.admission_id,),
            )
            ref_count = int(cur.fetchone()[0] or 0)
            cur.execute(
                "SELECT count(*) FROM okg.node_facts WHERE run_id = %s::uuid",
                (admitted.run_id,),
            )
            node_fact_count = int(cur.fetchone()[0] or 0)
            cur.execute(
                """
                SELECT authority_scope, allowed_record_kinds,
                       output_signature, status
                  FROM okg.producer_profiles
                 WHERE deployment_name = 'default'
                   AND producer_id = 'okg:lab-a'
                 ORDER BY profile_id DESC
                 LIMIT 1
                """
            )
            profile_row = cur.fetchone()
    # Admission has emitted node_facts but not yet published; the node must not
    # be visible in the last *published* generation. (The Chronos default-branch
    # checkout now surfaces admitted-but-unpublished facts, so reading HEAD here
    # would see the staged node — assert against the published snapshot.)
    published_before_publish = _published_node_count(
        clean_runtime, "poros:test:poros-publish-1"
    )

    assert admitted.status == "accepted"
    assert admitted.emitted is not None
    assert admitted.emitted["node_facts_inserted"] == 1
    assert admitted.fact_ref_count == 1
    assert envelope_row == (
        "accepted",
        "poros.graph.submit",
        "poros-publish-1",
        "poros:graph:poros-admission",
    )
    assert ref_count >= 2
    assert node_fact_count == 1
    assert published_before_publish == 0
    assert profile_row is not None
    assert profile_row[0]["signing_key_id"] == "lab-a-sign-1"
    assert profile_row[0]["root_pattern"] == "context"
    assert profile_row[0]["graph_classification"] == "public"
    assert profile_row[0]["requested_incorporation_mode"] == "fast_track"
    assert profile_row[1] == ["node"]
    assert profile_row[2]["nodes"] == [
        {
            "subtype": "context_packet",
            "clause_id": "poros.node.context_packet",
        }
    ]
    assert profile_row[3] == "active"

    publish = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    assert publish["status"] == "published", publish
    assert _node_at_generation(
        clean_runtime,
        generation_id=publish["generation_id"],
        node_id="poros:test:poros-publish-1",
    ) is not None


def test_unsafe_poros_graph_is_rejected_before_fact_emission(
    clean_runtime: str,
) -> None:
    candidate = _candidate("poros-unsafe-1", unsafe=True)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_context_packet_catalog(conn)
        with conn.transaction():
            # The unsafe graph carries an edge, so its receiver-owned producer
            # profile has an empty output signature (compat). This test proves
            # the downstream narrowing gate rejects the disallowed edge, so it
            # opts past the compat admission gate (see
            # harden-exchange-federation-admission) to reach that gate; the
            # compat gate itself is proven in test_admission_fail_closed.py.
            admitted = submit_poros_candidate_to_admission(
                conn,
                deployment_name="default",
                candidate=candidate,
                allow_unsigned_compat=True,
            )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, verdict->>'code'
                  FROM okg.admission_envelopes
                 WHERE admission_id = %s::uuid
                """,
                (admitted.admission_id,),
            )
            envelope_status, verdict_code = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM okg.node_facts WHERE run_id = %s::uuid",
                (admitted.run_id,),
            )
            node_fact_count = int(cur.fetchone()[0] or 0)
            cur.execute(
                "SELECT count(*) FROM okg.edge_facts WHERE run_id = %s::uuid",
                (admitted.run_id,),
            )
            edge_fact_count = int(cur.fetchone()[0] or 0)

    assert admitted.status == "rejected"
    assert admitted.payload_rejected is True
    assert envelope_status == "rejected"
    assert verdict_code == "narrowing_violation"
    assert admitted.emitted is None
    assert node_fact_count == 0
    assert edge_fact_count == 0


@pytest.mark.parametrize(
    (
        "root_pattern",
        "root_subtype",
        "expected_status",
        "expected_live_subtype",
        "expected_route",
    ),
    [
        ("context", "document", "accepted", "document", "evidence"),
        ("claim", "claim", "accepted", "claim", "attributed_claim"),
        (
            "dataset_reference",
            "dataset",
            "accepted",
            "dataset",
            "dataset_reference",
        ),
        ("proposal", "proposal", "pending", None, None),
    ],
)
def test_root_pattern_routing_avoids_domain_fact_import(
    clean_runtime: str,
    root_pattern: str,
    root_subtype: str,
    expected_status: str,
    expected_live_subtype: str | None,
    expected_route: str | None,
) -> None:
    root_id = f"poros:test:route:{root_pattern}:root"
    domain_id = f"poros:test:route:{root_pattern}:domain"
    candidate = _candidate_from_body(
        message_id=f"poros-route-{root_pattern}",
        body={
            "graph_id": f"poros:graph:route:{root_pattern}",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 9,
            "classification": "public",
            "graph_profile": root_pattern,
            "root_node_ids": [root_id],
            "nodes": [
                {
                    "node_id": root_id,
                    "subtype": root_subtype,
                    "attrs": {
                        "text": f"{root_pattern} root",
                        "locator": "s3://example/dataset.parquet",
                        "sha256": "sha256:dataset",
                        "access_constraints": ["receiver-policy-required"],
                    },
                },
                {
                    "node_id": domain_id,
                    "subtype": "domain_entity",
                    "attrs": {"name": "should not become canonical"},
                },
            ],
            "edges": [],
        },
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            admitted = submit_poros_candidate_to_admission(
                conn,
                deployment_name="default",
                candidate=candidate,
            )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT admission_mode, status
                  FROM okg.admission_envelopes
                 WHERE admission_id = %s::uuid
                """,
                (admitted.admission_id,),
            )
            admission_mode, envelope_status = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM okg.node_facts WHERE node_id = %s",
                (domain_id,),
            )
            domain_fact_count = int(cur.fetchone()[0] or 0)

    assert admitted.status == expected_status
    assert envelope_status == expected_status
    assert domain_fact_count == 0
    if root_pattern == "proposal":
        assert admission_mode == "review"
        assert admitted.emitted is None
        assert admitted.fact_ref_count == 0
        publish = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
        assert publish["status"] in {"published", "no_op"}, publish
        assert _live_node_count(clean_runtime, root_id) == 0
        return

    assert admission_mode == "fast_track"
    assert admitted.emitted is not None
    assert admitted.emitted["node_facts_inserted"] == 1
    publish = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert publish["status"] == "published", publish
    live_row = _node_at_generation(
        clean_runtime,
        generation_id=publish["generation_id"],
        node_id=root_id,
    )
    domain_live_count = _live_node_count(clean_runtime, domain_id)

    assert live_row is not None
    assert live_row["subtype"] == expected_live_subtype
    assert live_row["attrs"]["poros_route"] == expected_route
    assert live_row["attrs"]["poros_source_node_count"] == 2
    if root_pattern == "claim":
        assert live_row["attrs"]["claim_status"] == "attributed_not_canonical"
    if root_pattern == "dataset_reference":
        assert live_row["attrs"]["body_import"] == "not_imported_through_poros_v0"
    assert domain_live_count == 0


def test_conflicting_poros_assertion_does_not_overwrite_live_fact(
    clean_runtime: str,
) -> None:
    node_id = "poros:test:conflict:entity"
    first = _candidate_from_body(
        message_id="poros-conflict-first",
        body={
            "graph_id": "poros:graph:conflict:first",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 1,
            "classification": "public",
            "graph_profile": "fact_bundle",
            "root_node_ids": [node_id],
            "nodes": [
                {
                    "node_id": node_id,
                    "subtype": "domain_entity",
                    "attrs": {"name": "Alpha"},
                }
            ],
            "edges": [],
        },
    )
    second = _candidate_from_body(
        message_id="poros-conflict-second",
        body={
            "graph_id": "poros:graph:conflict:second",
            "origin_authority_id": "okg:lab-a",
            "origin_generation_id": 2,
            "classification": "public",
            "graph_profile": "fact_bundle",
            "root_node_ids": [node_id],
            "nodes": [
                {
                    "node_id": node_id,
                    "subtype": "domain_entity",
                    "attrs": {"name": "Beta"},
                }
            ],
            "edges": [],
        },
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            first_admission = submit_poros_candidate_to_admission(
                conn,
                deployment_name="default",
                candidate=first,
            )
    first_publish = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert first_admission.status == "accepted"
    assert first_publish["status"] == "published", first_publish

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.transaction():
            second_admission = submit_poros_candidate_to_admission(
                conn,
                deployment_name="default",
                candidate=second,
            )
    second_publish = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert second_admission.status == "accepted"
    assert second_publish["status"] == "published", second_publish

    live_attrs = _live_node_attrs(clean_runtime, node_id)
    conflict_rows = _live_conflict_assertion_rows(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_revision->'poros'->>'message_id'
              FROM okg.node_facts
             WHERE node_id = %s
             ORDER BY observed_at
            """,
            (node_id,),
        )
        direct_message_ids = [row[0] for row in cur.fetchall()]

    assert live_attrs["name"] == "Alpha"
    assert len(conflict_rows) == 1
    conflict_attrs = conflict_rows[0]["attrs"]
    assert conflict_attrs["asserted_record"]["node_id"] == node_id
    assert conflict_attrs["asserted_record"]["attrs"]["name"] == "Beta"
    assert conflict_attrs["existing_record"]["attrs"]["name"] == "Alpha"
    assert conflict_attrs["resolution_status"] == "unresolved"
    assert conflict_rows[0]["message_id"] == "poros-conflict-second"
    assert direct_message_ids == ["poros-conflict-first"]


def _seed_context_packet_catalog(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO okg.subtypes (name, metadata, catalog_version_id)
            VALUES ('context_packet', '{}'::jsonb, %s)
            """,
            (catalog_version_id,),
        )
        cur.execute(
            """
            INSERT INTO okg.subtype_ancestors
              (catalog_version_id, subtype_name, ancestor_name, depth)
            VALUES (%s, 'context_packet', 'context_packet', 0)
            ON CONFLICT DO NOTHING
            """,
            (catalog_version_id,),
        )
        cur.execute(
            """
            INSERT INTO okg.narrowings
              (parent_subtype, edge_type, child_subtype, catalog_version_id)
            VALUES ('context_packet', 'references', 'context_packet', %s)
            """,
            (catalog_version_id,),
        )
