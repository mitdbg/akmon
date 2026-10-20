from __future__ import annotations

import json
from datetime import datetime, timezone

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore, _edge_id
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.nomos import (
    activate_policy_bundle,
    compile_policy_graph,
    read_nodes_with_active_policy,
)
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    LINEAGE_SCHEMA,
    lineage_for_row,
    normalize_source_lineage,
    source_ids_from_lineage,
    stamp_attrs_with_source_lineage,
)
from okg.substrate.nomos.source_lineage_repair import (
    VIEW_REGISTRY_SOURCE,
    repair_laundered_derived_lineage,
    restamp_active_policy_lineage_fingerprints,
    restamp_publisher_derived_lineage,
    scan_laundered_derived_lineage,
)


pytestmark = pytest.mark.postgres


def test_normalized_lineage_envelope_is_deterministic_and_policy_pinned() -> None:
    lineage = normalize_source_lineage(
        {
            "sources": [
                {
                    "source_id": "google_drive",
                    "source_record_ids": [{"id": "g:1"}],
                    "policy_fingerprints": ["sha256:restricted"],
                },
                {"source_id": "public_docs"},
            ],
            "source_ids": ["google_drive", "public_docs"],
        },
        policy_fingerprint="sha256:publish-policy",
    )

    assert lineage["schema"] == LINEAGE_SCHEMA
    assert lineage["status"] == "ok"
    assert lineage["source_ids"] == ["google_drive", "public_docs"]
    assert json.dumps(lineage, sort_keys=True) == json.dumps(
        normalize_source_lineage(lineage),
        sort_keys=True,
    )
    fingerprints_by_source = {
        source["source_id"]: source.get("policy_fingerprints", [])
        for source in lineage["sources"]
    }
    assert fingerprints_by_source["google_drive"] == [
        "sha256:publish-policy",
        "sha256:restricted",
    ]
    assert fingerprints_by_source["public_docs"] == ["sha256:publish-policy"]


def test_graph_writes_stamp_policy_fingerprint_on_direct_nodes_and_edges(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:a",
                    "document",
                    {"classification": "internal", "text": "A"},
                    {"id": "doc:a"},
                    {"rev": "1"},
                ),
                NodeFact(
                    "doc:b",
                    "document",
                    {"classification": "internal", "text": "B"},
                    {"id": "doc:b"},
                    {"rev": "1"},
                ),
                EdgeFact(
                    "doc:a",
                    "doc:b",
                    "references",
                    {"classification": "internal"},
                    source_record_id={"id": "edge:a:b"},
                    source_revision={"rev": "1"},
                ),
            ],
            source="public_docs",
            policy_fingerprint="sha256:publish-policy",
        )
        graph = store.checkout()
        node_attrs = graph.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'doc:a'",
        )[0]["attrs"]
        edge_attrs = graph.query(
            "SELECT attrs FROM okg.graph_edges WHERE src = 'doc:a' AND dst = 'doc:b'",
        )[0]["attrs"]
        conn.commit()

    node_lineage = node_attrs[LINEAGE_ATTR]
    edge_lineage = edge_attrs[LINEAGE_ATTR]
    assert node_lineage["source_ids"] == ["public_docs"]
    assert node_lineage["sources"][0]["policy_fingerprints"] == [
        "sha256:publish-policy"
    ]
    assert edge_lineage["source_ids"] == ["public_docs"]
    assert edge_lineage["sources"][0]["policy_fingerprints"] == [
        "sha256:publish-policy"
    ]


def test_derived_fact_preserves_union_lineage_and_publish_policy(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:summary",
                    "document",
                    {
                        "classification": "internal",
                        "text": "derived from mixed inputs",
                        LINEAGE_ATTR: normalize_source_lineage(
                            ["public_docs", "google_drive"],
                            policy_fingerprint="sha256:source-policy",
                        ),
                    },
                    {"id": "summary"},
                    {"rev": "1"},
                )
            ],
            source=None,
            policy_fingerprint="sha256:publish-policy",
        )
        graph = store.checkout()
        attrs = graph.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'doc:summary'",
        )[0]["attrs"]
        conn.commit()

    lineage = attrs[LINEAGE_ATTR]
    assert lineage["source_ids"] == ["google_drive", "public_docs"]
    assert {
        source["source_id"]: source["policy_fingerprints"]
        for source in lineage["sources"]
    } == {
        "google_drive": ["sha256:publish-policy", "sha256:source-policy"],
        "public_docs": ["sha256:publish-policy", "sha256:source-policy"],
    }


def test_publisher_derived_fact_preserves_existing_union_lineage(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:publisher-summary",
                    "document",
                    {
                        "classification": "internal",
                        "text": "publisher derived",
                        LINEAGE_ATTR: normalize_source_lineage(
                            ["public_docs", "google_drive"],
                            policy_fingerprint="sha256:source-policy",
                        ),
                    },
                    {"id": "publisher-summary"},
                    {"rev": "1"},
                )
            ],
            source="_publisher_derived",
            policy_fingerprint="sha256:publish-policy",
        )
        graph = store.checkout()
        attrs = graph.query(
            """
            SELECT attrs
              FROM okg.graph_nodes
             WHERE node_id = 'doc:publisher-summary'
            """,
        )[0]["attrs"]
        conn.commit()

    lineage = attrs[LINEAGE_ATTR]
    assert lineage["source_ids"] == ["google_drive", "public_docs"]
    assert "_publisher_derived" not in lineage["source_ids"]
    assert {
        source["source_id"]: source["policy_fingerprints"]
        for source in lineage["sources"]
    } == {
        "google_drive": ["sha256:publish-policy", "sha256:source-policy"],
        "public_docs": ["sha256:publish-policy", "sha256:source-policy"],
    }


def test_publisher_derived_without_lineage_fails_closed() -> None:
    attrs = stamp_attrs_with_source_lineage({}, source_id="_publisher_derived")

    lineage = attrs[LINEAGE_ATTR]
    assert lineage["status"] == "missing"
    assert source_ids_from_lineage(lineage) == ()
    assert "_publisher_derived" not in lineage["source_ids"]


def test_preserve_existing_lineage_keeps_the_envelope_it_was_given() -> None:
    """A re-application keeps the lineage, whatever id it writes under.

    Regression for #1534. The publisher re-applies an already attributed
    derived fact under the id of the producer that emitted it. Without the
    preserve flag the write OVERWROTE the fact's inherited lineage with
    that producer id, so a derived node named its emitter as a root source
    and readers inherited the emitter's trust label.
    """
    existing = normalize_source_lineage("auth_fixture")

    preserved = stamp_attrs_with_source_lineage(
        {LINEAGE_ATTR: existing},
        source_id="_enricher:some_emitter",
        preserve_existing_lineage=True,
    )
    assert source_ids_from_lineage(preserved[LINEAGE_ATTR]) == ("auth_fixture",)

    # The default is unchanged: a genuine EMISSION still replaces the
    # envelope with the emitting source id, which is why the publisher has
    # to say explicitly that its write is a re-application.
    overwritten = stamp_attrs_with_source_lineage(
        {LINEAGE_ATTR: existing},
        source_id="_enricher:some_emitter",
    )
    assert source_ids_from_lineage(overwritten[LINEAGE_ATTR]) == (
        "_enricher:some_emitter",
    )


def test_preserve_existing_lineage_without_lineage_still_fails_closed() -> None:
    attrs = stamp_attrs_with_source_lineage(
        {},
        source_id="_enricher:some_emitter",
        preserve_existing_lineage=True,
    )

    lineage = attrs[LINEAGE_ATTR]
    assert lineage["status"] == "missing"
    assert source_ids_from_lineage(lineage) == ()
    assert "_enricher:some_emitter" not in lineage["source_ids"]


def test_publisher_derived_edge_inherits_endpoint_lineage(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:public",
                    "document",
                    {"text": "public"},
                    {"id": "doc:public"},
                    {"rev": "1"},
                )
            ],
            source="public_docs",
            policy_fingerprint="sha256:public-policy",
        )
        store.apply_facts(
            [
                NodeFact(
                    "doc:google",
                    "document",
                    {"text": "google"},
                    {"id": "doc:google"},
                    {"rev": "1"},
                )
            ],
            source="google_drive",
            policy_fingerprint="sha256:google-policy",
        )
        store.apply_facts(
            [
                EdgeFact(
                    "doc:public",
                    "doc:google",
                    "references",
                    {"kind": "publisher-derived"},
                )
            ],
            source="_publisher_derived",
        )
        graph = store.checkout()
        attrs = graph.query(
            """
            SELECT attrs
              FROM okg.graph_edges
             WHERE src = 'doc:public'
               AND dst = 'doc:google'
               AND edge_type = 'references'
            """
        )[0]["attrs"]
        conn.commit()

    lineage = attrs[LINEAGE_ATTR]
    assert lineage["source_ids"] == ["google_drive", "public_docs"]
    assert {
        source["source_id"]: source.get("policy_fingerprints", [])
        for source in lineage["sources"]
    } == {
        "google_drive": ["sha256:google-policy"],
        "public_docs": ["sha256:public-policy"],
    }


def test_restamp_publisher_derived_repairs_legacy_rows(
    clean_runtime: str,
) -> None:
    observed_at = datetime.now(timezone.utc)
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:public",
                    "document",
                    {"text": "public"},
                    {"id": "doc:public"},
                    {"rev": "1"},
                )
            ],
            source="public_docs",
            policy_fingerprint="sha256:public-policy",
        )
        store.apply_facts(
            [
                NodeFact(
                    "doc:google",
                    "document",
                    {"text": "google"},
                    {"id": "doc:google"},
                    {"rev": "1"},
                )
            ],
            source="google_drive",
            policy_fingerprint="sha256:google-policy",
        )
        session = store.checkout()
        with session.transaction():
            session.upsert_rows(
                "okg.graph_nodes",
                [
                    {
                        "node_id": "view:legacy",
                        "subtype": "view",
                        "attrs": {"view_id": "legacy"},
                        "text_hash": None,
                        "embedding": None,
                        "embedding_model": None,
                        "source": "_publisher_derived",
                        "source_record_id": {"view_id": "legacy"},
                        "source_revision": {},
                        "run_id": None,
                        "observed_at": observed_at,
                    }
                ],
            )
            session.upsert_rows(
                "okg.graph_edges",
                [
                    {
                        "edge_id": _edge_id(
                            "doc:public",
                            "references",
                            "doc:google",
                        ),
                        "src": "doc:public",
                        "dst": "doc:google",
                        "edge_type": "references",
                        "attrs": {"legacy": True},
                        "provenance": "derived_deterministic",
                        "confidence": 1.0,
                        "source": "_publisher_derived",
                        "source_record_id": {},
                        "source_revision": {},
                        "run_id": None,
                        "observed_at": observed_at,
                    }
                ],
            )

        result = restamp_publisher_derived_lineage(conn, batch_size=1)
        graph = store.checkout()
        node_attrs = graph.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'view:legacy'"
        )[0]["attrs"]
        edge_attrs = graph.query(
            """
            SELECT attrs
              FROM okg.graph_edges
             WHERE src = 'doc:public'
               AND dst = 'doc:google'
               AND edge_type = 'references'
            """
        )[0]["attrs"]
        conn.commit()

    assert result["nodes"]["restamped"] == 1
    assert result["edges"]["restamped"] == 1
    assert node_attrs[LINEAGE_ATTR]["source_ids"] == [VIEW_REGISTRY_SOURCE]
    assert edge_attrs[LINEAGE_ATTR]["source_ids"] == [
        "google_drive",
        "public_docs",
    ]
    assert "_publisher_derived" not in edge_attrs[LINEAGE_ATTR]["source_ids"]


def test_restamp_active_policy_fingerprint_preserves_valid_lineage_only(
    clean_runtime: str,
) -> None:
    deployment = "nomos-source-lineage-contract"
    scope = f"deployment:{deployment}"
    old_fingerprint = "sha256:old-policy"
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:source-lineage-contract",
        "policy_version": "v2",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_public_docs",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:reader"],
                "actions": ["graph.read"],
                "match": {
                    "deployment": deployment,
                    "scope": scope,
                    "source_id": "public_docs",
                },
            },
        ],
    })
    observed_at = datetime.now(timezone.utc)
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:contract",
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:stale",
                    "document",
                    {"text": "stale"},
                    {"id": "stale"},
                    {"rev": "1"},
                ),
                NodeFact(
                    "doc:other",
                    "document",
                    {"text": "other"},
                    {"id": "other"},
                    {"rev": "1"},
                ),
            ],
            source="public_docs",
            policy_fingerprint=old_fingerprint,
        )
        store.apply_facts(
            [
                NodeFact(
                    "doc:no-policy",
                    "document",
                    {"text": "no policy fingerprint"},
                    {"id": "no-policy"},
                    {"rev": "1"},
                ),
            ],
            source="public_docs",
        )
        store.apply_facts(
            [
                NodeFact(
                    "doc:uncovered",
                    "document",
                    {"text": "uncovered"},
                    {"id": "uncovered"},
                    {"rev": "1"},
                ),
            ],
            source="private_docs",
            policy_fingerprint=old_fingerprint,
        )
        store.apply_facts(
            [
                EdgeFact(
                    "doc:stale",
                    "doc:other",
                    "references",
                    {"kind": "stale-edge"},
                    source_record_id={"id": "edge"},
                    source_revision={"rev": "1"},
                )
            ],
            source="public_docs",
            policy_fingerprint=old_fingerprint,
        )
        session = store.checkout()
        with session.transaction():
            session.upsert_rows(
                "okg.graph_nodes",
                [
                    {
                        "node_id": "doc:malformed",
                        "subtype": "document",
                        "attrs": {
                            "text": "malformed",
                            LINEAGE_ATTR: {
                                "schema": LINEAGE_SCHEMA,
                                "status": "malformed",
                                "source_ids": ["public_docs"],
                                "sources": [],
                            },
                        },
                        "text_hash": None,
                        "embedding": None,
                        "embedding_model": None,
                        "source": "public_docs",
                        "source_record_id": {"id": "malformed"},
                        "source_revision": {"rev": "1"},
                        "run_id": None,
                        "observed_at": observed_at,
                    }
                ],
            )

        result = restamp_active_policy_lineage_fingerprints(
            conn,
            deployment_name=deployment,
            batch_size=1,
        )
        graph = store.checkout()
        rows = {
            row["node_id"]: row["attrs"][LINEAGE_ATTR]
            for row in graph.query(
                """
                SELECT node_id, attrs
                  FROM okg.graph_nodes
                 WHERE node_id IN (
                   'doc:stale',
                   'doc:other',
                   'doc:no-policy',
                   'doc:uncovered',
                   'doc:malformed'
                 )
                """
            )
        }
        edge_lineage = graph.query(
            """
            SELECT attrs
              FROM okg.graph_edges
             WHERE src = 'doc:stale'
               AND dst = 'doc:other'
               AND edge_type = 'references'
            """
        )[0]["attrs"][LINEAGE_ATTR]
        conn.commit()

    assert result["ok"] is True
    assert result["policy_fingerprint"] == bundle.policy_fingerprint
    assert result["nodes"]["restamped"] == 3
    assert result["nodes"]["skipped_missing_or_malformed"] == 1
    assert result["nodes"]["skipped_uncovered_source"] == 1
    assert result["edges"]["restamped"] == 1
    for lineage in (rows["doc:stale"], rows["doc:other"], edge_lineage):
        assert lineage["source_ids"] == ["public_docs"]
        assert set(lineage["sources"][0]["policy_fingerprints"]) == {
            old_fingerprint,
            bundle.policy_fingerprint,
        }
    assert rows["doc:no-policy"]["source_ids"] == ["public_docs"]
    assert rows["doc:no-policy"]["sources"][0]["policy_fingerprints"] == [
        bundle.policy_fingerprint
    ]
    assert rows["doc:uncovered"]["source_ids"] == ["private_docs"]
    assert rows["doc:uncovered"]["sources"][0]["policy_fingerprints"] == [
        old_fingerprint
    ]
    assert rows["doc:malformed"]["status"] == "malformed"
    assert rows["doc:malformed"]["sources"] == []


def test_restamp_active_policy_fingerprint_is_repeatable_for_versioned_rows(
    clean_runtime: str,
) -> None:
    deployment = "nomos-source-lineage-versioned"
    scope = f"deployment:{deployment}"
    old_fingerprint = "sha256:old-policy"
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:source-lineage-versioned",
        "policy_version": "v2",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_public_docs",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:reader"],
                "actions": ["graph.read"],
                "match": {
                    "deployment": deployment,
                    "scope": scope,
                    "source_id": "public_docs",
                },
            },
        ],
    })
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:contract",
        )
        store = ChronosGraphStore.from_connection(conn)
        for revision in range(8):
            store.apply_facts(
                [
                    NodeFact(
                        "doc:versioned",
                        "document",
                        {"text": f"version {revision}"},
                        {"id": "versioned"},
                        {"rev": str(revision)},
                    )
                ],
                source="public_docs",
                policy_fingerprint=old_fingerprint,
            )
        store.apply_facts(
            [
                NodeFact(
                    "doc:neighbor",
                    "document",
                    {"text": "neighbor"},
                    {"id": "neighbor"},
                    {"rev": "1"},
                ),
                EdgeFact(
                    "doc:versioned",
                    "doc:neighbor",
                    "references",
                    {"kind": "versioned-edge"},
                    source_record_id={"id": "edge"},
                    source_revision={"rev": "1"},
                ),
            ],
            source="public_docs",
            policy_fingerprint=old_fingerprint,
        )

        first = restamp_active_policy_lineage_fingerprints(
            conn,
            deployment_name=deployment,
            batch_size=1,
        )
        second = restamp_active_policy_lineage_fingerprints(
            conn,
            deployment_name=deployment,
            batch_size=1,
        )
        graph = store.checkout()
        node_lineage = graph.query(
            """
            SELECT attrs
              FROM okg.graph_nodes
             WHERE node_id = 'doc:versioned'
            """
        )[0]["attrs"][LINEAGE_ATTR]
        edge_lineage = graph.query(
            """
            SELECT attrs
              FROM okg.graph_edges
             WHERE src = 'doc:versioned'
               AND dst = 'doc:neighbor'
               AND edge_type = 'references'
            """
        )[0]["attrs"][LINEAGE_ATTR]
        conn.commit()

    assert first["ok"] is True
    assert first["nodes"]["restamped"] == 2
    assert first["edges"]["restamped"] == 1
    assert second["ok"] is True
    assert second["nodes"]["restamped"] == 0
    assert second["edges"]["restamped"] == 0
    assert second["nodes"]["already_current"] == 2
    assert second["edges"]["already_current"] == 1
    for lineage in (node_lineage, edge_lineage):
        assert lineage["source_ids"] == ["public_docs"]
        assert set(lineage["sources"][0]["policy_fingerprints"]) == {
            old_fingerprint,
            bundle.policy_fingerprint,
        }


def test_malformed_lineage_is_hidden_by_normal_reads(clean_runtime: str) -> None:
    deployment = "nomos-source-lineage-contract"
    scope = f"deployment:{deployment}"
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:source-lineage-contract",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_public_docs",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:reader"],
                "actions": ["graph.read"],
                "match": {
                    "deployment": deployment,
                    "scope": scope,
                    "source_id": "public_docs",
                },
            },
        ],
    })
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:contract",
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:visible",
                    "document",
                    {"classification": "internal", "text": "visible"},
                    {"id": "visible"},
                    {"rev": "1"},
                )
            ],
            source="public_docs",
            policy_fingerprint="sha256:publish-policy",
        )
        store.apply_facts(
            [
                NodeFact(
                    "doc:malformed",
                    "document",
                    {
                        "classification": "internal",
                        "text": "malformed",
                        LINEAGE_ATTR: {
                            "schema": LINEAGE_SCHEMA,
                            "status": "malformed",
                            "source_ids": ["public_docs"],
                            "sources": [{"source_id": "public_docs"}],
                        },
                    },
                    {"id": "malformed"},
                    {"rev": "1"},
                )
            ],
            source=None,
        )
        generation_id = store.checkpoint_generation(0, deployment_name=deployment)
        rows = read_nodes_with_active_policy(
            conn,
            deployment_name=deployment,
            scope=scope,
            subject="subject:reader",
            generation_id=generation_id,
        )
        conn.commit()

    assert [row["node_id"] for row in rows] == ["doc:visible"]


def test_legacy_row_columns_backfill_lineage_for_migration_reads() -> None:
    lineage = lineage_for_row(
        {},
        source_id="legacy_docs",
        source_record_id={"path": "docs/a.md"},
        source_revision={"commit": "abc123"},
    )

    assert lineage == {
        "schema": LINEAGE_SCHEMA,
        "status": "ok",
        "source_ids": ["legacy_docs"],
        "sources": [
            {
                "source_id": "legacy_docs",
                "source_records": [
                    {
                        "source_record_id": {"path": "docs/a.md"},
                        "source_revision": {"commit": "abc123"},
                    }
                ],
                "source_record_ids": [{"path": "docs/a.md"}],
                "source_revisions": [{"commit": "abc123"}],
            }
        ],
    }


# --- Laundered derived lineage: repair and doctor flag (issue #1544) ---------
#
# Every derived row written between PR #788 (2026-07-18, commit a083b466b1) and
# PR #1535 stores its EMITTING producer as its lineage source. The tests below
# seed rows in exactly that shape — by applying facts through the same graph
# write path with `derived_reapplication` left at its default False, which IS
# the #788-era publisher behaviour — and prove the repair restores the root
# lineage, that the doctor flags the rows until it does, and that a row whose
# lineage cannot be re-derived is REPORTED rather than skipped or guessed.

_LAUNDERING_EMITTER = "_enricher:laundry"
# A second, non-`_enricher:` producer family. Only `_enricher:*` rows are
# forced to carry a `derived_support` declaration at the graph write boundary,
# so an extractor-sourced derived row can legitimately exist with NO declared
# inputs at all — which is exactly the row the repair must refuse rather than
# invent a lineage for.
_LAUNDERING_EXTRACTOR = "_extractor:laundry"


def _seed_derived_producer(dsn: str, deployment: str) -> None:
    """Record the emitters the way the #788-era publisher recorded them.

    The publisher writes a `publisher_derived:<stage>` application envelope
    under the EMITTING producer's id and mints a `producer_profiles` row for
    it. Those two relations are what the repair reads to learn which graph
    `source` values are producers rather than real sources.
    """
    producers = (
        (_LAUNDERING_EMITTER, "enricher"),
        (_LAUNDERING_EXTRACTOR, "extractor"),
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            for producer, kind in producers:
                cur.execute(
                    """
                    INSERT INTO okg.producer_profiles (
                        deployment_name, producer_id, producer_kind,
                        trust_label, admission_mode, profile_fingerprint,
                        status
                    ) VALUES (
                        %(deployment)s, %(producer)s, %(kind)s,
                        'derived-deterministic-trusted', 'fast_track',
                        %(fingerprint)s, 'active'
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    {
                        "deployment": deployment,
                        "producer": producer,
                        "kind": kind,
                        "fingerprint": f"sha256:test-{kind}",
                    },
                )
                cur.execute(
                    """
                    INSERT INTO okg.admission_envelopes (
                        deployment_name, producer_id, producer_kind, intent,
                        admission_mode, status, idempotency_key
                    ) VALUES (
                        %(deployment)s, %(producer)s, %(kind)s,
                        'publisher_derived:pre_view', 'fast_track',
                        'accepted', %(key)s
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    {
                        "deployment": deployment,
                        "producer": producer,
                        "kind": kind,
                        "key": f"test-laundered-1544-{kind}",
                    },
                )


def _seed_laundered_graph(dsn: str) -> None:
    """Write one root source node, one laundered derived node and edge, and
    one laundered derived node that declares no inputs at all."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        for node_id, source_id in (
            ("doc:public", "public_docs"),
            ("doc:google", "google_drive"),
        ):
            store.apply_facts(
                [
                    NodeFact(
                        node_id,
                        "document",
                        {"text": node_id},
                        {"id": node_id},
                        {"rev": "1"},
                    )
                ],
                source=source_id,
            )
        # The #788-era derived re-application: attributed to the emitting
        # producer, `derived_reapplication` left at its default False, so the
        # lineage envelope is OVERWRITTEN with the emitter's own id and the
        # derived-provenance stamps never run.
        store.apply_facts(
            [
                NodeFact(
                    "derived:mid",
                    "document",
                    {"text": "derived"},
                    {
                        "id": "derived:mid",
                        "derived_from": ["doc:public"],
                        "derived_support": {
                            "schema": "okg.derived_support.v1",
                            "producer": _LAUNDERING_EMITTER,
                            "members": ["doc:public"],
                            "policy": "all_members",
                        },
                    },
                    {"rev": "1"},
                ),
            ],
            source=_LAUNDERING_EMITTER,
        )
        # A genuinely NON-DERIVABLE laundered row: it declares an input that
        # is not in the graph, so its root lineage cannot be rebuilt from
        # anything. The repair must refuse it by name, not invent one.
        store.apply_facts(
            [
                NodeFact(
                    "derived:dangling",
                    "document",
                    {"text": "dangling"},
                    {
                        "id": "derived:dangling",
                        "derived_from": ["doc:vanished"],
                    },
                    {"rev": "1"},
                ),
            ],
            source=_LAUNDERING_EXTRACTOR,
        )
        store.apply_facts(
            [
                EdgeFact(
                    "derived:mid",
                    "doc:google",
                    "references",
                    {"kind": "derived"},
                )
            ],
            source=_LAUNDERING_EMITTER,
        )


def test_laundered_derived_rows_are_invisible_to_the_legacy_restamp(
    clean_runtime: str,
) -> None:
    """Fail-before shape: the #1544 rows exist and the old repair finds none.

    `restamp_publisher_derived_lineage` selects `WHERE source =
    '_publisher_derived'`, the literal PR #788 retired, so it scans the
    laundered rows and restamps nothing. Without this the new repair could
    "pass" against rows the old one already handled.
    """
    deployment = "nomos-laundered-derived-legacy"
    _seed_derived_producer(clean_runtime, deployment)
    _seed_laundered_graph(clean_runtime)

    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        attrs = store.checkout().query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'derived:mid'"
        )[0]["attrs"]
        legacy = restamp_publisher_derived_lineage(
            conn,
            deployment_name=deployment,
        )
        conn.commit()

    # The node names its own emitter, and that reads as perfectly valid
    # lineage — which is why nothing flagged it.
    assert source_ids_from_lineage(attrs[LINEAGE_ATTR]) == (_LAUNDERING_EMITTER,)
    assert legacy["nodes"]["scanned"] == 0
    assert legacy["nodes"]["restamped"] == 0
    assert legacy["edges"]["restamped"] == 0


def test_repair_restores_root_lineage_and_reports_the_non_derivable_row(
    clean_runtime: str,
) -> None:
    deployment = "nomos-laundered-derived-repair"
    _seed_derived_producer(clean_runtime, deployment)
    _seed_laundered_graph(clean_runtime)

    with psycopg.connect(clean_runtime) as conn:
        result = repair_laundered_derived_lineage(
            conn,
            deployment_name=deployment,
        )
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        graph = store.checkout()
        node_attrs = graph.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'derived:mid'"
        )[0]["attrs"]
        orphan = graph.query(
            "SELECT attrs, source FROM okg.graph_nodes "
            "WHERE node_id = 'derived:dangling'"
        )[0]
        edge_attrs = graph.query(
            """
            SELECT attrs
              FROM okg.graph_edges
             WHERE src = 'derived:mid'
               AND dst = 'doc:google'
               AND edge_type = 'references'
            """
        )[0]["attrs"]
        conn.commit()

    assert result["ok"] is True
    assert _LAUNDERING_EMITTER in result["producer_inventory"]["producer_ids"]

    # The derived node now names the ROOT source it was declared from, and
    # no longer names the producer that emitted it.
    assert source_ids_from_lineage(node_attrs[LINEAGE_ATTR]) == ("public_docs",)
    assert result["nodes"]["repaired"] == 1

    # The derived edge is recombined from its two endpoints.
    assert source_ids_from_lineage(edge_attrs[LINEAGE_ATTR]) == (
        "google_drive",
        "public_docs",
    )
    assert result["edges"]["repaired"] == 1

    # REFUSED, not skipped: the row that declared no inputs is named in the
    # report with a reason, and its stored lineage is untouched.
    assert result["nodes"]["unrepairable"] == 1
    refused = result["nodes"]["unrepairable_rows"][0]
    assert refused["node_id"] == "derived:dangling"
    assert refused["reason"] == "declared_input_not_found"
    assert refused["declared_inputs"] == ["doc:vanished"]
    assert result["complete"] is False
    assert source_ids_from_lineage(orphan["attrs"][LINEAGE_ATTR]) == (
        _LAUNDERING_EXTRACTOR,
    )


def test_repair_is_idempotent_and_dry_run_writes_nothing(
    clean_runtime: str,
) -> None:
    deployment = "nomos-laundered-derived-idempotent"
    _seed_derived_producer(clean_runtime, deployment)
    _seed_laundered_graph(clean_runtime)

    with psycopg.connect(clean_runtime) as conn:
        dry = scan_laundered_derived_lineage(
            conn,
            deployment_name=deployment,
        )
        conn.rollback()
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        after_dry = store.checkout().query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'derived:mid'"
        )[0]["attrs"]
        conn.commit()

    assert dry["dry_run"] is True
    assert dry["nodes"]["repaired"] == 1
    # A dry run REPORTS the repair it would make and leaves the row laundered.
    assert source_ids_from_lineage(after_dry[LINEAGE_ATTR]) == (
        _LAUNDERING_EMITTER,
    )

    with psycopg.connect(clean_runtime) as conn:
        first = repair_laundered_derived_lineage(
            conn,
            deployment_name=deployment,
        )
        conn.commit()
    with psycopg.connect(clean_runtime) as conn:
        second = repair_laundered_derived_lineage(
            conn,
            deployment_name=deployment,
        )
        conn.commit()

    assert first["nodes"]["repaired"] == 1
    assert first["edges"]["repaired"] == 1
    # Second run: the repaired rows no longer name a producer, so only the
    # refused row is still laundered and nothing is rewritten.
    assert second["nodes"]["repaired"] == 0
    assert second["edges"]["repaired"] == 0
    assert second["nodes"]["laundered"] == 1
    assert second["nodes"]["unrepairable"] == 1
    assert second["edges"]["laundered"] == 0


def test_doctor_flags_laundered_rows_until_the_repair_clears_them(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The readiness rollup must go red on rows the missing-lineage count
    cannot see, and go green again only after the repair runs."""
    from okg.substrate.nomos import doctor as nomos_doctor

    deployment = "nomos-laundered-derived-doctor"
    _seed_derived_producer(clean_runtime, deployment)
    _seed_laundered_graph(clean_runtime)
    _activate_laundering_policy(clean_runtime, deployment)
    monkeypatch.setattr(
        nomos_doctor,
        "_registry_source_ids",
        lambda _deployment_name: {
            "status": "ok",
            "source_ids": ["google_drive", "public_docs"],
            "path": "test/source_registry.yaml",
        },
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        generation_id = store.checkpoint_generation(
            0,
            deployment_name=deployment,
        )

    with psycopg.connect(clean_runtime) as conn:
        before = nomos_doctor.source_lineage_readiness_report(
            conn,
            deployment_name=deployment,
            readiness_profile="dogfood",
            generation_id=generation_id,
        )
        conn.commit()

    laundered = before["laundered_derived_lineage"]
    assert laundered["measured"] is True
    # 2 nodes (derived:mid, derived:dangling) and 1 edge name the emitter.
    assert laundered["node_count"] == 2
    assert laundered["edge_count"] == 1
    assert laundered["total_count"] == 3
    # These rows are INVISIBLE to the pre-existing missing-lineage count,
    # which is the whole defect: valid-looking lineage naming the wrong id.
    assert before["missing_lineage"]["node_count"] == 0
    assert before["missing_lineage"]["edge_count"] == 0
    assert before["ok"] is False
    assert before["mode"] == "quarantine"
    assert [error["code"] for error in before["errors"]] == [
        "nomos_source_lineage_derived_producer_laundered",
    ]

    with psycopg.connect(clean_runtime) as conn:
        repair = repair_laundered_derived_lineage(
            conn,
            deployment_name=deployment,
        )
        conn.commit()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        partial_generation_id = store.checkpoint_generation(
            1,
            deployment_name=deployment,
        )
    with psycopg.connect(clean_runtime) as conn:
        partial = nomos_doctor.source_lineage_readiness_report(
            conn,
            deployment_name=deployment,
            readiness_profile="dogfood",
            generation_id=partial_generation_id,
        )
        conn.commit()

    assert repair["nodes"]["repaired"] == 1
    assert repair["edges"]["repaired"] == 1
    # The flag does NOT clear while the refused row is still there. The repair
    # will not invent a lineage for it, so retracting or re-emitting it is an
    # operator decision, and readiness stays red until one is made.
    assert partial["laundered_derived_lineage"]["total_count"] == 1
    assert partial["laundered_derived_lineage"]["node_count"] == 1
    assert partial["ok"] is False
    assert "nomos_source_lineage_derived_producer_laundered" in {
        error["code"] for error in partial["errors"]
    }

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        session = store.checkout()
        with session.transaction():
            session.execute(
                "DELETE FROM okg.graph_nodes WHERE node_id = 'derived:dangling'"
            )
        generation_id = store.checkpoint_generation(
            2,
            deployment_name=deployment,
        )
    with psycopg.connect(clean_runtime) as conn:
        after = nomos_doctor.source_lineage_readiness_report(
            conn,
            deployment_name=deployment,
            readiness_profile="dogfood",
            generation_id=generation_id,
        )
        conn.commit()

    assert after["laundered_derived_lineage"]["total_count"] == 0
    assert [
        error["code"] for error in after["errors"]
        if error["code"].startswith("nomos_source_lineage_derived")
    ] == []


def test_the_doctor_laundering_count_is_exact_not_sampled(
    clean_runtime: str,
) -> None:
    """A laundered row past the readiness sample boundary must still count.

    The surrounding lineage checks read the first 10000 rows of the published
    generation. A count that has to reach ZERO cannot be read off that sample:
    one laundered row ordered after the boundary would make a partial scan
    report clean.

    Seeding 10000 rows to cross the real boundary would cost more than it
    proves, so this pins the two things that make the count exact instead:
    the block does not claim to be sampled, and the paging that carries the
    scan past a batch boundary is driven directly at `batch_size=1` — the
    smallest page that forces every continuation.
    """
    from okg.substrate.nomos.doctor import _laundered_derived_lineage_report
    from okg.substrate.nomos.source_lineage_repair import (
        count_laundered_derived_rows,
        derived_producer_inventory,
    )

    deployment = "nomos-laundered-derived-exact"
    _seed_derived_producer(clean_runtime, deployment)
    _seed_laundered_graph(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "zzz:last-in-sort-order",
                    "document",
                    {"text": "tail"},
                    {
                        "id": "zzz:last-in-sort-order",
                        "derived_from": ["doc:public"],
                    },
                    {"rev": "1"},
                ),
            ],
            source=_LAUNDERING_EXTRACTOR,
        )

    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        report = _laundered_derived_lineage_report(
            conn,
            store.checkout(),
            deployment_name=deployment,
        )
        conn.commit()

    assert report["measured"] is True
    assert report["sampled"] is False
    # derived:mid, derived:dangling and the tail row, plus the derived edge.
    assert report["node_count"] == 3
    assert report["edge_count"] == 1
    assert report["total_count"] == 4

    with psycopg.connect(clean_runtime) as conn:
        producer_ids = frozenset(
            derived_producer_inventory(
                conn,
                deployment_name=deployment,
            )["producer_ids"]
        )
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        paged = count_laundered_derived_rows(
            store.checkout(),
            producer_ids=producer_ids,
            batch_size=1,
        )
        conn.commit()

    # One row per page: the same totals only come out if every continuation
    # runs. A scan that stopped at its first page would report 1 and 1.
    assert paged["nodes"]["producer_named"] == 3
    assert paged["edges"]["producer_named"] == 1


def test_a_self_naming_system_producer_is_not_flagged_as_laundered(
    clean_runtime: str,
) -> None:
    """`_catalog:restamp` and its siblings name themselves, correctly.

    Several substrate producers write through `apply_bounded_fact_batch` with
    `source=producer_id` and NO re-application flag, so the producer id in
    their lineage IS their provenance.
    `deployment_upgrade._nomos_policy_source_ids` blesses `_catalog:restamp`
    and `system:external_backed_collector` into every deployment's Nomos
    baseline for exactly that reason. Flagging them would turn readiness red on
    a healthy deployment after its first catalog apply, and no repair could
    clear it — the rows declare no inputs, so there is nothing to re-derive.

    Two independent guards keep that from happening, and this pins both: the
    producer-id set covers only derived-OUTPUT kinds, and, whatever that set
    contains, a row with no derivation declaration is never classified.
    """
    from okg.substrate.nomos.doctor import _laundered_derived_lineage_report
    from okg.substrate.nomos.source_lineage_repair import (
        derived_producer_inventory,
        laundered_row_kind,
    )

    deployment = "nomos-laundered-derived-self-naming"
    restamp = "_catalog:restamp"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.producer_profiles (
                    deployment_name, producer_id, producer_kind, trust_label,
                    admission_mode, profile_fingerprint, status
                ) VALUES (
                    %(deployment)s, %(producer)s, 'system', 'system-trusted',
                    'direct', 'sha256:test-catalog-restamp', 'active'
                )
                ON CONFLICT DO NOTHING
                """,
                {"deployment": deployment, "producer": restamp},
            )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:restamped",
                    "document",
                    {"text": "restamped"},
                    {"id": "doc:restamped"},
                    {"rev": "1"},
                )
            ],
            source=restamp,
        )

    with psycopg.connect(clean_runtime) as conn:
        inventory = derived_producer_inventory(
            conn,
            deployment_name=deployment,
        )
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        attrs = store.checkout().query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'doc:restamped'"
        )[0]["attrs"]
        report = _laundered_derived_lineage_report(
            conn,
            store.checkout(),
            deployment_name=deployment,
        )
        repair = repair_laundered_derived_lineage(
            conn,
            deployment_name=deployment,
        )
        conn.commit()

    # The row really does name its own producer — this is not a case the
    # fixture avoided, it is the case the guards have to survive.
    assert source_ids_from_lineage(attrs[LINEAGE_ATTR]) == (restamp,)
    assert restamp not in inventory["producer_ids"]
    assert report["total_count"] == 0
    assert repair["complete"] is True
    assert repair["nodes"]["unrepairable"] == 0
    # Guard two, independently: even if the id set did contain it, a row with
    # no derivation declaration is not classified.
    assert laundered_row_kind(
        attrs,
        restamp,
        producer_ids=frozenset({restamp}),
        source_record_id={"id": "doc:restamped"},
    ) is None


def test_an_absent_laundering_measurement_never_reads_as_zero() -> None:
    """An unscannable graph must not report a clean laundering count.

    Counts are ``None``, not ``0``, and the readiness chain turns that into
    its own error code. Reporting zero for an absent measurement is exactly
    how these rows stayed invisible for six weeks (#1544).
    """
    from okg.substrate.nomos.doctor import (
        _empty_lineage_graph_report,
        _unmeasured_laundered_report,
    )

    for status in ("not_checked", "missing", "error"):
        block = _empty_lineage_graph_report(status)["laundered_derived_lineage"]
        assert block["measured"] is False
        assert block["node_count"] is None
        assert block["edge_count"] is None
        assert block["total_count"] is None

    unmeasured = _unmeasured_laundered_report("producer_inventory_unavailable")
    assert unmeasured["measured"] is False
    assert unmeasured["total_count"] is None


def test_producer_inventory_refuses_when_neither_relation_can_be_read(
    clean_runtime: str,
) -> None:
    from okg.substrate.nomos.source_lineage_repair import (
        derived_producer_inventory,
    )

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE okg.producer_profiles RENAME TO pp_hidden")
            cur.execute(
                "ALTER TABLE okg.admission_envelopes RENAME TO ae_hidden"
            )
        inventory = derived_producer_inventory(
            conn,
            deployment_name="nomos-laundered-derived-unmeasured",
        )
        repair = repair_laundered_derived_lineage(
            conn,
            deployment_name="nomos-laundered-derived-unmeasured",
        )
        conn.rollback()

    assert inventory["measured"] is False
    assert inventory["status"] == "producer_inventory_unavailable"
    assert inventory["producer_ids"] is None
    assert {error["relation"] for error in inventory["errors"]} == {
        "admission_envelopes",
        "producer_profiles",
    }
    # The repair refuses outright rather than treating "no producers" as
    # "nothing laundered".
    assert repair["ok"] is False
    assert repair["reason"] == "producer_inventory_unavailable"


def test_readiness_errors_when_the_laundering_scan_cannot_be_taken(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The unmeasured arm has to actually fire, not just exist.

    A shape test on the unmeasured payload would stay green with the
    `not laundered["measured"]` branch reverted, so this drives the real
    readiness report with an inventory that cannot be read and asserts the
    deployment is quarantined rather than reported healthy.
    """
    from okg.substrate.nomos import doctor as nomos_doctor
    from okg.substrate.nomos import source_lineage_repair

    deployment = "nomos-laundered-derived-unscannable"
    _seed_derived_producer(clean_runtime, deployment)
    _seed_laundered_graph(clean_runtime)
    _activate_laundering_policy(clean_runtime, deployment)
    monkeypatch.setattr(
        nomos_doctor,
        "_registry_source_ids",
        lambda _deployment_name: {
            "status": "ok",
            "source_ids": ["google_drive", "public_docs"],
            "path": "test/source_registry.yaml",
        },
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(
        source_lineage_repair,
        "derived_producer_inventory",
        lambda _conn, **_kwargs: {
            "measured": False,
            "status": "producer_inventory_unavailable",
            "producer_ids": None,
            "from_admission_envelopes": None,
            "from_producer_profiles": None,
            "errors": [{"relation": "producer_profiles", "error": "test"}],
        },
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        generation_id = store.checkpoint_generation(
            0,
            deployment_name=deployment,
        )

    with psycopg.connect(clean_runtime) as conn:
        report = nomos_doctor.source_lineage_readiness_report(
            conn,
            deployment_name=deployment,
            readiness_profile="dogfood",
            generation_id=generation_id,
        )
        conn.commit()

    block = report["laundered_derived_lineage"]
    assert block["measured"] is False
    assert block["total_count"] is None
    assert report["ok"] is False
    assert report["mode"] == "quarantine"
    assert "nomos_source_lineage_laundered_scan_unavailable" in {
        error["code"] for error in report["errors"]
    }


def test_the_rollout_audit_recovers_a_laundered_deployment_with_the_repair() -> None:
    """A laundered deployment must be handed the repair verb, not `migrate`.

    Laundered rows quarantine the lineage report, and the generic quarantine
    arm recommends `okg migrate --apply`, which cannot touch them. The new
    branch has to win, and it has to name the deployment.
    """
    from okg.substrate.cli.nomos_rollout_audit import (
        _nomos_source_lineage_rollout_state_from_runtime_reports,
    )

    entry = {
        "name": "laundered-deployment",
        "kind": "deployment",
        "path": "deployments/laundered-deployment/deployment.yaml",
    }
    state = _nomos_source_lineage_rollout_state_from_runtime_reports(
        entry,
        active_state={"ok": True, "findings": []},
        readiness_profile="dogfood",
        lineage_report={
            "ok": False,
            "mode": "quarantine",
            "errors": [
                {"code": "nomos_source_lineage_derived_producer_laundered"},
            ],
        },
    )

    assert state["status"] == "blocked"
    assert "--repair-laundered-derived laundered-deployment" in (
        state["recovery_command"]
    )
    assert "migrate --apply" not in state["recovery_command"]


def _activate_laundering_policy(dsn: str, deployment: str) -> None:
    scope = f"deployment:{deployment}"
    bundle = compile_policy_graph({
        "policy_bundle_id": f"nomos:test:{deployment}",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": f"allow_{source_id.replace(':', '_')}_mcp_read",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:mcp_client"],
                "actions": ["graph.read.mcp"],
                "match": {
                    "deployment": deployment,
                    "scope": scope,
                    "source_id": source_id,
                },
            }
            # The producer ids are covered too: readiness unions graph
            # lineage source ids into policy_required_sources, and a
            # laundered row injects its EMITTER there. Without this the
            # missing-policy arm would fire first and mask the laundering
            # check this test exists to prove.
            for source_id in (
                "google_drive",
                "public_docs",
                _LAUNDERING_EMITTER,
                _LAUNDERING_EXTRACTOR,
            )
        ],
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:laundered-derived-repair",
            bootstrap=True,
        )
        conn.commit()
