"""fix-projection-alias-identity: bounded remediation of pre-fix damage.

(a) stranded link_cache (#685): a node stranded before the retry mechanism
    existed (no link_retry record) is recovered by a subtype-scoped link_cache
    invalidation drained by a normal projection cycle -- proven by a positive
    edge row delta.
(b) collapsed aliases (#686): the pass detects the collision and, under apply,
    quarantines the ambiguous alias row and invalidates the affected src nodes.
(c) multi-target enumeration (#710): read-only report of every offending
    (src, edge_type) group; never auto-picks a survivor.

Every pass is dry-run by default and mutates only under apply=True.
"""
from __future__ import annotations

import json
import uuid

import psycopg
import pytest

from okg.substrate.catalog.aliases import add_alias, seed_from_subtype
from okg.substrate.catalog.linker_config import LinkerRoute
from okg.substrate.graph_store import ChronosGraphStore, _edge_id
from okg.substrate.cli.main import main
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.projection import Projector, RegexEntityExtractor
from okg.substrate.publisher import Publisher
from okg.substrate.projection.remediation import (
    enumerate_multi_target_edges,
    remediate_collapsed_aliases,
    remediate_stranded_link_cache,
)
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _seed_generations(conn: psycopg.Connection, cv: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations
              (generation_id, deployment_name, status,
               catalog_version_id, published_at, correlation_id)
            SELECT g, 'default', 'published', %s, now(),
                   'default:generation:' || g::text
              FROM generate_series(1, 100) AS g
            ON CONFLICT (generation_id) DO NOTHING
            """,
            (cv,),
        )


def _insert_page(conn: psycopg.Connection, node_id: str, text: str) -> None:
    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES (%s, 'page', %s::jsonb, 'fixture', %s::jsonb, %s::jsonb,
                    now(), %s, %s, 'I')
            """,
            (
                node_id, json.dumps({"text": text}),
                json.dumps({"id": node_id}), json.dumps({"v": 1}),
                f"{run_id}:{node_id}", run_id,
            ),
        )


def _references_count(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.edge_facts "
            " WHERE edge_type = 'references' AND op = 'I'"
        )
        return int(cur.fetchone()[0])


def _projector(dsn: str) -> Projector:
    return Projector(
        dsn,
        extractor=RegexEntityExtractor([("concept", r"\b(dm)\b")]),
        routes=[
            LinkerRoute(
                entity_type="concept", src_subtype=None,
                edge_type="references", dst_template="{resolved}",
            )
        ],
        extractor_source_subtypes=["page"],
    )


def test_remediate_stranded_link_cache_recovers_edge_row_delta(
    clean_runtime: str,
) -> None:
    from tests.substrate.test_publisher_chronos_alias_recovery import (
        _publisher as production_publisher,
        _seed_catalog_and_graph,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)

    publisher = production_publisher(clean_runtime)
    assert publisher.cycle()["status"] == "published"

    # Simulate PRE-FIX damage: the strand left no retry record.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM okg.link_retry")
        # The alias lands now, but with no retry row the landing hook can't
        # clear the stamped cache -> the node stays a permanent cache hit.
        add_alias(
            conn, alias="dm", canonical="concept:diabetes", scope="concept",
        )

    # Alias-only cycle remains reused because the historical miss had no row.
    assert publisher.cycle().get("reused_generation") is True
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Dry-run pages the production source node and mutates nothing.
        dry = remediate_stranded_link_cache(
            conn,
            extractor_source_subtypes=["page"],
            deployment_name="test",
            apply=False,
            limit=1,
        )
        assert dry.applied is False
        assert dry.affected_count == 1
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.extractor_retry_queue")
            assert int(cur.fetchone()[0]) == 0

        applied = remediate_stranded_link_cache(
            conn,
            extractor_source_subtypes=["page"],
            deployment_name="test",
            apply=True,
            limit=1,
        )
        assert applied.applied is True
        assert applied.affected_count == 1

    recovered = publisher.cycle()
    assert recovered["status"] == "published"
    assert recovered["edges_added"] >= 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.extractor_retry_queue")
            assert int(cur.fetchone()[0]) == 0
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(recovered["generation_id"])
        assert graph.query(
            "SELECT src, dst FROM okg.graph_edges WHERE edge_type='mentions'"
        ) == [{"src": "page:one", "dst": "concept:diabetes"}]


def test_remediate_collapsed_aliases_detects_and_quarantines(
    clean_runtime: str,
    capsys,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["author"], narrowings=[])
        _seed_generations(conn, cv)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([
            NodeFact("author:a", "author", {"name": "J. Smith"}, {"id": "a"}, {"v": 1}),
            NodeFact("author:b", "author", {"name": "J. Smith"}, {"id": "b"}, {"v": 1}),
            NodeFact(
                "doc:x", "author",
                {"name": "doc", "text": "J. Smith"},
                {"id": "x"}, {"v": 1},
            ),
            NodeFact("doc:explicit", "author", {"name": "explicit"}, {}, {}),
            NodeFact(
                "doc:y", "author", {"name": "doc-y", "text": "J. Smith"},
                {}, {},
            ),
        ])
        store.close()
        # A derived edge minted through the ambiguous value (pre-fix wrong link).
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases (alias, canonical, scope, "
                " catalog_version_id) VALUES ('J. Smith', 'author:a', NULL, %s)",
                (cv,),
            )
        ChronosGraphStore.from_connection(conn).apply_facts([
            EdgeFact(
                "doc:x", "author:a", "mentions",
                attrs={"entity_value": "J. Smith"},
                source_record_id={"id": "m"}, source_revision={"v": 1},
            ),
            EdgeFact(
                "doc:y", "author:a", "mentions",
                attrs={"entity_value": "J. Smith"},
                source_record_id={"id": "m2"}, source_revision={"v": 1},
            ),
        ], source="_extractor:person", strict_endpoints=False)
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts([
            EdgeFact(
                "doc:explicit", "author:b", "mentions",
                attrs={"entity_value": "J. Smith"},
                source_record_id={"id": "explicit"},
                source_revision={"v": 1},
            ),
        ], source="human-curated", strict_endpoints=False)

    publisher = Publisher(
        clean_runtime,
        "default",
        extractor=RegexEntityExtractor([("person", r"J\. Smith")]),
        extractor_source_subtypes=["author"],
        routes=[LinkerRoute(
            entity_type="person", src_subtype=None,
            edge_type="mentions", dst_template="{resolved}",
        )],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    )
    before = publisher.cycle()
    assert before["status"] == "published"
    # The test writer binding clones the seeded catalog (aliases included)
    # into a deployment-bound version, which becomes the active catalog the
    # publish and the remediation run use. Assert against that version.
    assert int(before["catalog_version_id"]) > cv
    cv = int(before["catalog_version_id"])

    # A matching extractor edge added after the published pin must not enter
    # this run's stable page.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts([
            NodeFact("doc:late", "author", {"name": "late"}, {}, {}),
            EdgeFact(
                "doc:late", "author:a", "mentions",
                attrs={"entity_value": "J. Smith"},
                source_record_id={"id": "late"}, source_revision={"v": 2},
            ),
        ], source="_extractor:person", strict_endpoints=False)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        dry = remediate_collapsed_aliases(
            conn, subtype="author", field="name", apply=False, limit=1,
        )
        assert dry.applied is False
        assert dry.affected_count == 1
        assert dry.details["derived_edge_count"] == 1
        assert len(dry.details["affected_src_node_ids"]) == 1
        assert dry.details["next_cursor"] is not None
        # Nothing mutated by dry-run.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.aliases "
                " WHERE alias = 'J. Smith' AND catalog_version_id = %s",
                (cv,),
            )
            assert int(cur.fetchone()[0]) == 1

        applied = remediate_collapsed_aliases(
            conn, subtype="author", field="name", apply=True, limit=1,
        )
        assert applied.applied is True
        assert applied.details["aliases_quarantined"] == 1
        assert applied.details["retracted_edge_count"] == 1
        repeated = remediate_collapsed_aliases(
            conn,
            subtype="author",
            field="name",
            apply=True,
            limit=1,
            run_id=applied.details["run_id"],
        )
        assert repeated.details["idempotency_key"] == applied.details[
            "idempotency_key"
        ]
        # Resolve the source collision on mutable HEAD between pages. The run
        # must still use its persisted collision snapshot and pinned checkpoint.
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts([
            NodeFact("author:a", "author", {"name": "Alice Smith"}, {}, {"v": 2}),
            NodeFact("author:b", "author", {"name": "Bob Smith"}, {}, {"v": 2}),
        ])
        second = remediate_collapsed_aliases(
            conn,
            subtype="author",
            field="name",
            apply=True,
            limit=1,
            run_id=applied.details["run_id"],
            cursor=applied.details["next_cursor"],
        )
        assert second.details["retracted_edge_count"] == 1
        assert second.details["next_cursor"] is None
        with conn.cursor() as cur:
            cur.execute(
                "SELECT catalog_version_id, collision_snapshot "
                "FROM okg.projection_remediation_runs WHERE run_id = %s",
                (applied.details["run_id"],),
            )
            run_cv, collision_snapshot = cur.fetchone()
            assert int(run_cv) == cv
            assert [c["alias_value"] for c in collision_snapshot] == ["J. Smith"]
            cur.execute(
                "SELECT count(*) FROM okg.aliases "
                " WHERE alias = 'J. Smith' AND catalog_version_id = %s",
                (cv,),
            )
            assert int(cur.fetchone()[0]) == 0  # quarantined

    after = publisher.cycle()
    assert after["status"] == "published"
    assert after["edges_retracted"] >= 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.extractor_retry_queue")
            assert int(cur.fetchone()[0]) == 0
            cur.execute(
                "SELECT status FROM okg.projection_remediation_runs "
                "WHERE run_id = %s",
                (applied.details["run_id"],),
            )
            assert cur.fetchone()[0] == "completed"
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(after["generation_id"])
        surviving = graph.query(
            "SELECT src, source FROM okg.graph_edges "
            "WHERE attrs->>'entity_value' = 'J. Smith' ORDER BY src"
        )
        assert surviving == [
            {"src": "doc:explicit", "source": "human-curated"},
            {"src": "doc:late", "source": "_extractor:person"},
        ]

        # Operator fixes the underlying ambiguous source values; the field
        # sweep clears the standing collision and doctor returns clean.
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts([
            NodeFact(
                "author:a", "author", {"name": "Alice Smith"},
                {"id": "a"}, {"v": 2},
            ),
            NodeFact(
                "author:b", "author", {"name": "Bob Smith"},
                {"id": "b"}, {"v": 2},
            ),
        ])
        seed_from_subtype(
            conn, subtype="author", field="name", apply=True,
        )
    assert main([
        "--json", "doctor", "--check", "alias-collisions",
        "--dsn", clean_runtime,
    ]) == 0
    doctor = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert doctor["status"] == "OK"


def test_collapsed_alias_detection_reads_requested_branch_checkpoint(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["author"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([
            NodeFact("author:a", "author", {"name": "Alice"}, {}, {}),
            NodeFact("author:b", "author", {"name": "Bob"}, {}, {}),
        ])
        store.checkpoint_generation(
            branch="default", deployment_name="default", catalog_version_id=cv,
        )
        store.ensure_branch("repair", from_branch="default")
        store.apply_facts([
            NodeFact("author:a", "author", {"name": "J. Smith"}, {}, {"v": 2}),
            NodeFact("author:b", "author", {"name": "J. Smith"}, {}, {"v": 2}),
        ], branch="repair")
        store.checkpoint_generation(
            branch="repair", deployment_name="default", catalog_version_id=cv,
        )

        default = remediate_collapsed_aliases(
            conn,
            subtype="author",
            field="name",
            branch_name="default",
            apply=False,
            limit=10,
        )
        repair = remediate_collapsed_aliases(
            conn,
            subtype="author",
            field="name",
            branch_name="repair",
            apply=False,
            limit=10,
        )
        assert default.affected_count == 0
        assert repair.affected_count == 1


def test_projection_remediator_reads_but_never_writes_policy_and_ranking(
    clean_runtime: str,
) -> None:
    """Every fact write stamps the active Nomos policy fingerprint and reads
    the edge ranking, so the remediation role must read those tables (PACT
    remediator-nomos-policy-read), and only read them."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for table in (
            "okg.nomos_policy_bundles",
            "okg.nomos_policy_activations",
            # The edge ranking every edge write reads (graph store grant).
            "okg.edge_precedence",
        ):
            for privilege, expected in (
                ("SELECT", True),
                ("INSERT", False),
                ("UPDATE", False),
                ("DELETE", False),
            ):
                granted = conn.execute(
                    "SELECT has_table_privilege('projection_remediator', %s, %s)",
                    (table, privilege),
                ).fetchone()[0]
                assert granted is expected, (table, privilege, granted)


def test_projection_identity_roles_can_execute_required_deletes(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["author"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([
            NodeFact("author:a", "author", {"name": "J. Smith"}, {}, {}),
            NodeFact("author:b", "author", {"name": "J. Smith"}, {}, {}),
            NodeFact("doc:role", "author", {"text": "J. Smith"}, {}, {}),
            EdgeFact(
                "doc:role", "author:a", "mentions",
                attrs={"entity_value": "J. Smith"},
                source_record_id={"id": "role"}, source_revision={"v": 1},
            ),
        ], source="_extractor:person", strict_endpoints=False)
        store.checkpoint_generation(
            deployment_name="default", catalog_version_id=cv,
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, canonical, catalog_version_id) VALUES ('J. Smith','author:a',%s)",
                (cv,),
            )
            cur.execute("SET ROLE projection_remediator")
        try:
            result = remediate_collapsed_aliases(
                conn, subtype="author", field="name", apply=True, limit=10,
            )
            assert result.details["retracted_edge_count"] == 1
        finally:
            conn.execute("RESET ROLE")


def test_collapsed_alias_batch_rolls_back_all_mutations_on_receipt_fault(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(conn, subtypes=["author"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([
            NodeFact("author:a", "author", {"name": "J. Smith"}, {}, {}),
            NodeFact("author:b", "author", {"name": "J. Smith"}, {}, {}),
            NodeFact("doc:x", "author", {"text": "J. Smith"}, {}, {}),
            EdgeFact(
                "doc:x", "author:a", "mentions",
                attrs={"entity_value": "J. Smith"},
                source_record_id={"id": "wrong"}, source_revision={"v": 1},
            ),
        ], source="_extractor:person", strict_endpoints=False)
        store.checkpoint_generation(
            deployment_name="default", catalog_version_id=cv,
        )
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, canonical, catalog_version_id) VALUES ('J. Smith','author:a',%s)",
                (cv,),
            )

        real_apply = ChronosGraphStore.apply_facts

        def apply_then_fail(self, *args, **kwargs):
            real_apply(self, *args, **kwargs)
            raise RuntimeError("fault after graph edit before receipt")

        monkeypatch.setattr(ChronosGraphStore, "apply_facts", apply_then_fail)
        with pytest.raises(RuntimeError, match="before receipt"):
            remediate_collapsed_aliases(
                conn, subtype="author", field="name", apply=True, limit=10,
            )

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.extractor_retry_queue")
            assert int(cur.fetchone()[0]) == 0
            cur.execute("SELECT count(*) FROM okg.projection_remediation_batches")
            assert int(cur.fetchone()[0]) == 0
            cur.execute(
                "SELECT count(*) FROM okg.aliases WHERE alias = 'J. Smith' "
                "AND catalog_version_id = %s",
                (cv,),
            )
            assert int(cur.fetchone()[0]) == 1
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout()
        assert graph.query(
            "SELECT src FROM okg.graph_edges "
            "WHERE attrs->>'entity_value' = 'J. Smith'"
        ) == [{"src": "doc:x"}]


def test_enumerate_multi_target_edges_never_auto_picks(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    bib = f"bib:{sfx}"
    bib2 = f"bib:{sfx}:2"
    pa, pb = f"paper:{sfx}:a", f"paper:{sfx}:b"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = seed_catalog(
            conn,
            subtypes=["bib_reference", "paper"],
            narrowings=[("bib_reference", "resolves_to", "paper")],
        )
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.narrowings SET attr_rules = %s::jsonb "
                " WHERE catalog_version_id = %s AND parent_subtype = %s "
                "   AND edge_type = %s",
                (
                    json.dumps({"cardinality": "at_most_one_target"}),
                    cv, "bib_reference", "resolves_to",
                ),
            )
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(bib, "bib_reference", {}, {"id": bib}, {"v": 1}),
            NodeFact(bib2, "bib_reference", {}, {"id": bib2}, {"v": 1}),
            NodeFact(pa, "paper", {}, {"id": pa}, {"v": 1}),
            NodeFact(pb, "paper", {}, {"id": pb}, {"v": 1}),
            EdgeFact(bib, pa, "resolves_to", source_record_id={"id": "1"}, source_revision={"v": 1}),
            EdgeFact(bib, pb, "resolves_to", source_record_id={"id": "2"}, source_revision={"v": 1}),
            EdgeFact(bib2, pa, "resolves_to", source_record_id={"id": "3"}, source_revision={"v": 1}),
            EdgeFact(bib2, pb, "resolves_to", source_record_id={"id": "4"}, source_revision={"v": 1}),
        ], source="default", strict_endpoints=True)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        groups = enumerate_multi_target_edges(conn, catalog_version_id=cv)

    assert len(groups) == 2
    grp = groups[0]
    assert grp.src == bib
    assert grp.edge_type == "resolves_to"
    # BOTH targets are reported -- no survivor is picked.
    assert {t["dst"] for t in grp.targets} == {pa, pb}
    assert {t["edge_id"] for t in grp.targets} == {
        _edge_id(bib, "resolves_to", pa),
        _edge_id(bib, "resolves_to", pb),
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        first_page = enumerate_multi_target_edges(
            conn, catalog_version_id=cv, limit=1,
        )
        assert len(first_page) == 1
        cursor = json.dumps([first_page[0].src, first_page[0].edge_type])
        second_page = enumerate_multi_target_edges(
            conn, catalog_version_id=cv, cursor=cursor, limit=1,
        )
        assert [group.src for group in second_page] == [bib2]
