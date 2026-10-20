"""Link endpoints through real publishes (issue #3666, PACT link-endpoints-3666).

A commit cites two Jira keys. One record is loaded; the other is not, and the
route declares `on_miss`. The publish keeps both links: the missing one
reaches a "known by key, not loaded" endpoint at the record's real id. The
endpoint registers no alias, so its retry row keeps waiting for the real
record. When that record lands, the same edge id stays live and the
endpoint is withdrawn; when no link reaches an endpoint, it is retracted.
"""
from __future__ import annotations

import json
from typing import Any, Optional
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.catalog.aliases import add_alias
from okg.substrate.catalog.linker_config import (
    LINK_ENDPOINT_SOURCE,
    LinkEndpointSpec,
    LinkerRoute,
)
from okg.substrate.graph_store import ChronosGraphStore, NodeFact
from okg.substrate.producer_authority import materialize_producer_profile
from okg.substrate.projection import RegexEntityExtractor
from okg.substrate.projection.remediation import backfill_link_endpoints
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

DEPLOYMENT = "test"
CITING = "commit:r:abc"
OTHER = "commit:r:def"
MISSING = "jira:CMSTRANSF-452"
LOADED = "jira:CMSTRANSF-1"
SOURCE = "link-endpoint-fixture"

ENDPOINT_TEXT = (
    "CMSTRANSF-452 (known by key, not loaded: the graph holds no record for it)"
)

SPEC = LinkEndpointSpec(
    endpoint_id="jira:{value}",
    subtype="jira_issue",
    key_attr="issue_key",
    scope_attr="project",
    scope_regex=r"^([A-Z0-9]+)-",
)


class ExactResolver:
    """Exact alias lookup at the latest applied catalog version."""

    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn

    def resolve(self, entity: Any) -> Optional[str]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT canonical FROM okg.aliases WHERE alias = %s "
                "AND catalog_version_id = (SELECT max(catalog_version_id) "
                "FROM okg.catalog_versions WHERE status = 'applied')",
                (entity.value,),
            )
            row = cur.fetchone()
        return str(row[0]) if row else None


#: The CMS catalog's jira_issue declarations an endpoint meets: its key is an
#: alias field (seeded from the graph on every catalog apply) and a
#: searchable text field.
JIRA_METADATA = {"alias_fields": ["issue_key"], "searchable_text_fields": ["issue_key"]}


def seed(
    conn: psycopg.Connection,
    *,
    deployment: str = DEPLOYMENT,
    jira_metadata: Optional[dict[str, Any]] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = int(cur.fetchone()[0])
        cur.executemany(
            "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
            "VALUES (%s, %s::jsonb, %s)",
            [
                ("git_commit", "{}", cv),
                ("jira_issue", json.dumps(jira_metadata or {}), cv),
            ],
        )
        cur.executemany(
            "INSERT INTO okg.subtype_ancestors "
            "(catalog_version_id, subtype_name, ancestor_name, depth) "
            "VALUES (%s, %s, %s, 0)",
            [(cv, "git_commit", "git_commit"), (cv, "jira_issue", "jira_issue")],
        )
        cur.execute(
            "INSERT INTO okg.narrowings "
            "(parent_subtype, edge_type, child_subtype, catalog_version_id) "
            "VALUES ('git_commit', 'references', 'jira_issue', %s)",
            (cv,),
        )
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(CITING, "git_commit",
                     {"text": "fix CMSTRANSF-452, see CMSTRANSF-1"},
                     {"id": CITING}, {"v": 1}),
            NodeFact(OTHER, "git_commit", {"text": "see CMSTRANSF-1"},
                     {"id": OTHER}, {"v": 1}),
            NodeFact(LOADED, "jira_issue",
                     {"issue_key": "CMSTRANSF-1", "title": "a loaded ticket"},
                     {"id": LOADED}, {"v": 1}),
        ],
        source=SOURCE,
        run_id=str(uuid4()),
    )
    add_alias(conn, alias="CMSTRANSF-1", canonical=LOADED, scope="cms_jira_key")
    for producer_id, kind, kinds, signature, clauses in (
        (SOURCE, "source", (), {}, ()),
        (
            "_extractor:cms_jira_key", "extractor", ("edge",),
            {"edges": [{"src_subtype": "git_commit", "edge_type": "references",
                        "dst_subtype": "jira_issue", "clause_id": "allow_refs"}]},
            ({"id": "allow_refs", "effect": "allow", "record_kind": "edge",
              "triple": ["git_commit", "references", "jira_issue"]},),
        ),
    ):
        materialize_producer_profile(
            conn,
            deployment_name=deployment,
            producer_id=producer_id,
            producer_kind=kind,
            trust_label=(
                "repo-local-trusted" if kind == "source"
                else "derived-deterministic-trusted"
            ),
            admission_mode="fast_track",
            authority_scope={"source_family": producer_id},
            allowed_record_kinds=kinds,
            output_signature=signature,
            policy_clauses=clauses,
            output_scope_summary={"summary": "link endpoint fixture"},
            source_ref={"fixture": __name__},
            catalog_version_id=cv if kind != "source" else None,
        )


def publisher(dsn: str, *, on_miss: bool = True, deployment: str = DEPLOYMENT) -> Publisher:
    return Publisher(
        dsn,
        deployment,
        extractor=RegexEntityExtractor([("cms_jira_key", r"\b([A-Z]+-\d+)\b")]),
        extractor_source_subtypes=["git_commit"],
        routes=[LinkerRoute(
            entity_type="cms_jira_key",
            src_subtype=None,
            edge_type="references",
            dst_template="{resolved}",
            on_miss=SPEC if on_miss else None,
        )],
        resolver_factory=ExactResolver,
        catalog_identity=bind_test_catalog_writer(dsn, deployment),
    )


def node(conn: psycopg.Connection, node_id: str) -> Optional[dict[str, Any]]:
    rows = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout(
        "default").query(
        "SELECT node_id, subtype, attrs, source FROM okg.graph_nodes "
        "WHERE node_id = :id", {"id": node_id})
    return dict(rows[0]) if rows else None


def edges(conn: psycopg.Connection, src: str) -> dict[str, int]:
    rows = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout(
        "default").query(
        "SELECT edge_id, dst FROM okg.graph_edges "
        "WHERE src = :src AND edge_type = 'references'", {"src": src})
    return {str(row["dst"]): int(row["edge_id"]) for row in rows}


def endpoint_declarations(conn: psycopg.Connection) -> list[str]:
    rows = ChronosGraphStore.from_connection(conn, ensure_schema=False).checkout(
        "default").query(
        "SELECT node_id FROM okg.graph_node_declarers WHERE source = :src "
        "ORDER BY node_id", {"src": LINK_ENDPOINT_SOURCE})
    return [str(row["node_id"]) for row in rows]


def retry_rows(conn: psycopg.Connection) -> list[tuple]:
    return conn.execute(
        "SELECT node_id, entity_type, entity_value, ready_at IS NOT NULL "
        "FROM okg.link_retry ORDER BY 1, 3"
    ).fetchall()


def test_a_missing_target_keeps_its_link_through_an_endpoint(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed(conn)
    assert publisher(clean_runtime).cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        endpoint = node(conn, MISSING)
        assert endpoint is not None
        assert endpoint["subtype"] == "jira_issue"
        assert endpoint["source"] == LINK_ENDPOINT_SOURCE
        # The store stamps every row's lineage; an endpoint has none
        # (no source record), which is why access rules never read it.
        lineage = endpoint["attrs"]["nomos_source_lineage"]
        assert (lineage["status"], lineage["source_ids"]) == ("missing", [])
        assert {
            k: v for k, v in endpoint["attrs"].items()
            if not k.startswith("_") and k != "nomos_source_lineage"
        } == {
            "issue_key": "CMSTRANSF-452", "project": "CMSTRANSF",
            "load_state": "not_loaded", "label": "CMSTRANSF-452",
            "text": ENDPOINT_TEXT,
        }
        assert set(edges(conn, CITING)) == {LOADED, MISSING}
        assert retry_rows(conn) == [(CITING, "cms_jira_key", "CMSTRANSF-452", False)]
        # The loaded record is untouched.
        assert "load_state" not in node(conn, LOADED)["attrs"]


def test_without_on_miss_the_missing_link_is_dropped_as_before(clean_runtime: str) -> None:
    """Control: the same publish with the route's on_miss off."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed(conn)
    assert publisher(clean_runtime, on_miss=False).cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert node(conn, MISSING) is None
        assert set(edges(conn, CITING)) == {LOADED}
        assert retry_rows(conn) == [(CITING, "cms_jira_key", "CMSTRANSF-452", False)]


def test_an_endpoint_registers_no_alias_and_wakes_nothing(clean_runtime: str) -> None:
    """The landing hook wakes retry rows by alias value alone. Failure case:
    an endpoint alias would mark its own row ready on every publish, and no
    publish with no input change could be reused."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed(conn)
    pub = publisher(clean_runtime)
    assert pub.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM okg.aliases WHERE alias = 'CMSTRANSF-452' "
            "OR canonical = %s", (MISSING,)).fetchone()[0] == 0
    quiet = pub.cycle()
    assert quiet.get("reused_generation") is True, quiet
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert retry_rows(conn) == [(CITING, "cms_jira_key", "CMSTRANSF-452", False)]


def _aliases_for(conn: psycopg.Connection, value: str) -> list[str]:
    """The distinct canonicals `value` resolves to (a scoped and an unscoped
    row for the same record count once)."""
    return [str(row[0]) for row in conn.execute(
        "SELECT DISTINCT canonical FROM okg.aliases WHERE alias = %s ORDER BY 1",
        (value,),
    ).fetchall()]


def test_catalog_alias_seeding_skips_endpoints_and_still_seeds_the_real_record(
    clean_runtime: str,
) -> None:
    """Review H1. With the CMS declarations (issue_key an alias field and a
    searchable field), every catalog apply seeds aliases from the graph's
    jira_issue rows. An endpoint must not get one: it would wake its own
    retry row, resolve its own miss and outlive its retraction. Failure
    case: before the fix, seeding wrote CMSTRANSF-452 -> the endpoint."""
    from okg.substrate.catalog.aliases import seed_all_alias_fields
    from okg.substrate.catalog.modules_apply import _seed_alias_fields_for_catalog

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed(conn, jira_metadata=JIRA_METADATA)
    pub = publisher(clean_runtime)
    assert pub.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        endpoint = node(conn, MISSING)
        assert endpoint["attrs"]["load_state"] == "not_loaded"
        # The searchable text keeps the key and says what the row is.
        assert endpoint["attrs"]["text"].endswith(ENDPOINT_TEXT)
        cv = int(conn.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            "WHERE status = 'applied'").fetchone()[0])
        # Both seeding paths a catalog apply runs.
        seed_all_alias_fields(conn)
        _seed_alias_fields_for_catalog(conn, cv)
        assert _aliases_for(conn, "CMSTRANSF-452") == []
        assert _aliases_for(conn, "CMSTRANSF-1") == [LOADED]
        assert retry_rows(conn) == [(CITING, "cms_jira_key", "CMSTRANSF-452", False)]
    pub.cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _aliases_for(conn, "CMSTRANSF-452") == []
        assert retry_rows(conn) == [(CITING, "cms_jira_key", "CMSTRANSF-452", False)]
        assert node(conn, MISSING)["attrs"]["load_state"] == "not_loaded"
        # The real record lands: seeding now registers its key.
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [NodeFact(MISSING, "jira_issue",
                      {"issue_key": "CMSTRANSF-452", "title": "the real ticket"},
                      {"id": MISSING}, {"v": 1})],
            source=SOURCE,
            run_id=str(uuid4()),
        )
        seed_all_alias_fields(conn)
        assert _aliases_for(conn, "CMSTRANSF-452") == [MISSING]
        assert retry_rows(conn) == [(CITING, "cms_jira_key", "CMSTRANSF-452", True)]
    assert pub.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert "load_state" not in node(conn, MISSING)["attrs"]
        assert endpoint_declarations(conn) == []
        assert retry_rows(conn) == []


def test_when_the_record_loads_the_edge_stays_and_the_endpoint_goes(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed(conn)
    pub = publisher(clean_runtime)
    assert pub.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        edge_id = edges(conn, CITING)[MISSING]
        assert endpoint_declarations(conn) == [MISSING]
        # The real record lands, then its alias.
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [NodeFact(MISSING, "jira_issue",
                      {"issue_key": "CMSTRANSF-452", "title": "the real ticket"},
                      {"id": MISSING}, {"v": 1})],
            source=SOURCE,
            run_id=str(uuid4()),
        )
        add_alias(conn, alias="CMSTRANSF-452", canonical=MISSING, scope="cms_jira_key")
        assert retry_rows(conn) == [(CITING, "cms_jira_key", "CMSTRANSF-452", True)]
    assert pub.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert edges(conn, CITING)[MISSING] == edge_id
        loaded = node(conn, MISSING)
        assert loaded["source"] == SOURCE
        assert "load_state" not in loaded["attrs"]
        assert loaded["attrs"]["title"] == "the real ticket"
        assert retry_rows(conn) == []
        assert endpoint_declarations(conn) == []
    quiet = pub.cycle()
    assert quiet.get("reused_generation") is True, quiet


def test_an_endpoint_no_link_reaches_is_retracted(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed(conn)
    pub = publisher(clean_runtime)
    assert pub.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert node(conn, MISSING) is not None
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        # The citing record goes; another text record changes, so the
        # extractor runs.
        store.apply_facts(
            [NodeFact(CITING, "git_commit", {}, {"id": CITING}, {"v": 2}, op="R"),
             NodeFact(OTHER, "git_commit", {"text": "see CMSTRANSF-1 again"},
                      {"id": OTHER}, {"v": 2})],
            source=SOURCE,
            run_id=str(uuid4()),
        )
    assert pub.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert node(conn, CITING) is None
        assert node(conn, MISSING) is None
        assert endpoint_declarations(conn) == []
        assert node(conn, LOADED) is not None


def test_the_endpoint_goes_in_the_publish_that_removes_its_last_citation(
    clean_runtime: str,
) -> None:
    """Only the citing record is removed: no text changes, so the extractor
    does not run. The endpoint sweep runs on every publish anyway, so the
    endpoint goes in this publish, not a later one."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed(conn)
    pub = publisher(clean_runtime)
    assert pub.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [NodeFact(CITING, "git_commit", {}, {"id": CITING}, {"v": 2}, op="R")],
            source=SOURCE,
            run_id=str(uuid4()),
        )
    result = pub.cycle()
    assert result["status"] == "published"
    assert "link_endpoint_sweep" in (result.get("timings_ms") or {}), result
    assert "extractor" not in (result.get("timings_ms") or {}), result
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert node(conn, MISSING) is None
        assert endpoint_declarations(conn) == []
        assert node(conn, LOADED) is not None


def test_a_still_cited_endpoint_survives_a_publish_that_extracts_nothing(
    clean_runtime: str,
) -> None:
    """Failure case for the sweep: a publish with an unrelated change keeps
    an endpoint a live record still cites."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed(conn)
    pub = publisher(clean_runtime)
    assert pub.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [NodeFact("jira:CMSTRANSF-2", "jira_issue", {"issue_key": "CMSTRANSF-2"},
                      {"id": "jira:CMSTRANSF-2"}, {"v": 1})],
            source=SOURCE,
            run_id=str(uuid4()),
        )
    assert pub.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert node(conn, MISSING)["attrs"]["load_state"] == "not_loaded"
        assert endpoint_declarations(conn) == [MISSING]
        assert MISSING in edges(conn, CITING)


def test_an_on_miss_subtype_the_catalog_lacks_refuses_the_publish(
    clean_runtime: str,
) -> None:
    from okg.substrate.errors import SubstrateError

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed(conn)
    pub = publisher(clean_runtime)
    route = pub._extractor_stage.projector._router._table["cms_jira_key"][None]
    pub._extractor_stage.projector._router._table["cms_jira_key"][None] = LinkerRoute(
        entity_type=route.entity_type, src_subtype=None, edge_type=route.edge_type,
        dst_template=route.dst_template,
        on_miss=LinkEndpointSpec("ticket:{value}", "ticket", "key"),
    )
    with pytest.raises(SubstrateError, match="ticket"):
        pub.cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert node(conn, "ticket:CMSTRANSF-452") is None


def test_the_backfill_queues_only_records_with_waiting_misses(clean_runtime: str) -> None:
    """Switching on_miss on re-extracts nothing by itself; the bounded
    backfill pages the records holding unready retry rows into the queue,
    and the next publish writes their endpoints."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed(conn)
    assert publisher(clean_runtime, on_miss=False).cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        dry = backfill_link_endpoints(
            conn, entity_types=["cms_jira_key"], deployment_name=DEPLOYMENT, limit=10)
        assert (dry.applied, dry.details["node_ids"]) == (False, [CITING])
        with pytest.raises(ValueError):
            backfill_link_endpoints(
                conn, entity_types=["cms_jira_key"], deployment_name=DEPLOYMENT, limit=0)
        with pytest.raises(ValueError, match="deployment"):
            backfill_link_endpoints(
                conn, entity_types=["cms_jira_key"], deployment_name="", limit=10)
        applied = backfill_link_endpoints(
            conn, entity_types=["cms_jira_key"], deployment_name=DEPLOYMENT,
            limit=10, apply=True)
        assert applied.details["queued_count"] == 1
    result = publisher(clean_runtime).cycle()
    assert result["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert node(conn, MISSING)["attrs"]["load_state"] == "not_loaded"
        assert set(edges(conn, CITING)) == {LOADED, MISSING}
        assert conn.execute(
            "SELECT count(*) FROM okg.extractor_retry_queue").fetchone()[0] == 0
