"""The console Home reads against a real published graph.

PACT operator-console-live-home. Three reads back Home's chart, its contents
panel and its attempts list:

* ``console/attempts`` buckets the real checkpoint history by UTC day,
* ``console/ontology-rollup`` reports the record and relationship types from
  the maintained count rollup, authorized at the operator UI's Nomos boundary,
* ``console/generations`` now carries the full generation id, the created_at
  stamp and the total the console's parser requires.

The day arithmetic and the range vocabulary are proven without a database in
tests/substrate/console_views/test_attempts_by_day.py; what needs real
Postgres is that the columns those rules read exist and hold what the composer
claims, that the rollup's sums equal the generation's own counts, and that a
policy denial refuses the rollup rather than answering it.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Iterator

import psycopg
import pytest

pytest.importorskip("fastapi")
from fastapi import APIRouter, FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from okg.substrate.console_views.attempts_by_day import (  # noqa: E402
    AttemptsRangeInvalid,
    compose_attempts_by_day,
)
from okg.substrate.console_views.generations import compose_generations  # noqa: E402
from okg.substrate.console_views.ontology_rollup_console import (  # noqa: E402
    compose_ontology_rollup,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.mcp.session import open_session  # noqa: E402
from okg.substrate.nomos import (  # noqa: E402
    activate_policy_bundle,
    compile_policy_graph,
)
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.viz.record_endpoints import register_operator_record_routes  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate._db_helpers import seed_catalog_version  # noqa: E402

pytestmark = pytest.mark.postgres

DEPLOYMENT = "default"
SUBJECT = "subject:operator"


# --------------------------------------------------------------------------- #
# publishing                                                                   #
# --------------------------------------------------------------------------- #
def _node(node_id: str, subtype: str, **attrs: Any) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs=attrs,
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


def _publish(dsn: str, nodes: list[NodeFact], edges: list[EdgeFact] | None = None) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([*nodes, *(edges or [])], source="fixture")
    result = Publisher(
        dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    ).cycle()
    assert result["status"] == "published"


@pytest.fixture
def published(clean_runtime: str) -> str:
    """Two published generations: four records and two relationships, then one
    more record, so deltas, totals and type counts are all non-trivial."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    _publish(
        clean_runtime,
        [
            _node("page:alpha", "page", title="Alpha", text="alpha"),
            _node("page:beta", "page", title="Beta", text="beta"),
            _node("release:r1", "release", name="R1", text="r1"),
            _node("release:r2", "release", name="R2", text="r2"),
        ],
        [
            EdgeFact(src="page:alpha", dst="release:r1", edge_type="mentions"),
            EdgeFact(src="page:beta", dst="release:r2", edge_type="mentions"),
        ],
    )
    _publish(clean_runtime, [_node("release:r3", "release", name="R3", text="r3")])
    return clean_runtime


@contextmanager
def _session(dsn: str) -> Iterator[tuple[psycopg.Connection, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        yield conn, open_session(conn, deployment_name=DEPLOYMENT)


# --------------------------------------------------------------------------- #
# attempts                                                                     #
# --------------------------------------------------------------------------- #
def test_attempts_counts_the_real_checkpoints_into_their_days(published: str) -> None:
    with _session(published) as (conn, session):
        payload = compose_attempts_by_day(
            conn,
            deployment_name=DEPLOYMENT,
            session=session,
            requested_range="7d",
        )
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        history = store.list_generations(
            deployment_name=DEPLOYMENT, branch=session.branch_name, status=None,
        )

    # A 7-day span reaches back seven whole days from the read, so it holds
    # eight UTC days: today and the seven before it (``_days`` in
    # attempts_by_day). That is the boundary the attempt rows' "0 d".."7 d"
    # age labels use, so the chart and the list agree on what is in the span.
    assert len(payload["buckets"]) == 8
    # Both publishes happened during this test run, so they are on today's bar
    # and the totals account for every checkpoint the store holds.
    counted = sum(payload["totals"].values())
    assert counted + payload["other_status_count"] + payload["undated_count"] == len(history)
    assert payload["totals"]["published"] == 2
    assert payload["buckets"][-1]["published"] == 2
    assert payload["buckets"][-1]["day"] == datetime.now(timezone.utc).date().isoformat()


def test_attempts_answers_a_deployment_that_has_published_nothing(
    clean_runtime: str,
) -> None:
    """An empty deployment is a state, not a failure: the days come back with
    zeros so the chart can say "nothing ran" instead of breaking."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        session = SimpleNamespace(branch_name="default", generation_id=None)
        payload = compose_attempts_by_day(
            conn,
            deployment_name=DEPLOYMENT,
            session=session,
            requested_range="14d",
        )

    # Fourteen days back from the read plus the read's own day: 15 UTC days.
    assert len(payload["buckets"]) == 15
    assert payload["totals"] == {"published": 0, "blocked": 0, "running": 0}


def test_attempts_refuses_a_range_the_console_does_not_offer(published: str) -> None:
    with _session(published) as (conn, session):
        with pytest.raises(AttemptsRangeInvalid):
            compose_attempts_by_day(
                conn,
                deployment_name=DEPLOYMENT,
                session=session,
                requested_range="1y",
            )


def test_attempts_reads_the_created_at_column_the_checkpoints_hold(
    published: str,
) -> None:
    """Guards the composer's claim about WHERE the day comes from: if the
    column stopped being a parseable stamp, every attempt would land in
    undated_count instead of on a bar."""
    with _session(published) as (conn, session):
        payload = compose_attempts_by_day(
            conn,
            deployment_name=DEPLOYMENT,
            session=session,
            requested_range="7d",
        )
    assert payload["undated_count"] == 0
    assert payload["other_status_count"] == 0


# --------------------------------------------------------------------------- #
# ontology rollup                                                              #
# --------------------------------------------------------------------------- #
def _live_counts(conn: psycopg.Connection, session: Any) -> tuple[int, int]:
    from okg.substrate.console_views._chronos import generation_counts

    return generation_counts(conn, session)


def test_the_rollup_sums_match_the_generations_own_counts(published: str) -> None:
    """The number that must agree: Home prints these type counts beside the
    live record and relationship totals from health. If they disagree the page
    contradicts itself."""
    with _session(published) as (conn, session):
        payload = compose_ontology_rollup(
            conn,
            deployment_name=DEPLOYMENT,
            generation_id=session.generation_id,
        )
        nodes_live, edges_live = _live_counts(conn, session)

    assert sum(row["count"] for row in payload["node_types"]) == nodes_live
    assert sum(row["count"] for row in payload["relationship_types"]) == edges_live
    assert {row["type"] for row in payload["node_types"]} == {"page", "release"}
    assert [row["type"] for row in payload["relationship_types"]] == ["mentions"]
    assert payload["node_type_total"] == 2
    assert payload["relationship_type_total"] == 1
    # The shared enum, not a sentence: `test_the_rollup_states_its_basis_as_
    # the_shared_enum` below pins the exact value.
    assert payload["count_basis"] == "published_total_before_access_rules"


def test_the_rollup_orders_the_biggest_type_first(published: str) -> None:
    with _session(published) as (conn, session):
        payload = compose_ontology_rollup(
            conn,
            deployment_name=DEPLOYMENT,
            generation_id=session.generation_id,
        )
    counts = [row["count"] for row in payload["node_types"]]
    assert counts == sorted(counts, reverse=True)
    assert payload["node_types"][0]["type"] == "release"


def test_an_unmaintained_generation_fails_loud_instead_of_reporting_nothing(
    published: str,
) -> None:
    """A graph whose rollup was never maintained must not look like a graph
    with nothing in it."""
    from okg.substrate.console_views.console_count_rollup_read import (
        ConsoleCountRollupUnavailable,
    )

    with _session(published) as (conn, session):
        unmaintained = session.generation_id.rsplit(":", 1)[0] + ":000000000000"
        with pytest.raises(ConsoleCountRollupUnavailable):
            compose_ontology_rollup(
                conn,
                deployment_name=DEPLOYMENT,
                generation_id=unmaintained,
            )


# --------------------------------------------------------------------------- #
# the rollup route's authorization                                             #
# --------------------------------------------------------------------------- #
class _Pool:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def connection(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn)


class _Ctx:
    """The slice of VizContext the record routes read, pinned to a version."""

    def __init__(self, dsn: str, *, generation_id: str) -> None:
        self.dsn = dsn
        self.deployment_name = DEPLOYMENT
        self.session = SimpleNamespace(
            branch_name="default", generation_id=generation_id,
        )
        self.pool = _Pool(dsn)

    @contextmanager
    def conn(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            yield conn


def _client(dsn: str, *, generation_id: str) -> TestClient:
    app = FastAPI()
    router = APIRouter(prefix="/viz/api")
    register_operator_record_routes(router, ctx_provider=lambda: _Ctx(dsn, generation_id=generation_id))
    app.include_router(router)
    return TestClient(app)


def _rule(rule_id: str, *, match: dict[str, Any], effect: str = "allow") -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "effect": effect,
        "actions": ["graph.read.local_operator"],
        "subjects": [SUBJECT],
        "match": match,
        "priority": 100,
        "reason_code": f"nomos.test.{rule_id}",
    }


def _activate(dsn: str, rules: list[dict[str, Any]]) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:console-home-live-reads",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules,
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=DEPLOYMENT,
            bundle=bundle,
            scope=f"deployment:{DEPLOYMENT}",
            activated_by="test:operator",
        )
        conn.commit()


def _enforce(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)


def _generation(dsn: str) -> str:
    with _session(dsn) as (_conn, session):
        return str(session.generation_id)


def test_the_rollup_route_is_authorized_at_the_operator_boundary(
    published: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_id = _generation(published)
    _activate(published, [
        _rule("allow_ui_boundary", match={"resource_kind": "local_operator_boundary"}),
        _rule("allow_ui_rows", match={}),
    ])
    _enforce(monkeypatch)

    response = _client(published, generation_id=generation_id).get(
        "/viz/api/console/ontology-rollup",
    )

    assert response.status_code == 200
    body = response.json()
    assert body["generation_id"] == generation_id
    assert body["payload"]["node_type_total"] == 2

    with psycopg.connect(published) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT action, effect, target->>'read'
              FROM okg.nomos_decisions
             WHERE deployment_name = %s AND action LIKE 'graph.read%%'
             ORDER BY created_at, decision_row_id
            """,
            (DEPLOYMENT,),
        )
        decisions = [(str(a), str(e), str(read)) for a, e, read in cur.fetchall()]
    assert ("graph.read.local_operator", "allow", "ontology_rollup") in decisions


def test_a_policy_that_denies_the_boundary_refuses_the_rollup(
    published: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The failure this guards: a read that answers anyway, with counts of
    records the reader is not allowed to know exist."""
    generation_id = _generation(published)
    _activate(published, [_rule("rows_only", match={"resource_kind": "graph_object"})])
    _enforce(monkeypatch)

    response = _client(published, generation_id=generation_id).get(
        "/viz/api/console/ontology-rollup",
    )

    assert response.status_code == 403
    assert response.json()["detail"]["reason"] == "policy_denied"


# --------------------------------------------------------------------------- #
# generations                                                                  #
# --------------------------------------------------------------------------- #
def test_generations_carries_the_fields_the_console_parses(published: str) -> None:
    with _session(published) as (conn, session):
        payload = compose_generations(
            conn, deployment_name=DEPLOYMENT, session=session,
        )
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        history = store.list_generations(
            deployment_name=DEPLOYMENT, branch=session.branch_name, status=None,
        )

    assert payload["total"] == len(history)
    assert payload["total"] >= payload["shown"]
    newest = payload["generations"][0]
    assert newest["full_generation_id"] == session.generation_id
    # A stamp a client can order and subtract, not a humanised phrase.
    assert datetime.fromisoformat(newest["created_at"]).tzinfo is not None
    assert newest["age"].endswith("ago") or newest["age"] == "just now"


def test_generations_says_not_recorded_rather_than_zero(published: str) -> None:
    """No checkpoint column holds a publish's wall clock, the stage it stopped
    in, the producer it stopped on, or its error text. Null is the honest
    answer; a zero duration would draw a measured-looking bar."""
    with _session(published) as (conn, session):
        payload = compose_generations(
            conn, deployment_name=DEPLOYMENT, session=session,
        )
    for row in payload["generations"]:
        assert row["duration_ms"] is None
        assert row["stopped_stage"] is None
        assert row["stopped_producer"] is None
        assert row["error_text"] is None


def test_generations_keeps_every_field_its_existing_readers_use(
    published: str,
) -> None:
    """The shipped Home and Health read this route. A renamed or retyped field
    breaks them silently, so the old shape is asserted here by name."""
    with _session(published) as (conn, session):
        payload = compose_generations(
            conn, deployment_name=DEPLOYMENT, session=session,
        )

    assert {"deployment", "current_generation_id", "shown", "generations"} <= set(payload)
    row = payload["generations"][0]
    for field in (
        "generation_id", "status", "age", "is_current", "blocked_reason",
        "excluded_source_count", "delta_available", "delta_unavailable_reason",
        "nodes_added", "nodes_retracted", "edges_added", "edges_retracted",
        "zero_delta",
    ):
        assert field in row, field
    assert isinstance(row["is_current"], bool)
    assert isinstance(row["nodes_added"], int)


def test_generations_created_at_is_utc_whatever_the_column_held(
    published: str,
) -> None:
    """The requirement says ISO-8601 UTC. A naive stamp is read as UTC and a
    stamp in another zone is converted, so the field means one thing however
    the row was written.

    The stamps are written into the checkpoint table's own ``created_at``
    column (it is text), not patched onto a reader: the composer reads a
    bounded, projected window through ``recent_generations``, and a test that
    rewrote ``list_generations`` rows would never reach what it serves. The
    older generation gets the naive stamp and the newer one the -08:00 stamp,
    which keeps the column's lexical order -- the order the reader sorts by --
    the same as the publish order.
    """
    with psycopg.connect(published, autocommit=True) as conn:
        ids = [
            row[0]
            for row in conn.execute(
                """
                SELECT checkpoint_id
                  FROM _chronos_branch_interval_checkpoints
                 WHERE metadata::json ->> 'kind' = 'okg_generation'
                 ORDER BY created_at, checkpoint_id
                """
            ).fetchall()
        ]
        assert len(ids) == 2, ids
        for checkpoint_id, stamp in zip(
            ids, ["2026-09-20T06:00:00", "2026-09-20T20:00:00-08:00"],
        ):
            conn.execute(
                "UPDATE _chronos_branch_interval_checkpoints"
                " SET created_at = %s WHERE checkpoint_id = %s",
                (stamp, checkpoint_id),
            )

    with _session(published) as (conn, session):
        payload = compose_generations(
            conn, deployment_name=DEPLOYMENT, session=session,
        )

    written = [row["created_at"] for row in payload["generations"]]
    # Newest first: the -08:00 stamp converted, then the naive one read as UTC.
    assert written == ["2026-09-21T04:00:00+00:00", "2026-09-20T06:00:00+00:00"]
    for value in written:
        assert datetime.fromisoformat(value).utcoffset() == timedelta(0)


def test_the_rollup_states_its_basis_as_the_shared_enum(published: str) -> None:
    """One vocabulary with the record reads, not a sentence: the console maps
    known values to words and refuses an unknown one by name."""
    from okg.substrate.console_views.records import compose_published_versions  # noqa: F401

    with _session(published) as (conn, session):
        payload = compose_ontology_rollup(
            conn,
            deployment_name=DEPLOYMENT,
            generation_id=session.generation_id,
        )
    assert payload["count_basis"] == "published_total_before_access_rules"
