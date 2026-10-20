"""A legacy-shaped database-backed enricher through a real publish (#1793).

The external Archi distribution's enrichers are live database classes: they
read the graph with ``_chronos.query`` on the publisher's own connection,
resume from the ``okg.edge_facts`` cursor, anti-join through
``mint_edge_id`` and write with ``insert_deterministic_edges``. Sprint 14
must keep that working until an enricher SDK read API exists, so this file
drives a neutral fixture enricher with exactly that access pattern (a mirror
of ``archi.enrichment.jira_affects.JiraAffectsFromReferences`` over neutral
subtypes) through three ``Publisher.cycle()`` runs in an owned cluster and
asserts the semantics it relies on:

1. first run: the enricher runs on the publisher's live connection, which the
   Chronos-native cycle keeps in autocommit (each enricher statement is its
   own durable transaction; the savepoint branch of ``_run_enrichers`` only
   engages for a caller that passes its own transaction-mode connection to
   ``Publisher.cycle(conn=...)``, and the witness restore in
   ``_chronos.query`` for any in-transaction caller such as
   ``okg enricher backfill``, which opens ``autocommit=False`` and calls
   ``enrich()`` directly -- proven separately below), its derived edges are
   ingested into the published generation, and a sibling that raises after
   its own read neither blocks the publish nor removes the good enricher's
   edges;
2. clean rerun: the enricher is skipped and every derived edge is still live
   at the generation the cycle reports;
3. incremental rerun after one new source node and edge: ``is_first_run`` is
   False, the scope is the new node, exactly one edge is added and the
   earlier ones are untouched.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg
import pytest

from okg.substrate.consistency.transform_gate import TransformConsistencyGate
from okg.substrate.enrichers.base import EnrichResult, IncrementalContext
from okg.substrate.enrichers.derived_edges import (
    DerivedEdgeCandidate,
    insert_deterministic_edges,
    mint_edge_id,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.linkers import _chronos
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.transform.programs import enrichment_program
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer, seed_catalog

pytestmark = pytest.mark.postgres

_MAX_INCREMENTAL_SCOPE = 10_000
_WITNESS_GUC = "okg.source_program_checksum"


def _witness(conn: psycopg.Connection) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_setting(%s, true)", (_WITNESS_GUC,))
        return str(cur.fetchone()[0] or "")


class ReferenceImpactRollup:
    """``JiraAffectsFromReferences`` over neutral subtypes.

    ticket --references--> site|service  becomes  ticket --affects--> target.
    Every substrate call, the incremental scope rule, the anti-join and the
    dedupe-key recipe follow the Archi enricher line for line; only the
    subtype names, the id prefix and the observation log differ.
    """

    name = "fixture_reference_impact_rollup"
    publisher_skip_when_clean = True
    publisher_skip_when_catalog_changed_but_inputs_clean = True
    reads_edge_types = ("references",)
    reads_subtypes = ("ticket", "site", "service")
    emits_edge_types = ("affects",)
    target_subtypes = ("site", "service")
    requires_narrowings = tuple(
        ("ticket", "affects", target) for target in target_subtypes
    )

    def __init__(self) -> None:
        self.observations: list[dict[str, Any]] = []

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        incremental: IncrementalContext | None = None,
    ) -> EnrichResult:
        run_id = str(uuid.uuid4())
        observed_at = datetime.now(timezone.utc)
        observation: dict[str, Any] = {
            "generation_id": generation_id,
            "autocommit": conn.autocommit,
            "transaction_status": conn.info.transaction_status.name,
            "is_first_run": None if incremental is None else incremental.is_first_run,
            "dirty_node_ids": sorted(incremental.dirty_node_ids) if incremental else None,
            "change_basis": None if incremental is None else incremental.change_basis,
            "witness_before": _witness(conn),
        }
        self.observations.append(observation)
        scope_ids = _incremental_scope_node_ids(
            conn, incremental, edge_types=("references",),
        )
        observation["scope_ids"] = scope_ids
        if scope_ids == []:
            observation["candidate_rows"] = 0
            return EnrichResult(self.name, n_edges_emitted=0)
        scope_filter = ""
        params: dict[str, object] = {"target_subtypes": list(self.target_subtypes)}
        if scope_ids is not None:
            scope_filter = "AND (ref.src = ANY(:scope_ids) OR ref.dst = ANY(:scope_ids))"
            params["scope_ids"] = scope_ids

        with conn.cursor() as cur:
            candidates = _chronos.query(
                cur,
                f"""
                SELECT ref.src AS ticket_id,
                       ref.dst AS target_id,
                       target.subtype AS target_subtype,
                       count(*) AS reference_count
                  FROM okg.graph_edges ref
                  JOIN okg.graph_nodes ticket
                    ON ticket.node_id = ref.src
                   AND ticket.subtype = 'ticket'
                  JOIN okg.graph_nodes target
                    ON target.node_id = ref.dst
                   AND target.subtype = ANY(:target_subtypes)
                 WHERE ref.edge_type = 'references'
                   {scope_filter}
                 GROUP BY ref.src, ref.dst, target.subtype
                """,
                params,
            )
            observation["witness_after"] = _witness(conn)
            observation["candidate_rows"] = len(candidates)
            existing = _existing_live_pairs(
                cur,
                edge_type="affects",
                pairs=[(str(r["ticket_id"]), str(r["target_id"])) for r in candidates],
            )
            observation["existing_pairs"] = len(existing)
            candidates = [
                row for row in candidates
                if (str(row["ticket_id"]), str(row["target_id"])) not in existing
            ]
            if not candidates:
                return EnrichResult(self.name, n_edges_emitted=0)

            derived = []
            for row in candidates:
                src = str(row["ticket_id"])
                dst = str(row["target_id"])
                dedupe_key = uuid.uuid5(
                    uuid.NAMESPACE_URL, f"{self.name}|{src}|affects|{dst}",
                ).hex
                derived.append(
                    DerivedEdgeCandidate(
                        src=src,
                        edge_type="affects",
                        dst=dst,
                        attrs={
                            "match_type": "reference_impact",
                            "derived_from": "ticket.references",
                            "reference_count": int(row["reference_count"]),
                            "target_subtype": str(row["target_subtype"]),
                        },
                        source_record_id={
                            "src_node_id": src, "dst_node_id": dst, "enricher": self.name,
                        },
                        source_revision={"generation_id": generation_id},
                        dedupe_key=dedupe_key,
                    )
                )
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=derived,
                observed_at=observed_at,
                run_id=run_id,
            )
        observation["inserted"] = result.inserted
        observation["skipped"] = result.skipped
        return EnrichResult(
            self.name, n_edges_emitted=result.inserted, n_edges_skipped=result.skipped,
        )


def _incremental_scope_node_ids(
    conn: psycopg.Connection,
    incremental: IncrementalContext | None,
    *,
    edge_types: tuple[str, ...],
) -> list[str] | None:
    if incremental is None or incremental.is_first_run:
        return None
    ids = {n for n in incremental.dirty_node_ids if n.startswith("ticket:")}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT src FROM okg.edge_facts WHERE fact_id > %s AND edge_type = ANY(%s)",
            (incremental.prev_edge_watermark, list(edge_types)),
        )
        for (src,) in cur.fetchall():
            ids.add(src)
    if not ids:
        return []
    if len(ids) > _MAX_INCREMENTAL_SCOPE:
        return None
    return sorted(ids)


def _existing_live_pairs(
    cur: psycopg.Cursor, *, edge_type: str, pairs: list[tuple[str, str]],
) -> set[tuple[str, str]]:
    if not pairs:
        return set()
    unique_pairs = sorted(set(pairs))
    pairs_by_edge_id = {
        mint_edge_id(src, edge_type, dst): (src, dst) for src, dst in unique_pairs
    }
    existing: set[tuple[str, str]] = set()
    edge_ids = list(pairs_by_edge_id)
    for start in range(0, len(edge_ids), 5_000):
        rows = _chronos.query(
            cur,
            "SELECT edge_id FROM okg.graph_edges "
            "WHERE edge_id = ANY(:edge_ids) AND edge_type = :edge_type",
            {"edge_ids": edge_ids[start:start + 5_000], "edge_type": edge_type},
        )
        existing.update(
            pairs_by_edge_id[int(row["edge_id"])]
            for row in rows if int(row["edge_id"]) in pairs_by_edge_id
        )
    return existing


class RaisingAfterRead:
    """A sibling that performs a Chronos read and then raises, once.

    Registered AFTER the rollup. On the autocommit publish path the rollup's
    edge facts are already durable when this raises, so their survival is
    the durability proof; on a transaction-mode ``cycle(conn=...)`` the
    publisher's savepoint would be what keeps them.
    """

    name = "fixture_raising_after_read"
    publisher_skip_when_clean = True
    reads_subtypes = ("site",)
    reads_edge_types = ()
    emits_edge_types = ("affects",)
    requires_narrowings = (("ticket", "affects", "site"),)

    def __init__(self) -> None:
        self.calls = 0
        self.rows_before_raise: int | None = None

    def enrich(self, conn, *, generation_id, incremental=None):
        self.calls += 1
        if self.calls > 1:
            return EnrichResult(self.name, n_edges_emitted=0)
        with conn.cursor() as cur:
            rows = _chronos.query(
                cur,
                "SELECT node_id FROM okg.graph_nodes WHERE subtype = :subtype",
                {"subtype": "site"},
            )
        self.rows_before_raise = len(rows)
        raise RuntimeError("fixture: raise after a Chronos read")


def _node(node_id: str, subtype: str, revision: int = 1) -> NodeFact:
    return NodeFact(node_id, subtype, {}, {"fixture": node_id}, {"revision": revision})


def _edge(src: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src, dst, "references",
        source_record_id={"fixture": f"{src}->{dst}"},
        source_revision={"revision": 1},
    )


def _affects_edges(dsn: str) -> dict[int, tuple[str, str]]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        rows = _chronos.query(
            cur,
            "SELECT edge_id, src, dst FROM okg.graph_edges WHERE edge_type = 'affects'",
        )
    return {int(r["edge_id"]): (str(r["src"]), str(r["dst"])) for r in rows}


def _fact_count(dsn: str, source: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.edge_facts WHERE source = %s", (source,))
        return int(cur.fetchone()[0])


def _progress(dsn: str, deployment: str, name: str) -> tuple[str, Any, Any]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT last_status, generation_id, edge_watermark FROM okg.enricher_progress "
            "WHERE deployment_name = %s AND enricher_name = %s",
            (deployment, name),
        )
        row = cur.fetchone()
    assert row is not None, f"no progress row for {name}"
    return str(row[0]), row[1], row[2]


def test_legacy_enricher_keeps_live_connection_and_generation_semantics(
    clean_runtime: str, record_property, caplog,
) -> None:
    deployment = "legacy-enricher-semantics"
    source = f"_enricher:{ReferenceImpactRollup.name}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "site", "service"],
            narrowings=[
                ("ticket", "references", "site"),
                ("ticket", "references", "service"),
                ("ticket", "affects", "site"),
                ("ticket", "affects", "service"),
            ],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                _node("ticket:1", "ticket"),
                _node("ticket:2", "ticket"),
                _node("site:a", "site"),
                _node("service:b", "service"),
                _edge("ticket:1", "site:a"),
                _edge("ticket:1", "service:b"),
                _edge("ticket:2", "site:a"),
            ],
            source="fixture_source",
        )

    rollup = ReferenceImpactRollup()
    raiser = RaisingAfterRead()
    publisher = Publisher(
        clean_runtime,
        deployment,
        enrichers=[rollup, raiser],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )

    # -- 1. first run ---------------------------------------------------------
    with caplog.at_level("WARNING", logger="okg.substrate.publisher.publisher"):
        first = publisher.cycle()
    assert first["status"] == "published", first
    # The first generation publishes every edge: 3 source + 3 derived.
    assert first["edges_added"] == 6, first
    obs = rollup.observations
    assert len(obs) == 1
    # The Chronos-native cycle hands enrichers its live connection in
    # autocommit: no publisher transaction, no savepoint. The enrichment
    # witness is pinned for the connection's session, so it is still set when
    # the enricher runs and survives its `_chronos.query` read (issue #2965;
    # a per-transaction pin was gone before the enricher's first statement).
    assert obs[0]["autocommit"] is True, obs[0]
    assert obs[0]["transaction_status"] == "IDLE"
    assert (
        obs[0]["witness_before"]
        == obs[0]["witness_after"]
        == enrichment_program().checksum()
    ), obs[0]
    # Chronos-native mode passes generation_id=0 to legacy enrichers; Archi's
    # enrichers stamp it into source_revision (recorded, see docs).
    assert obs[0]["generation_id"] == 0
    assert obs[0]["is_first_run"] is True
    assert obs[0]["change_basis"] == "chronos_segment"
    assert obs[0]["dirty_node_ids"] == ["service:b", "site:a", "ticket:1", "ticket:2"]
    assert obs[0]["scope_ids"] is None
    assert obs[0]["candidate_rows"] == 3
    assert obs[0]["inserted"] == 3 and obs[0]["skipped"] == 0
    # The sibling read the graph on the same live connection, then raised;
    # the publish went through and the rollup's edges, already durable,
    # survived the sibling's failure.
    assert raiser.calls == 1 and raiser.rows_before_raise == 1
    assert any(
        "fixture_raising_after_read" in r.getMessage() and "raised" in r.getMessage()
        for r in caplog.records
    ), [r.getMessage() for r in caplog.records]
    first_edges = _affects_edges(clean_runtime)
    assert sorted(first_edges.values()) == [
        ("ticket:1", "service:b"), ("ticket:1", "site:a"), ("ticket:2", "site:a"),
    ]
    assert _fact_count(clean_runtime, source) == 3
    rollup_status, _, _ = _progress(clean_runtime, deployment, rollup.name)
    raiser_status, _, _ = _progress(clean_runtime, deployment, raiser.name)
    assert rollup_status == "success"
    assert raiser_status == "failed:RuntimeError"

    # -- 2. clean rerun -------------------------------------------------------
    # The sibling's held retry invalidates the early no-op reuse (P6), so the
    # enricher stage runs: the sibling retries and succeeds, the rollup is
    # skipped on clean inputs, and with no graph writes the cycle reports the
    # previous generation again as a no-op reuse.
    second = publisher.cycle()
    assert second["status"] == "published", second
    assert second.get("noop") is True and second.get("reused_generation") is True, second
    assert second["generation_id"] == first["generation_id"]
    assert len(rollup.observations) == 1, "clean inputs must skip the rollup"
    assert raiser.calls == 2
    second_edges = _affects_edges(clean_runtime)
    assert second_edges == first_edges
    assert _fact_count(clean_runtime, source) == 3
    assert _progress(clean_runtime, deployment, rollup.name)[0] == "success"
    assert _progress(clean_runtime, deployment, raiser.name)[0] == "success"

    # -- 3. incremental rerun -------------------------------------------------
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_node("ticket:3", "ticket"), _edge("ticket:3", "site:a")],
            source="fixture_source",
        )
    third = publisher.cycle()
    assert third["status"] == "published", third
    assert not third.get("noop"), third
    assert third["generation_id"] != first["generation_id"]
    # One new source edge plus the one derived edge.
    assert third["edges_added"] == 2, third
    assert len(rollup.observations) == 2
    inc = rollup.observations[1]
    assert inc["is_first_run"] is False
    assert inc["change_basis"] == "chronos_segment"
    assert "ticket:3" in inc["dirty_node_ids"]
    assert inc["scope_ids"] == ["ticket:3"]
    assert inc["candidate_rows"] == 1
    assert inc["existing_pairs"] == 0
    assert inc["inserted"] == 1 and inc["skipped"] == 0
    assert inc["autocommit"] is True and inc["transaction_status"] == "IDLE"
    # Chronos-native source writes never reach okg.edge_facts, so the legacy
    # fact-log cursor contributed nothing to the scope; the dirty node set did.
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.edge_facts WHERE edge_type = 'references'")
        assert int(cur.fetchone()[0]) == 0
    third_edges = _affects_edges(clean_runtime)
    assert set(first_edges).issubset(third_edges)
    assert len(third_edges) == 4
    assert third_edges[max(set(third_edges) - set(first_edges))] == ("ticket:3", "site:a")
    assert _fact_count(clean_runtime, source) == 4

    record_property("first_generation_id", first["generation_id"])
    record_property("second_generation_id", second["generation_id"])
    record_property("third_generation_id", third["generation_id"])
    record_property("derived_edges_first_cycle", len(first_edges))
    record_property("derived_edges_third_cycle", len(third_edges))
    record_property("edge_facts_after_third_cycle", _fact_count(clean_runtime, source))
    record_property("enricher_connection_autocommit", obs[0]["autocommit"])
    record_property("raiser_rows_before_raise", raiser.rows_before_raise)


class FullScanReferenceRollup:
    """ticket --references--> site  becomes  ticket --affects--> site.

    Reads the whole graph each run and leaves the dedupe key to
    ``insert_deterministic_edges``, so the key carries the endpoints' lineage
    (their source revisions) -- the recipe issue #1730 is about.
    """

    name = "fixture_full_scan_reference_rollup"
    reads_edge_types = ("references",)
    reads_subtypes = ("ticket", "site")
    emits_edge_types = ("affects",)
    requires_narrowings = (("ticket", "affects", "site"),)

    def __init__(self) -> None:
        self.observations: list[dict[str, Any]] = []

    def enrich(self, conn, *, generation_id, incremental=None):
        with conn.cursor() as cur:
            rows = _chronos.query(
                cur,
                """
                SELECT ref.src, ref.dst
                  FROM okg.graph_edges ref
                  JOIN okg.graph_nodes site
                    ON site.node_id = ref.dst AND site.subtype = 'site'
                 WHERE ref.edge_type = 'references'
                """,
            )
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src=str(row["src"]),
                        edge_type="affects",
                        dst=str(row["dst"]),
                        attrs={"match_type": "reference_impact"},
                    )
                    for row in rows
                ],
            )
        self.observations.append({
            "candidates": result.candidates,
            "inserted": result.inserted,
            "skipped_existing_dedupe": result.skipped_existing_dedupe,
            "skipped_existing_live": result.skipped_existing_live,
        })
        return EnrichResult(
            self.name, n_edges_emitted=result.inserted, n_edges_skipped=result.skipped,
        )


def _retract_node(node_id: str, subtype: str) -> NodeFact:
    return NodeFact(
        node_id, subtype, {}, {"fixture": node_id}, {"revision": 1}, op="R",
    )


def test_derived_edge_returns_after_its_endpoint_leaves_and_returns_unchanged(
    clean_runtime: str, record_property,
) -> None:
    """Issue #1730, through three real publishes.

    The returning endpoint carries the same source revision it had in run 1,
    so the derived edge's dedupe key is byte-identical to run 1's. Before the
    fix, ``insert_deterministic_edges`` skipped any candidate whose key was
    anywhere in ``okg.edge_facts``, so once the endpoint left scope and the
    derived edge left the graph, the edge never came back.
    """
    deployment = "derived-edge-rederive"
    source = f"_enricher:{FullScanReferenceRollup.name}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "site"],
            narrowings=[
                ("ticket", "references", "site"),
                ("ticket", "affects", "site"),
            ],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                _node("ticket:1", "ticket"),
                _node("site:a", "site"),
                _edge("ticket:1", "site:a"),
            ],
            source="fixture_source",
        )

    rollup = FullScanReferenceRollup()
    # Every deployment stack carries this gate (default policy "block"); it
    # is what removes the derived edge once its endpoint is gone.
    publisher = Publisher(
        clean_runtime,
        deployment,
        enrichers=[rollup],
        consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )
    affects_id = mint_edge_id("ticket:1", "affects", "site:a")

    # Run 1: the derived edge is inserted and published.
    first = publisher.cycle()
    assert first["status"] == "published", first
    assert _affects_edges(clean_runtime) == {affects_id: ("ticket:1", "site:a")}
    assert _fact_count(clean_runtime, source) == 1

    # Run 2: the endpoint leaves scope. The source's retraction removes its
    # own reference edge; the publish gate then drops the dangling derived edge.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_retract_node("site:a", "site")], source="fixture_source",
        )
    second = publisher.cycle()
    assert second["status"] == "published", second
    assert _affects_edges(clean_runtime) == {}

    # Run 3: the endpoint and its reference return unchanged.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_node("site:a", "site"), _edge("ticket:1", "site:a")],
            source="fixture_source",
        )
    third = publisher.cycle()
    assert third["status"] == "published", third
    returning = rollup.observations[-1]
    assert returning == {
        "candidates": 1,
        "inserted": 1,
        "skipped_existing_dedupe": 0,
        "skipped_existing_live": 0,
    }, rollup.observations
    assert _affects_edges(clean_runtime) == {affects_id: ("ticket:1", "site:a")}
    assert _fact_count(clean_runtime, source) == 2
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(DISTINCT dedupe_key) FROM okg.edge_facts WHERE source = %s",
            (source,),
        )
        # The same key both times: the returning edge is the run-1 edge.
        assert int(cur.fetchone()[0]) == 1

    record_property("first_generation_id", first["generation_id"])
    record_property("second_generation_id", second["generation_id"])
    record_property("third_generation_id", third["generation_id"])
    record_property("edge_facts_after_third_cycle", _fact_count(clean_runtime, source))


def _publish_then_drop_derived_edge(
    dsn: str, deployment: str, rollup: FullScanReferenceRollup,
) -> Publisher:
    """Runs 1 and 2 of the #1730 scenario: the derived edge is published,
    then its endpoint leaves and the gate drops the edge. The endpoint is
    back, unchanged, when this returns; no publish has seen it yet."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["ticket", "site"],
            narrowings=[
                ("ticket", "references", "site"),
                ("ticket", "affects", "site"),
            ],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                _node("ticket:1", "ticket"),
                _node("site:a", "site"),
                _edge("ticket:1", "site:a"),
            ],
            source="fixture_source",
        )
    publisher = Publisher(
        dsn,
        deployment,
        enrichers=[rollup],
        consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(dsn, deployment),
    )
    affects_id = mint_edge_id("ticket:1", "affects", "site:a")
    first = publisher.cycle()
    assert first["status"] == "published", first
    assert _affects_edges(dsn) == {affects_id: ("ticket:1", "site:a")}, first
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_retract_node("site:a", "site")], source="fixture_source",
        )
    second = publisher.cycle()
    assert second["status"] == "published", second
    assert _affects_edges(dsn) == {}, second
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_node("site:a", "site"), _edge("ticket:1", "site:a")],
            source="fixture_source",
        )
    return publisher


def _envelope_count(dsn: str, source: str, status: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.admission_envelopes "
            "WHERE source = %s AND status = %s",
            (source, status),
        )
        return int(cur.fetchone()[0])


def test_rederived_edge_that_admission_now_rejects_is_not_emitted_every_cycle(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, record_property,
) -> None:
    """Issue #1730 review probe. The edge was published once, its endpoint
    left, and the producer's policy no longer allows the edge type. When the
    endpoint returns unchanged, the key is re-derived once; admission rejects
    that attempt, and later cycles skip the key instead of writing (and
    rejecting) a new fact every cycle because the old fact once reached the
    graph."""
    from tests.substrate.test_derived_producer_authority import (
        _materialize_input_profile,
        _pin_enforce_mode,
    )

    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Enforce mode refuses a derived edge whose input is less trusted
        # than its output; trust the fixture source so run 1 publishes.
        _materialize_input_profile(
            conn, producer_id="fixture_source", trust_label="repo-local-trusted",
        )
    source = f"_enricher:{FullScanReferenceRollup.name}"
    rollup = FullScanReferenceRollup()
    publisher = _publish_then_drop_derived_edge(clean_runtime, "test", rollup)
    assert _fact_count(clean_runtime, source) == 1
    assert _envelope_count(clean_runtime, source, "rejected") == 0
    setup_calls = len(rollup.observations)

    # The producer's declared output no longer includes `affects` (its edge
    # triples come from requires_narrowings), so admission refuses the edge
    # with producer_edge_triple_not_allowed.
    rollup.emits_edge_types = ("references",)
    rollup.requires_narrowings = (("ticket", "references", "site"),)
    counts: list[tuple[int, int]] = []
    for _ in range(3):
        outcome = publisher.cycle()
        assert outcome["status"] == "published", outcome
        counts.append((
            _fact_count(clean_runtime, source),
            _envelope_count(clean_runtime, source, "rejected"),
        ))

    assert _affects_edges(clean_runtime) == {}
    # One re-derived attempt, rejected once; nothing after it.
    assert counts == [(2, 1), (2, 1), (2, 1)], counts
    # The rejection is the producer's emission decision. The count above
    # spans every intent and is 1, so scoping this read to the emission
    # intent cannot hide a second rejection recorded under another intent.
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT verdict->>'code' FROM okg.admission_envelopes "
            "WHERE source = %s AND status = 'rejected' "
            "AND intent = 'derived_edge_emission'",
            (source,),
        )
        codes = [str(row[0]) for row in cur.fetchall()]
    assert codes == ["producer_edge_triple_not_allowed"], codes
    assert [o["inserted"] for o in rollup.observations[setup_calls:]] == [1, 0, 0], (
        rollup.observations
    )
    record_property("fact_and_rejected_counts_per_cycle", repr(counts))


def test_repeated_enricher_calls_before_a_publish_write_one_fact(
    clean_runtime: str, record_property,
) -> None:
    """Issue #1730 review probe. The edge was published once and dropped;
    its endpoint is back. Three enricher calls with no publish between them
    write the re-derived fact once: the newer fact has not been admitted or
    applied yet, so the old published fact no longer approves another."""
    source = f"_enricher:{FullScanReferenceRollup.name}"
    rollup = FullScanReferenceRollup()
    _publish_then_drop_derived_edge(clean_runtime, "derived-edge-repeat", rollup)
    before = _fact_count(clean_runtime, source)
    assert before == 1
    setup_calls = len(rollup.observations)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for _ in range(3):
            rollup.enrich(conn, generation_id=0)

    inserted = [o["inserted"] for o in rollup.observations[setup_calls:]]
    assert inserted == [1, 0, 0], rollup.observations
    assert _fact_count(clean_runtime, source) == before + 1
    record_property("inserted_per_call", repr(inserted))


def _live_edge(cur, src: str, dst: str, attrs: dict[str, Any]) -> None:
    """Make ``src -affects-> dst`` live with exactly ``attrs``, the way the
    publisher projects an admitted derived fact."""
    ChronosGraphStore.from_connection(cur.connection).apply_facts(
        [EdgeFact(
            src, dst, "affects", attrs=attrs,
            source_record_id={"fixture": f"{src}->{dst}"},
            source_revision={"revision": 1},
        )],
        source="fixture_source",
    )


def _stored_attrs(cur, src: str, dst: str) -> dict[str, Any]:
    cur.execute(
        "SELECT attrs FROM okg.edge_facts WHERE src = %s AND dst = %s "
        "ORDER BY fact_id DESC LIMIT 1",
        (src, dst),
    )
    return dict(cur.fetchone()[0])


def test_live_derived_edge_is_not_emitted_again(clean_runtime: str) -> None:
    """Issue #1730 keeps the no-churn rule: a key already in the fact log whose
    edge is live on the branch is skipped, not written a second time."""
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"ticket:live-{sfx}", f"site:live-{sfx}"
    candidate = DerivedEdgeCandidate(
        src=src, edge_type="affects", dst=dst, attrs={"reason": "same"},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        seed_catalog(
            conn, subtypes=["ticket", "site"],
            narrowings=[("ticket", "affects", "site")],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_node(src, "ticket"), _node(dst, "site")], source="fixture_source",
        )
        first = insert_deterministic_edges(
            cur, source="_enricher:fixture-live", candidates=[candidate],
        )
        _live_edge(cur, src, dst, _stored_attrs(cur, src, dst))
        second = insert_deterministic_edges(
            cur, source="_enricher:fixture-live", candidates=[candidate],
        )
        cur.execute(
            "SELECT count(*) FROM okg.edge_facts WHERE source = %s",
            ("_enricher:fixture-live",),
        )
        facts = int(cur.fetchone()[0])

    assert first.inserted == 1
    assert second.inserted == 0 and second.skipped == 1, second
    assert facts == 1


def test_emitted_edge_with_an_absent_endpoint_is_not_emitted_again(
    clean_runtime: str,
) -> None:
    """Issue #1730 re-derives only an edge that could be live now. While an
    endpoint is absent from the branch, a key already in the fact log stays
    skipped, so an unpublished or dangling fact is not written every run."""
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"ticket:gone-{sfx}", f"site:gone-{sfx}"
    candidate = DerivedEdgeCandidate(
        src=src, edge_type="affects", dst=dst, attrs={"reason": "same"},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        seed_catalog(conn, subtypes=["ticket"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_node(src, "ticket")], source="fixture_source",
        )
        first = insert_deterministic_edges(
            cur, source="_enricher:fixture-gone", candidates=[candidate],
        )
        second = insert_deterministic_edges(
            cur, source="_enricher:fixture-gone", candidates=[candidate],
        )

    assert first.inserted == 1
    assert second.inserted == 0
    assert second.skipped_existing_dedupe == 1, second


class _UndeclaredEdgeEmitter:
    """Emits an edge type it does not declare, so enforce-mode admission
    rejects the fact and the edge never reaches the graph."""

    name = "fixture_undeclared_edge_emitter"
    reads_subtypes = ("auth_src",)
    reads_edge_types = ()
    emits_edge_types = ("auth_allows",)
    requires_narrowings = (("auth_src", "auth_allows", "auth_dst"),)

    def __init__(self) -> None:
        self.results: list[Any] = []

    def enrich(self, conn, *, generation_id, incremental=None):
        del generation_id, incremental
        with conn.cursor() as cur:
            result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src="auth:src", edge_type="auth_denies", dst="auth:dst",
                    ),
                ],
            )
        self.results.append(result)
        return EnrichResult(
            self.name, n_edges_emitted=result.inserted, n_edges_skipped=result.skipped,
        )


def test_rejected_derived_fact_is_not_emitted_again(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #1730: a known key is re-derived only when its prior fact reached
    the graph. Here admission rejected it, so on the next run the edge is not
    live and both endpoints are, yet the key stays skipped."""
    from tests.substrate.test_derived_producer_authority import (
        _pin_enforce_mode,
        _seed_authority_catalog_and_nodes,
    )

    _pin_enforce_mode(monkeypatch, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_authority_catalog_and_nodes(conn)

    emitter = _UndeclaredEdgeEmitter()
    publisher = Publisher(
        clean_runtime,
        "test",
        enrichers=[emitter],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
    )
    first = publisher.cycle()
    second = publisher.cycle()
    source = f"_enricher:{emitter.name}"

    assert first["derived_rejections"]["rejected_producer_count"] == 1, first
    assert len(emitter.results) == 2, emitter.results
    assert emitter.results[0].inserted == 1
    assert emitter.results[1].inserted == 0, emitter.results[1]
    assert emitter.results[1].skipped_existing_dedupe == 1, emitter.results[1]
    assert second["derived_rejections"]["rejected_producer_count"] == 0, second
    assert _fact_count(clean_runtime, source) == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        live = _chronos.query_strict(
            cur,
            "SELECT edge_id FROM okg.graph_edges WHERE edge_type = 'auth_denies'",
        )
        nodes = _chronos.query_strict(
            cur,
            "SELECT node_id FROM okg.graph_nodes WHERE node_id IN ('auth:src', 'auth:dst')",
        )
        cur.execute(
            "SELECT DISTINCT e.status FROM okg.admission_envelopes e "
            "JOIN okg.admission_record_refs r USING (admission_id) "
            "WHERE r.source = %s",
            (source,),
        )
        statuses = sorted(str(row[0]) for row in cur.fetchall())
    assert live == []
    assert sorted(str(r["node_id"]) for r in nodes) == ["auth:dst", "auth:src"]
    assert statuses == ["rejected"], statuses


def test_unreadable_branch_raises_instead_of_answering_empty(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #1730: the helper's branch reads decide liveness, so a failed
    read must raise; an empty answer would skip or re-derive everything."""
    sfx = uuid.uuid4().hex[:8]

    def unreadable(*_args, **_kwargs):
        raise RuntimeError("fixture: branch unreadable")

    monkeypatch.setattr(ChronosGraphStore, "from_connection", unreadable)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        with pytest.raises(RuntimeError, match="branch unreadable"):
            insert_deterministic_edges(
                cur,
                source="_enricher:fixture-unreadable",
                candidates=[DerivedEdgeCandidate(
                    src=f"ticket:u-{sfx}", edge_type="affects", dst=f"site:u-{sfx}",
                )],
            )
        cur.execute(
            "SELECT count(*) FROM okg.edge_facts WHERE source = %s",
            ("_enricher:fixture-unreadable",),
        )
        assert int(cur.fetchone()[0]) == 0
        # The lenient reader keeps its old contract.
        assert _chronos.query(cur, "SELECT node_id FROM okg.graph_nodes") == []


def test_chronos_read_inside_a_write_transaction_keeps_the_witness(
    clean_runtime: str, record_property,
) -> None:
    """The in-transaction path (``okg enricher backfill`` opens its
    connection with ``autocommit=False`` and calls ``enrich()`` directly): a
    ``_chronos.query`` in the middle of a witnessed write transaction must
    leave the transaction open and the ``okg.source_program_checksum``
    witness exactly as it found it."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["ticket"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_node("ticket:w", "ticket")], source="fixture_source",
        )
    with psycopg.connect(clean_runtime, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config(%s, %s, true)", (_WITNESS_GUC, "fixture-witness"),
            )
            before = _witness(conn)
            rows = _chronos.query(
                cur,
                "SELECT node_id FROM okg.graph_nodes WHERE subtype = :subtype",
                {"subtype": "ticket"},
            )
            after = _witness(conn)
            status = conn.info.transaction_status.name
        conn.rollback()

    assert before == "fixture-witness"
    assert after == before
    assert status == "INTRANS"
    assert [r["node_id"] for r in rows] == ["ticket:w"]
    record_property("witness_before", before)
    record_property("witness_after", after)
    record_property("rows", len(rows))
