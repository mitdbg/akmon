"""A live call never changes a pinned result (task.lre.pin, req.lre.pin-unchanged).

Issue #2013. Two claims, both against a real published generation:

* the SAME graph `expand` before and after a live call returns byte-identical
  rows and the same generation;
* nothing live reaches a published generation -- the node and edge counts at
  the pin are unchanged, and no live row appears in the graph tables.

These need a database. Nothing here can be proved on a fake: the whole point
is that the live path and the publish path share a connection and a session
and still do not touch each other, and a fake graph would make that true by
construction rather than by evidence.

The file is marked `postgres` and owned per pull request in
`tests/qa_lanes.yaml` under `substrate-db / core-db-contracts`. That is
deliberate: an owned test that skips in hosted CI proves nothing and reds the
summary gate (#2078, `scripts/ci/permanent_skips.py`). It uses the repository's
standard fixtures, so CI provisions it like every other database test and it
is never gated on an operator-provided variable.
"""
from __future__ import annotations

import copy
import uuid

import psycopg
import pytest

from okg.substrate.external_backed.live_dispatch import (
    GateOutcome,
    dispatch_live_relation,
)
from okg.substrate.external_backed.live_relations import (
    ANNOTATION_KEY,
    bind_live_relations,
    compile_args_model,
    parse_live_bindings,
    parse_live_relations,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import expand_neighbors, open_session
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

_DEGREE = 4

HITS = {
    "action": "external.opensearch.search",
    "sample_policy": "digest",
    "retention_class": "warm",
    "deadline_seconds": 5,
    "args": {"q": {"type": "string", "required": True}},
}

#: Rows the fake adapter returns. Shaped like graph neighbour rows ON PURPOSE:
#: if the envelope ever leaked into `neighbors`, rows that look nothing like
#: graph rows would make the leak obvious for the wrong reason. These would
#: blend in, so the test catches a leak the way production would suffer one.
LIVE_ROWS = [
    {"src": "n:live-1", "dst": "n:hub", "edge_type": "cites"},
    {"src": "n:live-2", "dst": "n:hub", "edge_type": "cites"},
]


def _publish_hub(conn: psycopg.Connection, sfx: str) -> tuple[str, list[str]]:
    seed_catalog(
        conn,
        subtypes=[SubtypeSpec("page")],
        narrowings=[("page", "cites", "page")],
    )
    hub = f"n:hub-{sfx}"
    peers = [f"n:peer-{sfx}-{i:03d}" for i in range(_DEGREE)]
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [NodeFact(hub, "page", {"text": "hub"}, {"id": hub}, {"v": 1})]
        + [NodeFact(p, "page", {"text": p}, {"id": p}, {"v": 1}) for p in peers]
        + [
            EdgeFact(
                src=p, dst=hub, edge_type="cites", attrs={},
                source_record_id={"src": p, "dst": hub},
                source_revision={"v": 1},
            )
            for p in peers
        ]
    )
    return hub, peers


def _bound():
    declared = parse_live_relations(
        {ANNOTATION_KEY: {"hits": dict(HITS)}}, subtype="page",
    )
    bindings = parse_live_bindings(
        {"hits": {"kind": "http", "config": {"url": "http://localhost"}}},
    )
    return bind_live_relations(declared, bindings)["hits"]


def _dispatch(node_id: str, *, generation_id, sampler):
    bound = _bound()
    return dispatch_live_relation(
        bound=bound,
        args_model=compile_args_model(bound.relation),
        args={"q": "anything"},
        node_id=node_id,
        gate=lambda relation, principal, node: GateOutcome(
            allowed=True, reason="allow", principal=principal or "service:x",
            action=relation.action,
        ),
        adapter=lambda **_: copy.deepcopy(LIVE_ROWS),
        write_sample=sampler,
        generation_id=generation_id,
        # No response budget: a direct call, outside MCP.
        room=None,
    )


def _counts(conn: psycopg.Connection) -> tuple[int, int]:
    """Row counts at the pin.

    `okg.graph_nodes` / `okg.graph_edges` are the published graph tables --
    Chronos views, not base tables. Counting them is the point: a live row
    that reached admission would have to appear HERE to be readable, and
    counting a staging table instead would pass while the graph was polluted.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.graph_nodes")
        nodes = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM okg.graph_edges")
        edges = cur.fetchone()[0]
    return nodes, edges


def test_a_graph_expand_is_identical_before_and_after_a_live_call(
    clean_runtime: str,
) -> None:
    """req.lre.pin-unchanged: same rows, same generation, same everything."""
    sfx = uuid.uuid4().hex[:8]
    samples: list[dict] = []
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, _ = _publish_hub(conn, sfx)
    Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        before = expand_neighbors(conn, session, hub, direction="in", limit=50)

        envelope = _dispatch(
            hub, generation_id=session.generation_id,
            sampler=lambda **kw: samples.append(kw),
        )
        assert envelope["state"] == "ok"
        # Fenced through the same primitive the #83 overlay uses, so compare
        # the values inside the markers rather than the raw strings.
        assert [r["src"].split("\n")[1] for r in envelope["rows"]] == [
            "n:live-1", "n:live-2",
        ], "the live call must really run"

        after = expand_neighbors(conn, session, hub, direction="in", limit=50)

    assert before == after
    assert before["pinned_generation_id"] == after["pinned_generation_id"]
    assert len(before["neighbors"]) == _DEGREE
    assert len(samples) == 1, "the call must leave exactly one sample"


def test_live_rows_never_enter_the_graph_or_a_published_generation(
    clean_runtime: str,
) -> None:
    """Nothing live is admitted, published, or visible to a later read.

    The counts are taken before and after, and a second publish cycle runs
    between them: if a live row could reach the admission path at all, the
    cycle is where it would land.
    """
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, _ = _publish_hub(conn, sfx)
    identity = bind_test_catalog_writer(clean_runtime, "default")
    Publisher(clean_runtime, "default", catalog_identity=identity).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        before_counts = _counts(conn)
        session = open_session(conn, deployment_name="default")
        _dispatch(
            hub, generation_id=session.generation_id,
            sampler=lambda **kw: None,
        )

    Publisher(clean_runtime, "default", catalog_identity=identity).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _counts(conn) == before_counts
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.graph_nodes "
                "WHERE node_id LIKE 'n:live-%%'"
            )
            assert cur.fetchone()[0] == 0
        session = open_session(conn, deployment_name="default")
        rows = expand_neighbors(conn, session, hub, direction="in", limit=50)

    ids = {str(row.get("src")) for row in rows["neighbors"]}
    assert not (ids & {"n:live-1", "n:live-2"}), (
        "a live row reached the pinned graph result"
    )


def test_the_live_envelope_is_separate_from_the_graph_result(
    clean_runtime: str,
) -> None:
    """The envelope sits BESIDE `neighbors`, and the graph result is untouched.

    LIVE_ROWS are shaped like graph neighbour rows, so a leak would blend in
    rather than announce itself -- which is how a real one would behave.
    """
    sfx = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        hub, _ = _publish_hub(conn, sfx)
    Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        graph = expand_neighbors(conn, session, hub, direction="in", limit=50)
        envelope = _dispatch(
            hub, generation_id=session.generation_id,
            sampler=lambda **kw: None,
        )

    assert envelope["trust"] == "untrusted"
    assert envelope["pinned_generation_id"] == graph["pinned_generation_id"]
    assert set(envelope) & {"neighbors", "items", "results"} == set()
    assert len(graph["neighbors"]) == _DEGREE
