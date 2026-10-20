"""Unit coverage for the read-only derived-node exposure report."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg
import pytest

from okg.substrate.derived_node_liveness import (
    canonical_derived_support,
    classify_derived_node,
    plan_derived_node_cleanup,
    report_derived_node_exposure,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


def _enricher_row(
    node_id: str,
    *,
    members: list[str] | None = None,
    policy: str = "any_member",
    source: str = "_enricher:caption",
    derived_support: Any = None,
    derived_from: Any = None,
) -> dict[str, Any]:
    if derived_support is None and members is not None:
        derived_support = {
            "schema": "okg.derived_support.v1",
            "producer": source,
            "members": members,
            "policy": policy,
        }
    record: dict[str, Any] = {}
    if derived_support is not None:
        record["derived_support"] = derived_support
    if derived_from is not None:
        record["derived_from"] = derived_from
    return {
        "node_id": node_id,
        "subtype": "derived_claim",
        "source": source,
        "source_record_id": record,
    }


def test_any_member_requires_one_live_member() -> None:
    row = _enricher_row("d:any", members=["a", "b"], derived_from=["a", "b"])
    supported = classify_derived_node(row, live_node_ids={"a", "d:any"})
    unsupported = classify_derived_node(row, live_node_ids={"d:any"})

    assert supported["support_policy_state"] == "valid_any_member"
    assert supported["verdict"] == "supported"
    assert unsupported["verdict"] == "unsupported"
    assert unsupported["cause"] == "no_live_support_members"


def test_all_member_fails_closed_when_one_member_is_absent() -> None:
    row = _enricher_row(
        "d:all",
        members=["a", "b"],
        policy="all_members",
        derived_from=["a", "b"],
    )
    result = classify_derived_node(row, live_node_ids={"a", "d:all"})
    assert result["support_policy_state"] == "valid_all_members"
    assert result["verdict"] == "unsupported"
    assert result["cause"] == "all_support_members_not_live"


def test_legacy_members_with_no_live_support_are_truly_unsupported() -> None:
    row = _enricher_row("d:legacy", derived_from=["gone"])

    result = classify_derived_node(row, live_node_ids={"d:legacy"})

    assert result["support_policy_state"] == "legacy_policy_missing"
    assert result["verdict"] == "unsupported"
    assert result["cause"] == "legacy_no_live_support_members"


def test_nul_member_and_empty_producer_are_unresolved() -> None:
    nul_member = _enricher_row(
        "d:nul",
        members=["a\x00b"],
        derived_from=["a\x00b"],
    )
    empty_producer = _enricher_row(
        "d:empty-producer",
        members=["a"],
        source="_enricher:",
        derived_from=["a"],
    )

    assert classify_derived_node(
        nul_member, live_node_ids={"a\x00b"}
    )["verdict"] == "unresolved"
    assert classify_derived_node(
        empty_producer, live_node_ids={"a"}
    )["cause"] == "support_producer_invalid"


@pytest.mark.parametrize(
    "record, expected_state",
    [
        ({}, "missing"),
        ({"derived_support": {"schema": "wrong"}}, "malformed"),
        (
            {
                "derived_support": {
                    "schema": "okg.derived_support.v1",
                    "producer": "_enricher:caption",
                    "members": ["a"],
                    "policy": "unknown",
                },
                "derived_from": ["a"],
            },
            "unknown_policy",
        ),
    ],
)
def test_missing_malformed_or_unknown_support_is_unresolved(
    record: dict[str, Any],
    expected_state: str,
) -> None:
    row = {
        "node_id": "d:legacy",
        "subtype": "derived_claim",
        "source": "_enricher:caption",
        "source_record_id": record,
    }
    result = classify_derived_node(row, live_node_ids={"a"})
    assert result["support_policy_state"] == expected_state
    assert result["verdict"] == "unresolved"


def test_curated_and_separately_managed_content_is_protected() -> None:
    curated = classify_derived_node(
        {
            "node_id": "curated:1",
            "subtype": "claim",
            "source": "curated:operator",
            "source_record_id": {},
        },
        live_node_ids={"curated:1"},
    )
    extractor = classify_derived_node(
        {
            "node_id": "extract:1",
            "subtype": "claim",
            "source": "_extractor:regex",
            "source_record_id": {},
        },
        live_node_ids={"extract:1"},
    )
    assert curated["verdict"] == "curated"
    assert curated["cause"] == "protected_curated_content"
    assert extractor["verdict"] == "separately_managed"
    assert extractor["producer_family"] == "_extractor"


def _source_row(node_id: str, source: str = "source:fixture") -> dict[str, Any]:
    return {
        "node_id": node_id,
        "subtype": "thing",
        "source": source,
        "source_record_id": {},
    }


def test_canonical_support_deduplicates_but_never_defaults() -> None:
    support, members = canonical_derived_support(
        producer="_enricher:test",
        members=["b", "a", "b"],
        policy="any_member",
    )
    assert members == ["a", "b"]
    assert support == {
        "schema": "okg.derived_support.v1",
        "producer": "_enricher:test",
        "members": ["a", "b"],
        "policy": "any_member",
    }
    for invalid_members, invalid_policy in [
        ([], "any_member"),
        ([""], "any_member"),
        (["a\x00b"], "all_members"),
        (["a"], None),
        (["a"], "unknown"),
    ]:
        with pytest.raises(ValueError):
            canonical_derived_support(
                producer="_enricher:test",
                members=invalid_members,
                policy=invalid_policy,
            )


class _CaptureCursor:
    rowcount = 1

    def __init__(self, conn: "_CaptureConn") -> None:
        self.conn = conn

    def __enter__(self) -> "_CaptureCursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, _sql: str, params: dict[str, Any]) -> None:
        self.conn.params = params


class _CaptureConn:
    params: dict[str, Any] | None = None

    def cursor(self) -> _CaptureCursor:
        return _CaptureCursor(self)


def test_enricher_emitter_persists_exact_support_and_rejects_omission() -> None:
    from okg.substrate.library.enrichers._fact_emitters import emit_node_fact

    common = {
        "enricher_name": "fixture",
        "node_id": "derived:1",
        "subtype": "thing",
        "attrs": {},
        "dedupe_key": "derived:1",
        "source_record_id": {"id": "1"},
        "source_revision": {"v": 1},
        "run_id": "00000000-0000-0000-0000-000000000001",
        "observed_at": datetime.now(timezone.utc),
        "derived_from": ["b", "a", "b"],
    }
    # Issue #1256: the omission is a signature error now, not a per-candidate
    # runtime refusal that a caller could reach in a publisher cycle.
    with pytest.raises(TypeError, match="support_policy"):
        emit_node_fact(_CaptureConn(), **common)
    with pytest.raises(ValueError, match="policy"):
        emit_node_fact(_CaptureConn(), **common, support_policy=None)

    conn = _CaptureConn()
    outcome = emit_node_fact(conn, **common, support_policy="any_member")
    assert outcome.emitted is True
    assert conn.params is not None
    source_record = getattr(conn.params["srcrec"], "obj", conn.params["srcrec"])
    assert source_record["derived_from"] == ["a", "b"]
    assert source_record["derived_support"] == {
        "schema": "okg.derived_support.v1",
        "producer": "_enricher:fixture",
        "members": ["a", "b"],
        "policy": "any_member",
    }


def test_cleanup_fixed_point_handles_any_all_self_and_cycles() -> None:
    prior = [_source_row("a"), _source_row("b")]
    candidate = [
        _source_row("b"),
        _enricher_row(
            "d:any", members=["a", "b"], derived_from=["a", "b"]
        ),
        _enricher_row(
            "d:all",
            members=["a", "b"],
            policy="all_members",
            derived_from=["a", "b"],
        ),
        _enricher_row(
            "d:downstream", members=["d:all"], derived_from=["d:all"]
        ),
        _enricher_row(
            "d:cycle-a",
            members=["a", "b", "d:cycle-b"],
            derived_from=["a", "b", "d:cycle-b"],
        ),
        _enricher_row(
            "d:cycle-b", members=["d:cycle-a"], derived_from=["d:cycle-a"]
        ),
        _enricher_row(
            "d:self", members=["d:self"], derived_from=["d:self"]
        ),
    ]
    # The prior snapshot contains the same declarations while A was live.
    prior.extend(candidate[1:-1])

    plan = plan_derived_node_cleanup(candidate, prior, frontier_node_ids={"a"})

    assert plan["blocking_declarations"] == []
    assert plan["retract_node_ids"] == ["d:all", "d:downstream", "d:self"]
    assert set(plan["support_fixed_point_node_ids"]) == {
        "d:any",
        "d:cycle-a",
        "d:cycle-b",
    }


def test_cleanup_blocks_unknown_and_reached_legacy_but_discloses_unrelated() -> None:
    prior = [_source_row("a")]
    related_legacy = _enricher_row("d:legacy", derived_from=["a"])
    unrelated_legacy = _enricher_row("d:unrelated", derived_from=["z"])
    unknown = _enricher_row(
        "d:unknown",
        members=["missing"],
        derived_from=["missing"],
    )
    candidate = [related_legacy, unrelated_legacy]
    prior.extend([related_legacy, unrelated_legacy])

    plan = plan_derived_node_cleanup(candidate, prior, frontier_node_ids={"a"})

    assert plan["retract_node_ids"] == []
    assert plan["blocking_declarations"] == [
        {
            "node_id": "d:legacy",
            "status": "legacy_policy_missing",
            "cause": "legacy_support_policy_missing",
        }
    ]
    assert plan["unevaluated_legacy_node_ids"] == ["d:unrelated"]

    unknown_plan = plan_derived_node_cleanup([unknown], prior)
    assert any(
        blocker["status"] == "unresolved_member"
        and blocker["node_id"] == "d:unknown"
        for blocker in unknown_plan["blocking_declarations"]
    )


def test_any_member_with_known_live_support_does_not_block_on_old_member() -> None:
    """A known-live member proves support without guessing the old member dead."""
    candidate = [
        _source_row("b"),
        _enricher_row(
            "d:any",
            members=["a", "b"],
            derived_from=["a", "b"],
        ),
    ]
    prior = list(candidate)

    plan = plan_derived_node_cleanup(candidate, prior, frontier_node_ids={"b"})

    assert plan["blocking_declarations"] == []
    assert plan["retract_node_ids"] == []
    assert plan["support_fixed_point_node_ids"] == ["d:any"]


def _first_publish_rows(count: int) -> list[dict[str, Any]]:
    return [_source_row(f"n{index:07d}") for index in range(count)]


def test_first_publish_frontier_still_blocks_and_retracts_as_before() -> None:
    """A first publish puts every node on the frontier (#2941).

    The frontier walk changed from a list to a deque; the verdicts it feeds
    must not. Among many plain source nodes: a declaration with a live member
    and a declaration resting on it are supported, a declaration whose member
    is nowhere is a blocker, and a blocker suppresses every retraction.
    """
    plain = _first_publish_rows(20_000)
    supported = _enricher_row(
        "d:ok", members=["n0000005"], derived_from=["n0000005"]
    )
    chained = _enricher_row("d:chain", members=["d:ok"], derived_from=["d:ok"])
    self_only = _enricher_row(
        "d:self", members=["d:self"], derived_from=["d:self"]
    )
    dangling = _enricher_row(
        "d:bad", members=["missing"], derived_from=["missing"]
    )

    clean = plan_derived_node_cleanup(
        [*plain, supported, chained, self_only], []
    )
    assert clean["blocking_declarations"] == []
    assert clean["support_fixed_point_node_ids"] == ["d:chain", "d:ok"]
    assert clean["retract_node_ids"] == ["d:self"]
    assert clean["frontier_node_count"] == len(plain) + 3

    blocked = plan_derived_node_cleanup(
        [*plain, supported, chained, self_only, dangling], []
    )
    assert blocked["blocking_declarations"] == [
        {
            "node_id": "d:bad",
            "status": "unresolved_member",
            "cause": "member_absent_from_candidate_and_prior",
            "member_ids": ["missing"],
        }
    ]
    assert blocked["retract_node_ids"] == []
    assert blocked["unsupported_node_ids"] == ["d:self"]


def _thread_cpu_seconds(fn: Any) -> float:
    import time

    start = time.thread_time()
    fn()
    return time.thread_time() - start


def test_first_publish_frontier_walk_stays_linear() -> None:
    """The cleanup plan inside the publish lock must not be quadratic (#2941).

    On a first publish the frontier is every node. The walk popped the head
    of a list, which shifts the rest of the list each time: 19.6 s of CPU at
    400,000 nodes on the operator laptop, against 2.5 s with a deque. Checks
    growth, not a budget (the timing-tests-robust rule): quadrupling the node
    count multiplies a linear plan by about 4 and the list version by about
    13, so exponent log4(t(4n) / t(n)) separates them on any machine. The two
    sizes alternate within each round and each keeps its best time.
    """
    import math

    small, large = _first_publish_rows(100_000), _first_publish_rows(400_000)
    t_small = t_large = math.inf
    exponent = math.inf
    for _ in range(3):
        t_small = min(
            t_small,
            _thread_cpu_seconds(lambda: plan_derived_node_cleanup(small, [])),
        )
        t_large = min(
            t_large,
            _thread_cpu_seconds(lambda: plan_derived_node_cleanup(large, [])),
        )
        exponent = math.log(t_large / t_small, 4)
        if exponent <= 1.5:
            break
    assert exponent <= 1.5, (
        f"cleanup plan grows as n^{exponent:.2f}: {t_small:.2f}s at 100k "
        f"nodes, {t_large:.2f}s at 400k"
    )


@dataclass(frozen=True)
class _Checkpoint:
    checkpoint_id: str
    metadata: dict[str, Any]


class _Graph:
    def __init__(self, rows: list[dict[str, Any]], edge_count: int) -> None:
        self.rows = rows
        self.edge_count = edge_count
        self.closed = False

    def query(self, sql: str, _params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if "FROM okg.graph_nodes ORDER" in sql:
            return list(self.rows)
        if "COUNT(*)" in sql and "graph_nodes" in sql:
            return [{"n": len(self.rows)}]
        if "COUNT(*)" in sql and "graph_edges" in sql:
            return [{"n": self.edge_count}]
        raise AssertionError(f"unexpected query: {sql}")

    def close(self) -> None:
        self.closed = True


class _Store:
    def __init__(self, checkpoint: _Checkpoint, graph: _Graph) -> None:
        self.checkpoint = checkpoint
        self.graph = graph

    def latest_generation(self, **_kwargs: Any) -> _Checkpoint:
        return self.checkpoint

    def resolve_generation(self, *_args: Any, **_kwargs: Any) -> _Checkpoint:
        return self.checkpoint

    def checkout_generation(self, *_args: Any, **_kwargs: Any) -> _Graph:
        return self.graph


def test_report_pins_generation_groups_rows_and_proves_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {
            "node_id": "a",
            "subtype": "source_record",
            "source": "source_a",
            "source_record_id": {},
        },
        _enricher_row(
            "d:any",
            members=["a"],
            derived_from=["a"],
        ),
        _enricher_row(
            "d:legacy",
            derived_support={"schema": "unknown"},
            derived_from=["a"],
        ),
        {
            "node_id": "x",
            "subtype": "artifact",
            "source": "_extractor:regex",
            "source_record_id": {},
        },
        {
            "node_id": "c",
            "subtype": "claim",
            "source": "curated:operator",
            "source_record_id": {},
        },
    ]
    checkpoint = _Checkpoint(
        "okg:test:branch:default:gen:1",
        {
            "kind": "okg_generation",
            "status": "published",
            "deployment_name": "test",
            "branch": "default",
        },
    )
    graph = _Graph(rows, edge_count=3)
    store = _Store(checkpoint, graph)

    from okg.substrate.graph_store import ChronosGraphStore

    monkeypatch.setattr(
        ChronosGraphStore,
        "from_connection",
        classmethod(lambda cls, _conn, **_kwargs: store),
    )
    monkeypatch.setattr(
        Publisher,
        "chronos_graph_branch_mutation_token",
        lambda _conn, *, branch_name: "token-stable",
    )

    report = report_derived_node_exposure(
        object(),
        deployment_name="test",
        branch="default",
        example_limit=1,
    )

    assert report["schema"] == "okg.derived_node_exposure.v1"
    assert report["generation"] == checkpoint.checkpoint_id
    assert report["summary"]["node_count"] == 5
    assert report["summary"]["edge_count"] == 3
    assert report["summary"]["by_verdict"]["unresolved"] == 1
    assert report["unresolved_legacy"]["ids"] == ["d:legacy"]
    assert report["unresolved_legacy"]["truncated"] is False
    assert report["mutation_proof"]["ok"] is True
    assert graph.closed is True


@pytest.mark.postgres
def test_disposable_published_exposure_report_is_read_only(
    clean_runtime: str,
) -> None:
    from chronos_core.branching._common import clear_all_registry_caches

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache
    from okg.substrate.library.sources.base import NodeFact

    deployment = "derived-node-exposure-disposable"
    clear_all_registry_caches()
    clear_schema_ensured_cache()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions "
                "(status, applied_at, deployment_name) "
                "VALUES ('applied', now(), %s)",
                (deployment,),
            )
        store = ChronosGraphStore.from_connection(conn)
        store.context.ensure_metadata()
        store.ensure_schema()
        store.ensure_branch("default", from_branch="main")
        store.apply_facts(
            [
                NodeFact("input:a", "thing", {}, {"id": "a"}, {"v": 1}),
                NodeFact("input:b", "thing", {}, {"id": "b"}, {"v": 1}),
            ],
            branch="default",
            source="source:fixture",
        )
        store.apply_facts(
            [
                NodeFact(
                    "derived:any",
                    "thing",
                    {},
                    {
                        "derived_from": ["input:a", "input:b"],
                        "derived_support": {
                            "schema": "okg.derived_support.v1",
                            "producer": "_enricher:fixture",
                            "members": ["input:a", "input:b"],
                            "policy": "any_member",
                        },
                    },
                    {"v": 1},
                ),
                NodeFact(
                    "derived:legacy-orphan",
                    "thing",
                    {},
                    {
                        "derived_from": ["input:gone"],
                        "derived_support": {
                            "schema": "okg.derived_support.v1",
                            "producer": "_enricher:fixture",
                            "members": ["input:gone"],
                            "policy": "any_member",
                        },
                    },
                    {"v": 1},
                ),
            ],
            branch="default",
            source="_enricher:fixture",
        )
        # Reconstruct a pre-v1 live row below the new authoritative write
        # boundary. Public writes must never be able to create this state.
        session = store.checkout("default")
        legacy_row = dict(
            session.query(
                "SELECT * FROM okg.graph_nodes "
                "WHERE node_id = 'derived:legacy-orphan'"
            )[0]
        )
        legacy_row["source_record_id"] = {"derived_from": ["input:gone"]}
        from okg.substrate.graph_store_internal.common import NODE_TABLE

        session.upsert_rows(NODE_TABLE, [legacy_row])
        store.apply_facts(
            [NodeFact("curated:1", "thing", {}, {"id": 1}, {"v": 1})],
            branch="default",
            source="curated:operator",
        )
        store.apply_facts(
            [NodeFact("extractor:1", "thing", {}, {"id": 1}, {"v": 1})],
            branch="default",
            source="_extractor:fixture",
        )

    published = Publisher(clean_runtime, deployment, enrichers=[], catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()
    assert published["status"] == "published", published
    generation = str(published["generation_id"])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        report = report_derived_node_exposure(
            conn,
            deployment_name=deployment,
            generation=generation,
            example_limit=20,
        )

    assert report["generation"] == generation
    assert report["summary"]["node_count"] == 6
    assert report["summary"]["by_verdict"] == {
        "curated": 1,
        "separately_managed": 1,
        "source_owned": 2,
        "supported": 1,
        "unsupported": 1,
    }
    assert report["unresolved_legacy"]["total"] == 0
    assert report["mutation_proof"]["ok"] is True
    if os.environ.get("OKG_CAPTURE_DERIVED_EXPOSURE"):
        print("DERIVED_NODE_EXPOSURE_REPORT=" + json.dumps(report, sort_keys=True))
