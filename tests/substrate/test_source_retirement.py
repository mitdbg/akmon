"""Runtime refinement tests for guarded source retirement (#1230)."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.source_retirement import (
    SourceRetirementPlan,
    _plan_from_graph,
    remediate_source_retirement,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


class _Graph:
    def __init__(self, *, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]):
        self.nodes = nodes
        self.edges = edges

    def query(self, sql: str, params: dict[str, Any] | None = None):
        params = params or {}
        if "FROM okg.graph_nodes" in sql and "source = ANY" in sql:
            return [row for row in self.nodes if row.get("source") in params["subjects"]]
        if "FROM okg.graph_edges" in sql:
            subjects = set(params["subjects"])
            node_ids = set(params.get("node_ids") or [])
            return [
                row
                for row in self.edges
                if row.get("source") in subjects
                or row.get("src") in node_ids
                or row.get("dst") in node_ids
            ]
        if "source_record_id->'derived_from'" in sql:
            return [
                row
                for row in self.nodes
                if str(row.get("source") or "").startswith("_enricher:")
                or (
                    isinstance(row.get("source_record_id"), dict)
                    and "derived_from" in row["source_record_id"]
                )
            ]
        raise AssertionError(sql)


def _node(node_id: str, source: str, *, derived_from: Any = None) -> dict[str, Any]:
    source_record_id: dict[str, Any] = {"record": node_id}
    if derived_from is not None:
        source_record_id["derived_from"] = derived_from
    return {
        "node_id": node_id,
        "subtype": "thing",
        "attrs": {},
        "source": source,
        "source_record_id": source_record_id,
        "source_revision": {"rev": 1},
        "run_id": "run-1",
        "observed_at": "2026-08-14T00:00:00Z",
    }


def _edge(edge_id: int, src: str, dst: str, source: str) -> dict[str, Any]:
    return {
        "edge_id": edge_id,
        "src": src,
        "dst": dst,
        "edge_type": "relates_to",
        "attrs": {},
        "provenance": "explicit",
        "confidence": 1.0,
        "source": source,
        "source_record_id": {"record": str(edge_id)},
        "source_revision": {"rev": 1},
        "run_id": "run-1",
        "observed_at": "2026-08-14T00:00:00Z",
    }


def _identity(*_args: Any, **_kwargs: Any):
    return (
        "alpha",
        {"ownership_id": "dep.alpha"},
        ("alpha", "dep.alpha"),
        "registry-digest",
    )


def _plan(monkeypatch: pytest.MonkeyPatch, graph: _Graph) -> SourceRetirementPlan:
    monkeypatch.setattr("okg.substrate.source_retirement._registry_identity", _identity)
    return _plan_from_graph(
        object(),  # planning below is graph-only after registry resolution
        graph,
        deployment_name="dep",
        branch_name="default",
        requested_source="alpha",
        pinned_checkpoint="checkpoint-1",
        catalog_version_id=7,
        branch_mutation_token="token-1",
    )


def test_plan_captures_source_owned_and_incident_edge_footprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(
        monkeypatch,
        _Graph(
            nodes=[_node("a", "dep.alpha"), _node("b", "dep.beta")],
            edges=[
                _edge(1, "a", "b", "dep.alpha"),
                _edge(2, "b", "a", "dep.beta"),
                _edge(3, "b", "elsewhere", "dep.alpha"),
            ],
        ),
    )

    assert [row["node_id"] for row in plan.nodes] == ["a"]
    assert [row["edge_id"] for row in plan.edges] == [1, 2, 3]
    assert plan.refusal_codes == ()
    assert plan.registry_source_id == "alpha"
    assert plan.source_subject_ids == ("alpha", "dep.alpha")
    assert len(plan.plan_digest) == 64


def test_plan_carries_nonempty_derived_frontier_to_publish_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(
        monkeypatch,
        _Graph(
            nodes=[
                _node("a", "dep.alpha"),
                _node("derived", "_enricher:test", derived_from=["a"]),
            ],
            edges=[],
        ),
    )
    assert plan.refusal_codes == ()
    assert plan.derived_support_frontier[0]["status"] == "legacy_policy_missing"


def test_plan_does_not_claim_unrelated_malformed_declaration_is_in_frontier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _plan(
        monkeypatch,
        _Graph(
            nodes=[
                _node("a", "dep.alpha"),
                _node("malformed", "_enricher:test", derived_from="a"),
            ],
            edges=[],
        ),
    )

    assert plan.refusal_codes == ()
    assert plan.derived_support_frontier == ()


@pytest.mark.parametrize("derived_from", [None, []])
def test_plan_leaves_unrelated_missing_declaration_to_exposure_report(
    monkeypatch: pytest.MonkeyPatch,
    derived_from: Any,
) -> None:
    plan = _plan(
        monkeypatch,
        _Graph(
            nodes=[
                _node("a", "dep.alpha"),
                _node("unknown", "_enricher:test", derived_from=derived_from),
            ],
            edges=[],
        ),
    )

    assert plan.refusal_codes == ()
    assert plan.derived_support_frontier == ()


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, _sql: str, _params: object = None) -> None:
        return None

    def fetchone(self):
        return None


class _Connection:
    def cursor(self) -> _Cursor:
        return _Cursor()


def test_apply_refuses_stale_digest_and_wrong_confirmation_before_core_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _plan(monkeypatch, _Graph(nodes=[_node("a", "dep.alpha")], edges=[]))
    monkeypatch.setattr("okg.substrate.source_retirement._registry_identity", _identity)
    monkeypatch.setattr(
        "okg.substrate.source_retirement.plan_source_retirement",
        lambda *_args, **_kwargs: base,
    )
    applied = False

    def _unexpected_apply(*_args: Any, **_kwargs: Any):
        nonlocal applied
        applied = True

    monkeypatch.setattr("okg.substrate.source_retirement._apply_plan", _unexpected_apply)

    with pytest.raises(ValueError, match="digest changed"):
        remediate_source_retirement(
            _Connection(),  # type: ignore[arg-type]
            source_name="alpha",
            deployment_name="dep",
            expected_plan_digest="wrong",
            confirm_source="alpha",
            apply=True,
        )
    with pytest.raises(ValueError, match="--confirm-source naming the source"):
        remediate_source_retirement(
            _Connection(),  # type: ignore[arg-type]
            source_name="alpha",
            deployment_name="dep",
            expected_plan_digest=base.plan_digest,
            confirm_source="dep.beta",
            apply=True,
        )
    assert applied is False


@pytest.mark.parametrize("confirmation", ["alpha", "dep.alpha"])
def test_apply_accepts_bare_name_or_ownership_id_as_confirmation(
    monkeypatch: pytest.MonkeyPatch, confirmation: str,
) -> None:
    """Issue #2477: the plan shows `dep.alpha`; typing it back must work."""
    base = _plan(monkeypatch, _Graph(nodes=[_node("a", "dep.alpha")], edges=[]))
    monkeypatch.setattr("okg.substrate.source_retirement._registry_identity", _identity)
    monkeypatch.setattr(
        "okg.substrate.source_retirement.plan_source_retirement",
        lambda *_args, **_kwargs: base,
    )
    applied: list[str] = []
    monkeypatch.setattr(
        "okg.substrate.source_retirement._apply_plan",
        lambda _conn, plan, **_kwargs: applied.append(plan.plan_digest) or {},
    )

    result = remediate_source_retirement(
        _Connection(),  # type: ignore[arg-type]
        source_name="alpha",
        deployment_name="dep",
        expected_plan_digest=base.plan_digest,
        confirm_source=confirmation,
        apply=True,
    )

    assert result.applied is True
    assert applied == [base.plan_digest]


def test_apply_refuses_cross_deployment_plan_before_core_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _plan(monkeypatch, _Graph(nodes=[_node("a", "dep.alpha")], edges=[]))
    crossed = replace(base, deployment_name="other")
    monkeypatch.setattr("okg.substrate.source_retirement._registry_identity", _identity)
    monkeypatch.setattr(
        "okg.substrate.source_retirement.plan_source_retirement",
        lambda *_args, **_kwargs: crossed,
    )
    applied = False

    def _unexpected_apply(*_args: Any, **_kwargs: Any):
        nonlocal applied
        applied = True

    monkeypatch.setattr("okg.substrate.source_retirement._apply_plan", _unexpected_apply)

    with pytest.raises(SubstrateError) as caught:
        remediate_source_retirement(
            _Connection(),  # type: ignore[arg-type]
            source_name="alpha",
            deployment_name="dep",
            expected_plan_digest=crossed.plan_digest,
            confirm_source="alpha",
            apply=True,
        )

    assert caught.value.data["refusal_codes"] == ["cross_deployment_plan"]
    assert caught.value.data["mutation_started"] is False
    assert applied is False


def test_plan_digest_changes_with_deployment_and_registry_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _plan(monkeypatch, _Graph(nodes=[_node("a", "dep.alpha")], edges=[]))
    other_deployment = _plan_from_graph(
        object(),
        _Graph(nodes=[_node("a", "dep.alpha")], edges=[]),
        deployment_name="other",
        branch_name="default",
        requested_source="alpha",
        pinned_checkpoint="checkpoint-1",
        catalog_version_id=7,
        branch_mutation_token="token-1",
    )
    assert other_deployment.plan_digest != first.plan_digest
    monkeypatch.setattr(
        "okg.substrate.source_retirement._registry_identity",
        lambda *_args, **_kwargs: (
            "alpha",
            {"ownership_id": "dep.alpha"},
            ("alpha", "dep.alpha"),
            "changed-registry-digest",
        ),
    )
    changed = _plan_from_graph(
        object(),
        _Graph(nodes=[_node("a", "dep.alpha")], edges=[]),
        deployment_name="dep",
        branch_name="default",
        requested_source="alpha",
        pinned_checkpoint="checkpoint-1",
        catalog_version_id=7,
        branch_mutation_token="token-1",
    )
    assert changed.plan_digest != first.plan_digest


def test_registry_identity_refuses_ambiguous_runtime_subject(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.source_retirement import _registry_identity

    registry = tmp_path / "source_registry.yaml"
    registry.write_text("sources: {}\n")
    monkeypatch.setattr(
        "okg.substrate.mcp.source_admin._load_registry_yaml",
        lambda _deployment: (
            registry,
            {
                "sources": {
                    "alpha": {
                        "ownership_id": "dep.shared",
                        "admission_policy": {"producer_id": "producer-a"},
                    },
                    "beta": {
                        "ownership_id": "dep.shared",
                        "admission_policy": {"producer_id": "producer-b"},
                    },
                }
            },
        ),
    )

    with pytest.raises(SubstrateError, match="ambiguous"):
        _registry_identity("dep", "dep.shared")


@pytest.mark.postgres
def test_source_retirement_apply_publish_and_replay_preserve_history(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from chronos_core.branching._common import clear_all_registry_caches

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.publisher import Publisher
    from okg.substrate.source_retirement import (
        bind_source_retirements_to_published_generation,
        mark_source_retirement_published,
        plan_source_retirement,
        source_registry_removal_authority,
    )

    deployment = "source-retirement-test"
    monkeypatch.setattr("okg.substrate.source_retirement._registry_identity", _identity)
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
                NodeFact("alpha:1", "thing", {}, {"id": 1}, {"v": 1}),
                NodeFact("alpha:2", "thing", {}, {"id": 2}, {"v": 1}),
            ],
            branch="default",
            source="dep.alpha",
        )
        store.apply_facts(
            [NodeFact("beta:1", "thing", {}, {"id": 3}, {"v": 1})],
            branch="default",
            source="dep.beta",
        )
        store.apply_facts(
            [
                EdgeFact("alpha:1", "beta:1", "relates_to"),
                EdgeFact("beta:1", "beta:1", "source_note"),
            ],
            branch="default",
            source="dep.alpha",
            strict_endpoints=False,
        )
        store.apply_facts(
            [EdgeFact("beta:1", "alpha:2", "points_to")],
            branch="default",
            source="dep.beta",
            strict_endpoints=False,
        )
    before = Publisher(clean_runtime, deployment, enrichers=[], catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()
    assert before["status"] == "published", before
    baseline = str(before["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        plan = plan_source_retirement(
            conn,
            source="alpha",
            deployment_name=deployment,
        )
        assert len(plan.nodes) == 2
        assert len(plan.edges) == 3
        with pytest.raises(RuntimeError, match="fault after graph edits"):
            remediate_source_retirement(
                conn,
                source_name="alpha",
                deployment_name=deployment,
                expected_plan_digest=plan.plan_digest,
                confirm_source="alpha",
                apply=True,
                _fault_after_graph=True,
            )
        after_fault = store.checkout("default")
        assert len(after_fault.query("SELECT node_id FROM okg.graph_nodes")) == 3
        assert len(after_fault.query("SELECT edge_id FROM okg.graph_edges")) == 3
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*), min(status) FROM okg.source_retirement_receipts "
                "WHERE deployment_name = %s",
                (deployment,),
            )
            assert cur.fetchone() == (1, "planned")

        # A mutation-free planned intent can be explicitly superseded after
        # real footprint drift; applying the old digest remains fail-closed.
        store.apply_facts(
            [NodeFact("alpha:3", "thing", {}, {"id": 4}, {"v": 1})],
            branch="default",
            source="dep.alpha",
        )
        with pytest.raises(ValueError, match="digest changed"):
            remediate_source_retirement(
                conn,
                source_name="alpha",
                deployment_name=deployment,
                expected_plan_digest=plan.plan_digest,
                confirm_source="alpha",
                apply=True,
            )
        refreshed = plan_source_retirement(
            conn,
            source="alpha",
            deployment_name=deployment,
        )
        assert refreshed.plan_digest != plan.plan_digest
        assert [row["node_id"] for row in refreshed.nodes] == [
            "alpha:1",
            "alpha:2",
            "alpha:3",
        ]
        result = remediate_source_retirement(
            conn,
            source_name="alpha",
            deployment_name=deployment,
            expected_plan_digest=refreshed.plan_digest,
            confirm_source="alpha",
            apply=True,
        )
        receipt = result.details["receipt"]
        assert result.applied is True
        assert result.details["replayed"] is True
        assert receipt["status"] == "awaiting_publish"

        live = store.checkout("default")
        assert live.query(
            "SELECT node_id FROM okg.graph_nodes ORDER BY node_id"
        ) == [{"node_id": "beta:1"}]
        assert live.query("SELECT edge_id FROM okg.graph_edges") == []

        # A source write after apply but before publication produces an
        # explicit continuation plan instead of stranding the receipt.
        store.apply_facts(
            [NodeFact("alpha:4", "thing", {}, {"id": 5}, {"v": 1})],
            branch="default",
            source="dep.alpha",
        )
        continuation_preview = remediate_source_retirement(
            conn,
            source_name="alpha",
            deployment_name=deployment,
        )
        assert continuation_preview.applied is False
        assert continuation_preview.details["continuation_required"] is True
        continuation_digest = continuation_preview.details["plan"]["plan_digest"]
        with pytest.raises(ValueError, match="digest changed"):
            remediate_source_retirement(
                conn,
                source_name="alpha",
                deployment_name=deployment,
                expected_plan_digest=refreshed.plan_digest,
                confirm_source="alpha",
                apply=True,
            )
        continuation = remediate_source_retirement(
            conn,
            source_name="alpha",
            deployment_name=deployment,
            expected_plan_digest=continuation_digest,
            confirm_source="alpha",
            apply=True,
        )
        assert continuation.details["continuation"] is True
        assert continuation.details["plan"]["counts"]["nodes"] == 4
        receipt = continuation.details["receipt"]
        live = store.checkout("default")
        assert live.query(
            "SELECT node_id FROM okg.graph_nodes ORDER BY node_id"
        ) == [{"node_id": "beta:1"}]
        historical = store.checkout_generation(baseline)
        assert len(historical.query("SELECT node_id FROM okg.graph_nodes")) == 3
        assert len(historical.query("SELECT edge_id FROM okg.graph_edges")) == 3
        # Timestamp eligibility alone is insufficient: an older published
        # generation still containing the exact plan footprint must not bind
        # the pending receipt.
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.source_retirement_receipts "
                "SET applied_at = (SELECT created_at::timestamptz - interval '1 second' "
                "FROM _chronos_branch_interval_checkpoints "
                "WHERE checkpoint_id = %s) WHERE retirement_id = %s",
                (baseline, receipt["retirement_id"]),
            )
        with pytest.raises(SubstrateError) as bad_bind:
            bind_source_retirements_to_published_generation(
                conn,
                deployment_name=deployment,
                branch_name="default",
                generation_id=baseline,
            )
        assert bad_bind.value.code == "source_retirement_refused"
        assert bad_bind.value.data["refusal_codes"] == [
            "retirement_footprint_still_live"
        ]
        assert bad_bind.value.data["remaining_node_ids"] == ["alpha:1", "alpha:2"]
        with pytest.raises(SubstrateError) as bad_manual_bind:
            mark_source_retirement_published(
                conn,
                retirement_id=receipt["retirement_id"],
                repair_generation_id=baseline,
            )
        assert bad_manual_bind.value.data["refusal_codes"] == [
            "retirement_footprint_still_live"
        ]
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.source_retirement_receipts SET applied_at = now() "
                "WHERE retirement_id = %s",
                (receipt["retirement_id"],),
            )
        with pytest.raises(SubstrateError, match="published retirement receipt"):
            with source_registry_removal_authority(
                conn,
                source="alpha",
                deployment_name=deployment,
            ):
                pass

    published = Publisher(clean_runtime, deployment, enrichers=[], catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()
    assert published["status"] == "published", published
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, repair_generation_id "
                "FROM okg.source_retirement_receipts "
                "WHERE retirement_id = %s",
                (receipt["retirement_id"],),
            )
            assert cur.fetchone() == (
                "published",
                str(published["generation_id"]),
            )
        # The explicit helper is idempotent after the publisher's automatic bind.
        mark_source_retirement_published(
            conn,
            retirement_id=receipt["retirement_id"],
            repair_generation_id=str(published["generation_id"]),
        )
        replay = remediate_source_retirement(
            conn,
            source_name="alpha",
            deployment_name=deployment,
            expected_plan_digest=continuation_digest,
            confirm_source="alpha",
            apply=True,
        )
        assert replay.details["replayed"] is True
        assert replay.details["status"] == "published"
        with source_registry_removal_authority(
            conn,
            source="alpha",
            deployment_name=deployment,
        ) as authority:
            assert authority["retirement_id"] == receipt["retirement_id"]
            assert authority["repair_generation_id"] == str(
                published["generation_id"]
            )


@pytest.mark.postgres
def test_retired_source_never_inherits_a_shared_node(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #1901: retirement withdraws the source's declarations of nodes it
    does not own. Without that, the owner's later retraction would pass the
    node to the retired source, whose name sorts first."""
    from chronos_core.branching._common import clear_all_registry_caches

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher
    from okg.substrate.source_retirement import plan_source_retirement

    def shared(source: str, op: str = "I") -> NodeFact:
        return NodeFact(
            "author:shared",
            "thing",
            {} if op == "R" else {"seen_in": source},
            {"author": "shared", "repo": source},
            {"v": 1},
            op=op,
            ownership_mode="substitute",
        )

    deployment = "source-retirement-declarers"
    monkeypatch.setattr("okg.substrate.source_retirement._registry_identity", _identity)
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
        store.apply_facts([shared("dep.omega")], branch="default", source="dep.omega")
        store.apply_facts(
            [
                NodeFact("alpha:1", "thing", {}, {"id": 1}, {"v": 1}),
                shared("dep.alpha"),
            ],
            branch="default",
            source="dep.alpha",
        )
        store.apply_facts([shared("dep.beta")], branch="default", source="dep.beta")
    before = Publisher(
        clean_runtime,
        deployment,
        enrichers=[],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert before["status"] == "published", before
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        plan = plan_source_retirement(conn, source="alpha", deployment_name=deployment)
        remediate_source_retirement(
            conn,
            source_name="alpha",
            deployment_name=deployment,
            expected_plan_digest=plan.plan_digest,
            confirm_source="alpha",
            apply=True,
        )
        live = store.checkout("default")
        assert sorted(
            str(row["source"])
            for row in live.query(
                "SELECT source FROM okg.graph_node_declarers "
                "WHERE node_id = 'author:shared'"
            )
        ) == ["dep.beta", "dep.omega"]

        store.apply_facts(
            [shared("dep.omega", op="R")], branch="default", source="dep.omega",
        )
        rows = store.checkout("default").query(
            "SELECT source FROM okg.graph_nodes WHERE node_id = 'author:shared'"
        )
        assert [row["source"] for row in rows] == ["dep.beta"]


@pytest.mark.postgres
def test_retirement_applies_after_catalog_applied_without_a_publish(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2477: a failed `add-source` applies a newer catalog and
    publishes nothing. The plan used to pin the last published checkpoint's
    catalog while apply compared the latest applied one, so every apply
    refused "catalog version drifted" and replanning never cleared it."""
    from chronos_core.branching._common import clear_all_registry_caches

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher
    from okg.substrate.source_retirement import plan_source_retirement

    deployment = "source-retirement-catalog-drift"
    monkeypatch.setattr("okg.substrate.source_retirement._registry_identity", _identity)
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
            [NodeFact("alpha:1", "thing", {}, {"id": 1}, {"v": 1})],
            branch="default",
            source="dep.alpha",
        )
    published = Publisher(
        clean_runtime,
        deployment,
        enrichers=[],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    ).cycle()
    assert published["status"] == "published", published
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest = store.latest_generation(
            deployment_name=deployment, branch="default", status="published",
        )
        pinned = int((latest.metadata or {}).get("catalog_version_id") or 0)
        # What a failed add-source leaves behind: a newer applied catalog,
        # and no publish that pins it.
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions "
                "(status, applied_at, deployment_name) "
                "VALUES ('applied', now(), %s) RETURNING catalog_version_id",
                (deployment,),
            )
            newer = int(cur.fetchone()[0])
        assert newer != pinned

        plan = plan_source_retirement(conn, source="alpha", deployment_name=deployment)
        assert plan.catalog_version_id == newer

        # A catalog applied AFTER the review still refuses the apply: the
        # catalog version is part of the reviewed plan digest.
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions "
                "(status, applied_at, deployment_name) "
                "VALUES ('applied', now(), %s) RETURNING catalog_version_id",
                (deployment,),
            )
            newest = int(cur.fetchone()[0])
        with pytest.raises(ValueError, match="digest changed"):
            remediate_source_retirement(
                conn,
                source_name="alpha",
                deployment_name=deployment,
                expected_plan_digest=plan.plan_digest,
                confirm_source="alpha",
                apply=True,
            )

        # Planning again now succeeds, which it never could before.
        replanned = plan_source_retirement(
            conn, source="alpha", deployment_name=deployment,
        )
        assert replanned.catalog_version_id == newest
        result = remediate_source_retirement(
            conn,
            source_name="alpha",
            deployment_name=deployment,
            expected_plan_digest=replanned.plan_digest,
            confirm_source="dep.alpha",
            apply=True,
        )
        assert result.applied is True
        assert result.details["receipt"]["status"] == "awaiting_publish"
        assert store.checkout("default").query(
            "SELECT node_id FROM okg.graph_nodes WHERE source = 'dep.alpha'"
        ) == []


@pytest.mark.postgres
def test_remove_source_resumes_a_failed_publish_without_retracting_twice(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Issue #2477: `okg deployment remove-source` against real PostgreSQL.

    The first apply disables the entry, retracts the records, and then its
    publish fails. The re-run reads the real receipt (`awaiting_publish`),
    publishes, and removes the entry without a second retraction.

    What is real and what is not: the retraction, the publishes (in-process
    `Publisher.cycle`), the receipt and the footprint reads run against
    PostgreSQL. The publish FAILURE is simulated by the fake `okg` runner,
    the claim is faked, and the single retraction is counted in Python by
    wrapping `remediate_source_retirement`. The end-to-end proof is the
    recorded lab-host run.
    """
    import json
    import subprocess

    import yaml
    from chronos_core.branching._common import clear_all_registry_caches

    from okg.substrate import source_retirement
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.mcp import source_admin
    from okg.substrate.publisher import Publisher
    from okg.substrate.sources import guided_remove

    deployment = "remove-source-resume"
    dep = tmp_path / deployment
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump(
        {"name": deployment, "postgres": {"dsn": clean_runtime}},
    ))
    registry = dep / "source_registry.yaml"
    registry.write_text(yaml.safe_dump({"sources": {"alpha": {
        "ownership_id": "dep.alpha",
        "enabled_by_default": True,
        "admission_policy": {"producer_id": "dep.alpha"},
    }}}))
    monkeypatch.setattr(source_admin, "_deployment_dir", lambda _name, **_kw: dep)

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
                NodeFact("alpha:1", "thing", {"title": "One"}, {"id": 1}, {"v": 1}),
                NodeFact("alpha:2", "thing", {"title": "Two"}, {"id": 2}, {"v": 1}),
            ],
            branch="default",
            source="dep.alpha",
        )
        store.apply_facts(
            [EdgeFact("alpha:1", "alpha:2", "relates_to")],
            branch="default",
            source="dep.alpha",
        )

    def publish() -> dict:
        return Publisher(
            clean_runtime,
            deployment,
            enrichers=[],
            catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
        ).cycle()

    assert publish()["status"] == "published"

    retractions: list[dict] = []
    real_remediate = source_retirement.remediate_source_retirement

    def counting(conn, **kwargs):
        if kwargs.get("apply"):
            retractions.append(kwargs)
        return real_remediate(conn, **kwargs)

    monkeypatch.setattr(source_retirement, "remediate_source_retirement", counting)
    runs: list[str] = []

    def runner(argv, **_kwargs):
        if "run" in argv:
            runs.append("run")
            if len(runs) == 1:
                body = {"run_summary": {"outcome": "failed"}}
                return subprocess.CompletedProcess(
                    argv, 1, json.dumps(body), "publish failed by the test",
                )
            result = publish()
            body = {"run_summary": {
                "outcome": "published", "generation_id": result["generation_id"],
            }}
            return subprocess.CompletedProcess(argv, 0, json.dumps(body), "")
        if "claim" in argv:
            return subprocess.CompletedProcess(argv, 0, "{}", "")
        raise AssertionError(argv)

    environ = {"PATH": "/usr/bin"}
    plan = guided_remove.plan_remove_source(
        deployment=str(dep), name="dep.alpha", environ=environ,
    )
    assert plan.action == guided_remove.RETIRE
    assert (plan.records, plan.relationships) == (2, 1)

    first = guided_remove.apply_remove_source(
        plan, runner=runner, environ=environ, okg_argv_prefix=["okg"],
    )
    assert first.status == "failed"
    assert first.as_dict()["failed_step"]["step"] == "publish"
    assert len(retractions) == 1
    entry = yaml.safe_load(registry.read_text())["sources"]["alpha"]
    assert entry["enabled_by_default"] is False  # disabled before retracting

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        receipt = guided_remove._receipt_for(conn, deployment, "default", "alpha")
        footprint = guided_remove._footprint(conn, plan)
    assert receipt is not None and receipt["status"] == "awaiting_publish"
    assert footprint == {
        "records": 0, "relationships": 0,
        "published_records": 2, "published_relationships": 1,
    }

    resumed_plan = guided_remove.plan_remove_source(
        deployment=str(dep), name="alpha", environ=environ,
    )
    assert resumed_plan.action == guided_remove.PUBLISH_PENDING
    resumed = guided_remove.apply_remove_source(
        resumed_plan, runner=runner, environ=environ, okg_argv_prefix=["okg"],
    )
    assert resumed.status == "removed", resumed.as_dict()
    assert len(retractions) == 1  # never retracted a second time
    assert "alpha" not in (yaml.safe_load(registry.read_text())["sources"] or {})

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        receipt = guided_remove._receipt_for(conn, deployment, "default", "alpha")
        footprint = guided_remove._footprint(conn, plan)
    assert receipt["status"] == "published"
    assert receipt["repair_generation_id"] == resumed.generation_id
    assert footprint == {
        "records": 0, "relationships": 0,
        "published_records": 0, "published_relationships": 0,
    }


@pytest.mark.postgres
@pytest.mark.parametrize("when", ["before_publish", "after_publish"])
def test_remove_source_when_records_come_back(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    when: str,
) -> None:
    """Issue #2477: records an explicit import writes during a removal.

    `before_publish`: the record lands between the retraction and the
    publish, so the publish cannot bind the retirement. A re-run continues
    the SAME, still unpublished retirement (awaiting_publish), retracts the
    new record, publishes, and removes the entry.

    `after_publish`: the record lands after the publish bound the
    retirement but before the entry was removed, so the registry step
    refuses a live footprint. A published retirement is never reopened, so
    the re-run is refused (this version cannot retract them), and the
    receipt keeps its published binding unchanged.

    The writes, retraction, receipts and in-process publishes are real; the
    `okg` children (publish, claim) are faked as in the test above.
    """
    import json
    import subprocess

    import yaml
    from chronos_core.branching._common import clear_all_registry_caches

    from okg.substrate.errors import RemoveSourceError
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.mcp import source_admin
    from okg.substrate.publish_outcome import classify_publish
    from okg.substrate.publisher import Publisher
    from okg.substrate.sources import guided_remove

    deployment = f"remove-source-again-{when.replace('_', '-')}"
    dep = tmp_path / deployment
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump(
        {"name": deployment, "postgres": {"dsn": clean_runtime}},
    ))
    registry = dep / "source_registry.yaml"
    registry.write_text(yaml.safe_dump({"sources": {"alpha": {
        "ownership_id": "dep.alpha",
        "enabled_by_default": True,
        "admission_policy": {"producer_id": "dep.alpha"},
    }}}))
    monkeypatch.setattr(source_admin, "_deployment_dir", lambda _name, **_kw: dep)

    clear_all_registry_caches()
    clear_schema_ensured_cache()

    def write(node_id: str, record: int) -> None:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
                [NodeFact(node_id, "thing", {}, {"id": record}, {"v": 1})],
                branch="default",
                source="dep.alpha",
            )

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
    write("alpha:1", 1)

    def publish() -> dict:
        return Publisher(
            clean_runtime,
            deployment,
            enrichers=[],
            catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
        ).cycle()

    assert publish()["status"] == "published"
    runs: list[str] = []

    def runner(argv, **_kwargs):
        if "claim" in argv:
            return subprocess.CompletedProcess(argv, 0, "{}", "")
        assert "run" in argv and "--force-enrichers" not in argv, argv
        runs.append("run")
        if len(runs) == 1 and when == "before_publish":
            write("alpha:2", 2)  # an explicit import, after the retraction
        try:
            result = publish()
        except Exception as exc:  # noqa: BLE001 - the child would exit 1
            return subprocess.CompletedProcess(argv, 1, "", f"{type(exc).__name__}: {exc}")
        if len(runs) == 1 and when == "after_publish":
            write("alpha:2", 2)  # an explicit import, before the entry goes
        body = {"run_summary": {
            "outcome": classify_publish(result).kind,
            "generation_id": result.get("generation_id"),
        }}
        return subprocess.CompletedProcess(argv, 0, json.dumps(body), "")

    environ = {"PATH": "/usr/bin"}

    def plan():
        return guided_remove.plan_remove_source(
            deployment=str(dep), name="alpha", environ=environ,
        )

    def apply(planned):
        return guided_remove.apply_remove_source(
            planned, runner=runner, environ=environ, okg_argv_prefix=["okg"],
        )

    def receipts() -> list[tuple]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT retirement_id, status, repair_generation_id, published_at "
                "FROM okg.source_retirement_receipts WHERE deployment_name = %s",
                (deployment,),
            )
            return cur.fetchall()

    first = apply(plan())
    assert first.status == "failed", first.as_dict()
    assert "alpha" in yaml.safe_load(registry.read_text())["sources"]

    if when == "after_publish":
        assert first.as_dict()["failed_step"]["step"] == "registry"
        bound = receipts()
        assert len(bound) == 1 and bound[0][1] == "published"
        with pytest.raises(RemoveSourceError, match="cannot retract them") as caught:
            plan()
        assert "not reopened" in caught.value.message
        # The published binding is audit evidence: left exactly as it was.
        assert receipts() == bound
        assert "alpha" in yaml.safe_load(registry.read_text())["sources"]
        return

    assert first.as_dict()["failed_step"]["step"] == "publish"
    again = plan()
    assert again.action == guided_remove.RETIRE
    assert "still unpublished retirement" in (again.note or "")
    second = apply(again)
    assert second.status == "removed", second.as_dict()["failed_step"]
    assert "alpha" not in (yaml.safe_load(registry.read_text())["sources"] or {})

    rows = receipts()
    assert [(row[1], row[2]) for row in rows] == [("published", second.generation_id)]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        footprint = guided_remove._footprint(conn, again)
    assert footprint == {
        "records": 0, "relationships": 0,
        "published_records": 0, "published_relationships": 0,
    }
