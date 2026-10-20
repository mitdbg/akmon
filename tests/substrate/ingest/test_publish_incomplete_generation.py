"""A failed source in a group run no longer starves other sources (#3342).

A source-sync run takes a group of sources: the selected one plus its
predecessors and dependents. Before #3342, when one of them failed and a
sibling succeeded, okg cut a blocked checkpoint that demanded the next run
repeat exactly the same sources, so every other source's sync was refused.

Now the run publishes the siblings' writes as the new generation and records
the failed sources in the generation's ``ingest_completeness`` marker. The old
freeze remains only when a failed source's own undo did not finish.

Fixture: ``first`` (P) <- ``second`` (D, predecessor P) and ``third`` (U,
independent).
"""
from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
import yaml

import okg.substrate.ingest.service as ingest_service
from okg.substrate.errors import SubstrateError
from okg.substrate.ingest import IngestOptions, run_ingest
from okg.substrate.ingest import incomplete_recovery as ir
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.source_jobs_inline import use_inline_source_jobs
from tests.substrate import test_ingest_service as fixtures


pytestmark = pytest.mark.postgres


def _deployment(tmp_path: Path, dsn: str) -> tuple[Path, IngestOptions]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = fixtures._write_layered_fixture_deployment(tmp_path)
    third_fixture = dep / "third.json"
    third_fixture.write_text(json.dumps({
        "nodes": [{
            "node_id": "svc:third",
            "subtype": "document",
            "attrs": {"text": "independent source"},
            "source_record_id": {"path": "third.json"},
        }],
        "edges": [],
    }))
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["third"] = {
        **fixtures._source("svc.third"),
        "module": "okg.substrate.library.sources.bootstrap_fixture",
        "class": "BootstrapFixtureSource",
        "params": {"fixture_path": str(third_fixture)},
        "source_policy": fixtures._fixture_source_privacy_policy(),
        "sync": {
            "triggers": ["manual", "reconcile"],
            "default_event_mode": "scope_complete",
            "reconcile_mode": "scope_complete",
        },
    }
    registry_path.write_text(yaml.safe_dump(registry))
    fixtures._claim_layered_fixture_catalog(dsn, dep)
    return dep, IngestOptions(dsn=dsn, deployment="svc", deployment_dir=dep)


def _add_node(dep: Path, fixture: str, node_id: str) -> None:
    path = dep / fixture
    payload = json.loads(path.read_text())
    payload["nodes"].append({
        "node_id": node_id,
        "subtype": "document",
        "attrs": {"text": f"new output {node_id}"},
        "source_record_id": {"path": fixture, "record": node_id},
    })
    path.write_text(json.dumps(payload))
    # The publish refuses an uncommitted deployment tree.
    fixtures._commit_layered_fixture_deployment(dep, message=f"add {node_id}")


def _break_second(dep: Path, dsn: str) -> Path:
    """Point ``second`` at a missing fixture; return its healthy path."""
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    healthy = Path(registry["sources"]["second"]["params"]["fixture_path"])
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        dep / "missing-second.json",
    )
    registry_path.write_text(yaml.safe_dump(registry))
    fixtures._claim_layered_fixture_catalog(dsn, dep)
    return healthy


def _repair_second(dep: Path, dsn: str, healthy: Path) -> None:
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["second"]["params"]["fixture_path"] = str(healthy)
    registry_path.write_text(yaml.safe_dump(registry))
    fixtures._claim_layered_fixture_catalog(dsn, dep)


def _group_run(options: IngestOptions) -> IngestOptions:
    """What the worker runs for ``second``: it and its predecessors."""
    return replace(
        options,
        include={"second"},
        include_predecessors=True,
        reset_cursor=True,
    )


def _latest(dsn: str, *, status: str | None):
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        return ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).latest_generation(deployment_name="svc", status=status)


def _admission_status(dsn: str, admission_id: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM okg.admission_envelopes "
            "WHERE admission_id = %s::uuid",
            (admission_id,),
        )
        return str(cur.fetchone()[0])


def _completeness(dsn: str) -> dict | None:
    from okg.substrate.ingest.published_completeness import (
        published_generation_completeness,
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        return published_generation_completeness(conn, "svc")


def _results(outcome) -> dict[str, dict]:
    return {result["name"]: result for result in outcome.report["results"]}


def test_failed_group_does_not_refuse_unrelated_source_sync(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep, options = _deployment(tmp_path, clean_runtime)
    baseline = run_ingest(options)
    assert baseline.any_failed is False, json.dumps(baseline.report, default=str)
    _break_second(dep, clean_runtime)
    _add_node(dep, "first.json", "svc:first:new")

    partial = run_ingest(_group_run(options))
    results = _results(partial)
    assert results["first"]["status"] == "OK"
    assert results["second"]["status"] == "FAIL"
    after_partial = _latest(clean_runtime, status="published")

    _add_node(dep, "third.json", "svc:third:new")
    unrelated = run_ingest(replace(
        options, include={"third"}, include_predecessors=False,
    ))

    assert unrelated.any_failed is False, json.dumps(
        unrelated.report, default=str,
    )
    assert "incomplete_recovery" not in unrelated.report
    (third,) = unrelated.report["results"]
    assert third["status"] == "OK"
    assert third["nodes_appended"] >= 1
    assert unrelated.report["publishes"][-1]["status"] == "published"
    latest = _latest(clean_runtime, status="published")
    assert latest is not None and after_partial is not None
    assert latest.checkpoint_id != after_partial.checkpoint_id
    assert latest.checkpoint_id == unrelated.report["publishes"][-1][
        "generation_id"
    ]
    assert _admission_status(clean_runtime, third["admission_id"]) == "published"


def test_group_failure_leaves_no_blocking_checkpoint(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    dep, options = _deployment(tmp_path, clean_runtime)
    assert run_ingest(options).any_failed is False
    _break_second(dep, clean_runtime)
    _add_node(dep, "first.json", "svc:first:new")

    partial = run_ingest(_group_run(options))

    assert partial.any_failed is True
    assert [p["status"] for p in partial.report["publishes"]] == ["published"]
    assert "blocked_reason" not in partial.report["publishes"][0] or not (
        partial.report["publishes"][0]["blocked_reason"]
    )
    latest_any = _latest(clean_runtime, status=None)
    assert latest_any is not None
    assert latest_any.metadata.get("status") == "published"
    state = ir.staged_source_redrive_state(dsn=clean_runtime, deployment="svc")
    assert state["latest_checkpoint_status"] == "published"
    assert ir.recovery_from_redrive_state(state, deployment="svc") is None


def test_group_failure_publishes_sibling_with_incomplete_marker(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    dep, options = _deployment(tmp_path, clean_runtime)
    baseline = run_ingest(options)
    (baseline_publish,) = baseline.report["publishes"]
    assert _completeness(clean_runtime)["status"] == "complete"
    healthy = _break_second(dep, clean_runtime)
    _add_node(dep, "first.json", "svc:first:new")

    partial = run_ingest(_group_run(options))
    results = _results(partial)
    (published,) = partial.report["publishes"]

    assert published["status"] == "published"
    assert published["generation_id"] != baseline_publish["generation_id"]
    assert results["second"]["undo_status"] in {"completed", "nothing_written"}
    # The run's own receipt keeps its v1 shape.
    assert partial.report["completeness"]["schema"] == "okg.ingest_completeness.v1"
    assert partial.report["completeness"]["status"] == "incomplete"
    marker = _completeness(clean_runtime)
    assert marker == {
        "generation_id": published["generation_id"],
        "status": "incomplete",
        "reason": "ingest_source_incomplete",
        "failed_sources": [{
            "source": "second",
            "ownership_id": "svc.second",
            "stage": results["second"]["stage"],
        }],
        "dependency_blocked_sources": [],
    }
    assert _admission_status(
        clean_runtime, results["first"]["admission_id"],
    ) == "published"
    assert _admission_status(
        clean_runtime, results["second"]["admission_id"],
    ) == "failed"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        generation = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(published["generation_id"])
        visible = generation.query(
            "SELECT count(*) AS n FROM okg.graph_nodes "
            "WHERE node_id = 'svc:first:new'",
        )[0]["n"]
    assert int(visible) == 1

    # An unrelated source's publish carries the marker forward.
    _add_node(dep, "third.json", "svc:third:new")
    assert run_ingest(replace(
        options, include={"third"}, include_predecessors=False,
    )).any_failed is False
    carried = _completeness(clean_runtime)
    assert carried["status"] == "incomplete"
    assert [item["source"] for item in carried["failed_sources"]] == ["second"]

    # Once `second` runs successfully the marker clears.
    _repair_second(dep, clean_runtime, healthy)
    repaired = run_ingest(_group_run(options))
    assert repaired.any_failed is False, json.dumps(repaired.report, default=str)
    assert _completeness(clean_runtime)["status"] == "complete"


def test_zero_delta_partial_failure_records_marker_without_new_writes(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    """A failure with no sibling writes still names the failed source once."""
    dep, options = _deployment(tmp_path, clean_runtime)
    assert run_ingest(options).any_failed is False
    _break_second(dep, clean_runtime)

    first = run_ingest(_group_run(options))
    (published,) = [
        p for p in first.report["publishes"] if not p.get("publish_skipped")
    ]
    assert published["status"] == "published"
    assert _completeness(clean_runtime)["failed_sources"][0]["source"] == "second"

    # The same failure again changes nothing: no new generation is cut.
    again = run_ingest(_group_run(options))
    assert again.any_failed is True
    latest = _latest(clean_runtime, status="published")
    assert latest is not None
    assert latest.checkpoint_id == published["generation_id"]


def _unfinished_undo_freeze(
    tmp_path: Path,
    dsn: str,
    monkeypatch: pytest.MonkeyPatch,
):
    """Cut a freeze the way this code still does: compensation raises."""
    dep, options = _deployment(tmp_path, dsn)
    baseline = run_ingest(options)
    (baseline_publish,) = baseline.report["publishes"]
    _break_second(dep, dsn)
    _add_node(dep, "first.json", "svc:first:new")

    with monkeypatch.context() as patch:
        use_inline_source_jobs(patch)
        original_retract = ingest_service._retract_chronos_run

        def retract(*args, **kwargs):
            if kwargs.get("source") == "svc.second":
                raise RuntimeError("injected compensation failure")
            return original_retract(*args, **kwargs)

        patch.setattr(ingest_service, "_retract_chronos_run", retract)
        partial = run_ingest(_group_run(options))
    return options, baseline_publish, partial


def test_failed_source_undo_not_finished_still_fails_closed(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When compensation raises, the pre-#3342 freeze still applies."""
    options, baseline_publish, partial = _unfinished_undo_freeze(
        tmp_path, clean_runtime, monkeypatch,
    )
    results = _results(partial)

    assert results["second"]["undo_status"] == "unresolved"
    (blocked,) = partial.report["publishes"]
    assert blocked["status"] == "blocked"
    assert blocked["blocked_reason"]["freeze_policy"] == (
        "post_3342_undo_unfinished"
    )
    assert blocked["blocked_reason"]["unfinished_undo_sources"] == ["second"]
    assert blocked["blocked_reason"]["reason"] == "ingest_source_incomplete"
    assert blocked["blocked_reason"]["recovery"]["state"] == "repair_required"
    latest = _latest(clean_runtime, status="published")
    assert latest is not None
    assert latest.checkpoint_id == baseline_publish["generation_id"]
    assert _admission_status(
        clean_runtime, results["first"]["admission_id"],
    ) == "blocked"

    with pytest.raises(SubstrateError) as refused:
        run_ingest(replace(
            options, include={"third"}, include_predecessors=False,
        ))
    assert refused.value.data["reason"] == "incomplete_recovery_contract_mismatch"


def test_post_change_fail_closed_freeze_survives_migrate(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The upgrade retirement never drops a freeze this code cut on purpose."""
    from okg.substrate.db.migrate import migrate
    from okg.substrate.ingest import published_completeness as pc

    options, _baseline, partial = _unfinished_undo_freeze(
        tmp_path, clean_runtime, monkeypatch,
    )
    (blocked,) = partial.report["publishes"]
    assert blocked["status"] == "blocked"

    assert pc.retire_pre_publish_incomplete_checkpoints(
        clean_runtime, apply=False,
    ) == []
    assert migrate(clean_runtime).incomplete_checkpoint_retirements == ()

    state = ir.staged_source_redrive_state(dsn=clean_runtime, deployment="svc")
    assert state["latest_checkpoint_id"] == blocked["generation_id"]
    assert ir.recovery_from_redrive_state(state, deployment="svc")["state"] == (
        ir.REPAIR_REQUIRED
    )
    with pytest.raises(SubstrateError) as refused:
        run_ingest(replace(
            options, include={"third"}, include_predecessors=False,
        ))
    assert refused.value.data["reason"] == "incomplete_recovery_contract_mismatch"


def test_failed_result_without_undo_status_fails_closed() -> None:
    from okg.substrate.ingest import published_completeness as pc

    assert pc.partial_failure_may_publish([
        {"name": "a", "status": "OK"},
        {"name": "b", "status": "FAIL", "undo_status": "completed"},
        {"name": "c", "status": "FAIL", "undo_status": "nothing_written"},
    ]) is True
    assert pc.unfinished_undo_sources([
        {"name": "b", "status": "FAIL"},
        {"name": "c", "status": "FAIL", "undo_status": "unresolved"},
    ]) == ["b", "c"]


def test_marker_merge_carries_replaces_and_clears() -> None:
    from okg.substrate.ingest import published_completeness as pc

    from types import SimpleNamespace

    owners = {
        name: SimpleNamespace(ownership_id=f"o.{name}") for name in "abc"
    }
    failed_b = pc.run_marker(
        {
            "completed_sources": ["a"],
            "failed_sources": [{"source": "b", "stage": "run"}],
            "dependency_blocked_sources": [
                {"source": "c", "blocked_by": ["b"]},
            ],
        },
        owners,
    )
    first = pc.merge_marker(None, failed_b)
    assert first["status"] == "incomplete"
    assert first["failed_sources"] == [
        {"source": "b", "ownership_id": "o.b", "stage": "run"},
    ]
    assert first["dependency_blocked_sources"] == [
        {"source": "c", "ownership_id": "o.c", "blocked_by": ["b"]},
    ]
    # A publish that is not an ingest run carries the marker unchanged.
    assert pc.merge_marker(first, None) == first
    assert pc.marker_changes(first, pc.merge_marker(first, None)) is False
    # A run of an unrelated source keeps both entries.
    other = pc.run_marker({"completed_sources": ["a"]}, owners)
    assert pc.merge_marker(first, other) == first
    # `b` and `c` complete: the marker is complete again.
    cleared = pc.merge_marker(
        first, pc.run_marker({"completed_sources": ["b", "c"]}, owners),
    )
    assert cleared["status"] == "complete"
    assert cleared["failed_sources"] == []
    assert pc.marker_changes(first, cleared) is True
    # No marker reads as complete, so a clean first run changes nothing.
    assert pc.marker_changes(None, cleared) is False


def test_marker_drops_sources_no_longer_registered() -> None:
    """A removed or re-owned source cannot stay listed forever."""
    from types import SimpleNamespace

    from okg.substrate.ingest import published_completeness as pc

    def entry(owner: str) -> SimpleNamespace:
        return SimpleNamespace(ownership_id=owner)

    registry = {"a": entry("o.a"), "b": entry("o.b"), "c": entry("o.c")}
    first = pc.merge_marker(None, pc.run_marker(
        {
            "completed_sources": ["a"],
            "failed_sources": [
                {"source": "b", "stage": "run"},
                {"source": "c", "stage": "run"},
            ],
        },
        registry,
        registry,
    ))
    assert [item["source"] for item in first["failed_sources"]] == ["b", "c"]

    # `b` leaves the registry and `c` is re-owned; a run of `a` drops both.
    later = {"a": entry("o.a"), "c": entry("o.c2")}
    merged = pc.merge_marker(first, pc.run_marker(
        {"completed_sources": ["a"]}, {"a": later["a"]}, later,
    ))
    assert merged["status"] == "complete"
    assert merged["failed_sources"] == []
    # Without the registry (a publish that is not an ingest run) both stay.
    assert pc.merge_marker(first, None) == first


def test_migrate_reports_a_retirement_failure_as_a_substrate_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.db import migrate as migrate_module
    from okg.substrate.ingest import published_completeness as pc

    seen: list[object] = []
    monkeypatch.setattr(
        migrate_module, "_migrate_schema", lambda dsn, **_kw: "schema-result",
    )

    def boom(dsn, deployments=None, **_kw):
        seen.append(deployments)
        raise RuntimeError("connection reset")

    monkeypatch.setattr(pc, "retire_pre_publish_incomplete_checkpoints", boom)
    with pytest.raises(SubstrateError) as failed:
        migrate_module.migrate("postgresql://unused", deployment_name="svc")
    assert failed.value.data["schema_migrated"] is True
    assert failed.value.data["error_type"] == "RuntimeError"
    # `--deployment` scopes the retirement.
    assert seen == [["svc"]]


def test_a_checkpoint_that_moves_during_retirement_is_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextlib import nullcontext

    from okg.substrate.db import connection
    from okg.substrate.ingest import published_completeness as pc

    monkeypatch.setattr(connection, "connect", lambda *_a, **_k: nullcontext())
    monkeypatch.setattr(
        pc, "_latest_incomplete_freeze",
        lambda _conn, *, deployment: ("ckpt-1", ir.REPAIR_REQUIRED),
    )

    def drift(**_kw):
        raise SubstrateError(
            "moved",
            data={"reason": "incomplete_recovery_supersede_checkpoint_drift"},
        )

    monkeypatch.setattr(ir, "supersede_incomplete_checkpoint", drift)
    assert pc.retire_pre_publish_incomplete_checkpoints(
        "postgresql://unused", ["svc"],
    ) == [{
        "deployment": "svc",
        "checkpoint_id": "ckpt-1",
        "retired_state": ir.REPAIR_REQUIRED,
        "applied": False,
        "skipped": "incomplete_recovery_supersede_checkpoint_drift",
    }]


def _seed_pre_3342_freeze(
    tmp_path: Path,
    dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[IngestOptions, dict]:
    """Cut the blocked checkpoint pre-#3342 code cut for a partial failure.

    The freeze path is unchanged production code (an unfinished undo still
    takes it); forcing the publish decision off reproduces exactly what the
    old rule wrote.
    """
    from okg.substrate.ingest import published_completeness as pc

    dep, options = _deployment(tmp_path, dsn)
    assert run_ingest(options).any_failed is False
    _break_second(dep, dsn)
    _add_node(dep, "first.json", "svc:first:new")
    with monkeypatch.context() as patch:
        patch.setattr(pc, "partial_failure_may_publish", lambda _results: False)
        (blocked,) = run_ingest(_group_run(options)).report["publishes"]
    assert blocked["status"] == "blocked"
    assert blocked["blocked_reason"]["recovery"]["state"] == "repair_required"
    return options, blocked


def test_upgrade_supersedes_existing_repair_required_checkpoint(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.db.migrate import migrate
    from okg.substrate.ingest import published_completeness as pc

    options, blocked = _seed_pre_3342_freeze(tmp_path, clean_runtime, monkeypatch)

    planned = pc.retire_pre_publish_incomplete_checkpoints(
        clean_runtime, apply=False,
    )
    assert [(r["checkpoint_id"], r["applied"]) for r in planned] == [
        (blocked["generation_id"], False),
    ]
    state = ir.staged_source_redrive_state(dsn=clean_runtime, deployment="svc")
    assert ir.recovery_from_redrive_state(state, deployment="svc")["state"] == (
        ir.REPAIR_REQUIRED
    )

    migrated = migrate(clean_runtime)

    (retired,) = migrated.incomplete_checkpoint_retirements
    assert retired["deployment"] == "svc"
    assert retired["checkpoint_id"] == blocked["generation_id"]
    assert retired["applied"] is True
    assert retired["retired_state"] == ir.REPAIR_REQUIRED
    assert retired["reason"] == pc.UPGRADE_SUPERSEDE_REASON
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "SELECT metadata::jsonb #> '{blocked_reason,recovery}', "
            "metadata::jsonb->>'status' "
            "FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = %s",
            (blocked["generation_id"],),
        )
        recovery, status = cur.fetchone()
    assert status == "blocked"
    assert recovery["state"] == ir.SUPERSEDED
    assert recovery["superseded_state"] == ir.REPAIR_REQUIRED
    assert recovery["superseded_by"] == pc.UPGRADE_SUPERSEDE_ACTOR
    assert recovery["supersede_reason"] == pc.UPGRADE_SUPERSEDE_REASON
    assert recovery["superseded_at"]

    # Idempotent: a second upgrade retires nothing.
    assert pc.retire_pre_publish_incomplete_checkpoints(clean_runtime) == []
    assert migrate(clean_runtime).incomplete_checkpoint_retirements == ()

    # The unrelated source syncs again.
    unrelated = run_ingest(replace(
        options, include={"third"}, include_predecessors=False,
    ))
    assert unrelated.any_failed is False, json.dumps(
        unrelated.report, default=str,
    )
    assert unrelated.report["publishes"][-1]["status"] == "published"


def test_upgrade_leaves_a_frozen_checkpoint_to_its_promotion(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A `frozen` checkpoint is promoted by the next run, so it is not retired."""
    from okg.substrate.db.migrate import migrate

    _options, blocked = _seed_pre_3342_freeze(tmp_path, clean_runtime, monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE _chronos_branch_interval_checkpoints SET metadata = "
            "jsonb_set(metadata::jsonb, '{blocked_reason,recovery,state}', "
            "'\"frozen\"')::text WHERE checkpoint_id = %s",
            (blocked["generation_id"],),
        )

    assert migrate(clean_runtime).incomplete_checkpoint_retirements == ()
    state = ir.staged_source_redrive_state(dsn=clean_runtime, deployment="svc")
    assert ir.recovery_from_redrive_state(state, deployment="svc")["state"] == (
        ir.FROZEN
    )


def _hold(dsn: str):
    return ir.source_sync_hold(
        dsn=dsn,
        deployment="svc",
        sources={"first": "svc.first", "second": "svc.second", "third": "svc.third"},
    )


def test_repair_run_whose_undo_finished_publishes_incomplete(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After a stamped freeze, a repair run that fails cleanly ends the hold.

    Stamped freeze, then finish-undo, then the exact repair run: `second`
    fails again but its undo completes. The run publishes the siblings and
    the marker instead of freezing again, and other sources sync.
    """
    from okg.substrate.ingest.run_base import finish_pending_undos

    options, _baseline, partial = _unfinished_undo_freeze(
        tmp_path, clean_runtime, monkeypatch,
    )
    (frozen_first,) = partial.report["publishes"]
    assert frozen_first["blocked_reason"]["freeze_policy"] == (
        "post_3342_undo_unfinished"
    )
    stranded = _results(partial)["first"]["admission_id"]
    finish_pending_undos(dsn=clean_runtime, deployment="svc", apply=True)
    # `first` has new output in the repair run too, so its admission carries
    # record refs (a zero-output admission has none to bind to a version).
    _add_node(options.deployment_dir, "first.json", "svc:first:repair")

    repair = run_ingest(_group_run(options))

    results = _results(repair)
    assert results["second"]["status"] == "FAIL"
    assert results["second"]["undo_status"] in {"completed", "nothing_written"}
    assert repair.report["publishes"][-1]["status"] == "published", (
        repair.report["publishes"]
    )
    latest_any = _latest(clean_runtime, status=None)
    assert latest_any.metadata.get("status") == "published"
    marker = _completeness(clean_runtime)
    assert marker["generation_id"] == latest_any.checkpoint_id
    assert marker["status"] == "incomplete"
    assert [item["source"] for item in marker["failed_sources"]] == ["second"]
    state = ir.staged_source_redrive_state(dsn=clean_runtime, deployment="svc")
    assert ir.recovery_from_redrive_state(state, deployment="svc") is None
    assert _hold(clean_runtime) is None
    # Both runs' sibling admissions are published.
    assert _admission_status(clean_runtime, stranded) == "published"
    assert _admission_status(
        clean_runtime, results["first"]["admission_id"],
    ) == "published"

    _add_node(options.deployment_dir, "third.json", "svc:third:new")
    unrelated = run_ingest(replace(
        options, include={"third"}, include_predecessors=False,
    ))
    assert unrelated.any_failed is False, json.dumps(
        unrelated.report, default=str,
    )
    assert unrelated.report["publishes"][-1]["status"] == "published"


def test_repair_run_with_an_unfinished_undo_still_fails_closed(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options, baseline_publish, _partial = _unfinished_undo_freeze(
        tmp_path, clean_runtime, monkeypatch,
    )

    with monkeypatch.context() as patch:
        use_inline_source_jobs(patch)
        original_retract = ingest_service._retract_chronos_run

        def retract(*args, **kwargs):
            if kwargs.get("source") == "svc.second":
                raise RuntimeError("injected compensation failure again")
            return original_retract(*args, **kwargs)

        patch.setattr(ingest_service, "_retract_chronos_run", retract)
        repair = run_ingest(_group_run(options))

    (blocked,) = repair.report["publishes"]
    assert blocked["status"] == "blocked"
    assert blocked["blocked_reason"]["recovery"]["state"] == "repair_required"
    assert blocked["blocked_reason"]["freeze_policy"] == (
        "post_3342_undo_unfinished"
    )
    latest = _latest(clean_runtime, status="published")
    assert latest.checkpoint_id == baseline_publish["generation_id"]
    with pytest.raises(SubstrateError) as refused:
        run_ingest(replace(
            options, include={"third"}, include_predecessors=False,
        ))
    assert refused.value.data["reason"] == "incomplete_recovery_contract_mismatch"


# --- #3375: a repair run whose own promotion is refused ---------------------


def _generation_marker(dsn: str, checkpoint_id: str) -> dict | None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT metadata::jsonb -> 'ingest_completeness' "
            "FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = %s",
            (checkpoint_id,),
        )
        return cur.fetchone()[0]


def _move_fixture(dep: Path, fixture: str, target: str) -> None:
    """Remove or restore a source's input file without touching its registry
    entry, so the exact repair contract (registry fingerprints) still holds."""
    (dep / fixture).rename(dep / target)
    fixtures._commit_layered_fixture_deployment(
        dep, message=f"move {fixture} to {target}",
    )


def _refused_repair_then_next_run(
    dep: Path,
    options: IngestOptions,
    dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    *,
    before_repair,
):
    """Stamped freeze, finish-undo, then a repair run whose promotion is refused.

    The refusal is a real publisher refusal: the in-run promotion is handed a
    branch mutation token that no longer matches, as when a concurrent
    publish moved the head. The frozen checkpoint stays latest; an unrelated
    run then promotes it before its own source writes. Returns the repair
    run, the next run, and the promoted generation id.
    """
    from okg.substrate.ingest.run_base import finish_pending_undos

    _add_node(dep, "first.json", "svc:first:new")
    with monkeypatch.context() as patch:
        use_inline_source_jobs(patch)
        original_retract = ingest_service._retract_chronos_run

        def retract(*args, **kwargs):
            if kwargs.get("source") == "svc.second":
                raise RuntimeError("injected compensation failure")
            return original_retract(*args, **kwargs)

        patch.setattr(ingest_service, "_retract_chronos_run", retract)
        frozen = run_ingest(_group_run(options))
    assert frozen.report["publishes"][-1]["blocked_reason"]["recovery"][
        "state"
    ] == ir.REPAIR_REQUIRED
    finish_pending_undos(dsn=dsn, deployment="svc", apply=True)
    before_repair()
    _add_node(dep, "first.json", "svc:first:repair")

    with monkeypatch.context() as patch:
        original_kwargs = ir.promotion_publish_kwargs

        def stale_head(**kwargs):
            guarded = dict(original_kwargs(**kwargs))
            guarded["expected_branch_mutation_token"] = "moved-by-a-concurrent-publish"
            return guarded

        patch.setattr(ir, "promotion_publish_kwargs", stale_head)
        repair = run_ingest(_group_run(options))

    assert repair.report["incomplete_recovery"]["status"] == "promotion_refused", (
        repair.report.get("incomplete_recovery")
    )
    state = ir.staged_source_redrive_state(dsn=dsn, deployment="svc")
    assert state["latest_checkpoint_id"] == (
        repair.report["incomplete_recovery"]["frozen_checkpoint_id"]
    )
    assert ir.recovery_from_redrive_state(state, deployment="svc")["state"] == (
        ir.FROZEN
    )

    _add_node(dep, "third.json", "svc:third:new")
    following = run_ingest(replace(
        options, include={"third"}, include_predecessors=False,
    ))
    assert following.any_failed is False, json.dumps(
        following.report, default=str,
    )
    assert following.report["incomplete_recovery"]["status"] == "promoted"
    promoted = following.report["incomplete_recovery"]["published_checkpoint_id"]
    return repair, following, promoted


def test_deferred_promotion_keeps_the_repair_runs_failure(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3375: the next run's promotion records what the repair run failed."""
    dep, options = _deployment(tmp_path, clean_runtime)
    assert run_ingest(options).any_failed is False
    _break_second(dep, clean_runtime)

    repair, _following, promoted = _refused_repair_then_next_run(
        dep, options, clean_runtime, monkeypatch, before_repair=lambda: None,
    )

    repair_second = _results(repair)["second"]
    assert repair_second["status"] == "FAIL"
    assert repair_second["undo_status"] in {"completed", "nothing_written"}
    marker = _generation_marker(clean_runtime, promoted)
    assert marker is not None
    assert marker["status"] == "incomplete"
    assert marker["failed_sources"] == [{
        "source": "second",
        "ownership_id": "svc.second",
        "stage": repair_second["stage"],
    }]
    assert marker["dependency_blocked_sources"] == []
    # The unrelated run that published after the promotion carries it.
    latest = _completeness(clean_runtime)
    assert latest["generation_id"] != promoted
    assert latest["status"] == "incomplete"
    assert [item["source"] for item in latest["failed_sources"]] == ["second"]


def test_deferred_promotion_of_a_complete_repair_clears_the_marker(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3375: a complete frozen receipt promoted later still reads complete.

    The published marker names `second` as failed before the repair, so a
    promotion that only carried the old marker forward would stay incomplete.
    """
    dep, options = _deployment(tmp_path, clean_runtime)
    assert run_ingest(options).any_failed is False
    _move_fixture(dep, "second.json", "second.json.away")
    _add_node(dep, "first.json", "svc:first:prior")
    prior = run_ingest(_group_run(options))
    assert prior.report["publishes"][-1]["status"] == "published"
    assert [
        item["source"] for item in _completeness(clean_runtime)["failed_sources"]
    ] == ["second"]

    repair, _following, promoted = _refused_repair_then_next_run(
        dep, options, clean_runtime, monkeypatch,
        before_repair=lambda: _move_fixture(dep, "second.json.away", "second.json"),
    )

    assert repair.any_failed is True  # its promotion was refused
    assert _results(repair)["second"]["status"] == "OK"
    assert repair.report["completeness"]["status"] == "complete"
    marker = _generation_marker(clean_runtime, promoted)
    assert marker is not None
    assert marker["status"] == "complete"
    assert marker["failed_sources"] == []
    assert marker["dependency_blocked_sources"] == []
    assert _completeness(clean_runtime)["status"] == "complete"


def test_deferred_run_marker_rebuilds_the_in_run_marker() -> None:
    """The frozen receipt alone rebuilds the marker the refused promotion
    would have published, held-back sources included."""
    from types import SimpleNamespace

    from okg.substrate.ingest import published_completeness as pc

    runnable = {
        name: SimpleNamespace(ownership_id=f"svc.{name}")
        for name in ("first", "second", "fourth")
    }
    registry = {**runnable, "third": SimpleNamespace(ownership_id="svc.third")}
    completeness = {
        "schema": pc.RUN_SCHEMA,
        "status": "incomplete",
        "reason": pc.INCOMPLETE_REASON,
        "selected_sources": ["first", "second", "fourth"],
        "completed_sources": ["first"],
        "failed_sources": [{"source": "second", "stage": "extract", "code": "E1"}],
        "dependency_blocked_sources": [
            {"source": "fourth", "blocked_by": ["second"]},
        ],
    }
    receipt = ir.blocker_receipt(
        completeness=completeness,
        state=ir.FROZEN,
        predecessor_checkpoint_id="gen-1",
        source_contract=[
            {"source": name, "ownership_id": entry.ownership_id}
            for name, entry in runnable.items()
        ],
        admission_ids=[],
    )

    rebuilt = pc.deferred_run_marker(receipt, registry)

    assert rebuilt == pc.run_marker(completeness, runnable, registry)
    merged = pc.merge_marker(None, rebuilt)
    assert merged["status"] == "incomplete"
    assert merged["failed_sources"] == [{
        "source": "second", "ownership_id": "svc.second",
        "stage": "extract", "code": "E1",
    }]
    assert merged["dependency_blocked_sources"] == [{
        "source": "fourth", "ownership_id": "svc.fourth",
        "blocked_by": ["second"],
    }]
    # A complete frozen receipt clears earlier entries for its sources.
    complete = ir.blocker_receipt(
        completeness={
            **completeness,
            "status": "complete",
            "reason": None,
            "completed_sources": ["first", "second", "fourth"],
            "failed_sources": [],
            "dependency_blocked_sources": [],
        },
        state=ir.FROZEN,
        predecessor_checkpoint_id="gen-1",
        source_contract=[
            {"source": name, "ownership_id": entry.ownership_id}
            for name, entry in runnable.items()
        ],
        admission_ids=[],
    )
    assert pc.merge_marker(merged, pc.deferred_run_marker(complete, registry))[
        "status"
    ] == "complete"
    # A receipt that is not a run completeness receipt adds nothing.
    assert pc.deferred_run_marker({"reason": "other"}, registry) is None
