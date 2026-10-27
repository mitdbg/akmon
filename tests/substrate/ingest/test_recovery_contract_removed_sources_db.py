"""A repair contract that names removed or renamed sources is retired safely.

PACT recovery-contract-removed-sources. A blocked ``repair_required``
checkpoint makes every later ingest repeat its exact source contract. When
the sources it holds back are then renamed or removed, no retry can match, and
before this change the only exit was a manual ``okg runtime
supersede-checkpoint``.

Fixture (neutral names): ``first`` <- ``second`` and an independent ``third``.
The freeze fails ``second`` and ``third`` with an unfinished undo, so the
checkpoint holds both back. ``second`` is then renamed to ``inventory`` (same
ownership id) and ``third`` is removed.

``test_twin_wedge_is_retired_by_migrate`` is the reproduction: on the base
commit every ingest is refused ``incomplete_recovery_contract_mismatch`` and
``migrate`` retires nothing, so it fails; with this change migrate retires the
contract and the next ingest runs.
"""
from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Callable

import psycopg
import pytest
import yaml

import okg.substrate.ingest.service as ingest_service
from okg.substrate.db.migrate import migrate
from okg.substrate.errors import SubstrateError
from okg.substrate.ingest import IngestOptions, run_ingest
from okg.substrate.ingest import incomplete_recovery as ir
from okg.substrate.ingest.run_base import finish_pending_undos
from okg.substrate.sources.registry import load_source_registry_admission
from tests.pytest.source_jobs_inline import use_inline_source_jobs
from tests.substrate import test_ingest_service as fixtures
from tests.substrate.ingest import test_publish_incomplete_generation as base


pytestmark = pytest.mark.postgres

# A test marked `spawns_source_jobs` keeps the production runtime (a spawned
# child per source) for its publishes; the rest prove retirement logic
# in-process.
@pytest.fixture(autouse=True)
def _inline_source_jobs(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if request.node.get_closest_marker("spawns_source_jobs") is None:
        use_inline_source_jobs(monkeypatch)


DEPLOYMENT = "svc"
FAILED = {"svc.second", "svc.third"}


def _edit_registry(dep: Path, dsn: str, edit: Callable[[dict], None]) -> None:
    path = dep / "source_registry.yaml"
    registry = yaml.safe_load(path.read_text())
    edit(registry["sources"])
    path.write_text(yaml.safe_dump(registry))
    fixtures._claim_layered_fixture_catalog(dsn, dep)
    fixtures._commit_layered_fixture_deployment(dep, message="edit the registry")


def _twin_freeze(
    tmp_path: Path,
    dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> IngestOptions:
    """Freeze a ``repair_required`` checkpoint holding back second and third.

    Both fail and neither undo finishes, so the run cuts the blocked freeze
    this code still writes.
    """
    dep, options = base._deployment(tmp_path, dsn)
    assert run_ingest(options).any_failed is False
    base._break_second(dep, dsn)
    _edit_registry(
        dep,
        dsn,
        lambda sources: sources["third"]["params"].update(
            fixture_path=str(dep / "missing-third.json"),
        ),
    )
    base._add_node(dep, "first.json", "svc:first:new")
    with monkeypatch.context() as patch:
        use_inline_source_jobs(patch)
        original_retract = ingest_service._retract_chronos_run

        def retract(*args, **kwargs):
            if kwargs.get("source") in FAILED:
                raise RuntimeError("injected compensation failure")
            return original_retract(*args, **kwargs)

        patch.setattr(ingest_service, "_retract_chronos_run", retract)
        partial = run_ingest(replace(
            options,
            include={"second", "third"},
            include_predecessors=True,
            reset_cursor=True,
        ))
    (blocked,) = partial.report["publishes"]
    assert blocked["status"] == "blocked"
    recovery = blocked["blocked_reason"]["recovery"]
    assert recovery["state"] == ir.REPAIR_REQUIRED
    assert recovery["required_sources"] == ["second", "third"]
    return options


def _rename_and_remove(dep: Path, dsn: str) -> None:
    """``second`` becomes ``inventory`` (same ownership id); ``third`` goes."""
    healthy = dep / "second.json"

    def edit(sources: dict) -> None:
        second = sources.pop("second")
        second["params"]["fixture_path"] = str(healthy)
        sources["inventory"] = second
        del sources["third"]

    _edit_registry(dep, dsn, edit)


def _entries(dep: Path):
    return load_source_registry_admission(
        dep / "source_registry.yaml", force_reload=True,
    ).entries


def _retire(
    dsn: str,
    dep: Path,
    *,
    actor: str = "okg migrate",
    apply: bool = True,
    deployment: str = DEPLOYMENT,
):
    return ir.retire_removed_source_contract(
        dsn=dsn,
        deployment=deployment,
        load_registry=lambda: (_entries(dep), dep),
        actor=actor,
        apply=apply,
    )


def _recovery(dsn: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT metadata::jsonb #> '{blocked_reason,recovery}', "
            "metadata::jsonb->>'status' "
            "FROM _chronos_branch_interval_checkpoints "
            "WHERE metadata::jsonb #>> '{blocked_reason,reason}' = %s "
            "ORDER BY created_at DESC LIMIT 1",
            (ir.BLOCKED_INGEST_REASON,),
        )
        recovery, status = cur.fetchone()
    assert status == "blocked", "a retirement never rewrites the status"
    return recovery


def _finish_undo(dsn: str) -> None:
    finish_pending_undos(dsn=dsn, deployment=DEPLOYMENT, apply=True)


def _unrelated(options: IngestOptions) -> IngestOptions:
    return replace(options, include={"first"}, include_predecessors=False)


# ---------------------------------------------------------------------------
# The reproduction: the twin wedge
# ---------------------------------------------------------------------------


def test_twin_wedge_is_retired_by_migrate(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    _rename_and_remove(options.deployment_dir, clean_runtime)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(options.deployment_dir.parent))

    with pytest.raises(SubstrateError) as wedged:
        run_ingest(_unrelated(options))
    assert wedged.value.data["reason"] in {
        "incomplete_recovery_contract_mismatch",
        "incomplete_recovery_contract_unsatisfiable",
    }

    migrated = migrate(clean_runtime, deployment_name=DEPLOYMENT)

    # The wedge on the base commit stops here: migrate retires nothing.
    reports = getattr(migrated, "source_contract_retirements", ())
    assert [item["status"] for item in reports] == ["retired"], reports
    (report,) = reports
    assert report["actor"] == "okg migrate"
    recovery = _recovery(clean_runtime)
    assert recovery["state"] == ir.SUPERSEDED
    assert recovery["superseded_by"] == "okg migrate"
    assert recovery["supersede_reason"] == (
        "contract_sources_removed: second, third"
    )
    retirement = recovery["contract_retirement"]
    assert {row["source"]: row["class"] for row in retirement["source_classes"]} == {
        "first": "present",
        "second": "renamed",
        "third": "removed",
    }
    assert retirement["registry_fingerprint"]
    assert len(retirement["deployment_revision"]) == 40

    healed = run_ingest(_unrelated(options))
    assert healed.any_failed is False, json.dumps(healed.report, default=str)
    assert healed.report["publishes"][-1]["status"] == "published"


@pytest.mark.spawns_source_jobs
def test_real_publish_proof_after_worker_start_retirement(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Freeze, rename and remove, retire as the worker, publish, and record it.

    Writes the proof to ``$RECOVERY_REAL_PUBLISH_OUT`` (default: the test's
    tmp dir). It records the checkpoint, the refusals before and after, the
    retirement report, the next generation with node and edge counts, and
    the source-sync queue and dead-letter depth. The doctor result and an
    MCP read are not measured here.
    """
    import os

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    _rename_and_remove(options.deployment_dir, clean_runtime)
    frozen = _recovery(clean_runtime)

    with pytest.raises(SubstrateError) as before:
        run_ingest(_unrelated(options))
    report = _retire(
        clean_runtime, options.deployment_dir, actor="okg runtime worker",
    )
    assert report["status"] == "retired", report
    after = run_ingest(_unrelated(options))
    assert after.any_failed is False, json.dumps(after.report, default=str)
    # This test keeps the real runtime: each source ran in its own child.
    assert after.report["results"]
    for result in after.report["results"]:
        assert result["source_process"]["pid"] != os.getpid(), result
    (published,) = [
        item for item in after.report["publishes"]
        if not item.get("publish_skipped")
    ]
    assert published["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest = store.latest_generation(
            deployment_name=DEPLOYMENT, status="published",
        )
        generation = store.checkout_generation(published["generation_id"])
        node_count = int(generation.query(
            "SELECT count(*) AS n FROM okg.graph_nodes",
        )[0]["n"])
        edge_count = int(generation.query(
            "SELECT count(*) AS n FROM okg.graph_edges",
        )[0]["n"])
        with conn.cursor() as cur:
            health = summarize_runtime_queue_health(cur)
    assert latest.checkpoint_id == published["generation_id"]
    proof = {
        "pact": "recovery-contract-removed-sources",
        "blocked_checkpoint_recovery_state_before": frozen["state"],
        "required_sources": frozen["required_sources"],
        "refusal_before_retirement": before.value.data["reason"],
        "retirement": {
            key: report.get(key)
            for key in (
                "status", "actor", "checkpoint_id", "retire_reason",
                "registry_fingerprint", "deployment_revision",
            )
        },
        "source_classes": report["source_classes"],
        "recovery_state_after": _recovery(clean_runtime)["state"],
        "published_generation": published["generation_id"],
        "node_count": node_count,
        "edge_count": edge_count,
        "source_sync_queue_depth": health["queue_depth"],
        "dlq_count": health["dlq_count"],
        "not_measured": ["doctor result", "one MCP read"],
    }
    out = Path(os.environ.get("RECOVERY_REAL_PUBLISH_OUT") or tmp_path / "proof.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(proof, indent=2, sort_keys=True, default=str) + "\n")
    assert proof["recovery_state_after"] == ir.SUPERSEDED
    assert proof["node_count"] > 0


def test_refusal_before_retirement_says_the_retry_is_hopeless(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    _rename_and_remove(options.deployment_dir, clean_runtime)
    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda *_a, **_k: pytest.fail("no source may run"),
    )

    with pytest.raises(SubstrateError) as refused:
        run_ingest(_unrelated(options))

    data = refused.value.data
    assert data["reason"] == "incomplete_recovery_contract_unsatisfiable"
    classes = {row["source"]: row for row in data["source_classes"]}
    assert classes["second"]["class"] == "renamed"
    assert classes["second"]["renamed_to"] == "inventory"
    assert classes["third"]["class"] == "removed"
    assert classes["first"]["class"] == "present"
    assert data["automatic_retirement_applies"] is True
    message = str(refused.value)
    assert "second was renamed to inventory" in message
    assert "third was removed" in message
    assert "okg migrate --deployment svc --apply" in refused.value.fix_hint


# ---------------------------------------------------------------------------
# Who retires, once
# ---------------------------------------------------------------------------


def test_worker_start_retires_with_worker_actor(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    _rename_and_remove(options.deployment_dir, clean_runtime)

    report = _retire(
        clean_runtime, options.deployment_dir, actor=ir.WORKER_ACTOR,
    )

    assert report["status"] == "retired"
    assert report["actor"] == "okg runtime worker"
    assert _recovery(clean_runtime)["superseded_by"] == "okg runtime worker"


def test_rerun_retires_nothing_more(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    _rename_and_remove(options.deployment_dir, clean_runtime)
    assert _retire(clean_runtime, options.deployment_dir)["status"] == "retired"
    before = _recovery(clean_runtime)

    assert _retire(clean_runtime, options.deployment_dir) is None
    assert _retire(
        clean_runtime, options.deployment_dir, actor=ir.WORKER_ACTOR,
    ) is None
    assert _recovery(clean_runtime) == before


def test_migrate_and_worker_race_reports_winner(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    _rename_and_remove(options.deployment_dir, clean_runtime)
    real = ir.supersede_incomplete_checkpoint

    def worker_lands_first(**kwargs):
        # The worker's write commits between migrate's read and its write.
        real(**{**kwargs, "actor": ir.WORKER_ACTOR, "record": None})
        return real(**kwargs)

    monkeypatch.setattr(ir, "supersede_incomplete_checkpoint", worker_lands_first)

    loser = _retire(clean_runtime, options.deployment_dir, actor=ir.MIGRATE_ACTOR)

    assert loser["status"] == "retired_by_other"
    assert loser["retired_by"] == "okg runtime worker"
    assert loser["retired_at"]
    assert loser["applied"] is False
    assert _recovery(clean_runtime)["superseded_by"] == "okg runtime worker"
    # The loser did not crash, and the deployment is no longer wedged.
    healed = run_ingest(_unrelated(options))
    assert healed.any_failed is False, json.dumps(healed.report, default=str)


# ---------------------------------------------------------------------------
# Nothing is retired when it is not safe
# ---------------------------------------------------------------------------


def _assert_still_held(dsn: str) -> None:
    assert _recovery(dsn)["state"] == ir.REPAIR_REQUIRED


def test_required_source_present_not_retired(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    # Only `third` goes; `second` still exists, unchanged.
    _edit_registry(
        options.deployment_dir, clean_runtime, lambda sources: sources.pop("third"),
    )

    report = _retire(clean_runtime, options.deployment_dir)

    assert report["status"] == "not_retired"
    assert report["reason"] == "required_source_present"
    _assert_still_held(clean_runtime)


def test_rebound_source_not_retired(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    dep = options.deployment_dir
    healthy = dep / "second.json"

    def edit(sources: dict) -> None:
        sources["second"]["params"]["fixture_path"] = str(healthy)  # rebound
        del sources["third"]

    _edit_registry(dep, clean_runtime, edit)

    report = _retire(clean_runtime, dep)

    assert report["status"] == "not_retired"
    assert report["reason"] == "required_source_rebound"
    second = {row["source"]: row for row in report["source_classes"]}["second"]
    assert second["class"] == "rebound"
    assert "catalog_fingerprint" in second["changed_fields"]
    _assert_still_held(clean_runtime)
    with pytest.raises(SubstrateError) as refused:
        run_ingest(_unrelated(options))
    assert refused.value.data["reason"] == (
        "incomplete_recovery_contract_unsatisfiable"
    )
    assert refused.value.data["automatic_retirement_applies"] is False
    assert "migrate --deployment" not in refused.value.fix_hint


def test_pending_undo_blocks_retirement(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest.run_base import take_run_base

    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    # A run of `second` that ended without an undo: its run base stays, and
    # the publisher's guard (`publish_block`) refuses every publish.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        assert take_run_base(
            store,
            deployment=DEPLOYMENT,
            branch="default",
            source="svc.second",
            run_id="run-ended-without-undo",
        ) is not None
    _rename_and_remove(options.deployment_dir, clean_runtime)

    report = _retire(clean_runtime, options.deployment_dir)

    assert report["status"] == "not_retired"
    assert report["reason"] == "incomplete_recovery_publish_blocked"
    assert report["publish_block"] == "source_run_undo_unfinished"
    assert report["sources"] == ["svc.second"]
    (step,) = report["clears_with"]
    assert step.endswith("runtime finish-undo --deployment svc --apply")
    _assert_still_held(clean_runtime)


def test_kept_rerun_of_removed_source_blocks_retirement(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.ingest.run_base import mark_rerun_required

    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    # What `finish-undo --accept-current` leaves behind for each source whose
    # partial rows the operator kept: a durable rerun-required row.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for source in ("svc.second", "svc.third"):
            mark_rerun_required(
                conn,
                deployment=DEPLOYMENT,
                source=source,
                run_id="kept-run",
                reason="keep the partial rows",
                actor="operator",
            )
    _rename_and_remove(options.deployment_dir, clean_runtime)

    report = _retire(clean_runtime, options.deployment_dir)

    assert report["status"] == "not_retired"
    assert report["reason"] == "incomplete_recovery_publish_blocked"
    assert report["publish_block"] == "source_rerun_required"
    assert report["sources"] == ["svc.second", "svc.third"]
    steps = " ".join(report["clears_with"])
    # `third` was removed: retire it. `second` was renamed to `inventory`, and
    # its ownership id now belongs to that live source, so retire-source
    # would delete its records: it reruns under the new name instead.
    assert "remediate retire-source --source svc.third" in steps
    assert "retire-source --source svc.second" not in steps
    assert "ingest --deployment svc --include inventory" in steps
    _assert_still_held(clean_runtime)


def test_copy_under_another_deployment_name_not_retired(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    _rename_and_remove(options.deployment_dir, clean_runtime)
    # The database is copied under another name: every checkpoint keeps the
    # old deployment name, and the caller is the new one.
    report = _retire(clean_runtime, options.deployment_dir, deployment="svc-copy")

    assert report["status"] == "not_retired"
    assert report["reason"] == "incomplete_recovery_deployment_renamed"
    assert report["recorded_deployment"] == "svc"
    assert "'svc-copy'" in report["message"] and "'svc'" in report["message"]
    _assert_still_held(clean_runtime)


def test_uncommitted_manifest_not_retired(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    dep = options.deployment_dir
    _rename_and_remove(dep, clean_runtime)
    (dep / "uncommitted-note.txt").write_text("not committed\n")

    report = _retire(clean_runtime, dep)

    assert report["status"] == "not_retired"
    assert report["reason"] == "manifest_uncommitted"
    assert "repository provenance inputs are not committed" in report["detail"]
    _assert_still_held(clean_runtime)


def test_migrate_without_deployment_reports_only(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    _rename_and_remove(options.deployment_dir, clean_runtime)

    migrated = migrate(clean_runtime)

    (candidate,) = migrated.source_contract_retirements
    assert candidate["status"] == "candidate"
    assert candidate["deployment"] == DEPLOYMENT
    assert candidate["required_sources"] == ["second", "third"]
    assert "migrate --deployment svc --apply" in candidate["message"]
    assert migrated.incomplete_checkpoint_retirements == ()
    _assert_still_held(clean_runtime)


# ---------------------------------------------------------------------------
# The doctor's hold report
# ---------------------------------------------------------------------------


def _hold(dsn: str, dep: Path):
    entries = _entries(dep)
    return ir.source_sync_hold(
        dsn=dsn,
        deployment=DEPLOYMENT,
        sources={name: entry.ownership_id for name, entry in entries.items()},
        registry=entries,
    )


def test_hold_report_lists_classes_and_says_retirement_applies(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _rename_and_remove(options.deployment_dir, clean_runtime)

    hold = _hold(clean_runtime, options.deployment_dir)

    classes = {row["source"]: row["class"] for row in hold["source_classes"]}
    assert classes == {
        "first": "present", "second": "renamed", "third": "removed",
    }
    assert hold["automatic_retirement_applies"] is True
    line = ir.source_sync_hold_line(hold)
    assert "second was renamed to inventory" in line
    assert "migrate --deployment svc --apply" in line


def test_hold_report_says_retirement_does_not_apply_while_a_source_exists(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _edit_registry(
        options.deployment_dir, clean_runtime, lambda sources: sources.pop("third"),
    )

    hold = _hold(clean_runtime, options.deployment_dir)

    classes = {row["source"]: row["class"] for row in hold["source_classes"]}
    assert classes["third"] == "removed"
    assert classes["second"] == "present"
    assert hold["automatic_retirement_applies"] is False
    # An all-present contract reports no change at all.
    assert "migrate --deployment" not in ir.source_sync_hold_line(hold)


# ---------------------------------------------------------------------------
# Worker start: the block run_worker runs, with the worker's own arguments
# ---------------------------------------------------------------------------


def _worker_start(dep: Path, dsn: str, monkeypatch: pytest.MonkeyPatch):
    """What run_worker passes: the resolved manifest and the source root."""
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.deployments.sync_root import (
        require_usable_source_configuration_authority,
    )
    from okg.substrate.runtime import worker

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(dep.parent))
    manifest = resolve_deployment(DEPLOYMENT)
    root = require_usable_source_configuration_authority(
        manifest.sync_root, command="source-sync worker startup",
    )
    return worker._retire_removed_source_contract_at_start(
        dsn=dsn,
        deployment=DEPLOYMENT,
        manifest=manifest,
        repo_root_override=root,
    )


def test_worker_start_block_retires_and_logs_at_info(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    _rename_and_remove(options.deployment_dir, clean_runtime)

    with caplog.at_level(logging.INFO):
        report = _worker_start(options.deployment_dir, clean_runtime, monkeypatch)

    assert report["status"] == "retired"
    assert report["actor"] == "okg runtime worker"
    assert _recovery(clean_runtime)["superseded_by"] == "okg runtime worker"
    started = [
        r for r in caplog.records if "worker start" in r.getMessage()
    ]
    assert [r.levelno for r in started] == [logging.INFO]
    # A second start finds nothing to retire and logs nothing.
    caplog.clear()
    assert _worker_start(options.deployment_dir, clean_runtime, monkeypatch) is None
    assert not caplog.records


def test_worker_start_logs_a_kept_contract_as_a_warning(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    options = _twin_freeze(tmp_path, clean_runtime, monkeypatch)
    _finish_undo(clean_runtime)
    # `second` still exists, so the hold is kept: a reason, not an error.
    _edit_registry(
        options.deployment_dir, clean_runtime, lambda sources: sources.pop("third"),
    )

    with caplog.at_level(logging.INFO):
        report = _worker_start(options.deployment_dir, clean_runtime, monkeypatch)

    assert report["reason"] == "required_source_present"
    started = [r for r in caplog.records if "worker start" in r.getMessage()]
    assert [r.levelno for r in started] == [logging.WARNING]
    _assert_still_held(clean_runtime)


def test_worker_start_survives_a_failing_retirement(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    from okg.substrate.runtime import worker

    def boom(**_kwargs):
        raise RuntimeError("database unreachable")

    monkeypatch.setattr(ir, "retire_removed_source_contract_for_deployment", boom)
    with caplog.at_level(logging.INFO):
        assert worker._retire_removed_source_contract_at_start(
            dsn=clean_runtime,
            deployment=DEPLOYMENT,
            manifest=None,
            repo_root_override=None,
        ) is None

    [record] = [r for r in caplog.records if "failed" in r.getMessage()]
    assert record.levelno == logging.ERROR
    assert record.exc_info and "database unreachable" in str(record.exc_info[1])
