"""A blocked recovery checkpoint is never promoted over a run still writing.

PACT publisher-starvation-4039 (issue #4039). The run-base guard supersedes the
source-sync workflow-row yield wherever it can see a run, but it does not check
a ``blocked`` checkpoint when that checkpoint is cut. A blocked checkpoint is
never served to readers; its only path to them is the promotion of a frozen
recovery checkpoint (``incomplete_recovery.promote_blocked_freeze``), which
runs a full new publish. That publish takes the guard's top check under the
publish lock, cuts a ``published`` checkpoint with a previous generation (so
the cut recheck runs), and carries the frozen branch mutation token.

This pins the claim with a test instead of adding a guard: a promotion is
refused while another source run is live, and refused by the branch mutation
token after that run's undo, and no promoted generation holds the run's rows.
"""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
import yaml

import okg.substrate.ingest.service as ingest_service
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest.service import IngestOptions, run_ingest
from okg.substrate.library.sources.base import NodeFact
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.pre_3342_freeze import freeze_partial_failures
from tests.substrate.test_ingest_service import (
    _claim_layered_fixture_catalog,
    _commit_layered_fixture_deployment,
    _write_layered_fixture_deployment,
)

pytestmark = pytest.mark.postgres

RUN_ID = "run-other-source"
RUN_SOURCE = "svc.other"
RUN_ROW = "svc:other-run-row"


def test_a_frozen_promotion_is_refused_for_a_live_run_and_then_by_the_token(
    tmp_path: Path, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.ingest.compensation import retract_chronos_run
    from okg.substrate.ingest.run_base import (
        hold_run_liveness,
        run_base_checkpoint_id,
        take_run_base,
    )

    freeze_partial_failures(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(clean_runtime, dep)
    options = IngestOptions(dsn=clean_runtime, deployment="svc", deployment_dir=dep)
    baseline = run_ingest(options).report["publishes"][0]
    assert baseline["status"] == "published"
    baseline_generation = baseline["generation_id"]

    # A source breaks, and the next run leaves a repair-required checkpoint.
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    healthy_second = Path(registry["sources"]["second"]["params"]["fixture_path"])
    repaired_second = dep / "repair-second.json"
    second_payload = healthy_second.read_text()
    registry["sources"]["second"]["params"]["fixture_path"] = str(repaired_second)
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(clean_runtime, dep)
    (blocked,) = run_ingest(replace(options, reset_cursor=True)).report["publishes"]
    assert blocked["status"] == "blocked"
    repaired_second.write_text(second_payload)
    _commit_layered_fixture_deployment(dep, message="repair fixture source")

    # Another source's run is live when the repair run reaches its promotion.
    live = []
    promotions: list[dict] = []
    real_promote = ingest_service._promote_frozen_recovery

    def promote_beside_a_live_run(opts, *, redrive_state, recovery, **kwargs):
        promotions.append({"redrive_state": redrive_state, "recovery": recovery})
        checkpoint_id = run_base_checkpoint_id("default", RUN_ID)
        live.append(hold_run_liveness(clean_runtime, checkpoint_id))
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            assert take_run_base(
                store, deployment="svc", branch="default", source=RUN_SOURCE,
                run_id=RUN_ID,
            ) is not None
        return real_promote(
            opts, redrive_state=redrive_state, recovery=recovery, **kwargs,
        )

    monkeypatch.setattr(
        ingest_service, "_promote_frozen_recovery", promote_beside_a_live_run,
    )
    try:
        recovered = run_ingest(replace(options, reset_cursor=True))
        frozen, refused = recovered.report["publishes"]
        assert frozen["status"] == "blocked"
        assert frozen["blocked_reason"]["recovery"]["state"] == "frozen"
        assert refused["status"] == "fail"
        assert refused["code"] == "source_run_in_progress", refused
        assert RUN_ID in refused["error"]
        (attempt,) = promotions

        # The run writes a row, then fails and is undone to its run base.
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            ChronosGraphStore.from_connection(
                conn, ensure_schema=False,
            ).apply_facts(
                [NodeFact(
                    RUN_ROW, "document", {"text": "written by the other run"},
                    {"id": "row"}, {"v": 1},
                )],
                source=RUN_SOURCE, run_id=RUN_ID, strict_endpoints=False,
            )
        live.pop().close()
        stats = retract_chronos_run(
            clean_runtime, admission_id=None, source=RUN_SOURCE, run_id=RUN_ID,
        )
        assert stats["mode"] == "undo_to_run_base", stats

        # The same promotion, from the state read before the run, now meets a
        # branch the run wrote and unwrote: the token refuses it.
        monkeypatch.setattr(
            ingest_service, "_promote_frozen_recovery", real_promote,
        )
        second = real_promote(
            options,
            redrive_state=attempt["redrive_state"],
            recovery=attempt["recovery"],
        )
        assert second["status"] == "fail"
        assert second["error_data"]["reason"] == "branch_mutation_token_changed"
    finally:
        for conn in live:
            conn.close()

    # No generation promoted from this freeze holds the other run's rows, and
    # readers still see the baseline.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest = store.latest_generation(deployment_name="svc", status="published")
        assert latest is not None
        assert latest.checkpoint_id == baseline_generation
        for generation in store.list_generations(
            deployment_name="svc", status="published",
        ):
            rows = store.checkout_generation(generation.checkpoint_id).query(
                "SELECT node_id FROM okg.graph_nodes WHERE node_id = :n",
                {"n": RUN_ROW},
            )
            assert rows == [], generation.checkpoint_id
