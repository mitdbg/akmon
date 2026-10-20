"""canonical-dev-okg: `okg provision` one-command bootstrap (end-to-end).

Postgres-gated: provisions okg-workspace against a clean ephemeral runtime and
asserts it drives the deployment to a published generation with a clean
preflight. The from-truly-empty extension creation is covered separately by
tests/substrate/db/test_bootstrap_preflight.py (ensure_required_extensions).
"""
from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate.graph_store import ChronosGraphStore

pytestmark = pytest.mark.postgres

_CHECKOUT = Path(__file__).resolve().parents[3]


@pytest.fixture(autouse=True)
def _okg_workspace_sync_root(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point okg-workspace's declared ``sync_root`` at this checkout.

    The manifest declares ``sync_root: ${OKG_WORKSPACE_SYNC_ROOT}``, and
    provision refuses when that variable cannot be resolved, because it
    would not know which checkout the file-derived ``pact`` source reads.
    These tests ingest the ``pact`` source from the checkout they run in.
    """
    assert (_CHECKOUT / "deployments" / "okg-workspace" / "deployment.yaml").is_file()
    monkeypatch.setenv("OKG_WORKSPACE_SYNC_ROOT", str(_CHECKOUT))


def test_provision_drives_deployment_to_published_generation(
    okg_workspace_isolated_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # okg-workspace is self-hosted-authority + nomos-enforcing: the migrate
    # provenance guard refuses a file-derived migrate from a non-sync-root
    # checkout against a mismatched DB, and a fresh DB default-denies for lack
    # of an active Nomos bundle. The isolated-clone runtime fixture provisions
    # an `okg_workspace_<suffix>` DB (identity: isolated → allowed) with
    # OKG_DSN pointed at it and Nomos enforcement off, exercising the real
    # provision path off to the side without relaxing the (fail-closed) guard.
    rc = main([
        "provision", "--deployment", "okg-workspace",
        "--include", "pact", "--publish-once", "--json",
    ])
    out = json.loads(capsys.readouterr().out)

    assert rc == 0, out
    assert out["ok"] is True
    assert out["generation_id"] is not None
    assert out["preflight"]["ok"] is True
    assert out["preflight"]["summary"]["blocker_count"] == 0
    # the orchestration ran every step in order; a fresh deployment ends by
    # projecting the view definitions its catalog step deferred (#2249)
    assert [s["step"] for s in out["steps"]] == [
        "extensions", "migrate", "catalog", "ingest", "views_registry",
    ]
    ingest = next(s for s in out["steps"] if s["step"] == "ingest")
    assert ingest["any_failed"] is False
    views = next(s for s in out["steps"] if s["step"] == "views_registry")
    assert views["publish_status"] == "published"
    assert views["publish_generation_id"] == out["generation_id"]
    assert views["publish_generation_id"] != ingest["generation_id"]
    assert ingest["timing_status"] == "available"
    assert ingest["timings_ms"]["total"] >= 0
    assert ingest["layer_timings"]
    assert [source["name"] for source in ingest["source_timings"]] == ["pact"]
    assert ingest["source_timings"][0]["timing_status"] == "available"
    assert ingest["source_timings"][0]["timings_ms"]["total"] >= 0
    assert ingest["final_publish_timing"]["status"] == "published"
    assert ingest["final_publish_timing"]["wall_ms"] >= 0
    assert ingest["final_publish_timing"]["reported_ms"] >= 0
    assert ingest["final_publish_timing"]["timings_ms"]["total"] >= 0
    assert ingest["layer_timings"][-1]["index"] == "final"
    assert ingest["layer_timings"][-1]["publish_status"] == "published"


def test_provision_re_provision_is_idempotent(
    okg_workspace_isolated_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A second provision on an unchanged repo publishes nothing new (the source
    dedups) yet still succeeds, reporting the existing generation — no drift, no
    duplicated data. This is also the refresh contract: re-running provision is
    safe and current.

    Runs against an isolated okg-workspace clone under a permissive Nomos
    posture (see ``okg_workspace_isolated_runtime``)."""

    def _provision() -> dict:
        rc = main([
            "provision", "--deployment", "okg-workspace",
            "--include", "pact", "--json",
        ])
        out = json.loads(capsys.readouterr().out)
        assert rc == 0, out
        return out

    first = _provision()
    second = _provision()

    assert first["ok"] is True
    assert second["ok"] is True
    assert first["generation_id"] is not None
    assert second["generation_id"] is not None
    # Idempotent in the way that matters: the unchanged re-run adds NO new data
    # — either no publish at all, or a no-op publish (nodes_added == 0). No
    # drift, no duplicated nodes.
    second_ingest = next(s for s in second["steps"] if s["step"] == "ingest")
    assert second_ingest.get("nodes_added") in (0, None), second_ingest
    # #2249: the fresh provision projected every view definition the catalog
    # step deferred and published them, so the re-run has none left to add.
    first_views = next(s for s in first["steps"] if s["step"] == "views_registry")
    assert first_views["upserted"] == first_views["entries"] > 0, first_views
    assert first_views["nodes_added"] == first_views["entries"], first_views
    # okg.graph_nodes is read through a checkout of the published generation;
    # a bare SELECT on the table template returns no rows.
    with psycopg.connect(okg_workspace_isolated_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(first["generation_id"]).query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE subtype = 'view_definition'",
        )
    assert len(rows) == first_views["entries"], rows
    # The re-run's catalog step found a published generation and applied the
    # registry itself, so provision publishes nothing extra.
    assert "views_registry" not in [s["step"] for s in second["steps"]]
