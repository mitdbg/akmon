"""Issue #1431 — the orchestrated ingest path undoes a refused run.

The runner streams durable chunks (one flush every 100 progress markers or
10,000 facts), so a retraction the policy forbids can arrive after several
chunks are already committed.  This test drives the orchestrated
`_run_source` path — the one `okg ingest` uses — with an adapter that emits
more than one flush-sized insertion prefix before the forbidden retraction.

This is a CHARACTERIZATION test: it passes on the code as it stood before
issue #1431 was fixed, because `_run_source` already compensated a failed run
(`_retract_chronos_run` + `_restore_source_state`).  Nothing pinned that
guarantee, so it is pinned here.  The behavior the fix ADDS — the same
guarantee for callers that drive `SourceRunner` directly, which had no undo
at all — is proven by
tests/substrate/test_source_runner_retraction_atomicity.py.
"""
from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.ingest import service as ingest_service
from okg.substrate.ingest.service import IngestOptions
from okg.substrate.sources.registry import load_source_registry

pytestmark = pytest.mark.postgres


# The runner flushes every 100 progress markers, so 250 records commit two
# whole chunks before the retraction is ever seen.
PREFIX_RECORDS = 250

_ADAPTER_SOURCE = """
    from okg.substrate.library.sources.base import (
        NodeFact,
        ProgressMarker,
        SourceRun,
    )

    RECORDS = {records}


    class LateRetractionSource:
        name = "late-retraction"
        profile = "mutable_api"
        last_run_stats: dict = {{}}

        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def run(self, run_id, *, mode="cursor", cursor=None, sync_scope=None):
            retract = cursor is not None

            def facts():
                for index in range(RECORDS):
                    yield NodeFact(
                        node_id="t:%d" % index,
                        subtype="ticket",
                        attrs={{"text": "ticket %d" % index}},
                        source_record_id={{"id": "r-%d" % index}},
                        source_revision={{"v": 1}},
                    )
                    yield ProgressMarker(
                        record_id="r-%d" % index,
                        fingerprint="fp-%d" % index,
                    )
                if retract:
                    yield NodeFact(
                        node_id="t:gone",
                        subtype="ticket",
                        attrs={{"text": "gone"}},
                        source_record_id={{"id": "r-gone"}},
                        source_revision={{"v": 1}},
                        op="R",
                    )

            return SourceRun(
                facts=facts(),
                completed_scope=False,
                next_cursor={{"cursor": "1"}},
                run_mode=mode,
            )
"""


_MANIFEST = {
    "name": "default",
    "nomos": {
        "deployment_posture": {
            "deployment_class": "org_operational_private",
            "read_principals": ["human_operator", "okg_service"],
            "write_principals": [
                "human_operator", "okg_service", "source_adapter",
            ],
            "mcp_write_default": "review",
            "export_policy": "internal_only",
            "secret_handling": "secret_ref_required",
            "live_call_default": "review",
            "allowed_source_sensitivities": ["internal", "private"],
            "allowed_data_classifications": ["internal", "private"],
            "allowed_exportability": ["no_export"],
            "privacy_obligations": ["audit"],
            "readiness_profiles": ["org-private"],
        },
    },
}


def _registry_entry(tmp_path: Path, ownership_id: str):
    path = tmp_path / "source_registry.yaml"
    path.write_text(yaml.safe_dump({
        "sources": {
            "late": {
                "ownership_id": ownership_id,
                "source_class": "mutable_api",
                "record_identity_kind": "remote_id",
                "record_identity_fields": ["id"],
                "source_revision_kind": "updated_at",
                "deletion_semantics": "explicit_delete",
                "publication_mode": "published_generation",
                "source_policy": {
                    "store_raw": True,
                    "sensitivity": "internal",
                    "data_classification": "internal",
                    "credential_ref_policy": "none",
                    "live_call_allowed": False,
                    "exportability": "no_export",
                    "retention": "normal",
                    "provenance": "declared",
                    "privacy_obligations": ["audit"],
                },
                "module": "late_retraction_adapter",
                "class": "LateRetractionSource",
                "admission_policy": {
                    "producer_id": ownership_id,
                    "producer_kind": "source",
                    "trust_label": "fixture-trusted",
                    "admission_mode": "fast_track",
                    "output_scope_summary": {"summary": "fixture facts"},
                },
            },
        },
    }))
    return load_source_registry(path)["late"]


def _durable(dsn: str, source: str) -> dict[str, int]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.source_progress "
            "WHERE source_name = %s AND key LIKE 'record:%%'",
            (source,),
        )
        progress = cur.fetchone()[0]
        cur.execute(
            "SELECT state, count(*) FROM okg.fact_batch_receipts GROUP BY state",
        )
        receipts = {row[0]: row[1] for row in cur.fetchall()}

    graph = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = graph.checkout().query(
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE source = :source",
            {"source": source},
        )
    finally:
        graph.close()
    return {
        "progress": progress,
        "receipts": receipts,
        "graph_nodes": int(rows[0]["n"]) if rows else 0,
    }


def test_late_forbidden_retraction_leaves_no_applied_chunks(
    clean_runtime: str, tmp_path: Path, monkeypatch,
) -> None:
    module_dir = tmp_path / "adapters"
    module_dir.mkdir()
    (module_dir / "late_retraction_adapter.py").write_text(
        textwrap.dedent(_ADAPTER_SOURCE.format(records=PREFIX_RECORDS)),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(module_dir))
    sys.modules.pop("late_retraction_adapter", None)

    dep = tmp_path / "deployments" / "default"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(_MANIFEST), encoding="utf-8",
    )

    ownership_id = "svc.late-retraction"
    entry = _registry_entry(tmp_path, ownership_id)
    options = IngestOptions(
        dsn=clean_runtime,
        deployment="default",
        deployment_dir=dep,
        publish=False,
        run_mode="cursor",
    )

    first = ingest_service._run_source(options, entry, "late")
    assert first["status"] == "OK", first
    before = _durable(clean_runtime, ownership_id)
    # Not a vacuous comparison: the permitted first run really landed.
    assert before["graph_nodes"] == PREFIX_RECORDS, before
    assert before["progress"] == PREFIX_RECORDS, before

    second = ingest_service._run_source(options, entry, "late")
    assert second["status"] != "OK", second
    assert "retraction" in str(second.get("error", "")).lower(), second

    after = _durable(clean_runtime, ownership_id)
    assert after["graph_nodes"] == before["graph_nodes"], (
        "refused run left graph nodes from earlier flushes"
    )
    assert after["progress"] == before["progress"], (
        "refused run left progress markers from earlier flushes"
    )
    # The refused run applied more than one chunk before the retraction
    # surfaced; every one of those receipts must end `compensated`, and the
    # permitted run's receipts must be untouched.
    assert after["receipts"].get("applied") == before["receipts"].get("applied")
    compensated = after["receipts"].get("compensated", 0)
    assert compensated - before["receipts"].get("compensated", 0) >= 2, (
        f"refused run did not compensate its applied chunks: {after['receipts']}"
    )
    assert not after["receipts"].get("prepared"), after["receipts"]
    assert not after["receipts"].get("compensating"), after["receipts"]
