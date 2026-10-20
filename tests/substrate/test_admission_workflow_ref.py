from __future__ import annotations

import sys
from types import SimpleNamespace

from okg.substrate.admission import workflow_ref_from_dbos


def test_workflow_ref_from_dbos_does_not_import_dbos(monkeypatch) -> None:
    monkeypatch.delitem(sys.modules, "dbos", raising=False)

    ref = workflow_ref_from_dbos("okg_source_sync")

    assert ref == {"workflow_name": "okg_source_sync"}
    assert "dbos" not in sys.modules


def test_workflow_ref_from_dbos_reads_loaded_dbos(monkeypatch) -> None:
    monkeypatch.setitem(
        sys.modules,
        "dbos",
        SimpleNamespace(
            DBOS=SimpleNamespace(workflow_id="wf-1", step_id="step-2"),
        ),
    )

    assert workflow_ref_from_dbos("okg_source_sync") == {
        "workflow_name": "okg_source_sync",
        "workflow_id": "wf-1",
        "step_id": "step-2",
    }
