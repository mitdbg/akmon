"""The console reads behind the 2026-09-23 wording round (Lens A audit).

No database: each read is decided by a recording connection or a stubbed
maintained read. The live reads are exercised in
``test_console_wording_reads_db.py``.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import psycopg
import pytest

from okg.substrate.console_views import attempt_detail, records
from okg.substrate.console_views.ontology_rollup_read import (
    OntologyBucketRow,
    OntologyRollup,
    OntologyRollupUnavailable,
)
from okg.substrate.console_views.producers import _adapt_run, _source_row
from okg.substrate.console_views.source_diagnostics import (
    _recovery_summary,
    _workflow_summary,
)


# --- A1 / A9: a run row says which phase it names and what it wrote --------

def test_a_run_serves_its_slowest_phase_and_no_invented_last_phase() -> None:
    run = _adapt_run({
        "status": "OK", "observed_at": "2026-09-20T21:16:39+00:00",
        "input_count": 0, "output_count": 1263, "total_ms": 4440.3,
        "slowest_phase": "source_runner.flush.chronos_apply",
        "nodes_appended": 571, "edges_appended": 692, "run_id": "r1",
    })
    assert run["slowest_phase"] == "source_runner.flush.chronos_apply"
    assert run["phase"] == run["slowest_phase"]
    assert run["last_phase"] is None
    assert run["last_phase_unavailable_reason"] == "run_timings_record_no_phase_order"
    assert (run["records_written"], run["relationships_written"]) == (571, 692)
    # A pulled run's `input_count` is pushed events, not items read.
    assert run["items_in"] is None
    assert run["source_events_in"] == 0


def test_a_run_row_without_write_columns_serves_null_not_zero() -> None:
    run = _adapt_run({"status": "completed", "run_id": "e1"})
    assert run["records_written"] is None
    assert run["relationships_written"] is None


# --- A3: the way back, as the convergence read records it -------------------

def test_a_recorded_recovery_command_reaches_the_producer_row() -> None:
    command = ("env -u OKG_DSN uv run okg ingest --deployment d "
               "--include docsite --mode scope_complete --enqueue --json")
    workflow = _workflow_summary({"convergence": {
        "state": "blocked", "reason": "completed_scope_watermark_stale",
        "recovery_action": {"action_kind": "completed_scope_reconcile",
                            "command": command},
    }})
    row = _source_row({"name": "docsite", "source_key": "d.docsite",
                       "in_registry": True, "workflow": workflow})
    assert row["workflow"]["recovery"]["command"] == command
    assert row["workflow"]["recovery"]["command_unavailable_reason"] is None


def test_a_credential_source_names_its_constraint_and_no_command() -> None:
    summary = _recovery_summary({
        "action_kind": "manual_or_credential_action",
        "reason": "credential_or_manual_source_cannot_be_auto_recovered",
        "constraints": {"credential_required": True, "manual_required": False},
    })
    assert summary == {
        "command": None,
        "command_unavailable_reason":
            "credential_or_manual_source_cannot_be_auto_recovered",
        "needs_credential": True,
        "needs_manual_run": False,
    }


def test_no_recorded_action_is_named_not_blank() -> None:
    summary = _recovery_summary(None)
    assert summary["command"] is None
    assert summary["command_unavailable_reason"] == "no_recovery_action_recorded"
    assert summary["needs_credential"] is None


# --- A8: which sources' runs wrote into a version ---------------------------

class _Cursor:
    def __init__(self, conn: "_Conn") -> None:
        self.conn = conn

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        self.conn.calls.append(params)
        if self.conn.fail:
            raise psycopg.errors.UndefinedTable("relation x does not exist")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.conn.rows


class _Tx:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *_: Any) -> None:
        return None


class _Conn:
    def __init__(self, rows: list[tuple[Any, ...]], fail: bool = False) -> None:
        self.rows = rows
        self.fail = fail
        self.calls: list[dict[str, Any]] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def transaction(self) -> _Tx:
        return _Tx()


def test_source_writes_are_bounded_by_the_two_versions() -> None:
    conn = _Conn([("jira", "d.jira", 1, 571, 692), ("twiki_eos", "d.twiki-eos", 1, 0, 0)])
    out = attempt_detail._source_writes(
        conn, deployment_name="d",
        created_at="2026-09-20T21:16:46+00:00",
        previous_created_at="2026-09-20T21:16:33+00:00",
        previous="read",
    )
    assert out["unavailable"] is None
    assert out["window"]["after"].startswith("2026-09-20T21:16:33")
    assert out["rows"][0] == {"source_name": "jira", "ownership_id": "d.jira", "runs": 1,
                              "records_written": 571,
                              "relationships_written": 692}
    assert len(conn.calls) == 1  # one grouped statement, not one per source


def test_the_first_version_counts_every_earlier_run() -> None:
    conn = _Conn([])
    out = attempt_detail._source_writes(
        conn, deployment_name="d", created_at="2026-09-20T21:15:45+00:00",
        previous_created_at=None, previous="none",
    )
    assert out["unavailable"] is None
    assert conn.calls[0]["after"] is None


def test_an_unread_predecessor_draws_no_window() -> None:
    conn = _Conn([("jira", "d.jira", 1, 1, 1)])
    out = attempt_detail._source_writes(
        conn, deployment_name="d", created_at="2026-09-20T21:15:45+00:00",
        previous_created_at=None, previous="unknown",
    )
    assert out["rows"] == []
    assert out["unavailable"] == {"reason": "previous_version_not_read"}
    assert conn.calls == []


def test_a_failed_runs_query_is_named_without_its_text() -> None:
    out = attempt_detail._source_writes(
        _Conn([], fail=True), deployment_name="d",
        created_at="2026-09-20T21:16:46+00:00",
        previous_created_at="2026-09-20T21:16:33+00:00", previous="read",
    )
    assert out["unavailable"] == {
        "reason": "source_run_diagnostics_query_failed",
        "error_type": "UndefinedTable",
    }
    assert "relation" not in str(out)


# --- A6: which record types hold text ----------------------------------------

def _rollup(*triples: tuple[str, str, str]) -> OntologyRollup:
    return OntologyRollup(generation_id="g", rows=[
        OntologyBucketRow(src, edge, dst, "p", 3) for src, edge, dst in triples
    ])


_SESSION = SimpleNamespace(deployment_name="d", generation_id="g")


def test_text_types_are_the_types_joined_to_chunks(monkeypatch) -> None:
    from okg.substrate.console_views import ontology_rollup_read

    monkeypatch.setattr(
        ontology_rollup_read, "read_ontology_rollup",
        lambda conn, **kw: _rollup(
            ("documentation_page", "contains", "document_chunk"),
            ("document_chunk", "member_of", "twiki_page"),
            ("document_chunk", "references", "jira_issue"),
            ("cmssw_release", "supersedes", "cmssw_release"),
        ),
    )
    types, reason = records._text_bearing_types(object(), session=_SESSION)
    assert (types, reason) == (["documentation_page", "twiki_page"], None)


def test_no_rollup_is_a_reason_not_an_empty_list(monkeypatch) -> None:
    from okg.substrate.console_views import ontology_rollup_read

    def _missing(conn: Any, **kw: Any) -> OntologyRollup:
        raise OntologyRollupUnavailable("g", "sentinel_absent", "no sentinel")

    monkeypatch.setattr(ontology_rollup_read, "read_ontology_rollup", _missing)
    types, reason = records._text_bearing_types(object(), session=_SESSION)
    assert types is None
    assert reason == "ontology_rollup_unavailable:sentinel_absent"


# --- A10: the not-retained sentence is plain words ---------------------------

def test_the_not_retained_detail_carries_no_runtime_vocabulary() -> None:
    import inspect

    source = inspect.getsource(records._locate_authority_payload)
    assert "only an indexed-v2 source authority retains" not in source
    assert "store records individually; this source does not" in source


@pytest.mark.parametrize("key", ["records_written", "relationships_written"])
def test_write_counts_refuse_a_boolean(key: str) -> None:
    field = "nodes_appended" if key == "records_written" else "edges_appended"
    assert _adapt_run({field: True})[key] is None
