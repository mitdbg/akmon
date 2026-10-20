"""Console read-layer defects found on the Archiv3 deployment (`cern-r3final`).

PACT console-archiv3-read-layer-fixes, issue #1924. No database: each case is
the recorded input the live deployment served, reduced to the fields the
composer decides from.
"""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any


from okg.substrate.console_views import health
from okg.substrate.console_views.record_layout import label_for


# --- R12: a person is titled by the name its source recorded ----------------


def test_a_person_is_titled_by_its_display_name() -> None:
    """Archiv3 `person:234c3d3b65e30f95` was titled by its hash.

    The stored attrs are exactly these keys (`display_name`, `person_id`,
    `text`, lineage); no key the label read looked at.
    """
    attrs = {
        "display_name": "Piljun Gwak",
        "person_id": "234c3d3b65e30f95",
        "text": "Piljun Gwak",
    }
    assert label_for("person:234c3d3b65e30f95", "person", attrs) == "Piljun Gwak"


def test_a_record_with_no_display_name_keeps_its_kind_label() -> None:
    assert label_for("person:ab", "person", {"person_id": "ab"}) == "Person · ab"


def test_a_title_still_wins_over_a_display_name() -> None:
    attrs = {"title": "Release notes", "display_name": "Someone"}
    assert label_for("doc:1", "document", attrs) == "Release notes"


# --- R3: a deployment with no runtime queue ----------------------------------


class _QueueCursor:
    def __init__(self, regclass: Any) -> None:
        self._regclass = regclass
        self.statements: list[str] = []

    def __enter__(self) -> "_QueueCursor":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def execute(self, sql: str, *_: Any) -> None:
        self.statements.append(sql)

    def fetchone(self) -> tuple[Any]:
        return (self._regclass,)


class _QueueConn:
    def __init__(self, regclass: Any) -> None:
        self.cur = _QueueCursor(regclass)

    def transaction(self) -> "_QueueConn":
        return self

    def __enter__(self) -> "_QueueConn":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def cursor(self) -> _QueueCursor:
        return self.cur


def test_an_absent_runtime_schema_is_read_as_not_observed(monkeypatch) -> None:
    """`dbos.workflow_status` does not exist on a batch-built deployment.

    The queue view then answers one `unavailable` row of zeros; the read must
    not reach it and must name the absence instead.
    """
    def must_not_summarize(_cur: Any) -> dict[str, Any]:
        raise AssertionError("no queue summary without a runtime schema")

    monkeypatch.setattr(health, "_runtime_queue_snapshot", must_not_summarize)
    snapshot = health._safe_runtime_queue_snapshot(_QueueConn(None))

    assert snapshot["status"] == "unavailable"
    assert snapshot["reason"] == health.RUNTIME_SCHEMA_ABSENT
    assert snapshot["observed"] is False
    assert snapshot["dlq_count"] is None
    assert set(snapshot["queue_depth"].values()) == {None}


def test_a_present_runtime_schema_is_summarized(monkeypatch) -> None:
    monkeypatch.setattr(
        health, "_runtime_queue_snapshot",
        lambda _cur: {"status": "available", "queue_depth": {"ingest": 0}},
    )
    snapshot = health._safe_runtime_queue_snapshot(
        _QueueConn("dbos.workflow_status"),
    )
    assert snapshot["status"] == "available"


def _checkpoint(identifier: str, status: str) -> SimpleNamespace:
    return SimpleNamespace(
        checkpoint_id=identifier,
        created_at=datetime(2026, 9, 20, tzinfo=timezone.utc),
        metadata={"status": status, "node_count": 2, "edge_count": 1},
    )


def _compose_health(monkeypatch, queue: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    from okg.substrate.graph_store_internal.generations import RecentGenerations

    current = _checkpoint("canonical:current", "published")
    # No `get_generation` on the fake: the pinned generation is a projected
    # read now (issue #2546), so a regression to the whole-document read fails.
    store = SimpleNamespace(
        recent_generations=lambda **kw: RecentGenerations(
            [current], scanned=1, scan_window=kw["scan_window"],
            window_exhausted=False,
        ),
        count_generations=lambda **kw: 1,
    )
    monkeypatch.setattr(
        health.ChronosGraphStore, "from_connection", lambda *a, **kw: store,
    )
    # The pinned generation is a projected read now (issue #2546); the fake
    # keeps its contract, nested keys included.
    def _read_generation_fields(conn, generation_id, keys, nested=None):
        assert generation_id == current.checkpoint_id
        fields = {key: current.metadata[key] for key in keys if key in current.metadata}
        for outer, inner in (nested or {}).items():
            value = current.metadata.get(outer)
            fields[outer] = (
                {key: value[key] for key in inner if key in value}
                if isinstance(value, dict) else None
            )
        return current.checkpoint_id, current.created_at, fields

    monkeypatch.setattr(health, "read_generation_fields", _read_generation_fields)
    monkeypatch.setattr(health, "_resource_snapshot", lambda *a, **kw: {})
    monkeypatch.setattr(
        health, "_critical_read_prerequisites",
        lambda *a, **kw: {"prerequisites": []},
    )
    monkeypatch.setattr(health, "_safe_runtime_queue_snapshot", lambda *a: queue)
    if "breakdown" in kwargs:
        breakdown = kwargs.pop("breakdown")
        monkeypatch.setattr(
            health, "_health_source_breakdown", lambda *a, **kw: breakdown,
        )
    session = SimpleNamespace(
        generation_id=current.checkpoint_id, branch_name="default",
        compat_generation_id=1,
    )
    return health.compose_health(
        None, deployment_name="cern-r3final", session=session, **kwargs,
    )


def test_a_deployment_with_no_runtime_queue_is_not_a_failing_check(
    monkeypatch,
) -> None:
    """Live Archiv3 served verdict `failing` on `dbos.workflow_status missing`
    beside queue_total 0 and dlq_count 0."""
    queue = health._unavailable_runtime_queue_snapshot(
        health.RUNTIME_SCHEMA_ABSENT, observed=False,
    )
    result = _compose_health(monkeypatch, queue, breakdown={})

    codes = {row["code"]: row["severity"] for row in result["checks"]}
    assert "runtime_queue_unavailable" not in codes
    assert codes["runtime_queue_not_observed"] == "warn"
    assert result["verdict"] != "failing"
    vitals = result["vitals"]
    assert vitals["queue_total"] is None
    assert vitals["dlq_count"] is None
    assert vitals["terminal_history_dlq_count"] is None
    assert set(vitals["queue_depth"].values()) == {None}
    assert vitals["queue_unavailable_reason"] == health.RUNTIME_SCHEMA_ABSENT


def test_a_queue_that_exists_but_cannot_be_read_still_fails(monkeypatch) -> None:
    """The failure case: a real queue whose read failed is still a fault."""
    queue = health._unavailable_runtime_queue_snapshot(
        "runtime_queue_read_timed_out",
    )
    result = _compose_health(monkeypatch, queue, breakdown={})

    codes = {row["code"]: row["severity"] for row in result["checks"]}
    assert codes["runtime_queue_unavailable"] == "fail"
    assert result["verdict"] == "failing"
    assert result["vitals"]["queue_total"] is None
    assert result["vitals"]["queue_unavailable_reason"] == (
        "runtime_queue_read_timed_out"
    )


def test_an_unavailable_queue_serves_no_zero_counts(monkeypatch) -> None:
    """The queue view answers zeros with `availability: unavailable`; those
    zeros are not measurements and are not served as counts."""
    queue = {
        "status": "unavailable",
        "reason": "dbos.workflow_status.status missing",
        "queue_depth": {"ingest": 0, "link": 0},
        "dlq_count": 0,
        "terminal_history_dlq_count": 0,
    }
    vitals = _compose_health(monkeypatch, queue, breakdown={})["vitals"]
    assert vitals["queue_total"] is None
    assert vitals["dlq_count"] is None
    assert set(vitals["queue_depth"].values()) == {None}
    assert vitals["queue_unavailable_reason"] == (
        "dbos.workflow_status.status missing"
    )


def test_an_available_queue_keeps_its_counts(monkeypatch) -> None:
    queue = {
        "status": "available",
        "queue_depth": {"ingest": 2, "link": 1},
        "dlq_count": 0,
        "terminal_history_dlq_count": 4,
    }
    vitals = _compose_health(monkeypatch, queue, breakdown={})["vitals"]
    assert vitals["queue_total"] == 3
    assert vitals["dlq_count"] == 0
    assert vitals["terminal_history_dlq_count"] == 4
    assert vitals["queue_unavailable_reason"] is None


# --- R4/R6: Health's source breakdown reads the producers' state ------------


ARCHIV3 = "cern-r3final"
ARCHIV3_SOURCES = [
    # (declared name, ownership id, health, convergence state, recorded reason)
    # -- what live `console/producers` served for cern-r3final, 2026-09-22.
    ("twiki_eos", "cern-r3final.twiki-eos", "healthy", "fresh",
     "source_not_required_for_readiness"),
    ("cmssw_releases", "cern-r3final.cmssw-releases", "stale", "blocked",
     "completed_scope_watermark_stale"),
    ("jira", "cern-r3final.jira", "stale", "manual_required",
     "manual_or_credential_constraint"),
    ("github_repo", "cern-r3final.github-repo", "healthy", "fresh",
     "source_not_required_for_readiness"),
    ("gitlab_repo", "cern-r3final.gitlab-repo", "healthy", "fresh",
     "source_not_required_for_readiness"),
    ("docsite", "cern-r3final.docsite", "stale", "blocked",
     "completed_scope_watermark_stale"),
]


def _source_diagnostics_payload() -> dict[str, Any]:
    return {
        "sources": [
            {
                "name": name,
                "source_key": key,
                "health": {"status": status},
                "workflow": {"state": state, "reason": reason},
            }
            for name, key, status, state, reason in ARCHIV3_SOURCES
        ],
    }


def _write_registry(tmp_path, monkeypatch) -> None:
    dep_dir = tmp_path / ARCHIV3
    dep_dir.mkdir()
    lines = ["sources:"]
    for name, key, *_ in ARCHIV3_SOURCES:
        lines += [f"  {name}:", f"    ownership_id: {key}"]
    (dep_dir / "source_registry.yaml").write_text("\n".join(lines) + "\n")
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda name: dep_dir if name == ARCHIV3 else None,
    )


class _ProgressCursor:
    def __init__(self, rows: list[tuple[str, Any]]) -> None:
        self._rows = rows

    def __enter__(self) -> "_ProgressCursor":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def execute(self, *_: Any, **__: Any) -> None:
        return None

    def fetchall(self) -> list[tuple[str, Any]]:
        return list(self._rows)


class _ProgressConn:
    def __init__(self, rows: list[tuple[str, Any]]) -> None:
        self._rows = rows

    def cursor(self) -> _ProgressCursor:
        return _ProgressCursor(self._rows)


def _every_source_ran() -> _ProgressConn:
    ran_at = datetime(2026, 9, 20, 21, 16, tzinfo=timezone.utc)
    return _ProgressConn([(key, ran_at) for _, key, *_ in ARCHIV3_SOURCES])


def test_the_breakdown_agrees_with_the_producers_read(tmp_path, monkeypatch) -> None:
    """Live Archiv3: Health said 6 of 6 healthy while Sources said 3 stale."""
    _write_registry(tmp_path, monkeypatch)
    states = health.producer_states_from_source_diagnostics(
        _source_diagnostics_payload(),
    )

    breakdown = health._health_source_breakdown(
        _every_source_ran(),
        deployment_name=ARCHIV3,
        excluded_sources=[],
        producer_states=states,
    )

    by_name = {row["name"]: row for row in breakdown["sources"]}
    for name, _key, status, state, reason in ARCHIV3_SOURCES:
        assert by_name[name]["status"] == status, name
        assert by_name[name]["state"] == state, name
        assert by_name[name]["state_reason"] == reason, name
    assert breakdown["summary"]["healthy"] == 3
    assert breakdown["summary"]["needs_review"] == 3
    assert breakdown["status_basis"] == "producer_state"
    assert breakdown["status_basis_unavailable_reason"] is None


def test_an_exclusion_still_overrides_the_producer_state(
    tmp_path, monkeypatch,
) -> None:
    _write_registry(tmp_path, monkeypatch)
    states = health.producer_states_from_source_diagnostics(
        _source_diagnostics_payload(),
    )
    breakdown = health._health_source_breakdown(
        _every_source_ran(),
        deployment_name=ARCHIV3,
        excluded_sources=["jira"],
        producer_states=states,
    )
    by_name = {row["name"]: row for row in breakdown["sources"]}
    assert by_name["jira"]["status"] == "excluded"


def test_a_source_the_producers_read_did_not_return_is_unknown_by_name(
    tmp_path, monkeypatch,
) -> None:
    """A configured source with no producer row is not quietly `healthy`."""
    _write_registry(tmp_path, monkeypatch)
    payload = _source_diagnostics_payload()
    payload["sources"] = [
        row for row in payload["sources"] if row["name"] != "docsite"
    ]
    breakdown = health._health_source_breakdown(
        _every_source_ran(),
        deployment_name=ARCHIV3,
        excluded_sources=[],
        producer_states=health.producer_states_from_source_diagnostics(payload),
    )
    by_name = {row["name"]: row for row in breakdown["sources"]}
    assert by_name["docsite"]["status"] == "unknown"
    assert by_name["docsite"]["latest_issue"]
    assert by_name["docsite"]["state"] is None


def test_without_the_producers_read_the_basis_is_named(tmp_path, monkeypatch) -> None:
    """The progress-row rule is still what a caller without the producers read
    gets, and the answer says which basis it used and why."""
    _write_registry(tmp_path, monkeypatch)
    breakdown = health._health_source_breakdown(
        _every_source_ran(),
        deployment_name=ARCHIV3,
        excluded_sources=[],
        producer_states=None,
        producer_states_unavailable_reason="producers_read_failed:RuntimeError",
    )
    assert breakdown["status_basis"] == "source_progress"
    assert breakdown["status_basis_unavailable_reason"] == (
        "producers_read_failed:RuntimeError"
    )
    assert all(row["state"] is None for row in breakdown["sources"])


# --- Low: "Items in" is not a recorded count of items read ------------------


def test_a_run_serves_no_items_in_the_run_never_recorded() -> None:
    """Every Archiv3 run served `items_in: 0`, including one that produced
    9,857 items. `input_count` is the number of pushed source events handed to
    the run (`ingest/service.py`), 0 for every pulled run -- not items read."""
    from okg.substrate.console_views.producers import _adapt_run

    run = _adapt_run({
        "status": "OK", "observed_at": "2026-09-20T21:16:44Z",
        "input_count": 0, "output_count": 9857, "total_ms": 9846.9,
        "slowest_phase": "source_runner.adapter_run",
    })
    assert run["items_in"] is None
    assert run["items_in_unavailable_reason"]
    assert run["source_events_in"] == 0
    assert run["items_out"] == 9857


# --- Low: one display name for a source on every route ----------------------


def test_a_source_s_display_name_is_its_declared_name(tmp_path, monkeypatch) -> None:
    from okg.substrate.console_views.producer_identity import source_display_names

    _write_registry(tmp_path, monkeypatch)
    names, reason = source_display_names(ARCHIV3)
    assert reason is None
    assert names["cern-r3final.twiki-eos"] == "twiki_eos"
    assert names["cern-r3final.cmssw-releases"] == "cmssw_releases"


def test_a_deployment_with_no_registry_says_why_it_has_no_names(
    tmp_path, monkeypatch,
) -> None:
    from okg.substrate.console_views.producer_identity import source_display_names

    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir", lambda name: None,
    )
    names, reason = source_display_names(ARCHIV3)
    assert names == {}
    assert reason == "no_source_registry_for_deployment"


def test_an_unreadable_registry_is_named_not_raised(tmp_path, monkeypatch) -> None:
    from okg.substrate.console_views.producer_identity import source_display_names

    dep_dir = tmp_path / ARCHIV3
    dep_dir.mkdir()
    (dep_dir / "source_registry.yaml").write_text("sources: [unclosed\n")
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda name: dep_dir if name == ARCHIV3 else None,
    )
    names, reason = source_display_names(ARCHIV3)
    assert names == {}
    assert reason is not None and reason.startswith("source_registry_unreadable:")


def test_an_attempt_s_source_rows_carry_the_declared_name() -> None:
    from okg.substrate.console_views.attempt_detail import _name_source_rows

    section = {"rows": [
        {"name": "cern-r3final.twiki-eos", "count": 5377},
        {"name": "cern-r3final.ghost", "count": 1},
    ]}
    _name_source_rows(section, {"cern-r3final.twiki-eos": "twiki_eos"}, None)
    assert section["rows"][0]["source_name"] == "twiki_eos"
    assert section["rows"][0]["ownership_id"] == "cern-r3final.twiki-eos"
    assert section["rows"][1]["source_name"] is None
    assert section["source_names_unavailable_reason"] is None
