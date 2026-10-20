"""The one producers read: one row per producer, and a bounded cost.

No database. What this read must guarantee -- that it lists each producer
once, that it refuses a bad parameter by name rather than clamping it, and
that it issues the same number of statements for 200 producers as for 5 -- is
decidable from a recording connection and a registry document.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.console_views import sources as sources_mod
from okg.substrate.console_views.producer_identity import ProducerIdentity
from okg.substrate.console_views.producers import (
    ProducersParameterError,
    compose_producers,
)


DEPLOYMENT = "producers-fixture"


class _RecordingCursor:
    def __init__(self, log: list[str]) -> None:
        self._log = log

    def __enter__(self) -> "_RecordingCursor":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def execute(self, sql: Any, *_: Any, **__: Any) -> None:
        self._log.append(" ".join(str(sql).split())[:80])

    def fetchall(self) -> list[Any]:
        return []

    def fetchone(self) -> Any:
        return None

    @property
    def description(self) -> None:
        return None


class _RecordingConn:
    """Counts every statement the read issues, whoever issues it."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def cursor(self, *_: Any, **__: Any) -> _RecordingCursor:
        return _RecordingCursor(self.statements)


def _session() -> SimpleNamespace:
    return SimpleNamespace(generation_id="gen:1", branch_name="default")


@pytest.fixture
def deployment(monkeypatch, tmp_path):
    """Install N configured sources and silence the runtime and rollup reads."""

    def install(count: int) -> Path:
        install.calls = {"rollup": 0, "runtime": 0}
        identities = [
            ProducerIdentity(
                name=f"source-{index:03d}",
                # The awkward shape on purpose: ownership_id is not the key.
                ownership_id=f"{DEPLOYMENT}.source_{index:03d}",
            )
            for index in range(count)
        ]
        monkeypatch.setattr(
            sources_mod, "registry_identities", lambda _name: identities
        )
        monkeypatch.setattr(
            sources_mod, "require_console_count_rollup_enabled", lambda **_: None
        )
        import okg.substrate.console_views.console_count_rollup_read as rollup

        # Honours `kinds` the way the real reader does. A stub that ignored
        # it handed a `source` dimension_value to the `source_subtype`
        # parser, which rightly refused it.
        from okg.substrate.derived_artifacts.maintained_derivation_constants import (
            encode_source_subtype,
        )

        def _rollup(conn, *, generation_id=None, kinds=None):
            install.calls["rollup"] += 1
            wanted = set(kinds) if kinds else None
            out: dict[tuple[str, str], int] = {}
            for identity in identities:
                if wanted is None or rollup.COUNT_KIND_SOURCE in wanted:
                    out[(rollup.COUNT_KIND_SOURCE, identity.ownership_id)] = 7
                if wanted is None or rollup.COUNT_KIND_SOURCE_SUBTYPE in wanted:
                    for subtype, count in (("document", 5), ("chunk", 2)):
                        key = encode_source_subtype(identity.ownership_id, subtype)
                        out[(rollup.COUNT_KIND_SOURCE_SUBTYPE, key)] = count
            return out

        monkeypatch.setattr(rollup, "read_console_count_rollup", _rollup)
        # No runtime on this fixture: freshness is the literal state.
        def _runtime(**_):
            install.calls["runtime"] += 1
            return {}

        monkeypatch.setattr(
            "okg.substrate.console_views.source_diagnostics."
            "compose_console_readiness",
            _runtime,
        )
        # The enricher and view composers are deliberately NOT stubbed: they
        # run against the recording connection, so their statements are
        # counted too and the bound covers the whole read.
        return tmp_path

    return install


def _compose(conn, dep_dir, **kwargs):
    return compose_producers(
        conn,
        deployment_name=DEPLOYMENT,
        deployment_dir=dep_dir,
        dsn="postgresql://unused/unused",
        session=_session(),
        **kwargs,
    )


def test_every_configured_producer_appears_once(deployment):
    dep_dir = deployment(5)
    payload = _compose(_RecordingConn(), dep_dir)

    names = [row["name"] for row in payload["producers"]]
    assert len(names) == len(set(names)), names
    assert payload["producer_count"] == 5, names
    assert all(row["kind"] == "source" for row in payload["producers"])


def test_a_row_carries_its_declared_ownership_id(deployment):
    dep_dir = deployment(3)
    payload = _compose(_RecordingConn(), dep_dir)

    row = next(r for r in payload["producers"] if r["name"] == "source-001")
    assert row["ownership_id"] == f"{DEPLOYMENT}.source_001"
    assert row["configured"] is True
    assert row["contribution"]["records_in_gen"] == 7


def test_freshness_is_a_named_state_not_a_blank(deployment):
    """No runtime supplies a watermark here; the state is printed, not hidden."""
    dep_dir = deployment(2)
    payload = _compose(_RecordingConn(), dep_dir)

    for row in payload["producers"]:
        assert row["freshness"], row
        assert row["freshness"].get("status"), row["freshness"]


def test_an_absent_latest_run_names_its_reason(deployment):
    dep_dir = deployment(2)
    payload = _compose(_RecordingConn(), dep_dir)

    row = payload["producers"][0]
    assert row["latest_run"] is None
    assert row["latest_run_unavailable_reason"]


@pytest.mark.parametrize(
    ("parameter", "value"),
    [
        ("producer_limit", 10_000),
        ("producer_limit", -1),
        ("run_limit", 999),
        ("run_limit", "many"),
        ("run_offset", -5),
    ],
)
def test_a_bad_parameter_is_refused_by_name(deployment, parameter, value):
    """Refused, never clamped to a value the caller did not ask for."""
    dep_dir = deployment(1)
    with pytest.raises(ProducersParameterError) as caught:
        _compose(_RecordingConn(), dep_dir, **{parameter: value})
    assert caught.value.parameter == parameter


def test_an_unknown_name_is_a_named_refusal(deployment):
    dep_dir = deployment(2)
    payload = _compose(_RecordingConn(), dep_dir, name="no-such-producer")

    assert payload["selected"]["found"] is False
    assert payload["selected"]["reason"]


def test_a_run_carries_no_invented_error_text():
    """No read records a run's message, so the row must not offer the key."""
    from okg.substrate.console_views.producers import _adapt_run

    run = _adapt_run({
        "status": "FAILED", "observed_at": "2026-09-21T00:00:00Z",
        "input_count": 1, "output_count": 0, "total_ms": 5.0,
        "slowest_phase": "adapter_run",
    })
    assert "error_text" not in run, (
        "error_text would always be null: v_source_run_diagnostics has no "
        "message column, so the key reads as 'this run recorded none'"
    )
    assert run["error_text_unavailable_reason"]
    assert run["outcome"] == "failed"


def test_a_truncated_run_history_reports_no_run_count(deployment, monkeypatch):
    """Asking for 2 runs must not make a producer that ran 3 times say 2."""
    dep_dir = deployment(1)
    three_runs = [
        {"run_id": f"r{i}", "status": "OK", "observed_at": f"2026-09-2{i}"}
        for i in range(2)
    ]
    monkeypatch.setattr(
        # producers.py imports this inside the function, so the patch must
        # land on the defining module, not on producers.
        "okg.substrate.console_views.source_diagnostics."
        "compose_source_diagnostics",
        lambda conn, **_: {
            "sources": [{
                "name": "source-000",
                "source_key": f"{DEPLOYMENT}.source_000",
                "in_registry": True,
                "contribution": {"records_in_gen": 7, "edges_in_gen": 0},
                "workflow": {}, "freshness": {"status": "not_evaluated"},
                "health": {"status": "healthy"}, "latest_run": {},
            }],
            # The query came back full at the cap: older runs may exist.
            "selected": {
                "run_history": three_runs,
                "run_summary": {"run_count": 2},
            },
            "read_authority": {},
        },
        raising=False,
    )
    payload = _compose(
        _RecordingConn(), dep_dir, name="source-000", run_limit=2
    )

    selected = payload["selected"]
    assert selected["run_history_truncated"] is True
    assert selected["more"] is True
    assert selected["row"]["run_count"] is None
    assert "truncated" in selected["row"]["run_count_unavailable_reason"]


def test_the_detail_lists_this_producer_s_records_by_type(deployment):
    """Recorded, and keyed the same way the producer's own total is."""
    dep_dir = deployment(3)
    payload = _compose(_RecordingConn(), dep_dir, name="source-001")

    by_type = payload["selected"]["records_by_type"]
    assert by_type["status"] == "available"
    assert by_type["types"] == [
        {"type": "document", "records": 5},
        {"type": "chunk", "records": 2},
    ]
    assert by_type["total"] == 7
    assert by_type["omitted"] == 0


def test_records_by_type_belong_to_one_producer_only(deployment):
    """Three producers share the rollup; a detail must take only its own."""
    dep_dir = deployment(3)
    payload = _compose(_RecordingConn(), dep_dir, name="source-002")

    by_type = payload["selected"]["records_by_type"]
    # 7 for THIS producer, not 21 for all three.
    assert by_type["total"] == 7


def test_relationships_by_type_is_named_as_not_recorded(deployment):
    """No rollup dimension pairs an edge with its producer, so say so."""
    dep_dir = deployment(1)
    payload = _compose(_RecordingConn(), dep_dir, name="source-000")

    rel = payload["selected"]["relationships_by_type"]
    assert rel["status"] == "not_recorded"
    assert "edge" in rel["reason"]
    # Never an empty list, which would read as "this producer emitted none".
    assert "types" not in rel


def test_an_unavailable_rollup_is_named_inline(deployment, monkeypatch):
    """A rollup this generation cannot serve is a named line, not a blank."""
    dep_dir = deployment(1)
    import okg.substrate.console_views.console_count_rollup_read as rollup

    real = rollup.read_console_count_rollup

    def _boom(conn, *, generation_id=None, kinds=None):
        if kinds and rollup.COUNT_KIND_SOURCE_SUBTYPE in set(kinds):
            raise RuntimeError("rollup absent for this generation")
        return real(conn, generation_id=generation_id, kinds=kinds)

    monkeypatch.setattr(rollup, "read_console_count_rollup", _boom)
    payload = _compose(_RecordingConn(), dep_dir, name="source-000")

    by_type = payload["selected"]["records_by_type"]
    assert by_type["status"] == "unavailable"
    assert by_type["reason"]
    assert by_type["types"] == []
    # Null, not zero: a zero would be indistinguishable from a real zero.
    assert by_type["total"] is None


def test_the_statement_count_does_not_grow_with_producers(deployment):
    """200 producers must cost what 5 cost: no per-producer query.

    WHAT THIS COUNTS, AND WHAT IT CANNOT.

    Counted: every statement the composers issue on the connection handed to
    `compose_producers`.

    Not counted there, and counted separately below instead:

    * The maintained count rollup. `read_console_count_rollup` serves it
      through a Chronos checkout of `okg.console_count_rollup`, which needs a
      real `psycopg` connection (`ChronosGraphStore.from_connection` reads
      `conn.info`) and a registered table. A recording double has neither, so
      the fixture stubs the reader and this test holds the number of ROLLUP
      READS equal instead of the statements each one costs.
    * `compose_console_readiness`, which opens its OWN connection from the
      DSN. Nothing it issues can reach this connection at any producer count.

    So the figure below is not the read's statement total, and this test does
    not claim it is. What this fixture measures is 7 statements on the shared
    connection plus 2 rollup reads and 1 runtime-readiness compose for the
    list (3 rollup reads for the detail, which adds records-by-type), the
    same at 5 producers and at 200.

    The whole read's real cost was measured separately, with a counting proxy
    over a real connection against the live `lane-d-home-render` deployment on
    2026-09-21: 42 statements for the list over three sources and 59 for the
    detail. Most of those are the Chronos checkout each rollup read performs,
    which is per READ, not per producer. What this test holds is the growth.
    """
    small_conn = _RecordingConn()
    _compose(small_conn, deployment(5))
    small = len(small_conn.statements)
    small_calls = dict(deployment.calls)

    large_conn = _RecordingConn()
    _compose(large_conn, deployment(200))
    large = len(large_conn.statements)
    large_calls = dict(deployment.calls)

    # Guard against the vacuous pass: a recording connection that captured
    # nothing would make "5 costs what 200 costs" true and meaningless.
    assert small > 0, "the recording connection captured no statement at all"
    assert small == large, (
        f"5 producers issued {small} statements, 200 issued {large}; "
        f"extra: {large_conn.statements[small:]}"
    )
    # A bounded read, not merely an equal one.
    assert large <= 12, f"{large} statements: {large_conn.statements}"
    # And the two reads this connection cannot see are bounded the same way.
    assert small_calls["rollup"] > 0, small_calls
    assert small_calls == large_calls, (
        f"5 producers made {small_calls} stubbed reads, 200 made {large_calls}"
    )
    # ONE rollup read per request, not one per composer. Each read performs a
    # Chronos `checkout_generation`, which takes a generation READ LEASE, and
    # that lease is a WRITE: it deletes expired pins and inserts one
    # (`history_read_guard.py`). The list used to read the rollup twice --
    # once in `compose_sources`, once in `compose_enricher_diagnostics` -- so
    # loading a read-only console page wrote to the pin table twice
    # (issue #2202). The number is the count, not merely a bound: a second
    # read is a second lease and this test is where that shows up.
    assert small_calls["rollup"] == 1, (
        f"the list read the count rollup {small_calls['rollup']} times; "
        "each read takes a generation read lease, which writes a pin row"
    )


def test_the_detail_costs_the_same_at_5_and_200_producers(deployment):
    """Records by type is one grouped read, not one per type or per producer.

    Counts what the test above counts, with the same two exclusions and for
    the same reasons; read its docstring for what the figure is and is not.
    """
    small_conn = _RecordingConn()
    _compose(small_conn, deployment(5), name="source-001")
    small = len(small_conn.statements)
    small_calls = dict(deployment.calls)

    large_conn = _RecordingConn()
    _compose(large_conn, deployment(200), name="source-001")
    large = len(large_conn.statements)
    large_calls = dict(deployment.calls)

    assert small > 0, "the recording connection captured no statement at all"
    assert small == large, (
        f"5 producers issued {small} statements, 200 issued {large}; "
        f"extra: {large_conn.statements[small:]}"
    )
    assert large <= 14, f"{large} statements: {large_conn.statements}"
    # The rollup reads this connection cannot count are equal too, and the
    # detail costs the SAME one read as the list: records-by-type is served
    # from the rows the request already read, not from a third checkout.
    assert small_calls["rollup"] == large_calls["rollup"] > 0, (
        f"5 producers made {small_calls} stubbed reads, 200 made {large_calls}"
    )
    # TWO on the detail, and exactly two: one shared read for the list's
    # dimensions, and records-by-type's own. That second read is deliberate --
    # it is the one rollup read that names its own unavailability inline, so
    # a generation missing the subtype dimension marks one section unavailable
    # instead of failing the whole page. Folding it into the shared read would
    # move that failure into `compose_sources`, which does not catch it.
    assert small_calls["rollup"] == 2, (
        f"the detail read the count rollup {small_calls['rollup']} times; "
        "each read takes a generation read lease, which writes a pin row"
    )


def test_the_list_and_the_detail_agree_on_a_run_s_outcome(deployment, monkeypatch):
    """One run, reported one way.

    `_latest_run_summary` overwrites the run's own `status` column with the
    timing-availability marker, so the list served `outcome: "available"` for
    the very run the detail served as `"failed"`. The live capture in this
    change's evidence shows both spellings of run 030f6551. The list row's
    outcome is the run's status or it is nothing.
    """
    from okg.substrate.console_views.source_diagnostics import _latest_run_summary

    dep_dir = deployment(1)
    run = {
        "run_id": "r0",
        "status": "FAILED",
        "timing_status": "available",
        "observed_at": "2026-09-21T00:00:00Z",
        "input_count": 1,
        "output_count": 0,
        "total_ms": 5.0,
        "slowest_phase": "adapter_run",
    }
    monkeypatch.setattr(
        "okg.substrate.console_views.source_diagnostics."
        "compose_source_diagnostics",
        lambda conn, **_: {
            "sources": [{
                "name": "source-000",
                "source_key": f"{DEPLOYMENT}.source_000",
                "in_registry": True,
                "contribution": {"records_in_gen": 7, "edges_in_gen": 0},
                "workflow": {},
                "freshness": {"status": "not_evaluated"},
                "health": {"status": "degraded"},
                # The real summary shape, built by the real function.
                "latest_run": _latest_run_summary([run]),
            }],
            "selected": {
                "run_history": [run],
                "run_summary": {"run_count": 1},
            },
            "read_authority": {},
        },
        raising=False,
    )

    payload = _compose(_RecordingConn(), dep_dir, name="source-000")

    listed = payload["producers"][0]["latest_run"]
    detailed = payload["selected"]["run_history"][0]
    assert listed["run_id"] == detailed["run_id"] == "r0"
    assert detailed["outcome"] == "failed"
    assert listed["outcome"] == "failed", (
        "the list served the timing-availability marker as the run's outcome: "
        f"{listed['outcome']!r}"
    )


def _failing_enricher(monkeypatch, message: str) -> None:
    """Make the enricher composer raise, with a message worth withholding."""
    def _boom(conn, **_: Any) -> dict[str, Any]:
        raise RuntimeError(message)

    monkeypatch.setattr(
        "okg.substrate.console_views.enricher_diagnostics."
        "compose_enricher_diagnostics",
        _boom,
        raising=False,
    )


def test_a_failed_authority_is_named_without_its_exception_text(
    deployment, monkeypatch
):
    """The console names what failed. It never serves the message.

    `str(exc)` put a psycopg error -- a table name, a column, a fragment of
    SQL, and on some drivers the DSN -- into a payload the browser renders.
    The reason code and the exception's class name say which authority failed
    and how; the message says things a read-only console must not publish.
    """
    dep_dir = deployment(1)
    message = 'relation "okg.enricher_runs" does not exist at okg://secret-dsn'
    _failing_enricher(monkeypatch, message)

    payload = _compose(_RecordingConn(), dep_dir)

    authority = payload["read_authority"]["enrichers"]
    assert authority["status"] == "unavailable"
    assert authority["reason"] == "enricher_diagnostics_composer_failed"
    assert authority["error_type"] == "RuntimeError"
    assert "error" not in authority, authority
    served = json.dumps(payload)
    assert message not in served
    assert "okg.enricher_runs" not in served
    assert "secret-dsn" not in served


def test_a_failed_authority_is_still_named_in_omitted_data(
    deployment, monkeypatch
):
    """Withholding the message must not withhold the fact."""
    dep_dir = deployment(1)
    _failing_enricher(monkeypatch, "boom")

    payload = _compose(_RecordingConn(), dep_dir)

    assert {"authority": "enricher_diagnostics", "kind": "enricher"} in (
        payload["omitted_data"]
    )


@pytest.mark.parametrize("parameter", ["run_limit", "producer_limit"])
def test_a_zero_limit_is_refused_by_name(deployment, parameter):
    """Zero is not a page size, and it manufactured a false truncation.

    `run_limit=0` made `truncated = len(history) >= 0 + 0` true, so a producer
    that ran three times was served as a truncated history with its run count
    withheld. `producer_limit=0` reached `max(1, ...)` in the source
    diagnostics composer and came back as one. The route's docstring promises
    a refusal by name, not a value the caller did not ask for.
    """
    dep_dir = deployment(1)
    with pytest.raises(ProducersParameterError) as caught:
        _compose(_RecordingConn(), dep_dir, **{parameter: 0})
    assert caught.value.parameter == parameter
    assert "at least 1" in caught.value.detail


def test_producer_limit_is_refused_above_the_ceiling_that_applies(deployment):
    """The declared ceiling is the one the read can honour.

    `producer_limit` accepted 500 and handed it to the source diagnostics
    composer, which clamps to 200 -- so 201 through 500 were accepted and
    silently served as 200. Refused by name, the way the route's docstring
    says, rather than answered with a limit the caller did not ask for.
    """
    dep_dir = deployment(1)
    with pytest.raises(ProducersParameterError) as caught:
        _compose(_RecordingConn(), dep_dir, producer_limit=201)
    assert caught.value.parameter == "producer_limit"
    assert "200" in caught.value.detail


def test_the_ceiling_the_read_declares_is_the_one_it_enforces(deployment):
    """No value the read accepts is clamped downstream."""
    from okg.substrate.console_views import source_diagnostics as diagnostics
    from okg.substrate.console_views.producers import PRODUCER_LIMIT_MAX

    dep_dir = deployment(1)
    seen: list[int] = []
    real = diagnostics.compose_source_diagnostics

    def _record(conn, **kwargs: Any) -> dict[str, Any]:
        seen.append(int(kwargs["source_limit"]))
        return real(conn, **kwargs)

    from okg.substrate.console_views import source_diagnostics as module

    module.compose_source_diagnostics = _record
    try:
        _compose(_RecordingConn(), dep_dir, producer_limit=PRODUCER_LIMIT_MAX)
    finally:
        module.compose_source_diagnostics = real

    # The composer clamps with max(1, min(value, 200)); a value it would
    # change is a value this read should have refused.
    assert seen == [PRODUCER_LIMIT_MAX]
    assert max(1, min(PRODUCER_LIMIT_MAX, 200)) == PRODUCER_LIMIT_MAX


def test_a_recorded_convergence_state_reaches_the_row() -> None:
    """The state the table prints is the runtime's, and nothing else.

    Every row of lane-d-home-render reads "State not recorded", which is what
    a null `workflow.state` must print. The column is kept rather than cut
    because a read does fill it: the runtime readiness surface records a
    convergence per source, `_workflow_summary` takes its `state` (or its
    `status`), and `_source_row` carries that through to the row the view
    draws. This walks that path with a recorded convergence and without one,
    so "not recorded" stays a fact about the deployment rather than a column
    nothing can ever fill.
    """
    from okg.substrate.console_views.producers import _source_row
    from okg.substrate.console_views.source_diagnostics import _workflow_summary

    recorded = _workflow_summary({"convergence": {"state": "updated"}})
    assert recorded["state"] == "updated"
    assert _source_row({"name": "s", "workflow": recorded})["workflow"]["state"] == "updated"

    # A deployment whose runtime records no sync status: convergence is empty,
    # and the row says so with a null rather than a guess at "updated".
    none = _workflow_summary({"convergence": {}})
    assert none["state"] is None
    assert none["status"] == "not_observed"
    assert _source_row({"name": "s", "workflow": none})["workflow"]["state"] is None


# --- what a failed sub-read of the sources composer may and may not serve ----

#: A runtime-readiness failure whose message carries everything a read-only
#: console must never publish: a password, a host name and a DSN scheme.
_LEAKY_RUNTIME_MESSAGE = (
    "could not connect to the source-sync runtime: "
    "dsn=postgresql://okg:hunter2@db.example/okg"
)


def _failing_runtime(monkeypatch, message: str = _LEAKY_RUNTIME_MESSAGE) -> None:
    """Make the runtime readiness composer raise, as a dead runtime does."""

    def _boom(**_: Any) -> dict[str, Any]:
        raise RuntimeError(message)

    monkeypatch.setattr(
        "okg.substrate.console_views.source_diagnostics."
        "compose_console_readiness",
        _boom,
    )


def test_a_failed_runtime_read_serves_no_exception_text(deployment, monkeypatch):
    """The sources authority is carried through this read; its text is not.

    `compose_source_diagnostics` wrote `str(exc)` into its own
    `read_authority`, and `compose_producers` carried that sub-tree verbatim
    under `read_authority.sources`. A connection failure therefore served the
    driver's message -- which carries the host it dialled and, on the usual
    spelling, the DSN with its password -- to a browser. The reason code and
    the exception's class say which authority failed and how; the message
    belongs in the server log.
    """
    dep_dir = deployment(1)
    _failing_runtime(monkeypatch)

    payload = _compose(_RecordingConn(), dep_dir)

    served = json.dumps(payload)
    assert "hunter2" not in served, served
    assert "db.example" not in served, served
    assert "postgresql://" not in served, served

    def _errors(node: Any, path: str = "") -> list[str]:
        if isinstance(node, dict):
            found = [f"{path}.error" for key in node if key == "error"]
            for key, value in node.items():
                found += _errors(value, f"{path}.{key}")
            return found
        if isinstance(node, list):
            out: list[str] = []
            for index, value in enumerate(node):
                out += _errors(value, f"{path}[{index}]")
            return out
        return []

    sources_authority = payload["read_authority"]["sources"]
    assert _errors(sources_authority) == [], sources_authority
    assert sources_authority["runtime_readiness"]["error_type"] == "RuntimeError"
    assert sources_authority["runtime_readiness"]["reason"]


def test_a_failed_runtime_read_is_named_in_omitted_data(deployment, monkeypatch):
    """A read that failed must not read as a deployment that records nothing.

    With the runtime unreachable every row is served `not_evaluated` freshness
    and a null state, which the page prints as "not evaluated" and "State not
    recorded" -- the same words a healthy deployment with no watermark gets.
    The failure has to reach the payload the page reads, or the two are
    indistinguishable.
    """
    dep_dir = deployment(2)
    _failing_runtime(monkeypatch)

    payload = _compose(_RecordingConn(), dep_dir)

    entry = next(
        row for row in payload["omitted_data"]
        if row.get("authority") == "runtime_readiness"
    )
    assert entry["reason"] == "console_readiness_composer_failed"
    # Not a producer KIND that went missing: every producer is still listed.
    # The sentence the page prints depends on telling those two apart.
    assert "kind" not in entry, entry
    assert entry["affects"]


def test_no_database_object_name_reaches_the_payload(deployment):
    """A read-only console names its authorities, not the objects behind them.

    `read_authority.sources.source_run_diagnostics.view` served the literal
    `okg.v_source_run_diagnostics` to the browser. An object name is the
    server's business; the payload carries the authority's reason code.
    """
    dep_dir = deployment(2)

    payload = _compose(_RecordingConn(), dep_dir)

    served = json.dumps(payload)
    assert "okg.v_" not in served, served
    assert "v_source_run_diagnostics" not in served, served


def test_a_source_row_carries_no_policy_reason_no_read_records(deployment):
    """`workflow.withheld_reason` was null on every source, always.

    `_workflow_summary` emits `status`, `state`, `active`, `active_work` and
    `stages` and has never emitted a policy reason, so the two keys the row
    carried were null for every source of every deployment while the field map
    said the publisher recorded them. A field no read supplies is null beside
    the reason it is, in this module's own convention.
    """
    dep_dir = deployment(1)

    payload = _compose(_RecordingConn(), dep_dir)

    workflow = payload["producers"][0]["workflow"]
    assert "withheld_reason" not in workflow, workflow
    assert "excluded_reason" not in workflow, workflow
    assert workflow["latest_issue"] is None
    assert workflow["latest_issue_unavailable_reason"]


@pytest.mark.parametrize(
    ("status", "recorded"),
    [
        ("withheld", "withheld:input_source_held:jira"),
        ("failing", "failed:IncompleteInputs"),
        ("wired-but-silent", "0 edges emitted since wiring"),
    ],
)
def test_an_enricher_carries_its_issue_in_every_state_that_records_one(
    status, recorded
):
    """`latest_issue` was carried for `withheld` alone and dropped twice over.

    `compose_enricher_diagnostics` records it for `failing` and for
    `wired-but-silent` as well, and in all three it is a recorded class or a
    fixed sentence -- `failed:<ExceptionClassName>`, the withheld prefix plus
    the source names the publisher held on, or the literal "0 edges emitted
    since wiring". None of them is an exception message, and dropping two of
    the three left the page with nothing to say about a failing enricher.
    """
    from okg.substrate.console_views.producers import _enricher_row

    row = _enricher_row({
        "name": "e", "producer_key": "_enricher:e",
        "status": status, "latest_issue": recorded,
    })

    assert row["workflow"]["latest_issue"] == recorded
    assert row["workflow"]["latest_issue_unavailable_reason"] is None


@pytest.mark.parametrize(
    ("status", "recorded"),
    [
        ("failing", "Latest materialization failed."),
        ("unknown", "No materialization run is recorded."),
        ("healthy", None),
    ],
)
def test_a_view_carries_the_issue_its_own_read_recorded(status, recorded) -> None:
    """`latest_issue` was hard-nulled for a view, under a reason that is false.

    The row carried `latest_issue: None` with
    `latest_issue_unavailable_reason: "view_diagnostics_records_no_issue"`,
    and that composer records one: `_issue` in
    `console_views/view_diagnostics.py` returns "Latest materialization
    failed." for `failing` and "No materialization run is recorded." for
    anything that is not `healthy`. Every one of okg-workspace's eight views
    carries the second, so a console reading this route printed "The run
    recorded no error text" over a read that had said exactly why.

    `healthy` is the one state that really records none, and it keeps a reason
    -- a different one, because the old code's reason claimed the READ records
    no issue rather than that this view has none.
    """
    from okg.substrate.console_views.producers import _view_row

    row = _view_row({
        "name": "v", "producer_key": "view:v",
        "status": status, "latest_issue": recorded,
    })

    assert row["workflow"]["latest_issue"] == recorded
    if recorded is None:
        assert row["workflow"]["latest_issue_unavailable_reason"] == (
            "view_diagnostics_recorded_no_issue_for_this_view"
        )
    else:
        assert row["workflow"]["latest_issue_unavailable_reason"] is None


def _enricher_rollup(monkeypatch, *, source_count: int, node_count: int, edge_count: int):
    """Seed one enricher whose rollup rows carry the `_enricher:` prefix.

    The three dimensions the read joins, and they are not the same domain.
    `source` counts a node by the value in its `source` column, so an
    enricher's own output lands there under `_enricher:<name>`. `enricher_node`
    counts the same rows PLUS the ones attributed through
    `source_record_id->>'linker'`, so it is a superset by construction. Both
    are recorded; neither is a guess.
    """
    import okg.substrate.console_views.console_count_rollup_read as rollup
    from okg.substrate.console_views import enricher_diagnostics as enrichers

    # The enricher composer's flag-off branch does a full-graph CASE scan
    # through a Chronos checkout, which a recording connection cannot serve.
    # The rollup path is the one that ships, and it is the one under test.
    monkeypatch.setattr(enrichers, "console_count_rollup_enabled", lambda: True)
    # The bounded run-history window is a Chronos checkpoint read and needs a
    # real `psycopg` connection (`ChronosGraphStore.from_connection` reads
    # `conn.info`). It contributes nothing to the identity join under test,
    # so it is served empty rather than reached.
    monkeypatch.setattr(
        enrichers, "_read_enricher_run_window", lambda *a, **k: []
    )

    key = "_enricher:code_reference"

    def _read(conn, *, generation_id=None, kinds=None):
        wanted = set(kinds) if kinds else None
        out: dict[tuple[str, str], int] = {}
        if wanted is None or rollup.COUNT_KIND_SOURCE in wanted:
            out[(rollup.COUNT_KIND_SOURCE, key)] = source_count
        if wanted is None or rollup.COUNT_KIND_ENRICHER_NODE in wanted:
            out[(rollup.COUNT_KIND_ENRICHER_NODE, key)] = node_count
        if wanted is None or rollup.COUNT_KIND_ENRICHER_EDGE in wanted:
            out[(rollup.COUNT_KIND_ENRICHER_EDGE, key)] = edge_count
        return out

    monkeypatch.setattr(rollup, "read_console_count_rollup", _read)


def test_an_enricher_is_listed_once_not_as_its_own_source(deployment, monkeypatch):
    """The rollup's `_enricher:` source row is the enricher, not a source.

    `compose_sources` lists every recorded key the registry does not claim, so
    an enricher's own output -- counted in the `source` dimension under
    `_enricher:code_reference` -- arrived as a source row. The enricher read
    then listed the same producer again as `code_reference`. Two rows, one
    `ownership_id`: a duplicate React key in the browser, and a producer the
    operator counts twice.
    """
    dep_dir = deployment(0)
    _enricher_rollup(monkeypatch, source_count=40, node_count=52, edge_count=9)

    payload = _compose(_RecordingConn(), dep_dir)

    keys = [row["ownership_id"] for row in payload["producers"]]
    assert len(keys) == len(set(keys)), f"duplicate ownership_id: {keys}"

    matching = [
        row for row in payload["producers"]
        if row["ownership_id"] == "_enricher:code_reference"
    ]
    assert len(matching) == 1
    row = matching[0]
    assert row["kind"] == "enricher"
    assert row["name"] == "code_reference"


def test_the_folded_source_row_keeps_its_counts_on_the_record(
    deployment, monkeypatch
):
    """Folding the source row must not lose the figure it carried.

    The enricher dimension is the wider one -- it also catches rows attributed
    through `source_record_id->>'linker'` -- so the row's own counts stay the
    enricher's. What the folded `source` row measured is carried beside them
    under its own name, because the two are different questions and this read
    answers neither by overwriting the other.
    """
    dep_dir = deployment(0)
    _enricher_rollup(monkeypatch, source_count=40, node_count=52, edge_count=9)

    payload = _compose(_RecordingConn(), dep_dir)
    row = next(
        r for r in payload["producers"]
        if r["ownership_id"] == "_enricher:code_reference"
    )

    assert row["contribution"]["records_in_gen"] == 52
    folded = row["contribution"]["folded_source_dimension"]
    assert folded["records_in_gen"] == 40
    assert folded["agrees"] is True


def test_a_source_dimension_larger_than_the_enricher_one_is_said_not_hidden(
    deployment, monkeypatch
):
    """Two recorded dimensions that disagree are reported, never reconciled.

    The `source` dimension is a subset of the `enricher_node` dimension by the
    expression that writes them. If it ever measures MORE, the rollup is not
    what this read believes it is, and picking either number would publish a
    figure no read supports.
    """
    dep_dir = deployment(0)
    _enricher_rollup(monkeypatch, source_count=61, node_count=52, edge_count=9)

    payload = _compose(_RecordingConn(), dep_dir)
    row = next(
        r for r in payload["producers"]
        if r["ownership_id"] == "_enricher:code_reference"
    )

    folded = row["contribution"]["folded_source_dimension"]
    assert folded["agrees"] is False
    assert folded["disagreement"] == (
        "source_dimension_exceeds_enricher_dimension"
    )
    # In `count_disagreements`, not `omitted_data`: nothing came back short.
    assert any(
        entry.get("authority") == "console_count_rollup"
        and entry.get("reason") == "source_dimension_exceeds_enricher_dimension"
        for entry in payload["count_disagreements"]
    )


def test_the_react_key_is_unique_by_construction(deployment, monkeypatch):
    """kind plus ownership_id, so a future collision cannot reach the browser.

    The identity join above is the fix. This is the second guard: even if some
    later read hands two rows the same `ownership_id`, the key the table uses
    carries the kind too, so the two rows are still distinct to React.
    """
    dep_dir = deployment(2)
    _enricher_rollup(monkeypatch, source_count=40, node_count=52, edge_count=9)

    payload = _compose(_RecordingConn(), dep_dir)

    row_keys = [row["row_key"] for row in payload["producers"]]
    assert len(row_keys) == len(set(row_keys))
    for row in payload["producers"]:
        assert row["row_key"] == f"{row['kind']}:{row['ownership_id']}"


def test_the_enricher_run_window_is_carried_not_dropped(deployment, monkeypatch):
    """An enricher's runs are a window of versions, and the read says so.

    `compose_enricher_diagnostics` reads the newest N published checkpoints,
    not an enricher's whole history, and serves that N as
    `enricher_runs_window`. Dropping it here left the producer page printing a
    run count and a latest run with no statement of what they were counted
    over -- so an enricher whose last run predates the window read as one that
    never ran.
    """
    dep_dir = deployment(0)
    _enricher_rollup(monkeypatch, source_count=40, node_count=52, edge_count=9)

    payload = _compose(_RecordingConn(), dep_dir)

    from okg.substrate.console_views.enricher_diagnostics import (
        ENRICHER_RUNS_WINDOW,
    )

    assert payload["enricher_runs_window"] == ENRICHER_RUNS_WINDOW
    assert payload["enricher_runs_window_unavailable_reason"] is None


def test_a_failed_enricher_read_reports_no_window_rather_than_a_number(
    deployment, monkeypatch
):
    """No window measured, no window served. The reason says which."""
    dep_dir = deployment(1)

    payload = _compose(_RecordingConn(), dep_dir)

    # The enricher composer reaches a Chronos checkout this connection cannot
    # serve, so the authority is unavailable -- exactly the case that must not
    # produce a figure.
    assert payload["read_authority"]["enrichers"]["status"] == "unavailable"
    assert payload["enricher_runs_window"] is None
    assert payload["enricher_runs_window_unavailable_reason"] == (
        "enricher_diagnostics_did_not_report_a_window"
    )


def test_a_blocked_source_carries_the_code_that_blocked_it(
    deployment, monkeypatch
):
    """`blocked` and its reason are one verdict, and both reach the page.

    `_source_convergence_row` decides a source's state and its reason
    together: `blocked` with `completed_scope_watermark_stale` is a whole
    answer. The producers read carried the state and dropped the code, so the
    producer page could say a source was blocked and could not say by what.
    """
    dep_dir = deployment(1)
    name = "source-000"

    def _runtime(**_):
        return {
            "details": {
                "readiness": {
                    "source_sync_status": {
                        "deployment": DEPLOYMENT,
                        "runtime_liveness": {"sources": {}},
                        "source_freshness": {"sources": {}},
                        "convergence": {
                            "sources": [{
                                "source_name": name,
                                "state": "blocked",
                                "reason": "completed_scope_watermark_stale",
                                "blocking_stage": "planned_action",
                            }],
                        },
                    },
                },
            },
        }

    monkeypatch.setattr(
        "okg.substrate.console_views.source_diagnostics."
        "compose_console_readiness",
        _runtime,
    )

    payload = _compose(_RecordingConn(), dep_dir)
    row = next(r for r in payload["producers"] if r["name"] == name)

    assert row["workflow"]["state"] == "blocked"
    assert row["workflow"]["reason"] == "completed_scope_watermark_stale"
    assert row["workflow"]["blocking_stage"] == "planned_action"
    assert row["workflow"]["reason_unavailable_reason"] is None


def test_a_producer_with_no_convergence_row_says_so_by_name(
    deployment, monkeypatch
):
    """No reason recorded, no reason served -- and the page is told which."""
    dep_dir = deployment(1)

    payload = _compose(_RecordingConn(), dep_dir)
    row = next(r for r in payload["producers"] if r["kind"] == "source")

    assert row["workflow"]["reason"] is None
    assert row["workflow"]["reason_unavailable_reason"] == (
        "convergence_recorded_no_reason_for_this_source"
    )


def test_every_recorded_convergence_reason_has_words(tmp_path):
    """A reason the graph can record must be a sentence the page can print.

    `_source_convergence_row` and its fallbacks assign every `reason` code by
    a literal in `source_sync_convergence.py`. The console maps each one to
    words in `console/model/sources/causes.ts`. An unmapped code falls back to
    its own words, which is honest but unhelpful -- so this holds the two
    files together: a reason added to the graph and not to the console fails
    here rather than reaching an operator as a snake_case token.
    """
    import re
    from pathlib import Path

    import okg.substrate.source_sync_convergence as convergence

    recorded = set(
        re.findall(
            r'"reason":\s*"([a-z][a-z0-9_]*)"|reason\s*=\s*"([a-z][a-z0-9_]*)"',
            Path(str(convergence.__file__)).read_text(encoding="utf-8"),
        )
    )
    codes = {code for pair in recorded for code in pair if code}
    # The three f-string reasons, spelled out: the source sees one suffix per
    # freshness status the row can hold.
    for prefix in ("source_progress_freshness", "completed_scope_watermark"):
        for suffix in ("stale", "missing", "not_evaluated", "fresh"):
            codes.add(f"{prefix}_{suffix}")

    causes = (
        Path("src/okg/operator_ui/web/src/operator-ui/console/model/sources")
        / "causes.ts"
    ).read_text(encoding="utf-8")
    mapped = set(re.findall(r"^  ([a-z][a-z0-9_]*):$", causes, re.MULTILINE))

    assert codes, "the reason scan found nothing; the assignment shape changed"
    assert codes - mapped == set(), (
        "convergence reasons the console cannot put into words: "
        f"{sorted(codes - mapped)}"
    )


def test_a_count_disagreement_is_not_reported_as_a_partial_read(
    deployment, monkeypatch
):
    """Two dimensions that disagree is not "the read came back short".

    `omitted_data` means one thing to the page: a read failed, so something
    is missing and the words on screen stand in for it. The Sources view
    renders every entry as that sentence. A count DISAGREEMENT is the
    opposite -- both figures were read, in full, and they contradict each
    other -- so filing it there made the page say a read had failed when
    none had. It is served under its own key, per producer, with both
    figures.
    """
    dep_dir = deployment(0)
    _enricher_rollup(monkeypatch, source_count=61, node_count=52, edge_count=9)

    payload = _compose(_RecordingConn(), dep_dir)

    assert not [
        entry for entry in payload["omitted_data"]
        if entry.get("reason") == "source_dimension_exceeds_enricher_dimension"
    ], "a disagreement was filed as a partial read"

    disagreements = payload["count_disagreements"]
    assert len(disagreements) == 1
    entry = disagreements[0]
    assert entry["ownership_id"] == "_enricher:code_reference"
    assert entry["name"] == "code_reference"
    assert entry["kind"] == "enricher"
    assert entry["reason"] == "source_dimension_exceeds_enricher_dimension"
    # BOTH figures, so the sentence the page prints can state them.
    assert entry["source_dimension_records"] == 61
    assert entry["enricher_dimension_records"] == 52


def test_counts_that_agree_leave_the_disagreement_list_empty(
    deployment, monkeypatch
):
    """The key is always present, and empty is the ordinary case."""
    dep_dir = deployment(0)
    _enricher_rollup(monkeypatch, source_count=40, node_count=52, edge_count=9)

    payload = _compose(_RecordingConn(), dep_dir)

    assert payload["count_disagreements"] == []


def test_a_configured_source_is_not_folded_into_an_enricher(
    deployment, monkeypatch
):
    """The fold removes a row NO registry declares. Not a declared one.

    `_fold_enricher_source_row` removes the `source`-dimension row an
    enricher's own output produces, on the ground that it is not a source:
    "no registry declares it and nothing ingests through it". That ground
    was asserted and never checked. A deployment whose `source_registry.yaml`
    declares a source with `ownership_id: _enricher:<name>` -- which nothing
    forbids -- had that configured source silently deleted from the one
    table the operator reads as complete.

    The fold now requires the row to be unconfigured. A configured one stays
    listed, and the collision is named on the enricher rather than resolved
    by deleting a declared producer.
    """
    dep_dir = deployment(0)
    _enricher_rollup(monkeypatch, source_count=40, node_count=52, edge_count=9)
    monkeypatch.setattr(
        sources_mod,
        "registry_identities",
        lambda _name: [
            ProducerIdentity(
                name="code_reference_source",
                ownership_id="_enricher:code_reference",
            )
        ],
    )

    payload = _compose(_RecordingConn(), dep_dir)

    listed = {
        (row["kind"], row["ownership_id"]) for row in payload["producers"]
    }
    assert ("source", "_enricher:code_reference") in listed, (
        "a configured source was folded away"
    )
    assert ("enricher", "_enricher:code_reference") in listed

    enricher = next(
        row for row in payload["producers"]
        if row["kind"] == "enricher"
        and row["ownership_id"] == "_enricher:code_reference"
    )
    note = enricher["contribution"]["source_dimension_not_folded"]
    assert note["reason"] == (
        "a_configured_source_of_the_registry_answers_to_this_key"
    )
    assert note["ownership_id"] == "_enricher:code_reference"
    assert "folded_source_dimension" not in enricher["contribution"]
