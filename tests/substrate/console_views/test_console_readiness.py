"""The console's own readiness read runs only the checks the console prints.

`okg dogfood status` is the full readiness composition. On
okg-workspace two of its checks dominate the console's wall time and neither
reaches the Sources page: the Nomos source-lineage report (15.4 s, 14.0 s of
it inside `count_laundered_derived_rows`) and the runtime queue's dead-letter
probe (5.6 s). The Sources page reads three things from readiness -- per-source
sync convergence, source freshness, and the published generation -- so the
console composes those and names every check it declined.

These tests are the guard on that claim. They monkeypatch the two expensive
authorities to raise: if the console read ever reaches one again, the read
fails loudly here instead of costing twenty seconds in a browser.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from okg.substrate.console_views import console_readiness


class _Cursor:
    def __init__(self) -> None:
        self.description = None

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def execute(self, *args: Any, **kwargs: Any) -> None:
        return None

    def fetchall(self) -> list[Any]:
        return []

    def fetchone(self) -> Any:
        return None


class _Conn:
    def cursor(self, *args: Any, **kwargs: Any) -> _Cursor:
        return _Cursor()

    def __enter__(self) -> "_Conn":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


@pytest.fixture()
def bounded(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stub every probe the console read is allowed to run, and only those."""
    called: dict[str, Any] = {}

    monkeypatch.setattr(console_readiness, "_connect", lambda dsn: _Conn())
    monkeypatch.setattr(
        console_readiness, "_statement_timeout", lambda conn, seconds: _Conn()
    )

    def _generation(
        conn: Any, deployment: str, session: Any = None
    ) -> dict[str, Any]:
        called["generation"] = True
        if session is not None:
            called["generation_session"] = session
        return {"status": "published", "generation_id": "gen-1"}

    def _mcp(conn: Any, deployment: str, session: Any = None) -> dict[str, Any]:
        called["mcp"] = True
        if session is not None:
            called["mcp_session"] = session
        return {"ok": True, "generation_id": "gen-1"}

    def _freshness(conn: Any, **kwargs: Any) -> dict[str, Any]:
        called["freshness"] = True
        return {"ok": True, "missing": [], "stale": [], "groups": {}}

    def _liveness(**kwargs: Any) -> dict[str, Any]:
        called["liveness"] = True
        return {"ok": True, "deployment": kwargs.get("deployment") or ""}

    monkeypatch.setattr(console_readiness, "_generation_status", _generation)
    monkeypatch.setattr(console_readiness, "_mcp_status", _mcp)
    monkeypatch.setattr(console_readiness, "_source_freshness_status", _freshness)
    monkeypatch.setattr(
        console_readiness, "_source_sync_liveness_status", _liveness
    )
    monkeypatch.setattr(
        console_readiness,
        "_source_groups_for_profile",
        lambda *a, **k: {"agent_context": ("git",)},
    )
    monkeypatch.setattr(console_readiness, "_source_aliases", lambda p: {})
    monkeypatch.setattr(
        console_readiness, "_progress_freshness_sources", lambda p, profile: set()
    )
    return called


def _compose(tmp_path: Path) -> dict[str, Any]:
    return console_readiness.compose_console_readiness(
        deployment_name="okg-workspace",
        deployment_dir=tmp_path,
        dsn="postgresql://redacted/redacted",
    )


def test_console_read_never_reaches_the_source_lineage_report(
    bounded: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The 15.4 s Nomos source-lineage report is not on the console path."""
    from okg.substrate.cli import deployment_readiness as ready

    def _explode(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(
            "the console readiness read reached the source-lineage report"
        )

    monkeypatch.setattr(
        ready, "_ready_nomos_source_lineage_status", _explode, raising=False
    )

    payload = _compose(tmp_path)

    assert payload["basis"] == console_readiness.CONSOLE_READINESS_BASIS
    assert "nomos.source_lineage" in payload["checks_not_run"]


def test_console_read_never_runs_the_dead_letter_probe(
    bounded: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The 5.6 s runtime-queue/DLQ probe is not on the console path."""
    from okg.substrate.cli import deployment_readiness as ready

    def _explode(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(
            "the console readiness read reached the runtime queue probe"
        )

    monkeypatch.setattr(ready, "_ready_queue_status", _explode, raising=False)

    payload = _compose(tmp_path)

    assert "queues.bounds" in payload["checks_not_run"]
    assert "queues.dlq" in payload["checks_not_run"]


def test_checks_not_run_names_every_declined_check(
    bounded: dict[str, Any], tmp_path: Path
) -> None:
    """Every declined check is named, and the count the page prints matches."""
    payload = _compose(tmp_path)

    declined = payload["checks_not_run"]
    assert declined == sorted(declined), "declined checks are served sorted"
    assert payload["checks_not_run_count"] == len(declined)
    for check in (
        "nomos.source_lineage",
        "queues.dlq",
        "deployment.code_drift",
        "doctor.deployment",
        "sources.optimization_budget",
    ):
        assert check in declined
    for check in payload["checks_run"]:
        assert check not in declined


def test_the_console_read_still_carries_what_the_sources_page_prints(
    bounded: dict[str, Any], tmp_path: Path
) -> None:
    """Sync convergence, freshness and the generation survive the narrowing."""
    payload = _compose(tmp_path)

    readiness = payload["details"]["readiness"]
    assert readiness["generation"]["status"] == "published"
    assert readiness["source_freshness"]["ok"] is True
    assert "source_sync_status" in readiness
    assert bounded == {
        "generation": True,
        "mcp": True,
        "freshness": True,
        "liveness": True,
    }


def test_the_page_session_reaches_the_generation_and_mcp_checks(
    bounded: dict[str, Any], tmp_path: Path
) -> None:
    """The pinned session is handed to both checks, so neither re-finds the
    latest generation (6.3 s and 7.8 s on okg-workspace, issue #2546)."""
    pinned = object()
    console_readiness.compose_console_readiness(
        deployment_name="okg-workspace",
        deployment_dir=tmp_path,
        dsn="postgresql://redacted/redacted",
        session=pinned,
    )
    assert bounded["generation_session"] is pinned
    assert bounded["mcp_session"] is pinned


def test_a_probe_that_fails_is_named_not_swallowed(
    bounded: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed probe reads `unavailable` by name; it never passes by omission."""

    def _boom(conn: Any, deployment: str, session: Any = None) -> dict[str, Any]:
        raise RuntimeError("password=hunter2 host=db.internal")

    monkeypatch.setattr(console_readiness, "_generation_status", _boom)

    payload = _compose(tmp_path)

    authority = payload["read_authority"]["generation"]
    assert authority["status"] == "unavailable"
    assert authority["error_type"] == "RuntimeError"
    assert "hunter2" not in repr(payload)
    assert "db.internal" not in repr(payload)


def _ids_the_full_run_emits() -> set[str]:
    """Every check id the full readiness run writes, from its call sites.

    TWO call shapes, because `_ready_add_check` takes the id positionally in
    most places and by keyword in thirteen. Scanning only the positional form
    missed ten ids -- every `<check>.unavailable` marker the run writes when a
    check cannot be reached, plus `generation.unavailable` -- so the guard
    below passed over exactly the ids that are easiest to add and hardest to
    notice.
    """
    import re
    from pathlib import Path

    import okg.substrate.cli.deployment as deployment_module
    import okg.substrate.cli.deployment_readiness as readiness_module

    ident = r'"([a-z_]+\.[a-z_.]+)"'
    shapes = (
        re.compile(r'_ready_add_check\(\s*\w+,\s*' + ident),
        re.compile(r'check_id\s*=\s*' + ident),
    )
    emitted: set[str] = set()
    for module in (deployment_module, readiness_module):
        source = Path(str(module.__file__)).read_text(encoding="utf-8")
        for pattern in shapes:
            emitted.update(pattern.findall(source))
    # None of these is a check of the deployment: they are the outer run's own
    # failure and skip markers.
    return emitted - {
        "database.connectivity",
        "database.dependent_checks",
        "database.live_checks",
        "deployment.apply",
        "env.authority",
        "env.resolved_dsn",
    }


def test_no_check_of_the_full_run_is_unaccounted_for() -> None:
    """Every id the full run emits is run here, declined, or not a check.

    The console's lists are pinned, not derived -- the full run declares its
    checks by calling `_ready_add_check` inline, so there is no registry to
    read. This holds the pinned lists against the call sites, in BOTH
    directions: an id added to readiness and not classified here fails, and
    an id classified here that the run no longer emits fails too. One
    direction alone let the pinned list keep naming a check that had been
    deleted, which the page would have gone on counting.
    """
    emitted = _ids_the_full_run_emits()
    assert emitted, "the check-id scan found nothing; the call shape changed"

    classified = (
        set(console_readiness.CHECKS_RUN)
        | set(console_readiness.CHECKS_NOT_RUN)
        | set(console_readiness.IDS_THAT_ARE_NOT_CHECKS)
    )
    assert emitted - classified == set(), (
        "a readiness id is neither run by the console, named as declined, nor "
        "named as one of the run's own not-a-check markers"
    )
    assert classified - emitted == set(), (
        "a pinned readiness id is no longer emitted by the full run"
    )


def test_the_keyword_call_shape_is_scanned_too() -> None:
    """The ten ids only the keyword form declares are really in the scan.

    Named one by one rather than counted, because the point of the guard is
    that a shape it cannot see is a shape it cannot hold: a count would have
    gone on passing if the regex silently matched ten other things.
    """
    emitted = _ids_the_full_run_emits()
    for check_id in (
        "deployment.writer_driver.unavailable",
        "doctor.okg_workspace.unavailable",
        "generation.unavailable",
        "mcp.graph_read.unavailable",
        "nomos.read_path.unavailable",
        "nomos.source_lineage.unavailable",
        "sources.freshness.unavailable",
        "sources.optimization_budget.unavailable",
        "sources.sync_liveness.unavailable",
        "sources.sync_status.unavailable",
    ):
        assert check_id in emitted, f"{check_id} is not in the scan"


def test_the_printed_count_counts_checks_and_not_markers() -> None:
    """`checks_not_run_count` is what the page prints as "N further checks".

    So it counts checks a reader could go and run, and not the marker the
    full run substitutes for a check it could not reach. The page runs
    `sources.freshness` itself; counting `sources.freshness.unavailable`
    there would have told the reader otherwise.
    """
    markers = set(console_readiness.IDS_THAT_ARE_NOT_CHECKS)
    assert not markers & set(console_readiness.CHECKS_NOT_RUN)
    assert not markers & set(console_readiness.CHECKS_RUN)
    for run in console_readiness.CHECKS_RUN:
        assert f"{run}.unavailable" not in console_readiness.CHECKS_NOT_RUN


# --- the MCP sample verdict is reused within its inputs (issue #2547) --------


class _SampleWorld:
    """Doubles for everything the MCP sample verdict depends on.

    `filtered` counts Nomos filter passes: each one is the 300 policy
    evaluations the console should pay once per set of inputs, not per read.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from okg.substrate import graph_store, nomos
        from okg.substrate.cli import deployment_readiness as readiness
        from okg.substrate.mcp import principal_readiness
        from okg.substrate.nomos import read_filter, runtime_enforcement, store

        self.filtered = 0
        self.visible = 2
        self.fingerprint: str | None = "policy-a"
        self.mode = "enforce"
        self.clock = 1000.0
        world = self

        class _Checkout:
            def query(self, sql: str) -> list[dict[str, Any]]:
                return [{"node_id": f"n{i}"} for i in range(3)]

        class _Store:
            def checkout_generation(self, generation_id: str, *, branch: str) -> Any:
                return _Checkout()

        def _filter(conn: Any, session: Any, rows: Any, **kwargs: Any) -> list[Any]:
            assert kwargs["audit"] is True
            world.filtered += 1
            return list(rows)[: world.visible]

        def _bundle(conn: Any, *, deployment_name: str, scope: str) -> Any:
            assert scope == f"deployment:{deployment_name}"
            if world.fingerprint is None:
                return None
            return type("_Bundle", (), {"policy_fingerprint": world.fingerprint})()

        monkeypatch.setattr(
            readiness, "_ready_mcp_principal_login_status", lambda c: None
        )
        monkeypatch.setattr(
            principal_readiness, "offline_principal_readiness", lambda c, d: {}
        )
        monkeypatch.setattr(nomos, "deployment_requires_nomos_runtime", lambda d: True)
        monkeypatch.setattr(
            graph_store.ChronosGraphStore,
            "from_connection",
            classmethod(lambda cls, conn, ensure_schema=False: _Store()),
        )
        monkeypatch.setattr(read_filter, "filter_node_rows_for_session", _filter)
        monkeypatch.setattr(store, "load_active_policy_bundle", _bundle)
        monkeypatch.setattr(
            runtime_enforcement,
            "deployment_nomos_enforcement_mode",
            lambda d: world.mode,
        )
        self.identity = ("1:providers-a", "2:bindings-a", "3:grants-a")
        self.identity_reads = 0

        def _identity(conn: Any, deployment_name: str) -> tuple[Any, ...]:
            world.identity_reads += 1
            return world.identity

        monkeypatch.setattr(
            readiness.McpSampleVerdicts, "identity_state", staticmethod(_identity)
        )
        self.verdicts = readiness.McpSampleVerdicts(clock=lambda: world.clock)
        monkeypatch.setattr(console_readiness, "_MCP_SAMPLE_VERDICTS", self.verdicts)


class _InfoConn:
    info = type("_Info", (), {"host": "127.0.0.1", "port": 5432, "dbname": "okg"})()


def _session(generation: str = "gen-1", **extra: Any) -> Any:
    from types import SimpleNamespace

    return SimpleNamespace(
        deployment_name="okg-workspace",
        generation_id=generation,
        branch_name="default",
        catalog_version_id=7,
        compat_generation_id=1,
        **extra,
    )


_NOT_THE_VERDICT = {"verdict_reuse", "principal_readiness", "probe_scope"}


def test_a_passing_sample_verdict_is_reused_within_one_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _SampleWorld(monkeypatch)
    first = console_readiness._mcp_status(_InfoConn(), "okg-workspace", _session())
    second = console_readiness._mcp_status(_InfoConn(), "okg-workspace", _session())

    assert world.filtered == 1
    # The identity rows are read on every request, the reusing one included.
    assert world.identity_reads == 2
    assert first["ok"] is True and second["ok"] is True
    assert first["verdict_reuse"]["status"] == "computed"
    assert second["verdict_reuse"]["status"] == "reused"
    # The reused verdict is the measured one, field for field.
    assert {k: v for k, v in first.items() if k not in _NOT_THE_VERDICT} == {
        k: v for k, v in second.items() if k not in _NOT_THE_VERDICT
    }
    assert second["visible_sample_count"] == 2
    # The principal checks are not part of the kept verdict: they ran again.
    assert "principal_readiness" in second


def test_a_new_generation_recomputes_the_sample_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    world = _SampleWorld(monkeypatch)
    console_readiness._mcp_status(_InfoConn(), "okg-workspace", _session("gen-1"))
    out = console_readiness._mcp_status(
        _InfoConn(), "okg-workspace", _session("gen-2")
    )

    assert world.filtered == 2
    assert out["verdict_reuse"]["status"] == "computed"
    assert out["pinned_generation_id"] == "gen-2"


@pytest.mark.parametrize(
    "change", ["policy", "no_policy", "binding", "grant", "provider", "mode", "expired"]
)
def test_a_changed_input_recomputes_the_sample_verdict(
    monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    world = _SampleWorld(monkeypatch)
    console_readiness._mcp_status(_InfoConn(), "okg-workspace", _session())
    if change == "policy":
        world.fingerprint = "policy-b"
    elif change == "no_policy":
        world.fingerprint = None
    elif change == "binding":
        # A revoked subject binding: same count, different row content.
        world.identity = (world.identity[0], "2:bindings-revoked", world.identity[2])
    elif change == "grant":
        world.identity = (world.identity[0], world.identity[1], "2:grants-b")
    elif change == "provider":
        world.identity = ("1:providers-b", world.identity[1], world.identity[2])
    elif change == "mode":
        world.mode = "audit"
    else:
        world.clock += world.verdicts.max_age_seconds + 1
    out = console_readiness._mcp_status(_InfoConn(), "okg-workspace", _session())

    assert world.filtered == 2
    assert out["verdict_reuse"]["status"] == "computed"


def test_a_blocker_is_never_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    """A sample with nothing visible is a blocker, and is measured again."""
    world = _SampleWorld(monkeypatch)
    world.visible = 0
    first = console_readiness._mcp_status(_InfoConn(), "okg-workspace", _session())
    second = console_readiness._mcp_status(_InfoConn(), "okg-workspace", _session())

    assert first["ok"] is False and second["ok"] is False
    assert world.filtered == 2
    assert second["verdict_reuse"]["status"] == "computed"


@pytest.mark.parametrize(
    "extra",
    [
        {"nomos_identity_claim": {"sub": "x"}},
        {"nomos_access_context": {"purpose": "support"}},
        {"nomos_multi_user": True},
    ],
)
def test_an_identity_bound_session_is_never_served_a_kept_verdict(
    monkeypatch: pytest.MonkeyPatch, extra: dict[str, Any]
) -> None:
    world = _SampleWorld(monkeypatch)
    console_readiness._mcp_status(_InfoConn(), "okg-workspace", _session())
    out = console_readiness._mcp_status(
        _InfoConn(), "okg-workspace", _session(**extra)
    )

    assert world.filtered == 2
    assert out["verdict_reuse"] == {"status": "not_reusable_session"}


def test_the_cli_probe_without_a_store_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`okg deployment ready` passes no store: every call measures, no marker."""
    from okg.substrate.cli import deployment_readiness as readiness

    world = _SampleWorld(monkeypatch)
    first = readiness._ready_mcp_status(
        _InfoConn(), "okg-workspace", session=_session()
    )
    second = readiness._ready_mcp_status(
        _InfoConn(), "okg-workspace", session=_session()
    )

    assert world.filtered == 2
    assert "verdict_reuse" not in first and "verdict_reuse" not in second
    assert first["ok"] is True


def test_the_payload_says_whether_the_sample_was_reused(
    bounded: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reuse marker reaches the served `read_authority`; a probe result
    without one (the CLI shape) adds nothing there."""
    assert _compose(tmp_path)["read_authority"]["mcp"] == {"status": "available"}

    marker = {"status": "reused", "age_seconds": 4.2, "max_age_seconds": 300.0}
    monkeypatch.setattr(
        console_readiness,
        "_mcp_status",
        lambda conn, deployment, session=None: {"ok": True, "verdict_reuse": marker},
    )
    assert _compose(tmp_path)["read_authority"]["mcp"] == {
        "status": "available",
        "verdict_reuse": marker,
    }


def test_a_failing_policy_read_is_a_named_blocker_not_a_reuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The key reads the active policy live; if that read fails, the probe
    reports the failure as the unkeyed probe would, and serves nothing kept."""
    from okg.substrate.nomos import store

    world = _SampleWorld(monkeypatch)
    console_readiness._mcp_status(_InfoConn(), "okg-workspace", _session())

    def _boom(conn: Any, **kwargs: Any) -> Any:
        raise RuntimeError("policy table unreadable")

    monkeypatch.setattr(store, "load_active_policy_bundle", _boom)
    out = console_readiness._mcp_status(_InfoConn(), "okg-workspace", _session())

    assert out["ok"] is False
    assert out["error"] == "RuntimeError"
    assert world.filtered == 1


class _IdentityCursor:
    def __init__(self, row: Any) -> None:
        self.row = row
        self.executed: list[tuple[str, Any]] = []

    def __enter__(self) -> "_IdentityCursor":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))

    def fetchone(self) -> Any:
        return self.row


def test_the_identity_state_reads_all_three_tables_for_the_deployment_scope() -> None:
    """The key's identity marker digests providers, bindings and grants, every
    row and status, under the scope the read filter evaluates in."""
    from okg.substrate.cli.deployment_readiness import McpSampleVerdicts

    cursor = _IdentityCursor(("0:p", "1:b", "2:g"))
    conn = type("_C", (), {"cursor": lambda self: cursor})()

    assert McpSampleVerdicts.identity_state(conn, "okg-workspace") == (
        "0:p",
        "1:b",
        "2:g",
    )
    sql, params = cursor.executed[0]
    for table in (
        "okg.nomos_identity_providers",
        "okg.nomos_subject_bindings",
        "okg.nomos_permission_grants",
    ):
        assert table in sql
    # Whole rows, not only the active ones: a status flip changes the digest.
    assert "status" not in sql
    assert params == {"deployment": "okg-workspace", "scope": "deployment:okg-workspace"}


def test_an_unreadable_identity_state_refuses_rather_than_reusing() -> None:
    from okg.substrate.cli.deployment_readiness import McpSampleVerdicts

    conn = type("_C", (), {"cursor": lambda self: _IdentityCursor(None)})()
    with pytest.raises(RuntimeError, match="identity state"):
        McpSampleVerdicts.identity_state(conn, "okg-workspace")


def test_the_probe_subject_is_part_of_the_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from okg.substrate.cli import deployment_readiness as readiness

    _SampleWorld(monkeypatch)
    key = readiness.McpSampleVerdicts.key(_InfoConn(), _session())
    assert "subject:mcp_client" in key
