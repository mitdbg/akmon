"""The TimescaleDB worker-headroom readiness check (#3143): unit tests.

The arithmetic and the judgement run on plain values; the probe runs against a
scripted cursor. ``test_timescaledb_workers_pg.py`` runs it on a real server
and through the readiness entry point.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.db import timescaledb_workers as tw

REPO_ROOT = Path(__file__).resolve().parents[3]
PRELOAD = "timescaledb,pg_cron,pg_textsearch"


def _judge(**overrides: Any) -> tuple[str, str, dict]:
    values: dict[str, Any] = {
        "max_worker_processes": 32,
        "max_parallel_workers": 8,
        "timescaledb_workers": 16,
        "launchers": 3,
        "schedulers": 6,
        "own_scheduler": True,
        "failed_runs_last_hour": 0,
        "history_bytes": 8192,
    }
    values.update(overrides)
    return tw.evaluate(**values)


@pytest.mark.parametrize(
    ("preload", "logical", "expected"),
    [
        (PRELOAD, 4, 3),
        ("timescaledb,pg_textsearch", 4, 2),
        ("timescaledb", 0, 1),
        ('"timescaledb", $libdir/pg_cron.so', 4, 3),
        ("my_pg_cron,pg_cronx", 0, 0),
        ("", 4, 1),
    ],
)
def test_launchers_come_from_the_settings(preload: str, logical: int, expected: int) -> None:
    assert tw.launcher_count(preload, logical) == expected


def test_the_old_eight_slot_image_is_flagged() -> None:
    level, message, details = _judge(max_worker_processes=8)
    assert level == "warning"
    assert details["required_worker_slots"] == 27  # 3 + 8 + 16
    assert details["worker_slot_headroom"] == -19
    assert "set it to at least 27" in message


def test_thirty_two_slots_pass() -> None:
    level, _, details = _judge()
    assert (level, details["status"], details["worker_slot_headroom"]) == ("pass", "ok", 5)


@pytest.mark.parametrize(("mwp", "level"), [(26, "warning"), (27, "pass")])
def test_headroom_edge(mwp: int, level: str) -> None:
    assert _judge(max_worker_processes=mwp)[0] == level


def test_pg_cron_launcher_counts_toward_the_edge() -> None:
    without_cron = tw.launcher_count("timescaledb", 4)
    with_cron = tw.launcher_count(PRELOAD, 4)
    assert _judge(max_worker_processes=26, launchers=without_cron)[0] == "pass"
    assert _judge(max_worker_processes=26, launchers=with_cron)[0] == "warning"


@pytest.mark.parametrize(("schedulers", "level"), [(14, "pass"), (15, "warning")])
def test_scheduler_cap_edge(schedulers: int, level: str) -> None:
    level_seen, message, _ = _judge(schedulers=schedulers)
    assert level_seen == level
    if level == "warning":
        assert "15 TimescaleDB schedulers are running" in message


def test_many_databases_with_few_schedulers_pass() -> None:
    # A shared host with 24 databases, 3 of them with TimescaleDB: only the
    # running schedulers count, so nothing is flagged.
    assert _judge(schedulers=3)[0] == "pass"


def test_a_database_without_its_scheduler_is_flagged() -> None:
    level, message, _ = _judge(own_scheduler=False)
    assert level == "warning"
    assert "no running scheduler" in message


@pytest.mark.parametrize(
    ("failed", "level"),
    [(tw.FAILED_RUNS_WARN_PER_HOUR - 1, "pass"), (tw.FAILED_RUNS_WARN_PER_HOUR, "warning")],
)
def test_failed_run_edge(failed: int, level: str) -> None:
    assert _judge(failed_runs_last_hour=failed)[0] == level


@pytest.mark.parametrize(
    ("size", "level"),
    [(tw.HISTORY_BYTES_WARN - 1, "pass"), (tw.HISTORY_BYTES_WARN, "warning")],
)
def test_history_size_edge(size: int, level: str) -> None:
    level_seen, message, _ = _judge(history_bytes=size)
    assert level_seen == level
    if level == "warning":
        assert "DELETE its rows WHERE succeeded IS FALSE" in message


def test_an_unread_part_is_a_warning_but_the_rest_is_still_judged() -> None:
    level, message, details = _judge(
        max_worker_processes=8, failed_runs_last_hour=None,
        unreadable=("failed job runs (no visibility)",),
    )
    assert level == "warning"
    assert details["status"] == "warning"
    assert details["worker_slot_headroom"] == -19
    assert "could not read failed job runs" in message
    level, _, details = _judge(launchers=None, unreadable=("settings",))
    assert (level, details["status"]) == ("warning", "unreadable")
    assert details["required_worker_slots"] is None


# --- the probe against a scripted connection ---------------------------------


class _Cursor:
    def __init__(self, script: list[Any]) -> None:
        self._script = script
        self._row: Any = None

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        result = self._script.pop(0)
        if isinstance(result, BaseException):
            raise result
        self._row = result

    def fetchone(self) -> Any:
        return self._row


class _Conn:
    def __init__(self, script: list[Any]) -> None:
        self.script = script

    def cursor(self) -> _Cursor:
        return _Cursor(self.script)


BASIC = (8, 8, 16, 4)
DENIED = PermissionError('permission denied to examine "shared_preload_libraries"\nDETAIL: x')


def test_probe_reports_the_old_image_and_a_failure_loop() -> None:
    conn = _Conn([(True,), BASIC, (PRELOAD,), (True, 5, False), (647168, True), (596,)])
    level, message, details = tw.timescaledb_worker_health(conn)
    assert conn.script == []
    assert level == "warning"
    assert (details["launchers"], details["running_schedulers"]) == (3, 5)
    assert "no running scheduler" in message
    assert "596 TimescaleDB job runs failed" in message


def test_probe_without_timescaledb_is_skipped() -> None:
    level, _, details = tw.timescaledb_worker_health(_Conn([(False,)]))
    assert (level, details["status"]) == ("skipped", "not_applicable")


def test_probe_for_a_plain_role_reads_what_it_can() -> None:
    conn = _Conn([(True,), BASIC, DENIED, (False, 0, False), (98304, False)])
    level, message, details = tw.timescaledb_worker_health(conn)
    assert conn.script == []
    assert (level, details["status"]) == ("warning", "unreadable")
    assert details["launchers"] is None
    assert details["running_schedulers"] is None
    assert details["failed_runs_last_hour"] is None
    assert details["history_bytes"] == 98304
    assert message.count(tw.PRIVILEGE_HINT) == 2
    assert "DETAIL" not in message


def test_headroom_survives_a_failed_history_visibility_check() -> None:
    conn = _Conn([(True,), BASIC, (PRELOAD,), (True, 5, True), (8192, False)])
    level, message, details = tw.timescaledb_worker_health(conn)
    assert details["worker_slot_headroom"] == -19
    assert "set it to at least 27" in message
    assert "cannot see every row" in message


def test_probe_without_the_history_table_says_so() -> None:
    conn = _Conn([(True,), BASIC, (PRELOAD,), (True, 6, True), (None, True), (0,)])
    level, message, details = tw.timescaledb_worker_health(conn)
    assert level == "warning"
    assert "(not found)" in message


def test_probe_with_unreadable_basic_settings_is_unreadable() -> None:
    conn = _Conn([(True,), RuntimeError("boom")])
    level, message, details = tw.timescaledb_worker_health(conn)
    assert (level, details["status"]) == ("warning", "unreadable")
    assert "RuntimeError: boom" in message


# --- the shipped configuration --------------------------------------------------


OPS_PG_COMPOSE = (
    "ops/pg/docker-compose.yaml",
    "ops/pg/docker-compose.bench.yaml",
    "ops/pg/docker-compose.golden.yaml",
    "src/okg/substrate/library/profiles/academic-researcher/compose.yaml",
)


def _setting(command: list[str], name: str) -> str | None:
    found = None
    for flag, value in zip(command, command[1:]):
        if flag == "-c" and value.startswith(f"{name}="):
            found = value.split("=", 1)[1]
    return found


@pytest.mark.parametrize("relative", OPS_PG_COMPOSE)
def test_every_compose_stack_on_the_local_image_sets_worker_slots(
    relative: str,
) -> None:
    document = yaml.safe_load((REPO_ROOT / relative).read_text(encoding="utf-8"))
    services = [
        service for service in document["services"].values()
        if "command" in service
    ]
    assert services, f"{relative} has no service with a postgres command"
    for service in services:
        command = [str(part) for part in service["command"]]
        assert _setting(command, "max_worker_processes") == str(
            tw.RECOMMENDED_MAX_WORKER_PROCESSES
        )


def test_the_required_ci_database_gets_the_image_default() -> None:
    """The required-CI compose file keeps its command equal to the ARC CI
    sidecar's (tests/ci/test_arc_image_warm.py), so it passes no
    max_worker_processes flag. It still gets 32: it builds this image, starts
    every cluster on a fresh volume, and mounts ops/pg/initdb, so initdb
    copies the Dockerfile's postgresql.conf.sample default.
    """
    service = yaml.safe_load(
        (REPO_ROOT / "ops/pg/docker-compose.required-ci.yaml").read_text(
            encoding="utf-8"
        )
    )["services"]["db"]
    command = [str(part) for part in service["command"]]
    assert _setting(command, "max_worker_processes") is None
    assert service["build"]["context"] == "."
    assert "./initdb:/docker-entrypoint-initdb.d:ro" in service["volumes"]


def test_the_image_default_and_the_ops_environments_agree() -> None:
    dockerfile = (REPO_ROOT / "ops/pg/Dockerfile").read_text(encoding="utf-8")
    assert (
        f"max_worker_processes = {tw.RECOMMENDED_MAX_WORKER_PROCESSES}"
        in dockerfile
    )
    values = {
        yaml.safe_load(path.read_text(encoding="utf-8"))["max_worker_processes"]
        for path in (REPO_ROOT / "ops/environments").glob(
            "*/postgres-capacity.yaml"
        )
    }
    assert min(values) == tw.RECOMMENDED_MAX_WORKER_PROCESSES
