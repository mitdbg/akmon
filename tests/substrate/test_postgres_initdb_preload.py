"""ops/pg/initdb creates pg_cron only on a server that preloads it.

``CREATE EXTENSION pg_cron`` fails on a fresh volume unless ``pg_cron`` is in
``shared_preload_libraries`` (``unrecognized configuration parameter
"cron.database_name"``). Substrate pg_cron schedules are retired (commit
627f805dd0; DBOS owns runtime schedules), so the okg-workspace dogfood server
does not preload it, and ``ops/pg/initdb/00-extensions.sql`` must skip the
extension there instead of aborting first-time setup. A compose that does
preload pg_cron (``ops/pg/docker-compose.yaml``) must name its own database in
``cron.database_name``, or the guarded create still fails.

The live proof in ``tests/live/postgres_launch`` starts the real image on an
empty data directory; these tests check the script and the compose flags.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main

REPO_ROOT = Path(__file__).resolve().parents[2]
INITDB = REPO_ROOT / "ops/pg/initdb"
# (compose path, service) for every OKG-shipped compose that mounts ops/pg/initdb.
INITDB_COMPOSES = (
    ("deployments/okg-workspace/docker-compose.okg-workspace.dogfood.yaml", "postgres"),
    ("ops/pg/docker-compose.yaml", "db"),
)
_DOLLAR_BODY = re.compile(r"\$\$(.*?)\$\$", re.DOTALL)
_PG_CRON_CREATE = re.compile(r"CREATE\s+EXTENSION\s+(IF\s+NOT\s+EXISTS\s+)?pg_cron", re.I)


def _without_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _unguarded_pg_cron_creates(sql: str) -> list[str]:
    """Name each pg_cron create that does not sit behind the preload check."""
    code = _without_comments(sql)
    problems = []
    for body in _DOLLAR_BODY.findall(code):
        if _PG_CRON_CREATE.search(body) and "current_setting('shared_preload_libraries')" not in body:
            problems.append("DO block creates pg_cron without checking shared_preload_libraries")
    if _PG_CRON_CREATE.search(_DOLLAR_BODY.sub("", code)):
        problems.append("top-level CREATE EXTENSION pg_cron")
    return problems


def _settings(command: list[str]) -> dict[str, str]:
    settings: dict[str, str] = {}
    for flag, value in zip(command, command[1:]):
        if flag == "-c":
            name, _, setting = str(value).partition("=")
            settings[name] = setting
    return settings


def _preloads_pg_cron(service: dict) -> bool:
    settings = _settings([str(part) for part in service["command"]])
    return "pg_cron" in settings.get("shared_preload_libraries", "").split(",")


def _pg_cron_database_problems(service: dict) -> list[str]:
    """A server that preloads pg_cron must name its own database for it."""
    if not _preloads_pg_cron(service):
        return []
    settings = _settings([str(part) for part in service["command"]])
    database = service["environment"]["POSTGRES_DB"]
    if settings.get("cron.database_name") != database:
        return [
            f"cron.database_name={settings.get('cron.database_name')!r}, "
            f"POSTGRES_DB={database!r}"
        ]
    return []


def _mounts_ops_pg_initdb(service: dict, compose_dir: Path) -> bool:
    for volume in service.get("volumes", []):
        source, _, target = str(volume).partition(":")
        if target.startswith("/docker-entrypoint-initdb.d"):
            return (compose_dir / source).resolve() == INITDB.resolve()
    return False


def _rendered_dogfood_postgres(capsys) -> tuple[dict, Path]:
    # The server command does not depend on the deployment, so render a
    # fixture deployment with an explicit database: a render never derives a
    # database name, and the workspace deployment does not ship everywhere.
    rc = main([
        "dogfood",
        "service",
        "render",
        "--deployment",
        "fixture",
        "--postgres-db",
        "okg_fixture",
        "--adapter",
        "docker-compose",
        "--repo-root",
        str(REPO_ROOT),
        "--okg-bin",
        "uv run okg",
        "--json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    service = yaml.safe_load(payload["content"])["services"]["postgres"]
    return service, Path(payload["destination"]).parent


def test_initdb_creates_pg_cron_only_behind_the_preload_check() -> None:
    sql = (INITDB / "00-extensions.sql").read_text(encoding="utf-8")
    # The premise: the script still creates pg_cron somewhere.
    assert _PG_CRON_CREATE.search(_without_comments(sql))
    assert _unguarded_pg_cron_creates(sql) == []
    for path in sorted(INITDB.glob("*.sql")):
        assert _unguarded_pg_cron_creates(path.read_text(encoding="utf-8")) == [], path


def test_the_guard_check_names_an_unguarded_create() -> None:
    """The pre-fix script, and a DO block that skips the check, both fail."""
    assert _unguarded_pg_cron_creates(
        "CREATE EXTENSION IF NOT EXISTS vector;\nCREATE EXTENSION IF NOT EXISTS pg_cron;\n"
    ) == ["top-level CREATE EXTENSION pg_cron"]
    assert _unguarded_pg_cron_creates(
        "DO $$\nBEGIN\n  CREATE EXTENSION IF NOT EXISTS pg_cron;\nEND\n$$;\n"
    ) == ["DO block creates pg_cron without checking shared_preload_libraries"]
    # A comment that mentions the statement is not a statement.
    assert _unguarded_pg_cron_creates("-- CREATE EXTENSION pg_cron;\n") == []


@pytest.mark.parametrize(("relative", "service_name"), INITDB_COMPOSES)
def test_static_compose_names_its_database_when_it_preloads_pg_cron(
    relative: str, service_name: str
) -> None:
    path = REPO_ROOT / relative
    service = yaml.safe_load(path.read_text(encoding="utf-8"))["services"][service_name]
    assert _mounts_ops_pg_initdb(service, path.parent)
    assert _pg_cron_database_problems(service) == []


def test_ops_pg_compose_still_preloads_pg_cron() -> None:
    """ops/pg keeps its pg_cron preload, so the guard still creates it there."""
    path = REPO_ROOT / "ops/pg/docker-compose.yaml"
    service = yaml.safe_load(path.read_text(encoding="utf-8"))["services"]["db"]
    assert _preloads_pg_cron(service)


def test_dogfood_postgres_does_not_preload_pg_cron(capsys) -> None:
    """627f805dd0 took pg_cron out of the dogfood server; keep it out."""
    service, compose_dir = _rendered_dogfood_postgres(capsys)
    assert _mounts_ops_pg_initdb(service, compose_dir)
    assert not _preloads_pg_cron(service)
    settings = _settings([str(part) for part in service["command"]])
    assert "cron.database_name" not in settings


def test_the_database_check_names_a_wrong_cron_database() -> None:
    service = {
        "command": [
            "postgres",
            "-c",
            "shared_preload_libraries=timescaledb,pg_cron,pg_textsearch",
            "-c",
            "cron.database_name=okg",
        ],
        "environment": {"POSTGRES_DB": "okg_workspace"},
    }
    assert _pg_cron_database_problems(service) == [
        "cron.database_name='okg', POSTGRES_DB='okg_workspace'",
    ]
