"""Served text per branch against a real database (issue #4280, PACT akb-surface-text).

The probe-mode tests in `test_surface_text.py` build a server with no database.
This one publishes a real generation, creates a real experiment branch with
`okg branch create`, and builds the server twice against that database: on
`default` and with `branch_name="trial"`. Each is read the way a client reads
it: a refusal through `call_tool`, the read-side resource through
`read_resource`, and that resource through the replay runner's own
`read_surface_text`, which is what goes into a replay's setup record.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import subprocess
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.substrate.cli.main import main  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.mcp.server import build_server  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate._db_helpers import seed_catalog_version  # noqa: E402
from tests.substrate.mcp._result_payload import (  # noqa: E402
    call_json,
    json_client_sessions,
    payload as _json_payload,
)

pytestmark = [pytest.mark.postgres, pytest.mark.serial_db]

DEPLOYMENT = "surfacetext"
BRANCH = "trial"
HINT = "TRIAL BRANCH HINT: {default}"

SURFACE_TEXT = f"""\
branches:
  {BRANCH}:
    fix_hints:
      invalid_operator_arguments: "{HINT}"
"""


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch) -> None:
    json_client_sessions(monkeypatch)


def _write_deployment(root: Path) -> Path:
    dep = root / DEPLOYMENT
    (dep / "schemas").mkdir(parents=True)
    (dep / "docs").mkdir()
    (dep / "source_registry.yaml").write_text("", encoding="utf-8")
    (dep / "docs" / "mcp-instructions.md").write_text("Search the deployment.", encoding="utf-8")
    (dep / "surface_text.yaml").write_text(SURFACE_TEXT, encoding="utf-8")
    (dep / "deployment.yaml").write_text(
        f"""
name: {DEPLOYMENT}
description: test
schema_dir: schemas
source_registry: source_registry.yaml
agent_surface:
  served: [inspect, search, expand, filter, map, aggregate]
  instructions: docs/mcp-instructions.md
  surface_text: surface_text.yaml
""".lstrip(),
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "-q", str(dep)], check=True)
    return dep


def _publish(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact("chunk:a", "document_chunk", {"text": "beam calibration"}, {"id": "chunk:a"}, {"v": 1})],
            source="default",
        )
    result = Publisher(dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT)).cycle()
    assert result["status"] == "published", result
    return str(result["generation_id"])


@pytest.fixture
def published(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> Iterator[tuple[str, str]]:
    deployments = tmp_path / "deployments"
    _write_deployment(deployments)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments))
    generation_id = _publish(clean_runtime)
    assert main([
        "--json", "branch", "create", BRANCH, "--role", "experiment",
        "--deployment", DEPLOYMENT, "--dsn", clean_runtime,
    ]) == 0, capsys.readouterr()
    capsys.readouterr()
    monkeypatch.chdir("/")
    yield clean_runtime, generation_id


class _InProcessSession:
    """The replay runner's session protocol over an in-process server."""

    def __init__(self, mcp: Any) -> None:
        self._mcp = mcp
        self.instructions = mcp._mcp_server.instructions

    async def read_resource(self, uri: str) -> str:
        contents = list(await self._mcp.read_resource(uri))
        assert len(contents) == 1, contents
        return contents[0].content


def _connector(mcp: Any):
    @contextlib.asynccontextmanager
    async def connect(_url: str, _headers: Any, _timeout: float) -> AsyncIterator[_InProcessSession]:
        yield _InProcessSession(mcp)

    return connect


def _read_side(mcp: Any) -> dict[str, Any]:
    contents = list(asyncio.run(mcp.read_resource(f"okg://readside-metadata/{DEPLOYMENT}")))
    return json.loads(contents[0].content)


def _refusal_hint(mcp: Any) -> str:
    async def call() -> Any:
        return await call_json(mcp, "search", {"query": "beam", "max_bytes": "8192"})

    result = asyncio.run(call())
    body = _json_payload(result)
    assert result.isError and body["error"]["code"] == "invalid_operator_arguments", body
    return body["error"]["fix_hint"]


def test_a_branch_layer_is_served_on_the_branch_and_recorded_by_the_runner(
    published: tuple[str, str],
) -> None:
    from okg.optimizer.replay.serve import read_surface_text

    dsn, _generation = published
    servers = {
        "default": build_server(dsn=dsn, deployment_name=DEPLOYMENT),
        BRANCH: build_server(dsn=dsn, deployment_name=DEPLOYMENT, branch_name=BRANCH),
    }
    try:
        hints = {name: _refusal_hint(mcp) for name, mcp in servers.items()}
        reports = {name: _read_side(mcp)["surface_text"] for name, mcp in servers.items()}
        recorded = {
            name: asyncio.run(read_surface_text(_connector(mcp), "in-process", {}, DEPLOYMENT))
            for name, mcp in servers.items()
        }
    finally:
        for mcp in servers.values():
            mcp._okg_pool.close()

    # The branch serves the changed hint; default serves the code's own.
    assert not hints["default"].startswith("TRIAL BRANCH HINT")
    assert hints[BRANCH] == HINT.format(default=hints["default"])
    assert reports["default"]["branch_layer"] is None
    assert reports[BRANCH]["branch_layer"] == BRANCH
    assert reports[BRANCH]["overridden"]["fix_hints"] == ["invalid_operator_arguments"]
    # What a replay records: two different digests, each the one the server reports.
    assert recorded == {name: report["digest"] for name, report in reports.items()}
    assert recorded["default"] != recorded[BRANCH]
