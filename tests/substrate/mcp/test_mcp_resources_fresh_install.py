"""PACT mcp-resources-fresh-install: every listed MCP resource reads cleanly.

Release test item 4 (#3480) connected a fresh install through
`okg agent connect` and found four MCP resources listed, every one of which
errored when read. Codex read them first and gave up without calling search.

These tests build the server the way the agent launcher does: for a
published deployment with no `.okg/up` state, from a working directory of
`/`. They list the resources through FastMCP and read each one through the
same path a client takes.

`okg install` does not make the deployment a Git repository. The fixture
git-inits the deployment directory to model an in-repo deployment, so the
read-status lookup has a repository root to search and the fresh-install
case still proves that no record means no listed resource.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import subprocess
from pathlib import Path
from typing import Any, Iterator

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.mcp import schema_digest  # noqa: E402
from okg.substrate.mcp.server import (  # noqa: E402
    build_server,
    readside_metadata_json,
)
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate._db_helpers import seed_catalog_version  # noqa: E402


# Tests that publish a generation carry postgres + serial_db; the readside
# serializer test needs no database.

DEPLOYMENT = "fresh"

#: The read-side fields the resource documents. Anything else in the
#: server's metadata (paths, the tool-profile config, contract lifecycle,
#: the manifest-resolution flag) is server-internal.
READSIDE_FIELDS = {
    "deployment",
    "instructions",
    "audit_questions",
    "skill_triggers",
    "skills",
}


def _write_deployment(root: Path) -> Path:
    """A published deployment, git-initialised to model an in-repo one."""
    dep = root / DEPLOYMENT
    (dep / "schemas").mkdir(parents=True)
    (dep / "docs").mkdir()
    (dep / "skills").mkdir()
    (dep / "source_registry.yaml").write_text("", encoding="utf-8")
    (dep / "docs" / "mcp-instructions.md").write_text(
        "Search the fresh deployment.", encoding="utf-8",
    )
    (dep / "docs" / "audit-questions.md").write_text(
        "1. Which chunks mention calibration?", encoding="utf-8",
    )
    (dep / "skills" / "lookup.md").write_text("# lookup\n", encoding="utf-8")
    # An unquoted YAML date loads as datetime.date, which json.dumps refuses.
    (dep / "docs" / "skill-triggers.yaml").write_text(
        "version: 1\nreviewed: 2026-10-07\nintents:\n"
        "  lookup:\n    skill: lookup\n    subtypes: [\"*\"]\n",
        encoding="utf-8",
    )
    (dep / "deployment.yaml").write_text(
        f"""
name: {DEPLOYMENT}
description: test
schema_dir: schemas
source_registry: source_registry.yaml
mcp_instructions: docs/mcp-instructions.md
skills_dir: skills
audit_questions: docs/audit-questions.md
skill_triggers: docs/skill-triggers.yaml
""".lstrip(),
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "-q", str(dep)], check=True)
    return dep


def _publish(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "chunk:a", "document_chunk", {"text": "beam calibration"},
                    {"id": "chunk:a"}, {"v": 1},
                ),
                NodeFact(
                    "chunk:b", "document_chunk", {"text": "gain drift"},
                    {"id": "chunk:b"}, {"v": 1},
                ),
                EdgeFact(
                    "chunk:a", "chunk:b", "next_chunk",
                    source_record_id={"id": "chunk:a-next"},
                    source_revision={"v": 1},
                ),
            ],
            source="default",
        )
    result = Publisher(
        dsn, DEPLOYMENT,
        catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    ).cycle()
    assert result["status"] == "published", result
    return str(result["generation_id"])


@pytest.fixture
def fresh_install(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[str, Path, str]]:
    deployments = tmp_path / "deployments"
    dep_dir = _write_deployment(deployments)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments))
    generation_id = _publish(clean_runtime)
    # The agent launcher starts the server from `/`.
    monkeypatch.chdir("/")
    yield clean_runtime, dep_dir, generation_id


def _server(dsn: str) -> Any:
    return build_server(dsn=dsn, deployment_name=DEPLOYMENT)


def _close(mcp: Any) -> None:
    mcp._okg_pool.close()
    if getattr(mcp, "_okg_write_pool", None):
        mcp._okg_write_pool.close()


def _list_uris(mcp: Any) -> list[str]:
    return sorted(str(r.uri) for r in asyncio.run(mcp.list_resources()))


def _read(mcp: Any, uri: str) -> str:
    contents = list(asyncio.run(mcp.read_resource(uri)))
    assert len(contents) == 1, (uri, contents)
    content = contents[0].content
    assert isinstance(content, str), (uri, type(content))
    return content


def _strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield str(key)
            yield from _strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _strings(item)


@pytest.mark.postgres
@pytest.mark.serial_db
def test_every_listed_resource_reads_on_a_fresh_install(
    fresh_install: tuple[str, Path, str],
) -> None:
    dsn, dep_dir, generation_id = fresh_install
    mcp = _server(dsn)
    try:
        uris = _list_uris(mcp)
        # No `okg deployment up` ran, so no lifecycle record exists and no
        # read-status resource may be listed.
        assert not [uri for uri in uris if "/read-status/" in uri], uris
        assert f"okg://readside-metadata/{DEPLOYMENT}" in uris
        assert f"okg://schema-digest/{DEPLOYMENT}" in uris

        payloads = {uri: json.loads(_read(mcp, uri)) for uri in uris}

        readside = payloads[f"okg://readside-metadata/{DEPLOYMENT}"]
        assert set(readside) == READSIDE_FIELDS
        assert readside["deployment"] == DEPLOYMENT
        assert readside["instructions"] == "Search the fresh deployment."
        assert sorted(readside["skills"]) == ["lookup"]
        assert readside["skill_triggers"]["reviewed"] == "2026-10-07"
        for text in _strings(readside):
            assert not text.startswith("/"), text
            assert str(dep_dir) not in text, text

        digest = payloads[f"okg://schema-digest/{DEPLOYMENT}"]
        assert digest["pinned_generation_id"] == generation_id
        assert isinstance(digest["pinned_generation_id"], str)
        assert "document_chunk" in digest["subtypes"]
        chunk = digest["subtypes"]["document_chunk"]
        assert chunk["node_count"] == 2
        assert "text" in chunk["attr_keys"]
        assert chunk["edges_out"] == ["next_chunk"]
        assert chunk["edges_in"] == ["next_chunk"]
        assert digest["edge_type_counts"] == {"next_chunk": 1}
        assert not digest.get("minimal"), digest
    finally:
        _close(mcp)


@pytest.mark.postgres
@pytest.mark.serial_db
def test_schema_digest_minimal_path_pins_the_text_generation_id(
    fresh_install: tuple[str, Path, str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn, _dep_dir, generation_id = fresh_install
    # Force the budget overflow so the resource falls back to the minimal
    # digest, which pins the generation a second time.
    monkeypatch.setattr(schema_digest, "MAX_DIGEST_BYTES", 16)
    mcp = _server(dsn)
    try:
        digest = json.loads(_read(mcp, f"okg://schema-digest/{DEPLOYMENT}"))
        assert digest["minimal"] is True, digest
        assert digest["pinned_generation_id"] == generation_id
        assert digest["subtypes"]["document_chunk"]["node_count"] == 2
        assert digest["edge_type_counts"] == {"next_chunk": 1}
    finally:
        _close(mcp)


@pytest.mark.postgres
@pytest.mark.serial_db
def test_a_lifecycle_record_in_the_deployment_repository_is_listed_and_read(
    fresh_install: tuple[str, Path, str],
) -> None:
    """The positive control: a record `okg deployment up` left is served.

    The record sits in the deployment repository's `.okg/up`, the server runs
    from `/`, and only the profile with a record is listed.
    """
    dsn, dep_dir, _generation_id = fresh_install
    record = dep_dir / ".okg" / "up" / f"{DEPLOYMENT}-dev.json"
    record.parent.mkdir(parents=True)
    record.write_text(json.dumps({
        "deployment": DEPLOYMENT,
        "operation_id": "up-fresh-dev",
        "lifecycle_plan_admission": {"plan_digest": "sha256:" + "8" * 64},
        "active_phase": None,
        "completed": True,
        "status": "completed",
        "phase_statuses": {"preflight": "completed"},
    }), encoding="utf-8")
    mcp = _server(dsn)
    try:
        uris = _list_uris(mcp)
        read_status = [uri for uri in uris if "/read-status/" in uri]
        assert read_status == [f"okg://deployment/{DEPLOYMENT}/read-status/dev"]
        payload = json.loads(_read(mcp, read_status[0]))
        assert payload["contents"]["operation_id"] == "up-fresh-dev"
        for uri in uris:
            json.loads(_read(mcp, uri))
    finally:
        _close(mcp)


def _slow_reader(monkeypatch: pytest.MonkeyPatch, *, when: str) -> None:
    """Make every digest statement whose SQL contains ``when`` sleep 2 s first.

    The sleep runs on the digest's own connection inside its transaction, so
    the digest's statement_timeout cancels it exactly as it would cancel a
    slow scan on a large graph.
    """
    real = schema_digest._generation_reader

    def reader(conn: Any, pinned_generation_id: Any) -> Any:
        read = real(conn, pinned_generation_id)

        def slow(sql: str, params: Any = None) -> Any:
            if when in sql:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_sleep(2)")
            return read(sql, params)

        return slow

    monkeypatch.setattr(schema_digest, "_generation_reader", reader)
    monkeypatch.setattr(schema_digest, "DIGEST_STATEMENT_TIMEOUT_MS", 300)


@pytest.mark.postgres
@pytest.mark.serial_db
def test_a_full_digest_that_times_out_falls_back_to_minimal_and_says_so(
    fresh_install: tuple[str, Path, str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn, _dep_dir, generation_id = fresh_install
    # Only the attr-key scan is slow: the full digest times out, the
    # count-only fallback does not.
    real_reader = schema_digest._generation_reader
    real_timeout = schema_digest.DIGEST_STATEMENT_TIMEOUT_MS
    _slow_reader(monkeypatch, when="jsonb_object_keys")
    mcp = _server(dsn)
    try:
        digest = json.loads(_read(mcp, f"okg://schema-digest/{DEPLOYMENT}"))
        assert digest["minimal"] is True, digest
        assert "timed out" in digest["minimal_reason"], digest
        assert "300 ms" in digest["minimal_reason"], digest
        assert digest["pinned_generation_id"] == generation_id
        assert digest["subtypes"]["document_chunk"]["node_count"] == 2

        # The pooled connection is still usable: with the slow scan gone and
        # the cache cleared, the same server serves the full digest.
        monkeypatch.setattr(schema_digest, "_generation_reader", real_reader)
        monkeypatch.setattr(
            schema_digest, "DIGEST_STATEMENT_TIMEOUT_MS", real_timeout,
        )
        mcp._okg_digest_cache.clear()
        full = json.loads(_read(mcp, f"okg://schema-digest/{DEPLOYMENT}"))
        assert not full.get("minimal"), full
        assert "text" in full["subtypes"]["document_chunk"]["attr_keys"]
    finally:
        _close(mcp)


@pytest.mark.postgres
@pytest.mark.serial_db
def test_a_minimal_digest_that_times_out_surfaces_the_error(
    fresh_install: tuple[str, Path, str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No invented counts: when even the count-only path times out, the
    read fails with the timeout rather than serving an empty digest."""
    dsn, _dep_dir, _generation_id = fresh_install
    _slow_reader(monkeypatch, when="okg.graph_")
    mcp = _server(dsn)
    try:
        with pytest.raises(Exception, match="statement timeout"):
            _read(mcp, f"okg://schema-digest/{DEPLOYMENT}")
        assert DEPLOYMENT not in mcp._okg_digest_cache
    finally:
        _close(mcp)


def test_readside_metadata_json_serializes_yaml_dates() -> None:
    """An unquoted YAML date in skill_triggers must not break the resource."""
    payload = json.loads(readside_metadata_json({
        "deployment": "acme",
        "instructions": "",
        "audit_questions": "",
        "skill_triggers": {"reviewed": datetime.date(2026, 10, 7)},
        "skills": {},
        "agent_surface_dir": Path("/srv/acme"),
        "deployment_dir": Path("/srv/acme"),
    }))
    assert payload["skill_triggers"] == {"reviewed": "2026-10-07"}
    assert set(payload) == READSIDE_FIELDS
