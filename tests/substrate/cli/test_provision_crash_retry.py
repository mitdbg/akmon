"""#1889: `okg provision` retried after a hard crash must still publish.

Kubernetes runs `okg provision --publish-once` as an init container and
restarts the same command after an out-of-memory kill. This test reproduces
that on a small scaffold deployment with its own database:

1. a child process runs `okg provision` and is killed with SIGKILL at a
   controlled point (no `finally`, no rollback, no lease release);
2. the same `okg provision` command is run again, in-process;
3. the retry must publish a generation that serves the fixture's nodes.

The crash points are module attributes of ``okg.substrate.ingest.service``
replaced, in the child only, by a function that SIGKILLs its own process:

* ``_publish``: the source run committed and its admission was accepted,
  then the process died before any publish (the heavy step, where the
  nightly lane's OOM kill most plausibly lands);
* ``_finalize_source_admission``: the source appended its facts, then the
  process died before the admission was finalized or the watermark advanced.

Both provision shapes are exercised: ``--publish-once`` (the chart default,
one publish after every source) and the per-layer default.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

import psycopg
import pytest
import yaml
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

import okg.substrate.cli.main as cli
from okg.substrate.deployment_scaffold import create_deployment_scaffold
from okg.substrate.graph_store import ChronosGraphStore


pytestmark = pytest.mark.postgres

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CHILD_TIMEOUT_S = 300

# Runs `okg <argv>` with one service function replaced by a hard kill.
# pg_cron is dropped from the required extensions exactly as the parent's
# private-database fixture does: pg_cron lives in the server's configured
# database, not in a per-test database.
_CRASHING_CHILD = """
import os, signal, sys
from okg.substrate.db import migrate as migration
migration.REQUIRED_EXTENSIONS.pop("pg_cron", None)
import okg.substrate.ingest.service as service
crash_point = sys.argv[1]
if not callable(getattr(service, crash_point, None)):
    sys.exit(f"unknown crash point {crash_point!r}")
def _hard_crash(*args, **kwargs):
    sys.stdout.flush()
    sys.stderr.write(f"SIMULATED HARD CRASH at {crash_point}\\n")
    sys.stderr.flush()
    os.kill(os.getpid(), signal.SIGKILL)
from tests.substrate.cli import _crash_source_child
if crash_point in _crash_source_child.SOURCE_CHILD_CRASH_POINTS:
    # Runs inside the source's child process; the child kills this process.
    _crash_source_child.install(crash_point)
else:
    setattr(service, crash_point, _hard_crash)
from okg.substrate.cli.main import main
sys.exit(main(sys.argv[2:]))
"""


@pytest.fixture
def crash_private_dsn(okg_dsn: str):
    """Own one fresh database: provision claims catalog ownership, which must
    never touch the shared test database."""
    options = conninfo_to_dict(okg_dsn)
    options.pop("dbname", None)

    def target(name: str) -> str:
        uri = "postgresql:///" + quote(name, safe="")
        if options:
            uri += "?" + urlencode(options, quote_via=quote)
        return uri

    name = "okg_test_crash_retry_" + uuid.uuid4().hex
    admin, dsn = target("postgres"), target(name)
    with psycopg.connect(admin, autocommit=True, connect_timeout=15) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))

    from okg.substrate.db import migrate as migration

    original = dict(migration.REQUIRED_EXTENSIONS)
    migration.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        yield dsn
    finally:
        from okg.substrate.db.pool import close_all

        close_all()
        migration.REQUIRED_EXTENSIONS.clear()
        migration.REQUIRED_EXTENSIONS.update(original)
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(name)
                ),
            )


_GIT_ENV = {
    "GIT_AUTHOR_NAME": "OKG crash-retry test",
    "GIT_AUTHOR_EMAIL": "crash-retry@example.invalid",
    "GIT_COMMITTER_NAME": "OKG crash-retry test",
    "GIT_COMMITTER_EMAIL": "crash-retry@example.invalid",
}


def _git(root: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=root,
        env={**os.environ, **_GIT_ENV},
        check=True,
        capture_output=True,
        text=True,
    )


def _latest_published(dsn: str, deployment: str):
    with psycopg.connect(dsn, autocommit=True) as conn:
        return ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).latest_generation(deployment_name=deployment, status="published")


def _published_node_ids(dsn: str, generation_id: str) -> list[str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        generation = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(generation_id)
        rows = generation.query(
            "SELECT node_id FROM okg.graph_nodes ORDER BY node_id",
        )
    return [str(row["node_id"]) for row in rows]


def _durable_state(dsn: str, deployment: str) -> dict[str, Any]:
    """What a crash left behind: the rows a retry has to deal with."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT e.status, e.admission_mode,
                   EXISTS (
                     SELECT 1 FROM okg.admission_record_refs r
                      WHERE r.admission_id = e.admission_id
                   ) AS has_refs
              FROM okg.admission_envelopes e
             WHERE e.deployment_name = %s AND e.producer_kind = 'source'
             ORDER BY e.created_at
            """,
            (deployment,),
        )
        envelopes = [
            {"status": row[0], "admission_mode": row[1], "has_refs": row[2]}
            for row in cur.fetchall()
        ]
        cur.execute(
            """
            SELECT source,
                   last_applied_rev <> '{}'::jsonb AS has_cursor,
                   last_reconcile_at IS NOT NULL AS reconciled,
                   replay_required IS NOT NULL AS replay_required
              FROM okg.source_watermarks
             ORDER BY source
            """
        )
        watermarks = [
            {
                "source": row[0],
                "has_cursor": row[1],
                "reconciled": row[2],
                "replay_required": row[3],
            }
            for row in cur.fetchall()
        ]
        cur.execute(
            """
            SELECT metadata::jsonb->>'status', count(*)
              FROM public._chronos_branch_interval_checkpoints
             WHERE metadata::jsonb->>'deployment_name' = %s
             GROUP BY 1
            """,
            (deployment,),
        )
        checkpoints = {str(row[0]): int(row[1]) for row in cur.fetchall()}
    return {
        "envelopes": envelopes,
        "watermarks": watermarks,
        "checkpoints": checkpoints,
    }


def _parse_emitted(emitted: str) -> dict[str, Any]:
    """The CLI's JSON document, whole or as its last line."""
    for candidate in (emitted, emitted.splitlines()[-1] if emitted else ""):
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return {"unparsed_output": emitted[-4000:]}


# The one source admission each crash point leaves behind.
_EXPECTED_CRASH_ENVELOPE = {
    # Source run finished and was admitted, with its fact references
    # attached; the publish that would sweep it never started.
    "_publish": {
        "status": "accepted", "admission_mode": "fast_track", "has_refs": True,
    },
    # Killed before admission: the envelope created before the run stays
    # pending, with no fact references.
    "_finalize_source_admission": {
        "status": "pending", "admission_mode": "fast_track", "has_refs": False,
    },
}


_PROVISION_SHAPES = {
    "publish_once": ["--publish-once"],
    "per_layer": [],
}


@pytest.mark.parametrize(
    "crash_point", ["_publish", "_finalize_source_admission"],
)
@pytest.mark.parametrize("shape", sorted(_PROVISION_SHAPES))
def test_provision_retry_after_hard_crash_publishes(
    tmp_path: Path,
    crash_private_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    record_property,
    crash_point: str,
    shape: str,
) -> None:
    dsn = crash_private_dsn
    root = tmp_path / "deployments"
    dep = create_deployment_scaffold("test", deployments_root=root).deployment_dir
    manifest = yaml.safe_load((dep / "deployment.yaml").read_text())
    manifest["release"] = {
        "channel": "dev",
        "pin": {"code_repository": str(_REPO_ROOT)},
    }
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8",
    )
    _git(dep, "init", "-q", "-b", "main")
    _git(dep, "add", "-A")
    _git(dep, "commit", "-q", "-m", "scaffold inputs")
    monkeypatch.setenv("OKG_DSN", dsn)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    argv = ["provision", "--deployment", "test", *_PROVISION_SHAPES[shape], "--json"]

    # ---- attempt 1: killed with SIGKILL at the crash point ----------------
    child = subprocess.run(
        [sys.executable, "-c", _CRASHING_CHILD, crash_point, *argv],
        cwd=_REPO_ROOT,
        env=dict(os.environ),
        capture_output=True,
        text=True,
        timeout=_CHILD_TIMEOUT_S,
    )
    assert child.returncode == -signal.SIGKILL, (
        f"the first provision must die at {crash_point}; "
        f"rc={child.returncode}\nstdout={child.stdout[-4000:]}\n"
        f"stderr={child.stderr[-4000:]}"
    )
    assert f"SIMULATED HARD CRASH at {crash_point}" in child.stderr
    assert _latest_published(dsn, "test") is None
    crashed_state = _durable_state(dsn, "test")
    record_property("crashed_state", json.dumps(crashed_state, sort_keys=True))
    # Pin what the crash left, so a crash point that moved (or a crash that
    # left nothing to recover) cannot pass this test vacuously.
    assert crashed_state["checkpoints"] == {}, crashed_state
    assert crashed_state["envelopes"] == [_EXPECTED_CRASH_ENVELOPE[crash_point]], (
        crashed_state
    )

    # ---- attempt 2: the same command, as an init container restarts it ----
    capsys.readouterr()
    rc = cli.main(argv)
    captured = capsys.readouterr()
    # A refusal is emitted as a JSON error on stderr; a result on stdout.
    emitted = captured.out.strip() or captured.err.strip()
    out = _parse_emitted(emitted)
    retried_state = _durable_state(dsn, "test")
    record_property("retry_payload", json.dumps(out, sort_keys=True, default=str))
    record_property("retried_state", json.dumps(retried_state, sort_keys=True))
    diagnosis = json.dumps(
        {
            "rc": rc,
            "failure_reasons": out.get("failure_reasons"),
            "failed_sources": out.get("failed_sources"),
            "publish_proof": out.get("publish_proof"),
            "error": out.get("error"),
            "unparsed_output": out.get("unparsed_output"),
            "crashed_state": crashed_state,
            "retried_state": retried_state,
        },
        indent=1,
        sort_keys=True,
        default=str,
    )
    assert rc == 0 and out.get("ok") is True, diagnosis
    published = _latest_published(dsn, "test")
    assert published is not None, diagnosis
    assert str(out["generation_id"]) == str(published.checkpoint_id), diagnosis
    assert _published_node_ids(dsn, published.checkpoint_id) == [
        "test:doc:1",
        "test:doc:1:section:1",
    ]


def test_publish_once_ingest_enforces_incomplete_recovery_before_sources(
    tmp_path: Path,
    crash_private_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The #1889 setting runs the WHOLE guarded pre-source block, including
    the incomplete-recovery resume. In repair-required state, a
    `--publish-once` style ingest (``publish=False`` plus the setting) with a
    narrower source set is refused before any source runs, exactly as a
    per-layer ingest is.

    Since #3342 a failed source whose undo finished publishes instead of
    freezing, so the freeze is cut the way it still happens: the failed
    source's compensation raises."""
    from dataclasses import replace

    import okg.substrate.ingest.service as ingest_service
    from okg.substrate.db import migrate as migration
    from okg.substrate.errors import SubstrateError
    from okg.substrate.ingest import IngestOptions, run_ingest
    from tests.pytest.catalog_fixture_builders import seed_catalog
    from tests.pytest.source_jobs_inline import use_inline_source_jobs
    from tests.substrate.test_ingest_service import (
        _claim_layered_fixture_catalog,
        _write_layered_fixture_deployment,
    )

    dsn = crash_private_dsn
    migration.ensure_required_extensions(dsn)
    migration.migrate(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    _claim_layered_fixture_catalog(dsn, dep)
    options = IngestOptions(
        dsn=dsn, deployment="svc", deployment_dir=dep, reset_cursor=True,
    )
    baseline = run_ingest(options).report["publishes"][0]
    assert baseline["status"] == "published", baseline

    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["second"]["params"]["fixture_path"] = str(
        dep / "missing-second.json"
    )
    registry_path.write_text(yaml.safe_dump(registry))
    _claim_layered_fixture_catalog(dsn, dep)
    with monkeypatch.context() as patch:
        use_inline_source_jobs(patch)
        original_retract = ingest_service._retract_chronos_run

        def retract(*args, **kwargs):
            if kwargs.get("source") == "svc.second":
                raise RuntimeError("injected compensation failure")
            return original_retract(*args, **kwargs)

        patch.setattr(ingest_service, "_retract_chronos_run", retract)
        blocked = run_ingest(options).report["publishes"][0]
    assert blocked["status"] == "blocked", blocked

    monkeypatch.setattr(
        ingest_service,
        "_run_source",
        lambda *_args, **_kwargs: pytest.fail("source must not run"),
    )
    with pytest.raises(SubstrateError) as mismatch:
        run_ingest(replace(
            options,
            include={"first"},
            include_predecessors=False,
            publish=False,
            redrive_staged_before_sources=True,
        ))

    assert mismatch.value.data["reason"] == (
        "incomplete_recovery_contract_mismatch"
    )
